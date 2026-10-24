"""`s3` disk shape cards (R2): objects and bytes per part by table shape and part type, and the multipart graph.

Every parameter comes from the manifest (`experiments/<experiment_id>.json` of the s3 report); expected values come
from `predictions/<experiment_id>.<policy>.json` through `params.report_dir`. `params.tables` lists the tables of
the case (`name`, `columns`, `order_by`, optional `partition_by`, `ttl`, `settings`, `row_bytes` or
`row_bytes_bounds`); `params.inserts` lists the INSERTs (`table`, `rows`, `count`, `insert` template).
"""
from __future__ import annotations

import json
import os
import time

from . import s3plain
from .base import DATABASE, POLL_S
from .lifecycle import LifecycleCard, check_observation, defaults
from .probes import REPO, _count, _sql, compare_ledger, register, window_rows

REGISTRY = os.path.join(REPO, 'docs', 'superpowers', 'cost-model', 'inputs-registry.json')
PART_COLUMNS = ('table, name, part_type, rows, level, active, path, partition_id, data_uncompressed_bytes, '
                'bytes_on_disk')
OBJECT_WRITE_OPS = ('PutObject', 'CreateMultipartUpload', 'UploadPart', 'CompleteMultipartUpload')
# Disks that write each part file as its own object and delete it with the part
# (Disks/DiskObjectStorage/DiskObjectStorageTransaction.cpp:274). The object checks hold only there: a CAS disk
# stores each payload once by content hash and leaves removal to its GC
# (Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.cpp:614), and a local disk
# writes files, not objects (Disks/DiskLocal.cpp:464).
OBJECT_PER_FILE_POLICIES = ('s3', 's3_cached', 's3plain', 's3plain_cached')
ALL_POLICIES = OBJECT_PER_FILE_POLICIES + ('cas', 'cas_cached', 'local')


def object_per_file(policy: str) -> bool:
    return policy in OBJECT_PER_FILE_POLICIES


def allocated(root: str, rel_dirs: list) -> dict:
    """Files, apparent bytes and allocated bytes under `root`/`rel` for every relative directory in `rel_dirs`."""
    out = {'files': 0, 'bytes': 0, 'allocated': 0}
    for rel in rel_dirs:
        for dirpath, _, names in os.walk(os.path.join(root, rel)):
            for name in names:
                st = os.lstat(os.path.join(dirpath, name))
                out['files'] += 1
                out['bytes'] += st.st_size
                out['allocated'] += st.st_blocks * 512
    return out


def tree_allocated(root: str) -> int:
    return allocated(root, [''])['allocated'] if os.path.isdir(root) else 0


# Object size buckets: small files, up to the default 1 MiB read buffer, up to the 32 MiB first upload buffer, beyond.
SIZE_BUCKETS = (('le_4KiB', 0, 4096), ('4KiB_1MiB', 4097, 1 << 20), ('1MiB_32MiB', (1 << 20) + 1, 32 << 20),
                ('gt_32MiB', (32 << 20) + 1, float('inf')))


def size_histogram(sizes: list) -> dict:
    """Fraction of objects per size bucket."""
    out = dict.fromkeys((b[0] for b in SIZE_BUCKETS), 0.0)
    for x in sizes:
        out[next(name for name, lo, hi in SIZE_BUCKETS if lo <= x <= hi)] += 1
    return {k: v / len(sizes) for k, v in out.items()} if sizes else out


def metadata_ratios(metadata: dict | None, objects: int, write_syscalls: int) -> dict:
    """Local metadata per object of the s3 disk: files, apparent and allocated bytes; write syscalls per object."""
    if not metadata or not objects:
        return {}
    return {'metadata_files_per_object': metadata['files'] / objects,
            'metadata_bytes_per_object': metadata['bytes'] / objects,
            'metadata_allocated_per_object': metadata['allocated'] / objects,
            'write_syscalls_per_object': write_syscalls / objects}


def load_registry() -> dict:
    with open(REGISTRY) as f:
        return json.load(f)


def predicted_part_type(registry: dict, table_settings: dict, block_bytes: int, rows: int, level: int = 0) -> str:
    """The registry predicate `part_type_predicate`: Compact when bytes, rows or level is below its minimum.

    Table settings of the manifest override the registry defaults, as `MergeTreeSettings` does.
    """
    d = {**defaults(registry), **table_settings}
    below = (block_bytes < d['min_bytes_for_wide_part'] or rows < d['min_rows_for_wide_part']
             or level < d['min_level_for_wide_part'])
    return 'Compact' if below else 'Wide'


def insert_part_type(registry: dict, table: dict, table_settings: dict, rows: int) -> str:
    """Part type of one inserted part of `rows` rows; ValueError when the row-size bounds straddle the threshold."""
    lo, hi = table['row_bytes_bounds'] if 'row_bytes_bounds' in table else (table['row_bytes'], table['row_bytes'])
    settings = {**table_settings, **table.get('settings', {})}
    types = {predicted_part_type(registry, settings, rows * b, rows) for b in (lo, hi)}
    if len(types) != 1:
        raise ValueError(f'table {table["name"]}: {rows} rows of {lo}..{hi} bytes straddle the part-type threshold')
    return types.pop()


class S3Card(LifecycleCard):
    """A manifest card over the tables of `params.tables`; the workload runs on `params.replica`."""

    # The variant reports reuse these cards with their own manifests and predictions; the s3plain capability gate
    # refuses some operations, and checks of the one-object-per-file model run only on OBJECT_PER_FILE_POLICIES.
    policies = ALL_POLICIES

    def supports(self, policy):
        gate = s3plain.supports(self.name, policy)
        return gate if gate is not None else super().supports(policy)

    def tables(self, ctx):
        return [t['name'] for t in self.p(ctx)['tables']]

    def table_settings(self, ctx, table: dict) -> dict:
        return {'storage_policy': ctx.policy, 'primary_key_lazy_load': 0, 'prewarm_mark_cache': 0,
                'prewarm_primary_key_cache': 0, **ctx.manifest['settings']['table'], **table.get('settings', {})}

    def ddl(self, ctx, table: dict) -> str:
        parts = [f'CREATE TABLE {DATABASE}.{table["name"]} ({table["columns"]}) ENGINE = MergeTree']
        if table.get('partition_by'):
            parts.append(f'PARTITION BY {table["partition_by"]}')
        parts.append(f'ORDER BY {table.get("order_by", "k")}')
        if table.get('ttl'):
            parts.append(f'TTL {table["ttl"]}')
        parts.append('SETTINGS ' + ', '.join(f'{k} = {_sql(v)}' for k, v in self.table_settings(ctx, table).items()))
        return ' '.join(parts)

    def setup(self, ctx):
        client = self.replica_client(ctx)
        client.query(f'CREATE DATABASE IF NOT EXISTS {DATABASE}')
        for table in self.p(ctx)['tables']:
            client.query(self.ddl(ctx, table))

    def run_inserts(self, ctx, inserts: list) -> int:
        client, seed, n = self.replica_client(ctx), self.p(ctx)['seed'], 0
        for spec in inserts:
            for i in range(spec.get('count', 1)):
                sql = spec['insert'].format(table=f'{DATABASE}.{spec["table"]}', rows=spec['rows'], seed=seed, i=i,
                                            n=spec.get('count', 1))
                client.query(sql, settings=ctx.manifest['settings']['query'])
                n += 1
        return n

    def inserted_rows(self, ctx) -> dict:
        out = {}
        for spec in self.p(ctx)['inserts']:
            out[spec['table']] = out.get(spec['table'], 0) + spec['rows'] * spec.get('count', 1)
        return out

    def measured(self, ctx):
        n = self.run_inserts(ctx, self.p(ctx)['inserts'])
        return {'unit_count': n, 'effective_counts': {'inserts': n, 'rows': sum(self.inserted_rows(ctx).values())},
                'observations': {}}

    def where_tables(self) -> str:
        return f"database = '{DATABASE}'"

    def all_parts(self, ctx) -> list:
        """Every part of the case's tables, inactive ones included, ordered by table, level and block range."""
        rows = self.rows_json(ctx, f'SELECT {PART_COLUMNS} FROM system.parts WHERE {self.where_tables()} '
                                   'ORDER BY table, level, min_block_number, max_block_number, name')
        return [{'table': r['table'], 'name': r['name'], 'part_type': r['part_type'], 'rows': int(r['rows']),
                 'level': int(r['level']), 'active': int(r['active']), 'path': r['path'],
                 'partition_id': r['partition_id'], 'data_uncompressed_bytes': int(r['data_uncompressed_bytes']),
                 'bytes_on_disk': int(r['bytes_on_disk'])} for r in rows]

    def snapshot_layout(self, ctx, parts: list) -> dict:
        """`table/part` -> {file (relative to the part directory): [remote key, size]} from `system.remote_data_paths`.

        A projection's files sit under `<name>.proj/` of their parent part and count towards it.
        """
        disk = ctx.policy.removesuffix('_cached')
        rows = self.rows_json(ctx, 'SELECT DISTINCT local_path, remote_path, size FROM system.remote_data_paths '
                                   f"WHERE disk_name IN ('{disk}', '{ctx.policy}')")
        anchors = []
        for part in parts:
            path = part['path']
            # Paths of system.parts are absolute; local_path is relative to the disk: match from `store/` on.
            rel = path[path.find('store/'):] if 'store/' in path else path
            anchors.append((rel.rstrip('/') + '/', f'{part["table"]}/{part["name"]}'))
        out = {}
        for r in rows:
            local = r['local_path'][r['local_path'].find('store/'):] if 'store/' in r['local_path'] else r['local_path']
            hit = next(((a, key) for a, key in anchors if local.startswith(a)), None)
            if hit is not None:
                out.setdefault(hit[1], {})[local[len(hit[0]):]] = [r['remote_path'], int(r['size'])]
        return out

    def drained(self, ctx) -> bool:
        """No merge or unfinished mutation; with `params.wait_outdated_removed`, no inactive part either."""
        where = self.where_tables()
        sql = (f'SELECT (SELECT count() FROM system.merges WHERE {where}) + '
               f'(SELECT count() FROM system.mutations WHERE is_done = 0 AND {where})')
        if self.p(ctx).get('wait_outdated_removed'):
            sql += f' + (SELECT count() FROM system.parts WHERE active = 0 AND {where})'
        return _count(self.replica_client(ctx), sql) == 0

    def wait_drained(self, ctx, timeout_s: float) -> None:
        deadline = time.monotonic() + timeout_s
        while not self.drained(ctx):
            if time.monotonic() >= deadline:
                raise TimeoutError(f'{self.name}: merges, mutations or outdated parts not drained in {timeout_s} s')
            time.sleep(POLL_S)
        for client in ctx.cluster.clients:
            client.query('SYSTEM FLUSH LOGS')

    def complete(self, ctx, timeout_s=300):
        self.wait_drained(ctx, timeout_s)
        st = self.state(ctx)
        st['parts'] = self.all_parts(ctx)
        st['layout'] = self.snapshot_layout(ctx, st['parts'])
        st['metadata'] = self.metadata_files(ctx, [p for p in st['parts'] if p['active']])

    def metadata_files(self, ctx, parts: list) -> dict:
        """Local metadata of the `s3` disk for `parts`: one small file per object under `<replica>/s3_meta/`."""
        root = os.path.join(ctx.pass_dir, self.p(ctx)['replica'], 's3_meta')
        rels = [p['path'][p['path'].find('store/'):] for p in parts if 'store/' in p['path']]
        return allocated(root, rels) if os.path.isdir(root) else None

    def teardown(self, ctx):
        """Counts rows after the window: a count() over lightweight-deleted parts reads `_row_exists` from S3."""
        st = self.state(ctx)
        if 'rows' not in st:
            st['rows'] = [sum(_count(self.replica_client(ctx), f'SELECT count() FROM {DATABASE}.{t}')
                              for t in self.tables(ctx))]
        super().teardown(ctx)

    def collect_layout(self, ctx):  # the lifecycle hook keyed by part name only; this card keys by table too
        raise NotImplementedError('S3Card snapshots its layout with snapshot_layout')

    def reclassify(self, ctx, bounds):
        """No merge in these cards; mutations and drops reclassify in their own cards."""

    def requests_by_file(self, ctx, draft) -> dict:
        """Workload attempts of the window per op and part file (`table/part/file`); keys no snapshot listed
        (the startup access-check probe, objects created and removed inside the window) count as `unlisted`."""
        st = self.state(ctx)
        names = {key: f'{part}/{f}' for layout in (st.get('layout_before', {}), st.get('layout', {}))
                 for part, files in layout.items() for f, (key, _) in files.items()}
        out = {}
        for r in window_rows(ctx):
            if r['caller'] != self.p(ctx)['replica'] or r['seq'] > draft['window']['end_seq']:
                continue
            name = names.get(r['key'], 'unlisted') if r['key'] else 'batch'
            out.setdefault(r['op'], {})
            out[r['op']][name] = out[r['op']].get(name, 0) + 1
        return {op: dict(sorted(v.items())) for op, v in sorted(out.items())}

    def part_view(self, parts: list) -> list:
        return [{k: p[k] for k in ('table', 'name', 'part_type', 'rows', 'level', 'active')} for p in parts]

    def object_counts(self, layout: dict, parts: list) -> dict:
        return {f'{p["table"]}/{p["name"]}': len(layout.get(f'{p["table"]}/{p["name"]}', {})) for p in parts}

    def ops_of(self, ctx, draft) -> dict:
        return draft['ledger']['foreground'].get(self.p(ctx)['replica'], {})

    def assertions(self, ctx, draft):
        pred = self.predicted(ctx)
        out = compare_ledger(pred, draft['ledger']['foreground'], self.mode == 'deterministic')
        for key, want in pred['expected']['observations'].items():
            out[f'observation.{key}'] = check_observation(want, draft['observations'].get(key), draft['observations'])
        out['rows.' + self.p(ctx)['replica']] = (self.expected_rows(ctx), self.state(ctx).get('rows', [None])[0])
        if self.mode == 'deterministic':
            out['window.crossing'] = ([], draft['window']['crossing'])
        return out

    def expected_rows(self, ctx) -> int:
        return self.p(ctx).get('expected_rows', sum(self.inserted_rows(ctx).values()))


@register
class ObjectsPerPart(S3Card):
    """Inserts with merges stopped: objects, object sizes and part type of every inserted part.

    The part type each insert must produce is computed from the registry predicate and asserted against
    `system.parts`; the object count per part comes from `system.remote_data_paths` and is reconciled with the
    ledger's object writes (requests besides them are `insert_tx_checks`).
    """

    name = 'objects-per-part'

    def expected_types(self, ctx) -> list:
        registry, tables = load_registry(), {t['name']: t for t in self.p(ctx)['tables']}
        out = []
        for spec in self.p(ctx)['inserts']:
            kind = insert_part_type(registry, tables[spec['table']], ctx.manifest['settings']['table'], spec['rows'])
            out += [[spec['table'], spec['rows'], kind]] * spec.get('count', 1)
        return sorted(out)

    def observe(self, ctx, draft):
        st = self.state(ctx)
        parts = [p for p in st['parts'] if p['active']]
        counts = self.object_counts(st['layout'], parts)
        # Rows after the window (the teardown renames s3plain directories) are not the inserts'.
        rows = [r for r in window_rows(ctx) if r['caller'] == self.p(ctx)['replica'] and r['upstream']['outcome'] == 'ok'
                and r['seq'] <= draft['window']['end_seq']]
        # On s3plain a part directory is an object too (create and rename); it is not a part file.
        dir_puts = sum(1 for r in rows if r['op'] == 'PutObject' and s3plain.is_directory_object(r['key']))
        put_sizes = sorted(r['req_payload_bytes'] for r in rows
                           if r['op'] == 'PutObject' and not s3plain.is_directory_object(r['key']))
        ops = self.ops_of(ctx, draft)
        writes = sum(ops.get(op, {}).get('attempts', 0) for op in ('PutObject', 'CreateMultipartUpload')) - dir_puts
        files = {key: sorted(layout) for key, layout in st['layout'].items() if key in counts}
        return {'parts': self.part_view(parts),
                'part_types': sorted([p['table'], p['rows'], p['part_type']] for p in parts),
                'objects_per_part': counts,
                'object_files': files,
                'objects_total': sum(counts.values()),
                'object_bytes_total': sum(size for key in counts for _, size in st['layout'].get(key, {}).values()),
                'put_payload_sizes': put_sizes,
                'object_size_histogram': size_histogram(put_sizes),
                'object_writes': writes,
                'directory_writes': dir_puts,
                'insert_tx_checks': sum(c['attempts'] for op, c in ops.items() if op not in OBJECT_WRITE_OPS),
                'metadata': st.get('metadata'),
                **metadata_ratios(st.get('metadata'), sum(counts.values()),
                                  draft['proc_io'][self.p(ctx)['replica']]['syscw']),
                'local_write_syscalls': draft['proc_io'][self.p(ctx)['replica']]['syscw'],
                'local_write_bytes': draft['proc_io'][self.p(ctx)['replica']]['write_bytes']}

    def assertions(self, ctx, draft):
        out = super().assertions(ctx, draft)
        obs = draft['observations']
        out['part_types.registry_predicate'] = (self.expected_types(ctx), obs['part_types'])
        # Every object of every part is one object-creating request: one PutObject or one multipart upload.
        if object_per_file(ctx.policy):
            out['object_writes==objects_total'] = (obs['objects_total'], obs['object_writes'])
        return out


@register
class BigInsertMultipart(ObjectsPerPart):
    """One insert whose single `String` column file is past the first upload buffer: the multipart graph.

    `params.multipart_file` names the part file expected to go multipart; its upload-part sizes come from the
    ledger and must follow `multipart_sizing_rule` (asserted through the prediction's observations).
    """

    name = 'big-insert-multipart'

    def supports(self, policy):
        if policy == 'local':
            return False, ('UNSUPPORTED: a local disk writes part files to the filesystem and sends no object request, '
                           'so there is no multipart graph (Disks/DiskLocal.cpp:464)')
        return super().supports(policy)

    def observe(self, ctx, draft):
        out = super().observe(ctx, draft)
        rows = [r for r in window_rows(ctx) if r['caller'] == self.p(ctx)['replica'] and r['upstream']['outcome'] == 'ok'
                and r['seq'] <= draft['window']['end_seq']]
        uploads = {r['response_body']['upload_id']: r['key'] for r in rows
                   if r['op'] == 'CreateMultipartUpload' and (r.get('response_body') or {}).get('upload_id')}
        sizes = {u: [] for u in uploads}
        for r in sorted((r for r in rows if r['op'] == 'UploadPart' and r.get('upload_id') in uploads),
                        key=lambda r: r['part_number']):
            sizes[r['upload_id']].append(r['req_payload_bytes'])
        layout = self.state(ctx)['layout']
        multipart_files = sorted(f for files in layout.values() for f, (key, _) in files.items() if key in uploads.values())
        return {**out, 'upload_part_sizes': sorted(sizes.values()), 'multipart_files': multipart_files}

    def assertions(self, ctx, draft):
        out = super().assertions(ctx, draft)
        out.update(multipart_assertions(self.ops_of(ctx, draft), draft['observations'], self.p(ctx)['multipart_file']))
        return out


def multipart_assertions(ops: dict, observations: dict, multipart_file: str) -> dict:
    """The multipart graph of one object: one CreateMultipartUpload, at least two UploadPart, one Complete."""
    n = lambda op: ops.get(op, {}).get('attempts', 0)  # noqa: E731
    return {'CreateMultipartUpload==1': (1, n('CreateMultipartUpload')),
            'UploadPart>=2': (True, n('UploadPart') >= 2),
            'CompleteMultipartUpload==1': (1, n('CompleteMultipartUpload')),
            'multipart_files': ([multipart_file], observations.get('multipart_files'))}
