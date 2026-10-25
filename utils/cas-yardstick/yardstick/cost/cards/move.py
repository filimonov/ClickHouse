"""TTL move cards: parts inserted on a local hot volume, then moved by TTL to the cold volume of the variant's disk.

The storage policy `tiered` has two volumes: `hot` on the server's `default` disk and `cold` on the disk under test
(`s3`, `s3plain` or `cas`). Moves are stopped while the setup inserts and waits for the TTL to pass, so every part
lands on `hot` and the window holds only the moves: `SYSTEM START MOVES`, then a wait until no part is left on `hot`.
Merges stay off (`max_parts_to_merge_at_once = 1`), so the moved parts are exactly the inserted ones.

Expected values come from `predictions/<experiment_id>.<policy>.json` of the TTL move report (`params.report_dir`).
For `cas` the per-op counts depend on the blob branch each file takes (fresh or hash hit, `system.cas_log`); the
card checks them against the publish graph of the prediction's `inputs.cas_graph` at the observed population.
"""
from __future__ import annotations

import json
import time

from . import s3plain
from .base import DATABASE, POLL_S
from .cas import BRANCH_SQL, CasCard, population
from .cas_probe import CAS_FAMILIES, attribute
from .lifecycle import check_observation
from .probes import _count, compare_ledger, register, window_rows
from .shapes import S3Card, insert_part_type, load_registry

POLICY = 'tiered'
HOT_DISK = 'default'
EXPIRY_TIMEOUT_S = 120
PART_SQL = ("SELECT name, part_type, rows, active, disk_name, bytes_on_disk, path FROM system.parts "
            "WHERE database = '{db}' AND table = '{table}' ORDER BY name FORMAT JSONEachRow")
PART_LOG_SQL = ("SELECT event_type, part_name, disk_name, size_in_bytes FROM system.part_log WHERE database = '{db}' "
                "AND table = '{table}' AND event_type IN ('MovePart', 'DownloadPart') AND event_time_microseconds >= "
                "fromUnixTimestamp64Micro({t0}) ORDER BY event_time_microseconds FORMAT JSONEachRow")


def tiered_policy(disk: str) -> dict:
    """Server settings (dotted XML paths) of the policy: `hot` first, so inserts and fetches land there."""
    root = f'storage_configuration.policies.{POLICY}'
    # move_factor 0: only the TTL moves a part, never free space on the hot volume.
    return {f'{root}.volumes.hot.disk': HOT_DISK, f'{root}.volumes.cold.disk': disk, f'{root}.move_factor': 0}


def json_lines(text: str) -> list:
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def cas_graph_bounds(graph: dict, parts: int, wide_parts: int, fresh: int, hits: int) -> dict:
    """op -> [lo, hi] of the CAS publish graph for `parts` moved parts at a given blob branch population."""
    out = {}
    terms = ((graph['per_part'], parts), (graph['per_fresh_blob'], fresh), (graph['per_hash_hit'], hits),
             (graph['per_moved_part_load'], parts), (graph['per_moved_wide_part_load'], wide_parts))
    for counts, n in terms:
        for op, k in counts.items():
            out[op] = out.get(op, 0) + k * n
    for op, k in graph.get('table_once', {}).items():
        out[op] = out.get(op, 0) + k
    bounds = {op: [v, v] for op, v in out.items()}
    for op, (lo, hi) in graph.get('cold_table_once', {}).items():
        base = bounds.setdefault(op, [0, 0])
        bounds[op] = [base[0] + lo, base[1] + hi]
    return bounds


def by_kind(requests_per_part: dict, kinds: dict, op: str) -> dict:
    """part type -> attempts of `op` per part; a list when parts of one type differ."""
    seen = {}
    for part, ops in requests_per_part.items():
        if part in kinds:
            seen.setdefault(kinds[part], set()).add(ops.get(op, 0))
    return {k: next(iter(v)) if len(v) == 1 else sorted(v) for k, v in sorted(seen.items())}


@register
class MoveTtl(S3Card):
    """One table on the `tiered` policy; the window moves every inserted part from `hot` to `cold`."""

    name = 'move'
    policies = ('s3', 's3plain', 'cas')
    # A CAS mount renewal may cross a window edge; s3 and s3plain assert an empty crossing list themselves.
    mode = 'stochastic'
    replicas = 1

    def table_clients(self, ctx) -> list:
        return ctx.cluster.clients[:self.replicas]

    def settings(self, ctx):
        out = super().settings(ctx)
        out['server'].update(tiered_policy(ctx.policy))
        return out

    def table_settings(self, ctx, table):
        return {**super().table_settings(ctx, table), 'storage_policy': POLICY}

    def inventory_families(self, ctx):
        return CAS_FAMILIES if ctx.policy == 'cas' else []

    def classify(self, ctx, attempts, families):
        if ctx.policy != 'cas':
            return super().classify(ctx, attempts, families)
        st = self.state(ctx)
        if 'gc_rounds' not in st:
            raise RuntimeError('GC rounds were not collected; request origin cannot be attributed')
        split = attribute(attempts, st['gc_rounds'], families)
        st['split'] = {part: [a['seq'] for a in rows] for part, rows in split.items()}
        return split

    def table_names(self, ctx) -> list:
        return [t['name'] for t in self.p(ctx)['tables']]

    def setup(self, ctx):
        if ctx.manifest.get('mode', self.mode) != self.mode:
            raise ValueError(f'{self.name}: manifest mode {ctx.manifest["mode"]} differs from the card mode {self.mode}')
        for client in self.table_clients(ctx):
            client.query(f'CREATE DATABASE IF NOT EXISTS {DATABASE}')
            for table in self.p(ctx)['tables']:
                client.query(self.ddl(ctx, table))
                client.query(f'SYSTEM STOP MOVES {DATABASE}.{table["name"]}')
        self.run_inserts(ctx, self.p(ctx)['inserts'])
        for client in self.table_clients(ctx)[1:]:
            for t in self.table_names(ctx):
                client.query(f'SYSTEM SYNC REPLICA {DATABASE}.{t} STRICT')
        self.wait_expired(ctx)
        hot = self.parts_by_replica(ctx)
        misplaced = {r: [p['name'] for p in parts if p['active'] and p['disk_name'] != HOT_DISK]
                     for r, parts in hot.items()}
        if any(misplaced.values()):
            raise RuntimeError(f'{self.name}: parts not on the hot disk before the window: {misplaced}')
        self.state(ctx)['hot_parts'] = {r: sorted(p['name'] for p in parts if p['active']) for r, parts in hot.items()}

    def wait_expired(self, ctx) -> None:
        """Wait on the server clock until the move TTL of every row has passed (moves are stopped meanwhile)."""
        ttl = int(self.p(ctx)['ttl_seconds'])
        client = self.replica_client(ctx)
        deadline = time.monotonic() + EXPIRY_TIMEOUT_S
        for t in self.table_names(ctx):
            sql = f'SELECT now() > (SELECT max(event_time) FROM {DATABASE}.{t}) + {ttl}'
            while _count(client, sql) != 1:
                if time.monotonic() >= deadline:
                    raise TimeoutError(f'{self.name}: TTL of {DATABASE}.{t} did not pass in {EXPIRY_TIMEOUT_S} s')
                time.sleep(POLL_S)

    def parts_by_replica(self, ctx) -> dict:
        out = {}
        for n, client in enumerate(self.table_clients(ctx), 1):
            rows = []
            for t in self.table_names(ctx):
                rows += [{**r, 'table': t, 'rows': int(r['rows']), 'active': int(r['active']),
                          'bytes_on_disk': int(r['bytes_on_disk'])}
                         for r in json_lines(client.query(PART_SQL.format(db=DATABASE, table=t)))]
            out[f'r{n}'] = rows
        return out

    def left_on_hot(self, ctx) -> int:
        tables = ', '.join(f"'{t}'" for t in self.table_names(ctx))
        where = f"database = '{DATABASE}' AND table IN ({tables})"
        return sum(_count(c, f"SELECT (SELECT count() FROM system.parts WHERE {where} AND active AND disk_name = "
                             f"'{HOT_DISK}') + (SELECT count() FROM system.moves WHERE {where})")
                   for c in self.table_clients(ctx))

    def measured(self, ctx):
        for client in self.table_clients(ctx):
            for t in self.table_names(ctx):
                client.query(f'SYSTEM START MOVES {DATABASE}.{t}')
        deadline = time.monotonic() + ctx.manifest['timeout_s']
        while self.left_on_hot(ctx):
            if time.monotonic() >= deadline:
                raise TimeoutError(f'{self.name}: parts still on the hot volume after {ctx.manifest["timeout_s"]} s')
            time.sleep(POLL_S)
        n = sum(len(v) for v in self.state(ctx)['hot_parts'].values())
        return {'unit_count': n, 'effective_counts': {'moved_parts': n}, 'observations': {}}

    def complete(self, ctx, timeout_s=300):
        self.wait_drained(ctx, timeout_s)
        st, t0 = self.state(ctx), ctx.window()['t_start_us']
        st['parts_after'] = self.parts_by_replica(ctx)
        st['part_log'] = {f'r{n}': [r for t in self.table_names(ctx) for r in json_lines(
            client.query(PART_LOG_SQL.format(db=DATABASE, table=t, t0=t0)))]
            for n, client in enumerate(self.table_clients(ctx), 1)}
        if ctx.policy == 'cas':
            st['events'] = json_lines(self.replica_client(ctx).query(BRANCH_SQL.format(disk='cas', t0=t0)))
            CasCard.collect_rounds(self, ctx)

    def teardown(self, ctx):
        # Layout and placement read after the window: on CAS `system.remote_data_paths` reads manifests.
        st = self.state(ctx)
        moved = [p for p in st.get('parts_after', {}).get(self.p(ctx)['replica'], []) if p['active']]
        if ctx.policy == 'cas':
            st['placement'] = CasCard.placement(self, ctx)
        else:
            st['layout'] = self.snapshot_layout(ctx, moved)
        st['rows'] = [sum(_count(c, f'SELECT count() FROM {DATABASE}.{t}') for t in self.table_names(ctx))
                      for c in self.table_clients(ctx)]
        for client in reversed(self.table_clients(ctx)):
            client.query(f'DROP DATABASE IF EXISTS {DATABASE} SYNC')

    def expected_part_types(self, ctx) -> list:
        registry, tables = load_registry(), {t['name']: t for t in self.p(ctx)['tables']}
        out = []
        for spec in self.p(ctx)['inserts']:
            kind = insert_part_type(registry, tables[spec['table']], ctx.manifest['settings']['table'], spec['rows'])
            out += [[spec['rows'], kind]] * spec.get('count', 1)
        return sorted(out)

    def observe(self, ctx, draft):
        st, end = self.state(ctx), draft['window']['end_seq']
        rows = [r for r in window_rows(ctx) if r['seq'] <= end and r['caller'] != 'rig']
        fg = set(st['split']['foreground']) if 'split' in st else None
        per_replica = {}
        for name, parts in st['parts_after'].items():
            moved = [p for p in parts if p['active'] and p['name'] in st['hot_parts'][name]]
            mine = [r for r in rows if r['caller'] == name and (fg is None or r['seq'] in fg)
                    and r['upstream']['outcome'] == 'ok']
            dirs = [r for r in mine if r['op'] == 'PutObject' and s3plain.is_directory_object(r['key'])]
            files = [r for r in mine if r['op'] == 'PutObject' and not s3plain.is_directory_object(r['key'])]
            log = st['part_log'][name]
            per_replica[name] = {
                'moved_parts': len(moved),
                'moved_part_types': sorted([p['rows'], p['part_type']] for p in moved),
                'moved_disks': sorted({p['disk_name'] for p in moved}),
                'moved_bytes': sum(p['bytes_on_disk'] for p in moved),
                'put_bytes': sum(r['req_payload_bytes'] for r in files),
                'get_bytes': sum(r['resp_payload_bytes'] for r in mine if r['op'] == 'GetObject'),
                'directory_writes': len(dirs),
                'move_part_events': sum(1 for e in log if e['event_type'] == 'MovePart'),
                'download_part_events': sum(1 for e in log if e['event_type'] == 'DownloadPart')}
        out = {'replicas': per_replica, **per_replica[self.p(ctx)['replica']]}
        if ctx.policy == 'cas':
            files = {name: f for name, f in st['placement'].items()
                     if name in st['hot_parts'][self.p(ctx)['replica']]}
            out['branch_population'] = population(st['events'], files)
            out['blob_files'] = {p: sorted(fn for fn, key in f.items() if key) for p, f in files.items()}
        else:
            out['requests_per_part'] = self.requests_per_part(ctx, rows)
            out['put_bytes_minus_moved_bytes'] = out['put_bytes'] - out['moved_bytes']
            layout = {part: files for part, files in st.get('layout', {}).items()}
            out['layout_bytes'] = sum(size for files in layout.values() for _, size in files.values())
            out['put_bytes_minus_layout_bytes'] = out['put_bytes'] - out['layout_bytes']
            out['layout_file_sizes'] = {part: {f: size for f, (_, size) in sorted(files.items())}
                                        for part, files in sorted(layout.items())}
            moved = [p for p in st['parts_after'][self.p(ctx)['replica']] if p['active']]
            kinds = {f'{p["table"]}/{p["name"]}': p['part_type'].lower() for p in moved}
            out['object_writes_per_part'] = by_kind(out['requests_per_part'], kinds, 'PutObject')
            out['load_gets_per_part'] = by_kind(out['requests_per_part'], kinds, 'GetObject')
        return out

    def requests_per_part(self, ctx, rows: list) -> dict:
        """part -> op -> attempts of the workload replica, by the cold layout's keys; other keys count as `unlisted`."""
        names = {key: part for part, files in self.state(ctx).get('layout', {}).items() for key, _ in files.values()}
        out = {}
        for r in rows:
            if r['caller'] != self.p(ctx)['replica']:
                continue
            part = names.get(r['key'], 'unlisted') if r['key'] else 'batch'
            ops = out.setdefault(part, {})
            ops[r['op']] = ops.get(r['op'], 0) + 1
        return {p: dict(sorted(v.items())) for p, v in sorted(out.items())}

    def assertions(self, ctx, draft):
        pred, obs, st = self.predicted(ctx), draft['observations'], self.state(ctx)
        out = compare_ledger(pred, draft['ledger']['foreground'], exact=True)
        for key, want in pred['expected']['observations'].items():
            out[f'observation.{key}'] = check_observation(want, obs.get(key), obs)
        rows = sum(self.inserted_rows(ctx).values())
        for n, actual in enumerate(st.get('rows', []), 1):
            out[f'rows.r{n}'] = (rows, actual)
        types = self.expected_part_types(ctx)
        for name, rep in obs['replicas'].items():
            out[f'moved.{name}.part_types'] = (types, rep['moved_part_types'])
            out[f'moved.{name}.disk'] = ([ctx.policy], rep['moved_disks'])
            out[f'moved.{name}.move_part_events'] = (len(types), rep['move_part_events'])
            out[f'moved.{name}.no_fetch'] = (0, rep['download_part_events'])
        if ctx.policy == 'cas':
            out['window.foreground_crossing'] = ([], sorted(set(st['split']['foreground'])
                                                            & set(draft['window']['crossing'])))
            out.update(self.cas_graph_assertions(ctx, draft, pred))
        else:
            out['window.crossing'] = ([], draft['window']['crossing'])
        return out

    def cas_graph_assertions(self, ctx, draft, pred) -> dict:
        obs = draft['observations']
        pop = obs['branch_population']
        wide = sum(1 for _, kind in obs['moved_part_types'] if kind == 'Wide')
        bounds = cas_graph_bounds(pred['inputs']['cas_graph'], obs['moved_parts'], wide, pop['fresh'], pop['hash_hit'])
        fg = draft['ledger']['foreground'].get(self.p(ctx)['replica'], {})
        out = {}
        for op, (lo, hi) in sorted(bounds.items()):
            got = fg.get(op, {}).get('attempts', 0)
            text = f'{lo}..{hi}'
            out[f'cas_graph.{op}'] = (text, text if lo <= got <= hi else got)
        out['cas_graph.no_adoption_or_resurrect'] = ([0, 0], [pop['adoption'], pop['resurrect']])
        return out


@register
class MoveTtlReplicated(MoveTtl):
    """The same move on two replicas: inserts on `r1`, `r2` fetches onto its hot volume before the window."""

    name = 'move-replicated'
    policies = ('s3',)
    replicas = 2

    def ddl(self, ctx, table):
        engine = f"ENGINE = ReplicatedMergeTree('/yardstick/{self.name}/{table['name']}', '{{replica}}')"
        return super().ddl(ctx, table).replace('ENGINE = MergeTree', engine, 1)
