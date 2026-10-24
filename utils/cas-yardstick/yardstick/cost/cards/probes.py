"""Phase-0 acceptance probes (design §5.3); the CAS probe lives in `cas_probe.py`, the estimate's samples in `samples.py`.

Manifests in the phase-0 report directory (`experiments/<experiment_id>.json`) carry every parameter. The
expected request counts live in `predictions/<experiment_id>.<policy>.json`; their `source_refs` cite the
pinned source that explains each number, so no count is restated here.
"""
from __future__ import annotations

import json
import math
import os
import time

from .. import ledger as L
from ..localio import Tracer, parse_strace, path_filter
from . import CARDS
from .base import DATABASE, POLL_S, CostCard

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))))
REPORT_DIR = os.path.join(REPO, 'docs', 'superpowers', 'reports', '2026-10-24-storage-cost-phase0')
GIB, MIB = 1 << 30, 1 << 20
HOURS_PER_MONTH = 730  # the AWS price-list month; storage prices are per GB-month
CREATE_OPS = ('PutObject', 'CopyObject', 'CompleteMultipartUpload')
WRITE_SYSCALLS = ('write', 'pwrite64', 'writev')
IO_SYSCALLS = WRITE_SYSCALLS + ('read', 'pread64', 'readv')
# Cell inputs (`observations.inputs`, registry quantities): upstream-ok requests per op `probe_<n>_requests`,
# 4XX responses `probe_<n>_error_requests`, and storage `probe_<category>_gib_months`.
REQUEST_INPUTS = {'PutObject': 'put', 'GetObject': 'get', 'HeadObject': 'head', 'ListObjectsV2': 'list',
                  'DeleteObject': 'delete', 'DeleteObjects': 'batch_delete'}
ERROR_INPUTS = {'HeadObject': 'head', 'GetObject': 'get'}

# --- arithmetic the predictions rest on ------------------------------------------------------

def residue_band(payload_bytes: int, max_block: int = 1 << 20, min_block: int = 1 << 16, frame: int = 25) -> tuple:
    """(lower, upper) bytes of a CODEC(NONE) column file holding `payload_bytes`.

    Each compressed block adds a 9-byte header and a 16-byte checksum. A block holds at most `max_block`
    bytes; one is cut early only at a granule boundary past `min_block`, which bounds the block count.
    """
    return (payload_bytes + frame * math.ceil(payload_bytes / max_block),
            payload_bytes + frame * math.ceil(payload_bytes / min_block))


def multipart_parts(size: int, first: int = 32 << 20, rest: int = 16 << 20) -> list:
    """Part sizes `WriteBufferFromS3` uploads for one object; `[]` means a single PutObject.

    The first buffer is max_single_part_upload_size, later ones min_upload_part_size (the 500-part growth
    step is out of range here). One full buffer plus a remainder starts the multipart upload.
    """
    if size <= first:
        return []
    parts, left = [first], size - first
    while left > 0:
        parts.append(min(rest, left))
        left -= parts[-1]
    return parts


def gib_months(byte_hours: float) -> float:
    return byte_hours / GIB / HOURS_PER_MONTH


def integrated(draft: dict) -> dict:
    """Workload usage per caller and op: foreground plus maintenance, which the cells price together."""
    out: dict = {}
    for part in ('foreground', 'maintenance'):
        for caller, ops in (draft['ledger'].get(part) or {}).items():
            for op, c in ops.items():
                t = out.setdefault(caller, {}).setdefault(op, dict.fromkeys(L.COUNT_FIELDS, 0))
                for k in t:
                    t[k] += c.get(k, 0)
    return out


def request_inputs(usage: dict) -> dict:
    """Per op, summed over callers: upstream-ok requests whose reply arrived, and HEAD/GET 4XX responses."""
    def total(op, field):
        return sum(ops.get(op, {}).get(field, 0) for ops in usage.values())
    out = {f'probe_{n}_requests': total(op, 'upstream_ok') - total(op, 'accepted_dropped')
           for op, n in REQUEST_INPUTS.items()}
    out.update({f'probe_{n}_error_requests': total(op, 'upstream_4xx') for op, n in ERROR_INPUTS.items()})
    return out


def storage_inputs(draft: dict) -> dict:
    """Upper byte-hour bound of every category that held bytes, in GiB-months: the drill flags any it misses."""
    return {f'probe_{cat}_gib_months': gib_months(b['upper'])
            for cat, b in draft['inventory']['byte_hours'].items() if b['upper'] > 0}


def category(draft: dict, name: str) -> dict:
    """Byte-hour bounds of one category; a category no byte ever entered is absent from the record."""
    return draft['inventory']['byte_hours'].get(name, {'lower': 0.0, 'upper': 0.0})


# --- shared helpers ---------------------------------------------------------------------------

def prediction(experiment_id: str, policy: str, report_dir: str | None = None) -> dict:
    with open(os.path.join(report_dir or REPORT_DIR, 'predictions', f'{experiment_id}.{policy}.json')) as f:
        return json.load(f)


def compare_ledger(pred: dict, foreground: dict, exact: bool) -> dict:
    """Assertions `ledger.<caller>.<op>` against the prediction; deterministic cards also forbid unpredicted ops.

    A range that holds reports its own text as the actual value, so `expected == actual` reads as "inside".
    """
    out, tol = {}, pred['tolerance']
    for caller, ops in pred['expected']['foreground'].items():
        for op, want in ops.items():
            got = foreground.get(caller, {}).get(op, {}).get('attempts', 0)
            lo, hi = (want, want) if isinstance(want, int) else (want['min'], want['max'])
            slack = 0 if exact else tol['abs'].get(op, 0) + tol['rel'].get(op, 0) * hi
            text = want if isinstance(want, int) else f'{lo}..{hi}'
            out[f'ledger.{caller}.{op}'] = (text, text if lo - slack <= got <= hi + slack else got)
    if exact:
        extra = sorted(f'{c}.{op}' for c, ops in foreground.items() for op, n in ops.items()
                       if n['attempts'] and op not in pred['expected']['foreground'].get(c, {}))
        out['ledger.unpredicted'] = ([], extra)
    return out


def window_rows(ctx) -> list:
    ctx.proxy.flush()
    start = ctx.window()['start_seq']
    return [r for r in L.read_rows(ctx.proxy.ledger_path) if r.get('kind') == 'attempt' and r['seq'] > start]


def _sql(value) -> str:
    return str(value) if isinstance(value, (int, float)) else "'" + str(value).replace("'", "\\'") + "'"


def _count(client, sql: str) -> int:
    return int(client.query(sql).strip() or 0)


class ManifestCard(CostCard):
    """A card whose DDL, generator, settings and controls all come from its manifest."""

    table = 't'
    replicated = False

    def p(self, ctx) -> dict:
        return ctx.manifest['params']

    def controls(self, ctx):
        return dict(ctx.manifest['controls'])

    def settings(self, ctx):
        return {k: dict(v) for k, v in ctx.manifest['settings'].items()}

    def tables(self, ctx):
        return [self.table]

    def clients(self, ctx) -> list:
        # The topology may run more servers than the card needs; only the card's replicas hold its tables.
        return ctx.cluster.clients[:self.replicas]

    def state(self, ctx) -> dict:
        if not hasattr(self, '_state'):
            self._state = {}
        return self._state.setdefault(ctx.pass_dir, {})

    def engine(self, ctx) -> str:
        table = {'storage_policy': ctx.policy, 'primary_key_lazy_load': 0, 'prewarm_mark_cache': 0,
                 'prewarm_primary_key_cache': 0, **ctx.manifest['settings']['table']}
        engine = (f"ReplicatedMergeTree('/yardstick/{self.table}', '{{replica}}')" if self.replicated
                  else 'MergeTree')
        return (f"ENGINE = {engine} ORDER BY {self.p(ctx).get('order_by', 'k')} SETTINGS "
                + ', '.join(f'{k} = {_sql(v)}' for k, v in table.items()))

    def setup(self, ctx):
        for client in self.clients(ctx):
            client.query(f'CREATE DATABASE IF NOT EXISTS {DATABASE}')
            client.query(f'CREATE TABLE {DATABASE}.{self.table} ({self.p(ctx)["columns"]}) {self.engine(ctx)}')

    def insert(self, ctx, i: int) -> None:
        sql = self.p(ctx)['insert'].format(table=f'{DATABASE}.{self.table}', i=i, rows=self.p(ctx)['rows'])
        self.clients(ctx)[0].query(sql, settings=ctx.manifest['settings']['query'])

    def measured(self, ctx):
        n = self.p(ctx).get('inserts', 1)
        for i in range(n):
            self.insert(ctx, i)
        return {'unit_count': n, 'effective_counts': {'inserts': n, 'rows': n * self.p(ctx)['rows']},
                'observations': {}}

    def complete(self, ctx, timeout_s=300):
        clients = self.clients(ctx)
        if self.replicated:
            for client in clients:
                client.query(f'SYSTEM SYNC REPLICA {DATABASE}.{self.table} STRICT')
        where = f"database = '{DATABASE}' AND table = '{self.table}'"
        deadline = time.monotonic() + timeout_s
        while any(_count(c, f'SELECT (SELECT count() FROM system.merges WHERE {where}) + (SELECT count() FROM '
                              f'system.mutations WHERE is_done = 0 AND {where}) + (SELECT count() FROM '
                              f'system.replication_queue WHERE {where})') for c in clients):
            if time.monotonic() >= deadline:
                raise TimeoutError(f'{self.name}: merges, mutations or the queue not drained in {timeout_s} s')
            time.sleep(POLL_S)
        self.state(ctx)['rows'] = [_count(c, f'SELECT count() FROM {DATABASE}.{self.table}') for c in clients]
        for client in ctx.cluster.clients:
            client.query('SYSTEM FLUSH LOGS')

    def observe(self, ctx, draft):
        return {'inputs': {**request_inputs(integrated(draft)), **storage_inputs(draft)}}

    def predicted(self, ctx) -> dict:
        return prediction(ctx.manifest['experiment_id'], ctx.policy, self.p(ctx).get('report_dir'))

    def assertions(self, ctx, draft):
        out = compare_ledger(self.predicted(ctx), draft['ledger']['foreground'], self.mode == 'deterministic')
        rows = self.p(ctx).get('expected_rows', self.p(ctx)['rows'] * self.p(ctx).get('inserts', 1))
        for n, actual in enumerate(self.state(ctx).get('rows', []), 1):
            out[f'rows.r{n}'] = (rows, actual)
        if self.mode == 'deterministic':
            out['window.crossing'] = ([], draft['window']['crossing'])
        return out


def register(cls):
    CARDS[cls.name] = cls()
    return cls


def active_by_creator(card, ctx, draft) -> dict:
    """Upper `active` byte-hours in the window per creating caller, in GiB-months; they sum to the record's bound
    because each identity's bound is taken on its own. The card's reclassification applies, as in the record."""
    bounds, w = ctx.replay(), draft['window']
    card.reclassify(ctx, bounds)
    out = {}
    for caller in sorted(set(bounds.creators.values())):
        sub = L.Bounds(bounds.lower, bounds.upper, {i: m for i, m in bounds.mutations.items()
                                                    if bounds.creators.get(i) == caller})
        sub.cats = bounds.cats
        active = L.byte_hours(sub, w['t_start_us'], w['t_end_us']).get('active', {'upper': 0.0})
        out[caller] = gib_months(active['upper'])
    return out


# --- the five probes --------------------------------------------------------------------------

@register
class S3Insert(ManifestCard):
    name = 'probe_s3_insert'
    policies = ('s3',)
    replicated = True


@register
class ReplicatedFetch(ManifestCard):
    name = 'probe_replicated_fetch'
    policies = ('s3',)
    replicas = 2
    replicated = True

    def observe(self, ctx, draft):
        # Two cells: `inserts` (r1 writes the part) and `recovery` (r1 serves it, r2 fetches and loads it), sharing
        # the storage tier. Storage splits by the caller that created each object; interserver bytes count in both
        # directions (`from_target`: what a replica's interserver server sent; `to_target`: what it received).
        out = super().observe(ctx, draft)
        usage = integrated(draft)
        relays = draft['interserver'].values()
        out['inputs'].update(
            probe_r2_get_requests=request_inputs({'r2': usage.get('r2', {})})['probe_get_requests'],
            probe_interserver_gib=sum(r['from_target'] for r in relays) / GIB,
            probe_interserver_to_gib=sum(r['to_target'] for r in relays) / GIB,
            **{f'probe_{c}_active_gib_months': v for c, v in active_by_creator(self, ctx, draft).items()})
        return out

    def assertions(self, ctx, draft):
        out = super().assertions(ctx, draft)
        r1, r2 = ctx.cluster.clients[:2]
        fg, relays = draft['ledger']['foreground'], draft['interserver']
        out['r2.PutObject>=1'] = (True, fg.get('r2', {}).get('PutObject', {}).get('attempts', 0) >= 1)
        # relay0 fronts r1's interserver port: what r1 sent to the fetching r2. Nobody fetches from r2.
        out['relay0.from_target>0'] = (True, relays['relay0']['from_target'] > 0)
        out['relay1.from_target'] = (0, relays['relay1']['from_target'])
        out['ReplicatedPartFetches.r1'] = (0, draft['events']['r1'].get('ReplicatedPartFetches', 0))
        out['ReplicatedPartFetches.r2'] = (1, draft['events']['r2'].get('ReplicatedPartFetches', 0))
        downloads = (f"SELECT count() FROM system.part_log WHERE event_type = 'DownloadPart' "
                     f"AND database = '{DATABASE}' AND table = '{self.table}'")
        out['part_log.r2.DownloadPart'] = (1, _count(r2, downloads))
        return out


@register
class MultipartFault(ManifestCard):
    name = 'probe_multipart_fault'
    policies = ('s3',)
    expected_exceptions = (RuntimeError,)

    def measured(self, ctx):
        for rule in self.p(ctx)['faults']:
            ctx.proxy.set_fault(rule)
        return super().measured(ctx)

    def complete(self, ctx, timeout_s=300):
        super().complete(ctx, timeout_s)
        rows = [r for r in window_rows(ctx) if r['caller'] == 'r1']
        st, bounds = self.state(ctx), ctx.replay()
        created = [r for r in rows if r['op'] == 'CreateMultipartUpload' and r['upstream']['outcome'] == 'ok']
        st['uploads'] = [(r['bucket'], r['key'], r['response_body']['upload_id']) for r in created]
        ids = {u[2] for u in st['uploads']}
        parts = [r for r in rows if r['op'] == 'UploadPart' and r.get('upload_id') in ids]
        two = [r for r in parts if r['part_number'] == 2]
        st['part_identities'] = len({(r['key'], r['upload_id'], r['part_number']) for r in parts})
        st['part2'] = {'attempts': len(two), 'sha256': sorted({r['payload_sha256'] for r in two}),
                       'correlated': any({r['seq'] for r in two} <= set(g) for g in L.correlate_retries(parts))}
        st['residue'] = {name: sum(p['size'] for ident, p in inv.incomplete.items() if ident[2] in ids)
                         for name, inv in (('lower', bounds.lower), ('upper', bounds.upper))}
        st['object_created'] = any((b, k) in inv.objects for b, k, _ in st['uploads']
                                   for inv in (bounds.lower, bounds.upper))

    def teardown(self, ctx):
        # The rig aborts the upload ClickHouse could not abort, as an observer through the proxy's rig listener:
        # the abort is replayed, not billed.
        for bucket, key, upload_id in self.state(ctx).get('uploads', []):
            ctx.rig.abort_multipart(bucket, key, upload_id)
        super().teardown(ctx)

    def observe(self, ctx, draft):
        after = ctx.replay()
        ids = {u[2] for u in self.state(ctx).get('uploads', [])}
        self.state(ctx)['after_abort'] = max(sum(p['size'] for ident, p in inv.incomplete.items() if ident[2] in ids)
                                             for inv in (after.lower, after.upper))
        return {**super().observe(ctx, draft), 'multipart': {k: v for k, v in self.state(ctx).items() if k != 'rows'}}

    def assertions(self, ctx, draft):
        out = super().assertions(ctx, draft)
        st, inputs = self.state(ctx), self.predicted(ctx)['inputs']
        lo, hi = inputs['residue_bytes_band']
        fg = draft['ledger']['foreground'].get('r1', {})
        bh = category(draft, 'incomplete')
        out['insert_failed'] = (True, bool(draft['observations'].get('expected_exceptions')))
        out['CreateMultipartUpload'] = (1, fg.get('CreateMultipartUpload', {}).get('attempts', 0))
        out['UploadPart>=2'] = (True, fg.get('UploadPart', {}).get('attempts', 0) >= 2)
        out['part_identities'] = (inputs['part_identities'], st['part_identities'])
        out['part2.attempts'] = (2, st['part2']['attempts'])
        out['part2.same_payload'] = (1, len(st['part2']['sha256']))
        out['part2.correlated'] = (True, st['part2']['correlated'])
        for name in ('lower', 'upper'):
            out[f'residue.{name}.in_band'] = (True, lo <= st['residue'][name] <= hi)
        out['no_object_created'] = (False, st['object_created'])
        out['incomplete_after_abort'] = (0, st['after_abort'])
        out['incomplete_byte_hours>0'] = (True, bh['lower'] > 0)
        return out


@register
class LocalVolume(ManifestCard):
    name = 'probe_local_volume'
    policies = ('local',)
    trace = True

    def data_dir_io(self, ctx, draft) -> dict:
        """Data-directory calls inside the window, over every server generation's trace."""
        w = draft['window']
        return parse_strace(Tracer.paths(ctx.pass_dir, 1), path_filter([ctx.local_dirs[0]]),
                            window=(w['t_start_us'] / 1e6, w['t_end_us'] / 1e6))

    def observe(self, ctx, draft):
        traced = self.data_dir_io(ctx, draft)
        io = traced['syscalls']
        window_s = (draft['window']['t_end_us'] - draft['window']['t_start_us']) / 1e6
        calls = sum(io.get(s, {}).get('count', 0) for s in IO_SYSCALLS)
        nbytes = sum(io.get(s, {}).get('bytes', 0) for s in IO_SYSCALLS)
        out = super().observe(ctx, draft)
        # Window averages of syscall demand: page cache and writeback sit between these and the device.
        out['inputs'].update(probe_block_capacity_gib=draft['local']['r1']['peak_allocated_bytes'] / GIB,
                             probe_block_iops=calls / window_s, probe_block_mibps=nbytes / MIB / window_s)
        return {**out, 'data_dir_io': io, 'data_dir_crossing': len(traced['crossing']),
                'data_dir_outside': traced['outside'],
                'data_dir_write_syscalls': sorted(s for s in WRITE_SYSCALLS if io.get(s, {}).get('count')),
                'window_s': window_s}

    def assertions(self, ctx, draft):
        out = super().assertions(ctx, draft)
        local, io = draft['local']['r1'], draft['observations']['data_dir_io']
        syscall = self.predicted(ctx)['inputs']['write_syscall']
        out['proc_io.write_bytes>0'] = (True, draft['proc_io']['r1']['write_bytes'] > 0)
        out[f'data_dir.{syscall}.count>0'] = (True, io.get(syscall, {}).get('count', 0) > 0)
        out[f'data_dir.{syscall}.histogram'] = (True, bool(io.get(syscall, {}).get('size_histogram')))
        out['union_peak>0'] = (True, local['peak_allocated_bytes'] > 0)
        out['device_fs_recorded'] = (True, bool(local['device']) and bool(local['fs']))
        out['limitations_recorded'] = (True, bool(local['limitations']))
        return out
