"""CAS cards (R4): entry branches of a part write, GC rounds and reclamation, and the mount lease.

Every parameter comes from the manifest (`experiments/<experiment_id>.json` of the CAS report); expected values and
acceptance bounds come from `predictions/<experiment_id>.<policy>.json` with `source_refs` into the pinned source.

Request origin follows `cas_probe.attribute`: a request inside one of the caller's own GC rounds
(`system.cas_gc_log` Start..Finish of one `round_id`) or on a control family flagged `maintenance` (lease, GC
heartbeat, GC state, pool metadata, loose roots) is maintenance; the rest is foreground. Lease renewals run on the
server's own schedule even with GC off, so the cards are `stochastic` in the runner's sense (a renewal may cross a
window boundary) while the foreground counts are still compared to the exact integer (`compare_ledger(exact=True)`).
"""
from __future__ import annotations

import json
import math
import time

from .. import ledger as L
from .base import DATABASE, POLL_S
from .cas_probe import CAS_FAMILIES, attribute, cohort, deletions, gc_rounds
from .lifecycle import LifecycleCard, NaturalMergeCard, check_observation
from .probes import CREATE_OPS, _count, compare_ledger, register, window_rows

DISK = 'cas'
# Branch evidence fields of `system.cas_log` (Pool/CasPartWriteTxn.cpp): a fresh blob is `blob_put` with
# `detail['publication_reason'] = 'absent'` (:528-536); a hash hit is `blob_reuse_adopt` with outcome `observed`
# after the mandatory HEAD found a Clean body (:441-450); an adopted blob of a hardlinked file is `blob_reuse_adopt`
# with outcome `adopt`, reason `manifest-trust`, emitted at promote (:941-949). Inline entries have no blob event:
# their placement (an empty remote path in `system.remote_data_paths`, ContentAddressedMetadataStorage.cpp:2003)
# is the evidence.
BRANCH_SQL = ("SELECT event_type, outcome, reason, object_kind, object_hash, ref_name, detail['action'] AS action, "
              "detail['publication_reason'] AS publication_reason, toUnixTimestamp64Micro(event_time_microseconds) "
              "AS t_us FROM system.cas_log WHERE disk_name = '{disk}' AND event_time_microseconds >= "
              "fromUnixTimestamp64Micro({t0}) AND event_type IN ('blob_put', 'blob_reuse_adopt', 'blob_reuse_resurrect', "
              "'manifest_put', 'build_publish', 'ref_move', 'ref_drop', 'precommit') ORDER BY t_us FORMAT JSONEachRow")
PLACEMENT_SQL = ("SELECT local_path, remote_path, size FROM system.remote_data_paths WHERE disk_name = '{disk}' "
                 "FORMAT JSONEachRow")
GC_FINISH_SQL = ("SELECT round_id, trigger, outcome, round, duration_ms, candidates_marked, objects_deleted, "
                 "manifests_deleted, entries_condemned, entries_graduated, entries_redeleted, "
                 "toUnixTimestamp64Micro(event_time_microseconds) AS t_us FROM system.cas_gc_log WHERE "
                 "event_type = 'Finish' AND event_time_microseconds >= fromUnixTimestamp64Micro({t0}) "
                 "ORDER BY t_us FORMAT JSONEachRow")
GC_START_SQL = ("SELECT round_id, toUnixTimestamp64Micro(event_time_microseconds) AS t_us FROM system.cas_gc_log "
                "WHERE event_type = 'Start' AND event_time_microseconds >= fromUnixTimestamp64Micro({t0}) "
                "ORDER BY t_us FORMAT JSONEachRow")
BRANCHES = ('fresh', 'hash_hit', 'adoption', 'inline')
# A lease renewal is the renewer's own write (Pool/CasMountRuntime.cpp:690) even when it lands inside a GC round.
RENEWAL = ('lease', 'PutObject')


def is_renewal(a) -> bool:
    family = L.family_of(CAS_FAMILIES, a.get('key'))
    return bool(family) and (family['name'], a['op']) == RENEWAL


def json_rows(client, sql: str) -> list:
    return [json.loads(line) for line in client.query(sql).splitlines() if line.strip()]


def branch_of(event: dict):
    """The entry branch one `cas_log` blob event evidences, or None for a non-branch event."""
    if event['event_type'] == 'blob_put':
        return 'fresh' if event['publication_reason'] == 'absent' else 'resurrect'
    if event['event_type'] == 'blob_reuse_adopt':
        if event['outcome'] == 'observed':
            return 'hash_hit'
        if event['outcome'] == 'adopt' and event['reason'] == 'manifest-trust':
            return 'adoption'
    if event['event_type'] == 'blob_reuse_resurrect':
        return 'resurrect'
    return None


def part_files(placement_rows: list, table_marker: str = '') -> dict:
    """part name -> file name -> blob key, or '' for an inline entry; from `system.remote_data_paths` rows."""
    out: dict = {}
    for r in placement_rows:
        parts = r['local_path'].rstrip('/').split('/')
        if len(parts) < 2 or (table_marker and table_marker not in r['local_path']):
            continue
        out.setdefault(parts[-2], {})[parts[-1]] = r['remote_path'].split('/blobs/', 1)[-1] if r['remote_path'] else ''
    return out


def population(events: list, files: dict) -> dict:
    """Branch population of the parts in `files`: blob events by branch, inline entries by placement."""
    out = dict.fromkeys(BRANCHES, 0)
    out['resurrect'] = 0
    for e in events:
        b = branch_of(e)
        if b:
            out[b] += 1
    out['inline'] = sum(1 for part in files.values() for key in part.values() if key == '')
    return out


def by_family(rows: list, families: list) -> dict:
    """caller -> family -> op -> attempts; a key outside every family is `other`."""
    out: dict = {}
    for r in rows:
        f = L.family_of(families, r.get('key'))
        name = f['name'] if f else ('batch' if not r.get('key') else 'other')
        ops = out.setdefault(r['caller'], {}).setdefault(name, {})
        ops[r['op']] = ops.get(r['op'], 0) + 1
    return out


class ScratchPoller:
    """Peak bytes of the files under every `cas_scratch` directory of one replica, polled on a thread.

    The runner's directory sampler (0.5 s) misses a staging plateau shorter than its interval; the staged files of a
    part live from the part write to the end of its commit (ContentAddressedTransaction.cpp:984-995, :593), so a
    poll every few milliseconds sees the plateau.
    """

    def __init__(self, replica_dir: str, interval_s: float):
        import threading  # noqa: PLC0415 (only the staging case polls)
        self.replica_dir, self.interval_s, self.peak = replica_dir, interval_s, 0
        self._stop, self._thread = threading.Event(), threading.Thread(target=self._run, daemon=True)

    def _size(self) -> int:
        import os  # noqa: PLC0415
        total = 0
        for root, dirs, files in os.walk(self.replica_dir):
            if 'cas_scratch' not in root.split(os.sep):
                continue
            for f in files:
                try:
                    total += os.stat(os.path.join(root, f)).st_size
                except FileNotFoundError:
                    pass  # removed between the listing and the stat: no longer staged
        return total

    def _run(self):
        while not self._stop.is_set():
            self.peak = max(self.peak, self._size())
            self._stop.wait(self.interval_s)

    def start(self):
        self._thread.start()

    def stop(self) -> int:
        self._stop.set()
        self._thread.join()
        return max(self.peak, self._size())


def pool_prefix(ctx) -> str:
    """The pass's CAS pool prefix in the bucket: `<run_id>/<policy>/<card>/<case>/k<n>/shared/` (runner key layout)."""
    return '/'.join(ctx.pass_dir.rstrip('/').split('/')[-5:]) + '/shared/'


class CasCard(LifecycleCard):
    """A lifecycle card on the CAS disk with GC-round attribution and per-family request observations."""

    policies = ('cas', 'cas_cached')
    mode = 'stochastic'

    def inventory_families(self, ctx):
        return CAS_FAMILIES

    def collect_rounds(self, ctx) -> None:
        rounds = {}
        for n, client in enumerate(ctx.cluster.clients, 1):
            exists = _count(client, "SELECT count() FROM system.tables WHERE database = 'system' AND name = 'cas_gc_log'")
            rounds[f'r{n}'] = gc_rounds(client) if exists else []
        self.state(ctx)['gc_rounds'] = rounds

    def classify(self, ctx, attempts, families):
        st = self.state(ctx)
        if 'gc_rounds' not in st:
            raise RuntimeError('GC rounds were not collected; request origin cannot be attributed')
        split = attribute(attempts, st['gc_rounds'], families)
        st['split'] = {part: [a['seq'] for a in rows] for part, rows in split.items()}
        st['split_families'] = {part: by_family(rows, families) for part, rows in split.items()}
        return split

    def placement(self, ctx) -> dict:
        """part -> file -> blob key ('' for inline) of this table, matched by the table's UUID in the local path.

        File sizes go to `state['placement_sizes']` (part -> file -> bytes).
        """
        client = self.replica_client(ctx)
        uuid = client.query(f"SELECT toString(uuid) FROM system.tables WHERE database = '{DATABASE}' "
                            f"AND name = '{self.table}'").strip()
        if not uuid:
            raise RuntimeError(f'{self.name}: table {DATABASE}.{self.table} has no UUID; placement cannot be matched')
        rows = [r for r in json_rows(client, PLACEMENT_SQL.format(disk=DISK)) if f'/{uuid}/' in r['local_path']]
        sizes = self.state(ctx).setdefault('placement_sizes', {})
        for r in rows:
            parts = r['local_path'].rstrip('/').split('/')
            sizes.setdefault(parts[-2], {})[parts[-1]] = int(r['size'])
        return part_files(rows, f'/{uuid}/')

    def gc_finishes(self, ctx) -> list:
        return json_rows(self.replica_client(ctx), GC_FINISH_SQL.format(t0=ctx.window()['t_start_us']))

    def observe(self, ctx, draft):
        st = self.state(ctx)
        return {'request_attribution': {'method': 'gc_round_windows+control_families',
                                        'rounds': {r: len(v) for r, v in st['gc_rounds'].items()}},
                'foreground_by_family': st['split_families']['foreground'].get(self.p(ctx)['replica'], {}),
                'maintenance_by_family': st['split_families']['maintenance'].get(self.p(ctx)['replica'], {})}

    def assertions(self, ctx, draft):
        pred = self.predicted(ctx)
        # Exact per op per caller: foreground excludes lease, heartbeat and round traffic by evidenced attribution.
        out = compare_ledger(pred, draft['ledger']['foreground'], exact=pred['inputs'].get('exact', True))
        for key, want in pred['expected']['observations'].items():
            out[f'observation.{key}'] = check_observation(want, draft['observations'].get(key), draft['observations'])
        rows = self.p(ctx).get('expected_rows', self.inserts(ctx) * self.p(ctx)['rows'])
        out['rows.' + self.p(ctx)['replica']] = (rows, self.state(ctx).get('rows', [None])[0])
        # Only maintenance may cross a window boundary: a foreground request that does would make the count unsure.
        fg = set(self.state(ctx)['split']['foreground'])
        out['window.foreground_crossing'] = ([], sorted(fg & set(draft['window']['crossing'])))
        return out


@register
class CasBranches(CasCard):
    """Inserts (then optional `params.steps`, such as a mutation); observes the entry branch of every part file.

    `params.parts_new` names how the observed parts are chosen: `active` (the table's active parts at the end) or
    `all` (every part the window created, outdated ones included).
    """

    name = 'cas-branches'

    def measured(self, ctx):
        poll_ms = self.p(ctx).get('scratch_poll_ms')
        poller = ScratchPoller(ctx.cluster.run_dir(1), poll_ms / 1000) if poll_ms else None
        if poller:
            poller.start()
        try:
            result = super().measured(ctx)
            for step in self.p(ctx).get('steps', []):
                self.replica_client(ctx).query(self.sql(ctx, step))
        finally:
            if poller:
                self.state(ctx)['scratch_peak_bytes'] = poller.stop()
        return result

    def complete(self, ctx, timeout_s=300):
        st, client = self.state(ctx), self.replica_client(ctx)
        if self.p(ctx).get('dropped'):
            # The table is gone: nothing to drain, no parts or placement to read.
            for c in ctx.cluster.clients:
                c.query('SYSTEM FLUSH LOGS')
            st.update(rows=[0], parts=[], placement={},
                      events=json_rows(client, BRANCH_SQL.format(disk=DISK, t0=ctx.window()['t_start_us'])))
            self.collect_rounds(ctx)
            return
        super().complete(ctx, timeout_s)
        st['parts'] = self.parts(ctx)
        st['events'] = json_rows(client, BRANCH_SQL.format(disk=DISK, t0=ctx.window()['t_start_us']))
        self.collect_rounds(ctx)

    def teardown(self, ctx):
        # system.remote_data_paths makes the server read manifests: read it after the window, before the drop, so its
        # requests are teardown traffic rather than workload.
        if not self.p(ctx).get('dropped'):
            self.state(ctx)['placement'] = self.placement(ctx)
        super().teardown(ctx)

    def observe(self, ctx, draft):
        st = self.state(ctx)
        want = {p['name'] for p in st['parts'] if p['active'] or self.p(ctx).get('parts_new') == 'all'}
        files = {name: f for name, f in st['placement'].items() if name in want}
        events = [e for e in st['events'] if branch_of(e)]
        hashes = {}
        for e in events:
            hashes.setdefault(branch_of(e), []).append(e['object_hash'])
        blob_files = {part: sorted(fn for fn, key in f.items() if key) for part, f in files.items()}
        local = (draft.get('local') or {}).get(self.p(ctx)['replica'], {})
        return {**super().observe(ctx, draft),
                'parts': st['parts'], 'placement': files, 'blob_files': blob_files,
                'inline_files': {part: sorted(fn for fn, key in f.items() if not key) for part, f in files.items()},
                'branch_population': population(events, files),
                'staged_bytes': sum(st.get('placement_sizes', {}).get(part, {}).get(fn, 0)
                                    for part, f in files.items() for fn, key in f.items() if key),
                'local_peak_bytes': local.get('peak_allocated_bytes'),
                'scratch_peak_bytes': st.get('scratch_peak_bytes'),
                'branch_hashes': {b: sorted(h) for b, h in hashes.items()},
                'manifest_puts': sum(1 for e in st['events'] if e['event_type'] == 'manifest_put'),
                'ref_events': {t: sum(1 for e in st['events'] if e['event_type'] == t)
                               for t in ('precommit', 'build_publish', 'ref_move', 'ref_drop')}}

    def assertions(self, ctx, draft):
        out = super().assertions(ctx, draft)
        obs, checks = draft['observations'], self.predicted(ctx)['inputs'].get('branch_checks', {})
        if 'adopted_columns' in checks:
            # Adoption evidence names hashes; the source part's placement maps them back to column files.
            source = obs['placement'].get(checks['adopted_columns']['source_part'], {})
            if not source:
                source = self.state(ctx)['placement'].get(checks['adopted_columns']['source_part'], {})
            # `cas_log` renders a blob id as `<algo>:<hex>` (blobIdOf); placement keys end in `<algo>/<2 hex>/<hex>`.
            by_hash = {key.rsplit('/', 1)[-1]: fn for fn, key in source.items() if key}
            adopted = sorted({by_hash.get(h.rsplit(':', 1)[-1], '?') for h in obs['branch_hashes'].get('adoption', [])})
            columns = sorted({fn.split('.', 1)[0] for fn in adopted if fn.endswith('.bin')})
            out['adoption.columns'] = (checks['adopted_columns']['columns'], columns)
            out['adoption.files_known'] = (True, '?' not in adopted)
        if checks.get('hash_hit_same_blob_keys'):
            # Hash hits: the second part's blob keys are exactly the first part's (decoded placement).
            parts = sorted(obs['placement'])
            keys = [sorted(v for v in obs['placement'][p].values() if v) for p in parts]
            out['hash_hit.same_blob_keys'] = (True, len(keys) == 2 and keys[0] == keys[1] and bool(keys[0]))
        return out


@register
class CasDrop(CasBranches):
    """Inserts, then `DROP TABLE … SYNC` with GC off: the foreground ref removal, exact per op."""

    name = 'cas-drop'


class GcCard(CasCard):
    """GC on in the server config; the card drives rounds with `SYSTEM CAS GC STOP/RUN/START` where it needs them."""

    def gc(self, ctx, verb: str) -> None:
        self.replica_client(ctx).query(f'SYSTEM CAS GC {verb} {DISK}')


@register
class GcLifecycle(GcCard):
    """Inserts, freezes the payload cohort, retires it (`params.retire`: `drop` or `merge`), waits for reclamation.

    Stages from `system.cas_gc_log` Finish rows and the ledger: `condemn` (first round after the retire with
    `entries_condemned > 0`), `manifest_delete` and `blob_delete` (last upstream-ok deletion of each kind of cohort
    key), `total` (the last of them). Delays are seconds from the retiring action, and rounds started after it.
    """

    name = 'gc-lifecycle'

    def measured(self, ctx):
        result = super().measured(ctx)
        st, client = self.state(ctx), self.replica_client(ctx)
        st['cohort'] = cohort(window_rows(ctx))
        st['t_retire_us'] = time.time_ns() // 1000
        if self.p(ctx)['retire'] == 'drop':
            client.query(f'DROP TABLE {DATABASE}.{self.table} SYNC')
        else:
            client.query(self.sql(ctx, self.p(ctx)['optimize']))
        return result

    def complete(self, ctx, timeout_s=300):
        st, client = self.state(ctx), self.replica_client(ctx)
        if self.p(ctx)['retire'] == 'merge':
            # The merge writes its result's blobs fresh; a source blob the result shared would stay referenced and show
            # as unreclaimed (assertion `cohort_reclaimed`), so the cohort needs no placement read in the window.
            super().complete(ctx, timeout_s)
        deadline = time.monotonic() + self.p(ctx)['reclaim_timeout_s']
        while True:
            st['deleted'] = deletions(window_rows(ctx), st['cohort'], st['t_retire_us'])
            if len(st['deleted']) == len(st['cohort']) or time.monotonic() >= deadline:
                break
            time.sleep(1)
        for c in ctx.cluster.clients:
            c.query('SYSTEM FLUSH LOGS')
        st['finishes'] = self.gc_finishes(ctx)
        st['starts'] = json_rows(client, GC_START_SQL.format(t0=ctx.window()['t_start_us']))
        st['rows'] = ([_count(client, f'SELECT count() FROM {DATABASE}.{self.table}')]
                      if self.p(ctx)['retire'] == 'merge' else [0])
        self.collect_rounds(ctx)

    def reclassify(self, ctx, bounds):
        st = self.state(ctx)
        if st.get('cohort'):
            bounds.reclassify(sorted(st['cohort']), 'garbage', st['t_retire_us'], f'retire by {self.p(ctx)["retire"]}')

    def observe(self, ctx, draft):
        st = self.state(ctx)
        t0 = st['t_retire_us']
        kinds = {'manifest_delete': [d['t_us'] for i, d in st['deleted'].items() if '/cas/manifests/' in i[1]],
                 'blob_delete': [d['t_us'] for i, d in st['deleted'].items() if '/blobs/' in i[1]]}
        condemn = next((f for f in st['finishes'] if int(f['t_us']) >= t0 and int(f['entries_condemned']) > 0), None)
        delays = {k: (max(v) - t0) / 1e6 if v else None for k, v in kinds.items()}
        delays['condemn'] = (int(condemn['t_us']) - t0) / 1e6 if condemn else None
        last = [d for d in (delays['manifest_delete'], delays['blob_delete']) if d is not None]
        delays['total'] = max(last) if last else None

        def rounds_until(t_us):
            return None if t_us is None else sum(1 for s in st['starts'] if t0 <= int(s['t_us']) <= t_us)
        stage_t = {k: (t0 + v * 1e6 if v is not None else None) for k, v in delays.items()}
        return {**super().observe(ctx, draft),
                'cohort_size': len(st['cohort']), 'cohort_reclaimed': len(st['deleted']),
                'cohort_manifests': sum(1 for i in st['cohort'] if '/cas/manifests/' in i[1]),
                'cohort_blobs': sum(1 for i in st['cohort'] if '/blobs/' in i[1]),
                'reclaim_delay_s': delays, 'reclaim_rounds': {k: rounds_until(t) for k, t in stage_t.items()},
                **{f'reclaim_rounds.{k}': rounds_until(t) for k, t in stage_t.items()},
                # Delays in round periods (interval + mean round duration), so fits at one interval serve another.
                **{f'reclaim_periods.{k}': (None if v is None else v / self.round_period_s(ctx)) for k, v in delays.items()},
                'deleting_callers': sorted({d['caller'] for d in st['deleted'].values()}),
                'gc_interval_s': self.p(ctx)['gc_interval_s']}

    def round_period_s(self, ctx) -> float:
        """Seconds from one round's start to the next: the interval plus the mean round duration."""
        durations = [int(f['duration_ms']) for f in self.state(ctx)['finishes'] if 'duration_ms' in f]
        return self.p(ctx)['gc_interval_s'] + (sum(durations) / len(durations) / 1000 if durations else 0.0)

    def assertions(self, ctx, draft):
        out = super().assertions(ctx, draft)
        obs = draft['observations']
        out['cohort_nonempty'] = (True, obs['cohort_size'] > 0)
        out['cohort_reclaimed'] = (obs['cohort_size'], obs['cohort_reclaimed'])
        out['condemn_before_delete'] = (True, None not in (obs['reclaim_delay_s']['condemn'], obs['reclaim_delay_s']['total'])
                                        and obs['reclaim_delay_s']['condemn'] <= obs['reclaim_delay_s']['total'] + 1e-6)
        garbage = draft['inventory']['byte_hours'].get('garbage', {'lower': 0.0})
        out['garbage_byte_hours>0'] = (True, garbage['lower'] > 0)
        return out


class PopulatedPool(GcCard):
    """Setup fills the pool with `params.parts` wide parts under `SYSTEM CAS GC STOP`, so no round folds during it.

    Each part has `params.columns_n` CODEC(NONE) `UInt64` columns of distinct values: one distinct `.bin` blob per
    column, one `.mrk2` blob shared by every column and part of the same row count, and one `primary.idx` blob.
    The pool's object count is listed by the rig (caller `rig`, excluded) once the setup is done.
    """

    def columns(self, ctx) -> str:
        n = self.p(ctx)['columns_n']
        return 'k UInt64, ' + ', '.join(f'c{i} UInt64 CODEC(NONE)' for i in range(n))

    def insert_sql(self, ctx, i: int) -> str:
        n, rows = self.p(ctx)['columns_n'], self.p(ctx)['rows']
        cols = ', '.join(f'k * {i + 2} + {j}' for j in range(n))
        return (f'INSERT INTO {DATABASE}.{self.table} SELECT number + {i} * {rows} AS k, {cols} '
                f'FROM numbers({rows})')

    def parts_n(self, ctx) -> int:
        return max(1, round(ctx.manifest['scale'] * self.p(ctx)['parts']))

    def setup(self, ctx):
        client = self.replica_client(ctx)
        self.gc(ctx, 'STOP')
        client.query(f'CREATE DATABASE IF NOT EXISTS {DATABASE}')
        client.query(f'CREATE TABLE {DATABASE}.{self.table} ({self.columns(ctx)}) {self.engine(ctx)}')
        for i in range(self.parts_n(ctx)):
            client.query(self.insert_sql(ctx, i), settings=ctx.manifest['settings']['query'])
        st = self.state(ctx)
        st['pool_objects'] = self.list_pool(ctx)

    def list_pool(self, ctx) -> dict:
        """Object count per family of the pass's pool, listed by the rig (its requests are caller `rig`)."""
        bucket = ctx.cluster.topology.bucket
        out: dict = {}
        for o in ctx.rig.list(bucket, pool_prefix(ctx)):
            f = L.family_of(CAS_FAMILIES, o['key'])
            name = f['name'] if f else 'other'
            out[name] = out.get(name, 0) + 1
        return out

    def objects(self, ctx) -> int:
        """Pool objects GC accounts for: blobs, their `.meta` siblings and part manifests."""
        po = self.state(ctx)['pool_objects']
        return sum(po.get(f, 0) for f in ('blob', 'blob_meta', 'manifest'))

    def inserts(self, ctx) -> int:
        return self.parts_n(ctx)

    def round_rows(self, ctx, draft) -> dict:
        """Per GC round in the window: attempts by family and op, inside the round's Start..Finish."""
        st = self.state(ctx)
        rows = {a['seq']: a for a in window_rows(ctx)}
        out = []
        for start, finish in st['gc_rounds'].get(self.p(ctx)['replica'], []):
            inside = [a for a in rows.values() if a['caller'] == self.p(ctx)['replica'] and not is_renewal(a)
                      and start <= a['t_start_us'] <= (math.inf if finish is None else finish)]
            out.append({'start_us': start, 'finish_us': finish, 'requests': len(inside),
                        'by_family': by_family(inside, CAS_FAMILIES).get(self.p(ctx)['replica'], {})})
        return out


@register
class GcScale(PopulatedPool):
    """Two manual rounds on a populated pool: the first folds every new edge, the second has nothing new.

    With the scheduler stopped no heartbeat worker runs; `SYSTEM CAS GC RUN` is a synchronous round that pulses the
    heartbeat once on acquiring the lease (Gc/CasGcScheduler.cpp:177-189, :330-338).
    """

    name = 'gc-scale'

    def measured(self, ctx):
        for _ in range(self.p(ctx)['manual_rounds']):
            self.gc(ctx, 'RUN')
        return {'unit_count': self.p(ctx)['manual_rounds'], 'effective_counts': {'rounds': self.p(ctx)['manual_rounds']},
                'observations': {}}

    def complete(self, ctx, timeout_s=300):
        for c in ctx.cluster.clients:
            c.query('SYSTEM FLUSH LOGS')
        st = self.state(ctx)
        st['finishes'] = self.gc_finishes(ctx)
        st['rows'] = [_count(self.replica_client(ctx), f'SELECT count() FROM {DATABASE}.{self.table}')]
        self.collect_rounds(ctx)

    def observe(self, ctx, draft):
        st = self.state(ctx)
        rounds = self.round_rows(ctx, draft)
        return {**super().observe(ctx, draft), 'pool_objects': st['pool_objects'], 'objects': self.objects(ctx),
                'parts': self.parts_n(ctx), 'rounds': rounds,
                'round_requests': [r['requests'] for r in rounds],
                'fold_round_requests': rounds[0]['requests'] if rounds else None,
                'deferred_round_requests': rounds[-1]['requests'] if len(rounds) > 1 else None,
                'round_duration_ms': [int(f['duration_ms']) for f in st['finishes']],
                'fold_round_duration_ms': int(st['finishes'][0]['duration_ms']) if st['finishes'] else None,
                'deferred_round_duration_ms': int(st['finishes'][-1]['duration_ms']) if len(st['finishes']) > 1 else None,
                'round_outcomes': [f['outcome'] for f in st['finishes']]}

    def assertions(self, ctx, draft):
        out = super().assertions(ctx, draft)
        obs = draft['observations']
        out['rounds'] = (self.p(ctx)['manual_rounds'], len(obs['rounds']))
        # The first round folds the setup's edges; every later one finds nothing new and defers (skip-unchanged,
        # Gc/CasGcScheduler.cpp:302-304, :413-416).
        out['round_outcomes'] = (['Success'] + ['Deferred'] * (self.p(ctx)['manual_rounds'] - 1), obs['round_outcomes'])
        return out


@register
class GcIdle(PopulatedPool):
    """A populated pool with nothing to reclaim: one manual round folds the setup's edges, then the scheduler runs
    `params.intervals` intervals on its own. Heartbeat rows (family `gc_hb`) and round rows (every other family
    inside a round) must both be non-zero and disjoint by key family (plan Review Focus 5)."""

    name = 'gc-idle'

    def setup(self, ctx):
        super().setup(ctx)
        self.gc(ctx, 'RUN')
        self.gc(ctx, 'START')

    def measured(self, ctx):
        time.sleep(self.p(ctx)['intervals'] * self.p(ctx)['gc_interval_s'] + self.p(ctx)['slack_s'])
        return {'unit_count': self.p(ctx)['intervals'], 'effective_counts': {}, 'observations': {}}

    def complete(self, ctx, timeout_s=300):
        for c in ctx.cluster.clients:
            c.query('SYSTEM FLUSH LOGS')
        st = self.state(ctx)
        st['finishes'] = self.gc_finishes(ctx)
        st['starts'] = json_rows(self.replica_client(ctx), GC_START_SQL.format(t0=ctx.window()['t_start_us']))
        st['rows'] = [_count(self.replica_client(ctx), f'SELECT count() FROM {DATABASE}.{self.table}')]
        self.collect_rounds(ctx)

    def observe(self, ctx, draft):
        st = self.state(ctx)
        rounds = self.round_rows(ctx, draft)
        replica = self.p(ctx)['replica']
        rows = [a for a in window_rows(ctx) if a['caller'] == replica and a['seq'] < draft['window']['end_seq']]
        fam = lambda a: (L.family_of(CAS_FAMILIES, a.get('key')) or {'name': 'batch'})['name']  # noqa: E731
        hb = [a for a in rows if fam(a) == 'gc_hb']
        in_round = {a['seq'] for a in rows if any(r['start_us'] <= a['t_start_us'] <= (r['finish_us'] or math.inf)
                                                  for r in rounds)}
        round_rows = [a for a in rows if a['seq'] in in_round and fam(a) != 'gc_hb' and not is_renewal(a)]
        # Outside every round only the heartbeat worker and the lease renewer run: any other maintenance family
        # there would mean round traffic leaked out of its window.
        outside = sorted({fam(a) for a in rows if a['seq'] not in in_round and fam(a) != 'lease'
                          and (L.is_maintenance(a, CAS_FAMILIES) or not a.get('key'))})
        window_s = (draft['window']['t_end_us'] - draft['window']['t_start_us']) / 1e6
        starts = [int(s['t_us']) for s in st['starts']]
        finishes = {f['round_id']: f for f in st['finishes']}
        gaps = []
        for (a, b) in zip(st['starts'], st['starts'][1:]):
            f = finishes.get(a['round_id'])
            if f:
                gaps.append({'start_gap_s': (int(b['t_us']) - int(a['t_us'])) / 1e6,
                             'after_finish_s': (int(b['t_us']) - int(f['t_us'])) / 1e6})
        return {**super().observe(ctx, draft), 'pool_objects': st['pool_objects'], 'objects': self.objects(ctx),
                'window_s': window_s, 'rounds': rounds, 'round_starts': len(starts),
                'heartbeat_requests': len(hb), 'heartbeat_by_op': by_family(hb, CAS_FAMILIES).get(replica, {}),
                'heartbeat_requests_per_s': len(hb) / window_s if window_s else 0.0,
                'round_requests': len(round_rows),
                'requests_per_idle_round': (len(round_rows) / len(in_window)) if (in_window := [
                    r for r in rounds if r['start_us'] >= draft['window']['t_start_us']]) else None,
                'round_families': sorted({fam(a) for a in round_rows}),
                'outside_round_families': outside,
                'round_outcomes': [f['outcome'] for f in st['finishes']],
                'round_duration_ms': [int(f['duration_ms']) for f in st['finishes']],
                'scheduler_gaps': gaps, 'gc_interval_s': self.p(ctx)['gc_interval_s']}

    def assertions(self, ctx, draft):
        out = super().assertions(ctx, draft)
        obs = draft['observations']
        out['heartbeat_requests>0'] = (True, obs['heartbeat_requests'] > 0)
        out['round_requests>0'] = (True, obs['round_requests'] > 0)
        # Round rows exclude gc_hb by construction; the check is that outside rounds only the heartbeat runs.
        out['families_disjoint'] = (True, obs['outside_round_families'] == ['gc_hb'])
        out['rounds_succeeded'] = (True, bool(obs['round_outcomes']) and all(o in ('Success', 'Deferred')
                                                                               for o in obs['round_outcomes']))
        return out


def is_cleanup_listing(a) -> bool:
    """The MergeTree cleanup thread's listing of a table's files (`MergeTreeData.cpp:4028`): a LIST of `_files/`."""
    return a['op'] == 'ListObjectsV2' and '/_files/' in ((a.get('query') or {}).get('prefix') or '')


def fold_prediction(rule: dict, new_manifests: int, tail_records: int) -> dict:
    """Requests per op of one fold round by the `gc_round_requests` rule, heartbeat pulse included."""
    out = dict(rule['fold'])
    out['GetObject'] = (out.get('GetObject', 0) + new_manifests * rule['gets_per_new_manifest']
                        + tail_records * rule['gets_per_tail_record'])
    return out


@register
class GcTables(PopulatedPool):
    """`params.tables` tables (default 1) on one pool, each taking the same insert stream, with card-driven rounds.

    Under `SYSTEM CAS GC STOP` the card runs `params.bursts` minutes: one INSERT per table at the start of each, then
    `params.rounds_per_burst` synchronous `SYSTEM CAS GC RUN` every `params.round_spacing_s`. Inserts and rounds
    never overlap, so a round's requests are the round's own; a cleanup listing that lands inside one is set apart.
    Each round is checked against the prediction's `inputs.rule`: a fold reads the base, one GET per new manifest and
    one GET per ref log record of every table's tail; a Deferred round reads the base only.
    """

    name = 'gc-tables'

    def tables_n(self, ctx) -> int:
        return int(self.p(ctx).get('tables', 1))

    def tables(self, ctx):
        return [f't{i}' for i in range(self.tables_n(ctx))]

    def setup(self, ctx):
        client = self.replica_client(ctx)
        self.gc(ctx, 'STOP')
        client.query(f'CREATE DATABASE IF NOT EXISTS {DATABASE}')
        for t in self.tables(ctx):
            client.query(f'CREATE TABLE {DATABASE}.{t} ({self.p(ctx)["columns"]}) {self.engine(ctx)}')
        self.state(ctx)['pool_objects'] = {}

    def inserts(self, ctx) -> int:
        return self.p(ctx)['bursts'] * self.tables_n(ctx)

    def measured(self, ctx):
        p, client, st = self.p(ctx), self.replica_client(ctx), self.state(ctx)
        log = st.setdefault('insert_log', [])
        for b in range(p['bursts']):
            t_burst = time.monotonic()
            for ti, t in enumerate(self.tables(ctx)):
                t0 = time.time_ns() // 1000
                # `t` keeps the tables' contents distinct: equal parts would take the hash-hit branch.
                client.query(p['insert'].format(table=f'{DATABASE}.{t}', i=b, t=ti, rows=p['rows'], seed=p['seed']),
                             settings=ctx.manifest['settings']['query'])
                log.append({'table': t, 'burst': b, 't0_us': t0, 't1_us': time.time_ns() // 1000})
            for r in range(p['rounds_per_burst']):
                delay = t_burst + p['first_round_s'] + r * p['round_spacing_s'] - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                self.gc(ctx, 'RUN')
            delay = t_burst + p['burst_period_s'] - time.monotonic()
            if delay > 0 and b + 1 < p['bursts']:
                time.sleep(delay)
        rounds = p['bursts'] * p['rounds_per_burst']
        return {'unit_count': rounds, 'effective_counts': {'rounds': rounds, 'inserts': len(log)}, 'observations': {}}

    def complete(self, ctx, timeout_s=300):
        for c in ctx.cluster.clients:
            c.query('SYSTEM FLUSH LOGS')
        st, client = self.state(ctx), self.replica_client(ctx)
        st['finishes'] = self.gc_finishes(ctx)
        st['rows'] = [sum(_count(client, f'SELECT count() FROM {DATABASE}.{t}') for t in self.tables(ctx))]
        self.collect_rounds(ctx)

    def round_rows(self, ctx, draft) -> list:
        """Per round: attempts by op, cleanup listings (with the catalog GET before each) set apart."""
        st, replica = self.state(ctx), self.p(ctx)['replica']
        rows = [a for a in window_rows(ctx) if a['caller'] == replica and not is_renewal(a)]
        out = []
        for start, finish in st['gc_rounds'].get(replica, []):
            inside = [a for a in rows if start <= a['t_start_us'] <= (math.inf if finish is None else finish)]
            listings = [a for a in inside if is_cleanup_listing(a)]
            drop = {a['seq'] for a in listings}
            for lst in listings:
                # The listing reads the catalog first (iterateDirectory): the latest catalog GET before it.
                before = [a for a in inside if a['seq'] < lst['seq'] and a['seq'] not in drop and a['op'] == 'GetObject'
                          and (L.family_of(CAS_FAMILIES, a.get('key')) or {}).get('name') == 'ref_catalog']
                if before:
                    drop.add(before[-1]['seq'])
            kept = [a for a in inside if a['seq'] not in drop]
            ops: dict = {}
            for a in kept:
                ops[a['op']] = ops.get(a['op'], 0) + 1
            out.append({'start_us': start, 'finish_us': finish, 'requests': len(kept), 'ops': ops,
                        'cleanup_listings': len(listings),
                        'by_family': by_family(kept, CAS_FAMILIES).get(replica, {})})
        return out

    def observe(self, ctx, draft):
        st, p = self.state(ctx), self.p(ctx)
        rule = self.predicted(ctx)['inputs']['rule']
        rounds, log = self.round_rows(ctx, draft), st.get('insert_log', [])
        outcomes = [f['outcome'] for f in st['finishes']]
        checked, folded_until = [], -math.inf
        for i, r in enumerate(rounds):
            outcome = outcomes[i] if i < len(outcomes) else None
            done = [e for e in log if e['t1_us'] < r['start_us']]
            if outcome == 'Success':
                new = sum(1 for e in done if e['t1_us'] > folded_until)
                want = fold_prediction(rule, new, rule['records_per_insert'] * len(done))
                folded_until = r['start_us']
            else:
                want = dict(rule['deferred'])
            residual = {op: r['ops'].get(op, 0) - want.get(op, 0) for op in sorted(set(r['ops']) | set(want))}
            checked.append({**r, 'outcome': outcome, 'inserts_before': len(done), 'predicted': want,
                            'residual': residual})
        folds = [c for c in checked if c['outcome'] == 'Success']
        deferred = [c for c in checked if c['outcome'] == 'Deferred']
        # Counted over the whole window: a listing inside a round is attributed to maintenance but is the table's.
        listings = sum(1 for a in window_rows(ctx) if a['caller'] == p['replica'] and is_cleanup_listing(a)
                       and a['seq'] < draft['window']['end_seq'])
        total = lambda rows, op: sum(c['ops'].get(op, 0) for c in rows)  # noqa: E731
        return {**CasCard.observe(self, ctx, draft), 'tables': self.tables_n(ctx), 'inserts': len(log),
                'rounds': checked, 'round_outcomes': outcomes,
                'fold_rounds': len(folds), 'deferred_rounds': len(deferred),
                'fold_round_ops': {op: total(folds, op) for op in OPS_GC},
                'deferred_round_ops': {op: total(deferred, op) for op in OPS_GC},
                'fold_round_get_predicted': sum(c['predicted'].get('GetObject', 0) for c in folds),
                'max_abs_round_residual': max((abs(v) for c in checked for v in c['residual'].values()), default=None),
                'cleanup_listings_in_rounds': sum(c['cleanup_listings'] for c in checked),
                'cleanup_listings': listings,
                'cleanup_listings_per_table': listings / self.tables_n(ctx),
                'round_duration_ms': [int(f['duration_ms']) for f in st['finishes']],
                'fold_round_duration_ms': [int(f['duration_ms']) for f in st['finishes'] if f['outcome'] == 'Success']}

    def assertions(self, ctx, draft):
        out = super().assertions(ctx, draft)
        obs, p = draft['observations'], self.p(ctx)
        out['rounds'] = (p['bursts'] * p['rounds_per_burst'], len(obs['rounds']))
        out['inserts'] = (self.inserts(ctx), obs['inserts'])
        return out


OPS_GC = ('GetObject', 'PutObject', 'ListObjectsV2', 'HeadObject', 'DeleteObject', 'DeleteObjects')


@register
class Lease(CasCard):
    """An idle mounted pool with GC off: only the lease renewer writes (Pool/CasMountRuntime.cpp:645-700)."""

    name = 'lease'

    def measured(self, ctx):
        time.sleep(self.p(ctx)['idle_s'])
        return {'unit_count': 1, 'effective_counts': {'idle_s': self.p(ctx)['idle_s']}, 'observations': {}}

    def inserts(self, ctx) -> int:
        return 0

    def complete(self, ctx, timeout_s=300):
        for c in ctx.cluster.clients:
            c.query('SYSTEM FLUSH LOGS')
        self.state(ctx)['rows'] = [0]
        self.collect_rounds(ctx)

    def observe(self, ctx, draft):
        out = super().observe(ctx, draft)
        window_s = (draft['window']['t_end_us'] - draft['window']['t_start_us']) / 1e6
        lease = out['maintenance_by_family'].get('lease', {})
        renewals = lease.get('PutObject', 0)
        return {**out, 'window_s': window_s, 'lease_by_op': lease, 'lease_ops': sorted(lease), 'lease_renewals': renewals,
                'lease_requests': sum(lease.values()), 'lease_requests_per_s': sum(lease.values()) / window_s}


def pool_objects(parts: int, columns_n: int) -> int:
    """Pool objects (blobs, `.meta` siblings, manifests) `PopulatedPool` creates.

    A part of `columns_n` CODEC(NONE) columns plus `k`, at 1 000 rows (one granule), has `columns_n + 1` distinct
    `.bin` blobs; every `.cmrk2` marks file of one granule is the same bytes, so one marks blob serves all parts
    (duplicate refs collapse to one upload, ContentAddressedTransaction.cpp:302-305); `primary.cidx` and the small
    files are inline. Each blob has a `.meta` sibling; each part one manifest.
    """
    return parts * (2 * (columns_n + 1) + 1) + 2


@register
class GcUnderLoad(CasCard, NaturalMergeCard):
    """`small-inserts.default` on the CAS disk with GC measured (validation, run in Task 7).

    Retired: payload objects the window created that no active part references at the end (merged-away parts'
    unique blobs and their manifests). Reclaimed: the retired objects GC deleted before the window ended. The two
    rates feed the `backlog_grows` condition of the `gc` cell.
    """

    name = 'gc-under-load'

    def complete(self, ctx, timeout_s=300):
        NaturalMergeCard.complete(self, ctx, timeout_s)
        st = self.state(ctx)
        st['parts'] = self.parts(ctx)
        st['created'] = {(r['bucket'], r['key']): r['req_payload_bytes'] for r in window_rows(ctx)
                         if (r['bucket'], r['key']) in cohort([r])}
        self.collect_rounds(ctx)

    def teardown(self, ctx):
        # As CasBranches: the placement read is teardown traffic, outside the window.
        self.state(ctx)['placement'] = self.placement(ctx)
        super().teardown(ctx)

    def observe(self, ctx, draft):
        st = self.state(ctx)
        # Blobs by placement; a manifest's key does not show in placement, so retired manifests are counted
        # (created less active parts) and their deletions taken from the ledger.
        # Outdated parts stay listed until their removal; only active parts keep their blobs referenced.
        active = {p['name'] for p in st['parts'] if p['active']}
        referenced = {key for name, part in st['placement'].items() if name in active for key in part.values() if key}
        retired = {i: b for i, b in st['created'].items()
                   if '/blobs/' in i[1] and i[1].split('/blobs/', 1)[-1] not in referenced}
        manifests = {i: b for i, b in st['created'].items() if '/cas/manifests/' in i[1]}
        gone = deletions(window_rows(ctx), set(retired), draft['window']['t_start_us'])
        gone_manifests = deletions(window_rows(ctx), set(manifests), draft['window']['t_start_us'])
        window_s = (draft['window']['t_end_us'] - draft['window']['t_start_us']) / 1e6
        return {**CasCard.observe(self, ctx, draft), 'window_s': window_s,
                'retired_objects': len(retired), 'retired_bytes': sum(retired.values()),
                'reclaimed_objects': len(gone), 'reclaimed_bytes': sum(retired[i] for i in gone),
                'retired_manifests': len(manifests) - len(active),
                'reclaimed_manifests': len(gone_manifests),
                'gc_finishes': len(self.gc_finishes(ctx))}

    def assertions(self, ctx, draft):
        out = CasCard.assertions(self, ctx, draft)
        out['retired_objects>0'] = (True, draft['observations']['retired_objects'] > 0)
        return out
