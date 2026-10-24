"""Phase-0 CAS probes: a dropped table's cohort reclaimed by GC, and the `Expect: 100-continue` path.

Payload key families say what an object is (its inventory category), not who caused a request: GC HEADs and DELETEs
the same blob keys foreground publication writes (tmp/src-26.8/src/Disks/DiskObjectStorage/MetadataStorages/
ContentAddressed/Gc/CasGc.cpp:580). Request origin is therefore causal: a request by replica `rN` that starts
inside one of `rN`'s GC rounds (`system.cas_gc_log` Start..Finish of one `round_id`) is maintenance. So is any
request on a control family only the lease keeper, GC and pool bootstrap touch (`maintenance` flag below), even
with GC off: the server renews its mount lease on its own schedule. Every other request is foreground. The GC
probe's cells price the integrated usage (foreground plus maintenance).
"""
from __future__ import annotations

import math
import time

from .. import ledger as L
from . import CARDS
from .base import DATABASE
from .probes import CREATE_OPS, ManifestCard, _count, category, integrated, window_rows

# CAS key families (tmp/src-26.8/src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h).
# Anchored on the layout's own suffix shapes so the pass prefix (which contains `/cas/` as the policy name) never
# matches. `payload` marks the families a dropped table turns into garbage; the rest are control state.
# `maintenance` marks control state that no foreground write touches (CasLayout.h, same directory):
# `gc/server-roots/<srid>/{owner,epoch,mount}` is the per-server lease subtree (CasLayout.h:419-449), renewed by the
# mount runtime every `mount_renew_period` (Pool/CasMountRuntime.h:70); `gc/hb` is the GC heartbeat the scheduler's
# own worker pulses (CasLayout.h:344, Gc/CasGcScheduler.cpp:499), a family of its own so heartbeat rows and round
# rows stay disjoint by key; `gc/state`, `gc/maintenance_state` and `gc/gen/...` are GC authority, cursor and
# per-round artifacts (CasLayout.h:332-390); `_pool_meta` is the pool-level metadata object probed at pool bootstrap
# and read-modify-written by the reader floor (CasLayout.h:466, Pool/CasPool.cpp:123, Pool/CasPoolMeta.cpp:116);
# `roots/` holds the loose mountpoint objects mirrored at their ClickHouse path, such as the server's startup write
# probe (CasLayout.h:317-329, :397, :453; ContentAddressedTransaction.cpp:906): no MergeTree part or table file lands
# there (those go to manifests and the namespace's `_files/`).
CAS_FAMILIES = [
    {'name': 'blob_meta', 'regex': r'/blobs/[a-z0-9]+/[0-9a-f]{2}/[0-9a-f]+\.meta$', 'category': 'metadata',
     'payload': False},
    {'name': 'blob', 'regex': r'/blobs/[a-z0-9]+/[0-9a-f]{2}/[0-9a-f]+$', 'category': 'active', 'payload': True},
    {'name': 'manifest', 'regex': r'/cas/manifests/.+/[0-9a-f]+-[0-9a-f]+/\d{6}(\.[a-z0-9]+)?$', 'category': 'active',
     'payload': True},
    # The ref layer by object kind (CasLayout.h:133-201, :246-268, :471-476): the immutable stream of one namespace
    # life (`_log` transactions and `_snap` snapshots), its mutable `_ckpt`, the life's verbatim table files under
    # `_files/`, and the pool-wide namespace catalog.
    {'name': 'ref_stream', 'regex': r'/cas/ns/stream/', 'category': 'metadata', 'payload': False},
    {'name': 'ref_ckpt', 'regex': r'/cas/ns/state/[^/]+/_ckpt', 'category': 'metadata', 'payload': False},
    {'name': 'ns_files', 'regex': r'/cas/ns/state/[^/]+/_files/', 'category': 'metadata', 'payload': False},
    {'name': 'ref_catalog', 'regex': r'/cas/ref_catalog$', 'category': 'metadata', 'payload': False},
    {'name': 'lease', 'regex': r'/gc/server-roots/', 'category': 'metadata', 'payload': False, 'maintenance': True},
    {'name': 'gc_hb', 'regex': r'/gc/hb$', 'category': 'metadata', 'payload': False, 'maintenance': True},
    {'name': 'gc', 'regex': r'/gc/(state|maintenance_state)$|/gc/gen/', 'category': 'metadata', 'payload': False,
     'maintenance': True},
    {'name': 'pool_meta', 'regex': r'/_pool_meta$', 'category': 'metadata', 'payload': False, 'maintenance': True},
    {'name': 'roots', 'regex': r'/roots/', 'category': 'metadata', 'payload': False, 'maintenance': True},
]
GC_ROUNDS_SQL = ("SELECT toUnixTimestamp64Micro(minIf(event_time_microseconds, event_type = 'Start')), "
                 "toUnixTimestamp64Micro(maxIf(event_time_microseconds, event_type = 'Finish')), "
                 "countIf(event_type = 'Finish'), countIf(event_type = 'Start') FROM system.cas_gc_log "
                 "GROUP BY round_id ORDER BY 1 FORMAT TSV")


def gc_rounds(client) -> list:
    """`[start_us, finish_us or None]` of every GC round on one server; a round still running has no finish.

    Raises ValueError for a round with no Start row: its window cannot be known, so attribution would guess.
    """
    out = []
    for line in client.query(GC_ROUNDS_SQL).splitlines():
        if line.strip():
            start, finish, done, started = (int(x) for x in line.split('\t'))
            if not started:
                raise ValueError(f'GC round finishing at {finish} us has no Start row in system.cas_gc_log')
            out.append([start, finish if done else None])
    return out


def attribute(attempts: list, rounds: dict, families: list) -> dict:
    """Foreground / maintenance / rig split: maintenance when inside one of the caller's own GC round windows or on
    a control family flagged `maintenance`."""
    out = {'foreground': [], 'maintenance': [], 'rig': []}
    for a in attempts:
        if a['caller'] == 'rig':
            out['rig'].append(a)
            continue
        inside = any(s <= a['t_start_us'] <= (math.inf if f is None else f) for s, f in rounds.get(a['caller'], []))
        out['maintenance' if inside or L.is_maintenance(a, families) else 'foreground'].append(a)
    return out


def _payload(key) -> bool:
    family = L.family_of(CAS_FAMILIES, key)
    return bool(family and family['payload'])


def cohort(rows: list) -> set:
    """(bucket, key) of payload and manifest objects r1 created in the window; control families are excluded."""
    return {(r['bucket'], r['key']) for r in rows if r['caller'] == 'r1' and r['op'] in CREATE_OPS
            and r['upstream']['outcome'] == 'ok' and _payload(r.get('key'))}


def deletions(rows: list, keys: set, after_us: int) -> dict:
    """identity -> {t_us, caller} of the first upstream-ok deletion of each cohort key after `after_us`."""
    out = {}
    for r in rows:
        if r['caller'] not in L.WORKLOAD_CALLERS or r['upstream']['outcome'] != 'ok' or r['t_start_us'] < after_us:
            continue
        if r['op'] == 'DeleteObject':
            hit = [(r['bucket'], r['key'])]
        elif r['op'] == 'DeleteObjects':
            hit = [(r['bucket'], d['key']) for d in (r.get('response_body') or {}).get('delete_results') or []
                   if d.get('deleted') or not d.get('code')]
        else:
            continue
        for ident in hit:
            if ident in keys and ident not in out:
                out[ident] = {'t_us': r['t_end_us'], 'caller': r['caller']}
    return out


def _register(cls):
    CARDS[cls.name] = cls()
    return cls


@_register
class CasGcCohort(ManifestCard):
    name = 'probe_cas_gc_cohort'
    policies = ('cas',)
    mode = 'stochastic'

    def inventory_families(self, ctx):
        return CAS_FAMILIES

    def complete(self, ctx, timeout_s=300):
        super().complete(ctx, timeout_s)
        st = self.state(ctx)
        st['cohort'] = cohort(window_rows(ctx))
        st['t_drop_us'] = time.time_ns() // 1000
        r1 = self.clients(ctx)[0]
        r1.query(f'DROP TABLE {DATABASE}.{self.table} SYNC')
        deadline = time.monotonic() + self.p(ctx)['reclaim_timeout_s']
        while True:
            gone = _count(r1, f"SELECT count() FROM system.tables WHERE database = '{DATABASE}' "
                              f"AND name = '{self.table}'") == 0
            st['deleted'] = deletions(window_rows(ctx), st['cohort'], st['t_drop_us'])
            if (gone and len(st['deleted']) == len(st['cohort'])) or time.monotonic() >= deadline:
                break
            time.sleep(1)
        last = max((d['t_us'] for d in st['deleted'].values()), default=None)
        st['reclaim_delay_s'] = None if last is None else (last - st['t_drop_us']) / 1e6
        for client in ctx.cluster.clients:
            client.query('SYSTEM FLUSH LOGS')
        t0 = ctx.window()['t_start_us']
        st['gc_rounds_reclaiming'] = sum(_count(c, (
            "SELECT count() FROM system.cas_gc_log WHERE event_type = 'Finish' AND event_time_microseconds >= "
            f"fromUnixTimestamp64Micro({t0}) AND objects_deleted + manifests_deleted + entries_redeleted > 0"))
            for c in ctx.cluster.clients)
        # Collected last: every round that started before the window ends is in the log by now.
        st['gc_rounds'] = {f'r{n}': gc_rounds(c) for n, c in enumerate(ctx.cluster.clients, 1)}
        st['expect_rows'] = sum(1 for r in window_rows(ctx) if r.get('expect_negotiated'))

    def classify(self, ctx, attempts, families):
        st = self.state(ctx)
        if 'gc_rounds' not in st:
            # Without the round windows every request would look foreground; fail the pass instead.
            raise RuntimeError('GC rounds were not collected; request origin cannot be attributed')
        return attribute(attempts, st['gc_rounds'], families)

    def reclassify(self, ctx, bounds):
        st = self.state(ctx)
        if st.get('cohort'):
            bounds.reclassify(sorted(st['cohort']), 'garbage', st['t_drop_us'], 'DROP TABLE SYNC')

    def observe(self, ctx, draft):
        st = self.state(ctx)
        expect = self.predicted(ctx)['inputs']['expect_negotiated_rows']
        return {**super().observe(ctx, draft),
                'request_attribution': {'method': 'gc_round_windows',
                                        'rounds': {r: len(v) for r, v in st['gc_rounds'].items()}},
                'cohort_size': len(st['cohort']), 'cohort_reclaimed': len(st['deleted']),
                'reclaim_delay_s': st['reclaim_delay_s'], 'gc_rounds_reclaiming': st['gc_rounds_reclaiming'],
                'deleting_callers': sorted({d['caller'] for d in st['deleted'].values()}),
                'expect_negotiated_rows': st['expect_rows'], 'expect_matches_source': st['expect_rows'] == expect}

    def assertions(self, ctx, draft):
        out = super().assertions(ctx, draft)
        obs, bh = draft['observations'], category(draft, 'garbage')
        out['cohort_nonempty'] = (True, obs['cohort_size'] > 0)
        out['cohort_reclaimed'] = (obs['cohort_size'], obs['cohort_reclaimed'])
        out['gc_log_evidence'] = (True, obs['gc_rounds_reclaiming'] > 0)
        out['deletes_by_r1'] = (['r1'], obs['deleting_callers'])  # the only replica holding the table
        out['reclaim_delay_s>0'] = (True, (obs['reclaim_delay_s'] or 0) > 0)
        out['garbage_byte_hours>0'] = (True, bh['lower'] > 0 and bh['upper'] > 0)
        out['expect_negotiated_rows'] = (self.predicted(ctx)['inputs']['expect_negotiated_rows'],
                                         obs['expect_negotiated_rows'])
        return out


@_register
class CasExpect(ManifestCard):
    """CAS conditional PUTs with `Expect: 100-continue`: the manifest lowers the disk's `expect_continue_min_bytes`
    so every conditional PUT negotiates, and the proxy answers `100 Continue` itself. Stochastic like every CAS
    card: lease renewals and ref-log writes run on the server's own schedule."""
    name = 'probe_cas_expect'
    policies = ('cas',)
    mode = 'stochastic'

    def inventory_families(self, ctx):
        return CAS_FAMILIES

    def complete(self, ctx, timeout_s=300):
        super().complete(ctx, timeout_s)
        # `gc: off`: no round may run. A GC log that exists is read, never assumed empty, so a round still shows.
        rounds = {}
        for n, client in enumerate(ctx.cluster.clients, 1):
            exists = _count(client, "SELECT count() FROM system.tables WHERE database = 'system' AND name = 'cas_gc_log'")
            rounds[f'r{n}'] = gc_rounds(client) if exists else []
        self.state(ctx)['gc_rounds'] = rounds

    def classify(self, ctx, attempts, families):
        st = self.state(ctx)
        if 'gc_rounds' not in st:
            raise RuntimeError('GC rounds were not collected; request origin cannot be attributed')
        return attribute(attempts, st['gc_rounds'], families)

    def window_attempts(self, ctx, draft) -> list:
        w = draft['window']
        return [r for r in L.read_rows(ctx.proxy.ledger_path) if r.get('kind') == 'attempt'
                and w['start_seq'] < r['seq'] < w['end_seq'] and r['caller'] in L.WORKLOAD_CALLERS]

    def observe(self, ctx, draft):
        rows = self.window_attempts(ctx, draft)
        by_op: dict = {}
        for r in rows:
            by_op[r['op']] = by_op.get(r['op'], 0) + 1
        counted: dict = {}
        for ops in integrated(draft).values():
            for op, c in ops.items():
                counted[op] = counted.get(op, 0) + c['attempts']
        negotiated = [r for r in rows if r.get('expect_negotiated')]
        expect = [r['seq'] for r in negotiated]
        return {**super().observe(ctx, draft),
                'expect_negotiated_ops': sorted({r['op'] for r in negotiated}),
                'plain_put_rows': sum(1 for r in rows if r['op'] == 'PutObject' and not r.get('expect_negotiated')),
                'request_attribution': {'method': 'control_families',
                                        'rounds': {r: len(v) for r, v in self.state(ctx)['gc_rounds'].items()}},
                'expect_negotiated_rows': len(expect), 'expect_negotiated_seqs_distinct': len(set(expect)),
                'wire_bytes_upper_rows': sum(1 for r in rows if r.get('wire_bytes_bound') == 'upper'),
                'window_rows_by_op': dict(sorted(by_op.items())), 'window_attempts_by_op': dict(sorted(counted.items()))}

    def assertions(self, ctx, draft):
        out = super().assertions(ctx, draft)
        obs = draft['observations']
        want = self.predicted(ctx)['inputs']['expect_negotiated_rows_min']
        out[f'expect_negotiated_rows>={want}'] = (True, obs['expect_negotiated_rows'] >= want)
        # Only conditional PUTs negotiate (PocoHTTPClient.cpp:665), and a negotiated PUT is one attempt: the interim
        # `100 Continue` adds no row. The classified PutObject attempts (foreground plus maintenance) less the plain
        # PUT rows bound the negotiated rows from above; a negotiated row the classification lost breaks the bound.
        out['expect_negotiated_ops'] = (True, set(obs['expect_negotiated_ops']) <= {'PutObject'})
        # Rows and classified attempts agree per op (a double count shows here); the bound below catches a lost row.
        out['expect_attempts_counted_once'] = (obs['window_rows_by_op'], obs['window_attempts_by_op'])
        room = obs['window_attempts_by_op'].get('PutObject', 0) - obs['plain_put_rows']
        out['expect_rows_within_put_attempts'] = (True, obs['expect_negotiated_rows'] <= room)
        out['no_gc_rounds'] = (0, sum(obs['request_attribution']['rounds'].values()))
        return out
