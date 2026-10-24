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
from .probes import CREATE_OPS, ManifestCard, _count, category, window_rows

# CAS key families (tmp/src-26.8/src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h).
# Anchored on the layout's own suffix shapes so the pass prefix (which contains `/cas/` as the policy name) never
# matches. `payload` marks the families a dropped table turns into garbage; the rest are control state.
# `maintenance` marks control state that no foreground write touches (CasLayout.h, same directory):
# `gc/server-roots/<srid>/{owner,epoch,mount}` is the per-server lease subtree (CasLayout.h:419-449), renewed by the
# mount runtime every `mount_renew_period` (Pool/CasMountRuntime.h:70); `gc/state`, `gc/maintenance_state`, `gc/hb`
# and `gc/gen/...` are GC authority, cursor, heartbeat and per-round artifacts (CasLayout.h:332-390); `_pool_meta` is
# the pool-level
# metadata object probed at pool bootstrap and read-modify-written by the reader floor (CasLayout.h:466,
# Pool/CasPool.cpp:123, Pool/CasPoolMeta.cpp:116).
CAS_FAMILIES = [
    {'name': 'blob_meta', 'regex': r'/blobs/[a-z0-9]+/[0-9a-f]{2}/[0-9a-f]+\.meta$', 'category': 'metadata',
     'payload': False},
    {'name': 'blob', 'regex': r'/blobs/[a-z0-9]+/[0-9a-f]{2}/[0-9a-f]+$', 'category': 'active', 'payload': True},
    {'name': 'manifest', 'regex': r'/cas/manifests/.+/[0-9a-f]+-[0-9a-f]+/\d{6}(\.[a-z0-9]+)?$', 'category': 'active',
     'payload': True},
    {'name': 'refs', 'regex': r'/cas/(ns/(stream|state)/|ref_catalog$)', 'category': 'metadata', 'payload': False},
    {'name': 'lease', 'regex': r'/gc/server-roots/', 'category': 'metadata', 'payload': False, 'maintenance': True},
    {'name': 'gc', 'regex': r'/gc/(state|maintenance_state|hb)$|/gc/gen/', 'category': 'metadata', 'payload': False,
     'maintenance': True},
    {'name': 'pool_meta', 'regex': r'/_pool_meta$', 'category': 'metadata', 'payload': False, 'maintenance': True},
    {'name': 'roots', 'regex': r'/roots/', 'category': 'metadata', 'payload': False},
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
