"""Sample cards of the phase-0 runtime estimate: minimal versions of `merge-natural` and `gc-scale`.

The lifecycle and CAS GC workstreams own the full cards; these only produce a pass at planned scale whose
duration the estimate extrapolates from.
"""
from __future__ import annotations

import time

from .base import DATABASE, POLL_S
from .cas_probe import CAS_FAMILIES
from .probes import CREATE_OPS, ManifestCard, _count, register


@register
class MergeNatural(ManifestCard):
    name = 'merge-natural'
    policies = ('s3', 'cas', 'local')
    mode = 'stochastic'

    def complete(self, ctx, timeout_s=300):
        """Natural merges have no end; wait until none has run for `quiet_s`, at most `quiesce_timeout_s`."""
        r1, p = self.clients(ctx)[0], self.p(ctx)
        sql = f"SELECT count() FROM system.merges WHERE database = '{DATABASE}' AND table = '{self.table}'"
        start = quiet_since = time.monotonic()
        while time.monotonic() - quiet_since < p['quiet_s'] and time.monotonic() - start < p['quiesce_timeout_s']:
            if _count(r1, sql):
                quiet_since = time.monotonic()
            time.sleep(POLL_S)
        self.state(ctx)['quiesced'] = time.monotonic() - quiet_since >= p['quiet_s']
        self.state(ctx)['active_parts'] = _count(r1, f"SELECT count() FROM system.parts WHERE active AND "
                                                     f"database = '{DATABASE}' AND table = '{self.table}'")
        super().complete(ctx, timeout_s)

    def observe(self, ctx, draft):
        return {k: self.state(ctx)[k] for k in ('quiesced', 'active_parts')}

    def assertions(self, ctx, draft):
        return {}


@register
class GcScale(ManifestCard):
    name = 'gc-scale'
    policies = ('cas',)
    mode = 'stochastic'

    def inventory_families(self, ctx):
        return CAS_FAMILIES

    def complete(self, ctx, timeout_s=300):
        """After the inserts, wait for one GC round that starts later to finish."""
        super().complete(ctx, timeout_s)
        t0, deadline = time.time_ns() // 1000, time.monotonic() + timeout_s
        sql = ("SELECT count(), max(duration_ms) FROM system.cas_gc_log WHERE event_type = 'Finish' AND outcome = "
               f"'Success' AND event_time_microseconds >= fromUnixTimestamp64Micro({t0}) FORMAT TSV")
        while True:
            for client in ctx.cluster.clients:
                client.query('SYSTEM FLUSH LOGS')
            found = [client.query(sql).split() for client in ctx.cluster.clients]
            done = [int(f[1]) for f in found if int(f[0])]
            if done or time.monotonic() >= deadline:
                break
            time.sleep(1)
        self.state(ctx)['gc_round_ms'] = done[0] if done else None

    def observe(self, ctx, draft):
        created = sum(c.get('upstream_ok', 0) for ops in draft['ledger']['foreground'].values()
                      for op, c in ops.items() if op in CREATE_OPS)
        return {'gc_round_ms': self.state(ctx)['gc_round_ms'], 'objects_created': created}

    def assertions(self, ctx, draft):
        return {'gc_round_finished': (True, self.state(ctx)['gc_round_ms'] is not None)}


