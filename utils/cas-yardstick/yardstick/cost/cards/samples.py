"""Sample card of the phase-0 runtime estimate: a minimal `gc-scale`.

The CAS GC workstream owns the full card; this one only produces a pass at planned scale whose duration the
estimate extrapolates from. The merge sample runs on the lifecycle card `merge-natural` (`lifecycle.py`).
"""
from __future__ import annotations

import time

from .cas_probe import CAS_FAMILIES
from .probes import CREATE_OPS, ManifestCard, register


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


