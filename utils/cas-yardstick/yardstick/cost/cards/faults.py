"""Fault cards: one INSERT under proxy-injected store faults; retries, their outcomes and the upload residue.

Every parameter comes from the manifest (`experiments/faults.<case>.json` of the replication report): the fault
rules (`params.faults`, the proxy's `set_fault` rule format), the table and the insert. Expected request counts
come from `predictions/<experiment_id>.<policy>.json`. The rules are installed at the start of the window and
cleared when the INSERT returns, so no later request of the pass can match them.
"""
from __future__ import annotations

import time

from .. import ledger as L
from .cas_probe import CAS_FAMILIES
from .lifecycle import LifecycleCard, check_observation
from .probes import compare_ledger, register, window_rows

FAILED = ('injected', 'dropped')
ABORT_WAIT_S = 30  # the server's own abort of a failed upload may land after the INSERT returned
POLL_S = 0.5


def retry_view(rows: list) -> dict:
    """Failed attempts, retry chains and the outcome of each chain's last attempt, from window rows of one caller.

    A chain is `ledger.correlate_retries`: each attempt after the first follows a failed one with the same
    identity (op, key, upload, part, payload). A failed attempt that no chain contains was not retried.
    """
    failed = [r for r in rows if r['downstream']['outcome'] in FAILED]
    chains = L.correlate_retries(rows)
    by_seq = {r['seq']: r for r in rows}
    in_chain = {s for g in chains for s in g}
    outcomes = {}
    for r in failed:
        key = f"{r['op']}.{r['downstream']['outcome']}"
        outcomes[key] = outcomes.get(key, 0) + 1
    return {'failed_attempts': len(failed), 'failed_by_op_outcome': dict(sorted(outcomes.items())),
            'chains': [{'op': by_seq[g[0]]['op'], 'attempts': len(g),
                        'downstream': [by_seq[s]['downstream']['outcome'] for s in g],
                        'upstream': [by_seq[s]['upstream']['outcome'] for s in g]} for g in chains],
            'failed_not_retried': sorted(r['seq'] for r in failed if r['seq'] not in in_chain),
            'extra_attempts': sum(len(g) - 1 for g in chains)}


@register
class Faults(LifecycleCard):
    """`params.inserts` INSERTs on `params.replica` with `params.faults` installed for exactly their duration."""

    name = 'faults'
    policies = ('s3', 's3plain', 'cas')
    expected_exceptions = (RuntimeError,)
    # CAS background ref and lease traffic makes its window non-deterministic; the card compares the others exactly.
    mode = 'stochastic'
    exact_policies = ('s3', 's3plain')

    def per_policy(self, ctx, key):
        value = self.p(ctx)[key]
        return value[ctx.policy] if isinstance(value, dict) else value

    def inventory_families(self, ctx):
        return CAS_FAMILIES if ctx.policy.removesuffix('_cached') == 'cas' else []

    def measured(self, ctx):
        for rule in self.p(ctx)['faults']:
            ctx.proxy.set_fault(rule)
        try:
            return super().measured(ctx)
        finally:
            ctx.proxy.clear_faults()

    def caller_rows(self, ctx, draft) -> list:
        replica = self.p(ctx)['replica']
        return [r for r in window_rows(ctx) if r['caller'] == replica and r['seq'] <= draft['window']['end_seq']]

    def complete(self, ctx, timeout_s=300):
        super().complete(ctx, timeout_s)
        # The incomplete inventory after the window, before the teardown or the runner's cleanup can abort anything.
        # The server aborts in the failed write buffer's cancel path; wait for that abort to reach the ledger.
        st, t0 = self.state(ctx), time.monotonic()
        while True:
            bounds = ctx.replay()
            st['incomplete_after'] = max(sum(p['size'] for p in inv.incomplete.values())
                                         for inv in (bounds.lower, bounds.upper))
            if st['incomplete_after'] == 0 or time.monotonic() - t0 >= ABORT_WAIT_S:
                break
            time.sleep(POLL_S)
        st['abort_wait_s'] = time.monotonic() - t0

    def observe(self, ctx, draft):
        rows = self.caller_rows(ctx, draft)
        st = self.state(ctx)
        created = [r for r in rows if r['op'] == 'CreateMultipartUpload' and r['upstream']['outcome'] == 'ok']
        aborts = [r for r in rows if r['op'] == 'AbortMultipartUpload']
        part2 = [r for r in rows if r['op'] == 'UploadPart' and r.get('part_number') == 2]
        fg = draft['ledger']['foreground'].get(self.p(ctx)['replica'], {})
        return {'retries': retry_view(rows),
                'outcomes': {op: {k: c[k] for k in ('attempts', 'injected', 'dropped', 'upstream_ok', 'accepted_dropped')}
                             for op, c in sorted(fg.items())},
                'uploads_created': len(created),
                'aborts': [{'upstream': r['upstream']['outcome'], 'downstream': r['downstream']['outcome']} for r in aborts],
                'part2_attempts': len(part2),
                'incomplete_after_window': st['incomplete_after'], 'abort_wait_s': st['abort_wait_s'],
                'insert_failed': bool(draft['observations'].get('expected_exceptions'))}

    def assertions(self, ctx, draft):
        pred = self.predicted(ctx)
        exact = ctx.policy in self.exact_policies
        out = compare_ledger(pred, draft['ledger']['foreground'], exact)
        for key, want in pred['expected']['observations'].items():
            out[f'observation.{key}'] = check_observation(want, draft['observations'].get(key), draft['observations'])
        obs, p = draft['observations'], self.p(ctx)
        out['faults_fired'] = (self.per_policy(ctx, 'expected_failed_attempts'), obs['retries']['failed_attempts'])
        out['insert_failed'] = (p['expect_insert_failure'], obs['insert_failed'])
        rows = 0 if p['expect_insert_failure'] else self.inserts(ctx) * p['rows']
        out['rows.' + p['replica']] = (rows, self.state(ctx).get('rows', [None])[0])
        if p['expect_insert_failure']:
            out['abort_by_server_ok'] = (True, any(a['upstream'] == 'ok' for a in obs['aborts']))
            out['incomplete_after_window'] = (0, obs['incomplete_after_window'])
        elif self.per_policy(ctx, 'expect_retry_chain'):
            out['failed_not_retried'] = ([], obs['retries']['failed_not_retried'])
        if exact:
            out['window.crossing'] = ([], draft['window']['crossing'])
        return out
