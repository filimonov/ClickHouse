"""`s3` disk operation cards (R2): mutations, drops, cold reads with and without the cache, and startup.

Setup creates the tables of `params.tables` and runs `params.inserts` before the window; the window holds only the
operation under test (`params.statements`, a restart, a select). Object identities come from
`system.remote_data_paths` snapshots before and after, so writes and deletes are attributed to parts.
"""
from __future__ import annotations

import os
import time

from .. import ledger as L
from .base import DATABASE, POLL_S
from .probes import _count, register, window_rows
from .shapes import S3Card, object_per_file, tree_allocated


def layout_keys(layout: dict, prefix: str | None = None) -> set:
    return {key for part, files in layout.items() if prefix is None or part.startswith(prefix)
            for key, _ in files.values()}


def rewrite_view(before: dict, after: dict, parts_before: list, parts_after: list) -> dict:
    """Per table, the objects of the active parts after the operation: new ones and ones kept from a source part.

    A wide part's mutation keeps unchanged column files as hard links to the same objects; a full rewrite keeps none.
    """
    out = {}
    tables = sorted({p['table'] for p in parts_before} | {p['table'] for p in parts_after})
    for t in tables:
        old = layout_keys(before, f'{t}/')
        active = [p for p in parts_after if p['table'] == t and p['active']]
        keys = {key for p in active for key, _ in after.get(f'{t}/{p["name"]}', {}).values()}
        out[t] = {'active_parts': sorted([p['name'], p['part_type']] for p in active),
                  'objects': len(keys), 'new_objects': len(keys - old), 'kept_objects': len(keys & old),
                  'removed_objects': len(old - layout_keys(after, f'{t}/'))}
    return out


class OperationCard(S3Card):
    """Setup = DDL + inserts; window = `params.statements`; then wait until drained (outdated parts removed)."""

    def setup(self, ctx):
        super().setup(ctx)
        self.run_inserts(ctx, self.p(ctx)['inserts'])
        self.wait_drained(ctx, ctx.manifest['timeout_s'])
        st = self.state(ctx)
        st['parts_before'] = self.all_parts(ctx)
        st['layout_before'] = self.snapshot_layout(ctx, st['parts_before'])

    def run_statements(self, ctx) -> int:
        client = self.replica_client(ctx)
        for s in self.p(ctx)['statements']:
            client.query(s['sql'].format(db=DATABASE), settings={**ctx.manifest['settings']['query'],
                                                                **s.get('settings', {})})
        return len(self.p(ctx)['statements'])

    def measured(self, ctx):
        n = self.run_statements(ctx)
        return {'unit_count': n, 'effective_counts': {'statements': n}, 'observations': {}}

    def expected_gone(self, ctx) -> set:
        """Objects the operation removes: those of the setup's parts that no part lists any more. None outside
        OBJECT_PER_FILE_POLICIES: removal there is not the part's (CAS GC) or there are no objects (local)."""
        if not object_per_file(ctx.policy):
            return set()
        st = self.state(ctx)
        return layout_keys(st.get('layout_before', {})) - layout_keys(st.get('layout', {}))

    def complete(self, ctx, timeout_s=300):
        super().complete(ctx, timeout_s)
        st = self.state(ctx)
        # system.parts forgets an outdated part before the reply of its DeleteObjects reaches the ledger, so the
        # replay is polled until the removed objects are gone; on timeout the assertion reports what is left.
        gone, deadline = self.expected_gone(ctx), time.monotonic() + timeout_s
        while True:
            st['live_keys'] = {ident[1] for ident in ctx.replay().upper.objects}
            if not gone & st['live_keys'] or time.monotonic() >= deadline:
                break
            time.sleep(POLL_S)

    def deleted(self, ctx, draft) -> dict:
        ops = self.ops_of(ctx, draft)
        return {'DeleteObjects_calls': ops.get('DeleteObjects', {}).get('attempts', 0),
                'DeleteObjects_keys': ops.get('DeleteObjects', {}).get('delete_keys', 0),
                'DeleteObject_calls': ops.get('DeleteObject', {}).get('attempts', 0)}

    def observe(self, ctx, draft):
        st = self.state(ctx)
        return {'parts_before': self.part_view(st['parts_before']), 'parts_after': self.part_view(st['parts']),
                'objects_before': self.object_counts(st['layout_before'], st['parts_before']),
                'rewrite': rewrite_view(st['layout_before'], st['layout'], st['parts_before'], st['parts']),
                'requests_by_file': self.requests_by_file(ctx, draft), **self.deleted(ctx, draft)}


@register
class Mutation(OperationCard):
    """ALTER UPDATE / ALTER DELETE / lightweight DELETE over tables of both part types.

    A Compact part has one data file, so any mutation rewrites it: no object of the source part survives.
    """

    name = 'mutation'

    def assertions(self, ctx, draft):
        out = super().assertions(ctx, draft)
        rewrite, st = draft['observations']['rewrite'], self.state(ctx)
        for t in self.p(ctx)['tables']:
            source_types = {p['part_type'] for p in st['parts_before'] if p['table'] == t['name'] and p['active']}
            out[f'source_part_type.{t["name"]}'] = ([t['part_type']], sorted(source_types))
            if t['part_type'] == 'Compact':
                out[f'full_rewrite.{t["name"]}.kept_objects'] = (0, rewrite[t['name']]['kept_objects'])
                out[f'full_rewrite.{t["name"]}.new_objects'] = (rewrite[t['name']]['objects'],
                                                                 rewrite[t['name']]['new_objects'])
        if object_per_file(ctx.policy):
            dead = layout_keys(st['layout_before']) - layout_keys(st['layout'])
            out['replaced_objects_deleted'] = ([], sorted(dead & st['live_keys'])[:5])
        return out


@register
class Drop(OperationCard):
    """DROP PARTITION, TTL expiry or DROP TABLE SYNC: every object of the dropped parts is deleted in the window.

    `params.dropped` names the `table/` or `table/<partition_id>` scope whose objects must be gone at the end.
    """

    name = 'drop'

    def measured(self, ctx):
        if self.p(ctx).get('statements'):
            return super().measured(ctx)
        return {'unit_count': 0, 'effective_counts': {'statements': 0}, 'observations': {}}

    def complete(self, ctx, timeout_s=300):
        p = self.p(ctx)
        if p.get('until_empty'):
            client, deadline = self.replica_client(ctx), time.monotonic() + timeout_s
            where = self.where_tables()
            # TTL expiry empties the part by a merge; the empty result and the source are then removed.
            while _count(client, f'SELECT count() FROM system.parts WHERE {where}'):
                if time.monotonic() >= deadline:
                    raise TimeoutError(f'{self.name}: {p["until_empty"]} not empty in {timeout_s} s')
                time.sleep(0.5)
        if p.get('table_dropped'):
            st = self.state(ctx)
            for client in ctx.cluster.clients:
                client.query('SYSTEM FLUSH LOGS')
            st['rows'] = [0]
            st['parts'], st['layout'] = [], {}
            gone, deadline = self.expected_gone(ctx), time.monotonic() + timeout_s
            while True:
                st['live_keys'] = {ident[1] for ident in ctx.replay().upper.objects}
                if not gone & st['live_keys'] or time.monotonic() >= deadline:
                    break
                time.sleep(POLL_S)
            return
        super().complete(ctx, timeout_s)

    def expected_gone(self, ctx) -> set:
        return self.dropped_keys(ctx) if object_per_file(ctx.policy) else set()

    def dropped_keys(self, ctx) -> set:
        st, scope = self.state(ctx), self.p(ctx)['dropped']
        table, _, partition = scope.partition('/')
        parts = {f'{p["table"]}/{p["name"]}' for p in st['parts_before']
                 if p['table'] == table and (not partition or p['partition_id'] == partition)}
        return {key for part in parts for key, _ in st['layout_before'].get(part, {}).values()}

    def observe(self, ctx, draft):
        out = super().observe(ctx, draft)
        return {**out, 'dropped_objects': len(self.dropped_keys(ctx))}

    def assertions(self, ctx, draft):
        out = super().assertions(ctx, draft)
        if object_per_file(ctx.policy):
            keys = self.dropped_keys(ctx)
            out['dropped_objects>0'] = (True, bool(keys))
            out['dropped_objects_deleted'] = ([], sorted(keys & self.state(ctx)['live_keys'])[:5])
        return out


@register
class ColdSelect(S3Card):
    """A restart, then one SELECT: GetObject requests and bytes of a cold read.

    Setup: DDL, inserts, `Cluster.stop/start` of the replica (memory caches empty), a `count()` that waits for the
    table to load, and with `params.warm_passes` that many runs of the query (the filesystem cache, when the policy
    has one, holds the read ranges). The window holds one more run of the query.
    """

    name = 'cold-select'

    def restart(self, ctx) -> float:
        index = int(self.p(ctx)['replica'][1:])
        ctx.cluster.stop(index)
        seconds = ctx.cluster.start(index)
        self.state(ctx)['restarts'] = self.state(ctx).get('restarts', 0) + 1
        return seconds

    def select(self, ctx) -> str:
        return self.replica_client(ctx).query(self.p(ctx)['query'].format(db=DATABASE),
                                              settings=ctx.manifest['settings']['query'])

    def setup(self, ctx):
        super().setup(ctx)
        self.run_inserts(ctx, self.p(ctx)['inserts'])
        self.wait_drained(ctx, ctx.manifest['timeout_s'])
        self.restart(ctx)
        client = self.replica_client(ctx)
        for t in self.tables(ctx):
            client.query(f'SELECT count() FROM {DATABASE}.{t}')
        st, replica = self.state(ctx), self.p(ctx)['replica']
        st['cache_before_warm'] = tree_allocated(self.cache_dir(ctx))
        seq0 = self.last_seq(ctx)
        for _ in range(self.p(ctx).get('warm_passes', 0)):
            self.select(ctx)
        rows = self.rows_after(ctx, seq0)
        st['warm'] = {'get_requests': sum(1 for r in rows if r['op'] == 'GetObject'),
                      'get_bytes': sum(r['resp_payload_bytes'] for r in rows if r['op'] == 'GetObject'),
                      'cache_allocated_after': tree_allocated(self.cache_dir(ctx))}
        st['restarted_before_window'] = st['restarts']

    def cache_dir(self, ctx) -> str:
        # CACHE_DISK in yardstick/config.py: `<disk>_cache/`, which the server resolves under the replica's `caches/`.
        return os.path.join(ctx.pass_dir, self.p(ctx)['replica'], 'caches', ctx.policy.removesuffix('_cached') + '_cache')

    def last_seq(self, ctx) -> int:
        if ctx.proxy is None:
            return 0
        ctx.proxy.flush()
        return max((r['seq'] for r in L.read_rows(ctx.proxy.ledger_path) if r.get('kind') == 'attempt'), default=0)

    def rows_after(self, ctx, seq0: int) -> list:
        if ctx.proxy is None:
            return []
        ctx.proxy.flush()
        return [r for r in L.read_rows(ctx.proxy.ledger_path) if r.get('kind') == 'attempt' and r['seq'] > seq0
                and r['caller'] == self.p(ctx)['replica'] and r['upstream']['outcome'] == 'ok']

    def measured(self, ctx):
        if not self.state(ctx).get('restarted_before_window'):
            raise RuntimeError(f'{self.name}: the window must follow a restart')
        self.state(ctx)['result'] = self.select(ctx).strip()
        return {'unit_count': 1, 'effective_counts': {'selects': 1}, 'observations': {}}

    def observe(self, ctx, draft):
        st = self.state(ctx)
        parts = [p for p in st['parts'] if p['active']]
        rows = [r for r in window_rows(ctx) if r['caller'] == self.p(ctx)['replica'] and r['op'] == 'GetObject'
                and r['upstream']['outcome'] == 'ok' and r['seq'] <= draft['window']['end_seq']]
        gets, nbytes = len(rows), sum(r['resp_payload_bytes'] for r in rows)
        by_file = {}
        keys = {key: f for files in st['layout'].values() for f, (key, _) in files.items()}
        for r in rows:
            name = keys.get(r['key'], '?')
            by_file[name] = by_file.get(name, 0) + 1
        return {'parts': self.part_view(parts), 'result': st['result'], 'get_requests': gets, 'get_bytes': nbytes,
                'bytes_per_get': nbytes / gets if gets else None, 'gets_by_file': dict(sorted(by_file.items())),
                'requests_by_file': self.requests_by_file(ctx, draft),
                'restarts_before_window': st['restarted_before_window'], 'warm': st['warm'],
                'warm_get_requests': st['warm']['get_requests'],
                'warm_get_bytes_per_part': st['warm']['get_bytes'] / len(parts) if parts else None,
                'cache_fill_io_per_byte': ((st['warm']['cache_allocated_after'] - st['cache_before_warm'])
                                           / st['warm']['get_bytes'] if st['warm']['get_bytes'] else None),
                'cache_allocated_before_warm': st['cache_before_warm']}

    def assertions(self, ctx, draft):
        out = super().assertions(ctx, draft)
        out['restarted_before_window'] = (1, draft['observations']['restarts_before_window'])
        return out


@register
class Startup(S3Card):
    """`Cluster.stop` then `Cluster.start` of a replica with intact state, inside the window.

    The window ends after a `count()` per table, which waits for the tables to load. `startup_seconds` is the
    rig's start-to-answer time plus that wait.
    """

    name = 'startup'

    def setup(self, ctx):
        super().setup(ctx)
        self.run_inserts(ctx, self.p(ctx)['inserts'])
        self.wait_drained(ctx, ctx.manifest['timeout_s'])

    def measured(self, ctx):
        index, st = int(self.p(ctx)['replica'][1:]), self.state(ctx)
        ctx.cluster.stop(index)
        t0 = time.monotonic()
        st['start_seconds'] = ctx.cluster.start(index)
        client = self.replica_client(ctx)
        for t in self.tables(ctx):
            client.query(f'SELECT count() FROM {DATABASE}.{t}')
        st['startup_seconds'] = time.monotonic() - t0
        return {'unit_count': 1, 'effective_counts': {'restarts': 1}, 'observations': {}}

    def observe(self, ctx, draft):
        st = self.state(ctx)
        parts = [p for p in st['parts'] if p['active']]
        return {'parts': self.part_view(parts), 'startup_seconds': st['startup_seconds'],
                'start_seconds': st['start_seconds'], 'active_parts': len(parts),
                'requests_by_file': self.requests_by_file(ctx, draft),
                'objects_total': sum(self.object_counts(st['layout'], parts).values())}

    def assertions(self, ctx, draft):
        out = super().assertions(ctx, draft)
        out['startup_seconds>0'] = (True, draft['observations']['startup_seconds'] > 0)
        return out


@register
class MergeOnly(OperationCard):
    """Inserts with merges stopped in the setup; the window holds only `OPTIMIZE TABLE … FINAL`.

    The result is read from `system.parts`; removal of the sources waits for `old_parts_lifetime`, outside the window.
    """

    name = 'merge-only'

    def observe(self, ctx, draft):
        out = super().observe(ctx, draft)
        st = self.state(ctx)
        sources = [p for p in st['parts_before'] if p['active']]
        results = [p for p in st['parts'] if p['active']]
        return {**out, 'sources': len(sources), 'source_types': sorted({p['part_type'] for p in sources}),
                'results': self.part_view(results)}

    def assertions(self, ctx, draft):
        out = super().assertions(ctx, draft)
        out['one_result'] = (1, len(draft['observations']['results']))
        return out
