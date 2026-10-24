"""Replication cards: how a follower acquires each result type (fetch or local execution) and what it costs.

Every parameter comes from the manifest (`experiments/<experiment_id>.json` of the replication report); expected
request counts and observation bounds come from `predictions/<experiment_id>.<policy>.json` through
`params.report_dir`. Tables are `ReplicatedMergeTree` on every replica of the card; `params.replica` (the
originator) runs the workload, `params.follower` is the replica whose acquisition path the card asserts.

Acquisition evidence is per result part from `system.part_log` of each replica: `NewPart`, `MergeParts` and
`MutatePart` are local execution, `DownloadPart` is a fetch. A fetched part's result type is the type of the
event that produced it on the replica that executed it.
"""
from __future__ import annotations

import json
import time

from .. import ledger as L
from .base import DATABASE, POLL_S
from .cas_probe import CAS_FAMILIES
from .lifecycle import LifecycleCard, check_observation
from .probes import ManifestCard, _count, _sql, compare_ledger, register, window_rows

LOCAL_EVENTS = {'NewPart': 'insert', 'MergeParts': 'merge', 'MutatePart': 'mutation'}
FETCH_EVENT = 'DownloadPart'
RESULT_TYPES = ('insert', 'merge', 'mutation')
COUNTERS = ('ReplicatedPartFetches', 'ReplicatedPartFetchesOfMerged', 'ReplicatedPartMerges',
            'ReplicatedPartMutations', 'ReplicatedPartFailedFetches')
PART_LOG_SQL = ("SELECT event_type, part_name, merged_from, error, size_in_bytes, "
                "toUnixTimestamp64Micro(event_time_microseconds) AS t_us, duration_ms "
                "FROM system.part_log WHERE database = '{db}' AND table = '{table}' "
                "AND event_type IN ('NewPart', 'MergeParts', 'MutatePart', 'DownloadPart') "
                "AND toUnixTimestamp64Micro(event_time_microseconds) >= {t0} ORDER BY t_us FORMAT JSONEachRow")


def json_rows(client, sql: str) -> list:
    return [json.loads(line) for line in client.query(sql).splitlines() if line.strip()]


def acquisitions(part_logs: dict) -> dict:
    """Per replica and result type, the result parts it executed locally and the ones it fetched.

    `part_logs` maps a replica name to its successful part-log rows. A fetched part whose producing event is on no
    replica's log (it was produced before the window) is typed `unknown`.
    """
    origin = {}
    for rows in part_logs.values():
        for r in rows:
            if r['event_type'] in LOCAL_EVENTS:
                origin.setdefault(r['part_name'], LOCAL_EVENTS[r['event_type']])
    out = {}
    for replica, rows in part_logs.items():
        mine = out.setdefault(replica, {t: {'local': [], 'fetch': []} for t in RESULT_TYPES + ('unknown',)})
        for r in rows:
            if r['event_type'] in LOCAL_EVENTS:
                mine[LOCAL_EVENTS[r['event_type']]]['local'].append(r['part_name'])
            elif r['event_type'] == FETCH_EVENT:
                mine[origin.get(r['part_name'], 'unknown')]['fetch'].append(r['part_name'])
        for paths in mine.values():
            for k in paths:
                paths[k] = sorted(paths[k])
    return out


def originators(part_logs: dict) -> dict:
    """result part -> the replica whose local event for it is earliest (the one that executed it first)."""
    first = {}
    for replica, rows in part_logs.items():
        for r in rows:
            if r['event_type'] in LOCAL_EVENTS:
                t = int(r['t_us'])
                if r['part_name'] not in first or t < first[r['part_name']][0]:
                    first[r['part_name']] = (t, replica)
    return {name: replica for name, (_, replica) in first.items()}


def follower_split(part_logs: dict) -> dict:
    """Per result type, how the replicas other than a part's originator acquired it.

    `fetch_fraction` = fetches / (fetches + local executions) over those follower acquisitions; `None` when the
    type has no follower acquisition. A covered part a follower never acquired counts in neither.
    """
    acq, orig = acquisitions(part_logs), originators(part_logs)
    out = {t: {'fetch': 0, 'local': 0} for t in RESULT_TYPES}
    for replica, types in acq.items():
        for t in RESULT_TYPES:
            for kind in ('fetch', 'local'):
                out[t][kind] += sum(1 for name in types[t][kind] if orig.get(name) != replica)
    for t, c in out.items():
        n = c['fetch'] + c['local']
        c['fetch_fraction'] = c['fetch'] / n if n else None
    return out


def request_roles(rows: list, originator: str) -> dict:
    """Per replica and role, request counts per op.

    The originator's reads of objects it created in the window are the sender side of a fetch (`sender`); its other
    requests are its own execution (`own`). Every request of another replica is `acquire`: fetches and load reads in
    the fetch cases, the replica's own merge or mutation in the local cases.
    """
    created = {(r['bucket'], r['key']) for r in rows if r['caller'] == originator and r['upstream']['outcome'] == 'ok'
               and r['op'] in ('PutObject', 'CompleteMultipartUpload')}
    out = {}
    for r in rows:
        if r['caller'] == originator:
            role = 'sender' if r['op'] in ('GetObject', 'HeadObject') and (r['bucket'], r['key']) in created else 'own'
        else:
            role = 'acquire'
        ops = out.setdefault(r['caller'], {}).setdefault(role, {})
        ops[r['op']] = ops.get(r['op'], 0) + 1
    return {c: {role: dict(sorted(ops.items())) for role, ops in sorted(roles.items())} for c, roles in sorted(out.items())}


def counter_view(events: dict, replicas: list) -> dict:
    return {r: {c: int(events.get(r, {}).get(c, 0)) for c in COUNTERS} for r in replicas}


def requests_by_caller(foreground: dict) -> dict:
    return {caller: {op: c['attempts'] for op, c in sorted(ops.items()) if c['attempts']}
            for caller, ops in sorted(foreground.items())}


class ReplicationCard(LifecycleCard):
    """`ReplicatedMergeTree` on every replica of the card; the originator `params.replica` runs the workload.

    `params.replica_settings` adds table settings per replica (`always_fetch_merged_part` on the follower forces
    the fetch path); manifest table settings apply to every replica.
    """

    policies = ('s3', 's3plain', 'cas', 'local')
    replicas = 2
    replicated = True

    def inventory_families(self, ctx):
        # CAS lease and GC control objects are maintenance by key family; GC is off in these cards.
        return CAS_FAMILIES if ctx.policy.removesuffix('_cached') == 'cas' else []

    def names(self) -> list:
        return [f'r{i}' for i in range(1, self.replicas + 1)]

    def client_of(self, ctx, name: str):
        if name not in self.names():
            raise ValueError(f'{self.name}: {name!r} is not one of the card\'s replicas {self.names()}')
        return ctx.cluster.clients[int(name[1:]) - 1]

    def clients(self, ctx) -> list:
        return ctx.cluster.clients[:self.replicas]

    def follower(self, ctx) -> str:
        return self.p(ctx)['follower']

    def ddl(self, ctx, replica: str) -> str:
        p = self.p(ctx)
        table = {'storage_policy': ctx.policy, 'primary_key_lazy_load': 0, 'prewarm_mark_cache': 0,
                 'prewarm_primary_key_cache': 0, **ctx.manifest['settings']['table'],
                 **p.get('replica_settings', {}).get(replica, {})}
        return (f"CREATE TABLE {DATABASE}.{self.table} ({p['columns']}) ENGINE = ReplicatedMergeTree("
                f"'/yardstick/{self.table}', '{{replica}}') ORDER BY {p.get('order_by', 'k')} SETTINGS "
                + ', '.join(f'{k} = {_sql(v)}' for k, v in table.items()))

    def setup(self, ctx):
        for name in self.names():
            client = self.client_of(ctx, name)
            client.query(f'CREATE DATABASE IF NOT EXISTS {DATABASE}')
            client.query(self.ddl(ctx, name))
        n = self.p(ctx).get('setup_inserts', 0)
        for i in range(n):
            self.insert(ctx, i)
        if n:
            self.sync(ctx)

    def sync(self, ctx, timeout_s: float = 300) -> None:
        """Every replica holds every part: SYNC REPLICA STRICT, then no queue, merge or mutation left."""
        ManifestCard.complete(self, ctx, timeout_s)

    def statements(self, ctx) -> int:
        client = self.replica_client(ctx)
        for s in self.p(ctx).get('statements', []):
            client.query(s['sql'].format(table=f'{DATABASE}.{self.table}'),
                         settings={**ctx.manifest['settings']['query'], **s.get('settings', {})})
        return len(self.p(ctx).get('statements', []))

    def measured(self, ctx):
        p = self.p(ctx)
        if p['window'] == 'inserts':
            n = self.inserts(ctx)
            for i in range(p.get('setup_inserts', 0), p.get('setup_inserts', 0) + n):
                self.insert(ctx, i)
            return {'unit_count': n, 'effective_counts': {'inserts': n, 'rows': n * p['rows']}, 'observations': {}}
        if p['window'] == 'statements':
            n = self.statements(ctx)
            return {'unit_count': n, 'effective_counts': {'statements': n}, 'observations': {}}
        raise ValueError(f'{self.name}: unknown params.window {p["window"]!r}')

    def part_logs(self, ctx) -> dict:
        t0 = ctx.window()['t_start_us']
        out = {}
        for name in self.names():
            rows = json_rows(self.client_of(ctx, name), PART_LOG_SQL.format(db=DATABASE, table=self.table, t0=t0))
            out[name] = [r for r in rows if int(r['error']) == 0]
        return out

    def complete(self, ctx, timeout_s=300):
        self.sync(ctx, timeout_s)
        st = self.state(ctx)
        st['part_logs'] = self.part_logs(ctx)
        st['active'] = {name: sorted(self.client_of(ctx, name).query(
            f"SELECT name FROM system.parts WHERE database = '{DATABASE}' AND table = '{self.table}' AND active "
            'ORDER BY name').split()) for name in self.names()}

    def observe(self, ctx, draft):
        st = self.state(ctx)
        logs = st['part_logs']
        families = self.inventory_families(ctx)
        # Foreground rows only, as the ledger summary counts them: CAS lease and GC control rows are maintenance.
        rows = [r for r in window_rows(ctx) if r['seq'] <= draft['window']['end_seq'] and r['caller'] in self.names()
                and not L.is_maintenance(r, families)]
        fetched = [r for r in logs[self.follower(ctx)] if r['event_type'] == FETCH_EVENT]
        return {'acquisitions': acquisitions(logs), 'follower_split': follower_split(logs),
                'request_roles': request_roles(rows, self.p(ctx)['replica']),
                'fetched_parts': len(fetched), 'fetched_bytes_on_disk': sum(int(r['size_in_bytes']) for r in fetched),
                'counters': counter_view(draft['events'], self.names()), 'active_parts': st['active'],
                'requests': requests_by_caller(draft['ledger']['foreground']),
                'interserver': {k: {d: v[d] for d in ('from_target', 'to_target')} for k, v in draft['interserver'].items()},
                'rows': dict(zip(self.names(), st.get('rows', [])))}

    def common_assertions(self, ctx, draft) -> dict:
        pred = self.predicted(ctx)
        out = compare_ledger(pred, draft['ledger']['foreground'], self.mode == 'deterministic')
        for key, want in pred['expected']['observations'].items():
            out[f'observation.{key}'] = check_observation(want, draft['observations'].get(key), draft['observations'])
        rows = self.p(ctx)['expected_rows']
        for name, actual in zip(self.names(), self.state(ctx).get('rows', [])):
            out[f'rows.{name}'] = (rows, actual)
        active = self.state(ctx)['active']
        out['active_parts.equal'] = (active[self.names()[0]], active[self.follower(ctx)])
        if self.mode == 'deterministic':
            out['window.crossing'] = ([], draft['window']['crossing'])
        return out

    def assertions(self, ctx, draft):
        return self.common_assertions(ctx, draft)


def path_assertions(acq: dict, counters: dict, relays: dict, originator: str, follower: str, expected: dict) -> dict:
    """The acquisition path of every result type in `expected` ({type: 'fetch'|'local'}) on the follower.

    The originator executes each result locally; the follower fetches exactly those results (fetch) or executes
    each of them itself (local). Counters are secondary evidence; relay bytes show who served a fetch (relay0
    fronts r1's interserver port, relay1 r2's).
    """
    out = {}
    fol, ori = acq[follower], acq[originator]
    c = counters[follower]
    fetched_any = False
    for t, path in sorted(expected.items()):
        results = ori[t]['local']
        out[f'path.{originator}.{t}.local>0'] = (True, bool(results))
        out[f'path.{originator}.{t}.fetch'] = ([], ori[t]['fetch'])
        if path == 'fetch':
            fetched_any = True
            out[f'path.{follower}.{t}.fetch'] = (results, fol[t]['fetch'])
            out[f'path.{follower}.{t}.local'] = ([], fol[t]['local'])
        elif path == 'local':
            out[f'path.{follower}.{t}.local'] = (results, fol[t]['local'])
            out[f'path.{follower}.{t}.fetch'] = ([], fol[t]['fetch'])
        else:
            raise ValueError(f'unknown acquisition path {path!r} for {t}')
        if t == 'merge':
            if path == 'local':
                out[f'counter.{follower}.ReplicatedPartFetchesOfMerged'] = (0, c['ReplicatedPartFetchesOfMerged'])
                out[f'counter.{follower}.ReplicatedPartMerges>=1'] = (True, c['ReplicatedPartMerges'] >= 1)
            else:
                out[f'counter.{follower}.ReplicatedPartFetchesOfMerged>=1'] = (True, c['ReplicatedPartFetchesOfMerged'] >= 1)
                out[f'counter.{follower}.ReplicatedPartMerges'] = (0, c['ReplicatedPartMerges'])
        elif t == 'mutation':
            if path == 'local':
                out[f'counter.{follower}.ReplicatedPartMutations>=1'] = (True, c['ReplicatedPartMutations'] >= 1)
                out[f'counter.{follower}.ReplicatedPartFetches'] = (0, c['ReplicatedPartFetches'])
            else:
                out[f'counter.{follower}.ReplicatedPartFetches>=1'] = (True, c['ReplicatedPartFetches'] >= 1)
                out[f'counter.{follower}.ReplicatedPartMutations'] = (0, c['ReplicatedPartMutations'])
        else:
            out[f'counter.{follower}.ReplicatedPartFetches'] = (len(results), c['ReplicatedPartFetches'])
    serving = f'relay{int(originator[1:]) - 1}'
    if fetched_any:
        out[f'{serving}.from_target>0'] = (True, relays[serving]['from_target'] > 0)
    else:
        out[f'{serving}.from_target'] = (0, relays[serving]['from_target'])
    nobody = f'relay{int(follower[1:]) - 1}'
    out[f'{nobody}.from_target'] = (0, relays[nobody]['from_target'])
    return out


@register
class ReplicationFetch(ReplicationCard):
    """The follower is forced to fetch (`always_fetch_merged_part` / `always_fetch_mutated_part`); inserted parts
    are always fetched. `params.expected_path` names the asserted path per result type."""

    name = 'replication-fetch'

    def assertions(self, ctx, draft):
        out = self.common_assertions(ctx, draft)
        obs = draft['observations']
        out.update(path_assertions(obs['acquisitions'], obs['counters'], obs['interserver'], self.p(ctx)['replica'],
                                   self.follower(ctx), self.p(ctx)['expected_path']))
        return out


@register
class ReplicationLocal(ReplicationFetch):
    """Default settings with every source part on the follower: the follower executes the merge or mutation
    itself and pays its own reads and writes (plan Review Focus 4)."""

    name = 'replication-local'


@register
class ReplicationZeroCopy(ReplicationFetch):
    """`allow_remote_fs_zero_copy_replication = 1` on every replica, the follower forced to fetch: the sender
    ships object references, not file contents."""

    name = 'replication-zero-copy'
    policies = ('s3',)


class NaturalReplicationCard(ReplicationCard):
    """Inserts on the originator with background merges on, a wait until no merge or queue entry has been seen on
    any replica for `params.quiet_s`, then `params.statements` (a mutation). Observes the per-type split."""

    mode = 'stochastic'

    def busy(self, ctx) -> bool:
        where = f"database = '{DATABASE}' AND table = '{self.table}'"
        sql = (f'SELECT (SELECT count() FROM system.merges WHERE {where}) + '
               f'(SELECT count() FROM system.replication_queue WHERE {where})')
        return any(_count(c, sql) for c in self.clients(ctx))

    def wait_quiet(self, ctx) -> bool:
        p = self.p(ctx)
        deadline = time.monotonic() + p['quiesce_timeout_s']
        quiet_since = time.monotonic()
        while time.monotonic() < deadline:
            if self.busy(ctx):
                quiet_since = time.monotonic()
            elif time.monotonic() - quiet_since >= p['quiet_s']:
                return True
            time.sleep(POLL_S)
        return False

    def measured(self, ctx):
        p, n = self.p(ctx), self.inserts(ctx)
        for i in range(n):
            self.insert(ctx, i)
        st = self.state(ctx)
        st['quiesced'] = self.wait_quiet(ctx)
        # A variant whose capability gate refuses the statements (s3plain: no mutation) skips them by name, cited in
        # the manifest; the record says so and the mutation split stays empty.
        skipped = p.get('skip_statements', {}).get(ctx.policy)
        n_statements = 0 if skipped else self.statements(ctx)
        return {'unit_count': n, 'effective_counts': {'inserts': n, 'rows': n * p['rows'], 'statements': n_statements},
                'observations': {'skipped_statements': skipped}}

    def observe(self, ctx, draft):
        out = super().observe(ctx, draft)
        split = out['follower_split']
        # Flat keys: predictions and the closure check compare top-level observations.
        return {**out, 'quiesced': self.state(ctx)['quiesced'],
                **{f'fetch_fraction_{t}': split[t]['fetch_fraction'] for t in RESULT_TYPES}}

    def assertions(self, ctx, draft):
        out = self.common_assertions(ctx, draft)
        out['quiesced'] = (True, draft['observations']['quiesced'])
        return out


@register
class ReplicationNaturalFit(NaturalReplicationCard):
    name = 'replication-natural-fit'


@register
class ReplicationNaturalHoldout(NaturalReplicationCard):
    name = 'replication-natural-holdout'


@register
class RestartCatchup(ReplicationCard):
    """The follower is stopped, the originator takes `params.inserts` paced inserts over `params.duration_s` with
    merges on and is left to quiesce, then the follower starts and catches up. All of it is the window.

    Sub-windows split the follower's traffic: from its start to the start of its first fetch is startup, the rest
    is catch-up; the originator's requests after the follower's start are the sender side of the catch-up.
    """

    name = 'restart-catchup'
    mode = 'stochastic'

    def measured(self, ctx):
        p, st = self.p(ctx), self.state(ctx)
        follower = int(self.follower(ctx)[1:])
        ctx.cluster.stop(follower)
        st['t_stop_us'] = time.time_ns() // 1000
        n, interval = self.inserts(ctx), p['duration_s'] / max(1, self.inserts(ctx))
        t0 = time.monotonic()
        base = p.get('setup_inserts', 0)
        for i in range(n):
            time.sleep(max(0.0, t0 + i * interval - time.monotonic()))  # the insert schedule is the workload
            self.insert(ctx, base + i)
        st['quiesced'] = self.wait_originator_quiet(ctx)
        st['t_start_us'] = time.time_ns() // 1000
        st['start_seconds'] = ctx.cluster.start(follower)
        return {'unit_count': n, 'effective_counts': {'inserts': n, 'rows': n * p['rows'], 'restarts': 1},
                'observations': {}}

    def wait_originator_quiet(self, ctx) -> bool:
        p, client = self.p(ctx), self.replica_client(ctx)
        where = f"database = '{DATABASE}' AND table = '{self.table}'"
        deadline, quiet_since = time.monotonic() + p['quiesce_timeout_s'], time.monotonic()
        while time.monotonic() < deadline:
            if _count(client, f'SELECT count() FROM system.merges WHERE {where}'):
                quiet_since = time.monotonic()
            elif time.monotonic() - quiet_since >= p['quiet_s']:
                return True
            time.sleep(POLL_S)
        return False

    def observe(self, ctx, draft):
        out = super().observe(ctx, draft)
        st, follower, originator = self.state(ctx), self.follower(ctx), self.p(ctx)['replica']
        fetches = [r for r in st['part_logs'][follower] if r['event_type'] == FETCH_EVENT]
        first_fetch_us = min((int(r['t_us']) - int(r['duration_ms']) * 1000 for r in fetches), default=None)
        rows = [r for r in window_rows(ctx) if r['seq'] <= draft['window']['end_seq']
                and r['caller'] in (follower, originator)]
        split = catchup_split(rows, follower, originator, st['t_start_us'], first_fetch_us)
        inserted = sum(1 for r in st['part_logs'][originator] if r['event_type'] == 'NewPart')
        return {**out, 'quiesced': st['quiesced'], 'start_seconds': st['start_seconds'],
                'parts_inserted_while_down': inserted, 'fetched_parts': len(fetches),
                'follower_local_merges': len([r for r in st['part_logs'][follower] if r['event_type'] == 'MergeParts']),
                'first_fetch_us': first_fetch_us, 't_follower_start_us': st['t_start_us'], **split,
                'catchup_per_fetched_part': ({op: n / len(fetches) for op, n in split['catchup'].items()}
                                             if fetches else None)}

    def assertions(self, ctx, draft):
        out = self.common_assertions(ctx, draft)
        obs = draft['observations']
        out['quiesced'] = (True, obs['quiesced'])
        out['fetched_parts>0'] = (True, obs['fetched_parts'] > 0)
        follower = int(self.follower(ctx)[1:])
        out['follower_restarted'] = (2, len(ctx.cluster.generations[follower]))
        return out


def catchup_split(rows: list, follower: str, originator: str, t_start_us: int, first_fetch_us) -> dict:
    """Request counts per op in the follower's startup and in the catch-up (both replicas) after its start."""
    startup, catchup = {}, {}
    for r in rows:
        if r['t_start_us'] < t_start_us:
            continue
        if r['caller'] == follower and (first_fetch_us is None or r['t_start_us'] < first_fetch_us):
            startup[r['op']] = startup.get(r['op'], 0) + 1
        else:
            catchup[r['op']] = catchup.get(r['op'], 0) + 1
    return {'startup': dict(sorted(startup.items())), 'catchup': dict(sorted(catchup.items()))}
