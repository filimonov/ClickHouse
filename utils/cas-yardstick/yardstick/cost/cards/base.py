"""The `CostCard` interface: one scenario of the cost rig, with defaults for the common lifecycle."""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable

DATABASE = 'yardstick'
POLL_S = 0.5


@dataclass
class CardContext:
    cluster: object
    proxy: object
    rig: object
    pass_dir: str
    local_dirs: list
    policy: str
    variant: str
    cache_enabled: bool
    manifest: dict
    replay: Callable
    window: Callable


def replicated_engine(policy: str, zk_path: str, extra_settings: str = '') -> str:
    extra = f', {extra_settings}' if extra_settings else ''
    return (f"ENGINE = ReplicatedMergeTree('{zk_path}', '{{replica}}') ORDER BY k SETTINGS storage_policy = '{policy}', "
            f'primary_key_lazy_load = 0, prewarm_mark_cache = 0, prewarm_primary_key_cache = 0{extra}')


def check_replicas(card, available: int) -> tuple:
    """The card's own replica requirement wins over a smaller CLI `--replicas`."""
    if available >= card.replicas:
        return True, ''
    return False, f'UNSUPPORTED (needs {card.replicas} replicas)'


def run_measured(card, ctx) -> dict:
    """Run `card.measured`; an exception the card declared is reported as an observation, not a failure."""
    try:
        return card.measured(ctx)
    except card.expected_exceptions as e:
        return {'unit_count': 0, 'ops': [],
                'observations': {'expected_exceptions': [{'type': type(e).__name__, 'message': str(e)}]}}


def _qualified(table: str) -> tuple:
    return tuple(table.split('.', 1)) if '.' in table else (DATABASE, table)


class CostCard:
    name: str
    policies: tuple = ()
    mode: str = 'deterministic'
    replicas: int = 1
    priority: str = 'P1'
    params_schema: dict = {}
    expected_exceptions: tuple = ()
    trace: bool = False

    def tables(self, ctx) -> list:
        raise NotImplementedError

    def controls(self, ctx) -> dict:
        return {'merges': 'stop', 'gc': 'off', 'ttl': 'none'}

    def settings(self, ctx) -> dict:
        table = {'max_parts_to_merge_at_once': 1} if self.controls(ctx)['merges'] == 'stop' else {}
        return {'table': table, 'query': {}, 'server': {}}

    def supports(self, policy: str) -> tuple:
        if policy in self.policies:
            return True, ''
        return False, f'policy {policy} not in {self.policies}'

    def setup(self, ctx) -> None:
        raise NotImplementedError

    def measured(self, ctx) -> dict:
        raise NotImplementedError

    def _queue_empty(self, ctx) -> bool:
        where = ' OR '.join(f"(database = '{db}' AND table = '{t}')" for db, t in map(_qualified, self.tables(ctx)))
        polls = (f'SELECT count() FROM system.replication_queue WHERE {where}',
                 f'SELECT count() FROM system.merges WHERE {where}',
                 f'SELECT count() FROM system.mutations WHERE is_done = 0 AND ({where})')
        return all(int(client.query(sql).strip()) == 0 for client in ctx.cluster.clients for sql in polls)

    def complete(self, ctx, timeout_s: float = 300) -> None:
        for client in ctx.cluster.clients:
            for table in self.tables(ctx):
                client.query('SYSTEM SYNC REPLICA %s.%s STRICT' % _qualified(table))
        deadline = time.monotonic() + timeout_s
        while not self._queue_empty(ctx):
            if time.monotonic() >= deadline:
                raise TimeoutError(f'{self.name}: replication queue, merges or mutations not drained in {timeout_s} s')
            time.sleep(POLL_S)
        for client in ctx.cluster.clients:
            client.query('SYSTEM FLUSH LOGS')

    def expected(self, ctx) -> dict:
        raise NotImplementedError

    def assertions(self, ctx, draft) -> dict:
        result = {}
        for table, rows in self.expected(ctx).items():
            for n, client in enumerate(ctx.cluster.clients, 1):
                actual = int(client.query('SELECT count() FROM %s.%s' % _qualified(table)).strip())
                result[f'rows.{table}.r{n}'] = (rows, actual)
                if rows:
                    result[f'nonzero.{table}.r{n}'] = (True, actual > 0)
        return result

    def inventory_families(self, ctx) -> list:
        return []

    def teardown(self, ctx) -> None:
        ctx.cluster.clients[0].query(f'DROP DATABASE IF EXISTS {DATABASE} SYNC')
