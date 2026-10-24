import unittest

from yardstick.cost import cards
from yardstick.cost.cards import base
from yardstick.cost.cards.base import CardContext, CostCard


class FakeClient:
    def __init__(self, log, n, answers=None):
        self.log, self.n, self.answers = log, n, answers or {}

    def query(self, sql, settings=None, timeout=3600):
        self.log.append((self.n, sql))
        for needle, answer in self.answers.items():
            if needle in sql:
                return answer
        return '0\n'


class FakeCluster:
    def __init__(self, replicas=2, answers=None):
        self.log = []
        self.clients = [FakeClient(self.log, i, answers) for i in range(1, replicas + 1)]


def make_ctx(cluster, **kw):
    fields = dict(cluster=cluster, proxy=None, rig=None, pass_dir='/p', local_dirs=[], policy='p', variant='v',
                  cache_enabled=False, manifest={}, replay=lambda: None, window=lambda: None)
    fields.update(kw)
    return CardContext(**fields)


class TwoTables(CostCard):
    name = 'two'
    policies = ('p',)
    mode = 'deterministic'
    replicas = 2

    def tables(self, ctx):
        return ['t1', 'other.t2']

    def expected(self, ctx):
        return {'t1': 10, 'other.t2': 0}


class DefaultsTest(unittest.TestCase):
    def test_cards_registry_holds_the_phase0_lifecycle_and_s3_cards(self):
        # Every research report adds its own card module; each module's tests pin its own cards.
        self.assertLessEqual({*['amplification-run', 'async-inserts', 'big-insert-multipart',
                                               'boundaries', 'cold-select', 'drop', 'gc-scale', 'io-sizes', 'merge-forced',
                                               'merge-natural', 'mutation', 'objects-per-part', 'probe_cas_expect',
                                               'probe_cas_gc_cohort', 'probe_local_volume', 'probe_multipart_fault',
                                               'probe_replicated_fetch', 'probe_s3_insert', 'small-inserts', 'startup']},
                             set(cards.CARDS))

    def test_class_defaults(self):
        card = TwoTables()
        self.assertEqual((card.priority, card.trace, card.expected_exceptions, card.params_schema),
                         ('P1', False, (), {}))
        self.assertEqual(CostCard.replicas, 1)
        ctx = make_ctx(FakeCluster())
        self.assertEqual(card.controls(ctx), {'merges': 'stop', 'gc': 'off', 'ttl': 'none'})
        self.assertEqual(card.inventory_families(ctx), [])
        self.assertEqual(card.supports('p'), (True, ''))
        self.assertEqual(card.supports('q')[0], False)
        self.assertIn('q', card.supports('q')[1])

    def test_default_settings_follow_merge_control(self):
        card = TwoTables()
        ctx = make_ctx(FakeCluster())
        self.assertEqual(card.settings(ctx), {'table': {'max_parts_to_merge_at_once': 1}, 'query': {}, 'server': {}})
        card.controls = lambda ctx: {'merges': 'measure', 'gc': 'off', 'ttl': 'none'}
        self.assertEqual(card.settings(ctx), {'table': {}, 'query': {}, 'server': {}})

    def test_default_teardown_drops_database_on_every_replica_last_first(self):
        cluster = FakeCluster()
        TwoTables().teardown(make_ctx(cluster))
        sql = 'DROP DATABASE IF EXISTS yardstick SYNC'
        self.assertEqual(cluster.log, [(2, sql), (1, sql)])


class CompleteTest(unittest.TestCase):
    def test_default_complete_queries(self):
        cluster = FakeCluster()
        TwoTables().complete(make_ctx(cluster))
        for n in (1, 2):
            sqls = [s for i, s in cluster.log if i == n]
            self.assertEqual(sqls[0], 'SYSTEM SYNC REPLICA yardstick.t1 STRICT')
            self.assertEqual(sqls[1], 'SYSTEM SYNC REPLICA other.t2 STRICT')
            polls = sqls[2:-1]
            self.assertEqual(len(polls), 3)
            self.assertIn('system.replication_queue', polls[0])
            self.assertIn('system.merges', polls[1])
            self.assertIn('system.mutations', polls[2])
            self.assertIn('is_done = 0', polls[2])
            for poll in polls:
                self.assertIn("database = 'yardstick' AND table = 't1'", poll)
                self.assertIn("database = 'other' AND table = 't2'", poll)
            self.assertEqual(sqls[-1], 'SYSTEM FLUSH LOGS')

    def test_complete_times_out_while_queue_not_empty(self):
        cluster = FakeCluster(answers={'system.replication_queue': '3\n'})
        with self.assertRaises(TimeoutError):
            TwoTables().complete(make_ctx(cluster), timeout_s=0)


class AssertionsTest(unittest.TestCase):
    def test_default_assertions_require_nonzero_rows(self):
        cluster = FakeCluster(answers={'FROM yardstick.t1': '0\n', 'FROM other.t2': '0\n'})
        got = TwoTables().assertions(make_ctx(cluster), {})
        self.assertEqual(got['rows.t1.r1'], (10, 0))
        self.assertEqual(got['nonzero.t1.r1'], (True, False))
        self.assertEqual(got['rows.other.t2.r2'], (0, 0))
        self.assertNotIn('nonzero.other.t2.r1', got)

    def test_assertions_pass_when_counts_match(self):
        cluster = FakeCluster(answers={'FROM yardstick.t1': '10\n'})
        got = TwoTables().assertions(make_ctx(cluster), {})
        self.assertTrue(all(e == a for e, a in got.values()))


class Boom(Exception):
    pass


class Raiser(CostCard):
    name = 'raiser'
    policies = ('p',)
    mode = 'deterministic'
    expected_exceptions = (Boom,)

    def measured(self, ctx):
        raise Boom('injected')


class ExceptionsTest(unittest.TestCase):
    def test_expected_exceptions_are_observations(self):
        result = base.run_measured(Raiser(), make_ctx(FakeCluster()))
        self.assertEqual(result['unit_count'], 0)
        self.assertEqual(result['ops'], [])
        self.assertEqual(result['observations']['expected_exceptions'], [{'type': 'Boom', 'message': 'injected'}])

    def test_undeclared_exception_propagates(self):
        card = Raiser()
        card.expected_exceptions = ()
        with self.assertRaises(Boom):
            base.run_measured(card, make_ctx(FakeCluster()))

    def test_normal_result_is_returned_unchanged(self):
        card = Raiser()
        card.measured = lambda ctx: {'unit_count': 5, 'ops': [1], 'observations': {'a': 1}}
        self.assertEqual(base.run_measured(card, make_ctx(FakeCluster())),
                         {'unit_count': 5, 'ops': [1], 'observations': {'a': 1}})


class ReplicasTest(unittest.TestCase):
    def test_replicas_precedence(self):
        card = TwoTables()
        self.assertEqual(base.check_replicas(card, 2), (True, ''))
        self.assertEqual(base.check_replicas(card, 3), (True, ''))
        self.assertEqual(base.check_replicas(card, 1), (False, 'UNSUPPORTED (needs 2 replicas)'))


class EngineTest(unittest.TestCase):
    def test_replicated_engine_clause(self):
        self.assertEqual(
            base.replicated_engine('cas', '/clickhouse/t/a'),
            "ENGINE = ReplicatedMergeTree('/clickhouse/t/a', '{replica}') ORDER BY k SETTINGS storage_policy = 'cas', "
            "primary_key_lazy_load = 0, prewarm_mark_cache = 0, prewarm_primary_key_cache = 0")
        self.assertTrue(base.replicated_engine('cas', '/z', 'old_parts_lifetime = 1').endswith(
            'prewarm_primary_key_cache = 0, old_parts_lifetime = 1'))


if __name__ == '__main__':
    unittest.main()
