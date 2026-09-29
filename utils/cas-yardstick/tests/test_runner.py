import os
import shutil
import tempfile
import unittest

from yardstick import report as R
from yardstick import runner
from yardstick.cards.base import Card

GET_PER_CALL = 7
ALL_COUNTERS = sorted({c for cs in R.AGGREGATES.values() for c in cs} | set(R.CAS_DIAGNOSTICS) | set(R.S3_ERROR_COUNTERS))


class FakeClient:
    """Every events() call advances each counter; DiskS3GetObject by GET_PER_CALL."""
    def __init__(self):
        self.queries = []
        self.calls = 0

    def query(self, sql, settings=None, timeout=3600):
        self.queries.append(sql)
        return ''

    def events(self):
        self.calls += 1
        return {c: self.calls * (GET_PER_CALL if c == 'DiskS3GetObject' else 1) for c in ALL_COUNTERS}

    def settle(self, table, timeout=60):
        self.queries.append(f'settle {table}')

    def drop_caches(self):
        self.queries.append('drop caches')


class FakeServer:
    def __init__(self):
        self.starts = []
        self.stops = []

    def start(self, run_dir):
        self.starts.append(run_dir)

    def stop(self, run_dir):
        self.stops.append(run_dir)


class FakeCard(Card):
    unit = 'op'
    stops_merges = False
    COUNTS = {'ops': 4}

    def __init__(self, name, has_hot=True, fail_in=None, supported=(True, '')):
        self.name = name
        self.has_hot = has_hot
        self.fail_in = fail_in
        self.supported = supported
        self.calls = []

    def supports(self, policy):
        return self.supported

    def setup(self, c, policy, counts):
        self.calls.append('setup')

    def cold_reset_needed(self):
        return False

    def measured(self, c, policy, counts, phase):
        self.calls.append(f'measured {phase}')
        if self.fail_in == phase:
            raise RuntimeError('boom')
        return {'unit_count': counts['ops'], 'ops': [{'n': i, 'ms': 10.0 * (i + 1)} for i in range(counts['ops'])]}

    def invariants(self, c, counts, phase, when):
        return {'rows': (1, 1)}

    def teardown(self, c):
        self.calls.append('teardown')


class RunOneTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        self.server = FakeServer()
        self.prefixes = []

        def write_config(run_dir, policy, endpoint, prefix, gc_enabled):
            self.prefixes.append(prefix)
            return 'hash-' + policy

        self.deps = runner.Deps(run_id='R1', runs_dir=self.dir, endpoint='http://127.0.0.1:1', gc=True, scale=1.0,
                                server=self.server, client_for=lambda run_dir: FakeClient(),
                                write_config=write_config, config_hashes={'cas': 'hash-cas', 's3': 'hash-s3'})

    def rows(self, rows, **match):
        return [r for r in rows if all(r[k] == v for k, v in match.items())]

    def test_failed_measured_still_tears_down_and_next_card_runs(self):
        bad = FakeCard('bad', fail_in='cold')
        good = FakeCard('good')
        rows = runner.run_matrix(['cas'], [bad, good], 1, self.deps)
        for phase in ('cold', 'hot'):
            self.assertEqual([r['value'] for r in self.rows(rows, card='bad', phase=phase)], ['FAILED'])
        self.assertIn('teardown', bad.calls)
        self.assertEqual(len(self.server.stops), 2)
        self.assertEqual(self.server.starts, self.server.stops)
        self.assertEqual(good.calls, ['setup', 'measured cold', 'measured hot', 'teardown'])
        self.assertTrue(self.rows(rows, card='good', phase='hot', metric='wall'))
        bad_dir = runner.run_dir_for(self.dir, 'R1', 'cas', 'bad', 1, '')
        with open(os.path.join(bad_dir, 'error.txt')) as f:
            self.assertIn('boom', f.read())

    def test_no_hot_rows_without_hot_pass(self):
        rows = runner.run_one('cas', FakeCard('cold_only', has_hot=False), 1, self.deps)
        self.assertTrue(self.rows(rows, phase='cold', metric='GET'))
        self.assertEqual(self.rows(rows, phase='hot'), [])

    def test_unsupported_rows_and_no_server(self):
        rows = runner.run_one('cas', FakeCard('m', supported=(False, 'write-once')), 1, self.deps)
        self.assertEqual(sorted((r['phase'], r['metric'], r['value']) for r in rows),
                         [('cold', 'status', 'UNSUPPORTED (write-once)'), ('hot', 'status', 'UNSUPPORTED (write-once)')])
        self.assertEqual(self.server.starts, [])

    def test_get_sample_is_the_events_delta(self):
        rows = runner.run_one('s3', FakeCard('c'), 1, self.deps)
        for phase in ('cold', 'hot'):
            [get] = self.rows(rows, phase=phase, metric='GET')
            self.assertEqual((get['value'], get['unit_count'], get['repeat']), (GET_PER_CALL, 4, 1))
        [median] = self.rows(rows, phase='cold', metric='median_op_ms')
        self.assertEqual(median['value'], 25.0)
        self.assertEqual(self.rows(rows, metric='CASRootGet'), [])  # CAS diagnostics on CAS policies only

    def test_prefix_is_fresh_per_repeat(self):
        runner.run_one('cas', FakeCard('c'), 1, self.deps)
        runner.run_one('cas', FakeCard('c'), 2, self.deps)
        self.assertEqual(self.prefixes, ['test/R1/cas/c/k1', 'test/R1/cas/c/k2'])

    def test_config_hash_mismatch_fails_the_pass(self):
        self.deps.config_hashes['cas'] = 'other'
        rows = runner.run_one('cas', FakeCard('c'), 1, self.deps)
        self.assertEqual({r['value'] for r in rows}, {'FAILED'})

    def test_policy_rotation(self):
        self.assertEqual(runner.policy_order(['a', 'b', 'c'], 1), ['a', 'b', 'c'])
        self.assertEqual(runner.policy_order(['a', 'b', 'c'], 2), ['b', 'c', 'a'])
        self.assertEqual(runner.policy_order(['a', 'b', 'c'], 4), ['a', 'b', 'c'])


class CardCountsTest(unittest.TestCase):
    def test_counts_scale_with_floor_of_one(self):
        card = FakeCard('c')
        self.assertEqual(card.counts(1.0), {'ops': 4})
        self.assertEqual(card.counts(0.01), {'ops': 1})


if __name__ == '__main__':
    unittest.main()
