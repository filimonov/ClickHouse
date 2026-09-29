import contextlib
import io
import json
import os
import shutil
import tempfile
import unittest

from yardstick import compare as cmp
from yardstick import gc
from yardstick import report as R

T0, T1 = 1_000_000, 2_000_000


class FakeClient:
    def __init__(self, rows):
        self.rows = rows
        self.queries = []

    def query(self, sql, settings=None, timeout=3600):
        self.queries.append(sql)
        return '\n'.join(json.dumps(r) for r in self.rows) if sql.startswith('SELECT') else ''


def row(round_id, t0, t1, finished):
    return dict(round_id=round_id, t0=t0, t1=t1, outcome='Success', objects_deleted=5, manifests_deleted=2,
                duration_ms=30, finished=finished)


class CollectTest(unittest.TestCase):
    def test_flags_and_flush_first(self):
        c = FakeClient([row('a', T0 + 10, T1 - 10, 1), row('b', T0 - 10, T1 - 10, 1), row('c', T0 + 10, T1 + 10, 0)])
        rounds = gc.collect(c, T0, T1)
        self.assertEqual(c.queries[0], 'SYSTEM FLUSH LOGS')
        self.assertIn(f't1 >= {T0} AND t0 <= {T1}', c.queries[1])
        self.assertEqual([(r['round_id'], r['started_in_window'], r['finished_in_window']) for r in rounds],
                         [('a', True, True), ('b', False, True), ('c', True, False)])

    def test_bounds_are_compared_as_integers(self):
        c = FakeClient([])
        gc.collect(c, T0, T1)
        self.assertIn('toUnixTimestamp64Micro(min(event_time_microseconds)) AS t0', c.queries[1])
        self.assertIn('toUnixTimestamp64Micro(max(event_time_microseconds)) AS t1', c.queries[1])

    def test_no_rows(self):
        self.assertEqual(gc.collect(FakeClient([]), T0, T1), [])

    def test_finished_after_window_end_is_not_finished_in_window(self):
        rounds = gc.collect(FakeClient([row('a', T0 + 1, T1 + 1, 1)]), T0, T1)
        self.assertFalse(rounds[0]['finished_in_window'])


class CompareTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir)

    def make_run(self, name, **manifest):
        d = os.path.join(self.dir, name)
        os.makedirs(d)
        fields = {f: 'x' for f in R.COMPAT_FIELDS}
        fields.update(manifest)
        R.write_manifest(os.path.join(d, 'manifest.tsv'), fields)
        rows = [dict(policy='cas', card='cycle', phase='cold', repeat=1, metric='wall', value=2.0, unit_count=1)]
        R.write_summary(os.path.join(d, 'summary.tsv'), R.build_summary(rows))
        return d

    def compare(self, a, b):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = cmp.compare_runs(a, b)
        return code, out.getvalue()

    def test_incompatible_prints_only_the_field(self):
        a, b = self.make_run('a', scale='1.0'), self.make_run('b', scale='0.1')
        code, out = self.compare(a, b)
        self.assertEqual(out, 'incompatible: scale\n')
        self.assertEqual(code, 1)

    def test_equal_manifests_render_the_comparison(self):
        a, b = self.make_run('a'), self.make_run('b')
        code, out = self.compare(a, b)
        self.assertEqual(code, 0)
        self.assertIn('cas cycle cold wall: cur 2.0', out)


class RunnerGcTest(unittest.TestCase):
    def run_card(self, policy, gc_rows):
        from tests.test_runner import FakeCard, FakeServer
        from yardstick import runner
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        c = FakeClient(gc_rows)
        deps = runner.Deps(run_id='R', runs_dir=d, endpoint='e', gc=True, scale=1.0, server=FakeServer([]),
                           client_for=lambda run_dir: _Client(c),
                           write_config=lambda *a: 'h', config_hashes={policy: 'h'})
        rows = runner.run_one(policy, FakeCard('c', has_hot=False), 1, deps)
        return d, c, rows

    def test_cas_window_writes_rounds_and_counts(self):
        d, c, rows = self.run_card('cas', [row('a', 0, 2 ** 62, 1)])
        by = {r['metric']: r['value'] for r in rows}
        self.assertEqual((by['gc_rounds'], by['gc_rounds_overlapping']), (0, 1))
        with open(os.path.join(d, 'R', 'cas', 'c', 'k1', 'cold', 'gc_rounds.tsv')) as f:
            lines = f.read().splitlines()
        self.assertEqual(lines[1].split('\t')[0], 'a')

    def test_plain_s3_never_queries_the_gc_log(self):
        d, c, rows = self.run_card('s3', [row('a', 0, 1, 1)])
        self.assertFalse([q for q in c.queries if 'cas_gc_log' in q])
        self.assertNotIn('gc_rounds', {r['metric'] for r in rows})

    def test_table_cell_shows_overlapping(self):
        rows = [dict(policy='cas', card='c', phase='cold', repeat=1, metric=m, value=v, unit_count=1)
                for m, v in (('gc_rounds', 2), ('gc_rounds_overlapping', 1))]
        table = R.render_table(R.build_summary(rows), ['cas'], [])
        self.assertIn('2 (1 overlapping)', table)


class _Client:
    def __init__(self, inner):
        self.inner = inner

    def query(self, sql, settings=None, timeout=3600):
        return self.inner.query(sql)

    def events(self):
        return {}

    def settle(self, table, timeout=60):
        pass

    def drop_caches(self):
        pass


if __name__ == '__main__':
    unittest.main()
