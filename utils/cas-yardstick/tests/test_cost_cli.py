import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace
from unittest import mock

from yardstick.cost import cli, estimate

SCRIPT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'yardstick.py')


def touch(path):
    with open(path, 'wb') as f:
        f.write(b'bin')


def counts(attempts=1, **kw):
    base = dict.fromkeys(('attempts', 'delivered', 'dropped', 'injected', 'rejected', 'upstream_ok', 'upstream_error',
                          'upstream_unknown', 'req_payload_bytes', 'resp_payload_bytes', 'list_pages', 'list_keys',
                          'delete_calls', 'delete_keys'), 0)
    base.update(attempts=attempts, **kw)
    return base


def ok_record(card='c', case='x', policy='s3', k=1, wall_s=2.0, cleanup_s=None):
    rec = {'experiment_id': f'{card}.{case}', 'card': card, 'case_id': case, 'policy': policy, 'k': k,
           'status': 'ok', 'wall_s': wall_s,
           'ledger': {'foreground': {'r1': {'PutObject': counts(3), 'UploadPart': counts(2), 'GetObject': counts(4),
                                            'HeadObject': counts(1), 'ListObjectsV2': counts(5, list_pages=5),
                                            'DeleteObjects': counts(1, delete_calls=1, delete_keys=7)}}},
           'interserver': {'relay0': {'from_target': 100, 'to_target': 5}, 'relay1': {'from_target': 50, 'to_target': 0}},
           'proc_io': {'r1': {'write_bytes': 4096}, 'r2': {'write_bytes': 1024}}}
    if cleanup_s is not None:
        rec['cleanup_s'] = cleanup_s
    return rec


class FakeProxy:
    def __init__(self, upstream, store_id, ledger_path, callers):
        self.ports, self.stopped, self.started = dict(callers), False, False

    def start(self):
        self.started = True

    def stop(self):
        self.stopped = True


class FakeRig:
    def __init__(self, *a):
        pass

    def ensure_bucket(self, bucket):
        pass


class FakeRustfs:
    def __init__(self, started):
        self.started, self.stopped = started, False

    def ensure(self):
        return self.started

    def stop(self):
        self.stopped = True


def write_manifest(d, card, case, policies, repeats=2, replicas=1):
    os.makedirs(os.path.join(d, 'experiments'), exist_ok=True)
    with open(os.path.join(d, 'experiments', f'{card}.{case}.json'), 'w') as f:
        json.dump({'experiment_id': f'{card}.{case}', 'card': card, 'case_id': case, 'policies': policies,
                   'replicas': replicas, 'mode': 'deterministic', 'scale': 1.0, 'repeats': repeats,
                   'timeout_s': 60}, f)


def run_args(tmp, manifests, **kw):
    base = dict(binary=os.path.join(tmp, 'clickhouse'), manifests=manifests, cases='all', policies='s3,cas',
                replicas=1, repeats=None, scale=1.0, runs_dir=os.path.join(tmp, 'runs'), port_base=19700,
                endpoint=None, keep_prefix=False, commit='abc123')
    base.update(kw)
    return SimpleNamespace(**base)


class RunTest(unittest.TestCase):
    def test_run_dispatch_and_exit_codes(self):
        with tempfile.TemporaryDirectory() as tmp:
            touch(os.path.join(tmp, 'clickhouse'))
            m = os.path.join(tmp, 'm')
            write_manifest(m, 'c', 'one', ['s3', 'cas', 'local'], repeats=2)
            write_manifest(m, 'c', 'two', ['cas'], repeats=1)
            cards = {'c': object()}
            calls = []
            proxies = []

            def fake_run_case(card, manifest, policy, k, deps):
                calls.append((manifest['case_id'], policy, k, deps.commit, deps.binary_sha256))
                status = 'FAILED' if (manifest['case_id'], policy, k) == ('one', 'cas', 2) else 'ok'
                if policy == 'unsupported':
                    status = 'UNSUPPORTED'
                rec = ok_record('c', manifest['case_id'], policy, k)
                return rec if status == 'ok' else {**rec, 'status': status, 'reason': 'boom'}

            def proxy_factory(*a):
                proxies.append(FakeProxy(*a))
                return proxies[-1]

            def go(args, started, run_case=fake_run_case):
                rustfs = FakeRustfs(started)
                out = io.StringIO()
                with redirect_stdout(out):
                    code = cli.cmd_run(args, run_case=run_case, proxy_factory=proxy_factory, rig_factory=FakeRig,
                                       rustfs=rustfs, cards=cards)
                return code, rustfs, out.getvalue()

            code, rustfs, out = go(run_args(tmp, [m]), started=True)
            # manifest x policy (in the intersection of manifest and --policies) x k, manifests in name order
            self.assertEqual([c[:3] for c in calls], [('one', 's3', 1), ('one', 's3', 2), ('one', 'cas', 1),
                                                     ('one', 'cas', 2), ('two', 'cas', 1)])
            self.assertEqual({c[3] for c in calls}, {'abc123'})
            self.assertEqual(len({c[4] for c in calls}), 1)
            self.assertEqual(len(calls[0][4]), 64)
            self.assertEqual(code, 1)
            self.assertTrue(proxies[0].started and proxies[0].stopped)
            self.assertTrue(rustfs.stopped)
            run_dirs = os.listdir(os.path.join(tmp, 'runs'))
            self.assertEqual(len(run_dirs), 1)
            self.assertTrue(run_dirs[0].startswith('cost-'))
            with open(os.path.join(tmp, 'runs', run_dirs[0], 'measurements.jsonl')) as f:
                rows = [json.loads(line) for line in f]
            self.assertEqual([r['status'] for r in rows], ['ok', 'ok', 'ok', 'FAILED', 'ok'])
            self.assertIn('one', out)
            self.assertIn('FAILED', out)

            # --cases selects by experiment_id, --repeats overrides the manifest
            calls.clear()
            code, rustfs, out = go(run_args(tmp, [m], cases='c.two', repeats=3, policies='cas'), started=False)
            self.assertEqual([c[:3] for c in calls], [('two', 'cas', 1), ('two', 'cas', 2), ('two', 'cas', 3)])
            self.assertEqual(code, 0)
            self.assertFalse(rustfs.stopped)  # a RustFS the CLI did not start stays up

            # UNSUPPORTED is not a failure; an external endpoint skips ensure and stop
            calls.clear()
            args = run_args(tmp, [m], cases='c.two', repeats=1, policies='cas', endpoint='http://h:9')
            unsupported = lambda *a: {**ok_record(), 'status': 'UNSUPPORTED', 'reason': 'no'}  # noqa: E731
            code, rustfs, out = go(args, started=True, run_case=unsupported)
            self.assertEqual(code, 0)
            self.assertFalse(rustfs.stopped)
            self.assertIn('UNSUPPORTED', out)

            # an exception still stops proxy and RustFS
            def explode(*a):
                raise RuntimeError('x')
            with self.assertRaises(RuntimeError):
                go(run_args(tmp, [m]), started=True, run_case=explode)
            self.assertTrue(proxies[-1].stopped)

    def test_table_columns(self):
        row = cli.table_row(ok_record())
        self.assertEqual(row, ['c.x', 's3', 'ok', '2.0', '5', '5', '5', '1/7', '150', '5120'])


class ProbeTest(unittest.TestCase):
    def test_probe_runs_five_with_drill_and_render_checks(self):
        with tempfile.TemporaryDirectory() as tmp:
            touch(os.path.join(tmp, 'clickhouse'))
            m, tools = os.path.join(tmp, 'm'), os.path.join(tmp, 'tools')
            os.makedirs(tools)
            for name in cli.PROBES:
                write_manifest(m, name, 'p', ['s3'], repeats=1)
            for script in ('drill.mjs', 'render_check.mjs'):
                touch(os.path.join(tools, script))
            cards = {n: object() for n in cli.PROBES}
            ran, node = [], []

            def fake_run_case(card, manifest, policy, k, deps):
                ran.append(manifest['card'])
                return ok_record(manifest['card'], 'p')

            def fake_subprocess(cmd, **kw):
                node.append(cmd)
                bad = cmd[1].endswith('render_check.mjs') and 'probe_local_volume' in cmd
                return SimpleNamespace(returncode=1 if bad else 0, stdout='', stderr='')

            def go(tools_dir):
                args = run_args(tmp, [m])
                args.manifests = m
                out = io.StringIO()
                with redirect_stdout(out), mock.patch.object(cli.subprocess, 'run', fake_subprocess):
                    code = cli.cmd_probe(args, run_case=fake_run_case, proxy_factory=FakeProxy, rig_factory=FakeRig,
                                         rustfs=FakeRustfs(False), cards=cards, tools_dir=tools_dir)
                return code, out.getvalue()

            code, out = go(tools)
            self.assertEqual(ran, list(cli.PROBES))
            self.assertEqual(len(node), 10)
            self.assertEqual([c[1].rsplit('/', 1)[1] for c in node[:2]], ['drill.mjs', 'render_check.mjs'])
            self.assertEqual(code, 2)
            self.assertEqual(out.count('PASS'), 4)
            self.assertIn('FAIL probe_local_volume: render check failed', out)

            node.clear()
            os.remove(os.path.join(tools, 'drill.mjs'))
            code, out = go(tools)
            self.assertEqual(code, 2)
            self.assertEqual(node, [])
            self.assertEqual(out.count('FAIL'), 5)
            self.assertIn('tool missing', out)


COVERAGE = {
    'cards': [
        {'name': 'a', 'family': 'insert', 'model_variants': [], 'cases': [
            {'case_id': 'p1', 'policies': ['s3', 'cas'], 'replicas': 1, 'scale': 1, 'repeats': 2, 'timeout_s': 100,
             'priority': 'P1', 'status': 'planned'},
            {'case_id': 'done', 'policies': ['s3'], 'replicas': 1, 'scale': 1, 'repeats': 9, 'timeout_s': 100,
             'priority': 'P1', 'status': 'run'}]},
        {'name': 'b', 'family': 'insert', 'model_variants': [], 'cases': [
            {'case_id': 'p2', 'policies': ['s3'], 'replicas': 1, 'scale': 1, 'repeats': 1, 'timeout_s': 0,
             'priority': 'P2', 'status': 'planned'}]},
        {'name': 'replication-zero-copy', 'family': 'repl', 'model_variants': [], 'cases': [
            {'case_id': 'z', 'policies': ['s3'], 'replicas': 2, 'scale': 1, 'repeats': 1, 'timeout_s': 0,
             'priority': 'P1', 'status': 'planned'}]},
        {'name': 'types-int', 'family': 'types', 'model_variants': [], 'cases': [
            {'case_id': 't', 'policies': ['s3'], 'replicas': 1, 'scale': 1, 'repeats': 1, 'timeout_s': 0,
             'priority': 'P1', 'status': 'planned'}]},
        {'name': 'lonely', 'family': 'solo', 'model_variants': [], 'cases': [
            {'case_id': 'q', 'policies': ['s3'], 'replicas': 1, 'scale': 1, 'repeats': 1, 'timeout_s': 0,
             'priority': 'P2', 'status': 'planned'}]}],
    'evidence': [
        {'coefficient': 'k_put', 'experiment_id': 'b.p2', 'policy': 's3', 'role': 'fit', 'status': 'planned'},
        {'coefficient': 'k_zc', 'experiment_id': 'replication-zero-copy.z', 'policy': 's3', 'role': 'fit',
         'status': 'planned'},
        {'coefficient': 'k_other', 'experiment_id': 'a.p1', 'policy': 's3', 'role': 'fit', 'status': 'planned'}]}
REGISTRY = {'quantities': [{'id': 'put_cost', 'coef': 'k_put', 'status': 'ok'},
                           {'id': 'zc_cost', 'coef': 'k_zc', 'status': 'ok'},
                           {'id': 'free', 'coef': None, 'status': 'ok'}]}
PRESETS_JS = """SCC.presets.SCENARIOS = {
  small_cluster: { inputs: { put_cost: 1, free: 2 } },
  zero_copy: { inputs: { zc_cost: 3 } }
};
"""


class EstimateTest(unittest.TestCase):
    def test_estimate_math_fallbacks_and_cut_order(self):
        recs = [ok_record('a', 'p1', wall_s=100, cleanup_s=10), ok_record('a', 'p1', wall_s=300, cleanup_s=30),
                ok_record('c', 'x', wall_s=50),
                {**ok_record('b', 'zzz', wall_s=999), 'status': 'FAILED'}]
        # b.p2 has no b record: family 'insert' median over a's records is used
        presets = estimate.presets_by_coefficient(PRESETS_JS, REGISTRY)
        self.assertEqual(presets, {'k_put': ['small_cluster'], 'k_zc': ['zero_copy']})
        est = estimate.estimate(recs, COVERAGE, REGISTRY, presets, budget_hours=0.1,
                                sample_of={'repl': 'x'})
        by_id = {c['experiment_id']: c for c in est.cases}
        self.assertEqual(sorted(by_id), ['a.p1', 'b.p2', 'lonely.q', 'replication-zero-copy.z', 'types-int.t'])
        a = by_id['a.p1']
        # card median wall 200 + timeout share 10 + cleanup median 20 = 230 per pass; 2 policies x 2 repeats
        self.assertEqual((a['passes'], a['pass_seconds'], a['basis']), (4, 200, 'card'))
        self.assertAlmostEqual(a['seconds'], 4 * 230)
        b = by_id['b.p2']
        self.assertEqual((b['pass_seconds'], b['basis']), (200, 'family'))
        self.assertAlmostEqual(b['seconds'], 1 * (200 + 0 + 20))
        self.assertEqual(by_id['replication-zero-copy.z']['basis'], 'sample')
        self.assertEqual(by_id['lonely.q']['basis'], 'global')
        self.assertTrue(by_id['lonely.q']['estimated_from_global'])
        raw_p1 = sum(c['seconds'] for c in est.cases if c['priority'] == 'P1')
        self.assertAlmostEqual(est.p1_hours, raw_p1 * 1.25 / 3600)
        raw_all = sum(c['seconds'] for c in est.cases)
        self.assertAlmostEqual(est.total_hours, raw_all * 1.25 / 3600)
        # cut order: P2 by family (largest first: insert, then solo), then zero-copy, then types-*
        self.assertEqual([c['label'] for c in est.cuts], ['P2 family insert', 'P2 family solo', 'replication-zero-copy*',
                                                          'types-*'][:len(est.cuts)])
        first = est.cuts[0]
        self.assertEqual(first['experiments'], ['b.p2'])
        self.assertEqual(first['coefficients'], ['k_put'])
        self.assertEqual(first['quantities'], ['put_cost'])
        self.assertEqual(first['presets'], ['small_cluster'])
        zc = next(c for c in est.cuts if c['label'] == 'replication-zero-copy*')
        self.assertEqual((zc['coefficients'], zc['quantities'], zc['presets']), (['k_zc'], ['zc_cost'], ['zero_copy']))
        self.assertTrue(est.reached or est.cuts[-1]['remaining_hours'] > 0.1)

    def test_no_presets_file_reports_unknown_and_budget_met_needs_no_cuts(self):
        est = estimate.estimate([ok_record('a', 'p1', wall_s=1)], COVERAGE, REGISTRY, None, budget_hours=72)
        self.assertEqual(est.cuts, [])
        self.assertTrue(est.reached)
        est = estimate.estimate([ok_record('a', 'p1', wall_s=3600)], COVERAGE, REGISTRY, None, budget_hours=1)
        self.assertEqual(est.cuts[0]['presets'], 'unknown')


class RegisteredTest(unittest.TestCase):
    def test_cost_subcommand_registered_in_yardstick_py(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location('yardstick_script', SCRIPT)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        parser = mod.build_parser()
        args = parser.parse_args(['cost', 'run', '--binary', 'b', '--manifests', 'd1', '--manifests', 'd2',
                                  '--cases', 'a.b,c.d', '--policies', 's3,cas', '--repeats', '2'])
        self.assertIs(args.func, cli.cmd_run)
        self.assertEqual((args.manifests, args.cases, args.repeats), (['d1', 'd2'], 'a.b,c.d', 2))
        self.assertIs(parser.parse_args(['cost', 'probe', '--binary', 'b']).func, cli.cmd_probe)
        self.assertIs(parser.parse_args(['cost', 'estimate', 'rd']).func, cli.cmd_estimate)
        self.assertIsNotNone(parser.parse_args(['report', 'x']).func)


if __name__ == '__main__':
    unittest.main()
