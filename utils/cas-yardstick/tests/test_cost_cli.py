import contextlib
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
    endpoints = []

    def __init__(self, endpoint, *a):
        FakeRig.endpoints.append(endpoint)

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
    with open(os.path.join(tmp, 'SHA256SUMS'), 'w') as f:
        f.write('d' * 64 + '  ' + os.path.join(tmp, 'clickhouse') + '\n')
    base = dict(binary=os.path.join(tmp, 'clickhouse'), source_dir=None, manifests=manifests, cases='all', policies='s3,cas',
                replicas=1, repeats=None, runs_dir=os.path.join(tmp, 'runs'), port_base=19700,
                endpoint=None, keep_prefix=False, commit='abc123')
    base.update(kw)
    return SimpleNamespace(**base)


class ManifestsTest(unittest.TestCase):
    def test_manifests_point_their_cards_at_their_own_report_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            os.makedirs(os.path.join(tmp, 'experiments'))
            for name, params in (('a', {'rows': 1}), ('b', {'rows': 1, 'report_dir': '/elsewhere'})):
                with open(os.path.join(tmp, 'experiments', f'{name}.json'), 'w') as f:
                    json.dump({'experiment_id': name, 'params': params}, f)
            got = {m['experiment_id']: m['params']['report_dir'] for m in cli._load_manifests([tmp])}
            self.assertEqual(got, {'a': tmp, 'b': '/elsewhere'})
            self.assertEqual(cli.report_dir_of(os.path.join(tmp, 'experiments')), tmp)
            self.assertEqual(cli.report_dir_of(tmp), tmp)


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
                self.assertEqual(len(deps.rig_commit), 40)
                self.assertEqual(deps.binary_download_sha256, 'd' * 64)
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
            # The shared observer client goes through the proxy's rig listener, never to the store directly.
            self.assertEqual(FakeRig.endpoints[-1], 'http://127.0.0.1:19134')
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

    def test_cleanup_failure_makes_run_exit_non_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            touch(os.path.join(tmp, 'clickhouse'))
            m = os.path.join(tmp, 'm')
            write_manifest(m, 'c', 'one', ['s3'], repeats=1)

            def dirty(card, manifest, policy, k, deps):
                return {**ok_record('c', 'one'), 'cleanup': {'aborted_uploads': 0, 'deleted_keys': 0,
                                                             'verified_empty': False, 'errors': ['delete_prefix: x']}}
            out = io.StringIO()
            with redirect_stdout(out):
                code = cli.cmd_run(run_args(tmp, [m], policies='s3'), run_case=dirty, proxy_factory=FakeProxy,
                                   rig_factory=FakeRig, rustfs=FakeRustfs(False), cards={'c': object()})
            self.assertEqual(code, 1)
            self.assertIn('cleanup failed', out.getvalue())

    def test_download_hash_and_source_commit_lookup(self):
        with tempfile.TemporaryDirectory() as tmp:
            os.makedirs(os.path.join(tmp, 'bin', '26.8'))
            binary = os.path.join(tmp, 'bin', '26.8', 'clickhouse')
            touch(binary)
            with open(os.path.join(tmp, 'bin', 'SHA256SUMS'), 'w') as f:
                # Paths are relative to an ancestor of the sums file, as in the repository's tmp/bin/SHA256SUMS.
                f.write('a' * 64 + '  bin/26.6/clickhouse\n' + 'b' * 64 + '  bin/26.8/clickhouse\n')
            self.assertEqual(cli.download_sha256(binary), 'b' * 64)
            self.assertIsNone(cli.download_sha256(os.path.join(tmp, 'elsewhere')))
            self.assertEqual(cli.source_dir_for(binary, root=tmp), None)
            os.makedirs(os.path.join(tmp, 'src-26.8'))
            self.assertEqual(cli.source_dir_for(binary, root=tmp), os.path.join(tmp, 'src-26.8'))

    def test_table_columns(self):
        row = cli.table_row(ok_record())
        self.assertEqual(row, ['c.x', 's3', 'ok', '2.0', '5', '5', '5', '1/7', '150', '5120'])


class OwnershipTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.m = os.path.join(self.tmp.name, 'm')
        touch(os.path.join(self.tmp.name, 'clickhouse'))
        write_manifest(self.m, 'c', 'one', ['s3'], repeats=1)

    def go(self, rustfs, proxy_factory=FakeProxy, args=None, patch=None):
        out = io.StringIO()
        with redirect_stdout(out), mock.patch.object(cli, 'Topology', patch or cli.Topology):
            return cli.cmd_run(args or run_args(self.tmp.name, [self.m]), run_case=lambda *a: ok_record('c', 'one'),
                               proxy_factory=proxy_factory, rig_factory=FakeRig, rustfs=rustfs, cards={'c': object()})

    def test_rustfs_ensure_exit_codes_decide_ownership(self):
        for code, expected in ((0, True), (3, False)):
            script = os.path.join(self.tmp.name, f'ensure{code}.sh')
            with open(script, 'w') as f:
                f.write(f'#!/bin/sh\nexit {code}\n')
            os.chmod(script, 0o755)
            self.assertIs(cli.Rustfs(script).ensure(), expected)
        script = os.path.join(self.tmp.name, 'ensure1.sh')
        with open(script, 'w') as f:
            f.write('#!/bin/sh\necho boom >&2\nexit 1\n')
        os.chmod(script, 0o755)
        with self.assertRaisesRegex(RuntimeError, 'boom'):
            cli.Rustfs(script).ensure()

    def test_rustfs_stopped_when_setup_raises_after_ensure(self):
        def broken(*a, **kw):
            raise RuntimeError('topology')
        rustfs = FakeRustfs(True)
        with self.assertRaises(RuntimeError):
            self.go(rustfs, patch=broken)
        self.assertTrue(rustfs.stopped)

    def test_rustfs_stopped_when_proxy_stop_raises(self):
        class BadStop(FakeProxy):
            def stop(self):
                raise OSError('stop')
        rustfs = FakeRustfs(True)
        with self.assertRaises(OSError):
            self.go(rustfs, proxy_factory=BadStop)
        self.assertTrue(rustfs.stopped)

    def test_empty_or_invalid_selection_fails_before_any_service(self):
        class Counting(FakeRustfs):
            def ensure(self):
                raise AssertionError('service started')
        err = io.StringIO()
        base = run_args(self.tmp.name, [self.m])
        for change, code, text in (
                (dict(cases='c.nope'), 1, 'c.nope'),
                (dict(policies='s3,gcs'), 1, 'gcs'),
                (dict(policies='cas'), 1, 'nothing to run'),
                (dict(cases='c.one,c.zzz'), 1, 'c.zzz')):
            args = SimpleNamespace(**{**vars(base), **change})
            err = io.StringIO()
            with mock.patch('sys.stderr', err):
                self.assertEqual(self.go(Counting(True), args=args), code)
            self.assertIn(text, err.getvalue())
        # probe: nothing for the five probes in these manifests -> exit 2, still no service
        args = SimpleNamespace(**{**vars(base), 'manifests': self.m})
        err = io.StringIO()
        with mock.patch('sys.stderr', err):
            code = cli.cmd_probe(args, run_case=None, proxy_factory=FakeProxy, rig_factory=FakeRig,
                                 rustfs=Counting(True), cards={})
        self.assertEqual(code, 2)
        self.assertIn('nothing to run', err.getvalue())
        args.policies = 'bogus'
        with mock.patch('sys.stderr', io.StringIO()):
            self.assertEqual(cli.cmd_probe(args, run_case=None, proxy_factory=FakeProxy, rig_factory=FakeRig,
                                           rustfs=Counting(True), cards={}), 2)


class ProbeTest(unittest.TestCase):
    def test_failed_probe_record_skips_node_tools(self):
        with tempfile.TemporaryDirectory() as tmp:
            touch(os.path.join(tmp, 'clickhouse'))
            m, tools = os.path.join(tmp, 'm'), os.path.join(tmp, 'tools')
            os.makedirs(tools)
            for name in cli.PROBES:
                write_manifest(m, name, 'p', ['s3'], repeats=1)
            for script in ('drill.mjs', 'render_check.mjs'):
                touch(os.path.join(tools, script))
            node = []

            def fake_run_case(card, manifest, policy, k, deps):
                rec = ok_record(manifest['card'], 'p')
                return {**rec, 'status': 'FAILED', 'reason': 'boom'} if manifest['card'] == 'probe_s3_insert' else rec

            def fake_subprocess(cmd, **kw):
                node.append(cmd)
                return SimpleNamespace(returncode=0, stdout='', stderr='')

            args = run_args(tmp, [m])
            args.manifests = m
            out = io.StringIO()
            with redirect_stdout(out), mock.patch.object(cli.subprocess, 'run', fake_subprocess), \
                        mock.patch.object(cli, '_git_commit', lambda *a: 'f' * 40):
                code = cli.cmd_probe(args, run_case=fake_run_case, proxy_factory=FakeProxy, rig_factory=FakeRig,
                                     rustfs=FakeRustfs(False), cards={n: object() for n in cli.PROBES}, tools_dir=tools)
            self.assertEqual(code, 2)
            self.assertIn('FAIL probe_s3_insert: run FAILED', out.getvalue())
            self.assertEqual(len(node), 2 * (len(cli.PROBES) - 1))
            self.assertFalse(any('probe_s3_insert' in cmd for cmd in node))
    def test_probe_runs_six_with_drill_and_render_checks(self):
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
                with redirect_stdout(out), mock.patch.object(cli.subprocess, 'run', fake_subprocess), \
                        mock.patch.object(cli, '_git_commit', lambda *a: 'f' * 40):
                    code = cli.cmd_probe(args, run_case=fake_run_case, proxy_factory=FakeProxy, rig_factory=FakeRig,
                                         rustfs=FakeRustfs(False), cards=cards, tools_dir=tools_dir)
                return code, out.getvalue()

            code, out = go(tools)
            self.assertEqual(ran, list(cli.PROBES))
            self.assertEqual(len(node), 2 * len(cli.PROBES))
            drill = node[0]
            self.assertEqual(drill[drill.index('--cell') + 1], drill[drill.index('--probe') + 1])
            # The drill reads predictions and formulas from the report directory the manifests came from.
            self.assertEqual(drill[drill.index('--report-dir') + 1], m)
            self.assertEqual([c[1].rsplit('/', 1)[1] for c in node[:2]], ['drill.mjs', 'render_check.mjs'])
            self.assertEqual(code, 2)
            self.assertEqual(out.count('PASS'), len(cli.PROBES) - 1)
            self.assertIn('FAIL probe_local_volume: render check failed', out)

            node.clear()
            os.remove(os.path.join(tools, 'drill.mjs'))
            code, out = go(tools)
            self.assertEqual(code, 2)
            self.assertEqual(node, [])
            self.assertEqual(out.count('FAIL'), len(cli.PROBES))
            self.assertIn('tool missing', out)


    def test_failed_assertion_fails_the_probe_before_node_tools(self):
        with tempfile.TemporaryDirectory() as tmp:
            touch(os.path.join(tmp, 'clickhouse'))
            m, tools = os.path.join(tmp, 'm'), os.path.join(tmp, 'tools')
            os.makedirs(tools)
            for name in cli.PROBES:
                write_manifest(m, name, 'p', ['s3'], repeats=1)
            for script in ('drill.mjs', 'render_check.mjs'):
                touch(os.path.join(tools, script))
            node = []

            def fake_run_case(card, manifest, policy, k, deps):
                rec = ok_record(manifest['card'], 'p')
                ok = manifest['card'] != 'probe_multipart_fault'
                rec['assertions'] = {'a': {'expected': 1, 'actual': 1, 'ok': True},
                                     'b': {'expected': 1, 'actual': 2, 'ok': ok}}
                return rec

            def fake_subprocess(cmd, **kw):
                node.append(cmd)
                return SimpleNamespace(returncode=0, stdout='', stderr='')

            args = run_args(tmp, [m])
            args.manifests = m
            out = io.StringIO()
            with redirect_stdout(out), mock.patch.object(cli.subprocess, 'run', fake_subprocess), \
                        mock.patch.object(cli, '_git_commit', lambda *a: 'f' * 40):
                code = cli.cmd_probe(args, run_case=fake_run_case, proxy_factory=FakeProxy, rig_factory=FakeRig,
                                     rustfs=FakeRustfs(False), cards={n: object() for n in cli.PROBES}, tools_dir=tools)
            self.assertEqual(code, 2)
            self.assertIn('FAIL probe_multipart_fault: assertions failed: b', out.getvalue())
            self.assertEqual(out.getvalue().count('PASS'), len(cli.PROBES) - 1)
            self.assertFalse(any('probe_multipart_fault' in cmd for cmd in node))


COVERAGE = {
    'cards': [
        {'name': 'a', 'family': 'insert', 'model_variants': [], 'cases': [
            {'case_id': 'p1', 'policies': ['s3', 'cas'], 'replicas': 1, 'scale': 1, 'repeats': 2, 'timeout_s': 100,
             'duration_s': 120, 'priority': 'P1', 'status': 'planned'},
            {'case_id': 'done', 'policies': ['s3'], 'replicas': 1, 'scale': 1, 'repeats': 9, 'timeout_s': 100,
             'duration_s': 120, 'priority': 'P1', 'status': 'run'}]},
        {'name': 'b', 'family': 'insert', 'model_variants': [], 'cases': [
            {'case_id': 'p2', 'policies': ['s3'], 'replicas': 1, 'scale': 1, 'repeats': 1, 'timeout_s': 900,
             'duration_s': 600, 'priority': 'P2', 'status': 'planned'}]},
        {'name': 'replication-zero-copy', 'family': 'repl', 'model_variants': [], 'cases': [
            {'case_id': 'z', 'policies': ['s3'], 'replicas': 2, 'scale': 1, 'repeats': 1, 'timeout_s': 0,
             'duration_s': 0, 'priority': 'P1', 'status': 'planned'}]},
        {'name': 'types-int', 'family': 'types', 'model_variants': [], 'cases': [
            {'case_id': 't', 'policies': ['s3'], 'replicas': 1, 'scale': 1, 'repeats': 1, 'timeout_s': 0,
             'duration_s': 0, 'priority': 'P1', 'status': 'planned'}]},
        {'name': 'lonely', 'family': 'solo', 'model_variants': [], 'cases': [
            {'case_id': 'q', 'policies': ['s3'], 'replicas': 1, 'scale': 1, 'repeats': 1, 'timeout_s': 0,
             'duration_s': 0, 'priority': 'P2', 'status': 'planned'}]}],
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
        # card median wall 200 is above the declared 120 s floor; + cleanup median 20 = 220 per pass; 2 x 2 passes
        self.assertEqual((a['passes'], a['pass_seconds'], a['basis'], a['floor_applied']), (4, 200, 'card', False))
        self.assertAlmostEqual(a['seconds'], 4 * 220)
        b = by_id['b.p2']
        # the family median 200 s is below the case's declared 600 s duration: the floor decides
        self.assertEqual((b['pass_seconds'], b['basis'], b['floor_applied']), (600, 'family', True))
        self.assertAlmostEqual(b['seconds'], 1 * (600 + 20))
        self.assertEqual(by_id['replication-zero-copy.z']['basis'], 'sample')
        self.assertEqual(by_id['lonely.q']['basis'], 'global')
        self.assertTrue(by_id['lonely.q']['estimated_from_global'])
        raw_p1 = sum(c['seconds'] for c in est.cases if c['priority'] == 'P1')
        self.assertAlmostEqual(est.p1_hours, raw_p1 * 1.25 / 3600)
        raw_all = sum(c['seconds'] for c in est.cases)
        self.assertAlmostEqual(est.total_hours, raw_all * 1.25 / 3600)
        # cut order: P2 by family (largest first), then zero-copy replication, then the types cases; 0.1 h is out of reach
        self.assertEqual([c['label'] for c in est.cuts], ['P2 family insert', 'P2 family solo',
                                                          'replication-zero-copy*', 'types-*'])
        self.assertFalse(est.reached)
        self.assertAlmostEqual(est.cuts[-1]['remaining_hours'], 880 * 1.25 / 3600)
        first = est.cuts[0]
        self.assertEqual(first['experiments'], ['b.p2'])
        self.assertEqual(first['coefficients'], ['k_put'])
        self.assertEqual(first['quantities'], ['put_cost'])
        self.assertEqual(first['presets'], ['small_cluster'])
        zc = next(c for c in est.cuts if c['label'] == 'replication-zero-copy*')
        self.assertEqual((zc['coefficients'], zc['quantities'], zc['presets']), (['k_zc'], ['zc_cost'], ['zero_copy']))

    def test_no_presets_file_reports_unknown_and_budget_met_needs_no_cuts(self):
        est = estimate.estimate([ok_record('a', 'p1', wall_s=1)], COVERAGE, REGISTRY, None, budget_hours=72)
        self.assertEqual(est.cuts, [])
        self.assertTrue(est.reached)
        est = estimate.estimate([ok_record('a', 'p1', wall_s=3600)], COVERAGE, REGISTRY, None, budget_hours=1)
        self.assertEqual(est.cuts[0]['presets'], 'unknown')


class EstimateCommandTest(unittest.TestCase):
    def test_sample_of_option_parsed_into_family_map(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = os.path.join(tmp, 'run')
            os.makedirs(run_dir)
            with open(os.path.join(run_dir, 'measurements.jsonl'), 'w') as f:
                f.write(json.dumps(ok_record('c', 'x', wall_s=50)) + '\n')
            cov = os.path.join(tmp, 'coverage.json')
            with open(cov, 'w') as f:
                json.dump(COVERAGE, f)
            reg = os.path.join(tmp, 'registry.json')
            with open(reg, 'w') as f:
                json.dump(REGISTRY, f)
            parser = argparse_parser()
            args = parser.parse_args(['cost', 'estimate', run_dir, '--coverage', cov, '--registry', reg,
                                      '--presets', '/nope', '--sample-of', 'repl=x', '--sample-of', 'solo=x', '--json'])
            self.assertEqual(args.sample_of, ['repl=x', 'solo=x'])
            out = io.StringIO()
            with redirect_stdout(out):
                self.assertEqual(args.func(args), 0)
            cases = {c['experiment_id']: c for c in json.loads(out.getvalue())['cases']}
            self.assertEqual((cases['replication-zero-copy.z']['basis'], cases['replication-zero-copy.z']['pass_seconds']),
                             ('sample', 50))
            self.assertEqual(cases['lonely.q']['basis'], 'sample')


class EstimateRegistryTest(unittest.TestCase):
    def test_default_registry_exists_and_a_missing_one_fails(self):
        args = argparse_parser().parse_args(['cost', 'estimate', 'rd'])
        self.assertEqual(os.path.basename(args.registry), 'inputs-registry.json')
        self.assertTrue(os.path.isfile(args.registry), args.registry)
        with tempfile.TemporaryDirectory() as tmp:
            cov = os.path.join(tmp, 'coverage.json')
            with open(cov, 'w') as f:
                json.dump(COVERAGE, f)
            args = argparse_parser().parse_args(['cost', 'estimate', tmp, '--coverage', cov, '--registry',
                                                 os.path.join(tmp, 'absent.json')])
            err = io.StringIO()
            with contextlib.redirect_stderr(err), redirect_stdout(io.StringIO()):
                self.assertEqual(args.func(args), 2)
            self.assertIn('absent.json', err.getvalue())

    def test_phase0_coverage_declares_duration_floors(self):
        """Every case declares a positive duration within its timeout; the paced amplification run keeps its 2 h floor.

        The phase-0 estimate seeded 120 s (600 s for gc-* and replication-*) per case. The research tasks replaced
        those seeds with measured floors (60 s where the median pass is shorter), including on the seed cases
        themselves, so the per-family equality no longer describes the file; the estimate reads whatever is declared.
        """
        with open(os.path.join(cli.REPO, 'docs', 'superpowers', 'cost-model', 'coverage.json')) as f:
            cov = json.load(f)
        cases = {(c['name'], k['case_id']): k for c in cov['cards'] for k in c['cases']}
        amp = cases[('amplification-run', 'amplification-run')]
        self.assertEqual((amp['duration_s'], amp['timeout_s'], amp['status']), (7200, 14400, 'cut'))
        for (card, case), k in cases.items():
            self.assertGreater(k['duration_s'], 0, (card, case))
            self.assertGreaterEqual(k['timeout_s'], k['duration_s'], (card, case))


def argparse_parser():
    import argparse
    p = argparse.ArgumentParser()
    cli.add_parsers(p.add_subparsers(dest='command'))
    return p


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
        # Scale is the manifest's: a CLI value would be recorded without being applied.
        for cmd in (['cost', 'run', '--binary', 'b', '--manifests', 'd'], ['cost', 'probe', '--binary', 'b']):
            with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
                parser.parse_args(cmd + ['--scale', '2'])
        self.assertIsNotNone(parser.parse_args(['report', 'x']).func)


if __name__ == '__main__':
    unittest.main()
