"""Fixture tests of the EBS demand card (`io-sizes`, R1), its manifests and the demand arithmetic."""
import json
import os
import shutil
import sys
import tempfile
import unittest
from types import SimpleNamespace

from yardstick.cost import localio
from yardstick.cost.cards import CARDS, io_sizes, probes

TOOLS = os.path.join(probes.REPO, 'utils', 'storage-cost-calculator', 'tools')
sys.path.insert(0, TOOLS)
import artifacts  # noqa: E402

R = os.path.join(probes.REPO, 'docs', 'superpowers', 'reports', '2026-10-24-storage-cost-ebs')
EXPERIMENTS = os.path.join(R, 'experiments')
with open(os.path.join(probes.REPO, 'docs', 'superpowers', 'cost-model', 'inputs-registry.json')) as _f:
    REGISTRY = {q['id']: q for q in json.load(_f)['quantities']}
MIB = 1 << 20


def manifest(experiment_id):
    with open(os.path.join(EXPERIMENTS, f'{experiment_id}.json')) as f:
        m = json.load(f)
    m['params']['report_dir'] = R
    return m


class ManifestsTest(unittest.TestCase):
    def test_every_manifest_validates_and_runs_on_local(self):
        for name in sorted(os.listdir(EXPERIMENTS)):
            with open(os.path.join(EXPERIMENTS, name)) as f:
                m = json.load(f)
            self.assertEqual(artifacts.validate_experiment(m), [], name)
            self.assertIn(m['card'], CARDS, name)
            self.assertEqual(m['policies'], ['local'], name)

    def test_io_sizes_manifests_trace_and_use_the_registry_io_size(self):
        for name in sorted(n for n in os.listdir(EXPERIMENTS) if n.startswith('io-sizes.')):
            p = manifest(name[:-5])['params']
            self.assertIs(p['trace'], True, name)
            self.assertEqual(p['ebs_max_io_bytes'], REGISTRY['ebs_max_io_bytes']['default'], name)
            self.assertLessEqual(set(p['phases']), set(io_sizes.PHASE_OPS), name)

    def test_every_prediction_validates_and_cites_the_pinned_source(self):
        for name in sorted(os.listdir(os.path.join(R, 'predictions'))):
            with open(os.path.join(R, 'predictions', name)) as f:
                pred = json.load(f)
            self.assertEqual(artifacts.validate_prediction(pred), [], name)
            self.assertEqual(pred['policy'], 'local', name)
            self.assertEqual(pred['expected']['foreground'], {}, name)  # a local disk sends no S3 request
            for ref in pred['source_refs']:
                self.assertTrue(ref['path'].startswith('tmp/src-26.8/'), ref)
                with open(os.path.join(probes.REPO, ref['path'])) as f:
                    self.assertLessEqual(ref['line'], sum(1 for _ in f), ref)

    def test_card_registered_traced_with_an_idle_control(self):
        card = CARDS['io-sizes']
        self.assertIsInstance(card, io_sizes.IoSizes)
        self.assertTrue(card.trace)
        self.assertEqual((card.policies, card.replicas), (('local',), 2))


class DemandArithmeticTest(unittest.TestCase):
    def test_size_distribution_buckets_and_rejects_empty(self):
        got = io_sizes.size_distribution({'4096': 2, '1024': 1, '1048576': 1, '2097152': 0})
        self.assertAlmostEqual(got['<=4KiB'], 0.75)
        self.assertAlmostEqual(got['1MiB'], 0.25)
        self.assertEqual(set(got), {label for label, _ in io_sizes.SIZE_BUCKETS})
        with self.assertRaises(ValueError):
            io_sizes.size_distribution({})

    def test_ios_per_mib_under_the_256_kib_rule(self):
        self.assertEqual(io_sizes.ios_per_mib({'bytes': 2 * MIB, 'ebs_ios': 8}), 4.0)
        with self.assertRaises(ValueError):
            io_sizes.ios_per_mib({'bytes': 0, 'ebs_ios': 0})

    def test_phase_summary_subtracts_the_idle_baseline(self):
        phase = {'name': 'insert', 'wall_s': 10.0, 'logical_bytes': 1_000_000,
                 'proc_io': {'read_bytes': 0, 'write_bytes': 1_500_000},
                 'traced': {'table': {'write': {'count': 4, 'bytes': 1_000_000, 'ebs_ios': 4,
                                                'size_histogram': {'262144': 4}},
                                      'read': {'count': 0, 'bytes': 0, 'ebs_ios': 0, 'size_histogram': {}}},
                            'keeper': {'write': {'count': 9, 'bytes': 900, 'ebs_ios': 9, 'size_histogram': {'128': 9}}}}}
        s = io_sizes.phase_summary(phase, {'write_bytes': 40_000.0, 'read_bytes': 0.0})
        # 1.5 MB written in 10 s, 40 kB/s of it the baseline: 1.1 MB net per 1 MB of parts.
        self.assertEqual(s['net_write_bytes'], 1_100_000)
        self.assertAlmostEqual(s['io_bytes']['write'], 1.1)
        self.assertEqual(s['io_bytes']['read'], 0.0)
        self.assertEqual(s['request_size_distribution']['write']['256KiB'], 1.0)  # the Keeper's writes are not the volume's
        self.assertNotIn('read', s['ebs_ios_per_mib'])
        with self.assertRaises(ValueError):
            io_sizes.phase_summary({**phase, 'logical_bytes': 0}, {})


class IdleCardTest(unittest.TestCase):
    """`observe` on a fixture pass: the baseline phase's rate is what the other phases subtract."""

    TRACE = ('11 100.500000 pwrite64(5</p/r1/keeper/log/changelog_1.bin>, "x"..., 4096, 0) = 4096 <0.000100>\n'
             '11 120.000000 write(6</p/local/r1/store/t/all_1_1_0/data.bin>, "x"..., 1048576) = 1048576 <0.001000>\n'
             '11 120.500000 write(7</p/r1/data/system/part_log/x.bin>, "x"..., 1000) = 1000 <0.000100>\n')

    def test_observe_subtracts_the_keeper_baseline(self):
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, True)
        with open(localio.Tracer(tmp, 1, 0).path(), 'w') as f:
            f.write(self.TRACE)
        card = io_sizes.IoSizes()
        ctx = SimpleNamespace(pass_dir=tmp, local_dirs=['/p/local/r1'],
                              cluster=SimpleNamespace(run_dir=lambda i: f'/p/r{i}'),
                              manifest=manifest('io-sizes.narrow-compact'))
        io = lambda w: {'read_bytes': 0, 'write_bytes': w}  # noqa: E731
        card.state(ctx)['phases'] = {
            'baseline': {'name': 'baseline', 't0_us': 100_000_000, 't1_us': 110_000_000, 'wall_s': 10.0,
                         'logical_bytes': 0, 'proc_io': {'r1': io(50_000), 'r2': io(10_000)}},
            'insert': {'name': 'insert', 't0_us': 119_000_000, 't1_us': 121_000_000, 'wall_s': 2.0,
                       'logical_bytes': 1_000_000, 'proc_io': {'r1': io(1_060_000), 'r2': io(2_000)}}}
        out = card.observe(ctx, {})
        self.assertEqual(out['baseline']['bps']['r1']['write_bytes'], 5000.0)
        self.assertEqual(out['baseline']['r1_minus_r2_write_bps'], 4000.0)
        self.assertAlmostEqual(out['baseline']['traced_keeper_write_bps'], 409.6)
        insert = out['phases']['insert']['summary']
        self.assertEqual(insert['net_write_bytes'], 1_050_000)  # 1 060 000 minus 2 s x 5 kB/s
        self.assertAlmostEqual(out['inserts.io_bytes.write'], 1.05)
        self.assertEqual(insert['traced_table_bytes']['write'], 1_048_576)
        self.assertEqual(insert['traced_bytes']['write'], 1_049_576)  # table and server classes, not Keeper
        self.assertEqual(out['inserts.ebs_ios_per_mib.write'], (4 + 1) / (1_049_576 / MIB))
        self.assertEqual(out['inserts.max_request_bucket.write'], 1048576)

    def test_virtual_and_system_files_are_not_volume_io(self):
        card = io_sizes.IoSizes()
        ctx = SimpleNamespace(local_dirs=['/p/local/r1'], cluster=SimpleNamespace(run_dir=lambda i: f'/p/r{i}'))
        cls = card.path_class(ctx)
        self.assertEqual([cls(p) for p in ('/p/local/r1/store/x.bin', '/p/r1/keeper/log/c.bin', '/p/r1/data/system/x.bin',
                                           '/p/r1/server.log', '/proc/self/stat', '/sys/fs/cgroup/cpu.max',
                                           '/etc/localtime', '/p/r10/data/x')],
                         ['table', 'keeper', 'server', 'server', None, None, None, None])

    def test_manifest_without_trace_is_refused(self):
        m = manifest('io-sizes.narrow-compact')
        m['params']['trace'] = False
        with self.assertRaisesRegex(ValueError, 'trace = true'):
            io_sizes.IoSizes().measured(SimpleNamespace(manifest=m, pass_dir='x'))



class FitTest(unittest.TestCase):
    """The pooling and hold-out rules of `ebs_fit`, on synthetic records."""

    def record(self, mid, experiment_id, observations, **extra):
        return {'status': 'ok', 'policy': 'local', 'measurement_id': mid, 'experiment_id': experiment_id,
                'observations': observations, **extra}

    def test_pooled_scalar_and_hold_out_slack(self):
        from yardstick.cost import ebs_fit
        fit = ebs_fit.pooled({'a': 1.0, 'b': 1.2, 'c': 1.4})
        self.assertAlmostEqual(fit['value'], 1.2)
        self.assertAlmostEqual(fit['spread'], 0.2)
        # slack = 2 x 0.2 + 10 % of 1.2 + 0.005 = 0.525
        self.assertTrue(ebs_fit.scalar_check(1.72, fit)['ok'])
        self.assertFalse(ebs_fit.scalar_check(1.73, fit)['ok'])
        self.assertEqual(fit['max'], 1.4)
        self.assertTrue(ebs_fit.envelope_check(1.53, fit)['ok'])
        self.assertFalse(ebs_fit.envelope_check(1.55, fit)['ok'])
        with self.assertRaises(ValueError):
            ebs_fit.pooled({})

    def test_demand_fit_skips_a_direction_without_bytes(self):
        from yardstick.cost import ebs_fit
        dist = dict.fromkeys((label for label, _ in io_sizes.SIZE_BUCKETS), 0.0)
        obs = lambda w: {'inserts.io_bytes.write': w, 'inserts.io_bytes.read': 0.0,  # noqa: E731
                         'inserts.ebs_ios_per_mib.write': 4.0,
                         'inserts.request_size_distribution.write': {**dist, '1MiB': 1.0}}
        recs = [self.record('m1', 'io-sizes.narrow-compact', obs(1.0)), self.record('m2', 'io-sizes.narrow-wide', obs(1.1)),
                self.record('m3', 'io-sizes.narrow-wide', obs(9.9), status='FAILED')]
        got = ebs_fit.demand_fit(recs, {'io-sizes.narrow-compact', 'io-sizes.narrow-wide'})
        self.assertAlmostEqual(got['io_bytes_per_op']['inserts.write']['value'], 1.05)
        self.assertIn('inserts.read', got['io_bytes_per_op'])
        self.assertNotIn('inserts.read', got['ebs_ios_per_mib'])  # no read bytes: no I/O size to count
        self.assertEqual(got['io_request_size_distribution']['inserts.write']['max_tv'], 0.0)
        self.assertEqual(got['measurement_ids'], ['m1', 'm2'])

    def test_peak_factor_window_ends_at_the_last_merge(self):
        from yardstick.cost import ebs_fit
        series = [[t * 1_000_000, 7, 0, w] for t, w in enumerate([0, 1, 2, 7, 8, 8, 8])]
        rec = self.record('m', 'merge-natural.default', {'quiet_cutoff_us': 6_000_000, 'proc_io_series': {'r1': series}},
                          window={'t_start_us': 0})
        got = ebs_fit.peak_factors([rec], {'merge-natural.default'}, 0.0, {'merge-natural.default': 2})
        # Active period 0..4 s: 8 bytes in 4 s, peak 5 B/s; the quiet tail after the last merge is excluded.
        self.assertAlmostEqual(got['m']['peak_factor'], 2.5)
        with self.assertRaises(ValueError):
            ebs_fit.active_window(self.record('x', 'merge-natural.default', {}, window={'t_start_us': 0}), 60)

    def test_ops_values_per_unit_net_of_the_baseline(self):
        from yardstick.cost import ebs_fit
        parts = lambda names: [{'name': n, 'rows': 100, 'active': 1} for n in names]  # noqa: E731
        recs = [self.record('m', 'mutation.compact-update-one-column', {'parts_before': parts(['a', 'b'])},
                            card='mutation', proc_io={'r1': {'write_bytes': 3000}}, wall_s=10),
                self.record('d', 'drop.partition', {'parts_before': parts(['a', 'b', 'c']), 'parts_after': parts(['c'])},
                            card='drop', proc_io={'r1': {'write_bytes': 1200}}, wall_s=10),
                self.record('s', 'startup.intact', {}, card='startup', proc_io={'r1': {'write_bytes': 5000}}, wall_s=10)]
        got = ebs_fit.ops_values(recs, ebs_fit.OPS_FIT, 100.0)
        self.assertEqual(got, {'mutations': {'m': 10.0}, 'deletes': {'d': 100.0}, 'recovery': {'s': 4000.0}})
        fit = ebs_fit.pooled({'a': 10.0, 'b': 12.0})
        self.assertTrue(ebs_fit.ops_check(14.6, fit)['ok'])   # slack 2 x 1 + 0.15 x 11 = 3.65
        self.assertFalse(ebs_fit.ops_check(14.7, fit)['ok'])

    def test_holdout_mode_reproduces_the_committed_ebs_table(self):
        import contextlib
        import io
        from yardstick.cost import ebs_fit
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', '..', 'docs', 'superpowers',
                            'reports', '2026-10-24-storage-cost-ebs', 'measurements.jsonl')
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            self.assertEqual(ebs_fit.main(['--holdout', 'io-sizes.mixed-holdout-3', path]), 0)
        got = json.loads(out.getvalue())
        # The README hold-out table: inserts.write 1.0209 against 1.0323 +- 0.1794, TV 0.096 <= 0.378.
        w = got['io_bytes_per_op.inserts.write']
        self.assertEqual((round(w['value'], 4), round(w['fit'], 4), round(w['slack'], 4)), (1.0209, 1.0323, 0.1794))
        d = got['io_request_size_distribution.inserts.write']
        self.assertEqual((round(d['tv'], 3), round(d['limit'], 3)), (0.096, 0.378))
        self.assertEqual(len(got), 10)


class PhaseWindowTest(unittest.TestCase):
    def test_logical_bytes_are_read_after_the_window_closes(self):
        # SYSTEM FLUSH LOGS writes the part log to disk; inside the window it would count as the operation's writes.
        card, events = io_sizes.IoSizes(), []
        card._io = lambda ctx: events.append('io') or {'r1': {'write_bytes': len(events)}}
        ctx = SimpleNamespace(pass_dir='p')
        card._phase(ctx, 'insert', lambda: events.append('work'), lambda: events.append('logical') or 7)
        self.assertEqual(events, ['io', 'work', 'io', 'logical'])
        self.assertEqual(card.state(ctx)['phases']['insert']['logical_bytes'], 7)


if __name__ == '__main__':
    unittest.main()
