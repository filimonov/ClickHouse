"""Fixture tests of the lifecycle (R5) cards, manifests and the arithmetic their predictions rest on."""
import copy
import json
import os
import sys
import tempfile
import unittest
from unittest import mock

from yardstick.cost import cli, ledger as L
from yardstick.cost.cards import CARDS, lifecycle, probes
from yardstick.cost.cards.base import CardContext
from yardstick.cost.ledger import Inventory

TOOLS = os.path.join(probes.REPO, 'utils', 'storage-cost-calculator', 'tools')
sys.path.insert(0, TOOLS)
import artifacts  # noqa: E402

R = os.path.join(probes.REPO, 'docs', 'superpowers', 'reports', '2026-10-24-storage-cost-lifecycle')
EXPERIMENTS = os.path.join(R, 'experiments')
with open(os.path.join(probes.REPO, 'docs', 'superpowers', 'cost-model', 'inputs-registry.json')) as _f:
    REGISTRY = json.load(_f)
MIB = 1 << 20
CARD_NAMES = ('boundaries', 'small-inserts', 'merge-natural', 'merge-forced', 'async-inserts', 'amplification-run')
CASES = {'boundaries': {'compact-below-bytes', 'wide-equal-bytes', 'wide-above-bytes', 'merge-cross', 'level-cross',
                        'multipart-below', 'multipart-equal', 'multipart-above', 'insert-split'},
         'small-inserts': {'default'}, 'merge-natural': {'default', 'wide'}, 'merge-forced': {'default'},
         'async-inserts': {'steady', 'bursty'}, 'amplification-run': {'2h'}}


def manifest(experiment_id):
    with open(os.path.join(EXPERIMENTS, f'{experiment_id}.json')) as f:
        m = json.load(f)
    m['params']['report_dir'] = R
    return m


def fake_registry(**values):
    return {'quantities': [{'id': k, 'default': v} for k, v in values.items()]}


class FakeClient:
    def __init__(self, answers=None):
        self.answers, self.log = answers or {}, []

    def query(self, sql, settings=None, timeout=3600):
        self.log.append(sql)
        return next((a for needle, a in self.answers.items() if needle in sql), '0\n')


class FakeCluster:
    def __init__(self, n=2, answers=None):
        self.clients = [FakeClient(answers) for _ in range(n)]


class FakeProxy:
    def __init__(self, path):
        self.ledger_path = path

    def flush(self):
        pass


def ctx_for(m, cluster, tmp, ledger_path):
    attempts = lambda: [r for r in L.read_rows(ledger_path) if r.get('kind') == 'attempt']  # noqa: E731
    return CardContext(cluster=cluster, proxy=FakeProxy(ledger_path), rig=None, pass_dir=tmp, local_dirs=[],
                       policy='s3', variant='s3', cache_enabled=False, manifest=m,
                       replay=lambda: L.replay(attempts(), Inventory(), []),
                       window=lambda: {'start_seq': 1, 't_start_us': 1_000_000})


def jsonl(rows):
    return ''.join(json.dumps(r) + '\n' for r in rows)


class ManifestsTest(unittest.TestCase):
    def test_every_manifest_validates(self):
        names = sorted(os.listdir(EXPERIMENTS))
        self.assertEqual({tuple(n[:-5].split('.', 1)) for n in names},
                         {(card, case) for card, cases in CASES.items() for case in cases})
        for name in names:
            with open(os.path.join(EXPERIMENTS, name)) as f:
                m = json.load(f)
            self.assertEqual(artifacts.validate_experiment(m), [], name)
            self.assertIn(m['card'], CARDS, name)
            self.assertEqual(m['params']['replica'], 'r1', name)
            # The runner never stops merges itself: a `stop` manifest carries the table setting that does.
            stopped = m['settings']['table'].get('max_parts_to_merge_at_once') == 1
            self.assertEqual(stopped, m['controls']['merges'] == 'stop', name)

    def test_every_manifest_has_a_valid_prediction_per_policy(self):
        for name in sorted(os.listdir(EXPERIMENTS)):
            m = manifest(name[:-5])
            for policy in m['policies']:
                pred = probes.prediction(m['experiment_id'], policy, R)
                self.assertEqual(artifacts.validate_prediction(pred), [], name)
                self.assertTrue(pred['source_refs'], name)
                for ref in pred['source_refs']:
                    self.assertTrue(ref['path'].startswith('tmp/src-26.8/'), ref)
                    with open(os.path.join(probes.REPO, ref['path'])) as f:
                        self.assertLessEqual(ref['line'], sum(1 for _ in f), ref)

    def test_hold_out_amplification_check_reads_the_committed_fit(self):
        pred = probes.prediction('merge-natural.wide', 's3', R)
        want = pred['expected']['observations']['amplification_bytes']
        self.assertEqual((want['model'], want['fan_in']), ('rewrite_levels', pred['inputs']['merge_effective_fan_in']))
        self.assertEqual(len(pred['inputs']['fit_measurement_ids']), 6)

    def test_lifecycle_cards_registered(self):
        self.assertTrue(set(CARD_NAMES) <= set(CARDS))
        self.assertIsInstance(CARDS['merge-natural'], lifecycle.MergeNatural)

    def test_cli_report_dir_reaches_the_card(self):
        found = {m['experiment_id']: m for m in cli._load_manifests([R])}
        self.assertEqual(found['boundaries.merge-cross']['params']['report_dir'], R)


class ArithmeticTest(unittest.TestCase):
    def test_boundaries_cases_derive_sizes_from_registry_thresholds(self):
        reg = fake_registry(min_bytes_for_wide_part=10 * MIB, multipart_first_part_bytes=32 * MIB,
                            multipart_next_part_bytes=16 * MIB)
        cases = lifecycle.boundary_cases(reg, row_bytes=40)
        below, above = cases['compact-below-bytes']['rows'] * 40, cases['wide-above-bytes']['rows'] * 40
        self.assertTrue(9 * MIB - 40 < below <= 9 * MIB)
        self.assertTrue(11 * MIB <= above < 11 * MIB + 40)
        self.assertEqual(cases['wide-equal-bytes']['rows'] * 40, 10 * MIB)
        merge = cases['merge-cross']
        self.assertEqual(merge['inserts'], 2)
        self.assertLess(merge['rows'] * 40, 10 * MIB)
        self.assertGreaterEqual(2 * merge['rows'] * 40, 10 * MIB)
        self.assertEqual(cases['level-cross']['min_level_for_wide_part'], 1)
        self.assertEqual(lifecycle.bin_bytes(cases['multipart-equal']['rows']), 32 * MIB)
        self.assertEqual(lifecycle.bin_bytes(cases['multipart-below']['rows']), 32 * MIB - 8)
        self.assertEqual(lifecycle.bin_bytes(cases['multipart-above']['rows']), 48 * MIB + 8)
        # A smaller threshold moves every case with it: the row counts are never fixed numbers.
        small = lifecycle.boundary_cases(fake_registry(min_bytes_for_wide_part=MIB, multipart_first_part_bytes=8 * MIB,
                                                       multipart_next_part_bytes=5 * MIB), row_bytes=40)
        self.assertTrue(MIB <= small['wide-equal-bytes']['rows'] * 40 < MIB + 40)  # 1 MiB is not a multiple of 40
        self.assertEqual(lifecycle.bin_bytes(small['multipart-equal']['rows']), 8 * MIB)

    def test_committed_boundaries_manifests_match_the_registry_derivation(self):
        derived = lifecycle.boundary_cases(REGISTRY, row_bytes=40)
        for case, want in derived.items():
            m = manifest(f'boundaries.{case}')
            self.assertEqual((m['params']['rows'], m['params']['inserts']), (want['rows'], want['inserts']), case)
            self.assertEqual(m['settings']['table'].get('min_level_for_wide_part'), want.get('min_level_for_wide_part'))

    def test_upload_requests_follow_the_buffer_policy(self):
        first, second = 32 * MIB, 16 * MIB
        self.assertEqual(lifecycle.upload_requests(first, first, second), ('put', [first]))
        self.assertEqual(lifecycle.upload_requests(first - 8, first, second), ('put', [first - 8]))
        self.assertEqual(lifecycle.upload_requests(first + second + 8, first, second),
                         ('multipart', [first, second, 8]))
        # The 501st buffer is the first to grow; it is full here, so its part shows the doubled size.
        kind, parts = lifecycle.upload_requests(first + 499 * second + 2 * second, first, second)
        self.assertEqual((kind, len(parts), parts[-1], parts[-2]), ('multipart', 501, 2 * second, second))

    def test_rows_for_bin_bytes_rejects_unreachable_sizes(self):
        with self.assertRaises(ValueError):
            lifecycle.rows_for_bin_bytes(32 * MIB - 1)
        self.assertEqual(lifecycle.bin_bytes(lifecycle.rows_for_bin_bytes(65561 * 3)), 65561 * 3)

    def test_squashed_parts(self):
        self.assertEqual(lifecycle.squashed_parts(2621122, 1048449, 65409), [1111953, 1111953, 397216])
        self.assertEqual(lifecycle.squashed_parts(1000, 1048449, 65409), [1000])

    def test_rewrite_levels_is_continuous_and_matches_whole_levels(self):
        self.assertEqual(lifecycle.rewrite_levels(1, 5), 0)
        self.assertAlmostEqual(lifecycle.rewrite_levels(5, 5), 1)
        self.assertAlmostEqual(lifecycle.rewrite_levels(25, 5), 2)
        self.assertAlmostEqual(lifecycle.rewrite_levels(64, 5), 2 + (64 / 25 - 1) / 4)
        self.assertAlmostEqual(lifecycle.amplification_model([25, 25], 5), 3)

    def test_rewrite_levels_caps_at_twelve_like_the_registry_ladder(self):
        self.assertEqual(lifecycle.rewrite_levels(5 ** 12, 5), 12)
        self.assertEqual(lifecycle.rewrite_levels(5 ** 13, 5), 12)
        self.assertAlmostEqual(lifecycle.rewrite_levels(5 ** 12 - 1, 5), 12, places=6)

    def test_amplification_rule_is_the_registry_formula_at_the_row_weighted_ratio(self):
        finals = [23, 23, 6, 6, 6]
        r = lifecycle.weighted_ratio(finals)
        self.assertAlmostEqual(r, (2 * 23 ** 2 + 3 * 6 ** 2) / 64)
        self.assertAlmostEqual(lifecycle.amplification_rule(finals, 6.1), 1 + lifecycle.rewrite_levels(r, 6.1))
        self.assertAlmostEqual(lifecycle.amplification_rule([32], 6.1), lifecycle.amplification_model([32], 6.1))
        e, a = lifecycle.check_observation({'model': 'rewrite_levels_at_weighted_ratio', 'fan_in': 6.1, 'abs': 0.01,
                                            'rel': 0}, lifecycle.amplification_rule(finals, 6.1),
                                           {'final_size_ratios': finals})
        self.assertEqual(e, a)
        with self.assertRaises(ValueError):
            lifecycle.check_observation({'model': 'unknown', 'fan_in': 6, 'abs': 0, 'rel': 0}, 1.0,
                                        {'final_size_ratios': finals})

    def test_check_observation_kinds(self):
        self.assertEqual(lifecycle.check_observation(3, 3, {}), (3, 3))
        e, a = lifecycle.check_observation({'min': 2, 'max': 5}, 4, {})
        self.assertEqual(e, a)
        e, a = lifecycle.check_observation({'min': 2, 'max': 5}, 6, {})
        self.assertNotEqual(e, a)
        e, a = lifecycle.check_observation({'reference': {'2': 0.5, '3': 0.5}, 'max_tv': 0.1}, {'2': 0.55, '3': 0.45}, {})
        self.assertEqual(e, a)
        e, a = lifecycle.check_observation({'reference': {'2': 1.0}, 'max_tv': 0.1}, {'3': 1.0}, {})
        self.assertNotEqual(e, a)
        obs = {'final_size_ratios': [25, 25]}
        e, a = lifecycle.check_observation({'model': 'rewrite_levels', 'fan_in': 5, 'abs': 0.1, 'rel': 0}, 3.05, obs)
        self.assertEqual(e, a)
        e, a = lifecycle.check_observation({'model': 'rewrite_levels', 'fan_in': 5, 'abs': 0.1, 'rel': 0}, 3.5, obs)
        self.assertNotEqual(e, a)


PART_LOG = [
    {'event_type': 'NewPart', 'part_name': f'all_{i}_{i}_0', 'part_type': 'Compact', 'rows': 100, 'size_in_bytes': 1000,
     'bytes_uncompressed': 4000, 'merged_from': [], 't_us': i} for i in range(1, 7)
] + [
    {'event_type': 'MergeParts', 'part_name': 'all_1_3_1', 'part_type': 'Compact', 'rows': 300, 'size_in_bytes': 2500,
     'bytes_uncompressed': 12000, 'merged_from': ['all_1_1_0', 'all_2_2_0', 'all_3_3_0'], 't_us': 10},
    {'event_type': 'MergeParts', 'part_name': 'all_1_6_2', 'part_type': 'Wide', 'rows': 600, 'size_in_bytes': 5000,
     'bytes_uncompressed': 24000, 'merged_from': ['all_1_3_1', 'all_4_4_0', 'all_5_5_0', 'all_6_6_0'], 't_us': 20},
]


class CardsTest(unittest.TestCase):
    def test_lifecycle_card_runs_its_sql_on_the_named_replica(self):
        m = manifest('boundaries.compact-below-bytes')
        card = CARDS['boundaries']
        cluster = FakeCluster(n=2, answers={'FORMAT JSONEachRow': ''})
        with tempfile.TemporaryDirectory() as tmp:
            ledger = os.path.join(tmp, 'ledger.jsonl')
            open(ledger, 'w').close()
            ctx = ctx_for(m, cluster, tmp, ledger)
            card.setup(ctx)
            card.measured(ctx)
            card.complete(ctx, timeout_s=1)
            self.assertTrue(any(s.startswith('CREATE TABLE') for s in cluster.clients[0].log))
            self.assertTrue(any(s.startswith('INSERT INTO yardstick.t') for s in cluster.clients[0].log))
            # SYSTEM FLUSH LOGS reaches every server; workload SQL only the named one.
            self.assertEqual([s for s in cluster.clients[1].log if s != 'SYSTEM FLUSH LOGS'], [])
            other = copy.deepcopy(m)
            other['params']['replica'] = 'r2'  # the card has one replica: r2 is not one of them
            with self.assertRaises(ValueError):
                card.measured(ctx_for(other, cluster, tmp, ledger))

    def test_parts_are_ordered_inserts_before_their_merge_results(self):
        m = manifest('merge-forced.default')
        cluster = FakeCluster(n=1, answers={'FORMAT JSONEachRow': ''})
        with tempfile.TemporaryDirectory() as tmp:
            CARDS['merge-forced'].parts(ctx_for(m, cluster, tmp, os.path.join(tmp, 'l')))
        self.assertIn('ORDER BY level, min_block_number, max_block_number', cluster.clients[0].log[-1])

    def test_insert_sql_carries_rows_seed_and_index(self):
        m = manifest('merge-natural.default')
        cluster = FakeCluster(n=1)
        with tempfile.TemporaryDirectory() as tmp:
            ctx = ctx_for(m, cluster, tmp, os.path.join(tmp, 'l'))
            CARDS['merge-natural'].insert(ctx, 3)
        sql = cluster.clients[0].log[-1]
        self.assertIn('numbers(100000)', sql)
        self.assertIn('number + 3 * 100000', sql)
        self.assertIn(f'sipHash64(k, {m["params"]["seed"]})', sql)

    def test_merge_natural_observations_shape(self):
        obs = lifecycle.merge_observations(PART_LOG, active_rows=[600], rows_per_insert=100)
        self.assertEqual(obs['part_log_merges'], [
            {'fan_in': 3, 'bytes_in': 3000, 'bytes_out': 2500, 'level': 1, 'rows': 300, 'part_type': 'Compact'},
            {'fan_in': 4, 'bytes_in': 5500, 'bytes_out': 5000, 'level': 2, 'rows': 600, 'part_type': 'Wide'}])
        self.assertEqual((obs['merges'], obs['inserted_parts'], obs['inserted_rows'], obs['inserted_bytes']),
                         (2, 6, 600, 6000))
        self.assertEqual(obs['fan_in_distribution']['3'], 0.5)
        self.assertEqual(obs['fan_in_distribution']['4'], 0.5)
        self.assertEqual(obs['result_size_distribution']['2-4'], 0.5)
        self.assertEqual(obs['result_size_distribution']['5-8'], 0.5)
        self.assertAlmostEqual(obs['amplification_bytes'], (6000 + 7500) / 6000)
        self.assertAlmostEqual(obs['amplification_rows'], (600 + 900) / 600)
        self.assertEqual((obs['max_level'], obs['active_parts'], obs['final_size_ratios']), (2, 1, [6.0]))

    def test_merge_observations_fail_on_unknown_source(self):
        broken = [r for r in PART_LOG if r['part_name'] != 'all_2_2_0']
        with self.assertRaises(ValueError):
            lifecycle.merge_observations(broken, [600], 100)

    def test_natural_merge_card_collects_the_part_log(self):
        m = manifest('merge-natural.default')
        m['params']['quiet_s'], m['params']['quiesce_timeout_s'] = 0, 1
        parts = [{'name': 'all_1_6_2', 'part_type': 'Wide', 'rows': '600', 'level': '2',
                  'data_uncompressed_bytes': '24000', 'active': 1}]
        cluster = FakeCluster(n=1, answers={"event_type IN ('NewPart', 'MergeParts')": jsonl(PART_LOG),
                                            'FROM system.parts WHERE': jsonl(parts),
                                            'FROM system.remote_data_paths': ''})
        card = CARDS['merge-natural']
        with tempfile.TemporaryDirectory() as tmp:
            ctx = ctx_for(m, cluster, tmp, os.path.join(tmp, 'l'))
            card.complete(ctx, timeout_s=1)
            m['params']['rows'] = 100
            obs = card.observe(ctx, {})
        self.assertTrue(obs['quiesced'])
        self.assertEqual(obs['merges'], 2)
        self.assertEqual(obs['final_size_ratios'], [6.0])

    def test_quiet_period_counts_a_merge_finished_between_polls(self):
        # No merge is ever active at a poll, but one finished at t = 5 s: the 10 s quiet period ends at 15 s.
        clock = {'t': 1000.0}
        m = manifest('merge-natural.default')
        m['params']['quiet_s'], m['params']['quiesce_timeout_s'] = 10, 60
        answers = {'event_type = \'MergeParts\'': f'{int((clock["t"] + 5) * 1e6)}\n'}
        cluster = FakeCluster(n=1, answers=answers)
        card = CARDS['merge-natural']

        def sleep(s):
            clock['t'] += s
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch.object(lifecycle.time, 'time', lambda: clock['t']), \
                mock.patch.object(lifecycle.time, 'sleep', sleep), \
                mock.patch.object(lifecycle.LifecycleCard, 'collect_layout', lambda self, ctx: None), \
                mock.patch.object(lifecycle.ManifestCard, 'complete', lambda self, ctx, timeout_s=300: None):
            ctx = ctx_for(m, cluster, tmp, os.path.join(tmp, 'l'))
            card.wait_quiet(ctx)
            self.assertTrue(card.state(ctx)['quiesced'])
            self.assertGreaterEqual(clock['t'], 1015.0)
            clock['t'] = 1000.0
            m['params']['quiesce_timeout_s'] = 12
            ctx = ctx_for(m, cluster, os.path.join(tmp, 'x'), os.path.join(tmp, 'l'))
            card.wait_quiet(ctx)
            self.assertFalse(card.state(ctx)['quiesced'])

    def test_completion_waits_again_when_the_final_part_log_shows_a_later_merge(self):
        # The quiet period was accepted at t, but the part log read afterwards holds a merge that ended after
        # t - quiet_s (it ran between the last flush and the last active poll): completion must wait again.
        m = manifest('merge-natural.default')
        m['params']['quiet_s'], m['params']['quiesce_timeout_s'] = 10, 100
        card = CARDS['merge-natural']
        accepted = iter([1010.0, 1030.0])
        late = [dict(PART_LOG[-1], t_us=int(1005e6))]
        logs = iter([PART_LOG[:-1] + late, PART_LOG[:-1] + late])
        calls = {'wait': 0}

        def wait_quiet(self, ctx, deadline=None):
            calls['wait'] += 1
            self.state(ctx)['quiesced'] = True
            return next(accepted)
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch.object(lifecycle.NaturalMergeCard, 'wait_quiet', wait_quiet), \
                mock.patch.object(lifecycle.ManifestCard, 'complete', lambda self, ctx, timeout_s=300: None), \
                mock.patch.object(lifecycle.LifecycleCard, 'part_log', lambda self, ctx: next(logs)), \
                mock.patch.object(lifecycle.LifecycleCard, 'parts', lambda self, ctx: []), \
                mock.patch.object(lifecycle.LifecycleCard, 'collect_layout', lambda self, ctx: None):
            ctx = ctx_for(m, FakeCluster(n=1), tmp, os.path.join(tmp, 'l'))
            card.complete(ctx, timeout_s=1)
            self.assertEqual(calls['wait'], 2)
            self.assertTrue(card.state(ctx)['quiesced'])
            self.assertEqual(card.state(ctx)['quiet_cutoff'], 1030.0)

    def test_completion_waits_again_when_an_active_part_has_no_part_log_row(self):
        # The final active parts include all_1_9_1, which no part_log row explains: a merge ended after the flush.
        m = manifest('merge-natural.default')
        m['params']['quiet_s'], m['params']['quiesce_timeout_s'] = 10, 100
        card = CARDS['merge-natural']
        accepted = iter([1010.0, 1030.0])
        actives = iter([[{'name': 'all_1_9_1', 'rows': 600, 'active': 1}], [{'name': 'all_1_6_2', 'rows': 600, 'active': 1}]])
        calls = {'wait': 0}

        def wait_quiet(self, ctx, deadline=None):
            calls['wait'] += 1
            self.state(ctx)['quiesced'] = True
            return next(accepted)
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch.object(lifecycle.NaturalMergeCard, 'wait_quiet', wait_quiet), \
                mock.patch.object(lifecycle.ManifestCard, 'complete', lambda self, ctx, timeout_s=300: None), \
                mock.patch.object(lifecycle.LifecycleCard, 'part_log', lambda self, ctx: PART_LOG), \
                mock.patch.object(lifecycle.LifecycleCard, 'parts', lambda self, ctx: next(actives)), \
                mock.patch.object(lifecycle.LifecycleCard, 'collect_layout', lambda self, ctx: None):
            ctx = ctx_for(m, FakeCluster(n=1), tmp, os.path.join(tmp, 'l'))
            card.complete(ctx, timeout_s=1)
        self.assertEqual(calls['wait'], 2)
        self.assertTrue(card.state(ctx)['quiesced'])
        self.assertEqual(card.state(ctx)['active_rows'], [600])

    def test_no_workload_request_after_the_observation_cutoff(self):
        m = manifest('merge-natural.default')
        card = CARDS['merge-natural']
        rows = [{'kind': 'mark', 'seq': 1, 'label': 'w', 't_us': 1}]
        for seq, t in ((2, 900), (3, 1100), (4, 5000)):
            rows.append({'kind': 'attempt', 'seq': seq, 'caller': 'r1', 'op': 'PutObject', 'key': 'k', 't_start_us': t,
                         'req_payload_bytes': 1, 'upstream': {'outcome': 'ok'}, 'response_body': {}})
        with tempfile.TemporaryDirectory() as tmp:
            ledger = os.path.join(tmp, 'ledger.jsonl')
            with open(ledger, 'w') as f:
                f.write(jsonl(rows))
            ctx = ctx_for(m, FakeCluster(n=1), tmp, ledger)
            card.state(ctx)['cutoff_us'] = 1000
            self.assertEqual(card.after_cutoff(ctx, {'window': {'t_end_us': 2000}}), [3])
            card.state(ctx)['cutoff_us'] = 1200
            self.assertEqual(card.after_cutoff(ctx, {'window': {'t_end_us': 2000}}), [])

    def test_observation_cutoff_is_recorded(self):
        m = manifest('merge-natural.default')
        card = CARDS['merge-natural']
        with tempfile.TemporaryDirectory() as tmp:
            ctx = ctx_for(m, FakeCluster(n=1), tmp, os.path.join(tmp, 'l'))
            st = card.state(ctx)
            st.update(part_log=PART_LOG, active_rows=[600], quiesced=True, quiet_cutoff=1010.5, cutoff_us=1010600000)
            m['params']['rows'] = 100
            obs = card.observe(ctx, {})
        self.assertEqual((obs['quiet_cutoff_us'], obs['cutoff_us']), (1010500000, 1010600000))

    def test_layout_accumulates_across_snapshots(self):
        # A long run snapshots the layout before old_parts_lifetime can remove a merged-away part's files.
        m = manifest('merge-natural.default')
        card = CARDS['merge-natural']
        first = jsonl([{'local_path': 'store/a/b/all_1_1_0/data.bin', 'remote_path': 'p/k1'}])
        second = jsonl([{'local_path': 'store/a/b/all_1_2_1/data.bin', 'remote_path': 'p/k2'}])
        with tempfile.TemporaryDirectory() as tmp:
            cluster = FakeCluster(n=1, answers={'FROM system.remote_data_paths': first})
            ctx = ctx_for(m, cluster, tmp, os.path.join(tmp, 'l'))
            card.collect_layout(ctx)
            cluster.clients[0].answers = {'FROM system.remote_data_paths': second}
            card.collect_layout(ctx)
            self.assertEqual(card.state(ctx)['remote_paths'], {'all_1_1_0': ['p/k1'], 'all_1_2_1': ['p/k2']})

    def test_layout_skips_files_without_an_object(self):
        # A CAS part keeps small files inline in its manifest: system.remote_data_paths lists them with an empty
        # remote path, and no object of the store backs them.
        m = manifest('merge-natural.default')
        m['policies'] = ['cas']
        card = CARDS['merge-natural']
        rows = jsonl([{'local_path': 'store/a/b/all_1_1_0/data.bin', 'remote_path': 'p/blob1'},
                      {'local_path': 'store/a/b/all_1_1_0/count.txt', 'remote_path': ''}])
        with tempfile.TemporaryDirectory() as tmp:
            cluster = FakeCluster(n=1, answers={'FROM system.remote_data_paths': rows})
            ctx = ctx_for(m, cluster, tmp, os.path.join(tmp, 'l'))
            card.collect_layout(ctx)
            self.assertEqual(card.state(ctx)['remote_paths'], {'all_1_1_0': ['p/blob1']})

    def test_long_runs_snapshot_the_layout_while_inserting(self):
        m = manifest('merge-natural.default')
        m['params'].update(inserts=3, interval_s=0)
        card = CARDS['amplification-run']
        snapshots = []
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(lifecycle, 'LAYOUT_EVERY_S', 0), \
                mock.patch.object(lifecycle.LifecycleCard, 'collect_layout', lambda self, ctx: snapshots.append(1)):
            card.measured(ctx_for(m, FakeCluster(n=1), tmp, os.path.join(tmp, 'l')))
        self.assertEqual(len(snapshots), 3)

    def test_merged_away_parts_become_outdated_at_their_merge(self):
        card = CARDS['merge-natural']
        m = manifest('merge-natural.default')
        with tempfile.TemporaryDirectory() as tmp:
            ctx = ctx_for(m, FakeCluster(n=1), tmp, os.path.join(tmp, 'l'))
            st = card.state(ctx)
            st['part_log'] = PART_LOG
            st['remote_paths'] = {f'all_{i}_{i}_0': [f'p/r1/k{i}a', f'p/r1/k{i}b'] for i in range(1, 7)}
            st['remote_paths'].update({'all_1_3_1': ['p/r1/m1'], 'all_1_6_2': ['p/r1/m2']})
            inv = Inventory()
            for key in [k for v in st['remote_paths'].values() for k in v]:
                inv.objects[('b', key)] = {'size': 1, 'category': 'active', 'created_us': 1, 'uncertain': False}
            bounds = L.Bounds(inv, copy.deepcopy(inv), {('b', k): [] for k in [k for v in st['remote_paths'].values() for k in v]})
            card.reclassify(ctx, bounds)
            got = {(rc['t_us'], tuple(sorted(k[1] for k in rc['keys']))) for rc in bounds.reclassified}
            self.assertEqual(got, {(10, ('p/r1/k1a', 'p/r1/k1b', 'p/r1/k2a', 'p/r1/k2b', 'p/r1/k3a', 'p/r1/k3b')),
                                   (20, ('p/r1/k4a', 'p/r1/k4b', 'p/r1/k5a', 'p/r1/k5b', 'p/r1/k6a', 'p/r1/k6b',
                                         'p/r1/m1'))})
            self.assertTrue(all(rc['category'] == 'outdated' for rc in bounds.reclassified))
            # A merged-away file whose object the pass never wrote cannot be classified: fail closed.
            st['remote_paths']['all_1_1_0'].append('p/r1/unknown')
            with self.assertRaises(ValueError):
                card.reclassify(ctx, L.Bounds(Inventory(), Inventory(), {}))

    def test_small_inserts_control_is_measure(self):
        m = manifest('small-inserts.default')
        card = CARDS['small-inserts']
        with tempfile.TemporaryDirectory() as tmp:
            ctx = ctx_for(m, FakeCluster(n=1), tmp, os.path.join(tmp, 'l'))
            self.assertEqual(card.controls(ctx)['merges'], 'measure')
            self.assertNotIn('max_parts_to_merge_at_once', card.settings(ctx)['table'])
        self.assertEqual((m['params']['inserts'], m['params']['rows'], m['params']['columns'].count('UInt64')),
                         (1000, 1000, 5))
        self.assertEqual(card.mode, 'stochastic')

    def test_boundaries_observes_upload_part_sizes_and_large_puts(self):
        m = manifest('boundaries.multipart-above')
        card = CARDS['boundaries']
        rows = [{'kind': 'mark', 'seq': 1, 'label': 'w', 't_us': 1}]

        def add(op, payload, **kw):
            body = kw.pop('response_body', {})
            rows.append({'kind': 'attempt', 'seq': len(rows) + 1, 'caller': 'r1', 'op': op, 'key': 'k',
                         'req_payload_bytes': payload, 'upstream': {'outcome': 'ok'}, 'response_body': body,
                         'upload_id': kw.get('upload_id'), 'part_number': kw.get('part_number')})
        add('CreateMultipartUpload', 0, response_body={'upload_id': 'u1'})
        add('UploadPart', 8, upload_id='u1', part_number=3)
        add('UploadPart', 32 * MIB, upload_id='u1', part_number=1)
        add('UploadPart', 16 * MIB, upload_id='u1', part_number=2)
        add('PutObject', 2 * MIB)
        add('PutObject', 100)
        with tempfile.TemporaryDirectory() as tmp:
            ledger = os.path.join(tmp, 'ledger.jsonl')
            with open(ledger, 'w') as f:
                f.write(jsonl(rows))
            ctx = ctx_for(m, FakeCluster(n=1), tmp, ledger)
            card.state(ctx)['parts'] = []
            obs = card.observe(ctx, {})
            card.state(ctx)['parts'] = [{'name': 'all_1_1_0', 'part_type': 'Wide', 'rows': 5, 'level': 0, 'active': 1},
                                        {'name': 'all_2_2_0', 'part_type': 'Wide', 'rows': 9, 'level': 0, 'active': 1},
                                        {'name': 'all_1_2_1', 'part_type': 'Wide', 'rows': 14, 'level': 1, 'active': 0}]
            self.assertEqual(card.observe(ctx, {})['active_parts_by_rows'], [[9, 'Wide', 0], [5, 'Wide', 0]])
        self.assertEqual(obs['upload_part_sizes'], [[32 * MIB, 16 * MIB, 8]])
        self.assertEqual(obs['active_parts_by_rows'], [])
        self.assertEqual(obs['large_put_sizes'], [2 * MIB])

    def test_assertions_compare_predicted_observations_both_directions(self):
        m = manifest('boundaries.merge-cross')
        card = CARDS['boundaries']
        pred = probes.prediction(m['experiment_id'], 's3', R)
        fg = {c: {op: dict.fromkeys(L.COUNT_FIELDS, 0) | {'attempts': w if isinstance(w, int) else w['min']}
                  for op, w in ops.items()} for c, ops in pred['expected']['foreground'].items()}
        with tempfile.TemporaryDirectory() as tmp:
            ctx = ctx_for(m, FakeCluster(n=1), tmp, os.path.join(tmp, 'l'))
            card.state(ctx)['rows'] = [m['params']['rows'] * m['params']['inserts']]
            draft = {'ledger': {'foreground': fg}, 'observations': copy.deepcopy(pred['expected']['observations']),
                     'window': {'crossing': []}}
            out = card.assertions(ctx, draft)
            self.assertEqual(sorted(n for n, (e, a) in out.items() if e != a), [])
            draft['observations']['parts'][-1]['part_type'] = 'Compact'
            out = card.assertions(ctx, draft)
            self.assertEqual(sorted(n for n, (e, a) in out.items() if e != a), ['observation.parts'])


if __name__ == '__main__':
    unittest.main()
