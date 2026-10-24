"""Fixture tests of the lifecycle (R5) cards, manifests and the arithmetic their predictions rest on."""
import copy
import json
import os
import sys
import tempfile
import unittest

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
         'small-inserts': {'default'}, 'merge-natural': {'default'}, 'merge-forced': {'default'},
         'async-inserts': {'steady', 'bursty'}}


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
        cluster = FakeCluster(n=1, answers={'FROM system.part_log': jsonl(PART_LOG),
                                            'FROM system.parts WHERE': jsonl(parts)})
        card = CARDS['merge-natural']
        with tempfile.TemporaryDirectory() as tmp:
            ctx = ctx_for(m, cluster, tmp, os.path.join(tmp, 'l'))
            card.complete(ctx, timeout_s=1)
            m['params']['rows'] = 100
            obs = card.observe(ctx, {})
        self.assertTrue(obs['quiesced'])
        self.assertEqual(obs['merges'], 2)
        self.assertEqual(obs['final_size_ratios'], [6.0])

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
        self.assertEqual(obs['upload_part_sizes'], [[32 * MIB, 16 * MIB, 8]])
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
