"""Fixture tests of the `s3plain` (`s3_plain_rewritable`) capability gate and manifests (R3)."""
import json
import os
import sys
import unittest

from yardstick.cost.cards import CARDS, probes, s3plain
from yardstick.cost.runner import run_case

from .test_cost_runner import FakeSampler, RunnerFixture, measurements_problems

TOOLS = os.path.join(probes.REPO, 'utils', 'storage-cost-calculator', 'tools')
sys.path.insert(0, TOOLS)
import artifacts  # noqa: E402

R = os.path.join(probes.REPO, 'docs', 'superpowers', 'reports', '2026-10-24-storage-cost-s3plain')
EXPERIMENTS = os.path.join(R, 'experiments')
REPORTS = os.path.join(probes.REPO, 'docs', 'superpowers', 'reports')
S3_EXPERIMENTS = os.path.join(REPORTS, '2026-10-24-storage-cost-s3', 'experiments')
TWIN_DIRS = (S3_EXPERIMENTS, os.path.join(REPORTS, '2026-10-24-storage-cost-lifecycle', 'experiments'))
SRC = os.path.join(probes.REPO, 'tmp', 'src-26.8')
PLAIN = ('s3plain', 's3plain_cached')


def load(path):
    with open(path) as f:
        return json.load(f)


class CapabilityGateTest(unittest.TestCase):
    def test_unsupported_ops_reported_with_cited_reason(self):
        card = CARDS['mutation']
        for policy in PLAIN:
            ok, reason = card.supports(policy)
            self.assertFalse(ok, policy)
            self.assertTrue(reason.startswith('UNSUPPORTED'), reason)
            for ref in s3plain.GATES['mutations']['refs']:
                self.assertIn(f'{ref["path"]}:{ref["line"]}', reason)
        # The gate is the variant's, not the card's: s3 still mutates.
        self.assertEqual(card.supports('s3'), (True, ''))

    def test_every_gate_cites_a_source_line_that_says_what_the_gate_claims(self):
        if not os.path.isdir(SRC):
            self.skipTest('tmp/src-26.8 (the pinned source tree) is not checked out')
        for op, gate in s3plain.GATES.items():
            self.assertIn(gate['supported'], (True, False), op)
            self.assertTrue(gate['refs'], op)
            for ref in gate['refs']:
                with open(os.path.join(probes.REPO, ref['path'])) as f:
                    line = f.read().splitlines()[ref['line'] - 1]
                self.assertIn(ref['token'], line, f'{op}: {ref["path"]}:{ref["line"]}')

    def test_cards_of_supported_operations_accept_both_plain_policies(self):
        for name in ('objects-per-part', 'big-insert-multipart', 'merge-only', 'drop', 'cold-select', 'startup',
                     'boundaries'):
            for policy in PLAIN if name != 'boundaries' else PLAIN[:1]:
                self.assertEqual(CARDS[name].supports(policy), (True, ''), (name, policy))

    def test_gates_cover_every_cost_operation(self):
        coverage = load(os.path.join(probes.REPO, 'docs', 'superpowers', 'cost-model', 'coverage.json'))
        self.assertEqual(set(s3plain.GATES), set(coverage['domain']['operations']) - {'gc', 'lease'})


class UnsupportedRecordTest(RunnerFixture):
    def test_unsupported_policy_yields_an_unsupported_record_and_launches_nothing(self):
        m = load(os.path.join(EXPERIMENTS, 'mutation.wide-delete.json'))
        m['params']['report_dir'] = R
        for policy in PLAIN:
            record = run_case(CARDS['mutation'], m, policy, 0, self.deps(), cluster_factory=self.factory,
                              config_writer=self.writer, sampler_factory=FakeSampler)
            self.assertEqual(record['status'], 'UNSUPPORTED')
            self.assertIn('MergeTreeData.cpp:6240', record['reason'])
            self.assertEqual((record['variant'], record['cache_enabled']), ('s3plain', policy.endswith('_cached')))
            self.assertEqual(measurements_problems(record), [])
        self.assertEqual(self.calls, [])


class ManifestsTest(unittest.TestCase):
    def test_every_manifest_validates_and_runs_only_plain_policies(self):
        names = sorted(os.listdir(EXPERIMENTS))
        self.assertTrue(names)
        for name in names:
            m = load(os.path.join(EXPERIMENTS, name))
            self.assertEqual(artifacts.validate_experiment(m), [], name)
            self.assertIn(m['card'], CARDS, name)
            self.assertTrue(m['policies'] and set(m['policies']) <= set(PLAIN), name)

    def test_reused_manifests_keep_the_s3_parameters(self):
        # A reused case differs from its s3 twin by its policies only, so the s3 and s3plain counts compare 1:1.
        for name in sorted(os.listdir(EXPERIMENTS)):
            twins = [os.path.join(d, name) for d in TWIN_DIRS if os.path.exists(os.path.join(d, name))]
            self.assertEqual(len(twins), 1, f'{name}: every s3plain case reuses exactly one existing case')
            twin = twins[0]
            mine, theirs = load(os.path.join(EXPERIMENTS, name)), load(twin)
            for key in ('card', 'params', 'settings', 'controls', 'mode', 'replicas'):
                self.assertEqual(mine[key], theirs[key], f'{name}: {key}')

    def test_supported_manifests_have_a_valid_prediction_per_policy(self):
        for name in sorted(os.listdir(EXPERIMENTS)):
            m = load(os.path.join(EXPERIMENTS, name))
            for policy in m['policies']:
                path = os.path.join(R, 'predictions', f'{m["experiment_id"]}.{policy}.json')
                if not CARDS[m['card']].supports(policy)[0]:
                    self.assertFalse(os.path.exists(path), f'{path}: an unsupported pass needs no prediction')
                    continue
                pred = load(path)
                self.assertEqual(artifacts.validate_prediction(pred), [], path)
                self.assertTrue(pred['source_refs'], path)

    def test_mutation_cases_are_all_reused_and_unsupported(self):
        mutations = sorted(n for n in os.listdir(S3_EXPERIMENTS) if n.startswith('mutation.'))
        self.assertTrue(mutations)
        for name in mutations:
            m = load(os.path.join(EXPERIMENTS, name))
            self.assertEqual(m['policies'], list(PLAIN), name)


if __name__ == '__main__':
    unittest.main()
