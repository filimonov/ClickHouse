"""The validation report's manifests: each runs an existing card on policies it supports, with a valid prediction."""
import json
import os
import sys
import unittest

from yardstick.cost.cards import CARDS, probes

TOOLS = os.path.join(probes.REPO, 'utils', 'storage-cost-calculator', 'tools')
sys.path.insert(0, TOOLS)
import artifacts  # noqa: E402

R = os.path.join(probes.REPO, 'docs', 'superpowers', 'reports', '2026-10-24-storage-cost-validation')
EXPERIMENTS = os.path.join(R, 'experiments')
REPORTS = os.path.dirname(R)


def manifests():
    for name in sorted(os.listdir(EXPERIMENTS)):
        with open(os.path.join(EXPERIMENTS, name)) as f:
            yield name, json.load(f)


class ValidationManifestsTest(unittest.TestCase):
    def test_every_manifest_validates_and_its_card_supports_every_policy(self):
        names = [n for n, _ in manifests()]
        self.assertTrue(names)
        for name, m in manifests():
            self.assertEqual(artifacts.validate_experiment(m), [], name)
            card = CARDS[m['card']]
            # The runner records the card's mode; a manifest naming another one would misstate the record.
            self.assertEqual(m['mode'], card.mode, name)
            for policy in m['policies']:
                ok, why = card.supports(policy)
                self.assertTrue(ok, (name, policy, why))

    def test_every_policy_has_a_valid_prediction_with_existing_source_lines(self):
        for name, m in manifests():
            for policy in m['policies']:
                pred = probes.prediction(m['experiment_id'], policy, R)
                self.assertEqual(artifacts.validate_prediction(pred), [], (name, policy))
                self.assertTrue(pred['source_refs'], name)
                for ref in pred['source_refs']:
                    self.assertTrue(ref['path'].startswith('tmp/src-26.8/'), ref)
                    with open(os.path.join(probes.REPO, ref['path'])) as f:
                        self.assertLessEqual(ref['line'], sum(1 for _ in f), ref)

    def test_no_prediction_key_is_shared_with_another_report(self):
        # The closure check keys predictions by (experiment, policy) across all reports.
        mine = set(os.listdir(os.path.join(R, 'predictions')))
        for report in os.listdir(REPORTS):
            other = os.path.join(REPORTS, report, 'predictions')
            if report != os.path.basename(R) and os.path.isdir(other):
                self.assertFalse(mine & set(os.listdir(other)), report)


if __name__ == '__main__':
    unittest.main()
