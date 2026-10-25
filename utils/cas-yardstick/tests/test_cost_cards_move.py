"""Fixture tests of the TTL move cards: manifests, predictions, the tiered policy XML and the CAS graph bounds."""
import json
import os
import shutil
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET

from yardstick.config import write_cost_config
from yardstick.cost.cards import CARDS, probes
from yardstick.cost.cards.move import by_kind, cas_graph_bounds, tiered_policy
from yardstick.cost.topology import Topology

sys.path.insert(0, os.path.join(probes.REPO, 'utils', 'storage-cost-calculator', 'tools'))
import artifacts  # noqa: E402

R = os.path.join(probes.REPO, 'docs', 'superpowers', 'reports', '2026-10-25-storage-cost-ttl-move')


def load(rel):
    with open(os.path.join(R, rel)) as f:
        return json.load(f)


class ManifestsTest(unittest.TestCase):
    def test_manifests_and_predictions_validate(self):
        names = sorted(os.listdir(os.path.join(R, 'experiments')))
        self.assertEqual(names, ['move-replicated.ttl-cas.json', 'move-replicated.ttl.json', 'move.ttl-holdout.json', 'move.ttl.json'])
        for name in names:
            m = load(f'experiments/{name}')
            self.assertEqual(artifacts.validate_experiment(m), [], name)
            card = CARDS[m['card']]
            self.assertEqual((m['mode'], m['replicas']), (card.mode, card.replicas), name)
            self.assertTrue(set(m['policies']) <= set(card.policies), name)
            for policy in m['policies']:
                pred = probes.prediction(m['experiment_id'], policy, R)
                self.assertEqual(artifacts.validate_prediction(pred), [], (name, policy))
                for ref in pred['source_refs']:
                    with open(os.path.join(probes.REPO, ref['path'])) as f:
                        self.assertLessEqual(ref['line'], sum(1 for _ in f), ref)

    def test_s3_prediction_is_the_insert_write_plus_one_load(self):
        pred = load('predictions/move.ttl.s3.json')
        i = pred['inputs']
        self.assertEqual(pred['expected']['foreground']['r1'],
                         {'PutObject': i['compact_parts'] * i['objects_per_compact_part']
                          + i['wide_parts'] * i['objects_per_wide_part'],
                          'GetObject': (i['compact_parts'] + i['wide_parts']) * i['load_gets_per_part']})


class TieredPolicyTest(unittest.TestCase):
    def test_policy_lists_hot_before_cold(self):
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp)
        d = os.path.join(tmp, 'r1')
        write_cost_config(d, 1, 's3', Topology(replicas=1), 'p/run', None, False, 'root', tiered_policy('s3'))
        root = ET.parse(os.path.join(d, 'etc', 'config.d', 'cost.xml')).getroot()
        policy = root.find('storage_configuration/policies/tiered')
        self.assertEqual([(v.tag, v.find('disk').text) for v in policy.find('volumes')],
                         [('hot', 'default'), ('cold', 's3')])
        self.assertEqual(policy.find('move_factor').text, '0')
        self.assertIsNotNone(root.find('storage_configuration/policies/s3'))


class ArithmeticTest(unittest.TestCase):
    def test_cas_graph_bounds(self):
        graph = load('predictions/move.ttl.cas.json')['inputs']['cas_graph']
        # 5 parts (2 Wide), 16 fresh blobs, 4 hash hits.
        self.assertEqual(cas_graph_bounds(graph, 5, 2, 16, 4),
                         {'HeadObject': [20, 20], 'PutObject': [67, 67], 'GetObject': [46, 47]})

    def test_cas_graph_bounds_with_the_table_birth_term(self):
        graph = load('predictions/move.ttl-holdout.cas.json')['inputs']['cas_graph']
        self.assertNotIn('cold_table_once', graph)
        self.assertEqual(cas_graph_bounds(graph, 5, 1, 10, 1),
                         {'HeadObject': [11, 11], 'PutObject': [58, 58], 'GetObject': [55, 55]})

    def test_by_kind(self):
        reqs = {'t/a': {'PutObject': 12}, 't/b': {'PutObject': 12}, 't/c': {'PutObject': 22}, 'unlisted': {'PutObject': 3}}
        kinds = {'t/a': 'compact', 't/b': 'compact', 't/c': 'wide'}
        self.assertEqual(by_kind(reqs, kinds, 'PutObject'), {'compact': 12, 'wide': 22})
        self.assertEqual(by_kind({'t/a': {'PutObject': 1}, 't/b': {}}, {'t/a': 'compact', 't/b': 'compact'}, 'PutObject'),
                         {'compact': [0, 1]})


if __name__ == '__main__':
    unittest.main()
