"""Fixture tests of the replication and fault cards: manifests, acquisition paths, fault rules, restart order."""
import json
import os
import sys
import unittest

from yardstick.cost.cards import CARDS, faults, probes, replication
from yardstick.cost.cards.base import CardContext

TOOLS = os.path.join(probes.REPO, 'utils', 'storage-cost-calculator', 'tools')
sys.path.insert(0, TOOLS)
import artifacts  # noqa: E402

R = os.path.join(probes.REPO, 'docs', 'superpowers', 'reports', '2026-10-24-storage-cost-replication')
EXPERIMENTS = os.path.join(R, 'experiments')
OWN_CARDS = {'replication-fetch', 'replication-local', 'replication-zero-copy', 'replication-natural-fit',
             'replication-natural-holdout', 'restart-catchup', 'faults'}


def manifest(experiment_id):
    with open(os.path.join(EXPERIMENTS, f'{experiment_id}.json')) as f:
        m = json.load(f)
    m['params']['report_dir'] = R
    return m


class FakeClient:
    def __init__(self, answers=None, fail_on=None):
        self.answers, self.log, self.fail_on = answers or {}, [], fail_on

    def query(self, sql, settings=None, timeout=3600):
        self.log.append(sql)
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError(sql, 'Code: 499. injected')
        return next((a for needle, a in self.answers.items() if needle in sql), '0\n')


class FakeCluster:
    def __init__(self, n=2, answers=None, fail_on=None):
        self.clients = [FakeClient(answers, fail_on) for _ in range(n)]
        self.events = []
        self.generations = {i: [{}] for i in range(1, n + 1)}

    def stop(self, i):
        self.events.append(('stop', i))

    def start(self, i):
        self.events.append(('start', i))
        self.generations[i].append({})
        return 1.5


class FakeProxy:
    def __init__(self):
        self.rules, self.cleared = [], 0

    def set_fault(self, rule):
        self.rules.append(rule)

    def clear_faults(self):
        self.cleared += 1
        self.rules = []


_PASSES = iter(range(1 << 30))


def ctx_for(m, cluster, policy='s3', proxy=None):
    return CardContext(cluster=cluster, proxy=proxy, rig=None, pass_dir=f'/nonexistent/{next(_PASSES)}', local_dirs=[],
                       policy=policy, variant='s3', cache_enabled=False, manifest=m, replay=None,
                       window=lambda: {'start_seq': 0, 't_start_us': 0})


def row(event, name, t, merged_from=(), duration_ms=0):
    return {'event_type': event, 'part_name': name, 'merged_from': list(merged_from), 'error': 0, 't_us': t,
            'duration_ms': duration_ms}


SOURCES = ['all_0_0_0', 'all_1_1_0']
MERGED = 'all_0_1_1'


def logs_merge(follower_event):
    """r1 inserts two parts and merges them; r2 fetched the inserted parts and then `follower_event`s the result."""
    r1 = [row('NewPart', SOURCES[0], 1), row('NewPart', SOURCES[1], 2), row('MergeParts', MERGED, 10, SOURCES)]
    r2 = [row('DownloadPart', SOURCES[0], 3), row('DownloadPart', SOURCES[1], 4), row(follower_event, MERGED, 12, SOURCES)]
    return {'r1': r1, 'r2': r2}


def counters(**r2):
    base = dict.fromkeys(replication.COUNTERS, 0)
    return {'r1': dict(base, ReplicatedPartMerges=1), 'r2': {**base, **r2}}


RELAY_SERVED = {'relay0': {'from_target': 5000, 'to_target': 100}, 'relay1': {'from_target': 0, 'to_target': 0}}
RELAY_IDLE = {'relay0': {'from_target': 0, 'to_target': 0}, 'relay1': {'from_target': 0, 'to_target': 0}}


def failed(checks):
    return sorted(name for name, (want, got) in checks.items() if want != got)


class ManifestsTest(unittest.TestCase):
    def test_every_manifest_validates_and_names_an_own_card(self):
        names = sorted(os.listdir(EXPERIMENTS))
        self.assertTrue(names)
        for name in names:
            with open(os.path.join(EXPERIMENTS, name)) as f:
                m = json.load(f)
            self.assertEqual(artifacts.validate_experiment(m), [], name)
            self.assertIn(m['card'], OWN_CARDS, name)
            self.assertIn(m['card'], CARDS, name)
            self.assertEqual(m['replicas'], CARDS[m['card']].replicas, name)
            stopped = m['settings']['table'].get('max_parts_to_merge_at_once') == 1
            self.assertEqual(stopped, m['controls']['merges'] == 'stop', name)
            for p in m['policies']:
                self.assertTrue(CARDS[m['card']].supports(p)[0], f'{name}: {p}')

    def test_every_manifest_has_a_valid_prediction_per_policy(self):
        for name in sorted(os.listdir(EXPERIMENTS)):
            m = manifest(name[:-5])
            for policy in m['policies']:
                pred = probes.prediction(m['experiment_id'], policy, R)
                self.assertEqual(artifacts.validate_prediction(pred), [], f'{name} {policy}')
                self.assertTrue(pred['source_refs'], f'{name} {policy}')
                for r in pred['source_refs']:
                    self.assertTrue(r['path'].startswith('tmp/src-26.8/'), r)

    def test_forced_cases_force_the_follower_through_its_table_settings(self):
        for eid, setting in (('replication-fetch.merge', 'always_fetch_merged_part'),
                             ('replication-fetch.mutation', 'always_fetch_mutated_part')):
            p = manifest(eid)['params']
            self.assertEqual(p['replica_settings']['r2'], {setting: 1}, eid)
            self.assertEqual(p['replica_settings']['r1'], {}, eid)
        for eid in ('replication-local.merge', 'replication-local.mutation'):
            self.assertEqual(manifest(eid)['params']['replica_settings'], {'r1': {}, 'r2': {}}, eid)

    def test_zero_copy_cases_enable_zero_copy_on_every_replica_and_run_on_s3_only(self):
        for case in ('insert', 'merge', 'mutation'):
            m = manifest(f'replication-zero-copy.{case}')
            self.assertEqual(m['settings']['table']['allow_remote_fs_zero_copy_replication'], 1)
            self.assertEqual(m['policies'], ['s3'])


class DdlTest(unittest.TestCase):
    def test_setup_creates_a_replicated_table_on_both_replicas_with_per_replica_settings(self):
        m = manifest('replication-fetch.merge')
        cluster = FakeCluster()
        ctx = ctx_for(m, cluster)
        CARDS['replication-fetch'].setup(ctx)
        r1 = [s for s in cluster.clients[0].log if s.startswith('CREATE TABLE')]
        r2 = [s for s in cluster.clients[1].log if s.startswith('CREATE TABLE')]
        self.assertEqual(len(r1), 1)
        self.assertEqual(len(r2), 1)
        self.assertIn("ReplicatedMergeTree('/yardstick/t', '{replica}')", r1[0])
        self.assertNotIn('always_fetch_merged_part', r1[0])
        self.assertIn('always_fetch_merged_part = 1', r2[0])
        # The four source parts are inserted on r1 only, before the window.
        inserts = [[s for s in c.log if s.startswith('INSERT')] for c in cluster.clients]
        self.assertEqual((len(inserts[0]), len(inserts[1])), (4, 0))

    def test_statements_window_runs_on_the_originator_only(self):
        m = manifest('replication-local.mutation')
        cluster = FakeCluster()
        CARDS['replication-local'].measured(ctx_for(m, cluster))
        self.assertTrue(any(s.startswith('ALTER TABLE yardstick.t UPDATE') for s in cluster.clients[0].log))
        self.assertFalse(cluster.clients[1].log)


class AcquisitionTest(unittest.TestCase):
    def test_fetched_parts_take_the_type_of_the_event_that_produced_them(self):
        acq = replication.acquisitions(logs_merge('DownloadPart'))
        self.assertEqual(acq['r2']['insert']['fetch'], SOURCES)
        self.assertEqual(acq['r2']['merge']['fetch'], [MERGED])
        self.assertEqual(acq['r2']['merge']['local'], [])
        self.assertEqual(acq['r1']['merge']['local'], [MERGED])

    def test_follower_split_counts_only_non_originator_acquisitions(self):
        split = replication.follower_split(logs_merge('MergeParts'))
        self.assertEqual(split['insert'], {'fetch': 2, 'local': 0, 'fetch_fraction': 1.0})
        self.assertEqual(split['merge'], {'fetch': 0, 'local': 1, 'fetch_fraction': 0.0})
        self.assertIsNone(split['mutation']['fetch_fraction'])

    def test_forced_fetch_passes_on_a_fetched_merge(self):
        acq = replication.acquisitions(logs_merge('DownloadPart'))
        checks = replication.path_assertions(acq, counters(ReplicatedPartFetchesOfMerged=1, ReplicatedPartFetches=3),
                                             RELAY_SERVED, 'r1', 'r2', {'merge': 'fetch'})
        self.assertEqual(failed(checks), [])

    def test_forced_fetch_fails_when_the_follower_merged_locally(self):
        acq = replication.acquisitions(logs_merge('MergeParts'))
        checks = replication.path_assertions(acq, counters(ReplicatedPartMerges=1, ReplicatedPartFetches=2),
                                             RELAY_IDLE, 'r1', 'r2', {'merge': 'fetch'})
        self.assertEqual(failed(checks), ['counter.r2.ReplicatedPartFetchesOfMerged>=1', 'counter.r2.ReplicatedPartMerges',
                                          'path.r2.merge.fetch', 'path.r2.merge.local', 'relay0.from_target>0'])

    def test_local_execution_passes_on_a_local_merge(self):
        acq = replication.acquisitions(logs_merge('MergeParts'))
        checks = replication.path_assertions(acq, counters(ReplicatedPartMerges=1, ReplicatedPartFetches=2),
                                             RELAY_IDLE, 'r1', 'r2', {'merge': 'local'})
        self.assertEqual(failed(checks), [])

    def test_local_execution_fails_when_the_follower_fetched(self):
        acq = replication.acquisitions(logs_merge('DownloadPart'))
        checks = replication.path_assertions(acq, counters(ReplicatedPartFetchesOfMerged=1, ReplicatedPartFetches=3),
                                             RELAY_SERVED, 'r1', 'r2', {'merge': 'local'})
        self.assertEqual(failed(checks), ['counter.r2.ReplicatedPartFetchesOfMerged', 'counter.r2.ReplicatedPartMerges>=1',
                                          'path.r2.merge.fetch', 'path.r2.merge.local', 'relay0.from_target'])

    def test_mutation_paths_in_both_directions(self):
        mutated = 'all_0_0_0_1'
        r1 = [row('MutatePart', mutated, 10, ['all_0_0_0'])]
        local = {'r1': r1, 'r2': [row('MutatePart', mutated, 11, ['all_0_0_0'])]}
        fetched = {'r1': r1, 'r2': [row('DownloadPart', mutated, 11)]}
        ok_local = replication.path_assertions(replication.acquisitions(local), counters(ReplicatedPartMutations=1),
                                               RELAY_IDLE, 'r1', 'r2', {'mutation': 'local'})
        ok_fetch = replication.path_assertions(replication.acquisitions(fetched), counters(ReplicatedPartFetches=1),
                                               RELAY_SERVED, 'r1', 'r2', {'mutation': 'fetch'})
        self.assertEqual((failed(ok_local), failed(ok_fetch)), ([], []))
        wrong = replication.path_assertions(replication.acquisitions(fetched), counters(ReplicatedPartFetches=1),
                                            RELAY_SERVED, 'r1', 'r2', {'mutation': 'local'})
        self.assertIn('path.r2.mutation.local', failed(wrong))
        self.assertIn('counter.r2.ReplicatedPartFetches', failed(wrong))

    def test_inserted_parts_must_all_be_fetched(self):
        logs = {'r1': [row('NewPart', 'all_0_0_0', 1), row('NewPart', 'all_1_1_0', 2)],
                'r2': [row('DownloadPart', 'all_0_0_0', 3)]}
        checks = replication.path_assertions(replication.acquisitions(logs), counters(ReplicatedPartFetches=1),
                                             RELAY_SERVED, 'r1', 'r2', {'insert': 'fetch'})
        self.assertEqual(failed(checks), ['counter.r2.ReplicatedPartFetches', 'path.r2.insert.fetch'])


class RequestRolesTest(unittest.TestCase):
    def test_originator_reads_of_its_own_new_objects_are_the_sender_side(self):
        def a(caller, op, key):
            return {'caller': caller, 'op': op, 'bucket': 'b', 'key': key, 'upstream': {'outcome': 'ok'}}
        rows = [a('r1', 'GetObject', 'src'),       # merge reads a source part: r1's own execution
                a('r1', 'PutObject', 'res'),       # merge writes the result
                a('r1', 'GetObject', 'res'),       # serving the fetch reads the result: sender
                a('r2', 'PutObject', 'copy'), a('r2', 'GetObject', 'copy')]
        self.assertEqual(replication.request_roles(rows, 'r1'),
                         {'r1': {'own': {'GetObject': 1, 'PutObject': 1}, 'sender': {'GetObject': 1}},
                          'r2': {'acquire': {'GetObject': 1, 'PutObject': 1}}})


class RestartCatchupTest(unittest.TestCase):
    def test_stops_and_starts_the_follower_around_the_originator_inserts(self):
        m = manifest('restart-catchup.10min')
        m['params'].update(duration_s=0, quiet_s=0, quiesce_timeout_s=5)
        m['scale'] = 0.002
        cluster = FakeCluster()
        CARDS['restart-catchup'].measured(ctx_for(m, cluster))
        self.assertEqual(cluster.events, [('stop', 2), ('start', 2)])
        self.assertTrue(any(s.startswith('INSERT') for s in cluster.clients[0].log))
        self.assertFalse(any(s.startswith('INSERT') for s in cluster.clients[1].log))

    def test_catchup_split_puts_follower_requests_before_its_first_fetch_in_startup(self):
        rows = [{'caller': 'r1', 'op': 'PutObject', 't_start_us': 5},      # before the restart: not counted
                {'caller': 'r2', 'op': 'GetObject', 't_start_us': 11},     # startup
                {'caller': 'r2', 'op': 'PutObject', 't_start_us': 21},     # catch-up (receiver)
                {'caller': 'r1', 'op': 'GetObject', 't_start_us': 22}]     # catch-up (sender)
        split = replication.catchup_split(rows, 'r2', 'r1', t_start_us=10, first_fetch_us=20)
        self.assertEqual(split, {'startup': {'GetObject': 1}, 'catchup': {'GetObject': 1, 'PutObject': 1}})


class FaultsTest(unittest.TestCase):
    def test_rules_are_installed_for_the_insert_and_cleared_after_it(self):
        m = manifest('faults.put-5xx')
        proxy, cluster = FakeProxy(), FakeCluster(n=1)
        seen = []
        original = cluster.clients[0].query

        def query(sql, settings=None, timeout=3600):
            if sql.startswith('INSERT'):
                seen.append(list(proxy.rules))
            return original(sql, settings, timeout)
        cluster.clients[0].query = query
        CARDS['faults'].measured(ctx_for(m, cluster, proxy=proxy))
        self.assertEqual(seen, [m['params']['faults']])
        self.assertEqual((proxy.rules, proxy.cleared), ([], 1))

    def test_rules_are_cleared_when_the_insert_fails(self):
        m = manifest('faults.multipart-abort')
        proxy = FakeProxy()
        ctx = ctx_for(m, FakeCluster(n=1, fail_on='INSERT'), proxy=proxy)
        with self.assertRaises(RuntimeError):
            CARDS['faults'].measured(ctx)
        self.assertEqual((proxy.rules, proxy.cleared), ([], 1))

    def test_retry_view_chains_a_failed_attempt_with_its_retry(self):
        def attempt(seq, outcome, up):
            return {'seq': seq, 'caller': 'r1', 'op': 'PutObject', 'store_id': 's', 'bucket': 'b', 'key': 'k',
                    'upload_id': None, 'part_number': None, 'payload_sha256': 'x', 'req_headers': {}, 'query': {},
                    'downstream': {'outcome': outcome}, 'upstream': {'outcome': up}}
        view = faults.retry_view([attempt(1, 'injected', 'not_sent'), attempt(2, 'delivered', 'ok')])
        self.assertEqual(view['failed_attempts'], 1)
        self.assertEqual(view['extra_attempts'], 1)
        self.assertEqual(view['failed_not_retried'], [])
        self.assertEqual(view['chains'][0]['downstream'], ['injected', 'delivered'])
        lone = faults.retry_view([attempt(1, 'dropped', 'ok')])
        self.assertEqual(lone['failed_not_retried'], [1])


if __name__ == '__main__':
    unittest.main()
