"""Fixture tests of the CAS (R4) cards, manifests and predictions: every assertion is shown to hold and to fail."""
import copy
import json
import os
import sys
import tempfile
import unittest
from unittest import mock

from yardstick.cost import cli, ledger as L
from yardstick.cost.cards import CARDS, cas, cas_probe, probes
from yardstick.cost.cards.base import CardContext
from yardstick.cost.ledger import Inventory

TOOLS = os.path.join(probes.REPO, 'utils', 'storage-cost-calculator', 'tools')
sys.path.insert(0, TOOLS)
import artifacts  # noqa: E402

R = os.path.join(probes.REPO, 'docs', 'superpowers', 'reports', '2026-10-24-storage-cost-cas')
EXPERIMENTS = os.path.join(R, 'experiments')
CASES = {'cas-branches': {'fresh', 'hash-hit', 'adoption', 'inline', 'staging-peak', 'mix-holdout'},
         'cas-drop': {'table-sync'},
         'gc-lifecycle': {'drop', 'merge'}, 'gc-idle': {'populated'},
         'gc-scale': {'objects-1k', 'objects-10k', 'objects-30k', 'objects-100k'},
         'lease': {'idle-10min', 'idle-20min'}, 'gc-under-load': {'default'}}
PASS_DIR = '/runs/cost-20261024T000000Z/cas/gc-idle/populated/k1'
P = 'cost-20261024T000000Z/cas/gc-idle/populated/k1/shared'
BLOB = f'{P}/blobs/ch128/7e/7e04f7775690055a4f834c628c1cd41d'
MANIFEST = f'{P}/cas/manifests/yardstick-r1/store/fa8/fa8589b7-1d4b-4398-8d0a-5755f6ed78e0@cas@/0000000000000001-0000000000000001/000001.zst'
LIFE = 'e8402a3ca625eaa5aa614fe57409ca84'


def manifest(experiment_id):
    with open(os.path.join(EXPERIMENTS, f'{experiment_id}.json')) as f:
        m = json.load(f)
    m['params']['report_dir'] = R
    return m


class FakeClient:
    def __init__(self, answers=None):
        self.answers, self.log = answers or {}, []

    def query(self, sql, settings=None, timeout=3600):
        self.log.append(sql)
        return next((a for needle, a in self.answers.items() if needle in sql), '0\n')


class FakeCluster:
    def __init__(self, n=1, answers=None):
        self.clients = [FakeClient(answers) for _ in range(n)]


class FakeProxy:
    def __init__(self, path):
        self.ledger_path = path

    def flush(self):
        pass


class Ledger:
    """A ledger file with a window mark at seq 1; rows 1 ms apart."""

    def __init__(self, path, t0=1_000_000):
        self.path, self.seq, self.t = path, 1, t0
        self.rows = [{'kind': 'mark', 'seq': 1, 'label': 'w', 't_us': t0}]

    def add(self, op, key, caller='r1', up='ok', t=None, **kw):
        self.seq += 1
        self.t = t if t is not None else self.t + 1000
        row = {'kind': 'attempt', 'seq': self.seq, 'caller': caller, 'store_id': 's', 't_start_us': self.t,
               't_end_us': self.t + 10, 'bucket': 'b', 'key': key, 'op': op, 'upload_id': None, 'part_number': None,
               'copy_source': None, 'req_payload_bytes': 0, 'resp_payload_bytes': 0, 'payload_sha256': None,
               'req_headers': {}, 'expect_negotiated': False,
               'downstream': {'outcome': 'delivered', 'status': 200},
               'upstream': {'outcome': up, 'http_status': 200, 's3_code': None}, 'response_body': {}}
        row.update(kw)
        self.rows.append(row)
        with open(self.path, 'w') as f:
            f.writelines(json.dumps(r) + '\n' for r in self.rows)
        return row


def ctx_for(m, tmp, ledger, cluster=None, pass_dir=None):
    attempts = lambda: [r for r in L.read_rows(ledger.path) if r.get('kind') == 'attempt']  # noqa: E731
    return CardContext(cluster=cluster or FakeCluster(), proxy=FakeProxy(ledger.path), rig=None,
                       pass_dir=pass_dir or tmp, local_dirs=[], policy='cas', variant='cas', cache_enabled=False,
                       manifest=m, replay=lambda: L.replay(attempts(), Inventory(), cas_probe.CAS_FAMILIES),
                       window=lambda: {'start_seq': 1, 't_start_us': ledger.rows[0]['t_us']})


def failing(out):
    return sorted(name for name, (expected, actual) in out.items() if expected != actual)


def event(event_type, outcome='', reason='', h='aa', action='', publication_reason=''):
    return {'event_type': event_type, 'outcome': outcome, 'reason': reason, 'object_kind': 'blob', 'object_hash': h,
            'ref_name': '', 'action': action, 'publication_reason': publication_reason, 't_us': 1}


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
            stopped = m['settings']['table'].get('max_parts_to_merge_at_once') == 1
            self.assertEqual(stopped, m['controls']['merges'] == 'stop', name)
            # GC cards measure GC on a known interval; the branch and lease cards keep it off.
            gc_card = m['card'] in ('gc-lifecycle', 'gc-idle', 'gc-scale', 'gc-under-load')
            self.assertEqual(m['controls']['gc'], 'measure' if gc_card else 'off', name)
            if m['card'] in ('gc-lifecycle', 'gc-idle'):
                self.assertEqual(m['settings']['server']['storage_configuration.disks.cas.cas_gc_interval_sec'],
                                 str(m['params']['gc_interval_s']), name)

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

    def test_cas_cards_registered_and_the_phase0_sample_fails_closed(self):
        for name, cls in (('cas-branches', cas.CasBranches), ('cas-drop', cas.CasDrop), ('gc-lifecycle', cas.GcLifecycle),
                          ('gc-idle', cas.GcIdle), ('gc-scale', cas.GcScale), ('lease', cas.Lease),
                          ('gc-under-load', cas.GcUnderLoad)):
            self.assertIsInstance(CARDS[name], cls)
        with open(os.path.join(probes.REPORT_DIR, 'experiments', 'gc-scale.sample.json')) as f:
            sample = json.load(f)
        self.assertNotIn('columns_n', sample['params'])  # the CAS card would fail on the missing parameter

    def test_cli_report_dir_reaches_the_card(self):
        found = {m['experiment_id']: m for m in cli._load_manifests([R])}
        self.assertEqual(found['cas-branches.fresh']['params']['report_dir'], R)


class FamiliesTest(unittest.TestCase):
    KEYS = {'blob': BLOB, 'blob_meta': BLOB + '.meta', 'manifest': MANIFEST,
            'ref_stream': f'{P}/cas/ns/stream/{LIFE}/_log/0000000000000001-0000000000000001.zst',
            'ref_ckpt': f'{P}/cas/ns/state/{LIFE}/_ckpt', 'ns_files': f'{P}/cas/ns/state/{LIFE}/_files/mutation_2.txt',
            'ref_catalog': f'{P}/cas/ref_catalog', 'lease': f'{P}/gc/server-roots/yardstick-r1/mount',
            'gc_hb': f'{P}/gc/hb', 'gc': f'{P}/gc/state', 'pool_meta': f'{P}/_pool_meta',
            'roots': f'{P}/roots/yardstick-r1/store'}

    def test_every_layout_key_has_its_family_and_category(self):
        # Key shapes from smoke run cost-20261024T072831Z.
        for name, key in self.KEYS.items():
            self.assertEqual(L.family_of(cas_probe.CAS_FAMILIES, key)['name'], name, key)
        maintenance = {f['name'] for f in cas_probe.CAS_FAMILIES if f.get('maintenance')}
        self.assertEqual(maintenance, {'lease', 'gc_hb', 'gc', 'pool_meta', 'roots'})
        self.assertEqual({f['name'] for f in cas_probe.CAS_FAMILIES if f['payload']}, {'blob', 'manifest'})

    def test_by_family_counts_per_caller_family_and_op(self):
        rows = [{'caller': 'r1', 'op': 'PutObject', 'key': BLOB}, {'caller': 'r1', 'op': 'PutObject', 'key': BLOB},
                {'caller': 'r1', 'op': 'ListObjectsV2', 'key': ''}, {'caller': 'r1', 'op': 'GetObject', 'key': 'x/y'}]
        self.assertEqual(cas.by_family(rows, cas_probe.CAS_FAMILIES),
                         {'r1': {'blob': {'PutObject': 2}, 'batch': {'ListObjectsV2': 1}, 'other': {'GetObject': 1}}})

    def test_pool_prefix_follows_the_runner_key_layout(self):
        class C:
            pass_dir = PASS_DIR
        self.assertEqual(cas.pool_prefix(C), P + '/')


class BranchesTest(unittest.TestCase):
    EVENTS = [event('blob_put', 'published', publication_reason='absent', h='f1'),
              event('blob_reuse_adopt', 'observed', h='h1'),
              event('blob_reuse_adopt', 'adopt', 'manifest-trust', h='a1'),
              event('blob_reuse_adopt', 'adopt', 'manifest-trust', h='a2'),
              event('blob_put', 'published', publication_reason='condemned', h='r1'),
              event('manifest_put')]

    def test_branch_of_reads_the_evidence_fields_both_directions(self):
        self.assertEqual([cas.branch_of(e) for e in self.EVENTS],
                         ['fresh', 'hash_hit', 'adoption', 'adoption', 'resurrect', None])
        # Event kinds alone are not evidence: an adopt without the manifest-trust reason is no branch.
        self.assertIsNone(cas.branch_of(event('blob_reuse_adopt', 'adopt', 'other')))

    def test_population_counts_blob_events_and_inline_placement(self):
        files = {'all_1_1_0': {'data.bin': 'ch128/f1/f1', 'checksums.txt': '', 'columns.txt': ''}}
        self.assertEqual(cas.population(self.EVENTS, files),
                         {'fresh': 1, 'hash_hit': 1, 'adoption': 2, 'inline': 2, 'resurrect': 1})

    def test_part_files_maps_placement_by_table_uuid(self):
        rows = [{'local_path': 'store/fa8/fa8589b7/all_1_1_0/data.bin', 'remote_path': f'{BLOB}', 'size': 5},
                {'local_path': 'store/fa8/fa8589b7/all_1_1_0/count.txt', 'remote_path': '', 'size': 1},
                {'local_path': 'store/aaa/other/all_1_1_0/data.bin', 'remote_path': 'x', 'size': 1}]
        self.assertEqual(cas.part_files(rows, '/fa8589b7/'),
                         {'all_1_1_0': {'data.bin': 'ch128/7e/7e04f7775690055a4f834c628c1cd41d', 'count.txt': ''}})

    def draft(self, card, ctx, population, hashes, placement, fg=None):
        st = card.state(ctx)
        st.update(parts=[{'name': p, 'active': 1} for p in placement], placement=placement, events=[],
                  split={'foreground': [], 'maintenance': [], 'rig': []}, rows=[100000],
                  split_families={'foreground': {}, 'maintenance': {}, 'rig': {}}, gc_rounds={'r1': []})
        obs = card.observe(ctx, {})
        obs.update(branch_population=population, branch_hashes=hashes)
        return {'ledger': {'foreground': fg or {}}, 'observations': obs, 'window': {'crossing': []}}

    def test_hash_hit_asserts_the_same_blob_keys_both_directions(self):
        m = manifest('cas-branches.hash-hit')
        card = CARDS['cas-branches']
        with tempfile.TemporaryDirectory() as tmp:
            ctx = ctx_for(m, tmp, Ledger(os.path.join(tmp, 'l')))
            pred = {'expected': {'foreground': {}, 'observations': {}}, 'tolerance': {'abs': {}, 'rel': {}},
                    'inputs': {'branch_checks': {'hash_hit_same_blob_keys': True}}}
            same = {'all_1_1_0': {'data.bin': 'k1', 'count.txt': ''}, 'all_2_2_0': {'data.bin': 'k1', 'count.txt': ''}}
            other = {'all_1_1_0': {'data.bin': 'k1'}, 'all_2_2_0': {'data.bin': 'k2'}}
            with mock.patch.object(type(card), 'predicted', return_value=pred):
                card.state(ctx)['rows'] = [200000]
                ok = card.assertions(ctx, self.draft(card, ctx, {}, {}, same))
                self.assertNotIn('hash_hit.same_blob_keys', failing(ok))
                bad = card.assertions(ctx, self.draft(card, ctx, {}, {}, other))
                self.assertIn('hash_hit.same_blob_keys', failing(bad))

    def test_adoption_maps_hashes_to_unchanged_columns_both_directions(self):
        m = manifest('cas-branches.adoption')
        card = CARDS['cas-branches']
        src = {'k.bin': 'ch128/k1/k1', 'k.mrk2': 'ch128/k2/k2', 'u1.bin': 'ch128/u1/u1', 'u2.bin': 'ch128/u2/u2',
               'u2.mrk2': 'ch128/u3/u3', 'checksums.txt': ''}
        placement = {'all_1_1_0': src, 'all_1_1_0_2': dict(src, **{'u1.bin': 'ch128/n1/n1'})}
        pred = {'expected': {'foreground': {}, 'observations': {}}, 'tolerance': {'abs': {}, 'rel': {}},
                'inputs': {'branch_checks': {'adopted_columns': {'source_part': 'all_1_1_0', 'columns': ['k', 'u2']}}}}
        with tempfile.TemporaryDirectory() as tmp:
            ctx = ctx_for(m, tmp, Ledger(os.path.join(tmp, 'l')))
            with mock.patch.object(type(card), 'predicted', return_value=pred):
                card.state(ctx)['rows'] = [300000]
                good = self.draft(card, ctx, {}, {'adoption': ['k1', 'k2', 'u2', 'u3']}, placement)
                self.assertEqual([n for n in failing(card.assertions(ctx, good)) if n.startswith('adoption')], [])
                # The mutated column adopted too: a branch the mutation must not take.
                bad = self.draft(card, ctx, {}, {'adoption': ['k1', 'u1', 'u2']}, placement)
                self.assertEqual([n for n in failing(card.assertions(ctx, bad)) if n.startswith('adoption')],
                                 ['adoption.columns'])
                unknown = self.draft(card, ctx, {}, {'adoption': ['k1', 'u2', 'zz']}, placement)
                self.assertIn('adoption.files_known', failing(card.assertions(ctx, unknown)))

    def test_foreground_exact_per_op_and_unpredicted_fail(self):
        m = manifest('cas-branches.fresh')
        card = CARDS['cas-branches']
        pred = {'expected': {'foreground': {'r1': {'PutObject': 3}}, 'observations': {'branch_population':
                {'fresh': 1, 'hash_hit': 0, 'adoption': 0, 'inline': 2, 'resurrect': 0}}},
                'tolerance': {'abs': {}, 'rel': {}}, 'inputs': {}}
        c = lambda n: dict(dict.fromkeys(L.COUNT_FIELDS, 0), attempts=n)  # noqa: E731
        with tempfile.TemporaryDirectory() as tmp:
            ctx = ctx_for(m, tmp, Ledger(os.path.join(tmp, 'l')))
            with mock.patch.object(type(card), 'predicted', return_value=pred):
                pop = {'fresh': 1, 'hash_hit': 0, 'adoption': 0, 'inline': 2, 'resurrect': 0}
                ok = self.draft(card, ctx, pop, {}, {'all_1_1_0': {}}, fg={'r1': {'PutObject': c(3)}})
                self.assertEqual(failing(card.assertions(ctx, ok)), [])
                bad = self.draft(card, ctx, dict(pop, fresh=2), {}, {'all_1_1_0': {}},
                                 fg={'r1': {'PutObject': c(4), 'HeadObject': c(1)}})
                self.assertEqual(failing(card.assertions(ctx, bad)),
                                 ['ledger.r1.PutObject', 'ledger.unpredicted', 'observation.branch_population'])
                crossing = self.draft(card, ctx, pop, {}, {'all_1_1_0': {}}, fg={'r1': {'PutObject': c(3)}})
                card.state(ctx)['split']['foreground'] = [7]
                crossing['window']['crossing'] = [7, 9]
                self.assertEqual(failing(card.assertions(ctx, crossing)), ['window.foreground_crossing'])

    def test_observer_requests_are_excluded_from_workload(self):
        m = manifest('cas-branches.fresh')
        card = CARDS['cas-branches']
        with tempfile.TemporaryDirectory() as tmp:
            led = Ledger(os.path.join(tmp, 'l'))
            led.add('PutObject', BLOB)
            ctx = ctx_for(m, tmp, led)
            card.state(ctx)['gc_rounds'] = {'r1': []}
            card.as_observer(ctx, lambda: led.add('GetObject', f'{P}/cas/ref_catalog'))
            split = card.classify(ctx, led.rows[1:], cas_probe.CAS_FAMILIES)
            self.assertEqual([r['op'] for r in split['foreground']], ['PutObject'])
            self.assertEqual([r['op'] for r in card.state(ctx)['observer_rows']], ['GetObject'])


class GcLifecycleTest(unittest.TestCase):
    def test_cohort_is_frozen_before_the_retire_and_excludes_control_families(self):
        m = manifest('gc-lifecycle.drop')
        card = CARDS['gc-lifecycle']
        with tempfile.TemporaryDirectory() as tmp:
            led = Ledger(os.path.join(tmp, 'l'))
            for key in FamiliesTest.KEYS.values():
                led.add('PutObject', key)
            ctx = ctx_for(m, tmp, led, FakeCluster(1))
            with mock.patch.object(type(card), 'insert'):
                card.measured(ctx)
            self.assertEqual(card.state(ctx)['cohort'], {('b', BLOB), ('b', MANIFEST)})
            self.assertTrue(any('DROP TABLE yardstick.t SYNC' in q for q in ctx.cluster.clients[0].log))

    def test_stages_and_delays_from_the_gc_log_and_the_ledger(self):
        m = manifest('gc-lifecycle.drop')
        card = CARDS['gc-lifecycle']
        with tempfile.TemporaryDirectory() as tmp:
            led = Ledger(os.path.join(tmp, 'l'))
            ctx = ctx_for(m, tmp, led)
            st = card.state(ctx)
            st.update(cohort={('b', BLOB), ('b', MANIFEST)}, t_retire_us=10_000_000,
                      deleted={('b', MANIFEST): {'t_us': 21_000_000, 'caller': 'r1'},
                               ('b', BLOB): {'t_us': 32_000_000, 'caller': 'r1'}},
                      finishes=[{'t_us': 9_000_000, 'entries_condemned': 5},
                                {'t_us': 21_000_000, 'entries_condemned': 1},
                                {'t_us': 32_000_000, 'entries_condemned': 0}],
                      starts=[{'t_us': 9_000_000}, {'t_us': 20_000_000}, {'t_us': 31_000_000}],
                      split_families={'foreground': {}, 'maintenance': {}}, gc_rounds={'r1': []}, rows=[0])
            obs = card.observe(ctx, {})
            self.assertEqual(obs['reclaim_delay_s'], {'manifest_delete': 11.0, 'blob_delete': 22.0, 'condemn': 11.0,
                                                      'total': 22.0})
            self.assertEqual(obs['reclaim_rounds'], {'manifest_delete': 1, 'blob_delete': 2, 'condemn': 1, 'total': 2})
            draft = {'ledger': {'foreground': {}}, 'observations': obs, 'window': {'crossing': []},
                     'inventory': {'byte_hours': {'garbage': {'lower': 1.0, 'upper': 2.0}}}}
            st['split'] = {'foreground': []}
            pred = {'expected': {'foreground': {}, 'observations': {}}, 'tolerance': {'abs': {}, 'rel': {}},
                    'inputs': {'exact': False}}
            with mock.patch.object(type(card), 'predicted', return_value=pred):
                self.assertEqual(failing(card.assertions(ctx, draft)), [])
                st['deleted'].pop(('b', BLOB))
                draft['observations'] = card.observe(ctx, {})
                self.assertEqual(failing(card.assertions(ctx, draft)), ['cohort_reclaimed'])


class GcIdleTest(unittest.TestCase):
    def run_idle(self, tmp, round_rows=True, hb_in_round=False):
        m = manifest('gc-idle.populated')
        card = CARDS['gc-idle']
        led = Ledger(os.path.join(tmp, 'l'))
        led.add('GetObject', f'{P}/gc/hb', t=2_000_000)
        led.add('PutObject', f'{P}/gc/hb', t=2_000_100)
        if round_rows:
            led.add('GetObject', f'{P}/gc/state', t=5_000_000)
            led.add('ListObjectsV2', '', t=5_000_100)
            led.add('GetObject', f'{P}/cas/ref_catalog', t=5_000_200)
        if hb_in_round:
            led.add('PutObject', f'{P}/gc/hb', t=5_000_300)
        led.add('PutObject', f'{P}/gc/server-roots/yardstick-r1/mount', t=6_000_000)
        ctx = ctx_for(m, tmp, led, pass_dir=PASS_DIR)
        st = card.state(ctx)
        st.update(pool_objects={'blob': 5000, 'blob_meta': 5000, 'manifest': 49}, gc_rounds={'r1': [[4_900_000, 5_500_000]]},
                  finishes=[{'round_id': 'a', 't_us': 5_500_000, 'outcome': 'Deferred', 'duration_ms': 600}],
                  starts=[{'round_id': 'a', 't_us': 4_900_000}], rows=[49000])
        split = card.classify(ctx, led.rows[1:], cas_probe.CAS_FAMILIES)
        draft = {'ledger': {'foreground': L.summarize(split['foreground'])},
                 'window': {'crossing': [], 'start_seq': 1, 'end_seq': led.seq + 1, 't_start_us': 1_000_000,
                            't_end_us': 11_000_000}}
        draft['observations'] = card.observe(ctx, draft)
        return card, ctx, draft

    def test_heartbeat_and_round_rows_are_nonzero_and_disjoint_by_family(self):
        with tempfile.TemporaryDirectory() as tmp:
            card, ctx, draft = self.run_idle(tmp, hb_in_round=True)
            obs = draft['observations']
            self.assertEqual((obs['heartbeat_requests'], obs['round_requests']), (3, 3))
            self.assertEqual(obs['round_families'], ['batch', 'gc', 'ref_catalog'])
            self.assertEqual(obs['objects'], 10049)
            self.assertAlmostEqual(obs['heartbeat_requests_per_s'], 0.3)
            pred = {'expected': {'foreground': {}, 'observations': {}}, 'tolerance': {'abs': {}, 'rel': {}},
                    'inputs': {}}
            with mock.patch.object(type(card), 'predicted', return_value=pred):
                self.assertEqual(failing(card.assertions(ctx, draft)), [])
        with tempfile.TemporaryDirectory() as tmp:
            card, ctx, draft = self.run_idle(tmp, round_rows=False)
            with mock.patch.object(type(card), 'predicted', return_value=pred):
                self.assertEqual(failing(card.assertions(ctx, draft)), ['round_requests>0'])

    def test_lease_and_heartbeat_are_maintenance_outside_rounds(self):
        with tempfile.TemporaryDirectory() as tmp:
            card, ctx, draft = self.run_idle(tmp)
            self.assertEqual(draft['ledger']['foreground'], {})
            self.assertEqual(draft['observations']['maintenance_by_family'],
                             {'gc_hb': {'GetObject': 1, 'PutObject': 1}, 'gc': {'GetObject': 1},
                              'batch': {'ListObjectsV2': 1}, 'ref_catalog': {'GetObject': 1},
                              'lease': {'PutObject': 1}})


class GcScaleTest(unittest.TestCase):
    def test_pool_sizes_are_built_deterministically(self):
        card = CARDS['gc-scale']
        sizes = {}
        for case in ('objects-1k', 'objects-10k', 'objects-30k', 'objects-100k'):
            m = manifest(f'gc-scale.{case}')
            with tempfile.TemporaryDirectory() as tmp:
                ctx = ctx_for(m, tmp, Ledger(os.path.join(tmp, 'l')))
                sql = [card.insert_sql(ctx, i) for i in range(card.parts_n(ctx))]
                self.assertEqual(len(set(sql)), len(sql), case)  # every part has its own values
                sizes[case] = cas.pool_objects(card.parts_n(ctx), m['params']['columns_n'])
        for case, target in (('objects-1k', 1000), ('objects-10k', 10000), ('objects-30k', 30000),
                             ('objects-100k', 100000)):
            self.assertLess(abs(sizes[case] - target) / target, 0.06, (case, sizes[case]))

    def test_round_rows_are_split_per_round_window(self):
        m = manifest('gc-scale.objects-1k')
        card = CARDS['gc-scale']
        with tempfile.TemporaryDirectory() as tmp:
            led = Ledger(os.path.join(tmp, 'l'))
            led.add('GetObject', f'{P}/gc/state', t=2_000_000)
            led.add('ListObjectsV2', '', t=2_000_100)
            led.add('GetObject', f'{P}/gc/state', t=4_000_000)
            ctx = ctx_for(m, tmp, led)
            card.state(ctx)['gc_rounds'] = {'r1': [[1_900_000, 2_100_000], [3_900_000, 4_100_000]]}
            rounds = card.round_rows(ctx, {})
            self.assertEqual([r['requests'] for r in rounds], [2, 1])


class LeaseTest(unittest.TestCase):
    def test_renewal_rate_from_maintenance_lease_rows(self):
        m = manifest('lease.idle-10min')
        card = CARDS['lease']
        with tempfile.TemporaryDirectory() as tmp:
            led = Ledger(os.path.join(tmp, 'l'))
            for i in range(3):
                led.add('PutObject', f'{P}/gc/server-roots/yardstick-r1/mount', t=1_000_000 + i * 10_000_000)
            ctx = ctx_for(m, tmp, led)
            card.complete(ctx)
            split = card.classify(ctx, led.rows[1:], cas_probe.CAS_FAMILIES)
            self.assertEqual(split['foreground'], [])
            draft = {'window': {'t_start_us': 0, 't_end_us': 30_000_000, 'crossing': []},
                     'ledger': {'foreground': {}}}
            obs = card.observe(ctx, draft)
            self.assertEqual((obs['lease_renewals'], obs['lease_requests_per_s']), (3, 0.1))


if __name__ == '__main__':
    unittest.main()
