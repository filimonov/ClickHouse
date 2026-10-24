"""Fixture tests of the phase-0 probe cards: every assertion is shown to hold and to fail."""
import copy
import json
import os
import sys
import tempfile
import unittest
from unittest import mock

from yardstick.cost import cli, ledger as L
from yardstick.cost.cards import CARDS, cas_probe, probes
from yardstick.cost.cards.base import CardContext
from yardstick.cost.ledger import Inventory

TOOLS = os.path.join(probes.REPO, 'utils', 'storage-cost-calculator', 'tools')
sys.path.insert(0, TOOLS)
import artifacts  # noqa: E402

R = probes.REPORT_DIR
with open(os.path.join(probes.REPO, 'docs', 'superpowers', 'cost-model', 'inputs-registry.json')) as _f:
    REGISTRY_IDS = {q['id'] for q in json.load(_f)['quantities']}
PREFIX = 'run/s3/probe/gate/k1'


def manifest(name):
    with open(os.path.join(R, 'experiments', f'{name}.gate.json')) as f:
        return json.load(f)


def counts(attempts, **kw):
    c = dict.fromkeys(L.COUNT_FIELDS, 0)
    c.update(attempts=attempts, **kw)
    return c


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
        self.ledger_path, self.ports, self.faults = path, {'rig': 1}, []

    def flush(self):
        pass

    def set_fault(self, rule):
        self.faults.append(rule)


class Ledger:
    """A ledger file with a window mark at seq 1; rows get increasing seqs and 1 ms apart timestamps."""

    def __init__(self, path, t0=1_000_000):
        self.path, self.seq, self.t = path, 1, t0
        self.rows = [{'kind': 'mark', 'seq': 1, 'label': 'w', 't_us': t0}]

    def add(self, op, key, caller='r1', up='ok', down='delivered', t=None, **kw):
        self.seq += 1
        self.t = t if t is not None else self.t + 1000
        row = {'kind': 'attempt', 'seq': self.seq, 'caller': caller, 'store_id': 's', 't_start_us': self.t,
               't_end_us': self.t + 10, 'bucket': 'b', 'key': key, 'op': op, 'upload_id': None, 'part_number': None,
               'copy_source': None, 'req_payload_bytes': 0, 'resp_payload_bytes': 0, 'payload_sha256': None,
               'req_headers': {}, 'expect_negotiated': False,
               'downstream': {'outcome': down, 'status': 200}, 'upstream': {'outcome': up, 'http_status': 200,
                                                                            's3_code': None},
               'response_body': {}}
        row.update(kw)
        self.rows.append(row)
        with open(self.path, 'w') as f:
            f.writelines(json.dumps(r) + '\n' for r in self.rows)
        return row


def ctx_for(card, tmp, ledger, policy, cluster=None, families=(), local_dirs=(), rig=None):
    attempts = lambda: [r for r in L.read_rows(ledger.path) if r.get('kind') == 'attempt']  # noqa: E731
    return CardContext(cluster=cluster or FakeCluster(), proxy=FakeProxy(ledger.path), rig=rig,
                       pass_dir=tmp, local_dirs=list(local_dirs), policy=policy, variant=policy,
                       cache_enabled=False, manifest=manifest(card.name),
                       replay=lambda: L.replay(attempts(), Inventory(), list(families)),
                       window=lambda: {'start_seq': 1, 't_start_us': ledger.rows[0]['t_us']})


def foreground(pred):
    """The ledger section a run exactly matching `pred` would record (a range takes its lower end)."""
    return {c: {op: counts(w if isinstance(w, int) else w['min']) for op, w in ops.items() if w}
            for c, ops in pred['expected']['foreground'].items()}


def failing(out):
    return sorted(name for name, (expected, actual) in out.items() if expected != actual)


class RegistryAndArtifactsTest(unittest.TestCase):
    def test_probes_and_samples_registered(self):
        self.assertTrue(set(cli.PROBES) <= set(CARDS))
        self.assertTrue({'merge-natural', 'gc-scale'} <= set(CARDS))

    def test_manifests_and_predictions_validate_and_name_registered_cards(self):
        for name in os.listdir(os.path.join(R, 'experiments')):
            with open(os.path.join(R, 'experiments', name)) as f:
                m = json.load(f)
            self.assertEqual(artifacts.validate_experiment(m), [], name)
            self.assertIn(m['card'], CARDS)
            self.assertTrue(set(m['policies']) <= set(CARDS[m['card']].policies) | {'s3', 'cas', 'local'})
        for name in os.listdir(os.path.join(R, 'predictions')):
            with open(os.path.join(R, 'predictions', name)) as f:
                pred = json.load(f)
            self.assertEqual(artifacts.validate_prediction(pred), [], name)
            self.assertTrue(pred['source_refs'], name)

    def test_formula_cells_named_after_probes_carry_the_predicted_counts(self):
        with open(os.path.join(R, 'formulas.json')) as f:
            named = {op: (fdoc['variant'], cell) for fdoc in json.load(f) for op, cell in fdoc['cells'].items()}
        # A probe's cells are named `<probe>` or `<probe>.<operation>`; the drill bills them together.
        cells = {}
        for name, (variant, cell) in named.items():
            probe = name.split('.')[0]
            v, rows = cells.get(probe, (variant, []))
            self.assertEqual(v, variant, name)
            cells[probe] = (variant, rows + cell['ledger_rows'])
        self.assertEqual(set(cells), set(cli.PROBES))
        self.assertEqual(sorted(n for n in named if n.startswith('probe_replicated_fetch')),
                         ['probe_replicated_fetch.inserts', 'probe_replicated_fetch.recovery'])
        for probe, (variant, ledger_rows) in cells.items():
            m = manifest(probe)
            self.assertEqual(variant, {'s3': 's3', 'cas': 'cas', 'local': 'ebs'}[m['policies'][0]])
            pred = probes.prediction(m['experiment_id'], m['policies'][0])
            requests = [r for r in ledger_rows if r['row_type'] == 'request']
            predicted = [(op, want) for ops in pred['expected']['foreground'].values() for op, want in ops.items()]
            # Every op the prediction names has a row, so the drill checks and bills it; an omission fails here.
            self.assertEqual({op for op, _ in predicted} - {r['op_kind'] for r in requests}, set(), probe)
            for op in {op for op, _ in predicted}:
                consts = sum(r['expr']['value'] for r in requests if r['op_kind'] == op and r['expr']['kind'] == 'const')
                exact = sum(w for o, w in predicted if o == op and isinstance(w, int))
                self.assertEqual(consts, exact, f'{probe} {op}')
                if any(o == op and not isinstance(w, int) for o, w in predicted):
                    inputs = [r for r in requests if r['op_kind'] == op and r['expr']['kind'] == 'input']
                    self.assertTrue(inputs, f'{probe} {op}: a predicted range needs a measured input row')
            for r in ledger_rows:
                if r['expr']['kind'] == 'input':
                    self.assertIn(r['expr']['id'], REGISTRY_IDS, probe)
        fetch = cells['probe_replicated_fetch'][1]
        self.assertEqual([(r['op_kind'], r['route'], r['expr']['id']) for r in fetch if r['row_type'] == 'transfer'],
                         [('interserver_from_target', 'ec2_ec2_cross_az', 'probe_interserver_gib'),
                          ('interserver_to_target', 'ec2_ec2_cross_az', 'probe_interserver_to_gib')])

    def test_settings_and_controls_come_from_the_manifest(self):
        card = CARDS['probe_multipart_fault']
        ctx = ctx_for(card, '/p', Ledger(os.devnull), 's3')
        self.assertEqual(card.settings(ctx)['server'], {'storage_configuration.disks.s3.s3_retry_attempts': '1'})
        self.assertEqual(CARDS['probe_cas_gc_cohort'].controls(ctx_for(CARDS['probe_cas_gc_cohort'], '/p',
                                                                       Ledger(os.devnull), 'cas'))['gc'], 'measure')
        self.assertIn('CODEC(NONE)', card.p(ctx)['columns'])


class CompareLedgerTest(unittest.TestCase):
    PRED = {'expected': {'foreground': {'r1': {'PutObject': 3, 'GetObject': {'min': 1, 'max': 2}}}},
            'tolerance': {'abs': {'PutObject': 1}, 'rel': {}}}

    def test_exact_both_directions(self):
        fg = {'r1': {'PutObject': counts(3), 'GetObject': counts(2)}}
        self.assertEqual(failing(probes.compare_ledger(self.PRED, fg, True)), [])
        fg['r1']['PutObject'] = counts(4)
        fg['r2'] = {'HeadObject': counts(1)}
        fg['r1']['GetObject'] = counts(3)
        self.assertEqual(failing(probes.compare_ledger(self.PRED, fg, True)),
                         ['ledger.r1.GetObject', 'ledger.r1.PutObject', 'ledger.unpredicted'])

    def test_stochastic_uses_tolerance_and_ignores_unpredicted(self):
        fg = {'r1': {'PutObject': counts(4), 'GetObject': counts(1)}, 'r2': {'HeadObject': counts(9)}}
        self.assertEqual(failing(probes.compare_ledger(self.PRED, fg, False)), [])
        fg['r1']['PutObject'] = counts(5)
        self.assertEqual(failing(probes.compare_ledger(self.PRED, fg, False)), ['ledger.r1.PutObject'])


def ok_draft(pred, crossing=()):
    return {'ledger': {'foreground': foreground(pred)}, 'window': {'crossing': list(crossing)}}


class S3InsertTest(unittest.TestCase):
    def test_assertions_both_directions(self):
        card = CARDS['probe_s3_insert']
        with tempfile.TemporaryDirectory() as tmp:
            ctx = ctx_for(card, tmp, Ledger(os.path.join(tmp, 'l')), 's3')
            card.state(ctx)['rows'] = [1000]
            pred = card.predicted(ctx)
            self.assertEqual(failing(card.assertions(ctx, ok_draft(pred))), [])
            draft = ok_draft(pred, crossing=[7])
            draft['ledger']['foreground']['r1']['PutObject'] = counts(12)
            draft['ledger']['foreground']['r1']['HeadObject'] = counts(1)
            card.state(ctx)['rows'] = [999]
            self.assertEqual(failing(card.assertions(ctx, draft)),
                             ['ledger.r1.PutObject', 'ledger.unpredicted', 'rows.r1', 'window.crossing'])


class ReplicatedFetchTest(unittest.TestCase):
    def draft(self, pred, from1=500, from2=0, fetch1=0, fetch2=1):
        d = ok_draft(pred)
        d['interserver'] = {'relay0': {'from_target': from1}, 'relay1': {'from_target': from2}}
        d['events'] = {'r1': {'ReplicatedPartFetches': fetch1}, 'r2': {'ReplicatedPartFetches': fetch2}}
        return d

    def test_assertions_both_directions(self):
        card = CARDS['probe_replicated_fetch']
        with tempfile.TemporaryDirectory() as tmp:
            good = ctx_for(card, tmp, Ledger(os.path.join(tmp, 'l')), 's3', FakeCluster(2, {'DownloadPart': '1\n'}))
            card.state(good)['rows'] = [1000, 1000]
            pred = card.predicted(good)
            self.assertEqual(failing(card.assertions(good, self.draft(pred))), [])
            bad = ctx_for(card, tmp + '/x', Ledger(os.path.join(tmp, 'l')), 's3', FakeCluster(2))
            card.state(bad)['rows'] = [1000, 0]
            draft = self.draft(pred, from1=0, from2=7, fetch1=1, fetch2=0)
            del draft['ledger']['foreground']['r2']
            self.assertEqual(failing(card.assertions(bad, draft)),
                             ['ReplicatedPartFetches.r1', 'ReplicatedPartFetches.r2', 'ledger.r2.GetObject',
                              'ledger.r2.PutObject', 'part_log.r2.DownloadPart', 'r2.PutObject>=1',
                              'relay0.from_target>0', 'relay1.from_target', 'rows.r2'])


class ReplicatedFetchObserveTest(unittest.TestCase):
    def test_inputs_split_storage_by_creator_and_count_both_directions(self):
        card = CARDS['probe_replicated_fetch']
        with tempfile.TemporaryDirectory() as tmp:
            led = Ledger(os.path.join(tmp, 'l'))
            led.add('PutObject', 'a', caller='r1', req_payload_bytes=3600, t=0)
            led.add('PutObject', 'b', caller='r2', req_payload_bytes=7200, t=0)
            led.add('GetObject', 'a', caller='r2')
            ctx = ctx_for(card, tmp, led, 's3', FakeCluster(2))
            bh = L.byte_hours(ctx.replay(), 0, 3_600_000_000)
            draft = {'ledger': {'foreground': L.summarize(led.rows[1:])}, 'window': {'t_start_us': 0, 't_end_us': 3_600_000_000},
                     'inventory': {'byte_hours': bh},
                     'interserver': {'relay0': {'from_target': 1 << 30, 'to_target': 1 << 20},
                                     'relay1': {'from_target': 0, 'to_target': 0}}}
            inputs = card.observe(ctx, draft)['inputs']
        self.assertEqual((inputs['probe_interserver_gib'], inputs['probe_interserver_to_gib']), (1.0, 1 / 1024))
        self.assertEqual(inputs['probe_r2_get_requests'], 1)
        r1, r2 = inputs['probe_r1_active_gib_months'], inputs['probe_r2_active_gib_months']
        self.assertAlmostEqual(r2, 2 * r1)
        self.assertAlmostEqual(r1 + r2, inputs['probe_active_gib_months'])


class ActiveByCreatorTest(unittest.TestCase):
    def test_split_follows_the_cards_reclassification(self):
        # An object the card reclassifies (garbage after a drop) is no longer `active` in the per-creator split.
        class Card:
            def reclassify(self, ctx, bounds):
                bounds.reclassify([('b', 'b')], 'garbage', 0, 'test')
        with tempfile.TemporaryDirectory() as tmp:
            led = Ledger(os.path.join(tmp, 'l'))
            led.add('PutObject', 'a', caller='r1', req_payload_bytes=3600, t=0)
            led.add('PutObject', 'b', caller='r2', req_payload_bytes=7200, t=0)
            ctx = ctx_for(CARDS['probe_replicated_fetch'], tmp, led, 's3', FakeCluster(2))
            out = probes.active_by_creator(Card(), ctx, {'window': {'t_start_us': 0, 't_end_us': 3_600_000_000}})
        self.assertGreater(out['r1'], 0)
        self.assertEqual(out.get('r2', 0), 0)


class MultipartFaultTest(unittest.TestCase):
    KEY = f'{PREFIX}/r1/abc/sbin'

    def run_probe(self, tmp, part2_payloads=('x', 'x'), part2_size=7_384_936, abort=True, extra=()):
        card = CARDS['probe_multipart_fault']
        led = Ledger(os.path.join(tmp, 'ledger'))
        led.add('CreateMultipartUpload', self.KEY, response_body={'upload_id': 'u1'})
        led.add('UploadPart', self.KEY, upload_id='u1', part_number=1, req_payload_bytes=33_554_432, payload_sha256='a')
        led.add('UploadPart', self.KEY, upload_id='u1', part_number=2, req_payload_bytes=part2_size,
                payload_sha256=part2_payloads[0], down='dropped')
        led.add('UploadPart', self.KEY, upload_id='u1', part_number=2, req_payload_bytes=part2_size,
                payload_sha256=part2_payloads[1])
        for _ in range(4):
            led.add('CompleteMultipartUpload', self.KEY, upload_id='u1', up='not_sent', down='injected')
        for _ in range(4):
            led.add('AbortMultipartUpload', self.KEY, upload_id='u1', up='not_sent', down='injected')
        for op, key in extra:
            led.add(op, key)
        rig = mock.Mock()
        rig.abort_multipart.side_effect = (lambda b, k, u: led.add('AbortMultipartUpload', k, caller='rig',
                                                                   upload_id=u)) if abort else None
        # The shared observer client (the proxy's rig listener) does the abort; no card builds its own client.
        ctx = ctx_for(card, tmp, led, 's3', FakeCluster(1), rig=rig)
        card.complete(ctx)
        card.teardown(ctx)
        replay = ctx.replay()
        t0, t1 = led.rows[0]['t_us'], led.t + 100
        draft = {'ledger': {'foreground': L.summarize([r for r in led.rows[1:] if r['caller'] != 'rig'])},
                 'window': {'crossing': []},
                 'inventory': {'byte_hours': L.byte_hours(replay, t0, t1)},
                 'observations': {'expected_exceptions': [{'type': 'RuntimeError', 'message': 'boom'}]}}
        draft['observations'].update(card.observe(ctx, draft))
        return card, ctx, draft, rig

    def test_assertions_hold_on_the_predicted_shape(self):
        with tempfile.TemporaryDirectory() as tmp:
            card, ctx, draft, rig = self.run_probe(tmp, extra=[('PutObject', f'{PREFIX}/r1/k')])
            rig.abort_multipart.assert_called_once_with('b', self.KEY, 'u1')
            self.assertEqual(failing(card.assertions(ctx, draft)), [])
            self.assertGreater(draft['observations']['inputs']['probe_incomplete_gib_months'], 0)
            self.assertFalse(hasattr(probes, 'rig_client'))

    def test_assertions_fail_on_the_wrong_shape(self):
        with tempfile.TemporaryDirectory() as tmp:
            card, ctx, draft, _ = self.run_probe(tmp, part2_payloads=('x', 'y'), part2_size=1, abort=False,
                                                 extra=[('CompleteMultipartUpload', self.KEY)])
            draft['observations'].pop('expected_exceptions')
            draft['ledger']['foreground']['r1'].pop('PutObject', None)
            self.assertEqual(failing(card.assertions(ctx, draft)),
                             ['incomplete_after_abort', 'insert_failed', 'ledger.r1.CompleteMultipartUpload',
                              'ledger.r1.PutObject', 'no_object_created', 'part2.correlated', 'part2.same_payload', 'residue.lower.in_band',
                              'residue.upper.in_band'])

    def test_multipart_shape_assertions_fail_when_the_shape_differs(self):
        with tempfile.TemporaryDirectory() as tmp:
            card, ctx, draft, _ = self.run_probe(tmp, extra=[('PutObject', f'{PREFIX}/r1/k')])
            fg = draft['ledger']['foreground']['r1']
            fg['CreateMultipartUpload'], fg['UploadPart'] = counts(2), counts(1)
            card.state(ctx)['part_identities'] = 3
            draft['inventory']['byte_hours']['incomplete'] = {'lower': 0.0, 'upper': 0.0}
            self.assertEqual([n for n in failing(card.assertions(ctx, draft)) if not n.startswith('ledger.')],
                             ['CreateMultipartUpload', 'UploadPart>=2', 'incomplete_byte_hours>0', 'part_identities'])

    def test_faults_installed_before_the_insert(self):
        card = CARDS['probe_multipart_fault']
        with tempfile.TemporaryDirectory() as tmp:
            ctx = ctx_for(card, tmp, Ledger(os.path.join(tmp, 'l')), 's3', FakeCluster(1))
            seen = []
            ctx.cluster.clients[0].query = lambda sql, settings=None, timeout=0: seen.append(len(ctx.proxy.faults))
            card.measured(ctx)
            self.assertEqual(seen, [3])
            self.assertEqual([f['op'] for f in ctx.proxy.faults],
                             ['UploadPart', 'CompleteMultipartUpload', 'AbortMultipartUpload'])

    def test_multipart_residue_prediction_arithmetic(self):
        pred = probes.prediction('probe_multipart_fault.gate', 's3')['inputs']
        self.assertEqual(probes.residue_band(1 << 20), ((1 << 20) + 25, (1 << 20) + 25 * 16))
        lo, hi = probes.residue_band(pred['s_chars_total'])
        self.assertEqual([lo, hi], pred['residue_bytes_band'])
        # 64 hex characters per row of 16, minus 2 for each leading zero byte hex() drops: never above 1 KiB a row.
        self.assertLessEqual(pred['s_chars_total'], pred['rows'] * 1024)
        self.assertGreater(pred['s_chars_total'], pred['rows'] * 1024 * 0.99)
        for size in (lo, hi):
            parts = probes.multipart_parts(size)
            self.assertEqual(len(parts), pred['part_identities'])
            self.assertEqual((parts[0], sum(parts)), (32 << 20, size))
        self.assertEqual(probes.multipart_parts(32 << 20), [])
        self.assertEqual(probes.multipart_parts((48 << 20) + 1), [32 << 20, 16 << 20, 1])
        self.assertEqual(pred['http_attempts_per_failing_call'], (pred['s3_retry_attempts'] + 1) ** 2)


CAS_P = f'{PREFIX}/shared'
BLOB = f'{CAS_P}/blobs/ch128/ab/ab' + '0' * 30
MANIFEST_KEY = f'{CAS_P}/cas/manifests/yardstick-r1/0123/0000000000000001-0000000000000002/000001.zst'
CONTROL = [f'{BLOB}.meta', f'{CAS_P}/cas/ns/stream/' + 'f' * 32 + '/_log/0001-0002.log',
           f'{CAS_P}/cas/ns/state/' + 'f' * 32 + '/_ckpt', f'{CAS_P}/cas/ref_catalog', f'{CAS_P}/gc/state',
           f'{CAS_P}/gc/hb', f'{CAS_P}/gc/gen/3/attempt/1/fold_seal', f'{CAS_P}/gc/server-roots/yardstick-r1/owner',
           f'{CAS_P}/_pool_meta', f'{CAS_P}/roots/yardstick-r1/clickhouse_access_check']


class CasGcCohortTest(unittest.TestCase):
    def test_cohort_excludes_control_families(self):
        with tempfile.TemporaryDirectory() as tmp:
            led = Ledger(os.path.join(tmp, 'l'))
            for key in [BLOB, MANIFEST_KEY] + CONTROL:
                led.add('PutObject', key)
            led.add('PutObject', f'{CAS_P}/blobs/ch128/cd/cd11', caller='r2')
            led.add('PutObject', f'{CAS_P}/blobs/ch128/ef/ef22', up='error')
            led.add('HeadObject', f'{CAS_P}/blobs/ch128/aa/aa33')
            self.assertEqual(cas_probe.cohort(led.rows[1:]), {('b', BLOB), ('b', MANIFEST_KEY)})
            names = [L.family_of(cas_probe.CAS_FAMILIES, k)['name'] for k in CONTROL]
            self.assertEqual(names, ['blob_meta', 'refs', 'refs', 'refs', 'gc', 'gc', 'gc', 'lease', 'pool_meta', 'roots'])
            # The pass prefix holds the policy name `cas`; anchored regexes never take it for a layout key.
            self.assertIsNone(L.family_of(cas_probe.CAS_FAMILIES, 'run/cas/probe_cas_gc_cohort/gate/k1/r1/x'))

    def test_deletions_after_drop_by_workload_callers(self):
        keys = {('b', BLOB), ('b', MANIFEST_KEY)}
        with tempfile.TemporaryDirectory() as tmp:
            led = Ledger(os.path.join(tmp, 'l'))
            led.add('DeleteObject', BLOB, t=10)
            led.add('DeleteObject', BLOB, caller='rig', t=20_000)
            led.add('DeleteObject', BLOB, up='error', t=20_100)
            led.add('DeleteObjects', None, caller='r2', t=30_000,
                    response_body={'delete_results': [{'key': MANIFEST_KEY, 'deleted': True, 'code': None}]})
            self.assertEqual(cas_probe.deletions(led.rows[1:], keys, 15_000),
                             {('b', MANIFEST_KEY): {'t_us': 30_010, 'caller': 'r2'}})
            led.add('DeleteObject', BLOB, t=40_000)
            self.assertEqual(set(cas_probe.deletions(led.rows[1:], keys, 15_000)), keys)

    def run_probe(self, tmp, reclaim=True, gc_rows='2\n', expect=False, payload=True, rounds=''):
        card = CARDS['probe_cas_gc_cohort']
        led = Ledger(os.path.join(tmp, 'ledger'))
        if payload:
            led.add('PutObject', BLOB, req_payload_bytes=1000, expect_negotiated=expect)
            led.add('PutObject', MANIFEST_KEY, req_payload_bytes=200)
        led.add('PutObject', CONTROL[4], req_payload_bytes=50)
        future = 4 * 10 ** 15  # microseconds in 2096: after the DROP the card stamps from the real clock
        if reclaim:
            led.add('DeleteObject', BLOB, t=future)
            led.add('DeleteObject', MANIFEST_KEY, t=future + 2_000_000)
        cluster = FakeCluster(1, {'GROUP BY round_id': rounds, 'system.tables': '0\n', 'cas_gc_log': gc_rows,
                                  'count() FROM yardstick.t': '20000\n'})
        ctx = ctx_for(card, tmp, led, 'cas', cluster, cas_probe.CAS_FAMILIES)
        card.p(ctx)['reclaim_timeout_s'] = 0
        card.complete(ctx)
        bounds = ctx.replay()
        card.reclassify(ctx, bounds)
        split = card.classify(ctx, led.rows[1:], cas_probe.CAS_FAMILIES)
        draft = {'ledger': {'foreground': L.summarize(split['foreground']), 'maintenance': L.summarize(split['maintenance'])},
                 'window': {'crossing': [], 't_start_us': led.rows[0]['t_us'], 't_end_us': future + 3_000_000},
                 'inventory': {'byte_hours': L.byte_hours(bounds, led.rows[0]['t_us'], future + 3_000_000)}}
        draft['observations'] = card.observe(ctx, draft)
        return card, ctx, draft, cluster

    def test_request_origin_follows_the_callers_gc_rounds(self):
        rows = [{'caller': c, 't_start_us': t, 'op': 'HeadObject', 'key': BLOB}
                for c, t in (('r1', 5), ('r1', 15), ('r2', 15), ('r1', 40), ('rig', 15))]
        out = cas_probe.attribute(rows, {'r1': [[10, 20], [30, None]], 'r2': []}, [])
        self.assertEqual([r['t_start_us'] for r in out['maintenance']], [15, 40])  # r1 inside its own rounds only
        self.assertEqual([(r['caller'], r['t_start_us']) for r in out['foreground']], [('r1', 5), ('r2', 15)])
        self.assertEqual(len(out['rig']), 1)
        client = FakeClient({'round_id': '10\t20\t1\t1\n30\t0\t0\t1\n'})
        self.assertEqual(cas_probe.gc_rounds(client), [[10, 20], [30, None]])
        # A round seen only by its Finish has no start to attribute from: a problem, not a start at 0.
        with self.assertRaisesRegex(ValueError, 'no Start row'):
            cas_probe.gc_rounds(FakeClient({'round_id': '0\t20\t1\t0\n'}))

    def test_control_families_are_maintenance_outside_gc_rounds(self):
        # Key shapes from the gate ledger of run cost-20261024T030843Z: lease renewal, GC heartbeat and state, pool meta.
        base = 'cost-20261024T030843Z/cas/probe_cas_gc_cohort/gate/k1/shared'
        control = [f'{base}/gc/server-roots/yardstick-r1/mount', f'{base}/gc/server-roots/yardstick-r2/epoch',
                   f'{base}/gc/server-roots/yardstick-r1/store/9a7/mount', f'{base}/gc/hb', f'{base}/gc/state',
                   f'{base}/gc/maintenance_state', f'{base}/gc/gen/1/attempt/1/blob_target/0/0',
                   f'{base}/gc/gen/0/attempt/0/fold_seal', f'{base}/_pool_meta']
        payload = [f'{base}/blobs/ch128/72/7293cdc82844686a6a356fb83506d0f6',
                   f'{base}/blobs/ch128/72/7293cdc82844686a6a356fb83506d0f6.meta',
                   f'{base}/cas/manifests/yardstick-r1/store/9a7/9a738b50-4881-4eed-8d2f-ab13dd48ea80@cas@/'
                   '0000000000000001-0000000000000001/000001.zst', f'{base}/cas/ref_catalog',
                   f'{base}/roots/yardstick-r1/clickhouse_access_check_b75a1931-bc70-4380-b7df-86fd56f821a4']
        rows = [{'caller': 'r1', 't_start_us': 5, 'op': 'GetObject', 'key': k} for k in control + payload]
        rows += [{'caller': 'r1', 't_start_us': 5, 'op': 'DeleteObjects', 'key': '', 'delete_keys': control[:2]},
                 {'caller': 'r1', 't_start_us': 5, 'op': 'DeleteObjects', 'key': '', 'delete_keys': control[:1] + payload[:1]}]
        out = cas_probe.attribute(rows, {'r1': []}, cas_probe.CAS_FAMILIES)
        self.assertEqual([r['key'] for r in out['maintenance']], control + [''])
        self.assertEqual([r['key'] for r in out['foreground']], payload + [''])
        self.assertEqual([f['name'] for f in cas_probe.CAS_FAMILIES if f.get('maintenance')], ['lease', 'gc', 'pool_meta'])

    def test_classify_without_collected_rounds_fails(self):
        card = CARDS['probe_cas_gc_cohort']
        with tempfile.TemporaryDirectory() as tmp:
            ctx = ctx_for(card, tmp, Ledger(os.path.join(tmp, 'l')), 'cas')
            with self.assertRaisesRegex(RuntimeError, 'GC rounds were not collected'):
                card.classify(ctx, [], cas_probe.CAS_FAMILIES)

    def test_gc_round_traffic_is_maintenance_and_priced_with_the_rest(self):
        with tempfile.TemporaryDirectory() as tmp:
            # The manifest put (second row) falls inside a round; the `gc/state` put (third row) is a control family.
            start = 1_000_000 + 2000
            card, ctx, draft, _ = self.run_probe(tmp, rounds=f'{start - 1}\t{start + 1}\t1\t1\n')
            self.assertEqual(draft['ledger']['maintenance']['r1']['PutObject']['attempts'], 2)
            self.assertEqual(draft['ledger']['foreground']['r1']['PutObject']['attempts'], 1)
            inputs = draft['observations']['inputs']
            self.assertEqual(inputs['probe_put_requests'], 3)  # cells price foreground and maintenance together
            self.assertGreater(inputs['probe_metadata_gib_months'], 0)
            self.assertEqual(draft['observations']['request_attribution'],
                             {'method': 'gc_round_windows', 'rounds': {'r1': 1}})

    def test_assertions_both_directions(self):
        with tempfile.TemporaryDirectory() as tmp:
            card, ctx, draft, cluster = self.run_probe(tmp)
            self.assertIn('DROP TABLE yardstick.t SYNC', cluster.clients[0].log)
            self.assertEqual(draft['observations']['cohort_size'], 2)
            self.assertAlmostEqual(draft['observations']['reclaim_delay_s'],
                                   (4 * 10 ** 15 + 2_000_010 - card.state(ctx)['t_drop_us']) / 1e6)
            pred = copy.deepcopy(card.predicted(ctx))
            # Blob and manifest puts; the `gc/state` put is maintenance (control family).
            pred['expected']['foreground'] = {'r1': {'PutObject': {'min': 2, 'max': 2}}}
            with mock.patch.object(type(card), 'predicted', return_value=pred):
                self.assertEqual(failing(card.assertions(ctx, draft)), [])
        with tempfile.TemporaryDirectory() as tmp:
            card, ctx, draft, _ = self.run_probe(tmp, reclaim=False, gc_rows='0\n', expect=True)
            self.assertEqual(failing(card.assertions(ctx, draft)),
                             ['cohort_reclaimed', 'deletes_by_r1', 'expect_negotiated_rows', 'gc_log_evidence',
                              'ledger.r1.HeadObject',
                              'ledger.r1.PutObject', 'reclaim_delay_s>0'])
            self.assertFalse(draft['observations']['expect_matches_source'])
        with tempfile.TemporaryDirectory() as tmp:
            card, ctx, draft, _ = self.run_probe(tmp, payload=False)
            self.assertEqual([n for n in failing(card.assertions(ctx, draft)) if not n.startswith('ledger.')],
                             ['cohort_nonempty', 'deletes_by_r1', 'garbage_byte_hours>0', 'reclaim_delay_s>0'])


class CasExpectTest(unittest.TestCase):
    """The `Expect: 100-continue` probe: a low `expect_continue_min_bytes` makes CAS conditional PUTs negotiate."""

    def run_probe(self, tmp, expect=True, duplicate=False, gc_log=False):
        card = CARDS['probe_cas_expect']
        led = Ledger(os.path.join(tmp, 'ledger'))
        led.add('PutObject', BLOB, req_payload_bytes=1000)
        led.add('PutObject', MANIFEST_KEY, req_payload_bytes=200, expect_negotiated=expect,
                req_headers={'if-none-match': '*', 'expect': '100-continue'} if expect else {})
        led.add('PutObject', f'{CAS_P}/gc/server-roots/yardstick-r1/mount', req_payload_bytes=50)  # lease renewal
        answers = {"name = 'cas_gc_log'": '1\n' if gc_log else '0\n', 'GROUP BY round_id': '10\t20\t1\t1\n',
                   'count() FROM yardstick.t': '3000\n'}
        ctx = ctx_for(card, tmp, led, 'cas', FakeCluster(1, answers), cas_probe.CAS_FAMILIES)
        card.complete(ctx)
        split = card.classify(ctx, led.rows[1:], cas_probe.CAS_FAMILIES)
        if duplicate:
            split['foreground'].append(led.rows[2])
        end = led.rows[-1]['seq'] + 1
        draft = {'ledger': {'foreground': L.summarize(split['foreground']), 'maintenance': L.summarize(split['maintenance'])},
                 'window': {'crossing': [], 'start_seq': 1, 'end_seq': end, 't_start_us': led.rows[0]['t_us'],
                            't_end_us': led.t + 1000},
                 'inventory': {'byte_hours': L.byte_hours(ctx.replay(), led.rows[0]['t_us'], led.t + 1000)}}
        draft['observations'] = card.observe(ctx, draft)
        return card, ctx, draft

    def test_manifest_lowers_the_expect_threshold_and_gc_is_off(self):
        m = manifest('probe_cas_expect')
        self.assertEqual(m['settings']['server'], {'storage_configuration.disks.cas.expect_continue_min_bytes': '1'})
        self.assertEqual((m['controls']['gc'], m['replicas'], CARDS['probe_cas_expect'].replicas), ('off', 1, 1))
        pred = probes.prediction('probe_cas_expect.gate', 'cas')
        self.assertTrue(any('expect_continue_min_bytes' in r['claim'] for r in pred['source_refs']))
        self.assertIn('probe_cas_expect', cli.PROBES)

    def test_assertions_both_directions(self):
        with tempfile.TemporaryDirectory() as tmp:
            card, ctx, draft = self.run_probe(tmp)
            obs = draft['observations']
            self.assertEqual((obs['expect_negotiated_rows'], obs['request_attribution']['method']),
                             (1, 'control_families'))
            # The lease renewal is maintenance although GC is off; foreground holds the blob and manifest PUTs.
            self.assertEqual(draft['ledger']['foreground']['r1']['PutObject']['attempts'], 2)
            pred = copy.deepcopy(card.predicted(ctx))
            pred['expected']['foreground'] = {'r1': {'PutObject': {'min': 2, 'max': 2}}}
            with mock.patch.object(type(card), 'predicted', return_value=pred):
                self.assertEqual(failing(card.assertions(ctx, draft)), [])
                with tempfile.TemporaryDirectory() as tmp2:
                    _, ctx2, bad = self.run_probe(tmp2, expect=False, duplicate=True)
                    self.assertEqual(failing(card.assertions(ctx2, bad)),
                                     ['expect_attempts_counted_once', 'expect_negotiated_rows>=1', 'ledger.r1.PutObject'])

    def test_gc_rounds_in_the_log_fail_the_gc_off_card(self):
        with tempfile.TemporaryDirectory() as tmp:
            card, ctx, draft = self.run_probe(tmp, gc_log=True)
            self.assertIn('no_gc_rounds', failing(card.assertions(ctx, draft)))


STRACE = """101 1.000001 write(5</d/local/r1/store/t/all_1_1_0/data.bin>, "..."..., 4096) = 4096
101 1.000002 write(6</d/r1/server.log>, "x", 1) = 1
101 1.000003 pwrite64(7</d/r1/data/other.bin>, "..."..., 512, 0) = 512
101 1.000004 read(5</d/local/r1/store/t/all_1_1_0/data.bin>, "..."..., 100) = 100
101 5.000000 write(5</d/local/r1/store/t/all_1_1_0/data.bin>, "..."..., 999) = 999
"""


class LocalVolumeTest(unittest.TestCase):
    def draft(self, write_bytes=4096, peak=8192, device='dm-0', limitations=('page cache not dropped',)):
        return {'ledger': {'foreground': {}}, 'window': {'crossing': [], 't_start_us': 0, 't_end_us': 2_000_000},
                'inventory': {'byte_hours': {}},
                'proc_io': {'r1': {'write_bytes': write_bytes}},
                'local': {'r1': {'peak_allocated_bytes': peak, 'device': device, 'fs': 'ext4',
                                 'limitations': list(limitations)}}}

    def test_observe_and_assertions_both_directions(self):
        card = CARDS['probe_local_volume']
        with tempfile.TemporaryDirectory() as tmp:
            with open(os.path.join(tmp, 'strace.r1.g0.txt'), 'w') as f:
                f.write(STRACE)
            ctx = ctx_for(card, tmp, Ledger(os.path.join(tmp, 'l')), 'local', FakeCluster(1), local_dirs=['/d/local/r1'])
            card.state(ctx)['rows'] = [4_000_000]
            draft = self.draft()
            draft['observations'] = card.observe(ctx, draft)
            obs = draft['observations']
            self.assertEqual(obs['data_dir_write_syscalls'], ['write'])
            inputs = obs['inputs']
            # The write at 5 s is after the 0..2 s window: counted as outside, not as demand.
            self.assertEqual((inputs['probe_block_iops'], inputs['probe_block_capacity_gib']), (1.0, 8192 / (1 << 30)))
            self.assertAlmostEqual(inputs['probe_block_mibps'], 4196 / (1 << 20) / 2)
            self.assertEqual((obs['data_dir_outside'], obs['data_dir_crossing']), (1, 0))
            self.assertEqual(failing(card.assertions(ctx, draft)), [])
            with open(os.path.join(tmp, 'strace.r1.g0.txt'), 'w') as f:
                f.write(STRACE.replace('write(5', 'writev(5'))
            bad = self.draft(write_bytes=0, peak=0, device='', limitations=())
            bad['observations'] = card.observe(ctx, bad)
            self.assertEqual(failing(card.assertions(ctx, bad)),
                             ['data_dir.write.count>0', 'data_dir.write.histogram', 'device_fs_recorded',
                              'limitations_recorded', 'proc_io.write_bytes>0', 'union_peak>0'])


class SampleManifestsTest(unittest.TestCase):
    def test_samples_carry_the_planned_scale(self):
        with open(os.path.join(R, 'experiments', 'merge-natural.sample.json')) as f:
            merge = json.load(f)
        with open(os.path.join(R, 'experiments', 'gc-scale.sample.json')) as f:
            gc = json.load(f)
        self.assertEqual((merge['params']['inserts'], merge['params']['rows'], merge['params']['columns'].count(',') + 1,
                          merge['controls']['merges']), (64, 100000, 5, 'measure'))
        self.assertEqual((gc['params']['columns'].count(',') + 1, gc['params']['inserts'],
                          gc['settings']['table']['min_bytes_for_wide_part'], gc['controls']['gc']),
                         (100, 1000, 0, 'measure'))
        self.assertEqual(gc['params']['insert'].count('AS n') + gc['params']['insert'].count('n * '), 100)


if __name__ == '__main__':
    unittest.main()
