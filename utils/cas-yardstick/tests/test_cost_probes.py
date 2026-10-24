"""Fixture tests of the phase-0 probe cards: every assertion is shown to hold and to fail."""
import copy
import json
import os
import sys
import tempfile
import unittest
from unittest import mock

from yardstick.cost import cli, ledger as L
from yardstick.cost.cards import CARDS, probes
from yardstick.cost.cards.base import CardContext
from yardstick.cost.ledger import Inventory

TOOLS = os.path.join(probes.REPO, 'utils', 'storage-cost-calculator', 'tools')
sys.path.insert(0, TOOLS)
import artifacts  # noqa: E402

R = probes.REPORT_DIR
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


def ctx_for(card, tmp, ledger, policy, cluster=None, families=(), local_dirs=()):
    attempts = lambda: [r for r in L.read_rows(ledger.path) if r.get('kind') == 'attempt']  # noqa: E731
    return CardContext(cluster=cluster or FakeCluster(), proxy=FakeProxy(ledger.path), rig=None,
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
            cells = {op: (fdoc['variant'], cell) for fdoc in json.load(f) for op, cell in fdoc['cells'].items()}
        self.assertEqual(set(cells), set(cli.PROBES))
        for probe, (variant, cell) in cells.items():
            m = manifest(probe)
            self.assertEqual(variant, {'s3': 's3', 'cas': 'cas', 'local': 'ebs'}[m['policies'][0]])
            pred = probes.prediction(m['experiment_id'], m['policies'][0])
            totals = {}
            for row in cell['ledger_rows']:
                if row['row_type'] == 'request':
                    totals[row['op_kind']] = totals.get(row['op_kind'], 0) + row['expr']['value']
            for op, n in totals.items():
                self.assertEqual(n, sum(ops[op] for ops in pred['expected']['foreground'].values() if op in ops),
                                 f'{probe} {op}')

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
        ctx = ctx_for(card, tmp, led, 's3', FakeCluster(1))
        card.measured_faults = None
        card.complete(ctx)
        rig = mock.Mock()
        rig.abort_multipart.side_effect = (lambda b, k, u: led.add('AbortMultipartUpload', k, caller='rig',
                                                                   upload_id=u)) if abort else None
        with mock.patch.object(probes, 'rig_client', return_value=rig):
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
            self.assertGreater(draft['observations']['probe_incomplete_gib_months'], 0)

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
            self.assertEqual(probes.cohort(led.rows[1:]), {('b', BLOB), ('b', MANIFEST_KEY)})
            names = [L.family_of(probes.CAS_FAMILIES, k)['name'] for k in CONTROL]
            self.assertEqual(names, ['blob_meta', 'refs', 'refs', 'refs', 'gc', 'gc', 'gc', 'lease', 'pool_meta', 'roots'])
            # The pass prefix holds the policy name `cas`; anchored regexes never take it for a layout key.
            self.assertIsNone(L.family_of(probes.CAS_FAMILIES, 'run/cas/probe_cas_gc_cohort/gate/k1/r1/x'))

    def test_deletions_after_drop_by_workload_callers(self):
        keys = {('b', BLOB), ('b', MANIFEST_KEY)}
        with tempfile.TemporaryDirectory() as tmp:
            led = Ledger(os.path.join(tmp, 'l'))
            led.add('DeleteObject', BLOB, t=10)
            led.add('DeleteObject', BLOB, caller='rig', t=20_000)
            led.add('DeleteObject', BLOB, up='error', t=20_100)
            led.add('DeleteObjects', None, caller='r2', t=30_000,
                    response_body={'delete_results': [{'key': MANIFEST_KEY, 'deleted': True, 'code': None}]})
            self.assertEqual(probes.deletions(led.rows[1:], keys, 15_000),
                             {('b', MANIFEST_KEY): {'t_us': 30_010, 'caller': 'r2'}})
            led.add('DeleteObject', BLOB, t=40_000)
            self.assertEqual(set(probes.deletions(led.rows[1:], keys, 15_000)), keys)

    def run_probe(self, tmp, reclaim=True, gc_rows='2\n', expect=False, payload=True):
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
        cluster = FakeCluster(1, {'system.tables': '0\n', 'cas_gc_log': gc_rows, 'count() FROM yardstick.t': '20000\n'})
        ctx = ctx_for(card, tmp, led, 'cas', cluster, probes.CAS_FAMILIES)
        card.p(ctx)['reclaim_timeout_s'] = 0
        card.complete(ctx)
        bounds = ctx.replay()
        card.reclassify(ctx, bounds)
        draft = {'ledger': {'foreground': L.summarize(led.rows[1:])}, 'window': {'crossing': []},
                 'inventory': {'byte_hours': L.byte_hours(bounds, led.rows[0]['t_us'], future + 3_000_000)}}
        draft['observations'] = card.observe(ctx, draft)
        return card, ctx, draft, cluster

    def test_assertions_both_directions(self):
        with tempfile.TemporaryDirectory() as tmp:
            card, ctx, draft, cluster = self.run_probe(tmp)
            self.assertIn('DROP TABLE yardstick.t SYNC', cluster.clients[0].log)
            self.assertEqual(draft['observations']['cohort_size'], 2)
            self.assertAlmostEqual(draft['observations']['reclaim_delay_s'],
                                   (4 * 10 ** 15 + 2_000_010 - card.state(ctx)['t_drop_us']) / 1e6)
            pred = copy.deepcopy(card.predicted(ctx))
            pred['expected']['foreground'] = {'r1': {'PutObject': {'min': 3, 'max': 3}}}
            with mock.patch.object(type(card), 'predicted', return_value=pred):
                self.assertEqual(failing(card.assertions(ctx, draft)), [])
        with tempfile.TemporaryDirectory() as tmp:
            card, ctx, draft, _ = self.run_probe(tmp, reclaim=False, gc_rows='0\n', expect=True)
            self.assertEqual(failing(card.assertions(ctx, draft)),
                             ['cohort_reclaimed', 'expect_negotiated_rows', 'gc_log_evidence', 'ledger.r1.HeadObject',
                              'ledger.r1.PutObject', 'reclaim_delay_s>0'])
            self.assertFalse(draft['observations']['expect_matches_source'])
        with tempfile.TemporaryDirectory() as tmp:
            card, ctx, draft, _ = self.run_probe(tmp, payload=False)
            self.assertEqual([n for n in failing(card.assertions(ctx, draft)) if not n.startswith('ledger.')],
                             ['cohort_nonempty', 'garbage_byte_hours>0', 'reclaim_delay_s>0'])


STRACE = """101 1.000001 write(5</d/local/r1/store/t/all_1_1_0/data.bin>, "..."..., 4096) = 4096
101 1.000002 write(6</d/r1/server.log>, "x", 1) = 1
101 1.000003 pwrite64(7</d/r1/data/other.bin>, "..."..., 512, 0) = 512
101 1.000004 read(5</d/local/r1/store/t/all_1_1_0/data.bin>, "..."..., 100) = 100
"""


class LocalVolumeTest(unittest.TestCase):
    def draft(self, write_bytes=4096, peak=8192, device='dm-0', limitations=('page cache not dropped',)):
        return {'ledger': {'foreground': {}}, 'window': {'crossing': [], 't_start_us': 0, 't_end_us': 2_000_000},
                'proc_io': {'r1': {'write_bytes': write_bytes}},
                'local': {'r1': {'peak_allocated_bytes': peak, 'device': device, 'fs': 'ext4',
                                 'limitations': list(limitations)}}}

    def test_observe_and_assertions_both_directions(self):
        card = CARDS['probe_local_volume']
        with tempfile.TemporaryDirectory() as tmp:
            with open(os.path.join(tmp, 'strace.r1.txt'), 'w') as f:
                f.write(STRACE)
            ctx = ctx_for(card, tmp, Ledger(os.path.join(tmp, 'l')), 'local', FakeCluster(1), local_dirs=['/d/local/r1'])
            card.state(ctx)['rows'] = [4_000_000]
            draft = self.draft()
            draft['observations'] = card.observe(ctx, draft)
            obs = draft['observations']
            self.assertEqual(obs['data_dir_write_syscalls'], ['write'])
            self.assertEqual((obs['probe_block_iops'], obs['probe_block_capacity_gib']), (1.0, 8192 / (1 << 30)))
            self.assertAlmostEqual(obs['probe_block_mibps'], 4196 / (1 << 20) / 2)
            self.assertEqual(failing(card.assertions(ctx, draft)), [])
            with open(os.path.join(tmp, 'strace.r1.txt'), 'w') as f:
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
