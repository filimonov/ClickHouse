import json
import os
import tempfile
import unittest

from yardstick.cost import measurements, runner
from yardstick.cost.cards.base import CostCard
from yardstick.cost.cluster import Cluster
from yardstick.cost.runner import Deps, open_uploads, run_case
from yardstick.cost.topology import Topology

FAMILIES = [{'name': 'lease', 'regex': r'/lease/', 'category': 'metadata', 'maintenance': True}]


class FakeProxy:
    """Writes a ledger JSONL: marks on `mark`, attempts on `emit`; the clock advances 10 us per row."""

    def __init__(self, path, calls):
        self.ledger_path, self.calls, self.seq, self.t = path, calls, 0, 1_000_000
        open(path, 'w').close()

    def _row(self, row):
        with open(self.ledger_path, 'a') as f:
            f.write(json.dumps(row) + '\n')

    def mark(self, label):
        self.seq += 1
        self.t += 10
        self._row({'kind': 'mark', 'seq': self.seq, 'label': label, 't_us': self.t})
        return self.seq

    def emit(self, op='PutObject', key='k', caller='r1', size=100, up='ok', end=True, **kw):
        self.seq += 1
        self.t += 10
        row = {'kind': 'attempt', 'seq': self.seq, 'caller': caller, 'store_id': 's', 't_start_us': self.t,
               't_end_us': self.t + 2 if end else None, 'bucket': 'b', 'key': key, 'op': op, 'upload_id': None,
               'part_number': None, 'copy_source': None, 'req_payload_bytes': size, 'resp_payload_bytes': 0,
               'downstream': {'outcome': 'delivered', 'status': 200},
               'upstream': {'outcome': up, 'http_status': 200, 's3_code': None},
               'response_body': kw.pop('body', {}), 'phase': 'window'}
        row.update(kw)
        self._row(row)

    def flush(self):
        self.calls.append('flush')

    open = ()

    def open_attempts(self):
        return [{'seq': s, 'caller': c, 't_start_us': 0, 'op': 'PutObject', 'key': 'k'} for s, c in self.open]

    def clear_faults(self):
        self.calls.append('clear_faults')


class FakeRig:
    def __init__(self, calls, prefix_objects=0, fail_delete=False):
        self.calls, self.objects, self.fail_delete = calls, prefix_objects, fail_delete
        self.aborts = []

    def list(self, bucket, prefix):
        self.calls.append('list')
        return [{'key': f'{prefix}{n}'} for n in range(self.objects)]

    def abort_multipart(self, bucket, key, upload_id):
        self.calls.append('abort')
        self.aborts.append((bucket, key, upload_id))

    def delete_prefix(self, bucket, prefix):
        self.calls.append('delete_prefix')
        if self.fail_delete:
            raise RuntimeError('store unreachable')
        deleted, self.objects = self.objects, 0
        return deleted


class FakeClient:
    def __init__(self, store, port):
        self.store, self.port = store, port

    def query(self, sql, settings=None, timeout=3600):
        return '1\n'

    def events(self):
        return dict(self.store.setdefault(self.port, {'Q': 0}))


class FakeRelay:
    def __init__(self, listen, target, log_path):
        self.n = 0

    def start(self):
        pass

    def stop(self):
        pass

    def snapshot(self, label):
        self.n += 1
        return {'to_target': 10 * self.n, 'from_target': 20 * self.n, 'connections_total': self.n, 'open': 1}


class FakeSampler:
    def __init__(self, paths, interval_s):
        self.paths = paths

    def start(self):
        pass

    def stop(self):
        return {'paths': self.paths, 'peak_allocated_bytes': 4096, 'samples': [[1, 0], [2, 4096]]}


class FakeCard(CostCard):
    name = 'fake'
    policies = ('s3', 'local')
    mode = 'deterministic'
    replicas = 1

    def __init__(self, calls):
        self.calls = calls
        self.on_setup = self.on_measured = self.on_teardown = lambda ctx: None

    def tables(self, ctx):
        return ['t']

    def inventory_families(self, ctx):
        return FAMILIES

    def setup(self, ctx):
        self.calls.append('setup')
        self.on_setup(ctx)

    def measured(self, ctx):
        self.calls.append('measured')
        self.on_measured(ctx)
        return {'unit_count': 2, 'ops': [], 'observations': {'part_type': 'compact'}, 'effective_counts': {'parts': 1}}

    def complete(self, ctx, timeout_s=300):
        self.calls.append('complete')

    def assertions(self, ctx, draft):
        return {'puts': (2, draft['ledger']['foreground']['r1']['PutObject']['attempts'])}

    def teardown(self, ctx):
        self.calls.append('teardown')
        self.on_teardown(ctx)


MANIFEST = {'case_id': 'small', 'card': 'fake', 'timeout_s': 5}


class RunnerFixture(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.runs_dir = tmp.name
        self.calls = []
        self.counters = {}
        self.topology = Topology(port_base=19700, replicas=1)
        self.topology.preflight_new_endpoints = lambda: []
        os.makedirs(os.path.join(tmp.name, 'run1'))
        self.proxy = FakeProxy(os.path.join(tmp.name, 'ledger.jsonl'), self.calls)
        self.rig = FakeRig(self.calls)
        self.card = FakeCard(self.calls)
        self.sh_log = []

    def sh(self, args):
        self.calls.append(f'sh_{args[0]}')
        run_dir = args[2] if args[0] == 'start' else args[1]
        if args[0] == 'start':
            with open(os.path.join(run_dir, 'server.pid'), 'w') as f:
                f.write('4242')

    def factory(self, *args, **kw):
        return Cluster(*args, sh_runner=self.sh,
                       client_factory=lambda port: FakeClient(self.counters, port),
                       relay_factory=lambda *a: FakeRelay(*a),
                       proc_io_reader=lambda pid: {f: 0 for f in ('rchar', 'wchar', 'syscr', 'syscw', 'read_bytes', 'write_bytes')},
                       **kw)

    @staticmethod
    def writer(replica_dir, replica, *rest):
        os.makedirs(replica_dir, exist_ok=True)
        return 'h' * 64

    def deps(self, **kw):
        fields = dict(binary='/bin/ch', run_id='run1', runs_dir=self.runs_dir, topology=self.topology,
                      proxy=self.proxy, rig=self.rig, store_id='s', scale=1.0, keep_prefix=False,
                      binary_sha256='0' * 64, commit='abc')
        fields.update(kw)
        return Deps(**fields)

    def go(self, policy='s3', **kw):
        return run_case(self.card, MANIFEST, policy, 0, self.deps(), cluster_factory=self.factory,
                        config_writer=self.writer, sampler_factory=FakeSampler, **kw)

    def writes(self, n, **kw):
        return lambda ctx: [ctx.proxy.emit(key='run1/s3/fake/small/k0/data/%d' % i, **kw) for i in range(n)]


class RunnerTest(RunnerFixture):
    def test_ok_record_shape_and_phases(self):
        self.card.on_setup = lambda ctx: ctx.proxy.emit(key='run1/s3/fake/small/k0/setup', size=50)
        self.card.on_measured = lambda ctx: (
            [ctx.proxy.emit(key='run1/s3/fake/small/k0/data/%d' % i, size=100) for i in range(2)],
            ctx.proxy.emit(op='ListObjectsV2', caller='rig', key=None, size=0))
        self.card.on_teardown = lambda ctx: ctx.proxy.emit(op='DeleteObject', key='run1/s3/fake/small/k0/setup', size=0)
        self.rig.objects = 0
        record = self.go()
        self.assertEqual(measurements_problems(record), [])
        self.assertEqual(record['status'], 'ok')
        self.assertEqual(record['measurement_id'], 'run1/s3/fake/small/k0')
        self.assertEqual((record['experiment_id'], record['variant'], record['cache_enabled']), ('fake.small', 's3', False))
        self.assertEqual(record['pass_dir'], os.path.join(self.runs_dir, 'run1/s3/fake/small/k0'))
        self.assertEqual(record['ledger']['foreground']['r1']['PutObject']['attempts'], 2)
        self.assertEqual(record['ledger']['foreground']['r1']['PutObject']['req_payload_bytes'], 200)
        self.assertEqual(record['ledger']['setup']['r1']['PutObject']['req_payload_bytes'], 50)
        self.assertEqual(record['ledger']['teardown']['r1']['DeleteObject']['attempts'], 1)
        self.assertEqual(record['ledger']['rig']['ListObjectsV2']['attempts'], 1)
        self.assertEqual(record['ledger']['maintenance'], {})
        self.assertEqual(record['window']['crossing'], [])
        self.assertLess(record['window']['t_start_us'], record['window']['t_end_us'])
        self.assertEqual(record['inventory']['initial'], 'empty_verified')
        self.assertGreater(record['inventory']['byte_hours']['active']['upper'], 0)
        self.assertEqual(record['assertions'], {'puts': {'expected': 2, 'actual': 2, 'ok': True}})
        self.assertEqual((record['unit_count'], record['observations'], record['effective_counts']),
                         (2, {'part_type': 'compact'}, {'parts': 1}))
        self.assertEqual(record['config_hashes'], ['h' * 64])
        self.assertEqual(record['events'], {'r1': {'Q': 0}})
        self.assertEqual(record['interserver'], {'relay0': {'to_target': 10, 'from_target': 20,
                                                           'connections_total': 1, 'open_at_end': 1}})
        self.assertEqual(record['local']['r1']['peak_allocated_bytes'], 4096)
        self.assertIsNone(record['local']['r1']['trace'])
        self.assertEqual(record['generations']['r1'][0]['pid'], 4242)
        self.assertEqual(record['cleanup'], {'aborted_uploads': 0, 'deleted_keys': 0, 'verified_empty': True, 'errors': []})
        self.assertEqual(self.calls[:3], ['list', 'sh_start', 'setup'])

    def test_unsupported_record(self):
        record = self.go(policy='cas')
        self.assertEqual(record['status'], 'UNSUPPORTED')
        self.assertIn('cas', record['reason'])
        self.assertEqual(measurements_problems(record), [])
        self.assertEqual(self.calls, [])

    def test_unsupported_when_replicas_missing(self):
        self.card.replicas = 2
        record = self.go()
        self.assertEqual((record['status'], record['reason']), ('UNSUPPORTED', 'UNSUPPORTED (needs 2 replicas)'))

    def test_prefix_not_empty_launches_nothing_and_deletes_nothing(self):
        self.rig.objects = 3
        record = self.go()
        self.assertEqual((record['status'], record['reason']), ('FAILED', 'prefix_not_empty'))
        self.assertEqual(self.calls, ['list'])
        self.assertEqual(self.rig.objects, 3)

    def test_failed_record_has_diagnostics_and_partial(self):
        def boom(ctx):
            os.makedirs(os.path.join(ctx.pass_dir, 'r1'), exist_ok=True)
            open(os.path.join(ctx.pass_dir, 'r1', 'server.err.log'), 'w').close()
            raise RuntimeError('query failed')
        self.card.on_measured = boom
        record = self.go()
        self.assertEqual(measurements_problems(record), [])
        self.assertEqual(record['status'], 'FAILED')
        self.assertEqual(record['reason'], 'RuntimeError: query failed')
        self.assertEqual(record['diagnostics']['server_logs'], [os.path.join(record['pass_dir'], 'r1', 'server.err.log')])
        self.assertIn('Traceback', record['diagnostics']['error_text'])
        self.assertEqual(sorted(record['partial']), ['config_hashes', 'effective_settings', 'generations'])
        self.assertIn('teardown', self.calls)  # cleanup still tears down
        self.assertEqual(record['cleanup']['verified_empty'], True)

    def test_deterministic_crossing_fails(self):
        self.card.on_measured = lambda ctx: ctx.proxy.emit(key='run1/s3/fake/small/k0/data/x', end=False)
        record = self.go()
        self.assertEqual((record['status'], record['reason']), ('FAILED', 'window_crossing'))
        self.assertEqual(len(record['partial']['window']['crossing']), 1)
        self.assertEqual(measurements_problems(record), [])

    def test_open_attempt_at_flush_fails_deterministic_card(self):
        def measured(ctx):
            ctx.proxy.emit(key='run1/s3/fake/small/k0/data/x')
            ctx.proxy.open = [(ctx.proxy.seq, 'r1'), (1, 'rig'), (0, 'r1')]  # begun in the window, no row yet
        self.card.on_measured = measured
        record = self.go()
        self.assertEqual((record['status'], record['reason']), ('FAILED', 'window_crossing'))
        self.assertEqual(len(record['partial']['window']['crossing']), 1)

    def test_open_attempt_from_an_earlier_pass_is_ignored(self):
        self.proxy.seq = 5  # earlier passes on the shared proxy already used these seqs
        self.card.on_measured = lambda ctx: (self.writes(2)(ctx), setattr(ctx.proxy, 'open', [(3, 'r1')]))
        self.assertEqual(self.go()['status'], 'ok')

    def test_open_attempt_after_the_window_does_not_cross(self):
        self.card.on_teardown = lambda ctx: setattr(ctx.proxy, 'open', [(ctx.proxy.seq + 1, 'r1')])
        self.card.on_measured = self.writes(2)
        self.assertEqual(self.go()['status'], 'ok')

    def test_open_attempt_is_recorded_for_stochastic_card(self):
        self.card.mode = 'stochastic'
        self.card.on_measured = lambda ctx: (self.writes(2)(ctx), setattr(ctx.proxy, 'open', [(ctx.proxy.seq, 'r1')]))
        record = self.go()
        self.assertEqual((record['status'], len(record['window']['crossing'])), ('ok', 1))

    def test_invalid_record_becomes_failed_invalid_record(self):
        self.card.on_measured = lambda ctx: ctx.proxy.emit(key='run1/s3/fake/small/k0/data/x')
        original = self.card.measured
        self.card.measured = lambda ctx: {**original(ctx), 'observations': {'bad': object()}}
        record = self.go()
        self.assertEqual((record['status'], record['reason'], record['partial']), ('FAILED', 'invalid_record', {}))
        self.assertIn('not JSON-serialisable', record['diagnostics']['error_text'])
        self.assertEqual(measurements_problems(record), [])
        self.assertIn('cleanup', record)

    def test_stochastic_crossing_is_recorded_not_failed(self):
        self.card.mode = 'stochastic'
        self.card.on_measured = lambda ctx: ctx.proxy.emit(key='run1/s3/fake/small/k0/data/x', end=False)
        record = self.go()
        self.assertEqual(record['status'], 'ok')
        self.assertEqual(len(record['window']['crossing']), 1)

    def test_lease_traffic_in_window_does_not_change_foreground(self):
        def measured(ctx):
            for i in range(2):
                ctx.proxy.emit(key='run1/s3/fake/small/k0/data/%d' % i)
            for i in range(5):
                ctx.proxy.emit(key='run1/s3/fake/small/k0/lease/%d' % i, size=10)
        self.card.on_measured = measured
        record = self.go()
        self.assertEqual(record['ledger']['foreground']['r1']['PutObject']['attempts'], 2)
        self.assertEqual(record['ledger']['maintenance']['r1']['PutObject']['attempts'], 5)
        self.assertEqual(record['assertions']['puts']['ok'], True)

    def test_expected_exception_recorded_as_observation(self):
        self.card.expected_exceptions = (ValueError,)

        def boom(ctx):
            raise ValueError('injected fault')
        self.card.on_measured = boom
        # The card's measured() returns normally in FakeCard, so wrap it: the exception comes out of measured.
        original = self.card.measured

        def measured(ctx):
            self.card.on_measured(ctx)
            return original(ctx)
        self.card.measured = measured
        self.card.assertions = lambda ctx, draft: {}
        record = self.go()
        self.assertEqual(record['status'], 'ok')
        self.assertEqual(record['unit_count'], 0)
        self.assertEqual(record['observations'],
                         {'expected_exceptions': [{'type': 'ValueError', 'message': 'injected fault'}]})

    def test_cleanup_order_and_record_persisted_after_cleanup_failure(self):
        def start_upload(ctx):
            key = 'run1/s3/fake/small/k0/big'
            ctx.proxy.emit(op='CreateMultipartUpload', key=key, size=0, body={'upload_id': 'u1'})
            ctx.proxy.emit(op='CreateMultipartUpload', key=key + '2', size=0, body={'upload_id': 'u2'})
            ctx.proxy.emit(op='CompleteMultipartUpload', key=key + '2', size=0, upload_id='u2')
        self.card.on_measured = start_upload
        self.card.assertions = lambda ctx, draft: {}
        self.rig.fail_delete = True
        record = self.go()
        self.assertEqual(record['status'], 'ok')
        self.assertEqual(self.rig.aborts, [('b', 'run1/s3/fake/small/k0/big', 'u1')])
        self.assertEqual(record['cleanup']['aborted_uploads'], 1)
        self.assertEqual(len(record['cleanup']['errors']), 1)
        self.assertIn('delete_prefix', record['cleanup']['errors'][0])
        self.assertEqual(cleanup_order(self.calls),
                         ['clear_faults', 'teardown', 'clear_faults', 'sh_stop', 'abort', 'delete_prefix'])
        path = os.path.join(self.runs_dir, 'record.json')
        measurements.write(path, record)
        self.assertEqual(measurements.read(path), record)

    def test_cleanup_order_when_the_measured_step_fails(self):
        def fail(ctx):
            ctx.proxy.emit(op='CreateMultipartUpload', key='run1/s3/fake/small/k0/big', size=0, body={'upload_id': 'u1'})
            raise RuntimeError('boom')
        self.card.on_measured = fail
        record = self.go()
        self.assertEqual(record['status'], 'FAILED')
        self.assertEqual(cleanup_order(self.calls), ['clear_faults', 'teardown', 'sh_stop', 'abort', 'delete_prefix'])
        self.assertEqual(record['cleanup'], {'aborted_uploads': 1, 'deleted_keys': 0, 'verified_empty': True, 'errors': []})

    def test_generation_accumulation_through_runner_restart(self):
        def restart(ctx):
            self.counters[19701]['Q'] = 150  # events at the close of generation 0
            ctx.cluster.stop(1)
            self.counters[19701]['Q'] = 0
            ctx.cluster.start(1)
            self.counters[19701]['Q'] = 30
        self.counters[19701] = {'Q': 100}
        self.card.on_measured = restart
        self.card.assertions = lambda ctx, draft: {}
        record = self.go()
        self.assertEqual(record['status'], 'ok')
        self.assertEqual(record['events']['r1']['Q'], 80)  # (150 - 100) + 30
        self.assertEqual(len(record['generations']['r1']), 2)

    def test_local_policy_uses_local_dirs_and_maps_variant(self):
        self.card.on_measured = self.writes(2)
        record = self.go(policy='local')
        self.assertEqual((record['variant'], record['status']), ('ebs', 'ok'))
        self.assertTrue(os.path.isdir(os.path.join(record['pass_dir'], 'local', 'r1')))

    def test_cached_policy_maps_to_base_variant(self):
        self.assertEqual(runner.variant_of('s3plain_cached'), ('s3plain', True))
        self.assertEqual(runner.variant_of('local'), ('ebs', False))


class OpenUploadsTest(unittest.TestCase):
    def test_only_workload_ok_creates_in_prefix_after_seq(self):
        def row(seq, op, key, caller='r1', up='ok', uid=None, body=None):
            return {'kind': 'attempt', 'seq': seq, 'caller': caller, 'bucket': 'b', 'key': key, 'op': op,
                    'upload_id': uid, 'upstream': {'outcome': up}, 'response_body': body or {}}
        p = 'pre/x'
        rows = [row(1, 'CreateMultipartUpload', f'{p}/a', body={'upload_id': 'old'}),
                row(5, 'CreateMultipartUpload', f'{p}/a', body={'upload_id': 'u1'}),
                row(6, 'CreateMultipartUpload', f'{p}/b', body={'upload_id': 'u2'}),
                row(7, 'AbortMultipartUpload', f'{p}/b', uid='u2'),
                row(8, 'CreateMultipartUpload', f'{p}/c', body={'upload_id': 'u3'}, caller='rig'),
                row(9, 'CreateMultipartUpload', 'other/d', body={'upload_id': 'u4'}),
                row(10, 'CreateMultipartUpload', f'{p}/e', up='error', body={'upload_id': 'u5'}),
                row(11, 'CreateMultipartUpload', f'{p}/f', body={'upload_id': 'u6'}),
                row(12, 'CompleteMultipartUpload', f'{p}/f', uid='u6', body={'complete_error': 'InternalError'})]
        self.assertEqual(open_uploads(rows, 2, p), [('b', f'{p}/a', 'u1'), ('b', f'{p}/f', 'u6')])


def cleanup_order(calls):
    return [c for c in calls if c in ('clear_faults', 'teardown', 'sh_stop', 'abort', 'delete_prefix')]


def measurements_problems(record):
    return measurements.problems(record)


class MeasurementsTest(unittest.TestCase):
    def test_write_refuses_invalid_record(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, 'r.json')
            with self.assertRaises(ValueError) as ctx:
                measurements.write(path, {'status': 'ok'})
            self.assertIn('schema', str(ctx.exception))
            self.assertFalse(os.path.exists(path))


if __name__ == '__main__':
    unittest.main()
