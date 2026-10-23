import os
import tempfile
import unittest

from unittest import mock

from yardstick.cost import cluster as cluster_module
from yardstick.cost.cluster import Cluster, GenerationCrashed
from yardstick.cost.topology import Topology


class FakeClient:
    def __init__(self, log, port):
        self.log, self.port = log, port
        self.counter = {'Q': 0}

    def query(self, sql, settings=None, timeout=3600):
        self.log.append(('query', self.port, sql))
        return '1\n'

    def events(self):
        return dict(self.counter)


class FakeRelay:
    fail_start = False

    def __init__(self, log, listen, target, log_path):
        self.log, self.listen, self.target = log, listen, target

    def start(self):
        self.log.append(('relay_start', self.listen, self.target))
        if self.fail_start:
            raise OSError('bind failed')

    def stop(self):
        self.log.append(('relay_stop', self.listen))


class ClusterFixture(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.pass_dir = tmp.name
        self.log = []
        self.fail_start_of = None
        self.io = {}
        self.topology = Topology(port_base=19700, replicas=2)
        self.topology.preflight_new_endpoints = lambda: []  # binding real ports would make the test host-dependent

    def sh(self, args):
        self.log.append(('sh', args[0], os.path.basename(args[2 if args[0] == 'start' else 1].rstrip('/'))))
        run_dir = args[2] if args[0] == 'start' else args[1]
        if args[0] == 'start':
            if args[2].endswith(f'/{self.fail_start_of}'):
                raise RuntimeError('server.sh start failed')
            with open(os.path.join(run_dir, 'server.pid'), 'w') as f:
                f.write(str(1000 + int(run_dir[-1]) + len([e for e in self.log if e[:2] == ('sh', 'start')])))

    def writer(self, replica_dir, replica, *rest):
        self.log.append(('config', replica))
        os.makedirs(replica_dir, exist_ok=True)
        return 'hash'

    def make(self, **kw):
        return Cluster('/bin/ch', self.pass_dir, self.topology, 's3', 'k', None, False, {},
                       sh_runner=self.sh, config_writer=self.writer,
                       client_factory=lambda port: FakeClient(self.log, port),
                       relay_factory=lambda *a: FakeRelay(self.log, *a),
                       proc_io_reader=lambda pid: dict(self.io.get(pid, {'rchar': 0})), **kw)


class ClusterTest(ClusterFixture):
    def test_start_order_and_readiness(self):
        cluster = self.make()
        cluster.start_all()
        zk = ('query', 19701, "SELECT count() FROM system.zookeeper WHERE path = '/'")
        self.assertEqual(self.log, [
            ('config', 1), ('config', 2),
            ('sh', 'start', 'r1'), zk,
            ('sh', 'start', 'r2'),
            ('relay_start', ('127.0.1.1', 19702), ('127.0.0.1', 19702)),
            ('relay_start', ('127.0.1.2', 19712), ('127.0.0.1', 19712)),
        ])
        self.assertEqual(len(cluster.clients), 2)
        self.assertEqual(len(cluster.relays), 2)
        self.assertEqual(len(cluster.startup_seconds), 2)
        self.assertEqual([len(cluster.generations[i]) for i in (1, 2)], [1, 1])

    def test_partial_failure_cleanup(self):
        self.fail_start_of = 'r2'
        cluster = self.make()
        with self.assertRaises(RuntimeError) as ctx:
            cluster.start_all()
        message = str(ctx.exception)
        self.assertIn('replica 2', message)
        self.assertIn(os.path.join(self.pass_dir, 'r2', 'server.err.log'), message)
        self.assertIn(('sh', 'stop', 'r1'), self.log)
        self.assertFalse([e for e in self.log if e[0] == 'relay_start'])
        self.assertIn(('sh', 'stop', 'r2'), self.log)
        self.assertLess(self.log.index(('sh', 'stop', 'r2')), self.log.index(('sh', 'stop', 'r1')))
        self.log.clear()
        cluster.stop_all()
        self.assertEqual(self.log, [])

    def test_relay_failure_stops_relays_and_servers(self):
        FakeRelay.fail_start = True
        self.addCleanup(setattr, FakeRelay, 'fail_start', False)
        cluster = self.make()
        with self.assertRaises(RuntimeError) as ctx:
            cluster.start_all()
        self.assertIn('relay', str(ctx.exception))
        stopped = [e for e in self.log if e[0] in ('relay_stop', 'sh') and e[1] != 'start']
        self.assertEqual(stopped.count(('sh', 'stop', 'r1')), 1)
        self.assertEqual(stopped.count(('sh', 'stop', 'r2')), 1)
        self.assertTrue([e for e in stopped if e[0] == 'relay_stop'])

    def test_preflight_problem_launches_nothing(self):
        cluster = self.make()
        cluster.topology.preflight_new_endpoints = lambda: ['cannot bind 127.0.0.1:19701: busy']
        with self.assertRaises(RuntimeError) as ctx:
            cluster.start_all()
        self.assertIn('19701', str(ctx.exception))
        self.assertEqual(self.log, [])

    def test_generation_accumulation_across_restart(self):
        cluster = self.make()
        cluster.start_all()
        pid0 = cluster.generations[2][0]['pid']
        cluster.clients[1].counter = {'Q': 100}
        self.io[pid0] = {'rchar': 1000}
        baseline = cluster.snapshot_counters(2)
        self.assertEqual(baseline['events'], {'Q': 100})
        cluster.clients[1].counter = {'Q': 150}
        self.io[pid0] = {'rchar': 1500}
        cluster.stop(2)
        self.assertIsNotNone(cluster.generations[2][0]['t_end_us'])
        seconds = cluster.start(2)
        self.assertGreaterEqual(seconds, 0)
        self.assertEqual(len(cluster.generations[2]), 2)
        pid1 = cluster.generations[2][1]['pid']
        self.assertNotEqual(pid0, pid1)
        cluster.clients[1].counter = {'Q': 30, 'New': 5}
        self.io[pid1] = {'rchar': 300}
        since = cluster.counters_since(2, baseline)
        self.assertEqual(since['events'], {'Q': 80, 'New': 5})
        self.assertEqual(since['proc_io'], {'rchar': 800})

    def test_crashed_generation_raises_named_error(self):
        cluster = self.make()
        cluster.start_all()
        baseline = cluster.snapshot_counters(2)
        dead_pid = cluster.generations[2][0]['pid']
        real_read = cluster._read

        def read(i, generation):
            if generation['pid'] == dead_pid:
                raise FileNotFoundError(f'/proc/{dead_pid}/io')
            return real_read(i, generation)

        cluster._read = read
        self.log.clear()
        cluster.stop(2)
        self.assertIn(('sh', 'stop', 'r2'), self.log)
        generation = cluster.generations[2][0]
        self.assertTrue(generation['crashed'])
        self.assertIsNone(generation['closed'])
        self.assertIn('FileNotFoundError', generation['crash_error'])
        cluster.start(2)
        with self.assertRaises(GenerationCrashed) as ctx:
            cluster.counters_since(2, baseline)
        self.assertEqual((ctx.exception.replica, ctx.exception.pid), (2, dead_pid))
        # A baseline taken after the crash only spans the new generation.
        later = cluster.snapshot_counters(2)
        self.assertEqual(cluster.counters_since(2, later), {'events': {'Q': 0}, 'proc_io': {'rchar': 0}})

    def test_keeper_readiness_retries_then_succeeds(self):
        calls = []

        class Flaky(FakeClient):
            def query(self, sql, settings=None, timeout=3600):
                calls.append(sql)
                if len(calls) <= 2:
                    raise RuntimeError('Keeper session not ready')
                return '1\n'

        self.topology = Topology(port_base=19700, replicas=1)
        self.topology.preflight_new_endpoints = lambda: []
        cluster = self.make()
        cluster._client_factory = lambda port: Flaky(self.log, port)
        with mock.patch.object(cluster_module, 'KEEPER_POLL_SEC', 0.001):
            cluster.start_all()
        self.assertEqual(len(calls), 3)

    def test_keeper_never_ready_fails_and_cleans_up(self):
        class Dead(FakeClient):
            def query(self, sql, settings=None, timeout=3600):
                raise RuntimeError('no session')

        cluster = self.make()
        cluster._client_factory = lambda port: Dead(self.log, port)
        with mock.patch.object(cluster_module, 'KEEPER_POLL_SEC', 0.001), \
                mock.patch.object(cluster_module, 'KEEPER_READY_TIMEOUT_SEC', 0.05):
            with self.assertRaisesRegex(RuntimeError, r'replica 1 failed to start: Keeper not ready after 0\.05 s'):
                cluster.start_all()
        self.assertIn(('sh', 'stop', 'r1'), self.log)
        self.assertNotIn(('sh', 'start', 'r2'), self.log)

    def test_stop_all_order(self):
        cluster = self.make()
        cluster.start_all()
        self.log.clear()
        cluster.stop_all()
        self.assertEqual([e for e in self.log if e[0] != 'query'], [
            ('sh', 'stop', 'r2'), ('sh', 'stop', 'r1'),
            ('relay_stop', ('127.0.1.1', 19702)), ('relay_stop', ('127.0.1.2', 19712))])


if __name__ == '__main__':
    unittest.main()
