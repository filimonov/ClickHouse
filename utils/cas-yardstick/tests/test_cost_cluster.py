import os
import tempfile
import time
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

    def fake_clock(self):
        now = [0.0]
        sleeps = []

        def sleep(s):
            sleeps.append(s)
            now[0] += s
        return mock.patch.multiple(cluster_module, _monotonic=lambda: now[0], _sleep=sleep), sleeps

    def test_preflight_problem_launches_nothing(self):
        cluster = self.make()
        cluster.topology.preflight_new_endpoints = lambda: ['cannot bind 127.0.0.1:19701: busy']
        patch, sleeps = self.fake_clock()
        with patch, self.assertRaises(RuntimeError) as ctx:
            cluster.start_all()
        self.assertIn('19701', str(ctx.exception))
        self.assertIn(f'after {cluster_module.PORT_FREE_WAIT_SEC} s', str(ctx.exception))
        self.assertEqual(self.log, [])
        # Bounded: the wait never exceeds its limit.
        self.assertLessEqual(sum(sleeps), cluster_module.PORT_FREE_WAIT_SEC)

    def test_preflight_waits_for_ports_held_by_the_previous_pass(self):
        # The servers of the previous lock holder may still be shutting down at hand-over.
        cluster = self.make()
        answers = [['cannot bind 127.0.0.1:19701: busy']] * 3 + [[]]
        cluster.topology.preflight_new_endpoints = lambda: answers.pop(0)
        patch, sleeps = self.fake_clock()
        with patch:
            cluster.start_all()
        self.assertEqual(answers, [])
        self.assertEqual(len(sleeps), 3)
        self.assertIn(('sh', 'start', 'r1'), self.log)

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

    def test_stop_all_stops_a_launched_but_unregistered_replica_by_pidfile(self):
        cluster = self.make()
        cluster.start_all()
        with mock.patch.object(cluster_module, 'traced_child_pid', side_effect=RuntimeError('no child')):
            cluster.trace_prefix = 'strace'
            cluster.stop(2)
            with self.assertRaises(RuntimeError):
                cluster.start(2)  # server.sh launched it, then the pid lookup failed
        self.assertNotIn(2, cluster._running)
        self.log.clear()
        cluster.stop_all()
        self.assertIn(('sh', 'stop', 'r2'), self.log)
        self.log.clear()
        cluster.stop_all()
        self.assertEqual([e for e in self.log if e[0] == 'sh'], [])  # each server is stopped once

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


class ServerShStopIdentityTest(unittest.TestCase):
    """`server.sh stop` signals only the process it started: the comm recorded at start must still match."""

    def stop_foreign(self, comm):
        import signal
        import subprocess
        server_sh = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'server.sh')
        foreign = subprocess.Popen(['sleep', '60'], start_new_session=True)
        try:
            with tempfile.TemporaryDirectory() as tmp:
                with open(os.path.join(tmp, 'server.pid'), 'w') as f:
                    f.write(str(foreign.pid))
                with open(os.path.join(tmp, 'server.port'), 'w') as f:
                    f.write('1')  # nothing answers SYSTEM SHUTDOWN there
                if comm is not None:
                    with open(os.path.join(tmp, 'server.comm'), 'w') as f:
                        f.write(comm + '\n')
                done = subprocess.run(['bash', server_sh, 'stop', tmp], capture_output=True, text=True, timeout=60,
                                      env={**os.environ, 'SERVER_STOP_TIMEOUT': '1'})
            self.assertIsNone(foreign.poll(), 'server.sh signalled a process it did not start')
            return done
        finally:
            foreign.send_signal(signal.SIGKILL)
            foreign.wait()

    def test_reused_pid_is_not_signalled(self):
        done = self.stop_foreign('clickhouse')
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn("is now 'sleep'", done.stdout)

    def test_no_sigkill_without_a_recorded_comm(self):
        done = self.stop_foreign(None)
        self.assertEqual(done.returncode, 1)
        self.assertIn('not sending SIGKILL', done.stderr)


class ServerShTracedKillTest(unittest.TestCase):
    """`server.sh` fallbacks under tracing: `server.pid` is the tracer, the server is its child in the same group."""

    FAKE_SERVER = '''import http.server, os, signal, sys
signal.signal(signal.SIGTERM, signal.SIG_IGN)
port = int(next(a.split('=')[1] for a in sys.argv if a.startswith('--http_port=')))
with open(os.environ['FAKE_CHILD_PID'], 'w') as f:
    f.write(str(os.getpid()))
class H(http.server.BaseHTTPRequestHandler):
    def _ok(self):
        self.send_response(200); self.end_headers(); self.wfile.write(b'1')
    do_GET = do_POST = _ok
    def log_message(self, *a):
        pass
http.server.HTTPServer(('127.0.0.1', port), H).serve_forever()
'''
    # Like `strace -f -o file`: runs the command as a child and waits, so killing it alone leaves the child running.
    FAKE_TRACER = '#!/bin/bash\ntrap "" TERM\n"$@" &\nwait\n'

    def test_stop_fallback_kills_the_traced_server_too(self):
        import signal
        import socket
        import subprocess
        server_sh = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'server.sh')
        with tempfile.TemporaryDirectory() as tmp:
            os.makedirs(os.path.join(tmp, 'etc'))
            open(os.path.join(tmp, 'etc', 'config.xml'), 'w').close()
            binary, tracer = os.path.join(tmp, 'fake-clickhouse'), os.path.join(tmp, 'fake-strace')
            with open(binary, 'w') as f:
                f.write('#!/usr/bin/env python3\n' + self.FAKE_SERVER)
            with open(tracer, 'w') as f:
                f.write(self.FAKE_TRACER)
            os.chmod(binary, 0o755)
            os.chmod(tracer, 0o755)
            s = socket.socket()
            s.bind(('127.0.0.1', 0))
            http_port = s.getsockname()[1]
            s.close()
            child_pid_file = os.path.join(tmp, 'child.pid')
            env = {**os.environ, 'TRACE_PREFIX': tracer, 'FAKE_CHILD_PID': child_pid_file, 'SERVER_STOP_TIMEOUT': '1'}
            subprocess.run(['bash', server_sh, 'start', binary, tmp, str(http_port - 1)], env=env, check=True,
                           capture_output=True, timeout=60, start_new_session=True)
            with open(child_pid_file) as f:
                child = int(f.read())
            try:
                done = subprocess.run(['bash', server_sh, 'stop', tmp], env=env, capture_output=True, text=True,
                                      timeout=60)
                self.assertIn('SIGKILL', done.stderr)
                for _ in range(50):
                    try:
                        os.kill(child, 0)
                    except ProcessLookupError:
                        break
                    time.sleep(0.1)
                else:
                    self.fail(f'traced server {child} survived the stop fallback')
            finally:
                try:
                    os.kill(child, signal.SIGKILL)
                except ProcessLookupError:
                    pass

    # A tracer that dies on TERM (as strace does) and a server that ignores TERM and never answers SELECT 1.
    SILENT_SERVER = '''import os, signal, time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
with open(os.environ['FAKE_CHILD_PID'], 'w') as f:
    f.write(str(os.getpid()))
while True:
    time.sleep(1)
'''
    MORTAL_TRACER = '#!/bin/bash\n"$@" &\nwait\n'

    def test_start_timeout_kills_the_traced_server_too(self):
        import signal
        import socket
        import subprocess
        server_sh = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'server.sh')
        with tempfile.TemporaryDirectory() as tmp:
            os.makedirs(os.path.join(tmp, 'etc'))
            open(os.path.join(tmp, 'etc', 'config.xml'), 'w').close()
            binary, tracer = os.path.join(tmp, 'fake-clickhouse'), os.path.join(tmp, 'fake-strace')
            with open(binary, 'w') as f:
                f.write('#!/usr/bin/env python3\n' + self.SILENT_SERVER)
            with open(tracer, 'w') as f:
                f.write(self.MORTAL_TRACER)
            os.chmod(binary, 0o755)
            os.chmod(tracer, 0o755)
            s = socket.socket()
            s.bind(('127.0.0.1', 0))
            http_port = s.getsockname()[1]
            s.close()
            child_pid_file = os.path.join(tmp, 'child.pid')
            env = {**os.environ, 'TRACE_PREFIX': tracer, 'FAKE_CHILD_PID': child_pid_file, 'SERVER_START_TIMEOUT': '2',
                   'SERVER_TERM_GRACE': '1'}
            done = subprocess.run(['bash', server_sh, 'start', binary, tmp, str(http_port - 1)], env=env,
                                  capture_output=True, text=True, timeout=90, start_new_session=True)
            child = None
            try:
                self.assertEqual(done.returncode, 1, done.stderr)
                self.assertIn('not answering', done.stderr)
                with open(child_pid_file) as f:
                    child = int(f.read())
                # A killed orphan is a zombie until its new parent reaps it: dead, though its pid still exists.
                def running(pid):
                    try:
                        with open(f'/proc/{pid}/stat') as f:
                            return f.read().rsplit(')', 1)[1].split()[0] != 'Z'
                    except FileNotFoundError:
                        return False
                self.assertFalse(running(child), f'traced server {child} survived the start timeout')
            finally:
                if child is not None:
                    try:
                        os.kill(child, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
