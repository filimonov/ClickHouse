import os
import subprocess
import tempfile
import threading
import unittest

from unittest import mock

from yardstick.cost import cluster as cluster_module
from yardstick.cost import localio
from yardstick.cost.cluster import Cluster

TOOL_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

STRACE_FIXTURE = """\
100 1700000000.000001 write(3</data/r1/a.bin>, "abc"..., 4096) = 4096
100 1700000000.000002 write(3</data/r1/a.bin>, "abc"..., 5000) = 5000
101 1700000000.000003 pread64(4</data/r1/b.bin>, "x"..., 8192, 0) = 8192
101 1700000000.000004 write(5<TCP:[127.0.0.1:1->127.0.0.1:2]>, "net"..., 100) = 100
101 1700000000.000005 write(1</dev/pts/3>, "log"..., 10) = 10
102 1700000000.000006 write(3</elsewhere/c.bin>, "zz"..., 512) = 512
102 1700000000.000007 write(3</data/r1/a.bin>, "bad"..., 4096) = -1 ENOSPC (No space left on device)
100 1700000000.000008 fsync(3</data/r1/a.bin>) = 0
103 1700000000.000009 write(3</data/r1/a.bin>, "ab"..., 4096 <unfinished ...>
104 1700000000.000010 read(7</data/r1/b.bin>, "", 65536) = 0
103 1700000000.000011 <... write resumed>) = 4096
105 1700000000.000012 write(3</data/r1/a.bin>, "f(x) = 7 y"..., 100) = 100
105 1700000000.000013 write(3</data/r1/a.bin>, "g(z) = 9"..., 300 <unfinished ...>
105 1700000000.000014 <... write resumed>) = 300
106 1700000000.000015 write(3</data/r1/a.bin>, "h) = 5 q"..., 64) = 64 <0.000021>
106 1700000000.000016 write(3</data/r1/a.bin>, "k"..., 1 <unfinished ...>
106 1700000000.000017 <... write resumed>, "x) = 8 ", 1) = 1
+++ exited with 0 +++
"""


class ProcIo(unittest.TestCase):
    def test_proc_io_grows(self):
        before = localio.proc_io(os.getpid())
        # tmpfs does not count write_bytes, so the file lives beside the tests.
        with tempfile.NamedTemporaryFile(dir=os.path.dirname(os.path.abspath(__file__))) as f:
            f.write(b'x' * (1 << 20))
            f.flush()
            os.fsync(f.fileno())
            after = localio.proc_io(os.getpid())
        d = localio.delta(before, after)
        self.assertEqual(set(d), set(localio.PROC_IO_FIELDS))
        self.assertGreaterEqual(d['write_bytes'], 1 << 20)
        self.assertGreaterEqual(d['wchar'], 1 << 20)


class DirSamplerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.a = os.path.join(self.tmp.name, 'a')
        self.b = os.path.join(self.tmp.name, 'b')
        os.mkdir(self.a)
        os.mkdir(self.b)

    def test_dir_sampler_union_peak_and_hardlink_dedup(self):
        samples, cond = [], threading.Condition()

        def on_sample(t_us, nbytes):
            with cond:
                samples.append(nbytes)
                cond.notify_all()

        def wait_for(pred):
            with cond:
                self.assertTrue(cond.wait_for(lambda: pred(samples[-1]) if samples else False, timeout=10))

        sampler = localio.DirSampler([self.a, self.b, os.path.join(self.tmp.name, 'absent')],
                                     interval_s=0.02, on_sample=on_sample)
        sampler.start()
        wait_for(lambda n: n == 0)
        path = os.path.join(self.a, 'f')
        with open(path, 'wb') as f:
            f.write(os.urandom(1 << 20))
        os.link(path, os.path.join(self.b, 'link'))
        size = os.stat(path).st_blocks * 512
        wait_for(lambda n: n == size)
        os.unlink(path)
        wait_for(lambda n: n == size)  # still reachable through the hardlink
        os.unlink(os.path.join(self.b, 'link'))
        wait_for(lambda n: n == 0)
        result = sampler.stop()
        self.assertEqual(result['peak_allocated_bytes'], size)
        self.assertEqual(result['paths'], [self.a, self.b, os.path.join(self.tmp.name, 'absent')])
        self.assertEqual(max(b for _, b in result['samples']), size)
        times = [t for t, _ in result['samples']]
        self.assertEqual(times, sorted(times))

    def test_dir_sampler_misses_subinterval_peak_documented(self):
        sampler = localio.DirSampler([self.a], interval_s=60)
        sampler.start()
        path = os.path.join(self.a, 'short')
        with open(path, 'wb') as f:
            f.write(b'x' * 100000)
        os.unlink(path)
        result = sampler.stop()
        self.assertEqual(result['peak_allocated_bytes'], 0)
        self.assertIn('shorter than the sampling interval', localio.DirSampler.LIMITATION)

    def test_replica_paths(self):
        for name in ('data', 'caches'):
            os.mkdir(os.path.join(self.a, name))
        local = os.path.join(self.tmp.name, 'local')
        self.assertEqual(localio.replica_paths(self.a, local),
                         [os.path.join(self.a, 'data'), os.path.join(self.a, 'caches'), local])
        self.assertEqual(localio.replica_paths(self.a), [os.path.join(self.a, 'data'), os.path.join(self.a, 'caches')])


class DeviceTest(unittest.TestCase):
    def test_device_of(self):
        with tempfile.TemporaryDirectory() as tmp:
            info = localio.device_of(tmp)
        self.assertTrue(info['device'])
        self.assertTrue(info['fs'])
        self.assertTrue(os.path.realpath(tmp).startswith(info['mountpoint'].rstrip('/')))

    def test_diskstats_parses_fixture(self):
        with tempfile.NamedTemporaryFile('w') as f:
            f.write('   8       0 sda 100 1 2000 30 400 5 6000 70 0 80 90\n 253 0 dm-0 7 0 8 9 10 0 11 12 0 13 14\n')
            f.flush()
            stats = localio.diskstats('dm-0', path=f.name)
            self.assertEqual(stats['reads_completed'], 7)
            self.assertEqual(stats['sectors_written'], 11)
            with self.assertRaises(KeyError):
                localio.diskstats('nvme9', path=f.name)


class StraceTest(unittest.TestCase):
    def parse(self, **kw):
        with tempfile.NamedTemporaryFile('w') as f:
            f.write(STRACE_FIXTURE)
            f.flush()
            return localio.parse_strace(f.name, **kw)

    def test_parse_strace_attributes_by_fd_path_and_bins_sizes(self):
        r = self.parse(path_filter=localio.path_filter(['/data/r1/']))
        self.assertEqual(r['tool'], 'strace')
        s = r['syscalls']
        # ENOSPC failure and non-file fds excluded; the `) = N` inside quoted buffers is not the return value
        self.assertEqual(s['write']['count'], 7)
        self.assertEqual(s['write']['bytes'], 4096 + 5000 + 4096 + 100 + 300 + 64 + 1)
        self.assertEqual(s['write']['size_histogram'], {'4096': 2, '8192': 1, '128': 1, '512': 1, '64': 1, '1': 1})
        self.assertEqual(s['pread64'], {'count': 1, 'bytes': 8192, 'size_histogram': {'8192': 1}})
        self.assertEqual(s['read'], {'count': 1, 'bytes': 0, 'size_histogram': {}})
        self.assertEqual(s['fsync'], {'count': 1, 'bytes': 0, 'size_histogram': {}})

    def test_parse_strace_without_filter_counts_all_absolute_fd_paths(self):
        s = self.parse()['syscalls']
        self.assertEqual(s['write']['bytes'], 4096 + 5000 + 4096 + 512 + 10 + 100 + 300 + 64 + 1)

    def test_size_bucket(self):
        self.assertEqual([localio.size_bucket(n) for n in (1, 4096, 4097, 65536)], ['1', '4096', '8192', '65536'])


class TracerTest(unittest.TestCase):
    def test_prefix(self):
        prefix = localio.Tracer('/p/pass', 2).prefix()
        self.assertEqual(prefix, 'strace -f -ttt -yy -e trace=write,pwrite64,writev,read,pread64,readv,fsync,fdatasync '
                                 '-o /p/pass/strace.r2.txt')
        with self.assertRaises(ValueError):
            localio.Tracer('/has space', 1).prefix()

    def test_preflight_trace(self):
        with tempfile.TemporaryDirectory() as tmp:
            scope = os.path.join(tmp, 'ptrace_scope')
            with open(scope, 'w') as f:
                f.write('1\n')
            self.assertEqual(localio.preflight_trace(scope_path=scope, which=lambda n: '/usr/bin/strace'), [])
            with open(scope, 'w') as f:
                f.write('2\n')
            problems = localio.preflight_trace(scope_path=scope, which=lambda n: None)
            self.assertEqual(len(problems), 2)
            self.assertIn('strace', problems[0])
            self.assertIn('ptrace_scope', problems[1])

    def test_tracer_preflight_method(self):
        self.assertIsInstance(localio.Tracer('/p', 1).preflight_trace(), list)

    def test_traced_child_pid_retries_until_child_appears(self):
        answers = [[], [], [77]]
        sleeps = []
        pid = localio.traced_child_pid(5, '/pd', read_children=lambda p: answers.pop(0), sleep=sleeps.append)
        self.assertEqual((pid, sleeps), (77, [0.05, 0.05]))

    def test_traced_child_pid_gives_up_naming_tracer_and_pass_dir(self):
        with self.assertRaises(RuntimeError) as cm:
            localio.traced_child_pid(5, '/pd', timeout_s=0, read_children=lambda p: [], sleep=lambda s: None)
        self.assertIn('tracer 5', str(cm.exception))
        self.assertIn('/pd', str(cm.exception))

    def test_traced_child_pid_rejects_several_children(self):
        with self.assertRaises(RuntimeError):
            localio.traced_child_pid(5, '/pd', read_children=lambda p: [1, 2])

    def test_traced_child_pid(self):
        parent = subprocess.Popen(['bash', '-c', 'sleep 30 & wait'])
        try:
            child = None
            for _ in range(100):
                try:
                    child = localio.traced_child_pid(parent.pid)
                    break
                except RuntimeError:
                    threading.Event().wait(0.05)
            self.assertIsNotNone(child)
            with open(f'/proc/{child}/comm') as f:
                self.assertEqual(f.read().strip(), 'sleep')
        finally:
            subprocess.run(['pkill', '-P', str(parent.pid)])
            parent.kill()
            parent.wait()


class LimitationsTest(unittest.TestCase):
    def test_limitations_list_keeper_and_pagecache(self):
        keeper = 'process_io includes embedded Keeper on replica 1'
        diskstats = 'host diskstats include RustFS, proxy and unrelated activity'
        pagecache = 'page cache not dropped (no root)'
        with tempfile.TemporaryDirectory() as tmp:
            writable = os.path.join(tmp, 'drop_caches')
            open(writable, 'w').close()
            self.assertEqual(localio.limitations(tmp, [], drop_caches_path=writable), [keeper, diskstats])
            missing = os.path.join(tmp, 'nope')
            self.assertEqual(localio.limitations(tmp, [], drop_caches_path=missing), [keeper, diskstats, pagecache])


class ClusterIntegration(unittest.TestCase):
    def test_cluster_uses_localio_proc_io(self):
        import inspect
        default = inspect.signature(Cluster.__init__).parameters['proc_io_reader'].default
        self.assertIs(default, localio.proc_io)

    def make_cluster(self, tmp, trace_prefix):
        return Cluster('/bin/true', tmp, None, 'p', 'k', None, False, {}, trace_prefix=trace_prefix)

    def test_trace_prefix_reaches_server_sh_env_for_start_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            os.makedirs(os.path.join(tmp, 'r2'))
            cluster = self.make_cluster(tmp, lambda i: f'strace -o x.r{i}')
            with mock.patch.object(cluster_module.subprocess, 'run') as run:
                cluster._run_server_sh(['start', '/bin/true', cluster.run_dir(2), '9000'])
                self.assertEqual(run.call_args.kwargs['env']['TRACE_PREFIX'], 'strace -o x.r2')
                cluster._run_server_sh(['stop', cluster.run_dir(2)])
                self.assertIsNone(run.call_args.kwargs.get('env'))
            untraced = self.make_cluster(tmp, None)
            with mock.patch.object(cluster_module.subprocess, 'run') as run:
                untraced._run_server_sh(['start', '/bin/true', untraced.run_dir(2), '9000'])
                self.assertIsNone(run.call_args.kwargs.get('env'))


class TracedPidTest(unittest.TestCase):
    def test_traced_generation_records_the_tracer_child_pid(self):
        with tempfile.TemporaryDirectory() as tmp:
            def sh(args):
                os.makedirs(args[2], exist_ok=True)
                with open(os.path.join(args[2], 'server.pid'), 'w') as f:
                    f.write('111')

            topology = mock.Mock()
            topology.tcp.return_value = 9000
            cluster = Cluster('/bin/true', tmp, topology, 'p', 'k', None, False, {}, sh_runner=sh,
                              trace_prefix='strace')
            with mock.patch.object(cluster_module, 'traced_child_pid', return_value=222) as child:
                cluster.start(2)
            child.assert_called_once_with(111, tmp)
            self.assertEqual(cluster.generations[2][0]['pid'], 222)


class ServerShTest(unittest.TestCase):
    def run_start(self, env_extra):
        with tempfile.TemporaryDirectory() as tmp:
            os.makedirs(os.path.join(tmp, 'run', 'etc'))
            open(os.path.join(tmp, 'run', 'etc', 'config.xml'), 'w').close()
            seen = os.path.join(tmp, 'seen.txt')
            fake = os.path.join(tmp, 'fake-clickhouse')
            with open(fake, 'w') as f:
                f.write(f'#!/bin/bash\necho "RIG_TRACE=${{RIG_TRACE:-unset}} argv=$*" > {seen}\n')
            os.chmod(fake, 0o755)
            env = {k: v for k, v in os.environ.items() if k != 'TRACE_PREFIX'}
            env.update(env_extra)
            proc = subprocess.run(['bash', os.path.join(TOOL_DIR, 'server.sh'), 'start', fake,
                                   os.path.join(tmp, 'run'), '39000'], env=env, capture_output=True, text=True, timeout=60)
            with open(seen) as f:
                return proc, f.read()

    def test_server_sh_honours_trace_prefix(self):
        proc, seen = self.run_start({'TRACE_PREFIX': 'env RIG_TRACE=1'})
        self.assertIn('RIG_TRACE=1 argv=server --config-file=', seen)
        self.assertNotEqual(proc.returncode, 0)  # the fake binary exits, so start reports failure

    def test_server_sh_without_trace_prefix(self):
        _, seen = self.run_start({})
        self.assertIn('RIG_TRACE=unset argv=server --config-file=', seen)


if __name__ == '__main__':
    unittest.main()
