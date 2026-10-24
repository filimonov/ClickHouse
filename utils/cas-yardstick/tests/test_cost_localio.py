import json
import os
import shutil
import subprocess
import tempfile
import time
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

    def test_dir_sampler_thread_failure_reraised_at_stop(self):
        calls = []

        def on_sample(t_us, nbytes):
            calls.append(t_us)
            if len(calls) == 2:  # the first sample is the baseline in start(); fail inside the thread
                raise PermissionError('scandir denied')
        sampler = localio.DirSampler([self.a], interval_s=0.01, on_sample=on_sample)
        sampler.start()
        for _ in range(500):
            if len(calls) >= 2:
                break
            time.sleep(0.01)
        with self.assertRaisesRegex(PermissionError, 'scandir denied'):
            sampler.stop()

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

    def test_window_clips_calls_and_lists_crossing_ones(self):
        # Two generations of one replica; the window is [10, 20] s. Durations come from strace -T.
        g0 = ('7 5.000000 write(3</data/a>, "s"..., 100) = 100 <0.000010>\n'           # setup: outside
              '7 9.999990 write(3</data/a>, "c"..., 200 <unfinished ...>\n'             # crosses the start
              '7 10.000100 <... write resumed>) = 200 <0.000110>\n'
              '7 12.000000 write(3</data/a>, "w"..., 300) = 300 <0.000020>\n')         # inside
        g1 = ('9 15.000000 pwrite64(4</data/b>, "w"..., 400, 0) = 400 <0.000020>\n'    # inside, after a restart
              '9 19.999999 write(4</data/b>, "e"..., 500) = 500 <0.000500>\n'          # crosses the end
              '9 25.000000 write(4</data/b>, "t"..., 600) = 600 <0.000010>\n')         # teardown: outside
        with tempfile.TemporaryDirectory() as tmp:
            paths = []
            for n, text in enumerate((g0, g1)):
                paths.append(localio.Tracer(tmp, 1, n).path())
                with open(paths[-1], 'w') as f:
                    f.write(text)
            self.assertEqual(localio.Tracer.paths(tmp, 1), paths)
            r = localio.parse_strace(paths, window=(10.0, 20.0))
        self.assertEqual(r['syscalls']['write'], {'count': 1, 'bytes': 300, 'size_histogram': {'512': 1}})
        self.assertEqual(r['syscalls']['pwrite64']['count'], 1)
        self.assertEqual([(c['syscall'], c['bytes']) for c in r['crossing']], [('write', 200), ('write', 500)])
        self.assertEqual((r['outside'], r['window'], r['files']), (2, [10.0, 20.0], paths))

    def test_size_bucket(self):
        self.assertEqual([localio.size_bucket(n) for n in (1, 4096, 4097, 65536)], ['1', '4096', '8192', '65536'])


class TracerTest(unittest.TestCase):
    def test_prefix(self):
        # A relative pass dir (a relative --runs-dir) still yields an absolute output path: server.sh changes directory.
        self.assertEqual(localio.Tracer('rel/pass', 1).path(), os.path.join(os.getcwd(), 'rel/pass/strace.r1.g0.txt'))
        # Each generation of a replica gets its own file: `strace -o` truncates, so a restart must not reuse one.
        prefix = localio.Tracer('/p/pass', 2, 3).prefix()
        self.assertEqual(prefix, 'strace -f -ttt -T -yy -e trace=write,pwrite64,writev,pwritev,pwritev2,read,pread64,readv,preadv,preadv2,fsync,fdatasync '
                                 '-o /p/pass/strace.r2.g3.txt')
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
            sampled = localio.DirSampler.LIMITATION
            self.assertEqual(localio.limitations(tmp, [], drop_caches_path=writable), [keeper, diskstats, sampled])
            missing = os.path.join(tmp, 'nope')
            self.assertEqual(localio.limitations(tmp, [], drop_caches_path=missing), [keeper, diskstats, sampled, pagecache])
            traced = localio.limitations(tmp, [], drop_caches_path=writable, traced=True)
            self.assertIn('syscall demand', traced[-1])


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
            cluster = self.make_cluster(tmp, lambda i, g: f'strace -o x.r{i}.g{g}')
            cluster.generations[2] = [{}]  # one generation already ran
            with mock.patch.object(cluster_module.subprocess, 'run') as run:
                cluster._run_server_sh(['start', '/bin/true', cluster.run_dir(2), '9000'])
                self.assertEqual(run.call_args.kwargs['env']['TRACE_PREFIX'], 'strace -o x.r2.g1')
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


class EbsDemandHelpersTest(unittest.TestCase):
    """Request-size parsing by path class and window, rate peaks and the idle baseline (EBS demand, R1)."""

    TRACE = (
        '101 100.000000 write(5</d/t/all_1_1_0/data.bin>, "x"..., 1048576) = 1048576 <0.001000>\n'
        '101 100.010000 write(5</d/t/all_1_1_0/data.bin>, "x"..., 300000) = 300000 <0.001000>\n'
        '101 100.020000 pwrite64(6</d/k/log/changelog.bin>, "x"..., 4096, 0) = 4096 <0.000100>\n'
        '102 100.030000 pread64(7</d/t/all_1_1_0/data.bin>, <unfinished ...>\n'
        '101 100.031000 fsync(5</d/t/all_1_1_0/data.bin>) = 0 <0.002000>\n'
        '102 100.040000 <... pread64 resumed>"x"..., 262144, 0) = 262144 <0.010000>\n'
        '101 100.050000 read(8</d/t/all_1_1_0/columns.txt>, "", 4096) = 0 <0.000010>\n'
        '101 100.060000 write(9<socket:[1]>, "x", 10) = 10 <0.000010>\n'
        '101 101.500000 write(5</d/t/all_2_2_0/data.bin>, "x"..., 100) = 100 <0.000010>\n'
        '101 101.999900 write(5</d/t/all_2_2_0/data.bin>, "x"..., 100) = 100 <0.001000>\n'
    )

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.path = os.path.join(self.tmp, 'strace.r1.g0.txt')
        with open(self.path, 'w') as f:
            f.write(self.TRACE)

    def classify(self, path):
        return 'table' if path.startswith('/d/t/') else 'keeper' if path.startswith('/d/k/') else None

    def test_parse_strace_by_classes_windows_and_ebs_ios(self):
        out = localio.parse_strace_by(self.path, self.classify, {'a': (100.0, 101.0), 'b': (101.0, 102.0)}, 262144)
        table = out['a']['classes']['table']
        # 1 MiB = 4 I/Os of 256 KiB, 300000 B = 2, the 256 KiB pread = 1; the zero-byte read moves nothing.
        self.assertEqual(table['write'], {'count': 2, 'bytes': 1348576, 'ebs_ios': 6,
                                          'size_histogram': {'1048576': 1, '524288': 1}})
        self.assertEqual(table['read'], {'count': 1, 'bytes': 262144, 'ebs_ios': 1, 'size_histogram': {'262144': 1}})
        self.assertEqual(table['fsync'], 1)
        self.assertEqual(out['a']['classes']['keeper']['write']['bytes'], 4096)
        # The call that ends after 102.0 straddles window b's edge.
        self.assertEqual(out['b']['classes']['table']['write']['count'], 1)
        self.assertEqual(out['b']['crossing'], 1)
        self.assertNotIn('socket', json.dumps(out))

    def test_vector_and_nowait_calls_are_traced_and_counted(self):
        # The thread-pool reader serves page-cache hits with preadv2(RWF_NOWAIT); a miss returns EAGAIN, then pread.
        self.assertTrue({'preadv2', 'preadv', 'pwritev', 'pwritev2'} <= set(localio.STRACE_SYSCALLS.split(',')))
        with open(self.path, 'w') as f:
            f.write('7 100.1 preadv2(5</d/t/p/a.bin>, [{iov_base="x"..., iov_len=1048576}], 1, 0, RWF_NOWAIT) '
                    '= 1048576 <0.000100>\n'
                    '7 100.2 preadv2(5</d/t/p/b.bin>, [{iov_base=0x1, iov_len=65536}], 1, 0, RWF_NOWAIT) '
                    '= -1 EAGAIN (Resource temporarily unavailable) <0.000010>\n'
                    '7 100.3 pwritev(6</d/t/p/c.bin>, [{iov_base="x"..., iov_len=4096}], 1, 0) = 4096 <0.000010>\n')
        out = localio.parse_strace_by(self.path, self.classify, {'a': (100.0, 101.0)}, 262144)['a']['classes']['table']
        self.assertEqual((out['read']['bytes'], out['read']['ebs_ios'], out['read']['count']), (1048576, 4, 1))
        self.assertEqual(out['write']['bytes'], 4096)

    def test_parse_strace_unchanged_by_the_shared_reader(self):
        out = localio.parse_strace(self.path, window=(100.0, 101.0))
        self.assertEqual(out['syscalls']['write']['count'], 2)  # the socket write has no absolute path
        self.assertEqual(out['syscalls']['pread64']['bytes'], 262144)

    def test_peak_factor_from_a_synthetic_series(self):
        # 1 s samples of cumulative write_bytes: 1, 1, 5, 1 MB/s; a restart (pid 8) never pairs with pid 7.
        rows = [[0, 7, 0, 0], [1_000_000, 7, 0, 1_000_000], [2_000_000, 7, 0, 2_000_000],
                [3_000_000, 7, 0, 7_000_000], [4_000_000, 7, 0, 8_000_000], [5_000_000, 8, 0, 0]]
        got = localio.peak_factor(rows, 0, 5_000_000)
        self.assertEqual((got['intervals'], got['mean_bps'], got['peak_bps']), (4, 2_000_000, 5_000_000))
        self.assertAlmostEqual(got['peak_factor'], 2.5)
        net = localio.peak_factor(rows, 0, 5_000_000, baseline_bps=1_000_000)
        self.assertAlmostEqual(net['peak_factor'], 4.0)
        with self.assertRaises(ValueError):
            localio.peak_factor(rows, 0, 500_000)
        with self.assertRaises(ValueError):
            localio.peak_factor(rows, 0, 5_000_000, baseline_bps=3_000_000)

    def test_net_io_subtracts_the_idle_baseline(self):
        got = localio.net_io({'read_bytes': 500, 'write_bytes': 10_000, 'rchar': 1}, {'write_bytes': 100.0}, 20)
        self.assertEqual(got, {'read_bytes': 500, 'write_bytes': 8000})

    def test_proc_io_sampler_follows_the_live_pid(self):
        pids = iter([7, 7, 8, 8, 8, 8, 8, 8])
        sampler = localio.ProcIoSampler(lambda: next(pids, 8), 0.01,
                                        reader=lambda pid: {'read_bytes': pid, 'write_bytes': pid * 10})
        sampler.start()
        time.sleep(0.05)
        rows = sampler.stop()
        self.assertEqual(rows[0][1:], [7, 7, 70])
        self.assertEqual(rows[-1][1:], [8, 8, 80])

