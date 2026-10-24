"""Local I/O accounting of the cost rig: /proc counters, allocated-bytes sampling and strace attribution."""
from __future__ import annotations

import glob
import os
import re
import shutil
import stat
import threading
import time

PROC_IO_FIELDS = ('rchar', 'wchar', 'syscr', 'syscw', 'read_bytes', 'write_bytes')
DISKSTATS_FIELDS = ('reads_completed', 'reads_merged', 'sectors_read', 'ms_reading', 'writes_completed',
                    'writes_merged', 'sectors_written', 'ms_writing', 'ios_in_progress', 'ms_io', 'weighted_ms_io')
# preadv2 carries the thread-pool reader's page-cache hits (RWF_NOWAIT); without it a warm scan looks like no reads.
STRACE_SYSCALLS = 'write,pwrite64,writev,pwritev,pwritev2,read,pread64,readv,preadv,preadv2,fsync,fdatasync'
REPLICA_SUBDIRS = ('data', 's3_meta', 'caches', 'cas_scratch')
PTRACE_SCOPE = '/proc/sys/kernel/yama/ptrace_scope'
DROP_CACHES = '/proc/sys/vm/drop_caches'


def proc_io(pid: int) -> dict:
    with open(f'/proc/{pid}/io') as f:
        values = {name: int(value) for name, value in (line.split(': ') for line in f.read().splitlines())}
    return {name: values[name] for name in PROC_IO_FIELDS}


def delta(a: dict, b: dict) -> dict:
    return {key: b[key] - a[key] for key in b}


def _now_us() -> int:
    return time.time_ns() // 1000


class DirSampler:
    """Samples allocated bytes (`st_blocks` x 512) over the union of `paths`, counting a hardlinked inode once."""

    LIMITATION = ('directory peak is sampled: a file that lives shorter than the sampling interval '
                  'can be missed entirely')

    def __init__(self, paths: list, interval_s: float = 0.5, on_sample=None):
        self.paths = list(paths)
        self.interval_s = interval_s
        self._on_sample = on_sample
        self._samples: list = []
        self._stop = threading.Event()
        self._thread = None
        self._error: BaseException | None = None

    def _measure(self) -> int:
        seen, total, stack = set(), 0, list(self.paths)
        while stack:
            current = stack.pop()
            try:
                with os.scandir(current) as it:
                    entries = list(it)
            except (FileNotFoundError, NotADirectoryError):
                continue  # paths appear and vanish while the server runs
            for entry in entries:
                try:
                    st = entry.stat(follow_symlinks=False)
                except FileNotFoundError:
                    continue
                if stat.S_ISDIR(st.st_mode):
                    stack.append(entry.path)
                elif stat.S_ISREG(st.st_mode) and (st.st_dev, st.st_ino) not in seen:
                    seen.add((st.st_dev, st.st_ino))
                    total += st.st_blocks * 512
        return total

    def _sample(self) -> None:
        t_us, nbytes = _now_us(), self._measure()
        self._samples.append([t_us, nbytes])
        if self._on_sample:
            self._on_sample(t_us, nbytes)

    def _run(self) -> None:
        try:
            while not self._stop.wait(self.interval_s):
                self._sample()
        except BaseException as e:  # noqa: BLE001 (handed to stop(): a dead sampler must not pass for a quiet disk)
            self._error = e

    def start(self) -> None:
        self._sample()  # the baseline exists before start returns, so later files are not folded into it
        self._thread = threading.Thread(target=self._run, name='dir-sampler', daemon=True)
        self._thread.start()

    def stop(self) -> dict:
        self._stop.set()
        self._thread.join()
        if self._error is not None:
            raise self._error
        self._sample()
        return {'paths': self.paths, 'peak_allocated_bytes': max(b for _, b in self._samples),
                'samples': self._samples}


def replica_paths(replica_dir: str, local_dir: str | None = None) -> list:
    paths = [os.path.join(replica_dir, d) for d in REPLICA_SUBDIRS if os.path.isdir(os.path.join(replica_dir, d))]
    return paths + ([local_dir] if local_dir else [])


def device_of(path: str, mountinfo: str = '/proc/self/mountinfo') -> dict:
    """Block device (kernel name), filesystem type and mountpoint holding `path`."""
    real = os.path.realpath(path)
    st_dev = os.stat(real).st_dev
    want = f'{os.major(st_dev)}:{os.minor(st_dev)}'
    best = None
    with open(mountinfo) as f:
        for line in f:
            head, tail = line.rstrip('\n').split(' - ')
            fields, rest = head.split(' '), tail.split(' ')
            mountpoint = fields[4].replace('\\040', ' ')
            if fields[2] != want or not (real == mountpoint or real.startswith(mountpoint.rstrip('/') + '/')):
                continue
            if best is None or len(mountpoint) > len(best[0]):
                best = (mountpoint, rest[0], rest[1])
    if best is None:
        raise RuntimeError(f'no mount found for {path} (device {want})')
    sys_link = f'/sys/dev/block/{want}'
    device = os.path.basename(os.path.realpath(sys_link)) if os.path.exists(sys_link) else best[2]
    return {'device': device, 'fs': best[1], 'mountpoint': best[0]}


def diskstats(device: str, path: str = '/proc/diskstats') -> dict:
    with open(path) as f:
        for line in f:
            fields = line.split()
            if fields[2] == device:
                return {name: int(v) for name, v in zip(DISKSTATS_FIELDS, fields[3:])}
    raise KeyError(f'{device} not in {path}')


def _read_children(pid: int) -> list:
    children = []
    for task in os.listdir(f'/proc/{pid}/task'):
        with open(f'/proc/{pid}/task/{task}/children') as f:
            children += [int(c) for c in f.read().split()]
    return children


def traced_child_pid(pid: int, pass_dir: str = '', timeout_s: float = 5, step_s: float = 0.05,
                     read_children=_read_children, sleep=time.sleep) -> int:
    """The single direct child of `pid`: the server under strace, whose own pid is the tracer's.

    The child appears a moment after launch, so an empty list is retried until `timeout_s`.
    """
    deadline = time.monotonic() + timeout_s
    while True:
        children = read_children(pid)
        if len(children) == 1:
            return children[0]
        if len(children) > 1:
            raise RuntimeError(f'expected one traced child of tracer {pid}, found {children}')
        if time.monotonic() >= deadline:
            raise RuntimeError(f'tracer {pid} (pass dir {pass_dir}) has no child after {timeout_s} s')
        sleep(step_s)


def preflight_trace(scope_path: str = PTRACE_SCOPE, which=shutil.which) -> list:
    problems = []
    if not which('strace'):
        problems.append('strace not found in PATH')
    try:
        with open(scope_path) as f:
            scope = int(f.read())
        if scope > 1:
            problems.append(f'ptrace_scope is {scope}; tracing own children needs <= 1')
    except FileNotFoundError:
        pass  # no Yama: ptrace of own children is unrestricted
    return problems


class Tracer:
    """One trace file per server generation: `strace -o` truncates, so a restart must not reuse the file."""

    def __init__(self, pass_dir: str, replica: int, generation: int = 0):
        self.pass_dir, self.replica, self.generation = pass_dir, replica, generation

    def path(self) -> str:
        # Absolute: server.sh changes into the replica directory before strace opens its output file.
        return os.path.abspath(os.path.join(self.pass_dir, f'strace.r{self.replica}.g{self.generation}.txt'))

    @staticmethod
    def paths(pass_dir: str, replica: int) -> list:
        """Every generation's trace of `replica`, in generation order."""
        found = glob.glob(os.path.join(os.path.abspath(pass_dir), f'strace.r{replica}.g*.txt'))
        return sorted(found, key=lambda p: int(re.search(r'\.g(\d+)\.txt$', p).group(1)))

    def prefix(self) -> str:
        # server.sh word-splits the prefix, so a path with whitespace would be cut apart.
        if re.search(r'\s', self.path()):
            raise ValueError(f'trace path must not contain whitespace: {self.path()}')
        # -T gives each call its duration, so a call that straddles a window edge can be told apart.
        return f'strace -f -ttt -T -yy -e trace={STRACE_SYSCALLS} -o {self.path()}'

    def preflight_trace(self) -> list:
        return preflight_trace()


def size_bucket(n: int) -> str:
    return str(1 << (n - 1).bit_length())


def path_filter(prefixes: list):
    prefixes = tuple(prefixes)
    return lambda path: path.startswith(prefixes)


_CALL = re.compile(r'^(?:(\d+)\s+)?(\d+\.\d+)\s+(\w+)\((\d+)<(/[^>]*)>')
_DONE = re.compile(r'^.*\)\s+=\s+(-?\d+)(?:\s.*)?$')
_UNFINISHED = re.compile(r'^(?:(\d+)\s+)?(\d+\.\d+)\s+(\w+)\((\d+)<(/[^>]*)>.*<unfinished \.\.\.>\s*$')
_RESUMED = re.compile(r'^(?:(\d+)\s+)?(\d+\.\d+)\s+<\.\.\. (\w+) resumed>.*\)\s+=\s+(-?\d+)(?:\s.*)?$')
_DURATION = re.compile(r'<(\d+\.\d+)>\s*$')


def _strace_calls(files):
    """`(syscall, path, ret, pid, start, end)` of every finished call on an fd that resolves to an absolute path.

    Unfinished calls are joined with their `resumed` line; pids do not carry over between generations. A call's end
    is its start plus the `-T` duration.
    """
    pending: dict = {}

    def duration(line):
        d = _DURATION.search(line)
        return float(d.group(1)) if d else 0.0

    for path in files:
        pending.clear()
        with open(path, errors='replace') as f:
            for raw in f:
                line = raw.rstrip('\n')
                m = _UNFINISHED.match(line)
                if m:
                    pending[m.group(1)] = (m.group(3), re.sub(r' \(deleted\)$', '', m.group(5)), float(m.group(2)))
                    continue
                m = _RESUMED.match(line)
                if m:
                    call = pending.pop(m.group(1), None)
                    if call and call[0] == m.group(3):
                        end = call[2] + duration(line) if _DURATION.search(line) else float(m.group(2))
                        yield call[0], call[1], int(m.group(4)), m.group(1), call[2], end
                    continue
                m, done = _CALL.match(line), _DONE.match(line)
                if m and done:
                    start = float(m.group(2))
                    yield (m.group(3), re.sub(r' \(deleted\)$', '', m.group(5)), int(done.group(1)), m.group(1), start,
                           start + duration(line))


def parse_strace(paths, path_filter=None, window=None) -> dict:
    """Per-syscall count, returned bytes and size histogram for fds that resolve to absolute paths.

    `paths` is one trace file or a list (one per server generation). Sockets, pipes and ttys carry no leading `/`
    in the `-yy` decoration and are not counted. Failed calls (negative return) are skipped. With `window`
    `(t0_s, t1_s)`, only calls that start and end inside it count; a call that straddles an edge is listed in
    `crossing`, and the rest are counted in `outside`. A call's end is its start plus the `-T` duration.
    """
    files = [paths] if isinstance(paths, str) else list(paths)
    syscalls: dict = {}
    crossing: list = []
    outside = 0
    for name, fd_path, ret, pid, start, end in _strace_calls(files):
        if ret < 0 or (path_filter and not path_filter(fd_path)):
            continue
        if window is not None:
            t0, t1 = window
            if start >= t0 and end <= t1:
                pass
            elif start <= t1 and end >= t0:
                crossing.append({'pid': pid or '', 'syscall': name, 'path': fd_path, 'start': start, 'end': end,
                                 'bytes': ret if name not in ('fsync', 'fdatasync') else 0})
                continue
            else:
                outside += 1
                continue
        entry = syscalls.setdefault(name, {'count': 0, 'bytes': 0, 'size_histogram': {}})
        entry['count'] += 1
        if name not in ('fsync', 'fdatasync'):
            entry['bytes'] += ret
            if ret > 0:
                bucket = size_bucket(ret)
                entry['size_histogram'][bucket] = entry['size_histogram'].get(bucket, 0) + 1
    out = {'tool': 'strace', 'syscalls': syscalls}
    if window is not None:
        out.update(window=list(window), crossing=crossing, outside=outside, files=files)
    return out


READ_SYSCALLS = ('read', 'pread64', 'readv', 'preadv', 'preadv2')
WRITE_SYSCALLS = ('write', 'pwrite64', 'writev', 'pwritev', 'pwritev2')


def ebs_ios(size: int, max_io_bytes: int) -> int:
    """I/O operations one request of `size` bytes counts under a provider rule of at most `max_io_bytes` per I/O."""
    return -(-size // max_io_bytes)


def parse_strace_by(paths, classify, windows: dict, max_io_bytes: int) -> dict:
    """One pass over the traces: per window, per path class and per direction, the calls inside the window.

    `classify(path)` names the class of a path (None drops it). `windows` maps a name to `(t0_s, t1_s)`; a call
    counts in a window when it starts and ends inside it, and a call that straddles a window edge is counted in
    that window's `crossing`. Per direction: `count`, `bytes`, `ebs_ios` (each request counted on its own under
    `max_io_bytes`, no merge credit) and `size_histogram` (power-of-two upper bounds); fsync calls are counted
    separately, with no bytes.
    """
    files = [paths] if isinstance(paths, str) else list(paths)

    def empty():
        return {'count': 0, 'bytes': 0, 'ebs_ios': 0, 'size_histogram': {}}

    out = {w: {'classes': {}, 'crossing': 0} for w in windows}
    for name, fd_path, ret, _pid, start, end in _strace_calls(files):
        if ret < 0:
            continue
        cls = classify(fd_path)
        if cls is None:
            continue
        for w, (t0, t1) in windows.items():
            if not (start >= t0 and end <= t1):
                if start <= t1 and end >= t0:
                    out[w]['crossing'] += 1
                continue
            entry = out[w]['classes'].setdefault(cls, {'write': empty(), 'read': empty(), 'fsync': 0})
            if name in ('fsync', 'fdatasync'):
                entry['fsync'] += 1
                continue
            if ret == 0:
                continue  # end of file: no bytes move
            d = entry['write' if name in WRITE_SYSCALLS else 'read']
            d['count'] += 1
            d['bytes'] += ret
            d['ebs_ios'] += ebs_ios(ret, max_io_bytes)
            bucket = size_bucket(ret)
            d['size_histogram'][bucket] = d['size_histogram'].get(bucket, 0) + 1
    return out


class ProcIoSampler:
    """`/proc/<pid>/io` every `interval_s`: `[t_us, pid, read_bytes, write_bytes]` rows for rate peaks.

    `pid_of()` is asked on every tick, so a restarted server is followed; rows of different pids never pair.
    """

    def __init__(self, pid_of, interval_s: float = 1.0, reader=proc_io):
        self.pid_of, self.interval_s, self.reader = pid_of, interval_s, reader
        self._rows: list = []
        self._stop = threading.Event()
        self._thread = None
        self._error: BaseException | None = None

    def _sample(self) -> None:
        pid = self.pid_of()
        try:
            io = self.reader(pid)
        except (FileNotFoundError, ProcessLookupError):
            return  # the process is between generations; the next tick sees the new pid
        self._rows.append([_now_us(), pid, io['read_bytes'], io['write_bytes']])

    def _run(self) -> None:
        try:
            while not self._stop.wait(self.interval_s):
                self._sample()
        except BaseException as e:  # noqa: BLE001 (handed to stop(): a dead sampler must not pass for an idle server)
            self._error = e

    def start(self) -> None:
        self._sample()
        self._thread = threading.Thread(target=self._run, name='proc-io-sampler', daemon=True)
        self._thread.start()

    def stop(self) -> list:
        self._stop.set()
        self._thread.join()
        if self._error is not None:
            raise self._error
        self._sample()
        return self._rows


def rate_intervals(rows: list, t0_us: int, t1_us: int, field: int = 3) -> list:
    """`(seconds, bytes)` between consecutive samples of one pid that both lie in `[t0_us, t1_us]`."""
    inside = [r for r in rows if t0_us <= r[0] <= t1_us]
    return [((b[0] - a[0]) / 1e6, b[field] - a[field]) for a, b in zip(inside, inside[1:]) if a[1] == b[1] and b[0] > a[0]]


def peak_factor(rows: list, t0_us: int, t1_us: int, baseline_bps: float = 0.0) -> dict:
    """Peak over mean of the `write_bytes` rate in `[t0_us, t1_us]`, both net of a constant `baseline_bps`.

    The peak is the largest single-interval rate (one sampling interval, about 1 s); the mean is total bytes over
    total time. ValueError when the window has no interval or no net write.
    """
    intervals = rate_intervals(rows, t0_us, t1_us)
    if not intervals:
        raise ValueError(f'no sampling interval inside [{t0_us}, {t1_us}]')
    seconds = sum(s for s, _ in intervals)
    mean = sum(b for _, b in intervals) / seconds - baseline_bps
    if mean <= 0:
        raise ValueError(f'no net write in [{t0_us}, {t1_us}]: mean {mean} B/s after a {baseline_bps} B/s baseline')
    peak = max(b / s for s, b in intervals) - baseline_bps
    return {'peak_bps': peak, 'mean_bps': mean, 'peak_factor': peak / mean, 'intervals': len(intervals),
            'seconds': seconds}


def net_io(delta_io: dict, baseline_bps: dict, wall_s: float) -> dict:
    """Process I/O of a phase minus what the idle baseline (embedded Keeper, background flushes) does in `wall_s`."""
    return {k: delta_io[k] - baseline_bps.get(k, 0.0) * wall_s for k in ('read_bytes', 'write_bytes')}


def limitations(pass_dir: str, local_dirs, drop_caches_path: str = DROP_CACHES, traced: bool = False) -> list:
    result = ['process_io includes embedded Keeper on replica 1',
              'host diskstats include RustFS, proxy and unrelated activity', DirSampler.LIMITATION]
    if not os.access(drop_caches_path, os.W_OK):
        result.append('page cache not dropped (no root)')
    if traced:
        result.append('IOPS and MiB/s are syscall demand averaged over the window; page cache and writeback sit '
                      'between them and the device, so they are not device peaks')
    return result
