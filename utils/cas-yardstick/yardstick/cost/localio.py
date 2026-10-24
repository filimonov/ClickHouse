"""Local I/O accounting of the cost rig: /proc counters, allocated-bytes sampling and strace attribution."""
from __future__ import annotations

import os
import re
import shutil
import stat
import threading
import time

PROC_IO_FIELDS = ('rchar', 'wchar', 'syscr', 'syscw', 'read_bytes', 'write_bytes')
DISKSTATS_FIELDS = ('reads_completed', 'reads_merged', 'sectors_read', 'ms_reading', 'writes_completed',
                    'writes_merged', 'sectors_written', 'ms_writing', 'ios_in_progress', 'ms_io', 'weighted_ms_io')
STRACE_SYSCALLS = 'write,pwrite64,writev,read,pread64,readv,fsync,fdatasync'
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
        while not self._stop.wait(self.interval_s):
            self._sample()

    def start(self) -> None:
        self._sample()  # the baseline exists before start returns, so later files are not folded into it
        self._thread = threading.Thread(target=self._run, name='dir-sampler', daemon=True)
        self._thread.start()

    def stop(self) -> dict:
        self._stop.set()
        self._thread.join()
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
    def __init__(self, pass_dir: str, replica: int):
        self.pass_dir, self.replica = pass_dir, replica

    def path(self) -> str:
        # Absolute: server.sh changes into the replica directory before strace opens its output file.
        return os.path.abspath(os.path.join(self.pass_dir, f'strace.r{self.replica}.txt'))

    def prefix(self) -> str:
        # server.sh word-splits the prefix, so a path with whitespace would be cut apart.
        if re.search(r'\s', self.path()):
            raise ValueError(f'trace path must not contain whitespace: {self.path()}')
        return f'strace -f -ttt -yy -e trace={STRACE_SYSCALLS} -o {self.path()}'

    def preflight_trace(self) -> list:
        return preflight_trace()


def size_bucket(n: int) -> str:
    return str(1 << (n - 1).bit_length())


def path_filter(prefixes: list):
    prefixes = tuple(prefixes)
    return lambda path: path.startswith(prefixes)


_CALL = re.compile(r'^(?:\d+\s+)?\d+\.\d+\s+(\w+)\((\d+)<(/[^>]*)>')
_DONE = re.compile(r'^.*\)\s+=\s+(-?\d+)(?:\s.*)?$')
_UNFINISHED = re.compile(r'^(?:(\d+)\s+)?\d+\.\d+\s+(\w+)\((\d+)<(/[^>]*)>.*<unfinished \.\.\.>\s*$')
_RESUMED = re.compile(r'^(?:(\d+)\s+)?\d+\.\d+\s+<\.\.\. (\w+) resumed>.*\)\s+=\s+(-?\d+)(?:\s.*)?$')


def parse_strace(path: str, path_filter=None) -> dict:
    """Per-syscall count, returned bytes and size histogram for fds that resolve to absolute paths.

    Sockets, pipes and ttys carry no leading `/` in the `-yy` decoration and are not counted.
    Failed calls (negative return) are skipped.
    """
    syscalls: dict = {}
    pending: dict = {}

    def record(name: str, fd_path: str, ret: int) -> None:
        if ret < 0 or (path_filter and not path_filter(fd_path)):
            return
        entry = syscalls.setdefault(name, {'count': 0, 'bytes': 0, 'size_histogram': {}})
        entry['count'] += 1
        if name not in ('fsync', 'fdatasync'):
            entry['bytes'] += ret
            if ret > 0:
                bucket = size_bucket(ret)
                entry['size_histogram'][bucket] = entry['size_histogram'].get(bucket, 0) + 1

    with open(path, errors='replace') as f:
        for line in f:
            m = _UNFINISHED.match(line)
            if m:
                pending[m.group(1)] = (m.group(2), re.sub(r' \(deleted\)$', '', m.group(4)))
                continue
            m = _RESUMED.match(line.rstrip('\n'))
            if m:
                call = pending.pop(m.group(1), None)
                if call and call[0] == m.group(2):
                    record(call[0], call[1], int(m.group(3)))
                continue
            m, done = _CALL.match(line), _DONE.match(line.rstrip('\n'))
            if m and done:
                record(m.group(1), re.sub(r' \(deleted\)$', '', m.group(3)), int(done.group(1)))
    return {'tool': 'strace', 'syscalls': syscalls}


def limitations(pass_dir: str, local_dirs, drop_caches_path: str = DROP_CACHES) -> list:
    result = ['process_io includes embedded Keeper on replica 1',
              'host diskstats include RustFS, proxy and unrelated activity']
    if not os.access(drop_caches_path, os.W_OK):
        result.append('page cache not dropped (no root)')
    return result
