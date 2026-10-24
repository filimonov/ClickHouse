"""EBS demand cards (R1): local I/O bytes and request sizes per operation on the `local` policy (variant `ebs`).

`io-sizes` runs on r1 under strace (the manifest must say `params.trace: true`). Each pass first idles for
`params.baseline_s`: that interval's `/proc/<pid>/io` rate is the baseline (embedded Keeper on r1, background log
flushes), subtracted from every phase. Then it runs `params.phases` in order (`insert`, `merge`, `select`), each in its
own window, so writes and reads are attributed to one operation. r2 runs beside r1 with no table and no Keeper: its
counters are the server-only idle control. Expected values come from `predictions/<experiment_id>.local.json`.

Measurement boundary: process counters are page-cache-side (`write_bytes` counts pages dirtied, `read_bytes` counts
cache misses), and strace sizes are syscall sizes. Device reads are not cold-read demand (no root to drop caches), so
read demand is the bytes the traced syscalls return: what the server issues, with no bound direction against a cold
device (readahead and block rounding add bytes, cached re-reads repeat them).
"""
from __future__ import annotations

import time

from ..localio import Tracer, net_io, parse_strace_by, proc_io
from .lifecycle import LifecycleCard, check_observation
from .probes import _count, compare_ledger, register

MIB = 1 << 20
PHASE_OPS = {'insert': 'inserts', 'merge': 'merges', 'select': 'reads'}
DIRECTIONS = ('write', 'read')
# Request-size buckets of `io_request_size_distribution` (registry enum `io_size_bucket`): power-of-two upper bounds.
SIZE_BUCKETS = (('<=4KiB', 4096), ('8KiB', 8192), ('16KiB', 16384), ('32KiB', 32768), ('64KiB', 65536),
                ('128KiB', 131072), ('256KiB', 262144), ('512KiB', 524288), ('1MiB', 1048576), ('>1MiB', None))
VOLUME_CLASSES = ('table', 'server')  # what an EBS volume of the server holds; the rig's Keeper is excluded


def size_bucket_label(upper: int) -> str:
    """The `io_size_bucket` of a power-of-two histogram key."""
    for label, bound in SIZE_BUCKETS:
        if bound is None or upper <= bound:
            return label
    raise AssertionError('unreachable: the last bucket is open')


def merge_classes(classes: dict, names=VOLUME_CLASSES) -> dict:
    """One direction table over several path classes."""
    out = {d: {'count': 0, 'bytes': 0, 'ebs_ios': 0, 'size_histogram': {}} for d in DIRECTIONS}
    for name in names:
        for d in DIRECTIONS:
            src = classes.get(name, {}).get(d)
            if not src:
                continue
            for k in ('count', 'bytes', 'ebs_ios'):
                out[d][k] += src[k]
            for b, n in src['size_histogram'].items():
                out[d]['size_histogram'][b] = out[d]['size_histogram'].get(b, 0) + n
    return out


def size_distribution(histogram: dict) -> dict:
    """Fraction of requests per `io_size_bucket`; every bucket present. ValueError on an empty histogram."""
    total = sum(histogram.values())
    if not total:
        raise ValueError('no requests: the distribution is undefined')
    out = dict.fromkeys((label for label, _ in SIZE_BUCKETS), 0)
    for upper, n in histogram.items():
        out[size_bucket_label(int(upper))] += n
    return {k: n / total for k, n in out.items()}


def ios_per_mib(direction: dict) -> float:
    """EBS I/Os per MiB of syscall demand; ValueError when the direction moved no bytes."""
    if not direction['bytes']:
        raise ValueError('no bytes: I/Os per MiB is undefined')
    return direction['ebs_ios'] / (direction['bytes'] / MIB)


def tv_distance(a: dict, b: dict) -> float:
    return 0.5 * sum(abs(a.get(k, 0.0) - b.get(k, 0.0)) for k in set(a) | set(b))


def phase_summary(phase: dict, baseline_bps: dict) -> dict:
    """Per-operation demand of one phase: bytes per logical byte, request sizes and EBS I/Os per MiB.

    `phase`: `wall_s`, `logical_bytes`, `proc_io` (r1 delta) and `traced` (classes of `parse_strace_by`).
    Write bytes are process `write_bytes` net of the baseline; read bytes are traced syscall reads of the volume
    classes (the page cache hides device reads).
    """
    if phase['logical_bytes'] <= 0:
        raise ValueError(f'phase {phase["name"]}: no logical bytes')
    net = net_io(phase['proc_io'], baseline_bps, phase['wall_s'])
    volume = merge_classes(phase['traced'])
    out = {'net_write_bytes': net['write_bytes'], 'net_read_bytes': net['read_bytes'],
           'io_bytes': {'write': net['write_bytes'] / phase['logical_bytes'],
                        'read': volume['read']['bytes'] / phase['logical_bytes']},
           'traced_bytes': {d: volume[d]['bytes'] for d in DIRECTIONS},
           'traced_table_bytes': {d: phase['traced'].get('table', {}).get(d, {}).get('bytes', 0) for d in DIRECTIONS},
           'requests': {d: volume[d]['count'] for d in DIRECTIONS},
           'ebs_ios': {d: volume[d]['ebs_ios'] for d in DIRECTIONS}}
    out['request_size_distribution'] = {d: size_distribution(volume[d]['size_histogram']) for d in DIRECTIONS
                                        if volume[d]['count']}
    out['ebs_ios_per_mib'] = {d: ios_per_mib(volume[d]) for d in DIRECTIONS if volume[d]['bytes']}
    out['max_request_bucket'] = {d: max((int(b) for b in volume[d]['size_histogram']), default=0) for d in DIRECTIONS}
    return out


def flat_observations(phases: dict, summaries: dict) -> dict:
    """Prediction-checkable keys `<op>.<quantity>.<direction>`."""
    out = {}
    for name, s in summaries.items():
        op = PHASE_OPS[name]
        for d in DIRECTIONS:
            out[f'{op}.io_bytes.{d}'] = s['io_bytes'][d]
            if d in s['ebs_ios_per_mib']:
                out[f'{op}.ebs_ios_per_mib.{d}'] = s['ebs_ios_per_mib'][d]
            out[f'{op}.max_request_bucket.{d}'] = s['max_request_bucket'][d]
            if d in s['request_size_distribution']:
                out[f'{op}.request_size_distribution.{d}'] = s['request_size_distribution'][d]
        out[f'{op}.logical_bytes'] = phases[name]['logical_bytes']
    return out


@register
class IoSizes(LifecycleCard):
    name = 'io-sizes'
    policies = ('local',)
    mode = 'stochastic'
    replicas = 2  # r2 is the idle control: a server without the table and without the embedded Keeper
    trace = True

    def inserts(self, ctx) -> int:
        return super().inserts(ctx) if 'insert' in self.p(ctx)['phases'] else 0

    def _pid(self, ctx, i: int) -> int:
        return ctx.cluster.generations[i][-1]['pid']

    def _io(self, ctx) -> dict:
        return {f'r{i}': proc_io(self._pid(ctx, i)) for i in range(1, self.replicas + 1)}

    def _phase(self, ctx, name: str, work, logical) -> None:
        """Run `work` inside the window; `logical()` (part-log reads, a log flush) runs after the window closes, so
        its own writes are not the operation's."""
        io0, t0 = self._io(ctx), time.time_ns() // 1000
        work()
        t1, io1 = time.time_ns() // 1000, self._io(ctx)
        self.state(ctx).setdefault('phases', {})[name] = {
            'name': name, 't0_us': t0, 't1_us': t1, 'wall_s': (t1 - t0) / 1e6, 'logical_bytes': logical(),
            'proc_io': {r: {k: io1[r][k] - io0[r][k] for k in io1[r]} for r in io1}}

    def _compressed_bytes(self, ctx) -> int:
        return _count(self.replica_client(ctx), f'SELECT sum(data_compressed_bytes) FROM system.parts '
                                                f'WHERE {self.where()} AND active')

    def _part_log_bytes(self, ctx, event: str) -> int:
        self.replica_client(ctx).query('SYSTEM FLUSH LOGS')
        return _count(self.replica_client(ctx), f"SELECT sum(size_in_bytes) FROM system.part_log WHERE {self.where()} "
                                                f"AND event_type = '{event}' AND error = 0")

    def measured(self, ctx):
        p = self.p(ctx)
        if p.get('trace') is not True:
            raise ValueError(f'{self.name}: the manifest must set params.trace = true (request sizes come from strace)')
        client = self.replica_client(ctx)
        # The idle interval is the measurement of the baseline, not a wait for a condition.
        self._phase(ctx, 'baseline', lambda: time.sleep(p['baseline_s']), lambda: 0)
        n = self.inserts(ctx)
        for name in p['phases']:
            if name == 'insert':
                def work():
                    for i in range(n):
                        self.insert(ctx, i)
                self._phase(ctx, name, work, lambda: self._part_log_bytes(ctx, 'NewPart'))
            elif name == 'merge':
                before = self._part_log_bytes(ctx, 'MergeParts')
                # OPTIMIZE FINAL returns after the merge.
                self._phase(ctx, name, lambda: client.query(self.sql(ctx, p['optimize'])),
                            lambda: self._part_log_bytes(ctx, 'MergeParts') - before)
            elif name == 'select':
                scanned = self._compressed_bytes(ctx)
                self._phase(ctx, name, lambda: client.query(self.sql(ctx, p['select']),
                                                             settings=ctx.manifest['settings']['query']),
                            lambda: scanned)
            else:
                raise ValueError(f'{self.name}: unknown phase {name!r}')
        return {'unit_count': max(n, 1), 'effective_counts': {'inserts': n, 'rows': n * p['rows']}, 'observations': {}}

    def path_class(self, ctx):
        """`table` (the local disk), `keeper` (excluded) or `server` (r1's own files); None for anything off the
        volume: `/proc`, `/sys`, `/etc`, the binary."""
        table = ctx.local_dirs[0].rstrip('/') + '/'
        run = ctx.cluster.run_dir(1).rstrip('/') + '/'
        keeper = run + 'keeper/'

        def cls(path):
            if path.startswith(table):
                return 'table'
            if path.startswith(keeper):
                return 'keeper'
            return 'server' if path.startswith(run) else None
        return cls

    def observe(self, ctx, draft):
        p, phases = self.p(ctx), self.state(ctx)['phases']
        windows = {name: (ph['t0_us'] / 1e6, ph['t1_us'] / 1e6) for name, ph in phases.items()}
        traced = parse_strace_by(Tracer.paths(ctx.pass_dir, 1), self.path_class(ctx), windows, p['ebs_max_io_bytes'])
        base = phases['baseline']
        rates = {r: {k: base['proc_io'][r][k] / base['wall_s'] for k in ('read_bytes', 'write_bytes')}
                 for r in base['proc_io']}
        keeper = traced['baseline']['classes'].get('keeper', {}).get('write', {}).get('bytes', 0)
        out = {'baseline': {'wall_s': base['wall_s'], 'bps': rates,
                            'traced_keeper_write_bps': keeper / base['wall_s'],
                            'r1_minus_r2_write_bps': rates['r1']['write_bytes'] - rates['r2']['write_bytes']},
               'phases': {}, 'crossing': {w: t['crossing'] for w, t in traced.items()}}
        summaries = {}
        for name, ph in phases.items():
            entry = {**{k: ph[k] for k in ('t0_us', 't1_us', 'wall_s', 'logical_bytes', 'proc_io')},
                     'traced': traced[name]['classes']}
            if name != 'baseline':
                summaries[name] = phase_summary({**ph, 'proc_io': ph['proc_io']['r1'], 'traced': traced[name]['classes']},
                                                rates['r1'])
                entry['summary'] = summaries[name]
            out['phases'][name] = entry
        out.update(flat_observations(phases, summaries))
        out['baseline.write_bps.r1'] = rates['r1']['write_bytes']
        out['baseline.write_bps.r2'] = rates['r2']['write_bytes']
        return out

    def assertions(self, ctx, draft):
        pred = self.predicted(ctx)
        out = compare_ledger(pred, draft['ledger']['foreground'], False)
        for key, want in pred['expected']['observations'].items():
            out[f'observation.{key}'] = check_observation(want, draft['observations'].get(key), draft['observations'])
        rows = self.inserts(ctx) * self.p(ctx)['rows']
        out['rows.' + self.p(ctx)['replica']] = (rows, self.state(ctx).get('rows', [None])[0])
        out['trace.files'] = (True, bool(Tracer.paths(ctx.pass_dir, 1)))
        for name in self.p(ctx)['phases']:
            got = draft['observations']['phases'][name]['summary']
            out[f'{name}.traced_bytes>0'] = (True, sum(got['traced_bytes'].values()) > 0)
        return out
