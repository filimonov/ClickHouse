"""Runs one (policy, card, case, repeat) pass of the cost rig and returns its `measurement/1` record."""
from __future__ import annotations

import os
import time
import traceback
from dataclasses import dataclass

from ..config import write_cost_config
from . import ledger as ledger_mod
from .cards.base import CardContext, check_replicas, run_measured
from .cluster import Cluster, GenerationCrashed
from .ledger import Inventory
from .localio import DirSampler, Tracer, device_of, limitations, parse_strace, replica_paths

VARIANTS = {'local': 'ebs', 's3': 's3', 's3plain': 's3plain', 'cas': 'cas'}
SAMPLE_INTERVAL_S = 0.5
CLOSING_OPS = ('CompleteMultipartUpload', 'AbortMultipartUpload')


@dataclass
class Deps:
    binary: str
    run_id: str
    runs_dir: str
    topology: object
    proxy: object
    rig: object
    store_id: str
    scale: float
    keep_prefix: bool
    binary_sha256: str
    commit: str


class CaseFailure(Exception):
    """A pass that ends FAILED with a stable `reason` instead of an exception description."""

    def __init__(self, reason: str, text: str):
        super().__init__(text)
        self.reason = reason


def variant_of(policy: str) -> tuple:
    return VARIANTS[policy.removesuffix('_cached')], policy.endswith('_cached')


def _now_us() -> int:
    return time.time_ns() // 1000


def open_uploads(rows: list, after_seq: int, key_prefix: str) -> list:
    """`(bucket, key, upload_id)` of multipart uploads a workload caller created and never completed or aborted."""
    created, closed = {}, set()
    for a in rows:
        if a.get('kind') != 'attempt' or a['seq'] <= after_seq or a['caller'] not in ledger_mod.WORKLOAD_CALLERS:
            continue
        if a['upstream']['outcome'] != 'ok' or not (a.get('key') or '').startswith(key_prefix + '/'):
            continue
        body = a.get('response_body') or {}
        if a['op'] == 'CreateMultipartUpload' and body.get('upload_id'):
            created[(a['bucket'], a['key'], body['upload_id'])] = True
        elif a['op'] in CLOSING_OPS and not body.get('complete_error'):
            closed.add((a['bucket'], a['key'], a.get('upload_id')))
    return [ident for ident in created if ident not in closed]


def run_case(card, manifest: dict, policy: str, k: int, deps: Deps, *, cluster_factory=Cluster,
             config_writer=write_cost_config, sampler_factory=DirSampler) -> dict:
    return _Pass(card, manifest, policy, k, deps, cluster_factory, config_writer, sampler_factory).run()


class _Pass:
    def __init__(self, card, manifest, policy, k, deps, cluster_factory, config_writer, sampler_factory):
        self.card, self.manifest, self.policy, self.k, self.deps = card, manifest, policy, k, deps
        self.cluster_factory, self.config_writer, self.sampler_factory = cluster_factory, config_writer, sampler_factory
        case = manifest['case_id']
        self.rel = f'{policy}/{card.name}/{case}/k{k}'
        self.pass_dir = os.path.join(deps.runs_dir, deps.run_id, self.rel)
        self.key_prefix = f'{deps.run_id}/{self.rel}'
        self.bucket = deps.topology.bucket
        self.labels = {role: f'{role}@{self.key_prefix}' for role in ledger_mod.MARK_ROLES}
        variant, cached = variant_of(policy)
        self.common = {
            'schema': 'measurement/1', 'measurement_id': f'{deps.run_id}/{self.rel}',
            'experiment_id': f'{card.name}.{case}', 'run_id': deps.run_id, 'card': card.name, 'case_id': case,
            'policy': policy, 'variant': variant, 'cache_enabled': cached, 'replicas': deps.topology.replicas,
            'k': k, 'scale': deps.scale, 'mode': card.mode, 'binary_sha256': deps.binary_sha256,
            'commit': deps.commit, 'pass_dir': self.pass_dir}
        self.sections: dict = {}
        self.cluster = None
        self.ctx = None
        self.families: list = []
        self.setup_seq = None
        self.cluster_up = False
        self.teardown_done = False
        self.samplers: list = []
        self.hashes: list = []

    # --- status records ---

    def unsupported(self, reason: str) -> dict:
        return {**self.common, 'status': 'UNSUPPORTED', 'reason': reason}

    def failed(self, reason: str, error_text: str) -> dict:
        logs = [p for i in range(1, self.deps.topology.replicas + 1)
                if os.path.exists(p := os.path.join(self.pass_dir, f'r{i}', 'server.err.log'))]
        record = {**self.common, 'status': 'FAILED', 'reason': reason,
                  'diagnostics': {'server_logs': logs, 'error_text': error_text}}
        if self.sections:
            record['partial'] = dict(self.sections)
        return record

    def run(self) -> dict:
        for supported, reason in (self.card.supports(self.policy),
                                  check_replicas(self.card, self.deps.topology.replicas)):
            if not supported:
                return self.unsupported(reason)
        # List with the trailing slash so k1 never matches k10.
        try:
            leftovers = self.deps.rig.list(self.bucket, self.key_prefix + '/')
        except Exception as e:
            return self.failed(f'{type(e).__name__}: {e}', traceback.format_exc())
        if leftovers:
            return self.failed('prefix_not_empty', f'{len(leftovers)} objects under {self.key_prefix}/; nothing launched')
        record = None
        try:
            try:
                record = self.measure()
            except CaseFailure as e:
                record = self.failed(e.reason, traceback.format_exc())
            except GenerationCrashed as e:
                record = self.failed('generation_crashed', str(e))
            except Exception as e:
                record = self.failed(f'{type(e).__name__}: {e}', traceback.format_exc())
        finally:
            cleanup = self.cleanup()
            if record is not None:
                self.finish(record, cleanup)
        return record

    def finish(self, record: dict, cleanup: dict) -> None:
        generations = {f'r{i}': [{'pid': g['pid'], 't_start_us': g['t_start_us'],
                                  't_end_us': g['t_end_us'] if g['t_end_us'] is not None else _now_us()}
                                 for g in gens]
                       for i, gens in sorted(self.cluster.generations.items())} if self.cluster else {}
        if record['status'] == 'ok':
            record['generations'] = generations
        elif generations:
            record.setdefault('partial', {})['generations'] = generations
        record['cleanup'] = cleanup

    # --- the measured sequence ---

    def mark(self, role: str) -> int:
        return self.deps.proxy.mark(self.labels[role])

    def measure(self) -> dict:
        deps, card, topology = self.deps, self.card, self.deps.topology
        n = topology.replicas
        local_dirs = [os.path.join(self.pass_dir, 'local', f'r{i}') for i in range(1, n + 1)] \
            if self.policy == 'local' else None
        for d in local_dirs or []:
            os.makedirs(d, exist_ok=True)
        self.ctx = CardContext(cluster=None, proxy=deps.proxy, rig=deps.rig, pass_dir=self.pass_dir,
                               local_dirs=local_dirs or [], policy=self.policy, variant=self.common['variant'],
                               cache_enabled=self.common['cache_enabled'], manifest=self.manifest,
                               replay=self.replay_so_far, window=self.window_so_far)
        controls, settings = card.controls(self.ctx), card.settings(self.ctx)
        self.families = card.inventory_families(self.ctx)
        self.sections['effective_settings'] = settings
        trace_prefix = None
        if card.trace:
            problems = Tracer(self.pass_dir, 1).preflight_trace()
            if problems:
                raise CaseFailure('trace_unavailable', '; '.join(problems))
            trace_prefix = lambda i: Tracer(self.pass_dir, i).prefix()  # noqa: E731

        def writer(*args):
            self.hashes.append(self.config_writer(*args))
            return self.hashes[-1]

        self.cluster = self.cluster_factory(deps.binary, self.pass_dir, topology, self.policy, self.key_prefix,
                                            local_dirs, controls['gc'] == 'measure', settings['server'],
                                            config_writer=writer, trace_prefix=trace_prefix)
        self.ctx.cluster = self.cluster
        self.setup_seq = self.mark('setup_start')
        self.cluster.start_all()
        self.cluster_up = True
        self.sections['config_hashes'] = list(self.hashes)
        card.setup(self.ctx)
        self.mark('window_start')
        t0 = time.monotonic()
        replicas = range(1, n + 1)
        baselines = {i: self.cluster.snapshot_counters(i) for i in replicas}
        relay_start = [relay.snapshot('window_start') for relay in self.cluster.relays]
        for i in replicas:
            sampler = self.sampler_factory(replica_paths(self.cluster.run_dir(i), local_dirs[i - 1] if local_dirs else None),
                                           SAMPLE_INTERVAL_S)
            sampler.start()
            self.samplers.append(sampler)
        result = run_measured(card, self.ctx)
        card.complete(self.ctx, timeout_s=self.manifest.get('timeout_s', 300))
        wall_s = time.monotonic() - t0
        self.collect_counters(baselines, relay_start, local_dirs)
        self.mark('window_end')
        self.teardown_done = True
        deps.proxy.clear_faults()  # a rule left by the measured step must not hit the teardown's own requests
        card.teardown(self.ctx)
        self.mark('teardown_end')
        deps.proxy.flush()
        self.sections.update(effective_counts=result.get('effective_counts', {}),
                             observations=result.get('observations', {}), unit_count=result['unit_count'],
                             wall_s=wall_s)
        self.collect_ledger()
        self.sections['assertions'] = {
            name: {'expected': expected, 'actual': actual, 'ok': expected == actual}
            for name, (expected, actual) in card.assertions(self.ctx, dict(self.sections)).items()}
        return {**self.common, 'status': 'ok', **self.sections}

    def collect_counters(self, baselines: dict, relay_start: list, local_dirs) -> None:
        events, proc_io, local = {}, {}, {}
        for i, baseline in baselines.items():
            since = self.cluster.counters_since(i, baseline)
            events[f'r{i}'], proc_io[f'r{i}'] = since['events'], since['proc_io']
        self.sections.update(events=events, proc_io=proc_io)
        self.sections['interserver'] = {}
        for n, (before, relay) in enumerate(zip(relay_start, self.cluster.relays)):
            after = relay.snapshot('window_end')
            self.sections['interserver'][f'relay{n}'] = {
                'to_target': after['to_target'] - before['to_target'],
                'from_target': after['from_target'] - before['from_target'],
                'connections_total': after['connections_total'] - before['connections_total'],
                'open_at_end': after['open']}
        for i, sampler in zip(baselines, self.samplers):
            sampled = sampler.stop()
            where = local_dirs[i - 1] if local_dirs else self.cluster.run_dir(i)
            traced = self.card.trace
            local[f'r{i}'] = {**sampled, **{key: device_of(where)[key] for key in ('device', 'fs')},
                              'attribution': 'process_io',
                              'trace': parse_strace(Tracer(self.pass_dir, i).path()) if traced else None,
                              'limitations': limitations(self.pass_dir, local_dirs)}
        self.samplers = []
        self.sections['local'] = local

    def pass_attempts(self, rows: list) -> dict:
        marks = {r['label']: r for r in rows if r.get('kind') == 'mark'}
        sliced = ledger_mod.slice(rows, self.labels)
        return {'marks': {role: marks[label] for role, label in self.labels.items()}, 'sliced': sliced}

    def collect_ledger(self) -> None:
        found = self.pass_attempts(ledger_mod.read_rows(self.deps.proxy.ledger_path))
        marks, by_phase = found['marks'], found['sliced'].attempts_by_phase
        pass_rows = by_phase['setup'] + by_phase['window'] + by_phase['teardown']
        workload = {p: [a for a in by_phase[p] if a['caller'] != 'rig'] for p in ('setup', 'window', 'teardown')}
        by_seq = {a['seq']: a for a in pass_rows}
        crossing = [s for s in found['sliced'].crossing if s in by_seq and by_seq[s]['caller'] != 'rig']
        t_w0, t_w1 = marks['window_start']['t_us'], marks['window_end']['t_us']
        self.sections['window'] = {'start_seq': marks['window_start']['seq'], 'end_seq': marks['window_end']['seq'],
                                   't_start_us': t_w0, 't_end_us': t_w1, 'crossing': crossing}
        split = ledger_mod.classify(by_phase['window'], self.families)
        self.sections['ledger'] = {
            'foreground': ledger_mod.summarize(split['foreground']),
            'maintenance': ledger_mod.summarize(split['maintenance']),
            'setup': ledger_mod.summarize(workload['setup']),
            'teardown': ledger_mod.summarize(workload['teardown']),
            'rig': ledger_mod.summarize(split['rig']).get('rig', {})}
        if crossing and self.card.mode == 'deterministic':
            raise CaseFailure('window_crossing', f'attempts {crossing} cross a window boundary in a deterministic card')
        attempts = sorted(sum(workload.values(), []), key=lambda a: a['seq'])
        bounds = ledger_mod.replay(attempts, Inventory(), self.families)
        uncertain = {t['id'] for inv in (bounds.lower, bounds.upper) for t in inv.transitions if t['uncertain']}
        self.sections['inventory'] = {'initial': 'empty_verified', 'byte_hours': ledger_mod.byte_hours(bounds, t_w0, t_w1),
                                      'uncertain_identities': len(uncertain)}

    # --- views for cards ---

    def pass_rows(self) -> list:
        self.deps.proxy.flush()
        return [r for r in ledger_mod.read_rows(self.deps.proxy.ledger_path)
                if r['seq'] > self.setup_seq and r.get('kind') == 'attempt' and r['caller'] != 'rig']

    def replay_so_far(self):
        return ledger_mod.replay(self.pass_rows(), Inventory(), self.families)

    def window_so_far(self) -> dict:
        mark = next((r for r in ledger_mod.read_rows(self.deps.proxy.ledger_path)
                     if r.get('label') == self.labels['window_start']), None)
        return {'start_seq': mark['seq'], 't_start_us': mark['t_us']} if mark else {}

    # --- cleanup ---

    def cleanup(self) -> dict:
        """Undo everything the pass left behind; never raises, so a finished record is still written."""
        deps, out, errors = self.deps, {'aborted_uploads': 0, 'deleted_keys': 0, 'verified_empty': False}, []

        def step(name, fn):
            try:
                return fn()
            except Exception as e:
                errors.append(f'{name}: {type(e).__name__}: {e}')

        step('clear_faults', deps.proxy.clear_faults)
        for sampler in self.samplers:
            step('sampler', sampler.stop)
        if self.cluster_up and not self.teardown_done:
            step('teardown', lambda: self.card.teardown(self.ctx))
        if self.cluster is not None:
            step('stop_all', self.cluster.stop_all)
        uploads = []
        if self.setup_seq is not None:
            step('flush', deps.proxy.flush)
            uploads = step('ledger', lambda: open_uploads(ledger_mod.read_rows(deps.proxy.ledger_path),
                                                          self.setup_seq, self.key_prefix)) or []
        for bucket, key, upload_id in uploads:
            if step(f'abort {key} {upload_id}', lambda: deps.rig.abort_multipart(bucket, key, upload_id) or True):
                out['aborted_uploads'] += 1
        if not deps.keep_prefix:
            out['deleted_keys'] = step('delete_prefix', lambda: deps.rig.delete_prefix(self.bucket, self.key_prefix + '/')) or 0
        remaining = step('verify', lambda: deps.rig.list(self.bucket, self.key_prefix + '/'))
        out['verified_empty'] = remaining == []
        return {**out, 'errors': errors}
