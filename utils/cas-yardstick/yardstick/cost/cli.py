"""`yardstick.py cost run|probe|estimate`: matrix runs of the cost cards, probe gates and the runtime estimate."""
from __future__ import annotations

import glob
import hashlib
import json
import os
import statistics
import subprocess
import sys
import time
import urllib.parse

from . import estimate as estimate_mod
from .proxy import LedgerProxy
from .runner import Deps, run_case
from .s3client import S3Client
from .topology import Topology

HERE = os.path.dirname(os.path.abspath(__file__))
TOOL_DIR = os.path.dirname(os.path.dirname(HERE))
REPO = os.path.dirname(os.path.dirname(TOOL_DIR))
TOOLS_DIR = os.path.join(REPO, 'utils', 'storage-cost-calculator', 'tools')
DEFAULT_ENDPOINT = 'http://127.0.0.1:19121'
DEFAULT_RUNS_DIR = os.path.join(REPO, 'tmp', 'cas-yardstick', 'runs')
PROBE_MANIFESTS = os.path.join(REPO, 'docs', 'superpowers', 'reports', '2026-10-24-storage-cost-phase0', 'experiments')
PROBES = ('probe_s3_insert', 'probe_replicated_fetch', 'probe_multipart_fault', 'probe_cas_gc_cohort',
          'probe_local_volume')
PUT_CLASS = ('PutObject', 'UploadPart', 'CreateMultipartUpload', 'CompleteMultipartUpload', 'CopyObject')
GET_CLASS = ('GetObject', 'HeadObject')
COLUMNS = ['case', 'policy', 'status', 'wall_s', 'PUT', 'GET', 'LIST', 'DEL calls/keys', 'inter-from B', 'local wB']


# --- rig lifecycle ---

class Rustfs:
    """`rustfs-bin.sh`: `ensure` reports whether this call started the store; only then is it stopped."""

    def __init__(self, script=os.path.join(TOOL_DIR, 'rustfs-bin.sh')):
        self.script = script

    def ensure(self) -> bool:
        done = subprocess.run([self.script, 'ensure'], capture_output=True, text=True)
        if done.returncode != 0:
            raise RuntimeError(f'rustfs-bin.sh ensure failed: {done.stderr.strip() or done.stdout.strip()}')
        return '(already up)' not in done.stdout

    def stop(self) -> None:
        subprocess.run([self.script, 'stop'], check=False)


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def _git_commit() -> str:
    return subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=REPO, capture_output=True, text=True,
                          check=True).stdout.strip()


def _read_json(path):
    with open(path, encoding='utf-8') as f:
        return json.load(f)


def _load_manifests(dirs) -> list:
    """Manifests of report directories (`<dir>/experiments/*.json`); an `experiments` directory itself also works."""
    paths = []
    for d in dirs:
        sub = os.path.join(d, 'experiments')
        paths += glob.glob(os.path.join(sub if os.path.isdir(sub) else d, '*.json'))
    return [_read_json(p) for p in sorted(set(paths))]


def _services(args, rustfs, proxy_factory, rig_factory, run_id):
    """Start (or reuse) the store and start the proxy; returns (rustfs_started, proxy, rig, topology)."""
    started = False if args.endpoint else rustfs.ensure()
    endpoint = args.endpoint or DEFAULT_ENDPOINT
    host = urllib.parse.urlparse(endpoint)
    topology = Topology(port_base=args.port_base, replicas=args.replicas, store=(host.hostname, host.port))
    ledger = os.path.join(args.runs_dir, run_id, 'ledger.jsonl')
    proxy = proxy_factory(topology.store, f'store@{endpoint}', ledger, topology.callers)
    return started, proxy, rig_factory(endpoint, 'clickhouse', 'clickhouse'), topology


def _session(args, body, *, proxy_factory, rig_factory, rustfs):
    """Run `body(deps, run_id)` with the store and proxy up; both are stopped in `finally`, the store only if started here."""
    run_id = 'cost-' + time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())
    started, proxy = False, None
    try:
        started, proxy, rig, topology = _services(args, rustfs, proxy_factory, rig_factory, run_id)
        proxy.start()
        problems = topology.verify_services(proxy, rig)
        if problems:
            sys.exit('; '.join(problems))
        deps = Deps(binary=args.binary, run_id=run_id, runs_dir=args.runs_dir, topology=topology, proxy=proxy, rig=rig,
                    store_id=f'store@{args.endpoint or DEFAULT_ENDPOINT}', scale=args.scale,
                    keep_prefix=args.keep_prefix, binary_sha256=_sha256(args.binary),
                    commit=args.commit or _git_commit())
        return body(deps, run_id)
    finally:
        if proxy is not None:
            proxy.stop()
        if started:
            rustfs.stop()


def _execute(matrix, deps, run_id, cards, run_case_fn) -> list:
    """Run each (manifest, policy, k) and append the record to measurements.jsonl; returns the records."""
    path = os.path.join(deps.runs_dir, run_id, 'measurements.jsonl')
    os.makedirs(os.path.dirname(path), exist_ok=True)
    records = []
    for manifest, policies, repeats in matrix:
        for policy in policies:
            for k in range(1, repeats + 1):
                record = run_case_fn(cards[manifest['card']], manifest, policy, k, deps)
                with open(path, 'a', encoding='utf-8') as f:
                    f.write(json.dumps(record, sort_keys=True, allow_nan=False) + '\n')
                records.append(record)
    return records


# --- run ---

def _sum_ops(record: dict, ops) -> dict:
    total = dict.fromkeys(('attempts', 'list_pages', 'delete_calls', 'delete_keys'), 0)
    for per_op in record.get('ledger', {}).get('foreground', {}).values():
        for op, c in per_op.items():
            if ops is None or op in ops:
                for key in total:
                    total[key] += c[key]
    return total


def table_row(r: dict) -> list:
    name = f'{r["card"]}.{r["case_id"]}'
    if r['status'] != 'ok':
        return [name, r['policy'], r['status'] + (f': {r["reason"]}' if r.get('reason') else ''), '', '', '', '', '', '', '']
    allops = _sum_ops(r, None)
    return [name, r['policy'], 'ok', f'{r["wall_s"]:.1f}', str(_sum_ops(r, PUT_CLASS)['attempts']),
            str(_sum_ops(r, GET_CLASS)['attempts']), str(allops['list_pages']),
            f'{allops["delete_calls"]}/{allops["delete_keys"]}',
            str(sum(v['from_target'] for v in r.get('interserver', {}).values())),
            str(sum(v.get('write_bytes', 0) for v in r.get('proc_io', {}).values()))]


def print_table(records: list) -> None:
    rows = [COLUMNS] + [table_row(r) for r in records]
    widths = [max(len(row[i]) for row in rows) for i in range(len(COLUMNS))]
    for row in rows:
        print('  '.join(cell.ljust(w) for cell, w in zip(row, widths)).rstrip())


def _matrix(args, cards, only=None) -> list:
    wanted = None if args.cases == 'all' else set(args.cases.split(','))
    chosen = set(args.policies.split(','))
    out = []
    for manifest in sorted(_load_manifests(args.manifests if isinstance(args.manifests, list) else [args.manifests]),
                           key=lambda m: m['experiment_id']):
        if (wanted is not None and manifest['experiment_id'] not in wanted) or (only and manifest['card'] not in only):
            continue
        policies = [p for p in manifest['policies'] if p in chosen]
        out.append((manifest, policies, args.repeats or manifest['repeats']))
    return out


def cmd_run(args, *, run_case=run_case, proxy_factory=LedgerProxy, rig_factory=S3Client, rustfs=None, cards=None) -> int:
    if cards is None:
        from .cards import CARDS as cards  # noqa: PLC0415
    matrix = _matrix(args, cards)
    missing = sorted({m['card'] for m, _, _ in matrix if m['card'] not in cards})
    if missing:
        print(f'no such card: {", ".join(missing)}', file=sys.stderr)
        return 1

    def body(deps, run_id):
        records = _execute(matrix, deps, run_id, cards, run_case)
        print_table(records)
        return 1 if any(r['status'] == 'FAILED' for r in records) else 0
    return _session(args, body, proxy_factory=proxy_factory, rig_factory=rig_factory, rustfs=rustfs or Rustfs())


# --- probe ---

def _node(tools_dir, script, *rest):
    path = os.path.join(tools_dir, script)
    if not os.path.isfile(path):
        return 'tool missing'
    done = subprocess.run(['node', path, *rest], capture_output=True, text=True)
    return None if done.returncode == 0 else ('drill failed' if script == 'drill.mjs' else 'render check failed')


def cmd_probe(args, *, run_case=run_case, proxy_factory=LedgerProxy, rig_factory=S3Client, rustfs=None, cards=None,
              tools_dir=TOOLS_DIR) -> int:
    if cards is None:
        from .cards import CARDS as cards  # noqa: PLC0415
    args.cases, args.policies = 'all', getattr(args, 'policies', None) or 's3,cas,local'
    args.manifests = args.manifests or PROBE_MANIFESTS
    args.repeats = 1
    matrix = _matrix(args, cards, only=set(PROBES))

    def body(deps, run_id):
        run_dir = os.path.join(deps.runs_dir, run_id)
        results = {}
        for probe in PROBES:
            mine = [m for m in matrix if m[0]['card'] == probe]
            if probe not in cards or not mine:
                results[probe] = ('FAIL', 'card or manifest missing', None)
                continue
            records = _execute(mine, deps, run_id, cards, run_case)
            drill_json = os.path.join(run_dir, f'drill-{probe}.json')
            reason = next((f'run {r["status"]}' for r in records if r['status'] != 'ok'), None)
            reason = reason or _node(tools_dir, 'drill.mjs', os.path.join(run_dir, 'measurements.jsonl'), '--probe',
                                     probe, '--out', drill_json)
            reason = reason or _node(tools_dir, 'render_check.mjs', drill_json, '--probe', probe)
            results[probe] = ('FAIL' if reason else 'PASS', reason, drill_json)
        for probe, (verdict, reason, path) in results.items():
            print(f'{verdict} {probe}' + (f': {reason}' if reason else '') + (f'  drill: {path}' if path else ''))
        return 2 if any(v == 'FAIL' for v, _, _ in results.values()) else 0
    return _session(args, body, proxy_factory=proxy_factory, rig_factory=rig_factory, rustfs=rustfs or Rustfs())


# --- estimate ---

def cmd_estimate(args) -> int:
    records = []
    path = os.path.join(args.run_dir, 'measurements.jsonl')
    if os.path.exists(path):
        with open(path, encoding='utf-8') as f:
            records = [json.loads(line) for line in f if line.strip()]
    registry = _read_json(args.registry) if os.path.exists(args.registry) else None
    presets = None
    if os.path.exists(args.presets):
        with open(args.presets, encoding='utf-8') as f:
            presets = estimate_mod.presets_by_coefficient(f.read(), registry)
    sample_of = dict(item.split('=', 1) for item in args.sample_of)
    est = estimate_mod.estimate(records, _read_json(args.coverage), registry, presets, args.budget_hours, sample_of)
    if args.json:
        print(json.dumps(est.__dict__, indent=2, sort_keys=True))
    else:
        print(estimate_mod.render(est))
    return 0


# --- argparse ---

def add_parsers(sub) -> None:
    cost = sub.add_parser('cost', help='cost rig: run, probe, estimate')
    csub = cost.add_subparsers(dest='cost_command', required=True)

    def common(p):
        p.add_argument('--binary', required=True)
        p.add_argument('--runs-dir', default=DEFAULT_RUNS_DIR)
        p.add_argument('--port-base', type=int, default=19700)
        p.add_argument('--endpoint', help=f'an already running store instead of rustfs-bin.sh on {DEFAULT_ENDPOINT}')
        p.add_argument('--commit', help='default: git rev-parse HEAD of the worktree')

    r = csub.add_parser('run', help='run manifests x policies x repeats')
    common(r)
    r.add_argument('--manifests', action='append', required=True, help='report directory with experiments/*.json')
    r.add_argument('--cases', default='all', help='comma-separated experiment ids, or all')
    r.add_argument('--policies', default='s3,cas', help='comma-separated, intersected with each manifest')
    r.add_argument('--replicas', type=int, default=1)
    r.add_argument('--repeats', type=int, help='default: the manifest\'s')
    r.add_argument('--scale', type=float, default=1.0)
    r.add_argument('--keep-prefix', action='store_true')
    r.set_defaults(func=cmd_run)

    p = csub.add_parser('probe', help='run the five probes, then drill and render checks; exit 2 on FAIL')
    common(p)
    p.add_argument('--manifests', default=PROBE_MANIFESTS)
    p.add_argument('--replicas', type=int, default=2)
    p.add_argument('--scale', type=float, default=1.0)
    p.add_argument('--keep-prefix', action='store_true')
    p.set_defaults(func=cmd_probe)

    e = csub.add_parser('estimate', help='hours for the planned cases and the cuts to reach a budget')
    e.add_argument('run_dir')
    e.add_argument('--coverage', default=os.path.join(REPO, 'docs', 'superpowers', 'cost-model', 'coverage.json'))
    e.add_argument('--registry', default=os.path.join(REPO, 'docs', 'superpowers', 'cost-model', 'registry.json'))
    e.add_argument('--presets', default=os.path.join(REPO, 'utils', 'storage-cost-calculator', 'presets.js'))
    e.add_argument('--budget-hours', type=float, default=72)
    e.add_argument('--sample-of', action='append', default=[], metavar='FAMILY=CASE',
                   help='case whose measured pass stands in for a family with no records')
    e.add_argument('--json', action='store_true')
    e.set_defaults(func=cmd_estimate)
