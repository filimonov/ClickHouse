"""`yardstick.py cost run|probe|estimate`: matrix runs of the cost cards, probe gates and the runtime estimate."""
from __future__ import annotations

import glob
import hashlib
import json
import os
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
          'probe_local_volume', 'probe_cas_expect')
PUT_CLASS = ('PutObject', 'UploadPart', 'CreateMultipartUpload', 'CompleteMultipartUpload', 'CopyObject')
GET_CLASS = ('GetObject', 'HeadObject')
ENSURE_ALREADY_UP = 3
SUPPORTED_POLICIES = ('s3', 's3plain', 'cas', 'local', 's3_cached', 's3plain_cached', 'cas_cached')
COLUMNS = ['case', 'policy', 'status', 'wall_s', 'PUT', 'GET', 'LIST', 'DEL calls/keys', 'inter-from B', 'local wB']


# --- rig lifecycle ---

class Rustfs:
    """`rustfs-bin.sh`: `ensure` reports whether this call started the store; only then is it stopped."""

    def __init__(self, script=os.path.join(TOOL_DIR, 'rustfs-bin.sh')):
        self.script = script

    def ensure(self) -> bool:
        """True when this call started the store (exit 0); False when it was already up (exit 3)."""
        done = subprocess.run([self.script, 'ensure'], capture_output=True, text=True)
        if done.returncode not in (0, ENSURE_ALREADY_UP):
            raise RuntimeError(f'rustfs-bin.sh ensure failed: {done.stderr.strip() or done.stdout.strip()}')
        return done.returncode == 0

    def stop(self) -> None:
        subprocess.run([self.script, 'stop'], check=False)


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def _git_commit(where: str = REPO) -> str:
    return subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=where, capture_output=True, text=True,
                          check=True).stdout.strip()


def download_sha256(binary: str):
    """Hash of `binary` as downloaded, from the nearest `SHA256SUMS` above it that lists it; None if none does.

    A self-extracting `clickhouse` rewrites itself on first start, so this differs from the executed binary's hash.
    A listed path resolves against the sums file's directory or any ancestor (they list repo-relative paths).
    """
    target = os.path.abspath(binary)
    d = os.path.dirname(target)
    while True:
        sums = os.path.join(d, 'SHA256SUMS')
        if os.path.isfile(sums):
            with open(sums, encoding='utf-8') as f:
                for line in f:
                    digest, _, name = line.strip().partition('  ')
                    base = d
                    while name:
                        if os.path.abspath(os.path.join(base, name)) == target:
                            return digest
                        if os.path.dirname(base) == base:
                            break
                        base = os.path.dirname(base)
        if os.path.dirname(d) == d:
            return None
        d = os.path.dirname(d)


def source_dir_for(binary: str, root: str = os.path.join(REPO, 'tmp')):
    """`<root>/src-<version>` for a binary at `.../<version>/clickhouse`, when that checkout exists."""
    candidate = os.path.join(root, 'src-' + os.path.basename(os.path.dirname(os.path.abspath(binary))))
    return candidate if os.path.isdir(candidate) else None


def source_commit(args) -> str:
    """The source commit the binary was built from: `--commit`, else HEAD of `--source-dir` or the pinned checkout."""
    if args.commit:
        return args.commit
    where = getattr(args, 'source_dir', None) or source_dir_for(args.binary)
    if not where:
        sys.exit(f'cannot tell the source commit of {args.binary}: pass --commit or --source-dir')
    return _git_commit(where)


def _read_json(path):
    with open(path, encoding='utf-8') as f:
        return json.load(f)


def report_dir_of(d: str) -> str:
    """The report directory of a `--manifests` value: the directory itself, or the parent of an `experiments` one."""
    d = os.path.abspath(d)
    return d if os.path.isdir(os.path.join(d, 'experiments')) else os.path.dirname(d)


def _load_manifests(dirs) -> list:
    """Manifests of report directories (`<dir>/experiments/*.json`); an `experiments` directory itself also works.

    A card reads its predictions from the report directory its manifest came from (`params.report_dir`), unless
    the manifest names another one.
    """
    out = []
    for d in dirs:
        sub = os.path.join(d, 'experiments')
        for path in sorted(glob.glob(os.path.join(sub if os.path.isdir(sub) else d, '*.json'))):
            manifest = _read_json(path)
            if isinstance(manifest.get('params'), dict):
                manifest['params'].setdefault('report_dir', report_dir_of(d))
            out.append((path, manifest))
    seen, manifests = set(), []
    for path, manifest in sorted(out, key=lambda x: x[0]):
        if path not in seen:
            seen.add(path)
            manifests.append(manifest)
    return manifests


def _services(args, proxy_factory, run_id):
    """Topology and proxy (not started) for the store at `args.endpoint` or the default."""
    endpoint = args.endpoint or DEFAULT_ENDPOINT
    host = urllib.parse.urlparse(endpoint)
    topology = Topology(port_base=args.port_base, replicas=args.replicas, store=(host.hostname, host.port))
    ledger = os.path.join(args.runs_dir, run_id, 'ledger.jsonl')
    proxy = proxy_factory(topology.store, f'store@{endpoint}', ledger, topology.callers)
    return proxy, topology


def _session(args, body, *, proxy_factory, rig_factory, rustfs):
    """Run `body(deps, run_id)` with the store and proxy up; both are stopped in `finally`, the store only if started here."""
    run_id = 'cost-' + time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())
    started, proxy = False, None
    try:
        if not args.endpoint:
            # Ownership is recorded before anything else can raise, so the finally still stops a store started here.
            started = rustfs.ensure()
        proxy, topology = _services(args, proxy_factory, run_id)
        proxy.start()
        # Every rig request (listings, observer aborts, cleanup) goes through the proxy's `rig` listener, so an
        # observer mutation is in the ledger; only `rustfs-bin.sh ensure` talks to the store directly.
        rig = rig_factory(f'http://127.0.0.1:{proxy.ports["rig"]}', 'clickhouse', 'clickhouse')
        problems = topology.verify_services(proxy, rig)
        if problems:
            sys.exit('; '.join(problems))
        deps = Deps(binary=args.binary, run_id=run_id, runs_dir=args.runs_dir, topology=topology, proxy=proxy, rig=rig,
                    store_id=f'store@{args.endpoint or DEFAULT_ENDPOINT}', keep_prefix=args.keep_prefix, binary_sha256=_sha256(args.binary), commit=source_commit(args),
                    binary_download_sha256=download_sha256(args.binary), rig_commit=_git_commit())
        return body(deps, run_id)
    finally:
        try:
            if proxy is not None:
                proxy.stop()
        finally:
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


def cleanup_failed(r: dict, keep_prefix: bool = False) -> bool:
    """A cleanup step raised, or the prefix was meant to be deleted and is not verified empty."""
    c = r.get('cleanup')
    return bool(c) and (bool(c['errors']) or (not keep_prefix and not c['verified_empty']))


def table_row(r: dict, keep_prefix: bool = False) -> list:
    name = f'{r["card"]}.{r["case_id"]}'
    if r['status'] != 'ok':
        return [name, r['policy'], r['status'] + (f': {r["reason"]}' if r.get('reason') else ''), '', '', '', '', '', '', '']
    allops = _sum_ops(r, None)
    status = 'ok, cleanup failed' if cleanup_failed(r, keep_prefix) else 'ok'
    return [name, r['policy'], status, f'{r["wall_s"]:.1f}', str(_sum_ops(r, PUT_CLASS)['attempts']),
            str(_sum_ops(r, GET_CLASS)['attempts']), str(allops['list_pages']),
            f'{allops["delete_calls"]}/{allops["delete_keys"]}',
            # bytes the relays received from the replicas' interserver servers, i.e. what the senders shipped
            str(sum(v['from_target'] for v in r.get('interserver', {}).values())),
            str(sum(v.get('write_bytes', 0) for v in r.get('proc_io', {}).values()))]


def print_table(records: list, keep_prefix: bool = False) -> None:
    rows = [COLUMNS] + [table_row(r, keep_prefix) for r in records]
    widths = [max(len(row[i]) for row in rows) for i in range(len(COLUMNS))]
    for row in rows:
        print('  '.join(cell.ljust(w) for cell, w in zip(row, widths)).rstrip())


def _matrix(args, cards, only=None) -> list:
    """(manifest, policies, repeats) per selected manifest; ValueError names a selection that matches nothing."""
    wanted = None if args.cases == 'all' else set(args.cases.split(','))
    chosen = args.policies.split(',')
    bad = [p for p in chosen if p not in SUPPORTED_POLICIES]
    if bad:
        raise ValueError(f'unsupported --policies value {", ".join(bad)}; supported: {", ".join(SUPPORTED_POLICIES)}')
    manifests = sorted(_load_manifests(args.manifests if isinstance(args.manifests, list) else [args.manifests]),
                       key=lambda m: m['experiment_id'])
    unknown = sorted((wanted or set()) - {m['experiment_id'] for m in manifests})
    if unknown:
        raise ValueError(f'unknown --cases value {", ".join(unknown)}: no such experiment_id in the manifests')
    out = []
    for manifest in manifests:
        if (wanted is not None and manifest['experiment_id'] not in wanted) or (only and manifest['card'] not in only):
            continue
        policies = [p for p in manifest['policies'] if p in chosen]
        out.append((manifest, policies, args.repeats or manifest['repeats']))
    if not any(policies for _, policies, _ in out):
        raise ValueError(f'nothing to run: no manifest matches --cases {args.cases} with --policies {args.policies}')
    return out


def cmd_run(args, *, run_case=run_case, proxy_factory=LedgerProxy, rig_factory=S3Client, rustfs=None, cards=None) -> int:
    if cards is None:
        from .cards import CARDS as cards  # noqa: PLC0415
    try:
        matrix = _matrix(args, cards)
    except ValueError as e:
        print(e, file=sys.stderr)
        return 1
    missing = sorted({m['card'] for m, _, _ in matrix if m['card'] not in cards})
    if missing:
        print(f'no such card: {", ".join(missing)}', file=sys.stderr)
        return 1

    def body(deps, run_id):
        records = _execute(matrix, deps, run_id, cards, run_case)
        print_table(records, deps.keep_prefix)
        return 1 if any(r['status'] == 'FAILED' or cleanup_failed(r, deps.keep_prefix) for r in records) else 0
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
    try:
        matrix = _matrix(args, cards, only=set(PROBES))
    except ValueError as e:
        print(e, file=sys.stderr)
        return 2

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
            reason = next((f'run {r["status"]}' + (f': {r["reason"]}' if r.get('reason') else '')
                           for r in records if r['status'] != 'ok'), None)
            reason = reason or ('cleanup failed' if any(cleanup_failed(r, deps.keep_prefix) for r in records) else None)
            failed = sorted(n for r in records for n, a in r.get('assertions', {}).items() if not a['ok'])
            reason = reason or (f'assertions failed: {", ".join(failed)}' if failed else None)
            # Several probes share a variant; each probe's formulas cell is named after the probe.
            reason = reason or _node(tools_dir, 'drill.mjs', os.path.join(run_dir, 'measurements.jsonl'), '--probe',
                                     probe, '--cell', probe, '--out', drill_json, '--report-dir',
                                     report_dir_of(args.manifests))
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
    if not os.path.isfile(args.registry):
        # The cut list names the quantities each cut removes; without the registry it would silently list none.
        print(f'registry not found: {args.registry}', file=sys.stderr)
        return 2
    registry = _read_json(args.registry)
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
        p.add_argument('--commit', help='source commit of the binary; default: HEAD of --source-dir')
        p.add_argument('--source-dir', help='source checkout of the binary; default: tmp/src-<version> for '
                                            'a binary at .../<version>/clickhouse')

    r = csub.add_parser('run', help='run manifests x policies x repeats')
    common(r)
    r.add_argument('--manifests', action='append', required=True, help='report directory with experiments/*.json')
    r.add_argument('--cases', default='all', help='comma-separated experiment ids, or all')
    r.add_argument('--policies', default='s3,cas', help='comma-separated, intersected with each manifest')
    r.add_argument('--replicas', type=int, default=1, help='servers the rig may start; each pass runs its card\'s count')
    r.add_argument('--repeats', type=int, help='default: the manifest\'s')
    r.add_argument('--keep-prefix', action='store_true')
    r.set_defaults(func=cmd_run)

    p = csub.add_parser('probe', help='run the phase-0 probes, then drill and render checks; exit 2 on FAIL')
    common(p)
    p.add_argument('--manifests', default=PROBE_MANIFESTS)
    p.add_argument('--replicas', type=int, default=2, help='servers the rig may start; each pass runs its card\'s count')
    p.add_argument('--keep-prefix', action='store_true')
    p.set_defaults(func=cmd_probe)

    e = csub.add_parser('estimate', help='hours for the planned cases and the cuts to reach a budget')
    e.add_argument('run_dir')
    e.add_argument('--coverage', default=os.path.join(REPO, 'docs', 'superpowers', 'cost-model', 'coverage.json'))
    e.add_argument('--registry', default=os.path.join(REPO, 'docs', 'superpowers', 'cost-model', 'inputs-registry.json'))
    e.add_argument('--presets', default=os.path.join(REPO, 'utils', 'storage-cost-calculator', 'presets.js'))
    e.add_argument('--budget-hours', type=float, default=72)
    e.add_argument('--sample-of', action='append', default=[], metavar='FAMILY=CASE',
                   help='case whose measured pass stands in for a family with no records')
    e.add_argument('--json', action='store_true')
    e.set_defaults(func=cmd_estimate)
