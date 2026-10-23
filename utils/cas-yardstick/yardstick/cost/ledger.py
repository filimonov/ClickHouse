"""Ledger reader for the cost rig: windows, per-caller counts, retry groups and bounded object inventory.

Pure functions over `ledger-row/1` rows (docs/superpowers/cost-model/schemas). The proxy writes the rows;
nothing here touches the network or the store.
"""
from __future__ import annotations

import copy
import json
import re
from collections import defaultdict

from .proxy import parse_operation  # noqa: F401  (re-exported: the ledger and the proxy share one op table)

PHASES = ('setup', 'window', 'teardown', 'outside')
MARK_ROLES = ('setup_start', 'window_start', 'window_end', 'teardown_end')
WORKLOAD_CALLERS = ('r1', 'r2', 'r3')
COUNT_FIELDS = ('attempts', 'delivered', 'dropped', 'injected', 'rejected', 'upstream_ok', 'upstream_error',
                'upstream_unknown', 'req_payload_bytes', 'resp_payload_bytes', 'list_pages', 'list_keys',
                'delete_calls', 'delete_keys')
# Signature and SDK bookkeeping change on every retry; everything else under x-amz-* is part of the request's meaning.
VOLATILE_HEADERS = {'x-amz-date', 'x-amz-security-token', 'x-amz-content-sha256', 'x-amz-user-agent',
                    'amz-sdk-invocation-id', 'amz-sdk-request'}
US_PER_HOUR = 3_600_000_000


def read_rows(path) -> list:
    with open(path, encoding='utf-8') as f:
        return [json.loads(line) for line in f if line.strip()]


# --- slicing -----------------------------------------------------------------------------------

class Sliced:
    def __init__(self, attempts_by_phase: dict, crossing: list):
        self.attempts_by_phase = attempts_by_phase
        self.crossing = crossing


def slice(rows: list, marks: dict) -> Sliced:  # noqa: A001 (name fixed by the interface)
    by_label = {}
    for r in rows:
        if r.get('kind') == 'mark':
            by_label.setdefault(r['label'], r)
    found = {}
    for role in MARK_ROLES:
        label = marks[role]
        if label not in by_label:
            raise ValueError(f'mark {label!r} for role {role} not found in ledger')
        found[role] = by_label[label]
    s0, w0, w1, e1 = (found[r]['seq'] for r in MARK_ROLES)
    t_w0, t_w1 = found['window_start']['t_us'], found['window_end']['t_us']

    by_phase = {p: [] for p in PHASES}
    crossing = []
    for a in rows:
        if a.get('kind') != 'attempt':
            continue
        seq = a['seq']
        if s0 < seq < w0:
            phase = 'setup'
        elif w0 < seq < w1:
            phase = 'window'
        elif w1 < seq < e1:
            phase = 'teardown'
        else:
            phase = 'outside'
        a['phase'] = phase
        by_phase[phase].append(a)
        end = a.get('t_end_us')
        if end is None or any(a['t_start_us'] <= t <= end for t in (t_w0, t_w1)):
            crossing.append(seq)
    return Sliced(by_phase, sorted(crossing))


# --- classification and counts -----------------------------------------------------------------

def family_of(families: list, key):
    """First family whose regex matches the key; objects outside every family are plain `active` data."""
    if key is not None:
        for f in families:
            if re.search(f['regex'], key):
                return f
    return None


def classify(attempts: list, families: list) -> dict:
    out = {'foreground': [], 'maintenance': [], 'rig': []}
    for a in attempts:
        if a['caller'] == 'rig':
            out['rig'].append(a)
            continue
        f = family_of(families, a.get('key'))
        out['maintenance' if f is not None and f.get('maintenance') else 'foreground'].append(a)
    return out


def summarize(attempts: list) -> dict:
    out: dict = {}
    for a in attempts:
        c = out.setdefault(a['caller'], {}).setdefault(a['op'], dict.fromkeys(COUNT_FIELDS, 0))
        body = a.get('response_body') or {}
        c['attempts'] += 1
        c[a['downstream']['outcome']] += 1
        up = a['upstream']['outcome']
        if up in ('ok', 'error', 'unknown'):
            c['upstream_' + up] += 1
        c['req_payload_bytes'] += a.get('req_payload_bytes') or 0
        c['resp_payload_bytes'] += a.get('resp_payload_bytes') or 0
        if a['op'] == 'ListObjectsV2':
            c['list_pages'] += 1
            c['list_keys'] += (body.get('list') or {}).get('key_count') or 0
        elif a['op'] == 'DeleteObject':
            c['delete_calls'] += 1
            c['delete_keys'] += 1
        elif a['op'] == 'DeleteObjects':
            c['delete_calls'] += 1
            c['delete_keys'] += len(body.get('delete_results') or [])
    return out


# --- retry correlation -------------------------------------------------------------------------

def _failed(a) -> bool:
    return a['downstream']['outcome'] in ('dropped', 'injected') or a['upstream']['outcome'] in ('error', 'unknown')


def _identity(a):
    headers = {k.lower(): v for k, v in (a.get('req_headers') or {}).items()}
    cond = tuple(sorted((k, v) for k, v in headers.items()
                        if k in ('if-match', 'if-none-match') or (k.startswith('x-amz-') and k not in VOLATILE_HEADERS)))
    return (a['caller'], a['op'], a['store_id'], a.get('bucket'), a.get('key'), a.get('upload_id'),
            a.get('part_number'), a.get('payload_sha256'), cond)


def correlate_retries(attempts: list) -> list:
    """Groups of seqs where each attempt after the first follows a failed one with the same identity."""
    chains: dict = {}
    groups = []
    for a in sorted(attempts, key=lambda x: x['seq']):
        ident = _identity(a)
        chain = chains.get(ident)
        if chain and _failed(chain[-1]):
            chain.append(a)
            continue
        if chain and len(chain) > 1:
            groups.append(chain)
        chains[ident] = [a]
    groups.extend(c for c in chains.values() if len(c) > 1)
    return sorted(([a['seq'] for a in g] for g in groups), key=lambda g: g[0])


# --- bounded lifecycle replay ------------------------------------------------------------------

class Inventory:
    """Objects and multipart parts at one instant, plus the transition log that `byte_hours` integrates."""

    def __init__(self, objects=None, incomplete=None, transitions=None):
        self.objects = objects if objects is not None else {}
        self.incomplete = incomplete if incomplete is not None else {}
        self.transitions = transitions if transitions is not None else []

    def _log(self, t_us, action, ident, size=0, category=None, uncertain=False, evidence=None):
        self.transitions.append({'t_us': t_us, 'action': action, 'id': ident, 'size': size,
                                 'category': category, 'uncertain': uncertain, 'evidence': evidence})

    def put_object(self, ident, size, t_us, family, uncertain=False):
        name = family['name'] if family else None
        category = family['category'] if family else 'active'
        self.objects[ident] = {'size': size, 'created_us': t_us, 'family': name, 'category': category,
                               'uncertain': uncertain}
        self._log(t_us, 'set', ident, size, category, uncertain)

    def delete_object(self, ident, t_us):
        self.objects.pop(ident, None)
        self._log(t_us, 'del', ident)

    def put_part(self, ident, size, t_us, uncertain=False):
        self.incomplete[ident] = {'size': size, 't_us': t_us, 'uncertain': uncertain}
        self._log(t_us, 'set', ident, size, 'incomplete', uncertain)

    def retire_parts(self, bucket, key, upload_id, t_us):
        for ident in [i for i in self.incomplete if i[:3] == (bucket, key, upload_id)]:
            del self.incomplete[ident]
            self._log(t_us, 'del', ident)

    def reclassify(self, identities, category: str, t_us: int, evidence):
        """Attach a logical category (`outdated`, `garbage`, ...) to objects from `t_us` onward."""
        for ident in identities:
            if ident in self.objects:
                self.objects[ident]['category'] = category
            self._log(t_us, 'cat', ident, category=category, evidence=evidence)

    def seed_transitions(self):
        """Give a hand-built initial inventory a log, so its bytes count from the moment they were created."""
        for ident, o in self.objects.items():
            self._log(o['created_us'], 'set', ident, o['size'], o['category'], o['uncertain'])
        for ident, p in self.incomplete.items():
            self._log(p['t_us'], 'set', ident, p['size'], 'incomplete', p.get('uncertain', False))


class Bounds:
    def __init__(self, lower: Inventory, upper: Inventory):
        self.lower = lower
        self.upper = upper

    def reclassify(self, identities, category, t_us, evidence):
        for inv in (self.lower, self.upper):
            inv.reclassify(identities, category, t_us, evidence)


def _range_length(spec, total):
    m = re.fullmatch(r'bytes=(\d+)-(\d+)', spec or '')
    return int(m.group(2)) - int(m.group(1)) + 1 if m else total


def _source(inv: Inventory, a):
    src = a.get('copy_source') or {}
    return inv.objects.get((src.get('bucket'), src.get('key'))), src


def _apply(inv: Inventory, a, t, families):
    op, b, k = a['op'], a.get('bucket'), a.get('key')
    body = a.get('response_body') or {}
    if op == 'PutObject':
        inv.put_object((b, k), a.get('req_payload_bytes') or 0, t, family_of(families, k))
    elif op == 'CopyObject':
        src, _ = _source(inv, a)
        inv.put_object((b, k), src['size'] if src else 0, t, family_of(families, k),
                       uncertain=src is None or src['uncertain'])
    elif op == 'UploadPart':
        inv.put_part((b, k, a.get('upload_id'), a.get('part_number')), a.get('req_payload_bytes') or 0, t)
    elif op == 'UploadPartCopy':
        src, spec = _source(inv, a)
        size = _range_length(spec.get('range'), src['size'] if src else 0)
        inv.put_part((b, k, a.get('upload_id'), a.get('part_number')), size, t,
                     uncertain=src is None or src['uncertain'])
    elif op == 'CompleteMultipartUpload':
        if body.get('complete_error'):
            return
        total, uncertain = 0, False
        listed = body.get('completed_parts')
        if listed is None:
            uncertain = True
        for p in listed or []:
            part = inv.incomplete.get((b, k, a.get('upload_id'), p['part_number']))
            if part is None or part['uncertain']:
                uncertain = True
            else:
                total += part['size']
        inv.retire_parts(b, k, a.get('upload_id'), t)
        inv.put_object((b, k), total, t, family_of(families, k), uncertain)
    elif op == 'AbortMultipartUpload':
        inv.retire_parts(b, k, a.get('upload_id'), t)
    elif op == 'DeleteObject':
        inv.delete_object((b, k), t)
    elif op == 'DeleteObjects':
        # Quiet mode lists only failures in the response; the proxy merges both sides into `deleted`/`code`.
        for r in body.get('delete_results') or []:
            if r.get('deleted') or not r.get('code'):
                inv.delete_object((b, r['key']), t)


def replay(attempts: list, initial: Inventory, families: list) -> Bounds:
    """`lower` applies only known-ok mutations, at their end time; `upper` adds unknown ones, at their start time."""
    bounds = []
    for include, time_of in ((('ok',), lambda a: a['t_end_us'] if a.get('t_end_us') is not None else a['t_start_us']),
                             (('ok', 'unknown'), lambda a: a['t_start_us'])):
        inv = copy.deepcopy(initial)
        if not inv.transitions:
            inv.seed_transitions()
        events = sorted(((time_of(a), a['seq'], a) for a in attempts if a['upstream']['outcome'] in include),
                        key=lambda e: e[:2])
        for t, _, a in events:
            _apply(inv, a, t, families)
        bounds.append(inv)
    return Bounds(*bounds)


def _integrate(inv: Inventory, t0: int, t1: int) -> dict:
    by_ident = defaultdict(list)
    for i, tr in enumerate(inv.transitions):
        by_ident[tr['id']].append((tr['t_us'], i, tr))
    totals: dict = defaultdict(float)
    for events in by_ident.values():
        events.sort(key=lambda e: e[:2])
        state = None
        for n, (t, _, tr) in enumerate(events):
            if tr['action'] == 'set':
                state = (tr['size'], tr['category'], tr['uncertain'])
            elif tr['action'] == 'del':
                state = None
            elif state is not None:
                state = (state[0], tr['category'], state[2])
            end = events[n + 1][0] if n + 1 < len(events) else t1
            overlap = min(end, t1) - max(t, t0)
            if state is not None and overlap > 0:
                size, category, uncertain = state
                totals['uncertain' if uncertain else category] += size * overlap / US_PER_HOUR
    return totals


def byte_hours(bounds: Bounds, t0_us: int, t1_us: int) -> dict:
    """Byte-hours per category inside [t0_us, t1_us]. Objects of unknown size land in `uncertain` with size 0."""
    lo, hi = _integrate(bounds.lower, t0_us, t1_us), _integrate(bounds.upper, t0_us, t1_us)
    cats = set(lo) | set(hi) | {'incomplete', 'uncertain'}
    return {c: {'lower': lo.get(c, 0.0), 'upper': hi.get(c, 0.0)} for c in sorted(cats)}


# --- row sanity --------------------------------------------------------------------------------

def check_rows(attempts: list) -> list:
    problems, seen, last = [], set(), None
    for a in attempts:
        seq = a['seq']
        if seq in seen:
            problems.append(f'seq {seq}: duplicate')
        seen.add(seq)
        if last is not None and seq < last:
            problems.append(f'seq {seq}: out of order after {last}')
        last = seq
        end = a.get('t_end_us')
        if end is None:
            problems.append(f'seq {seq}: no t_end_us (unfinished attempt)')
        elif end < a['t_start_us']:
            problems.append(f'seq {seq}: t_end_us before t_start_us')
        up = a['upstream']['outcome']
        if up == 'not_sent' and a['downstream']['outcome'] == 'delivered':
            problems.append(f'seq {seq}: delivered although never sent upstream')
        body = a.get('response_body') or {}
        if up == 'ok' and a['op'] == 'DeleteObjects' and body.get('delete_results') is None:
            problems.append(f'seq {seq}: DeleteObjects ok without delete_results')
        if up == 'ok' and a['op'] == 'CompleteMultipartUpload' and not body.get('complete_error') \
                and body.get('completed_parts') is None:
            problems.append(f'seq {seq}: CompleteMultipartUpload ok without completed_parts')
    return problems
