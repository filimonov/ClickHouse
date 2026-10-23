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
    by_label: dict = defaultdict(list)
    for r in rows:
        if r.get('kind') == 'mark':
            by_label[r['label']].append(r)
    found = {}
    for role in MARK_ROLES:
        label = marks[role]
        hits = by_label.get(label, [])
        if not hits:
            raise ValueError(f'mark {label!r} for role {role} is missing from the ledger')
        if len(hits) > 1:
            raise ValueError(f'mark {label!r} for role {role} appears {len(hits)} times (seq {[h["seq"] for h in hits]})')
        found[role] = hits[0]
    seqs = [found[r]['seq'] for r in MARK_ROLES]
    if any(x >= y for x, y in zip(seqs, seqs[1:])):
        raise ValueError('marks out of order, need setup_start < window_start < window_end < teardown_end by seq: '
                         + ', '.join(f'{r}={found[r]["label"]!r}@{found[r]["seq"]}' for r in MARK_ROLES))
    s0, w0, w1, e1 = seqs
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
    """Split attempts into `foreground`, `maintenance` (workload callers) and `rig`.

    A family's `maintenance` flag marks its keys as maintenance traffic. A `DeleteObjects` attempt has no key:
    it is maintenance iff it lists at least one key and every listed key matches a maintenance family.
    Other keyless attempts (bucket-level LIST) are foreground.
    """
    def maintenance(f):
        return f is not None and bool(f.get('maintenance'))

    out = {'foreground': [], 'maintenance': [], 'rig': []}
    for a in attempts:
        if a['caller'] == 'rig':
            out['rig'].append(a)
            continue
        if a.get('key') is None and a['op'] == 'DeleteObjects':
            keys = [r['key'] for r in (a.get('response_body') or {}).get('delete_results') or []]
            is_m = bool(keys) and all(maintenance(family_of(families, k)) for k in keys)
        else:
            is_m = maintenance(family_of(families, a.get('key')))
        out['maintenance' if is_m else 'foreground'].append(a)
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


CREATE_OPS = {'PutObject', 'CopyObject', 'UploadPart', 'UploadPartCopy', 'CompleteMultipartUpload'}
RETIRE_OPS = {'DeleteObject', 'DeleteObjects', 'AbortMultipartUpload', 'CompleteMultipartUpload'}


def _complete_size(inv: Inventory, a, cache: dict):
    """Object size and uncertainty from the listed parts; cached because the retire side may run first."""
    if a['seq'] not in cache:
        listed = (a.get('response_body') or {}).get('completed_parts')
        total, uncertain = 0, listed is None
        for p in listed or []:
            part = inv.incomplete.get((a.get('bucket'), a.get('key'), a.get('upload_id'), p['part_number']))
            if part is None or part['uncertain']:
                uncertain = True
            else:
                total += part['size']
        cache[a['seq']] = (total, uncertain)
    return cache[a['seq']]


def _create(inv: Inventory, a, t, families, cache, unknown):
    op, b, k = a['op'], a.get('bucket'), a.get('key')
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
        if (a.get('response_body') or {}).get('complete_error'):
            return
        size, uncertain = _complete_size(inv, a, cache)
        inv.put_object((b, k), size, t, family_of(families, k), uncertain)
    if unknown and (b, k) in inv.objects and op in ('PutObject', 'CopyObject', 'CompleteMultipartUpload'):
        inv.objects[(b, k)]['outcome_unknown'] = True


def _retire(inv: Inventory, a, t, cache):
    op, b, k = a['op'], a.get('bucket'), a.get('key')
    if op == 'AbortMultipartUpload':
        inv.retire_parts(b, k, a.get('upload_id'), t)
    elif op == 'CompleteMultipartUpload':
        if not (a.get('response_body') or {}).get('complete_error'):
            _complete_size(inv, a, cache)
            inv.retire_parts(b, k, a.get('upload_id'), t)
    elif op == 'DeleteObject':
        inv.delete_object((b, k), t)
    elif op == 'DeleteObjects':
        # Quiet mode lists only failures in the response; the proxy merges both sides into `deleted`/`code`.
        for r in (a.get('response_body') or {}).get('delete_results') or []:
            if r.get('deleted') or not r.get('code'):
                inv.delete_object((b, r['key']), t)


def _end(a):
    return a['t_end_us'] if a.get('t_end_us') is not None else a['t_start_us']


def replay(attempts: list, initial: Inventory, families: list) -> Bounds:
    """Replay the ledger twice into `Bounds(lower, upper)`, the byte-hours-minimising and -maximising readings.

    Every mutation has a creation side (put, copy, part upload, the object a Complete makes) and a
    retirement side (delete, abort, the parts a Complete retires). A Complete has both.
    - `lower`: creations at `t_end_us`, only when the outcome is ok; retirements at `t_start_us`,
      also when the outcome is `unknown`.
    - `upper`: creations at `t_start_us`, also when `unknown`; retirements at `t_end_us`, only when ok.
    `error`, `not_sent` and `rejected` outcomes change nothing. Objects made by an `unknown` attempt carry
    `outcome_unknown`. `completed_parts` is parsed by the proxy from the Complete request body, so it is
    available even when the response was not read.
    """
    plans = (
        (('ok',), ('ok', 'unknown'), _end, lambda a: a['t_start_us'], False),
        (('ok', 'unknown'), ('ok',), lambda a: a['t_start_us'], _end, True),
    )
    bounds = []
    for create_on, retire_on, create_t, retire_t, mark_unknown in plans:
        inv = copy.deepcopy(initial)
        if not inv.transitions:
            inv.seed_transitions()
        cache: dict = {}
        events = []
        for a in attempts:
            out = a['upstream']['outcome']
            if a['op'] in CREATE_OPS and out in create_on:
                events.append((create_t(a), a['seq'], 0, a))
            if a['op'] in RETIRE_OPS and out in retire_on:
                events.append((retire_t(a), a['seq'], 1, a))
        events.sort(key=lambda e: e[:3])
        for t, _, kind, a in events:
            if kind == 0:
                _create(inv, a, t, families, cache, mark_unknown and a['upstream']['outcome'] == 'unknown')
            else:
                _retire(inv, a, t, cache)
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
    """Byte-hours per category inside [t0_us, t1_us], clipped to the window.

    `lower` and `upper` are the per-category minimum and maximum of the two replays, so `lower <= upper` holds
    even when overlapping operations on one identity reorder differently in the two readings.
    Objects of unknown size land in `uncertain` with size 0.
    """
    a, b = _integrate(bounds.lower, t0_us, t1_us), _integrate(bounds.upper, t0_us, t1_us)
    cats = set(a) | set(b) | {'incomplete', 'uncertain'}
    return {c: {'lower': min(a.get(c, 0.0), b.get(c, 0.0)), 'upper': max(a.get(c, 0.0), b.get(c, 0.0))}
            for c in sorted(cats)}


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
