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
                'delete_calls', 'delete_keys', 'accepted_dropped', 'upstream_4xx')
# Signature and SDK bookkeeping change on every retry; everything else under x-amz-* is part of the request's meaning.
VOLATILE_HEADERS = {'x-amz-date', 'x-amz-security-token', 'x-amz-content-sha256', 'x-amz-user-agent',
                    'amz-sdk-invocation-id', 'amz-sdk-request'}
# Presigned-URL signature parameters; every other query parameter (pagination, uploadId, ...) is request meaning.
VOLATILE_QUERY = {'x-amz-date', 'x-amz-signature', 'x-amz-credential', 'x-amz-security-token', 'x-amz-expires',
                  'x-amz-signedheaders', 'x-amz-algorithm'}
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


def batch_keys(a) -> list:
    """Keys a `DeleteObjects` attempt names: per-key results when the response was parsed, else the request's."""
    results = (a.get('response_body') or {}).get('delete_results')
    if results is not None:
        return [r['key'] for r in results]
    return list(a.get('delete_keys') or [])


def is_maintenance(a, families: list) -> bool:
    """True when the attempt's key matches a family flagged `maintenance`.

    A `DeleteObjects` attempt has no key (the proxy records `""`): it is maintenance iff it names at least one key
    and every key matches a maintenance family. Other keyless attempts (bucket-level LIST) are not.
    """
    def flagged(f):
        return f is not None and bool(f.get('maintenance'))

    if not a.get('key') and a['op'] == 'DeleteObjects':
        keys = batch_keys(a)
        return bool(keys) and all(flagged(family_of(families, k)) for k in keys)
    return flagged(family_of(families, a.get('key')))


def classify(attempts: list, families: list) -> dict:
    """Split attempts into `foreground`, `maintenance` (workload callers, `is_maintenance`) and `rig`.

    Key families say what an object is, not who caused a request: cards that need request origin attribute it
    causally (see measurement-contract.md).
    """
    out = {'foreground': [], 'maintenance': [], 'rig': []}
    for a in attempts:
        if a['caller'] == 'rig':
            out['rig'].append(a)
            continue
        out['maintenance' if is_maintenance(a, families) else 'foreground'].append(a)
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
        if up == 'error' and 400 <= (a['upstream'].get('http_status') or 0) < 500:
            c['upstream_4xx'] += 1  # billed unless free-listed; 5XX and unsent requests are not
        if up == 'ok' and a['downstream']['outcome'] == 'dropped':
            # The store did the work (and bills it); only the reply to the caller was lost.
            c['accepted_dropped'] += 1
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
                        if k in ('if-match', 'if-none-match', 'range')
                        or (k.startswith('x-amz-') and k not in VOLATILE_HEADERS)))
    # A different range or listing page is a different request, not a retry of the failed one.
    query = tuple(sorted((k, v) for k, v in (a.get('query') or {}).items() if k.lower() not in VOLATILE_QUERY))
    return (a['caller'], a['op'], a['store_id'], a.get('bucket'), a.get('key'), a.get('upload_id'),
            a.get('part_number'), a.get('payload_sha256'), cond, query)


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
    """End-state inventories of the two replays, plus the per-identity transitions `byte_hours` bounds.

    `mutations` maps an identity to its transitions in seq order: `(t_start_us, t_end_us, certain, state)`,
    where `state` is `(size, category, uncertain)` or None (absent). `cats` holds `reclassify` calls.
    """

    def __init__(self, lower: Inventory, upper: Inventory, mutations=None, creators=None):
        self.lower = lower
        self.upper = upper
        self.mutations = mutations if mutations is not None else {}
        self.creators = creators if creators is not None else {}  # identity -> caller of its first creation
        self.cats: dict = defaultdict(list)
        self.reclassified: list = []  # the calls, so a record can be replayed from its raw ledger slice

    def reclassify(self, identities, category, t_us, evidence):
        identities = list(identities)
        for inv in (self.lower, self.upper):
            inv.reclassify(identities, category, t_us, evidence)
        for ident in identities:
            self.cats[ident].append((t_us, category))
        self.reclassified.append({'category': category, 't_us': t_us, 'evidence': evidence,
                                  'keys': [list(i) for i in identities]})


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
        results = (a.get('response_body') or {}).get('delete_results')
        if results is None:
            # Response lost or unparsed: the store may have deleted every requested key.
            results = [{'key': k, 'deleted': True, 'code': None} for k in a.get('delete_keys') or []]
        for r in results:
            if r.get('deleted') or not r.get('code'):
                inv.delete_object((b, r['key']), t)


def _end(a):
    return a['t_end_us'] if a.get('t_end_us') is not None else a['t_start_us']


def replay(attempts: list, initial: Inventory, families: list) -> Bounds:
    """Replay the ledger into `Bounds`: two end-state inventories and the transitions `byte_hours` integrates.

    Every mutation has a creation side (put, copy, part upload, the object a Complete makes) and a
    retirement side (delete, abort, the parts a Complete retires). A Complete has both.
    End states: each identity ends in its smallest possible state in `lower` and its largest in `upper`, over
    every order overlapping attempts could have landed in (`_final_states`); a creation counts in `lower` only
    when ok, a retirement in `upper` only when ok. `error`, `not_sent` and `rejected` outcomes change nothing. Objects made by
    an `unknown` attempt carry `outcome_unknown`. `completed_parts` is parsed by the proxy from the Complete
    request body, and `delete_keys` from the DeleteObjects request body, so both exist without a response.
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
    mutations, creators = _mutations(attempts, initial, families)
    out = Bounds(*bounds, mutations=mutations, creators=creators)
    _final_states(out, families)
    return out


def _mutations(attempts: list, initial: Inventory, families: list) -> tuple:
    """Per-identity transitions in seq order, and the caller that first created each identity. Sizes come from
    one seq-order pass that applies ok and unknown attempts, so a copy or a Complete reads the source and part
    sizes the request could have seen."""
    inv = copy.deepcopy(initial)
    inv.transitions = []
    out: dict = defaultdict(list)
    creators: dict = {}
    for ident, o in initial.objects.items():
        out[ident].append((o['created_us'], o['created_us'], True, (o['size'], o['category'], o['uncertain'])))
    for ident, p in initial.incomplete.items():
        out[ident].append((p['t_us'], p['t_us'], True, (p['size'], 'incomplete', p.get('uncertain', False))))
    cache: dict = {}
    for a in sorted(attempts, key=lambda x: x['seq']):
        outcome = a['upstream']['outcome']
        if outcome not in ('ok', 'unknown') or a['op'] not in CREATE_OPS | RETIRE_OPS:
            continue
        before = len(inv.transitions)
        if a['op'] in CREATE_OPS:
            _create(inv, a, a['t_start_us'], families, cache, False)
        if a['op'] in RETIRE_OPS:
            _retire(inv, a, a['t_start_us'], cache)
        for tr in inv.transitions[before:]:
            state = (tr['size'], tr['category'], tr['uncertain']) if tr['action'] == 'set' else None
            out[tr['id']].append((a['t_start_us'], _end(a), outcome == 'ok', state))
            if state is not None:
                creators.setdefault(tr['id'], a['caller'])
    return out, creators


def _possible(transitions: list, k: int, t: float) -> bool:
    """Can the state after transition `k` (-1: absent) hold at `t` in some feasible schedule?

    Each transition takes effect at one instant inside its `[t_start_us, t_end_us]`, in any order those instants
    allow (seq order is start order, not effect order); an `unknown` one may not take effect at all. The state of
    the last effect at or before `t` holds. So `k` can hold when it may have taken effect by `t`, and every other
    certain transition may have taken effect before it or may still be pending.
    """
    if k >= 0 and transitions[k][0] > t:
        return False
    end_k = transitions[k][1] if k >= 0 else None
    for j, (ts, te, certain, _) in enumerate(transitions):
        if j == k or not certain:
            continue
        before = end_k is not None and ts <= min(t, end_k)
        if not (before or te > t):
            return False
    return True


def _landed_floor(transitions: list) -> float:
    """Latest start among the certain transitions, or -inf: `_possible` at the end of time in closed form."""
    return max((ts for ts, _, certain, _ in transitions if certain), default=float('-inf'))


def _envelope(transitions: list, cats: list, t0: int, t1: int, totals: dict, sign: int) -> None:
    """Add the pointwise minimum (`sign` -1) or maximum (+1) over the possible states, per category, to `totals`.

    Every feasible schedule passes through possible states only (`_possible`), so integrating their minimum and
    maximum bounds its byte-hours, whatever order overlapping transitions land in.

    Sweep form of `_possible`, linear in the transitions plus the overlap at each point: let `M(t)` be the latest
    start among certain transitions that must have landed by `t` (`t_end <= t`). The absent initial state is
    possible iff there is none; transition `k` iff `t_start_k <= t` and `t_end_k >= M(t)`. `M` never decreases, so
    a transition that falls below it is dropped for good.
    """
    order = sorted(range(len(transitions)), key=lambda k: transitions[k][0])
    by_end = sorted((tr[1], tr[0]) for tr in transitions if tr[2])
    points = sorted({t0, t1} | {t for tr in transitions for t in tr[:2] if t0 < t < t1}
                    | {tc for tc, _ in cats if t0 < tc < t1})
    live: list = []
    started = ended = 0
    floor = float('-inf')
    for lo, hi in zip(points, points[1:]):
        if hi <= lo:
            continue
        mid = (lo + hi) / 2
        while started < len(order) and transitions[order[started]][0] <= mid:
            live.append(order[started])
            started += 1
        while ended < len(by_end) and by_end[ended][0] <= mid:
            floor = max(floor, by_end[ended][1])
            ended += 1
        live = [k for k in live if transitions[k][1] >= floor]
        possible = len(live) + (1 if floor == float('-inf') else 0)
        values: dict = defaultdict(list)
        for k in live:
            state = transitions[k][3]
            if state is None:
                continue
            size, category, uncertain = state
            for tc, c in cats:
                if transitions[k][0] <= tc <= mid:
                    category = c
            values['uncertain' if uncertain else category].append(size)
        for category, sizes in values.items():
            # A category the possible states do not all fill can hold zero bytes at this instant.
            pick = max(sizes) if sign > 0 else (min(sizes) if len(sizes) == possible else 0)
            totals[category] += pick * (hi - lo) / US_PER_HOUR


def _final_states(bounds, families) -> None:
    """Set each identity's end state to the smallest possible one in `lower` and the largest in `upper`.

    Size ties break toward absent in `lower` and toward present in `upper`: a zero-byte object and its deletion
    both hold 0 bytes, but only `upper` may claim the object still exists.
    """
    for ident, transitions in bounds.mutations.items():
        floor = _landed_floor(transitions)
        finals = ([-1] if floor == float('-inf') else []) + [k for k, tr in enumerate(transitions) if tr[1] >= floor]
        options = [(0, 0, k) if k < 0 or transitions[k][3] is None else (transitions[k][3][0], 1, k) for k in finals]
        for inv, (_, _, k) in ((bounds.lower, min(options)), (bounds.upper, max(options))):
            table = inv.objects if len(ident) == 2 else inv.incomplete
            if k < 0 or transitions[k][3] is None:
                table.pop(ident, None)
                continue
            size, category, uncertain = transitions[k][3]
            entry = table.get(ident)
            if entry is None:
                ts = transitions[k][0]
                family = family_of(families, ident[1])
                entry = ({'created_us': ts, 'family': family['name'] if family else None, 'category': category}
                         if len(ident) == 2 else {'t_us': ts})
                table[ident] = entry
            entry.update(size=size, uncertain=uncertain)
            if len(ident) == 2:
                entry['category'] = category  # replay runs before any reclassification
            if len(ident) == 2 and not transitions[k][2]:
                entry['outcome_unknown'] = True


def byte_hours(bounds: Bounds, t0_us: int, t1_us: int) -> dict:
    """Byte-hours per category inside [t0_us, t1_us], as `{lower, upper}`.

    Each transition takes effect at one instant inside `[t_start_us, t_end_us]`, in any order those instants
    allow, so a shrink counts from `t_start_us` in `lower` and a growth from `t_end_us`, and overlapping
    transitions on one identity are bounded over every feasible order, with no cap on how many overlap
    (`_possible`). The bound is pointwise, so it can be looser than the best schedule. Objects of unknown size
    land in `uncertain` with size 0.
    """
    lower: dict = defaultdict(float)
    upper: dict = defaultdict(float)
    for ident, transitions in bounds.mutations.items():
        cats = sorted(bounds.cats.get(ident, []))
        _envelope(transitions, cats, t0_us, t1_us, lower, -1)
        _envelope(transitions, cats, t0_us, t1_us, upper, +1)
    cats = set(lower) | set(upper) | {'incomplete', 'uncertain'}
    return {c: {'lower': lower.get(c, 0.0), 'upper': upper.get(c, 0.0)} for c in sorted(cats)}


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
