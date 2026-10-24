"""Recompute a measurement record's ledger counts and byte-hour bounds from its raw ledger slice.

`python3 -m yardstick.cost.reproduce <record.json> <ledger.jsonl[.gz]>` prints the differences and exits 1 if
there are any. The closure check runs it for every gate record, so an exported summary cannot drift from the rows
it came from. Foreground and maintenance are compared as their sum: the split can rest on evidence outside the
ledger (CAS GC round windows), the integrated usage cannot.
"""
from __future__ import annotations

import gzip
import json
import sys

from . import ledger as L

REL = 1e-9


def read(path) -> list:
    opener = gzip.open if str(path).endswith('.gz') else open
    with opener(path, 'rt', encoding='utf-8') as f:
        return [json.loads(line) for line in f if line.strip()]


def _merge(*sections) -> dict:
    out: dict = {}
    for sec in sections:
        for caller, ops in (sec or {}).items():
            for op, c in ops.items():
                t = out.setdefault(caller, {}).setdefault(op, dict.fromkeys(L.COUNT_FIELDS, 0))
                for k in L.COUNT_FIELDS:
                    t[k] += c.get(k, 0)
    return out


def _diff(name, want: dict, got: dict, out: list) -> None:
    for caller in sorted(set(want) | set(got)):
        for op in sorted(set(want.get(caller, {})) | set(got.get(caller, {}))):
            w, g = want.get(caller, {}).get(op, {}), got.get(caller, {}).get(op, {})
            for k in L.COUNT_FIELDS:
                if w.get(k, 0) != g.get(k, 0):
                    out.append(f'{name}: {caller} {op} {k} recorded {g.get(k, 0)}, replayed {w.get(k, 0)}')


def problems(record: dict, rows: list) -> list:
    from .cards import CARDS  # noqa: PLC0415 (only the card's key families are needed)
    rows = [dict(r) for r in rows]
    sliced = L.slice(rows, {role: f'{role}@{record["measurement_id"]}' for role in L.MARK_ROLES})
    phase = sliced.attempts_by_phase
    work = lambda p: [a for a in phase[p] if a['caller'] != 'rig']  # noqa: E731
    out: list = []
    led = record['ledger']
    _diff('ledger.foreground+maintenance', L.summarize(work('window')), _merge(led['foreground'], led['maintenance']), out)
    _diff('ledger.setup', L.summarize(work('setup')), led['setup'], out)
    _diff('ledger.teardown', L.summarize(work('teardown')), led['teardown'], out)
    _diff('ledger.rig', L.summarize([a for a in phase['window'] if a['caller'] == 'rig']), {'rig': led['rig']}, out)
    families = CARDS[record['card']].inventory_families(None)
    attempts = sorted(phase['setup'] + phase['window'] + phase['teardown'], key=lambda a: a['seq'])
    bounds = L.replay(attempts, L.Inventory(), families)
    for rc in record['inventory'].get('reclassified', []):
        bounds.reclassify([tuple(k) for k in rc['keys']], rc['category'], rc['t_us'], rc['evidence'])
    w = record['window']
    replayed = L.byte_hours(bounds, w['t_start_us'], w['t_end_us'])
    recorded = record['inventory']['byte_hours']
    for cat in sorted(set(replayed) | set(recorded)):
        for side in ('lower', 'upper'):
            a = replayed.get(cat, {}).get(side, 0.0)
            b = recorded.get(cat, {}).get(side, 0.0)
            if abs(a - b) > REL * max(1.0, abs(a), abs(b)):
                out.append(f'inventory.byte_hours.{cat}.{side} recorded {b}, replayed {a}')
    return out


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) != 2:
        print('usage: python3 -m yardstick.cost.reproduce <record.json> <ledger.jsonl[.gz]>', file=sys.stderr)
        return 2
    with open(argv[0], encoding='utf-8') as f:
        record = json.load(f)
    found = problems(record, read(argv[1]))
    for p in found:
        print(p)
    return 1 if found else 0


if __name__ == '__main__':
    sys.exit(main())
