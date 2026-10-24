"""Runtime estimate for the planned cost-rig cases, and the cuts that bring it under a time budget."""
from __future__ import annotations

import fnmatch
import re
import statistics
from dataclasses import dataclass, field

RERUN_RESERVE = 1.25


@dataclass
class Estimate:
    cases: list = field(default_factory=list)
    p1_hours: float = 0.0
    total_hours: float = 0.0
    budget_hours: float = 72
    cuts: list = field(default_factory=list)
    reached: bool = True


def _median(values: list):
    return statistics.median(values) if values else None


def _wall_by(records: list, pred) -> list:
    return [r['wall_s'] for r in records if r.get('status') == 'ok' and 'wall_s' in r and pred(r)]


def _pass_seconds(card: dict, case: dict, records: list, family_cards: dict, sample_of: dict):
    """(seconds, basis): the card's own median, else its family's, else the sampled case's, else the global median."""
    own = _median(_wall_by(records, lambda r: r['card'] == card['name']))
    if own is not None:
        return own, 'card'
    same_family = family_cards.get(card['family'], set())
    fam = _median(_wall_by(records, lambda r: r['card'] in same_family))
    if fam is not None:
        return fam, 'family'
    sample_case = sample_of.get(card['family'])
    sampled = _median(_wall_by(records, lambda r: r['case_id'] == sample_case)) if sample_case else None
    if sampled is not None:
        return sampled, 'sample'
    return _median(_wall_by(records, lambda r: True)) or 0, 'global'


def estimate(measurements: list, coverage: dict, registry: dict | None, preset_ids_by_coefficient: dict | None,
             budget_hours: float = 72, sample_of: dict | None = None) -> Estimate:
    sample_of = sample_of or {}
    family_cards: dict = {}
    for card in coverage['cards']:
        family_cards.setdefault(card['family'], set()).add(card['name'])
    cleanup = _median([r['cleanup_s'] for r in measurements if 'cleanup_s' in r]) or 0
    est = Estimate(budget_hours=budget_hours)
    for card in coverage['cards']:
        for case in card['cases']:
            if case['status'] != 'planned':
                continue
            measured, basis = _pass_seconds(card, case, measurements, family_cards, sample_of)
            # Probe-sized medians say little about a case that runs for hours: the case's declared duration is
            # the floor of every pass.
            per_pass = max(measured, case['duration_s'])
            passes = len(case['policies']) * case['repeats']
            est.cases.append({
                'experiment_id': f'{card["name"]}.{case["case_id"]}', 'card': card['name'], 'case_id': case['case_id'],
                'family': card['family'], 'priority': case['priority'], 'passes': passes, 'pass_seconds': per_pass,
                'basis': basis, 'estimated_from_global': basis == 'global', 'measured_seconds': measured,
                'floor_applied': case['duration_s'] > measured, 'seconds': passes * (per_pass + cleanup)})
    est.p1_hours = sum(c['seconds'] for c in est.cases if c['priority'] == 'P1') * RERUN_RESERVE / 3600
    est.total_hours = sum(c['seconds'] for c in est.cases) * RERUN_RESERVE / 3600
    _plan_cuts(est, coverage, registry, preset_ids_by_coefficient)
    return est


def _cut_groups(cases: list) -> list:
    """(label, cases) in cut order: P2 families largest first, then zero-copy replication, then the types cases."""
    p2: dict = {}
    for c in cases:
        if c['priority'] == 'P2':
            p2.setdefault(c['family'], []).append(c)
    groups = [(f'P2 family {fam}', members)
              for fam, members in sorted(p2.items(), key=lambda kv: -sum(c['seconds'] for c in kv[1]))]
    for label, pattern in (('replication-zero-copy*', 'replication-zero-copy*'), ('types-*', 'types-*')):
        groups.append((label, [c for c in cases if fnmatch.fnmatch(c['card'], pattern)
                               or fnmatch.fnmatch(c['case_id'], pattern)]))
    return groups


def _plan_cuts(est: Estimate, coverage: dict, registry: dict | None, presets: dict | None) -> None:
    remaining = est.total_hours
    cut_ids: set = set()
    for label, members in _cut_groups(est.cases):
        if remaining <= est.budget_hours:
            break
        members = [c for c in members if c['experiment_id'] not in cut_ids]
        if not members:
            continue
        ids = [c['experiment_id'] for c in members]
        cut_ids.update(ids)
        remaining -= sum(c['seconds'] for c in members) * RERUN_RESERVE / 3600
        coefs = sorted({e['coefficient'] for e in coverage['evidence'] if e['experiment_id'] in ids})
        quantities = sorted(q['id'] for q in (registry or {}).get('quantities', []) if q.get('coef') in coefs)
        est.cuts.append({
            'label': label, 'experiments': ids, 'remaining_hours': remaining, 'coefficients': coefs,
            'quantities': quantities,
            'presets': 'unknown' if presets is None else sorted({p for c in coefs for p in presets.get(c, [])})})
    est.reached = remaining <= est.budget_hours


def presets_by_coefficient(js_text: str, registry: dict | None) -> dict:
    """coefficient -> ids of presets whose block names a registry quantity of that coefficient."""
    start = re.search(r'SCC\.presets\.SCENARIOS\s*=\s*\{', js_text)
    if not start:
        return {}
    blocks, depth, i, key = {}, 1, start.end(), None
    body_start = None
    # Top-level keys sit at depth 1; their value block runs to the matching brace.
    while i < len(js_text) and depth > 0:
        ch = js_text[i]
        if depth == 1 and key is None:
            m = re.compile(r'\s*[\'"]?([A-Za-z_][\w.-]*)[\'"]?\s*:\s*\{').match(js_text, i)
            if m:
                key, body_start, depth, i = m.group(1), m.end(), 2, m.end()
                continue
        if ch == '{':
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 1 and key is not None:
                blocks[key], key = js_text[body_start:i], None
        i += 1
    out: dict = {}
    for q in (registry or {}).get('quantities', []):
        if not q.get('coef'):
            continue
        for preset, text in blocks.items():
            if re.search(rf'\b{re.escape(q["id"])}\b', text):
                out.setdefault(q['coef'], set()).add(preset)
    return {coef: sorted(ids) for coef, ids in out.items()}


def render(est: Estimate) -> str:
    lines = [f'P1: {est.p1_hours:.1f} h   P1+P2: {est.total_hours:.1f} h   budget: {est.budget_hours:g} h '
             f'(includes the {RERUN_RESERVE}x rerun reserve)']
    for c in est.cases:
        flag = '  (estimated from global median)' if c['estimated_from_global'] else ''
        floor = ' duration floor' if c['floor_applied'] else ''
        lines.append(f'  {c["experiment_id"]:<40} {c["priority"]} {c["passes"]:>3} passes x {c["pass_seconds"]:.0f} s '
                     f'[{c["basis"]}{floor}]{flag}')
    if not est.cuts:
        lines.append('within budget: no cuts')
    for c in est.cuts:
        lines.append(f'cut {c["label"]}: {len(c["experiments"])} cases, then {c["remaining_hours"]:.1f} h')
        lines.append(f'    coefficients: {", ".join(c["coefficients"]) or "-"}')
        lines.append(f'    quantities: {", ".join(c["quantities"]) or "-"}')
        presets = c['presets'] if c['presets'] == 'unknown' else (", ".join(c['presets']) or '-')
        lines.append(f'    presets: {presets}')
    if not est.reached:
        lines.append(f'budget NOT reached after all cuts: {est.cuts[-1]["remaining_hours"]:.1f} h left'
                     if est.cuts else 'budget NOT reached')
    return '\n'.join(lines)
