"""EBS demand fit (R1): coefficients from `local` records of `io-sizes` and natural-merge cards.

usage: python3 -m yardstick.cost.ebs_fit <measurements.jsonl>...   (prints the fit as JSON)
       python3 -m yardstick.cost.ebs_fit --holdout <experiment_id> <measurements.jsonl>...
           (checks the hold-out's pooled `io_bytes_per_op` and `io_request_size_distribution` against the fit;
           exit 1 if a key fails)

Rules, stated once so the fit and the hold-out predictions use the same ones:
- a per-record value is pooled by the mean over records; its spread is the largest |record - mean|;
- a hold-out passes when |value - fit| <= 2 x spread + 10 % of fit + 0.005 (scalars) or TV <= 2 x the largest fit TV
  + 0.05 (distributions); the 0.005 floor keeps near-zero terms (writes of a scan) from failing on baseline noise;
- `ebs_ios_per_mib` is the per-key upper envelope (largest record value): the first hold-out (types-holdout) showed
  that I/Os per MiB grow with the number of files per part, so a mean of narrow schemas under-counts; a hold-out of
  the envelope passes when its value <= 1.1 x envelope;
- the peak factor uses r1's 1-s `write_bytes` rate from the first sample to the last merge (quiet cutoff minus the
  quiet period), net of the idle card's median r1 rate.
"""
from __future__ import annotations

import json
import statistics
import sys

from .cards.io_sizes import DIRECTIONS, PHASE_OPS, SIZE_BUCKETS, tv_distance
from .localio import peak_factor

SCALAR_SLACK = (2.0, 0.10, 0.005)  # (x spread, + fraction of the fit value, + absolute floor)
ENVELOPE_SLACK = 1.10
TV_SLACK = (2.0, 0.05)


FIT_IO = {'io-sizes.narrow-compact', 'io-sizes.narrow-wide', 'io-sizes.types-holdout'}


def load(paths) -> list:
    out = []
    for path in paths:
        with open(path) as f:
            out += [json.loads(line) for line in f if line.strip()]
    return out


def ok(records, experiment_ids) -> list:
    return [r for r in records if r['status'] == 'ok' and r['experiment_id'] in experiment_ids
            and r['policy'] == 'local']


def pooled(values: dict) -> dict:
    """`values`: measurement id -> number. Mean, spread and the per-record residuals."""
    if not values:
        raise ValueError('no records to pool')
    mean = statistics.fmean(values.values())
    return {'value': mean, 'spread': max(abs(v - mean) for v in values.values()), 'max': max(values.values()),
            'residuals': {k: v - mean for k, v in sorted(values.items())}}


def pooled_distribution(values: dict) -> dict:
    labels = [label for label, _ in SIZE_BUCKETS]
    mean = {b: statistics.fmean(d[b] for d in values.values()) for b in labels}
    tvs = {k: tv_distance(d, mean) for k, d in sorted(values.items())}
    return {'value': mean, 'max_tv': max(tvs.values()), 'tv': tvs}


def idle_baseline(records) -> dict:
    rows = ok(records, {'io-sizes.idle'})
    if not rows:
        raise ValueError('no io-sizes.idle record: the baseline is unknown')
    obs = [r['observations']['baseline'] for r in rows]
    return {'r1_write_bps': statistics.median(o['bps']['r1']['write_bytes'] for o in obs),
            'r2_write_bps': statistics.median(o['bps']['r2']['write_bytes'] for o in obs),
            'r1_minus_r2_write_bps': statistics.median(o['r1_minus_r2_write_bps'] for o in obs),
            'traced_keeper_write_bps': statistics.median(o['traced_keeper_write_bps'] for o in obs),
            'measurement_ids': [r['measurement_id'] for r in rows]}


def demand_fit(records, experiment_ids) -> dict:
    """`io_bytes_per_op`, `ebs_ios_per_mib` and `io_request_size_distribution` per (op, direction) present."""
    rows = ok(records, experiment_ids)
    if not rows:
        raise ValueError(f'no ok record of {sorted(experiment_ids)}')
    out = {'io_bytes_per_op': {}, 'ebs_ios_per_mib': {}, 'io_request_size_distribution': {},
           'measurement_ids': [r['measurement_id'] for r in rows]}
    for op in PHASE_OPS.values():
        for d in DIRECTIONS:
            key = f'{op}.{d}'
            per = {r['measurement_id']: r['observations'].get(f'{op}.io_bytes.{d}') for r in rows}
            per = {k: v for k, v in per.items() if v is not None}
            if not per:
                continue
            out['io_bytes_per_op'][key] = pooled(per)
            ios = {r['measurement_id']: r['observations'][f'{op}.ebs_ios_per_mib.{d}'] for r in rows
                   if f'{op}.ebs_ios_per_mib.{d}' in r['observations']}
            dist = {r['measurement_id']: r['observations'][f'{op}.request_size_distribution.{d}'] for r in rows
                    if f'{op}.request_size_distribution.{d}' in r['observations']}
            if len(ios) == len(per):  # a direction some record never moved bytes in has no I/O size
                out['ebs_ios_per_mib'][key] = pooled(ios)
                out['io_request_size_distribution'][key] = pooled_distribution(dist)
    return out


def active_window(record, quiet_s: float) -> tuple:
    """`(t0_us, t1_us)` from the window start to the last merge of a natural-merge record."""
    cutoff = record['observations'].get('quiet_cutoff_us')
    if cutoff is None:
        raise ValueError(f'{record["measurement_id"]}: no quiet cutoff, the active period is unknown')
    return record['window']['t_start_us'], cutoff - int(quiet_s * 1e6)


def peak_factors(records, experiment_ids, baseline_bps: float, quiet_s: dict) -> dict:
    """measurement id -> `peak_factor` result of each natural-merge record; `quiet_s` per experiment id."""
    out = {}
    for r in ok(records, experiment_ids):
        t0, t1 = active_window(r, quiet_s[r['experiment_id']])
        out[r['measurement_id']] = peak_factor(r['observations']['proc_io_series']['r1'], t0, t1, baseline_bps)
    return out


def scalar_check(value: float, fit: dict) -> dict:
    slack = SCALAR_SLACK[0] * fit['spread'] + SCALAR_SLACK[1] * abs(fit['value']) + SCALAR_SLACK[2]
    return {'value': value, 'fit': fit['value'], 'slack': slack, 'ok': abs(value - fit['value']) <= slack}


def envelope_check(value: float, fit: dict) -> dict:
    limit = ENVELOPE_SLACK * fit['max']
    return {'value': value, 'envelope': fit['max'], 'limit': limit, 'ok': value <= limit}


def tv_check(value: dict, fit: dict) -> dict:
    limit = TV_SLACK[0] * fit['max_tv'] + TV_SLACK[1]
    tv = tv_distance(value, fit['value'])
    return {'tv': tv, 'limit': limit, 'ok': tv <= limit}


OPS = {'mutation': 'mutations', 'drop': 'deletes', 'startup': 'recovery'}
OPS_FIT = {'mutation.compact-update-one-column', 'mutation.wide-update-one-column', 'drop.partition', 'startup.intact'}
OPS_HOLD = {'mutation.compact-30k', 'drop.partition-2', 'startup.small'}
OPS_SLACK = (2.0, 0.15)  # (x spread, + fraction of the fit), fixed in the ops predictions before the run


def op_units(record) -> float:
    """Logical units of one ops record: source rows of the mutated parts, dropped parts, or one restart."""
    obs, card = record['observations'], record['card']
    if card == 'mutation':
        return sum(p['rows'] for p in obs['parts_before'] if p['active'])
    if card == 'drop':
        before = {p['name'] for p in obs['parts_before'] if p['active']}
        after = {p['name'] for p in obs['parts_after'] if p['active']}
        return len(before - after)
    if card == 'startup':
        return 1
    raise ValueError(f'{record["measurement_id"]}: not an ops card')


def ops_values(records, experiment_ids, baseline_bps: float) -> dict:
    """op -> measurement id -> net r1 write_bytes per unit (`op_units`), net of the idle r1 rate x wall time."""
    out = {}
    for r in ok(records, experiment_ids):
        units = op_units(r)
        if units <= 0:
            raise ValueError(f'{r["measurement_id"]}: no logical units')
        net = r['proc_io']['r1']['write_bytes'] - baseline_bps * r['wall_s']
        out.setdefault(OPS[r['card']], {})[r['measurement_id']] = net / units
    return out


def ops_check(value: float, fit: dict) -> dict:
    slack = OPS_SLACK[0] * fit['spread'] + OPS_SLACK[1] * abs(fit['value'])
    return {'value': value, 'fit': fit['value'], 'slack': slack, 'ok': abs(value - fit['value']) <= slack}


def holdout_checks(records, holdout_id: str) -> dict:
    """`coefficient.key` -> check of the hold-out's pooled value against the fit of `FIT_IO`."""
    fit = demand_fit(records, FIT_IO)
    held = demand_fit(records, {holdout_id})
    out = {}
    for key, f in fit['io_bytes_per_op'].items():
        if key in held['io_bytes_per_op']:
            out[f'io_bytes_per_op.{key}'] = scalar_check(held['io_bytes_per_op'][key]['value'], f)
    for key, f in fit['io_request_size_distribution'].items():
        if key in held['io_request_size_distribution']:
            out[f'io_request_size_distribution.{key}'] = tv_check(held['io_request_size_distribution'][key]['value'], f)
    if not out:
        raise ValueError(f'{holdout_id}: no key in common with the fit')
    return out


def main(argv) -> int:
    if argv[:1] == ['--holdout']:
        if len(argv) < 3:
            print('usage: python3 -m yardstick.cost.ebs_fit --holdout <experiment_id> <measurements.jsonl>...',
                  file=sys.stderr)
            return 2
        checks = holdout_checks(load(argv[2:]), argv[1])
        print(json.dumps(checks, indent=1, sort_keys=True))
        return 0 if all(c['ok'] for c in checks.values()) else 1
    records = load(argv)
    base = idle_baseline(records)
    fit = demand_fit(records, FIT_IO)
    print(json.dumps({'baseline': base, 'demand': fit}, indent=1, sort_keys=True))
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
