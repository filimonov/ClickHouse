"""Lifecycle cards (R5): part-type and multipart boundaries, small inserts, natural and forced merges, async inserts.

Every parameter comes from the manifest (`experiments/<experiment_id>.json` of the lifecycle report). Expected values
and acceptance bounds come from `predictions/<experiment_id>.<policy>.json`, found through `params.report_dir`; their
`source_refs` cite the pinned source. Workload SQL runs on the replica the manifest names in `params.replica`.
"""
from __future__ import annotations

import json
import math
import time
from concurrent.futures import ThreadPoolExecutor

from .base import DATABASE, POLL_S
from .probes import ManifestCard, _count, compare_ledger, register, window_rows

MIB = 1 << 20
LADDER_LEVELS = 12  # levels of the registry's write amplification ladder
AMPLIFICATION_MODELS = {}  # filled below: prediction `model` name -> function(final_ratios, fan_in)
# A fixed-width CODEC(NONE) column of a wide part: 8192-row granules (index_granularity) and a 25-byte frame per
# compressed block. A full UInt64 granule is 64 KiB = min_compress_block_size, so every granule closes its block.
GRANULE_ROWS = 8192
FRAME_BYTES = 25
MIN_COMPRESS_BLOCK = 65536
LARGE_PUT_BYTES = MIB  # part files below this are never near a multipart boundary
# Merge histograms: fan-in and result size in inserted-part units. Fixed here so fit and hold-out bin alike.
FAN_IN_BUCKETS = (('2', 2, 2), ('3', 3, 3), ('4', 4, 4), ('5', 5, 5), ('6', 6, 6), ('7-8', 7, 8), ('9-12', 9, 12),
                  ('13-20', 13, 20), ('21+', 21, math.inf))
RESULT_BUCKETS = (('2-4', 2, 4), ('5-8', 5, 8), ('9-16', 9, 16), ('17-32', 17, 32), ('33-64', 33, 64),
                  ('65-128', 65, 128), ('129-256', 129, 256), ('257-512', 257, 512), ('513+', 513, math.inf))
PART_COLUMNS = 'name, part_type, rows, level, data_uncompressed_bytes, active'
PART_LOG_COLUMNS = ('event_type, part_name, part_type, rows, size_in_bytes, bytes_uncompressed, merged_from, '
                    'toUnixTimestamp64Micro(event_time_microseconds) AS t_us')


# --- arithmetic the predictions rest on ------------------------------------------------------

def defaults(registry: dict) -> dict:
    return {q['id']: q['default'] for q in registry['quantities']}


def bin_bytes(rows: int, value_bytes: int = 8) -> int:
    """Bytes of a CODEC(NONE) fixed-width column file of a wide part holding `rows` values.

    Each full granule is one block (it reaches min_compress_block_size); a partial last granule is the final block.
    """
    if GRANULE_ROWS * value_bytes < MIN_COMPRESS_BLOCK:
        raise ValueError(f'{value_bytes}-byte values: a granule does not close a block, the block count is not exact')
    full, rest = divmod(rows, GRANULE_ROWS)
    return full * (GRANULE_ROWS * value_bytes + FRAME_BYTES) + (rest * value_bytes + FRAME_BYTES if rest else 0)


def rows_for_bin_bytes(target: int, value_bytes: int = 8) -> int:
    """Rows whose column file is exactly `target` bytes; ValueError when no row count gives that size."""
    block = GRANULE_ROWS * value_bytes + FRAME_BYTES
    if target % block == 0:
        return target // block * GRANULE_ROWS
    for full in range(target // block, -1, -1):
        rest = target - full * block - FRAME_BYTES
        if rest // value_bytes >= GRANULE_ROWS:
            break
        if rest > 0 and rest % value_bytes == 0:
            return full * GRANULE_ROWS + rest // value_bytes
    raise ValueError(f'no row count gives a {target}-byte column file of {value_bytes}-byte values')


def rows_below_bin_bytes(target: int, value_bytes: int = 8) -> int:
    """The largest row count whose column file is smaller than `target` bytes."""
    lo, hi = 0, target // value_bytes + 1
    while lo < hi:
        mid = (lo + hi + 1) // 2
        lo, hi = (mid, hi) if bin_bytes(mid, value_bytes) < target else (lo, mid - 1)
    return lo


def upload_requests(size: int, first: int, second: int, factor: int = 2, threshold: int = 500,
                    max_size: int = 5 << 30) -> tuple:
    """`('put', [size])` or `('multipart', part sizes)` that `WriteBufferFromS3` sends for one object.

    Buffers follow `ExpBufferAllocationPolicy`: the first holds `first` bytes, the second `second`, and every
    `threshold` buffers the size grows by `factor` up to `max_size`. An object that fits the first buffer, even
    exactly, is one PutObject.
    """
    if size <= first:
        return 'put', [size]
    parts, left, n, current = [], size, 0, 0
    while left > 0:
        n += 1
        if n == 1:
            current = first
        else:
            if n == 2:
                current = second
            if (n - 1) % threshold == 0:
                current = min(current * factor, max_size)
        parts.append(min(current, left))
        left -= parts[-1]
    return 'multipart', parts


def boundary_cases(registry: dict, row_bytes: int, value_bytes: int = 8) -> dict:
    """Row counts of the `boundaries` cases, placed relative to the pinned thresholds of `registry`.

    Part-type cases sit at 0.9, 1.0 and 1.1 of min_bytes_for_wide_part in uncompressed bytes of a `row_bytes`-wide
    row; the merge cases insert parts below the threshold whose sum crosses it. Multipart cases place one column file
    8 bytes below, exactly at, and 8 bytes past one second buffer beyond the first upload buffer.
    """
    d = defaults(registry)
    t, first, second = d['min_bytes_for_wide_part'], d['multipart_first_part_bytes'], d['multipart_next_part_bytes']
    rows_of = lambda num, den: -(-t * num // (den * row_bytes))  # noqa: E731 (ceil of t * num / den / row_bytes)
    return {
        'compact-below-bytes': {'inserts': 1, 'rows': t * 9 // (10 * row_bytes)},
        'wide-equal-bytes': {'inserts': 1, 'rows': rows_of(1, 1)},
        'wide-above-bytes': {'inserts': 1, 'rows': rows_of(11, 10)},
        'merge-cross': {'inserts': 2, 'rows': rows_of(6, 10)},
        'level-cross': {'inserts': 2, 'rows': rows_of(11, 10), 'min_level_for_wide_part': 1},
        'multipart-below': {'inserts': 1, 'rows': rows_below_bin_bytes(first, value_bytes)},
        'multipart-equal': {'inserts': 1, 'rows': rows_for_bin_bytes(first, value_bytes)},
        'multipart-above': {'inserts': 1, 'rows': rows_for_bin_bytes(first + second + value_bytes, value_bytes)},
    }


def squashed_parts(rows: int, min_rows: int, source_block_rows: int) -> list:
    """Rows of the parts an INSERT SELECT of `rows` rows creates: source blocks are joined until `min_rows` is
    reached (the byte minimum is not reached by the cases that use this), and a block is never split."""
    per = -(-min_rows // source_block_rows) * source_block_rows
    out = [per] * (rows // per)
    return out + ([rows % per] if rows % per else [])


def rewrite_levels(ratio: float, fan_in: float) -> float:
    """Times a byte is rewritten by merges while its part grows to `ratio` inserted parts, at effective `fan_in`.

    Whole levels up to the largest power of `fan_in` not above `ratio`, then the fraction of the next level, capped
    at 12 levels exactly as the registry ladder `write_amplification_rule` is.
    """
    if ratio <= 1:
        return 0.0
    level, size = 0, 1.0
    while size * fan_in <= ratio and level < LADDER_LEVELS:
        size *= fan_in
        level += 1
    if level == LADDER_LEVELS:
        return float(LADDER_LEVELS)
    return level + (ratio / size - 1) / (fan_in - 1)


def weighted_ratio(final_ratios: list) -> float:
    """Final part size, in inserted parts, that an inserted row ends up in on average: sum(r^2) / sum(r)."""
    return sum(r * r for r in final_ratios) / sum(final_ratios)


def amplification_rule(final_ratios: list, fan_in: float) -> float:
    """The registry rule `write_amplification_rule`: 1 + rewrite_levels at the row-weighted final part size."""
    return 1 + rewrite_levels(weighted_ratio(final_ratios), fan_in)


def amplification_model(final_ratios: list, fan_in: float) -> float:
    """Write amplification (bytes written / bytes inserted) predicted from the final parts' sizes in inserted-part
    units: each final part's share of the inserted rows was rewritten `rewrite_levels` times."""
    total = sum(final_ratios)
    return 1 + sum(r / total * rewrite_levels(r, fan_in) for r in final_ratios)


AMPLIFICATION_MODELS.update(rewrite_levels=amplification_model, rewrite_levels_at_weighted_ratio=amplification_rule)


def bucket(value: float, buckets: tuple) -> str:
    label = next((name for name, lo, hi in buckets if lo <= value <= hi), None)
    if label is None:
        raise ValueError(f'{value} is outside every bucket {[b[0] for b in buckets]}')
    return label


def distribution(values: list, buckets: tuple) -> dict:
    counts = dict.fromkeys((b[0] for b in buckets), 0)
    for v in values:
        counts[bucket(v, buckets)] += 1
    return {k: (n / len(values) if values else 0.0) for k, n in counts.items()}


def level_of(part_name: str) -> int:
    """`<partition>_<min>_<max>_<level>[_<mutation>]`; the partition id of these tables is `all`."""
    return int(part_name.split('_')[3])


def merge_observations(part_log: list, active_rows: list, rows_per_insert: int) -> dict:
    """Merge lifecycle observations from `system.part_log` rows (NewPart, MergeParts) of one table."""
    size = {r['part_name']: int(r['size_in_bytes']) for r in part_log}
    inserted = [r for r in part_log if r['event_type'] == 'NewPart']
    merges = sorted((r for r in part_log if r['event_type'] == 'MergeParts'), key=lambda r: int(r['t_us']))
    missing = sorted({s for m in merges for s in m['merged_from'] if s not in size})
    if missing:
        raise ValueError(f'merge sources without a part_log row: {missing}')
    listed = [{'fan_in': len(m['merged_from']), 'bytes_in': sum(size[s] for s in m['merged_from']),
               'bytes_out': int(m['size_in_bytes']), 'level': level_of(m['part_name']), 'rows': int(m['rows']),
               'part_type': m['part_type']} for m in merges]
    in_bytes, in_rows = sum(size[r['part_name']] for r in inserted), sum(int(r['rows']) for r in inserted)
    fan_ins = [m['fan_in'] for m in listed]
    results = [m['rows'] / rows_per_insert for m in listed]
    finals = [r / rows_per_insert for r in active_rows]
    return {
        'part_log_merges': listed, 'merges': len(listed), 'inserted_parts': len(inserted), 'inserted_rows': in_rows,
        'inserted_bytes': in_bytes, 'merged_bytes': sum(m['bytes_out'] for m in listed),
        'fan_in_mean': sum(fan_ins) / len(fan_ins) if fan_ins else 0.0,
        'fan_in_min': min(fan_ins, default=0), 'fan_in_max': max(fan_ins, default=0),
        'fan_in_distribution': distribution(fan_ins, FAN_IN_BUCKETS),
        'result_size_distribution': distribution(results, RESULT_BUCKETS),
        'max_level': max((m['level'] for m in listed), default=0),
        'active_parts': len(active_rows), 'final_size_ratios': sorted(finals, reverse=True),
        'amplification_bytes': (in_bytes + sum(m['bytes_out'] for m in listed)) / in_bytes if in_bytes else 0.0,
        'amplification_rows': (in_rows + sum(m['rows'] for m in listed)) / in_rows if in_rows else 0.0,
    }


def check_observation(want, got, observations: dict) -> tuple:
    """`(expected, actual)` of one predicted observation, equal exactly when the observation is accepted.

    `want` is a value (exact), `{min, max}` (a bound; either side may be absent), `{reference, max_tv}` (a
    distribution within total-variation distance of the reference) or `{model, fan_in, abs, rel}` (amplification
    against `AMPLIFICATION_MODELS[model]` over the record's `final_size_ratios`; an unknown model is an error).
    """
    if not isinstance(want, dict) or not ({'min', 'max', 'reference', 'model'} & set(want)):
        return want, got
    if 'reference' in want:
        ref = want['reference']
        tv = 0.5 * sum(abs(ref.get(k, 0.0) - got.get(k, 0.0)) for k in set(ref) | set(got))
        text = f'tv <= {want["max_tv"]}'
        return text, text if tv <= want['max_tv'] else f'tv {tv:.4f}'
    if 'model' in want:
        if want['model'] not in AMPLIFICATION_MODELS:
            raise ValueError(f'unknown amplification model {want["model"]!r}')
        predicted = AMPLIFICATION_MODELS[want['model']](observations['final_size_ratios'], want['fan_in'])
        slack = want['abs'] + want['rel'] * predicted
        text = f'{predicted:.4f} +- {slack:.4f}'
        return text, text if abs(got - predicted) <= slack else got
    lo, hi = want.get('min', -math.inf), want.get('max', math.inf)
    text = f'{want.get("min", "")}..{want.get("max", "")}'
    return text, text if got is not None and lo <= got <= hi else got


# --- cards --------------------------------------------------------------------------------------

class LifecycleCard(ManifestCard):
    """A manifest card whose workload runs on the replica `params.replica` names."""

    policies = ('s3', 's3plain', 'cas', 'local')

    def replica_client(self, ctx):
        name = self.p(ctx)['replica']
        index = int(name[1:]) if name[:1] == 'r' and name[1:].isdigit() else 0
        if not 1 <= index <= min(self.replicas, len(ctx.cluster.clients)):
            raise ValueError(f'{self.name}: params.replica {name!r} is not one of the card\'s {self.replicas} replica(s)')
        return ctx.cluster.clients[index - 1]

    def clients(self, ctx) -> list:
        return [self.replica_client(ctx)]

    def sql(self, ctx, template: str, **kw) -> str:
        p = self.p(ctx)
        return template.format(table=f'{DATABASE}.{self.table}', rows=p['rows'], seed=p['seed'], **kw)

    def inserts(self, ctx) -> int:
        # Lifecycle counts live in params; the manifest's scale applies to the insert count only.
        return max(1, round(ctx.manifest['scale'] * self.p(ctx)['inserts']))

    def insert(self, ctx, i: int) -> None:
        self.replica_client(ctx).query(self.sql(ctx, self.p(ctx)['insert'], i=i),
                                       settings=ctx.manifest['settings']['query'])

    def measured(self, ctx):
        n = self.inserts(ctx)
        for i in range(n):
            self.insert(ctx, i)
        return {'unit_count': n, 'effective_counts': {'inserts': n, 'rows': n * self.p(ctx)['rows']}, 'observations': {}}

    def rows_json(self, ctx, sql: str) -> list:
        text = self.replica_client(ctx).query(sql + ' FORMAT JSONEachRow')
        return [json.loads(line) for line in text.splitlines() if line.strip()]

    def where(self) -> str:
        return f"database = '{DATABASE}' AND table = '{self.table}'"

    def parts(self, ctx) -> list:
        """Every part of the table in `system.parts`, inactive ones included, as JSON-typed values.

        Ordered by level, then block range: inserted parts in insert order before their merge results (a name sort
        would put `all_1_8_1` before `all_2_2_0`).
        """
        rows = self.rows_json(ctx, f'SELECT {PART_COLUMNS} FROM system.parts WHERE {self.where()} '
                                   'ORDER BY level, min_block_number, max_block_number')
        return [{'name': r['name'], 'part_type': r['part_type'], 'rows': int(r['rows']), 'level': int(r['level']),
                 'data_uncompressed_bytes': int(r['data_uncompressed_bytes']), 'active': int(r['active'])}
                for r in rows]

    def part_log(self, ctx) -> list:
        return self.rows_json(ctx, f"SELECT {PART_LOG_COLUMNS} FROM system.part_log WHERE {self.where()} AND "
                                   "event_type IN ('NewPart', 'MergeParts') AND error = 0 ORDER BY t_us")

    def collect_layout(self, ctx) -> None:
        """Object keys of every part file, outdated parts included, from `system.remote_data_paths`.

        Called before the teardown: a merged-away part stays on disk for old_parts_lifetime, so its files are listed.
        """
        disk = ctx.policy.removesuffix('_cached')
        out: dict = {}
        if disk != 'local':
            rows = self.rows_json(ctx, f"SELECT local_path, remote_path FROM system.remote_data_paths "
                                       f"WHERE disk_name = '{disk}'")
            for r in rows:
                out.setdefault(r['local_path'].rstrip('/').split('/')[-2], []).append(r['remote_path'])
        self.state(ctx)['remote_paths'] = out

    def reclassify(self, ctx, bounds) -> None:
        """Objects of merged-away parts are `outdated` from their merge on (design §4.3): retained, not active."""
        st = self.state(ctx)
        merges = sorted((r for r in st.get('part_log', []) if r['event_type'] == 'MergeParts'),
                        key=lambda r: int(r['t_us']))
        if not merges or ctx.policy == 'local':  # a local disk writes no objects to classify
            return
        by_key = {ident[1]: ident for ident in set(bounds.mutations) | set(bounds.upper.objects) |
                  set(bounds.lower.objects)}
        for m in merges:
            keys = []
            for part in m['merged_from']:
                if part not in st['remote_paths']:
                    raise ValueError(f'{self.name}: no remote paths for merged-away part {part}')
                keys += st['remote_paths'][part]
            unknown = sorted(k for k in keys if k not in by_key)
            if unknown:
                raise ValueError(f'{self.name}: objects of merged-away parts not in the pass ledger: {unknown[:5]}')
            bounds.reclassify(sorted(by_key[k] for k in keys), 'outdated', int(m['t_us']),
                              f'MergeParts {m["part_name"]} (system.part_log)')

    def observe(self, ctx, draft):
        return {}

    def assertions(self, ctx, draft):
        pred = self.predicted(ctx)
        out = compare_ledger(pred, draft['ledger']['foreground'], self.mode == 'deterministic')
        for key, want in pred['expected']['observations'].items():
            out[f'observation.{key}'] = check_observation(want, draft['observations'].get(key), draft['observations'])
        rows = self.inserts(ctx) * self.p(ctx)['rows']
        out['rows.' + self.p(ctx)['replica']] = (rows, self.state(ctx).get('rows', [None])[0])
        st = self.state(ctx)
        if st.get('remote_paths') and any(r['event_type'] == 'MergeParts' for r in st.get('part_log', [])):
            outdated = draft['inventory']['byte_hours'].get('outdated', {'lower': 0.0})
            out['inventory.outdated_byte_hours>0'] = (True, outdated['lower'] > 0)
        if self.mode == 'deterministic':
            out['window.crossing'] = ([], draft['window']['crossing'])
        return out


@register
class Boundaries(LifecycleCard):
    """Part type of every produced part and the upload request sizes of large part files, at the pinned thresholds.

    `params.kind`: `insert` (inserts only), `merge` (inserts, then `params.optimize`) or `multipart` (one insert
    whose single column file is sized against the first upload buffer).
    """

    name = 'boundaries'

    def measured(self, ctx):
        result = super().measured(ctx)
        if self.p(ctx)['kind'] == 'merge':
            self.replica_client(ctx).query(self.sql(ctx, self.p(ctx)['optimize']))
        return result

    def complete(self, ctx, timeout_s=300):
        super().complete(ctx, timeout_s)
        self.state(ctx)['parts'] = self.parts(ctx)
        if self.p(ctx)['kind'] == 'merge':
            self.state(ctx)['part_log'] = self.part_log(ctx)
            self.collect_layout(ctx)

    def observe(self, ctx, draft):
        rows = [r for r in window_rows(ctx) if r['caller'] == self.p(ctx)['replica'] and r['upstream']['outcome'] == 'ok']
        uploads = {r['response_body']['upload_id']: [] for r in rows
                   if r['op'] == 'CreateMultipartUpload' and (r.get('response_body') or {}).get('upload_id')}
        for r in sorted((r for r in rows if r['op'] == 'UploadPart' and r.get('upload_id') in uploads),
                        key=lambda r: r['part_number']):
            uploads[r['upload_id']].append(r['req_payload_bytes'])
        parts = self.state(ctx)['parts']
        # Block numbers of one INSERT's parts need not follow squash order; sizes and types do not depend on it.
        return {'parts': parts,
                'active_parts_by_rows': sorted(([x['rows'], x['part_type'], x['level']] for x in parts if x['active']),
                                               reverse=True),
                'upload_part_sizes': sorted(uploads.values()),
                'large_put_sizes': sorted(r['req_payload_bytes'] for r in rows
                                          if r['op'] == 'PutObject' and r['req_payload_bytes'] >= LARGE_PUT_BYTES)}


class NaturalMergeCard(LifecycleCard):
    """Inserts with background merges running, then a wait until no merge has run for `params.quiet_s`."""

    mode = 'stochastic'

    def wait_quiet(self, ctx) -> None:
        """Wait until no merge has run for `quiet_s`, at most `quiesce_timeout_s`; `quiesced` says which ended it.

        Wall-clock time, so the part log's merge end times compare with the polls.
        """
        client, p = self.replica_client(ctx), self.p(ctx)
        active = f'SELECT count() FROM system.merges WHERE {self.where()}'
        finished = (f'SELECT toUnixTimestamp64Micro(max(event_time_microseconds)) FROM system.part_log '
                    f"WHERE {self.where()} AND event_type = 'MergeParts'")
        start = quiet_since = time.time()
        quiesced = False
        while time.time() - start < p['quiesce_timeout_s']:
            if _count(client, active):
                quiet_since = time.time()
            elif time.time() - quiet_since >= p['quiet_s']:
                # A merge that ran entirely between two polls shows only in the part log.
                client.query('SYSTEM FLUSH LOGS')
                quiet_since = max(quiet_since, _count(client, finished) / 1e6)
                if time.time() - quiet_since >= p['quiet_s'] and not _count(client, active):
                    quiesced = True
                    break
            time.sleep(POLL_S)
        self.state(ctx)['quiesced'] = quiesced

    def complete(self, ctx, timeout_s=300):
        self.wait_quiet(ctx)
        super().complete(ctx, timeout_s)
        self.state(ctx)['part_log'] = self.part_log(ctx)
        self.state(ctx)['active_rows'] = [r['rows'] for r in self.parts(ctx) if r['active']]
        self.collect_layout(ctx)

    def observe(self, ctx, draft):
        st = self.state(ctx)
        return {**merge_observations(st['part_log'], st['active_rows'], self.p(ctx)['rows']), 'quiesced': st['quiesced']}


@register
class SmallInserts(NaturalMergeCard):
    name = 'small-inserts'


@register
class MergeNatural(NaturalMergeCard):
    """Also runs the phase-0 estimate's sample manifest, which has no prediction: its assertions fail closed."""

    name = 'merge-natural'


@register
class AmplificationRun(NaturalMergeCard):
    """Small inserts at a fixed rate for a long time: the validation run of the amplification rule."""

    name = 'amplification-run'

    def measured(self, ctx):
        n, interval = self.inserts(ctx), self.p(ctx)['interval_s']
        t0 = time.monotonic()
        for i in range(n):
            time.sleep(max(0.0, t0 + i * interval - time.monotonic()))  # pacing is the workload, not a wait
            self.insert(ctx, i)
        return {'unit_count': n, 'effective_counts': {'inserts': n, 'rows': n * self.p(ctx)['rows']}, 'observations': {}}


@register
class MergeForced(LifecycleCard):
    """Inserts with background merges stopped, then `params.optimize` merges them in one forced merge."""

    name = 'merge-forced'

    def measured(self, ctx):
        result = super().measured(ctx)
        self.replica_client(ctx).query(self.sql(ctx, self.p(ctx)['optimize']))
        return result

    def complete(self, ctx, timeout_s=300):
        super().complete(ctx, timeout_s)
        self.state(ctx)['parts'] = self.parts(ctx)
        self.state(ctx)['part_log'] = self.part_log(ctx)
        self.collect_layout(ctx)

    def observe(self, ctx, draft):
        st = self.state(ctx)
        merged = merge_observations(st['part_log'], [r['rows'] for r in st['parts'] if r['active']], self.p(ctx)['rows'])
        # Bytes depend on compression; the forced merge's structure and row amplification are exact.
        return {'parts': st['parts'], 'amplification_rows': merged['amplification_rows'],
                'merges': [{k: m[k] for k in ('fan_in', 'level', 'rows', 'part_type')} for m in merged['part_log_merges']]}


@register
class AsyncInserts(LifecycleCard):
    """`params.inserts` small INSERTs with inline data under the manifest's async insert settings.

    `params.pattern` is `steady` (insert i starts at i / `rate_per_s`) or `bursty` (`bursts` of `burst_size`
    concurrent inserts, `pause_s` apart). Concurrency is the workload: with `wait_for_async_insert = 1` a lone
    sequential client never shares a flush.
    """

    name = 'async-inserts'
    mode = 'stochastic'

    def values(self, ctx, i: int) -> str:
        p = self.p(ctx)
        base = i * p['rows']
        return ', '.join(f'({k}, {(k * 2654435761 + p["seed"]) % (1 << 32)}, {k % 1000}, {k * 7}, {k // 3})'
                         for k in range(base, base + p['rows']))

    def insert(self, ctx, i: int) -> None:
        sql = self.sql(ctx, self.p(ctx)['insert'], i=i, values=self.values(ctx, i))
        self.replica_client(ctx).query(sql, settings=ctx.manifest['settings']['query'])

    def measured(self, ctx):
        p, n = self.p(ctx), self.inserts(ctx)
        if p['pattern'] == 'steady':
            with ThreadPoolExecutor(max_workers=p['concurrency']) as pool:
                t0, futures = time.monotonic(), []
                for i in range(n):
                    time.sleep(max(0.0, t0 + i / p['rate_per_s'] - time.monotonic()))  # the arrival schedule
                    futures.append(pool.submit(self.insert, ctx, i))
                for f in futures:
                    f.result()
        elif p['pattern'] == 'bursty':
            if p['bursts'] * p['burst_size'] != n:
                raise ValueError(f'{self.name}: bursts x burst_size != inserts ({p["bursts"]} x {p["burst_size"]} != {n})')
            with ThreadPoolExecutor(max_workers=p['burst_size']) as pool:
                for b in range(p['bursts']):
                    for f in [pool.submit(self.insert, ctx, b * p['burst_size'] + j) for j in range(p['burst_size'])]:
                        f.result()
                    if b + 1 < p['bursts']:
                        time.sleep(p['pause_s'])  # the gap between bursts is the workload
        else:
            raise ValueError(f'{self.name}: unknown params.pattern {p["pattern"]!r}')
        return {'unit_count': n, 'effective_counts': {'inserts': n, 'rows': n * p['rows']}, 'observations': {}}

    def complete(self, ctx, timeout_s=300):
        super().complete(ctx, timeout_s)
        self.state(ctx)['part_log'] = self.part_log(ctx)

    def observe(self, ctx, draft):
        created = [r for r in self.state(ctx)['part_log'] if r['event_type'] == 'NewPart']
        events = draft['events'].get(self.p(ctx)['replica'], {})
        n = self.inserts(ctx)
        return {'inserted_parts': len(created), 'inserts_per_part': n / len(created) if created else 0.0,
                'async_insert_queries': int(events.get('AsyncInsertQuery', 0)),
                'part_rows_max': max((int(r['rows']) for r in created), default=0)}
