"""Report arithmetic for the CAS yardstick: counter aggregates, statistics, TSV files, tables."""
import statistics
from collections import OrderedDict

AGGREGATES = {
    'GET': ['DiskS3GetObject'],
    'PUT': ['DiskS3PutObject', 'DiskS3UploadPart', 'DiskS3CreateMultipartUpload', 'DiskS3CompleteMultipartUpload'],
    'HEAD': ['DiskS3HeadObject'],
    'LIST': ['DiskS3ListObjects'],
    'DELETE': ['DiskS3DeleteObjects'],
    'COPY': ['DiskS3CopyObject', 'DiskS3UploadPartCopy'],
    'ABORT': ['DiskS3AbortMultipartUpload'],
    'bytes_read': ['ReadBufferFromS3Bytes'],
    'bytes_written': ['WriteBufferFromS3Bytes'],
}
CAS_DIAGNOSTICS = ['CASRequestAttempt', 'CASRootGet', 'CASRootPut', 'CASBlobPut', 'CASBlobHead', 'CASBlobHeadMiss',
                   'CASManifestPut', 'CASManifestGet', 'CASOtherGet', 'CASHotKeyQueueWaitMicroseconds',
                   'CASRefQueueWaitMicroseconds']
S3_ERROR_COUNTERS = ['S3ReadRequestsErrors', 'S3WriteRequestsErrors',
                     'DiskS3ReadRequestRetryableErrors', 'DiskS3WriteRequestRetryableErrors']
UNAVAILABLE = 'unavailable'
REQUEST_METRICS = ['GET', 'PUT', 'HEAD', 'LIST', 'DELETE', 'COPY', 'ABORT']
COMPAT_FIELDS = ['workload_rev', 'scale', 'counts', 'gc', 'repeats', 'config_hash', 'build_type', 'store']

SAMPLE_COLUMNS = ['policy', 'card', 'phase', 'repeat', 'metric', 'value', 'unit_count']
SUMMARY_COLUMNS = ['policy', 'card', 'phase', 'metric', 'median', 'min', 'max', 'repeats', 'per_unit', 'mark']
_TEXT_COLUMNS = {'policy', 'card', 'phase', 'metric', 'mark'}
TABLE_COLUMNS = ['wall', 'median_op_ms', 'p90_op_ms', 'GET', 'PUT', 'HEAD', 'LIST', 'DELETE', 'COPY', 'MiB_r', 'MiB_w', 'gc_rounds']


def delta(before, after):
    return {k: after[k] - before[k] for k in after if k in before}


def aggregate(d, present):
    out = {}
    for metric, counters in AGGREGATES.items():
        out[metric] = sum(d.get(c, 0) for c in counters) if all(c in present for c in counters) else UNAVAILABLE
    return out


def summarize(samples):
    return statistics.median(samples), min(samples), max(samples)


def paired_ratio(num, den):
    if any(d == 0 for d in den):
        return '0 → %g' % statistics.median(num)
    ratios = [n / d for n, d in zip(num, den)]
    m, lo, hi = summarize(ratios)
    return f'{m:.2f} ({lo:.2f}..{hi:.2f})'


def mark_requests(samples):
    return '~' if len(set(samples)) > 1 else ''


def mark_wall(samples):
    return 'noisy' if min(samples) > 0 and max(samples) / min(samples) > 1.2 else ''


def per_unit(value, unit_count):
    return value / unit_count


def _parse_number(s):
    if s == '':
        return ''
    try:
        return int(s)
    except ValueError:
        pass
    try:
        return float(s)
    except ValueError:
        return s


def _format(v):
    return v if isinstance(v, str) else repr(v)


def _write_tsv(path, columns, rows):
    with open(path, 'w', encoding='utf-8') as f:
        f.write('\t'.join(columns) + '\n')
        for r in rows:
            f.write('\t'.join(_format(r[c]) for c in columns) + '\n')


def _read_tsv(path, columns):
    with open(path, encoding='utf-8') as f:
        lines = f.read().split('\n')
    header = lines[0].split('\t')
    if header != columns:
        raise ValueError(f'{path}: unexpected header {header}')
    rows = []
    for line in lines[1:]:
        if line == '':
            continue
        cells = line.split('\t')
        rows.append({c: (v if c in _TEXT_COLUMNS else _parse_number(v)) for c, v in zip(columns, cells)})
    return rows


def write_samples(path, rows):
    _write_tsv(path, SAMPLE_COLUMNS, rows)


def read_samples(path):
    return _read_tsv(path, SAMPLE_COLUMNS)


def write_summary(path, summary):
    _write_tsv(path, SUMMARY_COLUMNS, summary)


def read_summary(path):
    return _read_tsv(path, SUMMARY_COLUMNS)


def build_summary(rows):
    groups = OrderedDict()
    for r in rows:
        groups.setdefault((r['policy'], r['card'], r['phase'], r['metric']), []).append(r)
    out = []
    for (policy, card, phase, metric), rs in groups.items():
        key = dict(policy=policy, card=card, phase=phase, metric=metric)
        values = [r['value'] for r in rs]
        text = [v for v in values if isinstance(v, str)]
        if text:
            out.append(dict(key, median=text[0], min=text[0], max=text[0], repeats=len(rs), per_unit='', mark=''))
            continue
        units = {r['unit_count'] for r in rs}
        if len(units) != 1:
            raise ValueError(f'unit_count differs within {key}: {sorted(units)}')
        median, lo, hi = summarize(values)
        if metric in REQUEST_METRICS:
            mark = mark_requests(values)
        elif metric == 'wall':
            mark = mark_wall(values)
        else:
            mark = ''
        unit = units.pop()
        out.append(dict(key, median=median, min=lo, max=hi, repeats=len(rs),
                        per_unit=per_unit(median, unit) if unit else '', mark=mark))
    return out


def _index(summary):
    return {(r['policy'], r['card'], r['phase'], r['metric']): r for r in summary}


def _cards(summary):
    return list(OrderedDict(((r['card'], r['phase']), None) for r in summary))


def _cell(idx, policy, card, phase, metric):
    status = idx.get((policy, card, phase, 'status'))
    if status is not None:
        return status['median']
    if metric in ('MiB_r', 'MiB_w'):
        r = idx.get((policy, card, phase, 'bytes_read' if metric == 'MiB_r' else 'bytes_written'))
        scale = 1 << 20
    else:
        r = idx.get((policy, card, phase, metric))
        scale = 1
    if r is None:
        return '-'
    v = r['median']
    if isinstance(v, str):
        return v
    v = v / scale
    if metric == 'wall':
        return f'{v:.2f}' + (' noisy' if r['mark'] == 'noisy' else '')
    return (f'{v:.1f}' if isinstance(v, float) and v != int(v) else str(int(v))) + r['mark']


def _ratio(idx, num_policy, den_policy, card, phase, metric):
    if (num_policy, card, phase, 'status') in idx or (den_policy, card, phase, 'status') in idx:
        return '-'
    n = idx.get((num_policy, card, phase, metric))
    d = idx.get((den_policy, card, phase, metric))
    if n is None or d is None or isinstance(n['median'], str) or isinstance(d['median'], str):
        return '-'
    if d['median'] == 0:
        return '0 → %g' % n['median']
    return f"{n['median'] / d['median']:.2f}"


def _grid(rows):
    widths = [max(len(r[i]) for r in rows) for i in range(len(rows[0]))]
    return '\n'.join('  '.join(c.ljust(w) for c, w in zip(r, widths)).rstrip() for r in rows)


def render_table(summary, policies_uncached, policies_cached, ratios_of=('cas', 's3', 's3plain')):
    idx = _index(summary)
    blocks = []
    for title, policies in (('uncached', policies_uncached), ('cached', policies_cached)):
        if not policies:
            continue
        for card, phase in _cards(summary):
            rows = [['policy'] + TABLE_COLUMNS]
            for p in policies:
                rows.append([p] + [_cell(idx, p, card, phase, c) for c in TABLE_COLUMNS])
            lines = [f'{title} / {card} / {phase}', _grid(rows)]
            base, others = ratios_of[0], ratios_of[1:]
            rrows = []
            for o in others:
                if base in policies and o in policies:
                    rrows.append([f'{base}/{o}'] + [_ratio(idx, base, o, card, phase, m)
                                                    for m in ('wall', 'GET', 'PUT', 'bytes_read')])
            if rrows:
                lines.append(_grid([['ratio', 'wall', 'GET', 'PUT', 'bytes']] + rrows))
            for p in policies:
                unit = [f'{m}={idx[(p, card, phase, m)]["per_unit"]:.3g}' for m in ['wall'] + REQUEST_METRICS
                        if (p, card, phase, m) in idx and isinstance(idx[(p, card, phase, m)]['per_unit'], float)]
                if unit:
                    lines.append(f'per unit, {p}: ' + ' '.join(unit))
            blocks.append('\n'.join(lines))
    return '\n\n'.join(blocks) + '\n'


def render_diagnostics(summary):
    idx = _index(summary)
    policies = list(OrderedDict((r['policy'], None) for r in summary if r['policy'].startswith('cas')))
    lines = []
    for card, phase in _cards(summary):
        for p in policies:
            for m in CAS_DIAGNOSTICS + S3_ERROR_COUNTERS:
                r = idx.get((p, card, phase, m))
                if r is not None:
                    lines.append(f'{p} {card} {phase} {m}: {r["median"]} ({r["min"]}..{r["max"]})')
    return '\n'.join(lines) + '\n'


def write_manifest(path, fields):
    _write_tsv(path, ['field', 'value'], [dict(field=k, value=str(v)) for k, v in fields.items()])


def read_manifest(path):
    with open(path, encoding='utf-8') as f:
        lines = f.read().split('\n')
    if lines[0] != 'field\tvalue':
        raise ValueError(f'{path}: unexpected header {lines[0]!r}')
    return dict(line.split('\t', 1) for line in lines[1:] if line)


def compatible(m1, m2):
    return [f for f in COMPAT_FIELDS if m1.get(f) != m2.get(f)]


def render_compare(cur_summary, ref_summary):
    ref = _index(ref_summary)
    lines = []
    for r in cur_summary:
        k = (r['policy'], r['card'], r['phase'], r['metric'])
        o = ref.get(k)
        if o is None:
            continue
        head = f'{k[0]} {k[1]} {k[2]} {k[3]}: cur {r["median"]} ({r["min"]}..{r["max"]}) vs ref {o["median"]} ({o["min"]}..{o["max"]})'
        if isinstance(r['median'], str) or isinstance(o['median'], str):
            lines.append(head + ('' if r['median'] == o['median'] else ' !'))
            continue
        pct = (r['median'] - o['median']) / o['median'] * 100 if o['median'] else None
        text = ' n/a' if pct is None else f' {pct:+.1f}%'
        flag = (k[3] == 'wall' and pct is not None and abs(pct) > 10) or \
               (k[3] in REQUEST_METRICS and r['median'] != o['median'])
        lines.append(head + text + (' !' if flag else ''))
    return '\n'.join(lines) + '\n'
