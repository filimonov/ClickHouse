"""R4-2: recompute `window.no_workload_after_cutoff` of every ok natural-merge record from its raw ledger slice.

    python3 docs/superpowers/reports/2026-10-24-storage-cost-validation/replay_cutoff.py

Counts r1 workload attempts inside the window that started after the record's `cutoff_us`, and compares them with the
assertion the card recorded.
"""
import gzip
import json
from pathlib import Path

REPORT = Path(__file__).resolve().parent

for line in (REPORT / 'measurements.jsonl').read_text().splitlines():
    m = json.loads(line)
    if m['card'] != 'merge-natural' or m['status'] != 'ok':
        continue
    o, w = m['observations'], m['window']
    with gzip.open(REPORT / 'raw' / m['card'] / m['run_id'] / 'ledger.jsonl.gz', 'rt') as f:
        rows = [json.loads(x) for x in f]
    after = [r['seq'] for r in rows if r.get('kind') == 'attempt' and r['caller'] == 'r1'
             and w['start_seq'] < r['seq'] <= w['end_seq'] and o['cutoff_us'] < r['t_start_us'] < w['t_end_us']]
    recorded = m['assertions']['window.no_workload_after_cutoff']['actual']
    print(f"| `{m['measurement_id']}` | {o['cutoff_us']} | {w['t_end_us']} | {len(after)} | {recorded == after} |")
