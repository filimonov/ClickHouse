"""The forced replication and fault cases (compared exactly on `s3` by the cards) once on `s3` against the pinned binary, two replicas.
Run from utils/cas-yardstick under the rig lock: YARDSTICK_LIVE=1 python3 -m unittest tests.live_replication -v
(about 10 minutes)."""
import glob
import io
import json
import os
import pathlib
import time
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace

from yardstick.cost import cli

ROOT = pathlib.Path(__file__).resolve().parents[3]
BINARY = ROOT / 'tmp' / 'bin' / '26.8' / 'clickhouse'
REPORT = ROOT / 'docs' / 'superpowers' / 'reports' / '2026-10-24-storage-cost-replication'


@unittest.skipUnless(os.environ.get('YARDSTICK_LIVE') == '1', 'set YARDSTICK_LIVE=1')
class LiveReplicationTest(unittest.TestCase):
    def test_deterministic_cases_reconcile(self):
        if not BINARY.exists():
            self.skipTest(f'no binary {BINARY}')
        manifests = [json.loads(pathlib.Path(p).read_text()) for p in glob.glob(str(REPORT / 'experiments' / '*.json'))]
        slow = {'replication-natural-fit', 'replication-natural-holdout', 'restart-catchup'}
        cases = sorted(m['experiment_id'] for m in manifests if m['card'] not in slow and 's3' in m['policies'])
        runs = ROOT / 'tmp' / 'cas-yardstick' / 'cost-runs' / f'live-replication-{time.strftime("%Y%m%d-%H%M%S")}'
        args = SimpleNamespace(binary=str(BINARY), runs_dir=str(runs), port_base=19700, endpoint=None, commit=None,
                               source_dir=None, manifests=[str(REPORT)], cases=','.join(cases), policies='s3',
                               replicas=2, repeats=1, keep_prefix=False)
        out = io.StringIO()
        with redirect_stdout(out):
            code = cli.cmd_run(args)
        self.assertEqual(code, 0, out.getvalue())


if __name__ == '__main__':
    unittest.main()
