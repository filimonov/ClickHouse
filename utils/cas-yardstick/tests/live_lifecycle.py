"""The deterministic lifecycle cases (`boundaries.*`, `merge-forced.default`) once on `s3` against the pinned binary.
Run from utils/cas-yardstick: YARDSTICK_LIVE=1 python3 -m unittest tests.live_lifecycle -v (about 10 minutes)."""
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
REPORT = ROOT / 'docs' / 'superpowers' / 'reports' / '2026-10-24-storage-cost-lifecycle'


@unittest.skipUnless(os.environ.get('YARDSTICK_LIVE') == '1', 'set YARDSTICK_LIVE=1')
class LiveLifecycleTest(unittest.TestCase):
    def test_deterministic_cases_reconcile(self):
        if not BINARY.exists():
            self.skipTest(f'no binary {BINARY}')
        cases = sorted(json.loads(pathlib.Path(p).read_text())['experiment_id']
                       for p in glob.glob(str(REPORT / 'experiments' / '*.json'))
                       if json.loads(pathlib.Path(p).read_text())['mode'] == 'deterministic')
        runs = ROOT / 'tmp' / 'cas-yardstick' / 'cost-runs' / f'live-lifecycle-{time.strftime("%Y%m%d-%H%M%S")}'
        args = SimpleNamespace(binary=str(BINARY), runs_dir=str(runs), port_base=19700, endpoint=None, commit=None,
                               source_dir=None, manifests=[str(REPORT)], cases=','.join(cases), policies='s3',
                               replicas=1, repeats=1, keep_prefix=False)
        out = io.StringIO()
        with redirect_stdout(out):
            code = cli.cmd_run(args)
        self.assertEqual(code, 0, out.getvalue())


if __name__ == '__main__':
    unittest.main()
