"""The phase-0 gate: the five probes against the pinned binary, with drill and render checks. Run from
utils/cas-yardstick: YARDSTICK_LIVE=1 python3 -m unittest tests.live_probes -v (about 10 minutes)."""
import io
import os
import pathlib
import time
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace

from yardstick.cost import cli

ROOT = pathlib.Path(__file__).resolve().parents[3]
BINARY = ROOT / 'tmp' / 'bin' / '26.8' / 'clickhouse'


@unittest.skipUnless(os.environ.get('YARDSTICK_LIVE') == '1', 'set YARDSTICK_LIVE=1')
class LiveProbesTest(unittest.TestCase):
    def test_five_probes_pass(self):
        if not BINARY.exists():
            self.skipTest(f'no binary {BINARY}')
        runs = ROOT / 'tmp' / 'cas-yardstick' / 'cost-runs' / f'live-probes-{time.strftime("%Y%m%d-%H%M%S")}'
        args = SimpleNamespace(binary=str(BINARY), runs_dir=str(runs), port_base=19700, endpoint=None, commit=None,
                               manifests=None, replicas=2, scale=1.0, keep_prefix=False)
        out = io.StringIO()
        with redirect_stdout(out):
            code = cli.cmd_probe(args)
        self.assertEqual(code, 0, out.getvalue())
        self.assertEqual(out.getvalue().count('PASS '), len(cli.PROBES), out.getvalue())


if __name__ == '__main__':
    unittest.main()
