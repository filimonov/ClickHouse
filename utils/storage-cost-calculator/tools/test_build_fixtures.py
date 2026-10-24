"""Tests for build_fixtures.py on the synthetic report under test/fixtures/report-sample."""

import contextlib
import copy
import io
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

import build_fixtures

CALC = Path(__file__).resolve().parents[1]
SAMPLE = CALC / "test/fixtures/report-sample"
REPORT = SAMPLE / "2026-10-24-storage-cost-sample"
NAME = "insert_basic.small_part.s3_default.puts_per_compact_part.fit.k0.json"


class BuildFixturesTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.out = self.tmp / "measured"

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def run_main(self, coverage=None, reports=None):
        cov = SAMPLE / "coverage.json"
        if coverage is not None:
            cov = self.tmp / "coverage.json"
            cov.write_text(json.dumps(coverage))
        err = io.StringIO()
        with contextlib.redirect_stderr(err), contextlib.redirect_stdout(io.StringIO()):
            code = build_fixtures.main(["--reports", str(reports or SAMPLE / "*/"), "--coverage", str(cov), "--out", str(self.out)])
        return code, err.getvalue()

    def coverage(self):
        return json.loads((SAMPLE / "coverage.json").read_text())

    def test_done_fit_edge_joins_prediction_measurement_and_cells(self):
        code, err = self.run_main()
        self.assertEqual(code, 0, err)
        # The done validation edge and the planned hold-out edge produce nothing.
        self.assertEqual(sorted(p.name for p in self.out.iterdir()), ["_coefficient_only.json", NAME])
        fx = json.loads((self.out / NAME).read_text())
        self.assertEqual(fx["edge_id"], "evidence[0]")
        self.assertEqual((fx["coefficient"], fx["role"], fx["variant"], fx["mode"]), ("puts_per_compact_part", "fit", "s3", "deterministic"))
        self.assertEqual(fx["measurement_id"], "run42/s3_default/insert_basic/small_part/k0")
        self.assertEqual(fx["inputs"]["parts_inserted"], 1)
        self.assertEqual(fx["expected_rows"], [
            {"caller": "r1", "op_kind": "CreateMultipartUpload", "quantity": {"min": 0, "max": 1}},
            {"caller": "r1", "op_kind": "PutObject", "quantity": 3},
        ])
        self.assertEqual(fx["measured_rows"], [{"caller": "r1", "op_kind": "PutObject", "quantity": 3}])
        self.assertEqual(fx["tolerance"], {"abs": {"PutObject": 0}, "rel": {}})
        # Only the measured card's cells: other_card.write belongs to another card.
        self.assertEqual(list(fx["cells"]), ["insert_basic.write"])
        self.assertEqual([c["id"] for c in fx["coefficients"]], ["puts_per_compact_part"])

    def test_rebuild_removes_stale_fixtures(self):
        self.out.mkdir()
        (self.out / "stale.json").write_text("{}")
        cov = self.coverage()
        cov["evidence"] = []
        code, err = self.run_main(cov)
        self.assertEqual(code, 0, err)
        self.assertEqual([p.name for p in self.out.iterdir()], ["_coefficient_only.json"])
        self.assertEqual(json.loads((self.out / "_coefficient_only.json").read_text())["edges"], [])

    def test_edge_without_cells_or_request_rows_is_coefficient_only(self):
        code, err = self.run_main()
        self.assertEqual(code, 0, err)
        only = json.loads((self.out / "_coefficient_only.json").read_text())
        self.assertEqual(only["coverage"], str((SAMPLE / "coverage.json").resolve()))
        self.assertEqual(only["edges"], [{"edge_id": "evidence[3]", "coefficient": "flush_interval", "role": "holdout",
                                          "experiment_id": "lifecycle_only.gate", "policy": "s3_default",
                                          "measurement_ids": ["run42/s3_default/lifecycle_only/gate/k0"]}])

    def test_coefficient_only_edge_still_needs_its_measurement(self):
        cov = self.coverage()
        cov["evidence"][3]["measurement_ids"] = ["run42/s3_default/lifecycle_only/gate/k7"]
        code, err = self.run_main(cov)
        self.assertEqual(code, 1)
        self.assertIn("lifecycle_only/gate/k7 is in no report", err)

    def test_missing_prediction_fails_with_the_edge(self):
        cov = self.coverage()
        cov["evidence"][0]["experiment_id"] = "insert_basic.other"
        code, err = self.run_main(cov)
        self.assertEqual(code, 1)
        self.assertIn("expected one prediction insert_basic.other.s3_default.json", err)
        self.assertFalse(self.out.exists())

    def test_missing_measurement_fails(self):
        cov = self.coverage()
        cov["evidence"][0]["measurement_ids"] = ["run42/s3_default/insert_basic/small_part/k9"]
        code, err = self.run_main(cov)
        self.assertEqual(code, 1)
        self.assertIn("measurement run42/s3_default/insert_basic/small_part/k9 is in no report", err)

    def test_done_edge_without_measurement_fails(self):
        cov = self.coverage()
        cov["evidence"][0]["measurement_ids"] = []
        code, err = self.run_main(cov)
        self.assertEqual(code, 1)
        self.assertIn("done edge cites no measurement", err)

    def test_invalid_coverage_fails(self):
        cov = self.coverage()
        cov["evidence"][0]["role"] = "guess"
        code, err = self.run_main(cov)
        self.assertEqual(code, 1)
        self.assertIn("coverage: $.evidence[0].role", err)

    def test_card_without_cells_fails(self):
        reports = self.tmp / "reports"
        shutil.copytree(REPORT, reports / REPORT.name)
        f = reports / REPORT.name / "formulas.json"
        doc = json.loads(f.read_text())
        del doc["cells"]["insert_basic.write"]
        f.write_text(json.dumps(doc))
        code, err = self.run_main(reports=reports / "*/")
        self.assertEqual(code, 1)
        self.assertIn("no s3 cell for card insert_basic", err)


@unittest.skipUnless(shutil.which("node"), "node is not installed")
class NodeCheckTest(unittest.TestCase):
    """The Node suite's measured-fixture test accepts the sample and rejects a measurement the model does not match."""

    def node_check(self, out, dev=False):
        env = dict(os.environ, SCC_MEASURED_FIXTURES=str(out))
        env.pop("SCC_DEV", None)
        if dev:
            env["SCC_DEV"] = "1"
        return subprocess.run(["node", "--test", "--test-name-pattern", "test_measured_fixtures_within_tolerance",
                               str(CALC / "test/pipeline_eval.test.js")], env=env, capture_output=True, text=True)

    def test_sample_within_tolerance_and_mismatch_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "measured"
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(build_fixtures.main(["--reports", str(SAMPLE / "*/"), "--coverage", str(SAMPLE / "coverage.json"),
                                                      "--out", str(out)]), 0)
            done = self.node_check(out)
            self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
            fx = json.loads((out / NAME).read_text())
            bad = copy.deepcopy(fx)
            bad["measured_rows"][0]["quantity"] = 4
            (out / NAME).write_text(json.dumps(bad))
            done = self.node_check(out)
            self.assertNotEqual(done.returncode, 0)
            self.assertIn("r1 PutObject: measured 4, model 3 exactly", done.stdout)
            # An unmodelled measured op is a failure too, not a silent pass.
            extra = copy.deepcopy(fx)
            extra["measured_rows"].append({"caller": "r1", "op_kind": "HeadObject", "quantity": 2})
            (out / NAME).write_text(json.dumps(extra))
            done = self.node_check(out)
            self.assertIn("r1 HeadObject: measured 2, no model row", done.stdout)

    def test_empty_directory_fails_unless_dev(self):
        with tempfile.TemporaryDirectory() as tmp:
            done = self.node_check(Path(tmp))
            self.assertNotEqual(done.returncode, 0)
            self.assertIn("no measured fixtures in", done.stdout)
            done = self.node_check(Path(tmp), dev=True)
            self.assertEqual(done.returncode, 0, done.stdout)
            self.assertIn("# SKIP no measured fixtures in", done.stdout)

    def test_coefficient_only_list_must_match_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "measured"
            with contextlib.redirect_stdout(io.StringIO()):
                build_fixtures.main(["--reports", str(SAMPLE / "*/"), "--coverage", str(SAMPLE / "coverage.json"), "--out", str(out)])
            only = json.loads((out / "_coefficient_only.json").read_text())
            only["edges"] = []
            (out / "_coefficient_only.json").write_text(json.dumps(only))
            done = self.node_check(out)
            self.assertNotEqual(done.returncode, 0)
            self.assertIn("evidence[3]", done.stdout)


if __name__ == "__main__":
    unittest.main()
