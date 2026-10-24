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

    def test_cells_get_the_coefficients_other_reports_own(self):
        # A variant's cells may read a coefficient another report owns (s3plain reuses s3 counts); the fixture
        # carries it, or the row would evaluate as unavailable.
        reports = self.tmp / "reports"
        shutil.copytree(SAMPLE, reports, ignore=shutil.ignore_patterns("coverage.json"))
        mine = reports / REPORT.name
        formulas = json.loads((mine / "formulas.json").read_text())
        cell = next(iter(formulas["cells"].values()))
        cell["ledger_rows"][0]["expr"] = {"kind": "mul", "dim": "count", "a": cell["ledger_rows"][0]["expr"],
                                          "b": {"kind": "coef", "id": "foreign_factor", "dim": "count"}}
        (mine / "formulas.json").write_text(json.dumps(formulas))
        other = reports / "2026-10-24-storage-cost-other"
        other.mkdir()
        (other / "coefficients.json").write_text(json.dumps([{"id": "foreign_factor", "kind": "scalar", "dim": "count",
                                                              "value": 1, "status": "ok"}]))
        code, err = self.run_main(reports=reports / "*/")
        self.assertEqual(code, 0, err)
        fx = json.loads((self.out / NAME).read_text())
        self.assertEqual(sorted(c["id"] for c in fx["coefficients"]), ["foreign_factor", "puts_per_compact_part"])

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
                                          "measurement_ids": ["run42/s3_default/lifecycle_only/gate/k0"],
                                          "request_rows_reconciled_by": "card"}])

    def test_coefficient_only_edge_still_needs_its_measurement(self):
        cov = self.coverage()
        cov["evidence"][3]["measurement_ids"] = ["run42/s3_default/lifecycle_only/gate/k7"]
        code, err = self.run_main(cov)
        self.assertEqual(code, 1)
        self.assertIn("lifecycle_only/gate/k7 is in no report", err)

    def test_borrowed_edge_adds_no_fixture(self):
        # The borrowed row replays the same records as its source edge, which already has a fixture.
        cov = self.coverage()
        cov["evidence"].append(dict(cov["evidence"][0], policy="s3plain", borrowed_from=cov["evidence"][0]["policy"],
                                    note="the plain requests equal the s3 ones"))
        code, err = self.run_main(cov)
        self.assertEqual(code, 0, err)
        self.assertEqual(sorted(p.name for p in self.out.iterdir()), ["_coefficient_only.json", NAME])

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

    def copy_report(self):
        reports = self.tmp / "reports"
        shutil.copytree(REPORT, reports / REPORT.name)
        return reports, reports / REPORT.name

    def test_card_without_cells_is_coefficient_only_even_with_request_rows(self):
        # The card reconciled its own request rows; the record's status carries that verdict.
        reports, rep = self.copy_report()
        f = rep / "formulas.json"
        doc = json.loads(f.read_text())
        del doc["cells"]["insert_basic.write"]
        f.write_text(json.dumps(doc))
        code, err = self.run_main(reports=reports / "*/")
        self.assertEqual(code, 0, err)
        only = json.loads((self.out / "_coefficient_only.json").read_text())
        self.assertEqual([(e["edge_id"], e["request_rows_reconciled_by"]) for e in only["edges"]],
                         [("evidence[0]", "card"), ("evidence[3]", "card")])
        self.assertEqual([p.name for p in self.out.iterdir()], ["_coefficient_only.json"])

    def test_edge_with_a_fixture_is_not_also_coefficient_only(self):
        # k0 lives in a report with the card's cell, k1 in one without: the edge is reconciled by k0's fixture.
        reports, rep = self.copy_report()
        other = reports / "2026-10-24-storage-cost-other"
        other.mkdir()
        record = json.loads((rep / "measurements.jsonl").read_text().splitlines()[0])
        record.update(k=1, measurement_id=record["measurement_id"].replace("/k0", "/k1"))
        (other / "measurements.jsonl").write_text(json.dumps(record) + "\n")
        doc = json.loads((rep / "formulas.json").read_text())
        del doc["cells"]["insert_basic.write"]
        (other / "formulas.json").write_text(json.dumps(doc))
        shutil.copy(rep / "coefficients.json", other / "coefficients.json")
        cov = self.coverage()
        cov["evidence"][0]["measurement_ids"].append(record["measurement_id"])
        code, err = self.run_main(cov, reports=reports / "*/")
        self.assertEqual(code, 0, err)
        only = json.loads((self.out / "_coefficient_only.json").read_text())
        self.assertEqual([e["edge_id"] for e in only["edges"]], ["evidence[3]"])
        self.assertIn(NAME, [p.name for p in self.out.iterdir()])

    def test_domain_cell_referencing_the_coefficient_is_selected(self):
        # Variant reports key cells by domain operation; such a cell joins the card's cells when it uses the coefficient.
        reports, rep = self.copy_report()
        f = rep / "formulas.json"
        doc = json.loads(f.read_text())
        doc["cells"]["insert"] = doc["cells"].pop("insert_basic.write")
        doc["cells"]["merge"] = doc["cells"].pop("other_card.write")  # a domain cell without the coefficient
        f.write_text(json.dumps(doc))
        code, err = self.run_main(reports=reports / "*/")
        self.assertEqual(code, 0, err)
        fx = json.loads((self.out / NAME).read_text())
        self.assertEqual(list(fx["cells"]), ["insert"])

    def test_cell_not_referencing_the_coefficient_fails(self):
        cov = self.coverage()
        cov["evidence"][0]["coefficient"] = "puts_per_wide_part"
        code, err = self.run_main(cov)
        self.assertEqual(code, 1)
        self.assertIn("no s3 cell of card insert_basic references puts_per_wide_part", err)

    def test_missing_coefficients_file_fails(self):
        reports, rep = self.copy_report()
        (rep / "coefficients.json").unlink()
        code, err = self.run_main(reports=reports / "*/")
        self.assertEqual(code, 1)
        self.assertIn("no coefficients.json", err)

    def test_missing_formulas_file_fails(self):
        reports, rep = self.copy_report()
        (rep / "formulas.json").unlink()
        code, err = self.run_main(reports=reports / "*/")
        self.assertEqual(code, 1)
        self.assertIn("no formulas.json", err)


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
            self.assertRegex(done.stdout, r"r1 PutObject: measured 4, model 3 within [0-9.]+e-9")
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
