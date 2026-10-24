"""Tests for generate_contract.py on the synthetic cost model under test/fixtures/contract-sample."""

import contextlib
import hashlib
import io
import json
import re
import shutil
import tempfile
import unittest
from pathlib import Path

import generate_contract as gc

CALC = Path(__file__).resolve().parents[1]
SAMPLE = CALC / "test/fixtures/contract-sample"
REPORT = "2026-10-24-storage-cost-alpha"


@unittest.skipUnless(shutil.which("node"), "node is not installed")
class GenerateTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        shutil.copytree(SAMPLE, self.tmp, dirs_exist_ok=True)
        (self.tmp / "code").mkdir()
        for js in CALC.glob("*.js"):
            shutil.copy(js, self.tmp / "code")
        self.cm, self.rep = self.tmp / "cost-model", self.tmp / "reports" / REPORT
        self.copy, self.page, self.doc = self.tmp / "copy.json", self.tmp / "page.html", self.tmp / "doc.md"

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def run_gc(self, command, *extra, mode="dev"):
        args = [command, "--mode", mode, "--cost-model", str(self.cm), "--reports", str(self.tmp / "reports/*-storage-cost-*"),
                "--page", str(self.page), "--doc", str(self.doc), "--copy", str(self.copy),
                "--code-dir", str(self.tmp / "code"), "--fixtures-out", str(self.tmp / "measured"), *extra]
        if command == "check" and mode == "dev":
            args.append("--skip-node")  # the checked-in Node suite is not about this sample
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = gc.main(args)
        return code, out.getvalue() + err.getvalue()

    def generate(self):
        code, log = self.run_gc("generate")
        self.assertEqual(code, 0, log)
        return json.loads(self.copy.read_text())

    def edit_json(self, path, fn):
        doc = json.loads(path.read_text())
        fn(doc)
        path.write_text(json.dumps(doc, indent=1))

    def outputs(self):
        return {p: p.read_bytes() for p in (self.cm / gc.CONTRACT, self.copy, self.page, self.doc)}

    def test_generate_writes_three_identical_copies(self):
        contract = self.generate()
        self.assertEqual((self.cm / gc.CONTRACT).read_bytes(), self.copy.read_bytes())
        embedded = re.search(r'<script type="application/json" id="contract">(.*?)</script>', self.page.read_text(), re.S)
        self.assertEqual(json.loads(embedded.group(1)), contract)
        doc = self.doc.read_text()
        self.assertIn(f"model-contract.json` version `{contract['version']}`", doc)
        self.assertIn("### `s3` / `inserts` {#formula-s3-inserts}", doc)
        self.assertIn("`parts_inserted × puts_per_compact_part`", doc)
        self.assertIn("| `puts_per_compact_part` | `3` | `count` | measured |", doc)
        self.assertIn("| `parts_inserted` | `simple` | Parts inserted per month |", doc)
        # The domain is complete for the loader; ebs cannot insert at all, so every family of the operation is out.
        self.assertEqual(contract["applicability"], {"ebs": {"inserts": {"write_path": "not_supported"}},
                                                     "s3": {"inserts": {"write_path": "supported"}}})
        self.assertEqual(contract["state_ids"], ["ok", "not_supported", "backlog_grows", "unsupported_cut", "unavailable"])
        self.assertEqual(list(contract["formulas"]["s3"]), ["inserts"])
        self.assertEqual(list(contract["card_cells"]), ["s3/probe_sample"])
        self.assertEqual(contract["owners"]["coefficients"], {"puts_per_compact_part": REPORT})
        self.assertTrue((self.tmp / "measured/_coefficient_only.json").exists())

    def test_generate_twice_gives_identical_bytes(self):
        self.generate()
        first = self.outputs()
        self.generate()
        self.assertEqual(self.outputs(), first)

    def test_outputs_do_not_depend_on_the_mode(self):
        # Coverage with a cell that has no evidence: dev reports it pending, final fails, both render the same bytes.
        self.edit_json(self.cm / "coverage.json", lambda cov: cov["domain"]["operations"].append("merges"))
        opts = gc.options(type("A", (), {k: str(v) for k, v in {
            "cost_model": self.cm, "reports": self.tmp / "reports/*-storage-cost-*", "page": self.page, "doc": self.doc,
            "copy": self.copy, "code_dir": self.tmp / "code", "fixtures_out": self.tmp / "measured"}.items()}))
        dev, final = gc.build(opts, "dev"), gc.build(opts, "final")
        self.assertEqual(dev[0], final[0])
        self.assertIn("cell s3/merges/write_path: no evidence and no unconditional not_supported state", final[2])
        self.assertIn("2 support cells have no evidence and no not_supported state", dev[3])  # ebs and s3 merges

    def test_check_detects_stale_embedded_copy_and_doc_block(self):
        self.generate()
        code, log = self.run_gc("check")
        self.assertEqual(code, 0, log)
        for path, old, new, expect in (
                (self.page, '"evidence_statement":"Built', '"evidence_statement":"Edited', f"stale: {self.page} differs from a fresh render (block page)"),
                (self.doc, "`parts_inserted × puts_per_compact_part`", "`2 × puts_per_compact_part`", f"stale: {self.doc} differs from a fresh render (block formulas)"),
                (self.copy, '"version": "', '"version": "x', f"stale: {self.copy} differs from a fresh render")):
            with self.subTest(path=path.name):
                text = path.read_text()
                self.assertIn(old, text)
                path.write_text(text.replace(old, new, 1))
                code, log = self.run_gc("check")
                self.assertEqual(code, 1)
                self.assertIn(expect, log)
                path.write_text(text)
        self.assertEqual(self.run_gc("check")[0], 0)

    def test_duplicate_coefficient_owner_fails(self):
        beta = self.tmp / "reports/2026-10-24-storage-cost-beta"
        beta.mkdir()
        shutil.copy(self.rep / "coefficients.json", beta)
        (beta / "README.md").write_text("# Beta\n")
        code, log = self.run_gc("generate")
        self.assertEqual(code, 1)
        self.assertIn(f"coefficient puts_per_compact_part is owned by both {REPORT} and 2026-10-24-storage-cost-beta", log)
        self.assertFalse(self.copy.exists(), "nothing is written when a problem is found")

    def test_duplicate_cell_owner_fails(self):
        def twice(doc):
            doc.append(dict(doc[0], owner="other"))
        self.edit_json(self.rep / "formulas.json", twice)
        code, log = self.run_gc("generate")
        self.assertEqual(code, 1)
        self.assertIn("cell s3.inserts is defined by both alpha", log)

    def test_cut_card_with_nonzero_default_fails(self):
        def cut(cov):
            for case in cov["cards"][0]["cases"]:
                case["status"] = "cut"
            for e in cov["evidence"]:
                e["status"] = "cut"
        self.edit_json(self.cm / "coverage.json", cut)
        self.edit_json(self.cm / "inputs-registry.json", lambda reg: reg["quantities"][1].update(default=3))
        code, log = self.run_gc("generate")
        self.assertEqual(code, 1)
        self.assertIn("puts_per_compact_part: non-zero default 3 for a cut coefficient", log)
        self.assertIn("puts_per_compact_part: depends only on cut", log)

    def test_p1_without_holdout_fails(self):
        self.edit_json(self.cm / "coverage.json", lambda cov: cov["evidence"].pop(1))
        code, log = self.run_gc("generate")
        self.assertEqual(code, 1)
        self.assertIn("puts_per_compact_part: P1 coefficient has no done holdout row", log)

    def test_manifest_includes_evaluator_sha_and_raw_digests(self):
        m = self.generate()["manifest"]
        code = self.tmp / "code"
        files = {n: hashlib.sha256((code / n).read_bytes()).hexdigest() for n in sorted(gc.CODE_FILES) if (code / n).exists()}
        self.assertEqual(m["code_files"], files)
        self.assertIn("model.js", files)
        self.assertEqual(m["code_sha256"], hashlib.sha256("".join(f"{h}  {n}\n" for n, h in files.items()).encode()).hexdigest())
        self.assertEqual(m["code_missing"], sorted(set(gc.CODE_FILES) - set(files)))
        record = json.loads((self.rep / "measurements.jsonl").read_text().splitlines()[0])
        self.assertEqual(m["evidence"], [{
            "report": REPORT, "commit": record["commit"], "rig_commit": record["rig_commit"], "binary_sha256": record["binary_sha256"],
            "raw_sha256sums": hashlib.sha256((self.rep / "raw/SHA256SUMS").read_bytes()).hexdigest()}])
        self.assertEqual((m["registry_version"], m["coverage_version"], m["billing_version"]), ("reg-1", "cov-1", "bill-1"))
        self.assertEqual(m["contract_version"], json.loads(self.copy.read_text())["version"])
        # An evaluator change without regeneration is a reproduction failure that names the file.
        with open(code / "billing.js", "a") as f:
            f.write("\n// edited\n")
        status, log = self.run_gc("check")
        self.assertEqual(status, 1)
        self.assertIn("manifest code_sha256 does not match the code files (changed: billing.js)", log)
        self.assertNotEqual(self.generate()["manifest"]["code_sha256"], m["code_sha256"])

    def test_closure_checks_fail_on_injected_defects(self):
        def dependency():
            self.edit_json(self.rep / "formulas.json",
                           lambda doc: doc[0]["cells"]["inserts"]["ledger_rows"][0]["expr"]["a"].update(id="parts_missing"))

        def support():
            def cut(cov):
                cov["cards"][0]["cases"][1]["status"] = "cut"
                cov["evidence"][1]["status"] = "cut"
                cov["evidence"][0]["status"] = "cut"
            self.edit_json(self.cm / "coverage.json", cut)
            self.edit_json(self.cm / "inputs-registry.json", lambda reg: reg["quantities"][1].update(status="unsupported"))

        def evidence():
            self.edit_json(self.rep / "coefficients.json", lambda cs: cs[0]["evidence"].update(commit="0badc0de"))

        def accounting():
            self.edit_json(self.cm / "billing-contract.json", lambda b: b.update(version="bill-2"))

        def reproduction():
            (self.rep / "raw/SHA256SUMS").write_text((self.rep / "raw/SHA256SUMS").read_text() + "\n")

        cases = (
            (dependency, False, "unknown input parts_missing"),
            (support, False, "cell s3/inserts depends on cut coefficient puts_per_compact_part and is not marked not_supported"),
            (evidence, True, f"puts_per_compact_part: provenance commit 0badc0de differs from {REPORT} records (78133cca87d)"),
            (accounting, True, "model-contract.json billing (version bill-1) differs from billing-contract.json (version bill-2)"),
            (reproduction, True, f"{REPORT}: raw/SHA256SUMS digest"),
        )
        for inject, after_generate, expect in cases:
            with self.subTest(check=inject.__name__):
                self.tearDown()
                self.setUp()
                if after_generate:
                    self.generate()
                inject()
                code, log = self.run_gc("check")
                self.assertEqual(code, 1, log)
                self.assertIn(expect, log)

    def test_not_supported_entry_on_a_performed_operation_fails(self):
        self.edit_json(self.rep / "formulas.json", lambda doc: doc[0].update(applicability={"inserts": {"write_path": "not_supported"}}))
        code, log = self.run_gc("generate")
        self.assertEqual(code, 1)
        self.assertIn("applicability s3.inserts.write_path is not_supported but the s3 cell of inserts performs the operation", log)

    def test_family_measured_only_by_cut_cases_is_unsupported_cut(self):
        def grow(cov):
            cov["domain"]["families"].append("faults")
            cov["cards"].append({"name": "faults", "family": "faults", "model_variants": ["s3"],
                                 "cases": [dict(cov["cards"][0]["cases"][0], case_id="put-5xx", status="cut", priority="P2")]})
        self.edit_json(self.cm / "coverage.json", grow)
        app = self.generate()["applicability"]
        self.assertEqual(app["s3"]["inserts"], {"write_path": "supported", "faults": "unsupported_cut"})
        self.assertEqual(app["ebs"]["inserts"], {"write_path": "not_supported", "faults": "not_supported"})

    def test_unknown_non_domain_cell_fails(self):
        def rename(doc):
            doc[0]["cells"]["bogus"] = doc[0]["cells"].pop("probe_sample")
        self.edit_json(self.rep / "formulas.json", rename)
        code, log = self.run_gc("generate")
        self.assertEqual(code, 1)
        self.assertIn("cell bogus is neither a domain operation nor a card or probe_* cell", log)

    def test_final_mode_fails_on_what_dev_reports_pending(self):
        self.edit_json(self.cm / "coverage.json", lambda cov: cov["domain"]["variants"].append("cas"))
        (self.rep / "raw/SHA256SUMS").unlink()
        code, log = self.run_gc("generate")
        self.assertEqual(code, 0, log)
        self.assertIn(f"pending: {REPORT}: raw/SHA256SUMS missing", log)
        self.assertIn("pending: code file presets.js missing", log)
        self.assertIn("pending: 1 support cells have no evidence", log)
        self.assertEqual(json.loads(self.copy.read_text())["manifest"]["evidence"][0]["raw_sha256sums"], "pending")
        code, log = self.run_gc("generate", mode="final")
        self.assertEqual(code, 1)
        for expect in (f"{REPORT}: raw/SHA256SUMS missing", "code file presets.js missing", "missing report: variant cas has no formulas.json",
                       "cell cas/inserts/write_path: no evidence and no unconditional not_supported state"):
            self.assertIn(f"generate_contract: {expect}", log)

    def test_page_must_load_the_hashed_code_in_order(self):
        text = self.page.read_text()
        self.page.write_text(text.replace('<script src="pipeline.js"></script>\n', ""))
        code, log = self.run_gc("generate")
        self.assertEqual(code, 1)
        self.assertIn("script tags", log)

    def test_missing_marker_fails(self):
        self.doc.write_text(self.doc.read_text().replace("<!-- inputs:end -->", ""))
        code, log = self.run_gc("generate")
        self.assertEqual(code, 1)
        self.assertIn("needs exactly one <!-- inputs:begin --> ... <!-- inputs:end --> pair", log)


if __name__ == "__main__":
    unittest.main()
