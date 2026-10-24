"""Tests for generate_contract.py on the synthetic cost model under test/fixtures/contract-sample."""

import contextlib
import hashlib
import io
import json
import re
import shutil
import tempfile
import unittest
import unittest.mock
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
        if command == "check":
            # The checked-in Node suite is not about this sample; final mode lists the flag as one more problem.
            args.append("--skip-node")
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

    def assert_failure(self, log, text):
        self.assertTrue(any(text in line and "pending:" not in line for line in log.splitlines()), log)

    def assert_pending(self, log, text):
        self.assertTrue(any(text in line and "pending:" in line for line in log.splitlines()), log)
        self.assertFalse(any(text in line and "pending:" not in line for line in log.splitlines()), log)

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
        self.edit_json(self.rep / "formulas.json", lambda doc: doc[0]["applicability"].update(merges={"write_path": "supported"}))
        opts = gc.options(type("A", (), {k: str(v) for k, v in {
            "cost_model": self.cm, "reports": self.tmp / "reports/*-storage-cost-*", "page": self.page, "doc": self.doc,
            "copy": self.copy, "code_dir": self.tmp / "code", "fixtures_out": self.tmp / "measured"}.items()}))
        dev, final = gc.build(opts, "dev"), gc.build(opts, "final")
        self.assertEqual(dev[0], final[0])
        self.assertIn("cell s3/merges/write_path: no evidence and no unconditional not_supported state", final[2])
        # ebs lists no family for merges, so only s3 needs evidence there.
        self.assertIn("1 support cells have no evidence and no not_supported state", dev[3])

    def test_check_detects_stale_embedded_copy_and_doc_block(self):
        self.generate()
        code, log = self.run_gc("check")
        self.assertEqual(code, 0, log)
        hint = "; run tools/generate_contract.py generate"
        for path, old, new, expect in (
                (self.page, '"evidence_statement":"Measured', '"evidence_statement":"Edited', f"stale: {self.page} differs from a fresh render (block page){hint}"),
                (self.doc, "`parts_inserted × puts_per_compact_part`", "`2 × puts_per_compact_part`", f"stale: {self.doc} differs from a fresh render (block formulas){hint}"),
                (self.copy, '"version": "', '"version": "x', f"stale: {self.copy} differs from a fresh render{hint}")):
            with self.subTest(path=path.name):
                text = path.read_text()
                self.assertIn(old, text)
                path.write_text(text.replace(old, new, 1))
                # Dev mode reports a stale file as pending; final mode fails on it.
                code, log = self.run_gc("check")
                self.assertEqual(code, 0, log)
                self.assert_pending(log, expect)
                code, log = self.run_gc("check", mode="final")
                self.assertEqual(code, 1)
                self.assert_failure(log, expect)
                path.write_text(text)
        code, log = self.run_gc("check")
        self.assertEqual(code, 0, log)
        self.assertNotIn("stale:", log)

    def write_breakeven(self, version, crossing):
        state = lambda *xs: {v: list(xs) for v in ("ebs", "s3")}
        doc = {"schema": "breakeven/1", "contract_version": version, "price_preset": "aws-us-east-1",
               "ladder_rule": "×1/4 … ×8 of each scenario's value",
               "scenarios": {"logs": {"label": "Logs", "sweeps": {
                   "data_bytes": {"values": [64, 256], "points": [], "states": state("ok"), "crossing": crossing, "problems": []},
                   "replicas": {"values": [1, 2], "points": [], "states": state("unavailable"), "crossing": None, "problems": []}}}}}
        (self.cm / "breakeven.json").write_text(json.dumps(doc, indent=1))

    def test_breakeven_block_rendered_and_checked(self):
        version = self.generate()["version"]
        # Absent file: pending in dev mode, a failure in final mode; the block says so.
        code, log = self.run_gc("check")
        self.assertEqual(code, 0, log)
        self.assert_pending(log, "breakeven.json is missing")
        self.assertIn("Not generated", self.doc.read_text())
        code, log = self.run_gc("check", mode="final")
        self.assert_failure(log, "breakeven.json is missing")

        crossing = [{"a": "ebs", "b": "s3", "x_between": [64, 256], "extrapolated": True}]
        self.write_breakeven(version, crossing)
        contract = self.generate()
        self.assertEqual(contract["version"], version, "sweeping must not change the version it names")
        self.assertEqual(contract["manifest"]["breakeven_sha256"], hashlib.sha256((self.cm / "breakeven.json").read_bytes()).hexdigest())
        doc = self.doc.read_text()
        self.assertIn("### Scenario `logs` {#breakeven-logs}", doc)
        self.assertIn("Ladders: ×1/4 … ×8 of each scenario's value.", doc)
        self.assertIn("| `data_bytes` | 64, 256 | `ebs`, `s3` | `ebs` / `s3` between 64 and 256 (extrapolated) | `ebs`: ok; `s3`: ok |", doc)
        self.assertIn("| `replicas` | 1, 2 | none, fewer than two variants have a total | none | `ebs`: unavailable; `s3`: unavailable |", doc)
        code, log = self.run_gc("check", mode="final")
        self.assertNotIn("breakeven", log, log)

        # An edited file no longer matches the manifest or the rendered block.
        self.write_breakeven(version, None)
        code, log = self.run_gc("check", mode="final")
        self.assertEqual(code, 1, log)
        self.assert_failure(log, "differs from a fresh render")
        # A file swept on another contract version is rejected, not rendered.
        self.write_breakeven("0000000000000000", crossing)
        code, log = self.run_gc("check", mode="final")
        self.assertEqual(code, 1, log)
        self.assert_failure(log, "was swept on contract version 0000000000000000")
        code, log = self.run_gc("generate", mode="final")
        self.assertEqual(code, 1, log)

    def test_check_renders_once(self):
        self.generate()
        calls = []
        real = gc.build

        def counted(*args, **kwargs):
            calls.append(args)
            return real(*args, **kwargs)
        with unittest.mock.patch.object(gc, "build", counted):
            code, log = self.run_gc("check")
        self.assertEqual(code, 0, log)
        self.assertEqual(len(calls), 1, "the closure check reuses the render of check")

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
        for key, name in (("registry", "inputs-registry.json"), ("coverage", "coverage.json"), ("billing", "billing-contract.json")):
            self.assertEqual(m[f"{key}_sha256"], hashlib.sha256((self.cm / name).read_bytes()).hexdigest())
        self.assertEqual(m["contract_version"], json.loads(self.copy.read_text())["version"])
        # An evaluator change without regeneration is a reproduction failure that names the file.
        with open(code / "billing.js", "a") as f:
            f.write("\n// edited\n")
        status, log = self.run_gc("check", mode="final")
        self.assertEqual(status, 1)
        self.assert_failure(log, "stale: manifest code_sha256 does not match the code files (changed: billing.js)")
        self.assertNotEqual(self.generate()["manifest"]["code_sha256"], m["code_sha256"])
        # An input edited without a version bump is stale too: the manifest names its sha256, not only its version.
        (self.cm / "coverage.json").write_text((self.cm / "coverage.json").read_text() + "\n")
        status, log = self.run_gc("check", mode="final")
        self.assert_failure(log, "stale: manifest coverage_sha256 differs from the sha256 of coverage.json")
        status, log = self.run_gc("check")
        self.assertEqual(status, 0, log)
        self.assert_pending(log, "stale: manifest coverage_sha256 differs from the sha256 of coverage.json")

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
            (accounting, True, "stale: model-contract.json billing (version bill-1) differs from billing-contract.json (version bill-2)"),
            (reproduction, True, f"stale: {REPORT}: raw/SHA256SUMS digest"),
        )
        for inject, after_generate, expect in cases:
            with self.subTest(check=inject.__name__):
                self.tearDown()
                self.setUp()
                if after_generate:
                    self.generate()
                inject()
                code, log = self.run_gc("check", mode="final")
                self.assertEqual(code, 1, log)
                self.assert_failure(log, expect)

    def test_not_supported_entry_on_a_performed_operation_fails(self):
        self.edit_json(self.rep / "formulas.json", lambda doc: doc[0].update(applicability={"inserts": {"write_path": "not_supported"}}))
        code, log = self.run_gc("generate")
        self.assertEqual(code, 1)
        self.assertIn("applicability s3.inserts.write_path is not_supported but the s3 cell of inserts performs the operation", log)

    def test_listed_family_measured_only_by_cut_cases_is_unsupported_cut(self):
        def grow(cov):
            cov["domain"]["families"] += ["faults", "reads_cache"]
            for fam in ("faults", "reads_cache"):
                cov["cards"].append({"name": fam, "family": fam, "model_variants": ["s3"],
                                     "cases": [dict(cov["cards"][0]["cases"][0], case_id="c", status="cut", priority="P2")]})
        self.edit_json(self.cm / "coverage.json", grow)
        self.edit_json(self.rep / "formulas.json", lambda doc: doc[0]["applicability"]["inserts"].update(faults="supported"))
        app = self.generate()["applicability"]
        self.assertEqual(app["s3"]["inserts"], {"write_path": "supported", "faults": "unsupported_cut", "reads_cache": "not_applicable"})
        self.assertEqual(app["ebs"]["inserts"], {"write_path": "not_supported", "faults": "not_supported", "reads_cache": "not_supported"})

    def test_unlisted_families_are_not_applicable_and_need_no_evidence(self):
        families = ["write_path", "faults", "objects", "thresholds", "multipart", "lifecycle", "mutations", "reads",
                    "startup", "replication", "cas_branches", "cas_gc", "cas_lease"]

        def grow(cov):
            cov["domain"]["families"] = families
            for fam in families[1:]:
                cov["cards"].append({"name": fam, "family": fam, "model_variants": ["s3"],
                                     "cases": [dict(cov["cards"][0]["cases"][0], case_id="c", status="planned", priority="P2")]})
        self.edit_json(self.cm / "coverage.json", grow)
        self.edit_json(self.rep / "formulas.json", lambda doc: doc[0]["applicability"]["inserts"].update(faults="supported"))
        contract = self.generate()
        row = contract["applicability"]["s3"]["inserts"]
        self.assertEqual(sorted(f for f, x in row.items() if x == "supported"), ["faults", "write_path"])
        self.assertEqual(sum(x == "not_applicable" for x in row.values()), 11)
        cov = json.loads((self.cm / "coverage.json").read_text())
        files = json.loads((self.rep / "formulas.json").read_text())
        # write_path has done evidence; faults is listed but has none; the other eleven need none.
        self.assertEqual(gc.closure_check._pending_cells(cov, files, contract["applicability"]), [("s3", "inserts", "faults")])
        self.assertEqual(len(gc.closure_check._pending_cells(cov, files)), 12, "without applicability every unbacked family needs evidence")

    def test_report_files_without_readme_fail(self):
        stray = self.tmp / "reports/2026-10-24-storage-cost-stray"
        stray.mkdir()
        shutil.copy(self.rep / "measurements.jsonl", stray)
        code, log = self.run_gc("generate")
        self.assertEqual(code, 1)
        self.assert_failure(log, f"{stray}: has measurements.jsonl but no README.md")
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = gc.closure_check.main(["--mode", "dev", "--skip-node", "--cost-model", str(self.cm),
                                          "--reports", str(self.tmp / "reports/*-storage-cost-*")])
        self.assertEqual(code, 1)
        self.assertIn(f"{stray}: has measurements.jsonl but no README.md", err.getvalue())

    def test_invalid_record_is_a_problem_not_a_none_identity(self):
        lines = (self.rep / "measurements.jsonl").read_text().splitlines()
        first = json.loads(lines[0])
        del first["commit"]
        (self.rep / "measurements.jsonl").write_text("\n".join([json.dumps(first)] + lines[1:]) + "\n")
        code, log = self.run_gc("generate")
        self.assertEqual(code, 1)
        self.assert_failure(log, f"{REPORT}: record {first['measurement_id']} is invalid: $: missing field commit")
        self.assertNotIn("None", log)

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
        # setUp copies every calculator script; remove one so a missing code file is exercised.
        (self.tmp / "code" / "presets.js").unlink()
        pending = (f"{REPORT}: raw/SHA256SUMS missing", "code file presets.js missing",
                   "variant cas: no formulas file lists a supported family")
        code, log = self.run_gc("generate")
        self.assertEqual(code, 0, log)
        for expect in pending:
            self.assert_pending(log, expect)
        self.assertEqual(json.loads(self.copy.read_text())["manifest"]["evidence"][0]["raw_sha256sums"], "pending")
        code, log = self.run_gc("generate", mode="final")
        self.assertEqual(code, 1)
        for expect in pending + ("missing report: variant cas has no formulas.json",):
            self.assert_failure(log, f"generate_contract: {expect}")

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

    def edit_input(self, fn):
        def go(doc):
            for q in doc["quantities"]:
                if q["kind"] == "input":
                    fn(q)
        self.edit_json(self.cm / "inputs-registry.json", go)

    def test_input_without_ui_tier_fails(self):
        self.edit_input(lambda q: q.pop("ui"))
        code, log = self.run_gc("generate")
        self.assertEqual(code, 1)
        self.assert_failure(log, "registry parts_inserted: input has no ui tier")

    def test_ui_short_label_over_40_characters_fails(self):
        self.edit_input(lambda q: q["ui"].update(short="x" * 41))
        code, log = self.run_gc("generate")
        self.assertEqual(code, 1)
        self.assert_failure(log, "registry parts_inserted: ui.short is 41 characters, at most 40")

    def test_unknown_ui_tier_fails(self):
        self.edit_input(lambda q: q["ui"].update(tier="basic"))
        code, log = self.run_gc("generate")
        self.assertEqual(code, 1)
        self.assertIn("ui.tier: expected one of", log)

    def test_contract_carries_measured_by_and_the_doc_the_placement_table(self):
        contract = self.generate()
        q = next(x for x in contract["registry"] if x["id"] == "parts_inserted")
        self.assertEqual(q["ui"]["tier"], "workload")
        self.assertTrue(q["ui"]["measured_by"].startswith(("Assumed:", "Default from", "Provider rule:", "Measured:")), q["ui"])
        self.assertIn("| `workload` | `parts_inserted` |  | Parts inserted | `simple` |", self.doc.read_text())
        # Cells without rows count nowhere; the s3 cell rests on a coefficient with a done fit and hold-out.
        self.assertEqual(contract["evidence_by_variant"], {"ebs": {"fit_and_holdout": 0, "fit_only": 0, "assumption": 0},
                                                           "s3": {"fit_and_holdout": 1, "fit_only": 0, "assumption": 0}})
        self.assertTrue(contract["evidence_statement"].startswith("Measured on ClickHouse commit 78133cca87d (release 26.8) against a RustFS S3 store."))


class RegistryUiTest(unittest.TestCase):
    def registry(self, settings):
        qs = [{"id": f"s{i}", "kind": "input", "ui": {"tier": "setting", "name": None, "short": "x"}} for i in range(settings)]
        return {"quantities": qs + [{"id": "m", "kind": "measured"}]}

    def test_setting_tier_capped_at_ten(self):
        self.assertEqual(gc.registry_ui_problems(self.registry(10)), [])
        self.assertEqual(len(gc.registry_ui_problems(self.registry(11))), 1)
        self.assertIn("11 inputs in the setting tier, at most 10", gc.registry_ui_problems(self.registry(11))[0])

    def test_live_registry_passes(self):
        reg = json.loads((gc.DEFAULTS["cost_model"] / "inputs-registry.json").read_text())
        self.assertEqual(gc.registry_ui_problems(reg), [])
        tiers = {q["id"]: q["ui"]["tier"] for q in reg["quantities"] if q["kind"] == "input"}
        for workload in ("data_bytes", "ingest_rate", "retention_days", "replicas", "shards", "topology"):
            self.assertEqual(tiers[workload], "workload", workload)
        for table_key in ("io_op", "io_direction", "io_size_bucket", "cas_write", "cas_branch", "gc_stage", "gc_round_term"):
            self.assertEqual(tiers[table_key], "hidden", table_key)

    def test_cite_names_the_measured_build(self):
        self.assertEqual(gc.cite("see tmp/src-26.8/src/Core/Settings.cpp:178 and src/A.cpp:1-3", "09d0772a842"),
                         "see src/Core/Settings.cpp:178 @ 09d0772a842 and src/A.cpp:1-3 @ 09d0772a842")
        self.assertEqual(gc.cite("src/A.cpp:12 @ 09d0772a842", "09d0772a842"), "src/A.cpp:12 @ 09d0772a842")


if __name__ == "__main__":
    unittest.main()
