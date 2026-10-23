"""Tests for the artifact validators; examples under cost-model/schemas/examples are the fixtures."""

import copy
import json
import unittest
from pathlib import Path

import artifacts

EXAMPLES = Path(__file__).resolve().parents[3] / "docs/superpowers/cost-model/schemas/examples"


def load(name):
    with open(EXAMPLES / name) as f:
        return json.load(f)


def without(d, path):
    """Deep copy of d with the key at path (tuple of keys) removed."""
    d = copy.deepcopy(d)
    node = d
    for key in path[:-1]:
        node = node[key]
    del node[path[-1]]
    return d


def with_value(d, path, value):
    d = copy.deepcopy(d)
    node = d
    for key in path[:-1]:
        node = node[key]
    node[path[-1]] = value
    return d


class ArtifactTestCase(unittest.TestCase):
    def assert_valid(self, validator, d):
        self.assertEqual(validator(d), [])

    def assert_each_detected(self, validator, example, paths):
        self.assertEqual(validator(example), [], "example must be valid")
        for path in paths:
            problems = validator(without(example, path))
            self.assertTrue(problems, f"removing {path} not detected")
            joined = "\n".join(problems)
            self.assertIn(path[-1], joined, f"problem for {path} does not name the field: {joined}")

    def assert_rejects(self, validator, d, needle):
        problems = validator(d)
        self.assertTrue(any(needle in p for p in problems), f"expected {needle!r} in {problems}")


def top_level(d):
    return [(k,) for k in d]


class LedgerRowTests(ArtifactTestCase):
    def test_ledger_row_example_valid_and_each_required_field_detected(self):
        attempt = load("ledger-row-attempt.json")
        mark = load("ledger-row-mark.json")
        nested = [
            ("downstream", "outcome"), ("downstream", "status"),
            ("upstream", "outcome"), ("upstream", "http_status"), ("upstream", "s3_code"),
            ("copy_source", "bucket"),
            ("response_body", "upload_id"), ("response_body", "etag"), ("response_body", "complete_error"),
            ("response_body", "completed_parts"), ("response_body", "delete_results"), ("response_body", "list"),
        ]
        attempt_with_copy = with_value(attempt, ("copy_source",), {"bucket": "b", "key": "k", "range": None})
        self.assert_each_detected(artifacts.validate_ledger_row, attempt, top_level(attempt))
        self.assert_each_detected(artifacts.validate_ledger_row, mark, top_level(mark))
        paths = [p for p in nested if p[0] != "copy_source"]
        self.assert_each_detected(artifacts.validate_ledger_row, attempt, paths)
        self.assert_each_detected(artifacts.validate_ledger_row, attempt_with_copy, [("copy_source", "bucket")])

    def test_ledger_row_rejects_bad_enums_and_unknown_kind(self):
        attempt = load("ledger-row-attempt.json")
        self.assert_rejects(artifacts.validate_ledger_row, with_value(attempt, ("op",), "Frobnicate"), "op")
        self.assert_rejects(artifacts.validate_ledger_row, with_value(attempt, ("phase",), "later"), "phase")
        self.assert_rejects(artifacts.validate_ledger_row, with_value(attempt, ("caller",), "r9"), "caller")
        self.assert_rejects(artifacts.validate_ledger_row, {"kind": "row"}, "kind")


class MeasurementTests(ArtifactTestCase):
    COMMON = ["schema", "measurement_id", "experiment_id", "run_id", "card", "case_id", "policy", "variant",
              "cache_enabled", "replicas", "k", "scale", "mode", "binary_sha256", "commit", "pass_dir", "status"]

    def test_measurement_example_valid_and_each_required_field_detected(self):
        ok = load("measurement-ok.json")
        self.assert_each_detected(artifacts.validate_measurement, ok, top_level(ok))
        nested = [
            ("effective_settings", "table"), ("effective_settings", "query"), ("effective_settings", "server"),
            ("window", "start_seq"), ("window", "end_seq"), ("window", "t_start_us"), ("window", "t_end_us"),
            ("window", "crossing"),
            ("ledger", "foreground"), ("ledger", "maintenance"), ("ledger", "setup"), ("ledger", "teardown"),
            ("ledger", "rig"),
            ("inventory", "initial"), ("inventory", "byte_hours"), ("inventory", "uncertain_identities"),
            ("proc_io", "r1", "rchar"), ("proc_io", "r1", "write_bytes"),
            ("generations", "r1", 0, "pid"),
            ("local", "r1", "peak_allocated_bytes"), ("local", "r1", "attribution"), ("local", "r1", "trace"),
            ("local", "r1", "limitations"),
            ("interserver", "relay0", "open_at_end"),
            ("assertions", "a1", "ok"),
            ("ledger", "foreground", "r1", "PutObject", "attempts"),
            ("ledger", "foreground", "r1", "PutObject", "delete_keys"),
        ]
        self.assert_each_detected(artifacts.validate_measurement, ok, nested)

    def test_measurement_unsupported_requires_reason(self):
        u = load("measurement-unsupported.json")
        self.assert_each_detected(artifacts.validate_measurement, u, top_level(u))
        self.assert_rejects(artifacts.validate_measurement,
                            with_value(u, ("effective_counts",), {}), "effective_counts")

    def test_measurement_failed_shape_requires_diagnostics_and_allows_partial(self):
        f = load("measurement-failed.json")
        self.assertIn("partial", f)
        required = [k for k in f if k != "partial"]
        self.assert_each_detected(artifacts.validate_measurement, f, [(k,) for k in required])
        self.assert_each_detected(artifacts.validate_measurement, f,
                                  [("diagnostics", "server_logs"), ("diagnostics", "error_text")])
        self.assert_valid(artifacts.validate_measurement, without(f, ("partial",)))
        self.assert_rejects(artifacts.validate_measurement,
                            with_value(f, ("partial", "bogus"), 1), "bogus")
        self.assert_rejects(artifacts.validate_measurement,
                            with_value(f, ("partial", "window"), {"start_seq": 1}), "window")

    def test_measurement_id_must_match_its_parts(self):
        ok = load("measurement-ok.json")
        self.assert_rejects(artifacts.validate_measurement, with_value(ok, ("k",), 7), "measurement_id")
        self.assert_rejects(artifacts.validate_measurement, with_value(ok, ("experiment_id",), "x.y"), "experiment_id")

    def test_measurement_rejects_unknown_status_and_variant(self):
        ok = load("measurement-ok.json")
        self.assert_rejects(artifacts.validate_measurement, with_value(ok, ("status",), "MAYBE"), "status")
        self.assert_rejects(artifacts.validate_measurement, with_value(ok, ("variant",), "nfs"), "variant")


class PredictionTests(ArtifactTestCase):
    def test_prediction_example_valid_and_each_required_field_detected(self):
        p = load("prediction.json")
        self.assert_each_detected(artifacts.validate_prediction, p, top_level(p))
        self.assert_each_detected(artifacts.validate_prediction, p, [
            ("expected", "foreground"), ("expected", "observations"),
            ("tolerance", "abs"), ("tolerance", "rel"),
            ("source_refs", 0, "claim"), ("source_refs", 0, "path"), ("source_refs", 0, "line"),
        ])

    def test_prediction_accepts_int_or_min_max_and_rejects_others(self):
        p = load("prediction.json")
        self.assert_rejects(artifacts.validate_prediction,
                            with_value(p, ("expected", "foreground", "r1", "PutObject"), {"min": 1}), "max")
        self.assert_rejects(artifacts.validate_prediction,
                            with_value(p, ("expected", "foreground", "r1", "PutObject"), "3"), "PutObject")


class ExperimentTests(ArtifactTestCase):
    def test_experiment_example_valid_and_each_required_field_detected(self):
        e = load("experiment.json")
        self.assert_each_detected(artifacts.validate_experiment, e, top_level(e))
        self.assert_each_detected(artifacts.validate_experiment, e, [
            ("controls", "merges"), ("controls", "gc"), ("controls", "ttl"),
            ("settings", "table"), ("settings", "query"), ("settings", "server"),
        ])

    def test_experiment_rejects_bad_controls_and_id(self):
        e = load("experiment.json")
        self.assert_rejects(artifacts.validate_experiment, with_value(e, ("controls", "gc"), "on"), "gc")
        self.assert_rejects(artifacts.validate_experiment, with_value(e, ("experiment_id",), "nope"), "experiment_id")


class CoverageTests(ArtifactTestCase):
    def test_coverage_example_valid_and_each_required_field_detected(self):
        c = load("coverage.json")
        self.assert_each_detected(artifacts.validate_coverage, c, top_level(c))
        self.assert_each_detected(artifacts.validate_coverage, c, [
            ("cards", 0, "name"), ("cards", 0, "family"), ("cards", 0, "model_variants"),
            ("cards", 0, "cases", 0, "case_id"), ("cards", 0, "cases", 0, "priority"),
            ("cards", 0, "cases", 0, "status"), ("cards", 0, "cases", 0, "timeout_s"),
            ("evidence", 0, "coefficient"), ("evidence", 0, "span"), ("evidence", 0, "role"),
            ("evidence", 0, "prediction_commit"), ("evidence", 0, "measurement_ids"),
            ("domain", "variants"), ("domain", "operations"), ("domain", "families"),
        ])

    def test_coverage_evidence_requires_prediction_commit_for_holdout(self):
        c = load("coverage.json")
        roles = {e["role"]: i for i, e in enumerate(c["evidence"])}
        self.assertIn("holdout", roles)
        self.assertIn("fit", roles)
        h = roles["holdout"]
        self.assert_rejects(artifacts.validate_coverage,
                            with_value(c, ("evidence", h, "prediction_commit"), None), "prediction_commit")
        # A fit row may carry null; the same row relabelled as validation may not.
        f = roles["fit"]
        self.assert_rejects(artifacts.validate_coverage,
                            with_value(c, ("evidence", f, "role"), "validation"), "prediction_commit")
        self.assert_valid(artifacts.validate_coverage, with_value(c, ("evidence", f, "prediction_commit"), None))

    def test_coverage_rejects_bad_role_priority(self):
        c = load("coverage.json")
        self.assert_rejects(artifacts.validate_coverage, with_value(c, ("evidence", 0, "role"), "guess"), "role")
        self.assert_rejects(artifacts.validate_coverage,
                            with_value(c, ("cards", 0, "cases", 0, "priority"), "P9"), "priority")


class RegistryTests(ArtifactTestCase):
    def test_registry_example_valid_and_each_required_field_detected(self):
        r = load("registry.json")
        self.assert_each_detected(artifacts.validate_registry, r, top_level(r))
        quantity_fields = ["id", "block", "label", "kind", "scope", "unit", "value_type", "enum", "table",
                           "default", "rule", "coef", "validity", "evidence", "hint", "status"]
        for i, q in enumerate(r["quantities"]):
            for f in quantity_fields:
                self.assertIn(f, q, f"example quantity {q['id']} lacks {f}")
        self.assert_each_detected(artifacts.validate_registry, r,
                                  [("quantities", 0, f) for f in quantity_fields])
        self.assert_each_detected(artifacts.validate_registry, r, [
            ("quantities", 0, "evidence", "kind"), ("quantities", 0, "evidence", "ref"),
            ("quantities", 0, "evidence", "version"),
            ("precedence", 0, "group"), ("precedence", 0, "default_locked"), ("precedence", 0, "rules"),
        ])

    def test_registry_groups_do_not_share_members(self):
        r = load("registry.json")
        group = r["precedence"][0]["group"]
        dup = copy.deepcopy(r)
        dup["precedence"].append({"group": [group[0]], "default_locked": [], "rules": {}})
        self.assert_rejects(artifacts.validate_registry, dup, "shares member")

    def test_registry_rejects_inconsistent_value_type_and_duplicate_ids(self):
        r = load("registry.json")
        enum_idx = next(i for i, q in enumerate(r["quantities"]) if q["value_type"] == "enum")
        self.assert_rejects(artifacts.validate_registry,
                            with_value(r, ("quantities", enum_idx, "enum"), None), "enum")
        dup = copy.deepcopy(r)
        dup["quantities"].append(copy.deepcopy(dup["quantities"][0]))
        self.assert_rejects(artifacts.validate_registry, dup, "duplicate")
        deriv = next(i for i, q in enumerate(r["quantities"]) if q["kind"] == "derivation")
        self.assert_rejects(artifacts.validate_registry,
                            with_value(r, ("quantities", deriv, "rule"), None), "rule")


class FormulasTests(ArtifactTestCase):
    TABLES = {"objects_per_part": 2}

    def test_formulas_example_valid_and_each_required_field_detected(self):
        f = load("formulas.json")
        self.assert_each_detected(artifacts.validate_formulas, f, top_level(f))
        cell = next(iter(f["cells"]))
        self.assert_each_detected(artifacts.validate_formulas, f, [
            ("cells", cell, "ledger_rows"), ("cells", cell, "allocation"), ("cells", cell, "states"),
            ("cells", cell, "notes"),
            ("cells", cell, "ledger_rows", 0, "row_type"), ("cells", cell, "ledger_rows", 0, "op_kind"),
            ("cells", cell, "ledger_rows", 0, "outcome_class"), ("cells", cell, "ledger_rows", 0, "category"),
            ("cells", cell, "ledger_rows", 0, "route"), ("cells", cell, "ledger_rows", 0, "scope"),
            ("cells", cell, "ledger_rows", 0, "expr"),
            ("cells", cell, "ledger_rows", 0, "expr", "dim"),
            ("cells", cell, "states", 0, "id"), ("cells", cell, "states", 0, "cond"),
            ("cells", cell, "states", 0, "reason"),
        ])

    def test_worked_formula_examples_valid(self):
        for name in ("example-part-type.json", "example-objects-per-part.json", "example-multipart-parts.json"):
            node = load(name)
            self.assertEqual(artifacts.validate_node(node, "$", False, tables=self.TABLES), [], name)

    def test_formulas_rejects_var_outside_sum_over(self):
        var = {"kind": "var", "name": "part_type", "dim": "enum:part_type"}
        self.assert_rejects(lambda n: artifacts.validate_node(n, "$", False), var, "sum_over")
        wrapped = {"kind": "sum_over", "set": "part_types", "var": "part_type", "dim": "count", "body": var}
        self.assertEqual(artifacts.validate_node(wrapped, "$", False), [])
        wrong_name = {"kind": "sum_over", "set": "part_types", "var": "other", "dim": "count", "body": var}
        self.assert_rejects(lambda n: artifacts.validate_node(n, "$", False), wrong_name, "part_type")
        # The same rule applies through a formulas file.
        f = load("formulas.json")
        cell = next(iter(f["cells"]))
        bad = with_value(f, ("cells", cell, "ledger_rows", 0, "expr"), var)
        self.assert_rejects(artifacts.validate_formulas, bad, "sum_over")

    def test_formulas_lookup_requires_table_coefficient_arity(self):
        node = load("example-objects-per-part.json")
        lookup = node["body"]
        self.assertEqual(lookup["kind"], "lookup")
        one_key = copy.deepcopy(node)
        one_key["body"]["keys"] = one_key["body"]["keys"][:1]
        self.assert_rejects(lambda n: artifacts.validate_node(n, "$", False, tables=self.TABLES), one_key, "arity")
        unknown = copy.deepcopy(node)
        unknown["body"]["table_id"] = "nope"
        self.assert_rejects(lambda n: artifacts.validate_node(n, "$", False, tables=self.TABLES), unknown, "nope")
        no_keys = copy.deepcopy(node)
        no_keys["body"]["keys"] = []
        self.assert_rejects(lambda n: artifacts.validate_node(n, "$", False), no_keys, "keys")

    def test_formulas_tables_derive_from_registry_and_coefficients(self):
        r = load("registry.json")
        self.assertEqual(artifacts.table_arities_from_registry(r)["objects_per_part"], 2)
        coefs = [{"id": "t", "kind": "table", "value": {"index": ["a", "b", "c"], "values": {}}},
                 {"id": "s", "kind": "scalar", "value": 1}]
        self.assertEqual(artifacts.table_arities_from_coefficients(coefs), {"t": 3})

    def test_formulas_grammar_rejections(self):
        c = {"kind": "const", "value": 1, "dim": "count"}
        def bad(node, needle):
            self.assert_rejects(lambda n: artifacts.validate_node(n, "$", False), node, needle)
        bad({"kind": "frob", "dim": "count"}, "kind")
        bad({"kind": "const", "value": 1}, "dim")
        bad({"kind": "const", "value": True, "dim": "count"}, "value")
        bad({"kind": "div", "a": c, "b": c, "on_zero": "inf", "dim": "count"}, "on_zero")
        bad({"kind": "div", "a": c, "b": c, "dim": "count"}, "on_zero")
        bad({"kind": "min", "args": [], "dim": "count"}, "args")
        bad({"kind": "branch", "cond": {"op": "~", "a": c, "b": c}, "then": c, "else": c, "dim": "count"}, "op")
        bad({"kind": "add", "a": c, "dim": "count"}, "b")
        bad({"kind": "price", "unit": "usd", "rule_id": "r", "dim": "usd"}, "quantity")
        ok = {"kind": "const", "value": "unavailable", "dim": "count"}
        self.assertEqual(artifacts.validate_node(ok, "$", False), [])


if __name__ == "__main__":
    unittest.main()
