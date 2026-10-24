"""Every committed cost-model artifact passes both the hand-written validators (`artifacts.py`) and the JSON Schema
documents under `docs/superpowers/cost-model/schemas`, so the two cannot drift apart unnoticed.

Needs the `jsonschema` package (with `referencing`); a missing package fails the suite rather than skipping it.
"""
import gzip
import itertools
import json
import unittest
from pathlib import Path

import artifacts
import closure_check

ROOT = Path(__file__).resolve().parents[3]
COST_MODEL = ROOT / "docs/superpowers/cost-model"
SCHEMAS = COST_MODEL / "schemas"
REPORTS = sorted(p for p in (ROOT / "docs/superpowers/reports").glob("*-storage-cost-*") if (p / "README.md").exists())
# Ledger slices are large; the first rows of a few slices per report exercise every row kind the proxy writes.
LEDGER_SLICES_PER_REPORT, LEDGER_ROWS_PER_SLICE = 3, 300


def schema_validator(name):
    import jsonschema
    import referencing
    from referencing.jsonschema import DRAFT202012
    store = referencing.Registry().with_resources(
        [(p.name, DRAFT202012.create_resource(json.loads(p.read_text()))) for p in SCHEMAS.glob("*.schema.json")])
    v = jsonschema.Draft202012Validator(json.loads((SCHEMAS / f"{name}.schema.json").read_text()), registry=store)
    return lambda d: [f"{'/'.join(map(str, e.absolute_path)) or '$'}: {e.message[:200]}" for e in itertools.islice(v.iter_errors(d), 3)]


def load(path):
    return json.loads(Path(path).read_text())


class CommittedArtifactsTest(unittest.TestCase):
    maxDiff = None

    def both(self, name, check, items):
        """`items` yields (label, document); each must pass `check` and the `name` schema."""
        schema, problems, count = schema_validator(name), [], 0
        for label, d in items:
            count += 1
            problems += [f"{label}: {p}" for p in check(d)]
            problems += [f"{label} (schema): {p}" for p in schema(d)]
        self.assertGreater(count, 0, f"no {name} artifact found")
        self.assertEqual(problems[:20], [], f"{len(problems)} problem(s) in {count} {name} artifact(s)")

    def test_registry(self):
        self.both("registry", artifacts.validate_registry, [("inputs-registry.json", load(COST_MODEL / "inputs-registry.json"))])

    def test_coverage(self):
        self.both("coverage", artifacts.validate_coverage, [("coverage.json", load(COST_MODEL / "coverage.json"))])

    def test_billing(self):
        # billing/1 has no JSON Schema document; the closure check is its only shape validator.
        self.assertEqual(closure_check.billing(load(COST_MODEL / "billing-contract.json")), [])

    def test_formulas_with_the_tables_they_read(self):
        coefficients = [c for r in REPORTS if (r / "coefficients.json").exists() for c in load(r / "coefficients.json")]
        tables = dict(artifacts.table_arities_from_registry(load(COST_MODEL / "inputs-registry.json")))
        tables.update(artifacts.table_arities_from_coefficients(coefficients))

        def files():
            for r in REPORTS:
                if (r / "formulas.json").exists():
                    d = load(r / "formulas.json")
                    for i, f in enumerate(d if isinstance(d, list) else [d]):
                        yield f"{r.name}/formulas.json[{i}]", f
        self.both("formulas", lambda f: artifacts.validate_formulas(f, tables), files())

    def test_measurements(self):
        def records():
            for r in REPORTS:
                if (r / "measurements.jsonl").exists():
                    for n, line in enumerate((r / "measurements.jsonl").read_text().splitlines(), 1):
                        if line.strip():
                            yield f"{r.name}/measurements.jsonl:{n}", json.loads(line)
        self.both("measurement", artifacts.validate_measurement, records())

    def test_predictions(self):
        self.both("prediction", artifacts.validate_prediction,
                  ((f"{r.name}/{p.name}", load(p)) for r in REPORTS for p in sorted((r / "predictions").glob("*.json"))))

    def test_experiments(self):
        self.both("experiment", artifacts.validate_experiment,
                  ((f"{r.name}/{p.name}", load(p)) for r in REPORTS for p in sorted((r / "experiments").glob("*.json"))))

    def test_ledger_rows_sample(self):
        def rows():
            for r in REPORTS:
                for g in sorted((r / "raw").rglob("ledger.jsonl.gz"))[:LEDGER_SLICES_PER_REPORT]:
                    with gzip.open(g, "rt") as f:
                        for n, line in enumerate(itertools.islice(f, LEDGER_ROWS_PER_SLICE), 1):
                            yield f"{g.relative_to(ROOT)}:{n}", json.loads(line)
        self.both("ledger-row", artifacts.validate_ledger_row, rows())


if __name__ == "__main__":
    unittest.main()
