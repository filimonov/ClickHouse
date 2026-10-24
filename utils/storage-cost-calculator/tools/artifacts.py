"""Hand-written validators for the cost-model artifacts (docs/superpowers/cost-model/schemas).

Each `validate_*` returns a list of "<path>: <message>" strings; empty means valid.
The validators are the source of truth. The *.schema.json files document the shapes for readers.
Shapes are strict: unknown keys are problems, so a typo cannot pass as an optional field.
"""

CALLERS = ("r1", "r2", "r3", "rig")
VARIANTS = ("ebs", "s3", "s3plain", "cas")
OPS = (
    "PutObject", "CopyObject", "CreateMultipartUpload", "UploadPart", "UploadPartCopy",
    "CompleteMultipartUpload", "AbortMultipartUpload", "GetObject", "HeadObject", "ListObjectsV2",
    "ListMultipartUploads", "ListParts", "DeleteObject", "DeleteObjects", "CreateBucket", "HeadBucket",
    "Unsupported", "Other",
)
PHASES = ("setup", "window", "teardown", "outside")
COUNTS_FIELDS = (
    "attempts", "delivered", "dropped", "injected", "rejected", "upstream_ok", "upstream_error",
    "upstream_unknown", "req_payload_bytes", "resp_payload_bytes", "list_pages", "list_keys",
    "delete_calls", "delete_keys", "accepted_dropped", "upstream_4xx",
)
PROC_IO_FIELDS = ("rchar", "wchar", "syscr", "syscw", "read_bytes", "write_bytes")
CATEGORIES = ("active", "outdated", "garbage", "temporary", "incomplete", "detached", "metadata")
MODES = ("deterministic", "stochastic")


# --- combinators: each takes (value, path) and returns a problem list -------------------------

def _is_int(v):
    return isinstance(v, int) and not isinstance(v, bool)


def _is_num(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _typed(label, pred):
    def check(v, path):
        return [] if pred(v) else [f"{path}: expected {label}, got {_short(v)}"]
    return check


def _short(v):
    text = repr(v)
    return text if len(text) <= 40 else text[:37] + "..."


integer = _typed("integer", _is_int)
number = _typed("number", _is_num)
string = _typed("string", lambda v: isinstance(v, str))
nonempty_string = _typed("non-empty string", lambda v: isinstance(v, str) and v != "")
boolean = _typed("boolean", lambda v: isinstance(v, bool))
free_object = _typed("object", lambda v: isinstance(v, dict))
anything = lambda v, path: []


def enum(*values):
    def check(v, path):
        return [] if v in values and not isinstance(v, bool) else [f"{path}: expected one of {list(values)}, got {_short(v)}"]
    return check


def literal(value):
    return enum(value)


def nullable(inner):
    return lambda v, path: [] if v is None else inner(v, path)


def either(*alternatives):
    def check(v, path):
        results = [alt(v, path) for alt in alternatives]
        if any(not r for r in results):
            return []
        detail = "; ".join(p for r in results for p in r)
        return [f"{path}: matches none of the allowed shapes: {detail}"]
    return check


def list_of(inner, min_len=0):
    def check(v, path):
        if not isinstance(v, list):
            return [f"{path}: expected array, got {_short(v)}"]
        out = [f"{path}: expected at least {min_len} items"] if len(v) < min_len else []
        for i, item in enumerate(v):
            out += inner(item, f"{path}[{i}]")
        return out
    return check


def dict_of(inner):
    def check(v, path):
        if not isinstance(v, dict):
            return [f"{path}: expected object, got {_short(v)}"]
        out = []
        for key, item in v.items():
            out += inner(item, f"{path}.{key}")
        return out
    return check


def obj(spec):
    """Object with exactly the keys of spec; every key is required (nullable ones must still be present)."""
    def check(v, path):
        if not isinstance(v, dict):
            return [f"{path}: expected object, got {_short(v)}"]
        out = []
        for key, inner in spec.items():
            if key not in v:
                out.append(f"{path}: missing field {key}")
            else:
                out += inner(v[key], f"{path}.{key}")
        for key in v:
            if key not in spec:
                out.append(f"{path}: unexpected field {key}")
        return out
    return check


def pair(inner):
    def check(v, path):
        if not isinstance(v, list) or len(v) != 2:
            return [f"{path}: expected [lo, hi]"]
        return inner(v[0], f"{path}[0]") + inner(v[1], f"{path}[1]")
    return check


def _run(checker, v, path="$"):
    return checker(v, path)


def obj_optional(spec, optional):
    """Like obj, but keys in `optional` may be absent."""
    strict = obj({k: v for k, v in spec.items() if k not in optional})

    def check(v, path):
        if not isinstance(v, dict):
            return [f"{path}: expected object, got {_short(v)}"]
        extra = {k: v[k] for k in optional if k in v}
        base = {k: x for k, x in v.items() if k not in optional}
        out = strict(base, path)
        for key, value in extra.items():
            out += spec[key](value, f"{path}.{key}")
        return out
    return check


# --- ledger row --------------------------------------------------------------------------------

_counts_payload = integer

_ATTEMPT = {
    "kind": literal("attempt"), "seq": integer, "caller": enum(*CALLERS), "store_id": nonempty_string,
    "t_start_us": integer, "t_end_us": nullable(integer),
    "method": nonempty_string, "path": string, "query": free_object,
    "req_headers": dict_of(string), "resp_headers": dict_of(string),
    "bucket": string, "key": string, "op": enum(*OPS),
    "upload_id": nullable(string), "part_number": nullable(integer),
    "copy_source": nullable(obj({"bucket": string, "key": string, "range": nullable(string)})),
    "req_payload_bytes": integer, "req_wire_bytes": integer,
    "resp_payload_bytes": integer, "resp_wire_bytes": integer,
    "payload_sha256": nullable(string), "expect_negotiated": boolean,
    "downstream": obj({"outcome": enum("delivered", "dropped", "injected", "rejected"),
                       "status": nullable(integer)}),
    "upstream": obj({"outcome": enum("ok", "error", "unknown", "not_sent"),
                     "http_status": nullable(integer), "s3_code": nullable(string)}),
    "response_body": obj({
        "upload_id": nullable(string), "etag": nullable(string), "complete_error": nullable(string),
        "completed_parts": nullable(list_of(obj({"part_number": integer, "etag": string}))),
        "delete_results": nullable(list_of(obj({"key": string, "deleted": boolean, "code": nullable(string)}))),
        "list": nullable(obj({"key_count": integer, "truncated": boolean})),
    }),
    "injected_rule": nullable(string),
    "phase": enum(*PHASES),
    "error": nullable(string),  # optional: proxy-side failure description
    "delete_keys": list_of(string),  # optional: keys a DeleteObjects request names, parsed before forwarding
}
_MARK = {"kind": literal("mark"), "seq": integer, "label": nonempty_string, "t_us": integer}


def validate_ledger_row(d):
    if not isinstance(d, dict):
        return ["$: expected object"]
    kind = d.get("kind")
    if kind == "attempt":
        return _run(obj_optional(_ATTEMPT, {"error", "delete_keys"}), d)
    if kind == "mark":
        return _run(obj(_MARK), d)
    return [f"$.kind: expected attempt or mark, got {_short(kind)}"]


# --- measurement -------------------------------------------------------------------------------

_COUNTS = obj({f: integer for f in COUNTS_FIELDS})
_OP_COUNTS = dict_of(_COUNTS)
_CALLER_OPS = dict_of(_OP_COUNTS)

_COMMON = {
    "schema": literal("measurement/1"), "measurement_id": nonempty_string, "experiment_id": nonempty_string,
    "run_id": nonempty_string, "card": nonempty_string, "case_id": nonempty_string,
    "policy": nonempty_string, "variant": enum(*VARIANTS), "cache_enabled": boolean,
    "replicas": integer, "k": integer, "scale": number, "mode": enum(*MODES),
    "binary_sha256": nonempty_string, "binary_download_sha256": nullable(nonempty_string),
    "commit": nonempty_string, "rig_commit": nonempty_string, "pass_dir": nonempty_string,
    "status": enum("ok", "UNSUPPORTED", "FAILED"),
}

_LOCAL_REPLICA = obj({
    "paths": list_of(string), "peak_allocated_bytes": integer,
    "samples": list_of(pair(integer)), "device": string, "fs": string,
    "attribution": literal("process_io"),
    "trace": nullable(obj({
        "tool": literal("strace"),
        "syscalls": dict_of(obj({"count": integer, "bytes": integer, "size_histogram": dict_of(integer)})),
        "window": pair(number), "outside": integer, "files": list_of(string),
        "crossing": list_of(obj({"pid": string, "syscall": nonempty_string, "path": string, "start": number,
                                 "end": number, "bytes": integer})),
    })),
    "limitations": list_of(string),
})

# Sections that only exist on status=ok (and, individually, inside FAILED.partial).
_OK_SECTIONS = {
    "effective_counts": free_object,
    "effective_settings": obj({"table": free_object, "query": free_object, "server": free_object}),
    "config_hashes": list_of(string),
    "window": obj({"start_seq": integer, "end_seq": integer, "t_start_us": integer, "t_end_us": integer,
                   "crossing": list_of(integer),
                   "open_attempts": list_of(obj({"seq": integer, "caller": enum(*CALLERS), "op": enum(*OPS),
                                                 "key": string}))}),
    "ledger": obj({"foreground": _CALLER_OPS, "maintenance": _CALLER_OPS, "setup": _CALLER_OPS,
                   "teardown": _CALLER_OPS, "rig": _OP_COUNTS, "complete": boolean}),
    "events": dict_of(dict_of(number)),
    "proc_io": dict_of(obj({f: integer for f in PROC_IO_FIELDS})),
    "generations": dict_of(list_of(obj({"pid": integer, "t_start_us": integer, "t_end_us": integer}))),
    "local": dict_of(_LOCAL_REPLICA),
    "interserver": dict_of(obj({"to_target": integer, "from_target": integer,
                                "connections_total": integer, "open_at_end": integer})),
    "inventory": obj_optional({
        "initial": enum("empty_verified", "declared"),
        "note": nonempty_string,  # optional: why the initial inventory is declared rather than verified
        "byte_hours": dict_of(obj({"lower": number, "upper": number})),
        "uncertain_identities": integer,
        # Logical categories attached after creation (`garbage` after a DROP, ...), replayable from the raw slice.
        "reclassified": list_of(obj({"category": enum(*CATEGORIES), "t_us": integer, "evidence": nonempty_string,
                                     "keys": list_of(pair(string))})),
    }, ("note",)),
    "observations": free_object,
    "assertions": dict_of(obj({"expected": anything, "actual": anything, "ok": boolean})),
    "wall_s": number,
    "unit_count": number,
}
_CLEANUP = obj({"aborted_uploads": integer, "deleted_keys": integer, "verified_empty": boolean,
                "errors": list_of(string)})
_FAILED_EXTRA = {
    "reason": nonempty_string,
    "diagnostics": obj({"server_logs": list_of(string), "error_text": string}),
}


def validate_measurement(d):
    if not isinstance(d, dict):
        return ["$: expected object"]
    status = d.get("status")
    spec = dict(_COMMON)
    if status == "ok":
        spec.update(_OK_SECTIONS)
        spec["cleanup"] = _CLEANUP
    elif status == "UNSUPPORTED":
        spec["reason"] = nonempty_string
    elif status == "FAILED":
        spec.update(_FAILED_EXTRA)
        spec["partial"] = lambda v, path: _validate_partial(v, path)
        spec["cleanup"] = _CLEANUP
    else:
        return [f"$.status: expected ok, UNSUPPORTED or FAILED, got {_short(status)}"]
    problems = _run(obj_optional(spec, optional=("partial", "cleanup") if status == "FAILED" else ("cleanup",) if status == "ok" else ()), d)
    problems += _check_identity(d)
    return problems


def _validate_partial(v, path):
    if not isinstance(v, dict):
        return [f"{path}: expected object, got {_short(v)}"]
    out = []
    for key, value in v.items():
        if key not in _OK_SECTIONS:
            out.append(f"{path}: unexpected section {key}")
        else:
            out += _OK_SECTIONS[key](value, f"{path}.{key}")
    return out


def _check_identity(d):
    out = []
    parts = [d.get(f) for f in ("run_id", "policy", "card", "case_id")]
    if all(isinstance(p, str) for p in parts) and _is_int(d.get("k")):
        expected = "/".join(parts) + f"/k{d['k']}"
        if d.get("measurement_id") != expected:
            out.append(f"$.measurement_id: expected {expected}, got {_short(d.get('measurement_id'))}")
    if isinstance(d.get("card"), str) and isinstance(d.get("case_id"), str):
        expected = f"{d['card']}.{d['case_id']}"
        if d.get("experiment_id") != expected:
            out.append(f"$.experiment_id: expected {expected}, got {_short(d.get('experiment_id'))}")
    return out


# --- prediction --------------------------------------------------------------------------------

def _range(v, path):
    """`{min, max}`, optionally with `basis` (`derived` or `assumption`) and the `reason` that basis requires."""
    out = obj_optional({"min": integer, "max": integer, "basis": enum("derived", "assumption"),
                        "reason": nonempty_string}, ("basis", "reason"))(v, path)
    if isinstance(v, dict) and ("basis" in v) != ("reason" in v):
        out.append(f"{path}: basis and reason go together")
    return out


_COUNT_OR_RANGE = either(integer, _range)


def validate_prediction(d):
    spec = {
        "schema": literal("prediction/1"), "experiment_id": nonempty_string, "policy": nonempty_string,
        "inputs": free_object,
        "expected": obj({
            "foreground": dict_of(dict_of(_COUNT_OR_RANGE)),
            "observations": free_object,
        }),
        "tolerance": obj({"abs": free_object, "rel": free_object}),
        "source_refs": list_of(obj({"claim": nonempty_string, "path": nonempty_string, "line": integer})),
        "prediction_commit": nonempty_string,
    }
    problems = _run(obj(spec), d)
    if not problems:
        for caller, ops in d["expected"]["foreground"].items():
            if caller not in CALLERS:
                problems.append(f"$.expected.foreground.{caller}: unknown caller")
            problems += [f"$.expected.foreground.{caller}.{op}: unknown op" for op in ops if op not in OPS]
    return problems


# --- experiment --------------------------------------------------------------------------------

def validate_experiment(d):
    spec = {
        "schema": literal("experiment/1"), "experiment_id": nonempty_string, "card": nonempty_string,
        "case_id": nonempty_string, "policies": list_of(nonempty_string, min_len=1), "replicas": integer,
        "mode": enum(*MODES),
        "controls": obj({"merges": enum("stop", "measure"), "gc": enum("off", "measure"),
                         "ttl": enum("none", "measure")}),
        "settings": obj({"table": free_object, "query": free_object, "server": free_object}),
        "params": free_object, "scale": number, "repeats": integer, "timeout_s": integer,
        "description": string,
    }
    problems = _run(obj(spec), d)
    if isinstance(d, dict) and isinstance(d.get("card"), str) and isinstance(d.get("case_id"), str):
        expected = f"{d['card']}.{d['case_id']}"
        if d.get("experiment_id") != expected:
            problems.append(f"$.experiment_id: expected {expected}, got {_short(d.get('experiment_id'))}")
    return problems


# --- coverage ----------------------------------------------------------------------------------

def validate_coverage(d):
    case = obj({
        "case_id": nonempty_string, "policies": list_of(nonempty_string, min_len=1), "replicas": integer,
        "scale": number, "repeats": integer, "timeout_s": integer, "duration_s": integer,
        "priority": enum("P1", "P2"), "status": enum("planned", "run", "cut"),
    })
    card = obj({"name": nonempty_string, "family": nonempty_string,
                "model_variants": list_of(nonempty_string), "cases": list_of(case)})
    evidence = obj({
        "coefficient": nonempty_string, "experiment_id": nonempty_string, "policy": nonempty_string,
        "span": dict_of(pair(number)), "role": enum("fit", "holdout", "validation"),
        "prediction_commit": nullable(nonempty_string), "measurement_ids": list_of(nonempty_string),
        "status": enum("planned", "done", "consumed", "cut"),
    })
    spec = {
        "schema": literal("coverage/1"), "version": nonempty_string,
        "cards": list_of(card), "evidence": list_of(evidence),
        "domain": obj({"variants": list_of(string), "operations": list_of(string), "families": list_of(string)}),
        "excluded": list_of(string),
    }
    problems = _run(obj(spec), d)
    if not problems:
        for i, c in enumerate(d["cards"]):
            for j, k in enumerate(c["cases"]):
                # The estimate floors each pass at duration_s; a pass longer than its timeout cannot finish.
                if not 0 <= k["duration_s"] <= k["timeout_s"]:
                    problems.append(f"$.cards[{i}].cases[{j}].duration_s: must be within 0..timeout_s "
                                    f"({k['timeout_s']}), got {k['duration_s']}")
        for i, row in enumerate(d["evidence"]):
            # A held-out row is only honest if the prediction predates the measurement.
            if row["role"] == "holdout" and row["prediction_commit"] is None:
                problems.append(f"$.evidence[{i}].prediction_commit: required for role holdout")
    return problems


# --- formulas grammar v1 -----------------------------------------------------------------------

NODE_KINDS = ("input", "coef", "lookup", "var", "const", "add", "sub", "mul", "div", "min", "max",
              "ceil_to", "floor_to", "branch", "sum_over", "price")
COND_OPS = ("<", "<=", ">", ">=", "==", "!=")
_BINARY = {"add": ("a", "b"), "sub": ("a", "b"), "mul": ("a", "b"), "ceil_to": ("x", "step"),
           "floor_to": ("x", "step")}


def validate_node(node, path, in_sum_over, tables=None, bound=()):
    """Validate one expression node. `tables` maps table id -> index arity; None skips lookup table checks.

    `bound` holds the variable names of the enclosing sum_over nodes.
    """
    if not isinstance(node, dict):
        return [f"{path}: expected node object, got {_short(node)}"]
    kind = node.get("kind")
    if kind not in NODE_KINDS:
        return [f"{path}.kind: expected one of {list(NODE_KINDS)}, got {_short(kind)}"]
    out = []
    if not isinstance(node.get("dim"), str) or not node.get("dim"):
        out.append(f"{path}: missing field dim")

    def child(key, **kw):
        if key not in node:
            out.append(f"{path}: missing field {key}")
            return
        out.extend(validate_node(node[key], f"{path}.{key}", kw.get("in_sum_over", in_sum_over),
                                 tables, kw.get("bound", bound)))

    def need(key, checker):
        if key not in node:
            out.append(f"{path}: missing field {key}")
        else:
            out.extend(checker(node[key], f"{path}.{key}"))

    allowed = {"kind", "dim"}
    if kind in ("input", "coef"):
        need("id", nonempty_string)
        allowed |= {"id"}
    elif kind == "const":
        need("value", lambda v, p: [] if _is_num(v) or isinstance(v, str) else [f"{p}: expected number, string or 'unavailable', got {_short(v)}"])
        allowed |= {"value"}
    elif kind == "var":
        need("name", nonempty_string)
        allowed |= {"name"}
        name = node.get("name")
        if not in_sum_over:
            out.append(f"{path}: var {_short(name)} is legal only inside sum_over")
        elif isinstance(name, str) and name not in bound:
            out.append(f"{path}: var {name} is not bound by an enclosing sum_over (bound: {list(bound)})")
    elif kind == "lookup":
        need("table_id", nonempty_string)
        need("keys", list_of(anything, min_len=1))
        allowed |= {"table_id", "keys"}
        keys = node.get("keys")
        if isinstance(keys, list):
            for i, k in enumerate(keys):
                out.extend(validate_node(k, f"{path}.keys[{i}]", in_sum_over, tables, bound))
        table_id = node.get("table_id")
        if tables is not None and isinstance(table_id, str):
            if table_id not in tables:
                out.append(f"{path}.table_id: unknown table {table_id}")
            elif isinstance(keys, list) and len(keys) != tables[table_id]:
                out.append(f"{path}.keys: arity {len(keys)} does not match table {table_id} index arity {tables[table_id]}")
    elif kind in _BINARY:
        for key in _BINARY[kind]:
            child(key)
        allowed |= set(_BINARY[kind])
    elif kind == "div":
        child("a")
        child("b")
        need("on_zero", enum("zero", "unavailable"))
        allowed |= {"a", "b", "on_zero"}
    elif kind in ("min", "max"):
        need("args", list_of(anything, min_len=1))
        allowed |= {"args"}
        if isinstance(node.get("args"), list):
            for i, a in enumerate(node["args"]):
                out.extend(validate_node(a, f"{path}.args[{i}]", in_sum_over, tables, bound))
    elif kind == "branch":
        allowed |= {"cond", "then", "else"}
        cond = node.get("cond")
        if not isinstance(cond, dict):
            out.append(f"{path}: missing field cond")
        else:
            if cond.get("op") not in COND_OPS:
                out.append(f"{path}.cond.op: expected one of {list(COND_OPS)}, got {_short(cond.get('op'))}")
            for side in ("a", "b"):
                if side not in cond:
                    out.append(f"{path}.cond: missing field {side}")
                else:
                    out.extend(validate_node(cond[side], f"{path}.cond.{side}", in_sum_over, tables, bound))
            out.extend(f"{path}.cond: unexpected field {k}" for k in cond if k not in ("op", "a", "b"))
        child("then")
        child("else")
    elif kind == "sum_over":
        need("set", nonempty_string)
        need("var", nonempty_string)
        allowed |= {"set", "var", "body"}
        if isinstance(node.get("var"), str):
            child("body", in_sum_over=True, bound=tuple(bound) + (node["var"],))
        else:
            child("body")
    elif kind == "price":
        need("unit", nonempty_string)
        need("rule_id", nonempty_string)
        child("quantity")
        allowed |= {"unit", "rule_id", "quantity"}
    out.extend(f"{path}: unexpected field {k}" for k in node if k not in allowed)
    return out


def table_arities_from_registry(registry):
    return {q["id"]: len(q["table"]["index"]) for q in registry.get("quantities", [])
            if q.get("value_type") == "table" and isinstance(q.get("table"), dict)}


def table_arities_from_coefficients(coefficients):
    return {c["id"]: len(c["value"]["index"]) for c in coefficients
            if c.get("kind") == "table" and isinstance(c.get("value"), dict)}


def validate_formulas(d, tables=None):
    problems = _run(obj({
        "schema": literal("formulas/1"), "owner": nonempty_string, "variant": enum(*VARIANTS),
        "cells": free_object,
    }), d)
    if problems:
        return problems
    for op, cell in d["cells"].items():
        problems += _validate_cell(cell, f"$.cells.{op}", tables)
    return problems


_ROW = {
    "row_type": enum("request", "storage", "transfer", "block_io", "block_capacity"),
    "op_kind": nonempty_string,
    "outcome_class": enum("ok", "accepted_dropped", "client_error", "server_error", "dropped", "not_sent", "all"),
    "category": nullable(enum(*CATEGORIES)),
    "route": nullable(enum("ec2_s3_same_region", "ec2_ec2_cross_az", "internet")),
    "scope": enum("variant", "replica", "volume"),
    "expr": anything,
    # Optional outcome facts the billing preset's free list matches on, and the caller a request row reconciles with.
    "status": either(integer, _typed("status class like 4XX", lambda v: isinstance(v, str) and len(v) == 3
                                     and v[0].isdigit() and v[1:] == "XX")),
    "s3_code": nullable(nonempty_string),
    "condition": nullable(nonempty_string),
    "caller": nullable(enum(*CALLERS)),
}
_ROW_OPTIONAL = ("status", "s3_code", "condition", "caller")


def _validate_cell(cell, path, tables):
    if not isinstance(cell, dict):
        return [f"{path}: expected object"]
    out = []
    for key in ("ledger_rows", "allocation", "states", "notes"):
        if key not in cell:
            out.append(f"{path}: missing field {key}")
    out += [f"{path}: unexpected field {k}" for k in cell if k not in ("ledger_rows", "allocation", "states", "notes", "excluded")]
    if "excluded" in cell:
        # Record usage the cell deliberately does not price; the drill flags any other unmodelled usage.
        out += _run(list_of(obj({"kind": enum("op", "category"), "id": nonempty_string, "reason": nonempty_string})),
                    cell["excluded"], f"{path}.excluded")
    for i, row in enumerate(cell.get("ledger_rows", []) if isinstance(cell.get("ledger_rows"), list) else []):
        rp = f"{path}.ledger_rows[{i}]"
        out += _run(obj_optional(_ROW, _ROW_OPTIONAL), row, rp)
        if isinstance(row, dict) and "expr" in row:
            out += validate_node(row["expr"], f"{rp}.expr", False, tables)
    if "ledger_rows" in cell and not isinstance(cell["ledger_rows"], list):
        out.append(f"{path}.ledger_rows: expected array")
    alloc = cell.get("allocation")
    if "allocation" in cell and alloc is not None:
        if not isinstance(alloc, dict) or not isinstance(alloc.get("shares"), list):
            out.append(f"{path}.allocation: expected null or {{shares: [...]}}")
        else:
            for i, share in enumerate(alloc["shares"]):
                sp = f"{path}.allocation.shares[{i}]"
                out += _run(obj({"cell": nonempty_string, "weight": anything}), share, sp)
                if isinstance(share, dict) and "weight" in share:
                    out += validate_node(share["weight"], f"{sp}.weight", False, tables)
    states = cell.get("states")
    if "states" in cell:
        if not isinstance(states, list):
            out.append(f"{path}.states: expected array")
        else:
            for i, st in enumerate(states):
                sp = f"{path}.states[{i}]"
                out += _run(obj({"id": enum("not_supported", "backlog_grows"), "cond": anything,
                                 "reason": nonempty_string}), st, sp)
                if isinstance(st, dict) and "cond" in st and st["cond"] is not True:
                    out += validate_node(st["cond"], f"{sp}.cond", False, tables)
    notes = cell.get("notes")
    if "notes" in cell:
        if not isinstance(notes, list):
            out.append(f"{path}.notes: expected array")
        else:
            for i, n in enumerate(notes):
                out += validate_node(n, f"{path}.notes[{i}]", False, tables)
    return out


# --- registry ----------------------------------------------------------------------------------

def validate_registry(d):
    quantity = obj({
        "id": nonempty_string, "block": nonempty_string, "label": nonempty_string,
        "kind": enum("input", "pinned", "measured", "derivation"),
        "scope": enum("cluster", "replica", "pool", "volume"),
        "unit": string, "value_type": enum("number", "enum", "table"),
        "enum": nullable(list_of(nonempty_string, min_len=1)),
        "table": nullable(obj({"index": list_of(nonempty_string, min_len=1), "dim": nonempty_string})),
        "default": nullable(either(number, string)),
        "rule": anything,
        "coef": nullable(nonempty_string),
        "validity": nullable(dict_of(pair(number))),
        "evidence": obj({"kind": enum("assumed", "source", "measured", "provider"),
                         "ref": nonempty_string, "version": nonempty_string}),
        "hint": nullable(obj({"sql": nonempty_string, "mapping": dict_of(string)})),
        "status": enum("ok", "unsupported"),
    })
    spec = {
        "schema": literal("registry/1"), "version": nonempty_string,
        "quantities": list_of(quantity),
        "sets": dict_of(list_of(nonempty_string)),
        "precedence": list_of(obj({"group": list_of(nonempty_string), "default_locked": list_of(nonempty_string),
                                   "rules": dict_of(anything)})),
    }
    problems = _run(obj(spec), d)
    if problems:
        return problems
    ids = {}
    for i, q in enumerate(d["quantities"]):
        qp = f"$.quantities[{i}]"
        if q["id"] in ids:
            problems.append(f"{qp}.id: duplicate id {q['id']}")
        ids[q["id"]] = q
        if q["value_type"] == "enum" and q["enum"] is None:
            problems.append(f"{qp}.enum: required when value_type is enum")
        if q["value_type"] == "table" and q["table"] is None:
            problems.append(f"{qp}.table: required when value_type is table")
        if q["kind"] == "derivation":
            if q["rule"] is None:
                problems.append(f"{qp}.rule: required for a derivation")
            else:
                problems += validate_node(q["rule"], f"{qp}.rule", False, table_arities_from_registry(d))
        elif q["rule"] is not None:
            problems.append(f"{qp}.rule: only a derivation has a rule")
    for set_id, members in d["sets"].items():
        problems += [f"$.sets.{set_id}: unknown member {m}" for m in members if m not in ids]
    for i, q in enumerate(d["quantities"]):
        if q["table"] is not None:
            problems += [f"$.quantities[{i}].table.index: unknown id {m}" for m in q["table"]["index"] if m not in ids]
    seen = {}
    tables = table_arities_from_registry(d)
    for i, pr in enumerate(d["precedence"]):
        pp = f"$.precedence[{i}]"
        in_group = set()
        for m in pr["group"]:
            if m not in ids:
                problems.append(f"{pp}.group: unknown id {m}")
            if m in in_group:
                problems.append(f"{pp}.group: duplicate member {m}")
            elif m in seen:
                problems.append(f"{pp}.group: shares member {m} with precedence[{seen[m]}]")
            in_group.add(m)
            seen.setdefault(m, i)
        problems += [f"{pp}.default_locked: {m} is not in group" for m in pr["default_locked"] if m not in pr["group"]]
        for derived, rule in pr["rules"].items():
            if derived not in pr["group"]:
                problems.append(f"{pp}.rules.{derived}: not in group")
            problems += validate_node(rule, f"{pp}.rules.{derived}", False, tables)
    return problems
