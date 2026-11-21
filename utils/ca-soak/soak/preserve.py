"""Evidence saved into the run directory before the stand is torn down, on every run end.

  ref_catalog.tsv, ref_catalog.raw   every row of the pool's namespace catalog (`cas/ref_catalog`), read
                                     straight from the store through a server's `s3` table function
  srv<N>/system_<table>.tsv          `system.cas_log` and `system.cas_gc_log` of server N, with column names;
                                     the newest `SYSTEM_LOG_ROWS` rows of the last `SYSTEM_LOG_HOURS` hours
"""

import json
import os
import urllib.parse
import urllib.request

SYSTEM_LOG_HOURS = 2
SYSTEM_LOG_ROWS = 2_000_000
SYSTEM_LOG_TABLES = ("cas_log", "cas_gc_log")
CATALOG_URL = "http://rustfs1:11121/test/soak_pool/cas/ref_catalog"
S3_USER = S3_PASSWORD = "clickhouse"


def catalog_rows(raw: str) -> list:
    """Parse the catalog object's text: a header line, then one JSON object per line; returns the `entry` rows."""
    rows = []
    for line in raw.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        d = json.loads(line)
        if d.get("kind") == "entry":
            rows.append(d)
    return rows


def catalog_tsv(rows: list) -> str:
    out = ["namespace\tstate\tlife\tother"]
    for d in rows:
        other = {k: v for k, v in d.items() if k not in ("kind", "ns", "state", "life")}
        out.append(f"{d.get('ns')}\t{d.get('state')}\t{d.get('life')}\t{json.dumps(other, sort_keys=True)}")
    return "\n".join(out) + "\n"


def dump_catalog(node, run_dir: str, url: str = CATALOG_URL) -> int:
    """Writes `ref_catalog.raw` and `ref_catalog.tsv`; returns the number of rows."""
    raw = node.query(f"SELECT * FROM s3('{url}', '{S3_USER}', '{S3_PASSWORD}', 'RawBLOB') FORMAT TSVRaw", timeout=300)
    with open(os.path.join(run_dir, "ref_catalog.raw"), "w") as f:
        f.write(raw + "\n")
    rows = catalog_rows(raw)
    if not rows and '"kind":"entry"' in raw:
        raise RuntimeError("ref catalog has entry rows the reader does not parse")
    with open(os.path.join(run_dir, "ref_catalog.tsv"), "w") as f:
        f.write(catalog_tsv(rows))
    return len(rows)


def system_log_query(table: str) -> str:
    return (f"SELECT * FROM (SELECT * FROM system.{table} WHERE event_time >= now() - INTERVAL {SYSTEM_LOG_HOURS} HOUR "
            f"ORDER BY event_time_microseconds DESC LIMIT {SYSTEM_LOG_ROWS}) ORDER BY event_time_microseconds "
            f"FORMAT TSVWithNames")


def dump_system_log(node, table: str, dst: str, timeout: float = 1800) -> int:
    """Streams one table to `dst`; returns the number of data rows."""
    params = urllib.parse.urlencode({"database": node.database, "max_execution_time": int(timeout)})
    req = urllib.request.Request(node.url + "?" + params, data=system_log_query(table).encode(), method="POST")
    lines = 0
    with urllib.request.urlopen(req, timeout=timeout) as resp, open(dst, "wb") as f:
        while True:
            chunk = resp.read(1 << 20)
            if not chunk:
                break
            lines += chunk.count(b"\n")
            f.write(chunk)
    return max(lines - 1, 0)


def preserve(nodes: dict, run_dir: str, log_fn, fail_fn) -> None:
    """Dump the catalog (from the first server that answers) and both system logs of every server.
    Every failure is reported through `fail_fn`, never swallowed: a run without its evidence cannot be analysed."""
    for n, node in sorted(nodes.items()):
        try:
            node.query("SYSTEM FLUSH LOGS", timeout=300)
        except Exception as e:   # noqa: BLE001
            log_fn(f"evidence: SYSTEM FLUSH LOGS on server {n} failed: {type(e).__name__}: {e}")
    caught = False
    for n, node in sorted(nodes.items()):
        try:
            rows = dump_catalog(node, run_dir)
            log_fn(f"evidence: ref catalog (read through server {n}): {rows} row(s) in ref_catalog.tsv")
            caught = True
            break
        except Exception as e:   # noqa: BLE001
            log_fn(f"evidence: ref catalog through server {n} failed: {type(e).__name__}: {str(e)[:300]}")
    if not caught:
        fail_fn("evidence: the ref catalog could not be dumped")
    for n, node in sorted(nodes.items()):
        os.makedirs(os.path.join(run_dir, f"srv{n}"), exist_ok=True)
        for table in SYSTEM_LOG_TABLES:
            dst = os.path.join(run_dir, f"srv{n}", f"system_{table}.tsv")
            try:
                rows = dump_system_log(node, table, dst)
                log_fn(f"evidence: server {n} system.{table}: {rows} row(s) -> {os.path.relpath(dst, run_dir)}")
            except Exception as e:   # noqa: BLE001
                log_fn(f"evidence: server {n} system.{table} failed: {type(e).__name__}: {str(e)[:300]}")
                fail_fn(f"evidence: server {n} system.{table} could not be dumped")
