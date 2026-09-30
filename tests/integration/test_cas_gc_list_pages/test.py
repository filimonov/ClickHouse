"""
Measures how many S3 LIST requests the GC's global ref walk spends per logical listing page
(`DiskS3ListObjects` against `CASRefGlobalListPages`) on RustFS and attributes them to the
`limit + 1` first page, the iterator's prefetch, resumed pages and other callers. It records and
prints; it asserts no ratio.
"""
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

STORAGE_POLICY = "cas_gc_list_pages"
BUCKET_PREFIX = "cas_gc_list_pages/"
TARGET_KEYS = 20000
MIN_KEYS = 5000
SEED_WALL_SECONDS = 600
INSERT_THREADS = 4
PARTITIONS = 50
REPORT_NAME = "list_pages_attribution.md"


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    cluster.add_instance(
        "node",
        main_configs=["configs/storage_conf.xml"],
        with_rustfs=True,
        stay_alive=True,
    )
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def count_log_keys():
    return sum(
        1
        for o in cluster.rustfs_client.list_objects(
            cluster.rustfs_bucket, BUCKET_PREFIX, recursive=True
        )
        if "/_log/" in o.object_name
    )


def insert_once(node, table):
    node.query(
        f"INSERT INTO {table} SELECT number, toString(number) FROM numbers({PARTITIONS * 2})"
    )


def seed(node):
    node.query("DROP TABLE IF EXISTS t SYNC")
    node.query(
        f"CREATE TABLE t (k UInt64, v String) ENGINE = MergeTree ORDER BY k "
        f"PARTITION BY k % {PARTITIONS} SETTINGS storage_policy = '{STORAGE_POLICY}'"
    )
    node.query("SYSTEM STOP MERGES t")
    start_keys = count_log_keys()
    insert_once(node, "t")
    per_insert = max(1, count_log_keys() - start_keys)
    logging.info("one insert (%d parts) added %d _log keys", PARTITIONS, per_insert)

    deadline = time.time() + SEED_WALL_SECONDS
    keys = count_log_keys()
    with ThreadPoolExecutor(INSERT_THREADS) as pool:
        while keys < TARGET_KEYS and time.time() < deadline:
            remaining = max(INSERT_THREADS, (TARGET_KEYS - keys) // per_insert)
            batch = min(remaining, 40)
            list(pool.map(lambda _: insert_once(node, "t"), range(batch)))
            keys = count_log_keys()
    return per_insert, keys


def events(node):
    out = node.query(
        "SELECT event, value FROM system.events "
        "WHERE event IN ('DiskS3ListObjects', 'CASRefGlobalListPages', 'CASRequestAttempt')"
    )
    return {k: int(v) for k, v in (r.split("\t") for r in out.strip().splitlines())}


def test_list_requests_per_logical_page():
    node = cluster.instances["node"]
    per_insert, keys = seed(node)
    assert keys >= MIN_KEYS, f"only {keys} _log keys seeded"

    log_lines_before = int(node.count_in_log("listUnder prefix=").strip())
    ev_before = events(node)
    node.query("SYSTEM FLUSH LOGS")
    ts = node.query("SELECT now64(6)").strip()

    node.query("SYSTEM CAS GC RUN")
    node.query("SYSTEM FLUSH LOGS")
    ev_after = events(node)
    delta = {k: ev_after.get(k, 0) - ev_before.get(k, 0) for k in ev_after}

    rows = node.query(
        "SELECT event_type, phase, outcome, phase_metrics['ref_keys_listed'], "
        "ProfileEvents['S3ListObjects'], ProfileEvents['CASRequestAttempt'], "
        "ProfileEvents['CASRefGlobalListPages'] "
        f"FROM system.cas_gc_log WHERE event_time_microseconds >= toDateTime64('{ts}', 6) "
        "ORDER BY event_time_microseconds FORMAT TSV"
    ).strip().splitlines()
    assert any(r.split("\t")[0] == "Finish" for r in rows), "the GC round did not finish"
    assert delta["DiskS3ListObjects"] > 0
    assert delta["CASRefGlobalListPages"] > 0

    trace = node.grep_in_log("listUnder prefix=").splitlines()[log_lines_before:]
    pat = re.compile(
        r"listUnder prefix=(\S+), cursor_set=(true|false), limit=(\d+), keys=(\d+), has_next=(true|false), s3_list_requests=(\d+)"
    )
    calls = [m.groups() for m in map(pat.search, trace) if m]

    def cls(c):
        return ("resumed" if c[1] == "true" else "first") + (" ref-stream" if c[0].endswith("ns/stream") else " other")

    table = {}
    for c in calls:
        row = table.setdefault(cls(c), [0, 0, 0])
        row[0] += 1
        row[1] += int(c[3])
        row[2] += int(c[5])

    lines = [
        "# LIST requests per logical page (GC global ref walk)",
        "",
        f"_log keys in bucket: {keys}; keys per insert ({PARTITIONS} parts): {per_insert}",
        f"DiskS3ListObjects delta: {delta['DiskS3ListObjects']}; CASRefGlobalListPages delta: {delta['CASRefGlobalListPages']}; "
        f"CASRequestAttempt delta: {delta['CASRequestAttempt']}",
        f"ratio: {delta['DiskS3ListObjects'] / delta['CASRefGlobalListPages']:.2f}",
        "",
        "| call class | calls | keys returned | DiskS3ListObjects |",
        "|---|---|---|---|",
    ]
    for name, (n, k, r) in sorted(table.items()):
        lines.append(f"| {name} | {n} | {k} | {r} |")
    lines += [
        f"| total | {sum(v[0] for v in table.values())} | {sum(v[1] for v in table.values())} | {sum(v[2] for v in table.values())} |",
        "",
        "Round rows (event_type, phase, outcome, ref_keys_listed, S3ListObjects, CASRequestAttempt, CASRefGlobalListPages):",
        "",
    ]
    lines += ["    " + r for r in rows]
    lines += ["", "Trace lines of the round (listUnder):", ""]
    lines += ["    " + t[t.find("listUnder"):] for t in trace if "listUnder" in t][:400]
    report = "\n".join(lines)
    print(report)
    path = os.path.join(cluster.instances_dir, REPORT_NAME)
    with open(path, "w") as f:
        f.write(report)
    print("attribution written to", path)
