"""S48: copies between tables of re-inserted content, under a running GC.

A statement that adds a reference in another table without reading the data (`MOVE PARTITION TO TABLE`,
`ATTACH PARTITION FROM`, `REPLACE PARTITION FROM`, `CREATE TABLE ... CLONE AS`) followed by the drop of its
source must never cost a body that the new table names, while GC rounds run and the content was dropped and
re-inserted shortly before.

Each iteration: insert deterministic content into a scratch table and drop it, run a few GC rounds so the
blobs carry GC rows, re-insert the same content into a source table, run one statement while a background
thread keeps issuing rounds, drop the source, read the destination back. Tables are plain `MergeTree` on the
first node: `CLONE AS` of a replicated table would reuse the source's Keeper path.

The statement may be refused with `NO_REPLICA_HAS_PART` when its source cannot be confirmed; that is retried a
bounded number of times and counted. A statement that succeeded must leave every row readable.

Verdicts: every destination reads back the checksum recorded from its source, at the statement and after the
pool is drained; the end checkpoint's fsck has no dangling reference or other hard finding; the counts of each
statement executed and refused are reported. A short dev run may meet no refusal and no unlucky interleaving: the
oracle is what the run proves, not that a window was hit.
"""

import threading

from soak.cluster import QueryError

from ..framework import gc as gc_mod, sql
from ..framework.base import Scenario, register
from ..framework.report import Verdict
from . import _common

_PREFIX = "s48"
_COLUMNS = "id UInt64, payload String"
_STATEMENTS = ("move", "attach", "replace", "clone")
_RETRIES = 5
_NO_REPLICA_HAS_PART = "NO_REPLICA_HAS_PART"


def _content(seed, rows, payload_bytes):
    return (f"SELECT number, concat(toString(cityHash64(number, {seed})), repeat('x', {payload_bytes})) "
            f"FROM numbers({rows})")


def _checksum(node, table):
    out = node.query(sql.table_checksum_query(table)).strip()
    count, total = out.split("\t")
    return int(count), int(total)


def _create(node, name):
    sql.create_ca_table(node, name, columns=_COLUMNS, order_by="id", engine="MergeTree")


class _GcLoop(threading.Thread):
    """Rounds on one node (a single explicit leader, as the harness requires) until stopped."""

    def __init__(self, node, period_s):
        super().__init__(daemon=True)
        self.node = node
        self.period_s = period_s
        self.stopping = threading.Event()
        self.rounds = 0
        self.errors = []

    def run(self):
        while not self.stopping.is_set():
            try:
                gc_mod.gc_round(self.node)
                self.rounds += 1
            except QueryError as e:
                if not e.is_aborted:
                    self.errors.append(str(e)[:200])
            except Exception as e:
                self.errors.append(str(e)[:200])
            self.stopping.wait(self.period_s)

    def finish(self):
        self.stopping.set()
        self.join(900)


@register
class S48CopiesUnderGc(Scenario):
    name = "S48"
    title = "copies between tables of re-inserted content under GC"
    priority = "P0"

    param_table = {
        "dev": {"iterations": 16, "rows": 2000, "payload_bytes": 2048, "gc_period_s": 0.2},
        "ci": {"iterations": 60, "rows": 20000, "payload_bytes": 2048, "gc_period_s": 0.1},
        "full": {"iterations": 240, "rows": 50000, "payload_bytes": 4096, "gc_period_s": 0.05},
    }

    def run(self, ctx, result):
        p = self.resolve_params(ctx.scale, getattr(ctx, "param_overrides", None) or {})
        node = ctx.cluster.node1
        rows, payload_bytes = int(p["rows"]), int(p["payload_bytes"])
        done = {s: 0 for s in _STATEMENTS}
        refused = {s: 0 for s in _STATEMENTS}
        gave_up = {s: 0 for s in _STATEMENTS}
        survivors = []   # (table, recorded checksum)
        mismatches = []

        loop = _GcLoop(node, float(p["gc_period_s"]))
        loop.start()
        try:
            for i in range(int(p["iterations"])):
                kind = _STATEMENTS[i % len(_STATEMENTS)]
                seed = 48_000 + i
                content = _content(seed, rows, payload_bytes)
                scratch, src, dst = f"{_PREFIX}_w{i}", f"{_PREFIX}_s{i}", f"{_PREFIX}_d{i}"

                _create(node, scratch)
                sql.insert_values(node, scratch, content)
                node.command(f"DROP TABLE {scratch} SYNC", timeout=600)
                for _ in range(i % 4):
                    gc_mod.gc_round(node)

                _create(node, src)
                sql.insert_values(node, src, content)
                expected = _checksum(node, src)
                if kind != "clone":
                    _create(node, dst)
                if kind == "replace":
                    sql.insert_values(node, dst, _content(seed + 100_000, rows, payload_bytes))

                statement = {
                    "move": f"ALTER TABLE {src} MOVE PARTITION ID 'all' TO TABLE {dst}",
                    "attach": f"ALTER TABLE {dst} ATTACH PARTITION ID 'all' FROM {src}",
                    "replace": f"ALTER TABLE {dst} REPLACE PARTITION ID 'all' FROM {src}",
                    "clone": f"CREATE TABLE {dst} CLONE AS {src}",
                }[kind]

                succeeded = False
                for _ in range(_RETRIES + 1):
                    try:
                        node.command(statement, timeout=600)
                        succeeded = True
                        break
                    except QueryError as e:
                        if _NO_REPLICA_HAS_PART not in str(e):
                            raise
                        refused[kind] += 1
                        if kind == "clone":
                            node.command(f"DROP TABLE IF EXISTS {dst} SYNC", timeout=600)
                if not succeeded:
                    gave_up[kind] += 1
                    if _checksum(node, src) != expected:
                        mismatches.append((src, "source after a refused statement", expected))
                    node.command(f"DROP TABLE IF EXISTS {src} SYNC", timeout=600)
                    node.command(f"DROP TABLE IF EXISTS {dst} SYNC", timeout=600)
                    continue

                done[kind] += 1
                node.command(f"DROP TABLE {src} SYNC", timeout=600)
                if _checksum(node, dst) != expected:
                    mismatches.append((dst, "right after the statement", expected))
                survivors.append((dst, expected))
        finally:
            loop.finish()

        # The end checkpoint drains GC and runs fsck; the survivors are read again after it.
        _common.standard_end(ctx, result, [], table_filter=f"table LIKE '{_PREFIX}_%'", optimize=False)
        for table, expected in survivors:
            if _checksum(node, table) != expected:
                mismatches.append((table, "after the pool drained", expected))

        result.add(Verdict.check(
            "every destination reads back the checksum of its source",
            "0 mismatches", f"{len(mismatches)} mismatches over {len(survivors)} destinations",
            not mismatches, f"first: {mismatches[:3]}" if mismatches else ""))
        total_done = sum(done.values())
        result.add(Verdict.check(
            "statements were executed", "> 0", str(total_done), total_done > 0,
            "a run in which every statement was refused checked nothing"))
        result.add(Verdict.check(
            "background GC rounds raised no error", "0", str(len(loop.errors)), not loop.errors,
            f"first: {loop.errors[:2]}" if loop.errors else ""))
        result.add(Verdict.reported(
            "statements executed / refused and retried / given up", "recorded",
            f"done={done} refused={refused} gave_up={gave_up} background_rounds={loop.rounds}",
            "refusals are NO_REPLICA_HAS_PART, retried up to a bound"))
