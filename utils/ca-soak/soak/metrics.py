import json
import sqlite3
import threading
import time

from soak import harness_failures
from soak.signals import CAS_SIGNAL_EVENTS

# The CAS signal counters ride the metrics curve as one column each, named EXACTLY after the
# ProfileEvent so a query against the soak db and a query against `system.events` spell the same
# thing. `None` (not 0) when the node was unreadable at that tick, so a probe gap stays visible.
_SIGNAL_COLS = list(CAS_SIGNAL_EVENTS)

_COLS = ["ts", "node", "parts_active", "parts_inactive", "table_rows", "bytes_on_disk",
         "pool_objects", "pool_bytes", "repl_queue", "mutations_pending", "merges",
         "fsck_reachable", "fsck_unreachable", "fsck_dangling", "restarts",
         # B165: per-node server memory, to catch a server OOM (mem_resident = process RSS bytes,
         # mem_tracking = ClickHouse's own MemoryTracking) before the kernel OOM-kills the node.
         "mem_resident", "mem_tracking",
         # fsck `stale_edge` (detail-mode only): blobs whose every source edge names a manifest that
         # no longer exists, so the incremental GC can never reclaim them. Asserted zero at every
         # checkpoint (run.py); recorded here so the curve shows WHEN a nonzero first appeared.
         "fsck_stale_edge"] + _SIGNAL_COLS

# Per-checkpoint summary of the per-phase GC log rows (`event_type = 'Phase'`, one row per phase per
# round attempt, landed 2026-07-26). One row here per (checkpoint, node, phase). `metrics_json` /
# `events_json` carry that phase's whole summed `phase_metrics` / `ProfileEvents` maps: the detector
# values have their own columns because they are asserted-on, and the maps are kept whole because the
# load study that comes next does not know yet which key it will need.
_PHASE_COLS = ["ts", "checkpoint", "node", "phase", "rounds", "calls", "total_us", "max_us",
               "logs_accounted", "logs_applied", "transactions_unapplied",
               "ref_folding_aborted", "metrics_json", "events_json"]

_PHASE_TEXT_COLS = ("checkpoint", "node", "phase", "metrics_json", "events_json")


def _ensure_columns(conn: sqlite3.Connection, table: str, cols, text_cols) -> None:
    """Add any column of `cols` the existing `table` does not have yet.

    A soak db is often reused across runs of DIFFERENT harness revisions; without this, the first
    INSERT after a schema addition fails and the whole metrics curve is lost for that run. Deliberately
    additive only — nothing is dropped or rewritten, and a failure to ALTER propagates rather than
    being swallowed."""
    have = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
    for c in cols:
        if c not in have:
            kind = "TEXT" if c in text_cols else "INTEGER"
            conn.execute(f'ALTER TABLE {table} ADD COLUMN "{c}" {kind}')


# One process-wide lock serializes every write on the shared connection (statements + commit): two
# threads interleaving execute/commit on one connection made the second commit fail with "cannot
# commit - no transaction is active". Writers that open their own connection (`write_retrying`) use
# a busy timeout instead and do not need it.
WRITE_LOCK = threading.RLock()
BUSY_TIMEOUT_S = 60
WRITE_ATTEMPTS = 5
WRITE_BACKOFF_S = 1.0



def write_failures() -> list:
    """Writes that stayed failing after every retry (the run verdict turns each into a harness failure)."""
    return [f for f in harness_failures.all_failures() if f.startswith("metrics db write")]


def reset_write_failures() -> None:
    harness_failures.reset()


def write_retrying(what: str, fn, *, attempts: int = WRITE_ATTEMPTS, backoff_s: float = WRITE_BACKOFF_S) -> bool:
    """Run the sqlite write `fn` under `WRITE_LOCK`, retrying a bounded number of times on
    `sqlite3.Error`. When it stays failing the failure is recorded (see `write_failures`) and False is
    returned: the workload and the checks go on, the verdict fails."""
    last = None
    for i in range(attempts):
        try:
            with WRITE_LOCK:
                fn()
            return True
        except sqlite3.Error as e:
            last = e
            print(f"metrics db write '{what}' failed (attempt {i + 1}/{attempts}): {type(e).__name__}: {e}", flush=True)
            if i + 1 < attempts:
                time.sleep(backoff_s * (i + 1))
    harness_failures.add(f"metrics db write '{what}' failed after {attempts} attempts: {type(last).__name__}: {last}")
    return False


def connect_own(path: str) -> sqlite3.Connection:
    """A short-lived connection for one writer; waits up to `BUSY_TIMEOUT_S` for the db lock."""
    return sqlite3.connect(path, timeout=BUSY_TIMEOUT_S)


def open_db(path: str) -> sqlite3.Connection:
    # check_same_thread=False: the metrics ticker, the checkpoint tick and the gc_phases writer use one
    # connection from different threads; `record` / `record_phases` serialize on WRITE_LOCK.
    conn = sqlite3.connect(path, check_same_thread=False, timeout=BUSY_TIMEOUT_S)
    conn.row_factory = sqlite3.Row
    cols_ddl = ", ".join(f'"{c}" INTEGER' if c != "node" else '"node" TEXT' for c in _COLS)
    conn.execute(f"CREATE TABLE IF NOT EXISTS metrics ({cols_ddl})")
    phase_ddl = ", ".join(
        f'"{c}" TEXT' if c in _PHASE_TEXT_COLS else f'"{c}" INTEGER' for c in _PHASE_COLS)
    conn.execute(f"CREATE TABLE IF NOT EXISTS gc_phases ({phase_ddl})")
    _ensure_columns(conn, "metrics", _COLS, ("node",))
    _ensure_columns(conn, "gc_phases", _PHASE_COLS, _PHASE_TEXT_COLS)
    conn.commit()
    return conn


def record(conn: sqlite3.Connection, snap: dict) -> bool:
    vals = [snap.get(c) for c in _COLS]
    placeholders = ", ".join("?" for _ in _COLS)
    names = ", ".join(f'"{c}"' for c in _COLS)

    def write():
        try:
            conn.execute(f"INSERT INTO metrics ({names}) VALUES ({placeholders})", vals)
            conn.commit()
        except sqlite3.Error:
            conn.rollback()
            raise
    return write_retrying("metrics", write)


def rows(conn: sqlite3.Connection) -> list:
    names = ", ".join(f'"{c}"' for c in _COLS)
    cur = conn.execute(f"SELECT {names} FROM metrics ORDER BY ts")
    return [dict(r) for r in cur.fetchall()]


def phase_row_dicts(ts: int, checkpoint: str, node: str, parsed_rows) -> list:
    """Turn `signals.parse_phase_summary` output into `gc_phases` rows. Pure: unit-tested without a db."""
    out = []
    for r in parsed_rows:
        out.append({
            "ts": ts,
            "checkpoint": checkpoint,
            "node": node,
            "phase": r["phase"],
            "rounds": r["rounds"],
            "calls": r["calls"],
            "total_us": r["total_us"],
            "max_us": r["max_us"],
            "logs_accounted": r["logs_accounted"],
            "logs_applied": r["logs_applied"],
            "transactions_unapplied": r["transactions_unapplied"],
            "ref_folding_aborted": r["ref_folding_aborted"],
            "metrics_json": json.dumps(r["metrics"], sort_keys=True),
            "events_json": json.dumps(r["events"], sort_keys=True),
        })
    return out


def record_phases(conn: sqlite3.Connection, phase_rows: list) -> int:
    """Persist `gc_phases` rows. Returns how many were written."""
    if not phase_rows:
        return 0
    names = ", ".join(f'"{c}"' for c in _PHASE_COLS)
    placeholders = ", ".join("?" for _ in _PHASE_COLS)
    values = [[row.get(c) for c in _PHASE_COLS] for row in phase_rows]

    def write():
        try:
            conn.executemany(f"INSERT INTO gc_phases ({names}) VALUES ({placeholders})", values)
            conn.commit()
        except sqlite3.Error:
            conn.rollback()
            raise
    return len(phase_rows) if write_retrying("gc_phases", write) else 0


def gc_phase_rows(conn: sqlite3.Connection) -> list:
    names = ", ".join(f'"{c}"' for c in _PHASE_COLS)
    cur = conn.execute(f"SELECT {names} FROM gc_phases ORDER BY ts")
    return [dict(r) for r in cur.fetchall()]


def snapshot_cluster(cluster, table: str, ts: int, fsck: dict | None = None, restarts: int = 0,
                     signals_by_node: dict | None = None) -> list:
    """Build per-node snapshot dicts from `system.parts` + (optional) an fsck result.

    The pool object count/bytes come from a backend LIST done elsewhere (or left
    `None` here and filled by the caller via an S3 list); keep this
    dependency-light — query only what system tables give per node.

    `signals_by_node` maps `repr(node)` to the CAS signal counters already read by the caller (see
    `soak.signals.read_signal_events`), or to `None` for a node that was legitimately unreadable. The
    read happens in the caller rather than here so a FAILED read surfaces there instead of being
    folded into this function's per-tick best-effort handling.
    """
    out = []
    for node in cluster.nodes():
        name = node.scalar("SELECT hostName()") if hasattr(node, "scalar") else ""
        parts_active = int(node.scalar(
            f"SELECT count() FROM system.parts WHERE table='{table}' AND active"))
        parts_inactive = int(node.scalar(
            f"SELECT count() FROM system.parts WHERE table='{table}' AND NOT active"))
        table_rows = int(node.scalar(
            f"SELECT sum(rows) FROM system.parts WHERE table='{table}' AND active") or 0)
        bytes_on_disk = int(node.scalar(
            f"SELECT sum(bytes_on_disk) FROM system.parts WHERE table='{table}' AND active") or 0)
        repl_queue = int(node.scalar(
            f"SELECT count() FROM system.replication_queue WHERE table='{table}'") or 0)
        mutations_pending = int(node.scalar(
            f"SELECT count() FROM system.mutations WHERE table='{table}' AND NOT is_done") or 0)
        merges = int(node.scalar(
            f"SELECT count() FROM system.merges WHERE table='{table}'") or 0)

        # B165: server memory. None (not 0) when unavailable, so a gap is visible rather than faked.
        def _mem(sql):
            try:
                v = node.scalar(sql)
                return int(v) if v not in (None, "") else None
            except Exception:
                return None
        mem_resident = _mem("SELECT value FROM system.asynchronous_metrics WHERE metric='MemoryResident'")
        mem_tracking = _mem("SELECT value FROM system.metrics WHERE metric='MemoryTracking'")

        snap = dict(
            ts=ts,
            node=name,
            parts_active=parts_active,
            parts_inactive=parts_inactive,
            table_rows=table_rows,
            bytes_on_disk=bytes_on_disk,
            pool_objects=None,
            pool_bytes=None,
            repl_queue=repl_queue,
            mutations_pending=mutations_pending,
            merges=merges,
            fsck_reachable=(fsck or {}).get("reachable"),
            fsck_unreachable=(fsck or {}).get("unreachable"),
            fsck_dangling=(fsck or {}).get("dangling"),
            fsck_stale_edge=(fsck or {}).get("stale_edge"),
            restarts=restarts,
            mem_resident=mem_resident,
            mem_tracking=mem_tracking,
        )
        sig = (signals_by_node or {}).get(repr(node)) or {}
        for event in _SIGNAL_COLS:
            snap[event] = sig.get(event)
        out.append(snap)
    return out
