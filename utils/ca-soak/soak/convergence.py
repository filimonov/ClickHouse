"""Stale-edge convergence at a checkpoint.

`cas-fsck --detail` reads `gc/state` and later lists the manifests. A GC round that commits in between
makes blobs look stale although the pool is correct. A real stale edge is permanent and shows on every
later scan, so a checkpoint fails only on keys that both a first scan and a scan after further GC rounds
report. An unfolded drop is not a stale edge: fsck reports it as `awaiting-gc`.
"""

import json
import time


class RoundWaitTimeout(Exception):
    """Fewer than the required GC rounds committed before the deadline."""


def stale_keys(fsck_result: dict) -> set:
    return {r["key"] for r in fsck_result.get("detail", []) if r.get("class") == "stale-edge"}


def highest_committed_round(nodes, disk: str = "ca"):
    """Highest round of a led, completed (`Success`) GC round on any node, or None when none answered."""
    best = None
    for node in nodes:
        try:
            node.query("SYSTEM FLUSH LOGS", timeout=120)
            out = node.query("SELECT max(round) FROM system.cas_gc_log "
                             f"WHERE event_type = 'Finish' AND outcome = 'Success' AND disk_name = '{disk}'", timeout=120)
        except Exception:   # noqa: BLE001 -- a node down under chaos only makes the reading lower, never wrong
            continue
        if out.strip().isdigit():
            best = max(best or 0, int(out))
    return best


def wait_for_committed_rounds(nodes, *, above_round, need=2, timeout_s=600, disk: str = "ca",
                              sleep_fn=time.sleep, now_fn=time.time):
    """Drive `SYSTEM CAS GC RUN` on every node until `need` led, non-deferred rounds numbered above
    `above_round` have committed. Returns the round numbers seen; raises `RoundWaitTimeout` otherwise."""
    t0 = now_fn()
    seen = set()
    while len(seen) < need:
        if now_fn() - t0 > timeout_s:
            raise RoundWaitTimeout(f"only {len(seen)} of {need} led, non-deferred GC rounds above round "
                                   f"{above_round} committed within {timeout_s} s")
        for node in nodes:
            try:
                out = node.query(f"SYSTEM CAS GC RUN {disk}", timeout=600, settings={"default_format": "JSONEachRow"})
            except Exception:   # noqa: BLE001 -- retried until the deadline
                sleep_fn(1)
                continue
            for line in out.splitlines():
                d = json.loads(line)
                if int(d.get("acquired_lease", 0)) and not int(d.get("deferred", 0)) and int(d.get("round", 0)) > above_round:
                    seen.add(int(d["round"]))
    return sorted(seen)


def converge(first: dict, *, rescan, wait_rounds, log_fn, label: str):
    """Decide a `--detail` scan that reports stale edges.

    `rescan()` runs the second detail scan; `wait_rounds()` blocks until the GC rounds committed
    (raising `RoundWaitTimeout`). Returns `(verdict, text, second_scan)` with verdict `converged`
    (the two stale sets are disjoint), `persistent` (they intersect) or `undecided` (the wait timed out)."""
    s1 = stale_keys(first)
    if not s1:
        return "persistent", f"stale_edge={first.get('stale_edge')} but the detail lists no stale-edge key to follow", None
    try:
        rounds = wait_rounds()
    except RoundWaitTimeout as e:
        log_fn(f"stale-edge convergence [{label}]: |S1|={len(s1)}, round wait failed: {e}")
        return "undecided", str(e), None
    second = rescan()
    s2 = stale_keys(second)
    if not s2 and second.get("stale_edge"):
        return "persistent", f"second scan: stale_edge={second.get('stale_edge')} but the detail lists no stale-edge key", second
    both = s1 & s2
    log_fn(f"stale-edge convergence [{label}]: |S1|={len(s1)} |S2|={len(s2)} |S1&S2|={len(both)} "
           f"(rounds committed between the scans: {rounds})")
    if both:
        return "persistent", f"{len(both)} stale-edge key(s) in both scans, first: {sorted(both)[:5]}", second
    return "converged", f"{len(s1)} stale-edge key(s) of the first scan are gone from the second", second
