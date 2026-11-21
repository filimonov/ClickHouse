"""Explicit GC driving for the scenario suite.

The README requires that every scenario drive `SYSTEM CAS GC RUN ca`
explicitly at checkpoints — even when background GC is enabled — so a report can separate workload
cost from reclamation cost. `SYSTEM CAS GC RUN ca` runs ONE synchronous GC
round on the node that receives it (only the lease holder makes progress; a non-leader round is a
cheap no-op), so a fixpoint drive issues rounds on both replicas until the unreachable count settles.
"""

import time

from soak.cluster import QueryError

GC_SQL = "SYSTEM CAS GC RUN {disk}"


def gc_round(node, disk: str = "ca", timeout: float = 600.0) -> bool:
    """Issue one synchronous GC round on `node`. Returns True on success. A `QueryError` is re-raised
    (a GC command that throws IS a finding) except that the call is the caller's to guard."""
    node.command(GC_SQL.format(disk=disk), timeout=timeout)
    return True


def gc_drive_round(cluster, disk: str = "ca", timeout: float = 600.0, log_fn=print, node_index: int = 0) -> int:
    """Issue ONE explicit GC round on a SINGLE replica (default node 0). Returns 1 if it completed
    without raising, else 0.

    Single-leader by design: issuing explicit GC on BOTH replicas in the same round creates two
    concurrent GC leaders, whose fold-seal collision can permanently orphan blobs (a reclaim leak —
    safety still holds, `dangling==0`; see BACKLOG "explicit-GC concurrent leaders leak"). Driving one
    replica keeps a single leader; the lease-gated background GC on the other replica is a no-op. An
    ABORTED (raced the background tick) is logged as a benign retry, not a failure."""
    node = cluster.nodes()[node_index]
    try:
        gc_round(node, disk, timeout)
        return 1
    except QueryError as e:
        if e.is_aborted:
            log_fn(f"GC round on {node.container} raced background tick (ABORTED) — benign retry")
        else:
            log_fn(f"GC round on {node.container} raised: {e}")
    except Exception as e:  # transport (node down) — background GC still runs; keep going
        log_fn(f"GC round on {node.container} transport error: {e}")
    return 0


def forced_gc_to_fixpoint(cluster, unreachable_fn, *, max_seconds: float = 240.0, stable: int = 3,
                          disk: str = "ca", round_timeout: float = 600.0, poll_interval_s: float = 3.0,
                          nudge_every_s: float = 12.0, log_fn=print,
                          sleep_fn=time.sleep, monotonic_fn=time.monotonic):
    """Drain the pool to its reclamation fixpoint and return (residual_unreachable, history).

    Drains PRIMARILY via the lease-gated BACKGROUND GC, which runs a single leader per round and
    reclaims cleanly. We issue ONE explicit single-node `SYSTEM CAS GC RUN`
    round up front (satisfies the README "drive explicit GC at checkpoints" + the cost-measurement
    requirement) and then POLL the fsck unreachable count, letting background GC do the work, until it
    stops decreasing for `stable` consecutive reads or `max_seconds` elapses. If the count stalls above
    zero we issue an occasional, WELL-SEPARATED single-node explicit nudge.

    Why not hammer explicit rounds: issuing explicit GC on BOTH replicas — or sustained explicit GC
    overlapping the 2 s background tick under load — creates two concurrent GC leaders. The fold seal
    correctly aborts the divergent fold (safety holds, `dangling==0`), but the colliding round can
    advance GC generation/cursor state past owner-removal events that were never folded, PERMANENTLY
    orphaning their blobs (a reclaim/liveness leak; see BACKLOG "GC-CONCURRENT-LEADER-LEAK"). Letting
    background GC own the drain keeps a single leader and reclaims cleanly."""
    history = []
    node = cluster.nodes()[0]

    def _explicit_round(reason):
        try:
            gc_round(node, disk, round_timeout)
        except QueryError as e:
            if e.is_aborted:
                log_fn(f"forced_gc_to_fixpoint: explicit {reason} raced background tick (ABORTED) — benign")
            else:
                log_fn(f"forced_gc_to_fixpoint: explicit {reason} on {node.container} raised: {e}")
        except Exception as e:
            log_fn(f"forced_gc_to_fixpoint: explicit {reason} transport error: {e}")

    _explicit_round("checkpoint round")
    deadline = monotonic_fn() + max_seconds
    last_nudge = monotonic_fn()
    while True:
        sleep_fn(poll_interval_s)
        try:
            n = int(unreachable_fn())
        except Exception as e:
            log_fn(f"forced_gc_to_fixpoint: unreachable probe failed: {e}")
            if monotonic_fn() > deadline:
                return (history[-1] if history else None), history
            continue
        history.append(n)
        if n == 0 or (len(history) >= stable and len(set(history[-stable:])) == 1):
            return n, history
        now = monotonic_fn()
        # Stalled above zero for the whole nudge window -> one well-separated explicit nudge.
        if n > 0 and (now - last_nudge) >= nudge_every_s:
            _explicit_round("nudge")
            last_nudge = now
        if now > deadline:
            return n, history


def drain_condemned_pipeline(cluster, unreachable_fn, *, sync_period_s: float = 3.0,
                             max_seconds: float = 150.0, disk: str = "ca", round_timeout: float = 600.0,
                             log_fn=print, sleep_fn=time.sleep, monotonic_fn=time.monotonic):
    """Drive the intent deletion pipeline to completion and return (residual, history).

    `GC` reclaims a blob over three rounds: round n creates its deletion intent, round n+1 deletes the
    body, round n+2 deletes the intent. `forced_gc_to_fixpoint` can report a bounded residual of
    condemned content (fsck `pending-gc`) as "settled" because it polls faster than rounds complete.
    Here we drive ONE single-leader round, pause `sync_period_s` (pacing only: nothing gates a round on
    a clock), and repeat until the residual reaches 0 or `max_seconds`. A healthy residual drains in
    about three rounds; a residual that does NOT drain within the budget is a real finding — the
    caller's `assert_no_leftovers` classifies the surviving residual by fsck class (unreachable/dangling
    => leak => FAIL).

    This counts bodies (`fsck.unreachable`), not intents: use `drain_intents` for the last round."""
    history = []
    node = cluster.nodes()[0]
    deadline = monotonic_fn() + max_seconds
    while True:
        try:
            n = int(unreachable_fn())
        except Exception as e:
            log_fn(f"drain_condemned_pipeline: unreachable probe failed: {e}")
            n = None
        if n is not None:
            history.append(n)
            if n == 0:
                return 0, history
        if monotonic_fn() >= deadline:
            return (n if n is not None else (history[-1] if history else None)), history
        try:
            gc_round(node, disk, round_timeout)
        except QueryError as e:
            if e.is_aborted:
                log_fn(f"drain_condemned_pipeline: round raced background tick (ABORTED) — benign")
            else:
                log_fn(f"drain_condemned_pipeline: round on {node.container} raised: {e}")
        except Exception as e:
            log_fn(f"drain_condemned_pipeline: round transport error: {e}")
        sleep_fn(sync_period_s)


def drain_intents(cluster, intent_listed_fn, *, max_seconds: float = 90.0, stable: int = 2,
                  disk: str = "ca", round_timeout: float = 600.0, log_fn=print,
                  sleep_fn=time.sleep, monotonic_fn=time.monotonic, pause_s: float = 3.0):
    """Drive rounds until the number of keys under `blob-intents/` (`fsck.intents_listed`; `intent_listed` in older builds) is 0 or has not
    changed for `stable` consecutive rounds. Returns (last_count_or_None, history).

    The body count reaches 0 one round before the last intent is deleted (round n+2), so a checkpoint
    that stops at `unreachable == 0` can still see intents. A count that stops above 0 is NOT a failure
    by itself: a blob that was referenced again after `GC` condemned it keeps its intent until it reaches
    zero references once more (docs `architecture/garbage-collection.md#orphan-intents`). The caller
    records the count; it decides nothing here."""
    history = []
    node = cluster.nodes()[0]
    deadline = monotonic_fn() + max_seconds
    while True:
        try:
            n = int(intent_listed_fn())
        except Exception as e:
            log_fn(f"drain_intents: intent probe failed: {e}")
            n = None
        if n is not None:
            history.append(n)
            if n == 0 or (len(history) > stable and len(set(history[-(stable + 1):])) == 1):
                return n, history
        if monotonic_fn() >= deadline:
            return (n if n is not None else (history[-1] if history else None)), history
        try:
            gc_round(node, disk, round_timeout)
        except QueryError as e:
            if not e.is_aborted:
                log_fn(f"drain_intents: round on {node.container} raised: {e}")
        except Exception as e:
            log_fn(f"drain_intents: round transport error: {e}")
        sleep_fn(pause_s)
