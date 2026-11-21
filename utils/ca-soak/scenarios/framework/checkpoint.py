"""The quiesced end checkpoint — the heart of every scenario's "did the pool come to rest cleanly?".

Sequence (README §"Common run contract"):
  quiesce → settle fsck → forced GC to fixpoint → final detailed fsck + dry-run →
  collect observations → dump raw extracts → run the common hard assertions.

Returns a dict of end-state observations and also stashes them on the result. Separated from
lifecycle.py to keep the import graph acyclic (this module is the one place that pulls together
lifecycle + observe + assertions + report).
"""

import time

from . import assertions, gc as gc_mod, lifecycle, observe
from .report import Verdict


def end_checkpoint(ctx, cluster, result, tables, *, table_filter=None, abandons=False,
                   expect_exception=False, since_event_time=None, optimize=True,
                   fsck_container=None) -> dict:
    """Run the quiesced end checkpoint and the common assertions; return the end-state dict."""
    # Resolve the fsck container HERE, not in this signature's default: a default argument binds
    # `lifecycle.DEFAULT_FSCK_CONTAINER` at import, which is read from the environment at import too.
    # A variant that publishes different container names then cannot be honoured however late the env
    # is set. That is not theoretical — S41 reached 43/45 verdicts with the two fsck ones failing as
    # "summary unavailable", because this default still named `ca-soak-ch1-1` while the stack was
    # `ca-s41-ch1-1` (2026-09-01). Third site of the same defect, after `lifecycle` and `observe`.
    fsck_container = fsck_container or lifecycle.fsck_container()
    t0 = time.monotonic()
    ctx.log("end checkpoint: quiescing cluster")
    try:
        quiesced_now = lifecycle.quiesce_cluster(cluster, tables, table_filter=table_filter,
                                                 optimize=optimize, log_fn=ctx.log)
        result.timings["quiesce_s"] = round(time.monotonic() - t0, 1)
    except Exception as e:
        result.note_anomaly(f"quiescence failed: {e}")
        result.add_inconclusive = True
        ctx.log(f"end checkpoint: quiescence raised: {e}")
        quiesced_now = None

    ctx.log("end checkpoint: settling fsck")
    try:
        lifecycle.settle_fsck(fsck_container, log_fn=ctx.log)
    except Exception as e:
        ctx.log(f"settle_fsck raised: {e}")

    ctx.log("end checkpoint: driving forced GC to fixpoint")
    tg = time.monotonic()
    residual, history = gc_mod.forced_gc_to_fixpoint(
        cluster, lifecycle.unreachable_probe(fsck_container), log_fn=ctx.log)
    result.timings["forced_gc_s"] = round(time.monotonic() - tg, 1)
    result.observations["gc_fixpoint_history"] = history
    result.observations["gc_residual_unreachable"] = residual
    ctx.log(f"end checkpoint: forced GC residual unreachable={residual} (rounds={len(history)})")

    # A bounded residual here is typically CONDEMNED content (fsck pending-gc): GC reclaims a blob over
    # three rounds (intent, body delete, intent delete) and forced_gc_to_fixpoint polls faster than
    # rounds complete. Drive single-leader rounds until the body count reaches 0, so a healthy pipeline
    # drains and assert_no_leftovers sees the true residual. A residual that will NOT drain survives
    # and is classified by fsck class downstream (unreachable/dangling => real leak => FAIL).
    if residual and residual > 0:
        ctx.log(f"end checkpoint: draining the intent deletion pipeline (residual={residual})")
        td = time.monotonic()
        residual, drain_hist = gc_mod.drain_condemned_pipeline(
            cluster, lifecycle.unreachable_probe(fsck_container), log_fn=ctx.log)
        result.timings["pipeline_drain_s"] = round(time.monotonic() - td, 1)
        result.observations["pipeline_drain_history"] = drain_hist
        result.observations["gc_residual_unreachable"] = residual
        ctx.log(f"end checkpoint: post-drain residual unreachable={residual}")

    # The last intent is deleted one round after the last body, so `unreachable == 0` does not mean
    # `blob-intents/` is empty. Record the count after the final rounds; the report shows it, no verdict
    # depends on it (a blob referenced again keeps its intent until it reaches zero references once more).
    try:
        ti = time.monotonic()
        intents_left, intent_hist = gc_mod.drain_intents(
            cluster, lifecycle.intent_listed_probe(fsck_container), log_fn=ctx.log)
        result.timings["intent_drain_s"] = round(time.monotonic() - ti, 1)
        result.observations["intent_listed_after_drain"] = intents_left
        result.observations["intent_drain_history"] = intent_hist
        ctx.log(f"end checkpoint: intent_listed after drain={intents_left} (history={intent_hist})")
    except Exception as e:
        ctx.log(f"drain_intents raised: {e}")

    ctx.log("end checkpoint: final detailed fsck + dry-run")
    fsck_final = {}
    fsck_det = {}
    dry = {}
    try:
        fsck_det = lifecycle.fsck_detail(fsck_container)
        fsck_final = fsck_det  # detailed fsck also carries the summary fields
    except Exception as e:
        ctx.log(f"final fsck_detail raised: {e}")
    try:
        dry = lifecycle.dryrun(fsck_container)
    except Exception as e:
        ctx.log(f"dryrun raised: {e}")

    ctx.log("end checkpoint: collecting observations")
    gc_all = observe.gc_log_all(cluster, since_event_time)
    ca_events = observe.ca_event_counts_all(cluster, since_event_time)
    pool = observe.pool_shape()
    mem = observe.cluster_memory(cluster)
    conts = observe.container_samples()

    s3_rates = {n.container: observe.s3_error_rates(n) for n in cluster.nodes()}
    result.observations["s3_error_rates"] = s3_rates
    worst_read = max((v["read_error_rate"] or 0.0) for v in s3_rates.values()) if s3_rates else 0.0
    worst_write = max((v["write_error_rate"] or 0.0) for v in s3_rates.values()) if s3_rates else 0.0
    result.add(Verdict("S3 error rates (info)", "recorded; store-dependent, no fixed budget",
                       f"read max {worst_read:.1%}, write max {worst_write:.1%}", "pass",
                       "10-20% read-error rates were invisible all campaign (2026-07-06 re-audit); "
                       "a spike here explains retry storms/slowness in the same window"))

    result.observations["pool_shape"] = pool
    result.observations["server_memory_final"] = mem
    result.observations["container_samples_final"] = conts
    result.observations["ca_event_counts"] = ca_events
    result.observations["gc_log"] = gc_all

    ctx.write_json("end_state.json", {
        "quiesced_now": quiesced_now,
        "fsck_final": {k: v for k, v in fsck_final.items() if k not in ("stdout", "stderr", "detail")},
        "dryrun_count": dry.get("count"),
        "gc_residual_unreachable": residual,
        "intent_listed_after_drain": result.observations.get("intent_listed_after_drain"),
        "gc_fixpoint_history": history,
        "pool_shape": pool,
        "gc_summary": gc_all.get("summary"),
        "ca_event_bad_total": ca_events.get("bad_total"),
    })

    ctx.log("end checkpoint: dumping raw extracts")
    try:
        observe.dump_standard_extracts(ctx, cluster)
    except Exception as e:
        ctx.log(f"dump_standard_extracts raised: {e}")

    # Forensics: if any object is dangling, unaccounted (outside the whole GC view — should be
    # impossible per INV-2 once GC has run), or legacy-unreachable, capture the full per-object
    # lifetime from system.cas_log BEFORE the next run's reset wipes the pool.
    # pending-gc / awaiting-gc rows are the deletion pipeline working as designed — NOT forensics
    # triggers (they churn nonzero on any active pool).
    try:
        det_rows = fsck_det.get("detail", []) if fsck_det else []
        if any(r.get("class") in ("dangling", "unaccounted", "unreachable") for r in det_rows):
            forensics = observe.dump_object_forensics(ctx, cluster, fsck_det)
            result.observations["forensics"] = forensics
            ctx.log(f"end checkpoint: object forensics captured -> {forensics}")
    except Exception as e:
        ctx.log(f"dump_object_forensics raised: {e}")

    ctx.log("end checkpoint: running common assertions")
    assertions.run_common_assertions(
        result, fsck_final=fsck_final, fsck_detail_res=fsck_det, dryrun_res=dry,
        ca_events=ca_events, gc_summary=gc_all.get("summary"), abandons=abandons,
        expect_exception=expect_exception, residual_after_gc=residual, pool_shape=pool)

    result.timings["end_checkpoint_s"] = round(time.monotonic() - t0, 1)
    return {
        "fsck_final": fsck_final, "fsck_detail": fsck_det, "dryrun": dry,
        "gc_all": gc_all, "ca_events": ca_events, "pool_shape": pool,
        "residual_unreachable": residual, "quiesced_now": quiesced_now,
    }
