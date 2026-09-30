"""CAS / Drilldown: GC, write-path and S3 detail as collapsed rows on one time axis."""
from . import panels as P
from .panels import SETTINGS, OVERVIEW_LINK, r, c, g

GC_WHERE = "disk_name IN ($disk) AND hostName() IN ($host) AND event_type = 'Finish' AND $timeFilter"
COUNTERS_ROW = "All CAS counters, nonzero in range"


def gc_finish_ts(title, desc, cols: dict, *, outcome="Success", unit="short", w=8, h=8, draw="line") -> dict:
    sel = ", ".join(f'{expr} AS "{label}"' for label, expr in cols.items())
    where = GC_WHERE + (f" AND outcome = '{outcome}'" if outcome else "")
    sql = f"""SELECT toUInt64(event_time) * 1000 AS t, {sel}
FROM clusterAllReplicas('$cluster', system.cas_gc_log)
WHERE {where}
ORDER BY t
{SETTINGS}"""
    return P.raw_ts(title, desc, sql, unit=unit, draw=draw, w=w, h=h)


def gc_rows() -> list:
    rounds = P.raw_ts(
        "Rounds per interval, by outcome and host",
        "Finished rounds per bucket. `NotALeader` on a host is normal: only the leader runs the fold. Recurring "
        "`Error` or `Aborted` is not; `Deferred` is a cheap round that found nothing changed.",
        f"""SELECT $timeSeries, concat(hostName(), ' ', toString(outcome)) AS series, count()
FROM clusterAllReplicas('$cluster', system.cas_gc_log)
WHERE {GC_WHERE}
GROUP BY t, series ORDER BY t
{SETTINGS}""", stack=True, draw="bars", interval="30m")
    leader = P.state_timeline(
        "GC leader (1 = this host leads)",
        "`CASGCIsLeader_<disk>` per host. Leadership moves on restart or lease loss; two hosts at 1 at the same time is a "
        "finding (CAS-133 records the leader identity in `gc/state`).",
        f"""SELECT $timeSeries, concat(hostName(), ' ', replaceOne(metric, 'CASGCIsLeader_', '')) AS series, max(value)
FROM clusterAllReplicas('$cluster', system.asynchronous_metric_log)
WHERE hostName() IN ($host) AND metric LIKE 'CASGCIsLeader_%' AND replaceOne(metric, 'CASGCIsLeader_', '') IN ($disk) AND $timeFilter
GROUP BY t, series ORDER BY t
{SETTINGS}""", w=8)
    duration = gc_finish_ts("Round duration by outcome",
                            "`duration_ms` per finished round, all outcomes. Phase rows do not sum to it: bookkeeping between phases is untimed (CAS-134).",
                            {"duration": "duration_ms"}, outcome=None, unit="ms", draw="points")
    last_rounds = P.table(
        "Last 50 rounds",
        "Finish rows, newest first, with counts and the error text. `error_code` is the structured twin of `error`.",
        f"""SELECT event_time AS "Time", hostName() AS "Host", round AS "Round", toString(trigger) AS "Trigger", toString(outcome) AS "Outcome",
       duration_ms AS "ms", candidates_marked AS "Marked", objects_deleted AS "Deleted", entries_condemned AS "Condemned",
       entries_graduated AS "Graduated", entries_redeleted AS "Redeleted", anomalies AS "Anomalies", fence_outs AS "Fence-outs",
       error_code AS "Code", error AS "Error"
FROM clusterAllReplicas('$cluster', system.cas_gc_log)
WHERE {GC_WHERE}
ORDER BY event_time DESC LIMIT 50
{SETTINGS}""", table="cas_gc_log")
    stages = gc_finish_ts("Stage flow per round: condemned → graduated → redeleted",
                          "The three stages of the retired-cursor pipeline. Condemned this round, graduated next round, redeleted the round after; in steady state the three lines track each other with a two-round lag.",
                          {"Condemned": "entries_condemned", "Graduated": "entries_graduated", "Redeleted": "entries_redeleted"})
    outcomes = gc_finish_ts("Retire candidates per round: spared, replaced, absent",
                            "`objects_spared` (in-degree > 0 at recheck), `objects_replaced` (412-saves: a resurrection won), `objects_absent` (already gone). A steady non-zero `replaced` means writers race GC on the same content.",
                            {"Spared": "objects_spared", "Replaced": "objects_replaced", "Absent": "objects_absent"})
    manifests = gc_finish_ts("Manifests deleted per round",
                             "`manifests_deleted`: owner-removed manifest bodies deleted or found absent (a batch delete cannot tell the two apart).",
                             {"Manifests": "manifests_deleted"})
    cumulative = P.raw_ts(
        "Cumulative stage differences over the period",
        "Running sums of condemned − graduated and graduated − redeleted. These are the durable backlog between stages and cannot go negative the way `CASGCPendingReclaim_*` does.",
        f"""SELECT t, sum(cg) OVER (ORDER BY t) AS "Condemned − graduated", sum(gr) OVER (ORDER BY t) AS "Graduated − redeleted"
FROM (SELECT toUInt64(event_time) * 1000 AS t, toInt64(entries_condemned) - toInt64(entries_graduated) AS cg,
             toInt64(entries_graduated) - toInt64(entries_redeleted) AS gr
      FROM clusterAllReplicas('$cluster', system.cas_gc_log)
      WHERE {GC_WHERE} AND outcome = 'Success')
ORDER BY t
{SETTINGS}""")
    phases_stack = P.raw_ts(
        "Phase durations per bucket, stacked",
        "`phase_duration_microseconds` summed per bucket and phase. On otel.demo `fold_ref_intake` and `pending_deletes` dominate; a phase that grows across rounds is the one to read in the round log.",
        f"""SELECT $timeSeries, toString(phase) AS phase, sum(phase_duration_microseconds) / 1e6
FROM clusterAllReplicas('$cluster', system.cas_gc_log)
WHERE disk_name IN ($disk) AND hostName() IN ($host) AND event_type = 'Phase' AND $timeFilter
GROUP BY t, phase ORDER BY t
{SETTINGS}""", unit="s", stack=True, draw="bars", interval="30m", w=12)
    phases_table = P.table(
        "Phases: duration and S3 requests per phase",
        "Per phase over the selected range. S3 requests issued on GC worker pools are not attributed to their phase and LIST is missing from Phase rows entirely (CAS-135); the round-level ProfileEvents in the Finish row are complete.",
        f"""SELECT toString(phase) AS "Phase", count() AS "Rounds",
       round(quantile(0.5)(phase_duration_microseconds) / 1e6, 2) AS "p50 s", round(quantile(0.99)(phase_duration_microseconds) / 1e6, 2) AS "p99 s",
       sum(ProfileEvents['S3ListObjects']) AS "LIST", sum(ProfileEvents['S3GetObject']) AS "GET", sum(ProfileEvents['S3HeadObject']) AS "HEAD",
       sum(ProfileEvents['S3PutObject']) AS "PUT", sum(ProfileEvents['S3DeleteObjects']) AS "DELETE"
FROM clusterAllReplicas('$cluster', system.cas_gc_log)
WHERE disk_name IN ($disk) AND hostName() IN ($host) AND event_type = 'Phase' AND $timeFilter
GROUP BY phase ORDER BY "p99 s" DESC
{SETTINGS}""", table="cas_gc_log", w=12)
    cost = gc_finish_ts("S3 requests per round, by verb",
                        "Round-level `ProfileEvents` from the Finish row. A GC round is ~55–100x the S3 cost of one insert on otel.demo, driven by GET and HEAD verification, not PUT.",
                        {"PUT": "ProfileEvents['S3PutObject']", "GET": "ProfileEvents['S3GetObject']", "HEAD": "ProfileEvents['S3HeadObject']",
                         "LIST": "ProfileEvents['S3ListObjects']", "DELETE": "ProfileEvents['S3DeleteObjects']"}, unit="short")
    per_object = gc_finish_ts("S3 requests per deleted object",
                              "All S3 requests of a round divided by objects deleted + marked + graduated + redeleted. Six requests per garbage blob was the otel.demo figure (audit F7).",
                              {"Requests per item": "(ProfileEvents['S3PutObject'] + ProfileEvents['S3GetObject'] + ProfileEvents['S3HeadObject'] + ProfileEvents['S3ListObjects'] + ProfileEvents['S3DeleteObjects']) / greatest(objects_deleted + candidates_marked + entries_graduated + entries_redeleted, 1)"})
    enumeration = P.metric_ts("Enumeration pages and bulk deletes per second",
                              "`CASGCEnumerationPages` (LIST pages the fold walked) and `CASBulkDeleteRequests`.",
                              {"Enumeration pages": r("CASGCEnumerationPages"), "Bulk delete requests": r("CASBulkDeleteRequests")}, unit="reqps")
    readahead = P.metric_ts("Fold read-ahead: hit, miss, wasted per second",
                            "`CASGCReadAhead*`: read-ahead efficiency of the fold. Wasted above miss means the prefetch window is too wide.",
                            {"Hit": r("CASGCReadAheadHit"), "Miss": r("CASGCReadAheadMiss"), "Wasted": r("CASGCReadAheadWasted")}, unit="reqps", stack=True)
    memory = P.metric_ts("Memory tracking per host",
                         "`CurrentMetric_MemoryTracking`. The ~1.2 GiB saw-tooth per round on otel.demo is the fold's working set (audit F19).",
                         {"Memory": g("MemoryTracking")}, unit="bytes", by_host=True)
    reflog = P.metric_ts("Ref-log keys written vs cleaned per second",
                         "`CASRootPut` against `CASRefCleanupObjectsDeleted`: the audit F14 balance. Written staying above cleaned means the `_log` population grows.",
                         {"Written (CASRootPut)": r("CASRootPut"), "Cleaned (CASRefCleanupObjectsDeleted)": r("CASRefCleanupObjectsDeleted")}, unit="reqps")
    sweeps = P.metric_ts("Ref sweeps deferred / re-armed, stale precommits reclaimed",
                         "`CASRefSweepDeferred`, `CASRefSweepRearmed`, `CASRefStalePrecommitsReclaimed` per 5-minute bucket.",
                         {"Sweep deferred": c("CASRefSweepDeferred"), "Sweep re-armed": c("CASRefSweepRearmed"), "Stale precommits reclaimed": c("CASRefStalePrecommitsReclaimed")})
    anomalies = gc_finish_ts("Anomalies per round", "`anomalies`: fold clamps surfaced and survived. Steady > 0 across rounds warrants reading the round log.",
                             {"Anomalies": "anomalies"}, outcome=None, draw="bars")
    counters = P.metric_ts("GC anomaly counters per 5 minutes",
                           "`CASGCUnmatchedRemoveDeltas` (benign per-key no-op when occasional), `CASGCCondemnMarkerUnconfirmedCarry`, `CASGCMetaWriteAnomaly`, `CASGCHeartbeatFenceOuts`, `CASGCStuckRemovals`, `CASGCNamespaceCleanupLeaks`. All but the first must stay at zero.",
                           {"Unmatched remove deltas": c("CASGCUnmatchedRemoveDeltas"), "Condemn marker unconfirmed carry": c("CASGCCondemnMarkerUnconfirmedCarry"),
                            "Meta write anomaly": c("CASGCMetaWriteAnomaly"), "Heartbeat fence-outs": c("CASGCHeartbeatFenceOuts"),
                            "Stuck removals": c("CASGCStuckRemovals"), "Namespace cleanup leaks": c("CASGCNamespaceCleanupLeaks")})
    anomaly_table = P.table("Fold clamps, GC anomalies and fence-outs from cas_log",
                            "`gc_fold_clamp`, `gc_anomaly`, `gc_fence_out` rows with their reason and detail map.",
                            f"""SELECT event_time AS "Time", hostName() AS "Host", toString(event_type) AS "Event", namespace AS "Namespace", round AS "Round", toString(outcome) AS "Outcome", toString(reason) AS "Reason", toString(detail) AS "Detail"
FROM clusterAllReplicas('$cluster', system.cas_log)
WHERE disk_name IN ($disk) AND hostName() IN ($host) AND event_type IN ('gc_fold_clamp', 'gc_anomaly', 'gc_fence_out') AND $timeFilter
ORDER BY event_time DESC LIMIT 200
{SETTINGS}""")
    return [
        P.row("GC: rounds", [rounds, leader, duration, last_rounds]),
        P.row("GC: stage flow", [stages, outcomes, manifests, cumulative]),
        P.row("GC: phases", [phases_stack, phases_table]),
        P.row("GC: round cost", [cost, per_object, enumeration, readahead, memory]),
        P.row("GC: ref-log cleanup", [reflog, sweeps]),
        P.row("GC: anomalies", [anomalies, counters, anomaly_table]),
    ]


def writes_rows() -> list:
    return []


def s3_rows() -> list:
    return []


def counters_row() -> dict:
    return P.row(COUNTERS_ROW, [])


def build() -> dict:
    rows = gc_rows() + writes_rows() + s3_rows()
    cr = counters_row()
    if cr["panels"]:
        rows.append(cr)
    return P.dashboard(
        "cas-drilldown", "CAS / Drilldown",
        "Content-addressed (CAS) storage internals for support engineers and CAS developers: garbage-collection rounds, "
        "stages and phases; the write path, ref lanes and mount leases; S3 requests by object class. Rows are collapsed; "
        "open the one the Overview pointed at.",
        rows, tags=["cas", "clickhouse"],
        links=[{"title": "CAS / Overview", "type": "link", "url": OVERVIEW_LINK["url"], "keepTime": True, "includeVars": True, "icon": "dashboard"}])
