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
        f"""SELECT toUInt64(event_time) * 1000 AS "Time", hostName() AS "Host", round AS "Round", toString(trigger) AS "Trigger", toString(outcome) AS "Outcome",
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
                            f"""SELECT toUInt64(event_time) * 1000 AS "Time", hostName() AS "Host", toString(event_type) AS "Event", namespace AS "Namespace", round AS "Round", toString(outcome) AS "Outcome", toString(reason) AS "Reason", toString(detail) AS "Detail"
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
    mounts = P.table(
        "Mounts now",
        "Live view from `system.cas_mounts` (one backend `listMounts` per query, hence the 5-minute refresh). `state` is the slot as the backend sees it; `lifecycle` is this host's own view of the disk. `is_leader` is NULL on rows describing other hosts' mounts.",
        f"""SELECT hostName() AS "Host", disk AS "Disk", hostname AS "Slot owner", state AS "State", lifecycle AS "Lifecycle",
       lifecycle_detail AS "Lifecycle detail", dateDiff('second', now(), expires_at) AS "Expires in (s)", writer_epoch AS "Epoch",
       renewal_sequence AS "Renewals", is_leader AS "GC leader", gc_fenced AS "Fenced", wedged_namespace_count AS "Wedged lanes"
FROM clusterAllReplicas('$cluster', system.cas_mounts)
WHERE disk IN ($disk)
ORDER BY "Host", "Disk", "Slot owner"
{SETTINGS}""", table="cas_mounts", interval="5m")
    renewals = P.metric_ts("Lease renewals per second",
                           "`CASMountRenewal*`: physical attempts, retries after the first PUT, resolved by an exact GET, recovered after retry, stopped by the external deadline.",
                           {"Attempts": r("CASMountRenewalAttempts"), "Retries": r("CASMountRenewalRetries"), "Resolved": r("CASMountRenewalResolved"),
                            "Recovered": r("CASMountRenewalRecovered"), "Deadline exceeded": r("CASMountRenewalDeadlineExceeded")}, unit="ops")
    remounts = P.metric_ts("Lease lost and remounts per 5 minutes",
                           "`CASMountLeaseLost` (once per Live → TransientNotLive generation) and whole-chain remount attempts / successes / failures.",
                           {"Lease lost": c("CASMountLeaseLost"), "Remount attempts": c("CASRemountAttempts"), "Remount succeeded": c("CASRemountSucceeded"), "Remount failed": c("CASRemountFailed")})
    lease_events = P.table("Renewal and remount events",
                           "`watermark_renew` (only nontrivial renewals: outcome recovered/failed) and `mount_remount` rows from `cas_log`, with `classification`, `attempts_sent`, `elapsed_ms` from the detail map.",
                           f"""SELECT toUInt64(event_time) * 1000 AS "Time", hostName() AS "Host", toString(event_type) AS "Event", toString(outcome) AS "Outcome",
       detail['classification'] AS "Classification", detail['attempts_sent'] AS "Attempts", detail['elapsed_ms'] AS "Elapsed ms",
       detail['step'] AS "Step", detail['error'] AS "Error", toString(reason) AS "Reason"
FROM clusterAllReplicas('$cluster', system.cas_log)
WHERE disk_name IN ($disk) AND hostName() IN ($host) AND event_type IN ('watermark_renew', 'mount_remount') AND $timeFilter
ORDER BY event_time DESC LIMIT 200
{SETTINGS}""")
    cond = P.metric_ts("Conditional writes per second",
                       "`CASConditionalWrite*`. Unresolved = response lost, resolved by a read; DefiniteFailure = the backend said no; FenceLostPostWrite = the write landed after the lease was gone.",
                       {"Attempts": r("CASConditionalWriteAttempts"), "Committed": r("CASConditionalWriteCommitted"), "Unresolved": r("CASConditionalWriteUnresolved"),
                        "Definite failure": r("CASConditionalWriteDefiniteFailure"), "Fence lost post-write": r("CASConditionalWriteFenceLostPostWrite")}, unit="ops")
    requests = P.metric_ts("Request outcomes per second",
                           "`CASRequest*`: reissues (retries), conflict pauses (409 on one key), resolve reads, gave up, refused, first-attempt fuse.",
                           {"Reissue": r("CASRequestReissue"), "Conflict pause": r("CASRequestConflictPause"), "Resolve read": r("CASRequestResolveRead"),
                            "Gave up": r("CASRequestGaveUp"), "Refused": r("CASRequestRefused"), "First-attempt fuse": r("CASRequestFirstAttemptFuse")}, unit="ops")
    conflicts = P.metric_ts("Compare-and-swap conflicts per second, by object class",
                            "`CAS<Class>CompareSwapConflict`. Root conflicts equal `CASRequestConflictPause` on otel.demo: replicas racing on `_ckpt`.",
                            {"Blob": r("CASBlobCompareSwapConflict"), "Manifest": r("CASManifestCompareSwapConflict"), "Root": r("CASRootCompareSwapConflict"),
                             "GC": r("CASGCCompareSwapConflict"), "Other": r("CASOtherCompareSwapConflict")}, unit="ops")
    lane_wait = P.metric_ts("Ref-lane wait per mutation (ms)",
                            "`CASRefQueueWaitMicroseconds / CASRefBatchedMutations` per 5-minute bucket. Read as a rate over minutes: a single second can sum hundreds of mutations finishing together (audit F25).",
                            {"Wait per mutation": "sum(ProfileEvent_CASRefQueueWaitMicroseconds) / greatest(sum(ProfileEvent_CASRefBatchedMutations), 1) / 1000"}, unit="ms")
    batching = P.metric_ts("Mutations per flush and flushes per second",
                           "`CASRefBatchedMutations / CASRefBatchFlushes` (3.1 in steady state, up to 34 in a removal burst) and `CASRefBatchFlushes` per second (≤ 5, the lane's bound).",
                           {"Mutations per flush": "sum(ProfileEvent_CASRefBatchedMutations) / greatest(sum(ProfileEvent_CASRefBatchFlushes), 1)",
                            "Flushes per second": r("CASRefBatchFlushes"), "Scope cuts per second": r("CASRefBatchScopeCuts")})
    lane_health = P.metric_ts("Ref-lane health counters per 5 minutes",
                              "`CASRefAppendWedged`, `CASRefNeedsRecovery` (must be zero), `CASRefAppendSealRejected` (occasional is the protocol working), `CASRefAppendUnwedged`, `CASRefAppendDefiniteFailure`.",
                              {"Wedged": c("CASRefAppendWedged"), "Needs recovery": c("CASRefNeedsRecovery"), "Seal rejected": c("CASRefAppendSealRejected"),
                               "Unwedged": c("CASRefAppendUnwedged"), "Definite failure": c("CASRefAppendDefiniteFailure")})
    checkpoints = P.metric_ts("Checkpoints and snapshots per 5 minutes",
                              "`CASRefCheckpoint*` and `CASRefSnapshotPublish*`. `IdenticalSkip` means the `_ckpt` overwrite was avoided (audit F5).",
                              {"Checkpoint published": c("CASRefCheckpointPublished"), "Checkpoint identical skip": c("CASRefCheckpointIdenticalSkip"),
                               "Checkpoint not advanced": c("CASRefCheckpointNotAdvanced"), "Snapshot publish dispatched": c("CASRefSnapshotPublishDispatched"),
                               "Snapshot publish backoff": c("CASRefSnapshotPublishBackoff")})
    hotkey = P.metric_ts("Hot-key lane",
                         "`CASHotKey*`: never engaged on otel.demo in a week. Wait per start in ms, plus cache starts, read starts and verdicts re-read.",
                         {"Wait per start (ms)": "sum(ProfileEvent_CASHotKeyQueueWaitMicroseconds) / greatest(sum(ProfileEvent_CASHotKeyCacheStarts) + sum(ProfileEvent_CASHotKeyReadStarts), 1) / 1000",
                          "Cache starts": c("CASHotKeyCacheStarts"), "Read starts": c("CASHotKeyReadStarts"), "Verdicts re-read": c("CASHotKeyCacheVerdictsReread")})
    blobs = P.metric_ts("Blob publications per second",
                        "`CASBlobHeadMiss` = new blobs; `CASBlobPutDeduplicated` = content already present; `CASBlobBodyPutAvoided` = body upload skipped; `CASBlobAdoptTrusted` counts the `delete_tmp` repoint re-adopting a part's own blobs, not dedup (audit F2).",
                        {"New (HeadMiss)": r("CASBlobHeadMiss"), "Deduplicated": r("CASBlobPutDeduplicated"), "Body put avoided": r("CASBlobBodyPutAvoided"), "Adopt trusted": r("CASBlobAdoptTrusted")}, unit="ops")
    dedup = P.metric_ts("Dedup ratio (%)", "`PutDeduplicated / (PutDeduplicated + HeadMiss)`. 0.27% on the otel.demo workload.",
                        {"Dedup %": "100 * sum(ProfileEvent_CASBlobPutDeduplicated) / greatest(sum(ProfileEvent_CASBlobPutDeduplicated) + sum(ProfileEvent_CASBlobHeadMiss), 1)"}, unit="percent")
    upload_pool = P.metric_ts("Blob upload pool: threads active and scheduled",
                              "`CurrentMetric_CASBlobUploadPoolThreadsActive` / `Scheduled` (max per bucket). Scheduled far above active means uploads queue behind the pool.",
                              {"Active": g("CASBlobUploadPoolThreadsActive"), "Scheduled": g("CASBlobUploadPoolThreadsScheduled"), "Threads": g("CASBlobUploadPoolThreads")})
    fanout = P.metric_ts("Upload fan-out per second", "`CASBlobUploadFanoutBatches` and `CASBlobUploadFanoutTasks`.",
                         {"Batches": r("CASBlobUploadFanoutBatches"), "Tasks": r("CASBlobUploadFanoutTasks")}, unit="ops")
    pfv = P.metric_ts("Part-folder view cache per second",
                      "`CASPartFolderView*`: hits, misses (rebuild from manifests), invalidations, validation mismatches, oversized bypasses. Misses cost ~3 manifest GETs per merge (audit F9).",
                      {"Hits": r("CASPartFolderViewHits"), "Misses": r("CASPartFolderViewMisses"), "Invalidations": r("CASPartFolderViewInvalidations"),
                       "Validation mismatches": r("CASPartFolderViewValidationMismatches"), "Oversized bypasses": r("CASPartFolderViewOversizedBypasses")}, unit="ops")
    pfv_ratio = P.metric_ts("Part-folder view hit ratio (%)", "`Hits / (Hits + Misses)` per 5-minute bucket.",
                            {"Hit %": "100 * sum(ProfileEvent_CASPartFolderViewHits) / greatest(sum(ProfileEvent_CASPartFolderViewHits) + sum(ProfileEvent_CASPartFolderViewMisses), 1)"}, unit="percent")
    pfv_size = P.metric_ts("Part-folder view cache size", "`CASPartFolderCacheBytes` (max per bucket).", {"Bytes": g("CASPartFolderCacheBytes")}, unit="bytes")
    decode = P.metric_ts("Manifest decode cache bytes vs 128 MiB default cap",
                         "`CASManifestDecodeCacheBytes` sat at its cap all week on otel.demo with 1.9–3.3k entries; part-folder rebuilds partly come from decode-cache eviction (audit F25).",
                         {"Bytes": g("CASManifestDecodeCacheBytes"), "Default cap": "134217728"}, unit="bytes")
    cache_entries = P.metric_ts("Cache entries", "`CASManifestDecodeCacheEntries`, `CASPartFolderCacheEntries`, `CASHotKeyCacheEntries`.",
                                {"Manifest decode": g("CASManifestDecodeCacheEntries"), "Part-folder view": g("CASPartFolderCacheEntries"), "Hot-key": g("CASHotKeyCacheEntries")})
    evictions = P.metric_ts("Ref-table evictions and manifest GETs for the part-folder view per second", "`CASRefTableEvictions`, `CASPartFolderManifestGets`.",
                            {"Ref-table evictions": r("CASRefTableEvictions"), "Part-folder manifest GETs": r("CASPartFolderManifestGets")}, unit="ops")
    relink = P.metric_ts("Relink-confirm refusals per 5 minutes, by reason",
                         "`CASRelinkConfirmRefused*` on the receiving replica of a fetch. Zero on otel.demo in a week.",
                         {"Ref mutation in flight": c("CASRelinkConfirmRefusedRefMutationInFlight"), "Lane wedged": c("CASRelinkConfirmRefusedLaneWedged"),
                          "Lane broken": c("CASRelinkConfirmRefusedLaneBroken"), "State lock busy": c("CASRelinkConfirmRefusedStateLockBusy"),
                          "Mount cannot speak": c("CASRelinkConfirmRefusedMountCannotSpeak")})
    return [
        P.row("Writes: mounts now", [mounts]),
        P.row("Writes: lease", [renewals, remounts, lease_events]),
        P.row("Writes: conditional writes", [cond, requests, conflicts]),
        P.row("Writes: ref lane", [lane_wait, batching, lane_health, checkpoints, hotkey]),
        P.row("Writes: blobs and dedup", [blobs, dedup, upload_pool, fanout]),
        P.row("Writes: caches", [pfv, pfv_ratio, pfv_size, decode, cache_entries, evictions]),
        P.row("Writes: replication", [relink]),
    ]


PRICE_VARIABLES = [
    P.constant("price_put_per_1000", "0.005", "PUT/LIST price per 1000 (USD)"),
    P.constant("price_get_per_1000", "0.0004", "GET/HEAD price per 1000 (USD)"),
]

S3_CLASSES = ("Blob", "Manifest", "Root", "GC", "Meta", "Other")
VERBS = ("Put", "Get", "Head", "List", "Delete")


def _has(cls: str, verb: str) -> bool:
    return cls != "Meta" or verb in ("Put", "Delete")


def _class_verb_series(verb: str) -> dict:
    return {cls: r(f"CAS{cls}{verb}") for cls in S3_CLASSES if _has(cls, verb)}


def s3_rows() -> list:
    verbs = P.metric_ts("S3 requests per second, by verb",
                        "`S3PutObject`, `S3GetObject`, `S3HeadObject`, `S3ListObjects`, `S3DeleteObjects` (one DELETE request may carry many keys).",
                        {"PUT": r("S3PutObject"), "GET": r("S3GetObject"), "HEAD": r("S3HeadObject"), "LIST": r("S3ListObjects"), "DELETE": r("S3DeleteObjects")}, unit="reqps", stack=True, w=12)
    per_host = P.metric_ts("S3 requests per second, per host", "Sum of the five verbs per host.",
                           {"Requests": "(sum(ProfileEvent_S3PutObject) + sum(ProfileEvent_S3GetObject) + sum(ProfileEvent_S3HeadObject) + sum(ProfileEvent_S3ListObjects) + sum(ProfileEvent_S3DeleteObjects)) / $interval"},
                           unit="reqps", by_host=True, w=12)
    put_by_class = P.metric_ts("PUT per second by object class",
                               "`CAS<Class>Put`: which subsystem writes. Blob = data, Manifest = part descriptions, Root = ref-log keys, GC = collector state, Meta = `.meta` sidecars, Other = everything else.",
                               _class_verb_series("Put"), unit="reqps", stack=True)
    get_by_class = P.metric_ts("GET per second by object class", "`CAS<Class>Get` (bodies); streams are counted separately in `CAS<Class>GetStream`.", _class_verb_series("Get"), unit="reqps", stack=True)
    head_by_class = P.metric_ts("HEAD per second by object class", "`CAS<Class>Head`. Blob HEAD is the writer's HEAD-before-PUT; its misses are new blobs, not errors.", _class_verb_series("Head"), unit="reqps", stack=True)
    list_by_class = P.metric_ts("LIST per second by object class", "`CAS<Class>List`. `CASRootList` at a constant 155 per 10 minutes on otel.demo is `clearOldTemporaryDirectories` (audit F16).", _class_verb_series("List"), unit="reqps", stack=True)
    delete_by_class = P.metric_ts("DELETE per second by object class", "`CAS<Class>Delete`.", _class_verb_series("Delete"), unit="reqps", stack=True)
    gc_total = " + ".join(f"sum(ProfileEvent_CASGC{v})" for v in VERBS)
    all_total = " + ".join(f"sum(ProfileEvent_CAS{cls}{v})" for cls in S3_CLASSES for v in VERBS if _has(cls, v))
    gc_share = P.metric_ts("GC share of all CAS requests (%)", "`CASGC*` verbs over all `CAS<Class>*` verbs, per 5-minute bucket.",
                           {"GC %": f"100 * ({gc_total}) / greatest({all_total}, 1)"}, unit="percent")
    cost = P.metric_ts("Estimated cost per day (USD)",
                       "Requests × price per 1000, scaled to a day. Prices are the dashboard variables (AWS S3 Standard us-east-1 defaults); DELETE is free.",
                       {"PUT + LIST": "(sum(ProfileEvent_S3PutObject) + sum(ProfileEvent_S3ListObjects)) / 1000 * $price_put_per_1000 * 86400 / $interval",
                        "GET + HEAD": "(sum(ProfileEvent_S3GetObject) + sum(ProfileEvent_S3HeadObject)) / 1000 * $price_get_per_1000 * 86400 / $interval"},
                       unit="currencyUSD", stack=True, w=12)
    throttling = P.metric_ts("Throttling per second",
                             "`S3ReadRequestsThrottling`, `S3WriteRequestsThrottling`: 429/503 answers. 537 in a week on otel.demo, mostly at restarts; the rest coincides with GC LIST bursts (audit F26).",
                             {"Read": r("S3ReadRequestsThrottling"), "Write": r("S3WriteRequestsThrottling")}, unit="reqps", w=12)
    write_errors = P.metric_ts("S3 write request errors per second (see description)",
                               "`S3WriteRequestsErrors` mixes HTTP 412 (conditional-write collisions, benign), 409 (concurrent conditional PUTs on one key, benign), 503 (throttling) and 500. Only the last two are errors; there is no counter for them alone (CAS-299). `S3ReadRequestsErrors` is not shown: on a CAS disk it equals `CASBlobHeadMiss`.",
                               {"Write errors (all statuses)": r("S3WriteRequestsErrors"), "Connect failure hints": r("CASRequestConnectFailureHint")}, unit="reqps", w=12)
    blob_errors = P.table("Errors from blob_storage_log",
                          "`error` text per event type, grouped, over the selected range. `PreconditionFailed` (412) and `ConditionalRequestConflict` (409) are the storage's expected answers to concurrent writes of one object, not failures.",
                          f"""SELECT toString(event_type) AS "Event", error_code AS "Code", substring(error, 1, 160) AS "Error", count() AS "Count", toUInt64(max(event_time)) * 1000 AS "Last"
FROM clusterAllReplicas('$cluster', system.blob_storage_log)
WHERE disk_name IN ($disk) AND hostName() IN ($host) AND error != '' AND $timeFilter
GROUP BY "Event", "Code", "Error" ORDER BY "Count" DESC LIMIT 50
{SETTINGS}""", table="blob_storage_log", w=12)
    per_query = P.table("S3 requests per query kind",
                        "From `query_log`: average PUT / GET / HEAD / LIST per finished query, by `query_kind`. Background merges are not here (see the next table).",
                        f"""SELECT query_kind AS "Kind", count() AS "Queries",
       round(sum(ProfileEvents['S3PutObject']) / count(), 2) AS "PUT/query", round(sum(ProfileEvents['S3GetObject']) / count(), 2) AS "GET/query",
       round(sum(ProfileEvents['S3HeadObject']) / count(), 2) AS "HEAD/query", round(sum(ProfileEvents['S3ListObjects']) / count(), 2) AS "LIST/query"
FROM clusterAllReplicas('$cluster', system.query_log)
WHERE hostName() IN ($host) AND type != 'QueryStart' AND $timeFilter
GROUP BY query_kind ORDER BY "Queries" DESC
{SETTINGS}""", table="query_log", w=12)
    per_part = P.table("S3 requests per part event",
                       "From `part_log`: average per `MergeParts`, `MutatePart`, `DownloadPart`, `RemovePart`. `NewPart` rows repeat the INSERT's own ProfileEvents and are excluded; merge PUTs are counted at the S3 call site and show in the class panels, not here.",
                       f"""SELECT toString(event_type) AS "Event", count() AS "Parts",
       round(sum(ProfileEvents['S3PutObject']) / count(), 2) AS "PUT/part", round(sum(ProfileEvents['S3GetObject']) / count(), 2) AS "GET/part",
       round(sum(ProfileEvents['S3HeadObject']) / count(), 2) AS "HEAD/part", round(avg(duration_ms)) AS "avg ms"
FROM clusterAllReplicas('$cluster', system.part_log)
WHERE hostName() IN ($host) AND event_type != 'NewPart' AND $timeFilter
GROUP BY event_type ORDER BY "Parts" DESC
{SETTINGS}""", table="part_log", w=12)
    per_round = P.table("S3 requests per GC round", "From `cas_gc_log` Finish rows with outcome Success: averages per round over the range.",
                        f"""SELECT count() AS "Rounds", round(avg(ProfileEvents['S3PutObject'])) AS "PUT/round", round(avg(ProfileEvents['S3GetObject'])) AS "GET/round",
       round(avg(ProfileEvents['S3HeadObject'])) AS "HEAD/round", round(avg(ProfileEvents['S3ListObjects'])) AS "LIST/round", round(avg(ProfileEvents['S3DeleteObjects'])) AS "DELETE/round",
       round(avg(duration_ms)) AS "avg ms"
FROM clusterAllReplicas('$cluster', system.cas_gc_log)
WHERE {GC_WHERE} AND outcome = 'Success'
{SETTINGS}""", table="cas_gc_log", w=24, h=4)
    latency = P.metric_ts("Average S3 request time (ms)", "`S3ReadMicroseconds / S3ReadRequestsCount` and the write pair, per 5-minute bucket.",
                          {"Read": "sum(ProfileEvent_S3ReadMicroseconds) / greatest(sum(ProfileEvent_S3ReadRequestsCount), 1) / 1000",
                           "Write": "sum(ProfileEvent_S3WriteMicroseconds) / greatest(sum(ProfileEvent_S3WriteRequestsCount), 1) / 1000"}, unit="ms", w=12)
    connections = P.metric_ts("New S3 connections per second", "`DiskConnectionsCreated`: one new TLS connection per ~98 requests on otel.demo (audit F27, unexplored).",
                              {"Connections created": r("DiskConnectionsCreated")}, unit="ops", w=12)
    return [
        P.row("S3: by verb", [verbs, per_host]),
        P.row("S3: who makes requests", [put_by_class, get_by_class, head_by_class, list_by_class, delete_by_class, gc_share]),
        P.row("S3: cost estimate", [cost]),
        P.row("S3: errors and throttling", [throttling, write_errors, blob_errors]),
        P.row("S3: cost per unit", [per_query, per_part, per_round]),
        P.row("S3: latency", [latency, connections]),
    ]


COUNTERS_SQL = f"""SELECT name AS "Counter",
       sumIf(value, name LIKE 'ProfileEvent_%') AS "Events in range",
       round(sumIf(value, name LIKE 'ProfileEvent_%') / greatest(dateDiff('hour', min(t), max(t)), 1)) AS "Events per hour",
       maxIf(value, name LIKE 'CurrentMetric_%') AS "Gauge max in range"
FROM (
    SELECT t, tupleElement(nv, 1) AS name, tupleElement(nv, 2) AS value
    FROM (
        SELECT event_time AS t, arrayJoin(arrayZip(names, values)) AS nv
        FROM (
            SELECT event_time,
                   (SELECT groupArray(name) FROM (SELECT name FROM system.columns WHERE database = 'system' AND table = 'metric_log'
                                                  AND (name LIKE 'ProfileEvent_CAS%' OR name LIKE 'CurrentMetric_CAS%') ORDER BY position)) AS names,
                   [COLUMNS('^(ProfileEvent|CurrentMetric)_CAS') APPLY toUInt64] AS values
            FROM clusterAllReplicas('$cluster', system.metric_log)
            WHERE hostName() IN ($host) AND $timeFilter
        )
    )
)
GROUP BY name HAVING sum(value) > 0
ORDER BY "Events in range" DESC, "Gauge max in range" DESC
{SETTINGS}"""


def counters_row() -> dict:
    return P.row(COUNTERS_ROW, [P.table(
        COUNTERS_ROW,
        "Every `ProfileEvent_CAS*` (summed) and `CurrentMetric_CAS*` (max) column of `metric_log` that was above zero in the range, largest first. New counters appear here without a panel. 99 of 184 never moved in a week on otel.demo.",
        COUNTERS_SQL, table="metric_log", h=16)])


def build() -> dict:
    rows = gc_rows() + writes_rows() + s3_rows() + [counters_row()]
    return P.dashboard(
        "cas-drilldown", "CAS / Drilldown",
        "Content-addressed (CAS) storage internals for support engineers and CAS developers: garbage-collection rounds, "
        "stages and phases; the write path, ref lanes and mount leases; S3 requests by object class. Rows are collapsed; "
        "open the one the Overview pointed at.",
        rows, tags=["cas", "clickhouse"],
        links=[{"title": "CAS / Overview", "type": "link", "url": OVERVIEW_LINK["url"], "keepTime": True, "includeVars": True, "icon": "dashboard"}],
        extra_variables=PRICE_VARIABLES)
