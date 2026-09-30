"""CAS / Overview: six status tiles and four rows, in plain language."""
from . import panels as P
from .panels import SETTINGS, DRILLDOWN_LINK, guarded, c

INTEGRITY_EVENTS = "('read_missing', 'dangling_access', 'corrupt_dangle', 'corrupt_decode', 'snap_journal_incoherent', 'exception')"
METRICS_USED = set()  # filled by _m() so tests can hand the checker exactly the names this module uses


def _m(name: str) -> str:
    METRICS_USED.add(name)
    return name


def _metric_sql(select: str) -> str:
    return f"""SELECT {select}
FROM clusterAllReplicas('$cluster', system.metric_log)
WHERE hostName() IN ($host) AND $timeFilter
{SETTINGS}"""


def tile_storage() -> dict:
    a = f"""SELECT
    (SELECT count() FROM system.clusters WHERE cluster = '$cluster') - uniqExact(host) AS "Hosts not reporting",
    countIf(lifecycle != 'live') AS "Disks not live"
FROM (SELECT hostName() AS host, disk, any(lifecycle) AS lifecycle
      FROM clusterAllReplicas('$cluster', system.cas_mounts)
      WHERE disk IN ($disk)
      GROUP BY host, disk)
{SETTINGS}"""
    b = _metric_sql(f"""{guarded(c(_m('CASMountLeaseLost')))} AS "Lease lost",
    dateDiff('second', max(event_time), now()) AS "Metrics age (s)\"""")
    return P.stat(
        "Storage online",
        "Every host can read and write the shared storage right now. "
        "Hosts not reporting: hosts in the cluster that did not answer. Disks not live: storage disks that lost their "
        "connection to the pool. Lease lost: how many times a host lost its write permission in the selected period. "
        "Metrics age: seconds since the last metrics row; when the storage is down the server stops writing its own logs, "
        "so a growing age is itself a warning. What to do: check the host and its object storage endpoint; "
        "see Drilldown → Writes: lease and Writes: mounts now.",
        [P.target(a, fmt="table", table="cas_mounts", ref="A"), P.target(b, fmt="table", ref="B")],
        {"Hosts not reporting": {"steps": [("green", None), ("red", 1)]},
         "Disks not live": {"steps": [("green", None), ("red", 1)]},
         "Lease lost": {"steps": [("green", None), ("red", 1)]},
         "Metrics age (s)": {"steps": [("green", None), ("yellow", 120), ("red", 600)], "unit": "s"}},
        links=[DRILLDOWN_LINK], interval="5m", h=7)


def tile_writes() -> dict:
    sql = _metric_sql(f"""{guarded(c(_m('CASRequestGaveUp')))} AS "Gave up",
    if(sum(ProfileEvent_{_m('CASConditionalWriteAttempts')}) = 0, NULL,
       round(100 * sum(ProfileEvent_{_m('CASConditionalWriteUnresolved')}) / sum(ProfileEvent_CASConditionalWriteAttempts), 3)) AS "Unresolved %\"""")
    return P.stat(
        "Writes safe",
        "Writes to the shared storage are confirmed. Gave up: writes abandoned after all retries (any value needs a look). "
        "Unresolved %: share of writes whose answer from the storage was lost and had to be re-checked by reading back; "
        "0.03% is normal on a healthy backend, a rise means storage timeouts. What to do: check object-storage latency and "
        "errors; see Drilldown → Writes: conditional writes and S3: errors and throttling.",
        [P.target(sql, fmt="table")],
        {"Gave up": {"steps": [("green", None), ("red", 1)]},
         "Unresolved %": {"steps": [("green", None), ("yellow", 0.1)], "unit": "percent"}},
        links=[DRILLDOWN_LINK], h=7)


def tile_gc() -> dict:
    a = f"""SELECT if(count() = 0, NULL, dateDiff('minute', max(event_time), now())) AS "Minutes since last success" /* now */
FROM clusterAllReplicas('$cluster', system.cas_gc_log)
WHERE disk_name IN ($disk) AND event_type = 'Finish' AND outcome IN ('Success', 'Deferred') AND event_date >= today() - 7
{SETTINGS}"""
    b = f"""SELECT countIf(outcome IN ('Error', 'Aborted', 'Stopped')) AS "Failed rounds"
FROM clusterAllReplicas('$cluster', system.cas_gc_log)
WHERE disk_name IN ($disk) AND event_type = 'Finish' AND $timeFilter
{SETTINGS}"""
    return P.stat(
        "GC: last success",
        "Garbage collection frees storage that deleted or merged data no longer needs. Rounds normally finish every 5–10 "
        "minutes. Minutes since last success: time since the last completed round anywhere in the cluster; yellow after "
        "30 minutes, red after 2 hours. Failed rounds: rounds in the selected period that ended with an error or were "
        "aborted. What to do: open Drilldown → GC: rounds for the error text; a long-running round is normal after a large "
        "deletion, a stopped one is not.",
        [P.target(a, fmt="table", table="cas_gc_log", ref="A"), P.target(b, fmt="table", table="cas_gc_log", ref="B")],
        {"Minutes since last success": {"steps": [("green", None), ("yellow", 30), ("red", 120)], "unit": "m"},
         "Failed rounds": {"steps": [("green", None), ("red", 1)]}},
        links=[DRILLDOWN_LINK], h=7)


def tile_garbage() -> dict:
    a = f"""SELECT if(count() = 0, NULL, round(100 * sum(net) / greatest(sum(cond), 1), 1)) AS "Backlog growth %, last 10 rounds",
       if(count() = 0, NULL, sum(net)) AS "Net objects, last 10 rounds" /* now */
FROM (SELECT toInt64(entries_condemned) - toInt64(entries_redeleted) AS net, toInt64(entries_condemned) AS cond
      FROM clusterAllReplicas('$cluster', system.cas_gc_log)
      WHERE disk_name IN ($disk) AND event_type = 'Finish' AND outcome = 'Success' AND event_date >= today() - 7
      ORDER BY event_time DESC LIMIT 10)
{SETTINGS}"""
    return P.stat(
        "Garbage trend",
        "Is garbage collection keeping up with deletions? Each round marks objects for deletion and deletes objects marked "
        "earlier. Backlog growth: over the last 10 rounds, the share of newly marked objects that were not yet deleted; "
        "near zero is steady state, it rises for about an hour after a server restart and recovers by itself. Yellow above "
        "25%, red above 50%. Net objects: marked minus deleted over those rounds. What to do: if red for more than two "
        "hours, open Drilldown → GC: stage flow and GC: round cost.",
        [P.target(a, fmt="table", table="cas_gc_log")],
        {"Backlog growth %, last 10 rounds": {"steps": [("green", None), ("yellow", 25), ("red", 50)], "unit": "percent"},
         "Net objects, last 10 rounds": {"fixed": "blue"}},
        links=[DRILLDOWN_LINK], h=7)


def tile_integrity() -> dict:
    a = f"""SELECT count() AS "Integrity events"
FROM clusterAllReplicas('$cluster', system.cas_log)
WHERE disk_name IN ($disk) AND event_type IN {INTEGRITY_EVENTS} AND $timeFilter
{SETTINGS}"""
    b = f"""SELECT if(count() = 0, NULL, sum(anomalies)) AS "GC anomalies"
FROM clusterAllReplicas('$cluster', system.cas_gc_log)
WHERE disk_name IN ($disk) AND event_type = 'Finish' AND $timeFilter
{SETTINGS}"""
    cc = _metric_sql(f"""{guarded(c(_m('CASRefNeedsRecovery')))} AS "Lanes needing recovery\"""")
    d = f"""SELECT max(v) AS "Wedged lanes"
FROM (SELECT hostName() AS host, argMax(value, event_time) AS v
      FROM clusterAllReplicas('$cluster', system.asynchronous_metric_log)
      WHERE hostName() IN ($host) AND metric LIKE 'CASGCWedgedNamespaces_%'
        AND replaceOne(metric, 'CASGCWedgedNamespaces_', '') IN ($disk) AND $timeFilter
      GROUP BY host)
{SETTINGS}"""
    return P.stat(
        "Integrity",
        "Signals that must stay at zero. Integrity events: the storage found data that should exist but does not, or "
        "could not be decoded. GC anomalies: garbage collection hit an inconsistency and worked around it; a value that "
        "stays above zero across rounds deserves a look. Wedged lanes and Lanes needing recovery: a table's write queue to "
        "the shared storage is stuck. What to do: any red value is a support case; keep the selected time range and open "
        "Drilldown → Needs attention and GC: anomalies.",
        [P.target(a, fmt="table", table="cas_log", ref="A"), P.target(b, fmt="table", table="cas_gc_log", ref="B"),
         P.target(cc, fmt="table", ref="C"), P.target(d, fmt="table", table="asynchronous_metric_log", ref="D")],
        {"Integrity events": {"steps": [("green", None), ("red", 1)]},
         "GC anomalies": {"steps": [("green", None), ("yellow", 1)]},
         "Lanes needing recovery": {"steps": [("green", None), ("red", 1)]},
         "Wedged lanes": {"steps": [("green", None), ("red", 1)]}},
        links=[DRILLDOWN_LINK], h=7)


def tile_s3() -> dict:
    a = _metric_sql(f"""if(count() = 0, NULL,
       round(60 * (sum(ProfileEvent_{_m('S3ReadRequestsThrottling')}) + sum(ProfileEvent_{_m('S3WriteRequestsThrottling')}))
             / greatest(dateDiff('second', min(event_time), max(event_time)), 1), 2)) AS "Throttling per minute\"""")
    b = f"""SELECT round(countIf(error != '' AND error NOT LIKE '%PreconditionFailed%' AND error NOT LIKE '%ConditionalRequestConflict%')
             / greatest(($__to - $__from) / 3600000, 1), 1) AS "S3 errors per hour"
FROM clusterAllReplicas('$cluster', system.blob_storage_log)
WHERE disk_name IN ($disk) AND $timeFilter
{SETTINGS}"""
    return P.stat(
        "S3 throttling",
        "The object storage is answering at full speed. Throttling per minute: requests the storage slowed down "
        "(HTTP 429/503) per minute over the selected period; zero is normal, bursts follow a server restart. S3 errors: "
        "failed uploads or deletes per hour, not counting the storage's expected answers to concurrent writes of the same "
        "object (HTTP 412 and 409); a few per day are normal and retried, yellow above 5 per hour, red above 50. What to do: sustained throttling means the request rate is above "
        "the bucket's limit; see Drilldown → S3: by verb and S3: errors and throttling.",
        [P.target(a, fmt="table", ref="A"), P.target(b, fmt="table", table="blob_storage_log", ref="B")],
        {"Throttling per minute": {"steps": [("green", None), ("yellow", 0.01), ("red", 10)]},
         "S3 errors per hour": {"steps": [("green", None), ("yellow", 5), ("red", 50)]}},
        links=[DRILLDOWN_LINK], h=7)


def tiles() -> list:
    return [tile_storage(), tile_writes(), tile_gc(), tile_garbage(), tile_integrity(), tile_s3()]


def rows() -> list:
    return []


def build() -> dict:
    return P.dashboard(
        "cas-overview", "CAS / Overview",
        "Health of content-addressed (CAS) storage for people who run the cluster: is storage online, are writes safe, "
        "is garbage collection keeping up, is anything wrong. Details for support engineers are on CAS / Drilldown.",
        tiles() + rows(), tags=["cas", "clickhouse"],
        links=[{"title": "CAS / Drilldown", "type": "link", "url": DRILLDOWN_LINK["url"], "keepTime": True, "includeVars": True, "icon": "dashboard"},
               {"title": "Operations: monitoring", "type": "link", "url": "https://docs.altinity.com/antalya/cas/operations/monitoring", "targetBlank": True, "icon": "doc"}])
