"""End-of-run evidence of a profile run (`soak.profile`): fault and GC summary, overlap evidence, log scan.

All of it reads the servers over HTTP (`soak.cluster.Node`) and the files of the run directory, so it works
for any store the stand runs on. Each function returns the text it writes; `scan_logs` also returns the
number of UNEXPECTED hits, which count as failures of the run.
"""

import glob
import gzip
import json
import os
import re
import time
from collections import Counter, defaultdict, deque
from datetime import datetime

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None


def _rows(node, sql, out):
    try:
        r = node.query(sql + " FORMAT JSONEachRow", timeout=300,
                       settings={"output_format_json_quote_64bit_integers": "0"})
        return [json.loads(line) for line in r.splitlines() if line.strip()]
    except Exception as e:
        out.append("  query on %r failed: %s" % (node, str(e)[:200].replace("\n", " ")))
        return None


def _hms(ts):
    return time.strftime("%H:%M:%S", time.localtime(ts))


def fault_summary(run_dir, nodes, journal_path):
    """Fault list with recovery, GC rounds by outcome per server (leadership changes, rounds suppressed with
    `parent_unconfirmed`, failed rounds), pending + standing rows over time, rounds around each freeze, and the
    intents curve."""
    out = []
    faults = []
    if os.path.exists(journal_path):
        faults = [json.loads(line) for line in open(journal_path) if line.strip()]
    out.append("== faults (%s)" % os.path.basename(journal_path))
    c = Counter((f["kind"], f["server"]) for f in faults)
    for (kind, srv), n in sorted(c.items()):
        out.append("  %-12s server %d: %d" % (kind, srv, n))
    out.append("  total %d (one line per affected server), not recovered %d" % (len(faults), sum(1 for f in faults if not f["recovered"])))
    for f in faults:
        out.append("  %s %s %s server %d (%s, leader before: %s) %s..%s  held %.0f s, served a read %.1f s after resume%s" % (
            "OK  " if f["recovered"] else "FAIL", f["kind"], f["target"], f["server"], f["why"], f["leader_before"],
            _hms(f["start"]), _hms(f["end"]), f["duration_s"], f["recover_s"], "" if f["recovered"] else "  NOT RECOVERED"))

    rounds = {}
    for srv, node in nodes.items():
        fin = _rows(node, "SELECT toFloat64(toUnixTimestamp64Micro(event_time_microseconds)) / 1e6 AS ts, toString(outcome) AS outcome, "
                          "toString(trigger) AS trigger, round, round_id, error_code, substring(error, 1, 200) AS error, objects_deleted, "
                          "entries_bodies_deleted, candidates_marked, fence_outs, duration_ms, "
                          "ProfileEvents['CASGCClampSuppressedPasses'] AS suppressed "
                          "FROM system.cas_gc_log WHERE event_type = 'Finish' ORDER BY event_time_microseconds", out)
        ph = _rows(node, "SELECT round_id, phase, phase_metrics FROM system.cas_gc_log WHERE event_type = 'Phase' "
                         "AND phase IN ('fold_reduce', 'pending_deletes')", out)
        rounds[srv] = (fin, ph or [])

    out.append("")
    out.append("== GC rounds per server (system.cas_gc_log, Finish rows; both triggers)")
    for srv in nodes:
        fin, ph = rounds[srv]
        if fin is None:
            continue
        by = Counter(r["outcome"] for r in fin)
        led = [r for r in fin if r["outcome"] != "NotALeader"]
        out.append("server %d: rounds=%d led=%d by outcome: %s" % (srv, len(fin), len(led), dict(sorted(by.items()))))
        acq = lost = 0
        prev = None
        for r in fin:
            cur = r["outcome"] != "NotALeader"
            if prev is False and cur:
                acq += 1
            if prev is True and not cur:
                lost += 1
            prev = cur
        lease_aborts = [r for r in fin if r["outcome"] == "Aborted" and re.search(r"lease|leader|moved during the round", r["error"], re.I)]
        out.append("  lease acquired after a NotALeader round (takeover or first lead after a restart): %d" % acq)
        out.append("  led round followed by NotALeader (leadership lost): %d; Aborted rounds whose error names the lease, "
                   "a leader or a moved gc/state: %d" % (lost, len(lease_aborts)))
        pu = [m for m in ph if m["phase"] == "fold_reduce" and int(m["phase_metrics"].get("parent_unconfirmed", 0)) == 1]
        out.append("  rounds suppressed with parent_unconfirmed=1 (fold_reduce row): %d; CASGCClampSuppressedPasses summed over rounds: %d" % (
            len(pu), sum(int(r["suppressed"]) for r in fin)))
        out.append("  heartbeat fence-outs by this server's rounds: %d" % sum(int(r["fence_outs"]) for r in fin))
        failed = Counter((r["outcome"], r["error_code"]) for r in fin if r["outcome"] in ("Aborted", "Error", "Stopped"))
        out.append("  rounds that ended failed, by (outcome, error_code): %s" % (dict(sorted(failed.items())) or "none"))
        for (o, code), n in sorted(failed.items()):
            ex = next(r for r in fin if (r["outcome"], r["error_code"]) == (o, code))
            out.append("    %s code %s x%d, e.g. round %s: %s" % (o, code, n, ex["round"], ex["error"].replace("\n", " ")[:160]))

    out.append("")
    out.append("== pending + standing rows over time (fold_reduce phase_metrics['pending_rows'], per 5 minutes)")
    for srv in nodes:
        fin, ph = rounds[srv]
        if fin is None:
            continue
        ts = {r["round_id"]: r["ts"] for r in fin}
        b = defaultdict(list)
        for m in ph:
            if m["phase"] == "fold_reduce" and m["round_id"] in ts:
                b[int(ts[m["round_id"]] // 300)].append((int(m["phase_metrics"].get("pending_rows", 0)), int(m["phase_metrics"].get("condemned_rows", 0))))
        for k in sorted(b):
            v = b[k]
            out.append("  server %d %s  rounds_with_fold=%d pending_rows avg=%.0f max=%d  condemned_rows max=%d" % (
                srv, _hms(k * 300), len(v), sum(x for x, _ in v) / len(v), max(x for x, _ in v), max(y for _, y in v)))

    out.append("")
    out.append("== around each long freeze (Finish rows of both servers)")
    for f in [f for f in faults if f["kind"] == "freeze_long"]:
        s, e, fs = f["start"], f["end"], f["server"]
        other = 3 - fs
        out.append("freeze of server %d (%s) %s..%s" % (fs, f["why"], _hms(s), _hms(e)))
        fo = rounds.get(other, (None, []))[0] or []
        took = [r for r in fo if s <= r["ts"] <= e + 5 and r["outcome"] != "NotALeader"]
        out.append("  server %d led %d round(s) while server %d was stopped; first led Finish %s s after the stop: %s" % (
            other, len(took), fs, ("%.0f" % (took[0]["ts"] - s)) if took else "-",
            ("round %s %s" % (took[0]["round"], took[0]["outcome"])) if took else "NO TAKEOVER"))
        pre = [r for r in fo if s - 60 <= r["ts"] < s and r["outcome"] != "NotALeader"]
        out.append("  server %d led %d round(s) in the 60 s before the stop (it was %s)" % (other, len(pre), "the leader already" if pre else "a follower"))
        ff, fp = rounds.get(fs, (None, []))
        mt = {m["round_id"]: m for m in fp if m["phase"] == "fold_reduce"}
        pd = {m["round_id"]: m for m in fp if m["phase"] == "pending_deletes"}
        after = [r for r in (ff or []) if r["ts"] >= e][:8]
        out.append("  server %d: first Finish rows after the resume (parent_unconfirmed, body deletes by this round):" % fs)
        for r in after:
            red = mt.get(r["round_id"], {}).get("phase_metrics", {})
            dl = pd.get(r["round_id"], {}).get("phase_metrics", {})
            out.append("    +%5.1f s round=%s trigger=%s outcome=%s error_code=%s parent_unconfirmed=%s objects_deleted=%s "
                       "entries_bodies_deleted=%s pending_deletes.bodies_deleted=%s duration_ms=%s" % (
                           r["ts"] - e, r["round"], r["trigger"], r["outcome"], r["error_code"], red.get("parent_unconfirmed", "-"),
                           r["objects_deleted"], r["entries_bodies_deleted"], dl.get("bodies_deleted", "-"), r["duration_ms"]))
        de = _rows(nodes[fs], "SELECT countIf(t < %f) AS before_stop, countIf(t >= %f AND t < %f) AS while_stopped, countIf(t >= %f) AS after_resume "
                              "FROM (SELECT toFloat64(toUnixTimestamp64Micro(event_time_microseconds)) / 1e6 AS t FROM system.cas_log "
                              "WHERE event_type = 'blob_delete') WHERE t BETWEEN %f AND %f" % (s, s, e, e, s - 15, e + 20), out)
        if de:
            out.append("  server %d blob_delete events (cas_log): %s in the 15 s before the stop, %s while stopped, %s in the 20 s after the resume" % (
                fs, de[0]["before_stop"], de[0]["while_stopped"], de[0]["after_resume"]))
        bad = [r for r in after if r["outcome"] == "Success" and (int(r["objects_deleted"]) or int(r["entries_bodies_deleted"]))]
        out.append("  server %d led rounds with body deletes among those: %d" % (fs, len(bad)))

    out.append("")
    out.append("== intents curve (intents_curve.jsonl, one online fsck per server per minute)")
    curve = os.path.join(run_dir, "intents_curve.jsonl")
    rows = [json.loads(line) for line in open(curve)] if os.path.exists(curve) else []
    out.append("  samples: %d; max intents_listed=%s; max dangling=%s" % (
        len(rows), max((r["intents_listed"] for r in rows), default="-"), max((r["dangling"] for r in rows), default="-")))
    for r in rows:
        out.append("  %s server %d intents_listed=%s pending_rows=%s condemned_rows=%s intents_minus_pending_rows=%s dangling=%s "
                   "reachable=%s unreachable=%s (gc row age %s s)" % (
                       r["t"], r["server"], r["intents_listed"], r["gc_pending_rows"], r["gc_condemned_rows"],
                       (r["intents_listed"] - r["gc_pending_rows"]) if r["gc_pending_rows"] is not None else None,
                       r["dangling"], r["reachable"], r["unreachable"], r["gc_row_age_s"]))

    out.append("")
    out.append("== server logs: parent_unconfirmed")
    for srv in nodes:
        n = 0
        first = []
        for fn in sorted(glob.glob(os.path.join(run_dir, "srv%d" % srv, "clickhouse-server.log*"))):
            if fn.endswith(".gz"):
                continue
            with open(fn, errors="replace") as fh:
                for line in fh:
                    if "parent_unconfirmed" in line:
                        n += 1
                        if len(first) < 3:
                            first.append(line.strip()[:300])
        out.append("  server %d: %d line(s)" % (srv, n))
        out.extend("    " + x for x in first)
    return "\n".join(out) + "\n"


OVERLAP_EVENTS = ["CASBlobReuseIntentPresent", "CASBlobBodyPutAvoided", "CASBlobReuseTokenRejected", "CASGCRetiredSparedByReref",
                  "CASGCIntentLeftOnSpare", "CASGCIntentCreated", "CASGCIntentConfirmedExisting", "CASGCRetiredIntentConfirmed",
                  "CASGCRetiredBodyDeleted", "CASGCIntentDeleted", "CASGCIntentAdopted", "CASGCRetireReplaced", "CASGCIntentStandingAnomaly",
                  "CASGCRetiredBodyDeleteFailed", "CASGCIntentUnconfirmedCarry"]


def overlap_summary(nodes, counts):
    """How often blobs were in a GC step while a writer needed the same content: `ProfileEvents` summed over
    `system.metric_log` (every life of the server), `cas_log` event counts, and per writer adoption the last
    lifecycle event of the blob before it (`cas_log` of both servers, within the janitor's retention)."""
    out = []
    total = Counter()
    events = []
    for srv, node in nodes.items():
        cols = ", ".join("sum(ProfileEvent_%s) AS %s" % (e, e) for e in OVERLAP_EVENTS)
        r = _rows(node, "SELECT %s FROM system.metric_log" % cols, out)
        out.append("== server %d (metric_log sums over all lives of the server)" % srv)
        if r:
            for e in OVERLAP_EVENTS:
                total[e] += int(r[0][e])
                out.append("  %-34s %s" % (e, r[0][e]))
        out.append("  -- cas_log event counts --")
        for x in _rows(node, "SELECT event_type, toString(outcome) AS outcome, count() AS n FROM system.cas_log WHERE event_type IN "
                             "('blob_put','blob_reuse_adopt','blob_reuse_resurrect','blob_retire','blob_delete','blob_intent_adopt',"
                             "'blob_intent_delete','indegree_zero') GROUP BY ALL ORDER BY 1, 2", out) or []:
            out.append("  %s %s %s" % (x["event_type"], x["outcome"], x["n"]))
        out.append("  -- cas_gc_log: rounds by outcome --")
        for x in _rows(node, "SELECT toString(trigger) AS trigger, toString(outcome) AS outcome, count() AS n FROM system.cas_gc_log "
                             "WHERE event_type = 'Finish' GROUP BY ALL ORDER BY 1, 2", out) or []:
            out.append("  %s %s %s" % (x["trigger"], x["outcome"], x["n"]))
        for x in _rows(node, "SELECT sum(entries_bodies_deleted) AS bodies_deleted, sum(entries_intent_confirmed) AS intent_confirmed, "
                             "sum(entries_condemned) AS condemned, sum(objects_spared) AS spared FROM system.cas_gc_log "
                             "WHERE event_type = 'Finish' AND outcome = 'Success'", out) or []:
            out.append("  cas_gc_log Finish sums (Success rounds only): %s" % x)
        events += _rows(node, "SELECT toUnixTimestamp64Micro(event_time_microseconds) AS ts, event_type AS ev, object_hash AS h "
                              "FROM system.cas_log WHERE event_type IN ('blob_reuse_adopt','blob_reuse_resurrect','indegree_zero',"
                              "'blob_intent_adopt','blob_delete') AND object_hash != ''", out) or []
    out.append("")
    out.append("== TOTAL over both servers")
    for e in OVERLAP_EVENTS:
        out.append("  %-34s %s" % (e, total[e]))
    out.append("")
    out.append("== writer adoptions by the last lifecycle event of the blob before the adoption")
    life = defaultdict(list)
    for x in events:
        if x["ev"] in ("indegree_zero", "blob_delete", "blob_reuse_resurrect"):
            life[x["h"]].append((int(x["ts"]), x["ev"]))
    for v in life.values():
        v.sort()
    by = Counter()
    for x in events:
        if x["ev"] != "blob_reuse_adopt":
            continue
        prev = [ev for ts, ev in life.get(x["h"], []) if ts <= int(x["ts"])]
        by[prev[-1] if prev else "none in window"] += 1
    for k, n in by.most_common():
        out.append("  %-24s %d" % (k, n))
    out.append("  (indegree_zero: the writer adopted a body GC had seen at in-degree 0 and had not yet deleted;"
               " blob_delete / blob_reuse_resurrect: a later incarnation adopted again before GC saw it)")
    out.append("")
    out.append("== driver-side evidence (intents workload counters)")
    for k in ("inserts", "boomerang_inserts", "inserts_of_unreferenced_content", "inserts_of_unreferenced_content_gap_lt_30s",
              "inserts_of_unreferenced_content_gap_lt_10s", "drops", "truncates", "drop_partitions", "attach_from", "attach_from_refused",
              "move_partition", "move_partition_refused", "clone_as", "clone_as_refused", "copy_gave_up", "copy_uncertain", "copy_dst_reads"):
        out.append("  %-48s %s" % (k, counts.get(k)))
    short = " ".join("%s=%s" % (e, total[e]) for e in ("CASBlobReuseIntentPresent", "CASBlobBodyPutAvoided", "CASGCRetiredSparedByReref",
                                                       "CASGCRetiredBodyDeleted", "CASGCIntentDeleted", "CASGCIntentAdopted"))
    return "\n".join(out) + "\n", short


LOG_HARD = re.compile(r"LOGICAL_ERROR|Logical error|Segmentation|\bFatal\b|Sanitizer|AddressSanitizer|SIGSEGV|SIGABRT")
LOG_SOFT = re.compile(r"CORRUPTED_DATA|dangling", re.I)
LOG_ANY = re.compile(LOG_HARD.pattern + "|" + LOG_SOFT.pattern, re.I)
FAULT_LINE = re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d) \[\w+\] (FAULT-START|FAULT-END) server (\d)")


def fault_windows(run_log):
    """Per server: [(from, to)] epochs, from a fault's start - 1 s to its end + 30 s; a fault without an end stays open."""
    windows = {1: [], 2: []}
    started = {}
    for line in open(run_log, errors="replace"):
        m = FAULT_LINE.match(line)
        if not m:
            continue
        t = time.mktime(time.strptime(m.group(1), "%Y-%m-%d %H:%M:%S"))
        n = int(m.group(3))
        if m.group(2) == "FAULT-START":
            started[n] = t
        elif n in started:
            windows[n].append((started.pop(n) - 1, t + 30))
    for n, t in started.items():
        windows[n].append((t - 1, float("inf")))
    return windows


def scan_logs(run_dir, run_log, server_tz, allow_file):
    """Scans both servers' logs for hard-failure patterns. UNEXPECTED counts as a failure; NOISE is a soft pattern
    (`CORRUPTED_DATA`, `dangling`) inside a fault window of that server; ALLOWED matches `allow_file`.
    Returns (text, unexpected)."""
    allow = []
    if os.path.exists(allow_file):
        for line in open(allow_file):
            if line.startswith("#") or not line.strip():
                continue
            rx, _, why = line.rstrip("\n").partition("\t")
            allow.append((re.compile(rx), why))
    windows = fault_windows(run_log)
    tz = ZoneInfo(server_tz) if (ZoneInfo and server_tz) else None
    out = []
    seen = set()
    counts = {"UNEXPECTED": 0, "NOISE": 0, "ALLOWED": 0}
    total_lines = 0

    def classify(n, line):
        key = (n, line[:120])   # the same event is in clickhouse-server.log and .err.log
        if key in seen:
            return None
        seen.add(key)
        ts = None
        m = re.match(r"(\d{4})\.(\d\d)\.(\d\d) (\d\d:\d\d:\d\d)", line)
        if m:
            dt = datetime.strptime("%s-%s-%s %s" % m.groups(), "%Y-%m-%d %H:%M:%S")
            ts = dt.replace(tzinfo=tz).timestamp() if tz else dt.timestamp()
        for rx, why in allow:
            if rx.search(line):
                counts["ALLOWED"] += 1
                return "ALLOWED (%s)" % why
        if LOG_HARD.search(line) or ts is None or not any(a <= ts <= b for a, b in windows[n]):
            counts["UNEXPECTED"] += 1
            return "UNEXPECTED"
        counts["NOISE"] += 1
        return "NOISE (inside a fault window of the server)"

    for n in (1, 2):
        files = sorted(glob.glob(os.path.join(run_dir, "srv%d" % n, "clickhouse-server*.log*")), key=lambda f: ("err" in f, f))
        files += sorted(glob.glob(os.path.join(run_dir, "srv%d" % n, "docker.log")))
        for fn in files:
            opener = gzip.open if fn.endswith(".gz") else open
            before = deque(maxlen=2)     # streamed: a rotated trace log is hundreds of MB
            after = 0
            with opener(fn, "rt", errors="replace") as f:
                for i, raw in enumerate(f):
                    line = raw.rstrip("\n")
                    total_lines += 1
                    cls = classify(n, line) if LOG_ANY.search(line) else None
                    if cls is not None:
                        out.append("%s server=%d %s:%d" % (cls, n, os.path.basename(fn), i + 1))
                        out.extend("     " + b[:400] for b in before)
                        out.append("    >" + line[:400])
                        after = 2
                    elif after:
                        out.append("     " + line[:400])
                        after -= 1
                    before.append(line)
    out.append("TOTAL scanned_lines=%d unexpected=%d noise=%d allowed=%d" % (total_lines, counts["UNEXPECTED"], counts["NOISE"], counts["ALLOWED"]))
    return "\n".join(out) + "\n", counts["UNEXPECTED"]
