"""Intent-churn workload and its oracle: the `intents` part of a profile run (`soak.profile`).

Aimed at the blob deletion-intent protocol. A fixed set of contents (`--contents`, 40) is inserted again
and again into short-lived MergeTree tables on both servers and dropped, so the same blobs are condemned
and referenced again all the time, while every server runs `SYSTEM CAS GC RUN` back to back:

- writers: create a table, insert 1-4 contents (half of them from 8 "hot" contents), read back, hold,
  end it with `DROP TABLE`, `TRUNCATE` or `DROP PARTITION`; 60 % of the dropped contents come back after
  0-25 s ("boomerang" inserts that land while GC holds the old blob in some step);
- 35 % of the tables end in a copy instead: `ATTACH PARTITION FROM`, `MOVE PARTITION TO TABLE` or
  `CREATE TABLE ... CLONE AS` into a new table, the source dropped at once, the copy read back and dropped;
- verifiers re-read random live tables and run `CHECK TABLE` on some;
- a sampler runs `SYSTEM CAS FSCK` on every server once a minute.

Oracle (each violation is a line in `failures.jsonl` and fails the run): a read-back that is not one of the
table's possible states (several are possible only after an operation of unknown outcome), any query error
outside a fault's noise window, a `CHECK TABLE` that is not 1, and an fsck hard finding (`HARD_FSCK`) that
repeats 5 s later. A refused copy (code 234) is retried and counted, not a failure.

Module state is per process: one profile run per process.
"""

import json
import os
import random
import re
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from soak import metrics as metrics_mod

M64 = (1 << 64) - 1
HARD_FSCK = ["dangling", "chain_broken", "corrupted_runs", "lifeless_keys", "intent_malformed", "intent_unread",
             "pending_without_intent"]

RUN = None
DISK = "ca"
POLICY = "ca"
NODES = {}          # server number -> (host, http port)
NOISE = None
GATE = None
DB_PATH = None
STOP = threading.Event()
LOGLOCK = threading.Lock()
LOG = print
CNT = {"inserts": 0, "reads": 0, "checks": 0, "drops": 0, "truncates": 0, "drop_partitions": 0, "tables": 0, "fsck_online": 0,
       "gc_calls": 0, "gc_led": 0, "gc_folded": 0, "noise": 0, "failures": 0, "uncertain_ops": 0, "boomerang_inserts": 0,
       "inserts_of_unreferenced_content": 0, "inserts_of_unreferenced_content_gap_lt_30s": 0, "inserts_of_unreferenced_content_gap_lt_10s": 0,
       "attach_from": 0, "attach_from_refused": 0, "move_partition": 0, "move_partition_refused": 0,
       "clone_as": 0, "clone_as_refused": 0, "copy_gave_up": 0, "copy_uncertain": 0, "copy_dst_reads": 0}
CLOCK = threading.Lock()
_TLS = threading.local()


def inc(k, n=1):
    with CLOCK:
        CNT[k] += n


def log(msg):
    LOG(f"[intents] {msg}")


def fail(kind, srv, table, query, err, extra=None):
    inc("failures")
    rec = {"ts": time.strftime("%F %T"), "kind": kind, "server": srv, "table": table, "query": query, "error": str(err)[:3000], "extra": extra}
    with LOGLOCK:
        with open(os.path.join(RUN, "failures.jsonl"), "a") as f:
            f.write(json.dumps(rec) + "\n")
    log("FAILURE kind=%s server=%s table=%s err=%s" % (kind, srv, table, str(err)[:300].replace("\n", " ")))


class NoiseWindows:
    """Per server: until when its errors are expected (a fault is in progress or just ended)."""
    FOREVER = 9999999999

    def __init__(self):
        self._until = {1: 0.0, 2: 0.0}
        self._lock = threading.Lock()

    def open(self, servers):
        with self._lock:
            for n in servers:
                self._until[n] = self.FOREVER

    def close(self, servers, tail_s):
        with self._lock:
            for n in servers:
                self._until[n] = time.time() + tail_s

    def active(self, srv):
        with self._lock:
            return time.time() < self._until[srv]


class Gate:
    """Lets a main-profile checkpoint pause the writers and verifiers: `pause` waits until no gated
    statement is in flight, and new gated statements wait until `resume`."""

    def __init__(self):
        self._cv = threading.Condition()
        self._paused = False
        self._inflight = 0

    def enter(self):
        with self._cv:
            while self._paused and not STOP.is_set():
                self._cv.wait(1)
            self._inflight += 1

    def exit(self):
        with self._cv:
            self._inflight -= 1
            self._cv.notify_all()

    def pause(self, limit_s=900):
        with self._cv:
            self._paused = True
            t0 = time.time()
            while self._inflight > 0 and time.time() - t0 < limit_s:
                self._cv.wait(1)
            return self._inflight

    def resume(self):
        with self._cv:
            self._paused = False
            self._cv.notify_all()


class ConnErr(Exception):
    pass


class QueryErr(Exception):
    def __init__(self, code, msg):
        super().__init__("Code %s: %s" % (code, msg))
        self.code = code
        self.msg = msg


def in_noise(srv):
    return NOISE.active(srv)


def q(srv, sql, settings=None, timeout=180):
    gated = getattr(_TLS, "gated", False)
    if gated:
        GATE.enter()
    try:
        return _q(srv, sql, settings, timeout)
    finally:
        if gated:
            GATE.exit()


def _q(srv, sql, settings, timeout):
    params = {"wait_end_of_query": "1"}
    if settings:
        params.update(settings)
    host, port = NODES[srv]
    url = "http://%s:%d/?%s" % (host, port, urllib.parse.urlencode(params))
    req = urllib.request.Request(url, data=sql.encode(), method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode()
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        m = re.search(r"Code: (\d+)", body)
        raise QueryErr(int(m.group(1)) if m else -1, body.strip()[:2000])
    except (urllib.error.URLError, ConnectionError, TimeoutError, OSError) as e:
        raise ConnErr(str(e))
    except Exception as e:  # http.client.RemoteDisconnected, BadStatusLine, IncompleteRead
        raise ConnErr("%s: %s" % (type(e).__name__, e))


def wait_up(srv, limit=400):
    t0 = time.time()
    while time.time() - t0 < limit and not STOP.is_set():
        try:
            _q(srv, "SELECT 1", None, 10)
            return True
        except (ConnErr, QueryErr):
            time.sleep(1)
    return False


# ---- contents -------------------------------------------------------------------------------------------------

COLUMNS = "cid UInt16, n UInt64, c1 UInt64, c2 String, c3 UInt32, c4 Float64, c5 Array(UInt16)"


def rows_of(c):
    return 4000 + (c * 7919) % 8000


def gen_sql(c):
    return ("SELECT toUInt16(%d) AS cid, number AS n, cityHash64(number, %d, 1) AS c1, hex(cityHash64(number, %d, 2)) AS c2, "
            "toUInt32(number %% 1000 + %d) AS c3, toFloat64(number) / 7 + %d AS c4, [toUInt16(number %% 65535), toUInt16(%d)] AS c5 "
            "FROM numbers(%d)" % (c, c, c, c, c, c, rows_of(c)))


GEN_SETTINGS = {"max_threads": "1", "max_insert_threads": "1"}


def expected_sums(srv, contents):
    out = {}
    for c in contents:
        r = q(srv, "SELECT sum(cityHash64(*)) FROM (%s)" % gen_sql(c), GEN_SETTINGS).strip()
        out[c] = int(r)
    return out


# ---- tables ---------------------------------------------------------------------------------------------------

class T:
    def __init__(self, srv, name):
        self.srv = srv
        self.name = name
        self.lock = threading.Lock()
        self.states = [dict()]      # possible {cid: copies}; more than one only after an op of unknown outcome
        self.maybe_gone = False     # CREATE/DROP of unknown outcome
        self.history = []           # (time, op) for failure forensics
        self.dead = False


REG = {}
REGLOCK = threading.Lock()
HOLD = {}        # cid -> number of live (table, copy) holders as the driver believes; approximate across kills
LAST_FREE = {}   # cid -> time the holder count last fell to 0
HOLDLOCK = threading.Lock()


def hold(c):
    with HOLDLOCK:
        if HOLD.get(c, 0) == 0:
            inc("inserts_of_unreferenced_content")
            gap = time.time() - LAST_FREE.get(c, 0)
            if gap < 30:
                inc("inserts_of_unreferenced_content_gap_lt_30s")
            if gap < 10:
                inc("inserts_of_unreferenced_content_gap_lt_10s")
        HOLD[c] = HOLD.get(c, 0) + 1


def release(c, n=1):
    with HOLDLOCK:
        HOLD[c] = max(0, HOLD.get(c, 0) - n)
        if HOLD[c] == 0:
            LAST_FREE[c] = time.time()


EXP = {}


def add_copy(s, c):
    d = dict(s)
    d[c] = d.get(c, 0) + 1
    return d


def state_sum(s):
    return sum(EXP[c] * n for c, n in s.items()) & M64


def state_rows(s):
    return sum(rows_of(c) * n for c, n in s.items())


def hist(t, op):
    t.history.append("%s %s" % (time.strftime("%T"), op))
    if len(t.history) > 60:
        del t.history[:20]


def quarantine(t):
    t.dead = True
    with REGLOCK:
        REG.pop(t.name, None)


def nap(t, secs):
    """Sleeps without the table lock so that a verifier can read the table meanwhile."""
    t.lock.release()
    try:
        time.sleep(secs)
    finally:
        t.lock.acquire()


def read_check(t, kind="read"):
    """Reads the table back and compares with the possible states. Caller holds t.lock. True if the table is fine."""
    sql = "SELECT count(), sum(cityHash64(*)) FROM %s" % t.name
    while True:
        try:
            r = q(t.srv, sql).split()
        except ConnErr:
            if STOP.is_set() and not in_noise(t.srv):
                return False
            if not wait_up(t.srv):
                return False
            continue
        except QueryErr as e:
            if e.code == 60 and t.maybe_gone:
                t.dead = True
                with REGLOCK:
                    REG.pop(t.name, None)
                return False
            if in_noise(t.srv):
                inc("noise")
                log("noise: %s read %s: %s" % (kind, t.name, str(e)[:200].replace("\n", " ")))
                time.sleep(2)
                if STOP.is_set():
                    return False
                continue
            fail("read_error", t.srv, t.name, sql, e, {"history": t.history[-30:], "states": [str(s) for s in t.states]})
            quarantine(t)
            return False
        break
    inc("reads")
    cnt, got = int(r[0]), int(r[1])
    ok = [s for s in t.states if state_sum(s) == got and state_rows(s) == cnt]
    if not ok:
        fail("read_mismatch", t.srv, t.name, sql, "count=%d sum=%d, possible=%s" % (cnt, got, [(state_rows(s), state_sum(s)) for s in t.states]),
             {"history": t.history[-30:], "states": [str(s) for s in t.states]})
        quarantine(t)
        return False
    t.states = ok[:1]
    t.maybe_gone = False
    return True


def check_table(t):
    sql = "CHECK TABLE %s" % t.name
    try:
        r = q(t.srv, sql, {"check_query_single_value_result": "1"}).strip()
    except ConnErr:
        return
    except QueryErr as e:
        if in_noise(t.srv):
            inc("noise")
            return
        fail("check_table_error", t.srv, t.name, sql, e, {"history": t.history[-30:]})
        return
    inc("checks")
    if r != "1":
        fail("check_table_failed", t.srv, t.name, sql, "result=%s" % r, {"history": t.history[-30:]})


def _with(d, c, k):
    r = dict(d)
    r.pop(c, None)
    if k:
        r[c] = k
    return r


def submultisets(s):
    """All {cid: copies} with 0 <= copies <= s[cid]. A TRUNCATE cut by a kill can leave any subset of the parts."""
    out = [dict()]
    for c, n in sorted(s.items()):
        out = [_with(d, c, k) for d in out for k in range(n + 1)]
        if len(out) > 5000:
            break
    return out


def partial_partition(s, c):
    return [_with(s, c, k) for k in range(s.get(c, 0) + 1)]


def mutate(t, sql, apply, what, settings=None, alts=None):
    """Runs a mutation; on an unknown outcome keeps both possible states and settles with a read. Caller holds t.lock."""
    hist(t, what)
    try:
        q(t.srv, sql, settings)
    except (ConnErr, QueryErr) as e:
        if isinstance(e, QueryErr) and not in_noise(t.srv):
            fail("mutation_error", t.srv, t.name, sql, e, {"history": t.history[-30:]})
            quarantine(t)
            return False
        inc("uncertain_ops")
        if isinstance(e, QueryErr):
            inc("noise")
        log("uncertain %s on %s: %s" % (what, t.name, str(e)[:160].replace("\n", " ")))
        t.states = t.states + [x for s in t.states for x in (alts(s) if alts else [apply(s)])]
        if what.startswith("create"):
            t.maybe_gone = True
        if not wait_up(t.srv):
            return False
        return read_check(t, "settle")
    t.states = [apply(s) for s in t.states]
    return True


# ---- copies between tables ------------------------------------------------------------------------------------

DDL_TEMPLATE = ("CREATE TABLE %s (%s) ENGINE = MergeTree PARTITION BY cid ORDER BY n SETTINGS storage_policy = '%s', "
                "min_bytes_for_wide_part = 0, old_parts_lifetime = 2, cleanup_delay_period = 1, cleanup_delay_period_random_add = 0")
COPY_KINDS = ("attach_from", "move_partition", "clone_as")
COPY_ENDING_P = 0.35
COPY_RETRIES = 5
NO_REPLICA_HAS_PART = 234   # a copy whose source cannot be confirmed is refused with this; the statement is retried


def ddl(name):
    return DDL_TEMPLATE % (name, COLUMNS, POLICY)


def pause_after_failure():
    time.sleep(1)


def hold_copy(c, n):
    with HOLDLOCK:
        HOLD[c] = HOLD.get(c, 0) + n


def drop_registered(t):
    """DROP TABLE of a registered table, releasing its contents. Caller holds t.lock. False if the drop failed."""
    hist(t, "drop table")
    try:
        q(t.srv, "DROP TABLE %s SYNC" % t.name)
    except ConnErr:
        t.maybe_gone = True
        inc("uncertain_ops")
        if wait_up(t.srv):
            read_check(t, "settle")
        return False
    except QueryErr as e:
        if in_noise(t.srv):
            inc("noise")
            t.maybe_gone = True
        else:
            fail("drop_error", t.srv, t.name, "DROP TABLE", e, {"history": t.history[-30:]})
            quarantine(t)
        pause_after_failure()
        return False
    inc("drops")
    for cc, nn in (t.states[0] if len(t.states) == 1 else {}).items():
        release(cc, nn)
    with REGLOCK:
        REG.pop(t.name, None)
    return True


def copy_ending(srv, t, kind, rnd):
    """Copies a partition (or the whole table) of t into a new table, drops t, reads the new table back, drops it.

    A copy that adds a reference without reading the bodies, followed at once by the drop of its source, is the
    sequence a GC round must not turn into a lost body. Caller holds t.lock and t.states holds exactly one state.
    True if t is gone and the writer must start over; False if the copy was given up and t is intact."""
    st = dict(t.states[0])
    c = rnd.choice(sorted(st))
    dname = t.name + "_cp"
    if kind == "clone_as":
        sql = "CREATE TABLE %s CLONE AS %s" % (dname, t.name)
        new = dict(st)
    else:
        sql = ("ALTER TABLE %s ATTACH PARTITION %d FROM %s" % (dname, c, t.name) if kind == "attach_from"
               else "ALTER TABLE %s MOVE PARTITION %d TO TABLE %s" % (t.name, c, dname))
        new = {c: st[c]}
        try:
            q(srv, ddl(dname))
        except (ConnErr, QueryErr) as e:
            log("copy %s: creating %s failed: %s" % (kind, dname, str(e)[:200].replace("\n", " ")))
            pause_after_failure()
            return False

    def cleanup_dst():
        try:
            q(srv, "DROP TABLE IF EXISTS %s SYNC" % dname)
        except (ConnErr, QueryErr):
            pass

    done = False
    hist(t, "%s c%d -> %s" % (kind, c, dname))
    for attempt in range(COPY_RETRIES + 1):
        try:
            q(srv, sql)
            done = True
            break
        except QueryErr as e:
            if e.code == NO_REPLICA_HAS_PART:
                inc(kind + "_refused")
                log("copy %s refused (attempt %d): %s" % (kind, attempt, e.msg[:160].replace("\n", " ")))
                pause_after_failure()
                if kind == "clone_as":
                    cleanup_dst()
                continue
            if not in_noise(srv):
                fail("copy_error", srv, t.name, sql, e, {"history": t.history[-30:]})
            else:
                inc("noise")
            err = e
        except ConnErr as e:
            err = e
        # unknown outcome: forget both tables, drop what is left
        inc("copy_uncertain")
        log("copy %s uncertain: %s" % (kind, str(err)[:200].replace("\n", " ")))
        for cc, nn in st.items():
            release(cc, nn)
        quarantine(t)
        if wait_up(srv):
            cleanup_dst()
            try:
                q(srv, "DROP TABLE IF EXISTS %s SYNC" % t.name)
            except (ConnErr, QueryErr):
                pass
        return True
    if not done:
        inc("copy_gave_up")
        cleanup_dst()
        read_check(t, "after a refused copy")
        return False

    inc(kind)
    dst = T(srv, dname)
    dst.states = [new]
    for cc, nn in new.items():
        hold_copy(cc, nn)
    if kind == "move_partition":
        t.states = [{k: v for k, v in st.items() if k != c}]
        release(c, st[c])
    with dst.lock:
        with REGLOCK:
            REG[dname] = dst
        inc("tables")
        if read_check(dst, "copy"):
            inc("copy_dst_reads")
        drop_registered(t)
        if dst.dead:
            return True
        nap(dst, rnd.random() * 2.0)
        if read_check(dst, "copy"):
            inc("copy_dst_reads")
        if time.time() >= DEADLINE[0] or STOP.is_set():
            hist(dst, "kept alive for the final read-back")
            return True
        drop_registered(dst)
    return True


DEADLINE = [0.0]


# ---- writers --------------------------------------------------------------------------------------------------

def writer(srv, wid, contents, hot, seed, deadline):
    _TLS.gated = True
    rnd = random.Random(seed)
    seq = 0
    pending = []   # (due, cid): re-insert after a random pause, so the content sits unreferenced for a while
    while not STOP.is_set() and time.time() < deadline:
        if not wait_up(srv):
            return
        seq += 1
        name = "t_s%d_w%d_%d" % (srv, wid, seq)
        t = T(srv, name)
        with t.lock:
            if not mutate(t, ddl(name), lambda s: s, "create"):
                pause_after_failure()
                continue
            with REGLOCK:
                REG[name] = t
            inc("tables")
            due = [c for (d, c) in pending if d <= time.time()]
            pending = [(d, c) for (d, c) in pending if d > time.time()]
            picks = due + [(rnd.choice(hot) if rnd.random() < 0.5 else rnd.choice(contents)) for _ in range(rnd.randint(1, 4))]
            for c in picks:
                if STOP.is_set() or t.dead:
                    break
                ok = mutate(t, "INSERT INTO %s %s" % (name, gen_sql(c)), (lambda cc: lambda s: add_copy(s, cc))(c),
                            "insert c%d" % c, GEN_SETTINGS)
                if t.dead:
                    break
                if not ok:
                    pause_after_failure()
                if ok:
                    inc("inserts")
                    hold(c)
                    if c in due:
                        inc("boomerang_inserts")
                    if rnd.random() < 0.5:
                        read_check(t)
                if rnd.random() < 0.3:
                    nap(t, rnd.random() * 0.5)
            if t.dead:
                continue
            nap(t, rnd.random() * 2.0)
            read_check(t)
            if t.dead:
                continue
            act = rnd.choice(["drop", "drop", "drop", "truncate", "drop_partition", "drop_partition"])
            present = sorted(t.states[0].keys()) if len(t.states) == 1 else []
            st0 = dict(t.states[0]) if len(t.states) == 1 else {}
            if act == "drop_partition" and present:
                c = rnd.choice(present)
                if mutate(t, "ALTER TABLE %s DROP PARTITION %d" % (name, c), (lambda cc: lambda s: {k: v for k, v in s.items() if k != cc})(c), "drop_partition c%d" % c,
                            alts=(lambda cc: lambda s: partial_partition(s, cc))(c)):
                    inc("drop_partitions")
                    release(c, st0.get(c, 0))
                    if rnd.random() < 0.6:
                        pending.append((time.time() + rnd.random() * 25, c))
                    nap(t, rnd.random() * 1.5)
                    read_check(t)
                if t.dead:
                    continue
            elif act == "truncate":
                if mutate(t, "TRUNCATE TABLE %s" % name, lambda s: {}, "truncate", alts=submultisets):
                    inc("truncates")
                    for cc, nn in st0.items():
                        release(cc, nn)
                    for c in present:
                        if rnd.random() < 0.6:
                            pending.append((time.time() + rnd.random() * 25, c))
                    read_check(t)
                if t.dead:
                    continue
            else:
                for c in present:
                    if rnd.random() < 0.6:
                        pending.append((time.time() + rnd.random() * 25, c))
            if rnd.random() < 0.2:
                # long hold: referenced content that survives many GC rounds and kills
                for _ in range(int(5 + rnd.random() * 35)):
                    if STOP.is_set() or time.time() >= deadline:
                        break
                    nap(t, 1.0)
                read_check(t)
                if t.dead:
                    continue
            if time.time() >= deadline or STOP.is_set():
                hist(t, "kept alive for the final read-back")
                break
            if len(t.states) == 1 and t.states[0] and rnd.random() < COPY_ENDING_P:
                if copy_ending(srv, t, rnd.choice(COPY_KINDS), rnd):
                    continue
                if t.dead:
                    continue
            hist(t, "drop table")
            try:
                q(srv, "DROP TABLE %s SYNC" % name)
                inc("drops")
                for cc, nn in (t.states[0] if len(t.states) == 1 else {}).items():
                    release(cc, nn)
                with REGLOCK:
                    REG.pop(name, None)
            except ConnErr:
                t.maybe_gone = True
                inc("uncertain_ops")
                if wait_up(srv):
                    read_check(t, "settle")
            except QueryErr as e:
                if in_noise(srv):
                    inc("noise")
                    t.maybe_gone = True
                else:
                    fail("drop_error", srv, name, "DROP TABLE", e, {"history": t.history[-30:]})
                    quarantine(t)
                pause_after_failure()
        time.sleep(rnd.random() * 0.3)


def verifier(srv, seed, deadline):
    _TLS.gated = True
    rnd = random.Random(seed)
    while not STOP.is_set() and time.time() < deadline:
        time.sleep(0.2 + rnd.random() * 0.8)
        with REGLOCK:
            ts = [t for t in REG.values() if t.srv == srv]
        if not ts:
            continue
        t = rnd.choice(ts)
        if not t.lock.acquire(blocking=False):
            continue
        try:
            if t.dead or t.name not in REG:
                continue
            if in_noise(srv):
                continue
            if read_check(t, "verify") and rnd.random() < 0.12:
                check_table(t)
        finally:
            t.lock.release()


def gc_loop(srv, seed, deadline):
    rnd = random.Random(seed)
    while not STOP.is_set() and time.time() < deadline:
        try:
            r = q(srv, "SYSTEM CAS GC RUN %s" % DISK, {"default_format": "JSONEachRow"}, timeout=300)
            inc("gc_calls")
            for line in r.splitlines():
                d = json.loads(line)
                if int(d.get("acquired_lease", 0)):
                    inc("gc_led")
                    if not int(d.get("deferred", 0)):
                        inc("gc_folded")
        except ConnErr:
            time.sleep(1)
        except QueryErr as e:
            if in_noise(srv):
                inc("noise")
            else:
                log("gc run error server=%d: %s" % (srv, str(e)[:300].replace("\n", " ")))
            time.sleep(1)
        time.sleep(rnd.random() * 0.4)


def fsck_row(srv):
    r = q(srv, "SYSTEM CAS FSCK %s" % DISK, {"default_format": "JSONEachRow", "output_format_json_quote_64bit_integers": "0"},
          timeout=900)
    return json.loads(r.splitlines()[0])


def hard_findings(row):
    return {k: row[k] for k in HARD_FSCK if int(row.get(k, 0)) != 0}


_COLUMNS_CHECKED = [False]


def fsck_online(srv, tag):
    """Online fsck; a hard finding is retried once after 5 s (docs: only a repeat is a finding)."""
    try:
        row = fsck_row(srv)
    except (ConnErr, QueryErr) as e:
        if not in_noise(srv):
            log("online fsck %s server=%d unavailable: %s" % (tag, srv, str(e)[:200].replace("\n", " ")))
        return None
    if not _COLUMNS_CHECKED[0]:
        _COLUMNS_CHECKED[0] = True
        missing = [k for k in HARD_FSCK + ["intents_listed", "reachable", "unreachable"] if k not in row]
        if missing:
            fail("fsck_columns_missing", srv, "-", "SYSTEM CAS FSCK", "the fsck row lacks %s; this oracle cannot run on this binary" % missing)
    inc("fsck_online")
    h = hard_findings(row)
    if h:
        time.sleep(5)
        try:
            row = fsck_row(srv)
        except (ConnErr, QueryErr):
            return row
        h = hard_findings(row)
        if h:
            fail("fsck_online_hard_finding", srv, "-", "SYSTEM CAS FSCK", json.dumps(h), {"row": row, "tag": tag})
    return row


def last_fold_row(srv):
    """The latest `fold_reduce` phase row of server srv: pending + standing rows, condemned rows, and its age."""
    r = q(srv, "SELECT toFloat64(toUnixTimestamp64Micro(event_time_microseconds)) / 1e6 AS ts, "
               "phase_metrics['pending_rows'] AS pending_rows, phase_metrics['condemned_rows'] AS condemned_rows "
               "FROM system.cas_gc_log WHERE event_type = 'Phase' AND phase = 'fold_reduce' "
               "ORDER BY event_time_microseconds DESC LIMIT 1 FORMAT JSONEachRow", timeout=30)
    return json.loads(r.splitlines()[0]) if r.strip() else {}


SAMPLE_COLS = ["ts", "server", "intents_listed", "reachable", "unreachable", "dangling", "pending_gc", "awaiting_gc",
               "gc_pending_rows", "gc_condemned_rows", "gc_row_age_s"]


def record_sample(rec):
    with LOGLOCK:
        with open(os.path.join(RUN, "intents_curve.jsonl"), "a") as f:
            f.write(json.dumps(rec) + "\n")
        def write():
            conn = metrics_mod.connect_own(DB_PATH)
            try:
                conn.execute("CREATE TABLE IF NOT EXISTS intents_samples (%s)" % ", ".join(
                    '"%s" %s' % (c, "REAL" if c in ("ts", "gc_row_age_s") else "INTEGER") for c in SAMPLE_COLS))
                conn.execute("INSERT INTO intents_samples VALUES (%s)" % ", ".join("?" for _ in SAMPLE_COLS), [rec.get(c) for c in SAMPLE_COLS])
                conn.commit()
            finally:
                conn.close()
        metrics_mod.write_retrying("intents_samples", write)


def fsck_loop(srv, deadline, every=60):
    """Once a minute: online fsck of server srv (hard findings checked) and the GC rows that own an intent."""
    while not STOP.is_set() and time.time() < deadline:
        for _ in range(every):
            if STOP.is_set():
                return
            time.sleep(1)
        row = fsck_online(srv, "periodic")
        if not row:
            continue
        try:
            g = last_fold_row(srv)
        except (ConnErr, QueryErr):
            g = {}
        rec = {"ts": time.time(), "t": time.strftime("%F %T"), "server": srv}
        for k in ("intents_listed", "reachable", "unreachable", "dangling", "pending_gc", "awaiting_gc"):
            rec[k] = int(row.get(k, 0))
        rec["gc_pending_rows"] = int(g["pending_rows"]) if "pending_rows" in g else None
        rec["gc_condemned_rows"] = int(g["condemned_rows"]) if "condemned_rows" in g else None
        rec["gc_row_age_s"] = round(time.time() - g["ts"], 1) if "ts" in g else None
        record_sample(rec)
        log("online fsck server=%d reachable=%s dangling=%s unreachable=%s pending_gc=%s awaiting_gc=%s intents_listed=%s "
            "gc_pending_rows=%s gc_condemned_rows=%s gc_row_age_s=%s" % (
                srv, rec["reachable"], rec["dangling"], rec["unreachable"], rec["pending_gc"], rec["awaiting_gc"],
                rec["intents_listed"], rec["gc_pending_rows"], rec["gc_condemned_rows"], rec["gc_row_age_s"]))


def dump_counts():
    with CLOCK:
        d = dict(CNT)
    tmp = os.path.join(RUN, "state", "intents_counts.json.tmp")
    with open(tmp, "w") as f:
        json.dump(d, f)
    os.replace(tmp, os.path.join(RUN, "state", "intents_counts.json"))


def counts_writer():
    while not STOP.is_set():
        dump_counts()
        STOP.wait(5)


def janitor(retain_min=40, period_s=300):
    """Keeps `system.cas_log` to the last `retain_min` minutes on both servers: on a churning pool it writes
    tens of MB per minute per server, which a 12 h run cannot keep."""
    while not STOP.wait(period_s):
        for srv in NODES:
            try:
                parts = _q(srv, "SELECT DISTINCT partition FROM system.parts WHERE database = 'system' AND table = 'cas_log' AND active "
                                "AND parseDateTimeBestEffort(partition) < now() - INTERVAL %d MINUTE FORMAT TSVRaw" % retain_min, None, 60)
            except (ConnErr, QueryErr) as e:
                log("janitor: server %d: %s" % (srv, str(e)[:160].replace("\n", " ")))
                continue
            for p in [x for x in parts.split("\n") if x]:
                try:
                    _q(srv, "ALTER TABLE system.cas_log DROP PARTITION '%s'" % p, None, 120)
                    log("janitor: dropped cas_log partition '%s' on server %d" % (p, srv))
                except (ConnErr, QueryErr) as e:
                    log("janitor: drop of '%s' on server %d: %s" % (p, srv, str(e)[:160].replace("\n", " ")))


# ---- lifecycle -------------------------------------------------------------------------------------------------

THREADS = []


def init(run_dir, nodes, noise, gate, log_fn, db_path, disk="ca", policy="ca"):
    global RUN, NODES, NOISE, GATE, LOG, DB_PATH, DISK, POLICY
    RUN, NODES, NOISE, GATE, LOG, DB_PATH, DISK, POLICY = run_dir, nodes, noise, gate, log_fn, db_path, disk, policy
    os.makedirs(os.path.join(RUN, "state"), exist_ok=True)


def start(duration_s, seed, contents_n=40, writers_n=4):
    """Starts the workload threads; they stop at the deadline (or `STOP`). Returns the deadline."""
    global EXP
    contents = list(range(1, contents_n + 1))
    e1 = expected_sums(1, contents)
    e2 = expected_sums(2, contents)
    if e1 != e2:
        raise RuntimeError("expected content checksums differ between the two servers")
    EXP = e1
    hot = random.Random(seed).sample(contents, min(8, len(contents)))
    log("seed=%d contents=%d hot=%s writers_per_server=%d duration=%ds" % (seed, len(contents), hot, writers_n, duration_s))
    deadline = time.time() + duration_s
    DEADLINE[0] = deadline
    for srv in (1, 2):
        for w in range(writers_n):
            THREADS.append(threading.Thread(target=writer, args=(srv, w, contents, hot, seed * 1000 + srv * 100 + w, deadline), name=f"intents-w{srv}-{w}"))
        THREADS.append(threading.Thread(target=verifier, args=(srv, seed + srv, deadline), name=f"intents-v{srv}a"))
        THREADS.append(threading.Thread(target=verifier, args=(srv, seed + srv + 50, deadline), name=f"intents-v{srv}b"))
        THREADS.append(threading.Thread(target=gc_loop, args=(srv, seed + srv + 99, deadline), name=f"intents-gc{srv}"))
        THREADS.append(threading.Thread(target=fsck_loop, args=(srv, deadline), name=f"intents-fsck{srv}"))
    for t in THREADS:
        t.daemon = True
        t.start()
    threading.Thread(target=counts_writer, daemon=True, name="intents-counts").start()
    return deadline


def stop_and_read_back():
    """Stops the workload (if the deadline has not already) and reads every table still registered."""
    STOP.set()
    for t in THREADS:
        t.join(timeout=900)
    alive = [t.name for t in THREADS if t.is_alive()]
    if alive:
        fail("workload_thread_hung", 0, "-", "-", "threads still running 900 s after the stop: %s" % alive)
    log("workload threads finished; final read-back of %d tables" % len(REG))
    STOP.clear()
    for srv in (1, 2):
        wait_up(srv)
    with REGLOCK:
        final = list(REG.values())
    for t in final:
        with t.lock:
            if not t.dead and read_check(t, "final"):
                check_table(t)
    with REGLOCK:
        d = {n: {"srv": t.srv, "states": t.states} for n, t in REG.items()}
    with open(os.path.join(RUN, "state", "intents_tables.json"), "w") as f:
        json.dump(d, f)
    dump_counts()
    log("done %s" % json.dumps(CNT))


# ---- end of the run: settle, drop everything, drain ----------------------------------------------------------

# An idle pool folds only every (gc_fold_max_defer_rounds + 1)-th GC round, and a fold uses intent nominations
# only when its round number is a multiple of that period. The default of gc_fold_max_defer_rounds is 8
# (CasPool.h, `gc_fold_max_defer_rounds`); the soak configs do not override it, so the period is 9.
NOMINATION_PERIOD = 9
PATIENCE_PERIODS = 2          # drain gives up after this many nomination periods of committed rounds without change
# ... and not before this many seconds without change either: when every round folds, 18 rounds commit in seconds,
# while a round suppresses its deletes until the catalog rows of just-dropped tables resolve, which takes wall time.
PATIENCE_MIN_S = 300
DRAIN_CAP_SECONDS = 3600         # `--drain-cap-s` of `soak.run`
DRAIN_CAP_ITERATIONS = 3000


def gc_both(tag, state):
    """One manual GC round on each server. `state` keeps the highest committed round number seen on a led round
    ('round' of the `SYSTEM CAS GC RUN` row; a deferred round mints none) and the pending + standing row count of
    the latest led, non-deferred row ('owned': rows that own an intent)."""
    for srv in (1, 2):
        try:
            r = _q(srv, "SYSTEM CAS GC RUN %s" % DISK, {"default_format": "JSONEachRow"}, 600)
        except (ConnErr, QueryErr) as e:
            log("%s: gc run server=%d: %s" % (tag, srv, str(e)[:200].replace("\n", " ")))
            continue
        for line in r.splitlines():
            d = json.loads(line)
            if not int(d.get("acquired_lease", 0)):
                continue
            state["round"] = max(state.get("round", 0), int(d.get("round", 0)))
            if not int(d.get("deferred", 0)):
                state["owned"] = int(d.get("pending_retired", 0))


def intents_listed(row, default=0):
    return row.get("intents_listed", default)


def blobs_present(row):
    """Blob bodies on the store: fsck counts every listed body as `reachable` (referenced) or `unreachable`."""
    return int(row.get("reachable", 0)) + int(row.get("unreachable", 0))


def intent_split(row, state):
    """(owned, orphan) intents. Owned = pending + standing rows of the last authoritative GC row (each owns an
    intent); orphan = the rest of the listed intents. An estimate: the GC row and the fsck listing are not one snapshot."""
    listed = int(intents_listed(row))
    if "owned" not in state:
        return None, None
    owned = min(listed, state["owned"])
    return owned, listed - owned


def fsck_summary(row, state):
    owned, orphan = intent_split(row, state)
    split = "intents_owned~%s intents_orphan~%s" % (owned, orphan) if owned is not None else "intents_owned/orphan=unknown"
    return "reachable=%s unreachable=%s pending_gc=%s awaiting_gc=%s intents_listed=%s (%s)" % (
        row.get("reachable"), row.get("unreachable"), row.get("pending_gc"), row.get("awaiting_gc"), intents_listed(row, None), split)


def fsck_key(row):
    return (row.get("reachable"), row.get("unreachable"), row.get("pending_gc"), row.get("awaiting_gc"), intents_listed(row, None))


def gc_until_quiet(max_iter, tag):
    """Settle: manual GC rounds on both servers until the fsck summary is unchanged for 3 iterations."""
    last = None
    quiet = 0
    state = {}
    for i in range(max_iter):
        gc_both(tag, state)
        row = None
        try:
            row = fsck_row(1)
        except (ConnErr, QueryErr):
            pass
        if row:
            cur = fsck_key(row)
            if i % 10 == 0 or cur != last:
                log("%s: iteration %d fsck %s" % (tag, i, fsck_summary(row, state)))
            quiet = quiet + 1 if cur == last else 0
            last = cur
            if quiet >= 3:
                return True
    return False


def user_tables(srv):
    return [x for x in _q(srv, "SELECT concat('`', database, '`.`', name, '`') FROM system.tables "
                               "WHERE database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema') FORMAT TSVRaw",
                          None, 60).split("\n") if x]


def tables_left():
    left = {}
    for srv in (1, 2):
        try:
            left[srv] = user_tables(srv)
        except (ConnErr, QueryErr):
            left[srv] = None
    return left


def wait_dropped_tables():
    """Tables whose DROP was cut by a kill are finished at restart; wait until both servers have none left."""
    for srv in (1, 2):
        for _ in range(120):
            try:
                n = int(_q(srv, "SELECT count() FROM system.dropped_tables", None, 30).strip())
            except (ConnErr, QueryErr):
                break
            if n == 0:
                break
            time.sleep(1)
        else:
            log("drain: server %d still has dropped tables pending" % srv)


def drop_all_tables():
    """Drops every user table on both servers, from system.tables, not from a registry: a table whose operation
    failed leaves the registry but keeps its blobs."""
    for attempt in range(3):
        names = {}
        for srv in (1, 2):
            try:
                names[srv] = user_tables(srv)
            except (ConnErr, QueryErr) as e:
                names[srv] = []
                log("drain: cannot list tables on server %d: %s" % (srv, str(e)[:200].replace("\n", " ")))
        if not any(names.values()):
            break
        log("drain: dropping %s tables (server 1, server 2) from system.tables, pass %d" % ("+".join(str(len(names[s])) for s in (1, 2)), attempt + 1))
        for srv in (1, 2):
            for n in names[srv]:
                try:
                    _q(srv, "DROP TABLE IF EXISTS %s SYNC" % n, None, 600)
                except (ConnErr, QueryErr) as e:
                    log("drain: drop %s on server %d: %s" % (n, srv, str(e)[:200].replace("\n", " ")))
    wait_dropped_tables()
    left = tables_left()
    for srv in (1, 2):
        if left[srv]:
            fail("drain_tables_left", srv, ",".join(left[srv][:5]), "DROP TABLE", "%d table(s) still on the server after 3 passes" % len(left[srv]))
    log("drain: tables left after the drop: server 1 = %s, server 2 = %s" % tuple("?" if left[s] is None else len(left[s]) for s in (1, 2)))


def drain_gc():
    """GC rounds on both servers until one of three ends the drain; returns (reason, fsck row, state).

    pool empty:  no blob body on the store (fsck `reachable + unreachable`) and `intents_listed == 0`.
    no change:   the fsck summary is unchanged for PATIENCE_PERIODS * NOMINATION_PERIOD committed GC rounds
                 (round numbers, not iterations: an idle pool commits a round only every NOMINATION_PERIOD-th GC call)
                 and for PATIENCE_MIN_S seconds.
    hard cap:    DRAIN_CAP_SECONDS or DRAIN_CAP_ITERATIONS."""
    tag = "drain"
    state = {}
    last = None
    round_at_change = None
    row = None
    t0 = time.time()
    patience = PATIENCE_PERIODS * NOMINATION_PERIOD
    for i in range(DRAIN_CAP_ITERATIONS):
        if time.time() - t0 > DRAIN_CAP_SECONDS:
            return "hard cap (%d s)" % DRAIN_CAP_SECONDS, row, state
        gc_both(tag, state)
        try:
            row = fsck_row(1)
        except (ConnErr, QueryErr):
            continue
        cur = fsck_key(row)
        rnd = state.get("round", 0)
        if cur != last or round_at_change is None:
            round_at_change = rnd
            time_at_change = time.time()
        if i % 10 == 0 or cur != last:
            log("%s: iteration %d committed_round=%d rounds_without_change=%d fsck %s" % (tag, i, rnd, rnd - round_at_change, fsck_summary(row, state)))
        last = cur
        if int(intents_listed(row)) == 0 and blobs_present(row) == 0:
            return "pool empty", row, state
        if rnd - round_at_change >= patience and time.time() - time_at_change >= PATIENCE_MIN_S:
            return "no change for %d committed rounds (%d nomination periods of %d) and %d s" % (
                rnd - round_at_change, PATIENCE_PERIODS, NOMINATION_PERIOD, time.time() - time_at_change), row, state
    return "hard cap (%d iterations)" % DRAIN_CAP_ITERATIONS, row, state


def final_fsck(tag):
    rows = {}
    for srv in (1, 2):
        row = fsck_online(srv, tag)
        rows[srv] = row
        with open(os.path.join(RUN, "fsck_online_%s_srv%d.json" % (tag, srv)), "w") as f:
            json.dump(row, f, indent=1)
        if row:
            log("%s: online fsck server=%d %s" % (tag, srv, json.dumps(row)))
    return rows


def settle():
    ok = gc_until_quiet(300, "settle")
    log("settle: GC %s" % ("quiet" if ok else "did not reach a fixed point in 300 iterations"))
    final_fsck("settled")


DRAIN_NOT_EMPTY = False   # set when the drain ended with blobs or intents left; the profile keeps the stand then


def drain():
    """GC until the pool is empty; a pool that is not empty when the drain ends fails the run, with the leftover
    classified. Returns the last fsck row (or None)."""
    global DRAIN_NOT_EMPTY
    reason, row, state = drain_gc()
    log("drain: ended by: %s" % reason)
    if reason == "pool empty":
        log("drain: GC drained the pool: blobs=0 intents=0")
    elif row is not None:
        owned, orphan = intent_split(row, state)
        referenced = int(row.get("reachable", 0))
        left = tables_left()
        msg = ("pool not empty: blobs on the store=%d (fsck: referenced by a table part=%d; not referenced=%d, of which pending_gc=%s "
               "awaiting_gc=%s; a live precommit's blobs are among the not-referenced ones, fsck does not tell them apart); "
               "intents=%s (owned by a GC row~%s, orphan~%s); tables left server1=%s server2=%s") % (
            blobs_present(row), referenced, int(row.get("unreachable", 0)), row.get("pending_gc"), row.get("awaiting_gc"),
            intents_listed(row, None), owned, orphan, "?" if left[1] is None else len(left[1]), "?" if left[2] is None else len(left[2]))
        log("drain: " + msg)
        DRAIN_NOT_EMPTY = True
        fail("drain_pool_not_empty", 0, "-", "SYSTEM CAS GC RUN", "ended by %s; %s" % (reason, msg))
    else:
        DRAIN_NOT_EMPTY = True
        fail("drain_pool_not_empty", 0, "-", "SYSTEM CAS FSCK", "ended by %s; no fsck row" % reason)
    rows = final_fsck("drained")
    dump_counts()
    return rows.get(1) or row
