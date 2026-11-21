"""A profile run: `python3 -m soak.run --profile {main,intents,combined} --binary PATH --duration D`.

One run = one directory, one docker stand (`soak.stand`), one fault schedule run by one `ChaosRunner`
through `chaos.ObservedFaultApplier`, one sqlite metrics store, one verdict:

  main      the phase-3 timeline of `soak.run` (ledger workload, model, checkpoints, metrics ticker);
  intents   the intent-churn workload and its oracle (`soak.intents`), with faults over the whole run;
  combined  both at once; the intents workload pauses while a main checkpoint quiesces the cluster.

Every profile ends the same way: settle and fsck, drop every table on both servers, restart each server,
GC until the pool is empty (a pool that does not empty fails the run), fsck again, then the fault and GC
summary, the overlap evidence, the log scan, and `RESULT=PASS|FAIL failures=N (...)` in `run.log` and
`run.done`. The exit status is 0 for PASS and 1 for FAIL.
"""

import json
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import traceback

from soak import chaos, evidence, intents
from soak import harness_failures, preserve
from soak import metrics as metrics_mod
from soak.cluster import Node
from soak.fsck import run_fsck, stale_edge_verdict, FsckTimeout, FsckUnavailable
from soak.stand import Stand, CA_SOAK_DIR

DISK = "ca"


class RunLog:
    """stdout and stderr of the run: every line gets a timestamp and goes to `run.log`. Lines are assembled per
    thread, so concurrent prints do not interleave inside a line."""

    def __init__(self, path):
        self.path = path
        self._lock = threading.Lock()
        self._tls = threading.local()

    def write(self, s):
        buf = getattr(self._tls, "buf", "") + s
        *lines, rest = buf.split("\n")
        self._tls.buf = rest
        if lines:
            stamp = time.strftime("%F %T")
            with self._lock:
                with open(self.path, "a") as f:
                    for line in lines:
                        f.write(f"{stamp} {line}\n")
        return len(s)

    def flush(self):
        pass

    def isatty(self):
        return False


def log(msg):
    print(f"[profile] {msg}", flush=True)


class Hooks:
    """What `run_phase3` calls in a profile run (see its docstring)."""

    def __init__(self, applier, extra_faults_fn, gate):
        self.apply_fault = applier
        self._extra = extra_faults_fn
        self._gate = gate

    def extra_faults(self, plan):
        return self._extra(plan)

    def checkpoint_enter(self, label):
        if self._gate is not None:
            t0 = time.time()
            left = self._gate.pause()
            log(f"intents workload paused for '{label}' in {time.time() - t0:.1f} s"
                + (f"; {left} statement(s) still in flight after the wait" if left else ""))

    def checkpoint_exit(self, label):
        if self._gate is not None:
            self._gate.resume()


def port_free(port):
    """True if nothing listens on the port; SO_REUSEADDR so that connections of a previous run still in
    TIME_WAIT do not count, as they do not stop docker from publishing the port."""
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.bind(("127.0.0.1", port))
        return True
    except OSError:
        return False
    finally:
        s.close()


def offline_fsck(run_dir, tag, failures):
    """`clickhouse disks cas-fsck --detail` over the read-only disk of the stand's fsck config (`soak.fsck`)."""
    container = os.environ["CA_SOAK_FSCK_CONTAINER"]
    try:
        r = run_fsck(container, detail=True, timeout_s=1800)
    except (FsckTimeout, FsckUnavailable) as e:
        failures.append(f"offline cas-fsck ({tag}) did not run: {e}")
        log(f"FAILURE offline cas-fsck ({tag}) did not run: {e}")
        return None
    with open(os.path.join(run_dir, f"fsck_offline_{tag}.txt"), "w") as f:
        f.write(r["stdout"])
        f.write(r["stderr"])
        f.write(f"exit={r['exit_code']}\n")
    summary = {k: v for k, v in r.items() if k not in ("stdout", "stderr", "detail")}
    log(f"offline cas-fsck ({tag}): exit={r['exit_code']} {summary}")
    if r["exit_code"] != 0:
        failures.append(f"offline cas-fsck ({tag}) exit {r['exit_code']}")
        log(f"FAILURE offline cas-fsck ({tag}) exit {r['exit_code']}")
    # `cas-fsck` does not exit non-zero on stale edges; the main checkpoints' verdict decides here too.
    verdict, why = stale_edge_verdict(r, detail=True)
    if verdict in ("absent", "found"):
        failures.append(f"offline cas-fsck ({tag}): {why}")
        log(f"FAILURE offline cas-fsck ({tag}): {why}")
    else:
        log(f"offline cas-fsck ({tag}): stale edges {verdict} ({why})")
    return r


def disk_sampler(stand, run_dir, stop, period_s=300):
    """Logs the disk the run uses every `period_s`: the run directory and, per container of the stand, its data
    directory (the image keeps `/var/lib/clickhouse` in an anonymous volume, which `docker ps -s` does not count)."""
    while not stop.wait(period_s):
        log(f"disk: {stand.disk_usage()}")


def write_db_tail(db_path, journal_path, counts):
    faults = [json.loads(line) for line in open(journal_path)] if os.path.exists(journal_path) else []

    def write():
        conn = metrics_mod.connect_own(db_path)
        try:
            conn.execute("CREATE TABLE IF NOT EXISTS faults (start REAL, end REAL, kind TEXT, target TEXT, server INTEGER, why TEXT, "
                         "leader_before INTEGER, recovered INTEGER, recover_s REAL)")
            for f in faults:
                conn.execute("INSERT INTO faults VALUES (?,?,?,?,?,?,?,?,?)", (f["start"], f["end"], f["kind"], f["target"], f["server"],
                             f["why"], f["leader_before"], int(f["recovered"]), f["recover_s"]))
            conn.execute("CREATE TABLE IF NOT EXISTS intents_counters (name TEXT, value INTEGER)")
            conn.executemany("INSERT INTO intents_counters VALUES (?, ?)", sorted(counts.items()))
            conn.commit()
        finally:
            conn.close()
    metrics_mod.write_retrying("faults/intents_counters", write)


def run_profile(args):
    from soak import run as run_mod   # deferred: soak.run imports this module

    profile = args.profile
    run_dir = os.path.abspath(args.run_dir or os.path.join(CA_SOAK_DIR, "logs", "runs", f"{profile}-{time.strftime('%Y%m%d-%H%M%S')}"))
    os.makedirs(run_dir, exist_ok=True)
    if os.path.exists(os.path.join(run_dir, "run.log")):
        print(f"run directory {run_dir} already holds a run.log", file=sys.stderr)
        return 2
    real_stdout = sys.stdout
    sys.stdout = sys.stderr = RunLog(os.path.join(run_dir, "run.log"))
    utc_offset = time.strftime("%z")
    log(f"time zone: run.log is in local time, UTC{utc_offset[:3]}:{utc_offset[3:]} ({time.strftime('%Z')}); server logs are in UTC")
    os.makedirs(os.path.join(run_dir, "state"), exist_ok=True)
    journal = os.path.join(run_dir, "faults.jsonl")
    db_path = os.path.join(run_dir, "metrics.db")
    duration = args.duration
    harness = []          # harness failures (text)
    harness_failures.reset()
    nodes = {}
    main_rc = None
    applier = None
    stand = None
    interrupted = []

    def on_signal(signum, _frame):
        interrupted.append(signum)
        raise KeyboardInterrupt(f"signal {signum}")

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    try:
        binary = os.path.abspath(args.binary)
        if not os.access(binary, os.X_OK):
            raise RuntimeError(f"binary {binary} is not executable")
        free_gb = int(subprocess.run(["df", "-BG", "--output=avail", run_dir], capture_output=True, text=True).stdout.split()[-1].rstrip("G"))
        if free_gb < args.min_free_gb:
            raise RuntimeError(f"only {free_gb} GB free under {run_dir}, --min-free-gb is {args.min_free_gb}")
        stand = Stand(run_dir, args.project, binary, args.port_base, args.compose_file, args.env_file or [], log)
        busy = [p for n in stand.ports.values() for p in n if not port_free(p)]
        if busy:
            raise RuntimeError(f"host ports {busy} are in use")
        version = subprocess.run([binary, "--version"], capture_output=True, text=True, timeout=120).stdout.strip().splitlines()[-1:]
        env = {"profile": profile, "binary": binary, "version": version, "duration_s": duration, "seed": args.seed,
               "chaos_seed": args.chaos_seed, "chaos_interval": args.chaos_interval, "fault_compress": args.fault_compress,
               "no_chaos": args.no_chaos, "contents": args.contents, "intent_writers": args.intent_writers, "project": args.project,
               "port_base": args.port_base, "compose_files": args.compose_file, "df_free_gb_before": free_gb,
               "date": time.strftime("%FT%T%z"), "utc_offset": utc_offset, "utc_offset_seconds": time.localtime().tm_gmtoff,
               "time_zone": time.strftime("%Z")}
        with open(os.path.join(run_dir, "env.json"), "w") as f:
            json.dump(env, f, indent=1)
        log(f"start: {json.dumps(env)}")

        stand.up()
        nodes = {n: Node("127.0.0.1", stand.ports[n][0]) for n in (1, 2)}
        server_tz = nodes[1].query("SELECT timezone()").strip()
        run_start = time.time()
        end_epoch = run_start + duration
        use_intents = profile in ("intents", "combined")
        noise = intents.NoiseWindows() if use_intents else None
        gate = intents.Gate() if profile == "combined" else None
        applier = chaos.ObservedFaultApplier(nodes, disk=DISK, journal_path=journal, log_fn=log, noise=noise, end_epoch=end_epoch)

        # The intents module also serves every profile's strict end and the cas_log janitor.
        intents.init(run_dir, {n: ("127.0.0.1", stand.ports[n][0]) for n in (1, 2)}, noise or intents.NoiseWindows(),
                     gate or intents.Gate(), log_fn=lambda m: print(m, flush=True), db_path=db_path, disk=DISK, policy=DISK)
        intents.DRAIN_CAP_SECONDS = args.drain_cap_s
        if use_intents:
            intents.start(duration, args.seed, contents_n=args.contents, writers_n=args.intent_writers)
        janitor = threading.Thread(target=intents.janitor, daemon=True, name="janitor")
        janitor.start()
        threading.Thread(target=disk_sampler, args=(stand, run_dir, intents.STOP), daemon=True, name="disk").start()

        if profile in ("main", "combined"):
            args.metrics = db_path
            args.failure_json = os.path.join(run_dir, "failure.json")
            def intents_track(plan):
                """Outside the phase-3 chaos window only: inside it the main schedule already faults about every
                `--chaos-interval` seconds, and the applier aims its freezes at the GC leader too."""
                lo, hi = run_mod.chaos_window(plan)
                return [f for f in chaos.intents_fault_track(args.seed, 60, max(60, duration - 90), compress=args.fault_compress)
                        if not lo <= f.t_offset < hi]

            hooks = Hooks(applier, intents_track if use_intents else (lambda plan: []), gate)
            result = {}

            def main_thread():
                try:
                    result["rc"] = run_mod.run_phase3(args, hooks)
                except SystemExit as e:
                    result["rc"] = e.code if isinstance(e.code, int) else 1
                except BaseException as e:   # noqa: BLE001 -- the verdict must see it
                    result["rc"] = 1
                    log(f"main profile raised {type(e).__name__}: {e}\n{traceback.format_exc()}")

            mt = threading.Thread(target=main_thread, name="main-profile", daemon=True)
            mt.start()
            while mt.is_alive():
                mt.join(5)
                if use_intents and "rc" in result and result["rc"] != 0 and not intents.STOP.is_set():
                    log("main profile failed; stopping the intents workload early")
                    intents.STOP.set()
            main_rc = result.get("rc", 1)
            log(f"main profile finished rc={main_rc}")
        else:
            window_end = max(60, duration - 90)
            schedule = [] if args.no_chaos else sorted(
                [f for f in chaos.generate_chaos_schedule(args.chaos_seed, window_end, args.chaos_interval) if f.t_offset >= 60]
                + chaos.intents_fault_track(args.seed, 60, window_end, compress=args.fault_compress), key=lambda f: f.t_offset)
            log(f"fault schedule: {len(schedule)} faults: " + ", ".join(
                f"t+{f.t_offset}s {f.target.value} {f.action.value} {f.duration_s}s" for f in schedule))
            chaos_stop = threading.Event()
            runner = run_mod.ChaosRunner(schedule, on_fault_done=lambda f: None, stop_event=chaos_stop,
                                         checkpoint_active=threading.Event(), apply_fn=applier)
            runner.start()
            while time.time() < end_epoch:
                time.sleep(5)
            chaos_stop.set()
            runner.join(timeout=900)
            if runner.error is not None:
                harness.append(f"fault runner error: {runner.error}")

        if use_intents:
            intents.stop_and_read_back()
        intents.STOP.set()   # stops the janitor

        # ---- the strict end -----------------------------------------------------------------------------
        for n in (1, 2):
            if not stand.running(n):
                harness.append(f"server {n} is not running after the workload")
                log(f"FAILURE server {n} is not running after the workload")
        janitor.join(timeout=600)
        intents.STOP.clear()
        for node in nodes.values():
            node.query("SYSTEM FLUSH LOGS", timeout=300)
        intents.settle()
        offline_fsck(run_dir, "settled", harness)
        log(f"disk: {stand.disk_usage()}")
        intents.drop_all_tables()
        for n in (1, 2):
            log(f"quiet-phase restart of server {n} (clean stop)")
            stand.restart(n)
        drained = intents.drain()
        offline = offline_fsck(run_dir, "drained", harness)
        if drained:
            log(f"objects left after drain (online fsck): blobs={intents.blobs_present(drained)} intents={intents.intents_listed(drained)} "
                f"dangling={drained.get('dangling')}")
        if offline:
            log(f"objects left after drain (offline fsck): blobs={int(offline.get('reachable', 0)) + int(offline.get('unreachable', 0))} "
                f"intents={offline.get('intents_listed', '?')}")

        # ---- evidence -------------------------------------------------------------------------------------
        for node in nodes.values():
            node.query("SYSTEM FLUSH LOGS", timeout=300)
        stand.copy_evidence()
        with open(os.path.join(run_dir, "faults_summary.txt"), "w") as f:
            f.write(evidence.fault_summary(run_dir, nodes, journal))
        overlap, short = evidence.overlap_summary(nodes, dict(intents.CNT))
        with open(os.path.join(run_dir, "overlap.txt"), "w") as f:
            f.write(overlap)
        log(f"overlap: {short}")
        scan, unexpected = evidence.scan_logs(run_dir, os.path.join(run_dir, "run.log"), server_tz,
                                              os.path.join(CA_SOAK_DIR, "configs", "log_allow.txt"))
        with open(os.path.join(run_dir, "log_scan.txt"), "w") as f:
            f.write(scan)
        log("log scan: " + scan.strip().splitlines()[-1])
    except KeyboardInterrupt:
        harness.append(f"interrupted ({interrupted})")
        log(f"FAILURE interrupted by signal {interrupted}")
        unexpected = None
    except BaseException as e:   # noqa: BLE001 -- the verdict must see it
        harness.append(f"harness error: {type(e).__name__}: {e}")
        log(f"FAILURE harness error: {type(e).__name__}: {e}\n{traceback.format_exc()}")
        unexpected = None
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        intents.STOP.set()
        if stand is not None:
            try:
                stand.unpause_servers()
                if nodes:
                    preserve.preserve(nodes, run_dir, log, harness.append)
                if not os.path.exists(os.path.join(run_dir, "srv1", "docker.log")):
                    stand.copy_evidence()
                log(f"disk: {stand.disk_usage()}")
                keep = args.keep_stand or intents.DRAIN_NOT_EMPTY
                if keep:
                    command = stand.removal_command()
                    reason = "the drain did not empty the pool" if intents.DRAIN_NOT_EMPTY else "--keep-stand"
                    log(f"stand KEPT ({reason}): containers and volumes of compose project {stand.project} are left in place; "
                        f"remove them later with: {command}")
                    if intents.DRAIN_NOT_EMPTY:
                        with open(os.path.join(run_dir, "KEPT_FOR_ANALYSIS"), "w") as f:
                            f.write(f"{reason}\nremove the stand with:\n{command}\n")
                else:
                    stand.down()
            except BaseException as e:   # noqa: BLE001
                harness.append(f"teardown error: {e}")
                log(f"FAILURE teardown error: {e}")

    counts = dict(intents.CNT)
    intents_failures = counts.get("failures", 0)
    main_failures = 0 if main_rc in (None, 0) else 1
    fault_failures = len(applier.failures) if applier is not None else 0
    scan_failures = unexpected if unexpected is not None else 0
    if unexpected is None and not harness:
        harness.append("log scan did not run")
    total = intents_failures + main_failures + len(harness) + fault_failures + scan_failures
    res = "PASS" if total == 0 else "FAIL"
    for h in harness:
        log(f"harness failure: {h}")
    line = (f"RESULT={res} failures={total} (driver {intents_failures + main_failures} [intents {intents_failures}, main {main_failures}], "
            f"harness {len(harness)}, faults {fault_failures}, log scan {scan_failures})")
    log(line)
    try:
        write_db_tail(db_path, journal, counts)
    except Exception as e:   # noqa: BLE001
        log(f"metrics db tail not written: {e}")
    kinds = []
    if os.path.exists(journal):
        kinds = [k for _, k in sorted({(f["start"], f["kind"]) for f in map(json.loads, open(journal))})]
    rc = 0 if res == "PASS" else 1
    with open(os.path.join(run_dir, "run.done"), "w") as f:
        f.write(f"RESULT={res} exit={rc} profile={profile} duration={duration} main_rc={main_rc} "
                f"intent_inserts={counts.get('inserts', 0)} intent_reads={counts.get('reads', 0)} kills={kinds.count('kill')} restarts={kinds.count('restart')} "
                f"freezes={kinds.count('freeze_long')} pauses={kinds.count('pause')} failures={total}\n")
    real_stdout.write(line + f"\nrun directory: {run_dir}\n")
    real_stdout.flush()
    return rc
