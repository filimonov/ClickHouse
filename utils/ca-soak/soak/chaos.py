import os
import subprocess
from dataclasses import dataclass
from enum import Enum
from soak.rng import splitmix64

class FaultTarget(str, Enum):
    CH1 = "ch1"
    CH2 = "ch2"
    BOTH = "both"
    RUSTFS = "rustfs"

class FaultAction(str, Enum):
    KILL = "kill"        # docker kill -s KILL (hard crash)
    RESTART = "restart"  # docker restart
    PAUSE = "pause"      # docker pause + unpause after duration (short freeze, 5..60s)
    # A LONG freeze of ONE replica (docker pause = cgroup freezer = SIGSTOP-equivalent for the whole
    # container; unpause = SIGCONT), held past the mount-lease TTL (30s) + GC fence margin (ttl/2=15s)
    # so the frozen replica's lease EXPIRES and the peer's GC leader fences it out. On unfreeze the
    # replica must detect the fence and self-remount (recovery path: project_ca_p31_mount_fence_recovery).
    # Never applied to BOTH replicas (the cluster must stay recoverable) or to RustFS.
    FREEZE_LONG = "freeze_long"

@dataclass(frozen=True)
class Fault:
    t_offset: int        # seconds from run start
    target: FaultTarget
    action: FaultAction
    duration_s: int      # for PAUSE: how long paused; for KILL: downtime before auto-restart

# Container names from docker-compose (project "ca-soak"), overridable through the same environment
# convention the scenario framework and `soak.cluster` use, so a fault aimed at another compose project
# (the live-GCS stand is `ca-live-gcs`) reaches a running container instead of silently no-op'ing on
# an exited one. Resolved at call time; `BOTH` is expanded by `_containers` and has no single container.
_DEFAULT_CONTAINER = {FaultTarget.CH1: "ca-soak-ch1-1", FaultTarget.CH2: "ca-soak-ch2-1",
                      FaultTarget.RUSTFS: "ca-soak-rustfs1-1"}
_CONTAINER_ENV = {FaultTarget.CH1: "CA_SOAK_NODE1_CONTAINER", FaultTarget.CH2: "CA_SOAK_NODE2_CONTAINER",
                  FaultTarget.RUSTFS: "CA_SOAK_RUSTFS_CONTAINER"}


def container_for(target: "FaultTarget") -> str:
    return os.environ.get(_CONTAINER_ENV[target], _DEFAULT_CONTAINER[target])

_TARGETS = [FaultTarget.CH1, FaultTarget.CH2, FaultTarget.BOTH, FaultTarget.RUSTFS]
# FREEZE_LONG is NOT in the uniform action pick: it is a rarer (~1/6) deterministic UPGRADE applied
# after the base pick (below), so the common fault mix stays kill/restart/short-pause and the cluster
# is not frozen for most of the run.
_ACTIONS = [FaultAction.KILL, FaultAction.RESTART, FaultAction.PAUSE]

def generate_chaos_schedule(seed: int, duration_s: int, mean_interval_s: int):
    """Deterministic fault schedule from a seed. Poisson-ish inter-arrival via splitmix64. Bounded so
    the cluster always stays recoverable (never a long simultaneous KILL of BOTH replicas).

    RustFS faults are scoped to GRACEFUL actions (`RESTART`/`PAUSE`) only — never `KILL`. This is a
    deliberate scoping of the chaos surface, not a workaround for a CA defect. See [[B145]]: a hard
    `docker kill -s KILL` of the RustFS container injects a transient post-restart read-visibility
    window (a `blobs/` key briefly returns `499 NoSuchKey` on the INSERT-dedup read path) that is an
    object-store recovery artifact of the `1.0.0-beta.8` test backend, NOT a CA durability defect.
    The decisive durability probe (write N objects -> `docker kill -s KILL` rustfs -> restart ->
    re-list/read-back) showed RustFS does NOT lose acked objects on a hard kill (0 acked-but-lost
    across 5 runs incl. continuous-write-mid-kill and kill-during-recovery), and the B145 capture had
    `fsck dangling=0` (no referenced blob was permanently missing) — both confirming the 499 was
    transient visibility, not loss. CA crash-recovery is about a ClickHouse SERVER crashing over a
    durable-enough store, so CH replicas KEEP `KILL`. The remaining open question — whether CA can
    reference a blob before the store has DURABLY acked it (an ordering bug) — cannot be cleanly
    tested against this beta store and must be re-tested against a crash-durable store
    (real S3 / MinIO-with-fsync); tracked as a B145 follow-up. The remap is deterministic: a
    RustFS+KILL slot becomes RustFS+RESTART, preserving schedule length/timing."""
    faults = []
    t = 0
    i = 0
    while True:
        r = splitmix64(seed ^ (i * 0x9E3779B1))
        # inter-arrival in [0.3, 1.7] * mean (deterministic, no floats-from-clock)
        gap = (mean_interval_s * (30 + (r % 140))) // 100
        t += max(1, gap)
        if t >= duration_s:
            break
        r2 = splitmix64(r)
        target = _TARGETS[(r2 >> 3) % len(_TARGETS)]
        action = _ACTIONS[(r2 >> 7) % len(_ACTIONS)]
        dur = 5 + ((r2 >> 11) % 56)   # 5..60s
        if target == FaultTarget.RUSTFS and action == FaultAction.KILL:
            # B145: never hard-kill the (non-crash-durable-for-this-purpose) test object store; a
            # graceful restart lets RustFS flush. Deterministic downgrade KILL -> RESTART.
            action = FaultAction.RESTART
        if target == FaultTarget.BOTH and action == FaultAction.KILL:
            dur = min(dur, 60)        # safety bound
        # Rare (~1/6) UPGRADE to a long single-replica freeze, held past the mount-lease TTL so the
        # frozen replica is GC-fenced and must self-remount on unfreeze. Must hit exactly ONE ClickHouse
        # replica: never BOTH (the cluster must stay recoverable — a peer keeps the shard live and does
        # the fencing), never RustFS (freezing the store is a different fault class). Duration 60..90s
        # reliably exceeds mount_lease_ttl (30s) + GC fence margin (ttl/2 = 15s).
        if ((r2 >> 9) % 6) == 0:
            action = FaultAction.FREEZE_LONG
            target = FaultTarget.CH1 if ((r2 >> 13) & 1) == 0 else FaultTarget.CH2
            dur = 60 + ((r2 >> 17) % 31)   # 60..90s
        faults.append(Fault(t_offset=t, target=target, action=action, duration_s=dur))
        i += 1
    return faults

def intents_fault_track(seed: int, t_start: int, t_end: int, *, compress: bool = False):
    """The fault track of the intent-churn workload over `[t_start, t_end)`: long freezes of whichever server
    leads GC when the fault fires (`ObservedFaultApplier` resolves the target, so a deposed but alive leader
    resumes mid-round), `docker kill` of one server, and short pauses of one server.

    `generate_chaos_schedule` upgrades only about one fault in six to a freeze, of a random replica, and the
    phase-3 timeline arms faults only in its chaos window; this track puts a leader freeze and a kill in every
    stretch of the run. Cadence: freeze every 8-12 min, kill and pause every 5-10 min; `compress` (short proof
    runs): freeze every 3-5 min, kill every 2-4 min, pause every 2-3 min."""
    import random
    rnd = random.Random(seed ^ 0xFA17)
    minutes = {FaultAction.FREEZE_LONG: (3, 5) if compress else (8, 12),
               FaultAction.KILL: (2, 4) if compress else (5, 10),
               FaultAction.PAUSE: (2, 3) if compress else (5, 10)}
    out = []
    for action, (lo, hi) in minutes.items():
        t = t_start + rnd.uniform(lo * 30, hi * 60)
        while t < t_end:
            target = FaultTarget.CH1 if rnd.random() < 0.5 else FaultTarget.CH2
            if action == FaultAction.FREEZE_LONG:
                dur = 60 + rnd.randrange(31)
            elif action == FaultAction.KILL:
                dur = 5 + rnd.randrange(26)
            else:
                dur = 5 + rnd.randrange(56)
            out.append(Fault(t_offset=int(t), target=target, action=action, duration_s=dur))
            t += rnd.uniform(lo * 60, hi * 60)
    return sorted(out, key=lambda f: f.t_offset)


class ObservedFaultApplier:
    """Applies one fault with the checks around it that make it evidence rather than weather:

    - starts only while both servers serve the pool (`system.cas_mounts.lifecycle = 'live'`), so the pool
      stays served by one of them; otherwise the fault is skipped and logged;
    - aims a `FREEZE_LONG` at the current GC leader (`is_leader` of the server's own row);
    - opens a noise window on the servers the fault affects, closed 25 s after they serve again;
    - after the fault, requires every affected server to serve a write-then-read through the CAS disk within
      `recover_limit_s` (a missed bound is a fault failure), then holds `quiet_s` of quiet;
    - records the fault in `journal_path` (one JSON line) and as `FAULT-START` / `FAULT-RESUMED` /
      `FAULT-END` lines in the run log, which the log scan reads as fault windows.

    `nodes` maps 1 and 2 to `soak.cluster.Node`s. `noise` is an object with `open(servers)` and
    `close(servers, tail_s)`, or None."""

    NOISE_TAIL_S = 25

    def __init__(self, nodes, *, disk, journal_path, log_fn, noise=None, end_epoch=None, start_margin_s=90,
                 quiet_s=30, recover_limit_s=180, apply_fn=None):
        self.nodes = nodes
        self.disk = disk
        self.journal_path = journal_path
        self.log = log_fn
        self.noise = noise
        self.end_epoch = end_epoch
        self.start_margin_s = start_margin_s
        self.quiet_s = quiet_s
        self.recover_limit_s = recover_limit_s
        self.apply_fn = apply_fn or apply_fault
        self.failures = []
        self.applied = 0
        self.skipped = 0
        self._probe_seq = 0

    def _own_row(self, n):
        import json
        r = self.nodes[n].query(
            f"SELECT lifecycle, is_leader FROM system.cas_mounts WHERE disk = '{self.disk}' AND is_leader IS NOT NULL "
            f"FORMAT JSONEachRow", timeout=5)
        rows = [json.loads(line) for line in r.splitlines() if line.strip()]
        return rows[0] if rows else None

    def live(self, n) -> bool:
        try:
            row = self._own_row(n)
        except Exception:
            return False
        return row is not None and row["lifecycle"] == "live"

    def leader(self):
        found = None
        for n in self.nodes:
            try:
                row = self._own_row(n)
            except Exception:
                continue
            if row is not None and row.get("is_leader") == 1:
                found = n
        return found

    def _probe(self, n) -> bool:
        """A write then a read through server n's CAS disk: proves the mount is back, not only the port."""
        import time
        self._probe_seq += 1
        t = f"default.soak_probe_{n}_{int(time.time())}_{self._probe_seq}"
        node = self.nodes[n]
        try:
            node.query(f"CREATE TABLE {t} (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS storage_policy = '{self.disk}'",
                       timeout=30)
            node.query(f"INSERT INTO {t} SELECT number FROM numbers(1000)", timeout=60)
            ok = node.query(f"SELECT count(), sum(x) FROM {t}", timeout=60).split() == ["1000", "499500"]
            node.query(f"DROP TABLE IF EXISTS {t} SYNC", timeout=60)
            return ok
        except Exception:
            try:
                node.query(f"DROP TABLE IF EXISTS {t} SYNC", timeout=30)
            except Exception:
                pass
            return False

    def _record(self, rec):
        import json
        with open(self.journal_path, "a") as f:
            f.write(json.dumps(rec) + "\n")

    def __call__(self, fault: Fault):
        import time
        if self.end_epoch is not None and time.time() + self.start_margin_s > self.end_epoch:
            self.skipped += 1
            self.log(f"FAULT skipped ({fault.action.value} {fault.target.value}): too close to the end of the run")
            return
        t_wait = time.time()
        while not (self.live(1) and self.live(2)):
            if time.time() - t_wait > 120:
                self.skipped += 1
                self.log(f"FAULT skipped ({fault.action.value} {fault.target.value}): both servers are not live on the pool")
                return
            time.sleep(2)
        led = self.leader()
        why = "schedule"
        if fault.action == FaultAction.FREEZE_LONG and led is not None:
            fault = Fault(t_offset=fault.t_offset, target=FaultTarget.CH1 if led == 1 else FaultTarget.CH2,
                          action=fault.action, duration_s=fault.duration_s)
            why = "gc leader"
        servers = {FaultTarget.CH1: [1], FaultTarget.CH2: [2]}.get(fault.target, [1, 2])
        start = time.time()
        if self.noise is not None:
            self.noise.open(servers)
        for n in servers:
            self.log(f"FAULT-START server {n} kind={fault.action.value} target={fault.target.value} "
                     f"duration={fault.duration_s}s why={why} leader_before={led}")
        try:
            self.apply_fn(fault)
        finally:
            resumed = time.time()
            for n in servers:
                self.log(f"FAULT-RESUMED server {n} kind={fault.action.value} after {resumed - start:.1f} s")
        recovered = {}
        for n in servers:
            t0 = time.time()
            ok = False
            while time.time() - t0 < self.recover_limit_s:
                if self._probe(n):
                    ok = True
                    break
                time.sleep(2)
            recovered[n] = (ok, round(time.time() - t0, 1))
            if ok:
                self.log(f"FAULT-END server {n} kind={fault.action.value} served a read {time.time() - t0:.1f} s after resume")
            else:
                msg = f"server {n} did not serve a read {self.recover_limit_s} s after {fault.action.value} ended"
                self.failures.append(msg)
                self.log(f"FAILURE {msg}")
        if self.noise is not None:
            self.noise.close(servers, self.NOISE_TAIL_S)
        self.applied += 1
        for n in servers:
            self._record({"kind": fault.action.value, "target": fault.target.value, "server": n, "why": why,
                          "leader_before": led, "start": start, "end": resumed, "duration_s": round(resumed - start, 1),
                          "recovered": recovered[n][0], "recover_s": recovered[n][1]})
        time.sleep(self.quiet_s)


def _containers(target: FaultTarget):
    if target == FaultTarget.BOTH:
        return [container_for(FaultTarget.CH1), container_for(FaultTarget.CH2)]
    return [container_for(target)]

def _is_running(container: str) -> bool:
    """Return True iff the container is in 'running' state."""
    r = subprocess.run(
        ["docker", "inspect", "--format", "{{.State.Status}}", container],
        capture_output=True, text=True, timeout=30)
    return r.returncode == 0 and r.stdout.strip() == "running"


def apply_fault(fault: Fault):
    """Execute a fault via docker. Thin wrapper; the driver schedules these. KILL is followed by a
    `docker start` after duration_s (so the node recovers); PAUSE is unpause after duration_s.

    After `docker start`, polls until the container is in 'running' state (up to 30s) so the caller's
    `wait_healthy` polling starts from a known container-running baseline."""
    import time
    cs = _containers(fault.target)
    if fault.action == FaultAction.KILL:
        for c in cs:
            subprocess.run(["docker", "kill", "-s", "KILL", c], capture_output=True)
        time.sleep(fault.duration_s)
        for c in cs:
            subprocess.run(["docker", "start", c], capture_output=True)
        # Wait for container to reach 'running' state before returning
        for c in cs:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if _is_running(c):
                    break
                time.sleep(2)
    elif fault.action == FaultAction.RESTART:
        for c in cs:
            subprocess.run(["docker", "restart", c], capture_output=True)
    elif fault.action in (FaultAction.PAUSE, FaultAction.FREEZE_LONG):
        # Both freeze the container via the cgroup freezer (docker pause = SIGSTOP-equivalent for all
        # tasks; unpause = SIGCONT). FREEZE_LONG differs only in duration (held past the mount-lease TTL
        # to force a fence-out), so the mechanics are identical.
        for c in cs:
            subprocess.run(["docker", "pause", c], capture_output=True)
        try:
            time.sleep(fault.duration_s)
        finally:
            for c in cs:
                subprocess.run(["docker", "unpause", c], capture_output=True)
