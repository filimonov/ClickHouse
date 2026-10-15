"""S47: decommission a dead pool member under a live workload, kill the decommission after each of its six
steps, rerun it under the operator rerun rule, and rejoin the member.

Stand (`docker-compose-s47.yml`): ch1 and ch2 run the phase 3 soak workload with its oracle; ch3 is the
victim; ch4 executes `SYSTEM CAS DROP POOL MEMBER` and is the only server whose requests pass the S3 proxy.

The kill points are exact in terms of what the store applied: the proxy parks one request of the
decommission, before or after the store applies it, and the card kills ch4 while it is parked. The parked
request never lands, and neither does a client retry of it. Step 3 is a read, so "after step 3" parks the
first write of the drain.

Cycles, each on a victim that holds data, ending with the victim rejoined empty:
  A        refusal while the victim's lease looks live, then one uninterrupted decommission;
  K1..K6   the decommission killed after step N, the rerun decided by the rerun rule
           (docs/en/antalya/cas/operations/migration.md#rejoin-rerun-rule), the refused start of the victim
           while the tombstone is unverified, and its rejoin after the run finishes;
  N        the victim's uuid file removed before the kill, so it rejoins under a new server uuid.

What this does not cover: the residuals of the lifecycle design (a late tombstone over a running member, a
drain write landing after a rejoin). Every victim here is dead before its decommission starts.
"""

import json
import re
import subprocess
import threading
import time
import urllib.request

from soak import fsck as fsck_mod
from soak.cluster import QueryError

from ..framework import cluster_boot
from ..framework import gc as gc_mod
from ..framework.assertions import assert_fsck_clean
from ..framework.base import Scenario, register
from ..framework.report import Verdict

_DISK = "ca"
_VICTIM = "ca-soak-ch3-1"
_VICTIM_SRID = "ca_soak_ch3"
_EXECUTOR = "ca-soak-ch4-1"
_PROXY = "http://localhost:8474"
_POOL_URL = "http://rustfs1:11121/test/soak_pool/"
_LOGS = cluster_boot.CA_SOAK_DIR / "logs"
_SHARED_TABLE = "s47_shared"
_EXECUTOR_TABLE = "s47_executor"
_FAREWELL_MARK = '"min_active_build_sequence":"18446744073709551615"'
_OWNER_PATH = f"server-roots/{_VICTIM_SRID}/owner"
_MOUNT_PATH = f"server-roots/{_VICTIM_SRID}/mount"
_NOT_FINISHED_RE = re.compile(r"decommission is not\s+finished")
_REJOINED_RE = re.compile(r"CAS server-root '" + _VICTIM_SRID + r"': rejoined as an empty member under server_uuid (\w+)")
_CATALOG_ENTRY_RE = re.compile(r'\{"kind":"entry","ns":"([^"]*)","state":"([^"]*)","life":"([0-9a-f]+)"')
# A dead member's lease lapses after `mount_lease_ttl_ms`, and GC fences it at the start of the next round.
# Under the soak workload one round took up to four minutes, so the bound covers two rounds and the TTL.
_FENCE_WAIT_S = 900.0
_HOLD_WAIT_S = 180.0
# A full fsck walks the whole pool while the soak writes to it.
_FSCK_TIMEOUT_S = 2400.0


def _kill_point(step: int, victim_lives: list) -> dict:
    """The proxy rule that parks the first request after step `step`, so the kill lands between steps."""
    if step == 1:
        return {"method": "PUT", "path_substring": _OWNER_PATH, "when": "before"}
    if step == 2:
        return {"method": "PUT", "path_substring": _OWNER_PATH, "when": "after"}
    if step == 3:
        paths = [f"/cas/ns/{kind}/{life}/" for life in victim_lives for kind in ("stream", "state")]
        return {"method": "PUT", "path_substrings": paths, "when": "before"}
    if step == 4:
        return {"method": "PUT", "path_substring": _MOUNT_PATH, "body_substring": _FAREWELL_MARK, "when": "before"}
    if step == 5:
        return {"method": "PUT", "path_substring": _OWNER_PATH, "skip": 1, "when": "before"}
    if step == 6:
        return {"method": "PUT", "path_substring": _OWNER_PATH, "skip": 1, "when": "after"}
    raise ValueError(step)


def _proxy(patch: dict) -> dict:
    req = urllib.request.Request(_PROXY + "/config", data=json.dumps(patch).encode(), method="POST",
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def _proxy_get(path: str) -> dict:
    with urllib.request.urlopen(_PROXY + path, timeout=10) as resp:
        return json.loads(resp.read())


def _docker(*args, timeout=180, check=True):
    return subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout, check=check)


def _container_running(container: str) -> bool:
    p = _docker("inspect", "-f", "{{.State.Running}}", container, check=False)
    return p.stdout.strip() == "true"


def _log_size(node_dir: str) -> int:
    """Server log files are written by the container user with mode 640; read them through a container."""
    p = _docker("run", "--rm", "-v", f"{_LOGS / node_dir}:/l:ro", "alpine:3", "sh", "-c",
                "stat -c %s /l/clickhouse-server.log 2>/dev/null || echo 0", check=False)
    return int((p.stdout.strip() or "0").splitlines()[-1])


def _log_since(node_dir: str, offset: int) -> str:
    p = _docker("run", "--rm", "-v", f"{_LOGS / node_dir}:/l:ro", "alpine:3", "sh", "-c",
                f"tail -c +{offset + 1} /l/clickhouse-server.log", check=False, timeout=300)
    return p.stdout


def _read_object(node, key: str):
    """The body line of a control object as a dict, read straight from the store; None if absent."""
    try:
        text = node.query(f"SELECT * FROM s3('{_POOL_URL}{key}', 'clickhouse', 'clickhouse', 'RawBLOB') FORMAT TSVRaw")
    except QueryError as e:
        if "404" in e.body or "NoSuchKey" in e.body or "does not exist" in e.body or "FILE_DOESNT_EXIST" in e.body:
            return None
        raise
    lines = text.splitlines()
    return json.loads(lines[1]) if len(lines) > 1 else None


def _owner(node):
    return _read_object(node, f"gc/server-roots/{_VICTIM_SRID}/owner")


def _mount(node):
    return _read_object(node, f"gc/server-roots/{_VICTIM_SRID}/mount")


def _victim_catalog(node) -> list:
    """(namespace, state, life) of every catalog row the victim owns."""
    text = node.query(f"SELECT * FROM s3('{_POOL_URL}cas/ref_catalog', 'clickhouse', 'clickhouse', 'RawBLOB') FORMAT TSVRaw")
    rows = [m.groups() for m in _CATALOG_ENTRY_RE.finditer(text)]
    if not rows and '"kind":"entry"' in text:
        raise AssertionError("ref catalog has entry rows the reader does not parse; this card is stale: " + text[:400])
    return [r for r in rows if r[0] == _VICTIM_SRID or r[0].startswith(_VICTIM_SRID + "/")]


def _victim_mount_rows(node) -> list:
    out = node.query(f"SELECT state, server_uuid, writer_epoch FROM system.cas_mounts "
                     f"WHERE disk = '{_DISK}' AND server_root_id = '{_VICTIM_SRID}' FORMAT JSONEachRow")
    return [json.loads(line) for line in out.splitlines() if line.strip()]


def _victim_state(node) -> str:
    rows = _victim_mount_rows(node)
    return ",".join(r["state"] for r in rows) if rows else "absent"


def _drop_member(node, timeout=600) -> dict:
    out = node.query(f"SYSTEM CAS DROP POOL MEMBER '{_VICTIM_SRID}' FROM DISK '{_DISK}'", timeout=timeout,
                     settings={"default_format": "JSONEachRow"})
    return json.loads(out.splitlines()[0])


def _wait(predicate, bound_s, interval_s=2.0):
    deadline = time.monotonic() + bound_s
    while True:
        value = predicate()
        if value or time.monotonic() >= deadline:
            return value
        time.sleep(interval_s)


def _wait_ping(node, bound_s=180.0) -> bool:
    return bool(_wait(lambda: node.ping(timeout=3), bound_s))


def _fsck() -> dict:
    """Pool-wide `cas-fsck` with the applet's own deadline off: under the soak load one scan outlasts its
    600 s default. The subprocess timeout still bounds it."""
    cmd = ["docker", "exec", "ca-soak-ch1-1", *fsck_mod._CLICKHOUSE_DISKS, "--disk", "ca_ro",
           "--query", "cas-fsck --timeout 0"]
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=_FSCK_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        raise fsck_mod.FsckTimeout(f"cas-fsck exceeded {_FSCK_TIMEOUT_S}s")
    line = next((ln for ln in p.stdout.splitlines() if ln.startswith("reachable=")), "")
    if not line:
        raise fsck_mod.FsckUnavailable(f"cas-fsck produced no summary: exit={p.returncode} stderr={p.stderr[-300:]!r}")
    return fsck_mod.parse_fsck_summary(line)


def _gc_round_on_survivors(nodes):
    """`SYSTEM CAS GC RUN` returns at once on a member that does not hold the GC lease, so ask every survivor."""
    for node in nodes:
        gc_mod.gc_round(node, disk=_DISK)


def _checksum(node, table) -> str:
    return node.query(f"SELECT count(), sum(cityHash64(id, payload)) FROM {table}")


class _Cycle:
    """Evidence of one cycle: observations, written as one JSON per cycle."""

    def __init__(self, ctx, result, name):
        self.ctx, self.result, self.name = ctx, result, name
        self.obs = {"cycle": name}

    def check(self, what, expected, observed, ok, note=""):
        self.obs.setdefault("checks", []).append({"what": what, "ok": bool(ok), "observed": str(observed)[:600]})
        self.result.add(Verdict.check(f"{self.name}: {what}", expected, str(observed)[:600], ok, note))
        self.ctx.log(f"S47 {self.name}: {'PASS' if ok else 'FAIL'} {what} -> {str(observed)[:300]}")
        return ok

    def save(self):
        self.ctx.write_json(f"cycle_{self.name}.json", self.obs)


@register
class S47(Scenario):
    name = "S47"
    title = "pool-member decommission under load: kill after each step, rerun, rejoin"
    priority = "P1"
    compose_variant = "s47"
    param_table = {
        "dev": {"victim_tables": 2, "shared_rows": 20000, "unique_rows": 5000,
                "cycles": "A,K1,K2,K3,K4,K5,K6,N", "soak_seed": 47, "soak_max_pool_gb": 6},
        "ci": {"victim_tables": 3, "shared_rows": 50000, "unique_rows": 20000,
               "cycles": "A,K1,K2,K3,K4,K5,K6,N", "soak_seed": 47, "soak_max_pool_gb": 20},
        "full": {"victim_tables": 6, "shared_rows": 200000, "unique_rows": 50000,
                 "cycles": "A,K1,K2,K3,K4,K5,K6,N", "soak_seed": 47, "soak_max_pool_gb": 40},
    }

    def run(self, ctx, result):
        p = ctx.params
        nodes = ctx.cluster.nodes()
        observer, victim, executor = nodes[0], nodes[2], nodes[3]
        self.ctx, self.result = ctx, result
        self.observer, self.victim, self.executor = observer, victim, executor
        self.survivors = (nodes[0], nodes[1])
        self.tables = [f"s47_v_{i}" for i in range(int(p["victim_tables"]))]
        self.shared_sql = (f"SELECT number AS id, repeat(toString(number % 997), 40) AS payload "
                           f"FROM numbers({int(p['shared_rows'])})")
        _proxy({"reset": True})

        observer.command(f"CREATE TABLE {_SHARED_TABLE} (id UInt64, payload String) ENGINE = MergeTree "
                         f"ORDER BY id SETTINGS storage_policy = '{_DISK}'")
        observer.command(f"INSERT INTO {_SHARED_TABLE} {self.shared_sql}")
        self.shared_checksum = _checksum(observer, _SHARED_TABLE)
        executor.command(f"CREATE TABLE {_EXECUTOR_TABLE} (id UInt64, payload String) ENGINE = MergeTree "
                         f"ORDER BY id SETTINGS storage_policy = '{_DISK}'")
        executor.command(f"INSERT INTO {_EXECUTOR_TABLE} SELECT number, toString(number) FROM numbers(1000)")
        self.executor_checksum = _checksum(executor, _EXECUTOR_TABLE)
        for t in self.tables:
            victim.command(f"CREATE TABLE {t} (id UInt64, payload String) ENGINE = MergeTree "
                           f"ORDER BY id SETTINGS storage_policy = '{_DISK}'")

        soak = self._start_soak()
        try:
            for name in [c.strip() for c in str(p["cycles"]).split(",") if c.strip()]:
                if soak.poll() is not None:
                    result.add(Verdict.check("soak workload alive during every cycle", "running",
                                             f"exited rc={soak.returncode} before cycle {name}", False))
                    break
                self._cycle(name)
        finally:
            _proxy({"hold_release": True})
            soak_rc = self._wait_soak(soak)
        soak_log = ctx.path("soak.log").read_text(errors="replace")
        result.add(Verdict.check("soak oracle (ch1/ch2) passes every checkpoint while the cycles ran",
                                 "rc=0 and PHASE3 OK", f"rc={soak_rc} ok_line={'PHASE3 OK' in soak_log}",
                                 soak_rc == 0 and "PHASE3 OK" in soak_log,
                                 "see soak.log and soak_failure.json in the run directory"))
        result.add(Verdict.check("shared content intact on the survivor at the end", self.shared_checksum,
                                 _checksum(observer, _SHARED_TABLE), _checksum(observer, _SHARED_TABLE) == self.shared_checksum))
        gc_mod.forced_gc_to_fixpoint(ctx.cluster, lambda: 0)
        assert_fsck_clean(result, _fsck())

    # ---- soak workload ----------------------------------------------------------------------------

    def _start_soak(self):
        duration = max(int(self.ctx.duration_s), 600)
        log = open(self.ctx.path("soak.log"), "w")
        cmd = ["python3", "-m", "soak.run", "--seed", str(self.ctx.params["soak_seed"]), "--phase", "3",
               "--duration", f"{duration}s", "--no-chaos", "--max-pool-gb", str(self.ctx.params["soak_max_pool_gb"]),
               "--metrics", str(self.ctx.path("soak.db")),
               "--failure-json", str(self.ctx.path("soak_failure.json"))]
        env = dict(__import__("os").environ)
        env["CA_SOAK_NODE_COUNT"] = "2"
        self.ctx.log(f"S47: starting the soak workload on ch1/ch2: {' '.join(cmd)}")
        proc = subprocess.Popen(cmd, cwd=str(cluster_boot.CA_SOAK_DIR), stdout=log, stderr=subprocess.STDOUT, env=env)
        self.soak_deadline = time.monotonic() + duration + 3600
        started = _wait(lambda: proc.poll() is not None or "=== STAGE" in self.ctx.path("soak.log").read_text(errors="replace"), 600, 5)
        if proc.poll() is not None:
            raise RuntimeError(f"the soak workload exited during setup rc={proc.returncode}")
        if not started:
            raise RuntimeError("the soak workload did not reach its first stage in 600 s")
        return proc

    def _wait_soak(self, proc):
        while proc.poll() is None and time.monotonic() < self.soak_deadline:
            time.sleep(10)
        if proc.poll() is None:
            proc.kill()
            return "killed (deadline)"
        return proc.returncode

    # ---- one cycle ----------------------------------------------------------------------------------

    def _cycle(self, name):
        ctx, observer, victim, executor = self.ctx, self.observer, self.victim, self.executor
        c = _Cycle(ctx, self.result, name)
        ctx.log(f"S47: ===== cycle {name} =====")
        _proxy({"hold": None, "trace_clear": True,
                "trace_substrings": [f"server-roots/{_VICTIM_SRID}/", "/cas/ns/", f"staging/{_VICTIM_SRID}/",
                                     f"roots/{_VICTIM_SRID}/", "cas/ref_catalog"]})

        # The victim holds data: content shared with the survivor, content of its own, and a merge.
        for t in self.tables:
            victim.command(f"INSERT INTO {t} {self.shared_sql}")
            victim.command(f"INSERT INTO {t} SELECT number + 100000000, randomPrintableASCII(64) "
                           f"FROM numbers({int(self.ctx.params['unique_rows'])})")
            victim.command(f"OPTIMIZE TABLE {t} FINAL", timeout=600)
        c.obs["victim_rows"] = {t: victim.scalar(f"SELECT count() FROM {t}") for t in self.tables}
        before = _victim_mount_rows(observer)
        c.obs["victim_before"] = before
        old_uuid = before[0]["server_uuid"] if before else None
        if name == "N":
            _docker("exec", _VICTIM, "rm", "-f", "/var/lib/clickhouse/uuid")

        _docker("kill", _VICTIM)
        c.obs["victim_killed_at"] = time.time()

        if name == "A":
            self._check_refusal_while_live(c)

        state = _wait(lambda: _victim_state(observer) == "fenced" and "fenced", _FENCE_WAIT_S, 3)
        if not c.check("the killed victim is fenced by GC", "fenced", _victim_state(observer), state == "fenced"):
            c.save()
            raise RuntimeError(f"cycle {name}: the victim never reached fenced")
        lives = [life for _ns, _st, life in _victim_catalog(observer)]
        c.obs["victim_catalog_before"] = _victim_catalog(observer)

        step = int(name[1:]) if name.startswith("K") else None
        if step is None:
            report = self._decommission(executor)
            c.obs["report"] = report
            c.check("the decommission under load reports member_retired = 1", "1",
                    report, report.get("member_retired") == 1 and not report.get("warnings"))
        else:
            self._killed_decommission(c, step, lives)
            self._recover_after_kill(c, step)

        rows = _victim_mount_rows(observer)
        c.check("no victim row in system.cas_mounts other than retired", "retired",
                rows, bool(rows) and all(r["state"] == "retired" for r in rows))
        c.check("survivor content shared with the victim is intact", self.shared_checksum,
                _checksum(observer, _SHARED_TABLE), _checksum(observer, _SHARED_TABLE) == self.shared_checksum)
        self._rejoin(c, old_uuid, new_uuid=(name == "N"))
        try:
            fsck = _fsck()
        except (fsck_mod.FsckTimeout, fsck_mod.FsckUnavailable) as e:
            c.obs["fsck"] = str(e)
            self.result.add(Verdict.inconclusive(f"{name}: fsck after the cycle", "dangling=0", str(e)))
            ctx.log(f"S47 {name}: INCONCLUSIVE fsck after the cycle -> {e}")
        else:
            c.obs["fsck"] = {k: fsck.get(k) for k in ("reachable", "unreachable", "dangling") if k in fsck}
            c.check("fsck after the cycle", "dangling=0", c.obs["fsck"],
                    fsck.get("dangling") == 0 and not fsck.get("partial"))
        c.obs["trace"] = _proxy_get("/trace")["trace"]
        c.save()

    def _check_refusal_while_live(self, c):
        """Right after the kill the victim's lease still looks live: the decommission must refuse."""
        try:
            report = _drop_member(self.executor)
            c.check("the decommission refuses a victim whose lease looks live", "ABORTED", report, False)
        except QueryError as e:
            c.obs["refusal_while_live"] = e.body
            c.check("the decommission refuses a victim whose lease looks live", "ABORTED alive or contended",
                    e.body[:400], "Code: 236" in e.body and "alive or contended" in e.body)
            c.check("the refusal names the terminal state to wait for", "text names terminated or fenced",
                    e.body[:400], "fenced" in e.body or "terminated" in e.body,
                    "the lifecycle design lists `NoWait` on a hard-killed member -> wait for state = 'fenced' "
                    "among the refusal texts")

    def _decommission(self, node, bound_s=_FENCE_WAIT_S):
        """Run the verb, waiting out a lease that still looks live (the killed victim's or a killed run's)."""
        deadline = time.monotonic() + bound_s
        while True:
            try:
                return _drop_member(node)
            except QueryError as e:
                if "alive or contended" not in e.body or time.monotonic() >= deadline:
                    raise
                time.sleep(5)

    def _killed_decommission(self, c, step, lives):
        ctx, executor = self.ctx, self.executor
        rule = _kill_point(step, lives)
        c.obs["kill_rule"] = rule
        c.obs["owner_before"] = _owner(self.observer)
        _proxy({"hold": rule})
        outcome = {}

        def run():
            try:
                outcome["report"] = _drop_member(executor)
            except Exception as e:  # the kill drops the connection
                outcome["error"] = f"{type(e).__name__}: {getattr(e, 'body', e)}"[:500]

        thread = threading.Thread(target=run, daemon=True)
        thread.start()

        def parked():
            hold = _proxy_get("/stats")["hold"]
            if hold["state"] != "held":
                return None
            first = hold["parked"][0]
            if rule["when"] == "after" and "upstream_status" not in first:
                return None
            return first
        first = _wait(lambda: parked() or (not thread.is_alive() and "finished"), _HOLD_WAIT_S, 0.1)
        if not isinstance(first, dict):
            _proxy({"hold_release": True})
            thread.join(timeout=60)
            c.check(f"the decommission reaches the kill point after step {step}", "a parked request",
                    f"{first}; outcome={outcome}", False)
            c.save()
            raise RuntimeError(f"cycle K{step}: kill point not reached")
        _docker("kill", _EXECUTOR)
        c.obs["executor_killed_at"] = time.time()
        c.obs["parked"] = _proxy_get("/stats")["hold"]["parked"]
        _proxy({"hold_release": True})
        thread.join(timeout=60)
        c.obs["killed_run_outcome"] = outcome
        ctx.log(f"S47 K{step}: killed ch4 with {first['method']} {first['path']} parked ({first['when']} the store)")
        c.check(f"the killed run printed no result (killed after step {step})", "no report",
                outcome, "report" not in outcome)
        c.obs["owner_after_kill"] = _owner(self.observer)
        c.obs["mount_after_kill"] = _mount(self.observer)

    def _recover_after_kill(self, c, step):
        observer, executor = self.observer, self.executor
        owner = c.obs["owner_after_kill"]
        before = c.obs["owner_before"]
        tombstoned = bool(owner.get("retired_at_ms")) and int(owner["owner_life"]) > int(before["owner_life"])
        verified = bool(owner.get("retirement_verified"))
        expected_tombstone = step >= 2
        c.check("the owner shows what the steps before the kill wrote",
                f"tombstoned={expected_tombstone} verified={step == 6}",
                f"tombstoned={tombstoned} verified={verified} owner={owner}",
                tombstoned == expected_tombstone and verified == (step == 6))

        _docker("start", _EXECUTOR)
        c.check("the executor restarts after the kill", "ping", _EXECUTOR, _wait_ping(executor))
        c.check("the executor's own data survives its kill", self.executor_checksum,
                _checksum(executor, _EXECUTOR_TABLE), _checksum(executor, _EXECUTOR_TABLE) == self.executor_checksum)

        if tombstoned and not verified:
            self._check_start_refused(c)

        # The rerun rule: no run in progress (the executor was killed), the row shows `retired`, the tombstone
        # is unverified, and no server of the id ran when the tombstone appeared (the victim was dead).
        # A killed run's lease reads `live`, then `expired`, then `fenced` once GC fences it, or `retired` over
        # a tombstone.
        settled = "retired" if tombstoned else "fenced"
        _wait(lambda: _victim_state(observer) == settled, _FENCE_WAIT_S, 3)
        c.obs["victim_state_before_rerun"] = _victim_state(observer)
        retired = c.obs["victim_state_before_rerun"] == "retired"
        rule_holds = retired and tombstoned and not verified
        c.obs["rerun_rule_holds"] = rule_holds
        if step == 1:
            # No tombstone: the id is not retired and the rule does not apply. The member is known dead, so a
            # new run decommissions it.
            c.check("after a kill before the tombstone the id is not retired", "fenced",
                    c.obs["victim_state_before_rerun"], c.obs["victim_state_before_rerun"] == "fenced")
            report = self._decommission(executor)
            c.obs["rerun_report"] = report
            c.check("a new decommission after the kill retires the member", "member_retired=1",
                    report, report.get("member_retired") == 1 and not report.get("warnings"))
        elif step == 6:
            c.check("after a kill past the verified mark the rule says no rerun", "retired, verified",
                    f"state={c.obs['victim_state_before_rerun']} verified={verified}", retired and not rule_holds)
        else:
            c.check("the rerun rule holds after the kill", "retired, unverified tombstone",
                    f"state={c.obs['victim_state_before_rerun']} owner={owner}", rule_holds)
            report = self._decommission(executor)
            c.obs["rerun_report"] = report
            c.check("the rerun converges", "member_retired=1, no warnings", report,
                    report.get("member_retired") == 1 and not report.get("warnings"))
        owner_now = _owner(observer)
        c.obs["owner_after_rerun"] = owner_now
        c.check("the owner carries a verified retirement", "retirement_verified=true", owner_now,
                bool(owner_now.get("retired_at_ms")) and bool(owner_now.get("retirement_verified")))

    def _check_start_refused(self, c):
        """A start of the victim while the tombstone is unverified refuses with `decommission is not finished`."""
        offset = _log_size("ch3")
        _docker("start", _VICTIM)
        came_up = _wait(lambda: not _container_running(_VICTIM) or _NOT_FINISHED_RE.search(_log_since("ch3", offset)), 120, 3)
        text = _log_since("ch3", offset)
        match = _NOT_FINISHED_RE.search(text)
        c.obs["refused_start_running"] = _container_running(_VICTIM)
        c.obs["refused_start_log"] = text[max(0, match.start() - 600):match.end() + 600] if match else text[-2000:]
        c.check("a victim start mid-decommission is refused: decommission is not finished", "refusal in the log",
                "refused" if match else "no refusal", bool(match))
        if _container_running(_VICTIM):
            _docker("kill", _VICTIM, check=False)
        c.obs["refused_start_served"] = bool(came_up) and self.victim.ping(timeout=2)

    def _rejoin(self, c, old_uuid, new_uuid):
        observer, victim = self.observer, self.victim
        rounds = 0
        deadline = time.monotonic() + _FENCE_WAIT_S
        while _victim_catalog(observer) and time.monotonic() < deadline:
            _gc_round_on_survivors(self.survivors)
            rounds += 1
            time.sleep(2)
        c.obs["gc_rounds_before_rejoin"] = rounds
        c.check("GC removes the victim's catalog rows", "0 rows", _victim_catalog(observer), not _victim_catalog(observer))
        attempts = []
        joined = None
        for _ in range(5):
            offset = _log_size("ch3")
            _docker("start", _VICTIM)
            up = _wait_ping(victim, 120)
            text = _log_since("ch3", offset)
            m = _REJOINED_RE.search(text)
            attempts.append({"up": up, "rejoined": m.group(1) if m else None,
                             "refusal": (re.search(r"previous life remains[^\n]{0,300}", text) or [None])[0]})
            if up and m:
                joined = m.group(1)
                break
            _docker("kill", _VICTIM, check=False)
            _gc_round_on_survivors(self.survivors)
        c.obs["rejoin_attempts"] = attempts
        if not c.check("the restarted victim rejoins", "rejoined log line and ping", attempts, joined is not None):
            return
        rows = _victim_mount_rows(observer)
        c.obs["victim_after_rejoin"] = rows
        uuid_now = rows[0]["server_uuid"] if rows else None
        c.check("system.cas_mounts shows the rejoined victim live", "live", rows,
                len(rows) == 1 and rows[0]["state"] == "live")
        c.check("the rejoin keeps or replaces the uuid as configured",
                "new uuid" if new_uuid else f"same uuid {old_uuid}", f"{old_uuid} -> {uuid_now}",
                (uuid_now != old_uuid) if new_uuid else (uuid_now == old_uuid))
        counts = {t: victim.scalar(f"SELECT count() FROM {t}") for t in self.tables}
        c.check("the victim's old tables attach empty", "0 rows each", counts, all(v == "0" for v in counts.values()))
        victim.command(f"INSERT INTO {self.tables[0]} SELECT number, 'rejoined' FROM numbers(100)")
        c.check("inserts on the rejoined victim work", "100", victim.scalar(f"SELECT count() FROM {self.tables[0]}"),
                victim.scalar(f"SELECT count() FROM {self.tables[0]}") == "100")
        victim.command(f"TRUNCATE TABLE {self.tables[0]}")
