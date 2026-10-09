import json
import os
import subprocess
import time
import urllib.request

import pytest
import urllib3
from minio import Minio
from urllib3.util import Timeout as _Urllib3Timeout

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)

DISK = "disk_cas_matrix"
POOLS = {
    True: {"node": "matrix_node_gc", "prefix": "cas_matrix_gc", "srid": "itest-cas-matrix-gc"},
    False: {"node": "matrix_node_nogc", "prefix": "cas_matrix_nogc", "srid": "itest-cas-matrix-nogc"},
}
# The disks run a 10 s lease renewed every 2 s, so the GC peer fences a slot whose token held for 12.5 s;
# the defaults (30 s, 10 s) give 41.5 s. `renewal_puts` counts the renewal's conditional PUTs, not the
# resolve reads between them.
# Every fault scenario: the proxy rule, or `None` for a stopped writer, and how long it holds.
SCENARIOS = {
    "put_503_90pct_5min": ({"rate": 0.9, "modes": ["503"], "methods": ["PUT"]}, 300),
    "access_lost_60s": ({"rate": 1.0, "modes": ["reset"], "methods": ["GET", "PUT", "HEAD", "POST", "DELETE"]}, 60),
    "sigstop_writer_60s": (None, 60),
}
RESULTS = {}


def _control(base_url, path, patch=None, timeout=10):
    if patch is None:
        request = urllib.request.Request("{}{}".format(base_url, path))
    else:
        request = urllib.request.Request(
            "{}{}".format(base_url, path),
            data=json.dumps(patch).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode())


def _wait_until(probe, timeout, interval=0.5):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = probe()
        except Exception as e:  # a query against a recovering server may fail; keep probing
            last = e
        if last is True or (last and not isinstance(last, Exception)):
            return last
        time.sleep(interval)
    raise AssertionError("condition did not become true within {}s; last={!r}".format(timeout, last))


def _rustfs_slot(prefix, srid):
    client = Minio(
        "{}:{}".format(cluster.rustfs_ip, cluster.rustfs_port),
        access_key=cluster.rustfs_access_key,
        secret_key=cluster.rustfs_secret_key,
        secure=False,
        http_client=urllib3.PoolManager(cert_reqs="CERT_NONE", timeout=_Urllib3Timeout(connect=5, read=5)),
    )
    response = client.get_object(cluster.rustfs_bucket, "{}/gc/server-roots/{}/mount".format(prefix, srid))
    try:
        return json.loads(response.read().decode().splitlines()[1])
    finally:
        response.close()
        response.release_conn()


def _local_lease(node, timeout=10):
    row = node.query(
        "SELECT lifecycle, lifecycle_reason, writer_epoch FROM system.cas_mounts "
        "WHERE disk = '{}' AND is_local = 1 FORMAT TSV".format(DISK),
        timeout=timeout,
    ).rstrip("\n")
    lifecycle, reason, epoch = row.split("\t")
    return lifecycle, reason, int(epoch)


def _renewal_attempts(node):
    return int(node.query("SELECT value FROM system.events WHERE event = 'CASMountRenewalAttempts'").strip() or 0)


def _log_count_since_start(node, pattern):
    script = (
        "start_line=$(grep -n 'Starting ClickHouse' /var/log/clickhouse-server/clickhouse-server.log | tail -1 | cut -d: -f1); "
        "tail -n +\"${start_line:-1}\" /var/log/clickhouse-server/clickhouse-server.log | grep -c -- '%s' || true"
    ) % pattern
    return int(node.exec_in_container(["bash", "-c", script]).strip())


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    for gc_peer, pool in POOLS.items():
        suffix = "gc" if gc_peer else "nogc"
        cluster.add_instance(pool["node"], main_configs=["configs/node_{}.xml".format(suffix)], with_rustfs=True, stay_alive=True)
        cluster.add_instance("matrix_peer_{}".format(suffix), main_configs=["configs/peer_{}.xml".format(suffix)], with_rustfs=True)
    cluster.base_cmd.extend(["--file", os.path.join(os.path.dirname(__file__), "docker_compose_proxy.yml")])
    control_url = None
    try:
        cluster.start()
        binding = subprocess.check_output(cluster.base_cmd + ["port", "s3proxy", "8474"], text=True).strip()
        control_url = "http://{}".format(binding)
        _wait_until(lambda: _control(control_url, "/healthz"), timeout=30)
        _control(control_url, "/config", {"reset": True})
        for pool in POOLS.values():
            cluster.instances[pool["node"]].query(
                "CREATE TABLE matrix_probe (id UInt64, payload String) "
                "ENGINE = MergeTree ORDER BY id SETTINGS storage_policy = 'cas_matrix'"
            )
        yield control_url
    finally:
        if control_url is not None:
            try:
                _control(control_url, "/config", {"reset": True})
            except Exception:
                pass
        try:
            with open(os.path.join(cluster.instances_dir, "cas_matrix_results.json"), "w") as out:
                json.dump(RESULTS, out, indent=2, sort_keys=True)
        except Exception:
            pass
        cluster.shutdown()


@pytest.mark.parametrize("gc_peer", [True, False], ids=["gc_peer", "no_gc_peer"])
@pytest.mark.parametrize("scenario", list(SCENARIOS))
def test_the_writer_survives_the_fault(start_cluster, gc_peer, scenario):
    control_url = start_cluster
    pool = POOLS[gc_peer]
    node = cluster.instances[pool["node"]]
    rule, hold_s = SCENARIOS[scenario]
    _control(control_url, "/config", {"reset": True})
    _wait_until(lambda: _local_lease(node)[:2] == ("live", ""), timeout=60)
    pid = node.get_process_pid("clickhouse")
    epoch_before = _local_lease(node)[2]
    attempts_before = _renewal_attempts(node)
    reacquires_before = _log_count_since_start(node, "reacquired under writer_epoch")

    fences_seen = 0
    if rule is None:
        node.exec_in_container(["bash", "-c", "kill -STOP {}".format(pid)], user="root")
    else:
        _control(control_url, "/config", dict(rule, path_substring="/test/{}/".format(pool["prefix"]), seed=1500 + len(RESULTS)))
    try:
        deadline = time.monotonic() + hold_s
        while time.monotonic() < deadline:
            try:
                fences_seen += bool(_rustfs_slot(pool["prefix"], pool["srid"])["gc_fenced"])
            except Exception:
                pass
            time.sleep(1)
    finally:
        if rule is None:
            node.exec_in_container(["bash", "-c", "kill -CONT {}".format(pid)], user="root")
        else:
            _control(control_url, "/config", {"reset": True})
    fault_end = time.monotonic()

    _wait_until(lambda: _local_lease(node)[:2] == ("live", ""), timeout=120)
    live_after_s = time.monotonic() - fault_end
    row_id = 1000 + len(RESULTS)
    _wait_until(lambda: node.query("INSERT INTO matrix_probe VALUES ({}, '{}')".format(row_id, scenario)) == "", timeout=120)
    insert_after_s = time.monotonic() - fault_end
    epoch_after = _local_lease(node)[2]
    reacquires = _log_count_since_start(node, "reacquired under writer_epoch") - reacquires_before

    RESULTS["{}/{}".format(scenario, "gc_peer" if gc_peer else "no_gc_peer")] = {
        "fences_seen": fences_seen,
        "epoch_before": epoch_before,
        "epoch_after": epoch_after,
        "reacquires": reacquires,
        "seconds_to_live": round(live_after_s, 1),
        "seconds_to_first_insert": round(insert_after_s, 1),
        "renewal_puts": _renewal_attempts(node) - attempts_before,
        "pid_unchanged": node.get_process_pid("clickhouse") == pid,
    }
    print("cas matrix {}: {}".format(scenario, RESULTS), flush=True)

    assert node.get_process_pid("clickhouse") == pid
    # A fault that outlasts the lease can be fenced more than once; each fence costs one epoch, and every
    # epoch step is a reacquire the server logged.
    assert epoch_after == epoch_before + reacquires, RESULTS
    if fences_seen:
        assert reacquires >= 1, RESULTS
    if not gc_peer:
        assert reacquires == 0, RESULTS
    assert node.query("SELECT count() FROM matrix_probe WHERE id = {}".format(row_id)).strip() == "1"
    assert _log_count_since_start(node, "whole-chain remount") == 0


def test_the_gc_peer_fenced_in_some_scenario():
    with_peer = {k: v for k, v in RESULTS.items() if k.endswith("/gc_peer")}
    assert len(with_peer) == len(SCENARIOS), RESULTS
    assert any(v["fences_seen"] for v in with_peer.values()), (
        "no scenario with the GC peer saw a fence: the fault windows did not outlast the lease", RESULTS)
