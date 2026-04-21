import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.xml"],
    user_configs=["configs/users.xml"],
    with_zookeeper=True,
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.xml"],
    user_configs=["configs/users.xml"],
    with_zookeeper=True,
    stay_alive=True,
)
# Third node without the cluster-variables ZK path — used to assert that
# cluster DDL errors cleanly when the config is missing.
node_no_zk = cluster.add_instance(
    "node_no_zk",
    main_configs=["configs/config_no_zk_path.xml"],
    user_configs=["configs/users.xml"],
    with_zookeeper=True,
)

nodes = [node1, node2]


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _drop_all_cluster_variables(node):
    rows = node.query(
        "SELECT name FROM system.custom_variables WHERE scope = 'cluster'"
    ).strip()
    for name in [r for r in rows.splitlines() if r]:
        node.query(f"DROP CLUSTER VARIABLE IF EXISTS {name}")


@pytest.fixture
def cleanup(started_cluster):
    yield
    # Best-effort cleanup from every node that might see the entry;
    # propagation between tests should not leak state.
    for node in nodes:
        try:
            _drop_all_cluster_variables(node)
        except Exception:
            pass


# -------- Regression locks: already green after step 2 --------

def test_zk_config_required(started_cluster, cleanup):
    err = node_no_zk.query_and_get_error("CREATE CLUSTER VARIABLE nope AS 1")
    assert "custom_variables_zookeeper_path" in err
    assert "BAD_ARGUMENTS" in err or "36" in err


def test_single_node_create_read_drop(started_cluster, cleanup):
    node1.query("CREATE CLUSTER VARIABLE sn_foo AS toUInt64(7)")
    assert node1.query("SELECT getVariable('cluster.sn_foo')").strip() == "7"
    node1.query("DROP CLUSTER VARIABLE sn_foo")
    err = node1.query_and_get_error("SELECT getVariable('cluster.sn_foo')")
    assert "UNKNOWN_IDENTIFIER" in err


# -------- Step 3: coordinator + watches --------

def test_cross_node_discovery(started_cluster, cleanup):
    node1.query("CREATE CLUSTER VARIABLE xn_foo AS toUInt64(42)")
    assert_eq_with_retry(
        node2,
        "SELECT getVariable('cluster.xn_foo')",
        "42\n",
        retry_count=40,
        sleep_time=0.25,
    )


def test_cross_node_drop(started_cluster, cleanup):
    node1.query("CREATE CLUSTER VARIABLE xd_foo AS toUInt64(1)")
    assert_eq_with_retry(node2, "SELECT getVariable('cluster.xd_foo')", "1\n")

    node1.query("DROP CLUSTER VARIABLE xd_foo")

    def no_longer_exists():
        err = node2.query_and_get_error("SELECT getVariable('cluster.xd_foo')")
        return "UNKNOWN_IDENTIFIER" in err

    deadline = time.time() + 10
    while time.time() < deadline:
        if no_longer_exists():
            return
        time.sleep(0.25)
    pytest.fail("node2 still sees cluster.xd_foo after DROP on node1")


def test_restart_picks_up_existing(started_cluster, cleanup):
    node1.query("CREATE CLUSTER VARIABLE rs_foo AS toUInt64(100)")
    assert_eq_with_retry(node2, "SELECT getVariable('cluster.rs_foo')", "100\n")

    node2.stop_clickhouse()
    node2.start_clickhouse()

    # On boot, node2 must load from ZK — no watch event to rely on.
    assert_eq_with_retry(
        node2,
        "SELECT getVariable('cluster.rs_foo')",
        "100\n",
        retry_count=40,
        sleep_time=0.25,
    )


def test_create_or_replace_propagates(started_cluster, cleanup):
    node1.query("CREATE CLUSTER VARIABLE repl_v AS toUInt64(1)")
    assert_eq_with_retry(node2, "SELECT getVariable('cluster.repl_v')", "1\n")

    node1.query("CREATE OR REPLACE CLUSTER VARIABLE repl_v AS toUInt64(2)")
    assert_eq_with_retry(node2, "SELECT getVariable('cluster.repl_v')", "2\n")


def test_system_table_sees_cluster_rows(started_cluster, cleanup):
    node1.query("CREATE CLUSTER VARIABLE st_foo AS toUInt64(5)")
    assert_eq_with_retry(node2, "SELECT getVariable('cluster.st_foo')", "5\n")

    for node in nodes:
        row = node.query(
            "SELECT name, scope, value, type FROM system.custom_variables "
            "WHERE name = 'st_foo' AND scope = 'cluster'"
        ).strip()
        assert row == "st_foo\tcluster\t5\tUInt64", f"{node.name}: {row!r}"


# -------- Step 4: leader election + REFRESH --------

def test_refresh_clause_accepted(started_cluster, cleanup):
    # After step 4 this must stop returning NOT_IMPLEMENTED.
    node1.query(
        "CREATE CLUSTER VARIABLE rc_wm REFRESH EVERY 1 SECOND AS toUInt64(now())"
    )


def test_refresh_updates_value(started_cluster, cleanup):
    node1.query(
        "CREATE CLUSTER VARIABLE rv_wm REFRESH EVERY 1 SECOND AS toUInt64(now())"
    )
    assert_eq_with_retry(
        node2,
        "SELECT getVariable('cluster.rv_wm') > 0",
        "1\n",
        retry_count=40,
        sleep_time=0.25,
    )

    first = int(node1.query("SELECT getVariable('cluster.rv_wm')").strip())
    time.sleep(3)
    later_n1 = int(node1.query("SELECT getVariable('cluster.rv_wm')").strip())
    later_n2 = int(node2.query("SELECT getVariable('cluster.rv_wm')").strip())
    assert later_n1 > first
    assert later_n2 > first


def test_refresh_single_leader_per_tick(started_cluster, cleanup):
    """Soft invariant: consecutive ticks should mostly be written by the same host
    (proves we're not hot-swapping the leader every tick)."""
    node1.query(
        "CREATE CLUSTER VARIABLE sl_wm REFRESH EVERY 1 SECOND AS toUInt64(now())"
    )
    assert_eq_with_retry(node2, "SELECT getVariable('cluster.sl_wm') > 0", "1\n")

    samples = []
    for _ in range(8):
        host = node1.query(
            "SELECT last_update_hostname FROM system.custom_variables "
            "WHERE name = 'sl_wm' AND scope = 'cluster'"
        ).strip()
        samples.append(host)
        time.sleep(1)
    # Most samples should share a host. Allow minority drift for failover cases.
    most_common_count = max(samples.count(h) for h in set(samples))
    assert most_common_count >= len(samples) - 2, samples


def test_system_refresh_variable(started_cluster, cleanup):
    node1.query(
        "CREATE CLUSTER VARIABLE sr_wm REFRESH EVERY 1 YEAR AS toUInt64(now())"
    )
    assert_eq_with_retry(node2, "SELECT getVariable('cluster.sr_wm') > 0", "1\n")
    first = int(node2.query("SELECT getVariable('cluster.sr_wm')").strip())
    time.sleep(1)
    node1.query("SYSTEM REFRESH CLUSTER VARIABLE sr_wm")
    assert_eq_with_retry(
        node2,
        f"SELECT getVariable('cluster.sr_wm') > {first}",
        "1\n",
        retry_count=40,
        sleep_time=0.25,
    )


def test_refresh_failover(started_cluster, cleanup):
    node1.query(
        "CREATE CLUSTER VARIABLE fo_wm REFRESH EVERY 2 SECOND AS toUInt64(now())"
    )
    assert_eq_with_retry(node2, "SELECT getVariable('cluster.fo_wm') > 0", "1\n")

    node1.stop_clickhouse(kill=True)
    try:
        # Wait out the ephemeral-lock holder's ZK session. Default session timeout
        # for Kazoo-style helpers in this repo is a few seconds; budget generously.
        deadline = time.time() + 60
        seen = None
        while time.time() < deadline:
            v = node2.query("SELECT getVariable('cluster.fo_wm')").strip()
            if seen is None:
                seen = v
            elif v != seen:
                return  # value moved; some replica is refreshing now
            time.sleep(1)
        pytest.fail("no refresh observed on node2 after node1 was killed")
    finally:
        node1.start_clickhouse()
