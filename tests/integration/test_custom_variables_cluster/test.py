import threading
import time

import pytest

from helpers.cluster import ClickHouseCluster, ZOOKEEPER_CONTAINERS
from helpers.network import PartitionManager
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
    """Every tick exactly one node writes to ZK (ephemeral lock serialises writers).
    The winner may alternate between ticks — we just assert every observed
    hostname is one of the real cluster members."""
    node1.query(
        "CREATE CLUSTER VARIABLE sl_wm REFRESH EVERY 1 SECOND AS toUInt64(now())"
    )
    assert_eq_with_retry(node2, "SELECT getVariable('cluster.sl_wm') > 0", "1\n")

    valid_hosts = {node1.hostname, node2.hostname}
    samples = []
    for _ in range(8):
        host = node1.query(
            "SELECT last_update_hostname FROM system.custom_variables "
            "WHERE name = 'sl_wm' AND scope = 'cluster'"
        ).strip()
        samples.append(host)
        time.sleep(1)

    assert all(h in valid_hosts for h in samples), (samples, valid_hosts)


def test_system_refresh_variable(started_cluster, cleanup):
    node1.query(
        "CREATE CLUSTER VARIABLE sr_wm REFRESH EVERY 1 YEAR AS toUInt64(now())"
    )
    assert_eq_with_retry(node2, "SELECT getVariable('cluster.sr_wm') > 0", "1\n")
    first = int(node2.query("SELECT getVariable('cluster.sr_wm')").strip())
    time.sleep(1)
    node1.query("SYSTEM REFRESH VARIABLE cluster.sr_wm")
    assert_eq_with_retry(
        node2,
        f"SELECT getVariable('cluster.sr_wm') > {first}",
        "1\n",
        retry_count=40,
        sleep_time=0.25,
    )


def test_refreshable_create_publishes_initialized_entry(started_cluster, cleanup):
    """CREATE pauses after in-memory publish. SYSTEM REFRESH during that pause
    must not report "not refreshable"."""

    errors = []
    saw_refresh_success = False
    last_refresh_error = ""

    def create_variable():
        try:
            node1.query(
                "CREATE CLUSTER VARIABLE init_pub REFRESH EVERY 1 YEAR AS toUInt64(now())"
            )
        except Exception as exc:
            errors.append(exc)

    node1.query("SYSTEM ENABLE FAILPOINT custom_variable_create_after_publish_pause")
    creator = threading.Thread(target=create_variable)
    creator.start()
    try:
        deadline = time.time() + 30
        while time.time() < deadline:
            if errors:
                break

            try:
                node1.query("SYSTEM REFRESH VARIABLE cluster.init_pub")
                saw_refresh_success = True
                break
            except Exception as exc:
                message = str(exc)
                last_refresh_error = message
                if "not refreshable" in message:
                    raise
                if "UNKNOWN_IDENTIFIER" not in message:
                    # Keep polling while CREATE is still wiring the entry.
                    pass

            time.sleep(0.1)
    finally:
        node1.query("SYSTEM DISABLE FAILPOINT custom_variable_create_after_publish_pause")
        creator.join(timeout=30)

    assert not creator.is_alive()
    if errors:
        raise errors[0]
    assert saw_refresh_success, last_refresh_error or "SYSTEM REFRESH VARIABLE never succeeded during paused CREATE"

    assert_eq_with_retry(
        node2,
        "SELECT getVariable('cluster.init_pub') > 0",
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


# -------- Pass 2: races, ZK disruption, discovery churn --------


def test_duplicate_create_cluster_race(started_cluster, cleanup):
    """Two nodes CREATE the same cluster variable concurrently.
    Exactly one succeeds; the other sees FILE_ALREADY_EXISTS.
    Both end up seeing the winner's value."""

    results = {}

    def create(node, tag):
        try:
            node.query(
                f"CREATE CLUSTER VARIABLE race_cv AS toUInt64({tag})"
            )
            results[tag] = "ok"
        except Exception as exc:
            results[tag] = str(exc)

    t1 = threading.Thread(target=create, args=(node1, 1))
    t2 = threading.Thread(target=create, args=(node2, 2))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    outcomes = list(results.values())
    ok_count = sum(1 for o in outcomes if o == "ok")
    # The loser either hits FILE_ALREADY_EXISTS (if the winner's ZK create
    # landed first) or the per-variable publish lock (if the winner is still
    # writing definition + value atomically). Either outcome proves the two
    # CREATEs were serialised.
    err_count = sum(
        1 for o in outcomes
        if "FILE_ALREADY_EXISTS" in o
        or "currently creating or refreshing" in o
    )
    assert ok_count == 1 and err_count == 1, results

    winner_tag = next(t for t, o in results.items() if o == "ok")
    expected = str(winner_tag)
    for node in nodes:
        assert_eq_with_retry(
            node,
            "SELECT getVariable('cluster.race_cv')",
            expected + "\n",
            retry_count=40,
            sleep_time=0.25,
        )


def test_create_or_replace_while_refreshing(started_cluster, cleanup):
    """OR REPLACE in the middle of a refresh cycle must converge both nodes
    on the new value and drop the REFRESH schedule."""

    node1.query(
        "CREATE CLUSTER VARIABLE repl_race REFRESH EVERY 1 SECOND AS toUInt64(now())"
    )
    assert_eq_with_retry(node2, "SELECT getVariable('cluster.repl_race') > 0", "1\n")

    # Let several ticks happen so the scheduler is genuinely active.
    time.sleep(2)

    node2.query(
        "CREATE OR REPLACE CLUSTER VARIABLE repl_race AS toUInt64(42)"
    )

    for node in nodes:
        assert_eq_with_retry(
            node,
            "SELECT getVariable('cluster.repl_race')",
            "42\n",
            retry_count=40,
            sleep_time=0.25,
        )

    # Stable at 42 across a window that would have covered several refresh ticks
    # if the schedule had survived.
    time.sleep(3)
    for node in nodes:
        assert node.query(
            "SELECT getVariable('cluster.repl_race')"
        ).strip() == "42"


def test_create_cluster_rolls_back_on_value_store_failure(started_cluster, cleanup):
    """If initial value publication fails, CREATE must fail and leave no
    definition-only ghost variable in Keeper."""

    node1.query("SYSTEM ENABLE FAILPOINT custom_variables_cluster_store_value_fail_once")
    try:
        err = node1.query_and_get_error(
            "CREATE CLUSTER VARIABLE publish_fail_cv AS toUInt64(11)"
        )
    finally:
        # ONCE failpoints auto-disable after trigger, but disable explicitly in
        # case CREATE failed before reaching the injection point.
        node1.query("SYSTEM DISABLE FAILPOINT custom_variables_cluster_store_value_fail_once")

    assert (
        "Injected failure while storing cluster variable" in err
        or "KEEPER_EXCEPTION" in err
    ), err

    for node in nodes:

        def missing(n=node):
            error = n.query_and_get_error(
                "SELECT getVariable('cluster.publish_fail_cv')"
            )
            return "UNKNOWN_IDENTIFIER" in error

        deadline = time.time() + 10
        while time.time() < deadline and not missing():
            time.sleep(0.25)
        assert missing(), f"{node.name} still sees cluster.publish_fail_cv"

    node1.query("CREATE CLUSTER VARIABLE publish_fail_cv AS toUInt64(11)")
    for node in nodes:
        assert_eq_with_retry(
            node,
            "SELECT getVariable('cluster.publish_fail_cv')",
            "11\n",
            retry_count=40,
            sleep_time=0.25,
        )


def test_cluster_refresh_failure_flap(started_cluster, cleanup):
    """Refresh fails on the leader after its dependency is dropped; the last-good
    value + last_error propagate via ZK to the peer; recovery clears the error."""

    for node in nodes:
        node.query("DROP TABLE IF EXISTS default.flap_src SYNC")
        node.query("CREATE TABLE default.flap_src (x UInt8) ENGINE = Memory")
        node.query("INSERT INTO default.flap_src VALUES (42)")

    node1.query(
        "CREATE CLUSTER VARIABLE flap_cv REFRESH EVERY 1 SECOND "
        "AS (SELECT max(x) FROM default.flap_src)"
    )
    assert_eq_with_retry(node2, "SELECT getVariable('cluster.flap_cv')", "42\n")

    for node in nodes:
        node.query("DROP TABLE default.flap_src SYNC")

    def is_valid_flipped(node):
        row = node.query(
            "SELECT has_value, is_valid, coalesce(last_error,'') != '' "
            "FROM system.custom_variables "
            "WHERE name = 'flap_cv' AND scope = 'cluster'"
        ).strip()
        return row == "1\t0\t1"

    deadline = time.time() + 30
    while time.time() < deadline:
        if all(is_valid_flipped(n) for n in nodes):
            break
        time.sleep(0.5)
    else:
        pytest.fail(
            "flap did not propagate: "
            + "; ".join(
                f"{n.name}: "
                + n.query(
                    "SELECT has_value, is_valid, last_error "
                    "FROM system.custom_variables WHERE name='flap_cv'"
                ).strip()
                for n in nodes
            )
        )

    # Last-good value is still visible on both nodes.
    for node in nodes:
        assert node.query(
            "SELECT getVariable('cluster.flap_cv')"
        ).strip() == "42"

    # Recovery: recreate table + data, expect is_valid back to 1 within a few ticks.
    for node in nodes:
        node.query("CREATE TABLE default.flap_src (x UInt8) ENGINE = Memory")
        node.query("INSERT INTO default.flap_src VALUES (7)")

    def recovered(node):
        row = node.query(
            "SELECT is_valid, coalesce(last_error,'') = '' "
            "FROM system.custom_variables "
            "WHERE name = 'flap_cv' AND scope = 'cluster'"
        ).strip()
        return row == "1\t1"

    deadline = time.time() + 30
    while time.time() < deadline:
        if all(recovered(n) for n in nodes):
            for node in nodes:
                node.query("DROP TABLE IF EXISTS default.flap_src SYNC")
            return
        time.sleep(0.5)
    pytest.fail("recovery did not happen in time")


def test_zk_disconnect_reads_continue(started_cluster, cleanup):
    """Partition node2 from ZK. getVariable on node2 must keep serving the
    cached value. After the partition heals, a new write from node1 reaches
    node2 again."""

    node1.query(
        "CREATE CLUSTER VARIABLE zkd_cv AS toUInt64(100)"
    )
    assert_eq_with_retry(node2, "SELECT getVariable('cluster.zkd_cv')", "100\n")

    with PartitionManager() as pm:
        pm.drop_instance_zk_connections(node2)
        # Cached reads keep working even while ZK is unreachable.
        for _ in range(5):
            assert node2.query(
                "SELECT getVariable('cluster.zkd_cv')"
            ).strip() == "100"
            time.sleep(0.5)

    # After partition heals, a new write on node1 should propagate.
    node1.query(
        "CREATE OR REPLACE CLUSTER VARIABLE zkd_cv AS toUInt64(101)"
    )
    assert_eq_with_retry(
        node2,
        "SELECT getVariable('cluster.zkd_cv')",
        "101\n",
        retry_count=80,
        sleep_time=0.25,
    )


def test_zk_session_loss_re_enumerate(started_cluster, cleanup):
    """Stop Keeper long enough for the ZK session to expire, restart it, and
    assert the coordinator on both nodes re-enumerates — a fresh CREATE on
    node1 must reach node2 without a node restart."""

    node1.query(
        "CREATE CLUSTER VARIABLE sess_cv AS toUInt64(1)"
    )
    assert_eq_with_retry(node2, "SELECT getVariable('cluster.sess_cv')", "1\n")

    cluster.stop_zookeeper_nodes(ZOOKEEPER_CONTAINERS)
    # Session timeout for the integration fixture is ~30 s; wait past it.
    time.sleep(35)
    cluster.start_zookeeper_nodes(ZOOKEEPER_CONTAINERS)

    # After the session returns, the coordinator should reconnect and a new
    # CREATE on node1 must reach node2.
    node1.query_with_retry(
        "CREATE OR REPLACE CLUSTER VARIABLE sess_cv AS toUInt64(2)",
        retry_count=40,
        sleep_time=1.0,
    )
    assert_eq_with_retry(
        node2,
        "SELECT getVariable('cluster.sess_cv')",
        "2\n",
        retry_count=60,
        sleep_time=1.0,
    )


def test_restart_during_refresh_no_leak(started_cluster, cleanup):
    """Kill the node most likely to hold the ephemeral refresh lock; after
    restart it should rejoin cleanly and the variable's value should keep
    advancing without any node getting stuck on a stale hostname."""

    node1.query(
        "CREATE CLUSTER VARIABLE rlk_cv REFRESH EVERY 1 SECOND AS toUInt64(now())"
    )
    assert_eq_with_retry(node2, "SELECT getVariable('cluster.rlk_cv') > 0", "1\n")

    node1.stop_clickhouse(kill=True)
    try:
        time.sleep(3)
        # While node1 is down, node2 must keep refreshing.
        before = int(node2.query("SELECT getVariable('cluster.rlk_cv')").strip())
        time.sleep(3)
        after = int(node2.query("SELECT getVariable('cluster.rlk_cv')").strip())
        assert after > before, (before, after)
    finally:
        node1.start_clickhouse()

    # After restart node1 sees the variable and the value continues to advance.
    assert_eq_with_retry(node1, "SELECT getVariable('cluster.rlk_cv') > 0", "1\n")
    v1 = int(node1.query("SELECT getVariable('cluster.rlk_cv')").strip())
    time.sleep(3)
    v2 = int(node1.query("SELECT getVariable('cluster.rlk_cv')").strip())
    assert v2 > v1, (v1, v2)


def test_or_replace_drops_refresh_schedule_on_peer(started_cluster, cleanup):
    """Reported by codex review. The peer's coordinator fast-path skipped rebuilds
    when the AST hash of the expression matched. If OR REPLACE only removed the
    REFRESH clause (keeping the same expression), the peer kept its old scheduler
    alive and continued publishing refreshed values via ZK, violating the new
    definition that says 'no refresh'."""

    node1.query(
        "CREATE CLUSTER VARIABLE rs_strip REFRESH EVERY 1 SECOND AS toUInt64(now())"
    )
    assert_eq_with_retry(node2, "SELECT getVariable('cluster.rs_strip') > 0", "1\n")

    # Let ticks run on both sides so both nodes have a live refresh scheduler.
    time.sleep(2)

    # Same expression, REFRESH removed. Hash of expression is unchanged — the
    # fast-path used to short-circuit and leave the old scheduler alive.
    node1.query(
        "CREATE OR REPLACE CLUSTER VARIABLE rs_strip AS toUInt64(now())"
    )

    # Give any lingering scheduler ticks a chance to fire.
    time.sleep(3)

    # After OR REPLACE the value is whatever CREATE evaluated once — not a
    # moving target. Sample on each node across another window; value must be
    # identical on each sample.
    for node in nodes:
        snapshots = []
        for _ in range(4):
            snapshots.append(node.query("SELECT getVariable('cluster.rs_strip')").strip())
            time.sleep(1)
        assert len(set(snapshots)) == 1, (node.name, snapshots)

    # system.custom_variables.refresh_interval is NULL for a non-refreshable var.
    for node in nodes:
        row = node.query(
            "SELECT refresh_interval IS NULL FROM system.custom_variables "
            "WHERE name = 'rs_strip' AND scope = 'cluster'"
        ).strip()
        assert row == "1", (node.name, row)


def test_drop_while_discovery_in_flight(started_cluster, cleanup):
    """On node1, create a batch of cluster variables; simultaneously drop one
    of them from node2. All other entries must eventually be visible on node2
    and the coordinator must keep processing subsequent DDL."""

    names = [f"batch_cv_{i:02d}" for i in range(20)]
    target = names[7]

    # Seed the target first so that DROP on node2 has something to race with.
    node1.query(f"CREATE CLUSTER VARIABLE {target} AS toUInt64(999)")
    assert_eq_with_retry(
        node2, f"SELECT getVariable('cluster.{target}')", "999\n"
    )

    def create_batch():
        for name in names:
            if name == target:
                continue
            node1.query(f"CREATE CLUSTER VARIABLE {name} AS toUInt64(1)")

    def drop_target():
        time.sleep(0.05)  # small head start for create_batch
        node2.query(f"DROP CLUSTER VARIABLE IF EXISTS {target}")

    t1 = threading.Thread(target=create_batch)
    t2 = threading.Thread(target=drop_target)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # Non-target entries must end up visible on both nodes.
    for node in nodes:
        for name in names:
            if name == target:
                continue
            assert_eq_with_retry(
                node,
                f"SELECT getVariable('cluster.{name}')",
                "1\n",
                retry_count=40,
                sleep_time=0.25,
            )

    # Target is gone on both nodes.
    for node in nodes:

        def target_gone(n=node):
            err = n.query_and_get_error(
                f"SELECT getVariable('cluster.{target}')"
            )
            return "UNKNOWN_IDENTIFIER" in err

        deadline = time.time() + 10
        while time.time() < deadline and not target_gone():
            time.sleep(0.25)
        assert target_gone(), (
            f"{node.name} still sees cluster.{target} after DROP"
        )

    # Coordinator is still alive: further DDL goes through.
    node1.query("CREATE CLUSTER VARIABLE batch_post AS toUInt64(1)")
    assert_eq_with_retry(
        node2, "SELECT getVariable('cluster.batch_post')", "1\n"
    )
