"""Reproductions of open MergeTree transaction defects.

Each test states the behaviour the server does not have, so each fails today and carries
`xfail(strict=True)`. Fixing a defect turns its test into a failure of the suite, which is the
signal to drop the decorator and keep the test as a regression test.

The node is private to this module because these reproductions cannot share a server: the
failpoints are global, and the removal of a part whose creation has not committed raises a
`LOGICAL_ERROR` that aborts a debug or sanitizer build.
"""

import concurrent.futures
import contextlib
import logging
import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/transactions.xml"],
    user_configs=["configs/users.xml"],
    stay_alive=True,
    with_zookeeper=True,
    # Transactions refuse to start unless Keeper advertises these.
    keeper_required_feature_flags=[
        "filtered_list",
        "multi_read",
        "list_with_stat_and_data",
        "check_stat",
    ],
)

FAILPOINTS = [
    "non_transactional_removal_store_fail_after_first_part",
    "non_transactional_drop_pause_before_publish",
    "transaction_force_unknown_state_after_commit",
    "transaction_log_pause_before_loading_entries",
    "version_metadata_store_creation_csn_fail",
    "version_metadata_store_pause_before_rename",
]


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        # `SET TRANSACTION SNAPSHOT` refuses a CSN in the reserved range, and a fresh server sits
        # at the top of it. A commit that changes nothing allocates no CSN, so insert a row.
        node.query("CREATE TABLE t_warmup (n UInt64) ENGINE = MergeTree ORDER BY n")
        node.query("BEGIN TRANSACTION; INSERT INTO t_warmup VALUES (1); COMMIT;")
        latest = int(node.query("SELECT transactionLatestSnapshot()").strip())
        assert latest > 32, f"latest snapshot {latest} is still a reserved CSN"
        yield cluster
    finally:
        # `test_drop_partition_of_uncommitted_creation_is_refused` reproduces a `LOGICAL_ERROR`,
        # and the abort it causes in a debug or sanitizer build is part of what it reports.
        cluster.shutdown(ignore_logical_errors=True, ignore_fatal=True)


@pytest.fixture(autouse=True)
def isolated_test(start_cluster):
    """Each test gets its own transaction sessions and leaves no failpoint armed."""
    global _session_epoch
    _session_epoch += 1
    _sessions.clear()
    yield
    if not server_is_up():
        return
    for failpoint in FAILPOINTS:
        try:
            # Disabling also releases whatever is parked at a pauseable failpoint.
            node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}", timeout=60)
        except Exception as e:
            logging.warning("could not disable failpoint %s: %s", failpoint, e)
    # A transaction left open by a failed assertion would hold a snapshot and pin part cleanup.
    for session in sorted(_sessions):
        with contextlib.suppress(Exception):
            node.http_query(
                None, data="ROLLBACK", params={"session_id": session}, timeout=60
            )


def server_is_up():
    try:
        node.query("SELECT 1", timeout=30)
        return True
    except Exception:
        return False


def restart_if_down():
    if not server_is_up():
        node.restart_clickhouse(stop_start_wait_sec=120, kill=True)


_session_epoch = 0
_sessions = set()


def session_id(session):
    name = f"session_{_session_epoch}_{session}"
    _sessions.add(name)
    return name


def tx(session, query, query_id=None, timeout=None):
    """Runs `query` in a session, so `BEGIN`/`COMMIT` span calls. Raises on a server error."""
    params = {"session_id": session_id(session)}
    if query_id is not None:
        params["query_id"] = query_id
    return node.http_query(None, data=query, params=params, timeout=timeout)


def tx_answer_with_error(session, query):
    params = {"session_id": session_id(session)}
    return node.http_query_and_get_answer_with_error(None, data=query, params=params)


def wait_for_no_merges(timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if node.query("SELECT count() FROM system.merges").strip() == "0":
            return
        time.sleep(0.2)
    raise RuntimeError(
        "merges are still running: "
        + node.query("SELECT database, table FROM system.merges")
    )


@contextlib.contextmanager
def merges_stopped():
    """Stops every merge, which the removal-store failpoint needs: it fires once, on the first
    batch of two or more parts to reach `NonTransactionalRemovalLocks::store`, and a merge of any
    table -- a system log included -- is indistinguishable there, both callers passing
    `LockKind::REMOVAL`.

    Enter this only once the test's tables exist. `SYSTEM STOP MERGES` without a table iterates
    the tables of every database and takes an action lock per table
    (`InterpreterSystemQuery::startStopAction`), so a table created afterwards is not covered. It
    also leaves a merge that is already running alone, hence the wait.
    """
    node.query("SYSTEM STOP MERGES")
    try:
        wait_for_no_merges()
        yield
    finally:
        node.query("SYSTEM START MERGES")


def require_parts(table, expected):
    """A precondition, not an assertion about the server: raises so the failure is distinguishable
    from the defect each test asserts."""
    actual = int(
        node.query(
            "SELECT count() FROM system.parts WHERE database = currentDatabase() "
            f"AND table = '{table}' AND active"
        ).strip()
    )
    if actual != expected:
        raise RuntimeError(
            f"{table} has {actual} active parts, expected {expected}: a merge got there first and "
            "the removal batch would not hold two parts"
        )


@contextlib.contextmanager
def failpoints(*names):
    for name in names:
        node.query(f"SYSTEM ENABLE FAILPOINT {name}")
    try:
        yield
    finally:
        for name in names:
            try:
                node.query(f"SYSTEM DISABLE FAILPOINT {name}", timeout=60)
            except Exception as e:
                logging.warning("could not disable failpoint %s: %s", name, e)


def running(query_id):
    return int(
        node.query(
            f"SELECT count() FROM system.processes WHERE query_id = '{query_id}'"
        ).strip()
    )


def wait_until_idle(query_id, timeout=20):
    """True if the query finished within `timeout` seconds."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if running(query_id) == 0:
            return True
        time.sleep(0.25)
    return running(query_id) == 0


def kill_query(query_id):
    node.query(f"KILL QUERY WHERE query_id = '{query_id}' SYNC FORMAT Null")


def covered_active_parts(table):
    """Active parts that another active part of the same partition covers."""
    return node.query(
        "WITH p AS (SELECT partition_id, min_block_number mn, max_block_number mx, level, name"
        "           FROM system.parts"
        f"          WHERE database = currentDatabase() AND table = '{table}' AND active)"
        " SELECT covered.name FROM p AS covered, p AS covering"
        " WHERE covered.name != covering.name"
        "   AND covered.partition_id = covering.partition_id"
        "   AND covering.mn <= covered.mn AND covering.mx >= covered.mx"
        "   AND covering.level > covered.level"
        " ORDER BY covered.name"
    ).split()


def two_parts(table, settings="old_parts_lifetime = 3600"):
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        f"CREATE TABLE {table} (n UInt64) ENGINE = MergeTree ORDER BY n SETTINGS {settings}"
    )
    node.query(f"INSERT INTO {table} VALUES (1)")
    node.query(f"INSERT INTO {table} VALUES (2)")


# https://github.com/ClickHouse/ClickHouse/issues/124488
@pytest.mark.xfail(
    strict=True,
    reason="Known bug #124488: SET TRANSACTION SNAPSHOT accepts a snapshot above the latest one",
)
def test_snapshot_above_latest_is_refused(start_cluster):
    node.query("DROP TABLE IF EXISTS t_snapshot SYNC")
    node.query("CREATE TABLE t_snapshot (n UInt64) ENGINE = MergeTree ORDER BY n")
    node.query("INSERT INTO t_snapshot VALUES (1)")
    latest = int(node.query("SELECT transactionLatestSnapshot()").strip())

    tx(1, "BEGIN TRANSACTION")
    accepted, error = tx_answer_with_error(
        1, f"SET TRANSACTION SNAPSHOT {latest + 1000000}"
    )
    tx(1, "ROLLBACK")

    # The latest snapshot and the reserved ones stay acceptable.
    for snapshot in (latest, 1, 3):
        tx(2, "BEGIN TRANSACTION")
        tx(2, f"SET TRANSACTION SNAPSHOT {snapshot}")
        assert tx(2, "SELECT count() FROM t_snapshot").strip() == "1"
        tx(2, "ROLLBACK")

    assert error and "INVALID_TRANSACTION" in error, (
        f"a snapshot {latest + 1000000} above the latest {latest} was accepted: {accepted!r}"
    )


# https://github.com/ClickHouse/ClickHouse/issues/124488
@pytest.mark.xfail(
    strict=True,
    reason="Known bug #124488: a mutation never finishes at a raised snapshot, because part "
    "selection tests visibility at the start CSN",
)
def test_mutation_at_raised_snapshot_finishes(start_cluster):
    node.query("DROP TABLE IF EXISTS t_raised_snapshot SYNC")
    node.query(
        "CREATE TABLE t_raised_snapshot (n UInt64, v UInt64) ENGINE = MergeTree "
        "PARTITION BY n ORDER BY n"
    )
    node.query("INSERT INTO t_raised_snapshot VALUES (1, 0)")

    tx(1, "BEGIN TRANSACTION")
    tx(2, "BEGIN TRANSACTION")
    tx(2, "INSERT INTO t_raised_snapshot VALUES (2, 0)")
    tx(2, "COMMIT")

    latest = node.query("SELECT transactionLatestSnapshot()").strip()
    tx(1, f"SET TRANSACTION SNAPSHOT {latest}")
    assert tx(1, "SELECT count() FROM t_raised_snapshot").strip() == "2"

    tx(1, "SET mutations_sync = 1")
    query_id = "raised_snapshot_mutation"
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        mutation = pool.submit(
            tx, 1, "ALTER TABLE t_raised_snapshot UPDATE v = 1 WHERE 1", query_id
        )
        finished = wait_until_idle(query_id)
        if not finished:
            kill_query(query_id)
        with contextlib.suppress(Exception):
            mutation.result(timeout=60)
        assert finished, "the mutation at a raised snapshot does not finish"

        tx(1, "COMMIT")
        assert node.query("SELECT n, v FROM t_raised_snapshot ORDER BY n") == "1\t1\n2\t1\n"
    finally:
        with contextlib.suppress(Exception):
            tx(1, "ROLLBACK")
        pool.shutdown(wait=False)


# https://github.com/ClickHouse/ClickHouse/issues/124488
@pytest.mark.xfail(
    strict=True,
    reason="Known bug #124488: a snapshot set to a commit the transaction log has not loaded is "
    "accepted, and the mutation that follows misses that commit's part",
)
def test_mutation_covers_part_of_unloaded_commit(start_cluster):
    node.query("DROP TABLE IF EXISTS t_unloaded_commit SYNC")
    node.query("DROP TABLE IF EXISTS t_unloaded_commit_poke SYNC")
    node.query(
        "CREATE TABLE t_unloaded_commit (n UInt64, v UInt64) ENGINE = MergeTree "
        "PARTITION BY n ORDER BY n"
    )
    node.query("INSERT INTO t_unloaded_commit VALUES (1, 0)")

    tx(1, "BEGIN TRANSACTION")
    latest = int(node.query("SELECT transactionLatestSnapshot()").strip())

    snapshot_id = "unloaded_commit_snapshot"
    mutation_id = "unloaded_commit_mutation"
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=2)
    held = False
    try:
        node.query("SYSTEM ENABLE FAILPOINT transaction_log_pause_before_loading_entries")
        node.query("SYSTEM ENABLE FAILPOINT transaction_force_unknown_state_after_commit")
        held = True

        commit = pool.submit(
            lambda: [
                tx(2, "BEGIN TRANSACTION"),
                tx(2, "INSERT INTO t_unloaded_commit VALUES (2, 0)"),
                tx(2, "COMMIT"),
            ]
        )

        # The commit is in Keeper once the updating thread it wakes is held before loading it.
        node.query(
            "SYSTEM WAIT FAILPOINT transaction_log_pause_before_loading_entries PAUSE",
            timeout=60,
        )
        node.query("SYSTEM DISABLE FAILPOINT transaction_force_unknown_state_after_commit")

        # The held commit's CSN is the next one. Accepting it at once is the defect; a correct
        # server waits for the log to load it, or refuses it.
        snapshot = pool.submit(
            tx, 1, f"SET TRANSACTION SNAPSHOT {latest + 1}", snapshot_id
        )
        if not wait_until_idle(snapshot_id):
            # A server that waits for the log is still waiting: release it.
            node.query(
                "SYSTEM DISABLE FAILPOINT transaction_log_pause_before_loading_entries"
            )
            held = False
        snapshot.result(timeout=60)

        tx(1, "SET mutations_sync = 1")
        mutation = pool.submit(
            tx, 1, "ALTER TABLE t_unloaded_commit UPDATE v = 1 WHERE 1", mutation_id
        )
        wait_until_idle(mutation_id)

        if held:
            node.query(
                "SYSTEM DISABLE FAILPOINT transaction_log_pause_before_loading_entries"
            )
            held = False

        # The updating thread finalizes an unknown-state transaction one iteration after loading
        # its entry, and an iteration needs a new entry to start: commit something to provide one.
        node.query(
            "CREATE TABLE t_unloaded_commit_poke (n UInt64) ENGINE = MergeTree ORDER BY n"
        )
        tx(3, "BEGIN TRANSACTION")
        tx(3, "INSERT INTO t_unloaded_commit_poke VALUES (1)")
        tx(3, "COMMIT")

        finished = wait_until_idle(mutation_id)
        if not finished:
            kill_query(mutation_id)
        with contextlib.suppress(Exception):
            mutation.result(timeout=60)
        commit.result(timeout=60)
        assert finished, "the mutation does not finish"

        tx(1, "COMMIT")
        assert node.query("SELECT n, v FROM t_unloaded_commit ORDER BY n") == "1\t1\n2\t1\n"
    finally:
        with contextlib.suppress(Exception):
            tx(1, "ROLLBACK")
        pool.shutdown(wait=False)


# https://github.com/ClickHouse/ClickHouse/issues/124488
@pytest.mark.xfail(
    strict=True,
    reason="Known bug #124488: the cleanup thread removes a part that a lowered snapshot still sees",
)
def test_lowered_snapshot_keeps_the_part_it_sees(start_cluster):
    table = "t_snapshot_keeps_part"
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        f"CREATE TABLE {table} (n UInt64) ENGINE = MergeTree ORDER BY n SETTINGS "
        "old_parts_lifetime = 3600, cleanup_delay_period = 1, max_cleanup_delay_period = 1, "
        "cleanup_delay_period_random_add = 0"
    )

    def parts_on_disk():
        return int(
            node.query(
                "SELECT count() FROM system.parts WHERE database = currentDatabase() "
                f"AND table = '{table}'"
            ).strip()
        )

    tx(1, "BEGIN TRANSACTION")
    tx(1, f"INSERT INTO {table} VALUES (1)")
    tx(1, "COMMIT")
    snapshot = int(node.query("SELECT transactionLatestSnapshot()").strip())

    tx(2, "BEGIN TRANSACTION")
    tx(2, f"TRUNCATE TABLE {table}")
    tx(2, "COMMIT")

    tx(3, "BEGIN TRANSACTION")
    tx(3, f"SET TRANSACTION SNAPSHOT {snapshot}")
    assert tx(3, f"SELECT count() FROM {table}").strip() == "1"

    node.query(f"ALTER TABLE {table} MODIFY SETTING old_parts_lifetime = 1")
    try:
        # Stop early once the part is gone: that is the defect the assertions below report.
        for _ in range(25):
            if parts_on_disk() == 0:
                break
            time.sleep(0.2)

        assert tx(3, f"SELECT count() FROM {table}").strip() == "1"
        assert parts_on_disk() == 1, "the cleanup removed a part the transaction still sees"
    finally:
        tx(3, "ROLLBACK")

    # Once no transaction can see it, the part goes.
    for _ in range(50):
        if parts_on_disk() == 0:
            break
        time.sleep(0.2)
    assert parts_on_disk() == 0


# https://github.com/ClickHouse/ClickHouse/issues/124486
@pytest.mark.xfail(
    strict=True,
    reason="Known bug #124486: a failed non-transactional DROP PARTITION leaves a part stamped "
    "for removal, which hides it from every transaction",
)
def test_failed_drop_partition_leaves_no_removal_stamp(start_cluster):
    two_parts("t_drop_stamp")
    with merges_stopped():
        require_parts("t_drop_stamp", 2)
        with failpoints("non_transactional_removal_store_fail_after_first_part"):
            error = node.query_and_get_error(
                "ALTER TABLE t_drop_stamp DROP PARTITION tuple()"
            )
        assert "CANNOT_WRITE_TO_FILE" in error, error

        stamped = node.query(
            "SELECT count() FROM system.parts WHERE database = currentDatabase() "
            "AND table = 't_drop_stamp' AND removal_csn != 0"
        ).strip()
        assert stamped == "0", "the failed statement left a part stamped for removal"

        tx(1, "BEGIN TRANSACTION")
        assert tx(1, "SELECT count(), sum(n) FROM t_drop_stamp").strip() == "2\t3"
        tx(1, "ROLLBACK")

        # The failure left no lock behind: the same statement succeeds now.
        node.query("ALTER TABLE t_drop_stamp DROP PARTITION tuple()")
        assert node.query("SELECT count() FROM t_drop_stamp").strip() == "0"


# https://github.com/ClickHouse/ClickHouse/issues/124486
@pytest.mark.xfail(
    strict=True,
    reason="Known bug #124486: a failed DROP PARTITION empties the partition after a reload",
)
def test_failed_drop_partition_survives_reload(start_cluster):
    two_parts("t_drop_reload")
    with merges_stopped():
        require_parts("t_drop_reload", 2)
        with failpoints("non_transactional_removal_store_fail_after_first_part"):
            error = node.query_and_get_error(
                "ALTER TABLE t_drop_reload DROP PARTITION tuple()"
            )
        assert "CANNOT_WRITE_TO_FILE" in error, error
        assert node.query("SELECT count(), sum(n) FROM t_drop_reload").strip() == "2\t3"

        node.query("DETACH TABLE t_drop_reload")
        node.query("ATTACH TABLE t_drop_reload")
        assert node.query("SELECT count(), sum(n) FROM t_drop_reload").strip() == "2\t3"


# https://github.com/ClickHouse/ClickHouse/issues/124486
@pytest.mark.xfail(
    strict=True,
    reason="Known bug #124486: a failed TRUNCATE empties the table after a reload",
)
def test_failed_truncate_survives_reload(start_cluster):
    two_parts("t_truncate_reload")
    with merges_stopped():
        require_parts("t_truncate_reload", 2)
        with failpoints("non_transactional_removal_store_fail_after_first_part"):
            error = node.query_and_get_error("TRUNCATE TABLE t_truncate_reload")
        assert "CANNOT_WRITE_TO_FILE" in error, error
        assert node.query("SELECT count(), sum(n) FROM t_truncate_reload").strip() == "2\t3"

        node.query("DETACH TABLE t_truncate_reload")
        node.query("ATTACH TABLE t_truncate_reload")
        assert node.query("SELECT count(), sum(n) FROM t_truncate_reload").strip() == "2\t3"


# https://github.com/ClickHouse/ClickHouse/issues/124486
@pytest.mark.xfail(
    strict=True,
    reason="Known bug #124486: a failed REPLACE PARTITION leaves the new part beside the ones it "
    "did not replace",
)
def test_failed_replace_partition_leaves_destination_intact(start_cluster):
    node.query("DROP TABLE IF EXISTS t_replace_src SYNC")
    node.query("DROP TABLE IF EXISTS t_replace_dst SYNC")
    node.query("CREATE TABLE t_replace_src (n UInt64) ENGINE = MergeTree ORDER BY n")
    node.query("CREATE TABLE t_replace_dst (n UInt64) ENGINE = MergeTree ORDER BY n")
    node.query("INSERT INTO t_replace_src VALUES (100)")
    node.query("INSERT INTO t_replace_dst VALUES (1)")
    node.query("INSERT INTO t_replace_dst VALUES (2)")

    with merges_stopped():
        require_parts("t_replace_dst", 2)
        with failpoints("non_transactional_removal_store_fail_after_first_part"):
            error = node.query_and_get_error(
                "ALTER TABLE t_replace_dst REPLACE PARTITION tuple() FROM t_replace_src"
            )
        assert "CANNOT_WRITE_TO_FILE" in error, error

        kept = node.query(
            "SELECT arraySort(groupArray(n)) FROM t_replace_dst"
        ).strip()
        assert kept == "[1,2]", f"the destination changed after a failed replace: {kept}"

        # The failure left no lock behind: the same statement succeeds now.
        node.query(
            "ALTER TABLE t_replace_dst REPLACE PARTITION tuple() FROM t_replace_src"
        )
        assert (
            node.query("SELECT arraySort(groupArray(n)) FROM t_replace_dst").strip()
            == "[100]"
        )


# https://github.com/ClickHouse/ClickHouse/issues/124486
@pytest.mark.xfail(
    strict=True,
    reason="Known bug #124486: when the rollback's own mark fails too, a failed DROP PARTITION "
    "still empties the table after a reload",
)
def test_failed_drop_partition_survives_reload_when_rollback_mark_fails(start_cluster):
    two_parts("t_rollback_mark")
    with merges_stopped():
        require_parts("t_rollback_mark", 2)
        with failpoints(
            "non_transactional_removal_store_fail_after_first_part",
            "version_metadata_store_creation_csn_fail",
        ):
            error = node.query_and_get_error(
                "ALTER TABLE t_rollback_mark DROP PARTITION tuple()",
                settings={"send_logs_level": "none"},
            )
        assert "CANNOT_WRITE_TO_FILE" in error, error
        assert node.query("SELECT count(), sum(n) FROM t_rollback_mark").strip() == "2\t3"

        node.query("DETACH TABLE t_rollback_mark")
        node.query("ATTACH TABLE t_rollback_mark")
        assert node.query("SELECT count(), sum(n) FROM t_rollback_mark").strip() == "2\t3"


# https://github.com/ClickHouse/ClickHouse/issues/124489
@pytest.mark.xfail(
    strict=True,
    reason="Known bug #124489: a DROP PARTITION racing a transaction that removes one of its "
    "parts is accepted, and the part is active beside the empty part covering it",
)
def test_drop_partition_racing_a_removal_is_refused(start_cluster):
    table = "t_drop_locked"
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        f"CREATE TABLE {table} (n UInt64) ENGINE = MergeTree ORDER BY n "
        "SETTINGS remove_empty_parts = 0"
    )
    node.query(f"SYSTEM STOP MERGES {table}")
    for n in (1, 2, 3):
        node.query(f"INSERT INTO {table} VALUES ({n})")

    failpoint = "non_transactional_drop_pause_before_publish"
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        with failpoints(failpoint):
            drop = pool.submit(
                node.query_and_get_answer_with_error,
                f"ALTER TABLE {table} DROP PARTITION tuple()",
            )
            node.query(f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE", timeout=60)

            tx(1, "BEGIN TRANSACTION")
            tx(1, f"ALTER TABLE {table} DROP PART 'all_2_2_0'")

            node.query(f"SYSTEM NOTIFY FAILPOINT {failpoint}")
            _, error = drop.result(timeout=120)

        tx(1, "ROLLBACK")

        assert error and "SERIALIZATION_ERROR" in error, (
            "the drop that raced with the removal was accepted"
        )
        assert covered_active_parts(table) == [], (
            "a part is active beside a part that covers it"
        )
    finally:
        with contextlib.suppress(Exception):
            tx(1, "ROLLBACK")
        pool.shutdown(wait=False)


# https://github.com/ClickHouse/ClickHouse/issues/124487
@pytest.mark.xfail(
    strict=True,
    reason="Known bug #124487: an acknowledged INSERT is lost when the server stops between the "
    "sync and the rename of the first version metadata store",
)
def test_acknowledged_insert_survives_kill_during_first_version_metadata_store(
    start_cluster,
):
    # A part inserted outside a transaction has no `txn_version.txt`. The first metadata store for
    # it is the removal lock taken by a transaction's DROP PARTITION. The store writes
    # `txn_version.txt.tmp` and renames it; a kill between the two must not lose the rows.
    node.query("DROP TABLE IF EXISTS mt_lost SYNC")
    node.query(
        "CREATE TABLE mt_lost (n UInt64) ENGINE = MergeTree ORDER BY n PARTITION BY n % 2 "
        "SETTINGS remove_empty_parts = 0"
    )
    node.query("INSERT INTO mt_lost VALUES (1), (3)")
    part_path = node.query(
        "SELECT path FROM system.parts WHERE database = currentDatabase() "
        "AND table = 'mt_lost' AND active"
    ).strip()
    assert part_path != ""

    failpoint = "version_metadata_store_pause_before_rename"
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        with failpoints(failpoint):
            tx(7, "BEGIN TRANSACTION")
            drop = pool.submit(
                tx_answer_with_error, 7, "ALTER TABLE mt_lost DROP PARTITION ID '1'"
            )
            node.query(f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE", timeout=60)

            listing = node.exec_in_container(["bash", "-c", f"ls -1 {part_path}"]).split()
            assert "txn_version.txt.tmp" in listing, listing
            assert "txn_version.txt" not in listing, listing

            node.restart_clickhouse(kill=True)
            with contextlib.suppress(Exception):
                drop.result(timeout=60)
    finally:
        pool.shutdown(wait=False)

    node.query("SYSTEM WAIT LOADING PARTS mt_lost")
    assert node.query("SELECT n FROM mt_lost ORDER BY n").strip() == "1\n3"
    assert (
        node.query(
            "SELECT count() FROM system.parts WHERE database = currentDatabase() "
            "AND table = 'mt_lost' AND active"
        ).strip()
        == "1"
    )


# Last in the file: today's `LOGICAL_ERROR` aborts a debug or sanitizer build, and the tests above
# need a server that is still running.
# https://github.com/ClickHouse/ClickHouse/issues/124489
@pytest.mark.xfail(
    strict=True,
    reason="Known bug #124489: removing a part whose creating transaction is still running raises "
    "LOGICAL_ERROR instead of SERIALIZATION_ERROR, which aborts a debug or sanitizer build",
)
def test_drop_partition_of_uncommitted_creation_is_refused(start_cluster):
    table = "t_uncommitted_drop"
    try:
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")
        node.query(
            f"CREATE TABLE {table} (n UInt64) ENGINE = MergeTree ORDER BY n PARTITION BY n % 2"
        )
        node.query(f"INSERT INTO {table} VALUES (1)")

        tx(1, "BEGIN TRANSACTION")
        tx(1, f"INSERT INTO {table} VALUES (2)")

        tx(2, "BEGIN TRANSACTION")
        tx(2, "SET TRANSACTION SNAPSHOT 3")
        assert tx(2, f"SELECT count() FROM {table}").strip() == "2"
        _, error = tx_answer_with_error(2, f"ALTER TABLE {table} DROP PARTITION 0")
        assert "SERIALIZATION_ERROR" in error, error
        tx(2, "ROLLBACK")

        # The creating transaction is unaffected.
        tx(1, "COMMIT")
        assert node.query(f"SELECT n FROM {table} ORDER BY n").strip() == "1\n2"

        # A transaction may drop a part it created itself.
        tx(3, "BEGIN TRANSACTION")
        tx(3, f"INSERT INTO {table} VALUES (4)")
        tx(3, f"ALTER TABLE {table} DROP PARTITION 0")
        tx(3, "COMMIT")
        assert node.query(f"SELECT n FROM {table} ORDER BY n").strip() == "1"
    finally:
        restart_if_down()
