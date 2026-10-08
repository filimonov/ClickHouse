#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-ordinary-database, no-replicated-database, no-parallel-replicas, no-random-merge-tree-settings
# Tag rationale: the failpoints are server-wide and hold the transaction log of every other test.

# Correct: a mutation after `SET TRANSACTION SNAPSHOT` to a commit not yet loaded into the log mutates that commit's part too.
# Today: the mutation hangs, because selection tests visibility at the start CSN (see the raised-snapshot test).
# With that fixed it commits and leaves that commit's part unmutated, which is what the reference rules out.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CURDIR"/transactions.lib

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT transaction_log_pause_before_loading_entries" 2>/dev/null || true
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT transaction_force_unknown_state_after_commit" 2>/dev/null || true
}
# A killed test (the runner's timeout) must not leave the log held for the tests that follow.
trap cleanup EXIT
trap 'exit 1' TERM INT HUP

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_unloaded_commit"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_unloaded_commit (n UInt64, v UInt64) ENGINE = MergeTree PARTITION BY n ORDER BY n"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_unloaded_commit VALUES (1, 0)"

tx 1 "BEGIN TRANSACTION"
latest=$($CLICKHOUSE_CLIENT -q "SELECT transactionLatestSnapshot()")

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT transaction_log_pause_before_loading_entries"
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT transaction_force_unknown_state_after_commit"

tx_async 2 "BEGIN TRANSACTION"
tx_async 2 "INSERT INTO t_unloaded_commit VALUES (2, 0)"
tx_async 2 "COMMIT"

# The commit is in Keeper once the updating thread, woken by it, is held before loading it.
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT transaction_log_pause_before_loading_entries PAUSE"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT transaction_force_unknown_state_after_commit"

session="${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}_tx1"
function running()
{
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id LIKE '$session%'"
}

# The CSN of the held commit is the next one. Accepting it is the defect; a correct server waits
# for the log to load it (and the loader is released below) or refuses it.
tx_async 1 "SET TRANSACTION SNAPSHOT $((latest + 1))"
for _ in {1..20}
do
    [ "$(running)" -eq 0 ] && break
    sleep 0.25
done
# A server that waits for the log is still waiting here: release it. One that accepted the
# snapshot at once must keep the log held while the mutation runs.
if [ "$(running)" -ne 0 ]
then
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT transaction_log_pause_before_loading_entries"
fi
tx_wait 1

tx 1 "SET mutations_sync = 1"
tx_async 1 "ALTER TABLE t_unloaded_commit UPDATE v = 1 WHERE 1"
# A server that accepted the snapshot while the log is held may wait here for a part it cannot see yet.
for _ in {1..20}
do
    [ "$(running)" -eq 0 ] && break
    sleep 0.25
done

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT transaction_log_pause_before_loading_entries"

# The updating thread finalizes an unknown-state transaction one iteration after it loaded the
# entry, and an iteration needs a new log entry to start: commit a transaction to provide one.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_unloaded_commit_poke"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_unloaded_commit_poke (n UInt64) ENGINE = MergeTree ORDER BY n"
$CLICKHOUSE_CLIENT -n -q "BEGIN TRANSACTION; INSERT INTO t_unloaded_commit_poke VALUES (1); COMMIT;"

# With the log released a correct server finishes the mutation within moments.
stuck=0
for _ in {1..20}
do
    [ "$(running)" -eq 0 ] && break
    sleep 0.25
done
if [ "$(running)" -ne 0 ]
then
    echo "the mutation does not finish"
    stuck=1
    $CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id LIKE '$session%' SYNC FORMAT Null"
fi
tx_wait 1
tx_wait 2

# A commit would wait for the mutation that did not finish.
if [ "$stuck" -eq 1 ]; then tx 1 "ROLLBACK"; else tx 1 "COMMIT"; fi

$CLICKHOUSE_CLIENT -q "SELECT n, v FROM t_unloaded_commit ORDER BY n"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_unloaded_commit"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_unloaded_commit_poke"
