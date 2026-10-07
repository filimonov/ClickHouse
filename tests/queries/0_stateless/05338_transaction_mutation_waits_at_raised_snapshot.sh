#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database, no-parallel-replicas, no-random-merge-tree-settings

# Correct: a mutation in a transaction rewrites every part visible at the raised snapshot, and finishes.
# Today: it waits for the part of the commit the snapshot was raised past, which is never selected.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CURDIR"/transactions.lib

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_raised_snapshot"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_raised_snapshot (n UInt64, v UInt64) ENGINE = MergeTree PARTITION BY n ORDER BY n"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_raised_snapshot VALUES (1, 0)"

tx 1 "BEGIN TRANSACTION"
tx 2 "BEGIN TRANSACTION"
tx 2 "INSERT INTO t_raised_snapshot VALUES (2, 0)"
tx 2 "COMMIT"

latest=$($CLICKHOUSE_CLIENT -q "SELECT transactionLatestSnapshot()")
tx 1 "SET TRANSACTION SNAPSHOT $latest"
tx 1 "SELECT 'rows seen', count() FROM t_raised_snapshot"

tx 1 "SET mutations_sync = 1"
tx_async 1 "ALTER TABLE t_raised_snapshot UPDATE v = 1 WHERE 1"

session="${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}_tx1"
stuck=0
for _ in {1..20}
do
    [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id LIKE '$session%'")" -eq 0 ] && break
    sleep 0.5
done

if [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id LIKE '$session%'")" -ne 0 ]
then
    echo "the mutation does not finish"
    stuck=1
    $CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id LIKE '$session%' SYNC FORMAT Null"
fi
tx_wait 1

tx 1 "SELECT n, v FROM t_raised_snapshot ORDER BY n"
# A commit would wait for the mutation that did not finish.
if [ "$stuck" -eq 1 ]; then tx 1 "ROLLBACK"; else tx 1 "COMMIT"; fi

$CLICKHOUSE_CLIENT -q "SELECT n, v FROM t_raised_snapshot ORDER BY n"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_raised_snapshot"
