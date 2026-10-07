#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database, no-parallel-replicas

# Correct: `SET TRANSACTION SNAPSHOT` refuses a snapshot the transaction log does not know yet, with `INVALID_TRANSACTION`.
# Today: it accepts a snapshot far above the latest one.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_snapshot"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_snapshot (n UInt64, v UInt64) ENGINE = MergeTree ORDER BY n"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_snapshot VALUES (1, 0)"

latest=$($CLICKHOUSE_CLIENT -q "SELECT transactionLatestSnapshot()")

echo "--- a snapshot above the latest one is refused"
$CLICKHOUSE_CLIENT -n -q "BEGIN TRANSACTION; SET TRANSACTION SNAPSHOT $((latest + 1000000)); ROLLBACK;" 2>&1 | grep -o -m1 "INVALID_TRANSACTION"

echo "--- the latest snapshot and the reserved ones are accepted"
$CLICKHOUSE_CLIENT -n -q "
    BEGIN TRANSACTION; SET TRANSACTION SNAPSHOT $latest; SELECT count() FROM t_snapshot; ROLLBACK;
    BEGIN TRANSACTION; SET TRANSACTION SNAPSHOT 1; SELECT count() FROM t_snapshot; ROLLBACK;
    BEGIN TRANSACTION; SET TRANSACTION SNAPSHOT 3; SELECT count() FROM t_snapshot; ROLLBACK;"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_snapshot"
