#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database, no-parallel-replicas

# Correct: dropping a part whose creating transaction is still running fails with `SERIALIZATION_ERROR`.
# Today: it fails with `LOGICAL_ERROR`, which aborts a debug or sanitizer build.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CURDIR"/transactions.lib

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_uncommitted_drop"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_uncommitted_drop (n UInt64) ENGINE = MergeTree ORDER BY n PARTITION BY n % 2"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_uncommitted_drop VALUES (1)"

tx 1 "BEGIN TRANSACTION"
tx 1 "INSERT INTO t_uncommitted_drop VALUES (2)"

tx 2 "BEGIN TRANSACTION"
tx 2 "SET TRANSACTION SNAPSHOT 3"
echo "--- the uncommitted part is visible, the drop is refused"
tx 2 "SELECT count() FROM t_uncommitted_drop"
tx 2 "ALTER TABLE t_uncommitted_drop DROP PARTITION 0" | grep -o -m1 "SERIALIZATION_ERROR\|LOGICAL_ERROR"
tx 2 "ROLLBACK"

echo "--- the creating transaction is unaffected"
tx 1 "COMMIT"
$CLICKHOUSE_CLIENT -q "SELECT n FROM t_uncommitted_drop ORDER BY n"

echo "--- a transaction may drop its own uncommitted part"
tx 3 "BEGIN TRANSACTION"
tx 3 "INSERT INTO t_uncommitted_drop VALUES (4)"
tx 3 "ALTER TABLE t_uncommitted_drop DROP PARTITION 0"
tx 3 "COMMIT"
$CLICKHOUSE_CLIENT -q "SELECT n FROM t_uncommitted_drop ORDER BY n"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_uncommitted_drop"
