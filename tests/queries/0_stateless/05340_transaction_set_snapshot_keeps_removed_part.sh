#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database, no-parallel-replicas

# Correct: the cleanup thread keeps a part that a lowered transaction snapshot can still see.
# Today: it removes the part while the transaction runs, and the transaction reads no rows.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CURDIR"/transactions.lib

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_snapshot_keeps_part"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_snapshot_keeps_part (n UInt64) ENGINE = MergeTree ORDER BY n
    SETTINGS old_parts_lifetime = 3600, cleanup_delay_period = 1, max_cleanup_delay_period = 1, cleanup_delay_period_random_add = 0"

function parts_on_disk()
{
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_snapshot_keeps_part'"
}

tx 1 "BEGIN TRANSACTION" > /dev/null
tx 1 "INSERT INTO t_snapshot_keeps_part VALUES (1)"
tx 1 "COMMIT"
snapshot=$($CLICKHOUSE_CLIENT -q "SELECT transactionLatestSnapshot()")

tx 2 "BEGIN TRANSACTION" > /dev/null
tx 2 "TRUNCATE TABLE t_snapshot_keeps_part"
tx 2 "COMMIT"

tx 3 "BEGIN TRANSACTION" > /dev/null
tx 3 "SET TRANSACTION SNAPSHOT $snapshot"
echo "--- before the cleanup may run"
tx 3 "SELECT count() FROM t_snapshot_keeps_part"

$CLICKHOUSE_CLIENT -q "ALTER TABLE t_snapshot_keeps_part MODIFY SETTING old_parts_lifetime = 1"

# Stop early once the part is gone: that is the defect, and the assertions below report it.
for _ in $(seq 1 25)
do
    [ "$(parts_on_disk)" == "0" ] && break
    sleep 0.2
done

echo "--- after the cleanup had its chance"
tx 3 "SELECT count() FROM t_snapshot_keeps_part"
echo "part kept: $(parts_on_disk)"
tx 3 "ROLLBACK"

echo "--- once the transaction ended the part is removed"
for _ in $(seq 1 50)
do
    [ "$(parts_on_disk)" == "0" ] && break
    sleep 0.2
done
echo "part kept: $(parts_on_disk)"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_snapshot_keeps_part"
