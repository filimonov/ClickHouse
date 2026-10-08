#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database, no-replicated-database, no-parallel, no-parallel-replicas
# no-parallel: the failpoint is global and pauses whichever statement reaches it first.

# Correct: a `DROP PARTITION` that races with a transaction removing one of its parts fails with `SERIALIZATION_ERROR`.
# Today: it is accepted, and the removed part becomes active beside the empty part covering it after the rollback.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CURDIR"/transactions.lib

FAILPOINT=non_transactional_drop_pause_before_publish

# A test killed by the runner's timeout must not leave the failpoint armed or the statement paused.
function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT $FAILPOINT" 2>/dev/null || true
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FAILPOINT" 2>/dev/null || true
}
trap cleanup EXIT
trap 'exit 1' TERM INT HUP

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_drop_locked"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_drop_locked (n UInt64) ENGINE = MergeTree ORDER BY n SETTINGS remove_empty_parts = 0"
$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_drop_locked"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_drop_locked VALUES (1)"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_drop_locked VALUES (2)"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_drop_locked VALUES (3)"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FAILPOINT"
drop_output="$CLICKHOUSE_TMP/${CLICKHOUSE_TEST_UNIQUE_NAME}_drop.out"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_drop_locked DROP PARTITION tuple()" > "$drop_output" 2>&1 &
drop_pid=$!
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $FAILPOINT PAUSE"

tx 1 "BEGIN TRANSACTION"
tx 1 "ALTER TABLE t_drop_locked DROP PART 'all_2_2_0'"

$CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT $FAILPOINT"
wait $drop_pid
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FAILPOINT"

echo "--- the drop that raced with the removal is refused"
grep -o -m1 "SERIALIZATION_ERROR" "$drop_output" || echo "accepted"
rm -f "$drop_output"

tx 1 "ROLLBACK"

echo "--- no part is active beside a part that covers it"
$CLICKHOUSE_CLIENT -q "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_drop_locked' AND active ORDER BY name"

echo "--- the partition can be dropped once the transaction is gone"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_drop_locked DROP PARTITION tuple()" 2>&1 | grep -o -m1 "DUPLICATE_DATA_PART"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_drop_locked"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_drop_locked"
