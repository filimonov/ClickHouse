#!/usr/bin/env bash
# Tags: no-parallel, no-shared-merge-tree
# no-parallel -- uses server-wide failpoints: one pauses the first mutation started on any MergeTree table,
#                the other postpones mutation selection on every table.
# no-shared-merge-tree -- the race is in the in-memory mutation registration of StorageMergeTree.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function cleanup()
{
    # Disabling the pause failpoint also releases a mutation still paused at it, so the background clients finish.
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT mt_pause_before_mutation_registration" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads" 2>/dev/null
    wait
}
trap cleanup EXIT

# Polls a query until it returns the expected value; fails the test instead of hanging.
function wait_for_query_result()
{
    local query=$1
    local expected=$2
    local result=""
    for _ in $(seq 1 600); do
        result=$($CLICKHOUSE_CLIENT --query "$query")
        if [[ "$result" == "$expected" ]]; then
            return 0
        fi
        sleep 0.1
    done
    echo "Timed out waiting for '$expected' from: $query (last result: '$result')"
    exit 1
}

# Starts a lightweight DELETE that pauses after allocating its block number and writing mutation_N.txt, before
# registering the entry, and waits until it is paused. The client runs in the background; its pid is in $paused_pid.
function start_paused_delete()
{
    local table=$1
    local predicate=$2
    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT mt_pause_before_mutation_registration"
    $CLICKHOUSE_CLIENT --lightweight_deletes_sync=0 --lightweight_delete_mode=alter_update --query "DELETE FROM $table WHERE $predicate" &
    paused_pid=$!
    # `SYSTEM WAIT FAILPOINT ... PAUSE` has no timeout of its own; bound it here so a DELETE that fails before
    # reaching the failpoint makes the test fail instead of hang.
    # shellcheck disable=SC2086
    if ! timeout 60 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT mt_pause_before_mutation_registration PAUSE"; then
        echo "The first DELETE did not reach the failpoint"
        exit 1
    fi
    if ! kill -0 "$paused_pid" 2>/dev/null; then
        echo "The first DELETE finished before it was released"
        exit 1
    fi
}

function delete_async()
{
    $CLICKHOUSE_CLIENT --lightweight_deletes_sync=0 --lightweight_delete_mode=alter_update --query "DELETE FROM $1 WHERE $2"
}

MUTATIONS="FROM system.mutations WHERE database = currentDatabase() AND table = 't_mut_order'"
STATE_QUERY="SELECT if(is_done, 'done', if(length(parts_postpone_reasons) > 0, 'postponed', 'pending')) $MUTATIONS ORDER BY toUInt64(extract(mutation_id, '[0-9]+')) FORMAT TSV"

echo "--- inversion: N+1 registers while N is paused"
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_mut_order;
    CREATE TABLE t_mut_order (id UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO t_mut_order SELECT number FROM numbers(100);
"
start_paused_delete t_mut_order "id < 50"
delete_async t_mut_order "id >= 50"
# The mutate task must postpone the part until mutation N is registered instead of applying N + 1 alone
# (which would move the part to data version N + 1 and lose mutation N forever).
wait_for_query_result "$STATE_QUERY" "postponed"
$CLICKHOUSE_CLIENT --query "SELECT mapValues(parts_postpone_reasons) $MUTATIONS"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT mt_pause_before_mutation_registration"
wait "$paused_pid"
wait_for_query_result "SELECT countIf(is_done) $MUTATIONS" "2"
$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM t_mut_order;
    SELECT count(), countIf(is_done) $MUTATIONS;
"

echo "--- gap: K is applied alone while N is paused, N+1 and N+3 wait; a part inserted above N is postponed too"
$CLICKHOUSE_CLIENT --query "
    DROP TABLE t_mut_order;
    -- Separate partitions keep the two parts apart (stopping merges would also stop mutation selection).
    CREATE TABLE t_mut_order (id UInt64) ENGINE = MergeTree PARTITION BY intDiv(id, 100) ORDER BY id;
    INSERT INTO t_mut_order SELECT number FROM numbers(100);
    SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
"
# K registers but cannot run while the selection failpoint is on.
delete_async t_mut_order "id < 25"
wait_for_query_result "$STATE_QUERY" "postponed"
start_paused_delete t_mut_order "id >= 25 AND id < 50"
delete_async t_mut_order "id >= 50 AND id < 100"
# A part inserted while N is paused gets a data version above N; a later mutation above it must not be applied either.
$CLICKHOUSE_CLIENT --query "INSERT INTO t_mut_order SELECT number FROM numbers(100, 100)"
delete_async t_mut_order "id >= 150"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads"
# Only K may run: the first part goes to data version K, everything above the paused N waits.
wait_for_query_result "$STATE_QUERY" "$(printf 'done\npostponed\npostponed')"
$CLICKHOUSE_CLIENT --query "
    SELECT name, count() FROM t_mut_order GROUP BY _part AS name ORDER BY name;
    SELECT is_done, mapValues(parts_postpone_reasons) $MUTATIONS ORDER BY toUInt64(extract(mutation_id, '[0-9]+'));
"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT mt_pause_before_mutation_registration"
wait "$paused_pid"
wait_for_query_result "SELECT countIf(is_done) $MUTATIONS" "4"
# Rows 0..99 are gone; the second part keeps 100..149.
$CLICKHOUSE_CLIENT --query "
    SELECT count(), min(id), max(id) FROM t_mut_order;
    SELECT count(), countIf(is_done) $MUTATIONS;
    DROP TABLE t_mut_order;
"
