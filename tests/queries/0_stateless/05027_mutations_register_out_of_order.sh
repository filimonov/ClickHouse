#!/usr/bin/env bash
# Tags: no-parallel, no-shared-merge-tree
# no-parallel -- uses a server-wide failpoint that pauses the first mutation started on any MergeTree table.
# no-shared-merge-tree -- the race is in the in-memory mutation registration of StorageMergeTree.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT mt_pause_before_mutation_registration" 2>/dev/null
}
trap cleanup EXIT

# Polls a query until it returns the expected value; fails the test instead of hanging.
function wait_for_query_result()
{
    local query=$1
    local expected=$2
    for _ in $(seq 1 600); do
        local result
        result=$($CLICKHOUSE_CLIENT --query "$query")
        if [[ "$result" == "$expected" ]]; then
            return 0
        fi
        sleep 0.1
    done
    echo "Timed out waiting for '$expected' from: $query (last result: '$result')"
    exit 1
}

MUTATIONS_QUERY="FROM system.mutations WHERE database = currentDatabase() AND table = 't_mut_order'"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_mut_order;
    CREATE TABLE t_mut_order (id UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO t_mut_order SELECT number FROM numbers(100);
    SYSTEM ENABLE FAILPOINT mt_pause_before_mutation_registration;
"

# The first DELETE allocates block number N and writes mutation_N.txt, then pauses before registering the entry.
$CLICKHOUSE_CLIENT --lightweight_deletes_sync=0 --lightweight_delete_mode=alter_update --query "DELETE FROM t_mut_order WHERE id < 50" &
first_delete_pid=$!

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT mt_pause_before_mutation_registration PAUSE"

# The second DELETE gets block number N + 1 and registers first (the failpoint pauses only once).
$CLICKHOUSE_CLIENT --lightweight_deletes_sync=0 --lightweight_delete_mode=alter_update --query "DELETE FROM t_mut_order WHERE id >= 50"

# The background mutate task must postpone the part until mutation N is registered instead of applying N + 1 alone
# (which would move the part to data version N + 1 and lose mutation N forever).
wait_for_query_result "SELECT if(is_done, 'done', if(length(parts_postpone_reasons) > 0, 'postponed', 'pending')) $MUTATIONS_QUERY" "postponed"
$CLICKHOUSE_CLIENT --query "SELECT mapValues(parts_postpone_reasons) $MUTATIONS_QUERY"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT mt_pause_before_mutation_registration"
wait $first_delete_pid

wait_for_query_result "SELECT countIf(is_done) $MUTATIONS_QUERY" "2"
$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM t_mut_order;
    SELECT count(), countIf(is_done) $MUTATIONS_QUERY;
    DROP TABLE t_mut_order;
"
