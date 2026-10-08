#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-ordinary-database
# no-parallel: the failpoint is global and fires once for whichever statement stores a removal batch first.
# All merges are stopped: a merge of any table stores a removal batch and would consume the global failpoint.

# Correct: a failed non-transactional `TRUNCATE` leaves the table intact, also after the table is loaded again.
# Today: the table is empty after the reload.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FAILPOINT=non_transactional_removal_store_fail_after_first_part

# A test killed by the runner's timeout must not leave the failpoint armed for the tests that follow.
function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM START MERGES" 2>/dev/null || true
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FAILPOINT" 2>/dev/null || true
}
trap cleanup EXIT
trap 'exit 1' TERM INT HUP

# Runs a statement with the failpoint armed and prints the error name it fails with.
function fail_statement()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FAILPOINT"
    $CLICKHOUSE_CLIENT -q "$1" 2>&1 | grep -o -m1 "CANNOT_WRITE_TO_FILE"
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FAILPOINT"
}

$CLICKHOUSE_CLIENT -n <<'EOF'
DROP TABLE IF EXISTS t;
CREATE TABLE t (n UInt64) ENGINE = MergeTree ORDER BY n SETTINGS old_parts_lifetime = 3600;
SYSTEM STOP MERGES;
SYSTEM STOP MERGES t;
INSERT INTO t VALUES (1);
INSERT INTO t VALUES (2);
EOF

fail_statement "TRUNCATE TABLE t"

$CLICKHOUSE_CLIENT -q "SELECT 'after failed truncate', count(), sum(n) FROM t"

$CLICKHOUSE_CLIENT -q "DETACH TABLE t"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE t"

$CLICKHOUSE_CLIENT -q "SELECT 'after reload', count(), sum(n) FROM t"

$CLICKHOUSE_CLIENT -q "DROP TABLE t"
