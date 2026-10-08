#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-ordinary-database
# no-parallel: both failpoints are global and fire once for whichever statement reaches them first.
# All merges are stopped: a merge of any table stores a removal batch and would consume the global failpoint.

# Correct: a failed `DROP PARTITION` keeps its rows even when marking the rollback of its parts fails too.
# Today: the empty covering part stays on disk and the table is empty after the reload.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

STORE_FAILPOINT=non_transactional_removal_store_fail_after_first_part
MARK_FAILPOINT=version_metadata_store_creation_csn_fail

# A test killed by the runner's timeout must not leave a failpoint armed for the tests that follow.
function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM START MERGES" 2>/dev/null || true
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $STORE_FAILPOINT" 2>/dev/null || true
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $MARK_FAILPOINT" 2>/dev/null || true
}
trap cleanup EXIT
trap 'exit 1' TERM INT HUP

$CLICKHOUSE_CLIENT -n <<'SQL'
DROP TABLE IF EXISTS t;
CREATE TABLE t (n UInt64) ENGINE = MergeTree ORDER BY n SETTINGS old_parts_lifetime = 3600;
SYSTEM STOP MERGES;
SYSTEM STOP MERGES t;
INSERT INTO t VALUES (1);
INSERT INTO t VALUES (2);
SQL

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $STORE_FAILPOINT"
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $MARK_FAILPOINT"
# The failure of the rollback's own mark is logged at error level; keep it out of the client output.
$CLICKHOUSE_CLIENT --send_logs_level=none -q "ALTER TABLE t DROP PARTITION tuple()" 2>&1 | grep -o -m1 "CANNOT_WRITE_TO_FILE"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $STORE_FAILPOINT"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $MARK_FAILPOINT"

$CLICKHOUSE_CLIENT -q "SELECT 'after failed drop', count(), sum(n) FROM t"

$CLICKHOUSE_CLIENT -q "DETACH TABLE t"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE t"

$CLICKHOUSE_CLIENT -q "SELECT 'after reload', count(), sum(n) FROM t"

$CLICKHOUSE_CLIENT -q "DROP TABLE t"
