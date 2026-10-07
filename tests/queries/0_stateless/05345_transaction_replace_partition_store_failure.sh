#!/usr/bin/env bash
# Tags: no-parallel, no-ordinary-database
# no-parallel: the failpoint is global and fires once for whichever statement stores a removal batch first.
# All merges are stopped: a merge of any table stores a removal batch and would consume the global failpoint.

# Correct: a failed non-transactional `REPLACE PARTITION` leaves the destination as it was.
# Today: the new part stays committed next to the parts that were not replaced.

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
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;
CREATE TABLE src (n UInt64) ENGINE = MergeTree ORDER BY n;
CREATE TABLE dst (n UInt64) ENGINE = MergeTree ORDER BY n;
SYSTEM STOP MERGES;
SYSTEM STOP MERGES dst;
INSERT INTO src VALUES (100);
INSERT INTO dst VALUES (1);
INSERT INTO dst VALUES (2);
EOF

fail_statement "ALTER TABLE dst REPLACE PARTITION tuple() FROM src"

$CLICKHOUSE_CLIENT -q "SELECT 'after failed replace', arraySort(groupArray(n)) FROM dst"

# The failure left no lock behind: the same statement succeeds now.
$CLICKHOUSE_CLIENT -q "ALTER TABLE dst REPLACE PARTITION tuple() FROM src"
$CLICKHOUSE_CLIENT -q "SELECT 'after retry', arraySort(groupArray(n)) FROM dst"

$CLICKHOUSE_CLIENT -q "DROP TABLE src"
$CLICKHOUSE_CLIENT -q "DROP TABLE dst"
