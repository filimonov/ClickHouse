#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database, no-parallel-replicas, no-object-storage, no-encrypted-storage

# Correct: a part inserted outside a transaction survives a reload that finds only a first-store `txn_version.txt.tmp`.
# Today: the loader discards the part as an interrupted creation, so an acknowledged INSERT is lost.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CURDIR"/transactions.lib

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_torn_first_store SYNC"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_torn_first_store (n UInt64) ENGINE = MergeTree ORDER BY n"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_torn_first_store SELECT number FROM numbers(3)"

part_path=$($CLICKHOUSE_CLIENT -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_torn_first_store' AND active")
record="$CLICKHOUSE_TMP/${CLICKHOUSE_TEST_UNIQUE_NAME}_first_store_record"

echo "--- no metadata file before the transaction"
test -e "${part_path}txn_version.txt" && echo "present" || echo "absent"

tx 1 "BEGIN TRANSACTION" > /dev/null
tx 1 "ALTER TABLE t_torn_first_store DROP PARTITION tuple()" > /dev/null
cp "${part_path}txn_version.txt" "$record"
tx 1 "ROLLBACK" > /dev/null

echo "--- the first store wrote a removal lock for a part created outside a transaction"
grep -c '^creation_tid: (1, 1, 00000000-0000-0000-0000-000000000000)$' "$record"
grep -c '^removal_csn: 0$' "$record"

$CLICKHOUSE_CLIENT -q "DETACH TABLE t_torn_first_store"
rm "${part_path}txn_version.txt"
cp "$record" "${part_path}txn_version.txt.tmp"
$CLICKHOUSE_CLIENT --send_logs_level=error -q "ATTACH TABLE t_torn_first_store"

echo "--- the part survives the reload"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(n) FROM t_torn_first_store"
$CLICKHOUSE_CLIENT -q "SELECT active FROM system.parts WHERE database = currentDatabase() AND table = 't_torn_first_store' ORDER BY name"

rm -f "$record"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_torn_first_store SYNC"
