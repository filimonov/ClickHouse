#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

TABLE=t_txn_version_recreate_on_append

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE ${TABLE}
    (
        x UInt64
    )
    ENGINE = MergeTree
    ORDER BY x
"

${CLICKHOUSE_CLIENT} -n -q "
    BEGIN TRANSACTION;
    INSERT INTO ${TABLE} SELECT number FROM numbers(10);
    COMMIT;
"

part_path=$(${CLICKHOUSE_CLIENT} -q "
    SELECT path
    FROM system.parts
    WHERE database = currentDatabase() AND table = '${TABLE}' AND active
    ORDER BY name
    LIMIT 1
    FORMAT TSVRaw
")

rm -f "${part_path}/txn_version.txt"

${CLICKHOUSE_CLIENT} -n -q "
    BEGIN TRANSACTION;
    TRUNCATE TABLE ${TABLE};
    ROLLBACK;
"

part_path_after=$(${CLICKHOUSE_CLIENT} -q "
    SELECT path
    FROM system.parts
    WHERE database = currentDatabase() AND table = '${TABLE}' AND active
    ORDER BY name
    LIMIT 1
    FORMAT TSVRaw
")

if [[ -f "${part_path_after}/txn_version.txt" ]]; then
    head -n 1 "${part_path_after}/txn_version.txt"
else
    echo "MISSING"
fi

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${TABLE}"
