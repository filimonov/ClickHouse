#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TEST_DB="${CLICKHOUSE_DATABASE}"
U_CREATE="cv_create_${TEST_DB}"
U_DROP="cv_drop_${TEST_DB}"
U_GET="cv_get_${TEST_DB}"
U_SYS="cv_sys_${TEST_DB}"
U_RO="cv_ro_${TEST_DB}"

cleanup() {
    $CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${U_CREATE}, ${U_DROP}, ${U_GET}, ${U_SYS}, ${U_RO}"
    $CLICKHOUSE_CLIENT --query "DROP VARIABLE IF EXISTS local.access_cv"
}
trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT --query "CREATE USER ${U_CREATE}, ${U_DROP}, ${U_GET}, ${U_SYS}, ${U_RO}"

# Seed a variable under the default (unrestricted) user.
$CLICKHOUSE_CLIENT --query "CREATE VARIABLE local.access_cv AS toUInt32(7)"

# --- CREATE_VARIABLE required for CREATE VARIABLE -------------------------
$CLICKHOUSE_CLIENT --user ${U_CREATE} --query "CREATE VARIABLE local.access_user AS 1" 2>&1 \
    | grep -q ACCESS_DENIED && echo "create_without_grant=DENIED" || echo "create_without_grant=ALLOWED"

$CLICKHOUSE_CLIENT --query "GRANT CREATE_VARIABLE ON *.* TO ${U_CREATE}"
$CLICKHOUSE_CLIENT --user ${U_CREATE} --query "CREATE VARIABLE local.access_user AS toUInt32(1)" 2>&1 | head -1
$CLICKHOUSE_CLIENT --user ${U_CREATE} --query "DROP VARIABLE local.access_user" 2>&1 \
    | grep -q ACCESS_DENIED && echo "drop_without_grant=DENIED" || echo "drop_without_grant=ALLOWED"
$CLICKHOUSE_CLIENT --query "DROP VARIABLE local.access_user"

# --- DROP_VARIABLE required for DROP VARIABLE -----------------------------
$CLICKHOUSE_CLIENT --user ${U_DROP} --query "DROP VARIABLE local.access_cv" 2>&1 \
    | grep -q ACCESS_DENIED && echo "bare_drop=DENIED" || echo "bare_drop=ALLOWED"

# --- getVariable requires getVariable access ------------------------------
$CLICKHOUSE_CLIENT --user ${U_GET} --query "SELECT getVariable('local.access_cv')" 2>&1 \
    | grep -q ACCESS_DENIED && echo "get_without_grant=DENIED" || echo "get_without_grant=ALLOWED"

$CLICKHOUSE_CLIENT --query "GRANT getVariable ON *.* TO ${U_GET}"
$CLICKHOUSE_CLIENT --user ${U_GET} --query "SELECT getVariable('local.access_cv')"

# system.custom_variables is visible to users with either SHOW_CUSTOM_VARIABLES
# or getVariable; asserting the getVariable branch pins that fallback.
$CLICKHOUSE_CLIENT --user ${U_GET} --query \
    "SELECT count() FROM system.custom_variables WHERE scope = 'local' AND name = 'access_cv'"

# --- SYSTEM REFRESH VARIABLE requires SYSTEM_CUSTOM_VARIABLES -------------
$CLICKHOUSE_CLIENT --user ${U_SYS} --query "SYSTEM REFRESH VARIABLE local.access_cv" 2>&1 \
    | grep -q ACCESS_DENIED && echo "sys_refresh_without_grant=DENIED" || echo "sys_refresh_without_grant=ALLOWED"

$CLICKHOUSE_CLIENT --query "GRANT SYSTEM_CUSTOM_VARIABLES ON *.* TO ${U_SYS}"
$CLICKHOUSE_CLIENT --user ${U_SYS} --query "SYSTEM REFRESH VARIABLE local.access_cv" 2>&1 | head -1

# --- Readonly profile blocks CREATE VARIABLE ------------------------------
$CLICKHOUSE_CLIENT --query "GRANT CREATE_VARIABLE ON *.* TO ${U_RO}"
$CLICKHOUSE_CLIENT --user ${U_RO} --readonly 1 --query "CREATE VARIABLE local.access_ro AS 1" 2>&1 \
    | grep -qE "ACCESS_DENIED|READONLY" && echo "readonly_blocks_create=DENIED" || echo "readonly_blocks_create=ALLOWED"
