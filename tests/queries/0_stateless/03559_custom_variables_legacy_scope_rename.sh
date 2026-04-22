#!/usr/bin/env bash
# Regression test for the `local_persistent` → `local` collapse: a
# pre-existing `variable_local_persistent_<name>.sql` file on disk must be
# picked up on startup, used as a plain `local` variable, and renamed to
# `variable_local_<name>.sql` so subsequent starts never see the legacy form.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TMP_DIR="${CLICKHOUSE_TMP}/custom_variables_legacy_${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$TMP_DIR"
mkdir -p "$TMP_DIR/metadata"
mkdir -p "$TMP_DIR/custom_variables"

LEGACY_NAME="legacy_scope"
LEGACY_FILE="${TMP_DIR}/custom_variables/variable_local_persistent_${LEGACY_NAME}.sql"
NEW_FILE="${TMP_DIR}/custom_variables/variable_local_${LEGACY_NAME}.sql"

# Seed a definition in the pre-collapse format. The scope token inside the
# blob is the old "local_persistent.<name>"; the loader must accept it.
cat > "${LEGACY_FILE}" <<SQL
CREATE VARIABLE local_persistent.${LEGACY_NAME} AS _CAST(7, 'UInt64')
SQL

${CLICKHOUSE_LOCAL} --path "$TMP_DIR" --query "SELECT getVariable('local.${LEGACY_NAME}')"

if [ -f "${LEGACY_FILE}" ]; then
    echo "legacy_file_still_there"
else
    echo "legacy_file_removed"
fi
if [ -f "${NEW_FILE}" ]; then
    echo "new_file_present"
else
    echo "new_file_missing"
fi

# Second start must see the new-format file and nothing else — no re-migration noise.
${CLICKHOUSE_LOCAL} --path "$TMP_DIR" --query "SELECT scope, value FROM system.custom_variables WHERE name = '${LEGACY_NAME}'"

${CLICKHOUSE_LOCAL} --path "$TMP_DIR" --query "DROP VARIABLE local.${LEGACY_NAME}" >/dev/null
rm -rf "$TMP_DIR"
