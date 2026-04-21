#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TMP_DIR="${CLICKHOUSE_TMP}/custom_variables_local_persistent_${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$TMP_DIR"
mkdir -p "$TMP_DIR"
mkdir -p "$TMP_DIR/metadata"

${CLICKHOUSE_LOCAL} --path "$TMP_DIR" --multiquery --query "
CREATE TABLE src (x UInt64) ENGINE=Memory;
INSERT INTO src VALUES (1);
CREATE VARIABLE local_persistent.cv_from_table AS (SELECT max(x) FROM src);
CREATE VARIABLE local_persistent.cv_stale REFRESH EVERY 1 SECOND AS (SELECT toUInt64(now()));
DROP TABLE src;
"

VALUES_DIR="${TMP_DIR%/}/custom_variables_values"
STALE_FILE="${VALUES_DIR}/cv_stale.bin"

python3 - "$STALE_FILE" <<'PY'
import struct
import sys

path = sys.argv[1]
data = bytearray(open(path, "rb").read())

def read_varuint(buf, pos):
    value = 0
    shift = 0
    while True:
        b = buf[pos]
        pos += 1
        value |= (b & 0x7F) << shift
        if b < 0x80:
            return value, pos
        shift += 7

pos = 0
pos += 8  # version
length, pos = read_varuint(data, pos)
pos += length  # type name
has_value = data[pos]
pos += 1
if has_value:
    field_type = data[pos]
    pos += 1
    if field_type != 0x01:
        raise SystemExit("Unexpected field type")
    _, pos = read_varuint(data, pos)
pos += 1  # is_valid
pos += 8  # last_update_time
data[pos:pos + 8] = struct.pack("<q", 0)  # last_successful_update_time
open(path, "wb").write(data)
PY

output="$(${CLICKHOUSE_LOCAL} --path "$TMP_DIR" --multiquery --format=TSV --query "
SELECT getVariable('local_persistent.cv_from_table');
SELECT getVariable('local_persistent.cv_stale');
SELECT sleep(3) FORMAT Null;
SELECT getVariable('local_persistent.cv_stale');
")"

IFS=$'\n' read -r -d '' -a lines <<<"${output}"$'\0'
persist="${lines[0]}"
v1="${lines[1]}"
v2="${lines[2]}"

echo "$persist"
if [ "$v2" -gt "$v1" ]; then
    echo 1
else
    echo 0
fi

${CLICKHOUSE_LOCAL} --path "$TMP_DIR" --query "DROP VARIABLE local_persistent.cv_from_table"
if [ -f "${VALUES_DIR}/cv_from_table.bin" ]; then
    echo "value_file_remains"
else
    echo "value_file_removed"
fi
