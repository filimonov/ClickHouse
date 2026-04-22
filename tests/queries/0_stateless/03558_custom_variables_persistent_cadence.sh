#!/usr/bin/env bash
# Tests that, after a restart, a refreshable local_persistent variable
# resumes its schedule from the persisted last_successful_update_time
# instead of restarting the clock. Without the fix, an almost-due
# refresh would be postponed by nearly a full interval.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TMP_DIR="${CLICKHOUSE_TMP}/custom_variables_cadence_${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$TMP_DIR"
mkdir -p "$TMP_DIR"
mkdir -p "$TMP_DIR/metadata"

${CLICKHOUSE_LOCAL} --path "$TMP_DIR" --multiquery --query "
CREATE TABLE src (x UInt64) ENGINE=Memory;
INSERT INTO src VALUES (1);
CREATE VARIABLE local_persistent.cv_hourly REFRESH AFTER 1 HOUR AS (SELECT max(x) FROM src);
"

VALUES_DIR="${TMP_DIR%/}/custom_variables_values"
VALUE_FILE="${VALUES_DIR}/cv_hourly.bin"

# Rewrite both timestamps in the persisted blob so the variable looks like it
# was last refreshed 3570 seconds ago — the next scheduled tick under
# EVERY 1 HOUR should therefore be ~30 seconds from "now" on reload.
python3 - "$VALUE_FILE" <<'PY'
import struct, sys, time

path = sys.argv[1]
data = bytearray(open(path, "rb").read())

def read_varuint(buf, pos):
    value = 0
    shift = 0
    while True:
        b = buf[pos]; pos += 1
        value |= (b & 0x7F) << shift
        if b < 0x80:
            return value, pos
        shift += 7

pos = 0
pos += 8  # format version
length, pos = read_varuint(data, pos)
pos += length  # type name
has_value = data[pos]; pos += 1
if has_value:
    field_type = data[pos]; pos += 1
    assert field_type == 0x01, "Unexpected field type"
    _, pos = read_varuint(data, pos)
pos += 1  # is_valid

target = int(time.time()) - 3570
data[pos:pos + 8] = struct.pack("<q", target)          # last_update_time
data[pos + 8:pos + 16] = struct.pack("<q", target)     # last_successful_update_time
open(path, "wb").write(data)
PY

# After reload, refresh_next_time should point to roughly 30 seconds from now,
# NOT an hour away — the scheduler must consult the persisted success time.
next_in_seconds="$(${CLICKHOUSE_LOCAL} --path "$TMP_DIR" --query "
SELECT toInt64(refresh_next_time) - toInt64(now())
FROM system.custom_variables
WHERE name = 'cv_hourly' AND scope = 'local_persistent'")"

# Accept up to 5 minutes of skew for CI jitter; anything approaching 3600
# seconds means the schedule was reset to the restart moment.
if [ -z "$next_in_seconds" ]; then
    echo "no_row"
elif [ "$next_in_seconds" -gt 300 ]; then
    echo "schedule_reset"
else
    echo "schedule_preserved"
fi

${CLICKHOUSE_LOCAL} --path "$TMP_DIR" --query "DROP VARIABLE local_persistent.cv_hourly" >/dev/null
rm -rf "$TMP_DIR"
