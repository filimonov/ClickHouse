#!/usr/bin/env bash
# Run TLC on MC_<Scenario>. Usage: run_tlc.sh <Scenario> [workers]
# Exit: 0 green, 1 violation, 2 parse/config error or missing tool.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
SCENARIO="${1:?scenario name, e.g. Base}"
WORKERS="${2:-auto}"
JAR="$ROOT/tmp/tla2tools.jar"
OUT="$ROOT/tmp/tla/$SCENARIO"
mkdir -p "$OUT"
if ! unzip -l "$JAR" 2>/dev/null | grep -q 'tlc2/TLC.class'; then
  curl -fsSL -o "$JAR" https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar || { echo "cannot download tla2tools.jar" >&2; exit 2; }
fi
MODULE="MC_$SCENARIO"
[ -f "$HERE/$MODULE.tla" ] && [ -f "$HERE/$MODULE.cfg" ] || { echo "missing $HERE/$MODULE.tla or .cfg" >&2; exit 2; }
cd "$HERE"
timeout 2700 java -XX:+UseParallelGC -Xmx16g -cp "$JAR" tlc2.TLC -workers "$WORKERS" -deadlock \
     -metadir "$OUT/states" -config "$MODULE.cfg" "$MODULE.tla" > "$OUT/tlc.log" 2>&1
RC=$?
rm -rf "$OUT/states"
if grep -qE '^Error: (Invariant|Action property|Temporal properties)' "$OUT/tlc.log"; then
  awk '/^Error: /,0' "$OUT/tlc.log" > "$OUT/trace.txt"
  echo "VIOLATION scenario=$SCENARIO $(grep -oE 'Error: (Invariant|Action property) [A-Za-z_0-9]+' "$OUT/tlc.log" | head -1) (trace: $OUT/trace.txt)"
  exit 1
fi
if grep -q 'Model checking completed. No error has been found' "$OUT/tlc.log"; then
  grep -E 'states generated|Finished in' "$OUT/tlc.log" | tail -2
  echo "GREEN scenario=$SCENARIO"
  exit 0
fi
[ $RC -eq 124 ] && echo "TIMEOUT scenario=$SCENARIO after 45 min: a bounds or model defect, see $OUT/tlc.log"
echo "TLC ERROR scenario=$SCENARIO rc=$RC (see $OUT/tlc.log)"; tail -15 "$OUT/tlc.log"
exit 2
