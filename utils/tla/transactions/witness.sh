#!/usr/bin/env bash
# Run one witness: inject the defect named by <WitnessName> and check only <Property>.
#
# Usage: witness.sh <Scenario> <Property> [<WitnessName>] [workers]
#
# A witness is a single named change to the model, guarded by Witness("<WitnessName>") in the action, that must
# make <Property> fail. The run therefore checks that property and nothing else: other properties are expected to
# fail too and are not the subject of the run. <WitnessName> defaults to <Property>; it differs when one property
# has several witnesses (the Assert_validateInfo_* variants).
#
# Two-change witnesses: when the modules also define Witness("<WitnessName>_only1") and Witness("<WitnessName>_only2")
# at the two sites, the witness is minimal only if either change alone leaves the property green. A RED main run is
# then followed by the two single-change runs, and the witness passes only if both of them are GREEN.
#
# Prints one line: RED|GREEN|ERROR|TIMEOUT <Property> states=<distinct> time=<s>
# Exit: 0 the witness fired (and, for a two-change witness, is minimal), 1 the run was green, 2 TLC error or timeout
#       or a two-change witness that is not minimal.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
SCENARIO="${1:?scenario name, e.g. Base}"
PROPERTY="${2:?property name, e.g. StableRead}"
WNAME="${3:-$PROPERTY}"
WORKERS="${4:-auto}"
JAR="$ROOT/tmp/tla2tools.jar"
LIMIT="${WITNESS_TIMEOUT:-600}"

[ -f "$JAR" ] || { echo "ERROR $PROPERTY states=0 time=0"; echo "missing $JAR" >&2; exit 2; }
[ -f "$HERE/MC_$SCENARIO.tla" ] && [ -f "$HERE/MC_$SCENARIO.cfg" ] || {
  echo "ERROR $PROPERTY states=0 time=0"; echo "missing $HERE/MC_$SCENARIO.{tla,cfg}" >&2; exit 2; }
grep -q "Witness(\"$WNAME\")" "$HERE"/*.tla || {
  echo "ERROR $PROPERTY states=0 time=0"; echo "no Witness(\"$WNAME\") hook in the modules" >&2; exit 2; }

# ---- one TLC run with the witness $1 applied; echoes "<verdict> <distinct> <seconds>"
run_one() {
  local wname="$1"
  local out="$ROOT/tmp/tla/w_${SCENARIO}_$wname"
  rm -rf "$out"; mkdir -p "$out"

  # The checked property: an action property is declared in Invariants.tla as "Name == [][...]_vars".
  local keyword="INVARIANT"
  grep -qE "^$PROPERTY == \[\]\[" "$HERE/Invariants.tla" && keyword="PROPERTY"

  # The configuration of the scenario, minus every property it checks, plus the one under test. A property list
  # may continue over following lines, so drop lines until the next configuration keyword.
  awk -v kw="$keyword" -v prop="$PROPERTY" -v wn="$wname" '
    function iskey(w) { return w ~ /^(SPECIFICATION|SPEC|INIT|NEXT|CONSTANT|CONSTANTS|INVARIANT|INVARIANTS|PROPERTY|PROPERTIES|SYMMETRY|VIEW|CONSTRAINT|CONSTRAINTS|ACTION_CONSTRAINT|ACTION_CONSTRAINTS|ALIAS|POSTCONDITION|CHECK_DEADLOCK)$/ }
    { first = $1 }
    iskey(first) { drop = (first ~ /^(INVARIANT|INVARIANTS|PROPERTY|PROPERTIES)$/) }
    drop { next }
    /^[[:space:]]*WITNESS_NAME[[:space:]]*=/ { printf "  WITNESS_NAME = \"%s\"\n", wn; next }
    { print }
    END { printf "%s %s\n", kw, prop }
  ' "$HERE/MC_$SCENARIO.cfg" > "$out/MC_W.cfg"

  sed "s/^\(----* *MODULE *\)MC_$SCENARIO\( *----*\)\$/\1MC_W\2/" "$HERE/MC_$SCENARIO.tla" > "$out/MC_W.tla"
  grep -q 'MODULE MC_W' "$out/MC_W.tla" || { echo "ERROR 0 0"; return; }

  local t0=$SECONDS
  ( cd "$out" && timeout "$LIMIT" java -XX:+UseParallelGC -Xmx16g -DTLA-Library="$HERE" -cp "$JAR" tlc2.TLC \
      -workers "$WORKERS" -deadlock -metadir "$out/states" -config MC_W.cfg MC_W.tla ) > "$out/tlc.log" 2>&1
  local rc=$? secs=$((SECONDS - t0))
  rm -rf "$out/states"
  local distinct
  # TLC prints the final count bare and the Progress counts with thousands separators; accept both, so that a
  # run that did not finish reports what it reached rather than the tail of a grouped number.
  distinct=$(grep -oE '[0-9][0-9,]* distinct states found' "$out/tlc.log" | tail -1 | grep -oE '^[0-9][0-9,]*' | tr -d ',')
  [ -n "${distinct:-}" ] || distinct=0

  if grep -qE "^Error: (Invariant|Action property) $PROPERTY is violated" "$out/tlc.log"; then
    awk '/^Error: /,0' "$out/tlc.log" > "$out/trace.txt"
    echo "RED $distinct $secs"
  elif grep -q 'Model checking completed. No error has been found' "$out/tlc.log"; then
    echo "GREEN $distinct $secs"
  elif [ $rc -eq 124 ]; then
    echo "TIMEOUT $distinct $secs"
  else
    echo "ERROR $distinct $secs"
  fi
}

read -r VERDICT DISTINCT SECS <<< "$(run_one "$WNAME")"
echo "$VERDICT $PROPERTY states=$DISTINCT time=$SECS"

case "$VERDICT" in
  RED) ;;
  GREEN)   exit 1 ;;
  TIMEOUT) echo "see $ROOT/tmp/tla/w_${SCENARIO}_$WNAME/tlc.log" >&2; exit 2 ;;
  *)       tail -15 "$ROOT/tmp/tla/w_${SCENARIO}_$WNAME/tlc.log" >&2; exit 2 ;;
esac

# ---- minimality of a two-change witness
if grep -q "Witness(\"${WNAME}_only1\")" "$HERE"/*.tla && grep -q "Witness(\"${WNAME}_only2\")" "$HERE"/*.tla; then
  RC=0
  for half in "${WNAME}_only1" "${WNAME}_only2"; do
    read -r V D S <<< "$(run_one "$half")"
    echo "$V $PROPERTY states=$D time=$S ($half, expected GREEN)"
    [ "$V" = "GREEN" ] || RC=2
  done
  [ $RC -eq 0 ] || { echo "witness $WNAME is not minimal: one change alone already violates $PROPERTY" >&2; exit 2; }
fi
exit 0
