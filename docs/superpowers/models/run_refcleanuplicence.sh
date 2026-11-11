#!/usr/bin/env bash
# Run every CaRefCleanupLicenceCore config and print a one-line PASS/FAIL verdict per config.
# Model: the per-life licence of covered ref-object cleanup (`Gc::cleanupRefObjects`), spec
# 2026-11-11-cas-ref-cleanup-per-life-licence-design.md. Results: CaRefCleanupLicenceCore_RESULTS.md.
#
# Sabotages and witnesses run FIRST: a green is only evidence once the property it rests on has been
# seen red and the interleavings it covers have been seen reachable.
#
#   sab_rederive          -> NamedLifeIntact   the delete resolves the name again after a rebirth
#   sab_reuseincarnation  -> NamedLifeIntact   rebirth reuses the erased incarnation
#   old_reuseincarnation  -> NamedLifeIntact   same, with the whole-catalog term of the old licence
#   sab_nolease           -> DeposedLeaderDeletesAtMostOneChunk   lease term dropped
#   sab_norow             -> ChangedRowDeletesAtMostOneChunk      row comparison dropped
#   witness_churnprogress -> WITNESS_LICENSED_AFTER_UNRELATED_WRITE
#   witness_rebirth       -> WITNESS_PENDING_CHUNK_ACROSS_REBIRTH
#   witness_twokeyplan    -> WITNESS_TWO_KEY_PLAN
#   safe                  -> GREEN   the specified licence
#   datafloor             -> GREEN   NamedLifeIntact with no licence term at all
#   nolife                -> GREEN   the row comparison implies the life term
#   old_churnblocked      -> GREEN   the old licence never licenses after a catalog write
#
# Exits nonzero if any expectation is unmet.
#
# `-workers 1`, NOT `-workers auto`: parallel BFS visits states in a nondeterministic order, so the
# reported depth and the printed trace are not reproducible run to run (see run_refcatalog.sh).
# Override with TLC_WORKERS=auto for a verdict without the numbers.
#
# COVERAGE=1 additionally re-runs `safe` under `-coverage 1` for the per-action invocation counts.
set -uo pipefail
cd "$(dirname "$0")"
JAR=../../../tmp/tla2tools.jar
[[ -f "$JAR" ]] || { echo "jar not found: $JAR" >&2; exit 3; }
MODULE=CaRefCleanupLicenceCore

# name               expectation(green|violation)  expected-invariant(asserted, not just logged)
CONFIGS=(
  "sab_rederive          violation  NamedLifeIntact"
  "sab_reuseincarnation  violation  NamedLifeIntact"
  "old_reuseincarnation  violation  NamedLifeIntact"
  "sab_nolease           violation  DeposedLeaderDeletesAtMostOneChunk"
  "sab_norow             violation  ChangedRowDeletesAtMostOneChunk"
  "witness_churnprogress violation  WITNESS_LICENSED_AFTER_UNRELATED_WRITE"
  "witness_rebirth       violation  WITNESS_PENDING_CHUNK_ACROSS_REBIRTH"
  "witness_twokeyplan    violation  WITNESS_TWO_KEY_PLAN"
  "safe                  green      -"
  "datafloor             green      -"
  "nolife                green      -"
  "old_churnblocked      green      -"
)

overall=0
printf '%-22s %-10s %-52s %-8s %s\n' "CONFIG" "EXPECT" "RESULT" "SECONDS" "VERDICT"
for row in "${CONFIGS[@]}"; do
  read -r name expect want <<<"$row"
  cfg="${MODULE}_${name}.cfg"
  log="../../../tmp/tlc_${MODULE}_${name}.log"
  meta="../../../tmp/tlc-meta-refcleanuplicence-${name}"
  rm -rf "$meta"
  start=$SECONDS
  /usr/bin/java -XX:+UseParallelGC ${TLC_JAVA_OPTS:-} -cp "$JAR" tlc2.TLC \
    -metadir "$meta" -workers "${TLC_WORKERS:-1}" -config "$cfg" "$MODULE.tla" >"$log" 2>&1
  elapsed=$((SECONDS - start))

  if grep -q "No error has been found" "$log"; then
    result="green"
  elif grep -q "is violated" "$log"; then
    result="violation:$(grep -oE '(Invariant|Property) [A-Za-z0-9_]+ is violated' "$log" | head -1 | awk '{print $2}')"
  else
    result="error"
  fi

  verdict="FAIL"
  case "$expect" in
    green)     [[ "$result" == "green" ]] && verdict="PASS" ;;
    violation) [[ "$result" == "violation:${want}" ]] && verdict="PASS" ;;
  esac
  [[ "$verdict" == "FAIL" ]] && overall=1

  printf '%-22s %-10s %-52s %-8s %s\n' "$name" "$expect" "$result" "$elapsed" "$verdict"
done

if [[ "${COVERAGE:-0}" == "1" ]]; then
  echo
  echo "COVERAGE=1: re-running the non-vacuity green with -coverage 1"
  log="../../../tmp/tlc_cov_${MODULE}_safe.log"
  meta="../../../tmp/tlc-meta-cov-refcleanuplicence-safe"
  rm -rf "$meta"
  /usr/bin/java -XX:+UseParallelGC ${TLC_JAVA_OPTS:-} -cp "$JAR" tlc2.TLC \
    -metadir "$meta" -workers "${TLC_WORKERS:-1}" -coverage 1 \
    -config "${MODULE}_safe.cfg" "$MODULE.tla" >"$log" 2>&1
  echo "  coverage safe: $log"
fi

echo
if [[ $overall -eq 0 ]]; then echo "ALL EXPECTATIONS MET"; else echo "SOME EXPECTATIONS UNMET"; fi
exit $overall
