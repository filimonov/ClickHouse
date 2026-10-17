#!/usr/bin/env bash
# Run every CaRefTableSnapshotLogCore config and print a one-line PASS/FAIL verdict per config.
# Model: v9 contiguous ref stream (spec 2026-07-27-cas-ref-chain-complete-cut-design.md,
# §2 INV-1/INV-2/INV-4, §4 Recovery). Results and traces: CaRefTableSnapshotLogCore_RESULTS.md.
#
# Sabotages run FIRST: a green is only evidence once the property it rests on has been seen red.
#
#   sab_reuseafterambiguous  -> INV_NO_PHANTOM   every-attempt rule (INV-1)
#   sab_gaponfail            -> INV_DENSE        contiguity (INV-1); rev.4's "safe id gap"
#   sab_noseal               -> INV_RECOVERY     slot occupancy (INV-2); the flip's control
#   sab_blindput             -> INV_NO_GHOST     conditional create is the fence (INV-2)
#   sab_scanistruth          -> INV_RECOVERY     LIST is a zero-trust hint (§4/§5)
#   sab_cleanupaboveckpt     -> INV_RECOVERY     log deletion gated at _ckpt.base (INV-4)
#   sab_staleckptcorruption  -> INV_NOFAIL       snapshot deletion STRICTLY below base (INV-4)
#   sab_sealclobbersbase     -> INV_RECOVERY     the _ckpt semantic-max merge (INV-4): a
#                                                skipped merge loses data SILENTLY
#   sab_sealclobbersbase_nofail -> INV_NOFAIL    the same merge, second consequence:
#                                                regressed base -> deleted snapshot -> corrupt
#   sab_noseal_nolate        -> GREEN     the flip's control: _sab_noseal minus the straggler
#   v9_safe                  -> GREEN     the honest protocol, no straggler
#   v9_flip_latepred         -> GREEN     THE FLIP: rev.4's expected-fail is now a proof
#   *_deep                   -> GREEN     the two greens again at MaxSeq = 5
#   witness_hintlie          -> W_NO_HINT_HOLE   reachability witness: a complete-LOOKING
#                                                enumeration that omits a PRESENT log
#                                                (the observed 0x1430c/0x1430d shape).
#                                                A violation is the EVIDENCE, so it is a PASS.
#   frontier_sab_*           -> exact named Task 5b ordering/frontier invariant
#   frontier_sab_snapshotatseal -> snapshot bases never name epoch-seal records
#   frontier_witness_*       -> reachability controls for every crash/race recovery window
#   frontier_safe            -> GREEN     LogDurable -> FrontierDurable -> Installed -> Acknowledged
#   cur_lateappend*, fix_*, sab_a*, sab_b* -> CAS-366 epoch slice: a zombie append lands in a seal
#                               slot GC reclaimed (INV_ACKED_REACHABLE); see RESULTS "CAS-366"
#
# Env: REFSNAPLOG_ONLY="name ..." runs only those configs; REFSNAPLOG_EPOCH=1 runs the CAS-366
# configs; REFSNAPLOG_OUT=<dir> holds logs and metadirs (default ../../../tmp); TLC_TIMEOUT
# (default 1200 s), TLC_WORKERS (default 4), TLC_JAVA_OPTS (default -Xmx8g). Refuses to start a
# run with less than 40 GiB free on /.
# Exits nonzero if any expectation is unmet.
set -uo pipefail
cd "$(dirname "$0")"
JAR="${TLC_JAR:-../../../tmp/tla2tools.jar}"
source ./tlc_temporal_gate.sh
check_tlc_pin "$JAR" || exit 3
# Task 5b's checker evidence always includes the positive temporal smoke, even though this module's
# new obligations are safety invariants and reachability controls rather than liveness properties.
check_tlc_temporal_gate "$JAR" || exit 4
MODULE=CaRefTableSnapshotLogCore

# name  expectation(green|violation)  expected-invariant(for the log line)
CONFIGS=(
  "sab_reuseafterambiguous violation  INV_NO_PHANTOM"
  "sab_gaponfail           violation  INV_DENSE"
  "sab_noseal              violation  INV_RECOVERY"
  "sab_blindput            violation  INV_NO_GHOST"
  "sab_scanistruth         violation  INV_RECOVERY"
  "sab_cleanupaboveckpt    violation  INV_RECOVERY"
  "sab_staleckptcorruption violation  INV_NOFAIL"
  "sab_sealclobbersbase    violation  INV_RECOVERY"
  "sab_sealclobbersbase_nofail violation INV_NOFAIL"
  "witness_hintlie         violation  W_NO_HINT_HOLE"
  "frontier_sab_ackbefore       violation INV_ACK_NOT_BEFORE_FRONTIER"
  "frontier_sab_nextbefore      violation INV_NEXT_ID_NOT_BEFORE_FRONTIER"
  "frontier_sab_installabove    violation INV_INSTALL_NOT_ABOVE_FRONTIER"
  "frontier_sab_staleadvance    violation INV_EXACT_COMMITTED_FRONTIER"
  "frontier_sab_snapshotabove   violation INV_SNAPSHOT_NOT_ABOVE_FRONTIER"
  "frontier_sab_sealabove       violation INV_SEAL_NOT_ABOVE_FRONTIER"
  "frontier_sab_snapshotatseal  violation INV_SNAPSHOT_NOT_EPOCH_SEAL"
  "frontier_witness_crash_prepared violation W_CRASH_PREPARED"
  "frontier_witness_crash_logdurable violation W_CRASH_LOG_DURABLE"
  "frontier_witness_crash_frontierdurable violation W_CRASH_FRONTIER_DURABLE"
  "frontier_witness_crash_installed violation W_CRASH_INSTALLED"
  "frontier_witness_lostresponse violation W_LOST_FRONTIER_RESPONSE"
  "frontier_witness_exactsuccessor violation W_EXACT_SUCCESSOR_ADOPTED"
  "frontier_witness_oldwriterseal violation W_OLD_WRITER_LOST_TO_SEAL"
  "frontier_witness_issuedlinearizes violation W_ISSUED_CAS_LINEARIZED_AFTER_FENCE_MOVE"
  "frontier_witness_snapshot violation W_SNAPSHOT_PUBLISHED_AT_FRONTIER"
  "frontier_witness_seal violation W_SEAL_PUBLISHED_AT_FRONTIER"
  "frontier_witness_lostseal violation W_LOST_SEAL_RESPONSE_RESOLVED"
  "frontier_safe             green      -"
  "sab_noseal_nolate       green      -"
  "v9_safe                 green      -"
  "v9_flip_latepred        green      -"
  "v9_safe_deep            green      -"
  "v9_flip_latepred_deep   green      -"
  "cur_lateappend          violation  INV_ACKED_REACHABLE"
  "cur_lateappend_3ep      violation  INV_ACKED_REACHABLE"
  "sab_a                   violation  INV_ACKED_REACHABLE"
  "fix_a_3ep               violation  INV_ACKED_REACHABLE"
  "sab_b                   violation  INV_ACKED_REACHABLE"
  "fix_b_3ep               violation  INV_ACKED_REACHABLE"
  "sab_b3                  violation  INV_ACKED_REACHABLE"
  "sab_a2_le               violation  INV_SUCCESSOR_NOT_REFUSED"
  "fix_a_witness           violation  W_SEAL_SLOT_RELANDED"
  "fix_a2_witness          violation  W_SEAL_SLOT_RELANDED"
  "fix_a2_witness_straggler violation W_STRAGGLER_ACKED_AFTER_TAKEOVER"
  "fix_a2_witness_unresolved violation W_ZOMBIE_UNRESOLVED"
  "cur_lateappend_t1       green      -"
  "fix_a                   green      -"
  "fix_a2_2ep              green      -"
  "fix_a2                  green      -"
  "fix_b                   green      -"
  "fix_b2                  green      -"
  "fix_b3                  green      -"
)
EPOCH_CONFIGS=()
for row in "${CONFIGS[@]}"; do
  read -r n _ <<<"$row"
  case "$n" in cur_lateappend*|fix_a*|fix_b*|sab_a|sab_a2_le|sab_b|sab_b3) EPOCH_CONFIGS+=("$row") ;; esac
done

# Review iterations need a bounded official gate that exercises every Task 5b obligation plus one
# unchanged legacy control without repeating the resource-heavy deep evidence already recorded.
if [[ "${REFSNAPLOG_FOCUSED:-0}" == 1 ]]; then
  CONFIGS=(
    "frontier_sab_ackbefore       violation INV_ACK_NOT_BEFORE_FRONTIER"
    "frontier_sab_nextbefore      violation INV_NEXT_ID_NOT_BEFORE_FRONTIER"
    "frontier_sab_installabove    violation INV_INSTALL_NOT_ABOVE_FRONTIER"
    "frontier_sab_staleadvance    violation INV_EXACT_COMMITTED_FRONTIER"
    "frontier_sab_snapshotabove   violation INV_SNAPSHOT_NOT_ABOVE_FRONTIER"
    "frontier_sab_sealabove       violation INV_SEAL_NOT_ABOVE_FRONTIER"
    "frontier_sab_snapshotatseal  violation INV_SNAPSHOT_NOT_EPOCH_SEAL"
    "frontier_witness_crash_prepared violation W_CRASH_PREPARED"
    "frontier_witness_crash_logdurable violation W_CRASH_LOG_DURABLE"
    "frontier_witness_crash_frontierdurable violation W_CRASH_FRONTIER_DURABLE"
    "frontier_witness_crash_installed violation W_CRASH_INSTALLED"
    "frontier_witness_lostresponse violation W_LOST_FRONTIER_RESPONSE"
    "frontier_witness_exactsuccessor violation W_EXACT_SUCCESSOR_ADOPTED"
    "frontier_witness_oldwriterseal violation W_OLD_WRITER_LOST_TO_SEAL"
    "frontier_witness_issuedlinearizes violation W_ISSUED_CAS_LINEARIZED_AFTER_FENCE_MOVE"
    "frontier_witness_snapshot violation W_SNAPSHOT_PUBLISHED_AT_FRONTIER"
    "frontier_witness_seal violation W_SEAL_PUBLISHED_AT_FRONTIER"
    "frontier_witness_lostseal violation W_LOST_SEAL_RESPONSE_RESOLVED"
    "frontier_safe             green      -"
    "v9_safe                   green      -"
  )
fi

if [[ "${REFSNAPLOG_EPOCH:-0}" == 1 ]]; then
  CONFIGS=("${EPOCH_CONFIGS[@]}")
fi
if [[ -n "${REFSNAPLOG_ONLY:-}" ]]; then
  ALL=("${CONFIGS[@]}")
  CONFIGS=()
  for row in "${ALL[@]}"; do
    read -r n _ <<<"$row"
    for want_name in $REFSNAPLOG_ONLY; do [[ "$n" == "$want_name" ]] && CONFIGS+=("$row"); done
  done
fi
OUT="${REFSNAPLOG_OUT:-../../../tmp}"
mkdir -p "$OUT"

overall=0
printf '%-26s %-10s %-30s %-8s %s\n' "CONFIG" "EXPECT" "RESULT" "SECONDS" "VERDICT"
for row in "${CONFIGS[@]}"; do
  read -r name expect want <<<"$row"
  cfg="${MODULE}_${name}.cfg"
  log="$OUT/tlc_${MODULE}_${name}.log"
  meta="$OUT/tlc-meta-refsnap-${name}"
  free_gib=$(df -BG --output=avail / | tail -1 | tr -dc '0-9')
  if (( free_gib < 40 )); then
    echo "refusing $name: only ${free_gib} GiB free on /" >&2
    exit 5
  fi
  rm -rf "$meta"
  start=$SECONDS
  timeout "${TLC_TIMEOUT:-1200}" nice -n 10 /usr/bin/java -XX:+UseParallelGC ${TLC_JAVA_OPTS:--Xmx8g} \
    -cp "$JAR" tlc2.TLC -metadir "$meta" -workers "${TLC_WORKERS:-4}" -config "$cfg" "$MODULE.tla" >"$log" 2>&1
  rc=$?
  elapsed=$((SECONDS - start))
  rm -rf "$meta"

  if grep -q "No error has been found" "$log"; then
    result="green"
  elif grep -qE "(Invariant|Property|Action property) [A-Za-z0-9_]+ is violated" "$log"; then
    result="violation:$(grep -oE '(Invariant|Property|Action property) [A-Za-z0-9_]+ is violated' "$log" \
                        | head -1 | sed -E 's/.* ([A-Za-z0-9_]+) is violated/\1/')"
  elif [[ $rc -eq 124 ]]; then
    result="incomplete"
  else
    result="error"
  fi

  verdict="FAIL"
  case "$expect" in
    green)     [[ "$result" == "green" ]] && verdict="PASS" ;;
    violation) [[ "$result" == "violation:${want}" ]] && verdict="PASS" ;;
  esac
  [[ "$verdict" == "FAIL" ]] && overall=1

  printf '%-26s %-10s %-30s %-8s %s\n' "$name" "$expect" "$result" "$elapsed" "$verdict"
done

echo
if [[ $overall -eq 0 ]]; then echo "ALL EXPECTATIONS MET"; else echo "SOME EXPECTATIONS UNMET"; fi
exit $overall
