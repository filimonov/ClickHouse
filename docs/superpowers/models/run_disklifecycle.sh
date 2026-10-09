#!/usr/bin/env bash
# Run every CaDiskLifecycle config and print a one-line PASS/FAIL verdict per config.
# Model: the pool lifecycle of a writable CAS mount (absorbing Stopping and Closed), its one lease
# thread (renewal and reacquire in sequence), the process-stop request shared by two disks, the GC
# scheduler, and the `SYSTEM CONTENT ADDRESSED FORGET` protocol.
#
# Expectations are asserted BY NAME. Sabotages run first: a green is only evidence once the property
# it rests on has been seen red.
#
#   sab_nogcselfexit            -> GcExitsAfterTerminal (temporal)  the GC scheduler never exits on a
#                                                                   terminal pool
#   sab_armafterintent          -> NoJoinWindowReclaim     arming ignores a stop and a terminal
#                                                          lifecycle: a claim arms inside the join window
#   sab_unearnedfarewell        -> I2EarnedFarewell        the farewell regardless of the drain outcome
#   sab_renewafterintent        -> RenewNotAfterIntent     a renewal starts after the FORGET intent
#   sab_liveunready             -> LiveOnlyWhenAdmissible  a claim with a too-old lease arms the
#                                                          fence and reports Live without the renewal
#   sab_latesuccessrearm        -> NoArmAfterLatch         a renewal success arms a latched pool
#   sab_interferearmrace        -> TerminalAbsorbing       interference between the claim's arming
#                                                          decision and the open loses
#   sab_forgetlosesarm          -> ForgetWinsArm           FORGET's intent in the same window loses
#   sab_secondstop              -> OneProcessStop          no once-flag: a second disk stops the process
#                                                          again
#   sab_farewellonstopping      -> NoFarewellOnStopping    the teardown writes the farewell on a Stopping
#                                                          pool
#   sab_reacqallocperattempt    -> OneAllocationPerFence   every claim attempt allocates an epoch, not one
#                                                          per fence
#   sab_oldopunchecked          -> OldEpochOpRefused       an operation admitted under the old epoch lands
#                                                          under the new one
#   sab_lossreacquires          -> I9LossTerminates        a loss found by the claim reacquires again
#   sab_drainignorespublish     -> NoLandingAfterFarewell  the drain ignores a snapshot publish in flight
#   sab_farewellfromterminal    -> NoFarewellAfterUnclearRenewal  a renewal that ended without a
#                                                          confirmed lease still lets the farewell out
#   old_farewellleasebound      -> GREEN   the farewell needs an unexpired lease: the old bound, under which
#                                          WNoFarewellAfterExpiry holds
#   main                        -> GREEN   every invariant, action property and liveness property
#                                          under FairSpec
#
# Witnesses are negated reachability -- a VIOLATION is the evidence:
#
#   witness_forgetdone            -> WForgetNeverDone         FORGET completes
#   witness_racedreplaced         -> WNoRacedStopping         FORGET completes on a pool a racing
#                                                             loss observation left Stopping
#   witness_livebyrenewal         -> WNoLiveByRenewal         a renewal arms and reports Live for a
#                                                             claim whose lease was too old
#   witness_forgetfarewell        -> WNoForgetFarewell        FORGET still writes the farewell
#   witness_pauseresume           -> WNoPauseResume           expiry, renewal, writes under the same epoch
#   witness_farewellafterexpiry   -> WNoFarewellAfterExpiry   the farewell is written after the lease expired
#   witness_reacquire             -> WNoReacquire             a reacquire opens writes under the next epoch
#   witness_interferencestop      -> WNoInterferenceStop      interference stops the disk; both disks
#                                                             raise; one request
#
# TLC names no property in a liveness counterexample ("Error: Temporal properties were violated."),
# so the `temporal` expectation asserts what can be asserted: a temporal violation happened, NO
# invariant violation happened, and the cfg declares exactly the one property named in the table.
# That keeps an unrelated red from passing as this one.
#
# Exits nonzero if any expectation is unmet.
#
# ROWS="name1 name2" runs only those rows; TLC_TIMEOUT=<seconds> overrides the per-row timeout.
#
# This is a whole-suite runner: it owns the config list and takes no positional arguments. To run one
# config by hand:
#   java -XX:+UseParallelGC -cp ../../../tmp/tla2tools.jar tlc2.TLC -workers 1 \
#        -config CaDiskLifecycle_<name>.cfg CaDiskLifecycle.tla
#
# `-workers 1`, NOT `-workers auto`, and that is deliberate (see run_refcatalog.sh for the full
# rationale: parallel BFS makes the reported depth, the state counts and WHICH shortest
# counterexample TLC prints vary between identical runs). Override with TLC_WORKERS=auto if you only
# want a verdict and not the numbers.
set -uo pipefail
cd "$(dirname "$0")"
JAR=../../../tmp/tla2tools.jar
[[ -f "$JAR" ]] || { echo "jar not found: $JAR" >&2; exit 3; }
source ./tlc_temporal_gate.sh
check_tlc_pin "$JAR" || exit 3
MODULE=CaDiskLifecycle

# name  expectation(green|violation|temporal)  expected-invariant/property(asserted, not just logged)
CONFIGS=(
  "sab_nogcselfexit             temporal   GcExitsAfterTerminal"
  "sab_unearnedfarewell         violation  I2EarnedFarewell"
  "sab_renewafterintent         violation  RenewNotAfterIntent"
  "sab_latesuccessrearm         violation  NoArmAfterLatch"
  "sab_secondstop               violation  OneProcessStop"
  "sab_farewellonstopping       violation  NoFarewellOnStopping"
  "sab_reacqallocperattempt     violation  OneAllocationPerFence"
  "sab_oldopunchecked           violation  OldEpochOpRefused"
  "sab_lossreacquires           violation  I9LossTerminates"
  "sab_drainignorespublish      violation  NoLandingAfterFarewell"
  "sab_farewellfromterminal     violation  NoFarewellAfterUnclearRenewal"
  "sab_armafterintent           violation  NoJoinWindowReclaim"
  "sab_liveunready              violation  LiveOnlyWhenAdmissible"
  "sab_interferearmrace         violation  TerminalAbsorbing"
  "sab_forgetlosesarm           violation  ForgetWinsArm"
  "main                         green      -"
  "witness_forgetdone           violation  WForgetNeverDone"
  "witness_racedreplaced        violation  WNoRacedStopping"
  "witness_livebyrenewal        violation  WNoLiveByRenewal"
  "witness_forgetfarewell       violation  WNoForgetFarewell"
  "witness_pauseresume          violation  WNoPauseResume"
  "witness_interferencestop     violation  WNoInterferenceStop"
  "witness_reacquire            violation  WNoReacquire"
  "witness_farewellafterexpiry  violation  WNoFarewellAfterExpiry"
  "old_farewellleasebound       green      -"
)
check_tlc_temporal_expectations "$JAR" "${CONFIGS[@]}" || exit 4

# The PROPERTY/PROPERTIES names a cfg declares, one per line (used by the `temporal` assertion).
declared_properties() {
  awk 'BEGIN { split("SPECIFICATION SPECIFICATIONS INIT NEXT INVARIANT INVARIANTS PROPERTY \
                      PROPERTIES CONSTANT CONSTANTS CONSTRAINT CONSTRAINTS ACTION_CONSTRAINT \
                      SYMMETRY VIEW CHECK_DEADLOCK ALIAS POSTCONDITION", k, " ")
              for (i in k) kw[k[i]] = 1 }
       { sub(/\\\*.*/, ""); if (NF == 0) next
         if ($1 in kw) { inprop = ($1 == "PROPERTY" || $1 == "PROPERTIES")
                         if (inprop) for (i = 2; i <= NF; i++) print $i
                         next }
         if (inprop) for (i = 1; i <= NF; i++) print $i }' "$1"
}

overall=0
printf '%-30s %-11s %-40s %-8s %s\n' "CONFIG" "EXPECT" "RESULT" "SECONDS" "VERDICT"
for row in "${CONFIGS[@]}"; do
  read -r name expect want <<<"$row"
  if [[ -n "${ROWS:-}" && " $ROWS " != *" $name "* ]]; then continue; fi
  cfg="${MODULE}_${name}.cfg"
  log="../../../tmp/tlc_${MODULE}_${name}.log"
  meta="../../../tmp/tlc-meta-lifecycle-${name}"
  rm -rf "$meta"
  start=$SECONDS
  timeout "${TLC_TIMEOUT:-3600}" /usr/bin/java -XX:+UseParallelGC ${TLC_JAVA_OPTS:-} -cp "$JAR" tlc2.TLC \
    -metadir "$meta" -workers "${TLC_WORKERS:-1}" -config "$cfg" "$MODULE.tla" >"$log" 2>&1
  rc=$?
  elapsed=$((SECONDS - start))

  if grep -q "No error has been found" "$log"; then
    result="green"
  elif grep -qE "(Invariant|Property|Action property) [A-Za-z0-9_]+ is violated" "$log"; then
    result="violation:$(grep -oE '(Invariant|Property|Action property) [A-Za-z0-9_]+ is violated' "$log" \
                        | head -1 | sed -E 's/.* ([A-Za-z0-9_]+) is violated/\1/')"
  elif grep -qE "Temporal propert(ies were|y [A-Za-z0-9_]+ was) violated" "$log"; then
    result="temporal:$(declared_properties "$cfg" | paste -sd, -)"
  elif [[ $rc -eq 124 ]]; then
    result="incomplete"
  else
    result="error"
  fi

  verdict="FAIL"
  case "$expect" in
    green)      [[ "$result" == "green" ]] && verdict="PASS" ;;
    violation)  [[ "$result" == "violation:${want}" ]] && verdict="PASS" ;;
    temporal)   [[ "$result" == "temporal:${want}" ]] && verdict="PASS" ;;
  esac
  [[ "$verdict" == "FAIL" ]] && overall=1

  printf '%-30s %-11s %-40s %-8s %s\n' "$name" "$expect" "$result" "$elapsed" "$verdict"
done

echo
if [[ $overall -eq 0 ]]; then echo "ALL EXPECTATIONS MET"; else echo "SOME EXPECTATIONS UNMET"; fi
exit $overall
