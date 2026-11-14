#!/usr/bin/env bash
# Run the `CaRefLogOnlyAppendCore` battery and assert every result by exact invariant name.
# With one or more cfg names as arguments, run only those rows.
set -uo pipefail
cd "$(dirname "$0")"

JAR=${TLC_JAR:-../../../tmp/tla2tools.jar}
MODULE=CaRefLogOnlyAppendCore
source ./tlc_temporal_gate.sh
check_tlc_pin "$JAR" || exit 3

# name                       expectation(green|violation)  expected-invariant
CONFIGS=(
    "sab_sealblind               violation  INV_GC_PREFIX"
    "sab_sealblind_monotone      violation  PROP_MONOTONE"
    "sab_sealskip                violation  INV_ACKED_REACHABLE"
    "sab_selective_immediate     violation  INV_NO_DELETE_SERVABLE"
    "sab_gccursor_naive          violation  INV_GC_CUT_SOUND"
    "sab_gccursor_naive_zombie   violation  INV_GC_PREFIX"
    "sab_gccursor_probefirst_zombie violation INV_GC_PREFIX"
    "sab_gccursor_probefirst_lateland violation INV_GC_CUT_SOUND"
    "sab_gccursor_anyhead_zombie violation INV_GC_PREFIX"
    "sab_walk_norecheck          violation  INV_OWNER_EXACT"
    "sab_seal_norecheck_zombie   violation  INV_NO_DELETE_SERVABLE"
    "sab_seal_norecheck_zombie_acked violation INV_LIVE_ACKED_REACHABLE"
    "res_readwindow              violation  INV_OWNER_EXACT"
    "res_readwindow_gcdelete     violation  INV_NO_DELETE_SERVABLE"
    "res_clocklies               violation  INV_ACKED_REACHABLE"
    "safe_lease                  green      -"
    "safe_lateland               green      -"
    "safe_lateland_eager         green      -"
    "safe_lateland_selective     green      -"
    "safe_selective_legacy       green      -"
    "safe_selective_terminal     green      -"
    "safe_clocklies_nocleanup    green      -"
    "safe_clocklies_gc           green      -"
    "safe_clocklies_gc_4txn      green      -"
    "safe_clocklies_gc_lateland  green      -"
    "safe_clocklies_eager        green      -"
    "witness_seal                violation  W_SealHappens"
    "witness_skipepoch           violation  W_SkipEpochHappens"
    "witness_adopt               violation  W_AdoptHappens"
    "witness_lazyafterclean      violation  W_LazyAfterCleanHappens"
    "witness_sealonlyepoch       violation  W_SealOnlyEpochHappens"
    "witness_sealconflict        violation  W_SealConflictHappens"
    "witness_lateseal            violation  W_LateSealHappens"
    "witness_sealafterfarewell   violation  W_SealAfterFarewellHappens"
    "witness_terminal            violation  W_TerminalHappens"
    "witness_mixedchain          violation  W_MixedChainHappens"
    "witness_cleanupseal         violation  W_CleanupSealHappens"
    "witness_basemovedunderwalk  violation  W_BaseMovedUnderWalkHappens"
    "witness_cursordeleted       violation  W_CursorDeletedHappens"
    "witness_cursorreused        violation  W_CursorReusedHappens"
)

selected()
{
    local name="$1"
    shift
    [[ $# -eq 0 ]] && return 0
    local requested
    for requested in "$@"
    do
        requested="${requested##*/}"
        requested="${requested%.cfg}"
        requested="${requested#${MODULE}_}"
        [[ "$requested" == "$name" ]] && return 0
    done
    return 1
}

overall=0
run_id="${MODULE}-$$-$(date +%s%N)"
printf '%-28s %-10s %-34s %-8s %-12s %s\n' "CONFIG" "EXPECT" "RESULT" "SECONDS" "DISTINCT" "VERDICT"
for row in "${CONFIGS[@]}"
do
    read -r name expect want <<<"$row"
    selected "$name" "$@" || continue

    cfg="${MODULE}_${name}.cfg"
    log="../../../tmp/tlc-${run_id}-${name}.log"
    meta="../../../tmp/tlc-meta-${run_id}-${name}"
    start=$SECONDS
    timeout "${TLC_TIMEOUT:-3600}" /usr/bin/java -XX:+UseParallelGC ${TLC_JAVA_OPTS:-} -cp "$JAR" tlc2.TLC \
        -metadir "$meta" -workers "${TLC_WORKERS:-1}" -config "$cfg" "$MODULE.tla" >"$log" 2>&1
    rc=$?
    elapsed=$((SECONDS - start))

    result=error
    if [[ $rc -eq 0 ]] && grep -q "No error has been found" "$log"
    then
        result=green
    elif grep -qE '(Invariant|Property|Action property) [A-Za-z0-9_]+ is violated' "$log"
    then
        violated="$(grep -oE '(Invariant|Property|Action property) [A-Za-z0-9_]+ is violated' "$log" \
            | sed -n '1{s/.* \([A-Za-z0-9_]*\) is violated/\1/p;}')"
        result="violation:${violated}"
    elif [[ $rc -eq 124 ]]
    then
        result=timeout
    fi
    distinct="$(grep -oE '[0-9]+ distinct states found' "$log" | tail -1 | grep -oE '^[0-9]+')"

    verdict=FAIL
    case "$expect" in
        green) [[ "$result" == green ]] && verdict=PASS ;;
        violation) [[ "$result" == "violation:${want}" ]] && verdict=PASS ;;
    esac
    [[ "$verdict" == FAIL ]] && overall=1
    printf '%-28s %-10s %-34s %-8s %-12s %s\n' "$name" "$expect" "$result" "$elapsed" "${distinct:--}" "$verdict"
    echo "    log: $log"
done
exit $overall
