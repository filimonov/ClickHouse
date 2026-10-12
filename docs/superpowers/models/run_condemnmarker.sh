#!/usr/bin/env bash
# Run the complete `CaGcCondemnMarkerGate` battery and assert every result by exact invariant name.
# With one or more cfg names as arguments, run only those rows while retaining the same assertions.
set -uo pipefail
cd "$(dirname "$0")"

JAR=${TLC_JAR:-../../../tmp/tla2tools.jar}
MODULE=CaGcCondemnMarkerGate
source ./tlc_temporal_gate.sh
check_tlc_pin "$JAR" || exit 3

# Sabotage first: the positive gate is evidence only after the missing-marker bug is seen red.
# name  expectation(green|violation)  expected-invariant
CONFIGS=(
    "bug                          violation  NoDangle"
    "bug_both                     violation  NoDangle"
    "bug_both_2w                  violation  NoDangle"
    "rv_bug                       violation  NoDangle"
    "f1_bug                       violation  NoDangle"
    "pm_g1_late_resolved_entry    violation  NoDangle"
    "pm_g1_late_resolved_stale    violation  NoDangle"
    "pm_g1_late_gaveup            violation  NoDangle"
    "pm_g1_nogateround_noregistry violation  NoDangle"
    "pm_g1_nojob                  violation  JobSparesPendingGuard"
    "pm_g1_norestamp_live         violation  NoWedgedCarry"
    "pm_g1_extdrop_noregistry     violation  NoDangle"
    "pm_g1_mpf_extdrop            violation  NoDangle"
    "fix                          green      -"
    "fix_pconf                    green      -"
    "fix_prefetch                 green      -"
    "fix_both                     green      -"
    "fix_both_2w                  green      -"
    "pm_g1_noregistry             green      -"
    "pm_g1_all_noregistry         green      -"
    "rv_g1_noregistry             green      -"
    "pm_g1_f1                     green      -"
    "pm_g1_live                   green      -"
    "pm_g1_late_resolved          green      -"
    "pm_g1_late_resolved_version  green      -"
    "pm_g1_mpf_noregistry         green      -"
    "rv_g1_mpf_noregistry         green      -"
    "pm_g1_mpf_late_resolved      green      -"
    "cur_headget                  violation  NoDangle"
    "sab_deferA                   violation  NoDangle"
    "sab_reheadB                  violation  NoDangle"
    "sab_republishC               violation  NoDangle"
    "fix_deferA                   green      -"
    "fix_deferA_keep              green      -"
    "fix_reheadB                  green      -"
    "fix_republishC               green      -"
    "cur_orphan                   violation  OrphanTracked"
    "fix_deferA_orphan            violation  OrphanTracked"
    "cur_orphan_retry             violation  OrphanTracked"
    "fix_deferA_keep_orphan       violation  OrphanTracked"
    "fix_deferA_keepany           green      -"
    "fix_deferA_keepany_orphan    green      -"
    "cur_headget_all              violation  NoDangle"
    "fix_deferA_all               green      -"
    "fix_deferA_keepany_all       green      -"
    "fix_reheadB_all              green      -"
    "fix_republishC_all           green      -"
    "cur_deposed                  violation  NoDangle"
    "sab_deferA_gonenow           violation  NoDangle"
    "fix_deferA_deposed           green      -"
    "fix_deferA_keepany_deposed   violation  OrphanTracked"
    "fix_reheadB_deposed          green      -"
    "fix_republishC_deposed       green      -"
    "fix_deferA_keepany_deposed_safe green   -"
    "sab_C_orphan_retry           violation  OrphanTracked"
    "fix_C_orphan_retry           green      -"
    "fix_C_bound_orphan           green      -"
    "fix_C_bound                  green      -"
    "fix_C_bound_coarse           violation  NoDangle"
    "fix_C_nobound_coarse         violation  NoDangle"
    "fix_B_bound_deposed          green      -"
    "fix_B_recur                  violation  NoDangle"
    "fix_C_recur                  green      -"
    "fix_C_recur_any              violation  NoDangle"
    "mixed_old                    violation  NoDangle"
    "mixed_new                    violation  NoDangleNewWriter"
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
OUT=${TLC_OUT:-../../../tmp/noclock/fix/cas360}
mkdir -p "$OUT"
printf '%-30s %-11s %-32s %12s %-8s %s\n' "CONFIG" "EXPECT" "RESULT" "DISTINCT" "SECONDS" "VERDICT"
for row in "${CONFIGS[@]}"
do
    read -r name expect want <<<"$row"
    selected "$name" "$@" || continue

    cfg="${MODULE}_${name}.cfg"
    log="$OUT/tlc-${run_id}-${name}.log"
    meta="$OUT/tlc-meta-${run_id}-${name}"
    start=$SECONDS
    timeout "${TLC_TIMEOUT:-1200}" nice -n 10 /usr/bin/java -XX:+UseParallelGC -Xmx8g ${TLC_JAVA_OPTS:-} -cp "$JAR" tlc2.TLC -noGenerateSpecTE \
        -metadir "$meta" -workers "${TLC_WORKERS:-4}" -deadlock -config "$cfg" "$MODULE.tla" >"$log" 2>&1
    rc=$?
    rm -rf "$meta"
    elapsed=$((SECONDS - start))
    distinct="$(grep -oE '[0-9,]+ distinct states found' "$log" | tail -1 | awk '{gsub(",", "", $1); print $1}')"

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

    verdict=FAIL
    case "$expect" in
        green) [[ "$result" == green ]] && verdict=PASS ;;
        violation) [[ "$result" == "violation:${want}" ]] && verdict=PASS ;;
    esac
    [[ "$verdict" == FAIL ]] && overall=1
    printf '%-30s %-11s %-32s %12s %-8s %s\n' "$name" "$expect" "$result" "${distinct:-?}" "$elapsed" "$verdict"
    printf '  log: %s\n' "$log"
done

if [[ $# -gt 0 ]]
then
    for requested in "$@"
    do
        requested="${requested##*/}"
        requested="${requested%.cfg}"
        requested="${requested#${MODULE}_}"
        if ! printf '%s\n' "${CONFIGS[@]}" | awk '{print $1}' | grep -qx -- "$requested"
        then
            echo "unknown ${MODULE} config: $requested" >&2
            overall=1
        fi
    done
fi

echo
if [[ $overall -eq 0 ]]
then
    echo "ALL EXPECTATIONS MET"
else
    echo "SOME EXPECTATIONS UNMET"
fi
exit "$overall"
