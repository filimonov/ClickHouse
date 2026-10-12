#!/usr/bin/env bash
# Run the `CaEpochAllocCore` battery (CAS-362, CAS-363) and assert every result by exact invariant name.
# With cfg names as arguments, run only those rows; `SLOW=1` adds the ten-minute rows. TLC limits: timeout 1200 s, 4 workers, 8 GiB heap, nice 10.
set -uo pipefail
cd "$(dirname "$0")"

JAR=${TLC_JAR:-../../../tmp/tla2tools.jar}
MODULE=CaEpochAllocCore
source ./tlc_temporal_gate.sh
check_tlc_pin "$JAR" || exit 3

# Sabotages first.
# name               expectation  expected-invariant
CONFIGS=(
    "sab_nonce           violation  EpochNeverReused"
    "sab_retire          violation  RetiredStaysRetired"
    "sab_retire_epoch    green      -"
    "sab_retire_delmount violation  SlotNeverRegresses"
    "cur_single          green      -"
    "cur_twin            violation  EpochNeverReused"
    "cur_twin_gcs        violation  EpochNeverReused"
    "cur_twin_dangle     green      -"
    "base_twin_dangle    violation  NoDangle"
    "cur_decom           violation  EpochNeverReused"
    "cur_decom_gcs       violation  EpochNeverReused"
    "cur_decom_dangle    violation  NoDangle"
    "cur_decom_reusedangle violation NoDangleAfterReuse"
    "cur_decom_slot      violation  SlotNeverRegresses"
    "cur_decom_retired   violation  RetiredStaysRetired"
    "fix_nonce           green      -"
    "fix_nonce_gcs       green      -"
    "fix_retire          green      -"
    "fix_retire_gcs      green      -"
    "fix_retire_twin     violation  EpochNeverReused"
    "fix_both            green      -"
)
# About ten minutes each.
SLOW_CONFIGS=(
    "fix_nonce_restart   green      -"
)
[[ "${SLOW:-0}" == 1 ]] && CONFIGS+=("${SLOW_CONFIGS[@]}")

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

out_dir="${TLC_OUT_DIR:-../../../tmp/noclock/fix/cas362}"
mkdir -p "$out_dir"
overall=0
run_id="${MODULE}-$$-$(date +%s%N)"
printf '%-18s %-11s %-32s %-10s %-8s %s\n' "CONFIG" "EXPECT" "RESULT" "STATES" "SECONDS" "VERDICT"
for row in "${CONFIGS[@]}"
do
    read -r name expect want <<<"$row"
    selected "$name" "$@" || continue

    cfg="${MODULE}_${name}.cfg"
    log="${out_dir}/tlc-${run_id}-${name}.log"
    meta="${out_dir}/tlc-meta-${run_id}-${name}"
    start=$SECONDS
    timeout "${TLC_TIMEOUT:-1200}" nice -n 10 /usr/bin/java -Xmx8g -XX:+UseParallelGC ${TLC_JAVA_OPTS:-} \
        -cp "$JAR" tlc2.TLC -metadir "$meta" -workers "${TLC_WORKERS:-4}" -noGenerateSpecTE \
        -config "$cfg" "$MODULE.tla" >"$log" 2>&1
    rc=$?
    rm -rf -- "$meta"
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
    states="$(grep -oE '[0-9,]+ distinct states found' "$log" | tail -1 | cut -d' ' -f1)"

    verdict=FAIL
    case "$expect" in
        green) [[ "$result" == green ]] && verdict=PASS ;;
        violation) [[ "$result" == "violation:${want}" ]] && verdict=PASS ;;
    esac
    [[ "$verdict" == FAIL ]] && overall=1
    printf '%-18s %-11s %-32s %-10s %-8s %s\n' "$name" "$expect" "$result" "${states:--}" "$elapsed" "$verdict"
    printf '  log: %s\n' "$log"
done

echo
if [[ $overall -eq 0 ]]
then
    echo "ALL EXPECTATIONS MET"
else
    echo "SOME EXPECTATIONS UNMET"
fi
exit "$overall"
