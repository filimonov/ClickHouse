#!/usr/bin/env bash
# Run the `CaEpochAllocCore` battery (CAS-362, CAS-363, member lifecycle) and assert every result by exact
# invariant name. With cfg names as arguments, run only those rows; `SLOW=1` adds the rows over ~2 minutes. TLC limits: timeout 1200 s, 4 workers, 8 GiB heap, nice 10.
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
    # CAS-363 re-creation above every earlier epoch, decommission tail of today. Suffix = invariant:
    # e EpochNeverReused, d NoDangle, s SlotNeverRegresses, r NoDangleAfterReuse, c CounterNeverRegresses.
    "cur_nz_e                             violation  EpochNeverReused"
    "cur_nz_d                             violation  NoDangle"
    "cur_nz_s                             violation  SlotNeverRegresses"
    "cur_nz_r                             violation  NoDangleAfterReuse"
    "cur_nz_c                             violation  CounterNeverRegresses"
    "fix_storetime_e                      violation  EpochNeverReused"
    "fix_storetime_d                      violation  NoDangle"
    "fix_storetime_s                      violation  SlotNeverRegresses"
    "fix_storetime_r                      violation  NoDangleAfterReuse"
    "fix_storetime_c                      violation  CounterNeverRegresses"
    "fix_storetime_fresh_e                violation  EpochNeverReused"
    "fix_storetime_fresh_d                violation  NoDangle"
    "fix_storetime_fresh_s                violation  SlotNeverRegresses"
    "fix_storetime_fresh_r                violation  NoDangleAfterReuse"
    "fix_storetime_fresh_c                violation  CounterNeverRegresses"
    "fix_storetime_fresh_nz_e             violation  EpochNeverReused"
    "fix_storetime_fresh_nz_d             violation  NoDangle"
    "fix_storetime_fresh_nz_s             violation  SlotNeverRegresses"
    "fix_storetime_fresh_nz_r             violation  NoDangleAfterReuse"
    "fix_storetime_fresh_nz_c             violation  CounterNeverRegresses"
    "fix_storetime_fresh_nz_slow_e        green      -"
    "fix_storetime_fresh_nz_slow_d        violation  NoDangle"
    "fix_storetime_fresh_nz_slow_s        violation  SlotNeverRegresses"
    "fix_storetime_fresh_nz_slow_r        green      -"
    "fix_storetime_fresh_nz_slow_c        green      -"
    "fix_poolhwm_e                        violation  EpochNeverReused"
    "fix_poolhwm_d                        violation  NoDangle"
    "fix_poolhwm_s                        violation  SlotNeverRegresses"
    "fix_poolhwm_r                        violation  NoDangleAfterReuse"
    "fix_poolhwm_c                        violation  CounterNeverRegresses"
    "fix_poolhwm_fresh_e                  violation  EpochNeverReused"
    "fix_poolhwm_fresh_d                  violation  NoDangle"
    "fix_poolhwm_fresh_s                  violation  SlotNeverRegresses"
    "fix_poolhwm_fresh_r                  violation  NoDangleAfterReuse"
    "fix_poolhwm_fresh_c                  violation  CounterNeverRegresses"
    "fix_poolhwm_fresh_nz_e               green      -"
    "fix_poolhwm_fresh_nz_d               violation  NoDangle"
    "fix_poolhwm_fresh_nz_s               violation  SlotNeverRegresses"
    "fix_poolhwm_fresh_nz_r               green      -"
    "fix_poolhwm_fresh_nz_c               green      -"
    "fix_storetime_fresh_nz_slow_mc_e     green      -"
    "fix_storetime_fresh_nz_slow_mc_d     green      -"
    "fix_storetime_fresh_nz_slow_mc_s     green      -"
    "fix_storetime_fresh_nz_slow_mc_r     green      -"
    "fix_storetime_fresh_nz_slow_mc_c     green      -"
    "fix_poolhwm_fresh_nz_mc_e            green      -"
    "fix_poolhwm_fresh_nz_mc_d            green      -"
    "fix_poolhwm_fresh_nz_mc_s            green      -"
    "fix_poolhwm_fresh_nz_mc_r            green      -"
    "fix_poolhwm_fresh_nz_mc_c            green      -"
    "fix_poolhwm_fresh_mc_e               violation  EpochNeverReused"
    "fix_poolhwm_fresh_mc_d               violation  NoDangle"
    "fix_poolhwm_fresh_mc_s               violation  SlotNeverRegresses"
    "fix_poolhwm_fresh_mc_r               violation  NoDangleAfterReuse"
    "fix_poolhwm_fresh_mc_c               violation  CounterNeverRegresses"
    "fix_storetime_nz_slow_e              violation  EpochNeverReused"
    "fix_storetime_nz_slow_d              violation  NoDangle"
    "fix_storetime_nz_slow_s              violation  SlotNeverRegresses"
    "fix_storetime_nz_slow_r              violation  NoDangleAfterReuse"
    "fix_storetime_nz_slow_c              violation  CounterNeverRegresses"
    "fix_poolhwm_nz_e                     violation  EpochNeverReused"
    "fix_poolhwm_nz_d                     violation  NoDangle"
    "fix_poolhwm_nz_s                     violation  SlotNeverRegresses"
    "fix_poolhwm_nz_r                     violation  NoDangleAfterReuse"
    "fix_poolhwm_nz_c                     violation  CounterNeverRegresses"
    "sab_storetime_back                   violation  CounterNeverRegresses"
    "sab_poolhwm_afterdelete              violation  CounterNeverRegresses"
    "sab_poolhwm_oldadmit                 violation  CounterNeverRegresses"
    # Owner floor (rejected alternative): floor in the owner tombstone, counter still deleted.
    "cur_revive                           violation  RevivalAboveEarlierLives"
    "fix_ownerfloor_ambig                 green      -"
    "sab_ownerfloor_nodelfail             violation  RetiredOwnerHasNoLiveMount"
    "sab_ownerfloor_tombafter             violation  EpochNeverReused"
    "sab_ownerfloor_stale                 violation  EpochNeverReused"
    "sab_ownerfloor_ignore                violation  RetiredOwnerHasNoLiveMount"
    "sab_ownerfloor_nomc                  violation  SlotNeverRegresses"
    "res_ownerfloor_zombie                violation  SlotNeverRegresses"
    "res_ownerfloor_zmint                 violation  SlotNeverRegresses"
    "res_ownerfloor_hostile               violation  EpochNeverReused"
    "res_ownerfloor_keepslot              violation  RetiredOwnerHasNoLiveMount"
    "res_ownerfloor_tooldeath             violation  RetiredOwnerHasNoLiveMount"
    # Member lifecycle: decommission keeps owner, epoch and the farewell mount; re-add reopens the srid.
    "keep_main                            violation  SlotNeverRegresses"
    "lc_core                              green      -"
    "lc_newuuid                           green      -"
    "lc_race                              green      -"
    "lc_twins                             green      -"
    "lc_nomc                              green      -"
    "lc_retry_live                        green      -"
    "lc_retry_witness                     violation  RetryNeverCompletes"
    "sab_lc_norecheck                     violation  LiveMatchesOwnerAtRest"
    "sab_lc_noconfirm                     violation  NoStaleDrainWithoutRerun"
    "sab_lc_refusedclean                  violation  NoStaleDrainWithoutRerun"
    "sab_lc_fenced                        violation  NoStaleDrainWithoutRerun"
    "sab_lc_nononce                       violation  NoStaleTombstone"
    "sab_lc_deletemount                   violation  SlotNeverRegresses"
    "sab_lc_tomblast                      violation  LiveMatchesOwnerAtRest"
    "sab_lc_delepoch                      violation  EpochNeverReused"
    "sab_lc_retryrefuses                  violation  RetryAcceptsTomb"
    "res_lc_pauseddrain                   violation  NoStaleDrain"
    "res_lc_tombover                      violation  LiveMatchesOwner"
    # Round 5 (lifecycle spec r3): own tombstone per run, verify, re-add as one owner write, fenced drains.
    "r5_core                              violation  DoneMeansRetired"
    "r5_newuuid                           violation  DoneMeansRetired"
    "r5_newuuid_witness                   violation  SecondUuidNeverLive"
    "sab_r5_noforeign                     green      -"
    "r5_twins                             green      -"
    "r5_inflight                          violation  DoneMeansRetired"
    "r5_pauseddrain                       green      -"
    "r5_retry_live                        green      -"
    "r5_retry_witness                     violation  RetryNeverCompletes"
    "sab_r5_keeptomb                      violation  DoneMeansRetired"
    "sab_r5_drainopen                     violation  NoStaleDrain"
    "sab_r5_foreignfenced                 violation  DoneMeansRetired"
    "sab_r5_skipclean                     violation  DoneMeansRetired"
    "res_r5_inflight                      violation  NoStaleDrain"
    "res_r5_inflight_norerun              violation  NoStaleDrainWithoutRerun"
    "res_r5_pauseddrain_inflight          violation  NoStaleDrain"
    # Round 5 rows above that check DoneMeansRetired fail it since round 6 keyed it on verified completions.
    "r5_race                              violation  DoneMeansRetired"
    # Round 6 (lifecycle spec r4/r5): rejoin over a retired owner, foreign terminal reclaim, rollback rules.
    "r6_core_z                            green      -"
    "r6_counterrule                       green      -"
    "r6_newuuid_witness                   violation  SecondUuidNeverLive"
    "r6_selffence                         green      -"
    "r6_selffence_witness                 violation  NoLiveAfterSelfFence"
    "r6_twins                             green      -"
    "r6_retry_live                        green      -"
    "sab_r6_keepforeignfenced             violation  NoWedge"
    "sab_r6_recheckretired                violation  LiveMatchesOwnerAtRest"
    "sab_r6_rollbackall                   violation  DoneMeansRetired"
    "sab_r6_rollbackfenced                violation  LiveMatchesOwnerAtRest"
    "sab_r6_nouuidcheck                   violation  NoForeignTombstone"
    "sab_r6_rejoinany                     violation  LiveMatchesOwnerAtRest"
    "res_r6_nogcdead                      violation  NoWedge"
    # Round 8 (lifecycle spec r8): one monotone owner_life; round-7 rows are superseded.
    "r8_core_z                            green      -"
    "r8_threeruns                         green      -"
    "sab_r8_dropmark                      violation  DoneMeansRetired"
    "sab_r8_foreignnoowner                violation  OldNotUnderNewOwner"
    "sab_r8_rechecknolife                 violation  NoResumeAfterRetire"
    "sab_r8_remountstops                  violation  LiveMatchesOwnerAtRest"
    "sab_r8_rollbacknolife                violation  DoneMeansRetired"
    "sab_r8_ownerfirst                    violation  OldNotUnderNewOwner"
    # Round 9 (lifecycle spec r9): the tool reads its victim; verified-only rejoin; a verified run is final.
    "r9_core_z                            green      -"
    "r9_core_g                            green      -"
    "r9_newuuid                           green      -"
    "r9_old                               green      -"
    "r9_threeruns                         green      -"
    "r9_newdecom                          green      -"
    "sab_r9_rejoinunverified              violation  NoDrainOfLaterLife"
    "res_r9_rerunafterverified            violation  NoDrainOfLaterLife"
    # Round 10 (lifecycle spec r10, step 6): split mark, rerun gated by the observed owner.
    "r10_core                             green      -"
    "r10_newuuid                          green      -"
    "res_r10_blindrerun                   violation  NoDrainOfLaterLife"
    # Round 12 (PR #2567 fix round): the tombstone on the admin's fenced plane; a rollback keeps any retirement.
    "r12_core_z                           green      -"
    "r12_tombambig                        green      -"
    "sab_r12_tombopen                     violation  NoTombOverLive"
    # Round 13: the operator reruns only on a `retiring` row.
    "r13_core                             green      -"
    "r13_newuuid                          green      -"
    "r13_core_z                           green      -"
    "sab_r13_rerunretired                 violation  NoDrainOfLaterLife"
    "res_r13_rerunreach                   violation  NoRerunWitness"
    "res_r13_rerundone                    violation  NoRerunDoneWitness"
    "res_r13z_rerundone                   violation  NoRerunDoneWitness"
    "sab_r13_rerunanyslot                 green      -"
    # Round 14: an unresolved tombstone lands until GC fences the admin's slot; the admin withholds its farewell.
    "r14_tombambig                        green      -"
    "r14_core_z                           green      -"
    "r14_old                              green      -"
    "sab_r14_farewellambig                violation  NoTombOverLive"
    "res_r14_farewellambig_r12bound       green      -"
    "res_r14_lateland                     violation  NoLateTombWitness"
)
# Two to twelve minutes each.
SLOW_CONFIGS=(
    "fix_nonce_restart   green      -"
    "fix_poolhwm_twins                    green      -"
    "fix_ownerfloor                       green      -"
    "fix_ownerfloor_nomove                green      -"
    "fix_ownerfloor_retry                 green      -"
    "lc_fence                             green      -"
    "lc_zombie                            green      -"
    "sab_lc_ownerfirst                    green      -"
    "r6_core_g                            green      -"
    "r6_newuuid_z                         green      -"
    "r6_newuuid_g                         green      -"
    "sab_r6_noverify                      violation  LiveMatchesOwnerAtRest"
    "r8_core_g                            green      -"
    "r8_newuuid                           green      -"
    "r8_old                               green      -"
    "r8_remount                           green      -"
    "r9_remount                           green      -"
    "res_r9_uuidreuse                     violation  OldNotUnderNewOwner"
    "r12_noverify                         green      -"
    "sab_r12_keepretired_open             violation  LiveMatchesOwnerAtRest"
)
[[ "${SLOW:-0}" == 1 ]] && CONFIGS+=("${SLOW_CONFIGS[@]}")
# Ad-hoc rows for exploration: EXTRA="name expect invariant;name expect invariant".
if [[ -n "${EXTRA:-}" ]]
then
    IFS=';' read -r -a extra_rows <<<"$EXTRA"
    CONFIGS+=("${extra_rows[@]}")
fi

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
