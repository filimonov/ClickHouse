# CAS lease PR 14: reacquire in the lease thread — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:executing-plans (or
> superpowers:subagent-driven-development). Steps use checkbox (`- [ ]`) syntax.

**Goal:** when the GC of another server fences our own mount body (R8), the renewal thread itself closes the
old epoch and takes the slot again under a new epoch, by a certificate: the ETag of the fenced body it read.
No remount request, no second thread, no observation window. Every step write is fixed once, so a retry
repeats the same bytes under the same precondition. Whatever the reacquire finds that is not its own fenced
body, its own claim or a transient failure is a loss row and stops the server.
**Architecture:** the renewer returns the fenced `Object` with R8; the worker hands it to `Pool::reacquireOnce`,
a resumable step machine over one `ReacquireStep` (stage, certificate, fixed counter write, allocated epoch,
fixed claim). New primitives: `writeEpochOnce` (the counter write as a reusable tuple), the `Reacquire` mode of
`claimMount` (`MountReacquireClaim`), the classifier `classifyReacquireSlot`. Writes open under `driver_mutex`
only after re-checking the end latch, stop, FORGET intent and the deadline (R23).
**Tech stack:** C++23, gtest (`unit_tests_dbms`), `InMemoryBackend` seams, praktika integration with the fault
proxy, TLA+ (`CaCasMountCore`, `CaDiskLifecycle`).

**Spec:** `/home/mfilimonov/workspace/ClickHouse/master/docs/superpowers/specs/2026-10-07-cas-mount-lease-series-design.md`
— §7 row 14 (both tables), §2 "Потеря" (R8, R16), "Получение заново после фенса GC" (R19–R24), "Старт",
"Монотонность claim", §9 rows `CaCasMountCore` шаг 3 and `CaDiskLifecycle` шаг 2. Decisions since r7:
`/home/mfilimonov/workspace/ClickHouse/master/tmp/lease-series/spec-r8-todo.md` (stop split, LIST corroboration).
Acceptance matrix: `docs/superpowers/specs/2026-10-05-cas-mount-lease-simplification.md` `{#gc-recovery-experiment}`.

**Base:** PR 12's head on `fix/antalya-26.6/cas-terminal-losses-stop` at execution time (`02ed2c6825a` at plan
time; it contains PRs 1, 4, 7, 8, 9a, 10 v2, 11a, 11b, 12). **Branch:** `fix/antalya-26.6/cas-reacquire-in-lease-thread`.
`$WT` = the executor's own worktree (never `cas-janitor-budget`, which is in use; read it only with
`git show <head>:<path>`). `$CA` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed`. `$M` =
`/home/mfilimonov/workspace/ClickHouse/master/docs/superpowers/models`. Every commit ends with:

```
Signed-off-by: Mikhail Filimonov <mfilimonov@altinity.com>
Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
```

**Files outside `$CA`:** `src/Storages/System/StorageSystemContentAddressedMounts.cpp` — one column description
(`remounting` → `fenced_by_gc`), so `system.cas_mounts` describes the value it now shows. Tests, the
integration modules, `docs/en/antalya/cas/`, the models in the master worktree.

## Global Constraints

- One topic per PR; the PR is self-contained and leaves a working product; no behaviour outside the
  spec row changes. Changelog: one sentence, category per `.github/PULL_REQUEST_TEMPLATE.md`.
- Allman braces; no `sleep` to fix races; tests use injected clocks and deterministic barriers.
- A test that expects `LOGICAL_ERROR` is a death test under `DEBUG_OR_SANITIZER_BUILD` (the error
  aborts there); prefer injecting `NOT_IMPLEMENTED`/`BAD_ARGUMENTS` where the rule allows.
- Shared code outside `CA/` (`src/IO/S3`, `base/poco`, `src/Daemon`, `src/Storages/System`) is a fork
  patch: minimal, motivated, portable; say in the plan which files outside `CA/` are touched and why.
- Comments explain why, not what; no references to plans, reviews, backlog ids or history in code.
- Prose style for PR text and comments: point first, plain words, backticks for names, `f` not `f()`.
- Build: `ninja -C build <target>` without `-j`, output to a log file in the build dir; the gate is
  `build/src/unit_tests_dbms --gtest_filter='CAS*'` (release) and the same in `build_asan`.
- Commit exact files only (`git add -- <path> …`), never a directory, never `-A`; no rebase/amend.
- Every build, test and gate command runs with `TMPDIR=<build_dir>/tmp`.
- New source files are globbed: run `cmake .` in `$WT/build` and `$WT/build_asan` after creating one.
- TLC rows: `timeout 1200`, `TLC_WORKERS=4`, `TLC_JAVA_OPTS=-Xmx8g`, `nice -n 10`, and the disk guard of Task 1;
  a row stopped by the timeout or the guard is reported as stopped, never as green.

## Review Focus

- **Failure asymmetry.** A false stop costs a restart; a false continue puts two writers on one slot. So the
  reacquire replaces exactly one body — the fenced one whose ETag it read — and everything else it reads is a
  loss row, except two things that are provably ours: our own claim bytes (unique `write_attempt_id`) and our
  counter bytes `{N + 1}` after an unclear attempt of ours (a collision there is settled by the slot write).
- **One tuple.** No step re-reads what it already fixed: the counter write keeps its expected ETag and bytes,
  the claim keeps its body, bytes and `start`. A refused or unobserved step repeats; it never re-decides.
- **Order.** `closeOldEpoch` finishes under the old `liveWriterEpoch` before the counter moves (the spec order;
  `tryRemountOnce` today cancels recoveries after publishing the new epoch). Open re-checks the latch, stop,
  FORGET intent and the deadline under `driver_mutex` in the step that arms the fence.

## Code vs spec (flagged; the plan follows the spec and the owner decisions)

1. Spec row «remount-нить не запускается». About fifteen gtests drive the remount worker through
   `scheduleRemountForTest`, and an unlatched terminal renewal that is not R8 still requests it. Plan: R8 never
   reaches the worker (no generation bump, `CASRemount*` untouched); the idle worker and its residual path stay
   until PR 15 deletes the machine with those tests.
2. Spec §5 «`TransientNotLive` … заменён на `paused`» (PR 14). Plan keeps the enum value (PR 15 removes the
   remount states) and shows it as `paused(fenced_by_gc)` instead of `paused(remounting)`.
3. Spec R22 «чтение каталога … `Corrupt` — `ledger_corrupt`». Code: `OwnershipCorrupt` also carries `Absent`
   (one `GET` 404). Plan: a catalog absence that a `LIST` confirms → `pool_identity_lost`; undecodable or
   ambiguous → `ledger_fault` (fatal class, per the stop split). An undecodable owner or counter object
   (spec silent) → `pool_identity_lost`.
4. Spec R16 lists `Replaced` and `IdentityLost` with `pool_identity_lost`; the code's `MountEnd::Replaced`
   closes the disk and keeps the server. Plan: in the reacquire both latch the new `MountEnd::PoolIdentityLost`
   (graceful stop) after a `LIST` agrees with each absence; `tryRemountOnce` keeps `Vanished(replaced)`.
5. Spec R17 excludes `MEMORY_LIMIT_EXCEEDED` via PR 3, not in this base. Plan: that code on the reacquire keeps
   PR 12's renewal-thread handling (fence tripped, thread ends, disk closed until restart).
6. Spec test row «`Reacquire` отвергает … `unsafe`, чужое тело». The base remount never passes the unsafe
   authorization, and PR 12 already stops on a foreign body: both tests are golden, not regressions.
7. Spec §9 `CaCasMountCore` шаг 3 sabotage «чистое тело другого epoch»: the model has no clean marker. Plan adds
   `TwinFarewell` and `cleanTok` behind `FarewellOn`; PR 13's farewell model may replace them.
8. Spec R21 «повторяется по R4 тем же кортежем». Plan: step writes run under `Retry::untilDefinitive` on the
   lease plane (the engine repeats the tuple with R4 spacing and resolves unclear attempts by reading); only
   `Refused`, `Conflict{NotObserved}` and read-class failures return to the step loop, which waits one
   `drawSpacing(kDefinitiveAttemptSpacingMs)` and repeats the stage.

## Setup

- [ ] `git -C $WT switch -c fix/antalya-26.6/cas-reacquire-in-lease-thread <PR 12 head>`; `cmake .` in both build
  dirs; `ninja -C $WT/build unit_tests_dbms > $WT/build/build_pr14_base.log 2>&1`; run the `CAS*` gate in release.
  All green is the baseline; record the count in the Task 12 notes.

## Task 1: `CaCasMountCore` step 3 — certificate and partial reacquire (master worktree)

**Files:** `$M/CaCasMountCore.tla`, every `$M/CaCasMountCore_*.cfg` (new constants FALSE/0), `$M/CaCasMountCoreStraggler.tla`,
new `$M/CaCasMountCore_reacq_cert.cfg`, `_sab_reacqanyfenced.cfg`, `_sab_reacqclean.cfg`, `_sab_reacqunknownpair.cfg`,
`_sab_reacqforgetsstep.cfg`, `_sab_reacqearly.cfg`, `_reacq_nostraggler.cfg`, `_witness_reacqopens.cfg`,
`_witness_reacqcontinues.cfg`, `$M/run_mount.sh`, `$M/CaCasMountCore_RESULTS.md`.
**Precondition:** `git -C /home/mfilimonov/workspace/ClickHouse/master status --short -- docs/superpowers/models/CaCasMountCore*`
shows nothing; otherwise stop and report.

- [ ] Constants: `CertReacquire` (gate), `FarewellOn`, `SabReacqAnyFenced`, `SabReacqClean`, `SabReacqUnknownPair`,
  `SabReacqForgetsStep`, `SabReacqEarly`. Variables (`reacqVars`, added to `vars`, `TypeOK`, `Init` and every
  `UNCHANGED` frame): `reacqCert \in [Incs -> {NoPend} \cup 0..MaxToken]` (the token our renewal read under the
  fence), `reacqOld \in [Incs -> 0..MaxEpoch]`, `cleanTok \in {NoPend} \cup 0..MaxToken`, history `reacqTookOther`,
  `reacqLostOwn`, `reacqResolved`.
- [ ] `Renew(a)` under `CertReacquire`: the fenced branch requires `mount.epoch = localEpoch[a] /\ (OwnBody(a) \/ SabReacqUnknownPair)`
  and also sets `reacqCert'[a] = mtoken`, `reacqOld'[a] = localEpoch[a]`; every other loss branch also sets
  `stopLatch'[a]`. `ReacqMode(a)` gains `/\ (~CertReacquire \/ reacqCert[a] # NoPend)`.
- [ ] `AllocBegin(a)` under `CertReacquire`: `~stopLatch[a]`, and in `ReacqMode` `adoptObs[a] = None \/ SabReacqEarly`
  (the renewal series resolved its tuple before the reacquire starts). `AdoptRead(a)` under `CertReacquire`:
  `~fenceLatched[a] \/ SabReacqEarly` (one thread: no renewal read while the reacquire runs).
- [ ] `ClaimAllowed(a)` when `CertReacquire /\ reacqCert[a] # NoPend`: `canClaim == monotoneOK /\ mount # None /\
  (mtoken = reacqCert[a] \/ (SabReacqAnyFenced /\ mount.uuid = Uuid(a) /\ mount.fenced) \/ (SabReacqClean /\
  mount.uuid = Uuid(a) /\ cleanTok = mtoken))`; no other ground applies. `ClaimApplyBody(a)` sets
  `reacqTookOther' = reacqTookOther \/ (reacqCert[a] # NoPend /\ ~(mount # None /\ mount.fenced /\ mount.uuid = Uuid(a)
  /\ mount.epoch = reacqOld[a] /\ OwnBody(a)))` — `bodyWriter` is a ghost, so this is authorship, not the token.
  `ClaimConfirmBody(a)` resets `reacqCert'[a] = NoPend`.
- [ ] A reacquire claim that is refused (`ClaimAttempt`'s `~ClaimPrecondition` branch) or whose resolve finds the
  slot moved (`ClaimResolve`'s third branch) stops: `stopLatch'[a] = TRUE`, `reacqLostOwn' = reacqLostOwn \/
  (claimTok[a] # NoPend /\ mtoken = claimTok[a])`. Under `SabReacqForgetsStep` `ClaimResolve`'s `ours` is FALSE for a
  reacquire claim. `ClaimResolve`'s `ours` branch sets `reacqResolved' = TRUE` when `reacqCert[a] # NoPend`.
- [ ] `TwinFarewell(a)`: `FarewellOn /\ Twins(a) # {} /\ OwnLive(a) /\ ~localLost[a] /\ mtoken < MaxToken` →
  `mtoken' = mtoken + 1`, `cleanTok' = mtoken + 1`, `localLost'[a]`, `stopLatch'[a]`; in `Next`.
- [ ] Invariants: `ReacquireTakesOnlyItsOwnFence == ~reacqTookOther`, `ReacquireKeepsItsOwnClaim == ~reacqLostOwn`.
  Witnesses: `W_ReacquireOpens == ~\E a \in Incs : remountedAfterFence[a] /\ claimState[a] = "idle" /\ CanWrite(a)`,
  `W_ReacquireContinues == ~reacqResolved`. In `CaCasMountCoreStraggler.tla`: `reacqStraggler` set by
  `LandedWhileLatched` restricted to `reacqCert[a] # NoPend`; invariant `NoStragglerInReacquire == ~reacqStraggler`.
- [ ] Cfgs from `twins_immclaim` with `TTL = 1`, `Reserve = 0`, `MaxClock = 2`, `MaxEpoch = 3`, `MaxToken = 6`,
  `CertReacquire = FarewellOn = TRUE`, invariants of `twins_immclaim` (minus `OpenLeavesReserve`) plus `FenceCostsEpoch`
  and the two new ones. Rows: `reacq_cert` green; `sab_reacqanyfenced`, `sab_reacqclean`, `sab_reacqunknownpair` →
  `ReacquireTakesOnlyItsOwnFence`; `sab_reacqforgetsstep` → `ReacquireKeepsItsOwnClaim`; `reacq_nostraggler`
  (wrapper) green on `NoStragglerInReacquire`; `sab_reacqearly` (wrapper) → `NoStragglerInReacquire`;
  `witness_reacqopens`, `witness_reacqcontinues` → their witness.
- [ ] Disk guard, run beside each row: `while kill -0 $TLC 2>/dev/null; do s=$(du -sk $META | cut -f1); [ "$s" -gt 5242880 ]
  && kill $TLC; sleep 10; done` (`$META` from `run_mount.sh`'s `meta=`). Run `ROWS="<new rows> twins_immclaim twins
  witness_laterenew witness_straggler_landed sab_reacquireredecides" TLC_TIMEOUT=1200 nice -n 10 $M/run_mount.sh`.
  Expected: `ALL EXPECTATIONS MET`; the controls keep their 2026-10-09 counts. If `reacq_cert` does not finish in
  1200 s, rerun it with `MaxToken = 5` and record both runs.
- [ ] Append `## 2026-10-09 step 3: certificate and partial reacquire {#2026-10-09-step-3}` to RESULTS (rows, counts,
  each counterexample trace, limits: `ReacquireTakesOnlyItsOwnFence` is a ghost-authorship property, its evidence
  is the three sabotages). Commit the exact files: `model(cas): reacquire by certificate, resumable claim`.

## Task 2: `CaDiskLifecycle` step 2 — `Reacquiring` in the lease thread (master worktree)

**Files:** `$M/CaDiskLifecycle.tla`, every `$M/CaDiskLifecycle_*.cfg` (`ReacquireInThread = FALSE`, sabotages FALSE),
new `_main_step2.cfg`, `_sab_reacqallocperattempt.cfg`, `_sab_oldopunchecked.cfg`, `_sab_lossreacquires.cfg`,
`_sab_armafterintent_step2.cfg`, `_sab_liveunready_step2.cfg`, `_sab_interferearmrace_step2.cfg`,
`_sab_forgetlosesarm_step2.cfg`, `_witness_reacquire.cfg`, `$M/run_disklifecycle.sh`, `$M/CaDiskLifecycle_RESULTS.md`.

- [ ] Constants `ReacquireInThread`, `SabReacqAllocPerAttempt`, `SabOldOpUnchecked`, `SabLossReacquires`. `PStates`
  gains `"Reacquiring"`; `lpc` gains `"Closing"`, `"Claiming"`. Variables: `allocated`, `allocCount \in 0..2`,
  `fenceEpoch \in -1..MaxGen`, `opEpoch \in -1..MaxGen`, history `staleOpLanded`, `reacqLive`.
- [ ] `LRenewFenced` under `ReacquireInThread`: no `reqGen`; `lpc' = "Closing"`, `lost' = TRUE`, `keeper' = "Terminal"`,
  `pstate' = IF pstate = "Live" THEN "Reacquiring" ELSE pstate`, `fenceEpoch' = epoch`, `allocated' = FALSE`,
  `allocCount' = 0`. `LClose`: `"Closing"` → `"Claiming"` (quiescence; no stop check, R24). `LAlloc`: `"Claiming"`,
  `~stop /\ ~LeaseTerminal`, `~allocated \/ SabReacqAllocPerAttempt`, `allocCount < 2` → `allocated'`, `allocCount' + 1`.
  `LStepFail`: `"Claiming"` → `allocated' = allocated /\ ~SabReacqAllocPerAttempt` (honest: a stutter).
  `LClaimLoss`: `"Claiming"` → `EnterStopping`, `lpc' = "Top"`; under `SabLossReacquires` → `lpc' = "Closing"` and
  `lossSeen'`. `LClaimed`: `"Claiming" /\ allocated /\ ~stop` → `lpc' = "Arming"`, `keeper' = "Active"`,
  `epoch' = epoch + 1`, `\E ready : deadlineOk' = ready`, `armReady' = CanArm(FALSE, FALSE)`. `LReacqAbandon`:
  `"Claiming" /\ (stop \/ LeaseTerminal)` → `"Top"`. `LArmOpen` and `LRenewOk` treat `"Reacquiring"` as they treat
  `"Transient"`; `LArmOpen` sets `reacqLive'` when it opens `"Reacquiring"` with `epoch = fenceEpoch + 1`.
  `OpAdmit`: `pstate = "Live" /\ ~lost /\ opEpoch = -1` → `opEpoch' = epoch`. `OpDurable`: `opEpoch # -1` →
  `opEpoch' = -1`, `staleOpLanded' = staleOpLanded \/ (landed /\ opEpoch # epoch)` with `landed == IF SabOldOpUnchecked
  THEN ~lost ELSE ~lost /\ opEpoch = epoch`.
- [ ] Properties: `OneAllocationPerFence == allocCount <= 1`; `I4cReacquiringImpliesLost`; `OldEpochOpRefused ==
  ~staleOpLanded`; action properties `LiveLeavesOnlyByFenceOrEnd == [][(pstate = "Live" /\ pstate' # "Live") =>
  (pstate' \in TerminalSet \/ (pstate' = "Reacquiring" /\ lpc' = "Closing"))]_vars`, `OneEpochPerFence ==
  [][(pstate = "Reacquiring" /\ pstate' = "Live") => epoch' = fenceEpoch + 1]_vars`, `NoOpenOverEnd ==
  [][(pstate = "Reacquiring" /\ pstate' = "Live") => (~intent /\ ~stop /\ deadlineOk')]_vars`; liveness
  `ReacquireSettles == (pstate = "Reacquiring") ~> (pstate # "Reacquiring" \/ stop)` under `FairSpec2 == FairSpec
  /\ WF_vars(LClose) /\ WF_vars(LAlloc) /\ WF_vars(LReacqAbandon) /\ SF_vars(LClaimedReady) /\ SF_vars(LRenewOkReady)`
  (`*Ready` = the action with `ready = TRUE`: fair storage answers, one success that leaves lease for the open).
  Witness `WNoReacquire == ~reacqLive`.
- [ ] Rows: `main_step2` green (every property, `FairSpec2`); `sab_reacqallocperattempt` → `OneAllocationPerFence`;
  `sab_oldopunchecked` → `OldEpochOpRefused`; `sab_lossreacquires` → `I9LossTerminates`; the four `*_step2` rows are
  the existing sabotages with `ReacquireInThread = TRUE`, expected red on the same property (if one is green, keep
  it, write why the new route closes that race, and report it); `witness_reacquire` → `WNoReacquire`. Existing rows
  keep their verdicts and counts.
- [ ] Run `TLC_TIMEOUT=1200 TLC_WORKERS=4 TLC_JAVA_OPTS=-Xmx8g nice -n 10 $M/run_disklifecycle.sh` with the disk guard.
  Append `## 2026-10-09 step 2: Reacquiring in the lease thread {#2026-10-09-step-2}` to RESULTS. Commit:
  `model(cas): reacquire in the lease thread, one allocation per fence`.

## Task 3: the certificate, the reacquire classifier, `pool_identity_lost`

**Files:** `$CA/Pool/CasMountLoss.{h,cpp}`, create `$CA/Pool/CasMountReacquire.h`, `src/Disks/tests/gtest_cas_mount_loss.cpp`.
**Produces:** `MountSlotReading::fenced`; `MountEnd::PoolIdentityLost`; `ReacquireCertificate`, `MountReacquireClaim`,
`ReacquireSlotVerdict`, `ReacquireSlotReading`, `classifyReacquireSlot`.

- [ ] `CasMountLoss.h`: `MountSlotReading` gains `/// For FencedByGc: the fenced body as read; its ETag certifies the
  reacquire.` `std::optional<Object> fenced;` set by `classifyMountSlot` on its `FencedByGc` return. `MountEnd` gains
  `PoolIdentityLost` (word `pool_identity_lost`, a loss end, graceful). `describeMountEnd`: `PoolIdentityLost` →
  `{head}the pool identity is lost ({detail}).{suffix}`; `SlotTaken` with no holder and a non-empty detail →
  `{head}{detail}.{suffix}`.
- [ ] `CasMountReacquire.h` (header only):

```cpp
/// Our own body as the GC fenced it, as read: its ETag is the only ground on which a reacquire replaces it.
struct ReacquireCertificate
{
    Object fenced;
    MountLease body;
};

/// The reacquire's claim, fixed before its first request so every retry sends the same bytes under the
/// certificate's ETag; `start_boot_ms` is taken before the body is built, as at startup.
struct MountReacquireClaim
{
    MountLease body;
    String bytes;
    uint64_t start_boot_ms = 0;
};

/// `Applied`: our claim bytes are in the slot. `Certified`: the certified fenced body still is.
/// `NewFence`: the GC fenced our claim body. `Loss`: a loss row, in `loss`.
enum class ReacquireSlotVerdict : uint8_t { Applied, Certified, NewFence, Loss, NotObserved };
struct ReacquireSlotReading
{
    ReacquireSlotVerdict verdict = ReacquireSlotVerdict::NotObserved;
    MountSlotReading loss;
    std::optional<ReacquireCertificate> new_fence;
};
/// R20's order on one observation: our claim bytes, the certificate's ETag, then the loss table. A body of
/// the fenced pair under another ETag is corrupt even when it looks like a GC fence: a fence never repeats.
ReacquireSlotReading classifyReacquireSlot(const Observation & seen, const ReacquireCertificate & certificate,
                                           const std::optional<MountReacquireClaim> & claim);
```

- [ ] `classifyReacquireSlot` (in `CasMountLoss.cpp`): `Object` with `bytes == claim->bytes` → `Applied`; `etag ==
  certificate.fenced.etag` → `Certified`; undecodable → `Loss{SlotCorrupt}`; a GC fence of `claim->body`
  (`isGcFenceOf`) → `NewFence` with that object; the fenced pair (uuid and `writer_epoch` of `certificate.body`) →
  `classifyMountSlot(seen, OwnMountBodies{uuid, epoch, .confirmed = unfenced(certificate.body)})` with
  `FencedByGc` turned into `SlotCorrupt`, where `unfenced` clears `gc_fenced` and decrements `seq`; any other uuid or
  epoch → `Loss{SlotTaken, holder}`; `ProvenAbsent` → `Loss{SlotAbsent}`; `NotObserved`/`Meta` → `NotObserved`.
- [ ] Tests (`TEST(CASMountLoss, …)`, the PR 12 helpers `ownBody(seq)` and in-memory objects): `AFenceVerdictCarriesTheFencedObject`;
  `ReacquireSeesItsOwnClaimBytesAsApplied`; `ReacquireSeesTheCertifiedEtagAsCertified`;
  `AFencedBodyOfThePairUnderAnotherEtagIsCorrupt` (same bytes re-put → new ETag); `ACleanMarkerOfThePairIsCorrupt`
  (`min_active_build_sequence = UINT64_MAX`, unfenced); `ATwinsCleanBodyAtANewerEpochIsTaken`; `AForeignBodyIsTaken`;
  `AGcFenceOfOurClaimIsANewFence`; `ProvenAbsentIsAbsent`; `TheIdentityAndCounterLinesNameWhatWasFound` (full strings of
  both new `describeMountEnd` lines). Red on the base: the symbols do not exist. Commit:
  `Carry the CAS fence certificate and classify a reacquire observation`.

## Task 4: the counter write as one reusable tuple

**Files:** `$CA/Pool/CasServerRoot.{h,cpp}`, `src/Disks/tests/gtest_cas_ownership_primitives.cpp`.
**Produces:** `EpochWrite`, `beginEpochWrite`, `writeEpochOnce`.

- [ ] Declarations (next to `allocateEpoch`):

```cpp
/// A reacquire's counter write, fixed by `beginEpochWrite`: `{read_next + 1}` under the read ETag. A retry
/// sends exactly this; `unclear` remembers an attempt that may have landed unconfirmed.
struct EpochWrite { Etag expected; uint64_t read_next = 0; bool unclear = false; };
/// Reads the counter; an absent one is `EpochCounterLost` only when a LIST agrees, else `OwnershipTransient`.
std::variant<EpochWrite, EpochAllocation> beginEpochWrite(CasOperation & op, const Layout & l, const String & srid);
/// One engine call of `write`. Our bytes found after an unclear attempt of ours are ours; `{read_next}` under
/// another ETag refreshes `expected` and is transient; anything else is `EpochAdvancedByOther`; absence that a
/// LIST confirms is `EpochCounterLost`.
EpochAllocation writeEpochOnce(CasOperation & op, const Layout & l, const String & srid, EpochWrite & write);
```

- [ ] `writeEpochOnce`: `op.replace(epochKey, encodeServerEpoch({read_next + 1}), expected, Retry::untilDefinitive())`;
  `Committed` → `EpochAllocated{read_next}`; `Refused`/`GaveUp` → `write.unclear |= sent_any` (Refused: false), return
  `OwnershipTransient` with the engine's message (`ABORTED` text is not used); `Conflict` → `unclear |= any_ambiguous`,
  `NotObserved` → transient (the next call re-reads only by sending the same tuple); the observation as above, with
  `EpochAdvancedByOther{read_next, observed, ABORTED "the writer epoch counter of this server_root_id was advanced by
  another process (read next_writer_epoch {}, found {})"}`. `allocateEpoch` and its `Reacquire` branch stay for
  `tryRemountOnce`.
- [ ] Tests (`TEST(CASOwnershipPrimitives, …)`, in-memory backend, `OperationForTest`): `ARefusedCounterWriteRepeatsTheSameTuple`
  (`refuseNextWrite` is a clean 412, so use a subclass whose `write` throws an `S3`-coded access denial once: two
  writes, equal bytes and expected value, one read, counter `{N + 1}`); `AnUnclearLandedWriteIsSettledByReading`
  (`injectAmbiguousLandedWrite` → `EpochAllocated{N}`, counter `{N + 1}`); `OurBytesAfterAnEarlierUnclearCallAreOurs`
  (the write lands, the first call ends `GaveUp` by a liveness that turns false after the attempt; second call →
  `EpochAllocated{N}`); `TheSameBytesWithoutAnUnclearAttemptAreAnotherProcess` (another writer puts `{N + 1}` first →
  `EpochAdvancedByOther`); `ACounterMovedFurtherStops` (`{N + 2}`); `AnAbsentCounterNeedsAList` (a backend that hides
  the key from `read` but lists it → transient; removed → `EpochCounterLost`). Red on the base: no symbols. Commit:
  `Write the CAS writer-epoch counter as one reusable tuple`.

## Task 5: `claimMount` in `Reacquire` mode

**Files:** `$CA/Pool/CasServerRoot.{h,cpp}`, `src/Disks/tests/gtest_cas_lossless_claim.cpp`.
**Produces:** `makeReacquireClaim`, `ReacquireClaimResult`, `claimMount(…, const ReacquireCertificate &, const MountReacquireClaim &, …)`.

- [ ] Declarations:

```cpp
/// The claim body a reacquire fixes once: our uuid, the allocated epoch, `seq` one above the fenced body's.
MountReacquireClaim makeReacquireClaim(UInt128 uuid, uint64_t writer_epoch, const ReacquireCertificate & certificate,
                                       uint64_t now_ms, uint64_t ttl_ms, uint64_t start_boot_ms);
struct ReacquireClaimResult
{
    std::optional<MountClaimed> claimed;   /// our claim bytes are in the slot
    Observation seen = NotObserved{};      /// otherwise what the write's resolve read saw
    bool transient = false;                /// refused or unobserved: repeat the same claim
};
/// The `Reacquire` mode: replaces only the body `certificate` names, by its ETag. No clean-marker, observed
/// death, unsafe authorization or absent-slot create applies; the caller classifies `seen`.
ReacquireClaimResult claimMount(CasOperation & op, const Layout & l, const String & srid,
                                const ReacquireCertificate & certificate, const MountReacquireClaim & claim,
                                const CasEventSink & sink = {});
```

- [ ] Body: `op.replace(mountKey, claim.bytes, certificate.fenced.etag, Retry::untilDefinitive())`; `Committed` →
  `claimed = MountClaimed{claim.body, claim.bytes, etag, claim.start_boot_ms}` and event `MountClaim` branch `reacquire`;
  `Conflict` → `seen` (an `Object` with `bytes == claim.bytes` → `claimed` from it: a retry found its own landed claim);
  `NotObserved`, `Refused`, `GaveUp` → `transient = true`. Nothing reads the slot before the write: the 412's resolve
  read is the one observation R20 classifies.
- [ ] Tests (`TEST(CASLosslessClaim, Reacquire…)`): `ReacquireReplacesTheCertifiedBody`; `ReacquireNeverCreatesAnAbsentSlot`
  (slot removed → no `create`, `seen` is `ProvenAbsent`, key still absent); `ReacquireNeverTakesACleanMarker` /
  `…AFencedBodyOfAnotherEpoch` (`seen` holds it, slot unchanged); `ARetriedClaimFindsItsOwnLandedBytes` (first call:
  `injectAmbiguousLandedWrite` and a liveness that turns false → `transient`; second call → `claimed`, one body in the
  slot). Red on the base: no overload. Commit: `Add the certificate-bound Reacquire mode of the CAS mount claim`.

## Task 6: the step machine — `closeOldEpoch` → `claimNewEpoch`

**Files:** `$CA/Pool/CasMountReacquire.h`, `$CA/Pool/CasPool.{h,cpp}`, `$CA/Pool/CasMountRuntime.{h,cpp}`,
create `src/Disks/tests/gtest_cas_reacquire.cpp`.
**Produces:** `ReacquireStage`, `ReacquireStep`, `ReacquireOutcome`; `Pool::reacquireOnce`;
`CasMountRuntime::openAfterReacquire`, `renewForReacquireOnce`; `PoolConfig::reacquire_before_open_hook_for_test`.

- [ ] Header additions:

```cpp
enum class ReacquireStage : uint8_t { CloseOldEpoch, ProbeIdentity, ObserveCatalog, ClaimOwner, AllocateEpoch,
                                      ClaimSlot, StartRenewer, Open };
/// One reacquire, kept across retries of its stages: a retry resumes at `stage` with the same fixed writes.
struct ReacquireStep
{
    ReacquireStage stage = ReacquireStage::CloseOldEpoch;
    ReacquireCertificate certificate;
    std::optional<EpochWrite> allocation;
    std::optional<uint64_t> writer_epoch;
    std::optional<MountReacquireClaim> claim;
    std::optional<MountClaimed> claimed;
    uint64_t deadline_boot_ms = 0;
};
/// `Retry`: a stage failed transiently; `Ended`: a loss was latched; `Abandoned`: stop, FORGET or an end
/// latched by another thread.
enum class ReacquireOutcome : uint8_t { Open, Retry, Ended, Abandoned };
```

- [ ] `Pool::reacquireOnce(ReacquireStep &)` runs from `step.stage` to the end or the first non-success, under
  `remount_mutex`. Before each stage: `mountEnd() != None`, `vanishedIntentPublished()` or stop → `Abandoned`.
  A loss is `latchMountEnd(end)` then `reactToMountEnd(end, holder, detail)` (in that order), outcome `Ended`.
  - `CloseOldEpoch`: `fence_generation` bump, `ref_ledger.cancelRecoveriesAndAwaitQuiescence()`,
    `ref_ledger.quiesceRefTablesForRemount()`, `remount_quiesce_hook_for_test` — all under the old `liveWriterEpoch`.
    No farewell: `MountLeaseRenewer::release` is not called on a fenced slot.
  - `ProbeIdentity`: `probePoolLifecycleGate` on `gc_requests.admit()`. `Recover` → next; `StayTransient` or a throw →
    `Retry`; `Replaced` → `PoolIdentityLost`; `IdentityLost` → `corroborateAbsence` on `poolMetaKey()` and `ownerKey`
    — both `ProvenAbsent` → `PoolIdentityLost`, else `Retry`.
  - `ObserveCatalog`: `CasRefCatalog::observe` → catalog next; `OwnershipTransient` → `Retry`; `OwnershipCorrupt`
    `Absent` → `corroborateAbsence(refCatalogKey)` → `PoolIdentityLost` or `Retry`; `Undecodable`/`Ambiguous` →
    `LedgerFault`.
  - `ClaimOwner`: `claimOwner(…, OwnershipAuthority::Reacquire, …)`: `OwnerHeld` → next; `Foreign`/`Tombstoned` and
    `OwnershipCorrupt` → `PoolIdentityLost`; `Absent` → `corroborateAbsence(ownerKey)` → `PoolIdentityLost` or `Retry`.
  - `AllocateEpoch`: `beginEpochWrite` once (stored in `step.allocation`), then `writeEpochOnce` on
    `lease_requests.admit(reacquireLive)`; `EpochAllocated` → `step.writer_epoch`; `EpochAdvancedByOther` → `SlotTaken`
    with its message as detail; `EpochCounterLost`/`OwnershipCorrupt` → `PoolIdentityLost`; transient → `Retry`.
  - `ClaimSlot`: `step.claim` built once by `makeReacquireClaim(our_uuid, *writer_epoch, certificate, now_ms, ttl,
    bootMsNow())`; `claimMount(…, certificate, *claim)`; `claimed` → next; else `corroborateAbsence` then
    `classifyReacquireSlot`: `Applied` → next; `NewFence` → `step = ReacquireStep{.certificate = *new_fence}`, `Retry`;
    `Loss` → its end; `Certified`/`NotObserved`/`transient` → `Retry`.
  - `StartRenewer`: `chassert(*writer_epoch > liveWriterEpoch())`; `installRenewer`, `startRenewer(*claimed)`,
    `setLiveWriterEpoch`, `setProcessEpoch`; `deadline = start + ttl`; when the renewal window does not fit (the
    arithmetic of `tryRemountOnce`'s `renewal_window_fits`), `renewForReacquireOnce()`: committed → new deadline;
    `slot` `FencedByGc` → fresh step from `slot->fenced`, `Retry`; latched → `Ended`; else `Retry` (resumes here).
  - `Open`: `config.reacquire_before_open_hook_for_test`, then `mount_runtime.openAfterReacquire(uuid, epoch,
    deadline)`; false → `Abandoned` if an end, stop or intent is set, else `step.stage = StartRenewer`, `Retry`.
  Logs: `LOG_INFO` "CAS disk '{}': the GC of another server fenced writer_epoch {}; reacquiring the mount slot" at
  the first stage, `LOG_WARNING` once per step on its first `Retry` naming the stage and error, `LOG_INFO`
  "… reacquired under writer_epoch {} (was {})" on open. No `CASRemount*` counter moves.
- [ ] `CasMountRuntime::openAfterReacquire(UInt128, uint64_t epoch, uint64_t deadline)`: under `driver_mutex`, false if
  `workers_stop_requested`, `vanished_intent`, `mountEnd() != None`, lifecycle not `TransientNotLive`, or
  `admit(fenceGeneration(), conditionalReserveMs() - margin)` would refuse at `deadline`; else `armMountFence`,
  `noteRemounted`, `renewal_driver_state = workers_started ? WorkerIdle : Dormant`, true
  (`/// One step with the decision: a latch or an expiry between them must keep the fence closed.`).
  `renewForReacquireOnce` = `renewRenewerOnce(admitRenewerCall(Parked, RemountCall), RemountCall, false, false)`;
  `renewRenewerOnce` now returns the `MountRenewResult` (callers take `attempt_start_boot_ms`).
- [ ] The direct path, so the step machine is testable without threads: `std::optional<ReacquireCertificate>
  owed_reacquire; /// guarded by driver_mutex`. In `DriverLease::finish`, a `DirectCall` terminal result with
  `slot->verdict == FencedByGc`, `slot->fenced` set and no latched end: `tripMountLost()`, `owed_reacquire = {…}`,
  and `schedule_remount_calls_for_test` is not counted. The constructor takes `std::function<ReacquireOutcome(
  ReacquireStep &)> reacquire_attempt` beside `remount_attempt`; `Pool` binds `reacquireOnce`.
  `runReacquireForTest()` (runtime and `Pool`): no workers, takes `owed_reacquire` into a member `ReacquireStep`
  (`LOGICAL_ERROR` if neither is set), `Dormant` → `Parked`, one `reacquire_attempt`, `Parked` → `Dormant` unless
  opened, returns the outcome; a later call resumes the kept step.
- [ ] Test harness in `gtest_cas_reacquire.cpp`: pools from `PoolConfig{.pool_prefix = "p", .server_root_id = "test",
  .boot_ms_fn, .retry_sleep_fn = advance, .wait_sleep_fn = advance}`; `fenceAndRenew(store, backend)` (PR 12's
  `fenceOutMount` + `renewWatermarkOnce` throws), then `store->runReacquireForTest()`. Each test names
  why it is red on the base (where the same scenario runs through `tryRemountOnce`):
  - `AGcFenceOfTheActiveEpochOpensANewEpoch`: `Open`, `writerEpoch() == E + 1`, counter `{E + 2}`, slot holds the
    claim body, `mountEnd() == None`, `mayMutate()`. Golden for the epoch; red for the API.
  - `TheOldEpochClosesBeforeTheCounterMoves`: `remount_quiesce_hook_for_test` records the counter bytes → `{E + 1}`.
    Red: the base cancels recoveries after the allocation.
  - `ATwinThatTookAndReleasedTheSlotStops`: after the fence the test writes our uuid, epoch `E + 5`, clean marker →
    `Ended`, `SlotTaken`, slot bytes unchanged, one graceful stop. Red: the base reclaims the clean marker.
  - `ACleanMarkerOfTheFencedPairStops` → `SlotCorrupt`; `AnAbsentSlotStops` (removed, LIST agrees) → `SlotAbsent`,
    key still absent (red: the base creates it); `AForeignBodyStops` → `SlotTaken` (golden);
    `TheUnsafeKnobNeverReclaimsATwin` (`.unsafe_remount_no_delay = true`, twin unfenced at `E + 5`) → `SlotTaken`, slot
    unchanged (golden).
  - `AStorm503OnTheCounterAllocatesOneNumber`: counter `PUT`s answer `503` (not applied) for 30 s of injected time →
    `Open`, epoch `E + 1`, counter `{E + 2}`, one counter `GET`, every counter `PUT` with equal bytes and `If-Match`.
    Red: the base's 90 s window ends the attempt and the next one re-reads.
  - `AnUnclearCounterIncrementIsSettledByReading`: `injectAmbiguousLandedWrite(epochKey)` → epoch `E + 1`, counter
    `{E + 2}` (golden on the epoch); `AnUnclearIncrementWhoseCallEndedIsStillOurs`: landed, then every request fails
    for 120 s, then healthy → `Open` with `E + 1`, counter `{E + 2}`. Red: the base allocates `E + 2`.
  - `ACounterAdvancedByAnotherProcessStops`: a backend hook bumps the counter between our read and write →
    `Ended`, `SlotTaken`, the `ERROR` line contains `advanced by another process`. Red: the base retries forever.
  - `ABreakAfterTheClaimContinuesWithOurBody`: `injectAmbiguousLandedWrite(mountKey)` → `Open`, one claim body;
    `ARefusedRenewalAfterALateClaimResumesWithoutANewEpoch`: the boot clock jumps past the window after the claim and
    the first renewal `PUT` is an access denial → second `runReacquireForTest` step resumes at `StartRenewer`,
    counter `{E + 2}`, one claim `PUT`. Red: the base allocates again.
  - `AnAbsentAnchorIsPoolIdentityLostAndNotCreated` (owner removed; LIST agrees) → `PoolIdentityLost`, owner absent;
    `AnAnchorAbsenceTheListContradictsRetries` → `Retry`, `mountEnd() == None`. Red: the base never stops.
  - `AnAbsentCounterIsPoolIdentityLost`; `AnAmbiguousCatalogIsALedgerFault` (fatal event, no graceful).
  - `InterferenceDuringReacquireNeverOpens`, `ForgetDuringReacquireNeverOpens`, `StopDuringReacquireNeverOpens`: the
    open hook latches `LedgerFault` / calls `publishVanishedIntentForTest` / `beginShutdownForTest` → not `Open`,
    `mayMutate()` false. `TheLeaseRunningOutBeforeOpenRenewsFirst`: the hook advances the clock past the deadline →
    `Retry`, then a renewal and `Open`.
  - `AFencedFloorProtectsItsPrefixUntilTheNewClaim`: `prefixEligible(*store, RootNamespace{"test/store/465/aa@cas@"},
    {E, m})` is false under the fenced body (`m` = its `min_active_build_sequence`) and true after `Open` (golden).
- [ ] Gate `CASReacquire*:CASMountStop*:CASLosslessClaim*:CASOwnershipPrimitives*`. Commit:
  `Reacquire a GC-fenced CAS mount by certificate in resumable steps`.

## Task 7: the renewal thread runs the reacquire

**Files:** `$CA/Pool/CasMountRuntime.{h,cpp}`, `$CA/Pool/CasPool.{h,cpp}`, `$CA/ContentAddressedMetadataStorage.{h,cpp}`,
`src/Storages/System/StorageSystemContentAddressedMounts.cpp`, `src/Disks/tests/gtest_cas_reacquire.cpp`,
`gtest_cas_mount_stop.cpp`, `gtest_cas_lifecycle_snapshot.cpp`.

- [ ] Failing tests first (`gtest_cas_reacquire.cpp`, a pool with `.background_watermark = true` and a test
  `worker_factory` that runs the loop on a joinable `std::thread`, barrier from `renewal_finished_hook_for_test`):
  `TheRenewalThreadReacquiresWithoutTheRemountWorker` — fence the slot, wait for `lifecycle() == Live` with epoch
  `E + 1`; `remountRequestedGenerationForTest() == 0`, `scheduleRemountCallCountForTest()` unchanged. Red: the
  base requests a remount. `NoRenewalWriteOfTheOldEpochAfterTheCounterMoves`: a recording backend; after the first
  counter `PUT` no mount `PUT` carries `writer_epoch == E`. `ALossFoundByTheReacquireStopsTheServer` (slot foreign
  after the fence) → one graceful stop. Retarget `CASMountStop.OurOwnFenceStillRemountsAndNothingMore` to
  `OurOwnFenceIsReacquiredAndNothingMore` (count unchanged, `runReacquireForTest()` → `Open`, no stop event).
- [ ] Runtime: Task 6's `finish` branch extends to `WorkerCall`: the state becomes `Parked`,
  `remount_requested_generation` is not touched. Every other unlatched terminal keeps today's branch.
  `renewalLoop`: under the lock, if `owed_reacquire` → take it and run `reacquireUntilSettled(std::move(cert))`
  outside the lock; the loop's `catch` covers it (R17 as in PR 12). `reacquireUntilSettled`: `ReacquireStep
  step{.certificate = …}`; loop `reacquire_attempt(step)`: `Open`/`Ended`/`Abandoned` return; `Retry` →
  `waitBeforeNextReacquireStep(Retry::spacedPause(Retry::drawSpacing(Retry::kDefinitiveAttemptSpacingMs), started,
  bootMsNow()))` (`wait_sleep_fn` when set, else `driver_cv.wait_for` ending on stop).
- [ ] Lifecycle word: `casLifecycleWords(TransientNotLive)` → `{"paused", "fenced_by_gc"}`; header comment of
  `ContentAddressedMetadataStorage.h` and the `lifecycle_reason` column description list `fenced_by_gc` instead of
  `remounting`; `gtest_cas_lifecycle_snapshot.cpp` expects `paused(fenced_by_gc)` (red by design).
- [ ] Gate `CAS*`. A pre-existing test that newly fails because an R8 renewal no longer requests a remount is
  retargeted to `runReacquireForTest` with the same assertion on the outcome; any other red: stop and report.
  List retargeted tests in the commit body. Commit: `Reacquire a GC-fenced CAS mount on the renewal thread`.

## Task 8: documentation

**Files:** `docs/en/antalya/cas/architecture/mounts-and-leases.md`, `docs/en/antalya/cas/operations/troubleshooting.md`.

- [ ] `mounts-and-leases.md`: `## When the GC fences this server {#gc-fence-reacquire}` — the renewal thread closes
  the old epoch (in-flight ref recoveries end), then takes the slot under a new `writer_epoch` only if the slot
  still holds the fenced body it read; meanwhile `lifecycle = paused`, `lifecycle_reason = fenced_by_gc`; transient
  failures retry about once a second; a twin, a foreign body, an absent slot, owner or counter, or a counter moved
  by another process stop the server (`slot_taken`, `slot_absent`, `pool_identity_lost`); no clean marker,
  observation or `cas_unsafe_remount_no_delay` applies here. Replace "remounting" wherever the page describes the
  reason. `troubleshooting.md`: the row "keeps losing its mount lease and self-remounting" says "reacquiring after a
  GC fence" and names `fenced_by_gc`.
- [ ] Commit: `Document the CAS reacquire after a GC fence`.

## Task 9: integration — fenced during an outage, same server, new epoch

**Files:** `tests/integration/test_cas_mount_renewal_retry/test.py`, new `configs/reacquire_disks.xml`,
`configs/reacquire_peer.xml`.

- [ ] `reacquire_disks.xml`: disk `disk_cas_reacq` (`cas_server_root_id` `itest-cas-reacq`, endpoint
  `http://s3proxy:11121/test/cas_mount_reacq/`, `cas_gc_enabled` false, the timing of `disk_cas_renewal`), policy
  `cas_mount_reacq`. `reacquire_peer.xml`: disk `disk_cas_reacq_peer` (`itest-cas-reacq-peer`, endpoint
  `http://rustfs1:11121/test/cas_mount_reacq/`, `cas_gc_enabled` 1, `cas_gc_interval_sec` 1, the same lease timing).
  Instances `reacq_node` (`stay_alive`) and `reacq_peer`, both `with_rustfs`.
- [ ] Tests (deadlines 60 s unless stated; `finally`: proxy reset):
  - `test_gc_fences_during_an_outage_and_the_same_server_continues_under_a_new_epoch`: insert a row; record pid,
    `writer_epoch` `E`, profile events; proxy `rate 1.0`, `modes ["503"]`, `methods ["PUT"]`, `path_substring` the
    reacq mount key, until RustFS shows `gc_fenced` true in the slot (the peer's GC; deadline 90 s); reset; wait for
    `lifecycle = live`. Then: same pid, `writer_epoch == E + 1`, slot not fenced and its `writer_epoch == E + 1`,
    `CASRemountAttempts` and `CASRemountSucceeded` deltas 0, no `whole-chain remount` line since start, the log has
    `reacquired under writer_epoch`, a new insert succeeds and both rows read back.
  - `test_a_fence_written_during_the_outage_without_a_gc_peer_is_reacquired`: the same, but the test writes the GC
    transform itself (decode the slot, `gc_fenced = true`, `seq + 1`, `put_object`) while the proxy blocks `PUT`s.
  - `test_a_twin_body_after_the_fence_stops_the_server`: fence as above, then put our uuid with `writer_epoch + 5` and
    the clean marker before resetting the proxy → pid gone, log has `holds a body of another incarnation`; restart.
- [ ] Run the module (`python -m ci.praktika run "integration" --test test_cas_mount_renewal_retry` from `$WT`, output to
  `$WT/build/test_cas_mount_renewal_retry_pr14.log`); a subagent summarizes. Commit:
  `Test that a GC-fenced CAS server continues under a new epoch`.

## Task 10: acceptance matrix (spec 10-05)

**Files:** new module `tests/integration/test_cas_mount_reacquire_matrix/` (`__init__.py`, `test.py`, `configs/` with the
two disks of Task 9 under `itest-cas-matrix*`, the proxy files copied from `test_cas_mount_renewal_retry`).

- [ ] Six tests, parametrized by `gc_peer in (True, False)` (the peer instance is started in both; `False` sets its
  `cas_gc_enabled` off via a second disk config) × scenario: `put_503_90pct_5min` (`rate 0.9`, `PUT`, 300 s),
  `access_lost_60s` (`rate 1.0`, `modes ["reset"]`, every method, 60 s), `sigstop_writer_60s` (`kill -STOP` the server
  pid, 60 s, `kill -CONT`). Each records into `cas_matrix_results.json` in the test's log dir: fences seen (slot
  reads with `gc_fenced`), epoch before and after, seconds from fault end to `live`, seconds to the first successful
  insert, `CASMountRenewalAttempts` in the window, pid unchanged. Assertions: the server ends `live` with the same pid;
  epoch after `== before + 1` when a fence was seen, else `== before`; an insert succeeds; no `whole-chain remount`
  line. With the peer, at least one of the three scenarios must show a fence (else the window did not cover the
  lease and the test fails, saying so).
- [ ] Run the module (about 15 min); paste the JSON table into the PR description. Commit:
  `Add the CAS reacquire acceptance matrix`.

## Task 11: mutations

- [ ] For each mutation, edit one line in `$WT`, `ninja -C $WT/build unit_tests_dbms`, run the named test under
  `ulimit -v 16000000`, expect red, `git -C $WT checkout -- <file>`:

| Mutation | Test that must fail |
|---|---|
| `classifyReacquireSlot` accepts any fenced body of the pair (drop the ETag compare) | `AFencedBodyOfThePairUnderAnotherEtagIsCorrupt` |
| `classifyReacquireSlot` drops rule 1 (claim bytes) | `ABreakAfterTheClaimContinuesWithOurBody` |
| `AllocateEpoch` calls `beginEpochWrite` on every attempt | `AnUnclearIncrementWhoseCallEndedIsStillOurs` |
| `EpochAdvancedByOther` → `Retry` | `ACounterAdvancedByAnotherProcessStops` |
| `openAfterReacquire` skips the `mountEnd()` check | `InterferenceDuringReacquireNeverOpens` |
| `openAfterReacquire` skips the deadline check | `TheLeaseRunningOutBeforeOpenRenewsFirst` |
| owner absence without `corroborateAbsence` | `AnAnchorAbsenceTheListContradictsRetries` |
| `finish` bumps `remount_requested_generation` for R8 | `TheRenewalThreadReacquiresWithoutTheRemountWorker` |
| `CloseOldEpoch` stage moved after `ClaimSlot` | `TheOldEpochClosesBeforeTheCounterMoves` |

- [ ] Record the table with red/green per row in the PR description. No commit.

## Task 12: gates and PR

- [ ] `ninja -C $WT/build unit_tests_dbms > $WT/build/build_pr14.log 2>&1`;
  `TMPDIR=$WT/build/tmp $WT/build/src/unit_tests_dbms --gtest_filter='CAS*' > $WT/build/test_cas_pr14.log 2>&1`; the
  same in `build_asan`; a Fast-test (no S3) compile. All green; counts against Setup's.
- [ ] PR to `antalya-26.6` per `.github/PULL_REQUEST_TEMPLATE.md`, category `Improvement`: "A CAS server whose mount
  the GC fenced during an outage now takes its slot again on the renewal thread, by the ETag of the fenced body,
  without a remount thread; anything else found in the slot stops the server." Mixed versions: format and observers
  unchanged; a peer on PR 12 keeps the remount chain. Model results (Tasks 1–2), the matrix table and the mutation
  table in the description.
