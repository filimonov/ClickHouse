# CAS lease PR 12: terminal losses and the process stop — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:executing-plans (or
> superpowers:subagent-driven-development). Steps use checkbox (`- [ ]`) syntax.

**Goal:** a renewal that reads a slot this server no longer owns ends the mount for good instead of
remounting: the fence is latched first, then the server is asked to stop, then one `ERROR` line says what
was in the slot. A ref lane fault or a broken renewal machine ends the process at once (`LOG_FATAL` +
`std::terminate`). Only our own `gc_fenced` body (R8) keeps today's remount chain; a twin found by that
chain stops the server too. Absence counts only when a `LIST` agrees with the `GET`.
**Architecture:** one pure classifier (`classifyMountSlot`) judges every slot observation against the
renewer's two bodies; the runtime latches a `MountEnd` (lock-free, first end wins), moves the lifecycle to
`Stopping` and reacts by the pool's `MountLossReaction`; a process-wide handler pair (`CasProcessStop`)
raises `SIGTERM` or terminates. **Tech stack:** C++23, gtest (`unit_tests_dbms`), `InMemoryBackend`
seams, the PR 6 wire harness (Poco HTTP server), praktika integration, TLA+.

**Spec:** `/home/mfilimonov/workspace/ClickHouse/master/docs/superpowers/specs/2026-10-07-cas-mount-lease-series-design.md`
— §7 row 12 (both tables), §2 "Потеря" (R8–R18 and the text around it), "Остановка сервера", "Shutdown",
§9 `CaDiskLifecycle` шаг 1 (done, `a67369873eb`). The owner decisions in
`/home/mfilimonov/workspace/ClickHouse/master/tmp/lease-series/spec-r8-todo.md` override it (see "Code vs spec").

**Base:** a merge of PR 11b's final head (`fix/antalya-26.6/cas-lossless-claim`, `506769e3372` at plan time)
and PR 10 v2 (`b9a51702188`, `fix/antalya-26.6/cas-ref-lane-fault-latch-squash`). **Branch:**
`fix/antalya-26.6/cas-terminal-losses-stop`. `$WT` = the executor's own worktree (never
`cas-janitor-budget`, which is in use); `$CA` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed`.
Every commit ends with:

```
Signed-off-by: Mikhail Filimonov <mfilimonov@altinity.com>
Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
```

**Files outside `$CA`:** none in production: the graceful stop is `::raise(SIGTERM)` from `$CA` (the body of
`BaseDaemon::terminate`; no `src/Daemon` include). Tests, the integration module, `docs/en/antalya/cas/`.

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
- New source files are globbed (`add_headers_and_sources` in `src/CMakeLists.txt`): run `cmake .` in
  `$WT/build` and `$WT/build_asan` after creating one, or the target silently omits it.

## Review Focus

- **Failure asymmetry of the verdict.** A false loss stops a healthy server (a restart); a missed loss keeps two
  writers on one slot (split-brain, data damage). So whatever is not provably ours or provably a GC fence of ours
  is a loss — except absence, where a proxy 404 is common: there `NotObserved` (retry) wins until `LIST` agrees.
- **Graceful vs fatal.** After the latch every write is fenced and the farewell and lane drain are skipped
  (Task 6), so a graceful shutdown writes nothing; check `finishTeardown`, `~Pool`, FORGET's teardown. A ledger
  fault or a renewal-machine defect ends without shutdown (owner decision).
- **First end wins**, lock-free: one `compare_exchange` on `mount_end`; `armMountFence` re-checks it `seq_cst`.

## Code vs spec (flagged; the plan follows the spec and the owner decisions)

1. Spec row 12 «R16 — в PR 14»; the dispatch lists R16 with the external class and the LIST rule. Plan follows
   the row: `identity_lost`/`vanished(replaced)` unchanged; `corroborateAbsence` is ready for PR 14.
2. Spec «колбэк … по умолчанию `::raise(SIGTERM)`»; decision (c) «a callback that calls `BaseDaemon::terminate`».
   Plan: the default is `::raise(SIGTERM)`, `std::terminate` if it fails — that function's body — so neither
   `src/Daemon` nor `programs/server/Server.cpp` changes. The literal call is one line in `Server.cpp`.
3. Spec order «латч → `raise` → строка `ERROR`»; (c) lists «`LOG_ERROR` + latch + graceful stop». Plan keeps the
   spec order: a throwing log line must not cancel the stop.
4. Spec tests «каждая строка R9–R15, R17 → латч, колбэк, `ERROR`», reasons `ledger_interference`/`ledger_corrupt`.
   Per (b)/(c): R9–R13 → latch, graceful, `ERROR`; `ledger_fault` and `local_defect` → latch, `FATAL`, terminate.
5. Spec «R18 → `closed`» does not say who wins against a loss latched after FORGET's intent; the plan follows
   `CaDiskLifecycle` (the loss wins, `stopping`).
6. Spec «в `stopping` … farewell не пишется» is PR 13's row; (c)'s mandatory "shutdown does not hang on a dead
   CAS disk" needs it now. Plan skips farewell and drain for loss ends only; FORGET's farewell stays (PR 13).
7. Spec «исход `claim.kind != Claimed` … ≈ 5 строк». Code: a newer epoch of ours makes `claimMount` throw
   `ABORTED`, undecodable bytes throw `CORRUPTED_DATA`. Plan re-reads the slot once on those codes (≈ 30 lines).
8. Spec R2 «`GaveUp` … только по stop». Code: `renewalLive` also ends on park, a non-`Live` lifecycle, a latched
   fence. Plan: `GaveUp` keeps today's handling; whoever closed the liveness reacts.
9. Spec R2 «точные байты текущей операции после чистого `412` → `slot_taken`» says nothing of our confirmed
   bytes under a foreign ETag; plan treats both as a clone → `slot_taken`.
10. Spec R17 excludes `MEMORY_LIMIT_EXCEEDED` via PR 3, not a prerequisite of 12. Plan: that code on the renewal
    thread keeps today's handling, never `local_defect`.
11. Spec is silent on the refusal class on `Stopping`. Plan: retry-later, because a terminal error lets
    `ReplicatedMergeTreePartCheckThread` detach parts while the server stops.
12. Spec stop line «CAS disk 'cas1' …»: `Pool` has no disk name; plan adds `PoolConfig::disk_name`.
13. Spec §9 has no `CaCasMountCore` row for PR 12; r8-todo adds the two-step absence. Task 1 adds it, and the
    farewell-on-`Stopping` invariant `CaDiskLifecycle` reported.
14. "Observe-only" (e) has no code term. Plan: `read_only` has no renewer; a writable pool without background
    workers (`context_available == false`, tools) gets `MountLossReaction::CloseMount`.

## Setup

- [ ] `git -C $WT switch -c fix/antalya-26.6/cas-terminal-losses-stop <11b-final-head>`, then
  `git -C $WT merge --no-ff b9a51702188` with subject `Merge PR 10 v2 (ref lane fault latch) into PR 11b:
  base for terminal losses` and the two trailers. Resolve conflicts (expected in `Pool/CasPool.cpp`:
  keep both the 10 v2 constructor callback and the 11b `tryRemountOnce`). `cmake .` in both build dirs,
  build `unit_tests_dbms`, run the `CAS*` gate in release: all green is the baseline; record the count.

## Task 1: models first (master worktree)

**Files:** `docs/superpowers/models/CaCasMountCore.tla`, all `CaCasMountCore_*.cfg`, new
`CaCasMountCore_absence_twostep.cfg`, `CaCasMountCore_sab_proxy404stop.cfg`,
`CaCasMountCore_witness_absencestop.cfg`, `run_mount.sh`, `CaCasMountCore_RESULTS.md`;
`CaDiskLifecycle.tla`, all `CaDiskLifecycle_*.cfg`, new `CaDiskLifecycle_sab_farewellonstopping.cfg`,
`run_disklifecycle.sh`, `CaDiskLifecycle_RESULTS.md`.
**Precondition:** `git -C /home/mfilimonov/workspace/ClickHouse/master status --short docs/superpowers/models/`
shows no change to these files (at plan time `CaCasMountCore.tla` and its cfgs carry a foreign uncommitted
edit); otherwise stop and report.

- [ ] `CaCasMountCore`: constants `TwoStepAbsence`, `ProxyMay404`, `SabAbsenceGetOnly` (FALSE in every existing
  cfg: their rows and counts do not move); variables `proxy404`, `absPend \in [Incs -> {"none", "getmiss"}]`,
  history `falseAbsenceStop`, `absenceStopped`. `ProxyToggle == ProxyMay404 /\ proxy404' = ~proxy404`.
  `RenewGetMiss(a)`: `TwoStepAbsence`, a live incarnation (`localEpoch[a] > 0 /\ ~localLost[a] /\ ~crashed[a]`),
  `absPend[a] = "none"`, `mount = None \/ proxy404` → `absPend' = "getmiss"`; under `SabAbsenceGetOnly` it stops
  at once (`localLost' = TRUE`, `absenceStopped' = TRUE`, `falseAbsenceStop' = falseAbsenceStop \/ mount # None`).
  `RenewListStep(a)`: from `"getmiss"`, `proxy404` (LIST failed) or `mount # None` (LIST lists it) → `"none"`;
  `mount = None` → stop as above. Invariant `AbsenceStopIsTrue == ~falseAbsenceStop` (a stopped incarnation
  writes nothing by the existing `SupersededWriterMakesNoMutation`); witness `W_NoAbsenceStop == ~absenceStopped`.
- [ ] Cfgs from `CaCasMountCore_stage1.cfg` (one actor, no twins): `absence_twostep` (`TwoStepAbsence =
  ProxyMay404 = TRUE`) green; `sab_proxy404stop` (+ `SabAbsenceGetOnly`) → `AbsenceStopIsTrue`;
  `witness_absencestop` → `W_NoAbsenceStop` (a real deletion, `ClearExpiredMount`, stops). Rows in `run_mount.sh`.
- [ ] `CaDiskLifecycle`: constant `SabFarewellOnStopping`; `FTeardown` writes the farewell only if
  `pstate # "Stopping" \/ SabFarewellOnStopping`; invariant `NoFarewellOnStopping == ~(farewell /\ everStopping)`
  checked by `main`; existing cfgs set the constant FALSE; row `sab_farewellonstopping` (`main` + TRUE) →
  `NoFarewellOnStopping`. The fatal class is not modelled: the process ends and no action follows.
- [ ] Run `run_mount.sh` (new rows; `stage1` and `twins` as controls) and `run_disklifecycle.sh` under
  `nice -n 10`; expected: `ALL EXPECTATIONS MET`. Append dated sections (rows, counts, the two traces) to
  both RESULTS files. Commit the exact files: `model(cas): two-step slot absence; no farewell on Stopping`.

## Task 2: process-stop handlers

**Files:** create `$CA/Pool/CasProcessStop.h`, `$CA/Pool/CasProcessStop.cpp`,
`src/Disks/tests/cas_process_stop_log.h`, `src/Disks/tests/gtest_cas_process_stop.cpp`.
**Produces:** `requestGracefulProcessStop`, `terminateProcess`, `replaceProcessStopHandlersForTest`;
`tests::processStopLog()`, `tests::ScopedStopLineCapture`.

- [ ] Header:

```cpp
#pragma once
#include <functional>

namespace DB::Cas
{

/// How a CAS mount that can no longer be this pool member ends the process; one lost slot stops every disk.
struct ProcessStopHandlers
{
    /// Empty: `::raise(SIGTERM)`, the body of `BaseDaemon::terminate`; the signal listener runs the shutdown.
    std::function<void()> graceful;
    std::function<void()> fatal;   /// empty: `std::terminate`
};
/// The first call in the process asks for the graceful stop; later calls do nothing.
void requestGracefulProcessStop() noexcept;
/// Ends the process; returns only when a test installed a `fatal` handler that returns.
void terminateProcess() noexcept;
/// Installs `handlers`, re-arms the once-per-process stop; only while no CAS pool runs.
ProcessStopHandlers replaceProcessStopHandlersForTest(ProcessStopHandlers handlers);

}
```

- [ ] `.cpp`: function-local static handlers, `std::atomic<bool> graceful_requested`. `requestGracefulProcessStop`:
  `if (graceful_requested.exchange(true)) return;`, then the handler or `if (::raise(SIGTERM) != 0) std::terminate();`
  (a stop that cannot be asked for must not leave a split brain running); a throwing test handler is swallowed.
  `terminateProcess`: the handler if set, else `std::terminate()`.
- [ ] `cas_process_stop_log.h` (inline, function-local statics, one mutex): `ProcessStopLog::reset(std::function<String()> probe = {})`
  clears and installs handlers appending `"graceful:" + probe()` / `"fatal:" + probe()`; `push`, `events`,
  `graceful`, `fatal`; `processStopLog()`; `ScopedStopLineCapture` — a `Poco::Channel` on logger `CasPool` for
  its lifetime (restoring channel and level like `ScopedRenewalLogCapture` in `gtest_cas_mount.cpp`) pushing
  `"error-line:" + text` / `"fatal-line:" + text` for messages that start with `CAS disk '`.
- [ ] `gtest_cas_process_stop.cpp`: an `Environment` whose `SetUp` calls `processStopLog().reset()`, registered by
  `[[maybe_unused]] const auto * const cas_process_stop_environment = ::testing::AddGlobalTestEnvironment(new CasProcessStopEnvironment);`,
  so no test in the binary can signal or abort the test process. Tests:

```cpp
TEST(CASProcessStop, TheGracefulStopIsRequestedOncePerProcess)
{
    auto & log = DB::Cas::tests::processStopLog();
    log.reset();
    requestGracefulProcessStop();
    requestGracefulProcessStop();
    EXPECT_EQ(log.graceful(), 1u);
    EXPECT_EQ(log.fatal(), 0u);
}

/// The default handlers really end the process: the statement dies by the signal it names, and nothing in
/// the child returns into gtest, which would call `std::exit` with pool threads alive.
TEST(CASProcessStopDeathTest, TheDefaultGracefulStopRaisesSigterm)
{
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT({ replaceProcessStopHandlersForTest({}); requestGracefulProcessStop(); std::_Exit(0); },
                ::testing::KilledBySignal(SIGTERM), "");
}

TEST(CASProcessStopDeathTest, TheDefaultFatalStopAborts)
{
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT({ replaceProcessStopHandlersForTest({}); terminateProcess(); std::_Exit(0); },
                ::testing::KilledBySignal(SIGABRT), "");
}
```

- [ ] Run `--gtest_filter='CASProcessStop*'`: red on the base (the header does not exist); green after.
  Commit: `Add process-wide stop handlers for a lost CAS mount`.

## Task 3: the slot classifier and the stop texts

**Files:** create `$CA/Pool/CasMountLoss.h`, `$CA/Pool/CasMountLoss.cpp`,
`src/Disks/tests/gtest_cas_mount_loss.cpp`. **Produces:** `MountEnd`, `mountEndWord`, `isLossEnd`,
`endsWithoutShutdown`, `MountLossReaction`, `MountSlotVerdict`, `OwnMountBodies`, `MountSlotReading`,
`classifyMountSlot`, `describeMountEnd`.

- [ ] Declarations (comments as written here; `Etag` is not default-constructible, so it is optional):

```cpp
/// Why a CAS mount ended for good. The first end latched wins.
enum class MountEnd : uint8_t
{
    None, SlotTaken, SlotAbsent, SlotCorrupt,
    FencedByGc,    /// the decommission pool only: it impersonates a dead member and never remounts
    LedgerFault, LocalDefect,
    Replaced,      /// the data root holds another pool
    Forgotten,     /// `SYSTEM CAS FORGET`
};
std::string_view mountEndWord(MountEnd end);   /// "slot_taken", …, "ledger_fault", "local_defect", "forgotten"
bool isLossEnd(MountEnd end);                  /// every end but `None`, `Replaced`, `Forgotten`
/// A broken process invariant: a shutdown would write under an identity another process may hold.
bool endsWithoutShutdown(MountEnd end);        /// `LedgerFault`, `LocalDefect`

/// `StopServer`: a writable mount of this server's own slot. `CloseMount`: writable without background
/// renewal (tools). `FailDecommission`: `Pool::openForDecommission`, which never remounts.
enum class MountLossReaction : uint8_t { StopServer, CloseMount, FailDecommission };
enum class MountSlotVerdict : uint8_t { Ours, FencedByGc, SlotTaken, SlotAbsent, SlotCorrupt, NotObserved };
/// The two bodies this server can have in its slot: the last confirmed one and the renewal in flight.
struct OwnMountBodies
{
    UInt128 server_uuid{};
    uint64_t writer_epoch = 0;
    std::optional<MountLease> confirmed;
    String confirmed_bytes;
    std::optional<Etag> confirmed_etag;
    std::optional<MountLease> in_flight;
};
struct MountSlotReading { MountSlotVerdict verdict = MountSlotVerdict::NotObserved; std::optional<MountLease> holder; };
/// Total: one verdict for every observation. Bodies are compared decoded, not as bytes.
MountSlotReading classifyMountSlot(const Observation & seen, const OwnMountBodies & own);
String describeMountEnd(MountEnd end, MountLossReaction reaction, const String & disk_name,
                        const String & server_root_id, const std::optional<MountLease> & holder, std::string_view detail);
```

- [ ] `classifyMountSlot`: `ProvenAbsent` → `SlotAbsent`; `NotObserved`/`Meta` → `NotObserved`; an
  `Object` whose `decodeMountLease` throws `CORRUPTED_DATA` → `SlotCorrupt` (other codes propagate); other
  `server_uuid` or `writer_epoch` → `SlotTaken`; exact `confirmed_bytes` under `confirmed_etag` → `Ours`;
  for each of `confirmed`, `in_flight`: a GC fence of it (`gc_fenced`, the own body not fenced,
  `seq == own.seq + 1`, every other field equal, the transform `computeHeartbeatFloor` writes) →
  `FencedByGc`; equal to it in every field → `SlotTaken` (our bytes under an incarnation we did not write:
  another process sent them); anything else → `SlotCorrupt`. `holder` = the decoded body when there is one.
- [ ] `describeMountEnd` builds exactly these lines (`{d}` disk, `{s}` server_root_id, holder fields from
  `describeMountHolder`-style `hostname=…, pid=…, server_uuid=…, writer_epoch=…, gc_fenced=…`):
  - `SlotTaken`: `CAS disk '{d}' (server_root_id={s}): the mount slot holds a body of another incarnation ({holder}).`
  - `SlotAbsent`: `CAS disk '{d}' (server_root_id={s}): the mount slot is absent; a slot is deleted by SYSTEM CAS DROP POOL MEMBER or by hand.`
  - `SlotCorrupt` with holder: `… the mount slot holds a body of this server's (server_uuid, writer_epoch) that this server did not write ({holder}).`; without: `… the mount slot holds bytes that do not decode as a mount lease.`
  - `FencedByGc`: `… the GC of another server fenced the decommission claim ({holder}).`
  - `LedgerFault`/`LocalDefect`: `… {detail}. The process ends now without a shutdown: a shutdown writes the farewell and the checkpoints under an identity another process may hold.`
  - Suffix by reaction for the other ends: `StopServer` — ` New changes are closed; server stop requested. Every CAS disk of this server stops with it.`
    plus, for `SlotTaken`, ` Check for two servers sharing one server_root_id or two starts with one UUID.`;
    `CloseMount` — ` New changes are closed; this disk stays closed until restart.`;
    `FailDecommission` — ` The decommission command fails and deletes nothing more.`
- [ ] Tests (`InMemoryBackend`: an `Object` with a real ETag comes from an `OperationForTest` write and read;
  `ownBody(seq)` = uuid 7, epoch 3, hostname `h`, pid 11, started 1000, expires 31000, floor 5,
  `write_attempt_id = seq`), one `TEST(CASMountLoss, …)` per row asserting `verdict` and `holder`: confirmed bytes
  and ETag → `Ours`; same bytes re-written → `SlotTaken`; in-flight body → `SlotTaken`; GC fence of confirmed /
  of in-flight → `FencedByGc`; own pair `gc_fenced` `seq + 5` / `hostname = "elsewhere"` → `SlotCorrupt`; epoch 4,
  epoch 2, epoch 4 fenced, epoch 2 farewell, uuid 8 → `SlotTaken`; `"not a mount lease"` → `SlotCorrupt`, no
  holder; `ProvenAbsent` → `SlotAbsent`; `NotObserved` → `NotObserved`. `TheStopLinesNameWhatWasFound` compares
  `describeMountEnd` for `SlotTaken`/`StopServer` and `SlotAbsent`/`CloseMount` with the full strings above.
- [ ] Red on the base (no `CasMountLoss.h`); green after. Commit: `Classify a CAS mount slot observation against our two bodies`.

## Task 4: absence needs a LIST

**Files:** `$CA/Pool/CasMountLoss.{h,cpp}`, create `src/Disks/tests/gtest_cas_absence_list.cpp`.
**Produces:** `Observation corroborateAbsence(CasOperation & op, const String & key, Observation seen);`

- [ ] Declaration comment: `/// ProvenAbsent only when a LIST of key as a prefix agrees: a proxy or a
  misrouted request answers a GET with 404 too. A failed LIST, or one that lists the key, is NotObserved;
  any other observation is returned unchanged.` Body: for `ProvenAbsent`, `op.list(key, "", 1, Retry::once())`;
  `key` sorts first among the keys it prefixes, so one entry decides; a listed `key` → `NotObserved`, else
  `ProvenAbsent`. `std::bad_alloc` and `isDeterministicLocalFailure` codes rethrow; any other
  `std::exception` → `NotObserved`.
- [ ] In-memory tests (`CASAbsenceList.*`): passes `Object`/`NotObserved` through; absent key + empty LIST →
  `ProvenAbsent`; a backend whose `read` hides the key but whose `list` shows it → `NotObserved`; a `list`
  that throws `Poco::TimeoutException` → `NotObserved`; a `list` throwing `NOT_IMPLEMENTED` propagates.
- [ ] Wire tests: copy `VerbRecordingServer`, `makeStorage`, `Rig` from
  `git -C /home/mfilimonov/workspace/ClickHouse/cas-95-1 show 71ec8123021:src/Disks/tests/gtest_cas_absence_per_verb.cpp`
  under `#if USE_AWS_S3`, with `Answer` taking the request (method and URI) so a LIST (`GET` with
  `list-type=2` in the query) can be answered apart from the object `GET`. Three tests:
  - `CASAbsenceList.AProxyThatAnswers404ToEverythingProvesNothing`: object `GET` bodyless 404, LIST bodyless
    404 → `op.read("k")` is empty (the `GET` rule is unchanged) and `corroborateAbsence` returns
    `NotObserved`; recorded methods `{"GET", "GET"}`.
  - `CASAbsenceList.A404WithAnEmptyListIsProvenAbsent`: `GET` 404; LIST 200
    `<ListBucketResult …><KeyCount>0</KeyCount><IsTruncated>false</IsTruncated></ListBucketResult>` →
    `ProvenAbsent`, no bucket requirement on the 404 body.
  - `CASAbsenceList.A404ContradictedByTheListIsNotObserved`: LIST 200 with `<Contents><Key>k</Key>…` →
    `NotObserved`.
- [ ] Red on the base (no symbol); green after. Commit: `Prove a CAS mount slot absent only when a LIST agrees`.

## Task 5: the renewer returns its verdict

**Files:** `$CA/Pool/CasServerRoot.{h,cpp}`, `src/Disks/tests/gtest_cas_heartbeat.cpp`.
**Consumes:** Tasks 3–4. **Produces:** `MountRenewResult::slot`, `MountRenewResult::local_fault`.

- [ ] `MountRenewResult` gains `/// For a conflict: what the read after it says about our slot.`
  `std::optional<MountSlotReading> slot;` and `/// This machine failed, not the store.` `bool local_fault = false;`.
  Private `OwnMountBodies ownBodies(const MountLease & in_flight) const` from `server_uuid`, `writer_epoch`,
  `last_confirmed_body`, `last_body`, `last_etag`.
- [ ] `readUntilObserved`: a read that finds nothing returns `corroborateAbsence(op, key, ProvenAbsent{})` unless
  that is `NotObserved`, then it reads again. `renewOn`'s until-definitive `Conflict` arm starts with
  `conflict->seen = corroborateAbsence(op, key, std::move(conflict->seen));`, and its "slot did not move" test
  becomes `classifyMountSlot(conflict->seen, ownBodies(lease_body)).verdict != MountSlotVerdict::Ours` → `break`.
- [ ] After the loop, `Conflict`: corroborate when `!until_definitive`; in a `try`, `result.slot =
  classifyMountSlot(...)`, `result.failure = renewConflictFailure(*result.slot)` (a throw stores the exception,
  `slot` stays empty). `renewConflictFailure` replaces `throwRenewConflict`, keeping its events and exceptions:
  `FencedByGc` → `fenced_by_gc`, `MountFencedException`; `SlotTaken` of our uuid → `superseded`, `ABORTED`
  "was superseded by another incarnation ({holder})"; foreign → `foreign_writer`,
  `CASMountReleaseSkippedForeignOccupant`, today's `ABORTED`; `SlotCorrupt` → `slot_corrupt`, `ABORTED` "holds a
  body of our (uuid, epoch) this server did not write ({holder})" or `CORRUPTED_DATA` "holds bytes that do not
  decode"; `SlotAbsent` → `vanished`, `FILE_DOESNT_EXIST`; `Ours`/`NotObserved` → today's retry-later.
- [ ] The `catch (...)` around the engine calls sets `result.local_fault = getCurrentExceptionCode() !=
  ErrorCodes::MEMORY_LIMIT_EXCEEDED;` (the server memory limit is not a defect of this machine).
- [ ] Tests; `RenewalScriptBackend` gains `uint32_t absent_reads` (while positive a mount-slot read returns
  nothing, as a proxy 404 does) and `Action::ThrowNotImplemented`:
  - `CASHeartbeat.AProxyAbsenceTheListContradictsKeepsReading`: `actions = {RefusePrecondition, Delegate}`,
    `absent_reads = 1` → `renewUntilDefinitive` `Committed`, two attempts with equal bytes and precondition,
    `read_calls == 2`. Red on the base (`FILE_DOESNT_EXIST`).
  - `OurUuidFencedAtAnotherEpochIsTaken` (`writer_epoch + 1`, `gc_fenced` → `SlotTaken`, `ABORTED`) and
    `AnUnknownFencedBodyOfOurPairIsCorrupt` (`gc_fenced`, `seq + 5` → `SlotCorrupt`): red on the base
    (`MountFencedException`).
  - `AnAbsenceTheListConfirmsIsSlotAbsent` (slot removed → `SlotAbsent`, `FILE_DOESNT_EXIST`);
    `AGcFenceOfTheUnclearRenewalIsAFence` (`LandThenThrow`, `cancel_after_write` fences what landed →
    `FencedByGc`); `AnInjectedLocalFailureIsALocalFault` (`ThrowNotImplemented` → `local_fault`). Codes are
    green on the base; the fields are new.
- [ ] Run `CASHeartbeat*:CASMountLoss*:CASAbsenceList*`. Heartbeat tests that expect `MountFencedException` for
  `gc_fenced` at another epoch or an unknown fenced body of our pair go red by design: expect `ABORTED`, nothing
  else. Any other red: stop and report. Commit: `Return the CAS mount slot verdict with a terminal renewal`.

## Task 6: latch, `Stopping`, the graceful stop

**Files:** `$CA/Pool/CasMountRuntime.{h,cpp}`, `$CA/Pool/CasPool.{h,cpp}`,
`$CA/ContentAddressedMetadataStorage.cpp`, create `src/Disks/tests/gtest_cas_mount_stop.cpp`.
**Produces:** `PoolLifecycle::Stopping`; `CasMountRuntime::latchMountEnd`, `mountEnd`, `reactToMountEnd`;
`MountConfig::loss_reaction`, `MountConfig::disk_name`; `PoolConfig::mount_loss_reaction` (default
`StopServer`), `PoolConfig::disk_name` (both appended last, so designated initializers keep compiling);
`Pool::mountEnd`, `Pool::latchMountEndForTest`; `LifecycleSnapshot::end`.

- [ ] Failing tests first (`gtest_cas_mount_stop.cpp`, pools from `DB::Cas::tests::openPoolForTest`, so
  `StopServer` and direct renewals through `renewWatermarkOnce`; helpers `rewriteSlot(backend, key, edit)`,
  `putSlotBytes`, `removeSlot` via `OperationForTest`):

```cpp
TEST(CASMountStop, EachSlotLossLatchesThenStopsThenLogs)
{
    using Damage = std::function<void(DB::Cas::Backend &, const String &)>;
    const auto edit = [](std::function<void(MountLease &)> f) -> Damage { return [f](auto & b, const String & k) { rewriteSlot(b, k, f); }; };
    const std::vector<std::tuple<const char *, Damage, const char *>> cases = {
        {"foreign uuid", edit([](MountLease & m) { m.server_uuid = DB::UInt128{0xF0F0}; }), "slot_taken"},
        {"newer epoch of ours", edit([](MountLease & m) { ++m.writer_epoch; m.write_attempt_id = DB::UInt128{77}; }), "slot_taken"},
        {"our pair, a body we never wrote", edit([](MountLease & m) { m.hostname = "elsewhere"; }), "slot_corrupt"},
        {"undecodable", [](auto & b, const String & k) { putSlotBytes(b, k, "not a mount lease"); }, "slot_corrupt"},
        {"absent", [](auto & b, const String & k) { removeSlot(b, k); }, "slot_absent"},
    };
    for (const auto & [name, damage, word] : cases)
    {
        SCOPED_TRACE(name);
        auto backend = std::make_shared<InMemoryBackend>();
        auto store = DB::Cas::tests::openPoolForTest(backend);
        auto & log = DB::Cas::tests::processStopLog();
        log.reset([&] { return String(store->mayMutate() ? "open/" : "closed/") + String(mountEndWord(store->mountEnd())); });
        DB::Cas::tests::ScopedStopLineCapture lines;
        const uint64_t remounts_before = store->scheduleRemountCallCountForTest();
        damage(*backend, store->layout().mountKey("test"));

        EXPECT_THROW(store->renewWatermarkOnce(), DB::Exception);

        const auto events = log.events();
        ASSERT_EQ(events.size(), 2u);
        EXPECT_EQ(events[0], "graceful:closed/" + String(word)) << "latched before the stop request";
        EXPECT_TRUE(events[1].starts_with("error-line:CAS disk ''") && events[1].contains("server stop requested")) << events[1];
        EXPECT_EQ(store->lifecycleSnapshot().lifecycle, PoolLifecycle::Stopping);
        EXPECT_EQ(store->scheduleRemountCallCountForTest(), remounts_before) << "a loss never remounts";
    }
}
```

  More tests in the same file, each red on the base:
  - `OurOwnFenceStillRemountsAndNothingMore`: GC transform (`fenceOutMount`) → throws, `mountEnd() == None`,
    remount count `+ 1`, `TransientNotLive`, no stop event; `tryRemountOnce()` true (all but the accessor green on the base).
  - `ALatchedMountNeverOpensAgain`: `latchMountEndForTest(SlotTaken)`, then `armMountFence(...)` → `mayMutate()`
    false; a second latch (`SlotAbsent`) returns false; `mountEnd()` stays `SlotTaken`.
  - `TwoDisksAskForOneStop`: pools `a` and `b` on one backend, both slots foreign → `graceful() == 1`, both `Stopping`.
  - `ALossBetweenForgetIntentAndClosedStops`: `publishVanishedIntentForTest()`, slot foreign, renew → stop;
    `forgetDisk({}, "r")` leaves `Stopping`/`slot_taken`. `ForgetClosesWithoutAStop`: `forgetDisk` →
    `VanishedForgotten`, no stop event, a later `latchMountEndForTest(SlotTaken)` returns false.
  - `AStoppingDiskRefusesWritesWithoutARequest`: after the loss `dropRef` throws the retry-later code, no request.
  - `ACloseOnlyMountClosesWithoutAStop`: `.mount_loss_reaction = CloseMount` → `Stopping`, no stop event, the line
    ends `this disk stays closed until restart.`
- [ ] Runtime. `std::atomic<MountEnd> mount_end{MountEnd::None};`.

```cpp
bool CasMountRuntime::latchMountEnd(MountEnd end) noexcept
{
    MountEnd expected = MountEnd::None;
    if (!mount_end.compare_exchange_strong(expected, end, std::memory_order_seq_cst))
        return false;
    /// `seq_cst` with the store and re-check in `armMountFence`: whichever runs second sees the other.
    mount_fence.lost.store(true, std::memory_order_seq_cst);
    fence_generation.fetch_add(1, std::memory_order_acq_rel);
    if (isLossEnd(end))
    {
        lifecycle_since_wall_s.store(wallClockNowSeconds(), std::memory_order_release);
        pool_lifecycle.store(PoolLifecycle::Stopping, std::memory_order_release);
        ProfileEvents::incrementNoTrace(ProfileEvents::CASMountLeaseLost);
    }
    return true;
}
```

  `reactToMountEnd(MountEnd, const std::optional<MountLease> &, std::string_view detail) noexcept`:
  `driver_cv.notify_all()` (no lock: a waiter that misses it wakes on its own timeout or on stop, and the
  ledger may call this with its own locks held); `StopServer` + `endsWithoutShutdown` → `LOG_FATAL` (in
  `try`) then `terminateProcess()`; `StopServer` otherwise → `requestGracefulProcessStop()` then
  `LOG_ERROR` (in `try`); other reactions → `LOG_ERROR` only. The text is `describeMountEnd(...)`.
  `remountTerminal()` also returns true once `mount_end != None`. `armMountFence`: store `lost = false` with
  `seq_cst`, then `if (mount_end.load(std::memory_order_seq_cst) != MountEnd::None)` re-latch `lost` and bump
  the generation. `finishTeardown`: after `stopBackgroundWorkers`, `if (isLossEnd(mount_end)) return;`
  (`/// No farewell: our slot holds another body, or our identity may be held by another process.`).
  `enterVanished`: after the target check, latch `Forgotten`/`Replaced`; when that fails and `mount_end` is
  not already that value, return (first end wins). `enterVanished`'s target `switch` gets `case Stopping:`
  with the other non-terminal values.
- [ ] `renewRenewerOnce`: after the renewer call and before `call.lease->finish`, compute
  `const MountEnd end = active == RenewalDriverState::StartupCall ? MountEnd::None : mountEndOf(result, config.loss_reaction);`
  and `const bool latched = end != MountEnd::None && latchMountEnd(end);`; after `finish` and the hooks,
  `if (latched) reactToMountEnd(end, result.slot ? result.slot->holder : std::nullopt, failureText(result));`.
  `mountEndOf`: non-terminal → `None`; `local_fault` → `LocalDefect`; `slot` verdict `SlotTaken`/
  `SlotAbsent`/`SlotCorrupt` → the same end; `FencedByGc` → `FencedByGc` only for `FailDecommission`
  (`/// The decommission pool impersonates a dead member; a remount would take its slot again.`), else
  `None`; anything else → `None`. `DriverLease::finish`'s terminal branch is skipped when
  `runtime.mount_end != None` (`/// The latch closed the fence; its winner reacts.`).
- [ ] `renewalLoop`'s `catch (...)`: `MEMORY_LIMIT_EXCEEDED` keeps today's body; any other exception →
  `if (latchMountEnd(MountEnd::LocalDefect)) reactToMountEnd(MountEnd::LocalDefect, std::nullopt, getCurrentExceptionMessage(false));`
  then `return` (the `chassert(false)` goes: the fatal stop is the reaction in every build).
- [ ] Pool: `PoolConfig::mountConfig()` projects both new fields; `lifecycleReasonDetail(Stopping)` returns
  `describeMountEnd` of the latched end with empty detail; `throwIfLifecycleTerminal` and
  `ContentAddressedMetadataStorage::checkOpAdmitted` refuse `Stopping` with `throwCasTransientUnavailable`
  (condition `this disk is stopping (<word>); the server is shutting down or the disk stays closed until restart`);
  `casLifecycleWords(Stopping)` → `{"stopping", mountEndWord(ps.end).data()}`; `tryRemountOnce` returns false
  at `arm_fence` when `mountEnd() != None`; `~Pool` phase 2 returns early (`drained` stays false) for a loss
  end. `ContentAddressedMetadataStorage` sets `pool_config.disk_name = disk_name;` and
  `pool_config.mount_loss_reaction = context_available ? StopServer : CloseMount;`.
- [ ] Run the whole `CAS*` gate. Rule for each newly red pre-existing test: it drove R9–R13 and expected a
  remount, `TransientNotLive` or a `vanished` reason → retarget to `Stopping` + one recorded graceful stop
  and no remount; anything else → stop and report the test and its diff. List the retargeted tests in the
  commit body. Commit: `Stop the server when a CAS renewal finds its slot taken, absent or corrupt`.

## Task 7: the fatal class — ledger fault and local defect

**Files:** `$CA/Pool/CasPool.{h,cpp}`; tests `gtest_cas_ref_wedge_every_attempt.cpp`, `gtest_cas_ref_writer.cpp`,
`gtest_cas_ref_recovery_cas_walk.cpp`, `gtest_cas_part_folder_access.cpp`, `gtest_cas_mount_stop.cpp`.

- [ ] Failing tests:
  - `CASRefLaneFault.ShutdownAfterAFaultSendsNothing` (`openPool`, a foreign txn at the next id as in
    `expectAppendSiteFault`): after the fault every backend request increments a counter and throws
    `Poco::TimeoutException`; `store.reset()` → counter 0, the injected retry sleep never called. Red on the base:
    the farewell `PUT` is sent. This is the "shutdown does not hang on a dead CAS disk" test.
  - `CASMountStop.AnInjectedLocalFailureEndsTheProcess`: the mount `PUT` throws `NOT_IMPLEMENTED` → events
    `{"fatal:closed/local_defect", "fatal-line:…"}`, no graceful stop.
  - Retarget, red by design after the change (each test calls `processStopLog().reset()` first):
    `expectOneLatchedFault` asserts `fatal() == 1`, `graceful() == 0`, `mountEnd() == LedgerFault`; every
    `scheduleRemountCallCountForTest() == before + 1` becomes `== before` in `expectAppendSiteFault`,
    `expectWedgeFault`, `expectRecoveryFault`, `NonReadyLaneAtNewIdLatchesBeforeTheReport`,
    `CASRefWriterAppendLane.I1AppendCorruptionSurfacesAndFencesTheMountForRemount`,
    `CASAnomalyPolicy.ForeignBytesAtWedgeKeyTripFenceAndRemount`, `CASAnomalyPolicy.NonReadyAtNewIdAllocationFaultsAndFailsClosed`,
    `CASRefRecoveryCasWalk.RetainedWriterAttemptMeetingASuccessorSealFaultsTheMount`,
    `CASPartFolderAccess.PublishEntriesAbandonsBuildOnPromoteFailure`, plus `fatal() == 1`;
    `RemountIsRequestedWhenTheFaultReportThrows` → `TerminateIsRequestedWhenTheFaultReportThrows`
    (`NOT_IMPLEMENTED` still propagates, `fatal() == 1`).
- [ ] Production: the ledger's latch callback is `[this] { (void)mount_runtime.latchMountEnd(MountEnd::LedgerFault); }`.
  `reportImpossibleInterference` drops `scheduleRemount()` and its `LOG_ERROR`; its first statement is a
  `SCOPE_EXIT` that formats the detail (`ref lane fault for namespace '{}' at '{}': {}`, in a `try`) and calls
  `reactToMountEnd(MountEnd::LedgerFault, std::nullopt, detail)` when `mountEnd() == LedgerFault` (an end that won
  earlier reacted for itself), so the reaction runs even when the hook or the event throws; the hook, the
  `ForeignInterference` event and the diagnostic `GET` stay before it, in that order. Header comment: "The
  reaction to a ref lane fault the ledger has already latched through `latch_mount_fault`: the event, one
  background `GET` for diagnosis, then the end of the mount — for a server's own mount, of the process." The
  hook's comment: "invoked before the event, so a test can make the report throw".
- [ ] Gate `CAS*`; Task 6's rule for any other red. Commit: `End the process on a CAS ref lane fault instead of remounting`.

## Task 8: a twin found by the remount stops the server

**Files:** `$CA/Pool/CasPool.{h,cpp}`, `$CA/Pool/CasMountRuntime.{h,cpp}`, `gtest_cas_mount_stop.cpp`.

- [ ] Failing tests (on the base `tryRemountOnce` returns false and the loop retries forever): after R8 (fence
  the slot, `renewWatermarkOnce` throws), rewrite the slot, then `tryRemountOnce()` is false and:
  `ARemountThatFindsATwinStops` — our uuid, `writer_epoch + 5`, unfenced → `SlotTaken`, one graceful stop;
  `ARemountThatFindsAForeignServerStops` — foreign uuid → `SlotTaken`; `ARemountThatFindsAnUnknownBodyOfTheFencedPairStops`
  — the fenced epoch, unfenced, other hostname → `SlotCorrupt`; `ARemountOverAStoreOutageStillRetries` — the claim
  read throws `Poco::TimeoutException` → `mountEnd() == None`.
- [ ] Production: `CasMountRuntime::fencedIdentity()` = `OwnMountBodies{server_uuid, liveWriterEpoch(),
  confirmedMountBody()}`, taken before `publish_writer_epoch`. At `mount_claim`, `claimMountAwaitingExpiry` goes in a
  `try`; on `ABORTED` or `CORRUPTED_DATA` read the slot once (`Retry::once()`; a throw leaves `NotObserved`),
  `corroborateAbsence`, `if (stopOnRemountObservation(seen, fenced)) return false;`, else `throw;`
  (`/// A newer epoch of ours and undecodable bytes are refusals, not observations; only a read names the loss.`).
  A non-`Claimed` result calls `stopOnRemountObservation(toObservation(claim.seen), fenced)` before today's
  warning. `Pool::stopOnRemountObservation` latches and reacts only for `SlotTaken`/`SlotCorrupt`: today's chain
  re-creates a missing slot until PR 14.
- [ ] Gate `CAS*`. Commit: `Stop the server when a CAS self-remount finds a twin in its slot`.

## Task 9: the decommission pool fails its command

**Files:** `$CA/Pool/CasPool.{h,cpp}` (`openForDecommission`; seam `PoolConfig::admin_opened_hook_for_test`,
`std::function<void(Pool &)>`, empty in production), `$CA/Tools/CasDecommission.cpp`, `gtest_cas_decommission.cpp`.

- [ ] Failing tests: `CASDecommission.ALostClaimFailsTheCommandAndDeletesNothingMore` — three staging objects of
  the victim; the seam (called right after `openForDecommission`) captures the admin `Pool *`; a `DeleteHookBackend`
  (`InMemoryBackend` counting `remove`) latches `admin->latchMountEndForTest(MountEnd::SlotTaken)` at the first
  staging `remove` → `decommissionPoolMember` throws `ABORTED` naming `the decommission claim`, one `remove` reached
  the backend, `graceful() == 0`. Red on the base (all three run). `AnAdminRenewalThatFindsATwinClosesWithoutAStop`:
  a pool with `mount_loss_reaction = FailDecommission`, slot rewritten with our uuid and a newer epoch →
  `renewWatermarkOnce` → `Stopping`, no stop event, the line ends `deletes nothing more.`; GC-fenced instead →
  `mountEnd() == FencedByGc`, no remount.
- [ ] Production: `openForDecommission` sets `mount_loss_reaction = FailDecommission`. `decommissionPoolMember`
  runs the staging, mountpoint and debris drains on
  `requests.admit([pool = std::weak_ptr<Pool>(admin)] { const auto p = pool.lock(); return p && p->mountEnd() == MountEnd::None; })`
  (`/// After the admin claim ends, not one more delete: the victim may be writing again.`) and calls
  `throwIfClaimLost(*admin, victim_srid)` — `ABORTED`, `CAS decommission '{}': the decommission claim was lost
  ({word}); the command stops and deletes nothing more` — before each namespace drop, the drains and `admin.reset()`.
- [ ] Gate `CAS*`. Commit: `Fail CAS pool-member decommission when its claim is lost`.

## Task 10: identity texts and documentation

**Files:** `$CA/Pool/CasServerRoot.cpp`, `src/Disks/tests/gtest_cas_mount.cpp`, `docs/en/antalya/cas/architecture/mounts-and-leases.md`,
`docs/en/antalya/cas/operations/troubleshooting.md`, `docs/en/antalya/cas/operations/migration.md`.

- [ ] Owner-identity refusals (CAS-156.2): foreign owner — "manually delete the owner object '{}' and restart"
  becomes "remove the old member with SYSTEM CAS DROP POOL MEMBER '<srid>' FROM DISK '<disk>' (offline:
  clickhouse-disks cas-drop-member) from a surviving server, then restart"; tombstoned — the same verb for
  re-adding; identity lost over existing data — "+ Recover the owner object from a backup of the bucket, or remove
  the member with SYSTEM CAS DROP POOL MEMBER and start with a new cas_server_root_id." A `gtest_cas_mount.cpp`
  test asserts each substring (red on the base).
- [ ] `mounts-and-leases.md` `## When the mount slot is lost {#slot-lost}`: reasons (`slot_taken`, `slot_absent`,
  `slot_corrupt`, `ledger_fault`, `local_defect`) → what was found → graceful stop or end without shutdown; R8
  still remounts; FORGET closes, the server runs; absence needs a `LIST` that agrees; read-only mounts and
  decommission never stop the server; exit code 0, so `Restart=always` restarts and `Restart=on-failure` does not;
  `clickhouse-local` has no `SIGTERM` handler and ends at once; a byte-identical clone is not detected (CAS-163).
  `troubleshooting.md` `### "server stop requested" {#server-stop-requested}`: the lines and what to check.
  `migration.md`: re-add (victim down, mount object present, `SYSTEM CAS DROP POOL MEMBER`, the harmless
  `slot tombstone failed: … object absent before tombstone write`; the `uuid` file on a persistent volume).
- [ ] Commit: `Name the supported recovery in CAS identity refusals; document the mount stop`.

## Task 11: integration — the server stops, with the watchdog and without

**Files:** `tests/integration/test_cas_mount_renewal_retry/test.py`, `s3_fault_proxy.py`, new
`configs/stop_disks.xml`, `configs/stop_disks_watchdog.xml`.

- [ ] Proxy: `"method_modes": None` in `_DEFAULT_CFG` (method → mode, overriding `modes` for a matched request);
  modes `"404"` (bodyless) and `"412"` (`PreconditionFailed` XML).
- [ ] Disks `disk_cas_stop` (`itest-cas-stop`, `http://s3proxy:11121/test/cas_mount_stop/`) and `disk_cas_stop_wd`
  (`itest-cas-stop-wd`, `…/cas_mount_stop_wd/`), timing of `disk_cas_renewal`; instances `stop_node` and
  `stop_node_watchdog` (`with_rustfs`, `stay_alive`; the second `env_variables={"CLICKHOUSE_WATCHDOG_ENABLE": "1"}`).
- [ ] Tests (deadlines 40 s; `finally`: proxy reset, slot removed, `start_clickhouse`):
  - `test_proxy_404_on_get_and_list_does_not_stop_the_server`: `rate 1.0`, `methods ["GET","PUT"]`,
    `path_substring "itest-cas-stop"`, `method_modes {"PUT": "412", "GET": "404"}` until `/stats`
    `by_mode["404"] >= 2`; reset → pid alive, `lifecycle` back to `live`, `writer_epoch` unchanged, no
    `server stop requested` in the log.
  - `test_a_slot_deleted_by_hand_stops_the_server`: remove the slot on `stop_node` → pid gone, log has `the mount
    slot is absent` and `Received termination signal`; restart → `live`.
  - `test_a_foreign_body_stops_the_server_under_the_watchdog`: precondition `pgrep -c clickhouse` ≥ 2 (else fail:
    no watchdog); replace the `server_uuid` value in the slot's raw bytes with another 32-hex value, `put_object`;
    both processes gone; log has `holds a body of another incarnation`.
- [ ] Run the whole module (`python -m ci.praktika run "integration" --test test_cas_mount_renewal_retry` from `$WT`,
  output to `$WT/build/test_cas_mount_renewal_retry.log`). If the deleted slot does not stop the server, read how
  RustFS answered the renewal `PUT` (`If-Match` on a missing key may be `404`, not `412`) and report it; do not
  weaken the test. Commit: `Test that a lost CAS mount slot stops the server`.

## Task 12: gates and PR

- [ ] `ninja -C $WT/build unit_tests_dbms > $WT/build/build_pr12.log 2>&1`;
  `TMPDIR=$WT/build/tmp $WT/build/src/unit_tests_dbms --gtest_filter='CAS*' > $WT/build/test_cas_pr12.log 2>&1`;
  the same in `build_asan`; a Fast-test (no S3) compile. All green; counts against Setup's.
- [ ] PR to `antalya-26.6` per `.github/PULL_REQUEST_TEMPLATE.md`, category `Improvement`: "A CAS server whose
  mount slot is taken by another incarnation, deleted or overwritten now stops instead of remounting, and a ref
  lane fault ends the process at once; a slot counts as deleted only when a LIST agrees." Mixed versions: format
  and observers unchanged; a peer on the base keeps remounting. Risk: the stop split is provisional.
