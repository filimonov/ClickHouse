---
description: 'Design for the first wave of commits added to the CAS improvements pull request on antalya-26.8: the port of the namespace-life retirement fix (PR 2570) with bounded retirement records (CAS-428), a conflicted catalog erase that re-reads after its pause (CAS-463), a 120 s default mount lease (CAS-405.3), a configurable request window (CAS-405.6), unbounded GC round budgets by default (CAS-29.11), one list request per CAS list page (CAS-29.13), four observability fixes and five CI fixes.'
sidebar_label: 'CAS: improvements bundle, wave 1'
sidebar_position: 113
slug: /superpowers/specs/cas-improvements-wave1-design
title: 'CAS: improvements bundle for antalya-26.8, wave 1'
doc_type: 'reference'
---

# CAS: improvements bundle for antalya-26.8, wave 1 — design {#cas-improvements-wave1}

- Date: 2026-11-12. Status: draft, not approved.
- Tasks: CAS-428, CAS-463, CAS-405.3, CAS-353, CAS-405.6, CAS-29.11, CAS-29.13, CAS-29.14, CAS-1.1, CAS-148,
  CAS-446, CAS-443, CAS-432, CAS-434, CAS-85, CAS-321.2, CAS-466. Port of PR
  [#2570](https://github.com/Altinity/ClickHouse/pull/2570).
- Pull request: [#2831](https://github.com/Altinity/ClickHouse/pull/2831), branch
  `fix/antalya-26.8/cas-improvements-bundle`, base `antalya-26.8`. The wave adds commits to that branch. It
  opens no new pull request.
- Base of the work: `fa1b89d1529`, the head of the late-request audit bundle
  (`docs/superpowers/specs/2026-11-11-cas-pr2-late-request-audit-bundle-design.md`). Work branch:
  `work/antalya-26.8/cas-improvements-wave1`.
- `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`. `T/` = `src/Disks/tests/`.
  `D/` = `docs/en/antalya/cas/`. Line numbers are those of `fa1b89d1529`.
- Drafts: PR 2570 is the series `2a034cdb374..dca5e45ce32` (ten commits). `ad04f95e441` is CAS-443.
  `2a4b5e72aa2` is CAS-432. The other stories have no draft.
- Measurements behind the defaults: `docs/superpowers/reports/2026-11-11-cas-defaults-review.md` on branch
  `cas-297-defaults-review` (`a49f8a8e45f`), called "the study" below.

## 1. Decision {#decision}

- **Nine stories, about 22 commits, added to PR 2831.** Each story is a commit series that reads on its own.
- **Order: A, B, C, D, E, F, G, H, I** ([§2](#order)).

| Story | Tasks | Area | What changes |
|---|---|---|---|
| A | PR 2570 | namespace lives | A cold admission refuses to publish a runtime for a life retired after its catalog read |
| B | CAS-428 | namespace lives | A retirement record exists only while a cold admission of that name is in flight |
| C | CAS-463 | GC catalog drain | A conflicted erase pauses first and then takes its resolution read |
| D | CAS-405.3, CAS-353 | mount lease | Default `cas_mount_lease_ttl_ms` is 120000; the budget log line gets two fields |
| E | CAS-405.6 | request window | The 90 s window becomes a per-plane default behind `cas_request_window_ms`, default 120000 |
| F | CAS-29.11 | GC budgets | Five per-round budgets default to 0, which means unbounded |
| G | CAS-29.13 | listing | A CAS list page costs one `ListObjectsV2` request, down from three |
| H | CAS-29.14, CAS-1.1, CAS-148, CAS-446 | observability | Round summary log line; `NULL` age before the first success; lane state by name; give-up text from facts |
| I | CAS-443, CAS-432, CAS-434, CAS-85, CAS-321.2, CAS-466 | CI | Five CI fixes and one recorded check |

- **Every commit builds and passes the `CAS*` gtest gate.** A test and the product change that turns it green
  share a commit. Red-first evidence is a log ([§12](#commits)).
- **No on-store format change. No model change** ([§16](#models)).
- **One new setting**: `cas_request_window_ms`. **Six default changes**: the mount lease and five GC round
  budgets. Every changed default has a key that restores the old value.
- **One change to shared object-storage code**: an opt-in flag that stops the async list iterator from
  prefetching the next page ([§13](#upstream)). It is its own commit, the last of story G, so it can be
  dropped alone.
- **Not in this wave**: CAS-29.3 (its premise is false at `fa1b89d1529`), the non-aborting drain outcome of
  CAS-463, CAS-321.3, CAS-29.1 and every stand rerun ([§19](#non-goals)).

## 2. Why this order {#order}

The stories are independent except where the list below says otherwise.

- **A before B.** B changes the record that A introduces. A is a port and stays close to the reviewed series.
  B is the only new design on that path, so it is one commit that a reviewer can read against A.
- **C after B.** C and A both touch `CasRefCatalog::deleteCompletedRemovingAtSnapshot`. A adds the
  `before_erase` call at the top of the loop body. C reorders the tail of the loop body.
- **D before E and before H.** The docs row of E and the CAS-446 answer both cite the 94 s write window that
  D creates.
- **E in two commits.** The first moves the constant without changing a value. The second adds the setting
  and the new default.
- **G: lever B, then lever A.** Lever B is inside `CA/`. Lever A touches shared code and is the commit a
  reviewer may ask to drop.
- **I last.** It touches only `ci/`, `tests/` and workflow files.

## 3. Story A: refuse a cold admission of a retired namespace life (PR 2570) {#story-a}

### 3.1 Problem {#story-a-problem}

A cold admission reads the catalog row of a namespace, sees life `L` as `Live`, and later publishes a runtime
for `L` under `ref_queue_mutex`. Between the read and the publish, the table can be dropped and GC can erase
the row of `L`. `CasRefLedger::invalidateRemovedCatalogLife` then finds no resident runtime and does nothing.
The admission publishes a runtime for a life that no longer exists:

- a reader serves the ref table of a removed namespace;
- a writer appends to a life whose objects GC deletes.

Three more gaps on the same path at `fa1b89d1529`:

- GC calls `invalidateRemovedCatalogLife` after the drain returns (`CA/Gc/CasGc.cpp:841`, `:4643`). A reader
  released right after the erase and before that loop is not refused. A drain that throws never runs the loop.
- A runtime whose removal is applied is evicted without a trace when a remote GC leader erased the row.
- `CasRefLedger::namespaceLife` trusts a recovered life whose removal is already terminal.

### 3.2 Change {#story-a-change}

The change is the combined diff of `2a034cdb374..dca5e45ce32`. It applies to `fa1b89d1529` without conflicts
in ten files: `CA/Gc/CasGc.cpp`, `CA/Gc/CatalogLifecycleReconciler.{cpp,h}`, `CA/Pool/CasPool.h`,
`CA/Pool/CasRefCatalog.{cpp,h}`, `CA/Pool/CasRefLedger.{cpp,h}`, `T/gtest_cas_gc_frontier_gate.cpp`,
`T/gtest_cas_ns_file_incarnation.cpp`. It lands as three commits.

**A1. Retirement count and admission check.**

- `struct RefNameSlot { RefTableRuntimePtr current; uint64_t retirements = 0; }`. The map `ref_name_slots`
  is keyed by namespace name and guarded by `ref_queue_mutex`.
- `invalidateRemovedCatalogLife` increments `retirements` of the name before it looks for a runtime.
- `uint64_t refNameRetirements(const RootNamespace &) const` reads the count. Five admission sites capture it
  before their catalog read:

| Site | Refusal kind |
|---|---|
| `acquireReadableRefTableRuntime` | Read |
| `acquireMutableRefTableRuntime` | Write |
| `namespaceLife`, cold path | Write |
| `namespaceStillLogicallyPresent` | Read |
| `dropNamespaceImpl` | Write |

- `acquireRefTableRuntime(life, admitted_generation, retirements_seen, RuntimeAdmission admission)` compares
  the count under `ref_queue_mutex`. A moved count refuses the admission.
- Refusal: `Read` throws through `throwCasTransientUnavailable`, `Write` through `throwCasWriteRetryLater`.
  Text: "a life of this name was retired while life {} was being admitted; retry from a fresh catalog
  observation". Both are retryable. The retry reads the catalog again.
- `refuseTerminalLife` makes `namespaceLife` refuse a recovered life whose removal is terminal.
- Test accessors: `refNameRetirementsForTest` on the ledger and the pool.

**A2. Announce the retirement before the erase.**

- `CasRefCatalog::deleteCompletedRemovingAtSnapshot` gets a last parameter
  `const std::function<void(const NamespaceLifeId &)> & before_erase = {}`. It is called once per life, after
  admission is confirmed and before the first erase write. A retry of the erase does not call it again. If it
  throws, no erase is sent.
- `CatalogLifecycleReconciler::reconcile(refresh_authority, before_erase)`: the second argument is required.
- `Gc::drainCompletedRemoving` passes a callback that calls `invalidateRemovedCatalogLife`.
- The two post-drain loops in `CA/Gc/CasGc.cpp` go away.

**A3. Record the retirement at eviction.**

- The eviction of a runtime with `removal_admission_closed` or an applied removal increments `retirements`
  of its name. The slot is erased only when its count is zero.

### 3.3 Tests {#story-a-tests}

All tests come from the series. Red runs are taken against `fa1b89d1529` with only the test files applied.

| # | Commit | Test | Fixture | Red before |
|---|---|---|---|---|
| A-1 | A1 | `CASNsFileIncarnation.ColdReaderDoesNotRepublishALifeRetiredAfterItsCatalogRead` | `CatalogReadRaceBackend`, `publishWithProductionBirth` | the parked reader gets a runtime |
| A-2 | A1 | `CASNsFileIncarnation.ColdWriterDoesNotRepublishALifeRetiredAfterItsCatalogRead` | same | the parked writer gets a runtime |
| A-3 | A1 | `CASNsFileIncarnation.RetirementWithASuccessorResidentIsRecordedAndSurvivesEviction` | `PoolConfig::ref_table_cache_bytes = 1` | no count exists |
| A-4 | A1 | `CASNsFileIncarnation.NamespaceLifeRefusesARecoveredTerminalLife` | `openPoolForTest` | the life is returned |
| A-5 | A2 | `CASCatalogLifecycleReconciler.AnnouncesEachLifeBeforeItsRowIsErased` | `DrainRaceBackend`, journal | no announcement exists |
| A-6 | A2 | `CASCatalogLifecycleReconciler.AFailingAnnouncementSendsNoErase` | same | no announcement exists |
| A-7 | A2 | `CASNsFileIncarnation.ColdReaderIsRefusedWhenReleasedRightAfterTheDrainErasesItsRow` | `ParkedAdmissionBackend`, `ParkedReader` | the reader gets a runtime |
| A-8 | A3 | `CASNsFileIncarnation.ColdAdmissionIsRefusedWhenARemoteLeaderRetiredTheLifeAndTheRuntimeWasEvicted` | `ParkedAdmissionBackend`, `ref_table_cache_bytes = 1` | the admission gets a runtime |

- A-1 to A-4 are green at A1 because the post-drain loops of `fa1b89d1529` still bump the count.
- The seven existing call sites of `reconcile` in `T/gtest_cas_gc_frontier_gate.cpp` pass a no-op second
  argument. The five test callers of `deleteCompletedRemovingAtSnapshot` in `T/gtest_cas_ref_catalog.cpp`
  compile unchanged.
- Parked-thread tests use the bounded waits of the series (`dca5e45ce32`): a test fails, it does not hang.

### 3.4 Not closed by story A {#story-a-not-closed}

- A slot with a non-zero count is never erased, so the map grows by one entry per retired name. Story B
  closes this.
- `acquireMutableRefTableRuntime` captures the count on every call, also when the runtime is resident. Its
  own `namespaceLife` call can bump the count through a catalog reconcile. The caller then gets a retryable
  refusal. The series accepts this.

## 4. Story B: bound the retirement records (CAS-428) {#story-b}

### 4.1 Problem {#story-b-problem}

After story A, a server keeps one `RefNameSlot` for every namespace name that was ever retired on it. A
workload that creates and drops tables grows `ref_name_slots` without bound until a remount clears the map.
A record is needed only while a cold admission of that name is between its capture and its publish. A later
admission reads the catalog after the retirement and needs no record.

### 4.2 Change {#story-b-change}

Files: `CA/Pool/CasRefLedger.{h,cpp}`, `CA/Pool/CasPool.h`, `T/gtest_cas_ns_file_incarnation.cpp`.

**Invariant.** A slot exists if and only if it has a `current` runtime or `admissions_in_flight > 0`.

- `RefNameSlot` gets `uint32_t admissions_in_flight = 0`. The ledger gets `uint64_t ref_name_slots_epoch`,
  guarded by `ref_queue_mutex`.
- `ColdAdmission beginColdAdmission(const RootNamespace & ns)` replaces `refNameRetirements(ns)` at the five
  capture sites. Under `ref_queue_mutex` it creates the slot if absent, increments `admissions_in_flight`,
  and stores the name, `retirements` and the epoch in the guard.
- `ColdAdmission` is move-only. Its destructor releases the count under `ref_queue_mutex` and erases the slot
  when the invariant no longer holds. It covers every path that leaves an admission without a publish: an
  absent or non-`Live` row, a resident runtime, an exception.
- A `ColdAdmission` is declared before any lock of its function. It is never destroyed while
  `ref_queue_mutex` or a runtime `state_mutex` is held.
- `acquireRefTableRuntime(life, admitted_generation, ColdAdmission && admission, RuntimeAdmission kind)`
  consumes the guard inside its critical section: compare the count, decrement `admissions_in_flight`,
  publish or refuse, erase the slot if it has neither a runtime nor an admission in flight. The guard is
  empty afterwards and its destructor does nothing.
- `invalidateRemovedCatalogLife` uses `find`. An absent slot gets no record. A present slot gets
  `++retirements`. After `current` is reset, the slot is erased when `admissions_in_flight == 0`.
- Eviction increments `retirements` as in A3 and erases the slot when `admissions_in_flight == 0`.
- `quiesceRefTablesForRemount` clears the map and increments `ref_name_slots_epoch`. A guard from an older
  epoch releases nothing. An admission that presents such a guard is refused with the generation-moved
  error, as a remount already does.
- Two captures on one name by one call chain (`acquireMutableRefTableRuntime` and then `namespaceLife`)
  count as two.
- `refNameRetirements` stays as the body of `refNameRetirementsForTest`. New accessor
  `refNameSlotCountForTest` on the ledger and the pool returns `ref_name_slots.size()`.
- The comment on `RefNameSlot::retirements` states the invariant above.

**Why the epoch.** Without it, an admission that captured count 0 before a remount compares against an absent
slot, which also reads as 0. The generation check already refuses this admission. The epoch makes the record
logic safe without relying on that check.

### 4.3 Tests {#story-b-tests}

All in `T/gtest_cas_ns_file_incarnation.cpp`, in this order.

| # | Test | Fixture | Red before |
|---|---|---|---|
| B-1 | `CASNsFileIncarnation.RetirementRecordsDoNotAccumulateAcrossCreateDropCycles` | `openPoolForTest`, `gc_fold_max_defer_rounds = 0`, `runRegularRoundReclaiming`, 50 cycles of create, drop, GC round | slot count is 50, expected 0 |
| B-2 | `CASNsFileIncarnation.RemoteLeaderRetirementsLeaveNoRecordWithoutAnAdmission` | two pools on one backend; the second pool leads GC | slot count is 1, expected 0 |
| B-3 | `CASNsFileIncarnation.ColdAdmissionInFlightKeepsTheRecordAndReleasesIt` | `ParkedAdmissionBackend` | slot count after the refused admission is 1, expected 0 |
| B-4 | `CASNsFileIncarnation.AbortedColdAdmissionsReleaseTheirInFlightCount` | `openPoolForTest`; one case per abort path of [§4.2](#story-b-change) | accessor missing; after the accessor: pin |
| B-5 | `CASNsFileIncarnation.ColdAdmissionOfAPredecessorIsRefusedAfterItsSuccessorIsEvicted` | `ParkedAdmissionBackend`, `ref_table_cache_bytes = 1` | pin: green before and after |

- B-3: while the admission is parked, the slot count is 1 and the retirement is recorded. After the release
  the admission is refused and the slot count is 0.
- B-4 cases: reader on an absent row; reader on a `Removing` row; writer whose runtime is resident;
  `namespaceStillLogicallyPresent` on an absent row; `dropNamespaceImpl` on a missing row and on an
  `expected_incarnation` mismatch; an admission that throws from its catalog read.
- B-5: an admission of predecessor `L1` parks after its catalog read. The table is dropped, GC erases `L1`,
  successor `L2` is published and then evicted. The released admission is refused. The slot count is 0.
- A-3 is renamed to `RetirementWithASuccessorResidentIsRecorded`. Its tail asserted that eviction keeps a
  slot with a recorded retirement. It now asserts that the slot is gone after the eviction. B-5 keeps the
  behaviour that the tail stood for.
- A-8 gets one more assertion after the join: the slot count is 0.
- The `CASNsFileIncarnation` suite runs under TSan-style interleavings only through its parked fixtures. The
  ASan gate covers the guard's lifetime.

### 4.4 Not closed by story B {#story-b-not-closed}

- `acquireMutableRefTableRuntime` now takes `ref_queue_mutex` twice per call when the runtime is resident:
  once to capture and once to release. The critical sections are a map lookup each ([§20](#risks)).

## 5. Story C: a conflicted catalog erase re-reads after its pause (CAS-463) {#story-c}

### 5.1 Problem {#story-c-problem}

`CasRefCatalog::deleteCompletedRemovingAtSnapshot` (`CA/Pool/CasRefCatalog.cpp:483-593`) erases one
`Removing` row with a conditional write. Its loop at `fa1b89d1529`:

1. conditional erase write against the snapshot's incarnation;
2. resolution read, which is also the snapshot of the next attempt;
3. on `Conflict`: pause `Retry::backoff(attempt + 1)`, growing to 5 s;
4. next erase write, against the snapshot read **before** the pause.

Any catalog write that lands during the pause makes the next erase conflict. The pause grows with every
loss, so the chance to lose grows too. Under steady DDL the loop runs out its 90 s request window or its 100
attempts and throws `NETWORK_ERROR`. The GC round ends as `Aborted` and the fold does not run. This is the
ddl16 stand pattern of CAS-463.

Second defect: `Gc::runRegularRound` sets the `deleted` metric of phase `pre_fold_ref_drain` only after
`drainCompletedRemoving` returns (`CA/Gc/CasGc.cpp:845`). A drain that erased rows and then threw reports
nothing.

### 5.2 Change {#story-c-change}

**C1. Pause before the resolution read.** File `CA/Pool/CasRefCatalog.cpp`. New loop body after the erase
write:

1. if the erase returned `Conflict`: `op.pause(Retry::backoff(attempt + 1))`;
2. admission check (unchanged, now after the pause);
3. resolution read (unchanged);
4. outcome evaluation (unchanged);
5. no pause at the end of the loop.

- The pause schedule does not change: the same full-jitter backoff, growing to 5 s. Only its place changes.
- The next erase write is conditioned on a read taken after the pause. The window in which another writer
  can invalidate it shrinks from the pause plus one `refresh_authority` call to that call alone.
- Rejected: the flat `Retry::conflictBackoff` (at most 200 ms) that `casUpdateImpl` uses. It reaches the
  100-attempt cap after about 10 s. The give-up of a permanently contended catalog would move from the
  request deadline to the attempt cap, which
  `CASRefCatalogRemoval.PerpetualConflictGivesUpWithinOneWindowNotAtTheAttemptCap` forbids.
- A non-`Conflict` outcome pauses nowhere, as before: it is terminal for the call.
- The request count per attempt does not change: one write and one read.
- The comment block at `:518-523` and the pause comment at `:582-585` are rewritten to match.
- Docs: `D/architecture/garbage-collection.md` (drain section, `:162-182`) states the order of pause and
  read.

**C2. Report the erases of a drain that throws.** Files `CA/Gc/CasGc.cpp`,
`CA/Gc/CatalogLifecycleReconciler.{h,cpp}`.

- The reconciler counts completed erases in a place the caller can read after an exception.
- `Gc::runRegularRound` and `Gc::rebuildBaseline` set the `deleted` metric from that count on every exit of
  the phase scope. `GcPhaseTimer` already emits its record from the destructor.
- The mechanism is left to the builder ([§21](#left-to-builder)). No new exception type and no change to
  what the drain throws.
- Docs: `docs/en/operations/system-tables/cas_gc_log.md` (`:75`) says that `deleted` is filled for an
  aborted drain.

### 5.3 Tests {#story-c-tests}

| # | Commit | Test | File, fixture | Red before |
|---|---|---|---|---|
| C-1 | C1 | `CASRefCatalogRemoval.AWriterLandingDuringEveryBackoffCannotStarveTheErase` | `T/gtest_cas_ref_catalog.cpp`; `WriteCountingBackend`, `FakeClock` with `sleepFn` wrapped to commit one external catalog write per pause | the loop runs to the 90 s fake deadline and throws code 210 |
| C-2 | C1 | `CASRefCatalogRemoval.AConflictedEraseSendsTheNextWriteFromAReadTakenAfterThePause` | same file; `WriteCountingBackend::journalFor`, `refuseNextWrite` | journal order is read, pause, write; expected write, pause, read, write |
| C-3 | C2 | `CASGCFrontierGate.AnAbortedDrainReportsTheRowsItErased` | `T/gtest_cas_gc_frontier_gate.cpp`; `DrainRaceBackend`, two eligible rows, the second erase refused on every attempt | the phase record has no `deleted` key; expected `deleted = 1` |

- C-1 green: outcome `Deleted` on the second erase write, exactly one pause.
- C-1 and C-2 refuse the first erase write with `refuseNextWrite`, as
  `CASRefCatalogRemoval.AConflictingEraseBacksOffBeforeItsRetry` (`T/gtest_cas_ref_catalog.cpp:1466`) does.
- Pins that must stay green unchanged:
  `CASCatalogLifecycleReconciler.RetriesFromTheMandatoryConflictResolutionCut`
  (`T/gtest_cas_gc_frontier_gate.cpp:3086`, four GETs), the tests at `T/gtest_cas_ref_catalog.cpp:1466`,
  `:1506` and `:1730`, and at `T/gtest_cas_gc_frontier_gate.cpp:3121` and `:3159`.

### 5.4 Not closed by story C {#story-c-not-closed}

- **A drain that still loses every race aborts the round** (CAS-463 AC #2). A non-aborting outcome lets the
  fold run with a `Removing` row left behind. That contradicts `DrainBeforeDecision` in
  `docs/superpowers/models/CaRefPreFoldDrainCore.tla`. It needs a model configuration first and gets its own
  task ([§19](#non-goals)).
- **Stand criterion** (CAS-463 AC #1, ddl16 rounds succeed) is not a unit test. C-1 is its stand-in.
- No `ProfileEvent` counts erase attempts. None is added.

## 6. Story D: default mount lease of 120 s (CAS-405.3, CAS-353) {#story-d}

### 6.1 Problem {#story-d-problem}

Defaults at `fa1b89d1529`: `cas_mount_lease_ttl_ms = 30000`, `cas_mount_renew_period_ms = 10000`. A
conditional write is admitted only while `TTL - period - 2 × envelope - margin` of slack remains. The
attempt envelope is 7000 ms and the safety margin 2000 ms, so the write reserve is 16000 ms and the slack is
4000 ms. One renewal `PUT` that takes longer than 4 s refuses every conditional write on the mount:

- CAS-353: a 6.5 s S3 stall on PR 2513 failed a `CREATE TABLE` and sent two system-log ref tables to
  `NEEDS RECOVERY`.
- The study: every `PUT` delayed 6 s for 60 s gives 58 `INSERT` errors at 30/10 and none at 120/10. A 90 s
  storage hang gives 578 errors at 30/10 and 4 at 120/10.

### 6.2 Change {#story-d-change}

**D1. Default.**

- `CA/ContentAddressedSettings.cpp:77`: `mount_lease_ttl_ms` 30000 → 120000. The renew period stays 10000.
  The slack becomes 94000 ms.
- `PoolConfig` (`CA/Pool/CasPool.h:225-226`) and `CasMountRuntime` (`CA/Pool/CasMountRuntime.h:70-71`) keep
  30000/10000. Their comments say that these are the values of a pool opened without disk settings, which is
  how unit tests open one.
- Integration tests that kill a server and wait out its lease set a short lease in their own configs:
  `test_cas_shared_pool`, `test_cas_drop_pool_member`, `test_cas_gc_sharded`, `test_cas_early_commit_restart`.
  Values: the ones the tests ran with before, 30000/10000. Stateless CAS lanes keep the new default.
- Docs: `D/configuration.md` (`:100`, `:101`, `:111`, `:127-134`), `D/architecture/mounts-and-leases.md`
  (`:145-146`, `:203`), `D/architecture/garbage-collection.md` (`:196-197`), `D/operations/migration.md`
  (`:214`), `D/operations/troubleshooting.md` (`:25-26`). They state the costs below and the key that
  restores 30 s.

Costs of the longer lease, measured in the study:

| Effect | 30 s lease | 120 s lease |
|---|---|---|
| Restart wait of a server that died without releasing its mount | 43.0 s | 138.1 s |
| GC fence of a dead mount | 102.6 s | 223.9 s |
| `system.cas_mounts` shows `expired` after (`ttl / 2` skew margin) | 45 s | 180 s |

`cas_unsafe_remount_no_delay` still skips the restart wait.

**D2. Budget log line (CAS-353 AC #2).**

- The `LOG_INFO` at the end of `validateCasRequestBudget` (`CA/Backend/CasRequestBudget.cpp:65-68`) gets
  `write_reserve_ms` (`2 × envelope + margin`) and `renewal_slack_ms` (`TTL - period - write reserve`).
  The slack is printed only when background renewal is on, where the cadence check has proven it positive.
- Docs: `D/architecture/mounts-and-leases.md` names the two fields and the relation.

### 6.3 Tests {#story-d-tests}

| # | Commit | Test | File, fixture | Red before |
|---|---|---|---|---|
| D-1 | D1 | `CASContentAddressedSettings.MountLeaseDefaultsSatisfyRequestBudget` | `T/gtest_cas_settings.cpp`; default settings object | TTL is 30000, expected 120000 |
| D-2 | D2 | `CASRequestBudget.BudgetLogLineNamesReserveAndSlack` | `T/gtest_cas_requests.cpp`, next to `EnvelopeIsValidatedNotTheBareAttempt` (`:3301`); `Poco::StreamChannel` capture as in `T/gtest_cas_pool.cpp:2378-2427` | the two keys are absent |

- D-1 also asserts that `validateCasRequestBudget` accepts the defaults and that the slack is 94000 ms.
- Unit tests that hard-code 30000/10000 through `PoolConfig` stay valid and unchanged.
- The four integration modules above run in the gates ([§14](#gates)).

### 6.4 Not closed by story D {#story-d-not-closed}

- CAS-353 AC #1 for a renewal `PUT` slower than 94 s. The request window bounds a `PUT` well below that.
- CAS-405.3 AC #2, the stand rerun at the new default.
- A renew period other than 10 s. At 120/40 the same 90 s hang gives 39 errors, at 120/20 it gives 2.

## 7. Story E: configurable request window (CAS-405.6) {#story-e}

### 7.1 Problem {#story-e-problem}

`Retry::standard`, `Retry::once` and the default of `Retry::untilLeaseSafe` carry a literal 90000 ms window
(`CA/Backend/CasRetry.h:57-58`, `:65`, `:72`). It cannot be configured. After story D the lease allows a
write for 94 s after a due renewal, so the window is the tighter bound. In the study a 90 s storage hang at
120/10 gives 4 errors, all from this window.

### 7.2 Change {#story-e-change}

**E1. The window becomes a plane default. No value changes.**

- `Retry::window_ms` (`CA/Backend/CasRetry.h:18`) becomes `std::optional<uint64_t>`. An empty value means
  "the default of the request plane". `standard`, `once` and the default argument of `untilLeaseSafe` leave
  it empty. `within(ms)` and `untilDefinitive` keep explicit values.
- `Retry::bind` (`CA/Backend/CasRetry.cpp:36-47`) gets a required parameter `default_window_ms`. There is no
  overload without it, so the compiler finds every call site.
- `CasRequests` (constructor at `CA/Backend/CasRequests.h:202`) gets `default_window_ms` and a private
  `bindPolicy(const Retry &)`. It replaces the 14 `policy.bind(owner.now_ms())` calls in
  `CA/Backend/CasRequests.cpp`, the one in `CasOperation::freeze` and the one in `CA/Backend/CasHotKeys.cpp:67`.
- `CasRequestBudget` (`CA/Backend/CasRequestBudget.h:15-47`) gets `request_window_ms` with default
  `kDefaultRequestWindowMs = 90000`.
- Every `CasRequests` gets the value from the budget of its pool: `mount_requests`, `lease_requests`,
  `gc_requests` (`CA/Pool/CasPool.cpp:191-222`), the bootstrap planes (`:425`, `:1016`) and the
  decommission planes (`CA/Tools/CasDecommission.cpp:495`, `:520`).

**E2. Setting and new default.**

- `DECLARE(UInt64, request_window_ms, 120000, ...)` in `CA/ContentAddressedSettings.cpp`. User key
  `cas_request_window_ms`. Read at mount, copied into the budget next to the other budget fields
  (`CA/ContentAddressedMetadataStorage.cpp:808-812`). `kDefaultRequestWindowMs` becomes 120000.
- `validateCasRequestBudget` throws `BAD_ARGUMENTS` when the window is not larger than one attempt envelope.
- `recovery_retry_budget_ms` (`CA/Backend/CasRequestBudget.h:44`) has no setting and gets no validation.
  The storage layer sets it to `2 × request_window_ms` at the same copy site. The struct default goes from
  120000 to 240000. Unit tests set it to 0, 1 and 5000 through `PoolConfig`, and `Pool::open` runs the
  validation, so a check against the window would refuse them.
- Why 120000: it exceeds the 90 s hang of the study plus one envelope (7000) plus one more attempt's
  timeout (5000).
- Docs: a row in `D/configuration.md` near `:111-112`; `D/architecture/blob-protocol.md:78`. They state that
  a failing request now gives up after 120 s, and the relation to the recovery budget (CAS-405.6 AC #3).

### 7.3 Tests {#story-e-tests}

| # | Commit | Test | File, fixture | Red before |
|---|---|---|---|---|
| E-1 | E1 | `CASRequests.StandardPolicyUsesThePlaneRequestWindow` | `T/gtest_cas_requests.cpp`; a `CasRequests` with plane window 7000, `FakeClock` | does not compile: no plane window exists |
| E-2 | E2 | `CASContentAddressedSettings.RequestWindowDefaultAndValidation` | `T/gtest_cas_settings.cpp` | the setting does not exist |
| E-3 | E2 | `CASPartWriteTxn.AGiveUpHonoursTheConfiguredRequestWindow` | `T/gtest_cas_part_write.cpp`; pool with window 5000, shape of `AGiveUpReportsHowManyAttemptsItSent` (`:898-917`) | gives up after 90 s of fake time, expected 5 s |

- E-1 cases: `standard` gives up at 7000 ms; `once` sends one attempt; `within(1000)` keeps 1000;
  `untilLeaseSafe` uses the plane window when it is the tighter bound.
- E-2 cases: default 120000; a window equal to the envelope is refused by `validateCasRequestBudget`; the
  default `CasRequestBudget` carries a recovery budget of 240000.
- Existing tests updated in E1 for the new `bind` signature and the optional field:
  `T/gtest_cas_requests.cpp:142-144`, `:634`; `T/gtest_cas_ref_catalog.cpp:1144`, `:2562`.
- Tests that set `recovery_retry_budget_ms` explicitly stay unchanged: `T/gtest_cas_ref_recovery_cas_walk.cpp`,
  `T/gtest_cas_ref_wedge_every_attempt.cpp`, `T/gtest_cas_ref_writer.cpp`.
- The derive at the storage layer has no unit seam. The stateless CAS lanes exercise it; the docs state the
  relation (CAS-405.6 AC #3).
- E1 is a refactor: the `CAS*` gate before and after is its evidence. E-1 is the one new test.

### 7.4 Not closed by story E {#story-e-not-closed}

- CAS-405.6 AC #2, the stand rerun with a 90 s hang.
- GC and the offline tools use `Retry::standard` too. Their worst case grows by 30 s ([§20](#risks)).

## 8. Story F: GC round budgets default to unbounded (CAS-29.11, reduced) {#story-f}

### 8.1 Problem {#story-f-problem}

Five per-round budgets default to 5000 entries. On a store with a backlog a round stops at the cap and
carries the rest to the next round. The backlog drains at 5000 entries per round per budget, however fast the
round is. Study runs, unreachable objects left in the pool, with the three work budgets (graduation,
redelete, ref cleanup) at 5000 and at 0:

| Run pair | Budgets 5000 | Budgets 0 |
|---|---|---|
| `gcD_M_i300` and `gcD_M_i300_b0`, GC interval 300 s | 25240 (round 117 s) | 12226 (round 46 s) |
| `mt1000_base` and `mt1000_b0`, 1000 tables, one run each | 74277 | 20372 |
| `gcH_i900` and `gcH_i900_b0`, load above GC capacity | 109584 | 107309 |

Where GC has capacity the caps leave two to four times the backlog. Above capacity they change nothing.

### 8.2 Change {#story-f-change}

- Five `DECLARE` defaults in `CA/ContentAddressedSettings.cpp` (`:69`, `:70`, `:73`, `:75`, `:76`) go from
  5000 to 0: `gc_round_graduation_budget`, `gc_round_redelete_budget`, `gc_round_ref_cleanup_budget`,
  `gc_round_handoff_prefix_wholesale_budget`, `gc_round_outcome_entry_budget`.
- `PoolConfig` already defaults four of them to 0 (`CA/Pool/CasPool.h:105-106`, `:130`, `:137`). Only
  `PoolConfig::gc_round_ref_cleanup_budget` (`:120`) goes from 5000 to 0.
- The value 0 already means "no limit" at every consumer (`CA/Gc/CasBlobInDegree.h:286-352`).
- A round has no time bound in this wave. The round deadline is CAS-29.10 ([§19](#non-goals)).
- The keys stay. A deployment that set one keeps its cap.
- Unchanged: `gc_round_sweep_namespace_budget` (20), `sweep_recovery_op_budget` (5000),
  `prefix_wholesale_budget` (20000).
- Docs: `D/configuration.md` (`:217`, `:224`, `:225`, `:228`, `:230`, `:231`) and
  `D/architecture/garbage-collection.md`
  (`:397`, `:579`, `:815-820`, `:945`): new defaults, the meaning of 0, and how to restore a cap.

### 8.3 Tests {#story-f-tests}

| # | Test | File, fixture | Red before |
|---|---|---|---|
| F-1 | `CASContentAddressedSettings.GcRoundBudgetsDefaultToUnbounded` | `T/gtest_cas_settings.cpp`; default settings object and default `PoolConfig` | five values are 5000, expected 0 |
| F-2 | `CASRefGc.ZeroCleanupBudgetProcessesEveryEntryInOneRound` | `T/gtest_cas_ref_gc.cpp`; the budget fixture near `:719`, ten entries, one round with budget 3 and one with budget 0 | pin: green before and after |

- F-1 also pins the three kept values: 20, 5000 and 20000.
- F-2: budget 3 leaves seven entries after one round. Budget 0 leaves none.

### 8.4 Not closed by story F {#story-f-not-closed}

- CAS-29.11 AC #1 asked to remove the keys and warn on use. The keys stay, so there is nothing to warn
  about. The criterion is superseded.
- CAS-29.11 AC #2 (the `entries_redeleted` sum past 5000 entries) has no test of its own. F-2 pins the
  meaning of 0 on the ref-cleanup budget only.
- The first round after an upgrade on a backlogged pool is long: it drains the whole backlog. The dead-member
  fence is evaluated at round start, so it waits for that round ([§20](#risks)).
- `gc_round_outcome_entry_budget` and `gc_round_handoff_prefix_wholesale_budget` at 0 have no study run.

## 9. Story G: one list request per CAS list page (CAS-29.13) {#story-g}

### 9.1 Problem {#story-g-problem}

`ObjectStorageBackend::listUnder` (`CA/Backend/CasObjectStorageBackend.cpp:1064-1163`) pays three
`ListObjectsV2` requests for a full page of 1000 keys:

1. the batch that holds the page (S3 caps `MaxKeys` at 1000);
2. a second batch for the one sentinel key that proves the prefix continues (`:1155-1159`);
3. the prefetch of the batch after that. `IObjectStorageIteratorAsync::nextBatch` schedules it after every
   batch (`ObjectStorageIteratorAsync.cpp:70-71`), and `~S3IteratorAsync` waits for it
   (`S3ObjectStorage.cpp:221-226`), so it is paid even when the caller stops.

Measured on RustFS: 67 requests for 21 pages of a 20000-key prefix
(`docs/superpowers/reports/2026-09-30-cas-gc-request-cuts/06-list-requests-per-page.md`). The GC ref walk
and the janitor list with pages of 1000 keys, so two of three list requests of a round buy nothing.

### 9.2 Change {#story-g-change}

**G1. Lever B: the sentinel fits the batch.** Files `CA/Backend/CasRequests.h`, `CA/Gc/CasGc.cpp`.

- One named constant, `kCasListPageKeys = 999`, next to `forEachListedKey` (`CA/Backend/CasRequests.h:323`).
  Its comment gives the reason: the store returns at most 1000 keys per request, and the page needs one key
  more than its size to prove that the prefix continues.
- It replaces the default `page_limit = 1000` of `forEachListedKey` and both `kListPageLimit = 1000`
  constants in `CA/Gc/CasGc.cpp` (`:4155`, `:4391`).
- `listUnder` does not change. A first page asks for `limit + 1 = 1000` keys. A resumed page gets the
  store's own batch of 1000. Both hold the 999 keys and the sentinel.
- The sentinel probe keeps its page of 32 (`CA/Backend/CasSentinelProbe.cpp:70`).
- Cost after G1: two requests per full page, the batch and the prefetch.

**G2. Lever A: no prefetch for a caller that reads one page.** Shared code, opt-in, default unchanged
([§13](#upstream)).

- `ObjectStorageControlRequest` (`src/IO/ObjectStorageRequestProfile.h`) gets `bool prefetch_next_page = true`.
- `IObjectStorageIteratorAsync` gets a protected `bool prefetch_next_batch = true`. `nextBatch` schedules
  the next batch after a batch only when the flag is set. When it is not set, the batch is scheduled when
  the consumer asks for it.
- `S3IteratorAsync` takes the flag from the request. `S3ObjectStorage::iterate` passes it.
- `ObjectStorageBackend::listUnder` sets `prefetch_next_page = false` on the request it hands to `iterate`
  (`CA/Backend/CasObjectStorageBackend.cpp:1061`). Other CAS requests do not list.
- Cost after G2: one request per page. A 10000-key prefix costs 11 requests (CAS-29.13 AC #1).
- Other async iterators (Azure) ignore the flag and keep prefetching.

### 9.3 Tests {#story-g-tests}

| # | Commit | Test | File, fixture | Red before |
|---|---|---|---|---|
| G-1 | G1 | `CASBackend.AListWalkOfTenThousandKeysStaysWithinItsRequestBound` | `T/gtest_cas_s3_single_attempt_client.cpp`; the local HTTP server of that file (`:~532`) extended to page a synthetic key list by `continuation-token` and `start-after`, to cap `MaxKeys` at 1000 and to count requests | 28 requests, bound is 21 |
| G-2 | G2 | same test, bound tightened to 11 | same | 21 requests |
| G-3 | G2 | `ObjectStorageIteratorAsync.WithoutPrefetchTheNextBatchIsFetchedOnDemand` | `src/Disks/DiskObjectStorage/ObjectStorages/tests/gtest_object_storage_iterator_async.cpp`; `ScriptedIteratorAsync` (`:27`) | the flag does not exist |
| G-4 | G2 | `ObjectStorageIteratorAsync.AnEmptyPageWithoutPrefetchStillFollowsTheToken` | same | the flag does not exist |

- The fake server answers `IsTruncated = true` exactly when keys remain. The counts 28 and 21 follow from
  that contract: nine full pages at three requests plus one, and ten full pages at two plus one. The builder
  records the measured numbers. The bound of 11 is the acceptance criterion and does not move.
- G-1 also checks the walk itself: 10000 keys, each once, in order.
- G-3: after the first batch is read, the scripted source has served one batch. It serves the second only
  when the consumer steps past the first. With the default flag it has served two. The existing tests of the
  file pin the default path.
- G-4 guards the empty-page loop of `nextBatch`: a page with no keys and a continuation token is followed
  when no batch was prefetched.
- Pins that must stay green unchanged (CAS-29.13 AC #3): `T/gtest_cas_backend.cpp:1482`
  (`CASObjectStorageBackend.EmulatedListTokenMatchesHeadToken`), `T/gtest_cas_backend_contract.cpp:143-153`,
  `CASBackendListing.ForEachWalksEveryPageOnce`, `CASBackendListing.ForEachEmptyPrefixVisitsNothing`.
- Suite `ObjectStorageIteratorAsync` is outside the `CAS*` filter. The gates run it by name
  ([§14](#gates)).

### 9.4 Not closed by story G {#story-g-not-closed}

- CAS-29.13 AC #2, the stand ratio of `S3ListObjects` per list page.
- The 9.3 requests per page seen on otel.demo. The RustFS runs do not explain it.
- If the consult refuses lever A, G2 is dropped. The bound stays at 21 and AC #1 is recorded as not met.
- The three measurement commits of branch `cas/gc-request-cuts` (`39964755af4`, `27436f2ae8c`,
  `f11659299ef`) are not taken. Their `LOG_TRACE` counter is process-wide and wrong under concurrency.

## 10. Story H: four observability fixes {#story-h}

Four independent commits. Each has its own problem, change and test.

### 10.1 H1: one `Information` line per finished GC round (CAS-29.14) {#story-h1}

- **Problem.** A finished round leaves a row in `system.cas_gc_log` and nothing at `Information` level in
  the server log. An operator with only the text log cannot see that GC runs, how long a round took or what
  it deleted.
- **Change.** `CasGcScheduler::runRoundLogged` (`CA/Gc/CasGcScheduler.cpp:192-328`) writes one `LOG_INFO`
  before `emit(fin)` on the success path (`:309`) and on the exception path (`:325`). Both scheduled and
  manual rounds (`SYSTEM CAS GC RUN`) pass through this function.
- **Which rounds.** The line is written for the outcomes `Success`, `Aborted`, `Failed` and `Stopped`. A
  Finish row with `NotALeader` or `Deferred` gets no `Information` line. A follower ticks every
  `cas_gc_interval_sec` and a deferred round did no work; both keep their `Debug` lines in the loop
  (`:413-420` and the follower branch below it).
- **Fields**: `round`, `outcome`, `trigger`, `duration_ms`, `deleted` (`objects_deleted`), `condemned`
  (`entries_condemned`) and `error_code` come from the `GcRoundLogRecord` being emitted
  (`CA/Gc/CasGcScheduler.h:21-85`). `keys_listed` is not in the record: the phase sink of `runRoundLogged`
  sums the phase metric `ref_keys_listed` into a local.
- **Docs.** `D/operations/monitoring.md`: the line, its fields and the matching `system.cas_gc_log` columns.

### 10.2 H2: `NULL` success age before the first success (CAS-1.1) {#story-h2}

- **Problem.** `system.cas_mounts.last_success_age_seconds` shows 0 and the metric
  `CASGCLastSuccessAgeSeconds_<disk>` reports 0 until GC succeeds once on this server. A threshold alert on
  the age reads "just succeeded" for a GC that never ran.
- **Change.**
  - `src/Storages/System/StorageSystemContentAddressedMounts.cpp:220-226` (the insert is at `:224`): insert
    the age only when `health->ever_succeeded`, else `insertDefault`. The column is already
    `Nullable(UInt64)` (`:54`). Its description drops "(0 if it never led)".
  - `src/Interpreters/ServerAsynchronousMetrics.cpp:413-414`: emit the metric only when `ever_succeeded`.
    Its description drops "(0 if it has never led one)".
- **Docs.** `docs/en/operations/system-tables/cas_mounts.md` (`:38`, `:57`, `:75`),
  `D/operations/monitoring.md` (`:149-158`), `D/operations/debugging.md` (`:79`),
  `D/architecture/mounts-and-leases.md` (`:390`), `D/operations/troubleshooting.md` (`:20`). They state that
  an alert needs an `absent` rule next to the threshold.

### 10.3 H3: the append-lane state by name (CAS-148) {#story-h3}

- **Problem.** `CA/Pool/CasRefLedger.cpp:4627-4634` logs a refused snapshot publication at `Warning` with
  the lane state as a number. State `Writing` is the normal race with an appender and fires on a healthy
  server.
- **Change.** `std::string_view refLaneStateName(RefLaneState)` next to the enum
  (`CA/Pool/CasRefLedger.h:51-59`), a `switch` without `default`. The message prints the name. `Writing`
  logs at `Debug`, every other state at `Warning`. The counter `CASRefSnapshotPublishBackoff` stays.

### 10.4 H4: the manifest give-up text from the give-up facts (CAS-446) {#story-h4}

- **Problem.** `CA/Pool/CasPartWriteTxn.cpp:697-702` reports every `GaveUp` of the part-manifest `PUT` as
  "UNCERTAIN (retry budget exhausted)". That is false when the write was never sent, which is the case of a
  drained mount lease.
- **Change.** The arm reads the `GaveUp` fields (`CA/Backend/CasWriteResult.h:60-70`) and picks one of six
  texts with a `switch` over `why` that has no `default`. The exception stays `throwCasWriteRetryLater`,
  code 210.

| `why` | `sent_any` | `deadline_source` | Text after "part-manifest PUT at '{key}'" |
|---|---|---|---|
| `FenceLost` | any | any | was refused: this server no longer holds the mount it was admitted under |
| `Deadline` | no | `Lease` | was not sent: the mount lease has too little time left, so writes are paused |
| `Deadline` | no | `Policy` | was not sent: the request window ran out before the first attempt |
| `Deadline` | yes | either | is UNCERTAIN: {attempts_sent} attempt(s) sent, none conclusive by the {lease, policy} deadline; the body may have landed |
| `Unresolved` | no | any | was not sent: a read of the current object did not complete ({last_seen}) |
| `Unresolved` | yes | any | is UNCERTAIN: {attempts_sent} attempt(s) sent and the resolving read did not complete ({last_seen}); the body may have landed |

- Every text ends with "the caller re-stages with a fresh ManifestId".
- No pause age in the text. The pool warning "writes are paused since {} ms ago"
  (`CA/Pool/CasMountRuntime.cpp:211`) fires at the same moment and carries it.
- `orThrow` (`CA/Backend/CasWriteResult.h:113-129`) is not touched. The six cases follow its split.

### 10.5 Tests {#story-h-tests}

| # | Commit | Test | File, fixture | Red before |
|---|---|---|---|---|
| H-1 | H1 | `CASGCLog.FinishLogLineMatchesCasGcLogRow` | `T/gtest_cas_gc_log.cpp`; fixture of `EmitsStartFinishWithCounts` (`:90`), `Poco::StreamChannel` capture as in `T/gtest_cas_pool.cpp:2378-2427` | no such line |
| H-2 | H1 | `CASGCLog.AnAbortedRoundLogsExactlyOneFinishLine` | same file; fixture of `AbortedFinishOnThrowingRound` (`:279`) | no such line |
| H-3 | H2 | `CASGCHealth.ReflectsLeadershipAndPendingReclaim`, extended | `T/gtest_cas_gc_log.cpp:719-746` | pin of `ever_succeeded` before and after the first round |
| H-4 | H2 | stateless test `cas_mounts_gc_success_age_null`, created with `add-test` | shape of `05010_cas_mounts_gc_health.sh`; inline disk with a long `cas_gc_interval_sec` | the column is 0, expected `NULL` |
| H-5 | H3 | `CASRefSnapshotPublishOrdering.LaneStateNamesCoverEveryState` | `T/gtest_cas_ref_snapshot_publish_ordering.cpp` | the helper does not exist |
| H-6 | H4 | `CASPartWriteTxn.StageManifestNotSentUnderDrainedLeaseSaysNotSent` | `T/gtest_cas_part_write.cpp`; `openPool`, `useVirtualMountRequestClock` (`:90-104`) | the text says "UNCERTAIN" |
| H-7 | H4 | `CASPartWriteTxn.StageManifestSentThenGaveUpSaysUncertain` | same file; `AmbiguousMetaWriteBackend` (`:870`) | the text has no attempt count |

- H-1: the captured line carries the same `round`, `outcome`, `duration_ms` and `deleted` as the Finish row
  the sink received. Exactly one line per Finish row with outcome `Success`. The fixture of
  `NotALeaderRoundEmitsOnlyTheLeasePhase` (`:693`) gets the same capture: a Finish row and no line.
- H-2: one line with outcome `Aborted` and a non-zero `error_code`.
- H-4: `NULL` before any round; a value below 60 after `SYSTEM CAS GC RUN`. The test also reads
  `system.asynchronous_metrics` after `SYSTEM RELOAD ASYNCHRONOUS METRICS`: the metric is absent before the
  round and present after it ([§21](#left-to-builder)).
- H-5: six states, six distinct non-empty names. The level split has no unit seam and is reviewed by
  reading.
- H-6 asserts "was not sent" and the absence of "UNCERTAIN". Both H4 tests assert code `NETWORK_ERROR`.

### 10.6 Not closed by story H {#story-h-not-closed}

- CAS-446 AC #2 is a decision, not code: CAS-405.3 closes the client-visible failure of a short stall. At
  120/10 the study's 20 s and 45 s process stalls cost no errors. The task note records it.
- CAS-446 AC #1 names the decommission tombstone write. It already reports by `sent_any`
  (`CA/Tools/CasDecommission.cpp:95-106`).

## 11. Story I: CI fixes {#story-i}

No product code. One commit per row.

| Commit | Task | Change | Check |
|---|---|---|---|
| I1 | CAS-443 | Take `ad04f95e441`: new `tests/integration/test_backward_compatibility/configs/skip_check_for_incorrect_settings.xml`, two lines in `test_custom_s3_disk_remote_url_allow_hosts.py` | `xmllint --noout`, `python3 -m py_compile`, the module run |
| I2 | CAS-432 | Take `2a4b5e72aa2`: `ci/defs/altinity_jobs.py:85-93` moves the two ASan cas-s3 stateless shards from `RunnerLabels.AMD_MEDIUM_CPU` to `RunnerLabels.AMD_LARGE`; workflows regenerated with `python3 -m ci.praktika yaml` | `git diff --stat .github/workflows` shows only the runner label |
| I3 | CAS-434 | `tests/broken_tests.yaml:151-155`: the rule of `03572_export_merge_tree_part_limits_and_table_functions` gets `check_types: [tsan, msan, asan]` and loses its `message` filter | the file loads as YAML |
| I4 | CAS-85, CAS-321.2 | `ci/jobs/scripts/clickhouse_proc.py`: `dump_system_tables` (`TABLES`, `:1353-1363`) adds `cas_gc_log` and `cas_log` when a server config has `<metadata_type>cas</metadata_type>` | `python3 -m py_compile`; the CAS lanes of PR 2831 attach both tables |
| (I5) | CAS-466 | Test-only fix, if the sibling check finds one | `CAS*` gate |

- **CAS-434.** On loaded sanitizer cas-s3 lanes the test exceeds its 60 s `wait_for_exports` poll or the
  300 s receive timeout of `OPTIMIZE FINAL`. Measured: ASan 2 of 2 runs `BROKEN` at 600 s, MSan 2 of 3 runs
  `FAIL` at 482 s. A run that dies at `OPTIMIZE` never prints `timeout: 60s`, so the `message` filter misses
  it. The `reason` names the lane load and these times. The task asked to forward-port the
  `no-cas-storage` tag; `5a1ffb222a5` removed that tag on purpose, so the tag does not come back.
- **CAS-85, CAS-321.2.** The marker keeps non-CAS lanes unchanged: they do not have the two tables, and a
  dump of a missing table would turn a green job red.
- **CAS-466.** The hang is fixed at `fa1b89d1529` by `50dbaee3a69` and `f1bdb36d374`. Left is AC #4: check
  the sibling tests that set `snapshot_log_count_threshold = 0` for the same wait: two in
  `T/gtest_cas_detached_work.cpp`, two in `T/gtest_cas_ref_snapshot_publish_ordering.cpp`, one in
  `T/gtest_cas_ref_chunked_flush.cpp`, two in `T/gtest_cas_ref_writer.cpp`. The result is a note in the
  task. Commit I5 exists only if a sibling needs a fix.
- I1 and I2 are picked with their author and message. A conflict is resolved toward the wave's tree.

## 12. Commit series {#commits}

Each line is one commit on the work branch. Docs ride with the commit that changes the behaviour.

| # | Subject | Stories' tests |
|---|---|---|
| A1 | Refuse a cold admission of a namespace life retired after its read | A-1 to A-4 |
| A2 | Announce a namespace-life retirement before its catalog erase | A-5 to A-7 |
| A3 | Record a retirement when a closed runtime is evicted | A-8 |
| B1 | Keep a retirement record only while a cold admission is in flight | B-1 to B-5 |
| C1 | Pause a conflicted catalog erase before its resolution read | C-1, C-2 |
| C2 | Report the erases of a pre-fold drain that throws | C-3 |
| D1 | Default the CAS mount lease to 120 s | D-1 |
| D2 | Print the write reserve and the renewal slack at mount | D-2 |
| E1 | Make the CAS request window a default of the request plane | E-1 |
| E2 | Add `cas_request_window_ms`, default 120 s | E-2, E-3 |
| F1 | Default five GC round budgets to unbounded | F-1, F-2 |
| G1 | List CAS pages of 999 keys so the sentinel fits the batch | G-1 |
| G2 | Let a list caller turn off the next-page prefetch | G-2 to G-4 |
| H1 | Log one `Information` line per finished GC round | H-1, H-2 |
| H2 | Report no GC success age before the first success | H-3, H-4 |
| H3 | Name the append-lane state in the snapshot publication refusal | H-5 |
| H4 | Build the manifest give-up text from the give-up facts | H-6, H-7 |
| I1 | Skip the unknown-config check on the upstream image in `test_custom_s3_disk_remote_url_allow_hosts` | module run |
| I2 | Run the ASan cas-s3 stateless shards on the LARGE runner | workflow diff |
| I3 | Mark `03572_export_merge_tree_part_limits_and_table_functions` broken under every sanitizer | YAML load |
| I4 | Dump `cas_gc_log` and `cas_log` from CI lanes that mount a CAS disk | CAS lanes |

Rules of the series:

- **Every commit builds and passes `CAS*`.** A test lands with the change that turns it green.
- **Red first.** Before a product change, its tests run against the tree without it. The failing output is
  saved as a log in the plan workspace. A test marked "pin" in a tests table is green before and after, and
  its log says so.
- **Work branch, then clean history.** Work commits may be many per story, with fix rounds. After the
  reviews and CI, the series above is rebuilt as new commits over `d8cbdfae3ec`, the head of PR 2831. The
  rebuilt tree must be identical to the gated work tree (`git rev-parse <head>^{tree}`).
- **The late-request audit bundle is replayed first.** `d8cbdfae3ec` does not contain it. Its stories land
  as their own commits below A1, as its spec lists them.
- **Trailers**: `Signed-off-by` and `Co-Authored-By`. Subjects are imperative, at most 72 characters.
- **One push**, after the gates of [§14](#gates) pass on the exact tree. The branch moves by fast-forward.

## 13. Production code outside `CA/` {#upstream}

| File | Commit | Change | Shared with upstream |
|---|---|---|---|
| `src/IO/ObjectStorageRequestProfile.h` | G2 | one field, default keeps today's behaviour | yes |
| `src/Disks/DiskObjectStorage/ObjectStorages/ObjectStorageIteratorAsync.{h,cpp}` | G2 | one protected flag; lazy scheduling of a batch that was not prefetched | yes |
| `src/Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.cpp` | G2 | the iterator takes the flag from the request | yes |
| `src/Interpreters/ServerAsynchronousMetrics.cpp` | H2 | one condition inside the CAS block | file yes, block no |
| `src/Storages/System/StorageSystemContentAddressedMounts.cpp` | H2 | CAS system table | no |
| `ci/jobs/scripts/clickhouse_proc.py` | I4 | two tables behind a config marker | yes |

**Consult question for the spec review: lever A.** The sketch of the shared change in `nextBatch`:

```cpp
while (true)
{
    if (!outcome_future.valid())            /// was: chassert(outcome_future.valid());
        outcome_future = scheduleBatch();
    BatchAndHasNext result = outcome_future.get();
    ...
    if (has_next_batch && prefetch_next_batch)   /// was: if (has_next_batch)
        outcome_future = scheduleBatch();
```

- With the default flag every path is as today: the future is valid at the top of the loop on every pass.
- With the flag off, a consumer that stops after one batch leaves no request in flight, so the destructor's
  `deactivate` has nothing to wait for.
- The catch block and `has_next_batch` handling do not change.
- Questions: is the lazy path correct for `getCurrentBatchAndScheduleNext` and for the empty-page loop; is a
  request field the right carrier, or should `iterate` get a parameter; is there a smaller change.
- A refusal drops G2 and nothing else ([§9.4](#story-g-not-closed)).

Everything else in the wave is inside `CA/`, `T/`, `D/`, `tests/` or `ci/`.

## 14. Gates {#gates}

| Gate | When | Command or tree |
|---|---|---|
| Release `CAS*` | every commit | `unit_tests_dbms --gtest_filter='CAS*'` from `cas-pr1/build` |
| Iterator suite | commit G2 and final | `unit_tests_dbms --gtest_filter='ObjectStorageIteratorAsync*'` |
| ASan `CAS*` | final tree | the same filter from `cas-pr1/build_asan` |
| Fast-test compile | final tree | the no-S3 configuration, `build_fast` |
| Stateless | final tree | tests matching `_cas_`, including the new H-4 test, on a local server |
| Integration | final tree | `test_cas_shared_pool`, `test_cas_drop_pool_member`, `test_cas_gc_sharded`, `test_cas_early_commit_restart`, `test_backward_compatibility` (the I1 module) |
| Style | final tree | the check functions of `ci/jobs/check_style.py` on the changed files |
| Task review | every task | spec compliance and code quality, per the plan |
| Final review | work head | whole-branch review twice (two independent reviewers) and the umbrella review |
| CI | head of PR 2831 | every red job is root-caused or gets a task |

- Builds are serialized with `flock` on one lock file and write to a log. No `-j`.
- A test log starts with the build marker of the binary it ran.
- The final gates run on the exact tree that is pushed. A change after them reruns them.
- Every red is root-caused or gets a task. "Known red" is not a state.

## 15. Measurements {#measurements}

One report: `docs/superpowers/reports/2026-11-12-cas-improvements-wave1-measurements.md`. Setup: the
standalone server and workload of
`docs/superpowers/reports/2026-11-11-cas-pr2-late-request-audit-bundle-measurements.md`, run on `fa1b89d1529`
and on the wave head.

Predictions, written before the runs:

| # | Prediction |
|---|---|
| M-1 | Insert and merge throughput of the churn workload stays within 10 % of the base |
| M-2 | The server log has no new `CA GC round failed` lines |
| M-3 | The server log has exactly one `Information` round line per Finish row of `system.cas_gc_log` with outcome `Success`, `Aborted`, `Failed` or `Stopped`, and none for the other rows |
| M-4 | The reclaim backlog is 0 at the end of the idle phase, on base and on head |
| M-5 | `system.cas_mounts.last_success_age_seconds` is `NULL` before the first round and a number after it |

- The request count of a list page is proven by G-1 and G-2, not by a run.
- The lease and window defaults were measured by the study. The wave does not repeat those runs.
- A failed prediction is a finding. It is reported, not explained away.

## 16. Models {#models}

No model changes in this wave.

- Stories A and B change how a server forgets a retired life in memory. The TLA+ models describe the
  catalog and the store, not the runtime cache.
- Story C keeps the barrier that `docs/superpowers/models/CaRefPreFoldDrainCore.tla` checks as
  `DrainBeforeDecision`: a drain that cannot finish still aborts the round. The non-aborting outcome needs a
  model configuration first ([§19](#non-goals)).
- Stories D, E and F change constants that the models take as parameters.

## 17. Documentation {#documentation}

`S/` is `docs/en/operations/system-tables/`.

| Commit | Files |
|---|---|
| C1 | `D/architecture/garbage-collection.md` (drain section) |
| C2 | `S/cas_gc_log.md` |
| D1 | `D/configuration.md`, `D/architecture/mounts-and-leases.md`, `D/architecture/garbage-collection.md`, `D/operations/migration.md`, `D/operations/troubleshooting.md` |
| D2 | `D/architecture/mounts-and-leases.md` |
| E2 | `D/configuration.md`, `D/architecture/blob-protocol.md` |
| F1 | `D/configuration.md`, `D/architecture/garbage-collection.md` |
| H1 | `D/operations/monitoring.md` |
| H2 | `S/cas_mounts.md`, `D/operations/monitoring.md`, `D/operations/debugging.md`, `D/architecture/mounts-and-leases.md`, `D/operations/troubleshooting.md` |

- Stories A, B, G, H3, H4 and I change no documented behaviour.
- User docs use plain words: no invariant tags, no task numbers.
- Every changed default is documented with its old value and the key that restores it.
- A new header gets an explicit `{#anchor}`.
- The pull request body is rewritten from `.github/PULL_REQUEST_TEMPLATE.md` when the series is pushed.

## 18. Acceptance mapping {#acceptance}

| Task | Criterion | Where |
|---|---|---|
| PR 2570 | a cold admission of a retired life is refused | A-1 to A-8 |
| CAS-428 | #1 retirement records are bounded | B-1 to B-5 |
| CAS-463 | #1 ddl16 rounds succeed | C-1 stands in; stand rerun not in wave |
| CAS-463 | #2 a contended erase does not abort the fold | not in wave ([§19](#non-goals)) |
| CAS-463 | #3 the round row shows erases done | C-3 |
| CAS-405.3 | #1 defaults and docs | D-1, docs of D1 |
| CAS-405.3 | #2 stand rerun | not in wave |
| CAS-353 | #1 a slow renewal `PUT` does not refuse writes | D1: slack 94 s |
| CAS-353 | #2 reserve and slack printed and documented | D-2, docs of D2 |
| CAS-405.6 | #1 configurable window | E-1 to E-3 |
| CAS-405.6 | #2 stand rerun | not in wave |
| CAS-405.6 | #3 recovery budget relation documented | docs of E2; derived at the storage layer |
| CAS-29.11 | #1 removed keys warn | superseded: the keys stay |
| CAS-29.11 | #2 redelete sum past 5000 | F-2 pins the meaning of 0 only |
| CAS-29.11 | #3 migration note | docs of F1 |
| CAS-29.13 | #1 at most 11 requests for 10000 keys | G-2 |
| CAS-29.13 | #2 stand ratio | not in wave |
| CAS-29.13 | #3 probe and fallback guarantees | the pins of [§9.3](#story-g-tests) |
| CAS-29.14 | #1, #2 | H-1, H-2 |
| CAS-1.1 | #1 to #3 | H-3, H-4, docs of H2 |
| CAS-148 | #1, #2 | H-5, review of the level split |
| CAS-446 | #1 | H-6, H-7 |
| CAS-446 | #2 | decision recorded in the task ([§10.6](#story-h-not-closed)) |
| CAS-443 | #1, #2 | I1, CI of PR 2831 |
| CAS-432 | #1 to #3 | I2, CI of PR 2831 |
| CAS-434 | #1 | I3, CI of PR 2831 |
| CAS-85 | #1, #2 | I4, CI of PR 2831 |
| CAS-321.2 | #1 | I4 |
| CAS-466 | #1 to #3 | already at `fa1b89d1529` |
| CAS-466 | #4 sibling check | note in the task, I5 if needed |

## 19. Non-goals {#non-goals}

- **CAS-29.3.** Its premise is false at `fa1b89d1529`. The task gets a note and no code.
- **CAS-463 AC #2**, the non-aborting drain outcome. It removes the barrier of `DrainBeforeDecision`. A new
  task carries it with a model configuration and a TLC run.
- **CAS-321.3.**
- **CAS-29.1 and CAS-29.6.** Second wave, same pull request.
- **CAS-29.10** (round deadline), **CAS-405.1**, **CAS-405.2**, **CAS-382**, **CAS-22**, **CAS-452**,
  **CAS-467**, **CAS-128.**
- **Stand reruns**: CAS-405.3 AC #2, CAS-405.6 AC #2, CAS-29.13 AC #2, CAS-463 AC #1.
- **Removing GC budget keys or adding a warning for them.**
- **A shared `renderGaveUp` helper.** It would change three pinned strings of `orThrow`.
- **Any on-store format change.**

## 20. Risks {#risks}

| Risk | Story | Bound or mitigation |
|---|---|---|
| A longer restart wait after an unclean stop: 138 s, was 43 s | D | documented; `cas_unsafe_remount_no_delay`; the old value by key |
| A dead mount is fenced later: 224 s, was 103 s | D | documented; the old value by key |
| A request that cannot succeed now fails after 120 s, was 90 s. GC and offline tools use the same window | E | `cas_request_window_ms` |
| Upgrade with a short lease set by hand: a 120 s window is cut by the lease bound at bind time | E | `Retry::bind` already takes the minimum; no new refusal at mount |
| The first GC round after upgrade on a backlogged pool is long and delays the dead-member fence | F | documented; a cap by key; the round deadline is CAS-29.10 |
| Unbounded graduation scales `HEAD` and `DELETE` load with the cohort | F | study: a 900 s interval graduated 15174 objects in one round of 1201 s |
| `gc_round_outcome_entry_budget` and `gc_round_handoff_prefix_wholesale_budget` at 0 have no run | F | code reading only; a cap by key |
| The erase still loses to a writer that lands between its read and its write | C | the round aborts as today; follow-up task |
| `acquireMutableRefTableRuntime` takes `ref_queue_mutex` one more time per call | B | two map lookups; measured by M-1 |
| A consumer of the async iterator relies on the prefetch | G | opt-in flag, default on; only `listUnder` turns it off |
| A page of 999 keys adds list pages: 0.1 % more pages | G | one request per page instead of three |
| A threshold-only alert on the GC success age goes silent before the first success | H2 | documented with the `absent` rule |
| Tools that parse the old give-up text | H4 | code 210 unchanged; the text was never documented |
| I2 regenerates workflow files | I | the diff is checked to hold only the runner label |

## 21. Left to the builder {#left-to-builder}

- **C2 mechanism.** How the count of completed erases reaches `Gc::runRegularRound` after a throw: an out
  parameter of the drain, a member of the reconciler, or a scope guard around the phase. Constraint: no new
  exception type, and the phase record of a successful drain is unchanged.
- **C1 give-up path.** After the reorder, the call that meets the expired deadline may be the resolution read
  instead of the erase write. `CASRefCatalogRemoval.PerpetualConflictGivesUpWithinOneWindowNotAtTheAttemptCap`
  decides: code `NETWORK_ERROR`, a message with "deadline", fewer than 100 attempts.
- **B guard shape.** Whether `ColdAdmission` holds a pointer to the ledger or a slot key, as long as its
  destructor releases the count on every path of [§4.2](#story-b-change).
- **D2 wording** of the two log fields beyond their names.
- **E1 plumbing.** Whether `CasRequests` stores the window or a pointer to the budget.
- **G-1 fixture.** How the HTTP server of `T/gtest_cas_s3_single_attempt_client.cpp` pages. If extending it
  costs more than a new fixture in `T/gtest_cas_backend_listing.cpp`, the test moves there. Its suite name
  must start with `CAS`.
- **H-4 metric half.** If the metric loop does not visit a disk declared inline in a table, the stateless
  test keeps the column half only, and the metric condition is reviewed by reading. The report says which.
- **H1 format** of the line, as long as it carries the eight fields by name.
- **Test names** may change for clarity. Suites keep the `CAS` prefix. The fixture and the red-before
  evidence of each row are binding.
