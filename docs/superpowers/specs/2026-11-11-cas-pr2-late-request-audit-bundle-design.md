---
description: 'Design for one pull request on antalya-26.8 that carries five CAS stories from the late-request audit as commit series: epoch order in the creator-fence certificate (CAS-364), a mount-slot read before a relink confirm answers `Yes` (CAS-365), a write lane that stays closed after an unresolved `Live -> Removing` write (CAS-367), one frozen retry policy for a namespace drop (CAS-69) and three small robustness fixes (CAS-373).'
sidebar_label: 'CAS: late-request audit bundle (PR2)'
sidebar_position: 112
slug: /superpowers/specs/cas-pr2-late-request-audit-bundle-design
title: 'CAS: five late-request audit fixes in one pull request'
doc_type: 'reference'
---

# CAS: five late-request audit fixes in one pull request — design {#cas-pr2-late-request-audit-bundle}

- Date: 2026-11-11. Status: draft, not approved.
- Tasks: CAS-364, CAS-365, CAS-367, CAS-69, CAS-373, CAS-455. Residual tasks: CAS-453, CAS-454, CAS-461, CAS-462,
  CAS-183, CAS-71.1.
- Base: the final head of `fix/antalya-26.8/cas-ref-cleanup-per-life-licence` ("PR1", specified in
  `docs/superpowers/specs/2026-11-11-cas-pr1-ref-cleanup-bundle-design.md`). PR1 sits on `2f89c5db182`, head
  of PR [#2805](https://github.com/Altinity/ClickHouse/pull/2805). Branch:
  `fix/antalya-26.8/cas-late-request-audit-bundle`.
- `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`. This document cites symbols, not
  line numbers: the base still moves.
- Draft commits (objects in this repository, diffs in `tmp/cas298/bundle/<sha>.diff`): `0c57ca5a337e`
  (CAS-364), `cfc79da94a9a` (CAS-365), `c7a242d05bf8` (CAS-367), `0f285d4e01d5` (CAS-373). Their reviews are in
  `tmp/noclock/drafts/prep/<task>/REVIEW.md`. CAS-69 has no draft.

## 1. Decision {#decision}

- **One pull request, five stories, nine commits.** Each story is a commit series that reads on its own.
- **Order: A, B, C, D, E** ([§2](#order)).

| Story | Tasks | Area | What changes |
|---|---|---|---|
| A | CAS-364 | mount slot | `classifyFenceCertificate` compares writer epochs before it trusts a death certificate |
| B | CAS-365 | relink confirm | `Pool::confirmExactRef` reads the mount slot once before it answers `Yes` |
| C | CAS-367 | namespace drop | The write lane stays closed after a `Live -> Removing` catalog write of unknown outcome |
| D | CAS-69 | namespace drop | `dropNamespaceImpl` freezes one retry policy and passes it to every read toward the removal |
| E | CAS-373 | three sites | A fenced writer's merge rejection is `FencedOut`; a vanished pool meta is `INVALID_STATE`; the bucket versioning probe is one bounded attempt |

- **Every commit builds and passes the `CAS*` gtest gate.** A test and the product change that turns it
  green share a commit. Red-first evidence is a log ([§9](#commits)).
- **No format change, no new setting, no port to `antalya-26.6`.**
- **Two new `ProfileEvents`**: `CASRelinkConfirmRefusedSlotMoved`, `CASRelinkConfirmRefusedSlotUnreadable`.
- **One change outside `CA/`**: the signature of `IObjectStorage::isBucketVersioningEnabled`
  ([§10](#upstream)). It is the last commit, so it can be dropped alone.
- **No model change.** The model for story B exists and already recommends this fix ([§13](#models)).
- **The pull request is opened after PR1 merges.** Its base is then the branch PR1 merged into. No stacked
  pull request.

## 2. Why one pull request and why this order {#order}

All five stories come from one audit question: where does the code act on a request, or on a state it read,
after that request or state stopped being current? Each story closes one such place. Four of them touch the
mount slot or the namespace catalog, so they share reviewers and tests.

- **Based on PR1, not on `2f89c5db182`.** Story E edits the `decide` step of `publishCkpt`, which PR1 rewrote
  (`refuseFrontierAgainstDurable`). The CAS-373 draft conflicts with PR1 in `CA/Gc/CasGc.cpp`,
  `CA/Pool/CasPoolMeta.cpp`, `CA/Pool/CasRefCkpt.h` and `src/Disks/tests/gtest_cas_ref_ckpt_join.cpp`.
- **A first.** It is the only High task, it is ten lines, and it depends on nothing.
- **B second.** It is the largest story. Its docs commit closes the series.
- **C before D.** Both edit `CasRefLedger::dropNamespaceImpl`. C restructures the `try`/`catch` around
  `beginRemoving`. D then threads one policy through the calls that C leaves in place. D also reuses the
  test hook that C adds.
- **E last.** Its three fixes are independent. The commit that touches `IObjectStorage` is the last one.

The CAS-364, CAS-365 and CAS-367 drafts apply on PR1 with a three-way merge, in that order. Story E is ported by
hand. One item of the CAS-373 draft, a comment in `CasGc.cpp` that said the licence "WIDENS", is gone: PR1
replaced that code.

## 3. Story A: compare writer epochs before trusting a death certificate (CAS-364) {#story-a}

### 3.1 Problem {#story-a-problem}

`isCreatorFenceTerminal` (`CA/Pool/CasServerRoot.cpp`) reads the mount slot and asks
`classifyFenceCertificate(lease, fence_writer_epoch)` whether the creator at `fence_writer_epoch` is dead.
Today the classifier answers in this order:

1. slot body is gc-fenced: `GcFenced`;
2. slot body is a clean release: `CleanFarewell`;
3. `lease.writer_epoch != fence_writer_epoch`: `SupersededEpoch`;
4. otherwise `None`.

Two defects follow:

- **A death certificate of a lower epoch kills a higher one.** A slot body at epoch 7 that is gc-fenced or
  farewelled certifies the death of a creator at epoch 8.
- **`!=` treats a lower slot epoch as a successor.** A live slot body at epoch 7 makes a creator at epoch 8
  `SupersededEpoch`.

Both callers act on a terminal answer. `CasRefLedger::resolveNamespaceLife` lets
`CasRefCatalog::reconcileStaleCreator` adopt the `Creating` row. `CasRefLedger::dropNamespaceImpl` lets
`CasRefCatalog::cancelStalledCreating` delete it.

**This is defence in depth.** No reachable history puts a slot below a live fence today:

- `tryRemountOnce` allocates the epoch, claims the slot, starts the renewer and only then publishes the
  live writer epoch;
- `claimMount` never lowers a slot (`CASClaimMountMonotonicity.LowerEpochOverANewerBodyFailsWithoutWriting`);
- a re-created slot takes a greater epoch from the durable counter at `Layout::epochKey`.

The classifier should not depend on those three facts at every future caller.

### 3.2 Change {#story-a-change}

`classifyFenceCertificate` compares epochs first:

| Slot epoch versus fence epoch | Slot body | Result |
|---|---|---|
| greater | any | `SupersededEpoch` |
| lower | any | `None` |
| equal | gc-fenced | `GcFenced` |
| equal | clean release | `CleanFarewell` |
| equal | live | `None` |

- The doc comment of `isCreatorFenceTerminal` (`CA/Pool/CasServerRoot.h`) says "strictly greater" and names
  the lower-epoch case as not terminal.
- No new enum value, event or message.

### 3.3 Tests {#story-a-tests}

Suite `CASFenceTerminal` in `src/Disks/tests/gtest_cas_mount.cpp`.

| Id | Test | Red at the base |
|---|---|---|
| A1 | `ALowerWriterEpochIsNotTerminalForTheHigherOne`: live, gc-fenced and farewelled bodies at epoch 7; `isCreatorFenceTerminal(..., 8)` is false for each | yes, all three return true |
| A2 | `AGreaterLiveWriterEpochIsTerminalForTheOldOne`: renamed from `ADifferentLiveWriterEpochIsTerminalForTheOldOne` | no, rename only |

The equal-epoch tests of the suite stay unchanged.

### 3.4 Not closed by story A {#story-a-residual}

- The refusal text does not name the slot's epoch when a fence is not proven dead. Placement: CAS-453.
- CAS-364 criterion 1 (`claimMount` refuses the same pair and any epoch not greater than the slot's) is met
  at the base. No code here.

## 4. Story B: read the mount slot before a relink confirm answers `Yes` (CAS-365) {#story-b}

### 4.1 Problem {#story-b-problem}

A replica that fetches a part by relink asks the sender to confirm that the sender still protects the exact
ref. The sender answers through `ContentAddressedMetadataStorage::confirmExactRef`, `Pool::confirmExactRef`
and `CasRefLedger::confirmExactRef`. The answer comes from the in-memory ledger with zero object-store I/O.

The in-memory fence can say "live" after the durable mount slot moved: another server claimed it, or GC
fenced it. The sender then answers `Yes` for a ref it no longer protects, and the receiver relinks to it.

Evidence: `docs/superpowers/models/CaRelinkConfirmCore_RESULTS.md`, section "CAS-365: the mount slot is not
the fence". Config `CaRelinkConfirmCore_cur_staleyes` violates `ConfirmedRelinkNeverDangles` with a 13-state
counterexample. `CaRelinkConfirmCore_fix_a` (one slot `GET` after the snapshot, ETag match, in-flight attempt
accepted by its write-attempt id) is green over 15,884,787 distinct states. The results recommend fix A.

### 4.2 Change {#story-b-change}

**Part 1: the renewer records which slot body it owns.** New class `MountSlotOwnership` in
`CA/Pool/CasServerRoot.h`:

| Member | Meaning |
|---|---|
| `landed(const Etag &)` | the renewer saw this slot body committed |
| `attempting(UInt128 write_attempt_id)` | a slot write with this attempt id is in flight |
| `attemptEnded` | no slot write is in flight |
| `holds(const Object & slot) const` | whether `slot` is a body this mount wrote |

`holds` returns true only if all of these are true:

- the body is not gc-fenced and not a clean release;
- the body carries this mount's `(server_uuid, writer_epoch)`;
- the object's ETag equals the last landed ETag, or the body's `write_attempt_id` equals the in-flight one.

The ETag covers the settled state. The attempt id covers the window between a renewal landing and its
answer arriving. `MountSlotOwnership` has its own mutex and does no I/O.

Hooks:

- `MountLeaseRenewer::start`: `landed(etag)` after `last_etag` is set.
- `MountLeaseRenewer::renewOn`: `attempting(write_attempt_id)` right after the attempt id is minted and
  before the operation is admitted, with `attemptEnded` in a scope guard. One `attempting` covers every
  resend: the loop resends the same body, attempt id and expected ETag. `landed` runs in the `Committed`
  branch after `last_etag` takes the committed ETag.
- `terminate` records nothing. After a farewell the body is a clean release, so `holds` is false.
- `CasMountRuntime::slotOwnership` returns the current renewer's `MountSlotOwnership` as a `shared_ptr`. It
  takes `driver_mutex` only to copy the pointer. It returns null when no renewer is installed.

**Part 2: `Pool::confirmExactRef` validates a `Yes`.** The inline forwarder in `CA/Pool/CasPool.h` becomes a
function in `CA/Pool/CasPool.cpp`:

1. Copy the `MountSlotOwnership` pointer. This happens before the ledger snapshot, so a remount between the
   snapshot and the `GET` is judged against the old mount's ownership and fails.
2. Ask `CasRefLedger::confirmExactRef`. If the ledger's answer is not `Yes`, return it at once, with zero
   I/O.
3. Read the mount slot once: `CasMountRuntime::slotHeldBy(ownership)` admits an operation on
   `mount_requests`, reads `Layout::mountKey(server_root_id)` under `Retry::once` and returns
   `slot && ownership.holds(*slot)`.
4. The read throws: increment `CASRelinkConfirmRefusedSlotUnreadable` and rethrow.
   `ContentAddressedMetadataStorage::confirmExactRef` already catches `DB::Exception`, logs it and answers
   `Unknown`.
5. The slot is not held: increment `CASRelinkConfirmRefusedSlotMoved` and answer `Unknown`.
6. Otherwise answer `Yes`.

Facts that bound the cost:

- **At most one `GET` per ledger `Yes`, none otherwise.** A ledger `Yes` with no renewer installed, or with
  mount admission refused, sends nothing. A final `Yes` always sent that `GET`. The `GET` can turn a ledger
  `Yes` into `Unknown`, so an `Unknown` answer costs zero or one request. `Retry::once` is one request at
  the CAS engine level. Its duration is bounded by the backend attempt timeout, not by the 90 s window.
- **No ledger mutex is held during the `GET`.** `CasRefLedger::confirmExactRef` releases `ref_queue_mutex`
  and the runtime's `state_mutex` when it returns.
- **No concurrency cap and no cache.** Each confirm pays its own read.
- **The read does not use the hot-key lane.** It goes to the backend directly.

Fail-closed cases, all answer `Unknown`:

| Case | Why |
|---|---|
| no renewer installed | `slotOwnership` is null |
| mount admission refused (fence lost, or no lease budget left) | the admit throws a give-up |
| slot absent | `slot` is empty |
| foreign claim, same pair with another attempt, gc-fence, farewell | `holds` is false |
| own renewal gave up after sending, and its copy lands later | `attemptEnded` cleared the id |

The last row is not a safety gap, and it is not a one-retry cost. A renewal that sent a request and ended
unresolved is terminal (`MountLeaseRenewerState::RenewalTerminal`). The runtime trips the mount fence when
it consumes that result. From then on the ledger answers `Unknown` with no `GET`, and confirms stay refused
until a remount installs a new renewer. This is the base behaviour for a lost mount. Story B does not
lengthen it. Two limits of that claim:

- `attemptEnded` runs before the runtime consumes the result. A confirm in that interval passes the ledger
  and sends its `GET`. The slot rule then decides: `Yes` if the slot still carries the last landed ETag,
  `Unknown` if the abandoned copy landed.
- A renewal that a stopping node cancels before it sent anything returns `MountRenewOutcome::NotAttempted`
  and keeps the renewer. It has no copy that can land, so the row does not apply to it.

**The `GET` and the `holds` check are two steps, and a renewal can commit between them.** The `GET` returns
the body with ETag E1. The renewal lands E2, calls `landed(E2)` and ends its attempt. `holds` then compares
E1 with E2 and finds no in-flight id, so the confirm answers `Unknown` on a healthy mount.

- The refusal is safe. It increments `CASRelinkConfirmRefusedSlotMoved`, and the receiver falls back as
  for any `Unknown`.
- The window has no fixed bound. It runs from the backend serving the `GET` to the `holds` check, and
  includes scheduling and mutex waits. A refusal needs a renewal commit inside it.
- The story keeps fix A as modelled and does not add a second read. Test B20 pins the behaviour, the
  integration runs of [§12](#measurements) measure its rate, and CAS-461 decides on a re-read from that
  rate.

**Stale "zero I/O" claims are corrected in the same commit as the code** when they sit in a touched product
file: the declaration and body comments of `CasRefLedger::confirmExactRef`, the comment near
`confirmLogger`, `CA/Pool/CasPool.h`, `CA/ContentAddressedExchange.h` and
`CA/ContentAddressedMetadataStorage.{h,cpp}`. The contract after this story: zero I/O when the
ledger's answer is not `Yes`; at most one mount-slot `GET` when it is. A final `Yes` required that `GET`. An
`Unknown` after a ledger `Yes` sent zero or one.

### 4.3 Tests {#story-b-tests}

Suite `CASMountSlotOwnership` in `src/Disks/tests/gtest_cas_mount.cpp` (commit 2):

| Id | Test | Red at the base |
|---|---|---|
| B1 | `HoldsTheLandedBody` | yes, with a stub `holds` that returns false |
| B2 | `HoldsTheInFlightAttemptBody` | yes |
| B3 | `DoesNotHoldAForeignUuidOrEpoch` | no, guards the stub's opposite |
| B4 | `DoesNotHoldAGcFencedOrFarewelledBody` | no |
| B5 | `DoesNotHoldTheAttemptBodyAfterTheAttemptEnded` | yes: it asserts `holds` is true before `attemptEnded`, which the stub fails |
| B6 | `RenewerRecordsItsStartAndEveryCommittedRenewal`: a real `MountLeaseRenewer` over the in-memory backend; `holds` is true for the slot after `start` and after each renewal | yes |

Suite `CASConfirmExactRef` in `src/Disks/tests/gtest_cas_confirm_exact_ref.cpp` (commit 3):

| Id | Test | Red at the base |
|---|---|---|
| B7 | `ForeignClaimWhileFenceLiveAnswersUnknown` | yes, answers `Yes` |
| B8 | `SamePairNewAttemptAnswersUnknown` | yes |
| B9 | `SlotDeletedAnswersUnknown` | yes |
| B10 | `SlotMovedBetweenSnapshotAndGetAnswersUnknown` (hook `setConfirmAfterLedgerSnapshotHookForTest`) | yes |
| B11 | `RemountBetweenSnapshotAndGetAnswersUnknown` | yes |
| B12 | `GcFenceOverInFlightRenewalAnswersUnknown` | yes |
| B13 | `OwnRenewInFlightAnswersYes` | no, guards against over-refusal |
| B14 | `OwnSlotAnswersYesWithOneRead`: exactly one backend request, a `GET` of the mount key | yes, zero requests |
| B15 | `FailedSlotReadThrowsAndIsCounted` | yes |
| B16 | `FenceLostAfterTheSnapshotThrowsAndIsCounted`: the hook trips the mount fence; no `GET` is sent; `CASRelinkConfirmRefusedSlotUnreadable` grows by 1 | yes, answers `Yes` |
| B17 | `LeaseBudgetSpentAfterTheSnapshotThrowsAndIsCounted`: the hook moves the clock past the lease deadline; no `GET` is sent | yes |
| B18 | `FarewellAfterTheSnapshotAnswersUnknown`: the hook writes a clean-release body with this mount's pair | yes |
| B19 | `NoRenewerAnswersUnknownWithoutARead`: no renewer is installed when the confirm starts and the ledger says `Yes` | yes |
| B20 | `RenewalCommittedBetweenGetAndCheckAnswersUnknown`: a backend hook runs one renewal after the `GET` is served; the answer is `Unknown` and `CASRelinkConfirmRefusedSlotMoved` grows by 1 | yes; pins the false refusal of [§4.2](#story-b-change) |

- Existing tests that assert zero requests on a `Yes` are changed to one mount-key `GET`. Tests that assert
  zero requests on other answers stay.
- B7, B9 and B15 assert the counter they name.
- B19: the product path with no renewer and a ledger `Yes` is a warmed read-only pool. A read-only open
  installs no renewer (`CA/Pool/CasPool.cpp` calls `mountWritable` only when `read_only` is false), its
  `mount_fence` stays unarmed and permits, and it recovers refs a writable mount wrote. The fixture is that
  pool. `renewerReset` is not such a path: at startup it runs before the pool is published, and in `FORGET`
  it follows the loss of the fence.
- The existing lost-fence test of this suite loses admission before the snapshot. B16 and B17 lose it after.
- A late landing of an abandoned renewal has no end-to-end test of its own. Once the runtime has consumed
  the terminal result, the fence is tripped and the ledger answers `Unknown` before any `GET`. Before that
  the confirm reads the slot. B5 and B8 cover the slot rule for that body: this mount's pair with an attempt
  id the ownership does not list.
- `tests/integration/test_cas_gcs_relink_liveness/test.py` adds `CASRelinkConfirmRefusedSlotUnreadable` to its
  must-be-zero list. `CASRelinkConfirmRefusedSlotMoved` stays off the list: a renewal that commits between
  the `GET` and the `holds` check refuses a healthy mount, so zero is not an invariant.
- `tests/integration/test_cas_replicated_relink/test.py` passes unchanged.

### 4.4 Not closed by story B {#story-b-residual}

- In emulated token modes the S3 SDK may retry inside the one engine attempt. `single_attempt_control_plane`
  is true only for a writable `Native` mount.
- A `Yes` proves the slot at the time of the `GET`. The receiver's use of the answer is bounded by the
  lease, as before.

## 5. Story C: keep the write lane closed after an unresolved removal write (CAS-367) {#story-c}

### 5.1 Problem {#story-c-problem}

`CasRefLedger::dropNamespaceImpl` closes the namespace's write lane (`removal_admission_closed`), then moves
the catalog row `Live -> Removing` with `CasRefCatalog::beginRemoving`. If that throws, the `catch` block
reads the catalog again. If the row still equals the observed `Live` row, it reopens the lane.

The reopen is unsafe when a write was sent and not answered. That request can land after the `catch` read.
The row is then `Removing` while the lane is open and no drop is running.

What follows at the PR1 head, by reading the code:

- **Writes stop at the next append.** A positive append re-reads the catalog, finds the life not `Live`,
  closes the lane and refuses with "exact catalog life is no longer Live".
- **The table does not open after a restart.** There is no `Removing -> Live` transition. A retried `DROP`
  is the only way forward.
- **An append can commit after the row turned `Removing`.** The lane exists to prevent this: it is closed and
  drained before `beginRemoving`. After the reopen, an append can pass its `Live` check, and the late write
  can land before the append commits.
- **No data is lost silently.** GC deletes a `Removing` life only after the terminal removal record is
  folded (`CasRefCatalog::deleteCompletedRemoving`), and the failed drop wrote none.

The harm is a table that stops working some time after a `DROP` that reported failure, with nothing that
links the two.

### 5.2 Change {#story-c-change}

Two flags on the ref-table runtime, both guarded by `ref_queue_mutex`:

| Flag | Set | Cleared | Effect |
|---|---|---|---|
| `removal_attempt_active` | by a removal attempt after `ensureRefTableRecovered`, once no other attempt holds it | scope guard at the end of the attempt | removal attempts on one namespace do not overlap |
| `removal_write_may_land` | in the `catch` block when a `Live -> Removing` attempt was sent, its outcome is unknown and the resolve read does not show `Removing` | by a later attempt on the same runtime that proves the row `Removing`; the lane then stays closed as `Removing`. Otherwise never on this runtime: a remount replaces the runtime, and `invalidateRemovedCatalogLife` drops it when GC erases the row | the `catch` block does not reopen the lane; `enforceRefTableCacheBudget` does not evict the runtime |

**When the outcome is unknown.** A local `removal_write_outcome_unknown` is set by the request observer of the
removal operation (`RequestObserver::on_write_attempt`). The engine calls it on the caller's thread before it
sends each write attempt, and `beginRemoving` is the only write through that operation. `Transitioned` and
`AlreadyRemoving` clear the local. `EntryChanged`, `FencedOut` and every exception leave it as the observer
set it.

- A `beginRemoving` that fails before any attempt is sent leaves the local clear: a failed or timed-out
  catalog read, a refusal from the first `decide`, a window spent before the first send. Nothing can land.
  The `catch` block then reopens the lane on the same terms as a failure before `beginRemoving`: a fresh
  exact `Live` row, an admitted operation, and no earlier attempt that left `removal_write_may_land`.
- Once one attempt is sent, only `Transitioned` and `AlreadyRemoving` clear the local. `EntryChanged` or
  `FencedOut` after a sent attempt keep the lane closed: the sent write can still land.
- Failures before `beginRemoving` is entered keep today's behaviour: the first catalog read, the
  row-disappeared and life-mismatch branches and the `gc/state` read. Nothing was sent, so a fresh exact
  `Live` row reopens the lane.

**What the operator sees.**

- A warning when the flag is set: ``CAS namespace '{}': the outcome of its Live -> Removing catalog write is
  unknown; writes stay refused until a retried removal completes or the disk is remounted``.
- The three refusals of positive ref mutations (`"CAS namespace '{}' is Removing: positive ref mutation
  admission is closed while ..."`) say "a removal write may still land" instead of "is Removing" when the
  flag is set. The row can still be `Live` in that state. `namespaceLife` is the first refusal a public
  mutation reaches; the other two are in `appendRefOpsOnRuntime`.

**The trade.** A drop whose write did not land, but whose outcome was unknown, leaves the table refusing
writes until `DROP` is retried or the disk is remounted. Before this story the table reopened. The story
prefers a refusal that starts at once and names its cause to one that starts at a later append with no
link to the failed `DROP`.

**How the lane reopens.** A remount replaces the runtime (`quiesceRefTablesForRemount`); the new one starts
open. A retried removal that reaches `Transitioned` or `AlreadyRemoving` finishes the drop. A retried removal
that fails leaves the lane closed.

**An unknown outcome that the resolve read settles.** The `catch` block reads the row once. `Removing` under
the same life means the write landed: the removal continues and the flag stays clear, so the runtime is not
pinned in the ref table cache and no refusal claims a write may still land. Any other result of that read
leaves the outcome unresolved and sets the flag. A later attempt on the same runtime that proves the row
`Removing` (`Transitioned`, `AlreadyRemoving`, or its own resolve read) clears the flag: the runtime is
evictable again and refusals name `Removing`.

Test hook: `setRemovalAttemptHookForTest` on `Pool` and `CasRefLedger`, called once per attempt that closed
and drained the lane, before the attempt's catalog read. An attempt that returns earlier (the stream already
holds its terminal transaction) does not call it.

### 5.3 Tests {#story-c-tests}

Suite `CASRefWriterNamespaceRemoval` in `src/Disks/tests/gtest_cas_ref_writer.cpp`.

| Id | Test | Red at the base |
|---|---|---|
| C1 | `AmbiguousBeginRemovingKeepsTheWriteLaneClosed`: the catalog write is sent, the answer is lost, the backend holds the conditional write and applies it on release; the next ref mutation is refused with the "may still land" text; the write then lands and the row is `Removing` | yes, the mutation is admitted |
| C2 | `FailedRetryAfterAmbiguousBeginRemovingDoesNotReopenTheLane` | yes |
| C3 | `AmbiguousBeginRemovingSurvivesRefTableEviction` | yes |
| C4 | `ConcurrentRemovalAttemptsDoNotOverlapAndKeepTheLaneClosed` | yes |
| C5 | `RefusedBeginRemovingBeforeAnySendReopensTheLane`, `EntryChangedBeforeAnySendReopensTheLane`, `ReadFailureInsideBeginRemovingReopensTheLane`: `beginRemoving` fails with nothing sent (no lease budget at the first gate, a changed row at the first `decide`, a failing catalog read); the `catch` read finds the observed `Live` row and the lane reopens | no at the base; red when the local is set before the call |
| C6 | `LeaseGoneAfterASentAttemptKeepsTheLaneClosed`, `EntryChangedAfterASentAttemptKeepsTheLaneClosed`, `FencedOutAfterASentAttemptKeepsTheLaneClosed`: one attempt is sent and held; the call then ends without a transition (the engine gives up at the lease deadline; the row changes and returns to the observed value before the `catch` read; the admission check inside `beginRemoving` fails and the lease budget returns before the `catch` read); the lane stays closed | not run at the base; green with the local set before the call, they guard the sent case against the narrowing |
| C7 | `FailureBeforeBeginRemovingReopensTheLane`: the `gc/state` read fails; the next ref mutation is admitted | no, guards unchanged behaviour |
| C8 | `RemountReopensTheLaneAfterAnAmbiguousBeginRemoving` | no, guards the documented way out |
| C9 | `ResolveReadProvingRemovingLeavesNoUnresolvedWrite`, `RetryProvingRemovingClearsTheUnresolvedWrite`: a row proven `Removing`, by the resolve read or by a later attempt, leaves no unresolved write: the runtime is evictable and a refusal names `Removing` | not applicable; the flag does not exist at the base |

C1 to C4 come from the draft. C5 to C9 close gaps its reviews listed.

- C1 needs a backend that delivers the held write later. The draft's backend only throws, so nothing lands;
  it is replaced.
- C6 uses a temporary loss of the lease budget on purpose. A permanently lost fence is not a red schedule:
  `refuseUnlessAdmitted` already keeps the lane closed in the `catch` block.

### 5.4 Not closed by story C {#story-c-residual}

- A `Live -> Removing` request from the old mount generation can land after a remount. Closing that needs the
  mount generation in the catalog write, which is a format change. Placement: CAS-454, which also records
  the format decision (CAS-367 criterion 3).
- Readers see a closed lane as "absent" while the row is `Live`. Placement: CAS-183.
- The wait for `removal_attempt_active` has no deadline. It ends when the other attempt ends. Story D bounds
  that attempt's object-store work up to the removal decision. It does not bound its local waits, its
  recovery or its terminal append ([§6.2](#story-d-change)). Placement: CAS-462.
- CAS-367 criterion 2 (a detector for a namespace stuck in `Removing`) is met at the base:
  `Gc::reportStuckRemovals` and `CASGCStuckRemovals`.

## 6. Story D: one frozen retry policy for a namespace drop (CAS-69) {#story-d}

### 6.1 Problem {#story-d-problem}

`dropNamespaceImpl` gives every read its own `Retry::standard` window. The worst case is the creator-fence
read. `cancelStalledCreating` calls its predicate inside `casUpdateImpl`'s `decide`, and the predicate calls
`isCreatorFenceTerminal` with no policy. That nested read gets a fresh 90 s window on every `decide`, so it
can outlast the window `casUpdateImpl` froze for the update, while the catalog hot-key lane is held.

`resolveNamespaceLife` already does this right: `const Retry policy = op.freeze(Retry::standard());`.

### 6.2 Change {#story-d-change}

- `dropNamespaceImpl` starts with `const Retry policy = initial_op.freeze(Retry::standard());`.
- `policy` goes to every read and write that leads to the removal decision: the first `CasRefCatalog::read`,
  `cancelStalledCreating`, the creator-fence read inside its predicate, the second `CasRefCatalog::read`, the
  `gc/state` read and `beginRemoving`.
- `CasRefCatalog::cancelStalledCreating` and `CasRefCatalog::beginRemoving` gain a trailing parameter
  `const Retry & policy = Retry::standard()`, as `CasRefCatalog::read` already has. They forward it to
  `casUpdateImpl`. No existing caller changes: the tests and the GC race fixture keep the default.
- `beginRemoving` passes the same `policy` to the catalog read in its mismatch branch
  (`catch (const CatalogEntryMismatchMarker &)`). That read decides between `AlreadyRemoving` and
  `EntryChanged`, so it is part of the removal decision.
- **The resolution read in the `catch` block keeps its own standard window.** Its job is to reopen the lane.
  Under an expired policy it would fail at once and leave a healthy table closed.
- **Local waits count against the window but are not cut short by it.** The drop waits for the lane to drain
  and, after story C, for another removal attempt. If such a wait outlasts the window, the next read gives
  up at once, nothing was sent, the resolution read finds the row `Live`, and the lane reopens. The drop ends
  with a retry-later error.

Bound after this story: the object-store work that leads to the removal decision ends at one deadline, set
at entry. That work is both catalog reads, the creator-fence read, the `gc/state` read and both catalog
updates, including the mismatch read inside `beginRemoving`.

Four parts of a drop keep their existing budgets, with the work nested in them:

| Part | Budget | Why it stays |
|---|---|---|
| the resolution read in the `catch` block | one standard window | see above |
| recovery (`ensureRefTableRecovered`) | `recovery_retry_budget_ms` bounds the outer retry loop; one walk inside it runs its own requests to the end | shared with table load |
| the terminal removal append (`appendRefOpsOnRuntime`) | `Retry::standard` for its catalog read and its `create`; the default policy of `publishCkpt` for a birth checkpoint and the committed frontier; `Retry::once` for a wedge resolution | it runs after `Removing` is durable; cutting it short can leave a `Removing` row with no terminal record |
| the wait for pending snapshot publishers after the append (`publish_settle_cv`) | no deadline | no publisher may carry the old runtime across the catalog deletion |

The append can also fail after its log record is durable, during the frontier publication. The drop then
ends with an error over a durable terminal record. Placement for the waits without a deadline: CAS-462.

### 6.3 Tests {#story-d-tests}

Pattern: `CASRefCatalogBirthWiring.APerpetuallyChurningCatalogEntryIsPacedAndEndsWithinOneWindow`
(`VirtualRetryClock::installOn`, a backend that fails chosen keys with a retryable error).

| Id | Test | Red at the base |
|---|---|---|
| D1 | `CASRefWriterNamespaceRemoval.DropWithASlowCreatorFenceReadEndsAtTheEntryDeadline`: a `Creating` row with a foreign creator; the first catalog read costs 60 virtual seconds; every read of the mount key then fails retryably; `dropNamespace` throws and virtual time from entry is at most one window plus one backoff | yes, 60 s plus a full window |
| D2 | `CASRefWriterNamespaceRemoval.DropSpendsOneWindowAcrossItsReads`: the first catalog read costs 60 virtual seconds; the `gc/state` read then fails retryably; total virtual time is at most one window plus one backoff | yes, 60 s plus a full window |
| D3 | `CASRefWriterNamespaceRemoval.AWaitLongerThanTheWindowEndsTheDropAndLeavesTheLaneOpen`: the removal-attempt hook advances the virtual clock past the window; `dropNamespace` throws retry-later; the next ref mutation is admitted | no at the base (the drop succeeds); green after |

D3 documents the new failure mode of [§6.2](#story-d-change) and guards the resolution read's own window.

D1 spends time before the creator-fence read on purpose. A mount read that fails from the start ends after
one window at the base too, because its exception propagates out of `decide`.

### 6.4 Not closed by story D {#story-d-residual}

- `dropNamespaceImpl` takes no deadline from its caller. "The caller's deadline" in CAS-69 criterion 2 is
  the window frozen at entry. No task asks for a query deadline on `DROP`. CAS-71.1 is a different change: a
  clamp deadline on `CasOperation` for reads nested in a lane hold.
- CAS-69 criterion 1 says "every read it issues". The story meets it for the reads that lead to the
  removal decision. One read `dropNamespaceImpl` issues itself is excluded on purpose: the resolution read
  in the `catch` block keeps a fresh standard window. Recovery and the terminal append are transitive and
  keep their own budgets ([§6.2](#story-d-change)).

## 7. Story E: three small robustness fixes (CAS-373) {#story-e}

### 7.1 A fenced writer's checkpoint merge rejection is `FencedOut` {#story-e-ckpt}

- **Problem.** `publishCkpt`'s `decide` (`CA/Pool/CasRefCkpt.cpp`) calls `mergeCkpt`. `mergeCommittedThrough`
  and `checkRefCkptInvariants` throw `CORRUPTED_DATA` when the contribution does not fit the durable body. A
  writer that already lost admission reaches that with a stale contribution. It reports corruption where
  nothing is corrupt.
- **Change.** `decide` catches `CORRUPTED_DATA` from `mergeCkpt`. If `!op.admitted()` it declines with
  `Decline::Fenced` and `publishCkpt` returns `FencedOut`. An admitted writer still gets `CORRUPTED_DATA`. The
  header comment in `CA/Pool/CasRefCkpt.h` says so.
- **Tests** in `src/Disks/tests/gtest_cas_ref_ckpt_join.cpp`, suite `CASRefCheckpointJoin`. The draft's bodies
  are refused as `Superseded` after PR1, so the tests use bodies that pass `refuseFrontierAgainstDurable`:

| Id | Test | Durable `_ckpt` | Contribution | Red at the base |
|---|---|---|---|---|
| E1 | `NonAdjacentFrontierFromAFencedOutWriterIsReportedFencedOutNotCorruption` | `committed_through=(10,1)`, `last_epoch_seal=(9,5)` | `committed_through=(7,9)`, `last_epoch_seal=(7,9)` | yes, `CORRUPTED_DATA` |
| E2 | `SealInvariantBreakFromAFencedOutWriterIsReportedFencedOutNotCorruption` | `committed_through=(5,10)`, `last_epoch_seal=(5,10)` | `committed_through=(5,12)` | yes |
| E3 | `NonAdjacentFrontierFromAnAdmittedWriterIsCorruption` | as E1 | as E1 | no, guards the admitted path |

### 7.2 A vanished pool meta is `INVALID_STATE` {#story-e-poolmeta}

- **Problem.** `rmwPoolMeta` (`CA/Pool/CasPoolMeta.cpp`) throws `LOGICAL_ERROR` "CAS pool meta '{}' vanished
  during {}" when the object disappears between its read and its write. An object removed from the bucket
  is an environment fault, not a program bug. `LOGICAL_ERROR` aborts debug and sanitizer builds.
- **Change.** Throw `INVALID_STATE` with the same text. Fix the comment that says the object "MUST be
  present". Callers: `admitOrValidate` and `PoolMeta::raiseMinReaderGeneration`.
- **Test** `CASPoolMeta.PoolMetaVanishingMidAdmissionIsInvalidStateNotLogicalError` in
  `src/Disks/tests/gtest_cas_pool_meta.cpp`. Red at the base: yes; it is a death in debug builds.

### 7.3 The bucket versioning probe is one bounded attempt {#story-e-probe}

- **Problem.** `S3ObjectStorage::isBucketVersioningEnabled` sends `GetBucketVersioning` through the default
  client. `checkPoolPreconditions` calls it from `Pool::open` on a GCS mount (`mode == Native` with generation
  tokens). It is the only control-plane request of the mount that ignores the mount's retry profile: the
  SDK retries it with its own backoff, outside the CAS engine's deadline.
- **Change.**
  - `IObjectStorage::isBucketVersioningEnabled` takes `const ObjectStorageControlRequest &`.
  - `S3ObjectStorage::isBucketVersioningEnabled` uses
    `getSingleAttemptClient(request.attempt_timeout_ms, request.connect_timeout_cap_ms,
    /*disable_adaptive_timeouts=*/ true)`, as `objectCreationClient` does. `clientForRetryProfile` alone would
    keep adaptive timeouts, whose first attempt waits 500 ms for a `GET` response. A slow answer would become
    "unknown".
  - `CasObjectStorageBackend` passes `controlRequest(0)`.
  - "Bounded" has the meaning it has for every single-attempt request: the SDK does not retry, and
    `attempt_timeout_ms` bounds each socket send and receive. It is not a total deadline.
  - `docs/en/antalya/cas/bucket-requirements.md` says the probe is one attempt and what "unknown" means.
- **Tests.**

| Id | Test | Red at the base |
|---|---|---|
| E4 | `CASBackendGeneration.CheckPoolPreconditionsProbesVersioningSingleAttemptAndBounded` (`gtest_cas_backend_generation.cpp`) | yes, the probe carries no request profile |
| E5 | `CASEnvelopeWiring.BucketVersioningProbeSingleAttemptOutlastsTheFirstAttemptTimeouts` (`gtest_cas_s3_single_attempt_client.cpp`) | yes |

The two test overrides of `isBucketVersioningEnabled` (`gtest_cas_s3_staging.cpp`,
`gtest_cas_backend_generation.cpp`) take the new parameter.

## 8. Fixed premises {#premises}

Decided by the owner before this document. Not open for the spec review:

- Story B uses `Retry::once` and no concurrency cap.
- Story C closes the lane until a completed retry or a remount. The format change that would bind the write
  to a mount generation is deferred to CAS-454.
- Story E keeps the signature change of `isBucketVersioningEnabled`.
- No port to `antalya-26.6`.
- The branch is based on the final PR1 head.

## 9. Commit series {#commits}

| # | Story | Subject | Files | Behaviour |
|---|---|---|---|---|
| 1 | A | Compare writer epochs before trusting a mount slot's death certificate | `CA/Pool/CasServerRoot.{h,cpp}`, `gtest_cas_mount.cpp` | changes |
| 2 | B | Track which mount slot body the lease renewer owns | `CA/Pool/CasServerRoot.{h,cpp}`, `CA/Pool/CasMountRuntime.{h,cpp}`, `gtest_cas_mount.cpp` | none; new unused state |
| 3 | B | Read the mount slot before a relink confirm answers yes | `CA/Pool/CasPool.{h,cpp}`, `CA/Pool/CasMountRuntime.{h,cpp}`, `CA/Pool/CasRefLedger.{h,cpp}` (comments), `CA/ContentAddressedExchange.h`, `CA/ContentAddressedMetadataStorage.{h,cpp}` (comments), `src/Common/ProfileEvents.cpp`, `docs/en/antalya/metrics_and_events.md`, `gtest_cas_confirm_exact_ref.cpp`, `tests/integration/test_cas_gcs_relink_liveness/test.py` | changes |
| 4 | B | Document the mount slot read of the relink confirm | `docs/en/antalya/cas/architecture/replication.md`, `manifests-and-refs.md` | none |
| 5 | C | Keep a namespace's write lane closed after an unresolved removal write | `CA/Pool/CasRefLedger.{h,cpp}`, `CA/Pool/CasPool.h`, `gtest_cas_ref_writer.cpp`, `docs/en/antalya/cas/architecture/namespaces.md` | changes |
| 6 | D | Freeze one retry policy for the whole namespace drop | `CA/Pool/CasRefLedger.cpp`, `CA/Pool/CasRefCatalog.{h,cpp}`, `gtest_cas_ref_writer.cpp` | changes |
| 7 | E | Decline a checkpoint merge rejection as fenced for a writer that lost admission | `CA/Pool/CasRefCkpt.{h,cpp}`, `gtest_cas_ref_ckpt_join.cpp` | changes |
| 8 | E | Report a vanished pool metadata object as an invalid state | `CA/Pool/CasPoolMeta.cpp`, `gtest_cas_pool_meta.cpp` | changes |
| 9 | E | Probe bucket versioning with one attempt at the full attempt timeout | `IObjectStorage.h`, `S3/S3ObjectStorage.{h,cpp}`, `CA/Backend/CasObjectStorageBackend.cpp`, three gtest files, `docs/en/antalya/cas/bucket-requirements.md` | changes |

Rules:

- **Bisectable.** Every commit builds `unit_tests_dbms` and passes `CAS*`.
- **Red first.** Each test marked "yes" is run red before its product change. The log goes to
  `master/tmp/prbundle/pr2/logs/red_<commit#>_<story>.log`.
- **A red run compiles.** New symbols get a bare declaration or a stub first. A compile failure is not red
  evidence.
- **Mutation checks run uncommitted** and under a memory cap. Each story names at least one: A, drop the
  epoch comparison; B, skip the `holds` check; C, clear the local on `FencedOut`, and clear it on
  `EntryChanged`; D, pass no policy to the creator-fence read; E, drop the `!op.admitted()` condition.
- **`LOGICAL_ERROR` expectations are death tests** under `DEBUG_OR_SANITIZER_BUILD`.
- **Commit 9 is prepared as a diff and reviewed by a second model before it is committed.** It touches a
  shared interface.
- Commits are signed off and carry the co-author trailer. No amend, no rebase.

## 10. Production code outside `CA/` {#upstream}

| File | Change | Why it cannot live in `CA/` |
|---|---|---|
| `src/Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h` | one parameter on `isBucketVersioningEnabled`; the default still returns `std::nullopt` | the probe is a virtual of the object storage |
| `src/Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.{h,cpp}` | the probe picks its client from the request | the client cache is private to `S3ObjectStorage` |
| `src/Common/ProfileEvents.cpp` | two events next to the existing `CASRelinkConfirmRefused*` block | all events are declared there |

- `isBucketVersioningEnabled` has one production caller (`CasObjectStorageBackend`) and one implementation
  (`S3ObjectStorage`). `CachedObjectStorage` and the other storages inherit the default.
- `IObjectStorage.h` already differs from upstream on this branch and already takes `ObjectStorageControlRequest`
  (declared in `src/IO/ObjectStorageRequestProfile.h`) in four methods.
- No hunk changes behaviour for a disk that is not CAS.
- Tests and docs outside `CA/` are not in this table. [§9](#commits) lists them per commit: eight gtest files
  under `src/Disks/tests/` and one integration module. [§14](#documentation) lists the five doc files.
- Story D changes no caller outside `CA/`: the two new parameters have defaults ([§6.2](#story-d-change)).

## 11. Gates {#gates}

| Gate | When | Command or tree |
|---|---|---|
| Release `CAS*` | every commit | `build/src/unit_tests_dbms --gtest_filter='CAS*'` |
| ASan and UBSan `CAS*` | final head | `build_asan`, same filter |
| Fast-test compile | final head | `-DENABLE_LIBRARIES=0 -DENABLE_TESTS=0`, objects of every product file the branch touches, including `S3ObjectStorage.cpp` in a build with S3 |
| Stateless `*_cas_*` | final head | standalone server, 58 tests |
| Integration | final head | `test_cas_replicated_relink`, `test_cas_gcs_relink_liveness` |
| Model | final head | `docs/superpowers/models/run_relinkconfirm.sh CaRelinkConfirmCore_cur_staleyes` red, `..._fix_a` green |
| Style | final head | the functions of `ci/jobs/check_style.py` |
| Whole-branch review | final head | one Opus review, one codex review, one umbrella review |

Every red is root-caused or gets a task.

## 12. Measurements before and after {#measurements}

**Question.** Does the branch slow the common paths it touches: table create and drop, ref cleanup, relink
fetch?

**Setup.** As for PR1: one standalone server on local object storage, port 19781, `cas_gc_interval_sec` 20,
15 min of load and 2 min idle. "Before" is the final PR1 binary. "After" is the final PR2 binary.

| Run | Binary | Workload | Source |
|---|---|---|---|
| R2 | PR1 final | inserts plus table churn | the PR1 "after" run |
| R4 | PR1 final | inserts only | the PR1 "after" run |
| S2 | PR2 final | inserts plus table churn | new |
| S4 | PR2 final | inserts only | new |

**Sampled.** Everything PR1 samples, plus `DROP TABLE` latency (p50, p99 and count from `system.query_log`)
and the two new counters. `run.sh` gets the latency export before R2 runs.

**Relink cost.** The two integration modules of [§11](#gates) run on both binaries. Compared: wall time of
each module, and on the sender the counters `CASRelinkConfirmRefusedSlotMoved` and
`CASRelinkConfirmRefusedSlotUnreadable`. Test B14 proves the request count: one `GET` per final `Yes`.

**Predictions, written before the runs.**

- P1. Churn tables created in S2 are within 10 % of R2.
- P2. `DROP TABLE` p50 and p99 in S2 are within 15 % of R2.
- P3. Ref objects deleted during load and `_log` objects at load end are within 10 % of R2 and R4.
- P4. `CASRelinkConfirmRefusedSlotUnreadable` is 0 in every run and in both integration modules.
  `CASRelinkConfirmRefusedSlotMoved` is 0 in the standalone runs, which do no relink. In the integration
  modules it is reported next to the number of confirms. The counter does not attribute a refusal to a
  renewal, so the runs give a rate and no per-refusal cause. They cannot tell the race of
  [§4.2](#story-b-change) from a defect in the slot rule: B1 to B20 are the evidence for the rule. The
  measured rate goes to CAS-461.
- P5. No "outcome of its Live -> Removing catalog write is unknown" warning appears on local object storage.
- P6. S2 and S4 have no `CA GC round failed` line that R2 and R4 lack.
- P7. Wall time of each integration module is within 15 %.

A failed prediction is a finding. It gets a root cause before the branch is called ready.

**Output.** `docs/superpowers/reports/2026-11-11-cas-pr2-late-request-audit-bundle-measurements.md`.

## 13. Models {#models}

- **Story B.** `CaRelinkConfirmCore` covers it. The model gate of [§11](#gates) re-runs two configs so the
  evidence is current. The implementation must match fix A: ETag equality, or the in-flight attempt id.
  The model's `SSlotRead` reads the slot and checks ownership in one atomic step. It covers the ownership
  predicate, not the split between the `GET` and the `holds` check. That split only turns a `Yes` into
  `Unknown` ([§4.2](#story-b-change)); test B20 pins it. The model is not extended.
- **Stories A, C, D, E.** No model. A is a pure function with a five-row table. C and D are covered by
  deterministic tests with a virtual clock and held-back writes. If the spec review finds an interleaving
  in C that the tests cannot pin, a small model of the lane flag is added before implementation.

## 14. Documentation {#documentation}

| File | Change | Commit |
|---|---|---|
| `docs/en/antalya/metrics_and_events.md` | two events | 3 |
| `docs/en/antalya/cas/architecture/replication.md` | the confirm step in the sequence diagram reads the mount slot | 4 |
| `docs/en/antalya/cas/architecture/manifests-and-refs.md` | the "zero object-store I/O" sentence | 4 |
| `docs/en/antalya/cas/architecture/namespaces.md` | the closed lane after an unresolved removal write, and how it reopens | 5 |
| `docs/en/antalya/cas/bucket-requirements.md` | the versioning probe | 9 |

User docs use plain words: no task ids and no invariant tags.

## 15. Acceptance mapping {#acceptance}

| Criterion | Met by |
|---|---|
| CAS-364 #1: `claimMount` refuses an epoch not greater than the slot's | the base: `CASClaimMountMonotonicity.*` |
| CAS-364 #2: `SupersededEpoch` requires a strictly greater epoch | A1, A2 |
| CAS-365 #1: the model fails without the slot read and holds with it | the model gate |
| CAS-365 #2: confirm refuses after a foreign claim while the local fence says live | B7 |
| CAS-367 #1: an ambiguous `beginRemoving` never reopens the lane | C1 to C4, C6 |
| CAS-455 #1: a `beginRemoving` that fails before any write is sent reopens the lane; one that sent a write keeps it closed | C5, C6 |
| CAS-367 #2: a stuck-`Removing` detector exists | the base: `Gc::reportStuckRemovals` |
| CAS-367 #3: the format decision is recorded | CAS-454 |
| CAS-69 #1: "`dropNamespaceImpl` freezes one policy at entry and every read it issues, including the creator-fence read, uses that policy" | D1, D2; the direct exception (the resolution read) and the transitive ones are in [§6.4](#story-d-residual) |
| CAS-69 #2: "A gtest with a slow creator-fence read shows the drop gives up at the caller's deadline, not at a fresh 90 s window" | D1 |
| CAS-373: each item has a unit test | E1 to E5, the pool meta test |

## 16. Non-goals and where they go {#non-goals}

| Not done here | Placement |
|---|---|
| Name the slot's epoch in the creator-fence refusal | CAS-453 |
| Bind the `Live -> Removing` write to the mount generation | CAS-454 |
| `resolveRef` reports "absent" during the closed-lane window | CAS-183 |
| A query deadline for the drop | none; no task asks for one |
| A deadline for the drop's local waits | CAS-462 |
| A second slot read after a refusal that a concurrent renewal explains | CAS-461 |
| A cap or cache for the confirm's slot read | none; the owner ruled it out |

## 17. Risks {#risks}

- **The confirm's `GET` slows relink fetches.** One request per fetched part on the sender. Check: P7 and
  test B14.
- **A slow mount read turns confirms into `Unknown`.** The receiver falls back as it does for any `Unknown`.
  Check: `CASRelinkConfirmRefusedSlotUnreadable` is documented and P4 holds in the integration modules.
- **A renewal that races the confirm refuses a healthy mount.** The window runs from the served `GET` to
  the `holds` check and has no fixed bound ([§4.2](#story-b-change)). Check: B20, the rate reported under
  P4, CAS-461.
- **Story C keeps a healthy table closed.** It needs a `beginRemoving` that sent a catalog write and ended
  without a known outcome: a lost answer, or a refusal or a changed row after the send (C6). A failure with
  nothing sent reopens the lane (C5). Check: P5, the warning text, and `namespaces.md` names both ways out.
- **Story D fails a drop that used to succeed late.** It needs a drain longer than 90 s. Check: D3 shows the
  lane reopens and the error is retry-later.
- **Story C and PR 2517.** If an expired lease becomes a pause instead of a remount, "until remount" means
  "until restart or a completed retry". Check: the docs name the completed retry first.
- **Base moves.** PR1 or PR 2805 can still change. The branch then merges the new base. No rebase.
- **The interface change conflicts upstream.** One parameter on one virtual. Check: it is the last commit
  and [§10](#upstream) lists every hunk.

## 18. Left to the builder {#builder}

- The names of private helpers and of test-local backends.
- Whether `slotHeldBy` returns a `bool` or a small enum, as long as the two counters stay distinct.
- The exact wording of comments and docs, within the facts of this document.
- The stub used for a red run of a new symbol.
