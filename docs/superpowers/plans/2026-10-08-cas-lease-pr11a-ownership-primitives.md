# CAS lease PR 11a: ownership primitives with typed outcomes — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:executing-plans (or
> superpowers:subagent-driven-development). Steps use checkbox (`- [ ]`) syntax.

**Goal:** the owner claim, the epoch allocation and the ownership catalog read return typed outcomes
instead of exception text; only startup may create the owner anchor and the epoch counter; the epoch
allocation is one immutable conditional write, and "counter advanced by another process" repeats at
startup and stops a reacquire. Startup behaviour is unchanged (golden tests). The one live change:
the self-remount (`Pool::tryRemountOnce`) no longer recreates a missing owner anchor or counter.

**Architecture:** typed results in `$CA/Pool/CasServerRoot.h` plus a small header for the shared
failure types and `CatalogObservation`. Every failure alternative carries the exception a startup
caller throws, so `claimOwnerOrThrow` and `allocateWriterEpoch` keep their signatures, codes and texts
as one-line wrappers. `allocateEpoch` replaces `readModifyWrite` with `replace`/`create` of fixed bytes
`{N + 1}` under one frozen deadline; the engine already settles an unclear attempt by our exact bytes.

**Tech stack:** C++23, gtest (`unit_tests_dbms`), `InMemoryBackend` seams, injected clocks; TLA+.

**Spec:** `/home/mfilimonov/workspace/ClickHouse/master/docs/superpowers/specs/2026-10-07-cas-mount-lease-series-design.md`
— section 7 row 11a (both tables), section 2 "Старт" (the PR 11a sentence), R19, R21, R22, section 9
row "`CaCasMountCore`, шаг 2a"; owner decisions of 2026-10-08 in
`/home/mfilimonov/workspace/ClickHouse/master/tmp/lease-series/spec-r8-todo.md` (PR 6 withdrawn).

**Base:** `ea46aa00861` (`lease/antalya-26.6/base-pr1-4-7-8-9a`). **Branch:**
`fix/antalya-26.6/cas-ownership-primitives`. `$CA` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed`.
Files outside `$CA`: only `src/Disks/tests/gtest_cas_ownership_primitives.cpp` (new). No format
change. The model is committed in the master worktree (`/home/mfilimonov/workspace/ClickHouse/master`,
branch `cas-gc-rebuild`). Every commit message ends with exactly, in this order:

```
Signed-off-by: Mikhail Filimonov <mfilimonov@altinity.com>
Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
```

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
- Every test and gate command runs with `TMPDIR=<build_dir>/tmp`.

## Review Focus

- **Goldens pass on the base unchanged** (Task 2 runs before any production edit). Editing a golden
  after Task 3 means behaviour changed.
- **Failure asymmetry of `Reacquire`.** Over-refusing a harmless advance costs one failed remount
  attempt now, one restart after PR 14. Under-refusing lets a process that lost its slot keep minting
  epochs beside a second process of the same `server_root_id` — the split brain the stop surfaces.
  So every unclear `Reacquire` branch is terminal.
- **Runner classification** (`runOwnershipStep`): inside a step every refusal is returned, so an
  escaping `CORRUPTED_DATA` is a decode failure → `OwnershipCorrupt`; `LOGICAL_ERROR`,
  `NOT_IMPLEMENTED`, `BAD_ARGUMENTS`, `ABORTED` propagate; any other `DB::Exception` → transient. Check
  that no step still *throws* `CORRUPTED_DATA` for a refusal.
- **One deadline per allocation:** `op.freeze(Retry::standard())` once, used by every call of the loop.

## Code vs spec (flagged; the plan follows the spec)

1. R22 «чтение каталога возвращает `Ok | Transient | Corrupt`»; code: an absent catalog is
   `CORRUPTED_DATA` from one `GET` (`throwMandatoryCatalogAbsent`). With PR 6 withdrawn a terminal
   absence needs LIST corroboration (PR 12), so 11a keeps today's verdict tagged
   `OwnershipCorrupt::Cause::Absent`. Owner/counter absence stays today's one-`GET` 404
   (`OwnerLost{Absent}`, `EpochCounterLost`); a failed read is `OwnershipTransient` (`NotObserved`),
   never absence.
2. R22 «Примитивы старта возвращают типизированные исходы, не текст исключения»; code: the
   `DecommissionRecovery` refusal of a live member throws `ABORTED` inside `allocateWriterEpoch`. Kept
   as a throw: it is the decommission command's own error and `Reacquire` never reaches that branch.
3. R22 names `Replaced`, `IdentityLost`; code: they come from `probePoolLifecycleGate` (remount only),
   already typed as `LifecycleGateVerdict`. Nothing to do.
4. R19/R21 «остановка» under `Reacquire`; 11a returns the terminal outcome and its one caller,
   `tryRemountOnce`, reports a failed attempt (today's handling of any claim error). The stop is PR 14
   (the PR 12 row says the R16 sources from 11a are handled there).
5. R21 «Временный отказ любого шага … повторяется по R4 тем же кортежем»; 11a's `OwnershipTransient`
   carries no tuple, so a remount retry forms a new allocation. The step object is PR 14 (its test:
   «`503` в `allocateWriterEpoch` → повтор без второго номера»).
6. Model: `AllocResolvedByRead` adopts "the counter's current value without bumping", not our bytes
   `{N + 1}`. Step 2a adds the byte rule behind a gate.
7. Size: the row says ±60; expect about +170/−60 production.

Not covered: LIST corroboration (PR 12), the reacquire step object and certificate (PR 14), stopping
the process (PR 12/14). `Refused`/`GaveUp` allocations and the inconclusive mount probe are
`OwnershipTransient` carrying today's exception.

---

## Task 0: worktree

- [ ] If `/home/mfilimonov/workspace/ClickHouse/cas-95-1` is free (`git status --short` shows nothing
  outside `contrib/`, `tmp/`): `git -C /home/mfilimonov/workspace/ClickHouse/cas-95-1 switch -c fix/antalya-26.6/cas-ownership-primitives ea46aa00861`.
  Otherwise ask the controller; the fallback is `cd /home/mfilimonov/workspace/ClickHouse && bash create_subtree_branch.sh cas-ownership-primitives lease/antalya-26.6/base-pr1-4-7-8-9a`,
  then `git -C cas-ownership-primitives branch -m cas-ownership-primitives fix/antalya-26.6/cas-ownership-primitives`
  and `cmake -S cas-ownership-primitives -B cas-ownership-primitives/build_asan -DSANITIZE=address`.
  `$WT` below is that worktree; `mkdir -p $WT/build/tmp $WT/build_asan/tmp`.

## Task 1: model step 2a (before any code)

**Files (master worktree, `docs/superpowers/models/`):** `CaCasMountCore.tla`, every
`CaCasMountCore_*.cfg`, new `CaCasMountCore_{twins_immalloc,sab_reacquireredecides,sab_refreshownpair_immalloc,witness_samebytes,witness_reacquirestops}.cfg`,
`run_mount.sh`, `CaCasMountCore_RESULTS.md`.

- [ ] Constants `ImmutableAlloc` (gate) and `SabReacquireRedecides`; variables `allocPend`,
  `allocAmbig`, `allocStopped`, `reacqSawForeign` (functions on `Incs`) and `claimedAfterForeign`;
  `allocVars` (the five) joins `vars`; `Init`: `NoPend`/`FALSE`; `TypeOK`:
  `allocPend \in [Incs -> {NoPend} \cup 0..MaxEpoch]`, the rest Boolean.
- [ ] Existing actions: `AllocEpoch`, `AllocResolvedByRead` get `/\ ~ImmutableAlloc`; `RemintEpoch`,
  `RemintEpochDecom` get `/\ ~(ImmutableAlloc /\ ReacqMode(a))`; `ClaimMount` gets
  `/\ allocPend[a] = NoPend /\ ~allocStopped[a]`,
  `/\ claimedAfterForeign' = (claimedAfterForeign \/ reacqSawForeign[a])` and
  `UNCHANGED << allocPend, allocAmbig, allocStopped, reacqSawForeign >>`; `Die(a)` resets the four
  per-incarnation variables at `a` (a restart is a new process); every other action gets
  `/\ UNCHANGED allocVars`. With the gate FALSE every existing state space is unchanged.
- [ ] New definitions; add `AllocBegin`, `AllocAttempt`, `AllocResolve` to `Next`:

```tla
NoPend == -1
\* Lost its slot to a GC fence and still running: it allocates as a reacquire.
ReacqMode(a) == localLost[a] /\ ~crashed[a] /\ localEpoch[a] > 0
allocFrame == << owner, epochWiped, mount, mtoken, clock, localLost, crashed, rejected, adoptObs,
                 fencedEpochs, wedged, wrote, rootEmpty, firstOwner, lostThenWrote, reclaimed,
                 remountedAfterFence, fenceUntil, obsToken, obsSince, observedReclaimEver,
                 supersededThenWrote, rgVars, epochRegressed, bodyWriter >>
\* One immutable conditional write: the expected value and the bytes `allocPend[a] + 1` are fixed here.
AllocBegin(a) ==
    /\ ImmutableAlloc /\ ~rejected[a] /\ ~wedged[a] /\ ~allocStopped[a] /\ allocPend[a] = NoPend
    /\ owner = Uuid(a) /\ epoch < MaxEpoch /\ (epoch > 0 \/ epochCeiling = 0)
    /\ ~(mount # None /\ mount.uuid = Uuid(a) /\ ~mount.fenced /\ OwnBody(a))
    /\ allocPend' = [allocPend EXCEPT ![a] = epoch]
    /\ UNCHANGED << epoch, epochCeiling, localEpoch, allocAmbig, allocStopped, reacqSawForeign,
                    claimedAfterForeign, allocFrame >>
\* Our bytes after an unclear attempt are ours, even when another process that read the same value
\* wrote them. The expected value: reissue. Anything else: a start re-decides, a reacquire stops.
AllocSettle(a, ambiguous) ==
    IF ambiguous /\ epoch = allocPend[a] + 1 THEN
        /\ localEpoch' = [localEpoch EXCEPT ![a] = allocPend[a] + 1]
        /\ allocPend' = [allocPend EXCEPT ![a] = NoPend]
        /\ allocAmbig' = [allocAmbig EXCEPT ![a] = FALSE]
        /\ UNCHANGED << allocStopped, reacqSawForeign >>
    ELSE IF ambiguous /\ epoch = allocPend[a] THEN
        /\ allocAmbig' = [allocAmbig EXCEPT ![a] = FALSE]
        /\ UNCHANGED << localEpoch, allocPend, allocStopped, reacqSawForeign >>
    ELSE
        /\ allocPend' = [allocPend EXCEPT ![a] = NoPend]
        /\ allocAmbig' = [allocAmbig EXCEPT ![a] = FALSE]
        /\ UNCHANGED localEpoch
        /\ IF ReacqMode(a)
           THEN /\ reacqSawForeign' = [reacqSawForeign EXCEPT ![a] = TRUE]
                /\ allocStopped' = IF SabReacquireRedecides THEN allocStopped
                                   ELSE [allocStopped EXCEPT ![a] = TRUE]
           ELSE UNCHANGED << allocStopped, reacqSawForeign >>
\* Applied and answered; applied, answer lost; lost before applying; refused cleanly.
AllocAttempt(a) ==
    /\ ImmutableAlloc /\ allocPend[a] # NoPend /\ ~allocAmbig[a]
    /\ \/ /\ epoch = allocPend[a] /\ epoch < MaxEpoch
          /\ epoch' = epoch + 1
          /\ epochCeiling' = IF epoch + 1 > epochCeiling THEN epoch + 1 ELSE epochCeiling
          /\ \/ /\ localEpoch' = [localEpoch EXCEPT ![a] = allocPend[a] + 1]
                /\ allocPend' = [allocPend EXCEPT ![a] = NoPend]
                /\ UNCHANGED allocAmbig
             \/ /\ allocAmbig' = [allocAmbig EXCEPT ![a] = TRUE]
                /\ UNCHANGED << localEpoch, allocPend >>
          /\ UNCHANGED << allocStopped, reacqSawForeign >>
       \/ /\ allocAmbig' = [allocAmbig EXCEPT ![a] = TRUE]
          /\ UNCHANGED << epoch, epochCeiling, localEpoch, allocPend, allocStopped, reacqSawForeign >>
       \/ /\ epoch # allocPend[a] /\ AllocSettle(a, FALSE) /\ UNCHANGED << epoch, epochCeiling >>
    /\ UNCHANGED << claimedAfterForeign, allocFrame >>
AllocResolve(a) ==
    /\ ImmutableAlloc /\ allocAmbig[a] /\ AllocSettle(a, TRUE)
    /\ UNCHANGED << epoch, epochCeiling, claimedAfterForeign, allocFrame >>
\* A reacquire that saw another process move the counter never claims the slot again.
ReacquireStopsOnForeignAdvance == ~claimedAfterForeign
W_SameBytesCollision ==
    ~\E i, j \in Incs : /\ i # j /\ Uuid(i) = Uuid(j) /\ ~crashed[i] /\ ~crashed[j]
                       /\ localEpoch[i] > 0 /\ localEpoch[i] = localEpoch[j]
W_ReacquireStops == ~\E a \in Incs : allocStopped[a]
```

- [ ] Configs: add `ImmutableAlloc = FALSE` and `SabReacquireRedecides = FALSE` to every existing
  `CaCasMountCore_*.cfg`. Each new cfg copies `CaCasMountCore_twins.cfg`, sets `ImmutableAlloc = TRUE`
  and states its expectation in the header: `twins_immalloc` (adds `INVARIANT
  ReacquireStopsOnForeignAdvance`; green); `sab_reacquireredecides` (`SabReacquireRedecides = TRUE`;
  `TypeOK`, `ReacquireStopsOnForeignAdvance`); `sab_refreshownpair_immalloc` (`SabRefreshOwnPair =
  TRUE`; `TypeOK`, `ClaimNeverRegressesEpoch`); `witness_samebytes` (`W_SameBytesCollision`);
  `witness_reacquirestops` (`W_ReacquireStops`). Add the five rows (green / violation + invariant) to
  `CONFIGS` and to the header list of `run_mount.sh`.
- [ ] `cd docs/superpowers/models && ./run_mount.sh > ../../../tmp/run_mount_pr11a.log 2>&1`.
  Expected: every row as listed, and every pre-existing row with the distinct-state count
  `CaCasMountCore_RESULTS.md` records for it. Any other colour: stop and report the trace; do not
  bend the model to fit. Expected red of `sab_reacquireredecides`: a fenced incarnation's allocation
  is refused cleanly after its twin's, it re-decides, allocates and reclaims its own fenced body.
- [ ] Append a "2026-10-08 step 2a" section to `CaCasMountCore_RESULTS.md`: the five rows with states
  and depth, each counterexample in one line, and the note that a collision's exclusivity is
  `ClaimMount`'s (strict monotonicity), not the counter's.
- [ ] Commit (master worktree), `git add --` the `.tla`, each cfg by name, `run_mount.sh`, the RESULTS
  file. Message: `model(cas): epoch allocation as one immutable write`.

## Task 2: golden tests of every start path (green on the base)

Start paths and their pins. New here: fresh root, restart, foreign/tombstoned owner, owner absent over
data, counter absent under a slot body, counter and slot absent, ambiguous catalog, foreign advance,
lost answer, own epoch fenced before the claim (`FencedSelf` loop). Already pinned, run in the golden
filter: unsafe open (`CASMountOpenWaits.UnsafeNoDelayOpensWithoutTheObservationWindow`), fence in the
adopt window (`CASPoolMountFence.OpenRecoversFromFenceInAdoptWindowWithFreshEpoch`), decommission
(`CASDecommission.RefusesLiveMember`, `.ClaimsDeadMemberAndBumpsEpoch`,
`.SuccessorReclaimAfterEpochDeleteKeepsOwnerAnchor`, and `CASMount.DecommissionRemint*`).

- [ ] Create `$WT/src/Disks/tests/gtest_cas_ownership_primitives.cpp`:

```cpp
#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCatalogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasServerRootFormats.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Poco/Exception.h>

namespace DB::ErrorCodes { extern const int CORRUPTED_DATA; }
using namespace DB::Cas;
using DB::Cas::tests::OperationForTest;

namespace
{
const String kSrid = "test";
const Layout kLayout("p");
const UInt128 kUuid(7);
using WriteAnswer = std::expected<String, RawConflict>;

PoolConfig config()
{
    PoolConfig c{.pool_prefix = "p", .server_root_id = kSrid};
    c.server_id = kUuid;
    c.retry_sleep_fn = [](uint64_t) {};
    return c;
}
std::optional<Object> get(Backend & b, const String & key) { OperationForTest op(b); return (*op).read(key, Retry::standard()); }
uint64_t counterNext(Backend & b) { return decodeServerEpoch(get(b, kLayout.epochKey(kSrid))->bytes).next_writer_epoch; }
void put(Backend & b, const String & key, const String & bytes)
{
    OperationForTest op(b);
    const auto got = (*op).read(key, Retry::standard());
    const WriteResult r = got ? (*op).replace(key, bytes, got->etag, Retry::standard()) : (*op).create(key, bytes, Retry::standard());
    ASSERT_TRUE(std::holds_alternative<Committed>(r)) << key;
}
void erase(Backend & b, const String & key) { OperationForTest op(b); ASSERT_EQ((*op).removeCurrent(key, Retry::standard()), Removal::Removed) << key; }
String ambiguousCatalog()
{
    const auto entry = [](const char * ns) { return CatalogEntry{.ns = RootNamespace{ns}, .state = NsState::Live, .incarnation = UInt128(42)}; };
    return encodeRefCatalog(RefCatalog{.entries = {entry("a"), entry("b")}});
}

/// Runs `hook` instead of the first write of the epoch counter after it is armed.
class EpochWriteHook : public InMemoryBackend
{
public:
    using Hook = std::function<WriteAnswer(InMemoryBackend &, const String &, const std::optional<String> &, TransportAccess &)>;
    WriteAnswer write(const String & key, const String & bytes, const std::optional<String> & expected, TransportAccess & access) override
    {
        if (!hook || key != kLayout.epochKey(kSrid))
            return InMemoryBackend::write(key, bytes, expected, access);
        return std::exchange(hook, {})(*this, bytes, expected, access);
    }
    Hook hook;
};
/// Another process that read the same value writes our very bytes first; our write is refused.
WriteAnswer raceHook(InMemoryBackend & b, const String & bytes, const std::optional<String> & expected, TransportAccess & access)
{
    b.InMemoryBackend::write(kLayout.epochKey(kSrid), bytes, expected, access);
    return std::unexpected(RawConflict{});
}
/// The write lands and its answer is lost.
WriteAnswer lostAnswerHook(InMemoryBackend & b, const String & bytes, const std::optional<String> & expected, TransportAccess & access)
{
    if (auto landed = b.InMemoryBackend::write(kLayout.epochKey(kSrid), bytes, expected, access); !landed)
        return landed;
    throw Poco::TimeoutException("injected: answer lost");
}
/// A GC fences the epoch this write allocates before our claim reaches the empty slot.
WriteAnswer fenceBeforeClaimHook(InMemoryBackend & b, const String & bytes, const std::optional<String> & expected, TransportAccess & access)
{
    WriteAnswer landed = b.InMemoryBackend::write(kLayout.epochKey(kSrid), bytes, expected, access);
    const MountLease fenced{.server_uuid = kUuid, .writer_epoch = decodeServerEpoch(bytes).next_writer_epoch - 1,
                            .gc_fenced = true, .write_attempt_id = UInt128(1)};
    b.InMemoryBackend::write(kLayout.mountKey(kSrid), encodeMountLease(fenced), std::nullopt, access);
    return landed;
}
/// A server root that ran once and closed: owner, counter `{2}`, a farewell body in the slot.
std::shared_ptr<EpochWriteHook> ranOnce() { auto b = std::make_shared<EpochWriteHook>(); Pool::open(b, config()).reset(); return b; }
std::shared_ptr<EpochWriteHook> seeded() { auto b = std::make_shared<EpochWriteHook>(); DB::Cas::tests::seedPoolMetaForRestart(*b); return b; }
void expectStartRefused(const BackendPtr & b, const String & text)
{
    DB::Cas::tests::expectThrowsCodeWithMessage(DB::ErrorCodes::CORRUPTED_DATA, text, [&] { Pool::open(b, config()); });
}
}

TEST(CASOwnershipStartGolden, FreshRootCreatesOwnerAndCounter)
{
    auto b = std::make_shared<EpochWriteHook>();
    EXPECT_EQ(Pool::open(b, config())->writerEpoch(), 1u);
    EXPECT_EQ(decodeOwner(get(*b, kLayout.ownerKey(kSrid))->bytes).server_uuid, kUuid);
    EXPECT_EQ(counterNext(*b), 2u);
}
TEST(CASOwnershipStartGolden, RestartKeepsOwnerAndTakesTheNextEpoch)
{
    auto b = ranOnce();
    const auto owner = get(*b, kLayout.ownerKey(kSrid));
    EXPECT_EQ(Pool::open(b, config())->writerEpoch(), 2u);
    EXPECT_EQ(get(*b, kLayout.ownerKey(kSrid))->etag, owner->etag);
    EXPECT_EQ(counterNext(*b), 3u);
}
TEST(CASOwnershipStartGolden, ForeignOrTombstonedOwnerRefusesWithoutCounterWrite)
{
    for (const auto & [owner, text] : {std::pair{OwnerObject{.server_uuid = UInt128(99)}, String("owned by a different server")},
                                       std::pair{OwnerObject{.server_uuid = kUuid, .retired_at_ms = 1752537600000ULL}, String("decommissioned")}})
    {
        auto b = seeded();
        put(*b, kLayout.ownerKey(kSrid), encodeOwner(owner));
        expectStartRefused(b, text);
        EXPECT_FALSE(get(*b, kLayout.epochKey(kSrid)));
    }
}
TEST(CASOwnershipStartGolden, OwnerAbsentOverDataRefusesWithoutCreatingIt)
{
    auto b = seeded();
    put(*b, kLayout.casManifestsServerPrefix(kSrid) + "t/debris", "x");
    expectStartRefused(b, "identity lost over existing data");
    EXPECT_FALSE(get(*b, kLayout.ownerKey(kSrid)));
}
TEST(CASOwnershipStartGolden, CounterAbsentUnderASlotBodyRefuses)
{
    auto b = ranOnce();
    erase(*b, kLayout.epochKey(kSrid));
    expectStartRefused(b, "refusing to re-mint epoch 1");
    EXPECT_FALSE(get(*b, kLayout.epochKey(kSrid)));
}
TEST(CASOwnershipStartGolden, CounterAndSlotAbsentOverAnEmptyRootMintOne)
{
    auto b = ranOnce();
    erase(*b, kLayout.epochKey(kSrid));
    erase(*b, kLayout.mountKey(kSrid));
    EXPECT_EQ(Pool::open(b, config())->writerEpoch(), 1u);
    EXPECT_EQ(counterNext(*b), 2u);
}
TEST(CASOwnershipStartGolden, AmbiguousCatalogRefusesBeforeAnyOwnershipWrite)
{
    auto b = seeded();
    put(*b, kLayout.refCatalogKey(), ambiguousCatalog());
    expectStartRefused(b, "refusing a decision from an ambiguous cut");
    EXPECT_FALSE(get(*b, kLayout.ownerKey(kSrid)));
    EXPECT_FALSE(get(*b, kLayout.epochKey(kSrid)));
}
TEST(CASOwnershipStartGolden, ForeignAdvanceAtStartupRepeatsWithTheNextValue)
{
    auto b = ranOnce();
    b->hook = raceHook;
    EXPECT_EQ(Pool::open(b, config())->writerEpoch(), 3u);   /// the other process took 2
    EXPECT_EQ(counterNext(*b), 4u);
}
TEST(CASOwnershipStartGolden, LostAnswerIsResolvedByOurBytesWithoutASecondNumber)
{
    auto b = ranOnce();
    b->hook = lostAnswerHook;
    EXPECT_EQ(Pool::open(b, config())->writerEpoch(), 2u);
    EXPECT_EQ(counterNext(*b), 3u);
}
TEST(CASOwnershipStartGolden, OwnEpochFencedBeforeClaimTakesAFreshOne)
{
    auto b = std::make_shared<EpochWriteHook>();
    b->hook = fenceBeforeClaimHook;
    EXPECT_EQ(Pool::open(b, config())->writerEpoch(), 2u);
    EXPECT_EQ(counterNext(*b), 3u);
}
```

- [ ] `cmake $WT/build > $WT/build/cmake_pr11a.log 2>&1` (a new test file), `ninja -C $WT/build unit_tests_dbms > $WT/build/ninja_pr11a_t2.log 2>&1`,
  then `TMPDIR=$WT/build/tmp $WT/build/src/unit_tests_dbms --gtest_filter='CASOwnershipStartGolden.*:CASMountOpenWaits.UnsafeNoDelay*:CASPoolMountFence.OpenRecoversFromFenceInAdoptWindow*:CASDecommission.RefusesLiveMember:CASDecommission.ClaimsDeadMember*:CASDecommission.SuccessorReclaimAfterEpochDelete*:CASMount.DecommissionRemint*' > $WT/build/test_pr11a_golden.log 2>&1`.
  Expected: all pass on the base. A golden failing here pins a wrong expectation: correct it to what
  the base does and report the difference.
- [ ] Commit `git add -- src/Disks/tests/gtest_cas_ownership_primitives.cpp`; message
  `Pin every CAS start path's ownership outcome in golden tests`.

## Task 3: the catalog read is typed

**Files:** create `$CA/Pool/CasOwnershipOutcome.h`; modify `$CA/Pool/CasRefCatalog.h`,
`$CA/Pool/CasRefCatalog.cpp`, `$CA/Pool/CasServerRoot.h`, `$CA/Pool/CasServerRoot.cpp`,
`$CA/Pool/CasPool.cpp`, the test file.

- [ ] Failing test (red on the base: `CasRefCatalog::observe` does not exist). Add to the anonymous
  namespace a fixture used by Tasks 3–5:

```cpp
/// An open-fence operation whose sleeps advance its own clock, so a bounded retry ends at once.
struct Plane
{
    explicit Plane(BackendPtr b) : requests(DB::Cas::tests::openRequestsForTest(std::move(b))), op(requests.admit())
    {
        requests.setNowFnForTest([this] { return clock; });
        requests.setSleepFnForTest([this](uint64_t ms) { clock += ms; });
    }
    uint64_t clock = 0;
    CasRequests requests;
    CasOperation op;
};
const ObserveRefCatalog kEmpty = [] { return RefCatalog{}; };
class FailingCatalogReads : public InMemoryBackend
{
public:
    using Backend::read;
    std::optional<Raw> read(const String & key, TransportAccess & access) override
    {
        if (fail && key == kLayout.refCatalogKey())
            throw Poco::TimeoutException("injected");
        return InMemoryBackend::read(key, access);
    }
    bool fail = false;
};
```

```cpp
TEST(CASOwnershipPrimitives, CatalogObservationIsTyped)
{
    auto b = std::make_shared<FailingCatalogReads>();
    Plane p(b);
    const auto cause = [&] { return std::get<OwnershipCorrupt>(CasRefCatalog::observe(p.op, kLayout, "t")).cause; };
    EXPECT_EQ(cause(), OwnershipCorrupt::Cause::Absent);
    put(*b, kLayout.refCatalogKey(), "garbage");
    EXPECT_EQ(cause(), OwnershipCorrupt::Cause::Undecodable);
    put(*b, kLayout.refCatalogKey(), ambiguousCatalog());
    EXPECT_EQ(cause(), OwnershipCorrupt::Cause::Ambiguous);
    put(*b, kLayout.refCatalogKey(), encodeRefCatalog(RefCatalog{}));
    EXPECT_TRUE(std::holds_alternative<RefCatalog>(CasRefCatalog::observe(p.op, kLayout, "t")));
    b->fail = true;
    EXPECT_TRUE(std::holds_alternative<OwnershipTransient>(CasRefCatalog::observe(p.op, kLayout, "t")));
}
```

- [ ] Build; expect a compile error naming `observe`.
- [ ] `CasOwnershipOutcome.h` (`#pragma once`; includes `Formats/CasRefCatalogFormat.h`, `<exception>`,
  `<variant>`):

```cpp
namespace DB::Cas
{
/// An ownership step the store did not answer; retry it. `error` is what a startup caller throws.
struct OwnershipTransient { std::exception_ptr error; };
/// An ownership object or the ref catalog was read and cannot be trusted. `error` is what a startup
/// caller throws. `Absent` is one read's 404, not yet corroborated by a listing.
struct OwnershipCorrupt
{
    enum class Cause : uint8_t { Undecodable, Absent, Ambiguous };
    Cause cause;
    std::exception_ptr error;
};
using CatalogObservation = std::variant<RefCatalog, OwnershipTransient, OwnershipCorrupt>;

inline RefCatalog catalogOrThrow(CatalogObservation && observation)
{
    if (auto * transient = std::get_if<OwnershipTransient>(&observation))
        std::rethrow_exception(transient->error);
    if (auto * corrupt = std::get_if<OwnershipCorrupt>(&observation))
        std::rethrow_exception(corrupt->error);
    return std::get<RefCatalog>(std::move(observation));
}
}
```

- [ ] `CasRefCatalog.cpp`: split `throwMandatoryCatalogAbsent` into `Exception mandatoryCatalogAbsent(const String & key)`
  (same code and text) and `throw mandatoryCatalogAbsent(key);` at its throw sites. Add
  `static CatalogObservation observe(CasOperation & op, const Layout & layout, std::string_view consumer);`
  to `CasRefCatalog` (header includes `CasOwnershipOutcome.h`; comment: "`read` for an ownership
  decision: a failed request is transient; an absent, undecodable or ambiguous catalog is corrupt").
  Body, in this order: (1) `op.read(layout.refCatalogKey(), Retry::standard())` in a `try`; a caught
  `Exception` with `isDeterministicLocalFailure(code)` is rethrown, any other is
  `OwnershipTransient{std::current_exception()}`; (2) nothing read →
  `OwnershipCorrupt{Cause::Absent, std::make_exception_ptr(mandatoryCatalogAbsent(key))}`;
  (3) `decodeRefCatalog` in a `try`; `LOGICAL_ERROR` rethrown, any other `Exception` →
  `OwnershipCorrupt{Cause::Undecodable, std::current_exception()}`; (4) `CatalogLifeIndex(catalog).hasAmbiguity()`
  → call `throwIfAmbiguous(consumer)` in a `try` and return `OwnershipCorrupt{Cause::Ambiguous,
  std::current_exception()}` from its `catch` (the same text the consumers print today); (5) the catalog.
- [ ] `CasServerRoot.h`: include `CasOwnershipOutcome.h`; `using ObserveRefCatalog =
  std::function<CatalogObservation()>;` (comment ends "… anything but a catalog ends the step with
  that outcome"). `CasServerRoot.cpp`: each `observe_catalog()` becomes `catalogOrThrow(observe_catalog())`
  (Tasks 4–5 replace them). `CasPool.cpp`: both observer lambdas become `CasOperation op =
  s->gc_requests.admit(); return CasRefCatalog::observe(op, s->pool_layout, "CAS server-root mount safety");`
  (remount: `this`, `"CAS server-root remount safety"`); both `(void)observe_catalog();` become
  `(void)catalogOrThrow(observe_catalog());`.
- [ ] Build; run `CASOwnership*:CASServerRoot*:CASMount*`; all pass.
- [ ] Commit `git add -- $CA/Pool/CasOwnershipOutcome.h $CA/Pool/CasRefCatalog.h $CA/Pool/CasRefCatalog.cpp $CA/Pool/CasServerRoot.h $CA/Pool/CasServerRoot.cpp $CA/Pool/CasPool.cpp src/Disks/tests/gtest_cas_ownership_primitives.cpp`;
  message `Return the CAS ownership catalog read as a typed observation`.

## Task 4: the owner claim is typed and has an authority

**Files:** `$CA/Pool/CasServerRoot.h`, `$CA/Pool/CasServerRoot.cpp`, the test file.

- [ ] Failing tests (red on the base: `claimOwner` does not exist):

```cpp
TEST(CASOwnershipPrimitives, OwnerOutcomesAreTyped)
{
    auto b = std::make_shared<InMemoryBackend>();
    Plane p(b);
    const auto claim = [&](OwnershipAuthority a) { return claimOwner(p.op, kLayout, kSrid, kUuid, a, kEmpty); };
    const auto why = [](OwnerClaim c) { return std::get<OwnerLost>(c).why; };
    put(*b, kLayout.ownerKey(kSrid), encodeOwner(OwnerObject{.server_uuid = UInt128(99)}));
    EXPECT_EQ(why(claim(OwnershipAuthority::Startup)), OwnerLost::Why::Foreign);
    put(*b, kLayout.ownerKey(kSrid), encodeOwner(OwnerObject{.server_uuid = kUuid, .retired_at_ms = 1}));
    EXPECT_EQ(why(claim(OwnershipAuthority::Reacquire)), OwnerLost::Why::Tombstoned);
    put(*b, kLayout.ownerKey(kSrid), "garbage");
    EXPECT_TRUE(std::holds_alternative<OwnershipCorrupt>(claim(OwnershipAuthority::Startup)));
    erase(*b, kLayout.ownerKey(kSrid));
    EXPECT_EQ(why(claim(OwnershipAuthority::Reacquire)), OwnerLost::Why::Absent);
    EXPECT_FALSE(get(*b, kLayout.ownerKey(kSrid))) << "a reacquire creates no owner anchor";
    EXPECT_TRUE(std::holds_alternative<OwnerHeld>(claim(OwnershipAuthority::Startup)));
    EXPECT_TRUE(get(*b, kLayout.ownerKey(kSrid)));
}
```

- [ ] Header: `enum class OwnershipAuthority : uint8_t { Startup, Reacquire };` with the comment
  "`Startup` may create the owner anchor and the epoch counter and re-decides when another process
  moves the counter; `Reacquire` creates nothing and stops there"; `struct OwnerHeld {};`;
  `struct OwnerLost { enum class Why : uint8_t { Foreign, Tombstoned, Absent }; Why why; std::exception_ptr error; };`;
  `using OwnerClaim = std::variant<OwnerHeld, OwnerLost, OwnershipTransient, OwnershipCorrupt>;`;
  `OwnerClaim claimOwner(CasOperation &, const Layout &, const String & srid, UInt128 our_uuid, OwnershipAuthority, const ObserveRefCatalog &);`
  and `void throwUnlessHeld(OwnerClaim && claim);` under the existing contract comment, which gains
  "Under `Reacquire` an absent anchor is `OwnerLost{Absent}`. `claimOwnerOrThrow` is `claimOwner`
  under `Startup`, throwing the outcome's exception."
- [ ] `CasServerRoot.cpp`, anonymous namespace:

```cpp
/// Every refusal inside a step is returned, so an escaping `CORRUPTED_DATA` is a decode failure.
/// Caller bugs and the decommission refusal propagate; anything else is the store not answering.
template <typename Outcome, typename Step>
Outcome runOwnershipStep(Step && step)
{
    try
    {
        return step();
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::CORRUPTED_DATA)
            return OwnershipCorrupt{OwnershipCorrupt::Cause::Undecodable, std::current_exception()};
        if (isDeterministicLocalFailure(e.code()) || e.code() == ErrorCodes::ABORTED)
            throw;
        return OwnershipTransient{std::current_exception()};
    }
}

/// The failure an observation carries, as the step's outcome; nullopt for a catalog.
template <typename Outcome>
std::optional<Outcome> catalogFailure(CatalogObservation & observation)
{
    if (auto * transient = std::get_if<OwnershipTransient>(&observation))
        return Outcome{std::move(*transient)};
    if (auto * corrupt = std::get_if<OwnershipCorrupt>(&observation))
        return Outcome{std::move(*corrupt)};
    return std::nullopt;
}

OwnerLost lostOwner(OwnerLost::Why why, Exception e)
{
    return OwnerLost{why, std::make_exception_ptr(std::move(e))};
}
```

  `throwIfOwnerRetired` becomes `std::optional<OwnerLost> retiredOwner(const OwnerObject &, const String & srid)`
  returning `lostOwner(Why::Tombstoned, Exception(...same text...))`. `claimOwnerStep` (same
  parameters as `claimOwner`) is the base body of `claimOwnerOrThrow` with these edits: (1) after the
  "owner present" block, `if (authority == OwnershipAuthority::Reacquire) return lostOwner(OwnerLost::Why::Absent,
  Exception(ErrorCodes::CORRUPTED_DATA, "CAS server-root '{}' has no owner anchor; a remount never creates one", srid));`;
  (2) each `throwIfOwnerRetired(x, srid); return;` → `if (auto lost = retiredOwner(x, srid)) return std::move(*lost); return OwnerHeld{};`;
  (3) each `throw Exception(ErrorCodes::CORRUPTED_DATA, …)` → `return lostOwner(Why, Exception(ErrorCodes::CORRUPTED_DATA, …))`,
  same text; `Foreign` for the two foreign-owner texts, `Absent` for "non-empty", "newly visible owned
  work" and "vanished during claim"; (4) each `serverRootSubtreeEmpty(op, l, srid, catalogOrThrow(observe_catalog()))`
  → `CatalogObservation c = observe_catalog(); if (auto f = catalogFailure<OwnerClaim>(c)) return std::move(*f);`
  then `serverRootSubtreeEmpty(op, l, srid, std::get<RefCatalog>(c))`; (5) `if (!occupant) return;` →
  `return OwnerHeld{};`. `claimOwner` keeps the observer-required `LOGICAL_ERROR` check and returns
  `runOwnershipStep<OwnerClaim>([&] { return claimOwnerStep(...); })`. `throwUnlessHeld` rethrows the
  `error` of any non-`OwnerHeld` alternative (`std::visit` with `detail::Overload`). `claimOwnerOrThrow`
  becomes `throwUnlessHeld(claimOwner(op, l, srid, our_uuid, OwnershipAuthority::Startup, observe_catalog));`.
- [ ] Build; run `CASOwnership*:CASServerRoot*:CASMount*`; all pass.
- [ ] Commit `git add -- $CA/Pool/CasServerRoot.h $CA/Pool/CasServerRoot.cpp src/Disks/tests/gtest_cas_ownership_primitives.cpp`;
  message `Return the CAS owner claim as a typed outcome with an authority`.

## Task 5: the epoch allocation is one immutable write with two outcomes for a foreign advance

**Files:** `$CA/Pool/CasServerRoot.h`, `$CA/Pool/CasServerRoot.cpp`, the test file.

- [ ] Failing tests (red on the base: `allocateEpoch` does not exist):

```cpp
TEST(CASOwnershipPrimitives, ReacquireCreatesNoCounter)
{
    auto b = std::make_shared<InMemoryBackend>();
    Plane p(b);
    EXPECT_TRUE(std::holds_alternative<EpochCounterLost>(allocateEpoch(
        p.op, kLayout, kSrid, OwnershipAuthority::Reacquire, EpochMintPolicy::NormalMount, 0, kEmpty)));
    EXPECT_FALSE(get(*b, kLayout.epochKey(kSrid)));
}
TEST(CASOwnershipPrimitives, ForeignAdvanceRepeatsAtStartupAndStopsAReacquire)
{
    for (const auto authority : {OwnershipAuthority::Startup, OwnershipAuthority::Reacquire})
    {
        auto b = std::make_shared<EpochWriteHook>();
        put(*b, kLayout.epochKey(kSrid), encodeServerEpoch(ServerEpoch{.next_writer_epoch = 5}));
        b->hook = raceHook;
        Plane p(b);
        const EpochAllocation got = allocateEpoch(p.op, kLayout, kSrid, authority, EpochMintPolicy::NormalMount, 0, kEmpty);
        if (authority == OwnershipAuthority::Startup)
            EXPECT_EQ(std::get<EpochAllocated>(got).writer_epoch, 6u);
        else
            EXPECT_EQ(std::get<EpochAdvancedByOther>(got).observed_next, 6u);
        EXPECT_EQ(counterNext(*b), authority == OwnershipAuthority::Startup ? 7u : 6u);
    }
}
TEST(CASOwnershipPrimitives, UnclearReacquireAllocationIsResolvedByOurBytes)
{
    auto b = std::make_shared<EpochWriteHook>();
    put(*b, kLayout.epochKey(kSrid), encodeServerEpoch(ServerEpoch{.next_writer_epoch = 5}));
    b->hook = lostAnswerHook;
    Plane p(b);
    EXPECT_EQ(std::get<EpochAllocated>(allocateEpoch(
        p.op, kLayout, kSrid, OwnershipAuthority::Reacquire, EpochMintPolicy::NormalMount, 0, kEmpty)).writer_epoch, 5u);
    EXPECT_EQ(counterNext(*b), 6u);
}
```

- [ ] Header: `struct EpochAllocated { uint64_t writer_epoch; };`,
  `struct EpochAdvancedByOther { uint64_t read_next; uint64_t observed_next; std::exception_ptr error; };`,
  `struct EpochCounterLost { std::exception_ptr error; };`, `using EpochAllocation =
  std::variant<EpochAllocated, EpochAdvancedByOther, EpochCounterLost, OwnershipTransient, OwnershipCorrupt>;`,
  `allocateEpoch` (the `allocateWriterEpoch` parameters with `OwnershipAuthority authority` after
  `srid`) and `uint64_t epochOrThrow(EpochAllocation && allocation);`. In the `allocateWriterEpoch`
  comment the last bullet becomes "otherwise write `{next + 1}` against the read incarnation as one
  conditional write; another process's advance re-decides at startup and is `EpochAdvancedByOther`
  under `Reacquire`, which also never mints over an absent counter".
- [ ] `CasServerRoot.cpp`: move the base `else` branch of the `decide` lambda (from "A missing `epoch`
  over a non-empty subtree" through `current.next_writer_epoch = 1`) into
  `EpochAllocation decideAbsentCounter(op, l, srid, policy, now_ms, observe_catalog)` returning
  `EpochAllocated{next}`. Its subtree and present-mount `CORRUPTED_DATA` throws become
  `return EpochCounterLost{std::make_exception_ptr(Exception(...same...))};`, the inconclusive-probe
  throw becomes `return OwnershipTransient{...same...};`, the live-member `ABORTED` throw stays, and its
  `observe_catalog()` use goes through `catalogFailure<EpochAllocation>`. Then:

```cpp
EpochAllocation allocateEpochStep(CasOperation & op, const Layout & l, const String & srid, OwnershipAuthority authority,
                                  EpochMintPolicy policy, uint64_t now_ms, const ObserveRefCatalog & observe_catalog)
{
    const String key = l.epochKey(srid);
    const Retry bound = op.freeze(Retry::standard());
    std::optional<Object> current = op.read(key, bound);
    bool previous_decision_saw_no_epoch = false;
    for (uint32_t unclear_conflicts = 0;;)
    {
        /// A winner may have installed the counter while owned work became visible.
        if (previous_decision_saw_no_epoch)
        {
            CatalogObservation catalog = observe_catalog();
            if (auto failure = catalogFailure<EpochAllocation>(catalog))
                return std::move(*failure);
            if (!serverRootSubtreeEmpty(op, l, srid, std::get<RefCatalog>(catalog)))
                return EpochCounterLost{std::make_exception_ptr(Exception(ErrorCodes::CORRUPTED_DATA,
                    "CAS server-root '{}' writer_epoch allocation conflicted and newly visible owned work blocks recreation", srid))};
        }
        previous_decision_saw_no_epoch = !current;
        uint64_t next = 0;
        if (current)
            next = decodeServerEpoch(current->bytes).next_writer_epoch;
        else if (authority == OwnershipAuthority::Reacquire)
            return EpochCounterLost{std::make_exception_ptr(Exception(ErrorCodes::CORRUPTED_DATA,
                "CAS server-root '{}' has no durable epoch object; a remount never mints one", srid))};
        else if (EpochAllocation fresh = decideAbsentCounter(op, l, srid, policy, now_ms, observe_catalog);
                 std::holds_alternative<EpochAllocated>(fresh))
            next = std::get<EpochAllocated>(fresh).writer_epoch;
        else
            return fresh;

        const String bytes = encodeServerEpoch(ServerEpoch{.next_writer_epoch = next + 1});
        WriteResult result = current ? op.replace(key, bytes, current->etag, bound) : op.create(key, bytes, bound);
        if (std::holds_alternative<Committed>(result))
            return EpochAllocated{next};
        const Conflict * conflict = std::get_if<Conflict>(&result);
        if (!conflict)
        {
            orThrow(std::move(result), fmt::format("CAS server-root '{}' writer_epoch allocation", srid));
            throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS server-root '{}': a conditional write was declined", srid);
        }
        /// An attempt of ours that landed is reported as committed, so what is there now is another
        /// process's -- even our own bytes, read after a clean refusal.
        std::optional<Object> seen;
        if (const Object * object = std::get_if<Object>(&conflict->seen))
            seen = *object;
        else if (std::holds_alternative<NotObserved>(conflict->seen))
            seen = op.read(key, bound);
        if (authority == OwnershipAuthority::Reacquire)
        {
            if (!seen)
                return EpochCounterLost{std::make_exception_ptr(Exception(ErrorCodes::CORRUPTED_DATA,
                    "CAS server-root '{}': the writer epoch counter vanished during a remount", srid))};
            const uint64_t observed = decodeServerEpoch(seen->bytes).next_writer_epoch;
            return EpochAdvancedByOther{next, observed, std::make_exception_ptr(Exception(ErrorCodes::ABORTED,
                "CAS server-root '{}': the writer epoch counter of this server_root_id was advanced by another process "
                "(read next_writer_epoch {}, found {})", srid, next, observed))};
        }
        op.pause(conflict->any_ambiguous ? Retry::backoff(++unclear_conflicts) : Retry::conflictBackoff());
        current = std::move(seen);
    }
}
```

  `allocateEpoch` keeps the observer-required `LOGICAL_ERROR` check and returns
  `runOwnershipStep<EpochAllocation>([&] { return allocateEpochStep(...); })`; `epochOrThrow` returns
  `writer_epoch` or rethrows the alternative's `error`; `allocateWriterEpoch` becomes
  `return epochOrThrow(allocateEpoch(op, l, srid, OwnershipAuthority::Startup, policy, now_ms, observe_catalog));`.
- [ ] Build; run `CASOwnership*:CASServerRoot*:CASMount*:CASDecommission*`; all pass, including the
  unchanged `CASServerRootSafety.EpochConflictRecomputesTheWholeEmptinessBundle` and
  `CASServerRootEpoch.ZeroCounterFailsAllocationWithoutWriting`.
- [ ] Commit `git add -- $CA/Pool/CasServerRoot.h $CA/Pool/CasServerRoot.cpp src/Disks/tests/gtest_cas_ownership_primitives.cpp`;
  message `Allocate the CAS writer epoch as one immutable conditional write`.

## Task 6: the self-remount creates neither the owner anchor nor the counter

**Files:** `$CA/Pool/CasPool.cpp` (`Pool::tryRemountOnce`), the test file.

- [ ] Failing tests. Behaviour-red on the base; on the base the second one aborts at
  `chassert(writer_epoch > mount_runtime.liveWriterEpoch())` in debug and sanitizer builds, so check
  the red in `build`:

```cpp
TEST(CASOwnershipReacquire, RemountDoesNotRecreateADeletedOwnerAnchor)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto pool = Pool::open(b, config());
    MountLease fenced = decodeMountLease(get(*b, kLayout.mountKey(kSrid))->bytes);
    fenced.gc_fenced = true;
    ++fenced.seq;
    put(*b, kLayout.mountKey(kSrid), encodeMountLease(fenced));
    erase(*b, kLayout.ownerKey(kSrid));
    EXPECT_FALSE(pool->tryRemountOnce());
    EXPECT_FALSE(get(*b, kLayout.ownerKey(kSrid)));
    EXPECT_EQ(pool->writerEpoch(), 1u);
}
TEST(CASOwnershipReacquire, RemountDoesNotRecreateADeletedEpochCounter)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto pool = Pool::open(b, config());
    erase(*b, kLayout.epochKey(kSrid));
    erase(*b, kLayout.mountKey(kSrid));
    EXPECT_FALSE(pool->tryRemountOnce());
    EXPECT_FALSE(get(*b, kLayout.epochKey(kSrid)));
}
```

- [ ] Run in `build` before the change; expected failures: `tryRemountOnce` true and the owner key
  present; the epoch key present. If the first test passes, the base refused for another reason
  (a non-empty subtree): stop and report what `tryRemountOnce` recorded.
- [ ] `Pool::tryRemountOnce`: `step = "owner_claim"` →
  `throwUnlessHeld(claimOwner(owner_op, pool_layout, srid, our_uuid, OwnershipAuthority::Reacquire, observe_catalog));`;
  `step = "writer_epoch_allocate"` → `const uint64_t writer_epoch = epochOrThrow(allocateEpoch(
  epoch_op, pool_layout, srid, OwnershipAuthority::Reacquire, EpochMintPolicy::NormalMount, 0, observe_catalog));`.
  The existing `catch (...)` records the outcome's exception and returns false. Comment above the
  owner step: "A remount creates neither the owner anchor nor the counter, and another process
  advancing the counter fails the attempt."
- [ ] Build; run `CASOwnership*:CASPoolRemount*:CASMountRemount*:CASLifecycleCondition*:CASForget*`; all pass.
- [ ] Commit `git add -- $CA/Pool/CasPool.cpp src/Disks/tests/gtest_cas_ownership_primitives.cpp`;
  message `Never recreate the CAS owner anchor or epoch counter in a self-remount`.

## Task 7: gates

- [ ] `ninja -C $WT/build unit_tests_dbms > $WT/build/ninja_pr11a_gate.log 2>&1` and the same for
  `build_asan`; exit 0. `TMPDIR=$WT/build/tmp $WT/build/src/unit_tests_dbms --gtest_filter='CAS*' > $WT/build/test_pr11a_gate.log 2>&1`
  and the same in `build_asan`; a subagent reports pass/fail counts and every failure; any red gets a
  root cause first. If `$WT/build_fast` exists: `ninja -C $WT/build_fast clickhouse > $WT/build_fast/ninja_pr11a_gate.log 2>&1`, exit 0.
- [ ] `git -C $WT diff ea46aa00861 --stat` lists exactly the six `$CA/Pool` files of Task 3 and the
  test file.

## PR text

Title: `Type the CAS ownership primitives and keep their creation to startup`

> The owner claim, the writer epoch allocation and the ownership catalog read return typed outcomes;
> each failure carries the exception startup throws, so startup keeps its codes and texts (golden
> tests pin every start path). The epoch is allocated by one conditional write of fixed bytes: at
> startup another process's advance re-decides as before; under the remount authority it is a
> terminal outcome. The self-remount no longer recreates a deleted owner anchor or epoch counter.
>
> Mixed versions: no format change; the remount rule is local to each server.
>
> Changelog category: Improvement
>
> Changelog entry: A `CAS` self-remount no longer recreates a missing owner anchor or writer epoch counter.
