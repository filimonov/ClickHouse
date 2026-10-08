# CAS lease PR 11b: lossless claim, renewer from the claim — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:executing-plans (or
> superpowers:subagent-driven-development). Steps use checkbox (`- [ ]`) syntax.

**Goal:** `claimMount` returns its observation losslessly and, for `Claimed`, `{body, bytes, etag,
start_boot_ms}` with `start` taken before the body is built. `MountLeaseRenewer::claim` (re-read and
re-write of the slot) is deleted: the renewer starts from that result, a start or remount writes the slot
once, and the lease runs from the claim's `start`. A late claim renews before writes open; one past its
lease-safe window fails the start. **Architecture:** `MountClaimResult` loses `body`/`etag`, gains
`claimed` and `seen`; the existing publication-horizon gate in `Pool::open`/`Pool::tryRemountOnce` now
measures from the claim's start, which is what renews a late claim. **Tech stack:** C++23, gtest
(`unit_tests_dbms`), `InMemoryBackend` seams, injected boot clocks; TLA+.

**Spec:** `/home/mfilimonov/workspace/ClickHouse/master/docs/superpowers/specs/2026-10-07-cas-mount-lease-series-design.md`
— §7 row 11b (both tables); §2 "Старт", "Потеря", "Получение заново"; §9 "`CaCasMountCore`, шаг 2b".
**Base:** the final head of `fix/antalya-26.6/cas-ownership-primitives` (PR 11a; `da854b34682` at plan
time). **Branch:** `fix/antalya-26.6/cas-lossless-claim`, created in the executor's worktree `$WT` by
`git -C $WT switch -c fix/antalya-26.6/cas-lossless-claim <11a-final-head>`.
`$CA` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed`. Outside `$CA`: tests and
`docs/en/antalya/cas/architecture/mounts-and-leases.md` (it describes the removed second write). No
format change. The model goes to the master worktree (branch `cas-gc-rebuild`). Commits end with:

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
- `mountObservationThresholdMs` and the observer wait in `claimMountAwaitingExpiry` stay untouched: the
  `max(own, declared)` rule (PR 2 v2) merges separately. The straggler renewal `PUT` after a park stays
  (PR 14 removes it).

## Review Focus

- **Deadline asymmetry.** Later than the claim's start lets the fence admit writes the stored body no
  longer backs (unsafe); earlier costs a renewal or a failed start (availability). So `start_boot_ms`
  is sampled before `makeMountBody` and nothing later moves it.
- **`start` sends nothing.** `seq` continues from the claim; the slot keeps `min_active_build_sequence = 0`
  until the first renewal — protects every new-epoch build (`prefixEligibleUnder`), delays a sweep ≤ one period.
- **One `MountClaim` row per start**; the `adopt` row and `Pool::open`'s `MountFencedException` catch
  go with the adopt window; `FencedSelf` still recovers with a fresh epoch.

## Code vs spec (flagged; the plan follows the spec)

1. Spec «ветка «свой claim повторён» удалена». Code: `claimMount` has none since PR 7; the remaining
   one is `MountLeaseRenewer::claim` (re-reads and rewrites our pair, or mints). 11b deletes it.
2. Spec «свой повторённый claim распознаётся по байтам». Code: `writeLoop` already confirms an unclear
   write by exact bytes (`obj->bytes == bytes`), green on the base; the test is red for the ETag only.
3. Spec «наблюдение claim без потерь (`Object` / `ProvenAbsent` / `NotObserved`)». Code: the first
   `GET` is `std::optional<Object>` — one-`GET` absence (PR 6 withdrawn; LIST corroboration CAS-352,
   PR 12), a failed read throws. Kept; `NotObserved` comes only from a raced write's resolve read.
4. Spec "Старт" «одно продление синхронно … с сегодняшним ограниченным окном: отказ — отказ старта».
   Code: `renew` is bounded by `Retry::untilLeaseSafe(confirmed_deadline)`; from the claim's start, a
   claim confirmed after `start + lease − margin` gets no attempt and the start fails. Kept, tested.
5. Spec R5 «остатка не хватает на запись». Code: the gate wants `period + 2 × envelope` (or
   `2 × envelope`) before `deadline − margin`, stricter than one conditional reserve. Unchanged.
6. Spec «`Claimed` несёт `{body, etag, start_boot_ms}`». Plan adds `bytes` (the settle read compares
   exact bytes, `last_body`).

## Task 1: model `CaCasMountCore` step 2b (master worktree, before any code)

**Files:** `docs/superpowers/models/CaCasMountCore.tla`, every `CaCasMountCore_*.cfg`, five new cfgs,
`run_mount.sh`, `CaCasMountCore_RESULTS.md`. **Precondition:** `git status --short docs/superpowers/models/`
in master is empty (at plan time a foreign uncommitted edit removes `StragglerAdopt`); otherwise stop.

- [ ] Constants (FALSE / `0` in every existing cfg): `ImmutableClaim` (`ASSUME ImmutableClaim =>
  ImmutableAlloc`), `Reserve`, `SabClaimDeadlineAtConfirm`, `SabOpenWithoutReserve`,
  `SabClaimResolveByPair`. Variables `claimStart`, `claimExp`, `claimTok` (`NoPend`, `Absent == -2` or
  a clock/token), `claimAmbig`, `claimState \in {"idle", "pending", "ontime", "late"}`, history
  `openedShort`, `lateOpened`; tuple `claimVars`, `UNCHANGED claimVars` elsewhere, `TypeOK` entries.
- [ ] Actions. `ClaimMount`'s `LET` moves to `ClaimAllowed(a)`; `ClaimMount` gets `~ImmutableClaim`.
  `OwnLive(a) == mount # None /\ mount.uuid = Uuid(a) /\ mount.epoch = localEpoch[a] /\ OwnBody(a) /\ ~mount.fenced`.
  - `ClaimBegin(a)`: `"idle"`, `ClaimMount`'s guards, `ClaimAllowed(a)` → `claimStart = clock`,
    `claimExp = IF mount = None THEN Absent ELSE mtoken`, `"pending"`.
  - `ClaimApply(a)`: `mount' = [uuid |-> Uuid(a), epoch |-> localEpoch[a], deadline |-> claimStart[a] + TTL,
    fenced |-> FALSE]`, `mtoken + 1`, `claimTok' = mtoken + 1`, other history as `ClaimMount`.
  - `Confirm(a)`: `fenceUntil'[a] = (IF SabClaimDeadlineAtConfirm THEN clock ELSE claimStart[a]) + TTL + d`
    for some `d \in 0..Drift`; `claimState' = IF clock + Reserve < claimStart[a] + TTL THEN "ontime" ELSE
    "late"`; clears `localLost`, `crashed`, `adoptObs` of `a`.
  - `ClaimAttempt(a)` (`"pending"`, `~claimAmbig`, `~crashed`): precondition holds → `ClaimApply /\
    Confirm`, or `ClaimApply` with `claimAmbig' = TRUE`; always `claimAmbig' = TRUE` alone (lost before
    applying); precondition fails → the claim ends (`"idle"`).
  - `ClaimResolve(a)`: `ours == IF SabClaimResolveByPair THEN mount matches (uuid, epoch, unfenced)
    ELSE claimTok[a] # NoPend /\ mtoken = claimTok[a]`; `ours` → `Confirm`; never applied and
    precondition still holds → reissue; else the claim ends.
  - `ClaimOpen(a)`: `"ontime"`/`"late"`, `~crashed`, `~localLost`, `OwnLive(a)`,
    `SabOpenWithoutReserve \/ clock + Reserve < mount.deadline` → clears `fenceLatched[a]` (only the
    open arms writes), `"idle"`, `openedShort' = openedShort \/ clock + Reserve >= mount.deadline`,
    `lateOpened' = lateOpened \/ claimState[a] = "late"`. `Die(a)` resets `a`'s claim vars; `Renew`,
    `AdoptRead`, `AdoptWrite` unchanged (the adopt pair stands for the straggler renewal `PUT`).
- [ ] Invariants: `LocalFenceWithinStamp == \A a \in Incs : OwnLive(a) => fenceUntil[a] <= mount.deadline + Drift`
  (confirmation does not move the deadline); `OpenLeavesReserve == ~openedShort` (a late claim with too
  little left does not open writes); witness `W_LateClaimRenewsThenOpens == ~lateOpened`.
- [ ] New cfgs copy `CaCasMountCore_twins_immalloc.cfg` plus `ImmutableClaim = TRUE`, `Reserve = 1` and
  the two invariants: `twins_immclaim` green; `sab_claimdeadlineatconfirm` → `LocalFenceWithinStamp`;
  `sab_opennoreserve` → `OpenLeavesReserve`; `sab_claimresolvebypair` → `OneWritableIncarnationPerUuid`;
  `witness_lateclaimrenews` → `W_LateClaimRenewsThenOpens`. Rows into `run_mount.sh` (sabotages first).
- [ ] From `docs/superpowers/models`: `TLC_WORKERS=8 TLC_JAVA_OPTS=-Xmx8g nice -n 10 ./run_mount.sh >
  ../../../tmp/run_mount_step2b.log 2>&1`. Expected `ALL EXPECTATIONS MET`; `twins_immalloc` and
  `twins` keep their distinct counts. A timeout is a FAIL to report, not a reason to shrink bounds.
  Each violation trace passes its sabotaged step; another invariant first = model defect, stop.
- [ ] RESULTS section `{#2026-10-08-step-2b}`: changes, table (cfg / expected / result / depth /
  generated / distinct), a line per trace, code cross-check (`ClaimBegin` ↔ boot sample before
  `makeMountBody`; `ClaimResolve` ↔ `writeLoop` byte compare; `ClaimOpen` ↔ `renewal_window_fits`),
  limit: `OpenLeavesReserve` restates the open guard — its evidence is the sabotage and the witness.
  Commit those files by name; message `model(cas): mount claim as one immutable operation`.

## Task 2: golden tests of the claim paths (green before and after)

**Files:** create `src/Disks/tests/gtest_cas_lossless_claim.cpp` (existing glob; `cmake` both build dirs
once). Base APIs only.

```cpp
#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasServerRootFormats.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Poco/Exception.h>

namespace DB::ErrorCodes { extern const int ABORTED; }
using namespace DB::Cas;
using DB::Cas::tests::OperationForTest;

namespace
{
const String kSrid = "s";
const Layout kLayout("pool");
const UInt128 kUuid(0x42);
using RawConflict = Backend::RawConflict;
using WriteAnswer = std::expected<String, RawConflict>;

std::optional<Object> get(Backend & b) { OperationForTest op(b); return (*op).read(kLayout.mountKey(kSrid), Retry::standard()); }
void put(Backend & b, const String & key, const String & bytes)
{
    OperationForTest op(b);
    const auto got = (*op).read(key, Retry::standard());
    ASSERT_TRUE(std::holds_alternative<Committed>(
        got ? (*op).replace(key, bytes, got->etag, Retry::standard()) : (*op).create(key, bytes, Retry::standard())));
}
MountLease slot(Backend & b) { return decodeMountLease(get(b)->bytes); }
MountLease lease(UInt128 uuid, uint64_t epoch, uint64_t seq, bool fenced, uint64_t min_active = 0)
{
    return {.server_uuid = uuid, .writer_epoch = epoch, .seq = seq, .expires_at_ms = 1,
            .min_active_build_sequence = min_active, .gc_fenced = fenced, .write_attempt_id = UInt128(100 + seq)};
}
/// What a GC fence-out does: keep the body, set `gc_fenced`, bump `seq`.
void fenceOut(Backend & b) { MountLease m = slot(b); m.gc_fenced = true; ++m.seq; put(b, kLayout.mountKey(kSrid), encodeMountLease(m)); }
/// Counts writes of the mount slot; `stall_ms` moves the boot clock once, before the next one lands;
/// `hook` replaces the next one.
class MountWriteClock : public InMemoryBackend
{
public:
    using Hook = std::function<WriteAnswer(InMemoryBackend &, const String &, const std::optional<String> &, TransportAccess &)>;
    std::shared_ptr<std::atomic<uint64_t>> boot = std::make_shared<std::atomic<uint64_t>>(10'000);
    std::atomic<int> mount_writes{0};
    uint64_t stall_ms = 0;
    Hook hook;

    WriteAnswer write(const String & key, const String & bytes, const std::optional<String> & expected, TransportAccess & access) override
    {
        if (key == kLayout.mountKey(kSrid))
        {
            ++mount_writes;
            *boot += std::exchange(stall_ms, 0);
            if (hook)
                return std::exchange(hook, {})(*this, bytes, expected, access);
        }
        return InMemoryBackend::write(key, bytes, expected, access);
    }
};
PoolConfig poolConfig(const std::shared_ptr<MountWriteClock> & b)
{
    PoolConfig c{.pool_prefix = "pool", .server_root_id = kSrid};
    c.server_id = kUuid;
    c.mount_lease_ttl_ms = std::chrono::milliseconds(30'000);
    c.retry_sleep_fn = [](uint64_t) {};
    c.boot_ms_fn = [boot = b->boot] { return boot->load(); };
    c.wait_sleep_fn = [boot = b->boot](uint64_t ms) { *boot += ms; };
    return c;
}
/// Epoch 7 (seq 7) was fenced by GC: a start allocates epoch 8 and reclaims at once with seq 8.
std::shared_ptr<MountWriteClock> fencedPredecessor()
{
    auto b = std::make_shared<MountWriteClock>();
    DB::Cas::tests::seedPoolMetaForRestart(*b, "pool");
    put(*b, kLayout.mountKey(kSrid), encodeMountLease(lease(kUuid, 7, 7, true)));
    put(*b, kLayout.epochKey(kSrid), encodeServerEpoch(ServerEpoch{.next_writer_epoch = 8}));
    b->mount_writes = 0;
    return b;
}
MountClaimResult claim(Backend & b) { OperationForTest op(b); return claimMount(*op, kLayout, kSrid, kUuid, 8, 1000, 30'000); }
}
TEST(CASLosslessClaim, GoldenClaimOutcomeByOccupant)
{
    using K = MountClaimResult;
    using P = MountPriorState;
    struct Case { const char * name; std::optional<MountLease> occupant; K::Kind kind; P prior; };
    const Case cases[] = {
        {"absent", std::nullopt, K::Claimed, P::None},
        {"foreign", lease(UInt128(9), 1, 3, false), K::ForeignOwner, P::None},
        {"own pair live", lease(kUuid, 8, 3, false), K::LiveDoubleStart, P::None},
        {"own pair fenced", lease(kUuid, 8, 3, true), K::FencedSelf, P::None},
        {"older fenced", lease(kUuid, 7, 3, true), K::Claimed, P::Fenced},
        {"older clean", lease(kUuid, 7, 3, false, std::numeric_limits<uint64_t>::max()), K::Claimed, P::Clean},
        {"older live", lease(kUuid, 7, 3, false), K::LiveDoubleStart, P::None},
    };
    for (const Case & c : cases)
    {
        auto b = std::make_shared<InMemoryBackend>();
        if (c.occupant)
            put(*b, kLayout.mountKey(kSrid), encodeMountLease(*c.occupant));
        const std::optional<Object> before = get(*b);
        const MountClaimResult r = claim(*b);
        EXPECT_EQ(r.kind, c.kind) << c.name;
        EXPECT_EQ(r.prior, c.prior) << c.name;
        if (c.kind != K::Claimed)
        {
            EXPECT_EQ(get(*b)->bytes, before->bytes) << c.name;
            continue;
        }
        const MountLease m = slot(*b);
        EXPECT_TRUE(m.server_uuid == kUuid && m.writer_epoch == 8 && !m.gc_fenced) << c.name;
        EXPECT_EQ(m.seq, c.occupant ? c.occupant->seq + 1 : 1u) << c.name;
    }
    auto b = std::make_shared<InMemoryBackend>();
    put(*b, kLayout.mountKey(kSrid), encodeMountLease(lease(kUuid, 9, 3, false)));
    DB::Cas::tests::expectThrowsCodeWithMessage(DB::ErrorCodes::ABORTED, "newer than the writer_epoch", [&] { claim(*b); });
}
TEST(CASLosslessClaim, GoldenOpenAndRemountPaths)
{
    auto fresh = std::make_shared<MountWriteClock>();
    ASSERT_TRUE(Pool::open(fresh, poolConfig(fresh)));
    EXPECT_EQ(slot(*fresh).writer_epoch, 1u);
    auto b = fencedPredecessor();
    auto store = Pool::open(b, poolConfig(b));
    EXPECT_TRUE(slot(*b).writer_epoch == 8 && !slot(*b).gc_fenced && store->mayMutate());
    fenceOut(*b);
    ASSERT_TRUE(store->tryRemountOnce());
    EXPECT_TRUE(slot(*b).writer_epoch == 9 && store->mayMutate());
    store.reset();
    ASSERT_TRUE(Pool::open(b, poolConfig(b))) << "a clean farewell is reclaimed at once";
    EXPECT_EQ(slot(*b).writer_epoch, 10u);
}
```

- [ ] `ninja -C $WT/build unit_tests_dbms > $WT/build/build_pr11b_t2.log 2>&1`;
  `TMPDIR=$WT/build/tmp $WT/build/src/unit_tests_dbms --gtest_filter='CASLosslessClaim.Golden*' > $WT/build/test_pr11b_t2.log 2>&1`.
  Expected: 2 PASSED on the base; a red golden is a wrong fixture — fix the fixture, not the code.
- [ ] Commit `git add -- src/Disks/tests/gtest_cas_lossless_claim.cpp`; message
  `Pin every CAS mount claim outcome and open path in golden tests`.

## Task 3: lossless claim result

**Files:** `$CA/Pool/CasServerRoot.h`, `$CA/Pool/CasServerRoot.cpp`, `$CA/Pool/CasPool.cpp`; tests
`gtest_cas_lossless_claim.cpp`, `gtest_cas_mount_claim_conflicts.cpp`, `gtest_cas_mount.cpp`, and any
`.body`/`.etag` reader the compiler names. **Produces:** `SlotObservation`, `MountClaimed`,
`MountClaimResult::{claimed, seen}`, `observedLease`, `claimMount(…, boot_ms_fn)`.

- [ ] Failing tests, appended:

```cpp
TEST(CASLosslessClaim, ClaimedCarriesTheLandedBytesTheirEtagAndTheStart)
{
    auto b = std::make_shared<MountWriteClock>();
    b->stall_ms = 1'000;
    OperationForTest op(*b);
    const MountClaimResult r = claimMount(*op, kLayout, kSrid, kUuid, 8, 1000, 30'000, {}, {}, {}, [&] { return b->boot->load(); });
    ASSERT_EQ(r.kind, MountClaimResult::Claimed);
    EXPECT_EQ(r.claimed->bytes, get(*b)->bytes);
    EXPECT_EQ(r.claimed->etag, get(*b)->etag);
    EXPECT_EQ(r.claimed->start_boot_ms, 10'000u) << "taken before the body is built, not when the write returns";
    EXPECT_EQ(b->boot->load(), 11'000u);
}
TEST(CASLosslessClaim, AnUnclearClaimThatLandedIsConfirmedByItsBytes)
{
    auto b = std::make_shared<MountWriteClock>();
    b->hook = [](InMemoryBackend & s, const String & bytes, const std::optional<String> & expected, TransportAccess & access) -> WriteAnswer
    {
        (void)s.InMemoryBackend::write(kLayout.mountKey(kSrid), bytes, expected, access);
        throw Poco::TimeoutException("injected: the claim landed and its answer was lost");
    };
    const MountClaimResult r = claim(*b);
    ASSERT_EQ(r.kind, MountClaimResult::Claimed);
    EXPECT_EQ(r.claimed->etag, get(*b)->etag);
    EXPECT_EQ(b->mount_writes.load(), 1) << "confirmed by reading our bytes, not by writing again";
}
TEST(CASLosslessClaim, AnotherBodyOfOurPairAfterAnUnclearClaimIsReportedWithItsBytes)
{
    auto b = std::make_shared<MountWriteClock>();
    const String other = encodeMountLease(lease(kUuid, 8, 1, false));
    b->hook = [other](InMemoryBackend & s, const String &, const std::optional<String> &, TransportAccess & access) -> WriteAnswer
    {
        (void)s.InMemoryBackend::write(kLayout.mountKey(kSrid), other, std::nullopt, access);
        throw Poco::TimeoutException("injected: another process of our pair landed first");
    };
    const MountClaimResult r = claim(*b);
    EXPECT_EQ(r.kind, MountClaimResult::LiveDoubleStart);
    ASSERT_TRUE(std::holds_alternative<Object>(r.seen));
    EXPECT_EQ(std::get<Object>(r.seen).bytes, other);
}
TEST(CASLosslessClaim, ARefusedClaimOverAnEmptySlotReportsTheAbsence)
{
    auto b = std::make_shared<MountWriteClock>();
    b->hook = [](InMemoryBackend &, const String &, const std::optional<String> &, TransportAccess &) -> WriteAnswer
    { return std::unexpected(RawConflict{}); };
    const MountClaimResult r = claim(*b);
    EXPECT_EQ(r.kind, MountClaimResult::LiveDoubleStart);
    EXPECT_TRUE(std::holds_alternative<ProvenAbsent>(r.seen)) << "absence is not 'nothing observed'";
}
```

  `CASMountClaimConflicts.ARacedReclaimThatObservedNothingNamesNoHolder` also asserts
  `EXPECT_TRUE(std::holds_alternative<NotObserved>(r.seen))`. Build. Expected: compile error
  `no member named 'claimed' in 'DB::Cas::MountClaimResult'` (and `'seen'`).
- [ ] `CasServerRoot.h`, above `MountClaimResult`: `using SlotObservation = std::variant<Object,
  ProvenAbsent, NotObserved>;` ("what one read of the mount slot established; `Object` keeps the exact
  bytes and their ETag"); `struct MountClaimed { MountLease body; String bytes; Etag etag; uint64_t
  start_boot_ms = 0; };` (Allman layout; "a claim this server confirmed: its body, those exact bytes,
  the ETag they landed under, and the `CLOCK_BOOTTIME` instant taken before the body was built — the
  lease runs from it"); `std::optional<MountLease> observedLease(const SlotObservation & seen);`.
  In `MountClaimResult`, `body` and `etag` become `std::optional<MountClaimed> claimed;` ("engaged
  exactly for `Claimed`") and `SlotObservation seen = NotObserved{};` ("for every other `Kind`, what
  decided it; a raced write reports its resolve read, which may have proven the slot absent or seen
  nothing, and then no message may name a holder"), between `kind` and `prior`; rewrite the struct's
  doc to match. `claimMount` gains a last parameter `const std::function<uint64_t()> & boot_ms_fn = {}`:
  "dates the claim's start (`CLOCK_BOOTTIME` when empty); pass the renewer's clock".
- [ ] `CasServerRoot.cpp`: `racedDoubleStart` is replaced by

```cpp
/// A claim write's verdict: the ETag it landed under, or what its resolve read saw when another write
/// won. Claims resolve by reading the body, so a metadata-only observation never reaches here.
std::variant<Etag, SlotObservation> claimWrite(WriteResult && result, const String & what)
{
    if (Committed * committed = std::get_if<Committed>(&result))
        return std::move(committed->etag);
    Conflict * conflict = std::get_if<Conflict>(&result);
    if (Object * object = conflict ? std::get_if<Object>(&conflict->seen) : nullptr)
        return SlotObservation{std::move(*object)};
    if (conflict && std::holds_alternative<ProvenAbsent>(conflict->seen))
        return SlotObservation{ProvenAbsent{}};
    if (conflict && std::holds_alternative<NotObserved>(conflict->seen))
        return SlotObservation{NotObserved{}};
    orThrow(std::move(result), what);
    throw Exception(ErrorCodes::LOGICAL_ERROR, "{}: ended without a verdict on the slot", what);
}
```

  Define `observedLease` (`Object` → `decodeMountLease(bytes)`, else `std::nullopt`). In `claimMount`,
  `const auto boot_clock = boot_ms_fn ? boot_ms_fn : std::function<uint64_t()>(defaultBootMs);`. In the
  absent and the reclaim branch: `const uint64_t start_boot_ms = boot_clock();` is the line right
  before `makeMountBody`; the body is encoded once into `String bytes`; the write goes through
  `claimWrite`; a `SlotObservation` result returns `{.kind = LiveDoubleStart, .seen = std::move(*raced)}`
  (the existing "never overwrite a slot that appeared under us" comment stays); an `Etag` returns
  `{.kind = Claimed, .claimed = MountClaimed{body, std::move(bytes), etag, start_boot_ms}}` (plus
  `.prior` on reclaim). Every no-write return carries `.seen = *got`.
- [ ] `claimMountAwaitingExpiry`: `mono_ms_fn` is the `boot_ms_fn` of both `claimMount` calls; the
  holder is `observedLease(r.seen)`, the incarnation `std::get_if<Object>(&r.seen)`; a re-read copies
  its ETag, then `r.seen = std::move(*got)`. Threshold arithmetic untouched.
- [ ] `Pool::open`: `const auto boot_ms = [raw] { return raw->bootMsNow(); };` into the direct
  `claimMount` calls (unsafe, `NoWait`). The unsafe re-claim tests `observedLease(claim.seen)` and
  copies `std::get<Object>(claim.seen).etag` before reassigning `claim`; the `NoWait` text and
  `mountDoubleStartMessage` take `observedLease(claim.seen)`.
- [ ] Existing tests: non-`Claimed` `.body` → `observedLease(r.seen)`, `Claimed` → `r.claimed->body`;
  `.etag` → `std::get<Object>(r.seen).etag`. Build; `--gtest_filter='CAS*'` release. Expected: all
  pass, goldens unchanged.
- [ ] Commit the three production files and the touched tests by name; message
  `Return the CAS mount claim's observation and confirmed body losslessly`.

## Task 4: renewer from the claim; deadline from the claim's start

**Files:** `$CA/Pool/CasServerRoot.h/.cpp`, `$CA/Pool/CasMountRuntime.h/.cpp`, `$CA/Pool/CasPool.cpp`;
tests `gtest_cas_lossless_claim.cpp`, `gtest_cas_heartbeat.cpp`, `gtest_cas_mount_claim_conflicts.cpp`,
`gtest_cas_pool.cpp`, `gtest_cas_gc_ack_floor.cpp`, `gtest_cas_mount.cpp`.

- [ ] Failing pool tests, appended (compile on Task 3, red by assertion):

```cpp
TEST(CASLosslessClaim, StartWritesTheSlotOnceAndRunsTheLeaseFromTheClaim)
{
    auto b = fencedPredecessor();
    b->stall_ms = 1'000;
    auto store = Pool::open(b, poolConfig(b));
    EXPECT_EQ(b->mount_writes.load(), 1) << "no second write after the claim";
    EXPECT_EQ(slot(*b).seq, 8u);
    b->boot->store(39'999);
    EXPECT_TRUE(store->mayMutate());
    b->boot->store(40'000);
    EXPECT_FALSE(store->mayMutate()) << "the deadline is the claim's start (10 000) plus the lease";
}
TEST(CASLosslessClaim, ALateClaimRenewsBeforeWritesOpenAndATooLateOneFailsTheStart)
{
    auto late = fencedPredecessor();
    late->stall_ms = 18'000;   /// 10 s of the lease-safe window left: below the publication horizon
    auto store = Pool::open(late, poolConfig(late));
    EXPECT_EQ(late->mount_writes.load(), 2);
    EXPECT_EQ(slot(*late).seq, 9u) << "the second write renews the claim";
    auto too_late = fencedPredecessor();
    too_late->stall_ms = 29'000;   /// confirmed at 39 000, after start + lease - margin = 38 000
    EXPECT_ANY_THROW(Pool::open(too_late, poolConfig(too_late)));
    EXPECT_EQ(too_late->mount_writes.load(), 1) << "no renewal fits inside the claim's lease";
}
TEST(CASLosslessClaim, RemountWritesTheSlotOnce)
{
    auto b = fencedPredecessor();
    auto store = Pool::open(b, poolConfig(b));
    fenceOut(*b);
    b->mount_writes = 0;
    ASSERT_TRUE(store->tryRemountOnce());
    EXPECT_EQ(b->mount_writes.load(), 1);
}
```

  Run. Expected: 3 FAILED — 2 writes and `mayMutate` true at 40 000 (the adopt started later); late
  `seq` 1 (the adopt reset it, no renewal) and the too-late open succeeds; the remount writes 2.
- [ ] No separate renewer unit test: the pool tests prove `start` sends nothing (`mount_writes == 1`)
  and that `seq` continues from the claim (late case `seq == 9`); the reworked `AnchorCarriesFloor`
  proves the first renewal writes the floor. After the change below, `renewer.start()` call sites fail
  to compile until migrated — that is the expected red for the unit files.
- [ ] `MountLeaseRenewer`: `uint64_t start(Liveness liveness = {})` becomes
  `void start(const MountClaimed & claimed);` — "Take over the slot `claimMount` just confirmed. No
  request: the lease runs from the claim's start." Delete `Etag claim(…)` (declaration and definition).

```cpp
void MountLeaseRenewer::start(const MountClaimed & claimed)
{
    if (renewer_state != MountLeaseRenewerState::New)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS mount-lease: start is allowed only in New state for key '{}'", key);
    if (claimed.body.server_uuid != server_uuid || claimed.body.writer_epoch != writer_epoch)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS mount-lease: key '{}' was claimed for writer_epoch {}, not ours {}",
                        key, claimed.body.writer_epoch, writer_epoch);
    seq = claimed.body.seq;
    last_etag = claimed.etag;
    last_body = claimed.bytes;
    last_committed_attempt_start_boot_ms = claimed.start_boot_ms;
    last_confirmed_body = claimed.body;
    const uint64_t ttl_ms = static_cast<uint64_t>(ttl.count());
    confirmed_deadline_boot_ms = claimed.start_boot_ms > std::numeric_limits<uint64_t>::max() - ttl_ms
        ? std::numeric_limits<uint64_t>::max() : claimed.start_boot_ms + ttl_ms;
    renewer_state = MountLeaseRenewerState::Active;
}
```

- [ ] `CasMountRuntime::startRenewer(const MountClaimed & claimed)`: same driver-state handling;
  `renewer->start(claimed)` (no liveness lambda), `publishConfirmedBody`, return `claimed.start_boot_ms`.
- [ ] `Pool::open`: `claim_anchor_boot_ms = store->mount_runtime.startRenewer(*claim.claimed);`, no
  `try`/`catch (const MountFencedException &)`; the loop comment names one fence path (`FencedSelf`);
  the redo comment: "The lease runs from the claim's own start. A claim that confirmed late leaves too
  little of it to arm the fence; one conditional renewal re-anchors first, and a claim past its
  lease-safe window cannot send one, so the start fails." `tryRemountOnce`: `startRenewer(*claim.claimed)`.
- [ ] Existing tests (mechanical; intent kept):
  - `gtest_cas_heartbeat.cpp`: `seedOwnClaim` returns `MountClaimed` (throws `ABORTED` unless
    `Claimed`) and takes the test's boot clock; `start()` → `start(claimed)`. `AnchorCarriesFloor`
    checks the floor after the first renewal. `RenewerAdoptEmitsClaimAndTerminateEmitsRelease` →
    `ClaimEmitsClaimAndTerminateEmitsRelease` (the row is `claimMount`'s `mint`). Delete
    `RenewerAdoptRefusesFencedSelfWithTypedError` (`start` reads nothing; `FencedSelf` is golden).
  - `gtest_cas_mount_claim_conflicts.cpp`: delete the seven `renewer.start()` tests through
    `FencedInsideAdoptionWindowRaisesMountFencedNotAborted` and helpers only they use (golden covers them).
  - `gtest_cas_pool.cpp`: delete `FenceInAdoptWindowBackend` and
    `OpenRecoversFromFenceInAdoptWindowWithFreshEpoch`. Counts drop by one in
    `PublicationHorizonUsesTheEnvelope` (2 → 1, 3 → 2) and in `RemountRenewerRedoUsesTheEnvelope`'s
    control. `StartupArmRedoesLeaseWriteWhenTheClaimConsumesTtl` stalls the first mount write and
    expects 2 writes. `RemountArmAnchorsAtClaimAttemptNotResponseTime` drives the clock by a mount-write
    stall, not a call-count sequence. Fix comments about "the renewer's adopt".
  - `gtest_cas_gc_ack_floor.cpp` (3 sites), `gtest_cas_mount.cpp`: `claimMount` first, then `start(*r.claimed)`.
- [ ] Build; `--gtest_filter='CAS*'` release. Expected: all pass, goldens unchanged. Commit production
  files and every touched test by name; message `Start the CAS mount renewer from the claim instead of rewriting the slot`.

## Task 5: docs, gates, PR

- [ ] `mounts-and-leases.md`, "Mount, unmount, crash": "mount claim and synchronous renewer start" →
  "mount claim (one conditional write; the renewer starts from its confirmed body)"; "If the claim
  consumed the TTL, …" → "The lease runs from the claim's start; if too little is left to arm the fence,
  one synchronous renewal re-anchors it, and a claim past its lease-safe window fails the start."
  "Deadline from the start": "A confirmed renewal or claim …". Commit; `Describe the single-write CAS mount claim`.
- [ ] Gates on the exact head, logs summarised by a subagent: `ninja … unit_tests_dbms` in `build` and
  `build_asan`; `TMPDIR=<dir>/tmp <dir>/src/unit_tests_dbms --gtest_filter='CAS*' > <dir>/test_pr11b_gate.log 2>&1`
  in both; from `$WT` `python -m ci.praktika run "integration" --test test_cas_mount_renewal_retry >
  $WT/build/test_pr11b_integration.log 2>&1`. All green; any red gets a root cause first.
- [ ] PR body per the template: one slot write per start or remount; lease from the claim's start; a
  late claim renews first. Mixed versions: no format change; one `MountClaim` row and one token change
  fewer per start; `seq` continues from the claim; `min_active_build_sequence` is `0` until the first
  renewal. Changelog (Improvement): "A CAS mount start writes its lease slot once and runs the lease
  from the claim's own start."
