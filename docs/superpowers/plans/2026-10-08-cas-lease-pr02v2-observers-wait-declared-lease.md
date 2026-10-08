# CAS lease PR 2 v2: observers wait for the longer of their own and the declared lease

**Goal.** An observer of a mount slot waits `L + L/20 + step` with `L = max(own lease, declared lease)`.
The declared lease is `expires_at_ms − started_at_ms` of the observed body, or 0 when `started_at_ms`
is 0 or the interval is not positive. A server with a shorter `cas_mount_lease_ttl_ms` can then never
fence or reclaim a holder within that holder's own lease. The GC leader also logs one `WARNING` per slot
whose declared lease differs from its own.

**Architecture.** `mountObservationThresholdMs(own, declared, cadence)` stays the single formula, gains
`max` and saturation, and `declaredMountLeaseMs(body)` reads the stamps. `computeHeartbeatFloor` takes the
observer's lease and step instead of a precomputed threshold and computes each slot's threshold from that
slot's body. `claimMountAwaitingExpiry` computes the threshold when an observation starts, from the
observed body. The fast paths (`gc_fenced`, the clean farewell marker, `cas_unsafe_remount_no_delay`) are
untouched.

**Tech stack.** C++23, gtest (`unit_tests_dbms`), `InMemoryBackend`, Poco logger capture.

**Spec.** `docs/superpowers/specs/2026-10-07-cas-mount-lease-series-design.md` r7, section 3 and the
section 1 "Настройки" paragraph. This plan follows the owner decision of 2026-10-08, which replaces
r7's §3 rule "длина lease держателя = настройка наблюдателя (одинакова в пуле)" and the §11 rejection
of reading the lease from the holder's body. The spec must be revised to match. Until then, this plan
is the authority for PR 2.

**Base.** `bc7f453aa4a` (squashed PR 1). Branch `fix/antalya-26.6/cas-observers-wait-declared-lease`.
Work in a fresh worktree. `cas-95-1` is in use: never edit it.
```bash
cd ~/workspace/ClickHouse && nohup bash create_subtree_branch.sh cas-pr02v2 bc7f453aa4a > master/tmp/cas-pr02v2-wt.log 2>&1
git -C ~/workspace/ClickHouse/cas-pr02v2 branch -m cas-pr02v2 fix/antalya-26.6/cas-observers-wait-declared-lease
```
Below, `W` is `~/workspace/ClickHouse/cas-pr02v2` and `CA/` is
`src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`. Code to harvest:
`21f200ba871` (plumbing) and `03330f1b96d` (warning). Both sit directly on `bc7f453aa4a`, so
`git show <sha> -- <file> | git apply` applies cleanly.

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

Every commit message ends with these two lines, in this order:
```
Signed-off-by: Mikhail Filimonov <mfilimonov@altinity.com>
Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
```
Files outside `CA/`: no production files. Tests: `src/Disks/tests/cas_test_helpers.h`,
`gtest_cas_mount.cpp`, `gtest_cas_gc_ack_floor.cpp`. Docs: three pages under `docs/en/antalya/cas/`.

Build and run a test: `ninja -C build unit_tests_dbms > build/ninja_pr02v2.log 2>&1; echo NINJA_EXIT=$?`,
then `build/src/unit_tests_dbms --gtest_filter='<filter>' > build/test_pr02v2_<task>.log 2>&1`.
A subagent reads the log.

## Review Focus

- **Failure asymmetry.** If an observer waits too little, it fences or reclaims a live holder, and two
  writers share a namespace. If it waits too long, recovery is delayed. With `max`, no body can give a
  threshold below today's. Saturation makes an overflowing sum wait forever instead of wrapping to a
  small threshold.
- **Exposure.** A body that declares a huge lease (a corrupt body, or a holder configured with one; the
  setting has no upper bound) makes the GC never fence that slot. A same-`server_root_id` restart then
  waits that long in `Pool::open`. This is the safe direction. The `WARNING` and the existing
  `on_wait_start` log (it prints the threshold) name it.
- In `computeHeartbeatFloor`, the same token means the same body, so a slot's threshold is fixed for one
  observation. A renewal that changes the lease also changes the token and restarts the observation.
- `SYSTEM CAS DROP POOL MEMBER` (`decommissionPoolMember` → `Pool::openForDecommission`) uses
  `MountClaimPolicy::NoWait`: one `claimMount`, which reclaims only `gc_fenced` or farewell slots and
  otherwise refuses with `ABORTED`. It gets the new rule only through the GC fence and needs no code.
  The second `claimMountAwaitingExpiry` caller is `Pool::tryRemountOnce` (self-remount). It shares the
  changed function and needs no caller change.
- Expected values in the regression tests are literals (`64'000`, `73'000`), never
  `mountObservationThresholdMs(...)`. Otherwise a formula mutation would change both sides.

---

### Task 1: One formula with the declared lease

**Files:** `CA/Pool/CasServerRoot.h`, `CA/Pool/CasServerRoot.cpp`, `CA/Gc/CasGc.cpp`,
`src/Disks/tests/gtest_cas_mount.cpp`, `src/Disks/tests/gtest_cas_gc_ack_floor.cpp`.
**Produces:** `uint64_t declaredMountLeaseMs(const MountLease &)`;
`uint64_t mountObservationThresholdMs(uint64_t own_lease_ms, uint64_t declared_lease_ms, uint64_t cadence_ms)`.
Every existing caller passes `/*declared_lease_ms=*/0`, so behaviour is unchanged.

- [ ] **Test.** Append to `gtest_cas_mount.cpp`, before `/// ---- Task 1 (Phase 2): \`listMounts\``:
```cpp
TEST(CASMountObservationThreshold, LongerOfOwnAndDeclaredLeasePlusDriftAndStep)
{
    EXPECT_EQ(mountObservationThresholdMs(30'000, 0, 1'000), 32'500u);
    EXPECT_EQ(mountObservationThresholdMs(30'000, 10'000, 1'000), 32'500u);
    EXPECT_EQ(mountObservationThresholdMs(30'000, 60'000, 1'000), 64'000u);
}

TEST(CASMountObservationThreshold, SaturatesInsteadOfWrapping)
{
    constexpr uint64_t max = std::numeric_limits<uint64_t>::max();
    /// `20 * (max / 21)` plus its twentieth is `max - 15`: the step alone decides the overflow.
    EXPECT_EQ(mountObservationThresholdMs(0, 20 * (max / 21), 14), max - 1);
    EXPECT_EQ(mountObservationThresholdMs(0, 20 * (max / 21), 16), max);
    EXPECT_EQ(mountObservationThresholdMs(30'000, max - 10, 0), max);
}

TEST(CASMountObservationThreshold, DeclaredLeaseIsThePositiveStampInterval)
{
    MountLease m;
    m.started_at_ms = 1'000;
    m.expires_at_ms = 61'000;
    EXPECT_EQ(declaredMountLeaseMs(m), 60'000u);
    m.started_at_ms = 0;
    EXPECT_EQ(declaredMountLeaseMs(m), 0u) << "a body without the start stamp";
    m.started_at_ms = 61'000;
    EXPECT_EQ(declaredMountLeaseMs(m), 0u) << "a farewell stamps both equal";
    m.started_at_ms = 70'000;
    EXPECT_EQ(declaredMountLeaseMs(m), 0u) << "expiry before start";
}
```
- [ ] **Run** `--gtest_filter='CASMountObservationThreshold.*'`. Expect a compile failure:
  `no matching function for call to 'mountObservationThresholdMs'` and `use of undeclared identifier 'declaredMountLeaseMs'`.
- [ ] **Implement.** In `CasServerRoot.h`, replace the paragraph `/// All callers share this one formula …`
  and the 2-argument declaration below it with:
```cpp
/// The lease a mount body declares: `expires_at_ms - started_at_ms` when `started_at_ms` is set and the
/// interval is positive, else 0. A body without the stamps decodes them as 0; a farewell stamps them equal.
uint64_t declaredMountLeaseMs(const MountLease & m);

/// How long an observer watches one slot's token on its own clock before treating the holder as dead:
/// `L + L / 20 + cadence_ms` with `L = max(own_lease_ms, declared_lease_ms)`, saturating at `UINT64_MAX`.
/// The longer lease wins: a holder may write for its own lease after its last confirmed renewal,
/// whatever the observer's lease is. `L / 20` allows a five-percent clock-rate difference; `cadence_ms`
/// is the observer's own poll step (restart reclaim: `max(1, mount_renew_period / 2)`; GC: the period).
uint64_t mountObservationThresholdMs(uint64_t own_lease_ms, uint64_t declared_lease_ms, uint64_t cadence_ms);
```
  In `CasServerRoot.cpp`, add `#include <base/arithmeticOverflow.h>` and replace the 2-argument definition:
```cpp
uint64_t declaredMountLeaseMs(const MountLease & m)
{
    if (m.started_at_ms == 0 || m.expires_at_ms <= m.started_at_ms)
        return 0;
    return m.expires_at_ms - m.started_at_ms;
}

uint64_t mountObservationThresholdMs(uint64_t own_lease_ms, uint64_t declared_lease_ms, uint64_t cadence_ms)
{
    const uint64_t lease_ms = std::max(own_lease_ms, declared_lease_ms);
    uint64_t threshold_ms = 0;
    /// A wrapped sum would fence early; waiting forever is the safe side.
    if (common::addOverflow(lease_ms, lease_ms / 20, threshold_ms)
        || common::addOverflow(threshold_ms, cadence_ms, threshold_ms))
        return std::numeric_limits<uint64_t>::max();
    return threshold_ms;
}
```
  Pass `/*declared_lease_ms=*/0` as the new middle argument at: `claimMountAwaitingExpiry`
  (`mountObservationThresholdMs(ttl_ms, poll)`), both `CasGc.cpp` sites (`mountObservationThresholdMs(\n ttl_ms, …mount_renew_period…)`)
  and `runSlowWalkSightingScenario` in `gtest_cas_gc_ack_floor.cpp`.
- [ ] **Run** the filter: 3 pass. Run the gate `CAS*`: green.
- [ ] **Commit** `git add -- CA/Pool/CasServerRoot.h CA/Pool/CasServerRoot.cpp CA/Gc/CasGc.cpp src/Disks/tests/gtest_cas_mount.cpp src/Disks/tests/gtest_cas_gc_ack_floor.cpp`
  (expand `CA/`), message `Give the mount observation threshold a declared-lease term`.

### Task 2: `computeHeartbeatFloor` takes the observer's lease and step

**Files:** `CA/Pool/CasServerRoot.h`, `CA/Pool/CasServerRoot.cpp`, `CA/Gc/CasGc.h`, `CA/Gc/CasGc.cpp`,
`src/Disks/tests/gtest_cas_mount.cpp`. Pure plumbing. Every existing test stays green.
**Produces:** `computeHeartbeatFloor(op, l, now_ms, mono_ms_fn, uint64_t observer_lease_ms,
uint64_t cadence_ms, MountObservationMap & obs, std::set<String> & lease_mismatch_warned)`;
`Gc::lease_mismatch_warned`.

- [ ] **Header.** Add `#include <set>`. Replace the declaration's `uint64_t stable_threshold_ms, MountObservationMap & obs`
  with `uint64_t observer_lease_ms, uint64_t cadence_ms, MountObservationMap & obs, std::set<String> & lease_mismatch_warned`.
  In its comment, replace "held for `>= stable_threshold_ms` OF THE LEADER'S OWN CLOCK" with "held, on the
  leader's own clock, for `mountObservationThresholdMs(observer_lease_ms, declaredMountLeaseMs(body), cadence_ms)`".
- [ ] **Definition.** Same signature, with `std::set<String> & /*lease_mismatch_warned*/` unnamed. Before
  `WriteResult fenced_out = op.readModifyWrite(` add `uint64_t threshold_ms = 0;`. In the lambda, right
  after `const MountLease m = decodeMountLease(observed->bytes);`, add
  `threshold_ms = mountObservationThresholdMs(observer_lease_ms, /*declared_lease_ms=*/0, cadence_ms);`.
  Replace both remaining `stable_threshold_ms` uses (`heldFor` and the fence-out `LOG_INFO` argument) with `threshold_ms`.
- [ ] **`Gc`.** Apply `git show 21f200ba871 -- CA/Gc/CasGc.h | git apply` (adds `std::set<String> lease_mismatch_warned;`
  after `mount_obs`). In `CasGc.cpp`, at both sites delete the `stable_threshold_ms` local and its two comment
  lines (`/// The formula is shared …` / `/// Share the identical formula …`). Add
  `const uint64_t renew_period_ms = static_cast<uint64_t>(store->poolConfig().mount_renew_period.count());`
  and call `computeHeartbeatFloor(op, layout, now_ms_fn(), mono_ms_fn, ttl_ms, renew_period_ms, mount_obs, lease_mismatch_warned)`.
- [ ] **Tests.** In `gtest_cas_mount.cpp`, add `#include <set>`. Replace
  `constexpr uint64_t kStableThresholdMs = 10'000;` and its comment with:
```cpp
/// The observing leader's own lease and step, and the threshold they give a body that declares nothing.
constexpr uint64_t kObserverLeaseMs = 30'000;
constexpr uint64_t kCadenceMs = 1'000;
constexpr uint64_t kStableThresholdMs = 32'500;
```
  Then migrate the 27 calls mechanically and check the counts:
```bash
sed -i '/^TEST(CASHeartbeatFloor/,/^}/{s/kStableThresholdMs, obs)/kObserverLeaseMs, kCadenceMs, obs, warned)/g; s/^\(\s*\)MountObservationMap obs;$/&\n\1std::set<String> warned;/}' src/Disks/tests/gtest_cas_mount.cpp
grep -c 'kCadenceMs, obs, warned)' src/Disks/tests/gtest_cas_mount.cpp   # 27
grep -c 'kStableThresholdMs, obs' src/Disks/tests/gtest_cas_mount.cpp    # 0
```
- [ ] **Run** `CASHeartbeatFloor.*:CASGcFenceOut.*:CASGCAckFloor.*`, then the gate: green (golden; on `bc7f453aa4a` too).
- [ ] **Commit** the five files, message `Pass the observer lease and step into computeHeartbeatFloor`.

### Task 3: The GC floor waits for the declared lease

**Files:** `CA/Pool/CasServerRoot.cpp`, `CA/Gc/CasGc.cpp` (comment), `src/Disks/tests/gtest_cas_mount.cpp`,
`src/Disks/tests/gtest_cas_gc_ack_floor.cpp`.

- [ ] **Test (unit).** Apply the `seedMount` hunk of `03330f1b96d`: add the trailing parameter
  `uint64_t started_at_ms = kNowMs` and use it for `m.started_at_ms`. Append after
  `TEST(CASHeartbeatFloor, EmptyPrefixYieldsNoLiveMounts)`:
```cpp
namespace
{
HeartbeatFloor floorAt(Ops & ops, const Layout & l, uint64_t mono_ms, MountObservationMap & obs, std::set<String> & warned)
{
    return computeHeartbeatFloor(ops.op, l, kNowMs, frozenClock(mono_ms), kObserverLeaseMs, kCadenceMs, obs, warned);
}
}

/// The body declares 60 s against the observer's 30 s: 60 s + 3 s + the 1 s step.
TEST(CASHeartbeatFloor, HolderDeclaringALongerLeaseIsWatchedForThatLease)
{
    Ops ops(std::make_shared<InMemoryBackend>());
    Layout l("p");
    seedMount(ops.op, l, "s1", /*expires*/ kNowMs + 60'000, /*fenced*/ false, /*min_active_build_sequence*/ 0);
    MountObservationMap obs;
    std::set<String> warned;
    floorAt(ops, l, 0, obs, warned);
    EXPECT_EQ(floorAt(ops, l, kStableThresholdMs, obs, warned).fenced_now, 0u)
        << "fenced after the observer's own lease while the declared lease still ran";
    EXPECT_EQ(floorAt(ops, l, 63'999, obs, warned).fenced_now, 0u);
    EXPECT_EQ(floorAt(ops, l, 64'000, obs, warned).fenced_now, 1u);
}

/// A shorter declared lease, a missing start stamp, equal stamps, an expiry before the start.
TEST(CASHeartbeatFloor, ShorterOrUnusableDeclaredLeaseIsWatchedForTheObserversOwn)
{
    Ops ops(std::make_shared<InMemoryBackend>());
    Layout l("p");
    seedMount(ops.op, l, "s1", /*expires*/ kNowMs + 10'000, false, 0);
    seedMount(ops.op, l, "s2", /*expires*/ 60'000, false, 0, /*seq*/ 1, /*started_at_ms*/ 0);
    seedMount(ops.op, l, "s3", /*expires*/ kNowMs, false, 0);
    seedMount(ops.op, l, "s4", /*expires*/ kNowMs - 1, false, 0);
    MountObservationMap obs;
    std::set<String> warned;
    floorAt(ops, l, 0, obs, warned);
    EXPECT_EQ(floorAt(ops, l, kStableThresholdMs - 1, obs, warned).fenced_now, 0u);
    EXPECT_EQ(floorAt(ops, l, kStableThresholdMs, obs, warned).fenced_now, 4u);
}

/// `gc_fenced` and farewell slots stay terminal whatever lease they declare: no observation, no write.
TEST(CASHeartbeatFloor, TerminalSlotsAreExcludedWhateverTheyDeclare)
{
    Ops ops(std::make_shared<InMemoryBackend>());
    Layout l("p");
    seedMount(ops.op, l, "s1", /*expires*/ kNowMs + 60'000, /*fenced*/ true, 0);
    seedMount(ops.op, l, "s2", /*expires*/ kNowMs + 60'000, false, std::numeric_limits<uint64_t>::max());
    MountObservationMap obs;
    std::set<String> warned;
    const HeartbeatFloor floor = floorAt(ops, l, 0, obs, warned);
    EXPECT_EQ(floor.already_fenced, 1u);
    EXPECT_EQ(floor.terminated, 1u);
    EXPECT_TRUE(obs.empty());
}
```
- [ ] **Test (round and rebuild).** In `gtest_cas_gc_ack_floor.cpp`, inside the anonymous namespace after
  `runSlowWalkSightingScenario`, add:
```cpp
/// Defaults give the observer 30 s + 1.5 s + 10 s = 41.5 s; the holder's 60 s gives 73 s.
void runLongerDeclaredLeaseScenario(SightingCall early_call)
{
    uint64_t mono = 0;
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    const Layout & layout = store->layout();
    ASSERT_EQ(store->poolConfig().mount_lease_ttl_ms.count(), 30'000);
    ASSERT_EQ(store->poolConfig().mount_renew_period.count(), 10'000);
    Gc gc(store, kGc, [] { return uint64_t{1'000'000}; }, [&mono] { return mono; });

    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref("srv-a:1", 1, 0xAA);
    writeBlobBody(*backend, layout, DB::UInt128(7));
    writeManifestRaw(*backend, layout, ns, r, {blobEntryFor("a", DB::UInt128(7))});
    publishCommittedTransition(*backend, layout, ns, "tbl", std::nullopt, r);

    /// Claims once with a 60 s lease and never renews.
    const String holder_srid = "long-lease-server";
    CasRequests renewer_requests = openRequestsForTest(backend);
    MountLeaseRenewer holder(renewer_requests, renewer_requests, layout, holder_srid, DB::UInt128(0x3333),
        /*writer_epoch=*/1, std::chrono::milliseconds(60'000), [] { return 1000u; }, [] { return 0u; }, {},
        std::chrono::milliseconds(0), [] { return 0u; });
    holder.start();
    const auto holder_fenced = [&]
    { return decodeMountLease(readObj(*backend, layout.mountKey(holder_srid))->bytes).gc_fenced; };

    ASSERT_EQ(gc.runRegularRound().fence_outs, 0u);
    /// The store's own slot renews before each decision so only the holder can be fenced.
    store->renewWatermarkOnce();
    mono = 41'500;
    if (early_call == SightingCall::Round)
        EXPECT_EQ(gc.runRegularRound().fence_outs, 0u);
    else
        ASSERT_TRUE(gc.rebuildBaseline(/*force*/ true).performed);
    EXPECT_FALSE(holder_fenced()) << "fenced after the observer's own lease while the declared lease still ran";

    store->renewWatermarkOnce();
    mono = 72'999;
    EXPECT_EQ(gc.runRegularRound().fence_outs, 0u);
    store->renewWatermarkOnce();
    mono = 73'000;
    EXPECT_EQ(gc.runRegularRound().fence_outs, 1u);
    EXPECT_TRUE(holder_fenced());
}
```
  and after `TEST(CASGcFenceOut, RebuildCountsASightingFromAfterItsRead)`:
```cpp
TEST(CASGcFenceOut, RoundWaitsForTheHoldersLongerLease) { runLongerDeclaredLeaseScenario(SightingCall::Round); }
TEST(CASGcFenceOut, RebuildWaitsForTheHoldersLongerLease) { runLongerDeclaredLeaseScenario(SightingCall::Rebuild); }
```
  (Format both on four lines, Allman.)
- [ ] **Run** `CASHeartbeatFloor.*Declar*:CASHeartbeatFloor.*Stamps*:CASHeartbeatFloor.Terminal*:CASGcFenceOut.*Longer*`.
  Expect red: `HolderDeclaringALongerLeaseIsWatchedForThatLease` (`fenced_now` Which is: 1, expected 0),
  `RoundWaitsForTheHoldersLongerLease` and `RebuildWaitsForTheHoldersLongerLease` (fenced at 41'500).
  Green before and after the fix (golden): `ShorterOrUnusable…`, `Terminal…`.
- [ ] **Implement.** In `computeHeartbeatFloor`, `/*declared_lease_ms=*/0` → `declaredMountLeaseMs(m)`.
  In `CasGc.cpp`, replace the round's comment block `/// Etag-guarded fence-out of dead mounts …` with:
```cpp
    /// Etag-guarded fence-out of dead mounts (liveness only — graduation paces on GC rounds via
    /// `new_round`). A slot is fenced only after `mount_obs` has watched its token hold unchanged on
    /// this leader's own monotonic clock for `mountObservationThresholdMs` over the longer of this
    /// server's lease and the one the slot declares, with the full renewal period as the step.
```
- [ ] **Run** the filter: all green. Gate `CAS*`: green.
- [ ] **Commit** the four files, message `Fence a mount slot only after the lease its holder declares`.

### Task 4: Restart reclaim waits for the declared lease

**Files:** `CA/Pool/CasServerRoot.h`, `CA/Pool/CasServerRoot.cpp`, `src/Disks/tests/gtest_cas_mount.cpp`.

- [ ] **Test.** Delete `TEST(CASMountAwaitExpiry, SkewedFarFutureExpiryHasNoEffectOnObservationThreshold)`
  and its comment: it asserts the rule this PR replaces. Put in its place:
```cpp
namespace
{
/// Restart reclaim as this server with a 30 s lease and a 1 s poll. Returns the observer clock at the
/// reclaim and records every threshold `on_wait_start` reports.
uint64_t reclaimAsShortLeaseObserver(Ops & ops, const Layout & l, std::vector<uint64_t> & thresholds)
{
    uint64_t mono = 0;
    const auto r = claimMountAwaitingExpiry(
        ops.op, l, "r", UInt128(1), /*our_epoch*/ 8, [] { return uint64_t{1000}; }, [&] { return mono; },
        /*ttl*/ 30'000, /*poll*/ 1'000, [&](uint64_t ms) { mono += ms; },
        [&](const MountLease &, uint64_t threshold_ms) { thresholds.push_back(threshold_ms); });
    EXPECT_EQ(r.kind, MountClaimResult::Claimed);
    EXPECT_EQ(r.prior, MountPriorState::UncleanObserved);
    return mono;
}
}

TEST(CASMountAwaitExpiry, HolderDeclaringALongerLeaseIsWaitedOutInFull)
{
    Ops ops(std::make_shared<InMemoryBackend>());
    Layout l("p");
    ASSERT_EQ(claimMount(ops.op, l, "r", UInt128(1), 7, /*now*/ 1000, /*ttl*/ 60'000).kind, MountClaimResult::Claimed);
    std::vector<uint64_t> thresholds;
    EXPECT_EQ(reclaimAsShortLeaseObserver(ops, l, thresholds), 64'000u)
        << "reclaimed before the predecessor's declared lease ran out";
    EXPECT_EQ(thresholds, std::vector<uint64_t>{64'000});
}

/// A shorter declared lease (10 s), a missing start stamp, equal stamps, an expiry before the start.
TEST(CASMountAwaitExpiry, ShorterOrUnusableDeclaredLeaseWaitsTheObserversOwn)
{
    const std::vector<std::pair<uint64_t, uint64_t>> stamps{{1'000, 11'000}, {0, 60'000}, {5'000, 5'000}, {70'000, 10'000}};
    for (const auto & [started, expires] : stamps)
    {
        SCOPED_TRACE(fmt::format("started_at_ms={} expires_at_ms={}", started, expires));
        Ops ops(std::make_shared<InMemoryBackend>());
        Layout l("p");
        const MountLease m{.server_uuid = UInt128(1), .writer_epoch = 7, .hostname = "h", .pid = 1,
            .started_at_ms = started, .seq = 1, .expires_at_ms = expires, .write_attempt_id = UInt128(1)};
        mustCommit(ops.op.create(l.mountKey("r"), encodeMountLease(m), Retry::standard()), "seeded mount");
        std::vector<uint64_t> thresholds;
        EXPECT_EQ(reclaimAsShortLeaseObserver(ops, l, thresholds), 33'000u);
        EXPECT_EQ(thresholds, std::vector<uint64_t>{32'500});
    }
}

/// The certificates of death need no wait, whatever lease the body declares.
TEST(CASMountAwaitExpiry, FencedAndFarewellSlotsReclaimWithoutWaitingWhateverTheyDeclare)
{
    for (const bool fenced : {true, false})
    {
        Ops ops(std::make_shared<InMemoryBackend>());
        Layout l("p");
        MountLease m{.server_uuid = UInt128(1), .writer_epoch = 7, .hostname = "h", .pid = 1,
            .started_at_ms = 1000, .seq = 1, .expires_at_ms = 61'000, .write_attempt_id = UInt128(1)};
        m.gc_fenced = fenced;
        m.min_active_build_sequence = fenced ? 0 : std::numeric_limits<uint64_t>::max();
        mustCommit(ops.op.create(l.mountKey("r"), encodeMountLease(m), Retry::standard()), "seeded mount");
        int sleeps = 0;
        const auto r = claimMountAwaitingExpiry(ops.op, l, "r", UInt128(1), 8, [] { return uint64_t{1000}; },
            [] { return uint64_t{0}; }, 30'000, 1'000, [&](uint64_t) { ++sleeps; });
        EXPECT_EQ(r.kind, MountClaimResult::Claimed);
        EXPECT_EQ(r.prior, fenced ? MountPriorState::Fenced : MountPriorState::Clean);
        EXPECT_EQ(sleeps, 0);
    }
}
```
- [ ] **Run** `CASMountAwaitExpiry.*`. Expect red only in `HolderDeclaringALongerLeaseIsWaitedOutInFull`
  (Which is: 33000, expected 64000; thresholds `{ 32500 }`). The other two are golden (green on `bc7f453aa4a` too).
- [ ] **Implement.** In `claimMountAwaitingExpiry`, replace the comment block and line
  `const uint64_t threshold_ms = mountObservationThresholdMs(ttl_ms, /*declared_lease_ms=*/0, poll);` with:
```cpp
    /// Set when an observation starts, from the observed body; one token is one body, so the threshold
    /// of an observation never changes.
    uint64_t threshold_ms = 0;
```
  In the `if (!observed || *observed != *current_etag)` branch, replace `if (on_wait_start && r.body)` and its line with:
```cpp
            if (!r.body)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS mount '{}': observed a mount token without its body", srid);
            threshold_ms = mountObservationThresholdMs(ttl_ms, declaredMountLeaseMs(*r.body), poll);
            if (on_wait_start)
                on_wait_start(*r.body, threshold_ms);
```
  The throw is unreachable: every `LiveDoubleStart` with a token carries its body, and the re-read branch
  fills both. It has no test, because a `LOGICAL_ERROR` test would be a death test.
  In `CasServerRoot.h`, in the `claimMountAwaitingExpiry` comment, replace
  "the full rate-bound threshold (`ttl_ms + ttl_ms / 20 + poll_interval_ms` — the lease TTL, a 5%
  clock-drift allowance, and one poll interval of discreteness. This rate bound ensures that a holder
  which last renewed before the observation began can no longer be within its lease."
  with "the threshold `mountObservationThresholdMs(ttl_ms, declaredMountLeaseMs(body), poll_interval_ms)` —
  after it, a holder that last renewed before the observation began is outside its own lease, whichever
  lease it runs;".
  In `mountDoubleStartMessage`, replace the three lines from `"   an authorization. Every server sharing this pool must run the SAME …`
  to `"   out a healthy one.\n"` with:
```cpp
        "   an authorization. The threshold uses the longer of this server's cas_mount_lease_ttl_ms and the\n"
        "   lease the holder's body declares (expires_at_ms - started_at_ms).\n"
```
- [ ] **Run** `CASMountAwaitExpiry.*:CASMountObservation.*:CASMount*DoubleStart*`, then the gate: green.
- [ ] **Commit** the three files, message `Wait out the lease a predecessor declares before reclaiming`.

### Task 5: `WARNING` when a holder declares a different lease

**Files:** `CA/Pool/CasServerRoot.h`, `CA/Pool/CasServerRoot.cpp`, `src/Disks/tests/cas_test_helpers.h`,
`gtest_cas_mount.cpp`, `gtest_cas_gc_ack_floor.cpp`.

- [ ] **Test.** `git show 03330f1b96d -- src/Disks/tests/cas_test_helpers.h | git apply` (`ScopedLogCapture`).
  From `git show 03330f1b96d -- src/Disks/tests/gtest_cas_mount.cpp`, copy the six tests from
  `HolderWithADifferentLeaseIsWarnedAboutWithoutChangingTheClassification` to `UnseenSridPrunedFromLeaseMismatchWarned`
  and `kLeaseMismatchMarker` (the `seedMount` hunk is already in). Then re-target them:
  `kStableThresholdMs, obs,\n kObserverLeaseMs, warned` and `kStableThresholdMs, obs, kObserverLeaseMs, warned` → `kObserverLeaseMs, kCadenceMs, obs, warned`;
  `kLeaseMismatchMarker = "advertises a lease"` → `"declares a lease"`; the expected text becomes
  `"This server is configured with cas_mount_lease_ttl_ms=30000 but the holder of slot s1 (hostname=h-s1, pid=100) declares a lease of 60000 ms. Observers wait for the longer of the two; a longer lease on one server delays recovery after that server's crash."`.
  From `git show 03330f1b96d -- src/Disks/tests/gtest_cas_gc_ack_floor.cpp`, copy `TEST(CASGCLeaseMismatch, RoundWarnsOnceAboutAHolderWithADifferentLease)`
  and change `"advertises a lease"` → `"declares a lease"` and `"advertises a lease of 100 ms."` → `"declares a lease of 100 ms."`.
- [ ] **Run** `CASHeartbeatFloor.*Warn*:CASHeartbeatFloor.*LeaseMismatch*:CASGCLeaseMismatch.*`.
  Expect red: `HolderWithADifferentLease…`, `LeaseMismatchIsWarnedOncePerServerRootId`, `UnseenSridPrunedFromLeaseMismatchWarned`
  and `RoundWarnsOnce…` (count 0). The three "is not warned about" tests stay green (golden).
- [ ] **Implement.** Take `warnOnceIfHolderLeaseDiffers` and the prune line from `03330f1b96d`'s
  `CasServerRoot.cpp` hunk, with this body:
```cpp
/// The setting is local, so different values are legal; a longer one slows recovery after its holder's
/// crash, which the operator should know about.
void warnOnceIfHolderLeaseDiffers(
    const String & srid, const MountLease & holder, uint64_t observer_lease_ms, std::set<String> & warned)
{
    const uint64_t declared_ms = declaredMountLeaseMs(holder);
    if (declared_ms == 0 || declared_ms == observer_lease_ms || !warned.insert(srid).second)
        return;
    LOG_WARNING(getLogger("CasHeartbeatFloor"),
        "This server is configured with cas_mount_lease_ttl_ms={} but the holder of slot {} (hostname={}, pid={}) "
        "declares a lease of {} ms. Observers wait for the longer of the two; a longer lease on one server "
        "delays recovery after that server's crash.",
        observer_lease_ms, srid, holder.hostname, holder.pid, declared_ms);
}
```
  Name the `lease_mismatch_warned` parameter. Call the helper right after the decode (before the
  `threshold_ms =` line). After the `obs` prune loop, add
  `std::erase_if(lease_mismatch_warned, [&](const String & s) { return !seen_srids.contains(s); });`
  and change the comment above it to "Prune every `obs` and `lease_mismatch_warned` entry for a srid this
  pass's walk never saw at all.". Append to the header comment of `computeHeartbeatFloor`:
  "A body whose declared lease is non-zero and differs from `observer_lease_ms` is named in one `WARNING`
  per srid; `lease_mismatch_warned` holds the srids already named and is pruned like `obs`. The check
  never changes a classification or a write."
- [ ] **Run** the filter, then the gate: green.
- [ ] **Commit** the five files, message `Warn once per slot when a holder declares a different lease`.

### Task 6: Docs, gates, mutation checks

**Files:** `docs/en/antalya/cas/architecture/mounts-and-leases.md`, `docs/en/antalya/cas/configuration.md`,
`docs/en/antalya/cas/architecture/garbage-collection.md`. No heading changes, so no anchors are needed.

- [ ] **`mounts-and-leases.md`.** Replace the text from "GC's own view of a dead server …" through
  "…does not make mixed thresholds safe." with the block below. Keep the following sentence
  "With the defaults (TTL 30 s, period 10 s, margin 2 s), …" as its own paragraph.
```markdown
GC's own view of a dead server is symmetric and clock-skew-immune: a slot becomes fence-eligible
only after the leader observes the *same* renewal token hold stable, on its own monotonic clock,
for `L + floor(L/20) + period`. `L` is the longer of the leader's own `cas_mount_lease_ttl_ms` and the
lease the slot's body declares (`expires_at_ms − started_at_ms`). A body without stamps, or with a
non-positive interval (a farewell), declares nothing. A re-mounting server waits out a predecessor the
same way with the step `max(1, floor(period/2))`. `period` is always the observer's own. The two stamps
are compared only with each other, never with the observer's clock. `expires_at_ms` alone is a
writer-stamped diagnostic used by `system.cas_mounts` and the non-authoritative decommission
epoch-recovery precheck. Local fencing is derived from the confirmed request's pre-I/O `BOOTTIME`
anchor plus the TTL, and wall-clock `now` stays audit-only.

`cas_mount_lease_ttl_ms` is local to each server. It is how long this server may write after its last
confirmed renewal, and every other server waits at least this long before fencing or reclaiming its
slot. Mixed values are safe, and so is changing the value one server at a time in any order. A longer
lease on one server delays recovery after that server's crash, and the GC leader logs one `WARNING` per
slot whose declared lease differs from its own. A server built without this rule waits only its own
lease: keep the values equal until every server of the pool runs a build with the rule.
```
- [ ] **`configuration.md`.** `cas_mount_lease_ttl_ms` row description → "How long this server may write
  after its last confirmed renewal; every other server waits at least this long before fencing or
  reclaiming its slot (≥ 1). Lower values shorten recovery after this server's crash but reduce
  tolerance for object-storage and scheduling delays". Replace the paragraph "All servers sharing a pool
  must run the same …" (up to "…does not make mixed thresholds safe.") with:
```markdown
Both settings are local. Startup reclaim and GC's fence-out judge liveness by the mount slot's write
token holding stable on the observer's own `CLOCK_BOOTTIME` for `L + floor(L / 20) + step`. `L` is the
longer of the observer's `cas_mount_lease_ttl_ms` and the lease the holder's body declares
(`expires_at_ms − started_at_ms`; a body without stamps or with a non-positive interval declares
nothing). Startup's step is `max(1, floor(cas_mount_renew_period_ms / 2))` and GC's is
`cas_mount_renew_period_ms`, both the observer's own. Mixed values are therefore safe in any order;
a longer lease on one server delays recovery after that server's crash. A server built without this
rule waits only its own lease, so keep the values equal until every server of the pool runs a build
with it.
```
- [ ] **`garbage-collection.md`.** In `heartbeat_floor` **Safety**, replace
  "`cas_mount_lease_ttl_ms + 5% + cas_mount_renew_period_ms`\n  (defaults 30 s and 10 s; every server sharing the pool must run the same values)"
  with "`L + 5% + cas_mount_renew_period_ms`, where `L` is the longer of this server's
  `cas_mount_lease_ttl_ms` and the lease the slot's body declares (defaults 30 s and 10 s)".
- [ ] **Check** `git -C W grep -n -i "same \`cas_mount\|identical \`cas_mount\|pool stopped\|SAME cas_mount" -- docs/en src`: no hits.
- [ ] **Gates.** Release `CAS*` and `build_asan` `CAS*`: green. Then run
  `python -m ci.praktika run "integration" --test test_cas_mount_renewal_retry` from `W` with the unchanged
  module: green. No new integration scenario. Two same-pool nodes with different leases plus a crash and
  real-time waits of 60 s or more is not cheap in this module, which has no same-pool node pair. The unit
  tests of Tasks 3–4 carry the behaviour.
  If another existing `CAS*` test turns red, its holder body declares a longer lease than its observer.
  Advance its fake clock to the new threshold and say so in the commit. Never weaken the rule.
- [ ] **Mutation checks** (one at a time in `CasServerRoot.cpp`; rebuild; run the filter; restore with
  `git checkout -- CA/Pool/CasServerRoot.cpp`; commit nothing):

| Mutation | Must go red |
|---|---|
| `std::max(own_lease_ms, declared_lease_ms)` → `own_lease_ms` | `LongerOfOwn…`, `HolderDeclaringALongerLease…` (floor and await), `CASGcFenceOut.*Longer*` |
| → `declared_lease_ms` | `LongerOfOwn…`, both `ShorterOrUnusable…` |
| `declaredMountLeaseMs(m)` → `0` in `computeHeartbeatFloor` | floor `HolderDeclaringALongerLease…`, `CASGcFenceOut.*Longer*` |
| `declaredMountLeaseMs(*r.body)` → `0` | `CASMountAwaitExpiry.HolderDeclaringALongerLeaseIsWaitedOutInFull` |
| drop the `addOverflow` branch (plain sum) | `SaturatesInsteadOfWrapping` |
| drop `m.started_at_ms == 0 \|\|` | `DeclaredLeaseIsThePositiveStampInterval`, both `ShorterOrUnusable…` |

- [ ] **Commit** the three docs, message `Document cas_mount_lease_ttl_ms as a per-server setting`.

**PR.** Title `Make CAS observers wait for the lease a mount holder declares`. Changelog category
"Improvement". Entry: "CAS GC fence-out and same-`server_root_id` restart reclaim now wait for the
longer of their own `cas_mount_lease_ttl_ms` and the lease the slot holder declares, so servers with
different lease settings no longer fence or reclaim each other early."
