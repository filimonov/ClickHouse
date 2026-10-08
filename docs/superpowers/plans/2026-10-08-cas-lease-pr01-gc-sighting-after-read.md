# CAS lease PR 1: GC dates a mount-token sighting after the read that returned it

**Goal.** `computeHeartbeatFloor` credits the first sighting of a mount token with a clock sample
taken after the slot read, in the first decision and in the re-decision after a refused fence-out.
The stability test keeps one sample taken at entry, before any read. The "elapsed" arithmetic never
wraps: a sample older than the sighting never counts as held. A failed slot read keeps the watch
already accumulated. Closes CAS-342.

**Architecture.** `computeHeartbeatFloor` (`CA/Pool/CasServerRoot.cpp`) takes the observer clock as
`const std::function<uint64_t()> &` instead of a `uint64_t` sample. It samples it once at entry
(`round_start_ms`, used only by the stability test) and again inside the decision lambda each time
it records a new sighting. `MountIncarnationObservation` gains `heldFor`, a saturating comparison.
Both callers in `CA/Gc/CasGc.cpp` (`Gc::runRegularRound` and `Gc::rebuildBaseline`) pass the
`mono_ms_fn` member instead of `mono_ms_fn()`.

**Tech stack.** C++23, gtest (`unit_tests_dbms`), `InMemoryBackend` subclasses that move an injected
clock on slot reads; TLC for the model check.

**Spec.** `docs/superpowers/specs/2026-10-07-cas-mount-lease-series-design.md`, revision 7: section 3
"Наблюдатели" (rows "первое наблюдение датируется" and "после неудачного fence-out"), section 7 PR
row 1 and tests row 1, section 9 row `CaMountObserveCore`. Backlog:
`docs/superpowers/cas/backlog/tasks/cas-342 - Timestamp-a-GC-mount-sighting-after-its-read-so-a-slow-round-cannot-fence-a-holder-that-still-has-authority.md`.

**Base and branch.** `altinity/antalya-26.6` at `5b7c9f503df`. Branch
`fix/antalya-26.6/cas-gc-sighting-after-read`. `CA/` =
`src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`. No file outside `CA/` and
`src/Disks/tests/` is touched. No on-storage format change.

**Signature this PR leaves for PR 2.** `computeHeartbeatFloor(CasOperation & op, const Layout & l,
uint64_t now_ms, const std::function<uint64_t()> & mono_ms_fn, uint64_t stable_threshold_ms,
MountObservationMap & obs)`.

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

## Review Focus

- Two samples, two jobs: `round_start_ms` (before any read) only confirms; `mono_ms_fn()` inside the
  lambda only dates a sighting. A swap in either direction is the defect class this PR fixes.
- The re-decision after a refused fence-out goes through the same lambda, so it dates its sighting
  after the resolve read. `CASHeartbeatFloor.RedecisionAfterARefusedFenceCountsFromItsRead` pins it.
- `heldFor` returns false when the sample precedes the sighting. With today's callers the calls are
  serialized (`CasGcScheduler::gc_round_mutex`) on a monotonic clock, so the wrap needs a clock that
  goes backwards; the check makes that case fail safe (no fence) instead of fencing.
- The Gc-level tests move the clock inside the backend only on reads of one slot key. They assert
  that key was read exactly once in the sighting call, so a second reader would fail them loudly.
- No test expects `LOGICAL_ERROR`.

## Files

| File | Change |
|---|---|
| `CA/Pool/CasServerRoot.h` | `computeHeartbeatFloor` signature; `MountIncarnationObservation::heldFor`; two doc sentences |
| `CA/Pool/CasServerRoot.cpp` | entry sample, `heldFor`, sighting sampled after the read |
| `CA/Gc/CasGc.cpp` | both callers pass `mono_ms_fn` |
| `src/Disks/tests/gtest_cas_mount.cpp` | 12 call sites migrate to `frozenClock`; `SlotClockBackend`; 5 new tests |
| `src/Disks/tests/gtest_cas_gc_ack_floor.cpp` | `SlotReadClockBackend`, scenario helper, 2 new tests |

The unit fixtures for `computeHeartbeatFloor` (`seedMount`, `Ops`, `kStableThresholdMs`) live in
`gtest_cas_mount.cpp`, and the Gc-level fence-out scenario lives in `gtest_cas_gc_ack_floor.cpp`.
`gtest_cas_heartbeat.cpp` tests `MountLeaseRenewer` only and is not touched.

---

## Task 0: Branch

- [ ] **Step 1.** In `/home/mfilimonov/workspace/ClickHouse/cas-95-1`:
  ```bash
  git fetch altinity antalya-26.6
  git switch -c fix/antalya-26.6/cas-gc-sighting-after-read 5b7c9f503df
  ```

## Task 1: Observer clock as a function (behaviour-preserving) + golden tests

**Consumes:** base code. **Produces:** the new signature; `frozenClock`; `SlotClockBackend`;
`kWalkMs`; two golden tests green now and after Task 3.

- [ ] **Step 1: enumerate call sites.** Run
  `git grep -n 'computeHeartbeatFloor(' -- src programs utils | grep -v '///'`.
  Expected on `5b7c9f503df`: the declaration (`CasServerRoot.h`), the definition
  (`CasServerRoot.cpp`), two production calls in `CA/Gc/CasGc.cpp` (in `Gc::runRegularRound` and
  `Gc::rebuildBaseline`), and 12 test calls, all in `src/Disks/tests/gtest_cas_mount.cpp`. If the
  output differs, migrate every listed call and note the difference in the PR description.

- [ ] **Step 2: header.** In `CA/Pool/CasServerRoot.h`, change the declaration:
  ```cpp
  HeartbeatFloor computeHeartbeatFloor(CasOperation & op, const Layout & l, uint64_t now_ms,
                                       const std::function<uint64_t()> & mono_ms_fn,
                                       uint64_t stable_threshold_ms, MountObservationMap & obs);
  ```
  In the doc comment above `struct HeartbeatFloor`, replace
  ```
  ///     fence-out): `obs` remembers, per srid, the incarnation last seen and the leader's OWN
  ///     monotonic clock reading (`mono_now_ms`) at the moment it first saw it. A body whose
  ```
  with
  ```
  ///     fence-out): `obs` remembers, per srid, the incarnation last seen and the reading of the
  ///     leader's OWN monotonic clock (`mono_ms_fn`) when it first saw it. A body whose
  ```
  and replace
  ```
  /// `mono_now_ms` is the OBSERVATION clock: the caller's OWN monotonic reading, never compared against
  ```
  with
  ```
  /// `mono_ms_fn` is the OBSERVATION clock: the caller's OWN monotonic clock, never compared against
  ```

- [ ] **Step 3: definition.** In `CA/Pool/CasServerRoot.cpp`, `computeHeartbeatFloor`: change the
  parameter `uint64_t mono_now_ms` to `const std::function<uint64_t()> & mono_ms_fn` (same position),
  add after `HeartbeatFloor floor;`:
  ```cpp
      const uint64_t round_start_ms = mono_ms_fn();
  ```
  and replace both remaining uses of `mono_now_ms` in the decision lambda with `round_start_ms`
  (the `stable` expression and the `MountIncarnationObservation{observed->etag, mono_now_ms}`).
  `grep -n mono_now_ms` on the two `CasServerRoot` files must print nothing.

- [ ] **Step 4: callers.** In `CA/Gc/CasGc.cpp`, both calls: `now_ms_fn(), mono_ms_fn(),` →
  `now_ms_fn(), mono_ms_fn,`.

- [ ] **Step 5: existing tests.** In `src/Disks/tests/gtest_cas_mount.cpp`, add to the anonymous
  namespace that ends after `renewMount` (just before its closing `}`):
  ```cpp

  /// An observer clock that does not move during a call: the entry sample and every sighting read the
  /// same value.
  std::function<uint64_t()> frozenClock(uint64_t ms)
  {
      return [ms] { return ms; };
  }
  ```
  Then migrate the 12 calls and the one comment:
  ```bash
  sed -i -e 's|/\*mono_now_ms\*/ 0|frozenClock(0)|' \
         -e 's|/\*mono\*/ kStableThresholdMs|frozenClock(kStableThresholdMs)|g' \
         -e 's|/\*mono\*/ 0|frozenClock(0)|g' \
         -e 's|monotonic clock (`mono_now_ms`)|monotonic clock (`mono_ms_fn`)|' \
         src/Disks/tests/gtest_cas_mount.cpp
  grep -n '/\*mono\|mono_now_ms' src/Disks/tests/gtest_cas_mount.cpp   # must print nothing
  ```

- [ ] **Step 6: golden tests.** In `gtest_cas_mount.cpp`, insert after the closing `}` of
  `TEST(CASHeartbeatFloor, FenceOutLosesTheIncarnationRaceAndReclassifiesLive)`:
  ```cpp
  namespace
  {
  /// Moves the observer clock by `read_cost_ms` on every read of a mount slot, so a call spends that
  /// long between its entry sample and the read it decides on. `renew_before_next_fence` makes the
  /// holder renew just before the next guarded write of a slot lands, so the fence-out is refused and
  /// decided again on the new token. `fail_slot_reads` makes every slot read time out.
  class SlotClockBackend : public InMemoryBackend
  {
  public:
      explicit SlotClockBackend(uint64_t & mono_) : mono(mono_) {}

      uint64_t read_cost_ms = 0;
      bool renew_before_next_fence = false;
      bool fail_slot_reads = false;

      std::optional<Raw> read(const String & key, TransportAccess & access) override
      {
          if (key.ends_with("/mount"))
          {
              if (fail_slot_reads)
                  throw Poco::TimeoutException("injected: mount slot read timed out");
              mono += read_cost_ms;
          }
          return InMemoryBackend::read(key, access);
      }

      std::expected<String, RawConflict> write(const String & key, const String & bytes,
                                               const std::optional<String> & expected_value,
                                               TransportAccess & access) override
      {
          if (renew_before_next_fence && expected_value && key.ends_with("/mount"))
          {
              renew_before_next_fence = false;
              const auto got = InMemoryBackend::read(key, access);
              MountLease m = decodeMountLease(got->bytes);
              m.seq += 1;
              EXPECT_TRUE(InMemoryBackend::write(key, encodeMountLease(m), got->value, access).has_value());
          }
          return InMemoryBackend::write(key, bytes, expected_value, access);
      }

  private:
      uint64_t & mono;
  };

  constexpr uint64_t kWalkMs = 4'000;
  }

  TEST(CASHeartbeatFloor, ConfirmingCallDecidesOnItsEntrySample)
  {
      uint64_t mono = 0;
      auto b = std::make_shared<SlotClockBackend>(mono);
      Layout l("p");
      Ops ops(b);
      seedMount(ops.op, l, "s1", /*expires*/ 10, /*fenced*/ false, /*min_active_build_sequence*/ 0);
      const auto clock = [&mono] { return mono; };
      MountObservationMap obs;

      computeHeartbeatFloor(ops.op, l, kNowMs, clock, kStableThresholdMs, obs);
      ASSERT_EQ(obs.at("s1").first_seen_mono_ms, 0u);

      /// The call starts one millisecond short of the threshold; its walk passes the threshold many times.
      mono = kStableThresholdMs - 1;
      b->read_cost_ms = 10 * kStableThresholdMs;
      const HeartbeatFloor slow = computeHeartbeatFloor(ops.op, l, kNowMs, clock, kStableThresholdMs, obs);
      EXPECT_EQ(slow.fenced_now, 0u);
      EXPECT_EQ(slow.live, 1u);
      EXPECT_EQ(obs.at("s1").first_seen_mono_ms, 0u) << "an unchanged token keeps its first sighting";

      b->read_cost_ms = 0;
      EXPECT_EQ(computeHeartbeatFloor(ops.op, l, kNowMs, clock, kStableThresholdMs, obs).fenced_now, 1u);
  }

  TEST(CASHeartbeatFloor, FailedSlotReadKeepsTheWatch)
  {
      uint64_t mono = 0;
      auto b = std::make_shared<SlotClockBackend>(mono);
      Layout l("p");
      Ops ops(b);
      seedMount(ops.op, l, "s1", /*expires*/ 10, /*fenced*/ false, /*min_active_build_sequence*/ 0);
      const auto clock = [&mono] { return mono; };
      MountObservationMap obs;

      computeHeartbeatFloor(ops.op, l, kNowMs, clock, kStableThresholdMs, obs);
      ASSERT_EQ(obs.at("s1").first_seen_mono_ms, 0u);

      mono = kStableThresholdMs / 2;
      b->fail_slot_reads = true;
      EXPECT_ANY_THROW(computeHeartbeatFloor(ops.op, l, kNowMs, clock, kStableThresholdMs, obs));
      ASSERT_TRUE(obs.contains("s1"));
      EXPECT_EQ(obs.at("s1").first_seen_mono_ms, 0u);

      b->fail_slot_reads = false;
      mono = kStableThresholdMs;
      EXPECT_EQ(computeHeartbeatFloor(ops.op, l, kNowMs, clock, kStableThresholdMs, obs).fenced_now, 1u)
          << "a failed read restarted the observation";
  }
  ```

- [ ] **Step 7: build and run.**
  ```bash
  ninja -C build unit_tests_dbms > build/ninja_pr01_t1.log 2>&1; echo NINJA_EXIT=$?
  build/src/unit_tests_dbms --gtest_filter='CASHeartbeatFloor.*:CASGcFenceOut.*:CASGCAckFloor.ExpiredMountFencedOutAndExcluded:CASGCRebuild.*' > build/test_pr01_t1.log 2>&1; echo TEST_EXIT=$?
  ```
  Expected: `NINJA_EXIT=0`, `TEST_EXIT=0`. The two new tests are behaviour-preservation tests: green
  here and after Task 3. `ConfirmingCallDecidesOnItsEntrySample` would fail if the stability test
  sampled after the read; `FailedSlotReadKeepsTheWatch` would fail if a read error erased the watch.

- [ ] **Step 8: commit.**
  ```bash
  git add -- src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h \
             src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.cpp \
             src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.cpp \
             src/Disks/tests/gtest_cas_mount.cpp
  git commit -m "Pass the GC observer clock to computeHeartbeatFloor as a function" \
             -m "Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
  ```

## Task 2: Elapsed time never wraps

**Consumes:** Task 1. **Produces:** `MountIncarnationObservation::heldFor`.

- [ ] **Step 1: failing test.** In `gtest_cas_mount.cpp`, after `FailedSlotReadKeepsTheWatch`:
  ```cpp
  TEST(CASHeartbeatFloor, SampleOlderThanTheSightingNeverFences)
  {
      auto b = std::make_shared<InMemoryBackend>();
      Layout l("p");
      Ops ops(b);
      seedMount(ops.op, l, "s1", /*expires*/ 10, /*fenced*/ false, /*min_active_build_sequence*/ 0);
      MountObservationMap obs;

      computeHeartbeatFloor(ops.op, l, kNowMs, frozenClock(kStableThresholdMs), kStableThresholdMs, obs);
      ASSERT_EQ(obs.at("s1").first_seen_mono_ms, kStableThresholdMs);

      /// A clock that went back: the unsigned difference would wrap past any threshold.
      const HeartbeatFloor floor = computeHeartbeatFloor(ops.op, l, kNowMs, frozenClock(kStableThresholdMs - 1),
                                                         kStableThresholdMs, obs);
      EXPECT_EQ(floor.fenced_now, 0u);
      EXPECT_EQ(floor.live, 1u);
  }
  ```

- [ ] **Step 2: run, expect red.** Build as in Task 1 Step 7 (log `build/ninja_pr01_t2.log`), then
  `build/src/unit_tests_dbms --gtest_filter='CASHeartbeatFloor.SampleOlderThanTheSightingNeverFences' > build/test_pr01_t2_red.log 2>&1`.
  Expected failures: `floor.fenced_now` `Which is: 1` vs `0u`, and `floor.live` `Which is: 0` vs
  `1u`.

- [ ] **Step 3: implement.** In `CasServerRoot.h`, `struct MountIncarnationObservation`, add after
  `first_seen_mono_ms`:
  ```cpp

      /// `sample_ms` is taken before the read that confirmed `etag`. A sample older than the sighting
      /// says nothing about how long the token held, so it never counts as held.
      bool heldFor(uint64_t threshold_ms, uint64_t sample_ms) const;
  ```
  In `CasServerRoot.cpp`, immediately before `HeartbeatFloor computeHeartbeatFloor(`:
  ```cpp
  bool MountIncarnationObservation::heldFor(uint64_t threshold_ms, uint64_t sample_ms) const
  {
      return sample_ms >= first_seen_mono_ms && sample_ms - first_seen_mono_ms >= threshold_ms;
  }

  ```
  and in the decision lambda replace
  ```cpp
                  const bool stable = it != obs.end() && it->second.etag == observed->etag
                      && round_start_ms - it->second.first_seen_mono_ms >= stable_threshold_ms;
  ```
  with
  ```cpp
                  const bool stable = it != obs.end() && it->second.etag == observed->etag
                      && it->second.heldFor(stable_threshold_ms, round_start_ms);
  ```

- [ ] **Step 4: run, expect green.** Rebuild (log `build/ninja_pr01_t2b.log`); run the Task 1
  Step 7 filter (log `build/test_pr01_t2.log`). Expected `TEST_EXIT=0`.

- [ ] **Step 5: commit.**
  ```bash
  git add -- src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h \
             src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.cpp \
             src/Disks/tests/gtest_cas_mount.cpp
  git commit -m "Never fence a mount on a sample older than its sighting" \
             -m "Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
  ```

## Task 3: Date every sighting after its read (the fix)

**Consumes:** Tasks 1–2. **Produces:** four regression tests, red before Step 4.

- [ ] **Step 1: unit tests.** In `gtest_cas_mount.cpp`, after `SampleOlderThanTheSightingNeverFences`:
  ```cpp
  TEST(CASHeartbeatFloor, FirstSightingCountsFromAfterItsRead)
  {
      uint64_t mono = 0;
      auto b = std::make_shared<SlotClockBackend>(mono);
      Layout l("p");
      Ops ops(b);
      seedMount(ops.op, l, "s1", /*expires*/ 10, /*fenced*/ false, /*min_active_build_sequence*/ 0);
      const auto clock = [&mono] { return mono; };
      MountObservationMap obs;

      /// The call starts at 0 and reaches the slot at `kWalkMs`.
      b->read_cost_ms = kWalkMs;
      ASSERT_EQ(computeHeartbeatFloor(ops.op, l, kNowMs, clock, kStableThresholdMs, obs).fenced_now, 0u);
      ASSERT_TRUE(obs.contains("s1"));
      EXPECT_EQ(obs.at("s1").first_seen_mono_ms, kWalkMs);

      /// One threshold after the first call started, less than one after its read.
      b->read_cost_ms = 0;
      mono = kStableThresholdMs;
      const HeartbeatFloor early = computeHeartbeatFloor(ops.op, l, kNowMs, clock, kStableThresholdMs, obs);
      EXPECT_EQ(early.fenced_now, 0u) << "fenced a token watched for less than the threshold";
      EXPECT_EQ(early.live, 1u);

      mono = kWalkMs + kStableThresholdMs;
      EXPECT_EQ(computeHeartbeatFloor(ops.op, l, kNowMs, clock, kStableThresholdMs, obs).fenced_now, 1u);
  }

  TEST(CASHeartbeatFloor, RedecisionAfterARefusedFenceCountsFromItsRead)
  {
      uint64_t mono = 0;
      auto b = std::make_shared<SlotClockBackend>(mono);
      Layout l("p");
      Ops ops(b);
      seedMount(ops.op, l, "s1", /*expires*/ 10, /*fenced*/ false, /*min_active_build_sequence*/ 0);
      const auto clock = [&mono] { return mono; };
      MountObservationMap obs;

      computeHeartbeatFloor(ops.op, l, kNowMs, clock, kStableThresholdMs, obs);
      ASSERT_TRUE(obs.contains("s1"));
      const Etag first = obs.at("s1").etag;

      /// Stable at this call's start, so it tries the fence-out. The holder renews first, the write is
      /// refused, and the call decides again on the new token after reading it.
      mono = kStableThresholdMs;
      b->read_cost_ms = kWalkMs;
      b->renew_before_next_fence = true;
      const HeartbeatFloor refused = computeHeartbeatFloor(ops.op, l, kNowMs, clock, kStableThresholdMs, obs);
      ASSERT_FALSE(b->renew_before_next_fence) << "the call never attempted the fence-out";
      ASSERT_EQ(refused.fenced_now, 0u);
      ASSERT_EQ(refused.live, 1u);
      ASSERT_NE(obs.at("s1").etag, first);

      /// Nothing reads after the re-decision, so the clock holds the value of the last read.
      const uint64_t reread_at = mono;
      ASSERT_GT(reread_at, kStableThresholdMs);
      EXPECT_EQ(obs.at("s1").first_seen_mono_ms, reread_at);

      /// One threshold after that call started, less than one after its re-read.
      b->read_cost_ms = 0;
      mono = 2 * kStableThresholdMs;
      EXPECT_EQ(computeHeartbeatFloor(ops.op, l, kNowMs, clock, kStableThresholdMs, obs).fenced_now, 0u)
          << "fenced the renewed token before it was watched for the threshold";

      mono = reread_at + kStableThresholdMs;
      EXPECT_EQ(computeHeartbeatFloor(ops.op, l, kNowMs, clock, kStableThresholdMs, obs).fenced_now, 1u);
  }
  ```

- [ ] **Step 2: Gc-level tests (round and rebuild).** In `src/Disks/tests/gtest_cas_gc_ack_floor.cpp`,
  inside the anonymous namespace that holds `runExpiredMountFenceOutScenario`, after that function:
  ```cpp
  /// Advances the GC's observer clock by `read_cost_ms` on every read of `watched_key`, so a GC call
  /// spends that long between its entry sample and the read it decides on. Atomics because the round's
  /// read-ahead pool reads other keys through this backend on other threads.
  class SlotReadClockBackend : public InMemoryBackend
  {
  public:
      String watched_key;
      std::atomic<uint64_t> mono{0};
      std::atomic<uint64_t> read_cost_ms{0};
      std::atomic<uint64_t> last_read_done_at{0};
      std::atomic<size_t> watched_reads{0};

      std::optional<Raw> read(const String & key, TransportAccess & access) override
      {
          if (key == watched_key)
          {
              last_read_done_at = mono += read_cost_ms.load();
              ++watched_reads;
          }
          return InMemoryBackend::read(key, access);
      }
  };

  enum class SightingCall : uint8_t
  {
      Round,
      Rebuild,
  };

  void runSlowWalkSightingScenario(SightingCall sighting_call)
  {
      auto backend = std::make_shared<SlotReadClockBackend>();
      auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
      const Layout & layout = store->layout();
      const String srid2 = "stale-server";
      backend->watched_key = layout.mountKey(srid2);

      const uint64_t ttl_ms = static_cast<uint64_t>(store->poolConfig().mount_lease_ttl_ms.count());
      const uint64_t threshold_ms = mountObservationThresholdMs(
          ttl_ms, static_cast<uint64_t>(store->poolConfig().mount_renew_period.count()));
      /// Longer than `threshold - TTL`: a sighting credited from before this walk fences a holder
      /// whose last renewal still gives it authority.
      const uint64_t walk_ms = threshold_ms - ttl_ms + 1'000;

      Gc gc(store, kGc, [] { return uint64_t{1'000'000}; }, [backend] { return backend->mono.load(); });

      const RootNamespace ns{"00/aa@cas@"};
      const ManifestRef r = ref("srv-a:1", 1, 0xAA);
      writeBlobBody(*backend, layout, DB::UInt128(7));
      writeManifestRaw(*backend, layout, ns, r, {blobEntryFor("a", DB::UInt128(7))});
      publishCommittedTransition(*backend, layout, ns, "tbl", std::nullopt, r);

      /// The rebuild needs a state to rebuild; one round before the slot exists leaves the rebuild as
      /// the call that first sees it.
      if (sighting_call == SightingCall::Rebuild)
          gc.runRegularRound();

      /// Claims once and never renews: a live-shaped body whose token never changes.
      CasRequests renewer_requests = openRequestsForTest(backend);
      MountLeaseRenewer srid2_renewer(renewer_requests, renewer_requests, layout, srid2, DB::UInt128(0x2222),
          /*writer_epoch=*/1,
          std::chrono::milliseconds(100), [] { return 1000u; }, [] { return 0u; }, {},
          std::chrono::milliseconds(0), [] { return 0u; });
      srid2_renewer.start();
      backend->watched_reads = 0;

      backend->read_cost_ms = walk_ms;
      if (sighting_call == SightingCall::Round)
      {
          const RoundReport first = gc.runRegularRound();
          ASSERT_EQ(first.fence_outs, 0u);
      }
      else
      {
          const RebuildReport rebuilt = gc.rebuildBaseline(/*force*/ true);
          ASSERT_TRUE(rebuilt.performed) << rebuilt.refusal;
      }
      ASSERT_EQ(backend->watched_reads.load(), 1u);
      const uint64_t sighted_at = backend->last_read_done_at.load();
      ASSERT_EQ(sighted_at, walk_ms);
      backend->read_cost_ms = 0;

      /// One threshold after the sighting call started, less than one after its read. The store's own
      /// mount renews before each round so only srid2 can be fenced.
      store->renewWatermarkOnce();
      backend->mono = threshold_ms;
      const RoundReport early = gc.runRegularRound();
      EXPECT_EQ(early.fence_outs, 0u) << "fenced a token watched for less than the threshold";

      store->renewWatermarkOnce();
      backend->mono = sighted_at + threshold_ms;
      const RoundReport due = gc.runRegularRound();
      EXPECT_EQ(due.fence_outs, 1u);
      EXPECT_TRUE(decodeMountLease(readObj(*backend, backend->watched_key)->bytes).gc_fenced);
  }
  ```
  After `TEST(CASGcFenceOut, ThresholdUnchangedByUnsafeKnob)`:
  ```cpp
  TEST(CASGcFenceOut, RoundCountsASightingFromAfterItsRead)
  {
      runSlowWalkSightingScenario(SightingCall::Round);
  }

  TEST(CASGcFenceOut, RebuildCountsASightingFromAfterItsRead)
  {
      runSlowWalkSightingScenario(SightingCall::Rebuild);
  }
  ```
  With the defaults (`mount_lease_ttl_ms` 30000, `mount_renew_period` 10000) the threshold is 41500
  and the walk is 12500, the spec's 11.5 s bound plus one second.

- [ ] **Step 3: run, expect red.** Build (log `build/ninja_pr01_t3.log`), then
  `build/src/unit_tests_dbms --gtest_filter='CASHeartbeatFloor.FirstSightingCountsFromAfterItsRead:CASHeartbeatFloor.RedecisionAfterARefusedFenceCountsFromItsRead:CASGcFenceOut.RoundCountsASightingFromAfterItsRead:CASGcFenceOut.RebuildCountsASightingFromAfterItsRead' > build/test_pr01_t3_red.log 2>&1`.
  Expected, all four failing:
  - `FirstSightingCountsFromAfterItsRead`: `first_seen_mono_ms` `Which is: 0` vs `kWalkMs`
    `Which is: 4000`; `early.fenced_now` `Which is: 1` with "fenced a token watched for less than
    the threshold"; `early.live` `Which is: 0`; the last call `Which is: 0` (already fenced).
  - `RedecisionAfterARefusedFenceCountsFromItsRead`: `first_seen_mono_ms` `Which is: 10000` vs
    `reread_at`; the third call `Which is: 1` with "fenced the renewed token ...".
  - `RoundCountsASightingFromAfterItsRead` and `RebuildCountsASightingFromAfterItsRead`:
    `early.fence_outs` `Which is: 1` with "fenced a token watched ..."; `due.fence_outs`
    `Which is: 0`.

  If a Gc-level test fails on an `ASSERT` before `early` (read count, `sighted_at`, `performed`),
  the fixture is wrong, not the fix: stop and report the log.

- [ ] **Step 4: implement.** In `CasServerRoot.cpp`, `computeHeartbeatFloor`, put a comment on the
  entry sample:
  ```cpp
      /// Taken before any read: it can confirm that a token held, never date a sighting.
      const uint64_t round_start_ms = mono_ms_fn();
  ```
  and in the decision lambda replace
  ```cpp
                      if (it == obs.end() || it->second.etag != observed->etag)
                          obs.insert_or_assign(srid, MountIncarnationObservation{observed->etag, round_start_ms});
  ```
  with
  ```cpp
                      /// A sample from before this read would count the walk to the slot as time watched.
                      if (it == obs.end() || it->second.etag != observed->etag)
                          obs.insert_or_assign(srid, MountIncarnationObservation{observed->etag, mono_ms_fn()});
  ```
  In `CasServerRoot.h`, after the replaced `mono_ms_fn` sentence of Task 1 Step 2, the line
  `/// any other node's clock. \`obs\` is owned by the caller ...` becomes
  ```
  /// any other node's clock. It is sampled once at entry for the stability test and after the read for
  /// each sighting. `obs` is owned by the caller and threaded across consecutive calls (one GC
  ```
  (the rest of that paragraph is unchanged; re-wrap only these two lines).

- [ ] **Step 5: run, expect green.** Rebuild (log `build/ninja_pr01_t3b.log`); run the four tests
  (log `build/test_pr01_t3.log`) and then the Task 1 Step 7 filter (log `build/test_pr01_t3_all.log`).
  Expected `TEST_EXIT=0` for both; `ConfirmingCallDecidesOnItsEntrySample` and
  `FailedSlotReadKeepsTheWatch` stay green.

- [ ] **Step 6: commit.**
  ```bash
  git add -- src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h \
             src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.cpp \
             src/Disks/tests/gtest_cas_mount.cpp \
             src/Disks/tests/gtest_cas_gc_ack_floor.cpp
  git commit -m "Date a GC mount-token sighting after the read that returned it" \
             -m "Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
  ```

## Task 4: Model check and gates

- [ ] **Step 1: model, no change.** From `/home/mfilimonov/workspace/ClickHouse/master`:
  ```bash
  docs/superpowers/models/run_mountobserve.sh > tmp/lease-series/pr01-mountobserve.log 2>&1; echo EXIT=$?
  ```
  Expected `EXIT=0`, every row `PASS`, and `sab_sighting_before_read`,
  `sab_redecision_stale_sample` and `sab_wrapping_stability` each `violation:NoFenceInsideAuthority`.
  These are the code's three rules: the sighting after the read (Task 3), the re-decision (Task 3),
  the non-wrapping comparison (Task 2). Do not edit the model or `CaMountObserveCore_RESULTS.md`.

- [ ] **Step 2: release gate.**
  `build/src/unit_tests_dbms --gtest_filter='CAS*' > build/test_pr01_gate.log 2>&1; echo TEST_EXIT=$?`
  Expected `TEST_EXIT=0`.

- [ ] **Step 3: ASan gate.**
  ```bash
  ninja -C build_asan unit_tests_dbms > build_asan/ninja_pr01.log 2>&1; echo NINJA_EXIT=$?
  build_asan/src/unit_tests_dbms --gtest_filter='CAS*' > build_asan/test_pr01_gate.log 2>&1; echo TEST_EXIT=$?
  ```
  Expected `NINJA_EXIT=0`, `TEST_EXIT=0`.

- [ ] **Step 4: style.** `git diff 5b7c9f503df -- src | grep -n '^+.*) {$\|^+.*else {$'` prints
  nothing (Allman).

## Task 5: Pull request text

- [ ] **Step 1.** Title: `Date a GC mount-token sighting after the read that returned it`. Body
  (template order, base `antalya-26.6`, do not push without the owner's go):

  ```
  The GC credited the first sighting of a mount token with a clock sample taken before the `LIST` of
  `gc/server-roots/` and the slot read. A slow walk then counted as time the token was watched, and a
  second round could write `gc_fenced` over a holder whose last renewal still gave it authority. Both
  callers of `computeHeartbeatFloor` (`Gc::runRegularRound`, `Gc::rebuildBaseline`) now pass the
  observer clock; the sighting is sampled after the read, including the re-decision after a refused
  fence-out. The stability test keeps the entry sample, and a sample older than the sighting never
  counts as held.

  Mixed versions: GC-only, in-memory state; no format change. Roll this out on GC nodes before any
  holder change that keeps authority across failed renewals.

  Related: https://github.com/Altinity/ClickHouse/issues/2431

  ### Changelog category (leave one):
  - Bug Fix (user-visible misbehavior in an official stable release)

  ### Changelog entry:
  CAS GC no longer fences a mount whose token it watched for less than the threshold after a slow walk to the slot.

  ### Documentation entry for user-facing changes
  - [ ] Documentation is written (mandatory for new features)
  ```
  `Related`, not `Closes`: #2431 is closed by PR 8 of the series; this PR closes backlog task CAS-342.
