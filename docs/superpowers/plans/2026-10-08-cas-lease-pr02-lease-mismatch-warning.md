# CAS lease PR 2: `WARNING` when a holder advertises a different lease

**Goal.** When the GC reads a mount body in `computeHeartbeatFloor` and the holder's
`expires_at_ms − started_at_ms` differs from this server's `cas_mount_lease_ttl_ms`, write one
`WARNING` line per `server_root_id`. The line blocks nothing and protects nothing. It is not written
when either stamp is zero (a missing field decodes as 0) or when the interval is not positive (a
farewell stamps `expires_at_ms == started_at_ms`).

**Architecture.** `computeHeartbeatFloor` gains two trailing parameters: the observer's own lease and
a caller-owned `std::set<String>` of srids already warned about. A file-local helper in
`CasServerRoot.cpp` checks every decoded body before classification and logs on the existing
`CasHeartbeatFloor` logger. `Gc` owns the set next to `mount_obs` and passes the `ttl_ms` it already
computes at both call sites (regular round and rebuild).

**Tech stack.** C++23, gtest (`unit_tests_dbms`), `InMemoryBackend`, Poco logger capture.

**Spec.** `docs/superpowers/specs/2026-10-07-cas-mount-lease-series-design.md`, revision 7: section 1
"Настройки" (last paragraph), section 3, section 7 rows "2".

**Depends on PR 1** (`docs/superpowers/plans/2026-10-08-cas-lease-pr01-gc-sighting-after-read.md`).
Start this branch only after PR 1 is merged into `altinity/antalya-26.6`. Every snippet below is
written against PR 1's result:
- `computeHeartbeatFloor(CasOperation & op, const Layout & l, uint64_t now_ms,
  const std::function<uint64_t()> & mono_ms_fn, uint64_t stable_threshold_ms, MountObservationMap & obs)`;
- both `Gc` calls pass the member `mono_ms_fn` (not `mono_ms_fn()`);
- tests pass the observer clock as `frozenClock(ms)`, a helper PR 1 adds to `gtest_cas_mount.cpp`.

This PR appends `observer_lease_ms` and `lease_mismatch_warned` after `obs`.

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

Files outside `CA/`: none in production. Test-only: `src/Disks/tests/cas_test_helpers.h`,
`src/Disks/tests/gtest_cas_mount.cpp`, `src/Disks/tests/gtest_cas_gc_ack_floor.cpp`.

## Review Focus

- The check runs on every decoded body, including fenced and farewell bodies, before classification.
  Only the two stamp rules silence it. Confirm it cannot change a counter, an `obs` entry or a write.
- `readModifyWrite` may call the decide lambda more than once per slot (conflict → re-read). The set
  must make that one line, not two.
- Both `Gc` call sites pass `ttl_ms` (the lease), not `stable_threshold_ms` (the observation window).
  The wiring test covers the regular round only; the rebuild call passes the same local.
- The text names `cas_mount_lease_ttl_ms`, the setting that exists on this base. PR 9b renames it.

`CA/` below = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`.

---

## Task 0: Branch

- [ ] **Step 1.** In `/home/mfilimonov/workspace/ClickHouse/cas-95-1`: `git fetch altinity antalya-26.6`;
  confirm `git grep -n frozenClock altinity/antalya-26.6 -- src/Disks/tests/gtest_cas_mount.cpp` prints
  PR 1's helper; then `git switch -c cas/lease-pr02-lease-mismatch-warning altinity/antalya-26.6`.

---

## Task 1: Plumb the observer lease and the warned set (no behaviour change)

**Files:** `CA/Pool/CasServerRoot.h`, `CA/Pool/CasServerRoot.cpp`, `CA/Gc/CasGc.h`, `CA/Gc/CasGc.cpp`,
`src/Disks/tests/gtest_cas_mount.cpp`.

**Produces:** `computeHeartbeatFloor(..., MountObservationMap & obs, uint64_t observer_lease_ms,
std::set<String> & lease_mismatch_warned)`; member `Gc::lease_mismatch_warned`; test constant
`kObserverLeaseMs`. The definition leaves both new parameters unnamed, so `-Wunused-parameter` stays
quiet and Task 2's tests compile against it and fail on their assertions, not on the build.

- [ ] **Step 1. Header.** In `CA/Pool/CasServerRoot.h` add `#include <set>` after `#include <optional>`,
  and change the declaration that ends with `MountObservationMap & obs);` to:
  ```cpp
  HeartbeatFloor computeHeartbeatFloor(CasOperation & op, const Layout & l, uint64_t now_ms,
                                       const std::function<uint64_t()> & mono_ms_fn,
                                       uint64_t stable_threshold_ms, MountObservationMap & obs,
                                       uint64_t observer_lease_ms, std::set<String> & lease_mismatch_warned);
  ```

- [ ] **Step 2. Definition.** In `CA/Pool/CasServerRoot.cpp`, change the definition's parameter list the
  same way, with the new parameters unnamed:
  ```cpp
  HeartbeatFloor computeHeartbeatFloor(CasOperation & op, const Layout & l, uint64_t now_ms,
                                       const std::function<uint64_t()> & mono_ms_fn,
                                       uint64_t stable_threshold_ms, MountObservationMap & obs,
                                       uint64_t /*observer_lease_ms*/, std::set<String> & /*lease_mismatch_warned*/)
  ```

- [ ] **Step 3. Enumerate call sites.**
  ```bash
  grep -rn 'computeHeartbeatFloor(' src/ programs/ utils/ | grep -v '///'
  ```
  It prints the declaration, the definition, the two calls in `CA/Gc/CasGc.cpp` (`Gc::runRegularRound`,
  `Gc::rebuildBaseline`) and the test calls in `src/Disks/tests/gtest_cas_mount.cpp` (the base ones
  plus those PR 1 added). Every printed call gets the edit below; the build in step 7 is the gate.

- [ ] **Step 4. `Gc` member.** In `CA/Gc/CasGc.h`, directly after `MountObservationMap mount_obs;`:
  ```cpp
      /// Srids `computeHeartbeatFloor` already warned about for a lease length different from ours.
      /// In-memory: a new leader names each mismatched holder once more.
      std::set<String> lease_mismatch_warned;
  ```
  (`<set>` is already included there.)

- [ ] **Step 5. `Gc` call sites.** In `CA/Gc/CasGc.cpp`, regular round (inside the
  `GcPhaseTimer t(phase_sink, "heartbeat_floor")` block):
  ```cpp
          const HeartbeatFloor floor = computeHeartbeatFloor(op, layout, now_ms_fn(), mono_ms_fn,
                                                             stable_threshold_ms, mount_obs, ttl_ms,
                                                             lease_mismatch_warned);
  ```
  Rebuild (the call after "Also fence out any dead mounts as part of the disaster-recovery pass"):
  ```cpp
      computeHeartbeatFloor(op, layout, now_ms_fn(), mono_ms_fn, stable_threshold_ms, mount_obs, ttl_ms,
                            lease_mismatch_warned);
  ```
  Both functions already define `const uint64_t ttl_ms` from `store->poolConfig().mount_lease_ttl_ms`.

- [ ] **Step 6. Test call sites.** In `src/Disks/tests/gtest_cas_mount.cpp`:
  - add `#include <set>` after `#include <map>`;
  - after `constexpr uint64_t kStableThresholdMs = 10'000;` add
    ```cpp
    /// The observing GC's own `cas_mount_lease_ttl_ms`.
    constexpr uint64_t kObserverLeaseMs = 30'000;
    ```
  - in every test that declares `MountObservationMap obs;`, add `std::set<String> warned;` on the
    next line;
  - in every call printed by step 3, replace the final `obs)` with `obs, kObserverLeaseMs, warned)`.

- [ ] **Step 7. Build and run (behaviour preservation: green before and after).**
  ```bash
  ninja -C build unit_tests_dbms > build/build_pr02_t1.log 2>&1; echo NINJA_EXIT=$?
  build/src/unit_tests_dbms --gtest_filter='CASHeartbeatFloor.*:CASGCAckFloor.*' > build/test_pr02_t1.log 2>&1; echo EXIT=$?
  ```
  Expect `NINJA_EXIT=0`, `EXIT=0`, and the same test names passing as on the base.

- [ ] **Step 8. Commit.**
  ```bash
  git add -- src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h \
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.cpp \
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h \
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.cpp \
    src/Disks/tests/gtest_cas_mount.cpp
  git commit -m "Pass the observer lease into computeHeartbeatFloor" \
    -m "Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
  ```

---

## Task 2: Warn once per srid about a holder with a different lease

**Files:** `src/Disks/tests/cas_test_helpers.h`, `src/Disks/tests/gtest_cas_mount.cpp`,
`src/Disks/tests/gtest_cas_gc_ack_floor.cpp`, `CA/Pool/CasServerRoot.h`, `CA/Pool/CasServerRoot.cpp`.

**Consumes:** Task 1's parameters. **Produces:** the `WARNING` line, the dedup and its pruning.

- [ ] **Step 1. Log capture helper.** In `src/Disks/tests/cas_test_helpers.h` add includes
  `<Common/Logger.h>`, `<Poco/AutoPtr.h>`, `<Poco/StreamChannel.h>`, `<sstream>`, `<string_view>` (skip
  any already present), and inside `namespace DB::Cas::tests`, after the two `openRequestsForTest`
  overloads:
  ```cpp
  /// Routes one logger into memory at `level` for the scope, then restores its channel and level.
  class ScopedLogCapture
  {
  public:
      ScopedLogCapture(const String & logger_name, const String & level)
          : logger(getLogger(logger_name))
          , channel(new Poco::StreamChannel(stream))
          , old_channel(logger->getChannel(), /*shared=*/true)
          , old_level(logger->getLevel())
      {
          logger->setChannel(channel.get());
          logger->setLevel(level);
      }

      ~ScopedLogCapture()
      {
          logger->setChannel(old_channel);
          logger->setLevel(old_level);
      }

      ScopedLogCapture(const ScopedLogCapture &) = delete;
      ScopedLogCapture & operator=(const ScopedLogCapture &) = delete;

      String captured() const { return stream.str(); }

      size_t count(std::string_view needle) const
      {
          const String haystack = captured();
          size_t n = 0;
          for (size_t pos = 0; (pos = haystack.find(needle, pos)) != String::npos; pos += needle.size())
              ++n;
          return n;
      }

  private:
      LoggerPtr logger;
      std::ostringstream stream; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
      Poco::AutoPtr<Poco::StreamChannel> channel;
      /// `shared=true`: the parked channel is not ours, so take a reference instead of stealing one.
      Poco::AutoPtr<Poco::Channel> old_channel;
      int old_level;
  };
  ```

- [ ] **Step 2. Unit tests.** In `src/Disks/tests/gtest_cas_mount.cpp`:
  - give `seedMount` a trailing parameter `uint64_t started_at_ms = kNowMs` and set
    `m.started_at_ms = started_at_ms;` instead of `m.started_at_ms = kNowMs;`;
  - in the same anonymous namespace, after `kObserverLeaseMs`:
    ```cpp
    constexpr std::string_view kLeaseMismatchMarker = "advertises a lease";
    ```
  - after `TEST(CASHeartbeatFloor, EmptyPrefixYieldsNoLiveMounts)` add:
  ```cpp
  TEST(CASHeartbeatFloor, HolderWithADifferentLeaseIsWarnedAboutWithoutChangingTheClassification)
  {
      auto b = std::make_shared<InMemoryBackend>();
      Layout l("p");
      Ops ops(b);
      seedMount(ops.op, l, "s1", /*expires*/ kNowMs + 60'000, /*fenced*/ false, /*min_active_build_sequence*/ 0);
      const String before = ops.op.read(l.mountKey("s1"), Retry::standard())->bytes;

      DB::Cas::tests::ScopedLogCapture capture("CasHeartbeatFloor", "warning");
      MountObservationMap obs;
      std::set<String> warned;
      const HeartbeatFloor floor = computeHeartbeatFloor(ops.op, l, kNowMs, frozenClock(0), kStableThresholdMs, obs,
                                                         kObserverLeaseMs, warned);

      EXPECT_EQ(capture.count(
          "This server is configured with cas_mount_lease_ttl_ms=30000 but the holder of slot s1 "
          "(hostname=h-s1, pid=100) advertises a lease of 60000 ms. Observers with a shorter lease can "
          "fence a live holder; set one value on every server of the pool with the pool stopped."), 1u)
          << capture.captured();
      EXPECT_EQ(floor.live, 1u);
      EXPECT_EQ(floor.fenced_now, 0u);
      EXPECT_EQ(ops.op.read(l.mountKey("s1"), Retry::standard())->bytes, before);
      EXPECT_EQ(warned, std::set<String>{"s1"});
  }

  TEST(CASHeartbeatFloor, HolderWithTheSameLeaseIsNotWarnedAbout)
  {
      auto b = std::make_shared<InMemoryBackend>();
      Layout l("p");
      Ops ops(b);
      seedMount(ops.op, l, "s1", /*expires*/ kNowMs + kObserverLeaseMs, /*fenced*/ false, /*min_active_build_sequence*/ 0);

      DB::Cas::tests::ScopedLogCapture capture("CasHeartbeatFloor", "warning");
      MountObservationMap obs;
      std::set<String> warned;
      computeHeartbeatFloor(ops.op, l, kNowMs, frozenClock(0), kStableThresholdMs, obs, kObserverLeaseMs, warned);

      EXPECT_EQ(capture.count(kLeaseMismatchMarker), 0u) << capture.captured();
      EXPECT_TRUE(warned.empty());
  }

  TEST(CASHeartbeatFloor, FarewellBodyIsNotWarnedAbout)
  {
      auto b = std::make_shared<InMemoryBackend>();
      Layout l("p");
      Ops ops(b);
      /// What `MountLeaseRenewer::terminate` writes: both stamps equal, farewell sentinel.
      seedMount(ops.op, l, "s1", /*expires*/ kNowMs, /*fenced*/ false,
                /*min_active_build_sequence*/ std::numeric_limits<uint64_t>::max());

      DB::Cas::tests::ScopedLogCapture capture("CasHeartbeatFloor", "warning");
      MountObservationMap obs;
      std::set<String> warned;
      const HeartbeatFloor floor = computeHeartbeatFloor(ops.op, l, kNowMs, frozenClock(0), kStableThresholdMs, obs,
                                                         kObserverLeaseMs, warned);

      EXPECT_EQ(floor.terminated, 1u);
      EXPECT_EQ(capture.count(kLeaseMismatchMarker), 0u) << capture.captured();
  }

  TEST(CASHeartbeatFloor, MissingStartStampIsNotWarnedAbout)
  {
      auto b = std::make_shared<InMemoryBackend>();
      Layout l("p");
      Ops ops(b);
      /// A body without `started_at_ms` decodes it as 0; the interval to `expires_at_ms` means nothing.
      seedMount(ops.op, l, "s1", /*expires*/ 60'000, /*fenced*/ false, /*min_active_build_sequence*/ 0,
                /*seq*/ 1, /*started_at_ms*/ 0);

      DB::Cas::tests::ScopedLogCapture capture("CasHeartbeatFloor", "warning");
      MountObservationMap obs;
      std::set<String> warned;
      computeHeartbeatFloor(ops.op, l, kNowMs, frozenClock(0), kStableThresholdMs, obs, kObserverLeaseMs, warned);

      EXPECT_EQ(capture.count(kLeaseMismatchMarker), 0u) << capture.captured();
  }

  TEST(CASHeartbeatFloor, LeaseMismatchIsWarnedOncePerServerRootId)
  {
      auto b = std::make_shared<InMemoryBackend>();
      Layout l("p");
      Ops ops(b);
      seedMount(ops.op, l, "s1", /*expires*/ kNowMs + 60'000, /*fenced*/ false, /*min_active_build_sequence*/ 0);
      seedMount(ops.op, l, "s2", /*expires*/ kNowMs + 60'000, /*fenced*/ false, /*min_active_build_sequence*/ 0);

      DB::Cas::tests::ScopedLogCapture capture("CasHeartbeatFloor", "warning");
      MountObservationMap obs;
      std::set<String> warned;
      computeHeartbeatFloor(ops.op, l, kNowMs, frozenClock(0), kStableThresholdMs, obs, kObserverLeaseMs, warned);
      /// A renewal gives s1 a new incarnation; the next round reads both bodies again.
      renewMount(ops.op, l, "s1");
      computeHeartbeatFloor(ops.op, l, kNowMs, frozenClock(1), kStableThresholdMs, obs, kObserverLeaseMs, warned);

      EXPECT_EQ(capture.count(kLeaseMismatchMarker), 2u) << capture.captured();
      EXPECT_EQ(capture.count("holder of slot s1 ("), 1u) << capture.captured();
      EXPECT_EQ(capture.count("holder of slot s2 ("), 1u) << capture.captured();
  }

  TEST(CASHeartbeatFloor, UnseenSridPrunedFromLeaseMismatchWarned)
  {
      auto b = std::make_shared<InMemoryBackend>();
      Layout l("p");
      Ops ops(b);
      seedMount(ops.op, l, "s1", /*expires*/ kNowMs + 60'000, /*fenced*/ false, /*min_active_build_sequence*/ 0);
      seedMount(ops.op, l, "s2", /*expires*/ kNowMs + 60'000, /*fenced*/ false, /*min_active_build_sequence*/ 0);

      MountObservationMap obs;
      std::set<String> warned;
      computeHeartbeatFloor(ops.op, l, kNowMs, frozenClock(0), kStableThresholdMs, obs, kObserverLeaseMs, warned);
      ASSERT_EQ(warned, (std::set<String>{"s1", "s2"}));

      ASSERT_EQ(ops.op.removeCurrent(l.mountKey("s2"), Retry::standard()), Removal::Removed);
      computeHeartbeatFloor(ops.op, l, kNowMs, frozenClock(1), kStableThresholdMs, obs, kObserverLeaseMs, warned);

      EXPECT_EQ(warned, std::set<String>{"s1"});
  }
  ```

- [ ] **Step 3. `Gc` wiring test.** In `src/Disks/tests/gtest_cas_gc_ack_floor.cpp`, at the end of the
  file:
  ```cpp
  /// The round passes the pool's own lease: the store's slot, renewed with that lease, is not named;
  /// a holder renewing with 100 ms is named once across two rounds.
  TEST(CASGCLeaseMismatch, RoundWarnsOnceAboutAHolderWithADifferentLease)
  {
      auto backend = std::make_shared<InMemoryBackend>();
      auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
      const Layout & layout = store->layout();

      const String holder_srid = "short-lease-server";
      CasRequests renewer_requests = openRequestsForTest(backend);
      MountLeaseRenewer holder(renewer_requests, renewer_requests, layout, holder_srid, DB::UInt128(0x2222),
          /*writer_epoch=*/1,
          std::chrono::milliseconds(100), [] { return 1000u; }, [] { return 0u; }, {},
          std::chrono::milliseconds(0), [] { return 0u; });
      holder.start();

      const RootNamespace ns{"00/aa@cas@"};
      const ManifestRef r = ref("srv-a:1", 1, 0xAA);
      const UInt128 blob = DB::UInt128(7);
      writeBlobBody(*backend, layout, blob);
      writeManifestRaw(*backend, layout, ns, r, {blobEntryFor("a", blob)});
      publishCommittedTransition(*backend, layout, ns, "tbl", std::nullopt, r);

      uint64_t gc_now = 1'000'000;
      uint64_t gc_mono = 0;
      Gc gc(store, kGc, [&] { return gc_now; }, [&] { return gc_mono; });

      ScopedLogCapture capture("CasHeartbeatFloor", "warning");
      gc.runRegularRound();
      gc_mono = 1;
      gc.runRegularRound();

      EXPECT_EQ(capture.count("advertises a lease"), 1u) << capture.captured();
      EXPECT_EQ(capture.count("This server is configured with cas_mount_lease_ttl_ms=30000 but the holder of slot "
                              "short-lease-server (hostname="), 1u) << capture.captured();
      EXPECT_EQ(capture.count("advertises a lease of 100 ms."), 1u) << capture.captured();
  }
  ```
  (`kGc`, `ref`, `writeBlobBody`, `writeManifestRaw`, `publishCommittedTransition`, `blobEntryFor` and
  `openRequestsForTest` are already in scope in this file; the seeding mirrors
  `runExpiredMountFenceOutScenario`.)

- [ ] **Step 4. Run: expect red.**
  ```bash
  ninja -C build unit_tests_dbms > build/build_pr02_t2red.log 2>&1; echo NINJA_EXIT=$?
  build/src/unit_tests_dbms --gtest_filter='CASHeartbeatFloor.*:CASGCLeaseMismatch.*' > build/test_pr02_t2red.log 2>&1; echo EXIT=$?
  ```
  Expected failures, each on its assertion (no warning exists yet):
  - `HolderWithADifferentLeaseIsWarnedAboutWithoutChangingTheClassification`: `capture.count(...)`
    `Which is: 0` vs `1u`; `warned` is empty vs `{ "s1" }`.
  - `LeaseMismatchIsWarnedOncePerServerRootId`: `Which is: 0` vs `2u`.
  - `UnseenSridPrunedFromLeaseMismatchWarned`: `ASSERT_EQ(warned, ...)` — `{}` vs `{ "s1", "s2" }`.
  - `CASGCLeaseMismatch.RoundWarnsOnceAboutAHolderWithADifferentLease`: `Which is: 0` vs `1u`.

  Green on this state, and they stay green: `HolderWithTheSameLeaseIsNotWarnedAbout`,
  `FarewellBodyIsNotWarnedAbout`, `MissingStartStampIsNotWarnedAbout` — no warning exists to suppress.
  Step 8 shows each of them goes red when its rule is removed. Paste the failure lines into the task
  report.

- [ ] **Step 5. Implementation.** In `CA/Pool/CasServerRoot.cpp`, directly before the definition of
  `computeHeartbeatFloor`:
  ```cpp
  namespace
  {
  /// Observers fence after their own lease, so a holder renewing on a longer one can be fenced while it
  /// may still write. The protocol cannot detect that; name the holder once so the operator can.
  void warnOnceIfHolderLeaseDiffers(
      const String & srid, const MountLease & holder, uint64_t observer_lease_ms, std::set<String> & warned)
  {
      /// A missing stamp decodes as 0, and a farewell stamps `expires_at_ms == started_at_ms`.
      if (holder.started_at_ms == 0 || holder.expires_at_ms <= holder.started_at_ms)
          return;
      const uint64_t holder_lease_ms = holder.expires_at_ms - holder.started_at_ms;
      if (holder_lease_ms == observer_lease_ms || !warned.insert(srid).second)
          return;
      LOG_WARNING(getLogger("CasHeartbeatFloor"),
          "This server is configured with cas_mount_lease_ttl_ms={} but the holder of slot {} (hostname={}, "
          "pid={}) advertises a lease of {} ms. Observers with a shorter lease can fence a live holder; set one "
          "value on every server of the pool with the pool stopped.",
          observer_lease_ms, srid, holder.hostname, holder.pid, holder_lease_ms);
  }
  }
  ```
  Name the two parameters of the definition (`uint64_t observer_lease_ms`,
  `std::set<String> & lease_mismatch_warned`). In the decide lambda, right after
  `const MountLease m = decodeMountLease(observed->bytes);`, add:
  ```cpp
                  warnOnceIfHolderLeaseDiffers(srid, m, observer_lease_ms, lease_mismatch_warned);
  ```
  Replace the prune block at the end of the function:
  ```cpp
      /// Prune every `obs` entry for a srid this pass's walk never saw at all.
      for (auto it = obs.begin(); it != obs.end(); )
          it = seen_srids.contains(it->first) ? std::next(it) : obs.erase(it);
  ```
  with:
  ```cpp
      /// Prune every `obs` and `lease_mismatch_warned` entry for a srid this pass's walk never saw at all.
      for (auto it = obs.begin(); it != obs.end(); )
          it = seen_srids.contains(it->first) ? std::next(it) : obs.erase(it);
      std::erase_if(lease_mismatch_warned, [&](const String & s) { return !seen_srids.contains(s); });
  ```

- [ ] **Step 6. Header contract.** In `CA/Pool/CasServerRoot.h`, in the comment block above
  `struct HeartbeatFloor`, after the paragraph that starts with "`now_ms` is WALL clock", add:
  ```cpp
  /// `observer_lease_ms` is this server's own `cas_mount_lease_ttl_ms`. A body whose stamps are both
  /// non-zero and whose `expires_at_ms - started_at_ms` is positive and differs from it is named in one
  /// `WARNING` per srid; `lease_mismatch_warned` holds the srids already named and is pruned like `obs`.
  /// The check never changes a classification or a write.
  ```

- [ ] **Step 7. Run: expect green.** Same two commands as step 4 (logs `build_pr02_t2.log`,
  `test_pr02_t2.log`). Expect `NINJA_EXIT=0`, `EXIT=0`, every `CASHeartbeatFloor` test and the wiring
  test passing.

- [ ] **Step 8. Mutation checks (do not commit any of them).** Apply one at a time, rebuild, run the
  named test, expect red, then revert that one line by hand:
  | Mutation in `warnOnceIfHolderLeaseDiffers` / `Gc` | Test that must go red |
  |---|---|
  | drop `holder.started_at_ms == 0 \|\|` | `MissingStartStampIsNotWarnedAbout` |
  | `<=` → `<` in the interval rule | `FarewellBodyIsNotWarnedAbout` |
  | drop `holder_lease_ms == observer_lease_ms \|\|` | `HolderWithTheSameLeaseIsNotWarnedAbout` |
  | replace `!warned.insert(srid).second` with `false` | `LeaseMismatchIsWarnedOncePerServerRootId` |
  | in `CasGc.cpp` regular round pass `stable_threshold_ms` instead of `ttl_ms` | `RoundWarnsOnceAboutAHolderWithADifferentLease` (the store's own slot is named too) |
  Record each red line in the task report. Rebuild and re-run step 7 green afterwards.

- [ ] **Step 9. Commit.**
  ```bash
  git add -- src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h \
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.cpp \
    src/Disks/tests/cas_test_helpers.h \
    src/Disks/tests/gtest_cas_mount.cpp \
    src/Disks/tests/gtest_cas_gc_ack_floor.cpp
  git commit -m "Warn once per slot when a holder advertises a different lease" \
    -m "Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
  ```

---

## Task 3: Gates and PR text

- [ ] **Step 1. Release gate.**
  ```bash
  ninja -C build unit_tests_dbms > build/build_pr02_gate.log 2>&1; echo NINJA_EXIT=$?
  build/src/unit_tests_dbms --gtest_filter='CAS*' > build/test_pr02_gate.log 2>&1; echo EXIT=$?
  ```
- [ ] **Step 2. ASan gate.** The same two commands with `build_asan` (logs in `build_asan/`).
- [ ] **Step 3. Style.** `utils/check-style/check-style 2>&1 | grep -E 'CasServerRoot|CasGc|cas_test_helpers|gtest_cas_mount|gtest_cas_gc_ack_floor'`
  prints nothing.
- [ ] **Step 4. Diff hygiene.** `git diff --stat altinity/antalya-26.6...HEAD` lists exactly the seven
  files of Tasks 1–2; `git status --short` shows nothing of ours unstaged.
- [ ] **Step 5. PR description** (template `.github/PULL_REQUEST_TEMPLATE.md`, target `antalya-26.6`):
  ```
  The GC writes one `WARNING` per `server_root_id` when a mount holder's `expires_at_ms - started_at_ms`
  differs from this server's `cas_mount_lease_ttl_ms`: an observer with a shorter lease can fence a
  live holder, and the protocol cannot detect it. The line changes nothing else. Skipped for a missing
  stamp and for farewell bodies. The set of warned srids drops a srid whose slot leaves the LIST,
  the same way `obs` does.

  **Mixed versions**: no format change; only GC leaders on this build warn.

  **Testing**: `CASHeartbeatFloor.*` (text, three silent cases, once per srid, pruning);
  `CASGCLeaseMismatch.*` (the round passes the pool's lease).

  ### Changelog category (leave one):
  - Improvement

  ### Changelog entry
  CAS GC logs a `WARNING` once per mount slot whose holder uses a different `cas_mount_lease_ttl_ms`.
  ```
