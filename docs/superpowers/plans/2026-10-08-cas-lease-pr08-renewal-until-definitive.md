# CAS lease PR 8: background renewal until a definitive outcome — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:executing-plans (or
> superpowers:subagent-driven-development) to implement this plan task by task. Steps use checkbox
> (`- [ ]`) syntax for tracking.

**Goal.** A storage outage pauses CAS writes instead of fencing the mount. The background mount-lease
renewal retries one logical renewal (one key, body, expected ETag, `write_attempt_id` and start)
about once a second until the store answers about the slot. The lease expiring is a pause: writes are
refused, one `WARNING` is logged, `system.cas_mounts` shows `lifecycle_reason = 'lease_expired'`, and
the first confirmed renewal whose `start + lease` is still ahead resumes writes under the same
`writer_epoch`. Closes [#2431](https://github.com/Altinity/ClickHouse/issues/2431), CAS-298, CAS-151
(renewal pacing with jitter only), CAS-256 (the repro is the acceptance check).

**Architecture.** `Retry::untilDefinitive` is a policy with no window and no lease bound whose
reissues are spaced 0.8–1.2 s from the start of the previous attempt. The background renewal runs it
on `lease_requests`: today's `farewell_requests` (open fence, plain sleep), renamed. `MountLeaseRenewer::renewUntilDefinitive` keeps one tuple across engine calls:
`Refused` starts a new call with the same tuple, `Conflict{NotObserved}` repeats only the read. The
confirmed deadline is published in `DriverLease::finish` under `driver_mutex`, so a remount can no
longer arm a new epoch between a renewal's commit and the publication of its deadline.

**Tech stack.** C++23, gtest (`unit_tests_dbms`, filter `CAS*`), `InMemoryBackend` subclasses on an
injected boot clock, pytest via praktika (`test_cas_mount_renewal_retry`).

**Spec.** `/home/mfilimonov/workspace/ClickHouse/master/docs/superpowers/specs/2026-10-07-cas-mount-lease-series-design.md`,
revision 7: section 2 "Продление" (R1–R7), section 1 "Состояния" (`lease_expired`) and "Шкала
времени", section 5 row "политика продления с границей по lease для фонового продления", section 7
rows 8 (PR and tests tables), section 9 row `CaMountRenewRetryCore`. Model evidence:
`docs/superpowers/models/CaMountRenewRetryCore_RESULTS.md`, section "2026-10-08: `Refused` and
`Conflict{NotObserved}` continue the same tuple" (commit `eb2e40e5eab`): the `Refused` guard
(`definitely_refused && !state.any_ambiguous`) is load-bearing; a re-`PUT` after `NotObserved` breaks
no invariant, so "only the read repeats" saves requests.

**Code base.** `/home/mfilimonov/workspace/ClickHouse/cas-95-1`. `CA/` =
`src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`. Draft PR
[#2474](https://github.com/Altinity/ClickHouse/pull/2474) (branch
`fix/antalya-26.6/cas-mount-renewal-no-deadline`) is reference material: `Retry::spacedPause`,
`Retry::drawSpacing`, the spaced engine pauses and several tests are reused; its third plane and its
`MountRenewPolicy` in the environment are not.

**Depends on (merged into `antalya-26.6` first; plans in `docs/superpowers/plans/`).**

| PR | What this plan consumes |
|---|---|
| 1 (`2026-10-08-cas-lease-pr01-gc-sighting-after-read.md`) | nothing in code; deployment order only: GC nodes get PR 1 before holders get PR 8, or a slow GC round can fence a holder that now lives longer on failed renewals |
| 4 (`2026-10-08-cas-lease-pr04-renewal-progress.md`) | `RequestObserver{on_write_attempt(uint32_t), on_failure(verb, key, e)}`, `CasOperation::observeRequests`, `MountRenewOperationEnvironment::requests`, the runtime's `renewalEnvironment` lambdas, `renewRenewerOnce` with `result = …` inside a `SCOPE_EXIT` block, `consumeRenewResult` without the attempt counters |
| 6 (`2026-10-08-cas-lease-pr06-absence-per-verb.md`) | `ProvenAbsent` on a `GET` only for `NoSuchKey`. A nameless `404` is a transient fault: under `untilDefinitive` the read loop retries it, so it never ends a renewal as "vanished" |

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

**Files outside `CA/`.** `src/Storages/System/StorageSystemContentAddressedMounts.cpp`: column
descriptions of `lifecycle_reason` and `lifecycle_since` only (the operator surface names the new
value). No code outside `CA/` changes. Docs: `docs/en/antalya/cas/architecture/mounts-and-leases.md`,
`docs/en/antalya/cas/operations/troubleshooting.md`, `docs/en/antalya/cas/operations/monitoring.md`,
`docs/en/operations/system-tables/cas_mounts.md`. `S3ObjectStorage.cpp` has a comment naming "the
mount and farewell planes"; it stays (the farewell still exists, and the file is shared).

**Budget.** About +200 production lines (the spec estimates +110). Over budget: cut comments first.

## Review Focus

- **Which renewals lose their deadline.** Only the background worker (`WorkerCall` →
  `renewUntilDefinitive`, lease plane). Startup (`renewRenewerForStartupOnce`), direct test calls
  (`renewWatermarkOnce`) and the remount re-anchor (`renewForRemount`) keep `Retry::untilLeaseSafe`.
  Section "Старт" of the spec keeps the startup renewal bounded ("старт при лежащем хранилище не висит").
- **The `Refused` guard.** A new engine call after `Refused` is safe only because `writeLoop` returns
  `Refused` when no attempt of the call was unclear. `ARefusalAfterAnUnclearAttemptIsSettledInsideTheCall`
  fences it, and Task 4 shows it fail with the guard removed.
- **One tuple.** Body, `write_attempt_id`, expected ETag and `attempt_start_boot_ms` are computed once
  per `renewOn` and reused by every engine call; the confirmed deadline is that first start plus the
  lease (R5).
- **What ends the background renewal.** A commit; a conflict whose read observed the slot (today's
  `throwRenewConflict` verdicts); a deterministic local failure; `environment.live` refusing (stop,
  park, a pool that left `Live`, a lost fence). The engine's `GaveUp{Deadline}` is unreachable before
  the clock wraps.
- **Deadline publication.** `DriverLease::finish` publishes under `driver_mutex` before the driver
  state leaves the call; `consumeRenewResult` no longer writes the deadline.
- **Lease plane sleep.** Plain, as today: a pause is at most 1.2 s (R4), a stop is seen at the next
  admission, and the farewell keeps its pauses after a stop.

## Spec and code disagreements (flagged, not resolved)

1. **Lease-plane sleep (ruled).** R2 asks for an interruptible sleep, R4 says none is needed. Ruling:
   R4 wins; the plane keeps the plain sleep, so the farewell keeps its pauses after a stop. R2's
   wording is to be corrected in the spec.
2. **"No deadline at all" vs section "Старт".** The dispatch says the spec wants no deadline. Section
   5 removes the lease bound "для фонового продления", and section "Старт" keeps the startup renewal
   "с сегодняшним ограниченным окном". The plan removes it only for the background renewal.
3. **`lifecycle` vocabulary before PR 9a.** Section 1 shows `paused` / `lease_expired`; `paused` and
   the mapping of today's values land in PR 9a. PR 8 reports `lifecycle = 'not_live'`,
   `lifecycle_reason = 'lease_expired'` (today's vocabulary plus the new reason).
4. **`CASMountLeaseExpired`.** Section 5 lists it among counters that "остаются и переименовываются"
   in PR 16. It does not exist on `5b7c9f503df` (`src/Common/ProfileEvents.cpp`). PR 8 adds the
   `WARNING` only, no counter.
5. **R4 and the first-attempt fuse.** R4: "Между попытками равномерно случайные 800–1200 мс". The
   engine still reissues a first-attempt fuse timeout at once (`reissueAtOnce`), at most once per
   engine call. Kept.
6. **Credential pause vs the 1.2 s sleep bound.** R2 asks for a renewal period after a credential
   `Refused`; R4 says a sleep is never longer than 1.2 s. The plan follows R2; a park request during
   that pause is seen at most one period later.
7. **What ends the renewal.** R2: "`GaveUp` … возможен только по stop". Until PR 15 the worker's
   liveness (`CasMountRuntime::renewalLive`) also ends it on a park request, a pool that left `Live`,
   or a lost fence; the existing remount chain needs that. Kept.
8. **Decommission.** `Pool::openForDecommission` runs a background renewal of the victim's lease;
   it becomes until-definitive too. Section 6 says decommission rules do not change. Flagged.
9. **Classifier rows of R2 that rename outcomes.** "чистый `412`, а прочитаны точные байты текущей
   операции → `slot_taken`" keeps today's terminal verdict and label (`throwRenewConflict`,
   `same_epoch_state_uncertain`); the names arrive with PR 12.

---

## Task 0: Branch

- [ ] In `/home/mfilimonov/workspace/ClickHouse/cas-95-1`:
  `git fetch altinity antalya-26.6 && git switch -c fix/antalya-26.6/cas-mount-renewal-until-definitive altinity/antalya-26.6`.
- [ ] Prove PRs 1, 4 and 6 are in the base. Each command must print at least one line, except the
  third, which must print nothing:
  ```bash
  git grep -n 'observeRequests' -- src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h
  git grep -n 'heldFor' -- src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h
  git grep -n 'RESOURCE_NOT_FOUND' -- src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasObjectStorageBackend.cpp
  ```
  If any fails, stop: this PR opens after its prerequisites merge.

## Task 1: The `lease_requests` plane (rename only)

A pure rename of `farewell_requests` to `lease_requests`: no sleep change and no behaviour change. The
plane keeps `CasRequests`'s plain sleep, so a pause is at most one spacing draw (1.2 s) and the
farewell keeps its pauses after a stop.

**Files:** `CA/Pool/CasPool.h`, `CA/Pool/CasPool.cpp`, `CA/Pool/CasMountRuntime.h`,
`CA/Pool/CasMountRuntime.cpp`, `CA/Tools/CasDecommission.cpp`.
**Produces:** `Pool::leaseRequests`, `CasMountRuntime(…, CasRequests & lease_requests_, …)`, member
`CasMountRuntime::lease_requests`.

- [ ] **Step 1. Rename.** Sweep derived with
  `git grep -n 'farewell_requests\|farewellRequests' -- src`; every hit is listed below.
  - `CasPool.h`: member `mutable CasRequests farewell_requests;` → `mutable CasRequests lease_requests;`.
    Accessor and its comment become:
    ```cpp
    /// The lease plane, on an open fence: the renewer's claim and adoption, the remount re-anchor and
    /// the farewell. None of them may be refused for want of the lease they establish or hand back.
    CasRequests & leaseRequests() { return lease_requests; }
    ```
    In the `PoolConfig::retry_sleep_fn` comment, replace "`farewell_requests`" with "`lease_requests`"
    and "the farewell plane" with "the lease plane".
  - `CasPool.cpp`, `Pool::Pool`: `, farewell_requests(` → `, lease_requests(` (arguments unchanged);
    `mount_runtime(pool_backend, mount_requests, farewell_requests, …` → `lease_requests`; in its comment
    "the mount and farewell planes" → "the mount and lease planes". `setCasRetrySleepForTest`:
    `farewell_requests.setSleepFnForTest(sleep_fn);` → `lease_requests.setSleepFnForTest(sleep_fn);`.
    `setCasRequestNowFnForTest`: `farewell_requests.setNowFnForTest(now_fn);` →
    `lease_requests.setNowFnForTest(now_fn);`.
  - `CasMountRuntime.h`: constructor parameter `CasRequests & farewell_requests_` →
    `CasRequests & lease_requests_`, its comment becomes `/// The two planes the \`MountLeaseRenewer\`
    runs on: bounded renewals under the mount fence; the claim, the remount re-anchor and the farewell
    on the open-fence lease plane. Owned by \`Pool\` and outliving this runtime.`; member
    `CasRequests & farewell_requests;` → `CasRequests & lease_requests;`.
  - `CasMountRuntime.cpp`: the constructor's parameter and initializer, and `installRenewer`'s
    `mount_requests, farewell_requests, layout, …` → `lease_requests`.
  - `CasDecommission.cpp`: comment "`mount_requests`/`farewell_requests`/`gc_requests`" →
    "`mount_requests`/`lease_requests`/`gc_requests`".
  - Re-run the grep: no hit under `src/Disks/DiskObjectStorage`.
- [ ] **Step 2. Preservation.** `ninja -C build unit_tests_dbms > build/build_pr08_t1.log 2>&1`, then
  filter `'CASPool*:CASPoolRemount*:CASHeartbeat*:CASMount*'`, log `build/test_pr08_t1.log`: all pass,
  as on the base. No new test: nothing observable changes.
- [ ] **Step 3. Commit.**
  ```bash
  git add -- src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h \
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.cpp \
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasMountRuntime.h \
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasMountRuntime.cpp \
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasDecommission.cpp
  ```
  Message `Rename the CAS farewell request plane to the lease plane`, trailer
  `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>` (same trailer on every commit).

## Task 2: `Retry::untilDefinitive` and R4 pacing in the engine

**Files:** `CA/Backend/CasRetry.h`, `CA/Backend/CasRetry.cpp`, `CA/Backend/CasRequests.h`,
`CA/Backend/CasRequests.cpp`, `src/Disks/tests/gtest_cas_requests.cpp`.
**Produces:** `Retry::untilDefinitive()`, `Retry::kDefinitiveAttemptSpacingMs`, `Retry::spacedPause`,
`Retry::drawSpacing`, `Retry::attempt_spacing_ms`.

- [ ] **Step 1. Declarations with stub bodies.** `CasRetry.h`: add `#include <limits>`. After
  `policy_deadline_ms`:
  ```cpp
      /// When set, a reissue in `CasOperation`'s write and read loops waits until a draw from [0.8, 1.2]
      /// of this has passed since the failed attempt started, instead of the growing backoff.
      std::optional<uint64_t> attempt_spacing_ms = std::nullopt;

      static constexpr uint64_t kDefinitiveAttemptSpacingMs = 1'000;
  ```
  After `once()`:
  ```cpp
      /// No window and no lease bound: the operation ends on a definitive answer, or when the fence or the
      /// caller's liveness refuses. Reissues are spaced by `kDefinitiveAttemptSpacingMs`.
      static Retry untilDefinitive() { return within(90'000); }
      /// `spacing_draw_ms` minus what the failed attempt took, not below 0. A sample before the start
      /// counts as no time taken.
      static uint64_t spacedPause(uint64_t spacing_draw_ms, uint64_t attempt_started_ms, uint64_t now_ms);
      /// A uniform draw in [0.8, 1.2] of `attempt_spacing_ms_`, saturating at the top of the range.
      static uint64_t drawSpacing(uint64_t attempt_spacing_ms_);
  ```
  `CasRetry.cpp`, stubs: `uint64_t Retry::spacedPause(uint64_t, uint64_t, uint64_t) { return 0; }`,
  `uint64_t Retry::drawSpacing(uint64_t) { return 0; }` (Allman layout).

- [ ] **Step 2. Failing tests.** `gtest_cas_requests.cpp`, after
  `TEST(CASRetry, BindSaturatesAndLeavesAnEqualLeaseOffTheLeaseSource)` (add `#include <algorithm>`):
  ```cpp
  TEST(CASRetrySpacing, UntilDefinitiveHasNoDeadlineAndSpacesItsAttempts)
  {
      constexpr uint64_t largest = std::numeric_limits<uint64_t>::max();
      const Retry policy = Retry::untilDefinitive();
      EXPECT_FALSE(policy.lease_deadline_ms.has_value());
      EXPECT_FALSE(policy.single_attempt);
      EXPECT_EQ(policy.attempt_spacing_ms, std::optional<uint64_t>(1'000));
      for (const uint64_t now : {uint64_t{0}, uint64_t{1'000'000}, largest - 1, largest})
      {
          const Retry::Bound bound = policy.bind(now);
          EXPECT_EQ(bound.deadline_ms, largest) << "now " << now;
          EXPECT_FALSE(bound.lease_bound) << "now " << now;
      }
      EXPECT_FALSE(Retry::standard().attempt_spacing_ms.has_value());
      EXPECT_FALSE(Retry::untilLeaseSafe(2'000'000, 2'000).attempt_spacing_ms.has_value());
  }

  TEST(CASRetrySpacing, SpacedPauseWaitsOutTheRestOfTheDraw)
  {
      EXPECT_EQ(Retry::spacedPause(1'000, 5'000, 5'000), 1'000u);
      EXPECT_EQ(Retry::spacedPause(1'000, 5'000, 5'300), 700u);
      EXPECT_EQ(Retry::spacedPause(1'000, 5'000, 6'000), 0u);
      EXPECT_EQ(Retry::spacedPause(1'000, 5'000, 10'000), 0u);
      EXPECT_EQ(Retry::spacedPause(1'000, 5'000, 4'000), 1'000u);
  }

  TEST(CASRetrySpacing, DrawIsWithinAFifthOfTheSpacing)
  {
      bool low = false;
      bool high = false;
      for (int i = 0; i < 2'000; ++i)
      {
          const uint64_t draw = Retry::drawSpacing(1'000);
          ASSERT_GE(draw, 800u);
          ASSERT_LE(draw, 1'200u);
          low = low || draw < 850;
          high = high || draw > 1'150;
      }
      /// The bounds alone cannot tell a draw from a constant.
      EXPECT_TRUE(low);
      EXPECT_TRUE(high);
      constexpr uint64_t largest = std::numeric_limits<uint64_t>::max();
      EXPECT_GE(Retry::drawSpacing(largest), largest - largest / 5) << "the top of the range must not wrap";
  }
  ```
  After `TEST(CASRequests, AmbiguousCreateThatNeverLandedIsReissued)`:
  ```cpp
  namespace
  {

  /// Each guarded write takes `write_ms` on the injected clock and, while `failing` is positive, throws a
  /// transport timeout after it.
  class SlowFailingWrites final : public CountingBackend
  {
  public:
      explicit SlowFailingWrites(FakeClock & clock_) : clock(clock_) {}

      std::expected<String, Backend::RawConflict> write(const String & key, const String & bytes,
          const std::optional<String> & expected_value, TransportAccess & access) override
      {
          if (expected_value)
          {
              clock.now += write_ms;
              if (failing > 0)
              {
                  --failing;
                  throw Poco::TimeoutException("injected write timeout");
              }
          }
          return CountingBackend::write(key, bytes, expected_value, access);
      }

      uint64_t write_ms = 0;
      int failing = 0;

  private:
      FakeClock & clock;
  };

  }

  /// Unclear writes are reissued past the standard 90 s window, each a spacing draw after the previous
  /// attempt started, and the read that settles each one is sent at once.
  TEST(CASRequestsSpacing, UnclearWritesAreReissuedPastAnyWindowOneSpacingApart)
  {
      FakeClock clock;
      auto backend = std::make_shared<SlowFailingWrites>(clock);
      auto requests = makeRequests(backend, clock);
      auto op = requests.admit();
      const Etag seen = *orThrow(op.create("k", "v1", Retry::standard()), "seed");
      constexpr int failures = 120;
      backend->write_ms = 300;
      backend->failing = failures;
      const uint64_t gets_before = backend->getTotal();
      const uint64_t start = clock.now;

      const WriteResult result = op.replace("k", "v2", seen, Retry::untilDefinitive());

      const auto * committed = std::get_if<Committed>(&result);
      ASSERT_NE(committed, nullptr);
      EXPECT_EQ(committed->attempts_sent, failures + 1u);
      EXPECT_GT(clock.now - start, 90'000u);
      ASSERT_EQ(clock.sleeps.size(), static_cast<size_t>(failures));
      for (const uint64_t pause : clock.sleeps)
      {
          EXPECT_GE(pause, 500u);
          EXPECT_LE(pause, 900u);
      }
      EXPECT_EQ(backend->getTotal() - gets_before, static_cast<uint64_t>(failures)) << "one settling read per unclear attempt";
  }

  TEST(CASRequestsSpacing, ReadRetriesAreSpaced)
  {
      FakeClock clock;
      auto backend = std::make_shared<CountingBackend>();
      auto requests = makeRequests(backend, clock);
      auto op = requests.admit();
      (void)orThrow(op.create("k", "v", Retry::standard()), "seed");
      backend->failNextReadWith("k", std::make_exception_ptr(Poco::TimeoutException("injected read failure")));
      backend->failNextReadWith("k", std::make_exception_ptr(Poco::TimeoutException("injected read failure")));

      const auto object = op.read("k", Retry::untilDefinitive());

      ASSERT_TRUE(object.has_value());
      ASSERT_EQ(clock.sleeps.size(), 2u);
      for (const uint64_t pause : clock.sleeps)
      {
          EXPECT_GE(pause, 800u);
          EXPECT_LE(pause, 1'200u);
      }
  }

  /// Golden: a policy without spacing keeps the growing backoff.
  TEST(CASRequestsSpacing, AStandardPolicyKeepsItsGrowingBackoff)
  {
      FakeClock clock;
      auto backend = std::make_shared<SlowFailingWrites>(clock);
      auto requests = makeRequests(backend, clock);
      auto op = requests.admit();
      const Etag seen = *orThrow(op.create("k", "v1", Retry::standard()), "seed");
      backend->failing = 3;

      const WriteResult result = op.replace("k", "v2", seen, Retry::standard());

      ASSERT_TRUE(std::holds_alternative<Committed>(result));
      ASSERT_EQ(clock.sleeps.size(), 3u);
      EXPECT_LE(clock.sleeps[0], 200u);
      EXPECT_LE(clock.sleeps[1], 400u);
      EXPECT_LE(clock.sleeps[2], 800u);
  }
  ```
- [ ] **Step 3. Red.** Build (`build/build_pr08_t2.log`), filter `'CASRetrySpacing.*:CASRequestsSpacing.*'`,
  log `build/test_pr08_t2_red.log`. Expect `UntilDefinitiveHasNoDeadline…` to fail on
  `policy.attempt_spacing_ms` (`Which is: nullopt`), `SpacedPause…` and `DrawIs…` on the stub zeros,
  `UnclearWrites…` on `ASSERT_NE(committed, nullptr)` (the 90 s window gives up), `ReadRetriesAreSpaced`
  on `EXPECT_GE(pause, 800u)`. `AStandardPolicyKeepsItsGrowingBackoff` passes (golden).
- [ ] **Step 4. Implementation.**
  - `CasRetry.h`: `untilDefinitive` becomes
    ```cpp
    static Retry untilDefinitive()
    {
        return {.window_ms = std::numeric_limits<uint64_t>::max(), .lease_deadline_ms = std::nullopt,
                .single_attempt = false, .attempt_spacing_ms = kDefinitiveAttemptSpacingMs};
    }
    ```
  - `CasRetry.cpp`:
    ```cpp
    uint64_t Retry::spacedPause(uint64_t spacing_draw_ms, uint64_t attempt_started_ms, uint64_t now_ms)
    {
        const uint64_t taken_ms = now_ms > attempt_started_ms ? now_ms - attempt_started_ms : 0;
        return taken_ms >= spacing_draw_ms ? 0 : spacing_draw_ms - taken_ms;
    }

    uint64_t Retry::drawSpacing(uint64_t attempt_spacing_ms_)
    {
        const uint64_t spread = attempt_spacing_ms_ / 5;
        const uint64_t low = attempt_spacing_ms_ - spread;
        const uint64_t high = attempt_spacing_ms_ > std::numeric_limits<uint64_t>::max() - spread
            ? std::numeric_limits<uint64_t>::max()
            : attempt_spacing_ms_ + spread;
        return low + thread_local_rng() % (high - low + 1);
    }
    ```
    (`high - low + 1` cannot wrap: `high - low <= 2 * spread < max`.)
  - `CasRequests.h`, `WriteState`, after `refresh_attempted`:
    `uint64_t attempt_started_ms = 0;` with `/// When the latest attempt was sent; sampled only under \`Retry::attempt_spacing_ms\`.`
    Change `pauseAndReissue`, `pauseForConflict`, `pauseFlat` to take `(WriteState & state, const Retry & policy, const Retry::Bound & bound)`;
    append to the `pauseAndReissue` comment "Under `Retry::attempt_spacing_ms` this and the two
    siblings below sleep the spaced pause instead." After `reissueAtOnce`:
    ```cpp
        /// The pause before a reissue: `unspaced_ms()`, or under `Retry::attempt_spacing_ms` what is left of a
        /// fresh draw since `attempt_started_ms`.
        template <typename UnspacedPause>
        uint64_t reissuePause(const Retry & policy, uint64_t attempt_started_ms, UnspacedPause && unspaced_ms) const
        {
            if (!policy.attempt_spacing_ms)
                return unspaced_ms();
            return Retry::spacedPause(Retry::drawSpacing(*policy.attempt_spacing_ms), attempt_started_ms, owner.now_ms());
        }
    ```
    `readLoop`: after `bool refresh_attempted = false;` add `uint64_t attempt_started_ms = 0;`; before
    `detail::recordAttempt();` add `if (policy.attempt_spacing_ms) attempt_started_ms = owner.now_ms();`
    (two lines, Allman not needed for a single statement); replace
    `const uint64_t pause_ms = Retry::backoff(++ordinary_reissues);` with
    ```cpp
            ++ordinary_reissues;
            const uint64_t pause_ms = reissuePause(
                policy, attempt_started_ms, [ordinary_reissues] { return Retry::backoff(ordinary_reissues); });
    ```
  - `CasRequests.cpp`:
    ```cpp
    std::optional<WriteResult> CasOperation::pauseAndReissue(WriteState & state, const Retry & policy, const Retry::Bound & bound)
    {
        ++state.reissues;
        const uint64_t pause_ms = reissuePause(
            policy, state.attempt_started_ms, [reissues = state.reissues] { return Retry::backoff(reissues); });
        return gatedPause(pause_ms, 2, state, bound, detail::recordReissue, /*should_sleep=*/true);
    }

    std::optional<WriteResult> CasOperation::pauseForConflict(WriteState & state, const Retry & policy, const Retry::Bound & bound)
    {
        const uint64_t pause_ms = reissuePause(policy, state.attempt_started_ms, Retry::conflictBackoff);
        return gatedPause(pause_ms, 2, state, bound, detail::recordConflictPause, /*should_sleep=*/true);
    }

    std::optional<WriteResult> CasOperation::pauseFlat(WriteState & state, const Retry & policy, const Retry::Bound & bound)
    {
        const uint64_t pause_ms = reissuePause(policy, state.attempt_started_ms, [] { return kConnectHintPauseMs; });
        return gatedPause(pause_ms, 2, state, bound, detail::recordReissue, /*should_sleep=*/true);
    }
    ```
    `writeLoop`: before `detail::recordAttempt();` add
    `if (policy.attempt_spacing_ms) state.attempt_started_ms = owner.now_ms();`; pass `policy` at its
    three call sites (`pauseAndReissue` twice, `pauseFlat` once). `readModifyWrite` and
    `readModifyWriteOnPresence`: `state.any_ambiguous ? pauseAndReissue(state, policy, bound) : pauseForConflict(state, policy, bound)`.
    Re-derive the call-site count with `grep -n 'pauseAndReissue\|pauseForConflict\|pauseFlat' CA/Backend/CasRequests.cpp`:
    every call passes `policy`.
- [ ] **Step 5. Green.** Filter `'CASRetry*:CASRequests*:CASHotKeys*'`, log `build/test_pr08_t2_green.log`.
- [ ] **Step 6. Commit** the five files of this task, message `Add a CAS retry policy with no deadline and spaced reissues`.

## Task 3: Deadline published under `driver_mutex` (R5)

**Files:** `CA/Pool/CasMountRuntime.h`, `CA/Pool/CasMountRuntime.cpp`, `src/Disks/tests/gtest_cas_pool.cpp`.
**Produces:** `MountConfig::renewal_finished_hook_for_test`, `CasMountRuntime::publishRenewedDeadline`.

Today `renewRenewerOnce` calls `DriverLease::finish`, which hands the driver back (`WorkerIdle`), and
only then `consumeRenewResult` calls `setMountDeadline` without the mutex. A remount that parks the idle
worker, installs a new renewer and calls `armMountFence` in that window has its deadline overwritten
by the old epoch's renewal.

- [ ] **Step 1. Test hook (no behaviour change).** `CasMountRuntime.h`, `MountConfig`, after
  `renewal_live_for_test`:
  ```cpp
      /// Deterministic test interposition on the renewing thread right after `DriverLease::finish`
      /// returned driver ownership, before the result is consumed. Runs without `driver_mutex`.
      std::function<void(const MountRenewResult &)> renewal_finished_hook_for_test = {};
  ```
  `CasMountRuntime.cpp`, `renewRenewerOnce`, right after
  `const RenewalDriverState returned_state = call.lease->finish(destination, &result);`:
  ```cpp
      if (config.renewal_finished_hook_for_test)
          config.renewal_finished_hook_for_test(result);
  ```
- [ ] **Step 2. Failing test.** `gtest_cas_pool.cpp`, after `TEST(CASPoolRemount, ImmediatePostRemountRenewalFailureIsNotDropped)`:
  ```cpp
  /// A renewal of the old epoch whose result is consumed after a remount armed the new epoch does not
  /// move the new epoch's deadline.
  TEST(CASMountRuntime, AnOldEpochRenewalDoesNotMoveTheNewEpochDeadline)
  {
      auto backend = std::make_shared<RuntimeRenewBackend>();
      const Layout layout("runtime-old-epoch-deadline");
      uint64_t wall_ms = 1000;
      std::atomic<uint64_t> boot_ms{100};
      const UInt128 uuid{1};
      constexpr uint64_t ttl_ms = 10'000;
      ASSERT_EQ(claimMount(*DB::Cas::tests::OperationForTest(backend), layout, "test", uuid, 1, wall_ms, ttl_ms).kind,
                MountClaimResult::Claimed);
      DB::Cas::tests::ManualBarrier finished;
      DB::Cas::tests::ManualBarrier remounted;
      DB::Cas::tests::ManualBarrier back_in_loop;
      std::atomic<uint32_t> commits{0};
      std::atomic<uint32_t> loop_passes{0};
      CasMountRuntime * runtime_ptr = nullptr;
      CasEventSink sink;
      RuntimeUnderTest runtime_holder(
          backend, layout,
          MountConfig{
              .mount_lease_ttl_ms = std::chrono::milliseconds(ttl_ms),
              .background_watermark = true,
              .boot_ms_fn = [&] { return boot_ms.load(); },
              .renewal_before_driver_lock_hook_for_test = [&]
              {
                  if (++loop_passes == 2)
                      back_in_loop.arriveAndWait();
              },
              .renewal_finished_hook_for_test = [&](const MountRenewResult & result)
              {
                  if (result.outcome == MountRenewOutcome::Committed && ++commits == 1)
                      finished.arriveAndWait();
              }},
          "test", sink, runtimeRenewBudget(), [&]
          {
              fenceOutMount(*backend, layout.mountKey("test"));
              const MountClaimResult fresh = claimMount(
                  *DB::Cas::tests::OperationForTest(backend), layout, "test", uuid, 2, wall_ms, ttl_ms);
              EXPECT_EQ(fresh.kind, MountClaimResult::Claimed);
              runtime_ptr->installRenewer(uuid, 2, [&] { return wall_ms; });
              const uint64_t fresh_anchor = runtime_ptr->startRenewer();
              runtime_ptr->armMountFence(uuid, 2, fresh_anchor + 5 * ttl_ms);
              runtime_ptr->noteRemounted();
              remounted.arriveAndWait();
              return true;
          });
      CasMountRuntime & runtime = *runtime_holder;
      runtime_ptr = &runtime;
      runtime.installRenewer(uuid, 1, [&] { return wall_ms; });
      const uint64_t anchor = runtime.startRenewer();
      runtime.armMountFence(uuid, 1, anchor + ttl_ms);
      /// The epoch-1 renewal is due at once; the epoch-2 renewer's first one is an hour after its claim.
      const uint64_t renewal_start = anchor + 3'600'000;
      boot_ms = renewal_start;
      runtime.startBackgroundWorkers(std::chrono::hours(1));

      finished.waitUntilArrived();
      runtime.tripMountLost();
      runtime.scheduleRemount();
      remounted.waitUntilArrived();
      remounted.release();
      finished.release();
      back_in_loop.waitUntilArrived();

      /// Past the old renewal's deadline (its start plus one lease), before the new epoch's.
      boot_ms = renewal_start + 2 * ttl_ms;
      EXPECT_TRUE(runtime.mayMutate()) << "the old epoch's renewal moved the new epoch's deadline";
      back_in_loop.release();
      runtime.stopBackgroundWorkers();
      runtime.finishTeardown(false);
  }
  ```
- [ ] **Step 3. Red.** Build (`build/build_pr08_t3.log`), filter `'CASMountRuntime.AnOldEpoch*'`, log
  `build/test_pr08_t3_red.log`. Expect `Value of: runtime.mayMutate() Actual: false Expected: true`
  with the message above.
- [ ] **Step 4. Fix.**
  - `CasMountRuntime.h`, `DriverLease`: `RenewalDriverState finish(RenewalDriverState ordinary_destination,
    const MountRenewResult * result = nullptr, const MountLeaseRenewer * renewer = nullptr);`.
    Private section, after `consumeRenewResult`:
    ```cpp
        /// Publish a committed renewal's deadline, its start plus the lease. Requires `driver_mutex`.
        void publishRenewedDeadline(uint64_t attempt_start_boot_ms);
    ```
  - `CasMountRuntime.cpp`, `DriverLease::finish`: new third parameter; first statement after the lock:
    ```cpp
        /// Published before the driver leaves the call, so a remount cannot arm a new epoch between this
        /// renewal's commit and its deadline; a renewer that was replaced moves nothing.
        if (result && result->outcome == MountRenewOutcome::Committed && renewer && renewer == runtime.mount_renewer.get())
            runtime.publishRenewedDeadline(result->attempt_start_boot_ms);
    ```
    New function:
    ```cpp
    void CasMountRuntime::publishRenewedDeadline(uint64_t attempt_start_boot_ms)
    {
        const uint64_t ttl_ms = static_cast<uint64_t>(config.mount_lease_ttl_ms.count());
        setMountDeadline(attempt_start_boot_ms > std::numeric_limits<uint64_t>::max() - ttl_ms
            ? std::numeric_limits<uint64_t>::max()
            : attempt_start_boot_ms + ttl_ms);
    }
    ```
    `renewRenewerOnce`: `call.lease->finish(destination, &result, call.renewer)`. `consumeRenewResult`:
    the `Committed` branch loses its `setMountDeadline(…)` statement and its `ttl_ms` local and keeps
    `reportMountRenewCompletion(result); return;`.
- [ ] **Step 5. Green.** Filter `'CASMountRuntime*:CASPoolRemount*:CASPool*:CASObservability*'`, log
  `build/test_pr08_t3_green.log`.
- [ ] **Step 6. Commit** the three files, message `Publish the CAS mount deadline under the driver mutex`.

## Task 4: One tuple per logical renewal; the worker renews until a definitive answer

**Files:** `CA/Backend/CasWriteResult.h`, `CA/Backend/CasRequests.cpp`, `CA/Pool/CasServerRoot.h`,
`CA/Pool/CasServerRoot.cpp`, `CA/Pool/CasMountRuntime.cpp`, `src/Disks/tests/gtest_cas_heartbeat.cpp`,
`src/Disks/tests/gtest_cas_pool.cpp`. **Consumes:** Tasks 1–3, PR 4's observer.
**Produces:** `Refused::credential`, `MountLeaseRenewer::renewUntilDefinitive`,
`MountRenewOperationEnvironment::credential_refusal_pause_ms`.

- [ ] **Step 1. Declarations with today's behaviour.**
  - `CasWriteResult.h`, `Refused`: append `bool credential = false;` and to its comment "`credential` is
    true when the refusal is of the class a fresh credential could fix (`isRefreshableCredentialError`)
    and the one refresh the call allowed did not help." `CasRequests.cpp`, `writeLoop`:
    `return Refused{e.code(), e.message(), state.attempts_sent, credential_answer};`.
  - `CasServerRoot.h`, `MountRenewOperationEnvironment`, last member:
    ```cpp
        /// How long the background renewal waits before its next engine call after a credential refusal.
        /// Each call may refresh the credentials once, so a one-second pace would refresh every second.
        uint64_t credential_refusal_pause_ms = 0;
    ```
    `MountLeaseRenewer`, public, after `renewForRemount`:
    ```cpp
        /// The background renewal, on the lease plane, retried until a definitive answer: a commit, a
        /// conflict the read observed, a local failure, or `environment.live` refusing. A refusal sends the
        /// same body again, and a refused precondition whose read failed repeats only the read.
        MountRenewResult renewUntilDefinitive(const MountRenewOperationEnvironment & environment);
    ```
    `CasServerRoot.cpp`: `renewUntilDefinitive` returns `renewOn(open_requests, environment);`.
  - `gtest_cas_heartbeat.cpp`, `RenewalScriptBackend`: add actions `ThrowAccessDenied`, `ThrowMalformed`,
    `RefusePrecondition`; field `const uint64_t * clock = nullptr;`; `Attempt` gains `uint64_t at_ms = 0;`
    and `attempts.push_back({key, bytes, expected_value, clock ? *clock : 0});`; fields
    `uint32_t refuse_reads = 0;` and `std::vector<size_t> puts_at_read;`. In `write`, after the
    `ThrowConnectHint` block:
    ```cpp
            if (action == Action::ThrowAccessDenied || action == Action::ThrowMalformed)
            {
    #if USE_AWS_S3
                if (action == Action::ThrowAccessDenied)
                    throw DB::S3Exception("injected renewal access denial", Aws::S3::S3Errors::ACCESS_DENIED);
                throw DB::S3Exception("injected malformed renewal request", Aws::S3::S3Errors::INVALID_REQUEST);
    #else
                throw DB::Exception(DB::ErrorCodes::ABORTED, "store refusals need the S3 client");
    #endif
            }
            if (action == Action::RefusePrecondition)
                return std::unexpected(RawConflict{});
    ```
    In `read`, after `++read_calls;`:
    ```cpp
            puts_at_read.push_back(attempts.size());
            if (refuse_reads > 0 && key.ends_with("/mount"))
            {
                --refuse_reads;
    #if USE_AWS_S3
                throw DB::S3Exception("injected read access denial", Aws::S3::S3Errors::ACCESS_DENIED);
    #else
                throw DB::Exception(DB::ErrorCodes::ABORTED, "store refusals need the S3 client");
    #endif
            }
    ```
  - `gtest_cas_pool.cpp`, `RuntimeRenewBackend`: add
    ```cpp
        /// While it answers true, every guarded write fails with a transport timeout before `fault` is
        /// consulted. Set it before the workers start.
        std::function<bool()> outage;
        /// Runs inside every write `outage` fails, before it throws.
        std::function<void()> on_outage_write;
        std::atomic<uint64_t> outage_writes{0};
    ```
    and in `write`, right after the `if (!expected_value) return …;` line:
    ```cpp
            if (outage && outage())
            {
                outage_writes.fetch_add(1, std::memory_order_relaxed);
                if (on_outage_write)
                    on_outage_write();
                throw Poco::TimeoutException("injected mount PUT outage");
            }
    ```
    `RuntimeUnderTest`: member `farewell` → `lease` (initializer and the `runtime(backend, mount, lease, …)`
    argument); public:
    ```cpp
        /// The retry wait of the mount and lease planes. Call before the workers start.
        void setRetrySleepForTest(const std::function<void(uint64_t)> & sleep_fn)
        {
            mount.setSleepFnForTest(sleep_fn);
            lease.setSleepFnForTest(sleep_fn);
        }
    ```
  Build: green, no test changed behaviour.

- [ ] **Step 2. Failing renewer tests** in `gtest_cas_heartbeat.cpp`, after
  `TEST(CASHeartbeat, RenewalRetriesOneImmutableBodyAndAdoptsLostResponse)`. All share this setup,
  written out in each test:
  ```cpp
      auto backend = std::make_shared<RenewalScriptBackend>();
      Layout layout("pool");
      const UInt128 uuid{0x1234};
      uint64_t wall_ms = 1000;
      uint64_t boot_ms = 100;
      Ops ops(backend, &boot_ms);
      backend->clock = &boot_ms;
      seedOwnClaim(ops.op, layout, "test", uuid, 9, wall_ms, 1000);
      MountLeaseRenewer renewer(
          ops.mount, ops.farewell, layout, "test", uuid, 9, std::chrono::milliseconds(1000),
          [&] { return wall_ms; }, [] { return uint64_t{7}; }, {}, std::chrono::milliseconds(20),
          [&] { return boot_ms; });
      renewer.start();
      backend->attempts.clear();
      backend->read_calls = 0;
      backend->puts_at_read.clear();
  ```
  ```cpp
  /// The background renewal outlives its lease with one tuple, and confirms its first start.
  TEST(CASHeartbeat, UntilDefinitiveRetriesOneTuplePastTheLease)
  {
      /* setup */
      for (int i = 0; i < 10; ++i)
          backend->actions.push_back(RenewalScriptBackend::Action::ThrowBefore);
      const uint64_t start = boot_ms;

      const MountRenewResult renewed = renewer.renewUntilDefinitive(renewalEnvironment(boot_ms));

      ASSERT_EQ(renewed.outcome, MountRenewOutcome::Committed);
      EXPECT_EQ(renewed.attempt_start_boot_ms, start);
      EXPECT_EQ(renewed.attempts_sent, 11u);
      ASSERT_EQ(backend->attempts.size(), 11u);
      EXPECT_GE(backend->attempts.back().at_ms, start + 8'000) << "the renewal kept retrying past the lease";
      for (size_t i = 1; i < backend->attempts.size(); ++i)
      {
          EXPECT_EQ(backend->attempts[i].bytes, backend->attempts[0].bytes);
          EXPECT_EQ(backend->attempts[i].expected, backend->attempts[0].expected);
          EXPECT_GE(backend->attempts[i].at_ms - backend->attempts[i - 1].at_ms, 800u);
          EXPECT_LE(backend->attempts[i].at_ms - backend->attempts[i - 1].at_ms, 1'200u);
      }
  }

  #if USE_AWS_S3
  /// A refusal is not an answer about the slot: the next engine call sends the same tuple, one spacing
  /// later after a malformed request and one renewal period later after a credential refusal.
  TEST(CASHeartbeat, ARefusedRenewalIsSentAgainWithTheSameTuple)
  {
      /* setup */
      backend->actions = {RenewalScriptBackend::Action::ThrowMalformed, RenewalScriptBackend::Action::ThrowAccessDenied,
                          RenewalScriptBackend::Action::ThrowAccessDenied, RenewalScriptBackend::Action::Delegate};
      MountRenewOperationEnvironment environment = renewalEnvironment(boot_ms);
      environment.credential_refusal_pause_ms = 10'000;
      const uint64_t start = boot_ms;

      const MountRenewResult renewed = renewer.renewUntilDefinitive(environment);

      ASSERT_EQ(renewed.outcome, MountRenewOutcome::Committed);
      EXPECT_EQ(renewed.attempt_start_boot_ms, start);
      EXPECT_EQ(renewed.attempts_sent, 4u);
      ASSERT_EQ(backend->attempts.size(), 4u);
      for (const auto & attempt : backend->attempts)
      {
          EXPECT_EQ(attempt.bytes, backend->attempts[0].bytes);
          EXPECT_EQ(attempt.expected, backend->attempts[0].expected);
      }
      const auto gap = [&](size_t i) { return backend->attempts[i].at_ms - backend->attempts[i - 1].at_ms; };
      EXPECT_GE(gap(1), 800u);
      EXPECT_LE(gap(1), 1'200u);
      EXPECT_EQ(gap(2), 10'000u);
      EXPECT_EQ(gap(3), 10'000u);
      EXPECT_EQ(backend->read_calls, 0u) << "a refusal owes no read";
  }

  /// A refused precondition whose read failed is not a lost slot: only the read repeats, and a read that
  /// finds the slot as the last confirmed renewal left it sends the same tuple again.
  TEST(CASHeartbeat, AfterACleanPreconditionRefusalOnlyTheReadRepeats)
  {
      /* setup */
      backend->actions = {RenewalScriptBackend::Action::RefusePrecondition, RenewalScriptBackend::Action::Delegate};
      backend->refuse_reads = 3;

      const MountRenewResult renewed = renewer.renewUntilDefinitive(renewalEnvironment(boot_ms));

      ASSERT_EQ(renewed.outcome, MountRenewOutcome::Committed);
      ASSERT_EQ(backend->attempts.size(), 2u);
      EXPECT_EQ(backend->attempts[1].bytes, backend->attempts[0].bytes);
      EXPECT_EQ(backend->attempts[1].expected, backend->attempts[0].expected);
      EXPECT_EQ(backend->read_calls, 4u);
      EXPECT_EQ(backend->puts_at_read, (std::vector<size_t>{1, 1, 1, 1})) << "no PUT between the reads";
  }

  /// A refusal that follows an unclear attempt does not end the engine call: the read settles the
  /// unclear attempt, and our attempt that landed meanwhile is adopted, not taken for another writer.
  TEST(CASHeartbeat, ARefusalAfterAnUnclearAttemptIsSettledInsideTheCall)
  {
      /* setup */
      backend->actions = {RenewalScriptBackend::Action::ThrowBeforeThenLandAfterResolve,
                          RenewalScriptBackend::Action::ThrowAccessDenied};

      const MountRenewResult renewed = renewer.renewUntilDefinitive(renewalEnvironment(boot_ms));

      ASSERT_EQ(renewed.outcome, MountRenewOutcome::Committed);
      EXPECT_TRUE(renewed.resolved_by_read);
      EXPECT_EQ(renewed.attempts_sent, 2u);
      EXPECT_EQ(backend->attempts.size(), 2u);
  }
  #endif
  ```
  In `gtest_cas_pool.cpp`, after the Task 3 test:
  ```cpp
  /// A mount PUT outage three leases long is a pause: the renewal keeps its tuple, its late success leaves
  /// writes closed and starts the next renewal at once, that one opens them under the same fence
  /// generation, and no remount is requested.
  TEST(CASMountRuntime, ALongOutageIsAPauseUnderTheSameEpoch)
  {
      const Layout layout("until-definitive-long-outage");
      const UInt128 uuid{1};
      uint64_t wall_ms = 1000;
      std::atomic<uint64_t> boot_ms{100};
      std::atomic<uint64_t> outage_until{0};
      std::atomic<uint32_t> remount_calls{0};
      std::vector<MountRenewResult> commits;
      std::vector<uint64_t> boot_at_commit;
      std::vector<bool> may_mutate_after_commit;
      DB::Cas::tests::ManualBarrier second_commit;
      CasMountRuntime * runtime_ptr = nullptr;
      auto backend = std::make_shared<RuntimeRenewBackend>();
      ASSERT_EQ(claimMount(*DB::Cas::tests::OperationForTest(backend), layout, "test", uuid, 1, wall_ms, 1000).kind,
                MountClaimResult::Claimed);
      CasEventSink sink;
      RuntimeUnderTest runtime_holder(
          backend, layout,
          MountConfig{
              .mount_lease_ttl_ms = std::chrono::milliseconds(1000),
              .background_watermark = true,
              .boot_ms_fn = [&] { return boot_ms.load(); },
              .renewal_finished_hook_for_test = [&](const MountRenewResult & result)
              {
                  if (result.outcome != MountRenewOutcome::Committed || commits.size() >= 2)
                      return;
                  commits.push_back(result);
                  boot_at_commit.push_back(boot_ms.load());
                  may_mutate_after_commit.push_back(runtime_ptr->mayMutate());
                  if (commits.size() == 2)
                      second_commit.arriveAndWait();
              }},
          "test", sink, runtimeRenewBudget(), [&]
          {
              ++remount_calls;
              return false;
          });
      CasMountRuntime & runtime = *runtime_holder;
      runtime_ptr = &runtime;
      runtime.installRenewer(uuid, 1, [&] { return wall_ms; });
      const uint64_t anchor = runtime.startRenewer();
      runtime.armMountFence(uuid, 1, anchor + 1000);
      const uint64_t generation = runtime.fenceGeneration();
      runtime_holder.setRetrySleepForTest([&](uint64_t ms) { boot_ms += ms; });
      /// The first renewal is due at once and meets three lease lengths of outage.
      boot_ms = anchor + 500;
      outage_until = anchor + 3'500;
      backend->outage = [&] { return boot_ms.load() < outage_until.load(); };
      runtime.startBackgroundWorkers(std::chrono::milliseconds(500));

      second_commit.waitUntilArrived();
      EXPECT_EQ(commits[0].attempt_start_boot_ms, anchor + 500) << "the start of the logical renewal, not of its last attempt";
      EXPECT_GT(commits[0].attempts_sent, 2u);
      EXPECT_GE(boot_at_commit[0], anchor + 3'500);
      EXPECT_FALSE(may_mutate_after_commit[0]) << "a success whose start plus lease is past opens no write";
      EXPECT_EQ(commits[1].attempt_start_boot_ms, boot_at_commit[0]) << "the next renewal starts at once";
      EXPECT_TRUE(may_mutate_after_commit[1]);
      EXPECT_EQ(runtime.fenceGeneration(), generation) << "a pause neither trips nor re-arms the fence";
      EXPECT_EQ(runtime.lifecycle(), PoolLifecycle::Live);
      EXPECT_EQ(remount_calls.load(), 0u);
      second_commit.release();
      runtime.stopBackgroundWorkers();
      runtime.finishTeardown(false);
  }

  /// A remount request ends a renewal that is retrying past its lease, at its next admission.
  TEST(CASMountRuntime, ARemountRequestEndsARenewalRetryingPastTheLease)
  {
      const Layout layout("until-definitive-park");
      const UInt128 uuid{1};
      uint64_t wall_ms = 1000;
      std::atomic<uint64_t> boot_ms{100};
      std::atomic<uint64_t> past_the_lease{std::numeric_limits<uint64_t>::max()};
      std::atomic<bool> held{false};
      std::atomic<uint64_t> writes_at_remount{0};
      DB::Cas::tests::ManualBarrier holding;
      DB::Cas::tests::ManualBarrier remount_entered;
      auto backend = std::make_shared<RuntimeRenewBackend>();
      ASSERT_EQ(claimMount(*DB::Cas::tests::OperationForTest(backend), layout, "test", uuid, 1, wall_ms, 1000).kind,
                MountClaimResult::Claimed);
      CasEventSink sink;
      RuntimeUnderTest runtime_holder(
          backend, layout,
          MountConfig{.mount_lease_ttl_ms = std::chrono::milliseconds(1000), .background_watermark = true,
                      .boot_ms_fn = [&] { return boot_ms.load(); }},
          "test", sink, runtimeRenewBudget(), [&]
          {
              writes_at_remount = backend->outage_writes.load();
              remount_entered.arriveAndWait();
              return false;
          });
      CasMountRuntime & runtime = *runtime_holder;
      runtime.installRenewer(uuid, 1, [&] { return wall_ms; });
      const uint64_t anchor = runtime.startRenewer();
      runtime.armMountFence(uuid, 1, anchor + 1000);
      /// Five lease lengths on: a renewal bounded by its lease ended long before.
      past_the_lease = anchor + 5'000;
      runtime_holder.setRetrySleepForTest([&](uint64_t ms) { boot_ms += ms; });
      backend->on_outage_write = [&]
      {
          if (boot_ms.load() >= past_the_lease.load() && !held.exchange(true))
              holding.arriveAndWait();
      };
      backend->outage = [] { return true; };
      runtime.startBackgroundWorkers(std::chrono::milliseconds(0));

      holding.waitUntilArrived();
      const uint64_t writes_at_park = backend->outage_writes.load();
      runtime.scheduleRemount();
      EXPECT_EQ(runtime.renewalDriverStateForTest(), RenewalDriverState::ParkRequested);
      holding.release();
      remount_entered.waitUntilArrived();
      EXPECT_EQ(writes_at_remount.load(), writes_at_park) << "no request starts after the remount request";
      remount_entered.release();
      runtime.stopBackgroundWorkers();
      runtime.finishTeardown(false);
  }
  ```
- [ ] **Step 3. Red.** Build (`build/build_pr08_t4.log`), filter
  `'CASHeartbeat.UntilDefinitive*:CASHeartbeat.ARefused*:CASHeartbeat.AfterAClean*:CASHeartbeat.ARefusalAfter*:CASMountRuntime.ALongOutage*:CASMountRuntime.ARemountRequestEnds*'`,
  log `build/test_pr08_t4_red.log`. Expect: `UntilDefinitive…` fails on `renewed.outcome` (the bounded
  stub ends `Terminal`); `ARefused…` and `AfterAClean…` fail on `renewed.outcome` (`Refused` and
  `Conflict{NotObserved}` are terminal today); `ALongOutage…` and `ARemountRequestEnds…` fail with
  `CAS test barrier timed out waiting for arrival` (the worker gives up at its lease and remounts).
  `ARefusalAfterAnUnclearAttemptIsSettledInsideTheCall` passes: it fences the engine guard, which
  exists today.
- [ ] **Step 4. Implementation.**
  - `CasServerRoot.h`: `renewOn` becomes
    `MountRenewResult renewOn(CasRequests & plane, const MountRenewOperationEnvironment & environment, bool until_definitive);`
    and, after it:
    ```cpp
        /// Repeats the read of a slot whose precondition was refused, one spacing apart, until it observes
        /// the slot. `NotObserved` only when `op` is no longer admitted.
        Observation readUntilObserved(CasOperation & op, const Retry & policy) const;
    ```
    Member after `last_etag`:
    ```cpp
        /// The bytes `last_etag` names. A read that returns them under that token says the slot did not move.
        String last_body;
    ```
    Replace the `PLANES.` paragraph of the class comment with:
    ```cpp
    /// PLANES. The bounded renewal (`renew`: startup and direct calls) is admitted under the mount fence,
    /// because it writes under the authority the fence tracks. The background renewal, the claim, the
    /// remount re-anchor and the farewell run on the lease plane, whose fence is open: the background
    /// renewal must outlive its lease, a self-remount claims with the fence already latched lost, and a
    /// farewell refused because the fence ran down would leave the slot looking live until GC fences it
    /// out. None is unguarded: every write is conditional, and a caller with shutdown facts hands them
    /// over as a `Liveness`.
    ```
    and the `renew` comment with `/// The bounded renewal, admitted under the mount fence: the startup re-anchor and direct calls.`
  - `CasServerRoot.cpp`: `renew` → `renewOn(mount_requests, environment, /*until_definitive=*/false)`,
    `renewForRemount` → `renewOn(open_requests, environment, /*until_definitive=*/false)`,
    `renewUntilDefinitive` → `renewOn(open_requests, environment, /*until_definitive=*/true)`.
    `start`: after `last_etag = etag;` add `last_body = body;`. `terminate`, `Committed` branch: after
    `last_etag = std::move(committed->etag);` add `last_body = body;`. New function:
    ```cpp
    Observation MountLeaseRenewer::readUntilObserved(CasOperation & op, const Retry & policy) const
    {
        while (op.admitted())
        {
            op.pause(Retry::drawSpacing(Retry::kDefinitiveAttemptSpacingMs));
            try
            {
                std::optional<Object> got = op.read(key, policy);
                if (!got)
                    return ProvenAbsent{};
                return std::move(*got);
            }
            catch (const Exception & e)
            {
                if (isDeterministicLocalFailure(e.code()))
                    throw;
            }
        }
        return NotObserved{};
    }
    ```
    In `renewOn`, add the parameter and replace the block from `CasOperation op = plane.admit(environment.live);`
    (PR 4's `op.observeRequests(environment.requests);` included) through the closing brace of
    `catch (...)` with:
    ```cpp
        CasOperation op = plane.admit(environment.live);
        /// Counted here rather than taken from one engine call: the background renewal may span several.
        uint32_t attempts_sent = 0;
        uint64_t last_attempt_start_boot_ms = attempt_start_boot_ms;
        op.observeRequests(RequestObserver{
            .on_write_attempt = [&](uint32_t)
            {
                ++attempts_sent;
                last_attempt_start_boot_ms = boot_clock();
                if (environment.requests.on_write_attempt)
                    environment.requests.on_write_attempt(attempts_sent);
            },
            .on_failure = environment.requests.on_failure,
        });
        const Etag expected = precondition();
        const Retry policy = until_definitive
            ? Retry::untilDefinitive()
            : Retry::untilLeaseSafe(confirmed_deadline_boot_ms, static_cast<uint64_t>(lease_safety_margin.count()));
        std::optional<WriteResult> written;
        try
        {
            written = op.replace(key, body, expected, policy);
            while (until_definitive)
            {
                bool credential_refusal = false;
                if (const Refused * refused = std::get_if<Refused>(&*written))
                {
                    /// `writeLoop` refuses only when no attempt of the call is unclear, so no copy of ours
                    /// can land behind a new call with the same tuple.
                    credential_refusal = refused->credential;
                }
                else if (Conflict * conflict = std::get_if<Conflict>(&*written))
                {
                    if (std::holds_alternative<NotObserved>(conflict->seen))
                        conflict->seen = readUntilObserved(op, policy);
                    if (std::holds_alternative<NotObserved>(conflict->seen))
                    {
                        written = GaveUp{GaveUp::Why::FenceLost, GaveUp::Source::Policy, attempts_sent != 0, NotObserved{}, attempts_sent};
                        break;
                    }
                    /// Our last confirmed body under the token we named: the store refused, the slot did not move.
                    const Object * seen = std::get_if<Object>(&conflict->seen);
                    if (!seen || seen->etag != expected || seen->bytes != last_body)
                        break;
                }
                else
                {
                    break;
                }
                op.pause(credential_refusal
                    ? environment.credential_refusal_pause_ms
                    : Retry::spacedPause(Retry::drawSpacing(Retry::kDefinitiveAttemptSpacingMs), last_attempt_start_boot_ms, boot_clock()));
                written = op.replace(key, body, expected, policy);
            }
        }
        catch (...)
        {
            /// The engine surfaces a deterministic local failure unchanged rather than reissuing it.
            markMountRenewTermination(MountRenewTerminalClassification::DeterministicFailure);
            result.failure = std::current_exception();
            result.attempts_sent = attempts_sent;
            return terminalResult(std::move(result));
        }
    ```
    In the four result branches replace `result.attempts_sent = committed->attempts_sent;` (and the
    `conflict->`, `refused->`, `gave_up->` versions) with `result.attempts_sent = attempts_sent;`. In the
    `Committed` branch add `last_body = body;` after `last_etag = std::move(committed->etag);`. The
    `Refused` branch stays terminal; only the bounded call reaches it.
  - `CasMountRuntime.cpp`, `renewRenewerOnce` (inside PR 4's block):
    ```cpp
            if (active == RenewalDriverState::RemountCall)
                result = call.renewer->renewForRemount(renewalEnvironment(worker_call));
            else if (worker_call)
                result = call.renewer->renewUntilDefinitive(renewalEnvironment(worker_call));
            else
                result = call.renewer->renew(renewalEnvironment(worker_call));
    ```
    `renewalEnvironment`: before the `return`, add
    ```cpp
        uint64_t period_ms = 0;
        if (worker_call)
        {
            std::lock_guard lock(driver_mutex);
            period_ms = static_cast<uint64_t>(std::max<int64_t>(0, renewal_period.count()));
        }
    ```
    and the last initializer `.credential_refusal_pause_ms = period_ms,`.
  - Re-target the three worker tests that forced a terminal renewal by expiring the clock inside the
    attempt; a transient fault is now retried, so each gets a definitive answer instead. Sweep derived with
    `grep -n 'before_throw = ' src/Disks/tests/gtest_cas_pool.cpp` filtered to tests that start background
    workers (the `ParkedRedo…` and `RenewWatermarkOnce…` hits drive bounded renewals and stay):
    `TerminalDepositionDoesNotTouchRenewerAfterReplacement`, `ImmediatePostRemountRenewalFailureIsNotDropped`,
    `DeterministicWorkerFailureFencesWithoutWaitingForCadence`. In each, replace the two lines
    `backend->fault = RuntimeRenewBackend::Fault::ThrowBefore;` and `backend->before_throw = …;` (and the
    comment above them) with
    ```cpp
        /// A definitive answer ends the worker's renewal; a transient fault would only be retried.
        fenceOutMount(*backend, layout.mountKey("test"));
    ```
- [ ] **Step 5. Green and the guard mutation.** Filter `'CAS*'`, log `build/test_pr08_t4_green.log`:
  all pass. Then delete `&& !state.any_ambiguous` from `if (definitely_refused && !state.any_ambiguous)`
  in `writeLoop`, rebuild, run `'CASHeartbeat.ARefusalAfterAnUnclearAttemptIsSettledInsideTheCall'`
  (log `build/test_pr08_t4_mutation.log`): it must fail on `renewed.outcome` (the new call meets our own
  landed body and ends `Terminal`). Restore the guard; `git diff -- …/CasRequests.cpp` shows only the
  `Refused{…, credential_answer}` change. Paste the mutation failure line into the commit body.
- [ ] **Step 6. Commit** the seven files of this task, message
  `Renew the CAS mount lease in the background until a definitive answer`.

## Task 5: An expired lease is a pause: one `WARNING`, `lease_expired` in `system.cas_mounts`

**Files:** `CA/Pool/CasMountRuntime.h`, `CA/Pool/CasMountRuntime.cpp`, `CA/Pool/CasPool.h`,
`CA/Pool/CasPool.cpp`, `CA/ContentAddressedMetadataStorage.h`, `CA/ContentAddressedMetadataStorage.cpp`,
`src/Storages/System/StorageSystemContentAddressedMounts.cpp`, `src/Disks/tests/gtest_cas_pool.cpp`,
`src/Disks/tests/gtest_cas_lifecycle_snapshot.cpp`.
**Produces:** `CasMountRuntime::leaseExpiredAtBootMs`, `Pool::LifecycleSnapshot::lease_expired`.

- [ ] **Step 1. Stubs.** `CasMountRuntime.h`, public, after `refAppendFenceOk`:
  ```cpp
      /// The confirmed deadline, once it has passed while the pool is `Live` and the fence is not lost: the
      /// lease expired and the renewal keeps retrying. Empty otherwise. I/O-free.
      std::optional<uint64_t> leaseExpiredAtBootMs() const;
  ```
  Private: `void warnOnceIfLeaseExpired() noexcept;` with `/// One \`WARNING\` per expiry, at the first renewal attempt after it.`
  and `std::atomic<bool> lease_expiry_warned{false};` after `fence_generation`. `CasMountRuntime.cpp`:
  `leaseExpiredAtBootMs` returns `std::nullopt`; `warnOnceIfLeaseExpired` has an empty body; in
  `renewalEnvironment`, first statement of PR 4's `on_write_attempt` lambda: `warnOnceIfLeaseExpired();`.
  `CasPool.h`, `LifecycleSnapshot`: `bool lease_expired = false;` with `/// \`Live\`, but the lease
  expired; \`since\` is then the deadline that passed.`.
- [ ] **Step 2. Failing tests.** `gtest_cas_pool.cpp`, after `ARemountRequestEndsARenewalRetryingPastTheLease`:
  ```cpp
  /// One `WARNING` per expiry, written at the first attempt after it, and the expiry is visible while it lasts.
  TEST(CASMountRuntime, ALeaseExpiryIsLoggedOnceAndReported)
  {
      ScopedRemountLogCapture logs;
      const Layout layout("until-definitive-expiry-log");
      const UInt128 uuid{1};
      uint64_t wall_ms = 1000;
      std::atomic<uint64_t> boot_ms{100};
      std::atomic<uint64_t> outage_until{0};
      std::vector<std::optional<uint64_t>> expired_at_write;
      std::atomic<uint32_t> commits{0};
      String log_at_second_commit;
      DB::Cas::tests::ManualBarrier second_commit;
      CasMountRuntime * runtime_ptr = nullptr;
      auto backend = std::make_shared<RuntimeRenewBackend>();
      ASSERT_EQ(claimMount(*DB::Cas::tests::OperationForTest(backend), layout, "test", uuid, 1, wall_ms, 1000).kind,
                MountClaimResult::Claimed);
      CasEventSink sink;
      RuntimeUnderTest runtime_holder(
          backend, layout,
          MountConfig{
              .mount_lease_ttl_ms = std::chrono::milliseconds(1000),
              .background_watermark = true,
              .boot_ms_fn = [&] { return boot_ms.load(); },
              .renewal_finished_hook_for_test = [&](const MountRenewResult & result)
              {
                  if (result.outcome == MountRenewOutcome::Committed && ++commits == 2)
                  {
                      log_at_second_commit = logs.captured();
                      second_commit.arriveAndWait();
                  }
              }},
          "test", sink, runtimeRenewBudget(), [] { return false; });
      CasMountRuntime & runtime = *runtime_holder;
      runtime_ptr = &runtime;
      runtime.installRenewer(uuid, 1, [&] { return wall_ms; });
      const uint64_t anchor = runtime.startRenewer();
      runtime.armMountFence(uuid, 1, anchor + 1000);
      runtime_holder.setRetrySleepForTest([&](uint64_t ms) { boot_ms += ms; });
      boot_ms = anchor + 500;
      outage_until = anchor + 3'500;
      backend->on_outage_write = [&] { expired_at_write.push_back(runtime_ptr->leaseExpiredAtBootMs()); };
      backend->outage = [&] { return boot_ms.load() < outage_until.load(); };
      runtime.startBackgroundWorkers(std::chrono::milliseconds(500));

      second_commit.waitUntilArrived();
      const String prefix = "CAS mount lease of 'test' expired ";
      size_t warnings = 0;
      for (size_t at = log_at_second_commit.find(prefix); at != String::npos; at = log_at_second_commit.find(prefix, at + 1))
          ++warnings;
      EXPECT_EQ(warnings, 1u) << log_at_second_commit;
      ASSERT_GE(expired_at_write.size(), 3u);
      EXPECT_FALSE(expired_at_write.front().has_value()) << "the first attempt runs inside the lease";
      EXPECT_EQ(expired_at_write.back(), std::optional<uint64_t>(anchor + 1000));
      EXPECT_FALSE(runtime.leaseExpiredAtBootMs().has_value()) << "a renewal with lease left ends the expiry";
      second_commit.release();
      runtime.stopBackgroundWorkers();
      runtime.finishTeardown(false);
  }
  ```
  `gtest_cas_lifecycle_snapshot.cpp` (add `#include <chrono>`), at the end:
  ```cpp
  /// An expired lease while the lifecycle stays `Live`: `not_live` / `lease_expired`, `since` is the
  /// deadline as wall time, and a deadline back in the future returns the row to `live`.
  TEST(CASLifecycleSnapshot, ExpiredLeaseIsNotLiveUntilRestored)
  {
      auto storage = openSnapshotStorage();
      commitOnePart(*storage);
      auto pool = storage->store();
      const auto wall_now_s = []
      {
          return static_cast<int64_t>(std::chrono::duration_cast<std::chrono::seconds>(
              std::chrono::system_clock::now().time_since_epoch()).count());
      };

      const int64_t before_s = wall_now_s();
      pool->setMountDeadline(pool->bootMsNow() - 5'000);
      const CasLifecycleSnapshot expired = storage->lifecycleSnapshot();
      const int64_t after_s = wall_now_s();

      EXPECT_EQ(expired.lifecycle, "not_live");
      EXPECT_EQ(expired.reason, "lease_expired");
      EXPECT_GE(static_cast<int64_t>(expired.since), before_s - 6);
      EXPECT_LE(static_cast<int64_t>(expired.since), after_s - 4);
      EXPECT_EQ(pool->lifecycle(), PoolLifecycle::Live) << "the lifecycle enum is not touched";

      pool->setMountDeadline(pool->bootMsNow() + 60'000);
      const CasLifecycleSnapshot restored = storage->lifecycleSnapshot();
      EXPECT_EQ(restored.lifecycle, "live");
      EXPECT_TRUE(restored.reason.empty()) << restored.reason;
      EXPECT_EQ(restored.since, 0);
  }
  ```
  Before running, confirm `ContentAddressedMetadataStorage::store()` is reachable from a test (the
  snapshot file's other tests use it); if it is private, use `Cas::tests` accessor the file already uses.
- [ ] **Step 3. Red.** Build (`build/build_pr08_t5.log`), filter
  `'CASMountRuntime.ALeaseExpiry*:CASLifecycleSnapshot.*'`, log `build/test_pr08_t5_red.log`. Expect
  `warnings` `0` vs `1`, `expired_at_write.back()` `nullopt`, `expired.lifecycle` `"live"` vs `"not_live"`.
  The other `CASLifecycleSnapshot` tests pass (golden).
- [ ] **Step 4. Implementation.**
  - `CasMountRuntime.cpp`:
    ```cpp
    std::optional<uint64_t> CasMountRuntime::leaseExpiredAtBootMs() const
    {
        if (lifecycle() != PoolLifecycle::Live || mount_fence.lost.load(std::memory_order_acquire))
            return std::nullopt;
        const uint64_t deadline = mount_fence.deadline_boot_ms.load(std::memory_order_acquire);
        if (bootMsNow() < deadline)
            return std::nullopt;
        return deadline;
    }

    void CasMountRuntime::warnOnceIfLeaseExpired() noexcept
    {
        try
        {
            const std::optional<uint64_t> expired_at = leaseExpiredAtBootMs();
            if (!expired_at || lease_expiry_warned.exchange(true, std::memory_order_relaxed))
                return;
            /// Written at the first attempt after the expiry, so a renewal stuck in a request reports late; the
            /// age says by how much.
            LOG_WARNING(getLogger("CasPool"),
                "CAS mount lease of '{}' expired {} ms ago under writer_epoch {}; the renewal keeps retrying "
                "and writes are refused until a renewal is confirmed",
                server_root_id, bootMsNow() - *expired_at, liveWriterEpoch());
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
            /// A lost diagnostic must not change how the renewal ends.
        }
    }
    ```
    `publishRenewedDeadline`: compute `deadline` into a local, `setMountDeadline(deadline);`, then
    `if (bootMsNow() < deadline) lease_expiry_warned.store(false, std::memory_order_relaxed);` (Allman).
    `armMountFence`: after the deadline store, `lease_expiry_warned.store(false, std::memory_order_relaxed);`.
  - `CasPool.cpp`, `Pool::lifecycleSnapshot`, before `return snap;`:
    ```cpp
        if (snap.lifecycle == PoolLifecycle::Live)
        {
            if (const std::optional<uint64_t> expired_at = mount_runtime.leaseExpiredAtBootMs())
            {
                snap.lease_expired = true;
                const uint64_t ago_s = (mount_runtime.bootMsNow() - *expired_at) / 1000;
                snap.since = ::time(nullptr) - static_cast<time_t>(ago_s);
            }
        }
    ```
  - `ContentAddressedMetadataStorage.cpp`, `lifecycleSnapshot`:
    ```cpp
        snap.lifecycle = ps.lease_expired ? "not_live" : casLifecycleToString(ps.lifecycle);
        snap.reason = ps.lease_expired ? "lease_expired" : casLifecycleReasonWord(ps.lifecycle);
    ```
    `ContentAddressedMetadataStorage.h`, `CasLifecycleSnapshot` comment: `reason` gains "`lease_expired`
    for a `not_live` row whose pool is `Live` but whose lease expired"; `since` gains "(for
    `lease_expired`, the deadline that passed)".
  - `StorageSystemContentAddressedMounts.cpp`: `lifecycle_reason` description → "The enum-clean sub-state
    word: replaced or forgotten for a vanished disk, lease_expired for a not_live disk whose mount lease
    expired while its renewal keeps retrying. Empty for every other lifecycle (so lifecycle || '(' ||
    lifecycle_reason || ')' reads e.g. vanished(forgotten))."; `lifecycle_since` description gains
    "; for lease_expired, the lease deadline that passed" before " NULL when live".
- [ ] **Step 5. Green.** Filter `'CAS*'`, log `build/test_pr08_t5_green.log`.
- [ ] **Step 6. Commit** the nine files, message `Show and log an expired CAS mount lease as a pause`.

## Task 6: Integration test, docs, S39 expectations

**Files:** `tests/integration/test_cas_mount_renewal_retry/test.py`, the four docs pages under
"Files outside `CA/`".

- [ ] **Step 1. Failing integration test.** In `test.py`, add `"CASMountLeaseLost",` to `RENEWAL_EVENTS`
  after `"CASMountRenewalDeadlineExceeded",`. After `test_landed_response_lost_adopts_exact_mount_write`:
  ```python
  EXPIRY_WARNING = "ms ago under writer_epoch"


  def _lease_state(node, timeout=None):
      row = node.query(
          "SELECT lifecycle, lifecycle_reason, writer_epoch FROM system.cas_mounts "
          "WHERE disk = '{}' AND server_root_id = '{}' LIMIT 1 FORMAT TSV".format(DISK, SERVER_ROOT_ID),
          timeout=timeout,
      ).rstrip("\n")
      assert row, "the local CAS mount row must be visible"
      lifecycle, reason, epoch = row.split("\t")
      return lifecycle, reason, int(epoch)


  def test_long_mount_put_outage_pauses_under_the_same_epoch(start_cluster):
      # A mount PUT outage longer than the lease: the renewal keeps retrying, writes pause while the lease
      # is expired, and the first confirmed renewal resumes them under the same writer_epoch. GC is off on
      # this disk, so nothing fences the slot meanwhile.
      node = start_cluster["node"]
      control_url = start_cluster["control_url"]
      _control(control_url, "/config", {"reset": True})
      _wait_until(lambda deadline: _lease_state(node, timeout=deadline.remaining())[0] == "live", timeout=60)
      epoch_before = _lease_state(node)[2]
      counters_before = _profile_events(node)
      warnings_before = _log_count_since_last_restart(node, EXPIRY_WARNING)
      outage_s = (MOUNT_LEASE_TTL_MS + MOUNT_RENEW_PERIOD_MS) / 1000.0

      _control(
          control_url,
          "/config",
          {"rate": 1.0, "modes": ["503"], "methods": ["PUT"], "path_substring": MOUNT_REQUEST_PATH, "seed": 803},
      )
      armed_at = time.monotonic()
      try:
          _wait_until(
              lambda deadline: _lease_state(node, timeout=deadline.remaining())[:2] == ("not_live", "lease_expired"),
              timeout=30,
          )
          error = node.query_and_get_error("INSERT INTO renewal_probe VALUES (301, 'during-outage')", timeout=60)
          assert error, "a write must be refused while the lease is expired"
          time.sleep(max(0.0, outage_s - (time.monotonic() - armed_at)))
      finally:
          _control(control_url, "/config", {"rate": 0.0})

      _wait_until(lambda deadline: _lease_state(node, timeout=deadline.remaining())[:2] == ("live", ""), timeout=40)
      delta = _event_delta(counters_before, _profile_events(node))
      assert _lease_state(node)[2] == epoch_before
      assert delta["CASRemountAttempts"] == 0, delta
      assert delta["CASMountLeaseLost"] == 0, delta
      assert delta["CASMountRenewalRetries"] > 0, delta
      assert _log_count_since_last_restart(node, EXPIRY_WARNING) - warnings_before == 1

      node.query("INSERT INTO renewal_probe VALUES (302, 'after-outage')")
      assert node.query("SELECT count() FROM renewal_probe WHERE id IN (301, 302)").strip() == "1"
  ```
  Run it on the base binary first (`altinity/antalya-26.6` build, `ninja -C build clickhouse >
  build/build_pr08_t6_base.log 2>&1`, then `python -m ci.praktika run "integration" --test
  test_cas_mount_renewal_retry > build/test_pr08_t6_red.log 2>&1` from the repo root). Expect the new
  test to fail with `condition did not become true within 30s; last=False` (the base remounts; its row
  reads `not_live` with an empty reason).
- [ ] **Step 2. Green.** Rebuild `clickhouse` on the Task 5 tree, rerun the module
  (`build/test_pr08_t6_green.log`): all tests pass; `test_transient_mount_renewal_retries_without_remount`
  is preservation.
- [ ] **Step 3. Docs.**
  - `docs/en/antalya/cas/architecture/mounts-and-leases.md`: replace the bullet starting
    "**Absolute deadline.**" with:
    ```markdown
    - **Deadline from the start.** Renewal uses `CLOCK_BOOTTIME`, not `CLOCK_MONOTONIC`, so a VM resumed
      from suspend correctly observes itself expired. A confirmed renewal sets the deadline to its own
      start plus the lease; a retry, `GET`, response timestamp, or wall-clock step never moves it. The
      background renewal has no deadline of its own: it retries one body every 0.8–1.2 s (measured from
      the start of the previous attempt) until the store answers about the slot or the server stops. A
      refusal (`AccessDenied`, `MalformedXML`, …) sends the same body again, one renewal period later for
      a credential refusal. The startup re-anchor and the remount re-anchor stay bounded by the last
      confirmed lease minus the safety margin.
    ```
    In the paragraph starting "**Losing the lease is neither read-only mode nor a process abort.**",
    replace "Any admitted deterministic failure, confirmed conflict, or ambiguity left at the
    deadline/attempt limit moves it to `RenewalTerminal`" with "A deterministic failure, a confirmed
    conflict, or, for a bounded renewal, ambiguity left at its deadline moves it to `RenewalTerminal`".
    After that paragraph add:
    ```markdown
    **An expired lease is a pause.** While the store does not answer, writes stop being admitted once the
    remaining lease cannot cover them, and the lease expires at its deadline. `system.cas_mounts` then
    shows `lifecycle = 'not_live'` and `lifecycle_reason = 'lease_expired'`, and the server logs one
    `WARNING`: `CAS mount lease of '<server_root_id>' expired <N> ms ago under writer_epoch <E>`. Nothing
    is remounted. The first confirmed renewal whose start plus the lease is still ahead resumes writes
    under the same `writer_epoch`; a renewal confirmed later than that leaves writes paused, and the next
    one starts at once.
    ```
  - `docs/en/antalya/cas/operations/troubleshooting.md`, decision-flow item 2: insert after its title
    "**External lease-safety exhaustion.**" the sentence "Only a bounded renewal ends this way: the
    startup re-anchor or the remount re-anchor. The background renewal keeps retrying; a long outage
    shows as `lifecycle_reason = 'lease_expired'` and resumes under the same `writer_epoch`."
  - `docs/en/antalya/cas/operations/monitoring.md`, row `CASMountRenewalDeadlineExceeded`: counting
    dimension "One per bounded logical renewal (startup or remount re-anchor) stopped by the external
    lease-safety deadline"; interpretation "Growth means a startup or remount renewal found too little
    confirmed lease left; the background renewal never counts here".
  - `docs/en/operations/system-tables/cas_mounts.md`: `lifecycle_reason` → "The enum-clean sub-state
    word: `replaced` or `forgotten` for a `vanished` disk, `lease_expired` for a `not_live` disk whose
    mount lease expired while its renewal keeps retrying. Empty for every other lifecycle, so
    `lifecycle || '(' || lifecycle_reason || ')'` reads e.g. `vanished(forgotten)`."; `lifecycle_since`
    gains "For `lease_expired`, the lease deadline that passed."
- [ ] **Step 4. Commit** `test.py` and the four docs files, message
  `Test and document a long CAS mount PUT outage as a pause`.
- [ ] **Step 5. S39 expectations (not on this branch).** The card
  `utils/ca-soak/scenarios/cards/s39_lease_fault_tolerance.py` lives in the master worktree
  (`/home/mfilimonov/workspace/ClickHouse/master`) and is edited there, by a separate commit, before
  the PR 8 soak acceptance. New leg-B contract for a long mount-PUT outage without a GC fence:
  - outage length `lease + period / 2` (35 s at defaults), below the ≈ 41.5 s point at which a peer's
    GC can fence (spec "Шкала времени");
  - `CASMountLeaseLost` delta `0`, `CASRemountAttempts` delta `0`, `writer_epoch` of the local row
    unchanged, `lifecycle_reason = 'lease_expired'` observed during the outage, `lifecycle = 'live'`
    after the clear, and one expiry `WARNING` in the server log;
  - the post-clear write and liveness checks stay as they are.
  A long leg that a GC fence ends is PR 12/14's contract and is not part of this card change.

## Task 7: Gates, acceptance, PR

- [ ] **Step 1. Size.** `git diff --numstat altinity/antalya-26.6 -- src/Disks/DiskObjectStorage src/Storages ':!src/Disks/tests'`:
  added minus deleted about +200.
- [ ] **Step 2. Gates.** `CAS*` green on `build` and `build_asan` (logs `build/test_pr08_gate.log`,
  `build_asan/test_pr08_gate.log`). On both, `--gtest_list_tests --gtest_filter='CASHeartbeat.*'`
  lists `ARefusedRenewalIsSentAgainWithTheSameTuple` and `ARefusalAfterAnUnclearAttemptIsSettledInsideTheCall`
  (they are under `#if USE_AWS_S3`). No test of this PR expects `LOGICAL_ERROR`.
- [ ] **Step 3. Acceptance (CAS-256).** On the PR binary, run S39 with the Task 6 Step 5 card and the
  sustained-fault repro of CAS-256 (ca-soak with `utils/ca-soak/docker-compose-s3faultproxy.yml`, 503 on
  mount `PUT` for 35 s). Record: no `CASMountLeaseLost`, no remount, and per merge result part at most
  one `MergeParts` row with `error != 0` in `system.part_log` during the outage window. Put the numbers
  in the PR description.
- [ ] **Step 4. PR** (template `.github/PULL_REQUEST_TEMPLATE.md`, base `antalya-26.6`):
  - Description: "A CAS mount-lease renewal no longer gives up at the lease deadline. The background
    renewal retries one body, one `write_attempt_id` and one start every 0.8–1.2 s until the object
    store answers about the slot; a store refusal sends the same body again, and a refused precondition
    whose read failed repeats only the read. An expired lease pauses writes (`lifecycle_reason =
    'lease_expired'`, one `WARNING`) instead of fencing and remounting, and the first renewal confirmed
    with lease left resumes them under the same `writer_epoch`. The renewal's deadline is now published
    under the driver mutex, so a remount cannot have its new deadline overwritten by the old epoch's
    renewal. Startup and remount re-anchors stay bounded. After a credential refusal the next renewal
    call waits one renewal period, so a park request during that pause is seen at most one period
    later. The background renewal of a decommission pool (`SYSTEM CAS DROP POOL MEMBER`) also retries
    until a definitive answer."
  - Mixed versions: nothing on the object store changes. Deploy PR 1 to GC nodes first (see the
    dependency table). A decommission run by a server with this PR keeps renewing the victim's lease
    through a store outage instead of failing at the lease deadline.
  - Changelog category: Bug Fix. Entry: "A CAS disk now pauses writes while the object store is
    unavailable and resumes them under the same `writer_epoch` instead of fencing and remounting after
    a few seconds of failed mount-lease renewals."
  - `Closes: https://github.com/Altinity/ClickHouse/issues/2431`; `Related:
    https://github.com/Altinity/ClickHouse/pull/2474`.
