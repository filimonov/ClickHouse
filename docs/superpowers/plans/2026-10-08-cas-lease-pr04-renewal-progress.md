# CAS lease PR 4: renewal progress is published while the renewal runs

**Goal.** While a mount-lease renewal is still retrying, `system.cas_mounts` (local row) shows its
conditional `PUT` count and last failed request, and `CASMountRenewalAttempts`/`CASMountRenewalRetries`
grow per attempt. Today `CasMountRuntime::consumeRenewResult` counts only after the renewal ends.

**Architecture.** `CasOperation` gets an optional `RequestObserver`: `writeLoop` calls it per write
attempt and per failed `PUT`, `readLoop` per failed body `GET`. `MountLeaseRenewer::renewOn`
installs the one passed in `MountRenewOperationEnvironment`. `CasMountRuntime` keeps
`RenewalProgress` under a leaf mutex, served by `Pool::renewalProgress` to three new `Nullable`
columns. The thread-local observability stack in `CasServerRoot.cpp` stays (PR 16 removes it).

**Tech stack.** C++23, gtest, `InMemoryBackend` faults and hooks, pytest (`test_cas_mount_renewal_retry`).
**Spec.** `docs/superpowers/specs/2026-10-07-cas-mount-lease-series-design.md`, revision 7: section 2
"Продление" R7 (last sentence), section 1 "Шкала времени" row "10 с", section 7 rows "4".
Backlog: CAS-50, renewal part only. **Independent**; PR 8 and PR 16 depend on it.

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

**Files outside `CA/`.** `src/Storages/System/StorageSystemContentAddressedMounts.cpp`: the table is
the operator surface; three columns appended, no existing column changes. Docs:
`docs/en/operations/system-tables/cas_mounts.md`, `docs/en/antalya/cas/architecture/mounts-and-leases.md`.

**Budget.** ≤ +90 production lines. If the diff exceeds it, cut prose comments first, not tests.

## Review Focus

- **Diagnostics never change an outcome.** The engine calls the observer only after its own rethrow
  guards. `on_failure` in the runtime catches everything; `on_write_attempt` only bumps two counters
  and takes the leaf lock. No retry decision or result field changes.
- **What is reported.** Failed conditional `PUT`s and failed body `GET`s (the resolve read of
  `replace`), only when `isStoreAnswer`. A clean `412` is not a failure. `refused` is exactly
  `isDefinitelyRefusedWrite`; anything else is `transport`.
- **Attempts are conditional `PUT`s**, as in `MountRenewResult::attempts_sent`; a `GET` failure can
  be the last error but is not counted.
- **Counter move.** Totals unchanged on every ending the `CASObservability` tests pin. Accepted
  difference: a renewal ended by a deterministic local failure now counts the attempt it sent.
- **Until PR 9a** the local row waits for `listMounts`; in a full outage the columns appear on the
  synthesized row once the listing fails.

`CA/` below = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`.

## Task 0: Branch

- [ ] In `cas-95-1`: `git fetch altinity antalya-26.6 && git switch -c cas/lease-pr04-renewal-progress altinity/antalya-26.6`.

## Task 1: Request observer in the engine

**Files:** `CA/Backend/CasRequests.h`, `CA/Backend/CasRequests.cpp`, `src/Disks/tests/gtest_cas_request_observer.cpp`
(new). **Produces:** `RequestObserver`, `CasOperation::observeRequests`, `isStoreAnswer`, `describeRequestFailure`.

- [ ] **Step 1. Declarations and formatter (no calls yet).** In `CA/Backend/CasRequests.h`, after
  `bool isDeterministicLocalFailure(int code);` add:
  ```cpp
  /// An exception the store or the transport raised, as opposed to a local fault.
  bool isStoreAnswer(const std::exception & e);
  /// `<VERB> <key>: <refused|transport> <code> <message>`; `code` is the store's error name
  /// (`SlowDown`) when it sent one, else the error code or exception name.
  String describeRequestFailure(std::string_view verb, const String & key, const std::exception & e);
  ```
  After `using ListedKeyFn = …;` add:
  ```cpp
  /// Live progress of one operation, for a caller that publishes it while the operation runs. Called
  /// on the operation's thread from inside the engine's retry loops; neither may throw.
  struct RequestObserver
  {
      /// A conditional write is about to send its `attempts_sent`-th physical attempt.
      std::function<void(uint32_t attempts_sent)> on_write_attempt;
      /// A conditional `PUT` or a body `GET` failed with a store answer (`isStoreAnswer`).
      std::function<void(std::string_view verb, const String & key, const std::exception & e)> on_failure;
  };
  ```
  In `class CasOperation`, public, after `bool admitted() const …;`:
  `void observeRequests(RequestObserver observer_) { observer = std::move(observer_); }`. Private, after `std::optional<ReadStop> last_read_stop;`: `RequestObserver observer;`.

  In `CA/Backend/CasRequests.cpp`, add `#include <Common/ErrorCodes.h>`, and after the definition of
  `isFirstAttemptFuseTimeout`:
  ```cpp
  bool isStoreAnswer(const std::exception & e)
  {
      const auto * db = dynamic_cast<const Exception *>(&e);
      return dynamic_cast<const Poco::Exception *>(&e) && !(db && isDeterministicLocalFailure(db->code()));
  }

  String describeRequestFailure(std::string_view verb, const String & key, const std::exception & e)
  {
      const auto & poco = dynamic_cast<const Poco::Exception &>(e);
      String code;
  #if USE_AWS_S3
      if (const auto * s3 = dynamic_cast<const S3Exception *>(&e))
          code = s3->getExceptionName();
  #endif
      if (const auto * db = dynamic_cast<const Exception *>(&e); code.empty() && db)
          code = String(ErrorCodes::getName(db->code()));
      if (code.empty())
          code = poco.name();
      return fmt::format("{} {}: {} {} {}", verb, key, isDefinitelyRefusedWrite(e) ? "refused" : "transport", code, poco.message());
  }
  ```

- [ ] **Step 2. Failing tests.** Create `src/Disks/tests/gtest_cas_request_observer.cpp` (the
  `refused` class is covered by `CASMountRenewalProgress.AStoreRefusalIsClassedRefused`):
  ```cpp
  /// Includes: <gtest/gtest.h>, CA/Backend/{CasFence,CasInMemoryBackend,CasRequests}.h,
  /// <Common/Exception.h>, <Poco/Exception.h>, <fmt/format.h>, <memory>, <vector>; extern
  /// `DB::ErrorCodes::BAD_ARGUMENTS` and `NETWORK_ERROR`; `using namespace DB::Cas;`.

  namespace
  {

  /// One seeded key and an operation whose observer records every call as a line.
  struct Fixture
  {
      std::shared_ptr<InMemoryBackend> backend = std::make_shared<InMemoryBackend>();
      CasRequests requests{backend, Fence::open(), [] { return uint64_t{100}; }, [](uint64_t) {}};
      Etag etag = std::get<Committed>(requests.admit().create("k", "v1", Retry::standard())).etag;
      std::vector<String> events;
      CasOperation op = requests.admit();

      Fixture()
      {
          op.observeRequests(RequestObserver{
              .on_write_attempt = [this](uint32_t attempts_sent) { events.push_back(fmt::format("attempt {}", attempts_sent)); },
              .on_failure = [this](std::string_view verb, const String & key, const std::exception & e)
              {
                  events.push_back(describeRequestFailure(verb, key, e));
              },
          });
      }
  };

  }

  TEST(CASRequestObserver, WriteAttemptsAndFailuresAreReportedAsTheyHappen)
  {
      Fixture f;
      f.backend->failNextWriteWith("k", std::make_exception_ptr(Poco::TimeoutException("injected write timeout")));
      f.backend->failNextReadWith("k", std::make_exception_ptr(DB::Exception(DB::ErrorCodes::NETWORK_ERROR, "injected read failure")));

      ASSERT_TRUE(std::holds_alternative<Committed>(f.op.replace("k", "v2", f.etag, Retry::standard())));
      EXPECT_EQ(f.events, (std::vector<String>{
          "attempt 1",
          "PUT k: transport Timeout injected write timeout",
          "GET k: transport NETWORK_ERROR injected read failure",
          "attempt 2",
      }));
  }

  /// A local bug is not an answer from the store, so it is not published as the request's error.
  TEST(CASRequestObserver, ALocalFailureIsNotReportedAsAStoreAnswer)
  {
      Fixture f;
      f.backend->failNextWriteWith("k", std::make_exception_ptr(DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "injected local failure")));

      EXPECT_THROW((void)f.op.replace("k", "v2", f.etag, Retry::standard()), DB::Exception);
      EXPECT_EQ(f.events, (std::vector<String>{"attempt 1"}));
  }
  ```

- [ ] **Step 3. Run red.** `ninja -C build unit_tests_dbms > build/build_pr04_t1.log 2>&1`, then
  `build/src/unit_tests_dbms --gtest_filter='CASRequestObserver.*' > build/test_pr04_t1_red.log 2>&1`.
  Both fail on `f.events`, `Which is: {}`; paste the lines into the commit body.

- [ ] **Step 4. Calls.** In `CasOperation::writeLoop`:
  ```cpp
          ++state.attempts_sent;
          state.sent_any = true;
          if (observer.on_write_attempt)
              observer.on_write_attempt(state.attempts_sent);
  ```
  In `catch (const Exception & e)`, after `if (isDeterministicLocalFailure(e.code())) throw;`, and in
  `catch (const std::exception & e)`, after `if (!dynamic_cast<const Poco::Exception *>(&e)) throw;`:
  ```cpp
              if (observer.on_failure)
                  observer.on_failure("PUT", key, e);
  ```
  In `CA/Backend/CasRequests.h`, `readLoop`, first statement of `catch (const std::exception & e)`:
  ```cpp
              if (observer.on_failure && verb == "read" && isStoreAnswer(e))
                  observer.on_failure("GET", subject, e);
  ```

- [ ] **Step 5. Green.** Same build, filter `'CASRequestObserver.*:CASRequests*:CASHotKeys*'`, log
  `build/test_pr04_t1_green.log`. All pass; `CASRequests*` and `CASHotKeys*` are preservation.

- [ ] **Step 6. Commit** the three files of this task (full paths in `git add --`), message
  `Report CAS write attempts and request failures to an observer`, trailer
  `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>` (same trailer on every commit).

## Task 2: Renewal progress in `CasMountRuntime`, counters per attempt

**Files:** `CA/Pool/{CasServerRoot.h,CasServerRoot.cpp,CasMountRuntime.h,CasMountRuntime.cpp,CasPool.h}`,
`src/Disks/tests/gtest_cas_mount_renewal_progress.cpp` (new). **Consumes:** Task 1. **Produces:** `RenewalProgress`, `CasMountRuntime::renewalProgress`,
`Pool::renewalProgress`, `MountRenewOperationEnvironment::requests`.

- [ ] **Step 1. Declarations (stub).** In `CA/Pool/CasMountRuntime.h`, before `class CasMountRuntime`:
  ```cpp
  /// The current logical renewal of the mount lease so far, or the last one once it ended.
  struct RenewalProgress
  {
      bool in_progress = false;
      uint32_t attempts_sent = 0;
      std::optional<String> last_error;
  };
  ```
  Public, after `void renewWatermarkOnce();`: `RenewalProgress renewalProgress() const;` with the
  comment `/// Takes only its own lock, so a renewal stuck in a request never holds up the reader.`
  In `CasMountRuntime.cpp`: `RenewalProgress CasMountRuntime::renewalProgress() const { return {}; }`
  (Allman layout). In `CA/Pool/CasPool.h`, after `LifecycleSnapshot lifecycleSnapshot() const;`:
  `RenewalProgress renewalProgress() const { return mount_runtime.renewalProgress(); }`.

- [ ] **Step 2. Failing tests.** Create `src/Disks/tests/gtest_cas_mount_renewal_progress.cpp` (a
  failed resolve `GET` as the last error is covered by `CASRequestObserver`):
  ```cpp
  /// Includes: <gtest/gtest.h>, CA/Backend/CasInMemoryBackend.h, CA/Pool/CasPool.h,
  /// <Common/Exception.h>, <Common/ProfileEvents.h>, <Poco/Exception.h>, "config.h", and
  /// <IO/S3Common.h> under `#if USE_AWS_S3`; extern `ProfileEvents::CASMountRenewalAttempts`;
  /// `using namespace DB::Cas;`.

  namespace
  {

  uint64_t attemptsCounter()
  {
      return ProfileEvents::global_counters[ProfileEvents::CASMountRenewalAttempts].load();
  }

  /// Fixed boot clock and no-op sleep: every armed fault is finite, so retries end without time passing.
  struct Fixture
  {
      std::shared_ptr<InMemoryBackend> backend = std::make_shared<InMemoryBackend>();
      PoolPtr store;
      String mount_key;

      explicit Fixture(const String & prefix)
      {
          backend->setAttemptTimeoutMs(10);
          store = Pool::open(backend, PoolConfig{
              .pool_prefix = prefix,
              .server_root_id = "test",
              .mount_lease_ttl_ms = std::chrono::milliseconds(1000),
              .cas_request_budget = CasRequestBudget{.attempt_timeout_ms = 10, .lease_safety_margin_ms = 20, .connect_timeout_cap_ms = std::nullopt},
              .boot_ms_fn = [] { return uint64_t{100}; },
              .retry_sleep_fn = [](uint64_t) {},
          });
          mount_key = store->layout().mountKey("test");
      }
  };

  }

  TEST(CASMountRenewalProgress, AttemptsAndLastErrorAreVisibleWhileTheRenewalRuns)
  {
      Fixture f("renewal-progress-in-flight");
      const auto timeout = std::make_exception_ptr(Poco::TimeoutException("injected renewal timeout"));
      f.backend->failNextWriteWith(f.mount_key, timeout);
      f.backend->failNextWriteWith(f.mount_key, timeout);
      const uint64_t attempts_before = attemptsCounter();
      /// Read at the third attempt, the first that reaches the store. Heap state and a raw pointer: the
      /// hook must not keep the pool alive, and it is removed before the pool's farewell write.
      auto in_flight = std::make_shared<std::optional<std::pair<RenewalProgress, uint64_t>>>();
      Pool * pool = f.store.get();
      f.backend->onBeforeWrite(f.mount_key, [in_flight, pool]
      {
          if (!*in_flight)
              *in_flight = std::make_pair(pool->renewalProgress(), attemptsCounter());
      });

      f.store->renewWatermarkOnce();
      f.backend->onBeforeWrite(f.mount_key, {});

      ASSERT_TRUE(in_flight->has_value());
      const auto & [seen, attempts_counted] = **in_flight;
      EXPECT_TRUE(seen.in_progress);
      EXPECT_EQ(seen.attempts_sent, 3u);
      EXPECT_EQ(seen.last_error, "PUT " + f.mount_key + ": transport Timeout injected renewal timeout");
      EXPECT_EQ(attempts_counted - attempts_before, 3u) << "attempts are counted as they are sent";

      const RenewalProgress ended = f.store->renewalProgress();
      EXPECT_FALSE(ended.in_progress);
      EXPECT_EQ(ended.attempts_sent, 3u);
      EXPECT_EQ(ended.last_error, seen.last_error) << "an ended renewal keeps its progress";

      f.store->renewWatermarkOnce();
      const RenewalProgress next = f.store->renewalProgress();
      EXPECT_EQ(next.attempts_sent, 1u);
      EXPECT_FALSE(next.last_error.has_value()) << "the next renewal starts clean";
  }

  #if USE_AWS_S3
  TEST(CASMountRenewalProgress, AStoreRefusalIsClassedRefused)
  {
      Fixture f("renewal-progress-refused");
      f.backend->failNextWriteWith(f.mount_key, std::make_exception_ptr(
          DB::S3Exception("the store refused the renewal", Aws::S3::S3Errors::ACCESS_DENIED, "AccessDenied")));

      EXPECT_THROW(f.store->renewWatermarkOnce(), DB::Exception);

      const RenewalProgress ended = f.store->renewalProgress();
      EXPECT_FALSE(ended.in_progress) << "a renewal that threw has still ended";
      EXPECT_EQ(ended.attempts_sent, 1u);
      EXPECT_EQ(ended.last_error, "PUT " + f.mount_key + ": refused AccessDenied the store refused the renewal");
  }
  #endif
  ```
  Before running, confirm in `CasMountRuntime::installRenewer` that the renewer's key is
  `layout().mountKey(server_root_id)`; if not, use the key it writes.

- [ ] **Step 3. Run red.** Build as in Task 1 (log `build/build_pr04_t2.log`), filter
  `'CASMountRenewalProgress.*'`, log `build/test_pr04_t2_red.log`. Expect both to fail: `seen.in_progress` is `false`, `attempts_sent` is `0`, `last_error` is
  `nullopt`, and `attempts_counted - attempts_before` is `0`, not `3` (the counter moves only at
  the end on the base).

- [ ] **Step 4. Implementation.**
  - `CA/Pool/CasServerRoot.h`, `MountRenewOperationEnvironment`, after `cancelled`:
    `RequestObserver requests;` with the comment `/// Live progress of the renewal's requests; empty publishes nothing.`
  - `CA/Pool/CasServerRoot.cpp`, `MountLeaseRenewer::renewOn`, after
    `CasOperation op = plane.admit(environment.live);`: `op.observeRequests(environment.requests);`.
  - `CA/Pool/CasMountRuntime.h`, private, after `driver_mutex` and its members:
    `mutable std::mutex renewal_progress_mutex;` (comment `/// A leaf lock, never held across I/O.`)
    and `RenewalProgress renewal_progress;`.
  - `CA/Pool/CasMountRuntime.cpp`: add `#include <base/scope_guard.h>`; the stub body becomes
    `std::lock_guard lock(renewal_progress_mutex); return renewal_progress;`. In
    `renewalEnvironment`, after `.cancelled = …`:
    ```cpp
            .requests = RequestObserver{
                .on_write_attempt = [this](uint32_t attempts_sent)
                {
                    /// Counted as sent, so a renewal stuck in retries shows in `system.events` before it ends.
                    ProfileEvents::incrementNoTrace(ProfileEvents::CASMountRenewalAttempts);
                    if (attempts_sent > 1)
                        ProfileEvents::incrementNoTrace(ProfileEvents::CASMountRenewalRetries);
                    std::lock_guard lock(renewal_progress_mutex);
                    renewal_progress.attempts_sent = attempts_sent;
                },
                .on_failure = [this](std::string_view verb, const String & key, const std::exception & e)
                {
                    /// Runs inside the engine's catch; a formatting failure must not change how the renewal ends.
                    try
                    {
                        String error = describeRequestFailure(verb, key, e);
                        std::lock_guard lock(renewal_progress_mutex);
                        renewal_progress.last_error = std::move(error);
                    }
                    catch (...)
                    {
                    }
                },
            },
    ```
    In `renewRenewerOnce`, replace `const MountRenewResult result = …;` (keep its comment) with:
    ```cpp
        {
            std::lock_guard lock(renewal_progress_mutex);
            renewal_progress = RenewalProgress{.in_progress = true};
        }
        MountRenewResult result;
        {
            SCOPE_EXIT({
                std::lock_guard lock(renewal_progress_mutex);
                renewal_progress.in_progress = false;
            });
            result = active == RenewalDriverState::RemountCall
                ? call.renewer->renewForRemount(renewalEnvironment(worker_call))
                : call.renewer->renew(renewalEnvironment(worker_call));
        }
    ```
    In `consumeRenewResult`, delete the `if (result.attempts_sent > 0)` block with the two
    increments and the two comment lines above it starting `/// The physical counters come off the
    result`. The `Resolved`/`Recovered`/`DeadlineExceeded` increments stay.

- [ ] **Step 5. Green.** Filter `'CASMountRenewalProgress.*:CASObservability.*:CASEvent.*:CASMount*'`,
  log `build/test_pr04_t2_green.log`. All pass. `CASObservability.RenewalCountersHaveExactPhysicalAndLogicalDeltas` and
  `CASObservability.ExternalLeaseDeadlineCountsOnceWithoutReconstructingAttempts` are golden for the
  counter move (both counters): green before and after, expectations unchanged.

- [ ] **Step 6. Commit** the six files of this task (full paths in `git add --`), message
  `Publish CAS mount renewal attempts and last error while it runs` plus the trailer.

## Task 3: `system.cas_mounts` columns, docs and the integration test

**Files:** `src/Storages/System/StorageSystemContentAddressedMounts.cpp`, `tests/integration/test_cas_mount_renewal_retry/test.py`,
and the two docs pages named under "Files outside `CA/`".

- [ ] **Step 1. Failing integration test.** In `test.py`, after
  `test_transient_mount_renewal_retries_without_remount`:
  ```python
  def test_renewal_progress_is_visible_while_the_renewal_retries(start_cluster):
      node = start_cluster["node"]
      control_url = start_cluster["control_url"]
      _control(control_url, "/config", {"reset": True})
      # Unlimited 503s on the mount PUT only: LIST and GET pass, so the table's own listing answers.
      _control(
          control_url,
          "/config",
          {"rate": 1.0, "modes": ["503"], "methods": ["PUT"], "path_substring": MOUNT_REQUEST_PATH, "seed": 802},
      )
      query = (
          "SELECT renewal_in_progress, renewal_attempts, last_renewal_error, toTypeName(renewal_in_progress), "
          "toTypeName(renewal_attempts), toTypeName(last_renewal_error) FROM system.cas_mounts "
          "WHERE disk = '{}' AND server_root_id = '{}' AND renewal_in_progress = 1 AND renewal_attempts >= 2 "
          "LIMIT 1 FORMAT TSV".format(DISK, SERVER_ROOT_ID)
      )
      try:
          row = _wait_until(lambda deadline: node.query(query, timeout=deadline.remaining()).strip(), timeout=20)
      finally:
          _control(control_url, "/config", {"rate": 0.0})

      _, _, error, *types = row.split("\t")
      assert types == ["Nullable(UInt8)", "Nullable(UInt64)", "Nullable(String)"], row
      assert error.startswith("PUT "), row
      assert "gc/server-roots/{}/mount: transport SlowDown ".format(SERVER_ROOT_ID) in error, row

      # Recovered or remounted, the disk is live again once the store answers; later tests need that.
      _wait_until(
          lambda deadline: _mount_snapshot(node, timeout=deadline.remaining())["lifecycle"] == "live",
          timeout=60,
      )
  ```
  Build `clickhouse` on the Task 2 tree (`ninja -C build clickhouse > build/build_pr04_t3.log 2>&1`)
  and run `python -m ci.praktika run "integration" --test test_cas_mount_renewal_retry >
  build/test_pr04_t3_red.log 2>&1`. Expect the new test to fail with `UNKNOWN_IDENTIFIER` for
  `renewal_in_progress`; the other tests pass.

- [ ] **Step 2. Columns.** In `StorageSystemContentAddressedMounts.cpp`, append after `lifecycle_since`:
  ```cpp
          {"renewal_in_progress", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>()), "1 while this server renews its mount lease. NULL on rows describing other servers' mounts."},
          {"renewal_attempts", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "Conditional PUTs sent by the current lease renewal, or by the last one once it ended. NULL on rows describing other servers' mounts."},
          {"last_renewal_error", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "The last failed request of that renewal: '<VERB> <key>: <refused|transport> <code> <message>'. NULL when none failed, and on rows describing other servers' mounts."},
  ```
  In `read`, after `col_lifecycle_since`, create the three columns as
  `ColumnNullable::create(ColumnUInt8::create() / ColumnUInt64::create() / ColumnString::create(), ColumnUInt8::create())`
  named `col_renewal_in_progress`, `col_renewal_attempts`, `col_last_renewal_error`. After the
  `appendLifecycle` lambda:
  ```cpp
      /// Process-local like GC health: NULL on peer rows.
      const auto appendRenewalProgress = [&](const std::optional<Cas::RenewalProgress> & progress)
      {
          col_renewal_in_progress->insert(progress ? Field(static_cast<UInt64>(progress->in_progress)) : Field());
          col_renewal_attempts->insert(progress ? Field(static_cast<UInt64>(progress->attempts_sent)) : Field());
          col_last_renewal_error->insert(progress && progress->last_error ? Field(*progress->last_error) : Field());
      };
  ```
  Declare `std::optional<Cas::RenewalProgress> progress;` before `if (store)`; the first statement
  inside it is `progress = store->renewalProgress();`. Per-mount loop, after `appendLifecycle(snap);`:
  `appendRenewalProgress(is_local_row ? progress : std::nullopt);`. In the `if (!emitted_row)` arm,
  after `appendLifecycle(snap);`: `appendRenewalProgress(progress);`. Append the three columns to
  `res_columns` after `col_lifecycle_since`, in this order.

- [ ] **Step 3. Docs.** `docs/en/operations/system-tables/cas_mounts.md`, after `lifecycle_since`:
  ```markdown
  - `renewal_in_progress` ([Nullable(UInt8)](/sql-reference/data-types/nullable)) — `1` while this server renews its mount lease.
  - `renewal_attempts` ([Nullable(UInt64)](/sql-reference/data-types/nullable)) — Conditional `PUT`s sent by the current renewal, or by the last one once it ended. Updated per attempt, so a renewal stuck in retries is visible before it ends.
  - `last_renewal_error` ([Nullable(String)](/sql-reference/data-types/nullable)) — The last failed request of that renewal, as `<VERB> <key>: <class> <code> <message>`. `class` is `refused` when the store definitely refused the request (access denied, malformed request, entity too large), `transport` otherwise; `code` is the store's error name, such as `SlowDown`. `NULL` when no request failed.
  ```
  Rename the `:::note Local-only GC-health columns` title to `Local-only columns` and start it with:
  "`is_leader`, `pending_reclaim`, `last_success_age_seconds`, `wedged_namespace_count`,
  `renewal_in_progress`, `renewal_attempts` and `last_renewal_error` are process-local facts about
  *this* server." In `docs/en/antalya/cas/architecture/mounts-and-leases.md`, table under
  `## system.cas_mounts {#mounts-table}`, after the `lifecycle` row:
  ```markdown
  | `renewal_in_progress`, `renewal_attempts`, `last_renewal_error` | the running (or last) lease renewal: conditional `PUT`s sent so far and the last failed request, updated per attempt; **`NULL` on every peer row** |
  ```

- [ ] **Step 4. Green.** Rebuild, rerun the module (`build/test_pr04_t3_green.log`): all pass;
  `test_transient_mount_renewal_retries_without_remount` is preservation for the counter move.

- [ ] **Step 5. Commit** the four files of this task (full paths in `git add --`), message
  `Show CAS mount renewal progress in system.cas_mounts` plus the trailer.

## Task 4: Gates and PR

- [ ] **Step 1. Size.** `git diff --numstat altinity/antalya-26.6 -- src/Disks/DiskObjectStorage src/Storages ':!src/Disks/tests'`:
  added minus deleted ≤ 90. Over budget: cut comments before code.
- [ ] **Step 2. Gates.** `CAS*` green on `build` and `build_asan` (logs `*/test_pr04_gate.log`); on
  both, `--gtest_list_tests` shows `CASMountRenewalProgress.AStoreRefusalIsClassedRefused`.
- [ ] **Step 3. PR body** (template `.github/PULL_REQUEST_TEMPLATE.md`, base `antalya-26.6`):
  - Description: "A failing CAS mount-lease renewal now shows its progress while it runs:
    `system.cas_mounts` gets local-row columns `renewal_in_progress`, `renewal_attempts`,
    `last_renewal_error`, and `CASMountRenewalAttempts`/`CASMountRenewalRetries` grow per attempt.
    The request engine reports attempts and failed `PUT`/`GET` requests to an optional observer;
    only the renewal installs one. A renewal ended by a deterministic local failure (e.g.
    `BAD_ARGUMENTS` from the transport) now counts the one attempt it sent; it counted none before."
  - Mixed versions: new columns only; nothing on the object store changes.
  - Changelog category: Improvement. Entry: "`system.cas_mounts` shows the running CAS mount-lease
    renewal's attempt count and last request error while the renewal is still retrying."
  - Related: CAS-50 (renewal part only).
