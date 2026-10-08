# CAS lease PR 9a: `system.cas_mounts` local row without I/O and `lifecycle` from the clock

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:executing-plans (or
> superpowers:subagent-driven-development) to implement this plan task by task. Steps use checkbox
> (`- [ ]`) syntax for tracking.

**Goal.** `system.cas_mounts … WHERE is_local = 1` answers from memory with zero object-store requests,
even while the renewal is stuck; `lifecycle` comes from the deadline and the clock; foreign rows use one
attempt per request; a failed listing is one row with `listing_error`. Closes CAS-159 (listing half).

**Architecture.** An `IStorageSystemOneBlock` filters a candidate block (`disk`, `is_local`; two rows
per CAS disk) with `VirtualColumnUtils::filterBlockWithPredicate` before any I/O. A kept `is_local = 1`
row is built from memory; a kept `is_local = 0` row runs `listMounts` with `Retry::once`, without the
disk's own slot.

**Tech stack.** C++23, gtest, pytest via praktika, stateless shell tests. **Spec.** `docs/superpowers/specs/2026-10-07-cas-mount-lease-series-design.md` r7: section 1 "Состояния",
"Системная таблица"; section 7 rows 9a. **Code base.** `/home/mfilimonov/workspace/ClickHouse/cas-95-1`;
`CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`.

**Depends on.** PR 8 plan (`2026-10-08-cas-lease-pr08-renewal-until-definitive.md`): Task 5's
`leaseExpiredAtBootMs`, `LifecycleSnapshot::lease_expired`, the `not_live`/`lease_expired` mapping,
`_lease_state`; Task 3's `DriverLease::finish(destination, result, renewer)`, `publishRenewedDeadline`.
PR 4 plan: `RenewalProgress`, `Pool::renewalProgress`, `renewal_progress_mutex`, the columns
`renewal_in_progress`, `renewal_attempts`, `last_renewal_error` (kept, same order).

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

**Files outside `CA/`.** `src/Storages/System/StorageSystemContentAddressedMounts.{h,cpp}`: the table is
the operator surface. It moves to `IStorageSystemOneBlock`, the standard way a system table gets a
pushed-down predicate; the constructor signature stays, so `attachSystemTables.cpp` does not change.
Tests: four `tests/queries/0_stateless/` scripts pin `vanished(forgotten)`; `test_cas_mount_renewal_retry`.
Docs: `docs/en/operations/system-tables/cas_mounts.md`, `docs/en/antalya/cas/architecture/mounts-and-leases.md`.
**Budget.** Spec +80; expect about +120 (row emission is one function). Over budget: cut comments.

## Review Focus

- **No I/O on the local path:** `lifecycleSnapshot`, `gcHealth`, `renewalProgress`, `confirmedMountBody`
  take a leaf lock or atomics; `store()` is a pointer snapshot plus a lifecycle check.
- **Which slot is local:** a writable disk's listing skips its own `server_root_id`. A read-only disk
  holds no slot, so its listing keeps every slot, its own `server_root_id` included.
- **Local `state`** classifies the last confirmed body. **`Retry::once`**: one attempt per LIST page and
  per GET, unbounded in time until PR 17b.

## Spec and code disagreements (flagged, not resolved)

1. `fenced_by_gc`, `stopping` (section 1) have no source before PR 12/14; not produced here.
2. Spec shows "—" as `live`'s reason; the local row writes `''` (PR 8's `("live", "")` check holds).
3. No listing-error column in the spec, and `state` stays a body description: the plan adds
   `listing_error Nullable(String)` (CAS-159 offers this or a `state` word).
4. The plan also writes `lifecycle_detail = ''`, `lifecycle_since = NULL` on foreign rows; the spec
   names only `lifecycle`/`lifecycle_reason`. CAS-159 AC #2 (owner-only slot) stays open.

## Task 0: Branch

- [ ] In `cas-95-1`: `git fetch altinity antalya-26.6 && git switch -c cas/lease-pr09a-system-table altinity/antalya-26.6`.
  Each command below prints at least one line, or stop (PR 4 and PR 8 are not merged yet):
  ```bash
  git grep -n 'leaseExpiredAtBootMs' -- src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasMountRuntime.h
  git grep -n 'renewal_in_progress' -- src/Storages/System/StorageSystemContentAddressedMounts.cpp
  git grep -n 'publishRenewedDeadline' -- src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasMountRuntime.cpp
  ```

## Task 1: `lifecycle` from the clock, new vocabulary

**Files:** `CA/Pool/CasMountRuntime.h`, `CA/Pool/CasMountRuntime.cpp`, `CA/Pool/CasPool.h`,
`CA/Pool/CasPool.cpp`, `CA/ContentAddressedMetadataStorage.h`, `CA/ContentAddressedMetadataStorage.cpp`,
`src/Disks/tests/gtest_cas_lifecycle_snapshot.cpp`.
**Produces:** `LeasePause`, `LeasePauseState`, `CasMountRuntime::leasePause`, `conditionalReserveMs`,
`Pool::LifecycleSnapshot::pause` (replaces PR 8's `lease_expired`).

- [ ] **Step 1. Failing tests.** In `gtest_cas_lifecycle_snapshot.cpp`:
  - `VanishedForgottenIsEnumCleanWithTimestampedDetail`: `"vanished"` → `"closed"`, the concatenation
    and the comments → `closed(forgotten)`.
  - PR 8's `ExpiredLeaseIsNotLiveUntilRestored`: rename to `ExpiredLeaseIsPausedUntilRestored`,
    `"not_live"` → `"paused"`, comment `` `not_live` `` → `` `paused` ``.
  - Append:
  ```cpp
  /// Lease left but not more than a conditional change's reserve: `paused` / `renewal_overdue`, `since` is
  /// the instant the reserve was crossed.
  TEST(CASLifecycleSnapshot, RenewalOverdueIsPausedBeforeTheLeaseEnds)
  {
      auto storage = openSnapshotStorage();
      commitOnePart(*storage);
      auto pool = storage->store();
      const auto & budget = pool->poolConfig().cas_request_budget;
      const uint64_t reserve = 2 * budget.attemptEnvelopeMs() + budget.lease_safety_margin_ms;
      const int64_t before_s = ::time(nullptr);
      pool->setMountDeadline(pool->bootMsNow() + reserve - 1'000);
      const CasLifecycleSnapshot overdue = storage->lifecycleSnapshot();
      const int64_t after_s = ::time(nullptr);
      EXPECT_EQ(overdue.lifecycle, "paused");
      EXPECT_EQ(overdue.reason, "renewal_overdue");
      EXPECT_GE(static_cast<int64_t>(overdue.since), before_s - 2);
      EXPECT_LE(static_cast<int64_t>(overdue.since), after_s - 1);
      pool->setMountDeadline(pool->bootMsNow() + reserve + 60'000);
      EXPECT_EQ(storage->lifecycleSnapshot().lifecycle, "live");
  }
  /// Today's lifecycle values in the new vocabulary.
  TEST(CASLifecycleSnapshot, OldLifecycleValuesMapToTheNewVocabulary)
  {
      const auto read = [](PoolLifecycle lc)
      {
          auto storage = openSnapshotStorage();
          storage->store()->setLifecycleForTest(lc);
          const CasLifecycleSnapshot snap = storage->lifecycleSnapshot();
          return snap.lifecycle + "(" + snap.reason + ")";
      };
      EXPECT_EQ(read(PoolLifecycle::TransientNotLive), "paused(remounting)");
      EXPECT_EQ(read(PoolLifecycle::IdentityLost), "identity_lost()");
      EXPECT_EQ(read(PoolLifecycle::VanishedReplaced), "vanished(replaced)");
  }
  ```
- [ ] **Step 2. Red.** `ninja -C build unit_tests_dbms > build/build_pr09a_t1.log 2>&1`; filter
  `'CASLifecycleSnapshot.*'` → `build/test_pr09a_t1_red.log`: `"vanished"`/`"closed"`, `"not_live"`/`"paused"`,
  `"live"`/`"paused"`, `"not_live()"`/`"paused(remounting)"`; the suite's other tests pass (golden).
- [ ] **Step 3. Implementation.**
  - `CasMountRuntime.h`, before `class CasMountRuntime`:
    ```cpp
    /// Why a `Live` mount refuses lease-bound operations, derived from the deadline and the clock:
    /// the lease left is not more than a conditional change's reserve, or the deadline has passed.
    enum class LeasePause : uint8_t { None, RenewalOverdue, LeaseExpired };
    struct LeasePauseState
    {
        LeasePause pause = LeasePause::None;
        uint64_t since_boot_ms = 0;   /// when the threshold was crossed; 0 for `None`
    };
    ```
    Public, after PR 8's `leaseExpiredAtBootMs`:
    ```cpp
        /// I/O-free; `None` unless the pool is `Live` and the fence is not lost.
        LeasePauseState leasePause() const;
        /// Two attempt envelopes and the safety margin: the reserve `refAppendFenceOk` admits against.
        uint64_t conditionalReserveMs() const;
    ```
  - `CasMountRuntime.cpp`, after `refAppendFenceOk`:
    ```cpp
    uint64_t CasMountRuntime::conditionalReserveMs() const
    {
        const uint64_t envelope_ms = cas_request_budget.attemptEnvelopeMs();
        const uint64_t two = envelope_ms > std::numeric_limits<uint64_t>::max() / 2
            ? std::numeric_limits<uint64_t>::max() : 2 * envelope_ms;
        const uint64_t margin = cas_request_budget.lease_safety_margin_ms;
        return two > std::numeric_limits<uint64_t>::max() - margin ? std::numeric_limits<uint64_t>::max() : two + margin;
    }
    LeasePauseState CasMountRuntime::leasePause() const
    {
        if (const std::optional<uint64_t> expired_at = leaseExpiredAtBootMs())
            return {LeasePause::LeaseExpired, *expired_at};
        if (lifecycle() != PoolLifecycle::Live || mount_fence.lost.load(std::memory_order_acquire) || refAppendFenceOk())
            return {};
        const uint64_t deadline = mount_fence.deadline_boot_ms.load(std::memory_order_acquire);
        const uint64_t reserve = conditionalReserveMs();
        return {LeasePause::RenewalOverdue, deadline > reserve ? deadline - reserve : 0};
    }
    ```
  - `CasPool.h`, `LifecycleSnapshot`: PR 8's `bool lease_expired = false;` → `LeasePause pause =
    LeasePause::None;` with `/// Set only for \`Live\`; \`since\` is then when the pause began.`
  - `CasPool.cpp`, `Pool::lifecycleSnapshot`, PR 8's `if (snap.lifecycle == PoolLifecycle::Live)` block →
    ```cpp
        if (snap.lifecycle == PoolLifecycle::Live)
        {
            const LeasePauseState pause = mount_runtime.leasePause();
            snap.pause = pause.pause;
            if (pause.pause != LeasePause::None)
            {
                const uint64_t now_boot_ms = mount_runtime.bootMsNow();
                const uint64_t ago_s = now_boot_ms > pause.since_boot_ms ? (now_boot_ms - pause.since_boot_ms) / 1000 : 0;
                snap.since = ::time(nullptr) - static_cast<time_t>(ago_s);
            }
        }
    ```
  - `ContentAddressedMetadataStorage.cpp`: `casLifecycleToString`, `casLifecycleReasonWord` and comments →
    ```cpp
    /// `lifecycle` and the one-word `lifecycle_reason` of `system.cas_mounts`; the full text is `detail`.
    std::pair<const char *, const char *> casLifecycleWords(const Cas::Pool::LifecycleSnapshot & ps)
    {
        switch (ps.lifecycle)
        {
            case Cas::PoolLifecycle::Live:
                if (ps.pause == Cas::LeasePause::RenewalOverdue)
                    return {"paused", "renewal_overdue"};
                if (ps.pause == Cas::LeasePause::LeaseExpired)
                    return {"paused", "lease_expired"};
                return {"live", ""};
            case Cas::PoolLifecycle::TransientNotLive:  return {"paused", "remounting"};
            case Cas::PoolLifecycle::IdentityLost:      return {"identity_lost", ""};
            case Cas::PoolLifecycle::VanishedReplaced:  return {"vanished", "replaced"};
            case Cas::PoolLifecycle::VanishedForgotten: return {"closed", "forgotten"};
        }
        return {"unknown", ""};
    }
    ```
    PR 8's two assignments in `lifecycleSnapshot` → `std::tie(snap.lifecycle, snap.reason) = casLifecycleWords(ps);` (`<tuple>`).
  - `ContentAddressedMetadataStorage.h`, `CasLifecycleSnapshot` comment: list the words of
    `casLifecycleWords` plus `constructing`/`shutdown`; `since` of a clock pause is when it began.
- [ ] **Step 4. Green** (`'CASLifecycleSnapshot.*:CASMountRuntime.*:CASPool*'`, `build/test_pr09a_t1_green.log`).
  **Commit** the seven files: `Derive the CAS mount lifecycle from the lease clock`.

## Task 2: The last confirmed mount body, readable without I/O

**Files:** `CA/Pool/CasServerRoot.h`, `CA/Pool/CasServerRoot.cpp`, `CA/Pool/CasMountRuntime.h`,
`CA/Pool/CasMountRuntime.cpp`, `CA/Pool/CasPool.h`, `src/Disks/tests/gtest_cas_lifecycle_snapshot.cpp`.
**Produces:** `MountLeaseRenewer::lastConfirmedBody`, `CasMountRuntime::confirmedMountBody`, `Pool::confirmedMountBody`.

- [ ] **Step 1. Stubs.** `CasMountRuntime.h`, public, after PR 4's `renewalProgress`:
  ```cpp
      /// The body this server last confirmed in its slot; empty before the first claim. Takes only
      /// `renewal_progress_mutex`, so a renewal stuck in a request never holds up the reader.
      std::optional<MountLease> confirmedMountBody() const;
  ```
  It returns `std::nullopt`. `CasPool.h`, after `renewalProgress`: `std::optional<MountLease>
  confirmedMountBody() const { return mount_runtime.confirmedMountBody(); }`.
- [ ] **Step 2. Failing test.** `gtest_cas_lifecycle_snapshot.cpp` (add `#include <thread>`), at the end
  (`ManualBarrier` and `CountingBackend` come from `cas_test_helpers.h`, already included):
  ```cpp
  /// The local row's body is the last confirmed one; reading it sends nothing while a renewal is stuck.
  TEST(CASLifecycleSnapshot, ConfirmedBodyIsReadWithoutIoWhileARenewalIsStuck)
  {
      auto backend = std::make_shared<CountingBackend>();
      auto store = DB::Cas::tests::openPoolForTest(backend);
      const auto requests = [&] { return backend->getTotal() + backend->headTotal() + backend->putTotal() + backend->listTotal(); };
      const std::optional<DB::Cas::MountLease> first = store->confirmedMountBody();
      ASSERT_TRUE(first.has_value());
      EXPECT_EQ(first->writer_epoch, store->writerEpoch());
      DB::Cas::tests::ManualBarrier in_put;
      backend->onBeforeWrite(store->layout().mountKey(kSrid), [&] { in_put.arriveAndWait(); });
      std::thread renewal([&] { store->renewWatermarkOnce(); });
      in_put.waitUntilArrived();
      const uint64_t before = requests();
      const std::optional<DB::Cas::MountLease> during = store->confirmedMountBody();
      (void)store->lifecycleSnapshot();
      (void)store->renewalProgress();
      EXPECT_EQ(requests(), before);
      ASSERT_TRUE(during.has_value());
      EXPECT_EQ(during->seq, first->seq) << "a renewal in flight is not confirmed yet";
      in_put.release();
      renewal.join();
      backend->onBeforeWrite(store->layout().mountKey(kSrid), {});
      EXPECT_EQ(store->confirmedMountBody()->seq, first->seq + 1);
  }
  ```
- [ ] **Step 3. Red** (`build/build_pr09a_t2.log`, `'CASLifecycleSnapshot.ConfirmedBody*'` → `build/test_pr09a_t2_red.log`): `first.has_value()` is false. **Step 4. Implementation.**
  - `CasServerRoot.h`, `MountLeaseRenewer`, public after `lastCommittedAttemptStartBootMs`:
    `const std::optional<MountLease> & lastConfirmedBody() const { return last_confirmed_body; }`.
    Private: `MountLease makeBody(uint64_t seq_, uint64_t wall_ms, uint64_t min_active_build_sequence,
    UInt128 write_attempt_id) const;` beside `encodeBody`; member `std::optional<MountLease>
    last_confirmed_body;` after `last_committed_attempt_start_boot_ms`.
  - `CasServerRoot.cpp`: `makeBody` returns the `MountLease` that `encodeBody` builds today;
    `encodeBody` becomes `return encodeMountLease(makeBody(seq_, wall_ms, min_active_build_sequence,
    write_attempt_id));`. In `start` and in PR 8's `renewOn`, the body is built as `const MountLease
    lease_body = makeBody(…)` (same arguments as today's `encodeBody` call) plus `const String body =
    encodeMountLease(lease_body);`, and each `last_committed_attempt_start_boot_ms = attempt_start_boot_ms;`
    on a confirmed write is followed by `last_confirmed_body = lease_body;`.
  - `CasMountRuntime.h`, private, after `RenewalProgress renewal_progress;`: `std::optional<MountLease>
    confirmed_body;` (guarded by `renewal_progress_mutex`) and `void publishConfirmedBody(const
    std::optional<MountLease> & body);`. `CasMountRuntime.cpp`: `confirmedMountBody` and
    `publishConfirmedBody` each take `std::lock_guard lock(renewal_progress_mutex);` and return or assign
    `confirmed_body`. Call `publishConfirmedBody(renewer->lastConfirmedBody());` in `startRenewer` right
    after `renewer->start(…)` returns, and in `DriverLease::finish` inside PR 8's committed-and-same-renewer
    `if`, after `runtime.publishRenewedDeadline(…);` (as `runtime.publishConfirmedBody(…)`).
- [ ] **Step 5. Green** (`'CASLifecycleSnapshot.*:CASMountRuntime.*:CASHeartbeat.*:CASPool*'`,
  `build/test_pr09a_t2_green.log`). **Commit** the six files: `Keep the last confirmed CAS mount body in memory`.

## Task 3: `listMounts` with one attempt and without this disk's slot

**Files:** `CA/Pool/CasServerRoot.h`, `CA/Pool/CasServerRoot.cpp`, `src/Disks/tests/gtest_cas_mount.cpp`.

- [ ] **Step 1. Failing test.** `gtest_cas_mount.cpp` (add `extern const int NETWORK_ERROR;` to its
  `DB::ErrorCodes` block), after `TEST(CASListMounts, NestedSridIsNotTruncated)`:
  ```cpp
  /// The skipped slot is not read, and a failed read ends the call at its first attempt.
  TEST(CASListMounts, SkipsOneSlotAndTriesOnce)
  {
      auto backend = std::make_shared<DB::Cas::tests::CountingBackend>();
      Layout layout("pool");
      const uint64_t now_ms = 1'000'000, ttl_ms = 10'000;
      Ops ops(backend);
      for (const String srid : {"a", "b"})
          ASSERT_EQ(claimMount(ops.op, layout, srid, UInt128{1}, 1, now_ms, ttl_ms).kind, MountClaimResult::Claimed);
      uint64_t gets = backend->getTotal();
      const std::vector<MountInfo> rows = listMounts(ops.op, layout, now_ms, ttl_ms / 2, Retry::once(), "a");
      ASSERT_EQ(rows.size(), 1u);
      EXPECT_EQ(rows[0].srid, "b");
      EXPECT_EQ(backend->getTotal() - gets, 1u) << "the skipped slot must not be read";
      for (int i = 0; i < 2; ++i)
          backend->failNextReadWith(layout.mountKey("b"),
              std::make_exception_ptr(DB::Exception(DB::ErrorCodes::NETWORK_ERROR, "injected transient read")));
      gets = backend->getTotal();
      EXPECT_ANY_THROW(listMounts(ops.op, layout, now_ms, ttl_ms / 2, Retry::once(), "a"));
      EXPECT_EQ(backend->getTotal() - gets, 1u);
  }
  ```
- [ ] **Step 2. Red:** compile error, no 6-argument `listMounts` (`build/build_pr09a_t3.log`).
- [ ] **Step 3. Implementation.** `CasServerRoot.h`, replace the `listMounts` declaration and its comment:
  ```cpp
  /// The `state` word of `system.cas_mounts` for a decoded body: `fenced`, `terminated`, `live` or `expired`.
  String classifyMountBody(const MountLease & lease, uint64_t now_ms, uint64_t skew_margin_ms);
  /// Every mount slot under `gc/server-roots/` except `skip_srid`'s, decoded and classified; zero writes.
  /// One LIST per page and one GET per slot, each under `retry`; a failed request ends the call.
  std::vector<MountInfo> listMounts(CasOperation & op, const Layout & layout, uint64_t now_ms, uint64_t skew_margin_ms,
                                    const Retry & retry = Retry::standard(), std::string_view skip_srid = {});
  ```
  `CasServerRoot.cpp`: `classifyMountBody` holds the four-way `if` that follows the decode today. In
  `listMounts`: compute `info.srid` before the read and `if (info.srid == skip_srid) return true;` (a
  `server_root_id` is never empty, so an empty `skip_srid` skips nothing); `op.read(listed.key, retry)`;
  `forEachListedKey(prefix, …, retry)`; after the decode `info.state = classifyMountBody(info.lease,
  now_ms, skew_margin_ms);`.
- [ ] **Step 4. Green** (`'CASListMounts.*:CASMount*'`, `build/test_pr09a_t3_green.log`; the other
  `listMounts` tests are golden). **Commit** the three files: `List CAS mount slots once and skip a given slot`.

## Task 4: `system.cas_mounts`: candidate block, local row, foreign rows

**Files:** `src/Storages/System/StorageSystemContentAddressedMounts.{h,cpp}`,
`tests/integration/test_cas_mount_renewal_retry/test.py`, and in `tests/queries/0_stateless/`:
`05020_cas_fsck.sh`, `05020_cas_fsck.reference`, `05023_cas_dropns_leaked_namespace.sh`,
`04290_cas_no_leftovers.sh`, `04295_cas_mutation_no_leftovers.sh`.

- [ ] **Step 1. Failing integration tests.** In `test.py` (add `import io`), PR 8's long-outage test
  waits for `("not_live", "lease_expired")` → `("paused", "lease_expired")`. After the last test:
  ```python
  def _query_events(node, query_id, names):
      node.query("SYSTEM FLUSH LOGS")
      return int(node.query(
          "SELECT {} FROM system.query_log WHERE query_id = '{}' AND type = 'QueryFinish'".format(
              " + ".join("ProfileEvents['{}']".format(n) for n in names), query_id)).strip())


  def test_local_row_sends_no_request_and_a_failed_listing_is_one_row(start_cluster):
      # Every request fails: the local row answers from memory; the listing makes one LIST attempt.
      node = start_cluster["node"]
      control_url = start_cluster["control_url"]
      _control(control_url, "/config", {"reset": True})
      _control(control_url, "/config", {"rate": 1.0, "modes": ["503"], "seed": 901})
      try:
          local = node.query(
              "SELECT server_root_id, lifecycle IS NOT NULL, toTypeName(lifecycle) FROM system.cas_mounts "
              "WHERE disk = '{}' AND is_local = 1 FORMAT TSV".format(DISK), query_id="pr09a-local", timeout=10).strip()
          failed = node.query(
              "SELECT server_root_id, state, listing_error != '' FROM system.cas_mounts WHERE disk = '{}' "
              "AND listing_error IS NOT NULL FORMAT TSV".format(DISK), query_id="pr09a-failed", timeout=30).rstrip("\n")
      finally:
          _control(control_url, "/config", {"rate": 0.0})
      assert local == "{}\t1\tNullable(String)".format(SERVER_ROOT_ID), local
      assert failed == "\t\t1", failed
      assert _query_events(node, "pr09a-local", ["S3ListObjects", "S3GetObject", "S3HeadObject"]) == 0
      assert _query_events(node, "pr09a-failed", ["S3ListObjects"]) == 1
      _wait_until(lambda deadline: _lease_state(node, timeout=deadline.remaining())[:2] == ("live", ""), timeout=60)


  def test_foreign_rows_carry_no_local_facts(start_cluster):
      node = start_cluster["node"]
      body, _ = _read_mount_object()
      peer_key = "cas_mount_renewal/gc/server-roots/itest-peer/mount"
      _rustfs_client(10).put_object(cluster.rustfs_bucket, peer_key, io.BytesIO(body), len(body))
      try:
          rows = node.query(
              "SELECT server_root_id, is_local, lifecycle IS NULL, lifecycle_reason IS NULL, is_leader IS NULL, "
              "renewal_in_progress IS NULL, listing_error IS NULL FROM system.cas_mounts WHERE disk = '{}' "
              "ORDER BY is_local FORMAT TSV".format(DISK)).strip().splitlines()
      finally:
          _rustfs_client(10).remove_object(cluster.rustfs_bucket, peer_key)
      # is_leader is NULL on the local row too: GC is off on this disk.
      assert rows == ["itest-peer\t0\t1\t1\t1\t1\t1", "{}\t1\t0\t0\t1\t0\t1".format(SERVER_ROOT_ID)], rows


  def test_a_disk_filter_lists_only_that_disk(start_cluster):
      node = start_cluster["budget_probe"]
      node.query("SELECT count() FROM system.cas_mounts WHERE disk = 'disk_cas_budget_capped'", query_id="pr09a-one")
      node.query("SELECT count() FROM system.cas_mounts", query_id="pr09a-both")
      assert _query_events(node, "pr09a-one", ["S3ListObjects"]) == 1
      assert _query_events(node, "pr09a-both", ["S3ListObjects"]) == 2
  ```
  Build `clickhouse` on the Task 3 tree (`build/build_pr09a_t4_base.log`); from the repo root run
  `python -m ci.praktika run "integration" --test test_cas_mount_renewal_retry > build/test_pr09a_t4_red.log 2>&1`.
  Expect `UNKNOWN_IDENTIFIER` (`is_local`, `listing_error`) in the first two, `assert 2 == 1` in the
  third (today a disk filter still lists every disk), and PR 8's long-outage test timing out on `paused`.
- [ ] **Step 2. Header and columns.** `StorageSystemContentAddressedMounts.h`: include
  `<Storages/System/IStorageSystemOneBlock.h>`, derive from `IStorageSystemOneBlock`; keep the
  `StorageID` constructor and `getName`; add `static ColumnsDescription getColumnsDescription();`,
  protected `void fillData(MutableColumns &, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>)
  const override;`, `Block getFilterSampleBlock() const override;`; drop `createVirtuals`, `read`,
  `isSystemStorage` (the base class has the same `_table`/`_database` virtuals). Class comment:
  ```cpp
  /// system.cas_mounts: per content-addressed disk, this server's mount built from memory (`is_local = 1`,
  /// no object-store request) and every other mount slot listed from the pool (`is_local = 0`, one attempt
  /// per request). A predicate on `disk` and `is_local` runs before any request.
  ```
  In the `.cpp`, `getColumnsDescription` returns today's list with: `lifecycle` `Nullable(String)`,
  "This server's lifecycle for the disk, from the confirmed lease and the clock: live, paused,
  identity_lost, vanished, closed, constructing (never started) or shutdown (torn down). NULL on rows of
  other servers' mounts."; `lifecycle_reason` `Nullable(String)`, "One word: renewal_overdue,
  lease_expired or remounting for paused, replaced for vanished, forgotten for closed, empty otherwise.
  NULL on rows of other servers' mounts."; `lifecycle_detail` adds "Empty on rows of other servers'
  mounts."; `lifecycle_since` adds "NULL on rows of other servers' mounts."; `state` adds "On the local
  row, the state of the body this server last confirmed, not read from the store."; after `last_renewal_error`:
  `{"is_local", std::make_shared<DataTypeUInt8>(), "1 for this server's own mount, built from memory without a request to the object store; 0 for a slot listed from the pool."}`,
  `{"listing_error", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "The error of a failed listing, on the one row it produces. NULL on every other row."}`.
  Constructor: `: IStorageSystemOneBlock(table_id_, getColumnsDescription()) {}`. `getFilterSampleBlock`
  returns `{{{}, std::make_shared<DataTypeString>(), "disk"}, {{}, std::make_shared<DataTypeUInt8>(), "is_local"}}`.
- [ ] **Step 3. Rows.** Replace `read` and `createVirtuals` with (includes: add
  `<Storages/VirtualColumnUtils.h>`, drop `SourceFromSingleChunk.h` and `QueryPipeline/Pipe.h`):
  ```cpp
  namespace
  {
  /// Facts about this server; absent on rows listed from the pool.
  struct LocalFacts
  {
      CasLifecycleSnapshot lifecycle;
      std::optional<Cas::CasGcScheduler::GcHealth> health;
      std::optional<Cas::RenewalProgress> progress;
  };
  /// A disk that is not mounted or is terminal gets a local row and no listing.
  Cas::PoolPtr storeOrNull(ContentAddressedMetadataStorage & ca)
  {
      try
      {
          return ca.store();
      }
      catch (const Exception & e)
      {
          if (e.code() != ErrorCodes::INVALID_STATE)
              throw;
          return nullptr;
      }
  }
  uint64_t skewMarginMs(const Cas::Pool & store) { return static_cast<uint64_t>(store.poolConfig().mount_lease_ttl_ms.count()) / 2; }
  }

  void StorageSystemContentAddressedMounts::fillData(
      MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * predicate, std::vector<UInt8>) const
  {
      const uint64_t now_ms = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch()).count());
      const auto emit = [&](const String & disk, const String & srid, const Cas::MountLease & b, const String & state,
                            const LocalFacts * local, const std::optional<String> & listing_error)
      {
          const auto health = local ? local->health : std::nullopt;
          const auto progress = local ? local->progress : std::nullopt;
          size_t c = 0;
          res_columns[c++]->insert(disk);
          res_columns[c++]->insert(srid);
          assert_cast<ColumnUUID &>(*res_columns[c++]).insertValue(UUID(b.server_uuid));
          res_columns[c++]->insert(b.hostname);
          res_columns[c++]->insert(b.pid);
          res_columns[c++]->insert(b.writer_epoch);
          res_columns[c++]->insert(b.seq);
          assert_cast<ColumnDateTime64 &>(*res_columns[c++]).insertValue(static_cast<Decimal64>(b.started_at_ms));
          assert_cast<ColumnDateTime64 &>(*res_columns[c++]).insertValue(static_cast<Decimal64>(b.expires_at_ms));
          res_columns[c++]->insert(b.min_active_build_sequence);
          res_columns[c++]->insert(static_cast<UInt8>(b.gc_fenced));
          res_columns[c++]->insert(state);
          res_columns[c++]->insert(health ? Field(static_cast<UInt64>(health->is_leader)) : Field());
          res_columns[c++]->insert(health ? Field(health->pending_reclaim) : Field());
          res_columns[c++]->insert(health ? Field(health->last_success_age_seconds) : Field());
          res_columns[c++]->insert(health ? Field(health->wedged_namespace_count) : Field());
          res_columns[c++]->insert(local ? Field(local->lifecycle.lifecycle) : Field());
          res_columns[c++]->insert(local ? Field(local->lifecycle.reason) : Field());
          res_columns[c++]->insert(local ? local->lifecycle.detail : String{});
          res_columns[c++]->insert(local && local->lifecycle.since != 0 ? Field(static_cast<UInt64>(local->lifecycle.since)) : Field());
          res_columns[c++]->insert(progress ? Field(static_cast<UInt64>(progress->in_progress)) : Field());
          res_columns[c++]->insert(progress ? Field(static_cast<UInt64>(progress->attempts_sent)) : Field());
          res_columns[c++]->insert(progress && progress->last_error ? Field(*progress->last_error) : Field());
          res_columns[c++]->insert(static_cast<UInt64>(local != nullptr));
          res_columns[c++]->insert(listing_error ? Field(*listing_error) : Field());
      };
      const auto emitLocal = [&](const String & disk, ContentAddressedMetadataStorage & ca)
      {
          LocalFacts local{.lifecycle = ca.lifecycleSnapshot(), .health = ca.gcHealth(), .progress = {}};
          std::optional<Cas::MountLease> body;
          String state;
          if (const Cas::PoolPtr store = storeOrNull(ca))
          {
              local.progress = store->renewalProgress();
              body = store->confirmedMountBody();
              if (body)
                  state = Cas::classifyMountBody(*body, now_ms, skewMarginMs(*store));
          }
          emit(disk, local.lifecycle.server_root_id, body.value_or(Cas::MountLease{}), state, &local, std::nullopt);
      };
      const auto emitListed = [&](const String & disk, ContentAddressedMetadataStorage & ca)
      {
          const Cas::PoolPtr store = storeOrNull(ca);
          if (!store)
              return;
          /// A read-only disk holds no slot: the slot under its `server_root_id` belongs to another disk.
          const auto & config = store->poolConfig();
          const String own_srid = config.read_only ? String{} : config.server_root_id;
          std::vector<Cas::MountInfo> mounts;
          try
          {
              /// The open fence: these rows matter most when the local mount fence has run down.
              Cas::CasOperation op = store->openRequests().admit();
              mounts = Cas::listMounts(op, store->layout(), now_ms, skewMarginMs(*store), Cas::Retry::once(), own_srid);
          }
          catch (...)
          {
              /// One row, so a failed LIST does not read as an empty pool.
              tryLogCurrentException(getLogger("StorageSystemContentAddressedMounts"), "listing mounts for disk '" + disk + "'");
              emit(disk, "", Cas::MountLease{}, "", nullptr, getCurrentExceptionMessage(false));
              return;
          }
          for (const auto & m : mounts)
              emit(disk, m.srid, m.lease, m.state, nullptr, std::nullopt);
      };
      const auto disks = context->getDisksMap();
      MutableColumnPtr candidate_disk = ColumnString::create();
      MutableColumnPtr candidate_is_local = ColumnUInt8::create();
      for (const auto & [disk_name, disk] : disks)
      {
          if (!ContentAddressedMetadataStorage::tryFromDisk(disk))
              continue;
          for (const UInt8 is_local : {UInt8{1}, UInt8{0}})
          {
              candidate_disk->insert(disk_name);
              candidate_is_local->insert(is_local);
          }
      }
      Block candidates{
          {ColumnPtr(std::move(candidate_disk)), std::make_shared<DataTypeString>(), "disk"},
          {ColumnPtr(std::move(candidate_is_local)), std::make_shared<DataTypeUInt8>(), "is_local"},
      };
      VirtualColumnUtils::filterBlockWithPredicate(predicate, candidates, context);
      const IColumn & kept_disk = *candidates.getByName("disk").column;
      const IColumn & kept_is_local = *candidates.getByName("is_local").column;
      for (size_t i = 0; i < candidates.rows(); ++i)
      {
          const String disk_name = kept_disk[i].safeGet<String>();
          auto & ca = *ContentAddressedMetadataStorage::tryFromDisk(disks.at(disk_name));
          if (kept_is_local.getUInt(i))
              emitLocal(disk_name, ca);
          else
              emitListed(disk_name, ca);
      }
  }
  ```
- [ ] **Step 4. Stateless tests.** Gate: `rg -n 'vanished\(forgotten\)' tests src/Disks/tests`; in every
  file it lists (the five above), `vanished(forgotten)` → `closed(forgotten)`; the gate then prints nothing.
- [ ] **Step 5. Green.** Rebuild `clickhouse`, rerun the module (`build/test_pr09a_t4_green.log`): all
  pass; the transient-retry, landed-response and PR 4 progress tests are preservation (`_mount_snapshot`
  now reads the in-memory row). Run `test_cas_drop_pool_member` (`build/test_pr09a_t4_drop_member.log`;
  peer rows through a writable and a read-only view of one pool) and, through the CAS-default stateless
  job (`clickhouse-praktika-tests` skill), the five scripts above plus `05010_cas_mounts_gc_health`,
  `05012_cas_mounts_typed_columns` (`build/test_pr09a_t4_stateless.log`). Then the `CAS*` gate.
  **Commit** the eight files: `Serve the local system.cas_mounts row without object-store I/O`.

## Task 5: Docs

**Files:** `docs/en/operations/system-tables/cas_mounts.md`, `docs/en/antalya/cas/architecture/mounts-and-leases.md`.

- [ ] `cas_mounts.md`, `## Description`, second paragraph →
  ```markdown
  Per CAS disk the table has one row for this server's own mount (`is_local = 1`), built from memory
  without a request to the object store, and one row per other mount slot listed from the pool
  (`is_local = 0`), one attempt per request. `WHERE is_local = 1` sends no request, so it answers while
  the store is down. A failed listing is one row with `listing_error` set.
  ```
  Columns take the Task 4 descriptions and types; add `is_local`, `listing_error`; `vanished(forgotten)`
  → `closed(forgotten)`. The note becomes `Local-only columns`, names the four `lifecycle*` columns too,
  set on the `is_local = 1` row only. Example query: `WHERE is_local = 1`.
- [ ] `mounts-and-leases.md`, the `lifecycle` row under `## system.cas_mounts {#mounts-table}`: "`lifecycle`
  is `live`, `paused` (`lifecycle_reason` `renewal_overdue`, `lease_expired` or `remounting`),
  `identity_lost`, `vanished` (`replaced`), `closed` (`forgotten`), `constructing` or `shutdown`, from the
  confirmed lease and the clock; `Nullable(String)`, `NULL` on other servers' rows. Releases before this
  one wrote `not_live` for `paused` and `vanished(forgotten)` for `closed(forgotten)`." In PR 8's
  paragraph "**An expired lease is a pause.**": `lifecycle = 'not_live'` → `lifecycle = 'paused'`.
- [ ] Gate: `rg -n "not_live|vanished\(forgotten\)" docs/en` prints only the "Releases before this one"
  sentence. Commit the two files: `Document the local system.cas_mounts row and the lifecycle values`.

## Task 6: Gates, soak cards, PR

- [ ] **Size** (`git diff --numstat altinity/antalya-26.6 -- src/Disks/DiskObjectStorage src/Storages ':!src/Disks/tests'`)
  and **gates**: `CAS*` green on `build` and `build_asan` (`*/test_pr09a_gate.log`). No test expects `LOGICAL_ERROR`.
- [ ] **Soak cards** (master worktree, separate commit, before acceptance), in
  `/home/mfilimonov/workspace/ClickHouse/master/utils/ca-soak/scenarios/cards/`:
  `s39_lease_fault_tolerance.py` `== "not_live"` → `== "paused"` (`saw_not_live` → `saw_paused`);
  `s43_same_uuid_recreation.py` `"vanished" in v` → `v == "closed(forgotten)"` and its two
  `vanished(...)` texts → `closed(forgotten)`. Gate: `rg -n 'not_live|vanished' utils/ca-soak/scenarios/cards`
  shows no `system.cas_mounts` value.
- [ ] **PR** (template `.github/PULL_REQUEST_TEMPLATE.md`, base `antalya-26.6`):
  - Description: the Goal paragraph above, plus: "`closed(forgotten)` replaces `vanished(forgotten)` and
    `paused` replaces `not_live`."
  - Mixed versions: `lifecycle`, `lifecycle_reason` go from `String` to `Nullable(String)` with the new
    words; new columns `is_local`, `listing_error`; a `clusterAllReplicas` query across versions sees
    both. Nothing on the object store changes.
  - Changelog: Improvement, "`system.cas_mounts` serves the local CAS mount row without object-store
    requests and reports `paused` and `closed` with their reasons from the lease clock." `Related: https://github.com/Altinity/ClickHouse/issues/2431`
