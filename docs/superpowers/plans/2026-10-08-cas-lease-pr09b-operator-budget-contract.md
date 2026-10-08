# CAS lease PR 9b: one budget snapshot for admission, the mount line and every operator text

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:executing-plans (or
> superpowers:subagent-driven-development) to implement this plan task by task. Steps use checkbox
> (`- [ ]`) syntax for tracking.

**Goal.** What the operator reads is what admission enforces: one `CasLeaseThresholds` per mount drives
admission, the mount line (14 s / 21 s / 30 s at defaults), refusal texts and the lifecycle; each pause,
expiry and resumption is one log line; `cas_mount_lease_ms` replaces `cas_mount_lease_ttl_ms` (a synonym).
Closes CAS-150, CAS-3, CAS-156.3, CAS-67, CAS-1.4, CAS-297 (lease rows), CAS-162 (part).
**Architecture.** `CasMountRuntime` holds the one `CasLeaseThresholds`; admission, `leasePause`, the
refusal text and the transition line read it, and `Pool::open` logs the mount line from it. The engine
asks the fence to explain `NoBudget` (`Fence::describe_no_budget`) and carries the text to the caller.
**Tech stack.** C++23, gtest, pytest via praktika. **Spec.** `docs/superpowers/specs/2026-10-07-cas-mount-lease-series-design.md`
r7: section 1 "Состояния", "Шкала времени", "Настройки"; section 6; section 7 rows 9b. **Code base.**
`cas-95-1`; `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`. **Depends on**
(plans in this directory): 9a (`LeasePause`, `leasePause`, `conditionalReserveMs`); PR 8 Task 5
(`warnOnceIfLeaseExpired`, `lease_expiry_warned`, `ALeaseExpiryIsLoggedOnceAndReported`, `EXPIRY_WARNING`);
PR 4 (`RenewalProgress`, `renewalEnvironment`'s `on_failure`, `describeRequestFailure`).

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

**Files outside `CA/`.** No production code; tests in `src/Disks/tests/` and `test_cas_mount_renewal_retry`;
docs in `docs/en/antalya/cas/` (Task 6). **Budget.** Spec +100; over budget, cut comments first.

## Review Focus

- **One instance, same rule:** `CasMountRuntime.cpp` reads no `attemptEnvelopeMs()`/`lease_safety_margin_ms`
  after Task 1; the admission inequality does not change. **Refusal path:** `describe_no_budget` runs only
  after `NoBudget`, reads atomics and a leaf lock. **Log lines:** renewing thread only, never throw.

## Spec and code disagreements (flagged, not resolved)

1. For a `Refused` error the spec replaces "the last sentence"; the plan replaces both "Renewal keeps
   retrying; …" and "Last renewal error: …". The single-request and lease-ended texts are derived.
2. Spec's last error `PUT …/mount: HTTP 503 Slow Down`; PR 4 writes `PUT <key>: transport SlowDown <message>`.
3. Not done: CAS-150's remount-begin/complete lines (remount goes in PR 14/15); CAS-67 AC #1 (a reload line
   in shared `DiskSelector.cpp`). CAS-3's `ERROR` also covers an operator's own `SYSTEM CAS FORGET`.

## Task 1: `CasLeaseThresholds` and the mount line

**Files:** `CA/Backend/CasRequestBudget.{h,cpp}`, `CA/Pool/CasMountRuntime.{h,cpp}`, `CA/Pool/CasPool.{h,cpp}`,
`CA/Pool/CasServerRoot.cpp`, `CA/ContentAddressedMetadataStorage.cpp`, `src/Disks/tests/gtest_cas_lease_thresholds.cpp`
(new), `tests/integration/test_cas_mount_renewal_retry/test.py`.

- [ ] **Step 0.** `git fetch altinity antalya-26.6 && git switch -c cas/lease-pr09b-budget-contract altinity/antalya-26.6`
  in `cas-95-1`; `git grep -n 'leasePause' -- '*CasMountRuntime.h'` prints a line, or stop (9a not merged).
- [ ] **Step 1. Stubs.** `CasRequestBudget.h` (add `#include <base/types.h>`), after `CasRequestBudget`:
  ```cpp
  /// The thresholds of one writable mount, computed once from its configuration. Admission, the mount line,
  /// `system.cas_mounts` and the refusal texts read this one value, so what is printed is what is enforced.
  /// Admission refuses an operation when the confirmed lease left is not more than its reserve.
  struct CasLeaseThresholds
  {
      uint64_t lease_ms = 0;
      uint64_t renew_period_ms = 0;
      uint64_t envelope_ms = 0;
      uint64_t margin_ms = 0;
      uint64_t conditionalNeedMs() const;         /// two envelopes: a write and its settlement read
      uint64_t conditionalReserveMs() const;      /// the need plus the margin
      uint64_t singleRequestReserveMs() const;    /// one envelope plus the margin
      /// Time after the start of the last confirmed renewal at which each class stops being admitted.
      uint64_t conditionalPauseAfterMs() const;
      uint64_t singleRequestPauseAfterMs() const;
  };
  CasLeaseThresholds leaseThresholds(const CasRequestBudget & budget, uint64_t lease_ms, uint64_t renew_period_ms);
  /// `14000` → `14`, `31500` → `31.5`.
  String formatSeconds(uint64_t ms);
  /// The line a writable mount logs at start: every threshold in seconds and the settings behind them.
  String describeMountTimeline(const String & disk, const String & server_root_id, const CasLeaseThresholds & t);
  ```
  Stubs return `0`, `{}`, `""`. `String disk_name;` (`/// For operator texts only.`) goes into `PoolConfig`
  after `server_root_id` and last into `MountConfig` (`= {}`); `PoolConfig::mountConfig` passes it last;
  `openPoolView` sets `pool_config.disk_name = disk_name;` after `server_root_id`. `CasMountRuntime` gets
  `const CasLeaseThresholds lease_thresholds;` after `cas_request_budget`, built in the member list as
  `leaseThresholds(cas_request_budget_, ttl_ms, period_ms)` from `config_`'s two durations, and a public
  `leaseThresholds()` accessor; `Pool` forwards it (`const CasLeaseThresholds & leaseThresholds() const`).
- [ ] **Step 2. Failing tests.** New `src/Disks/tests/gtest_cas_lease_thresholds.cpp`:
  ```cpp
  #include <gtest/gtest.h>
  #include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequestBudget.h>
  #include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>
  #include <Disks/tests/cas_test_helpers.h>
  namespace DB::ErrorCodes
  {
  extern const int BAD_ARGUMENTS;
  }
  using namespace DB::Cas;
  /// Defaults: lease 30 s, period 10 s, attempt 5 s with a 1 s connect cap (envelope 7 s), margin 2 s.
  TEST(CASLeaseThresholds, DefaultsGiveTheTimelineOfTheMountLine)
  {
      const CasLeaseThresholds t = leaseThresholds(CasRequestBudget{}, 30'000, 10'000);
      EXPECT_EQ(t.conditionalReserveMs(), 16'000u);
      EXPECT_EQ(t.singleRequestReserveMs(), 9'000u);
      EXPECT_EQ(t.conditionalPauseAfterMs(), 14'000u);
      EXPECT_EQ(t.singleRequestPauseAfterMs(), 21'000u);
      EXPECT_EQ(describeMountTimeline("cas1", "r1", t),
          "CAS disk 'cas1' (server_root_id=r1): renewal starts every 10 s. Without a confirmed renewal conditional "
          "changes pause after 14 s; deletes, publishes and uncached reads after 21 s; the lease ends after 30 s. "
          "Operations resume under the same writer_epoch after a confirmed renewal that leaves enough lease. A renewal "
          "that takes the full request envelope of 7 s ends at 17 s, after conditional changes pause. Another server "
          "may take this slot after it has seen the token unchanged for 31.5 s plus its own poll step "
          "(cas_mount_lease_ms=30000, cas_mount_renew_period_ms=10000, request envelope 7 s, cas_lease_safety_margin_ms=2000).");
  }
  /// The configuration rule in words and numbers, and which settings must match across the pool.
  TEST(CASLeaseThresholds, SettingTextsSayTheRuleAndWhatMustMatch)
  {
      DB::Cas::tests::expectThrowsCodeWithMessage(DB::ErrorCodes::BAD_ARGUMENTS,
          "a renewal must start while conditional changes are still admitted: cas_mount_renew_period_ms (14000) "
          "plus the conditional reserve (16000 = 2 × request envelope 7000 + cas_lease_safety_margin_ms 2000) "
          "must be less than cas_mount_lease_ms (30000)",
          [] { validateCasRequestBudget(CasRequestBudget{}, 30'000, 14'000, /*background_renewal=*/true); });
      const String text = mountDoubleStartMessage("r1", std::nullopt);
      EXPECT_NE(text.find("must run the same cas_mount_lease_ms;"), String::npos) << text;
      EXPECT_NE(text.find("cas_mount_renew_period_ms is local to each server"), String::npos) << text;
  }
  ```
  In `test.py`, `test_openpoolview_handoff_freezes_the_connect_cap_from_the_real_disk` replaces its two
  budget-line counts by `== 1` counts of `"CAS disk 'disk_cas_budget_capped' (server_root_id=itest-cas-budget-capped): renewal starts every 10 s"`,
  `"(cas_mount_lease_ms=30000, cas_mount_renew_period_ms=10000, request envelope 7 s, cas_lease_safety_margin_ms=2000)"`
  and `"(cas_mount_lease_ms=50000, cas_mount_renew_period_ms=10000, request envelope 15 s, cas_lease_safety_margin_ms=2000)"`;
  its comment says the envelope (7 s with the frozen 1000 ms cap, 15 s with the 5000 ms fallback) proves the cap.
- [ ] **Step 3. Red** (`build/build_pr09b_t1.log`; `'CASLeaseThresholds.*'` → `build/test_pr09b_t1_red.log`): zeros,
  empty texts, the old texts; the renewal-retry module fails `test_openpoolview…` with `0 == 1` (`…_red_it.log`).
- [ ] **Step 4. Implementation.** `CasRequestBudget.cpp` (add `#include <fmt/format.h>`); in place of
  `MOUNT_TIMELINE_FORMAT` write the string literal of `DefaultsGiveTheTimelineOfTheMountLine` with `cas1`,
  `r1`, `10`, `14`, `21`, `30`, `7`, `17`, `after`, `31.5`, `30000`, `10000`, `7`, `2000` replaced by `{}`:
  ```cpp
  namespace
  {
  uint64_t satAdd(uint64_t a, uint64_t b) { return a > std::numeric_limits<uint64_t>::max() - b ? std::numeric_limits<uint64_t>::max() : a + b; }
  uint64_t satSub(uint64_t a, uint64_t b) { return a > b ? a - b : 0; }
  }
  uint64_t CasLeaseThresholds::conditionalNeedMs() const { return satAdd(envelope_ms, envelope_ms); }
  uint64_t CasLeaseThresholds::conditionalReserveMs() const { return satAdd(conditionalNeedMs(), margin_ms); }
  uint64_t CasLeaseThresholds::singleRequestReserveMs() const { return satAdd(envelope_ms, margin_ms); }
  uint64_t CasLeaseThresholds::conditionalPauseAfterMs() const { return satSub(lease_ms, conditionalReserveMs()); }
  uint64_t CasLeaseThresholds::singleRequestPauseAfterMs() const { return satSub(lease_ms, singleRequestReserveMs()); }
  CasLeaseThresholds leaseThresholds(const CasRequestBudget & budget, uint64_t lease_ms, uint64_t renew_period_ms)
  {
      return {lease_ms, renew_period_ms, budget.attemptEnvelopeMs(), budget.lease_safety_margin_ms};
  }
  String formatSeconds(uint64_t ms)
  {
      if (ms % 1000 == 0)
          return fmt::format("{}", ms / 1000);
      String text = fmt::format("{}.{:03}", ms / 1000, ms % 1000);
      while (text.back() == '0')
          text.pop_back();
      return text;
  }
  String describeMountTimeline(const String & disk, const String & server_root_id, const CasLeaseThresholds & t)
  {
      const uint64_t renewal_end_ms = satAdd(t.renew_period_ms, t.envelope_ms);
      return fmt::format(MOUNT_TIMELINE_FORMAT, disk, server_root_id, formatSeconds(t.renew_period_ms),
          formatSeconds(t.conditionalPauseAfterMs()), formatSeconds(t.singleRequestPauseAfterMs()),
          formatSeconds(t.lease_ms), formatSeconds(t.envelope_ms), formatSeconds(renewal_end_ms),
          renewal_end_ms >= t.conditionalPauseAfterMs() ? "after" : "before",
          formatSeconds(satAdd(t.lease_ms, t.lease_ms / 20)), t.lease_ms, t.renew_period_ms,
          formatSeconds(t.envelope_ms), t.margin_ms);
  }
  ```
  - `validateCasRequestBudget`: the cadence check uses `leaseThresholds(budget, mount_lease_ttl_ms,
    mount_renew_period_ms)`, requires `mount_renew_period_ms < t.conditionalPauseAfterMs()` (today's
    inequality) and throws `"CAS disk: a renewal must start while conditional changes are still admitted:
    cas_mount_renew_period_ms ({}) plus the conditional reserve ({} = 2 × request envelope {} +
    cas_lease_safety_margin_ms {}) must be less than cas_mount_lease_ms ({})"`; the first check's
    `the mount lease TTL ({} ms)` → `cas_mount_lease_ms ({})`. Delete the closing `LOG_INFO` ("CAS request
    budget in effect…"). Header comments: `lease_safety_margin_ms` → "Reserved on top of the attempt
    envelopes in every admission"; `validateCasRequestBudget`'s first paragraph → "Throws `BAD_ARGUMENTS`
    unless envelope + margin < lease and, with background renewal, period + conditional reserve < lease."
  - `Pool::open`, after `chassert(store->isAlgoAdmitted(write_algo));`: `if (!store->config.read_only)` then
    `LOG_INFO(getLogger("CasPool"), "{}", describeMountTimeline(store->config.disk_name, store->config.server_root_id, store->leaseThresholds()));`.
  - `CasMountRuntime.cpp`: `admit` and `installRenewer` read `lease_thresholds.margin_ms`;
    `refAppendFenceOk` passes `lease_thresholds.conditionalNeedMs()`; 9a's `conditionalReserveMs` returns
    `lease_thresholds.conditionalReserveMs()`.
  - `mountDoubleStartMessage`: its three lines from `"   an authorization. Every server …"` to `"   out a healthy one.\n"` become
    ```cpp
        "   an authorization. Every server sharing this pool must run the same cas_mount_lease_ms;\n"
        "   cas_mount_renew_period_ms is local to each server. A server configured with a shorter lease\n"
        "   than its peers can fence out a healthy one.\n"
    ```
- [ ] **Step 5. Green:** `'CAS*'` (`build/test_pr09b_t1_green.log`), the renewal-retry module (`…_green_it.log`);
  `rg -n 'attemptEnvelopeMs\(\)|lease_safety_margin_ms' -- '*/Pool/CasMountRuntime.cpp'` prints nothing.
  **Commit** the ten files: `Derive CAS admission, mount line and thresholds from one value`.

## Task 2: Refusal texts name the reserve and the last renewal error

**Files:** `CA/Backend/CasRequestBudget.{h,cpp}`, `CA/Backend/CasFence.h`, `CA/Backend/CasRequests.{h,cpp}`,
`CA/Backend/CasWriteResult.h`, `CA/Pool/CasMountRuntime.{h,cpp}`, `CA/Pool/CasPool.cpp`,
`src/Disks/tests/gtest_cas_lease_thresholds.cpp`, `src/Disks/tests/gtest_cas_requests.cpp`.

- [ ] **Step 1. Stubs.** `CasRequestBudget.h`:
  ```cpp
  /// Why an operation that needs `needed_ms` of lease was not admitted. `remaining_ms` is empty once the
  /// confirmed lease has ended; `refused_code` is set when the renewal's last error is a store refusal.
  String describeLeaseRefusal(const String & disk, const CasLeaseThresholds & t, std::optional<uint64_t> remaining_ms,
                              uint64_t needed_ms, const std::optional<String> & last_renewal_error,
                              const std::optional<String> & refused_code);
  ```
  (returns `""`). `CasFence.h` (`#include <base/types.h>`), `Fence`, after `check_or_throw`:
  `std::function<String(uint64_t needed_ms)> describe_no_budget = {};` with `/// Why \`admit\` refused
  \`needed_ms\` with \`NoBudget\`, for the caller's error text. Empty explains nothing.` `CasWriteResult.h`, `GaveUp`, last member: `String lease_refusal;` (`/// The fence's explanation when
  the lease bound ended the call; empty otherwise.`). `CasMountRuntime.h`, public:
  `String describeAdmissionRefusal(uint64_t needed_ms) const;` (returns `""`).
- [ ] **Step 2. Failing tests.** `gtest_cas_lease_thresholds.cpp`:
  ```cpp
  TEST(CASLeaseThresholds, RefusalTextsNameTheReserveAndTheLastError)
  {
      const CasLeaseThresholds t = leaseThresholds(CasRequestBudget{}, 30'000, 10'000);
      EXPECT_EQ(describeLeaseRefusal("cas1", t, 14'000, 14'000, String("PUT gc/server-roots/r1/mount: transport SlowDown slow down"), std::nullopt),
          "CAS disk 'cas1': this change is not admitted: 14 s of the confirmed lease remain and a conditional change "
          "needs more than 16 s (two requests of 7 s plus 2 s). Renewal keeps retrying; changes resume after a "
          "confirmation that leaves enough lease. Last renewal error: PUT gc/server-roots/r1/mount: transport SlowDown slow down.");
      EXPECT_EQ(describeLeaseRefusal("cas1", t, std::nullopt, 7'000, String("PUT k: refused AccessDenied denied"), String("AccessDenied")),
          "CAS disk 'cas1': this request is not admitted: the confirmed lease has ended and a single request needs "
          "more than 9 s (one request of 7 s plus 2 s). The object store refuses the renewal request (AccessDenied); "
          "requests resume after the cause is fixed and a renewal is confirmed.");
      EXPECT_EQ(describeLeaseRefusal("cas1", t, 15'000, 15'200, std::nullopt, std::nullopt),
          "CAS disk 'cas1': this change is not admitted: 15 s of the confirmed lease remain and a conditional change "
          "needs more than 17.2 s (15.2 s of requests and retry pauses plus 2 s). Renewal keeps retrying; changes "
          "resume after a confirmation that leaves enough lease.");
  }
  ```
  `gtest_cas_requests.cpp`, after `AResolveReadRefusedForLeaseBudgetIsReportedAsTheLeaseDeadline`:
  ```cpp
  /// The fence's explanation of a lease refusal reaches the caller, on a write and on a read.
  TEST(CASRequests, ALeaseRefusalCarriesTheFenceExplanation)
  {
      FakeClock clock;
      auto backend = std::make_shared<CountingBackend>();
      Fence fence{[] { return uint64_t{0}; }, [](uint64_t, uint64_t) { return Fence::Admit::NoBudget; }, [](uint64_t) {},
                  [](uint64_t) { return String("CAS disk 'cas1': this change is not admitted"); }};
      auto requests = makeRequests(backend, clock, fence);
      auto op = requests.admit();
      expectThrowsCodeWithMessage(DB::ErrorCodes::NETWORK_ERROR, "create: CAS disk 'cas1': this change is not admitted",
                                  [&] { (void)orThrow(op.create("k", "v", Retry::standard()), "create"); });
      expectThrowsCodeWithMessage(DB::ErrorCodes::NETWORK_ERROR, "CAS disk 'cas1': this change is not admitted",
                                  [&] { (void)op.read("k", Retry::standard()); });
      EXPECT_EQ(backend->writeTotal(), 0u);
      EXPECT_EQ(backend->getTotal(), 0u);
  }
  ```
- [ ] **Step 3. Red** (`build/build_pr09b_t2.log`; `'CASLeaseThresholds.Refusal*:CASRequests.ALeaseRefusal*'`
  → `build/test_pr09b_t2_red.log`): empty texts; the write says "gave up at the lease deadline".
- [ ] **Step 4. Implementation.**
  - `CasRequestBudget.cpp`:
    ```cpp
    String describeLeaseRefusal(const String & disk, const CasLeaseThresholds & t, std::optional<uint64_t> remaining_ms,
                                uint64_t needed_ms, const std::optional<String> & last_renewal_error,
                                const std::optional<String> & refused_code)
    {
        const bool conditional = needed_ms >= t.conditionalNeedMs();
        String need;
        if (needed_ms == t.conditionalNeedMs())
            need = fmt::format("a conditional change needs more than {} s (two requests of {} s plus {} s)",
                formatSeconds(t.conditionalReserveMs()), formatSeconds(t.envelope_ms), formatSeconds(t.margin_ms));
        else if (needed_ms == t.envelope_ms)
            need = fmt::format("a single request needs more than {} s (one request of {} s plus {} s)",
                formatSeconds(t.singleRequestReserveMs()), formatSeconds(t.envelope_ms), formatSeconds(t.margin_ms));
        else
            need = fmt::format("{} needs more than {} s ({} s of requests and retry pauses plus {} s)",
                conditional ? "a conditional change" : "a request", formatSeconds(satAdd(needed_ms, t.margin_ms)),
                formatSeconds(needed_ms), formatSeconds(t.margin_ms));
        const char * what = conditional ? "changes" : "requests";
        String tail = refused_code
            ? fmt::format("The object store refuses the renewal request ({}); {} resume after the cause is fixed and a "
                          "renewal is confirmed.", *refused_code, what)
            : fmt::format("Renewal keeps retrying; {} resume after a confirmation that leaves enough lease.", what);
        if (!refused_code && last_renewal_error)
            tail += fmt::format(" Last renewal error: {}.", *last_renewal_error);
        return fmt::format("CAS disk '{}': this {} is not admitted: {} and {}. {}", disk, conditional ? "change" : "request",
            remaining_ms ? fmt::format("{} s of the confirmed lease remain", formatSeconds(*remaining_ms))
                         : String("the confirmed lease has ended"),
            need, tail);
    }
    ```
  - PR 4's `RenewalProgress` gains `std::optional<String> refused_code;` (`/// The store's error name when
    the last error is a definite refusal.`). The code lookup inside PR 4's `describeRequestFailure` becomes
    `String requestFailureCode(const std::exception & e)` beside it (same comment as its `code` today), and
    `on_failure` sets, under its lock, `renewal_progress.refused_code = isDefinitelyRefusedWrite(e) ?
    std::optional<String>(requestFailureCode(e)) : std::nullopt;` (computed before the lock).
  - `CasMountRuntime.cpp`:
    ```cpp
    String CasMountRuntime::describeAdmissionRefusal(uint64_t needed_ms) const
    {
        const uint64_t now = bootMsNow();
        const uint64_t deadline = mount_fence.deadline_boot_ms.load(std::memory_order_acquire);
        const RenewalProgress progress = renewalProgress();
        return describeLeaseRefusal(config.disk_name, lease_thresholds,
            now < deadline ? std::optional<uint64_t>(deadline - now) : std::nullopt, needed_ms,
            progress.last_error, progress.refused_code);
    }
    ```
    `CasPool.cpp`: the mount plane's `Fence{…}` in the `Pool` constructor gets a fourth closure
    `[this](uint64_t needed) { return mount_runtime.describeAdmissionRefusal(needed); }`.
  - `CasRequests.h`, `CasOperation`, private after `std::optional<ReadStop> last_read_stop;`:
    `mutable String last_lease_refusal;` (`/// The fence's explanation of the last gate, if it said \`NoBudget\`.`).
    `CasRequests.cpp`: `CasOperation::gate` starts with `last_lease_refusal.clear();` and its `NoBudget` case becomes
    ```cpp
            case Fence::Admit::NoBudget:
                if (owner.fence.describe_no_budget)
                    last_lease_refusal = owner.fence.describe_no_budget(needed_ms);
                return Gate::NoBudget;
    ```
    `gaveUp` builds its `GaveUp` in a local, sets `lease_refusal = last_lease_refusal` when `source ==
    GaveUp::Source::Lease`, returns it. `giveUpReadNoBudget` passes `last_lease_refusal.empty() ?
    String(what) : fmt::format("{}: {}", what, last_lease_refusal)` as `when`.
  - `CasWriteResult.h`, `orThrow`, case `GaveUp::Why::Deadline`: if `!g.lease_refusal.empty()`,
    `throwCasWriteRetryLater(fmt::format("{}: {}", what, g.lease_refusal));`; otherwise today's text.
- [ ] **Step 5. Green** `'CAS*'` (`build/test_pr09b_t2_green.log`). **Commit** the eleven files: `Explain a CAS lease refusal with the reserve and the last renewal error`.

## Task 3: One snapshot for admission, text and log; terminal states at `ERROR`

**Files:** `CA/Pool/CasMountRuntime.{h,cpp}`, `src/Disks/tests/gtest_cas_pool.cpp`, `tests/integration/test_cas_mount_renewal_retry/test.py`.

- [ ] **Step 1. Stub.** `CasMountRuntime.h`, public, empty body: `void logLeaseTransition() noexcept;`,
  `/// One line when the clock pause starts, becomes an expiry, or ends; a renewing thread stuck in a
  request reports late and says how late.` Private: PR 8's `lease_expiry_warned` → `std::atomic<LeasePause> logged_pause{LeasePause::None};`
  and `std::atomic<uint64_t> logged_pause_since_boot_ms{0};`.
- [ ] **Step 2. Failing tests.** `gtest_cas_pool.cpp`, after PR 8's `ALeaseExpiryIsLoggedOnceAndReported`
  (whose `prefix` becomes `"the mount lease ended "`):
  ```cpp
  /// Admission, the lease pause, the refusal text and the log line all turn at the printed threshold.
  TEST(CASMountRuntime, OneSnapshotTurnsAdmissionTextAndLogTogether)
  {
      ScopedRemountLogCapture logs;
      auto backend = std::make_shared<RuntimeRenewBackend>();
      const Layout layout("one-snapshot");
      std::atomic<uint64_t> boot_ms{100};
      uint64_t wall_ms = 1000;
      const UInt128 uuid{1};
      ASSERT_EQ(claimMount(*DB::Cas::tests::OperationForTest(backend), layout, "test", uuid, 1, wall_ms, 30'000).kind,
                MountClaimResult::Claimed);
      CasEventSink sink;
      RuntimeUnderTest runtime_holder(backend, layout,
          MountConfig{.mount_lease_ttl_ms = std::chrono::milliseconds(30'000), .boot_ms_fn = [&] { return boot_ms.load(); }, .disk_name = "cas1"},
          "test", sink, CasRequestBudget{}, [] { return false; });
      CasMountRuntime & runtime = *runtime_holder;
      runtime.installRenewer(uuid, 1, [&] { return wall_ms; });
      const uint64_t anchor = runtime.startRenewer();
      runtime.armMountFence(uuid, 1, anchor + 30'000);
      const CasLeaseThresholds & t = runtime.leaseThresholds();
      const auto count = [&](const String & needle) { return countOccurrences(logs.captured(), needle); };
      const String where = "CAS disk 'cas1' (server_root_id=test): ";
      boot_ms = anchor + t.conditionalPauseAfterMs() - 1;
      runtime.logLeaseTransition();
      EXPECT_TRUE(runtime.refAppendFenceOk());
      EXPECT_EQ(count(where), 0u);
      boot_ms = anchor + t.conditionalPauseAfterMs();
      runtime.logLeaseTransition();
      runtime.logLeaseTransition();   /// a second call at the same state logs nothing
      EXPECT_FALSE(runtime.refAppendFenceOk());
      EXPECT_EQ(runtime.leasePause().pause, LeasePause::RenewalOverdue);
      EXPECT_EQ(runtime.describeAdmissionRefusal(t.conditionalNeedMs()),
                describeLeaseRefusal("cas1", t, t.conditionalReserveMs(), t.conditionalNeedMs(), std::nullopt, std::nullopt));
      EXPECT_EQ(count(where + "conditional changes paused 0 s ago: the confirmed lease left is not more than their reserve of 16 s"), 1u);
      boot_ms = anchor + 30'250;
      runtime.logLeaseTransition();
      runtime.setMountDeadline(boot_ms + 30'000);
      runtime.logLeaseTransition();
      EXPECT_EQ(count(where + "the mount lease ended 0.25 s ago"), 1u);
      EXPECT_EQ(count(where + "operations resumed under writer_epoch"), 1u);
      EXPECT_EQ(count("after a pause of 16.25 s"), 1u) << logs.captured();
      runtime.finishTeardown(false);
  }
  ```
  `countOccurrences(haystack, needle)` is a small free helper added next to `ScopedRemountLogCapture`
  (the loop PR 8's expiry test inlines; that test then uses it too).
  `ScopedRemountLogCapture` gets a `const String & level = "information"` constructor parameter; then:
  ```cpp
  /// Entering a terminal state logs at ERROR, the level operators alert on.
  TEST(CASMountRuntime, TerminalTransitionsLogAtError)
  {
      for (const bool identity : {true, false})
      {
          ScopedRemountLogCapture errors("error");
          auto backend = std::make_shared<RuntimeRenewBackend>();
          const Layout layout("terminal-log");
          CasEventSink sink;
          RuntimeUnderTest runtime_holder(backend, layout, MountConfig{}, "test", sink, runtimeRenewBudget(), [] { return false; });
          CasMountRuntime & runtime = *runtime_holder;
          runtime.setLifecycleForTest(PoolLifecycle::TransientNotLive);
          if (identity)
              runtime.enterIdentityLost();
          else
              runtime.enterVanished(PoolLifecycle::VanishedReplaced, "test");
          EXPECT_NE(errors.captured().find(identity ? "entered IdentityLost" : "entered Vanished(replaced)"), String::npos)
              << errors.captured();
          runtime.finishTeardown(false);
      }
  }
  ```
  `test.py`: `EXPIRY_WARNING = "the mount lease ended"`.
- [ ] **Step 3. Red** (`'CASMountRuntime.OneSnapshot*:CASMountRuntime.ALeaseExpiry*:CASMountRuntime.TerminalTransitions*'`
  → `build/test_pr09b_t3_red.log`): the log counts are `0`; the terminal lines are below `error`. The
  admission and refusal assertions already pass (Tasks 1–2).
- [ ] **Step 4. Implementation.** `CasMountRuntime.cpp`: delete `warnOnceIfLeaseExpired` and the
  `lease_expiry_warned` stores in `publishRenewedDeadline` and `armMountFence`; PR 4's `on_write_attempt`
  calls `logLeaseTransition();` in its place; `renewRenewerOnce` calls it right after `call.lease->finish(…)`.
  ```cpp
  void CasMountRuntime::logLeaseTransition() noexcept
  {
      try
      {
          /// A tripped fence or a non-live pool is the remount's to report.
          if (lifecycle() != PoolLifecycle::Live || mount_fence.lost.load(std::memory_order_acquire))
              return;
          const LeasePauseState state = leasePause();
          const LeasePause before = logged_pause.exchange(state.pause, std::memory_order_relaxed);
          if (before == state.pause)
              return;
          if (before == LeasePause::None)
              logged_pause_since_boot_ms.store(state.since_boot_ms, std::memory_order_relaxed);
          const uint64_t now = bootMsNow();
          const String where = fmt::format("CAS disk '{}' (server_root_id={})", config.disk_name, server_root_id);
          const String ago = formatSeconds(now > state.since_boot_ms ? now - state.since_boot_ms : 0);
          switch (state.pause)
          {
              case LeasePause::RenewalOverdue:
                  LOG_WARNING(getLogger("CasPool"), "{}: conditional changes paused {} s ago: the confirmed lease left is "
                      "not more than their reserve of {} s; renewal keeps retrying under writer_epoch {}",
                      where, ago, formatSeconds(lease_thresholds.conditionalReserveMs()), liveWriterEpoch());
                  return;
              case LeasePause::LeaseExpired:
                  LOG_WARNING(getLogger("CasPool"), "{}: the mount lease ended {} s ago under writer_epoch {}; renewal "
                      "keeps retrying and operations that need the lease are refused until a renewal is confirmed",
                      where, ago, liveWriterEpoch());
                  return;
              case LeasePause::None:
              {
                  const uint64_t since = logged_pause_since_boot_ms.load(std::memory_order_relaxed);
                  LOG_INFO(getLogger("CasPool"), "{}: operations resumed under writer_epoch {} after a pause of {} s",
                      where, liveWriterEpoch(), formatSeconds(now > since ? now - since : 0));
                  return;
              }
          }
      }
      catch (...) // NOLINT(bugprone-empty-catch)
      {
          /// A lost diagnostic must not change how the renewal ends.
      }
  }
  ```
  `enterIdentityLost`, `enterVanished`: `LOG_WARNING` → `LOG_ERROR`.
- [ ] **Step 5. Green** `'CAS*'` (`build/test_pr09b_t3_green.log`) and the renewal-retry module
  (`build/test_pr09b_t3_green_it.log`; PR 8's long-outage test counts one expiry line). **Commit** the
  four files: `Log each CAS lease pause, expiry and resumption once`.

## Task 4: `cas_mount_lease_ms` with the synonym `cas_mount_lease_ttl_ms`

**Files:** `CA/ContentAddressedSettings.cpp`, `CA/ContentAddressedMetadataStorage.cpp`, `src/Disks/tests/gtest_cas_settings.cpp`.

- [ ] **Step 1. Rename** (a conflicting pair still silently takes the last value). In
  `LIST_OF_CONTENT_ADDRESSED_SETTINGS` the `mount_lease_ttl_ms` line becomes
  `ALIAS(UInt64, mount_lease_ms, 30000, "How long a confirmed renewal entitles this server to write, from the renewal's start, in milliseconds. Operations stop earlier; the mount line prints when. Must be the same on every server of the pool", 0, mount_lease_ttl_ms) \`.
  `mount_renew_period_ms` → "How often a renewal starts, in milliseconds; also the step at which this server
  observes other slots. Local to this server"; `lease_safety_margin_ms` → "Margin reserved on top of the
  request envelopes in every admission, in milliseconds". `validate` and `ContentAddressedMetadataStorage.cpp`
  (`extern`, `mount_lease_ttl(…)`) use `mount_lease_ms`; the `validate` text says `cas_mount_lease_ms must be >= 1`.
- [ ] **Step 2. Failing test.** `gtest_cas_settings.cpp` (extern `ContentAddressedSettingsUInt64 mount_lease_ms;`):
  ```cpp
  TEST(CASContentAddressedSettings, LeaseSynonymLoadsAndAConflictNamesBothKeys)
  {
      for (const char * keys : {"<cas_mount_lease_ttl_ms>40000</cas_mount_lease_ttl_ms>",
                                "<cas_mount_lease_ms>40000</cas_mount_lease_ms><cas_mount_lease_ttl_ms>40000</cas_mount_lease_ttl_ms>"})
      {
          auto cfg = makeConfig(String("<cas_server_root_id>srv1</cas_server_root_id>") + keys);
          ContentAddressedSettings s;
          s.loadFromConfig(*cfg, "disk", "/scratch", "/scratch", identity_macros);
          EXPECT_EQ(s[ContentAddressedSetting::mount_lease_ms].value, 40000u) << keys;
      }
      expectLoadFailureWithExactMessage(
          "<cas_server_root_id>srv1</cas_server_root_id><cas_mount_lease_ms>40000</cas_mount_lease_ms>"
          "<cas_mount_lease_ttl_ms>50000</cas_mount_lease_ttl_ms>",
          ErrorCodes::BAD_ARGUMENTS,
          "content_addressed disk `disk`: `cas_mount_lease_ms` (40000) and `cas_mount_lease_ttl_ms` (50000) set the "
          "same setting to different values; keep one");
  }
  ```
  Red (`build/test_pr09b_t4_red.log`): "expected settings load to fail".
- [ ] **Step 3. Implementation.** `loadFromConfig`, after the `both \`{}\` and \`cas_{}\`` loop (`#include <map>`):
  ```cpp
      /// A setting and its synonym name one value; two different values are a configuration error.
      std::map<std::string, std::pair<std::string, std::string>> by_setting;
      for (const std::string & key : config_keys)
      {
          const std::string_view base = splitRepeatIndex(key).base;
          const std::string_view name = base.starts_with(CAS_KEY_PREFIX) ? base.substr(CAS_KEY_PREFIX.size()) : base;
          if (!ContentAddressedSettingsImpl::hasBuiltin(name))
              continue;
          std::string value = config.getString(config_prefix + "." + key);
          const auto [it, inserted] = by_setting.try_emplace(
              std::string(ContentAddressedSettingsTraits::resolveName(name)), std::string(base), value);
          if (!inserted && it->second.second != value)
              throw Exception(ErrorCodes::BAD_ARGUMENTS,
                  "content_addressed disk `{}`: `{}` ({}) and `{}` ({}) set the same setting to different values; keep one",
                  config_prefix, it->second.first, it->second.second, base, value);
      }
  ```
  Green `'CASContentAddressedSettings.*:CASSettings.*'` (`build/test_pr09b_t4_green.log`; the
  both-spellings and legacy tests are golden; the renewal-retry configs keep `cas_mount_lease_ttl_ms`, so
  their startup checks the synonym end to end). **Commit** the three files:
  `Rename the CAS lease setting to cas_mount_lease_ms with a synonym`.

## Task 5: The decommission refusal names the state it waits for

**Files:** `CA/Pool/CasPool.cpp`, `CA/Pool/CasServerRoot.cpp`, `src/Disks/tests/gtest_cas_decommission.cpp`.

- [ ] **Failing test**, after `TEST(CASDecommission, RefusesLiveMember)`; red: the text is missing
  (`build/test_pr09b_t5_red.log`):
  ```cpp
  TEST(CASDecommission, ALiveMemberRefusalNamesTheFencedState)
  {
      auto backend = std::make_shared<InMemoryBackend>();
      auto victim = openVictim(backend);
      DB::Cas::tests::expectThrowsCodeWithMessage(ErrorCodes::ABORTED, "system.cas_mounts shows state = 'fenced'", [&]
          { Pool::openForDecommission(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim"); });
  }
  ```
- [ ] `CasPool.cpp`, the `NoWait` refusal: `"Refusing (no FORCE variant exists; stop the server or wait for its
  lease to lapse)."` → `"Refusing (no FORCE variant exists). A member stopped without a clean farewell becomes
  removable once a GC round on another member fences its slot: wait until system.cas_mounts shows state =
  'fenced' for it, or stop it cleanly."` ("alive or contended" stays; `test_cas_drop_pool_member` matches it).
  `CasServerRoot.cpp`, the "LIVE mount lease exists" refusal: `"(stop the server or wait for its lease to
  lapse)"` → `"(stop the server cleanly, or wait until system.cas_mounts shows state = 'fenced' for it)"`.
  Green `'CASDecommission.*'`. **Commit** the three files: `Say which state a CAS decommission waits for`.

## Task 6: Docs

- [ ] `configuration.md`: the lease row is `cas_mount_lease_ms` with Task 4's description plus "Change it
  only with the pool stopped. Synonym: `cas_mount_lease_ttl_ms`; both at different values is a
  configuration error."; `cas_mount_renew_period_ms` and `cas_lease_safety_margin_ms` take Task 4's
  descriptions, the period row adding "Period plus the conditional reserve must be less than
  `cas_mount_lease_ms`." Every other `cas_mount_lease_ttl_ms` → `cas_mount_lease_ms`. The paragraph "All
  servers sharing a pool must run the same …" starts "All servers sharing a pool must run the same
  `cas_mount_lease_ms`; `cas_mount_renew_period_ms` is local." Then:
  ```markdown
  A CAS disk removed from `storage_configuration` keeps renewing its mount lease until the server
  restarts, so no other server can take its `server_root_id`. `SYSTEM CAS FORGET '<disk>'` stops the
  renewal without a restart; another server can take the slot after its observation window.

  ### Lease defaults review {#lease-defaults-review}

  | Setting | Default | Evidence | Decision |
  |---|---|---|---|
  | `cas_mount_lease_ms` | `30000` | PR 8 acceptance | keep |
  | `cas_mount_renew_period_ms` | `10000` | PR 8 acceptance | keep |
  | `cas_attempt_timeout_ms` | `5000` | none | keep |
  | `cas_lease_safety_margin_ms` | `2000` | none | keep |
  ```
  Each "PR 8 acceptance" cell gets the PR 8 URL and the numbers its description reports for S39 leg B
  and the CAS-256 repro (outage length, `CASMountLeaseLost` delta, `writer_epoch` unchanged), or `none`.
- [ ] `architecture/mounts-and-leases.md`: after PR 8's "**An expired lease is a pause.**" paragraph,
  "**Thresholds.** A writable mount logs every threshold at start:" and the Task 1 default line as a code
  block; PR 8's quoted `WARNING` → the three Task 3 lines and Task 2's conditional refusal text.
  `architecture/garbage-collection.md`: `cas_mount_lease_ttl_ms` → `cas_mount_lease_ms`.
- [ ] `operations/monitoring.md`: `### Terminal mount states {#terminal-mount-states}`: counter, state, operator
  action, one row per hit of `rg -n 'M\(CAS' src/Common/ProfileEvents.cpp | rg -i terminal` (from its
  description); at least `CASIdentityLost` (`identity_lost`; restart or `SYSTEM CAS FORGET`),
  `CASDataRootVanished` (`vanished(replaced)`/`closed(forgotten)`; restart re-registers the disk),
  `CASMountExclusivityViolation` (single-writer guarantee broken; must stay `0`). These states log at
  `ERROR`; list the three Task 3 lines.
- [ ] `operations/troubleshooting.md`, row "Writes slow down or stall": "(`system.cas_mounts.expires_at`
  moving forward, `last_success_age_seconds` not climbing)" → "(`expires_at` of the `is_local = 1` row
  moving forward, `lifecycle = 'live'`)". `operations/migration.md`, **Preconditions**, the sentence naming
  `last_success_age_seconds` → "Check the member's `state` and `expires_at` in `system.cas_mounts`: a
  `live` row, or an `expires_at` that keeps moving, is not a candidate yet. A member stopped without a
  clean farewell becomes removable once its row shows `state = 'fenced'`, which needs GC on some member."
- [ ] Gates: `rg -n 'cas_mount_lease_ttl_ms' docs/en` only `configuration.md`'s synonym mentions;
  `rg -n 'last_success_age_seconds' docs/en/antalya/cas/operations/{migration,troubleshooting}.md` nothing.
  **Commit** the six files: `Document the CAS lease thresholds, synonym and terminal states`.

## Task 7: Gates and PR

- [ ] Size (`git diff --numstat altinity/antalya-26.6 -- src/Disks/DiskObjectStorage ':!src/Disks/tests'`);
  `CAS*` green on `build`, `build_asan` (`*/test_pr09b_gate.log`); renewal-retry and
  `test_cas_drop_pool_member` green. No test expects `LOGICAL_ERROR`.
- [ ] PR (`.github/PULL_REQUEST_TEMPLATE.md`, base `antalya-26.6`). Description: "A writable CAS disk logs
  at start when conditional changes, other operations and the lease stop without a confirmed renewal, from
  the code admission uses. A refused operation says how much lease is left, what it needs and the last
  renewal error. Each pause, expiry and resumption is one log line; `identity_lost`, `vanished` and `closed`
  log at `ERROR`. `cas_mount_lease_ms` is the lease setting, `cas_mount_lease_ttl_ms` a synonym. A
  decommission refusal names `state = 'fenced'`." Mixed versions: no format change; both lease names at
  different values refuse to load; the mount line replaces `CAS request budget in effect`. Changelog:
  Improvement, "CAS disks print their lease thresholds at start and explain every refusal caused by an
  unconfirmed lease; `cas_mount_lease_ms` replaces `cas_mount_lease_ttl_ms`."
