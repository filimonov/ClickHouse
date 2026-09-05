# CAS R2 series (specs 1 → 4 → 2 → 3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the CAS mount-lease renewal survive data-plane connection churn: reissue a connect-failure-hinted write without a settle read (spec 1), let engine reissues run as transport attempt ≥ 2 under one authoritative attempt envelope that includes TCP connect (spec 4), add the explicitly unsafe no-delay reclaim after a hard restart (spec 2), and measure then reduce connection churn on CAS-over-S3 disks (spec 3).

**Architecture:** All four changes live in CAS-owned code under `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/` plus the S3 object storage (`src/Disks/DiskObjectStorage/ObjectStorages/S3/`) and the HTTP connection pool counters (`src/Common/HTTPConnectionPool.cpp`). The request engine (`CasRequests`) gains a connect-failure hint path and a zero-pause fuse path; the transport learns the engine's attempt number through `ReadSettings`/`WriteSettings` and a small control-request context; `CasRequestBudget` becomes the single owner of the attempt envelope; the pool gets one new boolean setting consulted at one site; the connection pool gets per-reason counters before any keep-alive value is changed.

**Tech Stack:** C++23 (ClickHouse), gtest (`src/Disks/tests/gtest_cas_*.cpp`, `src/IO/tests/gtest_writebuffer_s3.cpp`, `src/Common/tests/gtest_connection_pool.cpp`), Python integration tests under `tests/integration/`, Praktika local runner.

**Specs:**
- `docs/superpowers/specs/2026-09-05-cas-presend-failure-reissue-design.md` (rev.6)
- `docs/superpowers/specs/2026-09-05-cas-adaptive-first-attempt-timeout-design.md` (rev.5)
- `docs/superpowers/specs/2026-09-05-cas-heartbeat-lease-design.md` (rev.5)
- `docs/superpowers/specs/2026-09-05-cas-connection-churn-design.md` (rev.3)

## Global Constraints

- No edits under `src/IO/S3/PocoHTTPClient.*`, `src/IO/S3Common.*` (`S3Exception`), `src/IO/ConnectionTimeouts.*`, `src/IO/S3AuthSettings.*`. Additive edits to `src/IO/ReadSettings.h`, `src/IO/WriteSettings.h`, `src/IO/ReadBufferFromS3.cpp`, `src/IO/WriteBufferFromS3.cpp` and `src/IO/S3/Requests.h` are limited to the attempt-seed field and its one-line application (Task B1); flag any other need instead of making it.
- Allman braces. No `sleep` to fix races. No fallback paths: an operation that fails propagates. Comments keep the reason, never a plan or review reference.
- Tests are written FIRST and run to FAIL before the step that makes them pass. Never add `no-*` tags to stateless tests. New tests go in new test cases, not appended to existing ones, except where a spec names an existing test to change.
- Commit with explicit paths only: `git commit -F <msg-file> -- <paths>`. Never `git add -A`. Never push. Never rebase or amend.
- Build: `flock build/.ninja_lock ninja -C build unit_tests_dbms > build/build_<task>.log 2>&1`; run gtests as `build/src/unit_tests_dbms --gtest_filter='<filter>' > build/test_<name>.log 2>&1`; have a subagent summarize each log. The CAS gate filter is exactly `CAS*` (never widen).
- Each spec is its own commit (spec 3: two commits). Each spec's commit is reviewed with `codex exec -m gpt-5.6-sol -c model_reasoning_effort=high --sandbox read-only - < <prompt-file>` until no MAJOR finding remains, before the next spec starts.
- Settings, events and docs: setting descriptions in `ContentAddressedSettings.cpp` use plain words; event descriptions in `src/Common/ProfileEvents.cpp` end with what a non-zero value means; docs headers carry `{#anchors}`; SQL/class/function names in backticks; functions written `f` not `f()`.
- Defaults (verbatim from the specs): `cas_mount_lease_ttl_ms` 30000, `cas_mount_renew_period_ms` 10000, `cas_attempt_timeout_ms` 5000 (now `≥ 1`), `cas_lease_safety_margin_ms` 2000, `connect_timeout_ms` 1000 → envelope 7000 ms (attempt + 2 × connect cap; the TLS handshake gets a second connect interval); `cas_unsafe_remount_no_delay` default `0`; connect-hint flat pause 50 ms; hint texts `Cannot assign requested address`, `Connection refused`, `No route to host`, `Network is unreachable`, `connect timed out`.

---

## Part A — Spec 1: connect-failure hint reissues a write without a preceding read

### Task A1: Pin the Poco texts and write the classifier

**Files:**
- Modify: `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h` (declare next to `isDefinitelyRefusedWrite`, line ~31)
- Modify: `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.cpp` (define near `isDefinitelyRefusedWrite`, line ~236)
- Test: `src/Disks/tests/gtest_cas_requests.cpp`

**Interfaces:**
- Produces: `bool DB::Cas::isConnectFailureHint(const std::exception & e);` — true only for a `DB::S3Exception` with `getS3ErrorCode() == Aws::S3::S3Errors::NETWORK_CONNECTION` whose `message()` contains one of the five substrings (case-sensitive). Returns false when `USE_AWS_S3` is off.

- [ ] **Step 1: Write the pinned-text test and the classifier-guard test**

Append to `src/Disks/tests/gtest_cas_requests.cpp` (the file already includes `IO/S3Common.h` under `USE_AWS_S3`; add `#include <Poco/Net/SocketImpl.h>`, `#include <Poco/Net/NetException.h>`, `#include <cerrno>`):

```cpp
/// The hint is a text match on this repository's Poco. These pins fail the build's own tests the day
/// `SocketImpl::error` changes a word, which is the only way a text match stays honest.
TEST(CASRequestsConnectHint, PocoTextsArePinned)
{
    const auto text_of = [](int err)
    {
        try
        {
            Poco::Net::SocketImpl::error(err);
        }
        catch (const Poco::Exception & e)
        {
            return e.displayText();
        }
        return std::string("did not throw");
    };
    EXPECT_THAT(text_of(EADDRNOTAVAIL), testing::HasSubstr("Cannot assign requested address"));
    EXPECT_THAT(text_of(ECONNREFUSED), testing::HasSubstr("Connection refused"));
    EXPECT_THAT(text_of(EHOSTUNREACH), testing::HasSubstr("No route to host"));
    EXPECT_THAT(text_of(ENETUNREACH), testing::HasSubstr("Network is unreachable"));
    /// The fifth text is the connect poll's own: `SocketImpl::connect` throws
    /// `Poco::TimeoutException("connect timed out", ...)` (SocketImpl.cpp ~138).
    EXPECT_THAT(Poco::TimeoutException("connect timed out", "10.255.255.1:9").displayText(),
                testing::HasSubstr("connect timed out"));
}

#if USE_AWS_S3
TEST(CASRequestsConnectHint, ClassifierGuards)
{
    using Aws::S3::S3Errors;
    for (const char * text : {"Cannot assign requested address", "Connection refused", "No route to host",
                              "Network is unreachable", "connect timed out"})
    {
        const DB::S3Exception hinted(fmt::format("Poco::Exception. Code: 1000, e.code() = 99, {}: 10.0.0.1:9000", text),
                                     S3Errors::NETWORK_CONNECTION);
        EXPECT_TRUE(isConnectFailureHint(hinted)) << text;
        /// The same text under another S3 error is not a transport verdict.
        const DB::S3Exception other(String(text), S3Errors::INTERNAL_FAILURE);
        EXPECT_FALSE(isConnectFailureHint(other)) << text;
    }
    EXPECT_FALSE(isConnectFailureHint(DB::S3Exception("Timeout", S3Errors::NETWORK_CONNECTION)));
    EXPECT_FALSE(isConnectFailureHint(DB::S3Exception("Connection reset by peer", S3Errors::NETWORK_CONNECTION)));
    EXPECT_FALSE(isConnectFailureHint(Poco::TimeoutException("connect timed out")));
    EXPECT_FALSE(isConnectFailureHint(std::runtime_error("Connection refused")));
}
#endif
```

- [ ] **Step 2: Run the tests, expect a compile failure**

Run: `flock build/.ninja_lock ninja -C build unit_tests_dbms > build/build_a1.log 2>&1; tail -5 build/build_a1.log`
Expected: error `use of undeclared identifier 'isConnectFailureHint'`.

- [ ] **Step 3: Declare and define the classifier**

`CasRequests.h`, after `bool isDefinitelyRefusedWrite(const std::exception & e);`:

```cpp
/// TRUE when a transport failure's text says the CONNECTION itself failed: no free local port, a
/// refused or unreachable peer, or the connect poll's own timeout. A hint, not a verdict: the same
/// errno can be reported after `send` or `recv`, so a hinted attempt keeps every property of an
/// ambiguous one; what the hint changes is only that the engine reissues before spending a read.
bool isConnectFailureHint(const std::exception & e);
```

`CasRequests.cpp`, after `isDefinitelyRefusedWrite`:

```cpp
bool isConnectFailureHint(const std::exception & e)
{
#if USE_AWS_S3
    const auto * s3 = dynamic_cast<const S3Exception *>(&e);
    if (!s3 || s3->getS3ErrorCode() != Aws::S3::S3Errors::NETWORK_CONNECTION)
        return false;
    /// This repository's Poco (`SocketImpl::error`, `SocketImpl::connect`) is the source of every text.
    static constexpr std::array<std::string_view, 5> texts{
        "Cannot assign requested address", "Connection refused", "No route to host",
        "Network is unreachable", "connect timed out"};
    const std::string_view message = s3->message();
    for (std::string_view text : texts)
        if (message.find(text) != std::string_view::npos)
            return true;
#endif
    return false;
}
```

Add `#include <array>` to the includes.

- [ ] **Step 4: Run the two tests, expect PASS**

Run: `flock build/.ninja_lock ninja -C build unit_tests_dbms > build/build_a1b.log 2>&1 && build/src/unit_tests_dbms --gtest_filter='CASRequestsConnectHint.*' > build/test_a1.log 2>&1; tail -3 build/test_a1.log`
Expected: `[  PASSED  ] 2 tests.`

### Task A2: Prove the fake S3 client carries the text to the caller

**Files:**
- Test: `src/IO/tests/gtest_writebuffer_s3.cpp`

- [ ] **Step 1: Add an injection model and the test**

After `PutObjectPreconditionFailedIngection` (line ~558):

```cpp
/// A transport failure shaped as `PocoHTTPClient` shapes one: the S3 error is `NETWORK_CONNECTION`
/// and the message is the Poco text, exception name empty.
struct PutObjectNetworkTextIngection: InjectionModel
{
    explicit PutObjectNetworkTextIngection(std::string text_) : text(std::move(text_)) {}
    std::optional<Aws::S3::Model::PutObjectOutcome> call(const Aws::S3::Model::PutObjectRequest & /*request*/) override
    {
        return Aws::Client::AWSError<Aws::Client::CoreErrors>(Aws::Client::CoreErrors::NETWORK_CONNECTION, "", text, false);
    }
    std::string text;
};
```

After `TEST_P(SyncAsync, PreconditionFailedNeverLogsAtError)`:

```cpp
TEST_F(WBS3Test, NetworkConnectionTextSurvives)
{
    for (const char * text : {"Cannot assign requested address", "Connection refused", "No route to host",
                              "Network is unreachable", "connect timed out"})
    {
        setInjectionModel(std::make_shared<MockS3::PutObjectNetworkTextIngection>(text));
        WriteSettings write_settings;
        write_settings.object_storage_retry_profile = ObjectStorageRetryProfile::SingleAttempt;
        write_settings.s3_max_unexpected_write_error_retries_override = 1;
        try
        {
            auto buffer = getWriteBuffer("network_text", write_settings);
            buffer->write('A');
            getAsyncPolicy().setAutoExecute(true);
            buffer->finalize();
            FAIL() << "the injected failure must surface";
        }
        catch (const DB::S3Exception & e)
        {
            EXPECT_EQ(e.getS3ErrorCode(), Aws::S3::S3Errors::NETWORK_CONNECTION) << text;
            EXPECT_THAT(e.message(), testing::HasSubstr(text));
        }
    }
}
```

- [ ] **Step 2: Build and run; expect PASS (this test pins existing behaviour)**

Run: `flock build/.ninja_lock ninja -C build unit_tests_dbms > build/build_a2.log 2>&1 && build/src/unit_tests_dbms --gtest_filter='WBS3Test.NetworkConnectionTextSurvives' > build/test_a2.log 2>&1; tail -3 build/test_a2.log`
Expected: PASS. If it fails on the error-code mapping, `WriteBufferFromS3::makeSinglepartUpload`'s rethrow (line ~714) is the site to read; do not change `src/IO` for it — report.

### Task A3: The hint path in `writeLoop`

**Files:**
- Modify: `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h` (`pauseFlat` declaration next to `pauseForConflict`, line ~381)
- Modify: `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.cpp` (`writeLoop` line ~830–990, new `pauseFlat` after `pauseForConflict` line ~830)
- Modify: `src/Common/ProfileEvents.cpp` (new event after `CASRequestFenceLostPostWrite`, line ~949)
- Test: `src/Disks/tests/gtest_cas_requests.cpp`

**Interfaces:**
- Produces: `std::optional<WriteResult> CasOperation::pauseFlat(WriteState & state, const Retry::Bound & bound);` — admission with `reservedFor(kConnectHintPauseMs, 2)`, records `CASRequestReissue`, sleeps `kConnectHintPauseMs` (50), leaves `state.reissues` untouched. ProfileEvent `CASRequestConnectFailureHint`.

- [ ] **Step 1: Write the six engine tests**

Append to `gtest_cas_requests.cpp` inside `#if USE_AWS_S3`:

```cpp
namespace
{
std::exception_ptr connectHint()
{
    return std::make_exception_ptr(DB::S3Exception(
        "Poco::Exception. Code: 1000, e.code() = 99, Cannot assign requested address: 10.0.0.1:9000",
        Aws::S3::S3Errors::NETWORK_CONNECTION));
}
}

TEST(CASRequestsConnectHint, HintedFailuresReissueWithoutARead)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    backend->failNextWriteWith("k", connectHint());
    backend->failNextWriteWith("k", connectHint());
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();

    WriteResult result = op.create("k", "v", Retry::standard());
    const auto * committed = std::get_if<Committed>(&result);
    ASSERT_NE(committed, nullptr);
    EXPECT_EQ(committed->attempts_sent, 3u);
    EXPECT_FALSE(committed->resolved_by_read);
    EXPECT_EQ(backend->writeTotal(), 3u);
    EXPECT_EQ(backend->getTotal(), 0u);                 /// no settle read before the commit
    ASSERT_EQ(clock.sleeps.size(), 2u);
    EXPECT_EQ(clock.sleeps[0], 50u);                    /// the flat pause, twice
    EXPECT_EQ(clock.sleeps[1], 50u);
}

TEST(CASRequestsConnectHint, ReissueMeetsPreconditionAndAdoptsOwnBytes)
{
    /// The hint was false: the write landed, its response was lost. The reissue meets 412, one read
    /// follows and proves the bytes are ours.
    {
        FakeClock clock;
        auto backend = std::make_shared<CountingBackend>();
        auto requests = makeRequests(backend, clock);
        auto op = requests.admit();
        const Etag seen = *orThrow(op.create("k", "v1", Retry::standard()), "create");
        backend->resetCounts();
        backend->injectAmbiguousLandedWrite("k");     /// lands, then throws
        WriteResult result = op.replace("k", "v2", seen, Retry::standard());
        const auto * committed = std::get_if<Committed>(&result);
        ASSERT_NE(committed, nullptr);
        EXPECT_TRUE(committed->resolved_by_read);
        EXPECT_EQ(backend->getTotal(), 1u);
    }
    /// Different ETag, other bytes: a conflict, as today.
    {
        FakeClock clock;
        auto backend = std::make_shared<CountingBackend>();
        auto requests = makeRequests(backend, clock);
        auto op = requests.admit();
        const Etag seen = *orThrow(op.create("k", "v1", Retry::standard()), "create");
        backend->failNextWriteWith("k", connectHint());
        /// A competitor lands during the flat pause: the engine's own sleep is the seam.
        bool competitor_landed = false;
        requests.setSleepFnForTest([&](uint64_t ms)
        {
            clock.sleepFn()(ms);
            if (!competitor_landed)
            {
                competitor_landed = true;
                auto other = requests.admit();
                orThrow(other.replace("k", "theirs", seen, Retry::standard()), "competitor");
            }
        });
        WriteResult result = op.replace("k", "v2", seen, Retry::standard());
        EXPECT_TRUE(std::holds_alternative<Conflict>(result));
        EXPECT_EQ(backend->getTotal(), 1u);
    }
    /// The ORIGINAL ETag is still current after the hinted attempt: the reissue simply commits.
    {
        FakeClock clock;
        auto backend = std::make_shared<CountingBackend>();
        auto requests = makeRequests(backend, clock);
        auto op = requests.admit();
        const Etag seen = *orThrow(op.create("k", "v1", Retry::standard()), "create");
        backend->resetCounts();
        backend->failNextWriteWith("k", connectHint());
        WriteResult result = op.replace("k", "v2", seen, Retry::standard());
        const auto * committed = std::get_if<Committed>(&result);
        ASSERT_NE(committed, nullptr);
        EXPECT_FALSE(committed->resolved_by_read);
        EXPECT_EQ(committed->attempts_sent, 2u);
        EXPECT_EQ(backend->getTotal(), 0u);
    }
}

TEST(CASRequestsConnectHint, OnceKeepsOneWriteAndOneRead)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    backend->failNextWriteWith("k", connectHint());
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    WriteResult result = op.create("k", "v", Retry::once());
    const auto * gave_up = std::get_if<GaveUp>(&result);
    ASSERT_NE(gave_up, nullptr);
    EXPECT_EQ(gave_up->why, GaveUp::Why::Unresolved);
    EXPECT_EQ(backend->writeTotal(), 1u);
    EXPECT_EQ(backend->getTotal(), 1u);
    EXPECT_TRUE(clock.sleeps.empty());
}

TEST(CASRequestsConnectHint, EarlierAmbiguityStillSettlesByRead)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    backend->injectAmbiguousWrite("k");           /// attempt 1: ordinary ambiguity -> read, backoff
    backend->failNextWriteWith("k", connectHint()); /// attempt 2: hinted -> flat pause, no read
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    WriteResult result = op.create("k", "v", Retry::standard());
    const auto * committed = std::get_if<Committed>(&result);
    ASSERT_NE(committed, nullptr);
    EXPECT_EQ(committed->attempts_sent, 3u);
    EXPECT_EQ(backend->getTotal(), 1u);
    ASSERT_EQ(clock.sleeps.size(), 2u);
    EXPECT_EQ(clock.sleeps[1], 50u);
}

TEST(CASRequestsConnectHint, GatesRefuseTheReissue)
{
    /// Deadline: hints until the window closes.
    {
        FakeClock clock;
        auto backend = std::make_shared<CountingBackend>();
        for (int i = 0; i < 100; ++i)
            backend->failNextWriteWith("k", connectHint());
        auto requests = makeRequests(backend, clock);
        requests.setAttemptReservationForTest(1'000);
        auto op = requests.admit();
        WriteResult result = op.create("k", "v", Retry::within(3'000));
        const auto * gave_up = std::get_if<GaveUp>(&result);
        ASSERT_NE(gave_up, nullptr);
        EXPECT_EQ(gave_up->why, GaveUp::Why::Deadline);
        EXPECT_TRUE(gave_up->sent_any);
        EXPECT_EQ(backend->getTotal(), 0u);
    }
    /// Fence: the fence trips during the pause.
    {
        FakeClock clock;
        auto backend = std::make_shared<CountingBackend>();
        backend->failNextWriteWith("k", connectHint());
        bool lost = false;
        Fence fence{
            [] { return uint64_t{1}; },
            [&](uint64_t, uint64_t) { return lost ? Fence::Admit::LostOrRearmed : Fence::Admit::Ok; },
            [](uint64_t) {}};
        auto requests = makeRequests(backend, clock, fence);
        requests.setSleepFnForTest([&](uint64_t ms) { clock.sleepFn()(ms); lost = true; });
        auto op = requests.admit();
        WriteResult result = op.create("k", "v", Retry::standard());
        const auto * gave_up = std::get_if<GaveUp>(&result);
        ASSERT_NE(gave_up, nullptr);
        EXPECT_EQ(gave_up->why, GaveUp::Why::FenceLost);
    }
    /// The documented deadline-edge difference: exactly one envelope left -> a hinted attempt gives up
    /// (today an ambiguous one would still spend its read).
    {
        FakeClock clock;
        auto backend = std::make_shared<CountingBackend>();
        backend->failNextWriteWith("k", connectHint());
        auto requests = makeRequests(backend, clock);
        requests.setAttemptReservationForTest(1'000);
        auto op = requests.admit();
        WriteResult result = op.create("k", "v", Retry::within(2'000 + 1'000));  /// two envelopes for the attempt, one left after it
        const auto * gave_up = std::get_if<GaveUp>(&result);
        ASSERT_NE(gave_up, nullptr);
        EXPECT_EQ(gave_up->why, GaveUp::Why::Deadline);
        EXPECT_TRUE(gave_up->sent_any);
        EXPECT_EQ(backend->getTotal(), 0u);
    }
}

TEST(CASRequestsConnectHint, AmbiguityAfterHintsStartsAtFirstBackoff)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    backend->failNextWriteWith("k", connectHint());
    backend->failNextWriteWith("k", connectHint());
    backend->failNextWriteWith("k", std::make_exception_ptr(Poco::TimeoutException("the write timed out")));
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    WriteResult result = op.create("k", "v", Retry::standard());
    ASSERT_TRUE(std::holds_alternative<Committed>(result));
    ASSERT_EQ(clock.sleeps.size(), 3u);
    EXPECT_EQ(clock.sleeps[0], 50u);
    EXPECT_EQ(clock.sleeps[1], 50u);
    /// `backoff(1)` is full jitter over [0, 200] ms (`CasRetry.h`): the hints did not advance the index.
    EXPECT_LE(clock.sleeps[2], 200u);
}
```

- [ ] **Step 2: Build and run; expect the six to FAIL on counts (reads > 0, sleeps ≠ 50)**

Run: `flock build/.ninja_lock ninja -C build unit_tests_dbms > build/build_a3.log 2>&1 && build/src/unit_tests_dbms --gtest_filter='CASRequestsConnectHint.*' > build/test_a3.log 2>&1; grep -c FAILED build/test_a3.log`
Expected: at least 5 failures (`OnceKeepsOneWriteAndOneRead` may already pass).

- [ ] **Step 3: Add the event, `pauseFlat`, and the branch**

`ProfileEvents.cpp`, after `CASRequestFenceLostPostWrite`:

```cpp
    M(CASRequestConnectFailureHint, "Number of CAS write attempts whose transport error named a failed connection (no free local port, refused or unreachable peer, connect timeout). The engine reissued them after a flat pause without a settle read. Growth means the server cannot open connections to the object store.", ValueType::Number) \
```

`CasRequests.cpp`: add `extern const Event CASRequestConnectFailureHint;` to the `ProfileEvents` block; add after `pauseForConflict`:

```cpp
/// A flat pause before reissuing an attempt whose failure text named a failed connection.
static constexpr uint64_t kConnectHintPauseMs = 50;

std::optional<WriteResult> CasOperation::pauseFlat(WriteState & state, const Retry::Bound & bound)
{
    const uint64_t needed = reservedFor(kConnectHintPauseMs, 2);
    switch (gate(needed))
    {
        case Gate::FenceLost: return gaveUp(GaveUp::Why::FenceLost, sourceFor(bound), state);
        case Gate::NoBudget:  return gaveUp(GaveUp::Why::Deadline, GaveUp::Source::Lease, state);
        case Gate::Ok: break;
    }
    if (!fits(needed, bound))
        return gaveUp(GaveUp::Why::Deadline, sourceFor(bound), state);
    detail::recordReissue();
    owner.sleep_ms(kConnectHintPauseMs);
    return std::nullopt;
}
```

In `writeLoop`: declare `bool connect_hint = false;` next to `bool credential_answer = false;`; in the `catch (const Exception & e)` arm, after the refusal check, add `connect_hint = isConnectFailureHint(e);`. After the block `if (refreshed && !policy.single_attempt && !state.any_ambiguous) {...}` insert:

```cpp
        /// The failure text named a failed CONNECTION. A read now would meet the same broken condition,
        /// so the reissue itself is the cheaper probe: the attempt stays ambiguous (`any_ambiguous` is
        /// set above), and if the reissue meets a refused precondition the read below settles it.
        if (connect_hint && !policy.single_attempt)
        {
            ProfileEvents::increment(ProfileEvents::CASRequestConnectFailureHint);
            if (auto given_up = pauseFlat(state, bound))
                return *given_up;
            continue;
        }
```

`CasRequests.h`: declare `std::optional<WriteResult> pauseFlat(WriteState & state, const Retry::Bound & bound);` after `pauseForConflict` with a two-line comment (flat pause, `state.reissues` untouched).

- [ ] **Step 4: Build and run the whole CAS gate**

Run: `flock build/.ninja_lock ninja -C build unit_tests_dbms > build/build_a3b.log 2>&1 && build/src/unit_tests_dbms --gtest_filter='CAS*' > build/test_a3_gate.log 2>&1; tail -3 build/test_a3_gate.log`
Expected: all PASS, including the six new tests.

### Task A4: Renewal twin

**Files:**
- Test: `src/Disks/tests/gtest_cas_heartbeat.cpp` (`RenewalScriptBackend` line ~82, tests after line ~740)

- [ ] **Step 1: Add the action and the test**

In `RenewalScriptBackend::Action` add `ThrowConnectHint`; in `write`, before the `ThrowBefore` branch:

```cpp
        if (action == Action::ThrowConnectHint)
        {
#if USE_AWS_S3
            throw DB::S3Exception("Poco::Exception. Code: 1000, e.code() = 99, Cannot assign requested address: 10.0.0.1:9000",
                                  Aws::S3::S3Errors::NETWORK_CONNECTION);
#else
            throw Poco::TimeoutException("connect timed out");
#endif
        }
```

Test (under `#if USE_AWS_S3`):

```cpp
TEST(CASHeartbeat, RenewalOverConnectFailuresRecoversWithoutASettleRead)
{
    auto backend = std::make_shared<RenewalScriptBackend>();
    Layout layout("pool");
    const String srid = "test";
    const UInt128 uuid{0x1234};
    uint64_t wall_ms = 1000;
    uint64_t boot_ms = 100;
    Ops ops(backend, &boot_ms);
    seedOwnClaim(ops.op, layout, srid, uuid, 9, wall_ms, 30000);
    MountLeaseRenewer renewer(
        ops.mount, ops.farewell, layout, srid, uuid, 9, std::chrono::milliseconds(30000),
        [&] { return wall_ms; }, [] { return uint64_t{7}; }, {}, std::chrono::milliseconds(2000),
        [&] { return boot_ms; });
    renewer.start();

    backend->attempts.clear();
    backend->read_calls = 0;
    /// Three seconds of "no free port" at 50 ms per hint, then the store answers.
    for (int i = 0; i < 60; ++i)
        backend->actions.push_back(RenewalScriptBackend::Action::ThrowConnectHint);
    backend->actions.push_back(RenewalScriptBackend::Action::Delegate);
    const MountRenewResult renewed = renewer.renew(renewalEnvironment(boot_ms));
    ASSERT_EQ(renewed.outcome, MountRenewOutcome::Committed);
    EXPECT_GT(renewed.attempts_sent, 1u);
    EXPECT_FALSE(renewed.resolved_by_read);            /// classification `committed_after_retry`
    EXPECT_EQ(backend->read_calls, 0u);
    EXPECT_EQ(backend->attempts.size(), 61u);
    for (const auto & attempt : backend->attempts)
        EXPECT_EQ(attempt.bytes, backend->attempts.front().bytes);
}
```

`Ops` advances `boot_ms` by every engine sleep (`sleep_step_ms == 0`), so 60 × 50 ms = 3 s of virtual time inside a 30 s lease.

- [ ] **Step 2: Build and run**

Run: `flock build/.ninja_lock ninja -C build unit_tests_dbms > build/build_a4.log 2>&1 && build/src/unit_tests_dbms --gtest_filter='CASHeartbeat.RenewalOverConnectFailures*' > build/test_a4.log 2>&1; tail -3 build/test_a4.log`
Expected: PASS (A3 landed; this test would have failed before A3 with `read_calls == 60`).

### Task A5: Texts, comment, docs, commit spec 1

**Files:**
- Modify: `src/Common/ProfileEvents.cpp` lines ~944 (`CASRequestReissue`) and ~946 (`CASRequestResolveRead`)
- Modify: `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h` line ~356
- Modify: `docs/en/antalya/cas/architecture/mounts-and-leases.md` line ~77

- [ ] **Step 1: Rewrite the three texts**

`CASRequestReissue`: `"Number of CAS requests re-sent after a failed or ambiguous attempt. The pause before a reissue is a jittered backoff, a flat pause, or none. Growth means the object store is throttling, failing, or contended, or connections to it cannot be opened."`

`CASRequestResolveRead`: `"Number of exact settlement reads the CAS request contract made: a body read, or a HEAD where the caller needs only presence, to settle a refused precondition or an ambiguous write. Under a reissuing policy a connect-failure hint defers the read until a later outcome requires it."`

`CasRequests.h` contract comment:

```cpp
    /// The write engine: one call, any policy. `Committed` and `Conflict` are proven by an exact read
    /// or by the reissue's own 2xx before they are reported; an attempt whose transport error named a
    /// failed connection is reissued before its read. `Refused`, `Declined` and `GaveUp` report what
    /// the store or the bounds said.
```

`mounts-and-leases.md` "Resolve before retry" bullet: `A transient or ambiguous conditional \`PUT\` is followed by one exact \`GET\`, except that an attempt whose transport error names a failed connection is reissued first after a flat pause and settled by the reissue's own answer (a 2xx) or by the exact \`GET\` that follows its \`412\`.` Keep the rest of the bullet.

- [ ] **Step 2: Commit spec 1**

```bash
cat > tmp/msg_spec1.txt <<'EOF'
cas: reissue a connect-failure-hinted write without a preceding settle read

A conditional write whose transport error names a failed connection (no free local port, refused
or unreachable peer, connect timeout) is reissued after a flat 50 ms pause instead of spending an
exact read against the same broken condition. The attempt stays ambiguous; a reissue that meets a
refused precondition is settled by the existing read. Renewal under ephemeral-port exhaustion
recovers within its window instead of burning the lease-safe reserve on reads that cannot connect.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
EOF
git commit -F tmp/msg_spec1.txt -- src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.cpp src/Common/ProfileEvents.cpp src/Disks/tests/gtest_cas_requests.cpp src/Disks/tests/gtest_cas_heartbeat.cpp src/IO/tests/gtest_writebuffer_s3.cpp docs/en/antalya/cas/architecture/mounts-and-leases.md
```

- [ ] **Step 3: Codex review of the commit; iterate until no MAJOR**

Write `tmp/pr2300-cicd-watch/review/prompt_impl1.md` naming the spec, the commit SHA (`git show --stat HEAD`) and asking for MAJOR/MINOR/NIT with concrete fixes; run `codex exec -m gpt-5.6-sol -c model_reasoning_effort=high --sandbox read-only - < tmp/pr2300-cicd-watch/review/prompt_impl1.md > tmp/pr2300-cicd-watch/review/codex_impl1.out 2>&1`. Fix MAJORs in a follow-up commit with explicit paths; re-run until `NO MAJOR FINDINGS`.

---

## Part B — Spec 4: engine reissues cooperate with the adaptive first-attempt timeout, one attempt envelope

### Task B1: The attempt seed reaches every CAS control request

**Files:**
- Modify: `src/IO/ReadSettings.h` (after line 170), `src/IO/WriteSettings.h` (after line 86)
- Modify: `src/IO/S3/Requests.h` (after `setClickhouseAttemptNumber`, line ~266)
- Modify: `src/IO/ReadBufferFromS3.cpp` line 568, `src/IO/WriteBufferFromS3.cpp` `getPutRequest` line ~728
- Modify: `src/Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.cpp` (`S3IteratorAsync` line ~152–245, `removeObjectIfTokenMatchesImpl` ~580, `removeObjectsIfExistImpl` ~636, `tryGetObjectMetadataWithNativeToken` ~820)
- Test: `src/IO/tests/gtest_writebuffer_s3.cpp`

**Interfaces:**
- Produces: `size_t ReadSettings::object_storage_attempt_number = 0;` and `size_t WriteSettings::object_storage_attempt_number = 0;` (0 = unset); `inline size_t DB::S3::seededAttemptNumber(size_t seed, size_t local) { return (seed == 0 ? 1 : seed) + local - 1; }` in `Requests.h`.
- Produces: `S3IteratorAsync(bucket, prefix, client, max_keys, with_tags, start_after, size_t attempt_seed = 0)`; `removeObjectIfTokenMatchesImpl(object, etag, client, size_t attempt_seed)`; `removeObjectsIfExistImpl(objects, client, size_t attempt_seed)`.

- [ ] **Step 1: Write the three seed tests against the mock client**

In `gtest_writebuffer_s3.cpp`, extend `MockS3::Client` (line ~239) with a recorder and a `ListObjectsV2` override:

```cpp
    mutable std::vector<size_t> attempts_seen;   /// `clickhouse-request` attempt of every verb, in order

    Aws::S3::Model::ListObjectsV2Outcome ListObjectsV2(const Aws::S3::Model::ListObjectsV2Request & request) const override
    {
        attempts_seen.push_back(DB::S3::getClickhouseAttemptNumber(request));
        auto & bucket = store->GetBucketStore(request.GetBucket());
        Aws::S3::Model::ListObjectsV2Result result;
        result.SetPrefix(request.GetPrefix());
        int emitted = 0;
        std::string last;
        const std::string after = request.ContinuationTokenHasBeenSet() ? request.GetContinuationToken()
                                : request.StartAfterHasBeenSet() ? request.GetStartAfter() : "";
        for (const auto & [key, data] : bucket.objects)
        {
            if (!key.starts_with(request.GetPrefix()) || key <= after)
                continue;
            if (emitted == request.GetMaxKeys())
            {
                result.SetIsTruncated(true);
                result.SetNextContinuationToken(last);
                break;
            }
            Aws::S3::Model::Object object;
            object.SetKey(key);
            object.SetSize(static_cast<long long>(data.size()));
            result.AddContents(std::move(object));
            last = key;
            ++emitted;
        }
        return Aws::S3::Model::ListObjectsV2Outcome(std::move(result));
    }
```

and record `attempts_seen.push_back(DB::S3::getClickhouseAttemptNumber(request));` as the first line of the existing `PutObject`, `GetObject`, `HeadObject`, `DeleteObject` overrides (add a `DeleteObjects` override that records and deletes each key). Then the tests:

```cpp
TEST_F(WBS3Test, S3RequestAttemptSeedReadHeaderSequence)
{
    getSettings()[Setting::s3_max_single_read_retries] = 2;   /// one local retry
    client->attempts_seen.clear();
    /// A read whose first attempt fails: an unset seed sends [1, 2]; seed 2 sends [2, 3].
    for (const auto [seed, first, second] : {std::tuple<size_t, size_t, size_t>{0, 1, 2}, {2, 2, 3}})
    {
        client->attempts_seen.clear();
        setInjectionModel(std::make_shared<MockS3::GetObjectFailOnceIngection>());
        ReadSettings read_settings;
        read_settings.object_storage_attempt_number = seed;
        S3::S3RequestSettings request_settings;
        request_settings[S3RequestSetting::max_single_read_retries] = 2;
        ReadBufferFromS3 buffer(client, bucket, "seeded", "", request_settings, read_settings);
        std::string out;
        readStringUntilEOF(out, buffer);
        ASSERT_EQ(client->attempts_seen.size(), 2u);
        EXPECT_EQ(client->attempts_seen[0], first);
        EXPECT_EQ(client->attempts_seen[1], second);
    }
}

TEST_F(WBS3Test, S3RequestAttemptSeedPutHeadDeleteCarryTheSeed)
{
    WriteSettings write_settings;
    write_settings.object_storage_attempt_number = 3;
    client->attempts_seen.clear();
    {
        auto buffer = getWriteBuffer("seeded_put", write_settings);
        buffer->write('A');
        getAsyncPolicy().setAutoExecute(true);
        buffer->finalize();
    }
    ASSERT_FALSE(client->attempts_seen.empty());
    EXPECT_EQ(client->attempts_seen.front(), 3u);
    /// Seed 0 adds no header: the attempt number reads back as 1 (the SDK's default).
    client->attempts_seen.clear();
    {
        auto buffer = getWriteBuffer("unseeded_put");
        buffer->write('A');
        getAsyncPolicy().setAutoExecute(true);
        buffer->finalize();
    }
    EXPECT_EQ(client->attempts_seen.front(), 1u);
}

TEST_F(WBS3Test, S3RequestAttemptSeedListPagesCarryTheSeed)
{
    auto & bucket_store = client->store->GetBucketStore(bucket);
    for (int i = 0; i < 5; ++i)
        bucket_store.PutObject(fmt::format("p/{}", i), "x");
    client->attempts_seen.clear();
    auto iterator = std::make_shared<S3IteratorAsync>(bucket, "p/", client, /*max_list_size=*/2, /*with_tags=*/false,
                                                      std::optional<std::string>("p/0"), /*attempt_seed=*/2);
    size_t seen = 0;
    for (; iterator->isValid(); iterator->next())
        ++seen;
    EXPECT_EQ(seen, 4u);
    ASSERT_EQ(client->attempts_seen.size(), 2u);   /// the initial page and one rebuilt page
    EXPECT_EQ(client->attempts_seen[0], 2u);
    EXPECT_EQ(client->attempts_seen[1], 2u);
}
```

`S3IteratorAsync` is in an anonymous namespace of `S3ObjectStorage.cpp`; move its class body to a new header `src/Disks/DiskObjectStorage/ObjectStorages/S3/S3IteratorAsync.h` (same content, `namespace DB`) so the test can construct it. `GetObjectFailOnceIngection`: add an `InjectionModel` whose first `GetObject` call returns `AWSError<CoreErrors>(CoreErrors::NETWORK_CONNECTION, "", "Timeout", /*retryable=*/true)` and later calls return `std::nullopt` (the mock's `GetObject` must consult the injection model first; add that if missing, mirroring `PutObject`).

- [ ] **Step 2: Build, expect compile failures on the missing fields / seed parameter**

Run: `flock build/.ninja_lock ninja -C build unit_tests_dbms > build/build_b1.log 2>&1; grep -m3 "error:" build/build_b1.log`

- [ ] **Step 3: Add the fields and apply the seed**

`ReadSettings.h` after `object_storage_attempt_timeout_ms`:
```cpp
    /// The caller's own attempt number for the request built from these settings, 1-based; 0 leaves the
    /// buffer's own numbering. A CAS reissue passes its count so the HTTP client sees attempt ≥ 2.
    size_t object_storage_attempt_number = 0;
```
Same field and comment in `WriteSettings.h`. `Requests.h`:
```cpp
/// The attempt number a request carries when its caller seeded one: the caller's attempt for the first
/// local try, then the local counter's increments. Seed 0 is "unseeded" and yields `local`.
inline size_t seededAttemptNumber(size_t seed, size_t local)
{
    return (seed == 0 ? 1 : seed) + local - 1;
}
```
`ReadBufferFromS3.cpp:568`: `S3::setClickhouseAttemptNumber(req, S3::seededAttemptNumber(read_settings.object_storage_attempt_number, attempt));`
`WriteBufferFromS3::getPutRequest`, after `req.SetContentType(...)`:
```cpp
    if (write_settings.object_storage_attempt_number != 0)
        S3::setClickhouseAttemptNumber(req, write_settings.object_storage_attempt_number);
```
`S3IteratorAsync`: new member `const size_t attempt_seed;`, constructor parameter; after `request->SetMaxKeys(...)` and again after building `paginated_request`: `if (attempt_seed != 0) S3::setClickhouseAttemptNumber(*request, attempt_seed);`.
`removeObjectIfTokenMatchesImpl`, `removeObjectsIfExistImpl`, and the HEAD in `tryGetObjectMetadataWithNativeToken` (the `S3::HeadObjectRequest` built for the profile-aware path): take `size_t attempt_seed` and call `S3::setClickhouseAttemptNumber(request, attempt_seed)` when nonzero. The legacy overloads pass 0. The profile-aware overloads pass a seed that Task B3 supplies (until B3 they pass 0).

- [ ] **Step 4: Build and run the three tests plus the existing WBS3 suite**

Run: `flock build/.ninja_lock ninja -C build unit_tests_dbms > build/build_b1b.log 2>&1 && build/src/unit_tests_dbms --gtest_filter='WBS3Test.*:SyncAsync*' > build/test_b1.log 2>&1; tail -3 build/test_b1.log`
Expected: all PASS.

### Task B2: One attempt envelope, everywhere

**Files:**
- Modify: `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequestBudget.h` / `.cpp`
- Modify: `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h` (~238), `CasObjectStorageBackend.h` (~71, ~105, ~201), `CasObjectStorageBackend.cpp`, `CasRequests.cpp` (~245)
- Modify: `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.cpp` (~785–800, ~986)
- Modify: `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.cpp` (~85–110, ~836–848, ~1004, ~1160, ~1498–1512), `Pool/CasMountRuntime.cpp` (~154)
- Modify: `src/Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.h` (~204, ~222, ~258), `.cpp` (~1117–1156)
- Modify: `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedSettings.cpp` (~83–84, validation ~244)
- Test: `src/Disks/tests/gtest_cas_requests.cpp`, new `src/Disks/tests/gtest_cas_s3_client_profile.cpp`, `src/Disks/tests/gtest_cas_mount_runtime.cpp` (~145), `src/Disks/tests/gtest_cas_pool.cpp`, `src/Disks/tests/gtest_cas_heartbeat.cpp`

**Interfaces:**
- Produces: `std::optional<uint64_t> CasRequestBudget::connect_timeout_cap_ms` (default 1000) and `uint64_t CasRequestBudget::attemptEnvelopeMs() const` (saturating `attempt_timeout_ms + 2 × connect_timeout_cap_ms`, `nullopt` adds nothing); `validateCasRequestBudget(budget, ttl, period, bool background_renewal)`; `virtual uint64_t Backend::attemptEnvelopeMs() const { return attemptTimeoutMs(); }`; `ObjectStorageBackend(object_storage, mode, single_attempt_control_plane, attempt_timeout_ms, connect_timeout_cap_ms = 0)` with `connectTimeoutCapMs()`; `S3ObjectStorage::getSingleAttemptClient(uint64_t request_timeout_ms, uint64_t connect_timeout_cap_ms)` cached by the pair; `S3ObjectStorage::clientForRetryProfile(profile, request_timeout_ms, connect_timeout_cap_ms)` (the profile-aware public overloads gain the cap parameter with default 0 until Task B3 replaces the pair by the context).

- [ ] **Step 1: Write the budget test**

Append to `gtest_cas_requests.cpp`:

```cpp
TEST(CASRequestBudget, EnvelopeIsValidatedNotTheBareAttempt)
{
    CasRequestBudget budget{.attempt_timeout_ms = 5000, .lease_safety_margin_ms = 2000, .connect_timeout_cap_ms = 1000};
    EXPECT_EQ(budget.attemptEnvelopeMs(), 7000u);
    EXPECT_EQ(CasRequestBudget{.attempt_timeout_ms = 5000, .lease_safety_margin_ms = 2000, .connect_timeout_cap_ms = std::nullopt}.attemptEnvelopeMs(), 5000u);
    /// Defaults with the default TTL / period are accepted.
    EXPECT_NO_THROW(validateCasRequestBudget(budget, 30000, 10000, /*background_renewal=*/true));
    /// A zero attempt timeout would reserve nothing while the request keeps the disk's own timeout.
    EXPECT_THROW(validateCasRequestBudget(CasRequestBudget{.attempt_timeout_ms = 0, .lease_safety_margin_ms = 2000,
                                                           .connect_timeout_cap_ms = std::nullopt}, 30000, 10000, true),
                 DB::Exception);
    /// The old inequality (attempt <= TTL - margin - period: 5000 <= 13000) accepted this; two envelopes
    /// of 15 s do not fit a 25 s lease behind a 10 s period and a 2 s margin.
    const CasRequestBudget wide{.attempt_timeout_ms = 5000, .lease_safety_margin_ms = 2000, .connect_timeout_cap_ms = 5000};
    try
    {
        validateCasRequestBudget(wide, 25000, 10000, true);
        FAIL() << "must refuse";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_THAT(e.message(), testing::HasSubstr("envelope"));
        EXPECT_THAT(e.message(), testing::HasSubstr("15000"));
    }
    /// Without background renewal only `envelope + margin < TTL` applies (15000 + 2000 < 25000).
    EXPECT_NO_THROW(validateCasRequestBudget(wide, 25000, 10000, /*background_renewal=*/false));
    /// Saturation: absurd values fail closed rather than wrap.
    EXPECT_THROW(validateCasRequestBudget(CasRequestBudget{.attempt_timeout_ms = std::numeric_limits<uint64_t>::max(),
                                                           .lease_safety_margin_ms = 1, .connect_timeout_cap_ms = 1},
                                          30000, 10000, true), DB::Exception);
}

TEST(CASRequests, ReservationIsTheEnvelope)
{
    struct EnvelopeBackend : InMemoryBackend
    {
        uint64_t attemptTimeoutMs() const override { return 5000; }
        uint64_t attemptEnvelopeMs() const override { return 7000; }
    };
    FakeClock clock;
    auto backend = std::make_shared<EnvelopeBackend>();
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    /// A write reserves two envelopes: 14 s fits a 14 s window, 13.999 s does not.
    EXPECT_TRUE(std::holds_alternative<Committed>(op.create("k", "v", Retry::within(14'000))));
    const WriteResult refused = op.create("k2", "v", Retry::within(13'999));
    const auto * gave_up = std::get_if<GaveUp>(&refused);
    ASSERT_NE(gave_up, nullptr);
    EXPECT_FALSE(gave_up->sent_any);
}
```

New file `src/Disks/tests/gtest_cas_s3_client_profile.cpp` (guarded by `#if USE_AWS_S3`), constructing an `S3ObjectStorage` over the `MockS3`-style client is not possible from `src/Disks/tests`; use a real client from `DB::S3::ClientFactory::instance().create(...)` exactly as `observeConditionalPut` does (`src/IO/S3/tests/gtest_aws_s3_client.cpp` ~455–490, without the server: `endpointOverride = "http://127.0.0.1:1"`), then:

```cpp
TEST(S3SingleAttemptClient, ConnectTimeoutIsCappedAndFrozen)
{
    auto make_storage = [](long connect_ms)
    {
        DB::S3::PocoHTTPClientConfiguration cfg = clientConfigurationForTest();   /// helper in this file, see above
        cfg.connectTimeoutMs = connect_ms;
        cfg.requestTimeoutMs = 30000;
        auto client = DB::S3::ClientFactory::instance().create(cfg, clientSettingsForTest(), "ACCESS_KEY_ID", "SECRET_ACCESS_KEY",
                                                               "", {}, {}, DB::S3::CredentialsConfiguration{});
        return std::make_shared<DB::S3ObjectStorage>(std::move(client), std::make_unique<DB::S3Settings>(),
                                                     DB::S3::URI("http://127.0.0.1:1/bucket/"), DB::S3Capabilities{},
                                                     nullptr, "disk");
    };
    auto storage = make_storage(20000);
    auto clone = storage->getSingleAttemptClient(/*request_timeout_ms=*/5000, /*connect_timeout_cap_ms=*/5000);
    EXPECT_EQ(clone->getClientConfiguration().connectTimeoutMs, 5000);
    EXPECT_EQ(clone->getClientConfiguration().requestTimeoutMs, 5000);

    auto narrow = make_storage(1000);
    EXPECT_EQ(narrow->getSingleAttemptClient(5000, 5000)->getClientConfiguration().connectTimeoutMs, 1000);
    /// A base of 0 means unbounded to Poco: it resolves to the cap, never to "no limit".
    EXPECT_EQ(make_storage(0)->getSingleAttemptClient(5000, 1000)->getClientConfiguration().connectTimeoutMs, 1000);
    /// Two caps under one request timeout are two clones: the cache key is the pair.
    EXPECT_NE(narrow->getSingleAttemptClient(5000, 1000).get(), narrow->getSingleAttemptClient(5000, 500).get());

    /// The reload path replaces the base client with a wider connect timeout; a clone rebuilt for the
    /// frozen cap 1000 stays at 1000.
    auto reloaded = make_storage(1000);
    (void)reloaded->getSingleAttemptClient(5000, 1000);
    reloaded->setClientForTest(make_storage(5000)->getS3StorageClient());
    EXPECT_EQ(reloaded->getSingleAttemptClient(5000, 1000)->getClientConfiguration().connectTimeoutMs, 1000);
}
```

`setClientForTest` is a new one-line test hook on `S3ObjectStorage` (`client->set(std::make_unique<S3::Client>(*new_client))` is not possible on a const client; instead expose `void setClientForTest(std::unique_ptr<S3::Client> &&)` and have the test build the second client directly rather than via `getS3StorageClient`).

`gtest_cas_mount_runtime.cpp`: rename `RefAppendFenceOkIsAdmitAtTheAttemptTimeout` to `RefAppendFenceOkIsAdmitAtTwoEnvelopes` and change its expected admission boundary from `attempt_timeout_ms` to `2 × attemptEnvelopeMs()` (read the existing test body; the fixture's budget gains `connect_timeout_cap_ms`).

`gtest_cas_pool.cpp` after `CASMountOpenWaits.FencedPriorReclaimsWithoutAnyWait`:

```cpp
TEST(CASMountOpenWaits, PublicationHorizonUsesTheEnvelope)
{
    /// TTL 1000, period 100, margin 50, attempt 100, cap 100: `period + 2 × attempt` = 300 fits the
    /// 950 ms safe window with 500 ms consumed, but `period + 2 × envelope` = 700 does not -- the open
    /// must re-anchor synchronously (one extra renewal write) before arming. (Validation: 100 + 600 +
    /// 50 < 1000.)
    auto b = std::make_shared<DB::Cas::tests::CountingBackend>();
    Layout l{"p"};
    DB::Cas::tests::seedPoolMetaForRestart(*b);
    uint64_t fake_boot = 0;
    PoolPtr store;
    ASSERT_NO_THROW(store = Pool::open(b, PoolConfig{
        .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "test",
        .mount_lease_ttl_ms = std::chrono::milliseconds(1000),
        .mount_renew_period = std::chrono::milliseconds(100),
        .cas_request_budget = CasRequestBudget{.attempt_timeout_ms = 100, .lease_safety_margin_ms = 50, .connect_timeout_cap_ms = 100},
        .boot_ms_fn = [&] { const uint64_t now = fake_boot; fake_boot += 500; return now; },   /// every read of the clock costs 500 ms
        .wait_sleep_fn = [&](uint64_t ms) { fake_boot += ms; },
    }));
    ASSERT_TRUE(store);
    /// Two mount writes: the claim and the synchronous re-anchor.
    EXPECT_GE(b->putOverwriteCount(l.mountKey("test")) + b->putCount(l.mountKey("test")), 2u);
}
```

Read `CasPool.cpp` ~836–848 first: the horizon check reads `bootMsNow()` once; the `+500` per read models a slow claim. If the fixture cannot make the horizon fail without the envelope (because the claim anchor is read earlier), adjust the clock steps so that `renewal_window_ms` with `attempt` fits and with `envelope` does not, and assert on the extra write. The remount twin repeats the fixture through `Pool::tryRemountOnce` (`CASPoolRemount.RemountArmAnchorsAtClaimAttemptNotResponseTime`, line ~1769, shows how a remount is driven).

`gtest_cas_heartbeat.cpp`:

```cpp
TEST(CASHeartbeat, RenewalStopsBeforeTheCutoffWhenEveryAttemptConsumesTheEnvelope)
{
    /// Every attempt costs the whole envelope (attempt 100 + 2 × cap 50 = 200 ms) and fails ambiguously.
    /// Under a 1000 ms lease with a 100 ms margin the renewal must stop issuing before the cutoff
    /// rather than start an attempt that cannot finish inside it.
    struct EnvelopeEatingBackend : RenewalScriptBackend
    {
        uint64_t * boot_ms = nullptr;
        uint64_t attemptTimeoutMs() const override { return 100; }
        uint64_t attemptEnvelopeMs() const override { return 200; }
        std::expected<String, RawConflict> write(const String & key, const String & bytes,
                                                 const std::optional<String> & expected_value, TransportAccess & access) override
        {
            if (expected_value && key.ends_with("/mount"))
            {
                attempts.push_back({key, bytes, expected_value});
                *boot_ms += 200;
                throw Poco::TimeoutException("the whole envelope, gone");
            }
            return InMemoryBackend::write(key, bytes, expected_value, access);
        }
        std::optional<Raw> read(const String & key, TransportAccess & access) override
        {
            *boot_ms += 200;
            throw Poco::TimeoutException("the read too");
        }
    };
    auto backend = std::make_shared<EnvelopeEatingBackend>();
    uint64_t wall_ms = 1000;
    uint64_t boot_ms = 100;
    backend->boot_ms = &boot_ms;
    Layout layout("pool");
    Ops ops(backend, &boot_ms);
    seedOwnClaim(ops.op, layout, "test", UInt128{0x1234}, 9, wall_ms, 1000);
    MountLeaseRenewer renewer(ops.mount, ops.farewell, layout, "test", UInt128{0x1234}, 9, std::chrono::milliseconds(1000),
                              [&] { return wall_ms; }, [] { return uint64_t{7}; }, {}, std::chrono::milliseconds(100),
                              [&] { return boot_ms; });
    renewer.start();
    const uint64_t cutoff = renewer.lastCommittedAttemptStartBootMs() + 1000 - 100;
    backend->attempts.clear();
    const MountRenewResult result = renewer.renew(renewalEnvironment(boot_ms));
    EXPECT_EQ(result.outcome, MountRenewOutcome::Terminal);
    EXPECT_LE(boot_ms, cutoff) << "the last attempt started inside the cutoff and the engine did not start one that could not finish";
}
```

`seedOwnClaim` writes the mount through `claimMount`, which goes through `write` with an expected value only on a refresh; if the seed hits the scripted branch, seed before assigning `boot_ms` or exclude `seq == 1` bodies. Read `seedOwnClaim` (gtest_cas_heartbeat.cpp ~70) first.

- [ ] **Step 2: Build, expect failures (missing field, `validateCasRequestBudget` arity, clone signature)**

Run: `flock build/.ninja_lock ninja -C build unit_tests_dbms > build/build_b2.log 2>&1; grep -m5 "error:" build/build_b2.log`

- [ ] **Step 3: Implement the envelope**

`CasRequestBudget.h`:
```cpp
    /// The cap the single-attempt client puts on one TCP connect and again on one TLS handshake,
    /// frozen when the pool opens as `min(disk connect_timeout_ms, attempt_timeout_ms)` (a configured
    /// zero means unbounded and is normalized to the attempt timeout) so a later client reload cannot
    /// widen the envelope. Empty when the storage has no S3 client: the envelope is the attempt alone.
    std::optional<uint64_t> connect_timeout_cap_ms = 1000;

    /// What ONE physical attempt is allowed to cost end to end: two connect caps (TCP, then TLS) plus
    /// the attempt timeout, saturating. The request contract reserves this, not the bare attempt timeout, before
    /// every attempt it starts.
    uint64_t attemptEnvelopeMs() const;
```
`CasRequestBudget.cpp`:
```cpp
uint64_t CasRequestBudget::attemptEnvelopeMs() const
{
    /// The TCP connect and the TLS handshake each get one connect interval from Poco, so an HTTPS
    /// attempt may spend two caps before any request I/O. Scheme-agnostic on purpose: conservative for
    /// plain HTTP, exact for HTTPS.
    const uint64_t cap = connect_timeout_cap_ms.value_or(0);
    const uint64_t connects = cap > std::numeric_limits<uint64_t>::max() / 2 ? std::numeric_limits<uint64_t>::max() : 2 * cap;
    return attempt_timeout_ms > std::numeric_limits<uint64_t>::max() - connects
        ? std::numeric_limits<uint64_t>::max()
        : attempt_timeout_ms + connects;
}

void validateCasRequestBudget(const CasRequestBudget & budget, uint64_t mount_lease_ttl_ms,
                              uint64_t mount_renew_period_ms, bool background_renewal)
{
    if (budget.attempt_timeout_ms == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "CAS request budget rejected: attempt_timeout_ms must be at least 1; a zero would reserve nothing "
            "while the request keeps the storage's own timeout");
    const uint64_t envelope = budget.attemptEnvelopeMs();
    /// Subtractions against the unsigned TTL: the sums could wrap for absurd values and read as small.
    const bool one_envelope_fits = envelope < mount_lease_ttl_ms
        && budget.lease_safety_margin_ms < mount_lease_ttl_ms - envelope;
    if (!one_envelope_fits)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "CAS request budget rejected: the attempt envelope ({} ms = attempt_timeout_ms {} + connect cap {}) "
            "plus lease_safety_margin_ms ({}) must be strictly less than the mount lease TTL ({} ms). "
            "A writable mount refuses to open with this budget.",
            envelope, budget.attempt_timeout_ms, budget.connect_timeout_cap_ms.value_or(0), budget.lease_safety_margin_ms, mount_lease_ttl_ms);
    if (background_renewal)
    {
        /// A renewal is a write: two envelopes (the attempt and its settlement read) after one period.
        const bool cadence_fits = mount_renew_period_ms < mount_lease_ttl_ms
            && envelope < (mount_lease_ttl_ms - mount_renew_period_ms) / 2
            && budget.lease_safety_margin_ms < mount_lease_ttl_ms - mount_renew_period_ms - 2 * envelope;
        if (!cadence_fits)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "CAS mount renewal cadence rejected: mount_renew_period_ms ({}) + 2 × attempt envelope ({} ms) + "
                "lease_safety_margin_ms ({}) must be strictly less than the mount lease TTL ({} ms)",
                mount_renew_period_ms, envelope, budget.lease_safety_margin_ms, mount_lease_ttl_ms);
    }
    LOG_INFO(getLogger("CasRequestBudget"),
        "CAS request budget in effect: attempt_timeout_ms={} connect_timeout_cap_ms={} envelope_ms={} lease_safety_margin_ms={} "
        "(mount_lease_ttl_ms={} mount_renew_period_ms={})",
        budget.attempt_timeout_ms, budget.connect_timeout_cap_ms.value_or(0), envelope, budget.lease_safety_margin_ms,
        mount_lease_ttl_ms, mount_renew_period_ms);
}
```
`CasPool.cpp` ~85–110: call `validateCasRequestBudget(config.cas_request_budget, ttl_ms, period_ms, config.background_watermark != nullptr)` and delete the `cadence_fits` block that follows (its check is now inside). Lines ~841 and ~1506: `period_ms + 2 * budget.attemptEnvelopeMs()` / `2 * budget.attemptEnvelopeMs()` for `renewal_window_ms` (saturating add helper if none exists in the file: use the existing pattern `a > max - b ? max : a + b`), and `renewal_window_fits` becomes STRICT (`renewal_window_ms < safe_deadline - now_boot_ms`), matching `CasMountRuntime::admit`, which refuses equality; the horizon tests assert the exact-boundary refusal. `CasMountRuntime::refAppendFenceOk`: `admit(fenceGeneration(), 2 * cas_request_budget.attemptEnvelopeMs())` with the comment "a write and its settlement read". Drains (`ContentAddressedMetadataStorage.cpp` ~986, `CasPool.cpp` ~1004, ~1160): `attemptEnvelopeMs() + lease_safety_margin_ms`.

`CasBackend.h`: after `attemptTimeoutMs`:
```cpp
    /// What one attempt may cost end to end, connect included; the contract reserves THIS. A backend
    /// with no connect notion answers its attempt timeout.
    virtual uint64_t attemptEnvelopeMs() const { return attemptTimeoutMs(); }
```
`ObjectStorageBackend`: constructor parameter `uint64_t connect_timeout_cap_ms_ = 0`, member `const uint64_t connect_timeout_cap_ms;`, `uint64_t attemptEnvelopeMs() const override` (saturating sum), `uint64_t connectTimeoutCapMs() const { return connect_timeout_cap_ms; }`. Pass the cap to every `clientForRetryProfile`-reaching call: `readSettingsFor` sets `rs.object_storage_connect_timeout_cap_ms = connect_timeout_cap_ms;` (new `ReadSettings`/`WriteSettings` field `uint64_t object_storage_connect_timeout_cap_ms = 0;` next to the attempt timeout — this is the one further additive `src/IO` settings field, covered by the B1 allowance); `conditionalWriteSettings` likewise; `iterate`, `removeObjectIfTokenMatches`, `removeObjectsIfExistUnderProfile`, `tryGetObjectMetadataWithNativeToken` overloads gain `uint64_t connect_timeout_cap_ms` (Task B3 folds these into the context).

`CasRequests.cpp:245`: `attempt_reservation_ms(backend->attemptEnvelopeMs())`.

`ContentAddressedMetadataStorage.cpp` ~785:
```cpp
    /// Frozen here, once: the connect cap every control request and every single-attempt clone carries.
    /// A later reload that widens the disk's connect timeout cannot widen the envelope the lease
    /// arithmetic was validated against.
    std::optional<uint64_t> connect_timeout_cap_ms;
    if (const auto s3_client = object_storage->tryGetS3StorageClient())
    {
        /// A configured zero is "unbounded" to Poco: the cap is then the attempt timeout itself.
        const auto configured = static_cast<uint64_t>(std::max<long>(0, s3_client->getClientConfiguration().connectTimeoutMs));
        connect_timeout_cap_ms = configured == 0 ? cas_attempt_timeout_ms : std::min(configured, cas_attempt_timeout_ms);
    }
    pool_config.cas_request_budget.connect_timeout_cap_ms = connect_timeout_cap_ms;
```
and the backend construction passes `pool_config.cas_request_budget.attempt_timeout_ms, connect_timeout_cap_ms`.

`S3ObjectStorage::getSingleAttemptClient(uint64_t request_timeout_ms, uint64_t connect_timeout_cap_ms)`: cache key `std::pair<uint64_t, uint64_t>`; after the request timeout override:
```cpp
    /// One TCP/TLS connect may not cost more than the cap the mount froze at open: the engine reserves
    /// attempt + cap per envelope, and a reloaded base client with a wider connect timeout must not
    /// widen what a reissue can spend.
    if (connect_timeout_cap_ms != 0)
        cfg.connectTimeoutMs = cfg.connectTimeoutMs <= 0 ? static_cast<long>(connect_timeout_cap_ms)
                                                        : std::min<long>(cfg.connectTimeoutMs, static_cast<long>(connect_timeout_cap_ms));
```
`clientForRetryProfile(profile, request_timeout_ms, connect_timeout_cap_ms)`; `readObject`/`writeObject` pass `read_settings.object_storage_connect_timeout_cap_ms` / `write_settings.object_storage_connect_timeout_cap_ms`. Add `void setClientForTest(std::unique_ptr<S3::Client> && new_client) { client->set(std::move(new_client)); }`.

`ContentAddressedSettings.cpp` ~83–84 descriptions: `"Budget for one HTTP attempt of a writable Native mount's control-plane requests (read, head, list, remove, conditional write), at least 1. With the connect cap it forms the attempt envelope the lease arithmetic reserves"` and `"Startup-only margin validated against the mount lease TTL: attempt envelope + this must be strictly less than the TTL, and renew period + 2 × envelope + this too"`; add `if (settings[ContentAddressedSetting::attempt_timeout_ms] == 0) throw ... "cas_attempt_timeout_ms must be >= 1"` next to the lease validations (~244).

- [ ] **Step 3b: The wiring test (Task B3 completes it)**

In `gtest_cas_s3_client_profile.cpp`, `CASEnvelopeWiring.FrozenCapTravelsFromTheClientToEveryVerb`: open a `ContentAddressedMetadataStorage` (the way `gtest_cas_s3_staging.cpp` builds one over an object storage; substitute the test `S3ObjectStorage` from `make_storage(1000)` with an `S3ObjectStorage` subclass `RecordingS3ObjectStorage` that records the `(request_timeout_ms, connect_timeout_cap_ms)` pair of every `getSingleAttemptClient` call) with `cas_attempt_timeout_ms = 5000`; assert `storage.poolConfigForTest().cas_request_budget.connect_timeout_cap_ms == 1000` and `.attemptEnvelopeMs() == 7000`; call `applyNewSettings` with a config carrying `<connect_timeout_ms>5000</connect_timeout_ms>` and again with `0`; after each, issue one control read (`existsFile` of a namespace file) and one conditional write (`CasPartWriteTxn`-free: a `SYSTEM CAS`-less path such as the GC state write through `pool()->gcRequestsForTest()`), and assert every recorded pair has cap `1000`. Until B3 lands, the write path records through `conditionalWriteSettings`'s cap field. Fails while any link is missing.

- [ ] **Step 4: Build, run the new tests and the CAS gate**

Run: `flock build/.ninja_lock ninja -C build unit_tests_dbms > build/build_b2b.log 2>&1 && build/src/unit_tests_dbms --gtest_filter='CAS*:S3SingleAttemptClient.*' > build/test_b2.log 2>&1; tail -3 build/test_b2.log`
Expected: all PASS. Existing tests whose fixtures used tiny budgets (`CASMountOpenWaits.UncleanOpenPaysOnlyTheObservationWindow` with attempt 50 / margin 50 / TTL 500 / period 100) now need `connect_timeout_cap_ms` explicitly (default 1000 would fail validation): set `.connect_timeout_cap_ms = std::nullopt` in those fixtures; list every fixture changed in the commit message.

### Task B3: The engine's attempt number reaches the transport

**Files:**
- Modify: `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasTransportAccess.h`, `CasRequests.h` (~184, ~410–450), `CasRequests.cpp` (`writeLoop` ~860, `probeSentinel` ~613)
- Modify: `src/IO/WriteSettings.h` (next to `ObjectStorageRetryProfile`, line ~18): `struct ObjectStorageControlRequest`
- Modify: `CasObjectStorageBackend.h/.cpp` (every `(profile, timeout_ms)` pair → `const ObjectStorageControlRequest &`), `IObjectStorage.h` (~275), `S3ObjectStorage.h/.cpp` (profile-aware overloads)
- Test: `src/Disks/tests/gtest_cas_sentinel_probe.cpp`, `src/Disks/tests/gtest_cas_bootstrap_ordering.cpp`, `src/Disks/tests/gtest_cas_requests.cpp`

**Interfaces:**
- Produces: `class TransportAccess { public: size_t attemptNo() const; ... }` constructed by `CasRequests::withTransportAccess(size_t attempt_no, Fn &&)`; every `Backend` primitive reads `access.attemptNo()`.
- Produces: `struct ObjectStorageControlRequest { ObjectStorageRetryProfile profile = Default; uint64_t attempt_timeout_ms = 0; uint64_t connect_timeout_cap_ms = 0; size_t attempt_number = 0; };` replacing the `(profile, request_timeout_ms[, cap])` parameters of `IObjectStorage::iterate`, `S3ObjectStorage::removeObjectIfTokenMatches`, `removeObjectsIfExistUnderProfile`, `tryGetObjectMetadataWithNativeToken`, `clientForRetryProfile`; `ObjectStorageBackend::controlRequest(size_t attempt_no) const` builds it from `controlPlaneProfile()`, `attempt_timeout_ms`, `connect_timeout_cap_ms`.

- [ ] **Step 1: Write the propagation tests**

`gtest_cas_requests.cpp`:
```cpp
TEST(CASRequests, TheTransportSeesTheEngineAttemptNumber)
{
    struct AttemptRecordingBackend : CountingBackend
    {
        std::vector<size_t> write_attempts, read_attempts, list_attempts;
        std::expected<String, RawConflict> write(const String & key, const String & bytes,
                                                 const std::optional<String> & expected, TransportAccess & access) override
        {
            write_attempts.push_back(access.attemptNo());
            return CountingBackend::write(key, bytes, expected, access);
        }
        std::optional<Raw> read(const String & key, TransportAccess & access) override
        {
            read_attempts.push_back(access.attemptNo());
            return CountingBackend::read(key, access);
        }
        RawListPage list(const String & prefix, const String & cursor, size_t limit, TransportAccess & access) override
        {
            list_attempts.push_back(access.attemptNo());
            return CountingBackend::list(prefix, cursor, limit, access);
        }
    };
    FakeClock clock;
    auto backend = std::make_shared<AttemptRecordingBackend>();
    backend->injectAmbiguousWrite("k");
    backend->failNextReadWith("k", std::make_exception_ptr(Poco::TimeoutException("read timed out")));
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    ASSERT_TRUE(std::holds_alternative<Committed>(op.create("k", "v", Retry::standard())));
    /// Attempt 1 ambiguous, attempt 2 commits. The settle read is its OWN read call: attempt 1 failed, 2 answered.
    EXPECT_EQ(backend->write_attempts, (std::vector<size_t>{1, 2}));
    EXPECT_EQ(backend->read_attempts, (std::vector<size_t>{1, 2}));
    backend->list_attempts.clear();
    (void)op.list("p/", "", 10, Retry::standard());
    EXPECT_EQ(backend->list_attempts, (std::vector<size_t>{1}));
}
```
`gtest_cas_sentinel_probe.cpp` (an `InMemoryBackend` subclass recording `access.attemptNo()` in `probeSentinelRaw`, answering `Indeterminate` once then `Present`):
```cpp
TEST(CASSentinelProbe, AttemptNumberPropagates)
{
    struct ProbeRecording : InMemoryBackend
    {
        std::vector<size_t> attempts;
        SentinelProbeResult probeSentinelRaw(const String & key, TransportAccess & access) override
        {
            attempts.push_back(access.attemptNo());
            if (attempts.size() == 1)
                return {ProbeOutcome::Indeterminate, std::nullopt};
            return InMemoryBackend::probeSentinelRaw(key, access);
        }
    };
    DB::Cas::tests::FakeClock clock;
    auto backend = std::make_shared<ProbeRecording>();
    CasRequests requests(backend, Fence::open(), clock.nowFn(), clock.sleepFn());
    auto op = requests.admit();
    (void)op.probeSentinel("probe", Retry::standard());
    EXPECT_EQ(backend->attempts, (std::vector<size_t>{1, 2}));
    EXPECT_EQ(clock.sleeps.size(), 1u);   /// propagation only: the probe keeps its ordinary backoff
}
```
`gtest_cas_bootstrap_ordering.cpp` (`RecordingBackend` is no longer `final`):
```cpp
TEST(CASBootstrapOrdering, ResidualListSucceedsOnTheSecondAttempt)
{
    /// Every LIST whose attempt number is 1 fails as the first-attempt fuse would; attempt 2 answers.
    struct FuseOnFirstList : RecordingBackend
    {
        RawListPage list(const String & prefix, const String & cursor, size_t limit, TransportAccess & access) override
        {
            if (access.attemptNo() == 1)
                throw Poco::TimeoutException("Timeout");
            return RecordingBackend::list(prefix, cursor, limit, access);
        }
    };
    auto b = std::make_shared<FuseOnFirstList>();
    /// A healthy pool without `_pool_meta` is the shape that needs the LIST: seed one residual key.
    createObj(*b, "p/store/residual", "x");
    PoolConfig config{.pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "test"};
    EXPECT_THROW(Pool::open(b, config), DB::Exception);   /// residual without meta refuses -- proving the LIST was answered
    bool listed_on_second = false;
    for (const auto & e : b->entries())
        listed_on_second |= e.op == RecordingBackend::Op::List;
    EXPECT_TRUE(listed_on_second);
}
```
Read `ResidualWithoutMetaFailsTypedWithZeroWrites` (line ~254) and reuse its exact seeding helper and expected error code so the assertion distinguishes "refused because listed" from "refused because the LIST failed" (the former is `CAS_*`-typed; the latter is the `Indeterminate` code 668 seen in R1).

- [ ] **Step 2: Build, expect compile failures (`attemptNo`)**

- [ ] **Step 3: Implement**

`CasTransportAccess.h`:
```cpp
class TransportAccess
{
    friend class CasRequests;
    explicit TransportAccess(size_t attempt_no_) : attempt_no(attempt_no_) {}
    size_t attempt_no;

public:
    TransportAccess(const TransportAccess &) = delete;
    TransportAccess & operator=(const TransportAccess &) = delete;
    /// The engine's 1-based count of physical attempts of the logical call this request belongs to,
    /// for the transport to number its request with. Only "1 versus more than 1" is relied upon.
    size_t attemptNo() const { return attempt_no; }
};
```
`CasRequests.h`: `withTransportAccess(size_t attempt_no, Fn && fn)` constructs `TransportAccess access(attempt_no);`. `readLoop`: `for (uint32_t attempt_no = 1, ordinary_reissues = 0;; ++attempt_no)`, passes `attempt_no`; backoff index becomes `Retry::backoff(++ordinary_reissues)` (Task B4 adds the zero-pause branch that increments only `attempt_no`). `writeLoop`: `owner.withTransportAccess(state.attempts_sent, ...)` (already incremented). `probeSentinel`: pass its `attempt`. `CasHotKeys` callers of `withTransportAccess` (grep) pass their own attempt counter or `1`.

`WriteSettings.h` after `ObjectStorageRetryProfile`:
```cpp
/// What a CAS control request carries into the object storage: the retry profile, the per-attempt
/// budget and connect cap the storage's single-attempt client must honour, and the caller's own
/// attempt number (0 = unset) so the HTTP client sees a reissue as attempt ≥ 2.
struct ObjectStorageControlRequest
{
    ObjectStorageRetryProfile profile = ObjectStorageRetryProfile::Default;
    uint64_t attempt_timeout_ms = 0;
    uint64_t connect_timeout_cap_ms = 0;
    size_t attempt_number = 0;
};
```
`IObjectStorage::iterate(prefix, max_keys, with_tags, start_after, const ObjectStorageControlRequest &)` replaces the `(profile, request_timeout_ms)` overload; `S3ObjectStorage` overloads likewise; `clientForRetryProfile(const ObjectStorageControlRequest & request)` → `getSingleAttemptClient(request.attempt_timeout_ms, request.connect_timeout_cap_ms)`; `S3IteratorAsync` receives `request.attempt_number` as its seed; the HEAD/DELETE/bulk-DELETE builders receive it too (B1 parameters). `ObjectStorageBackend`: `ObjectStorageControlRequest controlRequest(size_t attempt_no) const`, `readSettingsFor(const ObjectStorageControlRequest &)` sets profile, timeout, cap and `rs.object_storage_attempt_number = request.attempt_number`; `conditionalWriteSettings(size_t attempt_no)` sets `ws.object_storage_attempt_number = attempt_no` and the cap; every primitive passes `controlRequest(access.attemptNo())`; the `*Under` helpers take the context. `conditionalWriteSettingsForTest()` passes 1.

- [ ] **Step 4: Build, run the gate and the S3 object-storage tests**

Run: `flock build/.ninja_lock ninja -C build unit_tests_dbms > build/build_b3.log 2>&1 && build/src/unit_tests_dbms --gtest_filter='CAS*:S3*:WBS3Test.*' > build/test_b3.log 2>&1; tail -3 build/test_b3.log`
Expected: PASS.

### Task B4: First-attempt fuse: matcher and zero-pause reissue

**Files:**
- Modify: `CasRequests.h` (declaration next to `isConnectFailureHint`; `readLoop`; new `reissueAtOnce`), `CasRequests.cpp` (`writeLoop`), `src/Common/ProfileEvents.cpp`
- Test: `src/Disks/tests/gtest_cas_requests.cpp`

**Interfaces:**
- Produces: `bool isFirstAttemptFuseTimeout(const std::exception & e, size_t attempt_no);` — `attempt_no == 1`, `S3Exception` with `NETWORK_CONNECTION`, not `isConnectFailureHint`, message contains `Timeout` (Poco's `TimeoutException` name; pinned by test). `std::optional<WriteResult> CasOperation::reissueAtOnce(WriteState &, const Retry::Bound &)` — `reservedFor(0, 2)` gates, records `CASRequestReissue`, no sleep, `state.reissues` untouched. ProfileEvent `CASRequestFirstAttemptFuse`.

- [ ] **Step 1: Write the tests**

```cpp
TEST(CASRequestsFuse, MatcherPrecedence)
{
    using Aws::S3::S3Errors;
    /// The generic transport timeout text is Poco's exception name, pinned here.
    EXPECT_THAT(Poco::TimeoutException("the socket").displayText(), testing::StartsWith("Timeout"));
    const DB::S3Exception fuse("Poco::Exception. Code: 1000, e.code() = 0, Timeout: the socket", S3Errors::NETWORK_CONNECTION);
    EXPECT_TRUE(isFirstAttemptFuseTimeout(fuse, 1));
    EXPECT_FALSE(isFirstAttemptFuseTimeout(fuse, 2));
    const DB::S3Exception hint("Poco::Exception. Code: 1000, e.code() = 0, Timeout: connect timed out: 10.0.0.1:9", S3Errors::NETWORK_CONNECTION);
    EXPECT_FALSE(isFirstAttemptFuseTimeout(hint, 1));     /// spec 1 owns it
    EXPECT_TRUE(isConnectFailureHint(hint));
    EXPECT_FALSE(isFirstAttemptFuseTimeout(DB::S3Exception("Connection reset by peer", S3Errors::NETWORK_CONNECTION), 1));
    EXPECT_FALSE(isFirstAttemptFuseTimeout(DB::S3Exception("Timeout", S3Errors::INTERNAL_FAILURE), 1));
}

namespace
{
std::exception_ptr fuseTimeout()
{
    return std::make_exception_ptr(DB::S3Exception("Poco::Exception. Code: 1000, e.code() = 0, Timeout: the socket",
                                                   Aws::S3::S3Errors::NETWORK_CONNECTION));
}
}

TEST(CASRequestsFuse, FirstAttemptTimeoutReissuesWithoutSleep)
{
    /// Write: the settle read still runs (the request may have been sent), then a no-sleep reissue.
    {
        FakeClock clock;
        auto backend = std::make_shared<CountingBackend>();
        backend->failNextWriteWith("k", fuseTimeout());
        auto requests = makeRequests(backend, clock);
        auto op = requests.admit();
        WriteResult result = op.create("k", "v", Retry::standard());
        const auto * committed = std::get_if<Committed>(&result);
        ASSERT_NE(committed, nullptr);
        EXPECT_EQ(committed->attempts_sent, 2u);
        EXPECT_EQ(backend->getTotal(), 1u);
        EXPECT_TRUE(clock.sleeps.empty());
    }
    /// Read and LIST: no settle read, no sleep.
    {
        FakeClock clock;
        auto backend = std::make_shared<CountingBackend>();
        auto requests = makeRequests(backend, clock);
        auto op = requests.admit();
        orThrow(op.create("k", "v", Retry::standard()), "seed");
        backend->resetCounts();
        backend->failNextReadWith("k", fuseTimeout());
        EXPECT_TRUE(op.read("k", Retry::standard()).has_value());
        EXPECT_EQ(backend->getTotal(), 2u);
        EXPECT_TRUE(clock.sleeps.empty());
    }
    /// Attempts 1 and 2 failing: attempt 2 is not a first attempt, so exactly one sleep, after it.
    {
        FakeClock clock;
        auto backend = std::make_shared<CountingBackend>();
        backend->failNextWriteWith("k", fuseTimeout());
        backend->failNextWriteWith("k", fuseTimeout());
        auto requests = makeRequests(backend, clock);
        auto op = requests.admit();
        WriteResult result = op.create("k", "v", Retry::standard());
        ASSERT_TRUE(std::holds_alternative<Committed>(result));
        EXPECT_EQ(std::get<Committed>(result).attempts_sent, 3u);
        EXPECT_EQ(clock.sleeps.size(), 1u);
    }
}

TEST(CASRequestsFuse, GatesRefuseTheZeroPauseReissue)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    backend->failNextWriteWith("k", fuseTimeout());
    auto requests = makeRequests(backend, clock);
    requests.setAttemptReservationForTest(1'000);
    auto op = requests.admit();
    /// Two envelopes for the attempt, the read consumed none of the clock, but the window admits no
    /// second write of two envelopes.
    WriteResult result = op.create("k", "v", Retry::within(2'000));
    const auto * gave_up = std::get_if<GaveUp>(&result);
    ASSERT_NE(gave_up, nullptr);
    EXPECT_EQ(gave_up->why, GaveUp::Why::Deadline);
    /// `Retry::once()` never performs a second attempt.
    auto once_backend = std::make_shared<CountingBackend>();
    once_backend->failNextWriteWith("k", fuseTimeout());
    auto once_requests = makeRequests(once_backend, clock);
    auto once_op = once_requests.admit();
    (void)once_op.create("k", "v", Retry::once());
    EXPECT_EQ(once_backend->writeTotal(), 1u);
}

TEST(CASRequestsFuse, ReadLoopZeroPauseKeepsTheBackoffIndex)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    orThrow(op.create("k", "v", Retry::standard()), "seed");
    backend->failNextReadWith("k", fuseTimeout());
    backend->failNextReadWith("k", std::make_exception_ptr(Poco::TimeoutException("attempt 2: an ordinary fault")));
    EXPECT_TRUE(op.read("k", Retry::standard()).has_value());
    ASSERT_EQ(clock.sleeps.size(), 1u);
    /// The one sleep is `backoff(1)`: the zero-pause reissue did not advance the index.
    EXPECT_LE(clock.sleeps[0], 200u);   /// `backoff(1)` is full jitter over [0, 200] ms
}
```

- [ ] **Step 2: Build, expect compile failures**

- [ ] **Step 3: Implement**

`ProfileEvents.cpp`: `M(CASRequestFirstAttemptFuse, "Number of CAS control requests whose first HTTP attempt hit the adaptive first-attempt timeout. The engine reissued them at once as attempt 2 under the full attempt budget. Growth means the object store does not answer a fresh connection within the first-attempt timeout.", ValueType::Number) \`.

`CasRequests.cpp`:
```cpp
bool isFirstAttemptFuseTimeout(const std::exception & e, size_t attempt_no)
{
    if (attempt_no != 1 || isConnectFailureHint(e))
        return false;
#if USE_AWS_S3
    const auto * s3 = dynamic_cast<const S3Exception *>(&e);
    return s3 && s3->getS3ErrorCode() == Aws::S3::S3Errors::NETWORK_CONNECTION
        && s3->message().find("Timeout") != String::npos;
#else
    return false;
#endif
}

std::optional<WriteResult> CasOperation::reissueAtOnce(WriteState & state, const Retry::Bound & bound)
{
    const uint64_t needed = reservedFor(0, 2);
    switch (gate(needed))
    {
        case Gate::FenceLost: return gaveUp(GaveUp::Why::FenceLost, sourceFor(bound), state);
        case Gate::NoBudget:  return gaveUp(GaveUp::Why::Deadline, GaveUp::Source::Lease, state);
        case Gate::Ok: break;
    }
    if (!fits(needed, bound))
        return gaveUp(GaveUp::Why::Deadline, sourceFor(bound), state);
    detail::recordReissue();
    return std::nullopt;
}
```
`writeLoop`: `bool fuse = false;` set in the `catch (const Exception & e)` arm: `fuse = isFirstAttemptFuseTimeout(e, state.attempts_sent);` with `ProfileEvents::increment(ProfileEvents::CASRequestFirstAttemptFuse)` when true. At the end of the loop body, replace the final reissue by:
```cpp
        if (policy.single_attempt)
            return gaveUp(GaveUp::Why::Unresolved, sourceFor(bound), state);
        /// The first attempt met the adaptive first-attempt timeout: a connection-quality answer, not a
        /// store fault. The read above settled nothing, so re-send at once as attempt 2 under the full
        /// budget; the backoff index is untouched because no store fault was seen yet.
        if (fuse)
        {
            if (auto given_up = reissueAtOnce(state, bound))
                return *given_up;
            continue;
        }
        if (auto given_up = pauseAndReissue(state, bound))
            return *given_up;
```
`readLoop` (header), catch arm:
```cpp
        catch (const std::exception & e)
        {
            if (refreshAndClassifyReadFault(e, refresh_attempted) || policy.single_attempt)
                throw;
            if (isFirstAttemptFuseTimeout(e, attempt_no))
            {
                ProfileEvents::increment(ProfileEvents::CASRequestFirstAttemptFuse);   /// via a detail:: recorder
                const uint64_t needed = reservedFor(0, 1);
                switch (gate(needed))
                {
                    case Gate::FenceLost: giveUpReadFenceLost(verb, subject, "before the reissue");
                    case Gate::NoBudget:  giveUpReadNoBudget(verb, subject, "for the reissue");
                    case Gate::Ok: break;
                }
                if (!fits(needed, bound))
                    giveUpReadDeadline(verb, subject, bound, attempt_no);
                detail::recordReissue();
                continue;
            }
        }
        const uint64_t pause_ms = Retry::backoff(++ordinary_reissues);
```
(`detail::recordFirstAttemptFuse()` added next to `recordReissue`, since the header does not declare events.) `probeSentinel` is left as is (propagation only).

- [ ] **Step 4: Build and run the gate**

Run: `flock build/.ninja_lock ninja -C build unit_tests_dbms > build/build_b4.log 2>&1 && build/src/unit_tests_dbms --gtest_filter='CAS*' > build/test_b4.log 2>&1; tail -3 build/test_b4.log`

### Task B5: Integration proof on the GCS fake

**Files:**
- Modify: `tests/integration/test_cas_gcs/gcs_mocks/server.py` (`/_control/delay` ~832, application ~902)
- Modify: `tests/integration/test_cas_gcs/test.py`

- [ ] **Step 1: Extend the delay knob**

`/_control/delay?substr=S&ms=N&method=PUT|GET|LIST&once=0|1`: store `STORE.delay_method` (default `PUT`) and `STORE.delay_once`; a LIST is a `GET` with an empty key and a `prefix` query — match `method == "GET" and not key and "prefix" in query` when `delay_method == "LIST"` against the prefix value; when `once` is set, clear the knob after the first match. Rename the counter to `DelayedRequest` and keep `DelayedPut` incrementing for PUTs (existing callers). Reset clears the new fields.

- [ ] **Step 2: Write the test**

```python
def test_a_first_attempt_timeout_is_reissued_as_attempt_two():
    node = cluster.instances["node"]
    disk = "cas_gcs_hmac"          # policy name == disk name in this module (see CAS_DISKS)
    _control_post("/_control/reset")
    node.query("DROP TABLE IF EXISTS fuse_probe SYNC")
    node.query("CREATE TABLE fuse_probe (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS storage_policy = '{}'".format(disk))
    node.query("INSERT INTO fuse_probe VALUES (1)")
    _quiesce_merges(node, "fuse_probe")
    node.query("SYSTEM CAS GC STOP '{}'".format(disk))   # only the explicit round below may LIST

    # LIST: one GC round lists the `gc/` family; the first matching LIST is delayed past the 200 ms fuse.
    seq = _next_seq()
    _control_post("/_control/delay?substr=gc&ms=300&method=LIST&once=1")
    node.query("SYSTEM CAS GC RUN '{}'".format(disk))
    lists = [r for r in _captured_since(seq, CAS_DISKS[disk]) if r["method"] == "GET" and not r["key"] and "prefix=" in r["query"]]
    assert len(lists) >= 2, lists
    attempts = [r["headers"].get("clickhouse-request", "") for r in lists[:2]]
    assert attempts[0].endswith("attempt=1") or attempts[0] == "", attempts
    assert attempts[1].endswith("attempt=2"), attempts

    # Conditional PUT: delay the first `.meta` PUT of the next insert; expect PUT(1) -> GET -> PUT(2).
    seq = _next_seq()
    resolve_reads_before = _resolve_reads(node)
    _control_post("/_control/delay?substr=.meta&ms=300&method=PUT&once=1")
    node.query("INSERT INTO fuse_probe VALUES (2)")
    rows = [r for r in _captured_since(seq) if r["key"].endswith(".meta")]
    meta_key = rows[0]["key"]
    order = [(r["method"], r["headers"].get("clickhouse-request", "")) for r in rows if r["key"] == meta_key]
    assert order[0][0] == "PUT" and order[1][0] == "GET" and order[2][0] == "PUT", order
    assert order[2][1].endswith("attempt=2"), order
    assert _resolve_reads(node) - resolve_reads_before >= 1
    node.query("SYSTEM CAS GC START '{}'".format(disk))
```
`_captured_since(seq, bucket)` filters by bucket; the `disk` fixture of the module is not used so the test is not parametrized (one disk suffices for the transport proof). The delayed LIST's `prefix` query value must contain `gc` -- check the first captured LIST of a GC round in `_captured()` and adjust the substring to the exact family the round lists first.

- [ ] **Step 3: Run the module**

Run: `python3 -m ci.praktika run "integration" --test test_cas_gcs > build/test_b5_it.log 2>&1; tail -20 build/test_b5_it.log` (whole module; the CI targeted job is partial). Expected: all tests in the module pass.

### Task B6: Texts, docs, commit spec 4

- [ ] **Step 1: Docs**

`configuration.md`: row `cas_attempt_timeout_ms`: `Budget for one HTTP attempt of a writable Native mount's control-plane requests (read, head, list, remove, conditional write), at least 1. Together with the connect cap (min(connect_timeout_ms, this); a zero connect_timeout_ms counts as this) it forms the attempt envelope: one TCP connect and one TLS handshake under the cap each, then each socket operation under this timeout`; row `cas_lease_safety_margin_ms`: `... envelope + margin must be strictly less than the TTL, and renew period + 2 × envelope + margin too`; row `cas_mount_renew_period_ms`: replace "one request attempt" with "two attempt envelopes". `mounts-and-leases.md` ~82 "one configured attempt still fits" → "one attempt envelope (connect cap plus attempt timeout) still fits"; ~96 `attempt_timeout + safety_margin` → `2 × envelope + safety_margin`. `CasRequestBudget.h` field comments already written in B2.

- [ ] **Step 2: Commit spec 4 and review**

```bash
cat > tmp/msg_spec4.txt <<'EOF'
cas: engine reissues run as transport attempt >= 2 under one attempt envelope that includes connect

The single-attempt client's requests were all first attempts to the HTTP client, so every engine
reissue ran under the 200 ms adaptive first-attempt timeout and the configured attempt timeout
never applied. The engine's attempt number now reaches the transport through the control-request
context, a first-attempt timeout is reissued at once as attempt 2, and the reservation the engine
makes is the attempt envelope: attempt timeout plus a connect cap frozen at open, validated
against the lease TTL and the renewal cadence at every site that budgets a request.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
EOF
git commit -F tmp/msg_spec4.txt -- <every file touched in B1–B6, listed explicitly>
```
Then the codex review loop as in A5 (`prompt_impl4.md`).

---

## Part C — Spec 2: configurable lease timing plus an unsafe no-delay reclaim

### Task C1: `claimMount` accepts an explicit unsafe authorization

**Files:**
- Modify: `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h` (~190 enum, ~274 declaration), `CasServerRoot.cpp` (~858–890)
- Modify: `Pool/CasPool.cpp` (~811 exhaustive switch)
- Test: `src/Disks/tests/gtest_cas_mount.cpp`

**Interfaces:**
- Produces: `MountPriorState::UncleanUnsafe`; `claimMount(op, l, srid, our_uuid, our_epoch, now_ms, ttl_ms, proven_dead_incarnation = {}, sink = {}, unsafe_reclaim_authorization = {})` where `const std::optional<Etag> & unsafe_reclaim_authorization` reclaims a same-uuid, different-epoch, uncertified slot only when it equals the currently read etag; audit reason text `cas_unsafe_remount_no_delay`.

- [ ] **Step 1: Write the test** (in `gtest_cas_mount.cpp`, after the `CASMountAwaitExpiry` tests; `Ops` and `Layout` as used there)

```cpp
TEST(CASMountClaim, UnsafeAuthorizationIsTokenExact)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    Ops ops(b);
    ASSERT_EQ(claimMount(ops.op, l, "r", UInt128(1), 7, /*now*/ 1000, /*ttl*/ 30000).kind, MountClaimResult::Claimed);
    const Etag current = ops.op.read(l.mountKey("r"), Retry::standard())->etag;

    /// A stale token is refused: nothing authorizes a reclaim over a slot that moved.
    const Etag stale = *orThrow(ops.op.create("p/other", "x", Retry::standard()), "unrelated object");
    MountClaimResult refused = claimMount(ops.op, l, "r", UInt128(1), 8, 1000, 30000, /*proven_dead=*/{}, /*sink=*/{},
                                          /*unsafe_reclaim_authorization=*/stale);
    EXPECT_EQ(refused.kind, MountClaimResult::LiveDoubleStart);

    /// A foreign uuid is refused before the authorization is consulted.
    MountClaimResult foreign = claimMount(ops.op, l, "r", UInt128(2), 8, 1000, 30000, {}, {}, current);
    EXPECT_EQ(foreign.kind, MountClaimResult::ForeignOwner);

    /// The exact token reclaims, with the prior state and the audit reason naming the setting.
    std::vector<CasEvent> events;
    MountClaimResult reclaimed = claimMount(ops.op, l, "r", UInt128(1), 8, 1000, 30000, {},
                                            [&](CasEvent e) { events.push_back(std::move(e)); }, current);
    ASSERT_EQ(reclaimed.kind, MountClaimResult::Claimed);
    EXPECT_EQ(reclaimed.prior, MountPriorState::UncleanUnsafe);
    ASSERT_FALSE(events.empty());
    EXPECT_THAT(events.back().reason, testing::HasSubstr("cas_unsafe_remount_no_delay"));   /// `CasEvent::reason`
    EXPECT_EQ(decodeMountLease(ops.op.read(l.mountKey("r"), Retry::standard())->bytes).writer_epoch, 8u);
}
```
`stale` must be a foreign-key etag only if `claimMount` compares values, not key binding; if the engine refuses a cross-key `Etag` comparison (`valueFor` throws `LOGICAL_ERROR` for another key), build the stale token instead by refreshing the slot once more (`claimMount(... 7 ...)` again, then the earlier `current` is the stale one). `emitMountEvent`'s last argument becomes `CasEvent::reason`.

- [ ] **Step 2: Build, expect failures** (arity, enumerator)

- [ ] **Step 3: Implement**

`CasServerRoot.h`: add `UncleanUnsafe,   /// the operator's explicit `cas_unsafe_remount_no_delay` authorization carried the slot's exact token` to the enum; add the parameter to `claimMount` with the doc: "never reused from `proven_dead_incarnation`: that one says the token was OBSERVED dead, this one says the operator accepted the risk". `CasServerRoot.cpp` ~858:
```cpp
    const bool clean_marker = ...;
    const bool proven_dead = ...;
    const bool unsafe_authorized = unsafe_reclaim_authorization && *unsafe_reclaim_authorization == got->etag;
    if (existing.gc_fenced || clean_marker || proven_dead || unsafe_authorized)
    {
        ...
        const MountPriorState prior = existing.gc_fenced ? MountPriorState::Fenced
                                     : clean_marker       ? MountPriorState::Clean
                                     : proven_dead        ? MountPriorState::UncleanObserved
                                                           : MountPriorState::UncleanUnsafe;
        emitMountEvent(sink, CasEventType::MountClaim, srid, "reclaim", &existing,
            existing.gc_fenced ? "..." : clean_marker ? "..." : proven_dead ? "... observed dead by incarnation stability — reclaimed"
            : "same server_uuid, different writer_epoch, reclaimed at once under cas_unsafe_remount_no_delay — "
              "the operator accepted that a live predecessor with this uuid may still be writing");
```
`CasPool.cpp` ~811 switch: `case MountPriorState::UncleanUnsafe:` alongside `Fenced`/`UncleanObserved` (`unclean_reclaim = true`), and the log line gains "(reclaimed without observation under cas_unsafe_remount_no_delay)" when the prior is `UncleanUnsafe`. Also update the `claimMountAwaitingExpiry` call to pass `{}` for the new argument.

- [ ] **Step 4: Build and run** `--gtest_filter='CASMount*'`, expect PASS.

### Task C2: The setting and its one site

**Files:**
- Modify: `ContentAddressedSettings.cpp` (~74), `ContentAddressedMetadataStorage.h/.cpp` (member + `pool_config`), `Pool/CasPool.h` (`PoolConfig`), `Pool/CasPool.cpp` (~712–724)
- Test: `src/Disks/tests/gtest_cas_settings.cpp`, `src/Disks/tests/gtest_cas_pool.cpp`

- [ ] **Step 1: Tests**

`gtest_cas_settings.cpp` (read its existing shape; add):
```cpp
TEST(CASContentAddressedSettings, UnsafeRemountNoDelayIsOffByDefault)
{
    {
        auto cfg = makeConfig("<cas_server_root_id>srv1</cas_server_root_id>");
        ContentAddressedSettings s;
        s.loadFromConfig(*cfg, "disk", "/data", "/data/scratch", identity_macros);
        EXPECT_FALSE(s[ContentAddressedSetting::unsafe_remount_no_delay].value);
    }
    {
        auto cfg = makeConfig("<cas_server_root_id>srv1</cas_server_root_id><cas_unsafe_remount_no_delay>1</cas_unsafe_remount_no_delay>");
        ContentAddressedSettings s;
        s.loadFromConfig(*cfg, "disk", "/data", "/data/scratch", identity_macros);
        EXPECT_TRUE(s[ContentAddressedSetting::unsafe_remount_no_delay].value);
    }
}
```
(`ContentAddressedMetadataStorage` copies the value into `pool_config.unsafe_remount_no_delay`; the pool test below proves it is consulted.)
`gtest_cas_pool.cpp` after `UncleanOpenPaysOnlyTheObservationWindow` (same fixture, same tiny budget plus `.connect_timeout_cap_ms = 0`):
```cpp
TEST(CASMountOpenWaits, UnsafeNoDelayOpensWithoutTheObservationWindow)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l{"p"};
    DB::Cas::tests::seedPoolMetaForRestart(*b);
    ASSERT_EQ(claimMount(*DB::Cas::tests::OperationForTest(b), l, "test", UInt128(1), 7, 1000, 500).kind, MountClaimResult::Claimed);
    createObj(*b, l.epochKey("test"), encodeServerEpoch(ServerEpoch{.next_writer_epoch = 8}));
    std::vector<CasEvent> events;
    uint64_t fake_boot = 0;
    std::vector<uint64_t> waits;
    PoolPtr store;
    ASSERT_NO_THROW(store = Pool::open(b, PoolConfig{
        .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "test",
        .mount_lease_ttl_ms = std::chrono::milliseconds(500), .mount_renew_period = std::chrono::milliseconds(100),
        .cas_request_budget = CasRequestBudget{.attempt_timeout_ms = 50, .lease_safety_margin_ms = 50, .connect_timeout_cap_ms = std::nullopt},
        .unsafe_remount_no_delay = true,
        .event_sink = [&](CasEvent e) { events.push_back(std::move(e)); },
        .boot_ms_fn = [&] { return fake_boot; },
        .wait_sleep_fn = [&](uint64_t ms) { fake_boot += ms; waits.push_back(ms); },
    }));
    ASSERT_TRUE(store);
    EXPECT_TRUE(waits.empty()) << "no observation window under the unsafe setting";
    EXPECT_TRUE(std::ranges::any_of(events, [](const CasEvent & e) { return e.reason.find("cas_unsafe_remount_no_delay") != String::npos; }));
    EXPECT_EQ(decodeMountLease(DB::Cas::tests::OperationForTest(b)->read(l.mountKey("test"), Retry::standard())->bytes).writer_epoch, 8u);
}
```
(`PoolConfig::event_sink` is a `CasEventSink`, `CasPool.h:185`; `CasEvent::reason` is the required human-readable why, `Primitives/CasEvent.h:75`.)

- [ ] **Step 2: Build, expect failures**

- [ ] **Step 3: Implement**

`ContentAddressedSettings.cpp` after `mount_renew_period_ms`:
```cpp
    DECLARE(Bool, unsafe_remount_no_delay, false, "Reclaim a mount slot that carries this server's own uuid at once after a hard restart, without observing the slot's token for the lease TTL. Unsafe whenever two processes can hold the same server_uuid (a copied uuid file, a stalled predecessor): the predecessor may still start conditional writes until its own cutoff. Intended for test stands and deployments that guarantee one process per uuid", 0) \
```
`ContentAddressedMetadataStorage`: member `const bool cas_unsafe_remount_no_delay;`, `pool_config.unsafe_remount_no_delay = cas_unsafe_remount_no_delay;`. `PoolConfig`: `bool unsafe_remount_no_delay = false;` with a two-line comment. `CasPool.cpp` ~712, in the `WaitForExpiry` branch:
```cpp
        if (policy == MountClaimPolicy::WaitForExpiry)
        {
            CasOperation claim_op = store->gc_requests.admit();
            if (store->config.unsafe_remount_no_delay)
            {
                /// One bare attempt first: a slot held by OUR uuid under another epoch is reclaimed at
                /// once under the operator's authorization, carrying the exact token this read saw so a
                /// slot that moves in between is refused. Every other outcome takes the ordinary path.
                claim = claimMount(claim_op, store->pool_layout, srid, our_uuid, writer_epoch, now_ms(), ttl_ms,
                                   /*proven_dead_incarnation=*/{}, emit_mount_event);
                if (claim.kind == MountClaimResult::LiveDoubleStart && claim.etag
                    && claim.body && claim.body->server_uuid == our_uuid)
                    claim = claimMount(claim_op, store->pool_layout, srid, our_uuid, writer_epoch, now_ms(), ttl_ms,
                                       {}, emit_mount_event, /*unsafe_reclaim_authorization=*/claim.etag);
            }
            if (claim.kind != MountClaimResult::Claimed || !store->config.unsafe_remount_no_delay)
                claim = claimMountAwaitingExpiry(...as today...);
        }
```
Keep `tryRemountOnce` (~1428) untouched (it never reads the knob).

- [ ] **Step 4: Build and run** `--gtest_filter='CASMountOpenWaits.*:CASSettings.*:CASMountAwaitExpiry.*'`, expect PASS (the nine `CASMountAwaitExpiry` tests unchanged).

### Task C3: Remount observes through `waitSleep`; a superseded incarnation does not reclaim a live successor

**Files:**
- Modify: `Pool/CasPool.cpp` (~1424: `const auto sleep_ms = [](uint64_t ms) { std::this_thread::sleep_for(...); };` → `[this](uint64_t ms) { mount_runtime.waitSleep(ms); }`)
- Test: `src/Disks/tests/gtest_cas_pool.cpp` (`CASPoolRemount` family, ~1602–1800)

- [ ] **Step 1: Refactor with the existing remount tests green**

Run `--gtest_filter='CASPoolRemount.*:CASRemountWaits.*'` before and after the one-line change; both PASS.

- [ ] **Step 2: Write the test** (read `FenceOutThenSelfRemountRestoresWrites` at ~1602 and `RemountArmAnchorsAtClaimAttemptNotResponseTime` at ~1769 for how a runtime is fenced and a remount driven; reuse their helpers)

```cpp
TEST(CASMountRemount, SupersededIncarnationDoesNotReclaimALiveSuccessor)
{
    /// A opens; B opens over A's slot with the unsafe setting (same server_id, a "copied uuid"). A's next
    /// renewal meets the token guard and becomes terminal; A's remount then OBSERVES (the knob is not
    /// consulted there) while B keeps renewing from inside A's wait callback -- so A never reclaims.
    auto b = std::make_shared<InMemoryBackend>();
    Layout l{"p"};
    uint64_t boot_a = 0, boot_b = 0;
    PoolPtr pool_b;
    auto config_for = [&](uint64_t * boot, bool unsafe, std::function<void(uint64_t)> on_wait)
    {
        return PoolConfig{
            .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "test",
            .mount_lease_ttl_ms = std::chrono::milliseconds(1000), .mount_renew_period = std::chrono::milliseconds(200),
            .cas_request_budget = CasRequestBudget{.attempt_timeout_ms = 50, .lease_safety_margin_ms = 50, .connect_timeout_cap_ms = std::nullopt},
            .unsafe_remount_no_delay = unsafe,
            .boot_ms_fn = [boot] { return *boot; },
            .wait_sleep_fn = std::move(on_wait),
        };
    };
    PoolPtr pool_a = Pool::open(b, config_for(&boot_a, false, [&](uint64_t ms) { boot_a += ms; }));
    pool_b = Pool::open(b, config_for(&boot_b, true, [&](uint64_t ms) { boot_b += ms; }));
    ASSERT_TRUE(pool_a && pool_b);

    /// A's renewal is now refused by the token guard.
    const MountRenewResult renewed_a = pool_a->renewMountForTest();
    EXPECT_EQ(renewed_a.outcome, MountRenewOutcome::Terminal);
    EXPECT_FALSE(pool_a->mountRuntimeForTest().mayMutate());

    /// A's remount observes; every poll of A's wait, B renews (a live successor bumps the token).
    size_t polls = 0;
    pool_a->setWaitSleepForTest([&](uint64_t ms)
    {
        boot_a += ms;
        ++polls;
        boot_b += ms;
        ASSERT_EQ(pool_b->renewMountForTest().outcome, MountRenewOutcome::Committed);
    });
    EXPECT_FALSE(pool_a->tryRemountOnceForTest());
    EXPECT_GT(polls, 0u);
    EXPECT_EQ(decodeMountLease(DB::Cas::tests::OperationForTest(b)->read(l.mountKey("test"), Retry::standard())->bytes).writer_epoch,
              pool_b->writerEpochForTest());
}
```
The `*ForTest` accessors: use the ones the `CASPoolRemount` tests already call (grep `ForTest(` in the 1602–1800 range) and add none that an existing test does not already need, except `setWaitSleepForTest` if absent (one setter on `PoolConfig::wait_sleep_fn` through `mount_runtime`).

- [ ] **Step 3: Cutoff-only fencing** — a second block in the same test or a sibling `CASMountRemount.CutoffFencesWithoutRenewals`: advance `boot_a` past `lastCommittedAttemptStartBootMs + TTL − margin` with no renewals and assert `mayMutate() == false`.

- [ ] **Step 4: Build and run** `--gtest_filter='CASMountRemount.*:CASPoolRemount.*'`.

### Task C4: GC's threshold is untouched by the knob

**Files:**
- Test: `src/Disks/tests/gtest_cas_gc_round.cpp` (find the fence-out test with `grep -n "gc_fenced\|fence-out\|FenceOut" src/Disks/tests/gtest_cas_gc_*.cpp`)

- [ ] **Step 1: Duplicate that test's fixture with `PoolConfig::unsafe_remount_no_delay = true` as `CASGcFenceOut.ThresholdUnchangedByUnsafeKnob`** and assert the same number of GC rounds/polls before the fence as the original asserts. Additionally assert statically that GC never reads the knob: `grep -c unsafe_remount_no_delay src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/` must print 0 (record the command output in the commit message).

- [ ] **Step 2: Run** `--gtest_filter='CASGcFenceOut.*'`.

### Task C5: Integration: hard kills with and without the knob

**Files:**
- Modify: `tests/integration/test_cas_mount_renewal_retry/configs/storage_conf.xml` (add `<cas_mount_lease_ttl_ms>1000</cas_mount_lease_ttl_ms>`, `<cas_mount_renew_period_ms>200</cas_mount_renew_period_ms>`, `<cas_attempt_timeout_ms>50</cas_attempt_timeout_ms>`, `<cas_lease_safety_margin_ms>50</cas_lease_safety_margin_ms>`), new `configs/unsafe_remount.xml` with `<cas_unsafe_remount_no_delay>1</cas_unsafe_remount_no_delay>` inside the same disk block
- Modify: `tests/integration/test_cas_mount_renewal_retry/test.py`

With `connect_timeout_ms` 1000 the frozen cap is `min(1000, 50) = 50`, envelope 50 + 2 × 50 = 150: `150 + 50 < 1000` and `200 + 300 + 50 < 1000` hold.

- [ ] **Step 1: Write the test**

```python
def test_hard_restart_observes_then_the_unsafe_knob_skips_the_observation(start_cluster):
    node = start_cluster["node"]
    def log_count(pattern):
        return int(node.exec_in_container(["bash", "-c", "grep -c '{}' /var/log/clickhouse-server/clickhouse-server.log || true".format(pattern)]).strip())
    observation = "waiting ~1150 ms (token-stability observation)"
    before = log_count(observation)
    epoch_before = int(node.query("SELECT writer_epoch FROM system.cas_mounts WHERE disk = '{}' LIMIT 1".format(DISK)).strip())
    node.stop_clickhouse(kill=True)
    node.start_clickhouse()
    assert log_count(observation) == before + 1
    assert _mount_snapshot(node)["state"] == "live"
    # Enable the knob while the server is stopped, then a second hard kill.
    node.stop_clickhouse(kill=True)
    node.copy_file_to_container(os.path.join(os.path.dirname(__file__), "configs/unsafe_remount.xml"),
                                "/etc/clickhouse-server/config.d/unsafe_remount.xml")
    node.start_clickhouse()
    assert log_count(observation) == before + 1
    assert _mount_snapshot(node)["state"] == "live"
    epoch_after = int(node.query("SELECT writer_epoch FROM system.cas_mounts WHERE disk = '{}' LIMIT 1".format(DISK)).strip())
    assert epoch_after > epoch_before
    node.exec_in_container(["rm", "/etc/clickhouse-server/config.d/unsafe_remount.xml"])
```
The observation log line text comes from `claimMountAwaitingExpiry` (`CasServerRoot.cpp` ~1005): change "incarnation-stability observation" to "token-stability observation" there (spec 2 docs rule 3) and keep the `~{} ms` value (1000 + 50 + 100 = 1150). Check `system.cas_mounts` has a `writer_epoch` column (`grep writer_epoch src/Storages/System/StorageSystemCasMounts.cpp`); if the column is named differently, use that name.

- [ ] **Step 2: Run the module**: `python3 -m ci.praktika run "integration" --test test_cas_mount_renewal_retry > build/test_c5_it.log 2>&1`.

### Task C6: Docs, texts, commit spec 2

- [ ] **Step 1**: `configuration.md`: new row for `cas_unsafe_remount_no_delay` with the description's words; a paragraph under the lease rows with rules 1–2 of the spec (same values on every member, observer's own clock, change only with every member stopped; `TTL − margin − period − 2 × envelope = 4 s` with defaults). `mounts-and-leases.md`: ~116 two formulas (startup `TTL + floor(TTL/20) + max(1, floor(period/2))`, GC `TTL + floor(TTL/20) + period`); `UncleanUnsafe` row in the `MountPriorState` table and a transition `Live --> Live: same-uuid claim under cas_unsafe_remount_no_delay, no observation` in the diagram; ~203 drop "materialization grace if the predecessor was unclean (default 30 s)" and the sentence "If the grace period consumed the TTL, ..." becomes "If the claim consumed the TTL, one fresh synchronous renewal re-anchors the deadline before the fence is armed". `CasServerRoot.cpp` ~909 double-start message: replace the CLOCK SKEW caveat with the token-stability statement (liveness is judged by the token holding stable on this server's own clock; `expires_at_ms` is diagnostic). `CasMountRuntime` stale comment: grep `expires_at` in `Pool/CasMountRuntime.h` and correct it.

- [ ] **Step 2: Commit and review** (`tmp/msg_spec2.txt`, explicit paths, `prompt_impl2.md`).

---

## Part D — Spec 3: connection churn

### Task D1: Reason counters (the only code before the spike)

**Files:**
- Modify: `src/Common/ProfileEvents.cpp` (~1567–1573 DISK block), `src/Common/HTTPConnectionPool.h` (`Metrics`, ~26), `src/Common/HTTPConnectionPool.cpp` (~131 DISK metrics, ~758 `wipeExpiredImpl`, ~857 `atConnectionDestroy`)
- Test: `src/Common/tests/gtest_connection_pool.cpp`

**Interfaces:**
- Produces: `Metrics` gains `reset_disconnected, reset_keep_alive_age, reset_response_not_keep_alive, reset_incomplete_request_or_response, reset_unread_buffered_data, reset_store_limit, reset_preserve_exception, expired_max_requests, expired_age, expired_stale_peer` (all `ProfileEvents::end()` by default, set only for the DISK group).

- [ ] **Step 1: Write the test** (the fixture's `getPool()` uses the HTTP group; add `getDiskPool()` returning `HTTPConnectionPools::instance().getPool(HTTPConnectionGroupType::DISK, uri, ProxyConfiguration{})`)

```cpp
TEST_F(ConnectionPoolTest, ResetAndExpiredReasonsAreCounted)
{
    auto ka = Poco::Timespan(1, 0);
    timeouts.withHTTPKeepAliveTimeout(ka);
    auto pool = getDiskPool();
    auto metrics = pool->getMetrics();
    /// Keep-alive age: hold a connection past 0.9 × keep-alive and return it.
    {
        auto connection = pool->getConnection(timeouts, nullptr);
        echoRequest("Hello", *connection);
        sleepForMilliseconds(950);
        echoRequest("Hello", *connection);
    }
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.reset]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.reset_keep_alive_age]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.reset_disconnected]);
    /// Stored connection older than 0.8 × keep-alive: expired by age at the next wipe.
    {
        auto connection = pool->getConnection(timeouts, nullptr);
        echoRequest("Hello", *connection);
    }
    sleepForMilliseconds(900);
    pool->wipeExpired();
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.expired]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.expired_age]);
    /// Max requests: the second request of a max_requests=1 session returns as Expired(max_requests).
    timeouts.withHTTPKeepAliveMaxRequests(1);
    {
        auto connection = pool->getConnection(timeouts, nullptr);
        echoRequest("Hello", *connection);
    }
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.expired_max_requests]);
    /// Disconnected: the server closes; the returned connection is reset(disconnected).
    {
        auto connection = pool->getConnection(timeouts, nullptr);
        echoRequest("Hello", *connection);
        getServer().stop();
        wait_until([&] { return !connection->connected() || isStale(*connection); });
    }
    ASSERT_GE(DB::CurrentThread::getProfileEvents()[metrics.reset_disconnected]
              + DB::CurrentThread::getProfileEvents()[metrics.expired_stale_peer], 1);
}
```
Adapt the last block to what the fixture allows (read `ReconnectedWhenConnectionIsHoldTooLong` ~739 and `MaxRequests` ~851 for the exact server handling); each branch of the two functions must be hit by at least one assertion or the test says why it cannot (store limit and preserve exception need the group limits: set `HTTPConnectionPools::instance().setLimits(...)` as `StoreLimit` ~665 does).

- [ ] **Step 2: Build, expect compile failures on the new `Metrics` fields**

- [ ] **Step 3: Implement**

`ProfileEvents.cpp` after `DiskConnectionsElapsedMicroseconds`:
```cpp
    M(DiskConnectionsResetDisconnected, "Number of disk HTTP connections returned to the pool already disconnected. Growth means the object store or a proxy closes connections under the client.", ValueType::Number) \
    M(DiskConnectionsResetKeepAliveAge, "Number of disk HTTP connections returned to the pool after a request that started later than 0.9 of the keep-alive timeout. Growth means requests are long relative to the keep-alive timeout.", ValueType::Number) \
    M(DiskConnectionsResetResponseNotKeepAlive, "Number of disk HTTP connections whose last response carried Connection: close. Growth means the object store refuses keep-alive.", ValueType::Number) \
    M(DiskConnectionsResetIncompleteRequestOrResponse, "Number of disk HTTP connections returned before the request was fully sent or the response fully received. Growth means readers abandon responses early.", ValueType::Number) \
    M(DiskConnectionsResetUnreadBufferedData, "Number of disk HTTP connections returned with unread buffered response bytes. Growth means readers stop before the end of a response.", ValueType::Number) \
    M(DiskConnectionsResetStoreLimit, "Number of disk HTTP connections dropped because the pool's store limit was reached. Growth means more connections finish than the pool may keep.", ValueType::Number) \
    M(DiskConnectionsResetPreserveException, "Number of disk HTTP connections dropped because storing them threw. A non-zero value indicates memory pressure at the pool.", ValueType::Number) \
    M(DiskConnectionsExpiredMaxRequests, "Number of disk HTTP connections retired after their keep-alive request limit. Growth is expected under sustained load and bounded by http_keep_alive_max_requests.", ValueType::Number) \
    M(DiskConnectionsExpiredAge, "Number of stored disk HTTP connections wiped because they were older than 0.8 of the keep-alive timeout (0.1 above the soft limit). Growth means connections idle longer than the keep-alive timeout allows.", ValueType::Number) \
    M(DiskConnectionsExpiredStalePeer, "Number of stored disk HTTP connections wiped because the peer had already closed them. Growth means the object store closes idle connections before the client's keep-alive timeout.", ValueType::Number) \
```
`HTTPConnectionPool.h` `Metrics`: the ten new `const ProfileEvents::Event ... = ProfileEvents::end();` fields. `getMetricsForDiskConnectionPool` sets them. `atConnectionDestroy`:
```cpp
        if (connection.getKeepAliveRequest() >= connection.getKeepAliveMaxRequests())
        {
            ProfileEvents::increment(getMetrics().expired, 1);
            ProfileEvents::increment(getMetrics().expired_max_requests, 1);
            return;
        }
        const ProfileEvents::Event reason
            = !connection.connected() ? getMetrics().reset_disconnected
            : connection.isKeepAliveExpired(connection.getKeepAliveReliability()) ? getMetrics().reset_keep_alive_age
            : connection.mustReconnect() ? getMetrics().reset_response_not_keep_alive
            : !connection.isCompleted() ? getMetrics().reset_incomplete_request_or_response
            : connection.buffered() ? getMetrics().reset_unread_buffered_data
            : group->isStoreLimitReached() ? getMetrics().reset_store_limit
            : ProfileEvents::end();
        if (reason != ProfileEvents::end())
        {
            ProfileEvents::increment(getMetrics().reset, 1);
            ProfileEvents::increment(reason, 1);
            return;
        }
```
(`ProfileEvents::increment` on `end()` must not be called: guard every reason increment with `if (event != ProfileEvents::end())` inside a small `static void countReason(ProfileEvents::Event)` helper, since the HTTP and STORAGE groups leave them unset.) The `catch` that today increments `reset` after a failed store adds `reset_preserve_exception`. `wipeExpiredImpl`: count per popped connection: `isExpired(...) ? expired_age : expired_stale_peer` (the age check is evaluated first, as today), through the same helper; the aggregate `expired` increment in the `SCOPE_EXIT` stays. Verify `isKeepAliveExpired(getKeepAliveReliability())` is exactly what `mustReconnect` consults for its age half (`base/poco/Net/src/HTTPClientSession.cpp` ~375 and the `mustReconnect` definition) before writing the ordering.

- [ ] **Step 4: Build and run** `--gtest_filter='ConnectionPoolTest.*'`; commit spec 3 part 1 (`src/Common/ProfileEvents.cpp src/Common/HTTPConnectionPool.h src/Common/HTTPConnectionPool.cpp src/Common/tests/gtest_connection_pool.cpp`) with the message "cas: count disk connection resets and expiries by reason"; codex review.

### Task D2: The A/B spike (recorded, not committed as code)

- [ ] **Step 1: Stand.** Start rustfs as `tests/integration/helpers/clickhouse_proc.py::start_rustfs` does (read it for the binary path and flags); assert `rustfs --version` prints `1.0.0-rc.3`, else download the rc.3 binary to a versioned filename (`ci/tmp/rustfs-1.0.0-rc.3`) and fail closed if the version string mismatches. One server (`build/programs/clickhouse server`) with the lane's CAS disk config (`tests/config/config.d/cas_s3_storage*.xml`, find with `grep -rl metadata_type.*cas tests/config/config.d/`), `metric_log`/`text_log` on a local disk.
- [ ] **Step 2: Data and load.** `CREATE TABLE spike (k UInt64, v String) ENGINE = ReplacingMergeTree ORDER BY k SETTINGS storage_policy = '<cas policy>'`; insert 200 parts of 10k rows; a JOIN input table on the same policy. Load: `clickhouse-benchmark --concurrency N -i 0 --timelimit 300 < queries.sql` where `queries.sql` is 20 point/range reads with JOINs; ramp `N` from 4 doubling until `DiskS3GetObject` per second ≈ 400 or rustfs 5xx appear (record the found `N`).
- [ ] **Step 3: Arms.** For each of `5 s/100`, `30 s/100`, `5 s/10000`, `30 s/10000` (`http_keep_alive_timeout`/`http_keep_alive_max_requests` in the disk block) plus one arm with `<disk_connections_soft_limit>100</disk_connections_soft_limit>`: restart the server (new client pool), wait for TIME_WAIT to rustfs (`ss -tan state time-wait '( dport = :9001 )' | wc -l`) below 500, run the load twice with the arm order alternated (A B B A), record per run: `SELECT event, value FROM system.events WHERE event LIKE 'DiskConnections%' OR event LIKE 'DiskS3%Request%' OR event LIKE 'CASMount%'`, `DiskConnectionsTotal/Stored` from `system.metrics`, `grep -c 'Cannot assign requested address' <server log>`, TIME_WAIT peak, completed queries (from `clickhouse-benchmark` output), `cat /proc/sys/net/ipv4/ip_local_port_range`, `net.ipv4.tcp_tw_reuse`.
- [ ] **Step 4: Verdict.** Baseline is valid only if `EADDRNOTAVAIL > 0` or TIME_WAIT peak > 50% of the range. A treatment passes with zero `EADDRNOTAVAIL`, zero `CASMountLeaseLost`/`CASMountRenewalDeadlineExceeded`, TIME_WAIT peak < 25% of the range, completed queries within 10% of baseline, and `DiskS3GetObject / DiskConnectionsCreated` higher than the baseline by more than the spread between its two runs. Also record rustfs's `Keep-Alive` response header and whether a socket reused after 10/20/40 s idle is still accepted (`curl --keepalive-time`-style probe with `nc` or a Python `http.client` connection held across `time.sleep`).
- [ ] **Step 5: Record** the table (arm × counters with the reason breakdown) in `docs/superpowers/specs/2026-09-05-cas-connection-churn-design.md` under a new `## Implementation record {#implementation-record}` section, with the chosen `http_keep_alive_timeout` (and `max_requests` only if the reason breakdown attributes churn to `ExpiredMaxRequests`). Commit that section alone.

### Task D3: The CAS client profile

**Files:**
- Modify: `src/Disks/DiskObjectStorage/RegisterDiskObjectStorage.cpp` (~62), `src/Disks/DiskObjectStorage/ObjectStorages/ObjectStorageFactory.h/.cpp` (S3 creator ~133), `src/Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.h/.cpp` (constructor, `applyNewSettings` ~1018)
- Test: `src/Disks/tests/gtest_cas_s3_client_profile.cpp`

**Interfaces:**
- Produces: `struct S3ClientProfile { std::optional<uint64_t> http_keep_alive_timeout; std::optional<uint64_t> http_keep_alive_max_requests; }` (in `S3ObjectStorage.h`); `S3ObjectStorage::applyClientProfileDefaults(S3Settings &) const` applies each present value only where `settings.auth_settings[S3AuthSetting::<name>].changed == false`; `ObjectStorageFactory::create(name, config, config_prefix, context, skip_access_check, const ObjectStorageCreateHints & hints = {})` with `struct ObjectStorageCreateHints { bool cas_client_profile = false; }`; `RegisterDiskObjectStorage` sets `hints.cas_client_profile = config.getString(config_prefix + ".metadata_type", "") == "cas"` before creating the object storage(s).

- [ ] **Step 1: Tests** (same file as B2's client test; construct `S3Settings` from an XML config string with `Poco::Util::XMLConfiguration`, as `gtest_cas_s3_staging.cpp` does):

```cpp
TEST(S3ObjectStorageProfile, CasDefaultsApplyOnlyWhenUnset)
{
    /// A CAS disk without the settings gets the profile; explicit disk values win; a non-CAS disk is untouched.
    const S3ClientProfile profile{.http_keep_alive_timeout = 30, .http_keep_alive_max_requests = std::nullopt};
    {
        auto settings = settingsFromXml("<disk><endpoint>http://127.0.0.1:1/b/</endpoint></disk>");
        S3ObjectStorage::applyClientProfileDefaults(profile, *settings);
        EXPECT_EQ(settings->auth_settings[S3AuthSetting::http_keep_alive_timeout].value, 30u);
        EXPECT_EQ(settings->auth_settings[S3AuthSetting::http_keep_alive_max_requests].value, S3::DEFAULT_KEEP_ALIVE_MAX_REQUESTS);
    }
    {
        auto settings = settingsFromXml("<disk><endpoint>http://127.0.0.1:1/b/</endpoint><http_keep_alive_timeout>7</http_keep_alive_timeout></disk>");
        S3ObjectStorage::applyClientProfileDefaults(profile, *settings);
        EXPECT_EQ(settings->auth_settings[S3AuthSetting::http_keep_alive_timeout].value, 7u);
    }
    {
        /// A changed global `s3_http_keep_alive_timeout` counts as explicit: the loader marks it changed.
        DB::Settings global;
        global.set("s3_http_keep_alive_timeout", 11);
        auto settings = settingsFromXml("<disk><endpoint>http://127.0.0.1:1/b/</endpoint></disk>", global);
        S3ObjectStorage::applyClientProfileDefaults(profile, *settings);
        EXPECT_EQ(settings->auth_settings[S3AuthSetting::http_keep_alive_timeout].value, 11u);
    }
}

TEST(S3ObjectStorageProfile, ApplyNewSettingsPreservesTheProfile)
{
    /// Build a storage with the profile, reload it from a config without the setting, and read the effective value back.
    auto storage = storageWithProfile(S3ClientProfile{.http_keep_alive_timeout = 30});
    storage->applyNewSettings(*configWithout("http_keep_alive_timeout"), "disk", contextForTest(), ApplyNewSettingsOptions{.allow_client_change = true});
    EXPECT_EQ(storage->getS3StorageClient()->getClientConfiguration().http_keep_alive_timeout, 30u);
}

TEST(RegisterDiskObjectStorage, CasProfileReachesTheS3Creator)
{
    /// The factory's S3 creator applies the profile exactly when the hint says so. `getClient` builds
    /// the client without connecting, so an unreachable endpoint is fine.
    auto cfg = makeConfig("<type>s3</type><endpoint>http://127.0.0.1:1/bucket/</endpoint>"
                          "<access_key_id>a</access_key_id><secret_access_key>b</secret_access_key>");
    auto with_hint = ObjectStorageFactory::instance().create("d", *cfg, "disk", contextForTest(), /*skip_access_check=*/true,
                                                             ObjectStorageCreateHints{.cas_client_profile = true});
    auto without_hint = ObjectStorageFactory::instance().create("d", *cfg, "disk", contextForTest(), true, ObjectStorageCreateHints{});
    EXPECT_EQ(with_hint->getS3StorageClient()->getClientConfiguration().http_keep_alive_timeout, 30u);
    EXPECT_EQ(without_hint->getS3StorageClient()->getClientConfiguration().http_keep_alive_timeout, S3::DEFAULT_KEEP_ALIVE_TIMEOUT);
    /// And `RegisterDiskObjectStorage` derives the hint from `metadata_type`: pin the one-line rule.
    EXPECT_TRUE(casClientProfileHintFor(*makeConfig("<metadata_type>cas</metadata_type>"), "disk"));
    EXPECT_FALSE(casClientProfileHintFor(*makeConfig("<metadata_type>local</metadata_type>"), "disk"));
}
```
`casClientProfileHintFor(config, prefix)` is the small free function `RegisterDiskObjectStorage.cpp` uses to derive the hint (declared in `RegisterDiskObjectStorage.h`), so the rule is testable without a `DiskFactory` round trip.
`settingsFromXml`, `storageWithProfile`, `configWithout`, `contextForTest` are helpers in this file: `settingsFromXml` builds `Poco::AutoPtr<Poco::Util::XMLConfiguration>` from a `std::istringstream`, then `S3Settings::loadFromConfigForObjectStorage(*config, "disk", global_settings, "http", /*validate*/ false)`; `contextForTest` uses `DB::tests::TestGlobalContext` if present in `src/Common/tests/` (grep `getContext()` in `src/Disks/tests/gtest_cas_settings.cpp` for the helper CAS tests already use). If the last test needs a Local disk through `DiskFactory`, it may be simpler to assert on the S3 creator by calling `ObjectStorageFactory::instance().create(...)` directly with `hints.cas_client_profile = true` against a config with `<type>s3</type>` and an unreachable endpoint (`getClient` does not connect), then read `getS3StorageClient()->getClientConfiguration().http_keep_alive_timeout == 30`.

- [ ] **Step 2: Build, expect compile failures**

- [ ] **Step 3: Implement**

`ObjectStorageFactory.h`: `struct ObjectStorageCreateHints { bool cas_client_profile = false; };`, the `Creator` signature and `create` gain `const ObjectStorageCreateHints &`. S3 creator: after `loadFromConfigForObjectStorage`, `if (hints.cas_client_profile) S3ObjectStorage::applyClientProfileDefaults(S3ObjectStorage::casClientProfile(), *settings);` and pass the profile into the `S3ObjectStorage` constructor (new trailing parameter `std::optional<S3ClientProfile> client_profile = std::nullopt`, stored). `casClientProfile()` returns the value chosen by D2 (`http_keep_alive_timeout` from the record; `max_requests` only if the record says so). `applyClientProfileDefaults`:
```cpp
void S3ObjectStorage::applyClientProfileDefaults(const S3ClientProfile & profile, S3Settings & settings)
{
    /// A default, never an override: a value the disk section or a changed global setting supplied
    /// keeps precedence, which the loader records as `changed`.
    if (profile.http_keep_alive_timeout && !settings.auth_settings[S3AuthSetting::http_keep_alive_timeout].changed)
        settings.auth_settings[S3AuthSetting::http_keep_alive_timeout] = *profile.http_keep_alive_timeout;
    if (profile.http_keep_alive_max_requests && !settings.auth_settings[S3AuthSetting::http_keep_alive_max_requests].changed)
        settings.auth_settings[S3AuthSetting::http_keep_alive_max_requests] = *profile.http_keep_alive_max_requests;
}
```
(Assigning through the subscript operator sets `changed`; assert in the first test that a SECOND application does not alter an explicit value — it cannot, because the first application already marked it changed, which is the intended precedence.) `applyNewSettings`: after `apply_config_settings()`/`apply_endpoint_settings()` and before the dialect check: `if (client_profile) applyClientProfileDefaults(*client_profile, *modified_settings);`. `RegisterDiskObjectStorage.cpp`: `bool casClientProfileHintFor(const Poco::Util::AbstractConfiguration & config, const String & config_prefix) { return config.getString(config_prefix + ".metadata_type", "") == "cas"; }` (declared in `RegisterDiskObjectStorage.h`), and `ObjectStorageCreateHints hints{.cas_client_profile = casClientProfileHintFor(config, config_prefix)};` passed to both `create` calls (locations and main). Every other caller of `ObjectStorageFactory::create` (grep) passes `{}` by default.

- [ ] **Step 4: Build and run** `--gtest_filter='S3ObjectStorageProfile.*:RegisterDiskObjectStorage.*:CAS*'`.

### Task D4: Docs, commit spec 3 part 2

- [ ] **Step 1**: `configuration.md`: a paragraph "CAS client profile" under the disk settings: the values, the precedence (explicit disk XML > changed `s3_http_keep_alive_*` > CAS defaults), the effective pool ages (`0.8 ×` keep-alive for stored connections, `0.1 ×` above `disk_connections_soft_limit`, `0.9 ×` for a returned connection's age), and a pointer to the ten `DiskConnections*` reason events.
- [ ] **Step 2**: Commit (`tmp/msg_spec3b.txt`: "cas: apply a longer keep-alive profile to the S3 client of CAS disks" with the spike's before/after numbers in the body) with explicit paths; codex review.

---

## Final gate

- [ ] Run the whole CAS gtest gate: `build/src/unit_tests_dbms --gtest_filter='CAS*' > build/test_final_gate.log 2>&1` — all PASS; the ASan build too (`build_asan`) for the suites touched (`CASRequests*`, `CASHeartbeat*`, `CASMount*`).
- [ ] Run `python3 -m ci.praktika run "integration" --test test_cas_gcs` and `--test test_cas_mount_renewal_retry` — all PASS.
- [ ] Run the stateless CAS lane locally for the request-engine tests: `python3 -m ci.praktika run "Stateless tests (amd_binary, cas s3 storage, parallel)" --test 05024` (and the CAS-tagged tests that exercise conditional writes: grep `cas` in `tests/queries/0_stateless/*.sh` names).
- [ ] Update `tmp/pr2300-cicd-watch/STATE.md` with the commit list; do not push.
