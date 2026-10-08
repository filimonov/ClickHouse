# CAS lease PR 6: authoritative absence per verb — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:executing-plans (or
> superpowers:subagent-driven-development) to implement this plan task by task. Steps use checkbox
> (`- [ ]`) syntax for tracking.

**Goal:** a CAS `GET` proves a key absent only when the store names the miss `NoSuchKey`. A `GET` 404
the SDK could not name (`RESOURCE_NOT_FOUND`: no body, or a body it could not parse), `NoSuchBucket`
and routing errors are faults: the request engine reissues them to the deadline, and nothing becomes
`ProvenAbsent`. A `HEAD` 404 stays absence, as today. `FILE_DOESNT_EXIST` on local and emulated
backends stays absence.

**Why now:** from PR 12 on, `ProvenAbsent` on the mount slot stops the server (`slot_absent`, row R13
of spec section 2 "Потеря"). A 404 from a misrouted request or a proxy must not reach that row.

**Architecture:** every Native read that can answer "absent" goes through one classifier,
`isObjectNotFound` in `CA/Backend/CasObjectStorageBackend.cpp`. Its three callers are all `GET`-shaped:
`ObjectStorageBackend::readUnder` (both modes) and `ObjectStorageBackend::stream`. The fix removes
`RESOURCE_NOT_FOUND` from it. `HEAD` never reaches it: `ObjectStorageBackend::nativeHead` calls
`S3ObjectStorage::tryGetObjectMetadataWithNativeToken`, and `S3::getObjectInfoIfExists`
(`src/IO/S3/getObjectInfo.cpp`, `isNotFoundError`) decides absence there. That path is not touched.

**Tech stack:** C++23, gtest (`unit_tests_dbms`), a local Poco HTTP server in front of a real
`S3ObjectStorage` and `S3::Client`, `FakeClock`; praktika integration runs.

**Spec:** `/home/mfilimonov/workspace/ClickHouse/master/docs/superpowers/specs/2026-10-07-cas-mount-lease-series-design.md`,
section 4 "Срок запроса и отсутствие", paragraph "Отсутствие"; section 2 "Потеря" (the three
observations, R13); section 7 row 6 in both tables (PR row: "Авторитетное отсутствие по глаголу: `GET`
— только `NoSuchKey`; интеграционная приёмка на S3, GCS, RustFS", +20, after: —, closes: —; tests row:
"`GET` с `404` без имени → не отсутствие; `HEAD` `404` → отсутствие; интеграция: пустой пул
монтируется на трёх хранилищах"). Section 9 names no model for PR 6.

**Code base:** `/home/mfilimonov/workspace/ClickHouse/cas-95-1`, base `5b7c9f503df`
(`altinity/antalya-26.6`). `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`.

## Facts established by reading the code

How the SDK names a 404 (`contrib/aws/src/aws-cpp-sdk-core/source/client/AWSErrorMarshaller.cpp`):

| Response | SDK error type | Source |
|---|---|---|
| 404 with XML `<Code>NoSuchKey</Code>` | `NO_SUCH_KEY` | `XmlErrorMarshaller::Marshall` → `S3ErrorMapper` |
| 404 with XML `<Code>NoSuchBucket</Code>` | `NO_SUCH_BUCKET` | same |
| 404 with no body (every `HEAD`) | `RESOURCE_NOT_FOUND` | `GuessBodylessErrorType` |
| 404 with a body that is not XML | `RESOURCE_NOT_FOUND` | `FindErrorByHttpResponseCode` |

`ReadBufferFromS3::sendRequest` throws `S3Exception(error.GetMessage(), error.GetErrorType())` with no
exception name. On the `GET` path only the enum `NO_SUCH_KEY` reaches `isObjectNotFound`; the
`getExceptionName() == "NoSuchKey"` arm stays for throw sites that attach the name (test fakes, and the
same check in `finalizeConditionalWrite`).

Which verb each engine read goes by (`CA/Backend/CasRequests.cpp`):

| Engine entry | Verb | Lease-series callers |
|---|---|---|
| `CasOperation::read` | `GET` (`Backend::read` → `readUnder`) | `claimMount`, `claimMountAwaitingExpiry` (slot), `readOwnerObject` (owner), `Pool::open` pool meta |
| `CasOperation::probeSentinel` | `GET` (`probeSentinelUnder` → `readUnder`) | pool meta and owner probes in `CasPool.cpp`, mount probe in `allocateWriterEpoch` |
| `CasOperation::readModifyWrite` | `GET` (`observe`) | `allocateWriterEpoch` (epoch) |
| resolve read after a refused precondition, `ResolveWith::Body` (`create`, `replace`, `readModifyWrite`) | `GET` (`observe`) | slot claim and renewal `replace`, owner `create` |
| `CasOperation::stream` | `GET` (`Backend::stream`) | staged-copy source, GC source-edge runs; both throw on `nullptr` |
| `CasOperation::head`, `readModifyWriteOnPresence`, resolve with `ResolveWith::Presence` | `HEAD` (`observePresence`) | `CasPlainObjects.cpp` |

So every read the lease series classifies goes by `GET`. The existing test
`CASSentinelProbe.NativeClassifiesResourceNotFoundAsKeyAbsent` justifies absence by a `HEAD` answer,
while the probe it drives reads by `GET`; Task 3 replaces it.

`isAuthoritativeAbsence` (`CasRequests.cpp`) decides whether `readLoop` stops reissuing. It matches
`NO_SUCH_KEY` and `NO_SUCH_UPLOAD` only, so after the fix a nameless `GET` 404 is an ordinary
reissued fault and gives up with `NETWORK_ERROR` "gave up at the policy deadline". It is not changed.

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

- `isObjectNotFound` keeps `NO_SUCH_KEY`, the `NoSuchKey` name and `FILE_DOESNT_EXIST`, and loses only
  `RESOURCE_NOT_FOUND`. `CASAbsencePerVerb.GetNamedNoSuchKeyIsProvenAbsent` and
  `LocalFileAbsenceIsAbsentInBothModes` fence the kept arms.
- `HEAD` absence is unchanged: `HeadNotFoundIsProvenAbsent` is green before and after.
- A nameless 404 on the resolve read after a 412 becomes `Conflict{NotObserved}`, and a nameless 404 on
  the first read of `readModifyWrite` sends no write. Both are asserted on the wire (method list).
- `stream` changes too: it is a `GET` through the same classifier. Its two callers already throw on
  absence, so the visible change is the error kind and the reissue.
- Not changed, deliberately: conditional `DELETE` 404 (`S3ObjectStorage::removeObjectIfTokenMatches`
  maps any `isNotFoundError` to `Gone`, which callers treat like a mismatch); `HEAD`'s
  `isNotFoundError` also counting `NO_SUCH_BUCKET` (a bodyless response cannot carry that name).
- The tests do not prove what a real store sends; the integration runs in Task 6 do.

## Files

| File | Change |
|---|---|
| `CA/Backend/CasObjectStorageBackend.cpp` | `isObjectNotFound`: drop `RESOURCE_NOT_FOUND`, rewrite its comment; `probeSentinelUnder`: name the unnamed 404 in the `Indeterminate` comment |
| `CA/Backend/CasObjectStorageBackend.h` | doc of `probeSentinelRaw`: an unnamed 404 is `Indeterminate` |
| `src/Disks/tests/gtest_cas_absence_per_verb.cpp` | new: wire-level tests, suite `CASAbsencePerVerb` |
| `src/Disks/tests/gtest_cas_sentinel_probe.cpp` | replace `NativeClassifiesResourceNotFoundAsKeyAbsent`; one comment word |
| `src/Disks/tests/gtest_cas_s3_staging.cpp` | `FakeGenerationObjectStorage`: its `GET` miss throws the named `NoSuchKey` |
| `docs/en/antalya/cas/bucket-requirements.md` | one capability-table row |

No production file outside `CA/` is touched.

Test-fake sweep, derived with
`grep -rn 'RESOURCE_NOT_FOUND' src/Disks/tests src/IO/tests src/IO/S3/tests`: two CAS test files
throw it (`gtest_cas_sentinel_probe.cpp`, `gtest_cas_s3_staging.cpp`); the two hits in
`src/IO/tests/gtest_writebuffer_s3.cpp` and `gtest_cas_writebuffer_s3.cpp` are comments about
`WriteBufferFromS3`, not CAS reads. Re-run the grep after Task 4: only the sentinel test (as
`Indeterminate`), the staging fake's `getObjectMetadata` and the new test file may remain.

---

## Task 1: Branch

- [ ] **Step 1: Create the branch from the base**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
git status --short -- src/Disks docs/en/antalya    # expect no output
git switch -c cas-lease-pr06-absence-per-verb 5b7c9f503df
```

If `git status` shows edits, stop and report; they belong to another agent.

## Task 2: Golden wire tests (green on base)

**Files:** create `src/Disks/tests/gtest_cas_absence_per_verb.cpp`. `src/CMakeLists.txt` globs
`gtest*.cpp` with `CONFIGURE_DEPENDS`, so no build file changes.

**Consumes:** `DB::S3::ClientFactory`, `DB::S3ObjectStorage`, `ObjectStorageBackend` (5-argument
constructor), `CasRequests(BackendPtr, Fence, now, sleep)`, `FakeClock`, `makeLocalObjectStorageForTest`,
`nativeKeyUnder`, `expectThrowsCodeWithMessage` (`src/Disks/tests/cas_test_helpers.h`).

**Produces:** `VerbRecordingServer`, `Rig`, `notFound`, `namedNotFound`, `bodylessNotFound`, `Methods`,
and five tests. Every helper in the anonymous namespace is used in this task: an unused one fails the
build (`-Wunused-function`), which is why `unparseableNotFound` arrives in Task 3.

- [ ] **Step 1: Write the file**

```cpp
#include <gtest/gtest.h>

#include "config.h"

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasFence.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasObjectStorageBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/tests/cas_test_helpers.h>

#include <memory>
#include <string>
#include <vector>

#if USE_AWS_S3
#include <Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.h>
#include <IO/S3/Client.h>
#include <IO/S3/PocoHTTPClient.h>
#include <IO/S3/URI.h>
#include <IO/S3Common.h>
#include <IO/S3Settings.h>
#include <Common/HTTPConnectionPool.h>
#include <Common/RemoteHostFilter.h>
#include <Common/tests/gtest_global_context.h>

#include <Poco/AutoPtr.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/SharedPtr.h>
#include <Poco/ThreadPool.h>

#include <functional>
#include <limits>
#include <mutex>
#include <utility>
#endif

using namespace DB::Cas;

/// Local and emulated stores answer a missing file with `FILE_DOESNT_EXIST`, which stays absence.
TEST(CASAbsencePerVerb, LocalFileAbsenceIsAbsentInBothModes)
{
    for (const auto mode : {ObjectStorageBackend::Mode::Native, ObjectStorageBackend::Mode::EmulatedSingleProcess})
    {
        auto storage = DB::Cas::tests::makeLocalObjectStorageForTest();
        CasRequests requests(std::make_shared<ObjectStorageBackend>(storage, mode), Fence::open());
        auto op = requests.admit();
        const String key = mode == ObjectStorageBackend::Mode::Native
            ? DB::Cas::tests::nativeKeyUnder(storage, "absent/key")
            : String("absent/key");
        EXPECT_FALSE(op.read(key, Retry::once()).has_value());
        EXPECT_EQ(op.probeSentinel(key, Retry::once()).outcome, ProbeOutcome::KeyAbsent);
    }
}

#if USE_AWS_S3

namespace DB::ErrorCodes
{
    extern const int NETWORK_ERROR;
}

namespace DB::S3RequestSetting
{
    extern const S3RequestSettingsUInt64 max_single_read_retries;
}

namespace
{

using Methods = std::vector<std::string>;

/// A local endpoint standing in for S3. `answer` writes the response for each request method, and
/// every method is recorded, which is how a test states the verb an engine read went by.
class VerbRecordingServer
{
public:
    using Answer = std::function<void(const std::string & method, Poco::Net::HTTPServerResponse &)>;

    explicit VerbRecordingServer(Answer answer_)
        : answer(std::move(answer_))
        , socket(std::make_unique<Poco::Net::ServerSocket>(Poco::Net::SocketAddress("127.0.0.1", 0)))
        , factory(new Factory(*this))
        , params(new Poco::Net::HTTPServerParams())
        /// A private pool: the default one is shared by every local-server test in the binary.
        , pool("CASAbsenceServer")
        , server(std::make_unique<Poco::Net::HTTPServer>(factory, pool, *socket, params))
    {
        server->start();
    }

    ~VerbRecordingServer()
    {
        DB::HTTPConnectionPools::instance().dropCache();
        server->stop();
        pool.joinAll();
    }

    std::string url() const { return "http://" + socket->address().toString(); }

    Methods takeMethods()
    {
        std::lock_guard lock(mutex);
        return std::exchange(methods, {});
    }

private:
    class Handler : public Poco::Net::HTTPRequestHandler
    {
    public:
        explicit Handler(VerbRecordingServer & owner_) : owner(owner_) {}

        void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override
        {
            {
                std::lock_guard lock(owner.mutex);
                owner.methods.push_back(request.getMethod());
            }
            request.stream().ignore(std::numeric_limits<std::streamsize>::max());
            owner.answer(request.getMethod(), response);
        }

    private:
        VerbRecordingServer & owner;
    };

    class Factory : public Poco::Net::HTTPRequestHandlerFactory
    {
    public:
        explicit Factory(VerbRecordingServer & owner_) : owner(owner_) {}

        Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override
        {
            return new Handler(owner);
        }

    private:
        VerbRecordingServer & owner;
    };

    Answer answer;
    std::mutex mutex;
    Methods methods;
    std::unique_ptr<Poco::Net::ServerSocket> socket;
    Poco::SharedPtr<Factory> factory;
    Poco::AutoPtr<Poco::Net::HTTPServerParams> params;
    Poco::ThreadPool pool;
    std::unique_ptr<Poco::Net::HTTPServer> server;
};

void sendEmpty(Poco::Net::HTTPServerResponse & response, Poco::Net::HTTPResponse::HTTPStatus status)
{
    response.setStatus(status);
    response.setContentLength(0);
    response.send();
}

/// The message repeats the code: `S3Exception::isPreconditionFailed` also matches the token there.
void sendXmlError(Poco::Net::HTTPServerResponse & response, Poco::Net::HTTPResponse::HTTPStatus status, const std::string & code)
{
    const std::string body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>" + code + "</Code><Message>" + code
        + ": scripted</Message><RequestId>1</RequestId></Error>";
    response.setStatus(status);
    response.setContentType("application/xml");
    response.setContentLength(body.size());
    response.send() << body;
}

/// Every answer refuses a `PUT` with 412 and answers a `HEAD` with a bodyless 404; only the `GET` 404 differs.
VerbRecordingServer::Answer notFound(std::function<void(Poco::Net::HTTPServerResponse &)> get)
{
    return [get = std::move(get)](const std::string & method, Poco::Net::HTTPServerResponse & response)
    {
        if (method == "PUT")
            sendXmlError(response, Poco::Net::HTTPResponse::HTTP_PRECONDITION_FAILED, "PreconditionFailed");
        else if (method == "GET")
            get(response);
        else
            sendEmpty(response, Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
    };
}

VerbRecordingServer::Answer namedNotFound(const std::string & code)
{
    return notFound([code](auto & response) { sendXmlError(response, Poco::Net::HTTPResponse::HTTP_NOT_FOUND, code); });
}

VerbRecordingServer::Answer bodylessNotFound()
{
    return notFound([](auto & response) { sendEmpty(response, Poco::Net::HTTPResponse::HTTP_NOT_FOUND); });
}

/// A genuine `S3ObjectStorage` over `endpoint`, with no SDK retry and one `ReadBufferFromS3` attempt, so
/// the recorded methods are exactly the requests the engine sent.
std::shared_ptr<DB::S3ObjectStorage> makeStorage(const std::string & endpoint)
{
    (void)getContext();
    DB::RemoteHostFilter remote_host_filter;
    DB::S3::PocoHTTPClientConfiguration cfg = DB::S3::ClientFactory::instance().createClientConfiguration(
        "us-east-1",
        remote_host_filter,
        /* s3_max_redirects = */ 10,
        DB::S3::PocoHTTPClientConfiguration::RetryStrategy{.max_retries = 0},
        /* s3_slow_all_threads_after_network_error = */ false,
        /* s3_slow_all_threads_after_retryable_error = */ false,
        /* enable_s3_requests_logging = */ false,
        /* for_disk_s3 = */ true,
        /* opt_disk_name = */ {},
        /* request_throttler = */ {});
    cfg.endpointOverride = endpoint;
    cfg.connectTimeoutMs = 5000;
    cfg.requestTimeoutMs = 5000;
    cfg.s3_use_adaptive_timeouts = false;
    /// Each test owns its ephemeral-port server; a pooled connection must not outlive it.
    cfg.http_keep_alive_timeout = 0;
    auto client = DB::S3::ClientFactory::instance().create(
        cfg,
        DB::S3::ClientSettings{
            .use_virtual_addressing = false, .disable_checksum = false, .gcs_issue_compose_request = false, .is_s3express_bucket = false},
        "ACCESS_KEY_ID", "SECRET_ACCESS_KEY", "", {}, {}, DB::S3::CredentialsConfiguration{});
    auto settings = std::make_unique<DB::S3Settings>();
    settings->request_settings[DB::S3RequestSetting::max_single_read_retries] = 1;
    return std::make_shared<DB::S3ObjectStorage>(
        std::move(client), std::move(settings), DB::S3::URI(endpoint + "/test-bucket/"), DB::S3Capabilities{},
        DB::ObjectStorageKeyGeneratorPtr{}, "disk");
}

/// A writable Native backend over the server, as a writable mount builds it: single-attempt control plane.
struct Rig
{
    VerbRecordingServer server;
    std::shared_ptr<DB::S3ObjectStorage> storage;
    DB::Cas::tests::FakeClock clock;
    CasRequests requests;

    explicit Rig(VerbRecordingServer::Answer answer)
        : server(std::move(answer))
        , storage(makeStorage(server.url()))
        , requests(
              std::make_shared<ObjectStorageBackend>(
                  storage, ObjectStorageBackend::Mode::Native,
                  /*single_attempt_control_plane_=*/true, /*attempt_timeout_ms_=*/5000, /*connect_timeout_cap_ms_=*/5000),
              Fence::open(), clock.nowFn(), clock.sleepFn())
    {
    }
};

std::optional<String> declineObject(const std::optional<Object> &) { return std::nullopt; }
std::optional<String> declineMeta(const std::optional<Meta> &) { return std::nullopt; }

}

TEST(CASAbsencePerVerb, EachEngineReadGoesByItsVerb)
{
    Rig rig(namedNotFound("NoSuchKey"));
    auto op = rig.requests.admit();

    (void)op.read("k", Retry::once());
    EXPECT_EQ(rig.server.takeMethods(), (Methods{"GET"}));
    (void)op.probeSentinel("k", Retry::once());
    EXPECT_EQ(rig.server.takeMethods(), (Methods{"GET"}));
    (void)op.stream("k", Retry::once());
    EXPECT_EQ(rig.server.takeMethods(), (Methods{"GET"}));
    (void)op.readModifyWrite("k", declineObject, Retry::once());
    EXPECT_EQ(rig.server.takeMethods(), (Methods{"GET"}));
    (void)op.create("k", "v", Retry::once());
    EXPECT_EQ(rig.server.takeMethods(), (Methods{"PUT", "GET"})) << "the resolve read after a 412 reads the body";
    (void)op.head("k", Retry::once());
    EXPECT_EQ(rig.server.takeMethods(), (Methods{"HEAD"}));
    (void)op.readModifyWriteOnPresence("k", declineMeta, Retry::once());
    EXPECT_EQ(rig.server.takeMethods(), (Methods{"HEAD"}));
}

TEST(CASAbsencePerVerb, GetNamedNoSuchKeyIsProvenAbsent)
{
    Rig rig(namedNotFound("NoSuchKey"));
    auto op = rig.requests.admit();

    EXPECT_FALSE(op.read("k", Retry::once()).has_value());
    EXPECT_EQ(op.probeSentinel("k", Retry::once()).outcome, ProbeOutcome::KeyAbsent);
    EXPECT_EQ(op.stream("k", Retry::once()).get(), nullptr);

    bool decided_on_absence = false;
    (void)op.readModifyWrite("k", [&](const std::optional<Object> & seen) -> std::optional<String>
    {
        decided_on_absence = !seen.has_value();
        return std::nullopt;
    }, Retry::once());
    EXPECT_TRUE(decided_on_absence);

    const WriteResult created = op.create("k", "v", Retry::once());
    const auto * conflict = std::get_if<Conflict>(&created);
    ASSERT_NE(conflict, nullptr);
    EXPECT_TRUE(std::holds_alternative<ProvenAbsent>(conflict->seen));
}

TEST(CASAbsencePerVerb, GetNoSuchBucketIsNotAbsence)
{
    Rig rig(namedNotFound("NoSuchBucket"));
    auto op = rig.requests.admit();

    try
    {
        (void)op.read("k", Retry::once());
        ADD_FAILURE() << "a missing bucket must not read as an absent key";
    }
    catch (const DB::S3Exception & e)
    {
        EXPECT_EQ(e.getS3ErrorCode(), Aws::S3::S3Errors::NO_SUCH_BUCKET);
    }
    EXPECT_EQ(op.probeSentinel("k", Retry::once()).outcome, ProbeOutcome::ContainerAbsent);
}

/// A `HEAD` response has no body, so its 404 cannot carry a name; it stays absence.
TEST(CASAbsencePerVerb, HeadNotFoundIsProvenAbsent)
{
    Rig rig(bodylessNotFound());
    auto op = rig.requests.admit();

    EXPECT_FALSE(op.head("k", Retry::once()).has_value());
    bool decided_on_absence = false;
    (void)op.readModifyWriteOnPresence("k", [&](const std::optional<Meta> & seen) -> std::optional<String>
    {
        decided_on_absence = !seen.has_value();
        return std::nullopt;
    }, Retry::once());
    EXPECT_TRUE(decided_on_absence);
    EXPECT_EQ(rig.server.takeMethods(), (Methods{"HEAD", "HEAD"}));
}

#endif
```

The local test sits outside `#if USE_AWS_S3` because it needs no SDK.

- [ ] **Step 2: Build and run on the base**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
ninja -C build unit_tests_dbms > build/ninja_pr06.log 2>&1; echo NINJA_EXIT=$?
build/src/unit_tests_dbms --gtest_filter='CASAbsencePerVerb.*' > build/test_pr06_golden.log 2>&1; echo TEST_EXIT=$?
```

Expect `NINJA_EXIT=0`, `TEST_EXIT=0`, five tests passed. These are behaviour-preservation tests: green
on the base and on the branch. If any is red here, stop and report the failure; the fact it pins is
then wrong, and the plan's verb table must be re-derived before going on.

- [ ] **Step 3: Commit**

```bash
git add -- src/Disks/tests/gtest_cas_absence_per_verb.cpp
git commit -m "Pin the verb and absence answer of each CAS read" \
           -m "Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

## Task 3: Regression tests (red on base)

**Files:** `src/Disks/tests/gtest_cas_absence_per_verb.cpp`, `src/Disks/tests/gtest_cas_sentinel_probe.cpp`.

**Consumes:** `Rig`, `notFound`, `bodylessNotFound`, `Methods` (Task 2);
`ThrowingS3ObjectStorage`, `makeThrowingS3StorageForTest`, `makeRequests` (sentinel test file).

**Produces:** four red tests; Task 4 commits them with the fix, so the branch never holds a red commit.

- [ ] **Step 1: Add the wire regressions**

In `gtest_cas_absence_per_verb.cpp`, inside the anonymous namespace after `declineMeta`, add:

```cpp
/// What a generic HTTP server answers a request routed to the wrong place.
VerbRecordingServer::Answer unparseableNotFound()
{
    return notFound([](auto & response)
    {
        const std::string body = "404 page not found\n";
        response.setStatus(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
        response.setContentType("text/plain");
        response.setContentLength(body.size());
        response.send() << body;
    });
}

/// A `GET` 404 the SDK could not name proves nothing about the key: every read reports a fault, and no
/// write is decided on it.
void expectUnnamedGetNotFoundIsNotAbsence(VerbRecordingServer::Answer answer)
{
    Rig rig(std::move(answer));
    auto op = rig.requests.admit();

    try
    {
        (void)op.read("k", Retry::once());
        ADD_FAILURE() << "a GET 404 the store did not name must not read as an absent key";
    }
    catch (const DB::S3Exception & e)
    {
        EXPECT_EQ(e.getS3ErrorCode(), Aws::S3::S3Errors::RESOURCE_NOT_FOUND);
    }
    EXPECT_EQ(op.probeSentinel("k", Retry::once()).outcome, ProbeOutcome::Indeterminate);
    EXPECT_THROW((void)op.stream("k", Retry::once()), DB::S3Exception);

    bool decided = false;
    const WriteResult rmw = op.readModifyWrite("k", [&](const std::optional<Object> &) -> std::optional<String>
    {
        decided = true;
        return String("v");
    }, Retry::once());
    EXPECT_FALSE(decided);
    const auto * gave_up = std::get_if<GaveUp>(&rmw);
    ASSERT_NE(gave_up, nullptr);
    EXPECT_TRUE(std::holds_alternative<NotObserved>(gave_up->last_seen));
    EXPECT_EQ(rig.server.takeMethods(), (Methods{"GET", "GET", "GET", "GET"})) << "no write may follow the failed read";

    const WriteResult created = op.create("k", "v", Retry::once());
    const auto * conflict = std::get_if<Conflict>(&created);
    ASSERT_NE(conflict, nullptr);
    EXPECT_TRUE(std::holds_alternative<NotObserved>(conflict->seen));
}
```

After `HeadNotFoundIsProvenAbsent`, before `#endif`, add:

```cpp
TEST(CASAbsencePerVerb, GetBodylessNotFoundIsNotAbsence)
{
    expectUnnamedGetNotFoundIsNotAbsence(bodylessNotFound());
}

TEST(CASAbsencePerVerb, GetUnparseableNotFoundIsNotAbsence)
{
    expectUnnamedGetNotFoundIsNotAbsence(unparseableNotFound());
}

/// The engine reissues an unnamed 404 like any other fault and gives up at the deadline.
TEST(CASAbsencePerVerb, UnnamedGetNotFoundIsReissuedToTheDeadline)
{
    Rig rig(bodylessNotFound());
    auto op = rig.requests.admit();

    DB::Cas::tests::expectThrowsCodeWithMessage(DB::ErrorCodes::NETWORK_ERROR, "gave up at the policy deadline",
        [&] { (void)op.read("k", Retry::standard()); });
    const Methods methods = rig.server.takeMethods();
    EXPECT_GE(methods.size(), 2u);
    for (const auto & method : methods)
        EXPECT_EQ(method, "GET");
}
```

- [ ] **Step 2: Replace the sentinel test that justified `GET` absence by a `HEAD` answer**

In `src/Disks/tests/gtest_cas_sentinel_probe.cpp`, replace the comment block starting
`/// A real S3 HEAD's 404 has no response body` and the whole
`TEST(CASSentinelProbe, NativeClassifiesResourceNotFoundAsKeyAbsent)` with:

```cpp
/// The probe reads by `GET`, whose 404 carries a body. `RESOURCE_NOT_FOUND` means the SDK found no
/// `NoSuchKey` in it, which a misrouted request or a proxy also produces.
TEST(CASSentinelProbe, NativeClassifiesUnnamedNotFoundAsIndeterminate)
{
    auto storage = std::static_pointer_cast<ThrowingS3ObjectStorage>(makeThrowingS3StorageForTest());
    storage->throwOnObjectAccess(Aws::S3::S3Errors::RESOURCE_NOT_FOUND);
    ObjectStorageBackend backend(storage, ObjectStorageBackend::Mode::Native);

    auto requests = makeRequests(backend);
    auto op = requests.admit();
    EXPECT_EQ(probeSentinel(op, nativeKeyUnder(storage, "some/key"), Retry::once()).outcome, ProbeOutcome::Indeterminate);
}
```

`Retry::once` keeps the probe to one attempt, so this test does not depend on the deadline handling
PR 5 changes. In the comment above `NativeClassifiesNoSuchKeyAsKeyAbsent`, change
`NO_SUCH_KEY/NO_SUCH_BUCKET/ACCESS_DENIED HEAD` to `NO_SUCH_KEY/NO_SUCH_BUCKET/ACCESS_DENIED GET`.

- [ ] **Step 3: Run on the base, expect red**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
ninja -C build unit_tests_dbms > build/ninja_pr06.log 2>&1; echo NINJA_EXIT=$?
build/src/unit_tests_dbms --gtest_filter='CASAbsencePerVerb.*:CASSentinelProbe.*' > build/test_pr06_red.log 2>&1; echo TEST_EXIT=$?
grep -E '^\[  FAILED  \]|must not read as an absent key|no write may follow|expected DB::Exception' build/test_pr06_red.log
```

Expect `TEST_EXIT=1` and exactly four failed tests:
`CASAbsencePerVerb.GetBodylessNotFoundIsNotAbsence`, `CASAbsencePerVerb.GetUnparseableNotFoundIsNotAbsence`
(each with `a GET 404 the store did not name must not read as an absent key`, a `KeyAbsent` vs
`Indeterminate` mismatch printed as 1-byte objects, `Actual: it throws nothing.` for `stream`, and
`Value of: decided`), `CASAbsencePerVerb.UnnamedGetNotFoundIsReissuedToTheDeadline` (`expected
DB::Exception`) and `CASSentinelProbe.NativeClassifiesUnnamedNotFoundAsIndeterminate`. Keep
`build/test_pr06_red.log` and quote these lines in the task report. Do not commit.

## Task 4: The fix

**Files:** `CA/Backend/CasObjectStorageBackend.cpp`, `CA/Backend/CasObjectStorageBackend.h`,
`src/Disks/tests/gtest_cas_s3_staging.cpp`.

- [ ] **Step 1: Narrow `isObjectNotFound`**

In `CA/Backend/CasObjectStorageBackend.cpp`, replace the comment above `isObjectNotFound` from
`/// True when an exception from a read means` down to (not including) the `/// \`NO_SUCH_BUCKET\` is
deliberately NOT here` paragraph with:

```cpp
/// True when an exception from a `GET` means "the KEY is simply not there".
///   1. S3: the store named the miss `NoSuchKey` in the 404 body -- the SDK's `NO_SUCH_KEY`, or the
///      name where the throw site attaches it. A 404 the SDK could not name (`RESOURCE_NOT_FOUND`: no
///      body, or one it could not parse) is not proof: a misrouted request or a proxy answers the same
///      way. It propagates and is reissued like any other fault. A `HEAD` has no body to carry a name;
///      its absence is decided by `S3::getObjectInfoIfExists`.
///   2. Local / emulated: `DB::Exception` with `ErrorCodes::FILE_DOESNT_EXIST` (from
///      `ReadBufferFromFile` when `open(2)` returns ENOENT).
///
```

Keep the `NO_SUCH_BUCKET` paragraph as it is. In the body, delete the line
`            || s3e->getS3ErrorCode() == Aws::S3::S3Errors::RESOURCE_NOT_FOUND`, leaving:

```cpp
    if (const auto * s3e = dynamic_cast<const S3Exception *>(&e))
        return s3e->getS3ErrorCode() == Aws::S3::S3Errors::NO_SUCH_KEY
            || s3e->getExceptionName() == "NoSuchKey";
```

- [ ] **Step 2: Name the unnamed 404 where the probe classifies it**

In `ObjectStorageBackend::probeSentinelUnder`, change the `default:` comment
`/// Everything else (timeouts, 5xx, throttling, an unmodeled code) is inconclusive —` to
`/// Everything else (timeouts, 5xx, throttling, a 404 the SDK could not name, an unmodeled code) is inconclusive —`.

In `CA/Backend/CasObjectStorageBackend.h`, in the comment above `probeSentinelRaw`, after
``-- a distinction a bodyless HEAD 404 cannot make.`` add the line
`/// A 404 the SDK could not name is \`Indeterminate\`.`

- [ ] **Step 3: Run the regressions, expect green**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
ninja -C build unit_tests_dbms > build/ninja_pr06.log 2>&1; echo NINJA_EXIT=$?
build/src/unit_tests_dbms --gtest_filter='CASAbsencePerVerb.*:CASSentinelProbe.*' > build/test_pr06_green.log 2>&1; echo TEST_EXIT=$?
build/src/unit_tests_dbms --gtest_filter='CASS3Staging.*' > build/test_pr06_staging_red.log 2>&1; echo TEST_EXIT=$?
grep -E '^\[  FAILED  \]' build/test_pr06_staging_red.log
```

Expect the first run `TEST_EXIT=0`. Expect the staging run `TEST_EXIT=1` with
`CASS3Staging.GenerationBackendMayUseNativeOnlyCopy` failed by an exception from `startup`: its fake
answers a `GET` miss with `RESOURCE_NOT_FOUND`, so the empty-pool mount no longer sees an absent pool.
If it passes, record that in the report and still apply Step 4: the fake must model what a store sends.

- [ ] **Step 4: Make the staging fake answer a `GET` miss as a store does**

In `src/Disks/tests/gtest_cas_s3_staging.cpp`, `FakeGenerationObjectStorage::readObject`, replace the
comment and throw under `if (it == objects.end())` with:

```cpp
        if (it == objects.end())
            /// A store's `GET` 404 names the miss; the Native backend takes only that name as absence.
            throw DB::S3Exception("FakeGenerationObjectStorage: object does not exist",
                                   Aws::S3::S3Errors::NO_SUCH_KEY, "NoSuchKey");
```

In `FakeGenerationObjectStorage::readSmallObjectAndGetObjectMetadata`, replace its throw with:

```cpp
            throw DB::S3Exception("FakeGenerationObjectStorage: object does not exist",
                                   Aws::S3::S3Errors::NO_SUCH_KEY, "NoSuchKey");
```

Leave `getObjectMetadata` unchanged: it is the metadata surface, not a `GET`.

- [ ] **Step 5: Run the staging suite and the sweep**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
ninja -C build unit_tests_dbms > build/ninja_pr06.log 2>&1; echo NINJA_EXIT=$?
build/src/unit_tests_dbms --gtest_filter='CASS3Staging.*' > build/test_pr06_staging.log 2>&1; echo TEST_EXIT=$?
grep -rn 'RESOURCE_NOT_FOUND' src/Disks/tests src/IO/tests src/IO/S3/tests
```

Expect `TEST_EXIT=0`. The grep lists only: `gtest_cas_sentinel_probe.cpp` (the `Indeterminate` test and
its comment), `gtest_cas_s3_staging.cpp` (`getObjectMetadata`), `gtest_cas_absence_per_verb.cpp`
(the `RESOURCE_NOT_FOUND` assertion), and the two `WriteBufferFromS3` comments in `src/IO/tests`.
Anything else is a fake that still answers a `GET` miss without a name: fix it the same way.

- [ ] **Step 6: Commit**

```bash
git add -- src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasObjectStorageBackend.cpp \
           src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasObjectStorageBackend.h \
           src/Disks/tests/gtest_cas_absence_per_verb.cpp \
           src/Disks/tests/gtest_cas_sentinel_probe.cpp \
           src/Disks/tests/gtest_cas_s3_staging.cpp
git commit -m "Treat only a named NoSuchKey as absence on a CAS GET" \
           -m "A GET 404 the SDK could not name (no body, or an unparseable one) is now a reissued fault, not absence. HEAD 404 and FILE_DOESNT_EXIST are unchanged." \
           -m "Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

## Task 5: User documentation

**Files:** `docs/en/antalya/cas/bucket-requirements.md`.

- [ ] **Step 1: Add the row**

In the table under `## The capability table {#capability-table}`, directly after the row starting
`| Read-after-write on a fresh key |`, insert:

```markdown
| A `GET` of an absent key answers `404` with the code `NoSuchKey` | `Backend::get` | Only that answer proves a key absent. A `GET` `404` without a code (no body, or a body that is not an S3 error document) is retried as a transient error until the request deadline, so a store or proxy that answers this way cannot mount. A `HEAD` `404` has no body and is absence |
```

- [ ] **Step 2: Commit**

```bash
git add -- docs/en/antalya/cas/bucket-requirements.md
git commit -m "Document that a CAS bucket must name a missing key on GET" \
           -m "Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

## Task 6: Gates and integration acceptance

- [ ] **Step 1: Release gate**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
build/src/unit_tests_dbms --gtest_filter='CAS*' > build/test_pr06_gate.log 2>&1; echo TEST_EXIT=$?
```

Expect `TEST_EXIT=0`, `[  PASSED  ]`, no `[  FAILED  ]` line.

- [ ] **Step 2: ASan gate**

```bash
ninja -C build_asan unit_tests_dbms > build_asan/ninja_pr06.log 2>&1; echo NINJA_EXIT=$?
build_asan/src/unit_tests_dbms --gtest_filter='CAS*' > build_asan/test_pr06_gate.log 2>&1; echo TEST_EXIT=$?
```

Expect `NINJA_EXIT=0`, `TEST_EXIT=0`.

- [ ] **Step 3: RustFS and the GCS fake**

Every writable mount `GET`s absent keys: the capability probe reads its key back after the exact
delete (`runCapabilityProbe`, step 7), and an empty pool's meta, owner, epoch and slot are all read
before they exist. A store whose `GET` 404 carried no name would now fail the mount at the policy
deadline. Each module below starts on a fresh pool.

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
ninja -C build clickhouse > build/ninja_pr06_server.log 2>&1; echo NINJA_EXIT=$?
mkdir -p ci/tmp && ln -sf "$PWD/build/programs/clickhouse" ci/tmp/clickhouse
python -m ci.praktika run "integration" --test test_cas_s3 test_cas_mount_renewal_retry test_cas_gcs \
    > build/test_pr06_integration.log 2>&1; echo TEST_EXIT=$?
grep -E 'Failed: [0-9]+, Passed: [0-9]+' build/test_pr06_integration.log
```

Expect `Failed: 0`. `test_cas_s3` and `test_cas_mount_renewal_retry` mount on RustFS (`with_rustfs`).
`test_cas_gcs` mounts on its fake GCS (`tests/integration/test_cas_gcs/gcs_mocks/server.py`, which
answers a `GET` miss with `_no_such_key` and drops bodies on `HEAD`); it does not prove what Google
sends. No CAS integration test mounts on MinIO: MinIO fails the capability probe (see the comment in
`tests/integration/test_cas_s3/configs/storage_conf.xml`).

- [ ] **Step 4: Real GCS**

`test_gcs_live` mounts a CAS disk on a live bucket under a fresh random prefix. Its credentials live in
the git-ignored `ci/local.env`, which praktika loads by itself.

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
cp /home/mfilimonov/workspace/ClickHouse/master/ci/local.env ci/local.env
python -m ci.praktika run "integration" --test test_gcs_live > build/test_pr06_gcs_live.log 2>&1; echo TEST_EXIT=$?
grep -E 'test_native_conditional_gcs_hmac_mounts_and_keeps_generation_tokens' build/test_pr06_gcs_live.log
rm ci/local.env
```

Expect `test_native_conditional_gcs_hmac_mounts_and_keeps_generation_tokens` PASSED. SKIPPED means the
credentials did not load; that is not acceptance — fix the environment and rerun.

- [ ] **Step 5: Real AWS S3**

No integration test mounts on AWS. Run the PR binary as a standalone server against the bucket the
soak stand uses (`/home/mfilimonov/workspace/ClickHouse/master/utils/ca-soak/configs/storage_conf_awss3_ch1.xml`),
under a prefix of its own, with credentials from the git-ignored `.../utils/ca-soak/configs/aws.env`.

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
D=$PWD/tmp/pr06-aws; P=cas_pr06_20261008_r1
mkdir -p $D/conf/config.d $D/data $D/tmp $D/uf
cp programs/server/config.xml programs/server/users.xml $D/conf/
cat > $D/conf/config.d/cas_aws.xml <<EOF
<clickhouse>
    <storage_configuration>
        <disks>
            <cas_aws>
                <type>object_storage</type>
                <object_storage_type>s3</object_storage_type>
                <metadata_type>cas</metadata_type>
                <endpoint>https://test-altinity-support-team.s3.us-east-1.amazonaws.com/$P/pool/</endpoint>
                <region>us-east-1</region>
                <use_environment_credentials>1</use_environment_credentials>
                <cas_server_root_id>pr06-aws</cas_server_root_id>
            </cas_aws>
        </disks>
        <policies>
            <cas_aws><volumes><main><disk>cas_aws</disk></main></volumes></cas_aws>
        </policies>
    </storage_configuration>
</clickhouse>
EOF
ss -ltn | grep -E ':1968[1-3]\b'    # expect no output; pick other ports if any is taken
set -a; . /home/mfilimonov/workspace/ClickHouse/master/utils/ca-soak/configs/aws.env; set +a
nohup build/programs/clickhouse server --config-file=$D/conf/config.xml -- --path=$D/data/ --tmp_path=$D/tmp/ \
    --user_files_path=$D/uf/ --logger.log=$D/server.log --logger.errorlog=$D/server.err.log \
    --tcp_port=19681 --http_port=19682 --interserver_http_port=19683 --mysql_port=0 --postgresql_port=0 \
    --keeper_server.tcp_port=0 --prometheus.port=0 > $D/stdout.log 2>&1 &
echo SERVER_PID=$!
```

Then, once `build/programs/clickhouse client --port 19681 -q "SELECT version()"` prints the version of
the binary under test (not just any answer: other stands listen nearby):

```bash
C="build/programs/clickhouse client --port 19681"
$C -q "SELECT name FROM system.disks WHERE name = 'cas_aws'"
$C -q "CREATE TABLE t_pr06 (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS storage_policy = 'cas_aws'"
$C -q "INSERT INTO t_pr06 SELECT number FROM numbers(100)"
$C -q "SELECT sum(a) FROM t_pr06"
grep -c 'gave up at the policy deadline' $D/server.log
kill $SERVER_PID
```

Expect `cas_aws`, then `4950`, then `0`. Removing the prefix afterwards
(`aws s3 rm --recursive s3://test-altinity-support-team/cas_pr06_20261008_r1/`) deletes objects in a
shared bucket: ask the owner before running it.

- [ ] **Step 6: Confirm the commit set**

```bash
git diff --stat 5b7c9f503df..HEAD
```

Expect six files: the two `CasObjectStorageBackend` files, the three test files, `bucket-requirements.md`.

## PR text

Title: `Treat only a named NoSuchKey as absence on a CAS GET`

The last sentence of the mixed-versions paragraph is true only after Task 6 Steps 3–5 passed. Drop any
store that was not mounted, and say so.

Body, per `.github/PULL_REQUEST_TEMPLATE.md`:

```
A CAS `GET` treated every `404` as an absent key, including one the S3 SDK could not name
(`RESOURCE_NOT_FOUND`: no body, or a body that is not an S3 error document). A misrouted request or a
proxy answers that way too, and the lease rules about to land stop the server on an absent mount slot.
Now a `GET` proves absence only with `NoSuchKey`; an unnamed `404` is reissued like any other fault and
gives up at the deadline. `HEAD` `404` (no body, so no name) and `FILE_DOESNT_EXIST` on local disks
stay absence.

Every lease-protocol read (sentinel probe, mount slot, owner, epoch, the read after a `412`) goes by
`GET`; `CASAbsencePerVerb.EachEngineReadGoesByItsVerb` pins that on the wire.

Mixed versions: no on-storage or wire change; nodes of either version interoperate on one pool.
Risk: on a store whose `GET` `404` carries no `NoSuchKey`, a node with this change sees a retryable
error where an older node saw absence, and an empty-pool mount fails at the request deadline instead of
creating the pool. RustFS, GCS and AWS S3 send `NoSuchKey` (mounted on an empty pool on each).

### Changelog category (leave one):
- Improvement

### Changelog entry:
A content-addressed disk treats a `GET` answered with `404` as an absent object only when the store
names it `NoSuchKey`; an unnamed `404` is retried as a transient error.

### Documentation entry for user-facing changes
`docs/en/antalya/cas/bucket-requirements.md`: a bucket must answer a `GET` of an absent key with
`NoSuchKey`.
```
