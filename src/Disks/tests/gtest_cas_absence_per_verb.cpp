#include <gtest/gtest.h>

#include "config.h"

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasFence.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasObjectStorageBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/tests/cas_test_helpers.h>

#include <memory>
#include <optional>
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
VerbRecordingServer::Answer notFound(std::function<void(Poco::Net::HTTPServerResponse &)> answer_get)
{
    return [get = std::move(answer_get)](const std::string & method, Poco::Net::HTTPServerResponse & response)
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

#endif
