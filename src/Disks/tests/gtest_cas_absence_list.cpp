#include <gtest/gtest.h>

#include "config.h"

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasFence.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasMountLoss.h>
#include <Disks/tests/cas_test_helpers.h>

#include <Poco/Exception.h>

#include <exception>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#if USE_AWS_S3
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasObjectStorageBackend.h>
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

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

using namespace DB::Cas;

namespace
{

/// A GET of `hidden` finds nothing, as a proxy 404 does; every other verb sees the store as it is.
class HiddenReadBackend : public InMemoryBackend
{
public:
    explicit HiddenReadBackend(String hidden_) : hidden(std::move(hidden_)) {}

    std::optional<Raw> read(const String & key, TransportAccess & access) override
    {
        if (key == hidden)
            return std::nullopt;
        return InMemoryBackend::read(key, access);
    }

private:
    String hidden;
};

class ThrowingListBackend : public InMemoryBackend
{
public:
    explicit ThrowingListBackend(std::function<void()> throw_) : throw_error(std::move(throw_)) {}

    RawListPage list(const String & prefix, const String & cursor, size_t limit, TransportAccess & access) override
    {
        throw_error();
        return InMemoryBackend::list(prefix, cursor, limit, access);
    }

private:
    std::function<void()> throw_error;
};

void putObject(Backend & backend, const String & key)
{
    OperationForTest op(backend);
    (void)(*op).create(key, "v", Retry::standard());
}

Observation readAndCorroborate(const std::shared_ptr<InMemoryBackend> & backend, const String & key)
{
    OperationForTest op(*backend);
    const std::optional<Object> got = (*op).read(key, Retry::once());
    Observation seen = got ? Observation{*got} : Observation{ProvenAbsent{}};
    return corroborateAbsence(*op, key, std::move(seen));
}

}

TEST(CASAbsenceList, AnObjectOrNothingObservedPassesThrough)
{
    auto backend = std::make_shared<InMemoryBackend>();
    putObject(*backend, "k");
    EXPECT_TRUE(std::holds_alternative<Object>(readAndCorroborate(backend, "k")));

    OperationForTest op(*backend);
    EXPECT_TRUE(std::holds_alternative<NotObserved>(corroborateAbsence(*op, "k", NotObserved{})));
}

TEST(CASAbsenceList, AnAbsentKeyWithAnEmptyListIsProvenAbsent)
{
    auto backend = std::make_shared<InMemoryBackend>();
    /// Under the prefix, but not the key: the first listed entry decides.
    putObject(*backend, "kz");
    EXPECT_TRUE(std::holds_alternative<ProvenAbsent>(readAndCorroborate(backend, "k")));
}

TEST(CASAbsenceList, AKeyTheListShowsIsNotObserved)
{
    auto backend = std::make_shared<HiddenReadBackend>("k");
    putObject(*backend, "k");
    EXPECT_TRUE(std::holds_alternative<NotObserved>(readAndCorroborate(backend, "k")));
}

TEST(CASAbsenceList, AFailedListIsNotObserved)
{
    auto backend = std::make_shared<ThrowingListBackend>([] { throw Poco::TimeoutException("injected LIST timeout"); });
    EXPECT_TRUE(std::holds_alternative<NotObserved>(readAndCorroborate(backend, "k")));
}

TEST(CASAbsenceList, ALocalDefectInTheListPropagates)
{
    auto backend = std::make_shared<ThrowingListBackend>(
        [] { throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "injected local defect"); });
    try
    {
        (void)readAndCorroborate(backend, "k");
        ADD_FAILURE() << "a deterministic local failure must propagate";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::NOT_IMPLEMENTED);
    }
}

#if USE_AWS_S3

namespace DB::S3RequestSetting
{
    extern const S3RequestSettingsUInt64 max_single_read_retries;
}

namespace
{

using Methods = std::vector<std::string>;

/// A local endpoint standing in for S3. `answer` writes the response for each request, and every
/// method is recorded, which is how a test states the verb an engine read went by.
class VerbRecordingServer
{
public:
    using Answer = std::function<void(const Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse &)>;

    explicit VerbRecordingServer(Answer answer_)
        : answer(std::move(answer_))
        , socket(std::make_unique<Poco::Net::ServerSocket>(Poco::Net::SocketAddress("127.0.0.1", 0)))
        , factory(new Factory(*this))
        , params(new Poco::Net::HTTPServerParams())
        /// A private pool: the default one is shared by every local-server test in the binary.
        , pool("CASAbsenceListServer")
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
            owner.answer(request, response);
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

bool isList(const Poco::Net::HTTPServerRequest & request)
{
    return request.getMethod() == "GET" && request.getURI().find("list-type=2") != std::string::npos;
}

void sendEmpty(Poco::Net::HTTPServerResponse & response, Poco::Net::HTTPResponse::HTTPStatus status)
{
    response.setStatus(status);
    response.setContentLength(0);
    response.send();
}

void sendListing(Poco::Net::HTTPServerResponse & response, const std::string & contents)
{
    const std::string body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>test-bucket</Name><Prefix>k</Prefix>"
        "<KeyCount>" + std::string(contents.empty() ? "0" : "1") + "</KeyCount><MaxKeys>1</MaxKeys>"
        "<IsTruncated>false</IsTruncated>" + contents + "</ListBucketResult>";
    response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
    response.setContentType("application/xml");
    response.setContentLength(body.size());
    response.send() << body;
}

/// Every object `GET` is a bodyless 404; `answer_list` answers the LIST.
VerbRecordingServer::Answer getNotFound(std::function<void(Poco::Net::HTTPServerResponse &)> answer_list)
{
    return [list_answer = std::move(answer_list)](const Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
    {
        if (isList(request))
            list_answer(response);
        else
            sendEmpty(response, Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
    };
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

    Observation readAndCorroborate()
    {
        auto op = requests.admit();
        const std::optional<Object> got = op.read("k", Retry::once());
        if (got)
            return *got;
        return corroborateAbsence(op, "k", ProvenAbsent{});
    }
};

}

TEST(CASAbsenceList, AProxyThatAnswers404ToEverythingProvesNothing)
{
    Rig rig(getNotFound([](auto & response) { sendEmpty(response, Poco::Net::HTTPResponse::HTTP_NOT_FOUND); }));
    {
        auto op = rig.requests.admit();
        EXPECT_FALSE(op.read("k", Retry::once()).has_value()) << "the GET rule is unchanged";
    }
    rig.server.takeMethods();
    EXPECT_TRUE(std::holds_alternative<NotObserved>(rig.readAndCorroborate()));
    EXPECT_EQ(rig.server.takeMethods(), (Methods{"GET", "GET"}));
}

TEST(CASAbsenceList, A404WithAnEmptyListIsProvenAbsent)
{
    Rig rig(getNotFound([](auto & response) { sendListing(response, ""); }));
    EXPECT_TRUE(std::holds_alternative<ProvenAbsent>(rig.readAndCorroborate()));
    EXPECT_EQ(rig.server.takeMethods(), (Methods{"GET", "GET"}));
}

TEST(CASAbsenceList, A404ContradictedByTheListIsNotObserved)
{
    const auto listing_of = [](const std::string & key)
    {
        return getNotFound([key](auto & response)
        {
            sendListing(response, "<Contents><Key>" + key + "</Key><LastModified>2026-01-01T00:00:00.000Z</LastModified>"
                                  "<ETag>\"e1\"</ETag><Size>1</Size><StorageClass>STANDARD</StorageClass></Contents>");
        });
    };
    Rig rig(listing_of("k"));
    EXPECT_TRUE(std::holds_alternative<NotObserved>(rig.readAndCorroborate()));
    EXPECT_EQ(rig.server.takeMethods(), (Methods{"GET", "GET"}));

    /// The same listing naming another key under the prefix decodes and proves the absence, so the
    /// verdict above comes from the listed key, not from a listing the client failed to read.
    Rig control(listing_of("kz"));
    EXPECT_TRUE(std::holds_alternative<ProvenAbsent>(control.readAndCorroborate()));
}

#endif
