#include <gtest/gtest.h>

#include "config.h"

#if USE_AWS_S3

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasObjectStorageBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasFence.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.h>
#include <IO/S3/Client.h>
#include <IO/S3/URI.h>
#include <IO/S3/PocoHTTPClient.h>
#include <IO/S3Settings.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Common/RemoteHostFilter.h>

#include <memory>
#include <utility>
#include <vector>

/// The single-attempt client clone must cap its connect timeout at the value the mount froze at open,
/// never at the disk's (possibly wider, possibly reloaded, possibly unbounded) own connect timeout.
/// See docs/superpowers/specs/2026-09-05-cas-adaptive-first-attempt-timeout-design.md decision 3.

namespace
{

/// A `PocoHTTPClientConfiguration` that never resolves a real socket: `endpointOverride` points at a
/// port nothing listens on, so a test that never issues a request (every assertion here reads
/// `getClientConfiguration()`, which needs no network) never blocks or flakes on connection refusal.
DB::S3::PocoHTTPClientConfiguration clientConfigurationForTest(long connect_ms)
{
    DB::RemoteHostFilter remote_host_filter;
    DB::S3::PocoHTTPClientConfiguration cfg = DB::S3::ClientFactory::instance().createClientConfiguration(
        "us-east-1",
        remote_host_filter,
        /* s3_max_redirects = */ 100,
        DB::S3::PocoHTTPClientConfiguration::RetryStrategy{.max_retries = 0},
        /* s3_slow_all_threads_after_network_error = */ true,
        /* s3_slow_all_threads_after_retryable_error = */ true,
        /* enable_s3_requests_logging = */ false,
        /* for_disk_s3 = */ true,
        /* opt_disk_name = */ {},
        /* request_throttler = */ {});
    cfg.endpointOverride = "http://127.0.0.1:1";
    cfg.connectTimeoutMs = connect_ms;
    cfg.requestTimeoutMs = 30000;
    return cfg;
}

DB::S3::ClientSettings clientSettingsForTest()
{
    return DB::S3::ClientSettings{
        .use_virtual_addressing = false,
        .disable_checksum = false,
        .gcs_issue_compose_request = false,
        .is_s3express_bucket = false,
    };
}

std::shared_ptr<DB::S3ObjectStorage> makeStorageForTest(long connect_ms)
{
    auto client = DB::S3::ClientFactory::instance().create(
        clientConfigurationForTest(connect_ms), clientSettingsForTest(),
        "ACCESS_KEY_ID", "SECRET_ACCESS_KEY", "", {}, {}, DB::S3::CredentialsConfiguration{});
    return std::make_shared<DB::S3ObjectStorage>(
        std::move(client), std::make_unique<DB::S3Settings>(),
        DB::S3::URI("http://127.0.0.1:1/bucket/"), DB::S3Capabilities{},
        DB::ObjectStorageKeyGeneratorPtr{}, "disk");
}

/// A conditional write's own `finalize` never runs a real request here: this buffer just records
/// bytes and reports a fixed, grammar-valid ETag -- exactly what a real conditional PUT's response
/// would carry, without needing a live endpoint.
class StubConditionalWriteBuffer final : public DB::WriteBufferFromFileBase
{
public:
    StubConditionalWriteBuffer() : DB::WriteBufferFromFileBase(/*buf_size=*/8192, nullptr, 0) {}
    void sync() override {}
    std::string getFileName() const override { return "stub"; }
    std::optional<std::string> getResultObjectETag() const override { return "\"etag1\""; }

protected:
    void nextImpl() override {}
};

/// Records the client selection every profile-aware verb makes, without issuing a single real
/// request: HEAD returns a synthetic answer directly, and the conditional PUT's buffer never talks to
/// a socket. See test 6e (`CASEnvelopeWiring.FrozenCapTravelsFromTheClientToEveryVerb`) below.
class RecordingS3ObjectStorage : public DB::S3ObjectStorage
{
public:
    using DB::S3ObjectStorage::S3ObjectStorage;

    mutable std::vector<long> head_connect_timeouts;
    std::vector<long> write_connect_timeouts;

    std::optional<DB::ObjectMetadata> tryGetObjectMetadataWithNativeToken(
        const std::string &, bool, const DB::ObjectStorageControlRequest & request) const override
    {
        head_connect_timeouts.push_back(
            getSingleAttemptClient(request.attempt_timeout_ms, request.connect_timeout_cap_ms)
                ->getClientConfiguration().connectTimeoutMs);
        DB::ObjectMetadata metadata;
        metadata.etag = "\"etag1\"";
        return metadata;
    }

    std::unique_ptr<DB::WriteBufferFromFileBase> writeObject(
        const DB::StoredObject &,
        DB::WriteMode,
        std::optional<DB::ObjectAttributes>,
        size_t,
        const DB::WriteSettings & write_settings) override
    {
        write_connect_timeouts.push_back(
            getSingleAttemptClient(write_settings.object_storage_attempt_timeout_ms,
                                   write_settings.object_storage_connect_timeout_cap_ms)
                ->getClientConfiguration().connectTimeoutMs);
        return std::make_unique<StubConditionalWriteBuffer>();
    }
};

std::shared_ptr<RecordingS3ObjectStorage> makeRecordingStorageForTest(long connect_ms)
{
    auto client = DB::S3::ClientFactory::instance().create(
        clientConfigurationForTest(connect_ms), clientSettingsForTest(),
        "ACCESS_KEY_ID", "SECRET_ACCESS_KEY", "", {}, {}, DB::S3::CredentialsConfiguration{});
    return std::make_shared<RecordingS3ObjectStorage>(
        std::move(client), std::make_unique<DB::S3Settings>(),
        DB::S3::URI("http://127.0.0.1:1/bucket/"), DB::S3Capabilities{},
        DB::ObjectStorageKeyGeneratorPtr{}, "disk");
}

}

/// Test 6c of the spec: the clone's connect cap is the MIN of the base client's own connect timeout
/// and the requested cap, a configured-zero base is treated as unbounded (never "no limit"), the cache
/// key is the (request timeout, cap) pair, and a reloaded base client cannot widen a clone rebuilt for
/// the same cap.
TEST(S3SingleAttemptClient, ConnectTimeoutIsCappedAndFrozen)
{
    auto storage = makeStorageForTest(20000);
    auto clone = storage->getSingleAttemptClient(/*request_timeout_ms=*/5000, /*connect_timeout_cap_ms=*/5000);
    EXPECT_EQ(clone->getClientConfiguration().connectTimeoutMs, 5000);
    EXPECT_EQ(clone->getClientConfiguration().requestTimeoutMs, 5000);

    auto narrow = makeStorageForTest(1000);
    EXPECT_EQ(narrow->getSingleAttemptClient(5000, 5000)->getClientConfiguration().connectTimeoutMs, 1000);
    /// A base of 0 means unbounded to Poco: it resolves to the cap, never to "no limit".
    EXPECT_EQ(makeStorageForTest(0)->getSingleAttemptClient(5000, 1000)->getClientConfiguration().connectTimeoutMs, 1000);
    /// Two caps under one request timeout are two clones: the cache key is the pair.
    EXPECT_NE(narrow->getSingleAttemptClient(5000, 1000).get(), narrow->getSingleAttemptClient(5000, 500).get());

    /// The reload path replaces the base client with a wider connect timeout; a clone rebuilt for the
    /// frozen cap 1000 stays at 1000.
    auto reloaded = makeStorageForTest(1000);
    (void)reloaded->getSingleAttemptClient(5000, 1000);
    reloaded->setClientForTest(DB::S3::ClientFactory::instance().create(
        clientConfigurationForTest(5000), clientSettingsForTest(),
        "ACCESS_KEY_ID", "SECRET_ACCESS_KEY", "", {}, {}, DB::S3::CredentialsConfiguration{}));
    EXPECT_EQ(reloaded->getSingleAttemptClient(5000, 1000)->getClientConfiguration().connectTimeoutMs, 1000);
}

/// The snapshot half (the freeze computation `openPoolView` uses to build
/// `pool_config.cas_request_budget.connect_timeout_cap_ms`) and the verb-propagation half (every
/// profile-aware verb's clone carries the SAME frozen pair, through the `ObjectStorageControlRequest`
/// context `ObjectStorageBackend::controlRequest` builds). The propagation half is exercised over a
/// real Native `ObjectStorageBackend` built directly with the
/// frozen cap (not through a full `ContentAddressedMetadataStorage`/`Pool::open` bootstrap, which needs
/// a scripted S3 backend able to answer conditional PUT/GET/LIST/HEAD -- none exists in
/// `src/Disks/tests` today): `RecordingS3ObjectStorage` captures the actual `connectTimeoutMs` of the
/// clone each verb selects, without any network I/O.
TEST(CASEnvelopeWiring, FrozenCapTravelsFromTheClientToEveryVerb)
{
    /// A base client with connectTimeoutMs = 1000 and cas_attempt_timeout_ms = 5000: the narrower of
    /// the two wins, and the envelope is attempt + 2 * cap = 7000.
    auto storage = makeStorageForTest(1000);
    const auto cap = DB::ContentAddressedMetadataStorage::freezeConnectTimeoutCapMs(storage, /*cas_attempt_timeout_ms=*/5000);
    ASSERT_TRUE(cap.has_value());
    EXPECT_EQ(*cap, 1000u);
    DB::Cas::CasRequestBudget budget{.attempt_timeout_ms = 5000, .connect_timeout_cap_ms = cap};
    EXPECT_EQ(budget.attemptEnvelopeMs(), 7000u);

    /// A base client with connectTimeoutMs = 0 (Poco "unbounded") and a TTL wide enough for the
    /// resulting envelope (60000, per the spec's test 6e): the cap normalizes to the attempt timeout
    /// itself, never to "no limit" -- a snapshot computing `min(0, attempt)` would report 0 here.
    auto unbounded_storage = makeStorageForTest(0);
    const auto wide_cap = DB::ContentAddressedMetadataStorage::freezeConnectTimeoutCapMs(unbounded_storage, /*cas_attempt_timeout_ms=*/5000);
    ASSERT_TRUE(wide_cap.has_value());
    EXPECT_EQ(*wide_cap, 5000u);
    DB::Cas::CasRequestBudget wide_budget{.attempt_timeout_ms = 5000, .connect_timeout_cap_ms = wide_cap};
    EXPECT_EQ(wide_budget.attemptEnvelopeMs(), 15000u);
    EXPECT_NO_THROW(DB::Cas::validateCasRequestBudget(wide_budget, /*mount_lease_ttl_ms=*/60000, /*mount_renew_period_ms=*/10000,
                                                      /*background_renewal=*/false));

    /// A storage with no S3 client (not exercised here -- every storage above is S3) freezes `nullopt`;
    /// covered directly by `S3ObjectStorage::tryGetS3StorageClient` returning null for a non-S3 storage
    /// and `freezeConnectTimeoutCapMs` short-circuiting on it.

    /// Verb-propagation half: a Native backend built with `attempt_timeout_ms=5000`,
    /// `connect_timeout_cap_ms=1000` (the frozen cap from above). One control read (HEAD) and one
    /// conditional write (PUT) must both select a clone with `connectTimeoutMs == 1000` -- and stay at
    /// 1000 after the underlying client is reloaded with a wider (5000), then a zero (unbounded),
    /// connect timeout: the backend's OWN `connect_timeout_cap_ms` was frozen at construction and a
    /// later reload cannot widen it (spec decision 3).
    auto recording_storage = makeRecordingStorageForTest(1000);
    auto backend = std::make_shared<DB::Cas::ObjectStorageBackend>(
        recording_storage, DB::Cas::ObjectStorageBackend::Mode::Native,
        /*single_attempt_control_plane_=*/true, /*attempt_timeout_ms_=*/5000, /*connect_timeout_cap_ms_=*/1000);
    DB::Cas::CasRequests requests(backend, DB::Cas::Fence::open());
    auto op = requests.admit();

    ASSERT_TRUE(op.head("k", DB::Cas::Retry::once()).has_value());
    ASSERT_TRUE(std::holds_alternative<DB::Cas::Committed>(op.create("k", "v", DB::Cas::Retry::once())));
    EXPECT_EQ(recording_storage->head_connect_timeouts, (std::vector<long>{1000}));
    EXPECT_EQ(recording_storage->write_connect_timeouts, (std::vector<long>{1000}));

    /// Widen the disk's own connect timeout: the frozen cap must still win.
    recording_storage->setClientForTest(DB::S3::ClientFactory::instance().create(
        clientConfigurationForTest(5000), clientSettingsForTest(),
        "ACCESS_KEY_ID", "SECRET_ACCESS_KEY", "", {}, {}, DB::S3::CredentialsConfiguration{}));
    ASSERT_TRUE(op.head("k", DB::Cas::Retry::once()).has_value());
    ASSERT_TRUE(std::holds_alternative<DB::Cas::Committed>(op.create("k2", "v", DB::Cas::Retry::once())));
    EXPECT_EQ(recording_storage->head_connect_timeouts, (std::vector<long>{1000, 1000}));
    EXPECT_EQ(recording_storage->write_connect_timeouts, (std::vector<long>{1000, 1000}));

    /// Zero the disk's own connect timeout (Poco "unbounded"): still the frozen cap, never "no limit".
    recording_storage->setClientForTest(DB::S3::ClientFactory::instance().create(
        clientConfigurationForTest(0), clientSettingsForTest(),
        "ACCESS_KEY_ID", "SECRET_ACCESS_KEY", "", {}, {}, DB::S3::CredentialsConfiguration{}));
    ASSERT_TRUE(op.head("k", DB::Cas::Retry::once()).has_value());
    ASSERT_TRUE(std::holds_alternative<DB::Cas::Committed>(op.create("k3", "v", DB::Cas::Retry::once())));
    EXPECT_EQ(recording_storage->head_connect_timeouts, (std::vector<long>{1000, 1000, 1000}));
    EXPECT_EQ(recording_storage->write_connect_timeouts, (std::vector<long>{1000, 1000, 1000}));

    /// A second storage whose base connect timeout is 0 (unbounded) at freeze time: the cap normalizes
    /// to the attempt timeout itself (5000, from the snapshot half above), and every verb selects a
    /// clone at that cap -- never at "no limit".
    auto recording_storage2 = makeRecordingStorageForTest(0);
    auto backend2 = std::make_shared<DB::Cas::ObjectStorageBackend>(
        recording_storage2, DB::Cas::ObjectStorageBackend::Mode::Native,
        /*single_attempt_control_plane_=*/true, /*attempt_timeout_ms_=*/5000, /*connect_timeout_cap_ms_=*/5000);
    DB::Cas::CasRequests requests2(backend2, DB::Cas::Fence::open());
    auto op2 = requests2.admit();
    ASSERT_TRUE(op2.head("k", DB::Cas::Retry::once()).has_value());
    ASSERT_TRUE(std::holds_alternative<DB::Cas::Committed>(op2.create("k", "v", DB::Cas::Retry::once())));
    EXPECT_EQ(recording_storage2->head_connect_timeouts, (std::vector<long>{5000}));
    EXPECT_EQ(recording_storage2->write_connect_timeouts, (std::vector<long>{5000}));
}

#endif
