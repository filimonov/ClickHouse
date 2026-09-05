#include <gtest/gtest.h>

#include "config.h"

#if USE_AWS_S3

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.h>
#include <IO/S3/Client.h>
#include <IO/S3/URI.h>
#include <IO/S3/PocoHTTPClient.h>
#include <IO/S3Settings.h>
#include <Common/RemoteHostFilter.h>

#include <memory>
#include <utility>

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

/// This covers the SNAPSHOT half only: every verb's clone carrying the recorded pair through
/// `ContentAddressedMetadataStorage`'s pool_config needs a context struct threading the cap to each
/// verb, or a scripted S3 backend able to complete a full Native pool bootstrap -- neither is available
/// from `src/Disks/tests` today. What IS testable without a live pool open is the freeze computation
/// itself: `ContentAddressedMetadataStorage::freezeConnectTimeoutCapMs` is the exact function
/// `openPoolView` calls to build `pool_config.cas_request_budget.connect_timeout_cap_ms`.
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
}

#endif
