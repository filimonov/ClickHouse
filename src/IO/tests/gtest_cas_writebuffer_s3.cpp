#include <gtest/gtest.h>

#include "config.h"

#if USE_AWS_S3

#include <IO/S3/Requests.h>
#include <IO/S3/getObjectInfo.h>

#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageIterator.h>

#include <Common/logger_useful.h>

#include <fmt/format.h>

#include <Poco/AutoPtr.h>
#include <Poco/StreamChannel.h>

#include "WriteBufferS3TestMocks.h"

using namespace DB;

namespace
{

/// Captures what `WriteBufferFromS3` logs at `threshold` and above (default: Error). A message
/// logged below the threshold never reaches the channel, so an empty capture proves the site logged
/// below it rather than merely that this particular text was absent.
class ScopedWriteBufferS3ErrorLogCapture
{
public:
    explicit ScopedWriteBufferS3ErrorLogCapture(const std::string & threshold = "error")
        : logger(getLogger("WriteBufferFromS3"))
        , channel(new Poco::StreamChannel(stream))
        , old_channel(logger->getChannel(), /*shared=*/true)
        , old_level(logger->getLevel())
    {
        logger->setChannel(channel.get());
        logger->setLevel(threshold);
    }

    ~ScopedWriteBufferS3ErrorLogCapture()
    {
        logger->setChannel(old_channel);
        logger->setLevel(old_level);
    }

    std::string captured() const { return stream.str(); }

private:
    LoggerPtr logger;
    std::ostringstream stream;
    Poco::AutoPtr<Poco::StreamChannel> channel;
    /// `shared=true` is load-bearing: `AutoPtr(ptr)` would steal a reference the fixture never owned.
    Poco::AutoPtr<Poco::Channel> old_channel;
    int old_level;
};

}

/// A non-412 `PutObject` failure on the ordinary (Default) retry profile is a genuine error: the
/// client's one attempt IS the final answer, so the site logs it at Error.
TEST_P(SyncAsync, PutObjectErrorLogsErrorForDefaultProfile)
{
    setInjectionModel(std::make_shared<MockS3::PutObjectFailIngection>());

    ScopedWriteBufferS3ErrorLogCapture log_capture;
    EXPECT_THROW({
        auto buffer = getWriteBuffer("put_object_error_default_profile");
        buffer->write('A');
        buffer->next();

        getAsyncPolicy().setAutoExecute(true);
        buffer->finalize();
    }, DB::S3Exception);

    EXPECT_THAT(log_capture.captured(), testing::HasSubstr("S3Exception name FailInjection"));
    EXPECT_THAT(log_capture.captured(), testing::HasSubstr("PutObjectFailIngection"));
}

/// The same failure on the SingleAttempt profile (the CAS conditional-write client) is owned by an
/// outer retry loop that resolves the outcome and reissues; the one failed attempt is not terminal,
/// so nothing here reaches Error.
TEST_P(SyncAsync, PutObjectErrorLogsDebugForSingleAttemptProfile)
{
    setInjectionModel(std::make_shared<MockS3::PutObjectFailIngection>());

    WriteSettings write_settings;
    write_settings.object_storage_retry_profile = ObjectStorageRetryProfile::SingleAttempt;

    ScopedWriteBufferS3ErrorLogCapture log_capture;
    EXPECT_THROW({
        auto buffer = getWriteBuffer("put_object_error_single_attempt_profile", write_settings);
        buffer->write('A');
        buffer->next();

        getAsyncPolicy().setAutoExecute(true);
        buffer->finalize();
    }, DB::S3Exception);

    EXPECT_TRUE(log_capture.captured().empty());
}

/// A conditional write losing its precondition (412) is the caller's expected answer, handled one
/// frame up -- it says nothing to the operator, so it must stay below Information, independent of the
/// retry profile. The capture threshold is Information so that an Info-level line from the site would
/// be caught; the cancel path logs its own Info lines, so the assertion is on the site's text, not on
/// an empty capture.
TEST_P(SyncAsync, PreconditionFailedNeverLogsAtError)
{
    setInjectionModel(std::make_shared<MockS3::PutObjectPreconditionFailedIngection>());

    ScopedWriteBufferS3ErrorLogCapture log_capture("information");
    EXPECT_THROW({
        auto buffer = getWriteBuffer("put_object_precondition_failed");
        buffer->write('A');
        buffer->next();

        getAsyncPolicy().setAutoExecute(true);
        buffer->finalize();
    }, DB::S3Exception);

    EXPECT_THAT(log_capture.captured(), testing::Not(testing::HasSubstr("S3Exception name")));
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
    /// Seed 0 adds no header at all (the spec's rule for every verb but the read path).
    client->attempts_seen.clear();
    {
        auto buffer = getWriteBuffer("unseeded_put");
        buffer->write('A');
        getAsyncPolicy().setAutoExecute(true);
        buffer->finalize();
    }
    ASSERT_EQ(client->attempts_seen.size(), 1u);
    EXPECT_FALSE(client->attempts_seen.front().has_value());

    /// The native HEAD's seed: `S3ObjectStorage::tryGetObjectMetadataWithNativeToken`'s profile-aware
    /// overload now forwards `request.attempt_number`, like every other verb here; this exercises the
    /// seed-carrying layer directly -- `S3::getObjectInfoIfExists`, the same call
    /// `tryGetObjectMetadataImpl` makes.
    client->attempts_seen.clear();
    S3::getObjectInfoIfExists(*client, bucket, "seeded_head", /*version_id=*/{}, /*with_metadata=*/false,
                               /*with_tags=*/false, ObjectStorageRequestMode::Default, /*attempt_seed=*/4);
    ASSERT_EQ(client->attempts_seen.size(), 1u);
    EXPECT_EQ(client->attempts_seen.front(), 4u);
    client->attempts_seen.clear();
    S3::getObjectInfoIfExists(*client, bucket, "unseeded_head");
    ASSERT_EQ(client->attempts_seen.size(), 1u);
    EXPECT_FALSE(client->attempts_seen.front().has_value());

    /// Conditional (single) and bulk DELETE: reachable now through `S3ObjectStorage`'s
    /// `ObjectStorageControlRequest`-carrying overloads, which is what actually drives
    /// `removeObjectIfTokenMatchesImpl`/`removeObjectsIfExistImpl` with a real nonzero seed, through the
    /// object storage's own API rather than a lower-level free function.
    (void)getContext(); // BlobStorageLogWriter::create falls back to the global context
    auto delete_store = std::make_shared<MockS3::S3MemStrore>();
    delete_store->CreateBucket(bucket);
    auto owned_delete_client = std::make_unique<MockS3::Client>(delete_store);
    MockS3::Client * delete_client = owned_delete_client.get();
    S3::URI delete_uri;
    delete_uri.bucket = bucket;
    auto delete_object_storage = std::make_shared<S3ObjectStorage>(
        std::move(owned_delete_client),
        std::make_unique<S3Settings>(),
        delete_uri,
        S3Capabilities{},
        ObjectStorageKeyGeneratorPtr{},
        "seed-delete-disk");

    delete_client->attempts_seen.clear();
    delete_object_storage->removeObjectIfTokenMatches(StoredObject("unseeded-delete-key"), "etag-1");
    ASSERT_EQ(delete_client->attempts_seen.size(), 1u);
    EXPECT_FALSE(delete_client->attempts_seen.front().has_value());

    delete_client->attempts_seen.clear();
    delete_object_storage->removeObjectIfTokenMatches(
        StoredObject("seeded-delete-key"), "etag-1", ObjectStorageControlRequest{.attempt_number = 3});
    ASSERT_EQ(delete_client->attempts_seen.size(), 1u);
    EXPECT_EQ(delete_client->attempts_seen.front(), 3u);

    delete_client->attempts_seen.clear();
    delete_object_storage->removeObjectsIfExistUnderProfile({StoredObject("unseeded-bulk-key")}, ObjectStorageControlRequest{});
    ASSERT_EQ(delete_client->attempts_seen.size(), 1u);
    EXPECT_FALSE(delete_client->attempts_seen.front().has_value());

    delete_client->attempts_seen.clear();
    delete_object_storage->removeObjectsIfExistUnderProfile(
        {StoredObject("seeded-bulk-key")}, ObjectStorageControlRequest{.attempt_number = 3});
    ASSERT_EQ(delete_client->attempts_seen.size(), 1u);
    EXPECT_EQ(delete_client->attempts_seen.front(), 3u);
}

TEST_F(WBS3Test, S3RequestAttemptSeedListPagesCarryTheSeed)
{
    /// Drives the seed through the public `iterate` overload a real caller (the CAS backend's LIST
    /// primitive) uses, rather than the anonymous-namespace `S3IteratorAsync` directly -- that class is
    /// an implementation detail of `S3ObjectStorage.cpp` and not reachable from a test in this file.
    auto list_store = std::make_shared<MockS3::S3MemStrore>();
    list_store->CreateBucket(bucket);
    auto owned_list_client = std::make_unique<MockS3::Client>(list_store);
    MockS3::Client * list_client = owned_list_client.get();
    S3::URI list_uri;
    list_uri.bucket = bucket;
    auto list_object_storage = std::make_shared<S3ObjectStorage>(
        std::move(owned_list_client),
        std::make_unique<S3Settings>(),
        list_uri,
        S3Capabilities{},
        ObjectStorageKeyGeneratorPtr{},
        "seed-list-disk");

    auto & bucket_store = list_store->GetBucketStore(bucket);
    for (int i = 0; i < 5; ++i)
        bucket_store.PutObject(fmt::format("p/{}", i), "x");

    /// Profile is left at Default (not SingleAttempt): that would route through
    /// `clientForRetryProfile`'s single-attempt clone, whose `cloneWithConfigurationOverride` the mock
    /// client does not override, and the test would stop exercising the mock entirely.
    list_client->attempts_seen.clear();
    auto iterator = list_object_storage->iterate(
        "p/", /*max_keys=*/2, /*with_tags=*/false, std::optional<std::string>("p/0"),
        ObjectStorageControlRequest{.attempt_number = 2});
    size_t seen = 0;
    for (; iterator->isValid(); iterator->next())
        ++seen;
    EXPECT_EQ(seen, 4u);
    ASSERT_EQ(list_client->attempts_seen.size(), 2u);   /// the initial page and one rebuilt page
    EXPECT_EQ(list_client->attempts_seen[0], 2u);
    EXPECT_EQ(list_client->attempts_seen[1], 2u);

    /// Seed 0 adds no header on either page.
    list_client->attempts_seen.clear();
    auto unseeded_iterator = list_object_storage->iterate(
        "p/", /*max_keys=*/2, /*with_tags=*/false, std::optional<std::string>("p/0"), ObjectStorageControlRequest{});
    seen = 0;
    for (; unseeded_iterator->isValid(); unseeded_iterator->next())
        ++seen;
    EXPECT_EQ(seen, 4u);
    ASSERT_EQ(list_client->attempts_seen.size(), 2u);
    EXPECT_FALSE(list_client->attempts_seen[0].has_value());
    EXPECT_FALSE(list_client->attempts_seen[1].has_value());
}

#endif
