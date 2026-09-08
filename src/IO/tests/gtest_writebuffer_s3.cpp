#include <gtest/gtest.h>

#include "config.h"

#if USE_AWS_S3

#include "WriteBufferS3TestMocks.h"

using namespace DB;

INSTANTIATE_TEST_SUITE_P(WBS3
    , SyncAsync
    , ::testing::Values(true, false)
    , [] (const ::testing::TestParamInfo<SyncAsync::ParamType>& info_param) {
        std::string name = info_param.param ? "async" : "sync";
        return name;
  });

TEST_P(SyncAsync, ExceptionOnHead) {
    setInjectionModel(std::make_shared<MockS3::HeadObjectFailIngection>());

    getSettings()[Setting::s3_check_objects_after_upload] = true;

    EXPECT_THROW({
        try {
            auto buffer = getWriteBuffer("exception_on_head_1");
            buffer->write('A');
            buffer->next();

            getAsyncPolicy().setAutoExecute(true);
            buffer->finalize();
        }
        catch( const DB::Exception& e )
        {
            ASSERT_EQ(ErrorCodes::S3_ERROR, e.code());
            EXPECT_THAT(e.what(), testing::HasSubstr("Immediately after upload:"));
            throw;
        }
    }, DB::S3Exception);
}

TEST_P(SyncAsync, ExceptionOnPut) {
    setInjectionModel(std::make_shared<MockS3::PutObjectFailIngection>());

    EXPECT_THROW({
        try
        {
            auto buffer = getWriteBuffer("exception_on_put_1");
            buffer->write('A');
            buffer->next();

            getAsyncPolicy().setAutoExecute(true);
            buffer->finalize();
        }
        catch( const DB::Exception& e )
        {
            ASSERT_EQ(ErrorCodes::S3_ERROR, e.code());
            EXPECT_THAT(e.what(), testing::HasSubstr("PutObjectFailIngection"));
            throw;
        }
      }, DB::S3Exception);

    EXPECT_THROW({
        try {
            auto buffer = getWriteBuffer("exception_on_put_2");
            buffer->write('A');

            getAsyncPolicy().setAutoExecute(true);
            buffer->finalize();
        }
        catch( const DB::Exception& e )
        {
            ASSERT_EQ(ErrorCodes::S3_ERROR, e.code());
            EXPECT_THAT(e.what(), testing::HasSubstr("PutObjectFailIngection"));
            throw;
        }
      }, DB::S3Exception);

    EXPECT_THROW({
        try {
            auto buffer = getWriteBuffer("exception_on_put_3");
            buffer->write('A');
            getAsyncPolicy().setAutoExecute(true);
            buffer->preFinalize();

            getAsyncPolicy().setAutoExecute(true);
            buffer->finalize();
        }
        catch( const DB::Exception& e )
        {
            ASSERT_EQ(ErrorCodes::S3_ERROR, e.code());
            EXPECT_THAT(e.what(), testing::HasSubstr("PutObjectFailIngection"));
            throw;
        }
      }, DB::S3Exception);

}

TEST_P(SyncAsync, ExceptionOnCreateMPU) {
    setInjectionModel(std::make_shared<MockS3::CreateMPUFailIngection>());

    getSettings()[Setting::s3_max_single_part_upload_size] = 0; // no single part
    getSettings()[Setting::s3_min_upload_part_size] = 1; // small parts ara ok

    EXPECT_THROW({
        try {
            auto buffer = getWriteBuffer("exception_on_create_mpu_1");
            buffer->write('A');
            buffer->next();
            buffer->write('A');
            buffer->next();

            getAsyncPolicy().setAutoExecute(true);
            buffer->finalize();
        }
        catch( const DB::Exception& e )
        {
            ASSERT_EQ(ErrorCodes::S3_ERROR, e.code());
            EXPECT_THAT(e.what(), testing::HasSubstr("CreateMPUFailIngection"));
            throw;
        }
      }, DB::S3Exception);

    EXPECT_THROW({
        try {
            auto buffer = getWriteBuffer("exception_on_create_mpu_2");
            buffer->write('A');
            buffer->preFinalize();

            getAsyncPolicy().setAutoExecute(true);
            buffer->finalize();
        }
        catch( const DB::Exception& e )
        {
            ASSERT_EQ(ErrorCodes::S3_ERROR, e.code());
            EXPECT_THAT(e.what(), testing::HasSubstr("CreateMPUFailIngection"));
            throw;
        }
      }, DB::S3Exception);

    EXPECT_THROW({
        try {
            auto buffer = getWriteBuffer("exception_on_create_mpu_2");
            buffer->write('A');

            getAsyncPolicy().setAutoExecute(true);
            buffer->finalize();
        }
        catch( const DB::Exception& e )
        {
            ASSERT_EQ(ErrorCodes::S3_ERROR, e.code());
            EXPECT_THAT(e.what(), testing::HasSubstr("CreateMPUFailIngection"));
            throw;
        }
      }, DB::S3Exception);
}


TEST_P(SyncAsync, ExceptionOnCompleteMPU) {
    setInjectionModel(std::make_shared<MockS3::CompleteMPUFailIngection>());

    getSettings()[Setting::s3_max_single_part_upload_size] = 0; // no single part
    getSettings()[Setting::s3_min_upload_part_size] = 1; // small parts ara ok

    EXPECT_THROW({
        try {
            auto buffer = getWriteBuffer("exception_on_complete_mpu_1");
            buffer->write('A');

            getAsyncPolicy().setAutoExecute(true);
            buffer->finalize();
        }
        catch(const DB::Exception & e)
        {
            ASSERT_EQ(ErrorCodes::S3_ERROR, e.code());
            EXPECT_THAT(e.what(), testing::HasSubstr("CompleteMPUFailIngection"));
            throw;
        }
      }, DB::S3Exception);
}

TEST_P(SyncAsync, ExceptionOnUploadPart) {
    setInjectionModel(std::make_shared<MockS3::UploadPartFailIngection>());

    getSettings()[Setting::s3_max_single_part_upload_size] = 0; // no single part
    getSettings()[Setting::s3_min_upload_part_size] = 1; // small parts ara ok

    MockS3::EventCounts counters = {.multiUploadCreate = 1, .multiUploadAbort = 1};

    counters.uploadParts = 2;

    EXPECT_THROW({
        try {
            auto buffer = getWriteBuffer("exception_on_upload_part_1");

            buffer->write('A');
            buffer->next();
            buffer->write('A');
            buffer->next();

            getAsyncPolicy().setAutoExecute(true);

            buffer->finalize();
        }
        catch(const DB::Exception & e)
        {
            assertCountersEQ(counters);
            ASSERT_EQ(ErrorCodes::S3_ERROR, e.code());
            EXPECT_THAT(e.what(), testing::HasSubstr("UploadPartFailIngection"));
            throw;
        }
      }, DB::S3Exception);

    EXPECT_THROW({
        try {
            auto buffer = getWriteBuffer("exception_on_upload_part_2");
            getAsyncPolicy().setAutoExecute(true);

            buffer->write('A');
            buffer->next();

            buffer->write('A');
            buffer->next();

            buffer->finalize();
        }
        catch(const DB::Exception & e)
        {
            assertCountersEQ(counters);
            ASSERT_EQ(ErrorCodes::S3_ERROR, e.code());
            EXPECT_THAT(e.what(), testing::HasSubstr("UploadPartFailIngection"));
            throw;
        }
      }, DB::S3Exception);

    counters.uploadParts = 1;

    EXPECT_THROW({
        try {
            auto buffer = getWriteBuffer("exception_on_upload_part_3");
            buffer->write('A');

            buffer->preFinalize();

            getAsyncPolicy().setAutoExecute(true);
            buffer->finalize();
        }
        catch(const DB::Exception & e)
        {
            assertCountersEQ(counters);
            ASSERT_EQ(ErrorCodes::S3_ERROR, e.code());
            EXPECT_THAT(e.what(), testing::HasSubstr("UploadPartFailIngection"));
            throw;
        }
      }, DB::S3Exception);

    EXPECT_THROW({
        try {
            auto buffer = getWriteBuffer("exception_on_upload_part_4");
            buffer->write('A');

            getAsyncPolicy().setAutoExecute(true);
            buffer->finalize();
        }
        catch(const DB::Exception & e)
        {
            assertCountersEQ(counters);
            ASSERT_EQ(ErrorCodes::S3_ERROR, e.code());
            EXPECT_THAT(e.what(), testing::HasSubstr("UploadPartFailIngection"));
            throw;
        }
      }, DB::S3Exception);
}


TEST_F(WBS3Test, PrefinalizeCalledMultipleTimes) {
#ifdef DEBUG_OR_SANITIZER_BUILD
    GTEST_SKIP() << "this test trigger LOGICAL_ERROR, runs only if DEBUG_OR_SANITIZER_BUILD is not defined";
#else
    EXPECT_THROW({
        try {
            auto buffer = getWriteBuffer("prefinalize_called_multiple_times");
            buffer->write('A');
            buffer->next();
            buffer->preFinalize();
            buffer->write('A');
            buffer->next();
            buffer->preFinalize();
            buffer->finalize();
        }
        catch(const DB::Exception & e)
        {
            ASSERT_EQ(ErrorCodes::LOGICAL_ERROR, e.code());
            EXPECT_THAT(e.what(), testing::HasSubstr("write to prefinalized buffer for S3"));
            throw;
        }
    }, DB::Exception);
#endif
}

// The object ETag from the PutObject / CompleteMultipartUpload response is surfaced via
// getResultObjectETag() after a successful finalize() — lets content-addressed callers record the
// just-written incarnation's token WITHOUT a follow-up HEAD (CA head-after-put elimination).
TEST_F(WBS3Test, ResultObjectETagIsCaptured) {
    // Singlepart upload: the PutObject response ETag.
    {
        auto buffer = getWriteBuffer("singlepart-file");
        writeAsOneBlock(*buffer, 10);
        getAsyncPolicy().setAutoExecute(true);
        buffer->finalize();
        ASSERT_TRUE(buffer->getResultObjectETag().has_value());
        ASSERT_EQ(*buffer->getResultObjectETag(), "etag-singlepart-singlepart-file");
    }

    // Multipart upload: the final object ETag comes from CompleteMultipartUpload, NOT a per-part tag.
    {
        getSettings()[Setting::s3_max_single_part_upload_size] = 0; // no single part — force multipart
        getSettings()[Setting::s3_min_upload_part_size] = 1;
        auto buffer = getWriteBuffer("multipart-file");
        writeAsOneBlock(*buffer, 10);
        getAsyncPolicy().setAutoExecute(true);
        buffer->finalize();
        ASSERT_TRUE(buffer->getResultObjectETag().has_value());
        ASSERT_EQ(*buffer->getResultObjectETag(), "etag-multipart-multipart-file");
    }
}

TEST_P(SyncAsync, EmptyFile) {
    getSettings()[Setting::s3_check_objects_after_upload] = true;

    MockS3::EventCounts counters = {.headObject = 2, .putObject = 1};
    runSimpleScenario(counters, 0);
}

TEST_P(SyncAsync, ManualNextCalls) {
    getSettings()[Setting::s3_check_objects_after_upload] = true;

    {
        MockS3::EventCounts counters = {.headObject = 2, .putObject = 1};

        auto buffer = getWriteBuffer("manual_next_calls_1");
        buffer->next();

        getAsyncPolicy().setAutoExecute(true);
        buffer->finalize();

        assertCountersEQ(counters);
    }

    {
        MockS3::EventCounts counters = {.headObject = 2, .putObject = 1};

        auto buffer = getWriteBuffer("manual_next_calls_2");
        buffer->next();
        buffer->next();

        getAsyncPolicy().setAutoExecute(true);
        buffer->finalize();

        assertCountersEQ(counters);
    }

    {
        MockS3::EventCounts counters = {.headObject = 2, .putObject = 1, .writtenSize = 1};

        auto buffer = getWriteBuffer("manual_next_calls_3");
        buffer->next();
        buffer->write('A');
        buffer->next();

        getAsyncPolicy().setAutoExecute(true);
        buffer->finalize();

        assertCountersEQ(counters);
    }

    {
        MockS3::EventCounts counters = {.headObject = 2, .putObject = 1, .writtenSize = 2};

        auto buffer = getWriteBuffer("manual_next_calls_4");
        buffer->write('A');
        buffer->next();
        buffer->write('A');
        buffer->next();
        buffer->next();

        getAsyncPolicy().setAutoExecute(true);
        buffer->finalize();

        assertCountersEQ(counters);
     }
}

TEST_P(SyncAsync, SmallFileIsOnePutRequest) {
    getSettings()[Setting::s3_check_objects_after_upload] = true;

    {
        getSettings()[Setting::s3_max_single_part_upload_size] = 1000;
        getSettings()[Setting::s3_min_upload_part_size] = 10;

        MockS3::EventCounts counters = {.headObject = 2, .putObject = 1};

        runSimpleScenario(counters, 1);
        runSimpleScenario(counters, getSettings()[Setting::s3_max_single_part_upload_size] - 1);
        runSimpleScenario(counters, getSettings()[Setting::s3_max_single_part_upload_size]);
        runSimpleScenario(counters, getSettings()[Setting::s3_max_single_part_upload_size] / 2);
    }

    {
        getSettings()[Setting::s3_max_single_part_upload_size] = 10;
        getSettings()[Setting::s3_min_upload_part_size] = 1000;

        MockS3::EventCounts counters = {.headObject = 2, .putObject = 1};

        runSimpleScenario(counters, 1);
        runSimpleScenario(counters, getSettings()[Setting::s3_max_single_part_upload_size] - 1);
        runSimpleScenario(counters, getSettings()[Setting::s3_max_single_part_upload_size]);
        runSimpleScenario(counters, getSettings()[Setting::s3_max_single_part_upload_size] / 2);
    }
}

TEST_P(SyncAsync, LittleBiggerFileIsMultiPartUpload) {
    getSettings()[Setting::s3_check_objects_after_upload] = true;

    {
        getSettings()[Setting::s3_max_single_part_upload_size] = 1000;
        getSettings()[Setting::s3_min_upload_part_size] = 10;

        MockS3::EventCounts counters = {.headObject = 2, .multiUploadCreate = 1, .multiUploadComplete = 1, .uploadParts = 2};
        runSimpleScenario(counters, settings[Setting::s3_max_single_part_upload_size] + 1);

        counters.uploadParts = 101;
        runSimpleScenario(counters, 2 * settings[Setting::s3_max_single_part_upload_size]);
    }

    {
        getSettings()[Setting::s3_max_single_part_upload_size] = 10;
        getSettings()[Setting::s3_min_upload_part_size] = 1000;

        MockS3::EventCounts counters = {.headObject = 2, .multiUploadCreate = 1, .multiUploadComplete = 1, .uploadParts = 1};

        runSimpleScenario(counters, settings[Setting::s3_max_single_part_upload_size] + 1);
        runSimpleScenario(counters, 2 * settings[Setting::s3_max_single_part_upload_size]);
        runSimpleScenario(counters, settings[Setting::s3_min_upload_part_size] - 1);
        runSimpleScenario(counters, settings[Setting::s3_min_upload_part_size]);
    }
}

TEST_P(SyncAsync, BiggerFileIsMultiPartUpload) {
    getSettings()[Setting::s3_check_objects_after_upload] = true;

    {
        getSettings()[Setting::s3_max_single_part_upload_size] = 1000;
        getSettings()[Setting::s3_min_upload_part_size] = 10;

        auto counters = MockS3::EventCounts{.headObject = 2, .multiUploadCreate = 1, .multiUploadComplete = 1, .uploadParts = 2};
        runSimpleScenario(counters, settings[Setting::s3_max_single_part_upload_size] + settings[Setting::s3_min_upload_part_size]);

        counters.uploadParts = 3;
        runSimpleScenario(counters, settings[Setting::s3_max_single_part_upload_size] + settings[Setting::s3_min_upload_part_size] + 1);
        runSimpleScenario(counters, settings[Setting::s3_max_single_part_upload_size] + 2 * settings[Setting::s3_min_upload_part_size] - 1);
        runSimpleScenario(counters, settings[Setting::s3_max_single_part_upload_size] + 2 * settings[Setting::s3_min_upload_part_size]);
    }


    {
        // but not in that case, when s3_min_upload_part_size > s3_max_single_part_upload_size
        getSettings()[Setting::s3_max_single_part_upload_size] = 10;
        getSettings()[Setting::s3_min_upload_part_size] = 1000;

        auto counters = MockS3::EventCounts{.headObject = 2, .multiUploadCreate = 1, .multiUploadComplete = 1, .uploadParts = 2};
        runSimpleScenario(counters, settings[Setting::s3_max_single_part_upload_size] + settings[Setting::s3_min_upload_part_size]);
        runSimpleScenario(counters, settings[Setting::s3_max_single_part_upload_size] + settings[Setting::s3_min_upload_part_size] + 1);
        runSimpleScenario(counters, 2 * settings[Setting::s3_min_upload_part_size] - 1);
        runSimpleScenario(counters, 2 * settings[Setting::s3_min_upload_part_size]);

        counters.uploadParts = 3;
        runSimpleScenario(counters, 2 * settings[Setting::s3_min_upload_part_size] + 1);
    }
}

TEST_P(SyncAsync, IncreaseUploadBuffer) {
    getSettings()[Setting::s3_check_objects_after_upload] = true;

    {
        getSettings()[Setting::s3_max_single_part_upload_size] = 10;
        getSettings()[Setting::s3_min_upload_part_size] = 10;
        getSettings()[Setting::s3_upload_part_size_multiply_parts_count_threshold] = 1;
        // parts: 10 20 40 80  160
        // size:  10 30 70 150 310

        auto counters = MockS3::EventCounts{.headObject = 2, .multiUploadCreate = 1, .multiUploadComplete = 1, .uploadParts = 6};
        runSimpleScenario(counters, 350);

        auto actual_parts_sizes = MockS3::BucketMemStore::GetPartSizes(getCompletedPartUploads().back().second);
        ASSERT_THAT(actual_parts_sizes, testing::ElementsAre(10, 20, 40, 80, 160, 40));
    }

    {
        getSettings()[Setting::s3_max_single_part_upload_size] = 10;
        getSettings()[Setting::s3_min_upload_part_size] = 10;
        getSettings()[Setting::s3_upload_part_size_multiply_parts_count_threshold] = 2;
        getSettings()[Setting::s3_upload_part_size_multiply_factor] = 3;
        // parts: 10 10 30 30 90
        // size:  10 20 50 80 170

        auto counters = MockS3::EventCounts{.headObject = 2, .multiUploadCreate = 1, .multiUploadComplete = 1, .uploadParts = 6};
        runSimpleScenario(counters, 190);

        auto actual_parts_sizes = MockS3::BucketMemStore::GetPartSizes(getCompletedPartUploads().back().second);
        ASSERT_THAT(actual_parts_sizes, testing::ElementsAre(10, 10, 30, 30, 90, 20));
    }
}

TEST_P(SyncAsync, IncreaseLimited) {
    getSettings()[Setting::s3_check_objects_after_upload] = true;

    {
        getSettings()[Setting::s3_max_single_part_upload_size] = 10;
        getSettings()[Setting::s3_min_upload_part_size] = 10;
        getSettings()[Setting::s3_upload_part_size_multiply_parts_count_threshold] = 1;
        getSettings()[Setting::s3_max_upload_part_size] = 45;
        // parts: 10 20 40 45  45  45
        // size:  10 30 70 115 160 205

        auto counters = MockS3::EventCounts{.headObject = 2, .multiUploadCreate = 1, .multiUploadComplete = 1, .uploadParts = 7};
        runSimpleScenario(counters, 220);

        auto actual_parts_sizes = MockS3::BucketMemStore::GetPartSizes(getCompletedPartUploads().back().second);
        ASSERT_THAT(actual_parts_sizes, testing::ElementsAre(10, 20, 40, 45, 45, 45, 15));
    }
}

TEST_P(SyncAsync, StrictUploadPartSize) {
    getSettings()[Setting::s3_check_objects_after_upload] = false;

    {
        getSettings()[Setting::s3_max_single_part_upload_size] = 10;
        getSettings()[Setting::s3_strict_upload_part_size] = 11;

        {
            auto counters = MockS3::EventCounts{.multiUploadCreate = 1, .multiUploadComplete = 1, .uploadParts = 6};
            runSimpleScenario(counters, 66);

            auto actual_parts_sizes = MockS3::BucketMemStore::GetPartSizes(getCompletedPartUploads().back().second);
            ASSERT_THAT(actual_parts_sizes, testing::ElementsAre(11, 11, 11, 11, 11, 11));

            // parts: 11 22 33 44 55 66
            // size:  11 11 11 11 11 11
        }

        {
            auto counters = MockS3::EventCounts{.multiUploadCreate = 1, .multiUploadComplete = 1, .uploadParts = 7};
            runSimpleScenario(counters, 67);

            auto actual_parts_sizes = MockS3::BucketMemStore::GetPartSizes(getCompletedPartUploads().back().second);
            ASSERT_THAT(actual_parts_sizes, testing::ElementsAre(11, 11, 11, 11, 11, 11, 1));
        }
    }
}

/// Task 3: the actual PutObject request a single-part upload issues must carry the typed
/// NativeConditional mode exactly when the caller's WriteSettings asked for it -- the old blanket GCS
/// dialect stays authoritative over the wire until a later task; this only proves the mark reaches
/// the production request object (mirrors the HEAD/DELETE marking tests in
/// S3ObjectStorageConditionalOpsTest below).
TEST_F(WBS3Test, PutObjectNativeConditionalModePropagates)
{
    WriteSettings ws;
    ws.object_storage_request_mode = ObjectStorageRequestMode::NativeConditional;

    auto buffer = getWriteBuffer("native_conditional_put", ws);
    buffer->write('A');
    getAsyncPolicy().setAutoExecute(true);
    buffer->finalize();

    EXPECT_EQ(client->counters.putObject, 1);
    EXPECT_TRUE(client->last_put_object_native_conditional);
}

/// The control: an ordinary (Default-mode) single-part upload must NOT pick up the mark.
TEST_F(WBS3Test, PutObjectOrdinaryWriteRemainsDefault)
{
    auto buffer = getWriteBuffer("ordinary_put");
    buffer->write('A');
    getAsyncPolicy().setAutoExecute(true);
    buffer->finalize();

    EXPECT_EQ(client->counters.putObject, 1);
    EXPECT_FALSE(client->last_put_object_native_conditional);
}

/// A multipart upload's CompleteMultipartUpload request must carry the mode too (Task 4's native
/// adapter consumes it as a defense-in-depth guard against a conditional multipart completion), while
/// CreateMultipartUpload and UploadPart -- which no consumer needs marked -- must NOT.
TEST_F(WBS3Test, CompleteMultipartUploadNativeConditionalModePropagatesButCreateAndUploadPartDoNot)
{
    getSettings()[Setting::s3_max_single_part_upload_size] = 0; // force multipart
    getSettings()[Setting::s3_min_upload_part_size] = 1;

    WriteSettings ws;
    ws.object_storage_request_mode = ObjectStorageRequestMode::NativeConditional;

    auto buffer = getWriteBuffer("native_conditional_multipart", ws);
    buffer->write('A');
    buffer->next();
    buffer->write('A');

    getAsyncPolicy().setAutoExecute(true);
    buffer->finalize();

    EXPECT_EQ(client->counters.multiUploadComplete, 1);
    EXPECT_TRUE(client->last_complete_multipart_native_conditional);
    EXPECT_FALSE(client->last_create_multipart_native_conditional);
    EXPECT_FALSE(client->last_upload_part_native_conditional);
}

/// Mock-S3 coverage for token-exact removal plus ordinary and native-only copy modes.
class S3ObjectStorageConditionalOpsTest : public ::testing::Test
{
public:
    const String bucket = "cond-ops-bucket";
    const String disk_name = "cond-ops-disk";

    std::shared_ptr<S3ObjectStorage> object_storage;
    MockS3::Client * mock_client = nullptr;
    std::shared_ptr<MockS3::S3MemStrore> store;

protected:
    std::shared_ptr<S3ObjectStorage> createObjectStorage(
        const String & storage_bucket,
        const String & storage_disk_name,
        bool allow_native_copy,
        MockS3::Client *& client_out)
    {
        auto owned_client = std::make_unique<MockS3::Client>(store);
        client_out = owned_client.get();

        auto settings = std::make_unique<S3Settings>();
        settings->request_settings[S3RequestSetting::allow_native_copy] = allow_native_copy;

        S3::URI uri;
        uri.bucket = storage_bucket;
        S3Capabilities capabilities;
        ObjectStorageKeyGeneratorPtr key_generator;

        return std::make_shared<S3ObjectStorage>(
            std::move(owned_client),
            std::move(settings),
            std::move(uri),
            capabilities,
            key_generator,
            storage_disk_name);
    }

    void resetObjectStorage(bool allow_native_copy = true)
    {
        object_storage = createObjectStorage(bucket, disk_name, allow_native_copy, mock_client);
    }

    void SetUp() override
    {
        /// `removeObjectIfTokenMatches` and `copyObject` call `BlobStorageLogWriter::create`, which
        /// falls back to `Context::getGlobalContextInstance`
        /// when there is no query context. Force that global context to exist (harmless -- blob
        /// storage logging stays off by default) regardless of which other gtest TU ran first.
        (void)getContext();

        store = std::make_shared<MockS3::S3MemStrore>();
        store->CreateBucket(bucket);
        resetObjectStorage();
    }

    void TearDown() override
    {
        object_storage.reset();
        mock_client = nullptr;
        store.reset();
    }
};

TEST_F(S3ObjectStorageConditionalOpsTest, DefaultCopyObjectMayFallback)
{
    store->GetBucketStore(bucket).PutObject("src-key", "hello-world");
    mock_client->setInjectionModel(std::make_shared<MockS3::CopyObjectErrorInjection>(
        Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::ACCESS_DENIED, "AccessDenied", "access denied", false)));

    object_storage->copyObject(
        StoredObject("src-key"), StoredObject("dst-key"), ReadSettings{}, WriteSettings{}, std::nullopt);

    EXPECT_TRUE(object_storage->supportsCopyMode(ObjectStorageCopyMode::Default));
    EXPECT_TRUE(store->GetBucketStore(bucket).objects.contains("dst-key"));
    EXPECT_EQ(mock_client->counters.copyObject, 1);
    EXPECT_EQ(mock_client->counters.putObject, 1);
    EXPECT_FALSE(mock_client->last_copy_object_native_conditional);
    EXPECT_FALSE(mock_client->last_copy_object_if_match);
    EXPECT_FALSE(mock_client->last_copy_object_if_none_match);
}

TEST_F(S3ObjectStorageConditionalOpsTest, NativeOnlyCopyObjectUsesNativeTransport)
{
    store->GetBucketStore(bucket).PutObject("src-key", "hello-world");

    WriteSettings write_settings;
    write_settings.object_storage_copy_mode = ObjectStorageCopyMode::NativeOnly;
    object_storage->copyObject(
        StoredObject("src-key"), StoredObject("dst-key"), ReadSettings{}, write_settings, std::nullopt);

    EXPECT_TRUE(object_storage->supportsCopyMode(ObjectStorageCopyMode::NativeOnly));
    EXPECT_EQ(store->GetBucketStore(bucket).objects.at("dst-key"), "hello-world");
    EXPECT_EQ(mock_client->counters.copyObject, 1);
    EXPECT_EQ(mock_client->counters.putObject, 0);
    EXPECT_FALSE(mock_client->last_copy_object_native_conditional);
    EXPECT_FALSE(mock_client->last_copy_object_if_match);
    EXPECT_FALSE(mock_client->last_copy_object_if_none_match);
}

TEST_F(S3ObjectStorageConditionalOpsTest, NativeOnlyCopyObjectNeverFallsBack)
{
    store->GetBucketStore(bucket).PutObject("src-key", "hello-world");
    mock_client->setInjectionModel(std::make_shared<MockS3::CopyObjectErrorInjection>(
        Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::ACCESS_DENIED, "AccessDenied", "access denied", false)));

    WriteSettings write_settings;
    write_settings.object_storage_copy_mode = ObjectStorageCopyMode::NativeOnly;
    EXPECT_THROW(
        object_storage->copyObject(
            StoredObject("src-key"), StoredObject("dst-key"), ReadSettings{}, write_settings, std::nullopt),
        DB::S3Exception);

    EXPECT_EQ(mock_client->counters.copyObject, 1);
    EXPECT_EQ(mock_client->counters.putObject, 0);
    EXPECT_FALSE(store->GetBucketStore(bucket).objects.contains("dst-key"));

    resetObjectStorage(/*allow_native_copy=*/false);
    EXPECT_TRUE(object_storage->supportsCopyMode(ObjectStorageCopyMode::Default));
    EXPECT_FALSE(object_storage->supportsCopyMode(ObjectStorageCopyMode::NativeOnly));
    EXPECT_THROW({
        try
        {
            object_storage->copyObject(
                StoredObject("src-key"), StoredObject("disabled-dst-key"), ReadSettings{}, write_settings, std::nullopt);
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::NOT_IMPLEMENTED);
            throw;
        }
    }, DB::Exception);
    EXPECT_EQ(mock_client->counters.copyObject, 0);
    EXPECT_EQ(mock_client->counters.putObject, 0);
    EXPECT_FALSE(store->GetBucketStore(bucket).objects.contains("disabled-dst-key"));
}

TEST_F(S3ObjectStorageConditionalOpsTest, NativeOnlyCrossStorageCopyUsesNativeTransport)
{
    const String destination_bucket = "cond-ops-destination-bucket";
    store->CreateBucket(destination_bucket);
    store->GetBucketStore(bucket).PutObject("src-key", "hello-world");

    MockS3::Client * destination_client = nullptr;
    auto destination_storage = createObjectStorage(
        destination_bucket, "cond-ops-destination-disk", /*allow_native_copy=*/true, destination_client);

    WriteSettings write_settings;
    write_settings.object_storage_copy_mode = ObjectStorageCopyMode::NativeOnly;
    object_storage->copyObjectToAnotherObjectStorage(
        StoredObject("src-key"),
        StoredObject("dst-key"),
        ReadSettings{},
        write_settings,
        *destination_storage,
        std::nullopt);

    EXPECT_EQ(store->GetBucketStore(destination_bucket).objects.at("dst-key"), "hello-world");
    EXPECT_EQ(destination_client->counters.copyObject, 1);
    EXPECT_EQ(destination_client->counters.putObject, 0);
    EXPECT_EQ(mock_client->counters.getObject, 0);
}

TEST_F(S3ObjectStorageConditionalOpsTest, NativeOnlyCrossStorageCopyNeverFallsBackAfterAccessDenied)
{
    const String destination_bucket = "cond-ops-destination-bucket";
    store->CreateBucket(destination_bucket);
    store->GetBucketStore(bucket).PutObject("src-key", "hello-world");

    MockS3::Client * destination_client = nullptr;
    auto destination_storage = createObjectStorage(
        destination_bucket, "cond-ops-destination-disk", /*allow_native_copy=*/true, destination_client);
    destination_client->setInjectionModel(std::make_shared<MockS3::CopyObjectErrorInjection>(
        Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::ACCESS_DENIED, "AccessDenied", "access denied", false)));

    WriteSettings write_settings;
    write_settings.object_storage_copy_mode = ObjectStorageCopyMode::NativeOnly;
    EXPECT_THROW(
        object_storage->copyObjectToAnotherObjectStorage(
            StoredObject("src-key"),
            StoredObject("dst-key"),
            ReadSettings{},
            write_settings,
            *destination_storage,
            std::nullopt),
        DB::S3Exception);

    EXPECT_EQ(destination_client->counters.copyObject, 1);
    EXPECT_EQ(destination_client->counters.putObject, 0);
    EXPECT_EQ(mock_client->counters.getObject, 0);
    EXPECT_FALSE(store->GetBucketStore(destination_bucket).objects.contains("dst-key"));
}

TEST_F(S3ObjectStorageConditionalOpsTest, NativeOnlyCrossStorageCopyNeverFallsBackWhenNativeCopyIsDisabled)
{
    const String destination_bucket = "cond-ops-destination-bucket";
    store->CreateBucket(destination_bucket);
    store->GetBucketStore(bucket).PutObject("src-key", "hello-world");

    resetObjectStorage(/*allow_native_copy=*/false);
    MockS3::Client * destination_client = nullptr;
    auto destination_storage = createObjectStorage(
        destination_bucket, "cond-ops-destination-disk", /*allow_native_copy=*/true, destination_client);

    WriteSettings write_settings;
    write_settings.object_storage_copy_mode = ObjectStorageCopyMode::NativeOnly;
    EXPECT_THROW({
        try
        {
            object_storage->copyObjectToAnotherObjectStorage(
                StoredObject("src-key"),
                StoredObject("dst-key"),
                ReadSettings{},
                write_settings,
                *destination_storage,
                std::nullopt);
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::NOT_IMPLEMENTED);
            throw;
        }
    }, DB::Exception);

    EXPECT_EQ(destination_client->counters.copyObject, 0);
    EXPECT_EQ(destination_client->counters.putObject, 0);
    EXPECT_EQ(mock_client->counters.getObject, 0);
    EXPECT_FALSE(store->GetBucketStore(destination_bucket).objects.contains("dst-key"));
}

TEST_F(S3ObjectStorageConditionalOpsTest, NativeOnlyCopyToNonS3StorageFailsClosed)
{
    store->GetBucketStore(bucket).PutObject("src-key", "hello-world");

    Poco::TemporaryFile destination_directory;
    destination_directory.createDirectories();
    LocalObjectStorage destination_storage(LocalObjectStorageSettings(
        "cond-ops-local-destination", destination_directory.path(), /*read_only_=*/false));

    WriteSettings write_settings;
    write_settings.object_storage_copy_mode = ObjectStorageCopyMode::NativeOnly;
    EXPECT_THROW({
        try
        {
            object_storage->copyObjectToAnotherObjectStorage(
                StoredObject("src-key"),
                StoredObject("dst-key"),
                ReadSettings{},
                write_settings,
                destination_storage,
                std::nullopt);
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::NOT_IMPLEMENTED);
            throw;
        }
    }, DB::Exception);

    EXPECT_EQ(mock_client->counters.getObject, 0);
    EXPECT_FALSE(destination_storage.exists(StoredObject("dst-key")));
}

TEST_F(S3ObjectStorageConditionalOpsTest, RemoveObjectIfTokenMatchesSuccess)
{
    store->GetBucketStore(bucket).PutObject("key1", "data");

    auto result = object_storage->removeObjectIfTokenMatches(StoredObject("key1"), "etag-1");

    ASSERT_EQ(result.outcome, ConditionalRemoveOutcome::Removed);
    ASSERT_EQ(mock_client->counters.deleteObject, 1);
}

TEST_F(S3ObjectStorageConditionalOpsTest, RemoveObjectIfTokenMatchesPreconditionFailedIsTokenMismatch)
{
    mock_client->setInjectionModel(std::make_shared<MockS3::DeleteObjectErrorInjection>(
        Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::UNKNOWN, "PreconditionFailed", "precondition failed", false)));

    auto result = object_storage->removeObjectIfTokenMatches(StoredObject("key1"), "stale-etag");

    ASSERT_EQ(result.outcome, ConditionalRemoveOutcome::TokenMismatch);
}

TEST_F(S3ObjectStorageConditionalOpsTest, RemoveObjectIfTokenMatchesNotFoundIsNotFound)
{
    mock_client->setInjectionModel(std::make_shared<MockS3::DeleteObjectErrorInjection>(
        Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::NO_SUCH_KEY, "NoSuchKey", "not found", false)));

    auto result = object_storage->removeObjectIfTokenMatches(StoredObject("missing-key"), "any-etag");

    ASSERT_EQ(result.outcome, ConditionalRemoveOutcome::NotFound);
}

/// `tryGetObjectMetadataWithNativeToken` must mark its HEAD wrapper eligible for the typed
/// NativeConditional mode — the mark is what makes a GCS-mode client apply generation semantics to
/// this HEAD — and it must keep tryGetObjectMetadata's existing missing-object contract of returning
/// nullopt.
TEST_F(S3ObjectStorageConditionalOpsTest, NativeTokenHeadIsMarkedAndMissingIsNullopt)
{
    store->GetBucketStore(bucket).PutObject("existing-key", "some-body");

    auto found = object_storage->tryGetObjectMetadataWithNativeToken("existing-key", /*with_tags=*/false);
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(found->size_bytes, 9u);
    EXPECT_TRUE(mock_client->last_head_object_native_conditional);

    auto missing = object_storage->tryGetObjectMetadataWithNativeToken("missing-key", /*with_tags=*/false);
    EXPECT_FALSE(missing.has_value());
    EXPECT_TRUE(mock_client->last_head_object_native_conditional);
}

/// The token-exact DELETE removeObjectIfTokenMatches issues (CAS's `If-Match` reclaim) must be marked
/// eligible for the typed NativeConditional mode -- it is the exact-delete path a GCS generation token
/// belongs on.
TEST_F(S3ObjectStorageConditionalOpsTest, GenerationDeleteUsesNativeConditionalMode)
{
    store->GetBucketStore(bucket).PutObject("key1", "data");

    auto result = object_storage->removeObjectIfTokenMatches(StoredObject("key1"), "etag-1");

    ASSERT_EQ(result.outcome, ConditionalRemoveOutcome::Removed);
    EXPECT_TRUE(mock_client->last_delete_object_native_conditional);
}

/// An ordinary (non-conditional) delete must NOT pick up the native mark -- only the exact-token
/// delete path is content-addressed-storage-owned.
TEST_F(S3ObjectStorageConditionalOpsTest, OrdinaryDeleteRemainsDefault)
{
    store->GetBucketStore(bucket).PutObject("key1", "data");

    object_storage->removeObjectIfExists(StoredObject("key1"));

    /// Pin that the ordinary delete actually reached the singular DeleteObject hook this test reads --
    /// otherwise a future refactor onto the batch DeleteObjects path would silently stop exercising
    /// this assertion (the field would sit unwritten at its `false` initializer) and this test would
    /// keep passing while proving nothing.
    ASSERT_EQ(mock_client->counters.deleteObject, 1);
    EXPECT_FALSE(mock_client->last_delete_object_native_conditional);
}

[[maybe_unused]] static String fillStringWithPattern(String pattern, int n)
{
    String data;
    for (int i = 0; i < n; ++i)
    {
        data += pattern;
    }
    return data;
}

#endif
