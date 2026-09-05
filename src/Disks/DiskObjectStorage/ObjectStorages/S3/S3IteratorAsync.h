#pragma once

#include "config.h"

#if USE_AWS_S3

#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageIteratorAsync.h>
#include <IO/S3/Client.h>
#include <IO/S3/Requests.h>
#include <IO/S3/getObjectInfo.h>
#include <IO/S3Common.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/ProfileEvents.h>
#include <Common/quoteString.h>

namespace ProfileEvents
{
    extern const Event S3ListObjects;
    extern const Event S3ListObjectsMicroseconds;
    extern const Event DiskS3ListObjects;
}

namespace CurrentMetrics
{
    extern const Metric ObjectStorageS3Threads;
    extern const Metric ObjectStorageS3ThreadsActive;
    extern const Metric ObjectStorageS3ThreadsScheduled;
}

namespace DB
{

/// Split out of `S3ObjectStorage.cpp`'s anonymous namespace so a unit test can construct it directly
/// against a mock `S3::Client` (see `gtest_writebuffer_s3.cpp`).
class S3IteratorAsync final : public IObjectStorageIteratorAsync
{
public:
    S3IteratorAsync(
        const std::string & bucket_,
        const std::string & path_prefix,
        std::shared_ptr<const S3::Client> client_,
        size_t max_list_size,
        bool with_tags_,
        const std::optional<std::string> & start_after_,
        size_t attempt_seed_ = 0)
        : IObjectStorageIteratorAsync(
            CurrentMetrics::ObjectStorageS3Threads,
            CurrentMetrics::ObjectStorageS3ThreadsActive,
            CurrentMetrics::ObjectStorageS3ThreadsScheduled,
            ThreadName::S3_LIST_POOL)
        , client(client_)
        , request(std::make_unique<S3::ListObjectsV2Request>())
        , with_tags(with_tags_)
        , start_after_set(start_after_.has_value() && !start_after_->empty())
        , attempt_seed(attempt_seed_)
    {
        request->SetBucket(bucket_);
        request->SetPrefix(path_prefix);
        request->SetMaxKeys(static_cast<int>(max_list_size));
        if (start_after_set)
            request->SetStartAfter(*start_after_);
        if (attempt_seed != 0)
            S3::setClickhouseAttemptNumber(*request, attempt_seed);
    }

    ~S3IteratorAsync() override
    {
        /// Deactivate background threads before resetting the request to avoid data race.
        deactivate();
        request.reset();
        client.reset();
    }

private:
    bool getBatchAndCheckNext(RelativePathsWithMetadata & batch) override
    {
        ProfileEvents::increment(ProfileEvents::S3ListObjects);
        ProfileEvents::increment(ProfileEvents::DiskS3ListObjects);

        Aws::S3::Model::ListObjectsV2Outcome outcome;

        {
            ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::S3ListObjectsMicroseconds);
            outcome = client->ListObjectsV2(*request);
        }

        /// Outcome failure will be handled on the caller side.
        if (outcome.IsSuccess())
        {
            const auto next_continuation_token = outcome.GetResult().GetNextContinuationToken();
            if (start_after_set)
            {
                /// StartAfter should only be sent on the first request. AWS SDK doesn't provide
                /// a way to clear "has been set" flag, so we rebuild request for pagination.
                auto paginated_request = std::make_unique<S3::ListObjectsV2Request>();
                paginated_request->SetBucket(request->GetBucket());
                paginated_request->SetPrefix(request->GetPrefix());
                paginated_request->SetMaxKeys(request->GetMaxKeys());
                paginated_request->SetContinuationToken(next_continuation_token);
                if (attempt_seed != 0)
                    S3::setClickhouseAttemptNumber(*paginated_request, attempt_seed);
                request = std::move(paginated_request);
                start_after_set = false;
            }
            else
            {
                request->SetContinuationToken(next_continuation_token);
            }

            auto objects = outcome.GetResult().GetContents();
            for (const auto & object : objects)
            {
                ObjectMetadata metadata{
                    .size_bytes = static_cast<uint64_t>(object.GetSize()),
                    .last_modified = Poco::Timestamp::fromEpochTime(object.GetLastModified().Seconds()),
                    .etag = object.GetETag(),
                    .tags = {},
                    .attributes = {},
                };
                if (with_tags)
                    metadata.tags = S3::getObjectTags(*client, request->GetBucket(), object.GetKey());
                batch.emplace_back(std::make_shared<RelativePathWithMetadata>(object.GetKey(), std::move(metadata)));
            }

            /// It returns false when all objects were returned
            return outcome.GetResult().GetIsTruncated();
        }

        throw S3Exception(outcome.GetError().GetErrorType(),
                          "Could not list objects in bucket {} with prefix {}, S3 exception: {}, message: {}",
                          quoteString(request->GetBucket()), quoteString(request->GetPrefix()),
                          backQuote(outcome.GetError().GetExceptionName()), quoteString(outcome.GetError().GetMessage()));
    }

    std::shared_ptr<const S3::Client> client;
    std::unique_ptr<S3::ListObjectsV2Request> request;
    const bool with_tags;
    bool start_after_set;
    const size_t attempt_seed;
};

}

#endif
