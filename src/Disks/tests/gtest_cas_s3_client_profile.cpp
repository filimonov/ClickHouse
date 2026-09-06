#include <gtest/gtest.h>

#include "config.h"

#if USE_AWS_S3

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasObjectStorageBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasFence.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.h>
#include <Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.h>
#include <Disks/DiskObjectStorage/RegisterDiskObjectStorage.h>
#include <Disks/tests/cas_test_helpers.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/S3/Client.h>
#include <IO/S3/URI.h>
#include <IO/S3/PocoHTTPClient.h>
#include <IO/S3Defines.h>
#include <IO/S3Settings.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Common/RemoteHostFilter.h>
#include <Common/tests/gtest_global_context.h>
#include <Core/Settings.h>

#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include <Poco/Util/XMLConfiguration.h>

#include <IO/S3Common.h>
#include <aws/s3/S3Errors.h>

/// The single-attempt client clone must cap its connect timeout at the value the mount froze at open,
/// never at the disk's (possibly wider, possibly reloaded, possibly unbounded) own connect timeout.

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

/// A `<disk>` config section carrying `connect_timeout_ms`, for driving a reload through the real
/// `applyNewSettings` path (as a live disk's config reload would) rather than swapping the client
/// directly. Explicit static credentials keep the reload from falling through to the EC2 instance
/// metadata credentials provider (no access/secret key configured means "try every other provider"),
/// which would otherwise probe an unreachable metadata endpoint on every reload.
Poco::AutoPtr<Poco::Util::XMLConfiguration> configWithConnectTimeout(long connect_timeout_ms)
{
    std::istringstream xml_stream( // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        "<clickhouse><disk>"
        "<connect_timeout_ms>" + std::to_string(connect_timeout_ms) + "</connect_timeout_ms>"
        "<access_key_id>ACCESS_KEY_ID</access_key_id>"
        "<secret_access_key>SECRET_ACCESS_KEY</secret_access_key>"
        "</disk></clickhouse>");
    return new Poco::Util::XMLConfiguration(xml_stream);
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

/// A genuine `S3ObjectStorage` (not a `LocalObjectStorage`-based fake) whose object body is an
/// in-memory, ETag-conditional store faithful enough to satisfy a real writable Native `Pool::open`
/// bootstrap (`CasProbe`'s exact create/read/replace/remove/list sequence) without ever touching a
/// socket. Every profile-aware verb calls `recordSelection`, which reproduces
/// `S3ObjectStorage::clientForRetryProfile`'s one-line dispatch using only its two PUBLIC
/// destinations (`getSingleAttemptClient`/`getS3StorageClient`) -- the dispatch itself is exercised
/// end-to-end by `S3SingleAttemptClient.ConnectTimeoutIsCappedAndFrozen` and by every real
/// single-attempt/default S3 verb in `gtest_writebuffer_s3.cpp`, so this only needs to prove the cap
/// and attempt number a verb built actually reach the selected client.
class RecordingS3ObjectStorage : public DB::S3ObjectStorage
{
public:
    using DB::S3ObjectStorage::S3ObjectStorage;

    struct Selection
    {
        DB::ObjectStorageRetryProfile profile;
        uint64_t connect_timeout_cap_ms;
        size_t attempt_number;
        long selected_connect_timeout_ms;
    };
    mutable std::vector<Selection> selections;

    void recordSelection(const DB::ObjectStorageControlRequest & request) const
    {
        /// `getS3StorageClient` is non-const on the production interface; this method stays const to
        /// match the base-class overrides (`iterate`, `tryGetObjectMetadataWithNativeToken`) it is
        /// invoked from, hence the cast.
        auto used_client = request.profile == DB::ObjectStorageRetryProfile::SingleAttempt
            ? getSingleAttemptClient(request.attempt_timeout_ms, request.connect_timeout_cap_ms)
            : const_cast<RecordingS3ObjectStorage *>(this)->getS3StorageClient();
        selections.push_back({request.profile, request.connect_timeout_cap_ms, request.attempt_number,
                              used_client->getClientConfiguration().connectTimeoutMs});
    }

    std::unique_ptr<DB::ReadBufferFromFileBase> readObject(
        const DB::StoredObject & object, const DB::ReadSettings &, std::optional<size_t>, bool, bool) const override
    {
        std::lock_guard lock(mutex);
        auto it = objects.find(object.remote_path);
        if (it == objects.end())
            throw DB::S3Exception("RecordingS3ObjectStorage: object does not exist", Aws::S3::S3Errors::RESOURCE_NOT_FOUND);
        return std::make_unique<DB::ReadBufferFromOwnMemoryFile>(object.remote_path, it->second.bytes);
    }

    /// A real S3 GET answers with the object's own incarnation, which is what the CAS backend's
    /// Native `read` reads its bytes AND its token from in one request.
    DB::SmallObjectDataWithMetadata readSmallObjectAndGetObjectMetadata(
        const DB::StoredObject & object, const DB::ReadSettings &, size_t, std::optional<size_t>) const override
    {
        std::lock_guard lock(mutex);
        auto it = objects.find(object.remote_path);
        if (it == objects.end())
            throw DB::S3Exception("RecordingS3ObjectStorage: object does not exist", Aws::S3::S3Errors::RESOURCE_NOT_FOUND);
        DB::SmallObjectDataWithMetadata result;
        result.data = it->second.bytes;
        result.metadata.size_bytes = it->second.bytes.size();
        result.metadata.etag = it->second.etag;
        return result;
    }

    /// Buffers the whole body in memory so the entry is committed exactly once, at `finalize`, and
    /// only after the precondition has been checked -- mirroring a real conditional PUT's timing.
    class ConditionalWriteBuffer final : public DB::WriteBufferFromFileBase
    {
    public:
        ConditionalWriteBuffer(RecordingS3ObjectStorage & storage_, std::string key_,
                               std::string if_none_match_, std::string if_match_)
            : DB::WriteBufferFromFileBase(/*buf_size=*/8192, nullptr, 0)
            , storage(storage_), key(std::move(key_))
            , if_none_match(std::move(if_none_match_)), if_match(std::move(if_match_))
        {
        }

        void sync() override {}
        std::string getFileName() const override { return key; }
        std::optional<std::string> getResultObjectETag() const override { return committed_etag; }

    protected:
        void nextImpl() override
        {
            if (!offset())
                return;
            buffered.append(working_buffer.begin(), offset());
        }

        void finalizeImpl() override
        {
            next();
            committed_etag = storage.commitConditionalWrite(key, buffered, if_none_match, if_match);
        }

    private:
        RecordingS3ObjectStorage & storage;
        std::string key;
        std::string if_none_match;
        std::string if_match;
        std::string buffered;
        std::optional<std::string> committed_etag;
    };

    /// The conditional write's retry profile/timeout/cap ride on `WriteSettings` directly (it is not
    /// one of the four `ObjectStorageControlRequest`-taking verbs), exactly as the real
    /// `S3ObjectStorage::writeObject` reads them -- reconstructed here so `recordSelection` sees the
    /// same context that function would have built, before answering from memory.
    std::unique_ptr<DB::WriteBufferFromFileBase> writeObject(
        const DB::StoredObject & object, DB::WriteMode mode, std::optional<DB::ObjectAttributes>, size_t,
        const DB::WriteSettings & write_settings) override
    {
        if (mode != DB::WriteMode::Rewrite)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "RecordingS3ObjectStorage only supports Rewrite");
        recordSelection(DB::ObjectStorageControlRequest{
            .profile = write_settings.object_storage_retry_profile,
            .attempt_timeout_ms = write_settings.object_storage_attempt_timeout_ms,
            .connect_timeout_cap_ms = write_settings.object_storage_connect_timeout_cap_ms,
            .attempt_number = write_settings.object_storage_attempt_number});
        return std::make_unique<ConditionalWriteBuffer>(
            *this, object.remote_path, write_settings.object_storage_write_if_none_match,
            write_settings.object_storage_write_if_match);
    }

    std::optional<DB::ObjectMetadata> tryGetObjectMetadata(const std::string & path, bool /*with_tags*/) const override
    {
        std::lock_guard lock(mutex);
        auto it = objects.find(path);
        if (it == objects.end())
            return std::nullopt;
        DB::ObjectMetadata metadata;
        metadata.size_bytes = it->second.bytes.size();
        metadata.etag = it->second.etag;
        return metadata;
    }

    DB::ObjectMetadata getObjectMetadata(const std::string & path, bool with_tags) const override
    {
        auto metadata = tryGetObjectMetadata(path, with_tags);
        if (!metadata)
            throw DB::S3Exception("RecordingS3ObjectStorage: object does not exist", Aws::S3::S3Errors::RESOURCE_NOT_FOUND);
        return *metadata;
    }

    std::optional<DB::ObjectMetadata> tryGetObjectMetadataWithNativeToken(const std::string & path, bool with_tags) const override
    {
        return tryGetObjectMetadata(path, with_tags);
    }

    std::optional<DB::ObjectMetadata> tryGetObjectMetadataWithNativeToken(
        const std::string & path, bool with_tags, const DB::ObjectStorageControlRequest & request) const override
    {
        recordSelection(request);
        return tryGetObjectMetadata(path, with_tags);
    }

    bool exists(const DB::StoredObject & object) const override
    {
        std::lock_guard lock(mutex);
        return objects.contains(object.remote_path);
    }

    void removeObjectIfExists(const DB::StoredObject & object) override
    {
        std::lock_guard lock(mutex);
        objects.erase(object.remote_path);
    }

    void removeObjectsIfExist(const DB::StoredObjects & objects_to_remove) override
    {
        std::lock_guard lock(mutex);
        for (const auto & object : objects_to_remove)
            objects.erase(object.remote_path);
    }

    DB::ConditionalRemoveResult removeObjectIfTokenMatches(const DB::StoredObject & object, const std::string & etag) override
    {
        std::lock_guard lock(mutex);
        DB::ConditionalRemoveResult result;
        auto it = objects.find(object.remote_path);
        if (it == objects.end())
        {
            result.outcome = DB::ConditionalRemoveOutcome::NotFound;
            return result;
        }
        if (it->second.etag != etag)
        {
            result.outcome = DB::ConditionalRemoveOutcome::TokenMismatch;
            return result;
        }
        objects.erase(it);
        result.outcome = DB::ConditionalRemoveOutcome::Removed;
        return result;
    }

    DB::ConditionalRemoveResult removeObjectIfTokenMatches(
        const DB::StoredObject & object, const std::string & etag, const DB::ObjectStorageControlRequest & request) override
    {
        recordSelection(request);
        return removeObjectIfTokenMatches(object, etag);
    }

    /// `path` here is a bare CAS-relative prefix (e.g. `"pool/"`); a plain string-prefix scan over the
    /// in-memory keys already IS that key space.
    void listObjects(const std::string & path, DB::RelativePathsWithMetadata & children, size_t max_keys) const override
    {
        std::lock_guard lock(mutex);
        for (const auto & [key, entry] : objects)
        {
            if (!key.starts_with(path))
                continue;
            DB::ObjectMetadata metadata;
            metadata.size_bytes = entry.bytes.size();
            metadata.etag = entry.etag;
            children.push_back(std::make_shared<DB::RelativePathWithMetadata>(key, std::move(metadata)));
            if (max_keys != 0 && children.size() >= max_keys)
                break;
        }
    }

    DB::ObjectStorageIteratorPtr iterate(
        const std::string & path_prefix, size_t max_keys, bool with_tags,
        const std::optional<std::string> & start_after) const override
    {
        return DB::IObjectStorage::iterate(path_prefix, max_keys, with_tags, start_after);
    }

    DB::ObjectStorageIteratorPtr iterate(
        const std::string & path_prefix, size_t max_keys, bool with_tags,
        const std::optional<std::string> & start_after, const DB::ObjectStorageControlRequest & request) const override
    {
        recordSelection(request);
        return DB::IObjectStorage::iterate(path_prefix, max_keys, with_tags, start_after);
    }

    /// Checks the write-once/exact-token precondition against the current etag and, on success, stores
    /// `bytes` and mints the next etag -- the single place a conditional write's outcome is decided,
    /// exactly like a real store deciding it at PUT completion rather than at request send.
    std::string commitConditionalWrite(const std::string & key, const std::string & bytes,
                                       const std::string & if_none_match, const std::string & if_match)
    {
        std::lock_guard lock(mutex);
        auto it = objects.find(key);
        const bool exists_now = it != objects.end();
        if (!if_none_match.empty() && exists_now)
            throw DB::S3Exception("RecordingS3ObjectStorage: if-none-match precondition failed",
                                   Aws::S3::S3Errors::UNKNOWN, "PreconditionFailed");
        if (!if_match.empty() && (!exists_now || it->second.etag != if_match))
            throw DB::S3Exception("RecordingS3ObjectStorage: if-match precondition failed",
                                   Aws::S3::S3Errors::UNKNOWN, "PreconditionFailed");
        const std::string etag = std::to_string(next_etag++);
        objects[key] = Entry{bytes, etag};
        return etag;
    }

private:
    struct Entry
    {
        std::string bytes;
        std::string etag;
    };

    mutable std::mutex mutex;
    std::map<std::string, Entry> objects;
    uint64_t next_etag = 1;
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

/// Builds `S3Settings` from a `<disk>...</disk>` XML fragment exactly as a live disk's config section
/// would be loaded, so a test exercises the real `changed`-flag precedence rather than a hand-rolled one.
std::unique_ptr<DB::S3Settings> settingsFromXml(const std::string & disk_xml, const DB::Settings & global_settings = DB::Settings{})
{
    std::istringstream xml_stream("<clickhouse>" + disk_xml + "</clickhouse>"); // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config(new Poco::Util::XMLConfiguration(xml_stream));
    auto settings = std::make_unique<DB::S3Settings>();
    settings->loadFromConfigForObjectStorage(*config, "disk", global_settings, "http", /*validate_settings=*/false);
    return settings;
}

/// A `<disk>` config section deliberately missing `setting_name`, otherwise carrying the credentials a
/// reload needs so it never falls through to the (unreachable) EC2 instance metadata provider.
Poco::AutoPtr<Poco::Util::XMLConfiguration> configWithout(const std::string & /* setting_name */)
{
    std::istringstream xml_stream( // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        "<clickhouse><disk>"
        "<access_key_id>ACCESS_KEY_ID</access_key_id>"
        "<secret_access_key>SECRET_ACCESS_KEY</secret_access_key>"
        "</disk></clickhouse>");
    return new Poco::Util::XMLConfiguration(xml_stream);
}

/// A `<disk>` config section carrying explicit `http_keep_alive_timeout`/`http_keep_alive_max_requests`
/// values, for driving a reload that must win over the client profile.
Poco::AutoPtr<Poco::Util::XMLConfiguration> configWithExplicitKeepAlive(uint64_t timeout, uint64_t max_requests)
{
    std::istringstream xml_stream( // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        "<clickhouse><disk>"
        "<http_keep_alive_timeout>" + std::to_string(timeout) + "</http_keep_alive_timeout>"
        "<http_keep_alive_max_requests>" + std::to_string(max_requests) + "</http_keep_alive_max_requests>"
        "<access_key_id>ACCESS_KEY_ID</access_key_id>"
        "<secret_access_key>SECRET_ACCESS_KEY</secret_access_key>"
        "</disk></clickhouse>");
    return new Poco::Util::XMLConfiguration(xml_stream);
}

DB::ContextPtr contextForTest()
{
    return getContext().context;
}

/// A `<disk>inner</disk>` config wrapped in `<clickhouse>`, matching the shape `casClientProfileHintFor`
/// reads at `config_prefix = "disk"`.
Poco::AutoPtr<Poco::Util::XMLConfiguration> makeConfig(const std::string & inner)
{
    std::istringstream xml_stream("<clickhouse><disk>" + inner + "</disk></clickhouse>"); // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    return new Poco::Util::XMLConfiguration(xml_stream);
}

/// A storage carrying `profile` as its client profile but whose initial client was built directly
/// (bypassing the factory's `getClient`), so the defaults are known to come from `applyNewSettings`
/// reapplying the profile, not from anything baked in at construction.
std::shared_ptr<DB::S3ObjectStorage> storageWithProfile(const DB::S3ClientProfile & profile)
{
    auto client = DB::S3::ClientFactory::instance().create(
        clientConfigurationForTest(1000), clientSettingsForTest(),
        "ACCESS_KEY_ID", "SECRET_ACCESS_KEY", "", {}, {}, DB::S3::CredentialsConfiguration{});
    return std::make_shared<DB::S3ObjectStorage>(
        std::move(client), std::make_unique<DB::S3Settings>(),
        DB::S3::URI("http://127.0.0.1:1/bucket/"), DB::S3Capabilities{},
        DB::ObjectStorageKeyGeneratorPtr{}, "disk", /*for_disk_s3=*/true,
        /*credentials_refresh_callback=*/[] -> std::unique_ptr<const DB::S3::Client> { return nullptr; },
        std::optional<DB::S3ClientProfile>(profile));
}

}

namespace DB::S3AuthSetting
{
    extern const S3AuthSettingsUInt64 http_keep_alive_timeout;
    extern const S3AuthSettingsUInt64 http_keep_alive_max_requests;
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

    /// The reload path replaces the base client with a wider connect timeout, through the real
    /// `applyNewSettings` config-reload path (as `SYSTEM RELOAD CONFIG` would drive it); a clone
    /// rebuilt for the frozen cap 1000 stays at 1000.
    auto reloaded = makeStorageForTest(1000);
    (void)reloaded->getSingleAttemptClient(5000, 1000);
    reloaded->applyNewSettings(*configWithConnectTimeout(5000), "disk", contextForTest(),
                               DB::IObjectStorage::ApplyNewSettingsOptions{.allow_client_change = true});
    EXPECT_EQ(reloaded->getSingleAttemptClient(5000, 1000)->getClientConfiguration().connectTimeoutMs, 1000);
}

/// The snapshot half (the freeze computation `openPoolView` uses to build
/// `pool_config.cas_request_budget.connect_timeout_cap_ms`), the verb-propagation half (every
/// profile-aware verb's clone carries the SAME frozen pair, through the `ObjectStorageControlRequest`
/// context `ObjectStorageBackend::controlRequest` builds, over a directly-built `ObjectStorageBackend`),
/// and the full-chain half (a real `ContentAddressedMetadataStorage`, its writable-mount bootstrap
/// included, over the SAME in-memory store: `openPoolView` -> `pool_config` -> backend -> context ->
/// clone, with no link faked). Every profile-aware call records the actual client
/// `RecordingS3ObjectStorage::recordSelection` selected for it, reproducing the production selector's
/// dispatch over its public surface -- `RecordingS3ObjectStorage` answers from an in-memory
/// conditional store, but the cap and attempt number it observes came from the real CAS backend.
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

    /// Assert every recorded selection chose the frozen cap; `profile` is genuinely consulted by the
    /// production selector (a context that emitted `Default` would return the shared client, never a
    /// capped clone, so a wrong profile would show up here as `connectTimeoutMs` at the disk's own
    /// value instead of the cap).
    auto expectAllSelectedCap = [](const RecordingS3ObjectStorage & recorder, uint64_t expected_cap, size_t expected_count)
    {
        ASSERT_EQ(recorder.selections.size(), expected_count);
        for (const auto & s : recorder.selections)
        {
            EXPECT_EQ(s.profile, DB::ObjectStorageRetryProfile::SingleAttempt);
            EXPECT_EQ(s.connect_timeout_cap_ms, expected_cap);
            EXPECT_GE(s.attempt_number, 1u);
            EXPECT_EQ(s.selected_connect_timeout_ms, static_cast<long>(expected_cap));
        }
    };

    /// Verb-propagation half: a Native backend built with `attempt_timeout_ms=5000`,
    /// `connect_timeout_cap_ms=1000` (the frozen cap from above). One control read (HEAD) and one
    /// conditional write (PUT) must both select a clone with `connectTimeoutMs == 1000` -- and stay at
    /// 1000 after the underlying client is reloaded with a wider (5000), then a zero (unbounded),
    /// connect timeout: the backend's OWN `connect_timeout_cap_ms` was frozen at construction and a
    /// later reload cannot widen it.
    auto recording_storage = makeRecordingStorageForTest(1000);
    auto backend = std::make_shared<DB::Cas::ObjectStorageBackend>(
        recording_storage, DB::Cas::ObjectStorageBackend::Mode::Native,
        /*single_attempt_control_plane_=*/true, /*attempt_timeout_ms_=*/5000, /*connect_timeout_cap_ms_=*/1000);
    DB::Cas::CasRequests requests(backend, DB::Cas::Fence::open());
    auto op = requests.admit();

    ASSERT_FALSE(op.head("k", DB::Cas::Retry::once()).has_value());
    ASSERT_TRUE(std::holds_alternative<DB::Cas::Committed>(op.create("k", "v", DB::Cas::Retry::once())));
    expectAllSelectedCap(*recording_storage, /*cap=*/1000, /*expected_count=*/2);

    /// Widen the disk's own connect timeout, through the real config-reload path: the frozen cap must
    /// still win.
    recording_storage->selections.clear();
    recording_storage->applyNewSettings(*configWithConnectTimeout(5000), "disk", contextForTest(),
                                        DB::IObjectStorage::ApplyNewSettingsOptions{.allow_client_change = true});
    ASSERT_TRUE(op.head("k", DB::Cas::Retry::once()).has_value());
    ASSERT_TRUE(std::holds_alternative<DB::Cas::Committed>(op.create("k2", "v", DB::Cas::Retry::once())));
    expectAllSelectedCap(*recording_storage, /*cap=*/1000, /*expected_count=*/2);

    /// Zero the disk's own connect timeout (Poco "unbounded"), again via a real reload: still the
    /// frozen cap, never "no limit".
    recording_storage->selections.clear();
    recording_storage->applyNewSettings(*configWithConnectTimeout(0), "disk", contextForTest(),
                                        DB::IObjectStorage::ApplyNewSettingsOptions{.allow_client_change = true});
    ASSERT_TRUE(op.head("k", DB::Cas::Retry::once()).has_value());
    ASSERT_TRUE(std::holds_alternative<DB::Cas::Committed>(op.create("k3", "v", DB::Cas::Retry::once())));
    expectAllSelectedCap(*recording_storage, /*cap=*/1000, /*expected_count=*/2);

    /// A second storage whose base connect timeout is 0 (unbounded) at freeze time: the cap normalizes
    /// to the attempt timeout itself (5000, from the snapshot half above), and every verb selects a
    /// clone at that cap -- never at "no limit".
    auto recording_storage2 = makeRecordingStorageForTest(0);
    auto backend2 = std::make_shared<DB::Cas::ObjectStorageBackend>(
        recording_storage2, DB::Cas::ObjectStorageBackend::Mode::Native,
        /*single_attempt_control_plane_=*/true, /*attempt_timeout_ms_=*/5000, /*connect_timeout_cap_ms_=*/5000);
    DB::Cas::CasRequests requests2(backend2, DB::Cas::Fence::open());
    auto op2 = requests2.admit();
    ASSERT_FALSE(op2.head("k", DB::Cas::Retry::once()).has_value());
    ASSERT_TRUE(std::holds_alternative<DB::Cas::Committed>(op2.create("k", "v", DB::Cas::Retry::once())));
    expectAllSelectedCap(*recording_storage2, /*cap=*/5000, /*expected_count=*/2);

    /// Full-chain half: a real `ContentAddressedMetadataStorage` drives `Pool::open`'s writable-mount
    /// bootstrap (the mandatory capability battery included) entirely against a fresh in-memory store,
    /// proving `openPoolView` actually built `pool_config.cas_request_budget.connect_timeout_cap_ms`
    /// from THIS client and threaded it into the backend that answers every later request -- the two
    /// links `freezeConnectTimeoutCapMs` alone (above) cannot exercise. `existsFile` is the "control
    /// read" (a HEAD, per `CasPlainObjects::mountpointObjectExists`'s own doc comment); a transaction
    /// `writeFile` of the same loose path is the "conditional write" (`casPutObject`).
    auto chain_storage = makeRecordingStorageForTest(1000);
    auto chain_settings = DB::Cas::tests::makeSettingsForTest(
        "chainMount", std::filesystem::temp_directory_path() / fmt::format("cas_s3_client_profile_chain_{}", ::getpid()));
    auto metadata_storage = std::make_shared<DB::ContentAddressedMetadataStorage>(
        chain_storage, "pool", "chainMount", /*disk_name_=*/"", /*context_=*/nullptr, chain_settings);
    metadata_storage->startup();

    const std::string probe_path = "chain_probe_file.txt";
    chain_storage->selections.clear();
    EXPECT_FALSE(metadata_storage->existsFile(probe_path));
    {
        auto tx = metadata_storage->createTransaction();
        auto & ca_tx = dynamic_cast<DB::ContentAddressedTransaction &>(*tx);
        auto buf = ca_tx.writeFile(probe_path, 65536, DB::WriteMode::Rewrite, {});
        buf->write("hello", 5);
        buf->finalize();
    }
    ASSERT_FALSE(chain_storage->selections.empty());
    for (const auto & s : chain_storage->selections)
        EXPECT_EQ(s.selected_connect_timeout_ms, 1000);

    /// Reload the disk's own client wider, then to zero, through the REAL config-reload path (as a live
    /// disk's `SYSTEM RELOAD CONFIG` would): the mount's frozen cap must still win --
    /// `ObjectStorageBackend::connect_timeout_cap_ms` was captured once at `openPoolView` time, never
    /// re-read from the reloaded client.
    chain_storage->applyNewSettings(*configWithConnectTimeout(5000), "disk", getContext().context,
                                    DB::IObjectStorage::ApplyNewSettingsOptions{.allow_client_change = true});
    chain_storage->selections.clear();
    EXPECT_TRUE(metadata_storage->existsFile(probe_path));
    ASSERT_FALSE(chain_storage->selections.empty());
    for (const auto & s : chain_storage->selections)
        EXPECT_EQ(s.selected_connect_timeout_ms, 1000);

    chain_storage->applyNewSettings(*configWithConnectTimeout(0), "disk", getContext().context,
                                    DB::IObjectStorage::ApplyNewSettingsOptions{.allow_client_change = true});
    chain_storage->selections.clear();
    EXPECT_TRUE(metadata_storage->existsFile(probe_path));
    ASSERT_FALSE(chain_storage->selections.empty());
    for (const auto & s : chain_storage->selections)
        EXPECT_EQ(s.selected_connect_timeout_ms, 1000);
}

/// The CAS client profile's values are defaults, never overrides: an explicit disk-section value or a
/// changed global `s3_http_keep_alive_*` setting keeps precedence over the profile, exactly because the
/// loader already marked those fields `changed` before the profile is applied. Exercises the real
/// `{30, 10000}` profile and checks BOTH fields in every case, so a wrong or lost
/// `http_keep_alive_max_requests` cannot pass silently.
TEST(S3ObjectStorageProfile, CasDefaultsApplyOnlyWhenUnset)
{
    const DB::S3ClientProfile profile = DB::S3ObjectStorage::casClientProfile();
    ASSERT_EQ(profile.http_keep_alive_timeout, 30u);
    ASSERT_EQ(profile.http_keep_alive_max_requests, 10000u);
    {
        /// Defaults applied: neither setting given anywhere.
        auto settings = settingsFromXml("<disk><endpoint>http://127.0.0.1:1/b/</endpoint></disk>");
        DB::S3ObjectStorage::applyClientProfileDefaults(profile, *settings);
        EXPECT_EQ(settings->auth_settings[DB::S3AuthSetting::http_keep_alive_timeout].value, 30u);
        EXPECT_EQ(settings->auth_settings[DB::S3AuthSetting::http_keep_alive_max_requests].value, 10000u);
    }
    {
        /// Explicit disk value wins, for both settings at once.
        auto settings = settingsFromXml(
            "<disk><endpoint>http://127.0.0.1:1/b/</endpoint>"
            "<http_keep_alive_timeout>7</http_keep_alive_timeout>"
            "<http_keep_alive_max_requests>55</http_keep_alive_max_requests></disk>");
        DB::S3ObjectStorage::applyClientProfileDefaults(profile, *settings);
        EXPECT_EQ(settings->auth_settings[DB::S3AuthSetting::http_keep_alive_timeout].value, 7u);
        EXPECT_EQ(settings->auth_settings[DB::S3AuthSetting::http_keep_alive_max_requests].value, 55u);
    }
    {
        /// A changed global `s3_http_keep_alive_*` setting counts as explicit: the loader marks it
        /// changed, for both settings.
        DB::Settings global;
        global.set("s3_http_keep_alive_timeout", 11);
        global.set("s3_http_keep_alive_max_requests", 222);
        auto settings = settingsFromXml("<disk><endpoint>http://127.0.0.1:1/b/</endpoint></disk>", global);
        DB::S3ObjectStorage::applyClientProfileDefaults(profile, *settings);
        EXPECT_EQ(settings->auth_settings[DB::S3AuthSetting::http_keep_alive_timeout].value, 11u);
        EXPECT_EQ(settings->auth_settings[DB::S3AuthSetting::http_keep_alive_max_requests].value, 222u);
    }
}

/// `storageWithProfile` builds a storage whose settings have NOT had the profile applied yet, so the
/// first `applyNewSettings` call below is what actually applies it -- unlike the disk the factory
/// builds, where the profile is already applied before construction and simply survives every later
/// reload via the sticky `changed` flag in the settings copy. Once applied here, the same sticky flag
/// takes over: the profile keeps showing up across a later `SYSTEM RELOAD CONFIG` that touches
/// neither setting, and an explicit value given on a LATER reload still takes precedence over it.
/// Checks both `http_keep_alive_timeout` and `http_keep_alive_max_requests` at each stage.
TEST(S3ObjectStorageProfile, ApplyNewSettingsPreservesTheProfile)
{
    auto storage = storageWithProfile(DB::S3ObjectStorage::casClientProfile());

    /// Profile preserved on reload: the new config touches neither setting.
    storage->applyNewSettings(*configWithout("http_keep_alive_timeout"), "disk", contextForTest(),
                              DB::IObjectStorage::ApplyNewSettingsOptions{.allow_client_change = true});
    EXPECT_EQ(storage->getS3StorageClient()->getClientConfiguration().http_keep_alive_timeout, 30u);
    EXPECT_EQ(storage->getS3StorageClient()->getClientConfiguration().http_keep_alive_max_requests, 10000u);

    /// Explicit value wins on reload: a later reload with explicit values overrides the profile that
    /// was already in effect.
    storage->applyNewSettings(*configWithExplicitKeepAlive(7, 55), "disk", contextForTest(),
                              DB::IObjectStorage::ApplyNewSettingsOptions{.allow_client_change = true});
    EXPECT_EQ(storage->getS3StorageClient()->getClientConfiguration().http_keep_alive_timeout, 7u);
    EXPECT_EQ(storage->getS3StorageClient()->getClientConfiguration().http_keep_alive_max_requests, 55u);
}

/// `casClientProfileHintFor` (the one-line rule `registerDiskObjectStorage` uses to decide whether a
/// disk's S3 client gets the CAS keep-alive defaults) tested directly against the config shapes that
/// matter: a plain `metadata_type`, and -- the regression this pins -- a disk using the `<locations>`
/// form, where `metadata_type` lives on the disk's OWN `config_prefix` and is never visible from a
/// nested location's prefix. Deriving the hint per-location instead of once at the disk level would
/// silently produce `cas_client_profile = false` for every location of such a disk.
TEST(S3ObjectStorageProfile, CasClientProfileHintForMetadataType)
{
    EXPECT_TRUE(DB::casClientProfileHintFor(*makeConfig("<metadata_type>cas</metadata_type>"), "disk"));
    EXPECT_FALSE(DB::casClientProfileHintFor(*makeConfig("<metadata_type>local</metadata_type>"), "disk"));

    auto cfg = makeConfig(
        "<metadata_type>cas</metadata_type>"
        "<locations><main>"
        "<type>s3</type><local>true</local><enabled>true</enabled>"
        "<endpoint>http://127.0.0.1:1/bucket/</endpoint>"
        "<access_key_id>a</access_key_id><secret_access_key>b</secret_access_key>"
        "</main></locations>");
    EXPECT_TRUE(DB::casClientProfileHintFor(*cfg, "disk"));
    EXPECT_FALSE(DB::casClientProfileHintFor(*cfg, "disk.locations.main"));
}

/// Whether an `S3ObjectStorage` carries a client profile decides whether its constructed client gets
/// the CAS keep-alive defaults -- checked both ways, directly on the storage the factory's S3 creator
/// would build in each case (with a profile when the disk's `metadata_type` is `cas`, without one
/// otherwise), rather than through the factory itself.
TEST(S3ObjectStorageProfile, ClientProfileAppliesOnlyWhenGiven)
{
    auto with_profile = storageWithProfile(DB::S3ObjectStorage::casClientProfile());
    with_profile->applyNewSettings(*configWithout("http_keep_alive_timeout"), "disk", contextForTest(),
                                   DB::IObjectStorage::ApplyNewSettingsOptions{.allow_client_change = true});
    EXPECT_EQ(with_profile->getS3StorageClient()->getClientConfiguration().http_keep_alive_timeout, 30u);
    EXPECT_EQ(with_profile->getS3StorageClient()->getClientConfiguration().http_keep_alive_max_requests, 10000u);

    auto without_profile = makeStorageForTest(1000);
    without_profile->applyNewSettings(*configWithout("http_keep_alive_timeout"), "disk", contextForTest(),
                                      DB::IObjectStorage::ApplyNewSettingsOptions{.allow_client_change = true});
    EXPECT_EQ(without_profile->getS3StorageClient()->getClientConfiguration().http_keep_alive_timeout,
              DB::S3::DEFAULT_KEEP_ALIVE_TIMEOUT);
    EXPECT_EQ(without_profile->getS3StorageClient()->getClientConfiguration().http_keep_alive_max_requests,
              DB::S3::DEFAULT_KEEP_ALIVE_MAX_REQUESTS);
}

#endif
