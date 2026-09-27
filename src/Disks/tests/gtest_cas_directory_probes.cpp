#include <gtest/gtest.h>
#include "cas_test_helpers.h"

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
#include <IO/WriteHelpers.h>
#include <Core/Defines.h>

#include <fmt/ranges.h>

#include <algorithm>
#include <filesystem>
#include <mutex>
#include <string>
#include <vector>

namespace DB::ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
}

/// Per-TU declarations of the settings this file overrides, the pattern `cas_test_helpers.h`
/// documents: defined once in `ContentAddressedSettings.cpp`, declared by each consumer.
namespace DB::ContentAddressedSetting
{
    extern const ContentAddressedSettingsBool gc_enabled;
    extern const ContentAddressedSettingsUInt64 part_folder_cache_bytes;
    extern const ContentAddressedSettingsUInt64 manifest_decode_cache_bytes;
}

/// Directory probes on a path INSIDE a part (`<table>/<part>/<file>`) must be answered from the
/// part-folder view, never by a LIST of the table's `_files/` prefix. The metadata storage builds its
/// own backend from an `ObjectStoragePtr`, so the instrument sits at the `IObjectStorage` layer: a
/// `LocalObjectStorage` that counts every operation it is asked, by kind and key. Every method
/// `CasObjectStorageBackend` reaches is overridden, so "zero LISTs" is satisfied by the behaviour, not
/// by an unrecorded path.

using namespace DB::Cas::tests;

namespace
{

class CountingObjectStorage : public DB::LocalObjectStorage
{
public:
    using DB::LocalObjectStorage::LocalObjectStorage;

    enum class Kind { List, Get, Head, Put, Delete };

    bool exists(const DB::StoredObject & object) const override
    {
        record(Kind::Head, object.remote_path);
        return DB::LocalObjectStorage::exists(object);
    }

    std::unique_ptr<DB::ReadBufferFromFileBase> readObject(
        const DB::StoredObject & object, const DB::ReadSettings & read_settings,
        std::optional<size_t> read_hint, bool use_external_buffer,
        bool restrict_seek) const override
    {
        record(Kind::Get, object.remote_path);
        failIfArmed(object.remote_path);
        return DB::LocalObjectStorage::readObject(object, read_settings, read_hint, use_external_buffer, restrict_seek);
    }

    std::unique_ptr<DB::WriteBufferFromFileBase> writeObject(
        const DB::StoredObject & object, DB::WriteMode mode,
        std::optional<DB::ObjectAttributes> attributes,
        size_t buf_size,
        const DB::WriteSettings & write_settings) override
    {
        record(Kind::Put, object.remote_path);
        return DB::LocalObjectStorage::writeObject(object, mode, attributes, buf_size, write_settings);
    }

    void removeObjectIfExists(const DB::StoredObject & object) override
    {
        record(Kind::Delete, object.remote_path);
        DB::LocalObjectStorage::removeObjectIfExists(object);
    }

    void removeObjectsIfExist(const DB::StoredObjects & objects) override
    {
        for (const DB::StoredObject & object : objects)
            record(Kind::Delete, object.remote_path);
        DB::LocalObjectStorage::removeObjectsIfExist(objects);
    }

    DB::ObjectMetadata getObjectMetadata(const std::string & path, bool with_tags) const override
    {
        record(Kind::Head, path);
        return DB::LocalObjectStorage::getObjectMetadata(path, with_tags);
    }

    std::optional<DB::ObjectMetadata> tryGetObjectMetadata(const std::string & path, bool with_tags) const override
    {
        record(Kind::Head, path);
        return DB::LocalObjectStorage::tryGetObjectMetadata(path, with_tags);
    }

    void listObjects(const std::string & path, DB::RelativePathsWithMetadata & children, size_t max_keys) const override
    {
        record(Kind::List, path);
        DB::LocalObjectStorage::listObjects(path, children, max_keys);
    }

    bool existsOrHasAnyChild(const std::string & path) const override
    {
        record(Kind::List, path);
        return DB::LocalObjectStorage::existsOrHasAnyChild(path);
    }

    void copyObject(
        const DB::StoredObject & object_from, const DB::StoredObject & object_to,
        const DB::ReadSettings & read_settings, const DB::WriteSettings & write_settings,
        std::optional<DB::ObjectAttributes> object_to_attributes) override
    {
        record(Kind::Get, object_from.remote_path);
        record(Kind::Put, object_to.remote_path);
        DB::LocalObjectStorage::copyObject(object_from, object_to, read_settings, write_settings, object_to_attributes);
    }

    size_t listCount(std::string_view prefix) const { return count(Kind::List, prefix); }
    size_t getCount(std::string_view prefix) const { return count(Kind::Get, prefix); }
    [[maybe_unused]] size_t headCount(std::string_view prefix) const { return count(Kind::Head, prefix); }

    /// Every recorded key of `kind` containing `needle`, so a failure names the offender.
    std::vector<String> keys(Kind kind, std::string_view needle) const
    {
        std::lock_guard lock(mutex);
        std::vector<String> out;
        for (const auto & [k, key] : records)
            if (k == kind && key.find(needle) != String::npos)
                out.push_back(key);
        return out;
    }

    void reset()
    {
        std::lock_guard lock(mutex);
        records.clear();
    }

    /// Task 3: every GET of a key containing `needle` throws, so a resolved ref whose manifest
    /// cannot be read is an error, never a fall-through. Unused by this task's tests; the next
    /// task's tests in this file exercise it.
    [[maybe_unused]] void failReadsContaining(String needle)
    {
        std::lock_guard lock(mutex);
        fail_reads_containing = std::move(needle);
    }

private:
    void record(Kind kind, const std::string & key) const
    {
        std::lock_guard lock(mutex);
        records.emplace_back(kind, key);
    }

    void failIfArmed(const std::string & key) const
    {
        std::lock_guard lock(mutex);
        if (!fail_reads_containing.empty() && key.find(fail_reads_containing) != String::npos)
            throw DB::Exception(DB::ErrorCodes::CANNOT_READ_ALL_DATA, "CountingObjectStorage: injected read failure on '{}'", key);
    }

    size_t count(Kind kind, std::string_view prefix) const
    {
        std::lock_guard lock(mutex);
        size_t n = 0;
        for (const auto & [k, key] : records)
            if (k == kind && key.starts_with(prefix))
                ++n;
        return n;
    }

    mutable std::mutex mutex;
    mutable std::vector<std::pair<Kind, String>> records;
    String fail_reads_containing;
};

const std::string kTbl = "a11/a11a11a1-1111-4111-8111-111111111111";
const std::string kNonAtomicTbl = "data/db/tbl";

std::shared_ptr<DB::ContentAddressedMetadataStorage> openCountingStorage(
    std::shared_ptr<CountingObjectStorage> & out_object_storage, bool disable_caches)
{
    static std::atomic<uint64_t> counter{0};
    const String unique = std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1));
    const auto root = (std::filesystem::temp_directory_path() / ("cas_dir_probes_" + unique)).string();
    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    std::filesystem::create_directories(root, ec);

    out_object_storage = std::make_shared<CountingObjectStorage>(
        DB::LocalObjectStorageSettings("test", root, /*read_only_=*/false));

    auto settings = makeSettingsForTest(
        "test", std::filesystem::temp_directory_path() / ("cas_dir_probes_scratch_" + unique));
    /// A GC round LISTs on its own schedule; a timer is not a fence, so keep it off.
    settings[DB::ContentAddressedSetting::gc_enabled] = false;
    if (disable_caches)
    {
        settings[DB::ContentAddressedSetting::part_folder_cache_bytes] = 0;
        settings[DB::ContentAddressedSetting::manifest_decode_cache_bytes] = 0;
    }
    auto storage = std::make_shared<DB::ContentAddressedMetadataStorage>(
        out_object_storage, "pool", "srv1", "", nullptr, settings);
    storage->startup();
    return storage;
}

/// Publishes one part through the real transaction path: every (relative file, bytes) pair is written
/// and the transaction is committed, exactly as a MergeTree part write does.
void publishPart(DB::ContentAddressedMetadataStorage & storage, const std::string & part_path,
                 const std::vector<std::pair<std::string, std::string>> & files)
{
    auto tx = storage.createTransaction();
    auto & ca_tx = dynamic_cast<DB::ContentAddressedTransaction &>(*tx);
    for (const auto & [file, bytes] : files)
    {
        auto buf = ca_tx.writeFile(part_path + "/" + file, 65536, DB::WriteMode::Rewrite, {});
        buf->write(bytes.data(), bytes.size());
        buf->finalize();
    }
    tx->commit(DB::NoCommitOptions{});
}

std::string filesPrefixOf(DB::ContentAddressedMetadataStorage & storage, const std::string & table_path)
{
    /// The LIST that must not happen: the life's `_files/` prefix. Resolved through the storage so a
    /// layout change moves the assertion instead of silently matching nothing.
    const auto uuid = table_path.substr(table_path.find_last_of('/') + 1);
    const auto life = storage.readableNamespaceFilesLife(storage.liveNamespace(uuid));
    return life ? storage.store()->layout().namespaceFilesPrefix(*life) : "cas/ns/state/";
}

}

/// A file of a published part is not a directory; a non-projection nested directory is, lists its
/// children and is not empty; a projection directory keeps its deliberate empty answer. None of it
/// LISTs the table's `_files/` prefix, with or without the view caches.
class CASDirectoryProbes : public ::testing::TestWithParam<bool> {};

TEST_P(CASDirectoryProbes, PartFileAnswersFromTheViewWithoutAList)
{
    const bool disable_caches = GetParam();
    std::shared_ptr<CountingObjectStorage> os;
    auto storage = openCountingStorage(os, disable_caches);

    const std::string part = kTbl + "/all_1_1_0";
    publishPart(*storage, part, {
        {"columns.txt", "cols"}, {"data.bin", "data-bytes"},
        {"sub/inner.bin", "inner"}, {"p.proj/data.bin", "proj-bytes"}});
    /// The load has happened; from here on every probe must be answered from what it retained.
    ASSERT_TRUE(storage->existsDirectory(part));
    /// `existsDirectory(part)` above resolves the PartDir shape via `existsRef`, which never
    /// touches the part-folder view; prime the view/manifest-decode cache explicitly through a
    /// PartFile-shaped probe so the warm-cache assertions below measure the REPEAT probes, not the
    /// one-time cold decode a commit's cache invalidation leaves behind.
    ASSERT_FALSE(storage->existsDirectory(part + "/columns.txt"));
    const std::string files_prefix = filesPrefixOf(*storage, kTbl);
    os->reset();

    EXPECT_FALSE(storage->existsDirectory(part + "/columns.txt"));
    EXPECT_FALSE(storage->existsDirectory(part + "/data.bin"));
    EXPECT_TRUE(storage->existsDirectory(part + "/sub"));
    EXPECT_TRUE(storage->existsDirectory(part + "/sub/"));            /// Review Focus 1: trailing slash
    EXPECT_EQ(storage->listDirectory(part + "/sub"), (std::vector<std::string>{"inner.bin"}));
    EXPECT_FALSE(storage->isDirectoryEmpty(part + "/sub"));
    EXPECT_TRUE(storage->isDirectoryEmpty(part + "/p.proj"));          /// ProjectionDir keeps its answer
    EXPECT_TRUE(storage->existsDirectory(part + "/p.proj"));
    EXPECT_FALSE(storage->existsDirectory(part + "/absent"));

    /// A LOCAL-backed pool answers every emulated operation through `StoredObject(emu_root + "/" +
    /// key)` (`ObjectStorageBackend::emuPath`), so a recorded key carries the object storage's own
    /// key prefix ahead of the logical CAS key that `casManifestsPrefix`/the `_files` prefix name.
    const std::string emu_root = os->getCommonKeyPrefix() + "/";
    EXPECT_EQ(os->listCount(emu_root + files_prefix), 0u) << "keys: " << fmt::to_string(fmt::join(os->keys(CountingObjectStorage::Kind::List, "_files"), ", "));
    EXPECT_EQ(os->listCount(""), 0u) << "no LIST of any prefix for probes inside a resolved part";
    if (!disable_caches)
        EXPECT_EQ(os->getCount(""), 0u) << "warm view: no GET at all";
    else
        EXPECT_GT(os->getCount(emu_root + storage->store()->layout().casManifestsPrefix()), 0u) << "cold caches: the manifest is read, never listed";
}

INSTANTIATE_TEST_SUITE_P(Caches, CASDirectoryProbes, ::testing::Bool(),
    [](const ::testing::TestParamInfo<bool> & param_info) { return param_info.param ? "Disabled" : "Default"; });

/// Detached (Review Focus 4) and non-Atomic parts route through the same shape and the same view.
TEST(CASDirectoryProbes, DetachedAndNonAtomicPartFilesAnswerWithoutAList)
{
    std::shared_ptr<CountingObjectStorage> os;
    auto storage = openCountingStorage(os, /*disable_caches=*/false);

    const std::string part = kTbl + "/all_2_2_0";
    publishPart(*storage, part, {{"columns.txt", "cols"}, {"sub/x.bin", "x"}});
    {
        auto tx = storage->createTransaction();
        tx->moveDirectory(part, kTbl + "/detached/all_2_2_0");
        tx->commit(DB::NoCommitOptions{});
    }
    const std::string detached = kTbl + "/detached/all_2_2_0";
    ASSERT_TRUE(storage->existsDirectory(detached));

    const std::string na_part = kNonAtomicTbl + "/all_1_1_0";
    publishPart(*storage, na_part, {{"columns.txt", "cols"}, {"sub/y.bin", "y"}});
    ASSERT_TRUE(storage->existsDirectory(na_part));

    os->reset();
    EXPECT_FALSE(storage->existsDirectory(detached + "/columns.txt"));
    EXPECT_TRUE(storage->existsDirectory(detached + "/sub"));
    EXPECT_FALSE(storage->existsDirectory(na_part + "/columns.txt"));
    EXPECT_TRUE(storage->existsDirectory(na_part + "/sub"));
    EXPECT_EQ(os->listCount(""), 0u) << "keys: " << fmt::to_string(fmt::join(os->keys(CountingObjectStorage::Kind::List, ""), ", "));
}
