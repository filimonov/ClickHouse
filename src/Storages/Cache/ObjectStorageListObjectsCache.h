#pragma once

#include <chrono>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Common/CacheBase.h>

namespace DB
{

class ObjectStorageListObjectsCache
{
public:
    static ObjectStorageListObjectsCache & instance();

    struct Key
    {
        Key(
            const String & bucket_,
            const String & prefix_,
            const std::chrono::steady_clock::time_point & expires_at_ = std::chrono::steady_clock::now(),
            std::optional<UUID> user_id_ = std::nullopt);

        std::string bucket;
        std::string prefix;
        std::chrono::steady_clock::time_point expires_at;
        std::optional<UUID> user_id;

        bool operator==(const Key & other) const;
    };

    using Value = StorageObjectStorage::ObjectInfos;
    struct KeyHasher
    {
        size_t operator()(const Key & key) const;
    };

    struct IsStale
    {
        bool operator()(const Key & key) const;
    };

    struct WeightFunction
    {
        size_t operator()(const Value & value) const;
    };

    using Cache = CacheBase<Key, Value, KeyHasher, WeightFunction>;

    void set(
        const std::string & bucket,
        const std::string & prefix,
        const std::shared_ptr<Value> & value);

    Cache::MappedPtr get(const String & bucket, const String & prefix, bool filter_by_prefix = true);

    void clear();

    void setMaxSizeInBytes(std::size_t size_in_bytes_);
    void setMaxCount(std::size_t count);
    void setTTL(std::size_t ttl_in_seconds_);

private:
    ObjectStorageListObjectsCache();

    Cache cache;
    size_t ttl_in_seconds {0};
};

}
