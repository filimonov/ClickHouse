#include <Storages/Cache/ObjectStorageListObjectsCache.h>
#include <Common/TTLCachePolicy.h>
#include <boost/functional/hash.hpp>

namespace DB
{

template <typename Key, typename Mapped, typename HashFunction, typename WeightFunction, typename IsStaleFunction>
class ObjectStorageListObjectsCachePolicy : public TTLCachePolicy<Key, Mapped, HashFunction, WeightFunction, IsStaleFunction>
{
public:
    using BasePolicy = TTLCachePolicy<Key, Mapped, HashFunction, WeightFunction, IsStaleFunction>;
    using typename BasePolicy::MappedPtr;
    using typename BasePolicy::KeyMapped;
    using BasePolicy::cache;

    ObjectStorageListObjectsCachePolicy()
        : BasePolicy(std::make_unique<NoCachePolicyUserQuota>())
    {
    }

    std::optional<KeyMapped> getWithKey(const Key & key) override
    {
        if (const auto it = cache.find(key); it != cache.end())
        {
            if (!IsStaleFunction()(it->first))
            {
                return std::make_optional<KeyMapped>({it->first, it->second});
            }
            // found a stale entry, remove it but don't return. We still want to perform the prefix matching search
            BasePolicy::remove(it->first);
        }

        if (const auto it = findBestMatchingPrefixAndRemoveExpiredEntries(key); it != cache.end())
        {
            return std::make_optional<KeyMapped>({it->first, it->second});
        }

        return std::nullopt;
    }

private:
    auto findBestMatchingPrefixAndRemoveExpiredEntries(const Key & key)
    {
        const auto & prefix = key.prefix;

        auto best_match = cache.end();
        size_t best_length = 0;

        std::vector<Key> to_remove;

        for (auto it = cache.begin(); it != cache.end(); ++it)
        {
            const auto & candidate_bucket = it->first.bucket;
            const auto & candidate_prefix = it->first.prefix;

            if (candidate_bucket == key.bucket && prefix.starts_with(candidate_prefix))
            {
                if (IsStaleFunction()(it->first))
                {
                    to_remove.push_back(it->first);
                    continue;
                }

                if (candidate_prefix.size() > best_length)
                {
                    best_match = it;
                    best_length = candidate_prefix.size();
                }
            }
        }

        for (const auto & k : to_remove)
            BasePolicy::remove(k);

        return best_match;
    }
};

ObjectStorageListObjectsCache::Key::Key(
    const String & bucket_,
    const String & prefix_,
    const std::chrono::steady_clock::time_point & expires_at_,
    std::optional<UUID> user_id_)
    : bucket(bucket_), prefix(prefix_), expires_at(expires_at_), user_id(user_id_) {}

bool ObjectStorageListObjectsCache::Key::operator==(const Key & other) const
{
    return bucket == other.bucket && prefix == other.prefix;
}

size_t ObjectStorageListObjectsCache::KeyHasher::operator()(const Key & key) const
{
    std::size_t seed = 0;

    boost::hash_combine(seed, key.bucket);
    boost::hash_combine(seed, key.prefix);

    return seed;
}

bool ObjectStorageListObjectsCache::IsStale::operator()(const Key & key) const
{
    return key.expires_at < std::chrono::steady_clock::now();
}

size_t ObjectStorageListObjectsCache::WeightFunction::operator()(const Value & value) const
{
    std::size_t weight = 0;

    for (const auto & object : value)
    {
        weight += object->relative_path.capacity() + sizeof(ObjectMetadata);
    }

    return weight;
}

ObjectStorageListObjectsCache::ObjectStorageListObjectsCache()
    : cache(std::make_unique<ObjectStorageListObjectsCachePolicy<Key, Value, KeyHasher, WeightFunction, IsStale>>())
{
}

void ObjectStorageListObjectsCache::set(
    const std::string & bucket,
    const std::string & prefix,
    const std::shared_ptr<Value> & value)
{
    const auto key = Key{bucket, prefix, std::chrono::steady_clock::now() + std::chrono::seconds(ttl)};

    cache.set(key, value);
}

ObjectStorageListObjectsCache::Cache::MappedPtr ObjectStorageListObjectsCache::get(const String & bucket, const String & prefix, bool filter_by_prefix)
{
    const auto input_key = Key{bucket, prefix};
    auto pair = cache.getWithKey(input_key);

    if (!pair)
    {
        return {};
    }

    if (pair->key == input_key || !filter_by_prefix)
    {
        return pair->mapped;
    }

    auto filtered_objects = std::make_shared<std::vector<ObjectInfoPtr>>();
    filtered_objects->reserve(pair->mapped->size());

    for (const auto & object : *pair->mapped)
    {
        if (object->relative_path.starts_with(input_key.prefix))
        {
            filtered_objects->push_back(object);
        }
    }

    return filtered_objects;
}

void ObjectStorageListObjectsCache::setMaxSizeInBytes(std::size_t size_in_bytes_)
{
    cache.setMaxSizeInBytes(size_in_bytes_);
}

void ObjectStorageListObjectsCache::setMaxCount(std::size_t count)
{
    cache.setMaxCount(count);
}

void ObjectStorageListObjectsCache::setTTL(std::size_t ttl_)
{
    ttl = ttl_;
}

ObjectStorageListObjectsCache & ObjectStorageListObjectsCache::instance()
{
    static ObjectStorageListObjectsCache instance;
    return instance;
}

}
