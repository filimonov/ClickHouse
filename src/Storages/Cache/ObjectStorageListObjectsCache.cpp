#include <Storages/Cache/ObjectStorageListObjectsCache.h>

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

    MappedPtr get(const Key & key) override
    {
        if (const auto it = cache.find(key); it != cache.end())
        {
            if (IsStaleFunction()(it->first))
            {
                BasePolicy::remove(it->first);
                return {};
            }
            return it->second;
        }

        if (const auto it = findBestMatchingPrefix(key); it != cache.end())
        {
            if (IsStaleFunction()(it->first))
            {
                BasePolicy::remove(it->first);
                return {};
            }
            return it->second;
        }

        return {};
    }

    ///
     /// select * from s3(http://aws.s3.us-east-1/table_root/year=*/*.parquet)
     /// select * from s3(http://aws.s3.us-east-1/table_root/year={01..3}/*.parquet) -> lista de arquivos cacheada
     // year=4
    /// select * from s3(http://aws.s3.us-east-1/table_root/year=201*/*.parquet) -> lista de arquivos cacheada
    /// select * from s3(http://aws.s3.us-east-1/table_root/year=2011/*.parquet) ->
     //

    std::optional<KeyMapped> getWithKey(const Key & key) override
    {
        if (const auto it = cache.find(key); it != cache.end())
        {
            if (IsStaleFunction()(it->first))
            {
                BasePolicy::remove(it->first);
                return std::nullopt;
            }
            return std::make_optional<KeyMapped>({it->first, it->second});
        }

        if (const auto it = findBestMatchingPrefix(key); it != cache.end())
        {
            if (IsStaleFunction()(it->first))
            {
                BasePolicy::remove(it->first);
                return std::nullopt;
            }
            return std::make_optional<KeyMapped>({it->first, it->second});
        }

        return std::nullopt;
    }

    bool contains(const Key & key) const override
    {
        if (cache.contains(key))
        {
            return true;
        }

        return findAnyMatchingPrefix(key) != cache.end();
    }

private:
    auto findBestMatchingPrefix(const Key & key)
    {
        const auto & prefix = key.prefix;

        auto best_match = cache.end();
        size_t best_length = 0;

        for (auto it = cache.begin(); it != cache.end(); ++it)
        {
            const auto & candidate_prefix = it->first.prefix;

            if (prefix.compare(0, candidate_prefix.size(), candidate_prefix) == 0)
            {
                if (candidate_prefix.size() > best_length)
                {
                    best_match = it;
                    best_length = candidate_prefix.size();
                }
            }
        }

        return best_match;
    }

    auto findAnyMatchingPrefix(const Key & key) const
    {
        const auto & prefix = key.prefix;
        return std::find_if(cache.begin(), cache.end(), [&](const auto & it)
        {
            const auto & candidate_prefix = it.first.prefix;
            if (prefix.compare(0, candidate_prefix.size(), candidate_prefix) == 0)
            {
                return true;
            }

            return false;
        });
    }
};

ObjectStorageListObjectsCache::Key::Key(
    const String & bucket_,
    const String & prefix_,
    const std::chrono::system_clock::time_point & expires_at_,
    std::optional<UUID> user_id_)
    : bucket(bucket_), prefix(prefix_), expires_at(expires_at_), user_id(user_id_) {}

bool ObjectStorageListObjectsCache::Key::operator==(const Key & other) const
{
    return bucket == other.bucket && prefix == other.prefix;
}

size_t ObjectStorageListObjectsCache::KeyHasher::operator()(const Key & key) const
{
    return std::hash<String>()(key.prefix) + std::hash<String>()(key.bucket);
}

bool ObjectStorageListObjectsCache::IsStale::operator()(const Key & key) const
{
    return key.expires_at < std::chrono::system_clock::now();
}

size_t ObjectStorageListObjectsCache::WeightFunction::operator()(const Value & value) const
{
    std::size_t weight = 0;

    for (const auto & object : value)
    {
        weight += object->relative_path.size() + sizeof(ObjectMetadata);
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
    const auto key = Key{bucket, prefix, std::chrono::system_clock::now() + std::chrono::seconds(ttl)};

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

    if (pair->key == input_key || filter_by_prefix)
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
