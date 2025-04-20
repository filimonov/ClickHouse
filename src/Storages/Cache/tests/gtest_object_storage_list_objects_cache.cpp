#include <gtest/gtest.h>
#include <Storages/Cache/ObjectStorageListObjectsCache.h>
#include <memory>
#include <thread>

namespace DB
{

auto & initializeAndGetCacheInstance()
{
    static auto & cache = ObjectStorageListObjectsCache::instance();
    cache.setTTL(3);
    cache.setMaxCount(100);
    cache.setMaxSizeInBytes(1000000);
    return cache;
}

static auto & cache = initializeAndGetCacheInstance();

std::shared_ptr<ObjectStorageListObjectsCache::Value> createTestValue(const std::vector<std::string>& paths)
{
    auto value = std::make_shared<ObjectStorageListObjectsCache::Value>();
    for (const auto & path : paths)
    {
        value->push_back(std::make_shared<ObjectInfo>(path));
    }
    return value;
}


TEST(ObjectStorageListObjectsCacheTest, BasicSetAndGet)
{
    cache.clear();
    const std::string bucket = "test-bucket";
    const std::string prefix = "test-prefix/";
    auto value = createTestValue({"test-prefix/file1.txt", "test-prefix/file2.txt"});
    
    cache.set(bucket, prefix, value);
    
    auto result = cache.get(bucket, prefix);
    ASSERT_EQ(result->size(), 2);
    EXPECT_EQ((*result)[0]->getPath(), "test-prefix/file1.txt");
    EXPECT_EQ((*result)[1]->getPath(), "test-prefix/file2.txt");
}

TEST(ObjectStorageListObjectsCacheTest, CacheMiss)
{
    cache.clear();
    const std::string bucket = "test-bucket";
    const std::string prefix = "test-prefix/";
    
    auto result = cache.get(bucket, prefix);
    EXPECT_EQ(result, nullptr);
}

TEST(ObjectStorageListObjectsCacheTest, ClearCache)
{
    cache.clear();
    const std::string bucket = "test-bucket";
    const std::string prefix = "test-prefix/";
    auto value = createTestValue({"test-prefix/file1.txt", "test-prefix/file2.txt"});
    
    cache.set(bucket, prefix, value);
    cache.clear();
    
    auto result = cache.get(bucket, prefix);
    EXPECT_EQ(result, nullptr);
}

TEST(ObjectStorageListObjectsCacheTest, PrefixMatching)
{
    cache.clear();
    const std::string bucket = "test-bucket";
    const std::string short_prefix = "parent/";
    const std::string mid_prefix = "parent/child/";
    const std::string long_prefix = "parent/child/grandchild/";
    
    auto value = createTestValue(
    {
        "parent/child/grandchild/file1.txt",
        "parent/child/grandchild/file2.txt"});

    cache.set(bucket, mid_prefix, value);

    auto result1 = cache.get(bucket, mid_prefix);
    EXPECT_EQ(result1->size(), 2);

    auto result2 = cache.get(bucket, long_prefix);
    EXPECT_EQ(result2->size(), 2);

    auto result3 = cache.get(bucket, short_prefix);
    ASSERT_EQ(result3, nullptr);
}

TEST(ObjectStorageListObjectsCacheTest, PrefixFiltering)
{
    cache.clear();
    const std::string bucket = "test-bucket";
    const std::string short_prefix = "parent/";

    auto value = createTestValue({
        "parent/file1.txt",
        "parent/child1/file2.txt",
        "parent/child2/file3.txt"
    });
    
    cache.set(bucket, short_prefix, value);

    auto result = cache.get(bucket, "parent/child1/", true);
    EXPECT_EQ(result->size(), 1);
    EXPECT_EQ((*result)[0]->getPath(), "parent/child1/file2.txt");
}

TEST(ObjectStorageListObjectsCacheTest, TTLExpiration)
{
    cache.clear();
    const std::string bucket = "test-bucket";
    const std::string prefix = "test-prefix/";
    auto value = createTestValue({"test-prefix/file1.txt"});

    cache.set(bucket, prefix, value);
    
    // Verify we can get it immediately
    auto result1 = cache.get(bucket, prefix);
    EXPECT_EQ(result1->size(), 1);

    std::this_thread::sleep_for(std::chrono::seconds(4));

    auto result2 = cache.get(bucket, prefix);
    EXPECT_EQ(result2, nullptr);
}

TEST(ObjectStorageListObjectsCacheTest, BestPrefixMatch)
{
    cache.clear();
    const std::string bucket = "test-bucket";

    auto short_prefix = createTestValue({"a/b/c/d/file1.txt", "a/b/c/file1.txt", "a/b/file2.txt"});
    auto mid_prefix = createTestValue({"a/b/c/d/file1.txt", "a/b/c/file1.txt"});

    cache.set(bucket, "a/b/", short_prefix);
    cache.set(bucket, "a/b/c/", mid_prefix);

    // should pick mid_prefix, which has size 2. filter_by_prefix=false so we can assert by size
    auto result = cache.get(bucket, "a/b/c/d/", false);
    EXPECT_EQ(result->size(), 2u);
}

}
