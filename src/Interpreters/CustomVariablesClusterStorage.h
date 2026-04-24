#pragma once

#include <Interpreters/CustomVariableValueSnapshot.h>
#include <Interpreters/Context_fwd.h>

#include <Common/Logger.h>
#include <Common/ZooKeeper/ZooKeeperCachingGetter.h>

#include <Parsers/IAST_fwd.h>

#include <memory>
#include <optional>
#include <vector>

namespace zkutil { class ZooKeeperLock; }

namespace DB
{

/// ZooKeeper-backed storage for cluster-scoped custom variables.
///
/// Layout under <root>:
///   <root>/definitions/<escaped_name>     — DDL blob (CREATE REPLICATED VARIABLE ... AS CAST(...))
///   <root>/values/<escaped_name>          — binary CustomVariableValueSnapshot blob
///   <root>/values/<escaped_name>/lock     — ephemeral leader lock during refresh
///
/// This class is a thin I/O layer. Watchers and the coordinator thread are
/// implemented on top of it in a separate component.
class CustomVariablesClusterStorage
{
public:
    CustomVariablesClusterStorage(const ContextPtr & global_context_, const String & zookeeper_root_);

    const String & getRoot() const { return zookeeper_root; }

    /// Ensures <root>, <root>/definitions, <root>/values exist. Called lazily on first access.
    void createRootNodesIfNeeded();

    /// Definitions
    std::optional<String> tryLoadDefinition(const String & name);
    bool storeDefinition(const String & name, const String & create_query_text, bool throw_if_exists, bool replace_if_exists);
    bool removeDefinition(const String & name, bool throw_if_not_exists);
    std::vector<String> listDefinitionNames();

    /// Values
    std::optional<CustomVariableValueSnapshot> tryLoadValue(const String & name);
    void storeValue(const String & name, const CustomVariableValueSnapshot & snapshot);
    void removeValueRecursive(const String & name);

    /// Leader election for refresh. Returned object owns the lock; destruction releases it.
    /// Returns nullptr if another node already holds the lock.
    std::unique_ptr<zkutil::ZooKeeperLock> tryLockForRefresh(const String & name, const String & lock_holder_message);

    zkutil::ZooKeeperPtr getZooKeeper();

private:
    String definitionsPath() const { return zookeeper_root + "/definitions"; }
    String valuesPath() const { return zookeeper_root + "/values"; }
    String definitionPath(const String & escaped_name) const;
    String valuePath(const String & escaped_name) const;
    String lockPath(const String & escaped_name) const;

    ContextPtr global_context;
    String zookeeper_root;
    zkutil::ZooKeeperCachingGetter zookeeper_getter;
    LoggerPtr log;
};

using CustomVariablesClusterStoragePtr = std::shared_ptr<CustomVariablesClusterStorage>;

}
