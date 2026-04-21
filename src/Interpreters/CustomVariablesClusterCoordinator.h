#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/CustomVariablesClusterStorage.h>

#include <Common/ConcurrentBoundedQueue.h>
#include <Common/Logger.h>
#include <Common/ThreadPool.h>

#include <atomic>
#include <memory>

namespace DB
{

class CustomVariablesManager;

/// Keeps the in-memory view of cluster-scoped custom variables in sync with ZooKeeper
/// on every node. Single background thread, one event queue. Modelled on
/// UserDefinedSQLObjectsZooKeeperStorage.
///
class CustomVariablesClusterCoordinator
{
public:
    struct Event
    {
        enum class Kind : uint8_t { ResyncAll, DefinitionChanged, ValueChanged };
        Kind kind = Kind::ResyncAll;
        String name;  // empty for ResyncAll
    };

    CustomVariablesClusterCoordinator(
        ContextPtr global_context_,
        CustomVariablesClusterStoragePtr storage_,
        CustomVariablesManager & manager_);
    ~CustomVariablesClusterCoordinator();

    CustomVariablesClusterCoordinator(const CustomVariablesClusterCoordinator &) = delete;
    CustomVariablesClusterCoordinator & operator=(const CustomVariablesClusterCoordinator &) = delete;

    /// Starts the watcher thread. Safe to call multiple times; the first call
    /// also performs the initial synchronous load from ZK (best-effort; failures
    /// are logged and retried in the watcher loop).
    void start();

    /// Stops the watcher thread and joins it.
    void stop();

    /// Tells the watcher to re-inspect this variable name (called from local
    /// CREATE/DROP paths so we do not wait for the ZK watch round-trip).
    void poke(const String & name);

private:
    void watchLoop();
    void initialLoad();
    void resyncAll();
    void refreshOne(const String & name, bool rebuild_definition);

    /// Low-level ZK reads that also install fresh watches.
    Strings readDefinitionsAndInstallChildrenWatch();
    bool readDefinitionDataAndInstallWatch(const String & name, String & out);
    bool readValueDataAndInstallWatch(const String & name, String & out);

    ContextPtr global_context;
    CustomVariablesClusterStoragePtr storage;
    CustomVariablesManager & manager;
    LoggerPtr log;

    std::shared_ptr<ConcurrentBoundedQueue<Event>> queue;
    ThreadFromGlobalPool thread;
    std::atomic<bool> running{false};
    std::atomic<bool> loaded{false};
};

using CustomVariablesClusterCoordinatorPtr = std::unique_ptr<CustomVariablesClusterCoordinator>;

}
