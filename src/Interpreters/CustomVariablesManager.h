#pragma once

#include <Interpreters/CustomVariableValueSnapshot.h>
#include <Interpreters/CustomVariablesClusterStorage.h>
#include <Interpreters/CustomVariableKind.h>
#include <Interpreters/CustomVariablesDefinitionsDiskStorage.h>

#include <Storages/MaterializedView/RefreshSchedule.h>
#include <Storages/MaterializedView/RefreshSettings.h>

#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Core/Field.h>
#include <base/types.h>

#include <boost/smart_ptr/atomic_shared_ptr.hpp>
#include <boost/smart_ptr/shared_ptr.hpp>

#include <chrono>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <utility>
#include <unordered_map>
#include <vector>

namespace DB
{

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

class CustomVariablesClusterCoordinator;

class CustomVariablesManager
{
public:
    CustomVariablesManager();
    ~CustomVariablesManager();
    using Key = CustomVariableName;

    struct Definition
    {
        Key key;
        ASTPtr expression;
        ASTPtr refresh_strategy;
        DataTypePtr declared_type;
        std::chrono::system_clock::time_point load_time;
    };

    /// In-memory runtime value — same shape as the disk / Keeper serialization
    /// shape, so there is exactly one struct to reason about.
    using Value = CustomVariableValueSnapshot;

    struct RefreshState
    {
        std::chrono::sys_seconds last_completed_timeslot;
        std::chrono::sys_seconds last_attempt_time;
        String last_attempt_replica;
        String last_attempt_error;
        String previous_attempt_error;
        bool last_attempt_succeeded = false;
        Int64 attempt_number = 0;
        Int64 randomness = 0;
    };

    struct RefreshData
    {
        explicit RefreshData(RefreshSchedule schedule_) : schedule(std::move(schedule_)) {}

        std::mutex mutex;
        RefreshState state;
        RefreshSchedule schedule;
        RefreshSettings settings;
        std::chrono::system_clock::time_point next_refresh_time;
        BackgroundSchedulePoolTaskHolder task;
        bool stop_requested = false;
        bool out_of_schedule_refresh_requested = false;
    };

    struct Entry
    {
        Definition definition;
        boost::atomic_shared_ptr<const Value> value;
        std::unique_ptr<RefreshData> refresh;
    };

    using EntryPtr = std::shared_ptr<Entry>;
    using ValuePtr = boost::shared_ptr<const Value>;
    using Entries = std::vector<EntryPtr>;

    EntryPtr tryGetEntry(const Key & key) const;
    EntryPtr getEntry(const Key & key) const;
    bool hasEntry(const Key & key) const;
    Entries getAllEntries() const;

    void loadFromStorage(const ContextPtr & context, CustomVariablesDefinitionsDiskStorage & storage);
    void setEntry(const ContextPtr & context, const Key & key, EntryPtr entry);
    bool removeEntry(const Key & key);
    void prepareRefreshIfNeeded(const ContextPtr & context, const EntryPtr & entry);
    void schedulePreparedRefresh(const EntryPtr & entry);
    void startRefreshIfNeeded(const ContextPtr & context, const EntryPtr & entry);
    void refreshNow(const Key & key);
    void persistValueIfNeeded(const ContextPtr & context, const EntryPtr & entry) const;

    /// Lifecycle for the cluster-coordinator. No-op if storage is null.
    void startClusterCoordinator(const ContextPtr & global_context, CustomVariablesClusterStoragePtr storage);
    void stopClusterCoordinator();
    /// Nudge the coordinator to re-read this specific replicated variable name. Safe if coordinator is absent.
    void pokeClusterCoordinator(const String & name);

private:
    struct KeyHash
    {
        size_t operator()(const Key & key) const;
    };

    void stopRefreshTask(const EntryPtr & entry);
    void refreshTask(const ContextPtr & context, const EntryPtr & entry);
    void requestRefresh(const EntryPtr & entry, bool throw_if_not_refreshable);

    mutable std::shared_mutex mutex;
    std::unordered_map<Key, EntryPtr, KeyHash> entries;

    std::unique_ptr<CustomVariablesClusterCoordinator> cluster_coordinator;
};


/// Per-session store for CREATE TEMPORARY VARIABLE. RAM only, dies with
/// the session. No refresh scheduler, no cluster coordinator, no value
/// persistence — none of that applies to session-scoped variables.
class TemporaryVariables
{
public:
    using Entry = CustomVariablesManager::Entry;
    using EntryPtr = CustomVariablesManager::EntryPtr;
    using Entries = CustomVariablesManager::Entries;

    EntryPtr tryGetEntry(const String & name) const;
    bool hasEntry(const String & name) const;
    void setEntry(const String & name, EntryPtr entry);
    bool removeEntry(const String & name);
    Entries getAllEntries() const;

private:
    mutable std::mutex mutex;
    std::unordered_map<String, EntryPtr> entries;
};

}
