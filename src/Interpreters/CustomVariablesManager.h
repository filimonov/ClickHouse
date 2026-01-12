#pragma once

#include <Interpreters/ICustomVariablesDefinitionsStorage.h>

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

class CustomVariablesManager
{
public:
    using Key = CustomVariableName;

    struct Definition
    {
        Key key;
        ASTPtr expression;
        ASTPtr refresh_strategy;
        DataTypePtr declared_type;
        std::chrono::system_clock::time_point load_time;
    };

    struct Value
    {
        DataTypePtr runtime_type;
        Field value;
        std::chrono::system_clock::time_point last_update_time;
        std::chrono::system_clock::time_point last_successful_update_time;
        String last_update_hostname;
        String last_error;
        String last_error_type;
        bool has_value = false;
        bool is_valid = false;
    };

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

    void loadFromStorage(const ContextPtr & context, ICustomVariablesDefinitionsStorage & storage);
    void setEntry(const Key & key, EntryPtr entry);
    bool removeEntry(const Key & key);
    void startRefreshIfNeeded(const ContextPtr & context, const EntryPtr & entry);
    void refreshNow(const Key & key);
    void refreshAll();

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
};

}
