#include <Interpreters/CustomVariablesManager.h>

#include <Common/Exception.h>
#include <Common/ErrorCodes.h>
#include <Common/logger_useful.h>
#include <base/getFQDNOrHostName.h>

#include <Interpreters/CustomVariablesEvaluator.h>
#include <Interpreters/Context.h>
#include <Interpreters/CustomVariablesValuesDiskStorage.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/StorageID.h>

#include <Parsers/ASTCreateVariableQuery.h>
#include <Parsers/ASTRefreshStrategy.h>

#include <Storages/MaterializedView/RefreshScheduler.h>

#include <boost/make_shared.hpp>

#include <Common/thread_local_rng.h>

#include <Core/BackgroundSchedulePool.h>

#include <random>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_QUERY;
    extern const int UNKNOWN_IDENTIFIER;
}

namespace
{
LoggerPtr getLog()
{
    static LoggerPtr log = getLogger("CustomVariablesManager");
    return log;
}

void randomizeState(CustomVariablesManager::RefreshState & state)
{
    state.randomness = std::uniform_int_distribution<Int64>(Int64(-1e9), Int64(1e9))(thread_local_rng);
}

bool isLocalPersistentScope(CustomVariableName::Scope scope)
{
    return scope == CustomVariableName::Scope::LocalPersistent;
}

bool isValueStale(const RefreshSchedule & schedule, std::chrono::system_clock::time_point last_success, std::chrono::system_clock::time_point now)
{
    if (last_success.time_since_epoch().count() == 0)
        return true;
    const auto last_timeslot = std::chrono::floor<std::chrono::seconds>(last_success);
    const auto next_time = schedule.advance(last_timeslot);
    return now >= next_time;
}
}

size_t CustomVariablesManager::KeyHash::operator()(const Key & key) const
{
    return std::hash<size_t>{}(static_cast<size_t>(key.scope)) ^ (std::hash<String>{}(key.name) << 1);
}

CustomVariablesManager::EntryPtr CustomVariablesManager::tryGetEntry(const Key & key) const
{
    std::shared_lock lock(mutex);
    auto it = entries.find(key);
    if (it == entries.end())
        return nullptr;
    return it->second;
}

CustomVariablesManager::EntryPtr CustomVariablesManager::getEntry(const Key & key) const
{
    auto entry = tryGetEntry(key);
    if (!entry)
        throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "Custom variable '{}' not found", key.fullName());
    return entry;
}

bool CustomVariablesManager::hasEntry(const Key & key) const
{
    return tryGetEntry(key) != nullptr;
}

CustomVariablesManager::Entries CustomVariablesManager::getAllEntries() const
{
    Entries res;
    std::shared_lock lock(mutex);
    res.reserve(entries.size());
    for (const auto & [_, entry] : entries)
        res.push_back(entry);
    return res;
}

void CustomVariablesManager::loadFromStorage(const ContextPtr & context, ICustomVariablesDefinitionsStorage & storage)
{
    auto objects = storage.loadObjects();
    std::unordered_map<Key, EntryPtr, KeyHash> new_entries;
    new_entries.reserve(objects.size());
    Entries entries_to_refresh;

    for (const auto & [object_name, ast] : objects)
    {
        const auto * create_query = ast ? ast->as<ASTCreateVariableQuery>() : nullptr;
        if (!create_query)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unexpected custom variable definition for '{}'",
                object_name.fullName());

        Definition definition;
        definition.key = object_name;
        definition.expression = create_query->expression;
        definition.refresh_strategy = create_query->refresh_strategy;
        definition.declared_type = nullptr;
        definition.load_time = std::chrono::system_clock::now();

        auto entry = std::make_shared<Entry>();
        entry->definition = std::move(definition);

        const bool is_local_persistent = isLocalPersistentScope(object_name.scope);
        std::optional<CustomVariableValueSnapshot> snapshot;
        if (is_local_persistent)
            snapshot = context->getCustomVariablesValuesStorage().tryLoadValue(object_name.name);

        bool loaded_from_disk = false;
        bool need_immediate_refresh = false;

        try
        {
            entry->definition.declared_type = getCustomVariableExpressionType(entry->definition.expression, context);
        }
        catch (...)
        {
            tryLogCurrentException(getLog(), fmt::format("while resolving declared type for custom variable '{}'", object_name.fullName()));
        }

        if (snapshot && snapshot->has_value)
        {
            try
            {
                auto value = boost::make_shared<Value>();
                value->runtime_type = snapshot->runtime_type ? snapshot->runtime_type : entry->definition.declared_type;
                value->value = snapshot->value;
                value->last_update_time = snapshot->last_update_time;
                value->last_successful_update_time = snapshot->last_successful_update_time;
                value->last_update_hostname = snapshot->last_update_hostname;
                value->last_error = snapshot->last_error;
                value->last_error_type = snapshot->last_error_type;
                value->has_value = snapshot->has_value;
                value->is_valid = snapshot->is_valid;

                if (entry->definition.declared_type && (!value->runtime_type || !entry->definition.declared_type->equals(*value->runtime_type)))
                {
                    value->value = convertFieldToType(value->value, *entry->definition.declared_type);
                    value->runtime_type = entry->definition.declared_type;
                }

                checkCustomVariableSize(value->value);
                entry->value.store(boost::static_pointer_cast<const Value>(value));
                loaded_from_disk = true;
            }
            catch (...)
            {
                tryLogCurrentException(getLog(), fmt::format("while loading persisted custom variable '{}'", object_name.fullName()));
            }
        }

        if (!loaded_from_disk)
        {
            try
            {
                auto evaluated = evaluateCustomVariableExpression(entry->definition.expression, context);
                DataTypePtr declared_type = entry->definition.declared_type ? entry->definition.declared_type : evaluated.type;
                if (!entry->definition.declared_type)
                    entry->definition.declared_type = declared_type;
                Field value_field = std::move(evaluated.value);

                if (!declared_type->equals(*evaluated.type))
                    value_field = convertFieldToType(value_field, *declared_type);

                checkCustomVariableSize(value_field);

                auto value = boost::make_shared<Value>();
                value->runtime_type = declared_type;
                value->value = std::move(value_field);
                value->last_update_time = std::chrono::system_clock::now();
                value->last_successful_update_time = value->last_update_time;
                value->last_update_hostname = getFQDNOrHostName();
                value->has_value = true;
                value->is_valid = true;
                entry->value.store(boost::static_pointer_cast<const Value>(value));

                if (is_local_persistent)
                    persistValueIfNeeded(context, entry);
            }
            catch (...)
            {
                tryLogCurrentException(getLog(), fmt::format("while evaluating custom variable '{}'", object_name.fullName()));
                auto value = boost::make_shared<Value>();
                value->last_update_time = std::chrono::system_clock::now();
                value->last_error = getCurrentExceptionMessage(false);
                value->last_error_type = ErrorCodes::getName(getCurrentExceptionCode());
                value->has_value = false;
                value->is_valid = false;
                entry->value.store(boost::static_pointer_cast<const Value>(value));

                if (is_local_persistent)
                    persistValueIfNeeded(context, entry);
            }
        }

        if (!entry->definition.declared_type && snapshot && snapshot->runtime_type)
            entry->definition.declared_type = snapshot->runtime_type;

        if (loaded_from_disk && entry->definition.refresh_strategy)
        {
            const auto * refresh = entry->definition.refresh_strategy->as<ASTRefreshStrategy>();
            if (refresh)
            {
                RefreshSchedule schedule(*refresh);
                auto loaded_value = entry->value.load();
                auto last_success = loaded_value ? loaded_value->last_successful_update_time : std::chrono::system_clock::time_point{};
                const auto now = std::chrono::system_clock::now();
                need_immediate_refresh = isValueStale(schedule, last_success, now);
            }
        }

        auto [it, inserted] = new_entries.emplace(object_name, std::move(entry));
        if (need_immediate_refresh && inserted)
            entries_to_refresh.push_back(it->second);
    }

    Entries entries_to_stop;
    {
        std::unique_lock lock(mutex);
        entries_to_stop.reserve(entries.size());
        for (const auto & [_, entry] : entries)
            entries_to_stop.push_back(entry);
        entries.swap(new_entries);
    }

    for (const auto & entry : entries_to_stop)
        stopRefreshTask(entry);

    for (const auto & [_, entry] : entries)
        startRefreshIfNeeded(context, entry);

    for (const auto & entry : entries_to_refresh)
        requestRefresh(entry, false);
}

void CustomVariablesManager::setEntry(const ContextPtr & context, const Key & key, EntryPtr entry)
{
    EntryPtr stored_entry;
    std::unique_lock lock(mutex);
    auto it = entries.find(key);
    if (it != entries.end())
        stopRefreshTask(it->second);
    entries[key] = std::move(entry);
    stored_entry = entries[key];
    lock.unlock();

    if (context)
        persistValueIfNeeded(context, stored_entry);
}

bool CustomVariablesManager::removeEntry(const Key & key)
{
    std::unique_lock lock(mutex);
    auto it = entries.find(key);
    if (it == entries.end())
        return false;
    stopRefreshTask(it->second);
    entries.erase(it);
    return true;
}

void CustomVariablesManager::startRefreshIfNeeded(const ContextPtr & context, const EntryPtr & entry)
{
    if (!entry || !entry->definition.refresh_strategy)
        return;

    if (entry->refresh)
        return;

    const auto * refresh = entry->definition.refresh_strategy->as<ASTRefreshStrategy>();
    if (!refresh)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid refresh strategy");

    auto refresh_data = std::make_unique<RefreshData>(RefreshSchedule(*refresh));
    if (refresh->settings)
        refresh_data->settings.applyChanges(refresh->settings->changes);

    const auto now = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    refresh_data->state.last_completed_timeslot = now;
    refresh_data->state.last_attempt_replica = getFQDNOrHostName();
    randomizeState(refresh_data->state);

    auto value = entry->value.load();
    if (!value || !value->is_valid)
    {
        refresh_data->state.last_attempt_time = now;
        refresh_data->state.last_attempt_error = value ? value->last_error : "Initial evaluation failed";
        refresh_data->state.last_attempt_succeeded = false;
        refresh_data->state.attempt_number = 1;
    }

    const auto storage_id = StorageID("", "custom_variable." + entry->definition.key.fullName());
    refresh_data->task = context->getSchedulePool().createTask(storage_id, "CustomVariableRefresh",
        [this, context, entry] { refreshTask(context, entry); });

    entry->refresh = std::move(refresh_data);
    entry->refresh->task->schedule();
}

void CustomVariablesManager::refreshNow(const Key & key)
{
    requestRefresh(getEntry(key), true);
}

void CustomVariablesManager::refreshAll()
{
    const auto entries_snapshot = getAllEntries();
    for (const auto & entry : entries_snapshot)
        requestRefresh(entry, false);
}

void CustomVariablesManager::stopRefreshTask(const EntryPtr & entry)
{
    if (!entry || !entry->refresh)
        return;

    std::lock_guard lock(entry->refresh->mutex);
    entry->refresh->stop_requested = true;
    if (entry->refresh->task)
        entry->refresh->task->deactivate();

    entry->refresh->out_of_schedule_refresh_requested = false;
}

void CustomVariablesManager::requestRefresh(const EntryPtr & entry, bool throw_if_not_refreshable)
{
    if (!entry)
        return;

    if (!entry->refresh)
    {
        if (throw_if_not_refreshable)
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "Custom variable '{}' is not refreshable",
                entry->definition.key.fullName());
        return;
    }

    std::lock_guard lock(entry->refresh->mutex);
    entry->refresh->out_of_schedule_refresh_requested = true;
    entry->refresh->task->schedule();
}

void CustomVariablesManager::refreshTask(const ContextPtr & context, const EntryPtr & entry)
{
    if (!entry || !entry->refresh)
        return;

    std::unique_lock lock(entry->refresh->mutex);
    if (entry->refresh->stop_requested)
        return;

    const auto now = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    auto state_snapshot = entry->refresh->state;
    const bool out_of_schedule = entry->refresh->out_of_schedule_refresh_requested;
    entry->refresh->out_of_schedule_refresh_requested = false;
    auto [when, timeslot, planned_state] = planNextRefresh(
        now, entry->refresh->schedule, entry->refresh->settings, entry->refresh->state.last_attempt_replica, state_snapshot);
    (void)timeslot;

    if (out_of_schedule)
    {
        if (planned_state.attempt_number > 0)
            planned_state.attempt_number -= 1;
        when = std::chrono::system_clock::now();
    }

    entry->refresh->next_refresh_time = when;

    if (!out_of_schedule && now < when)
    {
        auto delay = std::chrono::duration_cast<std::chrono::milliseconds>(when - std::chrono::system_clock::now());
        auto delay_ms = delay.count() > 0 ? static_cast<size_t>(delay.count()) : 0;
        entry->refresh->task->scheduleAfter(delay_ms);
        return;
    }

    entry->refresh->state = std::move(planned_state);
    auto start_time = std::chrono::system_clock::now();
    lock.unlock();

    bool refreshed = false;
    String error_message;
    String error_type;
    try
    {
        auto evaluated = evaluateCustomVariableExpression(entry->definition.expression, context);
        DataTypePtr declared_type = entry->definition.declared_type ? entry->definition.declared_type : evaluated.type;
        Field value_field = std::move(evaluated.value);

        if (!declared_type->equals(*evaluated.type))
            value_field = convertFieldToType(value_field, *declared_type);

        checkCustomVariableSize(value_field);

        auto value = boost::make_shared<Value>();
        value->runtime_type = declared_type;
        value->value = std::move(value_field);
        value->last_update_time = std::chrono::system_clock::now();
        value->last_successful_update_time = value->last_update_time;
        value->last_update_hostname = getFQDNOrHostName();
        value->has_value = true;
        value->is_valid = true;
        entry->value.store(boost::static_pointer_cast<const Value>(value));

        refreshed = true;
        persistValueIfNeeded(context, entry);
    }
    catch (...)
    {
        error_message = getCurrentExceptionMessage(false);
        error_type = ErrorCodes::getName(getCurrentExceptionCode());

        auto value = boost::make_shared<Value>();
        auto previous = entry->value.load();
        if (previous && previous->has_value)
        {
            value->runtime_type = previous->runtime_type;
            value->value = previous->value;
            value->last_successful_update_time = previous->last_successful_update_time;
            value->has_value = true;
        }
        value->last_update_time = std::chrono::system_clock::now();
        value->last_update_hostname = getFQDNOrHostName();
        value->last_error = error_message;
        value->last_error_type = error_type;
        value->is_valid = false;
        entry->value.store(boost::static_pointer_cast<const Value>(value));
        persistValueIfNeeded(context, entry);
    }

    auto end_time = std::chrono::system_clock::now();
    lock.lock();
    if (entry->refresh->stop_requested)
        return;

    entry->refresh->state.last_attempt_time = std::chrono::floor<std::chrono::seconds>(end_time);
    entry->refresh->state.last_attempt_error = error_message;
    entry->refresh->state.last_attempt_succeeded = refreshed;

    if (refreshed)
    {
        entry->refresh->state.last_completed_timeslot = entry->refresh->schedule.timeslotForCompletedRefresh(
            entry->refresh->state.last_completed_timeslot,
            std::chrono::floor<std::chrono::seconds>(start_time),
            std::chrono::floor<std::chrono::seconds>(end_time),
            false);
        entry->refresh->state.previous_attempt_error = "";
        entry->refresh->state.attempt_number = 0;
        randomizeState(entry->refresh->state);
    }

    entry->refresh->task->schedule();
}

void CustomVariablesManager::persistValueIfNeeded(const ContextPtr & context, const EntryPtr & entry) const
{
    if (!context || !entry || !isLocalPersistentScope(entry->definition.key.scope))
        return;

    const auto value = entry->value.load();
    if (!value)
        return;

    CustomVariableValueSnapshot snapshot;
    snapshot.runtime_type = value->runtime_type;
    snapshot.value = value->value;
    snapshot.last_update_time = value->last_update_time;
    snapshot.last_successful_update_time = value->last_successful_update_time;
    snapshot.last_update_hostname = value->last_update_hostname;
    snapshot.last_error = value->last_error;
    snapshot.last_error_type = value->last_error_type;
    snapshot.has_value = value->has_value;
    snapshot.is_valid = value->is_valid;

    try
    {
        context->getCustomVariablesValuesStorage().storeValue(entry->definition.key.name, snapshot, context->getSettingsRef());
    }
    catch (...)
    {
        tryLogCurrentException(getLog(), fmt::format("while storing custom variable '{}' value", entry->definition.key.fullName()));
    }
}

}
