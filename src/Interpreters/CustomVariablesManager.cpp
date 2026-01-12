#include <Interpreters/CustomVariablesManager.h>

#include <Common/Exception.h>
#include <Common/ErrorCodes.h>
#include <Common/logger_useful.h>
#include <base/getFQDNOrHostName.h>

#include <Interpreters/CustomVariablesEvaluator.h>
#include <Interpreters/convertFieldToType.h>

#include <Parsers/ASTCreateVariableQuery.h>

#include <boost/make_shared.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_IDENTIFIER;
}

namespace
{
LoggerPtr getLog()
{
    static LoggerPtr log = getLogger("CustomVariablesManager");
    return log;
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

        try
        {
            entry->definition.declared_type = getCustomVariableExpressionType(entry->definition.expression, context);
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

            if (!entry->definition.declared_type)
            {
                try
                {
                    entry->definition.declared_type = getCustomVariableExpressionType(entry->definition.expression, context);
                }
                catch (...)
                {
                    tryLogCurrentException(getLog(), fmt::format("while resolving declared type for custom variable '{}'", object_name.fullName()));
                }
            }
        }

        new_entries.emplace(object_name, std::move(entry));
    }

    std::unique_lock lock(mutex);
    entries.swap(new_entries);
}

void CustomVariablesManager::setEntry(const Key & key, EntryPtr entry)
{
    std::unique_lock lock(mutex);
    entries[key] = std::move(entry);
}

bool CustomVariablesManager::removeEntry(const Key & key)
{
    std::unique_lock lock(mutex);
    return entries.erase(key) > 0;
}

}
