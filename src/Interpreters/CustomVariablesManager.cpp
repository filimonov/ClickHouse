#include <Interpreters/CustomVariablesManager.h>

#include <Common/Exception.h>
#include <Parsers/ASTCreateVariableQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

size_t CustomVariablesManager::KeyHash::operator()(const Key & key) const
{
    return std::hash<String>{}(key.scope) ^ (std::hash<String>{}(key.name) << 1);
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
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Custom variable '{}' not found", key.fullName());
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

void CustomVariablesManager::loadFromStorage(ICustomVariablesDefinitionsStorage & storage)
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
        definition.create_time = std::chrono::system_clock::now();

        auto entry = std::make_shared<Entry>();
        entry->definition = std::move(definition);
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
