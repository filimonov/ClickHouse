#include <Interpreters/CustomVariablesManager.h>

#include <Common/Exception.h>
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
