#pragma once

#include <Interpreters/ICustomVariablesDefinitionsStorage.h>

#include <Core/Field.h>
#include <base/types.h>

#include <boost/smart_ptr/atomic_shared_ptr.hpp>
#include <boost/smart_ptr/shared_ptr.hpp>

#include <chrono>
#include <memory>
#include <shared_mutex>
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
        std::chrono::system_clock::time_point create_time;
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

    struct Entry
    {
        Definition definition;
        boost::atomic_shared_ptr<const Value> value;
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

private:
    struct KeyHash
    {
        size_t operator()(const Key & key) const;
    };

    mutable std::shared_mutex mutex;
    std::unordered_map<Key, EntryPtr, KeyHash> entries;
};

}
