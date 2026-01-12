#pragma once

#include <base/types.h>

#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

struct Settings;

struct CustomVariableName
{
    String scope;
    String name;

    String fullName() const { return scope + "." + name; }
};

class ICustomVariablesDefinitionsStorage
{
public:
    using ObjectName = CustomVariableName;
    using Objects = std::vector<std::pair<ObjectName, ASTPtr>>;

    virtual ~ICustomVariablesDefinitionsStorage() = default;

    /// Loads all objects from storage.
    virtual Objects loadObjects() = 0;

    /// Stores an object definition.
    virtual bool storeObject(
        const ContextPtr & current_context,
        const ObjectName & object_name,
        ASTPtr create_query,
        bool throw_if_exists,
        bool replace_if_exists,
        const Settings & settings) = 0;

    /// Removes an object definition.
    virtual bool removeObject(
        const ContextPtr & current_context,
        const ObjectName & object_name,
        bool throw_if_not_exists) = 0;
};

}
