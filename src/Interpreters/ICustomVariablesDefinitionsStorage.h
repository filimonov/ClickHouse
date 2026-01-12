#pragma once

#include <base/types.h>

#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

struct Settings;

struct CustomVariableName
{
    enum class Scope
    {
        Local,
        LocalPersistent,
        Session,
        Cluster,
    };

    Scope scope;
    String name;

    static bool tryParseScope(const String & scope_str, Scope & scope_out)
    {
        if (scope_str == "local")
            scope_out = Scope::Local;
        else if (scope_str == "local_persistent")
            scope_out = Scope::LocalPersistent;
        else if (scope_str == "session")
            scope_out = Scope::Session;
        else if (scope_str == "cluster")
            scope_out = Scope::Cluster;
        else
            return false;

        return true;
    }

    static String scopeToString(Scope scope_value)
    {
        switch (scope_value)
        {
            case Scope::Local:
                return "local";
            case Scope::LocalPersistent:
                return "local_persistent";
            case Scope::Session:
                return "session";
            case Scope::Cluster:
                return "cluster";
        }

        return "";
    }

    String fullName() const { return scopeToString(scope) + "." + name; }
    bool operator==(const CustomVariableName & other) const { return scope == other.scope && name == other.name; }
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
