#pragma once

#include <base/types.h>

#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

struct Settings;

/// What storage domain a custom variable lives in. The same bare name can
/// exist independently in each domain; the DDL keyword and the read function
/// together determine which domain a statement targets.
enum class CustomVariableKind
{
    /// Default. CREATE VARIABLE, getVariable(). Definition on disk, value on
    /// this server only (persisted for restart resilience).
    Server,
    /// CREATE TEMPORARY VARIABLE, getTemporaryVariable(). RAM only, dies with
    /// the session. Never persisted, never distributed.
    Temporary,
    /// CREATE REPLICATED VARIABLE, getReplicatedVariable(). Keeper-backed:
    /// one canonical definition and value shared across all nodes. Requires
    /// <custom_variables_zookeeper_path> in server config.
    Replicated,
};

inline const char * kindDisplayName(CustomVariableKind kind)
{
    switch (kind)
    {
        case CustomVariableKind::Server: return "server";
        case CustomVariableKind::Temporary: return "temporary";
        case CustomVariableKind::Replicated: return "replicated";
    }
    return "";
}

struct CustomVariableName
{
    CustomVariableKind kind;
    String name;

    /// Bare name for display. Error messages that want to disambiguate kinds
    /// should include kindDisplayName() in their format string explicitly.
    String fullName() const { return name; }
    bool operator==(const CustomVariableName & other) const { return kind == other.kind && name == other.name; }
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
