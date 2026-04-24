#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterDropVariableQuery.h>

#include <Access/ContextAccess.h>
#include <Common/logger_useful.h>

#include <Interpreters/Context.h>
#include <Interpreters/CustomVariablesClusterStorage.h>
#include <Interpreters/CustomVariablesManager.h>
#include <Interpreters/CustomVariablesValuesDiskStorage.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Parsers/ASTDropVariableQuery.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int FILE_DOESNT_EXIST;
}

namespace
{
String getBareVariableName(const ASTPtr & ast)
{
    const auto * identifier = ast ? ast->as<ASTIdentifier>() : nullptr;
    if (!identifier)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Custom variable name is not an identifier");

    String name;
    if (!tryGetIdentifierNameInto(identifier, name) || name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Custom variable name must be a non-empty identifier");

    return name;
}
}

BlockIO InterpreterDropVariableQuery::execute()
{
    const auto & drop_query = query_ptr->as<ASTDropVariableQuery &>();
    auto bare_name = getBareVariableName(drop_query.variable_name);
    const CustomVariableKind kind = drop_query.kind;
    CustomVariableName object_name{kind, bare_name};

    const bool is_temporary = (kind == CustomVariableKind::Temporary);
    const bool is_replicated = (kind == CustomVariableKind::Replicated);
    const bool is_server = (kind == CustomVariableKind::Server);

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::DROP_VARIABLE);

    auto current_context = getContext();

    if (!drop_query.cluster.empty())
    {
        /// Grammar guarantees only server kind carries ON CLUSTER.
        chassert(is_server);
        DDLQueryOnClusterParams params;
        params.access_to_check = std::move(access_rights_elements);
        return executeDDLQueryOnCluster(query_ptr, current_context, params);
    }

    current_context->checkAccess(access_rights_elements);

    bool throw_if_not_exists = !drop_query.if_exists;

    if (is_replicated)
    {
        auto cluster_storage = current_context->getCustomVariablesClusterStorage();
        if (!cluster_storage)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Replicated custom variables require <custom_variables_zookeeper_path> in the server config");

        if (!cluster_storage->removeDefinition(object_name.name, throw_if_not_exists))
            return {};

        cluster_storage->removeValueRecursive(object_name.name);
        auto & cv_manager = current_context->getCustomVariablesManager();
        cv_manager.removeEntry(object_name);
        cv_manager.pokeClusterCoordinator(object_name.name);
    }
    else if (is_server)
    {
        auto & storage = current_context->getCustomVariablesDefinitionsStorage();
        if (!storage.removeObject(current_context, object_name, throw_if_not_exists))
            return {};

        current_context->getCustomVariablesManager().removeEntry(object_name);

        /// Server kind always persists its value on disk; clean up the .bin file too.
        try
        {
            current_context->getCustomVariablesValuesStorage().removeValue(object_name.name);
        }
        catch (...)
        {
            tryLogCurrentException(
                getLogger("InterpreterDropVariableQuery"),
                fmt::format("while removing persisted value for server variable '{}'", object_name.name));
        }
    }
    else
    {
        chassert(is_temporary);
        auto & manager = current_context->getSessionCustomVariablesManager();
        if (!manager.removeEntry(object_name.name))
        {
            if (throw_if_not_exists)
                throw Exception(
                    ErrorCodes::FILE_DOESNT_EXIST,
                    "{} variable '{}' doesn't exist",
                    kindDisplayName(kind),
                    object_name.name);
            return {};
        }
    }
    return {};
}

void registerInterpreterDropVariableQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterDropVariableQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterDropVariableQuery", create_fn);
}

}
