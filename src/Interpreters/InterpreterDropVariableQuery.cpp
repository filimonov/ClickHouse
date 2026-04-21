#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterDropVariableQuery.h>

#include <Access/ContextAccess.h>
#include <Common/logger_useful.h>

#include <Interpreters/Context.h>
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
    extern const int INCORRECT_QUERY;
    extern const int NOT_IMPLEMENTED;
}

namespace
{
CustomVariableName getCustomVariableName(const ASTPtr & ast, bool is_cluster_variable)
{
    const auto * identifier = ast ? ast->as<ASTIdentifier>() : nullptr;
    if (!identifier)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Custom variable name is not an identifier");

    if (is_cluster_variable)
    {
        if (identifier->name_parts.size() != 1 || identifier->name_parts[0].empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Cluster custom variable name must be a single identifier (no scope prefix)");
        return CustomVariableName{CustomVariableName::Scope::Cluster, identifier->name_parts[0]};
    }

    if (identifier->name_parts.size() != 2)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Custom variable name must be specified as scope.name");

    const auto & scope_str = identifier->name_parts[0];
    const auto & name = identifier->name_parts[1];
    if (scope_str.empty() || name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Custom variable name must be specified as scope.name");

    CustomVariableName::Scope scope;
    if (!CustomVariableName::tryParseScope(scope_str, scope))
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Unknown custom variable scope '{}'", scope_str);

    if (scope == CustomVariableName::Scope::Cluster)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Use DROP CLUSTER VARIABLE <name> syntax for cluster-scoped variables");

    return CustomVariableName{scope, name};
}
}

BlockIO InterpreterDropVariableQuery::execute()
{
    const auto & drop_query = query_ptr->as<ASTDropVariableQuery &>();
    auto object_name = getCustomVariableName(drop_query.variable_name, drop_query.is_cluster_variable);

    const bool is_session_scope = (object_name.scope == CustomVariableName::Scope::Session);
    const bool is_local_persistent = (object_name.scope == CustomVariableName::Scope::LocalPersistent);
    const bool is_cluster_scope = (object_name.scope == CustomVariableName::Scope::Cluster);
    if (is_cluster_scope)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DROP CLUSTER VARIABLE is not yet implemented");
    if (object_name.scope != CustomVariableName::Scope::Local && !is_session_scope && !is_local_persistent)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Only local, local_persistent, or session variables are supported in this phase");

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::DROP_VARIABLE);

    auto current_context = getContext();

    if (!drop_query.cluster.empty())
    {
        if (is_session_scope)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "ON CLUSTER is not supported for session variables");
        DDLQueryOnClusterParams params;
        params.access_to_check = std::move(access_rights_elements);
        return executeDDLQueryOnCluster(query_ptr, current_context, params);
    }

    current_context->checkAccess(access_rights_elements);

    bool throw_if_not_exists = !drop_query.if_exists;

    if (!is_session_scope)
    {
        auto & storage = current_context->getCustomVariablesDefinitionsStorage();
        if (!storage.removeObject(current_context, object_name, throw_if_not_exists))
            return {};

        current_context->getCustomVariablesManager().removeEntry(object_name);

        if (is_local_persistent)
        {
            try
            {
                current_context->getCustomVariablesValuesStorage().removeValue(object_name.name);
            }
            catch (...)
            {
                tryLogCurrentException(
                    getLogger("InterpreterDropVariableQuery"),
                    fmt::format("while removing persisted value for custom variable '{}'", object_name.fullName()));
            }
        }
    }
    else
    {
        auto & manager = current_context->getSessionCustomVariablesManager();
        if (!manager.removeEntry(object_name))
        {
            if (throw_if_not_exists)
                throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Custom variable '{}' doesn't exist", object_name.fullName());
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
