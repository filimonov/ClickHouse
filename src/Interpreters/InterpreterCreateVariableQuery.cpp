#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterCreateVariableQuery.h>

#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Interpreters/CustomVariablesManager.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Parsers/ASTCreateVariableQuery.h>
#include <Parsers/ASTIdentifier.h>

#include <chrono>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_QUERY;
    extern const int NOT_IMPLEMENTED;
}

namespace
{
CustomVariableName getCustomVariableName(const ASTPtr & ast)
{
    const auto * identifier = ast ? ast->as<ASTIdentifier>() : nullptr;
    if (!identifier || identifier->name_parts.size() != 2)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Custom variable name must be specified as scope.name");

    const auto & scope = identifier->name_parts[0];
    const auto & name = identifier->name_parts[1];
    if (scope.empty() || name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Custom variable name must be specified as scope.name");

    return CustomVariableName{scope, name};
}
}

BlockIO InterpreterCreateVariableQuery::execute()
{
    const auto & create_query = query_ptr->as<ASTCreateVariableQuery &>();
    auto object_name = getCustomVariableName(create_query.variable_name);

    if (object_name.scope != "local")
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Only local variables are supported in this phase");

    if (create_query.refresh_strategy)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "REFRESH is not supported for custom variables yet");

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::CREATE_VARIABLE);
    if (create_query.or_replace)
        access_rights_elements.emplace_back(AccessType::DROP_VARIABLE);

    auto current_context = getContext();

    if (!create_query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        params.access_to_check = std::move(access_rights_elements);
        return executeDDLQueryOnCluster(query_ptr, current_context, params);
    }

    current_context->checkAccess(access_rights_elements);

    bool throw_if_exists = !create_query.if_not_exists && !create_query.or_replace;
    bool replace_if_exists = create_query.or_replace;

    auto & storage = current_context->getCustomVariablesDefinitionsStorage();
    if (!storage.storeObject(
            current_context,
            object_name,
            query_ptr,
            throw_if_exists,
            replace_if_exists,
            current_context->getSettingsRef()))
    {
        return {};
    }

    CustomVariablesManager::Definition definition;
    definition.key = object_name;
    definition.expression = create_query.expression;
    definition.refresh_strategy = create_query.refresh_strategy;
    definition.declared_type = nullptr;
    definition.create_time = std::chrono::system_clock::now();

    auto entry = std::make_shared<CustomVariablesManager::Entry>();
    entry->definition = std::move(definition);
    current_context->getCustomVariablesManager().setEntry(object_name, std::move(entry));

    return {};
}

void registerInterpreterCreateVariableQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCreateVariableQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterCreateVariableQuery", create_fn);
}

}
