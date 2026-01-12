#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterCreateVariableQuery.h>

#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Interpreters/CustomVariablesEvaluator.h>
#include <Interpreters/CustomVariablesManager.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Parsers/ASTCreateVariableQuery.h>
#include <Parsers/ASTIdentifier.h>

#include <DataTypes/Utils.h>
#include <boost/make_shared.hpp>

#include <base/getFQDNOrHostName.h>

#include <chrono>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int FILE_ALREADY_EXISTS;
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

    const bool is_session_scope = (object_name.scope == "session");
    if (object_name.scope != "local" && !is_session_scope)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Only local or session variables are supported in this phase");

    if (create_query.refresh_strategy)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "REFRESH is not supported for custom variables yet");

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::CREATE_VARIABLE);
    if (create_query.or_replace)
        access_rights_elements.emplace_back(AccessType::DROP_VARIABLE);

    auto current_context = getContext();

    if (!create_query.cluster.empty())
    {
        if (is_session_scope)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "ON CLUSTER is not supported for session variables");
        DDLQueryOnClusterParams params;
        params.access_to_check = std::move(access_rights_elements);
        return executeDDLQueryOnCluster(query_ptr, current_context, params);
    }

    current_context->checkAccess(access_rights_elements);

    auto evaluated = evaluateCustomVariableExpression(create_query.expression, current_context);

    DataTypePtr declared_type = evaluated.type;
    CustomVariablesManager * manager = nullptr;
    if (is_session_scope)
        manager = &current_context->getSessionCustomVariablesManager();
    else
        manager = &current_context->getCustomVariablesManager();

    if (create_query.or_replace)
    {
        if (auto existing = manager->tryGetEntry(object_name))
        {
            if (existing->definition.declared_type)
            {
                if (!canBeSafelyCast(evaluated.type, existing->definition.declared_type))
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Cannot replace custom variable '{}' because expression type {} is not compatible with existing type {}",
                        object_name.fullName(),
                        evaluated.type->getName(),
                        existing->definition.declared_type->getName());

                declared_type = existing->definition.declared_type;
            }
        }
    }

    Field value_field = std::move(evaluated.value);
    if (!declared_type->equals(*evaluated.type))
        value_field = convertFieldToType(value_field, *declared_type);

    checkCustomVariableSize(value_field);

    bool throw_if_exists = !create_query.if_not_exists && !create_query.or_replace;
    bool replace_if_exists = create_query.or_replace;

    auto stored_query = query_ptr->clone();
    auto & stored_create_query = stored_query->as<ASTCreateVariableQuery &>();
    stored_create_query.expression = addTypeConversionToAST(stored_create_query.expression->clone(), declared_type->getName());
    stored_create_query.children.clear();
    stored_create_query.children.push_back(stored_create_query.variable_name);
    stored_create_query.children.push_back(stored_create_query.expression);
    if (stored_create_query.refresh_strategy)
        stored_create_query.children.push_back(stored_create_query.refresh_strategy);

    if (!is_session_scope)
    {
        auto & storage = current_context->getCustomVariablesDefinitionsStorage();
        if (!storage.storeObject(
                current_context,
                object_name,
                stored_query,
                throw_if_exists,
                replace_if_exists,
                current_context->getSettingsRef()))
        {
            return {};
        }
    }
    else
    {
        if (manager->hasEntry(object_name))
        {
            if (throw_if_exists)
                throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "Custom variable '{}' already exists", object_name.fullName());
            if (!replace_if_exists)
                return {};
        }
    }

    CustomVariablesManager::Definition definition;
    definition.key = object_name;
    definition.expression = stored_create_query.expression;
    definition.refresh_strategy = create_query.refresh_strategy;
    definition.declared_type = declared_type;
    definition.create_time = std::chrono::system_clock::now();

    auto entry = std::make_shared<CustomVariablesManager::Entry>();
    entry->definition = std::move(definition);

    auto value = boost::make_shared<CustomVariablesManager::Value>();
    value->runtime_type = declared_type;
    value->value = std::move(value_field);
    value->last_update_time = std::chrono::system_clock::now();
    value->last_successful_update_time = value->last_update_time;
    value->last_update_hostname = getFQDNOrHostName();
    value->has_value = true;
    value->is_valid = true;
    entry->value.store(boost::static_pointer_cast<const CustomVariablesManager::Value>(value));

    manager->setEntry(object_name, std::move(entry));

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
