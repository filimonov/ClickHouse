#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterCreateVariableQuery.h>

#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Interpreters/CustomVariableValueSnapshot.h>
#include <Interpreters/CustomVariablesClusterStorage.h>
#include <Interpreters/CustomVariablesEvaluator.h>
#include <Interpreters/CustomVariablesManager.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Parsers/ASTCreateVariableQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTRefreshStrategy.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ParserCreateVariableQuery.h>
#include <Parsers/parseQuery.h>

#include <Core/Settings.h>

#include <Common/FailPoint.h>
#include <Common/logger_useful.h>
#include <Common/ZooKeeper/ZooKeeperLock.h>
#include <DataTypes/Utils.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <boost/make_shared.hpp>

#include <base/getFQDNOrHostName.h>

#include <chrono>
#include <optional>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int FILE_ALREADY_EXISTS;
    extern const int INCORRECT_QUERY;
    extern const int NOT_IMPLEMENTED;
}

namespace FailPoints
{
    extern const char custom_variable_create_after_publish_pause[];
}

namespace
{
/// Extracts the bare identifier name. The DDL grammar uses ParserIdentifier so
/// `variable_name` is always a single-segment identifier by construction.
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

BlockIO InterpreterCreateVariableQuery::execute()
{
    const auto & create_query = query_ptr->as<ASTCreateVariableQuery &>();
    auto bare_name = getBareVariableName(create_query.variable_name);
    const CustomVariableKind kind = create_query.kind;
    CustomVariableName object_name{kind, bare_name};

    const bool is_temporary = (kind == CustomVariableKind::Temporary);
    const bool is_replicated = (kind == CustomVariableKind::Replicated);
    const bool is_server = (kind == CustomVariableKind::Server);

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::CREATE_VARIABLE);
    if (create_query.or_replace)
        access_rights_elements.emplace_back(AccessType::DROP_VARIABLE);

    auto current_context = getContext();

    if (!create_query.cluster.empty())
    {
        /// Grammar guarantees only the server kind carries an ON CLUSTER clause.
        chassert(is_server);
        DDLQueryOnClusterParams params;
        params.access_to_check = std::move(access_rights_elements);
        return executeDDLQueryOnCluster(query_ptr, current_context, params);
    }

    current_context->checkAccess(access_rights_elements);

    if (create_query.refresh_strategy)
    {
        /// Grammar guarantees temporary doesn't carry REFRESH; this handles the remaining
        /// sub-clauses (APPEND, DEPENDS ON) that come from the shared MV refresh grammar
        /// but aren't meaningful for variables.
        const auto * refresh = create_query.refresh_strategy->as<ASTRefreshStrategy>();
        if (!refresh)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid refresh strategy");
        if (refresh->append)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "APPEND is not supported for custom variables");
        if (refresh->dependencies)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "DEPENDS ON is not supported for custom variables");
        if (isCustomVariableExpressionConstant(create_query.expression, current_context))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "REFRESH is not allowed for constant custom variable expressions");
    }

    CustomVariablesManager * global_manager = is_temporary ? nullptr : &current_context->getCustomVariablesManager();
    TemporaryVariables * temp_manager = is_temporary ? &current_context->getSessionCustomVariablesManager() : nullptr;

    auto has_existing_entry = [&]() -> bool
    {
        return is_temporary ? temp_manager->hasEntry(object_name.name) : global_manager->hasEntry(object_name);
    };

    auto get_existing_entry = [&]() -> CustomVariablesManager::EntryPtr
    {
        return is_temporary ? temp_manager->tryGetEntry(object_name.name) : global_manager->tryGetEntry(object_name);
    };

    /// Check in-memory map first for already-known entries so duplicate CREATE / IF NOT EXISTS
    /// short-circuits without evaluating the expression. Durable stores still run their own
    /// atomic checks below for crash-safety.
    if (!create_query.or_replace)
    {
        if (has_existing_entry())
        {
            if (create_query.if_not_exists)
                return {};
            throw Exception(
                ErrorCodes::FILE_ALREADY_EXISTS,
                "{} variable '{}' already exists",
                kindDisplayName(kind),
                object_name.name);
        }
    }

    auto evaluated = evaluateCustomVariableExpression(create_query.expression, current_context);

    DataTypePtr declared_type = evaluated.type;

    /// For server and temporary kinds, the local cache is authoritative; check type
    /// compatibility against it up front. For the replicated kind the local cache can
    /// be stale (node just started, behind the coordinator, racing with discovery),
    /// so the OR REPLACE type check has to run against the canonical Keeper state
    /// under the publish lock — see the replicated branch below.
    if (create_query.or_replace && !is_replicated)
    {
        if (auto existing = get_existing_entry())
        {
            if (existing->definition.declared_type)
            {
                if (!canBeSafelyCast(evaluated.type, existing->definition.declared_type))
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Cannot replace {} variable '{}' because expression type {} is not compatible with existing type {}",
                        kindDisplayName(kind),
                        object_name.name,
                        evaluated.type->getName(),
                        existing->definition.declared_type->getName());

                declared_type = existing->definition.declared_type;
            }
        }
    }

    Field value_field = std::move(evaluated.value);
    checkCustomVariableSize(value_field);

    bool throw_if_exists = !create_query.if_not_exists && !create_query.or_replace;
    bool replace_if_exists = create_query.or_replace;

    auto build_stored_query = [&](const DataTypePtr & final_type) -> ASTPtr
    {
        auto query = query_ptr->clone();
        auto & stored = query->as<ASTCreateVariableQuery &>();
        ASTPtr expression_to_cast = stored.expression->clone();
        /// CAST argument is an ASTFunction arg and won't be auto-parenthesized when formatted;
        /// wrap bare SELECTs in a subquery so the persisted DDL round-trips through the parser.
        if (expression_to_cast->as<ASTSelectWithUnionQuery>() || expression_to_cast->as<ASTSelectQuery>())
            expression_to_cast = std::make_shared<ASTSubquery>(std::move(expression_to_cast));
        stored.expression = addTypeConversionToAST(std::move(expression_to_cast), final_type->getName());
        stored.children.clear();
        stored.children.push_back(stored.variable_name);
        stored.children.push_back(stored.expression);
        if (stored.refresh_strategy)
            stored.children.push_back(stored.refresh_strategy);
        return query;
    };

    std::unique_ptr<zkutil::ZooKeeperLock> cluster_publish_lock;
    CustomVariablesClusterStoragePtr cluster_storage;
    std::optional<String> previous_cluster_definition;
    bool cluster_definition_written = false;
    if (is_replicated)
    {
        cluster_storage = current_context->getCustomVariablesClusterStorage();
        if (!cluster_storage)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Replicated custom variables require <custom_variables_zookeeper_path> in the server config");

        /// OR REPLACE rewrites two znodes (definition and value). Without serialisation
        /// two concurrent replacers on different nodes could end up with writer A's
        /// definition next to writer B's value. Take the per-variable ephemeral lock
        /// around the whole publish so only one OR REPLACE (or CREATE) is in flight.
        /// Plain CREATE doesn't strictly need this — ZK's atomic create is enough
        /// to serialise conflicting CREATEs — but taking the lock unconditionally
        /// keeps the code simpler and doesn't meaningfully slow common cases.
        cluster_publish_lock = cluster_storage->tryLockForRefresh(object_name.name, getFQDNOrHostName());
        if (!cluster_publish_lock)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Another replica is currently creating or refreshing replicated variable '{}'; retry",
                object_name.name);

        if (replace_if_exists)
            previous_cluster_definition = cluster_storage->tryLoadDefinition(object_name.name);

        /// For OR REPLACE, run the type-compatibility check against the canonical
        /// Keeper definition while we hold the publish lock. This covers the case
        /// where the local cache had no entry (fresh node, stale watch) but Keeper
        /// does — without this, a replace could change the declared type out from
        /// under other replicas and break their type-converted last-good values.
        if (replace_if_exists && previous_cluster_definition)
        {
            try
            {
                ParserCreateVariableQuery parser;
                ASTPtr prev_ast = parseQuery(
                    parser,
                    previous_cluster_definition->data(),
                    previous_cluster_definition->data() + previous_cluster_definition->size(),
                    "",
                    0,
                    current_context->getSettingsRef()[Setting::max_parser_depth],
                    current_context->getSettingsRef()[Setting::max_parser_backtracks]);
                const auto * prev_create = prev_ast ? prev_ast->as<ASTCreateVariableQuery>() : nullptr;
                if (prev_create && prev_create->expression)
                {
                    auto prev_type = getCustomVariableExpressionType(prev_create->expression, current_context);
                    if (prev_type)
                    {
                        if (!canBeSafelyCast(evaluated.type, prev_type))
                            throw Exception(
                                ErrorCodes::BAD_ARGUMENTS,
                                "Cannot replace replicated variable '{}' because expression type {} is not compatible with existing type {}",
                                object_name.name,
                                evaluated.type->getName(),
                                prev_type->getName());
                        declared_type = prev_type;
                    }
                }
            }
            catch (const Exception & e)
            {
                /// Re-throw our own type-compat error; swallow other parse/analysis
                /// issues — on a parse failure, let the replace proceed with the
                /// new type rather than pinning an unparseable legacy definition.
                if (e.code() == ErrorCodes::BAD_ARGUMENTS)
                    throw;
                tryLogCurrentException(
                    getLogger("InterpreterCreateVariableQuery"),
                    fmt::format("while parsing previous Keeper definition for replicated variable '{}'", object_name.name));
            }
        }

        if (!declared_type->equals(*evaluated.type))
            value_field = convertFieldToType(value_field, *declared_type);

        auto stored_query_for_keeper = build_stored_query(declared_type);
        WriteBufferFromOwnString ddl_buf;
        IAST::FormatSettings format_settings(/*one_line=*/false);
        stored_query_for_keeper->format(ddl_buf, format_settings);
        if (!cluster_storage->storeDefinition(object_name.name, ddl_buf.str(), throw_if_exists, replace_if_exists))
            return {};
        cluster_definition_written = true;
    }
    else
    {
        /// Server/temporary: declared_type is already final (set above); convert
        /// value to it before the storage layer sees it.
        if (!declared_type->equals(*evaluated.type))
            value_field = convertFieldToType(value_field, *declared_type);
    }

    auto stored_query = build_stored_query(declared_type);
    auto & stored_create_query = stored_query->as<ASTCreateVariableQuery &>();

    if (is_server)
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
    else if (is_temporary)
    {
        /// Temporary: no durable storage. Second-chance in-memory existence check for the
        /// race where OR REPLACE was requested but the entry wasn't present at the earlier
        /// check; if it appeared meanwhile, setEntry below will simply replace it.
        if (temp_manager->hasEntry(object_name.name))
        {
            if (throw_if_exists)
                throw Exception(
                    ErrorCodes::FILE_ALREADY_EXISTS,
                    "{} variable '{}' already exists",
                    kindDisplayName(kind),
                    object_name.name);
            if (!replace_if_exists)
                return {};
        }
    }

    CustomVariablesManager::Definition definition;
    definition.key = object_name;
    definition.expression = stored_create_query.expression;
    definition.refresh_strategy = create_query.refresh_strategy;
    definition.declared_type = declared_type;
    definition.load_time = std::chrono::system_clock::now();

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

    if (is_temporary)
    {
        temp_manager->setEntry(object_name.name, entry);
        FailPointInjection::pauseFailPoint(FailPoints::custom_variable_create_after_publish_pause);
        return {};
    }

    global_manager->prepareRefreshIfNeeded(current_context, entry);

    if (is_replicated)
    {
        try
        {
            cluster_storage->storeValue(object_name.name, *value);
        }
        catch (...)
        {
            if (cluster_definition_written)
            {
                try
                {
                    if (previous_cluster_definition)
                        cluster_storage->storeDefinition(object_name.name, *previous_cluster_definition, false, true);
                    else
                        cluster_storage->removeDefinition(object_name.name, false);
                }
                catch (...)
                {
                    tryLogCurrentException(
                        getLogger("InterpreterCreateVariableQuery"),
                        fmt::format("while rolling back failed CREATE REPLICATED VARIABLE '{}'", object_name.name));
                }
            }
            throw;
        }

        /// Value is already published in Keeper. Install in-memory entry without
        /// re-persisting through best-effort path.
        global_manager->setEntry(nullptr, object_name, entry);
    }
    else
    {
        global_manager->setEntry(current_context, object_name, entry);
    }

    FailPointInjection::pauseFailPoint(FailPoints::custom_variable_create_after_publish_pause);
    global_manager->schedulePreparedRefresh(entry);

    /// Nudge the coordinator thread to pick up the new entry without waiting for ZK echo
    /// (the initial value was already written to ZK in the CREATE path above).
    if (is_replicated)
        current_context->getCustomVariablesManager().pokeClusterCoordinator(object_name.name);

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
