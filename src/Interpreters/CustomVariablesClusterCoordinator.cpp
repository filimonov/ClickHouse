#include <Interpreters/CustomVariablesClusterCoordinator.h>

#include <Common/Exception.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <base/sleep.h>

#include <Core/Settings.h>

#include <IO/ReadBufferFromString.h>

#include <Interpreters/Context.h>
#include <Interpreters/CustomVariableValueSnapshot.h>
#include <Interpreters/CustomVariablesEvaluator.h>
#include <Interpreters/CustomVariablesManager.h>
#include <Interpreters/convertFieldToType.h>

#include <Parsers/ASTCreateVariableQuery.h>
#include <Parsers/ParserCreateVariableQuery.h>
#include <Parsers/parseQuery.h>

#include <boost/make_shared.hpp>

#include <limits>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_parser_backtracks;
}

namespace
{
/// Re-enumerate sentinel; no variable name can be empty.
const String RESYNC_ALL{};

String makeWatchId(const String & prefix, const String & name)
{
    return fmt::format("CustomVariablesClusterCoordinator({}/{})", prefix, name);
}
}

CustomVariablesClusterCoordinator::CustomVariablesClusterCoordinator(
    ContextPtr global_context_,
    CustomVariablesClusterStoragePtr storage_,
    CustomVariablesManager & manager_)
    : global_context(std::move(global_context_))
    , storage(std::move(storage_))
    , manager(manager_)
    , log(getLogger("CustomVariablesClusterCoordinator"))
    , queue(std::make_shared<ConcurrentBoundedQueue<String>>(std::numeric_limits<size_t>::max()))
{
}

CustomVariablesClusterCoordinator::~CustomVariablesClusterCoordinator()
{
    stop();
}

void CustomVariablesClusterCoordinator::start()
{
    if (running.exchange(true))
        return;
    thread = ThreadFromGlobalPool(&CustomVariablesClusterCoordinator::watchLoop, this);
}

void CustomVariablesClusterCoordinator::stop()
{
    if (!running.exchange(false))
        return;
    queue->finish();
    if (thread.joinable())
        thread.join();
}

void CustomVariablesClusterCoordinator::poke(const String & name)
{
    if (!running)
        return;
    [[maybe_unused]] bool inserted = queue->emplace(name);
}

void CustomVariablesClusterCoordinator::watchLoop()
{
    setThreadName(ThreadName::CUSTOM_VARIABLES_CLUSTER);
    LOG_DEBUG(log, "Cluster variables coordinator started");

    while (running)
    {
        try
        {
            if (!loaded)
            {
                initialLoad();
                loaded = true;
            }

            String name;
            if (!queue->tryPop(name, /* timeout_ms */ 10000))
                continue;

            if (name.empty())
                resyncAll();
            else
                refreshOne(name);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Cluster variables coordinator loop");
            /// Force re-init of ZK session / re-enumerate on next tick.
            loaded = false;
            sleepForSeconds(1);
        }
    }

    LOG_DEBUG(log, "Cluster variables coordinator stopped");
}

void CustomVariablesClusterCoordinator::initialLoad()
{
    Strings names = readDefinitionsAndInstallChildrenWatch();
    for (const auto & name : names)
        refreshOne(name);
}

void CustomVariablesClusterCoordinator::resyncAll()
{
    Strings names = readDefinitionsAndInstallChildrenWatch();
    std::unordered_set<String> present(names.begin(), names.end());

    /// Drop cluster entries that disappeared in ZK.
    auto entries = manager.getAllEntries();
    for (const auto & entry : entries)
    {
        const auto & key = entry->definition.key;
        if (key.scope == CustomVariableName::Scope::Cluster && !present.contains(key.name))
            manager.removeEntry(key);
    }

    for (const auto & name : names)
        refreshOne(name);
}

void CustomVariablesClusterCoordinator::refreshOne(const String & name)
{
    String definition_blob;
    if (!readDefinitionDataAndInstallWatch(name, definition_blob))
    {
        /// Definition gone — drop from manager if we had it.
        manager.removeEntry(CustomVariableName{CustomVariableName::Scope::Cluster, name});
        return;
    }

    ASTPtr ast;
    try
    {
        ParserCreateVariableQuery parser;
        ast = parseQuery(
            parser,
            definition_blob.data(),
            definition_blob.data() + definition_blob.size(),
            "",
            0,
            global_context->getSettingsRef()[Setting::max_parser_depth],
            global_context->getSettingsRef()[Setting::max_parser_backtracks]);
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("parsing cluster variable definition '{}'", name));
        return;
    }

    const auto * create_query = ast ? ast->as<ASTCreateVariableQuery>() : nullptr;
    if (!create_query)
    {
        LOG_WARNING(log, "Definition blob for cluster variable '{}' is not a CREATE VARIABLE query", name);
        return;
    }

    CustomVariablesManager::Definition definition;
    definition.key = CustomVariableName{CustomVariableName::Scope::Cluster, name};
    definition.expression = create_query->expression;
    definition.refresh_strategy = create_query->refresh_strategy;
    definition.load_time = std::chrono::system_clock::now();
    try
    {
        definition.declared_type = getCustomVariableExpressionType(definition.expression, global_context);
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("resolving declared type for cluster variable '{}'", name));
    }

    auto entry = std::make_shared<CustomVariablesManager::Entry>();
    entry->definition = std::move(definition);

    String value_blob;
    std::optional<CustomVariableValueSnapshot> snapshot;
    if (readValueDataAndInstallWatch(name, value_blob))
    {
        try
        {
            ReadBufferFromString rb(value_blob);
            snapshot = readCustomVariableValueSnapshot(rb);
        }
        catch (...)
        {
            tryLogCurrentException(log, fmt::format("decoding value for cluster variable '{}'", name));
        }
    }

    if (snapshot)
    {
        auto value = boost::make_shared<CustomVariablesManager::Value>();
        value->runtime_type = snapshot->runtime_type ? snapshot->runtime_type : entry->definition.declared_type;
        value->value = snapshot->value;
        value->last_update_time = snapshot->last_update_time;
        value->last_successful_update_time = snapshot->last_successful_update_time;
        value->last_update_hostname = snapshot->last_update_hostname;
        value->last_error = snapshot->last_error;
        value->last_error_type = snapshot->last_error_type;
        value->has_value = snapshot->has_value;
        value->is_valid = snapshot->is_valid;

        if (entry->definition.declared_type && value->runtime_type
            && !entry->definition.declared_type->equals(*value->runtime_type))
        {
            try
            {
                value->value = convertFieldToType(value->value, *entry->definition.declared_type);
                value->runtime_type = entry->definition.declared_type;
            }
            catch (...)
            {
                tryLogCurrentException(log, fmt::format("converting value type for cluster variable '{}'", name));
            }
        }

        entry->value.store(boost::static_pointer_cast<const CustomVariablesManager::Value>(value));
    }

    /// Pass nullptr for context so the manager does not attempt to persist back to disk
    /// (cluster entries are written to ZK by the CREATE interpreter / refresh path).
    manager.setEntry(nullptr, entry->definition.key, entry);
}

Strings CustomVariablesClusterCoordinator::readDefinitionsAndInstallChildrenWatch()
{
    storage->createRootNodesIfNeeded();
    auto zookeeper = storage->getZooKeeper();
    const String path = storage->getRoot() + "/definitions";

    auto watcher = zookeeper->createWatchFromRawCallback(makeWatchId("definitions", ""), [&] -> Coordination::WatchCallback
    {
        return [q = queue](const Coordination::WatchResponse &)
        {
            [[maybe_unused]] bool inserted = q->emplace(RESYNC_ALL);
        };
    });

    Coordination::Stat stat;
    Strings children = zookeeper->getChildrenWatch(path, &stat, watcher);

    Strings names;
    names.reserve(children.size());
    for (const auto & escaped : children)
    {
        auto n = unescapeForFileName(escaped);
        if (!n.empty())
            names.push_back(std::move(n));
    }
    return names;
}

bool CustomVariablesClusterCoordinator::readDefinitionDataAndInstallWatch(const String & name, String & out)
{
    auto zookeeper = storage->getZooKeeper();
    const String path = storage->getRoot() + "/definitions/" + escapeForFileName(name);

    auto watcher = zookeeper->createWatchFromRawCallback(makeWatchId("definition", name), [&] -> Coordination::WatchCallback
    {
        return [q = queue, n = name](const Coordination::WatchResponse & response)
        {
            if (response.type == Coordination::Event::CHANGED
                || response.type == Coordination::Event::DELETED)
            {
                [[maybe_unused]] bool inserted = q->emplace(n);
            }
        };
    });

    Coordination::Stat stat;
    return zookeeper->tryGetWatch(path, out, &stat, watcher);
}

bool CustomVariablesClusterCoordinator::readValueDataAndInstallWatch(const String & name, String & out)
{
    auto zookeeper = storage->getZooKeeper();
    const String path = storage->getRoot() + "/values/" + escapeForFileName(name);

    auto watcher = zookeeper->createWatchFromRawCallback(makeWatchId("value", name), [&] -> Coordination::WatchCallback
    {
        return [q = queue, n = name](const Coordination::WatchResponse & response)
        {
            if (response.type == Coordination::Event::CHANGED
                || response.type == Coordination::Event::CREATED
                || response.type == Coordination::Event::DELETED)
            {
                [[maybe_unused]] bool inserted = q->emplace(n);
            }
        };
    });

    Coordination::Stat stat;
    if (zookeeper->tryGetWatch(path, out, &stat, watcher))
        return true;

    /// znode does not exist yet (CREATE on another node may not have written the
    /// value znode when we observed the definition). Install an `exists` watch so
    /// we wake up as soon as the value znode appears.
    zookeeper->existsWatch(path, &stat, watcher);
    return false;
}

}
