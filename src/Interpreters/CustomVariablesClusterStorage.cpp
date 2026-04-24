#include <Interpreters/CustomVariablesClusterStorage.h>

#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperLock.h>

#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <Interpreters/Context.h>

#include <base/getFQDNOrHostName.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_ALREADY_EXISTS;
    extern const int FILE_DOESNT_EXIST;
    extern const int KEEPER_EXCEPTION;
    extern const int NO_ZOOKEEPER;
}

namespace FailPoints
{
    extern const char custom_variables_replicated_store_value_fail_once[];
}

namespace
{
zkutil::GetZooKeeper makeGetZooKeeper(const ContextPtr & context)
{
    std::weak_ptr<const Context> weak_ctx = context;
    return [weak_ctx]() -> zkutil::ZooKeeperPtr
    {
        auto locked = weak_ctx.lock();
        if (!locked)
            throw Exception(ErrorCodes::NO_ZOOKEEPER, "Global context for replicated variables storage is gone");
        return locked->getZooKeeper();
    };
}
}

CustomVariablesClusterStorage::CustomVariablesClusterStorage(const ContextPtr & global_context_, const String & zookeeper_root_)
    : global_context(global_context_)
    , zookeeper_root(zookeeper_root_)
    , zookeeper_getter(makeGetZooKeeper(global_context_))
    , log(getLogger("CustomVariablesClusterStorage"))
{
    while (!zookeeper_root.empty() && zookeeper_root.back() == '/')
        zookeeper_root.pop_back();
    if (zookeeper_root.empty())
        zookeeper_root = "/";
}

zkutil::ZooKeeperPtr CustomVariablesClusterStorage::getZooKeeper()
{
    auto [zookeeper, session_status] = zookeeper_getter.getZooKeeper();
    if (session_status == zkutil::ZooKeeperCachingGetter::SessionStatus::New)
        createRootNodesIfNeeded();
    return zookeeper;
}

void CustomVariablesClusterStorage::createRootNodesIfNeeded()
{
    auto [zookeeper, _] = zookeeper_getter.getZooKeeper();
    zookeeper->createAncestors(zookeeper_root);
    zookeeper->createIfNotExists(zookeeper_root, "");
    zookeeper->createIfNotExists(definitionsPath(), "");
    zookeeper->createIfNotExists(valuesPath(), "");
}

String CustomVariablesClusterStorage::definitionPath(const String & escaped_name) const
{
    return definitionsPath() + "/" + escaped_name;
}

String CustomVariablesClusterStorage::valuePath(const String & escaped_name) const
{
    return valuesPath() + "/" + escaped_name;
}

String CustomVariablesClusterStorage::lockPath(const String & escaped_name) const
{
    return valuePath(escaped_name) + "/lock";
}

std::optional<String> CustomVariablesClusterStorage::tryLoadDefinition(const String & name)
{
    createRootNodesIfNeeded();
    auto zookeeper = getZooKeeper();
    const auto path = definitionPath(escapeForFileName(name));
    String data;
    if (!zookeeper->tryGet(path, data))
        return std::nullopt;
    return data;
}

bool CustomVariablesClusterStorage::storeDefinition(
    const String & name,
    const String & create_query_text,
    bool throw_if_exists,
    bool replace_if_exists)
{
    createRootNodesIfNeeded();
    auto zookeeper = getZooKeeper();
    const auto path = definitionPath(escapeForFileName(name));

    Coordination::Error code = zookeeper->tryCreate(path, create_query_text, zkutil::CreateMode::Persistent);
    if (code == Coordination::Error::ZOK)
        return true;
    if (code != Coordination::Error::ZNODEEXISTS)
        throw zkutil::KeeperException::fromPath(code, path);

    if (throw_if_exists)
        throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "Cluster custom variable '{}' already exists", name);
    if (!replace_if_exists)
        return false;

    zookeeper->set(path, create_query_text);
    return true;
}

bool CustomVariablesClusterStorage::removeDefinition(const String & name, bool throw_if_not_exists)
{
    createRootNodesIfNeeded();
    auto zookeeper = getZooKeeper();
    const auto path = definitionPath(escapeForFileName(name));
    Coordination::Error code = zookeeper->tryRemove(path);
    if (code == Coordination::Error::ZOK)
        return true;
    if (code == Coordination::Error::ZNONODE)
    {
        if (throw_if_not_exists)
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Cluster custom variable '{}' doesn't exist", name);
        return false;
    }
    throw zkutil::KeeperException::fromPath(code, path);
}

std::vector<String> CustomVariablesClusterStorage::listDefinitionNames()
{
    createRootNodesIfNeeded();
    auto zookeeper = getZooKeeper();
    Strings children = zookeeper->getChildren(definitionsPath());
    std::vector<String> names;
    names.reserve(children.size());
    for (const auto & escaped : children)
        names.push_back(unescapeForFileName(escaped));
    return names;
}

std::optional<CustomVariableValueSnapshot> CustomVariablesClusterStorage::tryLoadValue(const String & name)
{
    createRootNodesIfNeeded();
    auto zookeeper = getZooKeeper();
    const auto path = valuePath(escapeForFileName(name));
    String data;
    if (!zookeeper->tryGet(path, data))
        return std::nullopt;

    try
    {
        ReadBufferFromString in(data);
        return readCustomVariableValueSnapshot(in);
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("while decoding replicated variable value '{}' from {}", name, path));
        return std::nullopt;
    }
}

void CustomVariablesClusterStorage::storeValue(const String & name, const CustomVariableValueSnapshot & snapshot)
{
    fiu_do_on(FailPoints::custom_variables_replicated_store_value_fail_once,
        throw Exception(ErrorCodes::KEEPER_EXCEPTION, "Injected failure while storing replicated variable '{}'", name););

    createRootNodesIfNeeded();
    auto zookeeper = getZooKeeper();
    const auto path = valuePath(escapeForFileName(name));

    WriteBufferFromOwnString buf;
    writeCustomVariableValueSnapshot(snapshot, buf);
    const auto & data = buf.str();

    Coordination::Error code = zookeeper->tryCreate(path, data, zkutil::CreateMode::Persistent);
    if (code == Coordination::Error::ZOK)
        return;
    if (code != Coordination::Error::ZNODEEXISTS)
        throw zkutil::KeeperException::fromPath(code, path);

    zookeeper->set(path, data);
}

void CustomVariablesClusterStorage::removeValueRecursive(const String & name)
{
    createRootNodesIfNeeded();
    auto zookeeper = getZooKeeper();
    const auto path = valuePath(escapeForFileName(name));
    zookeeper->tryRemoveRecursive(path);
}

std::unique_ptr<zkutil::ZooKeeperLock> CustomVariablesClusterStorage::tryLockForRefresh(
    const String & name, const String & lock_holder_message)
{
    createRootNodesIfNeeded();
    auto zookeeper = getZooKeeper();
    const auto value_path = valuePath(escapeForFileName(name));
    /// Ensure parent exists so we can put an ephemeral child under it.
    zookeeper->createIfNotExists(value_path, "");

    auto lock = std::make_unique<zkutil::ZooKeeperLock>(
        zookeeper,
        value_path,
        "lock",
        lock_holder_message.empty() ? getFQDNOrHostName() : lock_holder_message,
        /*throw_if_lost=*/ false);

    if (!lock->tryLock())
        return nullptr;
    return lock;
}

}
