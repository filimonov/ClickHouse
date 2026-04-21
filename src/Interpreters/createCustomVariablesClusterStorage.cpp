#include <Interpreters/createCustomVariablesClusterStorage.h>

#include <Interpreters/Context.h>

#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

CustomVariablesClusterStoragePtr createCustomVariablesClusterStorage(const ContextMutablePtr & global_context)
{
    const String zk_path_key = "custom_variables_zookeeper_path";
    const auto & config = global_context->getConfigRef();
    if (!config.has(zk_path_key))
        return nullptr;

    String path = config.getString(zk_path_key);
    if (path.empty())
        return nullptr;

    return std::make_shared<CustomVariablesClusterStorage>(global_context, path);
}

}
