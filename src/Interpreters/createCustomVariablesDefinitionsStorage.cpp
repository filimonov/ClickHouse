#include <Interpreters/createCustomVariablesDefinitionsStorage.h>

#include <Interpreters/CustomVariablesDefinitionsDiskStorage.h>
#include <Interpreters/Context.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int NOT_IMPLEMENTED;
}

std::unique_ptr<ICustomVariablesDefinitionsStorage> createCustomVariablesDefinitionsStorage(const ContextMutablePtr & global_context)
{
    const String zookeeper_path_key = "custom_variables_definitions_zookeeper_path";
    const String disk_path_key = "custom_variables_path";

    const auto & config = global_context->getConfigRef();
    if (config.has(zookeeper_path_key))
    {
        if (config.has(disk_path_key))
        {
            throw Exception(
                ErrorCodes::INVALID_CONFIG_PARAMETER,
                "'{}' and '{}' must not be both specified in the config",
                zookeeper_path_key,
                disk_path_key);
        }

        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Custom variable definitions in ZooKeeper are not implemented yet");
    }

    String default_path = fs::path{global_context->getPath()} / "custom_variables" / "";
    String path = config.getString(disk_path_key, default_path);
    return std::make_unique<CustomVariablesDefinitionsDiskStorage>(global_context, path);
}

}
