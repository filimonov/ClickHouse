#include <Interpreters/createCustomVariablesValuesStorage.h>

#include <Interpreters/CustomVariablesValuesDiskStorage.h>
#include <Interpreters/Context.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <base/types.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

std::unique_ptr<CustomVariablesValuesDiskStorage> createCustomVariablesValuesStorage(const ContextMutablePtr & global_context)
{
    const String disk_path_key = "custom_variables_values_path";

    const auto & config = global_context->getConfigRef();
    String default_path = fs::path{global_context->getPath()} / "custom_variables_values" / "";
    String path = config.getString(disk_path_key, default_path);
    return std::make_unique<CustomVariablesValuesDiskStorage>(global_context, path);
}

}
