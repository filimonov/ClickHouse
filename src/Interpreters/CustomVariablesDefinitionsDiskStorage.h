#pragma once

#include <Interpreters/CustomVariableKind.h>
#include <Interpreters/Context_fwd.h>

#include <Common/Logger.h>

#include <Parsers/IAST_fwd.h>

#include <optional>
#include <utility>
#include <vector>

namespace DB
{

struct Settings;

/// On-disk storage for server-kind variable definitions. Writes one `.sql`
/// file per variable under <custom_variables_path>. The replicated kind has
/// its own Keeper-backed storage and does not go through this class.
class CustomVariablesDefinitionsDiskStorage
{
public:
    using ObjectName = CustomVariableName;
    using Objects = std::vector<std::pair<ObjectName, ASTPtr>>;

    CustomVariablesDefinitionsDiskStorage(const ContextPtr & global_context_, const String & dir_path_);

    Objects loadObjects();

    bool storeObject(
        const ContextPtr & current_context,
        const ObjectName & object_name,
        ASTPtr create_query,
        bool throw_if_exists,
        bool replace_if_exists,
        const Settings & settings);

    bool removeObject(
        const ContextPtr & current_context,
        const ObjectName & object_name,
        bool throw_if_not_exists);

private:
    String dir_path;
    LoggerPtr log;
    ContextPtr global_context;

    void createDirectory();

    String getFilePath(const ObjectName & object_name) const;
    std::optional<ObjectName> parseFileName(const String & file_name) const;
};

}
