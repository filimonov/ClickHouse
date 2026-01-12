#pragma once

#include <Interpreters/ICustomVariablesDefinitionsStorage.h>

#include <Common/Logger.h>

#include <optional>

namespace DB
{

class CustomVariablesDefinitionsDiskStorage final : public ICustomVariablesDefinitionsStorage
{
public:
    CustomVariablesDefinitionsDiskStorage(const ContextPtr & global_context_, const String & dir_path_);

    Objects loadObjects() override;

    bool storeObject(
        const ContextPtr & current_context,
        const ObjectName & object_name,
        ASTPtr create_query,
        bool throw_if_exists,
        bool replace_if_exists,
        const Settings & settings) override;

    bool removeObject(
        const ContextPtr & current_context,
        const ObjectName & object_name,
        bool throw_if_not_exists) override;

private:
    String dir_path;
    LoggerPtr log;
    ContextPtr global_context;

    void createDirectory();

    String getFilePath(const ObjectName & object_name) const;
    std::optional<ObjectName> parseFileName(const String & file_name) const;
};

}
