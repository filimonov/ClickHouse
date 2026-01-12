#pragma once

#include <Common/logger_useful.h>
#include <Core/Field.h>
#include <base/types.h>

#include <chrono>
#include <memory>
#include <optional>

namespace DB
{

class Context;
class IDataType;
struct Settings;

using ContextPtr = std::shared_ptr<const Context>;
using DataTypePtr = std::shared_ptr<const IDataType>;

struct CustomVariableValueSnapshot
{
    DataTypePtr runtime_type;
    Field value;
    std::chrono::system_clock::time_point last_update_time;
    std::chrono::system_clock::time_point last_successful_update_time;
    String last_update_hostname;
    String last_error;
    String last_error_type;
    bool has_value = false;
    bool is_valid = false;
};

class CustomVariablesValuesDiskStorage
{
public:
    CustomVariablesValuesDiskStorage(const ContextPtr & global_context, const String & dir_path);

    std::optional<CustomVariableValueSnapshot> tryLoadValue(const String & name) const;
    void storeValue(const String & name, const CustomVariableValueSnapshot & snapshot, const Settings & settings) const;
    void removeValue(const String & name) const;

private:
    void createDirectory() const;
    String getFilePath(const String & name) const;

    String dir_path;
    LoggerPtr log;
    ContextPtr global_context;
};

}
