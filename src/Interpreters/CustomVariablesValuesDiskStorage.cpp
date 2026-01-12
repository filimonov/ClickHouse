#include <Interpreters/CustomVariablesValuesDiskStorage.h>

#include <Common/FieldBinaryEncoding.h>
#include <Common/Exception.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>

#include <Core/Settings.h>

#include <DataTypes/DataTypeFactory.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>

#include <Interpreters/Context.h>

#include <base/types.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace Setting
{
    extern const SettingsBool fsync_metadata;
}

namespace ErrorCodes
{
    extern const int DIRECTORY_DOESNT_EXIST;
}

namespace
{
constexpr std::string_view file_suffix = ".bin";
constexpr UInt64 format_version = 1;

void writeTimePoint(WriteBuffer & out, std::chrono::system_clock::time_point timepoint)
{
    const auto seconds = std::chrono::duration_cast<std::chrono::seconds>(timepoint.time_since_epoch()).count();
    writeBinary(static_cast<Int64>(seconds), out);
}

std::chrono::system_clock::time_point readTimePoint(ReadBuffer & in)
{
    Int64 seconds = 0;
    readBinary(seconds, in);
    return std::chrono::system_clock::time_point{std::chrono::seconds(seconds)};
}

void writeSnapshot(const CustomVariableValueSnapshot & snapshot, WriteBuffer & out)
{
    writeBinary(format_version, out);
    writeStringBinary(snapshot.runtime_type ? snapshot.runtime_type->getName() : String{}, out);
    writeBinary(static_cast<UInt8>(snapshot.has_value), out);
    if (snapshot.has_value)
        encodeField(snapshot.value, out);
    writeBinary(static_cast<UInt8>(snapshot.is_valid), out);
    writeTimePoint(out, snapshot.last_update_time);
    writeTimePoint(out, snapshot.last_successful_update_time);
    writeStringBinary(snapshot.last_update_hostname, out);
    writeStringBinary(snapshot.last_error, out);
    writeStringBinary(snapshot.last_error_type, out);
}

std::optional<CustomVariableValueSnapshot> readSnapshot(ReadBuffer & in)
{
    UInt64 version = 0;
    readBinary(version, in);
    if (version != format_version)
        return std::nullopt;

    CustomVariableValueSnapshot snapshot;
    String type_name;
    readStringBinary(type_name, in);
    if (!type_name.empty())
        snapshot.runtime_type = DataTypeFactory::instance().get(type_name);

    UInt8 has_value = 0;
    readBinary(has_value, in);
    snapshot.has_value = (has_value != 0);
    if (snapshot.has_value)
        snapshot.value = decodeField(in);

    UInt8 is_valid = 0;
    readBinary(is_valid, in);
    snapshot.is_valid = (is_valid != 0);
    snapshot.last_update_time = readTimePoint(in);
    snapshot.last_successful_update_time = readTimePoint(in);
    readStringBinary(snapshot.last_update_hostname, in);
    readStringBinary(snapshot.last_error, in);
    readStringBinary(snapshot.last_error_type, in);
    return snapshot;
}

String makeDirectoryPathCanonical(const String & directory_path)
{
    auto canonical_directory_path = std::filesystem::weakly_canonical(directory_path);
    if (canonical_directory_path.has_filename())
        canonical_directory_path += std::filesystem::path::preferred_separator;
    return canonical_directory_path;
}
}

CustomVariablesValuesDiskStorage::CustomVariablesValuesDiskStorage(const ContextPtr & global_context_, const String & dir_path_)
    : dir_path(makeDirectoryPathCanonical(dir_path_))
    , log(getLogger("CustomVariablesValuesDiskStorage"))
    , global_context(global_context_)
{
}

void CustomVariablesValuesDiskStorage::createDirectory() const
{
    std::error_code create_dir_error_code;
    fs::create_directories(dir_path, create_dir_error_code);
    if (!fs::exists(dir_path) || !fs::is_directory(dir_path) || create_dir_error_code)
        throw Exception(
            ErrorCodes::DIRECTORY_DOESNT_EXIST,
            "Couldn't create directory {} reason: '{}'",
            dir_path,
            create_dir_error_code.message());
}

String CustomVariablesValuesDiskStorage::getFilePath(const String & name) const
{
    return dir_path + escapeForFileName(name) + String(file_suffix);
}

std::optional<CustomVariableValueSnapshot> CustomVariablesValuesDiskStorage::tryLoadValue(const String & name) const
{
    const String file_path = getFilePath(name);
    LOG_DEBUG(log, "Loading custom variable value for {} from {}", name, file_path);

    if (!fs::exists(file_path))
        return std::nullopt;

    try
    {
        ReadBufferFromFile in(file_path);
        auto snapshot = readSnapshot(in);
        if (!snapshot)
            LOG_WARNING(log, "Unsupported custom variable value format in {}", file_path);
        return snapshot;
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("while loading custom variable value from {}", file_path));
        return std::nullopt;
    }
}

void CustomVariablesValuesDiskStorage::storeValue(
    const String & name,
    const CustomVariableValueSnapshot & snapshot,
    const Settings & settings) const
{
    createDirectory();
    const String file_path = getFilePath(name);
    const String temp_file_path = file_path + ".tmp";
    LOG_DEBUG(log, "Storing custom variable value for {} to {}", name, file_path);

    try
    {
        WriteBufferFromFile out(temp_file_path);
        writeSnapshot(snapshot, out);
        out.next();
        if (settings[Setting::fsync_metadata])
            out.sync();
        out.close();

        fs::rename(temp_file_path, file_path);
    }
    catch (...)
    {
        fs::remove(temp_file_path);
        throw;
    }
}

void CustomVariablesValuesDiskStorage::removeValue(const String & name) const
{
    const String file_path = getFilePath(name);
    LOG_DEBUG(log, "Removing custom variable value for {} from {}", name, file_path);
    fs::remove(file_path);
}

}
