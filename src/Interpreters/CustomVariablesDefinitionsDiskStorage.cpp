#include <Interpreters/CustomVariablesDefinitionsDiskStorage.h>

#include <Common/StringUtils.h>
#include <Common/atomicRename.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>

#include <Core/Settings.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>

#include <Interpreters/Context.h>

#include <Parsers/IAST.h>
#include <Parsers/ParserCreateVariableQuery.h>
#include <Parsers/parseQuery.h>

#include <Poco/DirectoryIterator.h>
#include <Poco/Logger.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{
namespace Setting
{
    extern const SettingsBool fsync_metadata;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
}

namespace ErrorCodes
{
    extern const int DIRECTORY_DOESNT_EXIST;
    extern const int FILE_ALREADY_EXISTS;
    extern const int FILE_DOESNT_EXIST;
}

namespace
{
String makeDirectoryPathCanonical(const String & directory_path)
{
    auto canonical_directory_path = std::filesystem::weakly_canonical(directory_path);
    if (canonical_directory_path.has_filename())
        canonical_directory_path += std::filesystem::path::preferred_separator;
    return canonical_directory_path;
}

constexpr std::string_view file_prefix = "variable_";
constexpr std::string_view file_suffix = ".sql";

std::optional<std::pair<String, String>> splitScopeAndName(const String & stem)
{
    static constexpr std::string_view scopes[] = {"local_persistent", "local", "cluster", "session"};
    for (const auto & scope : scopes)
    {
        if (stem.starts_with(scope) && stem.size() > scope.size() && stem[scope.size()] == '_')
        {
            String name = stem.substr(scope.size() + 1);
            return std::pair<String, String>{String(scope), std::move(name)};
        }
    }
    return std::nullopt;
}
}

CustomVariablesDefinitionsDiskStorage::CustomVariablesDefinitionsDiskStorage(const ContextPtr & global_context_, const String & dir_path_)
    : dir_path(makeDirectoryPathCanonical(dir_path_))
    , log(getLogger("CustomVariablesDefinitionsDiskStorage"))
    , global_context(global_context_)
{
}

void CustomVariablesDefinitionsDiskStorage::createDirectory()
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

String CustomVariablesDefinitionsDiskStorage::getFilePath(const ObjectName & object_name) const
{
    return dir_path + String(file_prefix) + CustomVariableName::scopeToString(object_name.scope) + "_"
        + escapeForFileName(object_name.name) + String(file_suffix);
}

std::optional<CustomVariablesDefinitionsDiskStorage::ObjectName>
CustomVariablesDefinitionsDiskStorage::parseFileName(const String & file_name) const
{
    if (!file_name.starts_with(file_prefix) || !file_name.ends_with(file_suffix))
        return std::nullopt;

    size_t prefix_length = file_prefix.size();
    size_t suffix_length = file_suffix.size();
    String stem = file_name.substr(prefix_length, file_name.length() - prefix_length - suffix_length);

    auto parts = splitScopeAndName(stem);
    if (!parts)
        return std::nullopt;

    auto [scope_str, escaped_name] = *parts;
    String name = unescapeForFileName(escaped_name);
    if (name.empty())
        return std::nullopt;

    CustomVariableName::Scope scope;
    if (!CustomVariableName::tryParseScope(scope_str, scope))
        return std::nullopt;

    return ObjectName{scope, std::move(name)};
}

CustomVariablesDefinitionsDiskStorage::Objects CustomVariablesDefinitionsDiskStorage::loadObjects()
{
    LOG_INFO(log, "Loading custom variable definitions from {}", dir_path);

    if (!std::filesystem::exists(dir_path))
    {
        LOG_DEBUG(log, "The directory for custom variable definitions ({}) does not exist: nothing to load", dir_path);
        return {};
    }

    Objects objects;

    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator it(dir_path); it != dir_end; ++it)
    {
        if (it->isDirectory())
            continue;

        const String & file_name = it.name();
        auto object_name = parseFileName(file_name);
        if (!object_name)
            continue;

        const String path = dir_path + file_name;

        try
        {
            ReadBufferFromFile in(path);
            String create_query;
            readStringUntilEOF(create_query, in);

            ParserCreateVariableQuery parser;
            ASTPtr ast = parseQuery(
                parser,
                create_query.data(),
                create_query.data() + create_query.size(),
                "",
                0,
                global_context->getSettingsRef()[Setting::max_parser_depth],
                global_context->getSettingsRef()[Setting::max_parser_backtracks]);

            objects.emplace_back(*object_name, ast);
        }
        catch (...)
        {
            tryLogCurrentException(log, fmt::format("while loading custom variable definition from {}", path));
        }
    }

    return objects;
}

bool CustomVariablesDefinitionsDiskStorage::storeObject(
    const ContextPtr &,
    const ObjectName & object_name,
    ASTPtr create_query,
    bool throw_if_exists,
    bool replace_if_exists,
    const Settings & settings)
{
    createDirectory();
    String file_path = getFilePath(object_name);
    LOG_DEBUG(log, "Storing custom variable definition {} to file {}", object_name.fullName(), file_path);

    if (fs::exists(file_path))
    {
        if (throw_if_exists)
            throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "Custom variable '{}' already exists", object_name.fullName());
        if (!replace_if_exists)
            return false;
    }

    WriteBufferFromOwnString create_statement_buf;
    IAST::FormatSettings format_settings(/*one_line=*/false);
    create_query->format(create_statement_buf, format_settings);
    writeChar('\n', create_statement_buf);
    String create_statement = create_statement_buf.str();

    String temp_file_path = file_path + ".tmp";

    try
    {
        WriteBufferFromFile out(temp_file_path, create_statement.size());
        writeString(create_statement, out);
        out.next();
        if (settings[Setting::fsync_metadata])
            out.sync();
        out.close();

        if (replace_if_exists)
            fs::rename(temp_file_path, file_path);
        else
            renameNoReplace(temp_file_path, file_path);
    }
    catch (...)
    {
        fs::remove(temp_file_path);
        throw;
    }

    LOG_TRACE(log, "Custom variable definition {} stored", object_name.fullName());
    return true;
}

bool CustomVariablesDefinitionsDiskStorage::removeObject(
    const ContextPtr &,
    const ObjectName & object_name,
    bool throw_if_not_exists)
{
    String file_path = getFilePath(object_name);
    LOG_DEBUG(log, "Removing custom variable definition {} from file {}", object_name.fullName(), file_path);

    bool existed = fs::remove(file_path);
    if (!existed)
    {
        if (throw_if_not_exists)
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Custom variable '{}' doesn't exist", object_name.fullName());
        return false;
    }

    LOG_TRACE(log, "Custom variable definition {} removed", object_name.fullName());
    return true;
}

}
