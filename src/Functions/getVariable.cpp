#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>

#include <Access/ContextAccess.h>
#include <Core/Field.h>
#include <Interpreters/Context.h>
#include <Interpreters/CustomVariablesManager.h>
#include <Interpreters/CustomVariableKind.h>
#include <Interpreters/convertFieldToType.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int INCORRECT_QUERY;
    extern const int UNKNOWN_IDENTIFIER;
}

namespace
{

enum class ErrorHandlingMode : uint8_t
{
    Exception,
    Default,
};

/// Extract the bare variable name from the first (const String) argument.
/// No scope parsing: the function itself (by its registered name) decides which
/// storage domain to read. Callers that want a different domain use the matching
/// getTemporaryVariable / getReplicatedVariable function.
String parseVariableName(const ColumnsWithTypeAndName & arguments, const String & function_name)
{
    if (!isString(arguments[0].type))
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "The argument of function {} should be a constant string with the name of a variable",
            function_name);

    const auto * column = arguments[0].column.get();
    if (!column || !checkAndGetColumnConstStringOrFixedString(column))
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "The argument of function {} should be a constant string with the name of a variable",
            function_name);

    std::string_view name_view{column->getDataAt(0)};
    String name{name_view};
    if (name.empty())
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "The argument of function {} must be a non-empty string",
            function_name);
    return name;
}

template <CustomVariableKind Kind, ErrorHandlingMode Mode>
constexpr const char * functionNameFor()
{
    if constexpr (Kind == CustomVariableKind::Server)
        return Mode == ErrorHandlingMode::Exception ? "getVariable" : "getVariableOrDefault";
    else if constexpr (Kind == CustomVariableKind::Temporary)
        return Mode == ErrorHandlingMode::Exception ? "getTemporaryVariable" : "getTemporaryVariableOrDefault";
    else
        return Mode == ErrorHandlingMode::Exception ? "getReplicatedVariable" : "getReplicatedVariableOrDefault";
}

template <CustomVariableKind Kind, ErrorHandlingMode Mode>
class FunctionGetVariableImpl : public IFunction, WithContext
{
public:
    static constexpr auto name = functionNameFor<Kind, Mode>();

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionGetVariableImpl>(context_); }
    explicit FunctionGetVariableImpl(ContextPtr context_) : WithContext(context_) {}

    String getName() const override { return name; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    size_t getNumberOfArguments() const override { return (Mode == ErrorHandlingMode::Default) ? 2 : 1; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0, 1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto bare_name = parseVariableName(arguments, getName());
        getContext()->checkAccess(AccessType::getVariable);

        if constexpr (Kind == CustomVariableKind::Temporary)
            rejectInDistributedOrSessionlessContext();

        const auto & manager = managerFor();
        auto entry = manager.tryGetEntry(CustomVariableName{Kind, bare_name});
        if (entry && entry->definition.declared_type)
            return entry->definition.declared_type;

        if constexpr (Mode == ErrorHandlingMode::Exception)
        {
            if (!entry)
                throw Exception(
                    ErrorCodes::UNKNOWN_IDENTIFIER,
                    "No {} variable '{}'",
                    kindDisplayName(Kind),
                    bare_name);
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "{} variable '{}' has unknown type",
                kindDisplayName(Kind),
                bare_name);
        }
        else
        {
            return arguments[1].type;
        }
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const auto bare_name = parseVariableName(arguments, getName());
        getContext()->checkAccess(AccessType::getVariable);

        if constexpr (Kind == CustomVariableKind::Temporary)
            rejectInDistributedOrSessionlessContext();

        const auto & manager = managerFor();
        auto entry = manager.tryGetEntry(CustomVariableName{Kind, bare_name});

        if (entry)
        {
            auto value = entry->value.load();
            if (value && value->has_value)
            {
                Field field = value->value;
                return result_type->createColumnConst(input_rows_count, convertFieldToType(field, *result_type));
            }
        }

        if constexpr (Mode == ErrorHandlingMode::Exception)
        {
            if (!entry)
                throw Exception(
                    ErrorCodes::UNKNOWN_IDENTIFIER,
                    "No {} variable '{}'",
                    kindDisplayName(Kind),
                    bare_name);
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "{} variable '{}' has no value",
                kindDisplayName(Kind),
                bare_name);
        }
        else
        {
            const auto * default_column = arguments[1].column.get();
            if (!default_column || !isColumnConst(*default_column))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The 2nd argument of function {} should be a constant with the default value", String{name});
            Field field = (*default_column)[0];
            return result_type->createColumnConst(input_rows_count, convertFieldToType(field, *result_type));
        }
    }

private:
    const CustomVariablesManager & managerFor() const
    {
        if constexpr (Kind == CustomVariableKind::Temporary)
            return getContext()->getSessionCustomVariablesManager();
        else
            return getContext()->getCustomVariablesManager();
    }

    /// v1 semantics: temporary variables are a per-session concept. If the current
    /// execution has no session context (background tasks, remote-shard subqueries
    /// dispatched from Distributed tables / remote()/cluster()), refuse the call.
    /// Inlining the temporary value as a literal into the remote query text is a
    /// legitimate future feature; for now we prefer a clear, explicit rejection
    /// over silently returning a wrong value on remote shards.
    void rejectInDistributedOrSessionlessContext() const
    {
        if (!getContext()->hasSessionContext())
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "{} cannot be used in distributed or remote queries; temporary variables only exist on the initiating session",
                String{name});
    }
};

template <CustomVariableKind Kind>
FunctionDocumentation describeException(
    const String & function_name,
    const String & example_create,
    const String & example_select,
    const String & example_output)
{
    FunctionDocumentation doc;
    doc.description = fmt::format(
        "Returns the current value of a {} custom variable. Throws if the variable does not exist in the {} domain.",
        kindDisplayName(Kind),
        kindDisplayName(Kind));
    doc.syntax = function_name + "(variable_name)";
    doc.arguments = {{"variable_name", "The bare variable name.", {"const String"}}};
    doc.returned_value = {"Current value of the variable.", {"Any"}};
    doc.examples = {{"Usage example", example_create + "\n" + example_select, example_output}};
    doc.category = FunctionDocumentation::Category::Other;
    return doc;
}

template <CustomVariableKind Kind>
FunctionDocumentation describeDefault(
    const String & function_name,
    const String & example_select,
    const String & example_output)
{
    FunctionDocumentation doc;
    doc.description = fmt::format(
        "Returns the current value of a {} custom variable, or the provided default if the variable is not defined or has no value in the {} domain.",
        kindDisplayName(Kind),
        kindDisplayName(Kind));
    doc.syntax = function_name + "(variable_name, default_value)";
    doc.arguments = {
        {"variable_name", "The bare variable name.", {"const String"}},
        {"default_value", "Value to return if the variable is not defined.", {"Any"}},
    };
    doc.returned_value = {"Current value, or default_value.", {"Any"}};
    doc.examples = {{"Usage example", example_select, example_output}};
    doc.category = FunctionDocumentation::Category::Other;
    return doc;
}

}

REGISTER_FUNCTION(GetVariable)
{
    /// Server kind — the default. `getVariable` / `getVariableOrDefault`.
    factory.registerFunction<FunctionGetVariableImpl<CustomVariableKind::Server, ErrorHandlingMode::Exception>>(
        describeException<CustomVariableKind::Server>(
            "getVariable",
            "CREATE VARIABLE foo AS 1;",
            "SELECT getVariable('foo');",
            "┌─getVariable('foo')─┐\n│ 1                  │\n└────────────────────┘"),
        FunctionFactory::Case::Sensitive);

    factory.registerFunction<FunctionGetVariableImpl<CustomVariableKind::Server, ErrorHandlingMode::Default>>(
        describeDefault<CustomVariableKind::Server>(
            "getVariableOrDefault",
            "SELECT getVariableOrDefault('undefined', 42);",
            "┌─getVariableOrDefault('undefined', 42)─┐\n│ 42                                    │\n└───────────────────────────────────────┘"),
        FunctionFactory::Case::Sensitive);

    /// Temporary kind — session-local. `getTemporaryVariable` / `getTemporaryVariableOrDefault`.
    factory.registerFunction<FunctionGetVariableImpl<CustomVariableKind::Temporary, ErrorHandlingMode::Exception>>(
        describeException<CustomVariableKind::Temporary>(
            "getTemporaryVariable",
            "CREATE TEMPORARY VARIABLE scratch AS 1;",
            "SELECT getTemporaryVariable('scratch');",
            "┌─getTemporaryVariable('scratch')─┐\n│ 1                               │\n└─────────────────────────────────┘"),
        FunctionFactory::Case::Sensitive);

    factory.registerFunction<FunctionGetVariableImpl<CustomVariableKind::Temporary, ErrorHandlingMode::Default>>(
        describeDefault<CustomVariableKind::Temporary>(
            "getTemporaryVariableOrDefault",
            "SELECT getTemporaryVariableOrDefault('scratch', 0);",
            "┌─getTemporaryVariableOrDefault('scratch', 0)─┐\n│ 0                                           │\n└─────────────────────────────────────────────┘"),
        FunctionFactory::Case::Sensitive);

    /// Replicated kind — Keeper-shared. `getReplicatedVariable` / `getReplicatedVariableOrDefault`.
    factory.registerFunction<FunctionGetVariableImpl<CustomVariableKind::Replicated, ErrorHandlingMode::Exception>>(
        describeException<CustomVariableKind::Replicated>(
            "getReplicatedVariable",
            "CREATE REPLICATED VARIABLE fx AS 1.0;",
            "SELECT getReplicatedVariable('fx');",
            "┌─getReplicatedVariable('fx')─┐\n│ 1                           │\n└─────────────────────────────┘"),
        FunctionFactory::Case::Sensitive);

    factory.registerFunction<FunctionGetVariableImpl<CustomVariableKind::Replicated, ErrorHandlingMode::Default>>(
        describeDefault<CustomVariableKind::Replicated>(
            "getReplicatedVariableOrDefault",
            "SELECT getReplicatedVariableOrDefault('fx', 1.0);",
            "┌─getReplicatedVariableOrDefault('fx', 1.0)─┐\n│ 1                                         │\n└───────────────────────────────────────────┘"),
        FunctionFactory::Case::Sensitive);
}

}
