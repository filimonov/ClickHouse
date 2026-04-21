#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>

#include <Access/ContextAccess.h>
#include <Core/Field.h>
#include <Interpreters/Context.h>
#include <Interpreters/CustomVariablesManager.h>
#include <Interpreters/convertFieldToType.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_QUERY;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int UNKNOWN_IDENTIFIER;
}

namespace
{

enum class ErrorHandlingMode : uint8_t
{
    Exception,
    Default,
};

CustomVariableName parseVariableName(const ColumnsWithTypeAndName & arguments, const String & function_name)
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

    std::string_view full_name{column->getDataAt(0)};
    const auto dot_pos = full_name.find('.');
    if (dot_pos == std::string_view::npos || dot_pos == 0 || dot_pos + 1 >= full_name.size()
        || full_name.find('.', dot_pos + 1) != std::string_view::npos)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Custom variable name must be specified as scope.name");
    }

    String scope_str(full_name.substr(0, dot_pos));
    String name(full_name.substr(dot_pos + 1));

    CustomVariableName::Scope scope;
    if (!CustomVariableName::tryParseScope(scope_str, scope))
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Unknown custom variable scope '{}'", scope_str);

    return CustomVariableName{scope, std::move(name)};
}

const CustomVariablesManager & getManagerForScope(ContextPtr context, const CustomVariableName & name)
{
    if (name.scope == CustomVariableName::Scope::Session)
        return context->getSessionCustomVariablesManager();
    return context->getCustomVariablesManager();
}

template <ErrorHandlingMode mode>
class FunctionGetVariable : public IFunction, WithContext
{
public:
    static constexpr auto name = (mode == ErrorHandlingMode::Exception) ? "getVariable" : "getVariableOrDefault";

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionGetVariable>(context_); }
    explicit FunctionGetVariable(ContextPtr context_) : WithContext(context_) {}

    String getName() const override { return name; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    size_t getNumberOfArguments() const override { return (mode == ErrorHandlingMode::Default) ? 2 : 1; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0, 1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto variable_name = parseVariableName(arguments, getName());
        getContext()->checkAccess(AccessType::getVariable);

        const auto & manager = getManagerForScope(getContext(), variable_name);
        auto entry = manager.tryGetEntry(variable_name);
        if (entry && entry->definition.declared_type)
            return entry->definition.declared_type;

        if constexpr (mode == ErrorHandlingMode::Exception)
        {
            if (!entry)
                throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "Custom variable '{}' not found", variable_name.fullName());
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Custom variable '{}' has unknown type", variable_name.fullName());
        }
        else
        {
            return arguments[1].type;
        }
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const auto variable_name = parseVariableName(arguments, getName());
        getContext()->checkAccess(AccessType::getVariable);

        const auto & manager = getManagerForScope(getContext(), variable_name);
        auto entry = manager.tryGetEntry(variable_name);

        if (entry)
        {
            auto value = entry->value.load();
            if (value && value->has_value)
            {
                Field field = value->value;
                return result_type->createColumnConst(input_rows_count, convertFieldToType(field, *result_type));
            }
        }

        if constexpr (mode == ErrorHandlingMode::Exception)
        {
            if (!entry)
                throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "Custom variable '{}' not found", variable_name.fullName());
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Custom variable '{}' has no value", variable_name.fullName());
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
};

}

REGISTER_FUNCTION(GetVariable)
{
    {
        FunctionDocumentation::Description description = R"(
Returns the current value of a custom variable.
)";
        FunctionDocumentation::Syntax syntax = "getVariable(variable_name)";
        FunctionDocumentation::Arguments arguments = {
            {"variable_name", "The variable name in the form `scope.name`.", {"const String"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {
            "Returns the current value of the custom variable.", {"Any"}
        };
        FunctionDocumentation::Examples examples = {{
            "Usage example",
            R"(
CREATE VARIABLE local.foo AS 1;
SELECT getVariable('local.foo');
            )",
            R"(
┌─getVariable('local.foo')─┐
│ 1                        │
└──────────────────────────┘
            )"
        }};
        FunctionDocumentation documentation;
        documentation.description = description;
        documentation.syntax = syntax;
        documentation.arguments = arguments;
        documentation.returned_value = returned_value;
        documentation.examples = examples;
        documentation.category = FunctionDocumentation::Category::Other;

        factory.registerFunction<FunctionGetVariable<ErrorHandlingMode::Exception>>(documentation, FunctionFactory::Case::Sensitive);
    }

    {
        FunctionDocumentation::Description description = R"(
Returns the current value of a custom variable or a provided default if the variable is not defined or has no value.
)";
        FunctionDocumentation::Syntax syntax = "getVariableOrDefault(variable_name, default_value)";
        FunctionDocumentation::Arguments arguments = {
            {"variable_name", "The variable name in the form `scope.name`.", {"const String"}},
            {"default_value", "Value to return if the variable is not defined or has no value.", {"Any"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {
            "Returns the current value of the custom variable, otherwise `default_value`.", {"Any"}
        };
        FunctionDocumentation::Examples examples = {{
            "Usage example",
            R"(
SELECT getVariableOrDefault('local.undefined', 42);
            )",
            R"(
┌─getVariableOrDefault('local.undefined', 42)─┐
│ 42                                          │
└─────────────────────────────────────────────┘
            )"
        }};
        FunctionDocumentation documentation;
        documentation.description = description;
        documentation.syntax = syntax;
        documentation.arguments = arguments;
        documentation.returned_value = returned_value;
        documentation.examples = examples;
        documentation.category = FunctionDocumentation::Category::Other;

        factory.registerFunction<FunctionGetVariable<ErrorHandlingMode::Default>>(documentation, FunctionFactory::Case::Sensitive);
    }
}

}
