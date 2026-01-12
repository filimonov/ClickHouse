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
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

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

    return CustomVariableName{
        String(full_name.substr(0, dot_pos)),
        String(full_name.substr(dot_pos + 1))};
}

void validateScope(const CustomVariableName & name)
{
    if (name.scope != "local")
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Only local variables are supported in this phase");
}

class FunctionGetVariable : public IFunction, WithContext
{
public:
    static constexpr auto name = "getVariable";

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionGetVariable>(context_); }
    explicit FunctionGetVariable(ContextPtr context_) : WithContext(context_) {}

    String getName() const override { return name; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    size_t getNumberOfArguments() const override { return 1; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto variable_name = parseVariableName(arguments, getName());
        validateScope(variable_name);
        getContext()->checkAccess(AccessType::getVariable);

        auto entry = getContext()->getCustomVariablesManager().getEntry(variable_name);
        if (!entry->definition.declared_type)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Custom variable '{}' has unknown type", variable_name.fullName());

        return entry->definition.declared_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const auto variable_name = parseVariableName(arguments, getName());
        validateScope(variable_name);
        getContext()->checkAccess(AccessType::getVariable);

        auto entry = getContext()->getCustomVariablesManager().getEntry(variable_name);
        auto value = entry->value.load();
        if (!value || !value->has_value)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Custom variable '{}' has no value", variable_name.fullName());

        Field field = value->value;
        return result_type->createColumnConst(input_rows_count, convertFieldToType(field, *result_type));
    }
};

}

REGISTER_FUNCTION(GetVariable)
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

    factory.registerFunction<FunctionGetVariable>(documentation, FunctionFactory::Case::Sensitive);
}

}
