#include <Interpreters/CustomVariablesEvaluator.h>

#include <Common/FieldBinaryEncoding.h>
#include <Common/Exception.h>

#include <Columns/IColumn.h>
#include <DataTypes/DataTypeFactory.h>

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/evaluateConstantExpression.h>

#include <IO/WriteBufferFromString.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Core/Block.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_RESULT_OF_SCALAR_SUBQUERY;
    extern const int TOO_LARGE_STRING_SIZE;
}

namespace
{
void assertNoVariableAccess(const ASTPtr & expression)
{
    static constexpr std::string_view forbidden_names[] = {"getVariable", "getVariableOrDefault"};

    if (!expression)
        return;

    if (const auto * function = expression->as<ASTFunction>())
    {
        for (const auto & forbidden : forbidden_names)
        {
            if (function->name == forbidden)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Custom variable definition cannot reference function {}()",
                    function->name);
        }
    }

    for (const auto & child : expression->children)
        assertNoVariableAccess(child);
}

ContextMutablePtr createEvaluationContext(const ContextPtr & context)
{
    auto eval_context = Context::createCopy(context);
    eval_context->makeQueryContext();
    eval_context->setCurrentQueryId({});
    return eval_context;
}

ASTPtr wrapExpressionInSelect(const ASTPtr & expression)
{
    auto select_expr_list = std::make_shared<ASTExpressionList>();
    select_expr_list->children.push_back(expression->clone());

    auto select_query = std::make_shared<ASTSelectQuery>();
    select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expr_list));

    auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
    select_with_union_query->list_of_selects = std::make_shared<ASTExpressionList>();
    select_with_union_query->list_of_selects->children.push_back(std::move(select_query));
    select_with_union_query->children.push_back(select_with_union_query->list_of_selects);

    return select_with_union_query;
}

ASTPtr wrapSelectQueryInUnion(const ASTPtr & select_query)
{
    auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
    select_with_union_query->list_of_selects = std::make_shared<ASTExpressionList>();
    select_with_union_query->list_of_selects->children.push_back(select_query->clone());
    select_with_union_query->children.push_back(select_with_union_query->list_of_selects);
    return select_with_union_query;
}

ASTPtr normalizeToSelectWithUnion(const ASTPtr & expression)
{
    if (expression->as<ASTSelectWithUnionQuery>())
        return expression->clone();
    if (expression->as<ASTSelectQuery>())
        return wrapSelectQueryInUnion(expression);
    return wrapExpressionInSelect(expression);
}

EvaluatedCustomVariable executeScalarSelect(const ASTPtr & select_query, const ContextPtr & context)
{
    InterpreterSelectWithUnionQuery interpreter(select_query, context, SelectQueryOptions());
    auto io = interpreter.execute();

    PullingPipelineExecutor executor(io.pipeline);
    Block block;

    while (block.rows() == 0 && executor.pull(block))
    {
    }

    if (block.rows() == 0)
        throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY, "Scalar query returned empty result");

    if (block.rows() != 1)
        throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY, "Scalar query returned more than one row");

    Block next_block;
    while (next_block.rows() == 0 && executor.pull(next_block))
    {
    }

    if (next_block.rows() != 0)
        throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY, "Scalar query returned more than one row");

    block = materializeBlock(block);
    if (block.columns() != 1)
        throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY, "Scalar query returned more than one column");

    const auto & column_with_type = block.getByPosition(0);
    Field field;
    column_with_type.column->get(0, field);
    return EvaluatedCustomVariable{std::move(field), column_with_type.type};
}

DataTypePtr getCastTargetType(const ASTPtr & expression)
{
    const auto * function = expression->as<ASTFunction>();
    if (!function || function->name != "_CAST" || !function->arguments)
        return nullptr;

    const auto * args = function->arguments->as<ASTExpressionList>();
    if (!args || args->children.size() != 2)
        return nullptr;

    const auto * literal = args->children[1]->as<ASTLiteral>();
    if (!literal || literal->value.getType() != Field::Types::String)
        return nullptr;

    return DataTypeFactory::instance().get(literal->value.safeGet<String>());
}
}

EvaluatedCustomVariable evaluateCustomVariableExpression(const ASTPtr & expression, const ContextPtr & context)
{
    assertNoVariableAccess(expression);
    auto eval_context = createEvaluationContext(context);
    const bool is_select_query = expression->as<ASTSelectWithUnionQuery>() || expression->as<ASTSelectQuery>();

    if (!is_select_query)
    {
        if (auto constant = tryEvaluateConstantExpression(expression, eval_context))
            return EvaluatedCustomVariable{std::move(constant->first), std::move(constant->second)};
    }

    ASTPtr select_query = normalizeToSelectWithUnion(expression);
    return executeScalarSelect(select_query, eval_context);
}

DataTypePtr getCustomVariableExpressionType(const ASTPtr & expression, const ContextPtr & context)
{
    assertNoVariableAccess(expression);
    if (auto cast_type = getCastTargetType(expression))
        return cast_type;

    auto eval_context = createEvaluationContext(context);
    ASTPtr select_query = normalizeToSelectWithUnion(expression);

    auto sample_block = InterpreterSelectWithUnionQuery::getSampleBlock(select_query, eval_context);
    if (sample_block->columns() != 1)
        throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY, "Custom variable expression must return a single column");

    return sample_block->getByPosition(0).type;
}

void checkCustomVariableSize(const Field & value)
{
    WriteBufferFromOwnString buffer;
    encodeField(value, buffer);
    if (buffer.str().size() > MAX_CUSTOM_VARIABLE_SIZE)
        throw Exception(
            ErrorCodes::TOO_LARGE_STRING_SIZE,
            "Custom variable value is too large ({} bytes, max {})",
            buffer.str().size(),
            MAX_CUSTOM_VARIABLE_SIZE);
}

}
