#pragma once

#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

#include <cstddef>

namespace DB
{

constexpr size_t MAX_CUSTOM_VARIABLE_SIZE = 1024;

struct EvaluatedCustomVariable
{
    Field value;
    DataTypePtr type;
};

EvaluatedCustomVariable evaluateCustomVariableExpression(const ASTPtr & expression, const ContextPtr & context);
DataTypePtr getCustomVariableExpressionType(const ASTPtr & expression, const ContextPtr & context);
bool isCustomVariableExpressionConstant(const ASTPtr & expression, const ContextPtr & context);
void checkCustomVariableSize(const Field & value);

}
