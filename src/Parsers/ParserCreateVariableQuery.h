#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/// CREATE [OR REPLACE] [TEMPORARY | REPLICATED] VARIABLE [IF NOT EXISTS] name
///     [ON CLUSTER cluster] [REFRESH <strategy>] AS <expr_or_select>
class ParserCreateVariableQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE VARIABLE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
