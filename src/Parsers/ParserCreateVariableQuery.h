#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/// CREATE VARIABLE local.foo AS 1
class ParserCreateVariableQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE VARIABLE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
