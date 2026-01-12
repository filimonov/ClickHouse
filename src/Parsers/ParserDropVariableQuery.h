#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/// DROP VARIABLE local.foo
class ParserDropVariableQuery : public IParserBase
{
protected:
    const char * getName() const override { return "DROP VARIABLE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
