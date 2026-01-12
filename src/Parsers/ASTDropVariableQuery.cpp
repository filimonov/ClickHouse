#include <IO/Operators.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTDropVariableQuery.h>

namespace DB
{

ASTPtr ASTDropVariableQuery::clone() const
{
    auto res = std::make_shared<ASTDropVariableQuery>(*this);
    res->children.clear();

    res->variable_name = variable_name->clone();
    res->children.push_back(res->variable_name);

    return res;
}

void ASTDropVariableQuery::formatImpl(
    WriteBuffer & ostr, const IAST::FormatSettings & settings, IAST::FormatState & state, IAST::FormatStateStacked frame) const
{
    ostr << "DROP VARIABLE ";

    if (if_exists)
        ostr << "IF EXISTS ";

    variable_name->format(ostr, settings, state, frame);
    formatOnCluster(ostr, settings);
}

String ASTDropVariableQuery::getVariableName() const
{
    String name;
    tryGetIdentifierNameInto(variable_name, name);
    return name;
}

}
