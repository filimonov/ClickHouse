#include <IO/Operators.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTCreateVariableQuery.h>

namespace DB
{

ASTPtr ASTCreateVariableQuery::clone() const
{
    auto res = std::make_shared<ASTCreateVariableQuery>(*this);
    res->children.clear();

    res->variable_name = variable_name->clone();
    res->children.push_back(res->variable_name);

    res->expression = expression->clone();
    res->children.push_back(res->expression);

    if (refresh_strategy)
    {
        res->refresh_strategy = refresh_strategy->clone();
        res->children.push_back(res->refresh_strategy);
    }

    return res;
}

void ASTCreateVariableQuery::formatImpl(
    WriteBuffer & ostr, const IAST::FormatSettings & settings, IAST::FormatState & state, IAST::FormatStateStacked frame) const
{
    ostr << "CREATE ";

    if (or_replace)
        ostr << "OR REPLACE ";

    switch (kind)
    {
        case CustomVariableKind::Server:
            break;
        case CustomVariableKind::Temporary:
            ostr << "TEMPORARY ";
            break;
        case CustomVariableKind::Replicated:
            ostr << "REPLICATED ";
            break;
    }

    ostr << "VARIABLE ";

    if (if_not_exists)
        ostr << "IF NOT EXISTS ";

    variable_name->format(ostr, settings, state, frame);

    /// ON CLUSTER is only valid for the server kind. Temporary and Replicated grammar
    /// rejects ON CLUSTER at parse time, so `cluster` is guaranteed empty for them.
    if (kind == CustomVariableKind::Server)
        formatOnCluster(ostr, settings);

    if (refresh_strategy)
    {
        ostr << ' ';
        refresh_strategy->format(ostr, settings, state, frame);
    }

    ostr << " AS ";
    expression->format(ostr, settings, state, frame);
}

String ASTCreateVariableQuery::getVariableName() const
{
    String name;
    tryGetIdentifierNameInto(variable_name, name);
    return name;
}

}
