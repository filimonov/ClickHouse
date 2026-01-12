#include <Parsers/ASTDropVariableQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserDropVariableQuery.h>

namespace DB
{

bool ParserDropVariableQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_drop(Keyword::DROP);
    ParserKeyword s_variable(Keyword::VARIABLE);
    ParserKeyword s_if_exists(Keyword::IF_EXISTS);
    ParserKeyword s_on(Keyword::ON);
    ParserCompoundIdentifier name_p;

    String cluster_str;
    bool if_exists = false;

    ASTPtr variable_name;

    if (!s_drop.ignore(pos, expected))
        return false;

    if (!s_variable.ignore(pos, expected))
        return false;

    if (s_if_exists.ignore(pos, expected))
        if_exists = true;

    if (!name_p.parse(pos, variable_name, expected))
        return false;

    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    auto drop_variable_query = std::make_shared<ASTDropVariableQuery>();
    node = drop_variable_query;

    drop_variable_query->variable_name = variable_name;
    drop_variable_query->children.push_back(variable_name);

    drop_variable_query->if_exists = if_exists;
    drop_variable_query->cluster = std::move(cluster_str);

    return true;
}

}
