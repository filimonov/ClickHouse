#include <Parsers/ParserCreateVariableQuery.h>

#include <Interpreters/ICustomVariablesDefinitionsStorage.h>
#include <Parsers/ASTCreateVariableQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserRefreshStrategy.h>
#include <Parsers/ParserSelectWithUnionQuery.h>

namespace DB
{

bool ParserCreateVariableQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_variable(Keyword::VARIABLE);
    ParserKeyword s_or_replace(Keyword::OR_REPLACE);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserKeyword s_on(Keyword::ON);
    ParserKeyword s_refresh(Keyword::REFRESH);
    ParserKeyword s_as(Keyword::AS);
    ParserKeyword s_temporary(Keyword::TEMPORARY);
    ParserKeyword s_replicated(Keyword::REPLICATED);
    ParserIdentifier name_p;
    ParserSelectWithUnionQuery select_p;
    ParserExpression expression_p;
    ParserRefreshStrategy refresh_p;

    ASTPtr variable_name;
    ASTPtr expression;
    ASTPtr refresh_strategy;

    String cluster_str;
    bool or_replace = false;
    bool if_not_exists = false;
    CustomVariableKind kind = CustomVariableKind::Server;

    if (!s_create.ignore(pos, expected))
        return false;

    if (s_or_replace.ignore(pos, expected))
        or_replace = true;

    /// Exactly one kind modifier (or none = server).
    if (s_temporary.ignore(pos, expected))
        kind = CustomVariableKind::Temporary;
    else if (s_replicated.ignore(pos, expected))
        kind = CustomVariableKind::Replicated;

    if (!s_variable.ignore(pos, expected))
        return false;

    if (!or_replace && s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    if (!name_p.parse(pos, variable_name, expected))
        return false;

    /// Clause order: name [ON CLUSTER] [REFRESH] AS
    /// Grammar-level matrix: TEMPORARY and REPLICATED reject ON CLUSTER; TEMPORARY
    /// also rejects REFRESH. We simply don't accept those clauses for those kinds.
    if (kind == CustomVariableKind::Server && s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    if (kind != CustomVariableKind::Temporary && s_refresh.ignore(pos, expected))
    {
        if (!refresh_p.parse(pos, refresh_strategy, expected))
            return false;
    }

    if (!s_as.ignore(pos, expected))
        return false;

    if (!select_p.parse(pos, expression, expected))
    {
        if (!expression_p.parse(pos, expression, expected))
            return false;
    }

    auto create_variable_query = std::make_shared<ASTCreateVariableQuery>();
    node = create_variable_query;

    create_variable_query->variable_name = variable_name;
    create_variable_query->children.push_back(variable_name);

    create_variable_query->expression = expression;
    create_variable_query->children.push_back(expression);

    if (refresh_strategy)
    {
        create_variable_query->refresh_strategy = refresh_strategy;
        create_variable_query->children.push_back(refresh_strategy);
    }

    create_variable_query->or_replace = or_replace;
    create_variable_query->if_not_exists = if_not_exists;
    create_variable_query->cluster = std::move(cluster_str);
    create_variable_query->kind = kind;

    return true;
}

}
