#pragma once

#include <Interpreters/CustomVariableKind.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>

namespace DB
{

class ASTCreateVariableQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    ASTPtr variable_name;
    ASTPtr expression;
    ASTPtr refresh_strategy;

    bool or_replace = false;
    bool if_not_exists = false;
    CustomVariableKind kind = CustomVariableKind::Server;

    String getID(char delim) const override { return "CreateVariableQuery" + (delim + getVariableName()); }

    ASTPtr clone() const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override
    {
        return removeOnCluster<ASTCreateVariableQuery>(clone());
    }

    String getVariableName() const;

    QueryKind getQueryKind() const override { return QueryKind::Create; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
