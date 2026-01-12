#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>

namespace DB
{

class ASTDropVariableQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    ASTPtr variable_name;

    bool if_exists = false;

    String getID(char) const override { return "DropVariableQuery"; }

    ASTPtr clone() const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override
    {
        return removeOnCluster<ASTDropVariableQuery>(clone());
    }

    String getVariableName() const;

    QueryKind getQueryKind() const override { return QueryKind::Drop; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
