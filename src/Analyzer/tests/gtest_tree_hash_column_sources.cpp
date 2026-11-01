#include <gtest/gtest.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/DatabaseMemory.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/parseQuery.h>
#include <Poco/Util/MapConfiguration.h>
#include <Storages/ConstraintsDescription.h>
#include <Storages/MemorySettings.h>
#include <Storages/StorageMemory.h>

using namespace DB;

namespace DB
{
/// Thread-local count of nodes visited by IQueryTreeNode::getTreeHash (defined in IQueryTreeNode.cpp).
size_t & getTreeHashWorkCounter();
}

namespace
{

/// A database with one table that every subquery in these tests reads from.
struct State
{
    ContextMutablePtr context;

    static const State & instance()
    {
        static State state;
        return state;
    }

private:
    State()
        : context(Context::createCopy(getContext().context))
    {
        tryRegisterFunctions();
        tryRegisterAggregateFunctions();

        /// Function resolution consults the user defined function storage, which reads the server config.
        try
        {
            context->getConfigRef();
        }
        catch (const Poco::NullPointerException &)
        {
            context->setConfig(Poco::AutoPtr<Poco::Util::AbstractConfiguration>(new Poco::Util::MapConfiguration()));
        }

        static constexpr auto database_name = "tree_hash_column_sources_test_db";

        DatabasePtr database = std::make_shared<DatabaseMemory>(database_name, context);
        NamesAndTypesList columns{
            {"x", std::make_shared<DataTypeUInt64>()},
            {"y", std::make_shared<DataTypeUInt64>()},
            {"s", std::make_shared<DataTypeString>()},
        };
        database->attachTable(
            context,
            "t",
            std::make_shared<StorageMemory>(
                StorageID(database_name, "t"), ColumnsDescription{columns}, ConstraintsDescription{}, String{}, MemorySettings{}),
            {});
        DatabaseCatalog::instance().attachDatabase(database->getDatabaseName(), database);
        context->setCurrentDatabase(database_name);
    }
};

using TestSettings = std::vector<std::pair<String, Field>>;

ContextMutablePtr contextWith(const TestSettings & settings)
{
    auto context = Context::createCopy(State::instance().context);
    for (const auto & [name, value] : settings)
        context->setSetting(name, value);
    return context;
}

/// A subquery with `columns` projection columns `c0..c{columns-1}`, each an expression of
/// `depth` nested additions over `x`, plus the plain columns `y` and `s`.
String wideSubquery(size_t columns, size_t depth)
{
    String sql = "SELECT ";
    for (size_t i = 0; i < columns; ++i)
    {
        String expr = "x";
        for (size_t d = 0; d < depth; ++d)
            expr = "(" + expr + " + " + std::to_string(i * depth + d + 1) + ")";
        sql += expr + " AS c" + std::to_string(i) + ", ";
    }
    sql += "y, s FROM t";
    return sql;
}

size_t countNodes(const QueryTreeNodePtr & root)
{
    size_t count = 0;
    QueryTreeNodes stack{root};
    while (!stack.empty())
    {
        auto node = std::move(stack.back());
        stack.pop_back();
        ++count;
        for (const auto & child : node->getChildren())
            if (child)
                stack.push_back(child);
    }
    return count;
}

QueryTreeNodes collectNodes(const QueryTreeNodePtr & root)
{
    QueryTreeNodes result;
    std::unordered_set<const IQueryTreeNode *> seen;
    QueryTreeNodes stack{root};
    while (!stack.empty())
    {
        auto node = std::move(stack.back());
        stack.pop_back();
        if (!seen.insert(node.get()).second)
            continue;
        result.push_back(node);
        for (const auto & child : node->getChildren())
            if (child)
                stack.push_back(child);
    }
    return result;
}

struct Analyzed
{
    QueryTreeNodePtr tree;
    ContextMutablePtr context;
    /// Nodes of the subquery in FROM, the source of every `c*` column.
    size_t subquery_nodes = 0;
};

/// Build the query tree and run QueryAnalysisPass only.
Analyzed analyze(const String & sql, const TestSettings & settings = {})
{
    Analyzed result;
    result.context = contextWith(settings);

    ParserSelectWithUnionQuery parser;
    ASTPtr ast = parseQuery(parser, sql, 0, 10000, 100000000);
    result.tree = buildQueryTree(ast, result.context);

    QueryTreePassManager pass_manager(result.context);
    addQueryTreePasses(pass_manager);
    pass_manager.run(result.tree, 1);

    if (const auto * subquery = result.tree->as<QueryNode &>().getJoinTreeNode()->as<QueryNode>())
        result.subquery_nodes = countNodes(subquery->getProjectionNode());


    return result;
}

}

/// Equal trees must have equal hashes under every combination of compare options,
/// including hashes that skip column sources: skipping only makes the hash coarser.
TEST(TreeHashColumnSources, EqualNodesHaveEqualHashes)
{
    auto analyzed = analyze(
        "SELECT c0 AS k, c1, grouping(c0), min(c0), sum(y) FROM (" + wideSubquery(4, 3) + ") "
        "WHERE c0 > 1 AND c1 IN (1, 2) "
        "GROUP BY GROUPING SETS ((c0, c1), (c0), (c1)) HAVING sum(y) > 0 ORDER BY c0, c1 LIMIT 1 BY c0");

    auto nodes = collectNodes(analyzed.tree);
    /// Clones must be equal to the originals and hash the same.
    size_t original_count = nodes.size();
    for (size_t i = 0; i < original_count; ++i)
        nodes.push_back(nodes[i]->clone());
    ASSERT_GT(nodes.size(), 50u);

    const IQueryTreeNode::CompareOptions options[] = {
        {.compare_aliases = true, .hash_column_sources = true},
        {.compare_aliases = false, .hash_column_sources = true},
        {.compare_aliases = true, .hash_column_sources = false},
        {.compare_aliases = false, .hash_column_sources = false},
    };

    size_t equal_pairs = 0;
    for (size_t i = 0; i < nodes.size(); ++i)
    {
        for (size_t j = i + 1; j < nodes.size(); ++j)
        {
            for (const auto & option : options)
            {
                if (!nodes[i]->isEqual(*nodes[j], option))
                    continue;
                ++equal_pairs;
                EXPECT_EQ(nodes[i]->getTreeHash(option), nodes[j]->getTreeHash(option))
                    << nodes[i]->formatASTForErrorMessage() << " vs " << nodes[j]->formatASTForErrorMessage()
                    << " compare_aliases=" << option.compare_aliases << " hash_column_sources=" << option.hash_column_sources;
            }
        }
    }
    EXPECT_GT(equal_pairs, nodes.size());
}

/// Without sources the hash of a column depends on its name and type only; the full hash
/// still tells columns of different subqueries apart.
TEST(TreeHashColumnSources, ColumnHashWithoutSourcesIgnoresSource)
{
    auto first = analyze("SELECT c0 FROM (" + wideSubquery(2, 5) + ")");
    auto second = analyze("SELECT c0 FROM (" + wideSubquery(2, 7) + ")");

    auto column_of = [](const Analyzed & analyzed) { return analyzed.tree->as<QueryNode &>().getProjection().getNodes().at(0); };
    auto first_column = column_of(first);
    auto second_column = column_of(second);
    ASSERT_TRUE(first_column->as<ColumnNode>());
    ASSERT_FALSE(first_column->isEqual(*second_column, {.hash_column_sources = false}))
        << "the sources differ, so the columns are not equal";

    const IQueryTreeNode::CompareOptions without_sources{.hash_column_sources = false};
    const IQueryTreeNode::CompareOptions with_sources{.hash_column_sources = true};

    EXPECT_EQ(first_column->getTreeHash(without_sources), second_column->getTreeHash(without_sources));
    EXPECT_NE(first_column->getTreeHash(with_sources), second_column->getTreeHash(with_sources));

    size_t work_before = getTreeHashWorkCounter();
    first_column->getTreeHash(without_sources);
    EXPECT_EQ(getTreeHashWorkCounter() - work_before, 1u);

    work_before = getTreeHashWorkCounter();
    first_column->getTreeHash(with_sources);
    EXPECT_GT(getTreeHashWorkCounter() - work_before, first.subquery_nodes);
}
