#pragma once

#include <memory>
#include <unordered_map>
#include <unordered_set>

#include <city.h>

namespace DB
{

class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;

/** This structure holds query tree node ptr and its hash. It can be used as hash map key to avoid unnecessary hash
  * recalculations.
  *
  * Example of usage:
  * std::unordered_map<QueryTreeNodeConstRawPtrWithHash, std::string> map;
  */
template <typename QueryTreeNodePtrType, bool compare_aliases = true, bool hash_column_sources = true>
struct QueryTreeNodeWithHash
{
    QueryTreeNodeWithHash(QueryTreeNodePtrType node_) /// NOLINT
        : node(std::move(node_))
        , hash(node->getTreeHash({.compare_aliases = compare_aliases, .hash_column_sources = hash_column_sources}))
    {}

    QueryTreeNodePtrType node = nullptr;
    CityHash_v1_0_2::uint128 hash;
};

template <typename T, bool compare_aliases, bool hash_column_sources>
inline bool operator==(const QueryTreeNodeWithHash<T, compare_aliases, hash_column_sources> & lhs, const QueryTreeNodeWithHash<T, compare_aliases, hash_column_sources> & rhs)
{
    return lhs.hash == rhs.hash && lhs.node->isEqual(*rhs.node, {.compare_aliases = compare_aliases});
}

template <typename T, bool compare_aliases, bool hash_column_sources>
inline bool operator!=(const QueryTreeNodeWithHash<T, compare_aliases, hash_column_sources> & lhs, const QueryTreeNodeWithHash<T, compare_aliases, hash_column_sources> & rhs)
{
    return !(lhs == rhs);
}

using QueryTreeNodePtrWithHash = QueryTreeNodeWithHash<QueryTreeNodePtr>;
using QueryTreeNodePtrWithHashIgnoreAliases = QueryTreeNodeWithHash<QueryTreeNodePtr, /*compare_aliases*/ false>;
using QueryTreeNodeRawPtrWithHash = QueryTreeNodeWithHash<IQueryTreeNode *>;
using QueryTreeNodeConstRawPtrWithHash = QueryTreeNodeWithHash<const IQueryTreeNode *>;

using QueryTreeNodePtrWithHashSet = std::unordered_set<QueryTreeNodePtrWithHash>;
using QueryTreeNodePtrWithHashIgnoreAliasesSet = std::unordered_set<QueryTreeNodePtrWithHashIgnoreAliases>;
using QueryTreeNodeConstRawPtrWithHashSet = std::unordered_set<QueryTreeNodeConstRawPtrWithHash>;

template <typename Value>
using QueryTreeNodePtrWithHashMap = std::unordered_map<QueryTreeNodePtrWithHash, Value>;

template <typename Value>
using QueryTreeNodePtrWithHashIgnoreAliasesMap = std::unordered_map<QueryTreeNodePtrWithHashIgnoreAliases, Value>;

/** Keys hashed without column sources: a column contributes its name and type, not the subtree of
  * the table expression it comes from. Equality is still the full isEqual, so the containers behave
  * the same; only the hash is coarser (columns of the same name and type from different sources
  * collide). Use these for lookups among expressions of one query: GROUP BY, ORDER BY and LIMIT BY
  * keys, filter atoms, aggregate arguments, correlated columns. With the full hash every lookup of a
  * subquery column costs the size of that subquery.
  *
  * Keep the full hash where the hash stands for the identity of a subtree that may come from another
  * query or from table metadata: scalar subquery results, CTEs and lambdas in resolution, prepared
  * sets, query trees sent to shards, constraints and the comparison graph.
  */
using QueryTreeNodePtrWithHashNoSources = QueryTreeNodeWithHash<QueryTreeNodePtr, /*compare_aliases*/ true, /*hash_column_sources*/ false>;
using QueryTreeNodePtrWithHashIgnoreAliasesNoSources = QueryTreeNodeWithHash<QueryTreeNodePtr, /*compare_aliases*/ false, /*hash_column_sources*/ false>;
using QueryTreeNodeConstRawPtrWithHashNoSources = QueryTreeNodeWithHash<const IQueryTreeNode *, /*compare_aliases*/ true, /*hash_column_sources*/ false>;

using QueryTreeNodePtrWithHashNoSourcesSet = std::unordered_set<QueryTreeNodePtrWithHashNoSources>;
using QueryTreeNodePtrWithHashIgnoreAliasesNoSourcesSet = std::unordered_set<QueryTreeNodePtrWithHashIgnoreAliasesNoSources>;
using QueryTreeNodeConstRawPtrWithHashNoSourcesSet = std::unordered_set<QueryTreeNodeConstRawPtrWithHashNoSources>;

template <typename Value>
using QueryTreeNodePtrWithHashNoSourcesMap = std::unordered_map<QueryTreeNodePtrWithHashNoSources, Value>;

template <typename Value>
using QueryTreeNodePtrWithHashIgnoreAliasesNoSourcesMap = std::unordered_map<QueryTreeNodePtrWithHashIgnoreAliasesNoSources, Value>;

class ColumnNode;
using ColumnNodePtr = std::shared_ptr<ColumnNode>;
/// A set of correlated columns of a query. The source of a correlated column is an outer query.
using ColumnNodePtrWithHash = QueryTreeNodeWithHash<ColumnNodePtr, /*compare_aliases*/ true, /*hash_column_sources*/ false>;
using ColumnNodePtrWithHashSet = std::unordered_set<ColumnNodePtrWithHash>;

}

template <typename T, bool compare_aliases, bool hash_column_sources>
struct std::hash<DB::QueryTreeNodeWithHash<T, compare_aliases, hash_column_sources>>
{
    size_t operator()(const DB::QueryTreeNodeWithHash<T, compare_aliases, hash_column_sources> & node_with_hash) const
    {
        return node_with_hash.hash.low64;
    }
};
