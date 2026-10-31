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
template <typename QueryTreeNodePtrType, bool compare_aliases = true, bool compare_types = true, bool hash_column_sources = true>
struct QueryTreeNodeWithHash
{
    QueryTreeNodeWithHash(QueryTreeNodePtrType node_) /// NOLINT
        : node(std::move(node_))
        , hash(node->getTreeHash({.compare_aliases = compare_aliases, .compare_types = compare_types, .hash_column_sources = hash_column_sources}))
    {}

    QueryTreeNodePtrType node = nullptr;
    CityHash_v1_0_2::uint128 hash;
};

template <typename T, bool compare_aliases, bool compare_types, bool hash_column_sources>
inline bool operator==(const QueryTreeNodeWithHash<T, compare_aliases, compare_types, hash_column_sources> & lhs, const QueryTreeNodeWithHash<T, compare_aliases, compare_types, hash_column_sources> & rhs)
{
    return lhs.hash == rhs.hash && lhs.node->isEqual(*rhs.node, {.compare_aliases = compare_aliases, .compare_types = compare_types});
}

template <typename T, bool compare_aliases, bool compare_types, bool hash_column_sources>
inline bool operator!=(const QueryTreeNodeWithHash<T, compare_aliases, compare_types, hash_column_sources> & lhs, const QueryTreeNodeWithHash<T, compare_aliases, compare_types, hash_column_sources> & rhs)
{
    return !(lhs == rhs);
}

using QueryTreeNodePtrWithHash = QueryTreeNodeWithHash<QueryTreeNodePtr>;
using QueryTreeNodePtrWithHashIgnoreTypes = QueryTreeNodeWithHash<QueryTreeNodePtr, /*compare_aliases*/ false, /*compare_types*/ false>;
using QueryTreeNodeRawPtrWithHash = QueryTreeNodeWithHash<IQueryTreeNode *>;
using QueryTreeNodeConstRawPtrWithHash = QueryTreeNodeWithHash<const IQueryTreeNode *>;

using QueryTreeNodePtrWithHashSet = std::unordered_set<QueryTreeNodePtrWithHash>;
using QueryTreeNodePtrWithHashIgnoreTypesSet = std::unordered_set<QueryTreeNodePtrWithHashIgnoreTypes>;

/// Hashes a column by its name and type only, without its source subtree. For sets of GROUP BY keys
/// and similar lookups inside one query, where every key of a subquery column would otherwise hash
/// the whole subquery. Equality is still the full isEqual.
using QueryTreeNodePtrWithHashNoSources = QueryTreeNodeWithHash<QueryTreeNodePtr, /*compare_aliases*/ true, /*compare_types*/ true, /*hash_column_sources*/ false>;
using QueryTreeNodePtrWithHashNoSourcesSet = std::unordered_set<QueryTreeNodePtrWithHashNoSources>;
using QueryTreeNodeConstRawPtrWithHashSet = std::unordered_set<QueryTreeNodeConstRawPtrWithHash>;

template <typename Value>
using QueryTreeNodePtrWithHashMap = std::unordered_map<QueryTreeNodePtrWithHash, Value>;

class ColumnNode;
using ColumnNodePtr = std::shared_ptr<ColumnNode>;
using ColumnNodePtrWithHash = QueryTreeNodeWithHash<ColumnNodePtr>;
using ColumnNodePtrWithHashSet = std::unordered_set<ColumnNodePtrWithHash>;

}

template <typename T, bool compare_aliases, bool compare_types, bool hash_column_sources>
struct std::hash<DB::QueryTreeNodeWithHash<T, compare_aliases, compare_types, hash_column_sources>>
{
    size_t operator()(const DB::QueryTreeNodeWithHash<T, compare_aliases, compare_types, hash_column_sources> & node_with_hash) const
    {
        return node_with_hash.hash.low64;
    }
};
