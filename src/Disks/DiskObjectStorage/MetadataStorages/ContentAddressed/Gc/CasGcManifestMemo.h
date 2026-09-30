#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasEtag.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasBlobDigest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <base/types.h>
#include <deque>
#include <unordered_map>
#include <vector>

namespace DB::Cas
{

/// One `Blob`-placement entry of a folded manifest body, in body order.
struct ManifestFoldEntry
{
    BlobRef ref;
    UInt128 source_id;   /// `sourceEdgeId(ManifestId, path)`
    String path;
};

/// What a fold needs from a validated manifest body to replay its edges without reading it again.
/// Carries no sign and no transaction ordinal: those belong to the edge being folded, not the body.
struct ManifestFold
{
    Etag etag;
    std::vector<ManifestFoldEntry> entries;
};

/// Validated manifest bodies of ONE fold, keyed by `ManifestId`. A hit is equivalent to a fresh read
/// only while no folded body is deleted during the fold, so an instance must not outlive its fold.
/// Absence is never stored: only `insert` of a validated body adds an entry.
///
/// Bounded by `budget` bytes of charged retained storage, evicting the oldest insert first. The
/// charge overestimates: per manifest the map node, the etag strings' capacity and 64 B for the
/// bucket and FIFO slots; per entry its `sizeof` and path capacity; per distinct namespace its node
/// and capacity, once, since keys share one interned copy. Each retained `Etag` key is the full
/// manifest key and so holds its own copy of the namespace, charged with the etag's capacity.
class GcManifestMemo
{
public:
    static constexpr size_t kBudgetBytes = 64ull << 20;

    explicit GcManifestMemo(size_t budget_bytes_ = kBudgetBytes);
    GcManifestMemo(const GcManifestMemo &) = delete;
    GcManifestMemo & operator=(const GcManifestMemo &) = delete;

    /// The stored fold, or nullptr. A non-null result counts as a hit.
    const ManifestFold * find(const ManifestId & id);
    /// Same lookup without counting a hit.
    bool contains(const ManifestId & id) const;
    /// Stores `fold` unless its own charge, namespace included, exceeds the budget; then returns
    /// false and changes nothing. Evicts oldest inserts until the new charge fits.
    bool insert(const ManifestId & id, ManifestFold fold);

    size_t charged() const { return charged_bytes; }
    size_t hits() const { return hit_count; }
    size_t evictions() const { return eviction_count; }

private:
    struct Key
    {
        const String * root_namespace;   /// points at the interned copy in `namespaces`
        ManifestRef ref;
        bool operator==(const Key &) const = default;
    };
    struct KeyHash
    {
        size_t operator()(const Key & key) const;
    };
    struct Slot
    {
        ManifestFold fold;
        size_t charge;
    };
    using Folds = std::unordered_map<Key, Slot, KeyHash>;
    using Namespaces = std::unordered_map<String, size_t>;   /// interned namespace -> referencing folds

    static size_t foldCharge(const ManifestFold & fold);
    static size_t namespaceCharge(const String & root_namespace);

    const String * internedNamespace(const ManifestId & id) const;
    void evictOldest();

    const size_t budget_bytes;
    Namespaces namespaces;
    Folds folds;
    std::deque<Key> insertion_order;
    size_t charged_bytes = 0;
    size_t hit_count = 0;
    size_t eviction_count = 0;
};

}
