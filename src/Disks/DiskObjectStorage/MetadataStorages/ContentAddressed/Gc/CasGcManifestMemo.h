#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasEtag.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasBlobDigest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <base/types.h>
#include <list>
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
/// charge overestimates: per manifest the map and FIFO nodes and the etag strings' capacity; per
/// entry its `sizeof` and path capacity; per distinct namespace its node and capacity, once, since
/// keys share one interned copy; and the hash tables' bucket arrays as they are, since eviction does
/// not shrink them. Each retained `Etag` key is the full manifest key and so holds its own copy of
/// the namespace, charged with the etag's capacity.
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
    /// Evicts oldest inserts until `fold` fits and stores it. Returns false without storing when it
    /// cannot fit even alone; without evicting when its charge plus the current bucket arrays already
    /// exceeds the budget.
    bool insert(const ManifestId & id, ManifestFold fold);

    size_t charged() const { return charged_bytes + bucketBytes(); }
    /// The bucket arrays' share of `charged`.
    size_t bucketBytes() const;
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
    void releaseNamespace(const String & root_namespace);

    const size_t budget_bytes;
    Namespaces namespaces;
    Folds folds;
    /// A list, unlike a deque, frees its storage as eviction pops it.
    std::list<Key> insertion_order;
    size_t charged_bytes = 0;   /// what eviction frees; `bucketBytes` is measured apart
    size_t hit_count = 0;
    size_t eviction_count = 0;
};

}
