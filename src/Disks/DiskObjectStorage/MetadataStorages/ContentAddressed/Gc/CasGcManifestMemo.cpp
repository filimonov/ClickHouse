#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcManifestMemo.h>

#include <Common/Exception.h>
#include <functional>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

namespace DB::Cas
{

namespace
{

/// A node-based hash container's per-element links: the next pointer and the cached hash.
constexpr size_t kNodeLinks = 2 * sizeof(void *);
/// A list node's links: the next and previous pointers.
constexpr size_t kListLinks = 2 * sizeof(void *);

void hashCombine(size_t & seed, size_t value)
{
    seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
}

}

GcManifestMemo::GcManifestMemo(size_t budget_bytes_)
    : budget_bytes(budget_bytes_)
{
}

size_t GcManifestMemo::KeyHash::operator()(const Key & key) const
{
    size_t seed = std::hash<const String *>{}(key.root_namespace);
    hashCombine(seed, std::hash<uint64_t>{}(key.ref.writer_epoch));
    hashCombine(seed, std::hash<uint64_t>{}(key.ref.build_sequence));
    hashCombine(seed, std::hash<uint32_t>{}(key.ref.manifest_ordinal));
    return seed;
}

size_t GcManifestMemo::foldCharge(const ManifestFold & fold)
{
    size_t charge = sizeof(Folds::value_type) + kNodeLinks + sizeof(Key) + kListLinks + fold.etag.stringCapacity()
        + fold.entries.capacity() * sizeof(ManifestFoldEntry);
    for (const ManifestFoldEntry & entry : fold.entries)
        charge += entry.path.capacity();
    return charge;
}

size_t GcManifestMemo::namespaceCharge(const String & root_namespace)
{
    return sizeof(Namespaces::value_type) + kNodeLinks + root_namespace.capacity();
}

size_t GcManifestMemo::bucketBytes() const
{
    return (folds.bucket_count() + namespaces.bucket_count()) * sizeof(void *);
}

const String * GcManifestMemo::internedNamespace(const ManifestId & id) const
{
    const auto it = namespaces.find(id.root_namespace.string());
    return it == namespaces.end() ? nullptr : &it->first;
}

const ManifestFold * GcManifestMemo::find(const ManifestId & id)
{
    const String * root_namespace = internedNamespace(id);
    if (!root_namespace)
        return nullptr;
    const auto it = folds.find(Key{root_namespace, id.ref});
    if (it == folds.end())
        return nullptr;
    ++hit_count;
    return &it->second.fold;
}

bool GcManifestMemo::contains(const ManifestId & id) const
{
    const String * root_namespace = internedNamespace(id);
    return root_namespace && folds.contains(Key{root_namespace, id.ref});
}

bool GcManifestMemo::insert(const ManifestId & id, ManifestFold fold)
{
    if (contains(id))
        return true;

    String root_namespace = id.root_namespace.string();
    const size_t fold_charge = foldCharge(fold);
    /// The namespace counts even when already interned, so that evicting every other fold makes room
    /// unless a bucket array grows.
    if (fold_charge + namespaceCharge(root_namespace) + bucketBytes() > budget_bytes)
        return false;

    auto [ns_it, interned] = namespaces.try_emplace(std::move(root_namespace), 0);
    if (interned)
        charged_bytes += namespaceCharge(ns_it->first);
    ++ns_it->second;   /// held before evicting, so eviction cannot release this namespace

    while (charged() + fold_charge > budget_bytes && !insertion_order.empty())
        evictOldest();
    if (charged() + fold_charge > budget_bytes)
    {
        releaseNamespace(ns_it->first);
        return false;
    }

    std::list<Key> order_node{Key{&ns_it->first, id.ref}};
    folds.emplace(order_node.front(), Slot{std::move(fold), fold_charge});
    insertion_order.splice(insertion_order.end(), order_node);
    charged_bytes += fold_charge;

    /// The emplace may have grown the bucket array. The new fold is the newest, so it goes last.
    while (charged() > budget_bytes && !insertion_order.empty())
        evictOldest();
    return !insertion_order.empty();
}

void GcManifestMemo::evictOldest()
{
    const Key key = insertion_order.front();
    const auto it = folds.find(key);
    if (it == folds.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "GC manifest memo: the eviction order names a manifest the memo does not hold");
    insertion_order.pop_front();
    charged_bytes -= it->second.charge;
    folds.erase(it);
    ++eviction_count;
    releaseNamespace(*key.root_namespace);
}

void GcManifestMemo::releaseNamespace(const String & root_namespace)
{
    const auto it = namespaces.find(root_namespace);
    if (it == namespaces.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "GC manifest memo: a held namespace is not interned");
    if (--it->second == 0)
    {
        charged_bytes -= namespaceCharge(it->first);
        namespaces.erase(it);
    }
}

}
