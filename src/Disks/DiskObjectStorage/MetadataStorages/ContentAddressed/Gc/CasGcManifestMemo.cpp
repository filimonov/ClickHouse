#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcManifestMemo.h>

#include <base/defines.h>
#include <functional>

namespace DB::Cas
{

namespace
{

/// A node-based hash container's per-element links: the next pointer and the cached hash.
constexpr size_t kNodeLinks = 2 * sizeof(void *);
/// Bucket pointer plus FIFO slot per manifest, rounded up.
constexpr size_t kPerManifestBookkeeping = 64;

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
    size_t charge = sizeof(Folds::value_type) + kNodeLinks + kPerManifestBookkeeping + fold.etag.stringCapacity()
        + fold.entries.capacity() * sizeof(ManifestFoldEntry);
    for (const ManifestFoldEntry & entry : fold.entries)
        charge += entry.path.capacity();
    return charge;
}

size_t GcManifestMemo::namespaceCharge(const String & root_namespace)
{
    return sizeof(Namespaces::value_type) + kNodeLinks + sizeof(void *) + root_namespace.capacity();
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
    /// The namespace counts even when already interned, so that evicting every other fold always
    /// makes room.
    if (fold_charge + namespaceCharge(root_namespace) > budget_bytes)
        return false;

    auto [ns_it, interned] = namespaces.try_emplace(std::move(root_namespace), 0);
    if (interned)
        charged_bytes += namespaceCharge(ns_it->first);
    ++ns_it->second;   /// held before evicting, so eviction cannot release this namespace

    while (charged_bytes + fold_charge > budget_bytes && !insertion_order.empty())
        evictOldest();
    chassert(charged_bytes + fold_charge <= budget_bytes);

    const Key key{&ns_it->first, id.ref};
    folds.emplace(key, Slot{std::move(fold), fold_charge});
    insertion_order.push_back(key);
    charged_bytes += fold_charge;
    return true;
}

void GcManifestMemo::evictOldest()
{
    const Key key = insertion_order.front();
    insertion_order.pop_front();
    const auto it = folds.find(key);
    chassert(it != folds.end());
    charged_bytes -= it->second.charge;
    folds.erase(it);
    ++eviction_count;

    const auto ns_it = namespaces.find(*key.root_namespace);
    if (--ns_it->second == 0)
    {
        charged_bytes -= namespaceCharge(ns_it->first);
        namespaces.erase(ns_it);
    }
}

}
