#include <Storages/System/StorageSystemContentAddressedMounts.h>
#include <DataTypes/DataTypesNumber.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsDateTime.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Storages/VirtualColumnUtils.h>
#include <Interpreters/Context.h>
#include <Common/assert_cast.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <chrono>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_STATE;
}

StorageSystemContentAddressedMounts::StorageSystemContentAddressedMounts(const StorageID & table_id_)
    : IStorageSystemOneBlock(table_id_, getColumnsDescription())
{
}

ColumnsDescription StorageSystemContentAddressedMounts::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"disk", std::make_shared<DataTypeString>(), "Name of the content-addressed disk."},
        {"server_root_id", std::make_shared<DataTypeString>(), "Server root id owning the mount slot."},
        {"server_uuid", std::make_shared<DataTypeUUID>(), "UUID of the server incarnation holding the lease."},
        {"hostname", std::make_shared<DataTypeString>(), "Hostname recorded in the lease body."},
        {"process_id", std::make_shared<DataTypeUInt64>(), "Process id recorded in the lease body."},
        {"writer_epoch", std::make_shared<DataTypeUInt64>(), "Fenced writer epoch of the incarnation."},
        {"renewal_sequence", std::make_shared<DataTypeUInt64>(), "Lease renewal sequence number."},
        {"started_at", std::make_shared<DataTypeDateTime64>(3), "Time when the lease started."},
        {"expires_at", std::make_shared<DataTypeDateTime64>(3), "Time when the lease expires."},
        {"min_active_build_sequence", std::make_shared<DataTypeUInt64>(), "Oldest in-flight build sequence (UINT64_MAX means the mount said farewell)."},
        {"gc_fenced", std::make_shared<DataTypeUInt8>(), "1 if GC fenced this slot out (terminal)."},
        {"state", std::make_shared<DataTypeString>(), "Mount slot state: live, expired, terminated, fenced or corrupt. On the local row, the state of the body this server last confirmed, not read from the store."},
        {"is_leader", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>()), "1 if this server's GC scheduler holds this disk's leadership lease. NULL on rows describing other servers' mounts."},
        {"pending_reclaim", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()), "Cumulative condemned-minus-deleted backlog observed by this process's GC on this disk. NULL on rows describing other servers' mounts."},
        {"last_success_age_seconds", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "Seconds since this disk's GC last led a round (0 if it never led). NULL on rows describing other servers' mounts."},
        {"wedged_namespace_count", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "Ref-append lanes currently wedged on this disk. NULL on rows describing other servers' mounts."},
        {"lifecycle", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "This server's lifecycle for the disk, from the confirmed lease and the clock: live, paused, stopping, closed, constructing (never started) or shutdown (torn down). NULL on rows of other servers' mounts."},
        {"lifecycle_reason", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "One word: renewal_overdue, lease_expired or fenced_by_gc for paused, the end word for stopping, forgotten for closed, empty otherwise. NULL on rows of other servers' mounts."},
        {"lifecycle_detail", std::make_shared<DataTypeString>(), "The full typed reason text naming the actual cause when not live: what ended the mount, or the decommission by SYSTEM CAS FORGET at <time>. Empty when live. Empty on rows of other servers' mounts."},
        {"lifecycle_since", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "When this server entered the current non-live lifecycle state; for a pause from the lease clock, when the current reason's threshold was crossed (for lease_expired, the lease deadline). NULL when live (or the state is not backed by a live pool). NULL on rows of other servers' mounts."},
        {"renewal_in_progress", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>()), "1 while this server renews its mount lease. NULL on rows describing other servers' mounts."},
        {"renewal_attempts", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "Conditional PUTs sent by the current lease renewal, or by the last one once it ended. NULL on rows describing other servers' mounts."},
        {"last_renewal_error", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "The last failed request of that renewal: '<VERB> <key>: <refused|transport> <code> <message>'. NULL when none failed, and on rows describing other servers' mounts."},
        {"is_local", std::make_shared<DataTypeUInt8>(), "1 for this server's own mount, built from memory without a request to the object store; 0 for a slot listed from the pool."},
        {"listing_error", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "The error of a failed listing, on the one row it produces. NULL on every other row."},
    };
}

Block StorageSystemContentAddressedMounts::getFilterSampleBlock() const
{
    return {
        {{}, std::make_shared<DataTypeString>(), "disk"},
        {{}, std::make_shared<DataTypeUInt8>(), "is_local"},
    };
}

namespace
{
/// Facts about this server; absent on rows listed from the pool.
struct LocalFacts
{
    CasLifecycleSnapshot lifecycle;
    std::optional<Cas::CasGcScheduler::GcHealth> health;
    std::optional<Cas::RenewalProgress> progress;
};

/// A disk that is not mounted or is terminal gets a local row and no listing.
Cas::PoolPtr storeOrNull(ContentAddressedMetadataStorage & ca)
{
    try
    {
        return ca.store();
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::INVALID_STATE)
            throw;
        return nullptr;
    }
}

uint64_t skewMarginMs(const Cas::Pool & store)
{
    return static_cast<uint64_t>(store.poolConfig().mount_lease_ttl_ms.count()) / 2;
}
}

void StorageSystemContentAddressedMounts::fillData(
    MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * predicate, std::vector<UInt8>) const
{
    const uint64_t now_ms = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());

    const auto emit = [&](const String & disk, const String & srid, const Cas::MountLease & b, const String & state,
                          const LocalFacts * local, const std::optional<String> & listing_error)
    {
        const auto health = local ? local->health : std::nullopt;
        const auto progress = local ? local->progress : std::nullopt;
        size_t c = 0;
        res_columns[c++]->insert(disk);
        res_columns[c++]->insert(srid);
        assert_cast<ColumnUUID &>(*res_columns[c++]).insertValue(UUID(b.server_uuid));
        res_columns[c++]->insert(b.hostname);
        res_columns[c++]->insert(b.pid);
        res_columns[c++]->insert(b.writer_epoch);
        res_columns[c++]->insert(b.seq);
        assert_cast<ColumnDateTime64 &>(*res_columns[c++]).insertValue(static_cast<Decimal64>(b.started_at_ms));
        assert_cast<ColumnDateTime64 &>(*res_columns[c++]).insertValue(static_cast<Decimal64>(b.expires_at_ms));
        res_columns[c++]->insert(b.min_active_build_sequence);
        res_columns[c++]->insert(static_cast<UInt8>(b.gc_fenced));
        res_columns[c++]->insert(state);
        res_columns[c++]->insert(health ? Field(static_cast<UInt64>(health->is_leader)) : Field());
        res_columns[c++]->insert(health ? Field(health->pending_reclaim) : Field());
        res_columns[c++]->insert(health ? Field(health->last_success_age_seconds) : Field());
        res_columns[c++]->insert(health ? Field(health->wedged_namespace_count) : Field());
        res_columns[c++]->insert(local ? Field(local->lifecycle.lifecycle) : Field());
        res_columns[c++]->insert(local ? Field(local->lifecycle.reason) : Field());
        res_columns[c++]->insert(local ? local->lifecycle.detail : String{});
        res_columns[c++]->insert(local && local->lifecycle.since != 0 ? Field(static_cast<UInt64>(local->lifecycle.since)) : Field());
        res_columns[c++]->insert(progress ? Field(static_cast<UInt64>(progress->in_progress)) : Field());
        res_columns[c++]->insert(progress ? Field(static_cast<UInt64>(progress->attempts_sent)) : Field());
        res_columns[c++]->insert(progress && progress->last_error ? Field(*progress->last_error) : Field());
        res_columns[c++]->insert(static_cast<UInt64>(local != nullptr));
        res_columns[c++]->insert(listing_error ? Field(*listing_error) : Field());
    };

    const auto emitLocal = [&](const String & disk, ContentAddressedMetadataStorage & ca)
    {
        LocalFacts local{.lifecycle = ca.lifecycleSnapshot(), .health = ca.gcHealth(), .progress = {}};
        std::optional<Cas::MountLease> body;
        String state;
        if (const Cas::PoolPtr store = storeOrNull(ca))
        {
            local.progress = store->renewalProgress();
            body = store->confirmedMountBody();
            if (body)
                state = Cas::classifyMountBody(*body, now_ms, skewMarginMs(*store));
        }
        emit(disk, local.lifecycle.server_root_id, body.value_or(Cas::MountLease{}), state, &local, std::nullopt);
    };

    const auto emitListed = [&](const String & disk, ContentAddressedMetadataStorage & ca)
    {
        const Cas::PoolPtr store = storeOrNull(ca);
        if (!store)
            return;
        /// A read-only disk holds no slot: the slot under its `server_root_id` belongs to another disk.
        const auto & config = store->poolConfig();
        const String own_srid = config.read_only ? String{} : config.server_root_id;
        std::vector<Cas::MountInfo> mounts;
        try
        {
            /// The open fence: these rows matter most when the local mount fence has run down.
            Cas::CasOperation op = store->openRequests().admit();
            mounts = Cas::listMounts(op, store->layout(), now_ms, skewMarginMs(*store), Cas::Retry::once(), own_srid);
        }
        catch (...)
        {
            /// One row, so a failed listing does not read as an empty pool.
            tryLogCurrentException(getLogger("StorageSystemContentAddressedMounts"), "listing mounts for disk '" + disk + "'");
            emit(disk, "", Cas::MountLease{}, "", nullptr, getCurrentExceptionMessage(false));
            return;
        }
        for (const auto & m : mounts)
            emit(disk, m.srid, m.lease, m.state, nullptr, std::nullopt);
    };

    const auto disks = context->getDisksMap();
    MutableColumnPtr candidate_disk = ColumnString::create();
    MutableColumnPtr candidate_is_local = ColumnUInt8::create();
    for (const auto & [disk_name, disk] : disks)
    {
        if (!ContentAddressedMetadataStorage::tryFromDisk(disk))
            continue;
        for (const UInt8 is_local : {UInt8{1}, UInt8{0}})
        {
            candidate_disk->insert(disk_name);
            candidate_is_local->insert(is_local);
        }
    }
    Block candidates{
        {ColumnPtr(std::move(candidate_disk)), std::make_shared<DataTypeString>(), "disk"},
        {ColumnPtr(std::move(candidate_is_local)), std::make_shared<DataTypeUInt8>(), "is_local"},
    };
    VirtualColumnUtils::filterBlockWithPredicate(predicate, candidates, context);
    const IColumn & kept_disk = *candidates.getByName("disk").column;
    const IColumn & kept_is_local = *candidates.getByName("is_local").column;
    for (size_t i = 0; i < candidates.rows(); ++i)
    {
        const String disk_name = kept_disk[i].safeGet<String>();
        auto & ca = *ContentAddressedMetadataStorage::tryFromDisk(disks.at(disk_name));
        if (kept_is_local.getUInt(i))
            emitLocal(disk_name, ca);
        else
            emitListed(disk_name, ca);
    }
}

}
