#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

/// system.cas_mounts: per content-addressed disk, this server's mount built from memory (`is_local = 1`,
/// no object-store request) and every other mount slot listed from the pool (`is_local = 0`, one attempt
/// per request). A predicate on `disk` and `is_local` runs before any request.
/// The `is_leader` column is per-disk and supersedes the retired process-global `CasGcIsLeader` metric.
class StorageSystemContentAddressedMounts final : public IStorageSystemOneBlock
{
public:
    explicit StorageSystemContentAddressedMounts(const StorageID & table_id_);

    std::string getName() const override { return "SystemContentAddressedMounts"; }

    static ColumnsDescription getColumnsDescription();

protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * predicate, std::vector<UInt8>) const override;
    Block getFilterSampleBlock() const override;
};

}
