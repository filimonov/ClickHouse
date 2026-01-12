#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class StorageSystemCustomVariables final : public IStorageSystemOneBlock
{
public:
    StorageSystemCustomVariables(const StorageID & storage_id_, ColumnsDescription columns_description_);

    std::string getName() const override { return "SystemCustomVariables"; }

    static ColumnsDescription getColumnsDescription();

protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
