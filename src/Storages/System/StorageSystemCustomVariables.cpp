#include <Storages/System/StorageSystemCustomVariables.h>

#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <Common/FieldVisitorToString.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/CustomVariablesManager.h>
#include <Interpreters/formatWithPossiblyHidingSecrets.h>

#include <mutex>

namespace DB
{

StorageSystemCustomVariables::StorageSystemCustomVariables(const StorageID & storage_id_, ColumnsDescription columns_description_)
    : IStorageSystemOneBlock(storage_id_, std::move(columns_description_))
{
}

ColumnsDescription StorageSystemCustomVariables::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Variable name without the scope prefix."},
        {"scope", std::make_shared<DataTypeString>(), "Variable scope (local, local_persistent, session, cluster)."},
        {"value", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Last known value as string, NULL if missing."},
        {"load_time", std::make_shared<DataTypeDateTime>(), "Time when the variable definition was loaded into memory."},
        {"last_update", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "Time of the last update attempt."},
        {"refresh_next_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "Next scheduled refresh time."},
        {"last_update_hostname", std::make_shared<DataTypeString>(), "Hostname of the last updater."},
        {"last_successful_update", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "Time of the last successful update."},
        {"refresh_interval", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "Refresh interval in seconds."},
        {"expression", std::make_shared<DataTypeString>(), "Variable definition expression."},
        {"type", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Declared type of the variable."},
        {"last_error", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Last error message, if any."},
        {"last_error_type", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Last error type, if any."},
        {"has_value", std::make_shared<DataTypeUInt8>(), "Whether a last-good value exists."},
        {"is_valid", std::make_shared<DataTypeUInt8>(), "Whether the last refresh attempt succeeded."}
    };
}

void StorageSystemCustomVariables::fillData(
    MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto access = context->getAccess();
    if (!access->isGranted(AccessType::SHOW_CUSTOM_VARIABLES))
        return;

    auto append_entries = [&](const CustomVariablesManager::Entries & entries)
    {
        for (const auto & entry : entries)
        {
            const auto & definition = entry->definition;
            auto value = entry->value.load();

            size_t col = 0;
            res_columns[col++]->insert(definition.key.name);
            res_columns[col++]->insert(CustomVariableName::scopeToString(definition.key.scope));

            if (value && value->has_value)
                res_columns[col++]->insert(applyVisitor(FieldVisitorToString(), value->value));
            else
                res_columns[col++]->insertDefault();

            res_columns[col++]->insert(static_cast<UInt64>(std::chrono::system_clock::to_time_t(definition.load_time)));

            if (value)
                res_columns[col++]->insert(static_cast<UInt64>(std::chrono::system_clock::to_time_t(value->last_update_time)));
            else
                res_columns[col++]->insertDefault();

            if (entry->refresh)
            {
                std::lock_guard refresh_lock(entry->refresh->mutex);
                if (entry->refresh->next_refresh_time.time_since_epoch().count() != 0)
                    res_columns[col++]->insert(static_cast<UInt64>(std::chrono::system_clock::to_time_t(entry->refresh->next_refresh_time)));
                else
                    res_columns[col++]->insertDefault();
            }
            else
            {
                res_columns[col++]->insertDefault();
            }

            if (value && !value->last_update_hostname.empty())
                res_columns[col++]->insert(value->last_update_hostname);
            else
                res_columns[col++]->insertDefault();

            if (value && value->has_value)
                res_columns[col++]->insert(static_cast<UInt64>(std::chrono::system_clock::to_time_t(value->last_successful_update_time)));
            else
                res_columns[col++]->insertDefault();

            if (entry->refresh)
            {
                std::lock_guard refresh_lock(entry->refresh->mutex);
                if (entry->refresh->schedule.period.months == 0 && entry->refresh->schedule.period.seconds > 0)
                    res_columns[col++]->insert(static_cast<UInt64>(entry->refresh->schedule.period.seconds));
                else
                    res_columns[col++]->insertDefault();
            }
            else
            {
                res_columns[col++]->insertDefault();
            }

            res_columns[col++]->insert(definition.expression ? format({context, *definition.expression}) : "");

            if (definition.declared_type)
                res_columns[col++]->insert(definition.declared_type->getName());
            else if (value && value->runtime_type)
                res_columns[col++]->insert(value->runtime_type->getName());
            else
                res_columns[col++]->insertDefault();

            if (value && !value->last_error.empty())
                res_columns[col++]->insert(value->last_error);
            else
                res_columns[col++]->insertDefault();

            if (value && !value->last_error_type.empty())
                res_columns[col++]->insert(value->last_error_type);
            else
                res_columns[col++]->insertDefault();

            res_columns[col++]->insert(static_cast<UInt8>(value && value->has_value));
            res_columns[col++]->insert(static_cast<UInt8>(value && value->is_valid));
        }
    };

        append_entries(context->getCustomVariablesManager().getAllEntries());
    if (context->hasSessionContext())
        append_entries(context->getSessionCustomVariablesManager().getAllEntries());
}

}
