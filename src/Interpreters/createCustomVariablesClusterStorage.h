#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/CustomVariablesClusterStorage.h>

namespace DB
{

/// Returns nullptr if <custom_variables_zookeeper_path> is not configured.
CustomVariablesClusterStoragePtr createCustomVariablesClusterStorage(const ContextMutablePtr & global_context);

}
