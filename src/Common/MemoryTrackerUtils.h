#pragma once

#include <memory>
#include <optional>
#include <base/types.h>
#include <Common/MemoryTracker.h>

/// Return most strict (by hard limit) system (non query-level, i.e. server/user/merges/...) memory limit
std::optional<UInt64> getMostStrictAvailableSystemMemory();

std::optional<UInt64> getCurrentQueryHardLimit();

/// Return current query tracked memory usage
Int64 getCurrentQueryMemoryUsage();

/// Create a memory tracker under the current query memory tracker.
std::unique_ptr<MemoryTracker> tryCreateMemoryTrackerUnderCurrentQuery();
