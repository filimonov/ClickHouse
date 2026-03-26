#pragma once

#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>

#include <atomic>
#include <functional>
#include <string_view>

namespace DB
{

/// Generic async/sync dispatch for file operations in Keeper.
/// No domain knowledge — just schedules tasks on pools with sync fallback.
///
/// Behavior:
/// - disabled: run inline
/// - enabled + schedule succeeds: run on pool
/// - enabled + schedule fails: log + run inline
/// - after shutdown(): run inline
class KeeperFileOperationsExecutor
{
public:
    KeeperFileOperationsExecutor(
        bool enabled_,
        LoggerPtr log_,
        size_t cleanup_threads,
        size_t cleanup_max_queue,
        size_t prepare_threads);

    ~KeeperFileOperationsExecutor();

    /// Schedule a cleanup task (close, unlink, rename). Serialized pool.
    /// `what` is for logging (e.g. "close old changelog", "remove snapshot").
    void runCleanupTask(std::string_view what, std::function<void()> task);

    /// Schedule a preparation task (openat, fallocate). Concurrent pool.
    void runPrepareTask(std::string_view what, std::function<void()> task);

    /// Wait for both pools to drain.
    void wait();

    /// Mark as shutting down (subsequent tasks run inline) and wait.
    void shutdown();

private:
    bool enabled;
    std::atomic<bool> shutting_down{false};
    LoggerPtr log;
    ThreadPool cleanup_pool;
    ThreadPool prepare_pool;
};

}
