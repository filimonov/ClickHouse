#pragma once

#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>

#include <atomic>
#include <functional>
#include <future>
#include <string_view>

namespace DB
{

/// Generic async/sync dispatch for file operations in Keeper.
/// No domain knowledge — just schedules tasks on pools with sync fallback.
///
/// Behavior:
/// - disabled: run inline, return ready future
/// - enabled + schedule succeeds: run on pool, return future
/// - enabled + schedule fails: log + run inline, return ready future
/// - after shutdown(): run inline, return ready future
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
    /// Returns a future that completes when the task finishes.
    std::shared_future<void> runCleanupTask(std::string_view what, std::function<void()> task);

    /// Schedule a preparation task (openat, fallocate). Concurrent pool.
    /// Returns a future that completes when the task finishes.
    std::shared_future<void> runPrepareTask(std::string_view what, std::function<void()> task);

    /// Wait for both pools to drain.
    void wait();

    /// Mark as shutting down (subsequent tasks run inline) and wait.
    void shutdown();

private:
    /// Run task on the given pool, or inline as fallback. Returns a future.
    std::shared_future<void> dispatch(
        std::string_view what, std::function<void()> task,
        ThreadPool & pool, ThreadName thread_name);

    /// Run task inline and return a ready future.
    std::shared_future<void> runInline(std::string_view what, std::function<void()> task);

    bool enabled;
    std::atomic<bool> shutting_down{false};
    LoggerPtr log;
    ThreadPool cleanup_pool;
    ThreadPool prepare_pool;
};

}
