#pragma once

#include <Core/Block.h>
#include <Core/Settings.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/Chunk.h>
#include <Poco/Logger.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTrackerSwitcher.h>
#include <Common/SettingsChanges.h>
#include <Common/ThreadPool.h>

#include <future>
#include <shared_mutex>
#include <variant>
// #include <mutex>
// #include <unordered_map>
// #include <chrono>

namespace DB
{

/// A queue, that stores data for insert queries and periodically flushes it to tables.
/// The data is grouped by table, format and settings of insert query.
class AsynchronousInsertQueue : public WithContext
{
public:
    using Milliseconds = std::chrono::milliseconds;

    AsynchronousInsertQueue(ContextPtr context_, size_t pool_size_, bool flush_on_shutdown_);
    ~AsynchronousInsertQueue();

    struct PushResult
    {
        enum Status
        {
            OK,
            TOO_MUCH_DATA,
        };

        Status status;

        /// Future that allows to wait until the query is flushed.
        std::future<void> future{};

        /// Read buffer that contains extracted
        /// from query data in case of too much data.
        std::unique_ptr<ReadBuffer> insert_data_buffer{};

        /// Block that contains received by Native
        /// protocol data in case of too much data.
        Block insert_block{};
    };

    enum class DataKind : uint8_t
    {
        Parsed = 0,
        Preprocessed = 1,
    };

    static void validateSettings(const Settings & settings, LoggerPtr log);

    /// Force flush the whole queue.
    void flushAll();

    PushResult pushQueryWithInlinedData(ASTPtr query, ContextPtr query_context);
    PushResult pushQueryWithBlock(ASTPtr query, Block block, ContextPtr query_context);
    size_t getPoolSize() const { return pool_size; }

    /// This method should be called manually because it's not flushed automatically in dtor
    /// because all tables may be already unloaded when we destroy AsynchronousInsertQueue
    void flushAndShutdown();

    using OptionalTimePoint = std::optional<std::chrono::steady_clock::time_point>;

private:

    /// Manages per‑key (defined by insert type) insert statistics: pending bytes, moving
    /// average of insert size, timestamps of 2 last flushes, and last insert time.
    /// Lookup is under lock; thereafter all operations
    /// on a key’s stats are lock‑free via atomics.
    struct StatsManager
    {
        ///
        /// Lock‑free per‑key stats. You get one of these atomically under lock,
        /// then all updates/readers proceed without further locking.
        ///
        struct AtomicStats : std::enable_shared_from_this<AtomicStats>
        {
            AtomicStats() = default;

            using TimePoints = std::pair<OptionalTimePoint, OptionalTimePoint>;

            /// Times of the two most recent queue flushes.
            /// Used to calculate adaptive timeout.
            TimePoints getRecentTimePoints() const;
            void updateWithCurrentTime();

            /// RAII “insert intent”: reserves pending bytes on creation,
            /// allows in‑flight adjust, and on commit updates EMA & last‑insert,
            /// on destruction reverts the changes.
            class InsertIntent
            {
            public:
                InsertIntent(const InsertIntent &) = delete;
                InsertIntent & operator=(const InsertIntent &) = delete;
                InsertIntent(InsertIntent &&) noexcept;
                InsertIntent & operator=(InsertIntent &&) noexcept;
                ~InsertIntent();

                /// Adjust in‑flight reservation: delta of pending bytes, do nothing if no change
                void adjust(size_t new_bytes, size_t new_alloc);

                /// Finalize this insert: update moving average and last‑insert timestamp.
                void commit();

            private:
                friend struct AtomicStats;
                InsertIntent(std::shared_ptr<AtomicStats> stats_, size_t predicted_bytes_, size_t predicted_alloc_);

                std::shared_ptr<AtomicStats> stats;
                size_t                       bytes;
                size_t                       alloc;
            };

            /// Create a new intent for this key.
            InsertIntent createInsertIntent(size_t predicted_bytes, size_t predicted_alloc)
            {
                return InsertIntent(shared_from_this(), predicted_bytes, predicted_alloc);
            }

            /// Get current pending bytes.
            size_t getBytesPending() const noexcept { return bytes_pending.load(std::memory_order_relaxed); }

            /// Get current moving average of insert size.
            double getAverageInsertSize() const noexcept { return moving_average.load(std::memory_order_relaxed); }

            std::chrono::steady_clock::time_point getLastInsertTime() const noexcept { return last_insert.load(std::memory_order_relaxed); }

        private:

            friend struct StatsManager;

            /// Adjust the count of pending data (e.g., bytes queued but not yet flushed).
            void incrementPending(size_t amount) noexcept;
            void decrementPending(size_t amount) noexcept;

            /// Adjust the count of pending *allocated* data (e.g., capacity reserved).
            void incrementPendingAllocated(size_t amount) noexcept;
            void decrementPendingAllocated(size_t amount) noexcept;

            void updateMovingAverage(double actual) noexcept;
            void updateLastInsertTime() noexcept
            {
                last_insert.store(std::chrono::steady_clock::now(), std::memory_order_relaxed);
            }

            // how much bytes were collected for that insert type already & not yet flushed
            std::atomic<size_t> bytes_pending{0};

            // // how much bytes were collected for that insert type already & not yet flushed
            // std::atomic<size_t> bytes_pending_allocated{0};

            // average size of the insert (simple expontnial moving average), used for predicting of
            // the buffer size
            std::atomic<double> moving_average{0.0};

            std::atomic<std::chrono::steady_clock::time_point> last_insert;

            // last 2 flushes
            std::atomic<std::chrono::steady_clock::time_point> time_point1;
            std::atomic<std::chrono::steady_clock::time_point> time_point2;
        };

        using AtomicStatsPtr = std::shared_ptr<AtomicStats>;


        /// Get (or create) the AtomicStats for this key.
        AtomicStatsPtr getInsertStatistics(UInt128 key)
        {
            std::lock_guard lock(mutex);
            auto & ptr = stats_map[key];
            if (!ptr)
                ptr = std::make_shared<AtomicStats>();
            cleanupStaleNoLock();
            return ptr;
        }

    private:
        /// Remove entries where last_insert + stale_threshold < now && bytes_pending == 0
        void cleanupStaleNoLock();

        mutable std::mutex mutex;
        std::unordered_map<UInt128, AtomicStatsPtr> stats_map;

        // TODO: do we need to make it configurable?
        static constexpr std::chrono::minutes stale_threshold{10};
        static constexpr double  moving_average_alpha = 0.1;
    };


    StatsManager stats;

    struct InsertQuery
    {
    public:
        ASTPtr query;
        String query_str;
        std::optional<UUID> user_id;
        std::vector<UUID> current_roles;
        Settings settings;

        DataKind data_kind;
        UInt128 hash;

        StatsManager::AtomicStatsPtr current_insert_stats;

        InsertQuery(
            const ASTPtr & query_,
            const std::optional<UUID> & user_id_,
            const std::vector<UUID> & current_roles_,
            const Settings & settings_,
            DataKind data_kind_,
            StatsManager & stats_manager);

        InsertQuery(const InsertQuery & other) { *this = other; }
        InsertQuery & operator=(const InsertQuery & other);
        bool operator==(const InsertQuery & other) const;

    private:
        auto toTupleCmp() const { return std::tie(data_kind, query_str, user_id, current_roles, setting_changes); }

        std::vector<SettingChange> setting_changes;
    };

    struct DataChunk : public std::variant<String, Block>
    {
        using std::variant<String, Block>::variant;

        size_t byteSize() const
        {
            return std::visit([]<typename T>(const T & arg)
            {
                if constexpr (std::is_same_v<T, Block>)
                    return arg.bytes();
                else
                    return arg.size();
            }, *this);
        }

        // size_t allocatedBytes() const
        // {
        //     return std::visit([]<typename T>(const T & arg)
        //     {
        //         if constexpr (std::is_same_v<T, Block>)
        //             return arg.allocatedBytes();
        //         else
        //             return arg.capacity();
        //     }, *this);
        // }

        DataKind getDataKind() const
        {
            if (std::holds_alternative<Block>(*this))
                return DataKind::Preprocessed;
            return DataKind::Parsed;
        }

        bool empty() const
        {
            return std::visit([]<typename T>(const T & arg)
            {
                if constexpr (std::is_same_v<T, Block>)
                    return arg.rows() == 0;
                else
                    return arg.empty();
            }, *this);
        }

        const String * asString() const { return std::get_if<String>(this); }
        const Block * asBlock() const { return std::get_if<Block>(this); }
    };

    struct InsertData
    {
        struct Entry
        {
        public:
            DataChunk chunk;
            const String query_id;
            const String async_dedup_token;
            const String format;
            MemoryTracker * const user_memory_tracker;
            const std::chrono::time_point<std::chrono::system_clock> create_time;
            NameToNameMap query_parameters;

            Entry(
                DataChunk && chunk_,
                String && query_id_,
                const String & async_dedup_token_,
                const String & format_,
                MemoryTracker * user_memory_tracker_,
                StatsManager::AtomicStats::InsertIntent && insert_intent_);

            void resetChunk();
            void finish(std::exception_ptr exception_ = nullptr);

            std::future<void> getFuture() { return promise.get_future(); }
            bool isFinished() const { return finished; }

        private:
            StatsManager::AtomicStats::InsertIntent insert_intent;
            std::promise<void> promise;
            std::atomic_bool finished = false;
        };

        InsertData() = default;
        explicit InsertData(Milliseconds timeout_ms_) : timeout_ms(timeout_ms_) { }

        ~InsertData()
        {
            auto it = entries.begin();
            // Entries must be destroyed in context of user who runs async insert.
            // Each entry in the list may correspond to a different user,
            // so we need to switch current thread's MemoryTracker parent on each iteration.
            while (it != entries.end())
            {
                MemoryTrackerSwitcher switcher((*it)->user_memory_tracker);
                it = entries.erase(it);
            }
        }

        using EntryPtr = std::shared_ptr<Entry>;

        std::list<EntryPtr> entries;
        size_t size_in_bytes = 0;
        Milliseconds timeout_ms = Milliseconds::zero();
    };

    using InsertDataPtr = std::unique_ptr<InsertData>;

    struct Container
    {
        InsertQuery key;
        InsertDataPtr data;
    };

    /// Ordered container
    /// Key is a timestamp of the first insert into batch.
    /// Used to detect for how long the batch is active, so we can dump it by timer.
    using Queue = std::map<std::chrono::steady_clock::time_point, Container>;
    using QueueIterator = Queue::iterator;
    using QueueIteratorByKey = std::unordered_map<UInt128, QueueIterator>;

    struct QueueShard
    {
        mutable std::mutex mutex;
        mutable std::condition_variable are_tasks_available;

        Queue queue;
        QueueIteratorByKey iterators;

        OptionalTimePoint last_insert_time;
        std::chrono::milliseconds busy_timeout_ms;
    };

    const size_t pool_size;
    const bool flush_on_shutdown;

    std::vector<QueueShard> queue_shards;

    /// Logic and events behind queue are as follows:
    ///  - async_insert_busy_timeout_ms:
    ///   if queue is active for too long and there are a lot of rapid inserts, then we dump the data, so it doesn't
    ///   grow for a long period of time and users will be able to select new data in deterministic manner.
    ///
    /// During processing incoming INSERT queries we can also check whether the maximum size of data in buffer is reached
    /// (async_insert_max_data_size setting). If so, then again we dump the data.

    std::atomic<bool> shutdown{false};
    std::atomic<bool> flush_stopped{false};

    /// A mutex that prevents concurrent forced flushes of queue.
    mutable std::mutex flush_mutex;

    /// Dump the data only inside this pool.
    ThreadPool pool;

    /// Uses async_insert_busy_timeout_ms and processBatchDeadlines()
    std::vector<ThreadFromGlobalPool> dump_by_first_update_threads;

    LoggerPtr log = getLogger("AsynchronousInsertQueue");

    PushResult pushDataChunk(ASTPtr query, DataChunk chunk, ContextPtr query_context, InsertQuery key, StatsManager::AtomicStats::InsertIntent insert_intent);

    Milliseconds getBusyWaitTimeoutMs(
        const Settings & settings,
        const QueueShard & shard,
        const StatsManager::AtomicStats::TimePoints & flush_time_points,
        std::chrono::steady_clock::time_point now) const;

    InsertQuery preprocessInsertQuery(const ASTPtr & query, const ContextPtr & query_context, DataKind data_kind);

    void processBatchDeadlines(size_t shard_num);
    void scheduleDataProcessingJob(const InsertQuery & key, InsertDataPtr data, ContextPtr global_context);

    static void processData(
        InsertQuery key, InsertDataPtr data, ContextPtr global_context);

    template <typename LogFunc>
    static Chunk processEntriesWithParsing(
        const InsertQuery & key,
        const InsertDataPtr & data,
        const Block & header,
        const ContextPtr & insert_context,
        LoggerPtr logger,
        LogFunc && add_to_async_insert_log);

    template <typename LogFunc>
    static Chunk processPreprocessedEntries(
        const InsertDataPtr & data,
        const Block & header,
        LogFunc && add_to_async_insert_log);

    template <typename E>
    static void finishWithException(const ASTPtr & query, const std::list<InsertData::EntryPtr> & entries, const E & exception);

public:
    auto getQueueLocked(size_t shard_num) const
    {
        const auto & shard = queue_shards[shard_num];
        std::unique_lock lock(shard.mutex);
        return std::make_pair(std::ref(shard.queue), std::move(lock));
    }
};

}
