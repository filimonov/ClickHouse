#include <Interpreters/AsynchronousInsertQueue.h>

#include <Access/Common/AccessFlags.h>
#include <Access/EnabledQuota.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/LimitReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/copyData.h>
#include <Interpreters/AsynchronousInsertLog.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/queryToString.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Processors/Transforms/getSourceFromASTInsertQuery.h>
#include <QueryPipeline/BlockIO.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/IStorage.h>
#include <Common/CurrentThread.h>
#include <Common/DateLUT.h>
#include <Common/FieldVisitorHash.h>
#include <Common/SensitiveDataMasker.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/formatReadable.h>


namespace CurrentMetrics
{
    extern const Metric PendingAsyncInsert;
    extern const Metric PendingAsyncInsertBytes;
    extern const Metric PendingAsyncInsertBytesAllocated;
    extern const Metric AsynchronousInsertThreads;
    extern const Metric AsynchronousInsertThreadsActive;
    extern const Metric AsynchronousInsertThreadsScheduled;
    extern const Metric AsynchronousInsertQueueSize;
    extern const Metric AsynchronousInsertQueueBytes;
}

namespace ProfileEvents
{
    extern const Event AsyncInsertQuery;
    extern const Event AsyncInsertBytes;
    extern const Event AsyncInsertRows;
    extern const Event FailedAsyncInsertQuery;
    extern const Event RejectedAsyncInserts;
    extern const Event DelayedAsyncInserts;
}

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 allow_experimental_parallel_reading_from_replicas;
    extern const SettingsBool allow_experimental_query_deduplication;
    extern const SettingsDouble async_insert_busy_timeout_decrease_rate;
    extern const SettingsDouble async_insert_busy_timeout_increase_rate;
    extern const SettingsMilliseconds async_insert_busy_timeout_min_ms;
    extern const SettingsMilliseconds async_insert_busy_timeout_max_ms;
    extern const SettingsBool async_insert_deduplicate;
    extern const SettingsUInt64 async_insert_max_data_size;
    extern const SettingsUInt64 async_insert_max_query_number;
    extern const SettingsMilliseconds async_insert_poll_timeout_ms;
    extern const SettingsBool async_insert_use_adaptive_busy_timeout;
    extern const SettingsBool empty_result_for_aggregation_by_empty_set;
    extern const SettingsBool insert_allow_materialized_columns;
    extern const SettingsString insert_deduplication_token;
    extern const SettingsBool input_format_defaults_for_omitted_fields;
    extern const SettingsUInt64 log_queries_cut_to_length;
    extern const SettingsUInt64 max_columns_to_read;
    extern const SettingsNonZeroUInt64 max_parallel_replicas;
    extern const SettingsBool optimize_trivial_count_query;
    extern const SettingsUInt64 parallel_replicas_count;
    extern const SettingsString parallel_replicas_custom_key;
    extern const SettingsUInt64 parallel_replicas_custom_key_range_lower;
    extern const SettingsUInt64 parallel_replica_offset;
    extern const SettingsInt64 async_insert_preallocate_buffer_size;
    extern const SettingsUInt64 async_insert_bytes_to_throw_insert;
    extern const SettingsUInt64 async_insert_bytes_to_delay_insert;
    extern const SettingsUInt64 async_insert_rate_limit_bytes_per_second;
}

namespace ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
    extern const int UNKNOWN_EXCEPTION;
    extern const int UNKNOWN_FORMAT;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int INVALID_SETTING_VALUE;
    extern const int LIMIT_EXCEEDED;
}

static const NameSet settings_to_skip
{
    /// We don't consider this setting because it is only for deduplication,
    /// which means we can put two inserts with different tokens in the same block safely.
    "insert_deduplication_token",
    "log_comment",
};

void AsynchronousInsertQueue::StatsManager::AtomicStats::updateMovingAverage(double actual) noexcept
{
    double old_ema = moving_average.load(std::memory_order_relaxed);

    while (true)
    {
        double new_ema = (old_ema <= 0.0)
            ? actual
            : moving_average_alpha * actual + (1.0 - moving_average_alpha) * old_ema;

        if (moving_average.compare_exchange_weak(
                old_ema, new_ema,
                std::memory_order_relaxed,
                std::memory_order_relaxed))
        {
            break;
        }
        // compare_exchange_weak updates old_ema on failure
    }
}

void AsynchronousInsertQueue::StatsManager::AtomicStats::incrementPending(size_t amount) noexcept
{
    bytes_pending.fetch_add(amount, std::memory_order_relaxed);
    CurrentMetrics::add(CurrentMetrics::PendingAsyncInsertBytes, amount);
}

void AsynchronousInsertQueue::StatsManager::AtomicStats::decrementPending(size_t amount) noexcept
{
    bytes_pending.fetch_sub(amount, std::memory_order_relaxed);
    CurrentMetrics::sub(CurrentMetrics::PendingAsyncInsertBytes, amount);
}

void AsynchronousInsertQueue::StatsManager::AtomicStats::incrementPendingAllocated(size_t amount) noexcept
{
    // bytes_pending_allocated.fetch_add(amount, std::memory_order_relaxed);
    CurrentMetrics::add(CurrentMetrics::PendingAsyncInsertBytesAllocated, amount);
}

void AsynchronousInsertQueue::StatsManager::AtomicStats::decrementPendingAllocated(size_t amount) noexcept
{
    // bytes_pending_allocated.fetch_sub(amount, std::memory_order_relaxed);
    CurrentMetrics::sub(CurrentMetrics::PendingAsyncInsertBytesAllocated, amount);
}

AsynchronousInsertQueue::StatsManager::AtomicStats::InsertIntent::InsertIntent(
    std::shared_ptr<AsynchronousInsertQueue::StatsManager::AtomicStats> stats_,
    size_t predicted_bytes_,
    size_t predicted_alloc_)
    : stats(std::move(stats_))
    , bytes(predicted_bytes_)
    , alloc(predicted_alloc_)
{
    stats->incrementPending(bytes);
    stats->incrementPendingAllocated(alloc);
    CurrentMetrics::add(CurrentMetrics::PendingAsyncInsert);
}

AsynchronousInsertQueue::StatsManager::AtomicStats::InsertIntent::~InsertIntent()
{
    if (stats)
    {
        CurrentMetrics::sub(CurrentMetrics::PendingAsyncInsert);
        stats->decrementPending(bytes);
        stats->decrementPendingAllocated(alloc);
    }
}

// move constuctors
AsynchronousInsertQueue::StatsManager::AtomicStats::InsertIntent::InsertIntent(StatsManager::AtomicStats::InsertIntent && o) noexcept
    : stats(std::move(o.stats))
    , bytes(o.bytes)
    , alloc(o.alloc)
{
    o.stats.reset();
    o.bytes = 0;
    o.alloc = 0;
}

AsynchronousInsertQueue::StatsManager::AtomicStats::InsertIntent &
AsynchronousInsertQueue::StatsManager::AtomicStats::InsertIntent::operator=(StatsManager::AtomicStats::InsertIntent && o) noexcept
{
    if (this != &o)
    {
        if (stats)
        {
            CurrentMetrics::sub(CurrentMetrics::PendingAsyncInsert);
            stats->decrementPending(bytes);
            stats->decrementPendingAllocated(alloc);
        }

        stats = std::move(o.stats);
        bytes = o.bytes;
        alloc = o.alloc;

        o.stats.reset();
        o.bytes = 0;
        o.alloc = 0;
    }
    return *this;
}

void AsynchronousInsertQueue::StatsManager::AtomicStats::InsertIntent::adjust(size_t new_bytes, size_t new_alloc)
{
    if (!stats) return;

    if (new_bytes > bytes)
        stats->incrementPending(new_bytes - bytes);
    else if (bytes > new_bytes)
        stats->decrementPending(bytes - new_bytes);

    if (new_alloc > alloc)
        stats->incrementPendingAllocated(new_alloc - alloc);
    else if (alloc > new_alloc)
        stats->decrementPendingAllocated(alloc - new_alloc);

    bytes = new_bytes;
    alloc = new_alloc;
}

void AsynchronousInsertQueue::StatsManager::AtomicStats::InsertIntent::commit()
{
    if (!stats) return;
    stats->updateMovingAverage(static_cast<double>(bytes));
    stats->updateLastInsertTime();
}


void AsynchronousInsertQueue::StatsManager::cleanupStaleNoLock()
{
    auto now    = std::chrono::steady_clock::now();
    auto cutoff = now - stale_threshold;

    for (auto it = stats_map.begin(); it != stats_map.end(); )
    {
        auto stat = it->second;

        // atomically load pending count and last‐insert time:
        size_t pending = stat->getBytesPending();
        auto   last    = stat->getLastInsertTime();

        if (pending == 0 && last < cutoff)
            it = stats_map.erase(it);
        else
            ++it;
    }
}


AsynchronousInsertQueue::InsertQuery::InsertQuery(
    const ASTPtr & query_,
    const std::optional<UUID> & user_id_,
    const std::vector<UUID> & current_roles_,
    const Settings & settings_,
    DataKind data_kind_,
    StatsManager & stats_manager)
    : query(query_->clone())
    , query_str(queryToString(query))
    , user_id(user_id_)
    , current_roles(current_roles_)
    , settings(settings_)
    , data_kind(data_kind_)
{
    SipHash siphash;

    siphash.update(data_kind);
    query->updateTreeHash(siphash, /*ignore_aliases=*/ true);

    if (user_id)
    {
        siphash.update(*user_id);
        for (const auto & current_role : current_roles)
            siphash.update(current_role);
    }

    auto changes = settings.changes();
    for (const auto & change : changes)
    {
        if (settings_to_skip.contains(change.name))
            continue;

        setting_changes.emplace_back(change.name, change.value);
        siphash.update(change.name);
        applyVisitor(FieldVisitorHash(siphash), change.value);
    }

    hash = siphash.get128();

    current_insert_stats = stats_manager.getInsertStatistics(hash);

}

AsynchronousInsertQueue::InsertQuery &
AsynchronousInsertQueue::InsertQuery::operator=(const InsertQuery & other)
{
    if (this != &other)
    {
        query = other.query->clone();
        query_str = other.query_str;
        user_id = other.user_id;
        current_roles = other.current_roles;
        settings = other.settings;
        data_kind = other.data_kind;
        hash = other.hash;
        setting_changes = other.setting_changes;
        current_insert_stats = other.current_insert_stats;
    }

    return *this;
}

bool AsynchronousInsertQueue::InsertQuery::operator==(const InsertQuery & other) const
{
    return toTupleCmp() == other.toTupleCmp();
}

AsynchronousInsertQueue::InsertData::Entry::Entry(
    DataChunk && chunk_,
    String && query_id_,
    const String & async_dedup_token_,
    const String & format_,
    MemoryTracker * user_memory_tracker_,
    StatsManager::AtomicStats::InsertIntent && insert_intent_)
    : chunk(std::move(chunk_))
    , query_id(std::move(query_id_))
    , async_dedup_token(async_dedup_token_)
    , format(format_)
    , user_memory_tracker(user_memory_tracker_)
    , create_time(std::chrono::system_clock::now())
    , insert_intent(std::move(insert_intent_))
{
}

void AsynchronousInsertQueue::InsertData::Entry::resetChunk()
{
    if (chunk.empty())
        return;

    // To avoid races on counter of user's MemoryTracker we should free memory at this moment.
    // Entries data must be destroyed in context of user who runs async insert.
    // Each entry in the list may correspond to a different user,
    // so we need to switch current thread's MemoryTracker.
    MemoryTrackerSwitcher switcher(user_memory_tracker);
    chunk = {};
}

void AsynchronousInsertQueue::InsertData::Entry::finish(std::exception_ptr exception_)
{
    if (finished.exchange(true))
        return;

    resetChunk();

    if (exception_)
    {
        promise.set_exception(exception_);
        ProfileEvents::increment(ProfileEvents::FailedAsyncInsertQuery, 1);
    }
    else
    {
        promise.set_value();
    }
}

// lock‑free peek of the two time points with a “snapshot” CAS loop
AsynchronousInsertQueue::StatsManager::AtomicStats::TimePoints
AsynchronousInsertQueue::StatsManager::AtomicStats::getRecentTimePoints() const
{
    using TimePoint = std::chrono::steady_clock::time_point;

    TimePoint tp1, tp2, check;
    // Loop until we read a consistent pair (tp2 didn’t change mid‑read)
    do
    {
        tp2   = time_point2.load(std::memory_order_relaxed);
        tp1   = time_point1.load(std::memory_order_relaxed);
        check = time_point2.load(std::memory_order_relaxed);
    }
    while (check != tp2);

    OptionalTimePoint o1, o2;

    // atomics can't be optional, so we use 'default' value (zero timepoint) as 'no value'
    if (tp1 != TimePoint{}) o1 = tp1;
    if (tp2 != TimePoint{}) o2 = tp2;
    return {o1, o2};
}

// CAS‑based update: install `now` into time_point2, publish old into time_point1
void AsynchronousInsertQueue::StatsManager::AtomicStats::updateWithCurrentTime()
{
    using TimePoint = std::chrono::steady_clock::time_point;
    TimePoint now = std::chrono::steady_clock::now();

    TimePoint prev2 = time_point2.load(std::memory_order_relaxed);
    // CAS loop to install `now` into time_point2
    while (!time_point2.compare_exchange_weak(
               prev2,
               now,
               std::memory_order_release,
               std::memory_order_relaxed))
    {
        // on failure, prev2 is updated to the current value of time_point2
        // retry until successful
    }
    // now prev2 holds the “old” second‑flush time
    time_point1.store(prev2, std::memory_order_release);
}

AsynchronousInsertQueue::AsynchronousInsertQueue(ContextPtr context_, size_t pool_size_, bool flush_on_shutdown_)
    : WithContext(context_)
    , pool_size(pool_size_)
    , flush_on_shutdown(flush_on_shutdown_)
    , queue_shards(pool_size)
    , pool(
          CurrentMetrics::AsynchronousInsertThreads,
          CurrentMetrics::AsynchronousInsertThreadsActive,
          CurrentMetrics::AsynchronousInsertThreadsScheduled,
          pool_size)
{
    if (!pool_size)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "pool_size cannot be zero");

    const auto & settings = getContext()->getSettingsRef();

    for (size_t i = 0; i < pool_size; ++i)
        queue_shards[i].busy_timeout_ms
            = std::min(Milliseconds(settings[Setting::async_insert_busy_timeout_min_ms]), Milliseconds(settings[Setting::async_insert_busy_timeout_max_ms]));

    for (size_t i = 0; i < pool_size; ++i)
        dump_by_first_update_threads.emplace_back([this, i] { processBatchDeadlines(i); });
}

void AsynchronousInsertQueue::flushAndShutdown()
{
    try
    {
        LOG_TRACE(log, "Shutting down the asynchronous insertion queue");
        shutdown = true;

        for (size_t i = 0; i < pool_size; ++i)
        {
            auto & shard = queue_shards[i];

            shard.are_tasks_available.notify_one();
            chassert(dump_by_first_update_threads[i].joinable());
            dump_by_first_update_threads[i].join();

            if (flush_on_shutdown)
            {
                for (auto & [_, elem] : shard.queue)
                    scheduleDataProcessingJob(elem.key, std::move(elem.data), getContext());
            }
            else
            {
                for (auto & [_, elem] : shard.queue)
                    for (const auto & entry : elem.data->entries)
                        entry->finish(
                            std::make_exception_ptr(Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Wait for async insert timeout exceeded)")));
            }
        }

        pool.wait();
        LOG_TRACE(log, "Asynchronous insertion queue finished");
    }
    catch (...)
    {
        tryLogCurrentException(log);
        pool.wait();
    }
}

AsynchronousInsertQueue::~AsynchronousInsertQueue()
{
    for (const auto & shard : queue_shards)
    {
        for (const auto & [first_update, elem] : shard.queue)
        {
            const auto & insert_query = elem.key.query->as<const ASTInsertQuery &>();
            LOG_WARNING(log, "Has unprocessed async insert for {}.{}",
                        backQuoteIfNeed(insert_query.getDatabase()), backQuoteIfNeed(insert_query.getTable()));
        }
    }
}

void AsynchronousInsertQueue::scheduleDataProcessingJob(
    const InsertQuery & key, InsertDataPtr data, ContextPtr global_context)
{
    /// Intuitively it seems reasonable to process first inserted blocks first.
    /// We add new chunks in the end of entries list, so they are automatically ordered by creation time
    chassert(!data->entries.empty());
    const auto priority = Priority{data->entries.front()->create_time.time_since_epoch().count()};

    /// Wrap 'unique_ptr' with 'shared_ptr' to make this
    /// lambda copyable and allow to save it to the thread pool.
    auto data_shared = std::make_shared<InsertDataPtr>(std::move(data));
    try
    {
        pool.scheduleOrThrowOnError(
            [ key, global_context, my_data = data_shared]() mutable
            { processData(key, std::move(*my_data), std::move(global_context)); },
            priority);
    }
    catch (...)
    {
        for (auto & entry : (**data_shared).entries)
            entry->finish(std::current_exception());
    }
}

AsynchronousInsertQueue::InsertQuery AsynchronousInsertQueue::preprocessInsertQuery(const ASTPtr & query, const ContextPtr & query_context, DataKind data_kind)
{
    auto & insert_query = query->as<ASTInsertQuery &>();
    insert_query.async_insert_flush = true;

    InterpreterInsertQuery interpreter(
        query,
        query_context,
        query_context->getSettingsRef()[Setting::insert_allow_materialized_columns],
        /* no_squash */ false,
        /* no_destination */ false,
        /* async_insert */ false);

    auto table = interpreter.getTable(insert_query);
    auto sample_block = InterpreterInsertQuery::getSampleBlock(insert_query, table, table->getInMemoryMetadataPtr(), query_context);

    if (!FormatFactory::instance().isInputFormat(insert_query.format))
    {
        if (insert_query.format.empty() && insert_query.infile)
        {
            const auto & in_file_node = insert_query.infile->as<ASTLiteral &>();
            const auto in_file = in_file_node.value.safeGet<std::string>();
            const auto in_file_format = FormatFactory::instance().getFormatFromFileName(in_file);
            if (!FormatFactory::instance().isInputFormat(in_file_format))
                throw Exception(ErrorCodes::UNKNOWN_FORMAT, "Unknown input INFILE format {}", in_file_format);
        }
        else
            throw Exception(ErrorCodes::UNKNOWN_FORMAT, "Unknown input format {}", insert_query.format);
    }

    /// For table functions we check access while executing
    /// InterpreterInsertQuery::getTable() -> ITableFunction::execute().
    if (insert_query.table_id)
        query_context->checkAccess(AccessType::INSERT, insert_query.table_id, sample_block.getNames());

    insert_query.columns = std::make_shared<ASTExpressionList>();
    for (const auto & column : sample_block)
        insert_query.columns->children.push_back(std::make_shared<ASTIdentifier>(column.name));

    validateSettings(query_context->getSettingsRef(), log);

    return InsertQuery{query,
        query_context->getUserID(),
        query_context->getCurrentRoles(),
        query_context->getSettingsRef(),
        data_kind,
        stats};
}

AsynchronousInsertQueue::PushResult
AsynchronousInsertQueue::pushQueryWithInlinedData(ASTPtr query, ContextPtr query_context)
{
    query = query->clone();
    auto key = preprocessInsertQuery(query, query_context, DataKind::Parsed);

    size_t bytes_pending = key.current_insert_stats->getBytesPending();

    auto preallocate_buffer_size = query_context->getSettingsRef()[Setting::async_insert_preallocate_buffer_size];

    if (preallocate_buffer_size == -1)
        // adding 5%, because EMA prediction is 'smoothed', and we can have short spike over it.
        // also some small overestimation can still be cheaper than the realloc with doubling the size.
        preallocate_buffer_size = static_cast<size_t>(key.current_insert_stats->getAverageInsertSize() * 1.05);

    if (bytes_pending >= query_context->getSettingsRef()[Setting::async_insert_bytes_to_throw_insert] )
    {
        ProfileEvents::increment(ProfileEvents::RejectedAsyncInserts);

        throw Exception(
            ErrorCodes::LIMIT_EXCEEDED,
            "Too many bytes pending ({}) for that insert type. Query with id {} will be rejected. Flushes are processing significantly slower than inserts",
            formatReadableSizeWithBinarySuffix(bytes_pending),
            query_context->getCurrentQueryId()
        );
    }

    auto insert_intent = key.current_insert_stats->createInsertIntent(preallocate_buffer_size,preallocate_buffer_size);
    String bytes;
    bytes.reserve(preallocate_buffer_size);

    {
        /// Read at most 'async_insert_max_data_size' bytes of data.
        /// If limit is exceeded we will fallback to synchronous insert
        /// to avoid buffering of huge amount of data in memory.

        auto read_buf = getReadBufferFromASTInsertQuery(query);

        LimitReadBuffer limit_buf(
            *read_buf,
            query_context->getSettingsRef()[Setting::async_insert_max_data_size],
            /*throw_exception=*/false,
            /*exact_limit=*/{});

        WriteBufferFromString write_buf(bytes);

        // actually we can go over the async_insert_bytes_to_throw_insert limit if a single insert will be huge, bigger than prediced.
        if (bytes_pending >= query_context->getSettingsRef()[Setting::async_insert_bytes_to_delay_insert] )
        {
            ProfileEvents::increment(ProfileEvents::DelayedAsyncInserts);
            size_t speed = query_context->getSettingsRef()[Setting::async_insert_rate_limit_bytes_per_second];
            auto throttler = std::make_shared<Throttler>(speed);
            copyDataWithThrottler(limit_buf, write_buf, []() {}, throttler);
        }
        else
        {
            copyData(limit_buf, write_buf);
        }

        if (!read_buf->eof())
        {
            write_buf.finalize();

            /// Concat read buffer with already extracted from insert
            /// query data and with the rest data from insert query.
            std::vector<std::unique_ptr<ReadBuffer>> buffers;
            buffers.emplace_back(std::make_unique<ReadBufferFromOwnString>(bytes));
            buffers.emplace_back(std::move(read_buf));

            return PushResult
            {
                .status = PushResult::TOO_MUCH_DATA,
                .future = {},
                .insert_data_buffer = std::make_unique<ConcatReadBuffer>(std::move(buffers)),
            };
        }
    }

    insert_intent.adjust(bytes.size(), bytes.capacity());
    return pushDataChunk(std::move(query), std::move(bytes), std::move(query_context), std::move(key), std::move(insert_intent));
}


// TODO: add earlier checks & throttler to TCPHandler.cpp in the place where pushQueryWithBlock is called
AsynchronousInsertQueue::PushResult
AsynchronousInsertQueue::pushQueryWithBlock(ASTPtr query, Block block, ContextPtr query_context)
{
    query = query->clone();

    auto key = preprocessInsertQuery(query, query_context, DataKind::Preprocessed);

    size_t bytes_pending = key.current_insert_stats->getBytesPending();

    if (bytes_pending >= query_context->getSettingsRef()[Setting::async_insert_bytes_to_throw_insert] )
    {
        ProfileEvents::increment(ProfileEvents::RejectedAsyncInserts);

        throw Exception(
            ErrorCodes::LIMIT_EXCEEDED,
            "Too many bytes pending ({}) for that insert type. Query with id {} will be rejected. Flushes are processing significantly slower than inserts",
            formatReadableSizeWithBinarySuffix(bytes_pending),
            query_context->getCurrentQueryId()
        );
    }

    auto insert_intent = key.current_insert_stats->createInsertIntent(block.bytes(), block.allocatedBytes());

    return pushDataChunk(std::move(query), std::move(block), std::move(query_context), std::move(key), std::move(insert_intent));
}

AsynchronousInsertQueue::PushResult
AsynchronousInsertQueue::pushDataChunk(ASTPtr query, DataChunk chunk, ContextPtr query_context, InsertQuery key, StatsManager::AtomicStats::InsertIntent insert_intent)
{
    const auto & settings = query_context->getSettingsRef();
    auto & insert_query = query->as<ASTInsertQuery &>();

    auto data_kind = chunk.getDataKind();

    const auto flush_time_points = key.current_insert_stats->getRecentTimePoints();

    auto entry = std::make_shared<InsertData::Entry>(
        std::move(chunk),
        query_context->getCurrentQueryId(),
        settings[Setting::insert_deduplication_token],
        insert_query.format,
        CurrentThread::getUserMemoryTracker(),
        std::move(insert_intent));

    /// If data is parsed on client we don't care of format which is written
    /// in INSERT query. Replace it to put all such queries into one bucket in queue.
    if (data_kind == DataKind::Preprocessed)
        insert_query.format = "Native";

    /// Query parameters make sense only for format Values.
    if (insert_query.format == "Values")
        entry->query_parameters = query_context->getQueryParameters();

    InsertDataPtr data_to_process;
    std::future<void> insert_future;

    auto shard_num = key.hash % pool_size;
    auto & shard = queue_shards[shard_num];
    {
        std::lock_guard lock(shard.mutex);

        auto [it, inserted] = shard.iterators.try_emplace(key.hash);
        auto now = std::chrono::steady_clock::now();
        auto timeout_ms = getBusyWaitTimeoutMs(settings, shard, flush_time_points, now);
        if (timeout_ms != shard.busy_timeout_ms)
        {
            LOG_TRACE(
                log,
                "Asynchronous timeout {} from {} to {} for queue shard {}.",
                timeout_ms < shard.busy_timeout_ms ? "decreased" : "increased",
                shard.busy_timeout_ms.count(),
                timeout_ms.count(),
                size_t(shard_num));
        }

        if (inserted)
            it->second = shard.queue.emplace(now + timeout_ms, Container{key, std::make_unique<InsertData>(timeout_ms)}).first;

        auto queue_it = it->second;
        auto & data = queue_it->second.data;
        size_t entry_data_size = entry->chunk.byteSize();

        assert(data);
        auto size_in_bytes = data->size_in_bytes;
        data->size_in_bytes += entry_data_size;
        /// We rely on the fact that entries are being added to the list in order of creation time in `scheduleDataProcessingJob()`
        data->entries.emplace_back(entry);
        insert_future = entry->getFuture();

        LOG_TRACE(log, "Have {} pending inserts with total {} bytes of data for query '{}'",
            data->entries.size(), data->size_in_bytes, key.query_str);

        bool has_enough_bytes = data->size_in_bytes >= key.settings[Setting::async_insert_max_data_size];
        bool has_enough_queries
            = data->entries.size() >= key.settings[Setting::async_insert_max_query_number] && key.settings[Setting::async_insert_deduplicate];

        auto max_busy_timeout_exceeded = [&shard, &settings, &now, &flush_time_points]() -> bool
        {
            if (!settings[Setting::async_insert_use_adaptive_busy_timeout] || !shard.last_insert_time || !flush_time_points.first)
                return false;

            auto max_ms = Milliseconds(settings[Setting::async_insert_busy_timeout_max_ms]);
            return *shard.last_insert_time + max_ms < now && *flush_time_points.first + max_ms < *flush_time_points.second;
        };

        /// Here we check whether we have hit the limit on the maximum data size in the buffer or
        /// if the elapsed time since the last insert exceeds the maximum busy wait timeout.
        /// We also use the limit settings from the query context.
        /// This works because queries with the same set of settings are already grouped together.
        if (!flush_stopped && (has_enough_bytes || has_enough_queries || max_busy_timeout_exceeded()))
        {
            data->timeout_ms = Milliseconds::zero();
            data_to_process = std::move(data);
            shard.iterators.erase(it);
            shard.queue.erase(queue_it);
        }

        shard.last_insert_time = now;
        shard.busy_timeout_ms = timeout_ms;

        ProfileEvents::increment(ProfileEvents::AsyncInsertQuery);
        ProfileEvents::increment(ProfileEvents::AsyncInsertBytes, entry_data_size);

        if (data_to_process)
        {
            if (!inserted)
                CurrentMetrics::sub(CurrentMetrics::AsynchronousInsertQueueSize);
            CurrentMetrics::sub(CurrentMetrics::AsynchronousInsertQueueBytes, size_in_bytes);
        }
        else
        {
            if (inserted)
                CurrentMetrics::add(CurrentMetrics::AsynchronousInsertQueueSize);
            CurrentMetrics::add(CurrentMetrics::AsynchronousInsertQueueBytes, entry_data_size);
        }
    }

    if (data_to_process)
        scheduleDataProcessingJob(key, std::move(data_to_process), getContext());
    else
        shard.are_tasks_available.notify_one();

    return PushResult
    {
        .status = PushResult::OK,
        .future = std::move(insert_future),
        .insert_data_buffer = nullptr,
    };
}

AsynchronousInsertQueue::Milliseconds AsynchronousInsertQueue::getBusyWaitTimeoutMs(
    const Settings & settings,
    const QueueShard & shard,
    const StatsManager::AtomicStats::TimePoints & flush_time_points,
    std::chrono::steady_clock::time_point now) const
{
    if (!settings[Setting::async_insert_use_adaptive_busy_timeout])
        return settings[Setting::async_insert_busy_timeout_max_ms];

    const auto max_ms = Milliseconds(settings[Setting::async_insert_busy_timeout_max_ms]);
    const auto min_ms = std::min(std::max(Milliseconds(settings[Setting::async_insert_busy_timeout_min_ms]), Milliseconds(1)), max_ms);

    auto normalize = [&min_ms, &max_ms](const auto & t_ms) { return std::min(std::max(t_ms, min_ms), max_ms); };

    if (!shard.last_insert_time || !flush_time_points.first)
        return normalize(shard.busy_timeout_ms);

    const auto & last_insert_time = *shard.last_insert_time;
    const auto & [t1, t2] = std::tie(*flush_time_points.first, *flush_time_points.second);
    const double increase_rate = settings[Setting::async_insert_busy_timeout_increase_rate];
    const double decrease_rate = settings[Setting::async_insert_busy_timeout_decrease_rate];

    const auto decreased_timeout_ms = std::min(
        std::chrono::duration_cast<Milliseconds>(shard.busy_timeout_ms / (1.0 + decrease_rate)), shard.busy_timeout_ms - Milliseconds(1));

    /// Increase the timeout for frequent inserts.
    if (last_insert_time + min_ms > now)
    {
        auto timeout_ms = std::max(
            std::chrono::duration_cast<Milliseconds>(shard.busy_timeout_ms * (1.0 + increase_rate)),
            shard.busy_timeout_ms + Milliseconds(1));

        return normalize(timeout_ms);
    }
    /// Decrease the timeout if inserts are not frequent,
    /// that is, if the time since the last insert and the difference between the last two queue flushes were both
    /// long enough (exceeding the adjusted timeout).
    /// This ensures the timeout value converges to the minimum over time for non-frequent inserts.
    if (last_insert_time + decreased_timeout_ms < now && t1 + decreased_timeout_ms < t2)
        return normalize(decreased_timeout_ms);

    return normalize(shard.busy_timeout_ms);
}

void AsynchronousInsertQueue::validateSettings(const Settings & settings, LoggerPtr log)
{
    const auto max_ms = std::chrono::milliseconds(settings[Setting::async_insert_busy_timeout_max_ms]);

    if (max_ms == std::chrono::milliseconds::zero())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Setting 'async_insert_busy_timeout_max_ms' can't be zero");

    if (!settings[Setting::async_insert_use_adaptive_busy_timeout])
        return;

    /// Adaptive timeout settings.
    const auto min_ms = std::chrono::milliseconds(settings[Setting::async_insert_busy_timeout_min_ms]);

    if (min_ms > max_ms && log)
        LOG_WARNING(
            log,
            "Setting 'async_insert_busy_timeout_min_ms'={} is greater than 'async_insert_busy_timeout_max_ms'={}. Ignoring "
            "'async_insert_busy_timeout_min_ms'",
            min_ms.count(),
            max_ms.count());

    if (settings[Setting::async_insert_busy_timeout_increase_rate] <= 0)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Setting 'async_insert_busy_timeout_increase_rate' must be greater than zero");

    if (settings[Setting::async_insert_busy_timeout_decrease_rate] <= 0)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Setting 'async_insert_busy_timeout_decrease_rate' must be greater than zero");

    if (settings[Setting::async_insert_preallocate_buffer_size] < -1)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Setting 'async_insert_preallocate_buffer_size' should be 0, positive value, or -1 (auto, based on moving average of the inserts of the same type)");

}

void AsynchronousInsertQueue::flushAll()
{
    std::lock_guard flush_lock(flush_mutex);

    LOG_DEBUG(log, "Requested to flush asynchronous insert queue");

    /// Disable background flushes to avoid adding new elements to the queue.
    flush_stopped = true;
    std::vector<Queue> queues_to_flush(pool_size);

    for (size_t i = 0; i < pool_size; ++i)
    {
        std::lock_guard lock(queue_shards[i].mutex);
        queues_to_flush[i] = std::move(queue_shards[i].queue);
        queue_shards[i].iterators.clear();
    }

    size_t total_queries = 0;
    size_t total_bytes = 0;
    size_t total_entries = 0;

    for (size_t i = 0; i < pool_size; ++i)
    {
        auto & queue = queues_to_flush[i];
        total_queries += queue.size();
        for (auto & [_, entry] : queue)
        {
            total_bytes += entry.data->size_in_bytes;
            total_entries += entry.data->entries.size();
            scheduleDataProcessingJob(entry.key, std::move(entry.data), getContext());
        }
    }

    /// Note that jobs scheduled before the call of 'flushAll' are not counted here.
    LOG_DEBUG(log,
        "Will wait for finishing of {} flushing jobs (about {} inserts, {} bytes, {} distinct queries)",
        pool.active(), total_entries, total_bytes, total_queries);

    /// Wait until all jobs are finished. That includes also jobs
    /// that were scheduled before the call of 'flushAll'.
    pool.wait();

    LOG_DEBUG(log, "Finished flushing of asynchronous insert queue");
    flush_stopped = false;
}

void AsynchronousInsertQueue::processBatchDeadlines(size_t shard_num)
{
    auto & shard = queue_shards[shard_num];

    while (!shutdown)
    {
        std::vector<Container> entries_to_flush;
        {
            std::unique_lock lock(shard.mutex);

            const auto rel_time
                = std::min(shard.busy_timeout_ms, Milliseconds(getContext()->getSettingsRef()[Setting::async_insert_poll_timeout_ms]));
            shard.are_tasks_available.wait_for(
                lock,
                rel_time,
                [&shard, this]
                {
                    if (shutdown)
                        return true;

                    if (!shard.queue.empty() && shard.queue.begin()->first < std::chrono::steady_clock::now())
                        return true;

                    return false;
                });

            if (shutdown)
                return;

            if (flush_stopped)
                continue;

            const auto now = std::chrono::steady_clock::now();

            size_t size_in_bytes = 0;
            while (true)
            {
                if (shard.queue.empty() || shard.queue.begin()->first > now)
                    break;

                auto it = shard.queue.begin();
                size_in_bytes += it->second.data->size_in_bytes;

                shard.iterators.erase(it->second.key.hash);

                entries_to_flush.emplace_back(std::move(it->second));
                shard.queue.erase(it);
            }

            if (!entries_to_flush.empty())
            {
                CurrentMetrics::sub(CurrentMetrics::AsynchronousInsertQueueSize, entries_to_flush.size());
                CurrentMetrics::sub(CurrentMetrics::AsynchronousInsertQueueBytes, size_in_bytes);
            }
        }

        for (auto & entry : entries_to_flush)
            scheduleDataProcessingJob(entry.key, std::move(entry.data), getContext());
    }
}

namespace
{

using TimePoint = std::chrono::time_point<std::chrono::system_clock>;

void appendElementsToLogSafe(
    AsynchronousInsertLog & log, std::vector<AsynchronousInsertLogElement> elements, TimePoint flush_time, const String & flush_exception)
try
{
    using Status = AsynchronousInsertLogElement::Status;

    for (auto & elem : elements)
    {
        elem.flush_time = timeInSeconds(flush_time);
        elem.flush_time_microseconds = timeInMicroseconds(flush_time);
        elem.exception = flush_exception;
        elem.status = flush_exception.empty() ? Status::Ok : Status::FlushError;
        log.add(std::move(elem));
    }
}
catch (...)
{
    tryLogCurrentException("AsynchronousInsertQueue", "Failed to add elements to AsynchronousInsertLog");
}

void convertBlockToHeader(Block & block, const Block & header)
{
    auto converting_dag = ActionsDAG::makeConvertingActions(
        block.getColumnsWithTypeAndName(),
        header.getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name);

    auto converting_actions = std::make_shared<ExpressionActions>(std::move(converting_dag));
    converting_actions->execute(block);
}

String serializeQuery(const IAST & query, size_t max_length)
{
    return query.hasSecretParts()
        ? query.formatForLogging(max_length)
        : wipeSensitiveDataAndCutToLength(serializeAST(query), max_length);
}

}

void AsynchronousInsertQueue::processData(
    InsertQuery key, InsertDataPtr data, ContextPtr global_context)
try
{
    if (!data)
        return;

    setThreadName("AsyncInsertQ");

    const auto log = getLogger("AsynchronousInsertQueue");
    const auto & insert_query = assert_cast<const ASTInsertQuery &>(*key.query);

    auto insert_context = Context::createCopy(global_context);
    bool internal = false; // To enable logging this query
    bool async_insert = true;

    /// Disabled query spans. Could be activated by initializing this to a SpanHolder
    std::shared_ptr<OpenTelemetry::SpanHolder> query_span{nullptr};

    /// 'resetParser' doesn't work for parallel parsing.
    key.settings.set("input_format_parallel_parsing", false);
    /// It maybe insert into distributed table.
    /// It doesn't make sense to make insert into destination tables asynchronous.
    key.settings.set("async_insert", false);

    insert_context->makeQueryContext();

    /// Access rights must be checked for the user who executed the initial INSERT query.
    if (key.user_id)
    {
        insert_context->setUser(*key.user_id);
        insert_context->setCurrentRoles(key.current_roles);
    }

    insert_context->setSettings(key.settings);

    /// Set initial_query_id, because it's used in InterpreterInsertQuery for table lock.
    insert_context->setCurrentQueryId("");

    auto insert_query_id = insert_context->getCurrentQueryId();
    auto query_start_time = std::chrono::system_clock::now();

    Stopwatch start_watch{CLOCK_MONOTONIC};
    insert_context->setQueryKind(ClientInfo::QueryKind::INITIAL_QUERY);
    insert_context->setInitialQueryStartTime(query_start_time);
    insert_context->setCurrentQueryId(insert_query_id);
    insert_context->setInitialQueryId(insert_query_id);

    DB::CurrentThread::QueryScope query_scope_holder(insert_context);

    auto query_for_logging = serializeQuery(*key.query, insert_context->getSettingsRef()[Setting::log_queries_cut_to_length]);

    /// We add it to the process list so
    /// a) it appears in system.processes
    /// b) can be cancelled if we want to
    /// c) has an associated process list element where runtime metrics are stored
    auto process_list_entry = insert_context->getProcessList().insert(
        query_for_logging,
        key.query.get(),
        insert_context,
        start_watch.getStart());

    auto query_status = process_list_entry->getQueryStatus();
    insert_context->setProcessListElement(std::move(query_status));

    String query_database;
    String query_table;

    if (insert_query.table_id)
    {
        query_database = insert_query.table_id.getDatabaseName();
        query_table = insert_query.table_id.getTableName();
        insert_context->setInsertionTable(insert_query.table_id);
    }

    std::unique_ptr<DB::IInterpreter> interpreter;
    QueryPipeline pipeline;
    QueryLogElement query_log_elem;

    auto async_insert_log = global_context->getAsynchronousInsertLog();
    std::vector<AsynchronousInsertLogElement> log_elements;
    if (async_insert_log)
        log_elements.reserve(data->entries.size());

    auto add_entry_to_asynchronous_insert_log = [&, query_by_format = NameToNameMap{}](
        const InsertData::EntryPtr & entry,
        const String & parsing_exception,
        size_t num_rows,
        size_t num_bytes) mutable
    {
        if (!async_insert_log)
            return;

        AsynchronousInsertLogElement elem;
        elem.event_time = timeInSeconds(entry->create_time);
        elem.event_time_microseconds = timeInMicroseconds(entry->create_time);
        elem.database = query_database;
        elem.table = query_table;
        elem.format = entry->format;
        elem.query_id = entry->query_id;
        elem.bytes = num_bytes;
        elem.rows = num_rows;
        elem.exception = parsing_exception;
        elem.data_kind = entry->chunk.getDataKind();
        elem.timeout_milliseconds = data->timeout_ms.count();
        elem.flush_query_id = insert_query_id;

        auto get_query_by_format = [&](const String & format) -> const String &
        {
            auto [it, inserted] = query_by_format.try_emplace(format);
            if (!inserted)
                return it->second;

            auto query = key.query->clone();
            assert_cast<ASTInsertQuery &>(*query).format = format;
            it->second = serializeQuery(*query, insert_context->getSettingsRef()[Setting::log_queries_cut_to_length]);
            return it->second;
        };

        if (entry->chunk.getDataKind() == DataKind::Parsed)
            elem.query_for_logging = key.query_str;
        else
            elem.query_for_logging = get_query_by_format(entry->format);

        /// If there was a parsing error,
        /// the entry won't be flushed anyway,
        /// so add the log element immediately.
        if (!elem.exception.empty())
        {
            elem.status = AsynchronousInsertLogElement::ParsingError;
            async_insert_log->add(std::move(elem));
        }
        else
        {
            elem.status = AsynchronousInsertLogElement::Ok;
            log_elements.push_back(std::move(elem));
        }
    };

    try
    {
        interpreter = std::make_unique<InterpreterInsertQuery>(
            key.query, insert_context, key.settings[Setting::insert_allow_materialized_columns], false, false, true);

        pipeline = interpreter->execute().pipeline;
        chassert(pipeline.pushing());

        query_log_elem = logQueryStart(
            query_start_time,
            insert_context,
            query_for_logging,
            key.query,
            pipeline,
            interpreter,
            internal,
            query_database,
            query_table,
            async_insert);
    }
    catch (...)
    {
        logExceptionBeforeStart(query_for_logging, insert_context, key.query, query_span, start_watch.elapsedMilliseconds());

        if (async_insert_log)
        {
            for (const auto & entry : data->entries)
                add_entry_to_asynchronous_insert_log(entry, /*parsing_exception=*/ "", /*num_rows=*/ 0, entry->chunk.byteSize());

            auto exception = getCurrentExceptionMessage(false);
            auto flush_time = std::chrono::system_clock::now();
            appendElementsToLogSafe(*async_insert_log, std::move(log_elements), flush_time, exception);
        }
        throw;
    }

    auto finish_entries = [&](size_t num_rows, size_t num_bytes)
    {
        for (const auto & entry : data->entries)
        {
            if (!entry->isFinished())
                entry->finish();
        }

        if (!log_elements.empty())
        {
            auto flush_time = std::chrono::system_clock::now();
            appendElementsToLogSafe(*async_insert_log, std::move(log_elements), flush_time, "");
        }

        LOG_DEBUG(log, "Flushed {} rows, {} bytes for query '{}'", num_rows, num_bytes, key.query_str);
        key.current_insert_stats->updateWithCurrentTime();

        bool pulling_pipeline = false;
        logQueryFinish(
            query_log_elem, insert_context, key.query, pipeline, pulling_pipeline, query_span, QueryCache::Usage::None, internal);
    };

    try
    {
        Chunk chunk;
        auto header = pipeline.getHeader();

        if (key.data_kind == DataKind::Parsed)
            chunk = processEntriesWithParsing(key, data, header, insert_context, log, add_entry_to_asynchronous_insert_log);
        else
            chunk = processPreprocessedEntries(data, header, add_entry_to_asynchronous_insert_log);

        ProfileEvents::increment(ProfileEvents::AsyncInsertRows, chunk.getNumRows());

        if (chunk.getNumRows() == 0)
        {
            finish_entries(/*num_rows=*/ 0, /*num_bytes=*/ 0);
            return;
        }

        size_t num_rows = chunk.getNumRows();
        size_t num_bytes = chunk.bytes();

        auto source = std::make_shared<SourceFromSingleChunk>(header, std::move(chunk));
        pipeline.complete(Pipe(std::move(source)));

        CompletedPipelineExecutor completed_executor(pipeline);
        completed_executor.execute();

        finish_entries(num_rows, num_bytes);
    }
    catch (...)
    {
        bool log_error = true;
        logQueryException(query_log_elem, insert_context, start_watch, key.query, query_span, internal, log_error);
        if (!log_elements.empty())
        {
            auto exception = getCurrentExceptionMessage(false);
            auto flush_time = std::chrono::system_clock::now();
            appendElementsToLogSafe(*async_insert_log, std::move(log_elements), flush_time, exception);
        }
        throw;
    }
}
catch (const Exception & e)
{
    finishWithException(key.query, data->entries, e);
}
catch (const Poco::Exception & e)
{
    finishWithException(key.query, data->entries, e);
}
catch (const std::exception & e)
{
    finishWithException(key.query, data->entries, e);
}
catch (...)
{
    finishWithException(key.query, data->entries, Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Unknown exception"));
}

template <typename LogFunc>
Chunk AsynchronousInsertQueue::processEntriesWithParsing(
    const InsertQuery & key,
    const InsertDataPtr & data,
    const Block & header,
    const ContextPtr & insert_context,
    LoggerPtr logger,
    LogFunc && add_to_async_insert_log)
{
    size_t total_rows = 0;
    InsertData::EntryPtr current_entry;
    String current_exception;

    const auto & insert_query = assert_cast<const ASTInsertQuery &>(*key.query);
    auto format = getInputFormatFromASTInsertQuery(key.query, false, header, insert_context, nullptr);
    std::shared_ptr<ISimpleTransform> adding_defaults_transform;

    if (insert_context->getSettingsRef()[Setting::input_format_defaults_for_omitted_fields] && insert_query.table_id)
    {
        StoragePtr storage = DatabaseCatalog::instance().getTable(insert_query.table_id, insert_context);
        auto metadata_snapshot = storage->getInMemoryMetadataPtr();
        const auto & columns = metadata_snapshot->getColumns();
        if (columns.hasDefaults())
            adding_defaults_transform = std::make_shared<AddingDefaultsTransform>(header, columns, *format, insert_context);
    }

    auto on_error = [&](const MutableColumns & result_columns, Exception & e)
    {
        current_exception = e.displayText();
        LOG_ERROR(logger, "Failed parsing for query '{}' with query id {}. {}",
            key.query_str, current_entry->query_id, current_exception);

        for (const auto & column : result_columns)
            if (column->size() > total_rows)
                column->popBack(column->size() - total_rows);

        current_entry->finish(std::current_exception());
        return 0;
    };

    StreamingFormatExecutor executor(header, format, std::move(on_error), std::move(adding_defaults_transform));
    auto chunk_info = std::make_shared<AsyncInsertInfo>();

    for (const auto & entry : data->entries)
    {
        current_entry = entry;

        const auto * bytes = entry->chunk.asString();
        if (!bytes)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Expected entry with data kind Parsed. Got: {}", entry->chunk.getDataKind());

        auto buffer = std::make_unique<ReadBufferFromString>(*bytes);
        executor.setQueryParameters(entry->query_parameters);

        size_t num_bytes = bytes->size();
        size_t num_rows = executor.execute(*buffer);

        total_rows += num_rows;

        /// For some reason, client can pass zero rows and bytes to server.
        /// We don't update offsets in this case, because we assume every insert has some rows during dedup
        /// but we have nothing to deduplicate for this insert.
        if (num_rows > 0)
        {
            chunk_info->offsets.push_back(total_rows);
            chunk_info->tokens.push_back(entry->async_dedup_token);
        }

        add_to_async_insert_log(entry, current_exception, num_rows, num_bytes);
        current_exception.clear();
        entry->resetChunk();
    }

    Chunk chunk(executor.getResultColumns(), total_rows);
    chunk.getChunkInfos().add(std::move(chunk_info));
    return chunk;
}

template <typename LogFunc>
Chunk AsynchronousInsertQueue::processPreprocessedEntries(
    const InsertDataPtr & data,
    const Block & header,
    LogFunc && add_to_async_insert_log)
{
    size_t total_rows = 0;
    auto chunk_info = std::make_shared<AsyncInsertInfo>();
    auto result_columns = header.cloneEmptyColumns();

    for (const auto & entry : data->entries)
    {
        const auto * block = entry->chunk.asBlock();
        if (!block)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Expected entry with data kind Preprocessed. Got: {}", entry->chunk.getDataKind());

        Block block_to_insert = *block;
        if (!isCompatibleHeader(block_to_insert, header))
            convertBlockToHeader(block_to_insert, header);

        auto columns = block_to_insert.getColumns();
        for (size_t i = 0, s = columns.size(); i < s; ++i)
            result_columns[i]->insertRangeFrom(*columns[i], 0, columns[i]->size());

        total_rows += block_to_insert.rows();

        /// For some reason, client can pass zero rows and bytes to server.
        /// We don't update offsets in this case, because we assume every insert has some rows during dedup,
        /// but we have nothing to deduplicate for this insert.
        if (block_to_insert.rows() > 0)
        {
            chunk_info->offsets.push_back(total_rows);
            chunk_info->tokens.push_back(entry->async_dedup_token);
        }

        add_to_async_insert_log(entry, /*parsing_exception=*/ "", block_to_insert.rows(), block_to_insert.bytes());
        entry->resetChunk();
    }

    Chunk chunk(std::move(result_columns), total_rows);
    chunk.getChunkInfos().add(std::move(chunk_info));
    return chunk;
}

template <typename E>
void AsynchronousInsertQueue::finishWithException(
    const ASTPtr & query, const std::list<InsertData::EntryPtr> & entries, const E & exception)
{
    tryLogCurrentException("AsynchronousInsertQueue", fmt::format("Failed insertion for query '{}'", queryToString(query)));

    for (const auto & entry : entries)
    {
        if (!entry->isFinished())
        {
            /// Make a copy of exception to avoid concurrent usage of
            /// one exception object from several threads.
            entry->finish(std::make_exception_ptr(exception));
        }
    }
}

}
