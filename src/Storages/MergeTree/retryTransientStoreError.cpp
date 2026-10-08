#include <Storages/MergeTree/retryTransientStoreError.h>

#include <Interpreters/TransactionManager.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <base/sleep.h>

#include <algorithm>

namespace DB
{

namespace
{

constexpr UInt64 RETRY_TIMEOUT_SECONDS = 60;
constexpr UInt64 RETRY_BACKOFF_MS = 100;
constexpr UInt64 RETRY_MAX_BACKOFF_MS = 2000;

}

void retryTransientStoreError(LoggerPtr log, std::string_view what, const std::function<void()> & store)
{
    Stopwatch watch;
    UInt64 backoff_ms = RETRY_BACKOFF_MS;
    size_t attempts = 0;
    while (true)
    {
        ++attempts;
        try
        {
            store();
            if (attempts > 1)
                LOG_INFO(log, "Stored transaction metadata for {} after {} attempts", what, attempts);
            return;
        }
        catch (...)
        {
            if (!isRetryableException(std::current_exception()))
                throw;

            const bool give_up = watch.elapsedSeconds() >= RETRY_TIMEOUT_SECONDS
                || TransactionManager::instance().isShuttingDown();
            if (give_up)
            {
                LOG_ERROR(log, "Cannot store transaction metadata for {} after {} attempts in {:.1f} s, giving up: {}",
                    what, attempts, watch.elapsedSeconds(), getCurrentExceptionMessage(false));
                throw;
            }

            if (attempts == 1)
                LOG_WARNING(log, "Cannot store transaction metadata for {}, will retry: {}", what, getCurrentExceptionMessage(false));
            else
                LOG_DEBUG(log, "Cannot store transaction metadata for {}, attempt {}: {}", what, attempts, getCurrentExceptionMessage(false));
        }

        sleepForMilliseconds(backoff_ms);
        backoff_ms = std::min(backoff_ms * 2, RETRY_MAX_BACKOFF_MS);
    }
}

}
