#include <Common/AllocationInterceptors.h>
#include <Common/Allocator.h>
#include <Common/BitHelpers.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <Common/VersionNumber.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>

#include <base/errnoToString.h>
#include <base/getPageSize.h>

#include <Poco/Environment.h>
#include <Poco/Logger.h>
#include <sys/mman.h> /// MADV_POPULATE_WRITE
#include <base/mremap.h>
#include <base/getMemoryAmount.h>
#include <mutex>
#include <unordered_map>
#include <cstdlib>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int LOGICAL_ERROR;
}

}

namespace
{

using namespace DB;

#if defined(MADV_POPULATE_WRITE)
/// Address passed to madvise is required to be aligned to the page boundary.
auto adjustToPageSize(void * buf, size_t len, size_t page_size)
{
    const uintptr_t address_numeric = reinterpret_cast<uintptr_t>(buf);
    const size_t next_page_start = ((address_numeric + page_size - 1) / page_size) * page_size;
    return std::make_pair(reinterpret_cast<void *>(next_page_start), len - (next_page_start - address_numeric));
}

bool madviseSupportsMadvPopulateWrite()
{
    /// Can't rely for detection on madvise(MADV_POPULATE_WRITE) == EINVAL, since this will be returned in many other cases.
    VersionNumber linux_version(Poco::Environment::osVersion());
    VersionNumber supported_version(5, 14, 0);
    bool is_supported = linux_version >= supported_version;
    if (!is_supported)
        LOG_TRACE(getLogger("Allocator"), "Disabled page pre-faulting (kernel is too old).");
    return is_supported;
}

const bool is_supported_by_kernel = madviseSupportsMadvPopulateWrite();

#endif

void prefaultPages([[maybe_unused]] void * buf_, [[maybe_unused]] size_t len_)
{
#if defined(MADV_POPULATE_WRITE)
    if (len_ < POPULATE_THRESHOLD)
        return;

    if (!is_supported_by_kernel) [[unlikely]]
        return;

    auto [buf, len] = adjustToPageSize(buf_, len_, ::getPageSize());
    ::madvise(buf, len, MADV_POPULATE_WRITE);
#endif
}


/** Experimental handling of large buffers (SUP-31052).
  * Mode is selected by env CLICKHOUSE_LARGE_ALLOC:
  *   off     - current upstream behaviour (jemalloc for everything);
  *   mremap  - like 22.3: buffers >= threshold are mmap'ed, realloc is mremap(MREMAP_MAYMOVE);
  *   reserve - buffers >= threshold get a big MAP_NORESERVE virtual reservation and grow in place
  *             (pointer-stable, no copy, no mremap); on exhaustion a new reservation + memcpy.
  * CLICKHOUSE_LARGE_ALLOC_THRESHOLD (bytes, default 64 MiB), CLICKHOUSE_LARGE_ALLOC_RESERVE_FACTOR (default 16).
  */
namespace LargeAlloc
{
    enum class Mode { Off, Mremap, Reserve };

    struct Config
    {
        Mode mode = Mode::Off;
        size_t threshold = 64 * 1024 * 1024;
        size_t reserve_factor = 16;
        size_t reserve_min = 0;    /// at least this much virtual space per reservation (RAM / 8)
        size_t reserve_cap = 1ULL << 40;
        bool promote_only = false;   /// alloc() stays on jemalloc; a buffer moves into a reservation on its first large realloc
        bool protect = false;        /// reserve PROT_NONE without MAP_NORESERVE, mprotect(RW) the used prefix (strict-overcommit friendly)
        Config()
        {
            if (const char * v = getenv("CLICKHOUSE_LARGE_ALLOC_PROMOTE_ONLY")) // NOLINT(concurrency-mt-unsafe)
                promote_only = (*v == '1');
            if (const char * v = getenv("CLICKHOUSE_LARGE_ALLOC_PROTECT")) // NOLINT(concurrency-mt-unsafe)
                protect = (*v == '1');
            if (const char * m = getenv("CLICKHOUSE_LARGE_ALLOC")) // NOLINT(concurrency-mt-unsafe)
            {
                if (0 == strcmp(m, "mremap")) mode = Mode::Mremap;
                else if (0 == strcmp(m, "reserve")) mode = Mode::Reserve;
            }
            if (const char * t = getenv("CLICKHOUSE_LARGE_ALLOC_THRESHOLD")) // NOLINT(concurrency-mt-unsafe)
                threshold = std::max<size_t>(strtoull(t, nullptr, 10), static_cast<size_t>(::getPageSize()));
            if (const char * f = getenv("CLICKHOUSE_LARGE_ALLOC_RESERVE_FACTOR")) // NOLINT(concurrency-mt-unsafe)
                reserve_factor = std::max<size_t>(strtoull(f, nullptr, 10), 1);
            reserve_min = getMemoryAmountOrZero() / 8;
        }
    };

    const Config & config()
    {
        static const Config c;
        return c;
    }

    /// buf -> mapped length (reservation size in Reserve mode, exact size in Mremap mode).
    struct Registry
    {
        std::mutex mutex;
        std::unordered_map<void *, size_t> mapped;
    };
    Registry & registry()
    {
        static Registry r;
        return r;
    }
    void registerMapping(void * buf, size_t len) { auto & r = registry(); std::lock_guard lock(r.mutex); r.mapped[buf] = len; }
    /// Returns 0 if buf is not ours.
    size_t lookupMapping(void * buf) { auto & r = registry(); std::lock_guard lock(r.mutex); auto it = r.mapped.find(buf); return it == r.mapped.end() ? 0 : it->second; }
    void unregisterMapping(void * buf) { auto & r = registry(); std::lock_guard lock(r.mutex); r.mapped.erase(buf); }

    bool eligible(size_t size, size_t alignment)
    {
        const auto & c = config();
        return c.mode != Mode::Off && size >= c.threshold && alignment <= static_cast<size_t>(::getPageSize());
    }

    size_t reservationFor(size_t size)
    {
        const auto & c = config();
        size_t want = std::max(size * c.reserve_factor, c.reserve_min);
        want = std::max(want, size);
        size_t res = roundUpToPowerOfTwoOrZero(want);
        return std::min(res, std::max(c.reserve_cap, size));
    }

    size_t roundUpToPage(size_t n) { const size_t page = ::getPageSize(); return (n + page - 1) / page * page; }

    /// mmap a region. For Reserve mode the region is bigger than `size` and MAP_NORESERVE. Returns nullptr on failure (caller falls back to malloc).
    void * map(size_t size, size_t & mapped_len)
    {
        const auto & c = config();
        int flags = MAP_PRIVATE | MAP_ANONYMOUS;
        if (c.mode == Mode::Reserve)
        {
            mapped_len = reservationFor(size);
            flags |= MAP_NORESERVE;
        }
        else
            mapped_len = size;
        if (c.mode == Mode::Reserve && c.protect)
        {
            flags &= ~MAP_NORESERVE;
            void * buf = ::mmap(nullptr, mapped_len, PROT_NONE, flags, -1, 0);
            if (buf == MAP_FAILED)
                return nullptr;
            if (0 != ::mprotect(buf, roundUpToPage(size), PROT_READ | PROT_WRITE))
            {
                ::munmap(buf, mapped_len);
                return nullptr;
            }
            registerMapping(buf, mapped_len);
            return buf;
        }
        void * buf = ::mmap(nullptr, mapped_len, PROT_READ | PROT_WRITE, flags, -1, 0);
        if (buf == MAP_FAILED)
            return nullptr;
        registerMapping(buf, mapped_len);
        return buf;
    }

    void unmap(void * buf, size_t mapped_len)
    {
        unregisterMapping(buf);
        if (0 != ::munmap(buf, mapped_len))
            throw DB::ErrnoException(DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Allocator: Cannot munmap {}", ReadableSize(mapped_len));
    }

    /// Release pages of [new_size, old_size) so that RSS drops and the bytes read as zero if the buffer grows again.
    void releaseTail(void * buf, size_t new_size, size_t old_size)
    {
        const size_t page = ::getPageSize();
        char * base = reinterpret_cast<char *>(buf);
        size_t first_page = (new_size + page - 1) / page * page;
        if (first_page > new_size)
            memset(base + new_size, 0, std::min(first_page, old_size) - new_size);
        if (old_size > first_page)
        {
            ::madvise(base + first_page, old_size - first_page, MADV_DONTNEED);
            if (config().protect)
                ::mprotect(base + first_page, roundUpToPage(old_size) - first_page, PROT_NONE);
        }
    }
}

template <bool clear_memory, bool populate>
void * allocImpl(size_t size, size_t alignment)
{
    auto trace = CurrentMemoryTracker::alloc(size);

    void * buf = nullptr;
    if (alignment <= MALLOC_MIN_ALIGNMENT) [[likely]]
    {
        if constexpr (clear_memory)
            buf = __real_calloc(size, 1);
        else
            buf = __real_malloc(size);

        if (nullptr == buf) [[unlikely]]
        {
            [[maybe_unused]] auto rollback_trace = CurrentMemoryTracker::free(size);
            throw DB::ErrnoException(
                DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Allocator: Cannot malloc {}.", ReadableSize(static_cast<double>(size)));
        }
    }
    else
    {
        buf = nullptr;
        int res = __real_posix_memalign(&buf, alignment, size);

        if (0 != res) [[unlikely]]
        {
            [[maybe_unused]] auto rollback_trace = CurrentMemoryTracker::free(size);
            // The value of `errno` is not set according to the man: https://man7.org/linux/man-pages/man3/posix_memalign.3.html
            DB::ErrnoException::throwWithErrno(
                DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY, res, "Cannot allocate memory (posix_memalign) {}.", ReadableSize(size));
        }

        if constexpr (clear_memory)
            memset(buf, 0, size);
    }

    if constexpr (populate)
        prefaultPages(buf, size);

    trace.onAlloc(buf, size);
    return buf;
}

void freeImpl(void * buf)
{
    __real_free(buf);
}

void checkSize(size_t size)
{
    /// More obvious exception in case of possible overflow (instead of just "Cannot mmap").
    if (size >= MAX_ALLOCATION_SIZE) [[unlikely]]
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Too large size ({}) passed to allocator. It indicates an error.", size);
}

}


/// Constant is chosen almost arbitrarily, what I observed is 128KB is too small, 1MB is almost indistinguishable from 64MB and 1GB is too large.
extern const size_t POPULATE_THRESHOLD = std::max(Int64{16 * 1024 * 1024}, ::getPageSize());

template <bool clear_memory_, bool populate>
void * Allocator<clear_memory_, populate>::alloc(size_t size, size_t alignment)
{
    checkSize(size);
    if (!LargeAlloc::config().promote_only && LargeAlloc::eligible(size, alignment)) [[unlikely]]
    {
        size_t mapped_len = 0;
        if (void * buf = LargeAlloc::map(size, mapped_len))
        {
            auto trace = CurrentMemoryTracker::alloc(size);
            /// mmap'ed memory is zero-filled, nothing to do for clear_memory.
            if constexpr (populate)
                prefaultPages(buf, size);
            trace.onAlloc(buf, size);
            return buf;
        }
    }
    return allocImpl<clear_memory_, populate>(size, alignment);
}


template <bool clear_memory_, bool populate>
void Allocator<clear_memory_, populate>::free(void * buf, size_t size, size_t /* alignment */)
{
    try
    {
        checkSize(size);
        if (LargeAlloc::config().mode != LargeAlloc::Mode::Off && size >= LargeAlloc::config().threshold) [[unlikely]]
        {
            if (size_t mapped_len = LargeAlloc::lookupMapping(buf))
            {
                LargeAlloc::unmap(buf, mapped_len);
                auto trace = CurrentMemoryTracker::free(size);
                trace.onFree(buf, size);
                return;
            }
        }
        freeImpl(buf);
        auto trace = CurrentMemoryTracker::free(size);
        trace.onFree(buf, size);
    }
    catch (...)
    {
        DB::tryLogCurrentException("Allocator::free");
        throw;
    }
}

template <bool clear_memory_, bool populate>
void * Allocator<clear_memory_, populate>::realloc(void * buf, size_t old_size, size_t new_size, size_t alignment)
{
    checkSize(new_size);

    if (old_size == new_size)
    {
        /// nothing to do.
        /// BTW, it's not possible to change alignment while doing realloc.
        return buf;
    }

    if (LargeAlloc::config().mode != LargeAlloc::Mode::Off) [[unlikely]]
    {
        const auto & cfg = LargeAlloc::config();
        size_t mapped_len = old_size >= cfg.threshold ? LargeAlloc::lookupMapping(buf) : 0;
        if (mapped_len)
        {
            /// The buffer is ours.
            if (new_size < cfg.threshold)
            {
                /// Goes back to malloc.
                void * new_buf = allocImpl<clear_memory_, populate>(new_size, alignment);
                memcpy(new_buf, buf, new_size);
                LargeAlloc::unmap(buf, mapped_len);
                auto trace = CurrentMemoryTracker::free(old_size);
                trace.onFree(buf, old_size);
                return new_buf;
            }
            if (cfg.mode == LargeAlloc::Mode::Reserve && new_size <= mapped_len)
            {
                /// Grow or shrink in place inside the reservation. Pointer does not change.
                if (new_size > old_size)
                {
                    auto trace = CurrentMemoryTracker::alloc(new_size - old_size);
                    if (cfg.protect)
                    {
                        size_t from = LargeAlloc::roundUpToPage(old_size);
                        size_t to = LargeAlloc::roundUpToPage(new_size);
                        if (to > from && 0 != ::mprotect(reinterpret_cast<char *>(buf) + from, to - from, PROT_READ | PROT_WRITE))
                        {
                            [[maybe_unused]] auto rollback = CurrentMemoryTracker::free(new_size - old_size);
                            throw DB::ErrnoException(DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Allocator: Cannot mprotect {} (reservation grow)", ReadableSize(to - from));
                        }
                    }
                    trace.onAlloc(reinterpret_cast<char *>(buf) + old_size, new_size - old_size);
                    /// Pages beyond old_size were never written (or were released by releaseTail), so they read as zero.
                    if constexpr (populate)
                        prefaultPages(reinterpret_cast<char *>(buf) + old_size, new_size - old_size);
                }
                else
                {
                    LargeAlloc::releaseTail(buf, new_size, old_size);
                    auto trace = CurrentMemoryTracker::free(old_size - new_size);
                    trace.onFree(reinterpret_cast<char *>(buf) + new_size, old_size - new_size);
                }
                return buf;
            }
            if (cfg.mode == LargeAlloc::Mode::Mremap)
            {
                if (new_size > old_size)
                    [[maybe_unused]] auto t = CurrentMemoryTracker::alloc(new_size - old_size);
                void * new_buf = clickhouse_mremap(buf, old_size, new_size, MREMAP_MAYMOVE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
                if (MAP_FAILED == new_buf)
                {
                    if (new_size > old_size)
                        [[maybe_unused]] auto t = CurrentMemoryTracker::free(new_size - old_size);
                    throw DB::ErrnoException(DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Allocator: Cannot mremap memory chunk from {} to {}.", ReadableSize(old_size), ReadableSize(new_size));
                }
                if (new_size < old_size)
                    [[maybe_unused]] auto t = CurrentMemoryTracker::free(old_size - new_size);
                LargeAlloc::unregisterMapping(buf);
                LargeAlloc::registerMapping(new_buf, new_size);
                /// No need for zero-fill, because mmap guarantees it.
                if constexpr (populate)
                    prefaultPages(new_buf, new_size);
                return new_buf;
            }
            /// Reserve mode, reservation exhausted: new reservation + copy.
            size_t new_mapped_len = 0;
            if (void * new_buf = LargeAlloc::map(new_size, new_mapped_len))
            {
                auto trace_alloc = CurrentMemoryTracker::alloc(new_size);
                memcpy(new_buf, buf, old_size);
                LargeAlloc::unmap(buf, mapped_len);
                auto trace_free = CurrentMemoryTracker::free(old_size);
                trace_free.onFree(buf, old_size);
                trace_alloc.onAlloc(new_buf, new_size);
                if constexpr (populate)
                    prefaultPages(new_buf, new_size);
                return new_buf;
            }
            /// mmap failed: fall through to the generic copy below (malloc'ed destination, old mapping unmapped).
            void * new_buf = allocImpl<clear_memory_, populate>(new_size, alignment);
            memcpy(new_buf, buf, old_size);
            LargeAlloc::unmap(buf, mapped_len);
            auto trace = CurrentMemoryTracker::free(old_size);
            trace.onFree(buf, old_size);
            return new_buf;
        }
        if (LargeAlloc::eligible(new_size, alignment))
        {
            /// malloc'ed buffer crosses the threshold: move it into a mapping.
            size_t new_mapped_len = 0;
            if (void * new_buf = LargeAlloc::map(new_size, new_mapped_len))
            {
                auto trace_alloc = CurrentMemoryTracker::alloc(new_size);
                memcpy(new_buf, buf, std::min(old_size, new_size));
                freeImpl(buf);
                auto trace_free = CurrentMemoryTracker::free(old_size);
                trace_free.onFree(buf, old_size);
                trace_alloc.onAlloc(new_buf, new_size);
                if constexpr (populate)
                    prefaultPages(new_buf, new_size);
                return new_buf;
            }
        }
    }

    if (alignment <= MALLOC_MIN_ALIGNMENT) [[likely]]
    {
        /// Resize malloc'd memory region with no special alignment requirement.
        /// Realloc can do 2 possible things:
        /// - expand existing memory region
        /// - allocate new memory block and free the old one
        /// Because we don't know which option will be picked we need to make sure there is enough
        /// memory for all options
        auto trace_alloc = CurrentMemoryTracker::alloc(new_size);

        void * new_buf = __real_realloc(buf, new_size);
        if (nullptr == new_buf) [[unlikely]]
        {
            [[maybe_unused]] auto trace_free = CurrentMemoryTracker::free(new_size);
            throw DB::ErrnoException(
                DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY,
                "Allocator: Cannot realloc from {} to {}",
                ReadableSize(old_size),
                ReadableSize(new_size));
        }

        auto trace_free = CurrentMemoryTracker::free(old_size);
        trace_free.onFree(buf, old_size);
        trace_alloc.onAlloc(new_buf, new_size);
        buf = new_buf;

        if constexpr (clear_memory)
            if (new_size > old_size)
                memset(reinterpret_cast<char *>(buf) + old_size, 0, new_size - old_size);
    }
    else
    {
        /// Big allocs that requires a copy. MemoryTracker is called inside 'alloc', 'free' methods.
        void * new_buf = alloc(new_size, alignment);
        memcpy(new_buf, buf, std::min(old_size, new_size));
        free(buf, old_size);
        buf = new_buf;
    }

    if constexpr (populate)
        prefaultPages(buf, new_size);

    return buf;
}


template class Allocator<false, false>;
template class Allocator<true, false>;
template class Allocator<false, true>;
template class Allocator<true, true>;
