#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPoolMetaFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCatalogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasOrphanManifestSweep.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasMountLoss.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasMountReacquire.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/tests/cas_process_stop_log.h>
#include <Disks/tests/cas_test_helpers.h>

#include <Poco/Exception.h>

#include "config.h"

#if USE_AWS_S3
#include <IO/S3Common.h>
#endif

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <vector>

using namespace DB::Cas;
using DB::Cas::tests::OperationForTest;

namespace
{

const String kSrid = "test";
const Layout kLayout("p");
using WriteAnswer = std::expected<String, Backend::RawConflict>;
using Boot = std::shared_ptr<std::atomic<uint64_t>>;

std::optional<Object> read(Backend & backend, const String & key)
{
    OperationForTest op(backend);
    return (*op).read(key, Retry::once());
}

void put(Backend & backend, const String & key, const String & bytes)
{
    OperationForTest op(backend);
    const auto got = (*op).read(key, Retry::once());
    const WriteResult written = got ? (*op).replace(key, bytes, got->etag, Retry::once()) : (*op).create(key, bytes, Retry::once());
    ASSERT_TRUE(std::holds_alternative<Committed>(written)) << key;
}

void erase(Backend & backend, const String & key)
{
    OperationForTest op(backend);
    ASSERT_EQ((*op).removeCurrent(key, Retry::once()), Removal::Removed) << key;
}

MountLease slotBody(Backend & backend)
{
    return decodeMountLease(read(backend, kLayout.mountKey(kSrid))->bytes);
}

void rewriteSlot(Backend & backend, const std::function<void(MountLease &)> & edit)
{
    MountLease m = slotBody(backend);
    edit(m);
    put(backend, kLayout.mountKey(kSrid), encodeMountLease(m));
}

uint64_t counterNext(Backend & backend)
{
    return decodeServerEpoch(read(backend, kLayout.epochKey(kSrid))->bytes).next_writer_epoch;
}

template <typename B>
PoolPtr openPool(const std::shared_ptr<B> & backend, const Boot & boot, const std::function<void(PoolConfig &)> & edit = {})
{
    const auto advance = [boot](uint64_t ms) { boot->fetch_add(ms); };
    PoolConfig config{
        .pool_prefix = "p",
        .server_root_id = kSrid,
        .boot_ms_fn = [boot] { return boot->load(); },
        .retry_sleep_fn = advance,
        .wait_sleep_fn = advance,
    };
    if (edit)
        edit(config);
    return Pool::open(backend, std::move(config));
}

Boot newBoot()
{
    return std::make_shared<std::atomic<uint64_t>>(1'000'000);
}

/// The GC fences our own body and the next renewal finds it.
void fenceAndRenew(Pool & store, Backend & backend)
{
    rewriteSlot(backend, [](MountLease & m) { m.gc_fenced = true; ++m.seq; });
    EXPECT_THROW(store.renewWatermarkOnce(), DB::Exception);
    ASSERT_EQ(store.mountEnd(), MountEnd::None);
}

/// Runs the kept step until it settles; a step that never settles fails the test.
ReacquireOutcome reacquireUntilSettled(Pool & store)
{
    for (size_t i = 0; i < 100; ++i)
        if (const ReacquireOutcome outcome = store.runReacquireForTest(); outcome != ReacquireOutcome::Retry)
            return outcome;
    ADD_FAILURE() << "the reacquire never settled";
    return ReacquireOutcome::Retry;
}

struct Fenced
{
    std::shared_ptr<InMemoryBackend> backend = std::make_shared<InMemoryBackend>();
    Boot boot = newBoot();
    PoolPtr store;
    uint64_t epoch = 0;

    explicit Fenced(const std::function<void(PoolConfig &)> & edit = {})
        : store(openPool(backend, boot, edit))
        , epoch(store->writerEpoch())
    {
        DB::Cas::tests::processStopLog().reset();
        fenceAndRenew(*store, *backend);
    }
};

void expectStoppedOn(const Fenced & f, ReacquireOutcome outcome, MountEnd end)
{
    EXPECT_EQ(outcome, ReacquireOutcome::Ended);
    EXPECT_EQ(f.store->mountEnd(), end);
    EXPECT_FALSE(f.store->mayMutate());
    EXPECT_EQ(DB::Cas::tests::processStopLog().graceful(), 1u);
}

}

TEST(CASReacquire, AGcFenceOfTheActiveEpochOpensANewEpoch)
{
    Fenced f;

    EXPECT_EQ(f.store->runReacquireForTest(), ReacquireOutcome::Open);

    EXPECT_EQ(f.store->writerEpoch(), f.epoch + 1);
    EXPECT_EQ(counterNext(*f.backend), f.epoch + 2);
    const MountLease slot = slotBody(*f.backend);
    EXPECT_EQ(slot.writer_epoch, f.epoch + 1);
    EXPECT_FALSE(slot.gc_fenced);
    EXPECT_EQ(f.store->mountEnd(), MountEnd::None);
    EXPECT_EQ(f.store->lifecycle(), PoolLifecycle::Live);
    EXPECT_TRUE(f.store->mayMutate());
}

TEST(CASReacquire, TheOldEpochClosesBeforeTheCounterMoves)
{
    auto backend = std::make_shared<InMemoryBackend>();
    std::vector<uint64_t> counter_at_close;
    auto store = openPool(backend, newBoot(), [&](PoolConfig & c)
    {
        c.reacquire_quiesce_hook_for_test = [&] { counter_at_close.push_back(counterNext(*backend)); };
    });
    const uint64_t epoch = store->writerEpoch();
    fenceAndRenew(*store, *backend);

    EXPECT_EQ(store->runReacquireForTest(), ReacquireOutcome::Open);

    ASSERT_EQ(counter_at_close.size(), 1u);
    EXPECT_EQ(counter_at_close[0], epoch + 1) << "the old epoch closes before the counter moves";
}

TEST(CASReacquire, ATwinThatTookAndReleasedTheSlotStops)
{
    Fenced f;
    rewriteSlot(*f.backend, [](MountLease & m)
    {
        m.writer_epoch += 5;
        m.gc_fenced = false;
        m.min_active_build_sequence = std::numeric_limits<uint64_t>::max();
        m.write_attempt_id = DB::UInt128{55};
    });
    const String twin = read(*f.backend, kLayout.mountKey(kSrid))->bytes;

    expectStoppedOn(f, f.store->runReacquireForTest(), MountEnd::SlotTaken);
    EXPECT_EQ(read(*f.backend, kLayout.mountKey(kSrid))->bytes, twin);
}

TEST(CASReacquire, ACleanMarkerOfTheFencedPairStops)
{
    Fenced f;
    rewriteSlot(*f.backend, [](MountLease & m)
    {
        m.gc_fenced = false;
        m.min_active_build_sequence = std::numeric_limits<uint64_t>::max();
    });
    expectStoppedOn(f, f.store->runReacquireForTest(), MountEnd::SlotCorrupt);
}

TEST(CASReacquire, AnAbsentSlotStops)
{
    Fenced f;
    erase(*f.backend, kLayout.mountKey(kSrid));

    expectStoppedOn(f, f.store->runReacquireForTest(), MountEnd::SlotAbsent);
    EXPECT_FALSE(read(*f.backend, kLayout.mountKey(kSrid)).has_value()) << "a reacquire never creates the slot";
}

TEST(CASReacquire, AForeignBodyStops)
{
    Fenced f;
    rewriteSlot(*f.backend, [](MountLease & m)
    {
        m.server_uuid = DB::UInt128{0xF0F0};
        m.gc_fenced = false;
    });
    expectStoppedOn(f, f.store->runReacquireForTest(), MountEnd::SlotTaken);
}

TEST(CASReacquire, TheUnsafeKnobNeverReclaimsATwin)
{
    Fenced f([](PoolConfig & c) { c.unsafe_remount_no_delay = true; });
    rewriteSlot(*f.backend, [](MountLease & m)
    {
        m.writer_epoch += 5;
        m.gc_fenced = false;
        m.write_attempt_id = DB::UInt128{55};
    });
    const String twin = read(*f.backend, kLayout.mountKey(kSrid))->bytes;

    expectStoppedOn(f, f.store->runReacquireForTest(), MountEnd::SlotTaken);
    EXPECT_EQ(read(*f.backend, kLayout.mountKey(kSrid))->bytes, twin);
}

namespace
{

/// Records the reads and writes of the epoch counter; `fail_counter_writes_until` fails each counter write
/// before it reaches the store, as a `503` does, until the boot clock gets there.
class CounterBackend : public InMemoryBackend
{
public:
    struct Write { String bytes; std::optional<String> expected; };

    explicit CounterBackend(Boot boot_) : boot(std::move(boot_)) {}

    WriteAnswer write(const String & key, const String & bytes, const std::optional<String> & expected, TransportAccess & access) override
    {
        if (key == kLayout.epochKey(kSrid) && recording)
        {
            writes.push_back({bytes, expected});
            if (before_counter_write)
                std::exchange(before_counter_write, {})();
            if (boot->load() < fail_counter_writes_until)
            {
#if USE_AWS_S3
                throw DB::S3Exception("injected service unavailable", Aws::S3::S3Errors::SERVICE_UNAVAILABLE);
#else
                throw Poco::TimeoutException("injected service unavailable");
#endif
            }
        }
        return InMemoryBackend::write(key, bytes, expected, access);
    }

    std::optional<Raw> read(const String & key, TransportAccess & access) override
    {
        if (key == kLayout.epochKey(kSrid) && recording)
            ++counter_reads;
        return InMemoryBackend::read(key, access);
    }

    Boot boot;
    bool recording = false;
    uint64_t fail_counter_writes_until = 0;
    std::function<void()> before_counter_write;
    std::vector<Write> writes;
    size_t counter_reads = 0;
};

}

/// Every resolve read of a failed attempt is a counter `GET` too, so the one-read claim is that the
/// reads are the first one plus one per failed attempt: nothing re-reads to re-decide.
TEST(CASReacquire, AStorm503OnTheCounterAllocatesOneNumber)
{
    const Boot boot = newBoot();
    auto backend = std::make_shared<CounterBackend>(boot);
    auto store = openPool(backend, boot);
    const uint64_t epoch = store->writerEpoch();
    fenceAndRenew(*store, *backend);
    backend->recording = true;
    backend->fail_counter_writes_until = boot->load() + 30'000;

    EXPECT_EQ(store->runReacquireForTest(), ReacquireOutcome::Open);
    backend->recording = false;

    EXPECT_EQ(store->writerEpoch(), epoch + 1);
    EXPECT_EQ(counterNext(*backend), epoch + 2);
    ASSERT_GT(backend->writes.size(), 10u) << "the storm covered many attempts";
    for (const auto & write : backend->writes)
    {
        EXPECT_EQ(write.bytes, backend->writes.front().bytes);
        EXPECT_EQ(write.expected, backend->writes.front().expected);
    }
    EXPECT_EQ(backend->counter_reads, backend->writes.size()) << "one read, then one resolve read per failed attempt";
}

TEST(CASReacquire, AnUnclearCounterIncrementIsSettledByReading)
{
    Fenced f;
    f.backend->injectAmbiguousLandedWrite(kLayout.epochKey(kSrid));

    EXPECT_EQ(f.store->runReacquireForTest(), ReacquireOutcome::Open);

    EXPECT_EQ(f.store->writerEpoch(), f.epoch + 1);
    EXPECT_EQ(counterNext(*f.backend), f.epoch + 2);
}

namespace
{

/// The armed counter write lands and loses its answer; then every request fails until the boot clock
/// has moved `outage_ms` past it.
class LandedThenOutageBackend : public InMemoryBackend
{
public:
    explicit LandedThenOutageBackend(Boot boot_) : boot(std::move(boot_)) {}

    WriteAnswer write(const String & key, const String & bytes, const std::optional<String> & expected, TransportAccess & access) override
    {
        failIfDown();
        if (armed && key == kLayout.epochKey(kSrid))
        {
            armed = false;
            if (auto landed = InMemoryBackend::write(key, bytes, expected, access); !landed)
                return landed;
            down_until = boot->load() + outage_ms;
            throw Poco::TimeoutException("injected: answer lost");
        }
        return InMemoryBackend::write(key, bytes, expected, access);
    }

    std::optional<Raw> read(const String & key, TransportAccess & access) override
    {
        failIfDown();
        return InMemoryBackend::read(key, access);
    }

    RawListPage list(const String & prefix, const String & cursor, size_t limit, TransportAccess & access) override
    {
        failIfDown();
        return InMemoryBackend::list(prefix, cursor, limit, access);
    }

    Boot boot;
    bool armed = false;
    uint64_t outage_ms = 120'000;

private:
    void failIfDown() const
    {
        if (boot->load() < down_until)
            throw Poco::TimeoutException("injected outage");
    }
    uint64_t down_until = 0;
};

}

TEST(CASReacquire, AnIncrementLandedBeforeAnOutageAllocatesOneNumber)
{
    const Boot boot = newBoot();
    auto backend = std::make_shared<LandedThenOutageBackend>(boot);
    auto store = openPool(backend, boot);
    const uint64_t epoch = store->writerEpoch();
    DB::Cas::tests::processStopLog().reset();
    fenceAndRenew(*store, *backend);
    backend->armed = true;

    EXPECT_EQ(reacquireUntilSettled(*store), ReacquireOutcome::Open);

    EXPECT_EQ(store->writerEpoch(), epoch + 1);
    EXPECT_EQ(counterNext(*backend), epoch + 2);
}

TEST(CASReacquire, ACounterAdvancedByAnotherProcessStops)
{
    const Boot boot = newBoot();
    auto backend = std::make_shared<CounterBackend>(boot);
    auto store = openPool(backend, boot);
    DB::Cas::tests::processStopLog().reset();
    fenceAndRenew(*store, *backend);
    backend->recording = true;
    backend->before_counter_write = [&]
    {
        backend->recording = false;
        put(*backend, kLayout.epochKey(kSrid), encodeServerEpoch(ServerEpoch{.next_writer_epoch = counterNext(*backend) + 1}));
        backend->recording = true;
    };
    auto & log = DB::Cas::tests::processStopLog();
    DB::Cas::tests::ScopedStopLineCapture lines;

    EXPECT_EQ(store->runReacquireForTest(), ReacquireOutcome::Ended);

    EXPECT_EQ(store->mountEnd(), MountEnd::SlotTaken);
    EXPECT_EQ(log.graceful(), 1u);
    bool named = false;
    for (const auto & event : log.events())
        named |= event.starts_with("error-line:") && event.contains("advanced by another process");
    EXPECT_TRUE(named);
}

/// A counter rewritten with the value we read refreshes the expected ETag and fails the pass; the next
/// pass repeats the tuple it fixed, so an advance in between stops the server instead of being taken.
TEST(CASReacquire, ACounterWriteRepeatedAcrossPassesNeverReDecides)
{
    const Boot boot = newBoot();
    auto backend = std::make_shared<CounterBackend>(boot);
    auto store = openPool(backend, boot);
    DB::Cas::tests::processStopLog().reset();
    fenceAndRenew(*store, *backend);
    backend->recording = true;
    backend->before_counter_write = [&]
    {
        backend->recording = false;
        put(*backend, kLayout.epochKey(kSrid), read(*backend, kLayout.epochKey(kSrid))->bytes);
        backend->recording = true;
    };

    ASSERT_EQ(store->runReacquireForTest(), ReacquireOutcome::Retry);
    backend->recording = false;
    put(*backend, kLayout.epochKey(kSrid), encodeServerEpoch(ServerEpoch{.next_writer_epoch = counterNext(*backend) + 1}));
    backend->recording = true;

    EXPECT_EQ(store->runReacquireForTest(), ReacquireOutcome::Ended);
    EXPECT_EQ(store->mountEnd(), MountEnd::SlotTaken);
}

#if USE_AWS_S3
/// A refused counter write is repeated with the tuple it fixed: an advance in between stops the server
/// instead of being read as the next number to take.
TEST(CASReacquire, ARefusedCounterWriteNeverReDecides)
{
    const Boot boot = newBoot();
    auto backend = std::make_shared<CounterBackend>(boot);
    auto store = openPool(backend, boot);
    DB::Cas::tests::processStopLog().reset();
    fenceAndRenew(*store, *backend);
    backend->recording = true;
    backend->before_counter_write = [&]
    {
        backend->recording = false;
        put(*backend, kLayout.epochKey(kSrid), encodeServerEpoch(ServerEpoch{.next_writer_epoch = counterNext(*backend) + 1}));
        backend->recording = true;
        throw DB::S3Exception("injected counter access denial", Aws::S3::S3Errors::ACCESS_DENIED);
    };

    EXPECT_EQ(reacquireUntilSettled(*store), ReacquireOutcome::Ended);
    EXPECT_EQ(store->mountEnd(), MountEnd::SlotTaken);
}
#endif

TEST(CASReacquire, ABreakAfterTheClaimContinuesWithOurBody)
{
    auto backend = std::make_shared<DB::Cas::tests::CountingBackend>();
    const Boot boot = newBoot();
    auto store = openPool(backend, boot);
    const uint64_t epoch = store->writerEpoch();
    fenceAndRenew(*store, *backend);
    const String key = kLayout.mountKey(kSrid);
    const uint64_t slot_writes = backend->putOverwriteCount(key);
    backend->injectAmbiguousLandedWrite(key);

    EXPECT_EQ(store->runReacquireForTest(), ReacquireOutcome::Open);

    EXPECT_EQ(backend->putOverwriteCount(key), slot_writes + 1) << "one claim body";
    EXPECT_EQ(slotBody(*backend).writer_epoch, epoch + 1);
}

namespace
{

/// The armed write of the mount slot lands, then the boot clock jumps by `stall_ms`; the next one, the
/// first renewal, is refused.
class LateClaimBackend : public InMemoryBackend
{
public:
    explicit LateClaimBackend(Boot boot_) : boot(std::move(boot_)) {}

    WriteAnswer write(const String & key, const String & bytes, const std::optional<String> & expected, TransportAccess & access) override
    {
        if (key == kLayout.mountKey(kSrid) && armed)
        {
            ++slot_writes;
            if (slot_writes == 2)
            {
#if USE_AWS_S3
                throw DB::S3Exception("injected renewal access denial", Aws::S3::S3Errors::ACCESS_DENIED);
#else
                throw Poco::TimeoutException("injected renewal timeout");
#endif
            }
            WriteAnswer landed = InMemoryBackend::write(key, bytes, expected, access);
            if (slot_writes == 1)
                boot->fetch_add(stall_ms);
            return landed;
        }
        return InMemoryBackend::write(key, bytes, expected, access);
    }

    Boot boot;
    bool armed = false;
    uint64_t stall_ms = 25'000;
    size_t slot_writes = 0;
};

}

TEST(CASReacquire, ARefusedRenewalAfterALateClaimResumesWithoutANewEpoch)
{
    const Boot boot = newBoot();
    auto backend = std::make_shared<LateClaimBackend>(boot);
    auto store = openPool(backend, boot);
    const uint64_t epoch = store->writerEpoch();
    fenceAndRenew(*store, *backend);
    const uint64_t fenced_seq = slotBody(*backend).seq;
    backend->armed = true;

    EXPECT_EQ(reacquireUntilSettled(*store), ReacquireOutcome::Open);

    EXPECT_EQ(store->writerEpoch(), epoch + 1);
    EXPECT_EQ(counterNext(*backend), epoch + 2);
    EXPECT_EQ(backend->slot_writes, 3u) << "the claim, the refused renewal and the renewal that landed";
    EXPECT_EQ(slotBody(*backend).writer_epoch, epoch + 1);
    EXPECT_EQ(slotBody(*backend).seq, fenced_seq + 2) << "one claim, then one renewal above it";
    EXPECT_TRUE(store->mayMutate());
}

namespace
{

/// While `hidden`, a read of `key` finds nothing although a LIST still lists it.
class HiddenKeyBackend : public InMemoryBackend
{
public:
    std::optional<Raw> read(const String & k, TransportAccess & access) override
    {
        if (hidden && k == key)
            return std::nullopt;
        return InMemoryBackend::read(k, access);
    }
    String key;
    bool hidden = false;
};

}

TEST(CASReacquire, AnAbsentAnchorIsPoolIdentityLostAndNotCreated)
{
    Fenced f;
    erase(*f.backend, kLayout.ownerKey(kSrid));

    expectStoppedOn(f, f.store->runReacquireForTest(), MountEnd::PoolIdentityLost);
    EXPECT_FALSE(read(*f.backend, kLayout.ownerKey(kSrid)).has_value());
}

TEST(CASReacquire, AnAnchorAbsenceTheListContradictsRetries)
{
    auto backend = std::make_shared<HiddenKeyBackend>();
    backend->key = kLayout.ownerKey(kSrid);
    auto store = openPool(backend, newBoot());
    DB::Cas::tests::processStopLog().reset();
    fenceAndRenew(*store, *backend);
    backend->hidden = true;

    EXPECT_EQ(store->runReacquireForTest(), ReacquireOutcome::Retry);
    EXPECT_EQ(store->mountEnd(), MountEnd::None);
    EXPECT_TRUE(DB::Cas::tests::processStopLog().events().empty());

    backend->hidden = false;
    EXPECT_EQ(store->runReacquireForTest(), ReacquireOutcome::Open);
}

TEST(CASReacquire, AnAbsentCounterIsPoolIdentityLost)
{
    Fenced f;
    erase(*f.backend, kLayout.epochKey(kSrid));

    expectStoppedOn(f, f.store->runReacquireForTest(), MountEnd::PoolIdentityLost);
    EXPECT_FALSE(read(*f.backend, kLayout.epochKey(kSrid)).has_value());
}

TEST(CASReacquire, AnAmbiguousCatalogIsALedgerFault)
{
    Fenced f;
    const auto entry = [](const char * ns) { return CatalogEntry{.ns = RootNamespace{ns}, .state = NsState::Live, .incarnation = DB::UInt128(42)}; };
    put(*f.backend, kLayout.refCatalogKey(), encodeRefCatalog(RefCatalog{.entries = {entry("a"), entry("b")}}));

    EXPECT_EQ(f.store->runReacquireForTest(), ReacquireOutcome::Ended);

    EXPECT_EQ(f.store->mountEnd(), MountEnd::LedgerFault);
    EXPECT_EQ(DB::Cas::tests::processStopLog().fatal(), 1u);
    EXPECT_EQ(DB::Cas::tests::processStopLog().graceful(), 0u);
}

namespace
{

void expectNeverOpens(const std::function<void(Pool &)> & before_open)
{
    std::shared_ptr<Pool> * target = nullptr;
    Fenced f([&](PoolConfig & c) { c.reacquire_before_open_hook_for_test = [&] { before_open(**target); }; });
    target = &f.store;

    EXPECT_NE(f.store->runReacquireForTest(), ReacquireOutcome::Open);

    EXPECT_FALSE(f.store->mayMutate());
    EXPECT_NE(f.store->lifecycle(), PoolLifecycle::Live);
}

}

TEST(CASReacquire, InterferenceDuringReacquireNeverOpens)
{
    expectNeverOpens([](Pool & store) { ASSERT_TRUE(store.latchMountEndForTest(MountEnd::LedgerFault)); });
}

/// An end that is not a loss leaves the pool paused: only the end itself keeps the fence closed.
TEST(CASReacquire, AnEndWithoutALossLatchedDuringReacquireNeverOpens)
{
    expectNeverOpens([](Pool & store) { ASSERT_TRUE(store.latchMountEndForTest(MountEnd::Forgotten)); });
}

TEST(CASReacquire, ForgetDuringReacquireNeverOpens)
{
    expectNeverOpens([](Pool & store) { store.publishVanishedIntentForTest(); });
}

TEST(CASReacquire, StopDuringReacquireNeverOpens)
{
    expectNeverOpens([](Pool & store) { store.beginShutdownForTest(); });
}

TEST(CASReacquire, TheLeaseRunningOutBeforeOpenRenewsFirst)
{
    const Boot boot = newBoot();
    bool jumped = false;
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPool(backend, boot, [&](PoolConfig & c)
    {
        c.reacquire_before_open_hook_for_test = [&]
        {
            if (!std::exchange(jumped, true))
                boot->fetch_add(60'000);
        };
    });
    const uint64_t epoch = store->writerEpoch();
    fenceAndRenew(*store, *backend);

    EXPECT_EQ(store->runReacquireForTest(), ReacquireOutcome::Retry);
    EXPECT_FALSE(store->mayMutate());
    const uint64_t claim_seq = slotBody(*backend).seq;

    EXPECT_EQ(store->runReacquireForTest(), ReacquireOutcome::Open);
    EXPECT_GT(slotBody(*backend).seq, claim_seq) << "a renewal before the open";
    EXPECT_EQ(store->writerEpoch(), epoch + 1);
    EXPECT_TRUE(store->mayMutate());
}

TEST(CASReacquire, AFencedFloorProtectsItsPrefixUntilTheNewClaim)
{
    Fenced f;
    const MountLease fenced = slotBody(*f.backend);
    ASSERT_TRUE(fenced.gc_fenced);
    const RootNamespace ns{"test/store/465/aa@cas@"};
    const BuildPrefix prefix{f.epoch, fenced.min_active_build_sequence};

    EXPECT_FALSE(prefixEligible(*f.store, ns, prefix));
    ASSERT_EQ(f.store->runReacquireForTest(), ReacquireOutcome::Open);
    EXPECT_TRUE(prefixEligible(*f.store, ns, prefix));
}

namespace
{

/// A writable pool whose renewal thread runs: every renewal the thread finishes wakes `changed`.
struct Running
{
    template <typename B>
    explicit Running(const std::shared_ptr<B> & backend_, const std::function<void(const MountRenewResult &)> & on_renewal = {})
        : backend(backend_)
    {
        PoolConfig config{.pool_prefix = "p", .server_root_id = kSrid, .background_watermark = true};
        config.mount_renew_period = std::chrono::milliseconds(20);
        config.renewal_finished_hook_for_test = [this, on_renewal](const MountRenewResult & result)
        {
            if (on_renewal)
                on_renewal(result);
            notify();
        };
        DB::Cas::tests::processStopLog().reset([this] { notify(); return String{}; });
        store = Pool::open(backend_, std::move(config));
    }

    ~Running()
    {
        store.reset();
    }

    void notify()
    {
        std::lock_guard lock(mutex);
        ++changes;
        changed.notify_all();
    }

    /// Hang protection only: every renewal and every stop request re-checks `done`.
    bool waitFor(const std::function<bool()> & done)
    {
        std::unique_lock lock(mutex);
        return changed.wait_for(lock, std::chrono::seconds(60), [&] { return done(); });
    }

    std::shared_ptr<Backend> backend;
    PoolPtr store;
    std::mutex mutex;
    std::condition_variable changed;
    uint64_t changes = 0;
};

/// The GC's fence transform, retried when a renewal moves the slot between our read and our write.
void fenceWhileRenewing(Backend & backend)
{
    for (size_t i = 0; i < 1000; ++i)
    {
        OperationForTest op(backend);
        const auto got = (*op).read(kLayout.mountKey(kSrid), Retry::once());
        ASSERT_TRUE(got.has_value());
        MountLease m = decodeMountLease(got->bytes);
        m.gc_fenced = true;
        ++m.seq;
        if (std::holds_alternative<Committed>((*op).replace(kLayout.mountKey(kSrid), encodeMountLease(m), got->etag, Retry::once())))
            return;
    }
    FAIL() << "the renewal kept moving the slot";
}

/// Records, in order, the writer epoch of every mount slot write and every counter write.
class OrderRecorder : public InMemoryBackend
{
public:
    struct Entry { bool counter; uint64_t writer_epoch; };

    WriteAnswer write(const String & key, const String & bytes, const std::optional<String> & expected, TransportAccess & access) override
    {
        if (key == kLayout.mountKey(kSrid) || key == kLayout.epochKey(kSrid))
        {
            const bool counter = key == kLayout.epochKey(kSrid);
            std::lock_guard lock(mutex);
            entries.push_back({counter, counter ? 0 : decodeMountLease(bytes).writer_epoch});
        }
        return InMemoryBackend::write(key, bytes, expected, access);
    }

    std::vector<Entry> snapshot()
    {
        std::lock_guard lock(mutex);
        return entries;
    }

    void clear()
    {
        std::lock_guard lock(mutex);
        entries.clear();
    }

private:
    std::mutex mutex;
    std::vector<Entry> entries;
};

/// Holds the first mount-slot write after `armed` until `gate` is released, then fails it before it reaches
/// the store, as a `503` does: the renewal's next attempt re-checks its liveness.
class HeldRenewalBackend : public InMemoryBackend
{
public:
    WriteAnswer write(const String & key, const String & bytes, const std::optional<String> & expected, TransportAccess & access) override
    {
        if (key == kLayout.mountKey(kSrid) && armed.exchange(false))
        {
            gate.arriveAndWait();
            throw Poco::TimeoutException("injected: renewal held");
        }
        return InMemoryBackend::write(key, bytes, expected, access);
    }

    std::atomic<bool> armed{false};
    DB::Cas::tests::ManualBarrier gate;
};

}

TEST(CASReacquire, TheRenewalThreadReacquiresAnOwnFence)
{
    Running running(std::make_shared<InMemoryBackend>());
    Pool & store = *running.store;
    const uint64_t epoch = store.writerEpoch();

    fenceWhileRenewing(*running.backend);

    ASSERT_TRUE(running.waitFor([&] { return store.lifecycle() == PoolLifecycle::Live && store.writerEpoch() == epoch + 1; }));
    EXPECT_EQ(store.mountEnd(), MountEnd::None);
    EXPECT_EQ(slotBody(*running.backend).writer_epoch, epoch + 1);
}

TEST(CASReacquire, NoRenewalWriteOfTheOldEpochAfterTheCounterMoves)
{
    auto backend = std::make_shared<OrderRecorder>();
    Running running(backend);
    Pool & store = *running.store;
    const uint64_t epoch = store.writerEpoch();
    backend->clear();

    fenceWhileRenewing(*backend);
    ASSERT_TRUE(running.waitFor([&] { return store.lifecycle() == PoolLifecycle::Live && store.writerEpoch() == epoch + 1; }));
    /// One renewal of the new epoch after the open, so the window covers the thread's return to renewing.
    const uint64_t changes = running.changes;
    ASSERT_TRUE(running.waitFor([&] { return running.changes > changes + 1; }));

    bool counter_moved = false;
    for (const auto & entry : backend->snapshot())
    {
        counter_moved |= entry.counter;
        if (counter_moved && !entry.counter)
            EXPECT_NE(entry.writer_epoch, epoch) << "a mount write of the fenced epoch after the counter moved";
    }
    EXPECT_TRUE(counter_moved);
}

TEST(CASReacquire, ALossFoundByTheReacquireStopsTheServer)
{
    auto backend = std::make_shared<InMemoryBackend>();
    std::atomic<bool> taken{false};
    Running running(backend, [&](const MountRenewResult & result)
    {
        if (result.slot && result.slot->verdict == MountSlotVerdict::FencedByGc && !taken.exchange(true))
            rewriteSlot(*backend, [](MountLease & m)
            {
                m.server_uuid = DB::UInt128{0xF0F0};
                m.gc_fenced = false;
            });
    });
    Pool & store = *running.store;

    fenceWhileRenewing(*backend);

    ASSERT_TRUE(running.waitFor([&] { return DB::Cas::tests::processStopLog().graceful() == 1; }));
    EXPECT_TRUE(taken.load());
    EXPECT_EQ(store.mountEnd(), MountEnd::SlotTaken);
}

TEST(CASReacquire, ForgetDuringAWorkerRenewalClosesWithoutAStop)
{
    auto backend = std::make_shared<HeldRenewalBackend>();
    Running running(backend);
    Pool & store = *running.store;
    backend->armed = true;
    backend->gate.waitUntilArrived();

    /// FORGET publishes its intent and trips the fence before it stops the GC; the held renewal resumes only then.
    store.forgetDisk([&] { backend->gate.release(); }, "decommissioned by the test");

    EXPECT_EQ(store.lifecycle(), PoolLifecycle::VanishedForgotten);
    EXPECT_EQ(store.mountEnd(), MountEnd::Forgotten);
    EXPECT_EQ(DB::Cas::tests::processStopLog().graceful(), 0u);
    EXPECT_EQ(DB::Cas::tests::processStopLog().fatal(), 0u);
}

TEST(CASReacquire, AStopDuringAWorkerRenewalEndsWithoutAnEnd)
{
    auto backend = std::make_shared<HeldRenewalBackend>();
    Running running(backend);
    Pool & store = *running.store;
    const uint64_t epoch = store.writerEpoch();
    backend->armed = true;
    backend->gate.waitUntilArrived();
    const uint64_t changes = running.changes;

    store.beginShutdownForTest();
    backend->gate.release();

    ASSERT_TRUE(running.waitFor([&] { return running.changes > changes; }));
    EXPECT_EQ(store.mountEnd(), MountEnd::None);
    EXPECT_EQ(store.writerEpoch(), epoch);
    EXPECT_EQ(counterNext(*backend), epoch + 1);
    EXPECT_EQ(DB::Cas::tests::processStopLog().graceful(), 0u);
    EXPECT_EQ(DB::Cas::tests::processStopLog().fatal(), 0u);
}

TEST(CASReacquire, AForeignPoolMetaStopsTheReacquire)
{
    Fenced f;
    PoolMeta meta = decodePoolMeta(read(*f.backend, kLayout.poolMetaKey())->bytes);
    meta.pool_id = meta.pool_id + DB::UInt128(1);
    put(*f.backend, kLayout.poolMetaKey(), encodePoolMeta(meta));

    expectStoppedOn(f, reacquireUntilSettled(*f.store), MountEnd::PoolIdentityLost);
    EXPECT_EQ(counterNext(*f.backend), f.epoch + 1) << "no epoch is allocated after the identity probe fails";
}

TEST(CASReacquire, AnUnreadablePoolMetaRetriesTheReacquire)
{
    Fenced f;
    put(*f.backend, kLayout.poolMetaKey(), "not a pool meta");

    EXPECT_EQ(f.store->runReacquireForTest(), ReacquireOutcome::Retry);
    EXPECT_EQ(f.store->mountEnd(), MountEnd::None);
    EXPECT_FALSE(f.store->mayMutate());
    EXPECT_EQ(counterNext(*f.backend), f.epoch + 1);
}

TEST(CASReacquire, ChangedAlgosStillReacquire)
{
    Fenced f;
    PoolMeta meta = decodePoolMeta(read(*f.backend, kLayout.poolMetaKey())->bytes);
    const auto extra = static_cast<uint8_t>(BlobHashAlgo::XXH3_128);
    ASSERT_FALSE(std::binary_search(meta.algos_used.begin(), meta.algos_used.end(), extra));
    meta.algos_used.push_back(extra);
    std::sort(meta.algos_used.begin(), meta.algos_used.end());
    put(*f.backend, kLayout.poolMetaKey(), encodePoolMeta(meta));

    EXPECT_EQ(reacquireUntilSettled(*f.store), ReacquireOutcome::Open);
    EXPECT_EQ(f.store->writerEpoch(), f.epoch + 1);
}
