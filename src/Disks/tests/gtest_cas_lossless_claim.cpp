#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasServerRootFormats.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasMountReacquire.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Poco/Exception.h>

#include "config.h"
#include <IO/S3Common.h>

namespace DB::ErrorCodes { extern const int ABORTED; extern const int NETWORK_ERROR; }
using namespace DB::Cas;
using DB::Cas::tests::OperationForTest;

namespace
{
const String kSrid = "s";
const Layout kLayout("pool");
const UInt128 kUuid(0x42);
using RawConflict = Backend::RawConflict;
using WriteAnswer = std::expected<String, RawConflict>;

std::optional<Object> get(Backend & b) { OperationForTest op(b); return (*op).read(kLayout.mountKey(kSrid), Retry::standard()); }
void put(Backend & b, const String & key, const String & bytes)
{
    OperationForTest op(b);
    const auto got = (*op).read(key, Retry::standard());
    ASSERT_TRUE(std::holds_alternative<Committed>(
        got ? (*op).replace(key, bytes, got->etag, Retry::standard()) : (*op).create(key, bytes, Retry::standard())));
}
MountLease slot(Backend & b) { return decodeMountLease(get(b)->bytes); }
MountLease lease(UInt128 uuid, uint64_t epoch, uint64_t seq, bool fenced, uint64_t min_active = 0)
{
    return {.server_uuid = uuid, .writer_epoch = epoch, .hostname = "h", .pid = 1, .started_at_ms = 0, .seq = seq,
            .expires_at_ms = 1, .min_active_build_sequence = min_active, .gc_fenced = fenced,
            .write_attempt_id = UInt128(100 + seq)};
}
/// Counts writes of the mount slot; `stall_ms` moves the boot clock once, before the next one lands;
/// `hook` replaces the next one. While `refuse_mount_reads` is positive, a read of the slot is denied.
class MountWriteClock : public InMemoryBackend
{
public:
    using Hook = std::function<WriteAnswer(InMemoryBackend &, const String &, const std::optional<String> &, TransportAccess &)>;
    std::shared_ptr<std::atomic<uint64_t>> boot = std::make_shared<std::atomic<uint64_t>>(10'000);
    std::atomic<int> mount_writes{0};
    uint64_t stall_ms = 0;
    Hook hook;
    uint32_t refuse_mount_reads = 0;

    std::optional<Raw> read(const String & key, TransportAccess & access) override
    {
        if (key == kLayout.mountKey(kSrid) && refuse_mount_reads > 0)
        {
            --refuse_mount_reads;
#if USE_AWS_S3
            throw DB::S3Exception("injected read access denial", Aws::S3::S3Errors::ACCESS_DENIED);
#else
            throw DB::Exception(DB::ErrorCodes::ABORTED, "store refusals need the S3 client");
#endif
        }
        return InMemoryBackend::read(key, access);
    }

    WriteAnswer write(const String & key, const String & bytes, const std::optional<String> & expected, TransportAccess & access) override
    {
        if (key == kLayout.mountKey(kSrid))
        {
            ++mount_writes;
            *boot += std::exchange(stall_ms, 0);
            if (hook)
                return std::exchange(hook, {})(*this, bytes, expected, access);
        }
        return InMemoryBackend::write(key, bytes, expected, access);
    }
};
PoolConfig poolConfig(const std::shared_ptr<MountWriteClock> & b)
{
    PoolConfig c{.pool_prefix = "pool", .server_root_id = kSrid};
    c.server_id = kUuid;
    c.mount_lease_ttl_ms = std::chrono::milliseconds(30'000);
    c.retry_sleep_fn = [](uint64_t) {};
    c.boot_ms_fn = [boot = b->boot] { return boot->load(); };
    c.wait_sleep_fn = [boot = b->boot](uint64_t ms) { *boot += ms; };
    return c;
}
/// Epoch 7 (seq 7) was fenced by GC: a start allocates epoch 8 and reclaims at once with seq 8.
/// `occupant_epoch = 8` instead leaves a fenced body of the epoch the start allocates.
std::shared_ptr<MountWriteClock> fencedPredecessor(uint64_t occupant_epoch = 7)
{
    auto b = std::make_shared<MountWriteClock>();
    DB::Cas::tests::seedPoolMetaForRestart(*b, "pool");
    put(*b, kLayout.mountKey(kSrid), encodeMountLease(lease(kUuid, occupant_epoch, 7, true)));
    put(*b, kLayout.epochKey(kSrid), encodeServerEpoch(ServerEpoch{.next_writer_epoch = 8}));
    b->mount_writes = 0;
    return b;
}
MountClaimResult claim(Backend & b) { OperationForTest op(b); return claimMount(*op, kLayout, kSrid, kUuid, 8, 1000, 30'000); }
}

TEST(CASLosslessClaim, GoldenClaimOutcomeByOccupant)
{
    using K = MountClaimResult;
    using P = MountPriorState;
    struct Case { const char * name; std::optional<MountLease> occupant; K::Kind kind; P prior; };
    const Case cases[] = {
        {"absent", std::nullopt, K::Claimed, P::None},
        {"foreign", lease(UInt128(9), 1, 3, false), K::ForeignOwner, P::None},
        {"own pair live", lease(kUuid, 8, 3, false), K::LiveDoubleStart, P::None},
        {"own pair fenced", lease(kUuid, 8, 3, true), K::FencedSelf, P::None},
        {"older fenced", lease(kUuid, 7, 3, true), K::Claimed, P::Fenced},
        {"older clean", lease(kUuid, 7, 3, false, std::numeric_limits<uint64_t>::max()), K::Claimed, P::Clean},
        {"older live", lease(kUuid, 7, 3, false), K::LiveDoubleStart, P::None},
    };
    for (const Case & c : cases)
    {
        auto b = std::make_shared<InMemoryBackend>();
        if (c.occupant)
            put(*b, kLayout.mountKey(kSrid), encodeMountLease(*c.occupant));
        const std::optional<Object> before = get(*b);
        const MountClaimResult r = claim(*b);
        EXPECT_EQ(r.kind, c.kind) << c.name;
        EXPECT_EQ(r.prior, c.prior) << c.name;
        if (c.kind != K::Claimed)
        {
            EXPECT_EQ(get(*b)->bytes, before->bytes) << c.name;
            continue;
        }
        const MountLease m = slot(*b);
        EXPECT_TRUE(m.server_uuid == kUuid && m.writer_epoch == 8 && !m.gc_fenced) << c.name;
        EXPECT_EQ(m.seq, c.occupant ? c.occupant->seq + 1 : 1u) << c.name;
    }
    auto b = std::make_shared<InMemoryBackend>();
    put(*b, kLayout.mountKey(kSrid), encodeMountLease(lease(kUuid, 9, 3, false)));
    DB::Cas::tests::expectThrowsCodeWithMessage(DB::ErrorCodes::ABORTED, "newer than the writer_epoch", [&] { claim(*b); });
}

TEST(CASLosslessClaim, GoldenOpenPaths)
{
    auto fresh = std::make_shared<MountWriteClock>();
    ASSERT_TRUE(Pool::open(fresh, poolConfig(fresh)));
    EXPECT_EQ(slot(*fresh).writer_epoch, 1u);
    auto b = fencedPredecessor();
    auto store = Pool::open(b, poolConfig(b));
    EXPECT_TRUE(slot(*b).writer_epoch == 8 && !slot(*b).gc_fenced && store->mayMutate());
    store.reset();
    ASSERT_TRUE(Pool::open(b, poolConfig(b))) << "a clean farewell is reclaimed at once";
    EXPECT_EQ(slot(*b).writer_epoch, 9u);
}

TEST(CASLosslessClaim, ClaimedCarriesTheLandedBytesTheirEtagAndTheStart)
{
    auto b = std::make_shared<MountWriteClock>();
    b->stall_ms = 1'000;
    OperationForTest op(*b);
    const MountClaimResult r = claimMount(*op, kLayout, kSrid, kUuid, 8, 1000, 30'000, {}, {}, {}, [&] { return b->boot->load(); });
    ASSERT_EQ(r.kind, MountClaimResult::Claimed);
    EXPECT_EQ(r.claimed->bytes, get(*b)->bytes);
    EXPECT_EQ(r.claimed->etag, get(*b)->etag);
    EXPECT_EQ(r.claimed->start_boot_ms, 10'000u) << "taken before the body is built, not when the write returns";
    EXPECT_EQ(b->boot->load(), 11'000u);
}

TEST(CASLosslessClaim, AnUnclearClaimThatLandedIsConfirmedByItsBytes)
{
    auto b = std::make_shared<MountWriteClock>();
    b->hook = [](InMemoryBackend & s, const String & bytes, const std::optional<String> & expected, TransportAccess & access) -> WriteAnswer
    {
        (void)s.InMemoryBackend::write(kLayout.mountKey(kSrid), bytes, expected, access);
        throw Poco::TimeoutException("injected: the claim landed and its answer was lost");
    };
    const MountClaimResult r = claim(*b);
    ASSERT_EQ(r.kind, MountClaimResult::Claimed);
    EXPECT_EQ(r.claimed->etag, get(*b)->etag);
    EXPECT_EQ(b->mount_writes.load(), 1) << "confirmed by reading our bytes, not by writing again";
}

TEST(CASLosslessClaim, AnotherBodyOfOurPairAfterAnUnclearClaimIsReportedWithItsBytes)
{
    auto b = std::make_shared<MountWriteClock>();
    const String other = encodeMountLease(lease(kUuid, 8, 1, false));
    b->hook = [other](InMemoryBackend & s, const String &, const std::optional<String> &, TransportAccess & access) -> WriteAnswer
    {
        (void)s.InMemoryBackend::write(kLayout.mountKey(kSrid), other, std::nullopt, access);
        throw Poco::TimeoutException("injected: another process of our pair landed first");
    };
    const MountClaimResult r = claim(*b);
    EXPECT_EQ(r.kind, MountClaimResult::LiveDoubleStart);
    ASSERT_TRUE(std::holds_alternative<Object>(r.seen));
    EXPECT_EQ(std::get<Object>(r.seen).bytes, other);
}

TEST(CASLosslessClaim, ARefusedClaimOverAnEmptySlotReportsTheAbsence)
{
    auto b = std::make_shared<MountWriteClock>();
    b->hook = [](InMemoryBackend &, const String &, const std::optional<String> &, TransportAccess &) -> WriteAnswer
    { return std::unexpected(RawConflict{}); };
    const MountClaimResult r = claim(*b);
    EXPECT_EQ(r.kind, MountClaimResult::LiveDoubleStart);
    EXPECT_TRUE(std::holds_alternative<ProvenAbsent>(r.seen)) << "absence is not 'nothing observed'";
}

TEST(CASLosslessClaim, StartWritesTheSlotOnceAndRunsTheLeaseFromTheClaim)
{
    auto b = fencedPredecessor();
    b->stall_ms = 1'000;
    auto store = Pool::open(b, poolConfig(b));
    EXPECT_EQ(b->mount_writes.load(), 1) << "no second write after the claim";
    EXPECT_EQ(slot(*b).seq, 8u);
    b->boot->store(39'999);
    EXPECT_TRUE(store->mayMutate());
    b->boot->store(40'000);
    EXPECT_FALSE(store->mayMutate()) << "the deadline is the claim's start (10 000) plus the lease";
}

TEST(CASLosslessClaim, ALateClaimRenewsBeforeWritesOpenAndATooLateOneFailsTheStart)
{
    auto late = fencedPredecessor();
    late->stall_ms = 18'000;   /// 10 s of the lease-safe window left: below the publication horizon
    auto store = Pool::open(late, poolConfig(late));
    EXPECT_EQ(late->mount_writes.load(), 2);
    EXPECT_EQ(slot(*late).seq, 9u) << "the second write renews the claim";
    auto too_late = fencedPredecessor();
    too_late->stall_ms = 29'000;   /// confirmed at 39 000, after start + lease - margin = 38 000
    DB::Cas::tests::expectThrowsCodeWithMessage(DB::ErrorCodes::NETWORK_ERROR, "(deadline, no attempt sent",
                                                [&] { Pool::open(too_late, poolConfig(too_late)); });
    EXPECT_EQ(too_late->mount_writes.load(), 1) << "no renewal fits inside the claim's lease";
}

#if USE_AWS_S3
TEST(CASLosslessClaim, ARefusedClaimWhoseReadFailsReportsNothingObserved)
{
    auto b = std::make_shared<MountWriteClock>();
    b->hook = [&b](InMemoryBackend &, const String &, const std::optional<String> &, TransportAccess &) -> WriteAnswer
    {
        b->refuse_mount_reads = 1;
        return std::unexpected(RawConflict{});
    };
    const MountClaimResult r = claim(*b);
    EXPECT_EQ(r.kind, MountClaimResult::LiveDoubleStart);
    EXPECT_TRUE(std::holds_alternative<NotObserved>(r.seen)) << "a failed resolve read proves no absence";
    EXPECT_EQ(b->refuse_mount_reads, 0u) << "the resolve read was never refused";
}
#endif

/// Another process was allocated our epoch and its body is fenced: the start takes the next epoch and
/// reclaims that body.
TEST(CASLosslessClaim, AFencedBodyOfOurEpochRecoversWithAFreshEpoch)
{
    auto b = fencedPredecessor(/*occupant_epoch=*/8);
    auto store = Pool::open(b, poolConfig(b));
    ASSERT_TRUE(store);
    EXPECT_EQ(slot(*b).writer_epoch, 9u);
    EXPECT_FALSE(slot(*b).gc_fenced);
    EXPECT_TRUE(store->mayMutate());
}

namespace
{

/// Epoch 7 fenced by GC in the slot, and the certificate a reacquire read for it.
ReacquireCertificate certifyFencedSeven(Backend & b)
{
    put(b, kLayout.mountKey(kSrid), encodeMountLease(lease(kUuid, 7, 7, true)));
    Object fenced = *get(b);
    MountLease body = decodeMountLease(fenced.bytes);
    return ReacquireCertificate{.fenced = std::move(fenced), .body = std::move(body)};
}

/// An open-fence operation admitted under a liveness the test turns off and on.
struct LiveOperation
{
    explicit LiveOperation(Backend & b)
        : requests(DB::Cas::tests::openRequestsForTest(b)), op(requests.admit([this] { return live; }))
    {
        requests.setSleepFnForTest([](uint64_t) {});
    }
    bool live = true;
    CasRequests requests;
    CasOperation op;
};

ReacquireClaimResult reacquire(Backend & b, const ReacquireCertificate & certificate, const MountReacquireClaim & claim)
{
    OperationForTest op(b);
    return claimMount(*op, kLayout, kSrid, certificate, claim);
}

void expectRefusedAndSlotHolds(const ReacquireClaimResult & result, Backend & b, const String & bytes)
{
    EXPECT_FALSE(result.claimed.has_value());
    EXPECT_FALSE(result.transient);
    ASSERT_TRUE(std::holds_alternative<Object>(result.seen));
    EXPECT_EQ(std::get<Object>(result.seen).bytes, bytes);
    EXPECT_EQ(get(b)->bytes, bytes) << "the slot is unchanged";
}

}

TEST(CASLosslessClaim, ReacquireReplacesTheCertifiedBody)
{
    auto b = std::make_shared<InMemoryBackend>();
    const ReacquireCertificate certificate = certifyFencedSeven(*b);
    const MountReacquireClaim claim = makeReacquireClaim(kUuid, 8, certificate, 1000, 30'000, 5);
    EXPECT_EQ(claim.body.seq, 8u);
    EXPECT_EQ(claim.body.writer_epoch, 8u);
    EXPECT_EQ(claim.start_boot_ms, 5u);

    const ReacquireClaimResult result = reacquire(*b, certificate, claim);

    ASSERT_TRUE(result.claimed.has_value());
    EXPECT_EQ(result.claimed->bytes, claim.bytes);
    EXPECT_EQ(result.claimed->start_boot_ms, 5u);
    EXPECT_EQ(get(*b)->bytes, claim.bytes);
    EXPECT_EQ(get(*b)->etag, result.claimed->etag);
}

TEST(CASLosslessClaim, ReacquireNeverCreatesAnAbsentSlot)
{
    auto b = std::make_shared<InMemoryBackend>();
    const ReacquireCertificate certificate = certifyFencedSeven(*b);
    {
        OperationForTest op(*b);
        ASSERT_EQ((*op).removeCurrent(kLayout.mountKey(kSrid), Retry::standard()), Removal::Removed);
    }

    const ReacquireClaimResult result = reacquire(*b, certificate, makeReacquireClaim(kUuid, 8, certificate, 1000, 30'000, 5));

    EXPECT_FALSE(result.claimed.has_value());
    EXPECT_FALSE(result.transient);
    EXPECT_TRUE(std::holds_alternative<ProvenAbsent>(result.seen));
    EXPECT_FALSE(get(*b).has_value()) << "a reacquire never creates the slot";
}

TEST(CASLosslessClaim, ReacquireNeverTakesACleanMarker)
{
    auto b = std::make_shared<InMemoryBackend>();
    const ReacquireCertificate certificate = certifyFencedSeven(*b);
    const String clean = encodeMountLease(lease(kUuid, 7, 8, false, std::numeric_limits<uint64_t>::max()));
    put(*b, kLayout.mountKey(kSrid), clean);

    expectRefusedAndSlotHolds(reacquire(*b, certificate, makeReacquireClaim(kUuid, 8, certificate, 1000, 30'000, 5)), *b, clean);
}

TEST(CASLosslessClaim, ReacquireNeverTakesAFencedBodyOfAnotherEpoch)
{
    auto b = std::make_shared<InMemoryBackend>();
    const ReacquireCertificate certificate = certifyFencedSeven(*b);
    const String other = encodeMountLease(lease(kUuid, 6, 9, true));
    put(*b, kLayout.mountKey(kSrid), other);

    expectRefusedAndSlotHolds(reacquire(*b, certificate, makeReacquireClaim(kUuid, 8, certificate, 1000, 30'000, 5)), *b, other);
}

TEST(CASLosslessClaim, ARetriedClaimFindsItsOwnLandedBytes)
{
    auto b = std::make_shared<MountWriteClock>();
    const ReacquireCertificate certificate = certifyFencedSeven(*b);
    const MountReacquireClaim claim = makeReacquireClaim(kUuid, 8, certificate, 1000, 30'000, 5);
    LiveOperation operation(*b);
    b->hook = [&operation](InMemoryBackend & backend, const String & bytes, const std::optional<String> & expected, TransportAccess & access) -> WriteAnswer
    {
        if (auto landed = backend.InMemoryBackend::write(kLayout.mountKey(kSrid), bytes, expected, access); !landed)
            return landed;
        operation.live = false;
        throw Poco::TimeoutException("injected: answer lost");
    };

    const ReacquireClaimResult first = claimMount(operation.op, kLayout, kSrid, certificate, claim);
    EXPECT_TRUE(first.transient);
    EXPECT_FALSE(first.claimed.has_value());

    operation.live = true;
    const ReacquireClaimResult second = claimMount(operation.op, kLayout, kSrid, certificate, claim);
    ASSERT_TRUE(second.claimed.has_value());
    EXPECT_EQ(second.claimed->bytes, claim.bytes);
    EXPECT_EQ(get(*b)->bytes, claim.bytes);
    EXPECT_EQ(get(*b)->etag, second.claimed->etag);
}
