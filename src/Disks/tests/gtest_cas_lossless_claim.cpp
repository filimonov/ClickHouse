#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasServerRootFormats.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Poco/Exception.h>

namespace DB::ErrorCodes { extern const int ABORTED; }
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
/// What a GC fence-out does: keep the body, set `gc_fenced`, bump `seq`.
void fenceOut(Backend & b) { MountLease m = slot(b); m.gc_fenced = true; ++m.seq; put(b, kLayout.mountKey(kSrid), encodeMountLease(m)); }
/// Counts writes of the mount slot; `stall_ms` moves the boot clock once, before the next one lands;
/// `hook` replaces the next one.
class MountWriteClock : public InMemoryBackend
{
public:
    using Hook = std::function<WriteAnswer(InMemoryBackend &, const String &, const std::optional<String> &, TransportAccess &)>;
    std::shared_ptr<std::atomic<uint64_t>> boot = std::make_shared<std::atomic<uint64_t>>(10'000);
    std::atomic<int> mount_writes{0};
    uint64_t stall_ms = 0;
    Hook hook;

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
std::shared_ptr<MountWriteClock> fencedPredecessor()
{
    auto b = std::make_shared<MountWriteClock>();
    DB::Cas::tests::seedPoolMetaForRestart(*b, "pool");
    put(*b, kLayout.mountKey(kSrid), encodeMountLease(lease(kUuid, 7, 7, true)));
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

TEST(CASLosslessClaim, GoldenOpenAndRemountPaths)
{
    auto fresh = std::make_shared<MountWriteClock>();
    ASSERT_TRUE(Pool::open(fresh, poolConfig(fresh)));
    EXPECT_EQ(slot(*fresh).writer_epoch, 1u);
    auto b = fencedPredecessor();
    auto store = Pool::open(b, poolConfig(b));
    EXPECT_TRUE(slot(*b).writer_epoch == 8 && !slot(*b).gc_fenced && store->mayMutate());
    fenceOut(*b);
    ASSERT_TRUE(store->tryRemountOnce());
    EXPECT_TRUE(slot(*b).writer_epoch == 9 && store->mayMutate());
    store.reset();
    ASSERT_TRUE(Pool::open(b, poolConfig(b))) << "a clean farewell is reclaimed at once";
    EXPECT_EQ(slot(*b).writer_epoch, 10u);
}
