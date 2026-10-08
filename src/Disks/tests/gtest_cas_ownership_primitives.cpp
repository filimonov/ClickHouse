#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCatalogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasServerRootFormats.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Poco/Exception.h>

namespace DB::ErrorCodes { extern const int CORRUPTED_DATA; }
using namespace DB::Cas;
using DB::Cas::tests::OperationForTest;

namespace
{
const String kSrid = "test";
const Layout kLayout("p");
const UInt128 kUuid(7);
using RawConflict = Backend::RawConflict;
using WriteAnswer = std::expected<String, RawConflict>;

PoolConfig config()
{
    PoolConfig c{.pool_prefix = "p", .server_root_id = kSrid};
    c.server_id = kUuid;
    c.retry_sleep_fn = [](uint64_t) {};
    return c;
}
std::optional<Object> get(Backend & b, const String & key) { OperationForTest op(b); return (*op).read(key, Retry::standard()); }
uint64_t counterNext(Backend & b) { return decodeServerEpoch(get(b, kLayout.epochKey(kSrid))->bytes).next_writer_epoch; }
void put(Backend & b, const String & key, const String & bytes)
{
    OperationForTest op(b);
    const auto got = (*op).read(key, Retry::standard());
    const WriteResult r = got ? (*op).replace(key, bytes, got->etag, Retry::standard()) : (*op).create(key, bytes, Retry::standard());
    ASSERT_TRUE(std::holds_alternative<Committed>(r)) << key;
}
void erase(Backend & b, const String & key) { OperationForTest op(b); ASSERT_EQ((*op).removeCurrent(key, Retry::standard()), Removal::Removed) << key; }
String ambiguousCatalog()
{
    const auto entry = [](const char * ns) { return CatalogEntry{.ns = RootNamespace{ns}, .state = NsState::Live, .incarnation = UInt128(42)}; };
    return encodeRefCatalog(RefCatalog{.entries = {entry("a"), entry("b")}});
}

/// Runs `hook` instead of the first write of the epoch counter after it is armed.
class EpochWriteHook : public InMemoryBackend
{
public:
    using Hook = std::function<WriteAnswer(InMemoryBackend &, const String &, const std::optional<String> &, TransportAccess &)>;
    WriteAnswer write(const String & key, const String & bytes, const std::optional<String> & expected, TransportAccess & access) override
    {
        if (!hook || key != kLayout.epochKey(kSrid))
            return InMemoryBackend::write(key, bytes, expected, access);
        return std::exchange(hook, {})(*this, bytes, expected, access);
    }
    Hook hook;
};
/// Another process that read the same value writes our very bytes first; our write is refused.
WriteAnswer raceHook(InMemoryBackend & b, const String & bytes, const std::optional<String> & expected, TransportAccess & access)
{
    b.InMemoryBackend::write(kLayout.epochKey(kSrid), bytes, expected, access);
    return std::unexpected(RawConflict{});
}
/// The write lands and its answer is lost.
WriteAnswer lostAnswerHook(InMemoryBackend & b, const String & bytes, const std::optional<String> & expected, TransportAccess & access)
{
    if (auto landed = b.InMemoryBackend::write(kLayout.epochKey(kSrid), bytes, expected, access); !landed)
        return landed;
    throw Poco::TimeoutException("injected: answer lost");
}
/// A GC fences the epoch this write allocates before our claim reaches the empty slot.
WriteAnswer fenceBeforeClaimHook(InMemoryBackend & b, const String & bytes, const std::optional<String> & expected, TransportAccess & access)
{
    WriteAnswer landed = b.InMemoryBackend::write(kLayout.epochKey(kSrid), bytes, expected, access);
    MountLease fenced;
    fenced.server_uuid = kUuid;
    fenced.writer_epoch = decodeServerEpoch(bytes).next_writer_epoch - 1;
    fenced.gc_fenced = true;
    fenced.write_attempt_id = UInt128(1);
    b.InMemoryBackend::write(kLayout.mountKey(kSrid), encodeMountLease(fenced), std::nullopt, access);
    return landed;
}
/// A server root that ran once and closed: owner, counter `{2}`, a farewell body in the slot.
std::shared_ptr<EpochWriteHook> ranOnce() { auto b = std::make_shared<EpochWriteHook>(); Pool::open(b, config()).reset(); return b; }
std::shared_ptr<EpochWriteHook> seeded() { auto b = std::make_shared<EpochWriteHook>(); DB::Cas::tests::seedPoolMetaForRestart(*b); return b; }
void expectStartRefused(const BackendPtr & b, const String & text)
{
    DB::Cas::tests::expectThrowsCodeWithMessage(DB::ErrorCodes::CORRUPTED_DATA, text, [&] { Pool::open(b, config()); });
}

/// An open-fence operation whose sleeps advance its own clock, so a bounded retry ends at once.
struct Plane
{
    explicit Plane(BackendPtr b) : requests(DB::Cas::tests::openRequestsForTest(std::move(b))), op(requests.admit())
    {
        requests.setNowFnForTest([this] { return clock; });
        requests.setSleepFnForTest([this](uint64_t ms) { clock += ms; });
    }
    uint64_t clock = 0;
    CasRequests requests;
    CasOperation op;
};
const ObserveRefCatalog kEmpty = [] { return RefCatalog{}; };
class FailingCatalogReads : public InMemoryBackend
{
public:
    using Backend::read;
    std::optional<Raw> read(const String & key, TransportAccess & access) override
    {
        if (fail && key == kLayout.refCatalogKey())
            throw Poco::TimeoutException("injected");
        return InMemoryBackend::read(key, access);
    }
    bool fail = false;
};
}

TEST(CASOwnershipStartGolden, FreshRootCreatesOwnerAndCounter)
{
    auto b = std::make_shared<EpochWriteHook>();
    EXPECT_EQ(Pool::open(b, config())->writerEpoch(), 1u);
    EXPECT_EQ(decodeOwner(get(*b, kLayout.ownerKey(kSrid))->bytes).server_uuid, kUuid);
    EXPECT_EQ(counterNext(*b), 2u);
}
TEST(CASOwnershipStartGolden, RestartKeepsOwnerAndTakesTheNextEpoch)
{
    auto b = ranOnce();
    const auto owner = get(*b, kLayout.ownerKey(kSrid));
    EXPECT_EQ(Pool::open(b, config())->writerEpoch(), 2u);
    EXPECT_EQ(get(*b, kLayout.ownerKey(kSrid))->etag, owner->etag);
    EXPECT_EQ(counterNext(*b), 3u);
}
TEST(CASOwnershipStartGolden, ForeignOrTombstonedOwnerRefusesWithoutCounterWrite)
{
    for (const auto & [owner, text] : {std::pair{OwnerObject{.server_uuid = UInt128(99), .retired_at_ms = std::nullopt}, String("owned by a different server")},
                                       std::pair{OwnerObject{.server_uuid = kUuid, .retired_at_ms = 1752537600000ULL}, String("decommissioned")}})
    {
        auto b = seeded();
        put(*b, kLayout.ownerKey(kSrid), encodeOwner(owner));
        expectStartRefused(b, text);
        EXPECT_FALSE(get(*b, kLayout.epochKey(kSrid)));
    }
}
TEST(CASOwnershipStartGolden, OwnerAbsentOverDataRefusesWithoutCreatingIt)
{
    auto b = seeded();
    put(*b, kLayout.casManifestsServerPrefix(kSrid) + "t/debris", "x");
    expectStartRefused(b, "identity lost over existing data");
    EXPECT_FALSE(get(*b, kLayout.ownerKey(kSrid)));
}
TEST(CASOwnershipStartGolden, CounterAbsentUnderASlotBodyRefuses)
{
    auto b = ranOnce();
    erase(*b, kLayout.epochKey(kSrid));
    expectStartRefused(b, "refusing to re-mint epoch 1");
    EXPECT_FALSE(get(*b, kLayout.epochKey(kSrid)));
}
TEST(CASOwnershipStartGolden, CounterAndSlotAbsentOverAnEmptyRootMintOne)
{
    auto b = ranOnce();
    erase(*b, kLayout.epochKey(kSrid));
    erase(*b, kLayout.mountKey(kSrid));
    EXPECT_EQ(Pool::open(b, config())->writerEpoch(), 1u);
    EXPECT_EQ(counterNext(*b), 2u);
}
TEST(CASOwnershipStartGolden, AmbiguousCatalogRefusesBeforeAnyOwnershipWrite)
{
    auto b = seeded();
    put(*b, kLayout.refCatalogKey(), ambiguousCatalog());
    expectStartRefused(b, "refusing a decision from an ambiguous cut");
    EXPECT_FALSE(get(*b, kLayout.ownerKey(kSrid)));
    EXPECT_FALSE(get(*b, kLayout.epochKey(kSrid)));
}
TEST(CASOwnershipStartGolden, ForeignAdvanceAtStartupRepeatsWithTheNextValue)
{
    auto b = ranOnce();
    b->hook = raceHook;
    EXPECT_EQ(Pool::open(b, config())->writerEpoch(), 3u);   /// the other process took 2
    EXPECT_EQ(counterNext(*b), 4u);
    EXPECT_FALSE(b->hook) << "the injected write ran";
}
TEST(CASOwnershipStartGolden, LostAnswerIsResolvedByOurBytesWithoutASecondNumber)
{
    auto b = ranOnce();
    b->hook = lostAnswerHook;
    EXPECT_EQ(Pool::open(b, config())->writerEpoch(), 2u);
    EXPECT_EQ(counterNext(*b), 3u);
    EXPECT_FALSE(b->hook) << "the injected write ran";
}
TEST(CASOwnershipStartGolden, OwnEpochFencedBeforeClaimTakesAFreshOne)
{
    auto b = std::make_shared<EpochWriteHook>();
    b->hook = fenceBeforeClaimHook;
    EXPECT_EQ(Pool::open(b, config())->writerEpoch(), 2u);
    EXPECT_EQ(counterNext(*b), 3u);
    EXPECT_FALSE(b->hook) << "the injected write ran";
}

TEST(CASOwnershipPrimitives, CatalogObservationIsTyped)
{
    auto b = std::make_shared<FailingCatalogReads>();
    Plane p(b);
    const auto cause = [&] { return std::get<OwnershipCorrupt>(CasRefCatalog::observe(p.op, kLayout, "t")).cause; };
    EXPECT_EQ(cause(), OwnershipCorrupt::Cause::Absent);
    put(*b, kLayout.refCatalogKey(), "garbage");
    EXPECT_EQ(cause(), OwnershipCorrupt::Cause::Undecodable);
    put(*b, kLayout.refCatalogKey(), ambiguousCatalog());
    EXPECT_EQ(cause(), OwnershipCorrupt::Cause::Ambiguous);
    put(*b, kLayout.refCatalogKey(), encodeRefCatalog(RefCatalog{}));
    EXPECT_TRUE(std::holds_alternative<RefCatalog>(CasRefCatalog::observe(p.op, kLayout, "t")));
    b->fail = true;
    EXPECT_TRUE(std::holds_alternative<OwnershipTransient>(CasRefCatalog::observe(p.op, kLayout, "t")));
}
TEST(CASOwnershipPrimitives, OwnerOutcomesAreTyped)
{
    auto b = std::make_shared<InMemoryBackend>();
    Plane p(b);
    const auto claim = [&](OwnershipAuthority a) { return claimOwner(p.op, kLayout, kSrid, kUuid, a, kEmpty); };
    const auto why = [](OwnerClaim c) { return std::get<OwnerLost>(c).why; };
    put(*b, kLayout.ownerKey(kSrid), encodeOwner(OwnerObject{.server_uuid = UInt128(99), .retired_at_ms = std::nullopt}));
    EXPECT_EQ(why(claim(OwnershipAuthority::Startup)), OwnerLost::Why::Foreign);
    put(*b, kLayout.ownerKey(kSrid), encodeOwner(OwnerObject{.server_uuid = kUuid, .retired_at_ms = 1}));
    EXPECT_EQ(why(claim(OwnershipAuthority::Reacquire)), OwnerLost::Why::Tombstoned);
    put(*b, kLayout.ownerKey(kSrid), "garbage");
    EXPECT_TRUE(std::holds_alternative<OwnershipCorrupt>(claim(OwnershipAuthority::Startup)));
    erase(*b, kLayout.ownerKey(kSrid));
    EXPECT_EQ(why(claim(OwnershipAuthority::Reacquire)), OwnerLost::Why::Absent);
    EXPECT_FALSE(get(*b, kLayout.ownerKey(kSrid))) << "a reacquire creates no owner anchor";
    EXPECT_TRUE(std::holds_alternative<OwnerHeld>(claim(OwnershipAuthority::Startup)));
    EXPECT_TRUE(get(*b, kLayout.ownerKey(kSrid)));
}
