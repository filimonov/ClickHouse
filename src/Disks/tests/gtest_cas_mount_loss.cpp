#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasMountLoss.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasMountReacquire.h>
#include <Disks/tests/cas_test_helpers.h>

#include <limits>
#include <stdexcept>

using namespace DB::Cas;

namespace
{

MountLease ownBody(uint64_t seq)
{
    return MountLease{
        .server_uuid = DB::UInt128{7},
        .writer_epoch = 3,
        .hostname = "h",
        .pid = 11,
        .started_at_ms = 1000,
        .seq = seq,
        .expires_at_ms = 31000,
        .min_active_build_sequence = 5,
        .write_attempt_id = DB::UInt128{seq},
    };
}

MountLease gcFenceOf(MountLease m)
{
    m.gc_fenced = true;
    ++m.seq;
    return m;
}

/// One slot holding real bytes under a real ETag: every write goes through the request engine.
class Slot
{
public:
    Object put(const String & bytes)
    {
        OperationForTest op(*backend);
        if (const auto current = (*op).read("k", Retry::standard()))
            (void)(*op).replace("k", bytes, current->etag, Retry::standard());
        else
            (void)(*op).create("k", bytes, Retry::standard());
        auto got = (*op).read("k", Retry::standard());
        if (!got)
            throw std::runtime_error("the slot write did not land");
        return std::move(*got);
    }

    Object put(const MountLease & m) { return put(encodeMountLease(m)); }

private:
    std::shared_ptr<InMemoryBackend> backend = std::make_shared<InMemoryBackend>();
};

/// Our confirmed body `ownBody(1)` in the slot, and `ownBody(2)` in flight.
OwnMountBodies confirmedInSlot(Slot & slot)
{
    const Object confirmed = slot.put(ownBody(1));
    return OwnMountBodies{
        .server_uuid = DB::UInt128{7},
        .writer_epoch = 3,
        .confirmed = ownBody(1),
        .confirmed_bytes = confirmed.bytes,
        .confirmed_etag = confirmed.etag,
        .in_flight = ownBody(2),
    };
}

void expectReading(const MountSlotReading & reading, MountSlotVerdict verdict, const std::optional<MountLease> & holder)
{
    EXPECT_EQ(reading.verdict, verdict);
    ASSERT_EQ(reading.holder.has_value(), holder.has_value());
    if (holder)
        EXPECT_EQ(encodeMountLease(*reading.holder), encodeMountLease(*holder));
}

void expectVerdictFor(const MountLease & in_slot, MountSlotVerdict verdict)
{
    Slot slot;
    const OwnMountBodies own = confirmedInSlot(slot);
    expectReading(classifyMountSlot(slot.put(in_slot), own), verdict, in_slot);
}

}

TEST(CASMountLoss, OurConfirmedBytesUnderOurEtagAreOurs)
{
    Slot slot;
    const OwnMountBodies own = confirmedInSlot(slot);
    expectReading(classifyMountSlot(Object{own.confirmed_bytes, *own.confirmed_etag}, own),
                  MountSlotVerdict::Ours, ownBody(1));
}

TEST(CASMountLoss, OurConfirmedBytesWrittenAgainAreTaken)
{
    expectVerdictFor(ownBody(1), MountSlotVerdict::SlotTaken);
}

TEST(CASMountLoss, TheInFlightBodyIsTaken)
{
    expectVerdictFor(ownBody(2), MountSlotVerdict::SlotTaken);
}

TEST(CASMountLoss, AGcFenceOfTheConfirmedBodyIsAFence)
{
    expectVerdictFor(gcFenceOf(ownBody(1)), MountSlotVerdict::FencedByGc);
}

TEST(CASMountLoss, AGcFenceOfTheInFlightBodyIsAFence)
{
    expectVerdictFor(gcFenceOf(ownBody(2)), MountSlotVerdict::FencedByGc);
}

TEST(CASMountLoss, AFencedBodyOfOurPairAtAnUnknownSeqIsCorrupt)
{
    MountLease m = ownBody(1);
    m.gc_fenced = true;
    m.seq += 5;
    expectVerdictFor(m, MountSlotVerdict::SlotCorrupt);
}

TEST(CASMountLoss, OurPairFromAnotherHostIsCorrupt)
{
    MountLease m = ownBody(1);
    m.hostname = "elsewhere";
    expectVerdictFor(m, MountSlotVerdict::SlotCorrupt);
}

TEST(CASMountLoss, ANewerEpochOfOursIsTaken)
{
    MountLease m = ownBody(1);
    m.writer_epoch = 4;
    expectVerdictFor(m, MountSlotVerdict::SlotTaken);
}

TEST(CASMountLoss, AnOlderEpochOfOursIsTaken)
{
    MountLease m = ownBody(1);
    m.writer_epoch = 2;
    expectVerdictFor(m, MountSlotVerdict::SlotTaken);
}

TEST(CASMountLoss, AFencedNewerEpochOfOursIsTaken)
{
    MountLease m = gcFenceOf(ownBody(1));
    m.writer_epoch = 4;
    expectVerdictFor(m, MountSlotVerdict::SlotTaken);
}

TEST(CASMountLoss, AFarewellOfAnOlderEpochIsTaken)
{
    MountLease m = ownBody(1);
    m.writer_epoch = 2;
    m.min_active_build_sequence = std::numeric_limits<uint64_t>::max();
    expectVerdictFor(m, MountSlotVerdict::SlotTaken);
}

TEST(CASMountLoss, AForeignUuidIsTaken)
{
    MountLease m = ownBody(1);
    m.server_uuid = DB::UInt128{8};
    expectVerdictFor(m, MountSlotVerdict::SlotTaken);
}

TEST(CASMountLoss, UndecodableBytesAreCorruptWithoutAHolder)
{
    Slot slot;
    const OwnMountBodies own = confirmedInSlot(slot);
    expectReading(classifyMountSlot(slot.put(String("not a mount lease")), own), MountSlotVerdict::SlotCorrupt, std::nullopt);
}

TEST(CASMountLoss, AProvenAbsenceIsAbsent)
{
    Slot slot;
    expectReading(classifyMountSlot(ProvenAbsent{}, confirmedInSlot(slot)), MountSlotVerdict::SlotAbsent, std::nullopt);
}

TEST(CASMountLoss, NothingObservedIsNotObserved)
{
    Slot slot;
    expectReading(classifyMountSlot(NotObserved{}, confirmedInSlot(slot)), MountSlotVerdict::NotObserved, std::nullopt);
}

TEST(CASMountLoss, TheStopLinesNameWhatWasFound)
{
    MountLease holder = ownBody(1);
    holder.server_uuid = DB::UInt128{8};
    EXPECT_EQ(describeMountEnd(MountEnd::SlotTaken, MountLossReaction::StopServer, "cas1", "srv", holder, ""),
              "CAS disk 'cas1' (server_root_id=srv): the mount slot holds a body of another incarnation (hostname=h, "
              "pid=11, server_uuid=" + u128ToHex(DB::UInt128{8}) + ", writer_epoch=3, gc_fenced=false). New changes "
              "are closed; server stop requested. Every CAS disk of this server stops with it. Check for two servers "
              "sharing one server_root_id or two starts with one UUID.");
    EXPECT_EQ(describeMountEnd(MountEnd::SlotAbsent, MountLossReaction::CloseMount, "cas1", "srv", std::nullopt, ""),
              "CAS disk 'cas1' (server_root_id=srv): the mount slot is absent; a slot is deleted by SYSTEM CAS DROP "
              "POOL MEMBER or by hand. New changes are closed; this disk stays closed until restart.");
}

TEST(CASMountLoss, AnEndWithoutDetailNamesItself)
{
    EXPECT_EQ(describeMountEnd(MountEnd::LedgerFault, MountLossReaction::CloseMount, "cas1", "srv", std::nullopt, ""),
              "CAS disk 'cas1' (server_root_id=srv): the mount ended (ledger_fault). New changes are closed; this disk "
              "stays closed until restart.");
}

TEST(CASMountLoss, AFenceVerdictCarriesTheFencedObject)
{
    Slot slot;
    const OwnMountBodies own = confirmedInSlot(slot);
    const Object fenced = slot.put(gcFenceOf(ownBody(1)));

    const MountSlotReading reading = classifyMountSlot(fenced, own);

    ASSERT_EQ(reading.verdict, MountSlotVerdict::FencedByGc);
    ASSERT_TRUE(reading.fenced.has_value());
    EXPECT_EQ(reading.fenced->bytes, fenced.bytes);
    EXPECT_EQ(reading.fenced->etag, fenced.etag);
}

namespace
{

/// The slot after the GC fenced `ownBody(1)`, and the certificate a reacquire holds for it.
ReacquireCertificate fenceTheConfirmedBody(Slot & slot)
{
    (void)confirmedInSlot(slot);
    return ReacquireCertificate{.fenced = slot.put(gcFenceOf(ownBody(1))), .body = gcFenceOf(ownBody(1))};
}

struct FencedSlot
{
    Slot slot;
    ReacquireCertificate certificate = fenceTheConfirmedBody(slot);
};

/// Our claim under the next epoch, one `seq` above the fenced body.
MountReacquireClaim claimOver(const ReacquireCertificate & certificate)
{
    MountLease body = ownBody(certificate.body.seq + 1);
    body.writer_epoch = certificate.body.writer_epoch + 1;
    body.write_attempt_id = DB::UInt128{99};
    return MountReacquireClaim{.body = body, .bytes = encodeMountLease(body), .start_boot_ms = 1};
}

void expectLoss(const ReacquireSlotReading & reading, MountSlotVerdict verdict, const std::optional<MountLease> & holder)
{
    ASSERT_EQ(reading.verdict, ReacquireSlotVerdict::Loss);
    expectReading(reading.loss, verdict, holder);
}

}

TEST(CASMountLoss, ReacquireSeesItsOwnClaimBytesAsApplied)
{
    FencedSlot fenced;
    const MountReacquireClaim claim = claimOver(fenced.certificate);
    EXPECT_EQ(classifyReacquireSlot(fenced.slot.put(claim.bytes), fenced.certificate, claim).verdict,
              ReacquireSlotVerdict::Applied);
}

TEST(CASMountLoss, ReacquireSeesTheCertifiedEtagAsCertified)
{
    FencedSlot fenced;
    EXPECT_EQ(classifyReacquireSlot(fenced.certificate.fenced, fenced.certificate, claimOver(fenced.certificate)).verdict,
              ReacquireSlotVerdict::Certified);
}

TEST(CASMountLoss, AFencedBodyOfThePairUnderAnotherEtagIsCorrupt)
{
    FencedSlot fenced;
    const Object again = fenced.slot.put(fenced.certificate.fenced.bytes);
    ASSERT_NE(again.etag, fenced.certificate.fenced.etag);
    expectLoss(classifyReacquireSlot(again, fenced.certificate, claimOver(fenced.certificate)),
               MountSlotVerdict::SlotCorrupt, fenced.certificate.body);
}

TEST(CASMountLoss, ACleanMarkerOfThePairIsCorrupt)
{
    FencedSlot fenced;
    MountLease clean = ownBody(3);
    clean.min_active_build_sequence = std::numeric_limits<uint64_t>::max();
    expectLoss(classifyReacquireSlot(fenced.slot.put(clean), fenced.certificate, claimOver(fenced.certificate)),
               MountSlotVerdict::SlotCorrupt, clean);
}

TEST(CASMountLoss, ATwinsCleanBodyAtANewerEpochIsTaken)
{
    FencedSlot fenced;
    MountLease twin = ownBody(3);
    twin.writer_epoch += 5;
    twin.min_active_build_sequence = std::numeric_limits<uint64_t>::max();
    expectLoss(classifyReacquireSlot(fenced.slot.put(twin), fenced.certificate, claimOver(fenced.certificate)),
               MountSlotVerdict::SlotTaken, twin);
}

TEST(CASMountLoss, AForeignBodyIsTaken)
{
    FencedSlot fenced;
    MountLease foreign = ownBody(3);
    foreign.server_uuid = DB::UInt128{8};
    expectLoss(classifyReacquireSlot(fenced.slot.put(foreign), fenced.certificate, claimOver(fenced.certificate)),
               MountSlotVerdict::SlotTaken, foreign);
}

TEST(CASMountLoss, AGcFenceOfOurClaimIsANewFence)
{
    FencedSlot fenced;
    const MountReacquireClaim claim = claimOver(fenced.certificate);
    const Object fenced_claim = fenced.slot.put(gcFenceOf(claim.body));

    const ReacquireSlotReading reading = classifyReacquireSlot(fenced_claim, fenced.certificate, claim);

    ASSERT_EQ(reading.verdict, ReacquireSlotVerdict::NewFence);
    ASSERT_TRUE(reading.new_fence.has_value());
    EXPECT_EQ(reading.new_fence->fenced.etag, fenced_claim.etag);
    EXPECT_EQ(encodeMountLease(reading.new_fence->body), encodeMountLease(gcFenceOf(claim.body)));
}

TEST(CASMountLoss, ProvenAbsentIsAbsent)
{
    FencedSlot fenced;
    expectLoss(classifyReacquireSlot(ProvenAbsent{}, fenced.certificate, claimOver(fenced.certificate)),
               MountSlotVerdict::SlotAbsent, std::nullopt);
    EXPECT_EQ(classifyReacquireSlot(NotObserved{}, fenced.certificate, std::nullopt).verdict, ReacquireSlotVerdict::NotObserved);
}

TEST(CASMountLoss, TheIdentityAndCounterLinesNameWhatWasFound)
{
    EXPECT_EQ(describeMountEnd(MountEnd::PoolIdentityLost, MountLossReaction::StopServer, "cas1", "srv", std::nullopt,
                               "the owner anchor is absent and a LIST agrees"),
              "CAS disk 'cas1' (server_root_id=srv): the pool identity is lost (the owner anchor is absent and a LIST "
              "agrees). New changes are closed; server stop requested. Every CAS disk of this server stops with it.");
    EXPECT_EQ(describeMountEnd(MountEnd::SlotTaken, MountLossReaction::StopServer, "cas1", "srv", std::nullopt,
                               "the writer epoch counter was advanced by another process (read next_writer_epoch 4, found 5)"),
              "CAS disk 'cas1' (server_root_id=srv): the writer epoch counter was advanced by another process (read "
              "next_writer_epoch 4, found 5). New changes are closed; server stop requested. Every CAS disk of this "
              "server stops with it. Check for two servers sharing one server_root_id or two starts with one UUID.");
}
