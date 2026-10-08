#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasMountLoss.h>
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
