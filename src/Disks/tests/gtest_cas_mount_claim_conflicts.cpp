#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>
#include <Disks/tests/cas_test_helpers.h>

using namespace DB::Cas;
using DB::Cas::tests::MountSlotRaceBackend;
using DB::Cas::tests::OperationForTest;

namespace
{

void markMountGcFenced(CasOperation & op, const Layout & layout, const String & server_root_id)
{
    const String key = layout.mountKey(server_root_id);
    const auto got = op.read(key, Retry::standard());
    ASSERT_TRUE(got);
    MountLease lease = decodeMountLease(got->bytes);
    lease.gc_fenced = true;
    ASSERT_TRUE(std::holds_alternative<Committed>(
        op.replace(key, encodeMountLease(lease), got->etag, Retry::standard())));
}

}

/// A raced claim reports a body its caller renders into the fail-closed operator message. Reporting
/// the PROPOSER's own lease there names this very server as the existing mount, which sends an
/// operator hunting a second process that is not the one holding the slot. The write's own resolve
/// read already observed the occupant, so that is what the result must carry.
TEST(CASMountClaimConflicts, ALostCreateReportsTheOccupantNotTheProposer)
{
    auto backend = std::make_shared<MountSlotRaceBackend>();
    Layout layout("p");
    uint64_t now = 1000;
    CasRequests requests = openRequestsForTest(backend);
    /// Absent at our read; a foreign server mints the slot before our create lands.
    backend->before_put_if_absent = [&]
    {
        CasOperation racer = requests.admit();
        ASSERT_EQ(claimMount(racer, layout, "r", DB::UInt128(2), 1, now, /*ttl_ms=*/100).kind,
                  MountClaimResult::Claimed);
    };
    CasOperation op = requests.admit();
    const MountClaimResult claim = claimMount(op, layout, "r", DB::UInt128(1), 7, now, /*ttl_ms=*/100);

    EXPECT_EQ(claim.kind, MountClaimResult::LiveDoubleStart);
    const std::optional<MountLease> holder = observedLease(claim.seen);
    ASSERT_TRUE(holder.has_value()) << "the observed occupant's incarnation is what was read";
    EXPECT_EQ(holder->server_uuid, DB::UInt128(2)) << "the result named this server's own proposal";
    EXPECT_EQ(holder->writer_epoch, 1u);
    EXPECT_NE(mountDoubleStartMessage("r", holder).find(u128ToHex(DB::UInt128(2))), String::npos)
        << "the operator message must name the foreign holder";
}

/// The same for the reclaim branch: a body that changed under our own reclaim is the one the message must name.
TEST(CASMountClaimConflicts, ALostReclaimReportsTheObservedBodyNotTheProposer)
{
    auto backend = std::make_shared<MountSlotRaceBackend>();
    Layout layout("p");
    uint64_t now = 1000;
    CasRequests requests = openRequestsForTest(backend);
    CasOperation seed = requests.admit();
    ASSERT_EQ(claimMount(seed, layout, "r", DB::UInt128(1), 7, now, /*ttl_ms=*/100).kind,
              MountClaimResult::Claimed);
    markMountGcFenced(seed, layout, "r");
    /// A distinguishable body lands under our own reclaim, so a result carrying the proposal cannot
    /// pass by accident: our proposal would carry `seq` 2 and this process's pid.
    backend->before_put_overwrite = [&]
    {
        OperationForTest racer(*backend);
        const String key = layout.mountKey("r");
        const auto got = (*racer).read(key, Retry::standard());
        ASSERT_TRUE(got.has_value());
        MountLease raced = decodeMountLease(got->bytes);
        raced.pid = 4242;
        raced.seq = 99;
        ASSERT_TRUE(std::holds_alternative<Committed>(
            (*racer).replace(key, encodeMountLease(raced), got->etag, Retry::standard())));
    };
    CasOperation op = requests.admit();
    const MountClaimResult claim = claimMount(op, layout, "r", DB::UInt128(1), 8, now, /*ttl_ms=*/100);

    EXPECT_EQ(claim.kind, MountClaimResult::LiveDoubleStart);
    const std::optional<MountLease> holder = observedLease(claim.seen);
    ASSERT_TRUE(holder.has_value());
    EXPECT_EQ(holder->pid, 4242);
    EXPECT_EQ(holder->seq, 99u);
}

/// The residue of the same rule: a raced write whose resolve read proves the slot absent saw nobody, so
/// there is no holder to name. Reporting the lease this server merely PROPOSED would put this very
/// process in the "Existing mount" line of an operator message -- the same defect as naming it after a
/// conflict that did observe someone.
TEST(CASMountClaimConflicts, ARacedReclaimThatProvesTheSlotAbsentNamesNoHolder)
{
    auto backend = std::make_shared<MountSlotRaceBackend>();
    Layout layout("p");
    uint64_t now = 1000;
    CasRequests requests = openRequestsForTest(backend);
    CasOperation seed = requests.admit();
    ASSERT_EQ(claimMount(seed, layout, "r", DB::UInt128(1), 7, now, /*ttl_ms=*/100).kind,
              MountClaimResult::Claimed);
    markMountGcFenced(seed, layout, "r");
    /// The slot is removed under our own reclaim, so the refused precondition resolves to a proven
    /// absence rather than to an occupant.
    backend->before_put_overwrite = [&]
    {
        OperationForTest racer(*backend);
        const String key = layout.mountKey("r");
        const auto got = (*racer).read(key, Retry::standard());
        ASSERT_TRUE(got.has_value());
        ASSERT_EQ((*racer).remove(key, got->etag, Retry::standard()), Removal::Removed);
    };
    CasOperation op = requests.admit();
    const MountClaimResult claim = claimMount(op, layout, "r", DB::UInt128(1), 8, now, /*ttl_ms=*/100);

    EXPECT_EQ(claim.kind, MountClaimResult::LiveDoubleStart);
    EXPECT_TRUE(std::holds_alternative<ProvenAbsent>(claim.seen));
    EXPECT_FALSE(observedLease(claim.seen).has_value()) << "a result that observed nobody reported a lease anyway";
    const String message = mountDoubleStartMessage("r", observedLease(claim.seen));
    EXPECT_NE(message.find("could not be observed"), String::npos)
        << "the message named a holder nobody saw: " << message;
}
