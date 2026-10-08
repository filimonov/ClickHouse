#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasWriteResult.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasServerRootFormats.h>
#include <base/types.h>
#include <cstdint>
#include <optional>
#include <string_view>

namespace DB::Cas
{

/// Why a CAS mount ended for good. The first end latched wins.
enum class MountEnd : uint8_t
{
    None,
    SlotTaken,
    SlotAbsent,
    SlotCorrupt,
    FencedByGc,    /// the decommission pool only: it impersonates a dead member and never remounts
    LedgerFault,
    LocalDefect,
    Replaced,      /// the data root holds another pool
    Forgotten,     /// `SYSTEM CAS FORGET`
};
std::string_view mountEndWord(MountEnd end);   /// "slot_taken", …, "ledger_fault", "local_defect", "forgotten"
bool isLossEnd(MountEnd end);                  /// every end but `None`, `Replaced`, `Forgotten`
/// A broken process invariant: a shutdown would write under an identity another process may hold.
bool endsWithoutShutdown(MountEnd end);        /// `LedgerFault`, `LocalDefect`

/// `StopServer`: a writable mount of this server's own slot. `CloseMount`: writable without background
/// renewal (tools). `FailDecommission`: `Pool::openForDecommission`, which never remounts.
enum class MountLossReaction : uint8_t
{
    StopServer,
    CloseMount,
    FailDecommission,
};

enum class MountSlotVerdict : uint8_t
{
    Ours,
    FencedByGc,
    SlotTaken,
    SlotAbsent,
    SlotCorrupt,
    NotObserved,
};

/// The two bodies this server can have in its slot: the last confirmed one and the renewal in flight.
struct OwnMountBodies
{
    UInt128 server_uuid{};
    uint64_t writer_epoch = 0;
    std::optional<MountLease> confirmed = std::nullopt;
    String confirmed_bytes = {};
    std::optional<Etag> confirmed_etag = std::nullopt;
    std::optional<MountLease> in_flight = std::nullopt;
};

struct MountSlotReading
{
    MountSlotVerdict verdict = MountSlotVerdict::NotObserved;
    std::optional<MountLease> holder;
};

/// Total: one verdict for every observation. Bodies are compared decoded, not as bytes.
MountSlotReading classifyMountSlot(const Observation & seen, const OwnMountBodies & own);

/// ProvenAbsent only when a LIST of key as a prefix agrees: a proxy or a misrouted request answers a GET
/// with 404 too. A failed LIST, or one that lists the key, is NotObserved; any other observation is
/// returned unchanged.
Observation corroborateAbsence(CasOperation & op, const String & key, Observation seen);

String describeMountEnd(MountEnd end, MountLossReaction reaction, const String & disk_name,
                        const String & server_root_id, const std::optional<MountLease> & holder, std::string_view detail);

}
