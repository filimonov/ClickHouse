#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasWriteResult.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasServerRootFormats.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasMountLoss.h>
#include <base/types.h>
#include <cstdint>
#include <optional>

namespace DB::Cas
{

/// Our own body as the GC fenced it, as read: its ETag is the only ground on which a reacquire replaces it.
struct ReacquireCertificate
{
    Object fenced;
    MountLease body;
};

/// The reacquire's claim, fixed before its first request so every retry sends the same bytes under the
/// certificate's ETag; `start_boot_ms` is taken before the body is built, as at startup.
struct MountReacquireClaim
{
    MountLease body;
    String bytes;
    uint64_t start_boot_ms = 0;
};

/// `Applied`: our claim bytes are in the slot. `Certified`: the certified fenced body still is.
/// `NewFence`: the GC fenced our claim body. `Loss`: a loss row, in `loss`.
enum class ReacquireSlotVerdict : uint8_t
{
    Applied,
    Certified,
    NewFence,
    Loss,
    NotObserved,
};

struct ReacquireSlotReading
{
    ReacquireSlotVerdict verdict = ReacquireSlotVerdict::NotObserved;
    MountSlotReading loss = {};
    std::optional<ReacquireCertificate> new_fence = std::nullopt;
};

/// R20's order on one observation: our claim bytes, the certificate's ETag, then the loss table. A body of
/// the fenced pair under another ETag is corrupt even when it looks like a GC fence: a fence never repeats.
ReacquireSlotReading classifyReacquireSlot(const Observation & seen, const ReacquireCertificate & certificate,
                                           const std::optional<MountReacquireClaim> & claim);

}
