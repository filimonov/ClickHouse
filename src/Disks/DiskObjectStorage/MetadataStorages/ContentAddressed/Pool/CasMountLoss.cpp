#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasMountLoss.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Common/Exception.h>
#include <fmt/format.h>
#include <tuple>

namespace DB::ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

namespace DB::Cas
{

namespace
{

auto allFields(const MountLease & m)
{
    return std::tie(m.server_uuid, m.writer_epoch, m.hostname, m.pid, m.started_at_ms, m.seq, m.expires_at_ms,
                    m.min_active_build_sequence, m.gc_fenced, m.write_attempt_id);
}

/// The body `computeHeartbeatFloor` writes when it fences `own` out.
bool isGcFenceOf(const MountLease & seen, const MountLease & own)
{
    if (!seen.gc_fenced || own.gc_fenced || seen.seq != own.seq + 1)
        return false;
    MountLease unfenced = seen;
    unfenced.gc_fenced = false;
    unfenced.seq = own.seq;
    return allFields(unfenced) == allFields(own);
}

String describeHolder(const std::optional<MountLease> & holder)
{
    if (!holder)
        return "no body was read";
    return fmt::format("hostname={}, pid={}, server_uuid={}, writer_epoch={}, gc_fenced={}",
                       holder->hostname, holder->pid, u128ToHex(holder->server_uuid), holder->writer_epoch,
                       holder->gc_fenced);
}

std::string_view reactionSuffix(MountEnd end, MountLossReaction reaction)
{
    switch (reaction)
    {
        case MountLossReaction::StopServer:
            return end == MountEnd::SlotTaken
                ? " New changes are closed; server stop requested. Every CAS disk of this server stops with it."
                  " Check for two servers sharing one server_root_id or two starts with one UUID."
                : " New changes are closed; server stop requested. Every CAS disk of this server stops with it.";
        case MountLossReaction::CloseMount:
            return " New changes are closed; this disk stays closed until restart.";
        case MountLossReaction::FailDecommission:
            return " The decommission command fails and deletes nothing more.";
    }
    return {};
}

}

std::string_view mountEndWord(MountEnd end)
{
    switch (end)
    {
        case MountEnd::None: return "none";
        case MountEnd::SlotTaken: return "slot_taken";
        case MountEnd::SlotAbsent: return "slot_absent";
        case MountEnd::SlotCorrupt: return "slot_corrupt";
        case MountEnd::FencedByGc: return "fenced_by_gc";
        case MountEnd::LedgerFault: return "ledger_fault";
        case MountEnd::LocalDefect: return "local_defect";
        case MountEnd::Replaced: return "replaced";
        case MountEnd::Forgotten: return "forgotten";
    }
    return "none";
}

bool isLossEnd(MountEnd end)
{
    return end != MountEnd::None && end != MountEnd::Replaced && end != MountEnd::Forgotten;
}

bool endsWithoutShutdown(MountEnd end)
{
    return end == MountEnd::LedgerFault || end == MountEnd::LocalDefect;
}

MountSlotReading classifyMountSlot(const Observation & seen, const OwnMountBodies & own)
{
    if (std::holds_alternative<ProvenAbsent>(seen))
        return {MountSlotVerdict::SlotAbsent, std::nullopt};
    const Object * object = std::get_if<Object>(&seen);
    if (!object)
        return {MountSlotVerdict::NotObserved, std::nullopt};

    MountLease body;
    try
    {
        body = decodeMountLease(object->bytes);
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::CORRUPTED_DATA)
            throw;
        return {MountSlotVerdict::SlotCorrupt, std::nullopt};
    }

    if (body.server_uuid != own.server_uuid || body.writer_epoch != own.writer_epoch)
        return {MountSlotVerdict::SlotTaken, std::move(body)};
    if (own.confirmed_etag && object->etag == *own.confirmed_etag && object->bytes == own.confirmed_bytes)
        return {MountSlotVerdict::Ours, std::move(body)};
    for (const auto * mine : {&own.confirmed, &own.in_flight})
    {
        if (!*mine)
            continue;
        if (isGcFenceOf(body, **mine))
            return {MountSlotVerdict::FencedByGc, std::move(body)};
        /// Our bytes under an incarnation we did not write: another process sent them.
        if (allFields(body) == allFields(**mine))
            return {MountSlotVerdict::SlotTaken, std::move(body)};
    }
    return {MountSlotVerdict::SlotCorrupt, std::move(body)};
}

Observation corroborateAbsence(CasOperation & op, const String & key, Observation seen)
{
    if (!std::holds_alternative<ProvenAbsent>(seen))
        return seen;
    try
    {
        /// `key` sorts first among the keys it prefixes, so one entry decides.
        const ListPage page = op.list(key, "", 1, Retry::once());
        if (!page.keys.empty() && page.keys.front().key == key)
            return NotObserved{};
        return ProvenAbsent{};
    }
    catch (const std::bad_alloc &)
    {
        throw;
    }
    catch (const Exception & e)
    {
        if (isDeterministicLocalFailure(e.code()))
            throw;
        return NotObserved{};
    }
    catch (const std::exception &)
    {
        return NotObserved{};
    }
}

String describeMountEnd(MountEnd end, MountLossReaction reaction, const String & disk_name,
                        const String & server_root_id, const std::optional<MountLease> & holder, std::string_view detail)
{
    const String head = fmt::format("CAS disk '{}' (server_root_id={}): ", disk_name, server_root_id);
    switch (end)
    {
        case MountEnd::SlotTaken:
            return head + fmt::format("the mount slot holds a body of another incarnation ({}).", describeHolder(holder))
                + String(reactionSuffix(end, reaction));
        case MountEnd::SlotAbsent:
            return head + "the mount slot is absent; a slot is deleted by SYSTEM CAS DROP POOL MEMBER or by hand."
                + String(reactionSuffix(end, reaction));
        case MountEnd::SlotCorrupt:
            return head
                + (holder ? fmt::format("the mount slot holds a body of this server's (server_uuid, writer_epoch) "
                                        "that this server did not write ({}).", describeHolder(holder))
                          : String("the mount slot holds bytes that do not decode as a mount lease."))
                + String(reactionSuffix(end, reaction));
        case MountEnd::FencedByGc:
            return head + fmt::format("the GC of another server fenced the decommission claim ({}).", describeHolder(holder))
                + String(reactionSuffix(end, reaction));
        case MountEnd::LedgerFault:
        case MountEnd::LocalDefect:
            if (reaction == MountLossReaction::StopServer)
                return head + fmt::format("{}. The process ends now without a shutdown: a shutdown writes the farewell "
                                          "and the checkpoints under an identity another process may hold.", detail);
            return head + fmt::format("{}.", detail) + String(reactionSuffix(end, reaction));
        case MountEnd::None:
        case MountEnd::Replaced:
        case MountEnd::Forgotten:
            return head + fmt::format("the mount ended ({}).", mountEndWord(end));
    }
    return head;
}

}
