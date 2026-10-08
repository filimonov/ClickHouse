#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequestBudget.h>

#include <limits>
#include <fmt/format.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

namespace DB::Cas
{

uint64_t CasRequestBudget::attemptEnvelopeMs() const
{
    /// The TCP connect and the TLS handshake each get one connect interval from Poco, so an HTTPS
    /// attempt may spend two caps before any request I/O. Scheme-agnostic on purpose: conservative for
    /// plain HTTP, exact for HTTPS.
    const uint64_t cap = connect_timeout_cap_ms.value_or(0);
    const uint64_t connects = cap > std::numeric_limits<uint64_t>::max() / 2 ? std::numeric_limits<uint64_t>::max() : 2 * cap;
    return attempt_timeout_ms > std::numeric_limits<uint64_t>::max() - connects
        ? std::numeric_limits<uint64_t>::max()
        : attempt_timeout_ms + connects;
}

namespace
{
uint64_t satAdd(uint64_t a, uint64_t b)
{
    return a > std::numeric_limits<uint64_t>::max() - b ? std::numeric_limits<uint64_t>::max() : a + b;
}

uint64_t satSub(uint64_t a, uint64_t b)
{
    return a > b ? a - b : 0;
}
}

uint64_t CasLeaseThresholds::conditionalNeedMs() const { return satAdd(envelope_ms, envelope_ms); }
uint64_t CasLeaseThresholds::conditionalReserveMs() const { return satAdd(conditionalNeedMs(), margin_ms); }
uint64_t CasLeaseThresholds::singleRequestReserveMs() const { return satAdd(envelope_ms, margin_ms); }
uint64_t CasLeaseThresholds::conditionalPauseAfterMs() const { return satSub(lease_ms, conditionalReserveMs()); }
uint64_t CasLeaseThresholds::singleRequestPauseAfterMs() const { return satSub(lease_ms, singleRequestReserveMs()); }

CasLeaseThresholds leaseThresholds(const CasRequestBudget & budget, uint64_t lease_ms, uint64_t renew_period_ms)
{
    return {lease_ms, renew_period_ms, budget.attemptEnvelopeMs(), budget.lease_safety_margin_ms};
}

String formatSeconds(uint64_t ms)
{
    if (ms % 1000 == 0)
        return fmt::format("{}", ms / 1000);
    String text = fmt::format("{}.{:03}", ms / 1000, ms % 1000);
    while (text.back() == '0')
        text.pop_back();
    return text;
}

String describeMountTimeline(const String & disk, const String & server_root_id, const CasLeaseThresholds & t)
{
    const uint64_t renewal_end_ms = satAdd(t.renew_period_ms, t.envelope_ms);
    return fmt::format(
        "CAS disk '{}' (server_root_id={}): renewal starts every {} s. Without a confirmed renewal conditional "
        "changes pause after {} s; deletes, publishes and uncached reads after {} s; the lease ends after {} s. "
        "Operations resume under the same writer_epoch after a confirmed renewal that leaves enough lease. A renewal "
        "that takes the full request envelope of {} s ends at {} s, {} conditional changes pause. Another server "
        "may take this slot after it has seen the token unchanged for {} s plus its own poll step "
        "(cas_mount_lease_ms={}, cas_mount_renew_period_ms={}, request envelope {} s, cas_lease_safety_margin_ms={}).",
        disk, server_root_id, formatSeconds(t.renew_period_ms),
        formatSeconds(t.conditionalPauseAfterMs()), formatSeconds(t.singleRequestPauseAfterMs()),
        formatSeconds(t.lease_ms), formatSeconds(t.envelope_ms), formatSeconds(renewal_end_ms),
        renewal_end_ms >= t.conditionalPauseAfterMs() ? "after" : "before",
        formatSeconds(satAdd(t.lease_ms, t.lease_ms / 20)), t.lease_ms, t.renew_period_ms,
        formatSeconds(t.envelope_ms), t.margin_ms);
}

void validateCasRequestBudget(const CasRequestBudget & budget, uint64_t mount_lease_ttl_ms,
                              uint64_t mount_renew_period_ms, bool background_renewal)
{
    if (budget.attempt_timeout_ms == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "CAS request budget rejected: attempt_timeout_ms must be at least 1; a zero would reserve nothing "
            "while the request keeps the storage's own timeout");
    const uint64_t envelope = budget.attemptEnvelopeMs();
    /// Subtractions against the unsigned TTL: the sums could wrap for absurd values and read as small.
    const bool one_envelope_fits = envelope < mount_lease_ttl_ms
        && budget.lease_safety_margin_ms < mount_lease_ttl_ms - envelope;
    if (!one_envelope_fits)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "CAS request budget rejected: the attempt envelope ({} ms = attempt_timeout_ms {} + connect cap {}) "
            "plus lease_safety_margin_ms ({}) must be strictly less than cas_mount_lease_ms ({}). "
            "A writable mount refuses to open with this budget.",
            envelope, budget.attempt_timeout_ms, budget.connect_timeout_cap_ms.value_or(0), budget.lease_safety_margin_ms, mount_lease_ttl_ms);
    if (background_renewal)
    {
        /// A renewal must start while conditional changes are still admitted; each is a write plus its
        /// settlement read. Comparing against the pause time, not a sum, keeps absurd values from wrapping.
        const CasLeaseThresholds t = leaseThresholds(budget, mount_lease_ttl_ms, mount_renew_period_ms);
        if (mount_renew_period_ms >= t.conditionalPauseAfterMs())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "CAS disk: a renewal must start while conditional changes are still admitted: "
                "cas_mount_renew_period_ms ({}) plus the conditional reserve ({} = 2 × request envelope {} + "
                "cas_lease_safety_margin_ms {}) must be less than cas_mount_lease_ms ({})",
                mount_renew_period_ms, t.conditionalReserveMs(), t.envelope_ms, t.margin_ms, mount_lease_ttl_ms);
    }
}

}
