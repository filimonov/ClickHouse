#pragma once
#include <cstdint>
#include <optional>
#include <base/types.h>

namespace DB::Cas
{

/// The limits a writable mount is configured with. `CasMountRuntime::admit` measures a request against
/// `lease_safety_margin_ms` plus a caller-supplied need expressed in attempt envelopes (one for an
/// ordinary attempt, TWO for a ref-log append's write-plus-settlement-read -- see
/// `CasMountRuntime::refAppendFenceOk`), and the three `recovery_retry_*` fields bound a whole
/// ref-table recovery; see `validateCasRequestBudget` for the relationship a writable mount enforces
/// at startup.
struct CasRequestBudget
{
    /// Maximum client wait budgeted for one HTTP attempt. The request contract reserves this before
    /// every attempt it starts; the actual socket-level wait is configured on the object storage's
    /// client (the object storage backend's single-attempt client), not by this struct.
    uint64_t attempt_timeout_ms = 5000;
    /// Reserved on top of the attempt envelopes in every admission.
    uint64_t lease_safety_margin_ms = 2000;

    /// The cap the single-attempt client puts on one TCP connect and again on one TLS handshake,
    /// frozen when the pool opens as `min(disk connect_timeout_ms, attempt_timeout_ms)` (a configured
    /// zero means unbounded and is normalized to the attempt timeout) so a later client reload cannot
    /// widen the envelope. Empty when the storage has no S3 client: the envelope is the attempt alone.
    std::optional<uint64_t> connect_timeout_cap_ms = 1000;

    /// What ONE physical attempt is allowed to cost end to end: two connect caps (TCP, then TLS) plus
    /// the attempt timeout, saturating. The request contract reserves this, not the bare attempt timeout, before
    /// every attempt it starts.
    uint64_t attemptEnvelopeMs() const;

    /// Recovery-level retry (`CasRefLedger::ensureRefTableRecovered`): a whole ref-table recovery
    /// attempt (LIST + snapshot/log GETs + seal PUT) that fails with a transient NETWORK_ERROR is
    /// retried, with capped-exponential backoff, until this total wall-clock budget is spent — then the
    /// error propagates and the table's load fails for this touch (the `lazy_load_tables` database
    /// setting makes the NEXT touch retry). This sits ON TOP of each write's own `Retry` policy window
    /// (`Retry::standard()`'s 90s): one recovery attempt may itself burn ~90s inside a single seal PUT.
    /// Independent of the mount-lease invariants validated in `validateCasRequestBudget` — not part of
    /// that inequality set.
    uint64_t recovery_retry_budget_ms = 120000;
    uint64_t recovery_retry_initial_backoff_ms = 1000;
    uint64_t recovery_retry_max_backoff_ms = 30000;
};

/// The thresholds of one writable mount, computed once from its configuration. Admission, the mount line,
/// `system.cas_mounts` and the refusal texts read this one value, so what is printed is what is enforced.
/// Admission refuses an operation when the confirmed lease left is not more than its reserve.
struct CasLeaseThresholds
{
    uint64_t lease_ms = 0;
    uint64_t renew_period_ms = 0;
    uint64_t envelope_ms = 0;
    uint64_t margin_ms = 0;
    uint64_t conditionalNeedMs() const;         /// two envelopes: a write and its settlement read
    uint64_t conditionalReserveMs() const;      /// the need plus the margin
    uint64_t singleRequestReserveMs() const;    /// one envelope plus the margin
    /// Time after the start of the last confirmed renewal at which each class stops being admitted.
    uint64_t conditionalPauseAfterMs() const;
    uint64_t singleRequestPauseAfterMs() const;
};
CasLeaseThresholds leaseThresholds(const CasRequestBudget & budget, uint64_t lease_ms, uint64_t renew_period_ms);
/// `14000` -> `14`, `31500` -> `31.5`.
String formatSeconds(uint64_t ms);
/// The line a writable mount logs at start: every threshold in seconds and the settings behind them.
String describeMountTimeline(const String & disk, const String & server_root_id, const CasLeaseThresholds & t);

/// Why an operation that needs `needed_ms` of lease was not admitted. `remaining_ms` is empty once the
/// confirmed lease has ended; `refused_code` is set when the renewal's last error is a store refusal.
String describeLeaseRefusal(const String & disk, const CasLeaseThresholds & t, std::optional<uint64_t> remaining_ms,
                            uint64_t needed_ms, const std::optional<String> & last_renewal_error,
                            const std::optional<String> & refused_code);

/// Startup validation: a writable mount refuses to open with an inconsistent budget rather than
/// silently falling back to an unbounded or unsafe retry policy. Throws `BAD_ARGUMENTS` unless
/// envelope + margin < lease and, with background renewal, period + conditional reserve < lease.
///
/// A successor mounting over an unclean predecessor waits at least one lease TTL, plus its
/// materialization grace period, before trusting recovery listings. This is long enough for any
/// conditional PUT still in flight at the predecessor to either land or be abandoned by its own
/// exhausted retry budget. The predecessor's budget is constrained by
/// `attemptEnvelopeMs() + lease_safety_margin_ms < mount_lease_ttl_ms`, so no additional handover
/// check is needed here.
void validateCasRequestBudget(const CasRequestBudget & budget, uint64_t mount_lease_ttl_ms, uint64_t mount_renew_period_ms,
                              bool background_renewal);

}
