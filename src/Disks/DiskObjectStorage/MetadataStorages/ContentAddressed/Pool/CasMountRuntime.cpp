#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasMountRuntime.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasProcessStop.h>
#include <Common/Exception.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/thread_local_rng.h>
#include <base/scope_guard.h>
#include <fmt/ranges.h>
#include <algorithm>
#include <chrono>
#include <ctime>
#include <string_view>
#include <thread>
#include <type_traits>
#include <vector>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int MEMORY_LIMIT_EXCEEDED;
}
}

namespace ProfileEvents
{
    extern const Event CASDataRootVanished;
    extern const Event CASMountLeaseExpired;
    extern const Event CASMountLeaseLost;
    extern const Event CASMountReacquireAttempts;
    extern const Event CASMountReacquireSucceeded;
    extern const Event CASMountReacquireFailed;
    extern const Event CASMountRenewalAttempts;
    extern const Event CASMountRenewalRetries;
    extern const Event CASMountRenewalResolved;
    extern const Event CASMountRenewalRecovered;
}

namespace DB::Cas
{

namespace
{
/// Wall-clock seconds since epoch — the `since` timestamp the lifecycle snapshot reports (spec §7). A
/// wall clock, deliberately unlike the fence's `CLOCK_BOOTTIME`: this is an operator-facing DateTime, not
/// an interval measured across a possible VM suspend.
int64_t wallClockNowSeconds()
{
    return static_cast<int64_t>(std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
}

/// The end a terminal renewal latches. A GC fence of our own body is not an end: the reacquire
/// recovers from it.
MountEnd mountEndOf(const MountRenewResult & result, MountLossReaction reaction)
{
    if (result.outcome != MountRenewOutcome::Terminal)
        return MountEnd::None;
    if (result.local_fault)
        return MountEnd::LocalDefect;
    if (!result.slot)
        return MountEnd::None;
    switch (result.slot->verdict)
    {
        case MountSlotVerdict::SlotTaken: return MountEnd::SlotTaken;
        case MountSlotVerdict::SlotAbsent: return MountEnd::SlotAbsent;
        case MountSlotVerdict::SlotCorrupt: return MountEnd::SlotCorrupt;
        case MountSlotVerdict::FencedByGc:
            /// The decommission pool impersonates a dead member; a reacquire would take its slot again.
            return reaction == MountLossReaction::FailDecommission ? MountEnd::FencedByGc : MountEnd::None;
        case MountSlotVerdict::Ours:
        case MountSlotVerdict::NotObserved:
            return MountEnd::None;
    }
    return MountEnd::None;
}

String failureText(const MountRenewResult & result) noexcept
{
    try
    {
        if (result.failure)
            return getExceptionMessage(result.failure, /*with_stacktrace=*/ false);
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
    }
    return {};
}
}

CasMountRuntime::CasMountRuntime(
    BackendPtr backend_ptr_,
    CasRequests & mount_requests_,
    CasRequests & lease_requests_,
    const Layout & layout_,
    MountConfig config_,
    String server_root_id_,
    const CasEventSink & event_sink_,
    CasRequestBudget cas_request_budget_,
    std::function<ReacquireOutcome(ReacquireStep &)> reacquire_attempt_)
    : backend_ptr(std::move(backend_ptr_))
    , mount_requests(mount_requests_)
    , lease_requests(lease_requests_)
    , layout(layout_)
    , config(std::move(config_))
    , server_root_id(std::move(server_root_id_))
    , event_sink(event_sink_)
    , cas_request_budget(cas_request_budget_)
    , reacquire_attempt(std::move(reacquire_attempt_))
{
}

uint64_t CasMountRuntime::bootMs()
{
    struct timespec ts{};
    clock_gettime(CLOCK_BOOTTIME, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000 + static_cast<uint64_t>(ts.tv_nsec) / 1000000;
}

uint64_t CasMountRuntime::bootMsNow() const
{
    return config.boot_ms_fn ? config.boot_ms_fn() : bootMs();
}

void CasMountRuntime::waitSleep(uint64_t ms) const
{
    if (config.wait_sleep_fn)
        config.wait_sleep_fn(ms);
    else
        std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

bool CasMountRuntime::mayMutate() const
{
    return !mount_fence.lost.load(std::memory_order_acquire)
        && bootMsNow() < mount_fence.deadline_boot_ms.load(std::memory_order_acquire);
}

void CasMountRuntime::tripMountLost()
{
    mount_fence.lost.store(true, std::memory_order_release);
    /// A durable-effect caller admitted under the incarnation this trip just ended must never conclude
    /// the fence is fine again just because a LATER `armMountFence` happens to re-arm it (rev.7 [C2]).
    fence_generation.fetch_add(1, std::memory_order_acq_rel);
    /// FORGET publishes terminal intent before tripping the fence. That deliberate decommission is not
    /// an operational loss/recovery generation and must not pass through the transient-loss accounting
    /// edge. Ordinary external or renewal loss has no terminal intent and retains the single CAS winner.
    if (!vanishedIntentPublished())
        (void)noteLeaseLost();
}

void CasMountRuntime::tripFenceWithoutOperationalLoss()
{
    mount_fence.lost.store(true, std::memory_order_release);
    fence_generation.fetch_add(1, std::memory_order_acq_rel);
}

void CasMountRuntime::checkFenceOrThrow(uint64_t admitted_generation) const
{
    /// [D5]: tell only what is known here. A tripped fence (or a bumped generation) means this node no
    /// longer holds the mount incarnation the caller was admitted under -- but this same guard trips for a
    /// transient lease blip AND for a deliberate terminal decommission (FORGET) or a lost identity, and this
    /// code cannot tell them apart. So the CONDITION must NOT promise recovery ("temporarily unreachable"
    /// would misdiagnose the terminal case); it names both possibilities and points at the authoritative
    /// lifecycle. The CLASS is the write plane's uniform transient one (its 32 sibling write-transient sites
    /// already mint it): under genuine ambiguity the refusal must be retried, never consumed as damage.
    if (!mayMutate() || fenceGeneration() != admitted_generation)
        throwCasTransientUnavailable(
            fmt::format("content-addressed pool '{}'", server_root_id),
            "mount fence tripped: the durable write is refused because this node no longer holds the mount "
            "incarnation it was admitted under -- either a lease loss the disk auto-recovers from, or a "
            "FORGET decommission / lost identity that does NOT recover; consult "
            "system.cas_mounts for the disk's lifecycle before retrying");
}

Fence::Admit CasMountRuntime::admit(uint64_t admitted_generation, uint64_t needed_ms) const
{
    if (mount_fence.lost.load(std::memory_order_acquire) || fenceGeneration() != admitted_generation)
        return Fence::Admit::LostOrRearmed;
    const uint64_t now = bootMsNow();
    const uint64_t deadline = mount_fence.deadline_boot_ms.load(std::memory_order_acquire);
    if (now >= deadline)
        return Fence::Admit::NoBudget;
    /// Compared by subtraction rather than as the sum `needed_ms + margin`, which can wrap for an
    /// absurd configuration and then read as if there were room.
    const uint64_t remaining = deadline - now;
    if (needed_ms >= remaining || cas_request_budget.lease_safety_margin_ms >= remaining - needed_ms)
        return Fence::Admit::NoBudget;
    return Fence::Admit::Ok;
}

bool CasMountRuntime::refAppendFenceOk() const
{
    /// Two envelopes' worth of room under the live generation -- a write and its settlement read, which
    /// is what `writeLoop` reserves -- so a ref-log attempt is not started when it cannot plausibly
    /// finish, safety margin included, before the lease expires. `admit` adds the margin itself.
    const uint64_t needed_ms = conditionalReserveMs() - cas_request_budget.lease_safety_margin_ms;
    return admit(fenceGeneration(), needed_ms) == Fence::Admit::Ok;
}

uint64_t CasMountRuntime::conditionalReserveMs() const
{
    const uint64_t envelope_ms = cas_request_budget.attemptEnvelopeMs();
    const uint64_t two = envelope_ms > std::numeric_limits<uint64_t>::max() / 2
        ? std::numeric_limits<uint64_t>::max() : 2 * envelope_ms;
    const uint64_t margin = cas_request_budget.lease_safety_margin_ms;
    return two > std::numeric_limits<uint64_t>::max() - margin ? std::numeric_limits<uint64_t>::max() : two + margin;
}

LeasePauseState CasMountRuntime::leasePause() const
{
    if (const std::optional<uint64_t> expired_at = leaseExpiredAtBootMs())
        return {LeasePause::LeaseExpired, *expired_at};
    if (lifecycle() != PoolLifecycle::Live || mount_fence.lost.load(std::memory_order_acquire) || refAppendFenceOk())
        return {};
    const uint64_t deadline = mount_fence.deadline_boot_ms.load(std::memory_order_acquire);
    const uint64_t reserve = conditionalReserveMs();
    return {LeasePause::RenewalOverdue, deadline > reserve ? deadline - reserve : 0};
}

std::optional<uint64_t> CasMountRuntime::leaseExpiredAtBootMs() const
{
    if (lifecycle() != PoolLifecycle::Live || mount_fence.lost.load(std::memory_order_acquire))
        return std::nullopt;
    const uint64_t deadline = mount_fence.deadline_boot_ms.load(std::memory_order_acquire);
    if (bootMsNow() < deadline)
        return std::nullopt;
    return deadline;
}

void CasMountRuntime::warnOnceIfLeaseExpired() noexcept
{
    try
    {
        const std::optional<uint64_t> expired_at = leaseExpiredAtBootMs();
        if (!expired_at || lease_expiry_warned.exchange(true, std::memory_order_relaxed))
            return;
        ProfileEvents::incrementNoTrace(ProfileEvents::CASMountLeaseExpired);
        /// Written at the first attempt after the expiry, so a renewal stuck in a request reports late; the
        /// age says by how much.
        LOG_WARNING(getLogger("CasPool"),
            "CAS mount lease of '{}' expired {} ms ago under writer_epoch {}; the renewal keeps retrying "
            "and writes resume after a confirmed renewal that leaves enough lease",
            server_root_id, bootMsNow() - *expired_at, liveWriterEpoch());
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// A lost diagnostic must not change how the renewal ends.
    }
}

void CasMountRuntime::setMountDeadline(uint64_t deadline_boot_ms)
{
    mount_fence.deadline_boot_ms.store(deadline_boot_ms, std::memory_order_release);
}

void CasMountRuntime::armMountFence(uint64_t deadline_boot_ms)
{
    mount_fence.deadline_boot_ms.store(deadline_boot_ms, std::memory_order_release);
    lease_expiry_warned.store(false, std::memory_order_relaxed);
    /// A fresh lease incarnation is a fresh generation too: a durable-effect caller admitted under the
    /// PRIOR incarnation must re-check and abort rather than ride this re-arm through (rev.7 [C2]).
    fence_generation.fetch_add(1, std::memory_order_acq_rel);
    if (arm_mount_fence_interposition_hook_for_test)
        arm_mount_fence_interposition_hook_for_test();
    /// Open the gate LAST. A caller that observes `lost == false` with acquire semantics must also see
    /// the fresh generation; publishing the latch first exposes one admission window in which the dead
    /// generation looks live again.
    mount_fence.lost.store(false, std::memory_order_seq_cst);
    /// `seq_cst` with the store above and in `latchMountEnd`: whichever runs second sees the other.
    if (mount_end.load(std::memory_order_seq_cst) != MountEnd::None)
    {
        mount_fence.lost.store(true, std::memory_order_seq_cst);
        fence_generation.fetch_add(1, std::memory_order_acq_rel);
    }
}

bool CasMountRuntime::latchMountEnd(MountEnd end) noexcept
{
    MountEnd expected = MountEnd::None;
    if (!mount_end.compare_exchange_strong(expected, end, std::memory_order_seq_cst))
        return false;
    /// `seq_cst` with the store and re-check in `armMountFence`: whichever runs second sees the other.
    mount_fence.lost.store(true, std::memory_order_seq_cst);
    fence_generation.fetch_add(1, std::memory_order_acq_rel);
    if (isLossEnd(end))
    {
        lifecycle_since_wall_s.store(wallClockNowSeconds(), std::memory_order_release);
        pool_lifecycle.store(PoolLifecycle::Stopping, std::memory_order_release);
        ProfileEvents::incrementNoTrace(ProfileEvents::CASMountLeaseLost);
    }
    return true;
}

void CasMountRuntime::reactToMountEnd(
    MountEnd end, const std::optional<MountLease> & holder, std::string_view detail) noexcept
{
    /// No lock: a waiter that misses this wakes on its own timeout or on stop, and the ledger may call
    /// here with its own locks held.
    driver_cv.notify_all();
    const auto line = [&] { return describeMountEnd(end, config.loss_reaction, config.disk_name, server_root_id, holder, detail); };
    if (config.loss_reaction == MountLossReaction::StopServer && endsWithoutShutdown(end))
    {
        try
        {
            const String text = line();
            LOG_FATAL(getLogger("CasPool"), "{}", text);
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
        }
        terminateProcess();
        return;
    }
    if (config.loss_reaction == MountLossReaction::StopServer)
        requestGracefulProcessStop();
    try
    {
        const String text = line();
        LOG_ERROR(getLogger("CasPool"), "{}", text);
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
    }
}

uint64_t CasMountRuntime::minActive()
{
    std::lock_guard lk(builds_mutex);
    return active_build_seqs.empty() ? next_build_seq : *active_build_seqs.begin();
}

uint64_t CasMountRuntime::peekNextBuildSeq()
{
    std::lock_guard lk(builds_mutex);
    return next_build_seq;
}

void CasMountRuntime::renewWatermarkOnce()
{
    (void)renewRenewerOnce(RenewalKind::Direct);
}

uint64_t CasMountRuntime::allocateBuildSeq()
{
    std::lock_guard lk(builds_mutex);
    const uint64_t s = next_build_seq++;
    active_build_seqs.insert(s);
    return s;
}

void CasMountRuntime::registerInflightBuild(uint64_t seq, const PartWriteTxnPtr & build)
{
    /// The caller owns the build's shared pointer. Keep only a weak reference here so the registry does
    /// not extend the build lifetime; publication, abandonment, or destruction removes the entry.
    std::lock_guard lk(builds_mutex);
    inflight_builds[seq] = build;
}

void CasMountRuntime::retireBuildSeq(uint64_t seq)
{
    std::lock_guard lk(builds_mutex);
    active_build_seqs.erase(seq);
    inflight_builds.erase(seq);
}

void CasMountRuntime::cancelInflightBuildsForNamespace(const RootNamespace & ns)
{
    /// The removal callback is invoked only after the namespace-removal transaction is durable. Keep
    /// cancellation outside `builds_mutex`; `cancelForNamespaceRemoval` changes the build's atomic
    /// cancellation state and does not require the registry lock.
    std::vector<PartWriteTxnPtr> builds_to_check;
    {
        std::lock_guard lk(builds_mutex);
        for (const auto & entry : inflight_builds)
            if (auto build = entry.second.lock())
                builds_to_check.push_back(std::move(build));
    }
    for (const auto & build : builds_to_check)
        build->cancelForNamespaceRemoval(ns);
}

void CasMountRuntime::mintRandomProcessEpoch()
{
    /// Mint a nonzero equality-only identity. Keep it away from the zero/unarmed and UINT64_MAX/retired
    /// sentinels; 52 random bits are sufficient for the expected collision risk of this token.
    constexpr uint64_t EPOCH_MASK = (1ULL << 52) - 1;
    process_epoch.store(
        (thread_local_rng() ^ (static_cast<uint64_t>(thread_local_rng()) << 32)) & EPOCH_MASK,
        std::memory_order_relaxed);
    if (process_epoch.load(std::memory_order_relaxed) == 0)
        process_epoch.store(1, std::memory_order_relaxed);
}

void CasMountRuntime::setProcessEpoch(uint64_t v, std::memory_order order)
{
    process_epoch.store(v, order);
}

void CasMountRuntime::setLiveWriterEpoch(uint64_t v)
{
    live_writer_epoch.store(v, std::memory_order_release);
}

MountLeaseRenewer * CasMountRuntime::beginRenewal(RenewalKind kind)
{
    std::lock_guard lock(driver_mutex);
    if (kind == RenewalKind::Direct && config.background_watermark)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "CAS mount runtime: direct renewal is disabled when background ownership is configured");
    if ((kind == RenewalKind::Startup || kind == RenewalKind::Direct) && (workers_starting || workers_started))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS mount runtime: a caller renewal while the renewal thread runs");
    if (renewer_in_use)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS mount runtime: a renewal is already in progress");
    if (!mount_renewer)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS mount runtime: renewal without a renewer");
    if (mount_renewer->state() != MountLeaseRenewerState::Active)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS mount runtime: renewal requires an Active renewer");
    renewer_in_use = true;
    return mount_renewer.get();
}

void CasMountRuntime::finishRenewal(RenewalKind kind, const MountRenewResult & result, const MountLeaseRenewer * renewer)
{
    std::lock_guard lock(driver_mutex);
    renewer_in_use = false;
    /// Published before the renewer is released, so a reacquire cannot arm a new epoch between this
    /// renewal's commit and its deadline; a renewer that was replaced moves nothing.
    if (result.outcome == MountRenewOutcome::Committed && renewer == mount_renewer.get())
    {
        publishRenewedDeadline(result.attempt_start_boot_ms);
        publishConfirmedBody(renewer->lastConfirmedBody());
    }
    /// A latched end closed the fence already; the reacquire handles its own terminal renewals.
    if (result.outcome == MountRenewOutcome::Terminal && mountEnd() == MountEnd::None && kind != RenewalKind::Reacquire)
    {
        const MountSlotReading * slot = result.slot ? &*result.slot : nullptr;
        if (kind == RenewalKind::Startup || workers_stop_requested)
        {
            tripFenceWithoutOperationalLoss();
        }
        else if (slot && slot->verdict == MountSlotVerdict::FencedByGc && slot->fenced && slot->holder)
        {
            /// The fence is our own body: the reacquire replaces exactly it, by the ETag read here.
            tripMountLost();
            owed_reacquire = ReacquireCertificate{.fenced = *slot->fenced, .body = *slot->holder};
        }
        else
        {
            tripMountLost();
        }
    }
    driver_cv.notify_all();
}

void CasMountRuntime::installRenewer(
    UInt128 our_uuid,
    uint64_t writer_epoch,
    const std::function<uint64_t()> & now_ms)
{
    auto replacement = std::make_unique<MountLeaseRenewer>(
        mount_requests, lease_requests, layout, server_root_id, our_uuid, writer_epoch,
        config.mount_lease_ttl_ms, now_ms,
        [this] { return minActive(); },
        [this](CasEvent e) { emitEvent(std::move(e)); },
        std::chrono::milliseconds(cas_request_budget.lease_safety_margin_ms),
        [this] { return bootMsNow(); });

    std::lock_guard lock(driver_mutex);
    if (renewer_in_use)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS mount runtime: renewer replacement while a renewal is in progress");
    mount_renewer = std::move(replacement);
}

uint64_t CasMountRuntime::startRenewer(const MountClaimed & claimed)
{
    MountLeaseRenewer * renewer;
    {
        std::lock_guard lock(driver_mutex);
        if (!mount_renewer)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS mount runtime: startRenewer without a renewer");
        if (mount_renewer->state() != MountLeaseRenewerState::New)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS mount runtime: startRenewer requires a New renewer");
        if (renewer_in_use)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS mount runtime: startRenewer while a renewal is in progress");
        renewer = mount_renewer.get();
        renewer_in_use = true;
    }
    SCOPE_EXIT({
        std::lock_guard lock(driver_mutex);
        renewer_in_use = false;
        driver_cv.notify_all();
    });
    renewer->start(claimed);
    publishConfirmedBody(renewer->lastConfirmedBody());
    return claimed.start_boot_ms;
}

RenewalProgress CasMountRuntime::renewalProgress() const
{
    std::lock_guard lock(renewal_progress_mutex);
    return renewal_progress;
}

std::optional<MountLease> CasMountRuntime::confirmedMountBody() const
{
    std::lock_guard lock(renewal_progress_mutex);
    return confirmed_body;
}

void CasMountRuntime::publishConfirmedBody(const std::optional<MountLease> & body)
{
    std::lock_guard lock(renewal_progress_mutex);
    confirmed_body = body;
}

MountRenewOperationEnvironment CasMountRuntime::renewalEnvironment(bool worker_call)
{
    uint64_t period_ms = 0;
    if (worker_call)
    {
        std::lock_guard lock(driver_mutex);
        period_ms = static_cast<uint64_t>(std::max<int64_t>(0, renewal_period.count()));
    }
    return MountRenewOperationEnvironment{
        .boot_ms = [this] { return bootMsNow(); },
        .live = [this, worker_call]
        {
            return config.renewal_live_for_test ? config.renewal_live_for_test() : renewalLive(worker_call);
        },
        .cancelled = [this] { return renewalCancelled(); },
        .requests = RequestObserver{
            .on_write_attempt = [this](uint32_t attempts_sent)
            {
                warnOnceIfLeaseExpired();
                ProfileEvents::incrementNoTrace(ProfileEvents::CASMountRenewalAttempts);
                if (attempts_sent > 1)
                    ProfileEvents::incrementNoTrace(ProfileEvents::CASMountRenewalRetries);
                std::lock_guard lock(renewal_progress_mutex);
                renewal_progress.attempts_sent = attempts_sent;
            },
            .on_failure = [this](std::string_view verb, const String & key, const std::exception & e)
            {
                /// Runs inside the engine's catch; a formatting failure must not change how the renewal ends.
                try
                {
                    String error = describeRequestFailure(verb, key, e);
                    std::lock_guard lock(renewal_progress_mutex);
                    renewal_progress.last_error = std::move(error);
                }
                catch (...)
                {
                }
            },
        },
        .credential_refusal_pause = worker_call
            ? std::function<void()>([this, period_ms] { waitBeforeNextRenewalCall(period_ms); })
            : std::function<void()>{},
    };
}

bool CasMountRuntime::renewalLive(bool worker_call) const
{
    std::lock_guard lock(driver_mutex);
    if (workers_stop_requested)
        return false;
    return !(worker_call
        && (lifecycle() != PoolLifecycle::Live || mount_fence.lost.load(std::memory_order_acquire)));
}

bool CasMountRuntime::renewalCancelled() const
{
    std::lock_guard lock(driver_mutex);
    return workers_stop_requested;
}

void CasMountRuntime::waitBeforeNextRenewalCall(uint64_t ms)
{
    if (config.wait_sleep_fn)
    {
        config.wait_sleep_fn(ms);
        return;
    }
    std::unique_lock lock(driver_mutex);
    driver_cv.wait_for(lock, std::chrono::milliseconds(ms), [this] { return workers_stop_requested; });
}

void CasMountRuntime::sleepInterruptibly(uint64_t ms)
{
    std::unique_lock lock(driver_mutex);
    driver_cv.wait_for(lock, std::chrono::milliseconds(ms), [this] { return workers_stop_requested; });
}

void CasMountRuntime::consumeRenewResult(const MountRenewResult & result, bool propagate_failure)
{
    /// The renewer has already been released by `finishRenewal`; this is the single logical consumption
    /// boundary and it runs without `driver_mutex` or renewer access.
    if (result.resolved_by_read)
        ProfileEvents::incrementNoTrace(ProfileEvents::CASMountRenewalResolved);

    if (result.outcome == MountRenewOutcome::Committed && (result.attempts_sent > 1 || result.resolved_by_read))
        ProfileEvents::incrementNoTrace(ProfileEvents::CASMountRenewalRecovered);

    if (result.outcome != MountRenewOutcome::Terminal)
    {
        reportRenewal(result);
        return;
    }

    if (!result.failure)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS mount runtime: terminal renewal has no failure");
    try
    {
        std::rethrow_exception(result.failure);
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::LOGICAL_ERROR)
            throw;
    }
    catch (...)
    {
    }

    reportRenewal(result);

    if (propagate_failure)
        std::rethrow_exception(result.failure);
}

void CasMountRuntime::reportRenewal(const MountRenewResult & result) const noexcept
{
    const bool recovered = result.outcome == MountRenewOutcome::Committed
        && (result.attempts_sent > 1 || result.resolved_by_read);
    if (!recovered && result.outcome != MountRenewOutcome::Terminal)
        return;
    try
    {
        const uint64_t now = bootMsNow();
        const uint64_t elapsed_ms = now > result.attempt_start_boot_ms ? now - result.attempt_start_boot_ms : 0;
        const uint64_t remaining_ms = result.confirmed_deadline_boot_ms > now ? result.confirmed_deadline_boot_ms - now : 0;
        const std::string_view classification = !recovered ? toString(result.classification)
            : result.resolved_by_read ? "committed_by_read" : "committed_after_retry";
        if (event_sink)
        {
            CasEvent event;
            event.type = CasEventType::WatermarkRenew;
            event.outcome = recovered ? "recovered" : "failed";
            event.reason = recovered
                ? "CAS mount renewal committed after a retry or a settling read"
                : "CAS mount renewal ended without retained authority and fenced the mount";
            event.detail = {
                {"server_root_id", server_root_id},
                {"writer_epoch", std::to_string(result.writer_epoch)},
                {"seq", std::to_string(result.seq)},
                {"write_attempt_id", u128ToHex(result.write_attempt_id).substr(0, 12)},
                {"attempts_sent", std::to_string(result.attempts_sent)},
                {"elapsed_ms", std::to_string(elapsed_ms)},
                {"remaining_confirmed_budget_ms", std::to_string(remaining_ms)},
                {"classification", String{classification}},
            };
            try
            {
                event_sink(std::move(event));
            }
            catch (...) // NOLINT(bugprone-empty-catch)
            {
                /// A failing sink must not suppress the log lines below.
            }
        }
        auto log = getLogger("CasMountLeaseRenewer");
        for (uint32_t attempt_no = 2; attempt_no <= result.attempts_sent; ++attempt_no)
            LOG_DEBUG(log, "CAS mount renewal '{}' physical retry attempt {} (writer_epoch={}, seq={})",
                server_root_id, attempt_no, result.writer_epoch, result.seq);
        if (recovered)
            LOG_INFO(log, "CAS mount renewal '{}' recovered after {} physical attempts in {} ms "
                "(classification={}, confirmed_deadline_boot_ms={})",
                server_root_id, result.attempts_sent, elapsed_ms, classification, result.confirmed_deadline_boot_ms);
        else
            LOG_WARNING(log, "CAS mount renewal '{}' fenced after {} physical attempts in {} ms "
                "(classification={}, confirmed_deadline_boot_ms={})",
                server_root_id, result.attempts_sent, elapsed_ms, classification, result.confirmed_deadline_boot_ms);
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// A lost diagnostic must not change how the renewal ends.
    }
}

void CasMountRuntime::publishRenewedDeadline(uint64_t attempt_start_boot_ms)
{
    const uint64_t ttl_ms = static_cast<uint64_t>(config.mount_lease_ttl_ms.count());
    const uint64_t deadline = attempt_start_boot_ms > std::numeric_limits<uint64_t>::max() - ttl_ms
        ? std::numeric_limits<uint64_t>::max()
        : attempt_start_boot_ms + ttl_ms;
    setMountDeadline(deadline);
    if (bootMsNow() < deadline)
        lease_expiry_warned.store(false, std::memory_order_relaxed);
}

MountRenewResult CasMountRuntime::renewRenewerOnce(RenewalKind kind)
{
    MountLeaseRenewer * renewer = beginRenewal(kind);
    bool released = false;
    SCOPE_EXIT({
        if (!released)
        {
            std::lock_guard lock(driver_mutex);
            renewer_in_use = false;
            driver_cv.notify_all();
        }
    });
    if (kind == RenewalKind::Worker && config.renewal_admitted_hook_for_test)
        config.renewal_admitted_hook_for_test();
    {
        std::lock_guard lock(renewal_progress_mutex);
        renewal_progress = RenewalProgress{.in_progress = true, .attempts_sent = 0, .last_error = std::nullopt};
    }
    MountRenewResult result;
    {
        SCOPE_EXIT({ std::lock_guard lock(renewal_progress_mutex); renewal_progress.in_progress = false; });
        if (kind == RenewalKind::Reacquire)
        {
            MountRenewOperationEnvironment environment = renewalEnvironment(/*worker_call=*/false);
            environment.live = [this] { return reacquireLive(); };
            result = renewer->renewUntilDefinitive(environment);
        }
        else if (kind == RenewalKind::Worker)
            result = renewer->renewUntilDefinitive(renewalEnvironment(/*worker_call=*/true));
        else
            result = renewer->renew(renewalEnvironment(/*worker_call=*/false));
    }
    /// A startup renewal ends in the open's own exception: nothing is running yet that a stop would end.
    const MountEnd end = kind == RenewalKind::Startup ? MountEnd::None : mountEndOf(result, config.loss_reaction);
    const bool latched = end != MountEnd::None && latchMountEnd(end);
    finishRenewal(kind, result, renewer);
    released = true;
    if (config.renewal_finished_hook_for_test)
        config.renewal_finished_hook_for_test(result);
    if (latched)
        reactToMountEnd(end, result.slot ? result.slot->holder : std::nullopt, failureText(result));
    consumeRenewResult(result, /*propagate_failure=*/kind == RenewalKind::Startup || kind == RenewalKind::Direct);
    return result;
}

uint64_t CasMountRuntime::renewRenewerForStartupOnce()
{
    return renewRenewerOnce(RenewalKind::Startup).attempt_start_boot_ms;
}

MountRenewResult CasMountRuntime::renewForReacquireOnce()
{
    return renewRenewerOnce(RenewalKind::Reacquire);
}

bool CasMountRuntime::renewalWindowFits(uint64_t deadline_boot_ms) const
{
    const uint64_t safety_ms = cas_request_budget.lease_safety_margin_ms;
    const uint64_t safe_deadline = deadline_boot_ms > safety_ms ? deadline_boot_ms - safety_ms : 0;
    const uint64_t now_boot_ms = bootMsNow();
    const uint64_t period_ms = static_cast<uint64_t>(config.mount_renew_period.count());
    const uint64_t envelope_ms = cas_request_budget.attemptEnvelopeMs();
    const uint64_t two_envelopes_ms = envelope_ms > std::numeric_limits<uint64_t>::max() / 2
        ? std::numeric_limits<uint64_t>::max() : 2 * envelope_ms;
    const uint64_t renewal_window_ms = config.background_watermark
        ? (two_envelopes_ms > std::numeric_limits<uint64_t>::max() - period_ms
              ? std::numeric_limits<uint64_t>::max() : period_ms + two_envelopes_ms)
        : two_envelopes_ms;
    return now_boot_ms <= safe_deadline && renewal_window_ms < safe_deadline - now_boot_ms;
}

bool CasMountRuntime::reacquireLive() const
{
    std::lock_guard lock(driver_mutex);
    return !workers_stop_requested && !vanishedIntentPublished() && mountEnd() == MountEnd::None;
}

bool CasMountRuntime::openAfterReacquire(uint64_t deadline_boot_ms)
{
    /// One step with the decision: a latch or an expiry between them must keep the fence closed.
    std::lock_guard lock(driver_mutex);
    if (workers_stop_requested || vanishedIntentPublished() || mountEnd() != MountEnd::None
        || lifecycle() != PoolLifecycle::Reacquiring)
        return false;
    const uint64_t now = bootMsNow();
    if (deadline_boot_ms <= now || deadline_boot_ms - now <= conditionalReserveMs())
        return false;
    armMountFence(deadline_boot_ms);
    noteReacquired();
    driver_cv.notify_all();
    return true;
}

ReacquireOutcome CasMountRuntime::runReacquireForTest()
{
    {
        std::lock_guard lock(driver_mutex);
        if (workers_started)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS mount runtime: a test reacquire runs without workers");
        if (owed_reacquire)
        {
            reacquire_step_for_test = ReacquireStep{.certificate = std::move(*owed_reacquire)};
            owed_reacquire.reset();
        }
        if (!reacquire_step_for_test)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS mount runtime: no reacquire is owed");
    }
    const ReacquireOutcome outcome = runReacquireStep(*reacquire_step_for_test);
    if (outcome != ReacquireOutcome::Retry)
        reacquire_step_for_test.reset();
    return outcome;
}

void CasMountRuntime::renewerReset()
{
    std::lock_guard lock(driver_mutex);
    if (renewer_in_use)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS mount runtime: renewer reset while renewal is active");
    mount_renewer.reset();
}

ThreadFromGlobalPool CasMountRuntime::makeWorker(std::function<void()> body)
{
    if (config.worker_factory)
        return config.worker_factory(std::move(body));
    return ThreadFromGlobalPool(std::move(body));
}

void CasMountRuntime::startBackgroundWorkers(std::chrono::milliseconds period)
{
    {
        std::lock_guard lock(driver_mutex);
        if (renewer_in_use || workers_starting || workers_started || renewal_worker.joinable())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS mount runtime: background workers cannot start in the current state");
        if (!mount_renewer || mount_renewer->state() != MountLeaseRenewerState::Active)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS mount runtime: background workers require an Active renewer");
        workers_starting = true;
        workers_stop_requested = false;
        worker_loops_released = false;
        renewal_period = period;
    }

    ThreadFromGlobalPool worker;
    try
    {
        worker = makeWorker([this] { renewalLoop(); });
    }
    catch (...)
    {
        {
            std::lock_guard lock(driver_mutex);
            workers_stop_requested = true;
            worker_loops_released = true;
            driver_cv.notify_all();
        }
        if (worker.joinable())
            worker.join();
        {
            std::lock_guard lock(driver_mutex);
            workers_starting = false;
            workers_started = false;
        }
        tripFenceWithoutOperationalLoss();
        throw;
    }

    {
        std::lock_guard lock(driver_mutex);
        renewal_worker = std::move(worker);
        workers_starting = false;
        workers_started = true;
        worker_loops_released = true;
        driver_cv.notify_all();
    }
}

void CasMountRuntime::renewalLoop()
{
    setThreadName(ThreadName::CAS_LEASE_RENEWER);
    /// The tracker still counts this thread's allocations but never throws on them: a renewal must not
    /// fail on the server memory limit. `Global`, because a `User`-level lock leaves that limit enforced.
    LockMemoryExceptionInThread memory_exception_lock(VariableContext::Global);
    {
        std::unique_lock lock(driver_mutex);
        driver_cv.wait(lock, [this] { return worker_loops_released; });
        if (workers_stop_requested || workersMustExit())
            return;
    }

    while (true)
    {
        if (config.renewal_before_driver_lock_hook_for_test)
            config.renewal_before_driver_lock_hook_for_test();

        std::optional<ReacquireCertificate> owed;
        {
            std::unique_lock lock(driver_mutex);
            if (workers_stop_requested || workersMustExit())
                return;
            owed = std::exchange(owed_reacquire, std::nullopt);
            if (!owed)
            {
                if (!mount_renewer || mount_renewer->state() != MountLeaseRenewerState::Active)
                {
                    driver_cv.wait(lock);
                    continue;
                }
                const uint64_t last_anchor = mount_renewer->lastCommittedAttemptStartBootMs();
                const uint64_t period_ms = static_cast<uint64_t>(std::max<int64_t>(0, renewal_period.count()));
                const uint64_t due = last_anchor > std::numeric_limits<uint64_t>::max() - period_ms
                    ? std::numeric_limits<uint64_t>::max()
                    : last_anchor + period_ms;
                const uint64_t now = bootMsNow();
                if (now < due)
                {
                    /// Any notification can change the cadence decision, and a reacquire may publish an
                    /// already-overdue renewer anchor: re-sample state and BOOTTIME after every wake.
                    driver_cv.wait_for(lock, std::chrono::milliseconds(due - now));
                    continue;
                }
            }
        }

        try
        {
            if (owed)
            {
                (void)reacquireUntilSettled(std::move(*owed));
                continue;
            }
            const MountRenewResult result = renewRenewerOnce(RenewalKind::Worker);
            if (result.outcome == MountRenewOutcome::Terminal && mountEnd() == MountEnd::None && !reacquireOwed())
            {
                if (renewalCancelled() || vanishedIntentPublished())
                    return;
                /// Under the global memory lock this cannot be thrown; if it is, the handler below closes the disk.
                if (result.failure && getExceptionErrorCode(result.failure) == ErrorCodes::MEMORY_LIMIT_EXCEEDED)
                    std::rethrow_exception(result.failure);
                if (latchMountEnd(MountEnd::LocalDefect))
                    reactToMountEnd(MountEnd::LocalDefect, std::nullopt,
                        "a background renewal ended without a loss, a fence of our own body or a stop");
                return;
            }
        }
        catch (...)
        {
            /// The worker path does not propagate renewal failures, so anything arriving here is this
            /// loop's own state machine reporting that it was driven out of contract: every later renewal
            /// would be unaudited. The server memory limit is not such a defect; it keeps the bounded exit,
            /// where write admission still ends with the fence deadline.
            if (getCurrentExceptionCode() == ErrorCodes::MEMORY_LIMIT_EXCEEDED)
            {
                tripMountLost();
                tryLogCurrentException(getLogger("CasPool"), "CAS mount-lease renewal loop");
                chassert(false);
                return;
            }
            String detail;
            try
            {
                detail = getCurrentExceptionMessage(/*with_stacktrace=*/ false);
            }
            catch (...) // NOLINT(bugprone-empty-catch)
            {
            }
            if (latchMountEnd(MountEnd::LocalDefect))
                reactToMountEnd(MountEnd::LocalDefect, std::nullopt, detail);
            return;
        }
    }
}

ReacquireOutcome CasMountRuntime::reacquireUntilSettled(ReacquireCertificate certificate)
{
    ReacquireStep step{.certificate = std::move(certificate)};
    while (true)
    {
        const uint64_t started_boot_ms = bootMsNow();
        const ReacquireOutcome outcome = runReacquireStep(step);
        if (outcome != ReacquireOutcome::Retry)
            return outcome;
        waitBeforeNextReacquireStep(Retry::spacedPause(
            Retry::drawSpacing(Retry::kDefinitiveAttemptSpacingMs), started_boot_ms, bootMsNow()));
    }
}

ReacquireOutcome CasMountRuntime::runReacquireStep(ReacquireStep & step)
{
    ProfileEvents::incrementNoTrace(ProfileEvents::CASMountReacquireAttempts);
    const ReacquireOutcome outcome = reacquire_attempt(step);
    if (outcome == ReacquireOutcome::Open)
        ProfileEvents::incrementNoTrace(ProfileEvents::CASMountReacquireSucceeded);
    else if (outcome == ReacquireOutcome::Ended)
        ProfileEvents::incrementNoTrace(ProfileEvents::CASMountReacquireFailed);
    return outcome;
}

bool CasMountRuntime::reacquireOwed() const
{
    std::lock_guard lock(driver_mutex);
    return owed_reacquire.has_value();
}

void CasMountRuntime::waitBeforeNextReacquireStep(uint64_t ms)
{
    if (config.wait_sleep_fn)
    {
        config.wait_sleep_fn(ms);
        return;
    }
    std::unique_lock lock(driver_mutex);
    driver_cv.wait_for(lock, std::chrono::milliseconds(ms), [this] { return workers_stop_requested || workersMustExit(); });
}

void CasMountRuntime::stopBackgroundWorkers()
{
    ThreadFromGlobalPool * renewal_to_join = nullptr;
    {
        std::lock_guard lock(driver_mutex);
        if (!workers_started && !workers_starting && !renewal_worker.joinable())
            return;
        workers_stop_requested = true;
        worker_loops_released = true;
        driver_cv.notify_all();
        renewal_to_join = &renewal_worker;
    }

    if (renewal_to_join->joinable())
        renewal_to_join->join();

    {
        std::lock_guard lock(driver_mutex);
        workers_starting = false;
        workers_started = false;
        driver_cv.notify_all();
    }
}
bool CasMountRuntime::isVanished() const
{
    return lifecycle() == PoolLifecycle::VanishedForgotten;
}

void CasMountRuntime::setLifecycleForTest(PoolLifecycle lc)
{
    /// Direct store, no precondition — the test harness pins an exact cell of the class × state table.
    /// `VanishedForgotten` also latches `vanished_intent` so the forced terminal state matches what a
    /// natural `enterVanished` would leave behind (its truth semantics never depend on how it was reached).
    /// Stamp `since` to match a naturally-reached state (release-store before the state store below, so a
    /// snapshot reader that acquire-observes the forced state also observes the timestamp): 0 for `Live`,
    /// now for every non-`Live` value. Keeps the forced cell of the class × state table indistinguishable
    /// from a real transition for the introspection snapshot.
    lifecycle_since_wall_s.store(lc == PoolLifecycle::Live ? 0 : wallClockNowSeconds(), std::memory_order_release);
    pool_lifecycle.store(lc, std::memory_order_release);
    if (lc == PoolLifecycle::VanishedForgotten)
    {
        vanished_intent.store(true, std::memory_order_release);
        /// Keep the terminal-state guard consistent with the forced state, so a later `enterVanished`
        /// (unusual, but not forbidden) is a clean no-op rather than re-storing / re-logging.
        terminal_state_published.store(true, std::memory_order_release);
    }
}

bool CasMountRuntime::noteLeaseLost()
{
    /// `Live -> Reacquiring`, and nothing else. A compare-exchange FROM `Live` leaves every other
    /// state untouched, so a terminal state is never downgraded and a repeated call is a no-op. This is
    /// the only transition the runtime renewal consumer performs, and it needs no lock because of that discipline.
    PoolLifecycle expected = PoolLifecycle::Live;
    if (pool_lifecycle.compare_exchange_strong(
            expected, PoolLifecycle::Reacquiring, std::memory_order_acq_rel, std::memory_order_acquire))
    {
        /// The `since` the lifecycle snapshot reports for `paused(fenced_by_gc)` — the wall-clock instant this
        /// became non-`Live`. Only the winning transition writes it (the guard above), so it is not re-stamped.
        lifecycle_since_wall_s.store(wallClockNowSeconds(), std::memory_order_release);
        /// This transition can be reached while `driver_mutex` or the reacquire serializer lock is
        /// held. Trace-profile collection may allocate and enqueue a stack trace, so the lock-safe
        /// observability path must remain the direct atomic increment.
        ProfileEvents::incrementNoTrace(ProfileEvents::CASMountLeaseLost);
        return true;
    }
    return false;
}

void CasMountRuntime::noteReacquired()
{
    /// `Reacquiring -> Live` on a successful reclaim. A compare-exchange FROM `Reacquiring`
    /// never revives a terminal state ([D3]) and is a no-op if already `Live`.
    PoolLifecycle expected = PoolLifecycle::Reacquiring;
    if (pool_lifecycle.compare_exchange_strong(
            expected, PoolLifecycle::Live, std::memory_order_acq_rel, std::memory_order_acquire))
    {
        /// Back to `Live`: the lifecycle snapshot reports no `since` (0) for a live pool.
        lifecycle_since_wall_s.store(0, std::memory_order_release);
    }
}

void CasMountRuntime::enterVanished(const String & reason)
{
    /// First end wins: a loss latched earlier keeps the pool `Stopping`.
    if (!latchMountEnd(MountEnd::Forgotten) && mountEnd() != MountEnd::Forgotten)
        return;

    if (terminal_state_published.load(std::memory_order_acquire))
        return;

    if (config.vanished_reason_prepare_hook_for_test)
        config.vanished_reason_prepare_hook_for_test();
    String prepared_reason = reason;
    const int64_t prepared_since_wall_s = wallClockNowSeconds();
    static_assert(std::is_nothrow_move_assignable_v<String>);

    bool transitioned = false;
    /// The guard is published LAST. Reason preparation above is the only potentially-throwing step;
    /// once this block begins, the statically-proven-noexcept move and atomic stores either publish
    /// one complete terminal transition or observe that an earlier transition already completed.
    {
        std::lock_guard lock(driver_mutex);
        if (!terminal_state_published.load(std::memory_order_acquire))
        {
            transitioned = true;

            /// Publish the terminal-intent latch (spec §3) before the state store below and while holding
            /// the mutex used by every `driver_cv` terminal predicate.
            vanished_intent.store(true, std::memory_order_release);

            /// Record the reason BEFORE the state's release-store, so a reader that acquire-observes the
            /// terminal state (e.g. `Pool::throwIfLifecycleTerminal`) also observes this string. Written
            /// exactly once.
            vanished_reason = std::move(prepared_reason);

            /// The `since` the lifecycle snapshot reports for the `closed` row — the wall-clock instant
            /// of the terminal transition. Written before the `pool_lifecycle` release-store below, same
            /// as the reason, so an acquire-observer of the terminal state also observes it.
            lifecycle_since_wall_s.store(prepared_since_wall_s, std::memory_order_release);

            /// The driver mutex serializes terminal transitions, and no non-terminal transition can move
            /// a `Vanished` state (their compare-exchanges are keyed on `Live`/`Reacquiring`), so this
            /// value is absorbing.
            pool_lifecycle.store(PoolLifecycle::VanishedForgotten, std::memory_order_release);
            terminal_state_published.store(true, std::memory_order_release);
        }
    }

    driver_cv.notify_all();
    if (!transitioned)
        return;

    ProfileEvents::increment(ProfileEvents::CASDataRootVanished);
    LOG_WARNING(getLogger("CasPool"),
        "Content-addressed pool '{}' entered Vanished(forgotten): {}. The disk stays registered but store-class "
        "access now fails loud with a typed error (truth); restart re-registers the name.",
        server_root_id, reason);
}

void CasMountRuntime::publishVanishedIntent()
{
    /// FORGET's first step: publish the terminal-intent latch WITHOUT settling the state. The renewal thread and
    /// the reacquire consult `vanished_intent` at their step boundaries, so an in-flight reacquire abandons
    /// at its next stage — bounding FORGET's subsequent join to one stage + one backend timeout. The state
    /// store + WARN follow in `enterVanished` (step 6). Idempotent.
    {
        std::lock_guard lock(driver_mutex);
        vanished_intent.store(true, std::memory_order_release);
    }
    driver_cv.notify_all();
}

void CasMountRuntime::beginShutdownForTest()
{
    std::lock_guard lock(driver_mutex);
    workers_stop_requested = true;
    driver_cv.notify_all();
}

bool CasMountRuntime::workersRunningForTest() const
{
    std::lock_guard lock(driver_mutex);
    return workers_started && renewal_worker.joinable();
}

void CasMountRuntime::finishTeardown(const FarewellBlockers & blockers)
{
    stopBackgroundWorkers();

    /// No farewell: our slot holds another body, or our identity may be held by another process.
    if (isLossEnd(mountEnd()))
        return;

    if (!mount_renewer)
        return;

    std::vector<std::string_view> causes;
    if (blockers.drain_failed)
        causes.emplace_back("the shutdown drain failed");
    if (blockers.ref_lane_unsettled)
        causes.emplace_back("a ref-log lane still writing or wedged");
    if (blockers.snapshot_publish_in_flight)
        causes.emplace_back("a snapshot publish in flight");
    if (blockers.cleanup_duty_pending)
        causes.emplace_back("an undrained writer cleanup duty");
    if (mount_renewer->state() == MountLeaseRenewerState::RenewalTerminal)
        causes.emplace_back("the last renewal ended without a confirmed lease");
    if (!causes.empty())
    {
        LOG_WARNING(getLogger("CasPool"),
            "CAS store shutdown without the clean-release marker: {}; the next mount will treat this end as unclean",
            fmt::join(causes, ", "));
        return;
    }
    if (mount_renewer->state() != MountLeaseRenewerState::Active)
        return;
    try
    {
        mount_renewer->release();
    }
    catch (...)
    {
        tryLogCurrentException(getLogger("CasPool"), "CAS mount-lease: release during Pool teardown failed");
    }
}
}
