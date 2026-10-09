#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasFence.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequestBudget.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEvent.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasMountLoss.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasMountReacquire.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>
#include <Common/ThreadPool.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <set>

namespace DB::Cas
{

class PartWriteTxn;
using PartWriteTxnPtr = std::shared_ptr<PartWriteTxn>;

/// The pool-level lifecycle condition (rev.7 §1) a `Pool` moves through as its shared backing changes
/// underfoot. It is distinct from the storage-level `Constructing/Started/ShutDown` lifecycle (a null
/// published pool -- before `startup`/after `shutdown`) the metadata storage tracks. Ordering of the
/// enumerators is not significant; membership tests do the work.
///   - `Live`             — the steady state; the mount lease is (or was last) held.
///   - `TransientNotLive` — a GC fence of our own body was found; access is uncertain and the reacquire
///                          retries.
///   - `VanishedForgotten` — fully terminal: the disk was decommissioned by `FORGET`. Store-class access
///                          fails loud from here.
///   - `Stopping`         — a latched loss end (`CasMountRuntime::mountEnd`): the slot is no longer ours, so
///                          the mount never opens again; the server stops or the disk stays closed.
enum class PoolLifecycle : uint8_t
{
    Live,
    TransientNotLive,
    VanishedForgotten,
    Stopping,
};

using RuntimeWorkerFactory = std::function<ThreadFromGlobalPool(std::function<void()>)>;

/// Configuration owned by `CasMountRuntime`. `PoolConfig::mountConfig` projects the flat pool settings
/// into this value, keeping the pool's existing configuration interface unchanged while allowing this
/// lower-layer header to describe its own dependencies.
struct MountConfig
{
    std::chrono::milliseconds mount_lease_ttl_ms{30000};
    std::chrono::milliseconds mount_renew_period{10000};
    /// When false, tests drive `renewWatermarkOnce` explicitly. In production this flag enables both
    /// the merged mount-lease/build-watermark heartbeat and self-remount recovery.
    bool background_watermark = false;
    std::function<uint64_t()> boot_ms_fn = {};
    std::function<void(uint64_t)> wait_sleep_fn = {};
    RuntimeWorkerFactory worker_factory = {};
    /// Deterministic test interposition at the top of the renewal loop, before it acquires `driver_mutex`.
    std::function<void()> renewal_before_driver_lock_hook_for_test = {};
    /// Deterministic test interposition after a due background renewal has reserved the renewer, before
    /// renewer/backend I/O starts.
    std::function<void()> renewal_admitted_hook_for_test = {};
    /// Deterministic failure injection at the vanished-reason preparation boundary.
    std::function<void()> vanished_reason_prepare_hook_for_test = {};
    /// Test-only override of the renewal's liveness predicate, for exact pre/post-send gate
    /// interleavings. FALSE ends the renewal exactly as a lost fence does.
    std::function<bool()> renewal_live_for_test = {};
    /// Deterministic test interposition on the renewing thread right after `finishRenewal` released the
    /// renewer, before the result is consumed. Runs without `driver_mutex`.
    std::function<void(const MountRenewResult &)> renewal_finished_hook_for_test = {};
    /// How a loss end reaches beyond this mount; `disk_name` names the disk in the line that says so.
    MountLossReaction loss_reaction = MountLossReaction::StopServer;
    String disk_name = {};
};

/// Local, in-memory write fence. It is deliberately not checked by reading the object store for every
/// write: the `MountLeaseRenewer` is the sole lease reader/renewer. A successful renewal computes
/// `deadline_boot_ms` from its own confirmed request's pre-I/O `CLOCK_BOOTTIME` anchor plus the lease
/// TTL, never from the durable `expires_at_ms` stamp, which is a writer-stamped diagnostic only; a
/// foreign owner, newer `writer_epoch`, or failed renewal latches `lost`. Mutable operations are
/// allowed only while the latch is clear and the local deadline has not passed. The `writer_epoch` is
/// the durable fencing token.
///
/// The fence uses `CLOCK_BOOTTIME`, not `CLOCK_MONOTONIC`: monotonic time does not advance while a VM is
/// suspended, so a resumed sleeper would compute the same "not yet expired" verdict it had before the nap
/// even though wall time (and the GC leader's fence-out) moved far ahead — it could mutate the shared state
/// under a live writer.
/// `CLOCK_BOOTTIME` includes suspend time, so a resumed sleeper sees its fence expired.
/// Container pause is already safe under either clock (the process is frozen, so no local check runs).
struct MountFence
{
    /// Until something arms a real lease deadline, the permissive default allows mutations. UINT64_MAX =
    /// unarmed (never expires); otherwise a CLOCK_BOOTTIME-milliseconds instant.
    std::atomic<uint64_t> deadline_boot_ms{std::numeric_limits<uint64_t>::max()};
    std::atomic<bool> lost{false};
};

/// The current logical renewal of the mount lease so far, or the last one once it ended.
struct RenewalProgress
{
    bool in_progress = false;
    uint32_t attempts_sent = 0;
    std::optional<String> last_error;
};

/// Why a `Live` mount refuses lease-bound operations, derived from the deadline and the clock:
/// the lease left is not more than a conditional change's reserve, or the deadline has passed.
enum class LeasePause : uint8_t { None, RenewalOverdue, LeaseExpired };
struct LeasePauseState
{
    LeasePause pause = LeasePause::None;
    uint64_t since_boot_ms = 0;   /// when the threshold was crossed; 0 for `None`
};

/// What the teardown found unsettled. The farewell certifies that no write of this incarnation lands
/// after it, so any field set withholds it.
struct FarewellBlockers
{
    bool drain_failed = false;
    bool ref_lane_unsettled = false;
    bool snapshot_publish_in_flight = false;
    bool cleanup_duty_pending = false;
};

/// Owns the live writer-incarnation mechanics shared by the pool's mount and recovery orchestration:
/// the `MountLeaseRenewer`, local `MountFence`, build watermark and in-flight build registry,
/// `live_writer_epoch`, unclean-boundary marker, and the renewal thread. `Pool` retains the higher-level
/// claim/recovery sequence and its `remount_mutex`; in particular, the runtime does not acquire or own
/// the ref-ledger locks. The runtime receives its backend, layout, configuration, event sink, request
/// budget, and a callback that runs one pass of the reacquire, so it has no `Pool` back-reference.
/// `Pool` delegates preserve the existing callers and test seams.
class CasMountRuntime
{
public:
    CasMountRuntime(
        BackendPtr backend_ptr_,
        /// The two planes the `MountLeaseRenewer` runs on: bounded renewals under the mount fence; the
        /// background renewal and the farewell on the open-fence lease plane. Owned by `Pool` and
        /// outliving this runtime.
        CasRequests & mount_requests_,
        CasRequests & lease_requests_,
        const Layout & layout_,
        MountConfig config_,
        String server_root_id_,
        const CasEventSink & event_sink_,
        CasRequestBudget cas_request_budget_,
        /// One pass of the reacquire step machine over a GC fence of our own body.
        std::function<ReacquireOutcome(ReacquireStep &)> reacquire_attempt_);

    /// ---- per-server watermark and identity ----
    /// `process_epoch` is random and nonzero for this pool incarnation. GC compares it for equality,
    /// never ordering; a different value means that the previous writer incarnation is no longer live.
    uint64_t epoch() const { return process_epoch.load(std::memory_order_acquire); }
    uint64_t writerEpoch() const { return process_epoch.load(std::memory_order_acquire); }
    /// The GC floor: the oldest in-flight build_seq, or next_build_seq when no build is active (so a
    /// quiescent server's watermark floor advances to the next-to-be-allocated seq). Locks builds_mutex.
    uint64_t minActive();
    /// Test/assertion accessor for the next-to-allocate build_seq under the lock.
    uint64_t peekNextBuildSeq();
    /// Renew the merged mount heartbeat once, including its build-watermark floor. A read-only runtime
    /// has no renewer and fails with a logical exception rather than fabricating a heartbeat.
    void renewWatermarkOnce();
    /// Takes only its own lock, so a renewal stuck in a request never holds up the reader.
    RenewalProgress renewalProgress() const;
    /// The body this server last confirmed in its slot; empty before the first claim. Takes only
    /// `renewal_progress_mutex`, so a renewal stuck in a request never holds up the reader.
    std::optional<MountLease> confirmedMountBody() const;

    /// ---- local write fence ----
    /// Return whether a mutable operation may start under the locally observed lease state.
    bool mayMutate() const;
    /// Permanently latch the local fence as lost for this runtime incarnation.
    void tripMountLost();

    /// ---- the end of the mount ----
    /// Latches `end` unless an end is already latched (first end wins, lock-free), closes the fence for
    /// good and, for a loss end, moves the lifecycle to `Stopping`. True only for the winner, which alone
    /// calls `reactToMountEnd`. Safe under any lock: it takes none.
    bool latchMountEnd(MountEnd end) noexcept;
    MountEnd mountEnd() const { return mount_end.load(std::memory_order_seq_cst); }
    /// Says what ended the mount and acts on it by `MountConfig::loss_reaction`: the server-stop request
    /// before the `ERROR` line, so a throwing log cannot cancel the stop; for an end that must not run a
    /// shutdown, `LOG_FATAL` and the process ends at once.
    void reactToMountEnd(MountEnd end, const std::optional<MountLease> & holder, std::string_view detail) noexcept;
    /// Publish the BOOTTIME deadline from a successful lease renewal.
    void setMountDeadline(uint64_t deadline_boot_ms);
    /// Arm a new lease incarnation and clear any loss latched for the prior incarnation.
    void armMountFence(uint64_t deadline_boot_ms);
    /// Test-only interposition at the publication boundary between the re-armed generation and the
    /// live fence. A caller admitted from this hook must be refused: the old generation is already
    /// dead, while the new generation is not live until `lost` is cleared.
    void setArmMountFenceInterpositionHookForTest(std::function<void()> hook)
    {
        arm_mount_fence_interposition_hook_for_test = std::move(hook);
    }
    /// The fence clock: `CLOCK_BOOTTIME` in milliseconds (includes VM-suspend time, unlike
    /// CLOCK_MONOTONIC — see `MountFence`). Consults the injected `config.boot_ms_fn` if set (tests),
    /// otherwise `bootMs`.
    uint64_t bootMsNow() const;
    /// The real boot clock: `CLOCK_BOOTTIME` in milliseconds. Static so tests can compose it.
    static uint64_t bootMs();

    /// ---- fence-generation admission (rev.7 [C2]/[D1]) ----
    /// Bumped by EVERY `tripMountLost` (a fence loss) and EVERY `armMountFence` (a re-arm -- a fresh
    /// lease incarnation, e.g. after a self-remount). A durable-effect caller captures this value once
    /// at admission and compares it again immediately before its durable backend call: a DIFFERENT
    /// value means the lease incarnation moved from under it since admission -- even when the fence
    /// happens to be live again under a brand-new incarnation, the caller's write is stale and must not
    /// land. See `checkFenceOrThrow`.
    uint64_t fenceGeneration() const { return fence_generation.load(std::memory_order_acquire); }

    /// Fence-generation admission check for every durable CAS/PUT/DELETE (the plain-object surface,
    /// staging-buffer finalize): the caller captures `fenceGeneration()` once at admission and passes it
    /// back here immediately before its durable backend call -- and again before EVERY conditional-retry
    /// iteration, not just the first attempt. Throws the typed transient refusal
    /// (`throwCasTransientUnavailable`) when the fence is not currently held or the generation moved since
    /// admission; the caller's write must never reach the backend in either case.
    void checkFenceOrThrow(uint64_t admitted_generation) const;

    /// ---- pool lifecycle condition (rev.7 §1, spec §§1-3); enum at namespace scope below ----
    /// Atomic read of the current lifecycle (acquire).
    PoolLifecycle lifecycle() const { return pool_lifecycle.load(std::memory_order_acquire); }
    /// Whether the pool reached `VanishedForgotten`.
    bool isVanished() const;
    /// Whether the terminal-intent latch (`vanished_intent`) is published — set by `enterVanished`, OR
    /// EARLY (spec §5 step 1) by FORGET's `publishVanishedIntent`. This is the EARLIEST terminal signal: it can already be true
    /// while the state is still pre-terminal (mid-FORGET). Consulted alongside `isVanished()` by every
    /// background worker that must self-exit the moment the pool is (being driven) terminal — the renewal
    /// thread and the GC scheduler.
    bool vanishedIntentPublished() const { return vanished_intent.load(std::memory_order_acquire); }

    /// Non-terminal lease-loss transition: `Live -> TransientNotLive`. Idempotent and lock-free; a
    /// compare-exchange FROM `Live` only, so it never downgrades a terminal state. `tripMountLost`
    /// calls this (the lease-loss primitive). Returns true only to the compare-exchange winner, which owns
    /// the one-per-loss metric.
    bool noteLeaseLost();
    /// Non-terminal recovery transition: `TransientNotLive -> Live`. Called after a self-remount
    /// reclaimed a fresh incarnation. A compare-exchange FROM `TransientNotLive` only, so it NEVER
    /// revives a terminal state ([D3]).
    void noteRemounted();

    /// Test seam: force the lifecycle condition directly to `lc`, bypassing the natural transition
    /// preconditions (used by the operation-gate tests to pin each class × state cell without driving a
    /// full FORGET). For `VanishedForgotten` it also latches `vanished_intent`, so the
    /// forced state is indistinguishable from a naturally-reached one. Never used in production.
    void setLifecycleForTest(PoolLifecycle lc);

    /// Publish the terminal-intent latch (`vanished_intent`) WITHOUT settling the lifecycle state. This is
    /// spec §5 step 1 of `SYSTEM CAS FORGET`: publishing the latch FIRST makes a reacquire in flight
    /// abandon at its next stage and the renewal thread exit, so FORGET's subsequent join is bounded to one
    /// stage + one backend timeout. The state store + WARN happen later, in `enterVanished` at step 6.
    /// Idempotent. Publication is serialized by `driver_mutex` and followed by a condition-variable
    /// notification, so a worker cannot miss the terminal edge between its predicate sample and wait.
    void publishVanishedIntent();

    /// One-way transition to `VanishedForgotten` (spec §3). Publishes the terminal-intent latch if it is not
    /// already published, records `reason`, stores the state, then emits ONE WARN + one
    /// `CASDataRootVanished` ProfileEvent. Idempotent: the first terminal STATE transition wins (a
    /// dedicated latch keyed separately from `vanished_intent`, because FORGET publishes that intent latch
    /// early at step 1). `reason` is surfaced verbatim in the [D5] error message (see `vanishedReason`).
    /// Must be called under the caller's remount serialization (Pool::remount_mutex).
    void enterVanished(const String & reason);

    /// The reason string recorded by the winning `enterVanished`, or empty when none has run (a
    /// forced-for-test terminal state, or a non-terminal pool). `Pool::throwIfLifecycleTerminal` reads it
    /// to build the `VanishedForgotten` [D5] message (which carries the operator's decommission timestamp
    /// authored by `forgetDisk`). Safe to read only AFTER observing a terminal state via `lifecycle()`
    /// (acquire): the reason is written once, before the state's release-store, so a reader that
    /// acquire-observes the terminal state also observes the reason (release/acquire handoff).
    const String & vanishedReason() const { return vanished_reason; }

    /// Wall-clock second (`system_clock`, seconds since epoch) at which the pool ENTERED its current
    /// non-`Live` lifecycle state, or 0 while `Live`. This is the `since` the non-gated
    /// `system.cas_mounts` lifecycle snapshot (spec §7) reports. Written (release) at each
    /// lifecycle edge — `noteLeaseLost`/`enterVanished` set it to now, `noteRemounted`
    /// clears it to 0 — and by `setLifecycleForTest`, so a forced state carries a `since` indistinguishable
    /// from a naturally-reached one.
    ///
    /// Ordering vs the `pool_lifecycle` transition it accompanies: the TERMINAL edge (`enterVanished`)
    /// publishes this store BEFORE the state store, so a reader that acquire-observes a
    /// terminal state is guaranteed (release/acquire handoff) to observe this timestamp. The lock-free
    /// lease-loss/remount edges (`noteLeaseLost`, `noteRemounted`) stamp it in the compare-exchange's
    /// SUCCESS branch — after the CAS — because they may run on an already-terminal pool (`noteLeaseLost` is
    /// called before the caller's `isVanished()` gate), where a pre-CAS stamp would clobber the terminal
    /// `since`; a reader may therefore momentarily observe a just-entered `paused` with `since` not yet
    /// updated, a benign introspection artifact that converges within nanoseconds.
    time_t lifecycleSinceWallS() const
    {
        return static_cast<time_t>(lifecycle_since_wall_s.load(std::memory_order_acquire));
    }

    /// Extends `mayMutate` with a remaining-budget check. A ref-log attempt is refused unless the
    /// current lease has room for its configured timeout and safety margin, so work is not started when
    /// it cannot plausibly finish before the fence expires.
    bool refAppendFenceOk() const;

    /// The confirmed deadline, once it has passed while the pool is `Live` and the fence is not lost: the
    /// lease expired and the renewal keeps retrying. Empty otherwise. I/O-free.
    std::optional<uint64_t> leaseExpiredAtBootMs() const;
    /// I/O-free; `None` unless the pool is `Live` and the fence is not lost.
    LeasePauseState leasePause() const;
    /// Two attempt envelopes and the safety margin: the reserve `refAppendFenceOk` admits against.
    uint64_t conditionalReserveMs() const;

    /// TRUE once the pool has reached — or is being driven toward — a state on which the renewal thread
    /// must stop: a published terminal `Vanished` intent (`vanished_intent` — set early by
    /// FORGET, or by `enterVanished`, and already subsuming the settled `Vanished` state since it is
    /// published before the state store) OR a latched mount end (`mountEnd`). Consulted by the renewal
    /// thread at every wait. The GC scheduler, through `Pool`, applies the first only: it keeps running on
    /// a mount end.
    bool remountTerminal() const
    {
        return vanished_intent.load(std::memory_order_acquire) || mountEnd() != MountEnd::None;
    }

    /// The inter-attempt sleep the mount plane runs on. A plain sleep would hold a parked or stopping
    /// renewal for the whole capped backoff; this one wakes on the same stop signal the workers watch.
    /// It shortens a stop, not a fence loss: the fence cannot see a stop request, so a woken operation
    /// still reissues unless its own liveness predicate refuses.
    void sleepInterruptibly(uint64_t ms);

    /// The mount fence's admission verdict, as `Fence::admit` expects it: may a request admitted under
    /// `admitted_generation`, still expected to be running `needed_ms` from now, proceed?
    /// `LostOrRearmed` when the fence is latched lost or a fresh lease incarnation replaced the one the
    /// caller was admitted under; `NoBudget` when the live lease has no room left for `needed_ms` plus
    /// the safety margin, so nothing is begun that could land after this node's fence may be gone.
    Fence::Admit admit(uint64_t admitted_generation, uint64_t needed_ms) const;

    /// The `writer_epoch` of the live mount incarnation. Bumped by the reacquire after a GC fence-out — a
    /// `PartWriteTxn` minted under an older epoch fails closed on its next step.
    uint64_t liveWriterEpoch() const { return live_writer_epoch.load(std::memory_order_acquire); }

    /// ---- build registry ----
    /// Allocate a strictly-increasing `build_seq` and add it to the active set. A sequence is never
    /// reused or lowered, which lets the GC watermark advance monotonically.
    uint64_t allocateBuildSeq();
    /// Register the in-flight build so `dropNamespace`'s post-durable cancellation can reach it (weak_ptr).
    void registerInflightBuild(uint64_t seq, const PartWriteTxnPtr & build);
    /// Remove a build_seq from the active set + inflight map; idempotent (safe from publish/abandon/dtor).
    void retireBuildSeq(uint64_t seq);
    /// After the namespace-removal transaction is durable, cancel every in-flight build targeting `ns`.
    /// Live shared pointers are collected under `builds_mutex` and cancelled after releasing it, because
    /// cancellation may take a different path and must not run under the registry lock.
    void cancelInflightBuildsForNamespace(const RootNamespace & ns);

    /// ---- process epoch (identity) ----
    /// Mint the random nonzero process identity used by GC's equality check.
    void mintRandomProcessEpoch();
    /// Set `process_epoch` to the durable `writer_epoch`. The caller supplies the memory order because
    /// the initial writable claim and a later self-remount have different publication requirements.
    void setProcessEpoch(uint64_t v, std::memory_order order);
    /// Publish the live-incarnation `live_writer_epoch` with release ordering.
    void setLiveWriterEpoch(uint64_t v);

    /// ---- mount-lease renewer and persistent workers ----
    void installRenewer(UInt128 our_uuid, uint64_t writer_epoch, const std::function<uint64_t()> & now_ms);
    uint64_t startRenewer(const MountClaimed & claimed);
    uint64_t renewRenewerForStartupOnce();
    /// The reacquire's renewal before it opens: repeated until definitive on the lease plane, because a
    /// lease that ran out while the reacquire waited can only be regained by a renewal that outlives it.
    MountRenewResult renewForReacquireOnce();
    /// Whether a reacquire may go on: no stop, no FORGET intent, no latched end.
    bool reacquireLive() const;
    /// Opens the fence for the reacquired epoch, unless an end, a stop or FORGET intent is set, the pool
    /// is not paused, or the lease left at `deadline_boot_ms` cannot cover a conditional change.
    bool openAfterReacquire(uint64_t deadline_boot_ms);
    /// Without workers: one pass of the owed reacquire, or of the step a previous pass left unsettled.
    ReacquireOutcome runReacquireForTest();
    void renewerReset();
    void startBackgroundWorkers(std::chrono::milliseconds period);
    void stopBackgroundWorkers();
    void beginShutdownForTest();
    bool workersRunningForTest() const;

    /// Whether a lease ending at `deadline_boot_ms` still leaves room for the next renewal: the safe deadline
    /// is more than a renewal window away, where the window is two attempt envelopes plus, with background
    /// renewal, one period. Strict, so a horizon that fits exactly is refused.
    bool renewalWindowFits(uint64_t deadline_boot_ms) const;

    /// Join the renewal thread, then write the clean farewell only if `blockers` is empty, the mount
    /// did not end in a loss, and the renewer is `Active`; otherwise log every cause and write none.
    void finishTeardown(const FarewellBlockers & blockers);

    /// Sleep through the injected test hook when present; otherwise use the production thread sleep.
    /// `Pool` claim observation and materialization grace waits share this seam so tests control both.
    void waitSleep(uint64_t ms) const;
    /// Swap the wait hook after construction -- a test that must change what a wait DOES partway
    /// through a scenario (e.g. driving a second incarnation's renewal from inside the observed
    /// incarnation's own poll) cannot express that through `PoolConfig::wait_sleep_fn` alone, since
    /// that value is fixed at open time. Unsynchronized against `waitSleep`'s `const` read of the same
    /// field: safe only called from the test's own thread before any worker is running (no persistent
    /// renewal/remount worker reads `config.wait_sleep_fn` concurrently with this write).
    void setWaitSleepForTest(std::function<void(uint64_t)> fn) { config.wait_sleep_fn = std::move(fn); }

    /// Forward renewer events to the injected sink. The sink is held by reference so it observes the
    /// owning pool's current event routing for the runtime's entire lifetime.
    void emitEvent(CasEvent && e) const { if (event_sink) event_sink(std::move(e)); }

private:
    /// Who drives a renewal: the open before the workers start, a test without workers, the renewal
    /// thread's cadence, or the reacquire's renewal before it opens.
    enum class RenewalKind : uint8_t
    {
        Startup,
        Direct,
        Worker,
        Reacquire,
    };

    /// Reserves the `Active` renewer for one renewal: one renewal at a time, `Direct` never with
    /// background renewal configured, and `Startup`/`Direct` never while the renewal thread runs.
    MountLeaseRenewer * beginRenewal(RenewalKind kind);
    /// Releases the renewer and publishes what the renewal established: a committed deadline, or, for a
    /// terminal renewal without a latched end, the tripped fence and the reacquire owed for our own fence.
    void finishRenewal(RenewalKind kind, const MountRenewResult & result, const MountLeaseRenewer * renewer);
    MountRenewResult renewRenewerOnce(RenewalKind kind);
    MountRenewOperationEnvironment renewalEnvironment(bool worker_call);
    void consumeRenewResult(const MountRenewResult & result, bool propagate_failure);
    /// Publish a committed renewal's deadline, its start plus the lease. Requires `driver_mutex`.
    void publishRenewedDeadline(uint64_t attempt_start_boot_ms);
    void publishConfirmedBody(const std::optional<MountLease> & body);
    /// One `WARNING` per expiry, at the first renewal attempt after it.
    void warnOnceIfLeaseExpired() noexcept;
    /// The background renewal's wait after a credential refusal. Wakes on stop; `MountConfig::wait_sleep_fn`
    /// replaces it in tests.
    void waitBeforeNextRenewalCall(uint64_t ms);
    void renewalLoop();
    /// Runs the reacquire of `certificate` on the renewal thread until it opens, ends or is abandoned,
    /// about one second between passes.
    ReacquireOutcome reacquireUntilSettled(ReacquireCertificate certificate);
    /// Wakes on stop and on a terminal pool; `MountConfig::wait_sleep_fn` replaces it in tests.
    void waitBeforeNextReacquireStep(uint64_t ms);
    /// Whether a renewal found a GC fence of our own body that no reacquire has taken yet.
    bool reacquireOwed() const;
    ThreadFromGlobalPool makeWorker(std::function<void()> body);
    /// The renewal's liveness: facts the mount fence cannot see -- a shutdown request, a pool that left
    /// `Live`. FALSE ends the renewal.
    bool renewalLive(bool worker_call) const;
    /// Whether this node has already been asked to stop. Sampled ONCE, before the write, so a refusal
    /// caused by the stop cannot be mistaken for one that preceded it.
    bool renewalCancelled() const;
    void tripFenceWithoutOperationalLoss();

    /// ---- injected environment (no `Pool` back-reference); initialized first, in this order ----
    BackendPtr backend_ptr;
    CasRequests & mount_requests;
    CasRequests & lease_requests;
    const Layout & layout;
    MountConfig config;
    String server_root_id;
    const CasEventSink & event_sink;
    CasRequestBudget cas_request_budget;
    std::function<ReacquireOutcome(ReacquireStep &)> reacquire_attempt;

    /// Per-server build watermark. `process_epoch` is a random
    /// nonzero u64 minted once at open: GC checks it for EQUALITY (an object stamped with a different
    /// epoch is from a dead incarnation), never for ordering. next_build_seq is a strictly-increasing
    /// per-process counter (monotonicity is load-bearing — a seq is never reused or lowered);
    /// active_build_seqs holds the seqs of in-flight builds, so `minActive` yields the GC floor. The floor
    /// is published by the merged `mount_renewer`
    /// beat (there is no standalone watermark object anymore). ATOMIC because a self-remount re-stamps it
    /// (kept equal to `live_writer_epoch`) from the runtime-owned remount worker while `epoch`/`writerEpoch`
    /// may observe it; the ref-lane hot readers were moved to `liveWriterEpoch`, so this now backs only
    /// the identity accessors.
    std::atomic<uint64_t> process_epoch{0};
    std::mutex builds_mutex;
    uint64_t next_build_seq = 1;
    std::set<uint64_t> active_build_seqs;
    /// In-flight builds keyed by `build_seq`. `dropNamespace` upgrades these weak pointers only after its
    /// removal transaction is durable and cancels those targeting the removed namespace. The wiring owns
    /// the shared pointers, so an expired entry is simply skipped. Guarded by `builds_mutex`.
    std::map<uint64_t, std::weak_ptr<PartWriteTxn>> inflight_builds;

    /// Synchronous mount-lease protocol state. Constructed and started on a writable open after the
    /// owner/epoch/mount startup protocol; the runtime-owned renewal worker is its sole background
    /// driver and publishes successful anchors or terminal loss into the local fence. After both
    /// workers join, teardown releases an `Active` renewer so a same-server reopen can reclaim
    /// immediately. Null on a read-only open.
    std::unique_ptr<MountLeaseRenewer> mount_renewer;

    std::atomic<uint64_t> live_writer_epoch{0};

    /// One mutex/condition pair owns driver admission, worker lifecycle, cadence, the owed reacquire, and
    /// terminal predicates paired with `driver_cv`. It is never held across renewer/backend calls, the
    /// reacquire, logging, or joins.
    mutable std::mutex driver_mutex;
    mutable std::condition_variable driver_cv;
    /// Set while one renewal or `startRenewer` holds the renewer; the renewer is not replaced or reset meanwhile.
    bool renewer_in_use = false;
    bool workers_starting = false;
    bool workers_started = false;
    bool worker_loops_released = false;
    bool workers_stop_requested = false;
    std::chrono::milliseconds renewal_period{0};
    ThreadFromGlobalPool renewal_worker;
    /// A leaf lock, never held across I/O.
    mutable std::mutex renewal_progress_mutex;
    RenewalProgress renewal_progress;
    std::optional<MountLease> confirmed_body;   /// guarded by `renewal_progress_mutex`
    /// The certificate of a GC fence of our own body that a renewal found, until a reacquire takes it.
    std::optional<ReacquireCertificate> owed_reacquire;   /// guarded by `driver_mutex`
    /// The step `runReacquireForTest` left unsettled; only the test thread touches it.
    std::optional<ReacquireStep> reacquire_step_for_test;

    /// Local write fence. The unarmed default (`deadline_boot_ms = UINT64_MAX`, `lost = false`) permits
    /// mutation until a renewer supplies a real lease deadline or reports that the lease was lost. This
    /// is the gate at the ref-append mutation chokepoint.
    MountFence mount_fence;

    /// Fence-generation token (rev.7 [C2]): bumped by `tripMountLost` and `armMountFence`. See
    /// `fenceGeneration`/`checkFenceOrThrow`.
    std::atomic<uint64_t> fence_generation{0};
    std::atomic<bool> lease_expiry_warned{false};
    std::function<void()> arm_mount_fence_interposition_hook_for_test;
    /// See `latchMountEnd`. Read `seq_cst` by `armMountFence`, which must see a latch that raced its re-arm.
    std::atomic<MountEnd> mount_end{MountEnd::None};

    /// The pool lifecycle condition (rev.7 §1). Starts `Live`. Non-terminal transitions
    /// (`noteLeaseLost`/`noteRemounted`) are lock-free compare-exchanges guarded by their exact
    /// predecessor state; terminal predicate publication is serialized with worker waits by
    /// `driver_mutex`, while the caller's `Pool::remount_mutex` serializes the higher-level remount flow.
    std::atomic<PoolLifecycle> pool_lifecycle{PoolLifecycle::Live};
    /// Terminal-intent latch (spec §3), published before the state store by `enterVanished`, or EARLY
    /// (step 1) by `publishVanishedIntent` for FORGET. Consulted by `remountTerminal` and `reacquireLive`, so
    /// no claim/allocate/write happens after the pool is (being driven) terminal.
    std::atomic<bool> vanished_intent{false};
    /// Idempotency guard for the terminal STATE transition (`enterVanished`'s body). Distinct from
    /// `vanished_intent`: FORGET publishes that intent latch at step 1, so it can no longer serve as the
    /// "state transition already done" flag. Published last, after the no-throw reason move and terminal
    /// stores complete under `driver_mutex`; a preparation exception therefore leaves it clear so a later
    /// `enterVanished` can retry the whole transition.
    std::atomic<bool> terminal_state_published{false};
    /// The reason recorded by the winning `enterVanished` (see `vanishedReason`). Written once, BEFORE the
    /// `pool_lifecycle` release-store, and immutable thereafter — so a reader that acquire-observes a
    /// terminal state also observes this string. Empty when no terminal transition has run.
    String vanished_reason;

    /// Wall-clock second at which the current non-`Live` lifecycle state was entered; 0 while `Live` (see
    /// `lifecycleSinceWallS`). Set at every lifecycle edge with a release-store ordered before the
    /// `pool_lifecycle` transition it accompanies.
    std::atomic<int64_t> lifecycle_since_wall_s{0};
};

}
