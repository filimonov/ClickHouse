#pragma once

#include <Coordination/KeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>

#include <memory>


namespace DB
{

/// How the request interacts with other requests in the same session.
enum class SessionRequestMode : uint8_t
{
    /// Serialized through Raft in FIFO order (writes, quorum reads, Auth, Heartbeat, Close).
    Linear,
    /// Must wait for the preceding Linear request to commit (deferred non-quorum read with barrier).
    WaitPrevious,
    /// Breaks write batching: preceding batch must commit before this request is processed.
    /// Does not create an unresolved write entry (Reconfig goes through a special RAFT path
    /// that does not trigger the normal commit callback).
    Separator,
};

/// Where the request is executed.
enum class SessionRequestTarget : uint8_t
{
    /// Sent through Raft consensus (writes, quorum reads, Auth, Heartbeat, Close, Reconfig).
    Raft,
    /// Executed locally against the state machine (non-quorum reads).
    Local,
};

/// Lifecycle state of the request.
enum class SessionRequestState : uint8_t
{
    /// Created but not yet submitted to any execution path.
    Queued,
    /// Submitted to requests_queue (Raft) or to local read execution.
    Submitted,
    /// Waiting for a preceding write to commit (deferred read).
    Deferred,
    /// Response has been generated.
    Completed,
    /// Request was dropped (stale session, shutdown, etc.).
    Cancelled,
};

/// Unified request lifecycle object. Owns the ZooKeeper request, tracks its
/// classification (mode + target), state, and manages OTel spans and metrics
/// through lifecycle callbacks.
///
/// Replaces the old lightweight `struct SessionRequest { KeeperSessionPtr session; }`.
class SessionRequest
{
public:
    int64_t session_id;
    Coordination::ZooKeeperRequestPtr request;
    SessionRequestMode mode;
    SessionRequestTarget target;
    SessionRequestState state{SessionRequestState::Queued};
    int64_t create_time_ms{0};
    bool use_xid_64{false};

    /// Keeps the session alive for the duration of the request.
    KeeperSessionPtr session;

    /// Build the `KeeperRequestForSession` struct for submitting to `requests_queue` or local read.
    KeeperRequestForSession buildKeeperRequestForSession() const;

    /// Lifecycle callbacks -- update state, manage metrics and OTel spans.
    void onEnqueued();   /// Queued -> Submitted (pushed to requests_queue)
    void onFastPath();   /// Queued -> Submitted (fast local read, no queue)
    void onDeferred();   /// Queued -> Deferred (waiting for preceding write)
    void onReleased();   /// Deferred -> Submitted (preceding write committed)
    void onCompleted();  /// -> Completed
    void onCancelled();  /// -> Cancelled
};

}
