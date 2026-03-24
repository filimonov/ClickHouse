#pragma once

#include <Coordination/KeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>

#include <memory>


namespace DB
{

/// How the request interacts with other requests in the same session.
enum class RequestMode : uint8_t
{
    /// Serialized through Raft in FIFO order (writes, quorum reads, Auth, Heartbeat, Close).
    Linear,
    /// Must wait for the preceding Linear request to commit (deferred non-quorum read with barrier).
    WaitPrevious,
    /// Note: Reconfig bypasses session classification entirely and is pushed
    /// directly to requests_queue by `KeeperDispatcher::putRequest`, similar to SessionID.
};

/// Where the request is executed.
enum class RequestTarget : uint8_t
{
    /// Sent through Raft consensus (writes, quorum reads, Auth, Heartbeat, Close, Reconfig).
    Raft,
    /// Executed locally against the state machine (non-quorum reads).
    Local,
};

/// Lifecycle state of the request.
enum class RequestState : uint8_t
{
    /// Created but not yet submitted to any execution path.
    Queued,
    /// Submitted to requests_queue (Raft) or to local read execution.
    Submitted,
    /// Waiting for a preceding write to commit (deferred read).
    Deferred,
};

/// Wraps a raw ZooKeeper request with lifecycle tracking, OTel spans, metrics,
/// and optional session affinity. Used for all request paths through the Keeper
/// pipeline — both session-routed (writes, reads) and direct (Reconfig, SessionID,
/// dead session Close, `KeeperOverDispatcher` reads).
class RequestEnvelope
{
public:
    int64_t session_id{0};
    Coordination::ZooKeeperRequestPtr request;
    RequestMode mode{RequestMode::Linear};
    RequestTarget target{RequestTarget::Raft};
    RequestState state{RequestState::Queued};
    int64_t create_time_ms{0};
    bool use_xid_64{false};

    /// Keeps the session alive for the duration of the request.
    /// Null for requests that have no session (SessionID, dead session Close).
    KeeperSessionPtr session;

    /// Safety net: finalize any OTel spans that were initialized but not
    /// explicitly finalized via lifecycle methods.
    ~RequestEnvelope();

    /// Build the `KeeperRequestForSession` struct for submitting to `requests_queue` or local read.
    KeeperRequestForSession buildKeeperRequestForSession() const;

    /// Lifecycle callbacks -- update state, manage metrics and OTel spans.
    void onEnqueued();   /// Queued -> Submitted (pushed to requests_queue, after successful push)
    void onFastPath();   /// Queued -> Submitted (fast local read, no queue)
    void onDeferred();   /// Queued -> Deferred (waiting for preceding write)
    void onReleased();   /// Deferred -> Submitted (preceding write committed)
};

using RequestEnvelopePtr = std::shared_ptr<RequestEnvelope>;

}
