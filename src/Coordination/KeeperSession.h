#pragma once

#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Coordination/KeeperCommon.h>
#include <Coordination/RequestEnvelope.h>

#include <deque>
#include <mutex>
#include <optional>


namespace DB
{

class KeeperSession
{
public:
    enum class State : uint8_t
    {
        Active,
        Finishing,
        Closed,
    };

    struct ResponseAction
    {
        ZooKeeperResponseCallback callback;
        Coordination::ZooKeeperRequestPtr request;
        bool detach_after_delivery{false};
    };

    /// Callback types for request routing (injected by KeeperDispatcher at session creation).
    /// `RaftPushFunc` wraps the push to `requests_queue` with Close-vs-timeout logic.
    using RaftPushFunc = std::function<bool(KeeperRequestForSession &&, bool /*is_close*/)>;
    /// `LocalReadFunc` wraps `server->putLocalReadRequest` plus the `isLeaderAlive`
    /// check and error response fallback.
    using LocalReadFunc = std::function<void(const KeeperRequestForSession &)>;
    /// `FailReadFunc` wraps `addErrorResponses` for deferred reads whose write failed.
    using FailReadFunc = std::function<void(const KeeperRequestForSession &, Coordination::Error)>;

    KeeperSession(int64_t session_id, ZooKeeperResponseCallback callback,
                  RaftPushFunc raft_push, LocalReadFunc local_read,
                  FailReadFunc fail_read, bool quorum_reads);

    int64_t getSessionID() const { return session_id_; }

    /// True only when Active.
    bool canAcceptRequests() const;

    /// Active -> Finishing. Called when Close commits through RAFT.
    void markCloseCommitted();

    /// Extract callback for response delivery.
    /// Close: moves callback out, transitions -> Closed, sets detach_after_delivery.
    /// Non-Close: copies callback.
    /// Returns nullopt if already Closed or no callback.
    std::optional<ResponseAction> prepareResponse(
        const Coordination::ZooKeeperResponsePtr & response,
        Coordination::ZooKeeperRequestPtr request);

    /// Session expiry: extract callback, drain all deferred reads, -> Closed.
    /// Returns nullopt if already Closed or no callback.
    std::optional<ZooKeeperResponseCallback> beginFinish();

    /// Shutdown: clear state silently without delivering, -> Closed.
    void closeSilently();

    /// --- Request classification and routing ---

    /// Classifies the request (Linear/WaitPrevious) and routes it:
    /// - Linear -> pushes to Raft queue via `raft_push_`
    /// - WaitPrevious with preceding writes -> defers in FIFO
    /// - WaitPrevious with no preceding writes -> fast-path local read via `local_read_`
    ///
    /// Called from `KeeperDispatcher::putRequest` (TCP handler thread).
    /// Returns false if the session cannot accept requests.
    /// May throw TIMEOUT_EXCEEDED if Raft queue is full.
    bool addRequest(const Coordination::ZooKeeperRequestPtr & request, bool use_xid_64);

    /// Called from the commit callback when a write/exclusive request commits.
    /// Releases all WaitPrevious reads that were deferred behind this write,
    /// executing them via `local_read_`.
    void onWriteCommitted(Coordination::XID committed_xid);

    /// Called when a write fails (batch rejection, timeout, memory limit).
    /// Pops the unresolved write entry and sends error responses for its deferred reads.
    void onWriteFailed(Coordination::XID failed_xid, Coordination::Error error);

private:
    struct UnresolvedWrite
    {
        Coordination::XID xid;
        KeeperRequestsForSessions deferred_reads;
    };

    /// Classify the request into mode + target.
    std::pair<RequestMode, RequestTarget> classify(
        const Coordination::ZooKeeperRequestPtr & request) const;

    /// Walk the FIFO from front, pop stale entries (failed batches), return
    /// deferred reads for the entry matching committed_xid. Must be called
    /// with mutex_ held.
    KeeperRequestsForSessions popDeferredReads(Coordination::XID committed_xid);

    const int64_t session_id_;
    State state_ = State::Active;
    std::optional<ZooKeeperResponseCallback> callback_;
    std::deque<UnresolvedWrite> unresolved_writes_;
    mutable std::mutex mutex_;

    /// Injected routing functions.
    RaftPushFunc raft_push_;
    LocalReadFunc local_read_;
    FailReadFunc fail_read_;
    bool quorum_reads_;
};

}
