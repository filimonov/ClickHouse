#pragma once

#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Coordination/KeeperCommon.h>

#include <deque>
#include <mutex>
#include <optional>


namespace DB
{

struct SessionRequest
{
    KeeperSessionPtr session;
};

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

    KeeperSession(int64_t session_id, ZooKeeperResponseCallback callback);

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

    /// --- Deferred reads (unresolved writes FIFO) ---

    /// Lazily creates a FIFO entry for write_xid if the back doesn't match.
    /// Appends read_request to the back entry's deferred reads.
    void addDeferredRead(Coordination::XID write_xid, const KeeperRequestForSession & read_request);

    /// Skips stale front entries with xid < committed_xid (from failed batches).
    /// Pops front if xid == committed_xid, returns its deferred reads.
    KeeperRequestsForSessions takeDeferredReads(Coordination::XID committed_xid);

private:
    struct UnresolvedWrite
    {
        Coordination::XID xid;
        KeeperRequestsForSessions deferred_reads;
    };

    const int64_t session_id_;
    State state_ = State::Active;
    std::optional<ZooKeeperResponseCallback> callback_;
    std::deque<UnresolvedWrite> unresolved_writes_;
    mutable std::mutex mutex_;
};

}
