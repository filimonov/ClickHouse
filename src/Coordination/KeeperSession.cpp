#include <Coordination/KeeperSession.h>

#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>


namespace DB
{

KeeperSession::KeeperSession(int64_t session_id, ZooKeeperResponseCallback callback)
    : session_id_(session_id)
    , callback_(std::move(callback))
{
}

bool KeeperSession::canAcceptRequests() const
{
    std::lock_guard lock(mutex_);
    return state_ == State::Active;
}

void KeeperSession::markCloseCommitted()
{
    std::lock_guard lock(mutex_);
    if (state_ == State::Active)
        state_ = State::Finishing;
}

std::optional<KeeperSession::ResponseAction> KeeperSession::prepareResponse(
    const Coordination::ZooKeeperResponsePtr & response,
    Coordination::ZooKeeperRequestPtr request)
{
    std::lock_guard lock(mutex_);

    if (!callback_)
        return std::nullopt;

    ResponseAction action;
    action.request = std::move(request);

    if (response->xid != Coordination::WATCH_XID && response->getOpNum() == Coordination::OpNum::Close)
    {
        action.callback = std::move(*callback_);
        callback_.reset();
        state_ = State::Closed;
        action.detach_after_delivery = true;
    }
    else
    {
        action.callback = *callback_;
    }

    return action;
}

std::optional<ZooKeeperResponseCallback> KeeperSession::beginFinish()
{
    std::lock_guard lock(mutex_);

    unresolved_writes_.clear();
    state_ = State::Closed;

    if (!callback_)
        return std::nullopt;

    auto callback = std::move(*callback_);
    callback_.reset();
    return callback;
}

void KeeperSession::closeSilently()
{
    std::lock_guard lock(mutex_);
    unresolved_writes_.clear();
    callback_.reset();
    state_ = State::Closed;
}

void KeeperSession::addDeferredRead(Coordination::XID write_xid, const KeeperRequestForSession & read_request)
{
    std::lock_guard lock(mutex_);

    if (unresolved_writes_.empty() || unresolved_writes_.back().xid != write_xid)
        unresolved_writes_.push_back(UnresolvedWrite{.xid = write_xid, .deferred_reads = {}});

    unresolved_writes_.back().deferred_reads.push_back(read_request);
}

KeeperRequestsForSessions KeeperSession::takeDeferredReads(Coordination::XID committed_xid)
{
    std::lock_guard lock(mutex_);

    /// Skip stale entries from writes that were never committed (failed batches).
    /// XIDs are monotonically increasing per session, so any entry with xid < committed_xid
    /// belongs to a write that failed and will never commit.
    while (!unresolved_writes_.empty() && unresolved_writes_.front().xid < committed_xid)
        unresolved_writes_.pop_front();

    if (unresolved_writes_.empty() || unresolved_writes_.front().xid != committed_xid)
        return {};

    auto reads = std::move(unresolved_writes_.front().deferred_reads);
    unresolved_writes_.pop_front();
    return reads;
}

}
