#include <Coordination/KeeperSession.h>

#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Common/ZooKeeper/KeeperSpans.h>
#include <Common/ProfileEvents.h>

#include <chrono>


namespace ProfileEvents
{
    extern const Event KeeperStaleRequestsSkipped;
}

namespace DB
{

KeeperSession::KeeperSession(
    int64_t session_id,
    ZooKeeperResponseCallback callback,
    RaftPushFunc raft_push,
    LocalReadFunc local_read,
    bool quorum_reads)
    : session_id_(session_id)
    , callback_(std::move(callback))
    , raft_push_(std::move(raft_push))
    , local_read_(std::move(local_read))
    , quorum_reads_(quorum_reads)
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

std::pair<SessionRequestMode, SessionRequestTarget> KeeperSession::classify(
    const Coordination::ZooKeeperRequestPtr & request) const
{
    if (request->getOpNum() == Coordination::OpNum::Reconfig)
        return {SessionRequestMode::Separator, SessionRequestTarget::Raft};

    if (quorum_reads_ || !request->isReadRequest())
        return {SessionRequestMode::Linear, SessionRequestTarget::Raft};

    /// Non-quorum read: defer behind preceding writes (per-session barrier).
    return {SessionRequestMode::WaitPrevious, SessionRequestTarget::Local};
}

bool KeeperSession::addRequest(const Coordination::ZooKeeperRequestPtr & request, bool use_xid_64)
{
    using namespace std::chrono;
    auto now_ms = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();

    /// Prepare the SessionRequest and classify it.
    auto sr = std::make_shared<SessionRequest>();
    sr->session_id = session_id_;
    sr->request = request;
    sr->create_time_ms = now_ms;
    sr->use_xid_64 = use_xid_64;

    /// We must release the mutex before calling raft_push_ or local_read_ to avoid
    /// lock ordering issues with state machine locks. Prepare what to do under lock,
    /// then execute outside.
    enum class Action { PushRaft, FastLocalRead, Deferred };
    Action action;
    KeeperRequestForSession keeper_req;
    bool is_close = false;

    {
        std::lock_guard lock(mutex_);

        if (state_ != State::Active)
            return false;

        auto [mode, target] = classify(request);
        sr->mode = mode;
        sr->target = target;

        switch (mode)
        {
            case SessionRequestMode::Linear:
            {
                /// Record as unresolved write for barrier tracking.
                /// When the commit callback fires, onWriteCommitted pops this entry
                /// and releases any deferred reads behind it.
                unresolved_writes_.push_back(UnresolvedWrite{.xid = request->xid, .deferred_reads = {}});
                keeper_req = sr->buildKeeperRequestForSession();
                is_close = (request->getOpNum() == Coordination::OpNum::Close);
                action = Action::PushRaft;
                break;
            }
            case SessionRequestMode::Separator:
            {
                /// Separator (Reconfig) goes through a special RAFT path
                /// (KeeperStateMachine::reconfigure) that does NOT trigger the
                /// normal commit callback. Therefore we must NOT push an unresolved
                /// write entry -- it would never be popped, blocking all subsequent
                /// reads in this session indefinitely.
                keeper_req = sr->buildKeeperRequestForSession();
                action = Action::PushRaft;
                break;
            }
            case SessionRequestMode::WaitPrevious:
            {
                if (unresolved_writes_.empty())
                {
                    /// Fast path: no preceding writes, execute immediately.
                    keeper_req = sr->buildKeeperRequestForSession();
                    action = Action::FastLocalRead;
                }
                else
                {
                    /// Defer: attach to the last unresolved write.
                    sr->onDeferred();
                    keeper_req = sr->buildKeeperRequestForSession();
                    unresolved_writes_.back().deferred_reads.push_back(keeper_req);
                    action = Action::Deferred;
                }
                break;
            }
        }
    }

    /// Execute outside the lock.
    switch (action)
    {
        case Action::PushRaft:
            sr->onEnqueued();
            raft_push_(std::move(keeper_req), is_close);
            break;
        case Action::FastLocalRead:
            sr->onFastPath();
            local_read_(keeper_req);
            break;
        case Action::Deferred:
            /// Already stored in unresolved_writes_ above.
            break;
    }

    return true;
}

void KeeperSession::onWriteCommitted(Coordination::XID committed_xid)
{
    KeeperRequestsForSessions pending_reads;
    {
        std::lock_guard lock(mutex_);

        /// Reuse the existing takeDeferredReads logic (skip stale, pop front).
        /// But we can't call takeDeferredReads() since it also locks. Inline it.
        while (!unresolved_writes_.empty() && unresolved_writes_.front().xid < committed_xid)
            unresolved_writes_.pop_front();

        if (!unresolved_writes_.empty() && unresolved_writes_.front().xid == committed_xid)
        {
            pending_reads = std::move(unresolved_writes_.front().deferred_reads);
            unresolved_writes_.pop_front();
        }
    }

    /// Dispatch released reads outside the lock.
    for (auto & read_request : pending_reads)
    {
        /// Finalize the read_wait_for_write OTel span.
        ZooKeeperOpentelemetrySpans::maybeFinalize(
            read_request.request->spans.read_wait_for_write,
            [&]
            {
                return std::vector<OpenTelemetry::SpanAttribute>{
                    {"keeper.operation", Coordination::opNumToString(read_request.request->getOpNum())},
                    {"keeper.session_id", read_request.session_id},
                    {"keeper.xid", read_request.request->xid},
                };
            });

        local_read_(read_request);
    }
}

}
