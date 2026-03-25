#include <Coordination/KeeperSession.h>

#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Common/ZooKeeper/KeeperSpans.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

#include <chrono>


namespace ProfileEvents
{
    extern const Event KeeperStaleRequestsSkipped;
}

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB
{

KeeperSession::KeeperSession(
    int64_t session_id,
    ZooKeeperResponseCallback callback,
    const Callbacks & callbacks)
    : session_id_(session_id)
    , callback_(std::move(callback))
    , callbacks_(callbacks)
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

KeeperRequestsForSessions KeeperSession::extractDeferredReads(Coordination::XID target_xid)
{
    /// Find and erase the entry matching target_xid without disturbing other entries.
    /// Non-matching entries may belong to writes that are still in-flight (not yet
    /// committed or failed), so we must not pop them.
    ///
    /// This is O(n) scan of the deque, but unresolved_writes_ is typically very small
    /// (bounded by in-flight writes per session, usually 1-3).
    for (auto it = unresolved_writes_.begin(); it != unresolved_writes_.end(); ++it)
    {
        if (it->xid == target_xid)
        {
            auto reads = std::move(it->deferred_reads);
            unresolved_writes_.erase(it);
            return reads;
        }
    }

    return {};
}

std::pair<RequestMode, RequestTarget> KeeperSession::classify(
    const Coordination::ZooKeeperRequestPtr & request) const
{
    /// Reconfig bypasses session classification entirely (handled directly by KeeperDispatcher::putRequest).
    if (request->getOpNum() == Coordination::OpNum::Reconfig)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Reconfig must not reach KeeperSession::classify");

    if (callbacks_.quorum_reads || !request->isReadRequest())
        return {RequestMode::Linear, RequestTarget::Raft};

    /// Non-quorum read: defer behind preceding writes (per-session barrier).
    return {RequestMode::WaitPrevious, RequestTarget::Local};
}

bool KeeperSession::addRequest(const Coordination::ZooKeeperRequestPtr & request, bool use_xid_64)
{
    using namespace std::chrono;
    auto now_ms = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();

    /// Prepare the RequestEnvelope and classify it.
    auto envelope = std::make_shared<RequestEnvelope>();
    envelope->session_id = session_id_;
    envelope->request = request;
    envelope->create_time_ms = now_ms;
    envelope->use_xid_64 = use_xid_64;

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
        envelope->mode = mode;
        envelope->target = target;

        is_close = (request->getOpNum() == Coordination::OpNum::Close);

        switch (mode)
        {
            case RequestMode::Linear:
            {
                if (is_close)
                {
                    /// Close is terminal: transition to Finishing immediately.
                    /// No barrier entry — no requests can follow Close.
                    /// If Close fails in the batch, the client gets an error and
                    /// disconnects, triggering finishSession. No rollback needed.
                    state_ = State::Finishing;
                }
                else
                {
                    /// Record as unresolved write for barrier tracking.
                    unresolved_writes_.push_back(UnresolvedWrite{.xid = request->xid, .deferred_reads = {}});
                }

                keeper_req = envelope->buildKeeperRequestForSession(envelope);
                action = Action::PushRaft;
                break;
            }
            case RequestMode::WaitPrevious:
            {
                if (unresolved_writes_.empty())
                {
                    /// Fast path: no preceding writes, execute immediately.
                    keeper_req = envelope->buildKeeperRequestForSession(envelope);
                    action = Action::FastLocalRead;
                }
                else
                {
                    /// Defer: attach to the last unresolved write.
                    envelope->onDeferred();
                    keeper_req = envelope->buildKeeperRequestForSession(envelope);
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
            envelope->onEnqueued();
            try
            {
                callbacks_.raft_push(std::move(keeper_req), is_close);
            }
            catch (...)
            {
                envelope->onEnqueueFailed();
                /// Roll back session state so subsequent requests are not
                /// blocked behind a write that was never submitted.
                /// For Close: state_ was set to Finishing, but raft_push_ failed.
                /// We don't roll back — the client will get the exception,
                /// disconnect, and finishSession will clean up. Rolling back
                /// to Active would be wrong (we'd accept requests on a session
                /// whose Close was attempted).
                if (!is_close)
                {
                    std::lock_guard rollback_lock(mutex_);
                    if (!unresolved_writes_.empty() && unresolved_writes_.back().xid == request->xid)
                        unresolved_writes_.pop_back();
                }
                throw;
            }
            break;
        case Action::FastLocalRead:
            envelope->onFastPath();
            callbacks_.local_read(keeper_req);
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
        pending_reads = extractDeferredReads(committed_xid);
    }

    /// Dispatch released reads outside the lock.
    /// A single local_read_ failure must not abort dispatch of remaining reads,
    /// otherwise clients would never receive responses for those requests.
    for (auto & read_request : pending_reads)
    {
        try
        {
            if (read_request.envelope)
                read_request.envelope->onReleased();

            callbacks_.local_read(read_request);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

void KeeperSession::onWriteFailed(Coordination::XID failed_xid, Coordination::Error error)
{
    KeeperRequestsForSessions orphaned_reads;
    {
        std::lock_guard lock(mutex_);
        orphaned_reads = extractDeferredReads(failed_xid);
    }

    for (auto & read_request : orphaned_reads)
    {
        try
        {
            if (read_request.envelope)
                read_request.envelope->onFailedRelease("Write failed, deferred read aborted");

            callbacks_.fail_read(read_request, error);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

}
