#include <Coordination/KeeperSessionRegistry.h>

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>


namespace CurrentMetrics
{
    extern const Metric KeeperAliveConnections;
}

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB
{

void KeeperSessionRegistry::setCallbacks(KeeperSession::Callbacks callbacks)
{
    callbacks_ = std::move(callbacks);
}

void KeeperSessionRegistry::registerSession(int64_t session_id, ZooKeeperResponseCallback callback)
{
    std::lock_guard lock(mutex_);

    if (!active_sessions_.try_emplace(
            session_id,
            std::make_shared<KeeperSession>(session_id, std::move(callback), callbacks_)).second)
        throw Exception(DB::ErrorCodes::LOGICAL_ERROR, "Session with id {} already registered in dispatcher", session_id);

    CurrentMetrics::add(CurrentMetrics::KeeperAliveConnections);
}

KeeperSessionPtr KeeperSessionRegistry::findSession(int64_t session_id) const
{
    std::shared_lock lock(mutex_);

    auto it = active_sessions_.find(session_id);
    if (it == active_sessions_.end())
        return {};

    return it->second;
}

KeeperSessionPtr KeeperSessionRegistry::detachSession(int64_t session_id)
{
    std::lock_guard lock(mutex_);

    auto it = active_sessions_.find(session_id);
    if (it == active_sessions_.end())
        return {};

    auto session = std::move(it->second);
    active_sessions_.erase(it);
    CurrentMetrics::sub(CurrentMetrics::KeeperAliveConnections);
    return session;
}

void KeeperSessionRegistry::registerNewSessionCallback(int64_t internal_id, ZooKeeperResponseCallback callback)
{
    std::lock_guard lock(mutex_);
    new_session_callbacks_[internal_id] = std::move(callback);
}

ZooKeeperResponseCallback KeeperSessionRegistry::extractNewSessionCallback(int64_t internal_id, int32_t server_id, int32_t our_server_id)
{
    std::lock_guard lock(mutex_);

    if (server_id != our_server_id)
        return {};

    auto it = new_session_callbacks_.find(internal_id);
    if (it == new_session_callbacks_.end())
        return {};

    auto callback = std::move(it->second);
    new_session_callbacks_.erase(it);
    return callback;
}

int64_t KeeperSessionRegistry::nextInternalSessionId()
{
    return internal_session_id_counter_.fetch_add(1);
}

std::vector<KeeperSessionPtr> KeeperSessionRegistry::shutdown()
{
    std::lock_guard lock(mutex_);

    std::vector<KeeperSessionPtr> sessions;
    sessions.reserve(active_sessions_.size());

    for (auto & [session_id, session] : active_sessions_)
        sessions.push_back(std::move(session));

    active_sessions_.clear();
    new_session_callbacks_.clear();
    return sessions;
}

}
