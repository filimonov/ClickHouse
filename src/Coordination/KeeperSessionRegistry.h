#pragma once

#include <Coordination/KeeperSession.h>

#include <mutex>
#include <unordered_map>
#include <vector>


namespace DB
{

class KeeperSessionRegistry
{
public:
    /// Creates a KeeperSession and inserts into the active map.
    /// Throws LOGICAL_ERROR on duplicate session_id.
    void registerSession(int64_t session_id, ZooKeeperResponseCallback callback);

    /// Returns the session or nullptr if not found.
    /// Registry mutex is released before returning -- the caller uses
    /// the session's own mutex for further operations.
    KeeperSessionPtr findSession(int64_t session_id) const;

    /// Removes session from the active map and decrements KeeperAliveConnections.
    /// Returns the detached session (still alive via shared_ptr) or nullptr.
    KeeperSessionPtr detachSession(int64_t session_id);

    /// Registers a temporary callback for new session ID allocation.
    void registerNewSessionCallback(int64_t internal_id, ZooKeeperResponseCallback callback);

    /// Extracts (and removes) the new-session callback for the given internal_id.
    /// Returns empty callback if not found or if server_id != our_server_id.
    ZooKeeperResponseCallback extractNewSessionCallback(int64_t internal_id, int32_t server_id, int32_t our_server_id);

    /// Returns the next internal session ID (atomic increment).
    int64_t nextInternalSessionId();

    /// Atomically removes all sessions and new-session callbacks.
    /// Returns the detached sessions so the caller can send Close requests.
    std::vector<KeeperSessionPtr> shutdown();

private:
    mutable std::mutex mutex_;
    std::unordered_map<int64_t, KeeperSessionPtr> active_sessions_;
    std::unordered_map<int64_t, ZooKeeperResponseCallback> new_session_callbacks_;
    std::atomic<int64_t> internal_session_id_counter_{0};
};

}
