#pragma once

#include <Common/Logger.h>

#include <functional>


namespace Coordination
{

struct ZooKeeperRequest;
using ZooKeeperRequestPtr = std::shared_ptr<ZooKeeperRequest>;

struct ZooKeeperResponse;
using ZooKeeperResponsePtr = std::shared_ptr<ZooKeeperResponse>;

}
namespace DB
{

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;
class KeeperContext;
using KeeperContextPtr = std::shared_ptr<KeeperContext>;
class KeeperSession;
using KeeperSessionPtr = std::shared_ptr<KeeperSession>;
class RequestEnvelope;
using RequestEnvelopePtr = std::shared_ptr<RequestEnvelope>;

using SessionAndTimeout = std::unordered_map<int64_t, int64_t>;

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

/// Callback invoked by `KeeperDispatcher` to deliver responses to clients.
/// Must be safe for concurrent invocation from the response thread and session cleanup paths.
using ZooKeeperResponseCallback = std::function<void(const Coordination::ZooKeeperResponsePtr & response, Coordination::ZooKeeperRequestPtr request)>;

enum class KeeperDigestVersion : uint8_t
{
    NO_DIGEST = 0,
    V1 = 1,
    V2 = 2, // added system nodes that modify the digest on startup so digest from V0 is invalid
    V3 = 3, // fixed bug with casting, removed duplicate czxid usage
    V4 = 4  // 0 is not a valid digest value
};

struct KeeperDigest
{
    KeeperDigestVersion version{KeeperDigestVersion::NO_DIGEST};
    uint64_t value{0};
};

static constexpr auto KEEPER_CURRENT_DIGEST_VERSION = KeeperDigestVersion::V4;

struct KeeperResponseForSession
{
    int64_t session_id;
    Coordination::ZooKeeperResponsePtr response;
    Coordination::ZooKeeperRequestPtr request = nullptr;
};

using KeeperResponsesForSessions = std::vector<KeeperResponseForSession>;

struct KeeperRequestForSession
{
    int64_t session_id;
    int64_t time{0};
    Coordination::ZooKeeperRequestPtr request;
    int64_t zxid{0};
    std::optional<KeeperDigest> digest;
    int64_t log_idx{0};
    bool use_xid_64{false};
    RequestEnvelopePtr envelope;
};
using KeeperRequestsForSessions = std::vector<KeeperRequestForSession>;

inline static constexpr std::string_view tmp_keeper_file_prefix = "tmp_";

void moveFileBetweenDisks(
    DiskPtr disk_from,
    const std::string & path_from,
    DiskPtr disk_to,
    const std::string & path_to,
    std::function<void()> before_file_remove_op,
    LoggerPtr logger,
    const KeeperContextPtr & keeper_context);

}
