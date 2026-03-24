#include <Coordination/SessionRequest.h>

#include <Common/CurrentMetrics.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/KeeperSpans.h>

#include <chrono>


namespace CurrentMetrics
{
    extern const Metric KeeperOutstandingRequests;
}

namespace DB
{

KeeperRequestForSession SessionRequest::buildKeeperRequestForSession() const
{
    return KeeperRequestForSession
    {
        .session_id = session_id,
        .time = create_time_ms,
        .request = request,
        .digest = std::nullopt,
        .use_xid_64 = use_xid_64,
        .session_request = {},
    };
}

void SessionRequest::onEnqueued()
{
    state = SessionRequestState::Submitted;
    ZooKeeperOpentelemetrySpans::maybeInitialize(request->spans.dispatcher_requests_queue, request->tracing_context);
    CurrentMetrics::add(CurrentMetrics::KeeperOutstandingRequests);
}

void SessionRequest::onFastPath()
{
    state = SessionRequestState::Submitted;
    /// Fast-path reads bypass the requests_queue entirely, so no
    /// dispatcher_requests_queue span and no KeeperOutstandingRequests metric.
}

void SessionRequest::onDeferred()
{
    state = SessionRequestState::Deferred;
    ZooKeeperOpentelemetrySpans::maybeInitialize(request->spans.read_wait_for_write, request->tracing_context);
}

void SessionRequest::onReleased()
{
    state = SessionRequestState::Submitted;
    ZooKeeperOpentelemetrySpans::maybeFinalize(
        request->spans.read_wait_for_write,
        [&]
        {
            return std::vector<OpenTelemetry::SpanAttribute>{
                {"keeper.operation", Coordination::opNumToString(request->getOpNum())},
                {"keeper.session_id", session_id},
                {"keeper.xid", request->xid},
            };
        });
}

void SessionRequest::onCompleted()
{
    state = SessionRequestState::Completed;
}

void SessionRequest::onCancelled()
{
    state = SessionRequestState::Cancelled;

    /// Safety-net: finalize any open spans that were not finalized on the normal path.
    ZooKeeperOpentelemetrySpans::maybeFinalize(
        request->spans.read_wait_for_write,
        [&]
        {
            return std::vector<OpenTelemetry::SpanAttribute>{
                {"keeper.operation", Coordination::opNumToString(request->getOpNum())},
                {"keeper.session_id", session_id},
                {"keeper.xid", request->xid},
                {"keeper.stale", true},
            };
        },
        OpenTelemetry::SpanStatus::ERROR,
        "Request cancelled");
}

}
