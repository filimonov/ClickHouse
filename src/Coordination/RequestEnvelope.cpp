#include <Coordination/RequestEnvelope.h>

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

RequestEnvelope::~RequestEnvelope()
{
    if (!request)
        return;

    /// Safety net: finalize any OTel spans that were initialized but not
    /// explicitly finalized via lifecycle methods. This catches leaked spans
    /// from missed transitions (e.g., a deferred read whose write never committed).
    auto make_attributes = [&]
    {
        return std::vector<OpenTelemetry::SpanAttribute>{
            {"keeper.operation", Coordination::opNumToString(request->getOpNum())},
            {"keeper.session_id", session_id},
            {"keeper.xid", request->xid},
            {"keeper.leaked", true},
        };
    };

    ZooKeeperOpentelemetrySpans::maybeFinalize(
        request->spans.dispatcher_requests_queue, make_attributes,
        OpenTelemetry::SpanStatus::ERROR, "Span not explicitly finalized");
    ZooKeeperOpentelemetrySpans::maybeFinalize(
        request->spans.read_wait_for_write, make_attributes,
        OpenTelemetry::SpanStatus::ERROR, "Span not explicitly finalized");
}

KeeperRequestForSession RequestEnvelope::buildKeeperRequestForSession() const
{
    return KeeperRequestForSession
    {
        .session_id = session_id,
        .time = create_time_ms,
        .request = request,
        .digest = std::nullopt,
        .use_xid_64 = use_xid_64,
        .envelope = {},
    };
}

void RequestEnvelope::onEnqueued()
{
    state = RequestState::Submitted;
    ZooKeeperOpentelemetrySpans::maybeInitialize(request->spans.dispatcher_requests_queue, request->tracing_context);
    CurrentMetrics::add(CurrentMetrics::KeeperOutstandingRequests);
}

void RequestEnvelope::onEnqueueFailed()
{
    state = RequestState::Queued;
    CurrentMetrics::sub(CurrentMetrics::KeeperOutstandingRequests);
    ZooKeeperOpentelemetrySpans::maybeFinalize(
        request->spans.dispatcher_requests_queue,
        [&]
        {
            return std::vector<OpenTelemetry::SpanAttribute>{
                {"keeper.operation", Coordination::opNumToString(request->getOpNum())},
                {"keeper.session_id", session_id},
                {"keeper.xid", request->xid},
                {"keeper.enqueue_failed", true},
            };
        },
        OpenTelemetry::SpanStatus::ERROR,
        "Failed to enqueue request");
}

void RequestEnvelope::onFastPath()
{
    state = RequestState::Submitted;
    /// Fast-path reads bypass the requests_queue but still produce the
    /// dispatcher_requests_queue span for consistent OTel tracing.
    /// Both init and finalize happen here since the request never enters
    /// requestThread (which normally finalizes the span on pop).
    /// No KeeperOutstandingRequests metric since they don't enter the queue.
    ZooKeeperOpentelemetrySpans::maybeInitialize(request->spans.dispatcher_requests_queue, request->tracing_context);
    ZooKeeperOpentelemetrySpans::maybeFinalize(
        request->spans.dispatcher_requests_queue,
        [&]
        {
            return std::vector<OpenTelemetry::SpanAttribute>{
                {"keeper.operation", Coordination::opNumToString(request->getOpNum())},
                {"keeper.session_id", session_id},
                {"keeper.xid", request->xid},
                {"keeper.fast_path", true},
            };
        });
}

void RequestEnvelope::onDeferred()
{
    state = RequestState::Deferred;
    /// Initialize both spans: the request will appear in the requests_queue span
    /// (for consistent OTel tracing) and the read_wait_for_write span (for barrier tracking).
    ZooKeeperOpentelemetrySpans::maybeInitialize(request->spans.dispatcher_requests_queue, request->tracing_context);
    ZooKeeperOpentelemetrySpans::maybeInitialize(request->spans.read_wait_for_write, request->tracing_context);
}

void RequestEnvelope::onReleased()
{
    state = RequestState::Submitted;
    /// Finalize the dispatcher_requests_queue span (initialized in onDeferred).
    ZooKeeperOpentelemetrySpans::maybeFinalize(
        request->spans.dispatcher_requests_queue,
        [&]
        {
            return std::vector<OpenTelemetry::SpanAttribute>{
                {"keeper.operation", Coordination::opNumToString(request->getOpNum())},
                {"keeper.session_id", session_id},
                {"keeper.xid", request->xid},
            };
        });
    /// Finalize the read_wait_for_write span.
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

void RequestEnvelope::onFailedRelease(const std::string & reason)
{
    state = RequestState::Queued;

    auto make_attributes = [&]
    {
        return std::vector<OpenTelemetry::SpanAttribute>{
            {"keeper.operation", Coordination::opNumToString(request->getOpNum())},
            {"keeper.session_id", session_id},
            {"keeper.xid", request->xid},
        };
    };

    ZooKeeperOpentelemetrySpans::maybeFinalize(
        request->spans.dispatcher_requests_queue, make_attributes,
        OpenTelemetry::SpanStatus::ERROR, reason);
    ZooKeeperOpentelemetrySpans::maybeFinalize(
        request->spans.read_wait_for_write, make_attributes,
        OpenTelemetry::SpanStatus::ERROR, reason);
}

}
