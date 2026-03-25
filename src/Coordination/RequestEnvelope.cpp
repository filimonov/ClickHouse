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

std::vector<OpenTelemetry::SpanAttribute> RequestEnvelope::baseSpanAttributes() const
{
    return {
        {"keeper.operation", Coordination::opNumToString(request->getOpNum())},
        {"keeper.session_id", session_id},
        {"keeper.xid", request->xid},
    };
}

std::vector<OpenTelemetry::SpanAttribute> RequestEnvelope::baseSpanAttributes(
    std::initializer_list<OpenTelemetry::SpanAttribute> extra) const
{
    auto attrs = baseSpanAttributes();
    attrs.insert(attrs.end(), extra.begin(), extra.end());
    return attrs;
}

void RequestEnvelope::finalizeSpans(
    OpenTelemetry::SpanStatus status,
    const std::string & message)
{
    auto attrs = baseSpanAttributes();
    auto make_attrs = [&] { return attrs; };
    ZooKeeperOpentelemetrySpans::maybeFinalize(
        request->spans.dispatcher_requests_queue, make_attrs, status, message);
    ZooKeeperOpentelemetrySpans::maybeFinalize(
        request->spans.read_wait_for_write, make_attrs, status, message);
}

RequestEnvelope::~RequestEnvelope()
{
    if (!request)
        return;

    /// Safety net: finalize any OTel spans that were initialized but not
    /// explicitly finalized via lifecycle methods.
    /// Only finalize spans that were actually initialized (start_time_us != 0).
    /// Never-initialized spans would trigger a chassert in maybeFinalize.
    auto attrs = baseSpanAttributes({{"keeper.leaked", true}});
    auto make_attrs = [&] { return attrs; };

    if (request->spans.dispatcher_requests_queue.start_time_us != 0)
        ZooKeeperOpentelemetrySpans::maybeFinalize(
            request->spans.dispatcher_requests_queue, make_attrs,
            OpenTelemetry::SpanStatus::ERROR, "Span not explicitly finalized");

    if (request->spans.read_wait_for_write.start_time_us != 0)
        ZooKeeperOpentelemetrySpans::maybeFinalize(
            request->spans.read_wait_for_write, make_attrs,
            OpenTelemetry::SpanStatus::ERROR, "Span not explicitly finalized");
}

KeeperRequestForSession RequestEnvelope::buildKeeperRequestForSession(RequestEnvelopePtr self) const
{
    return KeeperRequestForSession
    {
        .session_id = session_id,
        .time = create_time_ms,
        .request = request,
        .digest = std::nullopt,
        .use_xid_64 = use_xid_64,
        .envelope = std::move(self),
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
    auto attrs = baseSpanAttributes({{"keeper.enqueue_failed", true}});
    ZooKeeperOpentelemetrySpans::maybeFinalize(
        request->spans.dispatcher_requests_queue,
        [&] { return attrs; },
        OpenTelemetry::SpanStatus::ERROR, "Failed to enqueue request");
}

void RequestEnvelope::onFastPath()
{
    state = RequestState::Submitted;
    /// Fast-path reads bypass the requests_queue but still produce the
    /// dispatcher_requests_queue span for consistent OTel tracing.
    /// No KeeperOutstandingRequests metric since they don't enter the queue.
    ZooKeeperOpentelemetrySpans::maybeInitialize(request->spans.dispatcher_requests_queue, request->tracing_context);
    auto attrs = baseSpanAttributes({{"keeper.fast_path", true}});
    ZooKeeperOpentelemetrySpans::maybeFinalize(
        request->spans.dispatcher_requests_queue,
        [&] { return attrs; });
}

void RequestEnvelope::onDeferred()
{
    state = RequestState::Deferred;
    ZooKeeperOpentelemetrySpans::maybeInitialize(request->spans.dispatcher_requests_queue, request->tracing_context);
    ZooKeeperOpentelemetrySpans::maybeInitialize(request->spans.read_wait_for_write, request->tracing_context);
}

void RequestEnvelope::onReleased()
{
    state = RequestState::Submitted;
    finalizeSpans();
}

void RequestEnvelope::onFailedRelease(const std::string & reason)
{
    state = RequestState::Queued;
    finalizeSpans(OpenTelemetry::SpanStatus::ERROR, reason);
}

}
