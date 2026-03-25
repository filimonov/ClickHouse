#include <Coordination/RequestEnvelope.h>

#include <Common/CurrentMetrics.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/KeeperSpans.h>

#include <chrono>
#include <string>
#include <vector>


namespace CurrentMetrics
{
    extern const Metric KeeperOutstandingRequests;
}

namespace DB
{

namespace
{

/// Common OTel attribute construction — only called when OTel is enabled
/// (maybeFinalize invokes the lambda only when span is set).
std::vector<OpenTelemetry::SpanAttribute> baseSpanAttributes(const RequestEnvelope & env)
{
    return {
        {"keeper.operation", Coordination::opNumToString(env.request->getOpNum())},
        {"keeper.session_id", env.session_id},
        {"keeper.xid", env.request->xid},
    };
}

std::vector<OpenTelemetry::SpanAttribute> baseSpanAttributes(
    const RequestEnvelope & env,
    std::initializer_list<OpenTelemetry::SpanAttribute> extra)
{
    auto attrs = baseSpanAttributes(env);
    attrs.insert(attrs.end(), extra.begin(), extra.end());
    return attrs;
}

/// Finalize both dispatcher_requests_queue and read_wait_for_write spans.
void finalizeSpans(
    RequestEnvelope & env,
    OpenTelemetry::SpanStatus status = OpenTelemetry::SpanStatus::OK,
    std::string_view message = {})
{
    /// Lazy: baseSpanAttributes is only called if maybeFinalize needs it.
    auto make_attrs = [&] { return baseSpanAttributes(env); };
    std::string msg(message);
    ZooKeeperOpentelemetrySpans::maybeFinalize(
        env.request->spans.dispatcher_requests_queue, make_attrs, status, msg);
    ZooKeeperOpentelemetrySpans::maybeFinalize(
        env.request->spans.read_wait_for_write, make_attrs, status, msg);
}

}

RequestEnvelope::~RequestEnvelope()
{
    if (!request)
        return;

    /// Safety net: finalize any OTel spans that were initialized but not
    /// explicitly finalized via lifecycle methods.
    /// Only finalize spans that were actually initialized (start_time_us != 0).
    /// Never-initialized spans would trigger a chassert in maybeFinalize.
    /// Attrs are built lazily inside the lambda — no allocation on the normal
    /// path where both spans were already finalized.
    auto make_attrs = [&] { return baseSpanAttributes(*this, {{"keeper.leaked", true}}); };

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
    ZooKeeperOpentelemetrySpans::maybeFinalize(
        request->spans.dispatcher_requests_queue,
        [&] { return baseSpanAttributes(*this, {{"keeper.enqueue_failed", true}}); },
        OpenTelemetry::SpanStatus::ERROR, "Failed to enqueue request");
}

void RequestEnvelope::onFastPath()
{
    state = RequestState::Submitted;
    /// Fast-path reads bypass the requests_queue but still produce the
    /// dispatcher_requests_queue span for consistent OTel tracing.
    /// No KeeperOutstandingRequests metric since they don't enter the queue.
    ZooKeeperOpentelemetrySpans::maybeInitialize(request->spans.dispatcher_requests_queue, request->tracing_context);
    ZooKeeperOpentelemetrySpans::maybeFinalize(
        request->spans.dispatcher_requests_queue,
        [&] { return baseSpanAttributes(*this, {{"keeper.fast_path", true}}); });
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
    finalizeSpans(*this);
}

void RequestEnvelope::onFailedRelease(std::string_view reason)
{
    state = RequestState::Queued;
    finalizeSpans(*this, OpenTelemetry::SpanStatus::ERROR, reason);
}

}
