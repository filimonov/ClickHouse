#include "config.h"

#if USE_NURAFT

#include <Coordination/KeeperSession.h>
#include <Coordination/RequestEnvelope.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>

#include <gtest/gtest.h>

#include <chrono>
#include <vector>

namespace DB::ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
}

using namespace DB;
using namespace Coordination;

namespace
{

/// Helper to create a write request with the given xid.
ZooKeeperRequestPtr makeWriteRequest(XID xid)
{
    auto req = std::make_shared<ZooKeeperCreateRequest>();
    req->xid = xid;
    req->path = "/test";
    return req;
}

/// Helper to create a read request with the given xid.
ZooKeeperRequestPtr makeReadRequest(XID xid)
{
    auto req = std::make_shared<ZooKeeperGetRequest>();
    req->xid = xid;
    req->path = "/test";
    return req;
}

/// Test fixture that creates a KeeperSession with controllable callbacks.
class KeeperSessionTest : public ::testing::Test
{
protected:
    struct PushedRequest
    {
        KeeperRequestForSession req;
        bool is_close;
    };

    struct LocalRead
    {
        KeeperRequestForSession req;
    };

    struct FailedRead
    {
        KeeperRequestForSession req;
        Coordination::Error error;
    };

    std::vector<PushedRequest> raft_pushes;
    std::vector<LocalRead> local_reads;
    std::vector<FailedRead> failed_reads;
    bool raft_push_should_throw = false;

    KeeperSessionPtr session;

    void SetUp() override
    {
        raft_pushes.clear();
        local_reads.clear();
        failed_reads.clear();
        raft_push_should_throw = false;

        auto raft_push = [this](KeeperRequestForSession && req, bool is_close) -> bool
        {
            if (raft_push_should_throw)
                throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Queue full");
            raft_pushes.push_back({std::move(req), is_close});
            return true;
        };

        auto local_read = [this](const KeeperRequestForSession & req)
        {
            local_reads.push_back({req});
        };

        auto fail_read = [this](const KeeperRequestForSession & req, Coordination::Error error)
        {
            failed_reads.push_back({req, error});
        };

        auto callback = [](const ZooKeeperResponsePtr &, ZooKeeperRequestPtr) {};

        session = std::make_shared<KeeperSession>(
            /*session_id=*/1, std::move(callback),
            std::move(raft_push), std::move(local_read),
            std::move(fail_read), /*quorum_reads=*/false);
    }
};

}

/// Test: write goes to Raft queue, read with no preceding writes goes to fast path.
TEST_F(KeeperSessionTest, WriteGoesToRaft_ReadGoesToFastPath)
{
    ASSERT_TRUE(session->addRequest(makeWriteRequest(1), false));
    ASSERT_EQ(raft_pushes.size(), 1);
    ASSERT_EQ(local_reads.size(), 0);

    /// Commit the write so the FIFO is clear.
    session->onWriteCommitted(1);

    /// Read with no preceding writes should take fast path.
    ASSERT_TRUE(session->addRequest(makeReadRequest(2), false));
    ASSERT_EQ(raft_pushes.size(), 1); // no new Raft push
    ASSERT_EQ(local_reads.size(), 1); // fast path
}

/// Test: read deferred behind write, released on commit.
TEST_F(KeeperSessionTest, ReadDeferredBehindWrite_ReleasedOnCommit)
{
    ASSERT_TRUE(session->addRequest(makeWriteRequest(1), false));
    ASSERT_TRUE(session->addRequest(makeReadRequest(2), false));

    /// Read should be deferred, not dispatched yet.
    ASSERT_EQ(raft_pushes.size(), 1);
    ASSERT_EQ(local_reads.size(), 0);

    /// Commit the write — deferred read should be released.
    session->onWriteCommitted(1);
    ASSERT_EQ(local_reads.size(), 1);
    ASSERT_EQ(local_reads[0].req.request->xid, 2);
}

/// Test: multiple reads deferred behind one write.
TEST_F(KeeperSessionTest, MultipleReadsDeferredBehindOneWrite)
{
    ASSERT_TRUE(session->addRequest(makeWriteRequest(1), false));
    ASSERT_TRUE(session->addRequest(makeReadRequest(2), false));
    ASSERT_TRUE(session->addRequest(makeReadRequest(3), false));

    ASSERT_EQ(local_reads.size(), 0);

    session->onWriteCommitted(1);
    ASSERT_EQ(local_reads.size(), 2);
    ASSERT_EQ(local_reads[0].req.request->xid, 2);
    ASSERT_EQ(local_reads[1].req.request->xid, 3);
}

/// Test: reads deferred behind different writes.
TEST_F(KeeperSessionTest, ReadsDeferredBehindDifferentWrites)
{
    ASSERT_TRUE(session->addRequest(makeWriteRequest(1), false));
    ASSERT_TRUE(session->addRequest(makeReadRequest(10), false));
    ASSERT_TRUE(session->addRequest(makeWriteRequest(2), false));
    ASSERT_TRUE(session->addRequest(makeReadRequest(20), false));

    ASSERT_EQ(local_reads.size(), 0);

    /// Commit W1 — R10 released.
    session->onWriteCommitted(1);
    ASSERT_EQ(local_reads.size(), 1);
    ASSERT_EQ(local_reads[0].req.request->xid, 10);

    /// Commit W2 — R20 released.
    session->onWriteCommitted(2);
    ASSERT_EQ(local_reads.size(), 2);
    ASSERT_EQ(local_reads[1].req.request->xid, 20);
}

/// Test: failed write releases deferred reads with error.
TEST_F(KeeperSessionTest, FailedWriteReleasesReadsWithError)
{
    ASSERT_TRUE(session->addRequest(makeWriteRequest(1), false));
    ASSERT_TRUE(session->addRequest(makeReadRequest(2), false));

    ASSERT_EQ(local_reads.size(), 0);
    ASSERT_EQ(failed_reads.size(), 0);

    /// Fail the write — deferred read should get error.
    session->onWriteFailed(1, Coordination::Error::ZCONNECTIONLOSS);
    ASSERT_EQ(local_reads.size(), 0);
    ASSERT_EQ(failed_reads.size(), 1);
    ASSERT_EQ(failed_reads[0].req.request->xid, 2);
    ASSERT_EQ(failed_reads[0].error, Coordination::Error::ZCONNECTIONLOSS);
}

/// Test: failed write W1, then commit W2 — R1 gets error, R2 gets dispatched.
TEST_F(KeeperSessionTest, FailedWrite_ThenCommit_MixedOutcome)
{
    ASSERT_TRUE(session->addRequest(makeWriteRequest(1), false));
    ASSERT_TRUE(session->addRequest(makeReadRequest(10), false));
    ASSERT_TRUE(session->addRequest(makeWriteRequest(2), false));
    ASSERT_TRUE(session->addRequest(makeReadRequest(20), false));

    /// Fail W1 — R10 gets error.
    session->onWriteFailed(1, Coordination::Error::ZOPERATIONTIMEOUT);
    ASSERT_EQ(failed_reads.size(), 1);
    ASSERT_EQ(failed_reads[0].req.request->xid, 10);
    ASSERT_EQ(local_reads.size(), 0);

    /// Commit W2 — R20 dispatched normally.
    session->onWriteCommitted(2);
    ASSERT_EQ(local_reads.size(), 1);
    ASSERT_EQ(local_reads[0].req.request->xid, 20);
}

/// Test: FIFO walk skips stale entries (W1 never committed, W2 commits).
TEST_F(KeeperSessionTest, FIFOWalkSkipsStaleEntries)
{
    ASSERT_TRUE(session->addRequest(makeWriteRequest(1), false));
    ASSERT_TRUE(session->addRequest(makeReadRequest(10), false));
    ASSERT_TRUE(session->addRequest(makeWriteRequest(2), false));
    ASSERT_TRUE(session->addRequest(makeReadRequest(20), false));

    /// W1 was never committed (stale from failed batch). W2 commits.
    /// The FIFO walk should pop W1 (stale), then pop W2 (match).
    session->onWriteCommitted(2);
    ASSERT_EQ(local_reads.size(), 1);
    ASSERT_EQ(local_reads[0].req.request->xid, 20);
    /// R10 was silently dropped (stale W1's read — destructor safety net handles spans).
}

/// Test: non-monotonic XIDs (auth xid=-4 after normal write).
TEST_F(KeeperSessionTest, NonMonotonicXIDs)
{
    /// Normal write, then auth (xid=-4), then read.
    ASSERT_TRUE(session->addRequest(makeWriteRequest(5), false));
    ASSERT_TRUE(session->addRequest(makeReadRequest(6), false));

    /// Simulate auth request going through Raft (xid=-4 is non-read).
    auto auth_req = std::make_shared<ZooKeeperAuthRequest>();
    auth_req->xid = -4;
    ASSERT_TRUE(session->addRequest(auth_req, false));

    /// Commit write xid=5 — read xid=6 released.
    session->onWriteCommitted(5);
    ASSERT_EQ(local_reads.size(), 1);
    ASSERT_EQ(local_reads[0].req.request->xid, 6);

    /// Commit auth xid=-4 — nothing deferred behind it.
    session->onWriteCommitted(-4);
}

/// Test: raft_push_ failure rolls back unresolved write, read not stuck.
TEST_F(KeeperSessionTest, RaftPushFailureRollsBack)
{
    /// First write succeeds.
    ASSERT_TRUE(session->addRequest(makeWriteRequest(1), false));
    ASSERT_EQ(raft_pushes.size(), 1);

    /// Second write fails.
    raft_push_should_throw = true;
    ASSERT_THROW(session->addRequest(makeWriteRequest(2), false), Exception);
    raft_push_should_throw = false;

    /// Commit first write.
    session->onWriteCommitted(1);

    /// Read should take fast path (W2 was rolled back, FIFO is empty).
    ASSERT_TRUE(session->addRequest(makeReadRequest(3), false));
    ASSERT_EQ(local_reads.size(), 1);
    ASSERT_EQ(local_reads[0].req.request->xid, 3);
}

/// Test: session in Finishing state rejects requests.
TEST_F(KeeperSessionTest, FinishingStateRejectsRequests)
{
    session->markCloseCommitted();
    ASSERT_FALSE(session->addRequest(makeWriteRequest(1), false));
    ASSERT_EQ(raft_pushes.size(), 0);
}

/// Test: read-only traffic after failed write doesn't hang (the main scenario).
TEST_F(KeeperSessionTest, ReadOnlyAfterFailedWrite_NoHang)
{
    ASSERT_TRUE(session->addRequest(makeWriteRequest(1), false));
    ASSERT_TRUE(session->addRequest(makeReadRequest(2), false));

    /// Write fails.
    session->onWriteFailed(1, Coordination::Error::ZOUTOFMEMORY);
    ASSERT_EQ(failed_reads.size(), 1);

    /// Subsequent reads should take fast path (FIFO is empty after failure cleanup).
    ASSERT_TRUE(session->addRequest(makeReadRequest(3), false));
    ASSERT_EQ(local_reads.size(), 1);
    ASSERT_EQ(local_reads[0].req.request->xid, 3);
}

/// Test: onWriteFailed with untracked xid (e.g. Reconfig) silently drops deferred reads.
/// This documents why failBatch must skip Reconfig: calling onWriteFailed with an xid
/// that has no unresolved_writes_ entry causes popDeferredReads to walk the entire FIFO,
/// popping all entries as "stale" and silently discarding their deferred reads.
TEST_F(KeeperSessionTest, OnWriteFailedWithUntrackedXID_SilentlyDropsReads)
{
    /// W1 with deferred R10.
    ASSERT_TRUE(session->addRequest(makeWriteRequest(1), false));
    ASSERT_TRUE(session->addRequest(makeReadRequest(10), false));
    ASSERT_EQ(local_reads.size(), 0);

    /// Call onWriteFailed with an xid that was never tracked (simulates Reconfig bug).
    /// popDeferredReads walks the FIFO: xid=1 != 99, pops as stale → R10 silently lost.
    session->onWriteFailed(99, Coordination::Error::ZCONNECTIONLOSS);

    /// R10 was silently dropped — NOT failed via fail_read_.
    /// This is why failBatch must filter out Reconfig at the caller level.
    ASSERT_EQ(failed_reads.size(), 0);
    ASSERT_EQ(local_reads.size(), 0);

    /// FIFO is now empty — subsequent reads take fast path.
    ASSERT_TRUE(session->addRequest(makeReadRequest(20), false));
    ASSERT_EQ(local_reads.size(), 1);
    ASSERT_EQ(local_reads[0].req.request->xid, 20);
}

/// Test: RequestEnvelope lifecycle — onEnqueued / onEnqueueFailed metric balance.
TEST(RequestEnvelopeTest, EnqueueFailedRollsBackMetric)
{
    auto req = makeWriteRequest(1);
    auto env = std::make_shared<RequestEnvelope>();
    env->session_id = 1;
    env->request = req;

    env->onEnqueued();
    ASSERT_EQ(env->state, RequestState::Submitted);

    env->onEnqueueFailed();
    ASSERT_EQ(env->state, RequestState::Queued);
    /// Metric balance: +1 then -1 = 0. No assertion on CurrentMetrics here,
    /// but the state transitions are verified.
}

/// Test: RequestEnvelope onFastPath state transition.
TEST(RequestEnvelopeTest, FastPathStateTransition)
{
    auto req = makeReadRequest(1);
    auto env = std::make_shared<RequestEnvelope>();
    env->session_id = 1;
    env->request = req;

    env->onFastPath();
    ASSERT_EQ(env->state, RequestState::Submitted);
}

/// Test: RequestEnvelope deferred -> released state transitions.
TEST(RequestEnvelopeTest, DeferredThenReleased)
{
    auto req = makeReadRequest(1);
    auto env = std::make_shared<RequestEnvelope>();
    env->session_id = 1;
    env->request = req;

    env->onDeferred();
    ASSERT_EQ(env->state, RequestState::Deferred);

    env->onReleased();
    ASSERT_EQ(env->state, RequestState::Submitted);
}

/// Test: RequestEnvelope deferred -> failed release.
TEST(RequestEnvelopeTest, DeferredThenFailedRelease)
{
    auto req = makeReadRequest(1);
    auto env = std::make_shared<RequestEnvelope>();
    env->session_id = 1;
    env->request = req;

    env->onDeferred();
    ASSERT_EQ(env->state, RequestState::Deferred);

    env->onFailedRelease("test failure");
    /// State doesn't change in onFailedRelease (spans are finalized, that's all).
    /// The envelope will be destroyed by the shared_ptr going out of scope.
}

#endif
