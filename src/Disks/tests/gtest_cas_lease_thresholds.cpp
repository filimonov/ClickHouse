#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequestBudget.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>
#include <Disks/tests/cas_test_helpers.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

using namespace DB::Cas;

/// Defaults: lease 30 s, period 10 s, attempt 5 s with a 1 s connect cap (envelope 7 s), margin 2 s.
TEST(CASLeaseThresholds, DefaultsGiveTheTimelineOfTheMountLine)
{
    const CasLeaseThresholds t = leaseThresholds(CasRequestBudget{}, 30'000, 10'000);
    EXPECT_EQ(t.conditionalReserveMs(), 16'000u);
    EXPECT_EQ(t.singleRequestReserveMs(), 9'000u);
    EXPECT_EQ(t.conditionalPauseAfterMs(), 14'000u);
    EXPECT_EQ(t.singleRequestPauseAfterMs(), 21'000u);
    EXPECT_EQ(describeMountTimeline("cas1", "r1", t),
        "CAS disk 'cas1' (server_root_id=r1): renewal starts every 10 s. Without a confirmed renewal conditional "
        "changes pause after 14 s; deletes, publishes and uncached reads after 21 s; the lease ends after 30 s. "
        "Operations resume under the same writer_epoch after a confirmed renewal that leaves enough lease. A renewal "
        "that takes the full request envelope of 7 s ends at 17 s, after conditional changes pause. Another server "
        "may take this slot after it has seen the token unchanged for 31.5 s plus its own poll step "
        "(cas_mount_lease_ms=30000, cas_mount_renew_period_ms=10000, request envelope 7 s, cas_lease_safety_margin_ms=2000).");
}

/// The configuration rule in words and numbers, and which settings must match across the pool.
TEST(CASLeaseThresholds, SettingTextsSayTheRuleAndWhatMustMatch)
{
    DB::Cas::tests::expectThrowsCodeWithMessage(DB::ErrorCodes::BAD_ARGUMENTS,
        "a renewal must start while conditional changes are still admitted: cas_mount_renew_period_ms (14000) "
        "plus the conditional reserve (16000 = 2 × request envelope 7000 + cas_lease_safety_margin_ms 2000) "
        "must be less than cas_mount_lease_ms (30000)",
        [] { validateCasRequestBudget(CasRequestBudget{}, 30'000, 14'000, /*background_renewal=*/true); });
    const String text = mountDoubleStartMessage("r1", std::nullopt);
    EXPECT_NE(text.find("must run the same cas_mount_lease_ms;"), String::npos) << text;
    EXPECT_NE(text.find("cas_mount_renew_period_ms is local to each server"), String::npos) << text;
}

TEST(CASLeaseThresholds, RefusalTextsNameTheReserveAndTheLastError)
{
    const CasLeaseThresholds t = leaseThresholds(CasRequestBudget{}, 30'000, 10'000);
    EXPECT_EQ(describeLeaseRefusal("cas1", t, 14'000, 14'000, LeaseClass::ConditionalChange, String("PUT gc/server-roots/r1/mount: transport SlowDown slow down"), std::nullopt),
        "CAS disk 'cas1': this change is not admitted: 14 s of the confirmed lease remain and a conditional change "
        "needs more than 16 s (two requests of 7 s plus 2 s). Renewal keeps retrying; changes resume after a "
        "confirmation that leaves enough lease. Last renewal error: PUT gc/server-roots/r1/mount: transport SlowDown slow down.");
    EXPECT_EQ(describeLeaseRefusal("cas1", t, std::nullopt, 7'000, LeaseClass::SingleRequest, String("PUT k: refused AccessDenied denied"), String("AccessDenied")),
        "CAS disk 'cas1': this request is not admitted: the confirmed lease has ended and a single request needs "
        "more than 9 s (one request of 7 s plus 2 s). The object store refuses the renewal request (AccessDenied); "
        "requests resume after the cause is fixed and a renewal is confirmed.");
    EXPECT_EQ(describeLeaseRefusal("cas1", t, 15'000, 15'200, LeaseClass::ConditionalChange, std::nullopt, std::nullopt),
        "CAS disk 'cas1': this change is not admitted: 15 s of the confirmed lease remain and a conditional change "
        "needs more than 17.2 s (15.2 s of requests and retry pauses plus 2 s). Renewal keeps retrying; changes "
        "resume after a confirmation that leaves enough lease.");
}

/// The class comes from the call site, not from the size of the need, and a zero need never prints a sum.
TEST(CASLeaseThresholds, RefusalClassComesFromTheCallSite)
{
    const CasLeaseThresholds t = leaseThresholds(CasRequestBudget{}, 30'000, 10'000);
    /// A delete re-observing under backoff reserves two envelopes yet stays a request.
    EXPECT_EQ(describeLeaseRefusal("cas1", t, 15'000, 14'000, LeaseClass::SingleRequest, std::nullopt, std::nullopt),
        "CAS disk 'cas1': this request is not admitted: 15 s of the confirmed lease remain and a request needs "
        "more than 16 s (14 s of requests and retry pauses plus 2 s). Renewal keeps retrying; requests resume "
        "after a confirmation that leaves enough lease.");
    EXPECT_EQ(describeLeaseRefusal("cas1", t, 1'500, 0, LeaseClass::SingleRequest, std::nullopt, std::nullopt),
        "CAS disk 'cas1': this request is not admitted: 1.5 s of the confirmed lease remain and the safety margin "
        "of 2 s must stay free. Renewal keeps retrying; requests resume after a confirmation that leaves enough lease.");
}
