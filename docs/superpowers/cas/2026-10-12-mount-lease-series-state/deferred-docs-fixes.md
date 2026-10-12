
## PR 10 v2 (branch fix/antalya-26.6/cas-ref-lane-fault-latch, re-review A-N1..N3) {#pr-10-v2-branch-fix-antalya-26-6-cas-ref-lane-fault-latch-re}
- `reportLatchedFault` catch comment: `describe` can throw `std::bad_alloc` at any site, not only at the recovery site — reword.
- `CASRefAppendSealRejected` description + `monitoring.md` row: name both gate causes (seal of the live epoch; no seal at all).
- `chainLinkFor` doc: "the attempt goes out and meets the seal" is false — the gate refuses before anything is sent.

## PR 12 (branch fix/antalya-26.6/cas-terminal-losses-stop-10) {#pr-12-branch-fix-antalya-26-6-cas-terminal-losses-stop-10}
- `migration.md#re-add`: decommission after a lost owner anchor is refused with "identity lost over existing data" only while the mount slot survives; with the slot gone too, `openForDecommission` throws "unknown pool member". Conclusion unchanged.
- Spec/plan prose P4/P5 from the PR 12 review (spec r8).
- InterpreterSystemQuery.cpp (outside CA/): 'a stuck transient/IdentityLost pool' text is stale after PR 15 — fix in a minimal fork-patch commit when that file is next touched.
- PR 15: plural-worker leftovers 'either runtime worker' in CasPool.cpp:673–674, CasMountRuntime.h:360, CasPool.h:188 → 'the renewal thread'; the P7 test renames (e.g. ForeignBytesAtWedgeKeyTripFenceAndRemount).
- debugging.md / troubleshooting.md case 3: 'cancelled' described as the shutdown class, but cancellation after the send classifies as fence_or_lifecycle_lost and terminal 'cancelled' looks unreachable in production (cancelled is sampled once before the write). Fix the text (or the sampling) in one place.
