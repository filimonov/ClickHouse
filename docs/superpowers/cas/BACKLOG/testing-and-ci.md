---
description: 'Live backlog — test coverage, CI/gate infrastructure, soak/chaos harness, and testing methodology rules.'
sidebar_label: 'Testing & CI'
sidebar_position: 6
slug: /superpowers/cas/backlog/testing-and-ci
title: 'CAS Backlog — Testing and CI'
doc_type: 'guide'
---

# CAS Backlog — Testing and CI {#testing-and-ci}

Part of the [CAS live backlog](/superpowers/cas/backlog). Topic file for test coverage, CI/gate
infrastructure, the soak/chaos harness, and testing-methodology rules.

## Unit-test gate (`CAS*`) {#unit-test-gate}

- **[review-14-coverage-gaps] highest-risk coverage gaps, remaining four** — TEST — Checked 2026-09-26: a complete real-provider contract row (grep for `RealProvider`/`ProviderContract`: zero matches), `DiskObjectStorageTransaction` CA dispatch/ordering coverage, `Expect:100-continue`, and the `LocalObjectStorage` TOCTOU/atomic-install path still have no dedicated test. Deterministic Native-mode coverage of unconditional blob streaming/copy, mutable conditional operations, and GCS request classification is covered; real-thread concurrency coverage is broad (~20 gtest files already drive `std::thread`/`ThreadPool`) but "more" was never quantified, so that sub-point is not separately claimed closed.
- **[pool-hash-consistency] CAS blob hash vs `checksums.txt` equivalence test** — TEST — `gtest_cas_pluggable_hash.cpp`'s `Sha256BuildWritesFullWidthDigestAndInlineEqualsBlob` only proves the hash formula equals itself at the `Core` level. No test drives a real part's `checksums.txt` bytes through the streaming `CaContentWriteBuffer` path and compares digests; the test's own comment defers this as "Task 7".
- **[ca-gtest-tmp-scratch-leak] every full CA gate leaves thousands of scratch dirs in `/tmp`** — {#ca-gtest-tmp-scratch-leak} — MINOR (TEST/INFRA) — `makeLocalObjectStorageForTest` (`cas_test_helpers.h`) still creates its root under `std::filesystem::temp_directory_path()` with no teardown-time removal and no relocation under the build dir. Related: `[GATE-DEBRIS]` below is a separate, still-unexplained cwd-litter family.
- **[imetadatastorage-override-coverage-gap] per-override unit-test coverage gap for `IMetadataStorage` overrides** — DESIRABLE — Not every one of the 6 current overrides (`MetadataStorageFromPlainObjectStorage`, `ContentAddressedMetadataStorage`, `MetadataStorageFromDisk`, `MetadataStorageFromCacheObjectStorage`, `MetadataStorageFromPlainRewritableObjectStorage`, `MetadataStorageFromStaticFilesWebServer`, `MetadataStorageWithPathWrapper`) is independently unit-tested.
- **[relink-positive-proof-log-line] positive-proof requirement (a specific log line) that a relink actually ran vs a dedup false-positive** — DESIRABLE — No log line or assertion distinguishes "relinked" from "deduped and looked the same" today.
- **[shard-reduce-api-production-dead] the sharded-GC reducer API is test-only while production re-implements its bucketing inline** — {#shard-reduce-api-production-dead} — MINOR (TEST/DEAD CODE) — `ShardReducer`/`manifestCleanupShard` (`Gc/CasGcShardPlan.h`) still have no production call site on either branch; the live sharded fold buckets by `blobShard` and calls `foldDeltasIntoGeneration` directly (`Gc/CasGc.cpp`). Fix, cheapest first: delete `ShardReducer`/`manifestCleanupShard` and re-point their tests at the production entry points, or make the round driver construct one per shard. `manifestCleanupShard` is the only routing function for the part-manifest cleanup axis, so deleting it should be a deliberate decision, not an oversight.
- **[gc-round-test-seam-drops-two-gates] `runOneGcRoundForTest` drops two gates its production twin enforces** — {#gc-round-test-seam-drops-two-gates} — MINOR (TEST) — `runGarbageCollectionRoundNow` opens with `checkNotReadOnly("GC round")` and a `gc_enabled` refusal (`ContentAddressedMetadataStorage.cpp`); `runOneGcRoundForTest` starts at `checkOpAdmitted(CasOpClass::Admin)` with neither, confirmed unchanged on both branches. A gtest driving the seam on an `<readonly>` mount still runs a destructive round the SQL verb would refuse. Fix: add both gates to the seam, or have it call the production method directly.
- **[rule-no-chassert-over-handled-branch] a reachable and handled state must not be `chassert`ed** — {#rule-no-chassert-over-handled-branch} — Standing rule: if the code below handles a state, do not assert it above; the handling is the assertion. Inverse of "picking both (assert and handle) is how a branch comes to exist in only half the builds." The originating example is fixed: `16cb681aadf` removed `chassert(lane_state == RefLaneState::Ready)` from `CasRefLedger.cpp`'s `commitRefChunk`; the handled path now routes through `RefLaneState::Faulted`, pinned by `CASAnomalyPolicy.NonReadyAtNewIdAllocationFaultsAndFailsClosed`.
- **[rule-structured-binding-silent-rebind] changing a returned element type silently re-binds every `auto & [a, b]` at its call sites** — {#rule-structured-binding-silent-rebind} — Standing rule: when a function's return type changes shape (element type, member count or order), grep every call site for destructuring (`auto & [`, `auto [`) and read each one; a reshape gets no compiler error. The originating bug is confirmed still fixed: `CASGCShardIncarnation.DiscoveryEqualsPresentShards` now iterates with named-field access (`life.ns`), no structured binding at all. Related in kind: `{#rule-no-chassert-over-handled-branch}`.
- **[uniform-pin-removed-testability] the uniform catalog-admission pin removed the suite's ability to test the un-admitted case** — {#uniform-pin-removed-testability} — The main construction gap is closed: `CASGCShardIncarnation.UncatalogedStreamLifeDefersWithoutInventingNamespace` (built by `aa018ef055c`, renamed by an unrelated life-id redesign at `6a3dd6a9245`) builds the namespace through the real writer path and strips its catalog entry afterwards. Not independently re-verified: the sibling test for the ABSENCE of a catalog entry, and whether the one end-to-end real-incarnation test that had been running entirely at the sentinel was re-fixed. **The rule stands: after a uniform pin, ask what the pin makes unconstructible.**
- **[test-helper-third-copy] test helpers: duplicated verbatim, while a shared home is already included** — {#test-helper-third-copy} — Half fixed: `ALWAYS_ADMITTED`, `generousDeadline` and `admittedOnceThenFenced` no longer exist anywhere (obsoleted by the uniform-admission pin above, not deduplicated). `creatorFence`, `fixedTerminality` and `findEntryForTest` are still duplicated verbatim between `gtest_cas_ns_creation_lifecycle.cpp` and `gtest_cas_ref_ckpt_join.cpp`, which already includes `cas_test_helpers.h`. Open question: what belongs in the shared header vs deliberately per-file, still deferred as a convention question.
- **Four narrow classification gaps left by the `gtest_cas_request_control.cpp` deletion** {#request-control-classification-gaps} — MINOR (TEST) — confirmed still open 2026-09-26 by reading `gtest_cas_requests.cpp` directly. Each shares its code path with an already-tested sibling, so none is a live correctness risk, but none is separately pinned by name:
  1. no write-path test names an unmodeled/`SlowDown` `S3Exception` falling through to ambiguity rather than `Refused` (the read-path instance, `CASRequests.AnUnmodeledStoreErrorOnAReadIsReissuedNotSurfaced`, has no write-path twin);
  2. `isEntityTooLargeError`'s specific S3 error name (`EntityTooLarge`) is untested by name, though the `||` chain mechanism is proven via its `MalformedXML`/`AccessDenied` siblings;
  3. `makeCasWriteRetryLaterExceptionPtr` (`CasRequests.h:89`) has no direct test against its direct-throw twin `throwCasWriteRetryLater`.
  Fix: add the three tests next to `gtest_cas_requests.cpp`'s existing siblings; add a direct classification test next to `CASWriteResult.OrThrowMapsEveryAlternative`.
- **[gate-filter-countingbackendshape-escape] ✅ CLOSED by renaming the suite to `CASCountingBackendShape` (`683b68606e2`, 2026-08-24)** — TEST/INFRA — The strict `CAS*` gate then passed `2141/2141` Debug tests from `298` suites and `2140/2140` ASan tests from `297` suites with zero sanitizer, leak, logical, fatal, or signal markers. The generator now emits the same `298`-suite strict set with the three remaining generic-infrastructure exclusions.
- **Engine test seam (if the checkpoint shows jitter-dependent reds):** `Retry::backoff` draws full jitter with no test seam, so a test driving an ambiguity under `untilLeaseSafe` with a short remaining lease is a coin flip; a `setBackoffFnForTest` on `CasRequests` (used by `pauseAndReissue`) would make it deterministic. `MountLeaseRenewer::renewOn`'s (renamed from `MountLeaseKeeper`) `catch (...)` arm reports `attempts_sent = 0` — an exception that escaped the engine carries no count; document at the field. `Pool/CasServerRoot.h` is a forbidden file for the source-comment pass; left for the engine-defect pass. (From the CP3/Task 7 review, 2026-09-03.)

### `[cas-tests-unchecked-optional-deref]` A test that dereferences a disengaged optional takes every later test in the binary with it {#cas-tests-unchecked-optional-deref}

A gtest that dereferences a disengaged `std::optional` does not fail — it aborts the process, and
every test scheduled after it in the same binary never runs. The gate then reports a smaller total
that still reads as green, so the regression that emptied the optional is invisible twice over: once
as its own missing failure, once as the suites it silently deleted from the run. This bit three times
in one night, each time presenting as "a suite disappeared" rather than as a failure.

The shape to write instead depends on the enclosing function's return type, and this is the part that
makes a blind `EXPECT_TRUE` → `ASSERT_TRUE` sweep wrong:

- **`void` test body** — `ASSERT_TRUE(x.has_value())` is correct and sufficient; `ASSERT_*` returns.
- **non-`void` helper** — `ASSERT_*` does not compile there (it expands to a bare `return;`). The
  helper must expect and then bail on its own: `EXPECT_TRUE(x.has_value()); if (!x) return {};`, or
  fold the guard into the value expression, `return x ? x->field : Field{};`. Both shapes already
  exist in the suite — `sealedCursorOf` and `holdOf` in `gtest_cas_gc_hold_grammar.cpp`, and
  `relinkTokenOf` in `gtest_cas_confirm_exact_ref.cpp` — and their comments state the reason.

**Measured on the branch at the time of writing**, not recalled: a scan for `const auto x = …`
followed within four lines by `x->` or `x.value()` with no intervening guard reports **13 candidate
sites** across 9 files, the largest groups being `gtest_ca_wiring.cpp`,
`gtest_cas_gc_frontier_gate.cpp`, `gtest_cas_orphan_nomination.cpp` and `gtest_cas_ref_writer.cpp`
(2 each). A first, naive version of the same scan reported 52 — the difference is entirely false
positives from shapes that ARE guarded: `if (const auto got = backend.get(…))`, and
`pending = e && e->delete_pending`. Any sweep must therefore be eyeballed per site, and the 13 are
candidates rather than confirmed defects; three were confirmed by reading
(`gtest_cas_lifecycle_condition.cpp:40`, `gtest_cas_orphan_nomination.cpp:180` and `:184`, each an
`EXPECT_TRUE` immediately followed by an unguarded `->`).

Separately, `EXPECT_TRUE(x.has_value())` appears 9 times against 401 `ASSERT_TRUE(x.has_value())`.
The `EXPECT` form is not wrong by itself — in a non-`void` helper it is the only option — but it is
the marker worth grepping for, because it is exactly where the author needed a guard and may have
stopped at the expectation.

The durable fix is not a one-off sweep: a sweep fixes today's sites and the next test written
reintroduces the class. What would actually close it is making the deref fail loudly at the point of
use — a checked accessor the CA test helpers use in place of `->` — so the shape is unavailable
rather than merely discouraged.

## Unit-test mutation coverage {#unit-test-mutation-coverage}

### `[cas-unit-test-mutation-battery]` Proposed: a dynamic mutation battery over the CAS unit tests (2026-09-03, deferred by the user) {#cas-unit-test-mutation-battery}

**Context.** The backend request-contract migration (spec
`docs/superpowers/specs/2026-09-02-cas-backend-token-contract-design.md`, revision 13) rewrote most of
the 142 CAS unit-test files: doubles moved from the legacy verbs to `CasRequests`, one-shot faults were
re-modelled for an engine that retries, counts pinned to the old controller were re-derived. The user
asked whether the tests still test something useful or degenerated into `2 + 2 = 4`. The static half of
that audit (two reviewers, per-test "killing mutation" + verdict DISCRIMINATING / WEAK / TAUTOLOGICAL)
is recorded in `docs/superpowers/cas/2026-09-03-test-vacuity-audit.md`; its ranked mutant lists are the
input to this item. The dynamic half was NOT run — other priorities (user, 2026-09-03). Still not run
as of 2026-09-25.

**Proposal (dynamic half).** Execute the mutants: a mutant nobody kills is a hole in the suite.

1. Merge the auditors' mutant lists into at most 20 distinct one-line production mutants (never a test
   line); one patch file each, with the tests the audit expects to go red.
2. Per mutant, in order: apply → `ninja unit_tests_dbms` in `build_debug` → run
   `build_debug/src/unit_tests_dbms --gtest_filter='CAS*'` (the gate filter is exactly `CAS*`) → record
   ran / passed / failed and the FAILED names → revert and verify `git status --porcelain -- src` is
   empty before the next mutant.
3. Verdict per mutant: KILLED (an expected test is red), KILLED-BY-OTHERS (red, but none of the expected
   tests — name what killed it), SURVIVED (zero reds: name the property no test pins). An abort in the
   debug build counts as killed by an assertion; say so.
4. Commit nothing from the mutants; finish with one clean `CAS*` gate to prove the tree is restored.
5. Output: the mutant table (patch, expected tests, actual reds, verdict), the SURVIVED list with the
   property each exposes, and for every test the static audit called TAUTOLOGICAL or WEAK whether the
   battery confirms or refutes it. Each SURVIVED mutant becomes a test to write, not a note.

**Cost.** ~20 incremental debug builds + ~20 `CAS*` runs, sequential (one builder); roughly 3-4 hours
of a cheap agent's time. Zero risk to the tree if step 2's revert check is honoured.

## Stateless and integration lanes {#stateless-integration-lanes}

- **[remote-data-paths-no-pushdown] `system.remote_data_paths` walks every disk, no `disk_name` pushdown** — {#remote-data-paths-no-pushdown} — DESIRABLE (upstream, needs consultation before editing generic code) — `StorageSystemRemoteDataPaths.cpp:153` still has no `disk_name` pushdown in `applyFilters`, confirmed unchanged on both branches. This is what timed out `04286_content_addressed_remote_data_paths` at 600s (root cause, not a CAS regression). Mitigated by tagging that one test (tag since renamed `no-content-addressed-storage` -> `no-cas-storage`, `c4f0ba4184f`); the general pushdown fix stays open.
- **[ca-s3-stateless-lane] full-lane run + remaining un-tagging** — TEST (PARTIAL) — Point-fixes landed (`04286`/`05008`/`05009`/`01271`/`03829`) and B86 removed, confirmed by the full local run in `docs/superpowers/cas/2026-09-04-stateless-lane-triage.md` (11137 tests, 0 class-B `404` recurrences). The exclusion tag was renamed `no-content-addressed-storage` -> `no-cas-storage` (`c4f0ba4184f`); 18 tests still carry it. The 3 pre-existing gtest failures this item originally named now run under renamed `CAS*`-prefixed suites (part of the tree-wide rename) but were never independently root-caused, only carried forward.
- **[unconditional-blob-publication-live-gate] real GCS and ordinary S3 acceptance for the rewritten blob lane** — GATE — Superseded evidence: `docs/superpowers/cas/2026-09-02-gcs-live-validation-ledger.md` reran the credentialed-HMAC groups (13/13 pass, the 2026-08-22 skip class gone), but is still not zero-skip — 9 `gcp_oauth` cases need application-default credentials that don't exist, and 4 "ambiguity" cases need the TLS-terminating proxy driver (`{#gcs-live-header-observability-tls-proxy}`), neither built. No evidence the ordinary `test_storage_s3` lane (blocked on the `clickhouse-server:23.3.19.33.altinitystable` image) was ever retested.
- **[gcs-endpoint-level-reload-regression] endpoint-level reload regression for the token-dialect pin** {#gcs-endpoint-level-reload-regression} — The reload dialect pin in `S3ObjectStorage::applyNewSettings` is checked against the fully merged settings, which is the only place the effective `http_client` exists (merged from the storage's current settings, any endpoint-level block, and the disk's own section). The existing test (`test_a_reload_that_would_flip_the_token_dialect_is_refused`, `tests/integration/test_cas_gcs/test.py`) only flips the disk-level key, which a disk-section-only guard would also refuse — confirmed unchanged on both branches 2026-09-26, and the test's own docstring still says so. The regression to write: a CAS disk mounts with no disk-level `http_client`, an endpoint-level `<s3>` block sets a different `http_client` at reload, the reload is refused, and a subsequent write proves the original client survived. Prerequisite still missing: the fake GCS server only mints numeric ETags and its own domain check rejects a numeric `If-Match` as "a generation, not an ETag" (`gcs_mocks/server.py`), so an ETag-dialect mount cannot be hosted in this fixture; it needs a second, non-numeric ETag shape kept separate from the generation domain. The write proving survival should assert an `If-None-Match` on the wire and no `x-goog-if-generation-match` anywhere — blob-body publication is unconditional and is not the probe for this property. Also worth noting for whoever writes it: an absent key is not a flip. Settings merge through `updateIfChanged`, which applies only values the incoming configuration sets, so deleting `http_client` leaves the previous value in force; only an explicitly different value flips the dialect.

## Soak and chaos (`ca-soak`) {#soak-and-chaos}

- **[b200-decommission-under-load] ca-soak scenario card: decommission under load** — TEST — `utils/ca-soak/scenarios/cards/s45_decommission_hidden_removing.py` (T8, validated live 2026-08-03) drives `cas-drop-member` against a killed victim with hidden `Removing` catalog rows and asserts no permanent debris plus a clean fsck, but it uses the CLI tool (not the SQL `SYSTEM CAS DROP POOL MEMBER` verb), has no live workload running during the decommission, and has no chaos variant killing the drop-member command mid-run at each phase. No occurrence of `SYSTEM CAS DROP POOL MEMBER` anywhere under `utils/ca-soak/scenarios/`. Also cover the known fail-closed narrowing: mid-retirement crash on a victim with namespace debris is refused until GC namespace-cleanup catches up.
- **[4h-continuous-chaos-soak] 4h continuous chaos soak** — TEST/GATE — One of three blockers eased: `SOAK-TTL-BAND-abort` was fixed 2026-07-09 (the checkpoint now degrades instead of aborting). The other two are still open as of the 2026-09-16 MSan RCA (`docs/superpowers/cas/2026-09-16-msan-cas-s3-shard-budget-rca.md` §H7/K1-K3, tracked as `{#janitor-page-hardcoded}` in `docs/superpowers/cas/BACKLOG/gc.md`): a compacting object store, and streaming/budgeted fsck at scale (next item). The only 4h-labeled attempt (2026-06-28) reached ~106 minutes before the (now-fixed) TTL-band abort; no run has been re-attempted since. Also re-confirms `[B165]` (server OOM at hour-4 soak, ~49 GiB RSS), formerly tracked in the now-deleted `operability-and-introspection.md` — still open, and this entry is its landing place once migrated into Backlog.md.
- **[b146-b154-fsck-timeout-at-scale] fsck timeout at scale** — TEST/GATE — Half fixed: `CasFsck` has a `deadline`/`partial` mechanism (present since at least `592b9b83568`) that degrades honestly instead of hanging. The underlying quadratic discovery/LIST cost at scale (183 GB / 2.14M objects) is not: the 2026-09-16 RCA's janitor page-budget, batched-delete and catalog-cut fixes (K1-K3) remain unbuilt.
- **[ci-full-scale-sweep] run dev-scale inconclusives at designed scale** — TEST — Mixed, not complete: `RUN_HISTORY.md` has `full`-scale rows for 20 of the named scenarios (S01-S11, S13-S15, S20, S21, S23, S29, S41), several still unresolved as of the latest run (2026-09-01): S03 fails (mount-lease `NETWORK_ERROR`), S05 fails, S15/S23/S29 inconclusive. S12/S22/S27 (infra-gated scenarios) are green at `ci` scale in every recent run but have never run at `full` scale. No systematic RSS-attribution or manifest-cap measurement doc exists.
- **[soak-harness-minors] soak-harness minors** — INFRA — Two open sub-points: the S01 scratch high-water sampler's coverage of the OPTIMIZE-FINAL spike is unconfirmed, and the `s3cache` scenario's positive-cache-hit assertion is unverified (the scenario file itself could not be located, possibly retired alongside S24). Resolved and dropped: `run_24h.sh` now archives prior-run logs under `logs/prev_<ts>` (`0cd9fc6cfff`); scenario cards no longer say `root_shards`; `pool_objects`/`pool_bytes` being `None` is a documented design choice, not an unreliability bug (`utils/ca-soak/soak/pool.py`). **OBSOLETE:** S24's pre-agreement `SYSTEM SYNC REPLICA` ask — S24 itself was retired 2026-08-22 (the conditional-blob-publication protocol it tested was superseded).
- **[soak-lock-hold-wait-metric] soak phase-3 lock-hold/wait metric sampling + budget gate** — DESIRABLE — `utils/ca-soak/scenarios/framework/sampler.py` still has no lock-hold/wait fields, only memory/pool-size/container samples.
- **[s23-soak-profiler-firehose-contamination] S23 soak memory-gate profiler-firehose contamination** — MINOR — Still unfixed; independently re-confirmed with more detail by `docs/superpowers/cas/2026-08-31-scenario-repair-ledger.md`: a 64 MiB threshold on S23's idle pool mostly measures ~176 MB/hour of the server's own telemetry.

### Soak-harness bugs found across the 2026-09-03/04 GCS and phase-3 runs {#soak-harness-bugs-2026-09-04}

- **Harness: `fixpoint_timeout_s` assumes a GC round every ~2 s** (`gc_interval_s=2` default) and gave
  1092 s for ~98 rounds; real GCS rounds took 18.8 s to 1250 s (round 7: 1114 s of its 1125 s in
  `pending_deletes`, 5000 throttled deletes). Derive the bound from observed round durations, as
  `wait_for_pool_drain` already does; otherwise every real-GCS soak with a backlog fails falsely.
  **Reproduced on real AWS 2026-09-04** (`ca_live_20260904_aws_r1`, 8-minute no-chaos smoke; read-only
  investigation report in the run's `tmp/aws_unreachable_debug_report.md`): `history=[3517, 2779]`,
  bound 300 s, while the pool converged to `unreachable=0 dangling=0 unaccounted=0` on its own 38
  minutes after the checkpoint started; condemned = graduated = redeleted = 4305, spared/replaced/
  absent 0, every unreachable object a `blobs/` key in `delete_pending` or `awaiting_gc`. Two concrete
  defects in `soak/checker.py`: (1) `poll_unreachable_to_stable` needs `stable=3` samples and each sample
  is a full fsck (~325 s at 4-5k objects), so three samples cannot fit a 300 s bound regardless of GC
  health; (2) `initial` is read before the first condemning round, so `max(300, 0.2 * initial)` collapses
  to the floor while the backlog is still rising; the `wait_for_pool_drain` precondition never engaged
  because `soak.pool`'s physical probe returns `None` on a live store. Fix: bound ≥ `stable` × measured
  fsck seconds plus a drain estimate taken from `pending_reclaim` in `system.cas_mounts` and the observed
  `system.cas_gc_log` round durations. Also: the run JSON's `fsck_status: "skipped"` is misleading when
  three fscks ran and only the final one was not reached.
  Owner: `utils/ca-soak/soak/run.py`.
- **`B152/B185` warning text is wrong for this occurrence.** `wait_for_pool_consistent` in
  `utils/ca-soak/soak/run.py` reports "did not HOLD dangling==0 … after a fault window", but the flap
  happened in the routine `gc_checkpoint` stage before any chaos fault. Broaden or correct the message so
  a real future finding is not dismissed as the known post-restart flap. (Soak 2026-09-04, phase 3, 30
  min, seed 20260904, binary 6ddaefbcc9e.)
- **Pool drain probe `None` under load.** One `pool_bytes=None` sample at 23:26:26Z inside the GC-drain
  window; plausibly a `docker exec`/`du` subprocess timeout under host I/O contention, unprovable from
  RustFS logs (known observability gap). `soak/pool.py` `pool_size` should log subprocess-timeout and
  empty-stdout as distinct causes. (Same run as above.)

### `[soak-harness-needsrecovery-after-near-ttl-pause]` The soak harness treats a near-TTL chaos pause's designed fence-then-recover as a fatal violation (2026-09-04) {#soak-harness-needsrecovery-after-near-ttl-pause}

GCS soak run 1 (`ca_live_20260904_r2`, phase 3, chaos): chaos fault #4 was `both pause` for 29 s
against a 30 s mount-lease TTL. `ch2`'s renewer fenced (`external_lease_deadline`, 0 physical
attempts), its ref append lane went `CASRefNeedsRecovery` at the committed-frontier publication
fence, and the harness's recovery checkpoint treats any nonzero `CASRefNeedsRecovery` as a
"LATE-PUT FENCING VIOLATION" and aborts the driver. The data model matched byte-for-byte on both
nodes (count 62496, same `sum_fp`/`uniq_keys`/`sum_v`/`sum_version`) and fsck came back clean
(`dangling=0 unreachable=0 stale_edge=0`); `system.cas_log` shows `ch2` recovered normally
(`mount_remount ok`) once the pause ended. `CASRefNeedsRecovery` is a cumulative counter: one
genuine, expected recovery event during a designed near-TTL pause keeps it nonzero in every later
snapshot by construction, which is what "stays 1 across three ticks" actually means here, not a
stuck lane. Ruled not attributable to the branch under measurement (`cas-gc-write-once-keys`): no
file it touches sits on the mount renewer or the ref append lane. Fix options for the harness: (a)
classify a chaos pause whose duration is within 1 s of the lease TTL as `freeze_long` rather than
an ordinary pause, since a fence-then-recover is the designed outcome at that margin; or (b) drop
`CASRefNeedsRecovery` from the must-stay-zero counter set at the recovery checkpoint specifically
for the case where the lane has since remounted and the data model matches, since the counter
cannot distinguish "recovered once, correctly" from "still stuck" without also reading the mount
state.

### `[s27-list-anomaly-aimed-at-a-retired-path]` ✅ CLOSED by `d6986f799f4` (`cas-gc-rebuild` only): S27 re-aimed at the prefixes GC actually pages {#s27-list-anomaly-aimed-at-a-retired-path}

**Found by the full scenario sweep (2026-08-31). S27 FAILED, and it failed for the right reason.**

The card injects LIST anomalies — pagination ambiguity, duplicate and missing pages — on
`cas/refs/` and checks that discovery survives them. Its verdict was:

> LIST anomalies were injected on cas/refs/ (test not vacuous): expected > 0 perturbed LISTs,
> observed 0 — proxy perturbed 0 LISTs — discovery may not have re-listed cas/refs/

Discovery does not list that prefix any more, and has not since discovery authority moved to the
pool-wide `cas/ref_catalog` object. The registry records the change in as many words: "Value 10 is
retired: discovery authority is the pool-wide `cas/ref_catalog` object rather than a roots registry
object or a physical stream listing." A grep of the CAS tree finds no listing of `cas/refs/` at all.

**The card did the right thing.** It carries a not-vacuous guard, and that guard is what fired: rather
than reporting a serene PASS for an injection that reached nothing, it failed and said the injection
reached nothing. A scenario without that guard would have been quietly reporting success against a
retired code path for as long as the path has been retired.

**The subject is not gone, only moved.** LIST is still load-bearing, for GC rather than discovery:
`CasNamespaceJanitor` pages `namespaceRootPrefix()`, `CasOrphanManifestSweep` pages
`casManifestsPrefix()`, and `CasGc` pages its own prefix. Pagination ambiguity on any of those is
exactly the hazard S27 was written for, and none of them is covered today.

**Fix direction:** re-aim the injection at the prefixes GC actually pages, and assert on GC's
outcome — no object deleted that a complete listing would have shown as reachable — rather than on
discovery's. Retiring the card instead would drop a real hazard class on the floor.

**Closed.** `d6986f799f4` re-aimed S27 at `cas/ns/stream/` (`Layout::casRefsPrefix` /
`Gc::enumerateRefPrefix`), matching this fix direction exactly; the card's comment
(`s23_s27_misc.py:583-589`) confirms the old `cas/refs/` target no longer exists in the layout.

### `[s45-drop-member-sweep-untested]` GC wins S45's race, so `cas-drop-member`'s own sweep path is never exercised {#s45-drop-member-sweep-untested}

**Found while triaging an S45 soak failure during the wire-keys proof phase (2026-08-30).**

S45 exists to prove that decommissioning a member does not leave its `Removing` catalog rows behind
as permanent debris, and it asserted that `cas-drop-member` reported `namespaces_removed >= 3`. It
reported zero, deterministically, including on the seed that passed on 2026-08-03.

The card cannot win the race it depends on. `cas-drop-member` refuses to run while the victim's mount
lease is alive, so the card must wait for the lease to lapse — and the SURVIVOR is the pool's GC
leader, which retires `Removing` namespaces pool-wide during exactly that wait. Instrumenting the
catalog on both sides of the wait showed it plainly: three victim and three survivor `removing` rows
right after the kill, and none of the six by the time the tool returned. The survivor's own rows went
too, and the tool never touches those, so GC — not the tool — did the sweeping. The tool then
correctly reported nothing to remove, with `slot_removed=true`.

The verdict has been rewritten to assert the invariant the scenario actually protects (no victim rows
survive the decommission) plus a precondition check that the hidden rows existed at the kill. **What
that loses is the only coverage of the tool's own sweep path.** When GC wins, that path never runs, so
a regression in `cas-drop-member`'s namespace sweeping would not be caught by any scenario.

**Fix direction:** give S45 a compose variant with `cas_gc_enabled` off on the survivor, so the
`Removing` rows persist through the lease-lapse wait and the tool is the only thing that can sweep
them. That makes the premise holdable by construction instead of by luck, and restores the assertion
that `namespaces_removed` matches the table count.

### `[soak-predown-textlog-scope]` `predown_dump.sh` only captures error-shaped `text_log` rows {#soak-predown-textlog-scope}

**Found by the T8 criterion-4 anomaly-arm injection** (Stage-B soak, `2026-08-03-stage-b-RESULTS.md`
`{#criterion-4-evidence}`): the GC round's own `INFORMATION`-level narration line — the exact text
explaining why destructive work was suppressed for that round, plus phase narration and hold-cause
detail generally — is not captured anywhere `predown_dump.sh` writes, because its `text_log` extract
(`text_log_error_shapes.tsv`) is scoped to error-shaped rows only. Once the cluster is torn down (or, as
here, simply reset for the next run), that narration is gone for good; the round's own structured
`system.content_addressed_garbage_collection_log` phase rows survived and carried the criterion, but the
human-readable confirmation did not.

**Fix direction:** `predown_dump.sh` should also capture `system.text_log` rows from the CAS loggers at
`Information` level, bounded by a time window and/or row cap (an unbounded dump risks turning the predown
step itself into the next `cas_log.tsv`-sized artifact). Not attempted here — recorded as a tooling gap
so the next investigation that needs this evidence doesn't rediscover the gap the hard way.

## Sanitizer lanes {#sanitizer-lanes}

### ASan stateless lanes: memory-ceiling failures are a memory-tracker snap, not CAS growth (established 2026-09-09, PR #2300 run 8) {#asan-memory-tracker-snap}

Facts, all measured on `Stateless tests (amd_asan_ubsan, cas s3 storage, parallel, 2/2)` and its plain twin `(amd_asan_ubsan, distributed plan, parallel, 2/2)` from `metric_log` (1 s samples), the `MemoryWorker` log lines and `trace_log` symbolised against the CI ASan binary:

- Resident memory is the same on both lanes: 16.5–20 GiB (CAS) and 1.2→21.6 GiB (plain) over the run, with 1600–1800 live server threads on both; the driver is threads × ASan fake stacks (`detect_stack_use_after_return`) plus shadow and redzones, as 483e00c4dd1 already recorded.
- `MemoryTracking` differs: plain stays at 0.2–1.3 GiB for the whole run; CAS sits at 14.7–19 GiB from 06:51:19 to the end. That is the sole cause of the `(total) memory limit exceeded: would use 15–17 GiB` failures on ordinary tests (17 + 5 on the two CAS shards).
- The plateau starts in ONE second: 06:51:18.676 `MemoryTracking` = 1.8 MB, 06:51:19.676 = 15.4 GiB. It is not an allocation (no trace rows of that size; `MemoryTrackingUncorrected` = −0.0 GiB, i.e. exactly one correction). It is `src/Common/MemoryWorker.cpp` (non-jemalloc branch, used by every sanitizer build, byte-identical to `altinity/antalya-26.6` at the time): `if (total_memory_tracker.get() < 0 || correct_tracker) MemoryTracker::updateAllocated(resident)` — a negative tracker amount was replaced by resident memory, i.e. by the ASan-inflated RSS.
- The 11.5 GiB burst just before is the stateless setup insert `INSERT INTO test.hits_s3 SELECT * FROM test.hits` (disk `s3_cache`, not CAS; 16 insert threads); it is released on both lanes (tracked back to 0.3 GiB at 06:50:35).
- The sub-zero dip is a few MB. Both lanes idle near zero (CAS min 88 KB, plain min 1.1 MB); the CAS lane crossed first. The drift source is `src/Common/memory.h` `untrackMemory`: without jemalloc an unsized delete subtracts `malloc_usable_size`, which the file itself calls inaccurate under sanitizers. Nothing CAS-specific was found. Run 3's "T1 disappeared" and run 1/8's "T1 present" are consistent with a near-zero coin flip, not with a CAS mechanism.
- The sibling shard 1/2 shows the same one-second jump (06:52:41).

Not established: the drift rate, and why the CAS lane's idle tracked baseline is ~1 MB lower than the plain lane's.

Fix candidates as originally proposed: (a) `MemoryWorker` non-jemalloc branch resets a negative amount to 0 instead of resident; (b) symmetric size accounting in `memory.h` without jemalloc; (c) fewer baseline threads on sanitizer lanes (the T4 pool profile, −34 % threads) for the RSS itself. Evidence: lane-g `tmp/pr2300-cicd-watch/run8/` (`symbolize.py`, metric logs, decompressed CI ASan binary under `bin/x/`).

**Update 2026-09-26.** Fix candidate (a) landed, **on `altinity/antalya-26.6` only**: `0c9c89743ccd` "use sanitizer info for allocated" (PR #2349, merged 2026-09-16, closes Altinity issue #2299). `MemoryWorker::getMemoryUsage` now returns a separate `allocated` field; under ASan/TSan/MSan it is `__sanitizer_get_current_allocated_bytes()` rather than resident, and the negative-tracker correction branch in `updateResidentMemoryThread` now passes that `allocated` value instead of `resident`. **Not yet ported to `cas-gc-rebuild`** (`src/Common/MemoryWorker.cpp` there still has the pre-fix branch). Candidates (b) and (c) remain open on both branches.

**Addendum (2026-09-07), alias `[asan-thread-count-fake-stack-ceiling]`.** Untried, orthogonal to
candidates (a)-(c) above: (d) shrink the ASan fake-stack footprint directly —
`detect_stack_use_after_return=0` weakens detection; `max_uar_stack_size_log=18` (default 20, ~11 MB/
thread) is the untried conservative alternative ("E2", never finished). Candidate (c) — fewer baseline
threads, e.g. via `[sanitizer-cas-thread-pool-profile]` below — is still the higher-value lever.

Related, separate investigation, not duplicated here: issue [#2298](https://github.com/Altinity/ClickHouse/issues/2298) (OPEN, "CAS-S3 ASan/MSan stateless shards hit GitHub's 6h timeout") and its RCA (`docs/superpowers/cas/2026-09-16-msan-cas-s3-shard-budget-rca.md`) found a *different* mechanism causing the ASan lane's `Code: 241` storms — the object store's own memory charged to the server through the shared cgroup — plus an unresolved CAS-GC backlog-runaway hypothesis under MSan; both are tracked as `{#janitor-page-hardcoded}` in `docs/superpowers/cas/BACKLOG/gc.md`.

### Sanitizer CI lanes need a smaller thread-pool profile for the CAS stateless stand (2026-09-07, run-3 triage) {#sanitizer-cas-thread-pool-profile}

Msan CAS lane throughput is 0.5k tests/h vs 2.0k/h for the plain S3 lane under the same sanitizer (tsan
CAS <0.9k/h vs 2.0k/h; ASan CAS 4.2k/h, fine), so the msan and tsan CAS shards never fit the 6h job
budget. The thread census points at pools, not tests: ≈2400 threads at 30 inline disks (BgSchPool 508,
IOWriter 165, CAS per-disk GC pools 523, …). `tests/config/install.sh` already has an
`is_sanitizer_build` branch and a `--cas-s3-storage` branch; add one `config.d/cas_sanitizer_pools.yaml`
linked only when both hold, with a short, explained key list: `threadpool_writer_pool_size` 500 →
64-100, `background_schedule_pool_size` 512 → 128-256 (renewal and cleanup tasks live there; not below
128), and on the default CAS disk `cas_gc_read_concurrency`/`cas_gc_meta_pool_size` 16 → 4 (inline disks
created by tests do not inherit these — there is no server-wide default for disk settings — so
`SYSTEM CAS FORGET` stays the lever for them). `max_thread_pool_free_size` is not a lever: local pools
built on the global pool count as busy (E1 with 50 changed nothing). Order: FORGET first (landed, run
4), this profile second, resharding (tsan ≥ 4, msan ≥ 6, plus the harness's own 4h budget) only as
insurance. Verify by the same `system.stack_trace` census per thread group, not by job wall time.

Alternative idea, recorded for completeness: give idle pool threads a bounded lifetime so a pool
shrinks between bursts and re-creates threads on demand. ClickHouse's `ThreadPool` never retires an
idle worker unless `threads.size() > min(max_threads, scheduled + max_free_threads)`, and local pools
pin their workers for the pool's lifetime, so this would be a change to the shared `ThreadPool` (idle
timeout + shrink) outside the CAS tree, with wide blast radius and its own warm-up cost per burst;
probably too much for the gain, and the per-disk pools would be better removed altogether (a
server-wide GC pool, `[gc-per-disk-thread-pools]` in `BACKLOG/gc.md`) than made shrinkable.

### CAS gtests: ~36 remaining PoolConfig hooks capture test-frame locals by reference (2026-09-08) {#poolconfig-hooks-capture-by-reference}

Found 2026-09-08 after ASan caught one real UAF (`gtest_cas_ref_writer.cpp:2828`, fixed `a726e933419`:
a Pool outlives its test frame through a background publish's `shared_from_this()`, deferred teardown
calls `boot_ms_fn` on a dead local). Census of the same `_fn = [&` shape, original count 92 across 10
files: `gtest_cas_pool.cpp` 51, `gtest_cas_mount.cpp` 23, `gtest_cas_writer_duties.cpp` 5,
`gtest_cas_ref_recovery_cas_walk.cpp` 4, `gtest_cas_observability.cpp` 2,
`gtest_cas_retirement_sweep.cpp` 2, `gtest_cas_ref_snapshot_publish_ordering.cpp` 2,
`gtest_cas_detached_work.cpp` 1, `gtest_cas_event_log.cpp` 1, `gtest_cas_gc_ack_floor.cpp` 1. Each site
needs a read (is the value mutated after the hook is installed, by whom); over half of the ref-writer
sites needed shared state, not a by-value capture. One task per file; run the suite 5x under ASan after
each. Alternative that removes the class: make `Pool` teardown not call config hooks (snapshot the
clock values it needs at construction), then the capture shape stops mattering.

**Progress (2026-09-25):** 8 of the 10 files are now at 0 by-reference captures (`gtest_cas_writer_duties.cpp`,
`gtest_cas_ref_recovery_cas_walk.cpp`, `gtest_cas_observability.cpp`, `gtest_cas_retirement_sweep.cpp`,
`gtest_cas_ref_snapshot_publish_ordering.cpp`, `gtest_cas_detached_work.cpp`, `gtest_cas_event_log.cpp`,
`gtest_cas_gc_ack_floor.cpp`); `gtest_cas_pool.cpp` and `gtest_cas_mount.cpp` are each exactly 18. 36
sites remain, concentrated in those 2 files.

### `[remount-test-seam-stale-generation-race]` `scheduleRemountForTest` can report `false` after the remount it scheduled already succeeded {#remount-test-seam-stale-generation-race}

TEST/INFRA. `CasMountRuntime::scheduleRemountForTest` (`CasMountRuntime.cpp:1080-1084`) releases and
re-acquires `driver_mutex` between issuing the remount and computing its return value; the worker
thread can complete the whole-chain remount in that window, so `gtest_cas_pool.cpp:4145`'s
`ASSERT_TRUE(store->scheduleRemountForTest())` (in `TEST(CASPoolRemount,
ParkedRedoRecoveryObservabilityPrecedesRemountResult)`, `:4097`) can see `false` for a remount that
actually succeeded. Test-only (production never calls this seam under this pattern), but a real tsan
flake. Fix: compute the return value under the same lock that increments the generation, or return the
requested generation and let the test wait for `remount_handled_generation >= requested`.

Source: `docs/superpowers/cas/random/pr2300-ci-triage-20260902.md` item 5 (Unit tests tsan, file
deleted by the u22 consolidation pass).

## CI infrastructure {#ci-infrastructure}

- **[GATE-DEBRIS] find the test that writes `test`/`test1`/`test2` into the repo-root cwd** — TEST/INFRA (small hygiene hunt) — `clickhouse-local`'s default database is a filesystem OVERLAY over the cwd, so those debris files shadow `default.test` and deterministically fail ~19 `clickhouse-local` tests in any full run launched from a poisoned checkout. Producer still not found; the shared worktree still shows the same *pattern* of fresh untracked cwd litter every run, though not proven to be literally this producer. Find it, make it write under its per-test dir; consider a pre-run debris sweep in the local praktika wrapper.
- **[tla-runner-static-metadir] every TLA runner shares a STATIC metadir, so two overlapping runs corrupt each other** — {#tla-runner-static-metadir} — Mostly fixed: `run_tlc.sh`/`run_ebo.sh` now derive `run_id="${MODULE}-$$-$(date +%s%N)"` and a metadir from it, a per-invocation unique path. The originating incident was a FALSE RED (`CaB140DangleMerge`'s `m_merged` config) caused entirely by the shared metadir: re-run on a clean metadir, `m_merged` completed GREEN in 9s over 20692441 states / 5326000 distinct, so the model itself was never at fault. Still missing: `models/README.md` does not document this metadir-uniqueness convention, so a future runner script can still reintroduce the shared-path bug.
- **[empty-set-survey-residues] empty-set survey: one violation reproduced but not explained, and one clean refusal** — {#empty-set-survey-residues} — From the empty-entity-set survey (`docs/superpowers/models/2026-07-30-empty-set-survey.md`). **The clean refusal** — `CaBuildRootPrecommit` with `Trees = {}` explicitly `ASSUME`s non-emptiness and errors — the ideal outcome. **The violation not explained** — `CaRefWriterCleanupCore` with `Builds = {}` still reports a violation whose counter-example is vacuously true, and the mechanism (how `Spec`'s fairness composes when every fair action is permanently disabled) is still not identified. `models/README.md` still has no explicit-`ASSUME`-for-empty-sets convention like `CaBuildRootPrecommit`'s. Eleven models have no entity set at all, so "zero namespaces" is inexpressible for them; any future gate model needs a namespace SET none of the existing catalog/ref models has.
- **[cas-log-zero-overhead-verify] verify default-enabled CAS system logs cost zero overhead with no CA disk configured** — VERIFY — No overhead-measurement doc exists anywhere under `docs/superpowers/cas/` for `system.cas_log`/`system.cas_gc_log` with no CA disk configured; still an unverified claim.

## Later {#later}

- **[no-step-injecting-crash-harness] no deterministic crash-at-step harness; crash-window coverage is bespoke per window** — {#no-step-injecting-crash-harness} — DESIRABLE (TEST/HARNESS) — Crash-consistency coverage today comes only as (a) integration/chaos `SIGKILL` at a wall-clock moment (`tests/integration/test_cas_shared_pool/test.py`, `test_cas_drop_pool_member/test.py`, `test_cas_gc_sharded/test.py`, the ca-soak chaos legs) — which window it lands in is luck; or (b) gtests that hand-build the post-crash state for ONE chosen window. No generic "abort at the Nth durable write of this protocol run, then re-drive and assert the invariant" wrapper exists on top of `InMemoryBackend`'s fault seams (`failNextCasPut`, `setHoldDeletes`, `injectAmbiguousPutIfAbsent`). Worth building the day a crash window is found by an incident rather than by a test.
- **[gcs-live-header-observability-tls-proxy] the live-GCS gate cannot see outbound headers; a TLS-terminating proxy is the option not taken** — {#gcs-live-header-observability-tls-proxy} — DESIRABLE (TEST/HARNESS) — Decision record, reconfirmed 2026-09-26: `tests/integration/test_gcs_live/test.py` still asserts only client-observable facts. A TLS-terminating proxy (keeping `endpoint` spelled `storage.googleapis.com` so `provider_type` stays GCS) would let a test read plaintext request headers, but the cost (a proxy container, a generated CA, per-disk proxy config) was deliberately not paid for a property the unit tests already establish without any network. `docs/superpowers/cas/2026-09-02-gcs-live-validation-ledger.md` reconfirms no GOOG4 signing or header-stripping defect has escaped to a live endpoint since, so the trigger condition for building it ("if a GOOG4 signing or header-stripping defect ever escapes the unit tests to a live endpoint") has not fired.
- **`SYSTEM SYNC REPLICA` on a lazy materialized view misses the MV** (opus-review triage T11) {#sync-replica-lazy-mv-proxy} — `trySyncReplica` casts to `StorageMaterializedView` at `InterpreterSystemQuery.cpp:2160` WITHOUT unwrapping the table proxy, confirmed unchanged on both branches 2026-09-26, so with `lazy_load_tables` enabled `SYSTEM SYNC REPLICA` on a lazy MV silently does not reach the MV. Non-CAS surface (it is the `lazy_load_tables` feature); belongs to the upstream carve-out rather than the CAS release. P3, but a silent no-op on a SYSTEM verb is worth a line.
