---
description: 'Live backlog — garbage collection: scalability, byte cost, correctness follow-ups, and observability.'
sidebar_label: 'GC'
sidebar_position: 2
slug: /superpowers/cas/backlog/gc
title: 'CAS Backlog — GC'
doc_type: 'guide'
---

# CAS Backlog — GC {#gc}

Part of the [CAS live backlog](/superpowers/cas/backlog). Reorganized by topic 2026-09-25. The current
plan of record for round cost is the spec
`docs/superpowers/specs/2026-09-25-cas-gc-rounds-in-minutes-design.md` (linked below as "spec"); its
measurement is `docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget-audit.md` (linked as
"audit `#fN`"). Items the spec fully absorbs are pointed to, not duplicated, from
[Rounds in minutes](#gc-rounds-in-minutes).

## Rounds in minutes (spec 2026-09-25) {#gc-rounds-in-minutes}

Fully covered by the spec, text removed here:

- **[gc-frontier-one-list]** — spec [B1](/superpowers/specs/cas-gc-rounds-in-minutes-design#b1-discovery): a per-life exact-GET probe plus a bounded per-life LIST only on change, stronger than either proposed lever.
- **[GC-DEFER-DECISION-LIST-COST]** {#gc-defer-decision-list-cost} — same spec section (B1). Was the largest GC cost item in this file (79% of GC wall time on the 2026-08 measurement; audit re-measures 30-42%).

Partially covered, full text kept under their own topic below:

- **`{#covered-log-cleanup-aborts-on-catalog-etag}`** (formerly `{#ref-cleanup-whole-catalog-token-stillness}`/CAS-079) — spec [B3](/superpowers/specs/cas-gc-rounds-in-minutes-design#b3-cleanup-licence), named explicitly as absorbing CAS-079 (per-row licensing, per-namespace refusal, adjudicated not yet built); the 2026-09-16 measurement (40 of 85 rounds abort at the catalog-etag check, 37 never read, 8 delete something) and the narrow-or-refresh-cut plan are still open. See [GC cost and throughput](#gc-cost-and-throughput).
- **`{#janitor-page-hardcoded}`** (CAS-034) — spec [B4](/superpowers/specs/cas-gc-rounds-in-minutes-design#b4-janitor) makes the janitor's page settings-driven; does not narrow its LIST scope. See [GC cost and throughput](#gc-cost-and-throughput).
- **`{#gc-outcome-budget-skews-round-report-counters}`** (CAS-101) — spec [C2](/superpowers/specs/cas-gc-rounds-in-minutes-design#c2-budgets-removed) closes refinement 1 only. See [GC observability](#gc-observability).
- **`[distributed gc_shards>1 parallel GC]`** — spec [§7](/superpowers/specs/cas-gc-rounds-in-minutes-design#multi-node-direction) records the direction, not built. See [GC cost and throughput](#gc-cost-and-throughput).

## GC correctness and safety {#gc-correctness-and-safety}

- **fsck-vs-GC blob-retention leak** {#fsck-gc-indegree-disagreement-2026-07-25} — KEEP — 56 blobs (104,755 bytes) are fsck-unreferenced but GC keeps in-degree 1, flat across 1000+ rounds: an unmatched `-1` (owning manifest gone) silently no-ops against a `+1`. Safety proven (debris, not loss; `CasGcUnmatchedRemoveDeltas` counts it); only `ca-gc-rebuild` clears it. Open: why the `-1` arrives unmatched — one traced case is a 43ms `tmp-fetch_*` publish/drop with the `+1` folding ~3 min later (unverified fold-order-inversion hypothesis). Needs a targeted repro, not more log reading.
- **Adopted fold seal referenced a pruned generation's run** {#adopted-seal-pruned-run-2026-07-25} — KEEP — Observed 2026-07-25: an adopted seal named a run `pruneSupersededGenerations` had already reclaimed, despite the guard at `CasGc.cpp:629-645`. Silent (no throw, per policy): the round reclaims nothing for that shard. Not established whether prune raced adopt. Needs a targeted test.
- **[orphan-sweep-absent-catalog-row-window]** {#orphan-sweep-absent-catalog-row-window} CAS-022 — KEEP, P2 — `sweepNamespace` refuses a namespace with no catalog row; the paged planner `planManifestCursorPage` does not (`Gc/CasOrphanManifestSweep.cpp:700-895`), so a namespace's first write can pass with no watermark. Contained by `promote`'s fail-closed missing-body check, not data loss. Fix: same durable-row precondition in the paged planner.
- **[stranded-generation-prefix-invisible-to-fsck]** {#stranded-generation-prefix-invisible-to-fsck} CAS-074 — KEEP, P2 — Three comments (`Gc/CasGc.cpp:1105-1149`) claim fsck backstops a skipped generation prefix; `runFsck` never lists `gc/` at all, so it's in no counter and understated in size (O(edges) snapshot runs). Owed: fix the prose, add a bounded `gc/gen/` advisory count, then consider a reclaimer. A second false "GC lists `blobs/`" claim (`CasServerRoot.h`, `gtest_cas_s3_staging.cpp`) is the same sweep, prose only.
- **[dead-member-frozen-build-floor]** {#dead-member-frozen-build-floor} CAS-077 — KEEP, protocol-adjacent — `prefixEligible` keys off the victim's own mount lease (`Gc/CasOrphanManifestSweep.cpp:45-64,476-493`); node loss freezes rather than removes the floor, so in-flight-at-loss manifest bodies and their blobs retain indefinitely absent decommission. Part-sized bytes, bounded by in-flight concurrency at loss. Owed: a named retain/report class (today `ca-fsck` mislabels it `in-flight-pre-precommit`), then a reclaim decision — PROTOCOL ADJACENT, user consult.
- **[janitor-cursor-rewind-on-list-error]** {#janitor-cursor-rewind-on-list-error} CAS-078 — KEEP, P3 — `NamespaceJanitor::runOnePage` resets its durable cursor on ANY LIST exception (`Gc/CasNamespaceJanitor.cpp:22-31`, deliberate and pinned), not only a genuine rejection; with the hardcoded one-page-per-round pacing (`{#janitor-page-hardcoded}`) a backend with a comparable failure rate can pin the janitor at the prefix head. Not loss. Owed: keep the cursor on a transient failure, reset only on deterministic rejection or N consecutive failures. Spec B4 changes the pacing knobs, not this.
- **[gc-lease-not-released-on-clean-stop]** {#gc-lease-not-released-on-clean-stop} CAS-099 — KEEP, minor, self-healing — A clean stop/shutdown leaves the durable `gc/state` lease untouched (`Gc/CasGcScheduler.h:105-109`); a peer waits ~2 heartbeat ticks (~2×`gc_interval_sec`, default 60s) before stealing. No correctness risk. Worth weighing: a "resign" write mirroring the mount side's farewell precedent — protocol consult, not decided.
- **[gc-silent-frontier-exit]** T6a's silent-frontier-exit enumeration gap — HARD — A walk-that-never-started shape found during T6a frontier-proof verification; not yet covered.
- **[removing-cursor-write-twice]** `Removing`→`RemovalReady` cursor-write-twice + final-deletion revalidation — VERIFY — Surfaced by a strategic review round; not independently verified against HEAD.
- **[namespace-removal-ordering-cost]** namespace-removal ordering + catalog size-bound cost — VERIFY — Verify the stated ordering still holds and size the catalog cost.
- **[stateless-reader-mark-fence-blind-spot]** a ref-less catalog-only reader may be invisible to the `use_count()==1` mark-union fence — DESIRABLE — The gated loop is live (`Pool/CasRefLedger.h:804,.cpp:1618`); verify whether a stateless reader can race a GC decision.
- **[clamp liveness]** scoped suppression under long persistent clamps — DESIRABLE→HARD — Fail-closed clamp+suppression self-heals; suppression-vs-liveness under a long clamp is unaddressed. The 2026-07-18 S38 starvation shape is MOOT since `d74c726ef9e3` (verified on `cas-gc-rebuild`, absent from `antalya-26.6`) retired the LIST-based late-log detector it starved. General item still stands on its own reproducers.
- **[ack-floor soak validation]** 3 scenario cards — TEST — SIGSTOP-hold-release, hard-KILL-fence-fsck, O(delta)+O(servers) regression guard. Implemented, unit/TLA-covered; floor semantics changed under freshness-v3 so the cards need updating. Still not soak-validated on either branch (`utils/ca-soak/scenarios/RUN_HISTORY.md` has no ack-floor entries, checked 2026-09-25).
- **[codex-11]** namespace drop misses an unregistered build → ownerless Live namespace — LOW — `Pool::beginPartWrite`'s allocate/register window (`CasPool.cpp:772-777`) can revive a Live-but-ownerless empty ref-table that GC never sweeps. Non-self-healing, narrow. Fix: a GC backstop, or a namespace generation in the birth-time gate.
- **[RECOVERED-INDEGREE-ATTRIBUTION]** move the recovered-in-degree check to the writer — DESIRABLE — `LOG_WARNING` (`CasBlobInDegree.cpp:418`) is a false alarm (56x/run, dedup-adopt-vs-condemn TOCTOU, GC correctly spares). Fix: downgrade to `ProfileEvent`+Debug, add a writer-side `BlobAdoptRacedCondemn` detector. RCA: `project_pr2073_ci_triage_2026_07_23`.
- **[CONDEMN-GRACE-WINDOW]** cool-down before condemning a just-zeroed blob — DESIRABLE — Removes the RECOVERED-INDEGREE noise but touches condemn timing/ack-floor invariants — needs TLA reasoning, protocol-step veto applies. Measure first (28 hot hashes, run 30019911967).
- **[REBUILD-SEAL-POINT-READ]** point-read closure for REBUILD seal discovery — HARD — `rebuildBaseline` finds the newest seal by probe-and-step-down over `gc/gen/<G>/` listings, not proof. Fix: a write-once `gc/gen/<G>/sealed` alias; a second residual (pruned-pool false "virgin" verdict) needs a marker that survives pruning, deferred. Reasoning: `.superpowers/sdd/2026-07-28-cas-ref-chain-stage-a-streams/task-8-report.md`.
- **[decode-cache-ttl-vs-gc-graduation-assert]** assert the decode-cache TTL stays below GC's condemn-to-graduate window — DESIRABLE — No assertion enforces `shard_decode_cache_ttl_ms` (200ms) against the two-round latency margin it depends on.
- **[CKPT-DAMAGE-NO-REPAIR-PATH]** {#ckpt-damage-no-repair-path} — KEEP, fix landed two residuals stand — `e337bb2c87dc` (verified on `cas-gc-rebuild`, absent from `antalya-26.6`) turned an undecodable `_ckpt` into a per-namespace hold. Open: (a) a held namespace still shuts the round-wide destructive gate; (b) no repair path — `publishCkpt` hits the same failure. Candidates: `fsck --repair`, or a recreate-on-undecodable arm — PROTOCOL-ADJACENT.
- **[ckpt-neverborn-gc-backstop]** {#ckpt-neverborn-gc-backstop} — HARD — Task 4-C correctly removed the unsafe `cleanupOrphanedBirthCkptBestEffort` call sites, trading for permanent debris that blocks decommission of a drained root. Needed: a backstop that reclaims only after an independent re-verified-empty LIST, never inferred from one conflict.
- **[repointRef non-resolving-key audit gap]** — MINOR — `CachedPartFolderAccess::repointRef` (`:283`) counts/logs a repoint even when `resolve(...)` returns `nullopt`; unreachable today. A defensive `throw LOGICAL_ERROR` would make the precondition explicit.
- **[REBUILD R4 residual — manifest-less blobs unreclaimable]** — TRACKED by design until R4 — A rebuild condemns nothing since Task 11, so a manifest-less blob is retained and shows as non-draining fsck `unaccounted`. Not a bug; no substitute reclamation (reopens the r5-finding-4 vector). Closes when the R4 registry lands.
- **[SUPPRESSED-HANDOFF-CONSUMPTION]** {#suppressed-handoff-consumption} — KEEP, minor bounded leak — A suppressed round's hand-off reclaim (`Gc/CasGc.cpp:1034-1059`) drops rather than defers a skipped generation; left to fsck. Reframed 2026-09-25: fires now only when `suppress_destructive` is true for a narrower reason (unproven frontier, lost CAS, a held namespace) since `kDefault` flipped — see `{#stage-b-7b-sequencing}` below — rarer, not gone. Pinned by `CasGcFrontierGate.TheHandOffReclaimIsInertUnderSuppression`.
- **[STAGE-B-7B-SEQUENCING]** {#stage-b-7b-sequencing} — DONE, kept for provenance — `bf396ffa50d1` re-keyed the fold-seal cursor to catalog incarnation, `58fd482a8008` flipped `kDefault = Authoritative` (both verified on `cas-gc-rebuild`, absent from `antalya-26.6`; `kDefault = Authoritative` re-confirmed live at `Gc/CasGc.h:65`, 2026-09-25). Was a HARD CONSTRAINT preventing a recreated-namespace edge miss; closed before the flip. One stale comment remains at `Gc/CasGc.cpp:2877-2878`.

### `[write-token-provenance-not-in-the-api]` ✅ CLOSED by the request engine's `resolved_by_read` (`996b61e04da6`, `adc0fa7007a3`) {#write-token-provenance}

**Closed.** Superseded by the `CasRequests`/`CasOperation` engine rewrite, not a direct response to this item: `996b61e04da6` removes the fallback `HEAD` for a write response carrying an incarnation slot, and `adc0fa7007a3` puts provenance in the API type itself (`Committed.resolved_by_read`, `Backend/CasWriteResult.h:42`). `Gc::acquireOrRenewLease` (`Gc/CasGc.cpp:4732`) now resolves through `CasOperation::readModifyWrite`, closing the GC-lease hazard described below. Tests: `resolved_by_read` in `gtest_cas_requests.cpp`, `gtest_cas_slot_occupy.cpp`, `gtest_cas_heartbeat.cpp`. `cas-gc-rebuild` only. The rest of this entry is kept as the record of the hazard that motivated the fix.

Found 2026-09-01 while designing self-authored mount reclaim; it is the blocker that killed revision 4
of `docs/superpowers/specs/2026-09-01-cas-self-authored-mount-reclaim-design.md`.

`ObjectStorageBackend::tokenFromWriteResult` decides how much to trust the token of a successful
conditional write, and the two dialects disagree about what to do when the response carries none:

- **Generation (GCS)** throws `CORRUPTED_DATA`, on the stated grounds that "there is no follow-up HEAD,
  so a broken or lying response can never be silently patched over by a later, **unrelated** read"
  (`CasObjectStorageBackend.h:187-193`);
- **ETag, and any backend with no write-time token at all** issues exactly that later, unrelated read —
  a fresh `HEAD` whose result is returned as if it were the write's token
  (`CasObjectStorageBackend.cpp:860-865`).

This is a deliberate scope boundary, not an oversight: `tokenFromWriteResult` was introduced by
`9b887ac8886` "Bind GCS CAS writes to exact response generations" (2026-08-21), whose own message says
"every other dialect keeps the pre-existing HEAD-fallback behavior unchanged". The GCS work saw the
attribution problem and fixed it only where it was working.

**The hazard.** Our write commits, the response carries no ETag, our fallback `HEAD` stalls, another
writer claims and arms authority, and our `HEAD` returns *their* token — which we then hold as the
token of our own write.

**There is already a consumer where this is not fail-safe.** `Gc::acquireOrRenewLease` takes its
`state_token` straight from the write result (`CasGc.cpp:4397`, `:4418` — both `casPut` results), and the round commit CASes
`gc/state` against it (`CasGc.cpp:944`). So a GC leader whose acquire response carried no ETag, and
whose fallback `HEAD` returned a *successor* leader's token, holds a token that matches the successor's
state — and its round commit succeeds over a leader that believes it holds the lease. The failing CAS
that is supposed to stop a displaced leader does not fail.

`MountLeaseKeeper::last_token` is the milder case: there the value is only an `If-Match` precondition,
so a wrong token costs a failed renewal and a fence rather than a wrong action — though even there the
containment is a property of the mount controller's own gates, not of the token. Nothing at the type
level distinguishes the two situations, and a consumer that treats the token as *identifying* our body
rather than merely *conditioning* the next write turns it into an overwrite of a live holder. Revision 4
of the reclaim design was exactly that consumer, which is how this was found.

(An earlier version of this entry claimed every current consumer was fail-safe. The GC lease path
refutes that; corrected 2026-09-02.)

**Fix direction: put provenance in `PutResult`, do not make the ETag path throw.** A `nullopt` is
structural for backends with no write-time token (local files, a non-S3 `IObjectStorage`), so throwing
would break them. Aligning the API means making "attributed to this write" versus "observed afterwards"
a fact the caller can see, after which each dialect maps onto it honestly and each consumer decides:
GCS keeps throwing (a missing generation there is a genuine anomaly), ETag and the token-less backends
return committed-with-unattributed-token.

Then revisit `MountLeaseKeeper::last_token`: an unattributed token is a guess, and the keeper might
better treat its own write as unresolved and re-resolve than store one.

Related: the *empty*-token case of this same fallback was fixed by `996b61e04da`
(`ObjectStorageBackend::isValidTokenValue` now rejects an empty/wildcard/list token at the
conditional-write entry); this item is about a *wrong* token, which that fix does not address. Both
come from the same three lines.

### `[gc-mf-cleanup-durable-retry]` Manifest-cleanup GC phase needs durable retry, not a cap {#gc-mf-cleanup-durable-retry}

**Found by a 24h soak (`soak-t6b-report.md`) after `gc_round_manifest_cleanup_budget` landed as one of
T6b's per-round work-envelope caps; the setting was removed entirely rather than tuned.**

The post-CAS `manifest_deletes` phase (`Gc::runRegularRound`, `Gc/CasGc.cpp`) is a **one-shot pipeline**:
the ref-log intake cursor that discovers each owner-removed manifest's `-1` edge commits in the SAME
round's CAS that produces the `mf_cleanup` set, before the deletes run. A cap on this phase does not defer
the excess to a later round of the same pipeline — a cap-declined entry is never re-derived, because the
cursor that would re-derive it has already moved past the log that produced it. The only remaining
reclaimer is the (much slower) orphan-manifest sweep backstop, which drains roughly 100 objects per round
and cannot keep pace with a real burst.

Soak evidence: run-1 (cap=5000) left 112,518 entries skipped, of which 110,218 were still unreachable at
checkpoint time (checkpoint FAIL). Run-2 (cap disabled) fully drained all 223,714 entries in-round with
zero left unreachable (PASS). The user decision was that the knob must not exist at all — a cap here
converts a bounded burst into a permanent leak, which is worse than no cap.

**Fix direction, when someone takes it:** real bounding needs the edge-consumption point moved to AFTER
the delete succeeds (durable retry), not before it, so a cap-declined entry stays discoverable by the next
round's intake instead of being silently dropped. This is a natural fit for a future
`gc-frontier-one-list` focused session (post-Stage-B), since it touches the same intake/cursor machinery.

### `[gc-reduce-zero-marker-dropped-on-carry]` a blob published during the reduce phase and then rolled back leaks until a rebuild {#gc-reduce-zero-marker-dropped-on-carry}

Raised by the head read-ahead consults (`docs/superpowers/worklogs/2026-09-03-cas-gc-head-read-ahead-consult.md`)
and NOT introduced by them. A blob observed absent when its `HEAD` is taken but present by the time the
merge closes it is not condemned that round, and the zero marker recording that is per-generation and
dropped on carry, so no later round re-examines the blob unless a delta touches it again. For ordinary
garbage this is correct: present-at-the-merge implies a publisher and therefore an edge. The residue is
a publication that lands mid-phase and then rolls back — that body has no edge and is never revisited.
The read-ahead widens the window quantitatively; it does not open the class. **Not sized**, and the fix
is a carried marker or a sweep concern rather than anything in the fold.

## GC cost and throughput {#gc-cost-and-throughput}

- **GC throughput collapse under a mass-DROP burst** {#gc-throughput-collapse-2026-07-25} — KEEP, historical RCA — Unbudgeted serial rounds diverge once DROP arrivals exceed one round's service rate (20s→1716s over 6 rounds, 188→20,046 candidates). Three defects: zero-depth meta-pool queue; permanent tombstones under the globally-enumerated ref prefix; `system.remote_data_paths` has no `disk_name` pushdown (tracked in `testing-and-ci.md`). The first two are the shape spec [Stage A](/superpowers/specs/cas-gc-rounds-in-minutes-design#stage-a)/[B](/superpowers/specs/cas-gc-rounds-in-minutes-design#stage-b) now re-derive from fresh data; kept as this incident's historical record.
### GC falls behind without bound under sustained small-part churn (measured 2026-09-15; formerly tracked separately, less precisely, as `[janitor-page-hardcoded]`/CAS-034) {#janitor-page-hardcoded}

Local msan rig, CI msan binary v26.6.4, 6 passes of the CI shard 2/3 test list on one server (3 h 24 min), RustFS
rc.3 in its own cgroup: GC round duration
24.3 s → 596.8 s (24x) against a 20 s interval, `CASGCPendingReclaim_cas_s3` 81 → 35,351 (436x), rounds per 10 min
23 → 1. Round length tracks the backlog (r = 0.92) more than the object store's delete latency (r = 0.57, RustFS
`delete` 0.4 → 44 ms with store size), so the loop is inside CAS: the bigger the backlog, the longer the round, the
more the backlog grows. GC is ~1/3 of all object-store operations in that run (run 3 events: ~890k GC reads, 290k
`CASGCMetaOps`, ~510k graduation HEADs of ~4.7 M S3 requests). On NVMe the tests do not feel it (iteration 6 / 1
median 1.17); on the CI runner's slow disk it is the likeliest amplifier of the msan/tsan CAS-S3 lane ramp
(issue #2298). The CI msan shard's own log shows round 27 at +61 min = ~135 s per round already in hour one.

**Which phases grow, and why** (live `system.cas_gc_log` of run 8, `cas_s3`, 5-minute windows; the `round` column
is 0 on `Phase` rows — use `round_id`):

| window | rounds | `defer_decision` | `fold_ref_intake` | `pending_deletes` | `fold_reduce` | `namespace_cleanup` |
|---|---|---|---|---|---|---|
| 0-5 min | 11 | 0.2 s | 0.5 s | 0.1 s | 0.6 s | 0.8 s |
| 15-20 min | 6 | 10.9 s | 9.7 s | 8.4 s | 3.9 s | 2.0 s |

- `defer_decision`: a full LIST of the ref-log prefix across every namespace each round, `ref_log_keys_listed`
  2,308 → 19,415, `namespaces_seen` 105 → 434 — of which `dead_life_debris` 51 → 403 (93-95 %): with one live test
  table on the server, almost everything the LIST walks is dropped tables' debris waiting for cleanup.
- `fold_ref_intake`: one GET per ref-log record (`logs_applied` 613 → 2,102 per round); the fold read-ahead (in `antalya-26.6` since `8f6cd74a3f5`, 2026-09-05, so ON in this run at its default `cas_gc_read_concurrency=16`)
  branch (2.4x intake) targets exactly this.
- `pending_deletes`: `deleted` 60 → 1,181 per round at ~7 ms each (exact-token DELETE + graduation HEAD); RustFS
  `delete` latency itself grows 0.4 → 44 ms with store size (run 7), which is where the loop closes.
- `fold_reduce`: one HEAD per zero-transition candidate.
The other 13 phases stay in the tens of milliseconds.

**Why the debris cleanup does not keep up** (`CasGc.cpp:352-386`, `CasNamespaceJanitor.cpp`): the janitor runs ONCE
per round over ONE page of 1,000 keys (page size hard-coded at `CasGc.cpp:363`), deleting dead-life objects one by one
(HEAD + exact `remove`; the bulk-delete path is not used), and it runs AFTER the LIST/intake phases, so what it removes
was already listed and read in the same round. Its throughput is `page × rounds/min`, and rounds/min collapses as the
listed debris grows (11 → 4 rounds per 5 min; namespace removals per window 417 → 10 while 403 dead lives waited):
a positive feedback loop. Namespace deaths themselves are folded (`new_removals`, ~35-40 per folding round). No
setting bounds this: `gc_round_ref_cleanup_budget` (5000) is not the limiter.

Asks, in order of size:
1. **Small, measurable first:** make the janitor's pages per round and page size settings (`gc_round_janitor_pages`,
   default 1; `gc_janitor_page_keys`, default 1000) and let a round run pages while dead candidates and a time budget
   remain; move `namespace_cleanup` BEFORE `defer_decision` so a round does not list what it is about to delete.
   A/B on the msan rig (`pages=10`) in one run. Spec [B4](/superpowers/specs/cas-gc-rounds-in-minutes-design#b4-janitor)
   makes the page count/size settings-driven; does NOT narrow the LIST scope to the removed namespace — still open.
2. **Medium:** batch the janitor's deletes through the existing bulk-delete path (`gc_bulk_delete_chunk_keys`; the
   LIST already carries etags, skip the per-key HEAD where the token is known), and delete the covered ref logs of
   dead lives together with them instead of waiting for retention prune.
3. **Structural, the real O(debris) fix:** replace the global hint enumeration (`enumerateRefPrefix`,
   `CasGc.cpp:3955`: one LIST of `<prefix>/cas/ns/stream/` over everything incl. dead lives) with the catalog cut
   (already one GET per round, `CasRefCatalog::read` in `listRefPrefix` `:4003`) plus one frontier probe per LIVE life
   (GET of `refLogKey(life, last_folded + 1)`, 404 = unchanged — the same probe fold intake already makes for
   `frontier_proven`/`absent_probes`), and a per-life LIST of `<prefix>/cas/ns/stream/<life>/_log/` only for the
   lives that fold. Run 8 at 20 min: 20 LIST pages / 19,415 keys (93 % debris) would become ~30-50 small requests
   with zero debris sensitivity. A cross-round index of dead lives does NOT help by itself: S3 still walks their
   keys, only the parsing is saved. Also note `gc_fold_threshold = 1` (`CasPool.h:160`) means every round with one
   new log anywhere folds and the defer verdict is computed AFTER the full LIST, so a deferred round pays the whole
   enumeration anyway (see the corrections below on the "deferred=0" claim).
4. Bound the work per round / make the drain rate independent of the backlog (batching deletes; merge the fold
   read-ahead is already in: `cas-gc-fold-read-ahead` was merged into `cas-gc-rebuild` as `e377741c725` and reached `antalya-26.6` as `8f6cd74a3f5` on 2026-09-05, so run 8 measured intake WITH it; the remaining lever is `[gc-intake-manifest-edge-serial-chain]`); back off rounds when the
   store's delete latency is high instead of stacking longer rounds.
5. Export `cas_gc_log` round and phase durations to the CI artifacts so this is visible per run.

Corrections from the run-8 `cas_gc_log` dump (brainstorm rev.2,
`docs/superpowers/specs/2026-09-15-cas-gc-dead-namespace-debris-cleanup-design.md`):
- "deferred = 0 throughout" is wrong for run 8: 12 of 85 rounds were deferred.
- The janitor's limiter is REQUEST COUNT, not page count: over 85 rounds it visited 55,285 keys and deleted 22,888,
  a full 1000-key page costs 1.3-7.4 s (~7 ms per key: HEAD + exact DELETE, serial). Raising pages per round first
  would lengthen rounds and partly cancel itself. Revised order: (1) batch the janitor's write-once `_log`/`_snap`
  deletes through `removeChunkWriteOnceOrOneByOne` (~60 lines, no license change; a page becomes 1-2 batch requests),
  (2) page settings at the existing phase position (the reorder before `defer_decision` is NOT invariant-preserving:
  `suppress_destructive` comes from the fold verdict), (3) the targeted drain of retired lives
  (`[targeted-drain-of-retired-lives]` below).
- `gc_round_ref_cleanup_budget` IS the limiter on the rounds where covered-log cleanup is live: of 73
  `ref_object_cleanup` rows, 66 deleted nothing and 6 sat exactly on the 5,000 cap — bimodal, i.e. cleanup fires only
  for lives that hold a checkpoint-authorized snapshot (`planRefCleanup` returns early without a checkpoint,
  `CasRefProtocol.cpp:822-835`), and short-lived test tables never publish one (run 8: ~40 `_snap` keys for 434 lives).
  Full detail on this class of aborts: `{#covered-log-cleanup-aborts-on-catalog-etag}` below.
- Safety notes from the codex review of the brainstorm: `retired_lives` are lives observed absent/replaced during
  reconciliation, not proof that this actor erased them; the janitor's liveness lambda is a per-page cached
  `authority_held` (`CasGc.cpp:364-367`, `4692-4703`) and cannot fence a concurrent new leader; `Removing` lives still
  have recovery readers (`namespaceStillLogicallyPresent`, `dropNamespaceImpl` retry), so deleting on the DROP path
  is rejected. Open question for the owner: may the round's drain phase perform physical deletes at all.

CI-side mitigation without GC changes: system logs of the CAS lanes on a plain disk (every log flush onto `cas_s3`
is new ref logs and objects), which also shrinks the RustFS object count.

Related: RustFS `delete`/`delete_version` latency growth with object count (RustFS side),
audit [F2](/superpowers/reports/otel-demo-cas-s3-budget-audit#f2) (`[PART-REMOVAL-REPOINT]` above).

### `[covered-log-cleanup-aborts-on-catalog-etag]` Covered-log cleanup aborts before its first delete whenever any other namespace changed since the fold (found by the janitor review, 2026-09-16; formerly tracked separately, less precisely, as `{#ref-cleanup-whole-catalog-token-stillness}`/CAS-079) {#covered-log-cleanup-aborts-on-catalog-etag}

Found by the codex review of the janitor brainstorm (`docs/superpowers/reports/2026-09-15-cas-gc-dead-namespace-debris-cleanup-codex-reviews/review_r2.md` MAJOR 1,
`BRAINSTORM.md` rev.3a §1(d) and §7 question 5). Outside the janitor's scope proper and may be the larger lever;
absorbed here as one entry with `{#janitor-page-hardcoded}` since both feed the same backlog-runaway loop. Spec
[B3](/superpowers/specs/cas-gc-rounds-in-minutes-design#b3-cleanup-licence) names this as "adjudicated not yet
built" (per-row licensing, per-namespace refusal); this entry is the measurement and plan behind that line. The
write-hotspot context it cited, `{#ref-catalog-write-hotspot}` (`performance.md`), still applies.

**Mechanism.** `Gc::cleanupRefObjects` deletes the `_log` objects of **live** lives that are already covered
by their checkpoint. Before its first chunk it re-reads the pool catalog and, in `authorityHolds`
(`Gc/CasGc.cpp:3590-3605`), requires `current_catalog.etag == folded.catalog_cut->etag` — equality of the
**whole pool catalog** with the cut taken at fold time — in addition to the per-namespace checks (row
unchanged, life resolves to the same incarnation, `gc/state` present with the same owner/sequence). The
etag condition is the strict one: any `CREATE` or `DROP` of any other table between the fold and the cleanup
changes it, and the caller then stops the entire pass before the first delete (`:3690-3691`). The code
comment says the strictness is deliberate: "every irreversible delete is still licensed by the SAME complete
catalog observation and GC lease that adopted the fold".

**Measured (run 8 of the msan rig, `RIG/run8/samples/cas_gc_log.tsv`, 85 `ref_object_cleanup` rows, stage
read off each row's ProfileEvents: the catalog GET classifies as `Other`, `/cas/ns/` reads as `Root`):**

| stage reached | rows |
|---|---|
| no object read at all (no checkpoint / no `checkpoint_snapshot_id`; `planRefCleanup` returns early) | 37 |
| nonempty chunk built, then `authorityHolds` false at the catalog comparison | **40** |
| failed after the catalog comparison | 0 |
| deleted something | 8 (7 at the 5,000 `gc_round_ref_cleanup_budget` cap) |

Under a workload that creates and drops tables continuously, the etag rarely survives from the fold to the
cleanup; the phases in between (`pending_deletes`, `fold_reduce`) are exactly the ones that grow over the run
(`{#janitor-page-hardcoded}`), so the window widens as the round lengthens.

**Why it matters beyond cleanup.** Covered logs the pass fails to delete stay under the live life's `_log/`,
are listed by every global `enumerateRefPrefix` LIST (`defer_decision` growth), and become dead-life debris
for the janitor when the table is dropped. So the aborts feed the very debris that S1/S2 make the janitor
remove faster: **longer round → cleanup aborts more → more objects → longer LIST → longer round.** The 8
successful rows deleted 23,840 objects; 40 aborted rounds at up to 5,000 each is the order of the backlog
this can produce per run.

**Plan, three steps, no step before the previous one's result:**

0. *Estimate from existing data, no code.* From run 8's Phase rows: the window between the fold's catalog cut
   and `ref_object_cleanup` per round, against the lane's CREATE/DROP rate. If the window is seconds and the
   rate is several per second, survival of the etag is structurally near zero and the question is not "how
   often" but "is whole-catalog equality required at all".
1. *Measurement with code, one local run.* Split the stop reason in `authorityHolds` into ProfileEvents:
   etag-only (row and life of THIS namespace unchanged), row/life changed, `gc/state` absent or owner/sequence
   changed; plus, per round, the number of objects planned and not deleted because of the abort. Release
   build in lane-g, local ca-s3 stateless lane, 60-90 min, dump `cas_gc_log` and events. Result: the share
   of aborts whose per-namespace licence was intact, and the undeleted volume per run. ca-impl, half a day.
2. *Brainstorm on the licence, only if etag-only dominates.* The narrow question: which observation must be
   unchanged to delete write-once `_log` keys of one life that its own checkpoint covers. Candidates:
   (a) narrow the licence to row + life + `gc/state`, dropping whole-catalog etag equality — the argument is
   that the keys are write-once, belong to one life, and the plan derives from that life's durable
   checkpoint, so other namespaces' churn changes neither the key set nor its coverage; the counter-argument
   to answer is the author's "same complete observation" comment, i.e. whether the fold seal or this life's
   coverage depends on the rest of the cut; (b) keep the licence, re-read the catalog and take a fresh cut
   immediately before the cleanup inside the same round, shrinking the window to milliseconds; (c) move the
   cleanup phase to right after the fold while the cut is fresh. Constraints in: the round stays one-pass,
   LIST trust is not reopened, no new object kinds. ca-arch with the code, then codex in a loop capped at
   three rounds.

**Not to do:** relax the etag check directly. It is a licence on irreversible deletes and the comment says
the strictness is intended. Numbers first, then the invariant, then code.

Related: `{#janitor-page-hardcoded}`, `[targeted-drain-of-retired-lives]` below,
audit [F2](/superpowers/reports/otel-demo-cas-s3-budget-audit#f2).

### `[targeted-drain-of-retired-lives]` FROZEN, not designed: draining a retired life's prefixes right after reconciliation (approach A of the janitor brainstorm, 2026-09-15) {#targeted-drain-of-retired-lives}

Source: `docs/superpowers/specs/2026-09-15-cas-gc-dead-namespace-debris-cleanup-design.md` rev.3 §A, and the three codex review rounds
(`docs/superpowers/reports/2026-09-15-cas-gc-dead-namespace-debris-cleanup-codex-reviews/`). Status by owner decision 2026-09-16: **frozen as "not
designed"**. It is not scheduled after S1/S2; whether it is worth building at all is decided by the S1/S2
measurements (`{#janitor-page-hardcoded}`).

**The idea.** `pre_fold_ref_drain` erases every eligible `Removing` row and the reconciler returns
`retired_lives` (`Gc/CatalogLifecycleReconciler.h:32-34`); both per-life prefixes are constructible from the
incarnation (`Formats/CasLayout.h:134-143`). A would, after a drain that reports `Authoritative` and
`DrainComplete`, and only for a life that the unambiguous `final_catalog_cut` no longer resolves, delete that
life's `namespaceStreamPrefix` (batched write-once deletes) and `namespaceStatePrefix` (exact removes), leaving
the global janitor page as a backstop. Cost per dead life: 2 LISTs, 1 batch delete, a few exact removes.
~150 lines plus a budget setting. The attraction: today's janitor finds dead-life debris only by paging the
global `<prefix>/cas/ns/stream/` LIST, one 1,000-key page per round, and 93-95% of listed keys are that debris.

**Why it is frozen: five review findings, each a legitimate reader or writer of a retired prefix that the
design did not fence, and each answered in the brainstorm by one more precondition rather than an
invariant.**

1. `retired_lives` records lives *observed* absent or replaced, including on `FencedOut`
   (`Gc/CatalogLifecycleReconciler.cpp:99-118`, `Pool/CasRefCatalog.cpp:474-484`, `525-536`); it is not proof
   that this actor erased the row. Only the conjunction with an unambiguous final cut is a licence (r1 #1).
2. Two GC actors can overlap. The janitor's liveness callback is a flag refreshed once per page
   (`Gc/CasGc.cpp:364-367`, `4692-4703`); `CasOperation::gate` samples it without refreshing the lease
   (`Backend/CasRequests.cpp:413-425`) and cannot cancel an in-flight delete. A successor leader's janitor and
   a stale leader's drain can hit the same prefix. Per-chunk authority refresh improves stopping, it is not
   fencing (r1 #2).
3. `Removing` lives still have readers: `namespaceStillLogicallyPresent` and a retried DROP both call
   `ensureRefTableRecovered` first (`Pool/CasRefLedger.cpp:5063-5071`, `5146-5155`), and recovery over drained
   inputs **throws `CORRUPTED_DATA`**, non-transiently: `chooseRecoveryGrounding` throws for a `Live` or
   `Removing` namespace with no readable `_ckpt` (`Pool/CasRefCkpt.cpp:142-151`); partial drainage throws on
   missing committed logs under an unchanged checkpoint (`:1067-1081`); the outer retry treats corruption as
   final (`:1489-1494`). Rev.2's "a cold probe answers present conservatively" and "a DROP retry proceeds to a
   terminal append" were both wrong. The window exists today (the janitor deletes the same bytes after the row
   erase); A narrows it from many rounds to part of one round and so raises the hit rate (r1 #4, r2 #2).
4. A drained prefix is not guaranteed empty: a snapshot publisher captures live state and then creates at the
   captured life's `_snap` key (`Pool/CasRefLedger.cpp:4543-4544`); its admission checks mount generation and
   runtime flags (`:4445-4456`), which retirement invalidates but cannot un-send. A PUT in flight lands after
   the drain. "Nothing can write under a dead incarnation" is withdrawn; the true guarantee is only "a
   successor life uses a different prefix" (r2 #4).
5. Bulk deletion is accounting-atomic, not physically all-or-nothing (`removeManyWriteOnce` contract), so a
   partially drained prefix is a normal outcome, and `deletePrefixWholesale`'s `out_fully_drained` means
   "enumeration exhausted", not "prefix empty" (`Gc/CasGc.cpp:3733-3757`) (r2 #3).

The accretion is the signal: after two rounds A carried "publisher quiescence", "per-chunk authority
refresh", "own budget", "backstop stays forever", and two blocking open questions (may the drain phase perform
physical deletes at all; how the callers in 3 handle the exception). The system's recovery model treats a
missing checkpoint of a `Live`/`Removing` life as corruption, not as "nothing to recover", and there is no
durable marker "this prefix may be destroyed" that recovery, the publisher, a DROP retry and a second GC
actor all consult. Without that marker A is a race by construction and the backstop does the real work.

**What would unfreeze it.** A retirement-fence invariant designed first, on its own: who may read or write a
retired life's prefix and until which durable event; recovery over a fenced life refuses (not
`CORRUPTED_DATA`), publisher admission and DROP retry check the fence, and the fence is visible in the
catalog cut a successor leader derives. Only then is a targeted drain a licence instead of a bet. That is
architectural work, not a janitor optimisation, and it is justified only if S1/S2 leave the janitor unable to
keep up with debris production.

**Decision pending from the owner, no default:** whether a round's drain phase may perform physical deletes
at all. Today the drain erases catalog rows under the lease (`Pool/CasRefCatalog.cpp:514-517`) and performs
no physical cleanup; nothing in the code states either way.

### `[gc-namespace-janitor-one-page-per-round-cannot-keep-up]` The namespace janitor examines one 1000-key page per round, so dead namespaces accumulate and every round's ref-prefix LIST grows with them {#gc-namespace-janitor-one-page-per-round}

Measured live (2026-09-04, `system.cas_gc_log` of the parallel stateless lane on the `cas_s3` disk,
`cas_gc_interval_sec = 5`, minio backend). Every regular round pays one LIST of `cas/ns/stream/` inside
`defer_decision` (three `system.stack_trace` samples of `CasGcSched` all sat in
`ObjectStorageBackend::listUnder`); the round is never `Deferred` on this disk, so
`[gc-deferred-round-pays-full-list]` does not apply — this is the FOLD round's fixed cost:

| window | rounds | avg `defer_decision` | keys listed | namespaces listed |
|---|---|---|---|---|
| 21:10 | 74 | 673 ms | 3 247 | 95 |
| 21:30 | 33 | 6 883 ms | 24 873 | 520 |
| 21:50 | 13 | 19 678 ms | 69 952 | 1 436 |

About 0.28 ms per key, i.e. ~280 ms per 1000-key page. At the end of the window 40 `MergeTree` tables
were alive while the LIST returned 1 404 namespaces with ~49 `_log` keys each: the prefix is almost
entirely dead namespaces of dropped test tables. `namespace_cleanup` (`runNamespaceJanitorPage`,
`janitor.runOnePage`) examines exactly one page per round (`janitor_pages = 1`, `janitor_keys = 1000`)
and deleted 150–300 keys per round, while the lane added ~27 000 keys per 10 minutes. Totals over the
46-minute run for `cas_s3`: `defer_decision` 889 s of ~1 830 s of GC wall time, then `fold_reduce`
393 s and `pending_deletes` 191 s. The pause between rounds (5 s) only divides the LIST count; the
janitor's page bound is what lets the LIST grow.

Proposed:

1. Give the janitor more than one page per round when the listing says the pool is debris-heavy —
   e.g. keep taking pages while the round's `namespaces_seen` exceeds the live-namespace count by an
   order of magnitude, bounded by a request budget, so a quiet pool still pays one page.
2. Lane config: raise `cas_gc_interval_sec` from 5 to 20 in (done 2026-09-04)
   `tests/config/config.d/cas_s3_storage_policy_for_merge_tree_by_default.xml` and
   `cas_storage_policy_for_merge_tree_by_default.xml`. The interval is a pause after the round, not
   a period; with 10 s rounds the scheduler ran two thirds of the time. Tests that need GC call
   `SYSTEM CAS GC` explicitly (18 tests) or configure their own disks with a 1 s interval, so they
   are unaffected. The product default (60 s) stays.
- **[fold-edge-run-memory]** {#fold-edge-run-memory} CAS-035 — KEEP — `foldDeltasIntoGeneration` holds two full in-memory copies of the whole shard edge-run (`Gc/CasBlobInDegree.cpp:389,678-681`; the whole pool at default `gc_shards=1`). Same O(pool)-per-round class as `[Lever B]`/`[gc-snapshot-log-structured-runs]` below. Its "can't skip the enumeration, the defer signal comes from it" caveat is pre-spec-B1 and needs re-examination once B1 lands (distinct from audit `#f19`'s LIST-memory cost, which B1 does fix).
- **[gc-snapshot-log-structured-runs]** {#gc-snapshot-log-structured-runs} hot-pool snapshot rewrite is O(edges) per pass — DESIRABLE — Dominant remaining byte cost; streaming reads/reference-parent runs (T2/T0) are DONE, log-structured incremental runs are not. Spec [C4](/superpowers/specs/cas-gc-rounds-in-minutes-design#c4-not-in-c) defers a graduation-cursor variant. Audit `#f29`: run size 11→125 MiB/round as backlog grew; Stage C's shorter rounds pay this more often per day.
- **[Lever B]** Incremental point-updatable in-degree — DESIRABLE — Makes a non-idle small-delta round O(delta); removes the O(shards) per-round ref-prefix LIST. Measured: 87ms@400 parts → 93s@10k tables → 398s@100k parts. Not touched by the spec (scoped to O(new), not a point-updatable structure).
- **[ADAPTIVE-GC-CADENCE]** journal-pressure-triggered fold — DESIRABLE — Trigger should key on per-shard pressure, not changed-shard count; needs a soak-swept knee. Spec [C3](/superpowers/specs/cas-gc-rounds-in-minutes-design#c3-pacing) adds a related but distinct deadline-driven pacing.
- **[distributed gc_shards>1 parallel GC]** shard claim/scheduler — DESIRABLE — Attempt-scoped generations (prerequisite) DONE; multi-worker claim/scheduler not built. Spec [§7](/superpowers/specs/cas-gc-rounds-in-minutes-design#multi-node-direction) records the target shape, "not implemented here."
- **[B148]** HEAD storm at retire — stored-token optimization — PARTIAL — Retire/recheck O(universe) HEAD phases are gone; residual condemn HEAD bounded by newly-condemned candidates. Stored-token skip needs a manifest schema change, still not built (no `stored-token`/`StoredToken` symbol on either branch, checked 2026-09-25). Related: **[PROMOTE-REVALIDATION-MINIMIZATION]**.
- **[process_epoch → writer_epoch]** stamp unification — DESIRABLE, flagged — `writer_instance_id` greps nowhere on either branch (checked 2026-09-25) while `process_epoch`/`writer_epoch` are both live. Unification may already have happened under a renamed field, or the premise is stale — no commit found either way.
- **[PART-REMOVAL-REPOINT]** part removal pays a wasted repoint of the doomed ref — DESIRABLE — Removal commits a full ~3-PUT repoint on a `delete_tmp_*` ref the next step deletes; same-txn supersede-clear (T8) elides it, the cross-txn removal flow doesn't. 2026-07-15: ≈22% of writer PUTs. Audit `#f2` (2026-09-25): 190,853 repoints/day, ~8% of PUTs, a third of GC intake — reconfirmed and quantified. Spec schedules the fix as a writer task parallel to Stage A (§10 item 6, §11); `concurrent_part_removal_threshold_for_remote_disk=1` is an unapplied stopgap, not the fix.
- **[GC-EMPTY-SHARD-PROBES]** constant per-round 404 probe floor — DESIRABLE — ≈1,174 `DiskS3ReadRequestsErrors`/round, constant regardless of round work (empty-shard structural probes); dominant class on a small/idle pool. Removed by `[Lever B]`, not built.
- **[REF-QUEUE-WAIT-MEASURE]** — superseded by audit `#f20`/`#f30`/`#f31` (2026-09-25), a far more precise re-measurement of the same ref-lane queue-wait cost (748ms of an 887ms insert) with an owner-approved fix.
- **[orphan-sweep-byte-budget]** orphan-manifest nomination is object-count-bounded, not byte-bounded — DESIRABLE — `nomination_budget` (`CasOrphanManifestSweep.cpp:605-638`) caps count; 256 MiB manifests can still reach ~25 GiB retained/round.
- **[gc-files-prefix-not-listed]** verify `_files` debris is reclaimed without a GC LIST of `rootsPrefix()` — DESIRABLE — Fold LISTs `casRefsPrefix()` but not `rootsPrefix()` (confirmed on both branches, 2026-09-25). Audit `#f16` confirms `roots/<ns>/files/<name>` has no index and is reclaimed only by the writer's own `removeNamespaceFile`. Open: does that remove fire on every orphaning path?
- **[CA-LOG-TABLES-RESTART-COST]** {#ca-log-tables-restart-cost} — A 6/40 soak restart took 178.9s against a 180s gate, 138.1s reloading CA log tables' Outdated parts. Direction: TTL/partitioning, bounded churn, lazy load. Audit `#f1` confirms the same class on otel.demo (`system.*` = 86% of parts) and recommends a local storage policy — cross-check `operability-and-introspection.md`.
- **[gc-checkpoint-timeout-tsan]** soak GC-checkpoint timeout assumes normal-speed throughput — MINOR, green-debt — Can blow its budget under TSan overhead while genuinely converging. Fix: sanitizer-aware multiplier.

### `[gc-multidelete-conditional-gap]` batch `DeleteObjects` cannot replace GC's exact-token deletes as-is {#gc-multidelete-conditional-gap}

T9's destructive-baseline soak measured **944,155** individual `DiskS3DeleteObjects` calls across a
single 90-minute specimen's four destructive families (`pending_deletes`, `manifest_deletes`,
`ref_object_cleanup`, generation pruning inside `round_commit`) — every one a single-key
`removeObjectIfTokenMatches` call (`Backend::deleteExact`, `Backend/CasObjectStorageBackend.cpp:955`)
carrying an `If-Match` ETag precondition, the exact-token-match safety property that stops GC from
deleting a body a writer has already displaced (the CAS resurrection-safety invariant). ClickHouse
already has a working batch-delete path — `deleteFilesFromS3` (`IO/S3/deleteFileFromS3.cpp:80`,
default batch 1000, `IO/S3Defines.h:48`), reachable via `S3ObjectStorage::removeObjectsImpl` — but
no CAS delete-family call site uses it, including `deletePrefixWholesale`, which already LISTs a
whole prefix in pages and still deletes each listed key one at a time
(`Gc/CasGc.cpp:3563-3570`). The reason is not an oversight: the batch `DeleteObjects` request only
sets `Key` per `Aws::S3::Model::ObjectIdentifier` (`deleteFileFromS3.cpp:118-122`) — AWS's batch API
has no per-key conditional precondition, so wiring GC's existing calls to it as-is means dropping
the exact-token check, which is a correctness regression, not an optimization.

**Ceiling, if the conditional gap is ever closed** (e.g. a design that proves a delete cohort
collision-free at round-commit time without a per-key check): `944,155 → ⌈944,155/1000⌉ = 945`
batch requests, a >99.9% cut in delete request count. This is a REQUEST-COUNT ceiling, not a
wall-time prediction — the soak's backend (RustFS) measures ~650–700µs mean per-delete latency
(`DiskS3WriteMicroseconds`/`DiskS3DeleteObjects` ≈ 645µs for `pending_deletes` alone), far below
real S3 RTT, so the wall-time win against AWS S3 is unmeasured by this specimen and likely larger
than what RustFS would show.

**Falsification:** if no design can prove a cohort of exact-token deletes collision-free without a
per-key conditional (i.e. the safety property is fundamentally incompatible with a keys-only batch
API), this item stays permanently blocked and the correct scope is delete-side concurrency
(`[gc-delete-concurrency-serial]`) instead. Full measurement:
`docs/superpowers/reports/2026-08-04-gc-destructive-baseline-perf.md#opp-multidelete`.

**Closed by construction for the three write-once families** (manifest bodies, ref `_log`, ref `_snap`):
see `[gc-manifests-are-immutable-so-reduce-and-deletes-can-be-cheap]` and the design it points to. The
gap remains exactly as stated for blobs.

### `[gc-pending-deletes-fan-out]` (formerly `[gc-delete-concurrency-serial]`) GC's destructive deletes run with almost no overlap {#gc-pending-deletes-fan-out}

The same T9 baseline measured `pending_deletes` and `manifest_deletes` running near-serially
despite already dispatching through a thread pool: `pending_deletes` wall (208.77s, ch1) is 87% of
the SUM of its individual requests' `DiskS3WriteMicroseconds` (181.3s) — the requests overlap very
little. `manifest_deletes` shows the same shape (409.52s wall vs. 368.56s summed, 90%). Together
these two phases are 618.29s of ch1's 4352.1s total phase wall (14.2%) in this specimen. A bounded
worker pool issuing K concurrent conditional deletes (same shape as the existing `meta_pool`) could
plausibly cut this toward `wall/K`, independent of `[gc-multidelete-conditional-gap]` — the two
levers compose (concurrent batch calls) rather than compete, once/if the conditional gap closes.

**Falsification:** if concurrent deletes against the same backend/prefix trigger throttling
(RustFS or S3 `SlowDown`/503) at a K nobody has tried yet, the real win is smaller than linear —
this baseline never issued concurrent deletes and cannot rule that out. Full measurement:
`docs/superpowers/reports/2026-08-04-gc-destructive-baseline-perf.md#opp-delete-concurrency`.

**Merged: `[gc-delete-concurrency-serial]` (the measurement below) + `[gc-pending-deletes-fan-out]` (the task that acts on it) into one entry, keeping the newer id/anchor.**

#### TASK `[gc-pending-deletes-fan-out]` Fan the blob `pending_deletes` loop out over a bounded worker pool; each blob keeps its exact-token HEAD + conditional DELETE

**Measured 2026-09-04, real-AWS smoke soak:** `pending_deletes` 351 s for 1261 blobs and 246 s for 882
(one HEAD ≈100 ms plus one single-key conditional `DeleteObjects` ≈150 ms per blob, serial, ~0.28 s per
blob); with `fold_reduce` above, these two phases made the 427 s and 328 s rounds that the soak harness'
300 s fixpoint bound cannot survive (`history=[3517, 2779]`). GCS run 2 showed the same at 551 s for
2731 blobs (`[gc-blob-pending-deletes-now-dominant]`); the T9 baseline showed it at 208 s
(`[gc-delete-concurrency-serial]`). Those two entries are the measurement; this is the task.

**Task.** The loop at `Gc/CasGc.cpp:700` does per blob: `op.head` → token compare → `op.remove(key,
observed etag)` → event, outcome row, meta scheduling. The network pair is independent per key and its
safety is per key (I5: exact-token delete; a resurrected blob mismatches and is left alone), so
concurrency changes nothing about safety. Shape: chunks of N = `cas_gc_read_concurrency` entries from
`redelete_now`; each worker runs head → compare → remove through an operation resumed under the round's
admitted generation, exactly as `GcReadAhead` workers do, so a fence that moves under the round fails the
worker's request the way it fails the main one; the worker returns `(Removal, observed)`; the owning
thread then runs the existing bookkeeping serially in the original order. `authority_held` is checked
before each chunk, as the serial loop checks it per entry. Not the read-ahead: that design never runs
the destructive decision, and this task keeps that rule (the decision and the delete run in the worker
only because they are one exact-token request; nothing is prefetched). Tests: a gtest with an
instrumented backend asserting that N deletes overlap, that a fence mid-chunk stops the remaining
chunks with no delete issued after it, that a mismatch during the chunk is `Replaced` and leaves the
object, and that outcomes/events/meta calls are identical to the serial loop's; the `CAS*` gate; a
soak round with mass removal reading `pending_deletes` from `system.cas_gc_log`. Acceptance: phase wall
≤ 2 × (serial wall / N) on the AWS stand; no change in `objects_deleted`/`spared`/`replaced` counts
for the same input. Falsification stays as in `[gc-delete-concurrency-serial]`: SlowDown/503 at a
concurrency nobody has tried; start with N = 8 and measure.

**Worth checking first, separately:** AWS added conditional deletes; if `DeleteObjects` accepts a
per-key ETag condition that general-purpose buckets enforce, the whole phase becomes one request per
1000 blobs. A store capability, so it would have to be proven by the capability probe the way the
exact-token DELETE 412 is proven today; not assumed.

### `[gc-fold-intake-readbuffer-head]` ✅ CLOSED by the request contract's read path (`e272e18f02c`, 2026-09-03) {#gc-fold-intake-readbuffer-head}

**Closed.** The backend's `read` no longer HEADs before it GETs: it goes through
`readSmallObjectAndGetObjectMetadata`, one `GetObject` whose own response carries the etag. The
2026-09-01 soak still shows the 1:1 pairing because its binary predates that commit; the first soak
against a later build is the confirmation. The rest of this entry is kept as the record of how the
pairing was found.

T9's baseline found `fold_ref_intake` — the single largest wall-time phase in a destructive round
(2303.0s of ch1's 4352.1s phase wall, 52.9%) — issuing `DiskS3GetObject` and `DiskS3HeadObject` in
an exact 1:1 pairing (1,183,381 each). This is NOT a regression of the predecessor's
`{#opp-fold-head}` (drop the HEAD in `foldManifestEdges`), which is confirmed delivered — the
source comment at `Gc/CasGc.cpp:1301-1312` states the HEAD was removed because the following GET
already carries the absence signal. The HEAD still visible here is a different, generic one:
`ReadBufferFromS3::getObjectSizeFromS3` (`IO/ReadBufferFromS3.cpp:463-469`) issues a `HeadObject`
to learn `Content-Length` before every ranged `GetObject`, for every S3 disk read in ClickHouse —
not CAS-specific.

**Not yet sized.** This entry only establishes that the pairing exists and where it comes from;
whether an existing known-size read-buffer constructor already avoids it on some call paths, and
what the real win would be, is unmeasured. **Falsification:** if the size-probe HEAD is required
for correctness on every generic S3 disk consumer (e.g. detecting a truncated/resized object
mid-read), this is a ClickHouse-wide question and does not belong on this CAS backlog at all. Full
measurement: `docs/superpowers/reports/2026-08-04-gc-destructive-baseline-perf.md#opp-fold-head-successor`.

### `[gc-intake-manifest-edge-serial-chain]` one manifest round trip per ref log is what `fold_ref_intake` still cannot overlap {#gc-intake-manifest-edge-serial-chain}

Measured (`docs/superpowers/worklogs/2026-09-04-cas-gc-fold-read-ahead-measurement.md`): with the fold
read-ahead on, `fold_ref_intake` improves by about 2.4x against a fixed per-request latency and then
stops, and the reason is a one-to-one count — 83 ref-log GETs against 83 manifest GETs in the measured
round. Ref-log keys are arithmetic, so the lookahead knows the next window of them before reading any;
a manifest key is named by the decoded body of the log that owns it, so the earliest the round can know
manifest N's key is after log N has been read AND decoded. Hinting "all the edges of this log" hints one
key whenever a log names one edge, which overlaps nothing with itself.

The fix needs a different mechanism than key arithmetic: decode an ALREADY-FETCHED later log purely to
learn its manifest keys and hint them, leaving the fold's own decode, its order and every decision
exactly where they are. That means a peek on the read-ahead that does not consume, and a speculative
decode whose failure must be discarded rather than acted on — the real decode still runs in order and
still holds the namespace at the right position. **Not sized**, and it is a design question rather than
a tactical one, which is why it is not part of the read-ahead change.

### `[gc-reduce-confirm-marker-read-ahead]` the graduation gate's meta re-check is the last serial read of `fold_reduce` {#gc-reduce-confirm-marker-read-ahead}

The fold's read-ahead (`GcReadAhead`, `cas_gc_read_concurrency`) now covers the checkpoints, the ref
logs, the manifest edges and the fresh zero-in-degree `HEAD`s. What remains serial in the reduce phase
is the graduation gate's `loadMeta` re-check, issued per carried condemned entry that has no in-process
confirmation — which, after a restart or a leadership change, is every entry graduating that round.
Its candidates are known only from the prior run's condemned sentinel rows, which the merge streams, so
hinting them needs a lookahead on the run cursor rather than a pre-pass over anything already in
memory. **Now sized enough to rank it:** with the zero-in-degree `HEAD`s read ahead, `fold_reduce` still
improves only about 1.2x against a fixed per-request latency, and this re-check is what it spends the
rest on (`docs/superpowers/worklogs/2026-09-04-cas-gc-fold-read-ahead-measurement.md`).

### `[gc-round-budgets-are-not-backpressure]` Round budgets throttle the consumer while the producer is unaware — the real fix is a time deadline {#gc-round-budgets-not-backpressure}

> Correction (2031-triage CAS-034, 2026-08-21): the title used to claim "four defaults changed" — those
> four budgets are still 5000 at HEAD, so the claim was stale and is removed rather than restated.

A per-round count cap is not backpressure. It bounds what GC does in one round while inserts and
merges — the producers of the work — know nothing about it. If arrival exceeds `budget × rounds/sec`,
the deficit is not smoothed, it accumulates. Whether that is harmless, degrading, or a leak depends
entirely on **what happens to the excess**, which turns out to differ per budget. Classified against
the code, not the names:

**A. Feedback loop (was capped, now unbounded).** `gc_round_graduation_budget`,
`gc_round_redelete_budget`. Excess is pushed back into `still_retired` "carry UNCHANGED"
(`CasBlobInDegree.cpp:472`), and the next round reads that list in full — `CasGc.h` marks the cost
`O(retired)`. So the round's cost grows with the debt while its useful work stays capped: rounds
lengthen, their rate drops, throughput drops, the debt grows faster. Worse than linear lag.

**B. Genuinely cursor-paced (unchanged, these caps are correct).** `manifest_sweep_list_budget_keys`,
`manifest_sweep_delete_budget_keys`, `gc_round_sweep_namespace_budget`,
`gc_round_sweep_recovery_op_budget`, `gc_round_prefix_wholesale_budget`. A cursor advances and never
regresses; a partially drained page or generation is simply finished next round. Nothing is
re-read, nothing accumulates. `gc_round_ref_cleanup_budget` is adjacent: it keeps no cursor but
`planRefCleanup` recomputes the same remaining candidates from durable state, so work is deferred,
not lost.

**C. A cap on one-shot work, i.e. a leak (was capped, now unbounded).**
`gc_round_handoff_prefix_wholesale_budget`. The struct's own comment says the hand-off "is a ONE-SHOT
event with no reclaimer behind it besides `fsck`: a generation it cannot fully reclaim this round is
never revisited (the parent-seal difference that triggers it does not recur)". This is the same shape
as the manifest-cleanup cap that was removed outright after a soak proved it leaked permanently.

**D. Audit loss (was capped, now unbounded).** `gc_round_outcome_entry_budget`. Nothing is retried on
exhaustion because the decision already happened; the only casualty is the audit row explaining it —
and it is dropped precisely on the busiest rounds, the ones an investigation would need.

**E. Not a throttle at all — an off switch (raised to effectively unbounded).**
`gc_frontier_probe_budget`. Exhaustion does not defer work: unprobed namespaces are simply unproven,
and one unproven namespace suppresses ALL destruction for the round (`CasGc.cpp:2047-2048`). It scales
with namespace count, i.e. with table count, so a value that is ample for ten namespaces becomes a
permanent GC stop for a large enough pool. **Its `0` cannot be redefined as "unbounded"**: unlike
every other budget here, `0` means "probe nothing", and the tests drive that exhaustion path
deliberately — so the default is spelled as a maximum instead. That inconsistency is itself an
operator trap and wants a proper sentinel.

**F. Memory bound, must stay capped.** `rebuild_edge_budget` — its comment is explicit that memory is
`O(budget)`, never `O(edges)`.

### What is still missing, and it is the real fix {#gc-budgets-need-a-deadline}

**A GC round has no time deadline anywhere in the code.** The count budgets have been serving as a
surrogate for one. That is why removing them is not free: a round holds the GC lease, and a round
that outruns the lease TTL gets fenced — the wedge class already fixed once in P3.1. The correct shape
is a per-round WALL-CLOCK deadline plus a cursor everywhere class A currently carries a list: the
round then does as much as it can inside its lease, stops cleanly, and resumes where it stopped
without re-reading the debt. Until that exists, the unbounded defaults above trade a silent
accumulation risk for a round-length risk, deliberately and with the user's decision.

Falsification for class A: with the caps off, a sustained-load soak should show round wall time
tracking arrival rate rather than climbing while `pending_condemned` climbs.

### `[gc-deferred-round-pays-full-list]` A Deferred GC round still pays the full ref-prefix listing — measured at 23% of server CPU under the parallel stateless lane {#gc-deferred-round-pays-full-list}

Measured live (2026-08-04, `system.trace_log` type=CPU, 10-minute window, evidence in the run's
`build/cpu_trace_diagnosis.md`): `CasGcScheduler::loop` appeared in 479/2097 (22.8%) of all sampled
CPU stacks and 70% of background-thread CPU. The single chain
`runRegularRound → enumerateRefPrefix → Backend::list → LocalObjectStorage::listObjects`
(`readdir`/`lstat`) was 11.7% — larger than any individual test query. Four test disks each ran a GC
round at ~1 Hz, and ~89% of those rounds finished `Deferred`: the full directory walk was paid every
second with no payoff.

Two independent contributors, each with its own fix:

1. **The listing is eager even when the round will defer.** The defer decision (fold threshold /
   nothing changed) is made AFTER enumerating. A cheap staleness probe before the walk — or feeding
   the defer decision from the previous round's cursor instead of a fresh enumeration — would make a
   quiet pool cost near nothing per round. This is the durable fix and applies to production pools,
   not just tests.
2. **The disks belonged to finished tests.** This is the known disk-lifecycle leak (custom disks are
   never torn down on `DROP TABLE`), here given a price for the first time: leaked 1 Hz schedulers
   from completed tests kept scanning for the rest of the run. The lifecycle redesign
   (`UNMOUNT` stops background work and ejects the disk) subsumes this half.

### TASK `[gc-condemn-head-read-ahead-pinned-window]` Free the condemn-time HEAD read-ahead window when the merge passes hinted keys, so mass-removal rounds stop paying inline HEADs {#gc-condemn-head-read-ahead-pinned-window}

**Measured 2026-09-04, real-AWS smoke soak (`ca_live_20260904_aws_r1`, binary 9bf134686af, `cas_gc_read_concurrency` 16, window 64):**

| round | condemned | inline `HEAD` | `CASGCReadAheadHit` | `Miss` | `Wasted` | `fold_reduce` |
|---|---|---|---|---|---|---|
| 20:54 | 1261 | 860 | 2453 | 862 | 64 | 128 s |
| 20:56 | 882 | 771 | 583 | 774 | 64 | 116 s |
| 21:09 | 1793 | 882 | 3146 | 4 | 19 | 42 s |

Misses equal the inline HEAD count and `Wasted` is exactly the window: the 64 slots hold hints the
merge never takes, `topUpHeadHints` (`Gc/CasGc.cpp:1801`, `while (pending() < window())`) hints nothing
more, and every `takeHead` at `:1828` degrades to a serial HEAD at ~150 ms on AWS. The third round shows
the same code with a free window: 1793 condemns in 42 s. Same signature as the GCS runs recorded under
`[gc-manifests-are-immutable-so-reduce-and-deletes-can-be-cheap]` (`Wasted=64` per round,
`epoch_crossings=0`), so this is the mechanism that pins the window without an epoch crossing.

**Task.** `head_candidates[shard]` is a superset of the keys the merge will actually take, in the
merge's own ascending key order (comment at `Gc/CasGc.cpp:1790`). A hinted key the merge has already
passed can never be taken. Rule at the hinting site: before topping up, and on any `takeHead` miss with
`pending() == window()`, `discardHead` every pending hint whose key sorts before the key being taken
(the read-ahead counts them as wasted, which is the honest figure). Then top up. Add a gtest that
builds a candidate superset with gaps and asserts hits/misses/wasted per round against the sequential
oracle, plus the existing `CAS*` gate. Acceptance: on a mass-removal round `Miss` is within one window
of zero and `fold_reduce` scales with the read-ahead, not with the inline HEAD count. Expected on the
AWS figures above: 128 s → ~40 s.

### GC per-disk thread pools → one server-wide pool (2026-09-07) {#gc-per-disk-thread-pools}

`Cas::Gc` owns `read_pool` (`Gc.h:982`, concurrency 16, `max_free` 16) and `GcMetaWriter`'s pool
(`gc_meta_pool_size` 16) for the DISK's lifetime although both are used only inside a round: ~17-32 idle
threads per CAS disk, measured 523 threads at 30 inline disks. A design-only spec exists
(`add795aba9bd`) but no implementation. Do it like `CasBlobUploadPool`: one pool initialised once from
server settings, per-disk settings bound per-round in-flight work; or create the pools per round.
Interim mitigation applied: `SYSTEM CAS FORGET` in every stateless test creating an inline disk.

## GC observability {#gc-observability}

### Issue #2211: `SYSTEM CAS GC RUN` on a follower silently does nothing (adjudicated 2026-08-21) {#issue-2211-gc-run-follower-noop}

https://github.com/Altinity/ClickHouse/issues/2211 — CONFIRMED as described; the report's code anchors
all check out on HEAD. History splits it in two:

1. **No-steal on manual runs is DELIBERATE** — commit `74d67b85021` (2026-07-13, "manual GC rounds
   never steal a lease"): the observation-window steal protocol's safety argument needs the two
   "incumbent frozen" observations spaced by real wall time (the loop's paced ticks); two manual
   calls can land microseconds apart and fake a frozen incumbent → two concurrent destructive GC
   actors. The pre-fix manual path also never heartbeat-protected an acquired lease. Keep as is;
   the issue itself agrees steal is the wrong fix.
2. **The silent success row was NOT a chosen contract** — commit `cb111510c1a` (2026-07-20) merely
   surfaced the already-computed `RoundReport` as a result set ("mirroring the DROP POOL MEMBER
   precedent", deferred-register item 11). No record anywhere (commits, specs, backlogs) weighs
   throw-vs-row for the follower case; the docs (`operations/debugging.md` `{#sql-gc-run}`) don't
   mention the follower no-op either. Genuine operator-contract gap.

Also confirmed: `GcLease` is `{owner: UInt128 random gc_id, seq}` — no host identity
(`CasGcStateFormat.h:17`), and `system.cas_mounts.is_leader` is populated only for the local mount,
so a follower cannot name the leader today.

Fix shape (DECIDED 2026-08-21, user call): keep the quiet idempotent OK — no exception. Rationale:
with default `distributed_ddl_output_mode=throw`, a throwing follower inverts the bug for
`ON CLUSTER` (leader ran the round, N-1 followers threw, the statement reports failure), and a node
inside the DDL fan-out cannot tell it is part of `ON CLUSTER`, so selective throwing is impossible.
A follower's "not my lease" is a valid outcome of "run a round here if this node may", and quiet OK
keeps scripts/harnesses that poke `RUN` on every node working. Instead, make the outcome
first-class and visible:
- add a `finish` column (`Success`/`NotALeader`/`Deferred` — already exists in `cas_gc_log`, the
  interpreter row just doesn't emit it) to the `RUN` result set, so the operator reads a word, not
  infers from `acquired_lease=0` + zeros;
- add advisory identity to `GcLease`, mirroring the existing `MountLease` precedent
  (`CasServerRootFormats.h` carries `hostname`/`pid`/`server_uuid` next to its protocol fields for
  exactly this purpose): `hostname` (+ `server_uuid`/`pid` for symmetry), written at acquire/steal.
  The protocol part stays untouched — `owner` MUST remain a random per-process-instance UInt128
  (a restarted server is a NEW GC actor and must not resume the old lease; hostname is neither
  unique nor per-instance), which is WHY host identity was never the owner: the advisory field was
  simply never needed until #2211 (YAGNI, not a considered rejection — no record deciding against
  it). Durable-format change, pre-release so no compat scaffolding
  ([[feedback_ca_no_compat_scaffolding_predev]]);
- follower `RUN` row then carries `leader_host` — `NotALeader, leader_host='replica-2'` in one read,
  no operator discovery query. Rejected as contract (user call): documenting a
  `clusterAllReplicas(system.cas_mounts) WHERE is_leader=1` discovery recipe as the way to find the
  leader — too strange a requirement once the row can name the holder;
- bonus: `system.cas_mounts.is_leader` can be populated for ALL rows (match `gc/state`
  hostname/server_uuid against mount slots), not local-only;
- docs `{#sql-gc-run}`: state the leadership model in one sentence.
No-steal on manual `RUN` stays untouched.

- **[GC round progress observability]** round-duration watchdog + fold-window events — HARD — A wedged round is only visible after the fact. Spec [C5](/superpowers/specs/cas-gc-rounds-in-minutes-design#c5-observability) adds `deadline_hit`/`carry_total`; audit `#f28` recommends a one-line per-round summary. Neither adds the watchdog alert or the fold-begin/end imbalance check.
- **[GC-FULL-TIME-ACCOUNTING]** {#round-duration-alarm} every millisecond of a round must be attributed — Timer coverage 99.986% complete. Remaining: name the `orphan_sweep` epilogue phase, add an `unaccounted_ms` column, a periodic progress log line — not delivered by spec C5 or audit `#f28`.
- **[fsck oracle gaps]** — MINOR — fsck under-reports orphan manifest bodies for ref-less namespaces; should enumerate `cas/manifests/` too.
- **[ProvenanceOp operability gap]** — MINOR — Both committed-ref writes and removal-mark repoints use `ProvenanceOp::Other` (confirmed unchanged on `antalya-26.6`, 2026-09-25) — no distinct op kind for the audit trail. Product-owner call.
- **[frontier-attribution-taxonomy]** classification taxonomy (6 classes) for unproven namespaces — DESIRABLE — No classification exists for why a frontier proof is missing; design only.
- **[refplan-dead-drop-counters]** {#refplan-dead-drop-counters} CAS-096 — KEEP, P3 — `dropped_holds`/`dropped_checkpoints` (`Gc/CasGc.h:215-216,298-299`) have no production producer and are always zero; a lost hold rides `dropped_parent_rows` instead. Owed: delete the dead adapters or give them a producer, add the counters to the REBUILD report row. Also: `CASGCUnmatchedAdoptedParentLives`'s description is stale since `4d40d4533473` (`cas-gc-rebuild` only) removed the warning it describes.
- **[gc-outcome-budget-skews-round-report-counters]** {#gc-outcome-budget-skews-round-report-counters} CAS-101 — KEEP, refinement 1 closed refinement 2 open — Round-report delete counters were tallied from the budget-capped outcome logs; **closed** by spec [C2](/superpowers/specs/cas-gc-rounds-in-minutes-design#c2-budgets-removed) (in-memory tallying). `GcFoldBegin`/`GcFoldEnd` still stamp the previous round number (`Gc/CasGc.cpp:719,756`, confirmed unfixed on both branches, 2026-09-25) — untouched by C2, still open; one-line fix is `new_round` on both events.

### `[gc-phase-rows-lose-worker-requests]` phase rows do not see requests made on worker pools {#gc-phase-rows-lose-worker-requests}

`GcPhaseTimer` diffs the round thread's `ProfileEvents`. Every request a read-ahead worker or a
`meta_pool` job performs lands on that worker's counters instead, so the S3 verb counts on
`fold_ref_intake` and `fold_reduce` now under-count by exactly the hinted requests — the same gap
`meta_pool_wait` has always had. The semantic metrics on those rows are unaffected, and
`CASGCReadAheadHit`/`Miss`/`Wasted` are on the row because they are incremented at the take site. The
fix is attribution at the worker boundary, which is a `GcPhaseTimer` change and not a read-ahead one.

## GC tooling and rebuild {#gc-tooling-and-rebuild}

- **[gc-rebuild follow-ups]** — MINOR — `rebuildBaseline` has no dedicated gc-round-log row; the "unowned-alive manifest edge over-protect" leak is bounded/fsck-visible; needs soak validation (`mc rm gc/state` → guard → recover). Hub item, cross-referenced below.
- **[rebuild-gcstate-decode-reason-unreported]** {#rebuild-gcstate-decode-reason-unreported} CAS-069 — KEEP — `rebuildBaseline`'s health probe swallows the decode exception (`Gc/CasGc.cpp:3874-3883`) before correctly refusing `CORRUPTED_DATA` — safety half sound, but the operator can't tell real damage from an environmental failure. Owed: log the exception, name the reason on the report row. See B8 below for a deeper gap the CAS-069/CAS-095 triages both missed.
- **[rebuild-refusal-leaves-run-and-seal-residue]** {#rebuild-refusal-leaves-run-and-seal-residue} CAS-094 — KEEP, P3 — The rebuild's last refusal (lost CAS, `:4377-4381`) lands AFTER an unconditional flush loop and the seal itself, leaving a complete durable residue — contrary to `ContentAddressedMetadataStorage.h:199-200`'s "writes nothing" claim. Not adoptable, not permanent (pruned in time). Owed: fix the comment, add a refusal audit event, derive attempt numbering from `lease.seq`.
- **[gc-dryrun-silent-on-damaged-state]** {#gc-dryrun-silent-on-damaged-state} CAS-095 — KEEP, P3 — `previewDeletes` prints `preview_deletes=0` both for a healthy pool AND for the missing-adopted-seal disaster case a regular round treats as `CORRUPTED_DATA` (`:3835-3838,4411-4426`). Owed: distinguish the three no-baseline outcomes, report per-shard, align the CLI description. Read-only; never authorizes a delete.
- **[F3]** `ca-gc-dryrun` reachability under-counts vs real GC/fsck — HARD — Systematic across S18/S25/S26/S33. Fix: dryrun uses the same reachability walk as GC/fsck. (Unrelated to the 2026-09-25 audit's own "F3" label — coincidentally shared numbering.)
- **[gc-rebuild-lease-interlock]** `rebuildBaseline` has no mount-lease interlock — HARD — A live server's fresh lease does not stop the offline DR rebuild from performing. Real safety gap in a destructive tool.
- **[REBUILD-SEAL-POINT-READ]** point-read closure for REBUILD seal discovery — HARD — `rebuildBaseline` finds the newest seal by probe-and-step-down, not proof. Fix: a write-once `gc/gen/<G>/sealed` alias for one residual; a pruned-pool false-"virgin" verdict needs a marker that survives pruning, deferred. Reasoning: `.superpowers/sdd/2026-07-28-cas-ref-chain-stage-a-streams/task-8-report.md`.
- **[FSCK-SCALE-TIMEOUT]** {#fsck-scale-timeout} `ca-fsck` cannot complete a large pool within its own deadline — MEASURED — Times out at ~29-31 GiB (`FSCK_EXIT=159`), returns nothing; raising the budget 180→600s did not help. Direction: bounded/streamed partial verdicts with a resumable cursor. A phase-3 soak's fsck-clean gate stays UNARMED at scale.
- **[rebuild-cannot-recover-undecodable-gc-state]** {#rebuild-cannot-recover-undecodable-gc-state} opus review B8 — KEEP, P2 — `rebuildBaseline` classifies an undecodable `gc/state` correctly, then unconditionally re-decodes the same bytes with no `try` inside `acquireOrRenewLease` (`Gc/CasGc.cpp:4732-4760`, confirmed identical and unfixed on `antalya-26.6`, 2026-09-25) — `GC REBUILD FORCE` throws `CORRUPTED_DATA` in exactly the scenario it exists for. Only workaround (external S3 delete) is named nowhere. Fix: tolerate an undecodable state in the rebuild path's lease acquisition.

## `[otel-demo-s3-budget-audit-2026-09-25]` otel.demo CAS S3 budget audit: twelve ranked findings on repeated and unnecessary work {#otel-demo-s3-budget-audit-2026-09-25}

Report: `docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget-audit.md`. Spec that absorbs the GC-side items:
`docs/superpowers/specs/2026-09-25-cas-gc-rounds-in-minutes-design.md` (A0 = F3, A4 = F4, verification F6/F8, open
questions F5/F7/F2). Stand: 2.3M PUT, 6.4M GET, 384k LIST per day on one replica for 43 MB/day of user data; 86% of
parts are `system.*` log tables on the CAS disk (F1, deliberate on the stand, a docs note for production); `delete_tmp` repoints 191k/day (F2,
`[PART-REMOVAL-REPOINT]`, now with a full cost line); carried condemned rows never persist `marker_confirmed` (F3,
one-line GC fix); one manifest body GET per edge with no per-round reuse (F4, 29% of GETs); `_ckpt` PUT per flush (F5,
21% of PUTs, owner decision); ~3 S3 LIST requests per 1000-key page (F6, verify); six GC requests per garbage blob (F7).

Most of F1-F31's findings are already threaded through this file and `BACKLOG/performance.md` and
`BACKLOG/operability-and-introspection.md` via the spec's own A/B/C items and per-finding `#fN` citations; this
entry is the pointer for the ones that are not yet individually cross-referenced anywhere: F6, F8, F10, F17, F19,
F23, F24, F28, F30 (checked 2026-09-26 — none of these nine appear under `docs/superpowers/cas/BACKLOG/`). Read the
report directly for those until each gets its own line.

## Later / design questions {#gc-later-design-questions}

- **[gc-checkpoint-record-tuning]** resident-snapshot incremental GC checkpoint tuning — DESIRABLE — `gc_checkpoint_records`/`gc_checkpoint_rounds`, distinct from the ref lane's own `_ckpt` object (audit `#f5`).
- **[gc-handoff-single-crash-leak]** single-crash leak window in the GC round hand-off — MINOR — Bounded, distinct from `[SUPPRESSED-HANDOFF-CONSUMPTION]` (suppression-driven vs a single-crash window).
- **[retired-refs-map-staleness]** `retired_refs` map staleness after retired-in-snapshot — MINOR — Needs live-field verification before closing.
- **[r11c-incarnation-mismatch-detector]** R11c incarnation-mismatch detector design — DESIRABLE — Distinct from the settled R11 vacuous-universe finding; design only.

## Former section anchors kept for existing citations {#gc-legacy-anchors}

Three sections were regrouped by topic on 2026-09-25. Kept empty here because `2031-triage.md` cites
them directly by anchor.

### GC scalability & byte cost {#gc-scalability}

Regrouped into [GC cost and throughput](#gc-cost-and-throughput).

### GC correctness / observability follow-ups {#gc-followups}

Regrouped into [GC correctness and safety](#gc-correctness-and-safety), [GC observability](#gc-observability), [GC tooling and rebuild](#gc-tooling-and-rebuild).

### New findings from the 2026-08-04 orphaned-open triage {#orphan-triage-2026-08-04}

Regrouped into all topic headings above. One item, `[gc-probe-a-counters-durability]`, was removed outright: probe A is fully deleted (`5b775616c36`, `cas-gc-rebuild` only; zero `ProbeA`/`CASGCProbeA` symbols on either branch).
