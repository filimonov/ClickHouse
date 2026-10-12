# Campaign: CAS mount lease series (spec → reviews → per-PR plans → SDD) {#campaign-cas-mount-lease-series-spec-reviews-per-pr-plans-sd}

Owner's order (2026-10-07): write a spec from the corrected design (chat), have independent reviewers
(codex gpt-6.1-sol xhigh, Fable subagent) compare it with the older documents and judge the best
design, order, cleanliness and PR split; iterate 5–15 codex rounds, each round must simplify or
clarify; fold every backlog item about leases and timeouts where possible; model protocol changes
in TLA+ (TLC via run_*.sh only); then one small plan per PR (TDD, SDD), one worktree (cas-95-1),
a series of branches off `altinity/antalya-26.6`, each branch = one PR = one self-contained
improvement. Unattended. UX and explanations understandable "даже для идиотов": no hidden logic,
no formulas behind settings. No huge PRs.

Standing rules: never push without an explicit order (the 2026-10-02 push was one-off); commit
exact files only; no rebase/amend; no commits on master; trailer `Co-Authored-By: Claude Fable 5.1
<noreply@anthropic.com>` only; specs/plans/backlog/models on `cas-gc-rebuild` (this worktree); code
in `/home/mfilimonov/workspace/ClickHouse/cas-95-1`; ninja without -j, logs in build dir, subagent
reads logs; gtest filter exactly `CAS*`; LOGICAL_ERROR tests are death tests; shared code
(base/poco, src/IO/S3) edits need the owner's decision — CAS-343 is such a change: design it,
flag it, implement only if the owner's prior framing allows (he asked to solve timeout problems
"по возможности"); cheap models for mechanical work.

## Inputs {#inputs}
- Spec (owner, 2026-10-05, has uncommitted edits — do NOT commit it): docs/superpowers/specs/2026-10-05-cas-mount-lease-simplification.md
- Plan v3 (owner, untracked — do NOT commit it): docs/superpowers/plans/2026-10-07-cas-mount-lease-plan-v3.md
- Older: specs/2026-10-01-cas-mount-renewal-without-lease-deadline-design.md, specs/2026-10-02-cas-mount-lease-settings-ux-notes.md, cas/2026-10-02-mount-lease-settings-external-study.md, cas/2026-10-02-mount-lease-settings-studies.md, plans/2026-10-02-*-part1.md, plans/2026-10-02-*-part2*.md, specs/2026-09-01-cas-self-authored-mount-reclaim-design.md
- Draft PRs #2474 / #2475 (reference material; owner: too big to review, redo in small PRs)
- Corrected design (chat 2026-10-07): pause≠loss; own gc_fenced → reacquire through the startup path in the lease thread; live foreign owner / slot absent / unexpected body under own (uuid,epoch) / interference → STOP THE SERVER (fail-close); local defect → keeper_failed, mount closed, server continues; state computed from the clock; two settings (lease, request deadline), lease carried in the body and honoured by observers with a cap; derived values printed; small PRs.

## Phases {#phases}
- [x] P0 context: backlog digest tmp/lease-series/backlog-digest.md (done), code facts checked on antalya-26.6 @5b7c9f503df
- [x] P1 spec r1 written (440 lines, uncommitted): docs/superpowers/specs/2026-10-07-cas-mount-lease-series-design.md
- [x] P2 review loop DONE: 6 rounds codex (r1–r6) + Fable r1–r4 / Opus r5–r6; both CONVERGED on r6; r7 = minors applied, committed; codex r7 = confirmation round (running)
- [x] P3 TLA+: all models updated before their PRs (24 commits)
- [x] P4 per-PR plans: 20 plans committed
- [x] P5 SDD: all 16 code PRs COMPLETE (2026-10-09 11:00)
- [x] P6 final report: tmp/lease-series/FINAL-REPORT.md (2026-10-09)

## State dir {#state-dir}
tmp/lease-series/: this file, backlog-digest.md, review-rN-codex.md, review-rN-fable.md, prompts/, logs/

## Log {#log}
- 23:50 cas-95-1 detached at altinity/antalya-26.6 (5b7c9f503df). Codex r1 launched (tmp/lease-series/run_codex.sh 1 → review-r1-codex.md, status CODEX_STATUS, log logs/codex-r1.log; retries on capacity). Fable reviewer agent lease-review-r1-fable → review-r1-fable.md. Watchdog cron 770beb71. NEXT: read both reviews → spec r2 → codex r2 + fable r2; after r2 start TLA+ modelling agent for the three models if the body-lease rule survives.
- 00:08 r1 reviews in: fable (B1, 10 MAJOR, 17 MINOR; "best of four, not ready"), codex (2 BLOCKER, 14 MAJOR; "best base, not ready"). Controller decisions for r2: drop observer-reads-lease-from-body (WARNING on mismatch only); reserve unchanged until audit (PR 14); every loss except own gc_fenced/FORGET stops the server incl. local defect; raise(SIGTERM) via BaseDaemon::terminate; keep cas_unsafe_remount_no_delay (startup setting); Conflict{NotObserved} continues; PR 3+4 merged, PR 7 split 7a/7b, PRs added for CAS-50, CAS-56.3, CAS-179, startup primitives; 15 PRs total.
- 00:17 spec r2 written (r1 saved as tmp/lease-series/spec-r1.md). Codex r2 + fable r2 (lease-review-r2-fable) launched with both r1 reviews attached. NEXT: read r2 reviews → r3; after r3 start TLA+ modelling agent (CaDiskLifecycle one-thread/Reacquiring/Stopping; CaCasMountCore certificate; CaMountRenewRetryCore Refused/NotObserved) — the body-lease observer change is dropped so CaMountObserveCore needs no protocol change.
- 00:24 owner clarified: temporary S3 loss with the slot intact (even gc_fenced) must CONTINUE; only a stolen/removed/overwritten slot stops the server. Spec r2 already behaves so; the 'one rule' sentence reworded.
- 00:40 codex r2 in: 1 BLOCKER (certificate authorship), 11 MAJOR; fable r2: 0 BLOCKER, 4 MAJOR. Both: r1 items fixed, best design, not ready.
- 01:05 spec r3 written (r2 saved as tmp/lease-series/spec-r2.md): authorship certificate, Reacquire mode, claim monotonicity PR, observation contract + strict absence, Conflict{NotObserved}→read only, terminate() on observing thread, hard+soft validation, period/margin settings kept, two pause thresholds in texts, system table without I/O, Faulted exit (ledger_corrupt), deadline 16a/16b as network part, settings 17a/17b, 19 PRs. Codex r3 + fable r3 launched. NEXT: read r3 → r4; if r3 converges on one side, run r4 anyway (owner wants ≥5 codex rounds, each simplifying); start TLA+ agent for CaMountRenewRetryCore (PR 7 prerequisite) after r3 if R1–R7 stable.
- 00:47 TLA agent lease-tla-renewretry started (CaMountRenewRetryCore, PR 7 prerequisite; runs via run_mountrenewretry.sh, nice 10; commits exact files).
- 01:10 codex r3: 7 MAJOR (settings migration, CAS-184 branch, owner/counter on reacquire, intermediate PR leaves old remount wide, models order, system table I/O, Global memory guard); fable r3: 3 MAJOR (reads at pause, PR 6 scope/test, R15 branch). 
- 01:35 spec r4 written (r3 saved as spec-r3.md). Series = 18 PRs (+17a/b). Codex r4 + fable r4 launched. TLA renew-retry agent still running. NEXT: r4 reviews → r5 (≥5 codex rounds required); fold TLA results; then start models for PR 7 (CaCasMountCore incarnations) and PR 12 (CaDiskLifecycle Stopping).
- 01:47 codex r4 DONE: 5 MAJOR, 0 BLOCKER. fable r4: 0 BLOCKER/MAJOR, "ready for planning", minors N1–N9 — BUT the Fable agent hit the account's Fable limit mid-run (review file was written before the failure). SECOND REVIEWER FROM r5 ON: opus (ca-arch, model opus).
- 00:26 TLA renew-retry DONE: commit eb2e40e5eab, 30/30 rows as expected (17 sabotages, 11 witnesses, safe 95.1M states, 87 min). Notes folded into spec r5.
- 04:40 (session was idle ~2.5 h: queued watchdog prompts) spec r5 written (r4 saved as spec-r4.md). Codex r5 launched (5th round, the owner's minimum). Opus r5 launched. NEXT: read r5 → if both converge → P3 models for PR 7 (CaCasMountCore incarnations) and PR 12 (CaDiskLifecycle Stopping) → P4 plans; else r6.
- 04:42 TLA agent lease-tla-mountcore-1 started (CaCasMountCore step 1: incarnations + claim monotonicity, before PR 7).
- 04:35 codex r5 DONE: CONVERGED yes (minors N1–N7). opus r5: 2 MAJOR (floor protects old-epoch prefixes under fenced body; stop must not apply to the decommission pool), 13 minors.
- 05:05 spec r6 written (r5 saved). RESULTS PR-number fix committed 5f15430aeed. Codex r6 + opus r6 launched. NEXT: if both converge → P3 (CaDiskLifecycle step 1 model for PR 12; mountcore step 1 running) → P4 per-PR plans starting with PR 1–8; else r7.
- 05:07 TLA agent lease-tla-lifecycle-1 started (CaDiskLifecycle step 1: Stopping/Closed, latch, one stop; before PR 12).
- 05:40 codex r6 + opus r6 both CONVERGED: yes (minors only). Spec r7 = all minors + lifecycle model findings; COMMITTED as spec revision 7 (see git log). Codex r7 confirmation launched. ENTERING P4: per-PR plans. Order: PR 1, 2, 3, 4, 5, 6 (independent), 7 (after mountcore step 1), 8, 9a, 9b, 10, 11a, 11b, 12, 13, 14, 15, 16, 17a, 17b, 18. Plans go to docs/superpowers/plans/2026-10-08-cas-lease-pr<NN>-<slug>.md. SDD in cas-95-1: branch per PR off altinity/antalya-26.6 (independent PRs) or off the prerequisite branch (dependent ones; the PR is opened only after the prerequisite merges). Gates per head: CAS* release + ASan; integration test_cas_mount_renewal_retry where touched.
- 05:45 P4 started: plan agents plan-pr01..plan-pr06 (opus, ca-surgeon) writing docs/superpowers/plans/2026-10-08-cas-lease-pr0N-*.md from tmp/lease-series/plans/PLAN-BRIEF.md. Running in parallel: codex r7 (confirmation), lease-tla-mountcore-1. NEXT: review each plan (quick read + flag spec/code disagreements), commit exact files; then SDD for PR 1 in cas-95-1 (branch lease/antalya-26.6/pr01-gc-sighting-after-read off altinity/antalya-26.6); plans for 7–10 after.
- 05:34 codex r7 (confirmation): CONVERGED yes, 4 MINOR (to fold into r8 later with implementation-time corrections; not blocking plans). Plans 1–6 written (PR 4 trimmed to 3 columns); plan-review-1-6 (sonnet) running; mountcore step-1 model still running (LateRenew rows). NEXT: apply plan review → commit plans → SDD PR 1.
- 05:45 plans 1,3,5,6 committed (8ca26b59ae9); plan review: PR 2 MAJOR (snippets against base signature; plan-pr02 rewriting against PR 1's), PR 4 under review. P5 SDD started: impl-pr01 (opus) implements the whole PR 1 in cas-95-1 (branch fix/antalya-26.6/cas-gc-sighting-after-read); ledger .superpowers/sdd/2026-10-08-cas-lease-pr01-gc-sighting-after-read/progress.md. NEXT: when impl-pr01 reports → review package (scripts/review-package PLAN BASE HEAD) → ca-review (opus) → fix loop → done; then PR 5 (smallest) and PR 3, PR 6 one at a time in the same worktree (each on its own branch off the base; checkout switches cost rebuilds — sequential).
- 05:44 plans 2 and 4 committed; all six plans for PR 1–6 ready (minors in tmp/lease-series/plans/review-plans-1-6.md go to each implementer).
- 05:46 plan agents plan-pr07, plan-pr08, plan-pr10 (opus) started; 9a/9b after pr08's plan (interfaces). impl-pr01 running.
- 06:47 PR 1 implemented (head b84322319cd, gates green); review-pr01 running. Next in the worktree after review: PR 5 (branch off base).
- 06:54 PR 1 COMPLETE (head bbaeb43d5db). impl-pr05 (sonnet) started on its own branch off the base.
- 06:58 mountcore step 1 DONE 49d9c01242f (29/29; sab_claimregress RED as expected); equal-epoch sabotage row requested as a follow-up.
- 07:10 plans 9a/9b committed. Plans done: 1–8, 9a, 9b, 10. Remaining plans: 11a, 11b, 12, 13, 14, 15, 16, 17a, 17b, 18 — write after PR 8 lands locally (their interfaces depend on 8/10/11).
- 07:13 PR 5 implemented (66583a32b03, c3dd843f967; release 2611, ASan 2615/0) → review-pr05 running. impl-pr03 (opus) started on its own branch off base.
- 07:13 PR 5 COMPLETE (head c3dd843f967).
- 07:37 PR 3 implemented (head e9b4cefb415; release 2613, ASan 2617/0, integration 4) → review-pr03 running. impl-pr06 (opus) started; live GCS/AWS acceptance authorised under prefixes ca_lease_pr06_<date>_<store> (to delete later — owner).
- 07:44 review-pr03: ready to merge; minors T1/P1/P2 queued as a fix round after PR 6 (worktree busy). PENDING QUEUE: pr03-minors.
- 08:17 PR 6 implemented (head c477577ba9b; release 2616, ASan 2620/0; 4 stores OK; live-GCS stale-assertion failure pre-existing → CAS-348 created 2c2d594b1ca) → review-pr06 running. impl-pr03 resumed for the minors fix round (worktree free). External prefixes to delete later: s3://…/ca_lease_pr06_20261008_aws, gs://…/ca_lease_pr06_20261008_gcs/, gs://…/ca_lease_pr06_20261008_gcsbase/.
- 08:21 review-pr06: needs changes (T1 + P1–P5); fix round queued after pr03-minors. QUEUE: pr03-minors (running) → pr06-fix → PR 7.
- 08:31 OWNER: decision on 17b DEFERRED — do not do 17b (and 18) now; other decisions also deferred (interrupted question list). Series queue excludes 17b/18; 17a last and optional.
- 08:32 PR 3 COMPLETE (head 1d4f516c506). Next: pr06 fix round.
- 08:47 PR 6 fix round 71ec8123021 (release 2617, ASan 2621/0) → rereview-pr06 running. impl-pr07 (opus) started on its own branch off base.
- 08:47 PR 6 COMPLETE (head 71ec8123021). Done locally: PR 1, 3, 5, 6. Running: impl-pr07; mountcore battery.
- 09:20 PR 7 implemented (head 33217601706; release 2615, ASan 2619/0) → review-pr07 running. impl-pr04 (opus) starting.
- 09:26 review-pr07: ready to merge; minors queued (pr07-minors) after PR 4. QUEUE: impl-pr04 (running) → pr07-minors → PR 8.
- 09:35 mountcore equal-epoch sabotage committed 3dbf425edd5 (30/30; sab_refreshownpair RED). Model gate for PR 7 complete (both sabotages). P3 remaining: reflane (PR 10, in its plan), mountcore 2a/2b (11a/11b), lifecycle step 2 + mountcore step 3 (14).
- 09:52 PR 4 implemented (head d8e3d54e234; release 2612, ASan 2616/0, integration 5; +113 accepted) → review-pr04 running; impl-pr07 resumed for the minors fix round.
- 09:57 review-pr04: ready; minors queued (pr04-minors: T1, T2, P1). QUEUE: pr07-minors (running) → pr04-minors → PR 8.
- 10:09 PR 7 COMPLETE (head f038006c8e4). Next: pr04-minors.
- 10:26 PR 4 COMPLETE (bc6b58f7c04). Local integration base lease/antalya-26.6/base-pr1-4-6 @ 0bd3e3c11d8 created (merges of 1,4,6; clean). impl-pr08 (opus) starting on top of it.
- 10:33 PR 1 squashed to bc7f453aa4a (owner's request); PR body in cas-95-1/tmp/pr-body-lease-pr01.md.
- 11:22 PR 8 implemented (head 67887f22370; production +306/−77 in 15 files; release 2644, ASan 2648/0; integration red-on-base 1/5 → 6 passed) → review-pr08 (opus) running. CAS-256 note committed 221cb783417. NEXT after review: PR 2 (needs PR 1 — branch off bc7f453aa4a), PR 10 (independent), then 9a (needs 8).
- 11:57 PR 8 fix round fe252af1d03 + 3e527b61615 (release 2648, ASan 2652/0, integration 6) → rereview-pr08 running. impl-pr10 (opus) starting (independent; model step in master worktree).
- 11:59 PR 8 COMPLETE (head 3e527b61615). Done locally: 1 (squashed bc7f453aa4a), 3, 4, 5, 6, 7, 8. Running: impl-pr10. QUEUE: pr08-nits (2 nits) → PR 2 (off bc7f453aa4a) → 9a (off PR 8 head) → 9b.
- 12:35 PR 10 implemented (head c9d0d4df672; model 62d81168e23 35/35; release 2619, ASan 2623/0) → review-pr10 running; impl-pr08 resumed for nits N1/N2.
- 12:43 review-pr10: needs changes (C1–C5 + prose); queued after pr08-nits. QUEUE: pr08-nits (running) → pr10-fix → PR 2 → 9a → 9b.
- 12:52 PR 8 nits done → final head 2c61e5ee301. impl-pr10 resumed for its fix round.
- 13:05 OWNER: new base branch antalya-26.6-20261008 (= fresh antalya-26.6 @ 0f5845abbb7, feature freeze today) created and PUSHED to altinity (authorized). Series PRs target antalya-26.6-20261008. PR 1 branch pushed; PR opened: https://github.com/Altinity/ClickHouse/pull/2509 (not draft). Each further PR: push + open only when the owner says (one-off authorization per PR unless stated otherwise). Branches of 3,4,5,6,7,8,10 are built on 5b7c9f503df (ancestor of the new base): fine, PRs merge cleanly as long as git merges; re-check merge-tree before each push.
- 13:15 PR 10 COMPLETE (head b22e5f42bf9). Done locally: 1,3,4,5,6,7,8,10. NEXT: PR 2 (branch off bc7f453aa4a = PR 1 squash; target base after PR 1 merges), then 9a (off PR 8 head 2c61e5ee301), 9b.
- 13:29 PR 5 squashed (58debc8b370), branch renamed fix/antalya-26.6/cas-probe-sentinel-boundary, pushed, PR opened https://github.com/Altinity/ClickHouse/pull/2510 → antalya-26.6-20261008. Open PRs: #2509 (PR 1), #2510 (PR 5).
- 13:40 PR 3 squashed (949578b2d73), branch renamed fix/antalya-26.6/cas-renewal-memory-guard, pushed, PR opened https://github.com/Altinity/ClickHouse/pull/2511. Open PRs: #2509 (1), #2510 (5), #2511 (3).
- 13:45 PR 2 implemented (head 03330f1b96d) → review-pr02 running. impl-pr09a (opus) starting off PR 8 head 2c61e5ee301.
- 13:47 PR 2 COMPLETE (03330f1b96d, on PR 1). Done locally: 1,2,3,4,5,6,7,8,10. Open: #2509 (1), #2510 (5), #2511 (3). Running: impl-pr09a.
- 13:53 watchdog: impl-pr09a alive (T2 committed 14e3bdd2d84 13:49, ninja running). Open: #2509/#2510/#2511. Owner raised PR 6 format-sensitivity concern; recommended option 2 (upstream 404=absent + LIST corroboration before terminal stop); awaiting decision.
- OWNER DECISION (2026-10-08 ~14:00): PR 6 (absence per verb) REJECTED — too format-sensitive (unnamed 404 / `NotFound` stores would never mount; upstream accepts both). Replace with LIST corroboration at the terminal decisions in PR 12: ProvenAbsent = GET 404 (named or not) AND key missing from a LIST of its prefix; LIST failure/404 = NotObserved → no stop. Port PR 6's wire-level HTTP harness (local server in front of the real S3 client) into PR 12's tests; drop the classifier, the bucket-requirements doc and the verb pins. Branch cas-lease-pr06-absence-per-verb kept locally only (not pushed) until the harness is ported.
  Consequences: (a) spec r8: remove PR 6 from section 7, redefine ProvenAbsent in the observation contract, PR 12 row gains the LIST step; (b) PR 12 plan: LIST corroboration before slot_absent / pool_identity_lost / reacquire anchor-absent + two harness tests (proxy 404 everywhere → NotObserved; GET 404 + LIST without key → stop); (c) model CaCasMountCore: absence observation two-step; (d) REBASE: PR 8 base `lease/antalya-26.6/base-pr1-4-6` contains PR 6 → rebuild base = PR 1 squash bc7f453aa4a + PR 4, cherry-pick PR 8's 4 commits and 9a's commits (merge-tree, no checkout), then re-run release+ASan CAS* gates and test_cas_mount_renewal_retry on the exact new trees. Do (d) AFTER impl-pr09a finishes (worktree in use).
- 13:59 OWNER: HOLD the PR 8/9a base rebuild (consequence d) until the earlier PRs are reviewed/squashed with the owner. Do not start it unattended.
- 14:02 PR 7 squashed (88953e60433), branch fix/antalya-26.6/cas-mount-claim-monotonicity, PR https://github.com/Altinity/ClickHouse/pull/2512. Open: #2509(1) #2510(5) #2511(3) #2512(7).
- 14:04 OWNER: add Signed-off-by to every commit. PR 7 rewritten 88953e60433→17c7f28b660 and force-pushed. All future squashes and implementer commits carry Signed-off-by + Co-Authored-By.
- 14:10 OWNER: do NOT rewrite #2509/#2510/#2511 for Signed-off-by (saves a CI run); add it only if those branches need another change anyway.
- 14:13 watchdog: impl-pr09a at T5 (docs committed 14:11, 615076ac0ca); release gate done 14:10, ASan gate running. Open PRs: #2509 #2510 #2511 #2512. PR 4 explained to owner; awaiting squash/open order. Disk 122G, mem 65G.
- 14:18 PR 4 squashed (0a8317178b0, Signed-off-by), branch fix/antalya-26.6/cas-mount-renewal-progress, PR https://github.com/Altinity/ClickHouse/pull/2513. Open: #2509(1) #2510(5) #2511(3) #2512(7) #2513(4). Remaining ready-but-dependent: 2 (on 1), 8 (on 1+4, base includes rejected 6 → rebuild on hold), 10 (on 1).
- 14:28 9a review: needs changes (2 minor tests, 2 nits, 7 prose) → fix round 1 running (impl-pr09a).
- 14:33 watchdog: impl-pr09a fix round 1 in progress (T2 red run 14:32, ninja clickhouse+unit_tests 14:33; docs + code edited, not yet committed). Disk 122G, mem 61G.
- OWNER DECISION (2026-10-08 ~15:00): PR 2 REWORK before opening. Observers (GC floor, same-srid restart reclaim, DROP POOL MEMBER — all via `mountObservationThresholdMs`) wait `max(own lease, declared lease) + 5% + own step`, declared = `expires_at_ms − started_at_ms` when `expires > started`, else 0 (zero stamps, farewell, bad dates → own value = today's behaviour). Never shorter than today; mixed versions safe in any order; the pool-wide "one value, pool stopped" rule disappears (setting becomes local: "how long this server may write after its last confirmed renewal; others wait at least this long"). Keep the once-per-slot mismatch WARNING with adjusted text ("nodes wait for the declared lease; a longer value delays recovery of this node's crashes"). Fast paths (gc_fenced / farewell → no wait) already in base, untouched. Old PR 2 branch head 03330f1b96d kept for harvesting. Plan: plan-pr02v2 (opus) now; impl after impl-pr09a's fix round frees the worktree. Model CaMountObserveCore: per-holder lease parameter (master worktree, after the plan).
- 14:51 9a fix round 1 done (07458806831) → rereview-pr09a running. Soak cards s39/s43 committed in master 3f5ad504ea3. Worktree free; impl-pr02v2 waits for the plan (plan-pr02v2 running). PR 10 explained; awaiting squash/open order.
- 14:52 PR 9a COMPLETE (07458806831, on PR 8). Next: 9b (plan exists) after PR 2 v2 impl; both need the worktree — PR 2 v2 first (owner priority).
- 14:53 watchdog: plan-pr02v2 running (started ~14:40, plan file not yet written — within budget). Nothing else running; worktree free. Disk 122G, mem 66G.
- 15:02 plan PR 2 v2 written+committed (6 tasks). impl-pr02v2 (sonnet, ca-impl) dispatched off bc7f453aa4a.
- OWNER DECISION (2026-10-08 ~15:15): PR 10 REWORK before opening. Occupied ref-log id: byte-equal own bytes (retry tail) → continue; EVERYTHING else — foreign transaction, successor's seal (a zombie: we must already be gone), undecodable object, impossible lane state — is ONE untyped `fault`, latched first in one `reportLatchedFault` (today → remount; PR 12 → stop); raw facts (key, decoded header / "does not decode" / exception text) only in the message. No `interference|corrupt|defect` enum. Ref-lane commit path (append + occupant read/decode) under `LockMemoryExceptionInThread(Global)` like PR 3 — the memory-limit "retry later" arm disappears; bad_alloc stays terminal. Successor-side recovery walk keeps distinguishing predecessor's records vs foreign (needed to seal). Model CaRefLaneCore: FaultHasExit stays; unreadable/defect/classify-retry witnesses collapse. PR 12: one row "ref-lane fault → stop" replaces R15–R17. Old branch head b22e5f42bf9 kept for harvesting. Plan: plan-pr10v2 (opus) now; impl after impl-pr02v2 frees the worktree.
- 15:13 watchdog: impl-pr02v2 T1 committed d7fbf52f033 15:09, T2 build 15:11, gate running 15:13. plan-pr10v2 running since ~15:17 (no file yet). Disk 122G, mem 66G.
- 15:32 Second worktree for gates: cas-janitor-budget (base 5b7c9f503df, release+ASan builds 10-07, clean). gate-pr08 (sonnet) checks out lease/antalya-26.6/pr08-on-pr1-4 (be0c4e4399c) there: release+ASan CAS*, test_cas_mount_renewal_retry. New base a4c9a64a87a = merge(PR1 bc7f453aa4a, PR4 0a8317178b0).
- 15:33 watchdog: impl-pr02v2 at T5 (4 commits to 49250f04c67, gate 15:33); gate-pr08 building release in janitor (141/711, 15:33); plan-pr10v2 file being written (76 KB, 15:33). Disk 122G, mem 56G.
- 15:34 plan PR 10 v2 written+committed (7 tasks, 15 tests, model collapse). impl waits for the worktree.
- 15:35 OWNER: remove in PR 8 the code PR 8 itself made dead, only where directly related (same context); weakly related dead code stays for PR 15/16. audit-pr08-dead (opus, read-only) classifying A/B/C → then a removal task on lease/antalya-26.6/pr08-on-pr1-4 in cas-janitor-budget after gate-pr08, re-gate.
- 15:41 audit-pr08-dead: no strictly dead code (give-up outcomes still produced by startup/remount re-anchors → PR 15). Fix round owed on pr08-on-pr1-4: A2 defect (false 'recovered before deadline' event reason after expiry, + test), A1 merge identical branches, A3 metrics_and_events.md rows, A4 renewOn comment, test comments citing DeadlineExceeded==0 as proof. OWED in master: s39 leg B → pause behaviour (needs a stand run). Dispatch after gate-pr08.
- 15:53 watchdog: impl-pr02v2 all 6 tasks committed (4bf103ba0c4), mutation runs in progress; gate-pr08 release PASSED 2641, ASan gate running. Disk 122G, mem 63G.
- 16:13 watchdog: impl-pr02v2 mutation 5 of 6 (16:12); impl-pr08-fix commits done (…989c861c71c), release gate done, ASan gate running. Disk 122G, mem 65G.
- 16:19 PR 8 fix A1–A4 done (head 989c861c71c; release 2642, ASan 2646/0, integration 6/6) → rereview-pr08-fix (lite). 9a transplanted onto it: lease/antalya-26.6/pr09a-on-pr8-new = c5cd8d0bfd0 (10 commits, no conflicts, same diff 559/354) → gate-pr09a in janitor.
- 16:21 PR 8 COMPLETE on PR1+PR4 (989c861c71c). Opens after #2513 merges (or stacked, owner's call).
- 16:34 watchdog: gate-pr09a: release 2646, integration renewal_retry 10/10, drop_pool_member exit 0, ASan running; impl-pr02v2 final gate running (16:33) after redoing mutation 2. Disk 122G, mem 63G.
- 16:37 9a gates green on new PR 8; squashed to lease/antalya-26.6/pr09a-squash (e1548867e453c8c27d86050a7d40bf6bee10109e). Janitor worktree free.
- 16:43 PR 2 v2 implemented (4bf103ba0c4; release 2632, ASan 2636/0, integration 4/4, 6 mutations red) → review-pr02v2 (opus). impl-pr10v2 running in janitor. cas-95-1 free for 9b next.
- 16:48 PR 2 v2 review: ready; minors N1+P1–P4 → fix round 1 (impl-pr02v2, cas-95-1).
- 16:53 watchdog: impl-pr02v2 fix round 1 — edits in tree, gate running 16:53 (commits pending); impl-pr10v2 — Tasks 2 and 3 committed (464c1b571af, 0cc12788b4b). Disk 122G, mem 64G.
- 17:05 PR 2 v2 fix round done (9d6c6e95fb0) → rereview. impl-pr09b (sonnet) started in cas-95-1 off 9a squash e1548867e45. impl-pr10v2 running in janitor.
- 17:05 PR 2 v2 COMPLETE, squashed (732155f15e7853b46fc32c5afb6886ddba5b53c8 on PR 1). Opens after #2509 merges (owner's call).
- 17:09 PR 10 v2 implemented (d1eed8f4dd2; release 2620, ASan 2624/0) → review-pr10v2 (opus).
- 17:13 watchdog: impl-pr09b T1 red 17:09 → green build 17:11 (no commit yet); review-pr10v2 running (started 17:06, +deposed-lane question). Disk 122G, mem 65G.
- 17:19 PR 10 v2 review: needs changes (3 minor code/test, 7 prose) + my ruling: deposed-lane gate latches too → fix round 1 (impl-pr10v2, janitor).
- 17:33 watchdog: impl-pr09b T1+T2 committed (3f7e4436277), T3 red build 17:33; impl-pr10v2 fix round 1: C1/C2/T1/prose/deposed gate committed b095aeda4d9 17:29, C3 (recovery walk latch) in progress, gate running. Disk 121G, mem 64G.
- 17:54 /tmp freed by owner (cas_unit_* debris, 12957 dirs). Both implementers resumed with TMPDIR=<build>/tmp. CAS-349 filed (eeee447f5fd); impl-cas349 (sonnet) edits in throw-away worktree cas-95-1/tmp/worktrees/cas349, branch fix/antalya-26.6/cas-unit-test-temp-roots off 5b7c9f503df; gates in janitor after impl-pr10v2 finishes. RULE: every gtest gate runs with TMPDIR=<build_dir>/tmp and cleans cas_unit_* after.
- 17:54 watchdog: after /tmp incident both implementers resumed; 9b T3 green build 17:41 → gate running; pr10v2 fix round 2 building/gating (TMPDIR in use: cas_unit_* under build/tmp in both trees); impl-cas349 editing. /tmp inodes 4%. Disk 122G, mem 56G.
- 18:07 PR 2 v2 opened https://github.com/Altinity/ClickHouse/pull/2516 (732155f15e7 on PR 1, stacked). Open: #2509(1) #2510(5) #2511(3) #2512(7) #2513(4) #2516(2v2). CI #2509: no PR-related reds; DCO missing sign-off on 1/5/3 — owner's call.
- 18:11 OWNER: DCO on #2509/#2510/#2511 stays as is (do not rewrite); every new commit/PR carries Signed-off-by (already the rule since PR 7).
- 18:13 watchdog: 9b T1–T6 committed (f09eb76c81a 18:10), ASan gate running; pr10v2 commit A b5a7430a3dd 18:05 (round 2), commit B (C3) gate running. Disk 121G, /tmp 4%, mem 67G.
- 18:21 CI #2513: 5 reds, none PR-related (Iceberg UBSan, Grype×2, Swarms infra, stateless cas-s3 server died on 04605_parallel_replicas_merging_aggregated_bucket_order = upstream ClickHouse#115986 class, no CAS frames). Recheck when pending finish; file an Altinity issue if it recurs.
- 18:27 PR 8 rewritten as 7 commits on PR1+PR4 base (head 2f650d9efe9, tree = 989c861c71c, Signed-off-by on each); 9a squash reparented onto it. Body tmp/pr-body-lease-pr08.md. Intermediate-commit compile check queued for janitor after CAS-349.
- 18:32 PR 8 opened https://github.com/Altinity/ClickHouse/pull/2517 (7 commits, head 2f650d9efe9, stacked on #2509+#2513). Open: 2509 2510 2511 2512 2513 2516 2517. Intermediate-commit compile check still owed (janitor after CAS-349).
- 18:33 PR 10 v2 READY: squash b9a51702188 on base. C3 follow-up (B) needs a small round; B-X1 → CAS-350 filed.
- 18:33 watchdog: CAS-349 gate running in janitor (before-run 18:29 recorded, fixed build 18:31, gate 18:33); review-pr09b running (started 18:30). Disk 121G, /tmp 4%, mem 68G.
- 18:38 9b review: needs changes (2 Important: refusal text not reaching INSERT; postCommit text false) → fix round 1 (impl-pr09b).
- 18:43 OWNER: every NEW PR ticks 'Regression jobs to run: CAS (content-addressed storage; Antalya only)' (ci_regression_cas). Templates in cas-95-1/tmp/pr-body-*.md updated; already-open PRs untouched.
- 18:44 OWNER: 9a/9b and later PRs open only AFTER #2517 (PR 8) merges; keep them ready locally, rebase onto the merge commit then. Meanwhile: 9b fix round, plans 11a/11b/12, C3 follow-up, CAS-349, spec r8, CaMountObserveCore per-holder lease.
- 18:46 MODEL DEBT audit: updated+TLC green: MountObserve (PR1), MountRenewRetry (PR8), DiskLifecycle (9a/12), CasMountCore (PR7), RefLane (10 v1/v2). OWED: MountObserve per-holder lease (PR 2 v2) → tla-observe-declared running now; CasMountCore two-step absence (PR 12, with its plan); RefLane recovery-seal (C3 follow-up). PR 3/4/5/9b need no model change.
- 18:49 backlog synced 9252461a3e5: 7 In Progress w/ PR links, 3 Done (153, 151, 56.3), 14 notes, new CAS-351 (C3 follow-up) and CAS-352 (LIST-confirmed absence, PR 12).
- 18:53 watchdog: 9b fix round 1 gate running (18:53); CAS-349 rebuilt 18:49, gate 18:53 (no report yet); TLA observe-declared running (log 18:53). Disk 121G, /tmp 4%, mem 63G.
- 19:03 CAS-349 done: 8ecf7bf2ca8 + bf17576790a; release 2608, ASan 2612/0; leftovers 164 → 0. review-cas349 (lite) dispatched. Janitor free again.
- 19:09 MODEL: CaMountObserveCore per-holder lease done (0c0203ed0f7): 26/26 rows + 12/12 sweep as expected; mixed_longer/shorter green, old_body_longer_holder = NoFenceInsideAuthority (rollout caveat), sab_own_lease_only = violation, witness wait ≥ holder lease; new invariant FencingWaitCoversDeclaredLease. Not modelled: a holder changing its lease between renewals (note for spec r8).
- 19:13 watchdog: bisect-pr08 at commit 6/6 (1–5 build OK, targeted tests pass); rereview-pr09b running; impl-cas349 waits for janitor. Disk 121G, /tmp 4%, mem 63G.
- 19:14 PR 8 bisect check: all 6 intermediate commits build, targeted tests pass (tmp/lease-series/pr08-bisect-check.md). Janitor → CAS-349 round 2 gates.
- 19:17 9b re-review: all addressed, one new test gap T2 → fix round 2 (impl-pr09b).
- 19:31 CAS-349 READY: squash 458b49619ee4472c2502e8212846564652e60d99 (tests only, 11 files +109/−12); re-review approve. Report nit: the 'cannot remove' grep is vacuous (unit tests have no logger channel) — empty TMPDIRs are the evidence.
- 19:34 watchdog: impl-pr09b fix round 2 (release gate done 19:32, ASan building). Base for 11a built: lease/antalya-26.6/base-pr1-4-7-8-9a = ea46aa00861 (9a line + PR 7, clean merge). plan-pr11a (opus) dispatched. Janitor free. Disk 121G, /tmp 4%, mem 68G.
- 19:44 9b COMPLETE (7 commits, 674c618ae18 on 9a). cas-95-1 free. Waiting: plan-pr11a.
- 19:53 watchdog: plan-pr11a writing (48 KB at 19:50); nothing else running; both worktrees free. Disk 121G, /tmp 4%, mem 68G.
- 19:55 11a: plan committed a5341d380a2; rulings recorded; tla-pr11a (model, master) + impl-pr11a (opus, janitor, off ea46aa00861) running. Next: plan 11b (after 11a's interfaces land).
- 20:13 watchdog: impl-pr11a T4+T5 committed (1454fce5360 20:10), T6 red/green builds 20:11; tla-pr11a TLC running (log 20:10). Disk 120G, /tmp 4%, mem 62G.
- 20:24 11a model: counterexample on the alloc split (stale own adopt write between guard and apply) → ruled: adopt blocked while allocation pends (driver serialization); rerun of affected rows; implementer to confirm the serialization in code.
- 20:33 watchdog: impl-pr11a Tasks 2–6 committed (6ecb91283ce 20:21), ASan gate running (20:28); tla-pr11a rerunning affected rows (TLC alive). Disk 118G, /tmp 4%, mem 62G.
- 20:40 11a: straggler renewal PUT can land after the allocation (pre-existing; no data risk thanks to the fence latch; costs an observation window). Model fix direction under analysis; PR 14 removes it by the one-tuple rule.
- 20:45 11a model step 2a committed 781506a4af9 (option 2; twins_immalloc green 513M states). Follow-up asked: straggler row (adopt allowed, fence latch on Write) — green expected if the latch is the real guard. review-pr11a (opus) dispatched with the straggler as item (4).
- 20:52 11a review: ready; minors → fix round 1. Straggler guards confirmed (fence latch; no reclaim without certificate).
- 20:53 watchdog: impl-pr11a fix round 1 in progress (gtest_cas_pool.cpp edited, ASan gate2 20:50); tla-pr11a latch rerun (TLC alive). Disk 117G, /tmp 4%, mem 60G.
- 21:09 11a model: straggler row green with the fence latch (3357e54496f): the product's guard suffices; witness that the straggler path is reached requested as the last follow-up. plan-pr11b dispatching.
- 21:13 watchdog: rereview-pr11a running (started 21:12); plan-pr11b writing (started 21:10); tla-pr11a witness rerun (TLC alive, twins_immalloc 513M states ~15 min). Disk 118G, /tmp 4%, mem 65G.
- 21:15 PR 11a COMPLETE (5 commits, ef8a1c57bad). 11b plan pending; its branch cuts from ef8a1c57bad.
- 21:33 11b plan committed 5e1d7b58a22 (5 tasks); impl-pr11b (opus, janitor, off ef8a1c57bad) running; model Task 1 waits for tla-pr11a's witness commit.
- 21:33 watchdog: impl-pr11b branched (no edits yet, reading — recheck next tick); tla-pr11a witness run active (TLC 3 min old, cfg edits in tree). Disk 119G, /tmp 4%, mem 64G.
- 21:36 11a model DONE (4 commits, witness that the straggler lands). tla-pr11b (Task 1) dispatching.
- 21:53 watchdog: impl-pr11b T2+T3 committed (af72543c4cd 21:46), T4 (renewer from the claim) red build 21:47 → editing runtime; tla-pr11b TLC running (log 21:47). Disk 121G, /tmp 4%, mem 61G.
- 21:58 OWNER DECISION: PR 12 stop mechanism split — external losses → raise(SIGTERM) via BaseDaemon::terminate (graceful); broken invariants (ledger_fault, local_defect) → LOG_FATAL + std::terminate(). Recorded in spec-r8-todo; goes into the PR 12 plan.
- 22:02 OWNER: stop split kept provisionally; PR 12 must prove shutdown does not hang on a dead disk; may be revisited.
- 22:13 watchdog: impl-pr11b T2–T5 committed (docs 506769e3372 22:11), final gates building; tla-pr11b TLC running (log 22:04, second cfg). Disk 118G, /tmp 4%, mem 51G.
- 22:34 watchdog: tla-pr11b TLC running (log 22:33); review-pr11b (22:34 start), plan-pr12 (22:15 start), ci-rca-2513 (22:22 start) running, files not yet written. Disk 116G, /tmp 4%, mem 64G.
- 22:42 PR 12 plan committed (13 tasks); rulings recorded; impl waits for review-pr11b (janitor) and tla-pr11b (model files).
- 22:43 11b review: needs changes (4 minor tests + prose) → fix round 1 (impl-pr11b, janitor). PR 12 impl waits for it.
- 22:51 CI RCA (tmp/lease-series/rca/ci-rca-2513-2510.md): all three pre-existing classes, none from PRs 4/5. (1) renewal PUT blocked 6 s in WriteBufferFromS3 finalize (S3 stall) → admission refused at 14 s (4 s slack) — #2431 family, same on #2512; evidence for 17b/18. (2) conditional PUT meets vanished key → 404 logged at Error (no Expect404ResponseScope) → .sh test fails via send_logs_level. (3) forceful shutdown leaves leases; restart reclaims disks sequentially ~40 s each → >100 s. Backlog tasks being filed. 8 GB elf deleted.
- 22:51 backlog: CAS-353 (slow renewal PUT refuses writes; 17b/18 evidence), CAS-354 (conditional PUT 404 at Error), CAS-355 (sequential lease reclaim after forceful shutdown) filed, 7710f7125f1.
- 22:53 watchdog: impl-pr11b fix round 1 committed (9d70e750325 22:48), gates building; tla-pr11b TLC process alive (battery log 22:37). Disk 108G, /tmp 4%, mem 60G.
- 23:04 PR 11b code COMPLETE (4 commits, dabbfabfb28, +813/−931). Janitor free → PR 12 impl (code tasks) now; PR 12 model tasks after tla-pr11b.
- 23:13 watchdog: impl-pr12 base merge 790a4c66e87, cmake regen (new files), red T2/T3 build 23:13; tla-pr11b single long TLC run (pid since ~22:50). Disk 100G (watch; threshold 60G), /tmp 4%, mem 59G.
- 23:35 INCIDENT: tla-pr11b row twins_immclaim ran 47 min, 29 GB spilled to tmp/tlc-meta-scratch, disk 121→83G. Killed by controller (7.97M gen / 1.94M distinct / 1.06M queued), scratch removed (disk 112G). Agent told: record not-converged, run small rows, reduced config only under ~30M states with a 20-min timeout. RULE: TLC rows get an explicit timeout ≤ 20 min and a disk check; metadir spill means the bounds are too large.
- 23:49 11b model: plan-literal row red (model artifact: second alloc/claim between claim and open); ruled latch-from-ClaimBegin + AllocBegin guard; reduced-bounds twins_immclaim.
- 23:53 watchdog: impl-pr12 T7 committed (dd1cdf6df55 23:44: stop on taken/absent/corrupt), T8 building; tla-pr11b reduced-bounds run 4 min in, no scratch spill. Disk 112G, mem 62G.
- 00:13 watchdog: impl-pr12 T8 (integration test: lost slot stops the server) committed f9c69b611e0, gate running; tla-pr11b: no TLC process, no commit — status check sent. Disk 111G, mem 67G.
- 00:21 11b model DONE (1d5b639d000, f41f88acf62). PR 11b COMPLETE. PR 12 model tasks can start in master (tla-pr12).
- 00:33 watchdog: tla-pr12 battery running (log 00:33, between rows); review-pr12 running (00:30 start). Disk 118G, mem 67G.
- 00:40 PR 12 review: 3 Important (thread-directed raise ineffective under Poco's blocked SIGTERM → kill(getpid); recovery text names a path that is refused again; graceful-class shutdown test missing) → fix round 1. NOTE for owner: C1 is a real catch on the SIGTERM mechanism.
- 00:50 PR 12 models committed (d05be03d197); follow-up: stop modelled as a permanent latch. Code fix round 1 running.
- 00:53 watchdog: impl-pr12 fix round 1 committed through 28048bf5ce3 (00:48), final gates running; tla-pr12 follow-up TLC rows running. Disk 118G, mem 62G.
- 00:56 PR 12 models DONE (1250c34a796). Waiting: impl-pr12 fix round 1 gates.
- 01:14 watchdog: rereview-pr12 running (01:09). Plans 13 (farewell rules) and 14 (reacquire in the lease thread) dispatched against PR 12's head. Disk 118G, mem 68G.
- 01:15 PR 12 re-review: round 1 all addressed; one new minor N1 → fix round 2.
- 01:30 PR 12 COMPLETE (10 commits, a470f914557). Janitor free → PR 13 after its plan; plans 13/14 in progress.
- 01:32 PR 14 plan committed (12 tasks + setup). tla-pr14 (models, master) dispatching; code after PR 13.
- 01:32 PR 13 plan committed (7 tasks). impl-pr13 (sonnet, janitor, off a470f914557) dispatching; its model task after tla-pr14.
- 01:33 watchdog: impl-pr13 branched 01:30 (reading plan); tla-pr14 started 01:31 (no run yet). Both just dispatched. Disk 118G, mem 68G.
- 01:53 watchdog: impl-pr13 all tasks committed (5111307d8ca docs), final gates running; tla-pr14 TLC rows running (sab_reacqclean metadir 2.1G, within guard). Disk 114G, mem 58G.
- 02:13 watchdog: review-pr13 running (02:10 start); tla-pr14 rows running (reacq_cert, metadir 2.1G). Disk 115G, mem 62G.
- 02:16 PR 13 review: approve, prose round; merge waits on its model (after tla-pr14).
- 02:18 PR 13 code complete (a18ad0dfbab). Janitor free → PR 14 code can start once its models commit (tla-pr14 running).
- 02:33 watchdog: impl-pr14 T3+T4 committed (45ea569ded4 02:33); tla-pr14 battery still running (log 02:33, no commit yet, ~60 min in). Disk 116G, mem 61G.
- 02:53 watchdog: impl-pr14 T5 committed (f92081563ed), T6 (step machine) building 02:48; tla-pr14 battery at row sab_reacquireredecides (re-running affected existing rows). Disk 117G, mem 67G.
- 03:13 watchdog: impl-pr14 T6 committed (af2f94292c6 02:54: step machine), T7 (renewal thread runs it) building 03:08; tla-pr14 battery still running (row sab_reacqclean; many rows re-run — ~100 min so far). Disk 116G, mem 57G.
- 03:34 watchdog: tla-pr14 DONE (0fd85e5a2e8, battery ALL DONE 03:33) — report pending; tla-pr13 dispatched; impl-pr14 at Task 10 (matrix integration module running), mutations done. Disk 117G, mem 65G.
- 03:35 PR 14 models done (2 commits); VIEW soundness check running (known trap).
- 03:41 PR 13 COMPLETE (model 4973c7e722a; 5 commits 470e1eb1f6d). Remaining code: 14 (running), 15, 16, (17a optional).
- 03:53 watchdog: impl-pr14 Task 10 matrix committed (55fb37682f5 03:53), final ASan build running. Nothing else running. Disk 117G, mem 66G.
- 04:14 watchdog: impl-pr14 finishing — no-S3 compile in janitor/build_fasttest (dir made by impl-pr12 earlier; 6015/7367, 9.5 GB), report being written 04:12. Disk 106G, mem 58G.
- 04:17 PR 14 code in review; plan-pr15 (remove the remount machine, inventory-first with the exit proof) dispatched against PR 14's head.
- 04:28 PR 14 review: pass; minors → fix round 1. Storm churn = arithmetic of the matrix's 10 s TTL, not a defect.
- 04:33 watchdog: impl-pr14 fix round 1 in progress (T3 committed dd78e01e64c 04:33, mutation runner building); plan-pr15 writing (started 04:20, no file yet). Disk 105G, mem 65G.
- 04:41 MERGED by owner 2026-10-08 20:21–20:23: #2509 (PR 1), #2510 (PR 5), #2511 (PR 3) → freeze branch d76b59f9295. Open: #2512 #2513 #2516 #2517. PR 15 plan committed (−1000 net); needs PR 3 in base. tla-pr15 dispatching.
- 04:49 PR 15 model done (13efdc37e8d, 14 remount rows removed). Code 15 waits for impl-pr14's fix round.
- 04:53 watchdog: impl-pr14 fix round 1 commits done (docs d9102b34fcc 04:40), final ASan gate running. Nothing else running. Disk 106G, mem 63G.
- 05:07 PR 14 COMPLETE (6 commits, 903e03ae516). impl-pr15 (opus, janitor) dispatching off merge(903e03ae516, PR 3 949578b2d73).
- 05:13 watchdog: impl-pr15 base merge done (58317e1dda6, docs conflict resolved), Task 2 deletion edits in tree, ASan build 05:12. Disk 106G, mem 66G.
- 05:33 watchdog: impl-pr15 Task 2 committed (b1a99700947: the six exit tests; 00f0e832bfe adapts PR 3's tests), Task 3 deletion edits in tree (14 files), no build running right now (editing). Disk 105G, mem 67G.
- 05:54 watchdog: impl-pr15 Task 3 deletion: release+ASan builds done (NINJA_EXIT=0, 05:48/05:49), 18 files edited; gates next. Disk 105G, mem 65G.
- 06:14 watchdog: impl-pr15 Task 3 committed (969facf8582: retire the remount-driving tests), Task 4 edits (13 files) building ASan. Disk 105G, mem 50G.
- 06:34 watchdog: impl-pr15 Task 4 committed (c64cf779b15: remove the self-remount and its second thread), Task 5 building. Disk 105G, mem 55G.
- 06:53 watchdog: impl-pr15 Task 5 committed (51549e5dc94: one renewal thread, no parking), Task 6 (producer-less states) edits in 18 files, ASan built 06:50. Disk 105G, mem 64G.
- 07:13 watchdog: impl-pr15 Tasks 6+7 committed (30ba2bc4cc6 states; c6f42bdaa32 MountFence fields), Task 8 (counters/columns, 22 files) building ASan. Disk 105G, mem 54G.
- 07:33 watchdog: impl-pr15 Task 8 committed (4b7b442c09e rename remount→reacquire in code), Task 10 (docs + clickhouse build for integration) in progress, 11 files edited. Disk 105G, mem 66G.
- 07:47 PR 15 code done (a087959dfcb, +1021/−4550) → review-pr15 (opus). plan-pr16 (observability stack + counter sweep) dispatched.
- 07:54 watchdog: review-pr15 (07:47 start) and plan-pr16 (07:48 start) running; no files yet. Disk 107G, mem 68G.
- 07:57 PR 16 plan committed (8 tasks, 18 counters removed, −200 prod). Impl after PR 15's review.
- 07:58 PR 15 review: product OK, test X3 unreliable + prose → fix round 1.
- 08:13 watchdog: impl-pr15 fix round 1 commits done (bfe05e141ab prose), integration modules running (matrix). PR 16 plan ready; impl after 15 closes. OWNER question pending: the series' net +955 production lines — proposed a 'shrink' pass after 16 (seams, text builders, comments; −400…−600).
- 08:31 PR 15 COMPLETE (9 commits, b81092d3e85, +1078/−4588). Janitor free → impl-pr16.
- 08:33 watchdog: impl-pr16 branched 08:31, reading plan/inventory. Disk 107G, mem 67G.
- 08:54 watchdog: impl-pr16 Task 2 goldens committed (3be7b593836), Task 3 (value-based classification) ASan gate running. Disk 107G, mem 66G.
- 09:13 watchdog: impl-pr16 Task 3 committed (8937b6f6c84: classification by value, thread-local gone), Task 4 (deadline sweep, 12 files) ASan gate running. Disk 107G, mem 65G.
- 09:34 watchdog: impl-pr16 Tasks 4+5 committed (77c60a42f63 deadline counter; c1167c432aa unreachable request counters), Task 6 (reacquire/expiry counters) gate running. PROPOSAL pending with owner: regroup 9a+9b, 11a+11b, split 12 (12a LIST / 12b stop handlers independent / 12c), merge 14+15. Disk 107G, mem 67G.
- 09:51 PR 16 code done (fd3f78bebc5, −289 prod) → review-pr16. CAS-356 filed (ed01195ab84): soak/grafana retarget held as a diff until the series is in the nightly; drilldown test needs a source override.
- 09:54 watchdog: review-pr16 running (09:47). Spec r8 revision dispatching (spec-r8 agent, opus) from tmp/lease-series/spec-r8-todo.md.
- 09:58 PR 16 review: code correct; two test gaps → fix round 1. NOTE for owner: ~/.claude/skills/analyzing-cas-health still uses the removed counter names.
- 10:06 spec r8 committed 0cb9107ed87 (34 todo items; new 'Баланс серии'; §12 'Отложено'). Sizes: agent measured +3537/−2582 (mine excluded :!src/**/tests/** — same net +955).
- 10:13 watchdog: impl-pr16 fix round 1 edits in tree (4 files), ASan gate running 10:13. Spec r8 done. Disk 107G, mem 66G.
- 10:22 PR 16 rereview: ready; one minor → fix round 2 (close-all-minors rule).
- 10:33 watchdog: impl-pr16 fix round 2 — mutation proof for the Ours arm running (10:33). Disk 107G, mem 67G.
- 10:53 watchdog: impl-pr16 fix round 2 — ASan gate running (10:53), release done. Disk 107G, mem 66G.
- 11:00 PR 16 COMPLETE (7 commits, 3b4037ae265, +354/−631 incl. tests; prod −289). All 16 code PRs done. Next: final campaign report.
- 11:01 CAMPAIGN END: final report written (FINAL-REPORT.md); watchdog cron 770beb71 deleted. Remaining items are owner-gated (see report §Ждёт владельца).
- 2026-10-12 09:43 OWNER: lease machinery disproportionate to its job (exclusion of a second writable mount of one srid). Research launched (research-no-clock-fencing, opus): time-dependency inventory, zombie write catalogue, clock-free prevention → tmp/lease-series/research/no-clock-fencing.md. Plan: independent codex adversarial check of its verdict before proposing. Series PRs 9a+ stay unopened meanwhile.
- 10-12 10:02 research report done (no-clock-fencing.md): ref-log/manifests/slot/blobs/GC already clock-free; Z5 plain objects need format change; Z6/Z8/Z15 closable. codex adversarial check running → research/no-clock-fencing-codex.md

## 2026-10-12 no-clock nomenclature {#2026-10-12-no-clock-nomenclature}
- base d76b59f9295 (altinity/antalya-26.6-20261008, has #2509-2511). PR 9a-16 frozen until the verdict.
- codex check done: Z8 404-fix REFUTED; blob SDK resend ABA; staging sweep, decommission, admission-to-dispatch gap; Z5 closable through manifests/refs without new wire fields (needs migration).
- inventory agent (opus) running -> tmp/noclock/inventory.md (prompt tmp/noclock/prompt-inventory.md). Next: codex completeness check, then one researcher per unsafe class.
- 2026-10-12 per-object audit: 15 read-only agents (brief tmp/noclock/audit/BRIEF.md) -> tmp/noclock/audit/{blob,meta,manifest,reflog,snapckpt,files,roots,catalog,gcctl,mount,epoch-owner,staging,poolprobe,protocol,transport}.md. Next: SUMMARY.md with the action table.
- 2026-10-12 audit done: 15 reports + tmp/noclock/audit/SUMMARY.md (packages P1-P6). Worst holes are NOT time-based (meta HEAD/GET race, manifest S15, epoch T6/T12, blob SDK resend).
- 2026-10-12 codex model-atomicity audit wave 1 (4 runs): tmp/noclock/atomicity/w1-{blob,reflog,mount,catalog}.md. Wave 2 = remaining models.
