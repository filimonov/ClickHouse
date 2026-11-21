---
id: CAS-297
title: >-
  Review every CAS default before the first deployment and record the evidence
  for each
status: To Do
assignee: []
created_date: '2026-09-26'
updated_date: '2026-11-21 08:22'
labels:
  - 'area:mounts'
  - 'area:gc'
  - 'complexity:medium'
  - 'risk:medium'
  - 'touches:settings'
  - 'confidence:solid'
  - 'needs:decision'
  - 'origin:otel-demo-audit'
  - 'origin:canary'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
milestone: m-0
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedSettings.cpp
  - src/Storages/MergeTree/MergeTreeSettings.cpp
documentation:
  - docs/en/antalya/cas/configuration.md
  - docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget-audit.md#f14
  - docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget-audit.md#f25
  - docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget-audit.md#f27
priority: critical
type: task
ordinal: 376000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The roadmap lists "defaults reviewed" as a first-deployment blocker. There are 31 CAS disk settings (`CA/ContentAddressedSettings.cpp`)
plus MergeTree and S3 settings that CAS relies on.
Defaults with evidence against them already: `gc_round_ref_cleanup_budget` 5000 (the cap caused the `_log` growth loop, audit F14;
spec C2 / CAS-29.11 removes the budgets), `manifest_decode_cache_bytes` 128 MiB (full on the canary, F25), `part_folder_cache_bytes`
64 MiB (weight bug CAS-22), keep-alive (one new TLS connection per ~98 requests, F27; CAS-283, CAS-74),
`concurrent_part_removal_threshold_for_remote_disk` 16.
Lease and request timing: `mount_lease_ttl_ms` 30000, `mount_renew_period_ms` 10000, `attempt_timeout_ms` 5000,
`lease_safety_margin_ms` 2000. Concurrency: `gc_read_concurrency` 16, `gc_meta_pool_size` 16, `gc_shards` 1, `gc_interval_sec` 60.
Inputs from other tasks: the decode-cache and parallel-removal tasks of this unit, CAS-107 (sizing model), CAS-128 (unexposed knobs).

Provenance: umbrella-roadmap.md section 7 bullet 'Defaults review before the first deployment'; filed 2026-09-26 (user decision). Verified 2026-09-26 against c16a2589f56 (cas-gc-rebuild) and 8d62c314ec1 (altinity/antalya-26.6).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A table lists every CAS setting with its default, the evidence (soak, canary, or none) and a keep or change decision
- [ ] #2 Every changed default is applied, with the old value noted for `compatibility` where the setting is user-visible
- [ ] #3 `configuration.md` states for each setting when an operator should change it
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
First recorded: 2026-09-26 (filed during the Backlog.md migration; no earlier trace in docs/superpowers history)

Identifier trace: the earliest docs mention of `mount_lease_ttl_ms` is 2026-07-02 (cacba14492b); the finding itself has no record before this migration, so the creation date stays 2026-09-26.

2026-11-11 defaults review: the per-setting table exists (32 disk settings, the server pool setting, timing and GC constants), from 66 stand runs and otel.demo data. Proposed: `cas_mount_lease_ttl_ms` 30000 -> 120000 with the renew period kept at 10000; `cas_gc_round_graduation_budget`, `cas_gc_round_redelete_budget`, `cas_gc_round_ref_cleanup_budget` 5000 -> 0; everything else kept with a reason each. Not settled by evidence: `cas_part_folder_cache_bytes` (0.5 MiB of 64 MiB used on otel.demo at 89 % hits; wait for CAS-22) and `cas_manifest_decode_cache_bytes` (full, no hit counters; CAS-452). AC #1 is met by the report; AC #2 and #3 need the owner's decision and a code branch. Report: docs/superpowers/reports/2026-11-11-cas-defaults-review.md on branch cas-297-defaults-review (worktree ~/workspace/ClickHouse/cas-297-defaults); run data master/tmp/cas297/runs; binary PR 2805 @41d4a78eeb9.

2026-11-12 defaults review: v2 of the report after direct (no proxy) reruns and the many-tables and table-churn scenarios the owner added (107 runs in all). Five default changes proposed: `cas_mount_lease_ttl_ms` 120000 and the four count budgets (graduation, redelete, ref_cleanup, outcome_entry) to 0; 27 settings kept with a reason each. Stand artefact found and stated in the report: the Python fault proxy caps throughput at about 17 INSERT/s and inflated every latency and GC-cost figure of the first version; fault results (error counts, lease behaviour) are not affected. Report v2: docs/superpowers/reports/2026-11-11-cas-defaults-review.md on branch cas-297-defaults-review (f9ec0861aff); run data master/tmp/cas297/runs; binary PR 2805 @41d4a78eeb9.

2026-11-12 correction after an independent check of the report against run data (docs/superpowers/reports/2026-11-11-cas-defaults-review.check.md.txt, a49f8a8e45f): the report proposes six default changes, not five: add `cas_gc_round_handoff_prefix_wholesale_budget` 5000 -> 0, because `PoolConfig` defaults it to unbounded with the note that a cap on this one-shot reclaim leaks objects (`Pool/CasPool.h`); not exercised in any run. `PoolConfig` defaults graduation, redelete, outcome and hand-off to 0 and ref-cleanup to 5000; the disk-setting layer says 5000 for all five. The proposed defaults do not fully meet the 90 s target: a 90 s fault still costs 1-4 client errors until CAS-405.6.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:22
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: ContentAddressedSettings.cpp:71,76,80 shows the budgets already 0 and mount_lease_ttl_ms 40000 (not the proposed 120000); report with the per-setting table is on branch cas-297-defaults-review, not in the checked tree; AC2/AC3 need owner decision. Changes: none. Links: Inputs: CAS-405.3 (lease 120 s), CAS-405.6, CAS-22, CAS-452, CAS-128. Pointers stale: Task text lists budgets 5000 and ttl 30000; code now has budgets 0 and ttl 40000. Would settle it: owner decision on the six proposed default changes (ttl 120 s still pending; see CAS-405.3) and whether configuration.md states change guidance per setting. Owner question: Approve the proposed default changes (lease TTL 120 s and the remaining budgets) from the defaults-review report?
---
<!-- COMMENTS:END -->
