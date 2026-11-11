---
id: CAS-29.8
title: >-
  Adjudicate and implement a per-life cleanup licence so one namespace's churn
  stops refusing every cleanup (spec B3)
status: To Do
assignee: []
created_date: '2026-08-21'
updated_date: '2026-11-11 07:52'
labels:
  - 'area:gc'
  - 'complexity:medium'
  - 'risk:high'
  - 'touches:protocol'
  - 'confidence:plausible'
  - 'needs:decision'
  - 'origin:review'
  - 'origin:2031-triage'
milestone: m-1
dependencies:
  - CAS-29.7
  - CAS-29.6
  - CAS-449
references:
  - >-
    docs/superpowers/reports/2026-11-11-cas-29.8-ref-cleanup-licence-adjudication.md
documentation:
  - >-
    docs/superpowers/specs/2026-09-25-cas-gc-rounds-in-minutes-design.md#b3-cleanup-licence
  - >-
    docs/superpowers/specs/2026-09-15-cas-gc-dead-namespace-debris-cleanup-design.md
parent_task_id: CAS-29
priority: high
type: design
ordinal: 114000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The whole-catalog etag equality in `authorityHolds` licenses irreversible deletes; its comment calls the strictness deliberate
("the SAME complete catalog observation and GC lease that adopted the fold"). After B2 the cleanup no longer uses the fold-time
listing: its inputs are a catalog cut, the adopted `gc/state` and the life's own checkpoint.
Hypothesis to adjudicate, not to assume: the cleanup takes its own fresh catalog cut at phase start; each chunk is licensed by
row value, life resolution (`throwIfAmbiguous`) and `gc/state` owner + `seq`; a refusal is scoped to its namespace and the pass
continues. It is also the only licence shape that survives multi-node execution.
Rejected: relaxing the etag check directly. Constraints: the round stays one-pass, LIST trust is not reopened (decision-2),
no new object kinds (decision-4). Process: ca-arch with the code, then codex review capped at three rounds, then code.
Proceed only if the measurement subtask shows etag-only refusals dominate.

Provenance: BACKLOG/gc.md#covered-log-cleanup-aborts-on-catalog-etag (plan step 2 and 'not to do'); formerly {#ref-cleanup-whole-catalog-token-stillness}/2031-triage CAS-079. u01's pointer bullet merges here. Verified 2026-09-26 against 59494ebf366 and 0dbbd797792.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 A written adjudication answers whether the fold seal or this life's coverage depends on the rest of the cut, reviewed to no MAJOR
- [ ] #2 A CREATE of an unrelated table between fold and cleanup no longer refuses the pass
- [ ] #3 A changed row or life of the cleaned namespace refuses that namespace only; a moved `gc/state` owner or seq refuses everything
- [ ] #4 On the ca-s3 lane, refusals whose row and life were unchanged are zero
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
First recorded: 2026-08-21 (e811b3d68f4, by 'CAS-079')

2031-triage CAS-079: label added; the audit ranks CAS-079 #3 in 'Where to start'.

2026-11-11 adjudication done (AC #1): docs/superpowers/reports/2026-11-11-cas-29.8-ref-cleanup-licence-adjudication.md, code read at 3fa3a01203b (antalya-26.8-pr-2159). ca-arch verdict, then codex gpt-6-astra review: round 1 not sound (0 CRITICAL, 4 MAJOR overclaims: reader safety under a stale `_ckpt`, durable-only eligibility, reader inventory, a non-discriminating alias test), round 2 sound with tightenings (0 CRITICAL, 0 MAJOR, 3 MINOR, applied). Neither round found a data-loss interleaving against the relaxation. Verdict: drop condition 1 (whole-catalog etag) from `authorityHolds`; keep row equality, `life_index.resolve` + `throwIfAmbiguous`, and `gc/state` lease owner + seq. A row or life mismatch skips that namespace and the pass continues; lease moved, `gc/state` absent, catalog read failure or an ambiguous index stop the pass. Keep the fold's cut as the reference observation: resolving the life from a fresh cut is unsafe before B2 because `folded.ref_tables` is keyed by name and ids restart per life. So this change does NOT depend on CAS-29.6; the description's 'fresh catalog cut at phase start' is permitted only as a comparison target. The description's 'Rejected: relaxing the etag check directly' is superseded by this adjudication: the etag term adds no protection beyond conditions 2-4 because no reader of a life's `_log`/`_snap` takes another namespace's row as input. Tests: retarget `CatalogTokenMoveBeforeFirstChunkRefusesEveryRefObjectDelete` and `CatalogTokenMoveBetweenChunksAllowsFirstAndRefusesSecondDelete`; 8 new tests listed in the report (the aliasing test must inject during a cleanup chunk, not from the post-hot-scan hook, or the fold's orphan sweep throws first). Not claimed: safety of a reader holding an older `_ckpt`. That race is independent of the catalog and exists today; it becomes routine once cleanup runs. Its one wrong outcome is the fsck tail walk (CAS-448). Measurement gate ('proceed only if etag-only refusals dominate'): CAS-447 shows 0 deletes in 3 h on PR 2794 shard 2/2 and 46/46 stopped passes locally; CAS-29.7 counters land with the code change. No code written yet.

2026-11-11 spec: docs/superpowers/specs/2026-11-11-cas-ref-cleanup-per-life-licence-design.md (draft, not approved by the owner). Model: docs/superpowers/models/CaRefCleanupLicenceCore.tla, results in CaRefCleanupLicenceCore_RESULTS.md (12 configurations, all expectations met, TLC -workers 1). No existing model changes. Owner decisions 2026-11-11: implementation starts after PR 2805 merges into antalya-26.8 and is not part of it; no backport to antalya-26.6; the TLA+ model is added. Spec review (codex, 3 rounds): round 3 ended with 1 MAJOR, the incarnation-freshness premise is not guaranteed across admission retries of createNamespace. Folded into the spec as a stated gap and tracked as CAS-449, which is delivery step 0 and a prerequisite of the licence change. The corrected text has not had a fresh review round.
<!-- SECTION:NOTES:END -->
