---
id: CAS-176
title: Publish a part with zero GETs in the common path (decision-5)
status: To Do
assignee: []
created_date: '2026-08-03'
updated_date: '2026-11-17 20:02'
labels:
  - 'area:write-path'
  - 'area:ref-ledger'
  - 'complexity:large'
  - 'risk:medium'
  - 'touches:protocol'
  - 'confidence:solid'
  - 'origin:otel-demo-audit'
  - 'origin:canary'
milestone: m-2
dependencies: []
references:
  - docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget-audit.md#f31
  - docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget-audit.md#f30
  - docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget-audit.md#f20
  - docs/superpowers/cas/umbrella-roadmap.md
documentation:
  - docs/superpowers/reports/2026-09-25-otel-demo-cas-s3-budget-audit.md
priority: high
type: feature
ordinal: 229000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
On the otel.demo canary a part publish takes 389 ms: 249 ms of ref-lane wait and 125 ms in five serial S3 reads (audit F30).
The five reads are two catalog `GET`s, two `_ckpt` `GET`s and one manifest re-read; 748 ms of an 887 ms insert is lane queue wait.
A flush is four serial round trips: catalog `GET`, `_ckpt` `GET`, `_log` `PUT`, `_ckpt` `PUT`. The 108.7 `GET`/part insert figure includes these reads.
decision-5 (owner, 2026-09-25, audit F31): namespaces are node-owned (`<server_root_id>/store/<uuid>@cas@`), so each read re-checks a race this process already closes in-process, or re-fetches bytes it already holds.
Verified: the only `Live`-to-`Removing` catalog write is `CasRefCatalog::beginRemoving`, reachable from the node's own drop path or from `CasDecommission` after it claims the victim's lease (fails closed while live, `CasPool.cpp:754-769`).
Four subtasks, one per read. Target: a publish is blob PUTs, one manifest PUT, two `_log` PUTs and two `_ckpt` PUTs, no GET; a flush goes from four round trips to two. Nothing changes in what is written or when.
Expected effect on F30's numbers: 125 ms saved directly and about half of the 249 ms lane wait, roughly a 2x faster publish.
Out of scope: the blob `HEAD` (decision-1) and the lazy `_ckpt` question (audit F5, separate owner decision).

Provenance: BACKLOG/ref-protocol.md#part-publish-zero-gets, absorbing BACKLOG/performance.md [ckpt-read-policy], #ref-catalog-read-per-commit and #writepath-candidates-post-stage1 item (5). Verified 2026-09-26 against b1c34d03479 (cas-gc-rebuild) and 0dbbd797792 (altinity/antalya-26.6): all four reads present on both.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A lone `INSERT` on a CAS disk issues no S3 `GET` on the common path (ProfileEvents on the `NewPart` row: catalog, `_ckpt` and manifest reads all zero)
- [ ] #2 A ref-lane flush is two serial round trips (`_log` PUT, `_ckpt` PUT) on the common path
- [ ] #3 A matched before/after publish latency and request count, measured on current HEAD (not the stage-1 1.59x figure, which predates `_ckpt`), is recorded
- [ ] #4 Every fallback read (412 on `_ckpt`, `Unresolved` staging PUT, invalidation edges) is covered by a gtest that forces it
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
First recorded: 2026-08-03 (73755caa6e5, by 'ckpt-read-policy')

CAS-487 delivers part of this task: two of the four reads are gone (the `_ckpt` read and the `_ckpt` write); a flush is one `_log` `PUT`. The catalog `GET` stays until Part B. The manifest re-read (CAS-176.3) stays. AC #3 (matched before/after on HEAD) is still owed.

2026-11-17: Merged 2026-11-17 as 0aa837e942e into antalya-26.8 (https://github.com/Altinity/ClickHouse/pull/2831). Partly delivered; the task stays open. PR 2831 delivers AC1 to AC3: a lone INSERT issues no catalog, `_ckpt` or manifest read, and an append is one `_log` PUT. AC4 needs one decision: the fallback reads it lists mostly no longer exist; close with that reason after CAS-176.1 AC3.

2026-11-17: Children after PR 2831 (https://github.com/Altinity/ClickHouse/pull/2831, merged as 0aa837e942e) : CAS-176.1 and CAS-176.3 landed, CAS-176.2 closed as obsolete, CAS-176.4 open.
<!-- SECTION:NOTES:END -->
