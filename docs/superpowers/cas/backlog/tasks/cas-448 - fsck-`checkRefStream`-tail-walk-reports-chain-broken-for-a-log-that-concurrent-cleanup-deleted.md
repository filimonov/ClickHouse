---
id: CAS-448
title: >-
  fsck `checkRefStream` tail walk reports chain-broken for a log that concurrent
  cleanup deleted
status: To Do
assignee: []
created_date: '2026-11-11 06:48'
updated_date: '2026-11-11 06:48'
labels:
  - 'area:fsck'
  - 'area:gc'
  - 'origin:review'
dependencies: []
priority: medium
type: bug
ordinal: 551000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`checkRefStream` (CA/Tools/CasFsck.cpp) samples `_ckpt`, then walks `base+1 .. committed_through`. An absent tail id goes to `recordChainBroken` with no `_ckpt` revalidation; the base path in the same function re-reads the `_ckpt` etag and records `Unchecked` when it moved. Interleaving on a live pool: fsck samples `_ckpt` (base 10, frontier 30); the writer publishes checkpoint 30; GC folds and `cleanupRefObjects` deletes log 11; fsck reads 11, gets 404, reports chain-broken. The pool is healthy. `recordUnchecked` then returns early for that namespace because `chain_broken` already holds it, so the later recovery leg cannot downgrade the verdict. Rare today because ref cleanup almost never runs (CAS-447); routine once CAS-29.8 drops the whole-catalog etag term. Fix shape: on an absent tail id re-read `_ckpt`; a changed etag means the view is stale, record `Unchecked` (or restart the namespace), as the base path does. Found by the CAS-29.8 adjudication review: docs/superpowers/reports/2026-11-11-cas-29.8-ref-cleanup-licence-adjudication.md.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A gtest deletes a covered tail log and advances `_ckpt` between fsck's `_ckpt` read and its tail read; fsck does not report chain-broken
- [ ] #2 A tail log missing under an unchanged `_ckpt` still reports chain-broken
- [ ] #3 Lands in the same release as the CAS-29.8 code change
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
