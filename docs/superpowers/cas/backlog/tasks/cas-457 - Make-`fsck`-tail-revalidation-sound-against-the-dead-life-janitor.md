---
id: CAS-457
title: Make `fsck` tail revalidation sound against the dead-life janitor
status: To Do
assignee: []
created_date: '2026-11-11 17:18'
labels:
  - 'area:fsck'
  - 'area:gc'
  - 'origin:review'
  - 'risk:low'
dependencies:
  - CAS-448
priority: low
type: bug
ordinal: 561000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
After CAS-448 `checkRefStream` rereads `_ckpt` when a tail log is absent and reports `ChainBroken` when the etag is unchanged. That premise holds for ref cleanup, which advances `_ckpt` before it deletes. The dead-life janitor (`CA/Gc/CasNamespaceJanitor.cpp`) also deletes `stream/<life>/_log` and `_snap`, and deletes `state/<life>/_ckpt` with an exact-token delete. `fsck`'s catalog cut can be older than the row's erase. Soundness then depends on the `_ckpt` delete landing first. Today that follows from LIST order (`namespaceStateRootPrefix` sorts before `namespaceStreamRootPrefix` in `Formats/CasLayout.h`), which is not a stated requirement. If the `_ckpt` delete is skipped or unconfirmed while stream keys are deleted, `fsck` reports a false `ChainBroken` for a namespace that is being removed. The janitor's unconfirmed-delete path was not traced end to end.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Trace of the janitor's delete order and its unconfirmed-delete path, with file and function names
- [ ] #2 Either the revalidation also rereads the catalog row and maps a changed or absent row to `Unchecked`, or the janitor's `_ckpt`-first order is a stated and tested requirement
- [ ] #3 A gtest runs `fsck` against a namespace whose stream keys the janitor deleted while `_ckpt` is still present, and sees no `ChainBroken`
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
