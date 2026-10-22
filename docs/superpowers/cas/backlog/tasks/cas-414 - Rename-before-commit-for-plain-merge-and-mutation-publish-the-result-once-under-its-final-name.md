---
id: CAS-414
title: >-
  Rename before commit for plain merge and mutation: publish the result once
  under its final name
status: To Do
assignee: []
created_date: '2026-10-22 20:18'
labels:
  - 'area:write-path'
  - 'complexity:small'
  - 'risk:medium'
  - 'touches:upstream-code'
  - 'confidence:solid'
  - 'needs:decision'
  - 'origin:review'
milestone: m-4
dependencies:
  - CAS-346
priority: medium
type: design
ordinal: 517000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
On `feature/cas-disk-transactions` (antalya-26.8) a replicated merge already publishes once: `MergeFromLogEntryTask::finalize` calls `renameMergedTemporaryPart` (which runs `Transaction::renameParts` inside the still-open storage transaction, a staged re-key on CAS) and only then `commitTransaction` — one manifest PUT under the final name, no `Move`, outside `DataPartsLock`. Plain merge (`MergePlainMergeTreeTask.cpp:157-161`) and both mutation tasks commit first and rename afterwards, so each pays an extra `Move` append.

Decide whether to move the storage commit after `renameMergedTemporaryPart`/`renameParts` for plain merge and mutations too (-1 ref-log append per merge/mutation, no change to the upstream writers beyond reordering two lines per task), or to keep the upstream ordering. INSERT sinks are out of scope: their `renameParts` sits after the early commit of #89658 and the fork guards that delayed it were removed on purpose. Measured on the stand 2026-10-22: replicated `OPTIMIZE` = `build_publish all_0_1_1`, `manifest_put 1`, `ref_move 0`; plain `OPTIMIZE` (05388) = `manifest_put 1`, `ref_move 1`.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Decision recorded here with the measured append counts for plain merge, mutation and replicated merge
- [ ] #2 If adopted: 05388 `optimize`/`mutate` oracles show `ref_move 0`; `PartsLockHoldMicroseconds` on INSERT/merge unchanged
- [ ] #3 Crash between the staged rename and the commit leaves no part and no ref (nothing durable exists yet); covered by a test if the order changes
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
