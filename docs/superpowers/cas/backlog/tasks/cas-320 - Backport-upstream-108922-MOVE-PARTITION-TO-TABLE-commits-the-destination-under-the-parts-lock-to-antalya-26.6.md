---
id: CAS-320
title: >-
  Backport upstream #108922 (MOVE PARTITION TO TABLE commits the destination
  under the parts lock) to antalya-26.6
status: To Do
assignee: []
created_date: '2026-09-28 15:50'
updated_date: '2026-11-21 08:25'
labels:
  - 'area:upstream'
  - 'touches:upstream-code'
  - 'risk:high'
  - 'complexity:small'
  - 'confidence:solid'
  - 'origin:review'
  - 'upstream:fixed-master'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
milestone: m-5
dependencies: []
references:
  - 'https://github.com/ClickHouse/ClickHouse/pull/108922'
priority: high
ordinal: 399000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
On antalya-26.6 StorageMergeTree::movePartitionToTable inserts the moved parts into the destination as PreActive, releases the destination parts lock for the source-side work and commits later; a merge of two active destination neighbours selected in that window spans the moved part's block (getMaxLevelInBetween refuses only a strictly higher level), and dest_transaction.commit then marks the moved part Outdated ('Tried to commit obsolete part ... covered by'), while the source commits its empty covering parts: the rows are active in neither table, the client sees success. Upstream fixed it in PR https://github.com/ClickHouse/ClickHouse/pull/108922 (commit 5f4c7619667, 2026-06-30): dest_transaction.commit(dest_data_parts_lock) inside the same critical section. antalya-26.6 lacks the commit (verified 2026-09-28 by grep). Found while designing the pending-versions rule (docs/superpowers/specs/2026-09-28-mergetree-uncommitted-version-boundary-design.md rev.6-rev.9); draft with a deterministic test: tmp/cas252/issue_move_partition_to_table_covered_by_merge.md (not filed; it is a backport, not an upstream issue).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 PR #108922 cherry-picked onto antalya-26.6 with the deterministic regression test from the draft (pause after destination preactivation, insert above the moved block, OPTIMIZE, resume; moved rows stay active)
- [ ] #2 Test red before the cherry-pick on antalya-26.6, green after
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:25
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: upstream PR 108922 'Fix move partition intersecting parts' merged 2026-07-03 and is in this 26.8 branch (2135a0d7aa8); `git log --grep` on altinity/antalya-26.6 finds no cherry-pick. Changes: upstream reference added. Would settle it: whether antalya-26.6 carries the fix under a different commit message (check the destination commit in StorageMergeTree::movePartitionToTable on that branch). Upstream: fixed-master (https://github.com/ClickHouse/ClickHouse/pull/108922); already here for antalya-26.8; antalya-26.6 needs a backport.
---
<!-- COMMENTS:END -->
