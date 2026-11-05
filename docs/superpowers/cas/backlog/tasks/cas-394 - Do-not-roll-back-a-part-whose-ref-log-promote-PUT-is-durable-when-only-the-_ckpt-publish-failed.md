---
id: CAS-394
title: >-
  Do not roll back a part whose ref-log promote PUT is durable when only the
  _ckpt publish failed
status: To Do
assignee: []
created_date: '2026-10-15 08:43'
updated_date: '2026-11-05 22:30'
labels:
  - 'area:refs'
dependencies: []
priority: high
ordinal: 492000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Reproduced: when the _ckpt publish fails after the promote ref-log PUT returned Committed, the INSERT gets Code 210 and the part is rolled back in memory, but recovery always replays the chunk. A restart (crash or graceful) before the background cleanup drops the ref (~5 min observed, idle backoff up to 600 s) brings the part back; a client retry duplicates the rows. Options: (C) revert the rolled-back CAS part promptly instead of waiting for the cleanup backoff — shrinks the window, cannot close it during an outage; (D) treat a Committed PUT with a failed frontier publish as success and keep the part — closes it, but reverses the 'frontier before waking a waiter' and 'a stale runtime must not acknowledge' rules; needs the ref-ledger TLA model first (why the rule exists, and that no path drops a committed chunk). The audit docs/superpowers/cas/2026-10-12-late-request-audit/atomicity/w1-reflog.md marks this checkpoint publication AMBIGUOUS-AS-DEFINITE.
Reproduction and analysis: tmp/dupcheck/REPORT.md (CAS base fef96d50e69).
<!-- SECTION:DESCRIPTION:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-10-15: https://github.com/Altinity/ClickHouse/pull/2577 makes the commit error texts honest (durable but not acknowledged / outcome unknown / nothing is durable); it does not fix the duplicate or the dedup loss.

2026-11-05: ReplicatedMergeTree branch of the C10 window confirmed by code (review of PR 2577 by k-morozov). MergeTreeData::Transaction::renameParts makes the CAS promote durable before the Keeper multi (ReplicatedMergeTreeSink::commitPart); when the _ckpt publish throws there, the part never reaches Keeper and StorageReplicatedMergeTree::checkParts moves it to detached/ignored_<part> at the next start. So for ReplicatedMergeTree the window ends in a silently detached part, not a duplicate: no block id in Keeper, a client retry is not deduplicated and lands a second copy only if someone ATTACHes the ignored part. For MergeTree the restart-before-cleanup duplicate stands as reproduced. Any fix here must handle both engines; the texts in PR 2577 say the behaviour per engine.
<!-- SECTION:NOTES:END -->
