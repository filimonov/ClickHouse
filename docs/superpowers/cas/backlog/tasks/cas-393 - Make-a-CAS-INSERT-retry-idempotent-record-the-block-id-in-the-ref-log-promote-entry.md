---
id: CAS-393
title: >-
  Make a CAS INSERT retry idempotent: record the block id in the ref-log promote
  entry
status: To Do
assignee: []
created_date: '2026-10-15 08:43'
updated_date: '2026-11-04 00:34'
labels:
  - 'area:refs'
dependencies: []
priority: high
ordinal: 491000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
A CAS INSERT whose ref-log commit has an unknown outcome (PUT without a definite answer) can only answer the client with an error; a client retry then duplicates the rows if the write landed. non_replicated_deduplication_window does not help: the local dedup log is written before the CAS commit and is not tied to its outcome, so a failed write leaves its block id and the retry is skipped (acknowledged loss), while dropping the id on failure turns a landed write into a duplicate.

Design direction: store the insert block id (or insert_deduplication_token) as an optional field of the ref-log promote entry, so it is durable exactly when the insert is. Recovery replays entries and rebuilds the window of recent block ids; the window moves into the snapshot/_ckpt so compaction does not lose it; N = non_replicated_deduplication_window. On a retry: run recovery first (resolves the unknown outcome), then check the block id: present -> success without insert, absent -> insert. For CAS tables the local dedup log becomes a cache or is disabled. Not in the part manifest: parts are content-addressed and outlive the insert (relink, merge).

Open questions: does the 26.6.4 ref-log parser ignore unknown keys (format frozen; see CAS-328; downgrade must keep working); async inserts; ReplicatedMergeTree on CAS (Keeper already dedups).
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

2026-11-04: the #2577 audit (https://github.com/Altinity/ClickHouse/pull/2577, alsugiliazova 2026-10-23, Medium) names the loss case: with non_replicated_deduplication_window > 0, MergeTreeSink::commitPart records the block id before the CAS commit and keeps it when the commit throws, so a client retry is reported as a duplicate and writes nothing, also when nothing was durable. Owner decision: not fixed in #2577; covered by this task (block id written in the same ref-log transaction).
<!-- SECTION:NOTES:END -->
