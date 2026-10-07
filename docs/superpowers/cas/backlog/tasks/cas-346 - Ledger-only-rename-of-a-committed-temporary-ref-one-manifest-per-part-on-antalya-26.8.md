---
id: CAS-346
title: >-
  Ledger-only rename of a committed temporary ref: one manifest per part on
  antalya-26.8
status: To Do
assignee: []
created_date: '2026-10-07 21:05'
labels:
  - 'area:ref-ledger'
  - 'area:write-path'
  - 'area:gc'
dependencies: []
priority: medium
type: enhancement
ordinal: 440000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Upstream PR ClickHouse/ClickHouse#89658 (26.8) commits the part's disk transaction right after finalize(), before the tmp_* -> final rename (MergeTreeSink.cpp:313, ReplicatedMergeTreeSink.cpp:477, MergePlainMergeTreeTask.cpp:157, MutatePlainMergeTreeTask.cpp:123, MergeFromLogEntryTask.cpp:418, MutateFromLogEntryTask.cpp:306, StorageReplicatedMergeTree.cpp createEmptyPartInsteadOfLost). On CAS the rename of a committed ref is republishRef: a second manifest, precommit, promote and ref_drop(tmp). Measured on the port of PR Altinity/ClickHouse#2461: one INSERT = 2 manifest_put / 2 precommit / 2 build_publish, CASRootPut 5 (26.6: 1/1/1, CASRootPut 2). The port currently works around it by skipping the early commit on content-addressed storage (spec docs/superpowers/specs/2026-10-07-cas-antalya-26.8-port-regressions-design.md, section 3), which puts the CAS commit back under data_parts_mutex for non-replicated INSERT and mutations.

Proposal: the manifest body does not contain the part name (PartManifest.ref is ManifestRef = epoch/build/ord; the name lives only in the ref table), so a rename needs no new manifest. Make the rename of a committed tmp_* ref one ref-log transaction over the same manifest M: RemoveCommitted(tmp, M) + AddPrecommit(final, M) + Promote(final, M). All three are legal owner_transition shapes (Pool/CasRefProtocol.cpp classifyOwnerTransitionShape); applied in order, RemoveCommitted frees the manifest so AddPrecommit under the new name passes manifestAlreadyOwned. The two-name transaction needs MutationScope::wholeShard (CasPartWriteTxn.cpp:860, CasRefLedger.cpp:3133) and must keep SetPublishedAt in the same append. Expected: 1 manifest_put per part, 2 ledger appends, upstream files untouched, the early-commit workaround can be dropped.

Prerequisite (real defect today for any -1/+1 pair on one manifest): Gc::foldManifestEdges (Gc/CasGc.cpp:1347) nominates the manifest into mf_cleanup on a -1 edge and a later +1 edge does not un-nominate it; the manifest-delete phase then deletes a body that has a committed owner again. Fix: erase the nomination on +1 (or skip bodies whose folded in-degree is positive) and add a gtest that runs a real GC round through the delete phase after such a transaction, both in one round and with the publish and the rename folded in separate rounds, then re-reads the part and runs FSCK.

Oracles: system.cas_log per isolated disk after SYSTEM FLUSH LOGS: one INSERT -> manifest_put = 1, build_publish = 1, ref_drop = 0 for tmp_* names; ProfileEvents CASRootPut from system.query_log for a one-part INSERT on a warm namespace.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Rename of a committed tmp_* ref on CAS emits no manifest_put: one INSERT / merge / mutation = exactly one manifest_put on an isolated disk
- [ ] #2 GC keeps the manifest body after RemoveCommitted/AddPrecommit/Promote over the same manifest, verified by a gtest that runs the round through the manifest-delete phase (same round and separate rounds)
- [ ] #3 The content-addressed skip of the early commitTransaction() in the eight upstream writer sites is removed and 05014_insert_dedup_disk_commit_failpoint still passes
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
