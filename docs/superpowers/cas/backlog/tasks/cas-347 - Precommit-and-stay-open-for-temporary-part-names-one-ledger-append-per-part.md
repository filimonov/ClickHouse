---
id: CAS-347
title: 'Precommit-and-stay-open for temporary part names: one ledger append per part'
status: To Do
assignee: []
created_date: '2026-10-07 21:05'
updated_date: '2026-11-14 10:18'
labels:
  - 'area:ref-ledger'
  - 'area:write-path'
  - 'area:testing'
dependencies:
  - CAS-346
priority: low
type: enhancement
ordinal: 441000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Follow-up to CAS-346. Even with a ledger-only rename, a part on antalya-26.8 costs two ledger appends (publish under tmp_*, then transfer to the final name) and briefly exists as a committed tmp_* ref. Design that reaches one append per part without touching the upstream writers:

- For a part whose directory name is temporary (tmp_* family, tmp-fetch_*; not detached/..., moving/..., *.tmp_proj), ContentAddressedTransaction::commit performs the heavy work only: upload blobs, stage manifest M, append AddPrecommit(tmp, M) (durable +1, GC-safe), and KEEPS the storage transaction open. No parked-handle registry: B59 read-your-writes keeps serving reads of the part (restore and clone read the temporary part before the rename through the open transaction), and hasActiveTransaction() stays true.
- renameTo(tmp -> final) stays inside the open transaction as on 26.6. The fork's Transaction::renameParts and the commit loop in Transaction::commit then call commitTransaction() under the final name, which is the single append RemovePrecommit(tmp, M) + AddPrecommit(final, M) + Promote(final, M) (MutationScope::wholeShard, SetPublishedAt in the same append). The if (hasActiveTransaction()) guard in MergeFromLogEntryTask works as intended.
- Destroying the transaction (dedup conflict in the sink, failed merge, removeRecursive(tmp)) aborts the precommit; after a crash the stale-precommit sweep reclaims it.
- Manifest contract: one manifest per part build; every standalone write into a precommitted part (txn_version.txt for MVCC, metadata_version.txt in clone) re-stages: RemovePrecommit(tmp, M1) + AddPrecommit(tmp, M2); M1 goes to GC. The MVCC INSERT oracle is therefore 2 manifest_put, not 1.
- Promote prepares everything outside data_parts_mutex; only the append stays under the lock (today promote also does a manifest GET + decode: CasPartWriteTxn.cpp:824, :911 - the staged body is already in memory).
- Relink (DataPartsExchange confirm -> promote of tmp-fetch_*) is a separate publication entry point and stays out of this scheme.
- Durability gate: 05014_insert_dedup_disk_commit_failpoint keeps the pre-Keeper assertion; add a second failpoint located after the durable precommit to test crash + sweep + GC reclamation with explicit sweep trigger and completion.

Depends on the GC nomination fix from CAS-346. Source: codex review of docs/superpowers/specs/2026-10-07-cas-antalya-26.8-port-regressions-design.md (tmp/pr2461/codex_review_report.md, findings 1-3, 9-16).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 One INSERT on CAS = one manifest_put and one ledger append; no committed tmp_* ref exists at any point, verified by a ref-table state test rather than by event absence
- [ ] #2 Restore and clone read a precommitted temporary part before the rename
- [ ] #3 Crash between precommit and rename: sweep reclaims the precommit, GC reclaims the blobs, no tmp_* ref or body remains
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
2026-10-22, on `feature/cas-disk-transactions` @ 7a16106160c (19 commits on PR Altinity/ClickHouse#2579 head e612f2f6c94, antalya-26.8): measured one INSERT on local object storage = 3 ref-log flushes (`precommit`, `publish`, `Move`; `CASRootPut` 3, `CASRequestAttempt` 21, 1 manifest PUT). The rename is now one `Move` append (CAS-346 done on the branch); this task's remaining target is the publish+Move pair -> one append.

2026-11-13: feasibility memo master/tmp/prbundle/wave2/promote-move-memo.md (code read at cas-pr1-clean 802077e5c1a, nothing run). Owner's idea: do the commit and the rename of a new part in ONE ref-log append. Verdict: feasible WITHOUT a format change as one append `[Promote(tmp), Move(tmp -> final), SetPublishedAt(final)]`: `RefTableState::applyTxnInPlace` applies ops in array order (`CA/Pool/CasRefProtocol.cpp:567-568`), the decoder has no cross-op rule, and a 26.8-tip node folds it (the ref protocol and log format files are byte-identical between the 26.8 tip and this head). Counts per new part (no blobs, warm namespace; one append = catalog GET, `_log` PUT, `_ckpt` GET + PUT): 26.6 every path 2 appends / 10 requests; 26.8 plain and replicated INSERT, plain merge, plain and replicated mutation 3 appends / 14 requests; 26.8 replicated merge already 2 appends / 10 (`MergeFromLogEntryTask.cpp:415` renames before `:416` commits). Saving: 4 requests per part on five of six paths. Cost: a process-local 'sealed' part state (uploaded and precommitted, not promoted) between `commitTransaction` and the rename, estimated 400-600 product lines in 5-6 CA files and 600-900 test lines; differences from a committed temp ref: a self-remount's stale-precommit sweep removes it (retryable INSERT error), and a leaked handle pins `min_active_build_sequence`. Variant W: for plain merges rename before commit as the replicated merge does (swap `MergePlainMergeTreeTask.cpp:157` and `:160`, an upstream file): 4 requests saved on that path alone. The older formula in cas-347 (`RemovePrecommit + AddPrecommit + Promote`) puts a -1/+1 edge pair on one manifest; `Promote + Move` emits no edge. Not established: whether 4 requests per part are material (the local A/B measures it), and readers of the temp part between commit and rename on createEmptyParts, MOVE/REPLACE PARTITION, part moves, zero-copy, text-index scratch refs, deduplicated-insert removal.

2026-11-14, owner decision: not built in this form (deviation from the upstream writer order). See CAS-485 and docs/superpowers/reports/2026-11-14-cas-promote-move-one-append-feasibility.md. If reopened: replace the op formula `RemovePrecommit(tmp) + AddPrecommit(final) + Promote(final)` with `[Promote(tmp), Move(tmp -> final), SetPublishedAt(final)]`. The pair emits no manifest edge, so the dependency on the GC un-nomination fix goes away. Prefer wiring A of the report (park the build in the disk layer, promote on first observation): it edits no upstream file. Wiring B (keep the storage transaction open) edits three upstream sites.
<!-- SECTION:NOTES:END -->
