---
id: CAS-347
title: 'Precommit-and-stay-open for temporary part names: one ledger append per part'
status: To Do
assignee: []
created_date: '2026-10-07 21:05'
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
