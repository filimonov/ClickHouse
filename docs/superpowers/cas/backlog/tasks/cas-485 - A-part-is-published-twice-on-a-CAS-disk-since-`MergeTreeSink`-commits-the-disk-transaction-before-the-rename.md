---
id: CAS-485
title: >-
  A part is published twice on a CAS disk since `MergeTreeSink` commits the disk
  transaction before the rename
status: To Do
assignee: []
created_date: '2026-11-13 21:17'
updated_date: '2026-11-14 10:18'
labels:
  - 'area:write-path'
  - 'area:ci'
  - 'origin:ci'
  - 'theme:ci-stabilization'
dependencies: []
priority: medium
ordinal: 590000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Since commit 20ca22bd34f ('add missing disk transactions commits', in antalya-26.8, not in antalya-26.6) `MergeTreeSink` commits the part's disk transaction right after `finalize` and then renames the part. On a CAS disk that publishes each part twice: once under the temporary name and once as the rename `Move` (see also d233c70cb5d, 53425e906a7). On 26.6 the part was published once, at the rename. Measured on the ASan cas-s3 stateless lane, shard 1/2 of PR 2812 (26.6) against PR 2799 (26.8): insert phase 'Wrote block -> Allocated block number' median 0.009 s -> 0.153 s, 'rename -> end' 0.125 s -> 0.054 s, net +0.07 s median / +0.14 s mean per insert; merges show the same shape (sorted -> rename 0.023 -> 0.166 s). Worth 9-17 % of the same-test slowdown of that lane between the branches (1.4-2.8 min of 16.2 min per shard) and extra S3 requests on every insert and merge in production. Evidence: master/tmp/lanes/ci/perf_26_6_vs_26_8/followup/ (out_a15_insert_phases.txt, out_a18_merge_phases.txt) and master/tmp/lanes/ci/perf_26_6_vs_26_8/REPORT.md. Not established: the exact request count per publish on each branch (no blob_storage_log dump exists for a 26.6 lane); whether the first publish can be skipped or folded into the rename without losing what 20ca22bd34f fixed. Related open finding from the same analysis: DROP TABLE x1.77, SELECT x1.16, CREATE TABLE x1.35 on 26.8 vs 26.6 in the CAS lane, causes unknown (need one 26.6 run that uploads blob_storage_log and cas_log).
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
2026-11-13: full follow-up report saved at master/tmp/lanes/ci/perf_26_6_vs_26_8/followup/REPORT.md. Extra lead from it: PUT creations per executed test x1.45 on 26.8, with the growth in merges of system log tables (system.* merge PUTs 97293 -> 256779, thread time x2.5, asynchronous_metric_log 5.8 -> 11.1 s per merge) while requests per foreground statement did not grow. Not priced. Settling runs listed in the report.

2026-11-13 CORRECTION (owner's remark): this is not a defect and 'published twice' is a misnomer. The 26.8 disk-transactions series is built on it: upstream 20ca22bd34f commits the part's disk transaction before the rename, and on CAS the rename of a committed part is one ref-log `Move` (d233c70cb5d, 53425e906a7, generation 2). The manifest and blobs are written once; what 26.8 adds per new part is one more ref-log transaction (the `Move`, with its `_log` and `_ckpt` writes). Removing it would mean revisiting the owner-move protocol. Treat this task as 'measure the price of the extra ref-log transaction per insert, merge and mutation on 26.8' (local A/B: master/tmp/lanes/ci/perf_26_6_vs_26_8/ab/); only if the measured price is material, consider making the `Move` cheaper inside the protocol. The 9-17 % figure above is an inference from log phase timings, not a measurement.

2026-11-13: feasibility memo master/tmp/prbundle/wave2/promote-move-memo.md (code read at cas-pr1-clean 802077e5c1a, nothing run). Owner's idea: do the commit and the rename of a new part in ONE ref-log append. Verdict: feasible WITHOUT a format change as one append `[Promote(tmp), Move(tmp -> final), SetPublishedAt(final)]`: `RefTableState::applyTxnInPlace` applies ops in array order (`CA/Pool/CasRefProtocol.cpp:567-568`), the decoder has no cross-op rule, and a 26.8-tip node folds it (the ref protocol and log format files are byte-identical between the 26.8 tip and this head). Counts per new part (no blobs, warm namespace; one append = catalog GET, `_log` PUT, `_ckpt` GET + PUT): 26.6 every path 2 appends / 10 requests; 26.8 plain and replicated INSERT, plain merge, plain and replicated mutation 3 appends / 14 requests; 26.8 replicated merge already 2 appends / 10 (`MergeFromLogEntryTask.cpp:415` renames before `:416` commits). Saving: 4 requests per part on five of six paths. Cost: a process-local 'sealed' part state (uploaded and precommitted, not promoted) between `commitTransaction` and the rename, estimated 400-600 product lines in 5-6 CA files and 600-900 test lines; differences from a committed temp ref: a self-remount's stale-precommit sweep removes it (retryable INSERT error), and a leaked handle pins `min_active_build_sequence`. Variant W: for plain merges rename before commit as the replicated merge does (swap `MergePlainMergeTreeTask.cpp:157` and `:160`, an upstream file): 4 requests saved on that path alone. The older formula in cas-347 (`RemovePrecommit + AddPrecommit + Promote`) puts a -1/+1 edge pair on one manifest; `Promote + Move` emits no edge. Not established: whether 4 requests per part are material (the local A/B measures it), and readers of the temp part between commit and rename on createEmptyParts, MOVE/REPLACE PARTITION, part moves, zero-copy, text-index scratch refs, deduplicated-insert removal.

2026-11-14 MEASURED (local A/B, master/tmp/lanes/ci/perf_26_6_vs_26_8/ab/RESULTS.md; release CI binaries 26.6.8.20003 of PR 2812 and 26.8.34.20001 of PR 2799, RustFS 1.0.0-rc.3, idle-server probes of 7 reps, store-side request log cross-checked with ProfileEvents in 80 of 84 probes): one INSERT costs 21 S3 requests on 26.6 and 25 on 26.8: PUT `_log` 2 -> 3, PUT `_ckpt` 2 -> 3, GET `ref_catalog` 2 -> 3, GET `_ckpt` 3 -> 4; `CASRefMove` 0 -> 1, `CASRefBatchFlushes` 2 -> 3. The same +4 on the second INSERT (20 -> 24), an OPTIMIZE merge (37 -> 41), a mutation (34 -> 38); CREATE, SELECT, ADD COLUMN, DROP unchanged (20/20, 3/3, 31/31 SYNC, 21/21 plain). Client wall time: INSERT 11.2 -> 15.0 ms (x1.34), second INSERT 10.9 -> 16.3 ms (x1.50), OPTIMIZE x1.16; server `query_duration_ms` INSERT 18 -> 24. The S3 span of the extra transaction is 1.9 ms; the rest of about 6 ms is a longer wait in the ref batch queue (`CASRefQueueWaitMicroseconds` 8.5 -> 15.5 ms). Refuted as causes of the 26.6 -> 26.8 gap: system log tables on CAS (3-8 % of foreground time on BOTH versions, system-log PUTs per second +4 %, the CI jump 97k -> 257k does not reproduce), packed parts and the second disk (CREATE +1.1 ms, 4 extra GETs of the second disk's catalog; DROP SYNC +0.5 ms). Does NOT reproduce locally on release binaries: the CI slowdowns of CREATE (1.35), SELECT (1.16), DROP (1.77): equal request counts on both versions, local ratios 1.00-1.05, so no CAS request-count change explains them; they remain unexplained (candidates not measured: sanitizer cost, lane concurrency, in-run drift).

2026-11-14, owner decision: promote and `Move` in one append is NOT built in this form. Reason: it moves the point where a part becomes a committed ref away from the upstream writer order, another deviation from the upstream protocol. Variant W (swap `MergePlainMergeTreeTask.cpp:157` and `:160`) is not built either: it edits an upstream file. Direction chosen instead: fewer store requests and a shorter serialized section per append, CAS-487.

Study on record: docs/superpowers/reports/2026-11-14-cas-promote-move-one-append-feasibility.md. Findings:
- Variant X, one append `[Promote(tmp), Move(tmp -> final), SetPublishedAt(final)]`: legal without a format change for encoder, decoder, state machine (`applyTxnInPlace` applies ops in order, `CasRefProtocol.cpp:567-568`), recovery, GC fold and orphan sweep; also on the 26.8 tip (protocol files byte-identical). From reading; no round trip was run.
- Saving: 4 requests per part (14 -> 10 without blobs) on five of six writer paths. A replicated merge already renames before the commit and costs two appends.
- Cost: a process-local sealed-part state between `commitTransaction` and the rename. 400-600 product lines in 5-6 CAS files, 600-900 test lines. Clone, fetch by bytes and restore read the part under its temporary name and would still need three appends.
- New exposure: a self-remount between seal and rename fails the INSERT (retryable); a leaked sealed handle pins `min_active_build_sequence` until restart. No path to silent data loss found.
- Rejected variants: Z (new op, generation 3, same saving); Y (skip one `_ckpt` rewrite, acknowledges above the frontier).
- Per-path request table for 26.6 and 26.8 with sites is in the report, section 1.
- Prose defect found: the comment of `05396_cas_root_put_per_insert` and the message of `ee662a8d383` name the three root PUTs as manifest, publish, move. They are precommit, promote and `Move`; a manifest PUT counts as `CASManifestPut`.
- Not reconciled: 21 `CASRequestAttempt` per INSERT on record against the 14 counted plus blob requests.
<!-- SECTION:NOTES:END -->
