---
description: 'Live backlog — the two items left open by the triage of Altinity/ClickHouse#2310 (ATTACH PARTITION FROM stalls on relink confirm): the uncounted residency arm of confirmExactRef, and the empty-file multipart retry in copyS3File. Carries the verdict on the report itself so it is not re-derived.'
sidebar_label: 'Issue #2310'
sidebar_position: 13
slug: /superpowers/cas/backlog/issue-2310
title: 'CAS Backlog — Altinity issue #2310 (ATTACH PARTITION FROM relink stall; closed 2026-09-14)'
doc_type: 'guide'
---

# CAS Backlog — Altinity issue #2310 {#issue-2310}

Part of the [CAS live backlog](/superpowers/cas/backlog). Triage record and the two open items from
[Altinity/ClickHouse#2310](https://github.com/Altinity/ClickHouse/issues/2310) (`ATTACH PARTITION FROM`
onto a `ReplicatedMergeTree` destination stalls on relink confirm; opened 2026-09-03 against package
`26.6.2.20001.altinityantalya` after `alter_attach_partition_cas` part 1 failed in
[clickhouse-regression run 33560831028](https://github.com/Altinity/clickhouse-regression/actions/runs/33560831028);
closed 2026-09-14). The stall is not a new defect: it is
[F11, `relink-confirm-lane-livelock`](/superpowers/cas/backlog/gcs#relink-confirm-lane-livelock), hit
through a second, independent workload on a package that predates the fix.

## Verdict: the reported stall is F11 on a pre-fix package {#verdict}

The failing job logged its receiver errors on 2026-09-01; the fix (`a43e2ef89d3`, rule 3 of
`CasRefLedger::confirmExactRef` refusing only for a queued/in-flight mutation of the asked-about ref,
plus `f4c87d0a6d8` and `b400b8c467e`) landed 2026-09-02, about a day later. On `clickhouse1`,
31541/31541 confirms answered `unknown` and zero `no`, while `ref_catalog` recovery (the report's
alternative explanation) accounts for at most ~1.5% of refusals (468/126/117 S3 errors across three
nodes) — the rest is the pre-fix rule-3 arm the fix removed. Gate: PR #2300 run 10 (head `f377ba3a499`,
carrying `5740d2a2953` = the antalya-26.6 landing of rule-3 scoping and `bf77615fe0a` =
`NO_REPLICA_HAS_PART` classification): `cas_alter_attach_1` and `cas_s3_cache_alter_attach_1` 8/8 on
x86 and aarch64. First fixed release `26.6.4.20001.altinityantalya`. Part 3 of the same suite is a
different mechanism, tracked separately as
[Altinity/ClickHouse#2343](https://github.com/Altinity/ClickHouse/issues/2343) (open). The `unproven`
line count and the `CASRelinkConfirmRefused*` counters were not collected on the closing run (outcome
pass, not a counter pass) — which is why the residency item below stays open.

## Open items {#open-items}

- **[attach-partition-cas-relink-residency] the residency arm of `confirmExactRef` answers `Unknown`
  with no counter and no log line — OPEN, fix built then closed unmerged.** `CasRefLedger.cpp`'s rule
  2 (residency) returns `ConfirmAnswer::Unknown` for a namespace absent from `ref_name_slots` or whose
  slot has no current runtime, bypassing the `refuse` lambda that attributes every other refusal to a
  `CASRelinkConfirmRefused*` event and a `TRACE` line (both arms are one map state: cache-budget
  eviction, catalog-life removal and remount all erase the slot the same way). `alter`-suite load —
  hundreds of short-lived tables against one pool — makes cache eviction likely, so a field `Unknown`
  there is indistinguishable from an unrecovered mount. A fix (`CASRelinkConfirmRefusedTableNotResident`
  plus two gtests) was built and gated (release `CAS*` 2529/2529, ASan 2533/2533, codex review NO
  MAJOR) as [Altinity/ClickHouse#2364](https://github.com/Altinity/ClickHouse/pull/2364), but its
  author closed it unmerged on 2026-09-16 ("low priority, may open a different one later"): neither the
  event nor the tests exist on `cas-gc-rebuild` or `altinity/antalya-26.6` (checked 2026-09-26). Also
  open, tracked as `CAS-170.1` in Backlog.md (migrated from `BACKLOG/gcs.md`): no refusal
  counter's increment has ever been observed reaching `system.events` on a live server, and
  `CASRelinkConfirmRefusedStateLockBusy` is exercised by no test. Verification, once this is picked
  back up: re-run `alter_attach_partition_cas` part 1 on a system-table-exporting build, read the
  `Relink confirm is unproven` line count and
  `SELECT event, value FROM system.events WHERE event LIKE 'CASRelinkConfirmRefused%'
  SETTINGS system_events_show_zero_values = 1` per node (post-fix GCS baseline: 230/230 `yes`, zero
  `unproven`, against 2999 `unproven` in two hours pre-fix).

- **[s3-empty-file-multipart-retry] a failed single-part upload or single-op copy of a 0-byte object
  retries as multipart and throws `LOGICAL_ERROR` — OPEN, not filed upstream.**
  `src/IO/S3/copyS3File.cpp:526` and `:751` call `performMultipartUpload`/`performMultipartUploadCopy`
  on `EntityTooLarge`/`InvalidRequest`/`InvalidArgument`/`AccessDenied` or a GCS-rewrite hint without
  re-checking object size, so a 0-byte object (which always takes the single-shot path) reaches
  `calculatePartSize(0)` and throws "Chosen multipart upload for an empty file. This must not happen"
  (`:328`; the same class exists in `copyAzureBlobStorageFile.cpp:114`). Observed once in run
  33560831028 on `ATTACH PARTITION FROM`; this is generic object-storage code that CAS merely
  exercised, and it is unchanged (verified 2026-09-26) on both `cas-gc-rebuild` and
  `altinity/antalya-26.6`, with no matching issue found on `ClickHouse/ClickHouse`. The ask: decide the
  empty-object behaviour on those two retry paths (a plain `PutObject` of an empty body, or a hard
  error naming the real cause) and file it upstream rather than fixing it under a CAS commit.

## What the report gets wrong, recorded so it is not re-derived {#report-corrections}

- **The error code.** The report cites `Code: 210 NETWORK_ERROR`; the current build throws
  `NO_REPLICA_HAS_PART` instead (`DataPartsExchange.cpp`), which both queue executors demote to INFO
  without a stack trace — itself a fix, made after a multi-hour false triage chased the network label
  (Altinity/ClickHouse#2219). Anything reading for `210` will not match a current build.
- **The source map.** Its line numbers are from an 2026-08-05 clone; rule 3 has been rewritten since —
  do not navigate the current source by those offsets.
- **The soundness argument for its own fix.** The report justifies byte-fetching on `Unknown` by "gate
  0 already proved the part is `Active`/`Outdated`". Gate 0 is an availability filter, not a proof
  (`rollbackDeletingParts` restores an `Outdated` part without updating the in-memory path that a
  `delete_tmp_*` rename left stale). The conclusion may survive; this argument for it does not.

## On the report's fix: `Unknown` is not `No` {#byte-fallback-note}

The report proposes splitting `No` from `Unknown` on the wire and byte-fetching on `Unknown` — option 5
of the F11 design, already rejected (see `CAS-170` in Backlog.md, migrated from `BACKLOG/gcs.md`), and it stays
rejected. The recorded reason there ("the byte request goes to the source whose state is in doubt") is
not the reason that actually holds:

- **Safety is not the objection.** A byte fetch onto a CA disk establishes its own GC protection
  through `PartWriteTxn`'s durable write order (`stageManifest` → `precommitAdd` → `putBlob` →
  `promote`, `CasPartWriteTxn.h:135`) and does not rest on the sender's ledger at all. Relink is the
  mechanism that lacks that independent protection and therefore needs the confirm; bytes do not. The
  recursion brake (`allow_ca_relink=false` on the re-request) also bounds the fallback to one attempt
  per fetch.
- **Cost is the objection.** Relink exists to move a part without moving its bytes; the failing job
  abandoned ~170k relinks, and a byte fallback on every one converges by paying the whole dataset in
  transfer, repeatedly — trading a bounded stall for unbounded traffic.
- **If it is ever needed, this is the shape.** Not "`Unknown` → always bytes" but a bounded escape: N
  consecutive unproven confirms for the same part from the same source → one byte fetch, `No` still
  throwing retry-later. Its design must argue safety from the `putBlob` contract above, not from gate
  0.

The cheaper path remains removing the causes of `Unknown` rather than building an escape from it. Rule
3 was the dominant cause and is gone; the residency arm above is the next one that cannot even be seen.
