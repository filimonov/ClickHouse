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
[Altinity/ClickHouse#2310](https://github.com/Altinity/ClickHouse/issues/2310), opened 2026-09-03
against package `26.6.2.20001.altinityantalya` after `alter_attach_partition_cas` part 1 failed in
[clickhouse-regression run 33560831028](https://github.com/Altinity/clickhouse-regression/actions/runs/33560831028).

The reported symptom — `ATTACH PARTITION FROM` onto a `ReplicatedMergeTree` destination, followers
fetch by relink, the source answers the confirm `unproven (unknown)` forever, the receiver throws
instead of falling back, replicas permanently miss rows — is **not a new defect**. It is
[F11, `relink-confirm-lane-livelock`](/superpowers/cas/backlog/gcs#relink-confirm-lane-livelock)
observed on a package that predates the fix, through a second and independent workload. This file
exists because the triage nevertheless turned up two things that are open, and because the report
argues a position about the failure taxonomy that is worth answering once rather than every time it
is raised again.

## Verdict: the reported stall is F11 on a pre-fix package {#verdict}

The failing job logged its receiver errors at `2026.09.01 23:28:20`. The fix — `a43e2ef89d3`, rule 3
of `CasRefLedger::confirmExactRef` refusing only for a queued or in-flight mutation that names the
asked-about ref, read from the `MutationScope` every lane item already carries — landed
`2026-09-02 21:24`, with `f4c87d0a6d8` (refusal attribution) and `b400b8c467e` (whole-shard arm
tests) behind it. The tested package is about a day older than the fix.

The report's own counts are the strongest evidence that the pre-fix rule 3 is what it hit: on
`clickhouse1`, 31541 of 31541 confirms answered `unknown` and **zero** answered `no`, while
`ref_catalog` S3 errors — the report's alternative explanation, a recovering mount — number 468, 126
and 117 across the three nodes. Recovery accounts for at most a percent and a half of the refusals;
the rest is the lane arm the fix removed. Note also that the report's own suggestion (C), second
bullet — stop answering `Unknown` merely because another ref's append is queued — is what shipped;
the analyst reached the same design independently.

An attach is one disk transaction per part, so each attached part's ref is published by its own
`MutationScope::ref` append. Under the shipped rule 3 a confirm about part `P` is refused only while
`P`'s own append is queued or carved, which is a window the replication log entry's ordering mostly
excludes — not the table-wide, continuously-occupied window that starved the job.

**Gate passed, issue closed 2026-09-14.** The re-run happened on PR #2300 run 10 (head `f377ba3a499`,
which carries `5740d2a2953` = the antalya-26.6 landing of rule-3 scoping, and `bf77615fe0a` = the
`NO_REPLICA_HAS_PART` classification): `cas_alter_attach_1` and `cas_s3_cache_alter_attach_1` passed on
x86 and aarch64 in both attempts (8/8). Part 3 of the same suite is a different mechanism
(store-timeout policy deadline plus common-pool `num_tries` inflation) and is tracked as
Altinity/ClickHouse#2343. First release with the fix: `26.6.4.20001.altinityantalya`. The
`unproven` line count and the `CASRelinkConfirmRefused*` readout asked for below were NOT collected
on that run (the regression job does not export system tables); the pass is by outcome, not by
counters — which is exactly why the residency-arm item below stays open.

## Open items {#open-items}

- **[attach-partition-cas-relink-residency] the residency arm of `confirmExactRef` answers `Unknown`
  with no counter and no log line, and it is the arm this workload is most likely to hit** — FIX IN
  REVIEW — https://github.com/Altinity/ClickHouse/pull/2364 (opened 2026-09-15), branch `fix/antalya-26.6/relink-confirm-residency-attribution`
  (dffc6acc563 + c4326ae6b1c on antalya-26.6 39cad60b7f3): one event
  `CASRelinkConfirmRefusedTableNotResident` for both arms (the budget evictor, catalog-life removal and
  remount all ERASE the slot, so cold and evicted are one map state; the `!current` arm is a guard for a
  state nothing produces today), both arms through `refuse`, the two residency gtests assert the delta.
  Gates: release `CAS*` 2529/2529, ASan 2533/2533; codex review NO MAJOR. Original ask kept below for
  the record. — `CasRefLedger.cpp:473-477` returns `ConfirmAnswer::Unknown` for a namespace that is absent
  from `ref_name_slots` or whose slot holds no current runtime, and both arms return *before* the
  `refuse` lambda (`:487`) that attributes every other refusal to one of the five
  `CASRelinkConfirmRefused*` events and a TRACE line. The header comment argues the omission
  deliberately — a cold or budget-evicted table is cache behaviour, not lane, mount or load — and
  that reasoning holds for what the counters are *for*; it does not hold for triage, because the
  only surviving trace of such a refusal is `answerContentAddressedConfirm`'s DEBUG line, which says
  `unknown` and cannot say why. `enforceRefTableCacheBudget` evicts idle runtimes by byte budget, and
  the `alter` suite's profile — hundreds of short-lived tables against one pool — is precisely the
  one that makes eviction likely, so if the post-fix re-run still shows `unknown` answers, today's
  instrumentation cannot distinguish "evicted from cache" from "unrecovered mount" without a new
  build. The ask: give the two arms an attribution (a sixth event, or a `TRACE` line naming the
  namespace, or both) so a refusal in the field is always answerable from logs and
  `system.events`. Related and already recorded in
  [`BACKLOG/gcs.md`](/superpowers/cas/backlog/gcs#relink-confirm-lane-livelock): no refusal
  counter's increment has been observed reaching `system.events` on a live server, and
  `CASRelinkConfirmRefusedStateLockBusy` is exercised by no test.
  **Verification gate (the issue-closing half is done, see the verdict above; the counter readout
  half is still owed):** on the next run that exports system tables, or on a local stand, re-run
  `alter_attach_partition_cas` part 1 against a post-`5740d2a2953` build. Read it
  through the count of `Relink confirm is unproven` lines (the answering peer logs one for every
  non-`Yes`, including the uncounted arms) and through
  `SELECT event, value FROM system.events WHERE event LIKE 'CASRelinkConfirmRefused%'
  SETTINGS system_events_show_zero_values = 1` on each node. The equivalent measurement on the GCS
  stand after the fix was 230 confirms, all `yes`, zero `unproven`, against 2999 `unproven` in two
  hours on the pre-fix binary.

- **[s3-empty-file-multipart-retry] a failed single-part upload or single-operation copy of a 0-byte
  object retries as multipart and throws `LOGICAL_ERROR`** — HARD — `src/IO/S3/copyS3File.cpp:526`
  (single-part upload retry) and `:751` (single-operation copy retry) call
  `performMultipartUpload`/`performMultipartUploadCopy` without re-checking the object size, so a
  zero-length object — which always takes the single path, both size predicates admitting it — lands
  in `calculatePartSize(0)` and throws `Chosen multipart upload for an empty file. This must not
  happen` (`:328`). Reachable whenever the backend answers the single-shot request with
  `EntityTooLarge`, `InvalidRequest`, `InvalidArgument`, `AccessDenied` or a GCS-rewrite hint;
  observed once in run 33560831028 on `ALTER TABLE … ATTACH PARTITION … FROM`. Not the replica
  divergence mechanism and not CAS-specific: this is generic object-storage code that CAS merely
  exercised. The ask: decide the empty-object behaviour on those two retry paths (a 0-byte object has
  no multipart form, so the retry is either a plain `PutObject` of an empty body or a hard error that
  names the real cause) and file it upstream rather than fixing it silently under a CAS commit.

## What the report gets wrong, recorded so it is not re-derived {#report-corrections}

- **The error code.** The report describes `Code: 210 NETWORK_ERROR`. Taxonomy row 3 now throws
  `NO_REPLICA_HAS_PART` (`DataPartsExchange.cpp:1551`), which both queue executors demote to INFO
  without a stack trace — the label change is itself a fix, made after a multi-hour false triage
  chased the network label (issue #2219). Anything reading for `210` will not match a current build.
- **The source map.** Its line numbers come from a clone of 2026-08-05. Rule 3 has been rewritten
  since; do not navigate by those offsets.
- **The soundness argument for its own fix.** The report justifies byte-fetching on `Unknown` by
  "gate 0 already proved the part is `Active`/`Outdated`". Gate 0 explicitly disclaims being a proof
  (`DataPartsExchange.cpp:242`: an availability filter, demoted in rev.5, because
  `rollbackDeletingParts` restores an `Outdated` part and the in-memory path is not updated by a
  `delete_tmp_*` rename). The conclusion may survive; this argument for it does not.

## On the report's fix: `Unknown` is not `No`, and the standing rejection is thinner than it reads {#byte-fallback-note}

The report proposes splitting `No` from `Unknown` on the wire and byte-fetching on `Unknown`. That is
option 5 of the F11 design, already rejected, and it stays rejected — but the recorded reason ("the
byte request goes to the very source whose state is in doubt") is not the reason that actually holds,
and leaving it as the file's only answer invites the proposal back every time somebody reads
taxonomy row 3.

What actually holds:

- **Safety is not the objection.** An ordinary byte fetch onto a CA disk establishes its own
  protection and does not rest on the sender's ledger at all: `PartWriteTxn`'s durable write order is
  `stageManifest` → `precommitAdd` → `putBlob` → `promote`, and the precommit edge must be durable
  *before* any existing blob incarnation is adopted, precisely because that edge is what protects the
  incarnation from GC while the build is in flight (`CasPartWriteTxn.h:135`). Relink is the mechanism
  that lacks an independent protection and therefore needs the T1 &lt; T2 confirm; bytes do not. The
  recursion brake (`allow_ca_relink=false` on the re-request) also bounds the fallback to one attempt
  per fetch.
- **Cost is the objection.** Relink exists to move a part without moving its bytes. The failing job
  abandoned about 170k relinks; a byte fallback on every one of them converges by paying the entire
  dataset in transfer, repeatedly. Trading a bounded stall for unbounded traffic is not obviously the
  better failure.
- **If it is ever needed, this is the shape.** Not "`Unknown` → always bytes", but a bounded escape:
  N consecutive unproven confirms for the same part from the same source → one byte fetch, with `No`
  still throwing retry-later (a `No` from gate 0 means the sender cannot stream the part either, so
  bytes would only fail differently). That is options 3 and 5 combined, and its design must argue
  safety from the `putBlob` contract above rather than from gate 0.

The cheaper path remains the one already taken: remove the causes of `Unknown` rather than build an
escape from it. Rule 3 was the dominant cause and is gone; the residency arm above is the next one
that cannot even be seen.
