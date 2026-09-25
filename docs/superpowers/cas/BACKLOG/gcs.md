---
description: 'Live backlog — CAS on Google Cloud Storage: the release gate, the relink-confirm liveness
  fix (F11) and its closing gate, the hot-control-key mutation limit (F3/F12) and its remaining fix (A1),
  and the harness/observability follow-ups. B1/B2 and the single-attempt-write audit are closed.'
sidebar_label: 'GCS'
sidebar_position: 12
slug: /superpowers/cas/backlog/gcs
title: 'CAS Backlog — Google Cloud Storage'
doc_type: 'guide'
---

# CAS Backlog — Google Cloud Storage {#gcs}

Part of the [CAS live backlog](/superpowers/cas/backlog). Topic file for everything specific to running
`CAS` on GCS. The run record with timestamps and evidence paths is the
[2026-09-02 live validation ledger](/superpowers/cas/gcs-live-validation-ledger-2026-09-02); this file
carries only the decisions and what is still open.

## What holds on GCS today {#what-holds}

Proved on the real bucket with the `gcs_hmac` client, binary of 2026-09-02 (full detail in the
[live validation ledger](/superpowers/cas/gcs-live-validation-ledger-2026-09-02)):

- The live gate `tests/integration/test_gcs_live`: 13 passed, 0 failed in four consecutive runs, covering
  batch `DeleteObjects`, multipart, native staged copy, and the condemned-staged retag.
- Two replicas over one pool survive a ten-minute connect-timeout storm with zero failed
  `INSERT`/`SELECT` and both mount leases held.
- The mount refuses a bucket verified as versioned and warns (rather than fails) when it cannot read the
  versioning configuration (`bd4cfc8d76f`); soft delete stays an operator precondition.

## Release gate: what is still unrun {#gate}

- **`[gcs-live-gate-oauth-and-ambiguity]`** — GATE — the `gcp_oauth` groups and the TLS-ambiguity arms
  have never executed against real Google. 10 `gcp_oauth` cases need an ADC triple or a GCE host; 4
  ambiguity cases need a TLS-terminating fault driver that does not exist yet (`s3_fault_proxy.py` is
  plain HTTP and cannot front `storage.googleapis.com`). Also open: the `test_storage_s3` lane once its
  historical image resolves, generation-aware LIST discovery, signed `x-goog-*` `extra_headers` for
  `gcs_hmac`. Decision needed: build the driver (days) or descope the four arms with a stated reason.
  Control contract: [live-results gate](/superpowers/cas/unconditional-blob-publication-live-results).

## Failure class 1: relink-confirm liveness (F11) {#relink-confirm-lane-livelock}

**`[relink-confirm-lane-livelock]`** — HARD / RELEASE GATE (data divergence, not data loss). The fix
(`CasRefLedger::confirmExactRef` rule 3 refuses only for a queued/carved mutation naming the exact ref,
via its `MutationScope`) shipped in `a43e2ef89d3` (+ `f4c87d0a6d8`, `b400b8c467e`), landed on
`antalya-26.6` as `5740d2a2953` (rule-3 scoping) + `bf77615fe0a` (`NO_REPLICA_HAS_PART` classification),
and first released in `26.6.4.20001.altinityantalya`. It is validated three independent ways: the TLA
model `CaRelinkConfirmCore.tla` under all sabotage flags; the `CAS*` gtest suite; the escalated two-node
fake-GCS liveness case `tests/integration/test_cas_gcs_relink_liveness` (zero refusals of any kind on
both nodes); and now, independently, by
[Altinity/ClickHouse#2310](/superpowers/cas/backlog/issue-2310) — closed 2026-09-14, gate passed on PR
#2300 run 10, `cas_alter_attach_1`/`cas_s3_cache_alter_attach_1` 8/8 on x86 and aarch64. Design:
[relink confirm liveness](/superpowers/specs/cas-relink-confirm-liveness-design), revision 10. Plan:
[implementation plan](/superpowers/plans/cas-relink-confirm-liveness).

**Still not closed on its own terms.** The item's stated release gate — a clean ten-minute phase-3 soak
on the real GCS stand, then a one-time two-hour closing soak — has not been re-run since
`.superpowers/sdd/2026-09-02-cas-relink-confirm-liveness/task-8-report.md`, whose run exited non-zero on
two harness-side blockers unrelated to the fix (a checkpoint-leak assert on the pool prefix's inherited
debris, and a fold round that holds the GC lease for hours, starving the chaos window). A re-run against
a fresh pool prefix removes both and is what would close this. Also open:
`CASRelinkConfirmRefusedStateLockBusy` is exercised by no test; no refusal counter's increment to
`system.events` has been observed live (issue #2310's residency item,
[`attach-partition-cas-relink-residency`](/superpowers/cas/backlog/issue-2310#open-items), covers one
arm of this; its proposed fix, Altinity/ClickHouse#2364, was closed unmerged 2026-09-16 and neither the
event nor its tests exist on `cas-gc-rebuild` or `antalya-26.6` — still unaddressed); and the liveness
case's two escalated constants (1000ms `_ckpt` delay, 80 inserts) were raised together and never
independently isolated.

**Closing checklist (2026-09-16).** The fix is in and measured; what keeps the item open is the gate run
and four hangers-on. One re-run closes most of it, so do them together:

1. **Re-run the ten-minute phase-3 soak on a fresh pool prefix.** Removes F13 (inherited-debris checkpoint
   assert) and F14 (one fault of four fired) at once. Pass = the criterion above (zero `unproven`, empty
   queues, no divergence) on a run whose chaos window actually ran all four faults.
2. **Prove every refusal counter reaches `system.events` on that run**, one query per name with
   `system_events_show_zero_values = 1` (must return 1 row each), and record any counter seen above zero.
   Today all are registered and none has fired live.
3. **Decide the counter merge on the same evidence.** Proposal: 7 → 4. `RefMutationInFlight` +
   `StateLockBusy` → one `Busy` (same operator action: retry; `StateLockBusy` has no test and no seam for
   one); `LaneWedged` + `LaneBroken` → one `LaneUnavailable` (the split is developer detail the trace line
   already carries); keep `MountCannotSpeak` and `RefTableNotResident` (PR #2364). Four names = four
   distinct operator actions: wait, repair the lane, look at lease/catalog, nothing. Rename only after the
   run, so the names in the spec, plan, integration test and `issue-2310.md` change once. Do NOT add
   counters for the `Unknown` returns above the ledger (`ContentAddressedMetadataStorage::confirmExactRef`,
   `Service::resolveContentAddressedConfirm`): malformed request, no route and disk lifecycle are
   misconfiguration or protocol errors, not operating states; the wrapper already logs, the handler should
   (three `LOG_DEBUG` lines, `DataPartsExchange.cpp:231`, `:250`, `:267`).
4. **Issue #2310**: run `alter_attach_partition_cas` part 1 against a post-fix package; it is the fast
   natural reproducer and the gate on closing the issue (`issue-2310.md`).
5. **Test cost**: measure whether the 1000 ms `_ckpt` delay alone reproduces the starvation at 40 inserts;
   if yes, halve the 6.5-minute liveness case.

**Operational workaround that works today:** alternate `SYSTEM STOP FETCHES` on one replica while the
other drains, then swap.

**`[relink-confirm-model-prose]`** (moved from `BACKLOG.md`, same design) — five prose imprecisions in
`CaRelinkConfirmCore` and its sabotage configs, found 2026-09-02, all still present: `"With only two
admitted shapes"` (`CaRelinkConfirmCore_sab_touchblind.cfg:1`) is not actually why rule 3 can't refuse
under `SabotageTouchBlind` (the predicate is constant-false regardless of shape count);
`SenderAdmitNoop`'s `~NoopDurable` guard (`CaRelinkConfirmCore.tla:215`) has no comment explaining the
defect it prevents; `CaRelinkConfirmCore_sab_stalecache.cfg:1`'s "lane quiescence" language no longer
matches the rule; `CaRelinkConfirmCore.tla:50`'s "removes exactly ONE load-bearing rule" now invites a
false partition reading; `:225`'s "same guard as `NsNoise`" is analogous, not identical, and the
`_sab_nopoison` narrative is loose about where graduation completes. None blocking.

## Failure class 2: hot control keys and the GCS mutation limit (F3, F12) {#gcs-hot-control-keys-429}

**`[gcs-hot-control-keys-429]`** — HARD / RELEASE GATE for GCS. GCS answers `429 SlowDown` above about
one mutation per second per object name; two CAS control objects hit this (`cas/ns/state/<ns>/_ckpt` and
`cas/ref_catalog`). Documented for operators at
[bucket-requirements.md#rate-consequences](/antalya/cas/bucket-requirements#rate-consequences).

**B1 + B2 are DONE.** `casUpdateImpl` (`CasRefCatalog.cpp:154-200`) now writes the catalog through
`op.hotKeys().submit(...)` under `Retry::standard()`, and its conflict wait,
`Retry::conflictBackoff()` (`Backend/CasRetry.h:36`), is full jitter `uniform(0, 200ms)` — replacing the
old 100-attempt no-backoff loop. Commits `2f4aa25b03c` + `37c9bd4356b`, both on `cas-gc-rebuild`.

**A1 is the remaining preferred step: coalesce `_ckpt` publications** to at most one per T seconds or K
flushes per namespace, keeping birth/epoch-seal/snapshot immediate.
`CasRefLedger.cpp:3984`'s `commitRefChunk` still calls `publishCkptContribution` synchronously inside
the lane tenure after every durable chunk — the coalescing that exists today
(`maybeScheduleSnapshotPublish`/`settleSnapshotPublish`, `:4124`) is the separate snapshot publisher, not
this. Must show the resulting `committed_through` lag is safe at every reader (INV-4 revalidation,
cross-epoch GC fold) and that unmount/shutdown flush the pending publish. Alternatives if A1 proves
insufficient: an async publisher outside the tenure (strongest effect on F11, but redefines
`NeedsRecovery`), a per-object token bucket (same ceiling, must combine with the async form). Rejected:
a rotating/generation-suffixed key (every reader would need to find the latest); sharding the catalog
(breaks the atomic ownership index); writing `Live` directly (loses the crash-safety of two-phase
creation).

**Cross-provider note for when A1 lands:** AWS answers `409 ConditionalRequestConflict` on the same hot
`_ckpt` key that GCS answers 429 for (RustFS/MinIO serialize the two `PUT`s locally, so the 409 never
shows there). Handling is correct today (`Unresolved` → resolve read → reissue), but the name is in no
classification list and the `PocoHTTPClient` `Response status: 409` line stays `Error`
(`[single-attempt-client-status-error-log-site]`, `src/IO/S3/PocoHTTPClient.cpp:740`, still unfixed).
When A1 lands, verify zero 429 on GCS and zero 409 on AWS for `_ckpt` under the soak's mutations stage.

**Measurements:**

**Measured again 2026-09-05, 8-minute no-chaos smoke (`ca_live_20260905_r1`, binary with the
single-attempt log-level change, 7ec4d8b0c39 relinked):** GCS answered 429 `The object exceeded the rate
limit for object mutation operations` 175 times on ch1 and 161 times on ch2, every one of them on the
node's own `cas/ns/state/<ns>/_ckpt`, in bursts of 30-45 per minute during the mutations / ttl_pressure
stages (`CASRefBatchFlushes` 408 on ch1 over the run). The engine absorbed all of them
(`CASRequestReissue` 178, `CASRequestResolveRead` 234, zero give-ups, zero failed queries), and the
`WriteBufferFromS3` line for them is now Debug (`S3Exception name SlowDown`), but each 429 still
leaves `<Error> AWSClient: Response status: 429, Too Many Requests` from `PocoHTTPClient`'s status
site, 174 / 161 lines per node -- the third log site named as a follow-up in
`docs/superpowers/cas/2026-09-04-single-attempt-client-log-level-proposal.md`. A1 stays the fix for
the rate; the log site is its own small item in the main BACKLOG.

**The same hot key on AWS S3 answers 409 `ConditionalRequestConflict` (seen 2026-09-05 in a manual test
on a Kubernetes CHI, key `.../cas/ns/state/<ns>/_ckpt`, object size 90):** AWS's error for a conditional
PUT that collides with another in-flight operation on the same object, "The conditional request cannot
succeed due to a conflicting operation against this resource", to be retried. It is NOT a 412 (the
precondition may still hold) and the SDK has no name for it ("Unable to parse ExceptionName"), which
makes the log line confusing: it reads like a refusal while it is the AWS spelling of the very
contention GCS spells as 429. Writers that can overlap on one `_ckpt`: the lane's frontier publish in
`commitRefChunk`, the asynchronous snapshot publisher (`tryPublishSnapshotAndAdvanceCheckpointOnce...`),
and a recovery walk (`runRecoveryWalkOnce` / `requireRecovery`, possibly from another replica). RustFS
and MinIO serialize the two PUTs and answer 412 to the loser, so the 409 never shows up locally.
Engine handling today, verified in `CasRequests.cpp`: the name is in no classification list
(`isDefinitelyRefusedWrite` covers malformed / EntityTooLarge / AccessDenied / credentials only), so it
falls into "outcome unknown" → `Unresolved` → exact resolve read → reissue with backoff inside the
budget; correct and safe, no failed query unless the 90 s budget runs out. The `WriteBufferFromS3` line
is Debug since 08c2a2ec25e; `PocoHTTPClient`'s `Response status: 409` stays Error
(`[single-attempt-client-status-error-log-site]`). When A1 lands, verify on BOTH providers: zero 429
on GCS and zero 409 on AWS for `_ckpt` under the soak's mutations stage; and classify the name
explicitly (a conflict-in-flight class next to `PreconditionFailed`) so the log says what it is.

## Failure class 3: conditional writes that made exactly one attempt (2026-09-02 audit) {#single-attempt-conditional-writes}

**`[cas-uncontrolled-conditional-writes]`** — the twenty-three call sites the
[GCS retry-coverage audit](/superpowers/cas/gcs-retry-coverage-audit-2026-09-02) found issuing a
conditional write with no retry above the single-attempt profile are **DONE for every actionable gap the
audit ranked**, closed by the same subsystem-wide migration as B1/B2: `2f4aa25b03c` +
`37c9bd4356b` ("cas: migrate every CAS subsystem onto `CasRequests`/`CasOperation`") + follow-up
`d528693ef24`. Verified directly: the mount's capability/pool-meta admission
(`CasPoolMeta.cpp:80,99,158`, now `op.readModifyWrite`/`op.create` under `Retry::standard()`); loose
table/namespace files (`CasPlainObjects.cpp:9-21`, now `op.readModifyWriteOnPresence`); `publishCkpt`'s
former hundred-iteration no-sleep loop (`CasRefCkpt.cpp:195-256`, now `op.readModifyWrite`); GC's
lease acquire/renew (`CasGc.cpp:4732-4737` `Gc::acquireOrRenewLease`, now
`store->openRequests().admit()` + `readModifyWrite`). The other GC liveness signal, `Gc::pulseHeartbeat`
(`CasGc.cpp:4713-4730`), stays single-attempt (`Retry::once()`) on purpose, not as a leftover gap: the
comment there states a lost pulse must not spend a retry budget fighting for the key, since the next
cadence tick supersedes it regardless. The migration's own commit message additionally claims the rest
of the audited mount surface (writer epoch, lease claim, keeper adopt, disk access check) moved the same
way; those individual sites were not re-audited one by one this pass, so a fresh call-site sweep
(mirroring the original audit) is worth one pass to confirm no site was missed, but the structural fix
(no production caller can reach the object-storage client unwrapped any more) makes a remaining gap
unlikely.

**Still open: the S3 client's own retry strategy.** A request the S3 client retries (not a `CasOperation`
verb) still retries up to `s3_retry_attempts` (default 500) with a 5 s cap and no jitter, so a
persistently throttled read/fold/resolve can block its thread for roughly forty minutes rather than
failing fast; the CAS operation deadline cannot preempt a request already inside that loop. Unchanged, a
different layer from the migration above.

How to stop finding these by inspection:
[making retry coverage structural](/superpowers/cas/retry-coverage-by-construction) — private virtuals
plus a controller-only handle, turning a future omission into a compile error.

## Environment and harness follow-ups {#environment}

- **`[gc-run-connect-failure-propagation]`** — DESIRABLE — a manual `SYSTEM CAS GC RUN`
  (`ContentAddressedMetadataStorage.cpp:614-651`, `runGarbageCollectionRoundNow`) still throws straight
  to its caller on a transport-level failure, while the background scheduler just retries next round.
  Decide whether the manual round should absorb transport failures the same way; the live gate's
  manual-round loop is strict by design.
- **`[confirm-refusal-reasons]`** — DESIRABLE — `Relink confirm is unproven (unknown)` still names no
  rule. A `ProfileEvent` per refusing rule (residency, warm, lane busy, row mismatch, fence), and the
  same breakdown in the debug line, would cut a repeat of the F11 investigation from an hour to a
  minute.
- **`[cas-throttling-by-key-class]`** — DESIRABLE — no `ProfileEvents` exist yet for `SlowDown`/429 on
  CAS control writes broken down by key class (`_ckpt`, `ref_catalog`, `gc/state`, lease objects).
- Connect-timeout storm 01:12-01:22 UTC (five Google front-end IPs, up to 2.9k timeouts/minute): provider
  or path, not a product defect; full detail in the
  [live validation ledger](/superpowers/cas/gcs-live-validation-ledger-2026-09-02).
- Harness debt, all still true: default container names are duplicated across `soak/chaos.py`,
  `soak/pool.py`, `scenarios/framework/observe.py` (one module should own them);
  `observe.RUSTFS_CONTAINER` is still an import-time constant used directly by
  `s15_s18_shards_lifecycle.py`, `s28_s33_corner.py`, `s34_s35_d1_churn.py` and
  `scripts/t8_s44_stuck_removing_discrimination.py` instead of a shared accessor;
  `lifecycle.DEFAULT_FSCK_CONTAINER` is dead. The `object_kind != 'none'` filter in `test_gcs_live` still
  has no local (stub-driven) coverage.

## Recommended order {#order}

1. **F11's closing gate.** A ten-minute phase-3 soak on the GCS stand against a fresh pool prefix (avoids
   the F13/F14 harness blockers), then the one-time two-hour closing soak. This is the only thing still
   standing between the fix and calling the item done.
2. **A1** (coalesce `_ckpt`). Gate: a ten-minute soak with 429 counts on `_ckpt` before and after.
3. **The two observability items** (`confirm-refusal-reasons`, `cas-throttling-by-key-class`) — cheap,
   and read through by steps 1 and 2.
4. **`[gcs-live-gate-oauth-and-ambiguity]`.** Build the fault driver or descope, independent of the above.
5. **By results:** the async publisher (A2) if A1 leaves tenures too long; a fresh call-site sweep of
   Failure class 3 if any gap turns up in production.

Open for brainstorming before code: for A1, which publications stay immediate, the coalescing policy,
the semantics of a lagging `committed_through` for GC fold/cleanup/INV-4, unmount behaviour, and the new
meaning of `NeedsRecovery`.
