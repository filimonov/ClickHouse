# CAS open items, 2026-10-18 {#cas-open-items-2026-10-18}

State on 2026-10-18 after the member-lifecycle (PR A/B/C), lease (#2517), blob-publication (#2555) and draft-prep work.
Base: `antalya-26.6-20261008` @ `8fefc27bd62`.

## Decisions pending {#decisions-pending}

- Open PRs for the six prepared drafts: CAS-364, 373, 366, 365, 367, 368 (branches `fix/antalya-26.6/cas-36x-…`,
  reports in `tmp/noclock/drafts/prep/<task>/`).
- #2517: after the base merge (`d410298e2c4`), check `cas_selects` on the new run and whether the server logs show
  write pauses; one green run is not proof.
- #2343: add a link to #2577 (the honest `_ckpt` error text promised there).
- Delete the local branch `fix/antalya-26.6/cas-ckpt-giveup-reason` (superseded by #2577).
- Disk: docker build cache and dangling images (~70 GB), `tmp/cas358` (5.7 GB).

## Small fixes proposed, not started {#small-fixes}

- CAS-354 / CAS-402: `Expect404ResponseScope` for an `If-Match` PUT in `ObjectStorageBackend::nativeConditionalPut`;
  removes the flaky `<Error> AWSClient: Response status: 404` stateless reds (RCA:
  `docs/superpowers/reports/2026-10-16-cas-aws404-conditional-put-rca.md`).
- CAS-404 (High): `system.cas_mounts.server_uuid` shows the uuid with its 64-bit halves swapped.
- CAS-403: `installRenewer` throws `LOGICAL_ERROR` when a stop races a remount; PR B's fenced tombstone added a
  production trigger.
- Cherry-pick the renewer stop-race fix (cause 1, `1a9f1fe5bcd`) to the release line.
- CAS-132 (High): `outcome` column in the `SYSTEM CAS GC RUN` result, so a non-leader run is visible.

## Merge order {#merge-order}

- PR A #2566 → PR B #2567 → PR C #2574. PR A is not in B/C: after A lands, merge the base into B and C.
- Rerun soak S47 after A and C: its only red is PR A's `NoWait` refusal text.
- #2570 (CAS-378), #2573 (CAS-392), #2577 (commit outcome texts), #2594 (blob attempt log level) are open.

## Default settings: survive a ~2 minute storage outage {#defaults-two-minute-outage}

Goal: a loss of connectivity to the object store for about 2 minutes passes without a lost mount, without a
remount and without client-visible write errors beyond a pause.

- Today: lease `cas_mount_lease_ttl_ms` 30 s, writes give up at the 90 s policy deadline, an attempt is bounded by
  the disk `request_timeout_ms` (30 s); RustFS read stalls of 90–100 s already fail CI (#2343).
- With #2516 (observers wait for the declared lease, holder cap 10 min) a longer lease is safe; with #2517 an
  expired lease is a pause, not a loss.
- To decide: lease TTL and renew period, the write policy deadline (90 s), attempt timeouts; cost: slower detection
  of a dead member and a longer GC floor hold. Verify on the fault-proxy stand with the store unreachable for 120 s
  (inserts, merges, selects; no `CASMountLeaseLost`, no remount, no client error).
- Measured (20 stand runs): #2517 alone removes lease loss and remount for full outages of 60–180 s; clients
  still get immediate `NETWORK_ERROR` (210). Work split into CAS-405.1 (fetch/cleanup busy loop), CAS-405.2
  (pause = wait), CAS-405.3 (lease 120 s / renew 40 s), CAS-405.4 (fast reclaim after a crash), CAS-405.5
  (`_ckpt` give-up text); CAS-256 reproduced.

## Data correctness {#data-correctness}

- CAS-393: idempotent `INSERT` retry — record the block id in the ref-log promote entry; recovery rebuilds the
  window; check how 26.6.4 reads unknown ref-log keys first. Today a retry after an unknown outcome can duplicate,
  and with `non_replicated_deduplication_window > 0` a retried insert can be lost (`tmp/dupcheck/REPORT.md`).
- CAS-394: do not roll back a part whose promote PUT is durable (model first). CAS-395: the same check for merges,
  mutations, `REPLACE`/`ATTACH PARTITION`, fetch.
- CAS-368 AC#2: model the late landing of a namespace-create step-1 write.
- Residuals that need a format change (CAS-367/368): catalog revision and `removal_by`. No task yet.

## CI and infrastructure {#ci-infra}

- #2343: collect RustFS logs and host metrics in CAS regression jobs; try RustFS 1.0 (`cas_alter_attach_3` is red
  in ~40% of runs).
- #2555 stress: the third server start under ASan did not finish in 80 s while replaying hundreds of `_log`
  objects per namespace. Not root-caused.
- Retry `Connection refused` inside the S3 client for single-attempt creation (nothing was sent, so a resend cannot
  duplicate). No task yet.
- CAS-396…402: follow-ups from the #2555 review.

## Lease (#2517) follow-ups {#lease-follow-ups}

- Move the renewal loop into the request engine (review simplification 6), after #2517.
- CAS-256: merges restart once a second during a write pause.
- CAS-298 AC#3/#4 (S39 on all replicas, CAS-344).
- CAS-389 (Low): stop-vs-renewal race, cause 2.

## Prose and housekeeping {#prose-housekeeping}

- Run the deferred prose batch `tmp/lease-series/deferred-docs-fixes.md`.
- Shrink the memory index `MEMORY.md` (over its 24.4 KB limit).
- S47 coverage: ASan/TSan builds, more seeds, the `cas-drop-member` CLI sweep (CAS-213.1).
