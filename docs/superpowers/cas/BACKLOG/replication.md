---
description: 'Live backlog — replication and MergeTree integration: MOVE PART/PARTITION onto CA disks, merge/insert retry interactions with the mount-lease fence, and cross-replica relink.'
sidebar_label: 'Replication'
sidebar_position: 5
slug: /superpowers/cas/backlog/replication
title: 'CAS Backlog — Replication and MergeTree integration'
doc_type: 'guide'
---

# CAS Backlog — Replication and MergeTree integration {#replication}

Part of the [CAS live backlog](/superpowers/cas/backlog). Topic file for `MOVE PART`/`PARTITION`
onto CA disks, merge/insert retry interactions with the mount-lease fence, and cross-replica relink.

- **[move-part-to-ca-architecturally-unimplemented] DONE** — `ALTER TABLE ... MOVE PART|PARTITION TO DISK|VOLUME <ca disk>` now works: L1 routes `moving/` onto a prefixed staging ref (`2f2a3b01aa6`, `4d73e198f6b`, `81eab8b6968`) and L2 makes `MergeTreePartsMover::clonePart` route CA destinations through one shared transaction (`4229a1477be`), all on `cas-gc-rebuild`. S36's single-part `MOVE PART` chaos leg is green. Off-CA moves and merges writing a brand-new part were unaffected throughout.
- **[same-pool-move-reads-every-byte] a same-pool CA↔CA move still reads every byte, and the target-side collision case is unverified (2031-triage CAS-120; formerly `[VERIFY-ca-ca-same-pool-move]`)** — {#same-pool-move-reads-every-byte} — KEEP — Target side is expected near-free (`HEAD`-before-`PUT` dedup elides the upload; `05025_cas_attach_partition_cross_disk` proves the analogous same-pool `ATTACH PARTITION FROM` publish writes the correct rows, though it asserts row content only, not the `CASBlobBodyPutAvoided` dedup metric), but whether the MOVE target ref collides with the source's existing ref in the SAME table namespace is UNVERIFIED — run the S37 CA↔CA 3-disk (`ca_local3`) leg to decide. Source side is confirmed not free regardless: `DataPartStorageOnDiskBase::clonePart`'s CA branch (`DataPartStorageOnDiskBase.cpp:771-808`) still runs `copyDirectoryContentIntoTransaction` (`:637-661`), a `readFile`+`writeFile` loop per file, purely to rediscover blob references the source manifest already names — sequential, one file at a time, because the CA transaction batches every file into one manifest and its staging map is not mutex-guarded (`:625-636`); no relink shortcut exists here (the interserver fetch path has `getRelinkOffer`/`confirmExactRef`, unused by `MOVE`). Low priority: two CA disks on the same pool relocate nothing physically; the realistic target is a different pool, where relink is impossible anyway.
- **[killed-mid-move-partition-duplicate] killed mid-`MOVE PARTITION` leaves a persistent duplicated part** — {#killed-mid-move-partition} — KEEP — Confirmed by the S37 chaos leg: hard-killing a replica mid-`ALTER TABLE ... MOVE PARTITION ID 'all' TO VOLUME 'cas'` leaves the partition duplicated after heal (`count()=200`, `uniqExact(id)=100`); a later merge consolidates but does not dedup rows, so it does not self-heal. Pre-existing — the same duplication predates the MOVE-to-CA fix, via a different failure mode; S36's single-part leg stays green, only the multi-generation `MOVE PARTITION` path duplicates. Likely a generic ClickHouse crash-atomicity/replication-replay bug, not CA-specific. Needs: repro on a non-CA multi-disk policy to confirm before attributing. Chaos-edge; does not block MOVE-to-CA.
- **[move-to-ca-relink-from-replica] `MOVE`/TTL-move onto a CA disk should relink from a replica that already holds the part, the way zero-copy `MOVE` fetches instead of copying** — {#move-to-ca-relink-from-replica} — KEEP — Zero-copy's `MergeTreePartsMover::clonePart` (`MergeTreePartsMover.cpp:241-263`) fetches via `tryToFetchIfShared` before copying, and `fetchExistsPart` (`StorageReplicatedMergeTree.cpp:5980`) brings the metadata onto exactly that disk; a CA destination never takes that branch (`supportZeroCopyReplication()` stays `false` for CA by design, `DiskObjectStorage.h:53-57`), so every replica hitting a `TTL ... TO VOLUME 'cas'` at about the same moment races the same local read+hash+upload, and the losers pay the full cost for nothing. Wanted: a CA-specific hook at the same seam, reusing the `getRelinkOffer`/`confirmExactRef` exchange the fetch path already has, adopting via `prepareAdoptFromManifest` with byte-clone fallback. Open: replica discovery (CAS has no cross-server index of ref holders) and whether the N-replica race needs an upload stagger. Related: `[same-pool-move-reads-every-byte]` is the local leg (no replica needed); this is the cross-replica leg. Builds on the landed forced-relink-on-fetch design (`b794a1517dd`).
- **[mutation-registration-race] concurrent mutations can register out of order and a target part silently skips one** — {#mutation-registration-race} — KEEP — `StorageMergeTree::prepareMutationEntry` allocates the mutation block number outside `currently_processing_in_background_mutex`; registration into `current_mutations_by_version` happens later, in `addPreparedMutationEntry`. Two concurrent mutations (e.g. two lightweight `DELETE`s) can register out of order, and `selectPartsToMutate` bounds the applicable range only by the map's end, so a mutate task running between the two registrations can apply the later one alone — the part silently skips the earlier mutation while still reporting `is_done = 1`. A fix landed and was reverted the same day (2026-09-09) on `cas-gc-rebuild` (`153eee4d1fa` / revert `c8d14a95d3a`), and, on a feature branch off `antalya-26.6` (`feature/antalya-26.6/fix-mutation-registration-race`, never merged into `altinity/antalya-26.6`), by `c4b2ef2c1e6` / revert `a04846cfdeb`; net effect today is unfixed on both `cas-gc-rebuild` and `altinity/antalya-26.6`. Not CA-specific — reachable via plain `StorageMergeTree`; upstream ClickHouse carries the same code shape. Needs: find the revert reason before re-landing.
- **[merge-progress-reset-mount-fence] merge "progress reset in loops" after a sustained S3 fault** — KEEP — The transient-blip arm is fixed: `MountLeaseKeeper` retries one immutable renewal body inside the last confirmed lease and resolves ambiguous responses by exact `GET`; the 15-minute S39 gate recovered 50 isolated short pulses with no fence loss. A sustained fault can still close the fence correctly, after which CAS reports every write's fence refusal as `ABORTED`; `ReplicatedMergeMutateTaskBase::executeStep` (`ReplicatedMergeMutateTaskBase.cpp:71-77`) still treats `ABORTED` as "not an error" with no backoff, so the scheduler can recompute the same merge in a tight loop (239 times in one repro). Needs a retry-later class outside that exemption. Also owed: fsck-to-fixpoint after fence-loss recovery, linked to the still-open S30 dangling-precommit gap. Repro: `utils/ca-soak/docker-compose-s3faultproxy.yml`.
- **[r3-acked-lost-dataloss] DONE** — acked-then-lost INSERT on cross-request retry (Keeper block_id committed durably before the CAS manifest, so a byte-identical retry deduped against a lost part with zero rows written) fixed by `77484196b0d` (`MergeTreeData::Transaction::renameParts`, `cas-gc-rebuild`): every part's disk-storage transaction closes before Keeper block_id registration. S40 10/10, `acked=3796 lost=0`; `dl_probe` LOST=0 (was ~198/1314); the original chaos recipe that lost 1118 rows is now zero-deficit. A narrower residual (a block_id outliving a part lost later) is out of scope. An upstream submission draft sits unsent at `tmp/upstream_issue_dedup_durability.md`.
- **[RPL-5 slice] DONE** — `REPLACE PARTITION`/`ATTACH PARTITION ... FROM` queue-clone relink is now tested on CA: `test_attach_partition_from_relinks_on_queue_fetch` and `test_replace_partition_relinks_on_queue_fetch` (`tests/integration/test_cas_replicated_relink/test.py`) both assert `assert_relinked` plus zero new `cas_blob_puts`/blobs — proving the cloned fetch relinks rather than byte-refetches.
- **[B66b] DONE** — relink-into-detached (zero-byte `to_detached` fetch for same-pool parts) landed in `fac69e10dbd`: lifts the `!to_detached` advertise gate behind `allow_ca_relink`, composes the temporary storage under the detached parent, and covers a third detached caller (`executeClonePartFromShard`) the original plan had missed.
- **[relink-advertises-only-first-ca-pool] DONE (2031-triage CAS-134)** — Fixed by the forced-relink-on-fetch design (`docs/superpowers/specs/2026-09-03-cas-fetch-forced-relink-design.md`, landed `b794a1517dd`): the receiver advertises the SET of its policy's CA pool ids and the sender matches the reservation's pool. `test_two_pool_policy_relinks_into_second_pool` proves it.
- **[zero-copy-parity-audit] inventory every zero-copy replication feature and classify it for CAS: parity, not-needed-by-construction, or missing** — {#zero-copy-parity-audit} — KEEP (RESEARCH) — Walk every site behind `allow_remote_fs_zero_copy_replication`, `supportZeroCopyReplication`, the `*SharedData*` family, `tryToFetchIfShared`, `remote_fs_metadata` and the ten `*zero_copy*` MergeTree settings, and write one row per site: parity / not-needed / missing. Known missing: `MOVE` (`[move-to-ca-relink-from-replica]`), and whether merge/mutation coordination should make one replica merge while the rest relink (today N replicas race the same upload). Known parity: metadata-only fetch, same-pool `ATTACH`/`REPLACE PARTITION` publish (`05025_cas_attach_partition_cross_disk`), the cross-replica queue-clone leg (`[RPL-5 slice]`, DONE), the three `disable_*_for_zero_copy_replication` guards (`05024_cas_freeze`, `[B66b]`). Output: the audit table plus one new backlog item per "missing" verdict. Its prerequisite design has landed (`b794a1517dd`); this is unblocked and ready to run as a standalone research pass.

## The relink manifest-decode fallback catches `CORRUPTED_DATA` only, not `UNKNOWN_FORMAT_VERSION` (2031-triage CAS-043) {#relink-fallback-unknown-format-version}

KEEP — `ContentAddressedMetadataStorage::prepareAdoptFromManifest`'s decode guard
(`ContentAddressedMetadataStorage.cpp:2393-2400`; function renamed from `prepareRelink`) still
degrades to a byte fetch only for `CORRUPTED_DATA` and rethrows `UNKNOWN_FORMAT_VERSION`, even though
`decodePartManifest`'s header gate (`Formats/CasPartManifestFormat.cpp:142` → `expectHeaderLine`) and
the critical-key rule (`Formats/CasTextFormat.cpp:320-322`, a `!`-prefixed key) both raise that code —
a garbled `v` field fails the whole fetch loudly instead of re-requesting bytes.

Not reachable today: relink is offered only when both sides carry the same pool UUID
(`DataPartsExchange.cpp:405-427`), and mounting a pool decodes `_pool_meta` through an
exact-generation gate (`Formats/CasPoolMetaFormat.cpp:100-108`), so a generation-skewed pair cannot
share a mounted pool. This is hardening, not a live defect.

Fix (one line plus a test): accept `UNKNOWN_FORMAT_VERSION` alongside `CORRUPTED_DATA` in that catch,
as `Pool/CasRefLedger.cpp:191` already does for the analogous case. Do this before any release admits
a mixed-generation pool (format-freeze rule, `docs/superpowers/cas/AGENTS.md#invariants` item 7).

## The `.tmp` + `replaceFile` write dance is unusable on a committed CA part (2031-triage CAS-057) {#tmp-replacefile-on-committed-part}

KEEP — `ContentAddressedTransaction::moveFile` only services sources staged in the same transaction
(the staged entry re-keys in place at `ContentAddressedTransaction.cpp:1549-1579`), so the generic
local-disk crash-safety pattern (write `<name>.tmp`, then `replaceFile`) cannot work against a
published part across two separate autocommit transactions: the `.tmp` write autocommits fine (it is
not blob-mandatory — `partFileMustStayBlob`, `:67-74` — so the autocommit gate at `:814-817` admits
it), but the following `replaceFile` finds nothing staged and throws `LOGICAL_ERROR` (`:1586`). Fails
closed, not silently.

Latent: the one named caller, `DeleteBitmapFileOps::writeBitmapToStorage`
(`src/Storages/MergeTree/UniqueKey/DeleteBitmapFileOps.cpp:48-71`), is reached only from
`MergeTreeBitmapStore::installBitmap`, which has no caller outside its own gtests — the UNIQUE KEY
delete-bitmap path is unwired and gated behind `allow_experimental_unique_key`
(`registerStorageMergeTree.cpp:745-749`).

The generic escape hatch already exists: `IDataPartStorage::supportsAtomicFileWrites`
(`IDataPartStorage.h:197-200`, true for CA via `ContentAddressedMetadataStorage.h:270`), used by
`VersionMetadataOnDisk::storeInfoToDataPartStorage` since `45e43b37aaf`
(`VersionMetadataOnDisk.cpp:323`). Owed when the delete-bitmap path is wired: make
`writeBitmapToStorage` take the same short-circuit (or one shared transaction), plus a CA-disk test.

## Read-only replicas {#read-only-replicas}

- **[cas-readonly-replica] cross-node read-only replica over a shared CAS pool** — {#cas-readonly-replica} — KEEP (DESIGN) — Design
  only, not implemented: `docs/superpowers/specs/2026-07-14-cas-readonly-replica-snapshot-pin-design.md`
  proposes a third `Store::open` mount mode (`reader`, alongside `writer` and `read_only`) that heartbeats
  a lightweight reader-lease and publishes one durable pin object (`gc/readers/<reader_id>`), reuses the
  upstream readonly-refresh MergeTree feature for snapshot-sourced part discovery, and adds one opaque
  `IDataPartStorage::snapshot_pin` handle so a running query's held `DataPart`s float a GC retention floor
  with no "snapshot too old" cap — retention is bounded by query duration and reader-lease TTL. None of
  `snapshot_pin`, `gc/readers/`, the `reader` mount mode, or the proposed `CaReadPinCore` TLA+ model exist
  in the tree. Depends on the ref snapshot+log design and the pool-member decommission design landing
  first. No live gap forces this today; it is read-scaling, not a bug fix.
