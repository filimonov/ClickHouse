---
description: 'Replication, recovery and fault graphs (owner replication) for the storage cost calculator: how a follower acquires inserted, merged and mutated parts on s3, s3plain, CAS and a local disk, zero-copy, the natural fetch split, restart catch-up and S3 retry and multipart-abort accounting, with source-cited predictions and reconciliation on the 26.8 binary.'
sidebar_label: 'Storage cost replication'
sidebar_position: 58
slug: /superpowers/reports/storage-cost-replication
title: 'Storage cost calculator — replication, recovery and fault graphs'
doc_type: 'reference'
---

# Storage cost calculator — replication, recovery and fault graphs {#replication}

A follower acquires every inserted part by fetch and, with default settings, executes merges and mutations itself on every variant: the natural split is fetch 1.0 for inserts and 0.0 for merges and mutations in all 24 natural records. Forced fetch and local execution reconcile exactly on `s3` and `ebs`; `s3plain` and CAS are measured against assumed ranges. 174 records over 3 runs, 168 ok; the failures are listed in §[Gaps](#gaps).

| Item | Value |
|---|---|
| Binary, executed | sha256 `b2cd30842534dd8e481e3e18fd54c82a2ab4e978ec0efe0d84f8704880e4f668` (`tmp/bin/26.8/clickhouse`) |
| Source commit | `tmp/src-26.8` at `09d0772a842c5d73cf3ef35b4c8b017d838215a0` |
| Report runs | `cost-20261024T115147Z`, `cost-20261024T121344Z`, `cost-20261024T123232Z`; `measurements.jsonl` holds their 174 records, 168 ok and 6 FAILED (kept: they are open reconciliation items) |
| Rig | every run executes the cards from a detached worktree at `f35fd96a730`, so all records name one rig commit; manifests and predictions are read from this directory |
| Earlier runs | `cost-20261024T093949Z` (smoke, s3, K=1: found the zero-copy receiver `HeadObject`); `cost-20261024T113828Z` (stopped after 66 records: local had no window start, CAS windows crossed, s3plain sent `HeadObject` the assumed ranges missed, CAS republishes a failed blob 8 times). Neither is in `measurements.jsonl` |
| Predictions | hold-out `replication-natural-holdout.wide` first at `630bc885ee4`; all cases `2c006846dbc`; CAS and s3plain ranges and the CAS fault graph revised at `0bc1e557850` after the stopped run, before the report runs |
| Store | RustFS behind the ledger proxy; every record replays from `raw/<card>/<run_id>/ledger.jsonl.gz` |
| Cuts | `replacement.default` and `faults.fetch` (P2), owner directive D14: no manifest; `replacement_requests_per_part` and `fetch_retry_rate` are `unsupported` |

Cards: `utils/cas-yardstick/yardstick/cost/cards/replication.py` (`replication-fetch`, `replication-local`,
`replication-zero-copy`, `replication-natural-fit`, `replication-natural-holdout`, `restart-catchup`) and
`cards/faults.py` (`faults`). Tables are `ReplicatedMergeTree` on two replicas; `r1` runs the workload, `r2` is the
follower. Fixture tests: `utils/cas-yardstick/tests/test_cost_cards_replication.py` (both directions of every
acquisition assertion); live: `tests/live_replication.py`.

## Operation graphs {#operation-graphs}

Paths are under `tmp/src-26.8/src/`.

### Which path a follower takes {#acquisition}

The rule is the lifecycle report's `acquisition_rule` (`MergeFromLogEntryTask.cpp:101`–`:349`,
`MutateFromLogEntryTask.cpp:85`–`:230`). The cases exercise three of its branches:

| Branch | Trigger in the cases | Path |
|---|---|---|
| Inserted part | always | fetch (`GET_PART` entry) |
| `always_fetch_merged_part` / `always_fetch_mutated_part` | table setting on the follower only | fetch (`MergeFromLogEntryTask.cpp:101`, `MutateFromLogEntryTask.cpp:85`) |
| Default settings, every source present, entry younger than 3600 s | default | local execution (`:224` and `MutateFromLogEntryTask.cpp:138` do not fire) |
| A source missing when the entry runs | natural merges only | fetch of the merged part (`MergeFromLogEntryTask.cpp:155`) |

An unsuccessful preparation makes the task call `executeFetch` (`ReplicatedMergeMutateTaskBase.cpp:207`). A fetch
writes a `DownloadPart` row (`StorageReplicatedMergeTree.cpp:6040`) and counts `ReplicatedPartFetches` (`:5983`);
a fetched `MERGE_PARTS` result also counts `ReplicatedPartFetchesOfMerged` (`:2955`). A local merge writes
`MergeParts` and counts `ReplicatedPartMerges` (`MergeFromLogEntryTask.cpp:495`); a local mutation writes
`MutatePart` and counts `ReplicatedPartMutations` (`MutateFromLogEntryTask.cpp:386`). The cards type each acquired
part by the event that produced it on the replica that executed it (`cards/replication.py` `acquisitions`).

### Fetch: s3, s3plain, local {#fetch}

- **Sender.** Without zero-copy and without a relink offer, the sender streams every checksummed file and the
  files without checksums (`Storages/MergeTree/DataPartsExchange.cpp:469`, `:491`, `:499`). Each file is read
  through the disk (`:567`): one `GetObject` per object of the part on `s3`.
- **Receiver.** Each received file is written through the disk (`:1094`): one `PutObject` per file. The part is
  then loaded (`:1322`, `Storages/MergeTree/IMergeTreeDataPart.cpp:1504`), which reads its small files back.
- **Local volume (`ebs`).** No object-store request; the bytes cross the interserver port and land on the
  follower's disk.

### Fetch: CAS relink {#relink}

On a content-addressed disk the sender offers a relink: it ships the part's manifest, not its blobs
(`DataPartsExchange.cpp:425`). The receiver decodes the manifest and builds the part from shared-pool blob
references through the attach path (`Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.cpp:2391`,
`prepareEntries` with `ProvenanceOp::Attach`): it stages its own manifest, precommits it, asks the sender to confirm
the offer over the interserver port, then promotes and renames the part. No blob body is read or written.

### Fetch: zero-copy (s3) {#zero-copy}

With `allow_remote_fs_zero_copy_replication = 1` the sender sends the part's local metadata, which names the
shared objects (`DataPartsExchange.cpp:456`–`:464`), and the receiver writes that metadata to local files and keeps
the objects shared (`:1250`). Zero-copy locks live in Keeper, not in the store
(`MergeFromLogEntryTask.cpp:296`).

### Local execution {#local-execution}

A follower that merges or mutates itself reads its own copy of the sources and writes its own result, exactly
like the originator: the s3 report's `merge_gets_per_source_part` and `mutation_gets_per_part` per replica. A
`ReplicatedMergeTree` mutation entry lives in Keeper (`StorageReplicatedMergeTree.cpp:8962`), so unlike a
`MergeTree` mutation no `mutation_N.txt` object is written.

### Branches the cases do not exercise {#branches-excluded}

- **Clone instead of fetch.** `fetchPart` may clone an unchanged local source of a mutation and still write
  `DownloadPart` and count `ReplicatedPartFetches` (`StorageReplicatedMergeTree.cpp:5775`). The forced mutation
  cases rewrite every row, so the source changes; the cards' relay assertion (`relay0.from_target > 0`) shows a
  peer transfer in every fetch case.
- **CAS byte fetch.** A CAS sender streams the part when the pools or capabilities differ (`DataPartsExchange.cpp:398`),
  and the receiver falls back to a byte fetch after a relink mechanism failure or retries after a failed confirm
  (`:985`, `:997`). The rig runs one shared pool, so only the relink branch is measured; a cluster on separate pools is
  priced by the `s3`-like stream graph, which this report does not measure for CAS.
- **Zero-copy fallback.** A zero-copy fetch that hits a retryable S3 or zero-copy error retries as a streamed
  fetch (`DataPartsExchange.cpp:1061`); not exercised.

### Restart and catch-up {#catchup}

A stopped replica keeps its queue in Keeper. On start it loads its tables (the s3 report's startup graph), then
works through its queue: an entry whose part is already covered is skipped (`StorageReplicatedMergeTree.cpp:2695`);
an inserted part that the originator has merged away is served by a replica having a covering part (`:5441`). The
catch-up therefore fetches the parts the originator holds at the restart, not every part inserted while it was down.

### Retries and failed uploads {#retries}

- **Two retry loops for the exercised writes.** The retry strategy retries a retryable error up to
  `s3_retry_attempts` times (`IO/S3/Client.cpp:116`), and the wrapper of `PutObject`, `UploadPart` and the other
  requests whose response body is read reissues the request up to `s3_retry_attempts + 1` times (`:852`). An
  `UploadPart` that keeps failing with a retryable 500 is sent `(s3_retry_attempts + 1)^2` times (4 measured with
  `s3_retry_attempts = 1`); `GetObject` bypasses the outer loop (`:567`) and a non-retryable error stops at once.
  One injected failure is retried once.
  `s3_retry_attempts` defaults to 500 (`IO/S3Defines.h:47`).
- **5XX before the store.** The proxy answers 500 without forwarding: the failed attempt never reached the store;
  the retry is the only billed request.
- **Lost response.** The store accepted the request; the reply was lost. The retry reaches the store again: both
  are billed.
- **Failed multipart upload.** A write buffer whose upload fails is cancelled and aborts its own multipart upload
  (`IO/WriteBufferFromS3.cpp:244`, `:469`): the incomplete parts leave the inventory without rig action.

### Measured graphs per variant {#graphs-measured}

Per acquired part, Compact parts of 5 `UInt64` columns (`acquire_requests`, `local_exec_requests`):

| Variant | Fetch: sender | Fetch: receiver | Local merge (4 sources) | Local mutation |
|---|---|---|---|---|
| `s3` | GET 11 (one per object) | PUT 11, GET 9 (load) | GET 32 (8 per source), PUT 11 | GET 26, PUT 11 |
| `s3plain` | GET 11 | PUT 13, GET 9, HEAD 1 | GET 32, PUT 13, HEAD 1 | refused by the capability gate |
| `cas` | GET 1–2.5 (manifest for the offer) | PUT 7, GET 8–9, LIST 0.5 (insert) | GET 9.7, PUT 7, HEAD 1 | GET 33, PUT 8.3, HEAD 1 |
| `ebs` (local) | none | none | none | none |

- **s3.** Sender reads each object once; receiver writes each file and reads 9 of 11 back to load (a Wide part:
  19 objects, also 9 load reads). Exact in every repeat.
- **s3plain.** As s3 plus the plain-rewritable directory metadata: 2 more `PutObject` and one `HeadObject` per
  part on each writing replica (the s3plain report owns that graph: a new directory writes its `prefix.path`
  object, a rename rewrites it). Mutations are refused (`cards/s3plain.py`
  `GATES['mutations']`), so no mutation case runs there.
- **CAS relink.** The interserver payload is the manifest, not the blobs: 0.4–0.9 % of a Compact part's bytes on
  disk, 0.3 % of the Wide hold-out's, so `acquire_bytes` on CAS is valid for the Compact fit shape only. The
  receiver's 7 `PutObject` match the CAS report's publish graph without blob uploads (its manifest and the
  precommit, promote and rename ref transactions); no blob is written. After promotion the receiver loads the part
  (`DataPartsExchange.cpp:1705`): its `GetObject` include blob reads of marks and index (a `shared/blobs/` GET in
  the Wide hold-out).
  CAS windows carry background ref traffic, so CAS values are means of K=3 with their spread in the record.
- **Zero-copy (s3).** No object is written or sent: the receiver checks the part's unique id with one
  `HeadObject` (`DataPartsExchange.cpp:1202`) and loads it (GET 9); the sender reads nothing
  (`zero_copy_requests`).
- **Exactness.** `s3` and `ebs` predictions are integers from the s3 report's coefficients and reconcile exactly.
  `s3plain` and CAS predictions are assumed ranges (`basis: assumption`); their records lie inside those ranges,
  which is not an exact validation: the measured integers in this section are the graph, checked only by
  repetition (s3plain equal in every repeat, CAS K=3 spread).
- **Sender attribution.** The originator's reads of objects it created in the window are the sender side, minus
  the same reads in the matching `replication-local` case where nothing is fetched (CAS reads its new manifest at
  promote, s3plain heads its new directory). Inserts use the merge case as that baseline.
- **Interserver bytes.** `acquire_bytes` is what the serving replica sent per byte on disk of the fetched part:
  1.0004–1.0008 for streamed parts (file framing), the manifest only for CAS.

### Restart and catch-up, measured {#graphs-restart}

With `r1` quiet before the restart, 500 inserted parts collapse into 5 active parts, and the follower fetches
exactly those 5 (no local merge) on every variant. Per fetched part, both replicas together
(`catchup_requests_per_missed_part`): `s3` GET 21.6, PUT 12.6; `s3plain` GET 21.6, PUT 14.6, HEAD 1; CAS GET 7.2,
PUT 5.8; `ebs` none. The fetched parts are the merged results, larger and partly Wide, so the per-part counts
exceed the Compact `acquire_requests`. Before its first fetch the follower sends its startup requests
(`startup_requests_after_restart`): `s3` GET 48, PUT 1, DELETE 1 with 3 parts to load.

### Faults per variant {#graphs-faults}

Extra attempts per failed attempt against the same insert without a fault (`faults.none`), `retry_outcome_mix`:

| Variant | 5XX before the store | Lost response |
|---|---|---|
| `s3`, `s3plain` | PUT +1 (the injected attempt is not billed) | PUT +1 (both reached the store) |
| `cas` | PUT +1, GET +1 | GET +1: CAS reads what landed instead of reissuing the PUT |

Multipart abort (`failed_upload_residue`): on `s3` and `s3plain` the INSERT fails, the failing part is sent
`(1 + 1)^2 = 4` times with `s3_retry_attempts = 1`, the server sends one `AbortMultipartUpload` that the store
accepts, and the incomplete inventory is 0 with no rig action. CAS republishes the failed blob 8 times (8 uploads,
8 aborts, 32 failed attempts).

## Cases and reconciliation {#reconciliation}

| Case | Policy | Records ok / failed | Kind |
|---|---|---|---|
| `faults.lost-response` | `cas` | 3 / 0 | fit |
| `faults.lost-response` | `s3` | 3 / 0 | fit |
| `faults.lost-response` | `s3plain` | 3 / 0 | fit |
| `faults.multipart-abort` | `cas` | 1 / 2 | — |
| `faults.multipart-abort` | `s3` | 3 / 0 | fit |
| `faults.multipart-abort` | `s3plain` | 3 / 0 | fit |
| `faults.multipart-abort-part3` | `s3` | 3 / 0 | holdout |
| `faults.multipart-abort-part3` | `s3plain` | 3 / 0 | holdout (consumed) |
| `faults.multipart-abort-part3b` | `cas` | 0 / 3 | — |
| `faults.none` | `cas` | 3 / 0 | fit |
| `faults.none` | `s3` | 3 / 0 | fit |
| `faults.none` | `s3plain` | 3 / 0 | fit |
| `faults.put-5xx` | `cas` | 3 / 0 | fit |
| `faults.put-5xx` | `s3` | 3 / 0 | fit |
| `faults.put-5xx` | `s3plain` | 3 / 0 | fit |
| `faults.put-5xx-nth7` | `cas` | 3 / 0 | holdout (consumed) |
| `faults.put-5xx-nth7` | `s3` | 3 / 0 | holdout |
| `faults.put-5xx-nth7` | `s3plain` | 3 / 0 | holdout (consumed) |
| `replication-fetch.insert` | `cas` | 3 / 0 | fit |
| `replication-fetch.insert` | `local` | 3 / 0 | fit |
| `replication-fetch.insert` | `s3` | 3 / 0 | fit |
| `replication-fetch.insert` | `s3plain` | 3 / 0 | fit |
| `replication-fetch.insert-wide` | `cas` | 3 / 0 | holdout (consumed) |
| `replication-fetch.insert-wide` | `local` | 3 / 0 | holdout |
| `replication-fetch.insert-wide` | `s3` | 3 / 0 | holdout |
| `replication-fetch.insert-wide` | `s3plain` | 3 / 0 | holdout (consumed) |
| `replication-fetch.merge` | `cas` | 3 / 0 | fit |
| `replication-fetch.merge` | `local` | 3 / 0 | fit |
| `replication-fetch.merge` | `s3` | 2 / 1 | fit |
| `replication-fetch.merge` | `s3plain` | 3 / 0 | fit |
| `replication-fetch.mutation` | `cas` | 3 / 0 | fit |
| `replication-fetch.mutation` | `local` | 3 / 0 | fit |
| `replication-fetch.mutation` | `s3` | 3 / 0 | fit |
| `replication-local.merge` | `cas` | 3 / 0 | fit |
| `replication-local.merge` | `local` | 3 / 0 | fit |
| `replication-local.merge` | `s3` | 3 / 0 | fit |
| `replication-local.merge` | `s3plain` | 3 / 0 | fit |
| `replication-local.merge-8` | `cas` | 3 / 0 | holdout (consumed) |
| `replication-local.merge-8` | `local` | 3 / 0 | holdout |
| `replication-local.merge-8` | `s3` | 3 / 0 | holdout |
| `replication-local.merge-8` | `s3plain` | 3 / 0 | holdout (consumed) |
| `replication-local.mutation` | `cas` | 3 / 0 | fit |
| `replication-local.mutation` | `local` | 3 / 0 | fit |
| `replication-local.mutation` | `s3` | 3 / 0 | fit |
| `replication-natural-fit.default` | `cas` | 3 / 0 | fit |
| `replication-natural-fit.default` | `local` | 3 / 0 | fit |
| `replication-natural-fit.default` | `s3` | 3 / 0 | fit |
| `replication-natural-fit.default` | `s3plain` | 3 / 0 | fit |
| `replication-natural-holdout.wide` | `cas` | 3 / 0 | holdout |
| `replication-natural-holdout.wide` | `local` | 3 / 0 | holdout |
| `replication-natural-holdout.wide` | `s3` | 3 / 0 | holdout |
| `replication-natural-holdout.wide` | `s3plain` | 3 / 0 | holdout |
| `replication-zero-copy.insert` | `s3` | 3 / 0 | fit |
| `replication-zero-copy.insert-wide` | `s3` | 3 / 0 | holdout |
| `replication-zero-copy.merge` | `s3` | 3 / 0 | fit |
| `replication-zero-copy.mutation` | `s3` | 3 / 0 | fit |
| `restart-catchup.10min` | `cas` | 1 / 0 | fit |
| `restart-catchup.10min` | `local` | 1 / 0 | fit |
| `restart-catchup.10min` | `s3` | 1 / 0 | fit |
| `restart-catchup.10min` | `s3plain` | 1 / 0 | fit |
| `restart-catchup.3min` | `cas` | 1 / 0 | holdout |
| `restart-catchup.3min` | `s3` | 1 / 0 | holdout |

Hold-out checks (prediction from the fit coefficients, measured mean of the hold-out records):

| Coefficient | Case | Policy | Cell | Predicted | Measured | Slack | Holds |
|---|---|---|---|---|---|---|---|
| `acquire_requests` | `replication-fetch.insert-wide` | `s3` | sender.GetObject | 19.0 | 19.0 | 0.0 | yes |
| `acquire_requests` | `replication-fetch.insert-wide` | `s3` | receiver.PutObject | 19.0 | 19.0 | 0.0 | yes |
| `acquire_requests` | `replication-fetch.insert-wide` | `s3` | receiver.GetObject | 9.0 | 9.0 | 0.0 | yes |
| `acquire_bytes` | `replication-fetch.insert-wide` | `s3` | bytes_ratio | 1.0008 | 1.0004 | 0.05 | yes |
| `acquire_requests` | `replication-fetch.insert-wide` | `local` | sender.GetObject | 0.0 | 0 | 0.0 | yes |
| `acquire_requests` | `replication-fetch.insert-wide` | `local` | receiver.PutObject | 0.0 | 0 | 0.0 | yes |
| `acquire_requests` | `replication-fetch.insert-wide` | `local` | receiver.GetObject | 0.0 | 0 | 0.0 | yes |
| `acquire_bytes` | `replication-fetch.insert-wide` | `local` | bytes_ratio | 1.0008 | 1.0004 | 0.05 | yes |
| `local_exec_requests` | `replication-local.merge-8` | `s3` | GetObject per source | 8.0 | 8.0 | 0.0 | yes |
| `local_exec_requests` | `replication-local.merge-8` | `s3` | PutObject | 11.0 | 11.0 | 0.0 | yes |
| `local_exec_requests` | `replication-local.merge-8` | `local` | GetObject per source | 0.0 | 0.0 | 0.0 | yes |
| `local_exec_requests` | `replication-local.merge-8` | `local` | PutObject | 0 | 0 | 0.0 | yes |
| `zero_copy_requests` | `replication-zero-copy.insert-wide` | `s3` | receiver.GetObject | 9.0 | 9.0 | 0.0 | yes |
| `zero_copy_requests` | `replication-zero-copy.insert-wide` | `s3` | receiver.HeadObject | 1.0 | 1.0 | 0.0 | yes |
| `zero_copy_requests` | `replication-zero-copy.insert-wide` | `s3` | receiver.PutObject | 0 | 0 | 0.0 | yes |
| `fetch_fraction` | `replication-natural-holdout.wide` | `s3` | insert | 1.0 | 1.0 | 0.05 | yes |
| `fetch_fraction` | `replication-natural-holdout.wide` | `s3` | merge | 0.0 | 0.0 | 0.05 | yes |
| `fetch_fraction` | `replication-natural-holdout.wide` | `s3` | mutation | 0.0 | 0.0 | 0.05 | yes |
| `fetch_fraction` | `replication-natural-holdout.wide` | `s3plain` | insert | 1.0 | 1.0 | 0.05 | yes |
| `fetch_fraction` | `replication-natural-holdout.wide` | `s3plain` | merge | 0.0 | 0.0 | 0.05 | yes |
| `fetch_fraction` | `replication-natural-holdout.wide` | `cas` | insert | 1.0 | 1.0 | 0.05 | yes |
| `fetch_fraction` | `replication-natural-holdout.wide` | `cas` | merge | 0.0 | 0.0 | 0.05 | yes |
| `fetch_fraction` | `replication-natural-holdout.wide` | `cas` | mutation | 0.0 | 0.0 | 0.05 | yes |
| `fetch_fraction` | `replication-natural-holdout.wide` | `local` | insert | 1.0 | 1.0 | 0.05 | yes |
| `fetch_fraction` | `replication-natural-holdout.wide` | `local` | merge | 0.0 | 0.0 | 0.05 | yes |
| `fetch_fraction` | `replication-natural-holdout.wide` | `local` | mutation | 0.0 | 0.0 | 0.05 | yes |
| `catchup_requests_per_missed_part` | `restart-catchup.3min` | `s3` | GetObject | 21.6 | 20.0 | 5.4 | yes |
| `catchup_requests_per_missed_part` | `restart-catchup.3min` | `s3` | PutObject | 12.6 | 11.0 | 3.15 | yes |
| `startup_requests_after_restart` | `restart-catchup.3min` | `s3` | DeleteObject | 1 | 1 | 2 | yes |
| `startup_requests_after_restart` | `restart-catchup.3min` | `s3` | GetObject | 48 | 48 | 2 | yes |
| `startup_requests_after_restart` | `restart-catchup.3min` | `s3` | PutObject | 1 | 1 | 2 | yes |
| `retry_outcome_mix` | `faults.put-5xx-nth7` | `s3` | PutObject|attempts | 12 | 12 | 0 | yes |
| `retry_outcome_mix` | `faults.put-5xx-nth7` | `s3` | PutObject|store_reached | 11 | 11 | 0 | yes |
| `failed_upload_residue` | `faults.multipart-abort-part3` | `s3` | cost-20261024T115147Z/s3/faults/multipart-abort-part3/k1 | 0 | 0 | 0 | yes |
| `failed_upload_residue` | `faults.multipart-abort-part3` | `s3` | cost-20261024T115147Z/s3/faults/multipart-abort-part3/k2 | 0 | 0 | 0 | yes |
| `failed_upload_residue` | `faults.multipart-abort-part3` | `s3` | cost-20261024T115147Z/s3/faults/multipart-abort-part3/k3 | 0 | 0 | 0 | yes |
| `failed_upload_residue` | `faults.multipart-abort-part3` | `s3plain` | cost-20261024T115147Z/s3plain/faults/multipart-abort-part3/k1 | 0 | 0 | 0 | yes |
| `failed_upload_residue` | `faults.multipart-abort-part3` | `s3plain` | cost-20261024T115147Z/s3plain/faults/multipart-abort-part3/k2 | 0 | 0 | 0 | yes |
| `failed_upload_residue` | `faults.multipart-abort-part3` | `s3plain` | cost-20261024T115147Z/s3plain/faults/multipart-abort-part3/k3 | 0 | 0 | 0 | yes |

Restart catch-up (K=1; requests recomputed from the raw ledger with maintenance excluded):

| Policy | Inserted while down | Fetched | Follower local merges | Startup requests | Catch-up requests |
|---|---|---|---|---|---|
| `cas` | 500 | 5 | 0 | {"DeleteObject": 2, "GetObject": 33, "HeadObject": 1, "ListObjectsV2": 5, "PutObject": 6} | {"GetObject": 36, "PutObject": 29} |
| `local` | 500 | 5 | 0 | {} | {} |
| `s3` | 500 | 5 | 0 | {"DeleteObject": 1, "GetObject": 48, "PutObject": 1} | {"GetObject": 108, "PutObject": 63} |
| `s3plain` | 500 | 5 | 0 | {"CopyObject": 1, "DeleteObject": 2, "GetObject": 53, "HeadObject": 1, "ListObjectsV2": 9, "PutObject": 1} | {"GetObject": 108, "HeadObject": 5, "PutObject": 73} |

## Coefficients {#coefficients}

| Coefficient | Index | Tolerance | Validity | Fit |
|---|---|---|---|---|
| `acquire_requests` | variant_id, result_type, replication_role, replication_op | abs 1.0, rel 0 | {"part_rows": [50000, 200000]} | per part the follower fetched (DownloadPart rows): the follower's requests (receiver) and the originator's reads of the objects it wrote in the window minus the same reads in the matching replication-local case, where nothing is fetched (sender; inserts use the merge baseline); s3, s3plain and ebs equal in every repeat, cas the mean of K=3; an absent op is 0 |
| `acquire_bytes` | variant_id, result_type | abs 0.0, rel 0.05 | {"part_rows": [50000, 200000]} | interserver bytes the originator sent (relay0 from_target) / bytes on disk of the fetched parts (DownloadPart size_in_bytes), mean of K=3 |
| `local_exec_requests` | variant_id, result_type, replication_op | abs 1.0, rel 0 | {"source_parts": [1, 4]} | the follower's requests per merge or mutation result it executed itself; merge rows are for 4 Compact sources of 50 000 rows (the per-source rule is in the README) |
| `zero_copy_requests` | result_type, replication_role, replication_op | abs 0, rel 0 | {"part_rows": [50000, 200000]} | as acquire_requests, with allow_remote_fs_zero_copy_replication = 1 |
| `fetch_fraction` | variant_id, result_type | abs 0.05, rel 0.0 | {"inserts": [24, 24]} | follower fetches / follower acquisitions per result type (cards/replication.py follower_split), mean of K=3 |
| `catchup_requests_per_missed_part` | variant_id, replication_op | abs 0, rel 0.25 | {"inserts_while_down": [250, 500]} | requests of both replicas after the follower's first catch-up fetch / parts it fetched; K=1 |
| `startup_requests_after_restart` | variant_id, replication_op | abs 2, rel 0 | {"active_parts": [3, 3]} | the follower's requests from its start to its first catch-up fetch; K=1 |
| `retry_outcome_mix` | variant_id, fault_class, replication_op, attempt_kind | abs 0, rel 0 | {"nth": [3, 7]} | per failed attempt: attempts (attempt_kind attempts) and attempts the store saw (store_reached: upstream outcome other than not_sent) of the fault case minus the fault-free insert (faults.none); an absent key is 0 |
| `failed_upload_residue` | variant_id | abs 0, rel 0 | {"upload_parts": [2, 3]} | incomplete multipart bytes in the replay after the window, before any rig abort |

Values are in `coefficients.json`. `replacement_requests_per_part` and `fetch_retry_rate` are `unsupported` in the registry (their only cases are cut), defaults 0.

## Gaps and limitations {#gaps}

- **s3 `replication-fetch.merge` k2 (FAILED, open).** The sender read the merged `data.bin` twice: the first
  `GetObject` was closed by the reader after 792 KB (downstream `dropped`, no rule installed) and reissued from
  offset 0. One of three repeats; cause not identified. `acquire_requests` carries `abs` tolerance 1 for it.
- **CAS `faults.multipart-abort` k2, k3 (FAILED, open).** After the 8 aborts, 32 MiB (one upload part) is still
  incomplete in the replay after a 30 s wait: an `UploadPart` that lands after its upload's abort, or an abort
  the store did not apply. `failed_upload_residue` has no CAS value; CAS residue of a failed upload is unpriced.
- **CAS `faults.multipart-abort-part3b` (FAILED, consumed).** The fresh CAS hold-out predicted 3 upload parts per
  publication; 72 MB is 4 parts (32 + 16 + 16 + 8 MiB), so `UploadPart` is 56, not 48. A prediction arithmetic
  error, not a graph change; no fresh CAS hold-out of the residue was run.
- **CAS and s3plain hold-outs consumed.** Their assumed ranges were revised (`0bc1e557850`) after the stopped run
  `cost-20261024T113828Z` had already run those cases, so their hold-out edges are `consumed`. The done hold-outs
  of every coefficient are on `s3` and `ebs`. Fresh CAS and s3plain hold-outs belong to Task 7.
- **CAS values are means.** CAS windows carry the background ref publisher; values are K=3 means and the absolute
  tolerance is half the widest K=3 range (the spreads are in the fit details).
- **Restart catch-up is K=1.** The 10-minute case runs once per variant; its tolerance (`rel` 0.25) is assumed,
  not derived from repeats, and the 3-minute hold-out runs on `s3` and `cas` only.
- **Natural split covers few merges.** Each natural record has 4 merges and 2 mutated parts on the follower; a
  follower fetch of a merge result (a source missing when the entry runs) never occurred, so `fetch_fraction` of
  merges and mutations is 0 with an assumed absolute tolerance of 0.05.
- **Fault outcomes are checked in the fit, not the card.** The cards assert that each fault fired and its retry
  chain; the billable split (`store_reached`) is derived from the records' ledger counts when the coefficient is
  fitted. A card assertion of it needs a rerun and is parked (§[Review](#review)).
- **Replacement and fetch faults are cut** (D14): a non-zero `replacement_rate` is unsupported.

## Cross-report id mismatches {#cross-report}

- None. The s3 report's `recovery` cell names `catchup_requests_per_missed_part` and its `inserts` notes name
  `acquire_requests[s3][insert]`; both ids are defined here. The CAS, EBS and s3plain reports reference no
  replication coefficient. The registry table of `catchup_requests_per_missed_part` gains a `replication_op` key
  (it was indexed by `variant_id` only): the s3 cell's catch-up row must look it up per op.

## Review {#review}

One Codex round (`gpt-6-astra`, reasoning high, read-only), run while the natural and restart runs were in
progress: 10 MAJOR, 1 MINOR, verdict REVISE. Dispositions:

| # | Finding | Disposition |
|---|---|---|
| 1 | Failed repeats are discarded before fitting | Closed by record: failed records stay in `measurements.jsonl` and are open items in §[Gaps](#gaps); CAS residue has no value; the s3 sender re-read sets `abs` 1 |
| 2 | "Exact" s3plain checks accept broad ranges | Closed: exactness is claimed for `s3` and `ebs` only; s3plain and CAS are stated as checked against assumed ranges (§[Measured graphs](#graphs-measured)) |
| 3 | Consumed hold-outs reused as independent evidence | Closed: every hold-out whose prediction changed in `0bc1e557850` is `consumed`; done hold-outs are on `s3` and `ebs` |
| 4 | Hold-out edges done without a residual check | Closed: each hold-out edge is checked against the fit (table in §[Cases](#reconciliation)); a failing check makes the edge `consumed`. Object-proportional fetch ops scale with objects per part; CAS `acquire_bytes` is valid for the Compact shape only |
| 5 | Stochastic tolerances not from K=3 | Closed for CAS: `abs` = half the widest K=3 range. Restart stays K=1 (controller ruling 8) with an assumed `rel` 0.25, labelled |
| 6 | CAS relink "no blob reads" is false | Closed: the receiver's load reads blobs after promotion (`DataPartsExchange.cpp:1705`); text corrected |
| 7 | Acquisition branches missing from graphs and coverage | Closed by exclusion: clone-instead-of-fetch, CAS byte-fetch fallbacks and zero-copy fallback are listed in §[Branches not exercised](#branches-excluded) with the reason each is outside the measured topology |
| 8 | Restart accounting mixes acquisition, local execution and maintenance | Closed: startup and catch-up are recomputed from the raw ledger with maintenance excluded; follower local merges are reported per record (0 means the catch-up is fetches only) |
| 9 | Retry outcomes discarded | Closed in the coefficient: `retry_outcome_mix` has an `attempt_kind` key (`attempts`, `store_reached`). The card-level assertion of the split is parked: it needs a new rig commit and a rerun of every fault case |
| 10 | Retry formula stated too generally | Closed: scoped to the retryable writes; `GetObject` bypasses the outer loop |

The MINOR (#10 in the table) is closed; no finding is parked except the card assertion of #9.

## Reproduce {#reproduce}

```
utils/cas-yardstick/rustfs-bin.sh ensure
YARDSTICK_LIVE=1 python3 utils/cas-yardstick/yardstick.py cost run --binary tmp/bin/26.8/clickhouse \
  --manifests docs/superpowers/reports/2026-10-24-storage-cost-replication --cases all \
  --policies s3,s3plain,cas,local --replicas 2 --runs-dir tmp/cas-yardstick/cost-runs
```
