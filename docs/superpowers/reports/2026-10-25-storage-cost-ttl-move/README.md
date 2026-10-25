---
description: 'What a TTL move from a local hot volume to a cold object-store volume costs on s3, s3_plain_rewritable and CAS: the insert write of the moved part plus one part load, per replica.'
sidebar_label: 'Storage cost TTL move'
sidebar_position: 61
slug: /superpowers/reports/storage-cost-ttl-move
title: 'Storage cost calculator — TTL move to a cold tier'
doc_type: 'reference'
---

# Storage cost calculator — TTL move to a cold tier {#ttl-move}

A TTL move of a part from the local hot volume to a cold object-store volume costs **the insert write of the same
part on the cold disk, plus one load of the moved part from the cold disk**. The local source part costs no
object request. Without zero-copy replication every replica moves its own copy.

- **Verdict:** the rule holds exactly on all three variants, plus a once-per-table term
  (`ttl_move_table_once_requests`): `s3plain` PUT 1 and HEAD 1 for `moving/`, CAS PUT 3 and GET 13 for the table's
  namespace birth on the pool.
- **Runs:** `cost-20261025T142655Z`, all cases in one session (rig commit `7186870ad4c`, `rig_dirty` false,
  RustFS 1.0.0-beta.9, two passes per case). The same counts were first measured in `cost-20261025T133717Z`,
  `cost-20261025T135003Z` and `cost-20261025T141926Z`, each case after its prediction; the report keeps the
  one-session rerun so its records name one rig commit. An uncited `s3` smoke pass (`cost-20261025T130324Z`) came
  first.
- **Binary:** `tmp/bin/26.8/clickhouse`, sha256 `b2cd30842534dd8e481e3e18fd54c82a2ab4e978ec0efe0d84f8704880e4f668`,
  source `09d0772a842c5d73cf3ef35b4c8b017d838215a0`.
- **Cards:** `move` and `move-replicated` in `utils/cas-yardstick/yardstick/cost/cards/move.py`.
- **Manifests:** `experiments/`; predictions: `predictions/<experiment_id>.<policy>.json`; coefficient:
  `coefficients.json`; records: `measurements.jsonl` and `raw/`.

## Move path {#move-path}

Paths are under `src/` at `09d0772a842`.

1. **Selection.** Each server schedules moves for its own parts from their TTL
   (`Storages/MergeTree/MergeTreeData.cpp:12126`).
2. **Clone.** Without `allow_remote_fs_zero_copy_replication` (false by default,
   `Storages/MergeTree/MergeTreeSettings.cpp:2155`) the move is the ordinary branch (`MergeTreeData.cpp:12348`):
   `makeCloneOnDisk` copies the part to `<table>/moving/<part>/` on the cold disk
   (`Storages/MergeTree/MergeTreePartsMover.cpp:281`, `Storages/MergeTree/IMergeTreeDataPart.cpp:2929`).
   - `s3`, `s3plain`: `copyDirectoryContent` calls `copyFile` per file (`Disks/IDisk.cpp:158`), which writes through
     the cold disk's `writeFile` exactly as an insert does (`Disks/IDisk.cpp:76`).
   - CAS: one disk transaction for the whole clone (`Storages/MergeTree/DataPartStorageOnDiskBase.cpp:889`–`:905`),
     published under the `moving/<part>` ref
     (`Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.cpp:1490`).
3. **Load.** The clone is loaded back from the cold disk (`MergeTreePartsMover.cpp:299`): 10 small files on a TTL
   table (the 9 the s3 startup graph measured, plus `ttl.txt`, `IMergeTreeDataPart.cpp:2493`). On CAS this is one
   manifest GET (`ContentAddressed/Pool/CasManifestReader.cpp:56`) and, for a Wide part, its first marks blob.
4. **Swap.** `moving/<part>` is renamed to `<part>` (`MergeTreePartsMover.cpp:313`): free on `s3` (local
   metadata), one `PutObject` on `s3plain` (`prefix.path` rewrite), one ref transaction on CAS (as the insert's
   rename from `tmp_insert_*`).
5. **Source removal.** The hot part is removed from the local disk after `old_parts_lifetime`: no object request.
6. **Replicas.** The replication log has no move entry (`Storages/MergeTree/ReplicatedMergeTreeLogEntry.h:27`), and
   each replica starts its own moves (`Storages/StorageReplicatedMergeTree.cpp:6184`): a follower moves its own copy
   and fetches nothing.

## Prediction {#prediction}

`move.ttl`: 3 Compact parts of 100 000 rows and 2 Wide parts of 500 000 rows (6 columns, 44 B per row), on `r1`.

| Variant | PutObject | GetObject | HeadObject | Rule |
|---|---|---|---|---|
| `s3` | 80 | 50 | 0 | 12 objects per Compact part, 22 per Wide part; 10 GETs per part load |
| `s3plain` | 91 | 50 | 6 | `s3` + directory create and rename per part + `moving/` once per table |
| `cas` | 67–89 | 42–54 | 17–27 | insert publish (PUT 7, GET 7 per part, blob branches) + 1 manifest GET per part + 1 marks GET per Wide part; exact per the observed branch population |

`move-replicated.ttl` (`s3`, 2 replicas, 2 Compact parts): `r1` and `r2` each PUT 24, GET 20.

## Results {#results}

Foreground requests of the moving replica, both passes identical. `HEAD`/`PUT`/`GET` are
`HeadObject`/`PutObject`/`GetObject`.

| Case | Variant | Predicted | Measured | Result |
|---|---|---|---|---|
| `move.ttl` | `s3` | PUT 80, GET 50 | PUT 80, GET 50 | exact |
| `move.ttl` | `s3plain` | PUT 91, GET 50, HEAD 6 | PUT 91, GET 50, HEAD 6 | exact |
| `move.ttl` | `cas` | publish graph at the observed branches: PUT 85, GET 44–45, HEAD 27 | PUT 88, GET 57, HEAD 27 | missed: namespace birth |
| `move.ttl-holdout` | `cas` | publish graph + birth term at the observed branches: PUT 70, GET 54, HEAD 16 | PUT 70, GET 54, HEAD 16 | exact |
| `move-replicated.ttl` | `s3`, `r1` and `r2` | PUT 24, GET 20 each | PUT 24, GET 20 each | exact |
| `move-replicated.ttl-cas` | `cas`, `r1` | PUT 21, GET 29, HEAD 2 | PUT 21, GET 29, HEAD 2 | exact |
| `move-replicated.ttl-cas` | `cas`, `r2` | PUT 17, GET 31, HEAD 2 | PUT 17, GET 31, HEAD 2 | exact |

- **Per part.** `s3` and `s3plain` wrote 12 objects per Compact part and 22 per Wide part, the objects of an insert
  of that part. Each moved part was loaded with 10 GETs (`move_load_gets_per_part`). `s3plain` added 2 directory
  PUTs and 1 HEAD per part, as on insert.
- **CAS per part.** 25 fresh blobs and 2 hash hits (`move.ttl`), 16 fresh (`move.ttl-holdout`). Per part 1 manifest
  PUT, 3 ref transactions, the promote manifest GET and one more manifest GET for the load; a Wide part also
  read its first marks blob (`cas_move_load_gets_per_part`).
- **CAS namespace birth.** In `move.ttl` every extra request precedes the first part's publish: `ref_catalog`
  PUT 2 and GET 8, `_ckpt` PUT 1 and GET 4 (one 404), `_log` GET 1 (404). After it, every ref transaction is the
  insert's catalog GET, `_log` PUT, `_ckpt` GET and PUT. The table was created on the tiered policy, so the CAS
  disk saw it first at the move. The hold-out with another part mix predicted the term and landed exactly.
- **Bytes.** Uploaded body bytes equal the object sizes of the cold layout exactly (`s3`, `s3plain`):
  29 059 964 B for the five parts. `bytes_on_disk` of the moved parts is 4 505 B less, so the calculator must price
  the move upload as the part's file bytes. Load GETs read 10 737 B for five parts on `s3`. CAS uploaded
  29 055 727 B and read 142 096 B, the manifests and ref objects included.
- **Replicas.** Each replica logged its own `MovePart` events (2 each), neither logged a `DownloadPart` in the
  window, and the interserver relays carried 0 B: the follower moved its own copy.
- **CAS follower.** With moves run one replica at a time (`params.sequential_moves`), `r2` found every blob `r1`
  had published: each blob is a hash hit (HEAD and a `.meta` GET), no blob PUT. `r2` still pays per part the
  manifest and 3 ref transactions in its own namespace, the load GET and its own namespace birth. It uploaded
  about 29 KB against 4.42 MB on `r1`. Moves that overlap on two replicas can both miss; this case does not
  measure that race.

## Rule for the calculator {#rule}

Per moved part on each replica: the insert write of that part on the cold disk, plus `move_load_gets_per_part`
GETs (`s3`, `s3plain`: `part_load_files` + 1 for `ttl.txt`) or `cas_move_load_gets_per_part` (CAS: 1 manifest
GET, +1 for a Wide part). Once per table and replica: `ttl_move_table_once_requests[variant_id, replication_op]`.
Bytes uploaded per moved part are the part's file bytes. The local source part costs no object request.
On CAS a replica that moves a part already published by another replica takes the hash-hit branch for every blob
(`cas_branch_requests`): HEAD and `.meta` GET per blob, no blob PUT and no blob bytes.

## Limits {#limits}

- One table shape (6 narrow columns), 100 000 and 500 000 rows per part, 2–5 parts per window.
- No filesystem cache variant, no projection, no partitioned table.
- The CAS two-replica case moved one replica after the other; concurrent moves of the same part were not measured.
- Moves ran within seconds of `SYSTEM START MOVES`. A statistics refresh (`refresh_statistics_interval`, 300 s)
  reads the moved parts later; the existing statistics refresh terms price it.
