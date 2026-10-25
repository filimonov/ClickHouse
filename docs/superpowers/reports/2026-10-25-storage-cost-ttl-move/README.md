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

- **Status:** source prediction, committed before the run.
- **Binary:** `tmp/bin/26.8/clickhouse`, source `09d0772a842`.
- **Cards:** `move` and `move-replicated` in `utils/cas-yardstick/yardstick/cost/cards/move.py`.
- **Manifests:** `experiments/`; predictions: `predictions/<experiment_id>.<policy>.json`.

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
