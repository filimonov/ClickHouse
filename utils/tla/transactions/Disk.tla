---- MODULE Disk ----
\* Part of the TLA+ model of MergeTree transactions; see
\* docs/superpowers/specs/2026-09-17-mergetree-transactions-tla-design.md
\* Baseline C++: upstream ClickHouse master 2c24b6b9291e
EXTENDS Types
VARIABLES disk, mdisk

DiskRec == [kind : {"None", "Legacy", "Info"}, info : VersionInfoType]
NoRecord == [kind |-> "None", info |-> EmptyInfo]
Legacy == [kind |-> "Legacy", info |-> EmptyInfo]
InfoRec(i) == [kind |-> "Info", info |-> i]
PartDiskType == [durable : DiskRec, cached : DiskRec,
                 tmp_durable : BOOLEAN, tmp_cached : BOOLEAN,
                 dir_durable : BOOLEAN, dir_cached : BOOLEAN]
MutDiskType == [file_durable : BOOLEAN, file_cached : BOOLEAN,
                tid : AllTids, csn_durable : AllCSNs, csn_cached : AllCSNs]

AbsentPartDisk == [durable |-> NoRecord, cached |-> NoRecord, tmp_durable |-> FALSE, tmp_cached |-> FALSE,
                   dir_durable |-> FALSE, dir_cached |-> FALSE]
LegacyPartDisk == [durable |-> Legacy, cached |-> Legacy, tmp_durable |-> FALSE, tmp_cached |-> FALSE,
                   dir_durable |-> TRUE, dir_cached |-> TRUE]
AbsentMutDisk == [file_durable |-> FALSE, file_cached |-> FALSE, tid |-> EmptyTID,
                  csn_durable |-> UnknownCSN, csn_cached |-> UnknownCSN]

Layered == DISK_MODE = "Layered"

DiskInit ==
  /\ disk = [p \in Parts |-> IF p \in LEGACY_PARTS THEN LegacyPartDisk ELSE AbsentPartDisk]
  /\ mdisk = [m \in Mutations |-> AbsentMutDisk]

DiskTypeOK ==
  /\ disk \in [Parts -> PartDiskType]
  /\ mdisk \in [Mutations -> MutDiskType]
  /\ ~Layered => \A p \in Parts : /\ disk[p].durable = disk[p].cached
                                  /\ disk[p].tmp_durable = disk[p].tmp_cached
                                  /\ disk[p].dir_durable = disk[p].dir_cached

DiskRead(p) == disk[p].cached
DiskDurable(p) == disk[p].durable
DiskDirExists(p) == disk[p].dir_cached
DiskTmpOnly(p) == disk[p].cached.kind = "None" /\ disk[p].tmp_cached
DiskHasInfo(p) == disk[p].cached.kind = "Info"
DiskInfo(p) == disk[p].cached.info

\* storeInfoToDataPartStorage: write tmp, fsync tmp, rename. The renamed file is cached; it is durable at once
\* in Durable mode or with FSYNC_PART_DIRECTORY; otherwise the tmp file is durable and the rename is not.
DiskWithInfo(p, info) ==
  IF Layered /\ ~FSYNC_PART_DIRECTORY
  THEN [disk EXCEPT ![p].cached = InfoRec(info), ![p].tmp_cached = FALSE, ![p].tmp_durable = TRUE]
  ELSE [disk EXCEPT ![p].cached = InfoRec(info), ![p].durable = InfoRec(info), ![p].tmp_cached = FALSE, ![p].tmp_durable = FALSE]
\* renameTempPartAndReplace, and the same setting that decides the metadata rename: one directory sync guard
\* covers both renames, so with fsync_part_directory on the part directory is in place durably at once.
DiskWithDir(p) ==
  IF Layered /\ ~FSYNC_PART_DIRECTORY THEN [disk EXCEPT ![p].dir_cached = TRUE]
  ELSE [disk EXCEPT ![p].dir_cached = TRUE, ![p].dir_durable = TRUE]
DiskWithoutDir(p) == [disk EXCEPT ![p] = AbsentPartDisk]
\* loadMetadata removes the tmp file it found (VersionMetadataOnDisk.cpp:58-60, removeTmpMetadataFile at :297)
DiskWithoutTmp(p) == [disk EXCEPT ![p].tmp_cached = FALSE, ![p].tmp_durable = FALSE]
\* fsync of one file: the metadata record only
DiskWithMetaSynced(p) == [disk EXCEPT ![p].durable = disk[p].cached, ![p].tmp_durable = FALSE]
\* directory fsync: the directory entry and the rename
DiskWithDirSynced(p) == [disk EXCEPT ![p].dir_durable = disk[p].dir_cached, ![p].durable = disk[p].cached,
                                     ![p].tmp_durable = disk[p].tmp_cached]
\* After a crash the cached layers are the durable ones, and two kinds of part keep nothing at all.
\* A part whose directory did not survive loses its metadata file with it, because the file is inside the
\* directory. A part in `unnamed` has not been renamed into place, so what survives carries a tmp_ name that
\* MergeTreeData::loadDataParts never loads; the crash is where the model discards it.
DiskAfterCrash(unnamed) ==
  [p \in Parts |-> IF ~disk[p].dir_durable \/ p \in unnamed THEN AbsentPartDisk
                  ELSE [disk[p] EXCEPT !.cached = disk[p].durable,
                                       !.tmp_cached = disk[p].tmp_durable,
                                       !.dir_cached = disk[p].dir_durable]]
MutDiskAfterCrash == [m \in Mutations |-> [mdisk[m] EXCEPT !.file_cached = mdisk[m].file_durable,
                                                           !.csn_cached = mdisk[m].csn_durable]]
====
