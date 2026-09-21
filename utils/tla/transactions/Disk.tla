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
\* Four independent durability facts about one part, because the C++ has four separate syncs and the loader
\* depends on a different one of them for each thing it reads.
\*   durable/cached: the record readable as txn_version.txt. The rename that publishes it is a dentry in the
\*     PART's own directory, so only a sync of that directory promotes it.
\*   tmp_durable/tmp_cached: txn_version.txt.tmp. Its content is fsynced by storeInfoToDataPartStorage itself
\*     (buf->sync, VersionMetadataOnDisk.cpp:355-357).
\*   dir_durable/dir_cached: the part directory exists, under whatever name.
\*   named_durable/named_cached: the part directory exists under its FINAL name. Separate from dir, because
\*     renameTempPartAndReplace is a second operation (IMergeTreeDataPart::renameTo at
\*     src/Storages/MergeTree/IMergeTreeDataPart.cpp:2891, then DataPartStorageOnDiskBase.cpp:783), and a sync
\*     that lands before it preserves only the tmp_ name, which MergeTreeData::loadDataParts skips
\*     (src/Storages/MergeTree/MergeTreeData.cpp:2964). Both bits are dentries in the PARENT directory, so a
\*     parent sync promotes them and a sync of the part's own directory does not.
\*   payload_durable: the data files. finalizePartAsync takes fsync_after_insert
\*     (src/Storages/MergeTree/MergeTreeDataWriter.cpp:1176-1179), which is a setting of its own and defaults to
\*     false (MergeTreeSettings.cpp:642). A part whose data files did not survive is broken at load
\*     (MergeTreeData.cpp:2618-2676), which is why the loader reads this bit.
PartDiskType == [durable : DiskRec, cached : DiskRec,
                 tmp_durable : BOOLEAN, tmp_cached : BOOLEAN,
                 dir_durable : BOOLEAN, dir_cached : BOOLEAN,
                 named_durable : BOOLEAN, named_cached : BOOLEAN,
                 payload_durable : BOOLEAN]
MutDiskType == [file_durable : BOOLEAN, file_cached : BOOLEAN,
                tid : AllTids, csn_durable : AllCSNs, csn_cached : AllCSNs]

AbsentPartDisk == [durable |-> NoRecord, cached |-> NoRecord, tmp_durable |-> FALSE, tmp_cached |-> FALSE,
                   dir_durable |-> FALSE, dir_cached |-> FALSE,
                   named_durable |-> FALSE, named_cached |-> FALSE, payload_durable |-> FALSE]
LegacyPartDisk == [durable |-> Legacy, cached |-> Legacy, tmp_durable |-> FALSE, tmp_cached |-> FALSE,
                   dir_durable |-> TRUE, dir_cached |-> TRUE,
                   named_durable |-> TRUE, named_cached |-> TRUE, payload_durable |-> TRUE]
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
                                  /\ disk[p].named_durable = disk[p].named_cached
                                  /\ disk[p].payload_durable = disk[p].dir_cached

DiskRead(p) == disk[p].cached
DiskDurable(p) == disk[p].durable
DiskDirExists(p) == disk[p].dir_cached
\* the part directory under the name the loader looks for
DiskNamed(p) == disk[p].named_cached
DiskTmpOnly(p) == disk[p].cached.kind = "None" /\ disk[p].tmp_cached
DiskHasInfo(p) == disk[p].cached.kind = "Info"
DiskInfo(p) == disk[p].cached.info

\* storeInfoToDataPartStorage: write tmp, fsync tmp, rename. The renamed file is cached; it is durable at once
\* in Durable mode or with FSYNC_PART_DIRECTORY, whose guard spans the replaceFile
\* (VersionMetadataOnDisk.cpp:361-363); otherwise the tmp file is durable and the rename is not.
DiskWithInfo(p, info) ==
  IF Layered /\ ~FSYNC_PART_DIRECTORY
  THEN [disk EXCEPT ![p].cached = InfoRec(info), ![p].tmp_cached = FALSE, ![p].tmp_durable = TRUE]
  ELSE [disk EXCEPT ![p].cached = InfoRec(info), ![p].durable = InfoRec(info), ![p].tmp_cached = FALSE, ![p].tmp_durable = FALSE]
\* createDirectories plus the data files: the part directory exists, under a TEMPORARY name, and its content is
\* durable only under FSYNC_AFTER_INSERT. The directory's own dentry is durable only once the parent is synced,
\* which under FSYNC_OUTER_RENAME the rename below does.
DiskWithDir(p) ==
  [disk EXCEPT ![p].dir_cached = TRUE,
               ![p].dir_durable = ~Layered,
               ![p].payload_durable = ~Layered \/ FSYNC_AFTER_INSERT]
\* renameTempPartAndReplace: the part directory takes its final name. renameTo syncs the moved directory and
\* then its parents (DataPartStorageOnDiskBase.cpp:786-800), so under the settings this promotes both the
\* part's own dentries, which is the inner rename, and the parent's, which is the name itself. The two are
\* separate constants here for the reason Types.tla gives.
DiskWithFinalName(p) ==
  LET outer == ~Layered \/ FSYNC_OUTER_RENAME
      inner == ~Layered \/ FSYNC_PART_DIRECTORY
      d1 == [disk EXCEPT ![p].named_cached = TRUE]
      d2 == IF outer THEN [d1 EXCEPT ![p].named_durable = TRUE, ![p].dir_durable = TRUE] ELSE d1
  IN IF inner THEN [d2 EXCEPT ![p].durable = disk[p].cached, ![p].tmp_durable = disk[p].tmp_cached] ELSE d2
DiskWithoutDir(p) == [disk EXCEPT ![p] = AbsentPartDisk]
\* loadMetadata removes the tmp file it found (VersionMetadataOnDisk.cpp:58-60, removeTmpMetadataFile at :297)
DiskWithoutTmp(p) == [disk EXCEPT ![p].tmp_cached = FALSE, ![p].tmp_durable = FALSE]
\* An fsync of the part's data files. It touches the metadata record's durability in neither direction: it
\* cannot publish the txn_version.txt rename, which is a dentry belonging to the directory sync below, and it
\* cannot unpublish the tmp file either. Writing tmp_durable = tmp_cached here was the second half of M43: after
\* a store has renamed the tmp file away, tmp_cached is FALSE while tmp_durable is TRUE, so the assignment
\* LOWERED it and a data-file fsync silently discarded the one thing a crash could still have recovered.
DiskWithFilesSynced(p) == [disk EXCEPT ![p].payload_durable = TRUE]
\* A sync of the PART's directory: the dentries inside it, which is the txn_version.txt rename. It moves the
\* pair <<durable, tmp_durable>> to its cached value in one step, which is why lowering tmp_durable is right
\* here and wrong above: the rename becoming durable is what removes the tmp file from the post-crash state.
\* DurabilityMonotone is the fence over that distinction.
DiskWithDirSynced(p) == [disk EXCEPT ![p].durable = disk[p].cached, ![p].tmp_durable = disk[p].tmp_cached]
\* A sync of the PARENT directory: the dentries naming the part directory itself, both the temporary name and,
\* once the rename has happened, the final one.
DiskWithParentSynced(p) == [disk EXCEPT ![p].dir_durable = disk[p].dir_cached,
                                        ![p].named_durable = disk[p].named_cached]
\* After a crash the cached layers are the durable ones, and a part whose FINAL name did not survive keeps
\* nothing: the loader looks the part up by that name, so a directory that survived only under its temporary
\* name is one MergeTreeData::loadDataParts skips, and a record inside a directory that did not survive at all
\* goes with it. The temporary directory a real restart finds and cleans up is not an action of this model; the
\* loader's view of it is that it never existed, which is what losing the record expresses.
DiskAfterCrash ==
  [p \in Parts |-> IF ~disk[p].named_durable THEN AbsentPartDisk
                  ELSE [disk[p] EXCEPT !.cached = disk[p].durable,
                                       !.tmp_cached = disk[p].tmp_durable,
                                       !.dir_cached = disk[p].dir_durable,
                                       !.named_cached = disk[p].named_durable]]
MutDiskAfterCrash == [m \in Mutations |-> [mdisk[m] EXCEPT !.file_cached = mdisk[m].file_durable,
                                                           !.csn_cached = mdisk[m].csn_durable]]
====
