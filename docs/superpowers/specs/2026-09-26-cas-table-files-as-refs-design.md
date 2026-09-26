---
description: 'Design study: store every table-level file of a CAS namespace as a ref (single-entry inline manifest) and the non-replicated deduplication window as claims inside the ref log, with a one-way migration at first mount under a bumped reader generation. Removes the second storage model of a namespace, the TableSubdir probe machinery and the per-insert rewrite of deduplication-log segments. Alternative to the cache of spec 2026-09-26-cas-directory-probes-no-list-design.md §3.2.'
sidebar_label: 'CAS table files as refs'
sidebar_position: 13
slug: /superpowers/specs/cas-table-files-as-refs-design
title: 'CAS table-level files as refs and deduplication claims in the ref log'
doc_type: 'design'
---

# CAS table-level files as refs and deduplication claims in the ref log — rev.1 (2026-09-26) {#cas-table-files-as-refs}

Design study, no code. Written to answer three questions before choosing between this and the
cache of `2026-09-26-cas-directory-probes-no-list-design.md` §3.2 (seven revisions, six review
rounds): how much simpler the code becomes, how risky the one-way migration is, and how small the
touch on upstream `MergeTree` code can be kept. CAS-95.1 (`PartFile` shape for paths inside a part)
is independent and needed either way.

Source paths are relative to `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/` and
line numbers refer to `altinity/antalya-26.6` at `8d62c314ec1`. This is a format change: it
revisits decision 4 (format frozen since 26.6.4) by moving to reader generation 2 with a one-way
migration, no compatibility read path.

## 1. Today: two storage models in one namespace {#today}

A namespace holds parts as refs: the ref table (snapshot plus log under `cas/ns/`, resident in
memory as `RefTableRuntime`) maps a ref name to a manifest; the manifest lists files, small ones
inline (`EntryPlacement::Inline`, `Formats/CasPartManifestFormat.h:41`), large ones as blobs.
Everything about parts (existence, listing, rename via `republishRef`, drop via tombstone, GC, fsck,
freeze, the TLA+ model) is built on that one model.

Table-level files use a second model: plain objects under `cas/ns/state/<life_id>/_files/<name>`
(`Formats/CasLayout.h:254`), written with HEAD-before-PUT by `CasPlainObjects`
(`Pool/CasPlainObjects.cpp:44-75`), never content-addressed, listed by a LIST of the prefix. The
files are `format_version.txt` (once per table), `mutation_<n>.txt` (`MergeTreeMutationEntry.cpp:63`,
`:94`, with `WriteMode::Append` of the CSN at `:114`) and `deduplication_logs/<segment>` (rewritten
on every insert: the CA disk cannot append, so `MergeTreeDeduplicationLog` rotates on every record,
`MergeTreeDeduplicationLog.cpp:240`; the CAS transaction carries the old bytes forward,
`ContentAddressedTransaction.cpp:830-851`).

What the second model costs, on this branch (non-test lines mentioning namespace files: 21 in
`ContentAddressedTransaction.cpp`, 15 in `CasLayout.h`, 12 in `ContentAddressedMetadataStorage.cpp`,
8 in `CasPool.cpp`, 15 in `CasPlainObjects.*`, 7 in `CasLayout.cpp`, one each in the janitor and
fsck; 14 gtest files):

- Every directory probe that is not a part or a container needs the LIST (`TableSubdir`,
  `ContentAddressedMetadataStorage.cpp:1702-1712`, `:1895-1904`; the `TableDir` listing `:1851-1855`),
  which is the steady-state half of issue #2439 and the reason spec §3.2 needed a striped cache.
- Rename copies the files with GET and PUT per file next to the `republishRef` loop
  (`ContentAddressedTransaction.cpp:1285-1305`); `moveFile`, `replaceFile`, `unlinkFile` and
  `removeRecursive` of a subdirectory each have a file branch (`:1483-1485`, `:1664`, `:1155-1170`).
- The janitor and fsck classify `_files/` keys separately (`Gc/CasNamespaceJanitor.cpp:88`,
  `Tools/CasFsck.cpp:538`).
- Each insert into a table with `non_replicated_deduplication_window > 0` is two independent write
  rounds: the log segment (HEAD plus PUT of the whole segment) before the part, then the part's own
  publish (`MergeTreeSink.cpp:381-407`).

## 2. Goals and non-goals {#goals}

Goals:

- One storage model per namespace: everything under a table directory is a ref.
- Table-level directory probes and file reads are answered from the resident ref table and cached
  manifests. No LIST, no per-probe GET.
- An insert into a deduplicating table costs no more requests than today, and produces no garbage
  object per insert.
- The touch on upstream `MergeTree` code is one interface extraction with no behavior change plus a
  one-line choice of implementation; `MergeTreeSink` is untouched.
- One-way migration at first writable mount of an upgraded build; an older build refuses to mount a
  migrated pool with a clear error instead of misreading it.

Non-goals:

- A compatibility read path for reader generation 1 pools in the generation 2 build. Migration is
  the only path forward; downgrade is not supported.
- Replicated tables: `ReplicatedMergeTree` deduplicates through Keeper and has no deduplication log
  on disk. Only `format_version.txt` and mutation files apply to it here.
- Changing what `MergeTreeMutationEntry` or `MergeTreeDeduplicationLog` write on non-CAS disks.

## 3. Design {#design}

### 3.1 Table-level files as refs {#file-refs}

**Naming.** A table-level file `<table>/<relative path>` becomes a ref named by its relative path
(`format_version.txt`, `mutation_5.txt`, `tmp_mutation_5.txt`). Ref names with `/` already exist
(`detached/<part>`, `moving/<part>`), so nested names need no new rule; after this change nothing
nested remains at table level because the deduplication log leaves the file system (§3.2).

**Manifest.** One `ManifestEntry` whose `path` is the file's last path component, inline when small,
a blob otherwise (the size rule `PartFolderAccess` already applies to part files). The manifest's
`ref` and `root_namespace_id` are filled as for a part.

**File versus folder.** A ref is a folder ref when its name matches the part grammar
(`looksLikePartDir`, `Parts/PartPathParser.cpp:150`) directly or after the `detached/` or `moving/`
prefix, or when it is a shadow (FREEZE) ref; every other ref is a file ref. `MergeTree` never
creates a table-level file named like a part, and the parser already relies on the same grammar to
find the part component. The alternative, a `file` flag in the manifest, is exact but changes the
manifest codec and costs a manifest fetch on every `existsDirectory` of a non-part name; it is
listed in §7 for the reviewers.

**Operation mapping** (metadata storage and transaction, `ContentAddressedMetadataStorage.cpp` and
`ContentAddressedTransaction.cpp`):

| operation on `<table>/<file>` | today | after |
|---|---|---|
| `existsFile` (`:1500-1505`) | catalog life, GET | `resolveRef` in memory |
| `readFile`, `getFileSize`, in-manifest bytes (`:2040-2052`) | GET | view of the ref, inline bytes or blob |
| `existsDirectory`, `listDirectory` of a subdirectory | LIST | `hasAnyRefWithPrefix`, `listRefs` by prefix (the `DetachedContainer` branch) |
| `listDirectory` of the table dir (`:1842-1861`) | refs plus LIST | refs only |
| `writeFile` Rewrite (`:825-851`) | HEAD, PUT | `publishEntries` of a one-entry manifest |
| `writeFile` Append (`:830-851`) | GET, HEAD, PUT | view, then `repointRef` with the merged bytes (`:406`), conflict surfaced by the ref lane |
| `unlinkFile` (`:1655-1667`) | HEAD, DELETE | `dropRefIfPresent` |
| `moveFile`, `replaceFile` (`:1470-1490`) | GET, PUT, DELETE | `republishRef` |
| `removeRecursive` of a subdirectory (`:1155-1170`) | LIST, DELETE per file | tombstone refs by prefix |
| table rename (`:1285-1305`) | `republishRef` loop plus GET and PUT per file | `republishRef` loop only |
| drop table | `dropNamespace` plus janitor of `_files/` | `dropNamespace` |

`DirShape::TableSubdir` disappears from `classifyDirectory` (`:1608-1617`); `Pool` and
`CasPlainObjects` lose the four namespace-file methods; `Layout` keeps `parseNamespaceFileKey` for
the janitor only (§3.3).

**Semantics preserved.** A file ref is tombstoned and reclaimed by GC like a part; today's plain
object is deleted in place. Overwrite is a repoint of the ref to a new manifest, the same protocol a
part replacement uses, so a concurrent writer is detected by the ref lane's precommit rather than by
an object etag. The callers of append (`MergeTreeMutationEntry::writeCSN` under the table's mutation
lock) serialize their own writes as today.

### 3.2 Deduplication claims in the ref log {#dedup-claims}

**What the log records.** Two new `RefOpKind` values (`Formats/CasRefLogFormat.h:35`):
`ClaimBlockIds = 6` carrying `ref_name` (the part) and `block_ids` (one or more), and
`ReleaseClaims = 7` carrying `ref_name`. The snapshot (`Formats/CasRefSnapshotFormat.h:53`) gains a
`claims` section: the retained claims in insertion order, each `(block_id, ref_name)`. Both are
generation 2 wire changes.

**What the runtime keeps.** Per namespace, an insertion-ordered journal of claims bounded by the
table's window, and an index `block_id → ref_name`. `ClaimBlockIds` appends and trims to the window;
`ReleaseClaims` removes every claim of that part (upstream's `dropPart`, `MergeTreeDeduplicationLog.h:154`,
which is what lets a `DROP PARTITION` followed by a re-insert succeed). A merge does not release
claims, exactly as upstream keeps records of merged parts until the window rotates.

**The upstream seam.** `MergeTreeDeduplicationLog` (`MergeTreeDeduplicationLog.h:134`) becomes the
file-based implementation of a new `IMergeTreeDeduplicationLog` interface with its five methods
(`addPart`, `dropPart`, `load`, `setDeduplicationWindowSize`, `shutdown`), a pure extraction with no
behavior change. `StorageMergeTree::loadDeduplicationLog` (`StorageMergeTree.cpp:1508-1521`) chooses
the implementation: `disk->isContentAddressed()` (`IDisk.h:477`, already present) selects the CAS
implementation, which lives under `ContentAddressed/` and reaches the pool through the disk's
metadata storage. `MergeTreeSink` (`:381-407`), the three `dropPart` callers in `StorageMergeTree`
(`:2535`, `:2657`, `:2924`), `setDeduplicationWindowSize` (`:840`) and `shutdown` (`:383`) are
untouched because they call the interface.

The CAS implementation: `addPart` checks the index under the runtime's `state_mutex`, and when no
block id is claimed appends one `ClaimBlockIds` op through the ref lane, then returns; `dropPart`
appends `ReleaseClaims`; `load` is a no-op (the runtime already recovered the claims);
`setDeduplicationWindowSize` trims the journal. The ordering is upstream's: the claim is durable
before the part is published, and a failed insert leaves the claim, as today.

**Cost per insert.** One ref-log op instead of a segment rewrite: no manifest, no object, no
garbage. The hot-key lane batches ops of one namespace that arrive close together
(`Backend/CasHotKeys`), so the claim and the part's own publish usually share one PUT. The
request-profile gate for this path pins the count.

**Rejected shapes.**

- Atomic claim-and-publish through `IDataPartStorage` and `MergeTreeSink` (claims carried into the
  part's commit, duplicate reported by the commit): stronger semantics, but a CAS-motivated change
  in `MergeTreeSink`'s flow and in the `IDataPartStorage` interface, and a new error path in the
  sink. The interface extraction touches nothing that runs.
- Intercepting writes to `deduplication_logs/` in the CAS transaction and folding them into the
  next part publish: the log write and the part commit are different disk transactions on possibly
  different threads, and a duplicate insert publishes no part at all.
- Deduplication log as a file ref: one manifest per insert, one garbage manifest per insert for GC,
  and the segment still rewritten whole.

### 3.3 Migration and the reader generation {#migration}

`G_BUILD` (`Formats/CasFormat.h:21`) becomes 2. The pool meta's `min_reader_generation`
(`Formats/CasPoolMetaFormat.cpp:155-158`) is the gate: a build refuses a pool whose floor is above
its `G_BUILD` with `UNKNOWN_FORMAT_VERSION`, which is exactly the fail-closed answer for an old
build on a migrated pool.

At the first writable mount of a generation 2 build on a pool with floor 1, after the mount lease is
held and before any table is loaded, for every cataloged `Live` life of this server root:

1. LIST the life's `_files/` prefix (one LIST per table, once).
2. For each `deduplication_logs/<segment>`: GET, parse the records (`ADD`/`DROP`, tab-separated,
   `MergeTreeDeduplicationLog.cpp:32-58`), replay them into `ClaimBlockIds`/`ReleaseClaims` ops in
   segment order. The table's window is a `MergeTree` setting the pool cannot read at mount, so the
   import keeps at most `kMaxImportedClaims` newest claims per table and the table trims to its own
   window at its first `setDeduplicationWindowSize`.
3. For every other file: GET, `publishEntries` of a one-entry manifest under the file's name.
4. DELETE each imported plain object after its ref or claims are durable.
5. When no `_files/` key remains under any life of this server root, compare-and-swap the pool
   meta's floor to 2 (`min_reader_generation` is already written by CAS).

Idempotent by construction: a crash between steps leaves objects that the next mount re-imports;
a ref that already exists with byte-equal content is a no-op publish; step 5 runs only on an empty
prefix. Multi-node pools: namespaces are server-root scoped, so each node migrates its own
namespaces at its own upgrade, and the floor is raised by whichever node finishes first, after which
a not-yet-upgraded node fails closed at mount. Operators upgrade all nodes of a pool in one window;
the error names the required generation.

Dead lives (absent from the catalog) keep their `_files/` debris until the namespace janitor deletes
it (`Gc/CasNamespaceJanitor.cpp:88`); that is the one consumer of `parseNamespaceFileKey` that stays,
and fsck keeps reporting such keys as debris (`Tools/CasFsck.cpp:538`).

### 3.4 What is removed and what stays {#removed}

Removed: `DirShape::TableSubdir` and its two branches; the file branches of `writeFile`,
`unlinkFile`, `moveFile`, `replaceFile`, `removeRecursive` and rename in the transaction; the four
namespace-file methods of `Pool` and `CasPlainObjects` (mountpoint objects stay);
`namespaceFileKey`/`namespaceFilesPrefix` in `Layout`; the whole of spec §3.2 (striped cache) and
CAS-95.2. Stays: `parseNamespaceFileKey` for janitor and fsck; `PartFile` (CAS-95.1); the
mountpoint-object surface.

## 4. Risks, ranked {#risks}

1. **No downgrade.** After the floor is raised, a 26.6.x build cannot mount the pool. Mitigation:
   the gate is explicit and named; the migration is the first action of the upgraded mount, so the
   window in which a pool is half-migrated and readable by an old build is the migration itself,
   during which old builds see only their own unmigrated namespaces.
2. **Migration crash.** Covered by idempotence (§3.3) and by tests 12-14. The one non-idempotent
   step, the floor CAS, is last and conditional on an empty prefix.
3. **Deduplication window across the upgrade.** Imported from the segments, so a retried insert
   spanning the upgrade is still deduplicated. If a segment is unparsable (upstream's `load` ignores
   broken logs), the import skips it with a warning, matching upstream.
4. **Claims journal growth.** Bounded per table by the window (upstream's bound) and per pool by
   `kMaxImportedClaims` at import. Counted in the ref-table weight like rows.
5. **Name-based file/folder split.** A table-level file named like a part would be treated as a
   folder. `MergeTree` creates none; the rule is pinned by a test and by `looksLikePartDir`'s own
   tests. §7 asks for a verdict on the flag alternative.
6. **Upstream drift.** The interface extraction is a fork patch on `MergeTreeDeduplicationLog.h`
   and `StorageMergeTree.cpp` that must be carried across rebases. It is small, has no behavior,
   and is the kind of extraction upstream might accept.
7. **Append conflicts on mutation files.** `repointRef` detects a concurrent writer where the etag
   did before; callers hold the table's mutation lock, as today.

## 5. Tests, failing-first {#tests}

Generation 2 codecs (`gtest_cas_ref_log_format`, `gtest_cas_ref_snapshot_format` or the existing
format suites):

1. `ClaimBlockIds` and `ReleaseClaims` round-trip; a generation 1 decoder rejects them.
2. Snapshot `claims` section round-trip, order preserved, rejected by generation 1.

File refs (`gtest_ca_wiring`, a new `gtest_cas_table_file_refs`):

3. `writeFile`/`readFile`/`existsFile`/`getFileSize` of `format_version.txt` cost no LIST and no
   GET beyond the manifest (`CountingBackend`).
4. Append to `mutation_1.txt` produces the concatenated bytes; a concurrent conflicting append is
   surfaced, not lost.
5. `unlinkFile`, `moveFile` (`tmp_mutation_1.txt` to `mutation_1.txt`), `replaceFile` behave as on
   a local disk; a missing source throws `FILE_DOESNT_EXIST`.
6. `listDirectory` of the table dir returns part names and file names; `existsDirectory` of a file
   ref answers false, of a part answers true.
7. Table rename carries file refs; drop tombstones them; GC reclaims the manifests.

Deduplication (`gtest_cas_dedup_claims`, mirroring the behaviours of
`tests/queries/0_stateless/*deduplication*` for non-replicated tables):

8. `addPart` claims and rejects a duplicate; the claim survives a restart (recovered from the log,
   then from a snapshot).
9. `dropPart` releases; a re-insert after release succeeds; a merge does not release.
10. Window trimming: the oldest claims fall out at `window`, `setDeduplicationWindowSize` shrinks.
11. Request profile: one insert into a deduplicating table issues at most today's request count and
    no manifest for the claim; the claim and the part publish share one hot-key PUT when issued
    back to back.

Migration (`gtest_cas_migration_g2`, integration `test_cas_upgrade_g1_to_g2` on the
`cas-nightly` 26.6.4 image data):

12. A generation 1 pool with three tables, mutation files and two deduplication segments migrates;
    every file is readable, every claim present, no `_files/` key remains, floor is 2.
13. Crash after step 3 for one table (fault injection): the next mount completes the migration; no
    duplicate refs, no lost file.
14. A generation 1 build refuses the migrated pool with `UNKNOWN_FORMAT_VERSION`; a generation 2
    build with an unmigrated sibling server root migrates it independently.
15. Stateless: `INSERT` with `non_replicated_deduplication_window` on a CAS disk deduplicates before
    and after a server restart; `DROP PARTITION` then re-insert succeeds.

## 6. Size and staging {#size}

Estimated: codecs and runtime claims 400-600 lines, file-ref mapping in the metadata storage and
transaction net negative, migration 300-400 lines, upstream extraction about 60 lines, tests the
largest part. Two to three weeks including review and one soak on the ca-soak stand, against about
one week for the cache alternative.

Staging options, for the reviewers: one generation bump with both parts (one migration, one
maintenance window), or generation 2 for file refs and generation 3 for claims (two migrations,
smaller reviews). The recommendation is one bump: the second migration would re-visit every table
for the deduplication segments anyway.

## 7. Questions for review {#questions}

1. Is there a smaller or more elegant way to get one storage model than file refs plus claims?
2. Can the upstream touch be smaller than an interface extraction plus one choice line? Is there
   an existing extension point that avoids even that?
3. Name-based file/folder split versus a manifest flag: which is safer for the paths `MergeTree`
   actually produces, including `tmp_mutation_*`, `detached/`, `moving/`, projections and FREEZE?
4. Migration: is one generation bump with both parts the right staging, and is the floor-raise rule
   (first node to finish raises it) acceptable for multi-node pools?
5. Deduplication ordering: is keeping upstream's claim-before-publish order right, or should the
   claim ride inside the part's own ref transaction despite the `MergeTreeSink` change?
