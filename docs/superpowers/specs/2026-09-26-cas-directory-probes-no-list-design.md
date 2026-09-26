---
description: 'Design for CAS-95.1 and CAS-95.2: answer MergeTree directory probes on a CAS disk without an S3 LIST. A path inside a resolved part gets its own directory shape answered from the part-folder view, and the table-level namespace file names are listed once per live table at mount, held for the mount and kept current by the two writers, instead of being listed on every probe. Closes the restart LIST storm of issue #2439. Rev.8 replaces the lazily populated striped cache of rev.3 to rev.7 with population at mount.'
sidebar_label: 'CAS directory probes without LIST'
sidebar_position: 12
slug: /superpowers/specs/cas-directory-probes-no-list-design
title: 'CAS directory probes without an S3 LIST'
doc_type: 'design'
---

# CAS directory probes without an S3 LIST — rev.8 (2026-09-27) {#cas-directory-probes-no-list}

Mini spec for backlog tasks CAS-95.1 and CAS-95.2 (parent CAS-95, issue
https://github.com/Altinity/ClickHouse/issues/2439). CAS-95.3 (the `detached` probe's catalog GETs) is
out of scope. Implementation branch: new branch off `altinity/antalya-26.6`; this spec lives on
`cas-gc-rebuild`. Rev.2 to rev.7 address codex review rounds 1 to 6
(`docs/superpowers/reports/2026-09-26-cas-directory-probes-no-list-codex-reviews/review_r{1,2,3,4,5,6}.md`).
The serialization mechanism changed twice: a version counter (rev.2) missed two interleavings; a
per-runtime mutex (rev.3, rev.4) needed a post-settle rule and a second ledger operation to survive a
remount. Rev.5 takes round 4's suggestion, a fixed bank of striped mutexes in the ledger keyed by the
life, which outlives every runtime and needs no such rule. Rev.5 also counts the held names in the
ref-table budget instead of capping them per table. Round 5 accepted the mechanism (no further
stale-set schedule or lock cycle found); rev.6 folds in its implementation-level findings: admission
re-checked inside the stripe, reset on a failed cache update, deterministic test schedules. Round 6
found no new mechanism defect; rev.7 admits the operation inside the ledger, adds the two regression
tests for the rev.6 branches, and fixes test wording. Rev.8 (2026-09-27) replaces the lazily
populated, striped cache of rev.3 to rev.7 with population at mount, the alternative that round 2 of
the separate design study `2026-09-26-cas-table-files-as-refs-design.md` proposed: when no LIST can
overlap a writer, the stripe, the admission-inside-the-stripe rule, the budget accounting and the
runtime-lifetime argument all disappear.

Source paths are relative to `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/` and
line numbers refer to `altinity/antalya-26.6` at `8d62c314ec1`.

## 1. Problem {#problem}

A CAS disk maps the generic `IDisk` directory probes of `MergeTree` onto pool requests. Two probes
currently cost one S3 LIST each:

**(a) A path inside a part is classified as a table subdirectory.** `classifyDirectory`
(`ContentAddressedMetadataStorage.cpp:1529`) has shapes for a part directory (`PartDir`) and a
projection directory (`ProjectionDir`), but a path `<table>/<part>/<file>` with a non-projection
`file` matches neither and falls through to `parseTableFilePath`, which returns `TableSubdir` with
`tail = <part>/<file>`. `existsDirectory` (`:1702-1712`) then calls `store()->listNamespaceFiles(*life)`,
one S3 LIST of the life's `_files/` prefix, to find out whether any table-level file starts with that
tail. The answer is always "no" for a file of a published part, and the part-folder view already
knows it.

The hot caller is `MergeTreeDataPartChecksum::checkSize`
(`src/Storages/MergeTree/MergeTreeDataPartChecksum.cpp:69`), which asks `existsDirectory(name)` for
every checksum entry of every part at load. On the otel.demo stand (26 tables, 1,672 parts) a restart
issued 77k LISTs in three minutes, received 537 `503 Slow Down` and failed 139 uploads, 108 of them
ref-lane writes (issue #2439, audit F15). The count grows with the number of part files.

**(b) Every table-directory listing lists the namespace files.** `listDirectory` for `TableDir`
(`:1842-1861`) unions the ref names from the in-memory ref table with `listNamespaceFiles(*life)`.
`MergeTreeData::clearOldTemporaryDirectories` iterates the table directory of every table once a
minute looking for `tmp_*` entries, so a warm node pays one LIST per table per minute (155 LISTs
per 10 minutes on otel.demo, audit F16). `existsDirectory`/`listDirectory` on a real table
subdirectory such as `deduplication_logs` (`:1702`, `:1897`), the transaction's subdirectory
removal (`ContentAddressedTransaction.cpp:1164`) and the table rename (`:1295`) pay the same LIST.

The namespace files of one life are stored under `cas/ns/state/<life_id>/_files/`
(`Formats/CasLayout.h:254`); the life id is the namespace incarnation, so a reborn namespace of the
same name has a different prefix. For a cataloged live life the only mutators are
`Pool::putNamespaceFile` and `Pool::removeNamespaceFile` (`Pool/CasPool.cpp:1208`, `:1850`), which
forward to `CasPlainObjects` (`Pool/CasPlainObjects.cpp:44`, `:72`). A live namespace is
`<server_root_id>/store/<u3>/<uuid>@cas@` (`liveNamespace`, `:1399-1404`), and the mount lease admits
one holder of a server root per fence generation, so those two mutators on this node are the only
writers of the prefix while the fence holds. The namespace janitor deletes `_files/` objects of dead
lives only (lives absent from a later catalog cut); fsck and replication do not write there.

## 2. Goals and non-goals {#goals}

Goals:

- A server restart issues no LIST per part or per part file. The LIST count of a restart is bounded
  by the number of tables, not parts.
- A warm node issues no LIST from `clearOldTemporaryDirectories`, and no probe of a table-level
  name or subdirectory issues a LIST after mount, for as long as the mount lease is held.
- Existing answers stay the same, with one deliberate exception: a nested directory inside a
  resolved part (`<table>/<part>/<dir>` with entries under `<dir>/` in the manifest) answers present
  and lists its children, where today it answers absent and empty. Every other path shape, including
  a part-shaped component whose ref does not resolve, keeps its current branch and answer.
- No fallback path: a failed LIST propagates; an unknown write outcome forgets, never guesses.

Non-goals:

- Changing the on-S3 layout. The format is frozen since 26.6.4 (decision 4), and moving the table-level
  files into the ref table (issue #2439, proposal 3) is not taken.
- The catalog GETs of the `detached` probe and of `system.detached_parts` polling (CAS-95.3).
- Shadow (FREEZE) part-file probes, which take the `ShadowIntermediate` branch and are not on the
  restart or steady-state path.
- Dropping the namespace files from the `TableDir` listing. The observed `MergeTree` callers would not
  notice, but `IDisk::listDirectory` is a public answer used by `clickhouse-disks`, backups and
  `system.remote_data_paths`.

## 3. Design {#design}

Two independent changes, delivered as two commits (3.1 first: it alone removes the restart storm).

### 3.1 CAS-95.1: a `PartFile` directory shape {#part-file-shape}

Add `DirShape::PartFile` to the enum in `ContentAddressedMetadataStorage.h:508`. In
`classifyDirectory`, after the `ProjectionDir` check and before the fall-through to
`parseTableFilePath`, a route with a non-empty ref and a non-empty file is a path that MAY be inside a
part. Classification stays pure path computation (no pool I/O, `:149`), so the shape carries both the
route and the table-file parse the old branch would have used:

```cpp
/// A path with a part-shaped component followed by more components: a file or nested directory of
/// a live, detached or moving part IF that ref resolves (shadow is routed above). The parser calls
/// every first component after the table root except `deduplication_logs` the part component
/// (`Parts/PartPathParser.cpp:188-197`), so whether this really is a part is decided by the ref, not
/// the path: `existsDirectory`/`listDirectory` take the old table-subdirectory branch when it does not
/// resolve.
if (r && !r->ref.empty() && !r->file.empty())
{
    dr.shape = DirShape::PartFile;
    dr.p = std::move(p);
    dr.r = std::move(r);
    dr.tf = Cas::parseTableFilePath(path);
    return dr;
}
```

`existsDirectory` answers it as the existing `existsFileOrDirectory` (`:1731-1738`) already does for
the same route, except that an unresolved ref keeps today's branch:

```cpp
case DirShape::PartFile:
{
    auto view = partAccess()->getView(dr.r->refKey(), Cas::Freshness::CachedForLoad);
    if (view)
        return view->hasDirectory(dr.r->file + "/");
    /// Not a part we know: `<table>/custom/sub` and a non-Atomic part-shaped table component answer
    /// exactly as before this shape existed.
    return dr.tf ? tableSubdirExists(*dr.tf) : liveTreeDirHasChildren(path);
}
```

`tableSubdirExists(tf)` is today's `TableSubdir` body (`:1702-1712`) extracted into a helper, used by
both cases; `listDirectory` mirrors it with `tableSubdirChildren(tf)` (`:1895-1904`) and
`listLiveTreeChildren(path)`. With a view, `listDirectory` returns
`view->listChildren(dr.r->file + "/")`, mirroring `ProjectionDir` (`:1888-1893`).

`PartFolderView::hasDirectory(prefix)` (`Parts/PartFolderAccess.cpp:126`) is non-empty
`entryRange(entries, prefix)`, so a plain file answers false and a nested directory answers true.
`CachedPartFolderAccess::getView` (`:154`) resolves the ref in the in-memory ref table first and
serves a warm `CachedForLoad` view without a request, so the restart probes cost no S3 request.
`ProjectionDir` stays as it is.

**Alternative not taken.** An early part-file branch at the top of `existsDirectory`, mirroring
`existsFileOrDirectory`, is a few lines smaller but needs the same unresolved-ref fall-through, leaves
`listDirectory` listing `_files/` for a path inside a part, and puts a second classification outside
the one switch every other shape uses.

### 3.2 CAS-95.2: namespace file names listed once at mount {#namespace-file-names}

**The table.** `Pool` gains a `NamespaceFileTable`: `std::map<NamespaceLifeId, std::set<String>>`
under one `std::mutex`, owned by the mount runtime (`mount_runtime`, the object that tracks the lease
and is reset by `noteLeaseLost` and rebuilt by the remount path, `Pool/CasPool.cpp:1309`). It holds
the table-level file names of every life this node can write. No I/O ever runs under its mutex.

**Population.** Inside `Pool::open`, after the mount lease is held and the ref catalog is readable
and before `open` returns (so before the disk serves any request), and again inside a successful
remount before the fence is re-armed for writers: for every cataloged `Live` life of this server root
(`namespaceFilesLifeIfReadable` over the catalog's namespaces of this root), one
`plain_objects.listNamespaceFiles(life)` and one table entry. A LIST failure propagates and the
mount fails, as any other mount step does. Cost: one LIST per live table per mount, the bound issue
#2439 asks for, and no more than today's restart already pays through `listDirectory` of each table
directory at load.

**Lives born after mount.** A life is born on this node only through the minting resolution
`Pool::namespaceLife(ns)` (`Pool/CasPool.cpp:2021`); files are written only under a `Live` life, and
a life is `Live` from its birth, so no file can exist under a life before this call returns for the
first time. `namespaceLife` therefore installs an empty set for a life absent from the table, under
the mutex, before returning. A life that was `Creating` at mount (a crashed birth) has no files for
the same reason and is covered by the same rule when it is resolved again. Nothing is ever populated
lazily: every life this node writes is in the table from before its first file.

**Read path.** `Pool::listNamespaceFiles(life)` (`:1218`): under the mutex, when the life is in the
table return a sorted copy of its set; otherwise release the mutex and run today's LIST through
`plain_objects.listNamespaceFiles(life)` without installing anything. Misses are exactly the lives
this mount does not own: test fixture lives, `openForDecommission` and read-only opens, which have
no mount runtime and keep today's behaviour unchanged.

**Write path.** `Pool::putNamespaceFile` runs `plain_objects.putNamespaceFile` and then, under the
mutex, inserts the name when the life is in the table. `Pool::removeNamespaceFile` erases the name
under the mutex first and then runs `plain_objects.removeNamespaceFile`. `dropNamespace` erases the
life's entry. So a name is present in the table only between a durable PUT and the start of its
removal: a PUT whose outcome is unknown leaves an object without a name (invisible until the next
mount, overwritten by the next write of that name, reported by fsck as it is today for any
`_files/` object the table does not list), and a DELETE whose outcome is unknown leaves at most an
object without a name. A name without an object cannot arise from a failure.

**Same-name concurrency.** Two operations on the same name of one life (a rewrite racing an unlink)
are not serialized here, exactly as a local disk does not serialize them; `MergeTree` never issues
them (mutation files are written and removed under the table's mutation lock, deduplication
segments are created and dropped by one thread under the log's `state_mutex`, `format_version.txt`
is written once), and the `Log` family holds its table lock across rewrites. Operations on different
names commute. A LIST never runs concurrently with a writer of the same life: population happens
before the disk is served, and misses list lives this mount does not write.

**Fence.** A hit is served while the mount runtime holds the lease, the rule the ref table itself
follows. `noteLeaseLost` clears the table under the mutex, so after the fence trips every call is a
miss and today's LIST refuses through the engine's admission (`Pool/CasPlainObjects.cpp:58-65`).
The remount rebuilds the table from fresh LISTs before writers resume.

**Why this is safe.**

- Single writer per lease: the namespace is server-root scoped, the mount lease admits one holder
  per generation, and every mutator of a live life's `_files/` on this node is one of the two wrapped
  methods, both of which update the table.
- No LIST overlaps a writer of the same life, so there is no interleaving to order and no version,
  stripe or runtime identity to reason about.
- Same trust in LIST as today: the mount-time LIST is the recovery-time cold LIST, the trusted kind
  (decision 2); the hot LISTs it replaces were the untrusted ones.
- Fail-close: unknown write outcomes leave invisible objects, never phantom names; a lost lease turns
  every read into today's refused LIST; a failed population fails the mount.
- Memory: names of this node's live tables, a few strings per table, outside every cache budget.

**Alternatives not taken.**

- A `std::map` inside `CasPlainObjects` or `ContentAddressedMetadataStorage` (round 1): the same
  table in an object without the mount lifetime; it would need its own clear on lease loss.
- A version counter bumped at write start and settle with an install check (rev.2): defeated by PUT
  and DELETE settling out of durable order and by a runtime replaced between LIST and install.
- A mutex inside each `RefTableRuntime` (rev.3, rev.4), then a fixed bank of striped mutexes in the
  ledger keyed by the life (rev.5 to rev.7), each held across the populating LIST and every write of
  that life, with admission re-checked inside the stripe and the names counted in the ref-table
  budget: all of it existed to order a lazily started LIST against concurrent writers and to survive
  the runtime's eviction and remount. Population at mount removes the overlap, and ownership by the
  mount runtime removes the lifetime question.
- Durable directory entries in the ref log (the design study, closed after two rounds): a format
  generation, a resync authority argument and still the same-name serialization.
- One ledger-wide mutex (round 3): serializes the inserts of every deduplicating table behind each
  other's PUT.
- A per-table cap on the held set (rev.4): exempted an oversized table from the warm-node guarantee.

### 3.3 What stays as it is {#unchanged}

- `existsDirectory` for `TableDir` (`namespaceStillLogicallyPresent`), the containers, `PartDir`,
  `ProjectionDir`, shadow and generic branches.
- `getNamespaceFile` (exact-key GET) does not consult the set. `existsFile` on a table-level file
  (`:1504`) keeps its GET.
- `CasPlainObjects` keeps its stateless LIST; only `Pool` gains the runtime-aware wrapper.
- The `DedupLogRotation` request-profile gate (`gtest_cas_namespace_file_request_profile.cpp`) keeps its
  literal counts: its life is a fixture life this mount did not populate, so it lists as today.
- The subdirectory removal (`ContentAddressedTransaction.cpp:1164`) and rename (`:1295`) keep calling
  `listNamespaceFiles`; they now read the held names, which is the same answer the single-writer
  argument gives for a fresh LIST, and their own `removeNamespaceFile`/`putNamespaceFile` calls keep
  the table current.

## 4. Tests {#tests}

Failing-first order, one test per behaviour. Files are named where they exist on the branch.

3.1, in `src/Disks/tests/gtest_ca_wiring.cpp` (`classifyDirectoryForTest`) and a new
`gtest_cas_directory_probes.cpp` over `CountingBackend`:

1. Routing: `<table>/<part>/columns.txt`, `<table>/detached/<part>/columns.txt`,
   `<table>/moving/<part>/columns.txt` and the non-Atomic `data/db/tbl/<part>/columns.txt` classify
   as `PartFile`; `<table>/<part>/<proj>.proj` stays `ProjectionDir`; `<table>/deduplication_logs`
   stays `TableSubdir`; a shadow part file stays `ShadowIntermediate`.
2. `existsDirectory` on a published part's file answers false; on a nested directory present in the
   manifest answers true; `listDirectory` of that directory lists its children; `listTotal() == 0`
   across all three.
3. Unresolved ref: `<table>/custom/sub` with a namespace file `custom/sub/x` answers true from the
   table-subdirectory branch, and with no such file answers false; a missing part's file answers as
   before this change.

3.2, in `src/Disks/tests/gtest_cas_namespace_file_request_profile.cpp` (production-born lives on a
pool opened by `Pool::open`, `CountingBackend`):

4. Population: a pool with three live tables holding files is opened; `listTotal()` counts exactly
   three LISTs of the three `_files/` prefixes during `open`, and `listNamespaceFiles` of each life
   afterwards issues no LIST and returns the names.
5. A `putNamespaceFile` and a `removeNamespaceFile` are visible in the next `listNamespaceFiles`
   without a LIST; the DELETE of the removed name is issued (the erase precedes it).
6. Birth after mount: `namespaceLife(ns)` of a new namespace installs an empty set; the first
   `listNamespaceFiles` issues no LIST; files written afterwards are listed.
7. Miss: a fixture life not owned by the mount lists with one LIST per call and installs nothing
   (the `DedupLogRotation` gate keeps its counts); an `openForDecommission` pool lists as today.
8. Ambiguous PUT (the backend lands the object, then fails every attempt past the policy deadline
   under `FakeClock`): the public call throws and the name is absent from the table; a later write of
   the same name succeeds and the name appears. Ambiguous DELETE: the name is absent from the table
   whether or not the object survived.
9. Fence loss: with a populated table, trip the fence; `listNamespaceFiles` is refused as today and
   serves no cached answer; after `tryRemountOnce` the table is rebuilt with one LIST per live table
   and hits resume.
10. Drop and same-name rebirth: `dropNamespace` erases the entry; the reborn life starts empty and
    does not see the old names.
11. Population failure: a backend that fails the LIST of one prefix makes `Pool::open` throw; no
    partially populated pool is returned.
12. Destructive consumers on a populated table: `removeRecursive` of a table subdirectory
    (`ContentAddressedTransaction.cpp:1164`) removes every file it would have removed with a fresh
    LIST, and a table rename (`:1295`) copies every file, both without a LIST.
13. Non-MergeTree: a `Log` table on the CAS disk (files at table level rewritten in place) inserts,
    restarts and appends with zero LIST after mount, per the probe report
    `docs/superpowers/reports/2026-09-27-cas-non-mergetree-engines-probe.md`.

Integration, new module `tests/integration/test_cas_directory_probes` with GC disabled
(`gc_enabled = 0`) so no maintenance LIST is counted:

14. Two restarts of one node, first with 2 tables × 20 parts, then with 2 tables × 200 parts;
    `system.events` `CASRootList` read after all parts are loaded is equal in both restarts.
15. After the second restart, the delta of `CASRootList` over two minutes without queries or DDL is
    zero (covers `clearOldTemporaryDirectories`).

No `LOGICAL_ERROR` is introduced by this change.
The ASan lane runs for the touched suites.

## 5. Acceptance {#acceptance}

- CAS-95 acceptance #1: restart LIST count independent of the part count (test 14).
- CAS-95 acceptance #2 for the `clearOldTemporaryDirectories` half: zero `CASRootList` on a warm node
  without DDL (test 15). The `system.detached_parts` half stays with CAS-95.3.
- Issue #2439 gets before/after `S3ListObjects` numbers from an otel.demo restart.

## 6. Documentation {#documentation}

One paragraph in `docs/en/antalya/cas/architecture/read-path.md` stating that part-level directory
probes are answered from the part manifest and that a table's file names are listed once per table at
mount and then kept in memory by the writers. No on-S3 format change, so the CAS format documents and
`NativeFormat.md` are untouched.
