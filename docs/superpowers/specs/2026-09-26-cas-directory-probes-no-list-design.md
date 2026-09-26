---
description: 'Design for CAS-95.1 and CAS-95.2: answer MergeTree directory probes on a CAS disk without an S3 LIST. A path inside a resolved part gets its own directory shape answered from the part-folder view, and the table-level namespace file names are held per resident ref-table runtime with write-through instead of being listed on every probe. Closes the restart LIST storm of issue #2439. Rev.3 folds in codex review rounds 1 and 2.'
sidebar_label: 'CAS directory probes without LIST'
sidebar_position: 12
slug: /superpowers/specs/cas-directory-probes-no-list-design
title: 'CAS directory probes without an S3 LIST'
doc_type: 'design'
---

# CAS directory probes without an S3 LIST — rev.3 (2026-09-26) {#cas-directory-probes-no-list}

Mini spec for backlog tasks CAS-95.1 and CAS-95.2 (parent CAS-95, issue
https://github.com/Altinity/ClickHouse/issues/2439). CAS-95.3 (the `detached` probe's catalog GETs) is
out of scope. Implementation branch: new branch off `altinity/antalya-26.6`; this spec lives on
`cas-gc-rebuild`. Rev.2 and rev.3 address codex review rounds 1 and 2
(`docs/superpowers/reports/2026-09-26-cas-directory-probes-no-list-codex-reviews/review_r{1,2}.md`).
Rev.3 replaces the version-counter protocol of rev.2 with one per-runtime mutex: round 2 found two
more interleavings the counter missed, and a third patch on one mechanism is the signal to change the
invariant instead.

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
- A warm node issues no LIST from `clearOldTemporaryDirectories`, and repeated probes of the same
  table-level subdirectory issue no LIST while the table's ref-table runtime stays resident.
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

### 3.2 CAS-95.2: namespace file names held per resident ref-table runtime {#namespace-file-names}

**Where the names live.** `CasRefLedger::RefTableRuntime` (`Pool/CasRefLedger.h:746`) is created per
namespace life under one admitted fence generation and never rebound: a remount detaches every
runtime (`quiesceRefTablesForRemount`, `Pool/CasRefLedger.cpp:1817-1865`), a drop ends it, a same-name
rebirth is a different object with its own `runtime_id`, and the cache budget may evict an idle one
(`enforceRefTableCacheBudget`, `:1736-1813`; a runtime some caller still holds a `shared_ptr` to is
never evicted). Those are the lifetimes the cached names get, so the runtime gains two fields:

```cpp
/// Serializes every namespace-file operation of THIS life on this node: the populating LIST, each
/// PUT and each DELETE, and the cache hit. Separate from `state_mutex` so ref-table work never waits
/// on namespace-file I/O. Held across the request on purpose: namespace-file writes of one table are
/// rare (dedup-log rotation, `format_version.txt` at create, rename) and already serialized by their
/// callers, and serializing them with the LIST is what makes the set exact without a version protocol.
std::mutex namespace_files_mutex;
/// Table-level file names of THIS life, as of one LIST plus this node's settled writes since, both
/// applied under `namespace_files_mutex`. Empty optional: not yet listed, or forgotten after a write
/// whose outcome is unknown. Guarded by `namespace_files_mutex`.
std::optional<std::set<String>> namespace_file_names;
```

The runtime lookup (`lookupRefTableRuntime`, `Pool/CasRefLedger.h:1036`) is private to the ledger, so
the ledger exposes one method and `Pool` stays its only caller:

- `lockNamespaceFiles(life, fence_generation)`: when the runtime for `life.ns` exists, its `life`
  equals the argument and its `admitted_fence_generation` equals `fence_generation`, returns a guard
  that owns a `shared_ptr` to that runtime and a `unique_lock` on its `namespace_files_mutex`, with
  access to `namespace_file_names`; otherwise `nullopt`. Holding the `shared_ptr` across the operation
  keeps the runtime non-evictable for its duration (`use_count() != 1`, `:1770`), so an operation
  always finishes against the object it started on; a runtime detached by remount meanwhile is simply
  a dead object that nothing reads again. Lock order: the lookup takes and releases `ref_queue_mutex`
  before the guard locks `namespace_files_mutex`; nothing under `namespace_files_mutex` takes
  `state_mutex` or `ref_queue_mutex`, and `CasPlainObjects` has no path back into the ledger, so the
  I/O held under it cannot deadlock with recovery, eviction or remount.

**Read path.** `Pool::listNamespaceFiles(life)` (`Pool/CasPool.cpp:1218`) becomes:

1. `CasOperation op = mount_requests.admit()`; `guard = ref_ledger.lockNamespaceFiles(life,
   op.generation())`.
2. Hit: `op.admitted()` and `guard` and the set is populated: return a sorted copy, no request. The
   admission check at the disk layer (`:1626`, `:1804`) is not enough on its own because the fence can
   drop between it and the read; today's LIST would then be refused by the engine, and the hit is
   refused the same way (it falls to step 3, whose LIST refuses).
3. Miss: run today's LIST through `plain_objects.listNamespaceFiles(life)` while still holding the
   guard; on success, when `guard` exists, install the names. A LIST failure propagates and installs
   nothing. Without a guard (no runtime for this life, or another fence generation) the call lists
   every time, which is today's behaviour for fixture lives and offline tools.

**Write path.** `Pool::putNamespaceFile` and `Pool::removeNamespaceFile` take the same guard before
the request, run the `plain_objects` call while holding it, and on success insert or erase the name
in a populated set. Because the LIST, every PUT and every DELETE of one life on this node run under
one mutex, the set is updated in durable order: there is no interleaving to reason about, and no
version counter.

**Failure rule.** A write that throws may have landed (an ambiguous PUT or DELETE), so the set is
reset before the exception propagates and the next read lists again. Nothing is guessed.

**A write that begins without a runtime.** A delayed write buffer captures only the life
(`ContentAddressedTransaction.cpp:838-851`), so its `putNamespaceFile` may run after the runtime was
evicted and before a new one exists; a runtime created during that write could LIST and miss the
object. Therefore a write whose guard lookup found no runtime re-looks-up after it settles (success or
failure) and, if a runtime for the life now exists, resets its set under the mutex. The reset happens
after any LIST that overlapped, because that LIST held the mutex until it installed.

**Memory.** The set holds the names of one table's files: `format_version.txt`, the dedup-log
segments (bounded by the deduplication window) and the table-level mutation files. It is the same
order as the ref names the runtime already holds for that table and is not added to the ref-table
byte budget, which measures snapshot and log-tail bytes (`weightOf`, `:1751`) and is enforced only
after recovery (`:1671`); no separate cap is introduced.

**Why this is safe.**

- Single writer per fence generation: the namespace is server-root scoped, the mount lease admits one
  holder per generation, every mutator of a cataloged live life's `_files/` on this node is one of the
  two wrapped methods, and a hit is served only under an operation admitted on the runtime's own
  generation.
- Exact set: the populating LIST and every write of the life on this node are totally ordered by
  `namespace_files_mutex`, and each write's effect is applied under the same critical section that
  performed it.
- Same trust in LIST as today: the cached set is the answer of one real LIST plus settled writes. A
  repeated LIST is not a stronger guarantee.
- Lifetime by construction: the set dies with the runtime (remount, drop, rebirth, eviction). No
  invalidation code is added. Eviction costs one LIST on the next access, which is why §2 promises
  "while the runtime stays resident" and not "once per life".
- Fail-close: unknown write outcome forgets the set; LIST failure installs nothing; lost fence refuses
  the hit.

**Cost of the mutex.** A `listDirectory` of a table directory waits for an in-flight namespace-file
write of the same table, and namespace-file writes of one table wait for each other. The callers
already serialize these writes (`MergeTreeDeduplicationLog` rotates under its `state_mutex`), and
`clearOldTemporaryDirectories` waiting on one table's rotation is bounded by that one request.

**Alternatives not taken.** A `std::map<RootNamespace, {incarnation, std::set<String>}>` inside
`CasPlainObjects`, `Pool` or `ContentAddressedMetadataStorage`: each needs its own eviction on drop
and its own clear on remount and fence loss; review round 1 confirmed none is smaller. A version
counter bumped at write start and settle with an install check (rev.2): round 2 found that concurrent
PUT and DELETE settling out of durable order and a runtime replaced between LIST and install both
defeat it, and closing them needs a runtime token, in-flight counting and a forget-on-transition rule.

### 3.3 What stays as it is {#unchanged}

- `existsDirectory` for `TableDir` (`namespaceStillLogicallyPresent`), the containers, `PartDir`,
  `ProjectionDir`, shadow and generic branches.
- `getNamespaceFile` (exact-key GET) does not consult the set. `existsFile` on a table-level file
  (`:1504`) keeps its GET.
- `CasPlainObjects` keeps its stateless LIST; only `Pool` gains the runtime-aware wrapper.
- The `DedupLogRotation` request-profile gate (`gtest_cas_namespace_file_request_profile.cpp`) keeps its
  literal counts: its life is a fixture life without a runtime.
- The subdirectory removal (`ContentAddressedTransaction.cpp:1164`) and rename (`:1295`) keep calling
  `listNamespaceFiles`; they now read the held names when present, which is the same answer the
  single-writer argument gives for a fresh LIST.

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

3.2, in `src/Disks/tests/gtest_cas_namespace_file_request_profile.cpp` (production-born life, runtime
present; the concurrent cases subclass `CountingBackend`, override `write`/`remove`/`list`, call the
base to land the request and then block on a `ManualBarrier` before returning, the pattern the file's
existing cases use):

4. Two `listNamespaceFiles` calls cost one LIST; a `putNamespaceFile` and a `removeNamespaceFile`
   between them are visible in the second answer without a LIST.
5. LIST in flight, PUT from another thread: the PUT does not start until the LIST installed (mutex);
   after both returned, `listNamespaceFiles` returns a set containing the name, with no further LIST.
6. PUT in flight (landed, not returned), LIST from another thread: the LIST waits; after both
   returned the set contains the name and no further LIST is issued.
7. Concurrent PUT and DELETE of one name: the second waits for the first; the final set equals the
   final storage state in both orders.
8. Ambiguous PUT and ambiguous DELETE (the backend lands the object, then throws): the next
   `listNamespaceFiles` issues a LIST and returns the true state.
9. Fence loss: with a populated set, trip the fence; `listNamespaceFiles` is refused as today (no
   cached answer served). After remount and a fresh runtime, the first call lists again.
10. Eviction: with `ref_table_cache_bytes` set so the runtime is evicted, the next call lists again.
11. Drop and same-name rebirth: the reborn life lists again and does not see the old names.
12. Write without a runtime: evict the runtime, start a PUT that blocks after landing, touch the
    namespace so a new runtime exists and `listNamespaceFiles` installs a set without the name, release
    the PUT; the next `listNamespaceFiles` issues a LIST and contains the name.
13. Destructive consumers on a warmed set: `removeRecursive` of a table subdirectory
    (`ContentAddressedTransaction.cpp:1164`) removes every file it would have removed with a fresh
    LIST, and a table rename (`:1295`) copies every file, both without a LIST.

Integration, new module `tests/integration/test_cas_directory_probes` with GC disabled
(`gc_enabled = 0`) so no maintenance LIST is counted:

14. Two restarts of one node, first with 2 tables × 20 parts, then with 2 tables × 200 parts;
    `system.events` `CASRootList` read after all parts are loaded is equal in both restarts.
15. After the second restart, the delta of `CASRootList` over two minutes without queries or DDL is
    zero (covers `clearOldTemporaryDirectories`).

No `LOGICAL_ERROR` is introduced by this change. The ASan lane runs for the touched suites.

## 5. Acceptance {#acceptance}

- CAS-95 acceptance #1: restart LIST count independent of the part count (test 14).
- CAS-95 acceptance #2 for the `clearOldTemporaryDirectories` half: zero `CASRootList` on a warm node
  without DDL (test 15). The `system.detached_parts` half stays with CAS-95.3.
- Issue #2439 gets before/after `S3ListObjects` numbers from an otel.demo restart.

## 6. Documentation {#documentation}

One paragraph in `docs/en/antalya/cas/architecture/read-path.md` stating that part-level directory
probes are answered from the part manifest and that a table's file names are listed once per resident
ref-table runtime on a node and then kept in memory with write-through. No on-S3 format change, so
the CAS format documents and `NativeFormat.md` are untouched.
