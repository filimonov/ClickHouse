---
description: 'Design for CAS-95.1 and CAS-95.2: answer MergeTree directory probes on a CAS disk without an S3 LIST. A part-file path gets its own directory shape answered from the part-folder view, and the table-level namespace file names are held once per namespace runtime with write-through instead of being listed on every probe. Closes the restart LIST storm of issue #2439.'
sidebar_label: 'CAS directory probes without LIST'
sidebar_position: 12
slug: /superpowers/specs/cas-directory-probes-no-list-design
title: 'CAS directory probes without an S3 LIST'
doc_type: 'design'
---

# CAS directory probes without an S3 LIST — rev.1 (2026-09-26) {#cas-directory-probes-no-list}

Mini spec for backlog tasks CAS-95.1 and CAS-95.2 (parent CAS-95, issue
https://github.com/Altinity/ClickHouse/issues/2439). CAS-95.3 (the `detached` probe's catalog GETs) is
out of scope. Implementation branch: new branch off `altinity/antalya-26.6`; this spec lives on
`cas-gc-rebuild`.

Source paths are relative to `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/` and
line numbers refer to `altinity/antalya-26.6` at `8d62c314ec1`.

## 1. Problem {#problem}

A CAS disk maps the generic `IDisk` directory probes of `MergeTree` onto pool requests. Two probes
currently cost one S3 LIST each:

**(a) A part-file path is classified as a table subdirectory.** `classifyDirectory`
(`ContentAddressedMetadataStorage.cpp:1529`) has shapes for a part directory (`PartDir`) and a
projection directory (`ProjectionDir`), but a path `<table>/<part>/<file>` with a non-projection
`file` matches neither and falls through to `parseTableFilePath`, which returns `TableSubdir` with
`tail = <part>/<file>`. `existsDirectory` (`:1702-1712`) then calls `store()->listNamespaceFiles(*life)`,
one S3 LIST of `roots/<ns>/_files/`, to find out whether any table-level file starts with that tail.
The answer is always "no" for a part file, and the part-folder view already knows it.

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
subdirectory such as `deduplication_logs` (`:1702`, `:1897`) and the transaction's subdirectory
removal (`ContentAddressedTransaction.cpp:1164`) and table rename (`:1295`) pay the same LIST.

The namespace files of one life are written only through `CasPlainObjects::putNamespaceFile` and
`removeNamespaceFile` (`Pool/CasPlainObjects.cpp:44`, `:72`), reached through `Pool::putNamespaceFile`
and `Pool::removeNamespaceFile` (`Pool/CasPool.cpp:1208`, `:1850`). A live namespace is
`<server_root_id>/store/<u3>/<uuid>@cas@` (`liveNamespace`, `:1399-1404`), so its files are written
by the node that owns the server root. No GC, fsck or replication path writes under `_files/`.

## 2. Goals and non-goals {#goals}

Goals:

- A server restart issues no LIST per part or per part file. The LIST count of a restart is bounded
  by the number of tables, not parts.
- A warm node issues no LIST from `clearOldTemporaryDirectories`, and repeated probes of the same
  table-level subdirectory issue no LIST after the first.
- Every existing answer stays the same: `existsDirectory` and `listDirectory` return what they return
  today for every path shape, including detached, moving and shadow parts.
- No fallback path: a failed LIST propagates as it does today.

Non-goals:

- Changing the on-S3 layout. The format is frozen since 26.6.4 (decision 4), and moving the table-level
  files into the ref table (issue #2439, proposal 3) is not taken.
- The catalog GETs of the `detached` probe and of `system.detached_parts` polling (CAS-95.3).
- Shadow (FREEZE) part-file probes, which take the `ShadowIntermediate` branch and are not on the
  restart or steady-state path.

## 3. Design {#design}

### 3.1 CAS-95.1: a `PartFile` directory shape {#part-file-shape}

Add `DirShape::PartFile` to the enum in `ContentAddressedMetadataStorage.h:508`. In
`classifyDirectory`, after the `ProjectionDir` check and before the fall-through to
`parseTableFilePath`, a route with a non-empty ref and a non-empty file is a path inside a part:

```cpp
/// A path INSIDE a part (live, detached or moving; shadow is routed above): a file or a nested
/// directory of the part folder. The part-folder view answers it; a LIST cannot.
if (r && !r->ref.empty() && !r->file.empty())
{
    dr.shape = DirShape::PartFile;
    dr.p = std::move(p);
    dr.r = std::move(r);
    return dr;
}
```

`existsDirectory` answers it exactly as the existing `existsFileOrDirectory` (`:1731-1738`) already
does for the same route:

```cpp
case DirShape::PartFile:
{
    auto view = partAccess()->getView(dr.r->refKey(), Cas::Freshness::CachedForLoad);
    return view && view->hasDirectory(dr.r->file + "/");
}
```

`PartFolderView::hasDirectory(prefix)` (`Parts/PartFolderAccess.cpp:126`) is non-empty
`entryRange(entries, prefix)`, so a plain file answers false and a nested directory answers true. A
missing ref (no view) answers false, as `ProjectionDir` does.

`listDirectory` returns `view ? view->listChildren(dr.r->file + "/") : {}`, mirroring `ProjectionDir`
(`:1888-1893`). Today the same path returns the table-level files under the tail, which for a part
path is always empty, so the visible answer for a real nested directory becomes correct rather than
different.

`ProjectionDir` stays as it is: it is the same shape with a different prefix rule, and merging the two
is not needed for this change.

### 3.2 CAS-95.2: namespace file names held per ref-table runtime {#namespace-file-names}

**Where the names live.** `CasRefLedger::RefTableRuntime` (`Pool/CasRefLedger.h:746`) is created per
namespace life under one admitted fence generation and is never rebound: a lost fence, a remount, a
drop or a same-name rebirth all produce a distinct runtime object. That is exactly the lifetime the
cached names need, so the runtime gains one field guarded by its existing `state_mutex`:

```cpp
/// Table-level file names of THIS life, as of one LIST plus this node's own writes since. Empty
/// optional: not yet listed, or forgotten after a write whose outcome is unknown.
std::optional<std::set<String>> namespace_file_names;
/// Bumped by every write-through, populated or not, so a LIST that raced a write is not installed.
uint64_t namespace_file_names_version = 0;
```

The runtime lookup (`lookupRefTableRuntime`, `Pool/CasRefLedger.h:1036`) is private to the ledger, so
the ledger exposes three small methods and `Pool` stays the only caller: `heldNamespaceFileNames(life)`
(the sorted names, or `nullopt` with the current version), `installNamespaceFileNames(life, version,
names)` and `noteNamespaceFileWrite(life, write, on_success)`.

**Read path.** `Pool::listNamespaceFiles(life)` (`Pool/CasPool.cpp:1218`) becomes:

1. `heldNamespaceFileNames(life)`: when the runtime for `life.ns` exists, its `life` equals the
   argument and the set is populated, return the sorted names (no request).
2. Otherwise run today's LIST through `plain_objects.listNamespaceFiles(life)` outside any mutex, then
   `installNamespaceFileNames(life, version_before, names)`, which installs only when the runtime
   still matches the life and the version is unchanged. A LIST that raced a write returns its own
   result but installs nothing; the next call lists again. A LIST failure propagates and installs
   nothing.

Lives without a runtime (test fixture lives, a namespace listed by an offline tool) keep listing every
time, which is today's behaviour.

**Write-through.** `Pool::putNamespaceFile` and `Pool::removeNamespaceFile` become:

```cpp
void Pool::putNamespaceFile(const NamespaceLifeId & life, const String & name, const String & bytes)
{
    ref_ledger.noteNamespaceFileWrite(life, [&] { plain_objects.putNamespaceFile(life, name, bytes); },
        /*on_success=*/[&](std::set<String> & names) { names.insert(name); });
}
```

`noteNamespaceFileWrite` bumps the version, runs the write outside the mutex, then under the mutex
applies `on_success` to the set when it is populated and the runtime still matches the life. If the
write throws, the set is reset (`namespace_file_names.reset()`) before the exception propagates: an
ambiguous PUT or DELETE may have landed, and forgetting makes the next read list again instead of
serving a guess. `removeNamespaceFile` erases the name the same way.

**Why this is safe.**

- Single writer: the namespace is server-root scoped (`liveNamespace`), only `Pool::putNamespaceFile`
  and `Pool::removeNamespaceFile` write under `_files/`, and every such write on this node goes through
  the write-through. A second node cannot hold the same server root: the mount lease admits one holder
  per generation, and a lost lease fences every request of the old holder.
- Same trust in LIST as today: the cached set is the answer of one real LIST plus known writes. Today's
  repeated LISTs are not a stronger guarantee; each answers from the same store.
- Lifetime by construction: the set dies with the runtime. No invalidation code is added for drop,
  rebirth, fence loss or remount because the runtime already handles each.
- Fail-close: an unknown write outcome forgets the set; a LIST failure installs nothing; a probe on a
  fenced pool never reaches the set because `existsDirectory`/`listDirectory` check admission first
  (`:1626`, `:1804`) and `CasPlainObjects` admits an operation for the LIST.

**Alternative placement, not chosen.** A `std::map<RootNamespace, {incarnation, std::set<String>}>`
inside `CasPlainObjects` under a new mutex. It keeps the ledger untouched but needs its own eviction
on `dropNamespace` and its own clear on remount, and `CasPlainObjects` is documented as owning no
mutex and no pool back-reference. Two lifecycles for one fact is the weaker design.

### 3.3 What stays as it is {#unchanged}

- `existsDirectory` for `TableDir` (`namespaceStillLogicallyPresent`) and for the containers, and the
  `PartDir`, `ProjectionDir`, shadow and generic branches.
- `getNamespaceFile` (exact-key GET) does not consult the set. `existsFile` on a table-level file
  (`:1504`) keeps its GET.
- `CasPlainObjects` keeps its stateless LIST; only `Pool` gains the runtime-aware wrapper.
- The `DedupLogRotation` request-profile gate (`gtest_cas_namespace_file_request_profile.cpp`) keeps its
  literal counts: its life is a fixture life without a runtime.

## 4. Tests {#tests}

Failing-first order, one test per behaviour:

1. `gtest_ca_wiring` (`classifyDirectoryForTest`): `<table>/<part>/columns.txt` and
   `<table>/detached/<part>/columns.txt` classify as `PartFile`; `<table>/<part>/<proj>.proj` still
   classifies as `ProjectionDir`; `<table>/deduplication_logs` still classifies as `TableSubdir`.
2. `existsDirectory` on a published part's file answers false and on a nested directory inside the
   manifest answers true, with `CountingBackend::listTotal() == 0` across the probes.
   `listDirectory` of the nested directory lists its children with zero LISTs.
3. `gtest_cas_namespace_file_request_profile`: on a production-born life (runtime present) two
   `listNamespaceFiles` calls cost one LIST; a `putNamespaceFile` and a `removeNamespaceFile` between
   them are visible in the second answer without a LIST; a drop and same-name rebirth lists again.
4. Race: a backend hook that performs a `putNamespaceFile` while the first LIST is in flight; the
   installed answer either contains the name or the next call lists again (version check).
5. Write failure: a backend that fails the PUT after the version bump; the next `listNamespaceFiles`
   issues a LIST.
6. Integration (`tests/integration/test_cas_s3` or a new module next to it): two tables with 200
   parts each, restart, `system.events` `CASRootList` after load is at most a small constant times
   the table count; over the following two minutes the delta of `CASRootList` is zero.

The LOGICAL_ERROR expectations, if any are added, are death tests. The ASan lane runs for the touched
suites.

## 5. Acceptance {#acceptance}

- CAS-95 acceptance #1: restart LIST count independent of the part count (integration test 6).
- CAS-95 acceptance #2 for the `clearOldTemporaryDirectories` half: zero `CASRootList` on a warm node
  without DDL (integration test 6). The `system.detached_parts` half stays with CAS-95.3.
- Issue #2439 gets before/after `S3ListObjects` numbers from an otel.demo restart.

## 6. Documentation {#documentation}

One paragraph in `docs/en/antalya/cas/architecture/read-path.md` stating that part-level directory
probes are answered from the part manifest and that table-level file names are listed once per table
life on a node and then kept in memory. No on-S3 format change, so the CAS format documents and
`NativeFormat.md` are untouched.
