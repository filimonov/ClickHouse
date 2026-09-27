---
description: 'Design for CAS-95.1: a path inside a resolved part on a CAS disk gets its own directory shape, answered from the part-folder view instead of an S3 LIST of the table-level file prefix. Removes the restart LIST storm of issue #2439 (one LIST per checksum entry per part at load). Rev.10 narrows the spec to this change; the table-level name cache (CAS-95.2) was closed after eight review rounds and lives in git history.'
sidebar_label: 'CAS part-file probes without LIST'
sidebar_position: 12
slug: /superpowers/specs/cas-directory-probes-no-list-design
title: 'CAS part-file directory probes without an S3 LIST'
doc_type: 'design'
---

# CAS part-file directory probes without an S3 LIST — rev.10 (2026-09-27) {#cas-directory-probes-no-list}

Spec for backlog task CAS-95.1 (parent CAS-95, issue
https://github.com/Altinity/ClickHouse/issues/2439). Implementation branch: new branch off
`altinity/antalya-26.6`; this spec lives on `cas-gc-rebuild`.

Rev.1 to rev.9 of this file also carried CAS-95.2, a resident copy of the table-level file names
that would have removed the steady-state LIST of `clearOldTemporaryDirectories` (one per table per
minute). Eight codex review rounds
(`docs/superpowers/reports/2026-09-26-cas-directory-probes-no-list-codex-reviews/review_r{1..8}.md`)
and a separate two-round design study (`2026-09-26-cas-table-files-as-refs-design.md`) did not
converge on a mechanism whose cost was below its benefit; the task is marked doubtful with the
challenges recorded in it, and the last full design is rev.9 (`e2772b9ddad`, closed `8d0bc675c1f`,
corrected `998c89c0975`). Rev.10 keeps only §3.1 of those revisions, which has had no open finding
since round 2.

Source paths are relative to `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/` and
line numbers refer to `altinity/antalya-26.6` at `8d62c314ec1`.

## 1. Problem {#problem}

A CAS disk maps the generic `IDisk` directory probes of `MergeTree` onto pool requests.
`classifyDirectory` (`ContentAddressedMetadataStorage.cpp:1529`) has shapes for a part directory
(`PartDir`) and a projection directory (`ProjectionDir`), but a path `<table>/<part>/<file>` with a
non-projection `file` matches neither and falls through to `parseTableFilePath`, which returns
`TableSubdir` with `tail = <part>/<file>`. `existsDirectory` (`:1702-1712`) then calls
`store()->listNamespaceFiles(*life)`, one S3 LIST of the life's `cas/ns/state/<life_id>/_files/`
prefix (`Formats/CasLayout.h:254`), to find out whether any table-level file starts with that tail.
For a file of a published part the answer is always "no", and the part-folder view already knows it.

The hot caller is `MergeTreeDataPartChecksum::checkSize`
(`src/Storages/MergeTree/MergeTreeDataPartChecksum.cpp:69`), which asks `existsDirectory(name)` for
every checksum entry of every part at load, through `DataPartStorageOnDiskFull::existsDirectory`
(`src/Storages/MergeTree/DataPartStorageOnDiskFull.cpp:89-97`). On the otel.demo stand (26 tables,
1,672 parts) a restart issued 77k LISTs in three minutes, received 537 `503 Slow Down` and failed
139 uploads, 108 of them ref-lane writes (issue #2439, audit F15). The count grows with the number
of part files.

The same fall-through makes `listDirectory` of such a path list the table-level files under the
tail (`:1895-1904`), which for a path inside a part is always empty.

## 2. Goals and non-goals {#goals}

Goals:

- A server restart issues no LIST per part or per part file: the LIST count of a restart is bounded
  by the number of tables (today's `listDirectory` of each table directory at load), never by parts.
- Existing answers stay the same, with one deliberate exception: a nested directory inside a
  resolved part (`<table>/<part>/<dir>` with entries under `<dir>/` in the manifest) answers present
  and lists its children, where today it answers absent and empty. Every other path shape, including
  a part-shaped component whose ref does not resolve, keeps its current branch and answer.
- No fallback path: a failed request propagates.

Non-goals:

- The steady-state LIST of `clearOldTemporaryDirectories` and of table-level subdirectory probes
  (CAS-95.2, doubtful, see the task).
- The catalog GETs of the `detached` probe and of `system.detached_parts` polling (CAS-95.3).
- Shadow (FREEZE) part-file probes, which take the `ShadowIntermediate` branch and are not on the
  restart path.
- The parser rule that takes any first component after the table uuid except `deduplication_logs`
  as the part component (`Parts/PartPathParser.cpp:188-197`), which misparses `partition_exports/`
  and the `tmp/` directory of persistent `Join` and `Set` (CAS-318). This change preserves that
  rule's answers.

## 3. Design {#design}

### 3.1 A `PartFile` directory shape {#part-file-shape}

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
serves a warm `CachedForLoad` view without a request, so the restart probes cost no S3 request: the
view a part's `checkSize` probes need is the one its load already fetched.

`ProjectionDir` stays as it is: it is the same shape with a different prefix rule, and merging the two
is not needed for this change.

**Alternative not taken.** An early part-file branch at the top of `existsDirectory`, mirroring
`existsFileOrDirectory`, is a few lines smaller but needs the same unresolved-ref fall-through, leaves
`listDirectory` listing `_files/` for a path inside a part, and puts a second classification outside
the one switch every other shape uses (codex rounds 1 and 2).

### 3.2 What stays as it is {#unchanged}

`existsDirectory` for `TableDir` (`namespaceStillLogicallyPresent`), the containers, `PartDir`,
`ProjectionDir`, `TableSubdir` for real table-level subdirectories, the shadow and generic branches;
every read, write and listing of table-level files; `CasPlainObjects`; the janitor and fsck.

## 4. Tests, failing-first {#tests}

In `src/Disks/tests/gtest_ca_wiring.cpp` (`classifyDirectoryForTest`) and a new
`src/Disks/tests/gtest_cas_directory_probes.cpp` over `CountingBackend`:

1. Routing: `<table>/<part>/columns.txt`, `<table>/detached/<part>/columns.txt`,
   `<table>/moving/<part>/columns.txt` and the non-Atomic `data/db/tbl/<part>/columns.txt` classify
   as `PartFile`; `<table>/<part>/<proj>.proj` stays `ProjectionDir`; `<table>/deduplication_logs`
   stays `TableSubdir`; a shadow part file stays `ShadowIntermediate`; a `tmp_restore_<part>-XXXXXXXX`
   file path on an Atomic table classifies as `PartFile` (its ref is a temporary part).
2. `existsDirectory` on a published part's file answers false; on a nested directory present in the
   manifest answers true; `listDirectory` of that directory lists its children; `listTotal() == 0`
   across all three, and the only ref resolution is the in-memory one.
3. Unresolved ref: `<table>/custom/sub` with a namespace file `custom/sub/x` answers true from the
   table-subdirectory branch (one LIST, as today), and with no such file answers false; a missing
   part's file answers as before this change; a non-Atomic `data/db/tbl/<part>/f` with no such ref
   takes `liveTreeDirHasChildren`.
4. Load profile: a table with 50 published parts is loaded through the metadata storage's
   `existsDirectory` for every checksum entry of every part; `listTotal()` of the `_files/` prefix is
   zero.

Integration, new module `tests/integration/test_cas_directory_probes` with GC disabled
(`gc_enabled = 0`) so no maintenance LIST is counted:

5. Two restarts of one node, first with 2 tables × 20 parts, then with 2 tables × 200 parts;
   `system.events` `CASRootList` read after all parts are loaded is equal in both restarts.

No `LOGICAL_ERROR` is introduced by this change. The ASan lane runs for the touched suites.

## 5. Acceptance {#acceptance}

- CAS-95 acceptance #1: restart LIST count independent of the part count (test 5).
- Issue #2439 gets before/after `S3ListObjects` numbers from an otel.demo restart. The steady-state
  half of acceptance #2 (`clearOldTemporaryDirectories`) is withdrawn with CAS-95.2; the
  `system.detached_parts` half stays with CAS-95.3.

## 6. Documentation {#documentation}

One sentence in `docs/en/antalya/cas/architecture/read-path.md`: part-level directory probes are
answered from the part manifest. No on-S3 format change, so the CAS format documents and
`NativeFormat.md` are untouched.
