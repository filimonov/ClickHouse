---
description: 'Design for CAS-95.1 and CAS-95.2: answer MergeTree directory probes on a CAS disk without an S3 LIST. A path inside a resolved part gets its own directory shape answered from the part-folder view, and the table-level namespace file names are listed once per live table at mount, held for the mount and kept current by the two writers, instead of being listed on every probe. Closes the restart LIST storm of issue #2439. Rev.9 folds in codex review round 7 on the mount-time population of rev.8.'
sidebar_label: 'CAS directory probes without LIST'
sidebar_position: 12
slug: /superpowers/specs/cas-directory-probes-no-list-design
title: 'CAS directory probes without an S3 LIST'
doc_type: 'design'
---

# CAS directory probes without an S3 LIST — rev.9, §3.2 closed (2026-09-27) {#cas-directory-probes-no-list}

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
runtime-lifetime argument all disappear. Rev.9 folds in round 7 (`review_r7.md`): the table is
generation-tagged and every hit, birth and population is admitted on the fence under the mutex; the
rebuilding LIST waits for in-flight namespace-file requests of the lost generation and, after an
unclean predecessor, for one attempt envelope; lives are enumerated from one fresh catalog cut;
decommission and read-only opens never populate. Round 8 (`review_r8.md`) then found three
CRITICAL defects in the quiescence before the rebuilding LIST, and the loop for §3.2 was closed: see
§0 below. §3.1 (CAS-95.1) is unaffected and goes to implementation alone.

## 0. Decision after eight review rounds: §3.1 only {#decision}

Every variant of a resident copy of the `_files/` names (lazy cache with a version counter, a mutex
per runtime, striped mutexes, durable directory entries in the ref log, population at mount) ran
into the same root fact: a `_files/` PUT or DELETE is a plain object write with no seal, and the
backend gives no bound on when it applies a request it has already accepted. After an unclean
crash of this node's previous incarnation, a DELETE it issued can land after the new incarnation's
LIST, and a name then stays in the copy while the object is gone until the next mount; a
`mutation_<n>.txt` in that state makes the table fail to load.

Correction (2026-09-27, user review): the mount-lease protocol already bounds this in time. The
predecessor's engine reserves one attempt envelope against its lease deadline before every attempt
it starts (`Backend/CasRequests.cpp:288`, `:432`, `:451-453`), so none of its attempts is still
running client-side at the deadline; and a successor reclaims an unclean slot only after observing
the lease lapse for a full TTL plus 5% (`claimMountAwaitingExpiry`,
`Pool/CasServerRoot.cpp:940-968`, default TTL 30 s). What remains is only the object store applying
a request after the client gave up on it, the assumption the ref log's `EpochSeal` was introduced
to remove by proof rather than by time (`Pool/CasPool.cpp:802-817`), and which every other unsealed
object of the pool (the mount slot, mountpoint objects, `_files/` today) already lives with. Whether
that assumption is acceptable for a resident copy is a design decision, not a proof gap; with it
accepted, the cross-process half of the round 8 finding is covered by the lease protocol, and the
open items are the in-process gate (one `std::shared_mutex`: namespace-file operations shared across
admission, table mutation and request; population at remount exclusive), synchronous lease renewal
during a long population, the catalog ambiguity check and the exact `dropNamespace` erasure. The
decision below stands until the user reopens CAS-95.2 on that basis. In-process, a deferred write-buffer
finalize created before a fence loss admits a fresh operation under the new generation and can
overlap the rebuilding LIST (`ContentAddressedTransaction.cpp:805-852`); closing that needs a gate
spanning admission, the table mutation and the whole request, which is the striped mutex of rev.5 to
rev.7 again. Today's code has the same windows but heals at the next LIST; a resident copy does not.

The only designs immune to this are the present one (a LIST per probe), a copy that is disabled for
any mount over an unclean predecessor (which forfeits the steady-state saving exactly after a crash),
or sealed table files, that is, table-level files as refs under a new format generation, which the
closed design study `2026-09-26-cas-table-files-as-refs-design.md` rejected for its own reasons.

Decision: implement §3.1 (CAS-95.1) now; it removes the restart storm of issue #2439 (77k LISTs,
`503`s, failed ref-lane uploads) and has no open finding since round 2. Leave the steady-state LIST
of `clearOldTemporaryDirectories` (one per table per minute, 155 per 10 minutes on otel.demo, about
0.26 requests per second) as it is, and close CAS-95.2 as "not worth its mechanism" with a pointer
to this section; reopen it only together with sealed table files. §3.2 below is kept as the record
of the last design and of what it would still need.

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

**The table.** `Pool` gains a `NamespaceFileTable`: the fence generation it was built under, and
`std::map<NamespaceLifePhysicalId, std::set<String>>` (the incarnation is pool-unique,
`Primitives/CasNamespaceLifeId.h:87`, and `NamespaceLifeId` has no ordering), under one `std::mutex`.
It holds the table-level file names of every life this node can write. No I/O ever runs under its
mutex. It exists only on the ordinary writable disk mount: `PoolConfig` gains `namespace_file_table`,
set by `ContentAddressedMetadataStorage::openPoolView` for a writable open and false for read-only
opens (`Pool/CasPool.cpp:558-565` skips `mountWritable`) and for `openForDecommission` (`:904`), which
shares `mountWritable` and the remount path; both keep today's LIST on every call.

**Admission.** Every use of the table takes `mount_requests.admit()` first and, under the mutex,
requires `op.admitted()` and `op.generation()` equal to the table's generation: a hit, the empty
entry installed at birth, and the swap-in of a freshly built table. A table of another generation is
never read or written; it is dropped. This is the engine's own fence check (deadline included,
`Backend/CasRequests.h:258`), not the disk layer's `checkOpAdmitted`, which accepts a `Live`
lifecycle without looking at the lease deadline (`ContentAddressedMetadataStorage.cpp:1242-1280`).

**Population.** Inside `mountWritable` (`Pool/CasPool.cpp:844-901`) after `armMountFence` and before
the pool is returned, and inside the remount path (`:1500-1560`) after `arm_fence` and before
`publish_live`, that is, at a point where the fence is armed and no writer of this pool can run yet:

1. Quiescence (§ below).
2. One fresh `CasRefCatalog::read` under an operation admitted on the armed generation. Select the
   rows with `state == Live` whose namespace starts with `<server_root_id>/` and whose table segment
   ends in `@cas@` (`liveNamespace`, `ContentAddressedMetadataStorage.cpp:1399-1404`; this excludes
   the FREEZE shadow namespaces of `shadowNamespace`, `:1411-1416`, which never hold table-level
   files), and build each `NamespaceLifeId::fromCatalogEntry(ns, incarnation)` directly. No
   `namespaceFilesLifeIfReadable` and no ref-table recovery: the catalog read is the one request.
3. For each selected life, `plain_objects.listNamespaceFiles(life)` (one logical enumeration; the
   physical request count is one per page of names, `Pool/CasPlainObjects.cpp:54-69`), into a
   temporary table built outside the mutex.
4. Swap the temporary table in under the mutex, under the admission rule. A LIST failure, a lost
   admission or a failed swap fails the mount (or the remount attempt) with the exception; the
   previous table, if any, was already dropped at the start of the attempt, so no partial table is
   ever visible.

Cost: one logical LIST per live table per mount, the bound issue #2439 asks for, and no more than
today's restart already pays through `listDirectory` of each table directory at load. Memory: the
full names of all table-level files of this node's live tables (mutation files, `Log` column files),
outside every cache budget and bounded by what the tables actually hold.

**Quiescence before the rebuilding LIST.** `_files/` writes carry no seal; a PUT or DELETE still in
flight when the LIST runs can land after it and leave the table naming a missing object (a late
DELETE) or missing a present one (a late PUT). Two sources, two rules:

- In-process, the requests this pool issued under the lost generation: `Pool` counts in-flight
  namespace-file requests (incremented before each `plain_objects` call in `putNamespaceFile`,
  `removeNamespaceFile` and the miss path of `listNamespaceFiles`, decremented when it settles); the
  remount path waits for zero after `quiesce_ref_tables` and before step 2. The engine refuses those
  requests once the fence is lost, so the wait is one attempt at most.
- Cross-process, the requests of a dead predecessor incarnation of this root: for a claim whose
  `MountPriorState` is `Fenced`, `UncleanObserved` or `UncleanUnsafe` (`:823-829`, no proof of a
  clean death), population waits one `attemptEnvelopeMs()` (`Backend/CasBackend.h:244`, what one
  physical attempt can cost end to end) after the claim before step 2; a dead process cannot retry,
  so after one envelope nothing of it can still land. `Clean` and `None` wait nothing. This
  reinstates for `_files/` the wait the ref log no longer needs (`:802-817`), for the stated reason
  that ref-log writes are sealed and `_files/` writes are not.

**Lives born after mount.** Every `_files/` write on this node obtains its life from
`Pool::namespaceLife(ns)` first (`ContentAddressedTransaction.cpp:836-851`, `:1293-1301`,
`:1472-1485`), and `CasRefLedger::namespaceLife` returns a life only once it is `Live`
(`Pool/CasRefLedger.cpp:4919-4969`). A life may also be born by ref publication
(`acquireMutableRefTableRuntime`, `:678-688`), but no `_files/` object can exist under any life
before the first `Pool::namespaceLife` call for it on this node. `Pool::namespaceLife` therefore
installs an empty set for a life absent from the table, under the admission rule. Nothing is ever
populated lazily.

**Read path.** `Pool::listNamespaceFiles(life)` (`:1218`): admit, lock, and when the life is in the
table of the admitted generation return a sorted copy; otherwise unlock and run today's LIST through
`plain_objects.listNamespaceFiles(life)` (counted in flight) without installing anything. Misses are
the lives this mount does not own: test fixture lives, decommission and read-only opens, and every
call on a pool whose table is of another generation, which the engine then refuses.

**Write path.** `Pool::putNamespaceFile` runs `plain_objects.putNamespaceFile` (counted) and then,
under admission and the mutex, inserts the name when the life is in the table.
`Pool::removeNamespaceFile` erases the name under admission and the mutex first, then runs
`plain_objects.removeNamespaceFile` (counted). `dropNamespace` erases the life's entry. So a name is
present only between a durable PUT and the start of its removal: a PUT whose outcome is unknown
leaves an object without a name (invisible until the next mount's LIST, overwritten by the next
write of that name), a DELETE whose outcome is unknown leaves at most an object without a name. A
name without an object cannot arise from a failure.

**Contract.** Each table operation is linearized at its mutex point: insert after the PUT settled,
erase before the DELETE starts, hit at the read. A directory listing therefore reflects every
operation that settled before it and none that started after it. It is not identical to an
instantaneous S3 LIST during a PUT or DELETE, and does not need to be: the callers that write
table-level files own them (a mutation entry is written and removed by the owner of that
mutation, `StorageMergeTree.cpp:1005-1015`; deduplication segments by one thread under the log's
`state_mutex`, `MergeTreeDeduplicationLog.cpp:247-310`; `Log` family files under the table's write
lock), and no caller races a rewrite of a name against its unlink.

**Why this is safe.**

- Single writer per lease: the namespace is server-root scoped, the mount lease admits one holder
  per generation, and every mutator of a live life's `_files/` on this node is one of the two wrapped
  methods, both of which update the table.
- No LIST overlaps a writer of the same life: population runs before any writer under the armed
  generation and after the requests of the previous generation and of a dead predecessor can no
  longer land.
- Same trust in LIST as today: the mount-time LIST is the recovery-time cold LIST, the trusted kind
  (decision 2); the hot LISTs it replaces were the untrusted ones.
- Fail-close: unknown write outcomes leave invisible objects, never phantom names; every use of the
  table is admitted on the fence; a failed population fails the mount attempt.

**Alternatives not taken.**

- A `std::map` inside `CasPlainObjects` or `ContentAddressedMetadataStorage` (round 1): the same
  table in an object without the mount lifetime.
- A version counter bumped at write start and settle with an install check (rev.2): defeated by PUT
  and DELETE settling out of durable order and by a runtime replaced between LIST and install.
- A mutex inside each `RefTableRuntime` (rev.3, rev.4), then a fixed bank of striped mutexes in the
  ledger keyed by the life (rev.5 to rev.7), each held across a lazily started LIST and every write
  of that life: population at mount removes the overlap they existed to order.
- Durable directory entries in the ref log (the design study, closed after two rounds): a format
  generation, a resync authority argument and still the same-name serialization.
- One ledger-wide mutex (round 3): serializes the inserts of every deduplicating table behind each
  other's PUT.
- Repairing the table on a read that finds the object gone: the exception would still propagate and
  the repair is a guess about why the object is gone; the quiescence rules prevent the state instead.

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
pool opened by `Pool::open`, `CountingBackend`; LIST counts are per `_files/` prefix, since a writable
open issues control-plane LISTs of its own, `Pool/CasPool.cpp:403-479`):

4. Population: a pool with three live tables holding files, one shadow (FREEZE) namespace, one
   `Creating` and one `Removing` row and one foreign-root namespace is opened; exactly the three live
   tables' `_files/` prefixes are listed once each, the others never; `listNamespaceFiles` of each
   live life afterwards issues no LIST and returns the names; a paginated prefix (more names than one
   page) is listed completely.
5. Admission: with a populated table, let the lease deadline pass without a trip; `listNamespaces`
   is refused (no cached answer); trip the fence explicitly; the same; after `tryRemountOnce` the
   table is rebuilt and hits resume under the new generation.
6. Quiescence, in-process: a DELETE of the lost generation parked in the backend after the fence
   trip; `tryRemountOnce` does not issue the rebuilding LIST until the DELETE settled (refused);
   the rebuilt table does not name the object. The same with a parked PUT.
7. Quiescence, cross-process: a claim over an `UncleanObserved` predecessor with a parked predecessor
   DELETE in the backend; population issues its LIST only after one attempt envelope under
   `FakeClock`; with a `Clean` predecessor it issues it immediately.
8. Birth: `namespaceLife(ns)` of a new namespace installs an empty set under admission; the same
   call after a fence trip installs nothing; a life born by ref publication gets its entry at its
   first `namespaceLife` call and before its first file.
9. Write-through: a `putNamespaceFile` and a `removeNamespaceFile` are visible in the next listing
   without a LIST; the DELETE of the removed name is issued after the erase.
10. Miss: a fixture life not owned by the mount lists with one LIST per call and installs nothing
    (the `DedupLogRotation` gate keeps its counts); `openForDecommission` and a read-only open list
    as today.
11. Ambiguous PUT (the backend lands the object, then fails every attempt past the policy deadline
    under `FakeClock`): the public call throws and the name is absent; a later write of the same
    name succeeds and the name appears. Ambiguous DELETE: the name is absent whether or not the
    object survived. Allocation failure in the insert after a durable PUT: the call throws and the
    name is absent.
12. Population failure: a backend that fails one prefix's LIST makes `Pool::open` throw and
    `tryRemountOnce` report failure with the fence closed; the next attempt succeeds cleanly.
13. Drop and same-name rebirth: `dropNamespace` erases the entry; the reborn life starts empty.
14. Destructive consumers on a populated table: `removeRecursive` of a table subdirectory
    (`ContentAddressedTransaction.cpp:1164`) removes every file it would have removed with a fresh
    LIST, and a table rename (`:1295`) copies every file, both without a LIST.
15. Non-MergeTree: `Log`, `TinyLog` and `StripeLog` tables on the CAS disk insert, restart, append,
    truncate, rename and drop with zero LIST after mount, per the probe report
    `docs/superpowers/reports/2026-09-27-cas-non-mergetree-engines-probe.md`; a post-mount `ATTACH`
    of a restored table gets its entry at birth.

Integration, new module `tests/integration/test_cas_directory_probes` with GC disabled
(`gc_enabled = 0`) so no maintenance LIST is counted:

16. Two restarts of one node, first with 2 tables × 20 parts, then with 2 tables × 200 parts;
    `system.events` `CASRootList` read after all parts are loaded is equal in both restarts.
17. After the second restart, the delta of `CASRootList` over two minutes without queries or DDL is
    zero (covers `clearOldTemporaryDirectories`).

No `LOGICAL_ERROR` is introduced by this change.
The ASan lane runs for the touched suites.

## 5. Acceptance {#acceptance}

With the decision of §0, the implementation plan covers §3.1, tests 1 to 3 and tests 16 and 17 with
the restart comparison only; acceptance #2 of CAS-95 is withdrawn for the `clearOldTemporaryDirectories`
half.


- CAS-95 acceptance #1: restart LIST count independent of the part count (test 16).
- CAS-95 acceptance #2 for the `clearOldTemporaryDirectories` half: zero `CASRootList` on a warm node
  without DDL (test 17). The `system.detached_parts` half stays with CAS-95.3.
- Issue #2439 gets before/after `S3ListObjects` numbers from an otel.demo restart.

## 6. Documentation {#documentation}

One paragraph in `docs/en/antalya/cas/architecture/read-path.md` stating that part-level directory
probes are answered from the part manifest and that a table's file names are listed once per table at
mount and then kept in memory by the writers. No on-S3 format change, so the CAS format documents and
`NativeFormat.md` are untouched.
