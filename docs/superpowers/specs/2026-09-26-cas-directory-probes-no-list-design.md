---
description: 'Design for CAS-95.1 and CAS-95.2: answer MergeTree directory probes on a CAS disk without an S3 LIST. A path inside a resolved part gets its own directory shape answered from the part-folder view, and the table-level namespace file names are held per resident ref-table runtime with write-through instead of being listed on every probe. Closes the restart LIST storm of issue #2439. Rev.7 folds in codex review rounds 1 to 6.'
sidebar_label: 'CAS directory probes without LIST'
sidebar_position: 12
slug: /superpowers/specs/cas-directory-probes-no-list-design
title: 'CAS directory probes without an S3 LIST'
doc_type: 'design'
---

# CAS directory probes without an S3 LIST — rev.7 (2026-09-26) {#cas-directory-probes-no-list}

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
tests for the rev.6 branches, and fixes test wording. The review loop stops here.

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
(`enforceRefTableCacheBudget`, `:1736-1813`). Those are the lifetimes the cached names get, so the
runtime gains two fields:

```cpp
/// Table-level file names of THIS life, as of one LIST plus this node's settled writes since, both
/// applied under the life's namespace-file stripe (see `namespace_file_stripes`). Empty optional:
/// not yet listed, or forgotten after a write whose outcome is unknown.
std::optional<std::set<String>> namespace_file_names;
/// Estimated bytes of `namespace_file_names` (sum of `name.size() + 64`), read by `weightOf` in
/// `enforceRefTableCacheBudget` alongside the snapshot and log-tail bytes.
std::atomic<uint64_t> namespace_file_names_bytes{0};
```

**What serializes them.** The ledger gains a fixed bank of mutexes, `namespace_file_stripes`
(64 `std::mutex`, indexed by a hash of `life.ns` and `life.incarnation`). Every namespace-file
operation of a life on this node, the populating LIST, each PUT and each DELETE, and the cache hit,
runs while holding the life's stripe: the runtime lookup, the request and the set update all happen
inside it. The stripe outlives every runtime, so a runtime detached by remount or evicted by the
budget while a write is in flight changes nothing: the successor's populating LIST needs the same
stripe and therefore runs after the write settled and its effect was applied to whichever runtime is
current at that moment. Two tables sharing a stripe serialize their namespace-file I/O with each
other; with 64 stripes and rare writes that is a collision, not a design property. The stripe is
held across the request on purpose: namespace-file writes of one table are rare (dedup-log rewrites,
`format_version.txt` at create, rename) and serializing them with the LIST is what makes the set
exact without any version protocol.

Lock order: the stripe is taken first; the runtime lookup inside it takes and releases
`ref_queue_mutex`; the set update takes `state_mutex` briefly. Nothing takes a stripe while holding
`ref_queue_mutex` or `state_mutex`, recovery, eviction and remount never take a stripe, and
`CasPlainObjects` has no path back into the ledger, so the I/O held under a stripe cannot deadlock
with any of them.

**The ledger API.** Two methods, with `Pool` as their only caller:

- `listNamespaceFilesHeld(life, list_fn)`: take the stripe, then admit an operation on the ledger's
  own `mount_requests` (admission happens after the wait, at the hit's linearization point, so a
  fence lost while waiting for the stripe is seen; `admitted` is the dynamic verdict,
  `Backend/CasRequests.h:258`). Look up the runtime for `life.ns`; when it exists, its `life` equals
  the argument, its `admitted_fence_generation` equals the operation's generation, its set is
  populated and the operation is admitted, return a sorted copy. Otherwise call `list_fn` (today's
  LIST, which admits its own operation and refuses as today when the fence is lost) and,
  after it returns, look the runtime up again (a fresh lookup, exact `NamespaceLifeId` equality, the
  `shared_ptr` pinned for the update); when it exists, install the result and update
  `namespace_file_names_bytes`. A LIST failure propagates and installs nothing.
- `noteNamespaceFileWrite(life, write_fn, on_success)`: under the stripe, run `write_fn`; then look
  up the current runtime for the life (any generation, since this is the runtime whose set must not
  go stale). On success apply `on_success` to a populated set and update the bytes. If `write_fn`
  throws, reset that runtime's set (an ambiguous PUT or DELETE may have landed) before rethrowing.
  If applying `on_success` itself throws (an allocation inside `std::set::insert` under a memory
  limit), the object is durable and the set would be stale, so the set and the byte count are reset
  before that exception propagates too.
  Without a current runtime nothing is updated: a runtime created later starts unpopulated and its
  LIST, which needs the stripe, sees the settled state.

**Read path.** `Pool::listNamespaceFiles(life)` (`Pool/CasPool.cpp:1218`) becomes
`ref_ledger.listNamespaceFilesHeld(life, [&] { return plain_objects.listNamespaceFiles(life); })`.
The admission check at the disk layer (`:1626`, `:1804`) is not enough on its own because the fence
can drop between it and the read; a hit is served only under an admission taken inside the stripe,
on the runtime's own generation. Lives without a runtime (test fixture lives, offline tools) list
every time, which is today's behaviour. `CasPlainObjects` admits its own operation for the request
(`Pool/CasPlainObjects.cpp:58-65`) on the same `mount_requests` fence; the operation admitted in the
ledger is evidence for the hit only.

**Write path.** `Pool::putNamespaceFile` and `Pool::removeNamespaceFile` become
`ref_ledger.noteNamespaceFileWrite(life, [&] { plain_objects.putNamespaceFile(life, name, bytes); },
[&](std::set<String> & s) { s.insert(name); })` and the same with `erase`. Because the LIST, every
PUT and every DELETE of one life on this node run under one stripe, the set is updated in durable
order, and the update goes to the runtime that is current when the write settled.

**Memory.** The names are part of the runtime's weight: `weightOf` (`:1751`) adds
`namespace_file_names_bytes`, and `enforceRefTableCacheBudget(life.ns)` runs after an install and
after a write-through that grew the set, outside the stripe. The existing policy then applies
unchanged: idle runtimes are evicted least-recently-touched first, the runtime just touched is kept,
and the floor is one table (`Pool/CasPool.h:315-321`). A table whose names alone exceed the budget
behaves as a table whose ref state alone does today. No separate cap and no exception to the
warm-node guarantee: the set stays as long as the runtime stays resident.

**Why this is safe.**

- Single writer per fence generation: the namespace is server-root scoped, the mount lease admits one
  holder per generation, every mutator of a cataloged live life's `_files/` on this node is one of the
  two wrapped methods, and a hit is served only under an operation admitted on the runtime's own
  generation.
- Exact set: the populating LIST and every write of the life on this node are totally ordered by the
  stripe, each write's effect is applied under the same critical section that performed it, to the
  runtime current at settle time, and a successor runtime cannot LIST before that.
- Same trust in LIST as today: the cached set is the answer of one real LIST plus settled writes. A
  repeated LIST is not a stronger guarantee.
- Lifetime by construction: the set dies with the runtime (remount, drop, rebirth, eviction). No
  invalidation code is added. Eviction costs one LIST on the next access, which is why §2 promises
  "while the runtime stays resident" and not "once per life".
- Fail-close: unknown write outcome forgets the set; LIST failure installs nothing; lost fence refuses
  the hit.

**Cost of the stripe.** A `listDirectory` of a table directory waits for an in-flight namespace-file
write of the same table (or of a table sharing its stripe), and namespace-file writes of one table
wait for each other. The stripe is the general serialization; callers only happen to serialize some
of it (`MergeTreeDeduplicationLog` holds its `state_mutex` across finalize and rotation, while a
delayed write buffer can finalize on another thread). Each holder's request is bounded by its retry
policy; a waiter may queue behind several colliding holders, and `std::mutex` gives no fairness.

**Alternatives not taken.**

- A `std::map<RootNamespace, {incarnation, std::set<String>}>` inside `CasPlainObjects`, `Pool` or
  `ContentAddressedMetadataStorage` (round 1): each needs its own eviction on drop and its own clear
  on remount and fence loss.
- A version counter bumped at write start and settle with an install check (rev.2): concurrent PUT
  and DELETE settling out of durable order and a runtime replaced between LIST and install both
  defeat it (round 2).
- A mutex inside each `RefTableRuntime` (rev.3, rev.4): it does not survive the remount that detaches
  the runtime, so it needs a post-settle rule and a second, generation-free ledger lookup to reset the
  successor (rounds 3 and 4). The stripe bank is that mutex moved to an object that outlives the
  runtime.
- One ledger-wide mutex (round 3): on a CAS disk every dedup-log append is a `putNamespaceFile` (the
  disk cannot append, `ContentAddressedTransaction.cpp:830-851`), so one mutex across S3 I/O would
  serialize the inserts of every deduplicating table in the pool behind each other's PUT latency and
  retries. Round 4 agreed.
- A per-table cap on the held set (rev.4): it silently exempted an oversized table from the warm-node
  guarantee and did not bound the aggregate (round 4).

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
  single-writer argument gives for a fresh LIST. Their own `removeNamespaceFile`/`putNamespaceFile`
  calls take the stripe one at a time, after the listing call released it.

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
5. LIST in flight, PUT from another thread: the PUT does not start until the LIST installed (stripe);
   after both returned, `listNamespaceFiles` returns a set containing the name, with no further LIST.
6. PUT in flight (landed, not returned), LIST from another thread: the LIST waits; after both
   returned the set contains the name and no further LIST is issued.
7. Concurrent PUT and DELETE of one name: the second waits for the first; the final set equals the
   final storage state in both orders.
8. Ambiguous PUT and ambiguous DELETE: the backend lands the first attempt and then throws on every
   later attempt, for the DELETE including attempts whose base result is `Gone`
   (`removeCurrent` otherwise treats `Gone` as success, `Backend/CasRequests.cpp:545-654`), and
   `FakeClock` advances past the policy deadline so the engine's settle-by-read cannot conclude
   (`:1039-1069`); the public call throws, and the next `listNamespaceFiles` on the same generation
   issues a LIST and returns the true state.
9. Fence loss: with a populated set, trip the fence; `listNamespaceFiles` is refused as today (no
   cached answer served). After remount and a fresh runtime, the first call lists again.
9b. Fence lost while waiting for the stripe: a first LIST blocks after its backend result while it
    holds the stripe; a second `listNamespaceFiles` starts and is observed waiting (waiter seam);
    trip the fence without remounting; release the first LIST. The second call refuses rather than
    returning the freshly populated old-generation set.
8b. Cache update fails after a durable PUT: inject a failure into the set mutation (a test-only
    hook on the ledger that throws from `on_success`); the public `putNamespaceFile` throws, and the
    next `listNamespaceFiles` issues a LIST and contains the name.
10. Eviction: with `ref_table_cache_bytes` set so the runtime is evicted, the next call lists again.
11. Drop and same-name rebirth: the reborn life lists again and does not see the old names.
12. Write without a runtime: evict the runtime, start a PUT that blocks before the base `write`, touch
    the namespace so a new runtime exists, and call `listNamespaceFiles` from another thread; wait
    until the ledger's test-only stripe waiter count for the life reads 1 (a counter incremented
    before the stripe lock is attempted and decremented after it is taken, the seam that makes the
    schedule deterministic); release the PUT; the listing then returns a set containing the name and
    a second call issues no LIST.
13. Write across remount: a PUT blocks before the base `write`; fence the mount out durably and
    `tryRemountOnce`, so runtime B replaces A (`tripMountLost` alone does not create B);
    `listNamespaceFiles` from another thread blocks on the stripe (same waiter-count seam as test
    12); release the PUT; the writer throws (the post-commit gate refuses a write whose fence
    generation changed, `Backend/CasRequests.h:237-240`), and the listing afterwards issues a LIST
    and contains the landed name.
14. Budget, broad-gap fixture (`ref_table_cache_bytes` is fixed at `Pool::open`,
    `Pool/CasPool.h:321`): open the pool with a budget that comfortably retains two small runtimes,
    assert both are resident, then install on the second runtime a name set larger than the
    remaining margin (many long names); the idle first runtime is evicted (`CASRefTableEvictions`
    increments) and the installed one is kept.
15. Destructive consumers on a warmed set: `removeRecursive` of a table subdirectory
    (`ContentAddressedTransaction.cpp:1164`) removes every file it would have removed with a fresh
    LIST, and a table rename (`:1295`) copies every file, both without a LIST.

Integration, new module `tests/integration/test_cas_directory_probes` with GC disabled
(`gc_enabled = 0`) so no maintenance LIST is counted:

16. Two restarts of one node, first with 2 tables × 20 parts, then with 2 tables × 200 parts;
    `system.events` `CASRootList` read after all parts are loaded is equal in both restarts.
17. After the second restart, the delta of `CASRootList` over two minutes without queries or DDL is
    zero (covers `clearOldTemporaryDirectories`).

Test seams (the stripe waiter count of tests 9b, 12, 13 and the `on_success` failure hook of test
8b) follow the existing `*ForTest` block on `Pool` (`Pool/CasPool.h:937-1138`) forwarding to the
private ledger; the waiter count is incremented only on the contended `try_lock` path so the
uncontended production path gains no accounting. No `LOGICAL_ERROR` is introduced by this change.
The ASan lane runs for the touched suites.

## 5. Acceptance {#acceptance}

- CAS-95 acceptance #1: restart LIST count independent of the part count (test 16).
- CAS-95 acceptance #2 for the `clearOldTemporaryDirectories` half: zero `CASRootList` on a warm node
  without DDL (test 17). The `system.detached_parts` half stays with CAS-95.3.
- Issue #2439 gets before/after `S3ListObjects` numbers from an otel.demo restart.

## 6. Documentation {#documentation}

One paragraph in `docs/en/antalya/cas/architecture/read-path.md` stating that part-level directory
probes are answered from the part manifest and that a table's file names are listed once per resident
ref-table runtime on a node and then kept in memory with write-through. The `ref_table_cache_bytes`
contract comment in `Pool/CasPool.h:311-321` gains the namespace-name bytes as a third component of
the weight (the branch has no user-facing description of that setting to update). No on-S3 format change, so the CAS format documents and
`NativeFormat.md` are untouched.
