---
description: 'Design for a coordinator-free CAS backend over local and shared POSIX filesystems (local disks, NFSv3/v4, CephFS, Lustre, GPFS, SMB with hard links): the full Cas::Backend contract from mkdir, linkat, unlinkat, rename and rmdir, with every key an incarnation-chain directory addressed through its directory handle so that every conditional operation is a server-decided create-if-absent, no lock exists and a delayed syscall can never reach a newer incarnation; durable through fsync; no in-process state; replaces EmulatedSingleProcess.'
sidebar_label: 'CAS POSIX shared backend'
sidebar_position: 50
slug: /superpowers/specs/cas-posix-shared-backend-design
title: 'CAS backend over local and shared POSIX filesystems'
doc_type: 'design'
---

# CAS backend over local and shared POSIX filesystems {#cas-posix-shared-backend}

Revision 2, 2026-09-08. Revision 1 used a writer-minted `mtime` token and a per-key lock directory
with a hold deadline and a timed break. The review in `tmp/cas-posix-spec-review-astra-r1.md` showed
that no timed lock can fence a delayed syscall (a paused holder's `rename` or `unlink` lands after the
break and destroys a newer incarnation), that an unlocked publication defeats exact deletion, that
NFSv3's 32-bit seconds cannot carry a 62-bit nonce, and that a lost reply to a successful `link`
makes the retransmit report a conflict for a write that landed. Revision 2 removes the lock and the
timestamp token: every conditional operation becomes a create-if-absent of a name the server
decides, every mutation is addressed through the directory handle it was decided in, and ownership
of a created name is provable by inode identity.

Decisions taken with the user: coordinator-free (no Keeper); one mode for local and shared
filesystems; lowest-common-denominator primitives only; every filesystem in `§2` is in scope.

## 1. Problem {#problem}

`object_storage_type = local` routes a CAS disk to `ObjectStorageBackend::Mode::EmulatedSingleProcess`
(`ContentAddressedMetadataStorage.cpp`, `openPoolView`), which implements the conditional contract of
`Cas::Backend` with one process-wide `std::mutex` and an in-memory token map minted from the file's
`mtime`. It is correct for exactly one process and silently wrong for two: two servers on one
directory keep independent token state, both pass the capability probe alone, and the mount only
logs at `Information`. It is not crash-durable even for one process: nothing in `LocalObjectStorage`
or in the CAS tree calls `fsync`, and mutable control objects are rewritten in place with `O_TRUNC`.

The goal is one backend mode, `Mode::Posix`, that satisfies the whole `Cas::Backend` contract
(`Backend/CasBackend.h`) on every filesystem in `§2`, with no in-process state, so that N servers
sharing a directory are exactly as safe as N servers sharing an S3 bucket, and one server on a local
disk is crash-durable. The request engine, GC, mount lease, ref lanes, `_ckpt` and every persisted
format are unchanged. Where this revision needs something from outside the backend it says so in
`§9`.

## 2. Supported filesystems and the facts relied on {#filesystems}

| Filesystem | Status | Mandatory mount options (`§6`) |
|---|---|---|
| Local ext4 / xfs / btrfs / zfs | supported; one process; replaces `EmulatedSingleProcess` | none |
| NFSv4, 4.1, 4.2 | supported | `hard`, `lookupcache=positive` |
| NFSv3 | supported | `hard`, `lookupcache=positive` |
| CephFS, Lustre, GPFS | supported (coherent caches) | none |
| SMB / CIFS | supported only where `link` works (Samba POSIX extensions, SMB3 POSIX) | `cache=none` or `actimeo=0` |

Five filesystem facts are relied on, each verified by the probe (`§8`), and each decided at the
server, never by a client cache:

1. `linkat(srcfd, dirfd, name)` fails with `EEXIST` when `name` exists in the directory `dirfd`
   names; on success the new name and `srcfd` share one inode (`st_dev`, `st_ino`).
2. `rename(dir_a, dir_b)` of a directory onto an existing **non-empty** directory fails
   (`ENOTEMPTY` or `EEXIST`); onto an absent name it succeeds atomically.
3. `rmdir` fails with `ENOTEMPTY` when the directory has any entry.
4. `unlinkat(dirfd, name)` removes exactly that name from exactly that directory.
5. An open directory handle keeps naming the same directory inode after the directory is renamed
   (NFS: the file handle is the inode; local: the descriptor is).

Not relied on: `O_EXCL`, `flock`/`fcntl`, extended attributes, `renameat2`, `rename` of a directory
onto an *empty* directory (used opportunistically, with `rmdir`-then-retry as the universal
fallback), `mtime`/`ctime`/inode numbers as identity, listing order or completeness.

## 3. Layout: every key is an incarnation-chain directory {#layout}

```
P(k)/                        <- parent (a plane prefix or a fan-out directory)
P(k)/D(k)/                   <- the object: a directory, exists iff the object exists or is in transition
P(k)/D(k)/<gen>-<n>          <- incarnation n of the chain gen; 16+16 hex, fixed width
P(k)/D(k)/.tmp-<uuid>        <- writer scratch inside the object directory, never an incarnation
P(k)/.new-<uuid>/            <- a creator's private directory before it is renamed onto D(k)
P(k)/.gone-<uuid>/           <- a removed object directory before its contents are deleted
```

- `gen` is a random 64-bit value drawn by the creator of the directory; `n` is dense from 1 within
  the chain. One directory holds exactly one chain (`§4`, creation), so "the current incarnation"
  is the file with the greatest `n`.
- The contract's **value** is the incarnation file name `<gen>-<n>`, minted by the writer. An
  incarnation file is written into `.tmp-<uuid>`, linked to its final name, and never modified, so
  `VALUE ⟹ CONTENT` holds by construction on every filesystem with no timestamp involved.
- A **tombstone** is an incarnation file whose content is the single line `{"type":"cas_posix_tombstone"}`.
  It is created only by `remove` and only as the successor of the incarnation being removed. A
  chain whose greatest `n` is a tombstone reads as **absent** and can never be extended: the
  tombstone occupies the one name a successor could have (`§4`).
- Every mutation of a chain is performed **relative to an open handle of its directory**
  (`linkat`, `unlinkat`, `openat`). A directory that has been renamed away (`§4`, removal) or
  replaced by a rebirth is a different inode, so a delayed or retransmitted operation from a stale
  writer reaches the directory it observed and never the object's current directory. This is what
  replaces revision 1's lock: not a promise the holder makes about time, but an address the
  filesystem resolves to the same inode forever.

Cost: two inodes per object (directory + file), one `readdir` of a tiny directory on every
`head`/`read`. For blobs that is a directory per content hash; the pool's `blobs/<algo>/<hh>/`
fan-out keeps parents small. The CAS key grammar (`Formats/CasLayout.h`) guarantees a key is never
a prefix of another key, so an object directory never contains another object.

## 4. Operations {#operations}

Helpers: `stage(dirfd, bytes)` creates `.tmp-<uuid>` in `dirfd`, writes, `fsync`s the file, keeps
it open; `settle(fd)` is `fsync` of a directory handle; `chain(dirfd)` is `readdir` of `dirfd`
returning the greatest `<gen>-<n>` and whether it is a tombstone.

| Contract method | Implementation |
|---|---|
| `write(k, bytes, expected = absent)` | `mkdir P(k)/.new-<uuid>`; open it as `newfd`; `stage(newfd, bytes)`; `linkat(tmp, newfd, <gen>-1)`; `unlinkat(tmp)`; `settle(newfd)`; `rename(.new-<uuid>, D(k))`. Success ⇒ `settle(P(k))`, return `<gen>-1`. `ENOTEMPTY`/`EEXIST` ⇒ `D(k)` exists: open it, `chain`: a live incarnation ⇒ remove our private directory, `RawConflict`; a tombstone on top ⇒ complete that removal (`§4.3`) and retry once; an empty directory (a crashed creator) ⇒ `rmdir D(k)` (`ENOTEMPTY` ⇒ someone populated it, `RawConflict`) and retry the `rename` once. Lost reply: `§5` |
| `write(k, bytes, expected = <gen>-<n>)` | `dirfd = open(D(k))` (`ENOENT` ⇒ `RawConflict`); `stage(dirfd, bytes)`; `linkat(tmp, dirfd, <gen>-<n+1>)`: `EEXIST` ⇒ (`§5` lost-reply check, else) `unlinkat(tmp)`, `RawConflict` — the store decided that a successor already exists, whether a writer's or a remover's tombstone; `ESTALE`/`ENOENT` on `dirfd` ⇒ the directory was removed, `RawConflict`; success ⇒ `unlinkat(tmp)`; `settle(dirfd)`; opportunistically `unlinkat(dirfd, <gen>-<n>)` and older; return `<gen>-<n+1>`. No read of the current incarnation is needed: the precondition IS the name, and the directory handle pins the chain |
| `remove(k, <gen>-<n>)` | `dirfd = open(D(k))` (`ENOENT` ⇒ `Gone`); `chain`: greatest is not `<gen>-<n>` ⇒ `Mismatch` (a successor exists, or the name is already a tombstone's predecessor); `stage(dirfd, tombstone)`; `linkat(tmp, dirfd, <gen>-<n+1>)`: `EEXIST` ⇒ `Mismatch` (a successor won the race; the object is untouched); success ⇒ **no successor can ever be created in this chain**; then `§4.3` (rename away and delete); `Removed`. A delayed `unlinkat` from this remover names one old incarnation in one directory handle and can never touch a successor or a rebirth. `DeleteMarker` is never returned |
| `publish(request)` | unconditional by protocol, implemented as "append an incarnation": `dirfd = open(D(k))`; absent ⇒ the `expected = absent` path above with the blob body; present ⇒ `chain`; a tombstone on top ⇒ complete the removal (`§4.3`) then the absent path; else `stage` (envelope + bounded payload copy, size-checked exactly as `emuPublishBlobAtomically` does today), `linkat(tmp, dirfd, <gen>-<n+1>)`, on `EEXIST` re-`chain` and retry with the new successor name, bounded by the request deadline; `unlinkat(tmp)`; `settle`; older incarnations unlinked opportunistically. The body is visible only when complete |
| `read(k)` | `dirfd = open(D(k))`; `chain`; absent/empty/tombstone ⇒ nullopt; `openat(dirfd, <gen>-<n>)`, `fstat`, read whole. The value is the name opened, so bytes and value belong to one incarnation by construction |
| `stream(k)` | as `read`, returning a `ReadBufferFromFileDescriptor` that owns the descriptor. A remote `unlink` of the name while streaming (NFSv3 `ESTALE`, since another client's unlink is not silly-renamed) propagates as the read error it is; the callers (GC, fsck) fail closed on read errors and never turn one into EOF |
| `head(k)` | `dirfd = open(D(k))`; `chain`; `fstatat` size. Absent/tombstone ⇒ nullopt |
| `list(prefix, cursor, limit)` | complete recursive enumeration of object directories under `prefix` (an object is a directory whose `chain` is a live incarnation), sorted lexicographically, spilled to a scratch file above a memory budget, paged by `cursor`. `.new-*`, `.gone-*`, `.tmp-*`, `.nfs*` and tombstone-topped or empty directories are skipped. Values absent (`supportsListTokens = false`). `readdir` order is hash order on most filesystems, so a lazy walk cannot implement "strictly after cursor" |
| `removeManyWriteOnce(keys)` | for each key: `§4.3` without a tombstone (the engine proves by `WriteOnceKey` that nothing will ever recreate the name): `rename(D(k), .gone-<uuid>)`, delete contents, `rmdir`. `ENOENT` is success |
| `probeSentinelRaw(k)` | `open(D(k))`: `ENOENT` with the pool root present ⇒ `KeyAbsent`; pool root absent ⇒ `ContainerAbsent`; `EACCES`/`EPERM` ⇒ `AccessDenied`; other errors ⇒ `Indeterminate`; present ⇒ `chain`, tombstone ⇒ `KeyAbsent`, else `Present` with the body. Authoritative absence on NFS/SMB holds under the mandatory mount options of `§6`, which the probe enforces |

### 4.1 Old incarnations {#old-incarnations}

A writer that created `<gen>-<n+1>` unlinks `<gen>-<n>` and any smaller `n` through the same
`dirfd`. A delayed or retransmitted `unlinkat` of an old name is harmless: `n` is dense and monotone
within a chain, a chain never restarts, and the handle names one directory inode. A crashed writer
leaves at most a few old incarnations, which the next successor writer or the namespace janitor's
existing bounded page removes; `head`/`read` pick the greatest `n`, so leftovers are invisible.

### 4.2 Creation is exclusive by rename, not by mkdir {#creation}

Two creators each build a private `.new-<uuid>` holding their own `<gen>-1` and race to `rename` it
onto `D(k)`. Fact 2 makes exactly one win: the loser's target is non-empty. A directory therefore
holds exactly one chain, and `chain` is well-defined. A crashed creator leaves `.new-<uuid>` (unique
name; the janitor removes it) or an empty `D(k)` only if the filesystem replaced nothing — the
empty-directory branch above handles it with `rmdir` (fact 3) and a retry.

### 4.3 Removal renames the directory away first {#removal}

After the tombstone is linked (`remove`) or for a `WriteOnceKey` (`removeManyWriteOnce`):
`rename(D(k), P(k)/.gone-<uuid>)`; `settle(P(k))`; through a handle of the renamed directory,
`unlinkat` every entry; `rmdir`. From the instant of the `rename`, `D(k)` is absent to every
reader; a stale writer holding the old `dirfd` can only add or remove names in the `.gone-*`
directory, which nobody reads. A rebirth creates a new directory inode at `D(k)` (`§4.2`), so
nothing a stale writer does can reach it. A crash between the tombstone and the `rename` leaves a
tombstone-topped `D(k)`: it reads as absent, can never be extended, and **anyone** may complete
`§4.3` on it — the next creator, publisher, or janitor page — because the tombstone proves no
successor can appear. A crash after the `rename` leaves `.gone-<uuid>`, which the janitor deletes.

## 5. Ambiguous outcomes and lost replies {#ambiguity}

A hard NFS mount retransmits an RPC whose reply was lost, and `link`, `rename`, `unlink` and `rmdir`
are not idempotent: the retransmit of a successful `link` returns `EEXIST`. The rule per primitive:

| Primitive | Outcome after a possibly-lost reply |
|---|---|
| `linkat(tmp, dirfd, name)` → `EEXIST` | `fstatat(dirfd, name)` and `fstat(tmp)`: **same `st_dev` and `st_ino` ⇒ our link succeeded**. Different inode ⇒ a real conflict. `fstatat` fails ⇒ transport ambiguity (below) |
| `rename(.new-<uuid>, D(k))` → `ENOENT` on the source | open `D(k)`, `chain`: our `<gen>-1` (our `gen`) ⇒ our rename succeeded. Another chain ⇒ conflict, our private directory is gone with its own reply, nothing to clean |
| `rename(D(k), .gone-<uuid>)` → `ENOENT` on the source | someone completed the removal (only a tombstoned or write-once directory is ever renamed away); if `.gone-<uuid>` exists, it is ours to delete |
| `unlinkat` → `ENOENT` | success: names in a chain are never recreated |
| `rmdir` → `ENOENT` | success; `ENOTEMPTY` ⇒ leave it |

Any other failure after a request may have been sent (a timeout, `EIO`, `ESTALE` on a handle that
should be valid, a failed `fsync`) is reported to the request engine as a **transport ambiguity**,
which the engine settles by an exact read exactly as it does for S3: the backend throws a
`Poco::Exception`-derived exception, the only class the engine treats as "may have landed"
(`Backend/CasRequests.cpp`, the `catch (const std::exception &)` arm of the write path re-throws
anything that is not a `Poco::Exception`). A bare `std::system_error` must never escape the
backend. The engine's wedge rule then applies unchanged: a sent ref-log slot is never reused.

`CAS_WRITE_UNATTRIBUTED` cannot arise in this mode: the value of a completed write is the name the
writer chose before the write.

## 6. Absence, caches and mandatory mount options {#absence}

The review of revision 1 enumerated every engine consumer of a null `head`/`read`/`stream` and
classified it. The classes:

- **Delay-class** (the majority): recovery's live tail stop, GC holds and skipped deletions,
  orphan-sweep retention, blob `HEAD` miss (a duplicate publication), diagnostics. A stale absence
  costs a retry, a duplicate upload or one round of delayed reclamation.
- **Authorization-class**, where a false absence would authorize a destructive or identity decision:
  1. the blob `.meta` read on a dedup hit — absent reads as `Clean` and authorizes adoption
     (`Pool/CasPartWriteTxn.cpp`); a false absence over a durable `Condemned` marker adopts a body
     whose exact delete is already pending;
  2. GC's blob `HEAD` before marker cleanup (`Gc/CasGc.cpp`, the pending-delete path);
  3. the owner and epoch claims' "absent, subtree provably empty" (`Pool/CasServerRoot.cpp`) and the
     bootstrap residual probe (`Backend/CasSentinelProbe.cpp`);
  4. the lifecycle `IdentityLost` verdict and the empty-table probe (`Pool/CasPool.cpp`,
     `ContentAddressedMetadataStorage.cpp`);
  5. the request engine's settlement read, which turns null into `ProvenAbsent`
     (`Backend/CasRequests.cpp`).

  The successor's epoch seal is **not** on this list: it is a create-if-absent on the dead epoch's
  next slot, decided at the server, so a stale negative entry can only make its `rename` fail, after
  which the engine re-reads the slot and keeps walking.

The authorization-class consumers live in the engine, which is out of scope, so the backend must
make absence authoritative rather than ask the engine to doubt it. On a local or coherent cluster
filesystem it is. On NFS the Linux client caches negative lookups for up to `acdirmax`, so a name
created by another client can read as absent for a minute; the only way to make `ENOENT`
server-decided is `lookupcache=positive` (or `none`). On CIFS the equivalent is `cache=none` or
`actimeo=0`. These options are therefore **mandatory**: the probe (`§8`) reads
`/proc/self/mountinfo` for the mount holding the pool root and a writable mount is refused, not
warned, when the filesystem type is `nfs`/`nfs4`/`cifs`/`smb3` and the option is missing.
`skip_access_check` cannot bypass this gate (`§8`).

Positive staleness is not a hazard: an incarnation file never changes, and an NFSv4 delegation is
recalled before another client can modify or unlink the delegated file, so cached data and
attributes of an incarnation are always true of that incarnation. Directory listings may be stale
by `acdirmax`; listings are hints. The emptiness probes in class 3 open the specific directories
they need (a lookup, server-decided under the mandatory option) rather than trusting `readdir`
of a parent; `probeSentinelRaw` is implemented as a lookup for the same reason.

## 7. Durability {#durability}

- An incarnation file is `fsync`ed before it is linked to its final name.
- After a name changes, `fsync` the directory handle; after a directory is created or renamed,
  `fsync` its parent. On Linux NFS a directory `fsync` is a no-op because namespace operations are
  synchronous at the server; on a local filesystem it is what makes the name durable.
- A failed `fsync` or `close` after the `linkat`/`rename` is reported as an ambiguity (`§5`), never
  as "nothing written".
- Durability is only as good as the export: an NFS export with `async`, or storage that
  acknowledges before persisting, is outside what any client can verify and is documented as an
  operator precondition next to the S3 ones.

## 8. Capability probe {#probe}

`checkPoolPreconditions` for `Mode::Posix` runs before the generic seven-step battery and fails
closed with `NOT_IMPLEMENTED` naming the failed fact, under a per-mount random prefix:

1. Mount-option gate (`§6`): filesystem type and options of the mount holding the pool root.
2. `linkat` semantics: `linkat(a, d, b)` succeeds and `fstatat(d, b)`/`fstat(a)` agree on
   `st_dev`/`st_ino`; `linkat(c, d, b)` fails `EEXIST` and `b` is still `a`'s inode.
3. `rename` of a directory onto a non-empty directory fails; onto an absent name succeeds.
4. `rmdir` of a directory with one entry fails `ENOTEMPTY`; after `unlinkat` it succeeds.
5. Handle stability: open a directory, `rename` it, `linkat` into the handle succeeds and the name
   appears under the new path.
6. `fsync` of a file and of a directory handle succeed.
7. Layout check: the pool root, if non-empty, holds object *directories* under its planes; a
   regular file where an object directory is expected is a pool written by the retired emulated
   layout, refused with a message naming migration (`§11`).

`checkSkipAccessCheckSupport` **refuses** `skip_access_check = true` for a writable `Mode::Posix`
mount, exactly as the generation dialect does: gate 1 is the only thing that makes absence
authoritative, and `Pool::open` skips `checkPoolPreconditions` on that path (`Pool/CasPool.cpp`).
`checkConditionalWriteSingleAttemptSupport` stays a no-op: there is no transparent retry layer
above the kernel's own RPC retransmit, which `§5` handles.

The generic battery then runs unchanged and exercises the same code paths a second process would,
because no per-process state is left to diverge. What a single-client probe cannot verify is
cross-client atomicity of facts 1–4 (an operator precondition and the subject of the two-client
integration test in `§10`) and power-loss durability of the export.

## 9. What this needs outside the backend {#outside-backend}

- **Dialect.** No new `Dialect` value: the mode reports `Dialect::Emulated`, whose grammar is
  "non-empty" (`Backend/CasEtag.cpp`), and the fixed-width `<gen>-<n>` name satisfies it. Adding
  `Dialect::Posix` would touch the wire vocabulary (`Formats/CasWireVocab.h`) for no gain.
- **Settings.** Two new disk settings through `ContentAddressedSettings` and `openPoolView`:
  `posix_list_memory_budget_bytes` (default 256 MiB; above it the sorted enumeration spills to
  `cas_scratch_path`) and `posix_janitor_debris_age_sec` (default 3600; `.new-*`, `.gone-*`,
  `.tmp-*`, empty and tombstone-topped directories older than this may be reclaimed by the
  namespace janitor's existing bounded page). No timing participates in any correctness decision.
- **Mode selection.** `object_storage_type = local` selects `Mode::Posix`; `EmulatedSingleProcess`
  and its `emu_*` state are deleted.
- **Janitor.** The namespace janitor's page (`Gc/CasNamespaceJanitor.cpp`) gains a backend-provided
  "reclaim debris under this prefix" step; it never removes anything younger than the debris age,
  so it cannot remove another live writer's private directory or temporary, and a tombstone-topped
  directory it completes cannot gain a successor (`§4.3`).
- **Nothing in formats, GC's decisions, the ref lanes, the mount lease or the request engine
  changes.**

## 10. Tests {#tests}

Unit (`CAS*` suites, gtest, run under the standard gate filter):

- `CASPosixBackend`: the full contract over a temporary directory, then the same battery driven by
  two independent `ObjectStorageBackend` instances on one directory (no shared in-process state:
  the two-process model for everything except client caches), including create-vs-create (one
  `rename` wins), overwrite-vs-overwrite from one predecessor (one `linkat` wins), remove-vs-
  overwrite from one predecessor (the tombstone and the successor race for one name; the loser
  reports `Mismatch`/`RawConflict`, the object is never both removed and extended), publish-vs-
  remove (a publication that lands after the tombstone rebirths the key in a new directory),
  a delayed `unlinkat` through a stale handle after rebirth (the new directory is untouched), a
  delayed `linkat` through a stale handle after `rename`-away (lands in `.gone-*`, invisible).
- `CASPosixLostReply`: a fault-injecting filesystem shim that drops the reply of a successful
  `linkat`, `rename`, `unlinkat` and `rmdir` once; the backend reports success by inode identity,
  never `RawConflict`, for the write that landed; an unresolvable case throws a
  `Poco::Exception`-derived error, never `std::system_error`.
- `CASPosixList`: hash-order `readdir` (shim) with page limit 1 enumerates every key exactly once in
  lexicographic order; spill to scratch above the memory budget; debris and tombstone-topped
  directories skipped.
- `CASPosixDurability`: crash injection (process kill and, in the shim, "power loss" discarding
  unsynced writes) between `stage` and `linkat`, between `linkat` and `unlinkat(tmp)`, between
  `rename` and the parent `fsync`, between tombstone and `rename`-away, and during `publish`: the
  object always reads as either the old incarnation, the complete new one, or absent-and-
  completable; debris is reclaimed by the janitor step.
- `CASPosixProbe`: each precondition fails closed on a shim that violates it (`linkat` unsupported,
  `rename` replacing a non-empty directory, `rmdir` ignoring `ENOTEMPTY`, handle not surviving
  `rename`, missing mount option, emulated-layout pool); `skip_access_check` refused.
- `CASPosixBackendDeathTest`: every `LOGICAL_ERROR` site as `EXPECT_DEATH` with `std::_Exit`.

Integration: `test_cas_posix_shared` — an NFS server container exporting one volume, two
`clickhouse-server` containers each mounting it as a **separate NFS client** (separate caches, the
mandatory options), a `ReplicatedMergeTree` table on both, inserts on both, `SYSTEM CAS GC RUN`,
`ca-fsck` `dangling=0`; kill one server mid-insert and verify the survivor fences and reclaims; run
once with the mount option missing and verify the mount is refused. Soak:
`utils/ca-soak/docker-compose-nfs.yml` with the existing scenario suite unchanged. The stateless
`cas storage` lane keeps its config and becomes the single-process durability lane.

## 11. Migration and out of scope {#out-of-scope}

Pools written by `EmulatedSingleProcess` use a flat-file layout and are refused by gate 7 of the
probe. The pool format has no production data, so no converter is written; the message names the
`clickhouse-disks` copy path as the migration.

Out of scope: Keeper-based coordination (a separate protocol track); SMB without hard links; the
single-write blob optimization (dropped from this revision: it conflicts with `BlobSource::open`'s
re-read contract and would mutate a published inode); trustworthy `list` values; any change to
persisted formats or to the request engine.
