---
description: 'Design for a coordinator-free CAS backend over local and shared POSIX filesystems (local disks, NFSv3/v4, CephFS, Lustre, GPFS): the full Cas::Backend contract from mkdir, link, rename, unlink and rmdir alone, built on one rule — no name is ever reused within the life of an object — so that every conditional operation is a server-decided create-if-absent and a delayed or replayed syscall can only ever reach the thing it was aimed at; durable through fsync; no in-process state; replaces EmulatedSingleProcess.'
sidebar_label: 'CAS POSIX shared backend'
sidebar_position: 50
slug: /superpowers/specs/cas-posix-shared-backend-design
title: 'CAS backend over local and shared POSIX filesystems'
doc_type: 'design'
---

# CAS backend over local and shared POSIX filesystems {#cas-posix-shared-backend}

Revision 3, 2026-09-08. Revision 1 used an `mtime` token and a timed per-key lock; the round-1
review (`tmp/cas-posix-spec-review-astra-r1.md`) showed a timed lock cannot fence a delayed syscall.
Revision 2 used incarnation-chain directories but reclaimed successor names and renamed the fixed
object directory; the round-2 review (`tmp/cas-posix-spec-review-astra-r2.md`) showed that every
reused name is a target for a paused writer's or remover's delayed operation, that `readdir` is not
an authoritative read, that Linux CIFS caches negative lookups for a second regardless of options,
and that blob payload reads bypass the backend. Revision 3 is built on one rule:

> **Within the life of an object, no name is ever created twice.** Chains, incarnations, markers
> and private directories all have names that are either unique (random) or monotone (never
> restarted). A delayed, replayed or paused syscall therefore either hits exactly the inode it was
> aimed at, or fails with `ENOENT`/`EEXIST` decided at the server. Nothing needs a lock, a
> timestamp, a handle-identity assumption, or an age.

Decisions taken with the user: coordinator-free (no Keeper); one mode for local and shared
filesystems; lowest-common-denominator primitives only. SMB is demoted from v1 (`§2`).

## 1. Problem {#problem}

`object_storage_type = local` routes a CAS disk to `ObjectStorageBackend::Mode::EmulatedSingleProcess`
(`ContentAddressedMetadataStorage.cpp`, `openPoolView`): the conditional contract of `Cas::Backend`
emulated with one process-wide `std::mutex` and an in-memory `mtime`-derived token map. Correct for
one process, silently wrong for two (independent token state, both pass the probe alone, the mount
logs at `Information`), and not crash-durable even for one (no `fsync` anywhere; mutable control
objects rewritten in place with `O_TRUNC`).

The goal is one backend mode, `Mode::Posix`, that satisfies the whole `Cas::Backend` contract
(`Backend/CasBackend.h`) on every filesystem in `§2`, with no in-process state, so that N servers
sharing a directory are as safe as N servers sharing an S3 bucket, and one server on a local disk is
crash-durable. The request engine, GC decisions, mount lease, ref lanes, `_ckpt` and every persisted
format are unchanged; what the mode needs outside the backend is listed in `§9`.

## 2. Supported filesystems and the facts relied on {#filesystems}

| Filesystem | Status | Mandatory mount options (`§6`) |
|---|---|---|
| Local ext4 / xfs / btrfs / zfs | supported; one process; replaces `EmulatedSingleProcess` | none |
| NFSv4, 4.1, 4.2 | supported | `hard`, `lookupcache=positive` |
| NFSv3 | supported | `hard`, `lookupcache=positive` |
| CephFS, Lustre, GPFS | supported (coherent caches) | none |
| SMB / CIFS | **not in v1**: the Linux client accepts a negative dentry for about one second regardless of `cache=`/`actimeo=` (`fs/smb/client/dir.c`, `cifs_d_revalidate`), so absence cannot be made authoritative; revisit with a Windows-side or coherence argument | — |

Facts relied on, each decided at the server and verified by the probe (`§8`):

1. `link(src, dst)` fails `EEXIST` when `dst` exists; on success `dst` and `src` are one inode.
2. `mkdir(p)` fails `EEXIST` when `p` exists.
3. `rename(dir_a, dir_b)` of a directory onto an existing non-empty directory fails
   (`ENOTEMPTY`/`EEXIST`); onto an absent name it succeeds atomically.
4. `rmdir(p)` fails `ENOTEMPTY` when `p` has any entry.
5. A `LOOKUP` of a name that does not exist returns `ENOENT` from the server, not from a client
   cache (`§6`).

Not relied on: `O_EXCL`, `flock`/`fcntl`, extended attributes, `renameat2`, timestamps, inode
numbers or handles as identity across time (handles are used only as an efficiency: a stale
handle failing `ESTALE` is an ambiguity, `§5`), listing order, listing completeness, or `rename`
onto an *empty* directory (used opportunistically; `rmdir`-then-retry is the universal fallback).

## 3. Layout {#layout}

```
P(k)/D(k)/                           the object; created once, NEVER renamed or removed (§3.3)
P(k)/D(k)/c-<m>/                     chain m (16 hex, monotone within D(k), never restarted)
P(k)/D(k)/c-<m>/<g>-<n>              incarnation n of chain m; g = 16 hex random per chain, n dense from 1
P(k)/D(k)/c-<m>/<g>-<n>/             a MARKER (a directory, so it can never be mistaken for data)
P(k)/D(k)/c-<m>/.tmp-<r>-<e>-<u>     writer scratch: server root r, writer epoch e, uuid u
P(k)/D(k)/.new-<r>-<e>-<u>/          a creator's private chain directory before rename
P(k)/D(k)/.gone-<u>/                 a chain renamed away for deletion
```

### 3.1 Values and the current incarnation {#values}

The contract's **value** of an incarnation is `c-<m>/<g>-<n>`, minted by the writer before the
write. An incarnation file is written into a private `.tmp-*`, linked to its final name, and never
modified; `VALUE ⟹ CONTENT` holds by construction. The **current incarnation** of an object is the
top of its greatest chain:

- the greatest chain is the greatest `m` for which `c-<m>` exists;
- the top of a chain is the greatest `n` for which `<g>-<n>` exists (one `g` per chain, `§4.2`);
- a top that is a **marker directory** is a **tombstone**: the object is absent (`§4.4`).

Both "greatest" are found by **lookup, not by listing**: `readdir` supplies a hint, then the
backend probes `c-<m+1>`, `<g>-<n+1>`, … with `fstatat` until the server answers `ENOENT`. Under
`§6` that answer is authoritative, so `chain(k)` returns the true current incarnation even when the
client's directory cache is stale. Cost: one extra `LOOKUP` per level in the common case.

### 3.2 Successor names are deterministic and exclusive {#successors}

Given a current incarnation `c-<m>/<g>-<n>`, its successor has exactly one name:

| Predecessor | Successor name | Created by |
|---|---|---|
| `n < K` (`K = 64`, `§9`) | `c-<m>/<g>-<n+1>` | `link` (data) or `mkdir` (tombstone) |
| `n ≥ K` (chain full) | `c-<m+1>` | `rename` of a private directory holding the new incarnation `<g'>-1` or a tombstone marker |

Because the name is a pure function of the predecessor, and facts 1–3 make its creation exclusive,
**at most one writer succeeds a given predecessor**: the contract's conditional overwrite and
conditional delete are decided by the server, and they are decided against each other (a writer's
successor and a remover's tombstone compete for the same name).

### 3.3 Nothing is ever reused {#no-reuse}

- An incarnation name `<g>-<n>` is never unlinked while its chain exists (`§4.3` reclaims space by
  renaming whole chains away, never by freeing a name inside a live chain).
- A chain name `c-<m>` is never created twice: `m` is monotone for the life of `D(k)`, and `D(k)`
  is never removed, so the floor is always discoverable (`§4.5`).
- `.tmp-*`, `.new-*` and `.gone-*` names carry a fresh uuid.
- `D(k)` is created once and never renamed or removed.

Consequences: a paused remover's `mkdir` of a tombstone, a paused writer's `link` of a successor,
a replayed `unlink`, a replayed `rename` — each names a thing that either still is exactly what it
was aimed at, or no longer exists and never will again. Path-based addressing is therefore as safe
as handle-based, which is what makes the design independent of NFSv4 volatile handles and of how a
client implements `linkat`.

Cost: a deleted object that is never reborn leaves `D(k)`, its tombstoned chain and the marker: three
empty inodes (`§10`).

## 4. Operations {#operations}

`tmp(dir, bytes)`: create `.tmp-<r>-<e>-<u>` in `dir`, write, `fsync`, keep open. `settle(dir)`:
`fsync` of the directory. `chain(k)`: `§3.1`. All errors not listed are handled per `§5`.

### 4.1 Contract methods {#contract-methods}

| Method | Implementation |
|---|---|
| `write(k, bytes, expected = absent)` | `chain(k)`: a live top ⇒ `RawConflict`. Otherwise `m₀` = the greatest existing chain (0 if none; `§4.5`), target `c-<m₀+1>`. Build `.new-*` in `D(k)` (`mkdir D(k)` first if absent, `EEXIST` fine) holding `<g>-1` via `tmp` + `link`; `settle`; `rename(.new-*, c-<m₀+1>)`. Success ⇒ `settle(D(k))`, return `c-<m₀+1>/<g>-1`. Target exists (`ENOTEMPTY`/`EEXIST`) ⇒ remove our `.new-*` (`§4.3`), `RawConflict` |
| `write(k, bytes, expected = c-<m>/<g>-<n>)` | `fstatat(D(k)/c-<m>, <g>-<n>)` must succeed, else `RawConflict` (the token names a chain this object does not have: rebirth, or a foreign token). `n < K`: `tmp` in `c-<m>`, `link(tmp, c-<m>/<g>-<n+1>)`: `EEXIST` ⇒ (`§5` ownership check, else) `RawConflict`; success ⇒ return `c-<m>/<g>-<n+1>`. `n ≥ K`: as the absent path with target `c-<m+1>` and content `<g'>-1`; success ⇒ return `c-<m+1>/<g'>-1`, then opportunistically `§4.3` on `c-<m>`. In both cases `unlink(tmp)` and `settle` afterwards |
| `remove(k, c-<m>/<g>-<n>)` | `chain(k)`: current is not that value ⇒ `Mismatch` (a successor exists) or `Gone` (absent). Create the successor **as a marker**: `n < K` ⇒ `mkdir(c-<m>/<g>-<n+1>)`; `n ≥ K` ⇒ `rename` a private directory holding one marker `<g'>-1/` onto `c-<m+1>`. `EEXIST`/`ENOTEMPTY` ⇒ `Mismatch` (a writer's successor won; the object is untouched). Success ⇒ the object is absent and this chain can never be extended; then `§4.3` on the chain's data files; `Removed`. `DeleteMarker` is never returned |
| `publish(request)` | unconditional by protocol: loop { `chain(k)`; absent (no chain or tombstone) ⇒ the absent-`write` path with the blob body (a tombstone top is a full chain for this purpose: the rebirth is `c-<m+1>`); present ⇒ the conditional path against the current value }, until a write succeeds or the internal attempt bound (`§9`) is hit, which is reported as a transport failure. The envelope + bounded payload copy and exact size check are those of `emuPublishBlobAtomically` today. Older incarnations are reclaimed by `§4.3` |
| `read(k)` | `chain(k)`; absent ⇒ nullopt; `open` the incarnation, `fstat`, read whole; value = the name opened |
| `stream(k)` | as `read`, returning a `ReadBufferFromFileDescriptor` owning the descriptor. A remote `unlink` of the chain while streaming surfaces as the read error it is (NFSv3 `ESTALE`); the callers (GC, fsck) fail closed on read errors |
| `head(k)` | `chain(k)`; absent ⇒ nullopt; `fstatat` size |
| `list(prefix, cursor, limit)` | `§4.6` |
| `removeManyWriteOnce(keys)` | for each: `rename(D(k)/c-<m>, .gone-<u>)` for every chain, delete contents, `rmdir`; then `rmdir D(k)`. Allowed to remove `D(k)` because the engine proves by `WriteOnceKey` (`Primitives/CasWriteOnceKey.h`) that the key is never created again, so no name under it can ever be reused. `ENOENT` is success |
| `probeSentinelRaw(k)` | `chain(k)` with error classification: pool root absent ⇒ `ContainerAbsent`; `EACCES`/`EPERM` ⇒ `AccessDenied`; other errors ⇒ `Indeterminate`; absent ⇒ `KeyAbsent`; present ⇒ `Present` with the body. Ordinary `read`/`head` classify a missing pool root the same way and throw, never return nullopt, so root loss is never flattened into key absence |

### 4.2 Why a chain has one `g` {#one-gen}

A chain directory is created by exactly one `rename` (fact 3), from a private directory holding
`<g>-1`; every later incarnation in it is `<g>-<n+1>` linked against an existing `<g>-<n>`
(checked by `fstatat` first). No path creates a second `g` in a chain.

### 4.3 Reclaiming space {#reclaim}

Space is reclaimed only by moving whole chains, never by freeing a name inside a live chain:

- When `c-<m+1>` exists, `c-<m>` is superseded: anyone may `rename(D(k)/c-<m>, D(k)/.gone-<u>)`,
  then `unlink` its entries and `rmdir` it. `c-<m>` is never created again (`§3.3`), so a delayed
  `link`/`mkdir` aimed at `c-<m>/…` fails `ENOENT`, and a replayed `rename` finds no source.
- A tombstoned chain (top is a marker) has its data files deleted the same way, but **the chain
  directory and its marker stay** until a rebirth chain `c-<m+1>` exists; they are the floor
  (`§4.5`).
- `.tmp-*` and `.new-*` belong to the server root and writer epoch in their name. They are removed
  by their writer on every path, and otherwise only when that epoch is dead by the mount-lease
  protocol's own certificate (a fenced or farewelled slot, `Pool/CasServerRoot.cpp`), which the
  namespace janitor already knows. **No age is used**: a slow live creator is never touched, and a
  fenced creator's later `rename` of a `.new-*` that was renamed away fails `ENOENT` (`§5`).
- `.gone-*` directories are deleted by whoever created them, else by the janitor (unique names,
  contents are dead by construction).

### 4.4 Tombstones {#tombstones}

A tombstone is the successor name created as a directory. It cannot be confused with data (a
data incarnation is a regular file), it is exclusive against a data successor (facts 1–3), and it
is never removed while its chain exists. Reads of a tombstoned object return absent. The next write
of the object is a rebirth at `c-<m+1>` (`§3.2`), so a tombstone is simply "a full chain whose top
is not data". A remover that crashes after the tombstone has already completed the contract's work
(`Removed` semantics: the incarnation is unreachable); the data files are reclaimed by anyone via
`§4.3`.

### 4.5 The floor {#floor}

`m₀` for a rebirth is the greatest existing `c-<m>` (found by lookup, `§3.1`). Since a tombstoned
chain is never renamed away before its successor exists, and `D(k)` is never removed, the floor is
always present for a key that ever existed, and `m` never restarts. The three-inode residue per
never-reborn deleted key is the price of not having a server that enforces conditions; it is ~12 KiB
of metadata on ext4 and zero data, and `§10` sizes it against the pool.

### 4.6 Listing {#listing}

`list` performs one complete recursive enumeration of object directories under `prefix` (an object
is a `D(k)` whose `chain` is a live data incarnation), sorts it lexicographically, spools it to a
scratch file under `cas_scratch_path`, and pages from that spool; the cursor is `<spool id>:<offset>`
and the spool lives until the cursor is exhausted or `posix_list_spool_ttl_sec` passes, so a full
walk costs one enumeration, not one per page. `.new-*`, `.gone-*`, `.tmp-*`, `.nfs*`, markers and
tombstoned or empty objects are skipped. Values are absent (`supportsListTokens = false`).

Freshness: before reading the top-level directory of the enumeration on an NFS mount, the backend
creates and unlinks a unique dotfile in it; the client's own mutation updates its cached change
attribute and invalidates the cached `readdir` of that directory, so the **top level** of every
listing is fresh. That is what the engine's emptiness proofs consume ("no key under
`cas/manifests/<root>/`" is "the directory has no entries"); deeper levels remain hints, as
everywhere in CAS.

## 5. Ambiguity and lost replies {#ambiguity}

A hard NFS mount retransmits an RPC whose reply was lost; `link`, `mkdir`, `rename`, `unlink` and
`rmdir` are not idempotent. Per primitive:

| Primitive | After a possibly-lost reply |
|---|---|
| `link(tmp, name)` → `EEXIST` | `stat(name)` vs `fstat(tmp)`: same `st_dev`/`st_ino` ⇒ ours, success. Different ⇒ real conflict. `stat` fails ⇒ ambiguity |
| `mkdir(marker)` → `EEXIST` | a marker is only ever created by a remover of *this* predecessor and the name is exclusive: if `name` is a directory ⇒ the tombstone exists ⇒ our `remove` succeeded (whether by us or by a concurrent remover of the same incarnation, the contract's outcome is `Removed` either way); if it is a file ⇒ a writer won ⇒ `Mismatch` |
| `rename(.new-*, c-<m>)` → `ENOENT` (source) | `stat(c-<m>/<g>-1)` with our `g` ⇒ ours. Otherwise **ambiguity** (our create may have landed and been superseded); never `RawConflict` |
| `rename(c-<m>, .gone-*)` → `ENOENT` | done by someone; if `.gone-<u>` exists it is ours to delete |
| `unlink`/`rmdir` → `ENOENT` | success |
| any `ESTALE` | ambiguity (the handle outlived the name; re-resolve by path on retry) |

Everything not resolved above — timeouts, `EIO`, a failed `fsync` or `close` after a mutation — is a
**transport ambiguity**: the backend throws a `Poco::Exception`-derived exception, the class the
request engine settles by an exact read (`Backend/CasRequests.cpp`; the write path re-throws any
non-`Poco::Exception` as a local fault and settles nothing). A bare `std::system_error` never
escapes the backend, and a `DB::Exception` is used only for definite, non-transport refusals. The
engine's wedge rule then applies unchanged.

`CAS_WRITE_UNATTRIBUTED` cannot arise: the value of a completed write is the name chosen before it.

## 6. Absence and caches {#absence}

Every engine consumer of a null `head`/`read`/`stream` was enumerated in the round-1 review. The
authorization-class ones — the blob `.meta` read on a dedup hit (absent reads as `Clean`,
`Pool/CasPartWriteTxn.cpp`), GC's blob `HEAD` before marker cleanup (`Gc/CasGc.cpp`), the owner and
epoch claims' emptiness proofs (`Pool/CasServerRoot.cpp`, `Backend/CasSentinelProbe.cpp`), the
lifecycle `IdentityLost` verdict and the empty-table probe (`Pool/CasPool.cpp`,
`ContentAddressedMetadataStorage.cpp`), and the request engine's settlement read — require that
absence be **server-decided**. The backend guarantees it by construction:

- every "does `x` exist" question is a `LOOKUP` of a specific name (`§3.1`), never a listing;
- the top level of a listing is made fresh by a local mutation (`§4.6`);
- on NFS, `lookupcache=positive` makes the Linux client send every negative lookup to the server
  (`fs/nfs/dir.c`: negative dentries are not cached under that option even while the parent's
  attributes are), which is exactly and only what the two rules above need; positive entries may be
  cached because a name, once it exists, is never replaced by different content (`§3.3`);
- on local and coherent cluster filesystems nothing is needed.

The mount-option gate is **mandatory** (`§8`): for filesystem types `nfs`/`nfs4` a writable mount is
refused without `lookupcache=positive` (or `none`) and `hard`. The mount is identified by the mount
id of the pool root (`statx` `STATX_MNT_ID`) matched against `/proc/self/mountinfo`, so bind mounts,
autofs (the root is opened first, which triggers it) and container namespaces resolve to the
effective superblock options; a pathname-prefix match is not used.

Positive staleness is not a hazard: an incarnation never changes, and `chain` never trusts a listing
for "which is current". The successor's epoch seal in the engine is a create-if-absent
(`rename` of a private chain onto `c-1` of the slot key), so a stale view can only make it fail and
re-read.

## 7. Durability {#durability}

- An incarnation file is `fsync`ed before it is linked; a private chain directory is `fsync`ed
  before it is renamed.
- After a name changes, `fsync` the directory; after a directory is created or renamed, `fsync` its
  parent; newly created ancestors are `fsync`ed in order. On Linux NFS a directory `fsync` is a
  no-op because namespace operations are synchronous at the server.
- A failed `fsync` or `close` after a mutation is an ambiguity (`§5`), never "nothing written".
- An NFS export with `async`, or storage that acknowledges before persisting, is outside what a
  client can verify: an operator precondition, documented next to the S3 ones.

## 8. Capability probe {#probe}

`checkPoolPreconditions` for `Mode::Posix` runs before the generic seven-step battery under a
per-mount random prefix and fails closed with `NOT_IMPLEMENTED` naming the failed fact:

1. Mount-option gate (`§6`).
2. Fact 1: `link(a, b)` succeeds and `b` is `a`'s inode; `link(c, b)` fails `EEXIST`.
3. Fact 2: second `mkdir` of one path fails `EEXIST`.
4. Fact 3: `rename` of a directory onto a non-empty directory fails; onto an absent name succeeds.
5. Fact 4: `rmdir` of a non-empty directory fails `ENOTEMPTY`.
6. `fsync` of a file and of a directory succeed.
7. Layout check: a regular file where `D(k)` is expected under a plane is a pool written by the
   retired emulated layout ⇒ refuse, naming migration (`§11`).

`checkSkipAccessCheckSupport` **refuses** `skip_access_check = true` for a writable `Mode::Posix`
mount (gate 1 is what makes absence authoritative, and `Pool::open` skips `checkPoolPreconditions`
on that path, `Pool/CasPool.cpp`). `checkConditionalWriteSingleAttemptSupport` stays a no-op.

The generic battery then runs unchanged and exercises the code paths a second process would. What
a single-client probe cannot verify is cross-client atomicity of facts 1–4 (an operator
precondition; the two-client integration test in `§10`) and export durability.

## 9. What this needs outside the backend {#outside-backend}

- **Blob payload reads.** `ContentAddressedMetadataStorage` hands the logical blob key to the
  object storage for ranged reads (`ContentAddressedMetadataStorage.cpp`, the read-path
  `StoredObject` construction), and `LocalObjectStorage` opens that path as a file. In
  `Mode::Posix` the key is a directory, so the read path must resolve the key to its current
  incarnation file **through the backend** (`Backend::resolveReadPath`, new) before constructing the
  `StoredObject`; offsets are unchanged (the envelope is inside the incarnation file) and the cache
  identity becomes the incarnation path, which is immutable. This is a wiring change in the CAS
  metadata storage, not in the engine's decisions.
- **Dialect.** None new: `Dialect::Emulated`, grammar "non-empty" (`Backend/CasEtag.cpp`). Values
  are persisted as strings in record streams (`Formats/CasRecordStreamFormat.cpp`) and compared
  only within one key's context; `c-<m>/<g>-<n>` fits.
- **Settings** through `ContentAddressedSettings` / `openPoolView`: `posix_chain_length` (`K`,
  default 64), `posix_list_memory_budget_bytes` (default 256 MiB), `posix_list_spool_ttl_sec`
  (default 600), `posix_publish_max_attempts` (default 16: the internal bound on the append loop,
  after which a transport failure is thrown for the engine's own retry; `TransportAccess` carries no
  deadline).
- **Mode selection.** `object_storage_type = local` selects `Mode::Posix`; `EmulatedSingleProcess`
  and its `emu_*` state are deleted.
- **Janitor.** The namespace janitor's page (`Gc/CasNamespaceJanitor.cpp`) gains a backend step
  "reclaim under this prefix": superseded chains, `.gone-*`, and `.tmp-*`/`.new-*` whose epoch is
  dead by the mount-lease certificate. No age participates.
- **Nothing in formats, GC decisions, the ref lanes, the mount lease or the request engine changes.**

## 10. Cost {#cost}

| Item | Cost |
|---|---|
| Object at rest | `D(k)` + one chain + one file: 3 inodes |
| Hot mutable key (`_ckpt`, `mount`) | one incarnation file per write, reclaimed a chain (`K` writes) at a time: at most `2K` files transiently, two chain directories |
| `head`/`read` | `readdir` hint + one `LOOKUP` per level (2) + `open` + `read`: ~5 RPCs on NFS, sub-millisecond locally |
| Conditional write | `tmp` create + write + `fsync` + `link` + `unlink` + directory `fsync`: ~6 RPCs |
| Deleted, never reborn key | 3 empty inodes (≈12 KiB metadata on ext4). For blobs that is per content hash ever reclaimed; the janitor may not remove them (`§3.3`). A pool that churns 10 M distinct blobs leaves ≈120 GiB of metadata: sized here so the decision is explicit |
| `list` | one full enumeration per walk, spooled; `O(M log M)` per walk, not per page |

The residue for deleted keys is the one cost that is structural. It is removable only by an actor
that can prove no writer can still be aiming at names under `D(k)`, which no coordinator-free
protocol can prove; a future GC-owned deep reclaim under the mount-lease certificates of *all*
roots is possible but is out of scope here.

## 11. Tests {#tests}

Unit (`CAS*` suites, gtest, standard gate filter):

- `CASPosixBackend`: the full contract over a temporary directory, then the same battery driven by
  two independent backend instances on one directory, including: create-vs-create (one `rename`
  wins); overwrite-vs-overwrite from one predecessor (one `link` wins); remove-vs-overwrite from one
  predecessor (marker vs file at one name; the loser reports `Mismatch`/`RawConflict`; never both
  `Removed` and success); publish-vs-remove (a publication after the tombstone rebirths at
  `c-<m+1>`; the tombstone stays); **two intervening successors then a delayed tombstone `mkdir`**
  (fails `EEXIST` on the occupied name, `Mismatch`); a stale token from before a rebirth (`fstatat`
  of the predecessor fails ⇒ `RawConflict`, no second `g` in a chain); chain rotation at `K` with
  two writers racing `c-<m+1>`; a delayed `rename` of a superseded chain after it was already
  reclaimed (`ENOENT`, harmless).
- `CASPosixLostReply`: a fault-injecting filesystem shim drops the reply of a successful `link`,
  `mkdir`, `rename`, `unlink`, `rmdir` once; ownership resolves by inode or by `g`; landed-then-
  superseded creation throws a `Poco::Exception`-derived ambiguity, never `RawConflict`; no
  `std::system_error` escapes.
- `CASPosixChain`: a shim serving stale `readdir` (missing the newest chain / incarnation / marker)
  while lookups are fresh: `chain` still returns the true current; a stale listing never yields an
  old value.
- `CASPosixList`: hash-order `readdir` with page limit 1 enumerates every key exactly once in order;
  spool reuse across pages; top-level freshness (a key created by the second instance after the
  first instance cached the directory is listed).
- `CASPosixDurability`: crash injection (kill, and shim "power loss" discarding unsynced writes) at
  each step of create, overwrite, remove, rotate and publish: the object always reads as the old
  incarnation, the complete new one, or absent; residue is reclaimed by the janitor step and only
  for dead epochs (a live slow creator's `.new-*` is untouched).
- `CASPosixProbe`: each fact fails closed on a violating shim; missing mount option; emulated
  layout; `skip_access_check` refused.
- `CASPosixReadPath`: table reads (full and ranged, with and without a cache disk) after
  publication, after a republish (new incarnation), after a rebirth.
- `CASPosixBackendDeathTest`: every `LOGICAL_ERROR` site as `EXPECT_DEATH` with `std::_Exit`.

Integration: `test_cas_posix_shared` — an NFS server container, two `clickhouse-server` containers
each mounting it as a separate NFS client with the mandatory options, a `ReplicatedMergeTree` on
both, inserts on both, `SYSTEM CAS GC RUN`, `ca-fsck` `dangling=0`; kill one mid-insert and verify
the survivor fences and reclaims; mount without `lookupcache=positive` and verify refusal. Soak:
`utils/ca-soak/docker-compose-nfs.yml` with the existing scenario suite unchanged; report inode
residue after the run. The stateless `cas storage` lane becomes the single-process durability lane.

## 12. Migration and out of scope {#out-of-scope}

Pools written by `EmulatedSingleProcess` use a flat-file layout and are refused by probe gate 7; no
converter (no production data). Out of scope: Keeper coordination; SMB (`§2`); the single-write
blob optimization (conflicts with `BlobSource::open`); trustworthy `list` values; deep reclaim of
deleted-key residue; any change to persisted formats or the request engine.
