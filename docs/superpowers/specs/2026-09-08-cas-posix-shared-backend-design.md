---
description: 'Design for a coordinator-free CAS backend over local and shared POSIX filesystems (local disks, NFSv3/v4, CephFS, Lustre, GPFS): the full Cas::Backend contract from link, mkdir, rename, unlink and rmdir, where every conditional operation is a server-decided create-if-absent of a deterministic successor name, every mutation is sourced from a uniquely named intent object created before the predecessor is observed, and a name is freed only when no intent on it exists — so a delayed or stalled syscall is refused at the server by a missing source, exactly as If-Match is refused by S3; durable through fsync; no in-process state and no timing assumption; replaces EmulatedSingleProcess.'
sidebar_label: 'CAS POSIX shared backend'
sidebar_position: 50
slug: /superpowers/specs/cas-posix-shared-backend-design
title: 'CAS backend over local and shared POSIX filesystems'
doc_type: 'design'
---

# CAS backend over local and shared POSIX filesystems {#cas-posix-shared-backend}

Revision 4, 2026-09-09. Three review rounds (`tmp/cas-posix-spec-review-astra-r{1,2,3}.md`)
established one theorem: on POSIX the server enforces only `EEXIST`/`ENOTEMPTY` on an existing name,
so an occupied name is the only fence, and any name freed while a paused or stalled writer still
aims at it becomes a false-success target — a timed lock (rev 1), reclaimed successor names (rev 2)
and reclaimed chain names (rev 3) all fail the same way, and the stall can be inside the syscall
itself (an NFS RPC retransmitted after a partition), where no user-space deadline can see it.
Revision 4 adds the one rule that closes it without a timing assumption, borrowed from the mount
lease's `min_active_build_sequence`: **a writer declares its intent as an object it owns before it
observes the predecessor, its mutation is sourced from that object by path, and a name is freed only
when no intent on it exists.** A stalled `link`/`rename` executing late finds its *source* gone and
fails at the server — the condition travels with the request, which is what `If-Match` does on S3.

Decisions taken with the user: coordinator-free (no Keeper); one mode for local and shared
filesystems; lowest-common-denominator primitives only; SMB out of v1 (`§2`).

## 1. Problem {#problem}

`object_storage_type = local` routes a CAS disk to `ObjectStorageBackend::Mode::EmulatedSingleProcess`
(`ContentAddressedMetadataStorage.cpp`, `openPoolView`): the conditional contract of `Cas::Backend`
emulated with one process-wide `std::mutex` and an in-memory `mtime`-derived token map. Correct for
one process, silently wrong for two, and not crash-durable even for one (no `fsync` anywhere; mutable
control objects rewritten in place with `O_TRUNC`).

The goal is one backend mode, `Mode::Posix`, that satisfies the whole `Cas::Backend` contract
(`Backend/CasBackend.h`) on every filesystem in `§2`, with no in-process state and no assumption
about how long a process or a syscall may stall, so that N servers sharing a directory are as safe
as N servers sharing an S3 bucket. The request engine, GC decisions, mount lease, ref lanes, `_ckpt`
and every persisted format are unchanged; what the mode needs outside the backend is in `§9`.

## 2. Supported filesystems and the facts relied on {#filesystems}

| Filesystem | Status | Mandatory mount options (`§6`) |
|---|---|---|
| Local ext4 / xfs / btrfs / zfs | supported; one process; replaces `EmulatedSingleProcess` | none |
| NFSv4, 4.1, 4.2 | supported | `hard`, `lookupcache=positive` |
| NFSv3 | supported | `hard`, `lookupcache=positive` |
| CephFS, Lustre, GPFS | supported (coherent caches) | none |
| SMB / CIFS | not in v1: the Linux client keeps a negative dentry for about one second regardless of options (`fs/smb/client/dir.c`, `cifs_d_revalidate`) | — |

Facts relied on, each decided at the server and verified by the probe (`§8`):

1. `link(src, dst)` fails `EEXIST` when `dst` exists and `ENOENT` when `src` does not; on success
   `dst` and `src` are one inode.
2. `mkdir(p)` fails `EEXIST` when `p` exists.
3. `rename(src, dst)` fails `ENOENT` when `src` does not exist; for directories it fails
   (`ENOTEMPTY`/`EEXIST`) when `dst` is a non-empty directory; onto an absent name it succeeds
   atomically.
4. `rmdir(p)` fails `ENOTEMPTY` when `p` has any entry.
5. A `LOOKUP` of an absent name is answered by the server, not by a client cache (`§6`).
6. A client's own create/unlink/rename in a directory invalidates that client's cached listing of
   it (Linux NFS: `nfs_post_op_update_inode` sets `NFS_INO_INVALID_DATA` on the parent for v3 and
   v4 creates).

Not relied on: `O_EXCL`, `flock`/`fcntl`, extended attributes, `renameat2`, timestamps, handle
identity across time (a stale handle's `ESTALE` is an ambiguity, `§5`), listing order or
completeness, any clock, any bound on the duration of a pause or of a syscall.

## 3. Layout {#layout}

```
P(k)/D(k)/                            the object directory; removable only when it holds no intent (§4.5)
P(k)/D(k)/c-<m>/                      chain m (16 hex; monotone while D(k) exists)
P(k)/D(k)/c-<m>/<g>-<n>               incarnation n of chain m; g = 16 hex random per chain, n dense from 1
P(k)/D(k)/c-<m>/<g>-<n>/              a MARKER (directory): the tombstone that ends a chain
P(k)/D(k)/c-<m>/.tmp-<r>-<e>-<u>      an INTENT to extend chain m with data: server root r, writer epoch e, uuid u
P(k)/D(k)/c-<m>/.tomb-<r>-<e>-<u>/    an INTENT to end chain m with a tombstone
P(k)/D(k)/.new-<r>-<e>-<u>/           an INTENT to create the next chain (a private chain directory)
P(k)/D(k)/.gone-<u>/                  a chain, or a killed intent, on its way to deletion
P(k)/.d-<u>                           a listing-freshness dotfile (§4.6), created and removed at once
```

### 3.1 Values and the current incarnation {#values}

The contract's **value** is `c-<m>/<g>-<n>`, minted by the writer before the write. An incarnation
file is written into an intent, linked to its final name, and never modified: `VALUE ⟹ CONTENT`
by construction. The **current incarnation** is the top of the greatest chain, both found by
**lookup, never by listing**: `readdir` gives a hint, then `fstatat` probes `c-<m+1>`, `<g>-<n+1>`,
… until the server answers `ENOENT` (fact 5). A probe that finds the hinted chain gone (`ENOENT`
on `c-<m>` itself) restarts from a fresh listing (fact 6 applied by the prober: it creates and
removes a `.d-<u>` in `D(k)` first). A top that is a marker is a **tombstone**: the object is
absent. A chain whose greatest chain directory is empty is in transition (a creator's `rename`
landed before its content — impossible by `§4.2`, hence an error, never absence).

### 3.2 Successor names are deterministic and exclusive {#successors}

For a current incarnation `c-<m>/<g>-<n>` the successor has exactly one name:

| Predecessor | Successor name | Created by |
|---|---|---|
| `n < K(k)` | `c-<m>/<g>-<n+1>` | `link` from an intent (data) or `mkdir` (tombstone) |
| `n ≥ K(k)` | `c-<m+1>` | `rename` of a `.new-*` intent holding `<g'>-1` (data) or a marker `<g'>-1/` (tombstone) |

`K(k)` is a **protocol constant** by plane, not a setting: `1` under `blobs/` (every publication is
a new chain, so a superseded body is reclaimable at once), `64` elsewhere. Because the successor
name is a pure function of the predecessor and facts 1–3 make its creation exclusive, at most one
writer succeeds a given predecessor, and a writer's successor and a remover's tombstone compete
for the same name: the contract's conditional overwrite and conditional delete are decided by the
server, against each other.

### 3.3 The intent rule {#intent-rule}

1. **Declare before observing.** A writer creates its intent object (`.tmp-*` in the chain it may
   extend, or `.new-*` in `D(k)`) **before** it looks up the predecessor. The intent's name carries
   the writer's server root and writer epoch.
2. **Mutate from the intent, by path.** The successor is created by `link(intent_path, name)` or
   `rename(intent_path, name)`. If the intent has been killed (`§4.4`), the server refuses with
   `ENOENT` on the source — at execution time, however late.
3. **Free a name only where no intent exists.** A superseded chain (`c-<m>` when `c-<m+1>` exists),
   a tombstoned chain, or `D(k)` itself may be renamed away for deletion only after a **fresh**
   listing (fact 6: the reclaimer first creates and removes `.d-<u>` in the directory it is about to
   list) shows no intent in it. An intent from a live epoch defers the reclaim; an intent from an
   epoch that is dead by the mount-lease certificate (`§4.4`) is killed first, then the reclaim
   proceeds.

Why this is sufficient. A writer can only aim at a name derived from a predecessor it observed
(rule 1: after its intent existed). A reclaimer frees only superseded names, and "superseded" is a
server-side fact established before the reclaimer's fresh listing. Either the writer's intent
existed at that listing — then the reclaim was deferred (live epoch) or the intent was killed (dead
epoch) and the writer's mutation fails `ENOENT` — or the intent was created after the listing, in
which case the writer observed the predecessor after the superseding successor existed and aims at
a later name. No clock, no bound on pauses, and nothing about *where* the stall happens: a `LINK`
RPC retransmitted a minute later is refused by the same `ENOENT`.

## 4. Operations {#operations}

`intent(dir)`: create `.tmp-<r>-<e>-<u>` in `dir` (or `.new-<r>-<e>-<u>/` in `D(k)`), then write and
`fsync`. `settle(dir)`: `fsync` of a directory. `chain(k)`: `§3.1`. `fresh(dir)`: create and unlink
`.d-<u>` in `dir`, then `readdir`. Errors not listed are handled per `§5`.

### 4.1 Contract methods {#contract-methods}

| Method | Implementation |
|---|---|
| `write(k, bytes, expected = absent)` | `mkdir D(k)` (`EEXIST` fine); `intent`: `.new-*` in `D(k)` holding `<g>-1` (written via a `.tmp-*` inside it and linked); `chain(k)`: a live top ⇒ remove own intent, `RawConflict`; else target `c-<m₀+1>` (`m₀` = greatest existing chain, 0 if none); `rename(.new-*, c-<m₀+1>)`: success ⇒ `settle(D(k))`, return `c-<m₀+1>/<g>-1`; `ENOTEMPTY`/`EEXIST` ⇒ remove own intent, `RawConflict`; `ENOENT` on source ⇒ `§5` |
| `write(k, bytes, expected = c-<m>/<g>-<n>)` | `n < K`: `intent` `.tmp-*` in `c-<m>` (`ENOENT` on `c-<m>` ⇒ `RawConflict`); `fstatat(c-<m>, <g>-<n>)` must succeed else remove intent, `RawConflict`; `link(.tmp-*, c-<m>/<g>-<n+1>)`: success ⇒ `unlink(.tmp-*)`, `settle`, return; `EEXIST` ⇒ `§5` ownership check, else remove intent, `RawConflict`; `ENOENT` on source ⇒ `§5`. `n ≥ K`: as the absent path but with `chain` required to be exactly the expected value and target `c-<m+1>` |
| `remove(k, c-<m>/<g>-<n>)` | `chain(k)`: not that value ⇒ `Mismatch` / `Gone`. Tombstone = the successor name **as a directory**, created from an intent so that a killed intent refuses with `ENOENT` exactly like a data write: `n < K` ⇒ `mkdir(c-<m>/.tomb-<r>-<e>-<u>)`, then `rename(.tomb-*, c-<m>/<g>-<n+1>)`; `n ≥ K` ⇒ `.new-*` holding a marker `<g'>-1/`, `rename` onto `c-<m+1>`. `EEXIST`/`ENOTEMPTY` ⇒ `Mismatch`; success ⇒ `Removed`; the data is reclaimed later by `§4.4`. `DeleteMarker` is never returned |
| `publish(request)` | unconditional by protocol: loop { `chain(k)`; absent ⇒ the absent-`write` path with the body; present ⇒ the conditional path against the current value } until success or `posix_publish_max_attempts`, then a transport failure. Envelope + bounded payload copy + exact size check as `emuPublishBlobAtomically` today |
| `read(k)` / `head(k)` / `stream(k)` | `chain(k)`; absent ⇒ nullopt / null; open the incarnation; value = the name opened. `stream` returns a `ReadBufferFromFileDescriptor` owning the descriptor; a remote unlink surfaces as the read error it is |
| `list(prefix, cursor, limit)` | `§4.6` |
| `removeManyWriteOnce(keys)` | for each key: `§4.4` on every chain and on `D(k)`, subject to the intent rule like any other reclaim (the `WriteOneKey` guarantee is not needed for safety; it only makes rebirth impossible in practice) |
| `probeSentinelRaw(k)` | `chain(k)` with error classification: pool root absent ⇒ `ContainerAbsent`; `EACCES`/`EPERM` ⇒ `AccessDenied`; other errors ⇒ `Indeterminate`; absent ⇒ `KeyAbsent`; present ⇒ `Present` with the body. Ordinary `read`/`head` classify a missing pool root the same way and throw |

### 4.2 Why a chain has one `g` and is never empty {#one-gen}

A chain directory becomes visible only by one `rename` of a `.new-*` that already holds `<g>-1`
(fact 3); every later incarnation in it is `<g>-<n+1>` linked against an existing `<g>-<n>`. Nothing
creates a second `g` in a chain, and nothing empties a live chain: names inside a live chain are
never unlinked (`§4.4` reclaims by moving whole chains).

### 4.3 Lost replies and ownership {#ownership}

`§5`.

### 4.4 Reclaim {#reclaim}

Reclaim is the only way a name is ever freed, and it obeys `§3.3` rule 3:

1. `fresh(dir)`; if any `.tmp-*`/`.new-*`/`.tomb-*` in `dir` belongs to a **live** epoch (per the
   mount-lease certificate: the slot is neither fenced nor farewelled nor superseded by a later
   epoch — `isCreatorFenceTerminal`, `Pool/CasServerRoot.cpp`; an unreadable slot is treated as live),
   stop: deferred.
2. Kill every intent of a dead epoch. **How depends on the primitive it sources**: an NFS `LINK`
   carries the source as a file handle, and a handle survives `rename`, so a `.tmp-*` is killed by
   `unlink`, which drops the inode's last link and makes its handle stale (`ESTALE` at execution);
   `RENAME` names its source by directory handle plus name, so a `.new-*`/`.tomb-*` is killed by
   `rename` to `D(k)/.gone-<u>` (`ENOENT` at execution). On a local filesystem the path is resolved
   at syscall time under the rename lock, so either works. A stalled mutation sourced from a killed
   intent is therefore refused at the server whenever it executes.
3. Free the target: `rename(c-<m>, D(k)/.gone-<u>)` for a superseded or tombstoned chain; for
   `D(k)` itself, when it holds no chain with a live top and no intent, `rmdir D(k)` (fact 4 makes
   this safe against a concurrent creator, whose `mkdir`+`.new-*` either precedes the `rmdir` and
   makes it fail, or follows it and recreates `D(k)` legitimately).
4. Delete `.gone-*` contents and `rmdir`.

Who reclaims: the writer that rotated a chain (opportunistically, for the chain it superseded), the
publisher that rebirthed a blob, and the namespace janitor's physical sweep (`§9`). Reclaim never
blocks a writer: a deferred reclaim is retried on the next visit.

Rebirth after full removal is safe: `D(k)` cannot be removed while any intent exists in it, so a
later creator is one that declared its intent after the removal and observed no chain.

### 4.5 Residue {#residue}

None permanent. A deleted key that is never reborn leaves nothing once its chains and `D(k)` are
reclaimed. Transient: intents of live epochs, superseded chains awaiting reclaim, `.gone-*`.

### 4.6 Listing {#listing}

`list` performs one complete recursive enumeration under `prefix`, sorted, spooled to a scratch
file under `cas_scratch_path` and paged by `<spool id>:<offset>` (spool kept until exhausted or
`posix_list_spool_ttl_sec`); an object is a `D(k)` whose `chain` is live data; `.new-*`, `.gone-*`,
`.tmp-*`, `.tomb-*`, `.d-*`, `.nfs*`, markers and tombstoned or empty objects are skipped;
values absent (`supportsListTokens = false`). On NFS mounts **every directory the enumeration
descends is listed after `fresh`** (fact 6), so the listing is complete as of the enumeration for
every level — the engine's emptiness proofs (`Pool/CasServerRoot.cpp`, `Backend/CasSentinelProbe.cpp`)
consume `keys`, never raw entries, and the `.d-*` names are filtered before pages are built. Cost:
two extra RPCs per directory per enumeration.

## 5. Ambiguity and lost replies {#ambiguity}

| Primitive | After a possibly-lost reply |
|---|---|
| `link(intent, name)` → `EEXIST` | `stat(name)` vs `stat(intent)`: same `st_dev`/`st_ino` ⇒ ours, success. Different ⇒ real conflict |
| `link`/`rename` → `ENOENT` on the source | the intent was killed (`§4.4`) or our earlier attempt consumed it: **ambiguity** |
| `rename(.new-*, c-<m>)` → `ENOENT` (source) | `stat(c-<m>/<g>-1)` with our `g` ⇒ ours. Otherwise ambiguity |
| `rename(.tomb-*, name)` → `EEXIST` | `name` is a directory ⇒ the tombstone exists (ours or a concurrent remover's of the same incarnation: `Removed` either way); a file ⇒ `Mismatch` |
| `rename(c-<m>, .gone-*)` → `ENOENT` | done by someone; own `.gone-<u>` is ours to delete |
| `unlink`/`rmdir` → `ENOENT` | success |
| any `ESTALE` | ambiguity |

Everything else after a request may have been sent — timeouts, `EIO`, a failed `fsync`/`close` —
is a **transport ambiguity**: the backend throws a `Poco::Exception`-derived exception, the class
the request engine settles by an exact read (`Backend/CasRequests.cpp`; a non-`Poco::Exception` is
re-thrown as a local fault and settles nothing). No `std::system_error` escapes; a `DB::Exception`
is used only for definite refusals. `CAS_WRITE_UNATTRIBUTED` cannot arise.

## 6. Absence and caches {#absence}

The authorization-class consumers of absence in the engine (blob `.meta` on a dedup hit, GC's
`HEAD` before marker cleanup, the owner/epoch emptiness proofs, `IdentityLost`, the empty-table
probe, the settlement read) require server-decided absence. The backend guarantees it by
construction: existence is always a `LOOKUP` of a specific name (`§3.1`); listings are fresh at
every level (`§4.6`); on NFS `lookupcache=positive` makes the client send every negative lookup to
the server (`fs/nfs/dir.c`), which is all the two rules need; positive caching is harmless because
a name, once it exists, never changes content. The mount-option gate is mandatory (`§8`): for
`nfs`/`nfs4` a writable mount is refused without `lookupcache=positive` (or `none`) and `hard`.
The mount is identified by the pool root's open descriptor: `mnt_id` from
`/proc/self/fdinfo/<fd>` (Linux ≥ 3.15) matched against `/proc/self/mountinfo`, so bind mounts,
autofs and container namespaces resolve to the effective superblock options.

## 7. Durability {#durability}

An intent's content is `fsync`ed before it is linked or renamed; after a name changes, `fsync` the
directory; after a directory is created or renamed, `fsync` its parent; new ancestors in order. On
Linux NFS a directory `fsync` is a no-op (namespace operations are synchronous at the server). A
failed `fsync`/`close` after a mutation is an ambiguity. An `async` export or storage that
acknowledges before persisting is an operator precondition, documented next to the S3 ones.

## 8. Capability probe {#probe}

`checkPoolPreconditions` for `Mode::Posix` runs before the generic battery under a per-mount random
prefix and fails closed with `NOT_IMPLEMENTED` naming the failed fact: (1) mount-option gate; (2)
fact 1 including `ENOENT` on a missing source; (3) fact 2; (4) fact 3 including `ENOENT` on a
missing source and refusal onto a non-empty directory; (5) fact 4; (6) fact 6: create a file in a
directory, list it, unlink it, list again — the unlinked name must be absent from the second
listing; (7) `fsync` of a file and of a directory succeed; (8) layout check: a regular file where
`D(k)` is expected under a plane is a retired-emulated-layout pool ⇒ refuse, naming migration.

`checkSkipAccessCheckSupport` refuses `skip_access_check = true` for a writable `Mode::Posix` mount
(`Pool::open` skips `checkPoolPreconditions` on that path, `Pool/CasPool.cpp`).
`checkConditionalWriteSingleAttemptSupport` stays a no-op. A single-client probe cannot verify
cross-client atomicity of facts 1–5 or export durability; those are operator preconditions and the
subject of `§11`'s two-client test.

## 9. What this needs outside the backend {#outside-backend}

- **Blob payload reads.** `ContentAddressedMetadataStorage` hands the logical blob key to the
  object storage for ranged reads (`ContentAddressedMetadataStorage.cpp`, read-path `StoredObject`
  construction); `LocalObjectStorage` opens it as a file. In `Mode::Posix` the read path obtains a
  **backend-owned ranged reader** (`Backend::openPayloadRange`, new): it resolves the key to the
  current incarnation, opens it, and on `ENOENT`/`ESTALE` mid-read re-resolves and reopens at the
  same payload offset — correct because every incarnation of a blob key carries the same payload
  bytes after the pool-constant envelope. This is what makes reclaim of a superseded blob chain safe
  for a query that is reading it on another client; a local reader keeps its inode anyway. Cache
  identity is the incarnation path (immutable).
- **Dialect.** `Dialect::Emulated`, grammar "non-empty" (`Backend/CasEtag.cpp`); values persist as
  strings compared within one key's context (`Formats/CasRecordStreamFormat.cpp`).
- **Settings** through `ContentAddressedSettings`/`openPoolView`: `posix_list_memory_budget_bytes`
  (256 MiB), `posix_list_spool_ttl_sec` (600), `posix_publish_max_attempts` (16). `K` is not a
  setting (`§3.2`).
- **Mode selection.** `object_storage_type = local` selects `Mode::Posix`; `EmulatedSingleProcess`
  and its `emu_*` state are deleted.
- **Physical sweep.** The namespace janitor today walks only `cas/ns/` with catalog membership
  (`Gc/CasNamespaceJanitor.cpp`) and has no epoch-death evidence. A backend-driven physical sweep
  is added to the GC leader's bounded janitor page: one prefix page per round across all planes,
  calling `§4.4` reclaim, with `isCreatorFenceTerminal` (`Pool/CasServerRoot.cpp`) plumbed in as the
  certificate source. The GC leader is the natural owner because it already reads every mount slot
  in phase 3. No GC *decision* changes.
- **Nothing in formats, GC's decisions, the ref lanes, the mount lease or the request engine
  changes.**

## 10. Cost {#cost}

| Item | Cost |
|---|---|
| Object at rest | `D(k)` + one chain + one file: 3 inodes |
| Conditional write | intent create + write + `fsync` + `link` + `unlink` + directory `fsync`: ~6 RPCs on NFS |
| `head`/`read` | `readdir` hint + one `LOOKUP` per level + `open` + `read`: ~5 RPCs on NFS |
| Hot mutable key | ≤ `K` incarnations per chain, two chains transiently; a superseded chain is reclaimed by its rotator when no intent is present |
| Blob republish | a new chain per publication (`K = 1`); the superseded body is reclaimable at once unless an intent from a live epoch is present in `D(k)` |
| Reclaim | `fresh` (2 RPCs) + listing + renames; deferred, never blocking |
| Deleted key | nothing permanent (`§4.5`) |
| `list` | one enumeration per walk, spooled; on NFS two extra RPCs per directory |
| Stalled writer | its intent defers reclaim of that one key until it completes, releases, or its epoch is certified dead; never a correctness cost |

## 11. Tests {#tests}

Unit (`CAS*` suites, gtest, standard gate filter):

- `CASPosixBackend`: the full contract on a temporary directory; then two independent backend
  instances on one directory: create-vs-create, overwrite-vs-overwrite, remove-vs-overwrite from one
  predecessor (never both `Removed` and success), publish-vs-remove with rebirth, chain rotation with
  two writers, stale token after rebirth (`RawConflict`).
- `CASPosixIntent` (the theorem's schedules): a writer declares intent, observes `g-7`, is paused
  (shim holds the `link`); fence `g-8`, reclaim `g-9`…, rotation, reclaim of the old chain **is
  deferred** while the intent's epoch is live; then the epoch is certified dead, the intent is
  killed, the old chain reclaimed, and the held `link` executes: `ENOENT`, reported as ambiguity,
  never success. Same with a `.new-*` creator across two rotations (round-3 finding 1) and with a
  remover's `.tomb-*` (round-3 finding 2). Same with the stall placed *inside* the syscall (the
  shim delays the RPC, not the caller). A live-epoch intent never has its target freed.
- `CASPosixLostReply`: dropped replies for `link`, `mkdir`, `rename`, `unlink`, `rmdir`; ownership by
  inode or `g`; landed-then-superseded creation ⇒ ambiguity; no `std::system_error` escapes.
- `CASPosixChain`: stale `readdir` hints (missing chains, missing incarnations, hinted chain gone)
  with fresh lookups: `chain` returns the true current or restarts from a fresh listing; never an
  old value.
- `CASPosixList`: hash-order `readdir`, page limit 1, exactly-once in order; spool reuse; per-level
  freshness (a key created by the second instance under a directory the first instance had cached
  is listed).
- `CASPosixReclaim`: reclaim of superseded/tombstoned chains and of `D(k)`; `rmdir D(k)` vs a
  concurrent creator; write-once keys; the janitor page never touches a live epoch's intent;
  deferred reclaim retried.
- `CASPosixDurability`: crash/power-loss injection at each step of create, overwrite, remove,
  rotate, publish, reclaim.
- `CASPosixReadPath`: table reads (full, ranged, cached) after publish, republish, rebirth, and
  with the incarnation reclaimed mid-read (reopen at offset).
- `CASPosixProbe`: each fact fails closed on a violating shim; missing mount option; emulated
  layout; `skip_access_check` refused.
- `CASPosixBackendDeathTest`: every `LOGICAL_ERROR` site as `EXPECT_DEATH` with `std::_Exit`.

Integration: `test_cas_posix_shared` — an NFS server container, two `clickhouse-server` containers as
separate NFS clients with the mandatory options, a `ReplicatedMergeTree` on both, inserts on both,
`SYSTEM CAS GC RUN`, `ca-fsck` `dangling=0`; a `SIGSTOP`ped server resumed after the other fenced it
and rotated its keys (the resumed server's writes must all fail as ambiguity, never succeed); mount
without `lookupcache=positive` refused. Soak: `utils/ca-soak/docker-compose-nfs.yml` with the
existing scenario suite; report transient residue after the run. The stateless `cas storage` lane
becomes the single-process durability lane.

## 12. Migration and out of scope {#out-of-scope}

Pools written by `EmulatedSingleProcess` are refused by probe gate 8; no converter (no production
data). Out of scope: Keeper coordination; SMB; the single-write blob optimization (conflicts with
`BlobSource::open`); trustworthy `list` values; any change to persisted formats or the request
engine.
