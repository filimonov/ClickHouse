---
description: 'Design for a coordinator-free CAS backend over local and shared POSIX filesystems (local disks, NFSv3/v4, CephFS, Lustre, GPFS, SMB with hard links): the full Cas::Backend contract from mkdir, link, rename and a writer-minted mtime incarnation token, durable through fsync, with no in-process state, replacing EmulatedSingleProcess.'
sidebar_label: 'CAS POSIX shared backend'
sidebar_position: 50
slug: /superpowers/specs/cas-posix-shared-backend-design
title: 'CAS backend over local and shared POSIX filesystems'
doc_type: 'design'
---

# CAS backend over local and shared POSIX filesystems {#cas-posix-shared-backend}

Revision 1, 2026-09-08. Brainstormed from the research in `tmp/cas-backends-research-2026-09-08.md`.
Decisions taken with the user: coordinator-free (no Keeper), one mode for local and shared
filesystems, lowest-common-denominator primitives only.

## 1. Problem {#problem}

`object_storage_type = local` routes a CAS disk to `ObjectStorageBackend::Mode::EmulatedSingleProcess`
(`ContentAddressedMetadataStorage.cpp`, `openPoolView`). That mode implements the conditional
contract of `Cas::Backend` with one process-wide `std::mutex` and an in-memory token map minted
from the file's `mtime`. It is correct for exactly one process and silently wrong for two:
two servers on one directory keep independent token state, both pass the capability probe alone,
and the mount only logs at `Information`. It is also not crash-durable even for one process:
nothing in `LocalObjectStorage` or in the CAS tree calls `fsync`, mutable control objects
(`_ckpt`, `mount`, `ref_catalog`) are rewritten in place with `O_TRUNC`, and a blob body is
written to disk twice (the hashing spill in `cas_scratch_path`, then the stream into a sibling
temporary file and `rename`).

The goal is one backend mode, `Mode::Posix`, that satisfies the whole `Cas::Backend` contract on
any POSIX filesystem in the supported list, with no in-process state, so that N servers sharing a
directory are exactly as safe as N servers sharing an S3 bucket, and a single server on a local
disk is crash-durable. The engine, GC, mount lease, ref lanes, `_ckpt` and every persisted format
are untouched: this is a backend, not a protocol.

## 2. Supported filesystems and the primitives trusted {#filesystems}

| Filesystem | Status | Notes |
|---|---|---|
| Local ext4 / xfs / btrfs / zfs | supported | one process; `Mode::Posix` replaces `EmulatedSingleProcess` |
| NFSv4, 4.1, 4.2 | supported | close-to-open consistency; negative dentry cache exists |
| NFSv3 | supported | NLM locks are not used; only `link`/`mkdir`/`rename` atomicity |
| CephFS, Lustre, GPFS | supported | coherent caches; stricter than NFS |
| SMB / CIFS | supported only when `link` works | Samba with POSIX extensions or SMB3 POSIX; otherwise the probe refuses |

Only four filesystem facts are relied on, and every one of them is verified by the probe at mount:

1. `mkdir` fails with `EEXIST` atomically at the server when the directory exists.
2. `link(temp, key)` fails with `EEXIST` atomically at the server when `key` exists.
3. `rename(temp, key)` within one directory atomically replaces `key`.
4. `utimensat` stores a nanosecond `mtime` and `fstat` returns it exactly.

Deliberately not relied on: `O_EXCL` (NFSv2 legacy, some SMB), `flock`/`fcntl` (NLM on NFSv3 is
unreliable, `nolock` mounts are common), extended attributes (NFSv4.2 only), `renameat2` with
`RENAME_NOREPLACE` (not forwarded over NFS), directory-listing consistency (a listing is a hint,
as everywhere in CAS).

## 3. The incarnation token {#token}

The token of an incarnation is the file's `st_mtim` in nanoseconds, **minted by the writer**, not
by the clock: before the final `link` or `rename`, the writer calls `utimensat` on the temporary
file with `mtime` set to a random 62-bit value (top bits clear so the value stays a valid positive
timestamp on every filesystem's on-disk format). Properties:

- `TOKEN ⟹ CONTENT` holds by uniqueness of the nonce, not by clock resolution; two writes in one
  clock quantum get different tokens.
- The token survives a process restart with no in-memory state; the `emu_token_state` map and its
  expiry sweep are deleted.
- `head` is `open` + `fstat` + `close`. `open` is what triggers close-to-open revalidation on NFS,
  so the attributes come from the server, not from the client's attribute cache. `stat` alone is
  never used to read a token.
- `read` returns the token from `fstat` on the same descriptor the bytes were read from, so the
  bytes and the token always belong to one incarnation even if the key is replaced mid-read.
- Blob bodies get the same treatment in `publish`, so GC's exact-token delete of a blob is the
  same code path as for a control object.
- `dialect` reports a new `Dialect::Posix`; its grammar is "decimal, non-empty" and its value is
  never shown to another backend (the `backend_id` binding already refuses that).

`supportsListTokens` returns `false`. `readdir` + `stat` on NFS may return cached attributes, and a
stale listed token equal to a persisted one would let GC skip a body read of an object that did
change. The contract already names the consequence: GC reads every root-shard body. That is the
correct fail-closed side.

An external `touch` on a pool file changes its token. That is conservative in every path: a
conditional write against the old token gets `RawConflict` and re-reads, an exact delete gets
`Mismatch` and the object is retained, the read cache misses.

## 4. Operations {#operations}

All temporary files live in the destination's own directory, named `<key>.tmp-<uuid>`, so every
`link` and `rename` is intra-directory. `D` below is the destination directory.

| Contract method | Implementation |
|---|---|
| `write(key, bytes, expected = absent)` | write `<key>.tmp-<uuid>`; `utimensat(nonce)`; `fdatasync`; `link(tmp, key)`: `EEXIST` ⇒ `unlink(tmp)`, return `RawConflict`; else `unlink(tmp)`; `fsync(D)`; return nonce. No lock: `EEXIST` is decided at the server and is immune to the client's negative dentry cache |
| `write(key, bytes, expected = t)` | acquire lock (`§5`); `open(key)` + `fstat`: `ENOENT` or `mtime != t` ⇒ unlock, `RawConflict`; write tmp; `utimensat(nonce)`; `fdatasync`; hold-deadline check; `rename(tmp, key)`; `fsync(D)`; unlock; return nonce |
| `remove(key, t)` | acquire lock; `open(key)` + `fstat`: `ENOENT` ⇒ `Gone`; `mtime != t` ⇒ `Mismatch`; hold-deadline check; `unlink(key)`; `fsync(D)`; unlock; `Removed`. `DeleteMarker` is never returned |
| `publish(request)` | as `emuPublishBlobAtomically` today (stream into tmp, bounded copy, size check) plus `utimensat(nonce)` and `fdatasync` before `rename`, `fsync(D)` after. No lock: publication is unconditional by protocol |
| `read(key)` | `open`; `fstat` (size, token); read whole; `close`. `ENOENT` ⇒ nullopt |
| `stream(key)` | `open`; return a `ReadBufferFromFileDescriptor` that owns the descriptor. `ENOENT` ⇒ null |
| `head(key)` | `open` + `fstat` + `close`; a directory ⇒ nullopt (pool subdirectories are not objects); `ENOENT` ⇒ nullopt |
| `list(prefix, cursor, limit)` | lazy depth-first `readdir` walk from the longest existing directory prefix, emitting keys in lexicographic order, resuming strictly after `cursor`; `.tmp-*`, `.lock*` and `.publish-*` names are skipped. Nothing is materialized beyond one page. Values are absent (`supportsListTokens = false`) |
| `removeManyWriteOnce(keys)` | `unlink` each; `ENOENT` is success; `fsync` each touched directory once |
| `probeSentinelRaw(key)` | `open`: `ENOENT` with the pool root present ⇒ `KeyAbsent`; pool root absent ⇒ `ContainerAbsent`; `EACCES`/`EPERM` ⇒ `AccessDenied`; any other error ⇒ `Indeterminate` |

Durability is the same rule everywhere: `fdatasync` the file before it becomes visible,
`fsync` the directory after the name changes. On NFS `fdatasync` is a `COMMIT`, and `link`,
`rename`, `unlink` and `mkdir` are synchronous at the server.

Every empty-token or unparsable-`fstat` case throws `CAS_WRITE_UNATTRIBUTED` (after a write) or
`CORRUPTED_DATA` (on observation), exactly as the emulated mode does today; a token is never
invented.

## 5. The per-key lock {#lock}

Conditional overwrite and exact delete need mutual exclusion between processes for the window
"observe token, then replace or unlink". The lock is a directory:

- **Acquire:** `mkdir(<key>.lock)`. `EEXIST` ⇒ contended. On success write `<key>.lock/owner`
  (tmp + `rename`) with `server_root_id`, `writer_epoch`, `pid`, a random nonce and the hostname.
  The owner file is diagnostics only; the lock is the directory.
- **Hold bound:** the holder records `CLOCK_BOOTTIME` at `mkdir` success and, immediately before
  its one mutating syscall (`rename` or `unlink`), checks `now < acquired + lock_hold_max_ms`
  (default 5000). Past the deadline it releases without mutating and reports the write as
  `Unresolved` to the request engine, which resolves by exact read as it does for any ambiguous
  store answer. `CLOCK_BOOTTIME` advances across a VM suspend, so a paused holder sees itself
  expired on resume.
- **Release:** `unlink(owner)`, `rmdir(<key>.lock)`.
- **Contention:** the contender sleeps with the request engine's flat jitter and retries; the
  request contract's deadline bounds the wait.
- **Breaking a dead holder's lock:** a contender that observes `<key>.lock` with the **same inode
  and `ctime`** across `lock_break_after_ms` (default 30000) on its own clock renames it to
  `<key>.lock.broken-<uuid>` (atomic; exactly one contender wins) and removes it recursively.
  Progress by a live holder changes nothing about the lock directory, so liveness of the holder is
  inferred from the holder's hold bound, not from activity: a holder past `lock_hold_max_ms` has
  promised never to mutate, and `lock_break_after_ms > lock_hold_max_ms` by a margin that covers
  clock-rate drift between two machines (ppm-scale) and scheduling lateness. This is the
  token-stability observation the mount lease already uses, applied to a 5-second lease.
- **Residual window:** a holder that passes its deadline check and is then descheduled between
  the check and the `rename` syscall for longer than the margin can still mutate after a break.
  This is the same window every lease system in CAS has (the mount lease's "OPEN: physical
  response after expiry") and is documented, not closed. The consequence is bounded to one
  conditional operation on one key: the engine's per-write fence recheck and exact-token delete
  protect the protocol above it.

`removeManyWriteOnce`, `publish` and create-if-absent take no lock.

## 6. Reads, caches and NFS mount options {#nfs}

- **Attribute cache:** every token read goes through `open` (close-to-open revalidation). `stat`
  is used only for "is this a directory" and in `list`.
- **Negative dentry cache:** a client that looked a name up and found it absent may report
  `ENOENT` for up to `acdirmax` after another client created it. Create-if-absent is immune
  (`link` decides at the server). Every other consumer of absence in CAS is a hint or a
  delay-class decision: a blob `HEAD` miss costs a duplicate publication, a GC tail probe folds
  one transaction later, a recovery walk of a **dead** epoch cannot race a creator. The one
  authoritative use of absence, the successor's epoch seal, is a create-if-absent on the dead
  epoch's slot and therefore server-decided: if this client's stale negative entry hid a log the
  predecessor wrote just before dying, the seal's `link` fails `EEXIST`, the engine re-reads the
  slot, finds the log and keeps walking. The documented requirement is still
  `lookupcache=positive`, and the probe warns when `/proc/self/mountinfo` shows an NFS mount
  without it.
- **Data cache:** a replaced key is a new inode; `open` on the name sees the new one. A reader
  holding the old descriptor keeps reading the old incarnation, which is what `stream` on a
  write-once object wants and what `read` on a mutable object tolerates (its token names what
  it read).
- **Directory cache:** `list` may be stale by `acdirmax`. Listings are hints.
- **Mount options documented as required for NFS:** `hard`, `lookupcache=positive`; recommended
  `actimeo` no larger than the default. No `nolock` dependency: no POSIX locks are used.

## 7. Single-write blob publication on a local filesystem {#single-write}

When `cas_scratch_path` and the pool root are on the same filesystem (`st_dev` equal, checked at
mount), the hashing spill file is created with a 256-byte placeholder envelope at offset 0. When
hashing completes, the writer `pwrite`s the real envelope over the placeholder, sets the nonce
`mtime`, `fdatasync`s, and `rename`s the spill file into `blobs/<algo>/<hh>/<hash>` (after the
mandatory `HEAD`, per the blob protocol). One write instead of two; no copy. When the devices
differ, the current stream-into-temp path is used unchanged. The `BlobSource::open` contract is
preserved: a second publication of the same source (condemned body, ambiguous first attempt)
re-streams from the retained source bytes, never from the moved file.

## 8. Capability probe {#probe}

`checkPoolPreconditions` for `Mode::Posix` runs before the generic seven-step battery, under the
per-mount random prefix, and fails closed with `NOT_IMPLEMENTED` naming the failed fact:

1. `utimensat` nonce round trip: write a file, set a random nanosecond `mtime`, `open` + `fstat`,
   exact equality. Refuses coarse-`mtime` filesystems.
2. `link` semantics: `link(a, b)` succeeds; `link(c, b)` fails `EEXIST`; `b` still has `a`'s nonce.
3. `mkdir` semantics: `mkdir(l)` succeeds; second `mkdir(l)` fails `EEXIST`.
4. `rename` replace: `rename(x, b)` replaces; `b` now has `x`'s nonce; `a`'s nonce is gone.
5. `fdatasync` and directory `fsync` return success.
6. Warnings only: NFS mount without `lookupcache=positive`; scratch on a different device
   (single-write disabled).

The generic battery then runs unchanged and exercises the same code paths a second process would,
because there is no per-process state left to diverge. What a single-client probe cannot verify
is the filesystem's cross-client atomicity of `link`/`mkdir`; that is the operator precondition,
documented in `docs/en/antalya/cas/bucket-requirements.md` next to the S3 ones, and verified by
the two-server integration test in `§10`.

`checkSkipAccessCheckSupport` and `checkConditionalWriteSingleAttemptSupport` stay no-ops for
this mode: it has no transparent retry layer.

## 9. Configuration {#configuration}

`object_storage_type = local` selects `Mode::Posix` (the only mode for local storage). New CAS
disk settings, unprefixed inside the disk block per the settings convention:

| Setting | Default | Meaning |
|---|---|---|
| `posix_lock_hold_max_ms` | 5000 | the holder's self-imposed bound on one conditional operation |
| `posix_lock_break_after_ms` | 30000 | how long a contender must observe an unchanged lock before breaking it; validated `>= 3 × posix_lock_hold_max_ms` |
| `posix_single_write_blob` | true | use the rename-in-place blob publication when scratch and pool share a device |

Every server sharing a pool must run identical lock timings, for the same reason the mount-lease
timings must match: a contender with a shorter break threshold can break a live holder's lock.

## 10. Tests {#tests}

Unit (`CAS*` suites, gtest, run under the standard gate filter):

- `CASPosixBackend`: the full `Cas::Backend` contract over a temporary directory, then the same
  battery driven by **two independent `ObjectStorageBackend` instances on one directory** —
  with no shared in-process state this is the two-process model — including the interleavings
  create-vs-create, overwrite-vs-overwrite on one token, delete-vs-overwrite, and publish-vs-delete.
- `CASPosixLock`: acquire/contend/release; hold deadline expiry via an injected clock (the
  existing test-hook pattern, hooks owning their clock state); break of a dead lock by inode and
  `ctime` stability; two breakers, one winner; a holder past its deadline never mutates.
- `CASPosixDurability`: crash injection between `link` and `unlink(tmp)`, between `rename` and
  `fsync(D)`, between `pwrite` of the envelope and `rename` in the single-write path: the
  destination is always either the old incarnation or the complete new one; temporaries are
  reclaimed by the next mount's staging sweep.
- `CASPosixProbe`: each precondition fails closed on a fake filesystem that violates it (coarse
  `mtime`, `link` unsupported, `rename` not replacing).
- `CASPosixBackendDeathTest`: every `LOGICAL_ERROR` site, as `EXPECT_DEATH` with `std::_Exit`.

Integration: `test_cas_posix_shared` — two `clickhouse-server` instances on one docker volume
exported by an NFS server container, a `ReplicatedMergeTree` table on both, inserts on both
replicas, `SYSTEM CAS GC RUN`, `ca-fsck` `dangling=0`, kill one server mid-insert and verify the
survivor fences and reclaims. Soak: `utils/ca-soak/docker-compose-nfs.yml` running the existing
scenario suite unchanged against the NFS volume.

The stateless `cas storage` lane keeps its config and becomes the single-process durability lane.

## 11. Out of scope {#out-of-scope}

Keeper-based coordination (a separate protocol track); SMB without hard links; filesystems with
coarse `mtime`; making `list` tokens trustworthy; any change to persisted formats or to the
request engine.
