---
description: 'Design for a per-disk local mount witness of the CAS disk: a lock and a small record of writer epochs in the server data directory let a restart on the same host and boot prove that the previous server process is dead, so the restart waits a settle window (21 s for an S3 disk with default settings) instead of the 131 s mount-lease observation threshold after SIGKILL, an OOM kill, an abort or a forced shutdown. No on-store format change, no change to shared server code, no clock compared between processes, the default lease stays 120 s.'
sidebar_label: 'CAS: mount lease witness'
sidebar_position: 114
slug: /superpowers/specs/cas-mount-lease-witness-design
title: 'CAS: restart after a kill without waiting out the own mount lease'
doc_type: 'reference'
---

# CAS: restart after a kill without waiting out the own mount lease — design {#cas-mount-lease-witness}

- Date: 2026-11-13. Status: draft, revised after review round 1 ([§17](#review-round-1)). Not approved.
- Tasks: CAS-480 (restart after a hard kill waits 131 s), CAS-355 (forced shutdown leaves the lease; this
  design takes no part of its option "release on the forced exit").
- Source tree: `a3967637843`, the head of pull request
  [#2831](https://github.com/Altinity/ClickHouse/pull/2831). Every `path:line` below was read at that commit.
  The implementation branches from it.
- Input: `tmp/prbundle/wave2/restart-memo.md` in the `master` worktree, called "the memo". Where the memo and
  the code disagree, the code wins: [§14](#memo-corrections).
- `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`. `T/` = `src/Disks/tests/`.
  `D/` = `docs/en/antalya/cas/`. `M/` = `docs/superpowers/models/` in the `master` worktree.
  `E/` = `tmp/prbundle/wave1/ci2/` in the `master` worktree.
- Kernel references are to Linux v6.14 unless a version is named.
- "Not verified" marks every statement that was not confirmed in the code. [§15](#not-verified) lists them.

## 1. Decision {#decision}

- **A writable CAS mount takes an exclusive `flock` on a per-disk witness file. When its mount claim carries
  proof, it writes its `writer_epoch` into that file before it sends the claim.**
- **A restart that takes the lock, on the same boot and the same witness file, and finds the epoch of the
  slot body listed in the file knows that the process which wrote the slot is dead.** It reclaims the slot
  after a settle window with the slot token unchanged, instead of the 131 s observation threshold.
- **Every other case waits exactly as today, and logs why.** Missing, unreadable or inconsistent local
  evidence never produces a shorter wait and never produces an error.
- **The mechanism compares no clock between processes.** The record holds no time. The settle window is
  measured on the restarting process's own clock, like today's wait.
- **Nothing on the store changes.** No new field in any stored object. The reclaim stays a write conditional
  on the slot token. A foreign observer (another server, the GC leader) waits as today.
- **No change to code shared with non-CAS ClickHouse.** All code is under `CA/`.
- **One new disk setting**, `cas_mount_lease_witness`, default `1`. With `0` the server behaves as today.
- **Claimed only for a data directory on ext4, xfs or tmpfs**, each after its recorded experiment. On any
  other file system, overlayfs included, the server waits in full and says so in one `Information` line.

Settle window `S` = `2 × envelope + cas_lease_safety_margin_ms + max(1, cas_mount_renew_period_ms / 2)`
([§3.5](#arithmetic)):

| Disk backend, default settings | Envelope | `S` |
|---|---|---|
| S3 (attempt timeout 5 s, connect cap 1 s) | 7000 ms | 21000 ms |
| Local object storage (no connect cap) | 5000 ms | 17000 ms |

Restart wait at default settings. "Wait" is the time `Pool::open` blocks before the reclaim; the server
needs about 1 s more to reach `Ready for connections` in the CI logs of [§2.1](#ci-evidence). The "after"
column is computed, not measured.

| Case | Before | After |
|---|---|---|
| Graceful stop (farewell written) | 0 s | 0 s |
| `SIGKILL`, OOM kill, abort, second `SIGTERM`, `SYSTEM KILL`, forced exit; restart on the same host, same boot, same witness file, listed file system | 135 s | `S`: 21 s (S3), 17 s (local) |
| The same, however long the server was down inside that boot | 135 s | `S` |
| The first unclean stop after: an upgrade from a build without the witness, a GC fence of the slot, a reclaim under `cas_unsafe_remount_no_delay`, a decommission and rejoin | 135 s | 135 s once; the next one waits `S` |
| Same host and boot, the witness file is another inode (directory copied, volume attached under another device number) | 135 s | 135 s |
| Host rebooted; start on another host; data directory copied or restored to another host | 135 s | 135 s |
| Data directory on a file system that is not listed (overlayfs, NFS, FUSE, …) | 135 s | 135 s |
| Previous process ran a build without the witness, or with the setting off | 135 s | 135 s |
| Another live process holds the witness lock | 135 s, then refusal if that process renews | the same |
| Slot fenced by a GC leader before the restart | 0 s | 0 s |

- 135 s is the 131 s threshold rounded up to the 5 s poll of the wait loop ([§2.2](#code-facts), F6).
- A server with N stale CAS disks from the configuration pays N waits, one after another, as today.

The three CI cases of [§2.1](#ci-evidence), each with its real backend:

| Lane | Disk backend | `S` | Data directory | Before | After |
|---|---|---|---|---|---|
| Regression hard restart | S3 endpoint (`E/hard-restart.md` section 4) | 21 s | bind mount of the runner's checkout; ext4 likely, not measured | 135 s against a 60 s budget | 21 s, if the measurement of A10 finds a listed file system |
| `test_backup_restore_on_cluster` | S3 (`tests/integration/test_backup_restore_on_cluster/configs/cas_storage.xml:6-8`) | 21 s | bind mount of a host directory (`tests/integration/helpers/cluster.py:5067`); the runner's root is `/dev/sda1` with Docker on `extfs` (`E/job_131775013725.log:1086-1087`, `:1657`) | 135 s against a 60 s budget | 21 s, if A10 finds a listed file system |
| Stress `arm_release` | local object storage (`tests/queries/0_stateless/05387_cas_publish_counts_replicated.sh:12-13`) | 17 s | the container layer: no volume covers `/var/lib/clickhouse` (`ci/jobs/stress_job.py:205-210`); Docker's driver is `overlay2` | 135 s against a 110 s budget | **unchanged: 135 s.** The lane is not fixed by this design as it runs today ([§13](#open-questions), question 6) |

What does not change:

- The default `cas_mount_lease_ttl_ms` (120000) and how long a holder may write.
- The threshold of a foreign observer: `L + L/20 + step`.
- `cas_unsafe_remount_no_delay`, the farewell, the GC fence and every other existing reclaim path.
- The GC leader lease in `gc/state`. A restarted leader takes leadership back by the existing protocol.

### 1.1 Provisional decisions {#provisional}

Taken so that work can proceed. **Provisional, owner may overrule.**

| # | Decision | Status in this design |
|---|---|---|
| P1 | After a proven local death the server still waits with the slot token unchanged; the wait is not 0 | Kept. The window is the formula `S` above: **one write reserve plus the poll step**, 21 s for an S3 disk with defaults, not the memo's 16 s ([§3.5](#arithmetic)) |
| P2 | No self-fence on the forced-exit path of `programs/server/Server.cpp` | Kept. Not in this story. It is the one change that would fix the stress lane on overlayfs |
| P3 | The witness is on by default; a per-disk setting turns it off | Kept. The setting is `cas_mount_lease_witness` |
| P4 | A live memory clone of a running holder (VM snapshot with memory, process checkpoint) is a documented residual, not defended | Kept ([§4](#threat-model), T3). The same residual covers two live processes that share one `(uuid, writer_epoch)` |
| P5 | The in-process self-remount after a lost lease is out of scope | Kept. The self-remount still records its claim, so a later restart can use it |

### 1.2 Failure asymmetry {#asymmetry}

- A wrong short wait puts two writers under one `server_root_id`. Recovery from that is manual.
- A wrong full wait costs about 114 s of start-up.
- So each check that fails selects today's path. Two statements are dangerous when false: "this process
  holds the lock on the witness inode that the record names", and "while a listed epoch holds the slot, no
  other process can write". [§4.1](#file-systems) limits where the first is trusted. The eligibility rule
  of [§3.3](#record-writes) establishes the second.

## 2. Problem {#problem}

### 2.1 CI evidence {#ci-evidence}

Run 43379266824 on `a3967637843`. Evidence files: `E/hard-restart.md` and `E/triage.md`.

- **Regression hard restart (8 jobs, plus the `add_config` restart of 3 more).** `atomic_insert` and
  `lightweight_delete` send `SIGKILL` and start the server at once. The server logs
  `waiting ~131000 ms (token-stability observation)` and does not listen. The framework waits 60 s, starts a
  second server, and that one dies on `Cannot lock file /var/lib/clickhouse/status`. Timeline of job
  131784879929: kill 08:48:15.669, start 08:48:16.738, give-up 08:49:16.916 (`hard-restart.md` sections 1
  and 2). One waiting server did come up: 135.6 s after its start (`triage.md` 2.2).
- **`test_backup_restore_on_cluster::test_shutdown_waits_for_backup` (3 jobs).** `SIGTERM` at 10:58:07.3
  with one connection open. The helper waits 60 s and runs `kill -9`. The new server starts at 10:59:33.97,
  logs the 131000 ms wait, and `start_clickhouse(start_wait_sec=60)` gives up (`triage.md` 3.2).
- **Stress `arm_release` (1 job).** `SIGTERM` at 11:51:30 with 24 connections. At 11:52:30.38 the server
  logs `Will shutdown forcefully` and exits without a farewell. The restart mounts two CAS disks defined in
  table metadata, both wait 131000 ms from 11:52:34, and the lane gives up at 11:54:22 (`triage.md` 3.3).

The first two were green at the 30 s lease, where the threshold is 36.5 s. The stress lane has no comparable
run at 30 s: its earlier runs did not take the forced exit (`triage.md` 3.3).

### 2.2 Code facts {#code-facts}

| # | Fact | Where |
|---|---|---|
| F1 | A writable open runs owner claim, epoch allocation, then the mount claim, on the caller's thread | `CA/Pool/CasPool.cpp:596`, `:627`, `:689`, `:785-788` |
| F2 | A slot with the claimer's uuid, an older epoch, not fenced, not farewelled is `LiveDoubleStart`: no write | `CA/Pool/CasServerRoot.cpp:1085-1087`, `:1113-1119` |
| F3 | The wait loop reclaims only after the slot token stayed unchanged for the threshold on the claimer's own clock | `CasServerRoot.cpp:1220-1300`; the test at `:1243` |
| F4 | Threshold = `L + L/20 + poll`, `L = max(own lease, lease the body declares)`, `poll = max(1, period / 2)`. Defaults: 131000 ms | `CasServerRoot.cpp:1202-1218`, `CasPool.cpp:695-702` |
| F5 | The clock is `CLOCK_BOOTTIME` of the claimer. The observation starts at a sample taken after the first read | `CasPool.cpp:787`, `CA/Pool/CasMountRuntime.cpp:78-88`, `CasServerRoot.cpp:1286` |
| F6 | The loop sleeps a whole `poll` between looks, so the reclaim happens at the first multiple of `poll` at or above the threshold: 135 s | `CasServerRoot.cpp:1298`; 135.6 s and 41.3 s in the CI logs |
| F7 | The stamped `expires_at_ms` never enters the decision. A lease that ended hours ago costs the full threshold | `CasServerRoot.cpp:1065-1084`; test `CASMountAwaitExpiry.PastExpiryStillPaysTheFullObservationThreshold`, `T/gtest_cas_mount.cpp:1326` |
| F8 | A changed token restarts the observation; more than 3 restarts refuse the start | `CasServerRoot.cpp:1279-1284`, `CA/Pool/CasServerRoot.h:386`, `CasPool.cpp:844` |
| F9 | `gc_fenced` or a farewell in the slot is reclaimed at once. So is any body under `cas_unsafe_remount_no_delay` | `CasServerRoot.cpp:1087-1100`, `CasPool.cpp:763-777` |
| F10 | After the reclaim nothing else waits. Recovery closes the old epoch with a seal | `CasPool.cpp:880-920` |
| F11 | A `writer_epoch` belongs to one incarnation. It is allocated by a conditional bump of the `epoch` object with a random nonce. Epoch 1 is handed out only when the `epoch` object is absent, the data subtree is empty and the mount object is authoritatively absent | `CasServerRoot.cpp:846-890`; `D/architecture/mounts-and-leases.md:233-240` |
| F12 | A claim never installs an epoch at or below the one in the slot | `CasServerRoot.cpp:1037-1046` |
| F13 | Every slot write of a holder carries its own `(server_uuid, writer_epoch)`: claim, adoption, renewal, farewell. A GC fence copies the body and sets `gc_fenced`. A decommission admin writes under an epoch it allocated itself | `CasServerRoot.cpp:905-917`, `:1689-1707`, `:2174-2184`, `:1403`; `CasPool.cpp:1074` |
| F14 | Nothing is persisted on local disk at mount or at renewal. The only local files of a CAS disk are write-buffer spill files in the scratch directory | search of `CA/` for local file writes; `CA/ContentAddressedTransaction.cpp:1019-1023`, `src/Disks/DiskObjectStorage/MetadataStorages/MetadataStorageFactory.cpp:232-239` |
| F15 | The server takes `flock(LOCK_EX \| LOCK_NB)` on `<path>/status` before disks start, and loads `<path>/uuid` | `programs/server/Server.cpp:2033`, `:2035`; `src/Common/StatusFile.cpp:65`, `:72-76` |
| F16 | `clickhouse disks` loads the same `uuid` file, takes no `status` lock, and creates its disks through the ordinary factory, so a disk without `<readonly>` is mounted writable | `programs/disks/DisksApp.cpp:602-610`, `programs/disks/DisksClient.cpp:192`, `CA/ContentAddressedMetadataStorage.cpp:864` |
| F17 | A read-only disk never runs the mount protocol | `CasPool.cpp:595-596`, `ContentAddressedMetadataStorage.cpp:864` |
| F18 | The server starts child processes by `vfork` and `execv` and closes no other descriptor in the child. A descriptor without `FD_CLOEXEC` reaches every executable dictionary, executable function and bridge | `src/Common/ShellCommand.cpp:244-297` |
| F19 | The watchdog process is forked during daemon initialization, before any disk starts | `src/Daemon/BaseDaemon.cpp:407-408`, `:627` |
| F20 | The name of a disk defined inline in a query is a hash of its configuration. A failed disk creation is not added to the disk selector | `src/Disks/DiskFromAST.cpp:88-100`, `src/Interpreters/Context.cpp:7606-7620` |
| F21 | The attempt envelope has no connect part when the object storage has no S3 client | `ContentAddressedMetadataStorage.cpp:705-720`, `CA/Backend/CasRequestBudget.cpp:21-30` |

Shutdown paths and the lease:

| Path | Lease released | Where |
|---|---|---|
| `SIGTERM`, everything finishes in time | Yes, if the ref lanes drained, the renewer is `Active` and writes are not paused | `Server.cpp:1823-1847`, `src/Interpreters/Context.cpp:1169-1173`, `src/Disks/DiskObjectStorage/DiskObjectStorage.cpp:540-552`, `ContentAddressedMetadataStorage.cpp:954-970`, `CasPool.cpp:1078-1139`, `CasMountRuntime.cpp:1230-1261` |
| `SIGTERM`, connections, refresh tasks or background queries remain after `shutdown_wait_unfinished` | **No.** `safeExit` runs before `global_context->shutdown` | `Server.cpp:4006-4018`; `base/base/safeExit.cpp:18`, `:37`; `src/Core/ServerSettings.cpp:126` |
| Forced exit after the storages were shut down | Yes | `Server.cpp:1847`, `:1887-1894` |
| `SYSTEM SHUTDOWN` | As the `SIGTERM` rows | `src/Interpreters/InterpreterSystemQuery.cpp:422` |
| Second `SIGTERM` | No | `src/Common/SignalHandlers.cpp:480-501` |
| `SYSTEM KILL` | No | `InterpreterSystemQuery.cpp:432` |
| `SIGKILL`, OOM kill, fatal signal, abort | No | no teardown runs |

Start-up order: the `status` lock (`Server.cpp:2033`), the uuid (`:2035`), disks of the configuration
(`:3117`, `Context.cpp:7715-7724`, `src/Disks/DiskObjectStorage/RegisterDiskObjectStorage.cpp:102`,
`DiskObjectStorage.cpp:554-558`, `ContentAddressedMetadataStorage.cpp:846-883`), table metadata
(`Server.cpp:3711`), listeners (`:3806`, `:4057-4062`). So the wait holds the `status` lock and precedes
every query port.

## 3. Mechanism {#mechanism}

Five rules carry the safety argument. Tests and the model refer to them by number.

1. **A lock that vouches is never released.** A descriptor through which a record was written stays open
   until the process ends. A descriptor that never wrote a record is closed with its mount attempt.
2. **Only the lock holder records.** A process writes the record only through a descriptor on which it
   holds the lock, and only its own `writer_epoch`.
3. **Evidence is inherited.** A process uses, as proof of death, only what the file held when it took the
   lock. What it writes later is never evidence for itself.
4. **A kept line survives only under an equal header.** A process copies an epoch of a predecessor into its
   own record only if the inherited header equals its own in every field.
5. **Only an eligible claim records.** A claim lists its epoch only if the claim itself carries proof that
   no other process can still write ([§3.3](#record-writes)).

### 3.1 Witness file and lock {#witness-file}

- **Path.** `<server path>/disks/<disk name>/cas_mount_witness`, one regular file per CAS disk. `<server
  path>` is `Context::getPath`, `<disk name>` the name the metadata storage was created with
  (`ContentAddressedMetadataStorage.cpp:290`). It does not follow `cas_scratch_path`: the witness belongs
  with the `uuid` file, not with spill data.
- **Who opens it.** `ContentAddressedMetadataStorage::openPoolView`, once per mount attempt, for a mount
  that is writable, has a `Context` and has the setting on. The handle goes into a new `PoolConfig` field
  and is owned by the mount attempt.
- **Open.** `open(path, O_RDWR | O_CREAT | O_CLOEXEC, 0666)`, as `StatusFile.cpp:65` does. Missing
  directories are created.
- **`O_CLOEXEC` is required, not optional.** Child processes inherit every descriptor without it (F18).
  With it, a child holds the open file description only between `vfork` and `execv`. If the server dies in
  that window the lock stays held until the `execv`. That direction is safe: a held lock selects the full
  wait.
- **File system check.** `fstatfs` on the descriptor. A type outside the list of [§4.1](#file-systems) makes
  the witness unavailable: the descriptor is closed, nothing is locked, read or written.
- **Lock.** `flock(fd, LOCK_EX | LOCK_NB)`. Exclusive, non-blocking, one attempt.
- **Identity.** `fstat` on the locked descriptor gives `(st_dev, st_ino)`.
- **Read.** The whole file, once, right after the lock is taken. This is the inherited record.

**Ownership of the descriptor (rule 1).**

- The handle belongs to the mount attempt. When the attempt ends without a record write through this
  handle, the handle closes the descriptor and the lock is free. Why this is safe: a certificate of death
  names an epoch listed in the file. This process listed none, so no later reader can conclude anything
  about it from this lock.
- From the moment the handle calls `pwrite` for a record, whatever the result, the descriptor is never
  closed: not at pool teardown, not after a farewell, not after a failed start-up. The kernel closes it
  when the process ends. Why: a listed epoch may still have requests in flight in this process after its
  pool is gone.
- **No process-wide registry of paths.** A later mount of the same disk in the same process opens the file
  again. If an earlier handle of this process still holds the lock, the new handle gets `EWOULDBLOCK` and
  is unwitnessed. That costs one full wait after the next unclean stop and needs no shared state.
- **Bound.** One process may have at most 1024 handles that wrote a record. The count never decreases. At
  the bound, a new mount opens no witness file and is unwitnessed, with one `Warning`. Failed disk
  creations with changing names (F20) therefore cannot exhaust descriptors: a creation that fails before
  its claim closes its descriptor, and one that fails after its claim counts toward the bound.

**The lock cannot be taken (`EWOULDBLOCK`).** Another live process holds it, or an earlier mount of this
process: a second writable mount of the same directory, a stopped server, a hard-linked copy.

- **Decision: full wait, not an error.** The mount continues exactly as today, with a `Warning`.
- The mount is **unwitnessed**: it reads no record and writes none (rule 2).
- Why not an error: today this case already ends correctly through the store. A renewing holder restarts
  the observation and the start is refused (F8). A holder whose renewals stopped loses the slot after the
  full threshold, which is the lease contract. An error would refuse mounts that are safe today.
- Why the unwitnessed mount must not record: its epoch would later be read by a process that took the lock
  after the *other* holder died, and would certify the death of a process that never held the lock.

| Mount kind | Witness |
|---|---|
| Server, writable CAS disk | lock + record |
| Read-only CAS disk (`<readonly>`) | none: no file is opened |
| `clickhouse disks`, writable mount with the server's `path` (F16) | the same file as the server. With a live server the lock is held: unwitnessed, full wait. With a dead server the tool takes the lock and may use the record |
| `clickhouse local` | a file under the `path` it runs with; a fresh temporary `path` never inherits a record |
| `SYSTEM CAS DROP POOL MEMBER` (`Pool::openForDecommission`, `CasPool.cpp:983-1076`) | none: the admin pool clears the witness field. It writes another member's slot and never waits |
| A `Pool` opened without the field (unit tests) | none |

Whether `clickhouse local` mounts a CAS disk writable: not verified.

### 3.2 Local record {#local-record}

The record is the content of the witness file. It is local state, not a store format: it has no `FormatId`
and no compatibility promise beyond the rules here.

- **Size.** Exactly 4096 bytes: text lines, then spaces, then one line feed.
- **Encoding.**

```text
cas_mount_witness 1
boot_id <36 characters of /proc/sys/kernel/random/boot_id>
witness_dev <decimal st_dev>
witness_ino <decimal st_ino>
server_root_id <the id, to the end of the line>
server_uuid <32 lowercase hex digits>
epoch <writer_epoch> <write_reserve_ms> <pid>
epoch ...
checksum <16 lowercase hex digits>
```

| Field | Use |
|---|---|
| version (`1`) | An unknown version makes the record unreadable |
| `boot_id` | The history of a lock exists only inside one boot |
| `witness_dev`, `witness_ino` | Identity of the file the writer had locked. A copy has the content but another inode ([§4.1](#file-systems)) |
| `server_root_id`, `server_uuid` | The slot and the identity the epochs belong to |
| `epoch` | One `writer_epoch` of this lineage whose claim was eligible, with the writer's write reserve (for the settle window) and its pid (log only). At most 2 lines: the epoch of the body the last claim replaced, if rule 4 allows it, and the epoch of that claim |
| `checksum` | `CityHash64` of every byte before the `checksum` line |

- **The record holds no time and no clock value.**
- **Why an epoch is enough.** One incarnation owns one epoch (F11), every body of a holder carries its
  epoch (F13), and the slot never goes back to a lower epoch (F12). So "the slot body has epoch `e`" and
  "`e` was listed by a process that held this lock" together name the process that wrote the body.
- **Two lines.** After the record is written and before the claim lands, the slot still holds the replaced
  body. The record keeps that epoch, so a process killed in between is still matched. A restart loop of any
  length keeps both lines.
- **Write.** One `pwrite` of the whole 4096-byte block at offset 0. No rename, no truncate.
- **No `fsync`.** A proof of death is used only inside the boot that wrote the record, and the page cache
  survives the death of the process. A kernel crash or power loss changes `boot_id`. Across a clean reboot
  the record is used only to recognise a farewell ([§3.3](#record-writes)); if it was lost, the claim is
  not eligible, which costs one full wait later.

| File content | Reading |
|---|---|
| Empty (a new file) | `absent` |
| Not exactly 4096 bytes, no checksum line, checksum mismatch, a line that does not parse | `unreadable`, with the detail |
| First line names another version | `unreadable: version N` |
| Valid | `present` |

- A torn write shows as a checksum mismatch: `unreadable`. The checksum detects damage with high
  probability; it does not make the write atomic.
- An unreadable or foreign record is overwritten by the first record this process writes.
- A `server_root_id` with a line feed, or a record that does not fit 4096 bytes, makes the witness
  unavailable for that mount.

### 3.3 When the record is written {#record-writes}

The record is written **only by `claimMount`, before the claim's write is handed to the request engine**:

| Site | Insert between |
|---|---|
| Fresh claim | `CasServerRoot.cpp:1004` and `:1006` |
| Reclaim | `:1089` and `:1091`; the prior state of `:1097-1100` is computed first |

Renewals, the adoption by the renewer, the farewell and the fences never touch the witness. The body keeps
its epoch through all of them.

**Eligibility (rule 5).** `claimMount` tells the witness what it is about to write: its epoch, the prior
state, the epoch and uuid of the body it replaces. The witness lists the epoch only for these claims:

| Claim | Eligible when | Why |
|---|---|---|
| Fresh: the slot is absent | the claim's `writer_epoch` is 1 | Epoch 1 exists only on a root that never had a holder (F11) |
| Prior state `UncleanObserved` | always | This claim itself watched the token for the full threshold |
| Prior state `Clean` (a farewell) | the farewell's `writer_epoch` is listed in the record this handle read, or was listed by this handle | The farewell belongs to a lineage that already satisfied the rule, and its holder gave the slot up |
| Prior state `UncleanLocalDeath` | always: [§3.4](#decision-table) grants it only for a listed epoch | The same, and the holder is dead |

Every other claim writes no epoch:

| Claim | Why not |
|---|---|
| Prior state `UncleanUnsafe` | No proof at all: a live process with this uuid may still write |
| Prior state `Fenced` | The wait behind the fence was another server's. A build without the declared-lease rule fences after its own, shorter lease (`D/configuration.md:125`, `:316-323`) |
| A terminal body of another `server_uuid` (rejoin under a new uuid) | The lineage of another directory |
| `Clean` over a farewell whose epoch is not listed | The farewell's holder is unknown to this directory: an older build, a decommission admin, a process without the lock, a holder that itself was not eligible |
| Fresh claim with an epoch above 1 | The slot vanished under a root that had holders |

- **A process without the lock never records (rule 2)**, whatever its claim.
- **Retraction.** A claim attempt can list its epoch and then lose the conditional write. The same process
  may later claim with the same epoch and another prior state. So before an ineligible claim, the witness
  rewrites the record without this handle's epoch if the file lists it. If that rewrite fails, the claim
  attempt fails with the I/O error: a record must never list an epoch whose claim was not eligible.
- **A failed write of an eligible record does not stop the claim.** The claim proceeds as in today's build.
  The slot then holds an epoch the file does not list, and the next restart waits the full threshold. The
  failure is logged ([§6](#observability)).
- **Carry-over (rule 4).** The record of an eligible claim holds the epoch of the replaced body, then the
  claim's epoch. The replaced epoch is kept only if this handle listed it itself, or the inherited record
  lists it under a header equal to this process's header in `boot_id`, `witness_dev`, `witness_ino`,
  `server_root_id` and `server_uuid`. Otherwise the record holds the claim's epoch alone.
- **A kill between the record and the claim's write** leaves the replaced body in the slot. The record
  still lists its epoch when rule 4 kept it.

**Wiring.** One witness handle per mount attempt reaches every `claimMount` call of that pool:

| Path | Calls | Result for the record |
|---|---|---|
| First open | `CasPool.cpp:771`, `:775`, `:785-788`, and through `claimMountAwaitingExpiry` `CasServerRoot.cpp:1244`, `:1254` | by the eligibility table |
| Open under `cas_unsafe_remount_no_delay` | the authorized call at `CasPool.cpp:775` | prior `UncleanUnsafe`: no epoch |
| Decommission | `CasPool.cpp:798`; the admin pool has no witness | nothing |
| The claim was fenced during the open (`FencedSelf`, `CasPool.cpp:801-814`), or the adoption met a fence (`:864-876`) | a new epoch, then a claim over the process's own fenced body | prior `Fenced`: no epoch; the earlier listing is left, it can never match again (F12) |
| A renewal with an unknown outcome | none | the body keeps its epoch |
| Self-remount after a lost lease | `CasPool.cpp:1574-1585` | a full wait over its own body gives `UncleanObserved`: the record lists the old epoch (own) and the new one. Over its own fenced body: no epoch |
| Every further self-remount | the same call | the same rule; the record always shows the last eligible claim |

Test P-8 walks this table end to end.

### 3.4 Decision at start-up {#decision-table}

The table applies only to a slot body with the claimer's `server_uuid`, an older `writer_epoch`, not
`gc_fenced` and not a farewell. Every other slot state keeps today's outcome:

| Slot | Outcome (unchanged) |
|---|---|
| Absent | fresh claim, no wait |
| `gc_fenced` or farewell, older epoch | reclaim at once |
| Another `server_uuid`, live | refused |
| Same uuid, newer epoch | `ABORTED` |
| Same uuid, same epoch | refused as `LiveDoubleStart` |

For the remaining case the evidence is evaluated **per observed token**. First matching row wins.

| # | Lock | Inherited record | Boot id | Witness inode | Slot epoch listed | Outcome | Reason logged |
|---|---|---|---|---|---|---|---|
| D0 | witness off or absent | – | – | – | – | full wait | `off` (once at mount) |
| D1 | file not usable | – | – | – | – | full wait | `unavailable: <detail>` |
| D2 | held by another descriptor | not read | – | – | – | full wait | `lock-held` |
| D3 | acquired | absent | – | – | – | full wait | `no-record` |
| D4 | acquired | unreadable | – | – | – | full wait | `record-unreadable: <detail>` |
| D5 | acquired | present, other `server_root_id` or `server_uuid` | – | – | – | full wait | `other-identity` |
| D6 | acquired | present | different | – | – | full wait | `other-boot` |
| D7 | acquired | present | same | recorded `(st_dev, st_ino)` differs from the locked file | – | full wait | `other-witness-file` |
| D8 | acquired | present | same | same | no | full wait | `not-recorded` |
| D9 | acquired | present | same | same | yes | **local death**: settle window | – |

- "Listed" means an `epoch` line of the inherited record has the body's `writer_epoch`.
- **No combination of local evidence is an error.** Start-up errors come only from the store, as today.
- **Token changes during a wait.** The loop re-reads the slot as today (F8). The new body is evaluated by
  the same table, and the change counts against the 3 restarts.
  - New body with the same listed epoch (the dead holder's last renewal landed late): D9 again, from the
    first sighting of the new token.
  - New body with another epoch: D8, the full threshold from the first sighting of the new token.
- **The reclaim stays conditional.** `claimMount` gets one more optional argument, the token authorized by
  local death. It reclaims only if that token equals the token it just read, and writes with `If-Match` on
  it (`CasServerRoot.cpp:1085-1096`). A slot that moved in between is re-read and decided again. A store
  that does not answer authorizes nothing.
- **New prior state.** `MountPriorState::UncleanLocalDeath` (`CasServerRoot.h:226-233`). The exhaustive
  switch at `CasPool.cpp:899-910` classifies it as unclean.

**Why the inode comparison stays.** The lock alone proves only that nobody holds *this* file. A copy of the
directory made on the same host in the same boot (`cp -a`, a block-level clone attached to the host) has
its own file with the original's content. Its lock is free while the original holder lives. The recorded
`(st_dev, st_ino)` is the only check that catches these two rows (T6, T7 of [§4](#threat-model)); the boot
id is equal and the lock is acquired. So the comparison cannot be dropped.

### 3.5 Arithmetic {#arithmetic}

Symbols: `e` = attempt envelope (`CasRequestBudget::attemptEnvelopeMs`, `CasRequestBudget.cpp:21-30`),
`m` = `lease_safety_margin_ms`, `p` = renewal period, `L` and the threshold as in F4.

| Quantity | Formula | S3 disk, defaults | Local object storage, defaults |
|---|---|---|---|
| Envelope `e` | `attempt_timeout + 2 × connect cap`; no connect part without an S3 client (F21) | 7000 ms | 5000 ms |
| Write reserve | `2 × e + m` (`CasRequestBudget.cpp:105`) | 16000 ms | 12000 ms |
| Poll step | `max(1, p / 2)` (`CasPool.cpp:701-702`) | 5000 ms | 5000 ms |
| **Settle window `S`** | `max(own write reserve, write reserve in the listed line) + own poll step`, saturating | **21000 ms** | **17000 ms** |
| Threshold | `L + L/20 + poll step` | 131000 ms | 131000 ms |

`S` is a formula. Every number of seconds in this document is its value for one named budget.

**Settle rule (D9).** With `t1` the claimer's clock sample taken after its first read of the token (F5):
reclaim when the token is still the same and `now − t1 >= S`. Both samples come from the claimer's own
clock.

**Sleep.** While a settle deadline is pending, the loop sleeps `min(poll, time to that deadline)`, at least
1 ms. Without one the loop sleeps `poll`, as today.

**Why `S` is the write reserve plus the poll step.**

- A live holder admits a mount-plane conditional write only while more than `2 × e + m` of its lease
  remains (`CasMountRuntime.cpp:156-176`, `CA/Backend/CasRequests.cpp:958-966`), and a one-envelope
  request only while more than `e + m` remains (`CasRequests.cpp:528-536`).
- Today a reclaim lands at least `poll / 1.05` after the lease ends, at the largest clock drift the rule
  allows (`M/CaMountObserveCore_RESULTS.md:22-23`).
- So today, at the reclaim, the youngest **admission** of the old holder is at least this old (S3 defaults):

| Request | Today, 5 % drift | Today, one clock | With `S` = 16 s | With `S` = 21 s |
|---|---|---|---|---|
| Conditional write (2 envelopes) | 16000 + 4762 = 20762 ms | 27000 ms | 16000 ms | 21000 ms |
| One-envelope request (read, token-exact delete, start of a blob publication) | 9000 + 4762 = 13762 ms | 20000 ms | 16000 ms | 21000 ms |

- These are bounds on admission. A request is handed to the transport after its admission
  (`CasRequests.cpp:959`, `:994`), so today the request on the wire can be younger than the table says.
- Requests of the open plane have no such bound today: the claim, the farewell and the background renewal
  are not gated by the lease (`CasServerRoot.h:623-629`). They are conditional on the slot token.
- After a proven death nothing is admitted and nothing is sent. Every request of the dead process, on any
  plane, left the process before the lock was free, and `t1` is later than the lock. So each is at least
  `S` old at the reclaim, measured at the wire.
- `S = write reserve + poll step` is at or above today's guaranteed minimum for every request kind and
  every valid setting: `poll >= poll / 1.05`.
- One write reserve alone (the memo's 16 s for S3) is below today's minimum for conditional writes by
  4762 ms. Nothing found in the code depends on those 4762 ms. The design still does not spend them: with
  the poll step added, the argument below needs no new claim about late requests.

**Reduction to today's protocol.** At a local-death reclaim:

1. The previous holder admits no write. Today it is "its lease has ended"; here it is "it is dead".
2. Every request it sent is at least as old as the youngest admission can be at an observed reclaim today.
3. No other process can write: the induction below.
4. The reclaim is conditional on the token, as today.
5. What follows the reclaim is the code path of `UncleanObserved` and `UncleanUnsafe`, unchanged (F10).

So every pair (state of the old holder, age of its requests in flight) reachable at a local-death reclaim
is reachable today at an observed reclaim. The only difference is that less wall time has passed since the
holder's last renewal. An epoch change seconds after the last renewal already happens on every graceful
restart. No component was found that assumes a lease has elapsed between two epochs; an exhaustive search
for such an assumption was not done.

**Induction: while a listed epoch holds the slot, no other process can write.**

Statement `G`: the slot body has epoch `e`, the witness file lists `e`, and no process other than the
incarnation of `e` passes its own lease check. "Passes its lease check" means what today's protocol means
after an observed reclaim.

- **Base.** An epoch is listed first by a fresh claim at epoch 1 or by an `UncleanObserved` claim.
  - Epoch 1: no other incarnation of this root ever existed (F11).
  - `UncleanObserved`: the claim watched the token for the full threshold. That is exactly the state from
    which today's protocol lets a successor write.
- **Step.** Assume `G` for epoch `e`.
  - Renewals of `e` keep `G`: a process gains the right to write only through its own confirmed slot
    write, and nobody else's write can land without replacing the body.
  - A farewell of `e`, then a `Clean` claim of `e'` over it: the incarnation of `e` gave the slot up after
    its drain, so nobody can write. The claim is eligible because `e` is listed. `G` holds for `e'`.
  - A proven death of `e`, then an `UncleanLocalDeath` claim of `e'`: the incarnation of `e` is dead, so
    nobody can write. `G` holds for `e'`.
- **What breaks the chain, and what the design does.** In each case the new epoch is not listed, so the
  next unclean stop pays the full wait, which re-establishes the base:

| Event | Why `G` may fail | Record |
|---|---|---|
| Reclaim under `cas_unsafe_remount_no_delay` | a live predecessor may still write | no epoch |
| The same history later: unsafe `B` over live `A`, `B` writes a farewell, `C` claims `Clean`, `C` dies | `A` may still write; `B`'s farewell epoch is not listed, so `C` is not eligible | no epoch for `C`; the restart after `C` waits in full |
| GC fence while the server was down, then a `Fenced` claim | the fence's wait belongs to another server and build | no epoch |
| Decommission, then a rejoin | the admin's farewell epoch is not listed | no epoch |
| Reclaim of a terminal body of another uuid | another lineage | no epoch |
| A process without the lock took the slot by the full wait | rule 2 | no epoch |
| The slot vanished and was minted again | the root had holders | no epoch |
| The record was lost or is unreadable | nothing to inherit | no epoch until the next base claim |

[§8](#formal-model) checks the induction by model, with one sabotage per row that can be expressed.

**Twin detection is a side effect, not a guarantee.** A live holder whose renewal lands within `S − p` of
its start (11 s for S3 defaults, above one envelope) changes the token inside the window. The loop then
restarts the window and, after 3 restarts, refuses the start. With `p` close to or above `S` the window can
miss a renewal. The design does not rely on it.

### 3.6 Late landing per write kind {#late-landing}

Question: how long after the death of the process can a request it already sent still land, and what
protects the successor.

**There is no bound the client can enforce.**

- While the process lives, the client abandons a control-plane attempt after one envelope. That bounds the
  client's wait, not the store.
- A blob publication is not under the envelope. Its creating request runs under the disk's
  `request_timeout_ms`, with ordinary multipart or a server-side copy
  (`CA/Backend/CasObjectStorageBackend.cpp:891-917`, `D/configuration.md:111`).
- After the process dies there is no client. Bytes already given to the kernel can still be delivered, a
  proxy can still forward them, and the store can still apply them. The user docs already say that nothing
  bounds when such requests stop (`D/operations/debugging.md:304-305`).
- A request that was only partly sent cannot be applied. A complete small request can, and so can a
  server-side copy the store has already accepted.

So no settle window covers late landing, and today's 131 s does not either. The protection is structural.
`S` only keeps the age of such requests no lower than today.

| Write kind | Request | Protection against a landing after the successor's claim | Verified |
|---|---|---|---|
| Mount slot renewal, farewell | `If-Match` on the holder's last token (`CasServerRoot.cpp:2023`, `:2210`) | The reclaim changed the token: the late write is refused. A landing before the reclaim makes the reclaim's own `If-Match` fail, and the loop decides again | code |
| Ref-log append | create-if-absent at a key that carries `(writer_epoch, sequence)` (`CA/Pool/CasRefLedger.cpp:3982`) | Recovery writes an epoch seal by create-if-absent in the dead epoch's stream; a late append loses to the occupied key (`CasRefLedger.cpp:1269`, `CasPool.cpp:890-895`, `D/configuration.md:114`) | comment and docs; the recovery walk was not traced |
| Catalog, checkpoint, blob marker, pool meta | create-if-absent or `If-Match` (`CA/Pool/CasRefCatalog.cpp:532`, `CA/Pool/CasRefCkpt.cpp:321`, `CA/Pool/CasBlobMeta.cpp:32-46`, `CA/Pool/CasPoolMeta.cpp:72`) | Lands only if nobody changed the key since the dead process read it. A successor that wrote the key first wins | request shape only |
| Namespace files | `HEAD`, then create-if-absent or `If-Match` with frozen bytes (`CA/Pool/CasPlainObjects.cpp:9-24`) | The same. A late file the successor never rewrites appears as a write that succeeded without an acknowledgement | request shape only |
| Part manifest body | create-if-absent at the manifest key (`CA/Pool/CasPartWriteTxn.cpp:696`, `CasRefLedger.cpp:259-265`) | Lands only if the key is absent. A dead epoch cannot commit a ref to it; an unreferenced manifest is left to the orphan-manifest sweep | request shape only; the sweep was not traced |
| Blob staging objects (`cas_staging_backend = s3`) | a key that carries the writer epoch (`CA/Formats/CasLayout.h:421`) | The key names the dead epoch; the mount-start sweep removes leftovers below the live epoch (`CasServerRoot.cpp:2257-2292`) | key shape and sweep; the request shape was not read |
| Blob publication | unconditional (`CA/Backend/CasBackend.h:193-195`, `CA/Pool/CasPartWriteTxn.cpp:508`) | The key is the content hash. Any server of the pool may publish the same key at any time; the mount lease never serialized blob keys. A dead epoch cannot commit a ref to the blob | docs and `M/CaBlobPublishCore_witness_late_landing.cfg`; the model is not tied to mount epochs |
| Token-exact delete by the holder | delete of one observed incarnation (`CasBackend.h:180-182`) | A replaced object has another token | request shape only |
| Batch delete of write-once keys | no precondition (`CasBackend.h:197-202`) | Sent only by GC and the namespace janitor (`CA/Gc/CasGc.cpp:112`, `:544`, `CA/Gc/CasNamespaceJanitor.cpp:33`), under the GC leader lease, which this design does not touch | call sites |

**Result.** No finite window helps, and every write kind is protected by its request shape. The table is
informative: the design does not rest on it. It rests on the reduction of [§3.5](#arithmetic): with
`S = write reserve + poll step` a late request after a local death is never younger than a late request can
be today. The unit test D-1 and the model invariant `RequestAgeAtLeast` hold that bound.

### 3.7 Boot id, and no clock between processes {#boot-id}

- **Boot id.** `/proc/sys/kernel/random/boot_id`: a random id the kernel draws once per boot. Locks do not
  survive a boot, so "the lock is free" says something about a recorded holder only under the boot id of
  its record.
- **Containers.** A container shares the host kernel: the same boot id (the memo's experiment,
  [§15](#not-verified)). A virtual machine with its own kernel has its own boot id: D6.
- **An unreadable boot id** makes the witness unavailable (D1).
- **No clock value crosses a process boundary.** The record stores no time. The settle window subtracts two
  samples of the claimer's own `CLOCK_BOOTTIME`, the clock the wait already uses
  (`CasMountRuntime.cpp:78-83`). `write_reserve_ms` in the record is a duration from the writer's
  configuration, not a clock reading. Time namespaces, clock offsets between containers and clock
  differences between hosts therefore cannot affect the decision.
- Boot id and lock behaviour of sandboxed kernels: not verified. An unlisted file system or an unreadable
  boot id there selects the full wait.

### 3.8 Code shape {#code-shape}

New unit `CA/Pool/CasMountWitness.{h,cpp}`:

```cpp
/// What the local witness knows about one mount-slot body.
struct MountWitnessVerdict
{
    bool local_death = false;
    String reason;                         /// when not local_death: one of the reasons of the decision table
    uint64_t holder_write_reserve_ms = 0;
    uint64_t holder_pid = 0;               /// log only
};

/// What a claim is about to write.
struct MountWitnessClaim
{
    uint64_t writer_epoch = 0;
    MountPriorState prior = MountPriorState::None;
    std::optional<uint64_t> replaced_epoch;   /// empty for a fresh claim
    bool replaced_own_uuid = true;
};

class MountWitness
{
public:
    virtual ~MountWitness() = default;
    /// From the record read when the lock was taken; never from what this handle wrote.
    virtual MountWitnessVerdict evidenceFor(const MountLease & slot_body) const = 0;
    /// Before the claim's write. Lists the epoch of an eligible claim; removes this handle's listing
    /// before an ineligible one. Throws only when that removal fails.
    virtual void beforeClaimWrite(const MountWitnessClaim & claim) = 0;
};
```

- `LocalMountWitness` implements it over the file. Its constructor takes a `WitnessHost`: the source of the
  boot id, the file-system type and the file calls. Production passes the Linux host. Tests pass a fake for
  the two values and keep the real file calls.
- `PoolConfig` (`CA/Pool/CasPool.h:228-234`) gets `std::shared_ptr<MountWitness> mount_witness`, empty by
  default. `Pool::mountWritable` and the self-remount pass it to `claimMount` and
  `claimMountAwaitingExpiry` ([§3.3](#record-writes), wiring). `Pool::openForDecommission` clears it. The
  renewer is not touched.
- `claimMount` (`CasServerRoot.h:330-334`) and `claimMountAwaitingExpiry` (`:388`) get trailing defaulted
  parameters. Existing call sites compile unchanged.
- No production code branches on a test flag. The seams are the `MountWitness` interface, the
  `WitnessHost` and the existing injected clock and sleep (`CasPool.h:260-262`,
  `CasMountRuntime.h:76-77`).
- On a platform without `/proc/sys/kernel/random/boot_id` the host reports the witness unavailable.

## 4. Threat model {#threat-model}

Evidence names: **record** = the inherited record lists the slot's epoch; **boot** = equal boot id;
**lock** = this handle took the exclusive lock; **inode** = the locked file is the recorded
`(st_dev, st_ino)`. Local death needs all four.

| # | The holder can be alive because | Check that catches it | Outcome |
|---|---|---|---|
| T1 | The data directory was copied to another host | boot | full wait (D6) |
| T2 | A disk or volume snapshot was restored on another machine or after a reboot | boot | full wait (D6) |
| T3 | A virtual machine snapshot with memory, or a process checkpoint, runs as a second copy; or two live processes share one `(uuid, writer_epoch)` after the `epoch` object was deleted by hand | none locally; the token changes if the other copy renews inside the settle window | **residual, not defended** (P4). Already unsupported: `D/architecture/mounts-and-leases.md:215-227`, `:236-240` |
| T4 | Two containers on one host share the volume | lock (`flock` excludes across containers on a listed file system); the second server also fails on `status` | no start, or full wait for a tool (D2) |
| T5 | Two hosts share the volume through a network file system | boot; the file system is also not listed | full wait (D1 or D6) |
| T6 | A block-level clone of the volume is attached to the same host | inode (`st_dev` differs) | full wait (D7) |
| T7 | The directory was copied on the same host | inode (`st_ino` differs) | full wait (D7) |
| T8 | A hard-linked copy of the directory (`cp -al`), or the same directory under a second mount point | lock: both names reach one inode | full wait (D2) while the original lives; local death after it died, which is true |
| T9 | The holder is a zombie | a zombie has no descriptors and cannot write | local death, correct |
| T10 | The holder is stopped (`SIGSTOP`, debugger, cgroup freezer) | lock: a stopped process keeps it | full wait (D2); a server also fails on `status` |
| T11 | The holder is alive and its renewals stall | lock | full wait (D2) |
| T12 | pid reuse | not applicable: nothing reads the pid | – |
| T13 | pid namespaces | not applicable: the lock is on the inode | – |
| T14 | Clocks differ between hosts, containers or time namespaces | not applicable: no clock is compared between processes | – |
| T15 | The volume was force-detached and attached elsewhere while the holder runs | boot | full wait (D6) |
| T16 | A tool mounted with the same uuid and without the `status` lock | lock: every writable mount takes the witness lock | full wait (D2) while the tool lives |
| T17 | The holder ran a build without the witness, or had it off | record: such a process lists nothing | full wait (D8) |
| T18 | The holder never held the lock (lock busy, unlisted file system, the bound of 1024) and took the slot by the full wait | record (rule 2) | full wait (D8) |
| T19 | An unsafe reclaim bypassed a live holder; later holders handed over cleanly; the last one died | record (rule 5): none of them was eligible | full wait (D8) |
| T20 | The slot was fenced by GC, or decommissioned and rejoined, and the next holder died | record (rule 5) | full wait (D8) |
| T21 | The witness file was deleted and created again while the holder runs | record: the new file is empty; the holder wrote its record before | full wait (D3) |
| T22 | The data directory was restored in place from a backup | record: old lines name old epochs, which never return to the slot (F12); or inode | full wait (D7 or D8) |
| T23 | A child process of the holder outlives it | the witness descriptor is `O_CLOEXEC` (F18): an executed child does not hold it. A child between `vfork` and `execv` does, briefly | local death, or a full wait while such a child holds the lock |
| T24 | The holder is dead, a request of it is still on the way | not a liveness question: [§3.6](#late-landing) | settle window, then the structural protections |
| T25 | The same process mounts the disk again (disk reload, self-remount) | rule 3: what a handle wrote is not evidence for it; a second handle finds the lock held | full wait, as today |

### 4.1 File systems {#file-systems}

The witness is used only on a file system whose `f_type` is on an allowlist. An unknown type selects the
full wait.

| File system | `f_type` | Status |
|---|---|---|
| tmpfs | `0x01021994` | **Supported.** The memo's experiment on Linux 6.14: exclusion, a stopped holder, a copy, a hard link, release after `SIGKILL`, a bind mount into a container |
| ext4 (and ext2, ext3) | `0xEF53` | **Supported only after the gate run of [§7.6](#tests-os) on it.** Not verified today |
| xfs | `0x58465342` | **Supported only after the gate run of [§7.6](#tests-os) on it.** Not verified today |
| overlayfs | `0x794C7630` | **Not on the list.** The server waits in full and logs one `Information` line that names the file system |
| NFS, CIFS/SMB, CephFS, FUSE, 9p, virtiofs, Lustre, GlusterFS, btrfs, ZFS, f2fs and every other type | not on the list | **Detected, full wait** (D1), the same `Information` line |

**Why `(st_dev, st_ino)` equality plus an acquired lock is sufficient on the three listed file systems.**

- `flock` on a file whose operations define no `flock` method locks the in-kernel inode of the opened file
  (`fs/locks.c:2174-2179`). ext4, xfs and tmpfs define none (`fs/ext4/file.c`, `fs/xfs/xfs_file.c`,
  `mm/shmem.c`).
- `stat` reports the superblock's device and the inode's number for the same in-kernel inode
  (`fs/stat.c:88-89`, `fs/xfs/xfs_iops.c:635-640`, `mm/shmem.c:1249`). `ext4_getattr` was not read.
- So two descriptors with equal `(st_dev, st_ino)` in one boot are on one inode and one lock. Hard links
  and bind mounts are other names of the same inode: they share the lock and report the same pair.
- Three assumptions remain, none verified:
  - a device number is not reused while a file of the old device is open;
  - ext4 and xfs do not reuse an inode number while the unlinked file is still open;
  - on a tmpfs mounted without `inode64` the inode counter wraps after 2^32 files (`mm/shmem.c:364-377`),
    so two live files can share a number. A copy would have to hit the witness's number.

**Why overlayfs is excluded.** On overlayfs the reported pair need not identify the lock. Linux 4.19 with
layers on different file systems and without `xino` reports, for a file of the upper layer, the device and
inode of the underlying file (`fs/overlayfs/inode.c:100-140` of v4.19), while `flock` locks the overlay's
own inode. The same file opened through the upper layer's path is another lock with the same reported
pair. ext4 is listed, so that second path would pass every check while the holder lives. An experiment on
one kernel cannot rule this out for all supported kernels and layer layouts.

- Detection is `fstatfs` on the witness descriptor. A bind mount reports the underlying file system.
- The mount logs the file system name and `f_type`, so a server log shows which row applied.
- A file system that reports a listed `f_type` but does not implement `flock` exclusion would break the
  claim. None is known; this is the trust the design places outside the repository, as `StatusFile` does
  for the data directory.
- Detach and re-attach of a volume by a storage driver may change `st_dev`: D7, full wait. Not verified.

## 5. Mixed versions, upgrade, downgrade {#mixed-versions}

The witness changes no stored byte, so every stored object stays readable in both directions.

| Situation | Behaviour |
|---|---|
| Previous process without the code, restart with the code | No line lists the slot's epoch: full wait (D3 or D8) |
| Previous process with the code, restart without it | The files are ignored: full wait |
| A server without the code observes a slot of a server with it (GC leader, `system.cas_mounts`) | Unchanged: same body, same threshold |
| A server without the code fences that slot | The fence copies the body and sets `gc_fenced`; the restart reclaims at once, as today, and lists no epoch |
| A server with the code reclaims by local death while a server without the code observes the same token | Both writes are conditional on the token. One lands, the other is refused, as with two observers today |
| A pool with a server that has the code and one that has not | No interaction: the witness is per server and per host |

Upgrade and downgrade:

- **Upgrade in place.** The upgraded server has no record. Its first claim is `Clean` over a farewell that
  no record lists, so it lists nothing. Its first unclean stop pays the full wait; that claim is
  `UncleanObserved` and starts the record.
- **Downgrade.** The older build ignores `cas_mount_witness`. The file stays, stale.
- **Downgrade, then upgrade again.** The stale record lists epochs that can never return to the slot (F12):
  D8. If the host rebooted in between: D6. The first eligible claim replaces the stale record.
- **A newer record version read by this build** is `unreadable` (D4) and is overwritten.
- **The setting key.** A build without the code refuses `cas_mount_lease_witness` with `UNKNOWN_SETTING`,
  as it does for `cas_request_window_ms` (`D/configuration.md:334`). The default is on, so the key is
  needed only to turn the witness off.

## 6. Observability {#observability}

Log lines. `{…}` are values. Logger names follow the existing lines.

| When | Level, logger | Text |
|---|---|---|
| Witness opened and locked | `Information`, `CasMountWitness` | `CAS mount '{srid}': mount witness '{path}' is locked (file system {name} 0x{f_type}, dev {dev}, inode {ino}, boot {boot_id}); inherited record: {present with epochs e1, e2 \| absent \| unreadable: detail}` |
| Witness off | `Information`, `CasMountWitness` | `CAS mount '{srid}': the mount witness is off (cas_mount_lease_witness = 0); a restart after an unclean stop waits the full mount lease observation` |
| File system not listed | `Information`, `CasMountWitness` | `CAS mount '{srid}': the mount witness is not used on file system {name} (0x{f_type}) of '{path}'; a restart after an unclean stop waits the full mount lease observation` |
| Witness not usable for another reason (open error, no boot id, the bound, lock held) | `Warning`, `CasMountWitness` | `CAS mount '{srid}': mount witness '{path}' is not used: {reason}. This mount writes no witness record; a restart after an unclean stop waits the full mount lease observation` |
| Local death (D9), per observed token | `Information`, `CasMountLease` | `CAS mount '{srid}': the mount witness proves that the process that wrote the mount slot is dead (writer_epoch {e}, pid {pid}; listed in this boot, lock taken on the recorded witness file); reclaiming once the slot token stays unchanged for ~{S} ms` |
| No evidence (D3 to D8), per observed token | `Information`, `CasMountLease` | `CAS mount '{srid}': the mount witness gives no evidence about the mount slot (writer_epoch {e}): {reason}; waiting the full observation threshold` |
| Full wait | unchanged | the line at `CasServerRoot.cpp:1292-1295` |
| Wait start, pool level | unchanged text, `CasPool.cpp:714-720` | printed with the wait that applies (`S` or the threshold) |
| Unclean reclaim | `Information`, `CasPool.cpp:913-919` | the existing line; for `UncleanLocalDeath` it ends with ` (the previous process was proven dead by the mount witness)` |
| A claim that lists no epoch | `Information`, `CasMountWitness` | `CAS mount '{srid}': writer_epoch {e} is not listed in the mount witness: {why}. A restart after an unclean stop of this process waits the full mount lease observation` |
| Record write failed | `Warning`, `CasMountWitness` | `CAS mount '{srid}': cannot write the mount witness record '{path}': {error}. The claim continues; a restart after an unclean stop waits the full mount lease observation` |

Reasons of the decision table, fixed strings: `lock-held: the witness lock is held by another process or by
an earlier mount of this process`, `no-record: the witness file holds no record`,
`record-unreadable: {detail}`, `other-identity: the record belongs to server_root_id '{srid}', server_uuid
{uuid}`, `other-boot: the record was written in boot {recorded}, this is boot {current}`,
`other-witness-file: the record names dev {d}, inode {i}; the locked file is dev {d}, inode {i}`,
`not-recorded: the record does not list writer_epoch {e}`, `unavailable: {detail}`.

Reasons a claim lists no epoch, fixed strings: `the slot was reclaimed under cas_unsafe_remount_no_delay`,
`the previous holder was fenced by GC`, `the slot was released by a holder this witness does not list`,
`the slot was taken over from another server_uuid`, `the slot was absent under a root that had holders`,
`this mount does not hold the witness lock`.

Existing surfaces:

- **`system.cas_log`.** The reclaim emits the existing `MountClaim` event with branch `reclaim`
  (`CasServerRoot.cpp:1101-1109`). Its `reason` for the new prior state is
  `same server_uuid, different writer_epoch, previous process proven dead by the mount witness — reclaimed`.
  Whether a mount event of the first open reaches the table is not verified: two comments disagree
  (`CasServerRoot.cpp:919-921`, `CasPool.cpp:723-728`). Tests assert on the server log.
- **`system.cas_mounts`: no new column.** The table shows the slot, and the prior state is a fact of one
  start-up, not of the slot. A column would be `NULL` on every peer row and stale after the first renewal.
- **No new metric.** A start-up event happens once per process; the log line and the `cas_log` row carry it.

Settings documentation, `D/configuration.md`, one new row:

> `cas_mount_lease_witness` | `1` | Keep a lock and a small record in
> `<path>/disks/<disk>/cas_mount_witness`, so that a restart on the same host can prove that the previous
> server process is dead. With proof, a restart after `SIGKILL`, an OOM kill, an abort or a forced shutdown
> waits `2 × envelope + cas_lease_safety_margin_ms + cas_mount_renew_period_ms / 2` with the mount slot
> unchanged (21 s for an S3 disk with defaults, 17 s for a local object storage) instead of the observation
> threshold (131 s). Without proof the restart waits the full threshold as before, and logs the reason:
> after a host reboot; on another host; from a copied or restored data directory; when the previous process
> ran a build without the witness or had it off; when another live process holds the lock; when the data
> directory is not on ext4, xfs or tmpfs (a container's own overlayfs layer is not supported: put the data
> directory on a volume); once after an upgrade, a GC fence of this server's slot, a start under
> `cas_unsafe_remount_no_delay` or a decommission. `0`: the file is not opened and every unclean restart
> waits the full threshold. A build without the witness refuses the key with `UNKNOWN_SETTING`.

The list of file systems in that row follows the gate runs of [§7.6](#tests-os).

## 7. Tests {#tests}

Rules:

- **Red first.** Each test runs against the tree without the product change of its commit, with the new
  declarations present and the new decision ignored. The failing output is saved as a log. A compile error
  is not red evidence.
- **Seams.** Lease-level tests pass a fake `MountWitness` and the existing fake clock and sleep. Witness
  tests use a real temporary directory, real `flock`, and a fake `WitnessHost` for the boot id and the
  file-system type. A copied directory is a copy of the witness file: same content, another inode.
- **Fakes prove the rules, not the kernel.** The lock and inode properties are tested with real processes
  in [§7.6](#tests-os) and [§7.7](#tests-integration).
- Suites keep the `CAS` prefix, so the `CAS*` gate runs them.

### 7.1 Record encoding {#tests-record}

`T/gtest_cas_mount_witness.cpp` (new), suite `CASMountWitnessRecord`. Fixture: the codec alone.

| # | Test | Red before |
|---|---|---|
| R-1 | `RoundTripsEveryField` | no codec: the decode returns `unreadable` |
| R-2 | `EncodedRecordIsOneBlock` | size is not 4096 |
| R-3 | `TornBlockIsUnreadable` (truncated at several offsets; one byte flipped) | a damaged block decodes as `present` |
| R-4 | `UnknownVersionIsUnreadable` | version 2 decodes as `present` |
| R-5 | `EmptyFileIsAbsent` | an empty file is `unreadable` |
| R-6 | `RecordThatDoesNotFitIsRefused` | an oversized record is written |

### 7.2 Witness file {#tests-witness}

Same file, suite `CASMountWitness`. Fixture: temporary directory, fake `WitnessHost`. A held lock is a
second handle in the test process: `flock` locks belong to the open file description.

| # | Test | Decision row or rule | Red before |
|---|---|---|---|
| L-1 | `OpenCreatesTheFileAndHoldsTheLock` | – | a second descriptor can lock the file |
| L-2 | `HeldLockGivesNoEvidenceAndWritesNoRecord` | D2, rule 2 | evidence is local death; the file changes |
| L-3 | `ReopenAfterTheHolderClosedProvesLocalDeath` | D9 | no evidence |
| L-4 | `EmptyAndUnreadableRecordsGiveNoEvidence` | D3, D4 | local death or an exception |
| L-5 | `RecordOfAnotherIdentityGivesNoEvidence` | D5 | local death |
| L-6 | `OtherBootIdGivesNoEvidence` | D6 | local death |
| L-7 | `CopiedFileGivesNoEvidence` | D7 | local death |
| L-8 | `UnlistedEpochGivesNoEvidence` | D8 | local death |
| L-9 | `WhatAHandleWroteIsNotEvidenceForIt` | rule 3 | an epoch listed through this handle is evidence |
| L-10 | `UnlistedFileSystemIsNotUsed` (NFS and overlayfs types) | D1 | the file is locked and written |
| L-11 | `UnreadableBootIdIsNotUsed` | D1 | the record is written with an empty boot id |
| L-12 | `HandleThatNeverWroteClosesItsDescriptor` | rule 1 | the lock stays held after the handle is gone |
| L-13 | `HandleThatWroteKeepsTheLockAfterItIsGone` | rule 1 | the lock is free |
| L-14 | `BoundOnWritingHandlesLeavesLaterMountsUnwitnessed` | [§3.1](#witness-file) | the 1025th handle opens the file |

### 7.3 Eligibility {#tests-eligibility}

Same file, suite `CASMountWitnessEligibility`. Fixture: a real witness file, claims described by
`MountWitnessClaim`.

| # | Test | Red before |
|---|---|---|
| E-1 | `FreshClaimAtEpochOneIsListed` | nothing is written |
| E-2 | `FreshClaimAtALaterEpochIsNotListed` | the epoch is listed |
| E-3 | `ObservedClaimIsListed` | nothing is written |
| E-4 | `CleanClaimIsListedOnlyOverAListedFarewell` | listed over an unlisted farewell |
| E-5 | `LocalDeathClaimIsListedAndKeepsTheReplacedEpoch` | the replaced epoch is dropped |
| E-6 | `UnsafeFencedAndForeignClaimsAreNotListed` (one case each) | the epoch is listed |
| E-7 | `IneligibleClaimRemovesAnEarlierListingOfItsEpoch` | the earlier listing stays |
| E-8 | `FailedRemovalFailsTheClaim` | the claim proceeds with the epoch listed |
| E-9 | `FailedWriteOfAnEligibleRecordLetsTheClaimProceed` | an exception escapes |
| E-10 | `ReplacedEpochSurvivesOnlyUnderAnEqualHeader` (other boot, other inode, other identity) | an epoch of another boot appears under this boot's header |
| E-11 | `MountWithoutTheLockListsNothing` | the file changes |

### 7.4 Claim site {#tests-recording}

`T/gtest_cas_mount.cpp`, suite `CASMountWitnessClaim`. Fixture: `InMemoryBackend` behind the instrumented
backend, a fake witness that logs its calls in order with the backend's writes.

| # | Test | Red before |
|---|---|---|
| H-1 | `FreshClaimIsDescribedBeforeItsCreate` | no witness call |
| H-2 | `ReclaimIsDescribedWithItsPriorStateBeforeItsReplace` (observed, clean, fenced, unsafe, foreign terminal, local death) | no witness call, or a wrong prior state |
| H-3 | `RenewalAdoptionFarewellAndFenceDoNotCallTheWitness` | pin: green before and after |
| H-4 | `WitnessFailureBeforeAnIneligibleClaimSendsNoWrite` | the write is sent |

### 7.5 Decision and pool level {#tests-decision}

`T/gtest_cas_mount.cpp`, suite `CASMountAwaitExpiryWitness`. Fixture: `claimMountAwaitingExpiry` over
`InMemoryBackend`, fake clock and sleep as `CASMountAwaitExpiry.PastExpiryStillPaysTheFullObservationThreshold`
(`:1326`), lease 100, poll 25, a fake witness with a scripted verdict per body.

| # | Test | Row | Red before |
|---|---|---|---|
| D-1 | `LocalDeathReclaimsAfterExactlyTheSettleWindow` | D9 | waits 130; prior `UncleanObserved` |
| D-2 | `LocalDeathUsesTheLargerRecordedWriteReserve` | D9 | waits the own reserve |
| D-3 | `LateRenewalOfTheDeadHolderRestartsTheSettleWindow` | D9, new token, same epoch | reclaims before the new window ends |
| D-4 | `TokenChangeToAnUnlistedEpochWaitsTheFullThreshold` | D9 then D8 | reclaims after the settle window |
| D-5 | `NoEvidenceWaitsExactlyAsWithoutAWitness` (one case per reason of D0 to D8) | D0 to D8 | pin for the wait; red on the logged reason |
| D-6 | `LocalDeathReclaimIsTokenExact` (the slot is replaced between the last read and the reclaim) | [§3.4](#decision-table) | pin once the argument exists: the foreign body is never overwritten |
| D-7 | `TerminalAndForeignSlotsIgnoreTheWitness` (fenced, farewell, foreign uuid, newer epoch, same pair) | unchanged slots | pin |
| D-8 | `RenewingHolderIsStillRefusedDespiteALocalDeathVerdict` (the witness lies) | twin detection | pin: `LiveDoubleStart` after 3 restarts |
| D-9 | `WaitLinesNameTheOutcome` (log capture as `ScopedRemountLogCapture`, `T/gtest_cas_pool.cpp:2396-2424`) | [§6](#observability) | the new lines are absent |
| D-10 | `SettleWindowFollowsTheBudget` (S3 budget: 21000; no connect cap: 17000) | [§3.5](#arithmetic) | a fixed number |

`T/gtest_cas_pool.cpp`, suite `CASMountOpenWaits` (next to `UncleanOpenPaysOnlyTheObservationWindow`,
`:2600`). Fixture: `Pool::open` calls over one `InMemoryBackend`, a real `LocalMountWitness` on a temporary
directory, fake boot clock and sleep. A killed pool is the stale-body replay of
`CASMountStartup.StaleSelfMountReclaimedAfterWait` (`T/gtest_cas_mount.cpp:1839`) plus releasing its lock
the way a dead process would.

| # | Test | Red before |
|---|---|---|
| P-1 | `KilledPoolReopensAfterTheSettleWindow` | the second open waits the threshold |
| P-2 | `ReopenWithACopiedWitnessOrAnotherBootWaitsTheFullThreshold` | pin |
| P-3 | `ReopenWithoutARecordWaitsTheFullThreshold` | pin |
| P-4 | `StalledLiveHolderIsNotReclaimedBeforeItsLeaseEnds` (first pool alive, its renewals fail, second open on the same directory) | pin: the second open never claims while the first passes `mayMutate` |
| P-5 | `UnsafeHistoryPaysAFullWaitBeforeTheWitnessIsUsedAgain` (holder `A` without a witness stays alive; `B` reclaims under the knob and writes a farewell; `C` claims; `C` is killed; `D` must wait the threshold; a pool killed after `D` reopens after `S`) | `D` reopens after `S` |
| P-6 | `DecommissionPoolHasNoWitness`; a rejoin after it lists nothing | the admin pool writes the record |
| P-7 | `KilledDuringItsOwnReclaimStillReopensAfterTheSettleWindow` (carry-over) | the third open waits the threshold |
| P-8 | `RecordFollowsEveryClaimOfOneProcess`: fresh open; a renewal with an unknown outcome; a self-remount (record: old and new epoch); a fence met by the adoption (no epoch for the new claim); a second self-remount; kill; reopen. The record is asserted after each step | the record misses a claim or lists an ineligible one |
| P-9 | `FencedSlotPaysOneFullWaitThenTheWitnessIsUsedAgain` | the pool after the fence is listed |

`T/gtest_cas_settings.cpp`: S-1 `CASContentAddressedSettings.MountLeaseWitnessDefaultsOn`. Red before: the
setting does not exist.

### 7.6 OS level, real processes {#tests-os}

`T/gtest_cas_mount_witness_os.cpp` (new), suite `CASMountWitnessOs`. The holder is the production
`LocalMountWitness` in the test process. The prober is **a second process**: a `fork` child that uses only
`open`, `flock`, `fstat` and `_exit`, and reports through its exit status. Nothing is faked except the
boot id.

| # | Test | Asserts |
|---|---|---|
| O-1 | `SecondProcessCannotLockWhileTheHolderLives` | the child gets `EWOULDBLOCK` |
| O-2 | `EveryPathToTheFileSharesTheLock` | the child is refused through a hard link, through a symlink, and through `/proc/<holder pid>/fd/<n>`; each path reports the holder's `(st_dev, st_ino)` |
| O-3 | `CopyIsAnotherLockAndAnotherInode` | the child locks a `cp -a` copy while the holder lives; the copy's `(st_dev, st_ino)` differs; a production handle opened on the copy answers `other-witness-file` |
| O-4 | `ExecutedChildDoesNotInheritTheLock` | the holder starts `/bin/sleep` by `fork` and `exec`; the handle that never wrote is destroyed; the prober locks the file while the `sleep` process lives |
| O-5 | `ChildThatDidNotExecKeepsTheLock` | a `fork` child that sleeps without `exec` keeps the lock after the parent's handle is gone; the lock is free when the child exits |
| O-6 | `KilledHolderProcessReleasesTheLock` | a child takes the lock and is stopped with `SIGSTOP`: the lock stays held. After `SIGKILL` a production handle takes the lock on the same `(st_dev, st_ino)` |
| O-7 | `FailedMountBeforeItsClaimReleasesTheLock` | `Pool::open` fails before the claim (the backend refuses); the prober then locks the file |
| O-8 | `FailedMountAfterItsClaimKeepsTheLock` | `Pool::open` fails after the claim (owner recheck refused); the prober is still refused after the pool object is destroyed |

**Gate runs for the listed file systems.** The suite takes its directory from `CAS_WITNESS_TEST_DIR` and
the expected type from `CAS_WITNESS_TEST_FS`. It first asserts, with the production `fstatfs` check, that
the directory is on that type, so a wrong mount cannot pass. The gate runs it three times: on tmpfs, on
ext4 and on xfs (loop-mounted images are enough). Without the variables the suite runs in the default
temporary directory with the file-system type answered by the seam, so it runs everywhere and skips nothing.

### 7.7 Integration {#tests-integration}

New module `tests/integration/test_cas_restart_witness/`: one node, **no lease setting** in its
configuration, one S3 CAS disk in the configuration and two local-object-storage CAS disks defined in table
metadata (the stress shape).

| # | Test | Asserts | Red before |
|---|---|---|---|
| I-0 | `test_data_directory_is_on_a_listed_file_system` | `stat -f -c '%T 0x%t' /var/lib/clickhouse` inside the node's container, and the witness-open log line; fails with the file system name when it is not ext4, xfs or tmpfs | – (measurement; it decides what I-1 to I-3 can show) |
| I-1 | `test_sigkill_restart_is_ready_inside_the_start_budget` | `stop_clickhouse(kill=True)`, then `start_clickhouse(start_wait_sec=60)` succeeds. Log: one local-death line with `~21000 ms` for the S3 disk and one with `~17000 ms` for each local disk; no `token-stability observation` line. From log timestamps, each reclaim comes at least `S` and at most `S` + 10 s after its local-death line. Every mount is `live` with a higher `writer_epoch` | `Cannot start ClickHouse`: the server waits 131000 ms |
| I-2 | `test_forced_shutdown_restart_is_ready_inside_the_start_budget` | `shutdown_wait_unfinished` = 2, a query holds a connection, `SIGTERM`; the old server logs `Will shutdown forcefully`; then as I-1 | the same |
| I-3 | `test_witness_lock_follows_the_server_process` | while the server runs, `flock -xn` on the witness file, on a hard link to it and on `/proc/<pid>/fd/<n>` is refused. A long-running executable function is started; its process has no descriptor on the witness inode. After `kill -9` of the server, with that child still alive, `flock -xn` succeeds | the child holds the descriptor, or the lock is free while the server lives |

Changed module `tests/integration/test_cas_mount_renewal_retry/` (lease 10 s, period 2 s, attempt timeout
500 ms; threshold 11500 ms; `S` = 3000 + 500 + 1000 = 4500 ms):

| # | Test | Asserts | Red before |
|---|---|---|---|
| I-4 | `test_hard_restart_observes_then_the_unsafe_knob_skips_the_observation` (`test.py:620-681`), rewritten in three phases | default: one local-death line with `~4500 ms`, no observation line. With a `cas_mount_lease_witness = 0` overlay: one `waiting ~11500 ms (token-stability observation)` line. With the unsafe overlay: neither | phase 1 has the observation line |
| I-5 | `test_restart_with_a_replaced_or_missing_witness_file_waits_in_full` | after the kill the witness file is replaced by a copy of itself (`cp -a`, `rm`, `mv`): reason `other-witness-file` and the full observation line. A second round deletes the file: `no-record` and the full observation line | pin before the feature; red against a build whose inode check is removed |
| I-6 | `test_unsafe_history_pays_one_full_wait` | kill; start under the unsafe overlay (an unsafe reclaim over the live body); stop cleanly; start without the overlay; kill; start: reason `not-recorded` and the full observation line. Kill again; start: the local-death line | the first restart after the unsafe history reclaims after `S` |

Existing test that must pass without a pin: I-7
`test_backup_restore_on_cluster/test.py::test_shutdown_waits_for_backup`, first attempt, no rerun.

- **A copied data directory in a second container was not chosen.** The inode replacement of I-5 and O-3
  exercise the same check. A foreign boot id cannot be produced in a container; L-6 and P-2 cover it
  through the host seam.
- Whether the node image has `flock(1)` of util-linux: not verified. If it has not, I-3 ships a helper.

Lease pins in test configurations:

| Configuration | Pin | Reason for the pin | After this story |
|---|---|---|---|
| `test_cas_early_commit_restart/configs/storage_conf.xml:16` | 30000 | restart wait only (`test.py:88-96`) | removed |
| `test_cas_gc_sharded/configs/storage_conf.xml:29` | 30000 | restart wait only (`test.py:261`) | removed |
| `test_cas_shared_pool/configs/storage_conf.xml:27` | 30000 | restart wait (`test.py:265-304`); a later assertion depends on the killed server's lease (`:313-322`) | removed only if the module passes without it; else kept with a comment that names the assertion |
| `test_cas_drop_pool_member/configs/storage_conf.xml:24` | 30000 | a peer waits for the killed member's lease and the GC fence (`test.py:248-282`): a foreign observer, unchanged here | kept |
| `test_cas_mount_renewal_retry/configs/storage_conf.xml:32-33` | 10000 / 2000 | renewal-retry timing | kept |
| `test_cas_mount_renewal_retry/configs/request_budget_disks.xml:47` | 50000 | request-budget probe | kept |
| `test_cas_mount_renewal_retry/configs/unsafe_remount.xml:8` | knob | tests the knob | kept |
| `test_backup_restore_on_cluster/configs/cas_storage.xml` | none | – | stays without a pin (I-7) |

`test_cas_gcs` and `test_cas_replicated_relink` kill a node without a pin; whether they restart it inside
a budget: not verified. The regression suites live in `Altinity/clickhouse-regression` and set no lease
(`E/hard-restart.md` section 4).

## 8. Formal model {#formal-model}

Two existing modules cover the two halves. Both are extended behind a feature constant that is off in
every existing configuration, so existing state counts do not change (the precedent is `RecoveryGenOn`,
`M/CaCasMountCore.tla:251-257`).

**What the models do not check.** They take "the lock is one lock" and "the boot is one boot" as given.
Neither model can find a file system whose inode identity lies, or a wrong boot id. The tests of
[§7.6](#tests-os) and the allowlist carry those two facts.

### 8.1 Timing: `CaMountObserveCore` {#model-timing}

Today: one holder, one observer, one slot (`M/CaMountObserveCore.tla:59-79`). The observer fences a token
it watched for `Threshold` (`:214-243`). Invariant `NoFenceInsideAuthority` (`:287-288`). A request that
lands after the fence is not modelled (`M/CaMountObserveCore_RESULTS.md:239-240`).

Add:

| Kind | Name | Meaning |
|---|---|---|
| Constant | `WitnessOn` | feature gate |
| Constant | `Settle`, `MinRequestAge` | the settle window; the bound of the new invariant |
| Variable | `holderDead` | set by `Die`; disables `RenewStart` and `Write`; a renewal in flight may still land |
| Variable | `lockTaken` | the observer holds the lock; enabled only when `holderDead` |
| Variable | `lastWrite` | history: holder clock of the last `Write` |
| Action | `Die` | the holder dies at any time |
| Action | `ObsTakeLock` | as above |
| Change | `ObsDecide` | fences when the existing rule holds, or when `lockTaken` and the observer's sample is at least `Settle` after its sighting of the token it read |

The extension compares no value of the holder's clock with the observer's clock.

Invariants:

- `NoClaimWhileWriteAdmissible`: `slotFenced => holderDead \/ hclock >= AuthorityEnd`. This is the
  statement "no successor claims while the previous holder can still have a write admitted". It replaces
  `NoFenceInsideAuthority` in the new configurations and equals it when `holderDead` is false.
- `NoWriteAfterFence`: unchanged.
- `RequestAgeAtLeast`: `slotFenced => hclock − lastWrite >= MinRequestAge`. Run with `AuthoritySpan` set to
  the write cutoff (the `_earlyfence` shape), so `lastWrite` models the last admitted request.

Configurations (scale of `_remount`: `TTL` 4, `Period` 2, `Allowance` 1, `Cadence` 1):

| Config | Expect | Shows |
|---|---|---|
| `_witness_death` | green | local death is safe; `RequestAgeAtLeast` holds with `MinRequestAge = Settle` |
| `_witness_death_age_tight` | `RequestAgeAtLeast` violated | `MinRequestAge = Settle + 1`: the bound is exact |
| `_sab_death_without_lock` | `NoClaimWhileWriteAdmissible` violated | local death decided while the holder lives |
| `_sab_death_no_settle` | `RequestAgeAtLeast` violated | `Settle` = 0 |
| `_witness_local_death_fences`, `_witness_late_renewal_after_death` | witness reached | the path exists; a renewal that lands after the death restarts the window |

### 8.2 Identity and lineage: `CaCasMountCore` {#model-identity}

Today: incarnations of one uuid (`Incs`, `M/CaCasMountCore.tla:424`), `crashed` set by `Die` (`:865-874`),
`ObservedReclaim` (`:1014-1034`), `CanWrite` (`:441`). The module has no farewell and no unsafe reclaim.
The twin configurations check `OneWritableIncarnationPerUuid` (`:1676-1677`) and
`SupersededWriterMakesNoMutation` (`:1602`), not `GlobalSupersededWriterMakesNoMutation` (header of
`M/CaCasMountCore_twins.cfg`).

Add:

| Kind | Name | Meaning |
|---|---|---|
| Constant | `LocalWitnessOn` | feature gate |
| Constant | `WitnessGroup` | the incarnations that share one witness file and one boot; the others are elsewhere |
| Constant | a third incarnation per uuid | needed for the induction: a live holder elsewhere, a holder in the group, a restarter |
| Variable | `lockHolder` | `None` or one member of `WitnessGroup`; taken at the member's first claim if free; released only by `Die` |
| Variable | `listed` | the epochs in the witness file |
| Variable | `inherited` | per incarnation: the value of `listed` when it took the lock (rule 3) |
| Action | `Farewell(a)` | the holder of the current body makes it terminal and stops writing |
| Action | `CleanClaim(a)` | a claim over a farewell, at once |
| Action | `UnsafeReclaim(a)` | a claim over a live body of the same uuid with no wait, behind a constant `UnsafeKnob` |
| Action | `LocalDeathReclaim(a)` | `a = lockHolder`, the body's epoch is in `inherited[a]`, the body has `a`'s uuid and a lower epoch and is not fenced; installs `a`'s body |
| Change | every claim action | adds the claim's epoch to `listed` exactly under the eligibility table of [§3.3](#record-writes), and only when `a = lockHolder` |
| History | `claimedOverWriter` | set when `LocalDeathReclaim` fires while some other incarnation satisfies `CanWrite` |

Invariants in the new configurations: `OneWritableIncarnationPerUuid`, `SupersededWriterMakesNoMutation`,
`NoPermanentWedge`, `ClaimNeverRegressesEpoch`, and the new `NoClaimOverLiveWriter == ~claimedOverWriter`.

| Config | Expect | Shows |
|---|---|---|
| `_witness_twins` (two incarnations, both in the group) | green | the restarter reclaims over a dead co-located holder only |
| `_witness_twin_elsewhere` (the holder is outside the group) | green; `LocalDeathReclaim` never enabled | a holder elsewhere is never listed |
| `_witness_triplet` (three incarnations: one elsewhere, two in the group; `UnsafeKnob` on) | green, **explored to completion** | the induction of [§3.5](#arithmetic), unsafe histories included |
| `_sab_localdeath_no_lock` | `NoClaimOverLiveWriter` violated | the reclaim ignores the lock |
| `_sab_localdeath_no_record` | violated | the reclaim ignores the record |
| `_sab_localdeath_own_entries` | violated | evidence from what the incarnation listed itself |
| `_sab_unlocked_writer_records` | violated | an incarnation without the lock lists its epoch |
| `_sab_unsafe_claim_listed` | violated | an unsafe reclaim lists its epoch |
| `_sab_clean_over_unlisted_listed` | violated | a `Clean` claim over an unlisted farewell lists its epoch: unsafe `B` over live `A`, farewell, `C`, death of `C`, `D` |
| `_witness_localdeathreclaim`, `_witness_unsafe_history_full_wait` | witness reached | the action fires; after an unsafe history the next reclaim is `ObservedReclaim` |

The `Fenced` exclusion of [§3.3](#record-writes) has no sabotage row: the model's `GcFence` already waits
the full threshold, so listing after a fence is safe in the model. The exclusion covers a fencing build
that the model does not describe.

Not modelled: the live memory clone and the shared `(uuid, epoch)` pair (T3). It is a stated residual; the
existing constraint `NoEpochWipe` prunes it.

### 8.3 Acceptance of the model {#model-acceptance}

- `M/run_mountobserve.sh` and `M/run_mount.sh` list the new rows with their exact expectations and print
  `ALL EXPECTATIONS MET`.
- Every red row is red on the named invariant.
- **Every green row ends with `0 states left on queue`, `_witness_triplet` included.** A bounded search that
  did not finish does not pass the gate. If the triplet does not complete at the first bounds, the bounds
  are reduced until it does; reduced bounds are accepted only if every sabotage row of the table is still
  red at the same bounds.
- `M/CaMountObserveCore_RESULTS.md` and `M/CaCasMountCore_RESULTS.md` get a section with the table of rows,
  state counts, TLC version and the command.

## 9. Commit series {#commits}

Model commits land in the `master` worktree (the `M/` files are not in the pull-request tree). Code commits
land on the work branch over `a3967637843`.

| # | Subject | Contains | Tests |
|---|---|---|---|
| M1 | Model the local-death settle window in `CaMountObserveCore` | [§8.1](#model-timing), configurations, runner rows, RESULTS section | TLC rows |
| M2 | Model the mount witness lineage in `CaCasMountCore` | [§8.2](#model-identity), configurations, runner rows, RESULTS section | TLC rows |
| W1 | Add the record codec of the CAS mount witness | `CA/Pool/CasMountWitness.{h,cpp}`: record struct, encode, decode. Nothing calls it | R-1 to R-6 |
| W2 | Add the CAS mount witness file with its exclusive lock | `MountWitness`, `LocalMountWitness`, `WitnessHost`, descriptor ownership and the bound, the file-system list, the eligibility rule. Nothing opens it | L-1 to L-14, E-1 to E-11, O-1 to O-6 |
| W3 | Describe every mount claim to the witness before its write | the two sites of [§3.3](#record-writes); `PoolConfig::mount_witness`; the wiring table; cleared for decommission | H-1 to H-4, P-6, P-8, P-9, O-7, O-8 |
| W4 | Reclaim after a settle window when the witness proves a local death | the decision of [§3.4](#decision-table); the `claimMount` argument; `MountPriorState::UncleanLocalDeath`; the switch and the log lines | D-1 to D-10, P-1 to P-5, P-7 |
| W5 | Open the mount witness for writable CAS disks by default | setting `cas_mount_lease_witness`; `ContentAddressedMetadataStorage::openPoolView` opens the witness; the integration module and the changed tests | S-1, I-0 to I-6 |
| W6 | Document the CAS mount witness | the files of the list below | – |
| W7 | Drop the restart-wait lease pins from CAS integration tests | the pin table of [§7.7](#tests-integration) | the three modules, I-7 |

**Gates of W5, the commit that enables the behaviour.** W5 is not written before all three are recorded:

| Gate | Evidence |
|---|---|
| G1, model | [§8.3](#model-acceptance), with `_witness_triplet` explored to completion |
| G2, operating system | suite `CASMountWitnessOs` green on tmpfs, ext4 and xfs ([§7.6](#tests-os)); a file system without a green run is taken off the list in W2 |
| G3, lanes | the measurement of A10: the file system of the data directory in the three CI lanes |

Rules of the series:

- **Every code commit builds and passes `CAS*`.** W1 to W4 add code that production does not reach until W5.
- A new `gtest_*.cpp` file needs `cmake .` in every build directory before the next build.
- Subjects are imperative and at most 72 characters. Trailers: `Signed-off-by` and `Co-Authored-By`.
- Comments in the code give reasons. They do not cite this document, the memo, task numbers or decision
  row numbers.

Documentation of W6:

| File | Change |
|---|---|
| `D/configuration.md` | the setting row; the restart-wait row of the lease-cost table (`:150`) and its paragraph (`:157-161`) |
| `D/architecture/mounts-and-leases.md` | a new section "Local mount witness" with the file, the rules, the supported file systems and the limits; `MountPriorState` table (`:270-276`): the new value; the state diagram; the paragraph on a killed server (`:352-354`); the clone section (`:215-227`): the witness does not defend a memory clone |
| `D/operations/troubleshooting.md` | a row: a restart after a kill waits about 135 s; the logged reasons and what to do for each, with "put the data directory on a volume" for overlayfs |
| `D/operations/migration.md` | upgrade notes (`:167-181`): the first unclean restart after an upgrade waits in full; the setting key and older builds |

User docs use plain words: no row numbers of this document, no task numbers. Every new header gets an
explicit `{#anchor}`.

## 10. Acceptance criteria {#acceptance}

| # | Criterion | Checked by |
|---|---|---|
| A1 | With default settings and the data directory on a listed file system, a server killed with `SIGKILL` and started at once reclaims each mount after its settle window (21 s for an S3 disk, 17 s for a local one) and is ready inside a 60 s start budget | I-1; measurement M-1 |
| A2 | The same after a forced shutdown (`Will shutdown forcefully`) | I-2 |
| A3 | Every row D0 to D8 waits exactly the threshold it waits today and logs its reason | D-5, L-2, L-4 to L-8, L-10, L-11, P-2, P-3 |
| A4 | A copied witness file, another boot id, a missing record, a held lock, an unlisted file system and a token that changes to an unlisted epoch never give the settle window | L-2, L-6, L-7, L-10, D-4, P-2 to P-4, I-5, O-3 |
| A5 | The reclaim after local death is conditional on the observed token | D-6 |
| A6 | A live holder is never reclaimed by local death | P-4, D-8, O-1, O-2; `_sab_death_without_lock` and `_sab_localdeath_no_lock` red |
| A7 | After an unsafe reclaim, a GC fence, a decommission or an upgrade, the next unclean stop waits the full threshold | E-4, E-6, P-5, P-6, P-9, I-6; `_sab_unsafe_claim_listed` and `_sab_clean_over_unlisted_listed` red |
| A8 | No stored object changes its bytes: the mount body, the owner and the epoch object encode as before | existing golden and round-trip tests of `CA/Formats`, unchanged and green |
| A9 | With `cas_mount_lease_witness = 0` no witness file is opened and the restart logs the full observation | I-4 phase 2 |
| A10 | **Lane measurement, before the design is said to fix a lane.** For each of the three CI lanes the file system of `/var/lib/clickhouse` is recorded in the measurement report. Method: integration, I-0 (`stat -f -c '%T 0x%t' /var/lib/clickhouse` through `exec_in_container`); regression, the same command through `docker compose exec` on a run of `atomic_insert`, and the witness-open line of the server log that the job uploads; stress, the same command as the first step of the job's container, and the witness-open line of the uploaded server log | the measurement report; gate G3 |
| A11 | The model gate G1 and the operating-system gate G2 are recorded before W5 | RESULTS sections; gate logs |
| A12 | The witness descriptor is `O_CLOEXEC`, a child process of the server holds no witness descriptor, and a mount that failed before its claim holds no lock | O-4, O-7, I-3 |
| A13 | Recording follows every claim of one process: first open, unknown renewal outcome, failed adoption, repeated self-remount | P-8 |
| A14 | `test_backup_restore_on_cluster::test_shutdown_waits_for_backup` passes on the first attempt without a lease pin | I-7 |
| A15 | The three configurations of the pin table lose their pin, or keep it with a named reason | W7 |
| A16 | Every code commit passes `CAS*`; the final tree passes the gates of the pull request (ASan `CAS*`, the no-S3 compile, the listed integration modules) | gate logs |
| A17 | CAS-480: the hard-restart regression jobs pass without a lease pin in the regression suites, if A10 finds their data directory on a listed file system | CI of the pull request |

Measurements, one report `docs/superpowers/reports/2026-11-13-cas-mount-lease-witness-measurements.md`.
Predictions, written before the runs:

| # | Prediction |
|---|---|
| M-1 | `SIGKILL` and an immediate start at the 120 s lease, S3 disk: `Ready for connections` within 21 s plus the normal start-up of that server; the log has the local-death line and no `~131000 ms` line |
| M-2 | The same with the witness file removed between kill and start: the `~131000 ms` line appears and the server is ready after about 135 s |
| M-3 | Kill, 200 s pause, start: the local-death line and a 21 s wait, not 0 and not 135 s |
| M-4 | A healthy server: the witness file is written at the claim and never during renewals; its modification time does not move while the server runs |
| M-5 | The stress lane as it runs today: the witness-open line names overlayfs and the restart after a forced exit still waits about 135 s |

A failed prediction is a finding. It is reported, not explained away.

## 11. Non-goals {#non-goals}

- **A shorter wait without local proof, however old the lease is.** After a host reboot, on another host or
  with a copied directory the server pays the full wait even when the lease ended long ago (F7). A later
  story could use the expiry recorded in the slot; that needs a clock that two processes can compare, which
  this design avoids.
- **A wait below the settle window after a long downtime.** A server down for an hour inside one boot still
  waits `S`.
- **A self-fence on the forced-exit path** (`Server.cpp:4018`). It would cover a forced exit on any file
  system and on another host, and needs a call in shared code (P2).
- **Support for overlayfs and other unlisted file systems.**
- **The in-process self-remount** (`CasPool.cpp:1574-1585`). It keeps today's wait (P5).
- **The GC leader lease.** A restarted server may be up for a while before it leads GC again.
- **Parallel waits over several disks, and listening before the disks are mounted** (CAS-355 option b).
- **A shorter wait for a foreign observer, a shorter lease, any on-store format change.**
- **Rounding of the full wait to the poll step** (135 s for a 131 s threshold) stays.
- **Defence against a live memory clone or a shared `(uuid, epoch)` pair** (P4).
- **Changes in `Altinity/clickhouse-regression` and in the stress job.**
- **A new column in `system.cas_mounts`, a new metric.**

## 12. Risks {#risks}

| Risk | Bound or mitigation |
|---|---|
| A listed file system does not exclude with `flock`, or reuses an inode identity | three file systems only; gate G2 with real processes on each; the reclaim stays conditional; a renewing holder is still caught by the token (D-8); the setting turns the witness off |
| A memory clone, or a second process with the same `(uuid, epoch)`, is stalled while the restart gets the settle window | stated residual (P4); already unsupported |
| The stress lane keeps its data directory on overlayfs and stays red | stated in [§1](#decision); question 6; M-5 confirms it |
| A regression or integration runner keeps the data directory on an unlisted file system | I-0 and A10 show it before the design is said to fix the lane |
| The first unclean stop after an upgrade, a GC fence or an unsafe start still waits 135 s | by design: the price of the eligibility rule; documented |
| The settle window is one write reserve plus the poll step, not one write reserve | 21 s and 17 s both fit the 60 s budgets; the owner may choose less ([§13](#open-questions)) |
| The witness file is created by another OS user (a tool run as root) and the server cannot open it | D1: full wait with a `Warning` that names the error |
| A test relied on the 131 s wait as a delay | I-4 keeps one full-wait phase; a search of `tests/` for the observation text found one test |
| A faster restart exposes tests that assume the restarted server leads GC at once | `test_cas_early_commit_restart` already has that race on the base (`triage.md` 3.1); it needs its own fix |
| `MountPriorState` gains a value; a switch outside `CasPool.cpp:899-910` misses it | `-Wswitch` fails the build; a search for the enum before W4 |
| Descriptors that wrote a record stay open for the life of the process | at most 1024; a mount that failed before its claim holds none |
| A later change breaks rule 2, 3 or 5, and a record certifies a live process | L-2, L-9, E-4, E-6, E-11, P-5; the sabotage rows of [§8.2](#model-identity) |
| A failed removal of a listing fails a claim that would succeed today | needs a lost conditional write and a local I/O error in one start; the start is retried |

## 13. Open questions for the owner {#open-questions}

1. **Settle window: one write reserve plus the poll step (21 s for an S3 disk, this design), one write
   reserve (16 s, the memo), or 0?** With one write reserve a conditional write of the dead process can be
   4.8 s younger at the reclaim than its admission can be today. Nothing in the code was found to depend on
   that; the per-kind protections of [§3.6](#late-landing) were verified by request shape only.
2. **Self-fence before the forced exit in `programs/server/Server.cpp`: yes or no (P2)?** This design: no.
   It is the only option here that fixes a forced exit on overlayfs.
3. **Witness on by default with `cas_mount_lease_witness` as the off switch (P3): confirm, and confirm the
   name.**
4. **Is the residual acceptable (P4): a live memory clone, or two live processes with one
   `(uuid, writer_epoch)`?** "No" means the settle window cannot be used where such copies can happen, and
   needs a platform signal that is not identified.
5. **Self-remount in a separate task (P5): confirm.**
6. **The stress lane.** Its data directory is on the container's overlayfs layer, which is not supported,
   so the lane stays at 135 s against a 110 s budget. Choose: give the stress job a volume for
   `/var/lib/clickhouse` (a CI change outside this story), take question 2, or raise the lane's budget.
7. **One record line per claim instead of one per slot write.** With no clock in the record, the epoch
   names the writer, so renewals write nothing locally. The price: when two live processes share one
   `(uuid, epoch)` and the one whose body is current stalls, a per-renewal record would still select the
   full wait; this design gives the settle window. That case is inside question 4. Confirm, or ask for the
   per-renewal record.

## 14. Corrections to the memo {#memo-corrections}

| Memo | Code or analysis | Effect |
|---|---|---|
| The restart waits 131 s and start-up takes about 5 s | The loop sleeps whole polls: the reclaim is at 135 s (27 × 5 s), and at 40 s for the 30 s lease. Start to ready is 135.6 s and 41.3 s in the CI logs, so start-up is about 1 s (`CasServerRoot.cpp:1298`) | numbers of [§1](#decision); the shortened sleep of [§3.5](#arithmetic) |
| A request of the old holder is at least 13.8 s old at the reclaim, and one write reserve restores that | 13.8 s is the minimum of a one-envelope admission. A conditional write reserves two envelopes: 20.8 s (`CasRequests.cpp:958-966`) | `S` = write reserve + poll step |
| The settle window is 16 s | It is a formula over the disk's budget: 21 s for S3 defaults, 17 s for a local object storage (F21) | [§3.5](#arithmetic) |
| Lease arithmetic on the shared boot-time clock: count the threshold from the holder's recorded renewal start | Removed. It needs two processes to read one clock. `/proc/self/timens_offsets` shows the offsets of the namespace for children, not of the caller's own (`kernel/time/namespace.c:365-378`), so equal offsets do not prove one clock | no time in the record; [§11](#non-goals) |
| The settle window counts from the lock | It counts from the first sighting of the token, the sample the loop already takes (`CasServerRoot.cpp:1286`). Later, so safe; it reuses the loop | [§3.5](#arithmetic) |
| The record names each slot body by `(writer_epoch, write_attempt_id)`, written before every slot write | Without a time per body the epoch is enough (F11 to F13). The record is written at a claim only | [§3.2](#local-record); question 7 |
| Every writable mount writes the record | Only a process that holds the lock, and only for an eligible claim. Otherwise a record can certify a process that never held the lock, or a lineage with a live bypassed holder | rules 2 and 5 |
| The descriptor is never closed | Never closed once it wrote a record; closed with a mount attempt that wrote none. A cap bounds the first kind | [§3.1](#witness-file) |
| overlayfs can be listed after an experiment | Its reported inode identity need not identify the lock | [§4.1](#file-systems) |
| All three CI cases fit their budgets | The stress lane keeps its data directory on overlayfs | [§1](#decision) |
| Model: check `GlobalSupersededWriterMakesNoMutation` in the twin extension | The twin configurations do not list that invariant; they check `OneWritableIncarnationPerUuid` (`M/CaCasMountCore_twins.cfg`) | [§8.2](#model-identity) |
| Forced exit: connections still open | The same block exits also for unfinished refresh tasks or background queries (`Server.cpp:4006`). Under TSan `safeExit` is `SYS_exit_group`, not `_exit` (`safeExit.cpp:18`) | fact table |
| `DisksApp.cpp:605-609`; whether the tool mounts writable is open | The uuid load is at `:602-610`; the tool creates disks through the ordinary factory (`DisksClient.cpp:192`) | F16 |
| Integration: a clone with a rewritten boot id | Not buildable in the harness; covered at unit level. The memo also did not list the existing test that asserts the observation line (`test_cas_mount_renewal_retry/test.py:620-681`) | [§7.7](#tests-integration) |

## 15. Not verified {#not-verified}

- Nothing was built or run. Every "after" number is computed.
- `flock` exclusion and inode identity on ext4 and xfs by experiment: gate G2. Only tmpfs was tested, by
  the memo, on one kernel. `ext4_getattr` was not read.
- That the kernel releases the lock only after every thread of a killed process has stopped; that a device
  number is not reused while a file on the old device is open; that ext4 and xfs keep the inode number of
  an unlinked open file.
- The file system of the data directory in the three CI lanes: A10. The runner evidence (`overlay2` over
  `extfs`, root on `/dev/sda1`) is from one integration job.
- Whether `clickhouse local` mounts a CAS disk writable; what `clickhouse disks` does without a `uuid` file.
- Whether `Context::shutdown` reaches disks defined in table metadata.
- The recovery walk and the epoch seal, beyond the comment at `CasPool.cpp:880-897` and the user docs. The
  design does not rest on it ([§3.6](#late-landing)).
- Which files are namespace files, and what a successor caches of them.
- How long a kernel delivers the data of a closed socket; how long a store may queue a request.
- Boot id and lock behaviour of sandboxed kernels; `st_dev` after a volume is detached and attached again.
- Whether any component assumes that a lease elapses between two epochs of one server.
- Whether a mount event of the first open reaches `system.cas_log`.
- How long a restarted server needs to lead GC again.
- Whether `CA/` builds on a platform without `CLOCK_BOOTTIME`.
- Whether the integration node image has `flock(1)`.
- `test_cas_gcs`, `test_cas_replicated_relink`.
- Per-commit builds, the TLC results and the measured start-up bounds: acceptance evidence that does not
  exist yet (A11, A16, M-1 to M-5).

**What this design does not cover.** It is about one mount slot and the process that last wrote it, on one
host, one boot and one of three file systems. It says nothing about GC leadership or about a foreign
observer.

## 16. Left to the builder {#left-to-builder}

- **Parameter shape.** Whether `claimMount` and `claimMountAwaitingExpiry` take the witness, the authorized
  token and the own write reserve as separate trailing parameters or as one small struct. Constraint:
  existing call sites compile unchanged.
- **How a handle that wrote keeps its descriptor open** after the handle object is gone: a process-wide
  list or a deliberate leak. Constraint: no lookup by path, and the count of [§3.1](#witness-file).
- **Checksum function,** if `CityHash64` is awkward at that layer: any 64-bit hash already linked into
  `dbms`. The 16-hex-digit line stays.
- **`EINTR` and short writes** of the single `pwrite`: a short write is a failed record write.
- **Test names** may change for clarity. Suites keep the `CAS` prefix. The fixture and the red-before
  evidence of each row are binding.
- **I-2 mechanics.** How the test holds a connection open across `SIGTERM`; the old server must log
  `Will shutdown forcefully`.
- **How a pool test frees the lock of a "killed" pool** without a test branch in production code: for
  example a handle owned by the test and released by it.
- **Model encoding.** Names of constants and variables, and whether the third incarnation is a new constant
  or a generalisation of `TwinUuids`. The rows, their expectations and the invariants are binding.

## 17. Review round 1 {#review-round-1}

Review text: `tmp/prbundle/wave2/review/spec-r1.out` in the `master` worktree. Each finding was checked
against the code or the kernel source before it was acted on.

| Finding | Verdict | Check | Change | Where |
|---|---|---|---|---|
| F1, blocker: `/proc/self/timens_offsets` certifies the namespace for children, not the caller's clock | Accepted | `proc_timens_show_offsets` calls `timens_for_children_get` (`kernel/time/namespace.c:365-378`; the review cites `:345-356`, the function is 20 lines lower) | The path that counted the threshold from a recorded renewal start is removed, with the start time in the record, the offset checks, its decision rows, its threat row, its tests, its model configurations and its commit. Nothing left compares a clock between processes | [§1](#decision), [§3.2](#local-record), [§3.4](#decision-table), [§3.7](#boot-id), [§8.1](#model-timing), [§11](#non-goals) |
| F2, blocker: on overlayfs equal `(st_dev, st_ino)` need not be one lock | Accepted | v4.19 `fs/overlayfs/inode.c:100-140`: an upper-layer file on a non-samefs overlay without `xino` keeps the underlying device and inode | overlayfs is off the list: ext4, xfs, tmpfs only, each after a recorded run. The three listed file systems were re-examined against `fs/locks.c:2174-2179` and `fs/stat.c:88-89`; three assumptions are stated. The inode comparison stays: it is the only check that catches a copy on the same host and boot (T6, T7) | [§3.4](#decision-table), [§4.1](#file-systems) |
| F3, major: unsafe `B` over live `A`, farewell, `C`, death of `C`: `D` certifies while `A` writes | Accepted | `CasPool.cpp:775`, `CasMountRuntime.cpp:1243`, `CasServerRoot.cpp:1087` allow each step | An eligibility rule replaces "an unsafe reclaimer records nothing": only a fresh claim at epoch 1, an observed claim, or a claim over a listed farewell or a proven death lists its epoch. `C` is not eligible. The induction is written as base, step and breakers | [§3.3](#record-writes), [§3.5](#arithmetic), T19, T20, P-5, I-6, [§8.2](#model-identity) |
| F4, major: the gate can pass with an unfinished triplet search and without process-level tests | Accepted | `M/CaCasMountCore.tla:441`: the admission check is local, so the three-incarnation history is material | The triplet must be explored to completion. A suite with real second processes covers lock aliases, `exec` inheritance, a child without `exec`, a killed holder and failed mounts, and runs on each listed file system. Three gates precede the enabling commit | [§7.6](#tests-os), [§8.3](#model-acceptance), [§9](#commits) |
| F5, major: the never-destroyed registry has no bound for failed dynamic disks | Accepted, with a simpler ownership than the review's cap alone | `DiskFromAST.cpp:88-100`, `Context.cpp:7606-7620` | No registry. The mount attempt owns the descriptor and closes it unless a record was written through it. Descriptors that wrote are bounded at 1024 | [§3.1](#witness-file), L-12 to L-14, O-7, O-8 |
| F6, minor: 21 s is not the value for the stress disks | Accepted | `05387_cas_publish_counts_replicated.sh:12-13`, `ContentAddressedMetadataStorage.cpp:705-720`, `CasRequestBudget.cpp:21-30` | `S` is stated as a formula with 21 s (S3) and 17 s (local); the CI table uses each lane's backend | [§1](#decision), [§3.5](#arithmetic), D-10, I-1 |

Unverified items of the review:

| Item | Resolution |
|---|---|
| `f_type` inside the lane containers | Acceptance A10 with the method per lane; gate G3; test I-0. Read now: integration binds a host directory (`cluster.py:5067`); the stress job mounts no volume at `/var/lib/clickhouse` (`stress_job.py:205-210`); the runner's Docker driver is `overlay2` over `extfs` (`E/job_131775013725.log:1086-1087`). Consequence: the stress lane is not fixed |
| ext4 and xfs experiments; overlayfs restrictions; `st_dev` after re-attach | Gate G2 for ext4 and xfs. overlayfs is excluded. A changed `st_dev` is D7: full wait |
| The late-write reduction; dispatch age is not admission age | Reworded: today's figures bound admission, and the wire can only be younger; after a death the bound holds at the wire, on every plane ([§3.5](#arithmetic)). The per-kind table is informative; the design rests on the reduction, held by D-1 and `RequestAgeAtLeast` |
| Wiring through an unknown renewal outcome, a failed adoption and repeated self-remounts | Settled by design: the record is written at a claim only. The wiring table of [§3.3](#record-writes) names every call; test P-8; acceptance A13 |
| A stalled local `pwrite` and renewal availability | Settled by design: renewals write nothing locally. A stalled write delays a claim, not a renewal |
| Per-commit builds, TLC results, measured bounds | Acceptance A11 and A16, measurements M-1 to M-5 |

Settled by reading in this round: the watchdog is forked before disks start (F19); `clickhouse disks`
mounts through the ordinary factory (F16); child processes inherit descriptors without `FD_CLOEXEC` (F18).

One change goes beyond the findings: the record lists epochs, not slot bodies, and is written per claim
(question 7). It follows from F1: with no time per body, nothing needs the body's own identity.
