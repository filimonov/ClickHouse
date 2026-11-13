---
description: 'Design for a per-disk local mount witness of the CAS disk: an abstract unix socket named after the mount slot, held for the life of the process, and a small record of writer epochs in the server data directory let a restart in the same boot and network namespace prove that the previous server process is dead, so the restart waits a settle window (21 s for an S3 disk with default settings) instead of the 131 s mount-lease observation threshold after SIGKILL, an OOM kill, an abort or a forced shutdown. No on-store format change, no change to shared server code, no clock compared between processes, no dependence on the file system, the default lease stays 120 s.'
sidebar_label: 'CAS: mount lease witness'
sidebar_position: 114
slug: /superpowers/specs/cas-mount-lease-witness-design
title: 'CAS: restart after a kill without waiting out the own mount lease'
doc_type: 'reference'
---

# CAS: restart after a kill without waiting out the own mount lease — design {#cas-mount-lease-witness}

- Date: 2026-11-13. Status: draft, revised after review rounds 1 and 2 and after a comparison of three
  death proofs ([§17](#review-round-1), [§18](#review-round-2), [§19](#proof-change)). Not approved.
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

- **The proof of death is an abstract unix socket.** A writable CAS mount binds a socket whose name is
  derived from the identity of its mount slot, and never closes it. The kernel frees the name when the
  process dies. A later process that binds the same name, in the same network namespace of the same boot,
  knows that no live process holds it.
- **A record file says which epoch that proves dead.** When a mount claim carries proof, the mount writes
  its `writer_epoch` into `cas_mount_witness` in the data directory before it sends the claim.
- **A restart gets a certificate of death only when every one of these holds:** it bound the name; the
  file's header equals its own in the slot identity (pool, object storage, slot key, server uuid), the boot
  id and the network namespace; the file is not blocked; the file lists the epoch of the slot body. It then
  reclaims the slot after a settle window with the slot token unchanged, instead of the 131 s threshold.
- **Every other case waits exactly as today, and logs why.** Missing, unreadable, foreign or inconsistent
  local evidence never produces a shorter wait and never produces an error.
- **Why the socket, of three candidates** ([§4.2](#alternatives)): it is the smallest mechanism that is
  safe. It needs no file-system semantics, so no allowlist and no inode comparison, and it works on
  overlayfs and tmpfs. A copy of the data directory cannot fabricate it. It covers a restart of the process
  and a restart of a container that keeps its network namespace.
  - Rejected: an exclusive `flock` on the witness file. It needs a file-system allowlist and an inode
    comparison, and does not cover overlayfs. It would add a pod replaced on the same node with the same
    volume.
  - Rejected: a recorded process identity checked through `/proc`. It covers only a restart inside one pid
    namespace and needs three more conjuncts to be safe.
- **The mechanism compares no clock between processes.** The record holds no time.
- **Nothing on the store changes.** No new field in any stored object. The reclaim stays a write conditional
  on the slot token. A foreign observer (another server, the GC leader) waits as today.
- **The code needs no elapsed lease after a reclaim.** It needs a higher epoch and a previous holder that
  starts no new write; a dead holder starts none ([§3.6](#late-landing)).
- **No change to code shared with non-CAS ClickHouse.** All code is under `CA/`.
- **One new disk setting**, `cas_mount_lease_witness`, default `1`. With `0` the server behaves as today.
- **A start under `cas_unsafe_remount_no_delay` blocks the witness of that disk** until an operator deletes
  the witness file.

The record lets a starting server say which situation it is in. The classification proves nothing; it
selects whether the proof is attempted and what is logged ([§3.7](#classification)):

| Case | Record against the starting process | Meaning | Outcome |
|---|---|---|---|
| (a) | same boot, same network namespace, same pid namespace | the process was restarted | proof attempted; settle window |
| (b) | same boot, same network namespace, another pid namespace | a container was restarted inside its pod, or the network namespace is shared (host network) | proof attempted; settle window |
| (c) | same boot, another network namespace | another pod or container on the same node | full wait |
| (d) | another boot | the node rebooted, or this is another node | full wait |
| (e) | no usable record | first start, upgrade, lost or blocked file | full wait |

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
| `SIGKILL`, OOM kill, abort, second `SIGTERM`, `SYSTEM KILL`, forced exit; restart in the same boot and network namespace: cases (a) and (b) | 135 s | `S`: 21 s (S3), 17 s (local) |
| The same, however long the server was down inside that boot | 135 s | `S` |
| The first unclean stop after: an upgrade from a build without the witness, a host reboot, a GC fence of the slot, a decommission and rejoin, a change of the disk's pool or endpoint | 135 s | 135 s once; the next one waits `S` |
| Any unclean stop after a start under `cas_unsafe_remount_no_delay` | 135 s (0 s while the knob is on) | the same, until an operator deletes the witness file |
| Another network namespace on the same node: case (c) | 135 s | 135 s |
| Host rebooted; start on another host; data directory copied or restored to another host: case (d) | 135 s | 135 s |
| Previous process ran a build without the witness, or with the setting off | 135 s | 135 s |
| Another live process holds the name | 135 s, then refusal if that process renews | the same |
| Slot fenced by a GC leader before the restart | 0 s | 0 s |

- 135 s is the 131 s threshold rounded up to the 5 s poll of the wait loop ([§2.2](#code-facts), F6).
- A server with N stale CAS disks from the configuration pays N waits, one after another, as today.
- The file system of the data directory does not matter.

The three CI cases of [§2.1](#ci-evidence). Each kills and starts the server inside one container, so each
is case (a):

| Lane | Disk backend | `S` | Data directory | Before | After |
|---|---|---|---|---|---|
| Regression hard restart | S3 endpoint (`E/hard-restart.md` section 4) | 21 s | bind mount of the runner's checkout; irrelevant | 135 s against a 60 s budget | 21 s |
| `test_backup_restore_on_cluster` | S3 (`tests/integration/test_backup_restore_on_cluster/configs/cas_storage.xml:6-8`) | 21 s | bind mount of a host directory; irrelevant | 135 s against a 60 s budget | 21 s |
| Stress `arm_release` | local object storage (`tests/queries/0_stateless/05387_cas_publish_counts_replicated.sh:12-13`) | 17 s | the container's overlayfs layer (`ci/jobs/stress_job.py:205-210`); irrelevant | 135 s against a 110 s budget, two disks in parallel | 17 s |

What does not change:

- The default `cas_mount_lease_ttl_ms` (120000) and how long a holder may write.
- The threshold of a foreign observer: `L + L/20 + step`.
- What `cas_unsafe_remount_no_delay` does to the wait, the farewell, the GC fence and every other existing
  reclaim path.
- The GC leader lease in `gc/state`. A restarted leader takes leadership back by the existing protocol.

### 1.1 Provisional decisions {#provisional}

Taken so that work can proceed. **Provisional, owner may overrule.**

| # | Decision | Status in this design |
|---|---|---|
| P1 | After a proven local death the server still waits with the slot token unchanged; the wait is not 0 | Kept. The window is the formula `S` above: **one write reserve plus the poll step**, 21 s for an S3 disk with defaults, not the memo's 16 s ([§3.5](#arithmetic)) |
| P2 | No self-fence on the forced-exit path of `programs/server/Server.cpp` | Kept. Not in this story. No CI lane needs it any more |
| P3 | The witness is on by default; a per-disk setting turns it off | Kept. The setting is `cas_mount_lease_witness` |
| P4 | A live memory clone of a running holder (VM snapshot with memory, process checkpoint) is a documented residual, not defended | Kept ([§4](#threat-model), T3). The same residual covers two live processes that share one `(uuid, writer_epoch)`, and a store rolled back in place |
| P5 | The in-process self-remount after a lost lease is out of scope | Kept. The self-remount still records its claim, so a later restart can use it |

### 1.2 Failure asymmetry {#asymmetry}

- A wrong short wait puts two writers under one `server_root_id`. Recovery from that is manual.
- A wrong full wait costs about 114 s of start-up.
- So each check that fails selects today's path, and every condition added in review is one more conjunct
  of the certificate. Two statements are dangerous when false: "no live process in this network namespace
  holds the name of this slot", and "while a listed epoch holds this slot, no other process can write".
  [§4.1](#kernel-facts) gives the kernel facts behind the first. The rules of [§3.3](#record-writes)
  establish the second.

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
| F1 | A writable open validates the pool meta, then runs owner claim, epoch allocation and the mount claim, on the caller's thread | `CA/Pool/CasPool.cpp:568-577`, `:596`, `:627`, `:689`, `:785-788` |
| F2 | A slot with the claimer's uuid, an older epoch, not fenced, not farewelled is `LiveDoubleStart`: no write | `CA/Pool/CasServerRoot.cpp:1085-1087`, `:1113-1119` |
| F3 | The wait loop reclaims only after the slot token stayed unchanged for the threshold on the claimer's own clock | `CasServerRoot.cpp:1220-1300`; the test at `:1243` |
| F4 | Threshold = `L + L/20 + poll`, `L = max(own lease, lease the body declares)`, `poll = max(1, period / 2)`. Defaults: 131000 ms | `CasServerRoot.cpp:1202-1218`, `CasPool.cpp:695-702` |
| F5 | The clock is `CLOCK_BOOTTIME` of the claimer. The observation starts at a sample taken after the first read | `CasPool.cpp:787`, `CA/Pool/CasMountRuntime.cpp:78-88`, `CasServerRoot.cpp:1286` |
| F6 | The loop sleeps a whole `poll` between looks, so the reclaim happens at the first multiple of `poll` at or above the threshold: 135 s | `CasServerRoot.cpp:1298`; 135.6 s and 41.3 s in the CI logs |
| F7 | The stamped `expires_at_ms` never enters the decision. A lease that ended hours ago costs the full threshold | `CasServerRoot.cpp:1065-1084`; test `CASMountAwaitExpiry.PastExpiryStillPaysTheFullObservationThreshold`, `T/gtest_cas_mount.cpp:1326` |
| F8 | A changed token restarts the observation; more than 3 restarts refuse the start | `CasServerRoot.cpp:1279-1284`, `CA/Pool/CasServerRoot.h:386`, `CasPool.cpp:844` |
| F9 | `gc_fenced` or a farewell in the slot is reclaimed at once. So is any body under `cas_unsafe_remount_no_delay` | `CasServerRoot.cpp:1087-1100`, `CasPool.cpp:763-777` |
| F10 | After the reclaim nothing waits and nothing branches on how the slot was won: the prior state selects one log line | `CasPool.cpp:846`, `:898-920`; every use of `MountPriorState` in `CA/` is in `CasServerRoot.cpp:1097-1110` and `CasPool.cpp:749-917` |
| F11 | A `writer_epoch` belongs to one incarnation **of one slot**. It is allocated by a conditional bump of the `epoch` object, which lives next to the slot under the pool prefix, with a random nonce. Epoch 1 is handed out only when the `epoch` object is absent, the data subtree is empty and the mount object is authoritatively absent | `CasServerRoot.cpp:846-890`; `CA/Formats/CasLayout.h:433-453`; `D/architecture/mounts-and-leases.md:233-240` |
| F12 | A claim never installs an epoch at or below the one in the slot | `CasServerRoot.cpp:1037-1046` |
| F13 | Every slot write of a holder carries its own `(server_uuid, writer_epoch)`: claim, adoption, renewal, farewell. A GC fence copies the body and sets `gc_fenced`. A decommission admin writes under an epoch it allocated itself | `CasServerRoot.cpp:905-917`, `:1689-1707`, `:2174-2184`, `:1403`; `CasPool.cpp:1074` |
| F14 | Nothing is persisted on local disk at mount or at renewal. The only local files of a CAS disk are write-buffer spill files in the scratch directory | search of `CA/` for local file writes; `CA/ContentAddressedTransaction.cpp:1019-1023`, `src/Disks/DiskObjectStorage/MetadataStorages/MetadataStorageFactory.cpp:232-239` |
| F15 | The server takes `flock(LOCK_EX \| LOCK_NB)` on `<path>/status` before disks start, and loads `<path>/uuid` | `programs/server/Server.cpp:2033`, `:2035`; `src/Common/StatusFile.cpp:65`, `:72-76` |
| F16 | `clickhouse disks` loads the same `uuid` file, takes no `status` lock, and creates its disks through the ordinary factory, so a disk without `<readonly>` is mounted writable | `programs/disks/DisksApp.cpp:602-610`, `programs/disks/DisksClient.cpp:192`, `CA/ContentAddressedMetadataStorage.cpp:864` |
| F17 | A read-only disk never runs the mount protocol | `CasPool.cpp:595-596`, `ContentAddressedMetadataStorage.cpp:864` |
| F18 | The server starts child processes by `vfork` and `execv` and closes no other descriptor in the child. A descriptor without the close-on-exec flag reaches every executable dictionary, executable function and bridge | `src/Common/ShellCommand.cpp:244-297` |
| F19 | The watchdog parent is forked during daemon initialization, before any disk starts, so the process that mounts is the child. The watchdog is on by default when no terminal is attached; the container entrypoint turns it off and `exec`s the server, which is then the container's main process | `src/Daemon/BaseDaemon.cpp:407-408`, `:627`; `programs/server/Server.cpp:573-587`; `docker/server/entrypoint.sh:249-250`, `:266` |
| F20 | The name of a disk defined inline in a query is a hash of its configuration. A failed disk creation is not added to the disk selector | `src/Disks/DiskFromAST.cpp:88-100`, `src/Interpreters/Context.cpp:7606-7620` |
| F21 | The attempt envelope has no connect part when the object storage has no S3 client | `ContentAddressedMetadataStorage.cpp:705-720`, `CA/Backend/CasRequestBudget.cpp:21-30` |
| F22 | The slot is the object `<pool prefix>/gc/server-roots/<server_root_id>/mount` on the disk's object storage. The pool prefix is configuration. `pool_id` is a random id in the pool's meta object, stable for the pool's life | `CasLayout.h:433-462`; `ContentAddressedMetadataStorage.cpp:755-785`; `CA/Formats/CasPoolMetaFormat.h:18-27` |
| F23 | A configured mount lease above 600000 ms is clamped to that value, so no holder has a longer lease | `CA/ContentAddressedSettings.cpp:28-33`; `D/configuration.md:100` |

Shutdown paths and the lease. This design changes none of them:

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

Six rules carry the safety argument. Tests and the model refer to them by number.

1. **A name that vouches is never released.** A socket whose mount wrote the file stays open until the
   process ends. A socket whose mount wrote nothing is closed with its mount attempt.
2. **Only the name holder writes.** A mount writes the file only while it holds the bound socket of its
   slot, and lists only its own `writer_epoch`.
3. **Evidence is inherited.** A process uses, as proof of death, only what the file held when it bound the
   name. What it writes later is never evidence for itself.
4. **One header rule.** An inherited record counts only if its header equals this mount's header in every
   field: slot identity, boot id, network namespace. Otherwise it is treated as absent. The same rule
   serves the death evidence, the carry-over and the eligibility of a claim over a farewell.
5. **Only an eligible claim lists its epoch.** A claim lists its epoch only if the claim itself carries
   proof that no other process can still write ([§3.3](#record-writes)).
6. **An unsafe claim blocks the file.** A claim under `cas_unsafe_remount_no_delay` replaces the record with
   a durable block. A blocked file gives no evidence and accepts no listing until an operator deletes it.

### 3.1 The socket {#witness-socket}

- **Name.** An abstract unix socket address: a zero byte, then `clickhouse-cas-mount/`, then 32 hex digits
  of a 128-bit hash of the slot identity of [§3.2](#local-record): `pool_id`, `store`, `slot_key`,
  `server_uuid`. 54 bytes of the 108 a unix address may have. The name is logged at mount.
- **Who binds it.** `Pool::mountWritable`, once per mount attempt, after the pool meta is validated and
  before the owner claim. `ContentAddressedMetadataStorage::openPoolView` puts a factory into a new
  `PoolConfig` field for a mount that is writable, has a `Context` and has the setting on. The pool calls
  the factory with the slot identity. The handle is owned by the mount attempt.
- **How.** `socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0)`, then `bind`. One attempt. The socket never
  listens and never connects: a connection attempt from outside is refused and uses no memory.
- **`SOCK_CLOEXEC` is required, not optional.** Child processes inherit every descriptor without it (F18).
  With it, a child holds the socket only between `vfork` and `execv`. If the server dies in that window
  the name stays bound until the `execv`. That direction is safe: a bound name selects the full wait.
- **Scope.** The kernel keeps abstract names per network namespace ([§4.1](#kernel-facts)). So the bind
  says something only about processes of this network namespace, and the record names that namespace.
- **Identity read at the bind.** The boot id from `/proc/sys/kernel/random/boot_id`. The network namespace
  and the pid namespace as the inode numbers of `/proc/self/ns/net` and `/proc/self/ns/pid`, read with
  `stat`, which follows the link. `lstat` returns the number of the link itself, which is useless. If the
  boot id or the network namespace cannot be read, the witness is not used.
- **After a successful bind** the mount reads the record file once. This is the inherited record.

**Ownership of the socket (rule 1).**

- The handle belongs to the mount attempt. When the attempt ends without a write of the file through this
  handle, the handle closes the socket and the name is free. Why this is safe: a certificate of death
  names an epoch listed in the file. This process listed none, so no later reader can conclude anything
  about it from this name.
- From the moment the handle writes the file, whatever the result, the socket is never closed: not at pool
  teardown, not after a farewell, not after a failed start-up. The kernel closes it when the process ends.
  Why: a listed epoch may still have requests in flight in this process after its pool is gone.
- **No process-wide table.** A later mount of the same slot in the same process binds again. If an earlier
  handle of this process still holds the name, the new handle gets `EADDRINUSE` and is unwitnessed. That
  costs one full wait after the next unclean stop and needs no shared state.
- **Bound.** One process may have at most 1024 sockets whose mounts wrote. A slot of the bound is reserved
  atomically when a handle is created and returned when the handle closes without a write; it is never
  returned after a write. At the bound, a new mount creates no socket and is unwitnessed, with one
  `Warning`. Failed disk creations with changing names (F20) therefore cannot exhaust descriptors.

**The name is taken (`EADDRINUSE`).** Something alive in this network namespace holds it: a second writable
mount of the same slot, a stopped server, an earlier mount of this process, or an unrelated process that
bound the name. Abstract names have no owner or permission, so any process of the namespace can do that.

- **Decision: full wait, not an error.** The mount continues exactly as today, with a `Warning`. A process
  that squats on the name can only cost the short wait; it cannot fake a death.
- The mount is **unwitnessed**: it reads no record and writes none (rule 2).
- Why not an error: today this case already ends correctly through the store. A renewing holder restarts
  the observation and the start is refused (F8). A holder whose renewals stopped loses the slot after the
  full threshold, which is the lease contract. An error would refuse mounts that are safe today.
- Why the unwitnessed mount must not write: its epoch would later be read by a process that bound the name
  after the *other* holder died, and would certify the death of a process that never held the name.

| Mount kind | Witness |
|---|---|
| Server, writable CAS disk | socket + record |
| Read-only CAS disk (`<readonly>`) | none |
| `clickhouse disks`, writable mount with the server's `path` (F16), in the server's network namespace | the same name. With a live server the name is taken: unwitnessed, full wait. With a dead server the tool binds and may use the record |
| The same tool from another network namespace | binds there; the record's header names another namespace: full wait |
| `clickhouse local` | a file under the `path` it runs with; a fresh temporary `path` never inherits a record |
| `SYSTEM CAS DROP POOL MEMBER` (`Pool::openForDecommission`, `CasPool.cpp:983-1076`) | none: the admin pool clears the witness field. It writes another member's slot and never waits |
| A `Pool` opened without the field (unit tests) | none |

Whether `clickhouse local` mounts a CAS disk writable: not verified.

### 3.2 Local record {#local-record}

The record is the file `<server path>/disks/<disk name>/cas_mount_witness`. `<server path>` is
`Context::getPath`, `<disk name>` the name the metadata storage was created with
(`ContentAddressedMetadataStorage.cpp:290`). It is local state, not a store format: it has no `FormatId`
and no compatibility promise beyond the rules here. The file is not locked: rule 2 makes the name holder
its only writer inside one network namespace.

- **Size.** Exactly 4096 bytes: text lines, then spaces, then one line feed.
- **Encoding.**

```text
cas_mount_witness 1
pool_id <32 lowercase hex digits>
store <description of the object storage, to the end of the line>
slot_key <the mount object's key, to the end of the line>
server_uuid <32 lowercase hex digits>
boot_id <36 characters of /proc/sys/kernel/random/boot_id>
net_ns <decimal inode number of /proc/self/ns/net>
epoch <writer_epoch> <write_reserve_ms> <pid> <pid_ns>
epoch ...
checksum <16 lowercase hex digits>
```

A blocked file has one line `blocked unsafe-reclaim <writer_epoch>` in place of the `epoch` lines.

| Field | Use |
|---|---|
| version (`1`) | An unknown version makes the record unreadable |
| `pool_id`, `store`, `slot_key`, `server_uuid` | **Slot identity**: which mount object, of which pool, on which object storage, for which server, the epochs belong to. An epoch means something only inside one slot (F11). The socket name is a hash of the same four |
| `boot_id` | Names and namespaces exist only inside one boot. It also separates hosts: the first network namespace has the same number on every machine |
| `net_ns` | The namespace in which the writer held the name |
| `epoch` | One `writer_epoch` whose claim was eligible, with the writer's write reserve (for the settle window), and its pid and pid namespace (for the log only). At most 2 lines: the epoch of the body the last claim replaced, if rule 4 allows it, and the epoch of that claim |
| `blocked` | Rule 6 |
| `checksum` | `CityHash64` of every byte before the `checksum` line |

**Slot identity, every component.** The slot is one object (F22). Its location has three parts, and the
header binds all of them plus the server:

| Component | Header field | Source |
|---|---|---|
| Which pool | `pool_id` | the validated pool meta (`CasPoolMetaFormat.h:18-27`) |
| Which object storage: endpoint and bucket, or the local root | `store` | `IObjectStorage::getDescription` and `getObjectsNamespace` |
| Which key: pool prefix and `server_root_id` | `slot_key` | `Layout::mountKey` (`CasLayout.h:459`) |
| Which server | `server_uuid` | the pool's server id |

One disk name can point at different slots over time: its prefix, endpoint, bucket, pool or
`cas_server_root_id` can change in the configuration. Each such change alters a header field and the
socket name.

- **The record holds no time and no clock value.**
- **Why an epoch is enough.** Inside one slot, one incarnation owns one epoch (F11), every body of a holder
  carries its epoch (F13), and the slot never goes back to a lower epoch (F12). So "the body of this slot
  has epoch `e`" and "`e` was listed for this slot by a process that held this name" together name the
  process that wrote the body.
- **Two lines.** After the record is written and before the claim lands, the slot still holds the replaced
  body. The record keeps that epoch, so a process killed in between is still matched. A restart loop of any
  length keeps both lines.
- **Write.** One `pwrite` of the whole 4096-byte block at offset 0. No rename, no truncate.
- **No `fsync` for epochs.** A record is used only inside the boot that wrote it (rule 4), and the page
  cache survives the death of the process. A kernel crash or power loss changes `boot_id`.
- **`fsync` for a block.** A block must outlive a reboot ([§3.3](#record-writes)).
- **The directory's file system does not matter.** The file is only read and written.

| File content | Reading |
|---|---|
| Missing or empty | `absent` |
| Not exactly 4096 bytes, no checksum line, checksum mismatch, a line that does not parse | `unreadable`, with the detail |
| First line names another version | `unreadable: version N` |
| Valid, with a `blocked` line | `blocked`, whatever the header says |
| Valid, header differs from this mount's header in any field | treated as `absent`; the differing field is logged |
| Valid, header equal | `present` |

- A torn write shows as a checksum mismatch: `unreadable`. The checksum detects damage with high
  probability; it does not make the write atomic.
- An absent, unreadable or foreign record is replaced only by the record of an eligible claim. A blocked
  file is never replaced.
- A `slot_key` or `store` with a line feed, or a record that does not fit 4096 bytes, makes the witness
  unavailable for that mount.

### 3.3 When the file is written {#record-writes}

The file is written **only by `claimMount`, before the claim's write is handed to the request engine**:

| Site | Insert between |
|---|---|
| Fresh claim | `CasServerRoot.cpp:1004` and `:1006` |
| Reclaim | `:1089` and `:1091`; the prior state of `:1097-1100` is computed first |

Renewals, the adoption by the renewer, the farewell and the fences never touch the witness. The body keeps
its epoch through all of them.

**Eligibility (rule 5).** `claimMount` tells the witness what it is about to write: its epoch, the prior
state, the epoch and uuid of the body it replaces. The witness lists the epoch only for these claims, and
only when the handle holds the name and the file is not blocked:

| Claim | Eligible when | Why |
|---|---|---|
| Fresh: the slot is absent | the claim's `writer_epoch` is 1 | Epoch 1 exists only on a slot that never had a holder (F11) |
| Prior state `UncleanObserved` | always | This claim itself watched the token for the full threshold |
| Prior state `Clean` (a farewell) | the farewell's `writer_epoch` is listed in the inherited record under an equal header (rule 4), or was listed by this handle | The farewell belongs to a lineage of this slot that already satisfied the rule, and its holder gave the slot up |
| Prior state `UncleanLocalDeath` | always: [§3.4](#decision-table) grants it only for a listed epoch under an equal header | The same, and the holder is dead |

Every other claim lists nothing:

| Claim | What the witness does | Why |
|---|---|---|
| Prior state `UncleanUnsafe` | **blocks the file** (rule 6) | No proof at all: a live process with this uuid may still write, with any lease up to the clamp (F23) |
| Prior state `Fenced` | nothing | The wait behind the fence was another server's |
| A terminal body of another `server_uuid` (rejoin under a new uuid) | nothing | Another lineage |
| `Clean` over a farewell whose epoch is not listed under an equal header | nothing | The farewell's holder is unknown to this witness: an older build, a decommission admin, a process without the name, another boot, another namespace, another slot |
| Fresh claim with an epoch above 1 | nothing | The slot vanished under a root that had holders |

- **A mount without the name never writes (rule 2)**, whatever its claim.
- **Retraction.** A claim attempt can list its epoch and then lose the conditional write. The same process
  may later claim with the same epoch and another prior state. So before an ineligible claim, the witness
  rewrites the record without this handle's epoch if the file lists it. If that rewrite fails, the claim
  attempt fails with the I/O error: a record must never list an epoch whose claim was not eligible.
- **A failed write of an eligible record does not stop the claim.** The claim proceeds as in today's build.
  The next restart most likely finds the epoch unlisted and waits the full threshold. The failure is
  logged ([§6](#observability)).
- **Carry-over.** The record of an eligible claim holds the epoch of the replaced body, then the claim's
  epoch. The replaced epoch is kept only if this handle listed it itself, or the inherited record lists it
  under an equal header (rule 4). Otherwise the record holds the claim's epoch alone.
- **A kill between the record and the claim's write** leaves the replaced body in the slot. The record
  still lists its epoch when the carry-over kept it.

**The block (rule 6).**

- **Written when:** `claimMount` is about to reclaim with prior state `UncleanUnsafe`, on a handle that
  holds the name. The block replaces the whole record and is followed by `fsync`. If the write or the
  `fsync` fails, the claim attempt fails with the I/O error.
- **Effect:** the file gives no evidence, and no claim lists an epoch, in this process and in every later
  one, whatever the header, the boot or the prior state. The server waits as it does today.
- **Cleared only by an operator:** delete the witness file while the server is stopped. The user docs name
  the condition: no other process with this server's uuid can be running, or at least 10 minutes have
  passed since the last start under the knob.
- **Why deletion is safe.** After it the file lists nothing. The next listing needs an eligible claim, and
  with a slot that has holders that is an `UncleanObserved` claim: a full threshold. A holder that the
  unsafe start bypassed cannot renew after that start, so its lease ends at most one lease later, and no
  lease exceeds 600 s (F23). The 10 minutes cover it.
- **Why there is no automatic restoration.** One full observation covers only the lease that the observed
  body declares and the observer's own (F4). A bypassed holder with a longer lease would survive it. The
  knob is an emergency and test knob; the design does not try to repair its history.
- **What the block cannot see.** An unsafe start by a build without the witness, with the setting off, or
  without the name leaves no block. For those the knob's own contract stands: it is allowed only where one
  process per uuid is guaranteed (`D/configuration.md:114`). The docs of the knob get this sentence.
- **No other history is blocked.** A fence, a decommission and a reclaim from another uuid only list
  nothing. A hidden holder behind them needs a fence by a build without the declared-lease rule in a pool
  with unequal leases, which the docs forbid (`D/configuration.md:125`, `:316-323`).

**Wiring.** One witness handle per mount attempt reaches every `claimMount` call of that pool:

| Path | Calls | Result for the file |
|---|---|---|
| First open | `CasPool.cpp:771`, `:775`, `:785-788`, and through `claimMountAwaitingExpiry` `CasServerRoot.cpp:1244`, `:1254` | by the eligibility table |
| Open under `cas_unsafe_remount_no_delay` | the authorized call at `CasPool.cpp:775` | prior `UncleanUnsafe`: the file is blocked |
| Decommission | `CasPool.cpp:798`; the admin pool has no witness | nothing |
| The claim was fenced during the open (`FencedSelf`, `CasPool.cpp:801-814`), or the adoption met a fence (`:864-876`) | a new epoch, then a claim over the process's own fenced body | prior `Fenced`: nothing; the earlier listing is left, it can never match again (F12) |
| A renewal with an unknown outcome | none | the body keeps its epoch |
| Self-remount after a lost lease | `CasPool.cpp:1574-1585` | a full wait over its own body gives `UncleanObserved`: the record lists the old epoch (own) and the new one. Over its own fenced body: nothing |
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

| # | Name | File read after the bind | Slot epoch listed | Outcome | Reason logged | Case |
|---|---|---|---|---|---|---|
| D0 | witness off or absent | – | – | full wait | `off` (once at mount) | – |
| D1 | witness not usable | – | – | full wait | `unavailable: <detail>` | – |
| D2 | taken (`EADDRINUSE`) | not read | – | full wait | `name-held` | – |
| D3 | bound | absent | – | full wait | `no-record` | (e) |
| D4 | bound | unreadable | – | full wait | `record-unreadable: <detail>` | (e) |
| D5 | bound | blocked | – | full wait | `blocked` | (e) |
| D6 | bound | header differs in any field: `pool_id`, `store`, `slot_key`, `server_uuid`, `boot_id`, `net_ns` | – | full wait | `other-header: <field>` | (c) for `net_ns`, (d) for `boot_id`, (e) for the slot identity |
| D7 | bound | present (header equal) | no | full wait | `not-recorded` | (a) or (b) |
| D8 | bound | present (header equal) | yes | **local death**: settle window | – | (a) or (b) |

- "Listed" means an `epoch` line of the inherited record has the body's `writer_epoch`.
- **The certificate is the conjunction of D8.** Each of D0 to D7 names one conjunct that failed.
- **No combination of local evidence is an error.** Start-up errors come only from the store, as today.
- **Token changes during a wait.** The loop re-reads the slot as today (F8). The new body is evaluated by
  the same table, and the change counts against the 3 restarts.
  - New body with the same listed epoch (the dead holder's last renewal landed late): D8 again, from the
    first sighting of the new token.
  - New body with another epoch: D7, the full threshold from the first sighting of the new token.
- **The reclaim stays conditional.** `claimMount` gets one more optional argument, the token authorized by
  local death. It reclaims only if that token equals the token it just read, and writes with `If-Match` on
  it (`CasServerRoot.cpp:1085-1096`). A slot that moved in between is re-read and decided again. A store
  that does not answer authorizes nothing.
- **New prior state.** `MountPriorState::UncleanLocalDeath` (`CasServerRoot.h:226-233`). The exhaustive
  switch at `CasPool.cpp:899-910` classifies it as unclean.

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

**Settle rule (D8).** With `t1` the claimer's clock sample taken after its first read of the token (F5):
reclaim when the token is still the same and `now − t1 >= S`. Both samples come from the claimer's own
clock.

**One clock, by the boot id.** The argument below compares the age of the dead holder's requests with the
length of the settle window. The holder counted its lease on `CLOCK_BOOTTIME`; the claimer counts `S` on
`CLOCK_BOOTTIME`. The certificate requires an equal boot id, so both are the same clock of the same kernel
and run at one rate. No clock reading is exchanged; only the rate matters, and it is equal by construction.

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
  plane, left the process before the name was free, and `t1` is later than the bind. So each is at least
  `S` old at the reclaim, measured at the wire.
- `S = write reserve + poll step` is at or above today's guaranteed minimum for every request kind and
  every valid setting: `poll >= poll / 1.05`.
- One write reserve alone (the memo's 16 s for S3) is below today's minimum for conditional writes by
  4762 ms. Nothing in the code depends on those 4762 ms ([§3.6](#late-landing)). The design still does not
  spend them.

**Reduction to today's protocol.** At a local-death reclaim:

1. The previous holder starts no write. Today it is "its lease has ended"; here it is "it is dead".
2. Every request it sent is at least as old as the youngest admission can be at an observed reclaim today.
3. No other process can write: the induction below.
4. The reclaim is conditional on the token, as today, and installs a strictly higher epoch (F12).
5. What follows the reclaim does not depend on how the slot was won (F10, [§3.6](#late-landing)).

So every pair (state of the old holder, age of its requests in flight) reachable at a local-death reclaim
is reachable today at an observed reclaim. The only difference is that less wall time has passed since the
holder's last renewal, and [§3.6](#late-landing) shows that no code reads that time.

**Induction: while a listed epoch holds the slot, no other process can write.**

Statement `G`: the body of this slot has epoch `e`, the witness file lists `e` for this slot, and no
process other than the incarnation of `e` passes its own lease check.

- **Base.** An epoch is listed first by a fresh claim at epoch 1 or by an `UncleanObserved` claim, on a
  file that is not blocked.
  - Epoch 1: no other incarnation of this slot ever existed (F11).
  - `UncleanObserved`: the claim watched the token for the full threshold. This is exactly the state from
    which today's protocol lets a successor write. A process other than the observed writer can still
    hold a lease at that point only through one of four things, none of which the base relies on:

| A holder hidden behind the observed body | What stands against it |
|---|---|
| A start under `cas_unsafe_remount_no_delay` that the witness saw | rule 6: the file is blocked, the base claim lists nothing |
| The same, by a build without the witness, with the setting off, or without the name | the knob's contract: one process per uuid; documented with the knob |
| The mount object deleted by hand under a live holder | the slot is then absent and the claim is fresh above epoch 1: not listed; the docs call the deletion a last resort after verifying that no server writes the root (`CasServerRoot.cpp:1193-1194`) |
| A fence by a build without the declared-lease rule, in a pool with unequal leases | the docs require equal leases in such a pool (`D/configuration.md:125`, `:316-323`) |

- **Step.** Assume `G` for epoch `e`.
  - Renewals of `e` keep `G`: a process gains the right to write only through its own confirmed slot
    write, and nobody else's write can land without replacing the body.
  - A farewell of `e`, then a `Clean` claim of `e'` over it: the incarnation of `e` gave the slot up after
    its drain, so nobody can write. The claim is eligible because `e` is listed for this slot under an
    equal header. `G` holds for `e'`.
  - A proven death of `e`, then an `UncleanLocalDeath` claim of `e'`: the incarnation of `e` is dead, so
    nobody can write. `G` holds for `e'`.
- **What breaks the chain, and what the design does.** In each case nothing is listed for the new epoch, so
  the next unclean stop pays the full wait:

| Event | Why `G` may fail | File |
|---|---|---|
| Reclaim under `cas_unsafe_remount_no_delay` | a live predecessor may still write, for up to its own lease | **blocked** until an operator deletes it |
| The same history later: unsafe `B` over live `A`, then any sequence of farewells, clean claims, observed claims and deaths | `A` may still write | still blocked: nothing is listed, nothing is certified |
| GC fence while the server was down, then a `Fenced` claim | the fence's wait belongs to another server and build | no epoch |
| Decommission, then a rejoin | the admin's farewell epoch is not listed | no epoch |
| Reclaim of a terminal body of another uuid | another lineage | no epoch |
| A process without the name took the slot by the full wait | rule 2 | no epoch |
| The slot vanished and was minted again | the slot had holders | no epoch |
| The disk now points at another pool, endpoint, prefix or server root | epochs of another slot | header mismatch: the record is absent |
| A host reboot, or a start in another network namespace | the name's history is gone or is another namespace's | header mismatch: the record is absent |
| The record was lost or is unreadable | nothing to inherit | no epoch until the next base claim |

[§8](#formal-model) checks the induction by model, with one sabotage per row that can be expressed.

**Twin detection is a side effect, not a guarantee.** A live holder whose renewal lands within `S − p` of
its start (11 s for S3 defaults, above one envelope) changes the token inside the window. The loop then
restarts the window and, after 3 restarts, refuses the start. With `p` close to or above `S` the window can
miss a renewal. The design does not rely on it.

### 3.6 What the code needs at a reclaim, and late landing {#late-landing}

**Does any executable code depend on the previous holder's lease having fully elapsed before recovery?
No.** The trace:

| Place | What it does | Depends on elapsed time |
|---|---|---|
| The comment at `CA/Backend/CasRequestBudget.h:73-78` | says a successor "waits at least one lease TTL, plus its materialization grace period, before trusting recovery listings" | **stale text.** No setting, constant or sleep with that meaning exists: a search of `CA/` for `materialization` and `grace` finds only comments, and those at `CasPool.cpp:890-897`, `:1601-1607` and `CA/Pool/CasPool.h:277-283` say the wait was removed |
| Mount open after the claim | owner recheck, renewer adoption, arming the fence; the prior state selects a log line | no (`CasPool.cpp:846-930`, F10) |
| Self-remount | no wait; asserts that the new epoch is strictly higher than the old | no (`CasPool.cpp:1601-1615`) |
| Ref-table recovery | walks each stream by arithmetic from the checkpoint; list results are diagnostics only; closes a dead epoch with a seal by create-if-absent at the next id; a late append that took that id first is adopted and the seal is tried at the following id; gives up after a bounded number of such displacements | no (`CA/Pool/CasRefLedger.cpp:1182-1345`, the list remark at `:1199-1200`, the seal at `:1269`, the straggler at `:1307-1314`) |
| GC, orphan-manifest sweep | a build prefix is eligible when the mount slot shows a higher epoch; a manifest body is deleted only when the namespace's sealed fold cursor is in a strictly higher epoch, which proves the dead epoch's seal was consumed | no: both are orderings of epochs (`CA/Gc/CasOrphanManifestSweep.cpp:588-599`, `:491-550`, the rule at `:536-550`) |
| GC, a precommit whose manifest body is absent | skipped when its build is below the mount slot's epoch, on the ground that the body "will never return" | no time; it needs the dead epoch's writer to start no new write (`CA/Gc/CasGc.cpp:3014-3040`) |

What the code does require at a reclaim is two facts, and both hold for a local-death reclaim:

1. **The new epoch is strictly higher** (F12; asserted at `CasPool.cpp:1615`).
2. **The previous holder starts no new write afterwards.** Today the elapsed lease provides it: a holder
   past its deadline admits nothing (`CasMountRuntime.cpp:156-170`). Here death provides it. Only
   `cas_unsafe_remount_no_delay` gives neither, which is why its docs call it unsafe.

Requests that the previous holder had already sent are a separate matter. No rule above gives them a time
limit, today or here.

**There is no bound the client can enforce on a sent request.**

- While the process lives, the client abandons a control-plane attempt after one envelope. That bounds the
  client's wait, not the store.
- A blob publication is not under the envelope. Its creating request runs under the disk's
  `request_timeout_ms`, with ordinary multipart or a server-side copy
  (`CA/Backend/CasObjectStorageBackend.cpp:891-917`, `D/configuration.md:111`).
- After the process dies there is no client. Bytes already given to the kernel can still be delivered, a
  proxy can still forward them, and the store can still apply them. The user docs already say that nothing
  bounds when such requests stop (`D/operations/debugging.md:304-305`).

So no settle window covers late landing, and today's 131 s does not either. The protection is structural.

| Write kind | Request | Protection against a landing after the successor's claim | Verified |
|---|---|---|---|
| Mount slot renewal, farewell | `If-Match` on the holder's last token (`CasServerRoot.cpp:2023`, `:2210`) | The reclaim changed the token: the late write is refused. A landing before the reclaim makes the reclaim's own `If-Match` fail, and the loop decides again | code |
| Ref-log append | create-if-absent at a key that carries `(writer_epoch, sequence)` (`CasRefLedger.cpp:3982`) | It lands before the seal and is adopted as history, or it loses to the seal (`CasRefLedger.cpp:1269-1314`). One append per table can be in flight | code of the walk around the seal |
| Part manifest body | create-if-absent at the manifest key (`CA/Pool/CasPartWriteTxn.cpp:696`, `CasRefLedger.cpp:259-265`) | A dead epoch cannot commit a ref to it once the seal is consumed, and GC deletes the body only after that (`CasOrphanManifestSweep.cpp:536-550`) | code |
| Catalog, checkpoint, blob marker, pool meta | create-if-absent or `If-Match` (`CA/Pool/CasRefCatalog.cpp:532`, `CA/Pool/CasRefCkpt.cpp:321`, `CA/Pool/CasBlobMeta.cpp:32-46`, `CA/Pool/CasPoolMeta.cpp:72`) | Lands only if nobody changed the key since the dead process read it. A successor that wrote the key first wins | request shape only |
| Namespace files | `HEAD`, then create-if-absent or `If-Match` with frozen bytes (`CA/Pool/CasPlainObjects.cpp:9-24`) | The same. A late file the successor never rewrites appears as a write that succeeded without an acknowledgement | request shape only |
| Blob staging objects (`cas_staging_backend = s3`) | a key that carries the writer epoch (`CasLayout.h:421`) | The key names the dead epoch; the mount-start sweep removes leftovers below the live epoch (`CasServerRoot.cpp:2257-2292`) | key shape and sweep |
| Blob publication | unconditional (`CA/Backend/CasBackend.h:193-195`, `CasPartWriteTxn.cpp:508`) | The key is the content hash. Any server of the pool may publish the same key at any time; the mount lease never serialized blob keys. A dead epoch cannot commit a ref to the blob | docs and `M/CaBlobPublishCore_witness_late_landing.cfg`; the model is not tied to mount epochs |
| Token-exact delete by the holder | delete of one observed incarnation (`CasBackend.h:180-182`) | A replaced object has another token | request shape only |
| Batch delete of write-once keys | no precondition (`CasBackend.h:197-202`) | Sent only by GC and the namespace janitor (`CA/Gc/CasGc.cpp:112`, `:544`, `CA/Gc/CasNamespaceJanitor.cpp:33`), under the GC leader lease, which this design does not touch | call sites |

**Result.** The settle window reduces to today's protocol: the code after a reclaim reads epochs and
tokens, never the time since the last renewal, and a late request after a local death is never younger
than a late request can be today. W4 corrects the stale comment in `CasRequestBudget.h`.

### 3.7 Boot id, namespaces and the classification {#classification}

What is recorded, how it is read, and what it is used for:

| Identifier | Read from | Where recorded | Used for |
|---|---|---|---|
| Boot id | `/proc/sys/kernel/random/boot_id` | header | decision (rule 4); case (d) |
| Network namespace | inode number of `/proc/self/ns/net`, by `stat` | header | decision (rule 4): the scope of the name; case (c) |
| Pid namespace | inode number of `/proc/self/ns/pid`, by `stat` | `epoch` line | the log only: case (a) against (b) |
| Pid | `getpid` | `epoch` line | the log only |

The host name is not recorded: the slot body already carries it.

- **Availability in containers.** All three files exist in the integration node image and inside a
  container; the boot id there equals the host's ([§15](#not-verified), experiment). They need `/proc`.
- **The classification proves nothing.** It names the case in the log and decides whether the bind is
  meaningful. Only an equal boot id and an equal network namespace are conjuncts of the certificate.
- **Case (b), a container restarted inside its pod.** The containers of one pod share one network
  namespace, which belongs to the pod and outlives a restart of a container. This is the documented
  behaviour of Kubernetes; it was not verified here. The new container has another pid namespace and the
  same network namespace: the bind is meaningful, and the old process, whose namespace-wide name is gone,
  is dead.
- **Host network.** With `hostNetwork` in Kubernetes or `--network host` in Docker the network namespace is
  the node's. Every such server of the node then shares one name space, and the name still separates them
  by slot identity. A replaced pod is then classified as (b), and the proof is real: the old process would
  hold the name in that same namespace.
- **A plain Docker container restart** creates new namespaces. The experiment saw them get the *same
  numbers*, because the kernel hands the lowest free number out again. That is safe: a network namespace
  is destroyed only after every socket in it is closed, so an equal number after a restart means the old
  holder's socket is gone ([§4.1](#kernel-facts)). The restart is then logged as case (a) and the proof
  succeeds, truthfully. When another namespace took the number in between, the restart is case (c).
- **A service manager that gives each start a private network namespace** makes every restart case (c).
- **No clock value crosses a process boundary.** The record stores no time. The settle window subtracts two
  samples of the claimer's own `CLOCK_BOOTTIME`, the clock the wait already uses
  (`CasMountRuntime.cpp:78-83`). `write_reserve_ms` in the record is a duration from the writer's
  configuration, not a clock reading. Time namespaces, clock offsets between containers and clock
  differences between hosts therefore cannot affect the decision.

### 3.8 Code shape {#code-shape}

New unit `CA/Pool/CasMountWitness.{h,cpp}`:

```cpp
/// Which mount object a witness record speaks about.
struct MountSlotIdentity
{
    UInt128 pool_id{};
    String store;        /// description of the object storage: endpoint and bucket, or the local root
    String slot_key;     /// the mount object's key: pool prefix and server root
    UInt128 server_uuid{};
};

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
    /// From the file as read after the bind; never from what this handle wrote.
    virtual MountWitnessVerdict evidenceFor(const MountLease & slot_body) const = 0;
    /// Before the claim's write. Lists the epoch of an eligible claim, removes this handle's listing
    /// before an ineligible one, blocks the file before an unsafe one. Throws when a removal or a
    /// block cannot be written.
    virtual void beforeClaimWrite(const MountWitnessClaim & claim) = 0;
};
```

- `LocalMountWitness` implements it. Its constructor takes the file path, the `MountSlotIdentity` and a
  `WitnessHost`: the source of the boot id and the namespace numbers, the socket calls and the file calls.
  Production passes the Linux host. Tests pass a fake for the three identifiers and keep the real socket
  and file calls.
- `PoolConfig` (`CA/Pool/CasPool.h:228-234`) gets a factory,
  `std::function<std::shared_ptr<MountWitness>(const MountSlotIdentity &)>`, empty by default.
  `Pool::mountWritable` calls it once and passes the handle to `claimMount` and
  `claimMountAwaitingExpiry`; the self-remount uses the same handle ([§3.3](#record-writes), wiring).
  `Pool::openForDecommission` clears the factory. The renewer is not touched.
- `claimMount` (`CasServerRoot.h:330-334`) and `claimMountAwaitingExpiry` (`:388`) get trailing defaulted
  parameters. Existing call sites compile unchanged.
- No production code branches on a test flag. The seams are the `MountWitness` interface, the factory, the
  `WitnessHost` and the existing injected clock and sleep (`CasPool.h:260-262`,
  `CasMountRuntime.h:76-77`).
- On a platform without abstract unix sockets or without `/proc` the host reports the witness unavailable.

## 4. Threat model {#threat-model}

Evidence names: **record** = the inherited record lists the slot's epoch and is not blocked; **slot** =
equal `pool_id`, `store`, `slot_key`, `server_uuid`; **boot** = equal boot id; **namespace** = equal
network namespace; **name** = this process bound the slot's name. Local death needs all five.

| # | The holder can be alive because | Check that catches it | Outcome |
|---|---|---|---|
| T1 | The data directory was copied to another host | boot | full wait (D6) |
| T2 | A disk or volume snapshot was restored on another machine or after a reboot | boot | full wait (D6) |
| T3 | A virtual machine snapshot with memory, or a process checkpoint, runs as a second copy; or two live processes share one `(uuid, writer_epoch)` of one slot, after the `epoch` object was deleted by hand or the store was rolled back in place | none locally; the token changes if the other copy renews inside the settle window | **residual, not defended** (P4). Already unsupported: `D/architecture/mounts-and-leases.md:215-227`, `:236-240` |
| T4 | Two containers of one pod share the volume | name: they share the network namespace | full wait (D2); a second server also fails on `status` |
| T5 | Two containers or pods with different network namespaces share the volume | namespace: the record names the other one | full wait (D6); a second server also fails on `status` |
| T6 | Two hosts share the volume through a network file system | boot | full wait (D6) |
| T7 | The directory was copied on the same host, or a block-level clone of the volume is attached to it, and the copy starts in the same network namespace | name: it is derived from the slot, not from the directory, so the original holds it | full wait (D2) |
| T8 | The same copy starts in another network namespace | namespace | full wait (D6) |
| T9 | The holder is a zombie | a zombie has no descriptors and cannot write; its name is free | local death, correct |
| T10 | The holder is stopped (`SIGSTOP`, debugger, cgroup freezer) | name: a stopped process keeps it | full wait (D2); a server also fails on `status` |
| T11 | The holder is alive and its renewals stall | name | full wait (D2) |
| T12 | pid reuse, pid namespaces | not applicable: no pid enters the decision | – |
| T13 | Clocks differ between hosts, containers or time namespaces | not applicable: no clock is compared between processes | – |
| T14 | The network namespace number was reused inside the boot | a namespace is destroyed only after its last socket is closed, so the old holder's socket is gone and the holder is dead ([§4.1](#kernel-facts)) | local death, correct |
| T15 | The holder moved to another network namespace, or the starter is in another one | namespace; the holder's socket stays in the namespace it was bound in and keeps that namespace alive | full wait (D6) |
| T16 | The volume was force-detached and attached elsewhere while the holder runs | boot, or namespace on the same node | full wait (D6) |
| T17 | A tool mounted with the same uuid and without the `status` lock | name: every writable mount binds it | full wait (D2) while the tool lives |
| T18 | The holder ran a build without the witness, or had it off | record: such a process lists nothing | full wait (D7) |
| T19 | The holder never held the name (taken, not usable, the bound of 1024) and took the slot by the full wait | record (rule 2) | full wait (D7) |
| T20 | The disk name was pointed at another pool, endpoint, prefix or server root, where a live process holds a slot with the same uuid and the same epoch number | slot: the header names the old slot, and the name differs | full wait (D6) |
| T21 | An unsafe reclaim bypassed a live holder, possibly one with a longer lease; later holders handed over, were observed, or died | record: the file is blocked (rule 6) | full wait (D5) until an operator deletes the file |
| T22 | The slot was fenced by GC, or decommissioned and rejoined, and the next holder died | record (rule 5) | full wait (D7) |
| T23 | The witness file was deleted, replaced or restored from a backup while the holder runs | name: the holder still holds it | full wait (D2) |
| T24 | A child process of the holder outlives it | the socket is close-on-exec (F18): an executed child does not hold it. A child between `vfork` and `execv` does, briefly | local death, or a full wait while such a child holds the name |
| T25 | An unrelated process bound the name | name | full wait (D2) and a `Warning`; never a false death |
| T26 | The holder is dead, a request of it is still on the way | not a liveness question: [§3.6](#late-landing) | settle window, then the structural protections |
| T27 | The same process mounts the disk again (disk reload, self-remount) | rule 3: what a handle wrote is not evidence for it; a second handle finds the name taken | full wait, as today |

### 4.1 Kernel facts behind the socket {#kernel-facts}

| Fact | Source |
|---|---|
| Abstract unix names live in a table of the socket's network namespace; a bind looks the name up there and fails with `EADDRINUSE` if it is present | `net/unix/af_unix.c:1320-1356`, `:386`, `:437-457`; `include/net/netns/unix.h:8-20` |
| The name is removed when the socket is released, which happens at the last close of its descriptor, so also when the process dies | `af_unix.c:666-674`, `:1098` |
| A user socket holds a reference on its network namespace until it is destroyed. So the namespace of a live holder cannot be destroyed | `net/core/sock.c:2244-2246`, `:2307` |
| Namespace inode numbers come from one allocator and are returned when the namespace is freed. A number is therefore unique among live namespaces and can be handed out again only after its namespace is gone | `fs/proc/generic.c:195-216`; `net/core/net_namespace.c:769-774`; `kernel/pid_namespace.c:105`, `:130`, `:154` |

From these: under one boot id, an equal network-namespace number means the same live namespace, or a new
one created after the old one was destroyed. In the first case a successful bind proves that no live
process holds the name. In the second the holder's socket was closed before the namespace could go, and a
socket that wrote is closed only by the death of its process (rule 1). Either way the holder is dead.

That the kernel closes a killed process's sockets only after every thread has stopped: not verified.

Experiment for this design (Linux 6.14, Python, Docker 29.2; predictions written first):

| Prediction | Result |
|---|---|
| A second process in the same namespace cannot bind while the holder lives | `EADDRINUSE` |
| The name stays bound while the holder is stopped with `SIGSTOP` | `EADDRINUSE` |
| After `SIGKILL` the name is free although a child the holder started by `exec` is alive | bound after 1.1 ms |
| A container that shares the holder's network namespace cannot bind; its namespace number is equal and its pid namespace differs | `EADDRINUSE`, equal, different |
| A container with its own network namespace binds the same name while the holder lives, and has another number | bound, different |
| The boot id inside a container equals the host's | equal |
| `docker restart` gives the container another network namespace number | **failed: the same number came back**, for the network and the pid namespace. The namespaces were new; the kernel reused the numbers. This is row T14 |
| A new network namespace can be made without root (`unshare -Urn`) | refused on this workstation. Tests that need another namespace use containers |

### 4.2 The three candidate proofs {#alternatives}

All three keep the record file, the slot identity, the eligibility rule, the block, the settle window and
the conditional reclaim. They differ in what proves the death.

| | Abstract socket (chosen) | `flock` on the witness file | Process identity through `/proc` |
|---|---|---|---|
| The statement | no live process of this network namespace holds the slot's name | no live process holds a lock on this inode | the process with this pid, in this pid namespace, is gone |
| Scope | one network namespace, one boot | one file system instance, one boot | one pid namespace, one boot |
| Case (a), process restart | yes | yes | yes |
| Case (b), container restart in a pod | yes | yes, on a listed file system | no |
| Pod replaced on the same node, same volume | no, unless host network | yes, if the volume stays mounted | no |
| Plain Docker restart | yes when the namespace number comes back, else full wait | yes, on a listed file system | yes when the number comes back, else full wait |
| Data directory on overlayfs or tmpfs | yes | no | yes |
| Depends on file-system semantics | no | yes: allowlist, inode identity, a gate per file system | no |
| A copy of the directory on the same host | the name is taken while the original lives | needs the recorded `(st_dev, st_ino)` to catch it | sees the live pid |
| Extra conjuncts it needs to be safe | none beyond the header | file-system allowlist; inode comparison; device-number retention | see below |
| Children of the server | `SOCK_CLOEXEC` | `O_CLOEXEC` | not applicable |
| Stopped holder | name held: full wait | lock held: full wait | pid exists: full wait |
| Memory clone, shared `(uuid, epoch)` | residual | residual | residual |

**Why not the process identity.** Read with the same rigor, it needs conjuncts that the socket does not:

- **Thread group and zombie.** `/proc/<pid>` is the group leader and exists while any thread lives. A
  killed server that nobody reaps stays a zombie, which a container without a reaping main process keeps
  forever. So "absent" is not enough: the check must read `/proc/<pid>/stat` and accept state `Z` with one
  thread as dead (`fs/proc/array.c:523`). A leader that exited while other threads run is also `Z`, with
  more than one thread, and is alive.
- **The start time is read in the reader's time namespace** (`fs/proc/array.c:586-587`). A reader in
  another time namespace sees another start time for the same live process. "Same pid, other start time"
  would then read as "pid reused, holder dead": a false certificate. It needs the time namespace as one
  more conjunct, or it must give the start time up and treat every live pid as the holder.
- **Which `/proc`.** A `/proc` mount shows the pid namespace of whoever mounted it, not of the reader. In a
  `/proc` of another namespace the recorded pid is absent or is another process. It needs a check that the
  mount is the reader's own namespace.
- **`hidepid`.** With `hidepid=2` a process of another user looks absent. It needs the mount options read
  and a refusal.
- **Pid namespace number reuse** is safe, by the same argument as T14. A nested pid namespace is caught by
  the namespace conjunct.
- With all of that it still covers only case (a). The socket covers (a) and (b) with fewer conditions, so
  the process identity adds nothing.

**Why not the `flock`.** It was the design of review rounds 1 and 2 ([§17](#review-round-1),
[§18](#review-round-2)). It is sound on ext4 and xfs, by a reading of the kernel source, but:

- It needs an allowlist of file systems, an inode comparison against copies, and a recorded gate run per
  file system, one of them by hand with root.
- It excludes overlayfs and tmpfs, so the stress lane stays red.
- What it would add over the socket: a pod replaced on the same node with the same volume, a plain Docker
  restart regardless of namespace numbers, and a service with a private network namespace per start. A
  later story can add it as a second proof for those cases; nothing here prevents it.

## 5. Mixed versions, upgrade, downgrade {#mixed-versions}

The witness changes no stored byte, so every stored object stays readable in both directions.

| Situation | Behaviour |
|---|---|
| Previous process without the code, restart with the code | No line lists the slot's epoch: full wait (D3 or D7) |
| Previous process with the code, restart without it | The file is ignored and no name is bound: full wait |
| A server without the code observes a slot of a server with it (GC leader, `system.cas_mounts`) | Unchanged: same body, same threshold |
| A server without the code fences that slot | The fence copies the body and sets `gc_fenced`; the restart reclaims at once, as today, and lists nothing |
| A server with the code reclaims by local death while a server without the code observes the same token | Both writes are conditional on the token. One lands, the other is refused, as with two observers today |
| A pool with a server that has the code and one that has not | No interaction: the witness is per server and per host |

Upgrade and downgrade:

- **Upgrade in place.** The upgraded server has no record. Its first claim is `Clean` over a farewell that
  no record lists, so it lists nothing. Its first unclean stop pays the full wait; that claim is
  `UncleanObserved` and starts the record.
- **Downgrade.** The older build ignores `cas_mount_witness`. The file stays, stale.
- **Downgrade, then upgrade again.** The stale record lists epochs that can never return to this slot
  (F12): D7. If the host rebooted in between: D6. The first eligible claim replaces the stale record.
- **A downgraded server that starts under the unsafe knob leaves no block.** This is the case "what the
  block cannot see" of [§3.3](#record-writes).
- **A newer record version read by this build** is `unreadable` (D4) and is replaced by the next eligible
  claim.
- **The setting key.** A build without the code refuses `cas_mount_lease_witness` with `UNKNOWN_SETTING`,
  as it does for `cas_request_window_ms` (`D/configuration.md:334`). The default is on, so the key is
  needed only to turn the witness off.

## 6. Observability {#observability}

Log lines. `{…}` are values. Logger names follow the existing lines.

| When | Level, logger | Text |
|---|---|---|
| Name bound | `Information`, `CasMountWitness` | `CAS mount '{srid}': mount witness name '@clickhouse-cas-mount/{hash}' is bound (boot {boot_id}, network namespace {net_ns}, pid namespace {pid_ns}); file '{path}': {record with epochs e1, e2 \| empty \| unreadable: detail \| blocked \| record of another header: field}; case: {restart of the process \| restart of a container in the same network namespace \| another network namespace \| another boot \| no record}` |
| Witness off | `Information`, `CasMountWitness` | `CAS mount '{srid}': the mount witness is off (cas_mount_lease_witness = 0); a restart after an unclean stop waits the full mount lease observation` |
| File blocked | `Warning`, `CasMountWitness` | `CAS mount '{srid}': mount witness '{path}' is blocked since a start under cas_unsafe_remount_no_delay (writer_epoch {e}); every restart after an unclean stop waits the full mount lease observation until the file is deleted while the server is stopped` |
| Name taken | `Warning`, `CasMountWitness` | `CAS mount '{srid}': mount witness name '@clickhouse-cas-mount/{hash}' is held by another process of this network namespace, or by an earlier mount of this process. This mount writes no witness record; a restart after an unclean stop waits the full mount lease observation` |
| Witness not usable for another reason (socket error, no boot id, no namespace, the bound, file error) | `Warning`, `CasMountWitness` | `CAS mount '{srid}': the mount witness is not used: {reason}. This mount writes no witness record; a restart after an unclean stop waits the full mount lease observation` |
| Local death (D8), per observed token | `Information`, `CasMountLease` | `CAS mount '{srid}': the mount witness proves that the process that wrote the mount slot is dead (writer_epoch {e}, pid {pid}; listed for this slot in this boot and network namespace, and its name is free); reclaiming once the slot token stays unchanged for ~{S} ms` |
| No evidence (D3 to D7), per observed token | `Information`, `CasMountLease` | `CAS mount '{srid}': the mount witness gives no evidence about the mount slot (writer_epoch {e}): {reason}; waiting the full observation threshold` |
| Full wait | unchanged | the line at `CasServerRoot.cpp:1292-1295` |
| Wait start, pool level | unchanged text, `CasPool.cpp:714-720` | printed with the wait that applies (`S` or the threshold) |
| Unclean reclaim | `Information`, `CasPool.cpp:913-919` | the existing line; for `UncleanLocalDeath` it ends with ` (the previous process was proven dead by the mount witness)` |
| A claim that lists nothing | `Information`, `CasMountWitness` | `CAS mount '{srid}': writer_epoch {e} is not listed in the mount witness: {why}. A restart after an unclean stop of this process waits the full mount lease observation` |
| An unsafe claim blocks the file | `Warning`, `CasMountWitness` | `CAS mount '{srid}': reclaiming under cas_unsafe_remount_no_delay; mount witness '{path}' is now blocked. Delete it while the server is stopped to use the witness again, once no other process with this server uuid can be running` |
| Record write failed | `Warning`, `CasMountWitness` | `CAS mount '{srid}': cannot write the mount witness record '{path}': {error}. The claim continues; a restart after an unclean stop may wait the full mount lease observation` |

Reasons of the decision table, fixed strings: `name-held: the witness name is held by another process or by
an earlier mount of this process`, `no-record: the witness file holds no record`,
`record-unreadable: {detail}`, `blocked: the file was blocked by a start under
cas_unsafe_remount_no_delay`, `other-header: {field} is {recorded}, this mount has {current}`,
`not-recorded: the record does not list writer_epoch {e}`, `unavailable: {detail}`.

Reasons a claim lists nothing, fixed strings: `the previous holder was fenced by GC`, `the slot was
released by a holder this witness does not list`, `the slot was taken over from another server_uuid`, `the
slot was absent under a root that had holders`, `this mount does not hold the witness name`, `the witness
file is blocked`.

Existing surfaces:

- **`system.cas_log`.** The reclaim emits the existing `MountClaim` event with branch `reclaim`
  (`CasServerRoot.cpp:1101-1109`). Its `reason` for the new prior state is
  `same server_uuid, different writer_epoch, previous process proven dead by the mount witness — reclaimed`.
  Whether a mount event of the first open reaches the table is not verified: two comments disagree
  (`CasServerRoot.cpp:919-921`, `CasPool.cpp:723-728`). Tests assert on the server log.
- **`system.cas_mounts`: no new column.** The table shows the slot, and the prior state is a fact of one
  start-up, not of the slot. A column would be `NULL` on every peer row and stale after the first renewal.
- **No new metric.** A start-up event happens once per process; the log line and the `cas_log` row carry it.
- **Who holds a name.** `ss -xlp` or `/proc/net/unix` shows abstract names with a leading `@`.

Settings documentation, `D/configuration.md`, one new row:

> `cas_mount_lease_witness` | `1` | Let a restart prove that the previous server process is dead. The
> server holds an abstract unix socket named after the disk's mount slot and keeps a small record in
> `<path>/disks/<disk>/cas_mount_witness`. With proof, a restart after `SIGKILL`, an OOM kill, an abort or
> a forced shutdown waits `2 × envelope + cas_lease_safety_margin_ms + cas_mount_renew_period_ms / 2` with
> the mount slot unchanged (21 s for an S3 disk with defaults, 17 s for a local object storage) instead of
> the observation threshold (131 s). The proof works for a restart of the process and for a restart of a
> container that keeps its network namespace, as inside a Kubernetes pod. Without proof the restart waits
> the full threshold as before, and logs the reason: after a host reboot; on another host; in another
> network namespace, as for a replaced pod; when the previous process ran a build without the witness or
> had it off; when another live process holds the name; once after an upgrade, a host reboot, a GC fence of
> this server's slot, a decommission, or a change of the disk's pool, endpoint or server root. A start
> under `cas_unsafe_remount_no_delay` blocks the witness of the disk: delete the file while the server is
> stopped to use it again. `0`: no socket is bound, the file is not opened and every unclean restart waits
> the full threshold. A build without the witness refuses the key with `UNKNOWN_SETTING`.

The row of `cas_unsafe_remount_no_delay` gets two sentences: a start under the knob blocks the mount witness
of the disk; and the knob may hide a live process with this uuid for as long as that process's lease, up to
10 minutes, also from later restarts that wait in full.

## 7. Tests {#tests}

Rules:

- **Red first.** Each test runs against the tree without the product change of its commit, with the new
  declarations present and the new decision ignored. The failing output is saved as a log. A compile error
  is not red evidence.
- **Seams.** Lease-level tests pass a fake `MountWitness` and the existing fake clock and sleep. Witness
  tests use a real temporary directory and real abstract sockets, with a fake `WitnessHost` for the boot
  id and the namespace numbers.
- **Fakes prove the rules, not the kernel.** The name's behaviour across processes and namespaces is tested
  with real processes and real containers in [§7.6](#tests-os) and [§7.7](#tests-integration).
- Suites keep the `CAS` prefix, so the `CAS*` gate runs them.

### 7.1 Record encoding {#tests-record}

`T/gtest_cas_mount_witness.cpp` (new), suite `CASMountWitnessRecord`. Fixture: the codec alone.

| # | Test | Red before |
|---|---|---|
| R-1 | `RoundTripsEveryField` (epoch lines and the block line) | no codec: the decode returns `unreadable` |
| R-2 | `EncodedRecordIsOneBlock` | size is not 4096 |
| R-3 | `TornBlockIsUnreadable` (truncated at several offsets; one byte flipped) | a damaged block decodes as `present` |
| R-4 | `UnknownVersionIsUnreadable` | version 2 decodes as `present` |
| R-5 | `EmptyFileIsAbsent` | an empty file is `unreadable` |
| R-6 | `RecordThatDoesNotFitIsRefused` | an oversized record is written |

### 7.2 Witness {#tests-witness}

Same file, suite `CASMountWitness`. Fixture: temporary directory, real sockets, fake `WitnessHost`. A held
name is a second handle in the test process: the kernel does not care which process holds a name.

| # | Test | Decision row or rule | Red before |
|---|---|---|---|
| L-1 | `HandleBindsTheNameOfItsSlot` (the name is the documented prefix and hash; another slot identity gives another name) | – | a second socket can bind the name |
| L-2 | `TakenNameGivesNoEvidenceAndWritesNothing` | D2, rule 2 | evidence is local death; the file changes |
| L-3 | `RebindAfterTheHolderClosedProvesLocalDeath` | D8 | no evidence |
| L-4 | `MissingEmptyAndUnreadableFilesGiveNoEvidence` | D3, D4 | local death or an exception |
| L-5 | `BlockedFileGivesNoEvidenceUnderAnyHeader` | D5, rule 6 | local death |
| L-6 | `AnyHeaderFieldThatDiffersMakesTheRecordAbsent` (one case per field: `pool_id`, `store`, `slot_key`, `server_uuid`, `boot_id`, `net_ns`) | D6, rule 4 | local death |
| L-7 | `UnlistedEpochGivesNoEvidence` | D7 | local death |
| L-8 | `WhatAHandleWroteIsNotEvidenceForIt` | rule 3 | an epoch listed through this handle is evidence |
| L-9 | `SecondDirectoryWithTheSameSlotCannotBindWhileTheFirstLives` (a copy of the directory, same slot identity) | T7 | the copy's handle gets evidence |
| L-10 | `UnreadableBootIdOrNamespaceIsNotUsed` | D1 | a record is written with an empty field |
| L-11 | `HandleThatNeverWroteClosesItsSocket` | rule 1 | the name stays taken after the handle is gone |
| L-12 | `HandleThatWroteKeepsTheNameAfterItIsGone` | rule 1 | the name is free |
| L-13 | `BoundOnWritingHandlesLeavesLaterMountsUnwitnessed`, also with concurrent opens from several threads | [§3.1](#witness-socket) | more than 1024 handles write |
| L-14 | `SocketIsCloseOnExecAndNeverListens` (descriptor flags; a connect to the name is refused) | [§3.1](#witness-socket) | the flag is missing or a connection is accepted |

### 7.3 Eligibility and the block {#tests-eligibility}

Same file, suite `CASMountWitnessEligibility`. Fixture: a real witness, claims described by
`MountWitnessClaim`.

| # | Test | Red before |
|---|---|---|
| E-1 | `FreshClaimAtEpochOneIsListed` | nothing is written |
| E-2 | `FreshClaimAtALaterEpochIsNotListed` | the epoch is listed |
| E-3 | `ObservedClaimIsListed` | nothing is written |
| E-4 | `CleanClaimIsListedOnlyOverAFarewellListedUnderAnEqualHeader` (listed; unlisted; listed in a record of another server root; of another pool; of another boot; of another network namespace) | listed in one of the five refused cases |
| E-5 | `LocalDeathClaimIsListedAndKeepsTheReplacedEpoch` | the replaced epoch is dropped |
| E-6 | `FencedAndForeignClaimsAreNotListed` (one case each) | the epoch is listed |
| E-7 | `IneligibleClaimRemovesAnEarlierListingOfItsEpoch` | the earlier listing stays |
| E-8 | `FailedRemovalFailsTheClaim` | the claim proceeds with the epoch listed |
| E-9 | `FailedWriteOfAnEligibleRecordLetsTheClaimProceed` | an exception escapes |
| E-10 | `ReplacedEpochSurvivesOnlyUnderAnEqualHeader` | an epoch of another boot or slot appears under this header |
| E-11 | `MountWithoutTheNameWritesNothing` | the file changes |
| E-12 | `UnsafeClaimBlocksTheFileAndSyncsIt` | the file keeps its epochs |
| E-13 | `BlockedFileListsNothingWhateverTheClaim` (fresh at epoch 1, observed, clean, in this handle and in a later one, after a changed boot id) | an epoch is listed |
| E-14 | `FailedBlockWriteFailsTheUnsafeClaim` | the claim proceeds without a block |

### 7.4 Claim site {#tests-recording}

`T/gtest_cas_mount.cpp`, suite `CASMountWitnessClaim`. Fixture: `InMemoryBackend` behind the instrumented
backend, a fake witness that logs its calls in order with the backend's writes.

| # | Test | Red before |
|---|---|---|
| H-1 | `FreshClaimIsDescribedBeforeItsCreate` | no witness call |
| H-2 | `ReclaimIsDescribedWithItsPriorStateBeforeItsReplace` (observed, clean, fenced, unsafe, foreign terminal, local death) | no witness call, or a wrong prior state |
| H-3 | `RenewalAdoptionFarewellAndFenceDoNotCallTheWitness` | pin: green before and after |
| H-4 | `WitnessFailureBeforeAClaimSendsNoWrite` | the write is sent |

### 7.5 Decision and pool level {#tests-decision}

`T/gtest_cas_mount.cpp`, suite `CASMountAwaitExpiryWitness`. Fixture: `claimMountAwaitingExpiry` over
`InMemoryBackend`, fake clock and sleep as `CASMountAwaitExpiry.PastExpiryStillPaysTheFullObservationThreshold`
(`:1326`), lease 100, poll 25, a fake witness with a scripted verdict per body.

| # | Test | Row | Red before |
|---|---|---|---|
| D-1 | `LocalDeathReclaimsAfterExactlyTheSettleWindow` | D8 | waits 130; prior `UncleanObserved` |
| D-2 | `LocalDeathUsesTheLargerRecordedWriteReserve` | D8 | waits the own reserve |
| D-3 | `LateRenewalOfTheDeadHolderRestartsTheSettleWindow` | D8, new token, same epoch | reclaims before the new window ends |
| D-4 | `TokenChangeToAnUnlistedEpochWaitsTheFullThreshold` | D8 then D7 | reclaims after the settle window |
| D-5 | `NoEvidenceWaitsExactlyAsWithoutAWitness` (one case per reason of D0 to D7) | D0 to D7 | pin for the wait; red on the logged reason |
| D-6 | `LocalDeathReclaimIsTokenExact` (the slot is replaced between the last read and the reclaim) | [§3.4](#decision-table) | pin once the argument exists: the foreign body is never overwritten |
| D-7 | `TerminalAndForeignSlotsIgnoreTheWitness` (fenced, farewell, foreign uuid, newer epoch, same pair) | unchanged slots | pin |
| D-8 | `RenewingHolderIsStillRefusedDespiteALocalDeathVerdict` (the witness lies) | twin detection | pin: `LiveDoubleStart` after 3 restarts |
| D-9 | `WaitLinesNameTheOutcomeAndTheCase` (log capture as `ScopedRemountLogCapture`, `T/gtest_cas_pool.cpp:2396-2424`) | [§6](#observability) | the new lines are absent |
| D-10 | `SettleWindowFollowsTheBudget` (S3 budget: 21000; no connect cap: 17000) | [§3.5](#arithmetic) | a fixed number |

`T/gtest_cas_pool.cpp`, suite `CASMountOpenWaits` (next to `UncleanOpenPaysOnlyTheObservationWindow`,
`:2600`). Fixture: `Pool::open` calls over `InMemoryBackend`, a real `LocalMountWitness` on a temporary
directory, fake boot clock and sleep. A killed pool is the stale-body replay of
`CASMountStartup.StaleSelfMountReclaimedAfterWait` (`T/gtest_cas_mount.cpp:1839`) plus releasing its name
the way a dead process would.

| # | Test | Red before |
|---|---|---|
| P-1 | `KilledPoolReopensAfterTheSettleWindow` | the second open waits the threshold |
| P-2 | `ReopenInAnotherBootOrNamespaceWaitsTheFullThreshold` | pin |
| P-3 | `ReopenWithoutARecordWaitsTheFullThreshold` | pin |
| P-4 | `StalledLiveHolderIsNotReclaimedBeforeItsLeaseEnds` (first pool alive, its renewals fail, second open on the same slot) | pin: the second open never claims while the first passes `mayMutate` |
| P-5 | `UnsafeStartBlocksTheWitnessUntilTheFileIsDeleted`: holder `A` without a witness and with a lease of 200 stays alive; `B` (lease 120) reclaims under the knob and is killed; `C` waits the threshold and is killed; `D` must wait the threshold again while `A` passes `mayMutate`. Then the file is deleted: the next killed pool is observed in full, and the one after it reopens after `S` | `D` reopens after `S` |
| P-6 | `DecommissionPoolHasNoWitness`; a rejoin after it lists nothing | the admin pool writes the file |
| P-7 | `KilledDuringItsOwnReclaimStillReopensAfterTheSettleWindow` (carry-over) | the third open waits the threshold |
| P-8 | `RecordFollowsEveryClaimOfOneProcess`: fresh open; a renewal with an unknown outcome; a self-remount (record: old and new epoch); a fence met by the adoption (nothing for the new claim); a second self-remount; kill; reopen. The file is asserted after each step | the record misses a claim or lists an ineligible one |
| P-9 | `FencedSlotPaysOneFullWaitThenTheWitnessIsUsedAgain` | the pool after the fence is listed |
| P-10 | `DiskRetargetedToAnotherPoolWaitsInFull`: two pools with the same server root and uuid. A witnessed holder of pool 1 is killed at epoch 1. A holder without a witness is alive on pool 2 at epoch 1, its renewals failing. A pool opened on pool 2 with pool 1's witness file must wait the threshold and must not claim while that holder passes `mayMutate` | reopens after `S` |
| P-11 | `CleanClaimIgnoresARecordOfAnotherServerRoot`: the file lists epoch 10 for root X. On root Y, a holder without the witness at epoch 10 writes a farewell. The clean claim of epoch 11 on Y lists nothing; a kill after it is observed in full | epoch 11 is listed |
| P-12 | `TakeoverWithoutTheNameThenDeathWaitsInFull`: the name is held elsewhere; a pool takes the slot by the full wait and is killed; the next pool, with the name, waits the threshold | reopens after `S` |

`T/gtest_cas_settings.cpp`: S-1 `CASContentAddressedSettings.MountLeaseWitnessDefaultsOn`. Red before: the
setting does not exist.

### 7.6 Process level, real processes {#tests-os}

`T/gtest_cas_mount_witness_os.cpp` (new), suite `CASMountWitnessOs`. The holder is the production
`LocalMountWitness` in the test process. The prober is **a second process**: a `fork` child that uses only
`socket`, `bind` and `_exit`, and reports through its exit status. Nothing is faked. The suite needs no
root, no namespace and no particular file system, so it runs in every ordinary run.

| # | Test | Asserts |
|---|---|---|
| O-1 | `SecondProcessCannotBindWhileTheHolderLives` | the child gets `EADDRINUSE` |
| O-2 | `ExecutedChildDoesNotInheritTheName` | the holder starts `/bin/sleep` by `fork` and `exec`; the handle that never wrote is destroyed; the prober binds while the `sleep` process lives |
| O-3 | `ChildThatDidNotExecKeepsTheName` | a `fork` child that sleeps without `exec` keeps the name after the parent's handle is gone; the name is free when the child exits |
| O-4 | `KilledHolderProcessFreesTheName` | a child binds the name and is stopped with `SIGSTOP`: the name stays taken. After `SIGKILL`, before the child is reaped, a production handle binds it |
| O-5 | `FailedMountBeforeItsClaimFreesTheName` | `Pool::open` fails before the claim (the backend refuses); the prober then binds |
| O-6 | `FailedMountAfterItsClaimKeepsTheName` | `Pool::open` fails after the claim (owner recheck refused); the prober is still refused after the pool object is destroyed |

Another network namespace cannot be made without root on every machine ([§4.1](#kernel-facts)). The
namespace cases are tested with containers in I-3.

### 7.7 Integration {#tests-integration}

New module `tests/integration/test_cas_restart_witness/`: two nodes, **no lease setting** in their
configuration. The first node has one S3 CAS disk in the configuration and two local-object-storage CAS
disks defined in table metadata (the stress shape). The node image keeps its root on overlayfs and has
`python3` ([§15](#not-verified), experiment).

| # | Test | Asserts | Red before |
|---|---|---|---|
| I-1 | `test_sigkill_restart_is_ready_inside_the_start_budget` | `stop_clickhouse(kill=True)`, then `start_clickhouse(start_wait_sec=60)` succeeds. Log: the witness line with `case: restart of the process`; one local-death line with `~21000 ms` for the S3 disk and one with `~17000 ms` for each local disk; no `token-stability observation` line. From log timestamps, each reclaim comes at least `S` and at most `S` + 10 s after its local-death line. Every mount is `live` with a higher `writer_epoch` | `Cannot start ClickHouse`: the server waits 131000 ms |
| I-2 | `test_forced_shutdown_restart_is_ready_inside_the_start_budget` | `shutdown_wait_unfinished` = 2, a query holds a connection, `SIGTERM`; the old server logs `Will shutdown forcefully`; then as I-1 | the same |
| I-3 | `test_witness_name_follows_the_server_process` | the name is taken from the server log. While the server runs, a bind of it inside the node's container is refused, and a bind inside the second node's container succeeds and reports another network namespace number. A long-running executable function is started. After `kill -9` of the server, with that child still alive, the bind inside the node's container succeeds | the child holds the socket, or the name is free while the server lives |
| I-4 | `test_container_restart_is_classified_and_safe` | the node's container is restarted by the harness. The witness line names the case. If it names the same network namespace, the local-death line follows and the mount is `live`; if it names another, the full observation line follows. Both outcomes pass; the test records which one happened | – (it pins the two legal outcomes of T14) |

Changed module `tests/integration/test_cas_mount_renewal_retry/` (lease 10 s, period 2 s, attempt timeout
500 ms; threshold 11500 ms; `S` = 3000 + 500 + 1000 = 4500 ms):

| # | Test | Asserts | Red before |
|---|---|---|---|
| I-5 | `test_hard_restart_observes_then_the_unsafe_knob_skips_the_observation` (`test.py:620-681`), rewritten in three phases | default: one local-death line with `~4500 ms`, no observation line. With a `cas_mount_lease_witness = 0` overlay: one `waiting ~11500 ms (token-stability observation)` line. With the unsafe overlay: neither. The test deletes the witness file at its end, with the server stopped | phase 1 has the observation line |
| I-6 | `test_restart_with_a_missing_witness_file_waits_in_full` | after the kill the witness file is deleted: `no-record` and the full observation line. The restart after the next kill has the local-death line | pin for the first half; red for the second |
| I-7 | `test_unsafe_start_blocks_the_witness` | kill; start under the unsafe overlay; the log has the block line. Stop cleanly; start without the overlay; kill; start: reason `blocked` and the full observation line. Kill; start: `blocked` again. Stop; delete the witness file; start; kill; start: the full observation line with `no-record`. Kill; start: the local-death line | the restart after the unsafe history reclaims after `S` |

Existing test that must pass without a pin: I-8
`test_backup_restore_on_cluster/test.py::test_shutdown_waits_for_backup`, first attempt, no rerun.

A foreign boot id cannot be produced in a container; L-6 and P-2 cover it through the host seam.

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
| `test_backup_restore_on_cluster/configs/cas_storage.xml` | none | – | stays without a pin (I-8) |

`test_cas_gcs` and `test_cas_replicated_relink` kill a node without a pin; whether they restart it inside
a budget: not verified. The regression suites live in `Altinity/clickhouse-regression` and set no lease
(`E/hard-restart.md` section 4).

## 8. Formal model {#formal-model}

Two existing modules cover the two halves. Both are extended behind a feature constant that is off in
every existing configuration, so existing state counts do not change (the precedent is `RecoveryGenOn`,
`M/CaCasMountCore.tla:251-257`).

**The models abstract the local death evidence as one exclusive token.** A member of a group holds it;
`Die` releases it; a holder never releases it otherwise. Whether the token is a file lock or a socket name
changes nothing in the models. The variables are named for the name.

**What the models do not check.** They take "the name is one name", "the boot is one boot" and "the record
speaks about this slot" as given. Neither model can find a kernel that frees a name early, a wrong boot
id, or a disk pointed at another pool. The kernel facts of [§4.1](#kernel-facts), the tests of
[§7.6](#tests-os) and the tests L-6, P-10 and P-11 carry those facts.

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
| Variable | `nameTaken` | the observer holds the name; enabled only when `holderDead` |
| Variable | `lastWrite` | history: holder clock of the last `Write` |
| Action | `Die` | the holder dies at any time |
| Action | `ObsTakeName` | as above |
| Change | `ObsDecide` | fences when the existing rule holds, or when `nameTaken` and the observer's sample is at least `Settle` after its sighting of the token it read |

**One rate.** The request age is counted in holder ticks and the settle window in observer ticks. The
module lets one clock run ahead of the other (`TickObserverOnly`, `:120-126`). In every witness
configuration `GainEvery = 0`: both clocks advance only together. That is the model's form of "equal boot
id, one clock" ([§3.5](#arithmetic)). No value of one clock is compared with a value of the other.

Invariants:

- `NoClaimWhileWriteAdmissible`: `slotFenced => holderDead \/ hclock >= AuthorityEnd`. This is the
  statement "no successor claims while the previous holder can still have a write admitted". It replaces
  `NoFenceInsideAuthority` in the new configurations and equals it when `holderDead` is false.
- `NoWriteAfterFence`: unchanged.
- `RequestAgeAtLeast`: `slotFenced => hclock − lastWrite >= MinRequestAge`. Run with `AuthoritySpan` set to
  the write cutoff (the `_earlyfence` shape), so `lastWrite` models the last admitted request.

Configurations (scale of `_remount`: `TTL` 4, `Period` 2, `Allowance` 1, `Cadence` 1, `GainEvery` 0):

| Config | Expect | Shows |
|---|---|---|
| `_witness_death` | green | local death is safe; `RequestAgeAtLeast` holds with `MinRequestAge = Settle` |
| `_witness_death_age_tight` | `RequestAgeAtLeast` violated | `MinRequestAge = Settle + 1`: the bound is exact |
| `_sab_death_without_name` | `NoClaimWhileWriteAdmissible` violated | local death decided while the holder lives |
| `_sab_death_no_settle` | `RequestAgeAtLeast` violated | `Settle` = 0 |
| `_sab_death_other_rate` | `RequestAgeAtLeast` violated | `GainEvery` > 0: the settle window counted on a faster clock |
| `_witness_local_death_fences`, `_witness_late_renewal_after_death` | witness reached | the path exists; a renewal that lands after the death restarts the window |

### 8.2 Identity and lineage: `CaCasMountCore` {#model-identity}

Today: incarnations of one uuid (`Incs`, `M/CaCasMountCore.tla:424`), `crashed` set by `Die` (`:865-874`)
and cleared by a later claim of the same incarnation (`:591-595`), `ObservedReclaim` (`:1014-1034`),
`CanWrite` (`:441`), one `TTL` for all. The module has no farewell and no unsafe reclaim. The twin
configurations check `OneWritableIncarnationPerUuid` (`:1676-1677`) and `SupersededWriterMakesNoMutation`
(`:1602`).

Add:

| Kind | Name | Meaning |
|---|---|---|
| Constant | `LocalWitnessOn` | feature gate |
| Constant | `WitnessGroup` | the incarnations that share one network namespace and one boot; the others are elsewhere |
| Constant | a third incarnation per uuid | needed for the induction: a live holder elsewhere, a holder in the group, a restarter |
| Constant | `UnsafeKnob` | enables `UnsafeReclaim` |
| Constant | `LeaseOf` | a lease length per incarnation, so that a bypassed holder can outlast one observation |
| Variable | `nameHolder` | `None` or one member of `WitnessGroup`. Released by `Die`. Taken again, if free, at the incarnation's next claim: a rebirth is a new process |
| Variable | `listed` | the epochs in the witness file |
| Variable | `blocked` | the file is blocked |
| Variable | `inherited` | per incarnation: the value of `listed` when it last took the name (rule 3); reset at every rebirth |
| Action | `Farewell(a)` | the holder of the current body makes it terminal and stops writing |
| Action | `CleanClaim(a)` | a claim over a farewell, at once |
| Action | `UnsafeReclaim(a)` | a claim over a live body of the same uuid with no wait; sets `blocked` when `a = nameHolder` |
| Action | `LocalDeathReclaim(a)` | `a = nameHolder`, not `blocked`, the body's epoch is in `inherited[a]`, the body has `a`'s uuid and a lower epoch and is not fenced; installs `a`'s body |
| Action | `OperatorClears` | sets `blocked` to false and empties `listed`; enabled only when no incarnation other than the writer of the current body satisfies `CanWrite` |
| Change | every claim action | adds the claim's epoch to `listed` exactly under the eligibility table of [§3.3](#record-writes), and only when `a = nameHolder` and not `blocked` |
| History | `claimedOverWriter` | set when `LocalDeathReclaim` fires while some other incarnation satisfies `CanWrite` |

**Invariants.**

- **The new safety property is about certificates only**: `NoClaimOverLiveWriter == ~claimedOverWriter`,
  "a claim made on a local-death certificate never overlaps a holder that can still admit writes". It is
  checked in every new configuration, with `UnsafeKnob` on or off. Histories keep their unsafe intervals:
  no constraint prunes a state in which two incarnations can write because of `UnsafeReclaim`.
- `OneWritableIncarnationPerUuid` is checked only in configurations with `UnsafeKnob` off, as today. An
  unsafe reclaim violates it on purpose.
- `SupersededWriterMakesNoMutation`, `NoPermanentWedge`, `ClaimNeverRegressesEpoch`: in the configurations
  with `UnsafeKnob` off.

| Config | `UnsafeKnob` | Expect | Shows |
|---|---|---|---|
| `_witness_twins` (two incarnations, both in the group) | off | green, all invariants | the restarter reclaims over a dead holder of its group only |
| `_witness_twin_elsewhere` (the holder is outside the group) | off | green; `LocalDeathReclaim` never enabled | a holder elsewhere is never listed |
| `_witness_triplet` (three incarnations: one elsewhere with a longer `LeaseOf`, two in the group) | on | `NoClaimOverLiveWriter` green, **explored to completion** | the induction of [§3.5](#arithmetic) through unsafe histories and unequal leases |
| `_sab_localdeath_no_name` | off | `NoClaimOverLiveWriter` violated | the reclaim ignores the name |
| `_sab_localdeath_no_record` | off | violated | the reclaim ignores the record |
| `_sab_localdeath_own_entries` | off | violated | evidence from what the incarnation listed itself |
| `_sab_unnamed_writer_records` | off | violated | an incarnation without the name lists its epoch |
| `_sab_unsafe_claim_listed` | on | violated | an unsafe reclaim lists its epoch and does not block |
| `_sab_block_cleared_by_observation` | on | violated | an observed claim clears the block: unsafe `B` over long-lease `A`, death of `B`, `C` observes and lists, death of `C`, `D` |
| `_sab_clean_over_unlisted_listed` | on | violated | a `Clean` claim over an unlisted farewell lists its epoch |
| `_witness_localdeathreclaim`, `_witness_blocked_full_wait`, `_witness_operator_clears` | on | witness reached | the action fires; after an unsafe history every reclaim is observed; after `OperatorClears` the lineage starts again from an observed claim |

The `Fenced` exclusion of [§3.3](#record-writes) has no sabotage row: the model's `GcFence` already waits
the full threshold, so listing after a fence is safe in the model. The exclusion covers a fencing build
that the model does not describe.

Not modelled: the slot identity (one slot in the module), the live memory clone and the shared
`(uuid, epoch)` pair (T3). The existing constraint `NoEpochWipe` prunes the last.

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
| W2 | Add the CAS mount witness with its per-slot socket name | `MountWitness`, `LocalMountWitness`, `WitnessHost`, the socket and its ownership, the bound, the header rule, the eligibility rule and the block. **`MountPriorState::UncleanLocalDeath` and its case in the switch at `CasPool.cpp:899-910`**: the eligibility rule is its first user; nothing produces the value yet. Nothing opens the witness | L-1 to L-14, E-1 to E-14, O-1 to O-4 |
| W3 | Describe every mount claim to the witness before its write | the two sites of [§3.3](#record-writes); the `PoolConfig` factory; the wiring table; cleared for decommission | H-1 to H-4, P-6, P-8, P-9, P-11, P-12, O-5, O-6 |
| W4 | Reclaim after a settle window when the witness proves a local death | the decision of [§3.4](#decision-table); the `claimMount` argument that produces `UncleanLocalDeath`; the log lines; the corrected comment in `CasRequestBudget.h:73-78` | D-1 to D-10, P-1 to P-5, P-7, P-10 |
| W5 | Open the mount witness for writable CAS disks by default | setting `cas_mount_lease_witness`; `ContentAddressedMetadataStorage::openPoolView` supplies the factory; the integration module and the changed tests | S-1, I-1 to I-7 |
| W6 | Document the CAS mount witness | the files of the list below | – |
| W7 | Drop the restart-wait lease pins from CAS integration tests | the pin table of [§7.7](#tests-integration) | the three modules, I-8 |

**Gates of W5, the commit that enables the behaviour.** W5 is not written before both are recorded:

| Gate | Evidence |
|---|---|
| G1, model | [§8.3](#model-acceptance), with `_witness_triplet` explored to completion |
| G2, processes | suite `CASMountWitnessOs` green ([§7.6](#tests-os)). The namespace half, I-3 and I-4, is green in the first run of W5 itself; W5 is not pushed without it |

Rules of the series:

- **Every code commit builds and passes `CAS*`.** W1 to W4 add code that production does not reach until W5.
- A new `gtest_*.cpp` file needs `cmake .` in every build directory before the next build.
- Subjects are imperative and at most 72 characters. Trailers: `Signed-off-by` and `Co-Authored-By`.
- Comments in the code give reasons. They do not cite this document, the memo, task numbers or decision
  row numbers.

Documentation of W6:

| File | Change |
|---|---|
| `D/configuration.md` | the setting row; two sentences in the row of `cas_unsafe_remount_no_delay` (`:114`); the restart-wait row of the lease-cost table (`:150`) and its paragraph (`:157-161`) |
| `D/architecture/mounts-and-leases.md` | a new section "Local mount witness" with the socket name, the record, the rules, the cases, the block and how to clear it, and the limits; `MountPriorState` table (`:270-276`): the new value; the state diagram; the paragraph on a killed server (`:352-354`); the clone section (`:215-227`): the witness does not defend a memory clone |
| `D/operations/troubleshooting.md` | a row: a restart after a kill waits about 135 s; the logged reasons and what to do for each: who holds the name (`ss -xlp`); a replaced pod; deleting a blocked witness file, with its condition |
| `D/operations/migration.md` | upgrade notes (`:167-181`): the first unclean restart after an upgrade waits in full; the setting key and older builds |

User docs use plain words: no row numbers of this document, no task numbers. Every new header gets an
explicit `{#anchor}`.

## 10. Acceptance criteria {#acceptance}

| # | Criterion | Checked by |
|---|---|---|
| A1 | With default settings a server killed with `SIGKILL` and started at once in the same network namespace reclaims each mount after its settle window (21 s for an S3 disk, 17 s for a local one) and is ready inside a 60 s start budget, with the data directory on overlayfs | I-1; measurement M-1 |
| A2 | The same after a forced shutdown (`Will shutdown forcefully`) | I-2 |
| A3 | Every row D0 to D7 waits exactly the threshold it waits today and logs its reason | D-5, L-2, L-4 to L-7, L-10, P-2, P-3 |
| A4 | Another boot id, another network namespace, a missing record, a taken name and a token that changes to an unlisted epoch never give the settle window | L-2, L-6, D-4, P-2 to P-4, I-3, I-6 |
| A5 | A record of another pool, object storage, slot key or server uuid never gives the settle window and never makes a clean claim eligible | L-6, E-4, P-10, P-11 |
| A6 | The reclaim after local death is conditional on the observed token | D-6 |
| A7 | A live holder is never reclaimed by local death, also when it is stopped or when its data directory was copied | P-4, D-8, L-9, O-1, O-4, I-3; `_sab_death_without_name` and `_sab_localdeath_no_name` red |
| A8 | After a GC fence, a decommission, a host reboot or an upgrade, the next unclean stop waits the full threshold. After a start under the unsafe knob every unclean stop does, until the witness file is deleted | E-4, E-6, E-12, E-13, P-5, P-6, P-9, I-7; `_sab_unsafe_claim_listed`, `_sab_block_cleared_by_observation` and `_sab_clean_over_unlisted_listed` red |
| A9 | No stored object changes its bytes: the mount body, the owner and the epoch object encode as before | existing golden and round-trip tests of `CA/Formats`, unchanged and green |
| A10 | **Lane check, before the design is said to fix a lane.** For each of the three CI lanes the witness line of the restarted server names `case: restart of the process`, and the local-death line follows. Read from the server logs the jobs upload | the measurement report |
| A11 | The gates G1 and G2 are recorded before W5 | RESULTS sections; gate logs |
| A12 | The socket is close-on-exec, a child process of the server does not hold the name, and a mount that failed before its claim holds none | L-14, O-2, O-5, I-3 |
| A13 | Recording follows every claim of one process: first open, unknown renewal outcome, failed adoption, repeated self-remount | P-8 |
| A14 | With `cas_mount_lease_witness = 0` no socket is bound, no witness file is opened and the restart logs the full observation | I-5 phase 2 |
| A15 | `test_backup_restore_on_cluster::test_shutdown_waits_for_backup` passes on the first attempt without a lease pin | I-8 |
| A16 | The three configurations of the pin table lose their pin, or keep it with a named reason | W7 |
| A17 | Every code commit passes `CAS*`; the final tree passes the gates of the pull request (ASan `CAS*`, the no-S3 compile, the listed integration modules) | gate logs |
| A18 | CAS-480: the hard-restart regression jobs and the stress `arm_release` job pass without a lease pin | CI of the pull request |

Measurements, one report `docs/superpowers/reports/2026-11-13-cas-mount-lease-witness-measurements.md`.

**Which checkouts.** Default-lease measurements must run without any lease pin.

- Integration: the work tree's own `tests/integration`, module `test_cas_restart_witness`, whose
  configuration has no lease setting.
- Regression: `Altinity/clickhouse-regression`, branch `release` at `4eb8a9490`, **without** the local
  commit `95739a313` of branch `cas-pin-mount-lease`. The clone at `tmp/regression/clickhouse-regression`
  in the `master` worktree carries that 30 s pin (`tmp/regression/REPORT.md`); the measurement uses a
  fresh checkout of `release` and records its commit id.

Predictions, written before the runs:

| # | Prediction |
|---|---|
| M-1 | `SIGKILL` and an immediate start at the 120 s lease, S3 disk: `Ready for connections` within 21 s plus the normal start-up of that server; the log has the local-death line and no `~131000 ms` line |
| M-2 | The same with the witness file removed between kill and start: the `~131000 ms` line appears and the server is ready after about 135 s |
| M-3 | Kill, 200 s pause, start: the local-death line and a 21 s wait, not 0 and not 135 s |
| M-4 | A healthy server: the witness file is written at the claim and never during renewals; its modification time does not move while the server runs |
| M-5 | The stress lane: the two disks of the restarted server each log a local-death line with `~17000 ms`, on overlayfs, and the restart fits the lane's budget |

A failed prediction is a finding. It is reported, not explained away.

## 11. Non-goals {#non-goals}

- **Cases (c), (d) and (e) keep the full wait.**
  - **(c), a replacement on the same node.** A replaced pod or container cannot prove the old one dead
    with local means. The old node, or the old pod, can be cut off from the control plane and still hold a
    lease it has not yet outlived. Only the lease, or a proof that reaches the old process, settles it.
  - **(d), another boot.** A reboot of the same node cannot be told from a cloned machine image by local
    identifiers: `machine-id` and every file are copied with an image. So nothing local may shorten the
    wait across boots.
  - **(e), no record.** Nothing to reason from.
  - For all three, the slot's age on the store's clock ([§11.1](#store-clock-age)) is what would make a
    slow replacement cost nothing.
- **A wait below the settle window after a long downtime.** A server down for an hour inside one boot still
  waits `S`.
- **Repairing the history of `cas_unsafe_remount_no_delay`.** The witness of that disk stays blocked until
  an operator deletes the file.
- **A `flock` on the witness file as a second proof.** It would add a pod replaced on the same node with
  the same volume ([§4.2](#alternatives)). A later story.
- **Releasing the lease on controlled exit paths, and a certificate from the watchdog parent.** A self-fence
  before the forced exit (`Server.cpp:4018`) needs a call in shared code (P2). The watchdog (F19) sees its
  child die and could say so, but it is off in the container image, and it would be a second mechanism.
- **The in-process self-remount** (`CasPool.cpp:1574-1585`). It keeps today's wait (P5).
- **The GC leader lease.** A restarted server may be up for a while before it leads GC again.
- **Parallel waits over several disks, and listening before the disks are mounted** (CAS-355 option b).
- **A shorter wait for a foreign observer, a shorter lease, any on-store format change.**
- **Rounding of the full wait to the poll step** (135 s for a 131 s threshold) stays.
- **Defence against a live memory clone, a shared `(uuid, epoch)` pair or a store rolled back in place**
  (P4).
- **Changes in `Altinity/clickhouse-regression` and in the stress job.**
- **A new column in `system.cas_mounts`, a new metric.**

### 11.1 Optional, separate: the slot's age on the store's clock {#store-clock-age}

This is not part of the mechanism. It is recorded as a candidate for its own small story.

The idea: one read of the mount object returns its `Last-Modified` and, in the same response, the store's
`Date`. Their difference is the age of the last renewal, measured by one clock, the store's. A claimer
could count the unchanged observation threshold from that age instead of from its own first look. Neither
the holder's clock nor the claimer's clock enters, and nothing local is needed, so it would work on
another node, in another namespace and after a reboot.

What it would cover: a server that was down longer than the lease, which today still waits 131 s; a
supervisor restart loop, which today starts the wait from zero each time; a replaced pod or node that took
longer than the lease to come up.

What it would not cover: a restart seconds after a kill. The lease is then still valid by every clock, and
only proof of death can shorten the wait. It would also need its own analysis: the resolution of
`Last-Modified` (one second), stores whose two headers come from different nodes, and a renewal that lands
long after it was sent.

## 12. Risks {#risks}

| Risk | Bound or mitigation |
|---|---|
| A kernel frees an abstract name, or reuses a namespace number, otherwise than [§4.1](#kernel-facts) reads the source | the experiment; suite `CASMountWitnessOs` and I-3 with real processes and containers; the reclaim stays conditional; a renewing holder is still caught by the token (D-8); the setting turns the witness off |
| A memory clone, or a second process with the same `(uuid, epoch)`, is stalled while the restart gets the settle window | stated residual (P4); already unsupported |
| An unsafe start that the witness could not see (older build, setting off) hid a live holder | the knob's contract; documented with the knob |
| Another process binds the name, by accident or on purpose | the server waits in full and logs a `Warning`; never a false death; `ss -xlp` names the holder |
| A security policy forbids unix sockets or hides `/proc` | D1: full wait with a `Warning` |
| A replaced pod, a plain container restart whose namespace number did not come back, or a service with a private network namespace per start still waits 135 s | by design: case (c); documented; the `flock` alternative is a later story |
| The first unclean stop after an upgrade, a reboot, a GC fence or a retargeted disk still waits 135 s | by design: the price of rules 4 and 5; documented |
| An operator forgets a blocked witness file after an emergency use of the unsafe knob | a `Warning` at every mount; a troubleshooting row |
| Kubernetes does not keep the pod's network namespace across a container restart in some runtime | then the restart is case (c): full wait, safe; not verified |
| The settle window is one write reserve plus the poll step, not one write reserve | 21 s and 17 s both fit the 60 s budgets; the owner may choose less ([§13](#open-questions)) |
| A test relied on the 131 s wait as a delay | I-5 keeps one full-wait phase; a search of `tests/` for the observation text found one test |
| A faster restart exposes tests that assume the restarted server leads GC at once | `test_cas_early_commit_restart` already has that race on the base (`triage.md` 3.1); it needs its own fix |
| `MountPriorState` gains a value; a switch outside `CasPool.cpp:899-910` misses it | `-Wswitch` fails the build; F10 lists every use |
| Sockets whose mounts wrote stay open for the life of the process | at most 1024, reserved atomically; a mount that failed before its claim holds none |
| A later change breaks rule 2, 3, 4, 5 or 6, and a record certifies a live process | L-2, L-6, L-8, E-4, E-11 to E-13, P-5, P-10, P-11; the sabotage rows of [§8.2](#model-identity) |
| A failed removal of a listing, or a failed block, fails a claim that would succeed today | needs a local I/O error on the witness file during a start; the start is retried; the setting turns the witness off |

## 13. Open questions for the owner {#open-questions}

1. **Settle window: one write reserve plus the poll step (21 s for an S3 disk, this design), one write
   reserve (16 s, the memo), or 0?** With one write reserve a conditional write of the dead process can be
   4.8 s younger at the reclaim than its admission can be today. No code reads that age
   ([§3.6](#late-landing)).
2. **The abstract socket as the only proof: confirm.** It covers a process restart and a container restart
   inside a pod, on any file system. It does not cover a pod replaced on the same node; the `flock` would,
   as a later second proof.
3. **Witness on by default with `cas_mount_lease_witness` as the off switch (P3): confirm, and confirm the
   name.**
4. **Is the residual acceptable (P4): a live memory clone, two live processes with one
   `(uuid, writer_epoch)`, or a store rolled back in place?** "No" means the settle window cannot be used
   where such copies can happen, and needs a platform signal that is not identified.
5. **Self-remount in a separate task (P5): confirm.**
6. **Self-fence before the forced exit in `programs/server/Server.cpp` (P2): still no?** No CI lane needs
   it now. It would remove even the settle window after a forced exit.
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
| The proof of death is an exclusive `flock` on a witness file, with the recorded inode | Replaced by an abstract socket named after the slot. The lock needs file-system semantics: an allowlist, an inode comparison, and no overlayfs or tmpfs | [§3.1](#witness-socket), [§4.2](#alternatives) |
| Lease arithmetic on the shared boot-time clock: count the threshold from the holder's recorded renewal start | Removed. It needs two processes to read one clock. `/proc/self/timens_offsets` shows the offsets of the namespace for children, not of the caller's own (`kernel/time/namespace.c:365-378`), so equal offsets do not prove one clock | no time in the record; [§11](#non-goals) |
| The settle window counts from the lock | It counts from the first sighting of the token, the sample the loop already takes (`CasServerRoot.cpp:1286`). Later, so safe; it reuses the loop | [§3.5](#arithmetic) |
| The record names each slot body by `(writer_epoch, write_attempt_id)`, written before every slot write | Without a time per body the epoch is enough inside one slot (F11 to F13). The record is written at a claim only | [§3.2](#local-record); question 7 |
| The record names the server root and the uuid | An epoch is allocated per slot, and one disk name can point at different slots. The header binds the pool, the object storage, the slot key and the uuid | [§3.2](#local-record) |
| Every writable mount writes the record | Only a process that holds the name, and only for an eligible claim; an unsafe claim blocks the file | rules 2, 5 and 6 |
| The descriptor is never closed | Never closed once its mount wrote; closed with a mount attempt that wrote nothing. A bound limits the first kind | [§3.1](#witness-socket) |
| The late landing of unconditional writes was not analysed; the settle window is recommended because of that gap | Traced: no code after a reclaim reads the time since the last renewal | [§3.6](#late-landing) |
| Model: check `GlobalSupersededWriterMakesNoMutation` in the twin extension | The twin configurations do not list that invariant; they check `OneWritableIncarnationPerUuid`, which an unsafe reclaim violates on purpose. The new property is about certificates only | [§8.2](#model-identity) |
| Forced exit: connections still open | The same block exits also for unfinished refresh tasks or background queries (`Server.cpp:4006`). Under TSan `safeExit` is `SYS_exit_group`, not `_exit` (`safeExit.cpp:18`) | fact table |
| `DisksApp.cpp:605-609`; whether the tool mounts writable is open | The uuid load is at `:602-610`; the tool creates disks through the ordinary factory (`DisksClient.cpp:192`) | F16 |
| Integration: a clone with a rewritten boot id | Not buildable in the harness; covered at unit level. The memo also did not list the existing test that asserts the observation line (`test_cas_mount_renewal_retry/test.py:620-681`) | [§7.7](#tests-integration) |

## 15. Not verified {#not-verified}

- Nothing of the product was built or run. Every "after" number is computed.
- The kernel facts of [§4.1](#kernel-facts) are a reading of v6.14 source plus one experiment on Linux
  6.14. Older kernels were not read. Before v5.20 the abstract-name table was global with a per-namespace
  comparison; that code was not read.
- That the kernel closes the sockets of a killed process only after every thread has stopped.
- That Kubernetes keeps a pod's network namespace across a restart of one of its containers. The Docker
  half was run: a container sharing another's namespace is excluded, and `docker restart` made new
  namespaces whose numbers came back.
- Whether `clickhouse local` mounts a CAS disk writable; what `clickhouse disks` does without a `uuid` file.
- Whether `Context::shutdown` reaches disks defined in table metadata.
- Which files are namespace files, and what a successor caches of them. The catalog, checkpoint, blob
  marker and delete rows of [§3.6](#late-landing) were checked by request shape only.
- How long a kernel delivers the data of a closed socket; how long a store may queue a request.
- Abstract sockets and `/proc/self/ns` under sandboxed kernels and strict security profiles. A refusal
  selects the full wait.
- Whether `IObjectStorage::getDescription` is stable across restarts for every object storage type. If it
  is not, the header never matches and the server waits in full.
- Whether a mount event of the first open reaches `system.cas_log`.
- How long a restarted server needs to lead GC again.
- Whether `CA/` builds on a platform without `CLOCK_BOOTTIME`.
- `test_cas_gcs`, `test_cas_replicated_relink`.
- Per-commit builds, the TLC results and the measured start-up bounds: acceptance evidence that does not
  exist yet (A11, A17, M-1 to M-5).

Verified by the experiment of [§4.1](#kernel-facts), in the integration node image
`altinityinfra/integration-test`: `/proc/self/ns/net`, `/proc/self/ns/pid` and the boot id are readable;
the boot id equals the host's; `stat` and `lstat` on the namespace links return different numbers; the
image has `python3` and `flock`; its root file system is overlayfs.

**What this design does not cover.** It is about one mount slot and the process that last wrote it, in one
boot and one network namespace. It says nothing about GC leadership or about a foreign observer.

## 16. Left to the builder {#left-to-builder}

- **Parameter shape.** Whether `claimMount` and `claimMountAwaitingExpiry` take the witness, the authorized
  token and the own write reserve as separate trailing parameters or as one small struct. Constraint:
  existing call sites compile unchanged.
- **How a handle that wrote keeps its socket open** after the handle object is gone: a process-wide list or
  a deliberate leak. Constraint: no lookup by name, and the reservation of [§3.1](#witness-socket).
- **The hash of the socket name,** as long as it has 128 bits and covers the four identity fields with
  their lengths.
- **The exact `store` string,** as long as it changes when the endpoint, the bucket or the local root
  changes and is stable across restarts of an unchanged configuration.
- **Checksum function,** if `CityHash64` is awkward at that layer: any 64-bit hash already linked into
  `dbms`. The 16-hex-digit line stays.
- **`EINTR` and short writes** of the single `pwrite`: a short write is a failed write.
- **Test names** may change for clarity. Suites keep the `CAS` prefix. The fixture and the red-before
  evidence of each row are binding.
- **I-2 mechanics.** How the test holds a connection open across `SIGTERM`; the old server must log
  `Will shutdown forcefully`.
- **How a pool test frees the name of a "killed" pool** without a test branch in production code: for
  example a handle owned by the test and released by it.
- **Model encoding.** Names of constants and variables, and whether the third incarnation is a new constant
  or a generalisation of `TwinUuids`. The rows, their expectations and the invariants are binding.

## 17. Review round 1 {#review-round-1}

Review text: `tmp/prbundle/wave2/review/spec-r1.out` in the `master` worktree. Each finding was checked
against the code or the kernel source before it was acted on. **The design reviewed in rounds 1 and 2 used
an exclusive `flock` on the witness file as the proof of death.** [§19](#proof-change) replaced that proof
by the socket. The "Change" column below says what each round did to the design of its time; the last
column says what remains of it now.

| Finding | Verdict | Check | Change at the time | Now |
|---|---|---|---|---|
| F1, blocker: `/proc/self/timens_offsets` certifies the namespace for children, not the caller's clock | Accepted | `proc_timens_show_offsets` calls `timens_for_children_get` (`kernel/time/namespace.c:365-378`; the review cites `:345-356`) | The path that counted the threshold from a recorded renewal start was removed, with everything that served it | Stands: no clock is compared between processes ([§3.7](#classification)) |
| F2, blocker: on overlayfs equal `(st_dev, st_ino)` need not be one lock | Accepted | v4.19 `fs/overlayfs/inode.c:100-140` | overlayfs left the file-system list | Moot: no file system takes part. It is one of the reasons the lock was replaced ([§4.2](#alternatives)) |
| F3, major: unsafe `B` over live `A`, farewell, `C`, death of `C`: `D` certifies while `A` writes | Accepted | `CasPool.cpp:775`, `CasMountRuntime.cpp:1243`, `CasServerRoot.cpp:1087` | The eligibility rule | Stands ([§3.3](#record-writes)) |
| F4, major: the gate can pass with an unfinished triplet search and without process-level tests | Accepted | `M/CaCasMountCore.tla:441` | A completed triplet run; a suite with real second processes | Stands; the suite tests the name instead of the lock ([§7.6](#tests-os), [§8.3](#model-acceptance)) |
| F5, major: the never-destroyed registry has no bound for failed dynamic disks | Accepted, with a simpler ownership | `DiskFromAST.cpp:88-100`, `Context.cpp:7606-7620` | No table of paths; the mount attempt owns the descriptor; a bound of 1024 | Stands, for the socket ([§3.1](#witness-socket)) |
| F6, minor: 21 s is not the value for the stress disks | Accepted | `05387_cas_publish_counts_replicated.sh:12-13`, `ContentAddressedMetadataStorage.cpp:705-720` | `S` as a formula: 21 s (S3), 17 s (local) | Stands ([§3.5](#arithmetic)) |

Settled by reading in round 1: the watchdog is forked before disks start (F19); `clickhouse disks` mounts
through the ordinary factory (F16); child processes inherit descriptors without the close-on-exec flag
(F18). One change went beyond the findings: the record lists epochs, not slot bodies, and is written per
claim (question 7).

## 18. Review round 2 {#review-round-2}

Review text: `tmp/prbundle/wave2/review/spec-r2.out` in the `master` worktree.

| Finding | Verdict | Check | Change at the time | Now |
|---|---|---|---|---|
| R2-F1, blocker: the record omits the pool, while epochs are allocated per pool | Accepted | `CasLayout.h:433-462`, `ContentAddressedMetadataStorage.cpp:755-785`, `CasPoolMetaFormat.h:18-27` | The header binds `pool_id`, `store`, `slot_key`, `server_uuid`; a differing field makes the record absent; tests L-6, P-10 | Stands; the socket name is a hash of the same four ([§3.2](#local-record)) |
| R2-F2, blocker: tmpfs inode numbers can repeat | Accepted | `mm/shmem.c:364-377` | tmpfs left the list; ext4 and xfs were shown not to reuse a number of an open file (`fs/ext4/ialloc.c:235-316`, `fs/ext4/inode.c:169-308`, `fs/inode.c:771-796`; `fs/xfs/xfs_inode.c:1194-1779`, `fs/xfs/xfs_icache.c:1933-1939`, `fs/xfs/xfs_super.c:662-672`) | Moot: no inode takes part. The reading is kept here for the later `flock` story |
| R2-F3, major: clean eligibility accepted a listing from a record of another server root | Accepted | the round-1 text | One header rule for the death evidence, the carry-over and clean eligibility; tests E-4, P-11 | Stands; the header's last field is now the network namespace ([§3](#mechanism), rule 4) |
| R2-F4, major: one full observation after an unsafe takeover does not outlast a hidden holder with a longer lease | Accepted | `CasServerRoot.cpp:1209-1218`, `:1289`; `CasMountRuntime.cpp:156-170` | An unsafe claim blocks the file durably until an operator deletes it; the 600 s clamp bounds the condition (F23); tests E-12 to E-14, P-5 | Stands ([§3.3](#record-writes)) |
| R2-F5, major: the green unsafe triplet checks an invariant that unsafe reclaim violates on purpose | Accepted | `M/CaCasMountCore.tla:441`, `:1676-1677` | The new property is about certificates only; a lease per incarnation; a `blocked` variable; rebirth retakes the token | Stands ([§8.2](#model-identity)) |
| R2-F6, minor: W2 uses an enumerator introduced in W4 | Accepted | `CasServerRoot.h:226-233`, `CasPool.cpp:899-910` | The enumerator and its switch case moved to W2 | Stands ([§9](#commits)) |

Unverified items of round 2:

| Item | Resolution |
|---|---|
| (1) Is there an executable dependency on the elapsed lease? | **No.** [§3.6](#late-landing) has the trace. The comment at `CasRequestBudget.h:73-78` is stale; W4 corrects it |
| (2) Request age in holder ticks, settle window in observer ticks | Both are `CLOCK_BOOTTIME` of one boot; the certificate requires the equal boot id for exactly this ([§3.5](#arithmetic), [§8.1](#model-timing)) |
| (3) Default-lease measurements need an unpinned regression checkout | The checkouts are named in [§10](#acceptance) |
| The descriptor bound under concurrent mounts | A slot is reserved atomically; test L-13 with threads |
| The token and the inherited record on a model actor's rebirth | Released by `Die`, taken and read again at the next claim ([§8.2](#model-identity)) |
| A partial eligible write need not leave an unlisted epoch | The log line says a restart "may wait" in full ([§6](#observability)) |

## 19. Change of the death proof {#proof-change}

After round 2 three proofs were compared on one threat table ([§4.2](#alternatives)): the `flock` of rounds
1 and 2, a recorded process identity, and an abstract unix socket. The socket was chosen.

| | What |
|---|---|
| **Chosen** | An abstract unix socket named after the slot identity, bound for the life of the process, scoped by the kernel to the network namespace |
| **Why** | It is the smallest mechanism that is safe: no file-system semantics, no inode comparison, no start-time or `/proc` reading. It covers a process restart and a container restart inside a pod |
| **Removed** | The lock on the witness file; the file-system allowlist and its `Information` line; the recorded `(st_dev, st_ino)` and its decision row; the kernel reading for ext4 and xfs as a condition; the per-file-system gate with its manual run; the lane measurement of file systems; the lock tests through hard links, symlinks and `/proc/<pid>/fd` |
| **Added** | The network namespace in the header; the pid namespace in the epoch line, for the log; the classification of cases (a) to (e); the kernel facts of [§4.1](#kernel-facts) and their experiment; tests L-9, L-14, O-1 to O-6, I-3, I-4 |
| **Unchanged** | The record file and its slot identity; the six rules, restated for the name; the eligibility rule; the block; the settle window and its formula; the conditional reclaim; the model properties; the commit series, with W2 renamed |
| **Threat rows that differ** | A copy of the directory on the same host: the lock needed the inode comparison, the socket's name is taken while the original lives (T7). A file system that lies about inodes: no longer a threat. An unrelated process that binds the name: new, and safe (T25). A replaced pod on the same node: the lock would cover it, the socket does not (case (c)). A reused namespace number: new, and safe (T14) |
| **CI lanes** | All three are case (a). The stress lane on overlayfs is now fixed: 17 s against a 110 s budget |
| **Failed prediction** | `docker restart` was expected to give another network namespace number. The same number came back. The analysis of T14 covers it: the outcome is a true certificate |
