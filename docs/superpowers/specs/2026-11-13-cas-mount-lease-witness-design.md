---
description: 'Design for a per-disk local mount witness of the CAS disk: a lock and a small record in the server data directory let a restart on the same host prove that the previous server process is dead, so the restart waits 21 s instead of the 131 s mount-lease observation threshold after SIGKILL, an OOM kill, an abort or a forced shutdown. No on-store format change, no change to shared server code, the default lease stays 120 s.'
sidebar_label: 'CAS: mount lease witness'
sidebar_position: 114
slug: /superpowers/specs/cas-mount-lease-witness-design
title: 'CAS: restart after a kill without waiting out the own mount lease'
doc_type: 'reference'
---

# CAS: restart after a kill without waiting out the own mount lease — design {#cas-mount-lease-witness}

- Date: 2026-11-13. Status: draft, not approved.
- Tasks: CAS-480 (restart after a hard kill waits 131 s), CAS-355 (forced shutdown leaves the lease; this
  design takes no part of its option "release on the forced exit").
- Source tree: `a3967637843`, the head of pull request
  [#2831](https://github.com/Altinity/ClickHouse/pull/2831). Every `path:line` below was read at that commit.
  The implementation branches from it.
- Input: `tmp/prbundle/wave2/restart-memo.md` in the `master` worktree, called "the memo". Where the memo and
  the code disagree, the code wins: [§14](#memo-corrections).
- `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`. `T/` = `src/Disks/tests/`.
  `D/` = `docs/en/antalya/cas/`. `M/` = `docs/superpowers/models/` in the `master` worktree.
- "Not verified" marks every statement that was not confirmed in the code. [§15](#not-verified) lists them.

## 1. Decision {#decision}

- **A writable CAS mount keeps an exclusive `flock` on a per-disk witness file for the life of the process
  and records, in that file, every mount-slot body before it sends it.**
- **A restart that takes the lock and finds the slot body named by the record of the same boot knows that
  the process which wrote the slot is dead.** It reclaims the slot after a settle window of 21 s with the
  slot token unchanged, instead of the 131 s observation threshold.
- **A restart that finds the record but not the same witness inode counts the unchanged 131 s threshold from
  the recorded start of the renewal**, not from its own first look. Both clocks are one `CLOCK_BOOTTIME`.
- **Every other case waits exactly as today, and logs why.** No local evidence, unreadable evidence or
  inconsistent evidence never produces a shorter wait and never produces an error.
- **Nothing on the store changes.** No new field in any stored object. The reclaim stays a write conditional
  on the slot token. A foreign observer (another server, the GC leader) waits as today.
- **No change to code shared with non-CAS ClickHouse.** All code is under `CA/`.
- **One new disk setting**, `cas_mount_lease_witness`, default `1`. With `0` the server behaves as today.

Restart wait at default settings (lease 120 s, renewal period 10 s, attempt timeout 5 s, margin 2 s). "Wait"
is the time `Pool::open` blocks before the reclaim; the server needs about 1 s more to reach
`Ready for connections` in the CI logs of [§2.1](#ci-evidence). The "after" column is computed, not measured.

| Case | Before | After |
|---|---|---|
| Graceful stop (farewell written) | 0 s | 0 s |
| `SIGKILL`, OOM kill, abort, second `SIGTERM`, `SYSTEM KILL`, forced exit; restart on the same host, same boot, same witness file | 135 s | 21 s |
| The same, server down so long that the recorded renewal started 110 s to 131 s before the first look | 135 s | 131 s minus that age |
| The same, recorded renewal started more than 131 s before the first look | 135 s | 0 s |
| Same host and boot, witness file is another inode (volume attached under another device number, directory copied) | 135 s | 131 s minus the age of the recorded renewal start |
| Host rebooted; start on another host; data directory copied or restored to another host | 135 s | 135 s |
| Previous process ran a build without the witness, or with the setting off, or could not write the record | 135 s | 135 s |
| Another live process holds the witness lock | 135 s, then refusal if that process renews | the same |
| Slot fenced by a GC leader before the restart | 0 s | 0 s |

- 135 s is the 131 s threshold rounded up to the 5 s poll of the wait loop ([§2.2](#code-facts), F6).
- 21 s is `2 × envelope + cas_lease_safety_margin_ms + max(1, cas_mount_renew_period_ms / 2)` =
  14000 + 2000 + 5000 ms ([§3.5](#arithmetic)).
- A server with N stale CAS disks from the configuration pays N waits, one after another, as today.

What does not change:

- The default `cas_mount_lease_ttl_ms` (120000) and how long a holder may write.
- The threshold of a foreign observer: `L + L/20 + step`.
- `cas_unsafe_remount_no_delay`, the farewell, the GC fence and every other existing reclaim path.
- The GC leader lease in `gc/state`. A restarted leader takes leadership back by the existing protocol.

### 1.1 Provisional decisions {#provisional}

Taken so that work can proceed. **Provisional, owner may overrule.**

| # | Decision | Status in this design |
|---|---|---|
| P1 | After a proven local death the server still waits with the slot token unchanged; the wait is not 0 | Kept. The window is **21 s, not the memo's 16 s**: one write reserve plus the poll step ([§3.5](#arithmetic)). 16 s would put the youngest conditional write of the dead process 4.8 s below the age it has today |
| P2 | No self-fence on the forced-exit path of `programs/server/Server.cpp` | Kept. Not in this story |
| P3 | The witness is on by default; a per-disk setting turns it off | Kept. The setting is `cas_mount_lease_witness` |
| P4 | A live memory clone of a running holder (VM snapshot with memory, process checkpoint) is a documented residual, not defended | Kept ([§4](#threat-model), T3) |
| P5 | The in-process self-remount after a lost lease is out of scope | Kept. The self-remount still records its slot writes, so a later restart can use them |

### 1.2 Failure asymmetry {#asymmetry}

- A wrong short wait puts two writers under one `server_root_id`. Recovery from that is manual.
- A wrong full wait costs 114 s of start-up.
- So each check that fails selects today's path. Only one statement is dangerous when false: "this process
  holds the lock on the recorded witness inode". [§4.1](#file-systems) limits where it is trusted.

## 2. Problem {#problem}

### 2.1 CI evidence {#ci-evidence}

Run 43379266824 on `a3967637843`. Evidence files: `tmp/prbundle/wave1/ci2/hard-restart.md` and
`tmp/prbundle/wave1/ci2/triage.md` in the `master` worktree.

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
| F11 | A holder samples `CLOCK_BOOTTIME` before each renewal request and, on success, sets its deadline to that sample plus the lease | `CasServerRoot.cpp:1805`, `:1818-1820`, `:1979`, `:2079-2081` |
| F12 | Every holder body carries a fresh random 128-bit `write_attempt_id`; resends of one renewal repeat it | `CasServerRoot.cpp:915`, `:1806`, `:1981`, `:1999-2000`; `CA/Formats/CasServerRootFormats.h:77-79` |
| F13 | `hostname` and `pid` in the body are diagnostics. The claim does not read them | `CasServerRoot.cpp:910-911`, `:992-1120` |
| F14 | Nothing is persisted on local disk at mount or at renewal. The only local files of a CAS disk are write-buffer spill files in the scratch directory | search of `CA/` for local file writes; `CA/ContentAddressedTransaction.cpp:1019-1023`, `src/Disks/DiskObjectStorage/MetadataStorages/MetadataStorageFactory.cpp:232-239` |
| F15 | The server takes `flock(LOCK_EX \| LOCK_NB)` on `<path>/status` before disks start, and loads `<path>/uuid` | `programs/server/Server.cpp:2033`, `:2035`; `src/Common/StatusFile.cpp:65`, `:72-76` |
| F16 | `clickhouse disks` loads the same `uuid` file and takes no `status` lock | `programs/disks/DisksApp.cpp:602-610` |
| F17 | A read-only disk never runs the mount protocol | `CasPool.cpp:595-596`, `CA/ContentAddressedMetadataStorage.cpp:864` |

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

1. **One lock.** A process takes the witness lock at most once per witness file and never releases it.
2. **Only the lock holder records.** A process writes the record only while it holds the lock, and only
   entries for bodies it is about to send itself.
3. **Evidence is inherited.** A process uses only the record it read when it took the lock. Entries it
   writes later are never evidence for itself.
4. **Entries survive only under an equal header.** A process keeps inherited entries in its own record only
   if the inherited header equals its own in every identity field.
5. **No proof, no record.** A process that reclaimed under `cas_unsafe_remount_no_delay` writes no record.

### 3.1 Witness file and lock {#witness-file}

- **Path.** `<server path>/disks/<disk name>/cas_mount_witness`, one regular file per CAS disk. `<server
  path>` is `Context::getPath`, `<disk name>` the name the metadata storage was created with
  (`ContentAddressedMetadataStorage.cpp:290`). It does not follow `cas_scratch_path`: the witness belongs
  with the `uuid` file, not with spill data.
- **Who opens it.** `ContentAddressedMetadataStorage::openPoolView`, for a mount that is writable, has a
  `Context` and has the setting on. The handle goes into a new `PoolConfig` field.
- **Open.** `open(path, O_RDWR | O_CREAT | O_CLOEXEC, 0666)`, as `StatusFile.cpp:65` does. Missing
  directories are created.
- **File system check.** `fstatfs` on the descriptor. A type outside the list of [§4.1](#file-systems) makes
  the witness unavailable: the descriptor is closed, nothing is locked, read or written.
- **Lock.** `flock(fd, LOCK_EX | LOCK_NB)`. Exclusive, non-blocking, one attempt.
- **Identity.** `fstat` on the locked descriptor gives `(st_dev, st_ino)`.
- **Lifetime.** A process-wide registry keyed by the witness path owns the handle and never drops it. The
  descriptor is closed only by the kernel when the process ends. Pool teardown, a farewell, a failed
  start-up, a disk reload and a self-remount do not close it. A second mount of the same disk in the same
  process gets the same handle.
- **Why never released.** A pool torn down without a drain can still have requests in flight in the same
  process. A released lock would certify a death that did not happen.
- **`O_CLOEXEC`.** A child process does not inherit the lock across `exec`. Between `fork` and `exec` the
  child holds the open file description; if the server dies in that window the lock stays held until the
  `exec`. That direction is safe: a held lock selects the full wait.

**The lock cannot be taken (`EWOULDBLOCK`).** Another live process holds it: a second writable mount of the
same directory, a stopped server, or a hard-linked copy of the directory.

- **Decision: full wait, not an error.** The mount continues exactly as today, with a `Warning`.
- The process is **unwitnessed** for this disk for its whole life: it reads no record and writes none
  (rule 2).
- Why not an error: today this case already ends correctly through the store. A renewing holder restarts
  the observation and the start is refused (F8). A holder whose renewals stopped loses the slot after the
  full threshold, which is the lease contract. An error here would refuse mounts that are safe today and
  would make the lock a second gate. The lock is evidence for a shorter wait and nothing else.
- Why the unwitnessed process must not record: if it did, its entries would later be read by a process that
  took the lock after the *other* holder died, and would certify the death of a process that never held
  the lock.

| Mount kind | Witness |
|---|---|
| Server, writable CAS disk | lock + record |
| Read-only CAS disk (`<readonly>`) | none: no file is opened |
| `clickhouse disks`, writable mount with the server's `path` | the same file as the server. With a live server the lock is held: unwitnessed, full wait. With a dead server the tool takes the lock and may use the record |
| `clickhouse local` | a file under the `path` it runs with; a fresh temporary `path` never inherits a record |
| `SYSTEM CAS DROP POOL MEMBER` (`Pool::openForDecommission`, `CasPool.cpp:983-1076`) | none: the admin pool clears the witness field. It writes another member's slot and never waits |
| A `Pool` opened without the field (unit tests) | none |

Whether `clickhouse disks` and `clickhouse local` mount a CAS disk writable: not verified.

### 3.2 Local record {#local-record}

The record is the content of the witness file. It is local state, not a store format: it has no `FormatId`
and no compatibility promise beyond the rules here.

- **Size.** Exactly 4096 bytes: text lines, then spaces, then one line feed.
- **Encoding.**

```text
cas_mount_witness 1
boot_id <36 characters of /proc/sys/kernel/random/boot_id>
boottime_offset_ns <signed decimal, or "unknown">
witness_dev <decimal st_dev>
witness_ino <decimal st_ino>
server_root_id <the id, to the end of the line>
server_uuid <32 lowercase hex digits>
attempt <writer_epoch> <write_attempt_id, 32 hex> <attempt_start_boot_ms> <write_reserve_ms> <pid>
attempt ...
checksum <16 lowercase hex digits>
```

| Field | Use |
|---|---|
| version (`1`) | An unknown version makes the record unreadable |
| `boot_id` | Lock history and clock values are comparable only inside one boot |
| `boottime_offset_ns` | The `boottime` offset of the writer's time namespace ([§3.7](#boot-clock)). Needed only to compare clock values |
| `witness_dev`, `witness_ino` | Identity of the file the writer had locked. A copy has the content but another inode |
| `server_root_id`, `server_uuid` | The slot and the identity the entries belong to |
| `attempt` | One mount-slot body this lineage sent: its epoch, its `write_attempt_id`, the start of the attempt on the boot-time clock, the writer's write reserve, the writer's pid (log only). Newest last, at most 4 |
| `checksum` | `CityHash64` of every byte before the `checksum` line |

- **Four entries.** The slot can hold the last confirmed body, a renewal whose outcome is unknown, and the
  claim and adoption bodies of an in-process remount that followed. A fifth entry drops the oldest.
  Dropping an entry can only remove a match, which selects the full wait.
- **Write.** One `pwrite` of the whole 4096-byte block at offset 0. No rename, no truncate.
- **No `fsync`.** The record is used only inside the boot that wrote it. The page cache survives the death
  of the process. A kernel crash or power loss changes `boot_id`, which voids the record anyway. An `fsync`
  would put local disk latency into every lease renewal for nothing.
- **Read.** `pread` of the whole file, once, right after the lock is taken.

| File content | Reading |
|---|---|
| Empty (a new file) | `absent` |
| Not exactly 4096 bytes, no checksum line, checksum mismatch, a line that does not parse | `unreadable`, with the detail |
| First line names another version | `unreadable: version N` |
| Valid | `present` |

- A torn write (a concurrent or interrupted `pwrite`) shows as a checksum mismatch: `unreadable`.
- An unreadable or foreign record is overwritten by the first record this process writes.
- A `server_root_id` with a line feed, or a record that does not fit 4096 bytes, makes the witness
  unavailable for that mount.

### 3.3 When the record is written {#record-writes}

A holder writes the record **before the request that carries the body is handed to the request engine**,
at four sites:

| Site | Body | Insert between | Recorded start |
|---|---|---|---|
| `claimMount`, fresh claim | `makeMountBody` | `CasServerRoot.cpp:1004` and `:1006` | a boot-time sample taken at the record call |
| `claimMount`, reclaim | `makeMountBody` | `:1089` and `:1091` | the same |
| `MountLeaseRenewer::start` (adoption) | `encodeBody` | `:1806` and `:1811` | the sample of `:1805` |
| `MountLeaseRenewer::renewOn` | `encodeBody` | `:1982` and `:2023` | the sample of `:1979` |

- **One sample, two users.** For a renewal the recorded start is the very value the holder later adds the
  lease to (F11). A record that carried an earlier value would let a restart count the lease out too soon.
  The model has a sabotage for this ([§8](#formal-model)).
- **One record per logical renewal.** Resends inside `renewOn` repeat the body, so they need no new entry.
- **Not recorded:** the farewell (`CasServerRoot.cpp:2171-2212`), the self-fence (`:1126-1147`) and the GC
  fence (`:1403`). Each leaves a body that is reclaimed at once anyway (F9).
- **A failed record write does not stop the slot write.** The renewal proceeds as in today's build. The
  slot then holds a body the record does not name, and the next restart waits the full threshold. The
  failure is logged ([§6](#observability)).
- **A kill between the record and the request** leaves the previous body in the slot. The record still
  names it: it is one of the four entries.
- **Carry-over (rule 4).** The first record a process writes starts from the inherited entries only if the
  inherited record is `present` and equals the process's own header in `boot_id`, `boottime_offset_ns`,
  `witness_dev`, `witness_ino`, `server_root_id` and `server_uuid`. Otherwise it starts empty. Without
  carry-over, a server killed during its own reclaim would pay the full wait on the next start.
- **Unsafe reclaim (rule 5).** `Pool::mountWritable` stops recording before the authorized `claimMount` at
  `CasPool.cpp:775`. The process stays unwitnessed.

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

| # | Lock | Inherited record | Boot id | Record names the slot body | Witness inode | Outcome | Reason logged |
|---|---|---|---|---|---|---|---|
| D0 | witness off or absent | – | – | – | – | full wait | `off` (once at mount) |
| D1 | file unusable | – | – | – | – | full wait | `unavailable: <detail>` |
| D2 | held by another process | not read | – | – | – | full wait | `lock-held` |
| D3 | acquired | absent | – | – | – | full wait | `no-record` |
| D4 | acquired | unreadable | – | – | – | full wait | `record-unreadable: <detail>` |
| D5 | acquired | present, other `server_root_id` or `server_uuid` | – | – | – | full wait | `other-identity` |
| D6 | acquired | present | different | – | – | full wait | `other-boot` |
| D7 | acquired | present | same | no | – | full wait | `not-recorded` |
| D8 | acquired | present | same | yes | same `(st_dev, st_ino)` | **local death**: settle window | – |
| D9 | acquired | present | same | yes | different, time-namespace offset equal | **credit**: full threshold from the recorded start | – |
| D10 | acquired | present | same | yes | different, offset different or unknown | full wait | `other-time-namespace` |

- "Names the slot body" means an inherited entry has the body's `writer_epoch` and `write_attempt_id`.
- **D8 with an equal time-namespace offset also gets the credit.** The reclaim happens at the earlier of
  the two deadlines.
- **No combination of local evidence is an error.** Start-up errors come only from the store, as today.
- **Token changes during a wait.** The loop re-reads the slot as today (F8). The new body is evaluated by
  the same table, and the change counts against the 3 restarts.
  - New body named by the inherited record (the dead holder's last renewal landed late): D8 or D9 again,
    from the first sighting of the new token.
  - New body not named: D7, the full threshold from the first sighting of the new token. A live renewing
    holder ends in the existing refusal.
- **The reclaim stays conditional.** `claimMount` gets one more optional argument, the token authorized by
  local death. It reclaims only if that token equals the token it just read, and writes with `If-Match` on
  it (`CasServerRoot.cpp:1085-1096`). A slot that moved in between is re-read and decided again.
- **New prior state.** `MountPriorState::UncleanLocalDeath` (`CasServerRoot.h:226-233`). The exhaustive
  switch at `CasPool.cpp:899-910` classifies it as unclean. A reclaim authorized by the credit keeps
  `UncleanObserved`: it is the existing certificate with an exact start.
- If both deadlines of D8 have passed at the same look, the prior state is `UncleanObserved`.

### 3.5 Arithmetic {#arithmetic}

Symbols: `e` = attempt envelope (`CasRequestBudget::attemptEnvelopeMs`, `CA/Backend/CasRequestBudget.cpp:21-30`),
`m` = `lease_safety_margin_ms`, `p` = renewal period, `L` and the threshold as in F4.

| Quantity | Formula | Defaults |
|---|---|---|
| Envelope `e` | `attempt_timeout + 2 × connect cap` | 7000 ms |
| Write reserve | `2 × e + m` (`CasRequestBudget.cpp:105`) | 16000 ms |
| Poll step | `max(1, p / 2)` (`CasPool.cpp:701-702`) | 5000 ms |
| **Settle window `S`** | `max(own write reserve, write reserve recorded in the matching entry) + own poll step`, saturating | **21000 ms** |
| Threshold | `L + L/20 + poll step` | 131000 ms |

**Settle rule (D8).** With `t1` the claimer's clock sample taken after its first read of the token (F5):
reclaim when the token is still the same and `now − t1 >= S`.

**Credit rule (D9).** With `t0` the recorded `attempt_start_boot_ms` of the matching entry: reclaim when the
token is still the same and `now − t0 >= threshold`. If `t0 > t1` the entry is ignored and the full wait
applies: a start in the future is not a fact of this boot.

**Sleep.** While a settle or credit deadline is pending, the loop sleeps `min(poll, time to the earliest
pending deadline)`, at least 1 ms. Without a pending witness deadline the loop sleeps `poll`, as today.

**Why the credit is sound.** The holder's authority for the token ends at `t0 + L` on its own clock (F11).
With an equal boot id and offset the claimer reads the same clock. The claimer waits to `t0 + L + L/20 +
poll`, which is today's rule with the exact start in place of a later first sighting.

**Why `S` is the write reserve plus the poll step.**

- A live holder starts a conditional write only while more than `2 × e + m` of its lease remains
  (`CasMountRuntime.cpp:156-176`, `CA/Backend/CasRequests.cpp:958-966`), and a one-envelope request only
  while more than `e + m` remains (`CasRequests.cpp:528-536`).
- Today a reclaim lands at least `poll / 1.05` after the lease ends, at the largest clock drift the rule
  allows (`M/CaMountObserveCore_RESULTS.md:22-23`).
- So today the youngest request of the old holder is, at the reclaim, at least this old:

| Request | Today, 5 % drift | Today, one clock | With `S` = 16 s | With `S` = 21 s |
|---|---|---|---|---|
| Conditional write (2 envelopes) | 16000 + 4762 = 20762 ms | 27000 ms | 16000 ms | 21000 ms |
| One-envelope request (read, token-exact delete, start of a blob publication) | 9000 + 4762 = 13762 ms | 20000 ms | 16000 ms | 21000 ms |

- After a proven death no request starts. Every request the dead process sent is at least `S` old at the
  reclaim, because `t1` is later than the lock, and the lock is later than the death.
- `S = write reserve + poll step` is at or above today's guaranteed minimum for every request kind and
  every valid setting: `poll >= poll / 1.05`.
- One write reserve alone (the memo's 16 s) is below today's minimum for conditional writes by 4762 ms.
  Nothing found in the code depends on those 4762 ms ([§3.6](#late-landing)). The design still does not
  spend them: with `S` = 21 s the argument below needs no new claim about late requests.

**Reduction to today's protocol.** At a local-death reclaim:

1. The previous holder admits no write. Today it is "its lease has ended"; here it is "it is dead".
2. Every request it sent is at least as old as the youngest request can be at an observed reclaim today.
3. The reclaim is conditional on the token, as today.
4. What follows the reclaim is the code path of `UncleanObserved` and `UncleanUnsafe`, unchanged (F10).

So every pair (state of the old holder, age of its requests in flight) reachable at a local-death reclaim
is reachable today at an observed reclaim. The only difference is that less wall time has passed since the
holder's last renewal. An epoch change seconds after the last renewal already happens on every graceful
restart and under `cas_unsafe_remount_no_delay`. No component was found that assumes a lease has elapsed
between two epochs; an exhaustive search for such an assumption was not done.

**Induction: no other process is inside a lease for this slot.** Every change of the slot body is
conditional on the previous token. A body is replaced by another writer only when its writer has no
authority left: it was fenced after the threshold, it wrote a farewell, it was observed for the threshold,
or it is dead by rule D8. The one path without proof is `cas_unsafe_remount_no_delay`; rule 5 keeps such a
process out of the record, so its successor waits the full threshold from a sighting that is later than
the unproven reclaim. Hence a process that still passes its own lease check can only be the writer of the
current body, and D8 proves that writer dead. [§8](#formal-model) checks this step by model.

**Twin detection is a side effect, not a guarantee.** A live holder whose renewal lands within
`S − p` of its start (11 s at defaults, above one envelope) changes the token inside the window, and D7
takes over. With `p` close to or above `S` the window can miss a renewal. The design does not rely on it.

### 3.6 Late landing per write kind {#late-landing}

Question: how long after the death of the process can a request it already sent still land, and what
protects the successor.

**There is no bound the client can enforce.**

- While the process lives, the client abandons a control-plane attempt after one envelope, 7 s. That bounds
  the client's wait, not the store.
- A blob publication is not under the envelope. Its creating request runs under the disk's
  `request_timeout_ms`, with ordinary multipart or a server-side copy
  (`CA/Backend/CasObjectStorageBackend.cpp:891-917`, `D/configuration.md:111`).
- After the process dies there is no client. Bytes already given to the kernel can still be delivered, a
  proxy can still forward them, and the store can still apply them. The user docs already say that nothing
  bounds when such requests stop (`D/operations/debugging.md:304-305`).
- A request that was only partly sent cannot be applied. A complete small request can, and so can a
  server-side copy the store has already accepted.
- How long the kernel keeps delivering data of a closed socket, and how long a store may queue a request:
  not verified, outside this repository.

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

**Result.** The analysis found no window longer than one write reserve that the successor must wait for:
no finite window helps, and every write kind is protected by its request shape. It did find that one write
reserve is below today's minimum request age for conditional writes. The settle window is therefore
`write reserve + poll step`, 21000 ms at defaults.

### 3.7 Boot-time clock {#boot-clock}

- **Clock.** `CLOCK_BOOTTIME`, the clock the lease already uses (`CasMountRuntime.cpp:78-83`,
  `CasServerRoot.cpp:95-100`).
- **Suspend.** It advances during suspend. A holder resumed after a long suspend sees its lease expired,
  and a record written before the suspend is dated correctly after it.
- **One boot, one clock.** All processes of one kernel instance read the same value, except across time
  namespaces.
- **Boot id.** `/proc/sys/kernel/random/boot_id`: a random id the kernel draws once per boot. Two values of
  `CLOCK_BOOTTIME` are compared only when both were taken under one boot id. Across boots or hosts the
  values are unrelated numbers, and the lock history is unrelated too.
- **Containers.** A container shares the host kernel: the same boot id and the same clock (the memo's
  experiment, [§15](#not-verified)).
- **Time namespaces.** A process in a time namespace reads `CLOCK_BOOTTIME` with a per-namespace offset.
  The record stores the writer's `boottime` offset from `/proc/self/timens_offsets`; the file being absent
  means 0. The credit needs equal offsets (D9, D10). The settle window does not compare clock values of two
  processes, so D8 does not need the offset.
- **Other hosts, virtual machines with their own kernel.** Another boot id: D6.
- **An unreadable boot id** makes the witness unavailable (D1).
- Which container runtimes set a time-namespace offset, and the behaviour of sandboxed kernels: not
  verified.

### 3.8 Code shape {#code-shape}

New unit `CA/Pool/CasMountWitness.{h,cpp}`:

```cpp
/// What the local witness knows about one mount-slot body.
struct MountWitnessVerdict
{
    enum class Kind : uint8_t { None, Credit, LocalDeath };
    Kind kind = Kind::None;
    String reason;                                    /// Kind::None: one of the reasons of the decision table
    std::optional<uint64_t> attempt_start_boot_ms;    /// set when clock values are comparable
    uint64_t holder_write_reserve_ms = 0;
    uint64_t holder_pid = 0;                          /// log only
};

class MountWitness
{
public:
    virtual ~MountWitness() = default;
    /// From the record inherited when the lock was taken; never from attempts this process recorded.
    virtual MountWitnessVerdict evidenceFor(const MountLease & slot_body) const = 0;
    /// Before the request that carries the body. Never throws: a failed write is logged and the slot
    /// write proceeds.
    virtual void recordAttempt(uint64_t writer_epoch, UInt128 write_attempt_id, uint64_t attempt_start_boot_ms) noexcept = 0;
    /// This process records nothing from here on.
    virtual void stopRecording(std::string_view why) noexcept = 0;
};
```

- `LocalMountWitness` implements it over the file. Its constructor takes a `WitnessHost`: the source of the
  boot id, the time-namespace offset, the file-system type and the file calls. Production passes the Linux
  host. Tests pass a fake for the three values and keep the real file calls.
- `PoolConfig` (`CA/Pool/CasPool.h:228-234`) gets `std::shared_ptr<MountWitness> mount_witness`, empty by
  default. `Pool::mountWritable` passes it to `claimMount`, `claimMountAwaitingExpiry` and
  `CasMountRuntime::installRenewer` (`CasMountRuntime.cpp:422-433`). `Pool::openForDecommission` clears it.
- `claimMount` (`CasServerRoot.h:330-334`) and `claimMountAwaitingExpiry` (`:388`) get trailing defaulted
  parameters. Existing call sites compile unchanged.
- No production code branches on a test flag. The seams are the `MountWitness` interface, the
  `WitnessHost` and the existing injected clock and sleep (`CasPool.h:260-262`,
  `CasMountRuntime.h:76-77`).
- On a platform without `/proc/sys/kernel/random/boot_id` the host reports the witness unavailable.

## 4. Threat model {#threat-model}

Evidence names: **record** = an inherited entry names the slot body; **boot** = equal boot id; **lock** =
this process took the exclusive lock; **inode** = the locked file is the recorded `(st_dev, st_ino)`.
Local death needs all four.

| # | The holder can be alive because | Check that catches it | Outcome |
|---|---|---|---|
| T1 | The data directory was copied to another host | boot | full wait (D6) |
| T2 | A disk or volume snapshot was restored on another machine or after a reboot | boot | full wait (D6) |
| T3 | A virtual machine snapshot with memory, or a process checkpoint, runs as a second copy | none locally; the token changes if the original renews inside the settle window | **residual, not defended** (P4). Already unsupported: `D/architecture/mounts-and-leases.md:215-227` |
| T4 | Two containers on one host share the volume | lock (`flock` excludes across containers); the second server also fails on `status` | no start, or full wait for a tool (D2) |
| T5 | Two hosts share the volume through a network file system | boot; the file system is also not on the list | full wait (D1 or D6) |
| T6 | A block-level clone of the volume is attached to the same host | inode (`st_dev` differs) | credit only (D9) |
| T7 | The directory was copied on the same host | inode (`st_ino` differs) | credit only (D9) |
| T8 | A hard-linked copy of the directory (`cp -al`) | lock: the copy shares the inode, so the original's lock is seen | full wait (D2) while the original lives; local death after it died, which is true |
| T9 | The holder is a zombie | a zombie has no descriptors and cannot write | local death, correct |
| T10 | The holder is stopped (`SIGSTOP`, debugger, cgroup freezer) | lock: a stopped process keeps it | full wait (D2); a server also fails on `status` |
| T11 | The holder is alive and its renewals stall | lock | full wait (D2) |
| T12 | pid reuse | not applicable: nothing reads the pid | – |
| T13 | pid namespaces | not applicable: the lock is on the inode | – |
| T14 | Clocks of two hosts differ | boot: no cross-host clock comparison is made | full wait (D6) |
| T15 | Holder and claimer are in different time namespaces | recorded offset | credit refused (D10); local death unaffected |
| T16 | The volume was force-detached and attached elsewhere while the holder runs | boot | full wait (D6) |
| T17 | A tool mounted with the same uuid and without the `status` lock | lock: every writable mount takes the witness lock | full wait (D2) while the tool lives |
| T18 | The holder ran a build without the witness, or had it off | record: such a process never writes an entry | full wait (D7) |
| T19 | The holder never held the lock (lock busy, unlisted file system) and took the slot by the full wait | record (rule 2) | full wait (D7) |
| T20 | The holder reclaimed under `cas_unsafe_remount_no_delay` while an older twin still had lease | record (rule 5) | full wait (D7), or no wait if the knob is still on, as today |
| T21 | The witness file was deleted and created again while the holder runs | record: the new file is empty; the holder writes to its unlinked inode | full wait (D3) |
| T22 | The data directory was restored in place from a backup | record: old entries name old bodies; or inode | full wait (D7) or credit (D9) |
| T23 | The holder is dead, a request of it is still on the way | not a liveness question: [§3.6](#late-landing) | settle window, then the structural protections |
| T24 | The same process mounts the disk again (disk reload, self-remount) | rule 3: its own entries are not evidence | full wait, as today |

### 4.1 File systems {#file-systems}

The witness is used only on a file system whose `f_type` is on an allowlist. An unknown type selects the
full wait.

| File system | `f_type` | Status |
|---|---|---|
| tmpfs | `0x01021994` | **Claimed.** Verified by the memo's experiment on Linux 6.14: exclusion, a stopped holder, a copy, a hard link, release after `SIGKILL`, a bind mount into a container |
| ext4 (and ext2, ext3) | `0xEF53` | **Claimed after the experiment of A10.** Not verified today |
| xfs | `0x58465342` | **Claimed after the experiment of A10.** Not verified today |
| overlayfs | `0x794C7630` | **Claimed only if the experiment of A10 passes on it.** Not verified today. Needed if a CI lane keeps the data directory on it |
| NFS, CIFS/SMB, CephFS, FUSE (including bucket mounts and FUSE-based volume drivers), 9p, virtiofs, Lustre, GlusterFS | not on the list | **Detected, full wait** (D1). `flock` there depends on a lock service, and inode identity on the server |
| btrfs, ZFS, f2fs and every other local file system | not on the list | **Detected, full wait** (D1). Unverified; each can be added after its experiment |

- Detection is `fstatfs` on the witness descriptor. A bind mount reports the underlying file system.
- The mount logs the file system name and `f_type`, so a CI log shows which row applied.
- A file system that reports a listed `f_type` but does not implement `flock` exclusion would break the
  claim. None is known; this is the trust the design places outside the repository, as `StatusFile` does
  for the data directory.
- That a device number is not reused while a process still holds a file on the old device: assumed from
  kernel behaviour, not verified.

## 5. Mixed versions, upgrade, downgrade {#mixed-versions}

The witness changes no stored byte, so every stored object stays readable in both directions.

| Situation | Behaviour |
|---|---|
| Previous process without the code, restart with the code | No entry names the slot body: full wait (D3 or D7) |
| Previous process with the code, restart without it | The files are ignored: full wait |
| A server without the code observes a slot of a server with it (GC leader, `system.cas_mounts`) | Unchanged: same body, same threshold |
| A server without the code fences that slot | The fence copies the body and sets `gc_fenced`; the restart reclaims at once, as today |
| A server with the code reclaims by local death while a server without the code observes the same token | Both writes are conditional on the token. One lands, the other is refused, as with two observers today |
| A pool with a server that has the code and one that has not | No interaction: the witness is per server and per host |

Upgrade and downgrade:

- **Upgrade in place.** The upgraded server has no record. Its first restart after an unclean stop pays the
  full wait. Every clean stop writes a farewell and needs no record.
- **Downgrade.** The older build ignores `cas_mount_witness`. The file stays, stale.
- **Downgrade, then upgrade again.** The stale record names bodies that are no longer in the slot: D7. If
  the host rebooted in between: D6. The first record the upgraded process writes replaces the stale one.
- **A newer record version read by this build** is `unreadable` (D4) and is overwritten.
- **The setting key.** A build without the code refuses `cas_mount_lease_witness` with `UNKNOWN_SETTING`,
  as it does for `cas_request_window_ms` (`D/configuration.md`, "The request window key"). The default is
  on, so the key is needed only to turn the witness off.

## 6. Observability {#observability}

Log lines. `{…}` are values. Logger names follow the existing lines.

| When | Level, logger | Text |
|---|---|---|
| Witness opened and locked | `Information`, `CasMountWitness` | `CAS mount '{srid}': mount witness '{path}' is locked (file system {name} 0x{f_type}, dev {dev}, inode {ino}, boot {boot_id}); inherited record: {present with N attempts \| absent \| unreadable: detail}` |
| Witness off | `Information`, `CasMountWitness` | `CAS mount '{srid}': the mount witness is off (cas_mount_lease_witness = 0); a restart after an unclean stop waits the full mount lease observation` |
| Witness not used (D1, D2) | `Warning`, `CasMountWitness` | `CAS mount '{srid}': mount witness '{path}' is not used: {reason}. This process writes no witness record; a restart after an unclean stop waits the full mount lease observation` |
| Local death (D8), per observed token | `Information`, `CasMountLease` | `CAS mount '{srid}': the mount witness proves that the process that wrote the mount slot is dead (writer_epoch {e}, pid {pid}; recorded in this boot, lock taken on the recorded witness file); reclaiming once the slot token stays unchanged for ~{S} ms` |
| Credit (D9, and D8 when it applies) | `Information`, `CasMountLease` | `CAS mount '{srid}': the mount witness dates the renewal that wrote the mount slot (writer_epoch {e}) {age} ms ago on this boot's clock; ~{remaining} ms of the ~{threshold} ms observation threshold remain` |
| No evidence (D3 to D7, D10), per observed token | `Information`, `CasMountLease` | `CAS mount '{srid}': the mount witness gives no evidence about the mount slot (writer_epoch {e}): {reason}; waiting the full observation threshold` |
| Full wait | unchanged | the line at `CasServerRoot.cpp:1292-1295` |
| Wait start, pool level | unchanged text, `CasPool.cpp:714-720` | printed with the wait that applies (`S`, the remaining credit, or the threshold) |
| Unclean reclaim | `Information`, `CasPool.cpp:913-919` | the existing line; for `UncleanLocalDeath` it ends with ` (the previous process was proven dead by the mount witness)` |
| Record write failed | `Warning`, `CasMountWitness`, once per run of failures | `CAS mount '{srid}': cannot write the mount witness record '{path}': {error}. The mount lease renewal continues; a restart after an unclean stop waits the full mount lease observation until a record is written again` |

Reasons, fixed strings: `lock-held: another live process holds the witness lock`, `no-record: the witness
file holds no record`, `record-unreadable: {detail}`, `other-identity: the record belongs to server_root_id
'{srid}', server_uuid {uuid}`, `other-boot: the record was written in boot {recorded}, this is boot
{current}`, `not-recorded: no recorded attempt matches the slot body`, `other-time-namespace: the recorded
clock offset is {recorded}, this process has {current}`, `unavailable: {detail}`.

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
> waits `2 × envelope + cas_lease_safety_margin_ms + cas_mount_renew_period_ms / 2` (21 s with defaults)
> with the mount slot unchanged, instead of the observation threshold (131 s). Without proof the restart
> waits the full threshold as before, and logs the reason: after a host reboot; on another host; from a
> copied or restored data directory; when the previous process ran a build without the witness or had it
> off; when another live process holds the lock; when the data directory is not on ext4, xfs, tmpfs or
> overlayfs. A copy of the directory on the same host waits the threshold counted from the last recorded
> renewal. `0`: the file is not opened and every unclean restart waits the full threshold. A build without
> the witness refuses the key with `UNKNOWN_SETTING`.

The list of file systems in that row follows the result of A10.

## 7. Tests {#tests}

Rules:

- **Red first.** Each test runs against the tree without the product change of its commit, with the new
  declarations present and the new decision ignored. The failing output is saved as a log. A compile error
  is not red evidence.
- **Seams.** Lease-level tests pass a fake `MountWitness` and the existing fake clock and sleep. Witness
  tests use a real temporary directory, real `flock`, and a fake `WitnessHost` for the boot id, the
  time-namespace offset and the file-system type. A lock held by a live process is a second open
  descriptor of the same file in the test process: `flock` locks belong to the open file description. A
  copied directory is a copy of the witness file: same content, another inode.
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
| R-6 | `KeepsTheNewestFourAttempts` | a fifth attempt is kept or the newest is dropped |
| R-7 | `RecordThatDoesNotFitIsRefused` | an oversized record is written |

### 7.2 Witness file {#tests-witness}

Same file, suite `CASMountWitness`. Fixture: temporary directory, fake `WitnessHost`.

| # | Test | Decision row | Red before |
|---|---|---|---|
| L-1 | `FirstOpenCreatesTheFileAndHoldsTheLock` | – | a second descriptor can lock the file |
| L-2 | `LockHeldByALiveDescriptorGivesNoEvidenceAndWritesNoRecord` | D2 | evidence is `LocalDeath`; the file changes |
| L-3 | `ReopenAfterTheHolderClosedProvesLocalDeath` | D8 | `None` |
| L-4 | `EmptyAndUnreadableRecordsGiveNoEvidence` | D3, D4 | `LocalDeath` or an exception |
| L-5 | `RecordOfAnotherIdentityGivesNoEvidence` | D5 | `LocalDeath` |
| L-6 | `OtherBootIdGivesNoEvidence` | D6 | `LocalDeath` |
| L-7 | `UnrecordedBodyGivesNoEvidence` | D7 | `LocalDeath` |
| L-8 | `CopiedFileGivesTheCreditOnly` | D9 | `LocalDeath` |
| L-9 | `OtherTimeNamespaceOffsetDropsTheCredit` | D10; D8 keeps `LocalDeath` without a start | `Credit` |
| L-10 | `OwnAttemptsAreNotEvidence` | rule 3 | an attempt recorded by this handle is evidence |
| L-11 | `InheritedAttemptsSurviveOnlyUnderAnEqualHeader` | rule 4 | attempts of another boot appear under this boot's header |
| L-12 | `UnlistedFileSystemIsNotUsed` | D1 | the file is locked and written |
| L-13 | `UnreadableBootIdIsNotUsed` | D1 | the record is written with an empty boot id |
| L-14 | `FailedRecordWriteDoesNotThrow` | [§3.3](#record-writes) | an exception escapes `recordAttempt` |
| L-15 | `StopRecordingLeavesTheFileUntouched` | rule 5 | a later attempt is written |

### 7.3 Recording sites {#tests-recording}

`T/gtest_cas_mount.cpp`, suite `CASMountWitnessRecording`. Fixture: `InMemoryBackend` behind the
instrumented backend, a fake witness that logs its calls in order with the backend's writes.

| # | Test | Red before |
|---|---|---|
| H-1 | `ClaimRecordsTheFreshBodyBeforeItsCreate` | no record call |
| H-2 | `ClaimRecordsTheReclaimBodyBeforeItsReplace` | no record call |
| H-3 | `RenewerStartRecordsTheAdoptionWithItsStartSample` | no record call |
| H-4 | `RenewalRecordsOnceWithTheSampleItsDeadlineUses` | no record call; with resends the count must stay 1 |
| H-5 | `FarewellAndFenceAreNotRecorded` | pin: green before and after |
| H-6 | `KillBetweenRecordAndWriteLeavesAMatchingRecord` | the previous body is not among the entries |

### 7.4 Decision at the lease level {#tests-decision}

`T/gtest_cas_mount.cpp`, suite `CASMountAwaitExpiryWitness`. Fixture: `claimMountAwaitingExpiry` over
`InMemoryBackend`, fake clock and sleep as `CASMountAwaitExpiry.PastExpiryStillPaysTheFullObservationThreshold`
(`:1326`), lease 100, poll 25, a fake witness with a scripted verdict per body.

| # | Test | Row | Red before |
|---|---|---|---|
| D-1 | `LocalDeathReclaimsAfterExactlyTheSettleWindow` | D8 | waits 130; prior `UncleanObserved` |
| D-2 | `LocalDeathUsesTheLargerRecordedWriteReserve` | D8 | waits the own reserve |
| D-3 | `LateRenewalOfTheDeadHolderRestartsTheSettleWindow` | D8, token change to a recorded body | waits 130 |
| D-4 | `TokenChangeToAnUnrecordedBodyWaitsTheFullThreshold` | D8 then D7 | reclaims after the settle window |
| D-5 | `NoEvidenceWaitsExactlyAsWithoutAWitness` (one case per reason of D0 to D7 and D10) | D0 to D7, D10 | pin for the wait; red on the logged reason |
| D-6 | `CreditFromALongPastStartReclaimsAtOnce` | D9 | waits 130 |
| D-7 | `CreditWaitsOnlyTheRemainder` (start 50 ago: waits 80) | D9 | waits 130 |
| D-8 | `CreditWithAStartInTheFutureIsIgnored` | D9 guard | reclaims early |
| D-9 | `EarlierOfSettleAndCreditWins` | D8 with credit | waits the later one |
| D-10 | `LocalDeathReclaimIsTokenExact` (the slot is replaced between the last read and the reclaim) | [§3.4](#decision-table) | pin once the argument exists: the foreign body is never overwritten |
| D-11 | `TerminalAndForeignSlotsIgnoreTheWitness` (fenced, farewell, foreign uuid, newer epoch, same pair) | unchanged slots | pin |
| D-12 | `RenewingHolderIsStillRefusedDespiteALocalDeathVerdict` (the witness lies for the first body) | twin detection | pin: `LiveDoubleStart` after 3 restarts |
| D-13 | `WaitLinesNameTheOutcome` (log capture as `ScopedRemountLogCapture`, `T/gtest_cas_pool.cpp:2396-2424`) | [§6](#observability) | the new lines are absent |

### 7.5 Pool level {#tests-pool}

`T/gtest_cas_pool.cpp`, suite `CASMountOpenWaits` (next to `UncleanOpenPaysOnlyTheObservationWindow`,
`:2600`). Fixture: two `Pool::open` calls over one `InMemoryBackend`, a real `LocalMountWitness` on a
temporary directory, fake boot clock and sleep. A killed pool is the stale-body replay of
`CASMountStartup.StaleSelfMountReclaimedAfterWait` (`T/gtest_cas_mount.cpp:1839`) plus closing its witness
handle.

| # | Test | Red before |
|---|---|---|
| P-1 | `KilledPoolReopensAfterTheSettleWindow` | the second open waits the threshold |
| P-2 | `ReopenWithACopiedWitnessOfAnotherBootWaitsTheFullThreshold` | pin |
| P-3 | `ReopenWithoutARecordWaitsTheFullThreshold` | pin |
| P-4 | `StalledLiveHolderIsNotReclaimedBeforeItsLeaseEnds` (first pool alive, its renewals fail, second open on the same directory) | pin: the second open never claims while the first passes `mayMutate` |
| P-5 | `UnsafeReclaimWritesNoRecord` | the record names the unsafe claim |
| P-6 | `DecommissionPoolHasNoWitness` | the admin pool records the victim's slot |
| P-7 | `KilledDuringItsOwnReclaimStillReopensAfterTheSettleWindow` (carry-over) | the third open waits the threshold |

`T/gtest_cas_settings.cpp`: S-1 `CASContentAddressedSettings.MountLeaseWitnessDefaultsOn`. Red before: the
setting does not exist.

### 7.6 Integration {#tests-integration}

New module `tests/integration/test_cas_restart_witness/`: one node, one CAS disk, **no lease setting** in
its configuration.

| # | Test | Asserts | Red before |
|---|---|---|---|
| I-1 | `test_sigkill_restart_is_ready_inside_the_start_budget` | `stop_clickhouse(kill=True)`, then `start_clickhouse(start_wait_sec=60)` succeeds; the log has one local-death line with `~21000 ms` and no `token-stability observation` line; the mount is `live` with a higher `writer_epoch` | `Cannot start ClickHouse`: the server waits 131000 ms |
| I-2 | `test_forced_shutdown_restart_is_ready_inside_the_start_budget` | `shutdown_wait_unfinished` = 2, a query holds a connection, `SIGTERM`; the old server logs `Will shutdown forcefully`; then as I-1 | the same |

Changed module `tests/integration/test_cas_mount_renewal_retry/` (lease 10 s, period 2 s; threshold
11500 ms; `S` = 3000 + 500 + 1000 = 4500 ms):

| # | Test | Asserts | Red before |
|---|---|---|---|
| I-3 | `test_hard_restart_observes_then_the_unsafe_knob_skips_the_observation` (`test.py:620-681`), rewritten in three phases | default: one local-death line with `~4500 ms`, no observation line. With a `cas_mount_lease_witness = 0` overlay: one `waiting ~11500 ms (token-stability observation)` line. With the unsafe overlay: neither | phase 1 has the observation line |
| I-4 | `test_restart_with_a_replaced_witness_file_gets_no_death_certificate` | after the kill the witness file is replaced by a copy of itself (`cp -a`, `rm`, `mv`); the restart logs the credit line and no local-death line. A second round deletes the file: `no-record` and the full observation line | pin before the feature; red against a build whose inode check is removed |
| I-5 | `test_restart_after_the_lease_ran_out_does_not_wait` | kill, 13 s pause, start: a credit line with `~0 ms` remaining; no settle wait | the full observation line |

Existing test that must pass without a pin: I-6
`test_backup_restore_on_cluster/test.py::test_shutdown_waits_for_backup`, first attempt, no rerun.

- **A copied data directory in a second container was not chosen.** The inode replacement of I-4 exercises
  the same check inside the existing one-node harness. A foreign boot id cannot be produced in a container
  without editing the record; L-6 and P-2 cover it through the host seam.
- I-1 to I-5 need the node's data directory on a listed file system. If the CI runners keep it elsewhere,
  the tests show `unavailable` and fail; that is the signal A10 asks for.

Lease pins in test configurations:

| Configuration | Pin | Reason for the pin | After this story |
|---|---|---|---|
| `test_cas_early_commit_restart/configs/storage_conf.xml:16` | 30000 | restart wait only (`test.py:88-96`) | removed |
| `test_cas_gc_sharded/configs/storage_conf.xml:29` | 30000 | restart wait only (`test.py:261`) | removed |
| `test_cas_shared_pool/configs/storage_conf.xml:27` | 30000 | restart wait (`test.py:265-304`); a later assertion mentions the killed server's lease (`:314-322`), not verified whether it needs the pin | removed if the module passes without it; else kept with a comment that names the assertion |
| `test_cas_drop_pool_member/configs/storage_conf.xml:24` | 30000 | a peer waits for the killed member's lease and the GC fence (`test.py:248-282`): a foreign observer, unchanged here | kept |
| `test_cas_mount_renewal_retry/configs/storage_conf.xml:32-33` | 10000 / 2000 | renewal-retry timing | kept |
| `test_cas_mount_renewal_retry/configs/request_budget_disks.xml:47` | 50000 | request-budget probe | kept |
| `test_cas_mount_renewal_retry/configs/unsafe_remount.xml:8` | knob | tests the knob | kept |
| `test_backup_restore_on_cluster/configs/cas_storage.xml` | none | – | stays without a pin (I-6) |

`test_cas_gcs` and `test_cas_replicated_relink` kill a node without a pin; whether they restart it inside
a budget: not verified. The regression suites live in `Altinity/clickhouse-regression` and set no lease
(`hard-restart.md` section 4); they need no change if their data directory is on a listed file system.

## 8. Formal model {#formal-model}

Two existing modules cover the two halves. Both are extended behind a feature constant that is off in
every existing configuration, so existing state counts do not change (the precedent is `RecoveryGenOn`,
`M/CaCasMountCore.tla:251-257`).

### 8.1 Timing: `CaMountObserveCore` {#model-timing}

Today: one holder, one observer, one slot, two clocks that nothing compares (`M/CaMountObserveCore.tla:59-79`).
The observer fences a token it watched for `Threshold` (`:214-243`). Invariant `NoFenceInsideAuthority`
(`:287-288`). A request that lands after the fence is not modelled (`M/CaMountObserveCore_RESULTS.md:239-240`).

Add:

| Kind | Name | Meaning |
|---|---|---|
| Constant | `WitnessOn` | feature gate |
| Constant | `SameBoot`, `ClockOffset` | the observer reads the holder's clock (`ClockOffset` = 0, no drift), or an unrelated one |
| Constant | `SameWitness` | the observer can take the lock on the holder's witness |
| Constant | `Settle`, `MinRequestAge` | the settle window; the bound of the new invariant |
| Variable | `holderDead` | set by `Die`; disables `RenewStart` and `Write`; a renewal in flight may still land |
| Variable | `rec` | starts of the attempts the holder recorded; written in `RenewStart` |
| Variable | `slotAttempt` | start of the attempt whose body is in the slot; set in `RenewLand` |
| Variable | `lockTaken` | the observer holds the lock; enabled only when `SameWitness` and `holderDead` |
| Variable | `lastWrite` | history: holder clock of the last `Write` |
| Action | `Die` | the holder dies at any time |
| Action | `ObsTakeLock` | as above |
| Change | `ObsDecide` | fences when any rule holds for the token it read: the existing one; **credit** (`SameBoot`, `slotAttempt ∈ rec`, sample ≥ `slotAttempt + Threshold`); **local death** (`SameBoot`, `lockTaken`, `slotAttempt ∈ rec`, sample ≥ sighting + `Settle`) |

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
| `_witness_credit` | green | credit from the recorded start never fences inside authority |
| `_witness_otherboot` | green; no local rule fires | `SameBoot` false, `ClockOffset` 3 |
| `_sab_credit_other_boot` | violation | credit used with an unrelated clock |
| `_sab_credit_unnamed` | violation | credit dated at the newest entry although the slot holds another body |
| `_sab_record_earlier_start` | violation | the record carries a start earlier than the one authority counts from |
| `_sab_death_without_lock` | violation | local death decided while the holder lives |
| `_sab_death_no_settle` | `RequestAgeAtLeast` violated | `Settle` = 0 |
| `_witness_local_death_fences`, `_witness_credit_fences`, `_witness_late_renewal_after_death` | witness reached | the three paths exist, including a renewal that lands after the death and restarts the window |

### 8.2 Identity: `CaCasMountCore` {#model-identity}

Today: incarnations of one uuid (`Incs`, `M/CaCasMountCore.tla:424`), `crashed` set by `Die` (`:865-874`),
`ObservedReclaim` (`:1014-1034`), the ghost `bodyWriter`, `CanWrite` (`:441`). The twin configurations
check `OneWritableIncarnationPerUuid` (`:1676-1677`) and `SupersededWriterMakesNoMutation` (`:1602`), not
`GlobalSupersededWriterMakesNoMutation` (header of `M/CaCasMountCore_twins.cfg`).

Add:

| Kind | Name | Meaning |
|---|---|---|
| Constant | `LocalWitnessOn` | feature gate |
| Constant | `WitnessGroup` | the incarnations that share one witness file and one boot; the others are elsewhere |
| Constant | a third incarnation per uuid | needed for the induction: an older stalled holder elsewhere, a dead holder, a restarter |
| Variable | `lockHolder` | `None` or one member of `WitnessGroup`; taken at the member's first claim if free; released only by `Die` |
| Variable | `recorded` | tokens of the bodies written by an incarnation while it was `lockHolder` (rule 2) |
| Variable | `inherited` | per incarnation: the value of `recorded` when it took the lock (rule 3) |
| Action | `LocalDeathReclaim(a)` | `a = lockHolder`, the current token is in `inherited[a]`, the body has `a`'s uuid and a lower epoch and is not fenced; installs `a`'s body as the reclaim branch of `ClaimMount` does |
| History | `claimedOverWriter` | set when `LocalDeathReclaim` fires while some twin satisfies `CanWrite` |

Invariants in the new configurations: `OneWritableIncarnationPerUuid`, `SupersededWriterMakesNoMutation`,
`NoPermanentWedge`, `ClaimNeverRegressesEpoch`, and the new `NoClaimOverLiveWriter == ~claimedOverWriter`.

| Config | Expect | Shows |
|---|---|---|
| `_witness_twins` (two incarnations, both in the group) | green | the restarter reclaims over a dead co-located holder only |
| `_witness_twin_elsewhere` (the holder is outside the group) | green; `LocalDeathReclaim` never enabled | a holder elsewhere never matches a record |
| `_witness_triplet` (three incarnations: one elsewhere, two in the group) | green | the induction of [§3.5](#arithmetic) |
| `_sab_localdeath_no_lock` | `NoClaimOverLiveWriter` violated | the reclaim ignores the lock |
| `_sab_localdeath_no_record` | violated | the reclaim ignores the record |
| `_sab_localdeath_own_entries` | violated | evidence from entries written after the lock was taken |
| `_sab_unlocked_writer_records` | violated | an incarnation without the lock writes the record (needs three incarnations) |
| `_sab_unsafe_writer_records` | violated | rule 5 dropped |
| `_witness_localdeathreclaim` | witness reached | the action fires |

Not modelled: the live memory clone (T3). It is a stated residual; no configuration covers it.

### 8.3 Acceptance of the model {#model-acceptance}

- `M/run_mountobserve.sh` and `M/run_mount.sh` list the new rows with their exact expectations and print
  `ALL EXPECTATIONS MET`.
- Every red row is red on the named invariant. Every green row with two incarnations ends with
  `0 states left on queue`.
- `_witness_triplet` either completes, or `M/CaCasMountCore_RESULTS.md` records the bound it reached. In
  the second case the induction step stays a stated limit and the owner decides whether that suffices.
- `M/CaMountObserveCore_RESULTS.md` and `M/CaCasMountCore_RESULTS.md` get a section with the table of rows,
  state counts, TLC version and the command.
- **Gate.** Commits W4 and W5 of [§9](#commits) are not written before these results are recorded.

## 9. Commit series {#commits}

Model commits land in the `master` worktree (the `M/` files are not in the pull-request tree). Code commits
land on the work branch over `a3967637843`.

| # | Subject | Contains | Tests |
|---|---|---|---|
| M1 | Model the local mount witness in `CaMountObserveCore` | [§8.1](#model-timing), configurations, runner rows, RESULTS section | TLC rows |
| M2 | Model the local-death reclaim in `CaCasMountCore` | [§8.2](#model-identity), configurations, runner rows, RESULTS section | TLC rows |
| W1 | Add the record codec of the CAS mount witness | `CA/Pool/CasMountWitness.{h,cpp}`: record struct, encode, decode. Nothing calls it | R-1 to R-7 |
| W2 | Add the CAS mount witness file with its exclusive lock | `MountWitness`, `LocalMountWitness`, `WitnessHost`, the process registry, the file-system list. Nothing opens it | L-1 to L-15 |
| W3 | Record every mount-slot write of the holder in the witness | the four sites of [§3.3](#record-writes); `PoolConfig::mount_witness`; `stopRecording` before the unsafe reclaim; cleared for decommission | H-1 to H-6, P-5, P-6 |
| W4 | Count the restart wait from the recorded renewal start | the credit rule in `claimMountAwaitingExpiry`; its log line; the shortened sleep | D-5 to D-8, D-13 (credit lines) |
| W5 | Reclaim after a settle window when the witness proves a local death | the local-death rule; the `claimMount` argument; `MountPriorState::UncleanLocalDeath`; the switch and the log lines | D-1 to D-4, D-9 to D-13, P-1 to P-4, P-7 |
| W6 | Open the mount witness for writable CAS disks by default | setting `cas_mount_lease_witness`; `ContentAddressedMetadataStorage::openPoolView` opens the witness; the integration module and the changed test | S-1, I-1 to I-5 |
| W7 | Document the CAS mount witness | the files of the list below | – |
| W8 | Drop the restart-wait lease pins from CAS integration tests | the pin table of [§7.6](#tests-integration) | the three modules, I-6 |

Rules of the series:

- **Every code commit builds and passes `CAS*`.** W1 to W5 add code that production does not reach until W6.
- A new `gtest_*.cpp` file needs `cmake .` in every build directory before the next build.
- Subjects are imperative and at most 72 characters. Trailers: `Signed-off-by` and `Co-Authored-By`.
- Comments in the code give reasons. They do not cite this document, the memo, task numbers or decision
  row numbers.

Documentation of W7:

| File | Change |
|---|---|
| `D/configuration.md` | the setting row; the restart-wait row of the lease-cost table (`:150`) and its paragraph (`:157-161`) |
| `D/architecture/mounts-and-leases.md` | a new section "Local mount witness" with the file, the rules and the limits; `MountPriorState` table (`:270-276`): the new value and the amended `UncleanObserved`; the state diagram; the crash paragraph (`:352-354`); the clone section (`:215-227`): the witness does not defend a memory clone |
| `D/operations/troubleshooting.md` | a row: a restart after a kill waits about 135 s; the logged reasons and what to do for each |
| `D/operations/migration.md` | upgrade notes (`:167-181`): the first unclean restart after an upgrade waits in full; the setting key and older builds |

User docs use plain words: no row numbers of this document, no task numbers. Every new header gets an
explicit `{#anchor}`.

## 10. Acceptance criteria {#acceptance}

| # | Criterion | Checked by |
|---|---|---|
| A1 | At default settings a server killed with `SIGKILL` and started at once on the same host reclaims its mount after a 21 s settle window and is ready inside a 60 s start budget | I-1; measurement M-1 |
| A2 | The same after a forced shutdown (`Will shutdown forcefully`) | I-2 |
| A3 | Every row D0 to D7 and D10 waits exactly the threshold it waits today and logs its reason | D-5, L-2, L-4 to L-7, L-9, L-12, L-13, P-2, P-3 |
| A4 | A copied witness file, another boot id, a missing record, a held lock and a token that changes to an unrecorded body never give the settle window | L-2, L-6, L-8, D-4, P-2 to P-4, I-4 |
| A5 | The reclaim after local death is conditional on the observed token | D-10 |
| A6 | A live holder is never reclaimed by local death | P-4, D-12; `_sab_death_without_lock` red |
| A7 | No stored object changes its bytes: the mount body, the owner and the epoch object encode as before | existing golden and round-trip tests of `CA/Formats`, unchanged and green |
| A8 | With `cas_mount_lease_witness = 0` no witness file is opened and the restart logs the full observation | I-3 phase 2; L-15 |
| A9 | The model results of [§8.3](#model-acceptance) are recorded before W4 and W5 | RESULTS sections |
| A10 | The seven-row lock experiment of the memo (section 8) is recorded for every file system on the list, on the kernel of a CI runner where possible, and the `f_type` of the data directory of the three CI lanes is recorded | the measurement report |
| A11 | `test_backup_restore_on_cluster::test_shutdown_waits_for_backup` passes on the first attempt without a lease pin | I-6 |
| A12 | The three configurations of the pin table lose their pin, or keep it with a named reason | W8 |
| A13 | Every code commit passes `CAS*`; the final tree passes the gates of the pull request (ASan `CAS*`, the no-S3 compile, the listed integration modules) | gate logs |
| A14 | CAS-480: the hard-restart regression jobs pass without a lease pin in the regression suites | CI of the pull request, if the regression data directory is on a listed file system (A10) |

Measurements, one report `docs/superpowers/reports/2026-11-13-cas-mount-lease-witness-measurements.md`.
Predictions, written before the runs:

| # | Prediction |
|---|---|
| M-1 | `SIGKILL` and an immediate start at the 120 s lease: `Ready for connections` within 21 s plus the normal start-up of that server; the log has the local-death line and no `~131000 ms` line |
| M-2 | The same with the witness file removed between kill and start: the `~131000 ms` line appears and the server is ready after about 135 s |
| M-3 | Kill, 200 s pause, start: a credit line with `~0 ms` remaining; no wait |
| M-4 | A healthy server: the witness file changes once per renewal period; the renewal duration (`system.cas_mounts`, the renewal log) does not change beyond noise |

A failed prediction is a finding. It is reported, not explained away.

## 11. Non-goals {#non-goals}

- **A self-fence on the forced-exit path** (`Server.cpp:4018`). It would cover a forced exit followed by a
  start on another host, and needs a call in shared code (P2).
- **The in-process self-remount** (`CasPool.cpp:1574-1585`). It keeps today's wait (P5).
- **The GC leader lease.** A restarted server may be up for a while before it leads GC again.
- **Parallel waits over several disks, and listening before the disks are mounted** (CAS-355 option b).
- **A shorter wait for a foreign observer, a shorter lease, any on-store format change.**
- **Rounding of the full wait to the poll step** (135 s for a 131 s threshold) stays.
- **Defence against a live memory clone** (P4).
- **Changes in `Altinity/clickhouse-regression`.**
- **A new column in `system.cas_mounts`, a new metric.**

## 12. Risks {#risks}

| Risk | Bound or mitigation |
|---|---|
| A listed file system does not exclude with `flock`, or reuses an inode identity | allowlist; the experiment of A10 per file system; the reclaim stays conditional; a renewing holder is still caught by the token (D-12); the setting turns the witness off |
| A memory clone of a running holder restarts and gets the settle window while the original is stalled | stated residual (P4); already unsupported |
| The settle window is 21 s, not the 16 s the memo announced | all three CI budgets (60 s, 60 s, 110 s) still hold; the owner may choose 16 s ([§13](#open-questions)) |
| The CI data directories are not on a listed file system, so CI stays red | the mount logs the file system; A10 records it before W6; overlayfs is added only after its experiment |
| The record write stalls on a slow local disk and delays a renewal | no `fsync`, one 4 KiB `pwrite` to the page cache; the renewal slack is 94 s; M-4 measures it |
| The witness file is created by another OS user (a tool run as root) and the server cannot open it | D1: full wait with a `Warning` that names the error |
| A test relied on the 131 s wait as a delay | I-3 keeps one full-wait phase; a search of `tests/` for the observation text found one test |
| A faster restart exposes tests that assume the restarted server leads GC at once | `test_cas_early_commit_restart` already has that race on the base (`triage.md` 3.1); it needs its own fix |
| `MountPriorState` gains a value; a switch outside `CasPool.cpp:899-910` misses it | `-Wswitch` fails the build; a search for the enum before W5 |
| The descriptor is never closed: one descriptor per CAS disk for the life of the process | bounded by the number of disks |
| Rule 2 or rule 3 is broken by a later change, and a record certifies a live process | L-2, L-10, L-11, L-15; `_sab_unlocked_writer_records` and `_sab_localdeath_own_entries` |

## 13. Open questions for the owner {#open-questions}

1. **Settle window: 21 s (`write reserve + poll step`) or 16 s (one write reserve, the memo's value) or 0?**
   This design uses 21 s. With 16 s a conditional write of the dead process can be 4.8 s younger at the
   reclaim than it can be today. Nothing in the code was found to depend on that; the per-kind protections
   of [§3.6](#late-landing) were verified by request shape, not by a full trace of recovery.
2. **Self-fence before the forced exit in `programs/server/Server.cpp`: yes or no (P2)?** This design: no.
3. **Witness on by default with `cas_mount_lease_witness` as the off switch (P3): confirm, and confirm the
   name.**
4. **Is the live memory clone acceptable as a documented residual (P4)?** "No" means the settle window
   cannot be used on platforms where such clones happen, and needs a platform signal that is not identified.
5. **Self-remount in a separate task (P5): confirm.**
6. **File systems: is overlayfs on the list if its experiment passes, and is the story allowed to merge
   with ext4 and xfs unverified if no runner offers them?** This design: a file system is listed only with
   a recorded experiment.
7. **GC leadership after a fast restart: is a follow-up task wanted that shortens the GC leader hand-back
   for a proven local death?** This design does not touch the GC lease.

## 14. Corrections to the memo {#memo-corrections}

| Memo | Code or analysis | Effect |
|---|---|---|
| The restart waits 131 s and start-up takes about 5 s | The loop sleeps whole polls: the reclaim is at 135 s (27 × 5 s), and at 40 s for the 30 s lease. Start to ready is 135.6 s and 41.3 s in the CI logs, so start-up is about 1 s (`CasServerRoot.cpp:1298`) | numbers of [§1](#decision); the shortened sleep of [§3.5](#arithmetic) |
| A request of the old holder is at least 13.8 s old at the reclaim, and one write reserve restores that | 13.8 s is the minimum of a one-envelope request. A conditional write reserves two envelopes and is at least 20.8 s old (`CasRequests.cpp:958-966`) | `S` = 21 s, not 16 s |
| The settle window counts from the lock | It counts from the first sighting of the token, the sample the loop already takes (`CasServerRoot.cpp:1286`). Later, so safe; it reuses the loop | [§3.5](#arithmetic) |
| The record keeps the last two attempts | A renewal with an unknown outcome followed by an in-process remount leaves three candidate bodies, four with its adoption | four entries |
| Every writable mount writes the record | Only a process that holds the lock, and not one that reclaimed under the unsafe knob, and not the decommission admin. Otherwise a record can certify a process that never held the lock | rules 2 and 5 |
| Lease arithmetic needs neither the lock nor the inode | The inode is not needed. The lock is required here: without it nothing is read, which keeps one rule and avoids reading a record a live holder is rewriting | D2 |
| The time-namespace offset gates the proposal | Only the credit compares clock values of two processes | D9, D10 |
| Model: check `GlobalSupersededWriterMakesNoMutation` in the twin extension | The twin configurations do not list that invariant; they check `OneWritableIncarnationPerUuid` (`M/CaCasMountCore_twins.cfg`) | [§8.2](#model-identity) |
| Forced exit: connections still open | The same block exits also for unfinished refresh tasks or background queries (`Server.cpp:4006`). Under TSan `safeExit` is `SYS_exit_group`, not `_exit` (`safeExit.cpp:18`) | fact table |
| `DisksApp.cpp:605-609` | The uuid load is at `:602-610` | reference |
| Integration: a clone with a rewritten boot id | Not buildable in the harness without editing the record; covered at unit level. The memo also did not list the existing test that asserts the observation line (`test_cas_mount_renewal_retry/test.py:620-681`) | [§7.6](#tests-integration) |
| The record is written with an attempt start | For a renewal it must be the same sample the holder's deadline uses, never an earlier one | [§3.3](#record-writes), `_sab_record_earlier_start` |

## 15. Not verified {#not-verified}

- Nothing was built or run. Every "after" number is computed.
- `flock` exclusion and inode identity on ext4, xfs and overlayfs. Only tmpfs was tested, by the memo, on
  one kernel.
- That the kernel releases the lock only after every thread of a killed process has stopped; that a device
  number is not reused while a file on the old device is open.
- Which file system holds the data directory in the three CI lanes.
- Whether `clickhouse disks` and `clickhouse local` mount a CAS disk writable.
- Whether the daemon's watchdog process is forked before disks start, so that it cannot hold the lock.
- Whether `Context::shutdown` reaches disks defined in table metadata.
- The recovery walk and the epoch seal, beyond the comment at `CasPool.cpp:880-897` and the user docs.
- Which files are namespace files, and what a successor caches of them.
- How long a kernel delivers the data of a closed socket; how long a store may queue a request.
- Which container runtimes set a time-namespace offset; boot id and clocks of sandboxed kernels.
- Whether any component assumes that a lease elapses between two epochs of one server.
- Whether a mount event of the first open reaches `system.cas_log`.
- How long a restarted server needs to lead GC again.
- Whether `CA/` builds on a platform without `CLOCK_BOOTTIME`.
- `test_cas_gcs`, `test_cas_replicated_relink` and the second reason of the `test_cas_shared_pool` pin.
- Whether `_witness_triplet` completes in TLC.

**What this design does not cover.** It is about one mount slot and the process that last wrote it, on one
host and one boot. It says nothing about GC leadership, about a foreign observer, or about pools that run
with `cas_unsafe_remount_no_delay`.

## 16. Left to the builder {#left-to-builder}

- **Parameter shape.** Whether `claimMount` and `claimMountAwaitingExpiry` take the witness, the authorized
  token and the own write reserve as separate trailing parameters or as one small struct. Constraint:
  existing call sites compile unchanged.
- **Registry shape.** A static map or a leaked singleton, as long as the handle of a path is created once
  per process and never destroyed.
- **Checksum function,** if `CityHash64` is awkward at that layer: any 64-bit hash already linked into
  `dbms`. The 16-hex-digit line stays.
- **`EINTR` and short writes** of the single `pwrite`: a short write is a failed record write.
- **Rate of the record-failure `Warning`.** Once per run of failures is the requirement.
- **Test names** may change for clarity. Suites keep the `CAS` prefix. The fixture and the red-before
  evidence of each row are binding.
- **I-2 mechanics.** How the test holds a connection open across `SIGTERM`; the old server must log
  `Will shutdown forcefully`.
- **Model encoding.** Names of constants and variables, and whether the third incarnation is a new constant
  or a generalisation of `TwinUuids`. The rows, their expectations and the invariants are binding.
