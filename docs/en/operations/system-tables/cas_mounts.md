---
description: 'System table containing the live mount and GC-health state of every server mounted onto a content-addressed (CAS) disk pool.'
sidebar_label: 'cas_mounts'
sidebar_position: 31
slug: /operations/system-tables/cas_mounts
title: 'system.cas_mounts'
doc_type: 'reference'
---

## Description {#description}

The `system.cas_mounts` table describes the mounts of every content-addressed (CAS) disk configured
on the node: this server's own mount and the mounts of the other servers sharing the pool. It exists
for incident-time diagnosis of leases, epochs, and GC leadership across a shared pool.

Per CAS disk the table has one row for this server's own mount (`is_local = 1`), built from memory
without a request to the object store, and one row per other mount slot listed from the pool
(`is_local = 0`), one attempt per request. `WHERE is_local = 1` sends no request, so it answers while
the store is down. A failed listing is one row with `listing_error` set.

A read-only CAS disk holds no mount slot. Its `is_local = 1` row carries only the disk, the
`server_root_id` and the local-only columns: the lease columns are zero (`server_uuid` all zeros,
`started_at` at the epoch) and `state` is empty. The slot under its `server_root_id`, if any, belongs
to another disk and is listed as an `is_local = 0` row.

## Columns {#columns}

- `disk` ([String](/sql-reference/data-types/string)) — Name of the content-addressed disk.
- `server_root_id` ([String](/sql-reference/data-types/string)) — Server root id owning the mount slot.
- `server_uuid` ([UUID](/sql-reference/data-types/uuid)) — UUID of the server incarnation holding the lease.
- `hostname` ([String](/sql-reference/data-types/string)) — Hostname recorded in the lease body.
- `process_id` ([UInt64](/sql-reference/data-types/int-uint)) — Process id recorded in the lease body.
- `writer_epoch` ([UInt64](/sql-reference/data-types/int-uint)) — Fenced writer epoch of the incarnation.
- `renewal_sequence` ([UInt64](/sql-reference/data-types/int-uint)) — Lease renewal sequence number.
- `started_at` ([DateTime64(3)](/sql-reference/data-types/datetime64)) — Time when the lease started.
- `expires_at` ([DateTime64(3)](/sql-reference/data-types/datetime64)) — Time when the lease expires.
- `min_active_build_sequence` ([UInt64](/sql-reference/data-types/int-uint)) — Oldest in-flight build sequence (`UINT64_MAX` means the mount said farewell).
- `gc_fenced` ([UInt8](/sql-reference/data-types/int-uint)) — `1` if GC fenced this slot out (terminal).
- `state` ([String](/sql-reference/data-types/string)) — One of `live`, `expired`, `terminated`, `fenced`, `corrupt`. On the local row, the state of the body this server last confirmed, not read from the store.
- `is_leader` ([Nullable(UInt8)](/sql-reference/data-types/nullable)) — `1` if this server's GC scheduler currently holds this disk's leadership lease.
- `pending_reclaim` ([Nullable(Int64)](/sql-reference/data-types/nullable)) — Cumulative two-phase deletion backlog observed by this process's GC on this disk (condemned entries minus executed exact-token deletes).
- `last_success_age_seconds` ([Nullable(UInt64)](/sql-reference/data-types/nullable)) — Seconds since this disk's GC last led a round (`0` if it has never led or GC is not running here).
- `wedged_namespace_count` ([Nullable(UInt64)](/sql-reference/data-types/nullable)) — Ref-append lanes currently wedged on this disk (an uncertain `PUT` exhausted its retry budget).
- `lifecycle` ([Nullable(String)](/sql-reference/data-types/nullable)) — This server's lifecycle for the disk, from the confirmed lease and the clock: `live`, `paused`, `identity_lost`, `vanished`, `closed`, `constructing` (never started) or `shutdown` (torn down). `NULL` on rows of other servers' mounts.
- `lifecycle_reason` ([Nullable(String)](/sql-reference/data-types/nullable)) — One word: `renewal_overdue`, `lease_expired` or `remounting` for `paused`, `replaced` for `vanished`, `forgotten` for `closed`, empty otherwise. `lifecycle || '(' || lifecycle_reason || ')'` reads e.g. `closed(forgotten)`. `NULL` on rows of other servers' mounts.
- `lifecycle_detail` ([String](/sql-reference/data-types/string)) — The full typed reason text naming the actual cause when not live: the vanish diagnosis (a data root replaced by a foreign pool, or decommissioned by `SYSTEM CAS FORGET` at a given time) or the identity-loss message. Empty when live. Empty on rows of other servers' mounts.
- `lifecycle_since` ([Nullable(DateTime)](/sql-reference/data-types/nullable)) — When this server entered the current non-live lifecycle state. For a pause from the lease clock, when the current reason's threshold was crossed (for `lease_expired`, the lease deadline). `NULL` when live, or when the state has no backing pool to date from. `NULL` on rows of other servers' mounts.
- `renewal_in_progress` ([Nullable(UInt8)](/sql-reference/data-types/nullable)) — `1` while this server renews its mount lease.
- `renewal_attempts` ([Nullable(UInt64)](/sql-reference/data-types/nullable)) — Conditional `PUT`s sent by the current renewal, or by the last one once it ended. Updated per attempt, so a renewal stuck in retries is visible before it ends.
- `last_renewal_error` ([Nullable(String)](/sql-reference/data-types/nullable)) — The last failed request of that renewal, as `<VERB> <key>: <class> <code> <message>`. `class` is `refused` when the store definitely refused the request (access denied, malformed request, entity too large), `transport` otherwise; `code` is the store's error name, such as `SlowDown`. `NULL` when no request failed.
- `is_local` ([UInt8](/sql-reference/data-types/int-uint)) — `1` for this server's own mount, built from memory without a request to the object store; `0` for a slot listed from the pool.
- `listing_error` ([Nullable(String)](/sql-reference/data-types/nullable)) — The error of a failed listing, on the one row it produces. `NULL` on every other row.

`lifecycle`/`lifecycle_reason`/`lifecycle_detail`/`lifecycle_since` are the SQL surface for
diagnosing an identity-lost or forgotten disk without reading server logs — see the
`IdentityLost`/`VanishedReplaced`/`VanishedForgotten` states on the
[mount-slot behavioral model](/antalya/cas/architecture/mounts-and-leases#mount-state-machines) for
what each lifecycle value means, and [`SYSTEM CAS FORGET`](/antalya/cas/operations/debugging#sql-forget)
for the command that produces `closed(forgotten)`.

:::note Local-only columns
`is_leader`, `pending_reclaim`, `last_success_age_seconds`, `wedged_namespace_count`,
`renewal_in_progress`, `renewal_attempts`, `last_renewal_error`, `lifecycle`, `lifecycle_reason`,
`lifecycle_detail` and `lifecycle_since` are process-local facts about *this* server. They are set
**only** on the `is_local = 1` row, and are `NULL` (`lifecycle_detail`: empty) on every row describing
another server's mount — stamping a local
health fact onto a peer's row would misread as "the peer is the GC leader" during an incident. To see the
peer's own view of these columns, query `system.cas_mounts` on that server.
:::

## Example {#example}

```sql
SELECT
    disk,
    server_root_id,
    state,
    writer_epoch,
    is_leader,
    pending_reclaim,
    last_success_age_seconds
FROM system.cas_mounts
WHERE is_local = 1
ORDER BY disk
FORMAT Vertical;
```

## See Also {#see-also}

- [`system.cas_gc_log`](/operations/system-tables/cas_gc_log) — per-round GC event log.
- [`system.cas_log`](/operations/system-tables/cas_log) — per-decision event log for the CAS garbage collector and writer.
- [`SYSTEM CAS GC RUN`](/sql-reference/statements/system#system-cas-gc-run) — run one GC round synchronously.
- [`SYSTEM CAS DROP POOL MEMBER`](/sql-reference/statements/system#system-cas-drop-pool-member) — permanently decommission a dead pool member's `server_root_id`.
