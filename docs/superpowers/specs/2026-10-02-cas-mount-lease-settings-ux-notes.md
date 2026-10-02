---
description: 'Problem statement and options: the CAS mount lease settings name mechanism internals, so the configured 30 s lease stops writes after 14 s and the operator cannot tell why. Records the arithmetic, the timeouts behind it, five ways to make the settings say what happens, and the answers to three open questions. No decision yet.'
sidebar_label: 'Mount lease settings: UX notes'
sidebar_position: 104
slug: /superpowers/specs/cas-mount-lease-settings-ux-notes
title: 'CAS mount lease settings do not say what happens'
doc_type: 'guide'
---

# CAS mount lease settings do not say what happens {#cas-mount-lease-settings-ux}

An operator reads `cas_mount_lease_ttl_ms = 30000` and expects writes to survive 30 s without the
object store. They stop after 14 s, and `system.cas_mounts` shows `live` for 16 more seconds. The
numbers follow from four settings through arithmetic that no setting names. This note records the
problem, why the numbers are what they are, the timeouts behind them, and five options. Nothing is
decided.

Related: [mount renewal without a lease deadline](2026-10-01-cas-mount-renewal-without-lease-deadline-design.md),
[issue #2431](https://github.com/Altinity/ClickHouse/issues/2431).

## What the operator sets and what happens {#set-vs-happens}

Operator-facing settings, with defaults (`docs/en/antalya/cas/configuration.md`,
`CA/ContentAddressedSettings.cpp`; `CA` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed`):

| Setting | Default | What the text says | What it controls in fact | Same on all servers? |
|---|---|---|---|---|
| `cas_mount_lease_ttl_ms` | 30000 | how long a lease is valid | when another server may act; writes stop 16 s earlier | yes, changed only with the pool stopped |
| `cas_mount_renew_period_ms` | 10000 | interval between renewals | renewal cadence, and part of the takeover wait | yes |
| `cas_attempt_timeout_ms` | 5000 | budget of one HTTP attempt | socket send and receive timeout of control-plane requests; twice over, how early writes stop | no |
| `cas_lease_safety_margin_ms` | 2000 | "startup-only margin" | part of every write admission | no |

What follows from them, and where:

| Quantity | Formula | Default | A setting? |
|---|---|---|---|
| Attempt envelope | `attempt + 2 × connect cap`, cap = `min(connect_timeout_ms, attempt)` (`CasRequestBudget::attemptEnvelopeMs`) | 7 s | no |
| Write reserve | `2 × envelope + margin` (`CasOperation::writeLoop`, `CasMountRuntime::budgetAdmits`) | 16 s | no |
| Writes stop after the last successful renewal started | `TTL − reserve` | 14 s | no |
| Outage tolerated before writes stop | `TTL − period − reserve` | 4 s | no |
| Lease expires | `TTL` | 30 s | yes |
| A restarted server may reclaim the slot | `TTL + TTL/20 + period/2` (`mountObservationThresholdMs`) | 36.5 s | no |
| GC on another server may fence the slot | `TTL + TTL/20 + period` | 41.5 s | no |

One outage on that scale. The last renewal started at 0 s; the store fails from 10 s on.

| Time | Event | Lease left | Writes | `system.cas_mounts` |
|---|---|---|---|---|
| 0 s | renewal succeeds | 30 s | admitted | `live` |
| 10 s | renewal due; requests fail, the renewal retries about once a second | 20 s | admitted | `live` |
| 14 s | lease left drops to the reserve | 16 s | refused, "no lease budget" | `live` |
| 30 s | lease expires | 0 | refused | `not_live`, `lease_expired` |
| 36.5 s | a restarted server could reclaim the slot | | | |
| 41.5 s | GC elsewhere could fence the slot | | | |

When a renewal lands, writes resume under the same `writer_epoch`. Nothing is fenced and nothing is
remounted by the outage itself.

The mismatches:

1. The lease setting says 30 s; writes stop at 14 s.
2. From 14 s to 30 s the mount reads `live` while every write is refused.
3. The tolerated outage (4 s) is not a setting. One second more of `cas_attempt_timeout_ms` takes two
   seconds off it.
4. `cas_lease_safety_margin_ms` is described as startup-only and takes part in every admission.
5. With `connect_timeout_ms = 0` on the disk the connect cap becomes the attempt timeout, the envelope
   becomes 15 s, and the reserve 32 s: more than the lease. The open is refused with an arithmetic
   message.
6. Two settings must match on every server of a pool. Nothing checks it; a server with a shorter
   threshold can fence a healthy peer.

## Why writes stop before the lease ends {#why-reserve}

A server must not have a write in flight when its authority ends. After the deadline, plus an allowance
for clock drift, an observer is entitled to act: GC drops the protection of the server's unfinished
work, or a successor takes the slot. A ref-log `PUT` that lands after that leaves a reference to blobs
GC may delete, or a second writer on one server root.

So a write is started only if it can finish before the deadline:

| Part of the reserve | Purpose | Needed for safety? |
|---|---|---|
| 7 s, first envelope | the `PUT` ends or times out before the deadline | yes |
| 7 s, second envelope | the read that settles an unclear `PUT` also ends before the deadline, so the outcome is known | no, by a code audit ([answers](#answers)). It buys a settled outcome and, by accident, 7 s of slack for a `PUT` that overruns its envelope |
| 2 s, margin | scheduling and clock granularity | belt |

Without any reserve: a `PUT` sent at 29.9 s can be in flight until about 37 s. A successor may act from
about 34.8 s and GC from about 39.5 s at the largest allowed drift, so the `PUT` can land after either.

ZooKeeper has the same shape. Its Java client pings at one third of the session timeout, treats the
connection as lost at two thirds (`Disconnected`: no writes, session still alive), and the server
expires the session at the full timeout (`Expired`). The CAS mount has the same three instants and
no name or setting for the second.

## Timeouts {#timeouts}

Only control-plane requests of CAS use the 5 s timeout. They run on a separate S3 client with one
attempt per call (`S3ObjectStorage::getSingleAttemptClient`); the CAS request engine retries.

| Requests | Connect | Send / receive | Retries |
|---|---|---|---|
| CAS control plane: lease, ref log, `gc/state`, checkpoints (conditional writes and the reads that settle them) | `min(disk connect timeout, 1 s)` | 5 s (`cas_attempt_timeout_ms`) | none in the SDK; the CAS engine retries |
| Data (blobs), ordinary ClickHouse S3 client | 1 s (`s3_connect_timeout_ms`) | 30 s (`s3_request_timeout_ms`) | up to 500 attempts |
| Azure in ClickHouse | 1 s (`azure_connect_timeout_ms`) | 30 s (`azure_request_timeout_ms`) | 10, backoff 10 ms to 1 s |

A writable CAS mount exists on S3 and GCS only (`checkConditionalWriteSingleAttemptSupport`).

SDK defaults, from `contrib`:

| SDK | Connect | Request timeout | Retries |
|---|---|---|---|
| AWS SDK for C++ | 1 s | 3 s (`requestTimeoutMs`); no limit for the whole request (`httpRequestTimeoutMs = 0`) | 10 retries, exponential backoff from 25 ms; 3 attempts in `standard` mode |
| Azure SDK for C++ | 5 min (curl transport) | none found | 3 retries, 800 ms to 60 s |

ClickHouse replaces both with its own HTTP client and the values of the first table.

The request timeout bounds one socket operation, not the request: see [answers](#answers), question 2.

## What the current work changes {#current-work}

| Before | After the mount-renewal change |
|---|---|
| a renewal gave up 4 s after it was due; the give-up fenced the mount and forced a remount (36.5 s or more) | the renewal retries until the store answers; nothing is fenced by an outage |
| an outage of 8 s cost a remount cycle | it costs at most 4 s of refused writes |
| silent | `lifecycle_reason = 'lease_expired'`, the counter `CASMountLeaseExpired`, one `WARNING` at the expiry |

Write admission, the reserve and the settings are unchanged. The table above is today's behaviour.

## Options {#options}

Common to every option, and worth doing whichever is chosen:

- One line at mount, in words: renewal every 10 s; writes stop 14 s after the last renewal; the lease
  expires at 30 s; another server may take over after 36.5 s.
- `system.cas_mounts` shows the instants: last renewed, writes stop at, lease expires at.
- The state changes when writes stop, not 16 s later: `lease_not_renewed` from the write stop,
  `lease_expired` from the expiry.
- The refusal names the cause and the setting, not "no lease budget".
- Config validation in words: what would happen, not which sum exceeds which.
- Fix the text of `cas_lease_safety_margin_ms`.

| Option | The operator sets | Writes stop (defaults) | Takeover | Protocol change | Cost |
|---|---|---|---|---|---|
| A. Keep | four internals | 14 s | 36.5 s | no | the problem stays; only the common items help |
| B. One envelope | four internals | 21 s | 36.5 s | writer side only | proof that the settling read needs no reserve |
| C. Reserve on the observer | lease length = write stop | 30 s | about 43.5 s | writer and observer | new safety argument and models; old observers must not meet new writers: two releases or a stopped pool |
| D. Name the outcome | `cas_mount_writes_stop_after_ms`; the lease length is derived (`+ reserve`) | whatever is set; 14 s keeps today's lease | unchanged at 14 s | no | one more name; a larger value lengthens the lease, which is a stopped-pool change |
| E. Tick | `cas_tick_ms`; everything is a whole number of ticks | 20 s with a 5 s tick | 36.5 s | no | the request timeout becomes part of the tick |

### E. One tick, everything in ticks {#option-tick}

ZooKeeper's `tickTime` model: one base unit, every other time a multiple of it.

`cas_tick_ms` is the longest one control-plane request may take, connect and its share of the margin
included. The rest is thirds of the lease, as in ZooKeeper:

| Quantity | Ticks | With `cas_tick_ms = 5000` | Today |
|---|---|---|---|
| one request | 1 | 5 s: connect 2 × 0.5 s, send and receive 3 s, margin share 1 s | 7 s + margin |
| renew every | 2 | 10 s | 10 s |
| writes stop when not renewed for | 4 | 20 s | 14 s |
| lease length in the store | 6 | 30 s | 30 s |
| outage tolerated before writes stop | 2 | 10 s | 4 s |
| a restarted server may reclaim | 6 × 1.05 + 1 | 36.5 s | 36.5 s |
| GC may fence | 6 × 1.05 + 2 | 41.5 s | 41.5 s |

Properties:

- The lease length and the renewal period keep today's values, so the two settings that must match
  across a pool do not change and no pool has to be stopped.
- The reserve is two ticks: the write and the read that settles it. The operator never sees it as
  arithmetic; they see "writes stop at 4 ticks".
- The cost is a tighter request: 3 s send and receive instead of 5 s (the AWS SDK default is 3 s) and a
  0.5 s connect instead of 1 s. To be checked against measured latency of control-plane requests
  before choosing a default.
- A slower store is handled by one number: a larger tick scales everything, and then the pool is
  stopped for the change because the lease length moves.
- The multipliers can stay fixed (2, 4, 6) or be settings (`cas_lease_renew_ticks`,
  `cas_lease_ticks`), as ZooKeeper has `initLimit` and `syncLimit`.

D and E combine: the tick is what the operator sets, the outcome names are what the operator reads.

## Answers to the first three questions {#answers}

Three read-only studies, in full in
[the studies file](../cas/2026-10-02-mount-lease-settings-studies.md). Code was read at `e1c38ca7478`;
nothing was built or run.

### 1. The second envelope is not a safety matter {#answer-second-envelope}

- With one envelope, an unclear `PUT` near the deadline ends as a give-up that says "sent, outcome not
  observed". The engine returns that same value today on three paths, one pinned by a test
  (`CASRequestsConnectHint.GatesRefuseTheReissue`).
- All ten consumers of a give-up were read: none acknowledges the write, none treats it as "did not
  land". A ref append becomes a wedged lane that keeps the exact key and bytes; it is settled later by a
  conditional create, never by a bare read.
- `remove`, batch delete and blob publish already reserve one envelope. No observer uses an envelope.
- Cost of one envelope: a `PUT` that goes unclear 9 to 16 s before the deadline wedges its lane, where
  today it is refused cleanly.
- Not proven: late landing of conditional writes outside the ref ledger (checkpoints, catalog, blob
  metadata) and callers above the CAS directory. The TLA+ models have no request duration.
- The second envelope has a side effect that matters: today every conditional `PUT` ends at least 9 s
  before the deadline; with one envelope, 2 s. Those 7 s are the only slack against the next answer.

### 2. One attempt is not bounded by a wall-clock deadline {#answer-request-deadline}

The connect, send and receive timeouts each bound one socket operation; every `recv` or `send` restarts
the timeout (`SocketImpl::receiveBytes`, `sendBytes`). No deadline for the whole attempt exists in
`PocoHTTPClient`, the AWS SDK path, `S3::Client`, the CAS backend or the CAS request engine;
`CasOperation::fits` checks the deadline only before an attempt is issued. GCS takes the same path.

| Case | Duration of one attempt |
|---|---|
| one phase stalls completely | up to 7 s: the envelope holds |
| several phases each near its timeout | 12 s for a `GET` with a stalled body; 17 s or more for a `PUT` with `Expect: 100-continue` |
| a response that trickles, one byte every 4 s | unbounded |
| `307` redirects or the `S3::Client::doRequest` loop | up to 11 requests in one attempt |
| client-side throttler (`s3_max_put_rps`, off by default) | unbounded |
| credentials fetched at signing (STS, GCP OAuth) | not bounded by the envelope |

The reserve rests on "one attempt ends within one envelope". The stall cases stay inside the reserve
plus the observer's margin by a small amount; the others do not. Tracked as CAS-343
(`docs/superpowers/cas/backlog`). The smallest fix found: one absolute deadline per attempt, set by the
CAS backend, to which the HTTP session clamps every connect, send and receive. It touches shared code
(`base/poco`, `src/IO/S3`).

### 3. Measured latency does not decide the request timeout yet {#answer-latency}

No readable source has per-request latency of control-plane requests with a tail.

| Store | Typical | Bad moments |
|---|---|---|
| AWS S3, demo stand, one week, 14.8 million conditional writes | `GET` mean 25 ms | 0.05 % of writes ended unresolved (timeout, conflict, connection loss and 5xx together) |
| MinIO in CI under ASan | mean 8 to 10 ms; p99.9 of the per-second mean 148 ms | one write took 10.7 s during a host stall |
| GCS over a WAN | `GET` 50 to 100 ms, write 150 to 400 ms, connect 40 to 85 ms | a ten-minute storm of connect timeouts; a two-minute DNS outage |

Typical requests are far below 3 s. Every observed stall lasted 5 to 12 s, which neither 3 s nor 5 s
survives, so this data shows no difference between the two. The tail of the connect time is not
measured anywhere. To decide: read the `S3FirstByte` and `DiskS3FirstByte` histograms and the request
spans on the demo stand for a week or more, or add a duration histogram for control-plane requests.

## Open questions {#open-questions}

1. Late landing of conditional writes outside the ref ledger, before the reserve is reduced (answer 1).
2. Where the per-attempt deadline lives: in the HTTP session, in the S3 client, or in both (CAS-343).
3. A week of per-request latency from the demo stand (answer 3).
4. Nothing verifies that the servers of a pool agree on the lease length and the renewal period.
   Should the mount body carry them, so that a mismatch is refused at mount?
5. Defaults: keep the 30 s lease (no stopped-pool change) or lengthen it.

## Recommendation {#recommendation}

In this order:

1. CAS-343 first. Until one attempt is bounded, "a request ends within one envelope" is a hope, the
   second envelope is the only slack, and neither the reserve nor a tick may shrink.
2. The common items for what the operator reads: they need no protocol change and remove the worst
   mismatch (`live` while writes are refused).
3. E for what the operator sets, with the reserve kept on the writer's side. The tick's composition
   follows from answers 1 to 3: with a bounded attempt and one envelope proven enough, the reserve is
   one tick; otherwise two.

It is a separate change after the renewal work, because it touches write admission and reporting that
the renewal work restructures.
