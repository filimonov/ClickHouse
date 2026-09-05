---
description: 'Design for making the CAS request engine cooperate with the S3 client's adaptive first-attempt timeouts: the engine reissue is presented to the transport as attempt N, so it runs under the full attempt budget on a fresh connection, and a first-attempt timeout is reissued at once as a connection-quality signal instead of being paced like a store fault.'
sidebar_label: 'CAS adaptive-timeout cooperation'
sidebar_position: 10
slug: /superpowers/specs/cas-adaptive-first-attempt-timeout-design
title: 'CAS reissues cooperate with the adaptive first-attempt timeout'
doc_type: 'design'
---

# CAS reissues cooperate with the adaptive first-attempt timeout {#cas-adaptive-first-attempt-timeout-design}

**Status:** DRAFT rev.1 (2026-09-05). Spec 4 of the R2 series. Implements the fix direction recorded
in BACKLOG.md ("Reads time out too … Root cause found: the S3 client's adaptive first-attempt
timeout", user ruling 2026-09-04: keep the fuse, make the CAS retry cooperate with it).

## The fuse and why every CAS attempt is a first attempt {#fuse}

With `s3_use_adaptive_timeouts` (default on) the first attempt of every request runs under
`TimeoutsForFirstAttempt` (`src/IO/ConnectionTimeouts.cpp`): 200 ms to the first byte for GET, POST,
PUT and HEAD, 500 ms for the rest of a GET body, 500 ms connect. `PocoHTTPClient` decides "first" as
`clickhouse_attempt == 1 && sdk_attempt == 1` (`PocoHTTPClient.cpp:487`). Upstream's own retry bumps
the ClickHouse attempt number (`Client.cpp:857`, `ReadBufferFromS3.cpp:568`), so its second try runs on
a fresh connection with the full timeouts. The CAS single-attempt client disables SDK retries and the
engine reissues by issuing a brand-new request: every reissue is attempt 1 again, the 200 ms fuse
applies again, and `attempt_timeout_ms` (5 s) never applies to any attempt at all.

Measured cost: the stateless CAS lane logs ~570 fuse trips in two hours, almost all `ListObjectsV2`
(a large prefix cannot start answering in 200 ms on rustfs); the startup residual LIST of
`cas_alter_attach_2` hit the fuse 30–33 times in a row and refused to start (R1); the namespace
janitor's root LIST trips it every GC round (R7, ~35/min). The catalog GET case is in the backlog entry.

## Decision {#decision}

Two changes, both in the CAS engine plus one small settings seam that is already ours:

1. **The engine's attempt number reaches the transport.** `TransportAccess` (constructed only by
   `CasRequests`) carries `attempt_no` (1-based, the engine's `attempts_started` for this logical
   write or read). `ObjectStorageBackend` copies it into a new `ReadSettings::object_storage_attempt_number`
   / `WriteSettings::object_storage_attempt_number` (both structs already carry the CAS retry profile
   and attempt timeout, `ReadSettings.h:165-170`, `WriteSettings.h:82-92`). `ReadBufferFromS3::sendRequest`
   seeds `setClickhouseAttemptNumber(req, base + attempt)` from it (today it passes its own `attempt`,
   which is 1 for CAS reads because `max_single_read_retries` is 1), and `WriteBufferFromS3::getPutRequest`
   seeds the same for the single-part PUT. `PocoHTTPClient` then sees attempt ≥ 2 on every engine
   reissue and applies the full `attempt_timeout_ms` on a fresh connection, exactly as it does for
   upstream retries. No change to `PocoHTTPClient` or to the fuse itself.
2. **A first-attempt timeout is a connection-quality answer.** In `writeLoop` and `readLoop`, an
   attempt whose `attempt_no == 1` fails with the transport's timeout text (`Timeout` from
   `NETWORK_CONNECTION`, the fuse) is reissued without backoff (the flat pause of spec 1), still under
   the deadline and fence gates; it stays ambiguous for the write path (the request may have been
   sent), so the settle read still runs before the reissue as today. Backoff remains for attempt ≥ 2
   and for every store-side fault. Expected on the lane: one fuse trip costs 200 ms plus one read, the
   double-hit case (p50 962 ms) disappears, LIST of a large prefix succeeds on attempt 2 under the
   full budget.

## What this does not change {#unchanged}

The fuse stays for every first attempt (user ruling). The single-attempt client and its request
timeout stay. Spec 1's pre-send classification is orthogonal: a not-sent attempt is reissued without a
read; a fuse timeout is reissued after the read. The LIST page bounds of `aee2a4b6880`/`23b67e74a85`
stay; they reduce what a single attempt has to enumerate, this spec makes the second attempt able to
finish it.

## Tests {#tests}

- Transport seam (`gtest_aws_s3_client.cpp`, already ours): a request built with
  `object_storage_attempt_number = 2` reaches `PocoHTTPClient` with `first_attempt == false` (observable
  through the `S3LatencyType` / timeouts chosen); with 1 or unset, `first_attempt == true`.
- Engine: a backend that fails attempt 1 with the fuse text and commits attempt 2 → `Committed` with
  one settle read, no backoff sleep (virtual clock), attempt 2 issued with `attempt_no == 2`.
- Engine: a backend that fails attempts 1 and 2 with the fuse text → attempt 2 was paced (backoff),
  proving the no-backoff rule is first-attempt-only.
- Bootstrap: the residual LIST over a backend whose first LIST attempt times out and whose second
  succeeds opens the pool (today: `Indeterminate` under `Retry::standard` only because every attempt
  is a first attempt at the transport).
- Integration (`test_cas_gcs` fake or the rustfs lane): a fake that delays the first byte of a LIST by
  300 ms answers within one engine call; `CASRequestResolveRead` accounts one read per fuse trip.

## Out of scope {#out-of-scope}

Changing the fuse values, disabling adaptive timeouts for CAS clients, and the catalog growth that
makes GETs of `ref_catalog` slow (backlog).
