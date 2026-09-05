---
description: 'Design for making the CAS request engine cooperate with the S3 client's adaptive first-attempt timeouts: the engine reissue is presented to the transport as attempt N, so it runs under the full attempt budget on a fresh connection, and a first-attempt timeout is reissued at once as a connection-quality signal instead of being paced like a store fault.'
sidebar_label: 'CAS adaptive-timeout cooperation'
sidebar_position: 10
slug: /superpowers/specs/cas-adaptive-first-attempt-timeout-design
title: 'CAS reissues cooperate with the adaptive first-attempt timeout'
doc_type: 'design'
---

# CAS reissues cooperate with the adaptive first-attempt timeout {#cas-adaptive-first-attempt-timeout-design}

**Status:** DRAFT rev.2 (2026-09-05). rev.1 was reviewed by `codex` (`gpt-5.6-sol`, xhigh; record in
`tmp/pr2300-cicd-watch/review/codex_spec4.final.md`): 3 MAJOR (attempt arithmetic, LIST and the other
direct control requests not covered, precedence against spec 1), folded in below. Spec 4 of the R2
series. Implements the fix direction recorded in BACKLOG.md ("Reads time out too … Root cause found:
the S3 client's adaptive first-attempt timeout", user ruling 2026-09-04: keep the fuse, make the CAS
retry cooperate with it).

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

Scope: the CAS single-attempt control-plane requests — conditional PUT, exact GET, native HEAD,
conditional and bulk DELETE, LIST, and the sentinel probe. Streaming reads of blob bodies and
multipart/copy publication keep their existing retry models (the SDK's own retries bump the attempt
there already).

1. **The engine's attempt number reaches the transport.** `TransportAccess` (constructed only by
   `CasRequests`) carries `attempt_no` (1-based). `readLoop` passes its loop counter; `writeLoop`
   passes the count of attempts it has started (every physical attempt, hinted ones included);
   the sentinel-probe loop (`CasRequests.cpp` ~613) passes its own. The backend threads it into one
   small **control-request context** `{retry profile, attempt timeout, attempt_no}` that replaces the
   current `(profile, timeout_ms)` pair on the overloads this branch already owns:
   `readSettingsFor`/`conditionalWriteSettings` (new fields `ReadSettings::object_storage_attempt_number`,
   `WriteSettings::object_storage_attempt_number`, 0 = unset), `tryGetObjectMetadataWithNativeToken`,
   `removeObjectIfTokenMatches`, `removeObjectsIfExistUnderProfile`, and the profile-aware `iterate`.
   Each S3 request built on these paths sets the `clickhouse-request` attempt as
   `effective = (seed == 0 ? 1 : seed) + local − 1`, where `local` is the buffer's own 1-based counter
   (`ReadBufferFromS3::sendRequest` keeps its `attempt`; `WriteBufferFromS3::getPutRequest` has none
   and applies the seed directly; `S3IteratorAsync` applies the same seed to the initial
   `ListObjectsV2Request` AND to every rebuilt page request of one backend invocation). Unset seed
   leaves every non-CAS caller exactly as today (`[1, 2, …]`). `PocoHTTPClient` then sees attempt ≥ 2
   on every engine reissue and applies the full `attempt_timeout_ms` on a fresh connection, as it does
   for upstream retries; the contract relies only on "1 versus greater than 1" (the SDK's own retry
   numbering overcounts, `Client.cpp:857`, and is left alone). No change to `PocoHTTPClient` or to the
   fuse itself.
2. **A first-attempt timeout is a connection-quality answer.** One central matcher
   `isFirstAttemptFuseTimeout(e, attempt_no)`: `attempt_no == 1`, `S3Exception` with S3 error
   `NETWORK_CONNECTION`, NOT a spec-1 connect-failure hint (checked first: `connect timed out` belongs
   to spec 1), and the generic transport-timeout text. For a qualifying **read** (GET/HEAD/LIST/probe):
   re-check admission with `reservedFor(0, 1)` and reissue with no sleep. For a qualifying **write**:
   the exact settlement read still runs (the request may have been sent), then admission with
   `reservedFor(0, 2)` and a no-sleep reissue. This is a separate zero-pause gated reissue helper, not
   spec 1's `pauseFlat`; it does not advance the exponential-backoff index, so a following failure
   starts at `backoff(1)`. Backoff remains for attempt ≥ 2 and for every store-side fault.

Expected on the lane: one fuse trip costs 200 ms plus (for writes) one read; the double-hit case
(p50 962 ms) disappears; a LIST of a large prefix succeeds on attempt 2 under the full budget, which
is what the bootstrap residual check and the namespace janitor need.

## What this does not change {#unchanged}

The fuse stays for every first attempt (user ruling). The single-attempt client and its request
timeout stay. Spec 1's pre-send classification is orthogonal: a not-sent attempt is reissued without a
read; a fuse timeout is reissued after the read. The LIST page bounds of `aee2a4b6880`/`23b67e74a85`
stay; they reduce what a single attempt has to enumerate, this spec makes the second attempt able to
finish it.

## Tests {#tests}

- Request construction (`gtest_aws_s3_client.cpp`, `gtest_writebuffer_s3.cpp`, both ours): the
  `clickhouse-request` attempt header sequence is `[1, 2]` with an unset seed and `[2, 3]` with seed 2
  for reads; the PUT carries the seed; an unset seed adds nothing to an ordinary request.
- LIST: the async iterator with seed 2 sends every page request (initial and rebuilt) with attempt 2.
- Engine (`FakeClock`): a backend that fails attempt 1 with a real `S3Exception{NETWORK_CONNECTION,
  "Timeout"}` and commits attempt 2 → `Committed`, attempts `[1, 2]`, one settlement read for the
  write case, zero sleeps; attempts 1 and 2 failing → attempts `[1, 2, 3]` and exactly one sleep,
  after attempt 2. Read and LIST variants without the settlement read.
- Precedence: `connect timed out` on attempt 1 takes spec 1's path (hint), not this one.
- Bootstrap: a backend that fails every LIST whose `access.attempt_no == 1` and answers otherwise
  opens the pool (fails without propagation regardless of which invocation is first).
- Integration (`test_cas_gcs` fake, delay control extended to LIST/prefix requests, header captured):
  a LIST whose first byte is delayed by 300 ms is answered within one engine call, the second request
  carrying attempt 2; a delayed conditional PUT shows `CASRequestResolveRead` +1.

## Out of scope {#out-of-scope}

Changing the fuse values, disabling adaptive timeouts for CAS clients, and the catalog growth that
makes GETs of `ref_catalog` slow (backlog).
