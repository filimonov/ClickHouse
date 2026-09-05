---
description: 'Design for making the CAS request engine cooperate with the S3 client's adaptive first-attempt timeouts: the engine reissue is presented to the transport as attempt N, so it runs under the full attempt budget on a fresh connection, and a first-attempt timeout is reissued at once as a connection-quality signal instead of being paced like a store fault.'
sidebar_label: 'CAS adaptive-timeout cooperation'
sidebar_position: 10
slug: /superpowers/specs/cas-adaptive-first-attempt-timeout-design
title: 'CAS reissues cooperate with the adaptive first-attempt timeout'
doc_type: 'design'
---

# CAS reissues cooperate with the adaptive first-attempt timeout {#cas-adaptive-first-attempt-timeout-design}

**Status:** DRAFT rev.4 (2026-09-05). rev.1 and rev.2 were reviewed by `codex` (`gpt-5.6-sol`, xhigh;
records `codex_spec4.final.md`, `codex_spec4r2.final.md`; rev.2 = NO MAJOR). rev.4 folds the combined
four-spec review (`codex_cross.final.md`): its one MAJOR — the attempt budget did not cover TCP connect —
is answered by the attempt envelope (decision 3); its MINORs (read-loop counters, the sentinel, the
observability texts, the user docs) are folded below. Spec 4 of the R2
series. Implements the fix direction recorded in BACKLOG.md ("Reads time out too … Root cause found:
the S3 client's adaptive first-attempt timeout", user ruling 2026-09-04: keep the fuse, make the CAS
retry cooperate with it).

## The fuse and why every CAS attempt is a first attempt {#fuse}

With `s3_use_adaptive_timeouts` (default on) the first attempt of every request runs under
`TimeoutsForFirstAttempt` (`src/IO/ConnectionTimeouts.cpp`): 200 ms to the first byte for GET, POST,
PUT, HEAD, DELETE and PATCH, 500 ms for the rest of a GET body, 500 ms connect. `PocoHTTPClient` decides "first" as
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
   `readSettingsFor` and `conditionalWriteSettings` (the latter embeds `SingleAttempt` and the backend
   timeout today and becomes `{SingleAttempt, attempt_timeout_ms, access.attempt_no}`; new fields
   `ReadSettings::object_storage_attempt_number`, `WriteSettings::object_storage_attempt_number`,
   0 = unset), `tryGetObjectMetadataWithNativeToken`,
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
   fuse itself. `readLoop` keeps TWO counters: `attempt_no` (physical numbering, handed to the
   transport) and `ordinary_reissues` (the exponential-backoff index); a zero-pause reissue advances
   only the first.
2. **A first-attempt timeout is a connection-quality answer** — for GET, HEAD, LIST, DELETE and the
   conditional PUT; the sentinel probe gets attempt propagation only (its backend converts the
   exception into `Indeterminate` before the engine sees it, and the outer loop's ordinary backoff
   stays). One central matcher
   `isFirstAttemptFuseTimeout(e, attempt_no)`: `attempt_no == 1`, `S3Exception` with S3 error
   `NETWORK_CONNECTION`, NOT a spec-1 connect-failure hint (checked first: `connect timed out` belongs
   to spec 1; a hinted attempt remains ambiguous and is reissued without a preceding settle read), and the generic transport-timeout text. For a qualifying **read** (GET/HEAD/LIST; NOT the
   sentinel probe, whose backend turns every transport exception into `Indeterminate` at
   `CasObjectStorageBackend.cpp` ~755 and whose outer loop keeps its ordinary backoff):
   re-check admission with `reservedFor(0, 1)` and reissue with no sleep. For a qualifying **write**:
   the exact settlement read still runs (the request may have been sent), then admission with
   `reservedFor(0, 2)` and a no-sleep reissue. This is a separate zero-pause gated reissue helper, not
   spec 1's `pauseFlat`; it does not advance the exponential-backoff index, so a following failure
   starts at `backoff(1)`. Backoff remains for attempt ≥ 2 and for every store-side fault.
3. **The attempt envelope includes connect.** Today the single-attempt client clone
   (`S3ObjectStorage::getSingleAttemptClient`) overrides only `requestTimeoutMs`; `connectTimeoutMs`
   stays the disk's `connect_timeout_ms` (default 1000 ms, `S3AuthSettings`), and on attempt ≥ 2 the
   adaptive cap does not apply (`ConnectionTimeouts::getAdaptiveTimeouts` returns the configured
   values for non-first attempts). The engine nevertheless reserved exactly `attempt_timeout_ms` per
   envelope, so a slow connect could overrun the reservation — the R2 renewal attempt of 11 s is that
   overrun. Two changes, both in CAS-owned code (`src/IO` untouched, per the user ruling):
   the clone also sets `cfg.connectTimeoutMs = min(base connectTimeoutMs, attempt_timeout_ms)`, and the
   backend exposes `attemptEnvelopeMs() = attemptTimeoutMs() + min(connect_timeout_ms, attemptTimeoutMs())`,
   which `CasRequests::attempt_reservation_ms` (`CasRequests.cpp` ~245) and the writable-open
   validation `attempt + margin < TTL` (`ContentAddressedMetadataStorage.cpp` ~986, `CasPool.cpp` ~1004,
   ~1160) use instead of the bare attempt timeout. With defaults the envelope is 6 s (5 s + 1 s).
   What this does NOT make hard: `requestTimeoutMs` becomes Poco's per-socket-operation send and
   receive timeouts, so a response that trickles bytes slower than the timeout can still exceed the
   envelope. An absolute wall-clock deadline across connect/send/receive needs a `PocoHTTPClient`
   change and stays in the backlog (R2 item (a) is closed for connect and left open for trickle).

Expected on the lane: one fuse trip costs 200 ms plus (for writes) one read; the double-hit case
(p50 962 ms) disappears; a LIST of a large prefix succeeds on attempt 2 under the full budget, which
is what the bootstrap residual check and the namespace janitor need.

## What this does not change {#unchanged}

The fuse stays for every first attempt (user ruling). The single-attempt client and its request
timeout stay. Spec 1's connect-failure hint is orthogonal: a connect-failure-hinted attempt remains
ambiguous but is reissued without a preceding settle read; a fuse timeout is reissued after the read. The LIST page bounds of `aee2a4b6880`/`23b67e74a85`
stay; they reduce what a single attempt has to enumerate, this spec makes the second attempt able to
finish it.

## Observability and documentation {#observability-docs}

1. `CASRequestReissue` (`ProfileEvents.cpp` ~944) is incremented by the zero-pause helper too and its
   description becomes pacing-agnostic ("re-sent after a failed or ambiguous attempt; the pause before
   it is a jittered backoff, a flat pause, or none"). `CASRequestResolveRead`'s "every ambiguity costs
   one" is qualified: every conflict and every ambiguity that is not a connect-failure hint (spec 1).
   New `CASRequestFirstAttemptFuse` per matched timeout.
2. The `writeLoop` contract comment (`CasRequests.h` ~356, "settles every ambiguity by an exact read
   before it reports anything") is rewritten: every verdict is still proven by a read or by the
   reissue's own 2xx; a connect-failure-hinted attempt is reissued before its read.
3. `configuration.md`: the `cas_attempt_timeout_ms` row names the conditional PUT among the requests
   and states the envelope (`attempt timeout + min(connect_timeout_ms, attempt timeout)` per physical
   attempt; send/receive are per-socket-operation bounds); the `cas_lease_safety_margin_ms` row uses
   the envelope in its validation formula.

## Tests, in the order they are written and made to pass {#tests}

1. `S3RequestAttemptSeed.ReadHeaderSequence` (`gtest_aws_s3_client.cpp`): the scripted client records
   `getClickhouseAttemptNumber` per request; a `ReadBufferFromS3` with an unset seed sends `[1, 2]`
   across a local retry, with seed 2 sends `[2, 3]`. Fails until the field and the arithmetic exist.
2. `S3RequestAttemptSeed.PutHeadDeleteCarryTheSeed`: the single-part PUT, native HEAD, conditional
   DELETE and bulk DELETE carry the nonzero seed, and add no header for seed 0.
3. `S3RequestAttemptSeed.ListPagesCarryTheSeed`: the async iterator with seed 2 sends the initial and
   every rebuilt (`start_after`) page request with attempt 2.
4. `CASRequestsFuse.MatcherPrecedence`: the matcher is false for a spec-1 hint text, false on attempt
   ≥ 2, false for a non-timeout `NETWORK_CONNECTION`, true for a first-attempt generic `Timeout`.
5. `CASRequestsFuse.FirstAttemptTimeoutReissuesWithoutSleep` (`FakeClock`): a backend failing attempt 1
   with a real `S3Exception{NETWORK_CONNECTION, "Timeout"}` and committing attempt 2 → `Committed`,
   attempts `[1, 2]`, one settlement read for the write case, zero sleeps; read and LIST variants with
   no settlement read; attempts 1 and 2 failing → attempts `[1, 2, 3]` and exactly one sleep, after
   attempt 2.
6. `CASRequestsFuse.GatesRefuseTheZeroPauseReissue`: deadline and fence refusals at the new gate
   return today's `GaveUp` verdicts; `Retry::once()` performs no second attempt.
6a. `CASRequestsFuse.ReadLoopZeroPauseKeepsTheBackoffIndex`: a read failing attempt 1 with the fuse and
   attempt 2 with an ordinary transport fault sleeps exactly `backoff(1)` before attempt 3 while the
   transport sees `[1, 2, 3]`. Fails while `readLoop` has one counter.
6b. `S3SingleAttemptClient.ConnectTimeoutIsCappedByTheAttemptBudget`
   (`src/Disks/tests/gtest_cas_s3_client_profile.cpp`, shared with spec 3): a base configuration with
   `connectTimeoutMs = 20000` and an attempt timeout of 5000 yields a clone with `connectTimeoutMs ==
   5000` and `requestTimeoutMs == 5000`; a base with 1000 keeps 1000.
6c. `CASRequests.ReservationIsTheEnvelope`: `attempt_reservation_ms == attemptEnvelopeMs()` for a
   backend reporting attempt 5000 / connect 1000 (6000) and attempt 5000 / connect 20000 (10000);
   the renewal twin (`gtest_cas_pool.cpp`) with `connect_timeout_ms > cas_attempt_timeout_ms` and a
   backend that consumes the whole envelope per attempt stops issuing before the lease cutoff
   (virtual clock) instead of overrunning it; the writable-open validation refuses `envelope + margin
   >= TTL` with the envelope, not the bare attempt timeout, in its message.
7. `CASBootstrapOrdering.ResidualListSucceedsOnTheSecondAttempt`: a backend that fails every LIST
   whose `access.attempt_no == 1` and answers otherwise opens the pool (fails without propagation
   regardless of which invocation is first).
8. `CASSentinelProbe.AttemptNumberPropagates`: the probe's read carries the loop's attempt number
   (propagation only).
9. Integration (`test_cas_gcs` fake, delay control extended to LIST/prefix requests, `clickhouse-request`
   header captured, delay applied to the FIRST matching request only so the fault is deterministic):
   a LIST whose first byte is delayed by 300 ms is answered within one engine call, the second request
   carrying attempt 2; a conditional PUT delayed the same way shows the request order
   PUT(1) → GET → PUT(2) and `CASRequestResolveRead` +1.

## Out of scope {#out-of-scope}

Changing the fuse values, disabling adaptive timeouts for CAS clients, and the catalog growth that
makes GETs of `ref_catalog` slow (backlog).
