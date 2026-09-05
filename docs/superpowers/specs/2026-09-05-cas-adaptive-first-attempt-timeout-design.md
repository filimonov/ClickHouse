---
description: 'Design for making the CAS request engine cooperate with the S3 client's adaptive first-attempt timeouts: the engine reissue is presented to the transport as attempt N, so it runs under the full attempt budget on a fresh connection, and a first-attempt timeout is reissued at once as a connection-quality signal instead of being paced like a store fault.'
sidebar_label: 'CAS adaptive-timeout cooperation'
sidebar_position: 10
slug: /superpowers/specs/cas-adaptive-first-attempt-timeout-design
title: 'CAS reissues cooperate with the adaptive first-attempt timeout'
doc_type: 'design'
---

# CAS reissues cooperate with the adaptive first-attempt timeout {#cas-adaptive-first-attempt-timeout-design}

**Status:** DRAFT rev.8 (2026-09-05; rev.8 folds round 4 `codex_cross_r4.final.md`: test 6e exercises the
initial-zero connect timeout at the snapshot seam, the control-request context names the cap, the doc
formulas use `attempt + 2 × cap` with zero normalized) — rev.7 rev.7 records the user's ruling on the `src/IO` constraint, closing round 3's MAJOR 1) — rev.6 rev.6 folds round 3 `codex_cross_r3.final.md`: a zero connect timeout is
normalized, the TLS handshake's second connect interval is budgeted (`attempt + 2 × cap`), an end-to-end wiring
test, strict horizon checks; the `src/IO` question of its MAJOR 1 is the user's call and is recorded in the
constraints paragraph below) — earlier: rev.5 rev.5 folds the fold's re-review `codex_cross_r2.final.md`: the
envelope becomes one authoritative budget value used at every lease-arithmetic site, its connect cap is
frozen at open so a client reload cannot widen it, a zero attempt timeout is refused, the residual list
is complete, the observability wording is operational). rev.1 and rev.2 were reviewed by `codex` (`gpt-5.6-sol`, xhigh;
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
   small **control-request context** `{retry profile, attempt timeout, connect-timeout cap, attempt_no}`
   that replaces the current `(profile, timeout_ms)` pair on the overloads this branch already owns:
   `readSettingsFor` and `conditionalWriteSettings` (the latter embeds `SingleAttempt` and the backend
   timeout today and becomes `{SingleAttempt, attempt_timeout_ms, connect_timeout_cap_ms, access.attempt_no}`; new fields
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
   fuse itself. **Constraint, as ruled by the user on 2026-09-05:** the "no `src/IO`" rule names the
   four transport components — `PocoHTTPClient`, `S3Exception` (`S3Common`), `ConnectionTimeouts`,
   `S3AuthSettings` — which stay untouched; the ADDITIVE edits this decision needs are permitted and
   are exactly these: two fields each in `ReadSettings.h` and `WriteSettings.h`
   (`object_storage_attempt_number`, `object_storage_connect_timeout_cap_ms`), one line in
   `ReadBufferFromS3::sendRequest`, three lines in `WriteBufferFromS3::getPutRequest`, and the inline
   `seededAttemptNumber` helper in `S3/Requests.h`. The branch already added
   `object_storage_retry_profile` / `object_storage_attempt_timeout_ms` to the same two settings
   structs, so this follows the established seam. `readLoop` keeps TWO counters: `attempt_no` (physical numbering, handed to the
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
3. **The attempt envelope includes connect, and there is exactly one of it.** Today the single-attempt
   client clone (`S3ObjectStorage::getSingleAttemptClient`) overrides only `requestTimeoutMs`;
   `connectTimeoutMs` stays the disk's `connect_timeout_ms` (default 1000 ms, `S3AuthSettings`), and on
   attempt ≥ 2 the adaptive cap does not apply (`ConnectionTimeouts::getAdaptiveTimeouts` returns the
   configured values for non-first attempts). The engine nevertheless reserved exactly
   `attempt_timeout_ms` per envelope, so a slow connect could overrun the reservation — the R2 renewal
   attempt of 11 s is that overrun. All changes are in CAS-owned code plus the additive `src/IO`
   edits listed under decision 1:
   - `CasRequestBudget` gains `std::optional<uint64_t> connect_timeout_cap_ms` and
     `attemptEnvelopeMs() = attempt_timeout_ms + 2 × connect_timeout_cap_ms` (saturating; `nullopt`
     contributes nothing). TWO cap intervals, not one: Poco gives the TLS handshake a fresh connect
     timeout after the TCP connect (`SecureSocketImpl.cpp` ~156, ~223), so an HTTPS attempt can spend
     `2 × cap` before any request I/O; the formula is scheme-agnostic on purpose (conservative for
     plain HTTP). The cap is FROZEN at writable open by `ContentAddressedMetadataStorage` from the S3
     client the storage was created with (`IObjectStorage::tryGetS3StorageClient()` →
     `getClientConfiguration().connectTimeoutMs`): a configured `connect_timeout_ms == 0` means
     "unbounded" to Poco and to the adaptive saturation (`ConnectionTimeouts.cpp` ~33), so it is
     normalized to `attempt_timeout_ms`; otherwise `min(connect_timeout_ms, attempt_timeout_ms)`. A
     storage with no S3 client freezes `nullopt`. A later `applyNewSettings` that raises
     `connect_timeout_ms`, or sets it to zero, cannot widen the envelope: the frozen cap is what every
     request and every clone carries.
   - `validateCasRequestBudget` refuses `attempt_timeout_ms == 0` for a writable Native mount (a zero
     leaves `requestTimeoutMs` at the disk's own value while reserving nothing) and validates,
     overflow-safely and strictly, `envelope + margin < TTL`, and with background renewal
     `period + 2 × envelope + margin < TTL` (this replaces the `attempt ≤ TTL − margin − period` check
     in `Pool::open`, `CasPool.cpp` ~95). Example the old check accepted and the new one refuses:
     TTL 25 s, period 10 s, margin 2 s, attempt 5 s, connect 5 s — a 15 s envelope, and the first
     scheduled renewal could not admit its two envelopes.
   - Every lease-arithmetic site uses the envelope: `CasRequests::attempt_reservation_ms`
     (`CasRequests.cpp` ~245) = `attemptEnvelopeMs()` (the backend forwards the budget it was built
     with); the open and remount publication horizons (`CasPool.cpp` ~841, ~1506) reserve
     `period + 2 × envelope` and refuse EQUALITY, as `CasMountRuntime::admit` does (`CasMountRuntime.cpp`
     ~149; today the horizons accept it and the fence refuses it, so a horizon that "fits" exactly
     starts a renewal the fence then rejects); `CasMountRuntime::refAppendFenceOk` (`CasMountRuntime.cpp` ~154) asks for
     `2 × envelope` (a write plus its settlement read, which is what `writeLoop` reserves); the three
     teardown drain deadlines (`ContentAddressedMetadataStorage.cpp` ~986, `CasPool.cpp` ~1004, ~1160)
     use `envelope + margin`.
   - The clone takes the cap: `getSingleAttemptClient(request_timeout_ms, connect_timeout_cap_ms)`,
     cached by the pair, sets `cfg.requestTimeoutMs = request_timeout_ms` and
     `cfg.connectTimeoutMs = (base connectTimeoutMs == 0) ? cap : min(base connectTimeoutMs, cap)` — a
     reloaded base of zero (unbounded) resolves to the frozen cap, never to "no limit". The
     control-request context of decision 1 carries the cap next to the timeout, so a clone rebuilt
     over a reloaded base client still honours the frozen cap.
   With defaults the envelope is 7 s (5 s + 2 × 1 s), and spec 2's scheduling-lateness figure is
   computed from it.
   What this does NOT make hard, stated so nobody reads "envelope" as a wall-clock deadline: it
   bounds one TCP connect and one TLS handshake under the frozen cap each, and each socket operation
   under `requestTimeoutMs`. An attempt can still exceed it when the response keeps every inactivity gap
   shorter than the timeout, when the request throttler or the resource scheduler waits before the
   connection is opened, during DNS resolution, across a redirect, and for a LIST whose store page is
   truncated: `S3IteratorAsync` rebuilds and `IObjectStorageIteratorAsync` prefetches the next page
   outside the engine's attempt (`ObjectStorageIteratorAsync.cpp` ~71). The engine's first-page LIST
   asks for `limit + 1` keys, so an untruncated first page is one request; walks and truncated pages
   keep the prefetch. An absolute wall-clock deadline needs a `PocoHTTPClient` change and a
   non-prefetching control LIST needs an iterator variant; both stay in the backlog (R2 item (a) is
   closed for connect and left open for the rest).

## What this does not change {#unchanged}

The fuse stays for every first attempt (user ruling). The single-attempt client and its request
timeout stay. Spec 1's connect-failure hint is orthogonal: a connect-failure-hinted attempt remains
ambiguous but is reissued without a preceding settle read; a fuse timeout is reissued after the read. The LIST page bounds of `aee2a4b6880`/`23b67e74a85`
stay; they reduce what a single attempt has to enumerate, this spec makes the second attempt able to
finish it.

## Observability and documentation {#observability-docs}

1. `CASRequestReissue` (`ProfileEvents.cpp` ~944) is incremented by the zero-pause helper too and its
   description becomes pacing-agnostic ("re-sent after a failed or ambiguous attempt; the pause before
   it is a jittered backoff, a flat pause, or none"). `CASRequestResolveRead` is described
   operationally: "exact settlement reads the CAS request contract made; under a reissuing policy a
   connect-failure hint (spec 1) defers the read until a later outcome requires it" (under
   `Retry::once` a hinted attempt still gets its immediate read, and a hinted attempt whose reissue
   meets `412` is settled by a later read). New `CASRequestFirstAttemptFuse` per matched timeout.
2. The `writeLoop` contract comment (`CasRequests.h` ~356, "settles every ambiguity by an exact read
   before it reports anything") is rewritten: `Committed` and `Conflict` are proven by an exact read
   or by the reissue's own 2xx; a connect-failure-hinted attempt is reissued before its read;
   `Refused`, `Declined` and `GaveUp` are what they always were.
3. `configuration.md`: the `cas_attempt_timeout_ms` row names the conditional PUT among the requests,
   says `≥ 1`, and states the envelope (`attempt timeout + 2 × cap`, `cap = attempt timeout` when
   `connect_timeout_ms` is 0, else `min(connect_timeout_ms, attempt timeout)`; one TCP connect and one
   TLS handshake under the cap each; send/receive are per-socket-operation bounds); the
   `cas_lease_safety_margin_ms` row and the `cas_mount_renew_period_ms` row (~96, "one request
   attempt") use the envelope in their formulas (`envelope + margin < TTL`,
   `period + 2 × envelope + margin < TTL`).
4. `mounts-and-leases.md`: the "Absolute deadline" bullet (~82, "one configured attempt still fits")
   says "one attempt envelope"; the "Request-budget admission" bullet (~96,
   `attempt_timeout + safety_margin`) says `2 × envelope + safety_margin`. The setting descriptions in
   `ContentAddressedSettings.cpp` (~83, ~84) and the field comments of `CasRequestBudget.h` say the
   same.

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
6b. `CASRequestBudget.EnvelopeIsValidatedNotTheBareAttempt` (`gtest_cas_requests.cpp`):
   `attemptEnvelopeMs()` is 7000 for attempt 5000 / cap 1000 and 5000 for `nullopt`;
   `validateCasRequestBudget` refuses attempt 0; refuses TTL 25000 / period 10000 / margin 2000 /
   attempt 5000 / cap 5000 (the old check accepted it) naming the envelope in its message; accepts the
   defaults (30000 / 10000 / 2000 / 5000 / 1000: 10000 + 14000 + 2000 < 30000). Fails until the field, the
   accessor and the new inequalities exist.
6c. `S3SingleAttemptClient.ConnectTimeoutIsCappedAndFrozen`
   (`src/Disks/tests/gtest_cas_s3_client_profile.cpp`, shared with spec 3): a base configuration with
   `connectTimeoutMs = 20000`, request timeout 5000 and cap 5000 yields a clone with
   `connectTimeoutMs == 5000` and `requestTimeoutMs == 5000`; a base with 1000 and cap 5000 keeps 1000;
   after the base client is replaced with `connectTimeoutMs = 5000` (the reload path), the clone
   rebuilt for cap 1000 has `connectTimeoutMs == 1000`; a base of 0 (unbounded) with cap 1000 yields
   1000; two caps under one request timeout yield two distinct clones (the cache key is the pair).
6d. `CASRequests.ReservationIsTheEnvelope`: `attempt_reservation_ms == attemptEnvelopeMs()` for a
   backend built with attempt 5000 / cap 1000 (7000); `CASMountRuntime.RefAppendFenceOkIsAdmitAtTwoEnvelopes`
   replaces `RefAppendFenceOkIsAdmitAtTheAttemptTimeout`; `CASMountOpenWaits.PublicationHorizonUsesTheEnvelope`
   and its remount twin: with the cap set so that `period + 2 × attempt` fits but
   `period + 2 × envelope` does not, the open re-anchors synchronously (one extra renewal write)
   before arming; the renewal twin (`gtest_cas_heartbeat.cpp`) over a backend that consumes the
   whole envelope per attempt stops issuing before the lease cutoff (virtual clock) instead of
   overrunning it; both horizon tests include the exact-boundary case (`period + 2 × envelope ==
   remaining` is refused).
6e. `CASEnvelopeWiring.FrozenCapTravelsFromTheClientToEveryVerb` (`gtest_cas_s3_client_profile.cpp`):
   a real `ContentAddressedMetadataStorage` opened over a test `S3ObjectStorage` whose client has
   `connectTimeoutMs = 1000` and `attempt_timeout_ms = 5000` reports `poolConfig().cas_request_budget
   .attemptEnvelopeMs() == 7000` and `connect_timeout_cap_ms == 1000`; after `applyNewSettings` with a
   config carrying `connect_timeout_ms = 5000` (and separately `0`), one control read and one
   conditional write are issued and the recording S3 storage shows both selected a clone with
   `connectTimeoutMs == 1000`. A second storage opened over a client whose `connectTimeoutMs` is 0
   (with `cas_mount_lease_ttl_ms` large enough for the budget: TTL 60000, attempt 5000 → envelope
   15000) reports `connect_timeout_cap_ms == 5000`, `attemptEnvelopeMs() == 15000`, and its requests
   select clones with `connectTimeoutMs == 5000` — a snapshot computing `min(0, attempt)` fails here.
   Fails while any link of the chain (snapshot → `pool_config` → backend → context → clone) is
   missing.
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
