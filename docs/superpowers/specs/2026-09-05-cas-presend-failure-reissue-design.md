---
description: 'Design for the CAS request engine reissuing a write attempt whose failure text says the connection itself failed (no free local port, connection refused, host or network unreachable, connect timed out) after a flat pause and without a preceding settle read. The attempt stays ambiguous; only the pacing changes.'
sidebar_label: 'CAS connect-failure reissue hint'
sidebar_position: 10
slug: /superpowers/specs/cas-presend-failure-reissue-design
title: 'CAS: a connect-failure hint reissues a write without a preceding read'
doc_type: 'design'
---

# CAS: a connect-failure hint reissues a write without a preceding read {#cas-presend-failure-reissue-design}

**Status:** DRAFT rev.4 (2026-09-05). rev.1–rev.3 were reviewed by `codex` (`gpt-5.6-sol`,
xhigh; records `codex_spec1.final.md`, `codex_spec1r2.final.md`, `codex_spec1r3.final.md`; rev.3 = NO MAJOR, its MINORs folded here).
rev.2's text whitelist cannot PROVE a request was not sent (Poco maps `send`/`recv` errno through the
same `SocketImpl::error` as `connect`), so rev.3 stops claiming it: the texts are an optimistic hint
that skips one read, and every safety statement of the engine stays as it is. The user ruled that
`src/IO` (PocoHTTPClient, S3Exception) stays untouched. Spec 1 of the R2 series.

## Decision {#decision}

`CasOperation::writeLoop` gains a **reissue hint**: when an attempt fails with a transport error whose
text says the connection itself failed (the five Poco connect-path texts below), the engine does not
spend a settle read on that attempt before reissuing. It reissues the same bytes under the same
precondition after a flat pause, still under the deadline and fence gates, and lets the reissue's
own outcome settle everything: a 2xx is a commit; a `412` is settled by the existing single exact
read, which adopts the body as `Committed{resolved_by_read}` only when the attempt is recorded as
ambiguous (it is), the read shows the original precondition is no longer satisfiable (for a
`replace`, a different observed ETag; for a `create`, any object), the bytes are ours, and the
post-commit gate admits. Nothing about `any_ambiguous`, `sent_any`, `attempts_sent` or the verdict
taxonomy changes: a hinted attempt is as ambiguous as it is today; only the pacing differs.

One deliberate availability difference at the deadline edge: today, with only one read envelope
left, the loop may still perform its settle read and answer `Committed` or `Conflict`; the hint path
requires `flat_ms + 2` envelopes for the reissue and otherwise gives up. That is the conservative
side (a `GaveUp` with `sent_any == true`), accepted and tested.

## Why {#why}

On 2026-09-05 (`cas_selects`, three nodes, rustfs) the host ran out of ephemeral ports at the start of
the concurrent-selects phase: 970 `Cannot assign requested address` in one minute. The mount-lease
renewal's single PUT failed that way in milliseconds; the engine then spent ~11 s on settle reads that
could not connect either, until the lease-safe reserve was gone and the mount was fenced (BACKLOG:
issue #2243, fix direction 1). When the failure says "no connection", a read before the reissue is a
wasted attempt against the same broken condition; the reissue itself is the cheaper probe.

## The hint texts {#hint-texts}

Every transport failure of a conditional write reaches `writeLoop` as `DB::S3Exception` with code
`S3_ERROR`, S3 error `NETWORK_CONNECTION`, an empty exception name and no nested exception; only the
Poco text survives (`PocoHTTPClient::makeRequestInternal` keeps `getCurrentExceptionMessage()`,
`WriteBufferFromS3` rethrows it). The texts come from this repository's Poco (`SocketImpl.cpp`):

| Event | Substring |
|---|---|
| `EADDRNOTAVAIL` | `Cannot assign requested address` |
| `ECONNREFUSED` | `Connection refused` |
| `EHOSTUNREACH` | `No route to host` |
| `ENETUNREACH` | `Network is unreachable` |
| connect poll timeout | `connect timed out` |

`isConnectFailureHint(const Exception &)`, local to `CasRequests.cpp` (the backend's own outcome
taxonomy stays `CASConditionalWriteUnresolved`): true only for a `dynamic_cast` to `S3Exception`
whose `getS3ErrorCode() == NETWORK_CONNECTION` and whose message contains one of the substrings
(case-sensitive). It is a hint, not a verdict: the same errno can be reported after `send` or `recv`,
and the design needs no exclusivity because a hinted attempt keeps every safety property of an
ambiguous one.

## Behaviour {#behaviour}

In `writeLoop`, in the `catch (const Exception & e)` arm, after the credential branch:

1. `hint = isConnectFailureHint(e)`; `any_ambiguous` is set exactly as today.
2. If `hint` and the policy is not `single_attempt`: skip the resolve read for THIS iteration and go
   to a new `pauseFlat` (fixed nonzero pause, e.g. 50 ms; checks the fence and deadline gates like
   `pauseAndReissue` with `reservedFor(flat_ms, 2)`; does not advance `state.reissues`; records
   `CASRequestReissue`). Then `continue`. The reissue's outcome is handled by the unchanged loop.
3. If the deadline or fence refuses the reissue, the existing `gaveUp` applies with `sent_any == true`
   and `any_ambiguous == true`: the caller sees exactly what it sees today for an ambiguous failure.
   No new `GaveUp::Why`; under `single_attempt` the existing settle read runs as today.
4. ProfileEvents: `CASRequestConnectFailureHint` per hinted attempt, recorded in `writeLoop`.

Cost of a false hint (the error was post-send and the write landed): the reissue meets `412`, one
read follows, byte-identical body → `Committed{resolved_by_read}`; otherwise `Conflict` — the same
outcomes the loop produces today after its read. No path can duplicate a write or hide a landed one.

## Tests, in the order they are written and made to pass {#tests}

Component tests, explicitly not end to end (the existing `gtest_writebuffer_s3.cpp` fake returns a
prebuilt `AWSError` and cannot exercise `SocketImpl`; the `PocoHTTPClient → AWSError` transformation
stays an acknowledged residual gap). Each test is written first and fails before its step:

1. `CASRequestsConnectHint.PocoTextsArePinned` — `SocketImpl::error(errno)` for the four errno values
   throws messages containing the four substrings; a timed `connect` against a black-hole address (or
   the `SocketImpl::connect` timeout producer) throws `connect timed out`. Fails until the pinned
   list exists.
2. `CASRequestsConnectHint.ClassifierGuards` — `S3Exception{NETWORK_CONNECTION, text}` hints for each
   of the five texts; the same text under a non-`NETWORK_CONNECTION` S3 error does not; `NETWORK_CONNECTION`
   without a listed text does not (`Timeout` alone does not).
3. `WriteBufferS3Fake.NetworkConnectionTextSurvives` — the fake client's `NETWORK_CONNECTION` error with
   each text reaches the `WriteBufferFromS3` caller as `S3Exception` with the substring intact.
4. `CASRequestsConnectHint.HintedFailuresReissueWithoutARead` — backend `write` throws the hinted
   exception twice, then commits → `Committed{attempts_sent == 3, resolved_by_read == false}`, zero reads
   before the commit, two flat pauses (virtual clock), `state.reissues == 0` afterwards.
5. `CASRequestsConnectHint.ReissueMeetsPreconditionAndAdoptsOwnBytes` — hinted attempt, the reissue
   meets `412`, the read observes a DIFFERENT ETag with our bytes → `Committed{resolved_by_read}`;
   with other bytes → `Conflict`; with the ORIGINAL ETag → the loop reissues (not adopts).
6. `CASRequestsConnectHint.OnceKeepsOneWriteAndOneRead` — under `Retry::once()` a hinted failure
   performs one write and the existing settle read, no sleep, and returns today's verdict.
7. `CASRequestsConnectHint.EarlierAmbiguityStillSettlesByRead` — ambiguous attempt, then a hinted one:
   the settle read runs before any reissue.
8. `CASRequestsConnectHint.GatesRefuseTheReissue` — hinted failures until the deadline → `GaveUp{Deadline,
   sent_any == true}`; until the fence trips → `GaveUp{FenceLost}`; with exactly one read envelope
   left → `GaveUp` (the documented deadline-edge difference).
9. `CASRequestsConnectHint.AmbiguityAfterHintsStartsAtFirstBackoff` — several hints, then a normal
   ambiguity: the first backoff drawn is `backoff(1)`.
10. Renewal twin (`gtest_cas_pool.cpp` renewer tests): `MountLeaseRenewer` over a backend failing with
    the hint text for 3 s recovers within its window, `attempts_sent > 1`, no `CASRequestResolveRead`
    increment, recovery classification `committed_after_retry`.

## Out of scope {#out-of-scope}

The port churn itself (spec 3), the lease schedule (spec 2), the adaptive first-attempt timeout (spec 4).
