---
description: 'Design for the CAS request engine treating a write attempt whose failure text proves no request reached the store (no free local port, connection refused, host or network unreachable, connect timed out) as not sent: reissued after a flat pause under the same deadline, never settled by a read. Text-based classification, deliberately conservative.'
sidebar_label: 'CAS pre-send failure reissue'
sidebar_position: 10
slug: /superpowers/specs/cas-presend-failure-reissue-design
title: 'CAS: a write that never left the host is reissued, not resolved'
doc_type: 'design'
---

# CAS: a write that never left the host is reissued, not resolved {#cas-presend-failure-reissue-design}

**Status:** DRAFT rev.3 (2026-09-05). rev.1 and rev.2 were reviewed by `codex` (`gpt-5.6-sol`,
xhigh; records in `tmp/pr2300-cicd-watch/review/codex_spec1.final.md` and `codex_spec1r2.final.md`).
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
read, which — because the attempt is still recorded as ambiguous — adopts a byte-identical body as
`Committed{resolved_by_read}`. Nothing about `any_ambiguous`, `sent_any`, `attempts_sent` or the
give-up verdicts changes: a hinted attempt is as ambiguous as it is today; only the pacing differs.

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

`isConnectFailureHint(const Exception &)`, declared in the CAS backend layer (so both
`CasRequests.cpp` and `CasObjectStorageBackend.cpp` can call one definition): true only for an
`S3Exception` with S3 error `NETWORK_CONNECTION` whose message contains one of the substrings. It is
a hint, not a verdict: the same errno can be reported after `send` or `recv`, and the design needs no
exclusivity because a hinted attempt keeps every safety property of an ambiguous one.

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
4. The backend's outcome taxonomy (`CASConditionalWriteUnresolved`) is unchanged; ProfileEvents:
   `CASRequestConnectFailureHint` per hinted attempt.

Cost of a false hint (the error was post-send and the write landed): the reissue meets `412`, one
read follows, byte-identical body → `Committed{resolved_by_read}`; otherwise `Conflict` — the same
outcomes the loop produces today after its read. No path can duplicate a write or hide a landed one.

## Tests {#tests}

Component tests, explicitly not end to end (the existing `gtest_writebuffer_s3.cpp` fake returns a
prebuilt `AWSError` and cannot exercise `SocketImpl`; an injectable session seam would touch
`src/IO`):

- Poco formatting: constructing `Poco::Net::NetException` through `SocketImpl::error` for each errno
  yields the five substrings (pins the texts against a Poco bump).
- `AWSError → S3Exception`: the fake client's `NETWORK_CONNECTION` error with each text reaches the
  `WriteBufferFromS3` caller with the substring intact and S3 error `NETWORK_CONNECTION`.
- Engine: backend `write` throws the `EADDRNOTAVAIL`-shaped exception twice, then commits →
  `Committed{attempts_sent == 3, resolved_by_read == false}` with zero reads before the commit.
- Engine: hinted attempt, then the reissue meets `412` and the read finds our bytes →
  `Committed{resolved_by_read == true}`; finds other bytes → `Conflict`.
- Engine: hinted failures until the deadline → `GaveUp{Deadline, sent_any == true}`; until the fence
  trips → `GaveUp{FenceLost}`; the flat pause is nonzero and `state.reissues` does not grow.
- Engine: a receive-phase `Timeout` text takes today's path (read before reissue).
- Renewal twin: `MountLeaseRenewer` over a backend failing with the hint text for 3 s recovers within
  its window, `attempts_sent > 1`, no `CASRequestResolveRead` increment, recovery classification
  `committed_after_retry`.

## Out of scope {#out-of-scope}

The port churn itself (spec 3), the lease schedule (spec 2), the adaptive first-attempt timeout (spec 4).
