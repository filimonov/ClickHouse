---
description: 'Design for the CAS request engine treating a write attempt whose failure text proves no request reached the store (no free local port, connection refused, host or network unreachable, connect timed out) as not sent: reissued after a flat pause under the same deadline, never settled by a read. Text-based classification, deliberately conservative.'
sidebar_label: 'CAS pre-send failure reissue'
sidebar_position: 10
slug: /superpowers/specs/cas-presend-failure-reissue-design
title: 'CAS: a write that never left the host is reissued, not resolved'
doc_type: 'design'
---

# CAS: a write that never left the host is reissued, not resolved {#cas-presend-failure-reissue-design}

**Status:** DRAFT rev.2 (2026-09-05). rev.1 was reviewed by `codex` (`gpt-5.6-sol`, xhigh; record in
`tmp/pr2300-cicd-watch/review/codex_spec1.final.md`): 4 MAJOR, all folded in below. The user ruled
(2026-09-05) that the transport layer (`src/IO/S3/PocoHTTPClient`, `S3Exception`) stays untouched:
the classifier reads the failure text that already reaches the engine. Spec 1 of the R2 series.

## Decision {#decision}

`CasOperation::writeLoop` gains one more attempt outcome next to the credential answer: **not sent**.
An attempt whose failure proves no request reached the store is neither a commit nor an ambiguity:
the engine leaves `state.any_ambiguous` untouched, does not spend a settle read on the attempt, and
reissues the same bytes under the same precondition after a flat pause, still under the deadline
and fence gates. Only when no earlier attempt of the same call is ambiguous; otherwise the existing
settle-read path runs, because that earlier attempt still needs settling. `readLoop` is unchanged.

## Why {#why}

On 2026-09-05 (`cas_selects`, three nodes, rustfs) the host ran out of ephemeral ports at the start of
the concurrent-selects phase: 970 `Cannot assign requested address` in one minute. The mount-lease
renewal's single PUT failed that way in milliseconds; the engine treated it as ambiguous and spent
~11 s on settle reads that could not connect either, until the lease-safe reserve was gone and the
mount was fenced (BACKLOG: issue #2243, fix direction 1). A request that never had a TCP connection
cannot have landed: reading the key back settles nothing the failure did not already settle.

## What reaches the engine, and what counts as not sent {#what-counts}

Every transport failure of a conditional write reaches `writeLoop` as `DB::S3Exception` with
ClickHouse code `S3_ERROR`, S3 error `NETWORK_CONNECTION`, an empty exception name and NO nested
exception: `PocoHTTPClient::makeRequestInternal` catches `Poco::Net::NetException`,
`Poco::IOException` and `Poco::TimeoutException`, keeps only `getCurrentExceptionMessage()` as the
client error message, and `WriteBufferFromS3` rethrows that text. What differs is the text, and the
text is produced by Poco in this repository (`base/poco/Net/src/SocketImpl.cpp`), so it is ours to
rely on and ours to test:

| Event | Text that reaches the engine (substring) |
|---|---|
| `EADDRNOTAVAIL` at implicit bind/connect | `Net Exception: Cannot assign requested address` |
| `ECONNREFUSED` | `Connection refused` |
| `EHOSTUNREACH` / `ENETUNREACH` | `Host is unreachable` / `Network is unreachable` |
| connect poll timeout (`SocketImpl::connect`) | `Timeout: connect timed out` |

`isPreSendFailure(const Exception &)`, in `CasRequests.cpp` next to `isDefinitelyRefusedWrite`:
true only when the exception is an `S3Exception` with S3 error `NETWORK_CONNECTION` AND its message
contains one of the four substrings above. Everything else — a generic `Timeout` (receive phase, the
200 ms first-byte fuse, `SO_ERROR=ETIMEDOUT` after connect), a `Connection reset`, a send-phase error —
stays ambiguous. The classifier is a whitelist of texts that Poco emits from the connect path only.

Why text is acceptable here: a false "not sent" is conservative. The reissue meets the store's own
`412`, the loop settles it with one read, and because `any_ambiguous` was never set the call returns
`Conflict` — a lost acknowledgement of a write that did land, never a duplicate application and never
a false `Committed`. Callers already handle `Conflict` (the renewer treats it as a same-uuid
uncertainty and terminalizes, exactly as it would today for that shape). A transport-seam test pins
the four texts so that a Poco or SDK change fails the test, not production.

## Behaviour {#behaviour}

In `writeLoop`, in the `catch (const Exception & e)` arm, after the credential branch:

1. `not_sent = isPreSendFailure(e)`.
2. A not-sent attempt never sets and never clears `state.any_ambiguous`. It skips the resolve read
   and reissues directly ONLY when `!state.any_ambiguous`; with an earlier ambiguity the existing
   settle-read path runs unchanged.
3. The reissue uses a new flat pause helper (`pauseFlat`): a fixed, nonzero pause (e.g. 50 ms) that
   checks the fence and deadline gates like `pauseAndReissue` but does NOT advance `state.reissues`,
   so a later genuine ambiguity does not start at an inflated backoff. Under `single_attempt` the call
   returns `GaveUp{Why::NotSent}` (a new `Why`, not a `Source`).
4. Accounting: `attempts_sent` and `sent_any` currently mean "an attempt was started". A proven
   pre-send failure must not count as possibly sent: `WriteState` gains `attempts_started`, and
   `sent_any` is set only when an attempt was NOT classified not-sent. Every consumer of `sent_any`
   is audited (`CasRefLedger.cpp` wedging at ~3906, `CasHotKeys.cpp:274`, `CasServerRoot.cpp:1650`
   renewal messages, `CasWriteResult.h` `orThrow`) and the new `Why::NotSent` is added to every
   exhaustive switch (`orThrow`, the renewal terminal classification at `CasServerRoot.cpp:1667`).
5. Backend outcome taxonomy: `ObjectStorageBackend::finalizeConditionalWrite` today records every
   such exception as `CASConditionalWriteUnresolved`; it gains `NotSent` using the same classifier so
   one attempt is not counted as both. ProfileEvents: `CASRequestNotSent` per attempt.

## Tests {#tests}

- Transport seam (`src/IO/S3/tests` or `gtest_writebuffer_s3.cpp`): a fake HTTP client that raises
  each of the four Poco exceptions makes `WriteBufferFromS3` throw an `S3Exception` whose message
  contains the pinned substring; a receive-phase timeout does not.
- Engine: a backend whose `write` throws the `EADDRNOTAVAIL`-shaped `S3Exception` twice and then commits
  → `Committed{attempts_sent == 1, resolved_by_read == false}`, `attempts_started == 3`, zero reads.
- Engine: ambiguous attempt → not-sent attempt → the settle read runs (one read), never a direct reissue.
- Engine: under `Retry::once()` → `GaveUp{NotSent, sent_any == false}`.
- Engine: false positive — the store applied the write but the backend reports the not-sent text:
  reissue → `412` → one read → `Conflict`; no second application.
- Engine: a burst of not-sent failures respects the deadline gate and the fence gate; the flat pause
  is nonzero; `state.reissues` does not grow.
- Renewal twin: `MountLeaseRenewer` over a backend failing pre-send for 3 s recovers within its window
  with no `CASRequestResolveRead` increment and the terminal classification `committed_after_retry`.

## Out of scope {#out-of-scope}

The port churn itself (spec 3), the lease schedule (spec 2), the adaptive first-attempt timeout (spec 4).
