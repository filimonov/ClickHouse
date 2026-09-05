---
description: 'Design for the CAS request engine treating a write attempt that failed before any byte reached the store (no route, no port, connect refused or timed out) as provably not sent: reissued at once under the same deadline, never settled by a read.'
sidebar_label: 'CAS pre-send failure reissue'
sidebar_position: 10
slug: /superpowers/specs/cas-presend-failure-reissue-design
title: 'CAS: a write that never left the host is reissued, not resolved'
doc_type: 'design'
---

# CAS: a write that never left the host is reissued, not resolved {#cas-presend-failure-reissue-design}

**Status:** DRAFT rev.1 (2026-09-05), awaiting review. Spec 1 of the R2 stability series
(`cas_selects` regression, 2026-09-05: mount lease lost under ephemeral-port exhaustion).

## Decision {#decision}

`CasOperation::writeLoop` gains one more attempt outcome next to the credential answer: **not sent**.
An attempt whose failure proves that no request reached the store — the connect phase failed — is
neither a commit nor an ambiguity. The engine does not mark `any_ambiguous`, does not spend a settle
read on it, and reissues the same bytes under the same precondition after a short pause, still under
the caller's deadline gate. Everything else is unchanged.

## Why {#why}

On 2026-09-05 (`cas_selects`, three nodes, rustfs) the host ran out of ephemeral ports at the start of
the concurrent-selects phase: 970 `Cannot assign requested address` in one minute. The lease renewal's
single PUT attempt failed that way in milliseconds; the engine then treated it as ambiguous and spent
~11 s on settle reads that could not connect either, until the lease-safe deadline reserve was gone,
and the mount was fenced. A request that never had a TCP connection cannot have landed: reading the
key back settles nothing that the failure did not already settle.

## What counts as not sent {#what-counts}

`isPreSendFailure(const std::exception &)`, in `CasRequests.cpp` next to `isDefinitelyRefusedWrite`:

- `Poco::Net::NetException` whose errno is `EADDRNOTAVAIL`, `ECONNREFUSED`, `EHOSTUNREACH` or
  `ENETUNREACH` (the S3 client wraps these into `DB::Exception` code `S3_ERROR` with the Poco text;
  the classifier reads the nested exception, never the message).
- `Poco::Net::ConnectionRefusedException`.
- A `Poco::TimeoutException` raised by the connect phase (`SocketImpl::connect`), as opposed to one
  raised while waiting for a response. The two are told apart by the exception's origin, which the
  HTTP session layer already records for the connection pool (`HTTPConnectionPool::prepareNewConnection`
  raises the connect failure; a receive timeout comes from `receiveResponse`). If the origin cannot be
  established, the failure is NOT classified as pre-send — the old ambiguous path applies.

A misclassification is safe in the direction that matters: reissuing the same bytes under the same
precondition after a landed write meets the store's own `412`, which the existing loop settles with
exactly one read. The cost of a wrong "not sent" verdict is one read, never a lost or duplicated write.

## Behaviour {#behaviour}

In `writeLoop`, in the `catch (const Exception & e)` arm, after the credential branch:

1. `not_sent = isPreSendFailure(e)`.
2. `not_sent` keeps `any_ambiguous` false and skips the resolve read, exactly like `credential_answer`.
3. Reissue goes through `pauseAndReissue` with a short fixed pause (the first backoff step), so a
   burst of pre-send failures is paced but not exponentially delayed; the deadline gate and the fence
   gate are checked as for every reissue. Under `single_attempt` the call returns `GaveUp` with a new
   reason `NotSent`, so the caller can say why.
4. `readLoop` gets the same classification: a pre-send failure of a read is reissued without counting
   as an observation.
5. ProfileEvents: `CASRequestNotSent` (per attempt). The `GaveUp` report names `NotSent` in its source.

## Tests {#tests}

- gtest (`gtest_cas_requests_*`): a backend whose `write` throws a wrapped `EADDRNOTAVAIL` twice and
  then commits — the result is `Committed` with `attempts_sent == 3`, `resolved_by_read == false`,
  and the backend saw **zero** reads.
- gtest: the same under `Retry::once()` returns `GaveUp{NotSent}`.
- gtest: a receive-phase timeout still takes the ambiguous path (one read), proving the classifier does
  not widen.
- The renewal twin: `MountLeaseRenewer` under a backend failing pre-send for 3 s recovers within its
  window with `attempts_sent > 1` and no `CASRequestResolveRead` increment.

## Out of scope {#out-of-scope}

The ephemeral-port churn itself (abandoned response bodies, `DiskConnectionsReset`) is a data-plane
S3 matter and is tracked in the backlog; the heartbeat lease model is spec 2; the adaptive first-attempt
timeout cooperation is spec 4.
