---
id: CAS-343
title: >-
  Bound one CAS control-plane attempt by a wall-clock deadline: the attempt
  envelope is only a per-socket-operation timeout
status: To Do
assignee: []
created_date: '2026-10-02 05:58'
updated_date: '2026-10-12 11:51'
labels:
  - 'area:mounts'
  - 'area:requests'
  - 'complexity:medium'
  - 'risk:high'
  - 'confidence:solid'
  - 'needs:repro'
  - 'origin:review'
  - 'origin:noclock-audit'
milestone: m-8
dependencies: []
priority: high
type: bug
ordinal: 436000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The lease arithmetic assumes one control-plane attempt ends within the attempt envelope (`cas_attempt_timeout_ms + 2 × connect cap`, 7 s by default): write admission reserves two envelopes plus the margin before the lease deadline so that no request is in flight when authority ends. The transport does not enforce that bound. In `PocoHTTPClient` the request timeout becomes the socket send and receive timeout, which bounds one socket operation; every `recv` or `send` restarts it (`SocketImpl::receiveBytes`, `sendBytes`). No deadline for the whole attempt exists in `PocoHTTPClient`, the AWS SDK path, `S3::Client`, the CAS backend or the CAS request engine; `CasOperation::fits` checks the deadline only before an attempt is issued.

Worst cases found by reading the code at `e1c38ca7478` (nothing was run):
- one stalled phase: 7 s, the envelope holds;
- several phases each near its timeout: 12 s for a `GET` with a stalled body, 17 s or more for a `PUT` with `Expect: 100-continue`;
- a response that trickles (one byte every 4 s): unbounded;
- up to 11 requests inside one attempt through `307` redirects (`s3_max_redirects`) or the `S3::Client::doRequest` loop; a region re-sign; a `HeadBucket` region probe; a reconnect after a failed first write;
- a client-side throttler (`s3_max_get_rps`, `s3_max_put_rps`; off by default): unbounded;
- credentials fetched at signing (STS, web identity, GCP OAuth token): not bounded by the envelope.

Consequence: a conditional write admitted with the full reserve can still be in flight after the holder's lease deadline and after an observer was entitled to fence the slot. The stall cases stay inside the reserve plus the observer's margin by a small amount; the trickle, redirect, throttler and credential cases do not. Pre-existing; found while checking the settings UX notes (`docs/superpowers/specs/2026-10-02-cas-mount-lease-settings-ux-notes.md`, open question 2).

Proposed fix (from the study): one absolute deadline per attempt, set by the CAS backend in a scope around the call (`now + envelope`), read by `PocoHTTPClient` at each request start and by `Poco::Net::HTTPSession`, whose connect, send and receive clamp their timeout to the time left and throw a timeout once it has passed. The session travels inside the response, so the clamp also covers the body read. This touches shared code (`base/poco`, `src/IO/S3`), so it needs a decision on where the deadline lives before any edit.

Study: `tmp/issue-2431/ux/q2-request-deadline.md` (phase table with file, function and line for every claim).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A test with a scripted HTTP server that trickles a response: one control-plane attempt ends with a timeout within the envelope
- [ ] #2 A test for a PUT with Expect: 100-continue whose phases each stall just under their timeout: the attempt ends within the envelope
- [ ] #3 Redirects, the region re-sign, the doRequest loop and a credential fetch inside one attempt are bounded by the same deadline, or refused for the single-attempt client
- [ ] #4 A client-side throttler cannot delay a control-plane attempt past its deadline
- [ ] #5 No change for the ordinary S3 client when no deadline is set
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-10-02: the study behind this task is in `docs/superpowers/cas/2026-10-02-mount-lease-settings-studies.md` (section Q2, phase table with file, function and line per claim); the consequence for the write reserve and the settings is in `docs/superpowers/specs/2026-10-02-cas-mount-lease-settings-ux-notes.md` (Answers). A companion audit there (Q1) found that the second envelope of the write reserve is not needed for safety by itself but is today's only slack against an attempt that overruns its envelope: do not reduce the reserve before this task lands.

2026-10-08: deferred by the owner. The transport wall-clock deadline (series item 17b) is not being done now; `cas_request_deadline_ms` does not exist.

2026-10-12 noclock audit (docs/superpowers/cas/2026-10-12-late-request-audit/objects/transport.md): unbounded within one attempt today — trickling response, `100-continue` + slow body, conditional multipart writes (`FoldSeal`/`GcOutcomes` up to 256 MiB, ≤18 requests under one 16 s reserve), two redirect loops of up to 11 resends (`Client::doRequest`, Poco 307), reconnect-and-rewrite in `HTTPClientSession::write`; no `SO_LINGER{1,0}` or `TCP_USER_TIMEOUT` anywhere, so the kernel sends buffered bytes after abort. Design: `mount_fence.deadline_boot_ms` already exists; hook via `IHTTPSessionDataHooks::atStart` (check before every send, clamp timeout, linger+user-timeout at the boundary), combined hook because `PooledConnection::sendRequest` overwrites hooks; ~170 lines in `src/`, nothing in `base/poco`. Counter `CASTransportSendAfterDeadline` first, then abort. Would let us drop `E` as the reserve unit and both `validateCasRequestBudget` inequalities (keep `P + M < L`); writes until ~27 s instead of 14 s. Open: thread-local scope misses parallel `publish`/`stream`; clamped timeout vs `SSL_write`; Linux-only `TCP_USER_TIMEOUT`.
<!-- SECTION:NOTES:END -->
