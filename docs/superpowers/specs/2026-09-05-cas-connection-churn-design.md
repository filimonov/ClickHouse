---
description: 'Design for the connection churn behind ephemeral-port exhaustion on CAS-over-S3 disks (issue #2243): instrument the pool resets by reason first, run a rate-controlled A/B spike on a verified rustfs stand, and only then ship a CAS client profile with a longer keep-alive, applied where the disk S3 client is created.'
sidebar_label: 'CAS connection churn'
sidebar_position: 10
slug: /superpowers/specs/cas-connection-churn-design
title: 'CAS: connection churn and ephemeral-port exhaustion'
doc_type: 'design'
---

# CAS: connection churn and ephemeral-port exhaustion {#cas-connection-churn-design}

**Status:** DRAFT rev.2 (2026-09-05). rev.1 was reviewed by `codex` (`gpt-5.6-sol`, xhigh; record
`tmp/pr2300-cicd-watch/review/codex_spec3.final.md`): 4 MAJOR — the causal reading of `Reset >> Expired`,
the placement of CAS defaults after the client exists, a spike without a reproduced baseline, and a
stale rustfs binary in the workspace — all folded in. Spec 3 of the R2 series; the measured part of
BACKLOG.md "Issue #2243 CONFIRMED: local port exhaustion fences out the mount lease".

## What was measured, and what it does and does not say {#measured}

CI, `Stateless tests (amd_asan_ubsan, cas s3 storage, parallel, 2/2)`, `metric_log` per 10 minutes:
`DiskConnectionsReused` 250–300k, `DiskConnectionsCreated` 5–25k, `Expired` 2–4k, `Reset` up to 22k.
rustfs logged 1 604 `Connection reset by peer`. The adaptive first-attempt fuse (spec 4) accounted for
~570 trips in two hours. These are lifecycle counters, not cohorts of one window, and each is composite
(`HTTPConnectionPool.cpp` ~763, ~871): `Expired` = stored connection older than `0.8 × keep-alive`
(`0.1 ×` once the shared DISK group passes `disk_connections_soft_limit` = 5 000), or stale (peer FIN/RST),
or `http_keep_alive_max_requests` reached; `Reset` = returned connection disconnected, or
`mustReconnect` (Poco: request STARTED more than `0.9 × keep-alive` ago — a 4.5 s request under the
5 s default returns as `Reset`), or incomplete request/response, or unread buffered bytes, or store
limit, or a preserve exception. So the numbers show heavy churn; they do not yet say which cause.

`cas_selects` (regression, three nodes, no system tables collected) hit 970 `EADDRNOTAVAIL` in one
minute at the start of the concurrent-selects phase. The backlog's #2243 reading (reporter: ~430 GET/s
× 60 s ≈ 26k of 28 232 ports) fits a TIME_WAIT rate equal to the request rate.

## Decision, in order {#decision}

1. **Reset and Expired by reason.** New ProfileEvents in `HTTPConnectionPool.cpp` split `Reset` into
   `disconnected`, `must_reconnect`, `incomplete_response`, `buffered`, `store_limit`,
   `preserve_exception`, and `Expired` into `age`, `max_requests`, `stale_peer`. Same file, same
   two functions, one increment per branch. This is the only code change made before the spike.
2. **The spike, as an A/B with a reproduced baseline.** Stand: the lane's rustfs launched as
   `clickhouse_proc.py::start_rustfs` does, with `rustfs --version` asserted to be `1.0.0-rc.3` (the
   workspace's cached `ci/tmp/rustfs` is `1.0.0-beta.9`; the runner downloads only when the file is
   absent — use a versioned filename and fail closed on mismatch); one server with the lane's CAS disk
   config, system logs moved to a local disk for the experiment; a `ReplacingMergeTree` table on the CAS
   disk with JOIN inputs also on CAS; a rate-controlled read load (target: the reporter's ~400 GET/s,
   reader count found by ramping, not fixed at 32 — S21 shows one-node rustfs throttling ~95% of GETs
   at 16 readers). Arms: `5 s/100` (default), `30 s/100`, `5 s/10000`, `30 s/10000`, identical data and
   query seeds, the client pool restarted between arms, plus one arm with a low `disk_connections_soft_limit`
   to exercise the `0.1 ×` regime. Per arm: cumulative `DiskConnections*` (with the new reasons),
   `DiskConnectionsTotal/Stored` gauges, `DiskConnectionsErrors`, `ReadBufferSeekCancelConnection`,
   physical S3 request and retry counts, completed queries, `CASMountRenewal*`/`CASMountLeaseLost`,
   TIME_WAIT filtered to rustfs's address in the server's network namespace, and `EADDRNOTAVAIL` in
   the server log; the ephemeral range recorded. Baseline must reproduce port pressure (`EADDRNOTAVAIL`,
   lease trouble, or filtered TIME_WAIT above a declared fraction of the range); treatment passes with
   zero `EADDRNOTAVAIL`, zero lease-loss/renewal-deadline events, TIME_WAIT with declared headroom,
   throughput within ~10% of baseline, and connections created per physical request lower by more than
   run-to-run variance.
3. **Ship what the spike proved, where the client is created.** The disk's S3 client is built in
   `ObjectStorageFactory`'s S3 creator (`ObjectStorageFactory.cpp` ~121-126: `loadFromConfigForObjectStorage`
   → `getClient`) BEFORE the metadata storage exists, so neither `ObjectStorageBackend` nor the CAS
   metadata factory can change it. The seam: `RegisterDiskObjectStorage` resolves a `cas` client-profile
   flag from `metadata_type` before `ObjectStorageFactory::create`; the S3 creator applies the CAS values
   as DEFAULTS only where the final `S3AuthSettings` fields are unchanged (explicit disk XML and changed
   `s3_http_keep_alive_*` stay highest precedence); `S3ObjectStorage` keeps the profile and reapplies it
   after the endpoint/disk merge in `applyNewSettings` before rebuilding the client. The single-attempt
   CAS client clones the base configuration, so it inherits the values. Which values: `http_keep_alive_timeout`
   first (candidate 30 s or the backlog's 60 s, chosen from measured inter-request gaps and the
   verified rustfs cap); `http_keep_alive_max_requests` only if the spike attributes churn to it.
   Note that Poco caps both from the server's `Keep-Alive` header and `Connection: close` forces a
   reconnect (`HTTPClientSession.cpp:375`); rustfs's documented HTTP/1 header-read timeout (75 s per its
   reverse-proxy docs) is to be verified against rc3's actual response headers and a socket-reuse probe
   across idle intervals before the value is fixed.
4. **Read path.** A `ReadBufferFromS3` whose HTTP range was fully received releases its connection
   reusable even if the caller has not consumed every buffered byte; only a buffer destroyed,
   retargeted or seeked BEFORE the range is fully received leaves an incomplete response and a `Reset`
   (`ReadBufferFromS3.cpp` ~435, `HTTPConnectionPool.cpp` ~547; `ReadBufferSeekCancelConnection` counts
   many of those). Whether draining small fixed-length remainders (bounded by bytes and a very short
   deadline, never in cancellation/shutdown/chunked/unknown-length cases) would help is decided by the
   reason counters from step 1, not assumed; it stays in the backlog until then.
5. **Not a fix, but not nothing:** lowering `disk_connections_soft_limit`/`store_limit` does not help
   (they gate keeping, not creating); the hard limit does stop creation and turns port pressure into
   `HTTP_CONNECTION_LIMIT_REACHED` — insufficient for lease safety, not ineffective.

## Documentation {#docs}

`docs/en/antalya/cas/configuration.md`: the CAS client profile values, the precedence (explicit disk
XML > changed `s3_http_keep_alive_*` > CAS defaults), and the effective `0.8 ×` / `0.1 ×` pool ages.

## Tests, in the order they are written and made to pass {#tests}

1. `HTTPConnectionPool.ResetAndExpiredReasonsAreCounted` (pool unit tests): each branch of
   `atConnectionDestroy` and `wipeExpired` increments exactly its reason counter. Fails until the
   counters exist.
2. The spike (step 2), recorded in this spec's implementation record as a before/after table with the
   reason breakdown; the shipped values are taken from it.
3. `S3ObjectStorageProfile.CasDefaultsApplyOnlyWhenUnset` (`gtest_aws_s3_client.cpp` / factory test): a
   `metadata_type = cas` disk without the settings gets the profile values; explicit disk values and a
   changed `s3_http_keep_alive_timeout` win; a non-CAS disk is untouched; `applyNewSettings` preserves
   the profile.
4. `RegisterDiskObjectStorage.CasProfileReachesTheS3Creator` (factory wiring): the flag derived from
   `metadata_type` reaches the creator.
5. Optional, only if an end-to-end reuse proof is still wanted: an integration test with a short
   test-specific keep-alive (not a 20 s sleep) showing `DiskConnectionsReused` +1 across an idle gap on
   the `test_cas_gcs` fake.

## Out of scope {#out-of-scope}

Renewal isolation (a dedicated connection with a keep-alive ping was rejected), spec 1, spec 2, spec 4;
the read-path drain until reason counters justify it.
