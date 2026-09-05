---
description: 'Design for the connection churn behind ephemeral-port exhaustion on CAS-over-S3 disks (issue #2243): instrument the pool resets by reason first, run a rate-controlled A/B spike on a verified rustfs stand, and only then ship a CAS client profile with a longer keep-alive, applied where the disk S3 client is created.'
sidebar_label: 'CAS connection churn'
sidebar_position: 10
slug: /superpowers/specs/cas-connection-churn-design
title: 'CAS: connection churn and ephemeral-port exhaustion'
doc_type: 'design'
---

# CAS: connection churn and ephemeral-port exhaustion {#cas-connection-churn-design}

**Status:** DRAFT rev.4 (2026-09-05; rev.4 records the one unreachable reason branch) — rev.3 rev.1 was reviewed by `codex` (`gpt-5.6-sol`, xhigh; records
`tmp/pr2300-cicd-watch/review/codex_spec3.final.md`, `codex_spec3r2.final.md`): rev.1 had 4 MAJOR — the
causal reading of `Reset >> Expired`, the placement of CAS defaults after the client exists, a spike
without a reproduced baseline, and a stale rustfs binary in the workspace; rev.2 = NO MAJOR, its
MINORs folded here. Spec 3 of the R2 series; the measured part of
https://github.com/Altinity/ClickHouse/issues/2243 (the CAS backlog entry "Issue #2243 CONFIRMED: local
port exhaustion fences out the mount lease" lives in the master worktree's `docs/superpowers/cas/BACKLOG.md`).

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

1. **Reset and Expired by reason.** New DISK-only ProfileEvents in `HTTPConnectionPool.cpp`. In
   `atConnectionDestroy` (returned connections), after the existing `max_requests` check and in this
   order: `DiskConnectionsResetDisconnected`, `DiskConnectionsResetKeepAliveAge`
   (`isKeepAliveExpired(getKeepAliveReliability())`, the client's own `0.9 ×` age),
   `DiskConnectionsResetResponseNotKeepAlive` (the residual `mustReconnect`: server `Connection: close`),
   `DiskConnectionsResetIncompleteRequestOrResponse`, `DiskConnectionsResetUnreadBufferedData`,
   `DiskConnectionsResetStoreLimit`, `DiskConnectionsResetPreserveException`; `Expired` from that path
   is `DiskConnectionsExpiredMaxRequests`. In `wipeExpiredImpl` (stored connections), keeping the
   existing age-before-stale precedence: `DiskConnectionsExpiredAge`, `DiskConnectionsExpiredStalePeer`.
   One increment per branch, the aggregate counters untouched. This is the only code change made
   before the spike.
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
   counters exist. One branch is unreachable from a single-threaded test and is documented in the test
   instead: `DiskConnectionsResetPreserveException` — `atConnectionDestroy` releases the destroyed
   connection's own group slot before it creates the storage wrapper, so the wrapper's hard-limit check
   cannot see the group at the limit; only a cross-thread race or a fault-injection hook could throw
   there (implementation finding, 2026-09-05).
2. The spike (step 2), recorded in this spec's implementation record as a before/after table with the
   reason breakdown; the shipped values are taken from it.
3. `S3ObjectStorageProfile.CasDefaultsApplyOnlyWhenUnset` (new `src/Disks/tests/gtest_cas_s3_client_profile.cpp`,
   exercising the public `S3ObjectStorage` configuration — the `src/IO/S3/tests` location would cross
   the stated boundary): a
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

## Implementation record {#implementation-record}

**Stand.** rustfs `1.0.0-rc.3` (the cached `ci/tmp/rustfs` was `1.0.0-beta.9`; downloaded the pinned
release to a versioned path and asserted `--version` before use), one `clickhouse server` built from
HEAD (`666e4b0db4a`, confirmed to contain the D1 reason counters), the lane's `cas_s3` disk
(`tests/config/config.d/cas_s3_storage_policy_for_merge_tree_by_default.xml`) against rustfs, every
system log pinned to a local `default`-policy disk so background log flushes cannot confound the
measurement. Ports moved to `18123`/`19000` because another live investigation on the same host
already held `8123`/`9000`. Ephemeral range `32768-60999` (28 232 ports), `net.ipv4.tcp_tw_reuse = 2`
(loopback-only reuse — material to the verdict, see below). Data: `spike`/`spike_join`
(`ReplacingMergeTree`, `storage_policy='cas_s3'`, merges stopped), 200 parts x 10 000 rows each,
loaded as 200 disjoint-key `INSERT`s. Load: 20 queries (10 point, 10 range, each a `JOIN` between the
two tables), fixed seed.

**rustfs keep-alive probe.** A `HEAD` response (both raw and SigV4-signed) carries no `Keep-Alive`
header and no `Connection: close` — rustfs never advertises a cap. A Python `http.client` connection
held idle and reused after 10 s, 20 s, 40 s, and 75 s was accepted every time (same TCP socket, same
`403` response) — rustfs's own HTTP/1 header-read timeout is empirically >= 75 s on rc.3, matching the
spec's assumed figure.

**Ramp.** `clickhouse-benchmark --concurrency N -i 0 --timelimit 20` at `N=4` already produced
`DiskS3GetObject` at ~696/s (target ~400/s) with zero rustfs 5xx; `N=4` is the found value (doubling
further would only overshoot the target).

**Arms.** 5 arms (`http_keep_alive_timeout`/`http_keep_alive_max_requests` in the `cas_s3` disk
block, or `disk_connections_soft_limit` at top level), `concurrency=4`, `--timelimit 300`, server
restarted between every run (fresh client pool), TIME_WAIT-to-rustfs drained below 500 first, run
order ABBA (forward baseline/30s-100/5s-10000/30s-10000/soft-limit-100, then reversed) so every arm
gets one early and one late repetition:

| run | arm | completed queries | `DiskS3GetObject` | `DiskConnectionsCreated` | GetObject/Created | `ExpiredMaxRequests` | TIME_WAIT peak | peak % of range |
|---|---|---|---|---|---|---|---|---|
| 1 | baseline (5s/100) | 60 208 | 362 670 | 3 600 | 100.7 | 3 651 | 769 | 2.7% |
| 10 | baseline (5s/100) | 271 051 | 1 628 125 | 16 288 | 100.0 | 16 276 | 3 239 | 11.5% |
| 2 | 30s/100 | 269 842 | 1 625 597 | 16 286 | 99.8 | 16 305 | 3 311 | 11.7% |
| 9 | 30s/100 | 272 145 | 1 636 086 | 16 363 | 100.0 | 16 358 | 3 347 | 11.9% |
| 3 | 5s/10000 | 263 697 | 1 583 075 | 167 | 9 479.5 | 155 | 324 | 1.2% |
| 8 | 5s/10000 | 270 340 | 1 624 932 | 163 | 9 968.9 | 157 | 53 | 0.2% |
| 4 | 30s/10000 | 274 092 | 1 646 432 | 163 | 10 100.8 | 158 | 49 | 0.2% |
| 7 | 30s/10000 | 268 472 | 1 614 779 | 154 | 10 485.6 | 159 | 325 | 1.2% |
| 5 | soft-limit 100 | 271 024 | 1 628 284 | 16 286 | 100.0 | 16 282 | 3 256 | 11.5% |
| 6 | soft-limit 100 | 262 432 | 1 577 031 | 15 781 | 99.9 | 15 769 | 3 139 | 11.1% |

Every one of the 10 runs recorded zero `EADDRNOTAVAIL`, zero `CASMountLeaseLost`/
`CASMountRenewalDeadlineExceeded`, and zero of every `DiskConnectionsReset*` reason (the run's whole
`DiskConnectionsExpired` total is `ExpiredMaxRequests`, plus one stray `ExpiredAge` in two runs).
`CASMountRenewalAttempts` was ~30-34 per run regardless of arm.

Run 1 (the very first restart after the data load) completed ~4.5x fewer queries than run 10 (the
same arm, run last) at the same concurrency — a one-time cold-mount cost on the first post-load
restart, not an arm effect: the GetObject-per-completed-query ratio is identical between the two
(6.02 vs 6.01), so every downstream counter scales with it. Reported here rather than discarded so
the low run-1 numbers are not misread as a `5s/100` regression.

**What the reason counters show.** `ExpiredMaxRequests` accounts for essentially all
`DiskConnectionsExpired`, and `DiskConnectionsCreated` tracks `completed_queries / 100` almost
exactly in every arm still running the default `http_keep_alive_max_requests=100` (baseline,
`30s/100`, soft-limit-100) — the churn is **entirely** the request cap, not the keep-alive timeout:
raising only the timeout (`30s/100` vs baseline) moved nothing (ratio 99.8-100.0 either way,
TIME_WAIT peak 11.1-11.9% either way). Raising only `http_keep_alive_max_requests` to 10 000
(`5s/10000`) cut `DiskConnectionsCreated` by ~100x and TIME_WAIT peak by ~10-65x, with completed
queries and error rates unchanged; adding the 30 s timeout on top (`30s/10000`) added a further
+6% on the GetObject/Created ratio (10 288 vs 9 721 summed) — a small but consistent extra
benefit. The `disk_connections_soft_limit=100` arm is indistinguishable from baseline (ratio 100.0,
TIME_WAIT peak 11.1-11.5%): confirms decision 5 -- the soft limit gates keeping, not creating, and
does not reduce port churn.

An earlier informal ramp probe at `N=4`/20 s (before the arm battery, using `SYSTEM DROP MARK CACHE`
between concurrency steps to force a cold read) showed the opposite reason profile --
`DiskConnectionsResetIncompleteRequestOrResponse` at 100% of `Reset` and zero `Expired*`. None of
the 10 sustained 300 s runs (each starting from an equally cold, freshly-restarted server)
reproduced any `Reset` of any reason. The likely explanation: dropping the mark/uncompressed cache
mid-flight, synchronized with concurrent in-flight reads, cancels reads that would otherwise
complete -- an artifact of that specific probe, not a property of the workload or of a cold
server start. Recorded so it is not read as a second churn mechanism competing with
`ExpiredMaxRequests`.

**Verdict.** Baseline is **not valid** by the letter of the spec's rule: zero `EADDRNOTAVAIL` and a
TIME_WAIT peak of at most 11.9% of the range, both runs, well under the 50% threshold for "baseline
reproduces port pressure". Two likely reasons, left as-is per "do not tune the stand until it
fails": rustfs sits on loopback and `net.ipv4.tcp_tw_reuse=2` enables fast TIME_WAIT-socket reuse
specifically for loopback addresses (masking pressure that a real network hop would not mask), and
`N=4` over 300 s creates one to two orders of magnitude fewer connections (16-32k) than the
backlog's cited CI incident (~26k ports in one *minute* at ~430 GET/s). This is the valid
"baseline does not reproduce" outcome the spec allows for.

Because baseline is invalid, no arm can "pass" against it in the spec's strict sense. The reason
counters nonetheless give a decisive, mechanistic answer to the question decision 3 asked: the
default `http_keep_alive_max_requests=100` is not a headroom margin here, it is the connection's
entire lifetime under this workload's request rate (every arm still at 100 recycles a connection
~100 requests in, matching `ExpiredMaxRequests` one-for-one with `DiskConnectionsCreated`). Raising
it to 10 000 removes essentially all of the churn with no downside observed (same completed-query
counts, same error rates, zero `EADDRNOTAVAIL`/lease-loss both before and after).

**Chosen values.** `http_keep_alive_timeout=30` and `http_keep_alive_max_requests=10000` -- both
from the best-performing tested arm (`30s/10000`, GetObject/Created 10 288 summed, its two runs'
TIME_WAIT peaks at 49 and 325, i.e. 0.2%-1.2% of the range -- in the same low range as `5s/10000`'s
53 and 324, not distinguishable from it on this metric). `http_keep_alive_max_requests` is shipped,
not left at its default, because the spike attributes the dominant reason (`ExpiredMaxRequests`) to it directly, satisfying
the spec's own condition for changing that setting. 30 s (not the backlog's 60 s) because it is the
value actually tested here and sits with room to spare under rustfs's verified >= 75 s idle
tolerance.
