---
description: 'Design for the connection churn behind ephemeral-port exhaustion on CAS-over-S3 disks (issue #2243): measure by cause with the pool metrics, raise the client keep-alive defaults for CAS disks, and treat the read path that abandons response bodies as the main churn engine.'
sidebar_label: 'CAS connection churn'
sidebar_position: 10
slug: /superpowers/specs/cas-connection-churn-design
title: 'CAS: connection churn and ephemeral-port exhaustion'
doc_type: 'design'
---

# CAS: connection churn and ephemeral-port exhaustion {#cas-connection-churn-design}

**Status:** DRAFT rev.1 (2026-09-05). Spec 3 of the R2 series; the measured part of BACKLOG.md
"Issue #2243 CONFIRMED: local port exhaustion fences out the mount lease" (fix directions 3 and the
read-path caveat).

## What was measured {#measured}

CI, `Stateless tests (amd_asan_ubsan, cas s3 storage, parallel, 2/2)`, `metric_log` per 10 minutes:
`DiskConnectionsReused` 250–300k, `DiskConnectionsCreated` 5–25k, of which `Expired` 2–4k and
`Reset` up to 22k. rustfs logged 1 604 `Connection reset by peer` (the client closing). The pool's
own accounting (`HTTPConnectionPool.cpp:870-905`): `Expired` = idle beyond `http_keep_alive_timeout`
(5 s default, `S3Defines.h:12`) or `http_keep_alive_max_requests` (100) reached; `Reset` = the
connection was closed by an exception, or its response body was not fully read, or the pool store
limit was hit. The adaptive first-attempt fuse (spec 4) contributed ~570 trips in two hours — about
1% of the resets. So on this lane: keep-alive/max-requests explain the `Expired` share (20–30% of
created), abandoned response bodies explain the bulk of `Reset`.

`cas_selects` (regression, three nodes) hit 970 `EADDRNOTAVAIL` in one minute at the start of the
concurrent-selects phase; the stand collects no system tables, so its `Reset`/`Expired` split is
unknown. The backlog's prediction for #2243 (reporter: ~430 GET/s × 60 s ≈ 26k of 28 232 ports) fits
a TIME_WAIT rate equal to the request rate, i.e. nearly every request on a fresh connection.

## Decision {#decision}

1. **Keep-alive defaults for CAS disks.** `ObjectStorageBackend` (or the CAS disk factory) sets
   `http_keep_alive_timeout = 30` and `http_keep_alive_max_requests = 10000` for the disk's S3 client
   unless the disk config sets them explicitly; `configuration.md` documents both and the reason.
   The server-advertised `Keep-Alive: timeout=N` header still mins the client value
   (`HTTPClientSession.cpp:377`); rustfs's HTTP server has no idle-connection close of its own (its
   `idle_timeout` is the early-response body drain), so 30 s holds against rustfs.
2. **Measure before and after, by cause.** The spike: a local stand (rustfs `1.0.0-rc.3` as the lane
   starts it in `clickhouse_proc.py::start_rustfs`, one server with the lane's CAS disk config), a
   3-minute load of 32 concurrent `SELECT … FINAL`/JOIN clients over a CAS table, sampled
   `DiskConnectionsCreated/Reused/Expired/Reset/Preserved` per 10 s, `ss -tan state time-wait | wc -l`,
   and `EADDRNOTAVAIL` in the server log — once with the defaults, once with the values above. Accept
   the defaults if `Created` falls by at least the `Expired` share and no `EADDRNOTAVAIL` appears;
   record the `Reset` share that remains.
3. **The read path is the main engine and is not in this spec.** A `ReadBufferFromS3` that stops
   before the end of its ranged body (`read_until_position`, early stop under LIMIT/cancel) leaves the
   connection unreusable and the pool resets it. That is upstream ClickHouse S3 read behaviour, not
   CAS; it goes to the backlog with the spike's `Reset` numbers as the measurement, and the fix
   direction "drain small remainders instead of resetting" for the upstream owners.
4. **Not a fix:** capping pool limits below the ephemeral range (backlog #2243 direction 4).

## Tests {#tests}

- gtest (`gtest_aws_s3_client.cpp`): a CAS disk built from a config without the two settings has
  `http_keep_alive_timeout == 30` and `http_keep_alive_max_requests == 10000`; with explicit values,
  the explicit values win.
- The spike's numbers, recorded in this spec's implementation record (before/after table).
- Integration (`test_cas_gcs` fake): a connection idle for 20 s is reused for the next CAS request
  (`DiskConnectionsReused` +1, `Created` unchanged) — the fake keeps the connection open.

## Out of scope {#out-of-scope}

Renewal isolation (a dedicated connection for the renewer was rejected: the store may close an idle
connection and a ping every ≤4 s would add 15 requests/min and machinery), spec 1, spec 2, spec 4.
