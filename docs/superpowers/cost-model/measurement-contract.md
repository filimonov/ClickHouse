---
description: 'What the storage cost rig records and how a reader must interpret it: ledger rows, windows, retry correlation, lifecycle replay bounds, relay and local I/O attribution, runner sequence and the limits of one workstation.'
sidebar_label: 'Measurement contract'
sidebar_position: 52
slug: /superpowers/cost-model/measurement-contract
title: 'Measurement contract'
doc_type: 'reference'
---

# Measurement contract {#measurement-contract}

Contract of the rig in `utils/cas-yardstick/yardstick/cost/`. Row and record shapes are in [schemas](schemas/README.md). Code references name the module that owns each rule.

## Ledger {#ledger}

`proxy.py` sits between every S3 caller and the store. Each S3 attempt becomes one `ledger-row/1` JSON line. `mark` rows carry a label and a timestamp.

- **Caller ports.** One listener per caller: `r1`, `r2`, `r3` (replicas) and `rig` (the observer). The port identifies the caller. Defaults are in `topology.py`.
- **Order.** `seq` and `t_*_us` are taken under one lock. Seq order is time order. An attempt that starts after a mark belongs to the next phase.
- **Request fidelity.** Request line, `Host`, path, query and headers are forwarded byte-exact, so the SigV4 signature still verifies. Bodies stream in 1 MiB pieces. Only `CompleteMultipartUpload` and `DeleteObjects` bodies up to 8 MiB are buffered, to parse `completed_parts` and per-key `delete_results`.
- **Identity per attempt.** `store_id`, `bucket`, `key`, `op` (parsed S3 operation), `upload_id`, `part_number`, `copy_source` (`bucket`, `key`, `range`), recorded request headers and response headers.
- **Bytes.** `req_payload_bytes` / `resp_payload_bytes` are body bytes. `req_wire_bytes` / `resp_wire_bytes` are head plus raw body, chunk framing included. Responses the proxy writes itself (411, injected faults) count as 0 response bytes. `payload_sha256` hashes the request body.
- **Outcomes.** `downstream` is what the caller saw: `delivered`, `dropped`, `injected`, `rejected`, with the status. `upstream` is what the store did: `ok`, `error`, `unknown`, `not_sent`, with `http_status` and `s3_code`. Injected downstream outcomes never overwrite the upstream outcome.
- **Chunked bodies.** Chunked request bodies are rejected with 411 `MissingContentLength`. The attempt is recorded as `rejected`.
- **`Expect`.** For `Expect: 100-continue` with a body the proxy answers `100 Continue` itself, sets `expect_negotiated`, and strips the header upstream.
- **Faults.** Rules set by `set_fault` match on `caller`, `op`, `key_regex`, `upload_id`, `part_number` and `nth` (1-based count of matching attempts since the rule was set, or `*`). Actions: a status reply (with or without sending the request to the store) or `drop_response` (forwarded, then the connection closes unanswered). Every attempt a rule fires on carries `injected_rule`. The runner clears all rules before teardown.
- **Open attempts.** An attempt that has begun and not finished has no row. `open_attempts` lists them, and the runner counts them as crossing a window boundary.

## Windows and phases {#windows}

`ledger.slice` cuts the ledger by four marks: `setup_start`, `window_start`, `window_end`, `teardown_end`. Marks must appear exactly once and in seq order, or the slice raises.

| Phase | Attempts with seq |
|---|---|
| `setup` | between `setup_start` and `window_start` |
| `window` | between `window_start` and `window_end` |
| `teardown` | between `window_end` and `teardown_end` |
| `outside` | everything else; other passes sharing the proxy |

An attempt **crosses** a boundary when it is unfinished or its `[t_start_us, t_end_us]` contains a window timestamp. A crossing attempt is listed in `window.crossing`.

## Classification {#classification}

`ledger.classify` splits window attempts three ways.

- **rig:** caller `rig`. Observer requests (listing, probe uploads, cleanup). They never count as workload.
- **maintenance:** workload caller whose key matches a card family flagged `maintenance`. A `DeleteObjects` attempt is maintenance only if it lists at least one key and every key matches a maintenance family.
- **foreground:** every other workload attempt, including keyless ones such as bucket-level LIST.

Objects outside every family are `active` data. Counts per caller and op are `attempts`, `delivered`, `dropped`, `injected`, `rejected`, `upstream_ok`, `upstream_error`, `upstream_unknown`, payload bytes, `list_pages`, `list_keys`, `delete_calls`, `delete_keys`.

## Retry correlation {#retries}

`correlate_retries` groups attempts into a retry chain. An attempt joins a chain when the previous attempt with the same identity failed (`dropped`, `injected`, upstream `error` or `unknown`). Identity is caller, op, store, bucket, key, upload id, part number, payload hash and the conditional / `x-amz-*` headers. Signature and SDK bookkeeping headers (`x-amz-date`, `x-amz-content-sha256`, `amz-sdk-invocation-id`, ...) are excluded. Every attempt is still counted and billed on its own. Chains are reported, never merged.

## Lifecycle replay and bounds {#replay}

`ledger.replay` rebuilds the object and incomplete-part inventory from workload attempts twice, as `Bounds(lower, upper)`.

- **Identities.** Objects: `(bucket, key)`. Incomplete parts: `(bucket, key, upload_id, part_number)`. A replacement affects only the matching identity. A failed condition leaves state unchanged.
- **Mutations.** Creation side: put, copy (destination identity, size from the recorded source), part upload, part copy (range length), and the object a completed upload makes. Retirement side: delete, `DeleteObjects` per-key results, abort, and the parts a completion retires. A completion has both. A completion with `complete_error` changes nothing.
- **Lower bound** (byte-hours minimising): creations at `t_end_us` and only when upstream is `ok`; retirements at `t_start_us`, also when upstream is `unknown`.
- **Upper bound** (byte-hours maximising): creations at `t_start_us`, also when `unknown`; retirements at `t_end_us` and only when `ok`. Objects made by an `unknown` attempt carry `outcome_unknown`.
- **Never change inventory:** upstream `error`, `not_sent`, and `rejected` attempts.
- **Initial inventory.** The runner replays from a verified-empty store (`inventory.initial: empty_verified`). A replay that starts elsewhere must declare its initial inventory with evidence (`declared`).
- **Observer mutations** (for example the rig aborting a probe's multipart upload) stay in replay although `rig` requests are excluded from workload counts.
- **Byte-hours.** `byte_hours(bounds, t0, t1)` integrates size over time clipped to the window, per category, as `{lower, upper}`. Both are the per-category min and max of the two replays, so `lower <= upper` always. Objects of unknown size land in `uncertain` with size 0. A lifetime that crosses a window edge contributes only its in-window part.
- **Categories.** `active`, `outdated`, `garbage`, `temporary`, `incomplete`, `detached`, `metadata`. A key family assigns the category at creation. `incomplete` is assigned to parts. A logical category (`outdated` after a merge, `garbage` after a drop, ...) is attached later with `reclassify(identities, category, t_us, evidence)`, which applies to both bounds from `t_us` onward and records the evidence. Without evidence the object keeps its creation category.
- **Row sanity.** `check_rows` reports duplicate or out-of-order seq, unfinished attempts, and missing `delete_results` or `completed_parts` on ok responses.

## Relay attribution {#relay}

`interserver_proxy.py` is a byte-counting TCP relay per replica. Relay `i` listens on `127.0.1.<i>:<interserver port of replica i>` and forwards to `127.0.0.1:<same port>`. Peers dial the alias.

- `from_target` is every byte replica `i` sent. The sender is exact.
- `to_target` is every byte its peers sent it. The receiver is known by construction: two-replica cards have one receiver, and the three-replica `replacement` card keeps `r1` and `r2` idle.
- The relay does not parse the protocol. It excludes client, Keeper and S3 traffic by construction.
- `snapshot` counts bytes of still-open connections, so a window delta is exact without waiting for connections to close. The measurement records `connections_total` and `open_at_end`.

## Local I/O {#local-io}

`localio.py`, per replica, for every variant.

- **`process_io`.** Delta of `/proc/<pid>/io` (`rchar`, `wchar`, `syscr`, `syscw`, `read_bytes`, `write_bytes`) over the window. It is process-wide and attribution is `process_io`. A process that dies before its counters are read fails the pass (`generation_crashed`); counters of restarted generations are summed.
- **Union peak.** `DirSampler` samples allocated bytes (`st_blocks` x 512) over the union of the replica's `data`, `s3_meta`, `caches`, `cas_scratch` directories (plus the `local` policy volume), counting a hardlinked inode once, every 0.5 s. `peak_allocated_bytes` is the maximum sample. A baseline sample exists before the window starts, and a final sample is taken at stop.
- **Device.** `device_of` resolves the block device, filesystem type and mountpoint from `/proc/self/mountinfo`.
- **Tracing.** Cards with `trace` run the server under `strace -f -yy -e trace=write,pwrite64,writev,read,pread64,readv,fsync,fdatasync`. `parse_strace` counts calls, returned bytes and a power-of-two request-size histogram for fds that resolve to absolute paths. Failed calls, sockets and pipes are skipped. Tracing needs `strace` and `ptrace_scope <= 1`, or the pass fails with `trace_unavailable`.
- **Limitations** recorded per replica: `process_io` includes the embedded Keeper on replica 1; host `diskstats` include the store, the proxy and unrelated activity; the page cache is not dropped without root. The directory peak is sampled, so a file that lives shorter than the interval can be missed.

## Runner sequence and cleanup {#runner}

`runner.py` runs one `(policy, card, case, k)` pass. Policy `local` maps to variant `ebs`; a `_cached` suffix enables the filesystem cache.

1. Refuse with `UNSUPPORTED` when the card does not support the policy or the replica count.
2. List the pass prefix `<run_id>/<policy>/<card>/<case>/k<k>/` (trailing slash, so `k1` never matches `k10`). A non-empty prefix fails with `prefix_not_empty` before anything starts.
3. Write configs, mark `setup_start`, start Keeper and replicas, run card `setup`.
4. Mark `window_start`. Snapshot counters and relays, start samplers. Run the measured step, then `complete` (drain replication queue, merges, mutations within `timeout_s`).
5. Collect counters, mark `window_end`, clear faults, run `teardown`, mark `teardown_end`, flush the proxy.
6. Collect the ledger and run replay. **Crossing rule:** a deterministic card that has any crossing attempt fails with `window_crossing`. A stochastic card records the attempts in `window.crossing`.
7. Evaluate card assertions into the record.
8. **Cleanup always runs and never raises.** Clear faults, stop samplers, run teardown if not done, stop servers, abort uploads the workload left open, delete the pass prefix (unless `keep_prefix`), verify the prefix is empty. Failures go to `cleanup.errors`. `verified_empty` must be true for a clean pass.

Store lifecycle: `rustfs-bin.sh ensure` exits 0 when this call started the store, 3 when it was already up and not ours, other codes on failure. Only a caller that saw exit 0 stops the store; an `ensure` that fails stops nothing.

## What one workstation cannot establish {#limits}

- Real network latency, S3 request rate limits, cross-AZ transfer and S3 throttling. The store is a local RustFS behind loopback.
- EBS volume behavior. The `local` policy measures request counts and sizes on a local device; IOPS credits, burst and latency are not measured.
- Per-path disk I/O. `process_io` is process-wide, and host `diskstats` include unrelated activity.
- Multi-AZ or multi-host replication cost. Interserver bytes are measured on loopback relays and priced by assumption.
- Behavior beyond the pinned binary and commit recorded in each measurement.

## Review {#review}

| Date | Reviewer | Scope | Findings | Closed |
|---|---|---|---|---|
