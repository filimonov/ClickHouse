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
- **Request fidelity.** Request line, `Host`, path, query and headers are forwarded byte-exact, so the SigV4 signature still verifies. Bodies stream in 1 MiB pieces. Only `CompleteMultipartUpload` and `DeleteObjects` bodies up to 8 MiB are buffered, to parse `completed_parts`, the requested `delete_keys` (before forwarding) and per-key `delete_results`.
- **Identity per attempt.** `store_id`, `bucket`, `key`, `op` (parsed S3 operation), `upload_id`, `part_number`, `copy_source` (`bucket`, `key`, `range`), recorded request headers and response headers.
- **Bytes.** `req_payload_bytes` / `resp_payload_bytes` are body bytes, counted as they cross, so a body cut midway keeps the bytes already relayed. `req_wire_bytes` / `resp_wire_bytes` are head plus raw body, chunk framing included. Responses the proxy writes itself (411, injected faults) count as 0 response bytes. `payload_sha256` hashes the request body.
- **Outcomes.** `downstream` is what the caller saw: `delivered`, `dropped`, `injected`, `rejected`, with the status. `upstream` is what the store did: `ok`, `error`, `unknown`, `not_sent`, with `http_status` and `s3_code`. Injected downstream outcomes never overwrite the upstream outcome.
- **Chunked bodies.** Chunked request bodies are rejected with 411 `MissingContentLength`. The attempt is recorded as `rejected`.
- **`Expect`.** For `Expect: 100-continue` with a body the proxy answers `100 Continue` itself, sets `expect_negotiated`, and strips the header upstream. One negotiated request is one attempt and one row. `wire_bytes_bound` is `upper` when the proxy negotiated and the store did not answer `ok`: a store handling `Expect` itself would have refused before the body, so `req_wire_bytes` only bounds the real bytes. Every other row is `exact`.
- **Faults.** Rules set by `set_fault` match on `caller`, `op`, `key_regex`, `upload_id`, `part_number` and `nth` (1-based count of matching attempts since the rule was set, or `*`). Actions: a status reply (with or without sending the request to the store) or `drop_response` (forwarded, then the connection closes unanswered). Every attempt a rule fires on carries `injected_rule`. The runner clears all rules before teardown.
- **Lost rows.** A row that fails to write makes every later `flush` (and `stop`) raise, so a ledger with a gap is never read as complete.
- **Open attempts.** An attempt that has begun and not finished has no row. `open_attempts` lists them, and the runner counts them as crossing a window boundary. A workload attempt begun between `setup_start` and `window_end` that still has no row when the ledger is read goes to `window.open_attempts`, and `ledger.complete` is false: its requests and inventory effect are unknown, so the record's request totals are lower bounds. The drill shows such a cell as unavailable (`incomplete ledger`) and prices nothing.
- **Observer client.** Every rig request (prefix listings, observer aborts, cleanup) goes through the proxy's `rig` listener, so an observer mutation is in the ledger. Only `rustfs-bin.sh ensure` talks to the store directly.

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
- **maintenance:** by default, a workload caller whose key matches a card family flagged `maintenance` (`ledger.is_maintenance`). A `DeleteObjects` attempt (key `""`) is maintenance only if it names at least one key (`delete_results`, else `delete_keys`) and every key matches a maintenance family.
- **foreground:** every other workload attempt, including keyless ones such as bucket-level LIST.

A card may replace this split with `classify` when key families cannot tell request origin. The CAS probe does: GC HEADs and DELETEs the same blob keys foreground publication writes (`CasGc.cpp:580` in the pinned source). Its payload families only assign inventory categories. A request by replica `rN` is maintenance when it starts inside one of `rN`'s GC rounds (`system.cas_gc_log`, Start to Finish of one `round_id`; a round without Finish is open-ended), or when its key is in a control family flagged `maintenance`: the lease subtree `gc/server-roots/`, GC state (`gc/state`, `gc/maintenance_state`, `gc/hb`, `gc/gen/`) and `_pool_meta` (key layout in `CasLayout.h`). No foreground write touches those keys, and the server renews its mount lease on its own schedule even with `gc: off`, so lease and GC-state traffic never reaches `foreground`. Every other request is foreground. With `controls.gc = measure` the cells price foreground plus maintenance together (integrated usage), and request totals are stochastic. Exact per-origin counts for CAS need `gc: off`.

**Row basis in the drill.** A formulas row whose expression reads a `measured` registry quantity (filled from the record's `observations.inputs`) declares `basis: measured`. The drill still reconciles it with the ledger and reports a disagreement, but never calls it exact: it feeds the ledger's own number back. Only `predicted` rows get an exactness verdict.

Objects outside every family are `active` data. Counts per caller and op are `attempts`, `delivered`, `dropped`, `injected`, `rejected`, `upstream_ok`, `upstream_error`, `upstream_unknown`, `upstream_4xx`, `accepted_dropped` (upstream ok, reply to the caller lost), payload bytes, `list_pages`, `list_keys`, `delete_calls`, `delete_keys`.

## Retry correlation {#retries}

`correlate_retries` groups attempts into a retry chain. An attempt joins a chain when the previous attempt with the same identity failed (`dropped`, `injected`, upstream `error` or `unknown`). Identity is caller, op, store, bucket, key, upload id, part number, payload hash, `Range`, the conditional / `x-amz-*` headers and the query string (pagination, upload id). Presigned-URL signature parameters are excluded from the query. Signature and SDK bookkeeping headers (`x-amz-date`, `x-amz-content-sha256`, `amz-sdk-invocation-id`, ...) are excluded. Every attempt is still counted and billed on its own. Chains are reported, never merged.

## Lifecycle replay and bounds {#replay}

`ledger.replay` rebuilds the object and incomplete-part inventory from workload attempts twice, as `Bounds(lower, upper)`.

- **Identities.** Objects: `(bucket, key)`. Incomplete parts: `(bucket, key, upload_id, part_number)`. A replacement affects only the matching identity. A failed condition leaves state unchanged.
- **Mutations.** Creation side: put, copy (destination identity, size from the recorded source), part upload, part copy (range length), and the object a completed upload makes. Retirement side: delete, `DeleteObjects` per-key results (the requested `delete_keys` when the response is missing), abort, and the parts a completion retires. A completion has both. A completion with `complete_error` changes nothing.
- **End states.** `lower` holds each identity's smallest possible final state, `upper` its largest (see the bounds below); a creation reaches `lower` only when upstream is `ok`, a retirement reaches `upper` only when `ok`. Objects made by an `unknown` attempt carry `outcome_unknown`. Cards read these inventories (residue, objects present).
- **Byte-hour bounds** follow each transition's effect on bytes, not its kind or its seq. Each transition takes effect at one instant inside its `[t_start_us, t_end_us]`, in any order those instants allow (seq is start order, so a PUT that started later can land first); an `unknown` one may not take effect. At an instant `t`, the state after transition `k` is possible when `k` may have taken effect by `t` and every other certain transition `j` may have taken effect before `k` (`t_start_j <= min(t, t_end_k)`) or may still be pending (`t_end_j > t`). The true state of every feasible schedule passes this test, so `lower` and `upper` integrate the minimum and maximum over the possible states, per category, with no cap on how many transitions overlap. The bound is pointwise and can be looser than the best single schedule. End states use the same test at the end of time: `lower` keeps each identity's smallest possible final state, `upper` its largest; a size tie breaks toward absent in `lower` and toward present in `upper` (a zero-byte object and its deletion both hold 0 bytes). Examples: 100 B over hours 0..5, overwritten with 1 B during [1, 2] and with 100 B during [3, 4], gives 203..401 byte-hours; 10 B, then 1 B during [1, 3] and 50 B during [2, 4], gives 14..170 byte-hours (one real schedule: 42.2) and final sizes 1 B and 50 B.
- **Never change inventory:** upstream `error`, `not_sent`, and `rejected` attempts.
- **Initial inventory.** Replay starts from an empty inventory. Listing the pass prefix proves no completed objects; incomplete multipart parts need `ListMultipartUploads` by prefix, which the pinned RustFS build (1.0.0-beta.9, see [Identities](#identities)) does not answer for a directory prefix. So the runner records `inventory.initial: declared` with `inventory.note` (objects listed empty; incomplete uploads not enumerable; prior passes abort their uploads from the ledger). `empty_verified` is recorded only when the rig client can list uploads by prefix and the listing is empty; a non-empty listing fails the pass with `prefix_not_empty`.
- **Observer mutations** (for example the rig aborting a probe's multipart upload) stay in replay although `rig` requests are excluded from workload counts.
- **Byte-hours.** `byte_hours(bounds, t0, t1)` integrates the bounds above, clipped to the window, per category, as `{lower, upper}`. The integration is a sweep, linear in the transitions plus their overlap: at `t` the possible states are the started transitions whose end is at or after `M(t)`, the latest start among certain transitions that must have landed by `t` (the absent state only while there is none); this is the possibility test above in closed form. Objects of unknown size land in `uncertain` with size 0 and are counted in `inventory.uncertain_identities`; the drill makes every cell with a storage row unavailable (`unknown object sizes (N)`) when that count is non-zero, so they are never priced at zero. A lifetime that crosses a window edge contributes only its in-window part. `Bounds.creators` names the caller that first created each identity, so bounds can be split by creator.
- **Categories.** `active`, `outdated`, `garbage`, `temporary`, `incomplete`, `detached`, `metadata`. A key family assigns the category at creation. `incomplete` is assigned to parts. A logical category (`outdated` after a merge, `garbage` after a drop, ...) is attached later with `reclassify(identities, category, t_us, evidence)`, which applies to both bounds from `t_us` onward and records the evidence. The record keeps the calls in `inventory.reclassified`, so `python3 -m yardstick.cost.reproduce` can recompute the bounds from the raw ledger slice. Without evidence the object keeps its creation category.
- **Row sanity.** `check_rows` reports duplicate or out-of-order seq, unfinished attempts, and missing `delete_results` or `completed_parts` on ok responses.

## Relay attribution {#relay}

`interserver_proxy.py` is a byte-counting TCP relay per replica. Relay `i` listens on `127.0.1.<i>:<interserver port of replica i>` and forwards to `127.0.0.1:<same port>`. Peers dial the alias.

- `from_target` is every byte replica `i`'s interserver server sent through relay `i`; `to_target` every byte peers sent to that server. Neither covers bytes replica `i` sends as an interserver client to a peer (those cross the peer's relay). The peer is known by construction: two-replica cards have one, and the three-replica `replacement` card keeps `r1` and `r2` idle.
- Both directions cross the network, so cells price both (`interserver_from_target`, `interserver_to_target` transfer rows) at the route's per-direction price.
- The relay does not parse the protocol. It excludes client, Keeper and S3 traffic by construction.
- `snapshot` counts bytes of still-open connections, so a window delta is exact without waiting for connections to close. The measurement records `connections_total` and `open_at_end`.

## Local I/O {#local-io}

`localio.py`, per replica, for every variant.

- **`process_io`.** Delta of `/proc/<pid>/io` (`rchar`, `wchar`, `syscr`, `syscw`, `read_bytes`, `write_bytes`) over the window. It is process-wide and attribution is `process_io`. A process that dies before its counters are read fails the pass (`generation_crashed`); counters of restarted generations are summed.
- **Union peak.** `DirSampler` samples allocated bytes (`st_blocks` x 512) over the union of the replica's `data`, `s3_meta`, `caches`, `cas_scratch` directories (plus the `local` policy volume), counting a hardlinked inode once, every 0.5 s. `peak_allocated_bytes` is the maximum sample. A baseline sample exists before the window starts, and a final sample is taken at stop.
- **Device.** `device_of` resolves the block device, filesystem type and mountpoint from `/proc/self/mountinfo`.
- **Tracing.** Cards with `trace` run the server under `strace -f -ttt -T -yy -e trace=write,pwrite64,writev,read,pread64,readv,fsync,fdatasync`, one output file per server generation (`strace.r<i>.g<n>.txt`; `-o` truncates, so a restart must not reuse a file). `parse_strace` counts calls, returned bytes and a power-of-two request-size histogram for fds that resolve to absolute paths, over all generations. Failed calls, sockets and pipes are skipped. The record's trace and the local probe's demand are clipped to the window marks: a call counts only if it starts and ends (start plus `-T` duration) inside the window; a call that straddles an edge is listed in `crossing`, and the rest are counted in `outside`. Tracing needs `strace` and `ptrace_scope <= 1`, or the pass fails with `trace_unavailable`.
- **Limitations** recorded per replica: `process_io` includes the embedded Keeper on replica 1; host `diskstats` include the store, the proxy and unrelated activity; the directory peak is sampled, so a file that lives shorter than the interval can be missed; the page cache is not dropped without root; for traced cards, IOPS and MiB/s are syscall demand averaged over the window, not device peaks. The drill shows them under every price.

## Runner sequence and cleanup {#runner}

`runner.py` runs one `(policy, card, case, k)` pass. Policy `local` maps to variant `ebs`; a `_cached` suffix enables the filesystem cache.

1. Refuse with `UNSUPPORTED` when the card does not support the policy, or the rig offers fewer servers (`--replicas`) than the card needs. The pass then starts exactly `card.replicas` servers and records that count in `replicas`, so an idle server never adds lease, GC or startup traffic. A request from any other workload port fails the pass with `unexpected_caller`.
2. List the pass prefix `<run_id>/<policy>/<card>/<case>/k<k>/` (trailing slash, so `k1` never matches `k10`). A non-empty prefix fails with `prefix_not_empty` before anything starts.
3. Write configs, mark `setup_start`, start Keeper and replicas, run card `setup`.
4. Mark `window_start`. Snapshot counters and relays, start samplers. Run the measured step, then `complete` (drain replication queue, merges, mutations within `timeout_s`).
5. Collect counters, mark `window_end`, clear faults, run `teardown`, mark `teardown_end`, flush the proxy.
6. Collect the ledger and run replay. **Crossing rule:** a deterministic card that has any crossing attempt fails with `window_crossing`. A stochastic card records the attempts in `window.crossing`.
7. Evaluate card assertions into the record. Any failed assertion makes the record `FAILED` with reason `assertions_failed`; the numbers stay in `partial`.
8. **Cleanup always runs and never raises.** Clear faults, stop samplers (a sampler thread error is raised at `stop` and lands in `cleanup.errors`), run teardown if not done, stop servers (`server.sh` fallbacks signal the server's process group, so a traced server dies with its tracer), abort uploads the workload left open, delete the pass prefix (unless `keep_prefix`), verify the prefix is empty. Failures go to `cleanup.errors`. `verified_empty` must be true for a clean pass. A record with cleanup errors (or, without `keep_prefix`, not verified empty) stays `ok`, but `cost run` and `cost probe` exit non-zero and mark it `cleanup failed`.

**Scale.** A record's `scale` is its manifest's; there is no CLI override. A card's `COUNTS` reach it as `ctx.counts`, each scaled by the manifest's `scale`.

**Runtime estimate.** `cost estimate` budgets each planned pass at `max(measured median, duration_s)`, where `duration_s` is the case's declared duration in `coverage.json`, plus the median cleanup, times the rerun reserve (`--rerun-reserve`, default 1.25). A missing or empty run directory, coverage or registry fails the command with exit 2.

### Identities {#identities}

Every record names the code, binary and store that produced it. A closure that groups records by identity must treat each field below as part of it.

| Field | Meaning |
|---|---|
| `binary_sha256` | The executed `clickhouse` binary. |
| `binary_download_sha256` | The download, from the nearest `SHA256SUMS` listing it. A self-extracting `clickhouse` rewrites itself on first start, so the two differ. |
| `commit` | The binary's source commit: `--commit`, or HEAD of `--source-dir` (default `tmp/src-<version>`). |
| `rig_commit` | HEAD of the rig worktree. |
| `rig_dirty` | `git status --porcelain -- utils/cas-yardstick` was non-empty at session start. `cost run` and `cost probe` refuse a dirty rig (exit 3) unless `--allow-dirty`, which records `true`. |
| `store_version` | Store build: the version pinned in `rustfs-bin.sh`, or `--store-version` with `--endpoint`. |
| `store_sha256` | Store binary: the sha256 pinned in `rustfs-bin.sh`, or `--store-sha256` with `--endpoint`. |

`rig_dirty`, `store_version` and `store_sha256` are optional in `measurement/1`: the records of the committed runs below lack them.

**Store pin.** `rustfs-bin.sh` runs only RustFS 1.0.0-beta.9, sha256 `bf4aca66af214c7733f2b775847e0227c113fc97d5f74752a305ce2b75933664`. It checks the binary before starting it, and the executable of a store already up (`/proc/<pid>/exe` of the pid it recorded). A mismatch or an unidentifiable store fails `ensure`.

**Committed runs, identified post hoc.** All 25 runs behind the committed records (`cost-20261024T035215Z` to `cost-20261024T180805Z`) used that build. The records do not say so; the identification rests on:

- `ci/tmp/rustfs` hashes to the pinned sha256 and reports `rustfs 1.0.0-beta.9` (git tag `1.0.0-beta.9`, commit `a7827ed91bf`).
- Its mtime, 2026-10-23 21:38 UTC, precedes the first run.
- `rustfs-bin.sh` has always started that path; the store process serving the rig on 2026-10-24 runs an executable with the same hash.

The design spec names RustFS 1.0.0-rc.3: that is the container image of `rustfs.sh`, which the cost rig does not use.

### Store and session {#session}

Store lifecycle: `rustfs-bin.sh ensure` exits 0 when this call started the store, 3 when it was already up and not ours, other codes on failure, and prints `store_version=<v> store_sha256=<h>`. Only a caller that saw exit 0 stops the store; an `ensure` that fails stops nothing.

**Session.** `cost run` and `cost probe` hold `tmp/yardstick.lock` (`fcntl.flock`, the file the report READMEs wrap runs in with `flock`) for the whole session and write `pid=<pid> run_id=<run>` into it. A second session fails at once with exit 3, naming the holder. A session started under `flock tmp/yardstick.lock` inherits the lock and runs. The run directory must not exist yet. SIGTERM stops servers, proxy and the store this session started before the process exits.

## What one workstation cannot establish {#limits}

- Real network latency, S3 request rate limits, cross-AZ transfer and S3 throttling. The store is a local RustFS behind loopback.
- EBS volume behavior. The `local` policy measures request counts and sizes on a local device; IOPS credits, burst and latency are not measured.
- Per-path disk I/O. `process_io` is process-wide, and host `diskstats` include unrelated activity.
- Multi-AZ or multi-host replication cost. Interserver bytes are measured on loopback relays and priced by assumption.
- Behavior beyond the pinned binary and commit recorded in each measurement.

## Review {#review}

| Date | Reviewer | Scope | Findings | Closed |
|---|---|---|---|---|
| 2026-10-24 | Codex (`tmp/codex_phase0_findings.md`), rulings by the plan controller | schemas, this contract, `yardstick/cost/*.py`, `model.js`, `contract.js`, `billing.js`, `drill.mjs`, `ui.js`, the phase-0 report | 18 MAJOR (two of them under R18), 5 MINOR; verdict REVISE | all 18 MAJOR and 4 MINOR fixed; 1 MINOR and the presets part of R18 parked |
| 2026-10-24 | Whole-branch final review of Plan 1 (opus), rulings by the plan controller | `78133cca87d..3c290a1c129` | 8 Important, 4 deferred items to fix before Plan 2, cheap minors; no Critical | all 8 Important and 4 deferred items fixed; 7 minors fixed, 1 done by documentation |

### Dispositions, 2026-10-24 {#review-2026-10-24}

The gate was re-run on the fixed code (`cost-20261024T024945Z`, five PASS); every record replays from its raw ledger slice.

| Finding | What | Disposition | Commit | Tests or reason |
|---|---|---|---|---|
| R1 MAJOR | `ledger.py`: replay endpoints are not byte-hour bounds for size-changing overwrites | fixed (fix round 1: bounds over every feasible effect order, not seq order; pointwise possibility test instead of the ruled permutation enumeration, so no cluster cap and no fail-closed fallback are needed) | `8c167405454`, fix round 1 | `ByteHourBounds` (203..401 case; `test_overlapping_overwrites_land_in_either_order`: the 42.2 byte-hour schedule and the 1 B final state; brute force over every schedule in any order) |
| R2 MAJOR | ambiguous `DeleteObjects` cannot reduce `lower` | fixed | `8c167405454` | `BatchDeleteKeys`, proxy `test_delete_keys_recorded_when_the_response_is_lost`, validator `test_ledger_row_batch_delete_keys_are_optional_strings` |
| R3 MAJOR | open attempts vanish from stochastic accounting | fixed | `8c167405454`, `5be403b1793` | `test_open_attempt_without_row_marks_the_ledger_incomplete`; drill `test_unavailable_states_bill_nothing_and_render_the_state` (incomplete ledger) |
| R4 MAJOR | shared observer client bypasses the proxy | fixed | `8c167405454` | `test_run_dispatch_and_exit_codes` (rig endpoint is the proxy's `rig` port); multipart probe aborts through `ctx.rig`, `rig_client` removed |
| R5 MAJOR | `empty_verified` does not cover incomplete uploads | fixed | `8c167405454` | `test_empty_inventory_verified_only_where_uploads_can_be_listed`; records say `declared` with a note |
| R6 MAJOR | CAS drill omits maintenance requests and metadata byte-hours | fixed | `8c167405454`, `5be403b1793` | CAS cell prices integrated requests and `active`, `garbage`, `metadata`; drill `test_reconciliation_catches_class_swaps_residuals_and_stochastic_tolerance`, `test_storage_must_match_the_record_and_evidence_is_required` |
| R7 MAJOR | key families cannot establish CAS request origin; `classify` misses key `""` | fixed | `8c167405454` | `test_request_origin_follows_the_callers_gc_rounds`, `test_gc_round_traffic_is_maintenance_and_priced_with_the_rest`, `test_classify_treats_empty_key_as_keyless` |
| R8 MAJOR | local demand divides lifetime trace counts by the window; trace file truncated on restart | fixed | `8c167405454` | `test_window_clips_calls_and_lists_crossing_ones`, `test_traced_card_counts_only_calls_inside_the_window`, `TracerTest.test_prefix`; gate: 3 800 writes, was 3 803 |
| R9 MAJOR | model rows cannot carry outcome rules; every `dropped` row is free | fixed | `5be403b1793`, `8c167405454` | `test_accepted_dropped_is_billed_and_dropped_is_not`, `test_validated_cell_rows_keep_their_outcome_facts`, `test_formulas_rows_carry_outcome_facts_and_cells_list_exclusions`, `AcceptedDropped` |
| R10 MAJOR | displayed values can bypass the contract | fixed | `5be403b1793` | `test_contract_validation_rejects_bad_dims_and_unregistered_observations`; found and fixed `contract.load` rejecting `cond: true` (`test_unconditional_state_is_not_an_expression`) |
| R11 MAJOR | unavailable states still priced | fixed | `5be403b1793` | `test_unavailable_states_bill_nothing_and_render_the_state` |
| R12 MAJOR | allocation stage not exercised | fixed | `5be403b1793` | `test_two_cells_share_one_bill_and_get_allocated_money`; gate: fetch probe as `inserts` + `recovery` sharing the storage tier |
| R13 MAJOR | reconciliation accepts wrong costs; IOPS plus MiB/s summed | fixed | `5be403b1793`, `8c167405454` | `test_reconciliation_catches_class_swaps_residuals_and_stochastic_tolerance`, `test_mixed_outcomes_compare_attempts_without_false_mismatch`; summary keyed by row type and op; counts gain `upstream_4xx` (`ClientErrors`) |
| R14 MAJOR | failed assertions and cleanup failures keep success | fixed | `8c167405454` | `test_failed_assertion_fails_the_record_and_keeps_the_numbers`, `test_cleanup_failure_makes_run_exit_non_zero` |
| R15 MAJOR | report identities disagree (download vs executed hash, rig vs source commit) | fixed | `8c167405454`, this commit | `test_download_hash_and_source_commit_lookup`; closure `test_identities_agree_within_a_report_and_drills_render`; report README and plan reconciled |
| R16 MAJOR | workstation qualifications absent beside the price | fixed | `5be403b1793` | evidence block in drill JSON and page; `render_check.mjs` requires it (`test_storage_must_match_the_record_and_evidence_is_required`) |
| R17 MINOR | `min`/`max` grammar mismatch | fixed | `5be403b1793` | `test_min_max_args_grammar_shared_with_the_validator` and `test_worked_formula_examples_valid` on `example-min-max.json` |
| R18 MAJOR | accounting and support/evidence closure weaker than §5.2 | fixed, presets part parked | `5be403b1793`, `8c167405454` | `AccountingReplayTest`, `test_planned_evidence_backs_nothing_and_cuts_reach_cells`, `test_holdout_disjoint_from_every_fit_validity_and_resolvable_ids`, `test_cost_reproduce`. Parked: cut propagation to presets, because `presets.js` does not exist yet; the check prints a note |
| MINOR | chunked response bytes lost when cut midway | fixed | `8c167405454` | `test_chunked_body_cut_midway_keeps_the_bytes_already_relayed` |
| MINOR | retry identity ignores `Range` and the query | fixed | `8c167405454` | `test_range_and_query_are_part_of_the_identity` |
| MINOR | fetch relay prices only one direction | fixed | `8c167405454`, `5be403b1793` | `test_inputs_split_storage_by_creator_and_count_both_directions`; contract wording narrowed (§Relay) |
| MINOR | acceptance ranges (`2000`, `8..11`) are not derived from source | labelled, derivation parked | fix round 1 | The ranges carry `basis: assumption` with a reason in the prediction files (values unchanged). Deriving them, or replacing them with tolerances from independent repetitions, needs repeated runs: LEDGER.md phase-0 follow-ups |

| Fix round 1 | silent fallbacks in `cas_probe.py` (missing `gc_rounds`, Finish without Start) | fixed: both fail the pass | fix round 1 | `test_classify_without_collected_rounds_fails`, `test_request_origin_follows_the_callers_gc_rounds` |
| Fix round 1 | drill's `server_error` remainder included attempts the store never saw | fixed: `server_error` = `upstream_error - upstream_4xx`; new class `not_sent` (unbilled) for injected, rejected or unconnected attempts; multipart Complete/Abort rows are `not_sent` | fix round 1 | `test_not_sent_attempts_are_their_own_class`, billing `test_accepted_dropped_is_billed_and_dropped_is_not` |

### Dispositions, final review, 2026-10-24 {#review-final-2026-10-24}

The gate was re-run on the fixed code: `cost-20261024T035215Z`, six PASS (the sixth is `probe_cas_expect`), `rig_commit` `8a9806dd0b7`. Every record replays from its raw ledger slice. The first test named for each fixed item failed before its fix; the extended brute force and the sweep equivalence test are guards that also pass on the old code.

| Item | What | Disposition | Commit | Tests |
|---|---|---|---|---|
| I1 | CAS lease, GC and pool-meta traffic landed in `foreground` | fixed: `lease`, `gc`, `pool_meta` families are `maintenance` (cited in `cas_probe.py`); `attribute` = in one of the caller's GC rounds or on a control family | `24d9ca2adf3` | `test_control_families_are_maintenance_outside_gc_rounds` (key shapes from gate run `cost-20261024T030843Z`), `test_lease_traffic_in_window_does_not_change_foreground` (CAS families, `gc: off`, real key shapes) |
| I2 | idle replicas measured and priced | fixed: a pass starts exactly `card.replicas` servers and records that count; a request from another workload port fails with `unexpected_caller` | `24d9ca2adf3` | `test_starts_exactly_the_cards_replicas_and_records_them`, `test_traffic_from_a_caller_beyond_the_cards_replicas_fails`; gate records say `replicas: 1` for one-replica probes |
| I3 | estimate rested on probe-sized medians and a tenth of the timeout; wrong default registry | fixed: `duration_s` per case (schema, validator, example, README); a pass costs `max(median, duration_s)`; default `inputs-registry.json`, missing registry fails | `fae8a736118`, `24d9ca2adf3` | `test_estimate_math_fallbacks_and_cut_order`, `test_default_registry_exists_and_a_missing_one_fails`, `test_phase0_coverage_declares_duration_floors`, `test_coverage_duration_is_a_floor_below_the_timeout` |
| I4 | measured passthrough rows labelled `exact` | fixed: rows declare `basis` (schema, validator, README); exactness only for `predicted`; a label contradicting the expression is a problem | `31e5df2070c` | `test_measured_rows_carry_no_exactness_claim`, `test_formulas_row_basis_is_predicted_or_measured` |
| I5 | objects of unknown size priced at zero | fixed: `uncertain_identities > 0` makes storage cells unavailable (`unknown object sizes (N)`); the evidence block lists the count | `31e5df2070c` | `test_unknown_object_sizes_make_storage_unavailable` |
| I6 | `--scale` recorded, never applied | fixed: flag removed; the record carries the manifest's `scale`; `COUNTS` reach the card scaled as `ctx.counts` | `24d9ca2adf3` | `test_record_scale_is_the_manifests_and_counts_follow_it`, parser rejects `--scale` in `test_cost_subcommand_registered_in_yardstick_py` |
| I7 | `Expect` path unproven live; no wire-bytes bound | fixed: sixth card `probe_cas_expect` (`expect_continue_min_bytes` = 1, cited); rows carry `wire_bytes_bound` (schema, validator, README); live: 24 negotiated rows, each one attempt | `64dd8717f92`, `31e5df2070c` | `CasExpectTest` (three tests), `test_expect_rejected_upstream_flags_wire_bytes_as_upper_bound`, `test_ledger_row_wire_bytes_bound`; gate PASS |
| I8 | byte-hour envelope cubic per identity | fixed: sweep over starts and the landed floor `M(t)`; 1440 overwrites well under 2 s (n=400 took 3.9 s before) | `24d9ca2adf3` | `test_many_overwrites_of_one_key_are_fast`, `test_sweep_matches_the_pointwise_definition` (300 random cases), existing bound tests unchanged |
| D1 | report glob limited to `2026-10-*` | fixed: `*-storage-cost-*` | `31e5df2070c` | `test_default_reports_glob_takes_any_date` |
| D2 | zero-byte tie-break inverted presence | fixed: ties break toward absent in `lower`, present in `upper`; brute force extended with `unknown` transitions and end states | `24d9ca2adf3` | `test_zero_byte_tie_breaks_toward_absent_in_lower_and_present_in_upper`, `test_every_schedule_with_unknown_transitions_lies_inside_bounds_and_end_states` |
| D3 | `server.sh` fallbacks killed only the tracer | fixed: fallbacks signal the server's process group | `24d9ca2adf3` | `test_stop_fallback_kills_the_traced_server_too` (fake tracer and server) |
| D4 | an `all` row beside another outcome row double-bills | fixed: `contract.load` and `validate_formulas` reject it | `31e5df2070c` | `test_all_row_beside_another_outcome_of_the_same_op_rejected`, `test_formulas_reject_an_all_row_beside_another_outcome_of_the_same_op` |
| Minor | connect failure recorded as upstream `error`, drill comment said `not_sent` | documented: the drill comment now says a connect failure is a `server_error` (unbilled like any 5XX); recording it as `not_sent` would contradict `check_rows` (a proxy-written 502 is `delivered`) | `31e5df2070c` | none (comment only) |
| Minor | a failed row write was silent | fixed: later `flush` and `stop` raise | `24d9ca2adf3` | `test_finish_removes_the_open_entry_even_when_the_write_fails` |
| Minor | sampler thread errors lost | fixed: raised at `stop` | `24d9ca2adf3` | `test_dir_sampler_thread_failure_reraised_at_stop` |
| Minor | billing hard-coded the unbilled classes | fixed: `server_error`, `dropped`, `not_sent` are free only through preset entries with `outcome_class` and source URLs | `31e5df2070c` | `test_unbilled_outcome_classes_come_from_the_preset_free_list` |
| Minor | Task 15 commit trailer deviation | recorded in LEDGER.md (attention table) | final docs commit | none |
| Minor | `active_by_creator` ignored `card.reclassify` | fixed | `24d9ca2adf3` | `test_split_follows_the_cards_reclassification` |
| Minor | report directory hard-coded | fixed: manifests set `params.report_dir` to their own report directory; `cost probe` passes `--report-dir` to the drill; phase-0 stays the default | `24d9ca2adf3` | `test_manifests_point_their_cards_at_their_own_report_directory`, `--report-dir` assertion in `test_probe_runs_six_with_drill_and_render_checks` |
| Minor | committed gate records stale | fixed: refreshed from `cost-20261024T035215Z`; README cites the run id | final docs commit | `closure_check.py --mode dev` (replay and SHA256SUMS) |
