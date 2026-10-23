# Storage Cost Calculator — Plan 1: Phase 0 — shared schemas, measurement rig, minimal evaluation slice, acceptance gate

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Freeze the shared schemas every later task consumes; extend `utils/cas-yardstick` into the measurement rig (store-side ledger proxy with `Expect` negotiation and fault injection, authenticated rig client, multi-replica cluster with embedded Keeper and interserver byte relays, `local` policy with traced per-process I/O, lifecycle replay to bounded categorised byte-hours, JSONL measurement records with process generations); build the minimal evaluation slice (typed evaluator with tables and lookups, billing with feasible cost-minimising provisioning, a minimal browser drill-down page); run the five acceptance probes end to end from ledger to the page's drill-down and record the runtime estimate.

**Architecture:** New package `utils/cas-yardstick/yardstick/cost/` beside the existing yardstick (the existing `run` command, cards and 139 tests stay untouched). A Python stdlib HTTP reverse proxy in front of RustFS writes one JSONL row per attempt; the listening port identifies the caller (`r1`, `r2`, `r3`, `rig`). A TCP relay per replica, bound to `127.0.1.<i>` and advertised as that replica's `interserver_http_host`, logs replication bytes with snapshots. A `Cluster` starts 1–3 servers (replica 1 embeds Keeper) and tracks process generations. A `CostCard` receives a `CardContext` (cluster, proxy, rig client, paths, replay) and declares tables, typed parameters, controls, mode, completion, assertions and observations; the runner writes a `Measurement` row per pass with explicit `ok` / `FAILED` / `UNSUPPORTED` shapes. The calculator's evaluator (`model.js`), contract loader (`contract.js`), billing (`billing.js`) and a minimal page (`index.html` + `ui.js` drill-down) are built here as classic browser scripts with Node export shims, so the gate proves ledger → model → billing → browser drill-down.

**Tech Stack:** Python 3.13 stdlib (`http.server`, `http.client`, `socket`, `hmac`, `hashlib`, `threading`, `json`, `unittest`); bash; `strace`; Node 22 (`node --test`, no packages); `tmp/bin/26.8/clickhouse`; RustFS 1.0.0-rc.3 binary `ci/tmp/rustfs`.

**Spec:** `docs/superpowers/specs/2026-10-23-storage-cost-calculator-design.md` (§3, §4.1–§4.6, §5.1–§5.3, §5.5, §6). Ledger of the run: `docs/superpowers/reports/2026-10-23-storage-cost-calculator/LEDGER.md`. Reviews that shaped this revision: `tmp/codex_plan_r1_findings.md`, `tmp/codex_plan_r2_findings.md`.

## Global Constraints

- Binary `tmp/bin/26.8/clickhouse`, sha256 `2a61ec2a0c0c2350745cce30523df914d5f9ee9e97728b1170407502e4121f46`, commit `09d0772a842c5d73cf3ef35b4c8b017d838215a0`; both recorded in every `Measurement`.
- Store: RustFS binary on `127.0.0.1:19121`, bucket `yardstick-cost`, credentials `clickhouse`/`clickhouse`, path-style. Proxy caller ports `r1=19131, r2=19132, r3=19133, rig=19134`. Server ports: `port_base + 10 × (i − 1)`: tcp `+0`, http `+1`, interserver `+2`; Keeper client `port_base + 8`, Keeper Raft `port_base + 9` (replica 1 only); `port_base` default `19700`. Server listeners bind `127.0.0.1`; relay `i` binds `127.0.1.<i>:<interserver port of i>`.
- Supported rig policies (exactly): `s3, s3plain, cas, local, s3_cached, s3plain_cached, cas_cached`. Model variant ids (exactly): `ebs, s3, s3plain, cas`; rig policy `local` measures variant `ebs`; `*_cached` policies measure the `cache_enabled = 1` branch of the remote variants.
- Maintenance controls never use `SYSTEM STOP MERGES`: no-merge cards use the table setting `max_parts_to_merge_at_once = 1` (`NO_MERGES`); GC via `cas_gc_enabled` in the disk config; TTL only where declared; lease and GC traffic inside a window is classified by key family into the `maintenance` bucket, never into `foreground`.
- Card mode: `deterministic` cards predict exact `foreground` attempts per op per caller; `stochastic` cards report tolerances. A deterministic card is `FAILED/window_crossing` when a workload-caller attempt starts before `mark_start` and ends inside the window, or starts inside and ends after `mark_end`.
- No third-party Python packages, no npm packages. Unit tests: `python3 -m unittest discover -s utils/cas-yardstick/tests -t utils/cas-yardstick` (139 existing must pass) and `node --test utils/storage-cost-calculator/test/`. Live tests live in `tests/live_*.py`, run with `YARDSTICK_LIVE=1`, otherwise skip.
- Commits: one per task, `git add` exact files, `git commit -s`, message ends with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. No push.
- Docs under `docs/` carry frontmatter and `{#anchors}`; comments say why, never history. `tmp/` is never committed; runs go to `tmp/cas-yardstick/cost-runs/<run-id>/`.
- Nothing here states a ClickHouse code fact as true; where a probe needs one, the implementer reads `tmp/src-26.8`, writes it with `path:line` into the probe's prediction file, and the probe asserts it. Two facts from the plan review are recorded as **obligations to verify in source**, not as truths: the CAS single-attempt S3 client may send `Expect: 100-continue` above a byte threshold; ordinary file writes may use `write`, not `pwrite64`.

## Review Focus

1. A PUT with `Expect: 100-continue` (CAS) — the proxy answers `100 Continue` itself, strips `Expect` upstream, streams the body, records `expect_negotiated: true`; counts are exact and wire bytes are flagged upper-bound for a rejected conditional write (Task 2 live `test_expect_put_through_cas_policy_counted_once`).
2. A `CompleteMultipartUpload` with HTTP 200 and an `<Error>` body — upstream S3 outcome `error`; replay creates nothing (Task 3 `test_complete_with_embedded_error_does_not_create_object`).
3. Replica 1 (Keeper) restarted inside a measured window — counters accumulate across process generations without negative deltas; the ledger window keeps the startup traffic (Task 8 `test_generation_accumulation_across_restart`; Task 6 live `test_restart_replica1_then_replicate`).
4. A manual gp3 allocation that is provider-invalid versus one that is valid but below demand — rejected with the constraint named, versus priced as entered with a note (Task 13 `test_manual_invalid_rejected_vs_below_demand_noted`).
5. An unrelated lease renewal during a deterministic CAS card — classified `maintenance`, the foreground prediction unaffected (Task 8 `test_lease_traffic_in_window_does_not_change_foreground`).

---

### Task 0: Shared schemas (frozen before any other task)

**Files:**
- Create: `docs/superpowers/cost-model/schemas/README.md`, `ledger-row.schema.json`, `measurement.schema.json`, `prediction.schema.json`, `experiment.schema.json`, `coverage.schema.json`, `registry.schema.json`, `formulas.schema.json` (grammar v1), `report-directory.md`, `examples/` (one valid instance per schema, including the three worked formula examples below)
- Create: `utils/storage-cost-calculator/tools/artifacts.py` (hand-written validators per artifact: `validate_ledger_row(d)`, `validate_measurement(d)`, `validate_prediction(d)`, `validate_experiment(d)`, `validate_coverage(d)`, `validate_registry(d)`, `validate_formulas(d)`, each returning a problem list; recursive node validation for the grammar; no generic JSON Schema interpreter)
- Test: `utils/storage-cost-calculator/tools/test_artifacts.py`

**Interfaces (the frozen shapes; later tasks reference these names verbatim):**

- **Ledger row** (`ledger-row/1`). Attempt: `{"kind":"attempt","seq":int,"caller":"r1|r2|r3|rig","store_id":str,"t_start_us":int,"t_end_us":int|null,"method","path","query":{},"req_headers":{} (content-length, content-md5, x-amz-*, if-*, range, expect, transfer-encoding),"resp_headers":{} (content-length, etag, x-amz-*, transfer-encoding),"bucket","key","op":str,"upload_id":str|null,"part_number":int|null,"copy_source":{"bucket","key","range"}|null,"req_payload_bytes","req_wire_bytes","resp_payload_bytes","resp_wire_bytes","payload_sha256":str|null,"expect_negotiated":bool,"downstream":{"outcome":"delivered|dropped|injected|rejected","status":int|null},"upstream":{"outcome":"ok|error|unknown|not_sent","http_status":int|null,"s3_code":str|null},"response_body":{"upload_id","etag","complete_error":str|null,"completed_parts":[{"part_number","etag"}]|null,"delete_results":[{"key","deleted":bool,"code":str|null}]|null,"list":{"key_count","truncated"}|null},"injected_rule":str|null,"phase":"setup|window|teardown|outside"}`. Mark: `{"kind":"mark","seq","label","t_us"}`. `phase` is filled by the runner when slicing (Task 3). `op` enum: `PutObject, CopyObject, CreateMultipartUpload, UploadPart, UploadPartCopy, CompleteMultipartUpload, AbortMultipartUpload, GetObject, HeadObject, ListObjectsV2, ListMultipartUploads, ListParts, DeleteObject, DeleteObjects, CreateBucket, HeadBucket, Unsupported, Other`.
- **Measurement** (`measurement/1`), discriminated by `status`:
  - common: `{"schema":"measurement/1","measurement_id":"<run_id>/<policy>/<card>/<case_id>/k<k>","experiment_id":"<card>.<case_id>","run_id","card","case_id","policy","variant":"ebs|s3|s3plain|cas","cache_enabled":bool,"replicas","k","scale","mode":"deterministic|stochastic","binary_sha256","commit","pass_dir","status"}`;
  - `UNSUPPORTED`: + `{"reason"}`;
  - `FAILED`: + `{"reason","diagnostics":{"server_logs":[path],"error_text"},"partial":{…any of the ok sections that were completed…}}`;
  - `ok`: + `{"effective_counts":{},"effective_settings":{"table":{},"query":{},"server":{}},"config_hashes":[str],"window":{"start_seq","end_seq","t_start_us","t_end_us","crossing":[seq]},"ledger":{"foreground":{caller:{op:Counts}},"maintenance":{caller:{op:Counts}},"setup":{…},"teardown":{…},"rig":{op:Counts}},"events":{replica:{event:delta}},"proc_io":{replica:{rchar,wchar,syscr,syscw,read_bytes,write_bytes}},"generations":{replica:[{"pid","t_start_us","t_end_us"}]},"local":{replica:{"paths":[str],"peak_allocated_bytes","samples":[[t_us,bytes]],"device","fs","attribution":"process_io","trace":{"tool":"strace","syscalls":{name:{"count","bytes","size_histogram":{bucket:count}}}}|null,"limitations":[str]}},"interserver":{relay:{"to_target","from_target","connections_total","open_at_end"}},"inventory":{"initial":"empty_verified|declared","byte_hours":{category:{"lower","upper"}},"uncertain_identities":int},"observations":{},"assertions":{name:{"expected","actual","ok"}},"wall_s","unit_count"}`; `Counts = {"attempts","delivered","dropped","injected","rejected","upstream_ok","upstream_error","upstream_unknown","req_payload_bytes","resp_payload_bytes","list_pages","list_keys","delete_calls","delete_keys"}`.
- **Experiment manifest** (`experiment/1`, typed parameters only, no action grammar): `{"schema":"experiment/1","experiment_id":"<card>.<case_id>","card","case_id","policies":[…],"replicas","mode","controls":{"merges":"stop|measure","gc":"off|measure","ttl":"none|measure"},"settings":{"table":{},"query":{},"server":{}},"params":{…card-typed…},"scale":float,"repeats":int,"timeout_s":int,"description"}`.
- **Prediction / fixture** (`prediction/1`, file `predictions/<experiment_id>.<policy>.json`): `{"schema":"prediction/1","experiment_id","policy","inputs":{registry_id:value},"expected":{"foreground":{caller:{op:int|{"min","max"}}},"observations":{}},"tolerance":{"abs":{},"rel":{}},"source_refs":[{"claim","path","line"}],"prediction_commit"}`. Roles live in `coverage.evidence`, not here.
- **Coverage** (`coverage/1`): `{"schema":"coverage/1","version","cards":[{"name","family","model_variants":[…],"cases":[{"case_id","policies":[…],"replicas","scale","repeats","timeout_s","priority":"P1|P2","status":"planned|run|cut"}]}],"evidence":[{"coefficient","experiment_id","policy","span":{param:[lo,hi]},"role":"fit|holdout|validation","prediction_commit":str|null,"measurement_ids":[str],"status":"planned|done|consumed|cut"}],"domain":{"variants":[…],"operations":[…],"families":[…]},"excluded":[str]}`.
- **Registry** (`registry/1`): `{"schema":"registry/1","version","quantities":[{"id","block","label","kind":"input|pinned|measured|derivation","scope":"cluster|replica|pool|volume","unit","value_type":"number|enum|table","enum":[…]|null,"table":{"index":[enum_input_ids],"dim"}|null,"default":number|str|null,"rule":node|null,"coef":str|null,"validity":{param_id:[lo,hi]}|null,"evidence":{"kind":"assumed|source|measured|provider","ref","version"},"hint":{"sql","mapping":{column:field}}|null,"status":"ok|unsupported"}],"sets":{set_id:[member_ids]},"precedence":[{"group":[ids],"default_locked":[ids],"rules":{derived_id:node}}]}`. Groups never share members (validator rule).
- **Formulas grammar v1** (`formulas/1`): node `{"kind":K,"dim":str,…}`, `K ∈ {input(id), coef(id), lookup(table_id, keys:[node]), var(name), const(value), add(a,b), sub(a,b), mul(a,b), div(a,b,on_zero:"zero|unavailable"), min(args), max(args), ceil_to(x,step), floor_to(x,step), branch(cond{op,a,b},then,else), sum_over(set,var,body), price(unit,rule_id,quantity)}`; `var` legal only inside `sum_over`; values are numbers or `unavailable`; a formulas file: `{"schema":"formulas/1","owner","variant","cells":{operation:{"ledger_rows":[{"row_type":"request|storage|transfer|block_io|block_capacity","op_kind":str,"outcome_class":"ok|client_error|server_error|dropped|all","category":"active|outdated|garbage|temporary|incomplete|detached|metadata"|null,"route":"ec2_s3_same_region|ec2_ec2_cross_az|internet"|null,"scope":"variant|replica|volume","expr":node}],"allocation":{"shares":[{"cell","weight":node}]}|null,"states":[{"id":"not_supported|backlog_grows","cond":node|true,"reason"}],"notes":[node]}}}`. Coefficient tables: `coefficients.json` entries `{"id","kind":"scalar|table","dim","value":number|{"index":[…],"values":{"<k1>|<k2>":number}},"validity","tolerance":{"abs","rel"},"evidence":{"kind","ref","version","measurement_ids":[…],"binary_sha256","commit"},"status"}`. Worked examples shipped in `examples/`: (1) part type: `branch(cond{op:"<", a:input(uncompressed_bytes_per_part), b:coef(min_bytes_for_wide_part)}, const(compact), …)` — enums are modelled as `const` with `dim:"enum:part_type"`; (2) objects per part: `lookup("objects_per_part", [input(shape), var(part_type)])`; (3) multipart parts: `branch(size ≤ first_buffer → const 1, else add(1, ceil_to(div(sub(size, first_buffer), part_size), 1)))` with coefficients for the sizing rule the research establishes.
- **Report directory** (`report-directory.md`): `docs/superpowers/reports/2026-10-<dd>-storage-cost-<name>/` with `README.md`, `measurements.jsonl`, `predictions/`, `experiments/`, `coefficients.json`, `formulas.json`, `raw/` (ledger slices, relay logs, strace outputs for cited measurements) + `raw/SHA256SUMS`.

- [ ] **Step 1: Failing tests** — one `test_<artifact>_example_valid_and_each_required_field_detected` per artifact, `test_formulas_rejects_var_outside_sum_over`, `test_formulas_lookup_requires_table_coefficient_arity`, `test_registry_groups_do_not_share_members`, `test_measurement_failed_shape_requires_diagnostics_and_allows_partial`, `test_coverage_evidence_requires_prediction_commit_for_holdout`.
- [ ] **Step 2: Run → FAIL. Step 3: Write schemas, examples, README, validators. Step 4: PASS.**
- [ ] **Step 5: Commit** — `cost-model: shared schemas v1 and artifact validators`.

### Task 1: RustFS launcher and authenticated rig client

**Files:**
- Create: `utils/cas-yardstick/rustfs-bin.sh`, `utils/cas-yardstick/yardstick/cost/__init__.py`, `utils/cas-yardstick/yardstick/cost/s3client.py`
- Modify: `utils/cas-yardstick/README.md` ("Cost rig")
- Test: `tests/test_cost_s3client.py` (offline), `tests/live_rustfs_bin.py`

**Interfaces:**
- `rustfs-bin.sh start [--data DIR] [--bin PATH] | stop | status | ensure`: `start` refuses when up; `ensure` starts only when down and writes `<data>/.started-by-rig`; `stop` stops only when `.started-by-rig` exists or `--force`; env as `rustfs.sh`; `RUSTFS_ADDRESS=127.0.0.1:19121`; pid file `<data>/.rustfs.pid`; ready = `GET /` 403 **and** `python3 -m yardstick.cost.s3client ensure-bucket yardstick-cost` succeeds.
- `class S3Client(endpoint, access_key, secret_key, region='us-east-1')`: SigV4 path-style; `ensure_bucket`, `put`, `get`, `head`, `delete`, `delete_prefix -> int` (verbose `DeleteObjects` batches), `list -> [{key,size,etag}]` (paginated), `list_multipart_uploads`, `list_parts`, `create_multipart`, `upload_part`, `complete_multipart(parts:[{part_number,etag}])`, `abort_multipart`. Through the proxy: `endpoint='http://127.0.0.1:19134'` and `Host` signed as sent (the proxy forwards `Host`, path and query byte-exact).

- [ ] **Step 1: Offline tests** — `test_sigv4_matches_aws_doc_example` (vectors + URL in the test), `test_list_pagination`, `test_delete_prefix_batches`.
- [ ] **Step 2–4: Run/implement/pass.**
- [ ] **Step 5: Live** — `test_start_status_stop_cycle_pid_ownership_and_refusal` (subprocess), `test_ensure_is_idempotent_and_stop_respects_ownership`, `test_put_get_head_delete_roundtrip`, `test_multipart_create_upload_list_complete_retires_all_parts` (3 parts, complete with 2 → `list_parts` on the upload fails/empty and the object has the two parts' size; record RustFS behaviour vs the AWS documentation in the test's docstring), `test_multipart_abort_roundtrip`.
- [ ] **Step 6: README.** **Step 7: Commit** — `yardstick: RustFS launcher and SigV4 rig client`.

### Task 2: Ledger proxy

**Files:**
- Create: `utils/cas-yardstick/yardstick/cost/proxy.py`
- Test: `tests/test_cost_proxy.py` (fake upstream), `tests/live_proxy_rustfs.py`

**Interfaces:**
- `class LedgerProxy(upstream: (host,port), store_id: str, ledger_path, callers: dict[str,int])`: `start()`, `stop()`, `mark(label) -> int`, `set_fault(rule) -> str`, `clear_faults()`, `flush()`. Fault rule as revision 2 plus `"upload_id": str|"*"`; actions `status{status, send_upstream}` and `drop_response{send_upstream:true}`; `send_upstream:false` drains the body and records `upstream.outcome='not_sent'`.
- Framing: request line, `Host`, path, query and signed headers forwarded byte-exact. **`Expect: 100-continue`**: the proxy answers `HTTP/1.1 100 Continue` to the client itself, removes `Expect` from the upstream request, then streams the body; `expect_negotiated: true`. Bodies with `Content-Length` are streamed in 1 MiB chunks while hashing and counting. `Transfer-Encoding: chunked` **requests** are answered `411` and recorded `downstream.outcome='rejected'`, `op='Unsupported'`. Upstream responses: status line and headers relayed; `Content-Length` bodies streamed; chunked upstream responses are re-chunked to the client as they arrive (no whole-body buffering); `HEAD` carries no body. Timeouts: 300 s read per direction, 60 s idle keep-alive; client disconnect mid-request → upstream connection closed, `downstream.outcome='dropped'`, `upstream.outcome='unknown'` if the response was not fully read. Error-body parsing only for `non-2xx` responses and for `CompleteMultipartUpload` 200 bodies with an XML content type (never for `GetObject` payloads). Parsed: `uploadId` from `CreateMultipartUpload` responses, `ETag` headers, `DeleteObjects` request keys and response entries (quiet mode: absent = deleted), `ListObjectsV2` `KeyCount`/`IsTruncated`, `CompleteMultipartUpload` request parts (number + ETag), `x-amz-copy-source(-range)`.

- [ ] **Step 1: Offline tests** — `test_forwards_put_preserving_request_line_host_query_and_signed_headers`, `test_rows_valid_per_artifacts_validator`, `test_expect_100_continue_answered_by_proxy_and_stripped_upstream` (fake upstream asserts no `Expect` header; client receives `100` then `200`), `test_chunked_request_rejected_411_recorded`, `test_chunked_upstream_response_rechunked_streaming` (fake upstream sends 3 chunks with delays; client receives 3 chunks before the upstream finishes), `test_head_no_body`, `test_fault_status_not_sent_drains_body`, `test_fault_drop_response_upstream_ok_downstream_dropped`, `test_fault_targets_upload_and_part`, `test_client_disconnect_marks_unknown`, `test_mark_and_seq_share_lock`, `test_payload_sha256_and_wire_bytes`, `test_complete_200_with_error_parsed_and_get_payload_never_parsed`, `test_delete_objects_verbose_and_quiet_parsed`, `test_completed_parts_parsed`, `test_keepalive_after_rejected_and_injected_requests`, `test_concurrent_500_rows_distinct_seq`; memory test in a separate process: `tests/test_cost_proxy_memory.py` spawns the proxy via `subprocess` with a tiny upstream, pushes a 64 MiB PUT and samples the proxy process's `VmRSS` at 50 ms → peak − baseline < 16 MiB.
- [ ] **Step 2–4: Run/implement/pass.**
- [ ] **Step 5: Live** (`live_proxy_rustfs.py`): signed round trips through caller `rig`; multipart create/upload/complete/abort; drop-response on `UploadPart` then client retry → two rows same `(upload_id, part_number)`; `test_expect_put_through_cas_policy_counted_once` is run in Task 14's probes (needs a server).
- [ ] **Step 6: Commit** — `yardstick: ledger proxy with Expect negotiation, framing rules and fault injection`.

### Task 3: Ledger slicing, windows, retry correlation, bounded lifecycle replay

**Files:**
- Create: `utils/cas-yardstick/yardstick/cost/ledger.py`
- Test: `tests/test_cost_ledger.py`

**Interfaces:**
- `parse_operation(method, path, query, headers) -> op` (rules as revision 2 plus `ListMultipartUploads`, `ListParts`, `CreateBucket`, `HeadBucket`).
- `read_rows(path)`; `slice(rows, marks: {setup_start, window_start, window_end, teardown_end}) -> Sliced{attempts_by_phase, crossing}` with `phase` assigned per attempt by `seq` against the marks and `crossing` = attempts whose `[t_start_us, t_end_us]` straddles `window_start` or `window_end` (both boundaries).
- `classify(attempts, families, maintenance_families: set[str]) -> {'foreground': [...], 'maintenance': [...]}` for workload callers; `summarize(attempts) -> {caller: {op: Counts}}`.
- `correlate_retries(attempts)`: identity = caller, op, store, bucket, key, upload_id, part_number, `payload_sha256`, conditional headers; predecessor with `downstream ∈ {dropped, injected}` or `upstream ∈ {error, unknown}`; else uncorrelated.
- `replay(attempts, initial: Inventory, families) -> Bounds{lower: Inventory, upper: Inventory}`: a known-ok mutation takes effect at `t_end_us` in `lower` and at `t_start_us` in `upper` (time bounds); an `unknown` outcome applies the mutation in `upper` and not in `lower` (state bounds); write/copy replace the identity (copy size = size of the source identity in the same inventory; unknown source → the identity is `uncertain` in both bounds); `UploadPart` ok → part identity replaced; `CompleteMultipartUpload` ok and no embedded error → object created with the sum of `completed_parts`, **all** parts of that upload retired; abort → all parts retired; `DeleteObject`/`DeleteObjects` per results; `error`/`not_sent`/`rejected` → unchanged. `Inventory.reclassify(identities, category, t_us, evidence)` for logical categories attached by cards.
- `byte_hours(bounds, t0_us, t1_us) -> {category: {lower, upper}}`; `check_rows(attempts) -> problems`.

- [ ] **Step 1: Tests** — `test_parse_operation_table`, `test_slice_phases_and_crossing_both_boundaries`, `test_classify_maintenance_by_family`, `test_summarize_shape`, `test_retry_correlation_identity_rules` (six cases), `test_replay_put_replaces_identity_only`, `test_replay_complete_retires_all_parts_including_omitted_and_sums_listed`, `test_complete_with_embedded_error_does_not_create_object`, `test_replay_abort_retires_parts`, `test_batch_delete_absent_key_is_deleted`, `test_batch_delete_quiet_mode`, `test_batch_delete_per_key_error_keeps_object`, `test_unknown_outcome_bounds_differ`, `test_time_bounds_lower_at_end_upper_at_start`, `test_copy_size_from_source_identity_else_uncertain`, `test_byte_hours_lower_upper_clipped`, `test_reclassify_outdated_then_garbage`.
- [ ] **Step 2–4.** **Step 5: Commit** — `yardstick: ledger slicing, retry correlation and bounded lifecycle replay`.

### Task 4: Interserver byte relay with snapshots

As revision 2 (`ByteRelay`, `snapshot(label)` including open connections, per-connection rows, sender-exact attribution, receiver by construction).

- [ ] Tests: `test_snapshot_includes_open_connection_bytes`, `test_connection_row_on_close`, `test_relay_survives_target_refusal`, `test_bind_alias_and_real_listener_same_port`. Commit — `yardstick: interserver byte relay with snapshots`.

### Task 5: Topology and configuration

**Files:** Modify `yardstick/config.py` (add `write_cost_config`), Create `yardstick/cost/topology.py`, Test `tests/test_cost_config.py`.

**Interfaces:**
- `Topology` as revision 2 plus `preflight_new_endpoints() -> [problems]` (only server, Keeper and relay endpoints must be free) and `verify_services(proxy: LedgerProxy, rig: S3Client) -> [problems]` (proxy object owns its ports; `rig.ensure_bucket` succeeds).
- `write_cost_config(replica_dir, replica, policy, topology, key_prefix, local_dir, gc_enabled, cas_server_root_id, server_settings: dict) -> hash`: `server_settings` (from the experiment manifest, e.g. a CAS disk setting) are rendered into the disk block or `<clickhouse>` root as given (keys are XML paths like `storage_configuration.disks.<disk>.cas_gc_interval_sec`); layout, Keeper, macros, listen and interserver host as revision 2.

- [ ] Tests as revision 2 plus `test_server_settings_rendered_at_xml_path`, `test_preflight_new_endpoints_ignores_proxy_and_store_ports`. Commit — `yardstick: cost topology and multi-replica config`.

### Task 6: Cluster control with process generations

**Files:** Create `yardstick/cost/cluster.py`, Test `tests/test_cost_cluster.py`, `tests/live_cluster.py`.

**Interfaces:**
- `Cluster(binary, pass_dir, topology, policy, key_prefix, local_dirs, gc_enabled, server_settings, server_sh)`: `start_all()` (preflight new endpoints → r1 → readiness `SELECT count() FROM system.zookeeper WHERE path='/'` → others → relays; partial failure stops started components and raises with log paths), `stop(i)`, `start(i) -> seconds`, `stop_all()`, `clients`, `relays`, `generations: {i: [{pid, t_start_us, t_end_us}]}`, `snapshot_counters(i) -> {events, proc_io}` which **closes the current generation's counters on `stop(i)`** (final read before SIGTERM) and accumulates across generations: `counters_since(i, baseline) = Σ closed generations + current − baseline`.

- [ ] Fake tests: `test_start_order_and_readiness`, `test_partial_failure_cleanup`, `test_generation_accumulation_across_restart` (counters 100 → stop (closed at 150) → start (new gen 0 → 30) → delta since baseline 100 = 80). Live: `test_cli_startup_sequence_rustfs_proxy_cluster` (RustFS ensure, proxy start, 2-replica `s3` cluster start — the exact CLI order), `test_two_replicas_replicate_through_relay`, `test_restart_replica1_then_replicate` (Keeper session recovery, then a new insert replicates). Commit — `yardstick: multi-replica cluster control with generations`.

### Task 7: Traced local I/O and the `CostCard` interface

**Files:** Create `yardstick/cost/localio.py`, `yardstick/cost/cards/__init__.py`, `yardstick/cost/cards/base.py`, Test `tests/test_cost_localio.py`, `tests/test_cost_cards_base.py`.

**Interfaces:**
- `proc_io(pid)`, `DirSampler(paths, interval_s=0.5)` counting allocated bytes with hardlink dedup and returning `samples` and `peak`; paths for a replica = `data/`, `s3_meta/`, `caches/`, `cas_scratch/` (if present), the `local` disk dir — the **union** peak is reported; `device_of(path)`; `Tracer(pid_launcher)`: launches the server under `strace -f -ttt -yy -e trace=write,pwrite64,writev,read,pread64,readv,fsync,fdatasync -o <pass_dir>/strace.r<i>.txt` when the card requests `trace: true` (the server.sh gets a `TRACE_PREFIX` env var honoured as a command prefix); `parse_strace(path) -> {syscall: {count, bytes, size_histogram}}` attributing by fd path (the `-yy` decoration); `preflight_trace() -> [problems]` (strace present, `ptrace_scope` allows tracing own children); `limitations(...)`.
- `CardContext = {cluster, proxy, rig, pass_dir, local_dirs, policy, variant, cache_enabled, manifest, replay: () -> Bounds, window: () -> Sliced}`.
- `class CostCard`: `name`, `policies`, `mode`, `params_schema` (typed manifest params), `tables(ctx)`, `controls(ctx)`, `settings(ctx) -> {table, query, server}` (server settings applied before configs are written), `supports(policy) -> (bool, reason)`, `setup(ctx)`, `measured(ctx) -> {unit_count, ops, observations}`, `complete(ctx, timeout_s)` (default as revision 2; cards override), `expected(ctx)`, `assertions(ctx, draft) -> {name: (expected, actual)}`, `inventory_families(ctx) -> [{name, regex, category, maintenance: bool}]`, `expected_exceptions: tuple` (exceptions a card expects from fault injection; caught by the runner and recorded as observations, not failures), `teardown(ctx)`; `replicated_engine(...)`; `--replicas` precedence as revision 2.

- [ ] Tests — `test_proc_io_grows`, `test_dir_sampler_union_peak_and_hardlink_dedup` (synchronised: the test writes, waits for a sample callback, deletes), `test_parse_strace_attributes_by_fd_path_and_bins_sizes` (fixture with `write(3</path/a.bin>, …)` lines), `test_preflight_trace`, `test_default_complete_queries` (FakeCluster), `test_expected_exceptions_are_observations`, `test_replicas_precedence`. Commit — `yardstick: traced local I/O and CostCard interface`.

### Task 8: Runner and measurement records

**Files:** Create `yardstick/cost/measurements.py`, `yardstick/cost/runner.py`, Test `tests/test_cost_runner.py`.

**Interfaces:**
- `Deps(binary, run_id, runs_dir, topology, proxy, rig, store_id, scale, keep_prefix: bool)`.
- `run_case(card, manifest, policy, k, deps) -> record`: `pass_dir`, `key_prefix`, local dirs; verify prefix empty (`rig.list`), else `FAILED/prefix_not_empty`; `mark('setup_start')`; cluster start (with `settings.server`, trace if requested); setup; `mark('window_start')`; counter snapshots; `measured` (expected exceptions caught); `complete`; `mark('window_end')`; snapshots; teardown; `mark('teardown_end')`; `flush`; slice/classify/summarize; crossing rule for deterministic cards; replay over the whole pass (`initial: empty_verified`) with families; byte-hours over the window; assertions; record. Cleanup in nested `finally`: clear faults → teardown (if not done) → `stop_all` → `rig.list_multipart_uploads` + abort each → `rig.delete_prefix` unless `keep_prefix` → verify prefix empty; cleanup outcome recorded in the record (`cleanup: {aborted_uploads, deleted_keys, verified_empty}`), and a failed cleanup never discards an already-complete record.
- `measurements.write(path, record)` validates via `artifacts.validate_measurement`.

- [ ] Tests — `test_ok_record_shape_and_phases`, `test_failed_record_has_diagnostics_and_partial`, `test_unsupported_record`, `test_deterministic_crossing_fails`, `test_lease_traffic_in_window_does_not_change_foreground` (maintenance family rows present; foreground counts equal the fixture prediction), `test_expected_exception_recorded_as_observation`, `test_cleanup_order_and_record_persisted_after_cleanup_failure`, `test_generation_accumulation_through_runner_restart` (card restarts a replica in `measured`). Commit — `yardstick: cost runner and measurement records`.

### Task 9: `cost` CLI and runtime estimate

**Files:** Modify `yardstick.py`; Create `yardstick/cost/cli.py`, `yardstick/cost/estimate.py`; Test `tests/test_cost_cli.py`.

**Interfaces:**
- `cost run --binary --cards --cases (ids or 'all') --policies --replicas --repeats --scale --runs-dir [--endpoint] [--keep-prefix]`: `rustfs-bin.sh ensure` unless `--endpoint`; proxy in-process; `verify_services`; matrix from the manifests; table; exit codes as revision 2.
- `cost probe --binary --runs-dir`: runs the five probes, then for each: `node tools/drill.mjs` (JSON) and the browser-page render check of Task 13 (`node tools/render_check.mjs` compares the page's drill-down HTML for the probe against known substituted values); PASS/FAIL; exit 2 on FAIL.
- `cost estimate <run-dir> --coverage …`: for each `cards[*].cases[*]` with status `planned`: `passes = len(policies) × repeats`; `pass_seconds = median full pass of the same card from the run dir, else of the same family, else of the sampled largest case`; adds `timeout_s × 0.1` and cleanup median; total × 1.25; prints P1 hours, P1+P2 hours; cut order (P2 by family → `replication-zero-copy` → `types-*`) with, for each cut, the dependent coefficients (`coverage.evidence`), registry quantities (`registry.coef`) and presets (from `presets.js` ids read as JSON via `node -e`) it disables.

- [ ] Tests — `test_run_dispatch_and_exit_codes`, `test_probe_runs_five_with_drill_and_render_checks`, `test_estimate_math_fallbacks_and_cut_traversal` (fixtures). Commit — `yardstick: cost CLI and runtime estimate`.

### Task 10: Artifact seeds and closure checks

**Files:** Create `docs/superpowers/cost-model/measurement-contract.md`, `inputs-registry.json`, `coverage.json`, `billing-contract.json`, `utils/storage-cost-calculator/tools/closure_check.py`; Test `tools/test_closure_check.py`.

**Interfaces:**
- Seeds: registry with the simple inputs, `cache_enabled`, `shape`/`part_type`/`type_mix` enums, the precedence group, `sets` (empty), and the advanced-block skeleton (block names and labels, no measured quantities yet); coverage with `domain` (variants × operations × families from spec §5.6) and the §5.6 cards with `cases` carrying scale/repeats/timeout; billing contract as revision 2 plus `ec2_ebs_bandwidth: [{instance_class, max_mibps, source_url, checked}]` and `multipart: {abort_days_after_initiation_default: 7, residence_time_assumption: "measured_or_assumed"}`; price preset id `aws-us-east-1` everywhere.
- `closure_check.py` with `dependency`, `support` (enumerates the full `domain`; a missing cell is a problem), `evidence`, `accounting` (prediction exactness for deterministic measurements; inventory bounds consistent; billing/allocation fixtures present and passing via `node --test` of the pricing fixtures), `reproduction`; modes `--mode dev` (checks only the artifacts present) and `--mode final` (missing artifacts are failures); `main()`.

- [ ] Tests — `test_dependency_unbound_var_scope_and_table_arity`, `test_support_enumerates_domain_and_cut_rule`, `test_evidence_distinct_holdout`, `test_accounting_exactness_and_bounds`, `test_reproduction_stale_copy`, `test_final_mode_fails_on_missing_artifact`, `test_seeds_pass_dev_mode`. Commit — `cost-model: artifact seeds and closure checks`.

### Task 11: Typed expression evaluator (classic script + Node shim)

**Files:** Create `utils/storage-cost-calculator/model.js`, `test/model.test.js`.

**Interfaces:**
- Distribution: classic scripts attaching to `window.SCC`; `if (typeof module !== 'undefined') module.exports = SCC.model;`.
- `SCC.model.evaluate(node, ctx) -> {value: number|null, state: 'ok'|'unavailable', extrapolated: boolean, trace}`; `ctx = {inputs: Map, coefs: Map<id, {kind, value, validity}>, tables: Map<id, {index, values}>, sets: Map, vars: Map, prices}`; `lookup` resolves enum keys to a table cell; `evaluateCell(cell, ctx) -> {rows: [{row…, quantity, trace}], allocation: {cell: weight}|null, extrapolated}`; `checkDims(node, registry)`; `render(node, registry) -> string`.

- [ ] Tests — `test_const_input_coef_lookup`, `test_dims_combine_and_mismatch`, `test_div_on_zero_policies`, `test_branch_records_branch_taken`, `test_sum_over_binds_var`, `test_lookup_missing_key_unavailable`, `test_coef_outside_validity_extrapolated`, `test_unavailable_propagates`, `test_render_symbolic_fixture`, `test_fractional_values_not_rounded_without_node`, `test_three_worked_examples_from_schemas` (part type, objects per part, multipart parts). Commit — `calculator: typed expression evaluator`.

### Task 12: Contract loader and formulas merge

As revision 2, with the contract shape: `{schema:"contract/1", version, manifest:{code_sha256 (over model.js, contract.js, billing.js, pipeline.js, presets.js — the latter two empty until Plan 3), contract_version, registry_version, coverage_version, billing_version, evidence:[{report, commit, raw_sha256sums}]}, evidence_statement, registry, sets, precedence, coefficients, tables, billing, variants, operations, applicability (variant × operation × family), formulas, states}`.

- [ ] Tests — `test_load_valid_minimal`, `test_duplicate_owner_rejected`, `test_unregistered_input_rejected`, `test_merge_rejects_two_owners_of_cell`, `test_applicability_domain_complete`. Commit — `calculator: contract loader and formulas merge`.

### Task 13: Billing, provisioning, drill-down printer and the minimal page

**Files:** Create `billing.js`, `tools/drill.mjs`, `tools/render_check.mjs`, `index.html` (template with markers and containers; the drill-down container works stand-alone with `?measurement=<url>&formulas=<url>` for the gate), `ui.js` (drill-down rendering only; Plan 3 extends it), `styles.css` (tokens only), `test/billing.test.js`, `test/ui_drill.test.js`, `test/fixtures/pricing/*.json`.

**Interfaces:**
- `SCC.billing.bill(rows, preset, scope, outcomeMix: {op_kind: {ok, client_error, server_error, dropped}}) -> {lines, total}`: rows with `outcome_class:'all'` are split by `outcomeMix` (default: all `ok`); request classes per 1000; `free` outcomes from the preset by `(status, s3_code, condition)`; storage tiers once; `incomplete` as storage; transfer by `route` (`ec2_s3_same_region` free, `ec2_ec2_cross_az` per GB per direction, `internet` egress); block via `allocateVolumes`; decimal units.
- `SCC.billing.allocateVolumes(demand: {capacity_gib (headroom already applied), peak_iops, peak_mibps}, preset, layouts, manual) -> {layout, volumes, monthly_cost, feasible, manual, notes, alternatives}`: per layout `n`: size = `max(ceil(capacity/n), ceil(iops_per_volume / iops_per_gib_max), size_min)`, `iops_per_volume = max(baseline, ceil(peak_iops/n), ceil(peak_mibps/n / mibps_per_iops_max))`, `mibps = max(baseline, ceil(peak_mibps/n))`; reject only when a bound cannot be met by increasing size; cheapest with tie-break; zero demand → smallest layout at baseline; manual: invalid → `feasible:false` + constraint; valid below demand → priced + note. Per-replica allocation; the formulas multiply by `replicas`.
- `tools/drill.mjs <measurement.jsonl> <prediction.json> <formulas.json> <billing-contract.json> <registry.json> --cell v/op --json`: the five stages, exit 1 on a deterministic mismatch.
- `index.html` + `ui.js`: `SCC.ui.renderDrill(container, stages)` renders the five stages (inputs, branch per row, rows predicted vs measured, billing lines, allocation) with `data-value` attributes; `tools/render_check.mjs` renders the same stages with the same function in Node (string output) and asserts the known substituted values and money appear; the browser check opens `index.html?measurement=…` via `file://` and `http.server`.

- [ ] Tests — `test_request_classes`, `test_outcome_mix_split_and_free_list`, `test_storage_tiers_once`, `test_incomplete_as_storage`, `test_transfer_routes`, `test_decimal_conversion`, `test_allocate_increases_size_for_iops_cap_instead_of_rejecting` (independent hand calculation in the fixture), `test_allocate_min_cost_tiebreak_alternatives`, `test_mibps_iops_coupling`, `test_manual_invalid_rejected_vs_below_demand_noted`, `test_zero_demand`, `test_pricing_fixtures_exact`, `test_drill_json_and_exit_code`, `test_render_drill_contains_substituted_values_and_money` (DOM-free string render; asserts specific numbers from a fixture, not headings). Commit — `calculator: billing, provisioning, drill-down printer and minimal page`.

### Task 14: Acceptance probes (phase-0 gate)

**Files:** Create `yardstick/cost/cards/probes.py`, report directory `docs/superpowers/reports/2026-10-24-storage-cost-phase0/` (convention, with `experiments/*.json`, `predictions/*.json`, `formulas.json` for the probe cells), and two **sample manifests** for the estimate (`experiments/merge-natural.sample.json`: 64 parts × 100 000 rows, 5 columns, natural merges; `experiments/gc-scale.sample.json`: 100 000 CAS objects then one GC round) whose cards are minimal versions owned later by Plan 2 Tasks 1 and 4. Register probes in `cards/__init__.py`. Tests `tests/test_cost_probes.py`, `tests/live_probes.py`.

**Probes (manifests carry every parameter; predictions cite source):**
- `probe_s3_insert` — as revision 2; mode deterministic.
- `probe_replicated_fetch` — as revision 2 plus `system.part_log` evidence on r2 (`event_type = 'DownloadPart'` count 1).
- `probe_multipart_fault` — `s3`, 1 replica; table `k UInt64, s String CODEC(NONE)`; generator `repeat(hex(sipHash64(number)), 64)` so each row is 1 KiB uncompressed and uncompressed = on-disk; rows `N` from the manifest, chosen so the `s` column object exceeds the multipart threshold the prediction cites; faults: `UploadPart part_number=2 nth=1 drop_response`, `CompleteMultipartUpload nth=* status 500 send_upstream:false`, `AbortMultipartUpload nth=* status 500 send_upstream:false`; `expected_exceptions = (RuntimeError,)` for the failing INSERT; assertions before cleanup: exactly one `CreateMultipartUpload`; the predicted number of distinct part identities; part 2 has two attempts with identical `payload_sha256`, correlated; `upper` and `lower` inventories both show the predicted residue bytes (`N × 1024` ± the predicted part framing) as `incomplete`; no object created; after rig abort: incomplete 0; `incomplete` byte-hours > 0; drill shows the incomplete-storage line.
- `probe_cas_gc_cohort` — as revision 2 with `settings.server` carrying the GC interval, cohort from the cited key families, DROP completion override, GC-log evidence, `reclaim_delay_s`, `garbage` byte-hours bounds > 0; and the `Expect` check: the ledger rows of this probe with `expect_negotiated: true` are counted once each (the prediction states whether the pinned CAS client sends `Expect`, cited).
- `probe_local_volume` — `local`, 1 replica, `trace: true`; 200 inserts × 20 000 rows into a 10-column table; assertions: `proc_io.write_bytes > 0`; `trace.syscalls.write.count > 0` **or** `pwrite64.count > 0` (whichever the source uses; the prediction cites it) with a size histogram attributed to paths under the data dir; union peak > 0; `device`, `fs`, `limitations` recorded; `drill.mjs` prices the capacity peak and IOPS demand through `allocateVolumes`; the page render check shows the allocation.

- [ ] Fixture tests for each probe's assertions (both directions), `test_cohort_excludes_control_families`, `test_multipart_residue_prediction_arithmetic`.
- [ ] Predictions committed before the live run. **Live gate** — `YARDSTICK_LIVE=1 python3 utils/cas-yardstick/yardstick.py cost probe --binary tmp/bin/26.8/clickhouse --runs-dir tmp/cas-yardstick/cost-runs > tmp/cost_probe.log 2>&1; echo $?` → 0; five PASS lines with drill JSON and render checks. A FAIL: systematic-debugging; prediction, instrumentation, setup and attribution checked independently; the original prediction kept with the demonstrated cause.
- [ ] **Runtime estimate** — run the two sample manifests once at planned scale; `cost estimate`; record hours, cut order and disabled quantities/presets in LEDGER.md.
- [ ] **Report** `README.md`, `raw/`, `SHA256SUMS`. **Commit** — `yardstick: acceptance probes and phase-0 gate record`.

### Task 15: Phase-0 review

- [ ] Codex review (scope: schemas, `measurement-contract.md`, `yardstick/cost/*.py`, `model.js`, `contract.js`, `billing.js`, `ui.js` drill, the phase-0 report; questions as revision 2) → close MAJORs test-first → `measurement-contract.md` §Review → LEDGER.md phase 0 done with gate numbers and cut decision → commit `cost-model: phase-0 review dispositions`.
