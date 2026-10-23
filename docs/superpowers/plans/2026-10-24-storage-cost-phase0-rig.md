# Storage Cost Calculator — Plan 1: Phase 0 — shared schemas, measurement rig, minimal evaluation slice, acceptance gate

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Freeze the shared schemas every later task consumes; extend `utils/cas-yardstick` into the measurement rig (store-side ledger proxy with fault injection, authenticated rig client, multi-replica cluster with embedded Keeper and interserver byte relays, `local` policy with per-process I/O accounting, lifecycle replay to categorised byte-hours, JSONL measurement records); build the minimal evaluation slice (typed evaluator, billing with feasible cost-minimising provisioning, a drill-down printer); run the five acceptance probes end to end from ledger to drill-down and record the runtime estimate.

**Architecture:** New package `utils/cas-yardstick/yardstick/cost/` beside the existing yardstick (the existing `run` command, cards and 139 tests stay untouched). A Python stdlib HTTP reverse proxy in front of RustFS writes one JSONL row per attempt; the listening port identifies the caller (`r1`, `r2`, `r3`, `rig`). A TCP relay per replica, bound to `127.0.1.<i>` and advertised as that replica's `interserver_http_host`, logs replication bytes per connection with periodic snapshots. A `Cluster` starts 1–3 servers; replica 1 embeds Keeper. A `CostCard` declares tables, counts, maintenance controls, completion condition, assertions and typed observations; the runner writes a `Measurement` row per pass. The calculator's evaluator (`model.js`) and billing (`billing.js`) are built here as classic browser scripts with a Node export shim, and `tools/drill.mjs` prints the five stages of a cell from a measurement row and a formulas file, so the gate proves ledger → model → billing → explanation.

**Tech Stack:** Python 3.13 stdlib (`http.server`, `http.client`, `socket`, `hmac`, `hashlib`, `threading`, `json`, `unittest`); bash; Node 22 (`node --test`, no packages); `tmp/bin/26.8/clickhouse`; RustFS 1.0.0-rc.3 binary `ci/tmp/rustfs`.

**Spec:** `docs/superpowers/specs/2026-10-23-storage-cost-calculator-design.md` (§3, §4.1–§4.6, §5.1–§5.3, §5.5, §6). Ledger of the run: `docs/superpowers/reports/2026-10-23-storage-cost-calculator/LEDGER.md`. Review that shaped this revision: `tmp/codex_plan_r1_findings.md`.

## Global Constraints

- Binary `tmp/bin/26.8/clickhouse`, sha256 `2a61ec2a0c0c2350745cce30523df914d5f9ee9e97728b1170407502e4121f46`, commit `09d0772a842c5d73cf3ef35b4c8b017d838215a0`; both recorded in every `Measurement`.
- Store: RustFS binary on `127.0.0.1:19121`, bucket `yardstick-cost`, credentials `clickhouse`/`clickhouse`, path-style addressing. Proxy caller ports `r1=19131, r2=19132, r3=19133, rig=19134`. Server ports: `port_base + 10 × (i − 1)`: tcp `+0`, http `+1`, interserver `+2`; Keeper client `port_base + 8`, Keeper Raft `port_base + 9` (replica 1 only); `port_base` default `19700`. All server listeners bind `127.0.0.1`; relay `i` binds `127.0.1.<i>:<interserver port of i>`.
- Supported policies (exactly): `s3, s3plain, cas, local, s3_cached, s3plain_cached, cas_cached`. No `local_cached`.
- Maintenance controls never use `SYSTEM STOP MERGES` (it also blocks mutations and is lost on restart): cards that must not merge use the table setting `max_parts_to_merge_at_once = 1` (existing `NO_MERGES`); GC is controlled by `cas_gc_enabled` in the disk config; TTL only where a card declares it; lease traffic is unavoidable and is classified by key family, not stopped.
- Window semantics (§Task 3): a window is `[mark_start, mark_end)` over attempt **start** timestamps assigned by the proxy under one lock together with `seq`; attempts whose `t_start_us < mark_end ≤ t_end_us` are *crossing* and are listed in the record; a deterministic card with a crossing attempt by a workload caller is `FAILED` with reason `window_crossing`, never silently counted.
- No third-party Python packages, no npm packages. Tests: `python3 -m unittest discover -s utils/cas-yardstick/tests -t utils/cas-yardstick` (139 existing must pass) and `node --test utils/storage-cost-calculator/test/`. Live tests (need RustFS and the binary) are in files named `tests/live_*.py`, run by the task that needs them with `python3 -m unittest tests.live_<name>`, and skipped (`unittest.SkipTest`) when `YARDSTICK_LIVE=1` is not set.
- Commits: one per task, `git add` exact files, `git commit -s`, message ends with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. No push.
- Docs under `docs/` carry frontmatter and `{#anchors}`; comments say why, never history. `tmp/` is never committed; runs go to `tmp/cas-yardstick/cost-runs/<run-id>/`.
- Nothing in this plan states a ClickHouse code fact as true; where a probe needs one (object count of an insert, multipart threshold, CAS key families), the implementer reads `tmp/src-26.8`, writes the fact with `path:line` into the probe's prediction file, and the probe asserts it.

## Review Focus

1. A request the proxy cannot frame (chunked request body, `Expect: 100-continue`, `Transfer-Encoding` on a response it cannot re-frame) — answered `501` with a ledger row `op='Unsupported'`, and the runner fails the pass (Task 2 live test `test_chunked_request_is_rejected_and_recorded`; Task 8 `test_unsupported_row_fails_pass`).
2. A `CompleteMultipartUpload` that returns HTTP 200 with an `<Error>` body — upstream S3 outcome `error`, replay does not create the object (Task 3 `test_complete_with_embedded_error_does_not_create_object`).
3. A `DeleteObjects` for an absent key — S3 reports it `Deleted`; quiet mode omits successes; replay treats absence from a quiet response as success and only `<Error>` entries as failures (Task 3 `test_batch_delete_absent_key_is_deleted`, `test_batch_delete_quiet_mode`, `test_batch_delete_per_key_error_keeps_object`).
4. Replica 1 (Keeper) restarted mid-run — the cluster comes back with the same Keeper state and the other replicas reconnect (Task 6 live test `test_restart_replica1_keeps_keeper_state`).
5. A manual gp3 allocation that is provider-invalid (throughput above `0.25 × IOPS`) versus one that is valid but below workload demand — the first is rejected with the constraint named, the second is priced as entered with a note (Task 13 `test_manual_invalid_rejected_vs_below_demand_noted`).

---

### Task 0: Shared schemas (frozen before any other task)

**Files:**
- Create: `docs/superpowers/cost-model/schemas/README.md`, `docs/superpowers/cost-model/schemas/ledger-row.schema.json`, `measurement.schema.json`, `prediction.schema.json`, `coverage.schema.json`, `registry.schema.json`, `formulas.schema.json` (grammar v1), `report-directory.md`
- Create: `utils/storage-cost-calculator/tools/jsonschema_min.py` (in-house validator: `type`, `required`, `enum`, `properties`, `items`, `additionalProperties`, `const`, `oneOf` over those)
- Test: `utils/storage-cost-calculator/tools/test_schemas.py`

**Interfaces (the frozen shapes; later tasks reference these names verbatim):**

- **Ledger row** (JSONL, one per attempt or mark). Attempt: `{"kind":"attempt","seq":int,"caller":"r1|r2|r3|rig","t_start_us":int,"t_end_us":int|null,"method":str,"path":str,"query":{str:str},"req_headers":{str:str} (subset: content-length, content-md5, x-amz-*, if-*, range, expect, transfer-encoding),"resp_headers":{str:str} (subset: content-length, etag, x-amz-*, transfer-encoding),"bucket":str|null,"key":str|null,"op":str (Task 3 enum),"upload_id":str|null,"part_number":int|null,"copy_source":{"bucket","key","range"}|null,"req_payload_bytes":int,"req_wire_bytes":int,"resp_payload_bytes":int,"resp_wire_bytes":int,"payload_sha256":str|null,"downstream":{"outcome":"delivered|dropped|injected","status":int|null},"upstream":{"http_status":int|null,"s3_code":str|null,"outcome":"ok|error|unknown|not_sent"},"response_body":{"upload_id":str|null,"etag":str|null,"complete_error":str|null,"delete_results":[{"key","deleted":bool,"code":str|null}]|null,"list":{"key_count":int,"truncated":bool}|null},"injected_rule":str|null}`. Mark: `{"kind":"mark","seq":int,"label":str,"t_us":int}`. `outcome: unknown` = the upstream request was sent but no complete response was read.
- **Measurement** (JSONL, one per pass): `{"schema":"measurement/1","run_id","card","policy","replicas":int,"k":int,"scale":float,"effective_counts":{},"effective_settings":{},"binary_sha256","commit","config_hashes":[str],"pass_dir","status":"ok|FAILED|UNSUPPORTED","reason":str|null,"window":{"start_seq","end_seq","t_start_us","t_end_us","crossing":[seq]},"ledger":{caller:{op:{"attempts","delivered","dropped","injected","upstream_ok","upstream_error","upstream_unknown","req_payload_bytes","resp_payload_bytes","list_pages","list_keys","delete_calls","delete_keys"}}},"background":{same shape, attempts of workload callers outside the window},"rig":{same shape},"events":{replica:{event:delta}},"proc_io":{replica:{rchar,wchar,syscr,syscw,read_bytes,write_bytes}},"local":{replica:{"paths":[str],"peak_allocated_bytes":int,"samples":[[t_us,bytes]],"device":str,"fs":str,"attribution":"process_io|host_diskstats","limitations":[str]}},"interserver":{relay_of:{"to_target","from_target","connections","open_at_end"}},"inventory":{"initial":"empty_verified|declared","byte_hours":{category:float},"uncertainty_byte_hours":float,"categories_evidence":str},"observations":{} (card-typed, see Task 7),"assertions":{name:{"expected","actual","ok":bool}},"wall_s":float,"unit_count":int}`.
- **Prediction / fixture** (`predictions/<card>.<policy>.json`): `{"schema":"prediction/1","card","policy","replicas","role":"fit|holdout|validation|probe","inputs":{registry_id:value},"settings":{},"expected":{caller:{op:{"attempts":int|{"min","max"}}}},"expected_observations":{},"tolerance":{"abs":{},"rel":{}},"source_refs":[{"claim","path","line"}],"prediction_commit":str}`.
- **Coverage** (`coverage.json`): `{"schema":"coverage/1","version","cards":[{"name","family","variants":[...],"replicas","priority":"P1|P2","status":"planned|run|cut"}],"evidence":[{"coefficient","card","variant","case":str,"span":{param:[lo,hi]},"role":"fit|holdout|validation","prediction_commit":str|null,"measurement_ids":[str],"status":"planned|done|consumed|cut"}],"excluded":[str]}`.
- **Registry** (`inputs-registry.json`): `{"schema":"registry/1","version","quantities":[{"id","block","label","kind":"input|pinned|measured|derivation","scope":"cluster|replica|pool|volume","unit","value_type":"number|enum","enum":[...]|null,"default":number|str|null,"rule":node|null,"coef":str|null,"validity":{param_id:[lo,hi]}|null,"evidence":{"kind":"assumed|source|measured|provider","ref"},"hint":{"sql","mapping":{column:field}}|null,"status":"ok|unsupported"}],"precedence":[{"group":[ids],"default_locked":[ids],"rules":{derived_id:node}}]}` — a precedence group lists all members (e.g. `data_bytes, ingest_rate, retention_days`), which two are locked by default, and the rule for each member when it is the derived one.
- **Formulas grammar v1** (`formulas.schema.json`): node = `{"kind":K, "dim":str, ...}` with `K ∈ {input(id), coef(id), var(name), const(value), add(a,b), sub(a,b), mul(a,b), div(a,b,on_zero:"zero|unavailable"), min(args[]), max(args[]), ceil_to(x,step), floor_to(x,step), branch(cond{op,a,b},then,else), sum_over(set,var,body), allocate(total,shares[{cell,weight}]), price(unit,rule_id,quantity)}`; `var` is legal only inside `sum_over`; `allocate` is legal only as a cell's top node; value types: number or `unavailable`; a formulas file: `{"schema":"formulas/1","owner":report_name,"variant","cells":{operation:{"ledger_rows":[{"row_type":"request|storage|transfer|block_io|block_capacity","op_kind":str,"outcome":"delivered|all","category":"active|outdated|garbage|temporary|incomplete|detached|metadata"|null,"direction":"in|out"|null,"scope":"variant|replica|volume","expr":node}],"states":[{"id":"not_supported|backlog_grows","cond":node|true,"reason":str}],"notes":[node]}}}`. Coefficient ids are owned by exactly one report; a second definition is rejected by the generator.
- **Report directory convention** (`report-directory.md`): `docs/superpowers/reports/2026-10-<dd>-storage-cost-<name>/` containing `README.md` (the report), `measurements.jsonl`, `predictions/*.json`, `coefficients.json` (`[{id,value,dim,validity,evidence:{kind,ref,measurement_ids,binary_sha256,commit},status}]`), `formulas.json`, `experiments/*.json` (experiment manifests, Plan 2), `raw/` (ledger slices and relay logs for the measurements cited, with sha256 list `raw/SHA256SUMS`).

- [ ] **Step 1: Failing tests** — `test_each_schema_accepts_its_example_and_rejects_missing_required` (each schema ships an `examples/` instance), `test_formulas_grammar_rejects_var_outside_sum_over`, `test_formulas_grammar_rejects_allocate_not_at_top`, `test_registry_precedence_group_rules_cover_every_member`, `test_validator_handles_oneOf_and_enum`.
- [ ] **Step 2: Run** `python3 -m unittest utils.storage-cost-calculator.tools.test_schemas` → FAIL. **Step 3: Write the schemas, examples, README and validator.** **Step 4: PASS.**
- [ ] **Step 5: Commit** — `cost-model: shared schemas (ledger, measurement, prediction, coverage, registry, formulas v1)`.

### Task 1: RustFS launcher and authenticated rig client

**Files:**
- Create: `utils/cas-yardstick/rustfs-bin.sh`, `utils/cas-yardstick/yardstick/cost/__init__.py`, `utils/cas-yardstick/yardstick/cost/s3client.py`
- Modify: `utils/cas-yardstick/README.md` ("Cost rig" section)
- Test: `utils/cas-yardstick/tests/test_cost_s3client.py` (SigV4 vectors, offline), `utils/cas-yardstick/tests/live_rustfs_bin.py`

**Interfaces:**
- `rustfs-bin.sh start [--data DIR] [--bin PATH] | stop | status | ensure`: `start` refuses when `status` is `up` (ownership: whoever started it stops it; the CLI uses `ensure` = start only if down and remember in `<data>/.started-by-cli`); env as `rustfs.sh`; `RUSTFS_ADDRESS=127.0.0.1:19121`; pid file `<data>/.rustfs.pid`; ready when `GET /` answers 403 **and** `s3client.ensure_bucket` succeeds (the script calls `python3 -m yardstick.cost.s3client ensure-bucket`).
- `class S3Client(endpoint: str, access_key: str, secret_key: str, region='us-east-1', caller_port: int|None)`: SigV4 (`AWS4-HMAC-SHA256`, `x-amz-content-sha256` of the payload), path-style, methods `ensure_bucket(bucket)`, `put(bucket, key, data: bytes)`, `get(bucket, key) -> bytes`, `head(bucket, key) -> dict|None`, `delete(bucket, key)`, `delete_prefix(bucket, prefix) -> int` (paginated list + `DeleteObjects` batches of 1000, verbose mode), `list(bucket, prefix) -> list[{key,size,etag}]` (paginated `list-type=2`), `list_multipart_uploads(bucket, prefix) -> list[{key,upload_id}]`, `list_parts(bucket, key, upload_id) -> list[{part_number,size,etag}]`, `abort_multipart(bucket, key, upload_id)`. When `caller_port` is given, requests go to `http://127.0.0.1:<caller_port>` (through the proxy as caller `rig`) with `Host` header set to the proxy address so signatures verify at RustFS (the proxy preserves `Host`? — no: the proxy rewrites nothing; RustFS validates the signature against the `Host` it receives, so the client signs with the **proxy** host and the proxy forwards `Host` unchanged; Task 2 keeps `Host` verbatim).

- [ ] **Step 1: Offline tests** — `test_sigv4_canonical_request_and_signature_match_aws_example` (the AWS documentation's GET example vectors, copied with the doc URL in the test), `test_list_pagination_follows_continuation_token` (fake HTTP server), `test_delete_prefix_batches_of_1000_and_returns_count`.
- [ ] **Step 2: Run → FAIL. Step 3: Implement `s3client.py` and `rustfs-bin.sh`. Step 4: PASS.**
- [ ] **Step 5: Live tests** (`YARDSTICK_LIVE=1 python3 -m unittest tests.live_rustfs_bin`): `test_start_status_stop_cycle_and_pid_ownership` (subprocess), `test_start_refuses_when_up`, `test_ensure_bucket_then_put_get_head_delete_roundtrip`, `test_multipart_create_upload_list_abort_roundtrip` (client issues `CreateMultipartUpload` + 2 `UploadPart` of 5 MiB + `list_parts` shows 2 + abort → `list_multipart_uploads` empty).
- [ ] **Step 6: README "Cost rig"** section (ports table, bucket, the three commands). **Step 7: Commit** — `yardstick: RustFS launcher and SigV4 rig client`.

### Task 2: Ledger proxy

**Files:**
- Create: `utils/cas-yardstick/yardstick/cost/proxy.py`
- Test: `utils/cas-yardstick/tests/test_cost_proxy.py` (fake upstream), `utils/cas-yardstick/tests/live_proxy_rustfs.py`

**Interfaces:**
- `class LedgerProxy(upstream: tuple[str,int], ledger_path: str, callers: dict[str,int])` with `start()`, `stop()`, `mark(label) -> int` (allocates `seq` and `t_us` under the same lock attempts use; a request whose start is allocated after the mark belongs to the next window), `set_fault(rule) -> str`, `clear_faults()`, `flush()` (fsync the ledger). Fault rule: `{"caller":"r1|*","op":str|"*","key_regex":str,"upload_id":str|"*","part_number":int|null,"nth":int|"*","action":{"kind":"status","status":int,"send_upstream":bool}|{"kind":"drop_response","send_upstream":true}}`; with `send_upstream:false` the upstream outcome is `not_sent`.
- Framing rules: request line, `Host`, path and query forwarded byte-exact (signatures depend on them); request bodies with `Content-Length` streamed in 1 MiB chunks while hashing (`payload_sha256`) and counting; `Transfer-Encoding: chunked` requests and `Expect: 100-continue` are answered `501` without forwarding and recorded as `op='Unsupported'`; responses are forwarded with their status line and headers; a chunked upstream response is read fully (cap 256 MiB) and re-sent with `Content-Length`; `HEAD` responses carry no body; read timeout 300 s per direction, idle keep-alive 60 s; when a client disconnects mid-request the upstream connection is closed and `downstream.outcome='dropped'` with `upstream.outcome='unknown'` if the response was not fully read; injected-status requests with `send_upstream:false` drain the request body before answering. The proxy parses: `uploadId` from `CreateMultipartUpload` responses, `ETag` headers, `<Error><Code>` from any body (including 200 responses of `CompleteMultipartUpload`), `DeleteObjects` request keys and response `<Deleted>`/`<Error>` entries (quiet mode: absent = deleted), `ListObjectsV2` `KeyCount`/`IsTruncated`, `x-amz-copy-source` and `x-amz-copy-source-range`.
- Rows are written per the Task 0 ledger-row schema; `seq` is monotonic across callers.

- [ ] **Step 1: Offline tests (fake upstream, signed requests not needed)** — `test_forwards_put_preserving_request_line_host_and_query`, `test_rows_match_ledger_schema` (validator), `test_chunked_request_is_rejected_and_recorded`, `test_expect_header_is_rejected_and_recorded`, `test_chunked_upstream_response_is_reframed`, `test_head_has_no_body`, `test_fault_status_without_upstream_records_not_sent`, `test_fault_drop_response_records_upstream_ok_and_downstream_dropped`, `test_fault_targets_part_number_of_upload` (rule `op=UploadPart, part_number=2, nth=1` hits only that part), `test_client_disconnect_mid_body_marks_unknown`, `test_mark_and_seq_share_lock` (100 threads issuing requests around a mark: every attempt's `seq` is on the correct side of the mark's `seq` relative to its `t_start_us`), `test_payload_sha256_and_wire_bytes`, `test_complete_multipart_200_with_error_body_parsed`, `test_delete_objects_request_and_response_parsed_including_quiet`, `test_streaming_peak_memory` (sampler thread records `VmRSS` every 50 ms during a 64 MiB PUT; peak − baseline < 16 MiB), `test_keepalive_connection_survives_injected_error`, `test_concurrent_requests_all_recorded` (50 threads × 10 requests → 500 rows, distinct `seq`).
- [ ] **Step 2: Run → FAIL. Step 3: Implement. Step 4: PASS.**
- [ ] **Step 5: Live** (`live_proxy_rustfs.py`): through caller `rig` with `S3Client(caller_port=19134)`: signed PUT/GET/HEAD/LIST/DELETE round trips verify at RustFS (`upstream.http_status` 200/204); multipart create/upload/list/abort; a `drop_response` fault on `UploadPart` followed by the client's retry shows two rows with the same `(upload_id, part_number)`.
- [ ] **Step 6: Commit** — `yardstick: ledger proxy with framing rules and fault injection`.

### Task 3: Ledger parsing, windows, retry correlation and lifecycle replay

**Files:**
- Create: `utils/cas-yardstick/yardstick/cost/ledger.py`
- Test: `utils/cas-yardstick/tests/test_cost_ledger.py`

**Interfaces:**
- `OPS = ('PutObject','CopyObject','CreateMultipartUpload','UploadPart','UploadPartCopy','CompleteMultipartUpload','AbortMultipartUpload','GetObject','HeadObject','ListObjectsV2','ListMultipartUploads','ListParts','DeleteObject','DeleteObjects','CreateBucket','HeadBucket','Unsupported','Other')`; `parse_operation(method, path, query, headers) -> str` (rules: `?uploads` POST → Create, GET → ListMultipartUploads; `uploadId`+`partNumber` PUT → UploadPart (`x-amz-copy-source` → UploadPartCopy); `uploadId` POST → Complete, DELETE → Abort, GET → ListParts; `?delete` POST → DeleteObjects; `x-amz-copy-source` PUT → CopyObject; `list-type=2` GET on a bucket → ListObjectsV2; HEAD on bucket → HeadBucket, on key → HeadObject; PUT on bucket → CreateBucket).
- `read_rows(path) -> list[dict]`; `window(rows, start_label, end_label) -> Window{start_seq,end_seq,t_start_us,t_end_us, attempts: list, crossing: list[seq]}` (membership by `seq` between the marks; crossing = `t_end_us` is null or `> t_end_us` of the window).
- `summarize(attempts, callers: set[str]) -> dict[caller -> dict[op -> counts]]` with the Measurement `ledger` shape.
- `correlate_retries(attempts) -> list[list[seq]]`: same caller, op, bucket, key, upload_id, part_number, `payload_sha256`, and conditional headers (`if-match`, `if-none-match`, `x-amz-*` conditions) with an earlier attempt whose downstream outcome was `dropped`/`injected` or upstream `error`/`unknown`; otherwise uncorrelated.
- `replay(attempts, initial: Inventory, families: list[{name, regex, category}]) -> Inventory` — `Inventory{objects: {(bucket,key): {size, created_us, family, category}}, incomplete: {(bucket,key,upload_id,part_number): {size, t_us}}, transitions: [...]}`; rules: `PutObject`/`CopyObject` with `upstream.outcome == 'ok'` replace the identity (size from `req_payload_bytes` / copy size from `resp_headers` or source size); `UploadPart` ok → part identity replaced; `CompleteMultipartUpload` ok and `complete_error == null` → parts retired, object created with the sum of the parts listed in the request body (proxy records the listed part numbers in `response_body.completed_parts`) — add that field to the ledger-row schema in this task with a schema version bump to `ledger-row/1.1`; `AbortMultipartUpload` ok → parts retired; `DeleteObject` ok → removed (absent key is still ok); `DeleteObjects` → per `delete_results` (quiet: listed keys without an `<Error>` entry are deleted); upstream `error`/`not_sent` → unchanged; upstream `unknown` → object marked `uncertain` and counted in `uncertainty_byte_hours` for its whole lifetime.
- `byte_hours(inventory, t0_us, t1_us) -> {category: float, 'uncertain': float}` integrating each identity's residency clipped to the window; category from the family list (regex over key, `metadata`, `active|…` assigned by the family; lifecycle categories that need logical evidence — `outdated`, `garbage`, `detached` — are attached by the card through `Inventory.reclassify(identities, category, t_us)` with the evidence recorded in `observations`).
- `check_rows(attempts) -> list[str]`: `Unsupported`/`Other` ops, missing `t_end_us` inside a window, crossing attempts by workload callers.

- [ ] **Step 1: Failing tests** — `test_parse_operation_table` (every rule), `test_window_membership_and_crossing`, `test_summarize_shape_matches_measurement_schema`, `test_retry_correlation_requires_identity_payload_and_failed_predecessor` (cases: equal-length different payload → not correlated; different bucket → not; different part → not; different `if-none-match` → not; two concurrent identical writes with no failure → not; dropped then identical → correlated), `test_replay_put_replaces_same_identity_only`, `test_replay_multipart_two_parts_replace_one_complete_listed_only` (parts 1,2,3 uploaded, complete lists 1,2 → object = p1'+p2, part 3 remains incomplete), `test_complete_with_embedded_error_does_not_create_object`, `test_replay_abort_retires_incomplete`, `test_batch_delete_absent_key_is_deleted`, `test_batch_delete_quiet_mode`, `test_batch_delete_per_key_error_keeps_object`, `test_unknown_outcome_counts_as_uncertain`, `test_byte_hours_clip_to_window_and_short_lived_object_counts`, `test_families_classify_metadata_vs_payload`, `test_reclassify_outdated_then_garbage`.
- [ ] **Step 2: Run → FAIL. Step 3: Implement (`ledger.py`; schema bump + proxy field). Step 4: PASS (proxy tests too).**
- [ ] **Step 5: Commit** — `yardstick: ledger windows, retry correlation and lifecycle replay`.

### Task 4: Interserver byte relay with window snapshots

**Files:**
- Create: `utils/cas-yardstick/yardstick/cost/interserver_proxy.py`
- Test: `utils/cas-yardstick/tests/test_cost_interserver_proxy.py`

**Interfaces:**
- `class ByteRelay(listen: (host,port), target: (host,port), log_path)` with `start()`, `stop()`, `snapshot(label) -> {to_target, from_target, connections_total, open: int}` (cumulative bytes including open connections at the instant of the call, written to the log as a `snapshot` row), per-connection rows `{t_open_us, t_close_us|null, peer, to_target, from_target}` on close; `delta(a, b)`.
- Attribution: relay `i` counts bytes **sent by replica i** (sender exact). Receiver attribution is by construction: cards with two replicas have one receiver; the three-replica `replacement` card keeps r1 and r2 idle (asserted via their `ReplicatedPartFetches` delta = 0) so r3 is the receiver.

- [ ] **Step 1: Failing tests** — `test_snapshot_includes_open_connection_bytes` (connection stays open across two snapshots; the delta equals the bytes sent between them), `test_connection_row_written_on_close`, `test_relay_survives_target_refusal`, `test_bind_alias_and_real_listener_pair` (bind `127.0.1.5:P` relay and `127.0.0.1:P` target at the same port).
- [ ] **Step 2–4: Run/implement/pass.** **Step 5: Commit** — `yardstick: interserver byte relay with snapshots`.

### Task 5: Topology and multi-replica configuration

**Files:**
- Modify: `utils/cas-yardstick/yardstick/config.py` (add `write_cost_config`; `write_config` unchanged)
- Create: `utils/cas-yardstick/yardstick/cost/topology.py`
- Test: `utils/cas-yardstick/tests/test_cost_config.py`

**Interfaces:**
- `@dataclass Topology(port_base=19700, replicas=1, callers={r1:19131,r2:19132,r3:19133,rig:19134}, store=('127.0.0.1',19121), bucket='yardstick-cost')` with `tcp(i)`, `http(i)`, `interserver(i)`, `keeper_client()`, `keeper_raft()`, `relay_host(i)`, `caller_port(i)`, `endpoint(i, key_prefix) -> f'http://127.0.0.1:{caller_port(i)}/{bucket}/{key_prefix}/'`, `preflight() -> list[str]` (every port free on its host; every `127.0.1.i` bindable).
- `write_cost_config(replica_dir: str, replica: int, policy: str, topology, key_prefix: str, local_dir: str|None, gc_enabled: bool, cas_server_root_id: str) -> str` (hash). Key layout: non-shared disks `{key_prefix}/r{replica}/`, CAS `{key_prefix}/shared/`; `local` disk path `{local_dir}/` (the runner passes a per-pass, per-replica directory); `<listen_host>127.0.0.1</listen_host>`; `<interserver_http_host>127.0.1.{replica}</interserver_http_host>`; `<zookeeper><node><host>127.0.0.1</host><port>{keeper_client}</port></node><session_timeout_ms>10000</session_timeout_ms></zookeeper>`; `<macros><shard>1</shard><replica>r{replica}</replica></macros>`; replica 1: `<keeper_server><tcp_port>{keeper_client}</tcp_port><server_id>1</server_id><log_storage_path>{replica_dir}/keeper/log</log_storage_path><snapshot_storage_path>{replica_dir}/keeper/snapshots</snapshot_storage_path><raft_configuration><server><id>1</id><hostname>127.0.0.1</hostname><port>{keeper_raft}</port></server></raft_configuration></keeper_server>`.

- [ ] **Step 1: Failing tests** — `test_ports_distinct_for_three_replicas_including_keeper`, `test_endpoint_uses_callers_port_and_bucket_and_prefix`, `test_key_layout_per_replica_vs_shared_cas`, `test_replica1_keeper_server_block_and_all_zookeeper_macros_listen_host`, `test_local_policy_path_is_the_given_dir`, `test_unknown_policy_rejected` (`local_cached`), `test_existing_write_config_hash_unchanged` (pin the current hash), `test_preflight_reports_busy_port` (bind a socket first).
- [ ] **Step 2–4: Run/implement/pass (all tests).** **Step 5: Commit** — `yardstick: cost topology and multi-replica config`.

### Task 6: Cluster control

**Files:**
- Create: `utils/cas-yardstick/yardstick/cost/cluster.py`
- Test: `utils/cas-yardstick/tests/test_cost_cluster.py` (fakes), `utils/cas-yardstick/tests/live_cluster.py`

**Interfaces:**
- `class Cluster(binary, pass_dir, topology, policy, key_prefix, local_dirs: list[str]|None, gc_enabled, server_sh=…)`: `start_all()` (preflight → replica 1 → readiness = `SELECT count() FROM system.zookeeper WHERE path = '/'` succeeds on r1 → others → `SELECT 1` each → relays up; on any failure stop what started and raise with the server log path), `stop(i)`, `start(i) -> float` (seconds launch→ready), `stop_all()`, `clients: list[Client]`, `pids`, `relays`, `startup_seconds`. Settings are in the generated XML; `server.sh` is used unchanged.

- [ ] **Step 1: Fake tests** — `test_start_all_order_and_readiness_queries`, `test_partial_start_failure_stops_started_servers_and_relays`, `test_preflight_failure_before_launch`.
- [ ] **Step 2–4: Run/implement/pass.**
- [ ] **Step 5: Live** (`live_cluster.py`, policy `s3`, 2 replicas): `test_two_replicas_replicate_an_insert_through_the_relay` (create `ReplicatedMergeTree` on both, insert 1 row on r1, `SYSTEM SYNC REPLICA` on r2 → count 1; relay 1 `from_target > 0`), `test_restart_replica1_keeps_keeper_state` (stop r1, start r1 → table still listed in `system.zookeeper` and r2 reconnects: a second insert replicates).
- [ ] **Step 6: Commit** — `yardstick: multi-replica cluster control with embedded Keeper`.

### Task 7: Local I/O accounting and the `CostCard` interface

**Files:**
- Create: `utils/cas-yardstick/yardstick/cost/localio.py`, `utils/cas-yardstick/yardstick/cost/cards/__init__.py`, `utils/cas-yardstick/yardstick/cost/cards/base.py`
- Test: `utils/cas-yardstick/tests/test_cost_localio.py`, `utils/cas-yardstick/tests/test_cost_cards_base.py`

**Interfaces:**
- `proc_io(pid) -> dict`, `delta(a,b)`; `class DirSampler(paths, interval_s=0.5)` with `start()`, `stop() -> {path: {peak_allocated_bytes, samples: [[t_us, bytes]]}}` counting **allocated** bytes (`st_blocks × 512`) and deduplicating hardlinks by `(st_dev, st_ino)`; `device_of(path) -> {device, fs, mountpoint}`; `diskstats(device)`; `limitations(pass_dir, local_dirs) -> list[str]` (adds `"process_io includes embedded Keeper on replica 1"`, `"host diskstats include RustFS, proxy and unrelated activity"`, `"page cache not dropped (no root)"` when `/proc/sys/vm/drop_caches` is not writable).
- `class CostCard`: `name`, `policies: tuple`, `replicas: int`, `priority`, `COUNTS`, `counts(scale)`, `tables(policy) -> list[str]`, `controls -> {'merges': 'stop|measure', 'gc': 'off|measure', 'ttl': 'none|measure'}`, `settings(policy) -> dict` (table and query settings applied, recorded as `effective_settings`), `supports(policy) -> (bool, reason)`, `setup(cluster, policy, counts)`, `measured(cluster, policy, counts) -> {'unit_count': int, 'ops': [...], 'observations': dict}` (typed per card: e.g. `part_log_merges: [...]`, `branch_evidence: {...}`, `startup_seconds`, `gc_rounds: [...]`), `complete(cluster, timeout_s=300)` (default: for each table on each replica `SYSTEM SYNC REPLICA <t> STRICT`; then poll until `system.replication_queue`, `system.merges`, `system.mutations WHERE is_done=0` are empty for the card's tables; `SYSTEM FLUSH LOGS`; cards override when the table no longer exists), `expected(counts) -> dict` (row counts per table), `assertions(cluster, measurement_draft) -> {name: (expected, actual)}` (default: `rows` per table equal `expected` on every replica, and nonzero where `expected` is nonzero), `inventory_families(policy) -> list[{name, regex, category}]`, `teardown(cluster)`. `replicated_engine(policy, zk_path, extra_settings)`.
- Precedence: `card.replicas` is the card's requirement; the CLI's `--replicas` must be ≥ it or the card is `UNSUPPORTED (needs N replicas)`.

- [ ] **Step 1: Failing tests** — `test_proc_io_write_bytes_grows_after_fsync`, `test_dir_sampler_counts_allocated_bytes_and_dedups_hardlinks` (file + hardlink → counted once), `test_dir_sampler_misses_subinterval_peak_documented` (file lives 50 ms with interval 500 ms → peak may be 0; the test asserts the limitation string is present), `test_limitations_list_keeper_and_pagecache`, `test_default_complete_queries_and_polling` (FakeCluster), `test_default_assertions_require_nonzero_rows`, `test_replicas_precedence_marks_unsupported`.
- [ ] **Step 2–4: Run/implement/pass.** **Step 5: Commit** — `yardstick: local I/O accounting and CostCard interface`.

### Task 8: Runner and measurement records

**Files:**
- Create: `utils/cas-yardstick/yardstick/cost/measurements.py`, `utils/cas-yardstick/yardstick/cost/runner.py`
- Test: `utils/cas-yardstick/tests/test_cost_runner.py`

**Interfaces:**
- `measurements.write(path, record: dict)` (validates against `measurement.schema.json` first), `read(path)`.
- `@dataclass Deps(binary, run_id, runs_dir, topology, proxy: LedgerProxy, rig: S3Client, local_root: str, scale: float)`.
- `run_card(card, policy, k, deps) -> dict`: `pass_dir = <runs_dir>/<run_id>/<policy>/<card>/k<k>/`, `key_prefix = <run_id>/<policy>/<card>/k<k>`, local dirs `pass_dir/local/r<i>/`; verify the key prefix is empty via `rig.list` (inventory `initial: empty_verified`); cluster start; `setup`; controls applied (`gc_enabled = controls['gc']=='measure'`); `mark('start')`, snapshots (events, proc_io, relays, DirSampler start); `measured`; `complete`; snapshots; `mark('end')`; `proxy.flush()`; ledger window, summaries, crossing check (deterministic cards: `FAILED/window_crossing`); replay over the pass with the card's families; `assertions`; teardown; `rig.delete_prefix` unless `--keep-prefix`; `stop_all`; write the record. `supports` false → `UNSUPPORTED` record with reason. Any exception → `FAILED` with `reason` and the server log paths; numbers omitted.

- [ ] **Step 1: Fake-based tests** — `test_record_validates_against_schema`, `test_rig_rows_excluded_from_ledger_but_replayed`, `test_window_crossing_fails_deterministic_card`, `test_unsupported_row_fails_pass`, `test_server_exception_marks_failed_without_numbers`, `test_unsupported_card_record`, `test_prefix_verified_empty_before_setup`.
- [ ] **Step 2–4: Run/implement/pass.** **Step 5: Commit** — `yardstick: cost runner and measurement records`.

### Task 9: `cost` CLI and runtime estimate

**Files:**
- Modify: `utils/cas-yardstick/yardstick.py`; Create: `utils/cas-yardstick/yardstick/cost/cli.py`, `utils/cas-yardstick/yardstick/cost/estimate.py`
- Test: `utils/cas-yardstick/tests/test_cost_cli.py`

**Interfaces:**
- `yardstick.py cost run --binary P --cards a,b --policies s3,cas --replicas N --repeats K --scale S --runs-dir D [--endpoint URL] [--keep-prefix]`: `rustfs-bin.sh ensure` unless `--endpoint`; starts the proxy in-process; runs; prints the table; exit 1 on FAILED, 0 otherwise (UNSUPPORTED is not a failure).
- `yardstick.py cost probe --binary P --runs-dir D`: runs the five probes (Task 14) through `run_card` and then `node utils/storage-cost-calculator/tools/drill.mjs` per probe (Task 13); prints per probe PASS/FAIL with the gate quantities; exit 2 on any FAIL.
- `yardstick.py cost estimate <run-dir> --coverage docs/superpowers/cost-model/coverage.json`: per card × variant: `passes = replicas_factor × K`, `pass_seconds = measured full pass (start→record) or the family's sampled pass`, plus completion timeout share and cleanup, × 1.25 rerun reserve; prints P1 hours, P1+P2 hours, and the cut order to reach 72 h (P2 by family, then `replication-zero-copy`, then `types-*`), with the registry quantities and presets each cut would disable (read from `coverage.evidence`).

- [ ] **Step 1: Tests** — `test_run_dispatches_and_exit_codes` (fakes for runner), `test_probe_runs_five_and_exit_2_on_fail`, `test_estimate_math_and_cut_order` (fixture measurements and coverage → expected hours and cut list).
- [ ] **Step 2–4: Run/implement/pass.** **Step 5: Commit** — `yardstick: cost CLI and runtime estimate`.

### Task 10: Artifact seeds and closure checks

**Files:**
- Create: `docs/superpowers/cost-model/measurement-contract.md`, `inputs-registry.json`, `coverage.json`, `billing-contract.json`, `utils/storage-cost-calculator/tools/closure_check.py`
- Test: `utils/storage-cost-calculator/tools/test_closure_check.py`

**Interfaces:**
- Seeds: registry with the simple inputs and the `data/ingest/retention` precedence group; coverage with the §5.6 families as `cards` and empty `evidence`; billing contract `{"schema":"billing/1","presets":[{"id":"aws-us-east-1","as_of","currency":"USD","units":"decimal","assumptions":[…§4.6…],"request_classes":[{"id","ops":[…],"price_per_1000","source_url","checked"}],"outcome_billing":{"source_url","checked","free":[{"status","s3_code"|null,"condition"}]},"storage":{"tiers":[{"up_to_gb","price_per_gb_month"}],"incomplete_multipart":"as_storage","source_url","checked"},"transfer":[{"id","price_per_gb","direction","source_url","checked"}],"block":{"gp3":{"price_per_gib_month","iops_baseline":3000,"iops_price","mibps_baseline":125,"mibps_price","iops_per_gib_max","mibps_per_iops_max","size_gib":[min,max],"iops":[min,max],"mibps":[min,max],"source_url","checked"}},"allocation":{"layouts":[{"id":"single","volumes":1},{"id":"stripe2","volumes":2},{"id":"stripe4","volumes":4}],"objective":"min_monthly_cost","tie_break":["fewest_volumes","smallest_total_iops"]}}]}` — values looked up on the AWS pages on the day with URLs and date (the `as_of` is that date).
- `closure_check.py` with five functions returning problem lists: `dependency(registry, formulas)` (ids unique; every `input/coef/var` reference resolves; `var` bound; scopes compatible per node (a `replica`-scoped quantity multiplied into a `cluster` cell needs an explicit `mul` by `replicas`, flagged otherwise); derivations acyclic; defaults within validity); `support(registry, coverage, formulas)` (every variant × operation in formulas has evidence or a `not_supported` state; cut cards → dependent coefficients `unsupported` and no non-zero default references them; presets (Plan 3) within validity); `evidence(coverage, coefficients)` (every P1 measured coefficient has a `fit` and a distinct `holdout` edge with `status: done`, provenance complete, pinned constants have `source` refs with `path:line`); `accounting(measurements, predictions)` (deterministic predictions match exactly; byte-hours reconcile with replay within `uncertainty_byte_hours`); `reproduction(contract, page, doc, raw_sha)` (generated copies identical; `raw/SHA256SUMS` verify). `main()` runs all available checks for the artifacts present and exits 1 on problems.
- `measurement-contract.md`: documents Tasks 2–8 as built (ledger fields, framing, windows, retry rule, replay rules, categories and how logical categories are attached, relay attribution, local I/O attribution and limitations, what the workstation cannot establish), plus a `Review` section.

- [ ] **Step 1: Tests** — `test_dependency_detects_unbound_var_and_scope_mismatch`, `test_support_fails_cut_card_with_nonzero_default`, `test_evidence_requires_distinct_holdout`, `test_accounting_exact_match_and_byte_hours_tolerance`, `test_reproduction_detects_stale_copy`, `test_seeds_pass_dependency_and_billing_shape`.
- [ ] **Step 2–4: Run/implement/pass.** **Step 5: Commit** — `cost-model: artifact seeds and closure checks`.

### Task 11: Typed expression evaluator (classic script + Node shim)

**Files:**
- Create: `utils/storage-cost-calculator/model.js`, `utils/storage-cost-calculator/test/model.test.js`

**Interfaces:**
- Distribution: classic scripts; each file ends with `if (typeof module !== 'undefined') module.exports = SCC.model;` and attaches to `window.SCC = window.SCC || {}` in browsers; tests `require()` them. `index.html` loads them with `<script src>` and works from `file://`.
- `SCC.model.evaluate(node, ctx) -> {value: number|null, state: 'ok'|'unavailable', extrapolated: boolean, trace}`; `ctx = {inputs: Map, coefs: Map<id,{value,validity}>, sets: Map, vars: Map, prices: PricePreset}`; `allocate` returns `{value: null, cells: {cell: number}, …}` and is only legal at a cell's top; `evaluateCell(cellNode, ctx) -> {cells|value, …}`; `checkDims(node, registry) -> string[]`; `render(node, registry) -> string` (symbolic).
- Grammar: Task 0's `formulas/1`.

- [ ] **Step 1: Tests** — `test_const_input_coef`, `test_dims_combine_and_mismatch_detected`, `test_div_on_zero_policies`, `test_branch_records_branch_taken`, `test_sum_over_binds_var_and_var_outside_fails`, `test_allocate_only_at_top_and_shares_sum`, `test_coef_outside_validity_sets_extrapolated_on_cell`, `test_unavailable_propagates`, `test_render_symbolic_matches_fixture`, `test_fractional_monthly_quantities_not_rounded_without_node`.
- [ ] **Step 2–4: Run/implement/pass (`node --test`).** **Step 5: Commit** — `calculator: typed expression evaluator`.

### Task 12: Contract loader (minimal) and formulas merge

**Files:**
- Create: `utils/storage-cost-calculator/contract.js`, `utils/storage-cost-calculator/test/contract.test.js`

**Interfaces:**
- `SCC.contract.load(json) -> Contract` validating `{schema:"contract/1", version, manifest:{evaluator_version, contract_version, registry_version, coverage_version, billing_version, evidence:[{report, commit, raw_sha256sums}]}, evidence_statement, registry, precedence, coefficients, billing (the billing-contract preset list verbatim), variants, operations, applicability, formulas (merged `cells` per variant), states}`; runs `checkDims`; rejects duplicate coefficient ids across owners and formulas referencing unregistered ids.
- `SCC.contract.mergeFormulas(files: FormulasFile[]) -> {formulas, problems}` (one owner per variant × operation; duplicates are problems).

- [ ] **Step 1: Tests** — `test_load_valid_minimal_contract`, `test_duplicate_coefficient_owner_rejected`, `test_formula_unregistered_input_rejected`, `test_merge_rejects_two_owners_of_same_cell`.
- [ ] **Step 2–4.** **Step 5: Commit** — `calculator: contract loader and formulas merge`.

### Task 13: Billing, provisioning and the drill-down printer

**Files:**
- Create: `utils/storage-cost-calculator/billing.js`, `utils/storage-cost-calculator/tools/drill.mjs`, `utils/storage-cost-calculator/test/billing.test.js`, `utils/storage-cost-calculator/test/fixtures/pricing/*.json`

**Interfaces:**
- `SCC.billing.bill(rows: LedgerRow[], preset, scope='variant') -> {lines:[{rule_id, unit, quantity, money, tiers}], total}` where `LedgerRow = {row_type, op_kind, outcome, category, direction, scope, quantity, dim, cell}` (Task 0 formulas row + evaluated quantity): request classes per 1000 by `op_kind`; outcome billing: rows with `outcome:'all'` are split by the measured outcome mix when provided, else billed; `free` list from the preset; storage tiers applied once over the variant's `storage` rows; `incomplete` category billed as storage; transfer by direction; block capacity/IOPS/MiB/s via `allocateVolumes`; `units:'decimal'` converts GiB→GB.
- `SCC.billing.allocateVolumes(demand: {capacity_gib, peak_iops, peak_mibps}, preset, manual: null | {volumes, size_gib, iops, mibps}) -> {layout, volumes:[{size_gib, iops, mibps}], monthly_cost, feasible: boolean, manual: boolean, notes:[str], alternatives:[{layout, monthly_cost}]}`: candidates = `preset.allocation.layouts`; for each layout, per-volume size = `ceil(capacity / volumes)` bounded by `size_gib` limits, IOPS = `max(baseline, ceil(peak_iops / volumes), ceil(peak_mibps / volumes / mibps_per_iops_max))`, MiB/s = `max(baseline, ceil(peak_mibps / volumes))`, reject layouts violating any bound; price; choose minimum with tie-break; zero demand → smallest layout at baseline; manual: provider-invalid → `feasible:false` with the constraint named and no price; valid but below demand → priced as entered, note `below demand`.
- `tools/drill.mjs <measurement.jsonl> <predictions.json> <formulas.json> <billing-contract.json> <registry.json> --cell <variant>/<operation>`: builds `ctx` from the prediction's inputs and the measurement's observations, evaluates the cell, bills, allocates, prints the five stages (inputs used; branch taken per row; ledger rows predicted vs measured with exact-match flags; billing lines; allocation) as text and JSON; exit 1 when a deterministic row mismatches.

- [ ] **Step 1: Tests** — `test_request_classes_per_1000`, `test_unknown_outcome_billed_listed_free_not`, `test_storage_tiers_once_at_scope`, `test_incomplete_billed_as_storage`, `test_decimal_conversion`, `test_allocate_min_cost_with_tiebreak_and_alternatives` (fixture where `stripe2` beats `single` because of the IOPS cap), `test_allocate_mibps_iops_coupling`, `test_manual_invalid_rejected_vs_below_demand_noted`, `test_zero_demand_baseline_single`, `test_pricing_fixtures_exact` (each fixture carries its hand derivation), `test_drill_prints_five_stages_and_exit_code` (fixture measurement + formulas).
- [ ] **Step 2–4.** **Step 5: Commit** — `calculator: billing, cost-minimising provisioning and drill-down printer`.

### Task 14: Acceptance probes (phase-0 gate)

**Files:**
- Create: `utils/cas-yardstick/yardstick/cost/cards/probes.py`, `docs/superpowers/reports/2026-10-24-storage-cost-phase0/` (report directory per convention: `README.md`, `predictions/*.json`, `formulas.json` for the probe cells, `experiments/*.json`), register probes in `cards/__init__.py`
- Test: `utils/cas-yardstick/tests/test_cost_probes.py` (assertion logic on fixtures), `utils/cas-yardstick/tests/live_probes.py` (the gate)

**Interfaces (five probes; each has an experiment manifest with DDL, seeded generator SQL, settings, operation sequence, completion, assertions, and a prediction file whose `source_refs` cite `tmp/src-26.8` `path:line`):**
- `probe_s3_insert` (`s3`, 1 replica, controls merges=stop, gc=off): one table `k UInt64, a UInt64, b String` `ReplicatedMergeTree`, one `INSERT … SELECT number, number*7, hex(sipHash64(number)) FROM numbers(1000)`; prediction: exact per-op attempts for `r1` (the implementer derives objects per part and transaction checks from source); assertion: exact equality per op, zero crossing, `rows == 1000`.
- `probe_replicated_fetch` (`s3`, 2 replicas): same insert on r1; `complete`; assertions: `r2` has `PutObject ≥ 1`; relay 1 `from_target > 0`; relay 2 `from_target == 0`; `ReplicatedPartFetches` delta on r2 = 1 and on r1 = 0; rows equal and nonzero.
- `probe_multipart_fault` (`s3`, 1 replica): one `String` column; generator `repeat(hex(sipHash64(number)), 64)` (1 KiB per row) × `N` rows where `N` is set in the manifest from the pinned multipart threshold so the column object exceeds it (asserted from the ledger: `CreateMultipartUpload == 1`, `UploadPart ≥ 2`); faults (installed before the insert): `{op:'UploadPart', part_number:2, nth:1, action:drop_response}`, `{op:'CompleteMultipartUpload', nth:'*', action:{status:500, send_upstream:false}}`, `{op:'AbortMultipartUpload', nth:'*', action:{status:500, send_upstream:false}}`; the insert fails (asserted); assertions on the replay **before cleanup**: incomplete inventory nonzero, ≥ 2 distinct part identities, part 2 has two attempts with identical `payload_sha256` (replacement, correlated by `correlate_retries`), no object created; then the rig lists multipart uploads and aborts through caller `rig`; assertions after: incomplete bytes 0, byte-hours of `incomplete` > 0 within the window; `drill.mjs` shows the incomplete storage line billed.
- `probe_cas_gc_cohort` (`cas`, 1 replica, controls gc=measure with `cas_gc_interval_sec` set to 5 in the manifest): insert 20 parts into one table; after `complete`, freeze `cohort = keys PUT by r1 in the window whose key matches the payload/manifest families` (families from the prediction file's key-layout citation; control families excluded); `DROP TABLE … SYNC`; completion override: poll `system.tables` absence and the ledger until every cohort key has a `DeleteObject(s)` row with `upstream.outcome == 'ok'` by caller `r1`, or 300 s; assertions: cohort nonempty, all reclaimed with GC-log evidence (`system.cas_gc_log` rows in the window), `reclaim_delay_s` recorded from the DROP to the last cohort deletion; replay reclassifies the cohort as `garbage` at DROP time and the record's byte-hours show `garbage > 0`.
- `probe_local_volume` (`local`, 1 replica): 200 inserts × 20 000 rows into a 10-column table; `DirSampler` on the local dir; assertions: `proc_io.write_bytes > 0`, `peak_allocated_bytes > 0`, `device`/`fs` recorded, `limitations` nonempty, `strace -c`-free request-size evidence: the probe runs under `strace -f -e trace=pwrite64,pread64 -o <pass_dir>/strace.txt` when `strace` is available and records a size histogram in `observations.io_sizes`; `drill.mjs` prices the capacity peak and the IOPS demand through `allocateVolumes` and prints the allocation.

- [ ] **Step 1: Fixture tests** for each probe's assertions (PASS and FAIL directions), `test_cohort_excludes_control_families`.
- [ ] **Step 2–4: Run/implement/pass.** Prediction files committed **before** the live run (commit hash recorded in each prediction's `prediction_commit`).
- [ ] **Step 5: Live gate** — `YARDSTICK_LIVE=1 python3 utils/cas-yardstick/yardstick.py cost probe --binary tmp/bin/26.8/clickhouse --runs-dir tmp/cas-yardstick/cost-runs > tmp/cost_probe.log 2>&1; echo $?` → exit 0, five PASS lines each followed by the drill-down text. A FAIL is investigated with the systematic-debugging skill: prediction, instrumentation, setup and attribution are checked independently; the original prediction stays in the file with the demonstrated cause appended.
- [ ] **Step 6: Runtime estimate** — run `small-inserts`-equivalent and `probe_cas_gc_cohort` at the planned scale as samples (they are the largest families), then `yardstick.py cost estimate` → record hours, cut order and affected quantities in LEDGER.md.
- [ ] **Step 7: Report** `docs/superpowers/reports/2026-10-24-storage-cost-phase0/README.md` with the five results, prediction vs ledger tables, limitations, `raw/` slices and `SHA256SUMS`.
- [ ] **Step 8: Commit** — `yardstick: acceptance probes and phase-0 gate record`.

### Task 15: Phase-0 review

- [ ] **Step 1: Codex review** (prompt via file; scope: `measurement-contract.md`, schemas, `yardstick/cost/*.py`, `model.js`, `billing.js`, `drill.mjs`, the phase-0 report; questions: can the ledger reproduce request counts and byte-hours; is caller/observer/background separation sound; can a displayed number come from outside the contract; what leaks) → `tmp/codex_phase0_review_r1.log`.
- [ ] **Step 2: Close MAJORs** test-first; dispositions in `measurement-contract.md` §Review.
- [ ] **Step 3: LEDGER.md** — phase 0 done with the gate numbers and the cut decision. **Step 4: Commit** — `cost-model: phase-0 review dispositions`.
