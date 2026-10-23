# Storage Cost Calculator — Plan 1: Phase 0 measurement rig and acceptance slice

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `utils/cas-yardstick` into the measurement rig the design's phase 0 needs — a store-side request ledger proxy with fault injection, a multi-replica cluster with interserver byte capture, a `local` policy with per-process I/O accounting, lifecycle replay to byte-hours, `measurements.json` records — and prove it with the five acceptance probes.

**Architecture:** New package `utils/cas-yardstick/yardstick/cost/` beside the existing yardstick code (the existing `run` command and cards stay untouched). A Python stdlib HTTP reverse proxy in front of RustFS writes one JSONL row per attempt; the listening port identifies the caller (replica 1–3 or the rig). A TCP relay per replica, bound to a distinct loopback address that the replica advertises as `interserver_http_host`, counts replication bytes per direction. A `Cluster` starts 1–3 server processes with embedded Keeper in the first. A `CostCard` declares its completion condition, maintenance and assertions; the runner writes a `Measurement` record per pass. Seeds of the research artifacts (registry, coverage, billing, measurement contract) and the closure-check script skeleton are created here so the acceptance slice runs end to end.

**Tech Stack:** Python 3.10+ stdlib only (`http.server`, `http.client`, `socket`, `threading`, `json`, `unittest`); bash; the pinned `clickhouse` binary at `tmp/bin/26.8/clickhouse`; RustFS 1.0.0-rc.3 (`ci/tmp/rustfs` binary, launched by a new `rustfs-bin.sh`, docker image optional).

**Spec:** `docs/superpowers/specs/2026-10-23-storage-cost-calculator-design.md` (§3 constraints, §4.3 invariants, §5.1 measurement contract, §5.3 phase 0 gate, §5.5 budget). Ledger of the unattended run: `docs/superpowers/reports/2026-10-23-storage-cost-calculator/LEDGER.md`.

## Global Constraints

- Binary: `tmp/bin/26.8/clickhouse`, sha256 `2a61ec2a0c0c2350745cce30523df914d5f9ee9e97728b1170407502e4121f46`; every measurement record carries it and the commit `09d0772a842c5d73cf3ef35b4c8b017d838215a0`.
- Store: RustFS 1.0.0-rc.3 on `127.0.0.1:19121`; the proxy listens on `19131` (replica 1), `19132` (replica 2), `19133` (replica 3), `19134` (rig), control on `19139`. Server ports: `port_base + 10 × (replica − 1)` with the yardstick layout (tcp, http = +1, interserver = +2) plus Keeper on `port_base + 8` (replica 1 only); default `port_base = 19700`. Interserver relays listen on `127.0.1.<replica>:<interserver port>`.
- No third-party Python packages. Tests are `unittest`, run with `python3 -m unittest discover -s utils/cas-yardstick/tests -t utils/cas-yardstick`; the existing 139 tests must keep passing.
- Caller identity, foreground/background and observer separation per spec §4.3: rig traffic uses port 19134 and is excluded from workload counts but kept in lifecycle replay.
- `Expect: 100-continue` is off by default at the pinned commit (`DEFAULT_EXPECT_CONTINUE_MIN_BYTES = 0`); the proxy records the `Expect` header if present and the ledger check fails when it appears, so an unhandled negotiation is never silently miscounted.
- Commits: one per task, `git add` exact files, `git commit -s`, message ends with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. No push.
- Docs under `docs/` carry frontmatter and `{#anchors}` on every heading; code comments say why, never history.
- `tmp/` is not committed; measurement runs go to `tmp/cas-yardstick/cost-runs/<run-id>/`.

## Review Focus

1. A client that sends `Transfer-Encoding: chunked` or `Expect: 100-continue` to the proxy — the proxy must forward it unchanged or fail loudly (test in Task 2: a request with `Expect` is recorded with `expect: true` and `check_ledger` raises).
2. Two attempts for the same key within milliseconds (SDK retry after a 5xx) — retry correlation must not merge distinct writes (Task 3 test `test_retry_correlation_requires_same_key_and_payload_hash`).
3. A `DeleteObjects` body with a key that does not exist — per-key outcome `NoSuchKey` vs `Deleted`; replay must not delete an object that was not deleted (Task 3 test `test_replay_batch_delete_partial_failure`).
4. A server that exits during a card (crash) — the runner must mark the pass FAILED with the server log path, never write a partial `Measurement` as valid (Task 8 test `test_run_card_server_exit_marks_failed`).
5. Port already in use by a foreign server (one listens on 19000–19003 on this host) — `Cluster.start` must refuse with the port number (Task 6 test `test_cluster_refuses_busy_port`).

---

### Task 1: RustFS from the binary and the proxy's place in the topology

**Files:**
- Create: `utils/cas-yardstick/rustfs-bin.sh`
- Modify: `utils/cas-yardstick/README.md` (new section "Cost rig")
- Test: `utils/cas-yardstick/tests/test_cost_rustfs_bin.py`

**Interfaces:**
- Produces: `rustfs-bin.sh start [--data DIR] [--bin PATH] | stop | status`; data dir default `<repo>/tmp/cas-yardstick/rustfs-bin-data`, port `19121`, credentials `clickhouse`/`clickhouse`, same env as `rustfs.sh` (`RUSTFS_SCANNER_ENABLED=false`, `RUSTFS_HEAL_ENABLED=false`, `RUSTFS_DURABILITY_MODE=relaxed`), pid file `<data>/.rustfs.pid`, ready when `GET /` answers 403.

- [ ] **Step 1: Write the failing test** — `test_cost_rustfs_bin.py::test_script_has_start_stop_status_and_pid_file` reads the script text and asserts the strings `start)`, `stop)`, `status)`, `.rustfs.pid`, `19121`, `RUSTFS_DURABILITY_MODE=relaxed` are present, and `test_script_refuses_nonempty_foreign_data_dir` asserts the marker check `.cas-yardstick-rustfs` is kept.
- [ ] **Step 2: Run it** — `python3 -m unittest tests.test_cost_rustfs_bin -v` from `utils/cas-yardstick`. Expected: FAIL (file missing).
- [ ] **Step 3: Write `rustfs-bin.sh`** modelled on `rustfs.sh` but running `${BIN:-<repo>/ci/tmp/rustfs} /data`-style with `RUSTFS_ADDRESS=127.0.0.1:19121`, `RUSTFS_VOLUMES` as the data dir, stdout to `<data>/rustfs.log`, `setsid nohup`; `stop` kills the pid; `status` prints `up`/`down`.
- [ ] **Step 4: Run the unit test (PASS) and a live check**: `utils/cas-yardstick/rustfs-bin.sh start && utils/cas-yardstick/rustfs-bin.sh status && curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:19121/` → `up`, `403`; then `stop`.
- [ ] **Step 5: README section "Cost rig"**: one paragraph (what the rig adds), the port table from Global Constraints, the three commands (`rustfs-bin.sh start`, `yardstick.py cost probe`, `yardstick.py cost run`).
- [ ] **Step 6: Commit** — `git add utils/cas-yardstick/rustfs-bin.sh utils/cas-yardstick/README.md utils/cas-yardstick/tests/test_cost_rustfs_bin.py && git commit -s -m "yardstick: RustFS from the cached binary for the cost rig"`.

### Task 2: Ledger proxy

**Files:**
- Create: `utils/cas-yardstick/yardstick/cost/__init__.py`, `utils/cas-yardstick/yardstick/cost/proxy.py`
- Test: `utils/cas-yardstick/tests/test_cost_proxy.py`

**Interfaces:**
- Produces: `class LedgerProxy(upstream: str, ledger_path: str, callers: dict[str, int], control_port: int)` with `start() -> None`, `stop() -> None`, `mark(label: str) -> int` (appends a `{"kind":"mark","label":...,"seq":N}` row and returns the next attempt seq), `set_fault(rule: dict) -> str` (rule id), `clear_faults() -> None`. Fault rule: `{"caller": "r1"|"*", "op": "PutObject"|"UploadPart"|"*", "key_regex": str, "nth": int, "action": {"status": 500} | {"drop_response": true}}`; `drop_response` forwards the request to the store, then closes the client connection without a response (upstream outcome recorded).
- Ledger row (JSONL), one per attempt: `seq, caller, t_start_us, t_end_us, method, path, query (dict), bucket, key, op (parsed, Task 3), status (int or null when dropped), upstream_status (int), s3_error_code (str|null), req_bytes, resp_bytes, expect (bool), upload_id, part_number, copy_source, injected (rule id|null), headers_subset {x-amz-*, content-length, etag}`.
- `main()`: `python3 -m yardstick.cost.proxy --upstream http://127.0.0.1:19121 --ledger PATH --caller r1=19131 --caller r2=19132 --caller r3=19133 --caller rig=19134 --control 19139`.

- [ ] **Step 1: Write failing tests** (stdlib `http.server` fake upstream in the test):
  - `test_forwards_put_and_records_row`: PUT `/test/a/b.txt` with 11 bytes via caller port → upstream receives body; row has `caller='r1'`, `method='PUT'`, `bucket='test'`, `key='a/b.txt'`, `req_bytes=11`, `status=200`, `t_end_us >= t_start_us`.
  - `test_records_expect_header_flag`: request with `Expect: 100-continue` → row `expect=True`.
  - `test_fault_status_injected_without_upstream_call`: rule `{op:'*', key_regex:'^a/', nth:1, action:{status:500}}` → first matching attempt gets 500, `injected=<rule id>`, `upstream_status=None`; second attempt passes through.
  - `test_fault_drop_response_reaches_upstream`: rule `{action:{drop_response:true}}` → upstream received the PUT, client sees a closed connection, row `status=None`, `upstream_status=200`.
  - `test_mark_rows_are_sequenced`: `mark('w0')` then a request then `mark('w1')` → ledger order mark, attempt, mark with increasing `seq`.
  - `test_streams_large_body_without_buffering_all`: 64 MiB PUT forwarded with peak proxy RSS growth under 16 MiB (read `/proc/self/status` VmRSS before/after) — guards streaming.
- [ ] **Step 2: Run** — `python3 -m unittest tests.test_cost_proxy -v`. Expected: FAIL (module missing).
- [ ] **Step 3: Implement `proxy.py`**: one `ThreadingHTTPServer` per caller port, handler forwards with `http.client.HTTPConnection` to the upstream, copies headers except hop-by-hop, streams request and response bodies in 1 MiB chunks counting bytes, parses `<Error><Code>` from non-2xx bodies (first 4 KiB) into `s3_error_code`, writes rows under a lock with `seq` from a shared counter; control server on `control_port` with `POST /fault`, `DELETE /fault`, `POST /mark`. Keep-alive supported (`protocol_version = 'HTTP/1.1'`).
- [ ] **Step 4: Run tests** — PASS.
- [ ] **Step 5: Commit** — `yardstick: ledger proxy with fault injection`.

### Task 3: Ledger parsing, summaries and lifecycle replay

**Files:**
- Create: `utils/cas-yardstick/yardstick/cost/ledger.py`, `docs/superpowers/cost-model/ledger.schema.json`
- Test: `utils/cas-yardstick/tests/test_cost_ledger.py`

**Interfaces:**
- Produces: `parse_operation(method: str, path: str, query: dict, headers: dict) -> str` returning one of `PutObject, CopyObject, CreateMultipartUpload, UploadPart, UploadPartCopy, CompleteMultipartUpload, AbortMultipartUpload, GetObject, HeadObject, ListObjectsV2, DeleteObject, DeleteObjects, Other` (rules: `?uploads` POST → Create; `uploadId`+`partNumber` PUT → UploadPart (`x-amz-copy-source` → UploadPartCopy); `uploadId` POST → Complete; `uploadId` DELETE → Abort; `?delete` POST → DeleteObjects; `x-amz-copy-source` PUT → CopyObject; `list-type=2` GET → ListObjectsV2; HEAD → HeadObject).
- `@dataclass Attempt` (all ledger row fields, plus `op`), `read_ledger(path, seq_from: int, seq_to: int) -> list[Attempt]`.
- `summarize(attempts: list[Attempt], callers: set[str]) -> dict[str, dict]`: per op → `{attempts, billed_class_hint, status_2xx, status_4xx, status_5xx, dropped, req_bytes, resp_bytes, list_pages, delete_keys}`.
- `correlate_retries(attempts) -> list[list[int]]` groups seqs of the same caller, op, key and `req_bytes` within 60 s where an earlier attempt had status ≥500 or dropped.
- `replay(attempts, initial: Inventory | None, delete_bodies: dict[int, list[str]] | None) -> Inventory` where `Inventory` holds `objects: dict[(bucket,key) -> (size, created_us)]`, `incomplete: dict[(bucket,key,upload_id,part_number) -> (size, t_us)]`, `transitions: list[(t_us, kind, identity, size)]`; `byte_hours(inventory_or_transitions, t0_us, t1_us) -> dict[category -> float]` with categories `object`, `incomplete`; rules per spec §5.1 (write = replacement by identity; multipart part replacement by (upload id, part number); complete = retire parts + create object with the summed size; abort = retire parts; failed statuses leave state unchanged; `DeleteObjects` applies per-key outcomes from the response body).
- `check_ledger(attempts) -> list[str]` problems: `expect` flag set, `op == 'Other'`, attempt without `t_end_us`.
- `ledger.schema.json`: JSON Schema (draft 2020-12) of a row; `validate_rows(path) -> list[str]` using a minimal in-house validator (required keys and types only).

- [ ] **Step 1: Failing tests** — `test_parse_operation_table` (one assertion per op in the rules above), `test_summarize_counts_pages_and_delete_keys`, `test_retry_correlation_requires_same_key_and_payload_hash` (two PUTs same key different `req_bytes` → two groups), `test_replay_put_replaces_same_identity_only`, `test_replay_multipart_part_replacement_and_completion` (two parts, part 1 replaced, complete → one object of size p1' + p2, zero incomplete), `test_replay_abort_retires_incomplete`, `test_replay_batch_delete_partial_failure` (body with keys a,b; response marks b `NoSuchKey` → only a removed), `test_byte_hours_short_lived_object_counts` (object alive 30 min inside a 1 h window → 0.5 × size), `test_check_ledger_flags_expect_and_other`, `test_schema_validates_proxy_row`.
- [ ] **Step 2: Run** — FAIL.
- [ ] **Step 3: Implement `ledger.py` and write `ledger.schema.json`** (fields of Task 2's row, `op`, plus `delete_keys: [str]` and `delete_results: [{key, outcome}]` which the proxy fills for `DeleteObjects` by parsing the request and response XML — add that parsing to `proxy.py` in this task).
- [ ] **Step 4: Run** — PASS; also `python3 -m unittest tests.test_cost_proxy` still PASS.
- [ ] **Step 5: Commit** — `yardstick: ledger parsing, retry correlation and lifecycle replay`.

### Task 4: Interserver byte relay

**Files:**
- Create: `utils/cas-yardstick/yardstick/cost/interserver_proxy.py`
- Test: `utils/cas-yardstick/tests/test_cost_interserver_proxy.py`

**Interfaces:**
- Produces: `class ByteRelay(listen: tuple[str,int], target: tuple[str,int], log_path: str)` with `start()`, `stop()`, `totals() -> dict{'to_target': int, 'from_target': int, 'connections': int}`; log row per closed connection `{t_open_us, t_close_us, peer, to_target, from_target}`. The relay is the advertised `interserver_http_host:port` of a replica (`127.0.1.<i>`), the target its real `127.0.0.1:port`.

- [ ] **Step 1: Failing tests** — `test_relay_counts_both_directions` (echo server as target; send 1000 bytes, receive 1000 → totals 1000/1000, 1 connection), `test_relay_survives_target_refusal` (target closed → client connection closed, no exception in the relay thread), `test_loopback_alias_bind` (bind `127.0.1.9:0` succeeds on this host).
- [ ] **Step 2: Run** — FAIL.
- [ ] **Step 3: Implement** with `socket` + two threads per connection, `selectors` optional.
- [ ] **Step 4: Run** — PASS.
- [ ] **Step 5: Commit** — `yardstick: interserver byte relay`.

### Task 5: Cluster configuration (replicas, Keeper, proxies, local policy)

**Files:**
- Modify: `utils/cas-yardstick/yardstick/config.py` (add `write_cost_config`, keep `write_config` unchanged)
- Create: `utils/cas-yardstick/yardstick/cost/topology.py`
- Test: `utils/cas-yardstick/tests/test_cost_config.py`

**Interfaces:**
- Produces: `@dataclass Topology(port_base: int = 19700, replicas: int = 1, proxy_ports: dict[str,int] = {r1:19131,r2:19132,r3:19133,rig:19134}, control_port: int = 19139, store_port: int = 19121)` with `tcp(i)`, `http(i)`, `interserver(i)`, `keeper()`, `relay_host(i) -> '127.0.1.<i>'`, `endpoint(i) -> 'http://127.0.0.1:<proxy port of r<i>>/test/'`.
- `write_cost_config(run_dir: str, replica: int, policy: str, topology: Topology, prefix: str, local_dir: str | None, gc_enabled: bool, cas_server_root_id: str) -> str` (config hash): policies `cas, s3, s3plain, local` and `_cached` variants; `local` is `<type>local</type><path>{local_dir}/r{replica}/</path>`; non-shared disks use prefix `{prefix}/r{replica}/`, CAS the shared `{prefix}/` with `cas_server_root_id` per replica; `<zookeeper><node><host>127.0.0.1</host><port>{keeper}</port></node></zookeeper>`; `<macros><replica>r{replica}</replica><shard>1</shard></macros>`; `<interserver_http_host>127.0.1.{replica}</interserver_http_host>`; replica 1 gets `<keeper_server>` (single raft node, `tcp_port` = `keeper()`, paths under run_dir); the endpoint goes through the replica's proxy port.

- [ ] **Step 1: Failing tests** — `test_topology_ports_do_not_collide` (all ports of a 3-replica topology distinct), `test_local_policy_config_has_path_under_local_dir`, `test_non_shared_prefix_is_per_replica_and_cas_is_shared`, `test_replica1_has_keeper_server_and_all_have_zookeeper_and_macros`, `test_endpoint_uses_proxy_port_of_replica`, `test_existing_write_config_unchanged` (hash of `write_config` output for `s3` equals the value computed before this task — compute once and pin).
- [ ] **Step 2: Run** — FAIL.
- [ ] **Step 3: Implement** in `config.py` + `topology.py`.
- [ ] **Step 4: Run all tests** — PASS (139 + new).
- [ ] **Step 5: Commit** — `yardstick: cost topology and multi-replica config`.

### Task 6: Cluster process control

**Files:**
- Modify: `utils/cas-yardstick/server.sh` (accept optional 4th arg `extra_args` passed through to the server command line)
- Create: `utils/cas-yardstick/yardstick/cost/cluster.py`
- Test: `utils/cas-yardstick/tests/test_cost_cluster.py`

**Interfaces:**
- Produces: `class Cluster(binary: str, run_dir: str, topology: Topology, policy: str, prefix: str, local_dir: str|None, gc_enabled: bool, server_sh: str)` with `start_all() -> None` (replica 1 first, waits for Keeper TCP port, then the others; starts one `ByteRelay` per replica), `stop(i)`, `start(i) -> float` (seconds from launch to `SELECT 1`), `stop_all()`, `clients: list[Client]` (from `yardstick.client`), `pids: list[int]`, `relays: list[ByteRelay]`, `startup_seconds: list[float]`. `start_all` refuses when any port answers already (`RuntimeError('port N busy')`).
- Uses `server.sh start <binary> <run_dir>/r<i> <tcp port> "--keeper_server.tcp_port=… --interserver_http_host=127.0.1.i"` (replica 1 adds the keeper args).

- [ ] **Step 1: Failing tests** with a `FakeServerSh` recorded by command — `test_start_all_orders_keeper_replica_first`, `test_cluster_refuses_busy_port` (bind a socket on `http(2)` first), `test_start_returns_seconds_and_records_pid`, `test_stop_all_stops_relays`.
- [ ] **Step 2: Run** — FAIL.
- [ ] **Step 3: Implement**; extend `server.sh` to append `$4` to the command line.
- [ ] **Step 4: Run tests** — PASS. Live check (needs RustFS + proxy running): `python3 -c` script starting a 2-replica `s3` cluster in `tmp/cas-yardstick/cost-runs/smoke`, `SELECT * FROM system.zookeeper WHERE path='/'` on replica 2 returns rows; `stop_all`.
- [ ] **Step 5: Commit** — `yardstick: multi-replica cluster control`.

### Task 7: Local I/O accounting and `CostCard` interface

**Files:**
- Create: `utils/cas-yardstick/yardstick/cost/localio.py`, `utils/cas-yardstick/yardstick/cost/cards/__init__.py`, `utils/cas-yardstick/yardstick/cost/cards/base.py`
- Test: `utils/cas-yardstick/tests/test_cost_localio.py`, `utils/cas-yardstick/tests/test_cost_cards_base.py`

**Interfaces:**
- `proc_io(pid: int) -> dict{rchar, wchar, syscr, syscw, read_bytes, write_bytes}` from `/proc/<pid>/io`; `delta(a, b)`.
- `class DirSampler(paths: list[str], interval_s: float = 1.0)` with `start()`, `stop() -> dict[path -> {peak_bytes, samples}]` (du-equivalent via `os.scandir` recursion).
- `diskstats(dev: str) -> dict{reads, read_sectors, writes, write_sectors}` from `/proc/diskstats`; `device_of(path) -> str` via `os.stat().st_dev` and `/proc/self/mountinfo` (returns the dm/loop name; documented as host-wide, not per process).
- `class CostCard` (in `cards/base.py`): attributes `name: str`, `policies: tuple[str, ...]`, `replicas: int = 1`, `maintenance: frozenset[str] = frozenset()` (subset of `{'merges','gc'}` the card measures; others are stopped: merges via `SYSTEM STOP MERGES` on every replica, GC via `cas_gc_enabled=0`), `COUNTS: dict[str,int]`, `counts(scale)`, methods `setup(cluster, policy, counts)`, `measured(cluster, policy, counts) -> dict{'unit_count': int, 'ops': list[dict]}`, `complete(cluster, timeout_s: float = 300) -> None` (default: on every replica `SYSTEM SYNC REPLICA <t> STRICT` for the card's tables, wait `system.replication_queue` empty and `system.merges`/`system.mutations` idle, `SYSTEM FLUSH LOGS`), `expected_rows(counts) -> int`, `assertions(cluster, summary: dict, inventory) -> dict[str, tuple]` (default: row count equal on all replicas), `teardown(cluster)`. Helper `replicated_engine(policy, table, extra_settings) -> str` building `ReplicatedMergeTree('/yardstick/{table}', '{replica}') ORDER BY k SETTINGS storage_policy = '<policy>', <TABLE_SETTINGS><extra>`.

- [ ] **Step 1: Failing tests** — `test_proc_io_self_grows_after_write` (write 1 MiB + `os.fsync` to a temp file; `write_bytes` delta ≥ 1 MiB), `test_dir_sampler_peak` (create then delete a 2 MiB file during sampling → peak ≥ 2 MiB), `test_device_of_returns_block_name`, `test_cost_card_default_complete_issues_sync_replica_and_flush` (FakeCluster with recorded queries), `test_replicated_engine_clause`.
- [ ] **Step 2: Run** — FAIL. **Step 3: Implement.** **Step 4: Run** — PASS.
- [ ] **Step 5: Commit** — `yardstick: local I/O accounting and CostCard interface`.

### Task 8: Measurement record and the cost runner

**Files:**
- Create: `utils/cas-yardstick/yardstick/cost/measurements.py`, `utils/cas-yardstick/yardstick/cost/runner.py`
- Test: `utils/cas-yardstick/tests/test_cost_runner.py`

**Interfaces:**
- `@dataclass Measurement`: `run_id, card, policy, replicas, k, scale, binary_sha256, commit, config_hashes: list[str], t_start_us, t_end_us, wall_s, unit_count, ledger: dict[caller -> summarize()]`, `background: dict[caller -> summarize()]` (attempts outside the window, same pass), `rig: summarize()`, `events: dict[replica -> delta dict]`, `proc_io: dict[replica -> delta]`, `local_peak_bytes: dict[replica -> int]`, `interserver: dict[replica -> totals]`, `inventory_byte_hours: dict`, `assertions: dict[name -> {expected, actual, ok}]`, `status: 'ok'|'FAILED'`, `error: str|None`; `write(path, m)` appends one JSON line to `measurements.json`; `read(path) -> list[Measurement]`.
- `@dataclass Deps(binary, run_id, runs_dir, topology, proxy: LedgerProxy, local_dir, scale, store_prefix='test')`.
- `run_card(card: CostCard, policy: str, k: int, deps: Deps) -> Measurement`: pass dir `<runs_dir>/<run_id>/<policy>/<card>/k<k>/`; cluster start; `card.setup`; stop maintenance not in `card.maintenance`; `proxy.mark('start')`, events/proc_io/relay snapshots, `DirSampler.start`; `card.measured`; `card.complete`; snapshots; `proxy.mark('end')`; ledger slice → summaries per caller (rig excluded from `ledger`, kept in replay); `replay` over the whole pass → byte-hours within the window; `card.assertions`; teardown; stop; record. Any exception → `status='FAILED'`, `error` with the pass dir and server log path, no partial numbers.
- `run_matrix(cards, policies, repeats, deps) -> list[Measurement]`.

- [ ] **Step 1: Failing tests** with fakes (FakeCluster, FakeProxy writing a ledger file, a card with two inserts) — `test_run_card_writes_measurement_with_ledger_slice_and_assertions`, `test_rig_traffic_excluded_from_ledger_but_replayed`, `test_run_card_server_exit_marks_failed` (FakeCluster raises mid-card), `test_maintenance_stopped_when_not_measured` (queries contain `SYSTEM STOP MERGES` when `'merges' not in card.maintenance`), `test_measurement_roundtrip_json`.
- [ ] **Step 2: Run** — FAIL. **Step 3: Implement.** **Step 4: Run** — PASS.
- [ ] **Step 5: Commit** — `yardstick: cost runner and measurement records`.

### Task 9: `cost` CLI

**Files:**
- Modify: `utils/cas-yardstick/yardstick.py` (add `cost` subcommand group)
- Create: `utils/cas-yardstick/yardstick/cost/cli.py`
- Test: `utils/cas-yardstick/tests/test_cost_cli.py`

**Interfaces:**
- `yardstick.py cost run --binary PATH --cards a,b --policies s3,cas --replicas N --repeats K --scale S --local-dir DIR --runs-dir DIR [--rustfs-bin PATH] [--keep-prefix]`: starts the proxy (and RustFS via `rustfs-bin.sh` unless `--endpoint`), runs the matrix, prints a table (card × policy: wall, PUT-class, GET-class, LIST pages, DELETE calls/keys, interserver bytes, local write bytes), writes `measurements.json`, `ledger.jsonl`, `interserver.jsonl` into the run dir; exit 1 on any FAILED.
- `yardstick.py cost probe --binary PATH --local-dir DIR`: runs the five acceptance probes (Task 11) and prints PASS/FAIL per probe with the numbers the gate requires; exit 2 on any FAIL.
- `yardstick.py cost estimate <run-dir>`: sums per-pass wall from `measurements.json`, multiplies by the coverage seed's card × policy × K counts (Task 10) and prints the projected hours with the P1-only and P1+P2 totals.

- [ ] **Step 1: Failing tests** — `test_cost_run_parses_options`, `test_cost_probe_lists_five_probes`, `test_cost_estimate_projects_hours_from_measurements` (fixture `measurements.json` with 2 passes of 30 s → formula output).
- [ ] **Step 2: Run** — FAIL. **Step 3: Implement.** **Step 4: Run** — PASS.
- [ ] **Step 5: Commit** — `yardstick: cost CLI (run, probe, estimate)`.

### Task 10: Artifact seeds and closure-check skeleton

**Files:**
- Create: `docs/superpowers/cost-model/measurement-contract.md`, `docs/superpowers/cost-model/inputs-registry.json`, `docs/superpowers/cost-model/coverage.json`, `docs/superpowers/cost-model/billing-contract.json`, `utils/storage-cost-calculator/tools/closure_check.py`
- Test: `utils/storage-cost-calculator/tools/test_closure_check.py` (unittest)

**Interfaces:**
- `inputs-registry.json`: `{"version": "0.1.0", "quantities": [{"id", "block", "kind": "input|pinned|measured|derivation", "scope": "cluster|replica|pool|volume", "unit", "default" | "rule" | "coef_ref", "range": [lo, hi] | null, "evidence": {"kind": "assumed|source|measured|provider", "ref"}}]}`; seed = the simple inputs (`data_bytes`, `ingest_rate`, `retention_days`, `replicas`, `shards`) and the precedence pairs of spec §4.2 as `"precedence": [{"authoritative": id, "derived": id, "rule": expr}]`.
- `coverage.json`: `{"version": "0.1.0", "cards": [{"name", "family", "variants": [...], "role": "fit|holdout|validation", "priority": "P1|P2", "coefficients": [ids]}], "excluded": [...]}` seeded from spec §5.6.
- `billing-contract.json`: `{"version":"0.1.0","presets":[{"id":"aws-us-east-1","as_of":"YYYY-MM-DD","currency":"USD","assumptions":[...spec §4.6...],"rules":[{"id","unit","price","per","source_url","checked":"date"}],"outcome_billing":{"source_url","checked","billed_statuses":[...],"exceptions":[...]},"ebs":{"gp3":{"baseline_iops":3000,"baseline_mibps":125,"iops_per_gib_max":500,"mibps_per_iops_max":0.25,"max_size_gib","max_iops","max_mibps","source_url","checked"}},"allocation":{"objective":"min_monthly_cost","tie_break":["fewest_volumes","smallest_total_iops"]}}]}` — the implementer looks the values up on the AWS pricing and documentation pages on the day and records the URLs and date; no value is recalled.
- `closure_check.py`: `check_dependency(registry) -> list[str]` (ids unique, derivation refs resolve, acyclic, units present), `check_support(registry, coverage) -> list[str]` (every `measured` quantity's `coef_ref` appears in some card's `coefficients`; every P1 measured coefficient has a `fit` and a `holdout` card), `check_billing(billing) -> list[str]` (every rule has `source_url` and `checked`; gp3 fields present), `main()` exits 1 on problems.
- `measurement-contract.md`: the spec §5.1 obligations as implemented in Tasks 2–8: ledger schema reference, parsed operations, caller ports, retry correlation rule, replay rules, byte-hour bounds (proxy timestamps: request start/response end), background policy, interserver relay, local I/O (`/proc/pid/io`, host-wide diskstats caveat), counter overlap list (`S3*` vs `DiskS3*`, from `src/Common/ProfileEvents.cpp` at the pinned commit), what one workstation cannot establish.

- [ ] **Step 1: Failing tests** — `test_registry_seed_passes_dependency_check`, `test_dependency_check_detects_cycle`, `test_support_check_requires_fit_and_holdout_for_p1`, `test_billing_check_requires_sources`.
- [ ] **Step 2: Run** — FAIL. **Step 3: Write the four artifacts and the script.** **Step 4: Run** — PASS; `python3 utils/storage-cost-calculator/tools/closure_check.py` exits 0 on the seeds.
- [ ] **Step 5: Commit** — `cost-model: artifact seeds and closure checks`.

### Task 11: Acceptance probes

**Files:**
- Create: `utils/cas-yardstick/yardstick/cost/cards/probes.py` (five `CostCard`s), register in `cards/__init__.py`
- Test: `utils/cas-yardstick/tests/test_cost_probes.py` (unit, with fakes) and the live gate run

**Interfaces (the five probes):**
- `ProbeS3Insert` (`probe_s3_insert`, policies `s3`, 1 replica, `maintenance=∅`): one table `k UInt64, a UInt64, b String`, one `INSERT … SELECT … FROM numbers(1000)`; `expected_ledger()` is **read from a file** `tmp/cas-yardstick/predictions/probe_s3_insert.json` written by the implementer after reading the pinned source (`DiskObjectStorageTransaction`, `MergeTreeDataPartWriterCompact/Wide`, `WriteBufferFromS3`): per op exact counts; assertion = exact equality per op for caller `r1`.
- `ProbeReplicatedFetch` (`s3`, 2 replicas): insert on r1, `complete` on both; assertions: `r2.PutObject ≥ 1`, interserver relay of r1 `from_target > 0` (bytes left r1) and relay of r2 `to_target == 0` unless r2 fetched from itself; row counts equal.
- `ProbeMultipartFault` (`s3`, 1 replica): table with one `String` column of 1 KiB values × 70 000 rows (≈ 70 MiB object, crosses the default multipart threshold at the pinned commit — the implementer confirms the threshold from source and records it in the prediction file); fault rule: `UploadPart` `nth=2` → drop response; assertions: ledger shows ≥ 2 distinct `(upload_id, part_number)` with part 2 appearing twice (replacement), the upload completes or aborts, and the inventory's incomplete bytes return to 0 after the rig's `AbortMultipartUpload` through caller `rig` for any upload still open at teardown.
- `ProbeCasGcCohort` (`cas`, 1 replica, `maintenance={'gc'}`, `cas_gc_interval_sec` set to 5 via config): insert 20 parts, `DROP TABLE … SYNC`, record the cohort = keys written by `r1` in the window; poll `system.cas_gc_log` and the rig's `ListObjectsV2` (caller `rig`) until every cohort key is gone or 300 s; assertions: cohort reclaimed, `reclaim_delay_s` measured from the DROP to the last cohort `DeleteObject(s)` in the ledger.
- `ProbeLocalVolume` (`local`, 1 replica): 200 inserts of 20 000 rows on the `local` policy with `DirSampler` on the local dir; assertions: `proc_io.write_bytes > 0`, `local_peak_bytes > 0`, and the record carries `device_of(local_dir)`, `diskstats` deltas and the host-specific label when the device also hosts the run dir.

- [ ] **Step 1: Unit tests with fakes** — each probe's `assertions` evaluated on a synthetic summary (PASS/FAIL both directions).
- [ ] **Step 2: Run** — FAIL. **Step 3: Implement the probes and the prediction file for `probe_s3_insert`.** **Step 4: Unit tests PASS.**
- [ ] **Step 5: Live gate** — `utils/cas-yardstick/rustfs-bin.sh start && python3 utils/cas-yardstick/yardstick.py cost probe --binary tmp/bin/26.8/clickhouse --local-dir tmp/cas-yardstick/local > tmp/cost_probe.log 2>&1; echo $?`. Expected exit 0 with five PASS lines. A FAIL is investigated with the systematic-debugging skill; a prediction mismatch on `probe_s3_insert` is a defect of the prediction unless the ledger shows an `Other` op or an `expect` flag (then a proxy defect).
- [ ] **Step 6: Record the gate in the ledger** — append to `docs/superpowers/reports/2026-10-23-storage-cost-calculator/LEDGER.md` the five probe results, the `probe_s3_insert` prediction vs ledger table, the local-volume device and label, and the runtime estimate from `yardstick.py cost estimate` for the coverage seed (P1 and P1+P2 hours).
- [ ] **Step 7: Commit** — `yardstick: acceptance probes (phase-0 gate)` with the ledger update.

### Task 12: Phase-0 review and hand-off

- [ ] **Step 1: Codex review of the measurement contract** — prompt via file `tmp/codex_mc_review_prompt.txt` (scope: `docs/superpowers/cost-model/measurement-contract.md`, `ledger.schema.json`, `yardstick/cost/*.py`; questions: can the ledger reproduce request counts and byte-hours; is caller/observer separation sound; what leaks); `codex exec -m gpt-6-astra -s read-only -c model_reasoning_effort=high --cd <worktree> - < prompt > tmp/codex_mc_review_r1.log`.
- [ ] **Step 2: Close every MAJOR** with a failing test first, then the fix; record dispositions in `measurement-contract.md` §"Review".
- [ ] **Step 3: Update LEDGER.md** step 4 → phase 0 done, with the gate numbers and the decision on P2 cuts if the estimate exceeds 72 h.
- [ ] **Step 4: Commit** — `cost-model: measurement contract reviewed`.
