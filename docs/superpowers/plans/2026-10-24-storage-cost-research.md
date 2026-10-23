# Storage Cost Calculator — Plan 2: Research programme (R5, R1–R4, hold-outs)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish, with evidence, every implementation fact the calculator needs — operation graphs per variant and operation, the coefficients with provenance, the capability/reuse matrix, lifecycle quantities — as versioned artifacts (`inputs-registry.json`, `coverage.json`, graph reports with `measurements.json`), each reviewed by Codex against the pinned source.

**Architecture:** Each research task follows spec §5.4: read the pinned source at `tmp/src-26.8` → write the operation graph with references → predict the ledger rows of each card → run the card on the Plan 1 rig → reconcile (exact integers on deterministic cards) → write the report → Codex review → close defects. Cards are `CostCard`s under `utils/cas-yardstick/yardstick/cost/cards/`. Predictions are JSON files next to the report so the page tests (Plan 3) read numbers. The registry and coverage artifacts grow in every task and the closure checks run at the end of every task.

**Tech Stack:** Plan 1 rig; Python stdlib; Codex CLI (`gpt-6-astra`, reasoning high, prompt via file, read-only).

**Spec:** `docs/superpowers/specs/2026-10-23-storage-cost-calculator-design.md` (§3 evidence policy, §4.3 invariants, §5.1 artifacts, §5.2 closure, §5.4 method, §5.5 budget and cut semantics, §5.6 coverage seed). Plan 1: `docs/superpowers/plans/2026-10-24-storage-cost-phase0-rig.md`.

## Global Constraints

- Source of truth for every code fact: `tmp/src-26.8` at commit `09d0772a842c5d73cf3ef35b4c8b017d838215a0`; every graph branch cites `path:line` there. The 26.6 tree in the worktree is never cited.
- Deterministic cards reconcile to the exact integer per operation kind for the measured caller; a miss is a graph defect and the task is not done until the cause is named (fixed, or recorded as an open item with the exact gap). Stochastic terms report absolute + relative tolerance from K=3.
- Hold-out cards are predicted and the prediction committed **before** the card is first run (commit hash in the report). A hold-out that drives a change is marked consumed; a fresh hold-out is added.
- Every new coefficient: registry entry (`kind: measured`, `evidence.ref` = report section + measurement run id), coverage row(s), validity range from the measured parameter span.
- Every report: `docs/superpowers/reports/2026-10-<dd>-storage-cost-<name>.md` with frontmatter, sections `Scope`, `Operation graphs`, `Predictions vs measurements`, `Gaps`, `Capability/reuse matrix`, `Coefficients`, `Review`, `How to re-run`; `measurements.json` and `predictions/*.json` beside it.
- Every report directory also carries `coefficients.json` (`[{id, value, dim, range, evidence:{kind, ref, run_id, binary_sha256, commit}, status}]`) and `formulas.json` (`{variant: {operation: {ledger_rows: [{op_kind, dim, expr}], notes: [expr]}}}` in the Plan 3 node language: `input, coef, const, add, sub, mul, div, min, max, ceil_to, floor_to, branch, sum_over, allocate, price`, every node with `dim`); Plan 3's generator reads these, so a graph that cannot be written as nodes is a finding, not a prose paragraph.
- Closure check after every task: `python3 utils/storage-cost-calculator/tools/closure_check.py` exits 0.
- Codex review per report: prompt via file, `codex exec -m gpt-6-astra -s read-only -c model_reasoning_effort=high --cd <worktree> - < prompt > tmp/codex_<name>_r1.log`; MAJORs closed before the task's final commit; dispositions in the report's `Review` section.
- Runtime budget: the phase-0 estimate (Plan 1, Task 11) governs; P2 cards are skipped when it exceeds 72 h, recorded in `coverage.json` (`"status": "cut"`) and in LEDGER.md, and the registry marks dependent coefficients `"status": "unsupported"`.
- Commits per task, `-s`, exact files, `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`; no push.

## Review Focus

1. A compact part whose uncompressed size crosses `min_bytes_for_wide_part` only after merging — the part-type rule must be applied per produced part, not per insert (Task 1 `boundaries` asserts part types of merge results).
2. A multipart upload whose object size equals the first-buffer size exactly — part count at the boundary (Task 2 `boundaries` asserts the uploaded part-size sequence from the ledger).
3. A mutation on a compact part — full rewrite, not hardlink reuse (Task 2/4 `mutation` runs both part types and asserts object counts).
4. A replica that re-merges locally instead of fetching — accounted as local execution with its own reads and writes, on CAS too (Task 5 `replication-local` forces it via `prefer_fetch_merged_part_time_threshold`-class settings established from source and asserts no fetch).
5. A GC round that finds nothing to reclaim on a populated pool — non-zero request cost (Task 4 `gc-idle` asserts the idle round's ledger rows and the heartbeat rows separately).

---

### Task 1: R5 — lifecycle definitions and part-type selection

**Files:**
- Create: `utils/cas-yardstick/yardstick/cost/cards/lifecycle.py` (`SmallInserts`, `MergeNatural`, `MergeForced`, `Boundaries`, `AsyncInserts`), `docs/superpowers/reports/2026-10-24-storage-cost-lifecycle.md`, `docs/superpowers/reports/2026-10-24-storage-cost-lifecycle/predictions/*.json`, `docs/superpowers/reports/2026-10-24-storage-cost-lifecycle/measurements.json`
- Modify: `docs/superpowers/cost-model/inputs-registry.json`, `docs/superpowers/cost-model/coverage.json`
- Test: `utils/cas-yardstick/tests/test_cost_cards_lifecycle.py`

**Interfaces:**
- Consumes: `CostCard`, `replicated_engine`, `run_card` (Plan 1).
- Produces: registry quantities `part_type_rule` (pinned, source), `min_bytes_for_wide_part`, `min_rows_for_wide_part`, `min_level_for_wide_part` (pinned, defaults at the commit), `parts_per_insert` (derivation), `write_amplification_rule` (derivation, fitted), `merge_fan_in_distribution`, `merge_result_size_distribution`, `old_parts_lifetime` (pinned), `acquisition_rule` (pinned: the settings and conditions that decide fetch vs local execution for merge and mutation results, from `MergeFromLogEntryTask.cpp` and `MutateFromLogEntryTask.cpp`), `async_insert_flush_rule` (pinned). Cards: `small-inserts` (5 cols, 1k rows × 1000 inserts, natural merges, `maintenance={'merges'}`), `merge-natural` (insert N parts then wait until the merge selector is idle; records `system.part_log` merge events: fan-in, bytes in/out), `merge-forced` (`OPTIMIZE TABLE … FINAL`, one merge), `boundaries` (inserts sized just below/above the byte and row thresholds and merge results crossing them; asserts `system.parts.part_type`), `async-inserts` (P2).

- [ ] **Step 1: Source → rules.** Read `MergeTreeData.cpp` (part type selection), `MergeTreeSettings.cpp` (defaults), `MergeFromLogEntryTask.cpp`, `MutateFromLogEntryTask.cpp`, `AsynchronousInsertQueue.cpp`; write the rules with `path:line` into the report's `Operation graphs` section and into the registry as pinned quantities. Commit the predictions for `boundaries` (part types per insert) before running it.
- [ ] **Step 2: Failing unit tests** — `test_boundaries_card_sizes_straddle_thresholds` (card computes sizes from the registry's thresholds; a fake registry with threshold 10 MiB yields inserts of 9 MiB and 11 MiB uncompressed), `test_merge_natural_collects_part_log_events` (fake client returns `part_log` rows → card result has `fan_in` list), `test_small_inserts_leaves_merges_on`.
- [ ] **Step 3: Run** — FAIL. **Step 4: Implement the cards.** **Step 5: Unit tests** — PASS.
- [ ] **Step 6: Run the cards** on policy `s3` (lifecycle is measured on one variant; others validate in their tasks): `yardstick.py cost run --binary tmp/bin/26.8/clickhouse --cards small-inserts,merge-natural,merge-forced,boundaries --policies s3 --repeats 3 --runs-dir tmp/cas-yardstick/cost-runs > tmp/cost_r5.log 2>&1`. Copy `measurements.json` beside the report.
- [ ] **Step 7: Reconcile** `boundaries` part types (exact), fit `write_amplification_rule` on `small-inserts` and `merge-natural` (state the fitted form, residuals, parameter span), record `merge_fan_in_distribution` and `merge_result_size_distribution`.
- [ ] **Step 8: Codex review** (scope: the report, the cards, the registry entries; question: do the rules match the source and do the cards exercise them) → close MAJORs.
- [ ] **Step 9: Closure check PASS; commit** — `research: lifecycle rules and amplification fit (R5)`.

### Task 2: R2 — `s3` disk operation graphs

**Files:**
- Create: `utils/cas-yardstick/yardstick/cost/cards/shapes.py` (`NarrowCompact`, `NarrowWide`, `WideCompact`, `WideWide`, `TypesFit`, `TypesHoldout`, `BigInsertMultipart`), `utils/cas-yardstick/yardstick/cost/cards/ops.py` (`Mutation`, `Drop`, `DetachAttach`, `ColdSelect`, `CacheEviction`, `Startup`), `docs/superpowers/reports/2026-10-24-storage-cost-s3.md` + `predictions/`, `measurements.json`
- Modify: registry, coverage
- Test: `utils/cas-yardstick/tests/test_cost_cards_shapes.py`, `tests/test_cost_cards_ops.py`

**Interfaces:**
- Produces: graphs for insert part (objects per part by shape, multipart sizing from `BufferAllocationPolicy`/`WriteBufferFromS3` with the effective settings as pinned quantities, the transaction's checks), merge, mutation (wide: rewritten columns + hardlinks; compact: full rewrite), lightweight delete, drop paths, detach/attach, cold read (`bytes_per_get`), startup with intact metadata; coefficients `objects_per_part[shape]`, `object_size_distribution[shape]`, `multipart_parts(size)`, `bytes_per_get[shape]`, `metadata_bytes_per_object`, `metadata_iops_per_object_write`, `startup_requests[s3]`; capability/reuse matrix rows for `s3`.
- Card shapes: `narrow-*` 5 columns (`UInt64`), `wide-*` 100 columns; compact/wide by row count against the pinned thresholds (asserted); `types-fit` and `types-holdout` 20 columns mixing `Nullable(UInt64)`, `Array(UInt32)`, `LowCardinality(String)`, `Map(String,UInt64)`, `JSON`, one projection, one `minmax` skip index, with different column orders and row counts so they are distinct datasets; `big-insert-multipart` one `String` column sized to cross the multipart threshold; `cold-select`: restart (`Cluster.stop/start`), `SELECT` touching N columns of M parts, cache on and off policies; `cache-eviction` (P2): working set 2× `max_size` of the cache disk; `startup`: externally timed `Cluster.start(i)` with the ledger window around it.

- [ ] **Step 1: Source → graphs** (`DiskObjectStorageTransaction.cpp`, `MergeTreeDataPartWriterWide/Compact.cpp`, `IMergeTreeDataPart.cpp` checksums/columns/count/primary files, `WriteBufferFromS3.cpp`, `BufferAllocationPolicy.cpp`, `S3Defines.h`, `MutateTask.cpp`, `MergeTreeData.cpp` drop/detach paths, `DiskObjectStorage.cpp` startup); write them with references; commit predictions for all cards, hold-outs first.
- [ ] **Step 2: Unit tests** for the cards (fakes): shapes assert part type and column count; `big-insert-multipart` asserts the ledger contains `CreateMultipartUpload` and ≥ 2 `UploadPart`; `cold-select` restarts before measuring; `startup` records seconds.
- [ ] **Step 3: Run** — FAIL. **Step 4: Implement.** **Step 5: Unit PASS.**
- [ ] **Step 6: Run fit cards** on `s3` and `s3_cached` (K=3): `narrow-compact,narrow-wide,wide-compact,types-fit,big-insert-multipart,merge-forced,mutation,drop,cold-select,startup`; then hold-outs `wide-wide,types-holdout,boundaries` once predictions are committed.
- [ ] **Step 7: Reconcile** exact integers per op for deterministic cards; tolerance for `cold-select` bytes; record gaps.
- [ ] **Step 8: Codex review** (source-to-graph audit: which branch is missing) → close MAJORs.
- [ ] **Step 9: Closure check; commit** — `research: s3 disk operation graphs (R2)`.

### Task 3: R3 — `s3_plain_rewritable` graphs and capability matrix

**Files:**
- Create: `docs/superpowers/reports/2026-10-24-storage-cost-s3plain.md` + `predictions/`, `measurements.json`
- Modify: registry, coverage; cards gain `supports(policy)` reasons from the capability gate in source
- Test: `utils/cas-yardstick/tests/test_cost_cards_s3plain.py`

**Interfaces:**
- Produces: the capability matrix row set for `s3plain` (which operations the disk refuses, cited from the capability gate in `MetadataStorageFromPlainRewritableObjectStorage*` at the commit), `__meta` object counts per operation, startup load (`ListObjectsV2` pages, GETs), rename/commit extras, `COPY` on hardlink; and the answer to "is `ReplicatedMergeTree` on this disk a supported deployment" with the source reference, encoded in coverage as `variants` for the replication cards.

- [ ] **Step 1: Source → graphs and the replication question**; commit predictions.
- [ ] **Step 2: Unit tests** — `test_s3plain_unsupported_ops_reported_with_reason`.
- [ ] **Step 3–5: Run/implement/pass.**
- [ ] **Step 6: Run** the same card set as Task 2 on `s3plain` (unsupported cards produce `UNSUPPORTED` rows with the reason) and, if replication is supported, the Task 5 replication cards.
- [ ] **Step 7: Reconcile; Step 8: Codex review; Step 9: closure check; commit** — `research: s3_plain_rewritable graphs and capability matrix (R3)`.

### Task 4: R4 — CAS graphs, branches, GC and lease

**Files:**
- Create: `utils/cas-yardstick/yardstick/cost/cards/cas.py` (`CasBranches`, `GcLifecycle`, `GcIdle`, `GcScale`, `Lease`), `docs/superpowers/reports/2026-10-24-storage-cost-cas.md` + `predictions/`, `measurements.json`
- Modify: registry, coverage
- Test: `utils/cas-yardstick/tests/test_cost_cards_cas.py`

**Interfaces:**
- Produces: the entry-branch graph per case (fresh blob, hash hit, trusted adoption, inline) with the evidence fields in `system.cas_log` that distinguish them (event kind **and** fields, plus decoded manifest placement where kinds coincide), staging behaviour (`staging_backend` default and what it writes locally), publication and ref-ledger writes; foreground ref removal on drop; GC terms: scheduler semantics at the commit (`CasGcScheduler.cpp`: when a round starts relative to the interval, heartbeat cadence and who pulses), per-round request terms by phase (`system.cas_gc_log` event types) as functions of pool objects, mounts and ref-log growth; reclaim delay by stage; lease renewals (`mount_renew_period_ms`); key families with the prefix pattern of each (from the key layout code) for lifecycle replay to classify bytes; coefficients `cas_objects_per_part[shape]`, `cas_branch_population[shape][branch]`, `cas_relink_requests[result_type]` (Task 5), `gc_round_cost(terms)`, `gc_heartbeat_requests_per_s`, `lease_requests_per_s`, `reclaim_delay_s[stage]`, `staging_peak_bytes_rule`.
- Cards: `cas-branches` (insert fresh data; insert identical data again → hash hits; `ALTER UPDATE` one column → adoption of the others; a table with tiny columns under the inline threshold → inline; each asserts the branch evidence counts); `gc-lifecycle` (as the probe, with the cohort followed through condemnation and deletion stages; reclaim delay per stage from `cas_gc_log`); `gc-idle` (populated pool, no changes, 3 intervals: rounds' and heartbeats' ledger rows separately, by key pattern); `gc-scale` (objects 1k/10k/100k × mounts 1 (P1); × mounts 2, × ref growth 2 levels (P2)); `lease` (idle mount 10 min, count renewals; extrapolated to 1 h with the measured period).

- [ ] **Step 1: Source → graphs** at `tmp/src-26.8/src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/…` (`ContentAddressedTransaction.cpp`, `Parts/*`, `Gc/CasGcScheduler.cpp`, `Gc/CasGc.cpp`, `Pool/*`, `ContentAddressedSettings.cpp`, key layout); commit predictions.
- [ ] **Step 2: Unit tests** — branch assertions on fake `cas_log` rows; `gc-idle` separates heartbeat keys from round keys by pattern; `gc-scale` builds the three pool sizes deterministically.
- [ ] **Step 3–5: Run/implement/pass.**
- [ ] **Step 6: Run** `cas-branches,gc-lifecycle,gc-idle,lease` and the Task 2 shape/ops cards on `cas` and `cas_cached`; `gc-scale` P1 subset; P2 rows if within budget.
- [ ] **Step 7: Reconcile** exact integers for `cas-branches` and the shape cards; fit `gc_round_cost` terms with ranges; `reclaim_delay_s` by stage with K=3 spread.
- [ ] **Step 8: Codex review; Step 9: closure check; commit** — `research: CAS graphs, branches, GC and lease (R4)`.

### Task 5: Replication, recovery and faults (all variants)

**Files:**
- Create: `utils/cas-yardstick/yardstick/cost/cards/replication.py` (`ReplicationFetch`, `ReplicationLocal`, `ReplicationNaturalFit`, `ReplicationNaturalHoldout`, `ReplicationZeroCopy`, `RestartCatchup`, `Replacement`), `utils/cas-yardstick/yardstick/cost/cards/faults.py` (`FaultPut5xx`, `FaultLostResponse`, `FaultMultipartAbort`, `FaultFetch`), `docs/superpowers/reports/2026-10-24-storage-cost-replication.md` + `predictions/`, `measurements.json`
- Modify: registry, coverage, the three variant reports (acquisition rows of the capability matrix)
- Test: `utils/cas-yardstick/tests/test_cost_cards_replication.py`

**Interfaces:**
- Produces: per variant and result type (insert, merge, mutation): acquisition cost at sender and receiver (ledger per caller + interserver bytes per relay), local-execution cost, the natural split under default settings (`fetch_fraction[result_type]` with K=3 spread), zero-copy costs on `s3`, restart catch-up (stop r2, work on r1 for `restart_downtime_min` scaled, start r2, measure r2's acquisition and startup), replacement (start r3 against a populated table), fault outcomes (`retry_outcome_mix`, `failed_upload_residue`, `fetch_retry_rate`).
- Forced cards set the acquisition settings established in Task 1 (`acquisition_rule`) to force fetch or local execution; `replication-natural-*` leave defaults.

- [ ] **Step 1: Predictions** from the Task 1 rule and the variant graphs; commit hold-out predictions first.
- [ ] **Step 2: Unit tests** — forced cards assert the acquisition path from `system.part_log`/`ReplicatedPartFetches*` deltas; fault cards install rules via `LedgerProxy.set_fault` and clear them in teardown; `restart-catchup` stops/starts the right replica.
- [ ] **Step 3–5: Run/implement/pass.**
- [ ] **Step 6: Run** on `s3`, `s3plain` (if supported), `cas`, `local` with 2 replicas; `replacement` with 3 (P2).
- [ ] **Step 7: Reconcile; Step 8: Codex review; Step 9: closure check; commit** — `research: replication, recovery and fault graphs`.

### Task 6: R1 — EBS demand and conversion

**Files:**
- Create: `utils/cas-yardstick/yardstick/cost/cards/localio_cards.py` (`IoSizes`: runs a shape card under `strace -f -e trace=pwrite64,pread64,fsync,fdatasync -o <file>` at smoke scale and histograms request sizes), `docs/superpowers/reports/2026-10-24-storage-cost-ebs.md` + `measurements.json`
- Modify: registry (`io_request_size_distribution[op]`, `proc_io_bytes[op]`, `ebs_io_rule` (provider-documented: SSD 256 KiB max I/O, merge of sequential small I/O; HDD 1 MiB; URL + date), `peak_factor` measured as the ratio of 1-s peak to mean `write_bytes` rate during `merge-natural`), coverage, billing contract (gp3 constraints with URL and date)
- Test: `utils/cas-yardstick/tests/test_cost_cards_localio.py`

- [ ] **Step 1: Document the measurement boundary**: `/proc/<pid>/io` per server (process-attributed, exact), host-wide diskstats (caveat), `strace` sizes at small scale, page cache dropped between cards (`sync; echo 3 > /proc/sys/vm/drop_caches` requires root — if unavailable, record "page cache not dropped; cold reads from the device are an upper bound"), host-specific label (single LVM root on this host).
- [ ] **Step 2: Unit tests** — strace histogram parser; peak factor from a synthetic series.
- [ ] **Step 3–5: Run/implement/pass.**
- [ ] **Step 6: Run** the shape, lifecycle, mutation, drop, cold-select, startup and replication cards on `local` (1 and 2 replicas); `io-sizes` at smoke scale.
- [ ] **Step 7: Derive** the EBS conversion with the dated rule and record its assumptions; **Step 8: Codex review; Step 9: closure check; commit** — `research: EBS demand and conversion (R1)`.

### Task 7: Hold-outs, per-variant validation and consolidation of artifacts

**Files:**
- Modify: all reports (hold-out sections), registry, coverage
- Create: `docs/superpowers/reports/2026-10-24-storage-cost-validation.md`

- [ ] **Step 1: Run the hold-outs** whose predictions were committed (`wide-wide`, `types-holdout`, `boundaries`, `replication-natural-holdout`) on every variant; `amplification-run` (P2, 2–4 h) if within budget.
- [ ] **Step 2: Record gaps** per coefficient; where a hold-out drives a change, mark it consumed and add a fresh one (new shape parameters), rerun.
- [ ] **Step 3: Per-variant validation** of lifecycle quantities (amplification, acquisition split) — differences between variants stay visible in the validation report.
- [ ] **Step 4: Evidence closure** — `closure_check.py` passes with every P1 measured coefficient having fit + hold-out; cut P2 rows marked.
- [ ] **Step 5: Codex review of the validation report and the two artifacts** (registry, coverage) → close MAJORs.
- [ ] **Step 6: LEDGER.md** — research phase done, open items listed; commit — `research: hold-outs and validation`.
