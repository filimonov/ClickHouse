# Storage Cost Calculator — Plan 2: Research programme (R5, R2, R3, R4, replication, R1, validation)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish with evidence every implementation fact the calculator needs — operation graphs per variant and operation as `formulas.json` cells, coefficients with provenance, the capability/reuse matrix, lifecycle quantities — in report directories per the Plan 1 convention, with every P1 coefficient backed by a fit edge and a distinct hold-out edge in `coverage.json`, each report reviewed by Codex against the pinned source.

**Architecture:** Every research task follows spec §5.4: read `tmp/src-26.8` → write the graph with `path:line` references → write **experiment manifests** (exact DDL, seeded generator, settings, operation sequence, completion, assertions, expected branch) → commit predictions (hold-outs first) → run on the Plan 1 rig → reconcile → write the report directory → Codex review → close defects → closure checks. Cards are `CostCard`s that read their manifest; a card name never defines an experiment, the manifest does.

**Tech Stack:** Plan 1 rig and schemas; Python stdlib; Codex CLI (`gpt-6-astra`, reasoning high, prompt via file, read-only).

**Spec:** `docs/superpowers/specs/2026-10-23-storage-cost-calculator-design.md` (§3, §4.3, §4.6, §5.1–§5.6). Plan 1: `docs/superpowers/plans/2026-10-24-storage-cost-phase0-rig.md` (Task 0 schemas are normative here).

## Global Constraints

- Code facts come only from `tmp/src-26.8` at `09d0772a842c5d73cf3ef35b4c8b017d838215a0`, cited `path:line` in `predictions/*.json` `source_refs` and in the report; the 26.6 tree is never cited.
- Experiment manifests `experiments/<card>.<case>.json`: `{"schema":"experiment/1","card","case","policies":[…],"replicas","ddl":[sql],"generator":{"sql_template","seed","rows","params"},"settings":{"table":{},"query":{},"server":{}},"sequence":[{"step","sql"|"action","on":"r1|r2|r3|rig"}],"controls":{"merges","gc","ttl"},"completion":{"kind":"default|custom","spec"},"assertions":[{"name","expr"}],"expected_branch":str|null,"tolerance":{"abs":{},"rel":{}}}` — add this schema to `docs/superpowers/cost-model/schemas/` in Task 1 and validate every manifest in a unit test.
- Deterministic cards reconcile to the exact integer per op for every workload caller; a miss is a graph defect: fix, or record as an open item with the exact gap. Stochastic terms: absolute + relative tolerance from K=3; both stated.
- Hold-out predictions are committed before the first run (`prediction_commit` = that hash). A hold-out that drives a change is `consumed` in `coverage.evidence` and a fresh case (new parameter values, new manifest) replaces it.
- `coverage.evidence` is per coefficient: one `fit` edge and one distinct `holdout` edge for every P1 measured coefficient, with `span`, `measurement_ids`, `status`; validation runs are `validation` edges. A coefficient id is owned by exactly one report (`coefficients.json`), and a formulas cell by exactly one `formulas.json`; the Plan 1 contract merge rejects duplicates.
- Report directories per `report-directory.md`: `README.md`, `measurements.jsonl`, `predictions/`, `experiments/`, `coefficients.json`, `formulas.json`, `raw/` + `SHA256SUMS`.
- Each task runs **and closes** the fit and hold-out edges of every coefficient it introduces (both predictions committed before the run; both measured; `status: done`) before it is complete; Task 7 performs integrated validation and replacement hold-outs only. After every task: `python3 utils/storage-cost-calculator/tools/closure_check.py --mode dev` exits 0.
- Every case named in a task is an executable manifest + card owned by that task (named below); a case name without a manifest is a plan defect.
- Every task adds the registry quantities of the advanced block(s) it owns with `label`, `hint.sql` and `hint.mapping` (Plan 3 renders the blocks from the registry); coefficient records carry `evidence.version`, `measurement_ids` and `tolerance`.
- Model variant for rig policy `local` is `ebs`; `*_cached` policies populate the `cache_enabled = 1` branch of the remote variants' cells (one `formulas.json` per variant, with `branch` on `input(cache_enabled)`).
- Codex review per report: prompt via file; `codex exec -m gpt-6-astra -s read-only -c model_reasoning_effort=high --cd <worktree> - < prompt > tmp/codex_<name>_r1.log`; MAJORs closed before the final commit; dispositions in `README.md` §Review.
- Budget and cuts: the phase-0 estimate governs. Cut order when over 72 h: P2 cards by family, then `replication-zero-copy`, then `types-*`. A cut sets `status: cut` on the card and its evidence edges, `status: unsupported` on dependent coefficients in the registry, and removes or changes any preset that needs them (Plan 3); recorded in LEDGER.md.
- Commits per task, `-s`, exact files, `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`; no push.

## Review Focus

1. A merge result whose uncompressed size crosses the compact/wide predicate — the part-type rule is applied per produced part (Task 1 `boundaries.merge-cross` asserts `system.parts.part_type` of the merge result).
2. An object whose size equals the first multipart buffer exactly — the uploaded part-size sequence is asserted from the ledger (Task 1 `boundaries.multipart-equal`).
3. A mutation of a compact part — full rewrite, asserted by object count (Task 2 `mutation.compact`).
4. A replica that executes a merge locally instead of fetching — accounted as local execution with its own reads and writes on every variant including CAS (Task 5 `replication-local.*` asserts `ReplicatedPartFetchesOfMerged == 0` and `ReplicatedPartMerges ≥ 1` on the follower).
5. A GC round on a populated pool with nothing to reclaim — non-zero request cost, heartbeat rows separated from round rows (Task 4 `gc-idle` asserts both counts nonzero and disjoint by key family).

---

### Task 1: R5 — lifecycle definitions, part-type and multipart boundaries

**Files:**
- Create: `docs/superpowers/cost-model/schemas/experiment.schema.json`, `utils/cas-yardstick/yardstick/cost/cards/manifest.py` (`load_manifest(path) -> Manifest`, `ManifestCard(CostCard)` that executes a manifest), `utils/cas-yardstick/yardstick/cost/cards/lifecycle.py` (`SmallInserts`, `MergeNatural`, `MergeForced`, `Boundaries`, `AsyncInserts` as `ManifestCard` subclasses with typed observations), `docs/superpowers/reports/2026-10-24-storage-cost-lifecycle/` (full convention)
- Modify: `inputs-registry.json`, `coverage.json`
- Test: `utils/cas-yardstick/tests/test_cost_manifest.py`, `tests/test_cost_cards_lifecycle.py`, `tests/live_lifecycle.py`

**Interfaces:**
- Registry additions (kind in brackets): `part_type_predicate` [pinned, source], `min_bytes_for_wide_part`, `min_rows_for_wide_part`, `min_level_for_wide_part` [pinned, defaults at the commit], `part_type` [derivation over uncompressed bytes, rows, level], `multipart_sizing_rule` [pinned: the buffer policy and its effective settings, from source], `multipart_parts(size)` [derivation], `parts_per_insert` [derivation], `write_amplification_rule` [derivation fitted on `small-inserts`/`merge-natural`; parameters and residuals recorded], `merge_fan_in_distribution`, `merge_result_size_distribution` [measured], `old_parts_lifetime` [pinned], `acquisition_rule` [pinned: settings and conditions deciding fetch vs local execution for merge and mutation results], `async_insert_flush_rule` [pinned].
- Experiments (cases constructed from the pinned predicate, never by row count alone): `boundaries.compact-below-bytes`, `boundaries.wide-above-bytes`, `boundaries.level-cross` (a merge whose result crosses `min_level_for_wide_part` when non-zero in the manifest's table settings), `boundaries.merge-cross` (compact inputs whose merge result is wide), `boundaries.multipart-below`, `boundaries.multipart-equal`, `boundaries.multipart-above` (object sizes relative to the first-buffer size from `multipart_sizing_rule`; assertion: the ledger's `UploadPart` size sequence); `small-inserts.default` (5 `UInt64` columns, 1 000 inserts × 1 000 rows, `merges: measure`); `merge-natural.default` (5 `UInt64` columns; insert 64 parts of 100 000 rows; wait until no merge for 60 s; observations `part_log_merges: [{fan_in, bytes_in, bytes_out, level}]`); `merge-natural.wide` (20 columns, 32 parts × 50 000 rows; hold-out for `merge_fan_in_distribution` and `merge_result_size_distribution`); `merge-forced.default` (5 columns, 8 parts × 100 000 rows, then `OPTIMIZE TABLE t FINAL`); `async-inserts.steady` (1 000 inserts of 100 rows at 20/s, `async_insert=1`, `async_insert_busy_timeout_ms=200`), `async-inserts.bursty` (10 bursts of 100 inserts) (P2); `amplification-run.2h` (5 columns, 1 000-row inserts at 1/s for 2 h; validation, P2, owned here, run in Task 7).

- [ ] **Step 1: Source → rules.** Read `MergeTreeData.cpp` (part type selection), `MergeTreeSettings.cpp`, `WriteBufferFromS3.cpp` + `BufferAllocationPolicy.cpp` + `S3Defines.h`, `MergeFromLogEntryTask.cpp`, `MutateFromLogEntryTask.cpp`, `AsynchronousInsertQueue.cpp`; write the rules with references into `README.md` §Operation graphs and the registry; write the manifests; commit the `boundaries.*` predictions (role `fit` for `*-below/above`, `holdout` for `merge-cross` and `multipart-equal`).
- [ ] **Step 2: Failing tests** — `test_every_manifest_validates`, `test_boundaries_cases_derive_sizes_from_registry_thresholds` (fake registry: 10 MiB threshold → cases of 9 and 11 MiB uncompressed and a merge case), `test_manifest_card_executes_sequence_on_named_replica` (FakeCluster), `test_merge_natural_observations_shape`, `test_small_inserts_control_is_measure`.
- [ ] **Step 3: Run → FAIL. Step 4: Implement. Step 5: PASS.**
- [ ] **Step 6: Live** — `yardstick.py cost run --cards boundaries,small-inserts,merge-natural,merge-forced --policies s3 --repeats 3 …`; copy `measurements.jsonl` and `raw/` into the report directory.
- [ ] **Step 7: Reconcile** `boundaries` exactly (part type, part-size sequence); fit the amplification rule; record distributions with spans; write `coefficients.json` (owner `lifecycle`), `formulas.json` (none — lifecycle supplies derivations, not cells), coverage edges (fit + holdout per coefficient; `merge_fan_in_distribution` hold-out = `merge-natural.wide` case with 20 columns).
- [ ] **Step 8: Codex review → close MAJORs. Step 9: closure check PASS; commit** — `research: lifecycle rules, part-type and multipart boundaries (R5)`.

### Task 2: R2 — `s3` disk operation graphs

**Files:**
- Create: `utils/cas-yardstick/yardstick/cost/cards/shapes.py`, `cards/ops.py`, `docs/superpowers/reports/2026-10-24-storage-cost-s3/` (convention) with `experiments/`
- Modify: registry, coverage
- Test: `tests/test_cost_cards_shapes.py`, `tests/test_cost_cards_ops.py`, `tests/live_s3.py`

**Interfaces:**
- `formulas.json` owner `s3`, variant `s3`, cells: `storage`, `inserts`, `merges`, `mutations`, `deletes`, `reads`, `recovery`, `local_disk` (states `not_supported` none). Coefficients (owner `s3`): `objects_per_part[shape][part_type]`, `object_size_distribution[shape]`, `insert_tx_checks` (requests the disk transaction issues per part besides object writes), `bytes_per_get[shape]`, `metadata_bytes_per_object`, `metadata_iops_per_object_write`, `startup_requests[s3]`, `cache_fill_io_per_byte`.
- Experiments: shapes `narrow-compact`, `narrow-wide`, `wide-compact`, `wide-wide` (hold-out), `types-fit` (20 columns: 5 `Nullable(UInt64)`, 5 `Array(UInt32)`, 5 `LowCardinality(String)`, 3 `Map(String,UInt64)`, 2 `JSON`; projection `SELECT k, sum(u0) GROUP BY k`; `minmax` skip index on `u1`), `types-holdout` (columns in the order `Array, JSON, Nullable, Map, LowCardinality` with counts 4/2/6/4/4; projection `SELECT k, max(a0) GROUP BY k` on the first `Array` column's `length`; `set(100)` index on the first `LowCardinality` column; 80 000 rows per insert), `big-insert-multipart` (one `String` column above the multipart threshold from `multipart_sizing_rule`), `mutation.wide-update-one-column` (`ALTER TABLE … UPDATE u1 = u1 + 1 WHERE 1`), `mutation.compact-update-one-column`, `mutation.wide-delete` (`ALTER TABLE … DELETE WHERE k % 10 = 0`), `mutation.lightweight-delete` (`DELETE FROM … WHERE k % 10 = 0`, `lightweight_deletes_sync = 2`), `drop.partition` (`DROP PARTITION`), `drop.ttl` (table `TTL d + INTERVAL 1 SECOND`, `merge_with_ttl_timeout = 1`, `controls.ttl = measure`), `drop.table-sync`, `detach-attach.default` (P2), `cold-select.cache-off`, `cold-select.cache-on` (restart via `Cluster.stop/start`, then `SELECT sum(u0), sum(u3) FROM t WHERE k BETWEEN a AND b` touching 2 of 40 columns over all parts; cache preparation: none for `cache-off`; for `cache-on` one warm pass, then the measured pass), `cache-eviction.2x` (P2: working set 2× the cache disk `max_size`), `startup.intact` (`Cluster.stop(1)`; `start(1)`; window around the start; observation `startup_seconds`).
- `mutation` cards include both part types by construction (two tables, one below and one above the predicate).

- [ ] **Step 1: Source → graphs** (`DiskObjectStorageTransaction.cpp`, `MergeTreeDataPartWriterWide/Compact.cpp`, `IMergeTreeDataPart.cpp`, `MergeTreeDataPartChecksum.cpp`, `WriteBufferFromS3.cpp`, `MutateTask.cpp`, `MergeTreeData.cpp` drop/detach, `DiskObjectStorage.cpp` startup); manifests; predictions committed (hold-outs first).
- [ ] **Step 2: Failing tests** — manifest validation; shape cards compute part type from the registry predicate and assert it from `system.parts`; `mutation.compact` asserts a full rewrite by object count; `cold-select` restarts before measuring; `startup` records seconds; `big-insert-multipart` assertion requires `CreateMultipartUpload == 1` and `UploadPart ≥ 2` (fixture tests in both directions).
- [ ] **Step 3–5: Run/implement/pass.**
- [ ] **Step 6: Live** — fit cards on `s3` and `s3_cached` (K=3); hold-outs after their predictions are committed.
- [ ] **Step 7: Reconcile; write `coefficients.json`, `formulas.json`, evidence edges (fit + holdout per coefficient; `bytes_per_get` hold-out = `cold-select.cache-off` on the `types-holdout` table; `metadata_*` hold-out = `wide-wide`).
- [ ] **Step 8: Codex review (source-to-graph audit). Step 9: closure check; commit** — `research: s3 disk operation graphs (R2)`.

### Task 3: R3 — `s3_plain_rewritable` graphs and capability matrix

**Files:**
- Create: `docs/superpowers/reports/2026-10-24-storage-cost-s3plain/` (convention)
- Modify: registry, coverage; `cards/*.py` `supports('s3plain')` reasons from the capability gate cited in source
- Test: `tests/test_cost_cards_s3plain.py`, `tests/live_s3plain.py`

**Interfaces:**
- `formulas.json` owner `s3plain`, variant `s3plain`: cells as `s3` plus `states: not_supported` for the operations the capability gate refuses (cited); coefficients `plain_meta_objects_per_part`, `plain_startup_requests(parts)`, `plain_rename_requests`, `plain_copy_on_hardlink` [pinned, source].
- Replication applicability for `s3plain` is **recorded here** (`coverage.cards[*].variants` for the replication cards) with the source reference; the runs happen in Task 5.

- [ ] **Step 1: Source → graphs and the replication question** (`MetadataStorageFromPlainRewritableObjectStorage*.cpp`, `MetadataStorageFromPlainObjectStorageOperations.cpp`); manifests (reuse Task 2 experiments with `policies: [s3plain]`); predictions committed.
- [ ] **Step 2: Tests** — `test_unsupported_ops_reported_with_cited_reason`.
- [ ] **Step 3–5.** **Step 6: Live** — Task 2 card set on `s3plain` and `s3plain_cached`; unsupported ones produce `UNSUPPORTED` records.
- [ ] **Step 7: Reconcile; artifacts; Step 8: review; Step 9: closure; commit** — `research: s3_plain_rewritable graphs and capability matrix (R3)`.

### Task 4: R4 — CAS graphs, branches, GC and lease

**Files:**
- Create: `utils/cas-yardstick/yardstick/cost/cards/cas.py` (`CasBranches`, `GcLifecycle`, `GcIdle`, `GcScale`, `Lease`), `docs/superpowers/reports/2026-10-24-storage-cost-cas/` (convention)
- Modify: registry, coverage
- Test: `tests/test_cost_cards_cas.py`, `tests/live_cas.py`

**Interfaces:**
- `formulas.json` owner `cas`, variant `cas`: all cells; `states: backlog_grows` with `cond` over measured reclaim vs retirement. Coefficients: `cas_key_families` [pinned: regex per family from the key layout, with categories], `cas_objects_per_part[shape][part_type]`, `cas_branch_population[shape][branch]` (fresh, hash_hit, adoption, inline), `cas_branch_requests[branch]`, `cas_publish_requests`, `cas_ref_requests_per_rename`, `cas_foreground_ref_removal_requests`, `gc_round_requests(objects, mounts, ref_growth)` (fitted terms with spans), `gc_round_duration(objects)`, `gc_heartbeat_requests_per_s`, `gc_scheduler_rule` [pinned: how round start relates to the interval and who pulses heartbeats], `lease_requests_per_s`, `reclaim_delay_s[stage]`, `staging_rule` [pinned: default backend and what it writes locally], `staging_peak_bytes_rule`.
- Experiments: `cas-branches.fresh` (insert new data), `cas-branches.hash-hit` (insert the identical data set twice; assertion on `system.cas_log` evidence fields the implementer identifies in source for hash hits, plus decoded manifest placement when kinds coincide), `cas-branches.adoption` (`ALTER TABLE … UPDATE u1 = u1 + 1 WHERE 1` on a wide part; assertion: adoption evidence count == columns − 1), `cas-branches.inline` (table of 5 `UInt8` columns, 100 rows: all entries below the inline threshold the source defines; assertion: zero blob PUTs), `gc-lifecycle.drop` and `gc-lifecycle.merge` (cohort frozen before the retiring action; stages from `cas_gc_log` event types; reclaim delay per stage), `gc-idle.populated` (10 000 objects, no changes, 3 intervals), `gc-scale.objects-{1k,10k,100k}` (P1; `gc-scale.objects-30k` is the hold-out for `gc_round_requests`, owned here), `gc-scale.mounts-2.10k` (P2; two replicas mounted, same objects), `gc-scale.refgrowth-{low,high}.10k` (P2; `low` = 1 000 part events between rounds, `high` = 20 000), `lease.idle-10min` (observation: renewals), `lease.idle-20min` (hold-out for `lease_requests_per_s`), `gc-under-load.default` (`small-inserts.default` with `gc: measure`; compares reclaimed vs retired; validation, owned here, run in Task 7).

- [ ] **Step 1: Source → graphs** (`ContentAddressed/ContentAddressedTransaction.cpp`, `Parts/*`, `Gc/CasGcScheduler.cpp`, `Gc/CasGc.cpp`, `Pool/*`, `ContentAddressedSettings.cpp`, key layout); manifests; predictions committed.
- [ ] **Step 2: Tests** — branch assertions on fixture `cas_log` rows (both directions); `gc-idle` separates heartbeat and round keys by family; `gc-scale` builds the three pool sizes deterministically; cohort freezing excludes control families.
- [ ] **Step 3–5.** **Step 6: Live** — `cas-branches.*`, `gc-lifecycle.*`, `gc-idle`, `lease`, `gc-scale` P1, and the Task 2 shape/ops cards on `cas` and `cas_cached`; P2 rows if within budget.
- [ ] **Step 7: Reconcile** (exact for branches and shapes; fitted GC terms with spans; delays with spread); evidence edges incl. hold-outs: `gc_round_requests` hold-out = `gc-scale.objects-30k` (new size), `lease_requests_per_s` hold-out = `lease.idle-20min`, `cas_branch_population` hold-out = `types-holdout` on `cas`.
- [ ] **Step 8: Review; Step 9: closure; commit** — `research: CAS graphs, branches, GC and lease (R4)`.

### Task 5: Replication, recovery and faults (all variants)

**Files:**
- Create: `utils/cas-yardstick/yardstick/cost/cards/replication.py`, `cards/faults.py`, `docs/superpowers/reports/2026-10-24-storage-cost-replication/` (convention)
- Modify: registry, coverage; the variant reports' `formulas.json` `recovery` cells reference the coefficients owned here
- Test: `tests/test_cost_cards_replication.py`, `tests/live_replication.py`

**Interfaces:**
- Coefficients (owner `replication`): `acquire_requests[variant][result_type]` at sender and receiver, `acquire_bytes[variant][result_type]`, `local_exec_requests[variant][result_type]`, `fetch_fraction[variant][result_type]` (natural split, K=3 spread), `zero_copy_requests[result_type]`, `catchup_requests_per_missed_part[variant]`, `startup_requests_after_restart[variant]`, `replacement_requests_per_part[variant]`, `retry_outcome_mix[class]`, `failed_upload_residue`, `fetch_retry_rate`.
- Experiments: `replication-fetch.{insert,merge,mutation}` (follower forced to fetch via the `acquisition_rule` settings from Task 1), `replication-local.{merge,mutation}` (forced local execution; assertions per result type from the follower's `system.part_log`: for `merge` an event `MergeParts` for the result part name and no `DownloadPart` for it; for `mutation` an event `MutatePart` for the result part and no `DownloadPart`; counters `ReplicatedPartMerges` / `ReplicatedPartMutations` as secondary evidence), `replication-natural-fit.default` and `replication-natural-holdout.wide` (defaults; observation: per-result-type split), `replication-zero-copy.{insert,merge,mutation}` (`s3`), `restart-catchup.10min` (stop r2; 10 min of `small-inserts` on r1 at scale 0.5; start r2; window around the restart until `complete`), `replacement.default` (P2; r3 joins a populated 20-part table; r1/r2 fetch deltas asserted 0), `fault-put-5xx` (rule `PutObject nth=3 status 500 send_upstream:false`), `fault-lost-response` (`PutObject nth=3 drop_response`), `fault-multipart-abort` (rule `UploadPart part_number=2 nth=* status 500 send_upstream:false`: the part can never succeed, so the server's own abort runs; assertions: the INSERT fails, `AbortMultipartUpload` by `r1` with `upstream.outcome='ok'`, incomplete inventory returns to 0 without rig action), `fault-fetch` (P2: relay stopped for 5 s during a fetch).

- [ ] **Step 1: Predictions** from Task 1's rule and the variant graphs; manifests; hold-out predictions committed first.
- [ ] **Step 2: Tests** — forced cards assert the acquisition path from `part_log`/counters (fixtures both directions); fault cards install and clear rules; `restart-catchup` stops/starts the right replica; `replacement` asserts idle replicas.
- [ ] **Step 3–5.** **Step 6: Live** — on `s3`, `s3plain` (per Task 3 applicability), `cas`, `local`; 2 replicas; `replacement` with 3 (P2).
- [ ] **Step 7: Reconcile; artifacts; Step 8: review; Step 9: closure; commit** — `research: replication, recovery and fault graphs`.

### Task 6: R1 — EBS demand and conversion

**Files:**
- Create: `utils/cas-yardstick/yardstick/cost/cards/io_sizes.py` (`IoSizes`: runs a manifest with `trace: true` — the Plan 1 `Tracer` with `strace -f -ttt -yy -e trace=write,pwrite64,writev,read,pread64,readv,fsync,fdatasync` — at scale 0.05 and histograms request sizes per syscall and per path), `docs/superpowers/reports/2026-10-24-storage-cost-ebs/` (convention) **including `formulas.json` owner `ebs`, variant `ebs`, all eight cells** (`storage` = provisioned capacity rows per §4.4, `inserts`/`merges`/`mutations`/`deletes`/`reads`/`recovery` = `block_io` rows from `io_bytes_per_op` and `io_request_size_distribution` converted by `ebs_io_rule`, `local_disk` = none) and the EBS `applicability` entries
- Modify: registry (`io_bytes_per_op[op]` measured via `proc_io`, `io_request_size_distribution[op]`, `ebs_io_rule` [provider: SSD max I/O size and sequential-merge rule, URL + date], `peak_factor` measured as 1-s peak / mean of `write_bytes` rate during `merge-natural`), coverage, billing contract (gp3 bounds with URL + date, if not yet recorded by Plan 1 Task 10)
- Test: `tests/test_cost_cards_io_sizes.py`, `tests/live_ebs.py`

- [ ] **Step 1: Measurement boundary** in the report: process-attributed bytes (`/proc/<pid>/io`, includes embedded Keeper on r1 — quantified by a Keeper-only idle card `io-sizes.idle`), host diskstats caveat, `strace` sizes at small scale, kernel cache state uncontrolled (no root) → observed device reads are not cold-read demand; the limitation propagates to the registry entries (`validity` and `evidence.ref`) and to the page's evidence statement.
- [ ] **Step 2: Tests** — strace parser on a fixture; peak factor from a synthetic series; idle card subtracts Keeper baseline.
- [ ] **Step 3–5.** **Step 6: Live** — shape, lifecycle, mutation, drop, cold-select, startup and replication cards on `local` (1 and 2 replicas); `io-sizes.*` at scale 0.05; `io-sizes.idle`.
- [ ] **Step 7: Derive** the EBS conversion with the dated rule (SSD only in Tier 1); write the eight EBS cells as nodes; coefficients, evidence edges (hold-out for `io_request_size_distribution` = `types-holdout` on `local`; for `peak_factor` = `merge-natural.wide`); **Step 8: review; Step 9: closure; commit** — `research: EBS demand, conversion and operation graphs (R1)`.

### Task 7: Hold-outs, per-variant validation and artifact consolidation

**Files:**
- Modify: every report directory (hold-out sections, `coverage.evidence` statuses), registry
- Create: `docs/superpowers/reports/2026-10-24-storage-cost-validation/` (convention; `README.md` = the validation report)

- [ ] **Step 1: Integrated validation** — `amplification-run.2h` (Task 1's card) and `gc-under-load.default` (Task 4's card) if within budget; natural lifecycle validation (`merge-natural.default`, `replication-natural-fit.default`) on every remote variant; replacement hold-outs for every edge marked `consumed` by Tasks 1–6.
- [ ] **Step 2: Record gaps** per coefficient; consumed hold-outs replaced and rerun.
- [ ] **Step 3: Per-variant validation** of amplification and acquisition split; differences stay in the report.
- [ ] **Step 4: Evidence closure** — `closure_check.py` passes: every P1 measured coefficient has `fit` + distinct `holdout` edges `done`; cut edges marked; dependent registry quantities `unsupported`.
- [ ] **Step 5: Codex review of the validation report, registry and coverage** → close MAJORs.
- [ ] **Step 6: LEDGER.md** — research done, open items; commit — `research: hold-outs and validation`.
