---
description: 'Design of the ClickHouse storage cost calculator: a measured, code-backed cost model for EBS, s3, s3_plain_rewritable and CAS disks, a research programme that derives and verifies each formula on RustFS, one consolidated formula document, and a single-page HTML+JS calculator for product managers and the Altinity blog.'
sidebar_label: 'Storage cost calculator design'
sidebar_position: 50
slug: /superpowers/specs/storage-cost-calculator-design
title: 'Storage cost calculator — design'
doc_type: 'reference'
---

# Storage cost calculator — design {#design}

Decision: build a storage cost calculator for ClickHouse that compares four ways to keep MergeTree data on AWS-class
infrastructure — local EBS volumes, the `s3` disk (metadata local), the `s3_plain_rewritable` disk (metadata on S3) and the
CAS disk (one copy of the data shared by all replicas) — with every formula derived from the source, verified on RustFS and
reviewed by Codex against the code. The calculator is a single page (HTML + JavaScript, no build step) that recomputes on
every change and lets the reader open any number down to the formula with its inputs substituted. Decided on 2026-10-23.

## 1. Intent {#intent}

Two readers, one tool.

- **A product manager** who does not know what storage will cost them, or how much a move from EBS to `s3` or CAS would
  save, and who has to decide "stay on EBS", "move to `s3`", "move to CAS". They know their data in round numbers (TB, GB
  ingested per day, days of retention, number of replicas); some have a running cluster and can get exact numbers when told
  how.
- **An Altinity blog article** that shows where CAS wins and by how much. The article is credible only if every number
  can be defended with a pointer to code and to a measurement, so the calculator doubles as the article's evidence.

Success: a sceptical reader who checks a line against their real AWS bill, or against the code, finds the model within the
stated error, and finds the EBS and `s3` columns as carefully modelled as the CAS one.

## 2. Goals, non-goals, anti-goals {#goals}

**Goals**

- One monthly cost per variant, broken down by workload type (storage, inserts, merges, mutations, deletes/GC, cold reads,
  local disk) and, inside each cell, by billable operation (bytes stored, PUT-class requests, GET-class requests, LIST,
  DELETE, network traffic, retries).
- Every cell opens to its formula with the current inputs substituted, and names the source: the section of the model
  document, the measurement report, the code.
- Inputs in the product manager's language on top; parts, blobs, columns, merge settings, GC period in collapsible
  advanced blocks with defaults derived from the simple inputs; a "how to get exact numbers" hint with SQL for each
  advanced block.
- Price presets per provider (AWS first, GCS second, Azure and generic S3-compatible later) and scenario presets per
  workload; every unit price editable.
- Operational notes next to the numbers: whether gp3 base IOPS/throughput suffice, which EC2 class the EBS bandwidth
  needs, how much local disk the `s3` and CAS variants need for metadata and what IOPS it sees.
- Immediate feedback: no wizard, no submit button.

**Non-goals**

- Compute (EC2) cost. Notes only: instance class implied by EBS bandwidth, local disk for metadata.
- Keeper/ZooKeeper cost, cross-region replication, backup cost.
- Query performance. The calculator says what storage costs, not how fast it reads.
- Variants outside the four named. Zero-copy replication on `s3` is an advanced toggle inside the `s3` column, not a fifth
  variant (§4.3).

**Anti-goals** (a working calculator that does these has failed)

- A model that flatters CAS: an `s3` or EBS column modelled less carefully than the CAS column, or a default that is
  realistic for CAS and pessimistic for the others.
- Numbers off from reality by a factor, or formulas that cannot be traced to code and measurement.
- Hiding merges inside inserts. For small, frequent inserts merge write amplification dominates the request bill, so
  merges are a block of their own with their own inputs.

## 3. Constraints {#constraints}

- **Ground truth binary:** antalya-26.8 with the CAS rework, Altinity PR 2647 (`feature/antalya-26.8/cas-disk-transactions`),
  build https://altinity-build-artifacts.s3.amazonaws.com/json.html?PR=2647&sha=09d0772a842c5d73cf3ef35b4c8b017d838215a0&name_0=PR.
  All four variants are measured on this one binary so they share the MergeTree code and settings. Commit
  `09d0772a842c5d73cf3ef35b4c8b017d838215a0` is in this repository; formula derivation reads the source at that exact SHA
  (a detached worktree without submodules is enough), never the 26.6 tree, and every coefficient records the binary sha256,
  the commit and the settings it was measured with.
- **Cross-check binary (CAS only):** antalya-26.6 with the older transactions and some optimisations not yet ported,
  Altinity PR 2644, https://altinity-build-artifacts.s3.amazonaws.com/PRs/2644/97ac10160b56ca5381604363989b2017fd6ea28c/build_amd_binary/clickhouse.
  Where 26.6 issues fewer requests than 26.8, the model gets a flag `optimisation <name> ported` with both values, and the
  report lists what remains to port.
- **Measurement store:** RustFS 1.0.0-rc.3, as used by the CAS CI lane (`ci/jobs/scripts/clickhouse_proc.py`,
  `start_rustfs`); a cached binary is at `~/workspace/ClickHouse/master/ci/tmp/rustfs` (copy it into the worktree's
  `ci/tmp/`, or let `download_rustfs` fetch it). Both ClickHouse binaries answered HTTP 200 on 2026-10-23.
- **Measurement rig:** extend `utils/cas-yardstick` (design `docs/superpowers/specs/2026-09-29-cas-yardstick-design.md`)
  rather than write a new rig. It already runs one card on `cas`, `s3` and `s3_plain` against one RustFS, counts requests by
  kind from `system.events` deltas, separates cold and hot runs and repeats K times.
- **Review:** every formula gets one Codex review against the code before it enters the consolidated document. Codex is
  installed (`~/.local/bin/codex`), model `gpt-6-astra`, prompt passed via file; the 2026-09-29 yardstick review used the same
  setup.
- **Measured first, forecast second.** The CAS column defaults to the measured 26.8 binary. A "next release (forecast)"
  toggle on the CAS card flips the flags for the 26.6 optimisations still to be ported; the card is then labelled
  *forecast* and lists the assumptions (which optimisations, their measured effect on 26.6, and that their interaction on
  26.8 is not measured). The blog may quote the forecast only with that label. CAS knobs (format generation 2, placement
  thresholds, inlining, packs) take the branch defaults unless a scenario preset says otherwise.
- **Scale:** measurements run at test scale (thousands of parts, GBs); the model is analytic in part count and bytes, and
  each formula states its validity range. Merge write amplification is additionally checked by one scaled-down long run
  (§5.3, `amplification-run`). The page promises per-term accuracy (request counts within 5 %, bytes within 10 % of the
  measured scenarios), not an end-to-end bill accuracy, and says so under the table; the merge block tells the reader to
  override amplification with their own `system.part_log` numbers when they have them.
- **Language:** calculator UI, formula document and reports in English.
- **Scope of cost:** storage and requests to storage only (§2).
- **Where it lives:** a new branch in this repository, worked on in a new worktree (§9).
- **Prose:** repo rules for docs (frontmatter, `{#anchors}` on every heading, inline code for SQL and class names).

## 4. The cost model {#model}

### 4.1 Neutral units {#units}

Formulas produce quantities in provider-neutral units; a price preset turns them into money. Nothing in a formula names a
provider.

Formulas do not aggregate straight into billable units. They first produce a **request ledger**: per API method (PUT, POST,
COPY, GET, HEAD, LIST, DELETE, multipart initiate/part/complete/abort), the number of attempts, the HTTP status class
(2xx, 4xx, 5xx, throttled), the number of keys for multi-key deletes and the number of pages and keys per LIST. The price
preset carries the provider's billing rules that map the ledger onto the units below (AWS: errors other than 403/404 are
billed; `DeleteObjects` is one request; LIST is billed per page; GCS: class A/B). `system.events` counters are not that
ledger: the `S3*RequestsCount`/`S3*RequestsErrors` family counts attempts and errors at the HTTP layer while `S3PutObject`
and friends count API calls, so the yardstick reads both, never sums overlapping families (`S3*` and `DiskS3*` increment for
the same call), and takes the authoritative ledger from the store side (§5.4).

Billing scope: unit prices with tiers (storage tiers, free request allowances) are applied once per variant at the bucket
or account scope the provider bills at, and the resulting money is allocated back to workload cells in proportion to their
share of that unit; "show calculation" shows the allocation. Bucket assumptions are stated in the price preset: storage
class (S3 Standard), versioning off, a lifecycle rule that aborts incomplete multipart uploads after N days, no KMS, no
VPC-endpoint or NAT charges. A month is 730 hours; sizes are GiB/TiB in the model and converted to the provider's GB where
its price list uses decimal units.

| Unit | Meaning |
|---|---|
| `GB_month_object` | bytes kept in object storage, averaged over the month |
| `req_put` | PUT, POST, COPY, multipart initiate/complete, conditional PUT |
| `req_get` | GET, HEAD |
| `req_list` | LIST (billed at the PUT tier on AWS, separate unit so GCS/Azure can differ) |
| `req_delete` | DELETE API calls (a `DeleteObjects` call with N keys is one call); the ledger keeps the key count separately |
| `GB_egress` | bytes leaving the provider region (default price 0) |
| `GB_cross_az` | bytes crossing availability zones inside the region (replica-to-replica fetches) |
| `GB_month_block` | provisioned block storage |
| `iops_month` | provisioned IOPS above the volume's free baseline |
| `mbps_month` | provisioned throughput (MB/s) above the free baseline |

### 4.2 Inputs {#inputs}

Simple inputs, always visible:

| Input | Default | Notes |
|---|---|---|
| Data size | 20 TB | logical, after compression, one copy |
| Ingest rate | 500 GB/day | unit selector: per second, minute, hour, day, month |
| Retention | 40 days | data size and ingest rate × retention must agree; the page shows the implied third value and lets the reader pick which two are inputs |
| Replicas | 3 | 1–6 |
| Scenario preset | "Logs" | fills the advanced blocks |
| Price preset | "AWS us-east-1 (2026-10)" | fills the unit prices |

Advanced blocks, collapsed, each with a one-line summary of its current values and a "how to get exact numbers" hint:

| Block | Inputs | Hint query |
|---|---|---|
| Schema | tables, columns per table, rows per insert, bytes per insert, compact/wide threshold, columns with marks/indexes/projections | `system.parts`, `system.columns`, `system.tables` |
| Inserts | inserts per unit of time, parts per insert (partitions touched), async insert on/off | `system.part_log` where `event_type='NewPart'` |
| Merges | write amplification (derived from insert size, retention, `max_bytes_to_merge_at_max_space_in_pool`, part count targets; overridable), merged bytes per day, merged parts per day | `system.part_log` where `event_type='MergeParts'` |
| Mutations and deletes | mutations per day and bytes rewritten, lightweight deletes, parts dropped per day (TTL, `DROP PARTITION`), bytes dropped | `system.part_log` where `event_type in ('MutatePart','RemovePart')`, `system.mutations` |
| Reads | bytes read by queries per day, cache hit ratio (an independent input, not derived from cache size), average bytes per GET for misses (measured default), local cache size (feeds the local-disk note only) | `system.events` (`S3GetObject`, `CachedReadBufferReadFromCacheBytes` vs `...FromSourceBytes`) |
| Topology | replicas in distinct availability zones (yes/no); restarts per replica per month with local state intact; replica replacements per month (full catch-up: fetch everything on EBS/`s3`, relink on CAS) | `system.events` (`ReplicatedPartFetches`), uptime history |
| EBS | volume type (gp3 default; io2, st1), provisioned IOPS, provisioned MB/s, headroom factor | `iostat`, `system.asynchronous_metrics` (`BlockReadOps_*`, `BlockWriteOps_*`) |
| CAS | GC round period, lease renewal period, number of pool members/mounts, hot/cold placement thresholds as the branch exposes them | `system.cas_gc_log`, `system.cas_log` |
| Reliability | retried-request fraction by operation class (default 0.5 %), failed-upload fraction and the uploaded fraction before failure, incomplete-multipart lifetime (lifecycle rule days; uploaded parts are billed as storage until aborted), whole-part fetch retries | `system.events` (`S3WriteRequestsErrors`, `S3ReadRequestsErrors`, `S3AbortMultipartUpload`, `ReplicatedPartFailedFetches`) |
| Prices | one editable row per neutral unit, tiered storage prices where the provider has them | — |

Input contract (shown in the UI next to the fields): all sizes are compressed, on-disk bytes as ClickHouse stores them
(`bytes_on_disk`), in GiB/TiB, with the provider's GB↔GiB conversion applied in the price layer; data size and ingest are
for one copy across the whole cluster (shards sum, replicas do not); cold-read bytes are per cluster, before any cache,
and the cache hit ratio is applied exactly once; a month is 730 hours; the identity data = ingest × retention assumes a
steady state (constant ingest, retention-based deletion) and the page says so when it fills the third value.

Derived quantities (parts created per day, live parts, average part size, objects per part, blobs per part on CAS, multipart
parts per object, merge write amplification, local-merge vs fetch split) are **editable assumptions**, pre-filled by the
scenario preset from the simple inputs and shown with their valid range and the rule that produced them; they are not
presented as deductions. Where the rule needs facts the simple inputs do not carry (uncompressed bytes and rows for the
compact/wide choice, partitions touched per insert, async-insert flush thresholds and arrival pattern, partition lifetimes
for merge amplification), the preset states the assumption and the field says so.

### 4.3 Per-variant models {#variants}

Each variant is a function from inputs to a table `workload type × neutral unit`. What each must cover; the coefficients
and the exact operation graphs come from the research programme (§5) and live in the consolidated document (§6).

**Common to all variants**

- **Stored bytes are physical byte-hours, not logical data × replicas.** The inventory per replica (or per pool for CAS)
  is: active parts; outdated parts between their replacement and their removal (`old_parts_lifetime`, extended while
  readers hold them); detached parts (an input, default 0); temporary and staging data; incomplete multipart uploads;
  the storage system's own metadata. `system.*_log` tables are assumed on a local disk and excluded. Objects shared by
  hardlink or by hash are counted once until their last reference disappears.
- **Replication is three rates, not one multiplier.** Per part type (insert result, merge result, mutation result) and per
  replica: fetched from a peer, produced locally (local merge/mutation, decided by `always_fetch_merged_part`,
  `prefer_fetch_merged_part_size_threshold`, `prefer_fetch_merged_part_time_threshold` in `MergeFromLogEntryTask::prepare`
  and `MutateFromLogEntryTask::prepare`), or relinked (CAS). Plus recovery traffic: restarts with intact state (catch-up of
  what was missed) and replica replacement (everything), at the rates from the Topology block, and failed fetch attempts.
- **Mutation reuse is not CAS-specific.** On EBS and on the `s3` disk unchanged columns of a mutated part are hardlinked,
  not rewritten; the model's per-operation capability/reuse matrix (which bytes each variant rewrites, copies, hardlinks or
  adopts by hash, for insert, merge, mutation, lightweight delete, `DROP`, TTL, detach/attach) is a research output, and CAS
  gets credit only for reuse the other variants do not have.
- **Physical layout is measured, not computed from column count.** The wide writer creates one stream per serialization
  substream (`Nullable`, `Array`, `LowCardinality`, `Map`, `JSON`, sparse), projections are sub-parts with their own
  files, skip indexes depend on type. The model carries objects-per-part and object-size distributions measured per
  scenario shape, with column count as one parameter among others.

**EBS (local disk × replicas)**

- Measured on a dedicated filesystem on a device that carries nothing else (not RustFS, Keeper, caches or system logs);
  if the workstation cannot provide one, the EBS results are labelled host-specific. The measured quantity is the demand
  at the ClickHouse level: bytes written and read and the number and size of I/O requests per insert, merge, mutation and
  read, with the page cache and writeback boundaries stated (`SYSTEM DROP ... CACHE`, `sync`, direct I/O where
  applicable). Workstation `iostat` counts are not EBS I/O counts: the model converts bytes and request sizes into EBS I/O
  operations with the published AWS accounting rules (gp3 counts 16 KiB units, HDD types 1 MiB units) and states that
  conversion in the formula. No EC2 run in this round; the EBS column says that its IOPS figures are derived.
- `GB_month_block` = provisioned capacity: physical peak bytes (active + outdated + temporary + merge headroom) × replicas
  × (1 + free-space headroom), with volume count, size, IOPS and throughput limits taken from the provider's current
  limits at build time, recorded with an `as_of` date (the reviewers of this design noted gp3 at 64 TiB, 80 000 IOPS and
  2 000 MiB/s per volume in 2026; 16 TiB applies to Outposts). gp3, io2 and st1 keep their own baseline and price rules.
- IOPS and MB/s demand per replica as a **peak**, not a daily mean: a peak factor (default from measured merge/insert
  concurrency, overridable) multiplies the mean demand, because merges and queries overlap. The peak is compared against
  the volume baseline; the excess becomes `iops_month` / `mbps_month`, and the note says whether the baseline suffices and
  which EC2 class carries the EBS bandwidth.
- `GB_cross_az` from the replication rates above. No request costs. Reads cost nothing beyond IOPS and MB/s; no page-cache
  assumption is made for EBS, and the read inputs feed only the IOPS demand there.

**`s3` disk (metadata local × replicas)**

- `GB_month_object` = physical byte-hours per replica (common model) × replicas, plus incomplete multipart residue.
- Per part written (insert, merge, mutation): the measured objects-per-part distribution; multipart thresholds
  (`s3_min_upload_part_size`, `s3_max_single_part_upload_size`) and the parts per object they imply; the checks the disk
  transaction issues before/after upload (`HEAD`/`GET`); so many `req_put`, `req_get`, `req_list`, read from the operation
  graph R2 establishes from the 26.8 source.
- Replication per the common three-rate model; a peer fetch costs the sender `req_get` and bytes from its own S3 and the
  receiver its own `req_put`; a local re-merge costs the replica its own merge reads and writes.
- Merge: reads source parts (`req_get`, bytes, through the cache when present), writes the merged part, deletes the
  sources after `old_parts_lifetime` (`req_delete`).
- Mutation: rewrites the affected columns, hardlinks the rest (capability/reuse matrix).
- Deletes: `req_delete` per API call, `DROP`/TTL paths, outdated-part removal delay feeding the byte-hours.
- Reads: `req_get` = miss bytes / measured bytes-per-GET for the scenario shape; cache fill and eviction churn count as
  local disk I/O.
- Startup and recovery: restart with intact local metadata (what the disk reads from S3 at start, measured from process
  launch to readiness), and replacement (full fetch), at the Topology rates.
- Local disk: metadata size (one small file per object) and IOPS from metadata writes, plus the cache disk. A filesystem
  cache is on by default for `s3`, `s3_plain_rewritable` and CAS; its size is an advanced input that feeds the local-disk
  note; the cold-read model takes the hit ratio from the Reads block.
- Zero-copy replication (`allow_remote_fs_zero_copy_replication`): an advanced toggle, off by default, with a note on its
  upstream status. When on, `GB_month_object` is one physical inventory and peers do not re-upload; what they still pay for
  inserts, merges and mutations is measured by R2 in the `replication-zero-copy` scenarios. The toggle exists so the `s3`
  column is not made to look worse than its best configuration.

**`s3_plain_rewritable` (metadata on S3 × replicas)**

- Operations the disk refuses (mutations, lightweight deletes; the yardstick design records error 344 from the disk's
  capability gate) make the column **not comparable** for that scenario: the card shows "not supported for this workload"
  instead of a monthly total, and the column is excluded from the savings deltas and from the "cheapest" mark. A column
  never gets a smaller total by doing less work. Whether `ReplicatedMergeTree` on this disk is a valid deployment at all is
  R3's first question; if it is not, the column is "not supported" whenever replicas > 1.
- As `s3`, plus the metadata objects the disk writes under its `__meta` prefix, the startup load (it lists metadata and
  directory contents; the plain `s3` disk does not list the bucket at start), the extra requests on rename/commit, and
  COPY where its hardlink implementation uses it.

**CAS (one copy × GC)**

- `GB_month_object` = one physical inventory for the pool, split into disjoint key families so nothing is counted twice:
  payload (blobs, inline data inside manifests, packs), per-part metadata (manifests, trees if the 26.8 format has them),
  ref ledger and its retained generations, GC state (snapshots, markers, outcomes), mount/owner/catalog state, staging
  debris; each family's byte-hours reconciled against the bucket inventory (a LIST of RustFS after each card), not against
  `system.parts`.
- Garbage: `garbage_bytes = retired_unique_bytes_per_second × mean_reclaim_delay_seconds`, where retired bytes are every
  byte no live manifest references (superseded merge and mutation inputs, drops, abandoned writes), and the reclaim delay
  is measured end to end on 26.8 from the last reference's removal through outdated-part retention, condemnation, grace
  rounds, budgets and confirmed deletion, including backlog under load. One post-drop round does not validate reclamation;
  the `gc-lifecycle` scenario follows inventory convergence.
- Per part written: the **operation graph** R4 establishes from the 26.8 source, with a branch per case: fresh blob (the
  `HEAD`-before-`PUT` protocol step, the blob `PUT`), hash hit, trusted adoption of an unchanged hardlinked entry, inline
  entry, staging (local by default on 26.6 via `staging_backend`; to re-check on 26.8; S3 staging adds temporary objects,
  promotion COPYs and cleanup), publication (manifest CAS `PUT`), ref-ledger writes on rename/commit (format generation 2).
  No budget from the 26.6 description is carried over unverified.
- Replication: a compatible same-pool peer relinks instead of fetching bulk data; it still transfers the manifest with
  its inline data and pays control requests. R4 measures relink cost for insert, merge and mutation results separately,
  and what a replica pays when it re-merges locally on a CAS disk instead of relinking (same `prefer_fetch_*` decision).
- Merge and mutation: as `s3` minus the reuse the capability/reuse matrix grants CAS beyond hardlinks (measured, not
  assumed; the model carries a dedup fraction per scenario with its measured default).
- Deletes and GC: foreground ref removal in `ContentAddressedTransaction` costs requests on the data path; the GC round
  has separate terms for idle/election work over a populated pool, ref folding, live-state scanning, retained-generation
  cleanup and reclamation, each parameterised by pool population (objects, manifests), mounts, ref-log growth and
  pagination. R4 measures populated idle pools and loaded pools at no fewer than three pool sizes and fits each term with
  its range. GC cost per month = Σ terms × rounds per month at the configured period.
- Lease: renewal requests per period per mount per replica, measured on an idle mount.
- Local disk: metadata, the cache disk, and the local staging area (peak space for concurrent insert/merge/mutation
  outputs, its reads and writes), reported in the local-disk note.

### 4.4 Output {#output}

- Four summary cards (one per variant) with the monthly total, delta against EBS and against `s3`, and a stacked bar of the
  composition by neutral unit class (bytes, traffic, PUT-class, GET-class).
- A table `workload type × variant`: storage, inserts, merges, mutations, deletes/GC, cold reads, local disk, total. EBS
  cells that do not apply show a dash, not zero.
- Clicking (or hovering) a cell opens its composition by operation (PUT, GET/HEAD, LIST, DELETE, traffic, retries) with
  amounts and bars; "show calculation" under it prints the formula with every input substituted and the result, then the
  source line: model section, measurement report, code reference.
- Notes under the table: EBS volume count and instance class; local disk size and IOPS for `s3` and CAS; anything the
  model marks as unsupported or unmeasured for the chosen inputs.
- Sensitivity view (my call): one chart of monthly total per variant against one swept input (replicas, insert size,
  retention, cold-read fraction), with break-even points marked. This is what the blog article needs: the regions where
  each variant is cheapest, including the one-replica, low-request and large-batch regions where CAS is not.
- Accuracy statement under the table: per-term tolerances from the hold-out scenarios, the "derived, not measured on EBS"
  note, and the forecast label when it applies.
- URL state (my call): all inputs encoded in the URL hash, together with the model version, coefficients version and price
  preset `as_of`, so a scenario linked from the article reproduces the same number later or says what changed.

## 5. Research programme {#research}

### 5.1 Phases and workstreams {#workstreams}

The programme runs in five phases; the five workstreams write their reports in parallel inside phases 2–3, but no formula
is final before phase 3 has run its hold-outs and phase 4 its review.

| Phase | What | Output |
|---|---|---|
| 0. Setup | binaries, source at the measured SHA, RustFS behind a store-side request ledger, the yardstick extensions (two-replica mode, `local` policy, barriers, inventory snapshots), counter semantics verified against `ProfileEvents.cpp` at the SHA, pinned server and disk configs, workload definitions and schema/file inventories per scenario | `tmp/bin`, yardstick commit, `measurement-contract.md` |
| 1. Lifecycle (R5) | part lifecycle on one disk type: parts per insert, part type by thresholds, merge selection and amplification, mutation bytes, `old_parts_lifetime`, local-merge vs fetch split; the quantities R1–R4 consume | R5 report v1 with measured distributions |
| 2. Calibration (R1–R4) | per-operation request, byte and I/O cost on each variant, isolated (merges stopped, GC explicit), exact integer reconciliation | per-variant operation graphs and coefficients |
| 3. Integrated validation | hold-out scenarios, two-replica lifecycle, GC under load, startup/recovery, long amplification run; stochastic terms get absolute + relative tolerances from observed variation | hold-out gaps, final coefficients |
| 4. Review and consolidation | Codex review per workstream, dispositions, consolidated document | §6 document, `model-contract.json` |

| # | Workstream | Derives | Measures |
|---|---|---|---|
| R1 | EBS | bytes, I/O request count and size per insert/merge/mutation/read as functions of the physical layout; conversion to EBS accounting units; peak factor; capacity constraints | dedicated-device block counters around each card |
| R2 | `s3` disk | operation graph per part write from the 26.8 source; multipart; transaction checks; replication send/receive; delete paths; startup; local metadata footprint; zero-copy | yardstick cards on `s3`, store-side ledger, local metadata dir size and IOPS |
| R3 | `s3_plain_rewritable` | as R2 plus `__meta` objects, startup load, rename/commit extras, COPY on hardlink; unsupported operations and replication validity | yardstick cards on `s3_plain` |
| R4 | CAS | operation graph per case, relink cost per part type, dedup beyond hardlinks, physical key families, reclaim delay, GC terms vs pool size, lease, staging; 26.6 vs 26.8 delta | yardstick cards on `cas` with both binaries; GC lifecycle to inventory convergence; `system.cas_gc_log` |
| R5 | Lifecycle (shared) | part type selection (uncompressed bytes, rows, level), parts per insert, async-insert flushing, natural merge selection and amplification, mutation bytes, removal timing, local-merge vs fetch split | `system.part_log` under insert scenarios with natural merges; long run |

### 5.2 Method per workstream {#method}

1. **Code → formula.** Read the code path at the measured SHA and write the operation graph and formula with named
   parameters and a reference per term (file, class, setting). State what is a constant of the code, what is a setting
   default, and what depends on the data.
2. **Formula → prediction.** Predict request counts by method, bytes and (for EBS) I/O for each scenario before measuring.
3. **Measure.** Run the scenario through the yardstick; collect the store-side ledger, `system.events` deltas,
   `system.blob_storage_log`, the bucket inventory before and after, and for EBS the block-device counters.
4. **Reconcile.** Isolated deterministic paths reconcile to the exact integer: a missing request is a formula defect, not
   tolerance. Stochastic terms (merge selection, GC under load, cache behaviour) get an absolute + relative tolerance taken
   from the observed variation over K runs, and the report states both. Background activity is separated before comparing
   (merges stopped in cards that do not measure them, system logs on a local disk, GC run explicitly); what still leaks in
   is measured by an empty card and subtracted. An unexplained gap stays in the report as an open item; the formula does not
   get a fudge factor.
5. **Codex review.** One read-only Codex round (model `gpt-6-astra`, reasoning high, prompt via file) asking whether the
   formula models the code at the measured SHA, which terms are wrong or missing, and whether the measurement attributes
   requests to the step it claims. Findings are fixed or answered in the report.
6. **Report.** `docs/superpowers/reports/2026-10-<dd>-storage-cost-<variant>.md`: operation graphs and formulas,
   predictions vs measurements per scenario (table), gaps and causes, Codex findings and dispositions, coefficients with
   provenance (binary sha256, commit, settings), and the exact commands to re-run. Raw measurements go next to the report
   as `measurements.json` (one record per scenario × policy × run × server with the request ledger, bytes, inventory
   deltas, wall time), so the page tests (§7) and the formula document read numbers, not prose.

### 5.3 Scenario matrix {#scenarios}

Each scenario asserts the shape it claims (part type from `system.parts`, object sizes from the inventory) before its
numbers count. Scenarios marked **hold-out** are not used to fit a formula: the formula is fixed on the others, then
predicted and measured on the hold-outs, and the hold-out gap is the accuracy the report states. Every formula records
the range of the parameters it was measured over; the page shows a warning outside that range. K=3 runs per scenario,
cold and hot where the distinction applies.

| Scenario | Shape | Role |
|---|---|---|
| narrow-compact | 5 columns, inserts below `min_bytes_for_wide_part` | fit |
| narrow-wide | 5 columns, inserts well above the wide threshold | fit |
| wide-compact | 100 columns, compact | fit |
| wide-wide | 100 columns, wide | hold-out |
| types | 20 columns mixing `Nullable`, `Array`, `LowCardinality`, `Map`, `JSON`; one projection; one skip index | hold-out |
| boundaries | part sizes just below and above `min_bytes_for_wide_part` / `min_rows_for_wide_part`; objects just below and above the multipart threshold | hold-out |
| small-inserts | 5 columns, 1k rows, 1000 inserts, natural merges allowed | fit (R5) |
| async-inserts | as small-inserts with `async_insert=1`, two flush settings | fit (R5) |
| big-insert-multipart | 20 columns, largest column above the multipart threshold | fit |
| merge-natural | insert N parts, let the merge selector work, measure merge requests, bytes and amplification | fit (R5) |
| merge-forced | `OPTIMIZE FINAL` as a controlled single merge, for the per-merge operation graph only | fit |
| mutation | `ALTER UPDATE` one column of a wide part; `ALTER DELETE`; lightweight delete; capability/reuse per variant | fit |
| drop | `DROP PARTITION`, TTL expiry, `DROP TABLE SYNC`; removal timing | fit |
| cold-select | restart, drop caches, `SELECT` touching N columns of M parts; bytes per GET; with and without cache | fit |
| startup | process launch to readiness, measured from outside the server; intact local state vs lost local state | fit |
| replication | two replicas; insert, natural merge, mutation on one; receiver fetched or re-merged; sender and receiver accounted | hold-out |
| replication-zero-copy | as `replication` with `allow_remote_fs_zero_copy_replication=1` on the `s3` disk | fit |
| replacement | add a third replica to a populated table: full catch-up on EBS/`s3`, relink on CAS | fit |
| amplification-run | 5 columns, 1k-row inserts every second for 2–4 hours; observed amplification vs R5 prediction; extrapolation to 40 days stated with its bounds, not claimed | validation (R5) |
| gc-lifecycle (CAS) | after `drop` and after `merge-natural`, follow GC rounds until the inventory converges; reclaim delay per stage | fit |
| gc-scale (CAS) | idle and loaded round cost at three pool sizes (1k, 10k, 100k objects) | fit |
| lease (CAS) | idle mount for one hour, count requests | fit |

### 5.4 Shared setup {#setup}

- Both binaries in the worktree's `tmp/bin/{26.8,26.6}/clickhouse` with sha256 recorded; the 26.8 source checked out at
  `09d0772a842c5d73cf3ef35b4c8b017d838215a0` in a detached worktree (no submodules) for derivation and for Codex.
- RustFS from `~/workspace/ClickHouse/master/ci/tmp/rustfs` (or `download_rustfs`), behind a **store-side request
  ledger**: a logging reverse proxy (builder's choice of tool) that records method, key, status, attempt, request and
  response bytes per request. This ledger is the authoritative request count; ClickHouse counters are mapped to it and
  the mapping is part of the measurement contract. One bucket per variant per run; distinct prefixes per replica for the
  non-shared disks, the shared pool for CAS.
- Yardstick extensions (the `--binary` option already exists): a `local` policy with block-device sampling on a dedicated
  device; two-replica mode (two server processes on one host, embedded Keeper in the first, `ReplicatedMergeTree`,
  per-server counters, sender/receiver accounting); **completion barriers** before every snapshot (`SYSTEM SYNC REPLICA`,
  empty replication queue, expected part and row counts on both replicas, `SYSTEM FLUSH LOGS`, GC run to completion
  via `SYSTEM CAS GC RUN` and `system.cas_gc_log`); bucket inventory snapshot per card; externally timed startup cards.
  Yardstick changes are part of the branch and get their own tests as the yardstick design requires.
- Request counting names (`S3PutObject`, `S3GetObject`, `S3HeadObject`, `S3ListObjects`, `S3DeleteObjects`,
  `S3CreateMultipartUpload`, `S3UploadPart`, `S3CompleteMultipartUpload`, `S3AbortMultipartUpload`, `S3CopyObject`,
  `S3ReadRequestsCount/Errors`, `S3WriteRequestsCount/Errors`, `DiskS3*`, `CAS*`) verified against
  `src/Common/ProfileEvents.cpp` at the measured SHA, with the overlap between families written down so none is summed
  twice; `system.blob_storage_log` for per-key attribution where it has the event (it lacks HEAD/LIST/COPY on 26.6).

## 6. Consolidated formula document {#formula-doc}

`docs/superpowers/cost-model/storage-cost-model.md`, English, the single source of truth the calculator implements.

- One section per variant, one subsection per workload type, each formula in the same shape: inputs, derived quantities,
  formula per neutral unit, coefficients with their origin (code constant / setting default / measured), source references
  (code, report section), validity range (e.g. "measured for 5–100 columns").
- A versioned **model contract** (`model-contract.json` next to the document): formula IDs, units, term structure,
  coefficients with provenance (binary sha256, commit, settings, scenario), validity domains and applicability
  (which variant supports which operation). The calculator loads it verbatim; the document's formula blocks are rendered
  from it (§7), so document, evaluation and "show calculation" share one definition.
- A section "26.6 vs 26.8" listing each unported optimisation, its effect on the formula and its flag name.
- A section "known inaccuracies" with the open items from the reports.
- A section "break-even regions" derived from sweeps of the model: for each swept input, where each variant is cheapest and
  by how much, including the regions where CAS is not; with the scenario presets that reproduce each claim, for the blog
  article. No claim enters this section before the model that produces it passed its Codex review.

## 7. The calculator page {#spa}

- Files: `utils/storage-cost-calculator/index.html`, `model.js`, `presets.js`, `model-contract.json` (copied from §6 by a
  checked-in script that fails if the two differ), `test/` with Node tests for `model.js`. Vanilla JavaScript, no framework,
  no bundler; `index.html` loads the two scripts and the JSON. Opening the file over `file://` must work, so the JSON is also
  embedded as a script block by the same copy script.
- `model.js` is pure: inputs in, per-variant tables of neutral units and money out, plus for every cell the list of terms
  with their substituted values, which the UI renders as the formula. The UI never computes a price. The same term
  definitions render symbolically (names instead of values), and a checked-in script writes that rendering into the
  formula document's formula blocks, so the document's formulas and the page's formulas are one definition; the prose
  around them is hand-written.
- Tests: every formula in §6 has a test that feeds the inputs of a measured scenario from `measurements.json` and asserts
  requests, bytes and money within the stated tolerance; tests also cover tier boundaries, the allocation of tiered prices
  back to cells, unsupported combinations rendering as "not supported", the forecast label, and the out-of-range warning.
- Layout (chosen from mockups on 2026-10-23): one screen, two panels. Left panel, sticky: scenario and price preset
  switches, the simple inputs as sliders with editable values, the advanced blocks as accordions whose headers show their
  current values. Right panel: four summary cards, the breakdown table, the opened cell's composition and formula, the notes,
  the sensitivity chart.
- Look: product-grade, not a developer tool. A design-token set (colours, type scale, spacing) on `:root`, light and dark
  themes, works at phone width, numbers formatted with thousands separators and the currency from the price preset. One
  accent for interaction, one green for "cheapest", one amber for notes; operation classes get four stable colours used in
  both the cards' stacked bars and the composition bars.
- Presets: scenarios "Logs" (many replicas, small frequent inserts, short retention), "Metrics" (narrow, very frequent small
  inserts, long retention), "Analytics batch" (wide, large hourly inserts, mutations), "Single replica archive" (one
  replica, large batches, few requests: a region where EBS or `s3` may win), "Custom"; prices "AWS us-east-1",
  "GCS us-central1", "Custom". Prices carry an `as_of` date shown in the UI and are looked up on the provider's public
  price pages when the page is built (the builder has web access), not recalled. If a provider's operation classes do not
  map onto §4.1 (GCS class A/B), the neutral unit list grows; the formulas never change for a price preset. AWS is the
  validated preset; the GCS preset ships with values and the structure but is labelled "not validated against a bill".
- Hints: each advanced block has a "Have a running cluster?" link that opens the SQL from §4.2 with a one-line explanation
  of which field each column fills.

## 8. Blog support {#blog}

The article is out of scope for this design, but the calculator must give it: the sensitivity view, URL-linkable
scenarios, and the "where CAS wins" section of the formula document with its presets. The article draft lives in the
`cas-blog-article` branch (`docs/superpowers/blog/2026-cas-storage-model/`), whose part 4 already states the S3 price tiers
and the commit request budget; those statements get re-checked against the measured model; disagreements are listed in the formula document's "known
inaccuracies" section for the blog branch to pick up. This branch does not edit the blog branch.

## 9. Repository layout and branch {#layout}

- Worktree `~/workspace/ClickHouse/storage-cost-calc`, branch `docs/storage-cost-calculator`, based on `cas-gc-rebuild`
  (my call: that is where `docs/superpowers` and the yardstick live; the measured binaries are downloaded, not built).
- Paths: this design at `docs/superpowers/specs/2026-10-23-storage-cost-calculator-design.md`; reports under
  `docs/superpowers/reports/`; the formula document and `coefficients.json` under `docs/superpowers/cost-model/`; the page
  under `utils/storage-cost-calculator/`; yardstick changes in `utils/cas-yardstick/`.
- Commits: one per workstream deliverable, signed off, exact files only (shared worktree rules). No push without an
  explicit request.

## 10. Left to the builder {#builder}

- The exact term list of each formula and every coefficient: that is the research output.
- Yardstick card definitions and the `--binary` option.
- The merge write-amplification derivation (R5) and its override UI.
- Visual design details within the constraints of §7; the slider ranges and the derivation rules that turn simple inputs
  into advanced defaults per scenario preset.
- The copy script that keeps `coefficients.json` and the formula document in step.
- How the page states unsupported combinations (e.g. an operation `s3_plain_rewritable` cannot do).
- The store-side ledger tool (proxy or RustFS access log) and its mapping to ClickHouse counters.
- The dedicated device for EBS measurement, or the decision to label results host-specific.

## 11. Open questions {#open}

- Price preset for Azure Blob: in scope for the preset structure, out of scope for values in this round.

## 12. Review history {#reviews}

| Date | Reviewer | Findings | Disposition |
|---|---|---|---|
| 2026-10-23 | Builder check (fresh agent, plan-from-document) | 11 questions | 8 folded into §3–§5 and §7 as decisions; 3 (zero-copy toggle, cache default, EBS read accounting) decided and recorded in §4.3 |
| 2026-10-23 | External review (pasted) | 8 findings: measured vs forecast default, merge replication strategy, GC garbage formula, non-comparable `s3_plain_rewritable` totals, request ledger before billing, EBS I/O accounting and gp3 limits, input contract, scenario independence | all applied |
| 2026-10-23 | Codex `gpt-6-astra`, adversarial, read-only (`tmp/codex_design_review_r1.log`) | 3 CRITICAL, 21 MAJOR, 1 MINOR; verdict REVISE | 24 applied (physical byte-hours, three-rate replication, CAS operation graph from the 26.8 source, disjoint key families, staging, capability/reuse matrix, startup/recovery, cache hit ratio as input, retry/multipart residue, completion barriers, store-side ledger, exact-integer reconciliation, dedicated device for EBS, phases with R5 first, hold-outs, model contract, billing scope). MINOR 23 (trim GCS and extra scenarios) applied in part: GCS stays as an unvalidated preset; cold/hot repeats only where the distinction applies |
