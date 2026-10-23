---
description: 'Design of the ClickHouse storage cost calculator: a measured, code-backed cost model for EBS, s3, s3_plain_rewritable and CAS disks, a bounded research programme that derives and verifies each formula on RustFS, one consolidated formula document with a typed model contract, and a single-page HTML+JS calculator for product managers and the Altinity blog.'
sidebar_label: 'Storage cost calculator design'
sidebar_position: 50
slug: /superpowers/specs/storage-cost-calculator-design
title: 'Storage cost calculator — design'
doc_type: 'reference'
---

# Storage cost calculator — design {#design}

Decision: build a storage cost calculator for ClickHouse that compares four ways to keep MergeTree data on AWS-class
infrastructure — local EBS volumes, the `s3` disk (metadata local), the `s3_plain_rewritable` disk (metadata on S3) and the
CAS disk (one copy of the data shared by all replicas). Every formula is derived from the source at one pinned commit,
verified against a store-side request ledger on RustFS, and reviewed by Codex against the code. The calculator is a single
page (HTML + JavaScript, no build step) that recomputes on every change and opens any number down to the formula with its
inputs substituted. Decided on 2026-10-23; revision 3 after two Codex rounds (§12).

**Scope freeze.** Tier 1, the deliverable of this design: one measured binary (antalya-26.8, PR 2647), AWS S3 Standard and
EBS gp3, the four variants, the workload shapes and terms listed in the coverage table (§5.4), AWS prices. Tier 2 (§11)
holds what is designed for but not built in this round: the 26.6-based "next release" forecast, GCS and Azure price values,
io2/st1 volumes, S3 staging for CAS, multi-shard layouts. Tier 2 items have a place in the model contract so they can be
added without changing formulas, and nothing in the page claims them.

## 1. Intent {#intent}

Two readers, one tool.

- **A product manager** who does not know what storage will cost them, or how much a move from EBS to `s3` or CAS would
  save, and who has to decide "stay on EBS", "move to `s3`", "move to CAS". They know their data in round numbers (TB, GB
  ingested per day, days of retention, number of replicas); some have a running cluster and can get exact numbers when told
  how.
- **An Altinity blog article** that shows where each variant is cheapest and by how much. The article is credible only
  if every number can be defended with a pointer to code and to a measurement, so the calculator doubles as the article's
  evidence.

Success: a sceptical reader who checks a line against the code, or against a measured scenario, finds the model within the
stated error, and finds the EBS and `s3` columns as carefully modelled as the CAS one.

## 2. Goals, non-goals, anti-goals {#goals}

**Goals**

- One monthly cost per variant, broken down by workload type (storage, inserts, merges, mutations, deletes/GC, reads,
  recovery, local disk) and, inside each cell, by billable operation (bytes stored, PUT-class requests, GET-class requests,
  LIST, DELETE, network traffic, block-storage capacity/IOPS/throughput).
- Every cell opens to its formula with the current inputs substituted, the branch taken and any allocation applied, and
  names the source: the section of the model document, the measurement, the code.
- Inputs in the product manager's language on top; everything else in collapsible advanced blocks whose values come from
  the scenario preset and can be overridden, with a "how to get exact numbers" hint (SQL) per block.
- Price presets per provider and scenario presets per workload; every unit price editable.
- Operational notes next to the numbers: gp3 baseline sufficiency, EC2 class implied by EBS bandwidth, local disk per
  replica for the remote variants.
- Immediate feedback: no wizard, no submit button.

**Non-goals**

- Compute (EC2) cost beyond the local disks the variants need. Notes only for instance class.
- Keeper cost, cross-region replication, backups, query performance.
- Variants outside the four named. Zero-copy replication on `s3` is an advanced toggle inside the `s3` column (§4.6).

**Anti-goals** (a working calculator that does these has failed)

- A model that flatters CAS: an `s3` or EBS column modelled less carefully than the CAS column, a default realistic for CAS
  and pessimistic for the others, or a billing rule that discounts an operation CAS issues more of.
- Numbers that cannot be traced to code and measurement, or a column that gets a smaller total by doing less work.
- Hiding merges inside inserts. For small, frequent inserts merge write amplification dominates the request bill, so merges
  are a block of their own with their own inputs.

## 3. Constraints {#constraints}

- **Ground truth:** antalya-26.8 with the CAS rework, Altinity PR 2647 (`feature/antalya-26.8/cas-disk-transactions`),
  build https://altinity-build-artifacts.s3.amazonaws.com/json.html?PR=2647&sha=09d0772a842c5d73cf3ef35b4c8b017d838215a0&name_0=PR,
  commit `09d0772a842c5d73cf3ef35b4c8b017d838215a0` (present in this repository; checked out detached, without submodules,
  at `tmp/src-26.8` for derivation and review). All four variants are measured on this one binary. Every coefficient records
  the binary sha256, the commit and the settings it was measured with.
- **Cross-check binary (Tier 2):** antalya-26.6, PR 2644,
  https://altinity-build-artifacts.s3.amazonaws.com/PRs/2644/97ac10160b56ca5381604363989b2017fd6ea28c/build_amd_binary/clickhouse.
  Downloaded and kept; not measured in Tier 1.
- **Store:** RustFS 1.0.0-rc.3 as in the CAS CI lane (`ci/jobs/scripts/clickhouse_proc.py`, cached at
  `~/workspace/ClickHouse/master/ci/tmp/rustfs`), behind the logging proxy of §5.5.
- **Rig:** extend `utils/cas-yardstick` (design `docs/superpowers/specs/2026-09-29-cas-yardstick-design.md`; it already has
  `--binary`, per-policy server processes, `system.events` deltas, cold/hot, K repeats).
- **Review:** one Codex round (`gpt-6-astra`, reasoning high, prompt via file, read-only) per workstream formula before it
  enters the consolidated document.
- **Evidence statement** that the page and the article carry verbatim: *"Request quantities validated on RustFS against the
  pinned ClickHouse build; AWS pricing rules checked against dated AWS documentation; no AWS bill reconciliation performed;
  EBS I/O figures derived from measured ClickHouse I/O with AWS accounting rules, not measured on EBS."*
- **Language:** calculator UI, formula document and reports in English.
- **Where it lives:** branch `docs/storage-cost-calculator`, worktree `~/workspace/ClickHouse/storage-cost-calc` (§9).
- **Prose:** repo rules for docs (frontmatter, `{#anchors}` on every heading, inline code for SQL and class names).

## 4. The cost model {#model}

### 4.1 Pipeline {#pipeline}

Every number on the page is produced by the same five stages, and "show calculation" shows each stage for the cell:

1. **Inputs and derivations** (§4.2, §4.3): registered quantities with scope, unit and default.
2. **Operation graphs** (§4.6): per variant and per operation (insert part, merge, mutation, delete, read, recovery, GC
   round), a branch structure that yields counts of API operations by kind and bytes, parameterised by the inputs.
3. **Request ledger**: the graph output expressed as attempts per S3 operation (PUT, COPY, POST (multipart initiate/
   complete), UploadPart, GET, HEAD, LIST pages, DELETE calls and keys, AbortMultipartUpload) with their outcome class, plus
   bytes by direction, block-storage I/O operations and capacity, and interserver bytes.
4. **Billing** (§4.7): the price preset's dated rules map ledger rows onto billable units and prices; tiers and free
   allowances are applied once at the scope the provider bills at.
5. **Allocation**: shared or tiered money is allocated back to workload cells in proportion to their share of the unit;
   the allocation is shown.

Nothing in stages 1–3 names a provider; nothing in stage 4 knows about workloads.

### 4.2 Input registry {#inputs}

Every quantity a formula depends on is registered as exactly one of: **I** a user input (simple or advanced), **P** a pinned
value (code constant, setting default, or a measured coefficient with provenance), or **D** a derivation (a rule over other
registered quantities, shown with its result and editable as an override). Scope is `cluster` (one logical copy, shards
summed), `replica`, or `pool` (CAS). Unsupported combinations disable the dependent calculation and the page says which
input caused it. Sizes are GiB/TiB on-disk compressed (`bytes_on_disk`) unless the row says otherwise; a month is 730 h.

| Block | Quantity | Kind | Scope | Default / rule |
|---|---|---|---|---|
| Simple | `data_bytes` | I | cluster | 20 TiB |
| Simple | `ingest_rate` | I | cluster | 500 GiB/day; unit selector s/min/h/day/month |
| Simple | `retention_days` | I | cluster | 40; see precedence §4.3 |
| Simple | `replicas` | I | cluster | 3 (1–6) |
| Simple | `shards` | I | cluster | 1 (Tier 1 supports 1; >1 disables with a Tier 2 note) |
| Schema | `tables` | I | cluster | 10 |
| Schema | `columns` | I | table | 40 |
| Schema | `type_mix` | I | table | `plain` / `mixed` / `complex`; selects the measured objects-per-part shape (§5.4 `types`) |
| Schema | `compression_ratio` | I | table | 5 (uncompressed / compressed); needed for part-type selection, which uses uncompressed bytes and rows |
| Schema | `rows_per_insert`, `bytes_per_insert` | I/D | insert | preset; one of the two is authoritative (§4.3) |
| Schema | `partitions_per_insert` | I | insert | 1 |
| Schema | `projections`, `skip_indexes` | I | table | 0, 0 (measured shapes: 0 and 1) |
| Schema | `min_bytes_for_wide_part`, `min_rows_for_wide_part` | P | — | defaults at the pinned SHA (`MergeTreeSettings.cpp`) |
| Schema | `part_type` | D | insert | compact if uncompressed bytes < `min_bytes_for_wide_part` and rows < `min_rows_for_wide_part`, else wide |
| Schema | `objects_per_part(part_type, type_mix, columns, projections, skip_indexes)` | P | part | measured distribution per shape (§5.4) |
| Inserts | `inserts_per_day` | D | cluster | `ingest_rate / bytes_per_insert` |
| Inserts | `parts_per_insert` | D | insert | `partitions_per_insert` (sync insert); async: by `async_insert_max_data_size` / `async_insert_busy_timeout_ms` (P) and `insert_arrival_pattern` (I: steady / bursty) |
| Inserts | `async_insert` | I | cluster | off |
| Merges | `write_amplification` | D | cluster | preset rule from `bytes_per_insert`, `retention_days`, `max_bytes_to_merge_at_max_space_in_pool` (P), `partitions_per_insert`; fitted on R5 scenarios; override allowed |
| Merges | `merged_bytes_per_day` | D | cluster | `ingest_rate × (write_amplification − 1)` |
| Merges | `merge_input_cache_hit` | P | replica | measured in `merge-natural` per variant (fraction of merge reads served from local cache) |
| Replication | `fetch_fraction[insert, merge, mutation]` | P/I | replica | measured per variant in `replication-*` (decided by `always_fetch_merged_part`, `prefer_fetch_merged_part_size_threshold`, `prefer_fetch_merged_part_time_threshold`); the remainder is executed locally (EBS, `s3`, `s3_plain_rewritable`) or relinked (CAS); override allowed |
| Mutations | `mutations_per_day` | I | cluster | preset |
| Mutations | `mutation_parts_fraction`, `mutation_columns_fraction` | I | mutation | 1.0, 1/columns (one column updated in every part) |
| Mutations | `lightweight_deletes_per_day` | I | cluster | 0 |
| Deletes | `ttl_dropped_bytes_per_day` | D | cluster | `ingest_rate` in steady state |
| Deletes | `drop_partition_per_month` | I | cluster | 0 |
| Deletes | `detached_bytes` | I | cluster | 0 |
| Deletes | `detach_attach_per_month` | I | cluster | 0 |
| Deletes | `old_parts_lifetime` | P | — | setting default at the SHA (480 s); reader holds measured in `drop` |
| Reads | `read_bytes_per_day` | I | cluster | preset |
| Reads | `cache_hit_ratio` | I | cluster | 0.9; applied once, to query reads, on every variant (EBS: OS page cache, remote: filesystem cache) |
| Reads | `bytes_per_get` | P | — | measured in `cold-select` per shape (range reads, over-read) |
| Reads | `cache_size_per_replica` | I | replica | 200 GiB; capacity of the local cache volume; also drives `cache-eviction` churn coefficient (P) |
| Topology | `distinct_azs` | I | cluster | yes |
| Topology | `restarts_per_replica_month`, `restart_downtime_min` | I | replica | 1, 10 |
| Topology | `replacements_per_month` | I | cluster | 0.1 |
| EBS | `volume_type` | I | replica | gp3 (Tier 1 only) |
| EBS | `free_space_headroom` | I | replica | 0.2 |
| EBS | `peak_factor` | I | replica | from measured insert/merge overlap (P), default 3; multiplies mean I/O demand |
| EBS | `provisioned_iops`, `provisioned_mbps` | I | volume | `auto` = max(baseline, peak demand) rounded to provider steps; or manual, with a note when below demand |
| EBS | `ebs_io_rule` | P | — | SSD: one I/O up to 256 KiB, larger split, sequential small I/O merged up to 256 KiB; HDD: 1 MiB (dated AWS doc) |
| EBS | `io_request_size_distribution(op)` | P | — | measured per operation on the `local` policy (`blktrace`/`iostat` request sizes) |
| Local disk (remote variants) | `metadata_bytes_per_object`, `metadata_iops_per_object_write` | P | replica | measured (`s3`, CAS) |
| Local disk | `staging_peak_bytes` | D | replica | CAS: concurrent insert+merge outputs × part size (local `staging_backend`, P: default `local` at the SHA) |
| Local disk | `local_volume_type` | P | replica | gp3; priced like EBS with its own IOPS demand |
| CAS | `gc_interval` | I | pool | setting default at the SHA |
| CAS | `gc_heartbeat_interval` | P | — | `max(50 ms, gc_interval / 4)` (`CasGcScheduler::heartbeatLoop`) |
| CAS | `round_duration(pool)` | P | pool | measured; rounds per month = `730 h / (gc_interval + round_duration)` (the scheduler waits after a round) |
| CAS | `mounts_per_replica` | P | replica | 1 per disk |
| CAS | `lease_renew_interval` | P | — | setting default at the SHA |
| CAS | `pool_objects`, `pool_manifests` | D | pool | from live parts × objects/blobs per part |
| CAS | `ref_log_growth_per_round` | D | pool | part events per round |
| CAS | `reuse_fraction[hash_hit, adoption, inline]` | P | op | measured per scenario shape in `cas-branches` |
| Reliability | `retry_rate[class]` | I | — | 0.005 per class (PUT-class, GET-class); each retried request counts as one extra attempt with the outcome mix measured in `fault-*` (P) |
| Reliability | `failed_upload_rate`, `uploaded_fraction_before_failure` | I/P | — | 0.001; 0.5 |
| Reliability | `incomplete_multipart_lifetime_days` | I | — | 7 (lifecycle rule; stated in the price preset) |
| Prices | one row per billable unit and rule | I | — | from the price preset with `as_of` |

The registry is the `inputs` section of the model contract (§6); the page renders advanced blocks from it. A quantity not
in the registry cannot appear in a formula; the contract check (§7) enforces this.

### 4.3 Precedence between overlapping inputs {#precedence}

Several quantities can be given two ways. The page shows a lock on the authoritative field; the other is derived and
greyed. Changing a preset or a simple input recomputes derived fields and keeps overrides, marked "overridden" with a
one-click reset. Rules:

| Pair | Authoritative by default | Derived |
|---|---|---|
| `data_bytes`, `ingest_rate`, `retention_days` | `ingest_rate`, `retention_days` | `data_bytes = ingest_rate × retention_days` (steady state; the page says so and lets the user lock `data_bytes` instead, in which case `retention_days` is derived) |
| `bytes_per_insert`, `rows_per_insert` | `bytes_per_insert` | `rows_per_insert = bytes_per_insert × compression_ratio / row_width` (preset row width) |
| `write_amplification`, `merged_bytes_per_day` | `write_amplification` | `merged_bytes_per_day` |
| `inserts_per_day`, `bytes_per_insert` | `bytes_per_insert` | `inserts_per_day` |

### 4.4 Physical byte accounting {#bytes}

Stored bytes are physical byte-hours. Each physical byte belongs to exactly one category at each instant:

| Category | Begins | Ends |
|---|---|---|
| active | part committed | part replaced (merge, mutation) or dropped |
| outdated | part replaced or dropped | final retaining reference disappears (`old_parts_lifetime`, extended by reader holds) |
| garbage (CAS only) | final reference disappears | confirmed deletion by GC |
| temporary / staging | write begins | commit or abort |
| incomplete multipart | `UploadPart` accepted | `CompleteMultipartUpload` or abort (lifecycle rule) |
| detached | `DETACH` | `ATTACH` or manual removal |
| storage-system metadata | object written | object deleted |

Per replica for EBS, `s3`, `s3_plain_rewritable`; per pool for CAS, split into disjoint key families (payload: blobs,
inline data, packs; per-part metadata: manifests; ref ledger and retained generations; GC state; mount/owner/catalog
state; staging debris). Objects shared by hardlink or by hash are counted once. `system.*_log` tables are assumed on a
local disk and excluded. Steady-state garbage: `garbage_bytes = retired_bytes_per_second × byte_weighted_mean_reclaim_delay`,
valid only when the measured GC keeps up (reclaimed ≥ retired over the measurement); otherwise the page shows "GC backlog
grows" instead of a number.

EBS capacity is provisioned, not integrated: `provisioned_bytes = peak(active + outdated + temporary) × (1 + headroom)`.

### 4.5 Replication model {#replication}

Per replica and per result type (insert part, merge result, mutation result) one of three happens: **fetched** from a
peer (sender reads its copy: `GET`-class and bytes on remote variants, device reads on EBS; bytes cross the AZ boundary
when `distinct_azs`; receiver writes its own copy), **executed locally** (the replica runs the merge or mutation itself:
its own reads and writes), or **relinked** (CAS: manifest and control traffic between replicas, no bulk blob transfer; the
manifest carries inline data). Inserts are always fetched (or relinked). Merge and mutation results split by
`fetch_fraction`, measured per variant.

Recovery adds: **restart with intact state** (`restarts_per_replica_month` × work produced during `restart_downtime_min`
by the other replicas, fetched or relinked on return, plus the startup reads of the variant), and **replacement**
(`replacements_per_month` × the whole dataset fetched or relinked, plus startup). Failed fetch attempts
(`ReplicatedPartFailedFetches`) are a measured fraction of attempts in `fault-*`.

Zero-copy replication (`allow_remote_fs_zero_copy_replication`, `s3` column, advanced toggle, off by default, with a note
on its upstream status): one physical copy, receivers do not re-upload; what they still pay for insert, merge and mutation
results is measured in `replication-zero-copy`.

### 4.6 Per-variant operation graphs {#variants}

Each variant defines, for each operation, a branch structure whose leaves are ledger rows. The exact graphs are the
research output; this section fixes what each graph must cover. A **capability/reuse matrix** (operation × variant → not
supported / rewritten / copied / hardlinked / adopted by hash / relinked) is produced by R2–R4 and is the single place the
page reads "not supported" from.

**EBS (local disk × replicas)**

- Insert, merge, mutation, read, recovery: device bytes and I/O requests with their measured size distribution, converted
  to EBS I/O operations by `ebs_io_rule`; demand as a peak (`peak_factor`); compared against the gp3 baseline (3 000 IOPS,
  125 MiB/s) and the volume limits (dated); excess becomes provisioned IOPS/throughput; capacity per §4.4.
- Mutation hardlinks unchanged columns (no rewrite). Reads: the cold fraction `(1 − cache_hit_ratio)` of
  `read_bytes_per_day` hits the device.
- Replication per §4.5; no request costs; cross-AZ bytes when `distinct_azs`.
- Notes: volume count (dataset / max volume size), EC2 class whose EBS bandwidth covers the peak MiB/s.
- Measurement: the `local` policy on a dedicated filesystem; if no dedicated device is available on the workstation, the
  R1 report labels its I/O distributions host-specific and says so in the page's evidence statement. Page-cache boundaries:
  `sync` + `echo 3 > /proc/sys/vm/drop_caches` between cards (`SYSTEM DROP ... CACHE` does not clear the OS cache).

**`s3` disk (metadata local × replicas)**

- Insert part: objects per part (measured distribution) → `PUT` or multipart (initiate, `UploadPart` × ceil(size /
  `s3_min_upload_part_size`), complete) by `s3_max_single_part_upload_size` (P), plus the transaction's `HEAD`/`GET`
  checks, from the graph R2 reads at the SHA.
- Merge: source reads (`GET`-class, bytes × (1 − `merge_input_cache_hit`)), result write as insert, source deletion after
  `old_parts_lifetime` (`DELETE` calls and keys).
- Mutation: rewritten columns as insert; the rest hardlinked (metadata only); lightweight delete writes the mask column.
- Delete paths: `DROP PARTITION`, TTL, `DROP TABLE SYNC`; detach/attach.
- Reads: `GET` = cold bytes / `bytes_per_get`; cache fill and eviction churn as local disk I/O (`cache-eviction`).
- Startup: what the disk reads from S3 at start with intact local metadata (measured externally, `startup`); the plain `s3`
  disk does not list the bucket at start.
- Replication per §4.5: sender `GET`s, receiver `PUT`s; local re-merge costs the replica its own merge.
- Local disk: metadata (bytes and IOPS per object write) + cache volume (`cache_size_per_replica`) + nothing else, priced
  as a gp3 volume with its IOPS demand; appears in the "Local disk" row with money, not only as a note.
- Zero-copy toggle per §4.5.

**`s3_plain_rewritable` (metadata on S3 × replicas)**

- As `s3`, plus `__meta` objects, startup load (lists metadata and directory contents), rename/commit extras, `COPY` where
  its hardlink implementation uses it; no local metadata, cache volume as `s3`.
- Operations the disk refuses (mutations, lightweight deletes; capability gate, error 344) make the column **not
  comparable** for that scenario: the card shows "not supported for this workload" instead of a total, and the column is
  excluded from savings deltas and the "cheapest" mark. Whether `ReplicatedMergeTree` on this disk is a valid deployment at
  all is R3's first question; if not, "not supported" whenever `replicas > 1`.

**CAS (one copy × GC)**

- Insert part: the graph R4 reads at the SHA with a branch per entry case — fresh blob (`HEAD`-before-`PUT`, blob `PUT`),
  hash hit, trusted adoption of an unchanged hardlinked entry, inline entry — then staging (local `staging_backend`:
  local writes and reads, peak space), publication (manifest CAS `PUT`), ref-ledger writes on rename/commit (format
  generation 2). Each branch's population per shape is measured in `cas-branches` with an assertion that the branch ran
  (`system.cas_log` event kinds).
- Merge, mutation: as `s3` for reads; writes through the same branch graph, so reuse beyond hardlinks is what the hash-hit
  and adoption populations measure, not an assumed fraction.
- Deletes: foreground ref removal in `ContentAddressedTransaction` (requests on the data path), then garbage per §4.4.
- GC: separate terms with their own scheduling semantics at the SHA — scheduler attempts and leader/follower outcome,
  rounds (`730 h / (gc_interval + round_duration)`), heartbeats (`gc_heartbeat_interval`, `gc/hb` read and conditional
  write), ref folding, live-state scanning (`LIST` pages over pool families), retained-generation cleanup, reclamation
  (`DELETE` calls and keys). Each term fitted on `gc-scale` where pool objects (3 levels), mounts (2 levels) and ref-log
  growth (2 levels) vary independently; namespaces pinned to 1 (Tier 1).
- Lease: renewals per `lease_renew_interval` per mount per replica (`lease`).
- Replication: relink per result type (`replication` on CAS asserts relink vs local re-merge from `system.cas_log` and
  `ReplicatedPartFetchesOfMerged`); interserver bytes measured (§5.5).
- Local disk: metadata + cache volume + staging peak, priced as for `s3`.
- Physical bytes per §4.4 key families, reconciled against the ledger's object lifecycle (§5.5), not `system.parts`.

### 4.7 Billing rules {#billing}

The price preset carries dated rules, not only unit prices. AWS (Tier 1):

- Requests per 1 000 by class: PUT/COPY/POST/LIST tier; GET/SELECT/HEAD tier; DELETE and AbortMultipartUpload free.
  `DeleteObjects` is one request. LIST is per page.
- Outcome mapping from the dated AWS "error code billing" document: successful and most client-error responses are
  billed; server errors are not; the documented exceptions (ownership-dependent 403, specific codes) are listed in the
  preset with the document date. An outcome the preset does not list is billed. No blanket exemption for 404: CAS's
  missing-object probes are billed as the document says.
- Storage per GiB-month by tier, applied once at the account/bucket scope; incomplete multipart parts are storage until
  completed or aborted; the lifecycle rule (`incomplete_multipart_lifetime_days`) bounds them.
- Data transfer: in-region EC2↔S3 free; cross-AZ EC2↔EC2 per GB in each direction; internet egress 0 by default.
- EBS gp3: GiB-month, provisioned IOPS above 3 000, provisioned MiB/s above 125, per volume; volume limits dated.
- Unit conversion GiB→GB where the price list is decimal.

GCS, Azure: the rule structure is the same (class A/B operations, early-deletion, egress); values are Tier 2.

### 4.8 Output {#output}

- Four summary cards (one per variant) with the monthly total, delta against EBS and against `s3`, and a stacked bar by
  unit class (bytes, traffic, PUT-class, GET-class, block storage). A not-comparable column shows "not supported for this
  workload" and no total.
- Table `workload type × variant`: storage, inserts, merges, mutations, deletes/GC, reads, recovery, local disk, total.
  Cells that do not apply show a dash.
- Click or hover opens the cell's composition by operation with amounts and bars; "show calculation" prints the five
  stages (§4.1) for the cell: inputs used, the branch taken, ledger rows, billing rule applied, allocation; then the source
  line: model section, measurement record, code reference at the SHA.
- Notes: EBS volume count and instance class; local disk per replica for the remote variants; unsupported or
  out-of-range inputs; the evidence statement (§3).
- Sensitivity view: monthly total per variant against one swept input (replicas, insert size, retention, read fraction),
  break-even points marked, including the regions where CAS is not cheapest.
- URL state: effective inputs plus model version, contract version and price preset `as_of`; a URL whose versions are
  not available renders "cannot reproduce: version X not present" rather than a different number.

## 5. Research programme {#research}

### 5.1 Scope and exit gates {#scope}

Tier 1 as frozen in §0. Two gates:

- **Phase-0 exit gate.** Before any parallel calibration: one `s3` insert reconciles the ledger against its predicted graph
  to the exact integer; a two-process replicated fetch is attributed sender/receiver; a proxy-injected failed multipart
  upload leaves the predicted residue and is aborted by the rig; one CAS garbage cohort is reclaimed and its reclaim delay
  measured; the full coverage table's runtime is estimated from these four and recorded in the ledger. If the estimate
  exceeds three workstation-days, P2 scenarios (§5.4) are cut before anything else, and the cut is recorded.
- **Consolidation gate.** No coefficient enters the contract without provenance, a fit scenario and (for P1 terms) a
  hold-out gap.

### 5.2 Phases and workstreams {#workstreams}

| Phase | What | Output |
|---|---|---|
| 0. Setup | binaries, source at the SHA, RustFS behind the ledger proxy, yardstick extensions (multi-process mode, `local` policy, per-card completion conditions, ledger and inventory integration, fault injection via the proxy, external startup timing), counter semantics at the SHA, pinned configs, executable scenario definitions (§5.4), phase-0 gate | yardstick commit, `measurement-contract.md`, gate record |
| 1. Lifecycle definitions (R5) | executable workload definitions; part type selection; parts per insert; amplification rule and its fit on one variant; mutation bytes; removal timing. R5 supplies definitions and assumptions; each variant validates its own amplification and replication split in phase 2–3, and differences stay visible in the report | R5 report v1 |
| 2. Calibration (R1–R4) | operation graphs from the source; isolated cards; exact-integer reconciliation; coefficients | per-variant reports v1 |
| 3. Integrated validation | hold-outs, multi-process lifecycle, GC under load, recovery, long amplification run; stochastic tolerances from observed variation | hold-out gaps, final coefficients |
| 4. Review and consolidation | Codex round per workstream, dispositions, consolidated document, contract | §6 document, `model-contract.json` |

| # | Workstream | Derives | Measures |
|---|---|---|---|
| R1 | EBS | I/O request size distributions per operation; EBS I/O conversion; peak factor; capacity | `local` policy with block counters and `blktrace` |
| R2 | `s3` | operation graphs per operation from the SHA; multipart; replication send/receive; delete paths; startup; metadata footprint; zero-copy | `s3` cards, ledger, local metadata dir |
| R3 | `s3_plain_rewritable` | as R2 plus `__meta`, startup load, rename/commit, `COPY`; capability matrix; replication validity | `s3_plain` cards |
| R4 | CAS | branch graph per entry case; relink; key families; reclaim delay; GC terms and scheduler semantics; lease; staging | `cas` cards, `cas-branches`, `gc-*`, `lease` |
| R5 | Lifecycle | executable definitions; part type; parts per insert; async inserts; amplification rule | `small-inserts`, `async-inserts`, `merge-natural`, `amplification-run` |

### 5.3 Method per workstream {#method}

1. **Code → graph.** Read the code path at the SHA; write the operation graph with named parameters and a reference per
   branch (file, class, setting). Mark each dependency's registry kind (I/P/D).
2. **Graph → prediction.** Predict ledger rows per scenario before measuring.
3. **Measure.** Run the executable scenario; collect the ledger, per-server `system.events`, `system.blob_storage_log`,
   `system.cas_log`/`system.cas_gc_log`, local disk and network counters.
4. **Reconcile.** Deterministic isolated paths reconcile to the exact integer per operation kind; a miss is a graph defect.
   Stochastic terms get absolute + relative tolerance from observed variation over K=3 and the report states both.
   Background traffic is reported separately with raw counts; it is never subtracted to make a deterministic path exact.
   An unexplained gap stays in the report as an open item; no fudge factors.
5. **Codex review.** One round against the code at the SHA; findings fixed or answered in the report.
6. **Report** `docs/superpowers/reports/2026-10-<dd>-storage-cost-<variant>.md` with `measurements.json` beside it (one
   record per scenario × policy × run × server: ledger rows, counters, inventory transitions, local I/O samples, wall
   time, binary sha256, settings).

### 5.4 Term-to-scenario coverage {#coverage}

Scenario definitions are **executable files** under `utils/cas-yardstick/yardstick/cards/`: DDL, deterministic data
generator with seed, partitioning, settings, operation sequence, completion condition, included maintenance, and the
assertions that the intended shape and branches occurred (part type from `system.parts`, object sizes from the ledger,
branch kinds from `system.cas_log`). "Five columns" is not a definition; the card file is. Frozen before phase 2.

Roles: **fit** (used to fix a graph or coefficient), **hold-out** (predicted before measurement; never used to fit; a
change driven by a hold-out result consumes it and requires a fresh hold-out, recorded in the report), **validation**
(long or integrated run reported as-is). Priority P1 runs in Tier 1 unconditionally; P2 is cut first if the phase-0 gate
estimate overruns.

| Term (§4.6) | Variants | Scenario | Role | Pri |
|---|---|---|---|---|
| objects per part, compact | all remote | `narrow-compact`, `wide-compact` | fit | P1 |
| objects per part, wide | all remote | `narrow-wide` | fit | P1 |
| objects per part, wide, 100 columns | all remote | `wide-wide` | hold-out | P1 |
| objects per part, complex types + projection + skip index | all remote | `types` | hold-out | P1 |
| part-type thresholds, multipart threshold | all | `boundaries` | hold-out | P1 |
| multipart graph | remote | `big-insert-multipart` | fit | P1 |
| parts per insert, part type by size | all | `small-inserts` | fit (R5) | P1 |
| async insert flushing | all | `async-inserts` | fit (R5) | P2 |
| merge graph (single merge) | all | `merge-forced` | fit | P1 |
| natural merge selection, amplification, `merge_input_cache_hit` | all, per variant | `merge-natural` | fit (R5) + per-variant validation | P1 |
| amplification extrapolation bounds | one variant | `amplification-run` (2–4 h) | validation | P2 |
| mutation graph, hardlink/adoption reuse, lightweight delete, capability matrix | all | `mutation` | fit | P1 |
| delete paths, `old_parts_lifetime`, reader holds | all | `drop` | fit | P1 |
| detach/attach, detached residency | all | `detach-attach` | fit | P2 |
| cold reads, `bytes_per_get`, cache on/off | remote | `cold-select` | fit | P1 |
| cache eviction churn | remote | `cache-eviction` (working set 2× cache) | fit | P2 |
| EBS read device demand | EBS | `cold-select` on `local` | fit | P1 |
| startup reads, intact state | all | `startup` | fit | P1 |
| replication: fetched / local / relinked per result type, sender/receiver, interserver bytes | all | `replication-fetch` (forced fetch), `replication-local` (forced local), `replication-natural` | fit, fit, hold-out | P1 |
| zero-copy | `s3` | `replication-zero-copy` | fit | P1 |
| restart catch-up | all | `restart-catchup` (stop B, work on A, start B) | fit | P1 |
| replacement | all | `replacement` (third replica) | fit | P2 |
| retries, failed uploads, multipart residue, lost response | remote | `fault-put-5xx`, `fault-lost-response`, `fault-multipart-abort` (proxy injection) | fit | P1 |
| failed fetch attempts | all | `fault-fetch` | fit | P2 |
| CAS branch populations (fresh, hash hit, adoption, inline), staging peak | CAS | `cas-branches` | fit | P1 |
| CAS foreground ref removal | CAS | `drop` on CAS | fit | P1 |
| CAS reclaim delay by stage, garbage cohort convergence | CAS | `gc-lifecycle` | fit | P1 |
| GC terms vs pool objects × mounts × ref growth | CAS | `gc-scale` (3×2×2) | fit | P1 (3×1×1), P2 (rest) |
| GC heartbeat, scheduler attempts, idle round | CAS | `gc-idle` | fit | P1 |
| lease renewals | CAS | `lease` | fit | P1 |
| local metadata footprint and IOPS | `s3`, CAS | every card on those policies (sampled) | fit | P1 |

Excluded in Tier 1 (listed so no builder adds them silently): S3 staging backend, namespaces > 1, shards > 1, io2/st1,
26.6 binary, GCS/Azure values, KMS, VPC endpoint charges, Keeper.

### 5.5 Measurement contract {#contract}

- **Ledger proxy.** A logging reverse proxy in front of RustFS (builder's choice of tool) records per attempt: unique
  attempt id, timestamp, caller identity (each server process uses its own access key; the rig uses a fourth), parsed S3
  operation (from method, query parameters and headers: `uploads`, `uploadId`/`partNumber`, `x-amz-copy-source`, `delete`
  with the parsed key count, `list-type` with page size and key count), key, exact HTTP status and S3 error code, request
  and response payload bytes, multipart upload id. Pre-store failures (connection errors, timeouts before the proxy) come
  from ClickHouse counters and are classified separately. Retries are correlated only where evidence exists (same key and
  payload within the SDK retry window); otherwise counted as independent attempts. The proxy can inject faults (status,
  dropped response after the store accepted) for `fault-*`.
- **Mapping.** `measurement-contract.md` maps ClickHouse counters to ledger columns and lists overlaps (`S3*` vs `DiskS3*`,
  `CAS*` semantic counters) so none is summed twice. The ledger is authoritative for requests and S3 bytes.
- **Object lifecycle and byte-hours.** Computed from ledger transitions per key (`PUT`/`CompleteMultipartUpload` create,
  `DELETE` delete, `UploadPart` adds incomplete bytes) with sizes, so short-lived objects count and byte-hours integrate
  exactly; inventory LISTs by the rig are observer traffic (fourth caller) and excluded from measured requests. GC
  convergence = reclamation of the designated garbage cohort (keys recorded at retirement), not an unchanged bucket.
- **Attribution.** Each card declares its completion condition (`SYSTEM SYNC REPLICA`, replication queue empty, expected
  part and row counts on every replica, `SYSTEM FLUSH LOGS`, and for GC cards the cohort reclaimed) and which maintenance
  it includes (merges stopped unless measured; GC on its scheduler unless the card measures a round). Traffic outside the
  card's declared scope is reported as background with raw counts, never subtracted to make a deterministic path exact.
- **Multi-process.** Up to three server processes on one host, embedded Keeper in the first, `ReplicatedMergeTree`;
  distinct prefixes per replica for non-shared disks, the shared pool for CAS. Interserver bytes per server from
  `NetworkSendBytes`/`NetworkReceiveBytes` deltas minus the ledger's S3 bytes for that caller, cross-checked with
  `ReplicatedPartFetches`/`ReplicatedPartFetchesOfMerged`; directional bytes map to cross-AZ charges when `distinct_azs`.
- **Local disk.** Per-server metadata/cache/staging directories on their own filesystem; size sampled at 1 s for peaks;
  block counters and request sizes per card on the `local` policy; OS page cache dropped between cards.
- **What one workstation cannot establish**, stated in every report and in the page's evidence statement: EBS device
  behaviour, actual cross-AZ transfer, AWS bill totals. These are derived with dated provider rules.

## 6. Consolidated formula document and model contract {#formula-doc}

`docs/superpowers/cost-model/storage-cost-model.md` (prose) and `docs/superpowers/cost-model/model-contract.json`
(machine-readable, versioned), English.

- The contract holds: the input registry (§4.2) with scope, unit, default and precedence; coefficients with provenance
  (binary sha256, commit, settings, scenario, run ids, tolerance); the operation graphs and formulas as a **small typed
  expression structure**: node kinds `input(id)`, `coef(id)`, `const`, `add`, `sub`, `mul`, `div`, `min`, `max`,
  `ceil_to(step)`, `floor_to(step)`, `branch(condition, then, else)` with comparison conditions on registered quantities,
  `sum_over(set)`, `allocate(total, shares)`, `price(unit, rule)`; dimensions on every node and a dimension check;
  applicability (capability matrix) per variant × operation; the billing rules per preset with `as_of`; the evidence
  statement; the contract version.
- Evaluation and "show calculation" are produced by one evaluator over this structure; the renderer shows the branch taken
  and the allocation. No arithmetic that changes a displayed number lives outside the contract.
- The prose document's formula blocks are rendered from the contract by a checked-in script with two modes: `generate`
  (writes the blocks, the standalone JSON and the embedded copy in the page) and `check` (fails if any of the three differs
  from a fresh render). The hand-written prose explains, cites reports and lists known inaccuracies and the 26.6 delta
  (Tier 2).
- A "break-even regions" section derived from sweeps, with presets that reproduce each claim; nothing enters it before the
  producing formulas passed review.

## 7. The calculator page {#spa}

- Files: `utils/storage-cost-calculator/index.html`, `model.js` (evaluator + renderer over the contract), `ui.js`,
  `presets.js`, `model-contract.json` (generated copy, also embedded in the page so `file://` works), `test/` (Node).
  Vanilla JavaScript, no framework, no bundler.
- Tests: (a) measured fixtures from `measurements.json`: requests, bytes and ledger rows within the stated tolerance for
  each fit and hold-out scenario; (b) pricing fixtures with exact expected money covering each billing class, outcome
  mapping, tier boundary, free allowance, GiB/GB conversion, volume baseline and allocation; (c) contract checks:
  dimension check passes, every input referenced exists in the registry, every variant × operation has an applicability
  entry, the three contract copies agree; (d) UI states: not-comparable column, out-of-range warning, overridden field,
  non-reproducible URL.
- Layout (chosen from mockups on 2026-10-23): one screen, two panels. Left, sticky: scenario and price preset switches,
  the simple inputs as sliders with editable values, advanced blocks as accordions whose headers show their current values
  and lock/override state. Right: four summary cards, the breakdown table, the opened cell's composition and five-stage
  calculation, notes, evidence statement, sensitivity chart.
- Look: product-grade. Design tokens on `:root`, light and dark themes, phone width works, thousands separators, the
  preset's currency. One accent for interaction, one green for "cheapest" (never applied to a not-comparable column), one
  amber for notes; four stable colours for unit classes shared by the cards' bars and the composition bars.
- Presets: scenarios "Logs" (many replicas, small frequent inserts, short retention), "Metrics" (narrow, very frequent
  inserts, long retention), "Analytics batch" (wide, large hourly inserts, mutations), "Single replica archive" (one
  replica, large batches, few requests), "Custom"; prices "AWS us-east-1 (as_of)" (Tier 1), "GCS" and "Azure" as
  structure with "values not provided" (Tier 2), "Custom".
- Hints: each advanced block has a "Have a running cluster?" link with the SQL that fills its fields
  (`system.parts`, `system.part_log`, `system.events`, `system.cas_gc_log`) and a one-line mapping column → field.

## 8. Blog support {#blog}

Out of scope to write; in scope to enable: the sensitivity view, versioned URL-linkable scenarios, the break-even section
of §6, and the evidence statement. The draft series on the `cas-blog-article` branch
(`docs/superpowers/blog/2026-cas-storage-model/`, part 4 states S3 price tiers and the commit request budget) is re-checked
against the measured model; disagreements are listed in the formula document's "known inaccuracies" for that branch to pick
up. This branch does not edit the blog branch.

## 9. Repository layout and branch {#layout}

- Worktree `~/workspace/ClickHouse/storage-cost-calc`, branch `docs/storage-cost-calculator`, based on `cas-gc-rebuild`
  (where `docs/superpowers` and the yardstick live; the measured binaries are downloaded, not built).
- Paths: this design; ledger and reports under `docs/superpowers/reports/2026-10-23-storage-cost-calculator/` and
  `docs/superpowers/reports/`; the formula document and contract under `docs/superpowers/cost-model/`; the page under
  `utils/storage-cost-calculator/`; yardstick changes in `utils/cas-yardstick/`; binaries and the 26.8 source under `tmp/`
  (not committed).
- Commits: one per deliverable, signed off, exact files only. No push without an explicit request.

## 10. Left to the builder {#builder}

- The operation graphs and every coefficient (research output); the capability/reuse matrix.
- The ledger proxy tool and the fault-injection mechanism; the mapping document.
- The dedicated device for `local`, or the host-specific label.
- Card files, including data generators and assertions.
- The amplification rule and the preset rules that fill advanced blocks from simple inputs, with ranges.
- Visual design within §7; slider ranges.
- The `generate`/`check` script.

## 11. Tier 2 (designed for, not built now) {#tier2}

- "Next release (forecast)" toggle on the CAS card: flags for the 26.6 optimisations not yet ported, each with its measured
  26.6 effect and the statement that their interaction on 26.8 is unmeasured; labelled forecast; excluded from validated
  savings. Requires measuring the 26.6 binary on the same cards.
- GCS and Azure price values and rules with `as_of`; io2 and st1 volume rules; S3 staging backend for CAS; shards > 1;
  namespaces > 1; KMS and VPC endpoint charges.
- Owner's note recorded in the ledger (D5): the forecast was the owner's preferred headline before review; two reviews
  showed an unmeasured combination cannot be the default, so it moved to Tier 2.

## 12. Review history {#reviews}

| Date | Reviewer | Findings | Disposition |
|---|---|---|---|
| 2026-10-23 | Builder check (fresh agent, plan-from-document) | 11 questions | folded into §3–§5, §7 as decisions |
| 2026-10-23 | External review (pasted) | 8 findings: measured vs forecast default, merge replication strategy, GC garbage formula, non-comparable `s3_plain_rewritable` totals, request ledger before billing, EBS I/O accounting and gp3 limits, input contract, scenario independence | all applied |
| 2026-10-23 | Codex `gpt-6-astra` r1 (`tmp/codex_design_review_r1.log`) | 3 CRITICAL, 21 MAJOR, 1 MINOR; REVISE | 24 applied, MINOR 23 (trim scope) applied in part |
| 2026-10-23 | Codex `gpt-6-astra` r2 (`tmp/codex_design_review_r2.log`) | 16 MAJOR, 2 MINOR; REVISE | revision 3 restructured the spec: scope freeze with Tier 1/Tier 2 (r2-5), input registry with I/P/D kinds (r2-1), precedence rules (r2-7), one-category-per-byte accounting and byte-weighted reclaim delay (r2-3), GC scheduler semantics incl. heartbeat and wait-after-round (r2-4), per-variant validation of lifecycle (r2-6), ledger schema with parsed operations and caller identity (r2-8), per-card completion conditions and no baseline subtraction (r2-9), byte-hours from ledger transitions and cohort convergence (r2-10), interserver bytes and three-process mode (r2-11), SSD 256 KiB I/O rule (r2-12), dated outcome mapping with no 404 exemption (r2-13), priced local disk and symmetric cache assumption (r2-14), evidence statement (r2-15), typed expression contract (r2-16), fresh hold-outs and pricing fixtures and non-reproducible URL state (r2-17), single artifact name and generate/check modes (r2-18), term-to-scenario coverage table with fault, detach, cache-eviction, restart, branch and GC-scale scenarios (r2-2) |
