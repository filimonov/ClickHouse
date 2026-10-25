---
description: 'Design of the ClickHouse storage cost calculator: a product contract (supported domain, input semantics, accounting invariants, calculation and output contract) plus a bounded research programme whose versioned artifacts establish every implementation fact with evidence and closure checks; delivered as a single-page HTML+JS calculator for product managers and the Altinity blog.'
sidebar_label: 'Storage cost calculator design'
sidebar_position: 50
slug: /superpowers/specs/storage-cost-calculator-design
title: 'Storage cost calculator — design'
doc_type: 'reference'
---

# Storage cost calculator — design {#design}

Decision: build a storage cost calculator for ClickHouse that compares four ways to keep MergeTree data on AWS-class
infrastructure — local EBS volumes, the `s3` disk, the `s3_plain_rewritable` disk and the CAS disk. Their physical layout,
sharing and replication mechanisms are established by the versioned graph artifacts, not asserted here. The design fixes **observable behaviour and evidence obligations**;
**versioned research artifacts** establish every implementation fact (what the code does, which settings matter, how many
requests an operation issues) with source references and measurements, and mechanical closure checks tie the artifacts to
the calculator. The calculator is a single page (HTML + JavaScript, no build step) that recomputes on every change and opens
any number down to the formula with its inputs substituted. Decided 2026-10-23; revision 4 after three Codex rounds and a
convergence consult (§12).

**Supported domain (Tier 1).** One measured binary (antalya-26.8, PR 2647), AWS S3 Standard in unversioned general-purpose
buckets, EBS gp3, the four variants, one shard, `ReplicatedMergeTree` with 1–6 replicas, the workload families of the
coverage artifact (§5.6), AWS prices. Tier 2 (§11) is designed for through versioned extensibility of the contract, not
through a promise that formulas never change: the 26.6-based "next release" forecast, GCS and Azure values, io2/st1, S3
staging for CAS, multi-shard layouts. The page claims nothing outside Tier 1 and renders unsupported combinations as such.

## 1. Intent {#intent}

Two readers, one tool.

- **A product manager** who does not know what storage will cost them, or how much a move from EBS to `s3` or CAS would
  save, and who has to decide "stay on EBS", "move to `s3`", "move to CAS". They know their data in round numbers; some have
  a running cluster and can get exact numbers when told how.
- **An Altinity blog article** that shows where each variant is cheapest and by how much, defensible with a pointer to
  code and to a measurement for every number.

Success: within the declared validity domain and stated uncertainty, a sceptical reader who checks a line against the code
or a measured scenario finds the model within the stated error, and finds the EBS and `s3` columns as carefully modelled as
the CAS one.

## 2. Goals, non-goals, anti-goals {#goals}

**Goals**

- One monthly cost per variant, broken down by workload type (storage, inserts, merges, mutations, deletes/GC, reads,
  recovery, local disk) and, inside each cell, by billable operation.
- Every cell opens to its calculation: inputs used, branch taken, request ledger rows, billing rule, allocation, and the
  evidence behind each coefficient (artifact, measurement record, source reference).
- Inputs in the product manager's language on top; advanced blocks filled by scenario presets and overridable, each with a
  "how to get exact numbers" SQL hint.
- Price presets per provider, scenario presets per workload, every unit price editable.
- Operational notes: EBS provisioning and instance class, local disk per replica for the remote variants.
- Immediate feedback: no wizard, no submit button.

**Non-goals**

- Compute (EC2) cost beyond the local disks the variants need; Keeper; cross-region replication; backups; query
  performance; variants outside the four. Zero-copy replication on `s3` is a toggle inside the `s3` column.

**Anti-goals**

- A model that flatters CAS: a column modelled less carefully than another, a default realistic for one variant and
  pessimistic for another, a billing rule that discounts an operation one variant issues more of, or a column that gets a
  smaller total by doing less work.
- Numbers without evidence, or evidence of the wrong kind (a measured claim backed only by reasoning, a code claim backed
  only by a measurement).
- Hiding merges inside inserts: merges are a block of their own.

## 3. Constraints and evidence policy {#constraints}

- **Pinned build:** antalya-26.8, Altinity PR 2647, commit `09d0772a842c5d73cf3ef35b4c8b017d838215a0`
  (https://altinity-build-artifacts.s3.amazonaws.com/json.html?PR=2647&sha=09d0772a842c5d73cf3ef35b4c8b017d838215a0&name_0=PR);
  source checked out detached at that commit; binary sha256 recorded with every coefficient. 26.6 (PR 2644) is kept for
  Tier 2 and not measured in Tier 1.
- **Store:** RustFS behind the ledger proxy of the measurement contract. Correction: the rig ran RustFS 1.0.0-beta.9,
  identified by sha256 after the runs; 1.0.0-rc.3, named here first, is the container image of `rustfs.sh`, which the
  cost rig does not use ([measurement contract](../cost-model/measurement-contract.md#identities)).
- **Rig:** extend `utils/cas-yardstick` (design `docs/superpowers/specs/2026-09-29-cas-yardstick-design.md`).
- **Evidence policy.** Every result carries one of four evidence kinds, and the page shows which: **source-derived**
  (a code path at the pinned commit, cited file and symbol, exercised by at least one fixture), **measured** (a card in the
  coverage artifact with provenance, validity domain, fit data and independent hold-out evidence), **provider-documented**
  (a dated provider document), **assumed** (a preset or user value, labelled as such). A universal "derived from source and
  verified on RustFS" claim is not made: prices, user inputs and extrapolations cannot satisfy it.
- **Review as budget, exit by evidence.** Each research artifact gets a Codex review (`gpt-6-astra`, reasoning high,
  prompt via file, read-only) within a defined scope when produced; discovered defects are closed, not deferred; a changed
  artifact reopens the artifacts that depend on it. The number of rounds is a budget recorded in the ledger, not the exit
  condition: an artifact is accepted when its closure checks (§5.2) pass and its review has no open MAJOR.
- **Evidence statement** the page and the article carry verbatim: *"Request quantities validated on RustFS against the
  pinned ClickHouse build; AWS pricing rules checked against dated AWS documentation; no AWS bill reconciliation performed;
  EBS I/O figures derived from measured ClickHouse I/O with AWS accounting rules, not measured on EBS."*
- **Language** English. **Where it lives** §9. **Prose** repo doc rules (frontmatter, `{#anchors}`, inline code).

## 4. Product contract {#product}

### 4.1 Pipeline {#pipeline}

Every number is produced by five stages, and "show calculation" shows each for the cell:

1. **Inputs and derivations**: registered quantities with scope, unit, default and evidence kind (§4.2, registry artifact).
2. **Operation graphs**: per variant and operation, a branch structure yielding operation counts and bytes (graph
   artifacts, one per variant).
3. **Request ledger**: attempts per S3 operation with outcome class, bytes by direction, block-storage I/O and capacity,
   interserver bytes.
4. **Billing**: the price preset's dated rules map ledger rows to billable units and money (billing artifact).
5. **Allocation**: shared or tiered money allocated back to cells in proportion to their share of the unit.

Stages 1–3 name no provider; stage 4 knows no workload. One shared definition (the model contract, §6) drives both
evaluation and explanation.

### 4.2 Input semantics {#inputs}

- **Simple inputs** (always visible): data size, ingest rate with a unit selector (per second … per month), retention,
  replicas, scenario preset, price preset. **Advanced blocks**: schema, inserts, merges, replication, mutations and deletes,
  reads and cache, topology and recovery, EBS, local disk, CAS, reliability, prices. Their contents are the registry
  artifact (§5.1); the page renders them from it.
- **Units and scope.** Sizes are on-disk compressed bytes (`bytes_on_disk`) in GiB/TiB unless a registry row says
  otherwise; GiB→GB conversion happens in billing where a price list is decimal. Scope is one of `cluster` (one logical
  copy), `replica`, `pool` (CAS), `volume`. A month is 730 h.
- **Stock and flow.** `data = ingest × retention` is a steady-state assumption, labelled on the page; the user locks any
  two of the three and the third is derived. Every other overlapping pair (bytes vs rows per insert, amplification vs
  merged bytes, inserts per day vs bytes per insert, part count vs part size) has one authoritative field by default, a
  lock to switch, and a derived, greyed partner. Changing a preset or a simple input recomputes derived fields, keeps
  overrides marked "overridden" with one-click reset.
- **Evidence kind per quantity**: user input (assumed), pinned (source-derived or provider-documented), measured
  coefficient, or derivation. A derivation is an expression over registered quantities; derivations are acyclic.
- **Validity domain.** Every measured coefficient carries the range it was measured over; inputs outside the range mark
  the dependent cells "outside measured range" and the page keeps computing.
- **Unsupported.** A combination the coverage artifact does not support disables the dependent cells with the reason; it
  never contributes zero silently (§5.5).

### 4.3 Accounting invariants {#invariants}

- **Physical bytes are byte-hours and each byte is in exactly one category at each instant:** active, outdated (replaced
  or dropped but still retained), garbage awaiting reclamation (CAS), temporary/staging, incomplete multipart, detached,
  storage-system metadata. Objects shared by hardlink or hash are counted once. Byte-hours come from the ledger's object
  lifecycle (§5.1 measurement contract), with bounded timing uncertainty, not from snapshots. Block-storage capacity is
  provisioned, not integrated: peak physical bytes (all categories present on the volume, detached and system metadata
  included) × (1 + headroom), allocated to feasible volumes.
- **Steady-state garbage** is stated only when reclamation keeps up with retirement over the measurement. When the
  backlog grows, the column displays "backlog grows; steady-state monthly cost unavailable", has no total, and is excluded
  from savings deltas and the "cheapest" mark.
- **Requests are attempts with outcomes.** The ledger records every attempt by parsed operation, outcome and bytes;
  billing decides what is charged. Retries are correlated only on evidence.
- **Foreground, background and observer traffic are distinguished.** Exact-integer claims are made only for cards whose
  unrelated background workers are stopped and joined; where background work stays active, requests need evidenced
  attribution or the interval is reported as an integrated stochastic measurement. The rig's own traffic (inventory, probes)
  is identified by caller and excluded.
- **Replication accounting distinguishes peer acquisition from local execution by result type.** The graph artifacts
  establish each supported acquisition mechanism per variant and account for sender and receiver traffic; a replica that
  executes a merge or mutation itself pays its own reads and writes. Which happens, how often and by which settings is
  established by research, not asserted here. Recovery (restart with
  intact state, replica replacement) is accounted the same way.
- **Local storage and peer traffic are included** for every variant: metadata, cache and staging volumes of the remote
  variants are priced as block storage with their own I/O demand; interserver bytes are measured directionally and charged
  per the topology (cross-AZ or not).
- **Competing deployments are priced independently.** Each variant's usage is aggregated across its own buckets and
  volumes at the provider's billing scope; variants are never aggregated together.
- **A column that cannot perform the workload is not comparable**: it shows "not supported for this workload", has no
  total, and is excluded from savings deltas and the "cheapest" mark.

### 4.4 Calculation semantics {#calculation}

- Five stages as §4.1; the aggregation scope of tiers and free allowances is the provider's (per account/region), applied
  once per variant; allocation back to cells is proportional to the cell's share of the unit and shown.
- **Feasible, minimal provisioning.** Automatic provisioning minimises monthly block-storage cost over the supported
  layouts, subject to the registered capacity, performance, headroom and topology constraints and the provider's dated
  coupling constraints; the billing artifact defines the candidate layouts and a deterministic tie-breaking rule. The same
  objective applies to every variant, including the remote variants' local volumes. Explicit manual allocations are priced
  as entered and identified as manual; impossible manual settings are rejected with the reason; multi-volume allocation is
  explicit.
- **Rounding.** Observed ledger counts and counts predicted for a concrete deterministic execution are integers. Monthly
  expected event and request quantities keep fractional values through billing; event frequencies and probabilities are
  never rounded. Discrete construction rules (parts per upload, volumes per replica) use explicit rounding nodes in the model
  contract. Money is rounded only for display.
- The same typed expression structure (§6) is evaluated for numbers and rendered for "show calculation", including the
  branch taken and the allocation.

### 4.5 Output contract {#output}

- Four summary cards (total, deltas against EBS and `s3`, stacked bar by unit class); the breakdown table `workload type ×
  variant`; cell drill-down with composition and the five-stage calculation; notes (EBS allocation and instance class,
  local disk per replica, unsupported or out-of-range inputs); the evidence statement; a sensitivity chart (total per
  variant against one swept input, break-even points marked, including where CAS is not cheapest).
- **Provenance and uncertainty** on every coefficient in the drill-down: evidence kind, artifact and version, measurement
  record, validity domain, tolerance.
- **Reproducible URLs**: effective inputs plus model, contract, registry, coverage and price-preset versions. A URL whose
  versions are not present renders "cannot reproduce: version X not present", never a different number.
- Not-comparable columns per §4.3; "cheapest" is never applied to one.
- Out-of-range status propagates to every dependent total, savings delta and sensitivity-chart point; these outputs stay
  available but are visibly labelled extrapolated. "Cheapest" and break-even annotations describe model estimates, not an
  ordering established beyond measurement uncertainty.

### 4.6 Billing contract requirements {#billing}

The billing artifact (§5.1) holds the dated rules; this section fixes what it must cover and the assumptions it may make.

- Tier 1 assumes: S3 Standard, unversioned general-purpose buckets, bucket-owner pays, no unrelated usage, no credits, no
  KMS, no VPC-endpoint or NAT charges; EBS gp3 only; in-region EC2↔S3 transfer free; cross-AZ EC2↔EC2 per GB per direction;
  internet egress 0 by default. Other configurations are unsupported, not approximated.
- Requests: per-class prices; the outcome mapping (which HTTP status / S3 error code / ownership case is billed) is taken
  from the dated AWS document and an outcome the artifact does not list is billed. No blanket exemption for any status.
- Storage: tiered per GiB-month at the provider's scope; incomplete multipart parts are storage from acceptance until
  completion or abort; the configured lifecycle abort threshold is an eligibility rule, and the residence time used in the
  model is a separate assumed or measured quantity, labelled.
- Block storage: capacity, provisioned IOPS and throughput with the dated baseline, limits and coupling constraints; the
  feasible allocation algorithm of §4.4.
- Every rule carries the document URL and the date it was checked.

## 5. Research programme {#research}

### 5.1 Artifacts {#artifacts}

Each artifact is versioned, has an owner (workstream), required contents, acceptance evidence, dependencies, and a Codex
review within its scope. A change to an artifact reopens its dependents.

| Artifact | Owner | Required contents | Acceptance |
|---|---|---|---|
| `measurement-contract.md` + `ledger.schema.json` | phase 0 | ledger proxy schema (per attempt: id, timestamps start/end, caller identity, parsed S3 operation with multipart id/part number/ETag, copy source and size, batch keys with per-key outcomes, upstream store outcome separate from injected downstream outcome, exact status and error code, payload and wire bytes); lifecycle replay rules: objects identified by store/bucket/key and incomplete parts by store/bucket/key/upload-id/part-number, replacement affecting only the matching identity; destination object identity recorded for every modifying request and both source and destination for COPY; completion and abort transitions; failed conditions leave state unchanged; bounded timing uncertainty; each replay declares its initial object and incomplete-part inventory with evidence or replays setup from a known empty state; observer-originated mutations (such as the rig's abort of a probe's multipart upload) stay in lifecycle replay even though their requests are excluded from workload billing; measurement boundaries and the treatment of lifetimes crossing them are explicit; counter-to-ledger mapping with overlaps listed; interserver byte measurement (dedicated-port capture or transparent proxy, directional, by endpoint, excluding client/Keeper/S3); local-volume I/O sampling for every variant; per-card completion conditions and background policy; multi-process layout (up to three servers, embedded Keeper, per-replica prefixes, shared CAS pool); fault injection | phase-0 probes (§5.3) replay end to end; review closed |
| `inputs-registry.json` | phase 0 seed, R1–R5 grow it | every quantity any expression references: id, kind (input / pinned / measured / derivation), scope, unit, default or rule, validity domain, evidence reference | dependency closure (§5.2) |
| `coverage.json` | phase 0 seed (§5.6), R1–R5 grow it | term → card(s) → role (fit / hold-out / validation) → priority; every measured P1 coefficient names a fit dataset and a separate hold-out; integrated hold-outs list the coefficients they validate; excluded branches listed | evidence and support closure |
| cards (`utils/cas-yardstick/yardstick/cards/*.py`) | R1–R5 | executable definitions: DDL, seeded generator, settings, operation sequence, completion condition, included maintenance, assertions that the intended shape and branches occurred (part type, object sizes, branch evidence from event fields and decoded placement, not event kinds alone) | runs; assertions pass |
| operation-graph reports, one per variant + lifecycle (`docs/superpowers/reports/2026-10-<dd>-storage-cost-<variant>.md` + `measurements.json`) | R1–R5 | the graph per operation with source references at the pinned commit; the settings and constants it depends on with effective values; predictions vs measurements per card; gaps and causes; capability/reuse matrix rows for the variant; coefficients with provenance; the per-variant validation of shared lifecycle quantities (amplification, acquisition split) | exact-integer reconciliation on deterministic cards; hold-out gaps within stated tolerance; review closed |
| `billing-contract.json` | phase 0 | §4.6 rules with URLs and dates; candidate layouts, cost-minimising allocation algorithm and tie-breaking rule; pricing fixtures with exact expected money, including several feasible allocations whose selected one is verified against independently calculated alternatives | pricing fixtures pass |
| `model-contract.json` + `storage-cost-model.md` | phase 4 | §6; fixtures with fractional monthly event frequencies verifying expansion into expected request quantities without intermediate rounding; a growing-backlog fixture verifying suppression of the total, deltas and cheapest mark | all closure checks; review closed |

### 5.2 Closure checks {#closure}

Run by checked-in scripts; all must pass before the page ships, and the page's test suite runs them.

1. **Dependency closure:** every expression reference resolves to a registry entry; units and scopes agree (dimension
   check); derivations are acyclic; defaults and overrides resolve deterministically.
2. **Support closure:** every enabled variant × operation × workload family has an applicability entry and evidence;
   cutting a card disables every dependent coefficient and combination and invalidates presets that need them (§5.5);
   unavailable states (not supported, backlog grows, cannot reproduce) suppress total, deltas and cheapest mark, verified
   by fixtures.
3. **Evidence closure:** every measured P1 coefficient has provenance, validity domain, fit data and independent hold-out
   evidence; pinned constants cite source or document; deterministic branches have exact fixtures.
4. **Accounting closure:** ledger replay reproduces request counts, lifecycle inventories and byte-hours with bounded
   error; attribution is evidenced per card; billing fixtures verify classes, units, tiers, provisioning and allocation.
5. **Reproduction closure:** immutable identities for evidence, model and prices; generated artifacts match a fresh
   render; evaluation is repeatable; unavailable versions fail explicitly.

What the checks cannot catch — a branch omitted from both a graph and the coverage artifact — is covered by two retained
practices: the per-variant **source-to-graph audit** in the Codex review of each graph report, and the **integrated
hold-outs** (natural replication, long run) whose gaps expose missing branches.

### 5.3 Phases and gates {#phases}

| Phase | What | Gate |
|---|---|---|
| 0. Setup and acceptance slice | binaries; source at the pinned commit; RustFS behind the ledger proxy; yardstick extensions (multi-process, `local` policy, completion conditions, ledger/lifecycle integration, fault injection, external startup timing); registry, coverage and billing seeds; `measurement-contract.md` | **Executable acceptance slice**: four probes run end to end through all five stages and the page's drill-down — one `s3` insert reconciled to the exact integer; a two-process replicated fetch attributed sender/receiver with directional bytes; a proxy-injected failed multipart upload with the predicted residue, aborted by the rig; one CAS garbage cohort reclaimed with its reclaim delay; and at least one probe exercising local-volume accounting: attributed local I/O with request-size information and an observed capacity peak with its measurement bound, carried through provisioning, pricing and drill-down, with the device/filesystem, cache/writeback boundaries, excluded rig activity and any host-specific limitation recorded (static pricing fixtures do not satisfy this probe). The multipart probe retains at least two distinct parts and replaces one before the abort, verifying coexistence and replacement accounting. Runtime of the full coverage seed estimated from these plus a sample of the largest natural-merge and GC cards, recorded in the ledger; cuts per §5.5 if over budget |
| 1. Lifecycle definitions (R5) | executable workload definitions; part-type selection, parts per insert, amplification rule and fit on one variant; mutation and deletion shapes; acquisition-vs-execution rule and settings | R5 report v1 reviewed |
| 2. Calibration (R1–R4) | graphs from source; isolated cards; exact-integer reconciliation; per-variant validation of lifecycle quantities | reports v1 reviewed |
| 3. Integrated validation | hold-outs; multi-process natural replication; GC under load; recovery; long run; stochastic tolerances from observed variation; fresh hold-outs where a hold-out drove a change | hold-out gaps recorded |
| 4. Consolidation | model contract, formula document, page tests | closure checks pass; review closed |

Workstreams: R1 EBS (`local` policy: I/O request size distributions per operation, conversion to EBS accounting, peak
factor, capacity; dedicated device or host-specific label); R2 `s3`; R3 `s3_plain_rewritable` (capability matrix,
replication validity first); R4 CAS (entry-branch graph, acquisition and execution, key families, reclaim delay, GC terms
and scheduler semantics, lease, staging); R5 lifecycle (shared definitions; each variant validates its own values).

### 5.4 Method per artifact {#method}

1. **Source → graph** at the pinned commit, with a reference per branch and the dependency kinds marked in the registry.
2. **Graph → prediction** of ledger rows per card, before measuring.
3. **Measure** through the rig: ledger, per-server counters, system logs, local-volume I/O, interserver bytes.
4. **Reconcile.** Deterministic isolated cards to the exact integer per operation kind (a miss is a graph defect);
   stochastic terms with absolute + relative tolerance from observed variation over K repeats; background traffic reported
   separately, never subtracted to make a deterministic path exact; unexplained gaps stay open in the report.
5. **Review** (Codex, scoped) and close defects; a changed graph reopens dependents.
6. **Report** with `measurements.json` beside it.

### 5.5 Budget and cut semantics {#budget}

- Budget: about three workstation-days of card runtime for Tier 1, estimated at the phase-0 gate from measured probe
  durations with frozen per-card scale, repeats (K=3), completion timeout and cleanup; includes hold-outs and a rerun
  reserve. P2 cards are cut first, then (if still over) `replication-zero-copy`, then the combined complex-type shape;
  measurement integrity and lifecycle coverage are never cut.
- **Cutting a card disables every dependent coefficient and input combination; a non-zero dependent workload makes the
  column unavailable for that input, never free.** Presets must stay entirely within measured coverage; a preset that
  needs a cut card is removed or changed. Defaults for cut terms are not non-zero constants (a replacement rate stays 0 if
  `replacement` is cut). Cuts are recorded in the ledger and the coverage artifact.

### 5.6 Coverage seed {#coverage-seed}

The initial `coverage.json`, owned by phase 0 and grown by R1–R5. Each row still needs its fit/hold-out assignment per
coefficient in the artifact; the seed lists families and priorities only.

| Family | Cards (seed) | Variants | Pri |
|---|---|---|---|
| objects per part by shape | `narrow-compact`, `narrow-wide`, `wide-compact` (fit); `wide-wide`, `types-holdout` (hold-out); `types-fit` (fit for the complex-type coefficients) | remote | P1 |
| thresholds | `boundaries` (part-type and multipart thresholds, asserting the actual part-size sequence) | all | P1 |
| multipart graph | `big-insert-multipart` | remote | P1 |
| lifecycle | `small-inserts`, `merge-natural`, `merge-forced`; `async-inserts` (P2); `amplification-run` 2–4 h (validation, P2) | all | P1/P2 |
| mutation and deletes | `mutation` (compact and wide, lightweight delete), `drop` (partition, TTL, `DROP TABLE SYNC`), `detach-attach` (P2) | all | P1/P2 |
| reads and cache | `cold-select` (cache on/off; EBS device demand on `local`), `cache-eviction` (P2) | all | P1/P2 |
| startup and recovery | `startup`, `restart-catchup`, `replacement` (P2) | all | P1/P2 |
| replication | `replication-fetch`, `replication-local` (fit); `replication-natural` (hold-out); `replication-natural-fit` (fit for the acquisition split); `replication-zero-copy` (`s3`) | all | P1 |
| faults | `fault-put-5xx`, `fault-lost-response`, `fault-multipart-abort` (proxy injection); `fault-fetch` (P2) | remote / all | P1/P2 |
| CAS branches | `cas-branches` (fresh, hash hit, adoption, inline; staging peak), `drop` on CAS (foreground ref removal) | CAS | P1 |
| CAS GC | `gc-lifecycle` (cohort reclaim delay by stage), `gc-idle` (scheduler and heartbeat semantics on a populated pool), `gc-scale` (pool objects × mounts × ref growth on the supported topology; the P1 subset is the one-dimensional object sweep, the rest P2) | CAS | P1/P2 |
| CAS lease | `lease` | CAS | P1 |
| local volumes | sampled on every card of the remote variants | remote | P1 |

Excluded in Tier 1: S3 staging backend, shards > 1, io2/st1, the 26.6 binary, GCS/Azure values, KMS, VPC endpoints,
Keeper, versioned buckets, namespace layouts other than those the supported tables and replica roots generate.

## 6. Model contract {#formula-doc}

`docs/superpowers/cost-model/model-contract.json` (versioned) and `docs/superpowers/cost-model/storage-cost-model.md`.

- The contract holds the registry, the coefficients with provenance, the graphs and formulas as a **small typed
  expression structure** (`input`, `coef`, `const`, `add`, `sub`, `mul`, `div`, `min`, `max`, `ceil_to`, `floor_to`,
  `branch` with comparisons on registered quantities, `sum_over`, `allocate`, `price`), dimensions on every node,
  applicability per variant × operation, billing rules by preset with dates, the evidence statement, and versions of every
  artifact it was built from.
- One evaluator produces numbers and "show calculation"; nothing that changes a displayed number lives outside the
  contract.
- The prose document's formula blocks are rendered from the contract by a script with `generate` and `check` modes; the
  check compares the standalone JSON, the embedded copy and the rendered blocks against a fresh render.
- "Break-even regions": sweeps of the model with the presets that reproduce each claim; nothing enters before the
  producing formulas passed review.

## 7. The calculator page {#spa}

- Files: `utils/storage-cost-calculator/index.html`, `model.js` (evaluator and renderer), `ui.js`, `presets.js`,
  `model-contract.json` (generated, also embedded so `file://` works), `test/` (Node). Vanilla JavaScript, no framework,
  no bundler.
- Tests: measured fixtures from `measurements.json` within stated tolerance; pricing fixtures with exact money; the five
  closure checks; UI states (not comparable, out of range, overridden, non-reproducible URL, unsupported input).
- Layout (chosen from mockups 2026-10-23): one screen, two panels. Left, sticky: presets, simple inputs as sliders with
  editable values, advanced accordions showing current values and lock/override state. Right: four cards, breakdown table,
  cell drill-down with the five-stage calculation, notes, evidence statement, sensitivity chart.
- Look: product-grade. Design tokens on `:root`, light and dark, phone width works, thousands separators, the preset's
  currency; one accent, one green for "cheapest" (never on a not-comparable column), one amber for notes; four stable
  colours for unit classes shared by card bars and composition bars.
- Presets: scenarios "Logs", "Metrics", "Analytics batch", "Single replica archive", "Custom" — each within measured
  coverage; prices "AWS us-east-1 (as_of)", "GCS" and "Azure" as structure labelled "values not provided" (Tier 2),
  "Custom".
- Hints: per advanced block, the SQL that fills its fields and a column → field mapping.

## 8. Blog support {#blog}

In scope to enable, not to write: sensitivity view, versioned URLs, the break-even section, the evidence statement. The
draft series on the `cas-blog-article` branch is re-checked against the model; disagreements are listed in the formula
document's "known inaccuracies". This branch does not edit the blog branch.

## 9. Repository layout and branch {#layout}

Worktree `~/workspace/ClickHouse/storage-cost-calc`, branch `docs/storage-cost-calculator` (base `cas-gc-rebuild`).
Paths: this design; ledger and reports under `docs/superpowers/reports/2026-10-23-storage-cost-calculator/` and
`docs/superpowers/reports/`; artifacts and the model under `docs/superpowers/cost-model/`; the page under
`utils/storage-cost-calculator/`; rig changes in `utils/cas-yardstick/`; binaries and the 26.8 source under `tmp/` (not
committed). Commits per deliverable, signed off, exact files only; no push without an explicit request.

## 10. Left to the builder {#builder}

Every implementation fact (graphs, settings, constants, counter names, scheduler semantics, multipart sizing, part-type
rule, acquisition rule, namespaces, key families) and every coefficient; the ledger proxy tool and fault injection; the
interserver capture method; the dedicated device or host-specific label; card files; preset rules and ranges; visual
design within §7; the `generate`/`check` script; the closure-check scripts.

## 11. Tier 2 {#tier2}

"Next release (forecast)" toggle (candidate differences between the pinned 26.6 and 26.8 builds, established by Tier-2
research, each labelled with its measured 26.6 effect and the unmeasured interaction, excluded from validated savings;
requires measuring 26.6 on the same cards); GCS and Azure values;
io2/st1; S3 staging; shards > 1; other namespace layouts; versioned buckets; KMS and VPC charges. Owner's note (ledger
D5): the forecast was the owner's preferred headline before review.

## 12. Review history and convergence decision {#reviews}

| Date | Reviewer | Findings | Disposition |
|---|---|---|---|
| 2026-10-23 | Builder check | 11 questions | folded as decisions |
| 2026-10-23 | External review (pasted) | 8 | all applied (revision 2) |
| 2026-10-23 | Codex `gpt-6-astra` r1 (log not retained in the repository) | 3 CRITICAL, 21 MAJOR, 1 MINOR; REVISE | applied (revision 2) |
| 2026-10-23 | Codex r2 (log not retained in the repository) | 16 MAJOR, 2 MINOR; REVISE | applied as revision 3 (Tier 1/2 scope freeze, registry, coverage table) |
| 2026-10-23 | Codex r3 (log not retained in the repository) | 12 MAJOR, 3 MINOR; REVISE | code-facing claims of revision 3 were wrong against the pinned source (part-type rule, multipart sizing, CAS acquisition vs execution, namespaces, interserver counters, gp3 coupling, versioning). Verified by the author against the pinned source; recorded in the ledger (A3) |
| 2026-10-23 | Codex r4 (log not retained in the repository) | 3 MAJOR, 3 MINOR; REVISE | all applied: cost-minimising provisioning with tie-break (§4.4), growing backlog = unavailable column (§4.3), rounding semantics (§4.4), local-volume probe and multipart part replacement in the acceptance slice (§5.3), lifecycle identity precision (§5.1), remaining implementation assertions removed (§0, §4.3, §11) |
| 2026-10-23 | Codex r5 (log not retained in the repository) | 0 MAJOR, 2 MINOR; **READY** | both applied: out-of-range status propagates to totals, deltas and chart (§4.5); lifecycle replay initial state and observer mutations (§5.1) |
| 2026-10-23 | Convergence consult, Codex `gpt-6.1-sol` (log not retained in the repository) | option C | **Revision 4 (this document):** the design keeps the product contract (supported domain, input semantics, accounting invariants, calculation and output contract, billing requirements, artifact contracts and gates) and makes no claim about the code; implementation facts move to versioned research artifacts with five closure checks and an executable acceptance slice at the phase-0 gate. r3 findings 4, 6, 7, 10, 11, 12, 14, 15 are design-level and are addressed in §4.3–§4.6, §5.2, §5.5; findings 1, 2, 3, 5, 8, 9, 13 are research-level and are now obligations of the artifacts in §5.1. Weakened per the consult: universal "derived from source and verified" → evidence policy (§3); accuracy promise scoped to validity domains (§1); Tier 2 "no formula changes" → versioned extensibility (§0); fixed review-round count → review as budget, exit by evidence (§3) |

## 13. Decisions after the spec {#decisions-after}

These rulings in [LEDGER.md](../reports/2026-10-23-storage-cost-calculator/LEDGER.md#decisions) change or sharpen this design. The ledger gives each reason and consequence.

| Ruling | Change |
|---|---|
| D14 | A 36-hour deadline cut every P2 card (§5.5); cut quantities are listed in `coverage.json` |
| D15 | `formulas/1` gains an optional per-family `applicability`; an omitted family is `not_applicable` |
| D16 | EBS IOPS use the provider's merged-stream rate, a lower bound; `peak_factor` is an assumed input |
| D17 | EBS deletes, restarts, reads and capacity are assumption rows; mutations are measured |
| D18 | The metrics preset stays outside EBS coverage and shows `extrapolated` by declaration |
| D19 | Presets without mutation workloads are append-only; the CAS multipart cut needs blobs above 32 MiB |
| D20 | One fix wave after the review; the raw-artifact split and several minors move to Tier 2 |
| D21 | Inputs get page tiers (`workload`, `setting`, `advanced`, `hidden`), labels of at most 40 characters and an (i) dialog |
| D22 | Workload knobs alone price all four variants; column states reduce to measured, `extrapolated`, `not_supported`, `prices_missing`, `cannot_reproduce`; this supersedes the fail-closed cut rule of §5.5 |
| D23 | Low-level inputs leave the default form; at most 10 settings |
| D24 | Formulas come from the source and measurements verify them; validity becomes "verified range"; derived formulas never extrapolate |
| D25 | The EBS column shows volume size, IOPS and throughput against the gp3 baseline |
