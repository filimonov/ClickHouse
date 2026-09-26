---
description: 'Consolidated live backlog of all still-pending CAS MergeTree work items. Index of the topic files under BACKLOG/, plus the Inbox append target for quick adds. Issue IDs preserved (never renumbered).'
sidebar_label: 'CAS Backlog (live)'
sidebar_position: 9
slug: /superpowers/cas/backlog
title: 'CAS MergeTree — Live Backlog (pending issues)'
doc_type: 'guide'
---

# CAS MergeTree — Live Backlog {#cas-backlog}

Live backlog: only open work. History and removed entries live in git; verification record in
`consolidation-2026-08/`.

This file is the **index**. The backlog itself lives in per-topic files under
[`BACKLOG/`](BACKLOG/), split 2026-08-04 so a reader (or grep) can go straight to one subsystem
instead of scanning one flat multi-hundred-item file. Item format is uniform:
`- **[id] Title** — PRIORITY — 1-3 lines: current status / the open ask / evidence pointer` (a few
genuinely-live long designs keep structured detail below their header line instead of being
compressed). Issue IDs are never renumbered.

**Blob-publication baseline since 2026-08-23.** Every blob decision begins with `HEAD`; an absent or
`Condemned` body is published unconditionally, while metadata/control objects retain native
conditional operations for create-if-absent and conditional replacement. The [real-storage gate](/superpowers/cas/unconditional-blob-publication-live-results)
and [performance gate](/superpowers/cas/unconditional-blob-publication-performance) are both still
blocked for the external/evidence reasons recorded in those reports. Backlog text below marked
historical or closed must not be read as the current body-publication API.

## Topic files {#topics}

| File | Items | Covers |
|---|---|---|
| [`BACKLOG/ref-protocol.md`](BACKLOG/ref-protocol.md) | 20 open | Rev.6 lease-boundary exclusivity, the ref-lane state machine, ref-ledger internals. Top items: `[PART-WRITE-RELEASE-SEAM]`, `[MOUNT-CLAIM-EPOCH-REGRESSION]`, `[cas-txn-commit-inside-noexcept-aftercommit]`. |
| [`BACKLOG/gc.md`](BACKLOG/gc.md) | 66 open, 3 closed records | GC scalability & byte cost, correctness/observability follow-ups, throughput-collapse and fsck-vs-GC RCAs. Top items: `[gc-frontier-one-list]`, `[GC-DEFER-DECISION-LIST-COST]`, `[gc-rebuild-lease-interlock]`. |
| [`BACKLOG/mounts-and-lifecycle.md`](BACKLOG/mounts-and-lifecycle.md) | 31 open | Mount-lease/fence recovery, CA disk lifecycle (rev.8 residuals), pool bootstrap, operator recovery. Top items: `[POOL-REFUSAL-NODE-FATAL]`, `[operator-replica-readd-uuid-trap]` (2026-09-04 field report: remove/re-add a replica under a k8s operator has no supported recovery), [`decommission-toctou-stamps-successor`](BACKLOG/mounts-and-lifecycle.md#decommission-toctou-stamps-successor) (merged home of the former `[decommission-successor-mount-race]`), disk-lifecycle-leak (deferred, prose section). |
| [`BACKLOG/gcs.md`](BACKLOG/gcs.md) | 7 open, 1 closed record, fix order | CAS on Google Cloud Storage: the live gate's unrun arms, the relink-confirm livelock, the hot-control-key 429 class, environment findings of 2026-09-02, observability/docs items, the recommended fix order. Top items: `[relink-confirm-lane-livelock]`, `[gcs-hot-control-keys-429]`, `[gcs-live-gate-oauth-and-ambiguity]`. |
| [`BACKLOG/formats-and-storage.md`](BACKLOG/formats-and-storage.md) | 39 open, 4 closed records | Staging/adoption, real-store backends (S3/GCS/Azure), the local/emulated backend, codec/format items. Top items: `[GATE #1: Azure]`, `[B66a]` concurrent-fetch torn read on local storage, `[sec4-decoder-size-bounds]`. |
| [`BACKLOG/replication.md`](BACKLOG/replication.md) | 9 open, 5 closed records | `MOVE PART`/`PARTITION` onto CA disks (landed, residuals open), merge/insert retry vs. the mount-lease fence, cross-replica relink, read-only replicas. Top items: `[merge-progress-reset-mount-fence]`, `[same-pool-move-reads-every-byte]`, `[zero-copy-parity-audit]`. |
| [`BACKLOG/testing-and-ci.md`](BACKLOG/testing-and-ci.md) | 38 open, 2 closed records | Test coverage & harness, gate-filter/gate-suite gaps, soak/chaos hygiene, standing testing-methodology rules. Top items: `[unconditional-blob-publication-live-gate]`, `[cas-tests-unchecked-optional-deref]`, `[4h-continuous-chaos-soak]`. |
| `BACKLOG/operability-and-introspection.md` | migrated to Backlog.md (docs/superpowers/cas/backlog), see `backlog task list -l area:observability` | — |
| [`BACKLOG/performance.md`](BACKLOG/performance.md) | 56 open, 4 closed records | Read/write path, write-path optimization candidates, stage 2 (postponed), scalability findings from the full-scale campaign. Top items: `[ckpt-read-policy]`, `[ref-catalog-write-hotspot]`, stage-2 concurrent commitPart (postponed). |
| [`BACKLOG/docs-and-cleanup.md`](BACKLOG/docs-and-cleanup.md) | 28 open, 4 closed records | Architecture/refactoring (no behavior change), minor/polish, source-layout residue, standing hygiene checklist items. Top items: `[refactor: CasGc split]`, `[Group G]` upstream carve-outs, `[cas-changelog-entry-missing]`. |
| [`BACKLOG/issue-2310.md`](BACKLOG/issue-2310.md) | 2 open | Issue CLOSED 2026-09-14 (gate passed on #2300 run 10, fix in 26.6.4.20001). Triage of Altinity/ClickHouse#2310 (`ATTACH PARTITION FROM` stalls on relink confirm): the verdict that it is `[relink-confirm-lane-livelock]` on a pre-fix package, and the two items that stayed open. Items: `[attach-partition-cas-relink-residency]`, `[s3-empty-file-multipart-retry]`. |

Items counted 2026-09-26: an item is a top-level bullet or section that carries an `[id]`, or an id-less leaf section
that is not narrative; a closed record is one marked DONE, CLOSED, OBSOLETE or SUPERSEDED and kept for provenance.

Priority legend: **GATE** = release gate; **HARD** = agreed-necessary, not yet done; **DESIRABLE** =
valuable, not committed; **DOC** = documentation debt; **TEST/INFRA** = validation/harness/CI;
**MINOR** = small concrete improvement; **VERIFY** = believed open, confirm before working. These
seven are the canonical set; individual items also carry more specific free-form qualifiers where an
item's author wanted to say something the seven don't capture (`QUESTION`, `DESIGN QUESTION`, `WATCH`,
`GAP`, `INFRA`, `LOW`/`LOW-PRI`, `PARTIAL`, `MEASURED`, `IN PROGRESS`, `TRACKED, by design`, and
similar) — read those as elaborating one of the seven, not as a competing taxonomy.

**2026-08-04 orphaned-open triage merge:** 367 open-verdict clusters from the docs-consolidation
corpus were 4-way classified; 54 effective new/still-open findings (57 minus 3 rechecked and closed
by design) were merged into the topic files above, each marked with a `## New findings from the
2026-08-04 orphaned-open triage` heading; 35 duplicates were folded into their existing matching item
as a confirmation note rather than inserted separately. Full triage record:
`.superpowers/sdd/2026-08-03-cas-docs-map-reduce-consolidation/orphan-triage-final.md`.

## Inbox {#inbox}

Groomed 2026-09-25 (unit `u1-inbox`): every item that was here has been triaged — closed with evidence, folded into an existing item, or moved into a topic file under `BACKLOG/`. See the commit that applied this grooming pass (`docs(cas): groom BACKLOG.md`) for the disposition and evidence of each item.

Append new items here — quick adds and concurrent-agent findings land in this section, unformatted is fine. They get triaged into the topic files above during the next grooming pass. Do not delete from here without triaging; do not hand-sort into a topic file without checking the item's anchor isn't referenced elsewhere first.

- **[cas-keep-alive-recommendation-backport]** — port the CAS keep-alive recommendation (`http_keep_alive_timeout=30`/`http_keep_alive_max_requests=10000`, `05e7fc2bdee`) from `cas-gc-rebuild` to `altinity/antalya-26.6`. Queued by the u11-specs-c grooming unit (`tmp/groom/u11-specs-c/backlog-entries.md` item 2).
