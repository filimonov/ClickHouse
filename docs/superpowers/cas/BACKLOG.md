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
| `BACKLOG/ref-protocol.md` | migrated to Backlog.md (docs/superpowers/cas/backlog), see `backlog task list -l area:ref-ledger` | — |
| `BACKLOG/gc.md` | migrated to Backlog.md (docs/superpowers/cas/backlog), see `backlog task list -l area:gc` | — |
| `BACKLOG/mounts-and-lifecycle.md` | migrated to Backlog.md (docs/superpowers/cas/backlog), see `backlog task list -l area:mounts` | — |
| `BACKLOG/gcs.md` | migrated to Backlog.md (docs/superpowers/cas/backlog), see `backlog task list -l area:gcs` | — |
| `BACKLOG/formats-and-storage.md` | migrated to Backlog.md (docs/superpowers/cas/backlog), see `backlog task list -l area:formats,area:backend` | — |
| `BACKLOG/replication.md` | migrated to Backlog.md (docs/superpowers/cas/backlog), see `backlog task list -l area:replication` | — |
| `BACKLOG/testing-and-ci.md` | migrated to Backlog.md (docs/superpowers/cas/backlog), see `backlog task list -l area:testing,area:ci,area:soak` | — |
| `BACKLOG/operability-and-introspection.md` | migrated to Backlog.md (docs/superpowers/cas/backlog), see `backlog task list -l area:observability` | — |
| `BACKLOG/performance.md` | migrated to Backlog.md (docs/superpowers/cas/backlog), see `backlog task list -l area:write-path,area:read-path,area:backend` | — |
| `BACKLOG/docs-and-cleanup.md` | migrated to Backlog.md (docs/superpowers/cas/backlog), see `backlog task list -l area:docs,area:upstream` | — |
| `BACKLOG/issue-2310.md` | migrated to Backlog.md (docs/superpowers/cas/backlog), see `backlog task list -l area:replication,area:upstream` | — |

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
