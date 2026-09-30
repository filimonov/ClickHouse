---
id: CAS-329
title: >-
  GC blob chain request cuts: blind marker create, exact-token DELETE without
  HEAD (CAS-30 PR2)
status: To Do
assignee: []
created_date: '2026-09-30 18:58'
updated_date: '2026-09-30 18:58'
labels:
  - cas
  - gc
dependencies:
  - CAS-30
priority: medium
ordinal: 418000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Two of the 7 requests per reclaimed blob exist only for the form of the code, not for safety, and go away as PR2 of the CAS-30 series (spec `docs/superpowers/specs/2026-09-30-cas-gc-meta-delete-names-its-condemnation-design.md`, r2):

- Request 2, `GET .meta` before the condemn marker write: replace `writeCondemnedMeta`'s read-then-write by `op.create` (If-None-Match: *; the write loop already settles a refusal with a body read): committed -> done; refused and observed Condemned -> done; refused and observed Clean -> `replace` with the observed etag. Common case (no marker) 1 request instead of 2.
- Request 5, `HEAD blob` before the exact-token delete (`performRedeleteIo`): reconstruct the live `Etag` from `PersistedEtag{dialect, value}` + the current backend id + key and issue `op.remove(key, etag)` directly; the backend already classifies Removed / TokenMismatch (=> Replaced) / NotFound (=> Absent). Check the round trip for every dialect (`etag`, `generation`, `emulated`).

Each is its own commit; A/B on the gc-ab stand (`utils/ca-soak/gc-ab`) in a GC-only window: HEAD and GET per reclaimed blob, and round time with 20 ms latency.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Per reclaimed blob on the happy path: 1 HEAD, 1 GET (graduation), 1 PUT, 2 DELETE — measured on the gc-ab stand
- [ ] #2 No fallback path: a refused create is settled by the write loop's own read; an unknown token form is a LOGICAL_ERROR, not a HEAD
- [ ] #3 TLA model `CaGcCondemnMarkerGate` updated: the blind create and the direct conditional delete keep every invariant of the r2 spec
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
