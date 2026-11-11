---
id: CAS-452
title: Count hits and misses of the CAS manifest decode cache
status: To Do
assignee: []
created_date: '2026-11-11 12:59'
labels:
  - 'area:observability'
  - 'area:read-path'
  - 'touches:settings'
dependencies: []
priority: low
type: enhancement
ordinal: 556000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`cas_manifest_decode_cache_bytes` (128 MiB) is at its budget on otel.demo (26.6.8, 2026-11-11: `CASManifestDecodeCacheBytes` 134003276, 2204 entries, about 61 KB each), but no ProfileEvent counts its hits, misses or evictions, so nothing shows whether a larger cache would save requests. The only indirect signal: `CASPartFolderViewMisses` 663590 equals `CASPartFolderManifestGets` 663260, the first read of each new part. CAS-297 lists this default as doubtful (audit F25); the defaults review could neither confirm nor reject it. Report: docs/superpowers/reports/2026-11-11-cas-defaults-review.md (branch cas-297-defaults-review).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 ProfileEvents for decode-cache hits, misses and evictions exist and are documented
- [ ] #2 One day of otel.demo data decides whether 128 MiB stays
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
