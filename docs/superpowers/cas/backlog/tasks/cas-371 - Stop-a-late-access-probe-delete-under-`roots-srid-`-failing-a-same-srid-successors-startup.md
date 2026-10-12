---
id: CAS-371
title: >-
  Stop a late access-probe delete under `roots/<srid>/` failing a same-srid
  successor's startup
status: To Do
assignee: []
created_date: '2026-10-12 11:51'
labels:
  - 'area:mounts'
  - 'complexity:trivial'
  - 'confidence:plausible'
  - 'risk:low'
  - 'origin:noclock-audit'
milestone: m-8
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPlainObjects.cpp
  - docs/superpowers/cas/2026-10-12-late-request-audit/objects/roots.md
priority: low
type: bug
ordinal: 465000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Only disk-root paths land in `roots/<srid>/`; the real writer is upstream `IDisk::checkAccess` (`clickhouse_access_check_<ServerUUID>`, always `test`). On S3 the body never changes, so `If-Match` is blind (ETag ABA): a zombie's late delete removes the successor's fresh probe and its `IDisk::startup` throws (RS4; next restart works). Late creates leave debris that makes `serverRootSubtreeEmpty` false (RS1, RS14, RS16). No data loss. Fix: detector (~50 lines: leftover/foreign probes after claim, distinct error when the probe vanishes, re-LIST after decommission drain, `roots/` in fsck); epoch-scoped probe key if RS4 is ever seen (~25 lines). Details: docs/superpowers/cas/2026-10-12-late-request-audit/objects/roots.md. Found by the late-request audit of base `d76b59f9295` (`altinity/antalya-26.6-20261008`) on 2026-10-12; harm is inferred from code unless marked verified.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 probe vanishing right after write has a distinct error
- [ ] #2 fsck reports `roots/` debris
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
