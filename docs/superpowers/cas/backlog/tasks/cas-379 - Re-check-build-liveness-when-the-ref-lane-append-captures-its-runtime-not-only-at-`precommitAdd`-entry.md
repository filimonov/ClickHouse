---
id: CAS-379
title: >-
  Re-check build liveness when the ref-lane append captures its runtime, not
  only at `precommitAdd` entry
status: To Do
assignee: []
created_date: '2026-10-12 11:52'
labels:
  - 'area:ref-ledger'
  - 'area:write-path'
  - 'complexity:small'
  - 'confidence:plausible'
  - 'risk:high'
  - 'needs:repro'
  - 'origin:noclock-audit'
milestone: m-8
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.cpp
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefLedger.cpp
  - tmp/noclock/atomicity/w2-ref.md
priority: high
type: bug
ordinal: 473000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`precommitAdd` passes `requireAlive`; cancellation or namespace rebirth happens before the generic append captures its runtime; the old closure can then grant ownership in the successor life. Promotion has a matching epoch-check gap. Code steps verified; committed-data loss inferred. tmp/noclock/atomicity/w2-ref.md (finding 2). Found by the codex model-atomicity audit (wave 2) on 2026-10-12; code steps verified by codex, harm as marked.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 verdict on reachability
- [ ] #2 liveness and life/epoch are checked under the same lock that captures the runtime
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
