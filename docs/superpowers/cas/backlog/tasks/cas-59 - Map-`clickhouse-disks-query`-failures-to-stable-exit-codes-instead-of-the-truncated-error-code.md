---
id: CAS-59
title: >-
  Map `clickhouse-disks --query` failures to stable exit codes instead of the
  truncated error code
status: To Do
assignee: []
created_date: '2026-08-22'
updated_date: '2026-11-21 08:22'
labels:
  - 'area:tooling'
  - 'area:upstream'
  - 'complexity:trivial'
  - 'risk:low'
  - 'touches:upstream-code'
  - 'confidence:solid'
  - 'origin:review'
  - 'upstream:fixed-master'
  - groomed-20261121
  - 'groom-20261121:rewrite'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
milestone: m-5
dependencies: []
references:
  - programs/disks/DisksApp.cpp
  - 'https://github.com/ClickHouse/ClickHouse/pull/82348'
priority: medium
type: bug
ordinal: 73000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`DisksApp::main` returns the raw ClickHouse error code as the process status (`programs/disks/DisksApp.cpp:622-623`), and POSIX keeps 8 bits.
Codes 256, 512 and 768 exist (`PARTITION_ALREADY_EXISTS`, `SET_NON_GRANTED_ROLE`, `CANNOT_EXECUTE_PROMQL_QUERY`, `src/Common/ErrorCodes.cpp:220,419,650`)
and exit 0: a failure that scripts read as success. Other codes above 255 report a mangled value.
The ca-soak harness and the CAS integration tests gate on this exit code. Map failures to a small set of stable nonzero codes before the change is carved out upstream.

Provenance: BACKLOG/operability-and-introspection.md#disks-exit-code-truncation item 1 (opus review M6); verified 2026-09-26 against 59494ebf366 (cas-gc-rebuild) and 0dbbd797792 (altinity/antalya-26.6).

Update 2026-11-21: The multiple-of-256 case (exit 0 on failure) is fixed by upstream PR 82348 and present in DisksApp.cpp. What remains: map failures above 255 to a stable small code set, document it and test it (AC #2).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Every failing `--query` batch exits nonzero, including one whose error code is a multiple of 256
- [ ] #2 The exit-code set is documented in the code and tested
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
First recorded: 2026-08-22 (f07a9ed680d, by 'disks-exit-code-truncation')
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:22
---
Grooming 2026-11-21. Verdict: rewrite (medium). Evidence: programs/disks/DisksApp.cpp:720,731 return `static_cast<UInt8>(code) ? code : 1`, so a code that is a multiple of 256 no longer exits 0 (AC #1); the same lines exist upstream (blame: merged PR 82348). Codes above 255 still reach the OS truncated, and AC #2 (documented, tested exit-code set) has no sign of work. Changes: upstream reference added; description: Update paragraph appended. Would settle it: whether a test of the exit code exists (grep tests/ for clickhouse-disks exit codes). Upstream: fixed-master (https://github.com/ClickHouse/ClickHouse/pull/82348); already here.
---
<!-- COMMENTS:END -->
