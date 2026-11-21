---
id: CAS-42
title: >-
  Close the build allocate-to-register window that lets a dropped namespace come
  back without an owner
status: To Do
assignee: []
created_date: '2026-07-18'
updated_date: '2026-11-21 08:26'
labels:
  - 'area:write-path'
  - 'area:gc'
  - 'complexity:small'
  - 'risk:medium'
  - 'confidence:plausible'
  - 'needs:repro'
  - 'origin:review'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
milestone: m-8
dependencies: []
references:
  - CA/Pool/CasPool.cpp
  - CA/Pool/CasMountRuntime.h
priority: low
type: bug
ordinal: 52000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`Pool::beginPartWrite` allocates the build sequence before registering the build for `dropNamespace`'s cancellation
(`CA/Pool/CasPool.cpp:1729,1736`). A drop landing in that window misses the build, which can later revive a `Live` but
ownerless empty ref table that GC never sweeps. Non-self-healing, narrow. Not re-checked since namespaces became
incarnation-keyed lives (`Pool::namespaceLife` mints a new life when the catalog names none).
Fix candidates: a GC backstop, or a namespace-life check in the birth-time gate.

Provenance: BACKLOG/gc.md [codex-11]; verified 2026-09-26 against aefe80eba98 (cas-gc-rebuild) and altinity/antalya-26.6.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A test with a drop injected between allocation and registration either reproduces the ownerless namespace or proves it impossible
- [ ] #2 If reproduced, the fix makes the late build fail closed and the test passes
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
First recorded: 2026-07-18 (73d58405952, by 'codex-11')
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:26
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: Pool::beginPartWrite (CasPool.cpp:1953-1961) still allocates the build seq before registerInflightBuild; a SCOPE_EXIT retires the seq only if registration is skipped; no life check in this window. Changes: none. Pointers stale: CasPool.cpp:1729,1736 are now :1953,:1960. Would settle it: whether a drop in the window can still revive an ownerless Live ref table under incarnation-keyed lives (Pool::namespaceLife); needs a repro.
---
<!-- COMMENTS:END -->
