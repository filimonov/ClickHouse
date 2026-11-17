---
id: CAS-490
title: >-
  Decide whether the reader-floor raise needs a staging switch for rolling
  upgrades
status: To Do
assignee: []
created_date: '2026-11-15 14:38'
updated_date: '2026-11-17 20:02'
labels: []
dependencies:
  - CAS-487
ordinal: 595000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
OWNER DECISION. One writable mount of a generation 3 build, including `clickhouse-disks cas-drop-member`, raises `min_reader_generation` to 3. Older builds then refuse the pool at their next mount. Today the procedure is to stop all older servers first. Decide whether to add a switch that stages the raise. Same decision family as CAS-410. Follow-up of CAS-487.
<!-- SECTION:DESCRIPTION:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-11-15: this is a question for the owner, not work to schedule: decide whether a rolling upgrade to generation 3 must be possible without stopping older servers first. If the answer is no, close this task; the upgrade page already states the procedure.

2026-11-17: PR 2831 body and docs/en/antalya/cas/operations/migration.md state: stop all older servers first. This task closes if the owner confirms "no rolling upgrade".
<!-- SECTION:NOTES:END -->
