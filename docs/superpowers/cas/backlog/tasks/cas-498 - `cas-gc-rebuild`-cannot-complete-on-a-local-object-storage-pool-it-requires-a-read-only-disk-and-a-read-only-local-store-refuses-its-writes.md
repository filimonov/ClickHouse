---
id: CAS-498
title: >-
  `cas-gc-rebuild` cannot complete on a local-object-storage pool: it requires a
  read-only disk, and a read-only local store refuses its writes
status: To Do
assignee: []
created_date: '2026-11-18 11:02'
updated_date: '2026-11-21 08:22'
labels:
  - 'origin:testing'
  - 'confidence:solid'
  - 'area:tooling'
  - 'complexity:medium'
  - 'risk:medium'
  - 'impact:operability'
  - 'needs:decision'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-high'
  - grooming-20261121-changed
dependencies: []
priority: medium
type: bug
ordinal: 603000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Found while testing `cas-migrate-pool` (branch work/antalya-26.8/cas-blob-intents, report master/.superpowers/sdd/2026-11-17-cas-blob-deletion-intents/task-13-fix1-report.md, item 6). `clickhouse-disks cas-gc-rebuild` refuses a writable disk config ('open the CA disk read-only'); with `<readonly>true</readonly>` on a local object storage its first write fails at once: 'CAS gc lease: the store refused the write: ... readonly (READONLY)'. S3 `readonly` is advisory, so S3 pools are unaffected; the same shape was fixed for `cas-migrate-pool` by dropping the read-only requirement (its protection is the deferred pool open). `cas-gc-rebuild` mounts nothing either? to be checked: it takes the GC lease, so the decision differs. Also: on a freshly migrated pool `cas-gc-rebuild` refuses with 'another GC leader holds the lease' until a server's background loop has taken the lease (owner 0 after migration; `acquireOrRenewLease` with allow_steal=false). `cas-drop-member` keeps the same read-only requirement: check it too.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Decision recorded: which protection keeps cas-gc-rebuild (and cas-drop-member) from acting as a pool member, and whether the read-only requirement stays
- [ ] #2 The command completes on a local-object-storage pool, or the docs say it is S3-only, with a test either way
- [ ] #3 A freshly migrated pool: the documented order of rebuild versus first server start is tested
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:22
---
Grooming 2026-11-21. Verdict: valid (high). Evidence: programs/disks/CommandCaGcRebuild.cpp:23-52 still requires a read-only-opened disk and errors 'open the CA disk read-only'; task reports the local store then refuses the first lease write. Changes: label area:tools -> area:tooling; size unset -> medium (M): needs a protection decision (AC #1) plus change and tests; risk set to medium; impact:operability; label needs:decision; type unset -> bug: the tool cannot complete on a supported pool kind. Size: M. Owner question: Which protection keeps cas-gc-rebuild and cas-drop-member from acting as a pool member, and does the read-only requirement stay (as cas-migrate-pool dropped it)?
---
<!-- COMMENTS:END -->
