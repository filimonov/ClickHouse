---
id: CAS-510
title: >-
  Stateless CAS-S3 lanes: run the recommended cached configuration, keep one
  uncached lane
status: To Do
assignee: []
created_date: '2026-11-20 22:22'
updated_date: '2026-11-21 08:24'
labels:
  - ci
  - tests
  - 'area:ci'
  - 'complexity:small'
  - 'risk:low'
  - 'confidence:plausible'
  - 'impact:operability'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-high'
  - grooming-20261121-changed
dependencies: []
priority: medium
type: enhancement
ordinal: 615000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The stateless CI lanes for CAS on S3 run MergeTree on the bare `cas_s3` disk: `tests/config/config.d/cas_s3_storage_policy_for_merge_tree_by_default.xml` defines the cache disk `cas_s3_cache` (filesystem cache, 5 GiB, over `cas_s3`) and its policy, but `<merge_tree><storage_policy>` points at `cas_s3`. The comment in that file gives the reason: the suite is mostly single-pass, the cache adds a one-time populate cost and routes large single-pass writes through the cache.

Why revisit:
- The user docs (`docs/en/antalya/cas/configuration.md`, `quick-start.md`) recommend the CACHED form; the bare form is described as the test configuration. The stateless suite therefore does not exercise what users are told to deploy; only the regression jobs `cas_s3_cache_*` do.
- Merges and mutations re-read recently written data, so 'single-pass' understates the benefit.
- The ordinary S3 lane's policy `s3` is on `cached_s3` (`s3_storage_policy_by_default.xml`), so the two lanes are not comparable today.

What the bare lane is good for (keep it): it found a real problem a cached lane hides: a horizontal merge of `system.metric_log` (2425 columns) held one S3 connection per column per source part (up to 11400, warning 'Too many active sessions in group Disk'): analysis in `tmp/intents/ci-20261120/sessions/RCA.md`; fixed by backporting upstream ClickHouse/ClickHouse PR 119730.

Proposal: the default policy of the CAS-S3 stateless lanes becomes `cas_s3_cache` (a one-line change in the config), and ONE lane (for example `amd_binary, cas s3 storage, parallel`) stays on bare `cas_s3` (a lane parameter or a second config file selected by the job).

Do not do it inside PR 2917: it would mix a configuration change with the protocol change and make lane durations before / after incomparable.

Acceptance: (1) measured durations of each CAS-S3 lane before and after on the same commit; (2) the uncached lane still runs the full suite; (3) tests that assert request counts or cache state on `cas_s3` are identified and pinned to the policy they need; (4) the comment in the config file states the new default and why one lane stays uncached; (5) system logs stay on a CAS policy (the file's existing rule: do not route them to the local disk).
<!-- SECTION:DESCRIPTION:END -->

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
created: 2026-11-21 08:24
---
Grooming 2026-11-21. Verdict: valid (high). Evidence: cas-pr1 tests/config/config.d/cas_s3_storage_policy_for_merge_tree_by_default.xml:95 still has <storage_policy>cas_s3</storage_policy> (bare, not cas_s3_cache). Changes: area set to ci; size unset -> small (S): one config line plus a lane parameter and a before/after duration measurement; risk set to low; confidence set to plausible; impact:operability; type unset -> enhancement: CI covers the recommended configuration; priority unset -> medium: no priority set; stateless suite does not exercise what the docs recommend. Size: S. Links: Not inside PR 2917 (per task).
---
<!-- COMMENTS:END -->
