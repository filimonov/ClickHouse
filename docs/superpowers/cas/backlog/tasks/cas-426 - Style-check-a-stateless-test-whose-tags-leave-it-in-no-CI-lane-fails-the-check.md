---
id: CAS-426
title: >-
  Style check: a stateless test whose tags leave it in no CI lane fails the
  check
status: To Do
assignee: []
created_date: '2026-11-03 22:08'
labels:
  - 'area:ci'
  - 'area:testing'
  - 'complexity:small'
  - 'risk:low'
  - 'confidence:solid'
dependencies: []
priority: medium
type: chore
ordinal: 529000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
05217_cas_packed_parts was tagged `cas-storage` + `no-parallel` in PR 2704 and ran in no CI job: `cas-storage` is skipped without `--cas-storage` (clickhouse-test:3240), every CAS stateless lane in `ci/defs/altinity_jobs.py` is a `parallel` lane, and parallel lanes pass `--no-sequential`, which drops `no-parallel` tests (clickhouse-test:4620). The skip is reported as a pass, so nobody noticed until a manual look at the report (PR 2704 audit comment, 2026-10-28; found by k-morozov 2026-11-03). Fixed for 05217 by moving its packed tables to a test-owned disk; nothing stops the next test.

**Design (simulate the lanes with the real `clickhouse-test`, no grep over tags)**
- New `ci/jobs/scripts/check_style/check_test_lanes.py`, registered in `ci/jobs/check_style.py` next to `check_embedded_doc_snippets`.
- Lanes: every `Stateless tests (...)` job from `ci.defs.job_configs.JobConfigs` and `ci.defs.altinity_jobs`; drop lanes that do not run the whole suite (`selected tests`, `targeted`, `flaky check`); collapse `N/M` batches. Lane tokens map to flags through `ci/jobs/functional_tests.py:OPTIONS_TO_TEST_RUNNER_ARGUMENTS`.
- Oracle: load `tests/clickhouse-test` as a module (SourceFileLoader, 0.08 s), build `args` per lane with its own `parse_args(flags)`, set `args.build_flags` from the build token (`amd_asan_ubsan` → ADDRESS+UNDEFINED, `amd_tsan` → THREAD, `amd_msan` → MEMORY, `amd_debug` → DEBUG, `*_binary` → RELEASE, `llvm_coverage` → WITH_COVERAGE), then `TestCase(suite, case, args, is_concurrent).should_skip_test(suite)`. The `--no-sequential` / `--no-parallel` split lives in the runner, not in `should_skip_test`: model it as `parallel_only` / `sequential_only` against the `no-parallel` tag.
- Verdict: a test with an empty lane set and no `disabled` tag fails the check. Message names the tags and the nearest lane and why it drops the test.
- Out of scope for v1 (false 'runs' only, never false 'skipped'): `queries-no-cloud-tests.txt` / private skip lists, `use_*` build-feature tags.
- Side output: lanes-per-test histogram, to spot tests covered by one lane only.
- Cost: ~16k tests × ~20 lanes, seconds. Fork-only today (Altinity lanes), shape upstreamable as is.

Precedent: `ci/jobs/scripts/check_style/various_checks.sh:199-218` (`SYSTEM DROP` requires `no-parallel`), grep-based; this one needs the tag semantics, hence Python.

Origin: PR 2704 AI audit 2026-10-28 (finding 2, regression-test direction); https://github.com/Altinity/ClickHouse/pull/2704
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The check fails on a copy of 05217 tagged `no-parallel, cas-storage` and passes once `no-parallel` is removed (both cases in the check's own self-test)
- [ ] #2 The check passes on the current tree, or every test it reports is fixed or tagged `disabled` in the same PR
- [ ] #3 Lane list and flag mapping are read from `ci.defs` and `ci/jobs/functional_tests.py`, not duplicated in the check
- [ ] #4 Skip decisions come from `clickhouse-test`'s own `should_skip_test`; the only modelled rule outside it is the `--no-sequential` / `--no-parallel` split
- [ ] #5 Style check job runtime grows by under 30 s
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
