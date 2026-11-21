---
id: CAS-335
title: >-
  Test CASIOTestAwsS3Client.ReadBufferFromS3AttemptSeedCarriesAcrossLocalRetry
  fails on loopback connect timeout under host load
status: To Do
assignee: []
created_date: '2026-10-01 02:34'
updated_date: '2026-11-21 08:20'
labels:
  - ci
  - tests
  - 'theme:ci-stabilization'
  - 'area:testing'
  - 'complexity:small'
  - 'risk:low'
  - 'confidence:plausible'
  - 'impact:operability'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
dependencies: []
priority: low
type: chore
ordinal: 427000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Observed 2026-10-01 on the CAS-30 branch gate (gc-request-cuts worktree, HEAD bce54128ca3, diff unrelated to the S3 client): the test failed 3/3 in isolation at load average ~6 with a loopback connect timeout, then passed 10/10 at load 0.4 and in the full rerun. The test's connect timeout is too tight for a loaded host; either widen it or make the scripted server accept before the client connects. Not a CAS defect; a gate-flake class.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The test passes 20/20 under an artificial load (stress -c 8) or its timing dependency is removed
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
created: 2026-11-21 08:20
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: Test still exists: src/IO/S3/tests/gtest_cas_aws_s3_client.cpp (ReadBufferFromS3AttemptSeedCarriesAcrossLocalRetry); no commit touching it since the cherry-pick of #2300; the file has no explicit connect timeout, so the timing knob sits in the shared client or fixture. Changes: area set to testing; size unset -> small (S): find the timeout, widen it or accept before connect; risk set to low; confidence set to plausible; impact:operability; type unset -> chore: test flake fix, no behaviour change. Size: S. Pointers stale: task cites no file path; the test is in src/IO/S3/tests, not src/Disks/tests. Would settle it: Where the loopback connect timeout is set (fixture or S3 client config) and whether any later run still flakes.
---
<!-- COMMENTS:END -->
