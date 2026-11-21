---
id: CAS-311
title: >-
  Report on the GC leader how many live precommit bindings belong to writer
  epochs that can no longer drain them
status: To Do
assignee: []
created_date: '2026-09-26 14:44'
updated_date: '2026-11-21 08:25'
labels:
  - 'area:observability'
  - 'area:gc'
  - 'complexity:small'
  - 'risk:low'
  - 'confidence:plausible'
  - 'needs:spec'
  - 'origin:soak'
  - 'impact:operability'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-medium'
  - grooming-20261121-changed
milestone: m-7
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasOrphanManifestSweep.cpp
  - src/Common/ProfileEvents.cpp
  - utils/ca-soak/scenarios/cards/s12_s14_faults.py
documentation:
  - docs/en/antalya/cas/operations/monitoring.md
priority: low
type: enhancement
ordinal: 390000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Stale precommit bindings are cleaned only by their own writer: the stale-precommit sweep retries until clean and emits
`precommit_reclaim` events (fixed after the 2026-07-13 S13 dangling-precommit regression). GC never mutates a writer's ref table.
While a binding lives it keeps its manifest body active for the orphan sweep (`CA/Gc/CasOrphanManifestSweep.cpp:217-218`).
A writer that dies again, or stays wedged, before a verified-clean sweep leaves those bindings with no second line of defense,
and nothing on the leader shows it: the existing counters (`CASRefSweepDeferred`, `CASRefSweepRearmed`,
`CASRefStalePrecommitsReclaimed`, `src/Common/ProfileEvents.cpp:823-825`) count only on the writer.
Cheap first step, no protocol change: count per round the live precommit bindings whose writer epoch is below the current
mount epoch of that server, and show it in the GC log or `system.cas_mounts`. A GC-side reclaim of such bindings is a new
protocol capability and needs a spec revision of the responsibility boundary; it is not part of this task.

Provenance: utils/ca-soak/scenarios/BACKLOG.md#S13-20260713T172032-3 (triage .superpowers/sdd/s13-triage-report.md Q4 recommendation 3); verified 2026-09-26 against 66087be0ffb (cas-gc-rebuild) and 8d62c314ec1 (altinity/antalya-26.6). Related: CAS-52 (undrained writer cleanup duties pin the build floor).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A GC round records the number of live precommit bindings older than their server's current mount epoch, per namespace or in total
- [ ] #2 A test with a writer killed after `precommitAdd` and never remounted shows the count nonzero until the binding is reclaimed
- [ ] #3 The operator docs say what a persistent nonzero value means and which action clears it
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
created: 2026-11-21 08:25
---
Grooming 2026-11-21. Verdict: valid (medium). Evidence: cas-pr1 Gc/: CasOrphanManifestSweep.cpp:316 still reads getPrecommits() only to keep manifests active; grep for stale_precommit / dead_epoch / precommit_bindings in Gc/*.cpp finds no GC-side count of dead-epoch bindings. Changes: impact:operability. Pointers stale: CasOrphanManifestSweep.cpp:217-218 (precommit handling now at :316 and :657). Would settle it: Not checked whether the blob-intent rework changed the precommit model (the task's premise that bindings keep a manifest active at CasOrphanManifestSweep.cpp:217-218) or whether a counter exists outs….
---
<!-- COMMENTS:END -->
