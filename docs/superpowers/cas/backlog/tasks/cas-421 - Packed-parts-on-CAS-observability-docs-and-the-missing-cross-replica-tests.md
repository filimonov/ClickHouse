---
id: CAS-421
title: 'Packed parts on CAS: observability, docs and the missing cross-replica tests'
status: To Do
assignee: []
created_date: '2026-10-23 01:50'
labels:
  - cas
dependencies: []
ordinal: 524000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
From the packed-parts review (tmp/review3/REPORT.md items 8 and 10). Observability: the Packed->Full downgrade is silent (no log line, nothing in system.disks shows allowsPackedParts). Docs: cas_allow_packed_parts gates INSERT/merge/mutation output only; fetch, ATTACH PARTITION FROM, MOVE, restore and relink preserve Packed (docs list only some); a table's min_bytes_for_full_part_storage has no effect on a CAS disk without the key; the unprefixed legacy spelling allow_packed_parts is accepted by new binaries and ignored by old ones, so 'an older binary rejects the key' holds for the cas_ spelling only; the destination-disk rule is written three times. Tests missing: replicas disagreeing on the setting (fetch of a Packed part by a replica with it off; merge/mutation on disagreeing replicas, hash_of_all_files compared), restart with the key removed while Packed parts exist, the Packed part left on a forbidding disk read back / CHECK TABLE / FREEZE / BACKUP, a merge of mixed Packed+Full sources, projections and skip indexes in the destination matrix, 05301 proving MODIFY SETTING took effect, GC/FSCK after DROP in the integration test.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A log line (and a system.disks column or cas_mounts column) makes the effective allowsPackedParts and each downgrade visible
- [ ] #2 configuration.md states the gate's scope once, lists the format-preserving paths, and drops the legacy-spelling upgrade claim
- [ ] #3 Two-node integration test with differing cas_allow_packed_parts: fetch, merge, mutation; hash_of_all_files equal across replicas
- [ ] #4 Stateless: Packed part on a forbidding disk survives restart, CHECK TABLE, FREEZE and BACKUP; mixed Packed+Full merge; projection + skip index in the destination matrix
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
