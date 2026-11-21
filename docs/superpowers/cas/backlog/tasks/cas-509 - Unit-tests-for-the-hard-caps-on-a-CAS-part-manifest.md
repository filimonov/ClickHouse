---
id: CAS-509
title: Unit tests for the hard caps on a CAS part manifest
status: To Do
assignee: []
created_date: '2026-11-20 08:28'
updated_date: '2026-11-21 08:24'
labels:
  - cas
  - tests
  - writer
  - 'area:testing'
  - 'complexity:medium'
  - 'risk:low'
  - 'confidence:solid'
  - 'impact:operability'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-high'
  - grooming-20261121-changed
dependencies: []
priority: medium
type: task
ordinal: 614000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Six hard caps in `stageManifest` (`src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.cpp`, about line 615) fail a write closed with `LIMIT_EXCEEDED` before the manifest body is written. No user can reach them (the entry cap needs about 524,000 columns; a 20,000-column insert did not finish in 20 minutes; files over 1 MiB spill to blobs; a record line is bounded by the 255-byte file name), so no soak scenario covers them: scenario card S07 was retired on 2026-11-20 by the owner's decision (no failpoints in soak scenarios; a condition a user cannot reach belongs to unit tests). Unit coverage today:

| cap | value | unit test |
|---|---|---|
| entries | 1048576 | none |
| one inline entry | 1 MiB | none |
| inline total | 16 MiB | none |
| manifest ordinal | 999999 (`CasTypes.h`) | none (one test checks ordinals 1 and 2) |
| encoded bytes | 256 MiB (`CasFormat.cpp`) | `ManifestCapEncodedBytesJustUnderStagesSuccessfully`, `...OverThrowsBeforeBodyWrite` in `src/Disks/tests/gtest_cas_part_write.cpp`: boundary on both sides; 'nothing durable' checked only as an unchanged key count over the first 100 listed keys |
| record line | 64 KiB (`CasPartManifestFormat.cpp`) | `StageManifestRejectsOversizedEntryRecordBeforeBodyWrite`, `CASPartManifestFormat.EncodeEnforcesEntryRecordLineCap`, `...DescriptorLineCap`: refusal only; no 'exactly at the cap is accepted' case |

To do, in `gtest_cas_part_write.cpp` under `CASPartWriteTxn`: (1) `StageManifestEntryCountCap`: 1048576 entries accepted, 1048577 refused; (2) `StageManifestInlineEntryCap`: one inline entry of 1 MiB accepted, 1 MiB + 1 byte refused; (3) `StageManifestInlineTotalCap`: 16 entries of 1 MiB accepted, 16 MiB + 1 byte refused; (4) `StageManifestOrdinalCap`: the 999999th stage accepted, the next refused (needs about a million real stages on the in-memory backend, or an existing way to start a build at a high ordinal; a test-only setter only if nothing else exists); (5) strengthen the encoded-bytes 'over' test; (6) add 'exactly at the cap accepted' for the record line. Every refusal asserts: `LIMIT_EXCEEDED`; nothing durable (the WHOLE manifest prefix listed and empty, no precommit, no ref); the same writer then stages, precommits and promotes a small manifest. Measure each boundary case; keep the suite fast (cheapest honest construction for the million-entry cases). When done, replace the retirement reason of card S07 in `utils/ca-soak/scenarios/cards/s06_s08_manifest_parts.py` and the line in `utils/ca-soak/scenarios/README.md` by the real test names. Full analysis as written on 2026-11-20: `tmp/intents/scen/S07-retire.md` (scratch; the table above is the content that matters).
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
Grooming 2026-11-21. Verdict: valid (high). Evidence: cas-pr1 src/Disks/tests/gtest_cas_part_write.cpp contains none of StageManifestEntryCountCap, StageManifestInlineEntryCap, StageManifestOrdinalCap (grep count 0). Changes: area set to testing; size unset -> medium (M): six cap tests incl. a million-stage ordinal case kept fast; risk set to low; confidence set to solid; impact:operability; type unset -> task: unit test coverage. Size: M.
---
<!-- COMMENTS:END -->
