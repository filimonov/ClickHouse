---
id: CAS-360
title: >-
  Stop a writer adopting a blob body that GC deleted between the writer's `HEAD`
  and its marker `GET`
status: To Do
assignee: []
created_date: '2026-10-12 11:51'
labels:
  - 'area:gc'
  - 'area:write-path'
  - 'complexity:small'
  - 'confidence:solid'
  - 'risk:high'
  - 'origin:noclock-audit'
milestone: m-8
dependencies: []
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.cpp
  - src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.cpp
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcMetaWriter.cpp
  - docs/superpowers/cas/2026-10-12-late-request-audit/objects/meta.md
  - docs/superpowers/cas/2026-10-12-late-request-audit/objects/blob.md
priority: critical
type: bug
ordinal: 454000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`PartWriteTxn::ensureBlobPresent` (`Pool/CasPartWriteTxn.cpp`) reads the body with `HEAD`, then the `.meta` marker with a separate `GET`. When the marker is absent it backfills `Clean` and returns `Observed`, with no second `HEAD`. GC (`Gc/CasGc.cpp:performRedeleteIo`, `applyRedeleteOutcome`) deletes the body by exact token and immediately schedules `scheduleConfirmedMetaDelete` in the same pass.

Schedule: blob is `delete_pending`; writer W precommits after the delete pass's cut (so `settleEntry` does not see its edge), W's `HEAD` lands before the body `DELETE`, GC deletes the body and then the marker, W's `GET` finds no marker, adopts the deleted body. Dangling ref; no pause needed, only a `GET` a few RTT late (first-attempt timeout resend, load, thread preemption). Verified in code by three independent codex runs (docs/superpowers/cas/2026-10-12-late-request-audit/atomicity/w1-blob.md, w1-reflog.md, w1-catalog.md); through relink it also breaks `ConfirmedRelinkNeverDangles`.

Models missed it because `CaGcCondemnMarkerGate:WAdopt` and `CaBlobPublishCore:ObserveMeta` read body presence and the marker in one step.

Fix (no format change): delete the marker one round after the body. Round R deletes the body and carries the entry as `delete_pending`; round R+1 settles it against fresh in-degree: `> 0` spares it (marker stays `Condemned`, the writer republishes), `= 0` does `HEAD` (expect `Gone`), deletes the marker synchronously and only then drops the entry. Clock-free: W's precommit precedes W's `HEAD`, which precedes the body delete, which precedes R+1's cut, so W's edge is in R+1's in-degree. Also closes the orphan-marker leak (failed advisory marker delete drops the entry today) and the false fsck `meta_without_body` ERROR.

Alternative considered: re-`HEAD` before adopting when the marker is absent (5–10 lines); relies on ETag uniqueness, which SDK resends break.

Details: docs/superpowers/cas/2026-10-12-late-request-audit/objects/meta.md (S9), docs/superpowers/cas/2026-10-12-late-request-audit/objects/blob.md (S13), docs/superpowers/cas/2026-10-12-late-request-audit/objects/SUMMARY.md#orphan-meta. Found by the late-request audit of base `d76b59f9295` (`altinity/antalya-26.6-20261008`) on 2026-10-12; harm is inferred from code unless marked verified.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `CaGcCondemnMarkerGate` splits `WAdopt` into `WHead`/`WMetaRead`; TLC shows the violation with the same-pass marker delete and holds with the deferred delete
- [ ] #2 gtest with a hook between the writer's `HEAD` and marker `GET`: GC deletes body and marker; the writer republishes instead of adopting
- [ ] #3 a failed marker delete is retried next round (entry dropped only after the marker delete is confirmed); fsck reports `meta_without_body` as a warning
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 CAS* gtest gate green (utils/cas-gate); LOGICAL_ERROR expectations are death tests
- [ ] #2 ASan lane green for touched suites
- [ ] #3 Docs updated where user-visible (docs/en/antalya/cas) and the spec if the on-S3 format is touched (frozen since 26.6.4: new version + compatibility path)
- [ ] #4 No fallback paths added; failures propagate
<!-- DOD:END -->
