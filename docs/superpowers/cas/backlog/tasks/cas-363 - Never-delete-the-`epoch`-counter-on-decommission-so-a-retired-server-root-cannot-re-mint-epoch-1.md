---
id: CAS-363
title: >-
  Never delete the `epoch` counter on decommission so a retired server root
  cannot re-mint epoch 1
status: To Do
assignee: []
created_date: '2026-10-12 11:51'
updated_date: '2026-10-12 21:30'
labels:
  - 'area:mounts'
  - 'area:tooling'
  - 'area:gc'
  - 'touches:on-s3-format'
  - 'complexity:medium'
  - 'confidence:solid'
  - 'risk:high'
  - 'origin:noclock-audit'
milestone: m-8
dependencies:
  - CAS-158
references:
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasDecommission.cpp
  - >-
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.cpp
  - docs/superpowers/cas/2026-10-12-late-request-audit/objects/epoch-owner.md
priority: high
type: bug
ordinal: 457000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`Tools/CasDecommission.cpp` deletes `mount` and `epoch`, then tombstones `owner`. A restarting victim then mints epoch 1 again (empty subtree, mount probe `KeyAbsent`). Epoch reuse breaks the monotone-epoch premise of `prefixEligibleUnder` and the remount `chassert`, and with a delayed GC worker or a zombie it can lose data: an orphan sweep with a pre-reset floor deletes a new in-flight manifest (S7), a late zombie writes or deletes reused manifest keys (S8), MD5-ETag ABA on byte-identical manifests (S9); the `epoch` ETag itself can repeat (S4). `DecommissionRecovery`'s wall-clock check only burns an epoch and is not a takeover path (S13).

Fix (no migration): retire the slot by one `If-Match` write that replaces `epoch` with a retired body; keep the owner tombstone for old binaries (~70–90 lines). Owner tombstone over a live successor stays in CAS-158. Details: docs/superpowers/cas/2026-10-12-late-request-audit/objects/epoch-owner.md (S4, S6–S11), docs/superpowers/cas/2026-10-12-late-request-audit/objects/manifest.md (S9). Found by the late-request audit of base `d76b59f9295` (`altinity/antalya-26.6-20261008`) on 2026-10-12; harm is inferred from code unless marked verified.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `CaCasMountCore` splits the decommission tail and adds `EpochNeverReused`; TLC holds with the fix
- [ ] #2 decommission followed by a victim restart never yields an epoch below the retired one (integration test)
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
Wave 2 (docs/superpowers/cas/2026-10-12-late-request-audit/atomicity/w2-misc.md finding 2, verified): with the counter missing and the mount probe seeing absence, an older prepared starter creates the mount while counter recreation lands independently; the model's atomic absence-and-mint hides it. Keeping `epoch` (this task) removes the missing-counter state.

2026-10-12 model (`008991cc06f`): `cur_decom` re-mints epoch 1, `cur_decom_reusedangle` dangles via the farewell floor, `cur_decom_retired` violates `RetiredStaysRetired`. NEW: deleting `mount` is a hazard by itself (`cur_decom_dangle`/`cur_decom_slot`): a claimer reads the slot absent and pauses before its `If-None-Match` mint; decommission writes its farewell and deletes `mount`; the paused mint lands at a lower epoch and the sweep holding the farewell floor deletes its in-flight manifest — no epoch reuse needed. Fix (green in `fix_retire`, `fix_both` with nonce, 66 M states): keep the farewell `mount`, replace `epoch` by ONE `If-Match` write of a retired body (after the `captures_match` check), then the owner tombstone; `allocateWriterEpoch` refuses a retired body. `sab_retire_delmount` red. Spell the marker as a critical key (`!retired_at_ms`): 26.6.4 `decodeServerEpoch` throws `UNKNOWN_FORMAT_VERSION` on `!` keys, so old allocators and the old decommission tool fail closed on a retired root; a plain field would be ignored (`sab_retire`). Ship together with CAS-362.
<!-- SECTION:NOTES:END -->
