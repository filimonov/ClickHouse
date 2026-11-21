---
id: CAS-482
title: >-
  Restart after a kill without the full mount-lease wait: a per-disk local lease
  witness
status: To Do
assignee: []
created_date: '2026-11-13 14:29'
updated_date: '2026-11-21 08:26'
labels:
  - 'area:lease'
  - 'origin:ci'
  - 'complexity:epic'
  - 'needs:decision'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-high'
  - grooming-20261121-changed
dependencies: []
priority: high
type: design
ordinal: 587000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
A server restarted after SIGKILL or a forced exit waits the full mount observation threshold (about 131 s at the 120 s lease) for its own previous incarnation before it listens (CAS-480, CAS-477, CAS-355). The lease must stay 120 s. Design memo: tmp/prbundle/wave2/restart-memo.md (2026-11-13). Proposal: the holder keeps an exclusive flock on a per-disk witness file for its whole life and records locally, before every mount-slot write, boot_id, the witness (st_dev, st_ino) and (writer_epoch, write_attempt_id, attempt_start_boot_ms). On restart: same boot, same witness inode, lock acquired and the slot body named by the record prove the writer of the slot is dead: wait one write reserve (16 s, token unchanged) instead of the threshold (new MountPriorState::UncleanLocalDeath; the reclaim stays If-Match on the token). With the same boot and a record match alone, count the unchanged threshold from the holder's recorded renewal start on CLOCK_BOOTTIME. Every failed check keeps today's full wait. No store-format change, no shared-code change. Residual: a live memory clone (VM snapshot with memory) whose original is stalled, already unsupported. Not verified: lock and inode identity on ext4, xfs, overlay, NFS, FUSE, CSI volumes; whether st_dev survives a pod remount. Before code: extend CaMountObserveCore.tla and CaCasMountCore.tla. Owner decisions open: settle window 16 s or 0; a self-fence before the forced exit in programs/server/Server.cpp; on by default; the memory-clone residual; the in-process self-remount.
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
2026-11-13: SHELVED by the owner: no local state, lock or socket for the one narrow case of a hard restart. The design material stays for reference: memo tmp/prbundle/wave2/restart-memo.md; spec docs/superpowers/specs/2026-11-13-cas-mount-lease-witness-design.md (three revisions: flock witness, then an abstract unix socket; two review rounds in tmp/prbundle/wave2/review/). Results worth keeping: nothing executable depends on the previous lease having fully elapsed (recovery and GC read epochs and seals, never time; the comment at Backend/CasRequestBudget.h:73-78 is stale); the forced exit ('Will shutdown forcefully', programs/server/Server.cpp:4006-4018) releases no lease; the wait ignores the slot's age. What remains on the table without local state: releasing the lease on exit paths the server controls (CAS-355), the slot's age on the store's clock (CAS-483), and test-side handling of hard kills (CAS-480).

2026-11-13: variant 'two thresholds in one lease' examined and rejected. Idea: the holder stops writing after 30 s without a renewal, foreign fencers keep waiting 120 s, a same-uuid restart waits 30 s. Read at a3967637843: (1) revival after local expiry already exists: the production background renewal is `renewUntilDefinitive` on the open plane (`CasMountRuntime.cpp:637`, bounded only for decommission at `CasPool.cpp:1004`) and a late commit re-arms the deadline (`publishRenewedDeadline`, `CasMountRuntime.cpp:603-611`); (2) the declared lease is already in the slot as `expires_at_ms - started_at_ms` and the claimer waits max(own, declared) (`CasServerRoot.cpp:1202-1218`). Why rejected: the 120 s default exists for the write window, not for slot ownership. Commit b528b234dca: 'A 30 s lease leaves a 4 s write window in an outage ... 120 s ... so the window is 94 s' (envelope 7 s = 5 s attempt + 2 x 1 s connect cap; write reserve 2 envelopes + 2 s margin = 16 s; renew period 10 s; 30 - 10 - 16 = 4). A 30 s write window restores exactly the 4 s figure: a refused admission throws a transient error instead of riding through the outage inside the 120 s request window. The restart wait cannot be shorter than the time the old holder may keep writing without a renewal; shortening one shortens the other by the same amount. Ways left that do not trade the write window: proof of death (this task, shelved), store-side protection for R1-R4 of tmp/prbundle/wave2/lease-inventory.md, lease release on controlled exits (CAS-355), test-side pins.

2026-11-17: The default mount lease is 40 s since PR 2831 (https://github.com/Altinity/ClickHouse/pull/2831, merged as 0aa837e942e) commit feb86da1f4a (30 s -> 120 s -> 40 s). Earlier notes that name 120 s no longer describe the default. At 40 s a restart after a kill waits about 47 s and the write window in a store outage is 14 s. The rejection text "the 120 s default exists for the write window" needs the same correction.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:26
---
Grooming 2026-11-21. Verdict: valid (high). Evidence: cas-pr1 Pool/: no UncleanLocalDeath, lease witness or flock (grep empty); only the CLOCK_BOOTTIME anchor at Pool/CasServerRoot.h:648,:706. Changes: label needs:decision; type unset -> design: the task body asks for model extensions and owner decisions before code. Pointers stale: description assumes a 120 s lease (default now 40 s, feb86da1f4a); memo path tmp/prbundle/wave2/restart-memo.md not checked. Owner question: Settle window 16 s or 0; self-fence before the forced exit in programs/server/Server.cpp; on by default; accept the memory-clone residual; in-process self-remount?
---
<!-- COMMENTS:END -->
