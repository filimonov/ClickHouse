---
id: CAS-480
title: >-
  Hard restart after SIGKILL waits about 131 s with the 120 s mount lease:
  regression suites time out at 60 s
status: Done
assignee: []
created_date: '2026-11-13 09:05'
updated_date: '2026-11-17 20:01'
labels:
  - 'area:lease'
  - 'origin:ci'
  - 'area:ci'
  - 'theme:ci-stabilization'
dependencies: []
priority: high
ordinal: 585000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
PR https://github.com/Altinity/ClickHouse/pull/2831 @a3967637843, CI run 43379266824: RegressionTestsAarch64 CAS (atomic_insert) 131784879929, CASS3Cache (atomic_insert) 131784881457, CASLightweightDelete (2) 131784881286, CASS3CacheLightweightDelete (2) 131784881281 fail the hard restart / SIGKILL scenarios. Root cause (report tmp/prbundle/wave1/ci2/hard-restart.md): after SIGKILL the restarted server waits out the old mount lease before it listens ('a stale-looking mount lease ... observing its write-token for up to ~131000 ms'); the regression framework's start_clickhouse(timeout=60) (helpers/cluster.py of Altinity/clickhouse-regression) gives up, starts a second server, which dies on 'Cannot lock file /var/lib/clickhouse/status'. With the 30 s lease the wait was 36.5 s and the same eight jobs passed (runs 43248330323, 43246617309). The x86 twins are expected to fail the same way. Remedies: pin cas_mount_lease_ttl_ms=30000 in cas_storage_config (helpers/cas_storage.py of the regression repo); or pass a start timeout of 150 s or more in the two hard-restart tests; or keep the 30 s default; or make the server listen before it mounts CAS disks. Owner decision, same question as the mixed-lease pool. Also unchecked: test_cas_gcs and test_cas_replicated_relink kill servers on a CAS disk without a lease pin.
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
2026-11-13: PR https://github.com/Altinity/ClickHouse/pull/2831 @a3967637843, run 43379266824: the x86 twins failed the same way: RegressionTestsRelease CAS (atomic_insert) 131803913516, CASS3Cache (atomic_insert) 131803913046 (both 'hard restart with big insert' 2m 1s and 'with small insert'), CASLightweightDelete (2) 131803912946, CASS3CacheLightweightDelete (2) 131803912697. Reproduced locally against the same tree: tmp/regression/logs/atomic_insert_cas_baseline.log.

2026-11-13: same run 43379266824: Stress test (arm_release) 131829639805 and Stress test (arm_debug) 131829639895 fail with 'Cannot start clickhouse-server': the server process is alive (pgrep finds it) but port 9000 refuses connections for 11 polls of 10 s, then the lane gives up. Consistent with the lease wait before listening (about 131 s at the 120 s default against the lane's 110 s start budget); not yet confirmed from the server log (the job log has no 'token-stability' line; the server log is in the job artifacts). Related: CAS-322, CAS-355 (the stress lanes' restart budget was already tight at the 30 s lease). The stress lanes were skipped in the previous run of this PR; compare with the base-branch run.

2026-11-13: PR https://github.com/Altinity/ClickHouse/pull/2831 @a3967637843, run 43379266824 (triage tmp/prbundle/wave1/ci2/triage.md): more jobs caused by the 120 s lease default (commit b528b234dca). (1) LightweightDelete (3): jobs 131803912863 and 131784881279: the framework's add_config(restart=True) sends TERM and kills the server after 3 s; the restart takes 135 s; a second server overwrites the pid file; later stops do nothing and the 300 s config-reload waits time out. (2) Integration test_backup_restore_on_cluster::test_shutdown_waits_for_backup in jobs 131775013725, 131829639566, 131829639864 (cascade: test_replicated_table_after_alters): the module mounts a CAS disk on all three nodes through configs/cas_storage.xml without a lease pin; the node is killed after the helper's 60 s stop wait, the restarted server waits ~131000 ms, start_clickhouse(start_wait_sec=60) gives up; the rerun passes; both tests passed on base and on the previous run. (3) Stress arm_release 131829639805 CONFIRMED from the server log: forced shutdown after 60 s with 24 open connections, two CAS disks created by stateless test 05387 (pc3a, pc3b) each wait ~131000 ms against the lane's 110 s start budget. Correction of the earlier note: Stress arm_debug 131829639895 has NO CAS disk (0 CasPool lines; the server was still loading tables after 109 s); it is not this class. Remedies as facts: a 30 s pin in the regression repo's cas_storage_config (verified locally, tmp/regression/REPORT.md) covers (1) and the eight hard-restart jobs; (2) needs a pin in tests/integration/test_backup_restore_on_cluster/configs/cas_storage.xml; (3) needs a pin in 05387 or a release of the lease on the forced-shutdown path (CAS-355 option a) or a larger start budget.

2026-11-17: Merged 2026-11-17 as 0aa837e942e into antalya-26.8 (https://github.com/Altinity/ClickHouse/pull/2831). Resolved in PR 2831 by `feb86da1f4a`: the restart after a kill waits about 47 s instead of 131 s, and all hard-restart regression jobs passed on `44fc40023c1`. The stateless and integration test configs skip the wait (`802077e5c1a`, `aee132f03c4`).
<!-- SECTION:NOTES:END -->
