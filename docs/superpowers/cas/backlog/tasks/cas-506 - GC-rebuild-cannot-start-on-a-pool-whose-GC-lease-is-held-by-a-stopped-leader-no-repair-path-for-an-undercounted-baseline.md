---
id: CAS-506
title: >-
  GC rebuild only as an exclusive clickhouse-disks command; remove SYSTEM CAS GC
  REBUILD
status: To Do
assignee: []
created_date: '2026-11-19 16:24'
updated_date: '2026-11-21 08:24'
labels:
  - cas
  - gc
  - rebuild
  - lease
  - 'area:gc'
  - 'complexity:large'
  - 'risk:high'
  - 'confidence:solid'
  - 'impact:operability'
  - 'impact:availability'
  - groomed-20261121
  - 'groom-20261121:valid'
  - 'groom-20261121:certainty-high'
  - grooming-20261121-changed
dependencies: []
priority: high
type: feature
ordinal: 611000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
OWNER DECISION 2026-11-19: the GC baseline rebuild makes sense only in exclusive mode inside `clickhouse-disks`; remove `SYSTEM CAS GC REBUILD` from the SQL surface completely, with every reference to it.

Why. The rebuild replaces the whole `gc/state`. As a command of a running server it cannot take the lease: `runGcRebuildNow` mints a fresh GC identity per call and `Gc::rebuildBaseline` calls `acquireOrRenewLease(allow_steal=false)` once, so it claims only an absent `gc/state` or the never-held lease; `FORCE` bypasses the health check, not the lease. By the code it answers 'another GC leader holds the lease' on every pool whose GC loop ever held the lease, a running server included (to be shown by an integration test). The same holds for `clickhouse-disks cas-gc-rebuild` today. In exclusive mode the lease question disappears: no leader exists by construction, no regular round races, and no new precommit appears.

Scope.
1. `clickhouse-disks cas-gc-rebuild [--force]`: establish exclusivity the way `cas-migrate-pool` does (every server stopped, mount slots fenced), then take the GC lease without observation and rebuild. Decide and state what it does with a precommit of a stopped writer whose manifest is unreadable (today: refuse, CORRUPTED_DATA).
2. Remove from SQL: the parser keyword and AST for `SYSTEM CAS GC REBUILD`, `InterpreterSystemQuery::runContentAddressedGcRebuild`, `ContentAddressedMetadataStorage::runGcRebuildNow` and its serialization with the scheduler / FORGET, the access type `SYSTEM CAS GC REBUILD` (`01271_show_privileges.reference`), the test `05011_cas_gc_rebuild_access.sh`, the mentions in `05016_cas_drop_pool_member_access.sh` and `test_cas_drop_pool_member`.
3. Every reference: error messages that name the SQL command (the `CORRUPTED_DATA` texts of the GC guard), `docs/en/antalya/cas/architecture/garbage-collection.md` (about lines 301, 716, 847, 1194), `configuration.md` (the list of SYSTEM verbs), `operations/debugging.md`, `operations/migration.md`; point them to the utility.
4. Tests: an integration test that rebuilds a pool WITH GC history (stopped servers) end to end and then starts the servers; the refusal while a server is running.
5. Check before removing: whether the SQL command shipped in released 26.6 builds; if yes, a changelog line (backward incompatible change).

This also gives the repair path for a baseline that an older rebuild may have undercounted (a live precommit with an unreadable manifest: see `docs/superpowers/specs/2026-11-19-cas-gc-body-delete-zero-proof.md`, section Rebuild).

Not in PR 2917: a follow-up PR. In PR 2917 only: a test that shows today's refusal and a true sentence in the docs. Related: CAS-498, CAS-504, CAS-505.
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
From the review of the rebuild refusal (branch work/antalya-26.8/cas-blob-intents, commit aaf36c1eda8): a refused rebuild gives its lease claim back by restoring the never-held value, which sets lease.seq back to 0; the next leader's first round then runs at attempt 1, the attempt the refused rebuild used for its first flush of each shard; if max_gen == snap_generation, putDeterministicArtifact throws CORRUPTED_DATA in that round: fails closed before adoption, at most one round per flush the rebuild made, debris pruned later by pruneSupersededGenerations. Seen only when a namespace reaches the 8,000,000-edge budget. When the lease design is revisited: release without rewinding the sequence.

CORRECTION 2026-11-19 (code read at 8f6a63403b7, not yet tested): the GC lease IS taken over in normal operation: the GC loop steals a lease whose (owner, seq) tuple and gc/hb heartbeat pair stayed frozen across one full observation window between two of its own ticks. The problem is the rebuild command only: runGcRebuildNow mints a fresh random gc_id per invocation and Gc::rebuildBaseline calls acquireOrRenewLease(allow_steal=false) once, so it claims only an absent gc/state or the never-held lease; FORCE bypasses only the health check, not the lease. Consequence to verify by a test: SYSTEM CAS GC REBUILD [FORCE] <disk> and clickhouse-disks cas-gc-rebuild answer 'another GC leader holds the lease' on EVERY pool whose GC loop ever acquired the lease, running server included (the loop's identity differs from the command's). The documented purpose (disaster recovery for a corrupt baseline that makes every round throw CORRUPTED_DATA) is exactly such a pool. No test runs the command end to end: only 05011_cas_gc_rebuild_access.sh (access rights, expects BAD_ARGUMENTS) and unit tests on one Gc instance. Options, revised: (1) on a running server the command runs under the scheduler's stable GC identity when that loop is the leader; (2) for a stopped pool the command observes one window (lease tuple and heartbeat frozen) and then takes over, the loop's own rule, no new time assumption; (3) audit command. First step: an integration test that shows the refusal.

OWNER 2026-11-19, on scope item 5: the SQL command did ship in 26.6, and that does not matter: CAS is experimental, not GA. No compatibility stub, no 'backward incompatible change' changelog line for removing it. Item 5 is closed.

OWNER 2026-11-20: 'Аварийного восстановления GC на пуле с историей нет вовсе - это надо чинить. Но в followup.' Confirmed as a follow-up PR right after PR 2917; evidence: the integration test `test_gc_rebuild_force_on_a_pool_with_gc_history` shows both forms refuse; the GC's corrupt-bookkeeping messages now say that no repair is available in this build.
<!-- SECTION:NOTES:END -->

## Comments

<!-- COMMENTS:BEGIN -->
author: grooming-20261121
created: 2026-11-21 08:24
---
Grooming 2026-11-21. Verdict: valid (high). Evidence: cas-pr1: runGcRebuildNow and `CAS GC REBUILD` still appear in ContentAddressedMetadataStorage.{h,cpp}, src/Interpreters/InterpreterSystemQuery.cpp and gtest_cas_gc_rebuild.cpp / gtest_cas_forget.cpp; the SQL command has not been removed. Changes: area set to gc; size unset -> large (L): exclusive utility mode, SQL removal across parser/access/tests/docs, end-to-end integration test; risk set to high; confidence set to solid; impact:operability; impact:availability; type unset -> feature: new exclusive repair command, SQL surface removed. Size: L. Links: Follow-up PR after 2917 (owner 2026-11-20); related CAS-498, CAS-504, CAS-505. Pointers stale: file title (find-why rebuild cannot start) differs from the task title (rebuild only as exclusive clickhouse-disks command); the task body is current.
---
<!-- COMMENTS:END -->
