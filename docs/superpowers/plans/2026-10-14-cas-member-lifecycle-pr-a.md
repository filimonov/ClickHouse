# CAS member lifecycle, PR A Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship PR A of the member-lifecycle spec: a new `server_root_id` may not contain `/`, an aborted transaction deletes its S3 staging object, the docs state the `AbortIncompleteMultipartUpload` lifecycle rule, a guard test pins that the decommission staging drain sees the writer's key space on both backend prefixes, and the three owner/decommission refusals name the supported recovery.

**Architecture:** Four independent, each-reviewable commits on one branch off `2a034cdb374` (`antalya-26.6`), no on-store format change. The nesting rule lives in `claimOwnerOrThrow`'s owner-absent branch, so existing nested members keep mounting while `validateServerRootId` stays permissive. The abort delete reuses `cleanupPendingTempFiles`' commit branch for both outcomes. Refusal texts keep the substrings the integration test and existing gtests match on (`alive or contended`, `explicitly decommissioned`, `owned by a different server`, `<cas_server_root_id>`).

**Tech Stack:** C++23 (ClickHouse fork), gtest (`unit_tests_dbms`), CAS test fixtures in `src/Disks/tests/cas_test_helpers.h`, Markdown docs under `docs/en/antalya/cas`.

**Spec:** `docs/superpowers/specs/2026-10-13-cas-member-lifecycle-design.md` (revision 10). PR A content: `#delivery` row A, `#tests` "PR A", `#srid-slash`, `#staging` (abort delete, lifecycle rule, drain-prefix test), `#operator` (refusal texts).

## Global Constraints

- Base commit: `2a034cdb374`. Worktree: `/home/mfilimonov/workspace/ClickHouse/cas-95-1` (its `build/` is configured). Branch: `fix/antalya-26.6/cas-member-lifecycle-pr-a`, created from `2a034cdb374`.
- C++: Allman braces; comments explain why, never reference a task, plan, spec section, backlog id or review round; no fallback paths (errors propagate); `LOGICAL_ERROR` expectations are death tests with `std::_Exit` in the child (none are needed in this PR).
- Failing-first: each new test fails on `2a034cdb374` for the stated reason, or is marked guard.
- Build: `cd /home/mfilimonov/workspace/ClickHouse/cas-95-1/build && flock /home/mfilimonov/workspace/ClickHouse/master/tmp/ninja.lock nice -n 10 ninja unit_tests_dbms > build_<task>.log 2>&1; echo NINJA_EXIT=$? >> build_<task>.log` (no `-j`; a fresh log name per run). Tests: from the worktree root, `mkdir -p build/tmp; TMPDIR=build/tmp build/src/unit_tests_dbms --gtest_filter='<names>' > build/test_<name>.log 2>&1`; gate: `--gtest_filter='CAS*'` (exactly that filter, never widened).
- Every new gtest lives in a `CAS*` suite so the gate runs it.
- Commits: `git add` with exact file paths only; no rebase, amend, stash or push; message trailers `Signed-off-by: Mikhail Filimonov <mfilimonov@altinity.com>` and `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Docs under `docs/en`: every heading carries `{#anchor}` (this PR adds no headings); code names in backticks; tech-prose style (`/home/mfilimonov/.claude/skills/tech-prose/STYLE.md`): point first, short sentences, no history in prose.
- On-store format: PR A adds no fields; `Formats/README.md` is untouched. (`owner_life` and `retirement_verified` are PR B.)
- Substrings other code matches on and must survive: `alive or contended` (`tests/integration/test_cas_drop_pool_member/test.py:190`), `explicitly decommissioned` (`test.py:311`), `decommissioned` and `<cas_server_root_id>` (`gtest_cas_mount.cpp` `CASServerRootClaim.*`), `owned by a different server` (asserted absent from the retired text).

## Rulings (spec ambiguities resolved here)

- **Retired-owner text in PR A says "configure a new id", not "rerun".** The spec's "decommission not finished; rerun" describes PR B, where the tombstone is written at step 2. In PR A the tombstone is still written last, after a clean drain, so a tombstoned owner means the decommission completed and a rerun is itself refused (integration case 10). The PR A text names the supported path that exists in PR A and forbids the hand-clear; PR B rewords it when the tombstone moves.
- **Foreign-owner text ends with "then start this server under a new `<cas_server_root_id>`".** Rejoin under the same id is PR C; in PR A the decommissioned id is tombstoned, so starting under it would hit the retired refusal. PR C drops the "new id" clause.
- **Test names use `CAS*` suites.** The spec names `ContentAddressedTransaction.AbortDeletesS3Staging`; the gate filter is exactly `CAS*`, so it is `CASS3Staging.AbortDeletesS3Staging`. The spec's single `CASDecommission.StagingDrainRemovesWhatTheWriterStaged (local and native prefixes)` is two tests, `...UnderTheLocalPrefix` and `...UnderTheNativePrefix`, because the native one needs the `USE_AWS_S3` fake.
- **D7 applies to every owner create, including decommission's adopt-from-lease fallback.** `openForDecommission` reaches `claimOwnerOrThrow` with an absent owner only when the owner was deleted by hand and the subtree is empty; a nested id is then refused with the D7 text, which says existing members keep working while their owner object exists. Pinned by a test; nothing is deleted on that path.
- **"Fails on base" for the drain-prefix tests is not claimed.** The spec marks them guard; they pass on `2a034cdb374`.
- **The lost-abort-delete leak is modelled by re-creating the writer's exact key.** The abort delete is best-effort, so a lost delete leaves the object at precisely the key the writer minted; the helper captures that key from a real transaction and re-creates it, which pins the prefix equality without a fake that drops deletes.

## Review Focus

Spec-implied inputs no task's tests would otherwise exercise; each line names the test that now pins it and the task that owns it.

1. A nested id whose owner object exists but whose `epoch` counter is absent (a member left by an older build mid-bootstrap) must still mount: the nesting check runs only when the owner is absent. Pinned by `CASServerRootId.ExistingIdWithSlashStillMounts` (Task 1), which seeds only the owner.
2. Decommission's adopt-from-lease fallback (owner deleted by hand, mount present, subtree empty) on a nested id is refused by the nesting rule and deletes nothing. Pinned by `CASDecommission.LeaseAdoptFallbackRefusesANestedIdWithoutOwner` (Task 1).
3. A transaction destroyed after its metadata storage was shut down (server shutdown with an open transaction) must not throw out of the `noexcept` cleanup now that abort also deletes S3 staging. Pinned by `CASS3Staging.AbortAfterStorageShutdownDoesNotThrow` (Task 2).
4. A staging object whose abort delete was lost is still reclaimed by the next mount of the same `server_root_id`. Pinned by `CASS3Staging.MountStartSweepsALostAbortDelete` (Task 3).
5. A decommission rerun over a tombstoned owner (integration case 10) and a server start over it both keep the `explicitly decommissioned` substring and never advise clearing the tombstone. Pinned by `CASServerRootClaim.RefusalsNameTheSupportedRecovery` (Task 4).

## File structure

| File | Responsibility in this PR |
|---|---|
| `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.cpp` | `claimOwnerOrThrow`: nesting refusal (Task 1), foreign-owner text (Task 4); `throwIfOwnerRetired` text (Task 4) |
| `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h` | contract comments of `validateServerRootId` and `claimOwnerOrThrow` (Task 1); sweeper comment (Task 2) |
| `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.cpp` | `NoWait` refusal text (Task 4) |
| `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.cpp` / `.h` | abort deletes S3 staging (Task 2) |
| `src/Disks/tests/gtest_cas_mount.cpp` | `CASServerRootId.*` nesting tests (Task 1), `CASServerRootClaim.RefusalsNameTheSupportedRecovery` (Task 4) |
| `src/Disks/tests/gtest_cas_decommission.cpp` | lease-adopt nesting test (Task 1), `NoWait` text test (Task 4) |
| `src/Disks/tests/gtest_cas_s3_staging.cpp` | abort tests (Task 2), drain-prefix and sweep tests (Task 3) |
| `docs/en/antalya/cas/configuration.md` | `cas_server_root_id` row (Task 1), `cas_staging_backend` row (Task 2) |
| `docs/en/antalya/cas/bucket-requirements.md` | multipart lifecycle rule (Task 2) |
| `docs/en/antalya/cas/architecture/mounts-and-leases.md` | owner-claim table rows (Tasks 1, 4), removal paragraph (Task 4) |
| `docs/en/antalya/cas/operations/migration.md` | decommission preconditions (Task 4) |
| `docs/en/antalya/cas/operations/troubleshooting.md` | three symptom rows (Task 4) |
| `docs/en/sql-reference/statements/system.md` | `DROP POOL MEMBER` notes (Tasks 1, 4) |

Every `CA/` path below means `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`. Line numbers are at `2a034cdb374`.

---

### Task 1: New `server_root_id`s without `/` (D7)

**Files:**
- Modify: `CA/Pool/CasServerRoot.cpp:525-566` (`claimOwnerOrThrow`, between the owner-absent emptiness check and the owner create at `:564`)
- Modify: `CA/Pool/CasServerRoot.h:75-85` (`validateServerRootId` comment), `:145-152` (`claimOwnerOrThrow` contract)
- Test: `src/Disks/tests/gtest_cas_mount.cpp` (after `CASServerRootId.ValidationAcceptsCleanPathsRejectsBad`, line 499)
- Test: `src/Disks/tests/gtest_cas_decommission.cpp` (after `CASDecommission.RefusesUnknownMember`, line 548)
- Docs: `docs/en/antalya/cas/configuration.md:92`, `docs/en/antalya/cas/architecture/mounts-and-leases.md:38-55`, `docs/en/sql-reference/statements/system.md:602-604`

**Interfaces:**
- Consumes: `claimOwnerOrThrow(CasOperation &, const Layout &, const String & srid, UInt128 our_uuid, const ObserveRefCatalog &)`; `ErrorCodes::BAD_ARGUMENTS` is already declared in `CasServerRoot.h:24-28` and visible in the `.cpp`.
- Produces: no new symbol. Behaviour: an absent owner for an `srid` containing `/` throws `BAD_ARGUMENTS` whose message contains `contains '/'`; nothing is written.

- [ ] **Step 0: Create the branch**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
git status --short | grep -v '^??' && echo "DIRTY: stop" || true
git checkout -b fix/antalya-26.6/cas-member-lifecycle-pr-a 2a034cdb374
git log --oneline -1
```
Expected: the dirty check prints nothing, the last line is `2a034cdb374 Merge pull request #2512 ...`.

- [ ] **Step 1: Write the failing tests in `gtest_cas_mount.cpp`**

Insert after the closing brace of `TEST(CASServerRootId, ValidationAcceptsCleanPathsRejectsBad)` (line 499):

```cpp
/// Decommission and the staging sweep select objects by key prefix, so a member `a/b` could be drained
/// by a decommission of `a`. The first claim of an id refuses a nested one; `validateServerRootId`
/// still accepts it so that members created before this rule keep mounting (the guard below).
TEST(CASServerRootId, NewIdWithSlashIsRefused)
{
    auto b = std::make_shared<InMemoryBackend>();
    const Layout l("p");
    Ops ops(b);
    DB::Cas::tests::expectThrowsCodeWithMessage(DB::ErrorCodes::BAD_ARGUMENTS, "contains '/'", [&]
    {
        claimOwnerOrThrow(ops.op, l, "shard-01/replica-a", UInt128(1), emptyCatalogObservation());
    });
    EXPECT_FALSE(ops.op.head(l.ownerKey("shard-01/replica-a"), Retry::standard()).has_value())
        << "a refused claim must not create the owner";
}

/// Guard: a member that already owns a nested id (its owner object exists, written by an older build)
/// still mounts under it. Only the owner is seeded: the epoch counter is minted by this very open, so
/// the nesting rule must not run once the owner is present.
TEST(CASServerRootId, ExistingIdWithSlashStillMounts)
{
    auto b = std::make_shared<InMemoryBackend>();
    const Layout l("p");
    {
        Ops seed(b);
        mustCommit(seed.op.create(l.ownerKey("shard-01/replica-a"),
            encodeOwner(OwnerObject{.server_uuid = UInt128(1), .retired_at_ms = std::nullopt}),
            Retry::standard()), "owner of an existing nested member");
    }
    auto store = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "shard-01/replica-a"});
    ASSERT_TRUE(store != nullptr);
    Ops check(b);
    const auto mount = check.op.read(l.mountKey("shard-01/replica-a"), Retry::standard());
    ASSERT_TRUE(mount.has_value());
    EXPECT_EQ(decodeMountLease(mount->bytes).server_uuid, UInt128(1));
}
```

- [ ] **Step 2: Write the failing test in `gtest_cas_decommission.cpp`**

Insert after the closing brace of `TEST(CASDecommission, RefusesUnknownMember)` (line 548):

```cpp
/// The adopt-from-lease fallback (owner deleted by hand, mount present, subtree empty) re-creates the
/// owner through the same claim as a first mount, so a nested id is refused there too, before any
/// object of the member is touched.
TEST(CASDecommission, LeaseAdoptFallbackRefusesANestedIdWithoutOwner)
{
    auto backend = std::make_shared<InMemoryBackend>();
    const Layout layout("p");
    OperationForTest raw_op(*backend);
    /// A member that already owned the nested id (its owner written by an older build), then one
    /// clean life of it, then the owner deleted by hand: the state the fallback exists for.
    ASSERT_TRUE(std::holds_alternative<Committed>((*raw_op).create(layout.ownerKey("shard-01/replica-a"),
        encodeOwner(OwnerObject{.server_uuid = UInt128(1), .retired_at_ms = std::nullopt}), Retry::once())));
    { auto member = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "shard-01/replica-a"}); }
    const auto owner = (*raw_op).read(layout.ownerKey("shard-01/replica-a"), Retry::once());
    ASSERT_TRUE(owner.has_value());
    ASSERT_EQ((*raw_op).remove(layout.ownerKey("shard-01/replica-a"), owner->etag, Retry::once()), Removal::Removed);
    const auto mount_before = (*raw_op).read(layout.mountKey("shard-01/replica-a"), Retry::once());
    const auto epoch_before = (*raw_op).read(layout.epochKey("shard-01/replica-a"), Retry::once());
    ASSERT_TRUE(mount_before.has_value());
    ASSERT_TRUE(epoch_before.has_value());

    DB::Cas::tests::expectThrowsCodeWithMessage(ErrorCodes::BAD_ARGUMENTS, "contains '/'", [&]
    {
        decommissionPoolMember(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "shard-01/replica-a");
    });

    EXPECT_FALSE((*raw_op).head(layout.ownerKey("shard-01/replica-a"), Retry::once()).has_value());
    EXPECT_EQ((*raw_op).read(layout.mountKey("shard-01/replica-a"), Retry::once())->etag, mount_before->etag);
    EXPECT_EQ((*raw_op).read(layout.epochKey("shard-01/replica-a"), Retry::once())->etag, epoch_before->etag);
}
```

`ErrorCodes::BAD_ARGUMENTS` resolves through `using namespace DB` and the declaration in `CasServerRoot.h:24-28` (`RefusesUnknownMember` already uses it); nothing to declare.

- [ ] **Step 2b: Move two existing tests off nested ids**

Two `CASServerRootSafety` tests claim `root/x` over an empty subtree and expect the claim to reach the create; after Step 4 a nested id is refused just before it. They test debris and the conflict recompute, not nesting, so they move to a flat id. In `gtest_cas_mount.cpp`:

- `OwnerConflictRevealsManifestBackend` (lines 117-133): replace `"p/gc/server-roots/root/x/owner"` with `"p/gc/server-roots/rootx/owner"` and `"p/cas/manifests/root/x/table/debris"` with `"p/cas/manifests/rootx/table/debris"`.
- `TEST(CASServerRootSafety, OpaqueStreamAndStateDebrisAloneDoesNotBlockRecreation)` (lines 766-778): replace both `"root/x"` with `"rootx"`.
- `TEST(CASServerRootSafety, OwnerConflictRecomputesTheWholeEmptinessBundle)` (lines 811-825): replace both `"root/x"` with `"rootx"`.

Leave `EveryCatalogLifecycleStateBlocksOwnerAndEpochRecreation` (line 746) and `ManifestAndLooseRootDebrisStillBlockRecreation` (line 790) as they are: both expect a throw, and with the rule placed after the emptiness check they still throw for the reason they are named for. Check the edit is exhaustive: `grep -n '"root/x"' src/Disks/tests/gtest_cas_mount.cpp` must list only lines in those two tests, `catalogOwning` (line 51), `OwnershipUsesAPathComponentBoundary` (lines 761-763, calls `serverRootSubtreeEmpty` directly) and `EpochConflictRecomputesTheWholeEmptinessBundle` (lines 831-840, calls `allocateWriterEpoch` directly).

- [ ] **Step 3: Build and run the tests to verify they fail**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1/build && flock /home/mfilimonov/workspace/ClickHouse/master/tmp/ninja.lock nice -n 10 ninja unit_tests_dbms > build_t1a.log 2>&1; echo NINJA_EXIT=$? >> build_t1a.log; tail -1 build_t1a.log
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1 && mkdir -p build/tmp && TMPDIR=build/tmp build/src/unit_tests_dbms --gtest_filter='CASServerRootId.*:CASDecommission.LeaseAdoptFallbackRefusesANestedIdWithoutOwner' > build/test_t1a.log 2>&1; grep -E '^\[ +(OK|FAILED) +\]' build/test_t1a.log
```
Expected: `NINJA_EXIT=0`; `NewIdWithSlashIsRefused` FAILED (no exception: the base creates the owner), `LeaseAdoptFallbackRefusesANestedIdWithoutOwner` FAILED (no exception: the base re-creates the owner and proceeds), `ExistingIdWithSlashStillMounts` OK (guard), `ValidationAcceptsCleanPathsRejectsBad` OK.

- [ ] **Step 4: Implement the refusal in `claimOwnerOrThrow`**

In `CA/Pool/CasServerRoot.cpp`, after the owner-absent emptiness check (the `throw ... "(identity lost over existing data) — refusing to re-claim"` ending at line 562) and before `const std::optional<Observation> occupant = conflictOrThrow(op.create(key, ...` (line 564), insert:

```cpp
    /// First claim of `srid`. A nested id is refused here rather than in `validateServerRootId`, so
    /// members that already own one keep mounting; after the emptiness check, so an id that lost its
    /// owner over data still gets the identity-lost refusal. Decommission and the staging sweep select
    /// objects by key prefix, so `a` would otherwise drain a live `a/b`.
    if (srid.find('/') != String::npos)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "CAS server-root '{}' has no owner and its id contains '/': a new server_root_id may not be nested, "
            "because a decommission selects objects by key prefix and would drain every member whose id starts "
            "with it. Configure a <cas_server_root_id> without '/'. A member that already owns a nested id keeps "
            "working while its owner object '{}' exists.",
            srid, key);
```

- [ ] **Step 5: Update the two header contracts in `CA/Pool/CasServerRoot.h`**

In the `validateServerRootId` comment (lines 75-85), replace the line
```
/// Throws `ErrorCodes::BAD_ARGUMENTS` on any violation. Fail closed — there is no sanitizing fallback.
```
with
```
/// Throws `ErrorCodes::BAD_ARGUMENTS` on any violation. Fail closed — there is no sanitizing fallback.
/// A `/` is still accepted here for members that already own a nested id; `claimOwnerOrThrow` refuses to
/// create a new one.
```

In the `claimOwnerOrThrow` contract (lines 145-152), replace
```
///   - owner absent AND the subtree is provably empty → `create` the owner (claim);
```
with
```
///   - owner absent AND `srid` contains '/' → throw `BAD_ARGUMENTS` (nested ids are not created; an
///     existing nested owner is honoured by the first bullet);
///   - owner absent AND the subtree is provably empty → `create` the owner (claim);
```

- [ ] **Step 6: Build and run the tests to verify they pass**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1/build && flock /home/mfilimonov/workspace/ClickHouse/master/tmp/ninja.lock nice -n 10 ninja unit_tests_dbms > build_t1b.log 2>&1; echo NINJA_EXIT=$? >> build_t1b.log; tail -1 build_t1b.log
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1 && TMPDIR=build/tmp build/src/unit_tests_dbms --gtest_filter='CASServerRootId.*:CASServerRootClaim.*:CASServerRootSafety.*:CASDecommission.*' > build/test_t1b.log 2>&1; grep -E '^\[ +(FAILED|PASSED) +\]' build/test_t1b.log
```
Expected: `NINJA_EXIT=0`; `[  PASSED  ]` with no `FAILED` lines. The two `CASServerRootSafety` tests moved to `rootx` in Step 2b pass for their own reason; if any other test now fails on the nesting rule, report it, do not weaken the rule.

- [ ] **Step 7: Docs**

`docs/en/antalya/cas/configuration.md:92`, the `cas_server_root_id` row: append to the description cell:
```
. A new id may not contain `/`: the first mount refuses to create a nested id, because `SYSTEM CAS DROP POOL MEMBER` selects objects by key prefix and would drain every member whose id starts with the victim's. An id that already owns its owner object keeps working
```

`docs/en/antalya/cas/architecture/mounts-and-leases.md`, after line 38 (`is a startup error.`), add the sentence:
```
A new `cas_server_root_id` may not contain `/`; ids that already own their owner object keep working.
```
and in the owner-claim table (line 48-53) insert before the row `| absent, subtree provably empty | ...`:
```
| absent, id contains `/` | `BAD_ARGUMENTS` — a new id may not be nested; an existing nested owner proceeds by the first row |
```

`docs/en/sql-reference/statements/system.md:602-604`: replace
```
Both `server_root_id` and `disk_name` are required string literals (a `server_root_id` is an opaque server-root path
that may contain `/`, not a plain identifier, so it cannot be written unquoted).
```
with
```
Both `server_root_id` and `disk_name` are required string literals (a `server_root_id` is an opaque string, not a
plain identifier, so it cannot be written unquoted; ids created before nesting was closed may contain `/`).
```

- [ ] **Step 8: Commit**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
git add src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.cpp \
        src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h \
        src/Disks/tests/gtest_cas_mount.cpp src/Disks/tests/gtest_cas_decommission.cpp \
        docs/en/antalya/cas/configuration.md docs/en/antalya/cas/architecture/mounts-and-leases.md \
        docs/en/sql-reference/statements/system.md
git commit -F - <<'EOF'
Refuse to create a nested server_root_id at the owner claim

`SYSTEM CAS DROP POOL MEMBER 'a'` and the staging sweep select objects by
key prefix, so a member `a/b` was drained together with `a`. The first
claim of an id (owner absent) now refuses an id containing `/`;
`validateServerRootId` keeps accepting one so members that already own a
nested id keep mounting. The adopt-from-lease decommission fallback goes
through the same claim and is refused for a nested id before any object
is touched.

Tests: `CASServerRootId.NewIdWithSlashIsRefused`,
`CASDecommission.LeaseAdoptFallbackRefusesANestedIdWithoutOwner` (red on
the base: the owner was created), guard
`CASServerRootId.ExistingIdWithSlashStillMounts`.

Signed-off-by: Mikhail Filimonov <mfilimonov@altinity.com>
Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

---

### Task 2: Abort deletes S3 staging; `AbortIncompleteMultipartUpload` doc rule

**Files:**
- Modify: `CA/ContentAddressedTransaction.cpp:167-212` (`cleanupPendingTempFiles`)
- Modify: `CA/ContentAddressedTransaction.h:36`, `:147-150`, `:195-196`
- Modify: `CA/Pool/CasServerRoot.h:604-612` (sweeper comment)
- Test: `src/Disks/tests/gtest_cas_s3_staging.cpp` (after `CASS3Staging.ReadYourWritesReturnsStagedBytesFromS3StagingObject`, line 934)
- Docs: `docs/en/antalya/cas/configuration.md:113` (`cas_staging_backend` row), `docs/en/antalya/cas/bucket-requirements.md:46` (after the `skip_access_check` paragraph, before `## Soft delete ...`)

**Interfaces:**
- Consumes: `ContentAddressedTransaction::cleanupPendingTempFiles() noexcept`, `PartStaging::PendingBlob{ref, staging_key, size, backend}`, `metadata_storage.objectStorage()->removeObjectIfExists(StoredObject)`; fixtures `makeFakeNativeCopyStorage`, `makeS3StagingMetadataStorageForTest`, `writeThroughS3Transaction` (all in the file's anonymous namespace, lines 103-845).
- Produces: behaviour only: after a transaction is destroyed without commit, `object_storage->listObjects(metadata_storage->stagingKeyPrefix(), ...)` is empty.

- [ ] **Step 1: Write the failing tests**

Insert after the closing brace of `TEST(CASS3Staging, ReadYourWritesReturnsStagedBytesFromS3StagingObject)` (line 934):

```cpp
/// An aborted transaction deletes its own S3 staging object instead of leaving it for the next mount.
TEST(CASS3Staging, AbortDeletesS3Staging)
{
    auto object_storage = makeFakeNativeCopyStorage(/*native_only_copy_supported=*/true);
    auto metadata_storage = makeS3StagingMetadataStorageForTest(object_storage, "mountAbort");
    metadata_storage->startup();

    auto tx = metadata_storage->createTransaction();
    auto & ca_tx = dynamic_cast<DB::ContentAddressedTransaction &>(*tx);
    writeThroughS3Transaction(
        ca_tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin", std::string(300, 'x'));

    DB::RelativePathsWithMetadata staged;
    object_storage->listObjects(metadata_storage->stagingKeyPrefix(), staged, /*max_keys=*/0);
    ASSERT_EQ(staged.size(), 1u);

    tx.reset();   /// never committed

    DB::RelativePathsWithMetadata after;
    object_storage->listObjects(metadata_storage->stagingKeyPrefix(), after, /*max_keys=*/0);
    EXPECT_TRUE(after.empty()) << "an aborted transaction must not leave its S3 staging object behind";
}

/// Guard: the abort delete runs in a `noexcept` destructor, also after the storage was shut down with
/// the transaction still open (server shutdown). Whatever the delete does then, nothing may escape.
TEST(CASS3Staging, AbortAfterStorageShutdownDoesNotThrow)
{
    auto object_storage = makeFakeNativeCopyStorage(/*native_only_copy_supported=*/true);
    auto metadata_storage = makeS3StagingMetadataStorageForTest(object_storage, "mountAbortLate");
    metadata_storage->startup();

    auto tx = metadata_storage->createTransaction();
    auto & ca_tx = dynamic_cast<DB::ContentAddressedTransaction &>(*tx);
    writeThroughS3Transaction(
        ca_tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin", std::string(300, 'x'));

    metadata_storage->shutdown();
    EXPECT_NO_THROW(tx.reset());
}
```

- [ ] **Step 2: Build and run to verify the first fails and the guard passes**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1/build && flock /home/mfilimonov/workspace/ClickHouse/master/tmp/ninja.lock nice -n 10 ninja unit_tests_dbms > build_t2a.log 2>&1; echo NINJA_EXIT=$? >> build_t2a.log; tail -1 build_t2a.log
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1 && TMPDIR=build/tmp build/src/unit_tests_dbms --gtest_filter='CASS3Staging.Abort*' > build/test_t2a.log 2>&1; grep -E '^\[ +(OK|FAILED) +\]' build/test_t2a.log
```
Expected: `AbortDeletesS3Staging` FAILED with `after.empty()` false (one object remains); `AbortAfterStorageShutdownDoesNotThrow` OK.

- [ ] **Step 3: Implement the abort delete**

In `CA/ContentAddressedTransaction.cpp`, replace the whole `else if (committed) { ... }` branch and the trailing two-line `/// else: an S3-mode pending blob of an ABORTED transaction ...` comment (lines 178-208) with:

```cpp
            else
            {
                /// The S3 staging object is private to this transaction and nothing reads
                /// `pending_blobs` after this call, so commit and abort delete it alike. An object that
                /// outlived an abort would stay until the next mount of this `server_root_id`.
                try
                {
                    metadata_storage.objectStorage()->removeObjectIfExists(StoredObject(pb.staging_key));
                }
                catch (...) // NOLINT(bugprone-empty-catch)
                {
                    /// Best-effort (noexcept context): a stubborn delete leaves debris for the mount-start sweep.
                }
            }
```
The loop body then reads: `if (pb.backend == Cas::StagingBackend::Local) { fs::remove } else { ...above... }`. Check with `grep -n 'committed' CA/ContentAddressedTransaction.cpp` that `committed` is still read elsewhere (commit sets it); if `cleanupPendingTempFiles` was its only reader and the compiler warns about an unused member, keep the member (commit writes it) and report.

- [ ] **Step 4: Fix the three header comments and the sweeper comment**

`CA/ContentAddressedTransaction.h:36`: replace
```
/// destructor removes private local staging, leaves aborted S3 staging for the mount-lease sweeper,
```
with
```
/// destructor removes the transaction's local and S3 staging,
```

`CA/ContentAddressedTransaction.h:147-150`: replace
```
        /// `staging_key` is either a private local temp path or an S3 staging object key returned by
        /// `CaContentWriteBuffer` at finalize. The backend selects cleanup and read-your-writes:
        /// local files are removed by this transaction, whereas aborted S3 objects must remain
        /// available to the mount-lease sweeper and to recovery of the promote source.
```
with
```
        /// `staging_key` is either a private local temp path or an S3 staging object key returned by
        /// `CaContentWriteBuffer` at finalize. The backend selects the read-your-writes source; both
        /// kinds are removed by this transaction on commit and on abort.
```

`CA/ContentAddressedTransaction.h:195-196`: replace
```
    /// Removes local staging files after commit or abort. On a successful commit it also removes
    /// S3 staging objects; aborted S3 objects are intentionally retained for lease-scoped cleanup.
```
with
```
    /// Removes the transaction's staging after commit or abort: local files and S3 staging objects
    /// alike. S3 deletes are best-effort; a lost delete leaves debris for the mount-start sweep.
```

`CA/Pool/CasServerRoot.h:606-610`: replace
```
/// A leaked S3 staging object happens two ways: (1) an exception between blob publication succeeding and
/// `cleanupPendingTempFiles` deleting the staging key (`ContentAddressedTransaction.cpp`), or (2) an
/// aborted/cancelled transaction whose pending blobs were staged but never promoted — by design,
/// `cleanupPendingTempFiles` deliberately leaves an S3 staging object in place on the abort path (never a
/// bare `fs::remove` on a remote key), so this sweeper is its ONLY reclaimer. Debris from either case is
```
with
```
/// A leaked S3 staging object is one whose transaction never ran `cleanupPendingTempFiles` (the process
/// died, or an exception hit between the upload and the cleanup) or whose best-effort delete on commit or
/// abort was lost; this sweeper is its only reclaimer. Debris from either case is
```

- [ ] **Step 5: Build and run to verify both pass**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1/build && flock /home/mfilimonov/workspace/ClickHouse/master/tmp/ninja.lock nice -n 10 ninja unit_tests_dbms > build_t2b.log 2>&1; echo NINJA_EXIT=$? >> build_t2b.log; tail -1 build_t2b.log
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1 && TMPDIR=build/tmp build/src/unit_tests_dbms --gtest_filter='CASS3Staging.*:CASStagingSweeper.*' > build/test_t2b.log 2>&1; grep -E '^\[ +(FAILED|PASSED) +\]' build/test_t2b.log
```
Expected: `NINJA_EXIT=0`, `[  PASSED  ]`, no `FAILED`. `SuccessfulCommitRemovesOrphanedS3StagingObject` must still pass (the commit branch is unchanged in effect).

- [ ] **Step 6: Docs**

`docs/en/antalya/cas/configuration.md:113`, `cas_staging_backend` row: replace the description cell
```
Blob staging backend (`local` \| `s3`); `s3` is opt-in and requires native same-store copy on writable mount
```
with
```
Blob staging backend (`local` \| `s3`); `s3` is opt-in and requires native same-store copy on writable mount. A transaction deletes its staging objects on commit and on abort; mount start removes leftovers of the same `server_root_id`. Neither can see a multipart upload that was never completed, so add an `AbortIncompleteMultipartUpload` lifecycle rule on the bucket to reclaim those
```

`docs/en/antalya/cas/bucket-requirements.md`, after the paragraph ending `pass.` (line 46) and before `## Soft delete is an operator precondition {#soft-delete}`, add:
```
**Incomplete multipart uploads.** With `cas_staging_backend = s3`, a process killed during a
multipart upload leaves an incomplete upload under `staging/<server_root_id>/`. Object `LIST` does
not show it, so neither the transaction's own delete nor the mount-start sweep can remove it, and its
parts stay billed. Add an `AbortIncompleteMultipartUpload` lifecycle rule on the bucket (one day is
enough: a staging upload completes within its own transaction or never).
```

- [ ] **Step 7: Commit**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
git add src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.cpp \
        src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.h \
        src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h \
        src/Disks/tests/gtest_cas_s3_staging.cpp \
        docs/en/antalya/cas/configuration.md docs/en/antalya/cas/bucket-requirements.md
git commit -F - <<'EOF'
Delete S3 staging on abort; document AbortIncompleteMultipartUpload

An aborted transaction left its S3 staging object until the next mount
of the same `server_root_id`; the comment calling it a re-readable
publication source was wrong, nothing reads `pending_blobs` after the
cleanup. Commit and abort now delete it alike, best-effort.

Incomplete multipart uploads are invisible to object LIST, so the docs
state the bucket lifecycle rule `AbortIncompleteMultipartUpload`.

Tests: `CASS3Staging.AbortDeletesS3Staging` (red on the base: one object
remains), guard `CASS3Staging.AbortAfterStorageShutdownDoesNotThrow`.

Signed-off-by: Mikhail Filimonov <mfilimonov@altinity.com>
Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

---

### Task 3: Guard: the decommission staging drain removes what the writer staged (local and native prefixes)

**Files:**
- Test: `src/Disks/tests/gtest_cas_s3_staging.cpp`: include, one helper in the anonymous namespace after `writeThroughS3Transaction` (line 845), two tests before `#if USE_AWS_S3` (line 1003), one test inside it after `CASS3Staging.GenerationBackendMayUseNativeOnlyCopy` (line 1337).

**Interfaces:**
- Consumes: `DB::Cas::decommissionPoolMember(BackendPtr, PoolConfig, const String & victim_srid)` (`CA/Tools/CasDecommission.h`), `DB::Cas::ObjectStorageBackend(ObjectStoragePtr, Mode)` (`CA/Backend/CasObjectStorageBackend.h`, already included), `ContentAddressedMetadataStorage::stagingKeyPrefix()`, `::shutdown()`, fixtures `makeFakeNativeCopyStorage` (Local type → `EmulatedSingleProcess`, keys under the storage root) and `makeFakeGenerationObjectStorageForTest` (`getType() == S3` → `Native`, bare keys; `USE_AWS_S3` only).
- Produces: helper `std::string leakWriterStagedObject(DB::IObjectStorage &, DB::ContentAddressedMetadataStorage &)` returning the physical key.

Why both prefixes: the drain lists `<pool_prefix>/staging/<srid>/` as a logical key and `ObjectStorageBackend` applies the local root itself (`emuPath`), while the writer mints keys through `physicalKey` (root-prefixed for Local, verbatim for Native). The two must meet at the same object, or a decommission reports an empty staging drain and leaves the debris.

- [ ] **Step 1: Add the include and the helper**

Add after line 5 (`#include <...CasObjectStorageBackend.h>`):
```cpp
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasDecommission.h>
```

Insert after `writeThroughS3Transaction` (line 845, still inside the anonymous namespace):

```cpp
/// Stages one blob through a real transaction, lets the transaction abort, and re-creates the exact key
/// the writer minted: the abort delete is best-effort, so a lost delete leaves the object at precisely
/// this key. Returns the key as the object storage sees it.
std::string leakWriterStagedObject(DB::IObjectStorage & object_storage, DB::ContentAddressedMetadataStorage & metadata_storage)
{
    std::string key;
    {
        auto tx = metadata_storage.createTransaction();
        auto & ca_tx = dynamic_cast<DB::ContentAddressedTransaction &>(*tx);
        writeThroughS3Transaction(
            ca_tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin", std::string(300, 'x'));
        DB::RelativePathsWithMetadata staged;
        object_storage.listObjects(metadata_storage.stagingKeyPrefix(), staged, /*max_keys=*/0);
        if (staged.size() != 1)
            throw std::runtime_error("expected exactly one staged object, got " + std::to_string(staged.size()));
        key = staged.front()->relative_path;
    }
    auto buf = object_storage.writeObject(DB::StoredObject(key), DB::WriteMode::Rewrite);
    buf->write("x", 1);
    buf->finalize();
    return key;
}
```

- [ ] **Step 2: Add the local-prefix drain test and the sweep test (before `#if USE_AWS_S3`)**

```cpp
/// Guard: the drain lists `<pool_prefix>/staging/<srid>/` as a LOGICAL key and the backend applies the
/// local root itself, while the writer mints its keys through `physicalKey`. Local prefix:
/// `EmulatedSingleProcess` over a `LocalObjectStorage`, whose physical keys carry its root.
TEST(CASDecommission, StagingDrainRemovesWhatTheWriterStagedUnderTheLocalPrefix)
{
    auto object_storage = makeFakeNativeCopyStorage(/*native_only_copy_supported=*/true);
    auto metadata_storage = makeS3StagingMetadataStorageForTest(object_storage, "victimLocal");
    metadata_storage->startup();
    const std::string leaked = leakWriterStagedObject(*object_storage, *metadata_storage);
    metadata_storage->shutdown();   /// clean farewell: the member is dead and claimable without a wait
    ASSERT_TRUE(object_storage->exists(DB::StoredObject(leaked)));

    auto backend = std::make_shared<DB::Cas::ObjectStorageBackend>(
        object_storage, DB::Cas::ObjectStorageBackend::Mode::EmulatedSingleProcess);
    const DB::Cas::DecommissionReport report = DB::Cas::decommissionPoolMember(
        backend, DB::Cas::PoolConfig{.pool_prefix = "pool", .server_root_id = "admin"}, "victimLocal");

    EXPECT_EQ(report.staging_objects_removed, 1u) << "the drain did not see the writer's key space";
    EXPECT_FALSE(object_storage->exists(DB::StoredObject(leaked)));
}

/// Guard: a staging object whose abort delete was lost is reclaimed by the next mount of the same
/// `server_root_id`, through the same prefix the writer minted it under.
TEST(CASS3Staging, MountStartSweepsALostAbortDelete)
{
    auto object_storage = makeFakeNativeCopyStorage(/*native_only_copy_supported=*/true);
    std::string leaked;
    {
        auto first_mount = makeS3StagingMetadataStorageForTest(object_storage, "mountSweep");
        first_mount->startup();
        leaked = leakWriterStagedObject(*object_storage, *first_mount);
        first_mount->shutdown();
    }
    ASSERT_TRUE(object_storage->exists(DB::StoredObject(leaked)));

    auto second_mount = makeS3StagingMetadataStorageForTest(object_storage, "mountSweep");
    second_mount->startup();
    EXPECT_FALSE(object_storage->exists(DB::StoredObject(leaked)));
}
```

`makeS3StagingMetadataStorageForTest` passes `"srv1"` as `server_id` for every call, so the second mount is the same `server_uuid`, and the clean farewell of the first makes the reclaim immediate.

- [ ] **Step 3: Add the native-prefix drain test (inside `#if USE_AWS_S3`, after `GenerationBackendMayUseNativeOnlyCopy`, before `#endif`)**

```cpp
/// Native prefix: keys pass through unchanged on both sides, so a `physicalKey` that started adding a
/// root for Native, or a drain that started adding one, would miss the writer's objects.
TEST(CASDecommission, StagingDrainRemovesWhatTheWriterStagedUnderTheNativePrefix)
{
    auto object_storage = makeFakeGenerationObjectStorageForTest();
    auto metadata_storage = makeS3StagingMetadataStorageForTest(object_storage, "victimNative");
    metadata_storage->startup();
    const std::string leaked = leakWriterStagedObject(*object_storage, *metadata_storage);
    metadata_storage->shutdown();
    ASSERT_TRUE(object_storage->exists(DB::StoredObject(leaked)));

    auto backend = std::make_shared<DB::Cas::ObjectStorageBackend>(
        object_storage, DB::Cas::ObjectStorageBackend::Mode::Native);
    const DB::Cas::DecommissionReport report = DB::Cas::decommissionPoolMember(
        backend, DB::Cas::PoolConfig{.pool_prefix = "pool", .server_root_id = "admin"}, "victimNative");

    EXPECT_EQ(report.staging_objects_removed, 1u) << "the drain did not see the writer's key space";
    EXPECT_FALSE(object_storage->exists(DB::StoredObject(leaked)));
}
```

- [ ] **Step 4: Build and run**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1/build && flock /home/mfilimonov/workspace/ClickHouse/master/tmp/ninja.lock nice -n 10 ninja unit_tests_dbms > build_t3.log 2>&1; echo NINJA_EXIT=$? >> build_t3.log; tail -1 build_t3.log
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1 && TMPDIR=build/tmp build/src/unit_tests_dbms --gtest_filter='CASDecommission.StagingDrain*:CASS3Staging.MountStartSweepsALostAbortDelete' > build/test_t3.log 2>&1; grep -E '^\[ +(OK|FAILED) +\]' build/test_t3.log
```
Expected: all three OK (guards). Only `report.staging_objects_removed` and the key's absence are asserted: the aborted build may leave manifest debris the drain retains with a warning, which is unrelated and already pinned by `RetainsDebrisWhoseEpochSealIsUnconsumed`. If the native variant fails on the fake's token handling (a `Mismatch` warning in `report.warnings`) rather than on an empty LIST, report it with the warning text; do not change production code in this task.

- [ ] **Step 5: Commit**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
git add src/Disks/tests/gtest_cas_s3_staging.cpp
git commit -F - <<'EOF'
Pin that the decommission staging drain sees the writer's key space

The drain lists `<pool_prefix>/staging/<srid>/` as a logical key and the
backend applies the local root itself, while the writer mints keys
through `physicalKey`. Two guard tests stage a blob through a real
transaction, model a lost abort delete, and decommission the member
over an EmulatedSingleProcess (root-prefixed) and a Native (verbatim)
backend. A third pins that the next mount's sweep reclaims the same
object.

Signed-off-by: Mikhail Filimonov <mfilimonov@altinity.com>
Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

---

### Task 4: Refusal texts name the supported recovery

**Files:**
- Modify: `CA/Pool/CasServerRoot.cpp:441-452` (`throwIfOwnerRetired`), `:542-553` (foreign owner in `claimOwnerOrThrow`)
- Modify: `CA/Pool/CasPool.cpp:754-770` (`NoWait` refusal in `mountWritable`)
- Test: `src/Disks/tests/gtest_cas_mount.cpp` (after `CASServerRootClaim.TombstonedSameOwnerFailsClosed`, line 594)
- Test: `src/Disks/tests/gtest_cas_decommission.cpp` (after `CASDecommission.RefusesLiveMember`, line 506)
- Docs: `docs/en/antalya/cas/operations/troubleshooting.md:24`, `docs/en/antalya/cas/operations/migration.md:206-210`, `docs/en/antalya/cas/architecture/mounts-and-leases.md:49-50,261-266`, `docs/en/sql-reference/statements/system.md:596-597`

**Interfaces:**
- Consumes: the three throw sites; `system.cas_mounts` columns `server_root_id`, `state` (values `live`, `expired`, `terminated`, `fenced`, `corrupt`; `src/Storages/System/StorageSystemContentAddressedMounts.cpp:41,51`); `SYSTEM CAS GC RUN` (`docs/en/sql-reference/statements/system.md:470`).
- Produces: message substrings tests match on: `SYSTEM CAS DROP POOL MEMBER 'r'` (foreign), `new <cas_server_root_id>` (retired), `state = 'fenced'` (`NoWait`). Preserved: `explicitly decommissioned`, `alive or contended`, `owned by a different server`, `<cas_server_root_id>`.

Why these states: the `NoWait` claim trusts no wall clock (`claimMount` reclaims only `gc_fenced`, the clean marker, or a caller-proven-dead token; `CasServerRoot.cpp:846-870`), while `system.cas_mounts` computes `expired` from wall-clock arithmetic (`:1230-1236`). A hard-killed member is removable only after GC's heartbeat floor fences it: a token held unchanged for `mountObservationThresholdMs(ttl, renew period)` = TTL + 5% + one renew period (`CasGc.cpp:708`).

- [ ] **Step 1: Write the failing test in `gtest_cas_mount.cpp`**

Insert after the closing brace of `TEST(CASServerRootClaim, TombstonedSameOwnerFailsClosed)` (line 594):

```cpp
/// Each identity refusal names the supported recovery: the decommission verb for a dead foreign owner,
/// a new id for a retired one. Neither advises a hand edit of the owner object as the first option.
TEST(CASServerRootClaim, RefusalsNameTheSupportedRecovery)
{
    auto b = std::make_shared<InMemoryBackend>();
    const Layout l("p");
    Ops ops(b);
    claimOwnerOrThrow(ops.op, l, "r", UInt128(1), emptyCatalogObservation());
    DB::Cas::tests::expectThrowsCodeWithMessage(DB::ErrorCodes::CORRUPTED_DATA, "SYSTEM CAS DROP POOL MEMBER 'r'", [&]
    {
        claimOwnerOrThrow(ops.op, l, "r", UInt128(2), emptyCatalogObservation());
    });

    mustCommit(ops.op.create(l.ownerKey("t"), encodeOwner(OwnerObject{
        .server_uuid = UInt128(1),
        .retired_at_ms = 1752537600000ULL,
    }), Retry::standard()), "tombstoned owner");
    try
    {
        claimOwnerOrThrow(ops.op, l, "t", UInt128(1), emptyCatalogObservation());
        FAIL() << "expected a tombstoned owner claim to fail closed";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::CORRUPTED_DATA);
        EXPECT_NE(e.message().find("explicitly decommissioned"), String::npos) << e.message();
        EXPECT_NE(e.message().find("new <cas_server_root_id>"), String::npos) << e.message();
        EXPECT_EQ(e.message().find("clear the owner object"), String::npos) << e.message();
    }
}
```

- [ ] **Step 2: Write the failing test in `gtest_cas_decommission.cpp`**

Insert after the closing brace of `TEST(CASDecommission, RefusesLiveMember)` (line 506):

```cpp
/// The refusal names the state a hard-killed member has to reach. `expired` in `system.cas_mounts` is
/// wall-clock arithmetic; the claim trusts only a terminal slot (`terminated` or `fenced`), and an
/// expired, unfenced lease takes exactly the branch an unexpired one takes here.
TEST(CASDecommission, NoWaitRefusalNamesTheTerminalStates)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto victim = openVictim(backend);

    DB::Cas::tests::expectThrowsCodeWithMessage(ErrorCodes::ABORTED, "state = 'fenced'", [&]
    {
        Pool::openForDecommission(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");
    });
}
```

- [ ] **Step 3: Build and run to verify both fail**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1/build && flock /home/mfilimonov/workspace/ClickHouse/master/tmp/ninja.lock nice -n 10 ninja unit_tests_dbms > build_t4a.log 2>&1; echo NINJA_EXIT=$? >> build_t4a.log; tail -1 build_t4a.log
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1 && TMPDIR=build/tmp build/src/unit_tests_dbms --gtest_filter='CASServerRootClaim.RefusalsNameTheSupportedRecovery:CASDecommission.NoWaitRefusalNamesTheTerminalStates' > build/test_t4a.log 2>&1; grep -E '^\[ +(OK|FAILED) +\]' build/test_t4a.log
```
Expected: both FAILED on `wrong branch:` / missing substring (the base texts lack `SYSTEM CAS DROP POOL MEMBER`, `new <cas_server_root_id>` and `state = 'fenced'`).

- [ ] **Step 4: Rewrite `throwIfOwnerRetired` (`CasServerRoot.cpp:441-452`)**

Replace the `throw` with:

```cpp
    throw Exception(ErrorCodes::CORRUPTED_DATA,
        "CAS server-root '{}' was explicitly decommissioned (owner tombstoned at {} ms by "
        "SYSTEM CAS DROP POOL MEMBER) and refuses to start again: writer epochs of a retired id must never "
        "repeat. To run a server on this pool again, configure a new <cas_server_root_id> for this disk and "
        "keep the local uuid file. Do not clear the tombstone by hand.",
        srid, *owner.retired_at_ms);
```

- [ ] **Step 5: Rewrite the foreign-owner refusal (`CasServerRoot.cpp:542-553`)**

Replace the comment and the `throw` with:

```cpp
        /// The common cause is a regenerated local uuid file (wiped /var/lib/clickhouse, a pod recreated
        /// without a persistent volume) while the pool kept the old identity; name it and the supported
        /// ways out, with the hand delete last: it is refused anyway while the subtree holds data.
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS server-root '{}' is owned by a different server (owner server_uuid={}, ours={}) — refusing to claim. "
            "Either this server's local uuid file was regenerated (/var/lib/clickhouse wiped, or a pod recreated "
            "without a persistent volume) while the pool kept the old identity, or two servers are configured with "
            "the same <cas_server_root_id>. Recover by restoring the old uuid file; or configure a fresh "
            "<cas_server_root_id> for this disk; or, if the owning server is permanently gone, decommission it "
            "first — SYSTEM CAS DROP POOL MEMBER '{}' FROM DISK '<disk>' on a surviving member (offline: "
            "clickhouse-disks cas-drop-member) — and then start this server under a new <cas_server_root_id>. "
            "Deleting the owner object '{}' by hand is a last resort, only after verifying that NO server uses this "
            "root; it is refused while the subtree still holds data.",
            srid, u128ToHex(owner->server_uuid), u128ToHex(our_uuid), srid, key);
```

- [ ] **Step 6: Rewrite the `NoWait` refusal (`CasPool.cpp:765-768`)**

Replace the `throw` with:

```cpp
                throw Exception(ErrorCodes::ABORTED,
                    "CAS decommission '{}': pool member is alive or contended — {}. Refusing; there is no FORCE "
                    "variant. A member can be removed only once its mount slot is terminal in system.cas_mounts "
                    "(server_root_id = '{}'): state = 'terminated' (the server stopped cleanly) or state = 'fenced' "
                    "(GC fenced its lease). A hard-killed member shows state = 'expired' first and is not removable "
                    "yet: GC fences a lease whose token has stayed unchanged for about one cas_mount_lease_ttl_ms "
                    "plus one cas_mount_renew_period_ms, in its next round on any live member (SYSTEM CAS GC RUN "
                    "starts one). Then rerun this command.",
                    srid, holder, srid);
```
Update the comment above it (lines 756-759) to:
```cpp
                /// No FORCE variant, no wait-and-observe: the decommission gate treats any live-looking
                /// or foreign-owner lease as an immediate refusal, and names the terminal states it
                /// accepts, because `expired` in system.cas_mounts is wall-clock arithmetic this claim
                /// never trusts. A raced write that observed nobody has no holder to describe.
```

- [ ] **Step 7: Build and run to verify they pass, plus the suites that match on these texts**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1/build && flock /home/mfilimonov/workspace/ClickHouse/master/tmp/ninja.lock nice -n 10 ninja unit_tests_dbms > build_t4b.log 2>&1; echo NINJA_EXIT=$? >> build_t4b.log; tail -1 build_t4b.log
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1 && TMPDIR=build/tmp build/src/unit_tests_dbms --gtest_filter='CASServerRootClaim.*:CASDecommission.*:CASMount*' > build/test_t4b.log 2>&1; grep -E '^\[ +(FAILED|PASSED) +\]' build/test_t4b.log
grep -rn 'alive or contended\|explicitly decommissioned' /home/mfilimonov/workspace/ClickHouse/cas-95-1/tests/integration/test_cas_drop_pool_member/test.py
```
Expected: `[  PASSED  ]`, no `FAILED`; the grep still finds both substrings at `test.py:190` and `:311` (the texts kept them, so the integration test needs no change).

- [ ] **Step 8: Docs**

`docs/en/antalya/cas/operations/troubleshooting.md`: after the row starting `| \`SYSTEM CAS DROP POOL MEMBER\` returns a non-empty \`warnings\` column |` (line 24) add three rows:
```
| `SYSTEM CAS DROP POOL MEMBER` refuses with `alive or contended` although the member is down | The claim trusts no wall clock: `expired` in `system.cas_mounts` is not a certificate of death; only a terminal slot (`terminated` or `fenced`) is reclaimable | Wait until the member's row shows `state = 'terminated'` (clean stop) or `state = 'fenced'` (GC fenced the stale lease, about one `cas_mount_lease_ttl_ms` plus one `cas_mount_renew_period_ms` after its last renewal; `SYSTEM CAS GC RUN` on any live member starts a round), then rerun |
| A server refuses to start with `owned by a different server` | The owner object binds the `server_root_id` to another `server_uuid`: the local uuid file was regenerated, or two servers share one `cas_server_root_id` | Restore the uuid file (keep it on a persistent volume); or configure a fresh `cas_server_root_id`; or, if the owning server is permanently gone, decommission it first with `SYSTEM CAS DROP POOL MEMBER` from a surviving member and start this server under a new `cas_server_root_id` |
| A server refuses to start with `explicitly decommissioned` | Its `server_root_id` was retired by `SYSTEM CAS DROP POOL MEMBER`; the tombstone is permanent so writer epochs never repeat | Configure a new `cas_server_root_id` for the disk; do not clear the tombstone by hand |
```

`docs/en/antalya/cas/operations/migration.md:206-210`, the **Preconditions** paragraph: replace
```
Check `system.cas_mounts` for the member's
`state` and `last_success_age_seconds` first — a `live` row, or one with a recent lease renewal,
means the member is not a decommission candidate yet.
```
with
```
The command claims the slot without waiting and trusts no wall clock, so it accepts only a terminal
slot in `system.cas_mounts`: `state = 'terminated'` (clean stop) or `state = 'fenced'` (GC fenced the
lease). A `live` or `expired` row is refused with `alive or contended`; a hard-killed member stays
`expired` until the next GC round on any live member fences it, about one `cas_mount_lease_ttl_ms`
plus one `cas_mount_renew_period_ms` after its last renewal (`SYSTEM CAS GC RUN` starts a round).
```

`docs/en/antalya/cas/architecture/mounts-and-leases.md`: replace the owner-table rows (lines 49-50)
```
| present, `retired_at_ms` set | `CORRUPTED_DATA` — explicitly decommissioned, refuses to resume |
| present, different `server_uuid` | `CORRUPTED_DATA` — names the regenerated-uuid-file cause |
```
with
```
| present, `retired_at_ms` set | `CORRUPTED_DATA` — explicitly decommissioned; the refusal says to configure a new `cas_server_root_id` |
| present, different `server_uuid` | `CORRUPTED_DATA` — names the regenerated-uuid-file cause and, for a dead owner, `SYSTEM CAS DROP POOL MEMBER` |
```
and in the **Permanent removal** paragraph (line 263) replace `with a no-wait policy (refuses immediately if the member is alive),` with `with a no-wait policy (refuses unless the slot is terminal: \`terminated\` or \`fenced\`; an \`expired\` slot is refused until GC fences it),`.

`docs/en/sql-reference/statements/system.md`, after the paragraph ending `it deletes namespace and drain state that cannot be recovered.` (line 596) add:
```
The claim accepts only a terminal mount slot (`state` `terminated` or `fenced` in
`system.cas_mounts`). A hard-killed member shows `expired` until the next GC round fences it; until
then the command refuses with `alive or contended`.
```

- [ ] **Step 9: Commit**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
git add src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.cpp \
        src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.cpp \
        src/Disks/tests/gtest_cas_mount.cpp src/Disks/tests/gtest_cas_decommission.cpp \
        docs/en/antalya/cas/operations/troubleshooting.md docs/en/antalya/cas/operations/migration.md \
        docs/en/antalya/cas/architecture/mounts-and-leases.md docs/en/sql-reference/statements/system.md
git commit -F - <<'EOF'
Name the supported recovery in the owner and decommission refusals

The foreign-owner refusal advised a hand delete that the next start
refuses over existing data; it now names `SYSTEM CAS DROP POOL MEMBER`
for a dead owner and the hand delete last. The retired-owner refusal
advised clearing the tombstone, which would let writer epochs repeat; it
now says to configure a new `cas_server_root_id`. The `NoWait` refusal
said "wait for its lease to lapse", but the claim trusts no wall clock:
it now names the terminal states (`terminated`, `fenced`) and how GC
reaches `fenced` for a hard-killed member.

Tests: `CASServerRootClaim.RefusalsNameTheSupportedRecovery`,
`CASDecommission.NoWaitRefusalNamesTheTerminalStates` (red on the base:
substrings absent). The substrings the integration test matches on
(`alive or contended`, `explicitly decommissioned`) are kept.

Signed-off-by: Mikhail Filimonov <mfilimonov@altinity.com>
Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

---

### Task 5: Branch gate

**Files:** none modified.

- [ ] **Step 1: Full build of the committed tree**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1 && git status --short | grep -v '^??' ; echo "(must print nothing above)"
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1/build && flock /home/mfilimonov/workspace/ClickHouse/master/tmp/ninja.lock nice -n 10 ninja unit_tests_dbms > build_t5.log 2>&1; echo NINJA_EXIT=$? >> build_t5.log; tail -1 build_t5.log
```
Expected: no tracked changes; `NINJA_EXIT=0`.

- [ ] **Step 2: Run the gate**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1 && TMPDIR=build/tmp build/src/unit_tests_dbms --gtest_filter='CAS*' > build/test_gate_pr_a.log 2>&1; echo GATE_EXIT=$? >> build/test_gate_pr_a.log; grep -E '^\[ +(FAILED|PASSED) +\]|GATE_EXIT=' build/test_gate_pr_a.log
```
Expected: `[  PASSED  ]` and `GATE_EXIT=0`, no `FAILED`. Hand the log to a subagent for a concise summary; any red is a root cause to report, never a "pre-existing".

- [ ] **Step 3: Docs checks**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
git diff 2a034cdb374..HEAD -- docs/en | grep -E '^\+#' ; echo "(no new headings expected)"
git diff 2a034cdb374..HEAD --stat
git log --oneline 2a034cdb374..HEAD
```
Expected: four commits; no `+#` lines (this PR adds no headings, so no anchors are owed).

- [ ] **Step 4: Report**

Return: the four commit SHAs, the gate log path (`build/test_gate_pr_a.log`), and the PR description draft below for the controller (the PR is not opened or pushed by this plan).

PR title: `CAS member lifecycle, PR A: no nested srids, abort deletes staging, refusal texts`

PR body (per `.github/PULL_REQUEST_TEMPLATE.md`): short description from the four commit subjects; Changelog category `Improvement`; Changelog entry: "CAS: a new `cas_server_root_id` may not contain `/`; an aborted transaction deletes its S3 staging object; `SYSTEM CAS DROP POOL MEMBER` and the owner refusals name the state or verb that resolves them; docs state the `AbortIncompleteMultipartUpload` bucket rule."; Documentation entry checkbox ticked. Link `Related:` to the spec commit on `cas-gc-rebuild` and the backlog ids it closes or advances (CAS-157, CAS-156.2, CAS-156.3, CAS-369; CAS-369.1 stays open).

## Risks / notes

- The nesting rule sits after the emptiness check and before the owner create (Task 1 Step 4). Consequences: a nested id over existing data still gets the identity-lost refusal (`EveryCatalogLifecycleStateBlocksOwnerAndEpochRecreation`, `ManifestAndLooseRootDebrisStillBlockRecreation` keep their reason); a nested id over an empty subtree never reaches the create, so the two tests that depended on reaching it moved to a flat id (Step 2b). No test config or doc example in the tree uses a nested `cas_server_root_id` (checked at `2a034cdb374`: values are flat names or `{replica}` macros).
- The decommission adopt-from-lease fallback for a nested id is now refused (Review Focus 2). Such a member can still be decommissioned while its owner object exists; the refusal text says so.
- The native-prefix guard depends on `FakeGenerationObjectStorage` accepting a decommission claim (conditional replace on the mount, token-exact delete of the listed key). The spec names this test for PR A; a failure caused by the fake's dialect is a finding, not a reason to patch production code.
- The integration test `test_cas_drop_pool_member` is unchanged: both substrings it matches on survive. It is not run by this plan (CI runs it).
- Not in PR A: `slot_removed` rename, tombstone-first decommission, `owner_life`, rejoin, `retired` in `system.cas_mounts` (PR B and C); the re-add procedure docs CAS-156.2 asks for land with PR C's rejoin.
