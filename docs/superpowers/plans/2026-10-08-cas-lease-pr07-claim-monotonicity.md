# CAS lease PR 7: claim monotonicity — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:executing-plans (or
> superpowers:subagent-driven-development) to implement this plan task by task. Steps use checkbox
> (`- [ ]`) syntax for tracking.

**Goal:** a mount claim never installs a `writer_epoch` lower than or equal to the epoch of the
same-`server_uuid` body it would replace — not at startup, not at decommission, not in the remount
reclaim. A delayed start that reads a newer body fails with `ABORTED` instead of replacing it. A
non-fenced body of our own `(uuid, epoch)` is `LiveDoubleStart` with no write (the refresh branch
"own claim replayed" is deleted). `decodeServerEpoch` rejects `next_writer_epoch = 0`. Closes CAS-178
and the decode half of CAS-160.

**Architecture:** all three paths (`Pool::mountWritable` for start and decommission,
`Pool::tryRemountOnce` for remount) reach the slot through `claimMount` in `CA/Pool/CasServerRoot.cpp`,
so the rule is one check there: after the foreign-owner check, a newer epoch in the slot throws; an
equal epoch is `FencedSelf` (fenced) or `LiveDoubleStart` (not fenced), never a write; only an older
epoch reaches the four reclaim grounds. `claimMountAwaitingExpiry` returns at once on a
`LiveDoubleStart` that carries our own pair, because no observation can authorize replacing it and the
loop would otherwise never end. The zero check is one condition in `decodeServerEpoch`.

**Tech stack:** C++23, gtest (`unit_tests_dbms`), `InMemoryBackend` subclasses as fault seams,
injected clocks. TLA+ (`CaCasMountCore`) run only.

**Spec:** `/home/mfilimonov/workspace/ClickHouse/master/docs/superpowers/specs/2026-10-07-cas-mount-lease-series-design.md`,
section 2 "Монотонность claim" and "Старт" (first paragraph), section 7 row 7 in both tables, section 9
row "`CaCasMountCore`, шаг 1".

**Backlog tasks** (master worktree, `docs/superpowers/cas/backlog/tasks/`): CAS-178 (acceptance: a
TLA+ run shows whether a regressed epoch can skip a seal; if it can, `claimMount` refuses the
regression and a gtest pins it) and CAS-160 (acceptance #1 only: decoding a zero counter throws
`CORRUPTED_DATA`; the `MountFence` fields half is PR 15 in the spec).

**Code base:** `/home/mfilimonov/workspace/ClickHouse/cas-95-1`, base `5b7c9f503df`
(`altinity/antalya-26.6`). `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`.

## Global Constraints

- One topic per PR; the PR is self-contained and leaves a working product; no behaviour outside the
  spec row changes. Changelog: one sentence, category per `.github/PULL_REQUEST_TEMPLATE.md`.
- Allman braces; no `sleep` to fix races; tests use injected clocks and deterministic barriers.
- A test that expects `LOGICAL_ERROR` is a death test under `DEBUG_OR_SANITIZER_BUILD` (the error
  aborts there); prefer injecting `NOT_IMPLEMENTED`/`BAD_ARGUMENTS` where the rule allows.
- Shared code outside `CA/` (`src/IO/S3`, `base/poco`, `src/Daemon`, `src/Storages/System`) is a fork
  patch: minimal, motivated, portable; say in the plan which files outside `CA/` are touched and why.
- Comments explain why, not what; no references to plans, reviews, backlog ids or history in code.
- Prose style for PR text and comments: point first, plain words, backticks for names, `f` not `f()`.
- Build: `ninja -C build <target>` without `-j`, output to a log file in the build dir; the gate is
  `build/src/unit_tests_dbms --gtest_filter='CAS*'` (release) and the same in `build_asan`.
- Commit exact files only (`git add -- <path> …`), never a directory, never `-A`; no rebase/amend.

## Review Focus

- **Check order in `claimMount`.** Foreign uuid first (unchanged), then `existing.writer_epoch >
  our_epoch` → throw, then equal → `FencedSelf` / `LiveDoubleStart`, then the reclaim grounds. A newer
  body that is `gc_fenced` or clean-marked must throw too: those are exactly the grounds that used to
  reclaim it. `LowerEpochOverANewerBodyFailsWithoutWriting` covers all five shapes.
- **Throw, not a new `Kind`.** A lower epoch is an error, not a state to wait out: returned as
  `LiveDoubleStart` it would enter `claimMountAwaitingExpiry`'s observation and, once the token held
  stable, loop forever (the reclaim would keep refusing). No caller switches exhaustively on `Kind`, and
  PR 11b replaces the result type anyway.
- **The early return in `claimMountAwaitingExpiry`.** Without it a stable same-pair body loops forever:
  `proven_dead` never reclaims a same pair, and an unchanged token never counts a restart.
  `AwaitingExpiryReturnsAtOnceOnABodyOfOurOwnPair` fails with a thrown "never ended" (not a hang) if it
  is removed.
- **Own sent attempt still confirms.** Removing the refresh branch is safe only because the request
  engine settles a landed-but-unacknowledged write inside the same call (`CasOperation::writeLoop`:
  precondition moved and our bytes at the key → `Committed`). `AConfirmedLandedReclaimIsStillClaimed`
  pins that for the reclaim branch, green before and after.
- **Existing tests that changed.** Five tests used the refresh branch as a fixture or as the subject;
  each now writes the slot directly or targets the reclaim branch (Task 5, Step 6). The enumeration
  method is in that step; the gate run before the update must fail exactly those five.
- **Not changed:** `CasDecommission.cpp` keeps its `next_writer_epoch != 0` guard (now unreachable
  through the decoder; it still protects the `- 1` below it). `allocateWriterEpoch`'s absent-object
  clamp to 1 stays: it applies to a default `ServerEpoch`, not to decoded bytes. `MountLeaseRenewer`
  adoption is untouched (PR 11b).

## Files

| File | Change |
|---|---|
| `CA/Formats/CasServerRootFormats.cpp` | `decodeServerEpoch`: zero → `CORRUPTED_DATA` |
| `CA/Formats/CasServerRootFormats.h` | `decodeServerEpoch` doc: names the zero rejection |
| `CA/Pool/CasServerRoot.cpp` | `claimMount`: newer-epoch throw, same-pair `LiveDoubleStart`, refresh branch deleted; `claimMountAwaitingExpiry`: early return on our own pair |
| `CA/Pool/CasServerRoot.h` | docs of `MountPriorState`, `claimMount`'s decision list, `MountClaimResult`, `claimMountAwaitingExpiry` |
| `CA/Pool/CasPool.cpp` | two comments that list "same-epoch refresh" as a claim outcome |
| `src/Disks/tests/gtest_cas_server_root_format.cpp` | decode test |
| `src/Disks/tests/gtest_cas_mount.cpp` | 6 new tests, 2 fixture backends, 2 tests updated |
| `src/Disks/tests/gtest_cas_mount_claim_conflicts.cpp` | 3 tests updated (1 fixture rewrite, 2 retargeted and renamed) |
| `docs/en/antalya/cas/architecture/mounts-and-leases.md` | claim-outcome tables and the slot diagram |

No file outside `CA/`, the gtest directory and `docs/en/antalya/cas` is touched.

---

## Task 1: Branch

- [ ] **Step 1: Create the branch from the base**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
git status --short -- src/Disks docs/en/antalya    # expect no output
git switch -c cas-lease-pr07-claim-monotonicity 5b7c9f503df
```

If `git status` shows edits under `src/Disks` or `docs/en/antalya`, stop and report: they belong to
another agent. On 2026-10-08 this checkout carried uncommitted edits to `CasServerRoot.cpp`,
`CasServerRoot.h` and `gtest_cas_mount.cpp`, the files this PR changes. In that case work in a separate
worktree instead: `cd ~/workspace/ClickHouse && bash create_subtree_branch.sh cas-lease-pr07 5b7c9f503df`
(hardlinks submodules and configures `build/`; a plain `git worktree add` re-clones every submodule),
then use that directory in place of `cas-95-1` in every command below. Its `build_asan` must be
configured before Task 7.

## Task 2: Model gate — `CaCasMountCore` step 1 (before any production change)

The spec requires the two-incarnation model before the implementation. Another task extends
`CaCasMountCore.tla` with incarnations and a claim-regression sabotage. This task only runs it.

- [ ] **Step 1: Confirm the model extension is committed**

```bash
cd /home/mfilimonov/workspace/ClickHouse/master
git status --short -- docs/superpowers/models/CaCasMountCore.tla docs/superpowers/models/run_mount.sh
git log -1 --format='%h %s' -- docs/superpowers/models/CaCasMountCore.tla
```

Expect no `git status` output. If either file is still modified, stop: the model task has not landed.

- [ ] **Step 2: Run the suite**

```bash
cd /home/mfilimonov/workspace/ClickHouse/master
mkdir -p tmp/lease-series
nice -n 10 docs/superpowers/models/run_mount.sh > tmp/lease-series/pr07-run_mount.log 2>&1; echo RUN_EXIT=$?
```

Expect `RUN_EXIT=0` (the script exits nonzero if any expectation is unmet). In the log, the
step-1 rows must be present with verdict OK: the claim-regression sabotage (a violation of the
claim-monotonicity invariant) and the step-1 witnesses (two live incarnations of one uuid; a delayed
start over a newer body). In the working tree on 2026-10-08 they were named `sab_claimregress`
(`ClaimNeverRegressesEpoch`), `witness_twolive` and `witness_delayedstart`.

- [ ] **Step 3: Record**

Keep the model commit SHA and the RESULT lines of those rows; they go into the PR description
("Model" line, Task 7). Do not edit model files.

## Task 3: `decodeServerEpoch` rejects zero

**Files:** `src/Disks/tests/gtest_cas_server_root_format.cpp`, `src/Disks/tests/gtest_cas_mount.cpp`,
`CA/Formats/CasServerRootFormats.cpp`, `CA/Formats/CasServerRootFormats.h`.

**Consumes:** `currentFormatHeader` (`cas_format_test_battery.h`), `Ops`, `mustCommit`,
`emptyCatalogObservation` (anonymous namespace of `gtest_cas_mount.cpp`),
`DB::Cas::tests::expectThrowsCode` (`cas_test_helpers.h`).

**Produces:** tests `CASServerEpochFormat.ZeroNextWriterEpochIsRejected`,
`CASServerRootEpoch.ZeroCounterFailsAllocationWithoutWriting`.

- [ ] **Step 1: Write the decode test**

In `gtest_cas_server_root_format.cpp`, directly after `TEST(CASFormatBattery, ServerEpoch) { ... }`:

```cpp
TEST(CASServerEpochFormat, ZeroNextWriterEpochIsRejected)
{
    const String data = currentFormatHeader("cas_epoch") + "{\"next_writer_epoch\":\"0\"}\n";
    try
    {
        decodeServerEpoch(data);
        FAIL() << "expected CORRUPTED_DATA";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::CORRUPTED_DATA);
    }
}
```

- [ ] **Step 2: Write the allocation test**

In `gtest_cas_mount.cpp`, directly after `TEST(CASServerRootEpoch, AllocatorIsMonotoneAndSurvivesMountConcept) { ... }`:

```cpp
/// No writer emits a zero counter; an allocation over one would hand out writer_epoch 0.
TEST(CASServerRootEpoch, ZeroCounterFailsAllocationWithoutWriting)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    Ops ops(b);
    mustCommit(ops.op.create(l.epochKey("r"), encodeServerEpoch(ServerEpoch{.next_writer_epoch = 0}), Retry::standard()),
               "zero counter");
    const auto before = ops.op.read(l.epochKey("r"), Retry::standard());
    ASSERT_TRUE(before.has_value());

    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&]
    {
        (void)allocateWriterEpoch(ops.op, l, "r", EpochMintPolicy::NormalMount, 0, emptyCatalogObservation());
    });
    EXPECT_EQ(ops.op.read(l.epochKey("r"), Retry::standard())->bytes, before->bytes);
}
```

- [ ] **Step 3: Build and run; expect red**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
ninja -C build unit_tests_dbms > build/ninja_pr07_t3.log 2>&1; echo NINJA_EXIT=$?
build/src/unit_tests_dbms --gtest_filter='CASServerEpochFormat.ZeroNextWriterEpochIsRejected:CASServerRootEpoch.ZeroCounterFailsAllocationWithoutWriting' \
    > build/test_pr07_t3_red.log 2>&1; echo TEST_EXIT=$?
```

Expect `NINJA_EXIT=0`, `TEST_EXIT=1`, and in the log `Failed` with `expected CORRUPTED_DATA` for the
first test and `expected DB::Exception` for the second (the base allocates epoch 0).

- [ ] **Step 4: Implement**

In `CA/Formats/CasServerRootFormats.cpp`, `decodeServerEpoch`, replace

```cpp
    if (!saw)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS server-epoch: missing next_writer_epoch");
```

with

```cpp
    if (!saw || e.next_writer_epoch == 0)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS server-epoch: missing or zero next_writer_epoch");
```

In `CA/Formats/CasServerRootFormats.h`, the doc of `decodeServerEpoch`, replace
`malformed input, a missing \`next_writer_epoch\`,` with
`malformed input, a missing or zero \`next_writer_epoch\`,`.

- [ ] **Step 5: Run again; expect green**

Same commands as Step 3, log `build/test_pr07_t3_green.log`. Expect `TEST_EXIT=0`. Then the format
battery, which encodes epoch 7, must stay green:

```bash
build/src/unit_tests_dbms --gtest_filter='CASFormatBattery.*:CASServerRoot*:CASMount*' > build/test_pr07_t3_neighbours.log 2>&1; echo TEST_EXIT=$?
```

Expect `TEST_EXIT=0`.

- [ ] **Step 6: Commit**

```bash
git add -- src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasServerRootFormats.cpp \
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasServerRootFormats.h \
    src/Disks/tests/gtest_cas_server_root_format.cpp src/Disks/tests/gtest_cas_mount.cpp
git commit -m "Reject a zero next_writer_epoch when decoding the epoch counter

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

## Task 4: A claim never replaces a newer epoch

**Files:** `src/Disks/tests/gtest_cas_mount.cpp`, `CA/Pool/CasServerRoot.cpp`.

**Consumes:** `claimMount`, `MountClaimResult`, `Pool::open`, `PoolConfig::{boot_ms_fn, wait_sleep_fn}`,
`InMemoryBackend::{read, write}` primitives with `Raw::{bytes, value}`,
`DB::Cas::tests::expectThrowsCodeWithMessage`.

**Produces:** tests `CASClaimMountMonotonicity.LowerEpochOverANewerBodyFailsWithoutWriting`,
`CASMountStartup.DelayedStartOverANewerIncarnationFailsWithoutWaiting`; fixture `NewerStartBackend`;
the error text `newer than the writer_epoch` (asserted).

- [ ] **Step 1: Declare `ABORTED` in the test file**

In `gtest_cas_mount.cpp`, `namespace DB::ErrorCodes`, add `extern const int ABORTED;` above
`extern const int BAD_ARGUMENTS;`.

- [ ] **Step 2: Write the unit test**

Directly after `TEST(CASClaimMount, SameEpochFencedIsNotRefreshable) { ... }`:

```cpp
/// An older allocation that reaches the slot late must not replace a newer incarnation of this server,
/// whatever certifies that incarnation's end: recovery seals only epochs below the live one.
TEST(CASClaimMountMonotonicity, LowerEpochOverANewerBodyFailsWithoutWriting)
{
    enum class Shape { Live, GcFenced, CleanFarewell, ProvenDead, UnsafeAuthorized };
    for (const Shape shape : {Shape::Live, Shape::GcFenced, Shape::CleanFarewell, Shape::ProvenDead,
                              Shape::UnsafeAuthorized})
    {
        SCOPED_TRACE(static_cast<int>(shape));
        auto b = std::make_shared<InMemoryBackend>();
        Layout l("p");
        Ops ops(b);
        const String key = l.mountKey("r");
        ASSERT_EQ(claimMount(ops.op, l, "r", UInt128(1), /*our_epoch=*/8, 1000, 30000).kind, MountClaimResult::Claimed);
        if (shape == Shape::GcFenced || shape == Shape::CleanFarewell)
        {
            const auto got = ops.op.read(key, Retry::standard());
            ASSERT_TRUE(got.has_value());
            MountLease body = decodeMountLease(got->bytes);
            ++body.seq;
            if (shape == Shape::GcFenced)
                body.gc_fenced = true;
            else
                body.min_active_build_sequence = std::numeric_limits<uint64_t>::max();
            mustCommit(ops.op.replace(key, encodeMountLease(body), got->etag, Retry::standard()), "terminal newer body");
        }
        const auto before = ops.op.read(key, Retry::standard());
        ASSERT_TRUE(before.has_value());
        const std::optional<Etag> token = before->etag;

        DB::Cas::tests::expectThrowsCodeWithMessage(DB::ErrorCodes::ABORTED, "newer than the writer_epoch", [&]
        {
            (void)claimMount(ops.op, l, "r", UInt128(1), /*our_epoch=*/7, 2000, 30000,
                             shape == Shape::ProvenDead ? token : std::nullopt, /*sink=*/{},
                             shape == Shape::UnsafeAuthorized ? token : std::nullopt);
        });
        EXPECT_EQ(ops.op.read(key, Retry::standard())->bytes, before->bytes) << "the newer body was replaced";
    }
}
```

- [ ] **Step 3: Write the startup test and its fixture**

In the anonymous namespace of `gtest_cas_mount.cpp`, directly after `class EpochConflictBackend { ... };`:

```cpp
/// Between this start's epoch allocation and its claim read, a second start of the same server
/// allocates the next epoch and installs its live body. Armed by the test after the bootstrap open.
class NewerStartBackend : public InMemoryBackend
{
public:
    String epoch_key;
    String mount_key;
    bool arm_on_next_epoch_write = false;
    std::optional<uint64_t> newer_epoch;

    std::expected<String, RawConflict> write(
        const String & key, const String & bytes,
        const std::optional<String> & expected_value, TransportAccess & access) override
    {
        auto result = InMemoryBackend::write(key, bytes, expected_value, access);
        if (arm_on_next_epoch_write && key == epoch_key && result)
        {
            arm_on_next_epoch_write = false;
            install_on_next_mount_read = true;
        }
        return result;
    }

    std::optional<Raw> read(const String & key, TransportAccess & access) override
    {
        if (install_on_next_mount_read && key == mount_key)
        {
            install_on_next_mount_read = false;
            installNewerStart(access);
        }
        return InMemoryBackend::read(key, access);
    }

private:
    bool install_on_next_mount_read = false;

    void installNewerStart(TransportAccess & access)
    {
        const auto epoch = InMemoryBackend::read(epoch_key, access);
        const auto mount = InMemoryBackend::read(mount_key, access);
        if (!epoch || !mount)
            throw std::runtime_error("NewerStartBackend: a control object is missing");
        ServerEpoch counter = decodeServerEpoch(epoch->bytes);
        newer_epoch = counter.next_writer_epoch;
        ++counter.next_writer_epoch;
        if (!InMemoryBackend::write(epoch_key, encodeServerEpoch(counter), epoch->value, access))
            throw std::runtime_error("NewerStartBackend: the epoch bump conflicted");
        const MountLease newer{
            .server_uuid = UInt128(1),
            .writer_epoch = *newer_epoch,
            .hostname = "newer-start",
            .pid = 2,
            .started_at_ms = 1,
            .seq = 1,
            .expires_at_ms = 4'000'000'000'000,
            .write_attempt_id = UInt128{0xB}};
        if (!InMemoryBackend::write(mount_key, encodeMountLease(newer), mount->value, access))
            throw std::runtime_error("NewerStartBackend: the mount install conflicted");
    }
};
```

Directly after `TEST(CASMountStartup, WriterEpochStrictlyIncreasesAcrossReopen) { ... }`:

```cpp
TEST(CASMountStartup, DelayedStartOverANewerIncarnationFailsWithoutWaiting)
{
    auto b = std::make_shared<NewerStartBackend>();
    const Layout l("p");
    b->epoch_key = l.epochKey("r");
    b->mount_key = l.mountKey("r");
    {
        /// Bootstrap: owner, counter and a clean farewell of epoch 1.
        auto first = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "r"});
    }
    b->arm_on_next_epoch_write = true;

    /// Shared, heap-owned: the Pool may outlive this frame through a background publish.
    auto fake_boot = std::make_shared<std::atomic<uint64_t>>(0);
    auto waits = std::make_shared<std::atomic<uint64_t>>(0);
    DB::Cas::tests::expectThrowsCodeWithMessage(DB::ErrorCodes::ABORTED, "newer than the writer_epoch", [&]
    {
        (void)Pool::open(b, PoolConfig{
            .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "r",
            .boot_ms_fn = [fake_boot] { return fake_boot->load(); },
            .wait_sleep_fn = [fake_boot, waits](uint64_t ms)
            {
                *fake_boot += ms;
                ++(*waits);
            }});
    });
    ASSERT_TRUE(b->newer_epoch.has_value()) << "the newer start was never installed";
    EXPECT_EQ(waits->load(), 0u) << "a newer incarnation is refused, not observed";
    Ops ops(b);
    const MountLease slot = decodeMountLease(ops.op.read(l.mountKey("r"), Retry::standard())->bytes);
    EXPECT_EQ(slot.writer_epoch, *b->newer_epoch);
    EXPECT_EQ(slot.write_attempt_id, UInt128{0xB});
}
```

`PoolConfig` designated initializers must follow the declaration order; `boot_ms_fn` and
`wait_sleep_fn` already appear in that order in `CASMountStartup.StaleSelfMountReclaimedAfterWait`.

- [ ] **Step 4: Build and run; expect red**

```bash
ninja -C build unit_tests_dbms > build/ninja_pr07_t4.log 2>&1; echo NINJA_EXIT=$?
build/src/unit_tests_dbms --gtest_filter='CASClaimMountMonotonicity.LowerEpochOverANewerBodyFailsWithoutWriting:CASMountStartup.DelayedStartOverANewerIncarnationFailsWithoutWaiting' \
    > build/test_pr07_t4_red.log 2>&1; echo TEST_EXIT=$?
```

Expect `TEST_EXIT=1`. The unit test fails in all five traces: `Live` with `expected DB::Exception`
(the base returns `LiveDoubleStart`), the other four with `expected DB::Exception` and `the newer body
was replaced` (the base reclaims). The startup test fails with `expected DB::Exception` and
`waits` nonzero (the base observes the newer body, then reclaims it with the lower epoch).

- [ ] **Step 5: Implement**

In `CA/Pool/CasServerRoot.cpp`, `claimMount`, directly after the foreign-owner block (the one ending
`return {.kind = MountClaimResult::ForeignOwner, .body = existing, .etag = std::nullopt};` and its
closing brace), insert:

```cpp
    /// An older allocation that reached the slot late must not replace a newer incarnation of this
    /// server: recovery seals only epochs below the live one, so the newer epoch's work would be lost.
    if (existing.writer_epoch > our_epoch)
    {
        emitMountEvent(sink, CasEventType::MountConflict, srid, "newer_epoch", &existing,
            "same server_uuid, newer writer_epoch in the mount slot — refusing to replace it");
        throw Exception(ErrorCodes::ABORTED,
            "CAS mount slot '{}' holds writer_epoch {} of this server_uuid, newer than the writer_epoch {} "
            "this claim allocated: another start or a decommission of server_root_id '{}' allocated after "
            "this one. Refusing to replace the newer incarnation ({})",
            key, existing.writer_epoch, our_epoch, srid, describeMountHolder(existing));
    }
```

In the same function, in the comment above the reclaim grounds, replace
`/// Same uuid, DIFFERENT epoch: reclaim ONLY on a certificate of death` with
`/// Same uuid, OLDER epoch: reclaim ONLY on a certificate of death`.

- [ ] **Step 6: Run again; expect green**

Same commands as Step 4, log `build/test_pr07_t4_green.log`. Expect `TEST_EXIT=0`. Then the
neighbouring suites:

```bash
build/src/unit_tests_dbms --gtest_filter='CASMount*:CASClaimMount*:CASServerRoot*:CASHeartbeat*:CASPool*:CASMountRemount*:CASDecommission*' \
    > build/test_pr07_t4_neighbours.log 2>&1; echo TEST_EXIT=$?
```

Expect `TEST_EXIT=0`. No existing test claims with a lower epoch than a same-uuid body in the slot
(Task 5, Step 6 gives the enumeration).

- [ ] **Step 7: Commit**

```bash
git add -- src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.cpp \
    src/Disks/tests/gtest_cas_mount.cpp
git commit -m "Refuse a mount claim whose writer_epoch is older than the slot's

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

## Task 5: Our own `(uuid, epoch)` in the slot is never written

**Files:** `src/Disks/tests/gtest_cas_mount.cpp`, `src/Disks/tests/gtest_cas_mount_claim_conflicts.cpp`,
`CA/Pool/CasServerRoot.cpp`, `CA/Pool/CasServerRoot.h`, `CA/Pool/CasPool.cpp`.

**Consumes:** `allocateWriterEpoch`, `claimMount`, `claimMountAwaitingExpiry`, `Poco::TimeoutException`
(`<Poco/Exception.h>`, already included), `MountSlotRaceBackend`, `OperationForTest`.

**Produces:** tests `CASClaimMountMonotonicity.TwoAllocatorsSharingAnEpochDoNotShareTheSlot`,
`CASClaimMountMonotonicity.AwaitingExpiryReturnsAtOnceOnABodyOfOurOwnPair`,
`CASClaimMountMonotonicity.AConfirmedLandedReclaimIsStillClaimed`; fixtures `EpochCollisionBackend`,
`LandedReplaceAckLostOnceBackend`; audit branch `same_epoch_occupied`.

- [ ] **Step 1: Write the fixtures**

In the anonymous namespace of `gtest_cas_mount.cpp`, directly after `class NewerStartBackend { ... };`:

```cpp
/// A competitor's conditional bump of the epoch counter lands with the very bytes ours sends, and ours
/// is lost unapplied. Our resolve read then finds our bytes under a moved precondition and reports the
/// allocation committed, so two allocators hold one writer_epoch.
class EpochCollisionBackend : public InMemoryBackend
{
public:
    String epoch_key;
    bool armed = false;

    std::expected<String, RawConflict> write(
        const String & key, const String & bytes,
        const std::optional<String> & expected_value, TransportAccess & access) override
    {
        if (armed && expected_value && key == epoch_key)
        {
            armed = false;
            if (!InMemoryBackend::write(key, bytes, expected_value, access))
                throw std::runtime_error("EpochCollisionBackend: the competitor's bump conflicted");
            throw Poco::TimeoutException("EpochCollisionBackend: response lost, our attempt did not apply");
        }
        return InMemoryBackend::write(key, bytes, expected_value, access);
    }
};

/// Applies the first conditional write of `key` after arming, then loses its response.
class LandedReplaceAckLostOnceBackend : public InMemoryBackend
{
public:
    String key;
    bool armed = false;

    std::expected<String, RawConflict> write(
        const String & k, const String & bytes,
        const std::optional<String> & expected_value, TransportAccess & access) override
    {
        if (armed && expected_value && k == key)
        {
            armed = false;
            (void)InMemoryBackend::write(k, bytes, expected_value, access);
            throw Poco::TimeoutException("LandedReplaceAckLostOnceBackend: simulated lost PUT response");
        }
        return InMemoryBackend::write(k, bytes, expected_value, access);
    }
};
```

- [ ] **Step 2: Write the three tests**

Directly after `TEST(CASClaimMountMonotonicity, LowerEpochOverANewerBodyFailsWithoutWriting) { ... }`:

```cpp
TEST(CASClaimMountMonotonicity, TwoAllocatorsSharingAnEpochDoNotShareTheSlot)
{
    auto b = std::make_shared<EpochCollisionBackend>();
    Layout l("p");
    Ops ops(b);
    b->epoch_key = l.epochKey("r");
    ASSERT_EQ(allocateWriterEpoch(ops.op, l, "r", EpochMintPolicy::NormalMount, 0, emptyCatalogObservation()), 1u);

    b->armed = true;
    const uint64_t shared_epoch
        = allocateWriterEpoch(ops.op, l, "r", EpochMintPolicy::NormalMount, 0, emptyCatalogObservation());
    ASSERT_FALSE(b->armed) << "the collision was never injected";
    ASSERT_EQ(shared_epoch, 2u);
    /// One bump past the shared epoch: the competitor's, which our resolve read took for ours.
    ASSERT_EQ(decodeServerEpoch(ops.op.read(l.epochKey("r"), Retry::standard())->bytes).next_writer_epoch, 3u);

    /// The competitor claims first.
    ASSERT_EQ(claimMount(ops.op, l, "r", UInt128(1), shared_epoch, 1000, 30000).kind, MountClaimResult::Claimed);
    const auto competitor = ops.op.read(l.mountKey("r"), Retry::standard());
    ASSERT_TRUE(competitor.has_value());

    const MountClaimResult ours = claimMount(ops.op, l, "r", UInt128(1), shared_epoch, 2000, 30000);
    EXPECT_EQ(ours.kind, MountClaimResult::LiveDoubleStart);
    ASSERT_TRUE(ours.body.has_value());
    EXPECT_EQ(ours.body->write_attempt_id, decodeMountLease(competitor->bytes).write_attempt_id);
    ASSERT_TRUE(ours.etag.has_value());
    EXPECT_TRUE(*ours.etag == competitor->etag);
    EXPECT_EQ(ops.op.read(l.mountKey("r"), Retry::standard())->bytes, competitor->bytes) << "the slot was written";
}

TEST(CASClaimMountMonotonicity, AwaitingExpiryReturnsAtOnceOnABodyOfOurOwnPair)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    Ops ops(b);
    /// Another process allocated epoch 7 as well; its body is in the slot.
    ASSERT_EQ(claimMount(ops.op, l, "r", UInt128(1), 7, 1000, 100).kind, MountClaimResult::Claimed);
    const auto before = ops.op.read(l.mountKey("r"), Retry::standard());
    ASSERT_TRUE(before.has_value());

    uint64_t mono = 0;
    int sleeps = 0;
    const auto r = claimMountAwaitingExpiry(ops.op, l, "r", UInt128(1), /*our_epoch=*/7,
        [] { return uint64_t{1000}; }, [&] { return mono; }, /*ttl_ms=*/100, /*poll_interval_ms=*/20,
        [&](uint64_t ms)
        {
            mono += ms;
            if (++sleeps > 100)
                throw std::runtime_error("the observation of our own (uuid, epoch) never ended");
        });
    EXPECT_EQ(r.kind, MountClaimResult::LiveDoubleStart);
    EXPECT_EQ(sleeps, 0) << "no observation can authorize replacing our own pair";
    EXPECT_EQ(ops.op.read(l.mountKey("r"), Retry::standard())->bytes, before->bytes);
}

/// The engine settles our own landed attempt inside the write call, so a claim never meets its own body
/// on a later call. Green before and after the refresh branch is removed.
TEST(CASClaimMountMonotonicity, AConfirmedLandedReclaimIsStillClaimed)
{
    auto b = std::make_shared<LandedReplaceAckLostOnceBackend>();
    Layout l("p");
    Ops ops(b);
    b->key = l.mountKey("r");
    ASSERT_EQ(claimMount(ops.op, l, "r", UInt128(1), 7, 1000, 30000).kind, MountClaimResult::Claimed);
    {
        const auto got = ops.op.read(b->key, Retry::standard());
        ASSERT_TRUE(got.has_value());
        MountLease fenced = decodeMountLease(got->bytes);
        fenced.gc_fenced = true;
        ++fenced.seq;
        mustCommit(ops.op.replace(b->key, encodeMountLease(fenced), got->etag, Retry::standard()), "fence-out");
    }

    b->armed = true;
    const MountClaimResult r = claimMount(ops.op, l, "r", UInt128(1), 8, 2000, 30000);
    ASSERT_FALSE(b->armed) << "the lost response was never injected";
    EXPECT_EQ(r.kind, MountClaimResult::Claimed);
    EXPECT_EQ(r.prior, MountPriorState::Fenced);
    ASSERT_TRUE(r.body.has_value());
    const MountLease slot = decodeMountLease(ops.op.read(b->key, Retry::standard())->bytes);
    EXPECT_EQ(slot.writer_epoch, 8u);
    EXPECT_EQ(slot.write_attempt_id, r.body->write_attempt_id);
}
```

- [ ] **Step 3: Build and run; expect two red, one green**

```bash
ninja -C build unit_tests_dbms > build/ninja_pr07_t5.log 2>&1; echo NINJA_EXIT=$?
build/src/unit_tests_dbms --gtest_filter='CASClaimMountMonotonicity.*' > build/test_pr07_t5_red.log 2>&1; echo TEST_EXIT=$?
```

Expect `TEST_EXIT=1` with exactly two failures:
`TwoAllocatorsSharingAnEpochDoNotShareTheSlot` (`ours.kind` is `0` — `Claimed` — where `1` —
`LiveDoubleStart` — is expected, and `the slot was written`) and
`AwaitingExpiryReturnsAtOnceOnABodyOfOurOwnPair` (`r.kind` is `0`). `AConfirmedLandedReclaimIsStillClaimed`
and `LowerEpochOverANewerBodyFailsWithoutWriting` pass. If the first test fails at
`ASSERT_EQ(shared_epoch, 2u)` instead, the engine did not take the competitor's bytes for ours: stop
and report — the collision the spec names is then not reachable this way.

- [ ] **Step 4: Implement the `claimMount` change**

In `CA/Pool/CasServerRoot.cpp`, `claimMount`, replace the whole same-epoch block — from the comment
line `/// Same uuid + same epoch: it is OUR OWN claim — but a FENCED body is terminal for this` through
the closing brace of `if (existing.writer_epoch == our_epoch) { ... }` (the block that ends with
`"own claim replayed — refreshed seq + expiry");` and `return {.kind = MountClaimResult::Claimed, ...};`)
— with:

```cpp
    /// Our own `(uuid, epoch)` is in the slot, but this call has written nothing: the engine settles a
    /// sent attempt inside the write itself. Another process was allocated the same epoch, and its body
    /// is never replaced. A fenced one is terminal for the pair; the caller takes a fresh epoch.
    if (existing.writer_epoch == our_epoch)
    {
        if (existing.gc_fenced)
        {
            emitMountEvent(sink, CasEventType::MountConflict, srid, "fenced_by_gc", &existing,
                "own (uuid, epoch) mount slot is GC-fenced — terminal for this incarnation; "
                "recover with a fresh writer_epoch");
            return {.kind = MountClaimResult::FencedSelf, .body = existing, .etag = std::nullopt};
        }
        emitMountEvent(sink, CasEventType::MountConflict, srid, "same_epoch_occupied", &existing,
            "own (uuid, epoch) is already in the mount slot, written by another process — refusing to write");
        return {.kind = MountClaimResult::LiveDoubleStart, .body = existing, .etag = got->etag};
    }
```

The `fenced_by_gc` event and its text are unchanged (`CASMountAudit` tests pin `branch == "fenced_by_gc"`).

- [ ] **Step 5: Run; expect the loop to be the last red**

```bash
ninja -C build unit_tests_dbms > build/ninja_pr07_t5b.log 2>&1; echo NINJA_EXIT=$?
build/src/unit_tests_dbms --gtest_filter='CASClaimMountMonotonicity.*' > build/test_pr07_t5_mid.log 2>&1; echo TEST_EXIT=$?
```

Expect `TEST_EXIT=1` with one failure: `AwaitingExpiryReturnsAtOnceOnABodyOfOurOwnPair`, `C++ exception
with description "the observation of our own (uuid, epoch) never ended"`. This is the evidence for the
early return below.

Then add it. In `claimMountAwaitingExpiry`, directly after

```cpp
        if (r.kind != MountClaimResult::LiveDoubleStart)
            return r;
```

insert:

```cpp
        /// Observation certifies only an older epoch dead; a body of our own pair is never replaced,
        /// so waiting on it could not end.
        if (r.body && r.body->server_uuid == our_uuid && r.body->writer_epoch == our_epoch)
            return r;
```

- [ ] **Step 6: Run the full gate; expect exactly the five refresh users red**

```bash
ninja -C build unit_tests_dbms > build/ninja_pr07_t5c.log 2>&1; echo NINJA_EXIT=$?
build/src/unit_tests_dbms --gtest_filter='CAS*' > build/test_pr07_t5_gate_red.log 2>&1; echo TEST_EXIT=$?
grep '^\[  FAILED  \]' build/test_pr07_t5_gate_red.log | sort -u
```

Expect exactly these five failed tests and no others:

- `CASMountAwaitExpiry.LiveRenewingTwinTimesOutAsDoubleStart` — its sleep hook renews the twin with
  `claimMount(…, 7, …)` over `(1, 7)` and asserts `Claimed`.
- `CASMountClaim.UnsafeAuthorizationIsTokenExact` — builds its stale token with a second
  `claimMount(…, 7, …)` over `(1, 7)`.
- `CASMountClaimConflicts.SlotChangedInsideAdoptionWindow` — its racer rewrites the slot with
  `claimMount(…, 7, …)` over `(1, 7)`.
- `CASMountClaimConflicts.ALostRefreshReportsTheObservedBodyNotTheProposer` — tests the deleted branch.
- `CASMountClaimConflicts.ARacedRefreshThatObservedNothingNamesNoHolder` — tests the deleted branch.

How the list was derived: every `claimMount(` and `claimMountAwaitingExpiry(` call in
`src/Disks/tests` (five files: `gtest_cas_heartbeat.cpp`, `gtest_cas_mount.cpp`,
`gtest_cas_mount_claim_conflicts.cpp`, `gtest_cas_pool.cpp`, `gtest_cas_ref_writer.cpp`; no helper
header calls them), classified by what the slot holds at the call. Only these five meet a non-fenced
body of the same `(uuid, epoch)`; none meets a same-uuid body with a higher epoch (pool fixtures seed
the counter one above the planted body, and remount callbacks claim epoch 2 over a fenced epoch 1).
`Pool::open` and `tryRemountOnce` always allocate above the slot. If the run shows any other failure,
the derivation missed a site: stop and classify it before editing.

- [ ] **Step 7: Update the five tests**

`gtest_cas_mount.cpp`, `CASMountAwaitExpiry.LiveRenewingTwinTimesOutAsDoubleStart`: replace the
`sleep_fn` lambda body's `ASSERT_EQ(claimMount(ops.op, l, "r", UInt128(1), 7, wall, 100).kind, MountClaimResult::Claimed);`
with a direct renewal:

```cpp
        const auto got = ops.op.read(l.mountKey("r"), Retry::standard());
        ASSERT_TRUE(got.has_value());
        MountLease renewed = decodeMountLease(got->bytes);
        ++renewed.seq;
        renewed.expires_at_ms = wall + 100;
        mustCommit(ops.op.replace(l.mountKey("r"), encodeMountLease(renewed), got->etag, Retry::standard()),
                   "twin renewal");
```

`gtest_cas_mount.cpp`, `CASMountClaim.UnsafeAuthorizationIsTokenExact`: replace

```cpp
    /// `Etag` equality compares (key, value), and it is minted only through the request planes -- there
    /// is no cross-key comparison to exercise here. Build the stale token by refreshing the SAME slot a
    /// second time (same uuid, same epoch): `stale`, read before this refresh, is then a genuinely stale
    /// token for the slot's CURRENT value, without touching an unrelated key.
    ASSERT_EQ(claimMount(ops.op, l, "r", UInt128(1), 7, /*now*/ 1000, /*ttl*/ 30000).kind, MountClaimResult::Claimed);
```

with

```cpp
    /// `Etag` equality compares (key, value), and it is minted only through the request planes -- there
    /// is no cross-key comparison to exercise here. Build the stale token by renewing the SAME slot once:
    /// `stale`, read before this renewal, is then a genuinely stale token for the slot's CURRENT value.
    {
        const auto got = ops.op.read(l.mountKey("r"), Retry::standard());
        ASSERT_TRUE(got.has_value());
        MountLease renewed = decodeMountLease(got->bytes);
        ++renewed.seq;
        mustCommit(ops.op.replace(l.mountKey("r"), encodeMountLease(renewed), got->etag, Retry::standard()),
                   "holder renewal");
    }
```

`gtest_cas_mount_claim_conflicts.cpp`, `SlotChangedInsideAdoptionWindow`: replace the hook body

```cpp
        CasOperation racer = requests.admit();
        claimMount(racer, layout, "r", DB::UInt128(1), 7, now + 1, /*ttl_ms=*/100);
```

with

```cpp
        OperationForTest racer(*backend);
        const String key = layout.mountKey("r");
        const auto got = (*racer).read(key, Retry::standard());
        ASSERT_TRUE(got.has_value());
        MountLease rewritten = decodeMountLease(got->bytes);
        ++rewritten.seq;
        ASSERT_TRUE(std::holds_alternative<Committed>(
            (*racer).replace(key, encodeMountLease(rewritten), got->etag, Retry::standard())));
```

`gtest_cas_mount_claim_conflicts.cpp`, `ALostRefreshReportsTheObservedBodyNotTheProposer`: rename to
`ALostReclaimReportsTheObservedBodyNotTheProposer`; replace its leading comment with
`/// The same for the reclaim branch: a body that changed under our own reclaim is the one the message must name.`;
directly after the seed's `ASSERT_EQ(claimMount(seed, …).kind, MountClaimResult::Claimed);` add
`markMountGcFenced(seed, layout, "r");`; and change the claim under test to
`claimMount(op, layout, "r", DB::UInt128(1), 8, now, /*ttl_ms=*/100)`. The hook and the assertions
stay. `markMountGcFenced` runs before the hook is installed, so it does not consume it.

`gtest_cas_mount_claim_conflicts.cpp`, `ARacedRefreshThatObservedNothingNamesNoHolder`: rename to
`ARacedReclaimThatObservedNothingNamesNoHolder`; in its leading comment replace
`The slot is removed under our own refresh` with `The slot is removed under our own reclaim`;
add `markMountGcFenced(seed, layout, "r");` after the seed claim; change the claim under test to epoch
`8`. The hook and the assertions stay.

The two retargeted tests are behaviour-preservation tests of the reclaim branch: green on the base and
on the branch.

- [ ] **Step 8: Header and caller comments**

`CA/Pool/CasServerRoot.h`, four replacements.

The `MountPriorState` doc — replace

```cpp
/// Which certificate of death, or the operator's explicit unsafe authorization, justified a
/// same-uuid, different-epoch mount reclaim. `None` when no
/// reclaim of that kind happened (a fresh claim, a
/// same-epoch refresh, `LiveDoubleStart`, `ForeignOwner`, `FencedSelf`).
```

with

```cpp
/// Which certificate of death, or the operator's explicit unsafe authorization, justified a
/// same-uuid, older-epoch mount reclaim. `None` when no reclaim of that kind happened (a fresh
/// claim, `LiveDoubleStart`, `ForeignOwner`, `FencedSelf`).
```

The decision list above `struct MountClaimResult` — replace

```cpp
///   - same `server_uuid` AND same `writer_epoch` as (our_uuid, our_epoch) → it is OUR OWN claim
///     (a replay / the renewer adopting it):
///       - `gc_fenced` → terminal for THIS (uuid, epoch) — a fence costs an epoch, so refreshing it
///         in place would reactivate a fenced incarnation → `FencedSelf` (no write);
///       - otherwise → refresh (`replace` to bump seq + fresh `expires_at_ms`) → `Claimed`;
///   - same `server_uuid`, DIFFERENT `writer_epoch` → reclaimed ONLY on a certificate of death, or the
```

with

```cpp
///   - same `server_uuid` and a NEWER `writer_epoch` than `our_epoch` → throw `ABORTED`, no write: an
///     older allocation never replaces a newer incarnation, whatever certifies its end;
///   - same `server_uuid` AND same `writer_epoch` → another process was allocated our epoch; never
///     refreshed:
///       - `gc_fenced` → terminal for THIS (uuid, epoch) → `FencedSelf` (no write);
///       - otherwise → `LiveDoubleStart` (no write);
///   - same `server_uuid`, OLDER `writer_epoch` → reclaimed ONLY on a certificate of death, or the
```

The `MountClaimResult::prior` doc — replace

```cpp
    /// same-uuid, different-epoch `Claimed` reclaim (`None` for
    /// every other `Kind`, and for the absent-slot / same-epoch-refresh `Claimed` cases).
```

with

```cpp
    /// same-uuid, older-epoch `Claimed` reclaim (`None` for
    /// every other `Kind`, and for the absent-slot `Claimed` case).
```

The `claimMountAwaitingExpiry` doc — replace

```cpp
///   - first attempt decided immediately for `Claimed` (fresh / refreshed / reclaimed via `Fenced` or
///     `Clean`), `ForeignOwner`, or `FencedSelf`;
///   - a `LiveDoubleStart` from OUR OWN uuid (a stale lease from a prior incarnation of this server,
```

with

```cpp
///   - first attempt decided immediately for `Claimed` (fresh / reclaimed via `Fenced` or `Clean`),
///     `ForeignOwner`, `FencedSelf`, or a `LiveDoubleStart` on our own (uuid, epoch);
///   - a `LiveDoubleStart` from OUR OWN uuid with an older epoch (a stale lease from a prior incarnation of this server,
```

`CA/Pool/CasPool.cpp`, `Pool::mountWritable`:

- the comment starting `/// One bare attempt first: a slot held by OUR uuid under another epoch is reclaimed at`:
  replace `under another epoch` with `under an older epoch`, and replace
  `freshly minted, or a same-epoch refresh) falls through to the ordinary observed path` with
  `freshly minted) falls through to the ordinary observed path`;
- the comment above `bool unclean_reclaim = false;`: replace
  `fresh mount / same-epoch refresh with nothing to hand over, are the proven ones.` with
  `fresh mount with nothing to hand over, are the proven ones.`

- [ ] **Step 9: Gate; expect green**

```bash
ninja -C build unit_tests_dbms > build/ninja_pr07_t5d.log 2>&1; echo NINJA_EXIT=$?
build/src/unit_tests_dbms --gtest_filter='CAS*' > build/test_pr07_t5_gate.log 2>&1; echo TEST_EXIT=$?
```

Expect `TEST_EXIT=0`.

- [ ] **Step 10: Commit**

```bash
git add -- src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.cpp \
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h \
    src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.cpp \
    src/Disks/tests/gtest_cas_mount.cpp src/Disks/tests/gtest_cas_mount_claim_conflicts.cpp
git commit -m "Never write a mount slot that holds our own uuid and epoch

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

## Task 6: User documentation

**Files:** `docs/en/antalya/cas/architecture/mounts-and-leases.md`.

- [ ] **Step 1: Claim-outcome tables** (section `## Mount claim outcomes {#claim-outcomes}`)

Replace the `Claimed` and `LiveDoubleStart` rows of the `Kind` table with:

```markdown
| `Claimed` | fresh claim (absent slot), or a certified reclaim of an older `writer_epoch` |
| `LiveDoubleStart` | same `server_uuid` with an older `writer_epoch` and no certificate of death yet — a live twin, wait it out; or the same `(uuid, epoch)` already in the slot — two starts got one epoch, refused at once |
```

Replace the `None` row of the `MountPriorState` table with
`| \`None\` | no reclaim needed (fresh claim) |`.

After the `Kind` table, add one paragraph:

```markdown
A claim never installs a `writer_epoch` at or below the one in the slot. When the slot holds a newer
`writer_epoch` of the same `server_uuid`, the claim fails with `ABORTED` and writes nothing: another
start or a decommission of this `server_root_id` allocated after this one.
```

- [ ] **Step 2: Slot diagram** (section `## Behavioral mount-slot model {#mount-state-machines}`)

Replace both `same-uuid claim with a fresh writer_epoch, instant reclaim` labels with
`same-uuid claim with a higher writer_epoch, instant reclaim`.

- [ ] **Step 3: Commit**

```bash
git add -- docs/en/antalya/cas/architecture/mounts-and-leases.md
git commit -m "Document that a mount claim never lowers or repeats the epoch

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

## Task 7: Gates and PR text

- [ ] **Step 1: Release gate**

```bash
cd /home/mfilimonov/workspace/ClickHouse/cas-95-1
build/src/unit_tests_dbms --gtest_filter='CAS*' > build/test_pr07_gate.log 2>&1; echo TEST_EXIT=$?
```

Expect `TEST_EXIT=0`.

- [ ] **Step 2: ASan gate**

```bash
ninja -C build_asan unit_tests_dbms > build_asan/ninja_pr07.log 2>&1; echo NINJA_EXIT=$?
build_asan/src/unit_tests_dbms --gtest_filter='CAS*' > build_asan/test_pr07_gate.log 2>&1; echo TEST_EXIT=$?
```

Expect `NINJA_EXIT=0`, `TEST_EXIT=0`. No new test expects `LOGICAL_ERROR`, so there is no death-test
split.

- [ ] **Step 3: Confirm the commit set**

```bash
git diff --stat 5b7c9f503df..HEAD
```

Expect exactly the nine files of the Files table.

## PR text

Title: `Keep CAS mount claims monotone in writer_epoch`

Body, per `.github/PULL_REQUEST_TEMPLATE.md`:

```
A mount claim could replace a same-`server_uuid` body with a NEWER `writer_epoch`: a start delayed
between its epoch allocation and its claim reclaimed a fenced, clean or observed-dead newer body with
its older epoch, and recovery seals only epochs below the live one. A claim of the same `(uuid, epoch)`
refreshed the body in place, which only happens when two starts were allocated one epoch.
`claimMount` now throws `ABORTED` on a newer epoch, returns `LiveDoubleStart` without a write on the
same pair (`claimMountAwaitingExpiry` returns it at once), and reclaims only older epochs.
`decodeServerEpoch` rejects `next_writer_epoch = 0` with `CORRUPTED_DATA`.

Mixed versions: no on-storage or wire change. A node of the previous version still refreshes a body of
its own pair and can still reclaim a newer epoch; a node of this version refuses both.

Model: `CaCasMountCore` step 1 (two incarnations of one uuid, claim monotonicity) at <model commit SHA>,
`run_mount.sh`: <the RESULT lines of the claim-regression sabotage and the two step-1 witnesses>.

### Changelog category (leave one):
- Bug Fix (user-visible misbehavior in an official stable release)

### Changelog entry:
A content-addressed disk no longer lets a delayed start of a server replace a newer incarnation of the same server in its mount slot; such a start now fails.

### Documentation entry for user-facing changes
- [x] `docs/en/antalya/cas/architecture/mounts-and-leases.md`: claim outcomes and the slot diagram.
```
