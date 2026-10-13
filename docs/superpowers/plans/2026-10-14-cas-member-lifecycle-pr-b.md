# CAS member lifecycle PR B: decommission keeps the control objects — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (chosen) to
> implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** `SYSTEM CAS DROP POOL MEMBER` never deletes `owner`, `epoch` or `mount`; every run tombstones the
owner right after its claim (a new `owner_life`), confirms the claim, drains on its fenced mount plane, writes a
clean farewell and marks the retirement verified; every claim re-reads the owner before installing the renewer;
S3 staging keys carry the writer epoch.

**Architecture:** `OwnerObject` gains `owner_life` and `retirement_verified`. `claimOwnerOrThrow` returns the
life it read or wrote; `Pool` records it; `recheckOwnerAfterClaim` runs after every `Claimed` (start, remount,
admin) and fences the claimer's own body before refusing. `decommissionPoolMember` becomes six steps around one
`Tombstone` value (life, previous verification, ETag) with one exit rule: after the tombstone and before the
farewell, every exit confirms the claim and rolls the tombstone back when the claim was lost, unless the owner
already held a verified retirement. The report's `slot_removed` becomes `member_retired`; GC-pending rows become
`namespaces_pending_gc`. `EpochMintPolicy`, the vanished-slot branch and the heartbeat prune go.

**Tech stack:** C++23, gtest (`src/Disks/tests`), `InMemoryBackend` hooks (`onBeforeWrite`,
`onWriteCommitted`, `failNextReadWith`, `injectAmbiguousLandedWrite`), `FakeClock`, `OperationForTest`.

**Spec:** `/home/mfilimonov/workspace/ClickHouse/master/docs/superpowers/specs/2026-10-13-cas-member-lifecycle-design.md`
(revision 10) — D1–D5, D8, D9, #owner-life, #claim-recheck, #decommission, #removed (PR B rows), #staging
(key and sweep only), #tests «PR B», #delivery row B.

**Base:** `fix/antalya-26.6/cas-362-epoch-nonce` @ `476cf0523a0` (CAS-362, two commits over `2a034cdb374`). Read
`git -C /home/mfilimonov/workspace/ClickHouse/cas-janitor-budget diff 2a034cdb374 476cf0523a0` first: the epoch
counter already carries `allocation_nonce`, `encodeServerEpoch` writes it, the format README names it. Line
numbers below are at `2a034cdb374` (the CAS-362 diff touches `CasServerRoot.cpp` only around `allocateWriterEpoch`).

**Branch:** `fix/antalya-26.6/cas-363-member-lifecycle`, created from `476cf0523a0` in the executor's own
worktree `$WT` (never `cas-janitor-budget`, never the master worktree). `$CA` =
`src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed`. Spec paths `CA/...` mean `$CA/...`.

**Files outside `$CA`:** `src/Interpreters/InterpreterSystemQuery.cpp` (one result column renamed, one added),
`programs/disks/CommandCaDropMember.cpp` (same two lines), `src/Disks/tests/*` (tests),
`docs/en/antalya/cas/**` (docs), `tests/integration/test_cas_drop_pool_member/test.py`. No other upstream file.

## Global Constraints

- C++: Allman braces (opening brace on its own line). Comments explain why, never what; no references to
  plans, reviews, backlog ids, finding ids or history in code or comments. No fallback paths: an error
  propagates; a best-effort cleanup before a throw (the self-fence in Task 3) is not a fallback.
- `LOGICAL_ERROR` expectations are death tests: the child calls `std::_Exit`; no new test in this plan expects
  one (every refusal here is `CORRUPTED_DATA`, `ABORTED` or `BAD_ARGUMENTS`).
- Failing-first: every new test fails on `476cf0523a0` for the reason stated in its step, or is marked
  **guard** (passes on the base; pins behaviour the change must keep).
- Build: `cd $WT/build && flock /home/mfilimonov/workspace/ClickHouse/master/tmp/ninja.lock nice -n 10 ninja unit_tests_dbms > build_<task>.log 2>&1; echo NINJA_EXIT=$? >> build_<task>.log`
  (no `-j`; a fresh log name per run, e.g. `build_t3_2.log`; read `NINJA_EXIT=` before trusting a test run).
- Tests: `TMPDIR=$WT/build/tmp $WT/build/src/unit_tests_dbms --gtest_filter='<names>' > $WT/build/test_<task>.log 2>&1`.
  Gate: `--gtest_filter='CAS*'`, exactly that filter, release and `build_asan`. A subagent reads each log.
- Commits: exact paths (`git add -- <path> …`), never a directory, never `-A`; no rebase, amend, stash or push.
  Every commit message ends with `Signed-off-by: Mikhail Filimonov <mfilimonov@altinity.com>` then
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Docs under `docs/en`: every heading carries `{#anchor}`; code names in backticks; functions as `f`, not `f()`;
  tech-prose style (point first, plain words, no history, no hype).
- On-store format: only the two optional owner fields the spec adds (`owner_life`, `retirement_verified`),
  additive plain keys, no `v` bump; record both in `$CA/Formats/README.md` in the same task.
- Upstream surfaces outside `$CA` get the minimal, portable edit named above and nothing else.
- Not in this PR: D6 rejoin, the `claimMount` foreign-terminal reclaim, `retired` in `system.cas_mounts` (PR C);
  D7, abort-deletes-staging, refusal texts, the multipart lifecycle rule (PR A); fsck (CAS-357).

## Review Focus

Inputs the spec implies but no listed test exercises, most likely to bite first; each line names the task that
owns the test added for it.

1. **A 26.6.4 owner body (no `owner_life`) under the new recheck.** Every upgraded server starts under life 0;
   its first decommission writes life 1. The recheck must compare 0 == 0, not treat 0 as "absent". Test
   `CASServerRootClaim.ClaimOverA2664OwnerReadsLifeZero` (Task 2) and the format test
   `CASOwnerFormat.BodyWithoutLifeReadsAsZeroAndUnverified` (Task 1).
2. **Owner absent at the recheck** (deleted by hand between claim and recheck). The recheck must refuse and
   self-fence, not dereference. Test `CASMount.ClaimRecheckRefusesAnAbsentOwner` (Task 3).
3. **The self-fence write itself fails** (store unreachable after the owner read). The refusal must still be
   thrown with the owner reason, and the slot left as it was. Test
   `CASMount.RecheckRefusalSurvivesAFailedSelfFence` (Task 3).
4. **A fence loss in the middle of a drain LIST page** (not on a delete). `forEachListedKey` throws out of the
   drain; the exception exit must still confirm and roll back. Test
   `CASDecommission.FenceLossOnAListPageStillRunsTheConfirm` (Task 4c).
5. **A staging key whose directory parses as an epoch but has deeper nesting** (`staging/a/7/x/y.tmp`, an
   `srid` `a/7` with a nested member). The sweep must leave it alone. Test
   `CASStagingSweeper.DeeperPathsUnderAnEpochDirectoryAreKept` (Task 5).

## Rulings (spec ambiguities resolved here; executors follow these)

- **R1 First claim writes `owner_life = 1`.** An absent owner reads as life 0, so "read value + 1" gives 1; a
  26.6.4 body (no field) reads as 0 and the first new write makes it 1. `encodeOwner` writes `owner_life` only
  when nonzero and `retirement_verified` only when true, so a never-written-by-new-code body keeps its bytes and
  the existing golden stays valid for the zero case.
- **R2 `CASMount.ForeignReclaimReadsTheOwnerFirst` moves to PR C.** It tests `claimMount` reclaiming a foreign
  terminal body, which #delivery assigns to PR C; in PR B `claimMount` still answers `ForeignOwner` for every
  foreign body (`CASMountAwaitExpiry.ForeignUuidFailsClosedImmediately` guards it).
- **R3 Operator surface.** `slot_removed` → `member_retired` (UInt8) and one new column `namespaces_pending_gc`
  (UInt64) before it, in SQL, CLI and the report. The retired uuid and life go into the `end` event detail
  (`server_uuid`, `owner_life`) and the log line, not into new columns.
- **R4 Step 2 finds another uuid → `ABORTED`**, nothing written, the admin's farewell lands on unwind.
- **R5 Fence loss inside the drain.** A per-object fence refusal is one warning and ends the walk
  (`return op.admitted()` from the listing callback); a fence refusal on a LIST page propagates. Both reach the
  confirm (warnings path or exception path).
- **R6 Outcome after step 5.** `member_retired = (owner.owner_life > tombstone.life)`: this run's mark or any
  later life means retired; the owner still at the tombstone's life means "rerun"; an absent or unreadable owner
  means "rerun" with the read error in `warnings`.
- **R7 Staging epoch is decimal**: `staging/<srid>/<writer_epoch>/<random32>.tmp`, `writer_epoch` as
  `std::to_string`; the sweeper parses it with `std::from_chars`.
- **R8 Release note** = a `### Upgrading and downgrading {#decommission-compatibility}` subsection in
  `docs/en/antalya/cas/operations/migration.md` plus the PR's Changelog entry.
- **R9 `claimOwnerOrThrow` returns `uint64_t`** (the life the owner holds after the call) and takes
  `RetiredOwnerPolicy` (default `Refuse`); `MountClaimPolicy::NoWait` maps to `AcceptForDecommission`.
- **R10 Exit ordering.** The confirm runs on the control plane while the admin `Pool` is alive, before
  `admin.reset()` writes the farewell: a farewell is a clean body and would read as a lost claim.
- **R11 `LongDrainOutlivesOneTtl` is a guard** with the real background renewer (TTL 1000 ms, period 200 ms,
  a backend whose roots deletes take 400 ms each); the drain's plane does not change what it proves.
- **R12 `MidRetirementCrashResumesViaMountLeaseFallback` → `OwnerDeletedByHandIsRecreatedFromTheLease`**:
  only the owner is struck (the counter stays, D9); the run recovers the uuid from the lease, re-creates the
  owner (life 1) and retires it.
- **R13 Victim rows after the drain.** Rows in `Removing` count as `namespaces_pending_gc`; a victim row in
  any other state after the drain (appeared after the cut) is a warning and blocks the farewell.
- **R14 `CASHeartbeatFloor.UnseenSridPrunedFromObservationMap` → `TerminalSlotLeavesTheObservationMap`**;
  `CASDecommissionCatalogDuties.CatalogTokenMovedBetweenOwnershipCutAndRetirementKeepsSlot` and its backend are
  deleted (the two retirement reads it interleaves no longer exist).

## File structure

| File | Responsibility after this PR |
|---|---|
| `$CA/Formats/CasServerRootFormats.{h,cpp}` | owner codec with `owner_life`, `retirement_verified` |
| `$CA/Formats/README.md` | the two fields recorded |
| `$CA/Pool/CasServerRoot.{h,cpp}` | `RetiredOwnerPolicy`, `claimOwnerOrThrow` returning the life, `OwnerExpectation`, `recheckOwnerAfterClaim`, `allocateWriterEpoch` without a policy, `claimMountAwaitingExpiry` without the vanished branch, `computeHeartbeatFloor` without the prune, `sweepOwnMountStaging` with a cutoff |
| `$CA/Pool/CasPool.{h,cpp}` | `recorded_owner_life`, the recheck in `mountWritable` and `tryRemountOnce`, D9 counter check and the skipped-probe log in `openForDecommission`, `captureMountIncarnation` |
| `$CA/Tools/CasDecommission.{h,cpp}` | the six-step run, `Tombstone`, `adminClaimStillHeld`, the report fields |
| `$CA/ContentAddressedTransaction.cpp`, `$CA/ContentAddressedMetadataStorage.cpp` | epoch staging key, sweep cutoff |
| `$CA/Backend/CasProbe.cpp` | versioning message text |
| `src/Disks/tests/gtest_cas_server_root_format.cpp`, `gtest_cas_mount.cpp`, `gtest_cas_decommission.cpp`, `gtest_cas_decommission_catalog_duties.cpp`, `gtest_cas_s3_staging.cpp`, `gtest_cas_pool.cpp` | tests |
| `src/Interpreters/InterpreterSystemQuery.cpp`, `programs/disks/CommandCaDropMember.cpp` | result row |
| `docs/en/antalya/cas/architecture/mounts-and-leases.md`, `configuration.md`, `operations/debugging.md`, `architecture/backend.md`, `operations/migration.md`, `operations/troubleshooting.md` | docs |
| `tests/integration/test_cas_drop_pool_member/test.py` | cases (6), (6b), (8), (10) |

## Setup

- [ ] `git -C /home/mfilimonov/workspace/ClickHouse/cas-janitor-budget branch --contains 476cf0523a0 | grep cas-362-epoch-nonce`
      (the base exists). Create `$WT` with `/home/mfilimonov/workspace/ClickHouse/master/.claude/skills`'s
      `create-worktree` recipe (`create_subtree_branch.sh`), branch `fix/antalya-26.6/cas-363-member-lifecycle`
      from `476cf0523a0`. Run `cmake .` in `$WT/build` and `$WT/build_asan`.
- [ ] Baseline: build `unit_tests_dbms`, run the release `CAS*` gate, record the green count in `$WT/tmp/baseline.txt`.

---

## Task 1: owner format — `owner_life` and `retirement_verified`

**Files:**
- Modify: `$CA/Formats/CasServerRootFormats.h:23-34`, `$CA/Formats/CasServerRootFormats.cpp:14-18,55-98`
- Modify: `$CA/Formats/README.md:35` (the `gc/server-roots` row)
- Test: `src/Disks/tests/gtest_cas_server_root_format.cpp`, `src/Disks/tests/gtest_cas_mount.cpp`

**Interfaces:**
- Produces: `OwnerObject{server_uuid, retired_at_ms, owner_life, retirement_verified}`; `encodeOwner`/`decodeOwner`
  round-trip both; wire keys `owner_life` (u64 as decimal string, like `next_writer_epoch`) and
  `retirement_verified` (JSON bool). Absent → 0 / false.

- [ ] **Step 1: write the failing format tests** (append to `gtest_cas_server_root_format.cpp`; update the
  battery golden so the zero case keeps its bytes):

```cpp
TEST(CASOwnerFormat, LifeAndVerifiedRoundTrip)
{
    OwnerObject o;
    o.server_uuid = hexToU128("0123456789abcdeffedcba9876543210");
    o.retired_at_ms = 1752537600000ULL;
    o.owner_life = 7;
    o.retirement_verified = true;
    EXPECT_EQ(encodeOwner(o), currentFormatHeader("cas_owner")
        + "{\"server_uuid\":\"0123456789abcdeffedcba9876543210\",\"retired_at_ms\":1752537600000,"
          "\"owner_life\":\"7\",\"retirement_verified\":true}\n");
    const OwnerObject back = decodeOwner(encodeOwner(o));
    EXPECT_EQ(back.owner_life, 7u);
    EXPECT_TRUE(back.retirement_verified);
    EXPECT_EQ(back.retired_at_ms, o.retired_at_ms);
}

/// The owner body written by 26.6.4 and by a never-retired owner: no life, no mark.
TEST(CASOwnerFormat, BodyWithoutLifeReadsAsZeroAndUnverified)
{
    const OwnerObject o = decodeOwner(currentFormatHeader("cas_owner")
        + "{\"server_uuid\":\"0123456789abcdeffedcba9876543210\"}\n");
    EXPECT_EQ(o.owner_life, 0u);
    EXPECT_FALSE(o.retirement_verified);
    OwnerObject zero;
    zero.server_uuid = o.server_uuid;
    EXPECT_EQ(encodeOwner(zero), currentFormatHeader("cas_owner")
        + "{\"server_uuid\":\"0123456789abcdeffedcba9876543210\"}\n")
        << "a zero life and a false mark are not written, so the 26.6.4 bytes stay reachable";
}

/// The 26.6.4 decoder's loop knows `server_uuid` and `retired_at_ms` and hands every other key to `skipUnknown`.
TEST(CASOwnerFormat, NewFieldsDecodeWithThe2664KeySet)
{
    const String body = encodeOwner(OwnerObject{
        .server_uuid = hexToU128("0123456789abcdeffedcba9876543210"),
        .retired_at_ms = 5, .owner_life = 3, .retirement_verified = true});
    DB::ReadBufferFromMemory in(body.data(), body.size());
    expectHeaderLine(in, FormatId::Owner);
    const String line = readLine(in, traitsFor(FormatId::Owner).line_cap, "owner");
    DB::ReadBufferFromMemory line_in(line.data(), line.size());
    JsonObjectReader r(line_in, "owner");
    String key;
    std::optional<UInt128> uuid;
    std::optional<uint64_t> retired;
    while (r.nextKey(key))
    {
        if (key == "server_uuid")
            uuid = r.readHex128();
        else if (key == "retired_at_ms")
            retired = r.readU64Number();
        else
            r.skipUnknown(key);
    }
    EXPECT_EQ(uuid, hexToU128("0123456789abcdeffedcba9876543210"));
    EXPECT_EQ(retired, 5u);
    EXPECT_TRUE(line_in.eof());
}
```
  Mirror the includes the CAS-362 test `CASServerEpochFormat.NonceBodyDecodesWithThe2664KeySet` added to this
  file (`CasTextFormat.h`, `ReadBufferFromMemory.h`, `<optional>`).

- [ ] **Step 2: write the failing ABA test** (append to `gtest_cas_mount.cpp`, inside the file's anonymous
  namespace add the backend; the test after `CASServerRootClaim.OwnerLostToARacerIsDecidedFromTheConflictObservation`):

```cpp
/// Owner ETags derived from the bytes, as S3 derives them: equal bytes bring back an equal ETag.
class ContentEtagOwnerBackend : public InMemoryBackend
{
public:
    std::optional<Raw> read(const String & key, TransportAccess & access) override
    {
        auto raw = InMemoryBackend::read(key, access);
        if (raw && isOwner(key))
            raw->value = contentValue(raw->bytes);
        return raw;
    }

    std::optional<RawMeta> head(const String & key, TransportAccess & access) override
    {
        auto meta = InMemoryBackend::head(key, access);
        if (meta && isOwner(key))
            if (const auto raw = InMemoryBackend::read(key, access))
                meta->value = contentValue(raw->bytes);
        return meta;
    }

    std::expected<String, RawConflict> write(const String & key, const String & bytes,
                                             const std::optional<String> & expected_value, TransportAccess & access) override
    {
        if (!isOwner(key))
            return InMemoryBackend::write(key, bytes, expected_value, access);
        std::optional<String> minted_expected;
        if (expected_value)
        {
            const auto current = InMemoryBackend::read(key, access);
            if (!current || contentValue(current->bytes) != *expected_value)
                return std::unexpected(RawConflict{});
            minted_expected = current->value;
        }
        const auto written = InMemoryBackend::write(key, bytes, minted_expected, access);
        if (!written)
            return written;
        return contentValue(bytes);
    }

private:
    static bool isOwner(const String & key) { return key.ends_with("/owner"); }
    static String contentValue(const String & bytes) { return "content-" + DB::Cas::tests::hexOf(bytes); }
};
```

```cpp
/// A tombstone sent against the owner's first ETag, delivered after a tombstone and a rejoin. Without a
/// life the rejoined owner's bytes equal the first owner's, so a content-derived ETag would admit it.
TEST(CASOwner, DelayedTombstoneAfterRejoinFailsOnLife)
{
    auto b = std::make_shared<ContentEtagOwnerBackend>();
    Layout l("p");
    Ops ops(b);
    const String key = l.ownerKey("r");
    const OwnerObject first{.server_uuid = UInt128(1), .owner_life = 1};
    mustCommit(ops.op.create(key, encodeOwner(first), Retry::standard()), "first owner");
    const Etag stale = currentEtag(ops.op, key);

    OwnerObject tombstone = first;
    tombstone.retired_at_ms = 1000;
    tombstone.owner_life = 2;
    mustCommit(ops.op.replace(key, encodeOwner(tombstone), currentEtag(ops.op, key), Retry::standard()), "tombstone");
    OwnerObject rejoined = first;
    rejoined.owner_life = 3;
    mustCommit(ops.op.replace(key, encodeOwner(rejoined), currentEtag(ops.op, key), Retry::standard()), "rejoin");
    ASSERT_NE(currentEtag(ops.op, key), stale) << "the life makes the rejoined bytes differ from the first";

    OwnerObject late = first;
    late.retired_at_ms = 2000;
    late.owner_life = 2;
    const WriteResult landed = ops.op.replace(key, encodeOwner(late), stale, Retry::standard());
    EXPECT_TRUE(std::holds_alternative<Conflict>(landed));
    EXPECT_EQ(decodeOwner(ops.op.read(key, Retry::standard())->bytes).owner_life, 3u);
}
```

- [ ] **Step 3: run to verify they fail.** `--gtest_filter='CASOwnerFormat.*:CASOwner.*'`. Expected: compile
  failure (`owner_life` is not a member of `OwnerObject`). Semantic reason, once the fields exist but are not
  encoded: `rejoined` bytes equal `first`, the content ETag equals `stale`, the late replace commits.

- [ ] **Step 4: implement.** `CasServerRootFormats.h`:

```cpp
struct OwnerObject
{
    UInt128 server_uuid{};
    std::optional<uint64_t> retired_at_ms;
    /// Every owner write sets this to the read value + 1, so two writes never carry the same bytes and an
    /// `If-Match` against an older incarnation cannot land late. 0 = written before the field existed.
    uint64_t owner_life = 0;
    /// Set by the decommission run whose claim, tombstone and farewell all held; a later tombstone refresh
    /// keeps it, a rollback drops it. A rerun treats only a verified retirement as already retired.
    bool retirement_verified = false;
};
```
  `CasServerRootFormats.cpp`: add `constexpr WireKey owner_life{"owner_life"};` and
  `constexpr WireKey retirement_verified{"retirement_verified"};` to `OwnerWire`; in `encodeOwner` after the
  `retired_at_ms` field: `if (o.owner_life != 0) writeU64StringField(out, OwnerWire::owner_life, o.owner_life, first);`
  `if (o.retirement_verified) writeBoolField(out, OwnerWire::retirement_verified, true, first);`
  In `decodeOwner`'s loop: `else if (key == OwnerWire::owner_life) o.owner_life = r.readU64String();`
  `else if (key == OwnerWire::retirement_verified) o.retirement_verified = r.readBool();`
  Update the header comment of `decodeOwner` ("tolerating absent `retired_at_ms`, `owner_life` and
  `retirement_verified`"). `README.md` row: after `optional retired_at_ms` add
  ``optional `owner_life` (u64 decimal string; absent in 26.6.4 bodies, read as 0; additive, no `v` bump), optional `retirement_verified` (bool; absent = false)``.
  The `CASFormatBattery.Owner` golden stays (`owner_life` 0 is not written).

- [ ] **Step 5: run to verify they pass**, then `--gtest_filter='CASFormatBattery.*:CASOwnerFormat.*:CASOwner.*'`.

- [ ] **Step 6: commit.**
```bash
git add -- $CA/Formats/CasServerRootFormats.h $CA/Formats/CasServerRootFormats.cpp $CA/Formats/README.md \
  src/Disks/tests/gtest_cas_server_root_format.cpp src/Disks/tests/gtest_cas_mount.cpp
git commit -m "Add owner_life and retirement_verified to the CAS owner object

Every owner write will set owner_life to the read value + 1, so no two owner
writes carry the same bytes and a delayed If-Match cannot land after a later
write. retirement_verified records a decommission whose claim, tombstone and
farewell all held. Both are plain optional keys that 26.6.4 readers skip.

Signed-off-by: Mikhail Filimonov <mfilimonov@altinity.com>
Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 2: the owner life in the claim

**Files:**
- Modify: `$CA/Pool/CasServerRoot.h:139-155`, `$CA/Pool/CasServerRoot.cpp:441-452,525-596`
- Modify: `$CA/Pool/CasPool.h` (new member + accessor near `liveWriterEpoch`, `:806`), `$CA/Pool/CasPool.cpp:598-604,1431-1436`
- Test: `src/Disks/tests/gtest_cas_mount.cpp`, `src/Disks/tests/gtest_cas_decommission.cpp`, `src/Disks/tests/gtest_cas_pool.cpp`

**Interfaces:**
- Produces:
```cpp
enum class RetiredOwnerPolicy : uint8_t
{
    Refuse,                  /// a server start or remount never resumes a tombstoned owner
    AcceptForDecommission,   /// the decommission admin impersonates a retired victim to drain or rerun
};

/// Returns the owner life after the call: 1 for a fresh claim, the stored value otherwise (0 for a body
/// written before the field existed).
uint64_t claimOwnerOrThrow(
    CasOperation & op, const Layout & l, const String & srid, UInt128 our_uuid,
    const ObserveRefCatalog & observe_catalog, RetiredOwnerPolicy retired_policy = RetiredOwnerPolicy::Refuse);

/// Pool: the owner life this process started under; every later claim must find it unchanged.
uint64_t Pool::ownerLife() const;
```
- Consumes: Task 1's fields.

- [ ] **Step 1: write the failing tests** (`gtest_cas_mount.cpp`, after `CASServerRootClaim.TombstonedSameOwnerFailsClosed`):

```cpp
TEST(CASServerRootClaim, FirstClaimWritesLifeOne)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    Ops ops(b);
    EXPECT_EQ(claimOwnerOrThrow(ops.op, l, "r", UInt128(1), emptyCatalogObservation()), 1u);
    const OwnerObject stored = decodeOwner(ops.op.read(l.ownerKey("r"), Retry::standard())->bytes);
    EXPECT_EQ(stored.owner_life, 1u);
    EXPECT_FALSE(stored.retirement_verified);
    /// A restart under the same owner writes nothing and reads the same life.
    const Etag before = currentEtag(ops.op, l.ownerKey("r"));
    EXPECT_EQ(claimOwnerOrThrow(ops.op, l, "r", UInt128(1), emptyCatalogObservation()), 1u);
    EXPECT_EQ(currentEtag(ops.op, l.ownerKey("r")), before);
}

TEST(CASServerRootClaim, ClaimOverA2664OwnerReadsLifeZero)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    Ops ops(b);
    mustCommit(ops.op.create(l.ownerKey("r"), encodeOwner(OwnerObject{.server_uuid = UInt128(1)}), Retry::standard()),
               "owner without a life");
    EXPECT_EQ(claimOwnerOrThrow(ops.op, l, "r", UInt128(1), emptyCatalogObservation()), 0u);
}

TEST(CASServerRootClaim, DecommissionPolicyAcceptsARetiredOwnerOfTheSameUuid)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    Ops ops(b);
    mustCommit(ops.op.create(l.ownerKey("r"), encodeOwner(OwnerObject{
        .server_uuid = UInt128(1), .retired_at_ms = 1752537600000ULL, .owner_life = 2}), Retry::standard()), "tombstone");
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&]
    {
        claimOwnerOrThrow(ops.op, l, "r", UInt128(1), emptyCatalogObservation(), RetiredOwnerPolicy::Refuse);
    });
    EXPECT_EQ(claimOwnerOrThrow(ops.op, l, "r", UInt128(1), emptyCatalogObservation(),
                                RetiredOwnerPolicy::AcceptForDecommission), 2u);
    /// A retired owner of ANOTHER uuid is still foreign under either policy.
    expectThrowsCodeWithMessage(DB::ErrorCodes::CORRUPTED_DATA, "owned by a different server", [&]
    {
        claimOwnerOrThrow(ops.op, l, "r", UInt128(2), emptyCatalogObservation(), RetiredOwnerPolicy::AcceptForDecommission);
    });
}

/// GUARD: a server start over a tombstoned owner is refused in this PR whether or not the retirement is
/// verified, and writes neither the counter nor the slot. (A later PR lets a verified, drained retirement
/// rejoin; the unverified arm stays.)
TEST(CASMount, PoolOpenNeverAcceptsARetiredOwner)
{
    for (const bool verified : {false, true})
    {
        SCOPED_TRACE(verified ? "verified" : "unverified");
        auto b = std::make_shared<InMemoryBackend>();
        Layout l("p");
        { auto first = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "r"}); }
        Ops ops(b);
        const auto owner = ops.op.read(l.ownerKey("r"), Retry::standard());
        OwnerObject tombstone = decodeOwner(owner->bytes);
        tombstone.retired_at_ms = 1;
        tombstone.owner_life += 1;
        tombstone.retirement_verified = verified;
        mustCommit(ops.op.replace(l.ownerKey("r"), encodeOwner(tombstone), owner->etag, Retry::standard()), "tombstone");
        const Etag epoch_before = currentEtag(ops.op, l.epochKey("r"));
        const Etag mount_before = currentEtag(ops.op, l.mountKey("r"));
        expectThrowsCodeWithMessage(DB::ErrorCodes::CORRUPTED_DATA, "decommissioned", [&]
        {
            Pool::open(b, PoolConfig{.pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "r"});
        });
        EXPECT_EQ(currentEtag(ops.op, l.epochKey("r")), epoch_before);
        EXPECT_EQ(currentEtag(ops.op, l.mountKey("r")), mount_before);
    }
}
```
  `gtest_cas_pool.cpp` (after `CASPoolRemount.FenceOutThenSelfRemountRestoresWrites`):
```cpp
TEST(CASPool, RecordsTheOwnerLifeItStartedUnder)
{
    auto backend = std::make_shared<InMemoryBackend>();
    { auto store = DB::Cas::tests::openPoolForTest(backend); EXPECT_EQ(store->ownerLife(), 1u); }
    auto again = DB::Cas::tests::openPoolForTest(backend);
    EXPECT_EQ(again->ownerLife(), 1u) << "a plain restart writes no owner and keeps the life";
}
```
  `gtest_cas_decommission.cpp` (after `AlwaysRenewsAdminClaimEvenWhenHostDiskIsObserveOnly`); add the helper to
  the file's anonymous namespace, it is reused by Tasks 3 and 4:
```cpp
/// What decommission step 2 writes, by hand: the next life, retired, the mark kept.
void tombstoneOwner(Backend & backend, const Layout & layout, const String & srid, bool verified = false)
{
    OperationForTest op(backend);
    const auto current = (*op).read(layout.ownerKey(srid), Retry::once());
    ASSERT_TRUE(current.has_value());
    OwnerObject owner = decodeOwner(current->bytes);
    owner.retired_at_ms = 1;
    owner.owner_life += 1;
    owner.retirement_verified = owner.retirement_verified || verified;
    ASSERT_TRUE(std::holds_alternative<Committed>(
        (*op).replace(layout.ownerKey(srid), encodeOwner(owner), current->etag, Retry::once())));
}

OwnerObject readOwner(Backend & backend, const Layout & layout, const String & srid)
{
    OperationForTest op(backend);
    const auto current = (*op).read(layout.ownerKey(srid), Retry::once());
    if (!current)
        throw std::runtime_error("owner absent: " + layout.ownerKey(srid));
    return decodeOwner(current->bytes);
}

MountLease readSlot(Backend & backend, const Layout & layout, const String & srid)
{
    OperationForTest op(backend);
    const auto current = (*op).read(layout.mountKey(srid), Retry::once());
    if (!current)
        throw std::runtime_error("mount absent: " + layout.mountKey(srid));
    return decodeMountLease(current->bytes);
}
```
```cpp
TEST(CASDecommission, OpensOverARetiredOwner)
{
    auto backend = std::make_shared<InMemoryBackend>();
    { auto victim = openVictim(backend); }
    const Layout layout("p");
    tombstoneOwner(*backend, layout, "victim");
    auto admin = Pool::openForDecommission(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");
    ASSERT_TRUE(admin != nullptr);
    EXPECT_EQ(admin->ownerLife(), 2u);
}
```

- [ ] **Step 2: run to verify they fail.** Expected: compile failure (`claimOwnerOrThrow` returns `void`,
  `RetiredOwnerPolicy` undeclared, `ownerLife` missing). `PoolOpenNeverAcceptsARetiredOwner` is a guard.

- [ ] **Step 3: implement `CasServerRoot.h`:** add the enum above the `claimOwnerOrThrow` declaration; change
  the declaration to the signature in Interfaces; rewrite its header comment:
```cpp
/// Claim or validate the owner anchor that binds `srid` to a server uuid, and return the owner life.
///   - present, equal `our_uuid`, not tombstoned → the stored life;
///   - present, equal `our_uuid`, tombstoned → `CORRUPTED_DATA` under `Refuse`; the stored life under
///     `AcceptForDecommission` (the admin runs or reruns a decommission over its retired victim);
///   - present, different uuid → `CORRUPTED_DATA` (foreign owner), tombstoned or not;
///   - absent, subtree provably empty → create `{our_uuid, owner_life = 1}` and return 1;
///   - absent, subtree non-empty → `CORRUPTED_DATA` (identity lost over data).
/// The owner object is never deleted. A rejoin after a verified retirement is the one supported reassignment
/// (not in this file yet); every other uuid change is a refusal.
```
  `CasServerRoot.cpp`: in `claimOwnerOrThrow`, the three return points become
  `if (retired_policy == RetiredOwnerPolicy::Refuse) throwIfOwnerRetired(*owner, srid); return owner->owner_life;`
  (present branch), `return 1;` after the committed create with
  `encodeOwner(OwnerObject{.server_uuid = our_uuid, .retired_at_ms = std::nullopt, .owner_life = 1})`, and the
  same policy check + `return reread->owner_life;` in the conflict-reread branch.

- [ ] **Step 4: implement `CasPool`:** in `CasPool.h` next to `liveWriterEpoch`:
```cpp
    /// The owner life this process started under. A tombstone, rollback or rejoin bumps it; the claim
    /// recheck and every remount require it unchanged, so a process never resumes under a later life.
    uint64_t ownerLife() const { return recorded_owner_life; }
```
  and a private member `uint64_t recorded_owner_life = 0;` after `remount_mutex`. In `mountWritable`
  (`CasPool.cpp:603-604`):
```cpp
    const RetiredOwnerPolicy retired_policy = policy == MountClaimPolicy::NoWait
        ? RetiredOwnerPolicy::AcceptForDecommission
        : RetiredOwnerPolicy::Refuse;
    CasOperation owner_op = store->gc_requests.admit();
    const uint64_t owner_life = claimOwnerOrThrow(owner_op, store->pool_layout, srid, our_uuid, observe_catalog, retired_policy);
    store->recorded_owner_life = owner_life;
```
  In `tryRemountOnce` (`:1436`): `(void)claimOwnerOrThrow(owner_op, pool_layout, srid, our_uuid, observe_catalog);`
  (the remount keeps `Refuse`; the life is compared by Task 3's recheck). Update the comment at `:925-929`
  of `openForDecommission` ("Owner anchor absent + mount absent = nothing to decommission" stays; add
  "a tombstoned owner of the victim is accepted: a rerun resumes over its own tombstone").

- [ ] **Step 5: run** `--gtest_filter='CASServerRootClaim.*:CASMount.PoolOpenNeverAcceptsARetiredOwner:CASPool.RecordsTheOwnerLifeItStartedUnder:CASDecommission.OpensOverARetiredOwner:CASDecommission.*'`.
  Expected: all pass except decommission tests that the base tail still breaks over a tombstone; note any such
  red in `$WT/tmp/task2-reds.txt` — Task 4 rewrites them. If a red is not in Task 4's rewrite list, stop and report.

- [ ] **Step 6: commit.**
```bash
git add -- $CA/Pool/CasServerRoot.h $CA/Pool/CasServerRoot.cpp $CA/Pool/CasPool.h $CA/Pool/CasPool.cpp \
  src/Disks/tests/gtest_cas_mount.cpp src/Disks/tests/gtest_cas_pool.cpp src/Disks/tests/gtest_cas_decommission.cpp
git commit -m "Record the CAS owner life at the claim and let decommission claim a retired owner

claimOwnerOrThrow returns the owner life it read or wrote (a fresh claim
writes 1) and Pool records it. The decommission admin may claim a tombstoned
owner of its victim's uuid, so a run can resume over its own tombstone; a
server start or remount still refuses one.

Signed-off-by: Mikhail Filimonov <mfilimonov@altinity.com>
Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 3: the claim recheck (D3)

**Files:**
- Modify: `$CA/Pool/CasServerRoot.h` (after `claimOwnerOrThrow`), `$CA/Pool/CasServerRoot.cpp` (after `claimOwnerOrThrow`)
- Modify: `$CA/Pool/CasPool.cpp:775-781` (`mountWritable`), `:1465-1493` (`tryRemountOnce`)
- Test: `src/Disks/tests/gtest_cas_mount.cpp`, `src/Disks/tests/gtest_cas_decommission.cpp`

**Interfaces:**
- Produces:
```cpp
/// What a claimer requires of the owner once its body is in the slot and before it installs the renewer.
struct OwnerExpectation
{
    UInt128 server_uuid;
    uint64_t owner_life;
    RetiredOwnerPolicy retired;
};

/// One `GET owner` after `claimMount` returned `Claimed`. Throws `CORRUPTED_DATA` unless the owner is exactly
/// `(expected.server_uuid, expected.owner_life)` and, under `Refuse`, not retired. Before throwing it rewrites
/// the claimer's own `(uuid, our_epoch)` body `gc_fenced`, so a successor need not wait the lease out; a
/// failed fence write is logged and the refusal is still thrown.
void recheckOwnerAfterClaim(
    CasOperation & op, const Layout & l, const String & srid, const OwnerExpectation & expected,
    uint64_t our_epoch, const CasEventSink & sink = {});
```
- Consumes: Task 2's `RetiredOwnerPolicy`, `Pool::ownerLife`, `tombstoneOwner`/`readSlot` helpers.

- [ ] **Step 1: write the failing tests** (`gtest_cas_mount.cpp`; the helpers `fenceOutMount` and `readObj`
  exist in `gtest_cas_pool.cpp` only — add local copies here named `fenceSlot`/`readSlotBody`, or move
  `fenceOutMount` into `cas_test_helpers.h` as `fenceOutMountForTest` and use it from both files):

```cpp
namespace
{
/// Decommission step 2 by hand: the next life, retired.
void tombstoneOwnerAt(Backend & backend, const Layout & l, const String & srid)
{
    DB::Cas::tests::OperationForTest op(backend);
    const auto current = (*op).read(l.ownerKey(srid), Retry::once());
    ASSERT_TRUE(current.has_value());
    OwnerObject owner = decodeOwner(current->bytes);
    owner.retired_at_ms = 1;
    owner.owner_life += 1;
    ASSERT_TRUE(std::holds_alternative<Committed>(
        (*op).replace(l.ownerKey(srid), encodeOwner(owner), current->etag, Retry::once())));
}

/// A rejoin by hand: the next life under `uuid`, not retired.
void rewriteOwnerAt(Backend & backend, const Layout & l, const String & srid, UInt128 uuid)
{
    DB::Cas::tests::OperationForTest op(backend);
    const auto current = (*op).read(l.ownerKey(srid), Retry::once());
    ASSERT_TRUE(current.has_value());
    const OwnerObject next{.server_uuid = uuid, .owner_life = decodeOwner(current->bytes).owner_life + 1};
    ASSERT_TRUE(std::holds_alternative<Committed>(
        (*op).replace(l.ownerKey(srid), encodeOwner(next), current->etag, Retry::once())));
}

MountLease slotBody(Backend & backend, const Layout & l, const String & srid)
{
    DB::Cas::tests::OperationForTest op(backend);
    const auto current = (*op).read(l.mountKey(srid), Retry::once());
    if (!current)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "test fixture: mount of '{}' is absent", srid);
    return decodeMountLease(current->bytes);
}

PoolConfig rootConfig()
{
    return PoolConfig{.pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "r"};
}
}

/// The tombstone lands between the owner claim and the mount write; the only write of the mount key in
/// this open is the reclaim of the first open's farewell.
TEST(CASMount, NormalMountRefusesARetiredOwnerAfterClaim)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    { auto first = Pool::open(b, rootConfig()); }
    bool fired = false;
    b->onBeforeWrite(l.mountKey("r"), [&]
    {
        if (std::exchange(fired, true))
            return;
        tombstoneOwnerAt(*b, l, "r");
    });
    expectThrowsCodeWithMessage(DB::ErrorCodes::CORRUPTED_DATA, "decommissioned", [&] { Pool::open(b, rootConfig()); });
    ASSERT_TRUE(fired);
    const MountLease slot = slotBody(*b, l, "r");
    EXPECT_EQ(slot.server_uuid, UInt128(1));
    EXPECT_EQ(slot.writer_epoch, 2u) << "the refused claim's own body, not the first open's farewell";
    EXPECT_TRUE(slot.gc_fenced) << "the refused claimer fences itself so nobody waits its lease out";
}

/// A victim prechecked before a decommission wakes after a new-uuid rejoin and reclaims its own farewell.
TEST(CASMount, ClaimRecheckRefusesAForeignOwner)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    { auto first = Pool::open(b, rootConfig()); }
    bool fired = false;
    b->onBeforeWrite(l.mountKey("r"), [&]
    {
        if (std::exchange(fired, true))
            return;
        rewriteOwnerAt(*b, l, "r", UInt128(2));
    });
    expectThrowsCodeWithMessage(DB::ErrorCodes::CORRUPTED_DATA, "different server", [&] { Pool::open(b, rootConfig()); });
    ASSERT_TRUE(fired);
    const MountLease slot = slotBody(*b, l, "r");
    EXPECT_EQ(slot.server_uuid, UInt128(1));
    EXPECT_EQ(slot.writer_epoch, 2u);
    EXPECT_TRUE(slot.gc_fenced);
}

TEST(CASMount, ClaimRecheckRefusesAnAbsentOwner)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    { auto first = Pool::open(b, rootConfig()); }
    bool fired = false;
    b->onBeforeWrite(l.mountKey("r"), [&]
    {
        if (std::exchange(fired, true))
            return;
        DB::Cas::tests::OperationForTest op(*b);
        ASSERT_EQ((*op).removeCurrent(l.ownerKey("r"), Retry::once()), Removal::Removed);
    });
    expectThrowsCodeWithMessage(DB::ErrorCodes::CORRUPTED_DATA, "vanished", [&] { Pool::open(b, rootConfig()); });
    ASSERT_TRUE(fired);
    EXPECT_TRUE(slotBody(*b, l, "r").gc_fenced);
}

/// The owner read refuses, and the self-fence write cannot reach the store: the refusal is still the
/// owner's reason and the slot keeps the claimer's live body for GC to fence.
TEST(CASMount, RecheckRefusalSurvivesAFailedSelfFence)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    { auto first = Pool::open(b, rootConfig()); }
    bool fired = false;
    b->onBeforeWrite(l.mountKey("r"), [&]
    {
        if (std::exchange(fired, true))
            return;
        tombstoneOwnerAt(*b, l, "r");
        b->failNextWriteWith(l.mountKey("r"), std::make_exception_ptr(
            DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "store refused the fence write")));
    });
    /// `failNextWriteWith` consumes the claim write itself if armed before it: arm it from inside the hook,
    /// after the claim's own `write` has passed the hook, by arming on the NEXT write (the self-fence).
    expectThrowsCodeWithMessage(DB::ErrorCodes::CORRUPTED_DATA, "decommissioned", [&] { Pool::open(b, rootConfig()); });
    ASSERT_TRUE(fired);
    EXPECT_FALSE(slotBody(*b, l, "r").gc_fenced) << "the fence write was refused; the body is untouched";
}
```
  Check the `failNextWriteWith` ordering claim before relying on it: `onBeforeWrite` runs before the armed
  failure is consumed (`applyWrite` order in `CasInMemoryBackend.cpp`). If the armed failure is taken before
  the hook, arm it instead from a subclass whose `write` override counts mount-key writes and throws on the
  second one; keep the assertion.

```cpp
/// The GC fenced our lease; the remount reclaims it, and the owner was tombstoned in between.
TEST(CASMount, FenceRecoveryReclaimRechecksTheOwner)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    auto store = Pool::open(b, rootConfig());
    fenceSlot(*b, l.mountKey("r"));
    EXPECT_THROW(store->renewWatermarkOnce(), DB::Exception);
    bool fired = false;
    b->onBeforeWrite(l.mountKey("r"), [&]
    {
        if (std::exchange(fired, true))
            return;
        tombstoneOwnerAt(*b, l, "r");
    });
    EXPECT_FALSE(store->tryRemountOnce());
    ASSERT_TRUE(fired);
    const MountLease slot = slotBody(*b, l, "r");
    EXPECT_EQ(slot.writer_epoch, 2u) << "the remount's fresh epoch reached the slot";
    EXPECT_TRUE(slot.gc_fenced) << "and was fenced by the refused recheck";
    EXPECT_FALSE(store->mayMutate());
}

/// Another process of the same uuid rejoined (the life moved) while this one was fenced: it must not
/// resume in place under the later life.
TEST(CASMount, RemountRefusesAfterASameUuidRejoin)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    std::vector<CasEvent> events;
    PoolConfig config = rootConfig();
    config.event_sink = [&](const CasEvent & e) { events.push_back(e); };
    auto store = Pool::open(b, config);
    ASSERT_EQ(store->ownerLife(), 1u);
    rewriteOwnerAt(*b, l, "r", UInt128(1));   /// life 2, same uuid, not retired
    fenceSlot(*b, l.mountKey("r"));
    EXPECT_THROW(store->renewWatermarkOnce(), DB::Exception);
    EXPECT_FALSE(store->tryRemountOnce());
    const MountLease slot = slotBody(*b, l, "r");
    EXPECT_EQ(slot.writer_epoch, 2u);
    EXPECT_TRUE(slot.gc_fenced);
    bool named = false;
    for (const CasEvent & e : events)
        if (e.type == CasEventType::MountRemount && e.detail.contains("error") && e.detail.at("error").find("life") != String::npos)
            named = true;
    EXPECT_TRUE(named) << "the remount's failure names the life change";
}
```
  `gtest_cas_decommission.cpp`:
```cpp
/// The victim restarts during its own decommission: its owner check passes before the tombstone, its mount
/// claim waits on the admin's live lease, the admin tombstones and farewells during that wait, the victim
/// reclaims the farewell and its recheck sees the tombstone.
TEST(CASDecommission, VictimClaimingAfterTheTombstoneFencesItselfAndFails)
{
    auto backend = std::make_shared<InMemoryBackend>();
    UInt128 victim_uuid{};
    { auto victim = openVictim(backend); victim_uuid = victim->poolConfig().server_id; }
    const Layout layout("p");
    auto admin = Pool::openForDecommission(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");
    const uint64_t admin_epoch = admin->writerEpoch();

    auto fake_boot = std::make_shared<std::atomic<uint64_t>>(0);
    bool decommissioned = false;
    PoolConfig victim_config{
        .pool_prefix = "p", .server_root_id = "victim",
        .boot_ms_fn = [fake_boot] { return fake_boot->load(); },
        .wait_sleep_fn = [&, fake_boot](uint64_t ms)
        {
            *fake_boot += ms;
            if (std::exchange(decommissioned, true))
                return;
            tombstoneOwner(*backend, layout, "victim");
            admin.reset();   /// the clean farewell the victim's next poll reclaims
        }};
    expectThrowsCodeWithMessage(ErrorCodes::CORRUPTED_DATA, "decommissioned", [&] { Pool::open(backend, victim_config); });
    ASSERT_TRUE(decommissioned);
    const MountLease slot = readSlot(*backend, layout, "victim");
    EXPECT_EQ(slot.server_uuid, victim_uuid);
    EXPECT_GT(slot.writer_epoch, admin_epoch);
    EXPECT_TRUE(slot.gc_fenced);
    EXPECT_TRUE(readOwner(*backend, layout, "victim").retired_at_ms.has_value());
}
```

- [ ] **Step 2: run to verify they fail.** `--gtest_filter='CASMount.NormalMount*:CASMount.ClaimRecheck*:CASMount.RecheckRefusal*:CASMount.FenceRecovery*:CASMount.RemountRefuses*:CASDecommission.VictimClaiming*'`.
  Expected on the base: `NormalMountRefusesARetiredOwnerAfterClaim`, `ClaimRecheckRefusesAForeignOwner`,
  `ClaimRecheckRefusesAnAbsentOwner`, `RecheckRefusalSurvivesAFailedSelfFence` and
  `VictimClaimingAfterTheTombstoneFencesItselfAndFails`: `Pool::open` succeeds (no recheck), the
  `expectThrowsCode*` `FAIL()` fires. `FenceRecoveryReclaimRechecksTheOwner` and
  `RemountRefusesAfterASameUuidRejoin`: `tryRemountOnce` returns true.

- [ ] **Step 3: implement `CasServerRoot.{h,cpp}`.** Header: the two declarations from Interfaces after
  `claimOwnerOrThrow`. Source, after `claimOwnerOrThrow` (uses `emitMountEvent`, defined later in the file's
  second anonymous namespace: move `recheckOwnerAfterClaim` below `claimMount` or forward-declare
  `emitMountEvent` next to the existing `describeMountHolder` forward declaration):
```cpp
namespace
{
/// Rewrite the claimer's own live body `gc_fenced`, preserving everything else as the GC fence does. Only
/// `(our_uuid, our_epoch)`, not yet fenced or farewelled, is touched; anything else in the slot is left alone.
void fenceOwnClaim(CasOperation & op, const Layout & l, const String & srid, UInt128 our_uuid, uint64_t our_epoch,
                   const CasEventSink & sink)
{
    const String key = l.mountKey(srid);
    WriteResult fenced = op.readModifyWrite(key,
        [&](const std::optional<Object> & observed) -> std::optional<String>
        {
            if (!observed)
                return std::nullopt;
            MountLease body = decodeMountLease(observed->bytes);
            const bool ours = body.server_uuid == our_uuid && body.writer_epoch == our_epoch;
            if (!ours || body.gc_fenced || body.min_active_build_sequence == std::numeric_limits<uint64_t>::max())
                return std::nullopt;
            body.gc_fenced = true;
            body.seq += 1;
            return encodeMountLease(body);
        },
        Retry::standard());
    if (std::holds_alternative<Committed>(fenced))
        emitMountEvent(sink, CasEventType::MountRelease, srid, "self_fenced", nullptr,
            "the owner recheck refused this claim; the slot is fenced so a successor need not wait");
    else if (!std::holds_alternative<Declined>(fenced))
        LOG_WARNING(getLogger("CasMountLease"),
            "CAS server-root '{}': the refused claim's mount body could not be fenced; its lease expires on its own", srid);
}
}

void recheckOwnerAfterClaim(
    CasOperation & op, const Layout & l, const String & srid, const OwnerExpectation & expected,
    uint64_t our_epoch, const CasEventSink & sink)
{
    const std::optional<OwnerObject> owner = readOwnerObject(op, l, srid);
    String refusal;
    if (!owner)
        refusal = fmt::format("CAS server-root '{}': the owner anchor vanished after the mount claim", srid);
    else if (owner->server_uuid != expected.server_uuid)
        refusal = fmt::format(
            "CAS server-root '{}' is owned by a different server since this claim began (owner server_uuid={}, "
            "ours={}); the mount slot was handed to another server and this claim is withdrawn",
            srid, u128ToHex(owner->server_uuid), u128ToHex(expected.server_uuid));
    else if (owner->retired_at_ms && expected.retired == RetiredOwnerPolicy::Refuse)
        refusal = fmt::format(
            "CAS server-root '{}' was decommissioned (tombstoned at {} ms) while this claim was in progress; "
            "refusing to run under a retired owner", srid, *owner->retired_at_ms);
    else if (owner->owner_life != expected.owner_life)
        refusal = fmt::format(
            "CAS server-root '{}': the owner moved from life {} to life {} since this process started (a "
            "retirement, a rollback or a rejoin happened in between); restart to continue under the current life",
            srid, expected.owner_life, owner->owner_life);
    if (refusal.empty())
        return;
    fenceOwnClaim(op, l, srid, expected.server_uuid, our_epoch, sink);
    throw Exception(ErrorCodes::CORRUPTED_DATA, "{}", refusal);
}
```
  (The retired check precedes the life check because a tombstone also moves the life and the operator should
  read "decommissioned".)

- [ ] **Step 4: wire it.** `mountWritable`, right after `claimed_prior = claim.prior;` (`:775`):
```cpp
        /// The owner may have moved since the claim began: a tombstone, a rollback or a rejoin. Nothing
        /// below runs under an owner this process did not start under.
        CasOperation recheck_op = store->gc_requests.admit();
        recheckOwnerAfterClaim(recheck_op, store->pool_layout, srid,
            OwnerExpectation{.server_uuid = our_uuid, .owner_life = owner_life, .retired = retired_policy},
            writer_epoch, emit_mount_event);
```
  `tryRemountOnce`, after the `claim.kind != Claimed` return (`:1472`) and before `renewer_install`:
```cpp
        step = "owner_recheck";
        CasOperation recheck_op = gc_requests.admit();
        recheckOwnerAfterClaim(recheck_op, pool_layout, srid,
            OwnerExpectation{.server_uuid = our_uuid, .owner_life = recorded_owner_life, .retired = RetiredOwnerPolicy::Refuse},
            writer_epoch, emit_mount_event);
```
  Update the `mountWritable` header comment (`:570-579`): the order is now owner claim → epoch → mount claim →
  **owner recheck** → renewer → fence. Update `docs` later (Task 8), not here.

- [ ] **Step 5: run** the Step 2 filter plus `CASMount*:CASMountLease.*:CASPoolRemount.*:CASDecommission.*`.
  Expected: Step 1 tests pass; the pre-existing suites pass except Task 4's rewrite list (append reds to
  `$WT/tmp/task3-reds.txt`; anything outside that list is a stop).

- [ ] **Step 6: commit.**
```bash
git add -- $CA/Pool/CasServerRoot.h $CA/Pool/CasServerRoot.cpp $CA/Pool/CasPool.cpp \
  src/Disks/tests/gtest_cas_mount.cpp src/Disks/tests/gtest_cas_decommission.cpp src/Disks/tests/cas_test_helpers.h
git commit -m "Re-read the CAS owner after every mount claim before installing the renewer

A claim can land after the owner was tombstoned, rolled back or handed to
another server. Every claimer (start, self-remount, decommission admin) now
reads the owner after its mount write and requires its own uuid and the life
it started under; otherwise it fences its own body and fails, so a successor
does not wait the refused lease out.

Signed-off-by: Mikhail Filimonov <mfilimonov@altinity.com>
Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
## Task 4a: decommission steps 1–3, the exits, the report (D1, D2, D5, D9)

**Files:**
- Modify: `$CA/Tools/CasDecommission.h:16-57`, `$CA/Tools/CasDecommission.cpp` (whole function; delete `deleteSlotObject` `:96-116`)
- Modify: `$CA/Pool/CasPool.cpp:937-947` (`openForDecommission`: D9 counter check)
- Modify: `src/Interpreters/InterpreterSystemQuery.cpp:1107-1131`, `programs/disks/CommandCaDropMember.cpp:55-64`
- Test: `src/Disks/tests/gtest_cas_decommission.cpp`, `src/Disks/tests/gtest_cas_decommission_catalog_duties.cpp`

**Interfaces:**
- Produces (`CasDecommission.h`):
```cpp
struct DecommissionReport
{
    String srid;
    uint64_t namespaces_removed = 0;
    uint64_t namespaces_already_removed = 0;
    /// Victim rows still `Removing` when the run ended: GC deletes them; no rerun is needed for them.
    uint64_t namespaces_pending_gc = 0;
    uint64_t committed_refs_removed = 0;
    uint64_t precommits_removed = 0;
    uint64_t edge_deltas_emitted = 0;
    uint64_t manifest_debris_removed = 0;
    uint64_t staging_objects_removed = 0;
    uint64_t mountpoint_objects_removed = 0;
    /// The owner carries a verified retirement of this run or a later life: do not rerun.
    bool member_retired = false;
    /// Drain failures, a lost claim, an unverified outcome: a non-empty list means rerun.
    std::vector<String> warnings;
};

/// Whether the slot still holds the admin's claim: the victim's uuid, `admin_epoch`, neither fenced nor
/// farewelled. Renewals change the ETag and `write_attempt_id`, so neither is compared.
bool adminClaimStillHeld(CasOperation & op, const Layout & layout, const String & srid, UInt128 victim_uuid, uint64_t admin_epoch);
```
  `decommissionPoolMember`'s signature is unchanged.
- Consumes: Task 2 (`RetiredOwnerPolicy` through `NoWait`, `Pool::ownerLife`), Task 3 (the admin's recheck).

- [ ] **Step 1: write the failing tests** (`gtest_cas_decommission.cpp`). First the fixture backends, in the
  anonymous namespace, replacing `SuccessorReclaimAfterFarewellBackend`, `SuccessorReclaimAfterEpochDeleteBackend`
  and `SuccessorOwnerRewriteBeforeTombstoneBackend` (delete those three and `snapshotPrefixObjects`):

```cpp
/// GC's fence-out by hand: the body kept, `gc_fenced` set, `seq` bumped, against the current incarnation.
void fenceSlot(Backend & backend, const Layout & layout, const String & srid)
{
    OperationForTest op(backend);
    const auto got = (*op).read(layout.mountKey(srid), Retry::once());
    ASSERT_TRUE(got.has_value());
    MountLease m = decodeMountLease(got->bytes);
    m.gc_fenced = true;
    m.seq += 1;
    ASSERT_TRUE(std::holds_alternative<Committed>(
        (*op).replace(layout.mountKey(srid), encodeMountLease(m), got->etag, Retry::once())));
}

/// Swaps the owner to another uuid on the second owner read after the admin's slot claim landed: the first
/// is the claim recheck, the second is decommission step 2.
class SwapOwnerBeforeTombstoneBackend : public InMemoryBackend
{
public:
    bool fired() const { return swapped; }

    std::expected<String, RawConflict> write(const String & key, const String & bytes,
                                             const std::optional<String> & expected_value, TransportAccess & access) override
    {
        const auto result = InMemoryBackend::write(key, bytes, expected_value, access);
        if (result && key == mount_key && expected_value && !decodeMountLease(bytes).gc_fenced
            && decodeMountLease(bytes).min_active_build_sequence != std::numeric_limits<uint64_t>::max())
            claimed = true;
        return result;
    }

    std::optional<Raw> read(const String & key, TransportAccess & access) override
    {
        if (claimed && !swapped && key == owner_key && ++owner_reads == 2)
        {
            const auto current = InMemoryBackend::read(key, access);
            const OwnerObject foreign{.server_uuid = UInt128(0xBEEF), .owner_life = decodeOwner(current->bytes).owner_life + 1};
            if (!InMemoryBackend::write(key, encodeOwner(foreign), current->value, access))
                throw std::runtime_error("owner swap fixture: the rewrite conflicted");
            swapped = true;
        }
        return InMemoryBackend::read(key, access);
    }

private:
    inline static const String mount_key = "p/gc/server-roots/victim/mount";
    inline static const String owner_key = "p/gc/server-roots/victim/owner";
    bool claimed = false;
    bool swapped = false;
    int owner_reads = 0;
};
```

```cpp
TEST(CASDecommission, KeepsEpochAndFarewellMount)
{
    auto backend = std::make_shared<InMemoryBackend>();
    uint64_t victim_epoch = 0;
    { auto victim = openVictim(backend); victim_epoch = victim->writerEpoch(); }
    const Layout layout("p");

    const auto report = decommissionPoolMember(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");

    EXPECT_TRUE(report.warnings.empty());
    EXPECT_TRUE(report.member_retired);
    const MountLease slot = readSlot(*backend, layout, "victim");
    EXPECT_EQ(slot.writer_epoch, victim_epoch + 1) << "the admin's own clean farewell stays in the slot";
    EXPECT_EQ(slot.min_active_build_sequence, std::numeric_limits<uint64_t>::max());
    EXPECT_FALSE(slot.gc_fenced);
    OperationForTest raw_op(*backend);
    const auto epoch = (*raw_op).read(layout.epochKey("victim"), Retry::once());
    ASSERT_TRUE(epoch.has_value());
    EXPECT_EQ(decodeServerEpoch(epoch->bytes).next_writer_epoch, victim_epoch + 2) << "the counter is never deleted";
    const OwnerObject owner = readOwner(*backend, layout, "victim");
    EXPECT_TRUE(owner.retired_at_ms.has_value());
    EXPECT_TRUE(owner.retirement_verified);
    EXPECT_EQ(owner.owner_life, 3u) << "claim 1, tombstone 2, verified mark 3";
}

/// The admin is GC-fenced between its claim and its tombstone: the confirm sees the fence and the
/// tombstone is rolled back to a fresh, unretired life.
TEST(CASDecommission, FenceBeforeTombstoneUntombstonesAndFails)
{
    auto backend = std::make_shared<InMemoryBackend>();
    { auto victim = openVictim(backend); }
    const Layout layout("p");
    {
        OperationForTest seed_op(*backend);
        (*seed_op).create("p/roots/victim/loose_file", "x", Retry::once());
    }
    bool fired = false;
    backend->onBeforeWrite(layout.ownerKey("victim"), [&]
    {
        if (std::exchange(fired, true))
            return;
        fenceSlot(*backend, layout, "victim");
    });

    const auto report = decommissionPoolMember(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");

    ASSERT_TRUE(fired);
    EXPECT_FALSE(report.member_retired);
    ASSERT_FALSE(report.warnings.empty());
    EXPECT_NE(report.warnings.front().find("claim"), String::npos) << report.warnings.front();
    const OwnerObject owner = readOwner(*backend, layout, "victim");
    EXPECT_FALSE(owner.retired_at_ms.has_value()) << "the tombstone was rolled back";
    EXPECT_EQ(owner.owner_life, 3u) << "claim 1, tombstone 2, rollback 3";
    OperationForTest raw_op(*backend);
    EXPECT_TRUE((*raw_op).head("p/roots/victim/loose_file", Retry::once()).has_value()) << "nothing was drained";
}

/// GUARD of the confirm's comparison: a renewal between the claim and the confirm changes the ETag and the
/// attempt id, and the confirm must not care; a fence, a farewell, another epoch or another uuid is a loss.
TEST(CASDecommission, ConfirmAcceptsARenewalBetweenClaimAndConfirm)
{
    auto backend = std::make_shared<InMemoryBackend>();
    const Layout layout("p");
    OperationForTest op(*backend);
    MountLease body{.server_uuid = UInt128(7), .writer_epoch = 5, .hostname = "h", .pid = 1,
                    .started_at_ms = 1, .seq = 1, .expires_at_ms = 2, .min_active_build_sequence = 0,
                    .gc_fenced = false, .write_attempt_id = UInt128(11)};
    ASSERT_TRUE(std::holds_alternative<Committed>((*op).create(layout.mountKey("v"), encodeMountLease(body), Retry::once())));
    EXPECT_TRUE(adminClaimStillHeld(*op, layout, "v", UInt128(7), 5));

    MountLease renewed = body;
    renewed.seq = 2;
    renewed.write_attempt_id = UInt128(12);
    ASSERT_TRUE(std::holds_alternative<Committed>((*op).replace(layout.mountKey("v"), encodeMountLease(renewed),
        (*op).read(layout.mountKey("v"), Retry::once())->etag, Retry::once())));
    EXPECT_TRUE(adminClaimStillHeld(*op, layout, "v", UInt128(7), 5));

    EXPECT_FALSE(adminClaimStillHeld(*op, layout, "v", UInt128(7), 6)) << "another epoch";
    EXPECT_FALSE(adminClaimStillHeld(*op, layout, "v", UInt128(8), 5)) << "another uuid";
    MountLease fenced = renewed;
    fenced.gc_fenced = true;
    ASSERT_TRUE(std::holds_alternative<Committed>((*op).replace(layout.mountKey("v"), encodeMountLease(fenced),
        (*op).read(layout.mountKey("v"), Retry::once())->etag, Retry::once())));
    EXPECT_FALSE(adminClaimStillHeld(*op, layout, "v", UInt128(7), 5)) << "fenced";
    MountLease farewell = renewed;
    farewell.min_active_build_sequence = std::numeric_limits<uint64_t>::max();
    ASSERT_TRUE(std::holds_alternative<Committed>((*op).replace(layout.mountKey("v"), encodeMountLease(farewell),
        (*op).read(layout.mountKey("v"), Retry::once())->etag, Retry::once())));
    EXPECT_FALSE(adminClaimStillHeld(*op, layout, "v", UInt128(7), 5)) << "farewelled";
    EXPECT_FALSE(adminClaimStillHeld(*op, layout, "absent", UInt128(7), 5));
}

/// Another server took the server_root_id between the admin's claim and its tombstone (a rejoin). Step 2
/// stops before it writes anything.
TEST(CASDecommission, StopsWhenTheOwnerUuidChanged)
{
    auto backend = std::make_shared<SwapOwnerBeforeTombstoneBackend>();
    { auto victim = openVictim(backend); }
    const Layout layout("p");
    {
        OperationForTest seed_op(*backend);
        (*seed_op).create("p/roots/victim/loose_file", "x", Retry::once());
    }

    expectThrowsCodeWithMessage(ErrorCodes::ABORTED, "another server", [&]
    {
        decommissionPoolMember(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");
    });

    ASSERT_TRUE(backend->fired());
    const OwnerObject owner = readOwner(*backend, layout, "victim");
    EXPECT_EQ(owner.server_uuid, UInt128(0xBEEF));
    EXPECT_FALSE(owner.retired_at_ms.has_value()) << "the new owner was not tombstoned";
    OperationForTest raw_op(*backend);
    EXPECT_TRUE((*raw_op).head("p/roots/victim/loose_file", Retry::once()).has_value()) << "nothing was drained";
}

TEST(CASDecommission, AbsentCounterIsRefused)
{
    auto backend = std::make_shared<InMemoryBackend>();
    { auto victim = openVictim(backend); }
    const Layout layout("p");
    OperationForTest raw_op(*backend);
    ASSERT_EQ((*raw_op).removeCurrent(layout.epochKey("victim"), Retry::once()), Removal::Removed);
    const auto owner_before = (*raw_op).read(layout.ownerKey("victim"), Retry::once());
    const auto mount_before = (*raw_op).read(layout.mountKey("victim"), Retry::once());

    expectThrowsCodeWithMessage(ErrorCodes::CORRUPTED_DATA, "no epoch counter", [&]
    {
        decommissionPoolMember(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");
    });

    EXPECT_FALSE((*raw_op).head(layout.epochKey("victim"), Retry::once()).has_value()) << "never recreated";
    EXPECT_EQ((*raw_op).read(layout.ownerKey("victim"), Retry::once())->etag, owner_before->etag);
    EXPECT_EQ((*raw_op).read(layout.mountKey("victim"), Retry::once())->etag, mount_before->etag);
}

/// The owner was deleted by hand while the counter survived: the run recovers the uuid from the lease,
/// re-creates the owner over the empty subtree and retires it.
TEST(CASDecommission, OwnerDeletedByHandIsRecreatedFromTheLease)
{
    auto backend = std::make_shared<InMemoryBackend>();
    { auto victim = openVictim(backend); }
    const Layout layout("p");
    OperationForTest raw_op(*backend);
    ASSERT_EQ((*raw_op).removeCurrent(layout.ownerKey("victim"), Retry::once()), Removal::Removed);

    const auto report = decommissionPoolMember(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "a2"}, "victim");

    EXPECT_TRUE(report.warnings.empty());
    EXPECT_TRUE(report.member_retired);
    const OwnerObject owner = readOwner(*backend, layout, "victim");
    EXPECT_TRUE(owner.retirement_verified);
    EXPECT_EQ(owner.owner_life, 3u);
    EXPECT_TRUE((*raw_op).head(layout.epochKey("victim"), Retry::once()).has_value());
}

/// A resolve read settles an ambiguous tombstone write: the bytes already match, so step 2 is committed.
TEST(CASDecommission, OwnerTombstoneAmbiguousSuccessResolvesToCommitted)
{
    auto backend = std::make_shared<InMemoryBackend>();
    { auto victim = openVictim(backend); }
    backend->injectAmbiguousLandedWrite("p/gc/server-roots/victim/owner");

    const auto report = decommissionPoolMember(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");

    EXPECT_TRUE(report.member_retired) << "the ambiguous write landed and must resolve to Committed";
    EXPECT_TRUE(report.warnings.empty());
    const OwnerObject owner = readOwner(*backend, Layout("p"), "victim");
    EXPECT_TRUE(owner.retirement_verified);
    EXPECT_EQ(owner.owner_life, 3u);
}
```
  Rewrite these existing tests (same file; keep each test's fixture, change the assertions):
  - `ErasesAllVictimNamespaces`: after the first run `EXPECT_TRUE(report.warnings.empty()); EXPECT_TRUE(report.member_retired); EXPECT_EQ(report.namespaces_pending_gc, 2u);`
    then `drainCompletedNamespaceRemovals` and the rerun: `member_retired` true, `namespaces_pending_gc` 0, `namespaces_already_removed` 0.
  - `RetainsDebrisWhoseEpochSealIsUnconsumed`: `EXPECT_FALSE(report.member_retired)`; add
    `EXPECT_TRUE(readOwner(*backend, Layout("p"), "victim").retired_at_ms.has_value()); EXPECT_FALSE(readOwner(...).retirement_verified);`
    (an incomplete drain leaves the unverified tombstone as the resume anchor).
  - `RemovesMutableSlotAndRefusesTombstonedRerun` → rename `RerunOverAVerifiedRetirementRetiresAgain`: first run
    `member_retired` true, pending 1; GC drain; rerun under `admin2`: `member_retired` true, pending 0, mount
    and epoch present, owner life 5 (`1,2,3` then `4,5`), verified. The final `expectThrowsCode(CORRUPTED_DATA, …)`
    block becomes a third run that also succeeds (`member_retired` true, life 7).
  - `SuccessorReclaimFencesSlotRetirementTail`, `SuccessorReclaimAfterEpochDeleteKeepsOwnerAnchor`,
    `SuccessorOwnerRewriteWinsBeforeTombstone`: delete.
  - `FencedSlotRetirementTailRetiresUncontendedSlot` → `UncontendedRunRetiresAndKeepsTheSlot`: `member_retired`
    true; `mount` and `epoch` present; owner retired and verified.
  - `RunsOnAnOpenFence` → `VerifiedMarkIsWrittenAfterTheAdminPoolIsGone`: a single run over a victim with one
    table: `member_retired` true, pending 1, owner verified (the mark is written on the control plane after
    `admin.reset()`).
  - `SuccessfulDecommissionLeavesTombstonedOwnerAnchor`: add `EXPECT_EQ(decodeOwner(after->bytes).owner_life, 3u); EXPECT_TRUE(decodeOwner(after->bytes).retirement_verified);`.
  - `FailedDrainKeepsSlotThenResumes` → `FailedDrainLeavesAnUnverifiedTombstoneThenResumes`: `first`:
    warnings, `member_retired` false, owner retired and NOT verified, mount present; `second` (disarmed):
    `member_retired` true, `mountpoint_objects_removed` 1, `namespaces_already_removed` 1, pending 1; `third`
    after GC: `member_retired` true, pending 0.
  - `ManifestDebrisFailureKeepsSlotThenResumes` → `ManifestDebrisFailureLeavesAnUnverifiedTombstone`: both runs
    `member_retired` false, owner retired, not verified; the mount survives.
  - `DrainClockUnifiesWithTheFarewellBootClockRegardlessOfHostUptime`,
    `OpeningRetriesPaceOnTheSameFakeClockAndSleepAsTheDrain`: `EXPECT_TRUE(report.member_retired)`; replace the
    "mount absent" assertion with `EXPECT_EQ(readSlot(*backend, Layout("p"), "victim").min_active_build_sequence, std::numeric_limits<uint64_t>::max())`
    (the farewell committed on the unified clock).
  - `MidRetirementCrashResumesViaMountLeaseFallback`: delete (replaced by `OwnerDeletedByHandIsRecreatedFromTheLease`).
  - `DuplicateLifeIdRefusesBeforeAnyNamespaceOrSlotMutation`, `CatalogCutIsValidatedBeforeImpersonationAndReusedForSelection`:
    unchanged (they throw before the claim).
  - `EmitsMemberDecommissionEvents`: add `EXPECT_EQ(member_events[2].detail.at("member_retired"), "1"); EXPECT_EQ(member_events[2].detail.at("owner_life"), "2");`
    (the tombstone's life; the mark is life 3).
  `gtest_cas_decommission_catalog_duties.cpp`: every `EXPECT_FALSE(report.slot_removed)` becomes
  `EXPECT_TRUE(report.member_retired)` with `EXPECT_TRUE(report.warnings.empty())` and
  `EXPECT_EQ(report.namespaces_pending_gc, 1u)` in `RemovingWithCheckpointResumesTerminalAndKeepsSlotForGc`
  (rename `…AndIsVerifiedWithTheRowPendingGc`) and `FoldedTerminalRemainsGcOwnedAndOnlyRequestsAnotherRound`;
  `OpaqueLifeDebrisWithoutCatalogOwnershipDoesNotBlockRetirement`: `EXPECT_TRUE(report.member_retired)`.
  `RemovingWithoutCheckpointIsCorruptionAndKeepsSlot` (rename `…AndLeavesAnUnverifiedTombstone`) and
  `PartialRemovalProgressStillWakesGcWhenLaterNamespaceFails`: keep the `slotObjectExists` asserts, add
  `EXPECT_TRUE(readOwnerDuty(*backend).retired_at_ms.has_value()); EXPECT_FALSE(readOwnerDuty(*backend).retirement_verified);`
  with a local `readOwnerDuty` helper (read + `decodeOwner` of `p/gc/server-roots/victim/owner`).
  `VictimEntryAppearingBeforeTheOwnershipCutKeepsSlot` → `VictimEntryAppearingAfterTheCutBlocksTheMark`:
  `EXPECT_FALSE(report.member_retired)`, warning contains `"appeared after"`, owner retired, not verified.
  `CatalogTokenMovedBetweenOwnershipCutAndRetirementKeepsSlot` and `MutateCatalogBetweenRetirementReadsBackend`: delete (R14).

- [ ] **Step 2: run to verify they fail.** `--gtest_filter='CASDecommission.*:CASDecommissionCatalogDuties.*'`.
  Expected: compile failure (`member_retired`, `namespaces_pending_gc`, `adminClaimStillHeld` undeclared). The
  semantic reasons once it compiles on the base tail: `KeepsEpochAndFarewellMount` (mount and epoch deleted),
  `FenceBeforeTombstoneUntombstonesAndFails` (owner never tombstoned, life absent), `StopsWhenTheOwnerUuidChanged`
  (no step 2, the run completes), `AbsentCounterIsRefused` (the base re-mints over the terminal farewell),
  `OwnerDeletedByHandIsRecreatedFromTheLease` (base deletes the counter), the ambiguous-tombstone test (base
  tombstones last; life absent).

- [ ] **Step 3: D9 in `openForDecommission`** (`CasPool.cpp`, after `config.server_id = *victim_uuid;`):
```cpp
    /// A member of this build always has a counter next to its owner. An absent counter is a state left by
    /// 26.6.4 or by a process that died between the owner create and the counter create; minting one here
    /// would hand out epoch 1 again over whatever that member wrote. The subtree of such a member is empty,
    /// so deleting its owner by hand and using a new server_root_id is the safe way forward.
    if (!owner_op.read(layout.epochKey(victim_srid), Retry::standard()))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS decommission '{}': the member has an owner but no epoch counter '{}'; a decommission never "
            "creates one. If no server uses this server_root_id, delete its owner object '{}' by hand and start "
            "the server with a new server_root_id.",
            victim_srid, layout.epochKey(victim_srid), layout.ownerKey(victim_srid));
```

- [ ] **Step 4: rewrite `decommissionPoolMember`.** Replace everything from the `/// Decommission is
  administrative …` comment (`:137`) to the end of the function with the code below; keep `nowMs`,
  `removalName` and `deleteListedPrefix` (the latter changes in Task 4c). New file-local helpers go in the
  anonymous namespace; `adminClaimStillHeld` is public.

```cpp
/// This run's tombstone: what a later exit needs to decide whether to keep it or roll it back.
struct Tombstone
{
    uint64_t life = 0;                 /// the life the tombstone carries
    bool previously_verified = false;  /// the owner already held a verified retirement; it is never undone
    Etag etag;                         /// the tombstone's incarnation, the rollback's precondition
};

/// Step 2. Reads the owner, requires the victim's uuid, replaces it by the next life, retired, keeping an
/// earlier verified mark. A paused earlier run cannot roll this tombstone back: its precondition is older.
Tombstone writeTombstone(CasOperation & op, const Layout & layout, const String & srid, UInt128 victim_uuid)
{
    const String key = layout.ownerKey(srid);
    const std::optional<Object> current = op.read(key, Retry::standard());
    if (!current)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS decommission '{}': the owner anchor vanished after the admin claim", srid);
    OwnerObject owner = decodeOwner(current->bytes);
    if (owner.server_uuid != victim_uuid)
        throw Exception(ErrorCodes::ABORTED,
            "CAS decommission '{}': the owner is now server_uuid={}, the claim was made for {}; another server "
            "took this server_root_id since the claim, nothing was removed",
            srid, u128ToHex(owner.server_uuid), u128ToHex(victim_uuid));

    Tombstone tombstone{.life = owner.owner_life + 1, .previously_verified = owner.retirement_verified};
    owner.owner_life = tombstone.life;
    owner.retired_at_ms = nowMs();
    WriteResult written = op.replace(key, encodeOwner(owner), current->etag, Retry::standard());
    if (const Committed * committed = std::get_if<Committed>(&written))
    {
        tombstone.etag = committed->etag;
        return tombstone;
    }
    if (std::holds_alternative<Conflict>(written))
        throw Exception(ErrorCodes::ABORTED,
            "CAS decommission '{}': the owner was rewritten between the claim and the tombstone (a rerun or a "
            "rejoin); nothing was removed, rerun to decide against the current owner", srid);
    orThrow(std::move(written), fmt::format("CAS decommission '{}' owner tombstone", srid));
    UNREACHABLE();
}

/// Undo this run's tombstone after a lost claim: the next life, not retired, no mark. A retirement an
/// earlier run verified is never undone, and a rollback whose precondition moved was overtaken by a later
/// owner write, which then owns the outcome.
void rollBackTombstone(CasOperation & op, const Layout & layout, const String & srid, UInt128 victim_uuid,
                       const Tombstone & tombstone, std::vector<String> & warnings)
{
    if (tombstone.previously_verified)
    {
        warnings.push_back("the admin claim was lost after the tombstone; the owner keeps its earlier verified retirement");
        return;
    }
    const OwnerObject restored{.server_uuid = victim_uuid, .owner_life = tombstone.life + 1};
    const WriteResult written = op.replace(layout.ownerKey(srid), encodeOwner(restored), tombstone.etag, Retry::standard());
    if (std::holds_alternative<Committed>(written))
        warnings.push_back("the admin claim was lost after the tombstone; the tombstone was rolled back (owner life "
                           + std::to_string(tombstone.life + 1) + "); rerun to decommission");
    else
        warnings.push_back("the admin claim was lost after the tombstone and the rollback did not land: the owner was "
                           "rewritten by a later run or a rejoin; check system.cas_mounts before rerunning");
}

/// Every exit after the tombstone and before the farewell: the tombstone stays while the claim holds.
void confirmClaimOrRollBack(CasOperation & op, const Layout & layout, const String & srid, UInt128 victim_uuid,
                            uint64_t admin_epoch, const Tombstone & tombstone, std::vector<String> & warnings)
{
    if (!adminClaimStillHeld(op, layout, srid, victim_uuid, admin_epoch))
        rollBackTombstone(op, layout, srid, victim_uuid, tombstone, warnings);
}
```
```cpp
bool adminClaimStillHeld(CasOperation & op, const Layout & layout, const String & srid, UInt128 victim_uuid, uint64_t admin_epoch)
{
    const std::optional<Object> slot = op.read(layout.mountKey(srid), Retry::standard());
    if (!slot)
        return false;
    const MountLease body = decodeMountLease(slot->bytes);
    return body.server_uuid == victim_uuid && body.writer_epoch == admin_epoch && !body.gc_fenced
        && body.min_active_build_sequence != std::numeric_limits<uint64_t>::max();
}
```
  The function body below calls `verifyAndMark`, which Task 4b's Step 3 defines. Implement 4a and 4b as one
  build (never a stub that reports `member_retired = false` for a verified run): write 4a's tests, 4b's
  tests, then both implementations, run both filters, and commit once with 4a's message. The split is for
  review granularity, not for two gate runs.

```cpp
DecommissionReport decommissionPoolMember(BackendPtr backend, PoolConfig config,
                                          const String & victim_srid, const CasEventSink & sink,
                                          const std::function<void()> & request_gc_round,
                                          const std::function<uint64_t()> & drain_now_fn,
                                          const std::function<void(uint64_t)> & drain_sleep_fn)
{
    DecommissionReport report;
    report.srid = victim_srid;
    bool gc_round_needed = false;
    SCOPE_EXIT({
        if (gc_round_needed && request_gc_round)
            request_gc_round();
    });

    CasRequests preflight_requests(backend, Fence::open());
    CasOperation preflight_op = preflight_requests.admit();
    const Layout catalog_layout(config.pool_prefix);
    const CasRefCatalog::Snapshot catalog_cut = CasRefCatalog::read(preflight_op, catalog_layout);
    catalog_cut.life_index.throwIfAmbiguous("CAS decommission");

    if (drain_now_fn && drain_sleep_fn)
    {
        if (!config.boot_ms_fn)
            config.boot_ms_fn = drain_now_fn;
        if (!config.retry_sleep_fn)
            config.retry_sleep_fn = drain_sleep_fn;
    }
    config.event_sink = sink;

    /// Step 1: the admin claim. `NoWait` refuses a live victim and accepts a tombstoned owner.
    PoolPtr admin = Pool::openForDecommission(std::move(backend), std::move(config), victim_srid);
    if (drain_now_fn && drain_sleep_fn)
    {
        admin->setCasRequestNowFnForTest(drain_now_fn);
        admin->setCasRetrySleepForTest(drain_sleep_fn);
    }
    const UInt128 victim_uuid = admin->poolConfig().server_id;
    const uint64_t admin_epoch = admin->writerEpoch();
    const Layout layout = admin->layout();

    /// The control plane: always admitted, so a fenced admin can still confirm, roll back and verify. It keeps
    /// its own shared_ptr to the backend and outlives `admin.reset()`.
    CasRequests control_requests(admin->poolBackendPtr(), Fence::open());
    if (drain_now_fn && drain_sleep_fn)
    {
        control_requests.setNowFnForTest(drain_now_fn);
        control_requests.setSleepFnForTest(drain_sleep_fn);
    }
    CasOperation control = control_requests.admit();

    EventEmitter{*admin}.emit([&](CasEvent & e)
    {
        e.type = CasEventType::MemberDecommission;
        e.outcome = "begin";
        e.reason = "operator decommission of pool member";
        e.detail = {{"server_root_id", victim_srid}, {"server_uuid", u128ToHex(victim_uuid)}};
    });

    /// Step 2: the tombstone. From here on every exit decides its fate (`confirmClaimOrRollBack`).
    const Tombstone tombstone = writeTombstone(control, layout, victim_srid, victim_uuid);

    try
    {
        /// Step 3: the claim must have held through the tombstone write.
        if (!adminClaimStillHeld(control, layout, victim_srid, victim_uuid, admin_epoch))
        {
            rollBackTombstone(control, layout, victim_srid, victim_uuid, tombstone, report.warnings);
        }
        else
        {
            /// Step 4: the drain, on the admin's fenced mount plane.
            drainVictim(*admin, catalog_cut, victim_srid, report, gc_round_needed);
            if (!report.warnings.empty())
                confirmClaimOrRollBack(control, layout, victim_srid, victim_uuid, admin_epoch, tombstone, report.warnings);
        }
    }
    catch (...)
    {
        /// The farewell `admin`'s destructor writes on unwind is a clean body, which the confirm would read as
        /// a lost claim, so the decision is made here, while the admin is alive.
        confirmClaimOrRollBack(control, layout, victim_srid, victim_uuid, admin_epoch, tombstone, report.warnings);
        for (const String & warning : report.warnings)
            LOG_WARNING(getLogger("CasDecommission"), "CAS decommission '{}': {}", victim_srid, warning);
        throw;
    }

    if (!report.warnings.empty())
    {
        LOG_WARNING(getLogger("CasDecommission"),
            "CAS decommission '{}': not verified ({} warnings); rerun the command", victim_srid, report.warnings.size());
        admin.reset();   /// the farewell still lands while the claim holds; after a lost claim it loses to the fence
    }
    else
    {
        /// Step 5: the farewell. `MountLeaseRenewer::terminate` reports no outcome; step 6 reads it back.
        admin.reset();
        /// Step 6: verify and mark.
        report.member_retired = verifyAndMark(control, layout, victim_srid, victim_uuid, admin_epoch, tombstone, report.warnings);
    }

    if (sink)
    {
        CasEvent e;
        e.type = CasEventType::MemberDecommission;
        e.outcome = "end";
        e.reason = "decommission finished";
        e.detail = {{"server_root_id", victim_srid},
                    {"server_uuid", u128ToHex(victim_uuid)},
                    {"owner_life", std::to_string(tombstone.life)},
                    {"namespaces_removed", std::to_string(report.namespaces_removed)},
                    {"namespaces_pending_gc", std::to_string(report.namespaces_pending_gc)},
                    {"warnings", std::to_string(report.warnings.size())},
                    {"member_retired", report.member_retired ? "1" : "0"}};
        sink(std::move(e));
    }
    return report;
}
```
  `drainVictim` is the old namespace loop + debris + staging + roots (`:201-301`) moved into a file-local
  function `void drainVictim(Pool & admin, const CasRefCatalog::Snapshot & catalog_cut, const String & victim_srid, DecommissionReport & report, bool & gc_round_needed)`,
  with `CasOperation op = admin.mountRequests().admit();` replacing the old open-plane `op` (Task 4c proves
  the plane; do the move now, the plane change is one line). After the roots drain, replace the old
  retirement-cut block (`:303-324`) with the GC-pending count (R13):
```cpp
    /// Rows still owned by the victim after the drain. `Removing` rows are GC's: they count as pending, not
    /// as a failure. Anything else appeared after the ownership cut and was not drained: rerun.
    const CasRefCatalog::Snapshot after_drain = CasRefCatalog::read(op, admin.layout());
    for (const CatalogEntry & entry : after_drain.catalog.entries)
    {
        if (entry.ns.string() != victim_srid && !entry.ns.string().starts_with(victim_namespace_prefix))
            continue;
        if (entry.state == NsState::Removing)
            ++report.namespaces_pending_gc;
        else
            report.warnings.push_back("namespace '" + entry.ns.string() + "' appeared after the ownership cut and was not drained; rerun");
    }
```
  `verifyAndMark` comes from Task 4b; declare it in the anonymous namespace above the main function.
  Delete `deleteSlotObject`. Update the header comments of `decommissionPoolMember` in `CasDecommission.h`
  (`:33-57`): the six steps, "never deletes `owner`, `epoch` or `mount`", "a rerun resumes over the run's own
  unverified tombstone; a verified retirement is rerun-safe and reports retired again".

- [ ] **Step 5: the result row.** `InterpreterSystemQuery.cpp:1107-1131`: replace the `slot_removed` column with
  `{"namespaces_pending_gc", std::make_shared<DataTypeUInt64>()}` placed after `namespaces_already_removed`
  and `{"member_retired", std::make_shared<DataTypeUInt8>()}` in `slot_removed`'s position; the inserts in the
  same order (`report.namespaces_pending_gc`, `static_cast<UInt8>(report.member_retired)`).
  `CommandCaDropMember.cpp:55-64`: print `namespaces_pending_gc=` after `namespaces_already_removed=` and
  `member_retired=` instead of `slot_removed=`.

- [ ] **Step 6: build and run** `--gtest_filter='CASDecommission.*:CASDecommissionCatalogDuties.*'` (with Task
  4b's step 6 code in place). Expected: all pass. Then the whole `CAS*` gate (release).

- [ ] **Step 7: commit** (one commit for 4a+4b if built together; otherwise this one):
```bash
git add -- $CA/Tools/CasDecommission.h $CA/Tools/CasDecommission.cpp $CA/Pool/CasPool.cpp \
  src/Interpreters/InterpreterSystemQuery.cpp programs/disks/CommandCaDropMember.cpp \
  src/Disks/tests/gtest_cas_decommission.cpp src/Disks/tests/gtest_cas_decommission_catalog_duties.cpp
git commit -m "Keep the CAS control objects on decommission and tombstone the owner first

Decommission no longer deletes owner, epoch or mount: the counter is what
keeps epochs from repeating across a member's lives, and the farewell mount
is what keeps a paused claimer from minting a lower epoch into an absent
slot. Every run writes its own tombstone (next owner life) right after its
claim, confirms the claim still holds, drains on its fenced mount plane,
writes its clean farewell and marks the retirement verified. A lost claim
rolls the tombstone back unless an earlier run verified the retirement. The
result column slot_removed becomes member_retired; rows GC still has to
delete are reported as namespaces_pending_gc, not as a failure. The admin
refuses a member without an epoch counter instead of minting one.

Signed-off-by: Mikhail Filimonov <mfilimonov@altinity.com>
Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 4b: steps 5–6, the verified mark and the outcome re-read

**Files:**
- Modify: `$CA/Tools/CasDecommission.cpp` (anonymous namespace: `verifyAndMark`, `outcomeFromOwner`)
- Test: `src/Disks/tests/gtest_cas_decommission.cpp`

**Interfaces:**
- Produces (file-local): `bool verifyAndMark(CasOperation & control, const Layout &, const String & srid, UInt128 victim_uuid, uint64_t admin_epoch, const Tombstone &, std::vector<String> & warnings)`.
- Consumes: Task 4a's `Tombstone`, `adminClaimStillHeld`.

- [ ] **Step 1: write the failing tests.** Fixture backend:

```cpp
/// Fences the admin's body right before its farewell lands, so the farewell conflicts and
/// `MountLeaseRenewer::terminate` returns without an outcome.
class FenceBeforeFarewellBackend : public InMemoryBackend
{
public:
    bool fired() const { return fenced; }

    std::expected<String, RawConflict> write(const String & key, const String & bytes,
                                             const std::optional<String> & expected_value, TransportAccess & access) override
    {
        if (!fenced && key == mount_key && expected_value
            && decodeMountLease(bytes).min_active_build_sequence == std::numeric_limits<uint64_t>::max())
        {
            const auto current = InMemoryBackend::read(key, access);
            MountLease body = decodeMountLease(current->bytes);
            body.gc_fenced = true;
            body.seq += 1;
            if (!InMemoryBackend::write(key, encodeMountLease(body), current->value, access))
                throw std::runtime_error("fence-before-farewell fixture: the fence conflicted");
            fenced = true;
        }
        return InMemoryBackend::write(key, bytes, expected_value, access);
    }

private:
    inline static const String mount_key = "p/gc/server-roots/victim/mount";
    bool fenced = false;
};
```
```cpp
TEST(CASDecommission, UnconfirmedFarewellIsNotRetired)
{
    auto backend = std::make_shared<FenceBeforeFarewellBackend>();
    { auto victim = openVictim(backend); }
    const Layout layout("p");

    const auto report = decommissionPoolMember(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");

    ASSERT_TRUE(backend->fired());
    EXPECT_FALSE(report.member_retired);
    ASSERT_FALSE(report.warnings.empty());
    EXPECT_NE(report.warnings.back().find("rerun"), String::npos) << report.warnings.back();
    const OwnerObject owner = readOwner(*backend, layout, "victim");
    EXPECT_TRUE(owner.retired_at_ms.has_value());
    EXPECT_FALSE(owner.retirement_verified);
    EXPECT_EQ(owner.owner_life, 2u) << "the tombstone stays; the mark was not written";
    EXPECT_TRUE(readSlot(*backend, layout, "victim").gc_fenced);
}

TEST(CASDecommission, VerifiedMarkDoesNotWaitForCatalogGc)
{
    auto backend = std::make_shared<InMemoryBackend>();
    {
        auto victim = openVictim(backend);
        makeTableWithRefs(*victim, "victim/db/t1", 1, 0);
    }
    const auto report = decommissionPoolMember(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");
    EXPECT_EQ(report.namespaces_removed, 1u);
    EXPECT_EQ(report.namespaces_pending_gc, 1u);
    EXPECT_TRUE(report.warnings.empty());
    EXPECT_TRUE(report.member_retired);
    EXPECT_TRUE(readOwner(*backend, Layout("p"), "victim").retirement_verified);
}

/// A rerun over a verified retirement loses its claim after its tombstone: nothing is rolled back and the
/// owner stays retired.
TEST(CASDecommission, RollbackKeepsAPreviousRetirement)
{
    auto backend = std::make_shared<InMemoryBackend>();
    { auto victim = openVictim(backend); }
    const Layout layout("p");
    const auto first = decommissionPoolMember(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "a1"}, "victim");
    ASSERT_TRUE(first.member_retired);
    ASSERT_EQ(readOwner(*backend, layout, "victim").owner_life, 3u);

    bool fired = false;
    backend->onWriteCommitted(layout.ownerKey("victim"), [&]
    {
        if (std::exchange(fired, true))
            return;
        fenceSlot(*backend, layout, "victim");   /// the rerun's tombstone landed; its claim is lost now
    });
    const auto rerun = decommissionPoolMember(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "a2"}, "victim");

    ASSERT_TRUE(fired);
    EXPECT_FALSE(rerun.member_retired);
    ASSERT_FALSE(rerun.warnings.empty());
    EXPECT_NE(rerun.warnings.front().find("earlier verified retirement"), String::npos) << rerun.warnings.front();
    const OwnerObject owner = readOwner(*backend, layout, "victim");
    EXPECT_TRUE(owner.retired_at_ms.has_value());
    EXPECT_TRUE(owner.retirement_verified);
    EXPECT_EQ(owner.owner_life, 4u) << "the rerun's tombstone refresh, kept";
}

/// Three runs: one verifies, one refreshes the tombstone and dies, one reruns and is fenced before its
/// confirm. The mark survives all three.
TEST(CASDecommission, RefreshKeepsTheVerifiedMark)
{
    auto backend = std::make_shared<InMemoryBackend>();
    { auto victim = openVictim(backend); }
    const Layout layout("p");
    ASSERT_TRUE(decommissionPoolMember(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "a1"}, "victim").member_retired);
    ASSERT_TRUE(readOwner(*backend, layout, "victim").retirement_verified);

    tombstoneOwner(*backend, layout, "victim");   /// a second run's step 2, then death: life 4, mark kept
    ASSERT_TRUE(readOwner(*backend, layout, "victim").retirement_verified);

    bool fired = false;
    backend->onWriteCommitted(layout.ownerKey("victim"), [&]
    {
        if (std::exchange(fired, true))
            return;
        fenceSlot(*backend, layout, "victim");
    });
    const auto third = decommissionPoolMember(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "a3"}, "victim");
    ASSERT_TRUE(fired);
    EXPECT_FALSE(third.member_retired);
    const OwnerObject owner = readOwner(*backend, layout, "victim");
    EXPECT_TRUE(owner.retired_at_ms.has_value());
    EXPECT_TRUE(owner.retirement_verified);
    EXPECT_EQ(owner.owner_life, 5u);
}

/// A run dies after each step; a rerun completes over the run's own tombstone.
TEST(CASDecommission, ResumesOverItsOwnTombstone)
{
    enum class DieAt { AfterTombstone, AfterFarewell, AfterMark };
    for (const DieAt die_at : {DieAt::AfterTombstone, DieAt::AfterFarewell, DieAt::AfterMark})
    {
        SCOPED_TRACE(static_cast<int>(die_at));
        auto backend = std::make_shared<InMemoryBackend>();
        { auto victim = openVictim(backend); }
        const Layout layout("p");
        {
            OperationForTest seed_op(*backend);
            (*seed_op).create("p/roots/victim/loose_file", "x", Retry::once());
        }
        /// `BAD_ARGUMENTS` is a deterministic local failure to the request engine: the attempt is not
        /// reissued and not resolved by a read, so the landed write surfaces as a refusal — the shape of a
        /// process that died right after its write landed.
        const auto died_with = [](const char * what)
        {
            return std::make_exception_ptr(DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "{}", what));
        };
        bool died = false;
        switch (die_at)
        {
            case DieAt::AfterTombstone:
                backend->onWriteCommitted(layout.ownerKey("victim"), [&]
                {
                    if (!std::exchange(died, true))
                        std::rethrow_exception(died_with("died after the tombstone"));
                });
                break;
            case DieAt::AfterFarewell:
                /// The farewell is written by `~Pool`, which swallows a throw; die on the verify read instead.
                backend->onWriteCommitted(layout.mountKey("victim"), [&]
                {
                    const MountLease body = readSlot(*backend, layout, "victim");
                    if (body.min_active_build_sequence == std::numeric_limits<uint64_t>::max() && !std::exchange(died, true))
                        backend->failNextReadWith(layout.ownerKey("victim"), died_with("died after the farewell"));
                });
                break;
            case DieAt::AfterMark:
                backend->onWriteCommitted(layout.ownerKey("victim"), [&]
                {
                    if (readOwner(*backend, layout, "victim").retirement_verified && !std::exchange(died, true))
                        std::rethrow_exception(died_with("died after the mark"));
                });
                break;
        }

        DecommissionReport first;
        bool threw = false;
        try
        {
            first = decommissionPoolMember(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "a1"}, "victim");
        }
        catch (const DB::Exception &)
        {
            threw = true;
        }
        ASSERT_TRUE(died);
        switch (die_at)
        {
            case DieAt::AfterTombstone:
                EXPECT_TRUE(threw) << "the tombstone write is refused after it landed; nothing else ran";
                break;
            case DieAt::AfterFarewell:
                EXPECT_FALSE(threw) << "a failed verify read is reported, not thrown";
                EXPECT_FALSE(first.member_retired);
                break;
            case DieAt::AfterMark:
                EXPECT_FALSE(threw) << "an unclear mark write is settled by the owner re-read";
                EXPECT_TRUE(first.member_retired) << "the re-read finds a life past the tombstone's";
                break;
        }
        const OwnerObject after_first = readOwner(*backend, layout, "victim");
        EXPECT_TRUE(after_first.retired_at_ms.has_value());
        EXPECT_EQ(after_first.retirement_verified, die_at == DieAt::AfterMark);

        const auto rerun = decommissionPoolMember(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "a2"}, "victim");
        EXPECT_TRUE(rerun.warnings.empty()) << fmt::format("{}", fmt::join(rerun.warnings, "; "));
        EXPECT_TRUE(rerun.member_retired);
        const OwnerObject owner = readOwner(*backend, layout, "victim");
        EXPECT_TRUE(owner.retirement_verified);
        EXPECT_EQ(owner.owner_life, die_at == DieAt::AfterMark ? 5u : 4u);
        OperationForTest raw_op(*backend);
        EXPECT_FALSE((*raw_op).head("p/roots/victim/loose_file", Retry::once()).has_value());
    }
}
```
  Check in `CasRequests.cpp` (`isDeterministicLocalFailure`) that `BAD_ARGUMENTS` from a backend write or
  read is neither reissued nor resolved by a read; if it is, pick the code that function names as
  deterministic and use it in `died_with`.
  Fixture assumption to verify once in `VerifiedMarkDoesNotWaitForCatalogGc`: a victim seeded by
  `makeTableWithRefs` must finish with empty `warnings` (its manifests are owned, so `sweepNamespace` skips
  them log-only). If the manifest sweep records a retention warning for them, seed the victim with a
  manifest-free namespace instead (`victim->namespaceLife(ns)` + `putNamespaceFile`, as
  `CASDecommissionCatalogDuties.FoldedTerminalRemainsGcOwnedAndOnlyRequestsAnotherRound` does) in this test,
  in `ErasesAllVictimNamespaces`' rewrite and in Task 4c's `InFlightDeleteAfterRejoinHitsOnlyIdenticalRootsBytes`.

- [ ] **Step 2: run to verify they fail.** On the base tail: `UnconfirmedFarewellIsNotRetired` (owner never
  tombstoned), `VerifiedMarkDoesNotWaitForCatalogGc` (slot not removed → `member_retired` false),
  `RollbackKeepsAPreviousRetirement` and `RefreshKeepsTheVerifiedMark` (rerun refused with `CORRUPTED_DATA`),
  `ResumesOverItsOwnTombstone` (the owner hook never fires before the end).

- [ ] **Step 3: implement** (anonymous namespace, above `decommissionPoolMember`):
```cpp
/// What the owner says once this run can no longer act: a life past the tombstone's means this run's mark or
/// a later writer's (a rerun, a rejoin), both "retired, do not rerun"; the tombstone's own life means rerun.
bool outcomeFromOwner(CasOperation & op, const Layout & layout, const String & srid, const Tombstone & tombstone,
                      std::vector<String> & warnings)
{
    try
    {
        if (const std::optional<Object> current = op.read(layout.ownerKey(srid), Retry::standard()))
            if (decodeOwner(current->bytes).owner_life > tombstone.life)
                return true;
    }
    catch (...)
    {
        warnings.push_back("could not re-read the owner: " + getCurrentExceptionMessage(/*with_stacktrace=*/false));
    }
    warnings.push_back("the retirement is not verified; rerun SYSTEM CAS DROP POOL MEMBER");
    return false;
}

/// Step 6. The mark records that this run's claim, tombstone and farewell all held; it does not wait for GC.
bool verifyAndMark(CasOperation & op, const Layout & layout, const String & srid, UInt128 victim_uuid,
                   uint64_t admin_epoch, const Tombstone & tombstone, std::vector<String> & warnings)
{
    const String owner_key = layout.ownerKey(srid);
    try
    {
        const std::optional<Object> slot = op.read(layout.mountKey(srid), Retry::standard());
        const std::optional<Object> current = op.read(owner_key, Retry::standard());
        if (slot && current)
        {
            const MountLease body = decodeMountLease(slot->bytes);
            OwnerObject owner = decodeOwner(current->bytes);
            const bool farewell_held = body.server_uuid == victim_uuid && body.writer_epoch == admin_epoch
                && !body.gc_fenced && body.min_active_build_sequence == std::numeric_limits<uint64_t>::max();
            const bool tombstone_held = owner.server_uuid == victim_uuid && owner.retired_at_ms
                && owner.owner_life == tombstone.life;
            if (farewell_held && tombstone_held)
            {
                owner.owner_life = tombstone.life + 1;
                owner.retirement_verified = true;
                if (std::holds_alternative<Committed>(op.replace(owner_key, encodeOwner(owner), current->etag, Retry::standard())))
                    return true;
            }
            else
                warnings.push_back(farewell_held ? "the owner is not this run's tombstone" : "the slot does not hold this run's farewell");
        }
    }
    catch (...)
    {
        warnings.push_back("verification failed: " + getCurrentExceptionMessage(/*with_stacktrace=*/false));
    }
    return outcomeFromOwner(op, layout, srid, tombstone, warnings);
}
```
  `outcomeFromOwner` returning true after a warning was pushed: clear the warnings pushed by this function's
  failed arm before returning true — keep a `const size_t before = warnings.size();` at entry of
  `verifyAndMark` and `warnings.resize(before)` when the re-read reports retired, so a retired outcome
  carries no "rerun" text.

- [ ] **Step 4: run** `--gtest_filter='CASDecommission.*:CASDecommissionCatalogDuties.*'`. Expected: all pass.

- [ ] **Step 5: commit** (if not folded into Task 4a's commit):
```bash
git add -- $CA/Tools/CasDecommission.cpp src/Disks/tests/gtest_cas_decommission.cpp
git commit -m "Verify a CAS decommission and mark the owner before reporting it retired

After the farewell the run reads the slot and the owner back: its own clean
farewell and its own tombstone, then the verified mark (next life). A later
failure or an unclear mark write re-reads the owner: a life past the
tombstone's is retired, the tombstone's own life means rerun.

Signed-off-by: Mikhail Filimonov <mfilimonov@altinity.com>
Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 4c: the fenced drain plane and the concurrency tests

**Files:**
- Modify: `$CA/Tools/CasDecommission.cpp:60-94` (`deleteListedPrefix`), `drainVictim`; `$CA/Gc/CasOrphanManifestSweep.cpp:560-566` (`sweepNamespace`'s operation)
- Test: `src/Disks/tests/gtest_cas_decommission.cpp`

**Interfaces:**
- Consumes: Task 4a/4b. `sweepNamespace` keeps its signature; its internal operation moves from
  `store.openRequests()` to `store.mountRequests()` (grep first: at `2a034cdb374` its only production caller
  is `CasDecommission.cpp:287`; if a GC path calls it too, add a `CasRequests & plane` parameter instead and
  pass `admin.mountRequests()` from the drain).

- [ ] **Step 1: write the failing tests.** Fixture backends:

```cpp
/// The first roots delete under the victim lets the admin's local lease deadline pass (the fake clock jumps a
/// TTL); every delete admitted on the mount plane after it is refused. Optionally also GC-fences the body.
class ExpireLeaseOnFirstRootsDeleteBackend : public InMemoryBackend
{
public:
    std::shared_ptr<DB::Cas::tests::FakeClock> clock;
    uint64_t jump_ms = 0;
    bool fence_body = false;
    bool fired() const { return done; }

    RawRemoval remove(const String & key, const String & expected_value, TransportAccess & access) override
    {
        if (!done && key.starts_with("p/roots/victim/"))
        {
            done = true;
            clock->now += jump_ms;
            if (fence_body)
            {
                const auto current = InMemoryBackend::read(mount_key, access);
                MountLease body = decodeMountLease(current->bytes);
                body.gc_fenced = true;
                body.seq += 1;
                if (!InMemoryBackend::write(mount_key, encodeMountLease(body), current->value, access))
                    throw std::runtime_error("expire fixture: the fence conflicted");
            }
        }
        return InMemoryBackend::remove(key, expected_value, access);
    }

private:
    inline static const String mount_key = "p/gc/server-roots/victim/mount";
    bool done = false;
};

/// On the staging LIST (the request before the roots LIST) lets the admin's local lease deadline pass and
/// GC-fences its body in the store: the fence loss then surfaces on the roots LIST page, not on a delete.
class ExpireLeaseBeforeRootsListBackend : public InMemoryBackend
{
public:
    using Backend::list;
    std::shared_ptr<DB::Cas::tests::FakeClock> clock;
    uint64_t jump_ms = 0;
    bool fired() const { return done; }

    RawListPage list(const String & prefix, const String & cursor, size_t limit, TransportAccess & access) override
    {
        RawListPage page = InMemoryBackend::list(prefix, cursor, limit, access);
        if (!done && prefix == "p/staging/victim/")
        {
            done = true;
            clock->now += jump_ms;
            const auto current = InMemoryBackend::read(mount_key, access);
            MountLease body = decodeMountLease(current->bytes);
            body.gc_fenced = true;
            body.seq += 1;
            if (!InMemoryBackend::write(mount_key, encodeMountLease(body), current->value, access))
                throw std::runtime_error("expire-before-list fixture: the fence conflicted");
        }
        return page;
    }

private:
    inline static const String mount_key = "p/gc/server-roots/victim/mount";
    bool done = false;
};

/// A store whose `roots/` objects carry content-derived ETags (as S3 does) and whose deletes under the
/// victim's prefixes are accepted but not applied while `capture` is set: the attempt is still in flight and
/// the test delivers it later with `replay`.
class LateDeleteBackend : public InMemoryBackend
{
public:
    struct Late { String key; Etag etag; };
    std::vector<Late> late;
    bool capture = false;

    std::optional<Raw> read(const String & key, TransportAccess & access) override
    {
        auto raw = InMemoryBackend::read(key, access);
        if (raw && isRoots(key))
            raw->value = contentValue(raw->bytes);
        return raw;
    }

    std::optional<RawMeta> head(const String & key, TransportAccess & access) override
    {
        auto meta = InMemoryBackend::head(key, access);
        if (meta && isRoots(key))
            if (const auto raw = InMemoryBackend::read(key, access))
                meta->value = contentValue(raw->bytes);
        return meta;
    }

    RawListPage list(const String & prefix, const String & cursor, size_t limit, TransportAccess & access) override
    {
        RawListPage page = InMemoryBackend::list(prefix, cursor, limit, access);
        for (RawListedKey & listed : page.keys)
            if (listed.value && isRoots(listed.key))
                if (const auto raw = InMemoryBackend::read(listed.key, access))
                    listed.value = contentValue(raw->bytes);
        return page;
    }

    std::expected<String, RawConflict> write(const String & key, const String & bytes,
                                             const std::optional<String> & expected_value, TransportAccess & access) override
    {
        if (!isRoots(key))
            return InMemoryBackend::write(key, bytes, expected_value, access);
        std::optional<String> minted_expected;
        if (expected_value)
        {
            const auto current = InMemoryBackend::read(key, access);
            if (!current || contentValue(current->bytes) != *expected_value)
                return std::unexpected(RawConflict{});
            minted_expected = current->value;
        }
        const auto written = InMemoryBackend::write(key, bytes, minted_expected, access);
        if (!written)
            return written;
        return contentValue(bytes);
    }

    RawRemoval remove(const String & key, const String & expected_value, TransportAccess & access) override
    {
        if (capture && isVictim(key))
        {
            OperationForTest op(*this);
            if (const auto meta = (*op).head(key, Retry::once()))
                late.push_back({key, meta->etag});
            return RawRemoval::Removed;
        }
        if (!isRoots(key))
            return InMemoryBackend::remove(key, expected_value, access);
        const auto current = InMemoryBackend::read(key, access);
        if (!current)
            return RawRemoval::Gone;
        if (contentValue(current->bytes) != expected_value)
            return RawRemoval::Mismatch;
        return InMemoryBackend::remove(key, current->value, access);
    }

    /// Deliver every captured attempt now, through the ordinary exact-incarnation delete.
    std::vector<std::pair<String, Removal>> replay()
    {
        std::vector<std::pair<String, Removal>> outcomes;
        OperationForTest op(*this);
        for (const Late & attempt : late)
            outcomes.emplace_back(attempt.key, (*op).remove(attempt.key, attempt.etag, Retry::once()));
        return outcomes;
    }

private:
    static bool isRoots(const String & key) { return key.starts_with("p/roots/victim/"); }
    static bool isVictim(const String & key)
    {
        return isRoots(key) || key.starts_with("p/staging/victim/") || key.starts_with("p/cas/manifests/victim/")
            || key.starts_with("p/cas/ns/");
    }
    static String contentValue(const String & bytes) { return "content-" + DB::Cas::tests::hexOf(bytes); }
};

/// Each delete under the victim's roots prefix takes `delay`, as a slow store would.
class SlowRootsDeleteBackend : public InMemoryBackend
{
public:
    std::chrono::milliseconds delay{0};

    RawRemoval remove(const String & key, const String & expected_value, TransportAccess & access) override
    {
        if (key.starts_with("p/roots/victim/"))
            std::this_thread::sleep_for(delay);
        return InMemoryBackend::remove(key, expected_value, access);
    }
};
```
  Tests:
```cpp
TEST(CASDecommission, DrainStopsOnTheAdminLeaseFence)
{
    auto backend = std::make_shared<ExpireLeaseOnFirstRootsDeleteBackend>();
    { auto victim = openVictim(backend); }
    const Layout layout("p");
    {
        OperationForTest seed_op(*backend);
        (*seed_op).create("p/roots/victim/a_first", "x", Retry::once());
        (*seed_op).create("p/roots/victim/b_second", "x", Retry::once());
    }
    auto clock = std::make_shared<DB::Cas::tests::FakeClock>();
    clock->now = 1'000'000'000'000'000ULL;
    backend->clock = clock;
    backend->jump_ms = 31'000;   /// past the default 30 s TTL

    const auto report = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "a1"}, "victim",
        /*sink=*/{}, /*request_gc_round=*/{},
        [clock] { return clock->nowFn()(); }, [clock](uint64_t ms) { clock->sleepFn()(ms); });

    ASSERT_TRUE(backend->fired());
    EXPECT_FALSE(report.member_retired);
    ASSERT_FALSE(report.warnings.empty());
    EXPECT_EQ(report.mountpoint_objects_removed, 1u) << "the first delete was already sent";
    OperationForTest raw_op(*backend);
    EXPECT_TRUE((*raw_op).head("p/roots/victim/b_second", Retry::once()).has_value())
        << "a delete admitted after the lease deadline is refused locally";
    const OwnerObject owner = readOwner(*backend, layout, "victim");
    EXPECT_TRUE(owner.retired_at_ms.has_value()) << "the claim still holds in the store, so the tombstone stays";
    EXPECT_FALSE(owner.retirement_verified);
    EXPECT_EQ(owner.owner_life, 2u);
}

/// The admin is fenced in the store AND past its local deadline: the drain stops, and the rollback goes
/// through the control plane, which the mount fence does not gate.
TEST(CASDecommission, RollbackRunsOnTheControlPlaneAfterAFence)
{
    auto backend = std::make_shared<ExpireLeaseOnFirstRootsDeleteBackend>();
    { auto victim = openVictim(backend); }
    const Layout layout("p");
    {
        OperationForTest seed_op(*backend);
        (*seed_op).create("p/roots/victim/a_first", "x", Retry::once());
        (*seed_op).create("p/roots/victim/b_second", "x", Retry::once());
    }
    auto clock = std::make_shared<DB::Cas::tests::FakeClock>();
    clock->now = 1'000'000'000'000'000ULL;
    backend->clock = clock;
    backend->jump_ms = 31'000;
    backend->fence_body = true;

    const auto report = decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "a1"}, "victim",
        /*sink=*/{}, /*request_gc_round=*/{},
        [clock] { return clock->nowFn()(); }, [clock](uint64_t ms) { clock->sleepFn()(ms); });

    ASSERT_TRUE(backend->fired());
    EXPECT_FALSE(report.member_retired);
    const OwnerObject owner = readOwner(*backend, layout, "victim");
    EXPECT_FALSE(owner.retired_at_ms.has_value()) << "rolled back";
    EXPECT_EQ(owner.owner_life, 3u);
    EXPECT_TRUE(readSlot(*backend, layout, "victim").gc_fenced);
}

TEST(CASDecommission, FenceLossOnAListPageStillRunsTheConfirm)
{
    auto backend = std::make_shared<ExpireLeaseBeforeRootsListBackend>();
    { auto victim = openVictim(backend); }
    const Layout layout("p");
    auto clock = std::make_shared<DB::Cas::tests::FakeClock>();
    clock->now = 1'000'000'000'000'000ULL;
    backend->clock = clock;
    backend->jump_ms = 31'000;

    EXPECT_THROW(decommissionPoolMember(
        backend, PoolConfig{.pool_prefix = "p", .server_root_id = "a1"}, "victim",
        /*sink=*/{}, /*request_gc_round=*/{},
        [clock] { return clock->nowFn()(); }, [clock](uint64_t ms) { clock->sleepFn()(ms); }), DB::Exception);

    ASSERT_TRUE(backend->fired());
    const OwnerObject owner = readOwner(*backend, layout, "victim");
    EXPECT_FALSE(owner.retired_at_ms.has_value()) << "the exception exit confirmed and rolled back";
    EXPECT_EQ(owner.owner_life, 3u);
}

/// GUARD (real time): the background renewer keeps the admin's lease alive across a drain longer than one
/// TTL. TTL 1 s, renewal every 200 ms, three roots deletes of 400 ms each.
TEST(CASDecommission, LongDrainOutlivesOneTtl)
{
    auto backend = std::make_shared<SlowRootsDeleteBackend>();
    backend->delay = std::chrono::milliseconds(400);
    const CasRequestBudget small_budget{
        .attempt_timeout_ms = 100, .lease_safety_margin_ms = 100, .connect_timeout_cap_ms = std::nullopt};
    { auto victim = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "victim",
        .mount_lease_ttl_ms = std::chrono::milliseconds(1000), .mount_renew_period = std::chrono::milliseconds(200),
        .cas_request_budget = small_budget}); }
    {
        OperationForTest seed_op(*backend);
        for (const char * name : {"a", "b", "c"})
            (*seed_op).create(String("p/roots/victim/") + name, "x", Retry::once());
    }

    const auto started = std::chrono::steady_clock::now();
    const auto report = decommissionPoolMember(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "a1",
        .mount_lease_ttl_ms = std::chrono::milliseconds(1000), .mount_renew_period = std::chrono::milliseconds(200),
        .cas_request_budget = small_budget}, "victim");

    EXPECT_GE(std::chrono::steady_clock::now() - started, std::chrono::milliseconds(1200));
    EXPECT_TRUE(report.warnings.empty()) << fmt::format("{}", fmt::join(report.warnings, "; "));
    EXPECT_TRUE(report.member_retired);
    EXPECT_EQ(report.mountpoint_objects_removed, 3u);
}

/// GUARD: once the owner is tombstoned, the admin's own self-remount fails on the owner check and never
/// reclaims under a fresh epoch.
TEST(CASDecommission, FencedAdminFailsInsteadOfRemounting)
{
    auto backend = std::make_shared<InMemoryBackend>();
    { auto victim = openVictim(backend); }
    const Layout layout("p");
    auto admin = Pool::openForDecommission(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "a1"}, "victim");
    const uint64_t admin_epoch = admin->writerEpoch();
    tombstoneOwner(*backend, layout, "victim");
    fenceSlot(*backend, layout, "victim");
    EXPECT_THROW(admin->renewWatermarkOnce(), DB::Exception);
    EXPECT_FALSE(admin->tryRemountOnce());
    const MountLease slot = readSlot(*backend, layout, "victim");
    EXPECT_EQ(slot.writer_epoch, admin_epoch);
    EXPECT_TRUE(slot.gc_fenced);
    EXPECT_FALSE(admin->mayMutate());
}

/// The admin is GC-fenced after its tombstone; its renewer trips during the drain and its self-remount fails
/// (owner retired). The run's exit confirms, finds the claim lost and rolls the tombstone back. Real time:
/// TTL 1 s, renewal every 200 ms, a 400 ms delete per roots object.
TEST(CASDecommission, FailedRemountStillRunsTheConfirm)
{
    auto backend = std::make_shared<SlowRootsDeleteBackend>();
    backend->delay = std::chrono::milliseconds(400);
    const CasRequestBudget small_budget{
        .attempt_timeout_ms = 100, .lease_safety_margin_ms = 100, .connect_timeout_cap_ms = std::nullopt};
    { auto victim = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "victim",
        .mount_lease_ttl_ms = std::chrono::milliseconds(1000), .mount_renew_period = std::chrono::milliseconds(200),
        .cas_request_budget = small_budget}); }
    const Layout layout("p");
    {
        OperationForTest seed_op(*backend);
        for (const char * name : {"a", "b", "c", "d"})
            (*seed_op).create(String("p/roots/victim/") + name, "x", Retry::once());
    }
    bool fenced = false;
    backend->onWriteCommitted(layout.ownerKey("victim"), [&]
    {
        if (!std::exchange(fenced, true))
            fenceSlot(*backend, layout, "victim");
    });
    const uint64_t remount_failures_before = ProfileEvents::global_counters[ProfileEvents::CASRemountFailed].load();

    const auto report = decommissionPoolMember(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "a1",
        .mount_lease_ttl_ms = std::chrono::milliseconds(1000), .mount_renew_period = std::chrono::milliseconds(200),
        .cas_request_budget = small_budget}, "victim");

    ASSERT_TRUE(fenced);
    EXPECT_FALSE(report.member_retired);
    EXPECT_GT(ProfileEvents::global_counters[ProfileEvents::CASRemountFailed].load(), remount_failures_before)
        << "the admin's self-remount ran and failed";
    const OwnerObject owner = readOwner(*backend, layout, "victim");
    EXPECT_FALSE(owner.retired_at_ms.has_value()) << "rolled back: the claim was lost";
    EXPECT_EQ(owner.owner_life, 3u);
    EXPECT_TRUE(readSlot(*backend, layout, "victim").gc_fenced);
}

/// A paused run resumes after a rerun refreshed the tombstone: its rollback names an older incarnation and
/// cannot remove the rerun's tombstone.
TEST(CASDecommission, PausedRunCannotUntombstoneARerunsTombstone)
{
    auto backend = std::make_shared<InMemoryBackend>();
    { auto victim = openVictim(backend); }
    const Layout layout("p");
    std::optional<DecommissionReport> rerun;
    bool armed = true;
    backend->onWriteCommitted(layout.ownerKey("victim"), [&]
    {
        if (!std::exchange(armed, false))
            return;
        /// Run 1 is paused right after its tombstone. GC fences it; the rerun reclaims, refreshes the
        /// tombstone, drains, farewells and verifies.
        fenceSlot(*backend, layout, "victim");
        rerun = decommissionPoolMember(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "a2"}, "victim");
    });

    const auto first = decommissionPoolMember(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "a1"}, "victim");

    ASSERT_TRUE(rerun.has_value());
    EXPECT_TRUE(rerun->member_retired);
    EXPECT_FALSE(first.member_retired);
    ASSERT_FALSE(first.warnings.empty());
    EXPECT_NE(first.warnings.front().find("did not land"), String::npos) << first.warnings.front();
    const OwnerObject owner = readOwner(*backend, layout, "victim");
    EXPECT_TRUE(owner.retired_at_ms.has_value());
    EXPECT_TRUE(owner.retirement_verified);
    EXPECT_EQ(owner.owner_life, 4u) << "run 1 tombstone 2, rerun tombstone 3, rerun mark 4";
}

/// A delete attempt sent by a run and delivered after a rejoin can remove only an object whose key and bytes
/// repeat: the roots objects. Namespace, manifest and staging keys of the new member never match.
TEST(CASDecommission, InFlightDeleteAfterRejoinHitsOnlyIdenticalRootsBytes)
{
    auto backend = std::make_shared<LateDeleteBackend>();
    const RootNamespace ns("victim/db/t1");
    {
        auto victim = openVictim(backend);
        makeTableWithRefs(*victim, ns.string(), 1, 0);   /// no orphan manifest: its retention would block the mark
    }
    const Layout layout("p");
    {
        OperationForTest seed_op(*backend);
        (*seed_op).create("p/staging/victim/3/old.tmp", "x", Retry::once());
        (*seed_op).create("p/roots/victim/clickhouse_access_check", "probe", Retry::once());
    }
    backend->capture = true;
    const auto report = decommissionPoolMember(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "a1"}, "victim");
    backend->capture = false;
    ASSERT_TRUE(report.member_retired);
    ASSERT_FALSE(backend->late.empty());

    /// The rejoin and the new member's first writes, by hand: the next owner life, a new staging object, a
    /// new manifest, and the access probe with the same bytes under the same key.
    {
        OperationForTest op(*backend);
        const auto owner = (*op).read(layout.ownerKey("victim"), Retry::once());
        const OwnerObject rejoined{.server_uuid = decodeOwner(owner->bytes).server_uuid,
                                   .owner_life = decodeOwner(owner->bytes).owner_life + 1};
        ASSERT_TRUE(std::holds_alternative<Committed>((*op).replace(layout.ownerKey("victim"), encodeOwner(rejoined), owner->etag, Retry::once())));
        ASSERT_TRUE(std::holds_alternative<Committed>((*op).create("p/staging/victim/9/new.tmp", "y", Retry::once())));
        ASSERT_TRUE(std::holds_alternative<Committed>((*op).create("p/roots/victim/clickhouse_access_check", "probe", Retry::once())));
    }
    const ManifestRef new_ref{.writer_epoch = 9, .build_sequence = 1, .manifest_ordinal = 1};
    const ManifestId new_manifest = writeManifestRaw(*backend, layout, ns, new_ref, {});

    const auto outcomes = backend->replay();
    OperationForTest raw_op(*backend);
    EXPECT_FALSE((*raw_op).head("p/roots/victim/clickhouse_access_check", Retry::once()).has_value())
        << "identical bytes repeat the ETag: the late delete lands on the new member's probe (the documented residual)";
    EXPECT_TRUE((*raw_op).head("p/staging/victim/9/new.tmp", Retry::once()).has_value());
    EXPECT_TRUE((*raw_op).head(layout.manifestKey(new_manifest), Retry::once()).has_value());
    for (const auto & [key, outcome] : outcomes)
        if (!key.starts_with("p/roots/victim/"))
            EXPECT_NE(key, "p/staging/victim/9/new.tmp");
}
```
  Expected failure on the base: `DrainStopsOnTheAdminLeaseFence`, `RollbackRunsOnTheControlPlaneAfterAFence`
  and `FenceLossOnAListPageStillRunsTheConfirm` (the base drains on the open plane: both objects deleted, no
  tombstone); `FailedRemountStillRunsTheConfirm` (no tombstone, no rollback); `PausedRunCannotUntombstone…`
  (the hook never fires before the base's final tombstone; the nested rerun then refuses the tombstone);
  `InFlightDeleteAfterRejoin…` (base deletes the slot; `member_retired` missing). The two guards pass on the
  base once they compile (`member_retired` → `slot_removed` on the base).

- [ ] **Step 2: run to verify they fail** (`--gtest_filter='CASDecommission.DrainStops*:CASDecommission.Rollback*:CASDecommission.FenceLoss*:CASDecommission.LongDrain*:CASDecommission.FencedAdmin*:CASDecommission.FailedRemount*:CASDecommission.PausedRun*:CASDecommission.InFlight*'`).

- [ ] **Step 3: implement.** `deleteListedPrefix`: the callback's catch block ends with
  `return op.admitted();` (one warning, then stop walking once the fence is gone) and the success path keeps
  `return true;`; its header comment: "runs on the admin's mount plane, so a fenced admin's deletes are
  refused locally; only attempts already sent can land late". `drainVictim`: every listing and delete uses
  `CasOperation op = admin.mountRequests().admit();` (one operation for the whole drain). `sweepNamespace`
  (`CasOrphanManifestSweep.cpp:564`): `CasOperation op = store.mountRequests().admit();` with the comment
  "the decommission drain runs under the admin's lease; a fenced admin must stop deleting". Also check its
  tests in `gtest_cas_orphan_manifest_sweep.cpp` still open writable pools (they do: `openPoolForTest`).

- [ ] **Step 4: run** `--gtest_filter='CASDecommission.*:CASDecommissionCatalogDuties.*:CASOrphanManifestSweep.*'`
  then the release `CAS*` gate. Expected: green.

- [ ] **Step 5: commit.**
```bash
git add -- $CA/Tools/CasDecommission.cpp $CA/Gc/CasOrphanManifestSweep.cpp src/Disks/tests/gtest_cas_decommission.cpp
git commit -m "Drain a decommissioned CAS member on the admin's fenced mount plane

The drain's listings and deletes are admitted under the admin's lease, so a
fenced admin stops deleting locally and only attempts already sent can land
late. The confirm and the rollback stay on the always-admitted control plane.

Signed-off-by: Mikhail Filimonov <mfilimonov@altinity.com>
Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 5: staging keys carry the writer epoch (D8)

**Files:**
- Modify: `$CA/Pool/CasPool.h` (new struct + method near `fenceGeneration`, `:510-513`), `$CA/Pool/CasPool.cpp`
- Modify: `$CA/ContentAddressedTransaction.cpp:899-931`
- Modify: `$CA/Pool/CasServerRoot.h:604-632`, `$CA/Pool/CasServerRoot.cpp:1824-1861` (`sweepOwnMountStaging`)
- Modify: `$CA/ContentAddressedMetadataStorage.cpp:892-897`
- Test: `src/Disks/tests/gtest_cas_s3_staging.cpp`

**Interfaces:**
- Produces:
```cpp
/// The live incarnation at one instant: the writer epoch and the fence generation it was admitted under.
struct MountIncarnation
{
    uint64_t writer_epoch = 0;
    uint64_t fence_generation = 0;
};
/// Throws the transient refusal when the fence moved between the two reads, so the pair is never mixed.
MountIncarnation Pool::captureMountIncarnation() const;
void Pool::setIncarnationCaptureHookForTest(std::function<void()> hook);

/// Deletes `<mount_staging_prefix><epoch>/<leaf>` with `epoch < live_writer_epoch` and flat
/// `<mount_staging_prefix><leaf>` keys (epoch 0); leaves every other path alone.
void sweepOwnMountStaging(IObjectStorage & object_storage, const String & mount_staging_prefix, uint64_t live_writer_epoch) noexcept;
```
- Consumes: nothing from Tasks 1–4.

- [ ] **Step 1: write the failing tests** (`gtest_cas_s3_staging.cpp`, next to `CASStagingSweeper.RemovesOnlyObjectsUnderGivenMountPrefix`,
  which gains the `live_writer_epoch` argument `1` and keeps its assertions):

```cpp
TEST(CASStagingSweeper, OldEpochSweepKeepsSuccessorStaging)
{
    auto storage = DB::Cas::tests::makeLocalObjectStorageForTest();
    const std::string root = storage->getCommonKeyPrefix();
    auto put = [&](const std::string & key)
    {
        auto buf = storage->writeObject(DB::StoredObject(key), DB::WriteMode::Rewrite);
        buf->write("x", 1);
        buf->finalize();
    };
    put(root + "/p/staging/m/1/a.tmp");
    put(root + "/p/staging/m/2/b.tmp");
    put(root + "/p/staging/m/3/c.tmp");

    DB::Cas::sweepOwnMountStaging(*storage, root + "/p/staging/m/", /*live_writer_epoch=*/3);

    EXPECT_FALSE(storage->exists(DB::StoredObject(root + "/p/staging/m/1/a.tmp")));
    EXPECT_FALSE(storage->exists(DB::StoredObject(root + "/p/staging/m/2/b.tmp")));
    EXPECT_TRUE(storage->exists(DB::StoredObject(root + "/p/staging/m/3/c.tmp"))) << "the live epoch's own staging";
}

TEST(CASStagingSweeper, FlatLegacyKeysAreSwept)
{
    auto storage = DB::Cas::tests::makeLocalObjectStorageForTest();
    const std::string root = storage->getCommonKeyPrefix();
    auto buf = storage->writeObject(DB::StoredObject(root + "/p/staging/m/legacy.tmp"), DB::WriteMode::Rewrite);
    buf->write("x", 1);
    buf->finalize();
    DB::Cas::sweepOwnMountStaging(*storage, root + "/p/staging/m/", /*live_writer_epoch=*/1);
    EXPECT_FALSE(storage->exists(DB::StoredObject(root + "/p/staging/m/legacy.tmp"))) << "a flat key counts as epoch 0";
}

TEST(CASStagingSweeper, NestedOtherMemberKeysAreKept)
{
    auto storage = DB::Cas::tests::makeLocalObjectStorageForTest();
    const std::string root = storage->getCommonKeyPrefix();
    auto put = [&](const std::string & key)
    {
        auto buf = storage->writeObject(DB::StoredObject(key), DB::WriteMode::Rewrite);
        buf->write("x", 1);
        buf->finalize();
    };
    put(root + "/p/staging/a/b/1/x.tmp");   /// member `a/b`, nested under `a`
    put(root + "/p/staging/a/1/own.tmp");
    DB::Cas::sweepOwnMountStaging(*storage, root + "/p/staging/a/", /*live_writer_epoch=*/5);
    EXPECT_TRUE(storage->exists(DB::StoredObject(root + "/p/staging/a/b/1/x.tmp"))) << "`b` is not an epoch";
    EXPECT_FALSE(storage->exists(DB::StoredObject(root + "/p/staging/a/1/own.tmp")));
}

TEST(CASStagingSweeper, DeeperPathsUnderAnEpochDirectoryAreKept)
{
    auto storage = DB::Cas::tests::makeLocalObjectStorageForTest();
    const std::string root = storage->getCommonKeyPrefix();
    auto buf = storage->writeObject(DB::StoredObject(root + "/p/staging/a/7/x/y.tmp"), DB::WriteMode::Rewrite);
    buf->write("x", 1);
    buf->finalize();
    DB::Cas::sweepOwnMountStaging(*storage, root + "/p/staging/a/", /*live_writer_epoch=*/9);
    EXPECT_TRUE(storage->exists(DB::StoredObject(root + "/p/staging/a/7/x/y.tmp"))) << "member `a/7`'s key, not `a`'s";
}

/// The staging key's epoch and the admission generation the finalize rechecks come from one capture: a
/// self-remount between the two reads refuses the write instead of staging new-generation bytes under the
/// old epoch. Without the hook, the key carries the live epoch.
TEST(ContentAddressedTransaction, StagingEpochMatchesAdmissionGeneration)
{
    auto object_storage = makeFakeNativeCopyStorage(/*native_only_copy_supported=*/true);
    auto metadata_storage = makeS3StagingMetadataStorageForTest(object_storage, "mountE");
    metadata_storage->startup();
    const DB::Cas::PoolPtr pool = metadata_storage->poolForTest();
    const uint64_t epoch_before = pool->liveWriterEpoch();
    const std::string path = "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin";

    {
        auto tx = metadata_storage->createTransaction();
        auto & ca_tx = dynamic_cast<DB::ContentAddressedTransaction &>(*tx);
        writeThroughS3Transaction(ca_tx, path, std::string(300, 'x'));
        DB::RelativePathsWithMetadata staged;
        object_storage->listObjects(metadata_storage->stagingKeyPrefix(), staged, /*max_keys=*/0);
        ASSERT_EQ(staged.size(), 1u);
        EXPECT_NE(staged.front()->relative_path.find("/" + std::to_string(epoch_before) + "/"), std::string::npos)
            << staged.front()->relative_path;
    }

    const String mount_key = pool->layout().mountKey("mountE");
    bool remounted = false;
    pool->setIncarnationCaptureHookForTest([&]
    {
        if (std::exchange(remounted, true))
            return;
        DB::Cas::tests::fenceOutMountForTest(*pool->poolBackendPtr(), mount_key);
        try { pool->renewWatermarkOnce(); } catch (const DB::Exception &) {}
        ASSERT_TRUE(pool->tryRemountOnce());
    });
    auto tx = metadata_storage->createTransaction();
    auto & ca_tx = dynamic_cast<DB::ContentAddressedTransaction &>(*tx);
    EXPECT_THROW(ca_tx.writeFile(path, 65536, DB::WriteMode::Rewrite, {}), DB::Exception);
    ASSERT_TRUE(remounted);
    EXPECT_EQ(pool->liveWriterEpoch(), epoch_before + 1);
    DB::RelativePathsWithMetadata staged;
    object_storage->listObjects(metadata_storage->stagingKeyPrefix() + "/" + std::to_string(epoch_before) + "/", staged, 0);
    EXPECT_TRUE(staged.empty()) << "nothing of the new generation was staged under the old epoch";
}
```
  (`fenceOutMountForTest` is the helper moved to `cas_test_helpers.h` in Task 3; if Task 3 kept it local,
  move it now — one definition.)

- [ ] **Step 2: run to verify they fail.** Expected: compile failure (`sweepOwnMountStaging` takes two
  arguments; `setIncarnationCaptureHookForTest` missing). Semantic reasons once the signatures exist:
  `OldEpochSweepKeepsSuccessorStaging` and `NestedOtherMemberKeysAreKept` (the base deletes everything under
  the prefix, the local listing is recursive); `StagingEpochMatchesAdmissionGeneration` (the base key has no
  epoch segment).

- [ ] **Step 3: implement `Pool::captureMountIncarnation`** (`CasPool.h` next to `fenceGeneration`;
  `CasPool.cpp` anywhere after `mountWritable`):
```cpp
MountIncarnation Pool::captureMountIncarnation() const
{
    /// Admission first, then the epoch, then the admission again: a remount publishes the new epoch
    /// before it bumps the generation, so a generation that held across the epoch read dates the epoch.
    const uint64_t generation = mount_requests.admit().generation();
    if (incarnation_capture_hook_for_test)
        incarnation_capture_hook_for_test();
    const uint64_t writer_epoch = liveWriterEpoch();
    checkFenceOrThrow(generation);
    return MountIncarnation{.writer_epoch = writer_epoch, .fence_generation = generation};
}
```
  with a private member `std::function<void()> incarnation_capture_hook_for_test;` and the setter.
  `ContentAddressedTransaction.cpp:899-931`: replace the key build and the generation capture with
```cpp
            const Cas::PoolPtr pool = metadata_storage.store();
            const Cas::MountIncarnation incarnation = pool->captureMountIncarnation();
            const std::string staging_key = metadata_storage.stagingKeyPrefix() + "/"
                + std::to_string(incarnation.writer_epoch) + "/" + getRandomASCIIString(32) + ".tmp";
```
  and the finalize callback `[pool, generation = incarnation.fence_generation] { pool->checkFenceOrThrow(generation); }`.
  Keep the comment about the finalize recheck; add one line: "the epoch in the key and the generation the
  finalize rechecks are one capture, so a remount between them cannot stage new-generation bytes under an old epoch".

- [ ] **Step 4: implement the sweep** (`CasServerRoot.cpp:1824`):
```cpp
namespace
{
/// The epoch a staging key under the mount's prefix belongs to: `<epoch>/<leaf>` parses, a flat `<leaf>`
/// is epoch 0 (written before keys carried one), anything else belongs to another member nested under this
/// prefix and is nobody's to sweep here.
std::optional<uint64_t> stagedEpochOf(std::string_view rest)
{
    const size_t slash = rest.find('/');
    if (slash == std::string_view::npos)
        return rest.empty() ? std::nullopt : std::optional<uint64_t>(0);
    const std::string_view dir = rest.substr(0, slash);
    const std::string_view leaf = rest.substr(slash + 1);
    if (dir.empty() || leaf.empty() || leaf.find('/') != std::string_view::npos)
        return std::nullopt;
    uint64_t epoch = 0;
    const auto [end, ec] = std::from_chars(dir.data(), dir.data() + dir.size(), epoch);
    if (ec != std::errc{} || end != dir.data() + dir.size())
        return std::nullopt;
    return epoch;
}
}

void sweepOwnMountStaging(IObjectStorage & object_storage, const String & mount_staging_prefix, uint64_t live_writer_epoch) noexcept
{
    try
    {
        RelativePathsWithMetadata children;
        object_storage.listObjects(mount_staging_prefix, children, /*max_keys=*/0);
        size_t removed = 0;
        for (const auto & child : children)
        {
            const String & key = child->relative_path;
            if (!key.starts_with(mount_staging_prefix))
                continue;
            const std::optional<uint64_t> epoch = stagedEpochOf(std::string_view(key).substr(mount_staging_prefix.size()));
            if (!epoch || *epoch >= live_writer_epoch)
                continue;   /// the live epoch's own in-flight uploads, or a nested member's keys
            try
            {
                object_storage.removeObjectIfExists(StoredObject(key));
                ++removed;
            }
            catch (...) // NOLINT(bugprone-empty-catch)
            {
                /// One stubborn key must not abort the sweep; a later mount retries it.
            }
        }
        if (removed)
            LOG_INFO(getLogger("CasStagingSweeper"), "Reclaimed {} staging object(s) below writer epoch {} under '{}' at mount start",
                     removed, live_writer_epoch, mount_staging_prefix);
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// A LIST failure at mount time must never fail the mount; the debris is bounded and retried later.
    }
}
```
  Update the header comment (`CasServerRoot.h:604-632`): keys are `staging/<srid>/<writer_epoch>/<random>.tmp`;
  the cutoff is the live epoch fixed at the start of the sweep; flat keys are epoch 0; the prefix rule stays.
  `ContentAddressedMetadataStorage.cpp:894`: pass `pool->liveWriterEpoch()` as the third argument.
  Add `#include <charconv>` to `CasServerRoot.cpp`.

- [ ] **Step 5: run** `--gtest_filter='CASStagingSweeper.*:ContentAddressedTransaction.*:CASS3Staging.*'`. Expected: green.

- [ ] **Step 6: commit.**
```bash
git add -- $CA/Pool/CasPool.h $CA/Pool/CasPool.cpp $CA/ContentAddressedTransaction.cpp \
  $CA/Pool/CasServerRoot.h $CA/Pool/CasServerRoot.cpp $CA/ContentAddressedMetadataStorage.cpp \
  src/Disks/tests/gtest_cas_s3_staging.cpp src/Disks/tests/cas_test_helpers.h
git commit -m "Put the writer epoch into CAS S3 staging keys and sweep only older epochs

A mount's startup sweep deleted every staging object under its prefix,
including a successor's in-flight uploads when two processes of one member
overlapped. Keys are now staging/<srid>/<writer_epoch>/<random>.tmp and the
sweep deletes only epochs below the live one; flat keys count as epoch 0 and
a nested member's keys are left alone. The epoch and the admission generation
the finalize rechecks come from one capture.

Signed-off-by: Mikhail Filimonov <mfilimonov@altinity.com>
Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
## Task 6: CAS-271 — log the skipped capability probe, fix the probe's text

**Files:**
- Modify: `$CA/Pool/CasPool.cpp:487-519` (the `skip_access_check` branch of `Pool::open`), `:904-921` (`openForDecommission`)
- Modify: `$CA/Backend/CasProbe.cpp:33-41`
- Test: `src/Disks/tests/gtest_cas_decommission.cpp`, `src/Disks/tests/gtest_cas_probe.cpp`

**Interfaces:**
- Produces (file-local in `CasPool.cpp`): `void logSkippedCapabilityProbe(const String & srid, const String & pool_prefix, std::string_view reason)`.

- [ ] **Step 1: write the failing tests.** `gtest_cas_decommission.cpp` (a log capture on the `CasPool`
  logger; copy `ScopedRenewalLogCapture` from `gtest_cas_mount.cpp:361-392` into `cas_test_helpers.h` as
  `ScopedLoggerCapture(const String & logger_name, const String & level)` and use it from both files):
```cpp
TEST(CASDecommission, OpenLogsTheSkippedProbe)
{
    auto backend = std::make_shared<InMemoryBackend>();
    { auto victim = openVictim(backend); }
    DB::Cas::tests::ScopedLoggerCapture capture("CasPool", "information");
    auto admin = Pool::openForDecommission(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "a1"}, "victim");
    const String log = capture.captured();
    EXPECT_NE(log.find("capability probe was skipped"), String::npos) << log;
    EXPECT_NE(log.find("versioning"), String::npos) << log;
    EXPECT_NE(log.find("conditional"), String::npos) << log;
    EXPECT_NE(log.find("list-after-write"), String::npos) << log;
}
```
  `gtest_cas_probe.cpp`: find the test that asserts the delete-marker message (grep `has no override` or
  `versioning delete marker`); change its expected substring to `does not make a versioned bucket usable` and
  add `EXPECT_EQ(message.find("has no override"), String::npos)`. If no test pins the text, add one that
  drives `runCapabilityProbe` over an `InMemoryBackend` with `setSimulateDeleteMarkers(true)` and checks both.

- [ ] **Step 2: run to verify they fail.** Expected: no such log line; the old text still says "has no override".

- [ ] **Step 3: implement.** `CasPool.cpp`, anonymous namespace:
```cpp
/// What a mount that skipped the capability battery did not prove about its store. Said once per open at
/// the default level, because nothing else tells an operator the checks did not run.
void logSkippedCapabilityProbe(const String & srid, const String & pool_prefix, std::string_view reason)
{
    LOG_INFO(getLogger("CasPool"),
        "CAS pool '{}' (prefix '{}'): the capability probe was skipped ({}); unverified on this mount: bucket "
        "versioning, conditional-write support, list-after-write visibility",
        srid, pool_prefix, reason);
}
```
  Call it in the `else` branch of `Pool::open` (`:505`) with `"skip_access_check"` and in `openForDecommission`
  right after `config.skip_access_check = true;` with `fmt::format("decommission of pool member '{}'", victim_srid)`.
  `CasProbe.cpp:39`: replace `This is NOT ignorable and has no override. Use a bucket where versioning was NEVER enabled`
  with `Use a bucket where versioning was NEVER enabled; skip_access_check skips this probe but does not make a versioned bucket usable`.

- [ ] **Step 4: run** `--gtest_filter='CASDecommission.OpenLogsTheSkippedProbe:CASProbe*'`. Expected: green.

- [ ] **Step 5: commit.**
```bash
git add -- $CA/Pool/CasPool.cpp $CA/Backend/CasProbe.cpp src/Disks/tests/gtest_cas_decommission.cpp \
  src/Disks/tests/gtest_cas_probe.cpp src/Disks/tests/cas_test_helpers.h src/Disks/tests/gtest_cas_mount.cpp
git commit -m "Log when a CAS mount skips the capability probe

skip_access_check and the decommission open skip the battery without a trace;
one line at the default level now names what went unverified. The probe's
versioning message no longer claims there is no override.

Signed-off-by: Mikhail Filimonov <mfilimonov@altinity.com>
Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 7: removals (#removed, PR B rows)

**Files:**
- Modify: `$CA/Pool/CasServerRoot.h:157-185` (`EpochMintPolicy`, `allocateWriterEpoch`), `$CA/Pool/CasServerRoot.cpp:429-431,598-708,989-1010,1045-1052,1145-1147`
- Modify: `$CA/Pool/CasPool.cpp:606-631,746-748,793-795,1437-1440`, `$CA/Pool/CasPool.h:420-433`
- Test: `src/Disks/tests/gtest_cas_mount.cpp`, `gtest_cas_ref_recovery_cas_walk.cpp`, `gtest_cas_ref_writer.cpp`

**Interfaces:**
- Produces: `uint64_t allocateWriterEpoch(CasOperation & op, const Layout & l, const String & srid, const ObserveRefCatalog & observe_catalog);`
  (no policy, no clock). Everything else keeps its signature.
- Consumes: Task 4a's D9 check (makes `DecommissionRecovery` unreachable).

- [ ] **Step 1: write the rewritten test** (`gtest_cas_mount.cpp`, replacing `CASHeartbeatFloor.UnseenSridPrunedFromObservationMap`):
```cpp
/// A slot that turns terminal leaves the observation map in the walk that sees it; slots never vanish, so
/// there is nothing else to prune.
TEST(CASHeartbeatFloor, TerminalSlotLeavesTheObservationMap)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    Ops ops(b);
    seedMount(ops.op, l, "s1", /*expires*/ 10, /*fenced*/ false, /*min_active_build_sequence*/ 0);
    seedMount(ops.op, l, "s2", /*expires*/ 10, /*fenced*/ false, /*min_active_build_sequence*/ 0);
    MountObservationMap obs;
    computeHeartbeatFloor(ops.op, l, kNowMs, frozenClock(0), kStableThresholdMs, obs);
    ASSERT_TRUE(obs.contains("s1"));
    ASSERT_TRUE(obs.contains("s2"));

    renewMount(ops.op, l, "s1");
    const auto got = ops.op.read(l.mountKey("s2"), Retry::standard());
    MountLease farewell = decodeMountLease(got->bytes);
    farewell.min_active_build_sequence = std::numeric_limits<uint64_t>::max();
    mustCommit(ops.op.replace(l.mountKey("s2"), encodeMountLease(farewell), got->etag, Retry::standard()), "s2 farewell");

    const HeartbeatFloor floor = computeHeartbeatFloor(ops.op, l, kNowMs, frozenClock(kStableThresholdMs), kStableThresholdMs, obs);
    EXPECT_EQ(floor.terminated, 1u);
    EXPECT_TRUE(obs.contains("s1"));
    EXPECT_FALSE(obs.contains("s2"));
}
```
  This is a **guard** (the base erases a terminal slot mid-walk too); it replaces the test whose premise
  (a vanished `/mount`) the removal retires.

- [ ] **Step 2: delete** `CASMount.DecommissionRemintOverTerminalMountMintsDistinctEpoch`,
  `CASMount.DecommissionRemintOverLiveMountRefuses`,
  `CASMountAwaitExpiry.PersistentSlotVanishPacesAndBoundsRestartsInsteadOfSpinning` and `AlwaysVanishesBackend`
  (`gtest_cas_mount.cpp`); update every `allocateWriterEpoch(…, EpochMintPolicy::NormalMount, 0, …)` call to
  `allocateWriterEpoch(op, l, srid, observe)` — 21 sites in `gtest_cas_mount.cpp`, one each in
  `gtest_cas_ref_recovery_cas_walk.cpp` and `gtest_cas_ref_writer.cpp` (`git grep -n 'EpochMintPolicy' -- src/Disks/tests`
  must return nothing afterwards).

- [ ] **Step 3: implement.** `CasServerRoot.h`: delete the `EpochMintPolicy` enum; the new declaration and a
  header comment whose absent-epoch branch reads: `KeyAbsent` → mint epoch 1; `Present` → `CORRUPTED_DATA`
  ("durable epoch state was lost while a mount is live or recently live; refusing to re-mint epoch 1. If no
  server uses this root, start with a new server_root_id; a decommission never recreates a counter");
  anything else → `CORRUPTED_DATA`. `CasServerRoot.cpp`: delete the forward declaration at `:429-431`,
  the `policy`/`now_ms` parameters, the `DecommissionRecovery` arm (`:649-676`) — the `Present` case throws
  unconditionally with the text above; `claimMountAwaitingExpiry` (`:993-1007`): `if (!got) return r;` with
  the comment "the slot is never deleted by this build; a read that finds nothing is the last result's own
  answer"; `computeHeartbeatFloor`: delete `seen_srids` (`:1045-1052`, `:1068`) and the final prune loop
  (`:1145-1147`); keep the `!observed` arm's `obs.erase`. `CasPool.cpp`: delete `epoch_policy` and the
  `now_ms` hoist comment (`:606-628`), the four calls become `allocateWriterEpoch(<op>, store->pool_layout, srid, observe_catalog)`
  (`:630`, `:747`, `:794`) and `allocateWriterEpoch(epoch_op, pool_layout, srid, observe_catalog)` (`:1439`).
  `CasPool.h:420-433` (`openForDecommission` comment): "Impersonates the victim's owner uuid (`readOwnerUuid`,
  or, when the owner was deleted by hand, the uuid of the surviving lease) and mounts writable under
  `MountClaimPolicy::NoWait`: a live victim lease is an immediate `ABORTED` refusal; a tombstoned owner of the
  victim is accepted; a member without an epoch counter is refused (`CORRUPTED_DATA`). Throws `BAD_ARGUMENTS`
  when neither owner nor lease exists." Also in `claimMount`'s `ForeignOwner` arm comment (`:811-812`) and the
  `same_epoch_occupied` arm: keep; `claimOwnerOrThrow`'s header was rewritten in Task 2.
  `git grep -n 'slot_removed\|DecommissionRecovery\|EpochMintPolicy\|seen_srids' -- src programs docs` must
  return nothing except `design-history.md`.

- [ ] **Step 4: run** the release `CAS*` gate and `cmake .` is not needed (no new files). Expected: green,
  count = baseline − 3 deleted + the tests added in Tasks 1–6 (record the number in `$WT/tmp/gate_t7.txt`).

- [ ] **Step 5: commit.**
```bash
git add -- $CA/Pool/CasServerRoot.h $CA/Pool/CasServerRoot.cpp $CA/Pool/CasPool.h $CA/Pool/CasPool.cpp \
  src/Disks/tests/gtest_cas_mount.cpp src/Disks/tests/gtest_cas_ref_recovery_cas_walk.cpp src/Disks/tests/gtest_cas_ref_writer.cpp
git commit -m "Remove the CAS epoch re-mint policy and the vanished-slot handling

A decommission never mints over an absent counter any more, so the
DecommissionRecovery branch of allocateWriterEpoch is unreachable; mount
slots are never deleted, so the observation loop's vanished-slot pacing and
the heartbeat floor's prune of unseen server roots have nothing to do.

Signed-off-by: Mikhail Filimonov <mfilimonov@altinity.com>
Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 8: docs, release note, integration test

**Files:**
- Modify: `docs/en/antalya/cas/architecture/mounts-and-leases.md:41-56,120-129,185-204,261-266,274-279`
- Modify: `docs/en/antalya/cas/configuration.md:135-138`, `docs/en/antalya/cas/operations/debugging.md:143-146`,
  `docs/en/antalya/cas/architecture/backend.md:124-129`, `docs/en/antalya/cas/operations/migration.md:167-213`,
  `docs/en/antalya/cas/operations/troubleshooting.md:24`
- Modify: `tests/integration/test_cas_drop_pool_member/test.py:163-312`

- [ ] **Step 1: `mounts-and-leases.md`.**
  - `#owner-claim` table: the retired row becomes `| present, retired_at_ms set | CORRUPTED_DATA for a server start or remount; the decommission admin accepts its own victim's tombstone |`;
    add after the table: "The owner carries `owner_life`, a counter every owner write sets to the read value
    + 1 by `If-Match`, so two owner writes never repeat their bytes and a delayed `If-Match` against an
    older incarnation cannot land. A process records the life it started under; after every mount claim
    (start, self-remount, decommission) it reads the owner once more and requires its own `server_uuid`
    and that life (`recheckOwnerAfterClaim`); otherwise it fences its own mount body and fails, so a
    successor does not wait the refused lease out. `retirement_verified` marks a decommission whose claim,
    tombstone and farewell all held."
  - `:127`: delete "and by the non-authoritative decommission epoch-recovery precheck".
  - State diagram: replace `Live --> Absent: decommission tail, mount then epoch then owner tombstone` with
    `Terminated --> Terminated: decommission farewell, owner tombstoned and verified`; the counter table row
    for `writer_epoch` gains "never deleted: a retired `server_root_id` keeps its counter".
  - `#mount-lifecycle` "Permanent removal" paragraph:
    "**Permanent removal** of a dead replica (`Cas::decommissionPoolMember`, `SYSTEM CAS DROP POOL MEMBER '<server_root_id>' FROM DISK '<disk>'`)
    keeps `owner`, `epoch` and `mount` and removes only data, in six steps: (1) claim the victim's slot as an
    administrative writer with a no-wait policy (a live member is refused; a member without an epoch counter
    is refused); (2) tombstone the owner at the next `owner_life`; (3) confirm the slot still holds the
    admin's claim, else roll the tombstone back unless an earlier run verified the retirement; (4) drop every
    namespace, sweep manifest debris, drain `staging/` and `roots/`, all on the admin's fenced mount plane;
    (5) write the admin's clean farewell; (6) read the farewell and the tombstone back and set
    `retirement_verified`. Every exit between (2) and (5) runs (3). Catalog rows GC still has to delete are
    reported as `namespaces_pending_gc`, not as a failure; the operator reruns only after a run that reported
    `member_retired = 0`."
  - `#mounts-table`: no change in this PR (`retired` is PR C).
- [ ] **Step 2: `configuration.md:137`**: delete "and by the non-authoritative decommission epoch-recovery
  precheck". **`debugging.md:145`**: `vanished` arm: replace "or a decommission raced the renewal" with
  "a decommission never deletes a mount slot". **`backend.md:128`**: after "with the battery skipped" add
  "and logs one line naming what went unverified (versioning, conditional writes, list-after-write)".
  **`troubleshooting.md:24`**: `| SYSTEM CAS DROP POOL MEMBER returns member_retired = 0 or a non-empty warnings column | The run stopped before its verified mark: a drain step failed, the admin lost its claim, or the farewell could not be confirmed; the owner keeps an unverified tombstone as the resume anchor | Rerun the same command; it resumes over its own tombstone. namespaces_pending_gc alone is not a reason to rerun |`.
- [ ] **Step 3: `migration.md#decommission`.** The second paragraph: "claims the victim's mount slot as an
  administrative writer (refusing immediately if the member is still alive), tombstones its owner, drops
  every table namespace the member owned, sweeps manifest debris, drains its staging and mountpoint objects,
  writes its farewell and marks the retirement verified. The member's control objects (`owner`, `epoch`,
  `mount`) are kept: the epoch counter is what keeps a returning server from reusing an epoch." The columns
  sentence: `namespaces_pending_gc` after `namespaces_already_removed`, `member_retired` instead of
  `slot_removed`. The resumability sentences: "A per-object drain failure is recorded as a `warning`; the
  owner keeps an unverified tombstone and the next run resumes over it. `member_retired = 1` means this run's
  claim, tombstone and farewell all held; rows still listed under `namespaces_pending_gc` are deleted by GC
  and need no rerun." **Verification**: "After `member_retired = 1`, `system.cas_mounts` shows the member's
  row as `terminated`; a `SYSTEM CAS GC RUN` no longer waits on its heartbeat." Add:

```markdown
### Upgrading and downgrading {#decommission-compatibility}

| Case | Behaviour |
|---|---|
| A `server_root_id` retired by 26.6.4 (its `epoch` and `mount` deleted) | not repaired: a decommission rerun refuses the absent counter; use a new `server_root_id` |
| A 26.6.4 decommission that stopped midway | the same refusal; use a new `server_root_id` |
| Flat 26.6.4 staging keys `staging/<srid>/<name>.tmp` | swept as epoch 0 at the next mount |
| Downgrade to 26.6.4 with a retired `server_root_id` kept by this version | 26.6.4 refuses to start it and its GC treats the farewell as terminal |
| Downgrade: `owner_life`, `retirement_verified`, epoch staging keys | skipped by 26.6.4 readers; staging swept recursively |
| Mixed versions: a 26.6.4 decommission tool rewrites an owner | the life drops to 0; servers of this version fail their owner check and stop (fail-closed) |
| Mixed versions: a 26.6.4 member restarts during a decommission | its owner check refuses after the tombstone; before it there is no recheck, as in 26.6.4 |

Rolling upgrades keep the old races on the old binaries; decommission from an upgraded server.
```
- [ ] **Step 4: integration test.** In `test.py`: case (6) comment and asserts: 11 fields; `fields[0] == SRID2`;
  `int(fields[1]) >= 1`; `int(fields[3]) >= 1` (`namespaces_pending_gc`, index after `namespaces_already_removed`);
  `int(fields[9]) == 1` (`member_retired`) and `fields[10] == ""` — the first run is already verified. Case (6b):
  loop `GC RUN` + rerun until `int(fields[3]) == 0`, asserting `int(fields[9]) == 1` on every rerun. Case (8):
  `SELECT state FROM system.cas_mounts WHERE server_root_id = '<SRID2>' AND disk = '<CA_DISK>'` equals
  `terminated`. Case (10): the rerun succeeds: 11 fields, `int(fields[9]) == 1`, `fields[10] == ""`.
  Run: `python -m ci.praktika run "integration" --test test_cas_drop_pool_member` from `$WT`, output to
  `$WT/build/test_integration_dpm.log`; a subagent reads it.
- [ ] **Step 5: commit.**
```bash
git add -- docs/en/antalya/cas/architecture/mounts-and-leases.md docs/en/antalya/cas/configuration.md \
  docs/en/antalya/cas/operations/debugging.md docs/en/antalya/cas/architecture/backend.md \
  docs/en/antalya/cas/operations/migration.md docs/en/antalya/cas/operations/troubleshooting.md \
  tests/integration/test_cas_drop_pool_member/test.py
git commit -m "Document the CAS member lifecycle: decommission keeps the control objects

Signed-off-by: Mikhail Filimonov <mfilimonov@altinity.com>
Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 9: gates and the pull request

- [ ] Release gate: `CAS*` in `$WT/build`; ASan gate: build `unit_tests_dbms` in `$WT/build_asan` (same
  `flock`, log `build_asan/build_gate.log`) and run `CAS*` with `TMPDIR=$WT/build_asan/tmp`. Both green; the
  real-time tests (`LongDrainOutlivesOneTtl`, `FailedRemountStillRunsTheConfirm`) pass under ASan or their
  TTL/period are raised (TTL 3 s, period 500 ms, delete 1 s) in one follow-up commit.
- [ ] `git grep -n 'slot_removed' -- src programs docs tests` → only `design-history.md`.
- [ ] `git grep -n -E 'plan|BACKLOG|CAS-[0-9]+|review|finding' -- $CA/Tools/CasDecommission.cpp $CA/Pool/CasServerRoot.cpp $CA/Pool/CasPool.cpp | grep -v 'explain'`
  → every hit is a word in ordinary prose, not a reference.
- [ ] PR body from `.github/PULL_REQUEST_TEMPLATE.md`, category "Improvement", changelog entry: "`SYSTEM CAS DROP POOL MEMBER`
  keeps the member's `owner`, `epoch` and `mount` objects and tombstones the owner before draining; the result
  column `slot_removed` is renamed `member_retired` and `namespaces_pending_gc` is added. Every CAS mount
  re-reads its owner after claiming its slot. S3 staging keys carry the writer epoch." Links:
  `Closes:` the GitHub issues for CAS-363, CAS-158, CAS-156, CAS-157, CAS-271 if they exist as issues (else
  `Related:` the backlog task ids in the PR text only). Do not push (the owner pushes).

---

## Self-review

**Spec coverage.** D1 (Task 4a: no delete anywhere; `KeepsEpochAndFarewellMount`). D2 (4a tombstone right
after the claim, 4b mark and refresh, 4a/4c rollback by own ETag and the verified exception, exits by
exception and by warnings). D3 (Task 3, both claim paths; the life compared on remount). D4 (Task 1 format, Task
2 writes life 1, every writer in 4a/4b bumps it). D5 (admin keeps `background_watermark = true` — unchanged;
`FencedAdminFailsInsteadOfRemounting`, `FailedRemountStillRunsTheConfirm`). D8 (Task 5). D9 (4a `AbsentCounterIsRefused`,
Task 7 deletes the policy). #claim-recheck step 1 (self-fence `gc_fenced`, Task 3) and step 2 (three refusal
texts). #decommission steps 1–6 and "deleted from the tail" (4a). #removed rows 1–5 (Tasks 4a, 7) and row 6
(Task 8). `member_retired` rename (4a, R3). CAS-271 (Task 6). Release note (Task 8, R8). #tests «PR B»: every
listed name has a step except `CASMount.ForeignReclaimReadsTheOwnerFirst` (R2, PR C); integration cases (8) and
(10) (Task 8). Not covered by design: #rejoin, `system.cas_mounts` `retired`, PR A items.

**Placeholder scan.** No "TBD"/"similar to"; every test step carries its body; Task 4a's function body calls
`verifyAndMark`, which Task 4b's Step 3 defines in full, and the two tasks are built as one.

**Type consistency.** `claimOwnerOrThrow(op, l, srid, uuid, observe, policy) -> uint64_t` (Task 2) is what
Task 3's `mountWritable` wiring and Task 7's call-site sweep use. `OwnerExpectation{server_uuid, owner_life, retired}`
(Task 3) matches both wirings. `Tombstone{life, previously_verified, etag}` (4a) is what 4b's `verifyAndMark`
and 4c's tests rely on (`owner_life` 2 → 3 → …). `adminClaimStillHeld(op, layout, srid, uuid, epoch)` is
declared in 4a and used by the 4a guard test and 4b. `DecommissionReport::member_retired`/`namespaces_pending_gc`
are named identically in 4a, the interpreter, the CLI and the integration test. `sweepOwnMountStaging(storage, prefix, live_epoch)`
and `MountIncarnation{writer_epoch, fence_generation}` (Task 5) match their tests. `fenceOutMountForTest`
lives in `cas_test_helpers.h` from Task 3 on (Task 5 uses it); `tombstoneOwner`, `readOwner`, `readSlot`,
`fenceSlot` are the decommission file's helpers (Task 2/4a).

**Review Focus.** Each of the five lines has its test in the named task (Tasks 1, 2, 3, 3, 4c, 5).
