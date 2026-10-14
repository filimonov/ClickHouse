# CAS member lifecycle PR C: a retired member rejoins at start — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** a server started with a `server_root_id` whose decommission is verified and drained rejoins the
pool as an empty member, with the same or a new `server_uuid`, inside `claimOwnerOrThrow`; `claimMount`
reclaims another uuid's terminal body once the owner reads as the claimer's own life; `system.cas_mounts`
shows `retired`. No new command, no new setting.

**Architecture:** one new `RetiredOwnerPolicy` value, `RejoinIfVerified`, passed only by `Pool::open`.
Over a retired owner it runs `rejoinRetiredOwner`: verified mark, `epoch` and `mount` present, slot terminal,
the old life's access probe deleted, subtree empty, then (new uuid only) the terminal `mount` body rewritten
under the new uuid, then the owner rewritten `{our uuid, owner_life + 1}` by `If-Match`. A lost `If-Match`
re-decides from a fresh owner read. `claimMount` gains an optional `OwnerExpectation`; with it, a terminal
body of another uuid below our epoch is reclaimed after one `GET owner`. The owner check shared with
`recheckOwnerAfterClaim` moves into one helper, `ownerRefusal`. `listMounts` reads the owner of terminal and
fenced rows.

**Tech stack:** C++23, gtest (`src/Disks/tests`), `InMemoryBackend` hooks (`onBeforeWrite`,
`failNextWriteWith`), `OperationForTest`, pytest integration (`tests/integration`, RustFS).

**Spec:** `/home/mfilimonov/workspace/ClickHouse/master/docs/superpowers/specs/2026-10-13-cas-member-lifecycle-design.md`
(revision 10): D6, `#rejoin` (including the `claimMount` change), `#operator` (`retired`), `#delivery` row C,
`#tests` «PR C», `#compat` rows on rejoin and downgrade, `#checks` (old table definitions attach empty).
Model: `docs/superpowers/models/CaEpochAllocCore_RESULTS.md` rounds 8–10 (`r8_newuuid`, `r9_newuuid`,
`r10_newuuid`, `sab_r8_foreignnoowner`, `sab_r8_ownerfirst`, `sab_r9_rejoinunverified`).

**Base:** PR B, `fix/antalya-26.6/cas-363-member-lifecycle` @ `81e5e081ea5`. Line numbers below are at
`81e5e081ea5`; every edit is located by its symbol first. Later PR B commits are merged in with `git merge`
(never rebase).

**Branch:** `fix/antalya-26.6/cas-member-lifecycle-pr-c`, created from `81e5e081ea5` in the executor's own
worktree `$WT` (never `cas-janitor-budget`, never the master worktree). `$CA` =
`src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed`.

**Files outside `$CA`:** `src/Disks/tests/*` (tests), `docs/en/antalya/cas/**`,
`docs/en/operations/system-tables/cas_mounts.md`, `docs/en/sql-reference/statements/system.md`,
`tests/integration/test_cas_drop_pool_member/test.py`. No other upstream file.

## Global Constraints

- C++: Allman braces (opening brace on its own line). Comments explain why, never what; no references to
  plans, specs, reviews, backlog ids, finding ids, model row names or history in code or comments. No
  fallback paths: an error propagates; a refused rejoin writes nothing it cannot stand behind.
- `LOGICAL_ERROR` expectations are death tests (`#ifndef DEBUG_OR_SANITIZER_BUILD` throw arm, `#else`
  `EXPECT_DEATH` in a `Cas*DeathTest` suite, child exits with `std::_Exit`, `<base/defines.h>` included
  explicitly, both arms proven with `--gtest_list_tests` on release and ASan). No test in this plan expects
  one: every refusal here is `CORRUPTED_DATA` or `ABORTED`.
- Failing-first: every new test fails on the state before its task's implementation step for the reason its
  step names, or is marked **guard**; a guard whose task makes it load-bearing gets a sabotage step (remove
  the guarded clause, see the test fail, restore).
- Build: `cd $WT/build && flock /home/mfilimonov/workspace/ClickHouse/master/tmp/ninja.lock nice -n 10 ninja unit_tests_dbms > build_<task>.log 2>&1; echo NINJA_EXIT=$? >> build_<task>.log`
  (no `-j`; a fresh log name per run, e.g. `build_t2_3.log`; read `NINJA_EXIT=` before trusting a test run).
- Tests: `TMPDIR=$WT/build/tmp $WT/build/src/unit_tests_dbms --gtest_filter='<names>' > $WT/build/test_<task>.log 2>&1`.
  Gate: `--gtest_filter='CAS*'`, exactly that filter, release (`$WT/build`) and ASan (`$WT/build_asan`). A
  subagent reads each log and returns a short summary.
- Every new gtest lives in a `CAS*` suite.
- Commits: exact paths (`git add -- <path> …`), never a directory, never `-A`; no rebase, amend, stash or push.
  Every commit message ends with exactly these two trailers and no others:
  `Signed-off-by: Mikhail Filimonov <mfilimonov@altinity.com>` then
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. No session links.
- Scope: no new command, no new setting, no new `ErrorCodes`, no new `ProfileEvents`. Prefer deleting or
  reusing code over adding machinery.
- On-store format: frozen since 26.6.4. This PR adds no field and no key; `$CA/Formats/README.md` is untouched.
- Legacy 26.6.4 states (deleted `epoch`/`mount`, tombstones without `retirement_verified`) are refused and
  documented, never repaired.
- Docs under `docs/en`: every heading carries `{#anchor}`; code names in backticks; functions as `f`, not
  `f()`; tech-prose style (`/home/mfilimonov/.claude/skills/tech-prose/STYLE.md`: point first, plain words,
  short sentences, no history, no hype).
- Not in this PR: anything of PR A (`fix/antalya-26.6/cas-member-lifecycle-pr-a`, #2566, not merged and not in
  the base); fsck (CAS-357); multipart abort at mount (CAS-369.1); binding reruns to `(uuid, owner_life)`.

## Review Focus

Inputs the spec implies but its PR C test list does not exercise, most likely to bite first; each names the
test added for it and the owning task.

1. **A decommission rerun claims the slot while a new-uuid rejoin is between its reads and its mount write.**
   The rejoin's `If-Match` must fail, the re-decision must refuse "still running", and the owner must stay
   the verified tombstone. `CASRejoin.DecommissionRerunDuringARejoinWins` (Task 3).
2. **`roots/<srid>/` holds the old life's access probe and another object.** Only the probe is deleted; the
   other object refuses the rejoin and survives. `CASRejoin.DeletesTheOldAccessProbeAndKeepsOtherRootsObjects`
   (Task 2).
3. **A same-uuid start over a slot left by a losing racing rejoiner of another uuid.** The start must rejoin
   and reclaim that terminal body through the owner read, not wedge on `ForeignOwner`.
   `CASRejoin.SameUuidRejoinsOverAForeignTerminalBody` (Task 2).
4. **A remount (not a start) over a verified retirement.** It must fail and never rejoin.
   `CASRejoin.RemountDoesNotRejoin` (Task 2, guard).
5. **The decommission admin over a slot holding another uuid's terminal body.** The admin's owner is
   retired by design, so the foreign reclaim must not apply to it; the run refuses and writes nothing.
   `CASDecommission.AdminDoesNotReclaimAForeignTerminalBody` (Task 1, guard).

## Rulings (spec ambiguities resolved here; executors follow these)

- **R1 Which caller rejoins.** `RetiredOwnerPolicy::RejoinIfVerified` is passed only by
  `Pool::mountWritable` under `MountClaimPolicy::WaitForExpiry` (that is, `Pool::open`). The remount keeps the
  default `Refuse`, the admin `AcceptForDecommission`. The recheck after a rejoin uses `Refuse`.
  `recheckOwnerAfterClaim`'s retired test becomes `!= AcceptForDecommission`, so no third value ever accepts
  a retired owner there.
- **R2 The admin never takes the foreign reclaim.** The spec requires the owner to read "not retired"; the
  admin's victim owner is retired. `openForDecommission` passes no expectation, so a foreign terminal body
  refuses decommission ("alive or contended") until a start rejoins the id.
- **R3 Foreign body at or above our epoch stays `ForeignOwner`.** The spec places the reclaim "after the
  `newer_epoch` check"; that check is same-uuid only and throws. A foreign body is reclaimable only when
  `writer_epoch < our_epoch`; otherwise `ForeignOwner`, no write.
- **R4 Refusal codes and texts.** Unverified tombstone: `CORRUPTED_DATA` from `throwIfOwnerRetired`, text
  contains `decommission is not finished`. Verified, `epoch` or `mount` missing: `CORRUPTED_DATA`, text
  contains `cannot rejoin` and `new <cas_server_root_id>`. Verified, slot live-looking: `ABORTED`, text
  contains `still running`. Verified, subtree not empty: `ABORTED`, text contains `previous life remains`.
  `throwIfOwnerRetired` over a verified owner (a remount) says the process stops and a restart rejoins.
- **R5 "New uuid" means the starter's uuid differs from the retired owner's** (the model's `JoinMount`), not
  from the slot body's.
- **R6 The rewritten mount body** keeps every field of the terminal body except `server_uuid`; it clears
  `gc_fenced`, sets the clean marker (`min_active_build_sequence = UINT64_MAX`) and bumps `seq`, as the GC
  fence and `fenceOwnClaim` do.
- **R7 Access-probe key** = `roots/<srid>/clickhouse_access_check_<uuid>`, the uuid of the retired owner in
  `IDisk::checkAccess`'s dashed lowercase form (`8-4-4-4-12`). Deleted by `If-Match` on its `HEAD` ETag; any
  `Removal` outcome then goes to the emptiness check; transport errors propagate.
- **R8 Bounded re-decision:** at most 4 owner decisions per claim, then `ABORTED` (`kept changing`).
- **R9 `retired`** is shown for a `terminated` or `fenced` row whose owner has `retired_at_ms`, verified or
  not (spec #operator). An owner read error propagates, as the mount read's does.
- **R10 Test placement.** `CASRejoin.*` tests live in `gtest_cas_decommission.cpp`: they need its
  decommission fixtures (`makeTableWithRefs`, `readOwner`, `readSlot`, `fenceSlot`, `FailingDeleteBackend`).
  Spec test names are kept. Added beyond the spec list: Review Focus tests 1–5,
  `CASMount.ForeignReclaimReadsTheOwnerFirst` (spec PR B list, moved here by PR B's ruling R2),
  `CASMount.ForeignTerminalBodyAtOrAboveOurEpochStaysForeign`,
  `CASMount.ClaimPathsReclaimAForeignTerminalBodyOfTheirOwnOwner`, `CASListMounts.TerminalSlotOfATombstonedOwnerIsRetired`.
- **R11 `CASMount.PoolOpenNeverAcceptsARetiredOwner` loses its verified arm** and becomes
  `CASMount.PoolOpenRefusesAnUnverifiedRetirement`. The verified arm pinned PR B's interim behaviour; the
  verified case is now `CASRejoin.*`.
- **R12 PR A text conflict.** PR A rewrites `throwIfOwnerRetired` and the foreign-owner text in
  `claimOwnerOrThrow`; neither change is in this base. PR C rewrites `throwIfOwnerRetired` from PR B's text and
  appends one sentence to the foreign-owner text. Whichever PR merges second resolves: keep PR C's
  `throwIfOwnerRetired`; keep PR A's foreign-owner text, drop its "starting this server under a new
  `<cas_server_root_id>`" clause, add PR C's sentence.
- **R13 Integration.** A new test function retires `node2` itself first, so it does not depend on the order
  of the module's tests. Case (8) of the existing test expects `retired`.
- **R14 Soak (CAS-213)** is acceptance on the soak stand after the PR is built; it is listed in Task 6 as a
  hand-off item, not run by the implementer.

## File structure

| File | Responsibility after this PR |
|---|---|
| `$CA/Pool/CasServerRoot.h` | `RetiredOwnerPolicy::RejoinIfVerified`; `claimOwnerOrThrow` and `claimMount` contracts; `claimMount`/`claimMountAwaitingExpiry` take `foreign_reclaim_owner`; `MountInfo` state list |
| `$CA/Pool/CasServerRoot.cpp` | `ownerRefusal`, the foreign arm of `claimMount`, `rejoinRetiredOwner`, `accessProbeKey`, the decision loop in `claimOwnerOrThrow`, `throwIfOwnerRetired` texts, `listMounts` `retired` |
| `$CA/Pool/CasPool.{h,cpp}` | `mountWritable` passes `RejoinIfVerified` and the foreign expectation; `tryRemountOnce` passes its expectation; `open` doc |
| `src/Disks/tests/gtest_cas_mount.cpp` | `CASMount.*` foreign reclaim, `CASListMounts.*`, edited `PoolOpen…` and `ConflictReread…` tests |
| `src/Disks/tests/gtest_cas_decommission.cpp` | `CASRejoin.*`, `CASDecommission.AdminDoesNotReclaimAForeignTerminalBody` |
| `docs/en/antalya/cas/architecture/mounts-and-leases.md`, `operations/migration.md`, `operations/troubleshooting.md`, `docs/en/operations/system-tables/cas_mounts.md`, `docs/en/sql-reference/statements/system.md` | operator flow: re-add = restart the server |
| `tests/integration/test_cas_drop_pool_member/test.py` | case (8) `retired`; `test_retired_member_rejoins_empty` |

## Setup

- [ ] `git -C /home/mfilimonov/workspace/ClickHouse/cas-janitor-budget branch --contains 81e5e081ea5 | grep cas-363-member-lifecycle`
      (the base exists). Create `$WT` with the `create-worktree` skill (`create_subtree_branch.sh`), branch
      `fix/antalya-26.6/cas-member-lifecycle-pr-c` from `81e5e081ea5`. Run `cmake .` in `$WT/build` and
      `$WT/build_asan`.
- [ ] Baseline: build `unit_tests_dbms` (release), run the `CAS*` gate, record the pass count and any red in
      `$WT/tmp/baseline.txt`. A red baseline stops the plan: report it.

---

## Task 1: `claimMount` reclaims another uuid's terminal body after reading the owner (D6, second half)

**Files:**
- Modify: `$CA/Pool/CasServerRoot.h` (`claimMount` and `claimMountAwaitingExpiry` declarations and their
  comments, `:224-304`, `:315-361`)
- Modify: `$CA/Pool/CasServerRoot.cpp` (`claimMount` `:769-891`, `recheckOwnerAfterClaim` `:921-956`,
  `claimMountAwaitingExpiry` `:996-1066`)
- Modify: `$CA/Pool/CasPool.cpp` (`Pool::mountWritable` claim calls `:701-736`; `Pool::tryRemountOnce`
  `claimMountAwaitingExpiry` call `:1501-1510`)
- Test: `src/Disks/tests/gtest_cas_mount.cpp` (after `CASMount.RecheckForDecommissionStillRefusesForeignOwnerAndMovedLife`),
  `src/Disks/tests/gtest_cas_decommission.cpp` (after `CASDecommission.OpenLogsTheSkippedProbe`, end of file)

**Interfaces:**
- Consumes: `OwnerExpectation{server_uuid, owner_life, retired}`, `readOwnerObject`, `fenceOwnClaim` (PR B).
- Produces:
  - `std::optional<String> ownerRefusal(const std::optional<OwnerObject> & owner, const String & srid, const OwnerExpectation & expected)`
    (file-local in `CasServerRoot.cpp`; nullopt = the owner matches).
  - `claimMount(..., const std::optional<Etag> & unsafe_reclaim_authorization = {}, const std::optional<OwnerExpectation> & foreign_reclaim_owner = {})`.
  - `claimMountAwaitingExpiry(..., const CasEventSink & sink = {}, const std::optional<OwnerExpectation> & foreign_reclaim_owner = {})`.
  - Behaviour: a body of another `server_uuid` that is terminal (`gc_fenced` or clean) with
    `writer_epoch < our_epoch` is reclaimed (`prior = Fenced` or `Clean`) when `foreign_reclaim_owner` is set
    and the owner read now matches it; every other foreign body is `ForeignOwner`, no write.

- [ ] **Step 1: write the failing tests in `gtest_cas_mount.cpp`.** Append after
  `CASMount.RecheckForDecommissionStillRefusesForeignOwnerAndMovedLife` (it closes the block that defines
  `seedOwner` and `liveBody`):

```cpp
namespace
{
MountLease terminalBody(UInt128 uuid, uint64_t epoch, bool fenced)
{
    MountLease body = liveBody(uuid, epoch);
    if (fenced)
        body.gc_fenced = true;
    else
        body.min_active_build_sequence = std::numeric_limits<uint64_t>::max();
    return body;
}

const OwnerExpectation uuid_one_at_life_two{.server_uuid = UInt128(1), .owner_life = 2, .retired = RetiredOwnerPolicy::Refuse};

OwnerObject liveOwner(UInt128 uuid, uint64_t life)
{
    return OwnerObject{.server_uuid = uuid, .retired_at_ms = std::nullopt, .owner_life = life, .retirement_verified = false};
}

/// The slot's body with another `server_uuid`, everything else kept: what a rejoin under a new uuid leaves.
void rewriteSlotUuid(Backend & backend, const Layout & l, const String & srid, UInt128 uuid)
{
    DB::Cas::tests::OperationForTest op(backend);
    const auto got = (*op).read(l.mountKey(srid), Retry::standard());
    ASSERT_TRUE(got.has_value());
    MountLease m = decodeMountLease(got->bytes);
    m.server_uuid = uuid;
    m.seq += 1;
    ASSERT_TRUE(std::holds_alternative<Committed>(
        (*op).replace(l.mountKey(srid), encodeMountLease(m), got->etag, Retry::standard())));
}
}

/// After a rejoin under a new uuid the slot can hold another uuid's farewell or self-fence; the server
/// whose owner life it is takes it over.
TEST(CASMount, ForeignTerminalBodyIsReclaimable)
{
    for (const bool fenced : {false, true})
    {
        SCOPED_TRACE(fenced ? "gc_fenced" : "clean farewell");
        auto b = std::make_shared<InMemoryBackend>();
        Layout l("p");
        Ops ops(b);
        seedOwner(ops, l, liveOwner(UInt128(1), 2));
        mustCommit(ops.op.create(l.mountKey("r"), encodeMountLease(terminalBody(UInt128(2), 7, fenced)), Retry::standard()), "slot");
        const MountClaimResult claim = claimMount(ops.op, l, "r", UInt128(1), 8, 1000, 100, {}, {}, {}, uuid_one_at_life_two);
        ASSERT_EQ(claim.kind, MountClaimResult::Claimed);
        EXPECT_EQ(claim.prior, fenced ? MountPriorState::Fenced : MountPriorState::Clean);
        const MountLease slot = slotBody(*b, l, "r");
        EXPECT_EQ(slot.server_uuid, UInt128(1));
        EXPECT_EQ(slot.writer_epoch, 8u);
    }
}

/// The owner is read before the reclaim: a reclaim that a later recheck refuses would leave an old-uuid
/// body that a 26.6.4 starter (no recheck) could take.
TEST(CASMount, ForeignReclaimReadsTheOwnerFirst)
{
    struct Case
    {
        String name;
        std::optional<OwnerObject> owner;
        std::optional<OwnerExpectation> expected;
    };
    OwnerObject retired = liveOwner(UInt128(1), 2);
    retired.retired_at_ms = 5;
    const std::vector<Case> cases{
        {"owner of another uuid", liveOwner(UInt128(3), 2), uuid_one_at_life_two},
        {"owner retired", retired, uuid_one_at_life_two},
        {"owner at another life", liveOwner(UInt128(1), 3), uuid_one_at_life_two},
        {"owner absent", std::nullopt, uuid_one_at_life_two},
        {"no expectation passed", liveOwner(UInt128(1), 2), std::nullopt}};
    for (const Case & c : cases)
    {
        SCOPED_TRACE(c.name);
        auto b = std::make_shared<InMemoryBackend>();
        Layout l("p");
        Ops ops(b);
        if (c.owner)
            seedOwner(ops, l, *c.owner);
        mustCommit(ops.op.create(l.mountKey("r"), encodeMountLease(terminalBody(UInt128(2), 7, false)), Retry::standard()), "slot");
        const auto before = ops.op.read(l.mountKey("r"), Retry::standard());
        EXPECT_EQ(claimMount(ops.op, l, "r", UInt128(1), 8, 1000, 100, {}, {}, {}, c.expected).kind, MountClaimResult::ForeignOwner);
        EXPECT_EQ(ops.op.read(l.mountKey("r"), Retry::standard())->etag, before->etag);
    }
}

/// A foreign body at or above our epoch is never replaced: a delayed claimer with a lower epoch stays out.
TEST(CASMount, ForeignTerminalBodyAtOrAboveOurEpochStaysForeign)
{
    for (const uint64_t body_epoch : {8, 9})
    {
        SCOPED_TRACE(body_epoch);
        auto b = std::make_shared<InMemoryBackend>();
        Layout l("p");
        Ops ops(b);
        seedOwner(ops, l, liveOwner(UInt128(1), 2));
        mustCommit(ops.op.create(l.mountKey("r"), encodeMountLease(terminalBody(UInt128(2), body_epoch, false)), Retry::standard()), "slot");
        const auto before = ops.op.read(l.mountKey("r"), Retry::standard());
        EXPECT_EQ(claimMount(ops.op, l, "r", UInt128(1), 8, 1000, 100, {}, {}, {}, uuid_one_at_life_two).kind, MountClaimResult::ForeignOwner);
        EXPECT_EQ(ops.op.read(l.mountKey("r"), Retry::standard())->etag, before->etag);
    }
}

/// GUARD: a live body of another uuid is never taken, whatever the owner says.
TEST(CASMount, ForeignLiveBodyStaysForeignOwner)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    Ops ops(b);
    seedOwner(ops, l, liveOwner(UInt128(1), 2));
    mustCommit(ops.op.create(l.mountKey("r"), encodeMountLease(liveBody(UInt128(2), 7)), Retry::standard()), "slot");
    const auto before = ops.op.read(l.mountKey("r"), Retry::standard());
    EXPECT_EQ(claimMount(ops.op, l, "r", UInt128(1), 8, 1000, 100, {}, {}, {}, uuid_one_at_life_two).kind, MountClaimResult::ForeignOwner);
    EXPECT_EQ(ops.op.read(l.mountKey("r"), Retry::standard())->etag, before->etag);
}

/// A start and a self-remount pass their owner expectation, so both take over a terminal body of another uuid.
TEST(CASMount, ClaimPathsReclaimAForeignTerminalBodyOfTheirOwnOwner)
{
    {
        SCOPED_TRACE("start");
        auto b = std::make_shared<InMemoryBackend>();
        Layout l("p");
        { auto first = Pool::open(b, rootConfig()); }
        rewriteSlotUuid(*b, l, "r", UInt128(2));
        auto store = Pool::open(b, rootConfig());
        const MountLease slot = slotBody(*b, l, "r");
        EXPECT_EQ(slot.server_uuid, UInt128(1));
        EXPECT_EQ(slot.writer_epoch, store->writerEpoch());
    }
    {
        SCOPED_TRACE("self-remount");
        auto b = std::make_shared<InMemoryBackend>();
        Layout l("p");
        auto store = Pool::open(b, rootConfig());
        fenceSlot(*b, l.mountKey("r"));
        DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::ABORTED, [&] { store->renewWatermarkOnce(); });
        rewriteSlotUuid(*b, l, "r", UInt128(2));
        EXPECT_TRUE(store->tryRemountOnce());
        const MountLease slot = slotBody(*b, l, "r");
        EXPECT_EQ(slot.server_uuid, UInt128(1));
        EXPECT_FALSE(slot.gc_fenced);
    }
}
```

- [ ] **Step 2: write the admin guard in `gtest_cas_decommission.cpp`** (append at the end of the file):

```cpp
/// GUARD: the admin's owner is retired by design, so it never takes another uuid's terminal body; the run
/// refuses before writing anything.
TEST(CASDecommission, AdminDoesNotReclaimAForeignTerminalBody)
{
    auto backend = std::make_shared<InMemoryBackend>();
    { auto victim = openVictim(backend); }
    const Layout layout("p");
    {
        OperationForTest op(*backend);
        const auto got = (*op).read(layout.mountKey("victim"), Retry::once());
        MountLease body = decodeMountLease(got->bytes);
        body.server_uuid = UInt128(5);
        body.seq += 1;
        ASSERT_TRUE(std::holds_alternative<Committed>(
            (*op).replace(layout.mountKey("victim"), encodeMountLease(body), got->etag, Retry::once())));
    }
    const OwnerObject owner_before = readOwner(*backend, layout, "victim");
    const MountLease slot_before = readSlot(*backend, layout, "victim");

    expectThrowsCodeWithMessage(ErrorCodes::ABORTED, "alive or contended", [&]
    {
        decommissionPoolMember(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");
    });

    const OwnerObject owner_after = readOwner(*backend, layout, "victim");
    EXPECT_EQ(owner_after.owner_life, owner_before.owner_life);
    EXPECT_FALSE(owner_after.retired_at_ms.has_value());
    EXPECT_EQ(readSlot(*backend, layout, "victim").seq, slot_before.seq);
}
```

- [ ] **Step 3: build and run to verify they fail.**
  `--gtest_filter='CASMount.Foreign*:CASMount.ClaimPaths*:CASDecommission.AdminDoesNotReclaimAForeignTerminalBody'`.
  Expected: compile failure (`claimMount` takes no expectation argument). After Step 4's signature change
  only (the declaration and a body that ignores the argument), the semantic reasons:
  `ForeignTerminalBodyIsReclaimable` FAILED (`ForeignOwner`, not `Claimed`), `ClaimPathsReclaim…` FAILED
  (start: `ABORTED` double-start message; remount: `tryRemountOnce` returns false), the other three OK
  (guards: today every foreign body is refused).

- [ ] **Step 4: implement.** In `CasServerRoot.h`, `claimMount`'s declaration gains
  `const std::optional<OwnerExpectation> & foreign_reclaim_owner = {}` after `unsafe_reclaim_authorization`,
  and `claimMountAwaitingExpiry` gains the same parameter after `sink`. Replace the comment bullet
  "different `server_uuid` → `ForeignOwner` (do NOT write, regardless of expiry or prior state)" with:

```cpp
///   - different `server_uuid` → `ForeignOwner`, no write, unless the body is terminal (`gc_fenced` or the
///     clean marker), its `writer_epoch` is below `our_epoch`, and a fresh owner read matches
///     `foreign_reclaim_owner`: then it is reclaimed like an older epoch of our own (`prior = Fenced` or
///     `Clean`). A rejoin under a new uuid leaves such a body; reading the owner first means a reclaim never
///     lands under somebody else's owner.
```
  In `CasServerRoot.cpp`, in the anonymous namespace that ends just before `claimMount` (after
  `emitMountEvent`), add:

```cpp
/// Why `owner` is not the one `expected` names; nullopt when it is.
std::optional<String> ownerRefusal(const std::optional<OwnerObject> & owner, const String & srid, const OwnerExpectation & expected)
{
    if (!owner)
        return fmt::format("CAS server-root '{}': the owner anchor vanished after the mount claim", srid);
    if (owner->server_uuid != expected.server_uuid)
        return fmt::format(
            "CAS server-root '{}' is owned by a different server since this claim began (owner server_uuid={}, "
            "ours={}); the mount slot was handed to another server and this claim is withdrawn",
            srid, u128ToHex(owner->server_uuid), u128ToHex(expected.server_uuid));
    if (owner->retired_at_ms && expected.retired != RetiredOwnerPolicy::AcceptForDecommission)
        return fmt::format(
            "CAS server-root '{}' was decommissioned (tombstoned at {} ms) while this claim was in progress; "
            "refusing to run under a retired owner", srid, *owner->retired_at_ms);
    if (owner->owner_life != expected.owner_life)
        return fmt::format(
            "CAS server-root '{}': the owner moved from life {} to life {} since this process started (a "
            "retirement, a rollback or a rejoin happened in between); restart to continue under the current life",
            srid, expected.owner_life, owner->owner_life);
    return std::nullopt;
}
```
  `recheckOwnerAfterClaim` becomes:

```cpp
void recheckOwnerAfterClaim(
    CasOperation & op, const Layout & l, const String & srid, const OwnerExpectation & expected,
    uint64_t our_epoch, const CasEventSink & sink)
{
    const std::optional<String> refusal = ownerRefusal(readOwnerObject(op, l, srid), srid, expected);
    if (!refusal)
        return;

    /// The fence only spares a successor the lease wait; its failure must not replace the owner's reason.
    try
    {
        fenceOwnClaim(op, l, srid, expected.server_uuid, our_epoch, sink);
    }
    catch (...)
    {
        tryLogCurrentException(getLogger("CasMountLease"),
            fmt::format("CAS server-root '{}': the refused claim's mount body could not be fenced", srid));
    }
    throw Exception(ErrorCodes::CORRUPTED_DATA, "{}", *refusal);
}
```
  In `claimMount`: add the parameter to the definition; move `const bool clean_marker = …` up to right after
  `const MountLease existing = decodeMountLease(got->bytes);`; replace the foreign block with:

```cpp
    /// A live body of another uuid is never taken. A terminal one below our epoch is what a rejoin under a new
    /// uuid finds; it is reclaimed only once the owner reads as this claimer's own life.
    if (existing.server_uuid != our_uuid)
    {
        const bool reclaimable = (existing.gc_fenced || clean_marker) && existing.writer_epoch < our_epoch
            && foreign_reclaim_owner && !ownerRefusal(readOwnerObject(op, l, srid), srid, *foreign_reclaim_owner);
        if (!reclaimable)
        {
            emitMountEvent(sink, CasEventType::MountConflict, srid, "foreign_owner", &existing,
                "mount slot is held by a foreign server_uuid — refusing to take over across identities");
            return {.kind = MountClaimResult::ForeignOwner, .body = existing, .etag = std::nullopt};
        }
    }
```
  A reclaimable foreign body passes the two same-uuid epoch checks untouched (its epoch is below ours) and
  reaches the existing reclaim branch. In that branch's event text, put first:
  `existing.server_uuid != our_uuid ? "terminal body of another server_uuid, the owner is ours — reclaimed" :`.
  In `claimMountAwaitingExpiry`, add the parameter to the definition and pass `foreign_reclaim_owner` as the
  last argument of both `claimMount` calls.

  In `CasPool.cpp` `Pool::mountWritable`, after `store->recorded_owner_life = owner_life;`:

```cpp
    /// Only a server start takes over another uuid's terminal body: the decommission admin's owner is
    /// retired by design and would never match.
    const std::optional<OwnerExpectation> foreign_reclaim_owner = policy == MountClaimPolicy::WaitForExpiry
        ? std::optional<OwnerExpectation>(OwnerExpectation{
            .server_uuid = our_uuid, .owner_life = owner_life, .retired = RetiredOwnerPolicy::Refuse})
        : std::nullopt;
```
  Pass it in the `WaitForExpiry` branch: the first unsafe `claimMount` call gets
  `/*unsafe_reclaim_authorization=*/{}, foreign_reclaim_owner` after `emit_mount_event`; the second gets
  `foreign_reclaim_owner` after `claim.etag`; `claimMountAwaitingExpiry` gets it after `emit_mount_event`. The
  `NoWait` call is unchanged. In `Pool::tryRemountOnce`, `claimMountAwaitingExpiry` gets
  `OwnerExpectation{.server_uuid = our_uuid, .owner_life = recorded_owner_life, .retired = RetiredOwnerPolicy::Refuse}`
  after `emit_mount_event`.

- [ ] **Step 5: run to verify they pass**, then the sabotage checks, one at a time, restoring after each:
  delete `&& !ownerRefusal(readOwnerObject(op, l, srid), srid, *foreign_reclaim_owner)` → expect
  `ForeignReclaimReadsTheOwnerFirst` FAILED for the first four cases; delete `&& existing.writer_epoch < our_epoch`
  → expect `ForeignTerminalBodyAtOrAboveOurEpochStaysForeign` FAILED. Paste both failure lines in the report.
- [ ] **Step 6: run** `--gtest_filter='CASMount*:CASMountAwaitExpiry*:CASMountLease*:CASMountClaim*:CASDecommission*'`; all green.
- [ ] **Step 7: commit.**

```bash
git add -- $CA/Pool/CasServerRoot.h $CA/Pool/CasServerRoot.cpp $CA/Pool/CasPool.cpp \
  src/Disks/tests/gtest_cas_mount.cpp src/Disks/tests/gtest_cas_decommission.cpp
git commit -m "Let a CAS start reclaim another uuid's terminal mount body of its own owner

After a rejoin under a new server_uuid the slot can hold the previous
uuid's farewell or self-fence. claimMount now reclaims such a terminal
body below its epoch once a fresh owner read shows the claimer's uuid
and life, not retired. A start and a self-remount pass that expectation;
the decommission admin does not.

Signed-off-by: Mikhail Filimonov <mfilimonov@altinity.com>
Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 2: rejoin a verified, drained retirement at `Pool::open` (D6, first half)

**Files:**
- Modify: `$CA/Pool/CasServerRoot.h` (`RetiredOwnerPolicy` `:144-148`, `claimOwnerOrThrow` contract `:150-162`)
- Modify: `$CA/Pool/CasServerRoot.cpp` (`throwIfOwnerRetired` `:443-460`, a new anonymous-namespace block
  before `claimOwnerOrThrow`, `claimOwnerOrThrow` `:533-604`)
- Modify: `$CA/Pool/CasPool.cpp` (`Pool::mountWritable` `:627-632`), `$CA/Pool/CasPool.h` (`Pool::open` comment `:427-430`)
- Test: `src/Disks/tests/gtest_cas_decommission.cpp` (a `CASRejoin` block at the end),
  `src/Disks/tests/gtest_cas_mount.cpp` (`CASMount.PoolOpenNeverAcceptsARetiredOwner`,
  `CASServerRootClaim.ConflictRereadAppliesTheRetiredPolicy`)

**Interfaces:**
- Consumes: Task 1's `claimMount` foreign reclaim and `foreign_reclaim_owner` wiring;
  `serverRootSubtreeEmpty`, `conflictOrThrow`, `readOwnerObject`, `Layout::serverRootDataPrefix`.
- Produces:
  - `enum class RetiredOwnerPolicy { Refuse, AcceptForDecommission, RejoinIfVerified }`.
  - `claimOwnerOrThrow(op, l, srid, our_uuid, observe_catalog, RetiredOwnerPolicy::RejoinIfVerified)` returns the
    new life after a rejoin; refusals per R4.
  - File-local `String accessProbeKey(const Layout &, const String & srid, UInt128 server_uuid)` and
    `std::optional<uint64_t> rejoinRetiredOwner(CasOperation &, const Layout &, const String & srid, UInt128 our_uuid, const OwnerObject & owner, const Etag & owner_etag, const ObserveRefCatalog &)`
    (nullopt = an `If-Match` lost; decide again). Task 3 adds the mount rewrite inside `rejoinRetiredOwner`.

- [ ] **Step 1: edit the two existing tests in `gtest_cas_mount.cpp`.**
  - `CASMount.PoolOpenNeverAcceptsARetiredOwner`: rename to `PoolOpenRefusesAnUnverifiedRetirement`, drop the
    `for (const bool verified …)` loop and `SCOPED_TRACE`, keep the body with `tombstone.retirement_verified = false;`
    and the message `"decommission is not finished"`. New comment: `/// GUARD: a start over an unverified
    tombstone is refused and writes neither the counter nor the slot.`
  - `CASServerRootClaim.ConflictRereadAppliesTheRetiredPolicy`: the loop runs over
    `{RetiredOwnerPolicy::Refuse, RetiredOwnerPolicy::AcceptForDecommission, RetiredOwnerPolicy::RejoinIfVerified}`;
    the `SCOPED_TRACE` prints `static_cast<int>(policy)`; the refusal branch is `if (policy != RetiredOwnerPolicy::AcceptForDecommission)` and expects
    `"decommission is not finished"` (the racing body is an unverified tombstone).

- [ ] **Step 2: write the failing `CASRejoin` tests** at the end of `gtest_cas_decommission.cpp`:

```cpp
namespace
{
PoolConfig victimConfig(UInt128 uuid = UInt128{})
{
    return PoolConfig{.pool_prefix = "p", .server_id = uuid, .server_root_id = "victim"};
}

/// One completed decommission of the victim with nothing left for GC: the state a rejoin starts from.
void retireVictim(const std::shared_ptr<InMemoryBackend> & backend, const String & admin_srid)
{
    const auto report = decommissionPoolMember(backend, PoolConfig{.pool_prefix = "p", .server_root_id = admin_srid}, "victim");
    if (!report.member_retired || report.namespaces_pending_gc != 0)
        throw std::runtime_error("fixture: the decommission did not retire the victim cleanly");
}

bool victimSubtreeEmpty(Backend & backend)
{
    const Layout layout("p");
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    return serverRootSubtreeEmpty(op, layout, "victim", CasRefCatalog::read(op, layout).catalog);
}

/// GC removes the dropped namespaces' catalog rows and manifests over a few rounds; a rejoin waits for all of them.
void gcUntilVictimSubtreeEmpty(const std::shared_ptr<InMemoryBackend> & backend)
{
    auto store = Pool::open(backend, PoolConfig{
        .pool_prefix = "p", .server_root_id = "gc", .gc_fold_threshold = 1, .gc_fold_max_defer_rounds = 0});
    Gc gc(store, UInt128{991});
    for (int round = 0; round < 8 && !victimSubtreeEmpty(*backend); ++round)
        runRegularRoundReclaiming(gc);
    if (!victimSubtreeEmpty(*backend))
        throw std::runtime_error("fixture: GC left data of the victim's previous life");
}

/// The slot body rewritten by hand; `edit` changes the decoded body before it is written back.
void rewriteSlot(Backend & backend, const Layout & layout, const std::function<void(MountLease &)> & edit)
{
    OperationForTest op(backend);
    const auto got = (*op).read(layout.mountKey("victim"), Retry::once());
    ASSERT_TRUE(got.has_value());
    MountLease body = decodeMountLease(got->bytes);
    edit(body);
    body.seq += 1;
    ASSERT_TRUE(std::holds_alternative<Committed>(
        (*op).replace(layout.mountKey("victim"), encodeMountLease(body), got->etag, Retry::once())));
}
}

TEST(CASRejoin, SameUuidStartsEmptyAndContinuesTheCounter)
{
    auto backend = std::make_shared<InMemoryBackend>();
    {
        auto victim = Pool::open(backend, victimConfig());
        makeTableWithRefs(*victim, "victim/db/t1", 2, 0);
    }
    ASSERT_TRUE(decommissionPoolMember(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim").member_retired);
    gcUntilVictimSubtreeEmpty(backend);
    const Layout layout("p");
    const OwnerObject retired = readOwner(*backend, layout, "victim");
    ASSERT_TRUE(retired.retirement_verified);
    const uint64_t admin_epoch = readSlot(*backend, layout, "victim").writer_epoch;

    auto rejoined = Pool::open(backend, victimConfig());

    EXPECT_GT(rejoined->writerEpoch(), admin_epoch) << "the kept counter continues past every earlier life";
    EXPECT_EQ(rejoined->ownerLife(), retired.owner_life + 1);
    const OwnerObject owner = readOwner(*backend, layout, "victim");
    EXPECT_EQ(owner.owner_life, retired.owner_life + 1);
    EXPECT_FALSE(owner.retired_at_ms.has_value());
    EXPECT_FALSE(owner.retirement_verified);
    EXPECT_TRUE(victimSubtreeEmpty(*backend)) << "the member comes back empty";
    EXPECT_EQ(readSlot(*backend, layout, "victim").writer_epoch, rejoined->writerEpoch());
}

TEST(CASRejoin, NewUuidReclaimsTheOldLifesFarewell)
{
    auto backend = std::make_shared<InMemoryBackend>();
    { auto victim = Pool::open(backend, victimConfig()); }
    retireVictim(backend, "admin");
    const Layout layout("p");
    const MountLease farewell = readSlot(*backend, layout, "victim");

    auto rejoined = Pool::open(backend, victimConfig(UInt128(77)));

    const OwnerObject owner = readOwner(*backend, layout, "victim");
    EXPECT_EQ(owner.server_uuid, UInt128(77));
    EXPECT_FALSE(owner.retired_at_ms.has_value());
    const MountLease slot = readSlot(*backend, layout, "victim");
    EXPECT_EQ(slot.server_uuid, UInt128(77));
    EXPECT_GT(slot.writer_epoch, farewell.writer_epoch);
    EXPECT_EQ(slot.writer_epoch, rejoined->writerEpoch());
}

/// The old victim reclaimed the admin's farewell under a fresh epoch, saw the tombstone and fenced itself.
TEST(CASRejoin, NewUuidReclaimsASelfFencedOldVictimBody)
{
    auto backend = std::make_shared<InMemoryBackend>();
    { auto victim = Pool::open(backend, victimConfig()); }
    retireVictim(backend, "admin");
    const Layout layout("p");
    uint64_t victim_epoch = 0;
    {
        OperationForTest op(*backend);
        victim_epoch = allocateWriterEpoch(*op, layout, "victim", [] { return RefCatalog{}; });
    }
    rewriteSlot(*backend, layout, [&](MountLease & body)
    {
        body.writer_epoch = victim_epoch;
        body.min_active_build_sequence = 0;
        body.gc_fenced = true;
    });

    auto rejoined = Pool::open(backend, victimConfig(UInt128(77)));

    const MountLease slot = readSlot(*backend, layout, "victim");
    EXPECT_EQ(slot.server_uuid, UInt128(77));
    EXPECT_EQ(slot.writer_epoch, rejoined->writerEpoch());
    EXPECT_FALSE(slot.gc_fenced);
}

TEST(CASRejoin, RefusesWhileTheDecommissionRuns)
{
    auto backend = std::make_shared<InMemoryBackend>();
    { auto victim = Pool::open(backend, victimConfig()); }
    retireVictim(backend, "admin");
    const Layout layout("p");
    auto rerun_admin = Pool::openForDecommission(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin2"}, "victim");
    const OwnerObject before = readOwner(*backend, layout, "victim");

    for (const UInt128 uuid : {UInt128{}, UInt128(77)})
    {
        SCOPED_TRACE(u128ToHex(uuid));
        expectThrowsCodeWithMessage(ErrorCodes::ABORTED, "still running", [&] { Pool::open(backend, victimConfig(uuid)); });
    }

    const OwnerObject after = readOwner(*backend, layout, "victim");
    EXPECT_EQ(after.owner_life, before.owner_life);
    EXPECT_TRUE(after.retirement_verified);
    EXPECT_EQ(readSlot(*backend, layout, "victim").writer_epoch, rerun_admin->writerEpoch()) << "the admin's live body is untouched";
}

/// The verified mark does not wait for GC; the rejoin does.
TEST(CASRejoin, RefusesWhileDataRemains)
{
    auto backend = std::make_shared<InMemoryBackend>();
    {
        auto victim = Pool::open(backend, victimConfig());
        makeTableWithRefs(*victim, "victim/db/t1", 1, 0);
    }
    const auto report = decommissionPoolMember(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");
    ASSERT_TRUE(report.member_retired);
    ASSERT_EQ(report.namespaces_pending_gc, 1u);
    const Layout layout("p");
    const OwnerObject before = readOwner(*backend, layout, "victim");
    OperationForTest raw_op(*backend);
    const Etag epoch_before = (*raw_op).read(layout.epochKey("victim"), Retry::once())->etag;

    expectThrowsCodeWithMessage(ErrorCodes::ABORTED, "previous life remains", [&] { Pool::open(backend, victimConfig()); });

    EXPECT_EQ(readOwner(*backend, layout, "victim").owner_life, before.owner_life);
    EXPECT_EQ((*raw_op).read(layout.epochKey("victim"), Retry::once())->etag, epoch_before) << "no epoch was allocated";
    gcUntilVictimSubtreeEmpty(backend);
    EXPECT_NO_THROW(Pool::open(backend, victimConfig()));
}

TEST(CASRejoin, RefusesSlotRetiredBy2664)
{
    const Layout layout("p");
    {
        SCOPED_TRACE("26.6.4 end state: tombstone without life or mark, no epoch, no mount");
        auto backend = std::make_shared<InMemoryBackend>();
        OperationForTest op(*backend);
        const String tombstone = encodeOwner(OwnerObject{.server_uuid = UInt128{}, .retired_at_ms = 1});
        ASSERT_TRUE(std::holds_alternative<Committed>((*op).create(layout.ownerKey("victim"), tombstone, Retry::once())));
        expectThrowsCodeWithMessage(ErrorCodes::CORRUPTED_DATA, "new <cas_server_root_id>", [&] { Pool::open(backend, victimConfig()); });
        EXPECT_EQ((*op).read(layout.ownerKey("victim"), Retry::once())->bytes, tombstone);
        EXPECT_FALSE((*op).head(layout.epochKey("victim"), Retry::once()).has_value());
        EXPECT_FALSE((*op).head(layout.mountKey("victim"), Retry::once()).has_value());
    }
    {
        SCOPED_TRACE("verified retirement whose epoch counter was deleted by hand");
        auto backend = std::make_shared<InMemoryBackend>();
        { auto victim = Pool::open(backend, victimConfig()); }
        retireVictim(backend, "admin");
        OperationForTest op(*backend);
        ASSERT_EQ((*op).removeCurrent(layout.epochKey("victim"), Retry::once()), Removal::Removed);
        expectThrowsCodeWithMessage(ErrorCodes::CORRUPTED_DATA, "cannot rejoin", [&] { Pool::open(backend, victimConfig()); });
        EXPECT_FALSE((*op).head(layout.epochKey("victim"), Retry::once()).has_value());
        EXPECT_TRUE(readOwner(*backend, layout, "victim").retired_at_ms.has_value());
    }
}

/// Starter 88 rewrote the slot and stalls before its owner write; starter 77 writes its owner first. The
/// winner takes the loser's terminal body through the owner read; the loser is refused and writes nothing.
TEST(CASRejoin, ConcurrentStartersWithDifferentUuidsOneWins)
{
    auto backend = std::make_shared<InMemoryBackend>();
    { auto victim = Pool::open(backend, victimConfig()); }
    retireVictim(backend, "admin");
    const Layout layout("p");
    bool loser_wrote_mount = false;
    backend->onBeforeWrite(layout.ownerKey("victim"), [&]
    {
        if (std::exchange(loser_wrote_mount, true))
            return;
        rewriteSlot(*backend, layout, [](MountLease & body) { body.server_uuid = UInt128(88); });
    });

    auto winner = Pool::open(backend, victimConfig(UInt128(77)));

    ASSERT_TRUE(loser_wrote_mount);
    EXPECT_EQ(readOwner(*backend, layout, "victim").server_uuid, UInt128(77));
    EXPECT_EQ(readSlot(*backend, layout, "victim").server_uuid, UInt128(77));
    expectThrowsCodeWithMessage(ErrorCodes::CORRUPTED_DATA, "different server", [&] { Pool::open(backend, victimConfig(UInt128(88))); });
    EXPECT_EQ(readSlot(*backend, layout, "victim").writer_epoch, winner->writerEpoch());
}

/// A start of the old life allocated its epoch before the decommission and claims only after a rejoin.
TEST(CASRejoin, DelayedLowerEpochClaimAfterRejoinIsRefused)
{
    for (const UInt128 rejoiner : {UInt128{}, UInt128(77)})
    {
        SCOPED_TRACE(rejoiner == UInt128{} ? "same uuid" : "new uuid: a foreign farewell");
        auto backend = std::make_shared<InMemoryBackend>();
        { auto victim = Pool::open(backend, victimConfig()); }
        const Layout layout("p");
        uint64_t delayed_epoch = 0;
        {
            OperationForTest op(*backend);
            delayed_epoch = allocateWriterEpoch(*op, layout, "victim", [] { return RefCatalog{}; });
        }
        retireVictim(backend, "admin");
        { auto rejoined = Pool::open(backend, victimConfig(rejoiner)); }
        OperationForTest op(*backend);
        const auto before = (*op).read(layout.mountKey("victim"), Retry::once());
        const OwnerExpectation old_life{.server_uuid = UInt128{}, .owner_life = 1, .retired = RetiredOwnerPolicy::Refuse};
        if (rejoiner == UInt128{})
            expectThrowsCode(ErrorCodes::ABORTED, [&] { claimMount(*op, layout, "victim", UInt128{}, delayed_epoch, 1, 100, {}, {}, {}, old_life); });
        else
            EXPECT_EQ(claimMount(*op, layout, "victim", UInt128{}, delayed_epoch, 1, 100, {}, {}, {}, old_life).kind, MountClaimResult::ForeignOwner);
        EXPECT_EQ((*op).read(layout.mountKey("victim"), Retry::once())->etag, before->etag);
    }
}

/// The victim restarts while the admin holds the slot; it reclaims the farewell, sees the tombstone and
/// fences itself. A completed rerun later, the same victim rejoins on its next start.
TEST(CASRejoin, VictimRefusedAfterFarewellRejoinsOnItsNextStart)
{
    auto backend = std::make_shared<InMemoryBackend>();
    { auto victim = Pool::open(backend, victimConfig()); }
    const Layout layout("p");
    auto admin = Pool::openForDecommission(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim");
    auto fake_boot = std::make_shared<std::atomic<uint64_t>>(0);
    bool decommissioned = false;
    PoolConfig waiting = victimConfig();
    waiting.boot_ms_fn = [fake_boot] { return fake_boot->load(); };
    waiting.wait_sleep_fn = [&, fake_boot](uint64_t ms)
    {
        *fake_boot += ms;
        if (std::exchange(decommissioned, true))
            return;
        tombstoneOwner(*backend, layout, "victim");
        admin.reset();
    };
    expectThrowsCodeWithMessage(ErrorCodes::CORRUPTED_DATA, "decommissioned", [&] { Pool::open(backend, waiting); });
    ASSERT_TRUE(readSlot(*backend, layout, "victim").gc_fenced);

    retireVictim(backend, "admin2");
    auto rejoined = Pool::open(backend, victimConfig());
    EXPECT_FALSE(readOwner(*backend, layout, "victim").retired_at_ms.has_value());
    EXPECT_EQ(readSlot(*backend, layout, "victim").writer_epoch, rejoined->writerEpoch());
}

TEST(CASRejoin, RefusesWhileAnUnfinishedDecommissionLeftData)
{
    auto backend = std::make_shared<FailingDeleteBackend>();
    { auto victim = Pool::open(backend, victimConfig()); }
    {
        OperationForTest op(*backend);
        ASSERT_TRUE(std::holds_alternative<Committed>((*op).create("p/roots/victim/kept", "x", Retry::once())));
    }
    backend->failWithTokenMismatch("p/roots/victim/kept");
    ASSERT_FALSE(decommissionPoolMember(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin"}, "victim").member_retired);
    const Layout layout("p");
    const OwnerObject tombstone = readOwner(*backend, layout, "victim");
    ASSERT_TRUE(tombstone.retired_at_ms.has_value());
    ASSERT_FALSE(tombstone.retirement_verified);

    for (const UInt128 uuid : {UInt128{}, UInt128(77)})
    {
        SCOPED_TRACE(u128ToHex(uuid));
        expectThrowsCodeWithMessage(ErrorCodes::CORRUPTED_DATA, "decommission is not finished", [&] { Pool::open(backend, victimConfig(uuid)); });
    }
    EXPECT_EQ(readOwner(*backend, layout, "victim").owner_life, tombstone.owner_life);

    backend->disarm();
    retireVictim(backend, "admin2");
    EXPECT_NO_THROW(Pool::open(backend, victimConfig()));
}

/// `IDisk::checkAccess` of the old life wrote its probe after the drain. The rejoin deletes that one key;
/// any other object under `roots/<srid>/` still blocks it.
TEST(CASRejoin, DeletesTheOldAccessProbeAndKeepsOtherRootsObjects)
{
    const UInt128 old_uuid = hexToU128("0123456789abcdeffedcba9876543210");
    const String probe = "p/roots/victim/clickhouse_access_check_01234567-89ab-cdef-fedc-ba9876543210";
    auto backend = std::make_shared<InMemoryBackend>();
    { auto victim = Pool::open(backend, victimConfig(old_uuid)); }
    retireVictim(backend, "admin");
    OperationForTest op(*backend);
    ASSERT_TRUE(std::holds_alternative<Committed>((*op).create(probe, "test", Retry::once())));
    ASSERT_TRUE(std::holds_alternative<Committed>((*op).create("p/roots/victim/backup.zip", "zip", Retry::once())));

    expectThrowsCodeWithMessage(ErrorCodes::ABORTED, "previous life remains", [&] { Pool::open(backend, victimConfig(UInt128(77))); });
    EXPECT_FALSE((*op).head(probe, Retry::once()).has_value()) << "the old life's probe is deleted first";
    EXPECT_TRUE((*op).head("p/roots/victim/backup.zip", Retry::once()).has_value());

    ASSERT_EQ((*op).removeCurrent("p/roots/victim/backup.zip", Retry::once()), Removal::Removed);
    EXPECT_NO_THROW(Pool::open(backend, victimConfig(UInt128(77))));
}

/// A losing racing rejoiner of uuid 88 left its terminal body; the old uuid's own restart still rejoins.
TEST(CASRejoin, SameUuidRejoinsOverAForeignTerminalBody)
{
    auto backend = std::make_shared<InMemoryBackend>();
    { auto victim = Pool::open(backend, victimConfig()); }
    retireVictim(backend, "admin");
    const Layout layout("p");
    rewriteSlot(*backend, layout, [](MountLease & body) { body.server_uuid = UInt128(88); });

    auto rejoined = Pool::open(backend, victimConfig());

    const MountLease slot = readSlot(*backend, layout, "victim");
    EXPECT_EQ(slot.server_uuid, UInt128{});
    EXPECT_EQ(slot.writer_epoch, rejoined->writerEpoch());
}

/// GUARD: only a start rejoins. A process that was fenced and finds its id retired stops.
TEST(CASRejoin, RemountDoesNotRejoin)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend, victimConfig());
    const Layout layout("p");
    tombstoneOwner(*backend, layout, "victim", /*verified=*/true);
    fenceSlot(*backend, layout, "victim");
    expectThrowsCode(ErrorCodes::ABORTED, [&] { store->renewWatermarkOnce(); });
    const OwnerObject before = readOwner(*backend, layout, "victim");

    EXPECT_FALSE(store->tryRemountOnce());

    const OwnerObject after = readOwner(*backend, layout, "victim");
    EXPECT_EQ(after.owner_life, before.owner_life);
    EXPECT_TRUE(after.retired_at_ms.has_value());
}
```
  Check `#include <functional>` and `CasRefCatalog.h` are reachable in the file (`makeTableWithRefs` already
  calls `CasRefCatalog::read`); add `#include <functional>` if missing.

- [ ] **Step 3: build and run to verify they fail.** `--gtest_filter='CASRejoin.*:CASMount.PoolOpen*:CASServerRootClaim.ConflictReread*'`.
  Expected: compile failure (`RetiredOwnerPolicy::RejoinIfVerified` does not exist). With only the enum value
  added (Step 4's first edit), every `CASRejoin` test FAILED at its `Pool::open` over the retired owner
  (`CORRUPTED_DATA`, "was explicitly decommissioned" or "different server"), except `RemountDoesNotRejoin` OK
  (guard); `PoolOpenRefusesAnUnverifiedRetirement` and `ConflictReread…` FAILED on the message
  ("decommission is not finished" absent). Paste the FAILED lines.

- [ ] **Step 4: implement.**
  `CasServerRoot.h`:

```cpp
enum class RetiredOwnerPolicy : uint8_t
{
    Refuse,                  /// a remount never resumes a tombstoned owner
    AcceptForDecommission,   /// the decommission admin impersonates a retired victim to drain or rerun
    RejoinIfVerified,        /// a server start takes over a verified, drained retirement as an empty member
};
```
  `claimOwnerOrThrow` contract: replace the tombstoned bullet and the last two lines with:

```cpp
///   - present and tombstoned, under `RejoinIfVerified` (a server start), any uuid: rejoin when the retirement
///     is verified, `epoch` and `mount` exist, the slot is terminal and the subtree is empty (after deleting
///     the old life's access probe); a new uuid first rewrites the terminal mount body under its own uuid;
///     the owner becomes `{our_uuid, owner_life + 1}` and that life is returned. Otherwise
///     `CORRUPTED_DATA` (not verified; `epoch` or `mount` missing) or `ABORTED` (slot live, data left);
///   - present, equal `our_uuid`, tombstoned → `CORRUPTED_DATA` under `Refuse`; the stored life under
///     `AcceptForDecommission`;
///   - present, different uuid, not tombstoned → `CORRUPTED_DATA` (foreign owner);
/// The owner object is never deleted; a rejoin is the one supported change of its uuid.
```
  `CasServerRoot.cpp`, `throwIfOwnerRetired`:

```cpp
void throwIfOwnerRetired(const OwnerObject & owner, const String & srid, const String & owner_key)
{
    if (!owner.retired_at_ms)
        return;

    if (owner.retirement_verified)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS server-root '{}' was decommissioned (owner tombstoned at {} ms, life {}); this process started "
            "under an earlier life and stops. Restart the server: a start rejoins a finished decommission as an "
            "empty member",
            srid, *owner.retired_at_ms, owner.owner_life);

    /// The hand repair is for a tombstone that landed over a live member after its decommission lost the
    /// slot and died: a rerun would drain that member.
    const String repaired = encodeOwner(OwnerObject{
        .server_uuid = owner.server_uuid, .retired_at_ms = std::nullopt, .owner_life = owner.owner_life + 1,
        .retirement_verified = false});
    const size_t body_begin = repaired.find('\n') + 1;
    const String repaired_body = repaired.substr(body_begin, repaired.size() - body_begin - 1);
    throw Exception(ErrorCodes::CORRUPTED_DATA,
        "CAS server-root '{}' was decommissioned (owner tombstoned at {} ms, life {}) and the decommission is not "
        "finished: its retirement is not verified. Rerun SYSTEM CAS DROP POOL MEMBER '{}' FROM DISK '<disk>' on a "
        "live member until it reports member_retired = 1, then restart this server: it rejoins empty. An id "
        "retired by 26.6.4 cannot rejoin; configure a new <cas_server_root_id> for this disk. Only if this server "
        "was running when the tombstone appeared and no decommission of it is in progress, keep the header line "
        "of the owner object '{}', replace its body line with {} and restart; a rerun would delete its data",
        srid, *owner.retired_at_ms, owner.owner_life, srid, owner_key, repaired_body);
}
```
  New anonymous-namespace block right before `claimOwnerOrThrow`:

```cpp
namespace
{
/// The key `IDisk::checkAccess` writes under the disk root, `clickhouse_access_check_<server uuid>`.
String accessProbeKey(const Layout & l, const String & srid, UInt128 server_uuid)
{
    const String hex = u128ToHex(server_uuid);
    return fmt::format("{}clickhouse_access_check_{}-{}-{}-{}-{}", l.serverRootDataPrefix(srid),
        hex.substr(0, 8), hex.substr(8, 4), hex.substr(12, 4), hex.substr(16, 4), hex.substr(20));
}

/// Take over the retired `owner` as `our_uuid` at its next life. nullopt: an `If-Match` lost to another
/// write, and the caller decides again from a fresh owner read.
std::optional<uint64_t> rejoinRetiredOwner(
    CasOperation & op, const Layout & l, const String & srid, UInt128 our_uuid, const OwnerObject & owner,
    const Etag & owner_etag, const ObserveRefCatalog & observe_catalog)
{
    /// A rerun of an unfinished decommission drains whoever owns the id when it starts.
    if (!owner.retirement_verified)
        throwIfOwnerRetired(owner, srid, l.ownerKey(srid));

    const std::optional<Object> mount = op.read(l.mountKey(srid), Retry::standard());
    if (!mount || !op.head(l.epochKey(srid), Retry::standard()))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS server-root '{}' is retired but its {} object is missing (retired by 26.6.4, or deleted by hand), so "
            "it cannot rejoin: a new life could repeat an earlier writer_epoch. Configure a new "
            "<cas_server_root_id> for this disk", srid, mount ? "epoch" : "mount");
    const MountLease slot = decodeMountLease(mount->bytes);
    if (!slot.gc_fenced && slot.min_active_build_sequence != std::numeric_limits<uint64_t>::max())
        throw Exception(ErrorCodes::ABORTED,
            "CAS server-root '{}' is retired and its mount slot is held (server_uuid={}, writer_epoch={}): a "
            "decommission of it is still running. Restart this server after the run has finished",
            srid, u128ToHex(slot.server_uuid), slot.writer_epoch);

    /// A late `IDisk::checkAccess` of the old life can land after the drain; nothing else of it may remain.
    const String probe = accessProbeKey(l, srid, owner.server_uuid);
    if (const std::optional<Meta> probe_meta = op.head(probe, Retry::standard()))
        (void)op.remove(probe, probe_meta->etag, Retry::standard());
    if (!serverRootSubtreeEmpty(op, l, srid, observe_catalog()))
        throw Exception(ErrorCodes::ABORTED,
            "CAS server-root '{}' is retired but data of its previous life remains: the decommission is not finished "
            "or GC has not yet deleted its catalog rows. Rerun SYSTEM CAS DROP POOL MEMBER '{}' FROM DISK '<disk>' "
            "and SYSTEM CAS GC RUN on a live member until the run reports namespaces_pending_gc = 0, then restart "
            "this server", srid, srid);

    const uint64_t life = owner.owner_life + 1;
    if (conflictOrThrow(
            op.replace(l.ownerKey(srid), encodeOwner(OwnerObject{.server_uuid = our_uuid, .retired_at_ms = std::nullopt, .owner_life = life}),
                       owner_etag, Retry::standard()),
            fmt::format("CAS server-root '{}' rejoin owner write", srid)))
        return std::nullopt;
    LOG_INFO(getLogger("CasMountLease"),
        "CAS server-root '{}': rejoined as an empty member under server_uuid {} at owner life {} (retired life {} of {})",
        srid, u128ToHex(our_uuid), life, owner.owner_life, u128ToHex(owner.server_uuid));
    return life;
}
}
```
  `claimOwnerOrThrow`: replace the present-owner block (`if (const std::optional<OwnerObject> owner = readOwnerObject(op, l, srid)) { … }`)
  with a decision loop; the foreign-owner `throw` text gains, at its end, the sentence
  `" Once that server is decommissioned (SYSTEM CAS DROP POOL MEMBER), a restart of this server rejoins the id as an empty member."`:

```cpp
    /// Every lost rejoin write means another start or a decommission run wrote the owner or the slot; a few
    /// fresh decisions settle any such race.
    constexpr size_t max_owner_decisions = 4;
    for (size_t decision = 1;; ++decision)
    {
        const std::optional<Object> current = op.read(key, Retry::standard());
        if (!current)
            break;
        const OwnerObject owner = decodeOwner(current->bytes);
        if (owner.retired_at_ms && retired_policy == RetiredOwnerPolicy::RejoinIfVerified)
        {
            if (const std::optional<uint64_t> life = rejoinRetiredOwner(op, l, srid, our_uuid, owner, current->etag, observe_catalog))
                return *life;
            if (decision == max_owner_decisions)
                throw Exception(ErrorCodes::ABORTED,
                    "CAS server-root '{}': the owner or the mount slot kept changing during {} rejoin attempts; "
                    "restart to try again", srid, max_owner_decisions);
            continue;
        }
        if (owner.server_uuid == our_uuid)
        {
            if (retired_policy != RetiredOwnerPolicy::AcceptForDecommission)
                throwIfOwnerRetired(owner, srid, key);
            return owner.owner_life;
        }
        throw Exception(ErrorCodes::CORRUPTED_DATA, /* the existing foreign-owner text, plus the sentence above */ …);
    }
```
  In the absent-owner tail, the race re-read's `if (retired_policy == RetiredOwnerPolicy::Refuse)` becomes
  `if (retired_policy != RetiredOwnerPolicy::AcceptForDecommission)`. Keep the existing `/// Mirror
  mountDoubleStartMessage's operator guidance` comment above the foreign throw.

  `CasPool.cpp` `Pool::mountWritable`: the claim passes its own policy; the recheck keeps `retired_policy`:

```cpp
    const uint64_t owner_life = claimOwnerOrThrow(owner_op, store->pool_layout, srid, our_uuid, observe_catalog,
        policy == MountClaimPolicy::NoWait ? RetiredOwnerPolicy::AcceptForDecommission : RetiredOwnerPolicy::RejoinIfVerified);
```
  `CasPool.h`, comment of `Pool::open`, append: "A start over a verified, drained retirement of its
  `server_root_id` rejoins it as an empty member (`claimOwnerOrThrow`)."

- [ ] **Step 5: run to verify they pass**, then the whole release `CAS*` gate. Any other newly failing test
  asserted that a start over a verified, drained retirement is refused: report its name and assertion before
  changing it, and change it only to assert the rejoin (R11 is the one known case).
- [ ] **Step 6: commit.**

```bash
git add -- $CA/Pool/CasServerRoot.h $CA/Pool/CasServerRoot.cpp $CA/Pool/CasPool.h $CA/Pool/CasPool.cpp \
  src/Disks/tests/gtest_cas_decommission.cpp src/Disks/tests/gtest_cas_mount.cpp
git commit -m "Rejoin a decommissioned CAS server root at server start

A start over a verified retirement whose data is gone takes the owner at
the next life under its own server_uuid, same or new, and continues the
kept epoch counter. It deletes the old life's access probe first. An
unverified tombstone, a held slot, leftover data or missing control
objects refuse with the action to take. Remounts never rejoin.

Signed-off-by: Mikhail Filimonov <mfilimonov@altinity.com>
Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 3: a new-uuid rejoin rewrites the mount body before the owner

**Why separate:** without it new binaries are already safe (Task 1's owner read). This step closes the
mixed-version case: a 26.6.4 starter of the old uuid that passed its owner check before the decommission
reclaims the old farewell by `If-Match`, and 26.6.4 has no claim recheck. Model: `sab_r8_ownerfirst` red.

**Files:**
- Modify: `$CA/Pool/CasServerRoot.cpp` (`rejoinRetiredOwner`)
- Test: `src/Disks/tests/gtest_cas_decommission.cpp` (after `CASRejoin.RemountDoesNotRejoin`)

**Interfaces:**
- Consumes: Task 2's `rejoinRetiredOwner` and its `mount` read.
- Produces: no new symbol. Behaviour: when `our_uuid != owner.server_uuid`, the terminal body is rewritten
  `{our_uuid, same writer_epoch, clean, seq + 1}` by `If-Match` on the read ETag before the owner write; a
  lost `If-Match` returns nullopt.

- [ ] **Step 1: write the failing tests:**

```cpp
/// A 26.6.4 starter of the old uuid read the farewell before the rejoin and reclaims it by `If-Match` with
/// no owner recheck; it fires just before the rejoin's owner write.
TEST(CASRejoin, NewUuidReplacesTheMountBeforeTheOwner)
{
    auto backend = std::make_shared<InMemoryBackend>();
    { auto victim = Pool::open(backend, victimConfig()); }
    retireVictim(backend, "admin");
    const Layout layout("p");
    OperationForTest raw_op(*backend);
    const auto old_farewell = (*raw_op).read(layout.mountKey("victim"), Retry::once());
    std::optional<WriteResult> old_claim;
    backend->onBeforeWrite(layout.ownerKey("victim"), [&]
    {
        if (old_claim)
            return;
        MountLease body = decodeMountLease(old_farewell->bytes);
        body.writer_epoch += 1;
        body.min_active_build_sequence = 0;
        body.seq += 1;
        OperationForTest op(*backend);
        old_claim = (*op).replace(layout.mountKey("victim"), encodeMountLease(body), old_farewell->etag, Retry::once());
    });

    auto rejoined = Pool::open(backend, victimConfig(UInt128(77)));

    ASSERT_TRUE(old_claim.has_value());
    EXPECT_TRUE(std::holds_alternative<Conflict>(*old_claim)) << "the rejoin rewrote the mount body first";
    EXPECT_EQ(readSlot(*backend, layout, "victim").server_uuid, UInt128(77));
}

/// A decommission rerun claims the slot between the rejoin's reads and its first write: the rejoin's
/// `If-Match` fails, the next decision sees the admin's live body and refuses; the owner stays retired.
TEST(CASRejoin, DecommissionRerunDuringARejoinWins)
{
    auto backend = std::make_shared<InMemoryBackend>();
    { auto victim = Pool::open(backend, victimConfig()); }
    retireVictim(backend, "admin");
    const Layout layout("p");
    const OwnerObject before = readOwner(*backend, layout, "victim");
    PoolPtr rerun_admin;
    bool fired = false;
    const auto admin_claims = [&]
    {
        if (std::exchange(fired, true))
            return;
        rerun_admin = Pool::openForDecommission(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "admin2"}, "victim");
    };
    backend->onBeforeWrite(layout.mountKey("victim"), admin_claims);
    backend->onBeforeWrite(layout.ownerKey("victim"), admin_claims);

    expectThrowsCodeWithMessage(ErrorCodes::ABORTED, "still running", [&] { Pool::open(backend, victimConfig(UInt128(77))); });

    ASSERT_TRUE(rerun_admin != nullptr);
    const OwnerObject after = readOwner(*backend, layout, "victim");
    EXPECT_EQ(after.server_uuid, before.server_uuid);
    EXPECT_EQ(after.owner_life, before.owner_life);
    EXPECT_TRUE(after.retirement_verified);
    EXPECT_EQ(readSlot(*backend, layout, "victim").writer_epoch, rerun_admin->writerEpoch());
}
```

- [ ] **Step 2: run to verify they fail.** `--gtest_filter='CASRejoin.NewUuidReplaces*:CASRejoin.DecommissionRerun*'`.
  Expected: `NewUuidReplacesTheMountBeforeTheOwner` FAILED (`old_claim` is `Committed`, then `Pool::open`
  throws `ABORTED`: a live foreign body); `DecommissionRerunDuringARejoinWins` FAILED (the owner write landed,
  the owner moved to uuid 77; the open fails with the double-start message, not "still running").
- [ ] **Step 3: implement.** In `rejoinRetiredOwner`, between the emptiness check and the owner write:

```cpp
    /// A new uuid takes the slot before the owner. A 26.6.4 starter of the old uuid that passed its owner
    /// check before the decommission then loses its `If-Match` on the old farewell instead of running under
    /// our owner: it has no claim recheck.
    if (our_uuid != owner.server_uuid)
    {
        MountLease ours = slot;
        ours.server_uuid = our_uuid;
        ours.gc_fenced = false;
        ours.min_active_build_sequence = std::numeric_limits<uint64_t>::max();
        ours.seq += 1;
        if (conflictOrThrow(op.replace(l.mountKey(srid), encodeMountLease(ours), mount->etag, Retry::standard()),
                            fmt::format("CAS server-root '{}' rejoin mount write", srid)))
            return std::nullopt;
    }
```
- [ ] **Step 4: run** `--gtest_filter='CASRejoin.*:CASMount*:CASDecommission*'`; all green.
- [ ] **Step 5: commit.**

```bash
git add -- $CA/Pool/CasServerRoot.cpp src/Disks/tests/gtest_cas_decommission.cpp
git commit -m "Rewrite the CAS mount body before the owner on a new-uuid rejoin

A 26.6.4 starter of the old uuid that passed its owner check before the
decommission could otherwise reclaim the old farewell and run under the
new owner: 26.6.4 has no claim recheck. With the slot rewritten first its
If-Match fails.

Signed-off-by: Mikhail Filimonov <mfilimonov@altinity.com>
Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 4: `retired` in `system.cas_mounts`

**Files:**
- Modify: `$CA/Pool/CasServerRoot.cpp` (`listMounts` `:1230-1268`), `$CA/Pool/CasServerRoot.h` (`MountInfo` comment `:459-462`)
- Modify: `docs/en/operations/system-tables/cas_mounts.md` (`state` column)
- Modify: `tests/integration/test_cas_drop_pool_member/test.py` (case (8))
- Test: `src/Disks/tests/gtest_cas_mount.cpp` (after `CASMount.ClaimPathsReclaimAForeignTerminalBodyOfTheirOwnOwner`)

**Interfaces:**
- Produces: `MountInfo::state` gains `retired`: a `terminated` or `fenced` row whose owner has `retired_at_ms`.

- [ ] **Step 1: write the failing test** after `CASMount.ClaimPathsReclaimAForeignTerminalBodyOfTheirOwnOwner`
  (Task 1), where `rootConfig`, `tombstoneOwnerAt`, `rewriteOwnerAt`, `fenceSlot` and `liveBody` are defined:

```cpp
/// A terminal row reads its owner: a tombstone makes it `retired`; a live row is never re-read.
TEST(CASListMounts, TerminalSlotOfATombstonedOwnerIsRetired)
{
    const auto state_of = [](Ops & ops, const Layout & l, const String & srid) -> String
    {
        for (const MountInfo & m : listMounts(ops.op, l, /*now_ms=*/0, /*skew_margin_ms=*/0))
            if (m.srid == srid)
                return m.state;
        return "absent";
    };
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    Ops ops(b);
    { auto first = Pool::open(b, rootConfig()); }
    EXPECT_EQ(state_of(ops, l, "r"), "terminated");
    tombstoneOwnerAt(*b, l, "r");
    EXPECT_EQ(state_of(ops, l, "r"), "retired");
    fenceSlot(*b, l.mountKey("r"));
    EXPECT_EQ(state_of(ops, l, "r"), "retired") << "a fenced slot of a retired owner";
    rewriteOwnerAt(*b, l, "r", UInt128(2));
    EXPECT_EQ(state_of(ops, l, "r"), "fenced") << "a rejoined owner";

    mustCommit(ops.op.create(l.ownerKey("x"), encodeOwner(OwnerObject{
        .server_uuid = UInt128(1), .retired_at_ms = 5, .owner_life = 3}), Retry::standard()), "retired owner of x");
    mustCommit(ops.op.create(l.mountKey("x"), encodeMountLease(liveBody(UInt128(1), 4)), Retry::standard()), "live slot of x");
    EXPECT_EQ(state_of(ops, l, "x"), "live") << "a non-terminal row keeps its lease state";
}
```

- [ ] **Step 2: run to verify it fails:** `--gtest_filter='CASListMounts.*'`. Expected:
  `TerminalSlotOfATombstonedOwnerIsRetired` FAILED (`terminated` where `retired` is expected).
- [ ] **Step 3: implement.** In `listMounts`, after the state chain and before `out.push_back`:

```cpp
        /// A decommission keeps the slot; the owner says whether the member behind a terminal row is retired.
        if (info.state == "terminated" || info.state == "fenced")
            if (const std::optional<OwnerObject> owner = readOwnerObject(op, layout, info.srid); owner && owner->retired_at_ms)
                info.state = "retired";
```
  `MountInfo` comment: after `` `fenced` (`gc_fenced`), `` add `` `retired` (terminated or fenced, owner tombstoned:
  a decommissioned member; one more `GET` per such row), ``. The `listMounts` comment "One LIST + one GET
  per slot" becomes "One LIST, one GET per slot, and one more per terminated or fenced slot".
  `cas_mounts.md` `state` line: `One of `live`, `expired`, `terminated`, `fenced`, `retired`, `corrupt`.
  `retired` is a terminated or fenced slot of a decommissioned member; a server started under that
  `server_root_id` rejoins it.`
  `test.py` case (8): comment `# (8) node2's mount slot stays and reads as `retired`: the decommission keeps
  the control objects and tombstones the owner.`; expected value `"retired"`.
- [ ] **Step 4: run** `--gtest_filter='CASListMounts.*:CASHeartbeatFloor.*'`; green.
- [ ] **Step 5: commit.**

```bash
git add -- $CA/Pool/CasServerRoot.cpp $CA/Pool/CasServerRoot.h src/Disks/tests/gtest_cas_mount.cpp \
  docs/en/operations/system-tables/cas_mounts.md tests/integration/test_cas_drop_pool_member/test.py
git commit -m "Show decommissioned CAS members as retired in system.cas_mounts

Signed-off-by: Mikhail Filimonov <mfilimonov@altinity.com>
Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 5: operator docs and the rejoin integration test

**Files:**
- Modify: `docs/en/antalya/cas/architecture/mounts-and-leases.md` (`#owner-claim`, `#claim-outcomes`,
  `#mount-state-machines`, `#mount-lifecycle`, `#mounts-table`)
- Modify: `docs/en/antalya/cas/operations/migration.md` (`#decommission`, new `#rejoin`, `#decommission-compatibility`)
- Modify: `docs/en/antalya/cas/operations/troubleshooting.md`, `docs/en/sql-reference/statements/system.md`
  (`#system-cas-drop-pool-member`)
- Modify: `tests/integration/test_cas_drop_pool_member/test.py`

- [ ] **Step 1: integration test first.** Add after `_assert_retired_or_racing_stop`:

```python
MANIFESTS_PREFIX = POOL + "/cas/manifests/" + SRID2 + "/"
ROOTS_PREFIX = POOL + "/roots/" + SRID2 + "/"


def _srid2_mount(node1):
    return tuple(
        node1.query(
            "SELECT state, server_uuid FROM system.cas_mounts "
            "WHERE server_root_id = '{}' AND disk = '{}'".format(SRID2, CA_DISK)
        )
        .strip()
        .split("\t")
    )


def _retire_srid2(node1):
    # A rejoin needs a verified retirement, no catalog row left for GC and no object left under the id.
    report_tsv = ""
    for _ in range(60):
        report_tsv, fields = _drop_member(node1)
        if (
            int(fields[9]) == 1
            and int(fields[3]) == 0
            and _count(MANIFESTS_PREFIX) == 0
            and _count(ROOTS_PREFIX) == 0
        ):
            return
        node1.query("SYSTEM CAS GC RUN '{}'".format(CA_DISK))
        time.sleep(1.0)
    pytest.fail("node2 never reached a drained, verified retirement: {}".format(report_tsv))
```
  and a new test at the end of the file:

```python
def test_retired_member_rejoins_empty():
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    # Start from a retired node2 whatever ran before: a stop of a stopped server only warns.
    node2.stop_clickhouse(kill=True)
    _retire_srid2(node1)
    assert _srid2_mount(node1)[0] == "retired"

    # Same uuid: the restart rejoins, then the member writes a table of its own.
    node2.start_clickhouse()
    state, old_uuid = _srid2_mount(node1)
    assert state == "live"
    node2.query("DROP TABLE IF EXISTS t3 SYNC")
    node2.query(
        "CREATE TABLE t3 (id Int64, s String) ENGINE = MergeTree() ORDER BY id "
        "SETTINGS storage_policy = '{}'".format(STORAGE_POLICY)
    )
    node2.query("INSERT INTO t3 SELECT number, toString(number) FROM numbers(1000)")
    assert int(node2.query("SELECT count() FROM t3")) == 1000

    node2.stop_clickhouse()
    _retire_srid2(node1)
    assert _srid2_mount(node1)[0] == "retired"

    # The same uuid comes back empty: the local table definition attaches with no rows and takes writes.
    node2.start_clickhouse()
    assert _srid2_mount(node1) == ("live", old_uuid)
    assert int(node2.query("SELECT count() FROM t3")) == 0
    node2.query("INSERT INTO t3 SELECT number, toString(number) FROM numbers(10)")
    assert int(node2.query("SELECT count() FROM t3")) == 10

    # A new uuid (the local uuid file regenerated) rejoins the same id, also empty.
    node2.stop_clickhouse()
    _retire_srid2(node1)
    node2.exec_in_container(["rm", "-f", "/var/lib/clickhouse/uuid"], user="root")
    node2.start_clickhouse()
    state, new_uuid = _srid2_mount(node1)
    assert state == "live"
    assert new_uuid != old_uuid
    assert int(node2.query("SELECT count() FROM t3")) == 0

    node2.query("DROP TABLE t3 SYNC")
```
  Run from `$WT`: `python -m ci.praktika run "integration" --test test_cas_drop_pool_member > $WT/build/test_integration_rejoin.log 2>&1`;
  a subagent reads the log. Expected on Task 4's tree: both old tests pass; the new test passes too, since the
  behaviour landed in Tasks 2–4 (this test is the end-to-end proof, not a red-first unit). If `t3` fails to
  attach after a rejoin, or the start fails with `previous life remains` after `_retire_srid2` returned, stop
  and report: the first is the spec's `#checks` item (an old table definition must attach empty), the second
  is a fixture or GC finding, and neither is to be patched around.

- [ ] **Step 2: `mounts-and-leases.md`.**
  - `#owner-claim` opening paragraph: "`claimOwnerOrThrow` binds `server_root_id` to one `server_uuid` per
    owner life. The owner object is never deleted. Decommission tombstones it in place; a server start over a
    verified, drained retirement takes it over at the next life ([rejoin](#rejoin))."
  - Table: replace the `retired_at_ms set` row with two rows:
    `| present, retired_at_ms set, retirement_verified | a server start rejoins (below); a remount fails with CORRUPTED_DATA; the decommission admin accepts its victim's tombstone |`
    `| present, retired_at_ms set, not verified | CORRUPTED_DATA for a server start or remount: the decommission is not finished; the decommission admin accepts it |`
    The `different server_uuid` row: `| present, different server_uuid, not tombstoned | CORRUPTED_DATA — names the regenerated-uuid-file cause |`.
  - The bullet "A **second server with a different `server_uuid`** is refused at this gate and can never take
    over, regardless of lease expiry." ends instead with "and takes over only by a rejoin after the id's
    decommission is verified and drained."
  - New subsection right after the owner-claim bullets:

```markdown
### Rejoin at start {#rejoin}

A server whose `server_root_id` was decommissioned rejoins on its next start, with the same or a new
`server_uuid`. There is no command. It comes back empty: the decommission deleted the data of the
previous life.

`claimOwnerOrThrow` rejoins only from `Pool::open`, never from a self-remount, and only when:

- the owner carries `retirement_verified`;
- `epoch` and `mount` exist and the slot body is terminal (clean farewell or `gc_fenced`);
- the subtree is empty, after deleting the old life's access-probe key `roots/<server_root_id>/clickhouse_access_check_<uuid>`.

Then a new `server_uuid` first rewrites the terminal `mount` body under its own uuid, and the owner becomes
`{server_uuid, owner_life + 1}` by `If-Match`. A lost `If-Match` re-reads the owner and decides again. The
kept counter gives the new life an epoch above every earlier one.

| Refusal | Code | Cause |
|---|---|---|
| `decommission is not finished` | `CORRUPTED_DATA` | the tombstone is not verified |
| `cannot rejoin` | `CORRUPTED_DATA` | `epoch` or `mount` missing: retired by 26.6.4, or deleted by hand |
| `still running` | `ABORTED` | a decommission run holds the slot |
| `previous life remains` | `ABORTED` | catalog rows, manifests or mountpoint objects of the old life are left |
```
  - `#claim-outcomes`: the `ForeignOwner` row becomes `| ForeignOwner | different server_uuid — refused, except a terminal body below the claimer's epoch when a fresh owner read shows the claimer's uuid and life (a server start or self-remount after a rejoin) |`.
  - State diagram: add `Terminated --> Live: rejoin of a verified, drained retirement, any server_uuid` and
    `Fenced --> Live: rejoin of a verified, drained retirement, any server_uuid` after the decommission edge.
  - `#mount-lifecycle` "Permanent removal" paragraph: append "A later start under that `server_root_id`
    rejoins it as an empty member ([rejoin](#rejoin))."
  - `#mounts-table` `state` row: `one of live, expired, terminated, fenced, retired, corrupt; retired = terminated or fenced with a tombstoned owner (one more GET per such row)`,
    with each value in backticks; the opening sentence "one `LIST` … plus one `GET` per slot" gains ", plus one
    owner `GET` per terminated or fenced slot".
- [ ] **Step 3: `migration.md`.**
  - `#decommission` first paragraph: "Scaling down — removing a server and its data from the pool — is a
    distinct operation from an ordinary restart or a temporary outage: it fences the member's current life
    and reclaims the storage attributable only to it. The data is gone for good; the `server_root_id` can
    rejoin later as an empty member ([rejoin](#rejoin))."
  - **Preconditions**: "the operation fences that `server_root_id` out even if the server comes back online"
    → "a server that comes back under that id after the run starts empty".
  - **Verification**: "shows the member's row as `terminated`" → "shows the member's row as `retired`".
  - New subsection before `#decommission-compatibility`:

```markdown
### Bringing a member back {#rejoin}

Restart the server. A server whose `server_root_id` was decommissioned rejoins the pool on its next start,
with the same or a new `server_uuid`. It comes back empty: its local table definitions attach with no data.

The start rejoins only after the decommission finished:

- the last run reported `member_retired = 1`;
- a rerun reports `namespaces_pending_gc = 0` (run `SYSTEM CAS GC RUN` between reruns);
- `system.cas_mounts` shows the member as `retired`.

| Start fails with | Action |
|---|---|
| `decommission is not finished` | rerun `SYSTEM CAS DROP POOL MEMBER` until `member_retired = 1`, then restart |
| `previous life remains` | rerun the decommission and `SYSTEM CAS GC RUN` until `namespaces_pending_gc = 0`, then restart |
| `still running` | wait for the running decommission, then restart |
| `cannot rejoin` | configure a new `cas_server_root_id`: the id was retired by 26.6.4 or its control objects were deleted |

After a decommission run that printed no result (the tool was killed), check `system.cas_mounts` before
rerunning: a `live` row is a member that already rejoined, and a rerun would delete its data.
```
  - `#decommission-compatibility` table: the first row's behaviour ends with "; it cannot rejoin"; add rows:
    `| Downgrade: a server_root_id rejoined under a new server_uuid | start it once on this version before downgrading; 26.6.4 then starts it normally |`,
    `| Downgrade: a server_root_id rejoined under the same server_uuid | 26.6.4 starts it and continues the counter |`,
    `| Mixed versions: a 26.6.4 server starts with a retired server_root_id | refused, as in 26.6.4: it cannot rejoin |`.
- [ ] **Step 4: `troubleshooting.md`**: add a row after the `DROP POOL MEMBER` row:
  `| A server fails to start with decommission is not finished, previous life remains, still running or cannot rejoin | Its server_root_id was decommissioned and the rejoin's precondition does not hold yet | See [bringing a member back](/antalya/cas/operations/migration#rejoin) |`
  (code names in backticks). **`system.md`** `#system-cas-drop-pool-member`: "fencing that `server_root_id`
  from ever writing again" → "fencing the member's current life"; "since it fences the member out even if it
  later comes back online, and it deletes" → "since it deletes"; append to the paragraph: "A server started
  later under the same `server_root_id` rejoins as an empty member
  ([details](/antalya/cas/operations/migration#rejoin))."
- [ ] **Step 5: prose checks.** `git grep -n -i -E 'never rejoin|ever writing again|comes back online|never reassigned' -- docs/en`
  → no hit. Every new heading has `{#…}`. Read each changed paragraph against STYLE.md once.
- [ ] **Step 6: run the integration module** as in Step 1 and record the result.
- [ ] **Step 7: commit.**

```bash
git add -- docs/en/antalya/cas/architecture/mounts-and-leases.md docs/en/antalya/cas/operations/migration.md \
  docs/en/antalya/cas/operations/troubleshooting.md docs/en/sql-reference/statements/system.md \
  tests/integration/test_cas_drop_pool_member/test.py
git commit -m "Document CAS member rejoin and test it end to end

Re-adding a decommissioned member is a restart of the server, with the
same or a new server_uuid; it comes back empty. The integration test
retires node2, restarts it under the same uuid and then a new one, and
checks its table attaches empty each time.

Signed-off-by: Mikhail Filimonov <mfilimonov@altinity.com>
Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 6: gates and hand-off

- [ ] Release gate: `CAS*` in `$WT/build`. ASan gate: build `unit_tests_dbms` in `$WT/build_asan` (same
  `flock`, log `build_asan/build_gate.log`), run `CAS*` with `TMPDIR=$WT/build_asan/tmp`. Both green; compare
  the pass count with `$WT/tmp/baseline.txt` (it grows by the new tests, nothing disappears).
- [ ] `git grep -n -E 'plan|BACKLOG|CAS-[0-9]+|review|finding|sab_|r[0-9]+_' -- $CA/Pool/CasServerRoot.cpp $CA/Pool/CasServerRoot.h $CA/Pool/CasPool.cpp`
  on the lines this PR added (`git diff 81e5e081ea5 -- <file>`) → no reference to an internal document.
- [ ] `git diff 81e5e081ea5 --stat` lists only the files in "File structure".
- [ ] PR body from `.github/PULL_REQUEST_TEMPLATE.md`, category "Improvement", changelog entry: "A CAS server
  whose `server_root_id` was decommissioned with `SYSTEM CAS DROP POOL MEMBER` rejoins the pool as an empty
  member on its next start, with the same or a new server uuid. `system.cas_mounts` shows decommissioned
  members as `retired`." Base: `antalya-26.6`, after PR B merged (`Related:` PR B
  https://github.com/Altinity/ClickHouse/pull/2567). Do not push: the owner pushes.
- [ ] Hand-off, not run here: the CAS-213 soak (decommission under load through the SQL verb, kill the tool at
  each step and rerun, then restart the member and check it rejoins empty).

---

## Self-review

**Spec coverage.** D6 first half: Task 2 (rejoin in `claimOwnerOrThrow` from `Pool::open` only, any uuid,
verified only, terminal slot, counter kept, emptiness after the probe delete, owner by `If-Match`, conflict
re-decides) and Task 3 (new uuid rewrites the mount first). D6 second half: Task 1 (foreign terminal reclaim
after a fresh owner read requiring `(our uuid, recorded life, not retired)`; live foreign body stays
`ForeignOwner`; both contracts' comments). `#rejoin` "remount never rejoins": `RemountDoesNotRejoin`.
`#rejoin` concurrent starters: `ConcurrentStartersWithDifferentUuidsOneWins`. `#rejoin` delayed lower epoch:
`DelayedLowerEpochClaimAfterRejoinIsRefused` plus `ForeignTerminalBodyAtOrAboveOurEpochStaysForeign`.
`#rejoin` victim during decommission: `VictimRefusedAfterFarewellRejoinsOnItsNextStart`. Access probe (CAS-371
part): Task 2. `#operator` `retired`: Task 4. `#operator` refusal texts for rejoin: R4, Task 2. `#checks` old
table definitions attach empty: Task 5 integration. `#checks` "`throwIfOwnerRetired` remains only for the
cases rejoin refuses": Task 2 (unverified, remount). `#compat` rejoin and downgrade rows: Task 5 Step 3.
`#tests` «PR C»: every listed name has a step; integration in Task 5; soak is a hand-off (R14).
Not covered by design: PR A items, fsck, CAS-369.1, binding reruns to the original life (CAS-385).

**Placeholder scan.** The foreign-owner throw in Task 2 Step 4 says "the existing text plus the sentence
above": the existing text is unchanged PR B code and the sentence is given verbatim. Every test step carries its full
body; helpers used before their definition were checked against file order (Task 4's test sits after Task 1's
block for that reason).

**Type consistency.** `OwnerExpectation{server_uuid, owner_life, retired}` (PR B) is what `ownerRefusal`,
`claimMount`'s `foreign_reclaim_owner`, the `mountWritable` and `tryRemountOnce` wiring and the tests use.
`RetiredOwnerPolicy::RejoinIfVerified` (Task 2) is used by `mountWritable` and
`ConflictRereadAppliesTheRetiredPolicy`. `rejoinRetiredOwner(op, l, srid, our_uuid, owner, owner_etag, observe_catalog) -> std::optional<uint64_t>`
(Task 2) is what Task 3 edits. `victimConfig`, `retireVictim`, `rewriteSlot`, `gcUntilVictimSubtreeEmpty`,
`victimSubtreeEmpty` are defined once in Task 2 and reused by Task 3. `terminalBody`, `liveOwner`,
`rewriteSlotUuid` (Task 1) and `tombstoneOwnerAt`, `rewriteOwnerAt`, `fenceSlot`, `seedOwner`, `liveBody`,
`slotBody`, `rootConfig` (PR B) live in `gtest_cas_mount.cpp`.

**Review Focus.** Each line has its test in the named task: 1 → Task 3, 2–4 → Task 2, 5 → Task 1.
