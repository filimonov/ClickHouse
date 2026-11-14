# CAS one-request ref-log append: implementation plan {#cas-one-request-ref-append-plan}

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan
> task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** An ordinary CAS ref-log append becomes one create-if-absent of `_log/{epoch,seq}`; `_ckpt` is written
only at namespace creation and at snapshot publication; an epoch change is recorded by a forward-linked tombstone.

**Architecture:** Eleven commits on one branch, readers before writers. Commits 1 to 4a add the generation 3 codecs,
the shared chain walk, the GC cut and the lease marker while every writer keeps today's rule and `G_BUILD` stays 2.
Commit 5c switches the writer and advertises generation 3 in the same commit; commit 6 makes the tombstone lazy;
commit 7 deletes what the switch left dead.

**Tech Stack:** C++ (`CA/` metadata storage of the ClickHouse fork), GoogleTest (`unit_tests_dbms`), stateless and
integration tests, Markdown docs.

**Spec:** `/home/mfilimonov/workspace/ClickHouse/master/docs/superpowers/specs/2026-11-14-cas-one-request-ref-append-design.md`
(revision 4, commit `f4cd2fa2d75`; binding: a conflict between this plan and the spec resolves to the spec). "§N"
below is a section of that spec. Part B (§7) and TLA+ work are out of scope.

## Global Constraints {#global-constraints}

### Paths {#paths}

| Name | Value |
|---|---|
| `WT` | `/home/mfilimonov/workspace/ClickHouse/cas-pr1`, branch `work/antalya-26.8/cas-reflog-one-request`, base `802077e5c1a` |
| `M` | `/home/mfilimonov/workspace/ClickHouse/master` (spec, models `docs/superpowers/models/`, this plan) |
| `R` | `$M/tmp/reflog1` (ledger, briefs, `reports/task<N>.md`, saved red logs `logs/`) |
| `CA/` | `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/` |
| `T/` | `src/Disks/tests/` |
| `D/` | `docs/en/antalya/cas/` |

Every `path:line` in this plan was read in `WT` at `802077e5c1a`. Lines move as tasks land: locate by the named
symbol, use the line as a hint. In commands, `CA`, `T` and `D` are shell variables with the values above, without
the trailing slash.

### Binding rules {#binding-rules}

- **Worktree.** Modify files only under `WT`. Never touch another worktree. No push, no pull request. The only
  writes outside `WT` are reports and saved logs under `$R`.
- **No destructive commands.** No `git clean`, `git reset --hard`, `git checkout -- <file>`, `git stash`,
  `git rebase`, `git commit --amend`. Never delete or re-create a build directory. A hung test binary is stopped
  by its exact PID.
- **C++.** Allman braces. No `sleep` to fix a race. No fallback paths: let errors propagate. Say "exception", not
  "crash", for logical errors. Function names in prose and comments as `f`, not `f()`. Comments explain why, in
  one or two lines, and carry no task ids, no history, no references to plans or reviews.
- **`LOGICAL_ERROR` tests.** A test that expects `LOGICAL_ERROR` is a death test in a `*DeathTest` suite: debug
  and sanitizer builds abort on it. Death suites must not construct `DiskObjectStorage` in the parent process.
  No task below adds a `LOGICAL_ERROR`; an implementer who needs one follows this rule.
- **Builds.** Build directories that exist: `build` (plain), `build_asan` (ASan + UBSan), `build_fast`. No `-j`.
  The log goes to a file in the build directory; read it with `grep -a` or a subagent, never by pasting it.

  ```bash
  cd $WT/build && ninja unit_tests_dbms > build_<tag>.log 2>&1; \
    echo "NINJA_EXIT=$? HEAD=$(git -C $WT rev-parse --short=11 HEAD)" >> build_<tag>.log
  cd $WT/build && ./src/unit_tests_dbms --gtest_filter='<filter>' > test_<tag>.log 2>&1; \
    echo "GTEST_EXIT=$?" >> test_<tag>.log
  grep -a 'NINJA_EXIT' $WT/build/build_<tag>.log; grep -a -c ' error: ' $WT/build/build_<tag>.log
  grep -a '\[  PASSED  \]\|\[  FAILED  \]\|GTEST_EXIT' $WT/build/test_<tag>.log | tail -20
  ```

  Tags: `t<task>_red`, `t<task>_green`, `t<task>_gate`; the same with `build_asan` for the ASan gate
  (`t<task>_asan`). A test log is evidence only if the build log of the same tag ends with `NINJA_EXIT=0`.
- **Tests.** During a task a narrower `--gtest_filter` is fine. The gate at the end of every task is EXACTLY
  `--gtest_filter='CAS*'` on the plain build, and the same on `build_asan` where the task says "ASan gate"
  (tasks that touch the lane, recovery, GC or the lease). The pass count is the `PASSED` number.
- **Red first.** Write the test before the product change and run it. The red run fails on an assertion, not on
  a compile error: a new signature is first added as a stub with today's behaviour. Copy the red log to
  `$R/logs/red_t<task>.log`. A test marked "pin" is green before and after.
- **Re-targeting an existing test** that fails after a product change: (a) it pins a mechanism this change
  removes (§2.6): delete it, or rewrite it to the replacing rule when §10 has no test for that case; (b) it
  asserts a `_ckpt` field or a request count as a side detail: re-target the assertion; (c) anything else is a
  product defect: stop and report. The task report lists every deleted or rewritten test with its class.
- **Stateless tests** are created with `./tests/queries/0_stateless/add-test <name>`; no `no-*` tags unless
  strictly needed; a failpoint test needs the `no-fasttest` tag.
- **Docs** under `docs/`: every heading has an explicit `{#anchor}`; no invariant tags in user docs.
- **Formats.** The on-store format of generations 1 and 2 stays readable. No change to non-CAS (upstream) code
  unless a task names the file and says why.
- **Scope.** Change only the files the task lists. A defect found elsewhere goes into the report, not the diff.
- **Commit.** Exactly one commit per task, by exact file list, without `-s` (the trailers are written by hand so
  that their order is fixed). The commit step of a task gives the `git add` line for its fixed files; test files
  changed by the re-targeting rule are added by name and listed in the report. Never `git add -A` or `git add .`.
  After the commit `git status --short` prints only `?? config_file_for_test.xml`.

  ```bash
  cd $WT && git add <files> && git diff --cached --name-only   # must equal the list: fixed files + re-targeted tests
  git commit -F - <<'EOF'
  <subject from the task, at most 72 characters>

  <body from the task: the why, literal names in backticks>

  Signed-off-by: Mikhail Filimonov <mfilimonov@altinity.com>
  Co-Authored-By: Claude Fable 5.2 <noreply@anthropic.com>
  EOF
  ```

  No other trailer, no session link, no backlog id, none of the words "spec", "plan", "task", "review".
- **Report.** `$R/reports/task<N>.md`: what changed, red evidence, pass counts (plain, ASan), deleted or
  rewritten tests, deviations, doubts. Implementers do not dispatch subagents.

### Test vocabulary {#test-vocabulary}

- `ScopedReadCeiling c(n)` (Task 1, `T/cas_test_helpers.h`): `setReadCeilingForTest(n)` for the scope. Tests that
  expect a refusal pin `ScopedReadCeiling(2)` explicitly, so they survive `G_BUILD = 3` in Task 5c.
- Raw-epoch fixture of `T/gtest_cas_ref_recovery_cas_walk.cpp`: `HidingListBackend` (`:149`; knobs `late_key` /
  `late_bytes`, `ambiguous_put_substr`, `before_cas_put`, `watched_substr` + `on_key`), `openWalkPool` (`:389`),
  `burnEpochsUpTo(backend, layout, W)` (`:402`), `seedTxn` (`:448`), `seedCkpt` (`:457`), `lifeEpochCkpt` (`:462`),
  `readLogTxn` (`:472`). `CountingBackend` (`T/cas_test_helpers.h:1941-1955`): `getCount(key)`, `writeCount(key)`,
  `putTotal`, `listTotal`.
- Notation in test steps: `{e,s}` is `RefTxnId{e, s}`; `T{e,s}->n` a tombstone at `{e,s}` with `next_epoch = n`;
  `S{e,s}` an old-format seal (no `next_epoch`); `H{e,1}<-{p,q}` a sequence-1 record whose backlink is `{p,q}`;
  "retry-later" is `ErrorCodes::NETWORK_ERROR` from `throwCasWriteRetryLater` (`CA/Backend/CasRequests.cpp:96`).

## Review Focus {#review-focus}

1. **A build of commits 1 to 4a opens a pool that commit 5c wrote.** It must refuse at mount, an already-mounted
   GC must hold the namespace, and it must not write an old `_ckpt` body over a generation 3 one. Owner: Task 1;
   pins `CASPoolMeta.FloorThreeIsRefusedBelowTheChainCeiling`,
   `CASGCBoundedWalk.HeaderThreeCkptUnderCeilingTwoHoldsTheNamespace`,
   `CASRefCheckpoint.OldContributionIntoAGenerationThreeBodyIsRefused`.
2. **The recovery tombstone lands in a slot cleanup reclaimed** (§5 row 21). Its create succeeds on a chain that
   is no longer the chain. Owner: Task 5c; `CASRefWriterRecovery.RestartsWhenTheBaseMovesUnderTheTombstoneCreate`,
   with the control that the same schedule without a base move installs.
3. **The marker at a self-remount.** A recovery under the new epoch that pairs it with the old marker installs
   lazily over an epoch that ended uncleanly. Owner: Task 4
   (`CASPoolRemount.SelfRemountPublishesTheRemountClaimsMarkerWithTheEpoch`) and Task 6
   (`CASPool.SelfRemountRecoveryUsesTheRemountClaimsMarker`).
4. **A GC cursor below the snapshot id with an object at its key** (§5 rows 7, 8). The object is a stray; reading
   it folds a record that is not history. Owner: Task 2 (`CASRefCursorSuccessor.CursorBelowTheSnapshotIsNeverRead`)
   and Task 3 (`CASGCBoundedWalk.StrayAtAReclaimedCursorKeyIsNotFolded`, `.StrayAfterAReclaimedTombstoneIsNotFolded`).
5. **A reader walks a life that is erased under it** (§5 row 22). A deleted key reads as the end and a late create
   as a record. Owner: Task 2 (fsck, `sweepNamespace`) and Task 5a (ledger recovery, presence probe); tests
   `LifeErasedDuringTheScanIsJanitorPending`, `SweepNamespaceSkipsALifeErasedUnderItsWalk`,
   `LifeErasedUnderTheWalkIsRemovedNotInstalled`, `PresenceProbeOverAnErasedLifeDoesNotThrow`.

---

### Task 0: Baseline (controller, no commit) {#task-0}

- [ ] **Step 1:** `cd $WT && git rev-parse --abbrev-ref HEAD` prints `work/antalya-26.8/cas-reflog-one-request`,
  `git rev-parse --short=11 HEAD` prints `802077e5c1a`, `git status --short` prints only
  `?? config_file_for_test.xml`.
- [ ] **Step 2:** Build and run the `CAS*` gate on `build` (tag `t0_gate`) and on `build_asan` (tag `t0_asan`).
  Record both `PASSED` numbers in `$R/LEDGER.md` as `B0` and `A0`. A red baseline stops the run.

### Task 1: Commit 1, generation 3 codecs {#task-1}

Implementer `ca-impl`. Reviewer `ca-review-lite`. Read §6 and §11. After this task nothing writes or admits
generation 3: `G_BUILD` stays 2 and every log or `_ckpt` a build writes keeps today's header.

**Files:**
- Modify: `CA/Formats/CasFormat.h:14-22`
- Modify: `CA/Formats/CasRefLogFormat.h:86-105` (`RefLogTxn`), `:125-131`, `:176-186`
- Modify: `CA/Formats/CasRefLogFormat.cpp:24-34` (wire keys), `:130-139` (`writeLogMeta`), `:230-260`
  (`validateEpochSealGrammarStructural`), `:288-305` (`encodeRefLogTxn`), `:307-415` (`decodeRefLogTxn`)
- Modify: `CA/Formats/CasRefCkptFormat.h:55-94` (`RefCkpt`), `:116-123`; `CA/Formats/CasRefCkptFormat.cpp:34-93`,
  `:95-127`, `:129-187`
- Modify: `CA/Pool/CasRefCkpt.cpp:152-171` (`mergeCkpt` carries the generation)
- Test: `T/cas_test_helpers.h`, `T/gtest_cas_ref_epoch_seal_format.cpp`, `T/gtest_cas_format.cpp`,
  `T/gtest_cas_ref_ckpt.cpp`, `T/gtest_cas_pool_meta.cpp`, `T/gtest_cas_gc_bounded_walk.cpp`

**Interfaces:**
- Consumes: `expectHeaderLine` returns `TextHeader{type, v}` (`CA/Formats/CasTextFormat.cpp:416-425`);
  `sniffHeaderLine(bytes)->v`; `JsonObjectReader::skipUnknown` throws `UNKNOWN_FORMAT_VERSION` on a `!` key (`:315-324`).
- Produces:

  ```cpp
  constexpr uint32_t G_REF_CHAIN = 3;                                     /// CasFormat.h, after G_REF_MOVE
  std::optional<uint64_t> RefLogTxn::next_epoch = std::nullopt;           /// declared last; wire key "!next_epoch"
  uint32_t refLogTxnHeaderGeneration(const RefLogTxn & txn);
  String encodeRefLogTxnAtGeneration(const RefLogTxn & txn, uint32_t header_generation);
  String encodeRefLogTxn(const RefLogTxn & txn);     /// = AtGeneration(txn, refLogTxnHeaderGeneration(txn))
  void validateEpochSealGrammarStructural(const RefLogTxn & txn, uint32_t header_generation);
  uint32_t RefCkpt::generation = 1;                                       /// declared last; the header it is read at or written with
  ```

- Rules the signatures do not state:
  - `refLogTxnHeaderGeneration`: `G_REF_CHAIN` iff `next_epoch` is set; else `G_REF_MOVE` iff the transaction has a
    `Move`; else `currentCompatibilityVersion()`. A head with `!prev_epoch` keeps header 1 or 2 until Task 5c.
  - Grammar at header 3: a backlink only at sequence 1 with `prev.writer_epoch < txn_id.writer_epoch` (no
    adjacency); an `EpochSeal` transaction must carry `next_epoch > txn_id.writer_epoch`; `next_epoch` on anything
    but a tombstone is `CORRUPTED_DATA`. At header 1 or 2: today's rules, and a set `next_epoch` is `CORRUPTED_DATA`.
  - `encodeRefLogTxnAtGeneration` refuses (`CORRUPTED_DATA`) a generation below what the content needs (a `Move`
    needs 2, `next_epoch` needs 3) or above `G_REF_CHAIN`. Plan decision: this explicit-generation encoder is how
    tests build a header 3 head before Task 5c and a header 1 seal or head after it; production calls `encodeRefLogTxn`.
  - `decodeRefLogTxn` validates with the header it read (`expectHeaderLine(...).v`).
  - `RefCkpt` at generation 3: `life_epoch` is required, `committed_through` and `last_epoch_seal` must be absent
    (`CORRUPTED_DATA` on encode and decode). At generation 1 or 2: today's invariants (`:34-93`). `encodeRefCkpt`
    writes the header `ckpt.generation` (1 to 3, else `CORRUPTED_DATA`); `decodeRefCkpt` sets it from the header.
    Plan decision: the generation is a field of the struct, default 1, so the 201 existing `RefCkpt{...}` sites
    keep writing today's body and `operator==` (the `IdenticalSkip` test, `CA/Pool/CasRefCkpt.cpp:313`) compares it.
  - `mergeCkpt`: `merged.generation = std::max(a.generation, b.generation)`, then the invariants of that generation.

- [ ] **Step 1: Write the failing tests**

  `T/cas_test_helpers.h`: `struct ScopedReadCeiling` and
  `inline RefLogTxn tombstoneTxn(const String & ns, RefTxnId id, std::optional<uint64_t> next_epoch, std::optional<RefTxnId> prev = std::nullopt)`.

  ```cpp
  /// T/gtest_cas_ref_epoch_seal_format.cpp, suite CASRefEpochSealFormat
  TEST(..., NextEpochMustExceedOwnEpoch)
  //  encodeRefLogTxn(tombstoneTxn(ns, {5,3}, 5)) and (..., 4): CORRUPTED_DATA
  //  ScopedReadCeiling c(3); decode(encode(tombstoneTxn(ns, {5,3}, 6))).next_epoch == 6; sniffHeaderLine(bytes)->v == 3
  TEST(..., NextEpochOnlyOnATombstone)
  //  RefLogTxn birth{ns, {5,3}, {namespaceBirthOp()}, std::nullopt}; birth.next_epoch = 6; encode: CORRUPTED_DATA
  TEST(..., HeaderThreeTombstoneRequiresNextEpoch)
  //  seal = tombstoneTxn(ns, {5,3}, std::nullopt): encodeRefLogTxnAtGeneration(seal, 3): CORRUPTED_DATA
  //  sniffHeaderLine(encodeRefLogTxn(seal))->v == 1 and it decodes under ScopedReadCeiling(2)
  TEST(..., SparseBacklinkNeedsHeaderThree)
  //  head{ns, {7,1}, ops, RefTxnId{4,9}}: encodeRefLogTxnAtGeneration(head, 1): CORRUPTED_DATA (header 1 demands
  //  epoch 6); ScopedReadCeiling c(3); decode(encodeRefLogTxnAtGeneration(head, 3)).prev_epoch_seal == RefTxnId{4,9}
  TEST(..., ReadCeilingTwoRejectsHeaderThree)
  //  bytes = encodeRefLogTxn(tombstoneTxn(ns, {5,3}, 7)); ScopedReadCeiling c(2); decode: UNKNOWN_FORMAT_VERSION
  TEST(..., BacklinkHeadKeepsItsHeaderBeforeTheWriterCommit)
  //  head{ns, {5,1}, ops, RefTxnId{4,2}}: refLogTxnHeaderGeneration == 1, header v == 1; with a Move op: 2
  TEST(..., HeaderBelowThreeRejectsNextEpoch)
  //  encodeRefLogTxnAtGeneration(tombstoneTxn(ns, {5,3}, 7), 1): CORRUPTED_DATA
  //  header-3 text of that tombstone with "\"v\":3" replaced by "\"v\":1": decode: CORRUPTED_DATA

  /// T/gtest_cas_ref_ckpt.cpp, suite CASRefCheckpoint
  TEST(..., DecodesGenerationsOneToThree)
  //  g1 = RefCkpt{.life_epoch = 2, .committed_through = RefTxnId{2,5}, .checkpoint_snapshot_id = RefTxnId{2,4}}
  //  decode(encode(g1)) == g1 and .generation == 1; the same text with "\"v\":2" decodes with generation == 2
  //  g3 = RefCkpt{.life_epoch = 2, .checkpoint_snapshot_id = RefTxnId{2,4}}; g3.generation = 3
  //  sniff(encode(g3))->v == 3; ScopedReadCeiling(3): decode == g3; ScopedReadCeiling(2): UNKNOWN_FORMAT_VERSION
  TEST(..., HeaderThreeRejectsOldFields)
  //  generation 3 with committed_through, or with last_epoch_seal, or without life_epoch: encode CORRUPTED_DATA
  //  header-3 text carrying "committed_epoch"/"committed_seq": decode CORRUPTED_DATA under ScopedReadCeiling(3)
  TEST(..., IdenticalSkipComparesGeneration)
  //  ScopedReadCeiling c(3); durable = encode(RefCkpt{.life_epoch = 2}) stored at the key
  //  publishCkpt(contribution RefCkpt{.life_epoch = 2}) == IdenticalSkip, writeCount(key) unchanged
  //  contribution with generation = 3: Published, stored header v == 3
  TEST(..., OldContributionIntoAGenerationThreeBodyIsRefused)                       /// Review Focus 1
  //  ScopedReadCeiling c(3); durable generation 3 {life_epoch 2}
  //  publishCkpt(RefCkpt{.committed_through = RefTxnId{2,1}}): CORRUPTED_DATA; writeCount(key) unchanged

  /// T/gtest_cas_format.cpp
  TEST(CASFormat, ChainGenerationIsNotAdvertisedYet)
  //  G_REF_CHAIN == 3; G_BUILD == 2; currentReadCeiling() == 2; changePoints(FormatId::RefLog).back().generation == 2

  /// T/gtest_cas_pool_meta.cpp: pin, Review Focus 1
  TEST(CASPoolMeta, FloorThreeIsRefusedBelowTheChainCeiling)
  //  bytes = pool meta encoded with min_reader_generation = 3
  //  ScopedReadCeiling(2): decodePoolMeta: UNKNOWN_FORMAT_VERSION; ScopedReadCeiling(3): decodes, floor == 3

  /// T/gtest_cas_gc_bounded_walk.cpp: Review Focus 1
  TEST(CASGCBoundedWalk, HeaderThreeCkptUnderCeilingTwoHoldsTheNamespace)
  //  publish and drop a blob-bearing ref as in the test at :388, then overwrite the namespace's `_ckpt` with
  //  encodeRefCkpt(generation 3, life_epoch 1); ScopedReadCeiling c(2); run one Authoritative round:
  //  metric(intake, "frontier_proven") == 0; metric(intake, "frontier_namespaces") == 1
  //  backend->deleteTotal() == 0; the blob is still present (head)
  ```

- [ ] **Step 2: Run red.** Add the declarations of the Produces block first: `refLogTxnHeaderGeneration` returns
  today's header, `encodeRefLogTxnAtGeneration` ignores its argument, `next_epoch` and `generation` exist but are
  not encoded. Filter `CASRefEpochSealFormat*:CASRefCheckpoint*:CASFormat*:CASPoolMeta*:CASGCBoundedWalk*`.
  Expected: every new test fails on an assertion except the pins
  (`FloorThreeIsRefusedBelowTheChainCeiling`, `ChainGenerationIsNotAdvertisedYet`,
  `BacklinkHeadKeepsItsHeaderBeforeTheWriterCommit`).
- [ ] **Step 3: Implement** the Produces block and its rules in the files above. `writeLogMeta` gains the
  `!next_epoch` field after the `!prev_*` pair; the decoder reads it as `r.readU64String()`.
- [ ] **Step 4: Run green**, then the gate `CAS*` on `build` (tag `t1_gate`). Expected: `B0` plus 14.
- [ ] **Step 5: Commit.**
  `git add $CA/Formats/CasFormat.h $CA/Formats/CasRefLogFormat.h $CA/Formats/CasRefLogFormat.cpp $CA/Formats/CasRefCkptFormat.h $CA/Formats/CasRefCkptFormat.cpp $CA/Pool/CasRefCkpt.cpp $T/cas_test_helpers.h $T/gtest_cas_ref_epoch_seal_format.cpp $T/gtest_cas_format.cpp $T/gtest_cas_ref_ckpt.cpp $T/gtest_cas_pool_meta.cpp $T/gtest_cas_gc_bounded_walk.cpp`.
  Subject: ``Add generation 3 codecs for the CAS ref log and `_ckpt` ``. Body: the readers of a forward-linked tombstone
  and of a two-field `_ckpt` must exist in every build before any build writes them; `G_BUILD` stays 2, so
  production still refuses both.

### Task 2: Commit 2, the chain walk and its read-only consumers {#task-2}

Implementer `ca-surgeon`. Reviewer `ca-review` (opus). Read §2.7, §3.1, §3.2, §3.3. After this task readers
handle both formats under the test ceiling; writers are unchanged.

**Files:**
- Modify: `CA/Pool/CasRefProtocol.h:30-43` (next to `CatalogLifeIndex`), `:240-351` (`RefTableState`), `:726-848`
- Modify: `CA/Pool/CasRefProtocol.cpp:355-416` (`applyOp`), `:496-570` (`applyTxnInPlace`), `:944-1030`
  (`readCheckpointSnapshotBase`), `:1032-1105` (`recoverRefTableDetailedFromAuthority`); add `walkRefChain`,
  `resolveRefCursorSuccessor`
- Modify: `CA/Pool/CasRefCkpt.h:14-25`, `CA/Pool/CasRefCkpt.cpp:173-214` (`chooseRecoveryGrounding`)
- Modify: `CA/Gc/CasOrphanManifestSweep.cpp:124-157`, `:239-440` (`activeManifestKeys`), `:615-680` (`sweepNamespace`)
- Modify: `CA/Tools/CasFsck.cpp:112-155`, `:312-420` (`checkRefStream`), `:491-510`, `:606-680`, the dangling
  verdicts at `:665`, `:778`, `:1074`
- Modify: `CA/Tools/CasInspect.cpp:179-190`, `:519-520`; `CA/Gc/CasGc.cpp:4812-4829` (`rebuildBaseline`)
- Test: `T/gtest_cas_ref_statemachine.cpp`, `T/gtest_cas_ref_protocol.cpp`, `T/gtest_cas_recovery_grounding.cpp`,
  `T/gtest_cas_orphan_manifest_sweep.cpp`, `T/gtest_cas_fsck.cpp`, `T/gtest_cas_gc_rebuild.cpp`,
  `T/gtest_cas_inspect.cpp`, and every `T/` file that calls a changed signature

**Interfaces:**
- Consumes: Task 1's `RefLogTxn::next_epoch`, `RefCkpt::generation`, `encodeRefLogTxnAtGeneration`,
  `ScopedReadCeiling`, `tombstoneTxn`. `CkptSample{ckpt, etag}` and `readCkpt` (`CA/Pool/CasRefCkpt.h:122-130`).
  `KeyReader` (`CA/Pool/CasKeyReader.h:15-26`).
- Produces (all in `CA/Pool/CasRefProtocol.h` unless marked):

  ```cpp
  /// The tombstone a chain last crossed and the epoch its successor lies in.
  struct RefEpochTransition { RefTxnId seal_id; uint64_t next_epoch = 0; bool operator==(const RefEpochTransition &) const = default; };
  /// §3.1 successor table. nullopt iff `txn` contains `RemoveNamespace`.
  std::optional<RefTxnId> refChainSuccessor(const RefLogTxn & txn);
  /// Set iff `txn` is a tombstone: {txn_id, next_epoch}, or {txn_id, writer_epoch + 1} for one without the field.
  std::optional<RefEpochTransition> refEpochTransitionOf(const RefLogTxn & txn);
  /// CORRUPTED_DATA unless: `open` set => next is `{open.next_epoch, 1}` and names `open.seal_id` in its backlink;
  /// `open` unset => next carries no backlink.
  void validateRefChainLink(const std::optional<RefEpochTransition> & open, const RefLogTxn & next);
  /// True iff `catalog` holds a `Live` or `Removing` row with exactly this incarnation.
  bool catalogStillNamesLife(const RefCatalog & catalog, const NamespaceLifeId & life);

  const std::optional<RefEpochTransition> & RefTableState::getOpenTransition() const;
  bool RefTableState::isTerminal() const;                 /// not Live and `remove_txn_id` set
  RefTxnId RefTableState::chainNextId(uint64_t life_epoch) const;   /// `{life_epoch,1}` when nothing is applied
  uint64_t RefTableState::chainEpoch(uint64_t life_epoch) const;    /// epoch of `chainNextId`

  struct RefChainOrigin
  {
      RefTxnId first_key;
      std::optional<RefEpochTransition> open_transition;  /// set when `first_key` follows a tombstone
      std::optional<Etag> ckpt_token;       /// unset: a GC round's own cursor; a failure propagates, no re-read
      bool confirm_base_after_end = true;   /// false: the caller re-reads `_ckpt` after its own last request
      std::optional<RefTxnId> old_frontier; /// `committed_through` of a generation 1 or 2 `_ckpt`
  };
  enum class RefChainEnd : uint8_t { AbsentKey, Terminal, BaseMoved };
  struct RefChainWalk { RefChainEnd end = RefChainEnd::AbsentKey; RefTxnId first_absent{};
                        std::optional<RefEpochTransition> open_transition; uint64_t records = 0; };
  using RefChainVisitor = std::function<void(RefLogTxn && txn, uint64_t stored_bytes)>;
  RefChainWalk walkRefChain(CasOperation & op, const Layout & layout, const NamespaceLifeId & life,
                            const RefChainOrigin & origin, const RefChainVisitor & visit, KeyReader * reader = nullptr);

  enum class RefCursorResumeKind : uint8_t { Key, Terminal, Hold };
  struct RefCursorResume { RefCursorResumeKind kind = RefCursorResumeKind::Hold; RefTxnId next_key{};
                           std::optional<RefEpochTransition> open_transition; uint64_t body_gets = 0;
                           uint64_t absent_probes = 0; String detail; };
  /// §3.2 steps 2 and 3 for a nonzero `cursor`. Reads no key at or below a cursor that is below `snapshot_id`.
  RefCursorResume resolveRefCursorSuccessor(CasOperation & op, const Layout & layout, const NamespaceLifeId & life,
                                            const RefTxnId & cursor, const std::optional<RefTxnId> & snapshot_id);

  /// RecoveryGrounding (CasRefCkpt.h): `walk_from` is always set for a recoverable life (the base's successor, or
  /// `{life_epoch,1}`); `committed_through` is set only from a generation 1 or 2 body.
  struct RecoveredRefTable { RefTableState state; std::optional<RefTxnId> newest_snapshot_id;
                             std::optional<RefEpochTransition> open_transition; RefChainEnd end; RefTxnId first_absent; };
  /// nullopt: the base moved under the walk; the caller re-reads `_ckpt` and calls again.
  std::optional<RecoveredRefTable> recoverRefTableDetailedFromAuthority(
      CasOperation & op, const Layout & layout, const std::optional<CatalogEntry> & catalog_entry,
      const std::optional<CkptSample> & ckpt, KeyReader * reader = nullptr);
  constexpr size_t kRefReadOnlyRecoveryMaxRestarts = 3;
  ```

- Rules the signatures do not state:
  - **Replay** (`applyTxnInPlace`, replacing the density check at `:515` and the lifecycle test at `:555-557`). With
    nothing applied, the first record is at sequence 1 (as today). Otherwise the id must equal
    `refChainSuccessor` of the last applied record; a record after a terminal one is `CORRUPTED_DATA`. Then
    `validateRefChainLink(open_transition, txn)`. Applying a tombstone sets the open transition; applying its
    head clears it. `stateFromSnapshot` leaves it unset; `swap` swaps it. `nextTxnId` (`:268`) stays for the
    writer until Task 5c.
  - **`applyOp`.** `EpochSeal`: legal while never-born, refused only when `remove_txn_id` is set (replaces the
    `Live` test at `:393-412`). `NamespaceBirth`: refused when `remove_txn_id` is set (`:359-367`).
  - **`walkRefChain`.** Exact reads in successor order starting at `origin.first_key`; read-ahead only inside one
    epoch and only through `reader`; each record is decoded, checked with `validateRefChainLink` against the walk's
    own open transition, then handed to `visit`. End: a terminal record (`Terminal`, no `_log` key after it is read), or
    the first absent key (`AbsentKey`). At either end, with `ckpt_token` set and `confirm_base_after_end`: re-read
    `_ckpt`; a different token, or an absent `_ckpt`, is `BaseMoved`. Plan decision: the re-read also follows a
    terminal end, because a late `RemoveNamespace` in a slot cleanup reclaimed would otherwise end the walk of a
    live table; the cost is one `GET` per walk of a removed stream. An absent key at or below `old_frontier` is
    `CORRUPTED_DATA`, after the same re-read says the token is unchanged. A `CORRUPTED_DATA` from decode,
    validation or `visit` with `ckpt_token` set: re-read `_ckpt` first; a different token is `BaseMoved`, the
    same token rethrows. Every exit, a throw included, discards the hints it gave `reader`.
  - **`readCheckpointSnapshotBase`.** Keeps the same-id log and the predecessor read. The check at `:980-991` goes.
    New check: a predecessor that carries `next_epoch` must name the base's epoch; one without it must be in
    the epoch before the base's.
  - **`resolveRefCursorSuccessor`**, the one algorithm the tests do not determine:

    ```text
    c = cursor, S = snapshot_id
    if !S or c >= S:  got = read(c)
        absent -> Hold; RemoveNamespace -> Terminal; else Key{refChainSuccessor(got)}, open = refEpochTransitionOf(got)
    else:  e = S.writer_epoch
        loop: if e == c.writer_epoch -> Key{c.writer_epoch, c.ref_sequence + 1}
              head = read({e,1}); absent, undecodable or without a backlink -> Hold
              p = head.prev_epoch_seal
              if p == c                    -> Key{e,1}, open = {c, e}
              if p.writer_epoch < c.writer_epoch, or (p.writer_epoch == c.writer_epoch and p < c) -> Hold
              if p.writer_epoch == c.writer_epoch -> Key{c.writer_epoch, c.ref_sequence + 1}
              e = p.writer_epoch
    ```

  - **Consumers.** `recoverRefTableDetailedFromAuthority`: `walkRefChain` with `ckpt_token = ckpt->etag`,
    `old_frontier` from a generation 1 or 2 body, the visitor is `RefReplayBuilder::applyOne`. Its four callers
    (`CA/Tools/CasFsck.cpp:140`, `:644`; `CA/Gc/CasOrphanManifestSweep.cpp:256`; `CA/Gc/CasGc.cpp:4823`) re-read
    `_ckpt` and retry on `nullopt`, at most `kRefReadOnlyRecoveryMaxRestarts` times; past that the sweep keeps
    every candidate, fsck records the life unchecked, `REBUILD` throws.
  - **Orphan sweep.** The removal walk (`:270-440`) starts at `resolveRefCursorSuccessor(cursor, checkpoint_snapshot_id)`
    (or `{life_epoch,1}` for a zero cursor) and runs `walkRefChain` from there with `ckpt_token` set; `Hold` or a
    walk that did not end keeps every candidate. After the walk the sweep reads `gc/state` and the seal it names
    again and compares `coverageOf` for this namespace; a moved cursor restarts the walk once, a second move
    keeps every candidate. `sweepNamespace` (`:626-680`) re-reads the catalog after its walk and deletes nothing
    unless `catalogStillNamesLife`. `:546` is unchanged.
  - **fsck.** `checkRefStream` and the owner replay share one walk result per life and read no key below the base;
    "hole" is reported only below a generation 1 or 2 frontier or against a listed id above the walk's end. One
    catalog re-read after the per-life loop: a life it no longer names is counted `namespace_janitor_pending`,
    not a finding. A `Dangling` verdict from `recoverLateRefTable` is reported only after one more batched
    catalog re-read still names the life.
  - **Inspect** prints `generation` and `next_epoch`; `committed_through` and `last_epoch_seal` under the label
    `old_format`; a log named by key below the base gets `"note":"may be a stray"`.

- [ ] **Step 1: Write the failing tests**

  ```cpp
  /// T/gtest_cas_ref_statemachine.cpp, suite CASRefStateMachine (ScopedReadCeiling c(3) in each)
  TEST(..., ReplayRejectsHeadThatDoesNotNameItsTombstone)
  //  birth {1,1}; T{1,2}->4. H{4,1}<-{1,1}: CORRUPTED_DATA. {4,1} without a backlink: CORRUPTED_DATA.
  //  H{4,1}<-{1,2} applies; getOpenTransition() == nullopt; getGreatestApplied() == {4,1}
  TEST(..., ReplayRejectsRecordAfterTombstoneInTheSameEpoch)
  //  after T{1,2}->4: {1,3}, H{3,1}<-{1,2} and H{5,1}<-{1,2} are each CORRUPTED_DATA
  TEST(..., TombstoneIsLegalBeforeBirthAndRefusedAfterTerminal)
  //  empty state: T{2,1}->5 applies; getOpenTransition() == RefEpochTransition{{2,1}, 5}; lifecycle not Live
  //  birth H{5,1}<-{2,1} applies, lifecycle Live. remove_namespace at {5,2}; then T{5,3}->7: CORRUPTED_DATA
  TEST(..., BirthAfterTerminalIsRefused)
  //  after the terminal {5,2}: a birth at {5,3} and a birth at {6,1} are each CORRUPTED_DATA; isTerminal()
  TEST(..., OldSealIsFollowedByTheNextNumericEpoch)
  //  birth {1,1}; S{1,2}; H{2,1}<-{1,2} applies. On a second state: H{3,1}<-{1,2}: CORRUPTED_DATA
  TEST(..., RecordAtANewEpochWithoutATombstoneIsRefused)
  //  birth {1,1}; H{2,1}<-{1,1}: CORRUPTED_DATA; chainNextId(1) == {1,2}; chainEpoch(1) == 1
  //  empty state: chainNextId(7) == {7,1}

  /// T/gtest_cas_ref_protocol.cpp, suite CASRefChainWalk (CountingBackend, ScopedReadCeiling c(3))
  TEST(..., EndsAtTheFirstAbsentKeyAndConfirmsTheBase)
  //  `_ckpt` generation 3 {life 1}; logs {1,1} birth, {1,2}. end AbsentKey, first_absent {1,3}, records 2
  //  getCount(log {1,1}) + getCount({1,2}) + getCount({1,3}) == 3; getCount(ckpt_key) == 1
  TEST(..., FollowsAForwardLinkAndSkipsUnwrittenEpochs)
  //  {1,1}, T{1,2}->5, H{5,1}<-{1,2}. first_absent {5,2}; getCount of {2,1}, {3,1}, {4,1}, {1,3} are all 0
  TEST(..., EndsAtATombstoneWhoseSuccessorIsAbsent)
  //  {1,1}, T{1,2}->5. first_absent {5,1}; open_transition == RefEpochTransition{{1,2}, 5}
  TEST(..., StopsAtATerminalRecordAndReadsNoKeyAfterIt)
  //  terminal at {1,2} and an object at {1,3}: end Terminal; getCount({1,3}) == 0; getCount(ckpt_key) == 1
  //  with a hook replacing `_ckpt` when {1,2} is read: BaseMoved
  TEST(..., BaseMovedUnderTheWalkIsARestart)
  //  a hook replaces `_ckpt` (new token) when {1,3} is read: end BaseMoved
  TEST(..., FailedValidationWithAMovedBaseIsARestartNotCorruption)
  //  {1,1}, T{1,2}->5, H{5,1}<-{1,1}: CORRUPTED_DATA. With the hook replacing `_ckpt` when {5,1} is read: BaseMoved
  TEST(..., OldFrontierAboveTheEndIsCorruption)
  //  `_ckpt` generation 1, committed_through {1,3}; logs {1,1}, {1,2}: CORRUPTED_DATA
  //  with {1,3} and {1,4} present: first_absent {1,5}, records 4
  TEST(..., ReadAheadIsDroppedWhenTheWalkThrows)
  //  a KeyReader that counts hint and discard: after the CORRUPTED_DATA above, reader.pending() == 0
  TEST(CASRefProtocol, BaseAtSequenceOneNeedsATombstoneThatNamesItsEpoch)
  //  `_ckpt` generation 3 {life 1, snapshot {5,1}}, `_snap` {5,1}, H{5,1}<-{1,2}:
  //  T{1,2}->5 at {1,2}: readCheckpointSnapshotBase returns predecessor_seal_id {1,2}
  //  T{1,2}->4, an ordinary record, or nothing at {1,2}: CORRUPTED_DATA; S{4,7} named by H{5,1}<-{4,7}: accepted

  /// same file, suite CASRefCursorSuccessor
  TEST(..., CursorAtOrAboveTheSnapshotIsReadForItsKind)
  //  no snapshot, cursor {1,2}: ordinary -> Key {1,3}; T{1,2}->5 -> Key {5,1}, open {{1,2},5}; S{1,2} -> Key {2,1}
  //  terminal -> Terminal; absent -> Hold. body_gets == 1 in the four present cases
  TEST(..., CursorBelowTheSnapshotIsNeverRead)                                      /// Review Focus 4
  //  S = {5,3}; H{5,1}<-{1,2}; strays stored at {1,2} and {1,3}. cursor {1,2} -> Key {5,1}, open {{1,2},5}
  //  getCount(log {1,2}) == 0; getCount(log {1,3}) == 0
  TEST(..., OrdinaryCursorBelowTheSnapshotResumesInItsEpoch)
  //  S = {1,9}, cursor {1,4} -> Key {1,5}; body_gets == 0
  TEST(..., BackChainCrossesIntermediateEpochs)
  //  H{3,1}<-{1,2}, H{5,1}<-{3,2}, S = {5,2}, cursor {1,2} -> Key {3,1}; body_gets == 2
  TEST(..., BacklinkBelowOrBeforeTheCursorHolds)
  //  S = {5,2}, H{5,1}<-{1,2}: cursor {2,7} -> Hold; cursor {1,5} -> Hold; head {5,1} absent -> Hold

  /// T/gtest_cas_recovery_grounding.cpp
  TEST(CASRecoveryGrounding, GenerationThreeWalksFromTheBaseOrTheLifeEpochWithoutAFrontier)
  //  generation 3 {life 4}: walk_from {4,1}, committed_through nullopt; with snapshot {6,2}: walk_from {6,3}
  //  generation 1 {life 4} without committed_through: walk_from {4,1}

  /// T/gtest_cas_orphan_manifest_sweep.cpp, suite CASOrphanManifestSweep
  TEST(..., RemovalAboveTheCursorAcrossASkippedEpochStaysProtected)
  //  chain {1,1} publish m1, T{1,2}->5, H{5,1}<-{1,2} removing m1; seal cursor {1,1}: the sweep deletes 0, m1 exists
  TEST(..., RemovalWalkDoesNotTrustAReclaimedCursorKey)
  //  snapshot {5,2}; cursor {1,2} with a stray at its key that removes nothing; H{5,1}<-{1,2} removes m1:
  //  m1 is protected; getCount(log {1,2}) == 0
  TEST(..., LaggingSweepRestartsOnceWhenItsCursorMoved)
  //  a hook advances `gc/state` to a seal with a higher cursor during the walk: `_log` reads show two walks, m1
  //  survives; with the hook firing on every walk: exactly two walks, the sweep deletes 0
  TEST(..., SweepNamespaceSkipsALifeErasedUnderItsWalk)                             /// Review Focus 5
  //  a hook erases the catalog row and deletes the life's logs after the walk's first read: sweepNamespace
  //  returns 0 and issues no delete (deleteTotal unchanged)

  /// T/gtest_cas_fsck.cpp, suite CASFsck
  TEST(..., ChainWithASkippedEpochIsClean)            //  T{1,2}->5, H{5,1}<-{1,2}: no hole, no dangling, no anomaly
  TEST(..., LifeErasedDuringTheScanIsJanitorPending)                                /// Review Focus 5
  //  the row is erased and the logs deleted between the catalog cut and the walk: report.dangling == 0,
  //  report.namespace_janitor_pending_lives == 1, no corruption finding for that life
  TEST(..., LateRecheckOfAnErasedLifeIsNotDangling)
  //  a blob of the life is absent and the life is erased after the per-life loop: report.dangling == 0

  /// T/gtest_cas_gc_rebuild.cpp, T/gtest_cas_inspect.cpp
  TEST(CASGCRebuild, RebuildFollowsASkippedEpoch)     //  cursor of the rebuilt seal == {5,1} for the chain above
  TEST(CASGCRebuild, RebuildFailsWhenTheBaseKeepsMoving)   //  `_ckpt` replaced on every walk: rebuildBaseline throws
  TEST(CASInspect, PrintsGenerationNextEpochAndTheStrayNote)
  //  output of a generation 3 `_ckpt` contains "\"generation\":3"; of T{1,2}->5 contains "\"next_epoch\":5";
  //  of a log below the base contains "may be a stray"
  ```

- [ ] **Step 2: Run red.** Stubs first: the new free functions and `RefTableState` accessors with today's
  behaviour (`getOpenTransition` returns nullopt, `walkRefChain` and `resolveRefCursorSuccessor` throw
  `NOT_IMPLEMENTED`), the new signature of `recoverRefTableDetailedFromAuthority` wrapping today's body. Filter
  `CASRefStateMachine*:CASRefChainWalk*:CASRefCursorSuccessor*:CASRecoveryGrounding*:CASOrphanManifestSweep*:CASFsck*:CASGCRebuild*:CASInspect*`.
- [ ] **Step 3: Implement** in the order: replay rules, `walkRefChain`, `resolveRefCursorSuccessor`,
  `chooseRecoveryGrounding`, read-only recovery, then the four consumers.
- [ ] **Step 4: Run green; re-target** existing tests by the rule in Global Constraints (expected classes: a
  replayed birth after a terminal record and an epoch crossing without a seal, class (a); callers of the changed
  `recoverRefTableDetailedFromAuthority` signature, class (b)).
- [ ] **Step 5: Gates.** `CAS*` on `build` (`t2_gate`) and on `build_asan` (`t2_asan`).
- [ ] **Step 6: Commit.**
  `git add $CA/Pool/CasRefProtocol.h $CA/Pool/CasRefProtocol.cpp $CA/Pool/CasRefCkpt.h $CA/Pool/CasRefCkpt.cpp $CA/Gc/CasOrphanManifestSweep.cpp $CA/Tools/CasFsck.cpp $CA/Tools/CasInspect.cpp $CA/Gc/CasGc.cpp $T/gtest_cas_ref_statemachine.cpp $T/gtest_cas_ref_protocol.cpp $T/gtest_cas_recovery_grounding.cpp $T/gtest_cas_orphan_manifest_sweep.cpp $T/gtest_cas_fsck.cpp $T/gtest_cas_gc_rebuild.cpp $T/gtest_cas_inspect.cpp`
  plus the re-targeted tests. Subject: `Read the CAS ref chain to its first absent key`. Body: one walk decides where
  history ends for read-only recovery, fsck, inspect, `REBUILD` and the orphan sweep, so a reader no longer needs
  `committed_through`; fsck and `sweepNamespace` confirm the catalog life after their walks because a deleted key
  reads as the end.

### Task 3: Commit 3, GC intake cut {#task-3}

Implementer `ca-surgeon`. Reviewer `ca-review` (opus). Read §3.2, §3.3 (GC rows), §3.4.

**Files:**
- Modify: `CA/Gc/CasGc.h:676-684` (`FrontierUnproven`), `:829-843`, the private members near `:1016`
- Modify: `CA/Gc/CasGc.cpp:2475-2530` (walk targets), `:2700-2955` (per-namespace start, witnesses, ceiling,
  absent-key arm), `:2956-3010` (per-log fold), `:3296-3312` (unchanged, read only), `:3536-3541`
- Test: `T/gtest_cas_gc_bounded_walk.cpp`, `T/gtest_cas_gc_frontier_gate.cpp`, and the `T/gtest_cas_gc_*` files the
  re-targeting rule reaches

**Interfaces:**
- Consumes (Task 2): `resolveRefCursorSuccessor`, `refChainSuccessor`, `refEpochTransitionOf`,
  `validateRefChainLink`, `RefEpochTransition`; (Task 1) `RefCkpt::generation`, `G_REF_CHAIN`.
- Produces:

  ```cpp
  /// CasGc.h. In-process only: nothing is persisted and `cas_fold_seal` does not change.
  struct RefFoldMemo { RefTxnId cursor; RefTxnId successor_key; std::optional<RefEpochTransition> open_transition; };
  std::unordered_map<UInt128, RefFoldMemo> ref_fold_memo;      /// keyed by the physical life id
  static constexpr uint64_t kRefIntakeBudgetMs = 60'000;
  /// new value of FoldResult::FrontierUnproven
  IntakeSliceCut,
  ```

- Rules:
  - **Start** (replaces `:2794-2800`). Zero cursor: `{life_epoch,1}`. Memo whose `cursor` equals the durable
    cursor: probe `successor_key`. Else `resolveRefCursorSuccessor(cursor, checkpoint_snapshot_id)`; `Hold` holds
    the namespace with `HoldReason::UnconsumedSealCrossing`, `Terminal` proves it.
  - **Cut** (replaces the ceiling at `:2872-2883` and the empty-frontier arm at `:2838-2848`). The fold reads
    in successor order and stops at the first absent key or at a terminal record; either one, read in this
    round, proves the frontier. Nothing above the absent key is folded in this round. A `_ckpt` without
    `committed_through` proves nothing by itself.
  - **Absent key with a witness above it.** Witnesses stay the listing, the snapshot id and the carried hold
    (`:2731-2739`); `committed_through` counts only when `checkpoint.generation < G_REF_CHAIN`. A witness in the
    same epoch as the absent key holds with `GapBelowWitness` (today's arm); the forward crossing through
    `crossFromSeal` (`:2925-2954`) is removed, because a tombstone's successor is known from its body.
  - **Per log.** `validateRefChainLink` with the walk's open transition before `manifestEdgesOfTxn`; a failure
    holds with `HoldReason::BodyUndecodable`. No new `HoldReason` value: the enum is persisted
    (`CA/Formats/CasFoldSealFormat.h:45`).
  - `gc_frontier_probe_budget` (`:2493-2517`) and the per-epoch accounting (`:3296-3312`) are unchanged.
  - **Memo.** Written when a namespace's fold ends at an absent key: `{new cursor, that key, open transition}`.
    Erased on a hold and when the life leaves the catalog cut.
  - **Slice.** `deadline = mono_ms_fn() + kRefIntakeBudgetMs`, taken when the ref intake starts. Pass 1 visits
    every walk target and is never cut: its start resolution, the successor probe and at most one read window
    (`reads.window()` logs). Pass 2 continues the targets that did not end, starting at index `round mod n` of
    them, and starts no read once `mono_ms_fn() >= deadline`. A namespace left unfinished keeps the cursor it
    reached, is counted in `frontier_namespaces` and not in `frontier_proven`, with `FrontierUnproven::IntakeSliceCut`.

- [ ] **Step 1: Write the failing tests** (`T/gtest_cas_gc_bounded_walk.cpp`, suite `CASGCBoundedWalk`; helpers of
  that file: `runRoundCapturingIntake`, `metric`, `cursorOf`, `coverageOf`, `publishAt`, `writeTxnAt`; a fake
  `mono_ms_fn` is passed to `Gc`'s constructor (`CA/Gc/CasGc.h:452`))

  ```cpp
  TEST(..., QuietNamespaceCostsCkptAndOneProbeOnceTheSuccessorIsRemembered)
  //  logs {1,1}..{1,3}; round 1 folds them. resetCounts; round 2 with the same Gc:
  //  getCount(ckpt_key) == 1; getCount(log {1,4}) == 1; getCount(log {1,3}) == 0; frontier_proven == 1
  TEST(..., ColdCursorCostsOneMoreRead)
  //  as above, but round 2 runs on a new Gc object: getCount(ckpt_key) == 1, getCount({1,3}) == 1, getCount({1,4}) == 1
  TEST(..., ARoundStopsAtItsFirstAbsentKey)
  //  ChasingWriterBackend writes {1,4} right after the round read it absent, then {1,5}:
  //  cursorOf == {1,3}; frontier_proven == 1; the next round ends with cursorOf == {1,5}
  TEST(..., GenerationThreeCkptWithoutAFrontierIsWalkedNotProven)
  //  ScopedReadCeiling c(3); `_ckpt` generation 3 {life 1}, logs {1,1}, {1,2}, zero cursor: cursorOf == {1,2}
  TEST(..., TerminalRecordProvesTheFrontierWithoutAProbe)
  //  terminal at {1,3}: frontier_proven == 1; getCount(log {1,4}) == 0
  TEST(..., SliceCutLeavesTheNamespaceUnprovenAndKeepsItsCursor)
  //  one namespace with a tail of 3 * window logs; the fake clock passes 60'000 ms after pass 1:
  //  cursorOf is the last log of pass 1's window; frontier_proven == 0; frontier_namespaces == 1; deleteTotal() == 0
  //  the next round, with the clock held, ends at the tail with frontier_proven == 1
  TEST(..., PassOneIsNeverCutAndPassTwoRotatesByRound)
  //  three namespaces, each with a tail of 3 * window; clock already past the slice when the intake starts:
  //  every cursor advanced by one window. With a clock that expires after one pass-2 namespace:
  //  round r completes the namespace at index r mod 3 of the unfinished ones
  TEST(..., CursorOnATombstoneResumesAtTheLinkedEpoch)                              /// §5 row 19
  //  ScopedReadCeiling c(3); round 1 folds through T{1,3}->5 (absent {5,1}); then H{5,1}<-{1,3} lands and the
  //  tombstone object is deleted; round 2 on a new Gc with snapshot {5,1}: cursorOf == {5,1}; getCount(log {1,4}) == 0
  TEST(..., CursorOnAnOldSealResumesAtTheNextEpoch)
  //  cursor on S{1,3}, H{2,1}<-{1,3}: cursorOf == {2,1} after the round
  TEST(..., CursorBelowTheSnapshotIsNeverRead)
  //  cursor {1,2}, snapshot {5,2}, H{5,1}<-{1,2}, new Gc: getCount(log {1,2}) == 0; getCount(log {1,3}) == 0
  TEST(..., StrayAtAReclaimedCursorKeyIsNotFolded)                                  /// Review Focus 4, §5 row 8
  //  as above, with a stray at {1,2} that removes the ref of a live blob, and one at {1,3}: after
  //  runRoundsUntilAbsent-style rounds the blob is present; cursorOf == {5,2}
  TEST(..., StrayAfterAReclaimedTombstoneIsNotFolded)                               /// §5 row 7
  //  the tombstone T{1,3}->5 is cleaned; strays at {1,3} and {1,4} remove a live blob's ref: the blob stays
  TEST(..., HeadThatDoesNotNameItsTombstoneHoldsTheNamespace)
  //  T{1,2}->5, H{5,1}<-{1,1}: coverageOf(...)->hold is set, cursorOf == {1,2}, deleteTotal() == 0
  ```

  The tests at `:231`, `:280` and `:388` pin the removed ceiling: class (a), replaced by `ARoundStopsAtItsFirstAbsentKey`,
  `QuietNamespaceCosts...` and `HeaderThreeCkptUnderCeilingTwoHoldsTheNamespace`.
- [ ] **Step 2: Run red** with the memo, the constant and the enum value declared and unused. Filter `CASGC*`.
- [ ] **Step 3: Implement** the rules in `Gc::fold`.
- [ ] **Step 4: Run green; re-target** `T/gtest_cas_gc_frontier_gate.cpp`, `T/gtest_cas_gc_arithmetic_intake.cpp`,
  `T/gtest_cas_gc_hold_grammar.cpp`, `T/gtest_cas_ref_gc.cpp`, `T/gtest_cas_list_liar_end_to_end.cpp` by the rule.
- [ ] **Step 5: Gates.** `CAS*` on `build` (`t3_gate`) and `build_asan` (`t3_asan`).
- [ ] **Step 6: Commit.** `git add $CA/Gc/CasGc.h $CA/Gc/CasGc.cpp $T/gtest_cas_gc_bounded_walk.cpp` plus the
  re-targeted tests. Subject: `Cut the CAS GC ref intake at the first absent key`. Body: the fold no longer
  takes its bound from `committed_through`; it resumes from a cursor without reading a key that cleanup may
  have reclaimed, and a fixed time slice bounds a walk that a writer outruns.

### Task 4: Commit 4, `last_unclean_epoch` in the mount lease {#task-4}

Implementer `ca-impl`. Reviewer `ca-review` (opus). Read §4. The marker is recorded and published; nothing
consumes it until Task 6.

**Files:**
- Modify: `CA/Formats/CasServerRootFormats.h:66-93`; `CA/Formats/CasServerRootFormats.cpp:31-42`, `:154-172`, `:174-230`
- Modify: `CA/Pool/CasServerRoot.h:630-700`; `CA/Pool/CasServerRoot.cpp:905-917` (`makeMountBody`), `:992-1120`
  (`claimMount`, writes at `:1004` and `:1089`), `:1689-1705` (`encodeBody`), `:2171-2185` (`terminate`); read
  and leave as they are: `:654-667` (rejoin copies the slot), `:1126-1140` and `:1403` (`fencedCopy`)
- Modify: `CA/Pool/CasMountRuntime.h:373`, `:394-399`, `:533`; `CA/Pool/CasMountRuntime.cpp:317-320`, `:422-442`
- Modify: `CA/Pool/CasPool.cpp:859`, `:965`, `:1620`, `:1632`; `CA/Pool/CasPool.h:835-842`
- Test: `T/gtest_cas_pool.cpp`

**Interfaces:**
- Produces:

  ```cpp
  std::optional<uint64_t> MountLease::last_unclean_epoch = std::nullopt;   /// declared last; wire key "last_unclean_epoch"
  struct LiveEpochPair { uint64_t writer_epoch = 0; uint64_t last_unclean_epoch = 0; };   /// CasServerRootFormats.h, after MountLease
  /// §4 table; `predecessor` is the body the claim replaces, nullopt when the slot was absent.
  uint64_t lastUncleanEpochAfter(const std::optional<MountLease> & predecessor, MountPriorState prior, uint64_t claimer_epoch);
  MountLease makeMountBody(UInt128 uuid, uint64_t epoch, uint64_t seq, uint64_t now_ms, uint64_t ttl_ms, uint64_t last_unclean_epoch);
  MountLeaseRenewer::MountLeaseRenewer(..., uint64_t writer_epoch_, uint64_t last_unclean_epoch_, ...);   /// after writer_epoch_
  void CasMountRuntime::installRenewer(UInt128 our_uuid, uint64_t writer_epoch, uint64_t last_unclean_epoch,
                                       const std::function<uint64_t()> & now_ms);
  void CasMountRuntime::setLiveWriterEpoch(uint64_t writer_epoch, uint64_t last_unclean_epoch);
  LiveEpochPair CasMountRuntime::liveEpochPair() const;
  LiveEpochPair Pool::liveEpochPairForTest() const;
  ```

- Rules:
  - `lastUncleanEpochAfter`: no slot: `claimer_epoch - 1`. Body without the field: its `writer_epoch`, also when
    it is a clean farewell. `Clean` with the field: the predecessor's value. `Fenced`, `UncleanObserved`,
    `UncleanUnsafe`: the predecessor's `writer_epoch`.
  - `MountClaimResult::body` (`CA/Pool/CasServerRoot.h:288`) is the written body, so both callers take the marker
    from `claim.body->last_unclean_epoch` and pass it to `installRenewer` and `setLiveWriterEpoch`.
  - Plan decision, the pair. Two atomics, no lock on the append path: `setLiveWriterEpoch` stores the marker,
    then the epoch; `liveEpochPair` loads the epoch, then the marker. A reader that sees the new epoch then sees
    the new marker. The other interleaving pairs the old epoch with the new marker, which is higher: recovery
    then writes a tombstone it could have skipped. The unsafe pairing, new epoch with old marker, cannot be read.
  - `setLiveWriterEpochForTest(epoch)` keeps the current marker.

- [ ] **Step 1: Write the failing tests**

  ```cpp
  /// T/gtest_cas_pool.cpp
  TEST(CASPool, ClaimSetsLastUncleanEpoch)        //  claimMount called directly; each case decodes the stored body
  //  no slot, claimer 7                                            -> 6
  //  predecessor epoch 5, clean farewell, last_unclean_epoch 3     -> 3
  //  predecessor epoch 5, clean farewell, field absent             -> 5
  //  predecessor epoch 5, gc_fenced, last_unclean_epoch 3          -> 5
  //  predecessor epoch 5, proven dead by its token, value 3        -> 5
  //  predecessor epoch 5, unsafe reclaim authorization, value 3    -> 5
  TEST(CASPool, EveryBodyWriterKeepsOrResetsIt)
  //  after a claim at epoch 7 with marker 5: the body of MountLeaseRenewer::start, of one renewal, of the farewell
  //  (release), of fenceOwnClaim and of a GC fence each decode with last_unclean_epoch == 5
  //  a rejoin rewrite of a slot with value 4 decodes with 4
  TEST(CASPool, LeaseBodyWithoutTheFieldDecodesAsAbsent)
  //  encodeMountLease of a body with the value 5 contains "\"last_unclean_epoch\":\"5\""; with nullopt it does
  //  not contain the key and equals today's bytes; a body with an unknown ordinary key still decodes
  TEST(CASPoolRemount, SelfRemountPublishesTheRemountClaimsMarkerWithTheEpoch)      /// Review Focus 3
  //  mount at epoch E; fence it out; self-remount to E2: store->liveEpochPairForTest() == LiveEpochPair{E2, E}
  TEST(CASMountRuntime, LiveEpochPairIsPublishedTogether)
  //  setLiveWriterEpoch(9, 8): liveEpochPair() == {9, 8} and liveWriterEpoch() == 9
  ```

- [ ] **Step 2: Run red** with the field and the functions declared: the marker is not written and
  `liveEpochPair` returns `{epoch, 0}`. Filter `CASPool*:CASMount*:CASClaimMount*:CASServerRoot*`.
- [ ] **Step 3: Implement.** `grep -n 'installRenewer(\|setLiveWriterEpoch(\|makeMountBody(' $WT/src -r` lists
  every caller; each one compiles only with the marker supplied.
- [ ] **Step 4: Run green.** Gates: `CAS*` on `build` (`t4_gate`) and `build_asan` (`t4_asan`).
- [ ] **Step 5: Commit.**
  `git add $CA/Formats/CasServerRootFormats.h $CA/Formats/CasServerRootFormats.cpp $CA/Pool/CasServerRoot.h $CA/Pool/CasServerRoot.cpp $CA/Pool/CasMountRuntime.h $CA/Pool/CasMountRuntime.cpp $CA/Pool/CasPool.h $CA/Pool/CasPool.cpp $T/gtest_cas_pool.cpp`
  plus every `T/` file that calls a changed signature. Subject:
  `Record the last unclean epoch in the CAS mount lease`. Body: a successor must know which earlier epochs ended
  without a clean farewell; the epoch object cannot carry it because it is written before the predecessor's end
  is classified, so the lease body does.

### Task 4a: Commit 4a, writer recovery reads through `walkRefChain` {#task-4a}

Implementer `ca-surgeon`. Reviewer `ca-review` (opus). Behaviour unchanged, request for request, except the two
paths named below. This task exists because it turns Task 5c's recovery change into deletions and one
replacement instead of a rewrite.

**Files:**
- Modify: `CA/Pool/CasRefLedger.cpp:911-1410` (`runRecoveryWalkOnce`)
- Test: `T/gtest_cas_ref_recovery_cas_walk.cpp`

**Interfaces:**
- Consumes: `walkRefChain`, `RefChainOrigin`, `RefChainWalk` (Task 2).
- Produces: nothing new. `runRecoveryWalkOnce` keeps its signature (`CA/Pool/CasRefLedger.cpp:911-914`).
- Shape after the task: the loop at `:1077-1350` becomes "walk, then decide at the absent key". The walk is
  called with `ckpt_token = sampled_ckpt->etag`, `confirm_base_after_end = false`,
  `old_frontier = grounding.committed_through` and no `reader`. Two paths differ from today, both intended:
  an undecodable or mislinked record costs one `_ckpt` re-read and restarts when the base moved, where today it
  fails at once (§3.1); a stream that ends in a terminal record is not probed after it, where today recovery
  reads sequence 1 of every later epoch (§2.7).
  Its visitor keeps today's per-record work: the retained-attempt check (`:1090-1105`), `apply_one`, and for a
  record above the sampled frontier the two-successor probe (`:1121-1140`) and `publish_recovered_frontier`.
  At the absent key the code that stays is today's: the same-epoch-seal hole verdict (`:1201-1219`), the live-epoch
  break (`:1221`), the not-`Live` skip (`:1236-1242`), the numeric seal create and its conflict arms
  (`:1246-1349`); each of them then calls the walk again from the next key.

- [ ] **Step 1: Write the golden pin on the unmodified tree.**

  ```cpp
  TEST(CASRefRecoveryCasWalk, RecoveryRequestSequenceIsUnchangedByTheSharedWalk)
  //  burnEpochsUpTo(backend, layout, 4); seedCkpt(lifeEpochCkpt(1, RefTxnId{1,2})); seedTxn {1,1} birth, {1,2},
  //  and {1,3} above the frontier; open the pool; store->listRefs(ns)
  //  the ordered list of (method, key) the backend saw under the namespace's prefix and at its `_ckpt` key
  //  equals a literal vector written into the test from this run on the unmodified tree
  ```

- [ ] **Step 2: Run it green on the unmodified tree** (filter `CASRefRecoveryCasWalk*:CASRefWriterRecovery*`,
  tag `t4a_pin`); save the log as `$R/logs/pin_t4a.log`. The 39 `CASRefRecoveryCasWalk` tests are the fence.
- [ ] **Step 3: Refactor** to the shape above.
- [ ] **Step 4: Run the same filter green**, the literal vector untouched. Request counts of an existing test
  over a removed namespace may drop: class (b).
- [ ] **Step 5: Gates.** `CAS*` on `build` (`t4a_gate`, expected: the count after Task 4 plus 1) and `build_asan`.
- [ ] **Step 6: Commit.** `git add $CA/Pool/CasRefLedger.cpp $T/gtest_cas_ref_recovery_cas_walk.cpp`. Subject:
  ``Route CAS writer recovery through `walkRefChain` ``. Body: recovery and the
  read-only consumers must not disagree about where a chain ends; the frontier publishes and the numeric seals
  stay on top of the shared walk, so no request changes on a valid stream.

### Task 5a: Commit 5 part 1, catalog confirmation after a ledger recovery walk {#task-5a}

Implementer `ca-impl`. Reviewer `ca-review` (opus). Read §2.4 step 6, §3.1 (rule and table), §5 row 22.
Safe on its own by the argument of §3.1 "Why it is enough": the step adds one catalog read and only narrows
what recovery installs or reports; no object format and no write changes.

**Files:**
- Modify: `CA/Pool/CasRefLedger.h:1165` (declaration); `CA/Pool/CasRefLedger.cpp:1544-1791`
  (`ensureRefTableRecovered`), its 13 callers (`:315`, `:390`, `:413`, `:2044`, `:2054`, `:2222`, `:2971`, `:4715`,
  `:5227`, `:5290`, `:5315`, `:5403`, `:5491`), `:5325-5425` (`namespaceStillLogicallyPresent`)
- Test: `T/gtest_cas_ref_writer.cpp`

**Interfaces:**
- Consumes: `catalogStillNamesLife` (Task 2); `invalidateRemovedCatalogLife` (`CA/Pool/CasRefLedger.cpp:796-840`).
- Produces:

  ```cpp
  enum class RefRecoveryOutcome : uint8_t { Recovered, LifeRemoved };
  [[nodiscard]] RefRecoveryOutcome CasRefLedger::ensureRefTableRecovered(
      const RootNamespace & ns, RefTableRuntime & rt, const std::optional<DetachedStopToken> & token = {});
  ```

- Rules:
  - After a walk returned a result and before `installRecoveryResult` (`:1699`), and also before a non-transient
    exception of the walk leaves the function: with `state_mutex` released, one `CasRefCatalog::read` on
    `mount_requests.resume(admitted_generation)`. If `!catalogStillNamesLife(snapshot.catalog, rt.life)`: `invalidateRemovedCatalogLife(rt.life)`,
    install nothing, return `LifeRemoved` (the walk's exception is dropped: it was about a removed life). If the
    catalog read itself throws, the walk's exception wins, as at `:5254-5259`.
  - Plan decision: a return value, not an exception, so that each caller states its answer:

    | Caller | On `LifeRemoved` |
    |---|---|
    | `resolveRef` `:315`, `namespaceFilesLifeIfReadable` `:5315` | `std::nullopt` |
    | `listRefs` `:390` | empty map |
    | `hasAnyRefWithPrefix` `:413` | `false` |
    | `appendRefOpsOnRuntime` `:2222`, `namespaceLife` cold `:5290`, the test accessors `:2044`, `:2054` | `throwCasWriteRetryLater` naming the removed life |
    | `flushRefBatch` `:2971` | `complete_error` of the batch with the same retry-later error |
    | snapshot publish `:4715` | `return false` |
    | `namespaceLife` resident `:5227` | fall through to the cold path (as `row_gone`, `:5267`) |
    | `namespaceStillLogicallyPresent` `:5403` | its own catalog decision (`:5415-5424`): row gone `false`, any other row of the name `true`; never throws |
    | `dropNamespaceImpl` `:5491` | `return {}`, as for a name without a row (`:5450`) |

- [ ] **Step 1: Write the failing tests** (`T/gtest_cas_ref_writer.cpp`, suite `CASRefWriterRecovery`; the hook is
  a backend callback on the first `_log/` read of the walk that erases the catalog row with
  `deleteCompletedRemovingForTest`-style raw catalog writes and deletes the life's `_log`, `_snap` and `_ckpt` keys)

  ```cpp
  TEST(..., LifeErasedUnderTheWalkIsRemovedNotInstalled)                            /// Review Focus 5
  //  cold pool, hook armed: expectThrowsCode(NETWORK_ERROR, dropRef(ns, "part_1")); refTableRecoveredForTest(ns) is false
  //  a second pool, hook armed: resolveRef(ns, "part_1") == std::nullopt; listRefs(ns).empty()
  //  no call throws CORRUPTED_DATA
  TEST(..., AbsentCkptOfAnErasedLifeIsNotCorruption)
  //  row erased and `_ckpt` deleted before recovery's first read: dropRef throws NETWORK_ERROR, not CORRUPTED_DATA
  //  control: `_ckpt` deleted while the row stays Live: CORRUPTED_DATA
  TEST(..., CreatingRowIsNotAConfirmation)
  //  the hook rewrites the row to Creating with the same incarnation: the outcome equals the erased case
  TEST(..., PresenceProbeOverAnErasedLifeDoesNotThrow)
  //  Removing row; the hook erases it during the probe's recovery: namespaceStillLogicallyPresent(ns) == false
  //  the hook erases it and adds a Live row of the same name with a new incarnation: == true
  TEST(..., ConfirmationCostsOneCatalogReadPerInstall)
  //  a cold resolveRef on an untouched namespace: getCount(layout.refCatalogKey()) grows by exactly 1 over today's count
  ```

- [ ] **Step 2: Run red** with the return type changed, the function always returning `Recovered`, and every
  caller already handling `LifeRemoved` by the table. Filter `CASRefWriter*`. Expected: `CORRUPTED_DATA` where the tests
  want retry-later or absent.
- [ ] **Step 3: Implement** the confirmation.
- [ ] **Step 4: Run green; re-target** request-count assertions that now see one more catalog `GET` (class (b)).
- [ ] **Step 5: Gates.** `CAS*` on `build` (`t5a_gate`) and `build_asan` (`t5a_asan`).
- [ ] **Step 6: Commit.** `git add $CA/Pool/CasRefLedger.h $CA/Pool/CasRefLedger.cpp $T/gtest_cas_ref_writer.cpp`
  plus the re-targeted tests. Subject: `Confirm the catalog life after a CAS ref recovery walk`. Body: GC on another
  server can erase a life and its janitor delete the objects while recovery walks them; a row that is still
  there after the walk proves no delete preceded the reads, and a missing one is a removed life, not corruption.

### Task 5b: Commit 5 part 2, the pool floor raise moves into `mountWritable` {#task-5b}

Implementer `ca-impl`. Reviewer `ca-review-lite`. Read §6 "Pool floor". Safe on its own: the same one-way raise
to `G_REF_MOVE` that every `Pool::open` performs today, earlier in the same mount; the only new effect is that
a decommission mount raises it too.

**Files:**
- Modify: `CA/Pool/CasPool.cpp:598-610` (removed from `Pool::open`), `:880-966` (`mountWritable`); `CA/Pool/CasPool.h`
  (the declaration)
- Test: `T/gtest_cas_pool.cpp`, `T/gtest_cas_decommission.cpp`

**Interfaces:**
- Produces: `static void Pool::raiseReaderFloor(const PoolPtr & store, uint64_t generation);` called from
  `mountWritable` after the claim loop (`:878`) and before `now_boot_ms` is sampled (`:931`), with `G_REF_MOVE`.
  It keeps today's body: `gc_requests.admit`, `PoolMeta::raiseMinReaderGeneration`, the `LOG_WARNING`, the update
  of `store->meta.min_reader_generation`; a floor already at or above `generation` sends nothing. Task 5c changes
  the argument to `G_REF_CHAIN`.

- [ ] **Step 1: Write the failing tests**

  ```cpp
  TEST(CASPool, DecommissionMountRaisesThePoolFloor)
  //  a pool whose meta has min_reader_generation 1 and one retired server root; Pool::openForDecommission(...):
  //  the stored pool meta decodes with min_reader_generation == G_REF_MOVE
  TEST(CASPool, FloorRaiseIsCountedByTheRenewalWindowCheck)
  //  a backend hook advances the boot clock by more than mount_lease_ttl_ms during the pool-meta write:
  //  the open succeeds and writeCount(layout.mountKey(srid)) is one more than in a run without the hook
  TEST(CASPool, FloorIsRaisedBeforeTheWorkersStart)                                 /// pin
  //  with the pool-meta write failing (injected Refused): the open throws and no `_log/` or `_ckpt` key was written
  ```

- [ ] **Step 2: Run red** (filter `CASPool*:CASDecommission*`). Expected: the first two fail on assertions.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run green.** Gates: `CAS*` on `build` (`t5b_gate`) and `build_asan` (`t5b_asan`).
- [ ] **Step 5: Commit.**
  `git add $CA/Pool/CasPool.h $CA/Pool/CasPool.cpp $T/gtest_cas_pool.cpp $T/gtest_cas_decommission.cpp`. Subject:
  ``Raise the CAS pool reader floor in `mountWritable` ``. Body: a decommission
  mount bypasses `Pool::open` and so never raised the floor; raising it before the renewal-window check makes
  that check count the time the raise takes.

### Task 5c: Commit 5 part 3, the generation 3 protocol with a tombstone at every recovery {#task-5c}

Implementer `ca-surgeon`. Reviewer `ca-review` (opus), then a second fresh `ca-review` on the test re-targeting.
Read §2 whole, §5, §6, §11. No further safe split exists: once `G_BUILD` is 3 and one generation 3 object is
written, the stamp, the append, the wedge, recovery, genesis and `_ckpt` creation must all follow the new rule.
The state after this task is the always-eager protocol; with correct clocks it is model config
`CaRefLogOnlyAppendCore_safe_lateland_eager`. No config runs the always-eager rule under a lying clock.

**Files:**
- Modify: `CA/Formats/CasFormat.h:16`, `CA/Formats/CasFormat.cpp:30-36`, `:40-66`; `CA/Formats/CasRefLogFormat.cpp`
  (`refLogTxnHeaderGeneration`); `CA/Formats/README.md:24`, `:90-95` (only the two statements this commit makes
  false: the `_ckpt` field list and `G_BUILD == 2`; the rest of that file is Task 8's)
- Modify: `CA/Pool/CasRefCkpt.h:55-117`, `CA/Pool/CasRefCkpt.cpp` (new functions; `publishCkpt` stays until Task 7)
- Modify: `CA/Pool/CasRefCatalog.cpp:631-637` (`completeCreation`)
- Modify: `CA/Pool/CasRefProtocol.h:490-515` (`RecoveryResult`)
- Modify: `CA/Pool/CasRefLedger.h:690-704`, `:797-968`, `:1070`; `CA/Pool/CasRefLedger.cpp:146-175`, `:911-1410`,
  `:1793-1815`, `:2400-2620`, `:3070-3115`, `:3443-3521`, `:3737-3853`, `:3961-3977`, `:4205-4254`, `:4381-4388`,
  `:4863-4880`
- Modify: `CA/Pool/CasPool.cpp` (the argument of `raiseReaderFloor`)
- Test: `T/gtest_cas_ref_writer.cpp`, `T/gtest_cas_ref_recovery_cas_walk.cpp`, `T/gtest_cas_ref_ckpt.cpp`,
  `T/gtest_cas_format.cpp`, `T/gtest_cas_ref_epoch_seal_format.cpp`, `T/gtest_cas_pool.cpp`, `T/cas_test_helpers.h`,
  and every `T/gtest_cas_*.cpp` the re-targeting rule reaches
- Test: `tests/integration/test_cas_gcs_relink_liveness/test.py:30-32`, `:191`, `:224`

**Interfaces:**
- Consumes: everything Tasks 1 to 5b produce.
- Produces:

  ```cpp
  constexpr uint32_t G_BUILD = 3;       /// REF_LOG_HISTORY {{1,1},{2,2},{3,3}}; new REF_CKPT_HISTORY {{1,1},{3,3}}
  /// refLogTxnHeaderGeneration: G_REF_CHAIN iff next_epoch or prev_epoch_seal is set

  /// CasRefCkpt.h
  /// Create-if-absent of the generation 3 body {life_epoch}. A present body is left as it is, whatever it holds.
  CkptPublishOutcome createCkptAtCreation(CasOperation & op, const Layout & layout, const NamespaceLifeId & life,
                                          uint64_t life_epoch, const Retry & policy = Retry::standard());
  /// Conditional replace of a present generation 3 body that raises `checkpoint_snapshot_id`. Writes nothing when
  /// the object is absent, below generation 3, or already at or above `snapshot_id` in id order.
  CkptPublishOutcome publishCkptSnapshot(CasOperation & op, const Layout & layout, const NamespaceLifeId & life,
                                         const RefTxnId & snapshot_id, const Retry & policy = Retry::standard());
  enum class CkptStampOutcome : uint8_t { Stamped, TokenMismatch, Unresolved };
  struct CkptStampResult { CkptStampOutcome outcome; std::optional<CkptSample> stamped; };
  /// One conditional replace on `accepted.etag` with {life_epoch, checkpoint_snapshot_id} at generation 3.
  CkptStampResult stampCkptGeneration3(CasOperation & op, const Layout & layout, const NamespaceLifeId & life,
                                       const CkptSample & accepted);

  /// CasRefProtocol.h, RecoveryResult: `last_epoch_seal` is removed (the state carries the open transition)
  CkptSample accepted_ckpt;             /// the `_ckpt` the installed table rests on; its `life_epoch` names the first key
  uint64_t chain_epoch = 0;
  /// CasRefLedger.h, RefTableRuntime: `last_epoch_seal` is removed
  CkptSample accepted_ckpt;             /// guarded by state_mutex; replaced by install and by the lane's stamp
  /// CasRefLedger.cpp
  RefLogTxn makeEpochTombstoneTxn(const RootNamespace & ns, const RefTxnId & id, uint64_t next_epoch,
                                  const std::optional<RefEpochTransition> & open);   /// replaces makeEpochSealTxn
  bool CasRefLedger::stampBeforeFirstWrite(const RootNamespace & ns, RefTableRuntime & rt, CasOperation & op);
  ```

- Rules, by area. Line numbers are today's.
  - **Advertise.** `G_BUILD = 3`, the two histories, the assertions at `CA/Formats/CasFormat.cpp:35-36` retargeted
    to `G_REF_CHAIN`. `raiseReaderFloor(store, G_REF_CHAIN)`.
  - **`_ckpt` writers.** `completeCreation` calls `createCkptAtCreation` with `observed.creator->writer_epoch`.
    Snapshot publication (`:4872`) calls `publishCkptSnapshot`. No other production site writes `_ckpt` except the
    stamp. `PreparedRefChunk::birth_contribution` and `commit_contribution` go (`:690-704`, `:3452-3458`,
    `:3510-3518`).
  - **Append** (`commitRefChunk`). Id: `rt->state.chainNextId(rt->accepted_ckpt.ckpt.life_epoch)` under
    `state_mutex` (replaces `allocateRefTxnId`, `:3744`). Backlink: the open transition's `seal_id` iff the id is
    at sequence 1 and a transition is open (replaces `chainLinkFor`, `:3754`). The birth-time publish
    (`:3834-3853`) and the frontier publish with its outcome arms (`:4205-4254`) are removed; the install hold
    (`:4258-4342`) follows the create directly. A conflict whose occupant is a tombstone: lane `Closed` with
    today's rejection (`:3961-3977`). The deposed-lane check (`:3090-3115`) becomes: chain epoch above the live
    epoch closes the lane; chain epoch below it sends the lane to `NeedsRecovery` (Task 6 replaces this arm).
  - **Stamp, lane.** Plan decision: the lane's stamp lands here, not in Task 6, so that no append can precede the
    stamp even if the argument "an unstamped chain never ends in the live epoch" is wrong. `stampBeforeFirstWrite`
    runs in `commitRefChunk` before the id is allocated when `rt->accepted_ckpt.ckpt.generation < G_REF_CHAIN`:
    `Stamped` stores the new sample in the runtime; `TokenMismatch` or `Unresolved` puts the lane in
    `NeedsRecovery` and fails the batch retry-later.
  - **Wedge** (`resolveWedgeOnce`). The publish at `:2559-2620` is removed: an attempt proven durable under the
    same wedge is installed after the lease recheck.
  - **Recovery** (`runRecoveryWalkOnce`), §2.4 steps 1 to 7 with the rule "seal iff the stream is not terminal
    and the chain epoch `X` is below the live epoch `W`":
    1. `readCkpt`, `chooseRecoveryGrounding`, the base as today. `accepted = sampled`.
    2. `walkRefChain` with `ckpt_token = accepted.etag`, `confirm_base_after_end = false`, `old_frontier` from a
       body below generation 3; the visitor keeps the retained-attempt check and `apply_one`. `BaseMoved` restarts.
       The frontier publishes (`:1026-1061`, `:1356-1375`) and the two-successor verdict (`:1114-1140`) are removed.
    3. While `AbsentKey`, not terminal and `X < W`: stamp if `accepted.ckpt.generation < G_REF_CHAIN`
       (`TokenMismatch` restarts, `Unresolved` is retry-later); count the attempt against
       `kRefRecoveryMaxSlotAttemptsPerEpoch`; create `makeEpochTombstoneTxn(ns, first_absent, W, open)` with
       `Retry::once`. Committed: apply it, `X = W`. Conflict with a readable occupant: apply it through the same
       visitor and continue the walk from its successor (§2.3 table: an ordinary record keeps `X`; a tombstone
       sets `X` to the epoch it names; a terminal ends the loop). Unreadable occupant, `GaveUp`, `Refused`:
       today's retry-later arms (`:1271-1337`), nothing installed.
    4. Re-read `_ckpt`; a token other than `accepted.etag` restarts.
    5. Task 5a's confirmation (unchanged, in `ensureRefTableRecovered`), then install: `X > W` gives lane `Closed`; a terminal stream keeps today's closed
       positive lane; otherwise `Ready`. The runtime takes `accepted_ckpt`.
  - **Genesis.** A never-born stream is not skipped (`:1236-1242` goes): its first absent key is
    `{life_epoch,1}` and it gets a tombstone there like any chain. `validateEpochSealGrammarContextual` stays at
    `:1262` and at `CA/Pool/CasRefProtocol.cpp:979`: with a set-once `life_epoch` its rule (a backlink iff the
    epoch is above `life_epoch`) holds for a tombstone and for a base.
  - **Snapshot admission** (`:4381-4388`): the seal term reads `rt.state.getOpenTransition()` instead of
    `last_epoch_seal`.

- [ ] **Step 1: Write the failing tests.** Raw-epoch tests go to `T/gtest_cas_ref_recovery_cas_walk.cpp` (plan
  decision: that file owns the fixture), request-count tests on a live writer to `T/gtest_cas_ref_writer.cpp`.
  Old-format fixtures use `encodeRefLogTxnAtGeneration(txn, 1)` and `RefCkpt` with `generation = 1`.

  ```cpp
  /// T/gtest_cas_ref_writer.cpp, suite CASRefWriterAppendLane (warm namespace; counts are per call)
  TEST(..., PositiveAppendIsOneCatalogReadAndOneCreate)
  //  updateRefPublishedAt on a warm ref: total backend requests == 2: one GET of layout.refCatalogKey(), one
  //  write of the next `_log` key; getCount(ckpt_key) and writeCount(ckpt_key) unchanged
  TEST(..., RefRemovalIsOneRequest)
  //  dropRef on a warm ref: total requests == 1, the `_log` write; no `_ckpt` request, no catalog read
  TEST(..., LostAdmissionAfterDurablePutNeedsRecoveryAndIsNotAcknowledged)
  //  the fence generation moves in CarvePhaseForTest::PostDurableInstall: dropRef throws; the `_log` object
  //  exists; laneStateForTest(ns) == RefLaneState::NeedsRecovery
  TEST(..., DurableUnacknowledgedAppendIsReplayedByRecovery)
  //  continuing: the next resolveRef recovers; the dropped ref is absent (the durable log is history)
  TEST(CASRefWriterTombstone, LaneStampMismatchNeedsRecovery)
  //  fixture: `_ckpt` generation 1 {life W} of a never-born stream, W the live epoch; recovery installs Ready
  //  with accepted generation 1. A hook replaces `_ckpt` before the first append: the append throws
  //  NETWORK_ERROR, laneStateForTest == NeedsRecovery, no `_log` key was written
  //  without the hook: the stored `_ckpt` has header 3 before the first `_log` write (request journal order)

  /// T/gtest_cas_ref_recovery_cas_walk.cpp, suite CASRefWriterTombstone
  TEST(..., FirstAppendOfAnEpochWritesTombstoneThenHead)
  //  chain {1,1} birth, {1,2}; live epoch 4; one publish: readLogTxn {1,3} is T->4 (header 3);
  //  readLogTxn {4,1}.prev_epoch_seal == RefTxnId{1,3} and its header is 3; no key in epochs 2 and 3
  TEST(..., BacklinkSkipsEpochsThatNeverOpenedTheNamespace)
  //  burnEpochsUpTo(.., 6) without touching ns in 2..5: T{1,3}->6, H{6,1}<-{1,3}; no key in epochs 2 to 5
  TEST(..., TombstoneOnlyEpochIsClosedAtItsFirstSlot)                               /// §5 row 1
  //  durable T{1,3}->4 and nothing at {4,1}; live epoch 6: T{4,1}->6 with prev {1,3}; then H{6,1}<-{4,1}
  TEST(..., LatePredecessorAppendIsAdoptedAndTheTombstoneMovesOneSlot)              /// §5 rows 4, 5
  //  late_key = log {1,3} with an ordinary record that lands when the tombstone create is sent:
  //  the recovered table contains that record; T{1,4}->4
  TEST(..., LatePredecessorTombstoneIsAdoptedAndTheNextOneLinksToIt)                /// §5 row 3
  //  live epoch 6; late_key = log {1,3} holding T->4 (a tombstone of epoch 4 in flight) that lands at the create:
  //  recovery adopts it; T{4,1}->6 with prev {1,3}
  TEST(..., NewerTombstoneClosesTheLane)                                            /// §5 row 16
  //  a writer of epoch 4 is Ready at next id {4,2}; T{4,2}->7 is written under it: its next append throws
  //  INVALID_STATE; laneStateForTest == Closed. A recovery of epoch 4 over T{1,3}->7: lane Closed, no write
  TEST(..., TerminalOccupantEndsTheStream)                                          /// §5 row 14
  //  late_key = log {1,3} holding a remove_namespace: recovery writes no tombstone; the table is terminal
  /// same file, suite CASRefWriterRecovery
  TEST(..., DeadEpochIsSealedInOneWriteWithoutACkptWrite)
  //  quiet namespace, snapshot base {1,2}, live epoch 4, cold listRefs. Recovery is 7 requests, counted per key
  //  from the first `_ckpt` read: getCount(ckpt_key) == 2; getCount(log {1,2}) == 1; getCount(snap {1,2}) == 1;
  //  getCount(log {1,3}) == 1; writeCount(log {1,3}) == 1; one catalog GET more than admission's two;
  //  writeCount(ckpt_key) == 0
  TEST(..., UnresolvedTombstoneInstallsNothing)                                     /// §5 row 2
  //  ambiguous_put_substr = the tombstone key: listRefs throws NETWORK_ERROR; refTableRecoveredForTest is false
  TEST(..., StampsCkptBeforeTheFirstNewWrite)
  //  old-format pool: `_ckpt` generation 1 with committed_through {1,2}; cold listRefs at live epoch 4: in the
  //  request journal the `_ckpt` write (header 3, fields life_epoch only) precedes the write of log {1,3}
  TEST(..., RestartsWhenTheBaseMovesUnderTheWalk)                                   /// §5 row 17
  //  a hook publishes a snapshot (new `_ckpt` token) when log {1,3} is read: refRecoveryRestartsForTest == 1
  //  and the table equals the one recovered from the new base
  TEST(..., RestartsWhenTheBaseMovesUnderTheTombstoneCreate)                        /// Review Focus 2, §5 row 21
  //  before_cas_put on the tombstone key {1,3}: store ordinary records at {1,3} and {1,4}, a snapshot at {1,4}
  //  with its `_ckpt` publish, then delete log {1,3} (cleanup). The create succeeds in the emptied slot.
  //  Expected: refRecoveryRestartsForTest == 1; the installed table contains both records;
  //  the next tombstone is T{1,5}->4. Control without the injected writes: restarts == 0, T{1,3}->4
  TEST(..., FollowsOldSealsByPlusOneAndLinksAfterThem)                              /// §5 row 18
  //  old chain {1,1}, S{1,2}, S{2,1}<-{1,2} (header 1); live epoch 5: T{3,1}->5 with prev {2,1}, then H{5,1}<-{3,1}
  /// same file, suite CASRefWriterGenesis
  TEST(..., BirthInALaterEpochSealsTheCreationSlotAndWritesNoCkpt)                  /// §5 row 13
  //  namespace created at epoch 2 (`_ckpt` generation 3 {life 2}); first publish at live epoch 5:
  //  T{2,1}->5 without a backlink; the birth is H{5,1}<-{2,1}; writeCount(ckpt_key) == 0 since creation;
  //  the stored `_ckpt` still has life_epoch 2
  TEST(..., LateBirthOfAPredecessorIsAdoptedOrRefused)
  //  late_key = log {2,1} with a birth that lands at the tombstone create: the birth is history, T{2,2}->5
  //  the birth arriving after T{2,1}->5 is durable: its writer's create conflicts and its lane is Closed

  /// T/gtest_cas_ref_writer.cpp, suite CASRefWriterSnapshotPublish
  TEST(..., GenerationThreeBodyHasTwoFields)
  //  after a snapshot publication: readCkptForTest: generation == 3, life_epoch and checkpoint_snapshot_id set,
  //  committed_through and last_epoch_seal nullopt; the publication is 3 requests: `_snap` PUT, `_ckpt` GET, `_ckpt` PUT
  /// T/gtest_cas_ref_ckpt.cpp, suite CASRefCheckpoint
  TEST(..., DurableLifeEpochIsNeverReplaced)
  //  createCkptAtCreation(life_epoch 2), then createCkptAtCreation(life_epoch 5): stored life_epoch == 2
  //  publishCkptSnapshot({5,3}): life_epoch still 2, checkpoint_snapshot_id {5,3}
  TEST(..., OnlyCreationCreatesAnAbsentCkpt)
  //  on an absent key: publishCkptSnapshot({2,3}) writes nothing; stampCkptGeneration3 is TokenMismatch
  //  publishCkptSnapshot({2,2}) over a body at {2,3}: writeCount unchanged
  /// T/gtest_cas_format.cpp, T/gtest_cas_ref_epoch_seal_format.cpp, T/gtest_cas_pool.cpp
  TEST(CASFormat, RefLogAndRefCkptHistoriesRecordGenerationThree)   /// replaces ChainGenerationIsNotAdvertisedYet
  //  G_BUILD == 3; changePoints(RefLog).back() == {3,3}; changePoints(RefCkpt) == {{1,1},{3,3}}; every other class {1,1}
  TEST(CASRefEpochSealFormat, BacklinkHeadIsHeaderThree)   /// replaces BacklinkHeadKeepsItsHeaderBeforeTheWriterCommit
  //  head{ns, {5,1}, ops, RefTxnId{4,2}}: header v == 3; a record without a link and without a Move: v == 1
  TEST(CASPool, WritableMountRaisesTheFloorToThreeBeforeAnyGenerationThreeObject)
  //  pool meta floor 2; open: stored min_reader_generation == 3, and in the request journal that write precedes
  //  every `_ckpt` and `_log` write. DecommissionMountRaisesThePoolFloor now expects G_REF_CHAIN
  ```

- [ ] **Step 2: Run red.** Stubs: the three `_ckpt` functions calling `publishCkpt`, `makeEpochTombstoneTxn`
  calling `makeEpochSealTxn`, `stampBeforeFirstWrite` returning true. Filter
  `CASRefWriter*:CASRefRecoveryCasWalk*:CASRefCheckpoint*:CASFormat*:CASRefEpochSealFormat*:CASPool*`.
- [ ] **Step 3: Implement** in the order: `G_BUILD`, the histories, the header rule and the floor argument;
  `_ckpt` functions and `completeCreation`; recovery; append and stamp; wedge; snapshot publication.
- [ ] **Step 4: Run green; re-target.** Expected classes: the 39 `CASRefRecoveryCasWalk` tests and
  `T/gtest_cas_ref_wedge_every_attempt.cpp` (numeric seals, frontier publishes: class (a), rewritten where the
  schedule still exists); `CommittedChunkPublishesFrontierBeforeInstallAndAck`,
  `CheckpointConflictAfterLogCommitRequiresRecoveryWithoutInstall`,
  `FenceMovementAtCheckpointPublicationRequiresRecoveryWithoutInstall` (`T/gtest_cas_ref_writer.cpp:1450`, `:1598`,
  `:1693`: class (a), replaced by the four `CASRefWriterAppendLane` tests above); assertions on
  `committed_through` after a pool write in the other files (class (b)). `lastEpochSealForTest` and
  `setLastEpochSealForTest` are re-pointed at the open transition or removed with their tests.
- [ ] **Step 5: Gates.** `CAS*` on `build` (`t5c_gate`) and `build_asan` (`t5c_asan`).
- [ ] **Step 6: Integration lever.** In `tests/integration/test_cas_gcs_relink_liveness/test.py` move the delay
  from `_ckpt` `PUT` to `_log` `PUT` (`:30-32`) and keep the assertions at `:191` and `:224`. Run
  `cd $WT && python -m ci.praktika run "integration" --test test_cas_gcs_relink_liveness > build/test_t5c_integration.log 2>&1`;
  expected: passed. The other integration tests are run in Task 6.
- [ ] **Step 7: Commit.**
  `git add $CA/Formats/CasFormat.h $CA/Formats/CasFormat.cpp $CA/Formats/CasRefLogFormat.cpp $CA/Formats/README.md $CA/Pool/CasRefCkpt.h $CA/Pool/CasRefCkpt.cpp $CA/Pool/CasRefCatalog.cpp $CA/Pool/CasRefProtocol.h $CA/Pool/CasRefLedger.h $CA/Pool/CasRefLedger.cpp $CA/Pool/CasPool.cpp $T/cas_test_helpers.h $T/gtest_cas_ref_writer.cpp $T/gtest_cas_ref_recovery_cas_walk.cpp $T/gtest_cas_ref_ckpt.cpp $T/gtest_cas_format.cpp $T/gtest_cas_ref_epoch_seal_format.cpp $T/gtest_cas_pool.cpp tests/integration/test_cas_gcs_relink_liveness/test.py`
  plus the re-targeted tests. Subject: `Append a CAS ref log in one request under generation 3`. Body: a durable log
  on the chain is committed history, so no append, wedge resolution or recovery writes `_ckpt`; the same commit
  raises the pool floor and the build's read ceiling to 3, because an older reader takes a `_ckpt` without
  `committed_through` for an empty frontier.

### Task 6: Commit 6, the lazy lane tombstone and the selective rule {#task-6}

Implementer `ca-surgeon`. Reviewer `ca-review` (opus). Read §2.3, §2.4, §4, §5 rows 3, 9, 10, 11, 20. The state
after this task is model config `CaRefLogOnlyAppendCore_safe_lateland_selective`.

**Files:**
- Modify: `CA/Pool/CasRefLedger.h:96-120` (constructor), `:757`; `CA/Pool/CasRefLedger.cpp:207-240`, `:1595-1602`
  (the pair capture), `runRecoveryWalkOnce` (the seal condition), `flushRefBatch` at the deposed-lane check
  (`:3090-3115` today), `:4381-4388`; `CA/Pool/CasPool.cpp:250`
- Test: `T/gtest_cas_ref_recovery_cas_walk.cpp`, `T/gtest_cas_ref_writer.cpp`, `T/gtest_cas_pool.cpp`
- Test: `tests/integration/test_cas_ref_move_compat/test.py`, `tests/integration/test_cas_insert_fault_recovery/test.py`,
  `tests/integration/test_cas_ref_snaplog/`, `tests/integration/test_cas_gcs/`, `tests/integration/test_cas_drop_pool_member/`
- Create: `tests/queries/0_stateless/<number>_cas_ref_append_requests.sh` and `.reference` (through `add-test`)

**Interfaces:**
- Consumes: `LiveEpochPair`, `CasMountRuntime::liveEpochPair` (Task 4); `makeEpochTombstoneTxn`,
  `stampBeforeFirstWrite`, `RefTableRuntime::accepted_ckpt` (Task 5c).
- Produces:

  ```cpp
  /// CasRefLedger constructor: one more argument after live_epoch_fn_
  std::function<LiveEpochPair()> live_epoch_pair_fn_;
  /// The lane's tombstone step. True: the chain epoch equals the live epoch and the batch may proceed.
  bool CasRefLedger::ensureEpochOpenForAppend(const RootNamespace & ns, const std::shared_ptr<RefTableRuntime> & rt,
                                              std::vector<std::shared_ptr<RefMutationItem>> & owned_items);
  ```

- Rules:
  - **Recovery.** It captures `live_epoch_pair_fn()` once (`:1602`) and seals iff the stream is not terminal and
    `X <= pair.last_unclean_epoch` (and `X < W`). A recovery that writes nothing sends no stamp.
  - **Lane step**, in `flushRefBatch` at the place of today's deposed-lane check, before any preview and before
    id allocation. Read the live epoch once; re-check `may_mutate` and the superseded flags; compare with the
    chain epoch `X`. `X == W`: proceed. `X > W`: lane `Closed`, today's rejection. `X < W`: stamp if needed
    (`stampBeforeFirstWrite`), then a lane attempt for `makeEpochTombstoneTxn(ns, chain next id, W, open)`: armed
    (`Ready -> Writing`), sent with `Retry::standard`, installed after the lease recheck in the install hold; it
    sets the open transition. Unresolved: lane `Wedged`, the batch fails retry-later, no head is built.
    Conflict: own bytes install; a later epoch's tombstone gives `Closed`; any other readable occupant clears
    the attempt and gives `NeedsRecovery`; an unreadable occupant gives `Wedged`.
  - **Wedge resolution** of a tombstone attempt re-creates the same key and bytes (`resolveWedgeOnce` needs no
    new arm beyond installing the transition).
  - **Snapshot admission** (`:4381-4388`): one more term, the chain epoch equals the live epoch.
  - The lane tombstone needs no `_ckpt` re-read (§4).

- [ ] **Step 1: Write the failing tests**

  ```cpp
  /// T/gtest_cas_ref_recovery_cas_walk.cpp, suite CASRefWriterRecovery
  TEST(..., CleanPredecessorCostsSixReadsAndNoWrite)
  //  the fixture of DeadEpochIsSealedInOneWriteWithoutACkptWrite, the predecessor ended with a clean farewell:
  //  the same per-key read counts (6 reads), writeCount(log {1,3}) == 0, putTotal unchanged
  TEST(..., UncleanPredecessorSealsBeforeInstall)
  //  the same after an unclean end of epoch 1: T{1,3}->W is durable before listRefs returns (the 6 reads, 1 write)
  TEST(..., EagerRuleSurvivesACleanEpochThatNeverReadTheNamespace)                  /// transitivity
  //  epoch 1 writes ns and ends uncleanly; epoch 2 mounts, never touches ns, ends cleanly; epoch 3 reads ns:
  //  T{1,k}->3 is written (the marker stayed 1)
  TEST(..., UncleanReaderIsSealedByTheNextReaderAndTheWriterLinksThroughIt)         /// §5 row 11
  //  epoch 1 writes; epoch 2 reads and ends uncleanly; epoch 3 only reads; epoch 4 publishes:
  //  T{1,k}->3 (from the recovery of 3), T{3,1}->4 with prev {1,k}, H{4,1}<-{3,1}; no key in epoch 2
  /// suite CASRefWriterGenesis
  TEST(..., EagerRuleSealsANeverBornStream)
  //  created at epoch 2, epoch 2 ends uncleanly; a cold read at epoch 4 writes T{2,1}->4
  /// suite CASRefWriterTombstone
  TEST(..., BacklinkSkipsEpochsThatOnlyRead)                                        /// §5 row 10
  //  epoch 1 writes; epoch 2 reads ns and ends cleanly (no key in epoch 2, no tombstone); epoch 3 publishes:
  //  T{1,k}->3, H{3,1}<-{1,k}; no key in epoch 2
  TEST(..., AmbiguousTombstoneWedgesAndBlocksTheHead)                               /// §5 row 2
  //  ambiguous_put_substr = the tombstone key: the publish throws NETWORK_ERROR; refLaneWedgedForTest(ns);
  //  no `_log` key in the live epoch; after the fault clears, the next publish writes the same tombstone bytes,
  //  then {W,1}
  TEST(..., DetachedRuntimeInARemountWindowWritesNoTombstone)
  //  a batch is carved for the old runtime after setLiveWriterEpoch published the new epoch and before
  //  quiesceRefTablesForRemount (remount_quiesce_hook_for_test): it fails retry-later and writes no tombstone
  TEST(..., LateRecoveryTombstoneOfAnEarlierBuildIsAdopted)                         /// §5 row 20
  //  late_key = the chain's next slot holding T->U with U below the live epoch W, landing at the lane's create:
  //  the lane goes NeedsRecovery, recovery adopts it, the next publish writes T{U,1}->W with that backlink
  /// T/gtest_cas_ref_writer.cpp
  TEST(CASRefWriterStalePrecommitSweep, ReadingEpochWithStalePrecommitsWritesATombstoneAndOneRemoval)
  //  epoch 1 leaves a stale precommit and ends cleanly; a read at epoch 2 after the sweep is due:
  //  exactly two `_log` writes, T{1,k}->2 then the removal H{2,1}<-{1,k}; no `_ckpt` write
  TEST(CASRefWriterSnapshotPublish, ReadingEpochDoesNotPublish)
  //  tail above the snapshot threshold written by epoch 1; epoch 2 (clean predecessor) only reads:
  //  pendingSnapshotPublishesForTest(ns) == 0; no `_snap` and no `_ckpt` write
  /// T/gtest_cas_pool.cpp
  TEST(CASPool, SelfRemountRecoveryUsesTheRemountClaimsMarker)                      /// Review Focus 3
  //  epoch E writes ns; fence-out and self-remount to E2; the first read of ns under E2 writes T{E,k}->E2
  ```

- [ ] **Step 2: Run red** with `live_epoch_pair_fn_` plumbed and unused and `ensureEpochOpenForAppend` keeping
  Task 5c's arm. Filter `CASRefWriter*:CASRefRecoveryCasWalk*:CASPool*`.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run green; re-target** Task 5c tests that pinned the always-eager rule (class (a)): a test whose
  schedule needs a tombstone at recovery (`DeadEpochIsSealedInOneWriteWithoutACkptWrite`,
  `RestartsWhenTheBaseMovesUnderTheTombstoneCreate`, `StampsCkptBeforeTheFirstNewWrite`, the conflict tests) keeps
  an unclean predecessor in its fixture; the others assert the same durable objects, now written by the lane.
- [ ] **Step 5: Gates.** `CAS*` on `build` (`t6_gate`) and `build_asan` (`t6_asan`).
- [ ] **Step 6: Stateless test.** `./tests/queries/0_stateless/add-test cas_ref_append_requests.sh`; tag
  `no-fasttest` (object storage), disk as in `tests/queries/0_stateless/05054_cas_part_removal_one_ref_drop.sh:18-23`.
  Plan decision, the oracle: for one `INSERT` of one part and one `OPTIMIZE ... FINAL`, read
  `ProfileEvents` of the query from `system.query_log` and print the sums of the `CAS*Put` and `CAS*Get`
  counters of the classes that hold `_log`, `_ckpt` and the catalog (find the class in `CA/` by
  `grep -rn 'CASRootPut' $WT/src/Disks`). The reference must show zero `_ckpt` writes and 8 metadata requests for
  the blob-less part of §1; a different number is reported, not pasted.
- [ ] **Step 7: Integration tests.** Add to `test_cas_ref_move_compat/test.py`: an older server refuses to mount
  after a new server stamped a namespace (expects the generation error in its log), and an older GC leader
  that was already mounted holds the namespace and deletes nothing. Add to
  `test_cas_insert_fault_recovery/test.py`: a kill between the tombstone and the head, then a restart; the
  inserted rows are absent, the next insert succeeds, and the table is consistent. Re-target by `grep` for
  `_ckpt`, `committed_through`, `seal` in `test_cas_ref_snaplog`, `test_cas_gcs`, `test_cas_drop_pool_member`. Run:
  `python -m ci.praktika run "integration" --test "test_cas_ref_move_compat test_cas_insert_fault_recovery test_cas_ref_snaplog test_cas_gcs test_cas_drop_pool_member test_cas_gcs_relink_liveness" > build/test_t6_integration.log 2>&1`.
  If `integration-test:latest` fails with `exec format error`, set `DOCKER_BASE_TAG=<n>-amd64`.
- [ ] **Step 8: Commit.**
  `git add $CA/Pool/CasRefLedger.h $CA/Pool/CasRefLedger.cpp $CA/Pool/CasPool.cpp $T/gtest_cas_ref_recovery_cas_walk.cpp $T/gtest_cas_ref_writer.cpp $T/gtest_cas_pool.cpp tests/queries/0_stateless/<number>_cas_ref_append_requests.sh tests/queries/0_stateless/<number>_cas_ref_append_requests.reference`
  plus the integration test files changed in Step 7 and the re-targeted tests.
  Subject: `Write the CAS epoch tombstone lazily, eagerly after an unclean end`. Body: an
  epoch that does not write a namespace leaves no object; recovery still seals before install when the chain
  ends in an epoch that may have a request in flight, which the lease marker records.

### Task 7: Commit 7, remove what the switch left dead {#task-7}

Implementer `ca-impl`. Reviewer `ca-review` (opus). Read §2.6 and the "Texts, counters and docs" row of §13. A
symbol is deleted only when `grep -rn '<symbol>' $WT/src` (untruncated) shows no caller outside its own
definition and tests of the removed mechanism.

**Files:**
- Modify: `CA/Pool/CasRefCkpt.h`, `CA/Pool/CasRefCkpt.cpp:39-58`, `:94-148`, `:152-171`, `:226-356`;
  `CA/Pool/CasRefProtocol.h:726-795`, `CA/Pool/CasRefProtocol.cpp:847-942`; `CA/Pool/CasKeyReader.h:46-53`,
  `CA/Pool/CasKeyReader.cpp:8-30`
- Modify: `CA/Pool/CasRefLedger.h`, `CA/Pool/CasRefLedger.cpp` (`chainLinkFor`, `publishCkptContribution` `:4684`,
  `Occupant::SuccessorSeal` renamed `Tombstone`, the test accessors of `last_epoch_seal`)
- Modify: `CA/Gc/CasGc.cpp`, `CA/Gc/CasGc.h` (`FrontierUnproven::CheckpointFrontierEmpty`, `CommittedBelowCursor`,
  `crossFromSeal`)
- Modify: `src/Common/ProfileEvents.cpp:851`, `:882-885`, `:1014-1015`
- Modify (comments only): `CA/Pool/CasPool.cpp:889-895`, `:1600-1608`; `CA/Pool/CasPool.h:276-283`;
  `CA/Pool/CasRefLedger.cpp:1964-1970`; `CA/Pool/CasServerRoot.cpp:1033-1036`; `CA/Formats/CasRefCkptFormat.h:12-54`
- Test: the `T/` files that pin a deleted symbol

**Interfaces:**
- Removes: `mergeCkpt`, `publishCkpt`, `CkptPublishOutcome::SuccessorSeal`, `::Superseded`,
  `refuseFrontierAgainstDurable`, the `life_epoch` decrease refusal, `nextRefLogIdWithinCommittedFrontier`,
  `hintRefLogsWithinEpoch`, `discardRefLogHintsOfEpoch`, `crossEpochFromSeal` with `EpochCrossOutcome` and
  `EpochCrossResult`, `publishCkptContribution`, `chainLinkFor`, `CASRefCheckpointFrontierRefused`.
- Keeps, plan decision: `encodeRefCkpt` and `encodeRefLogTxnAtGeneration` for generations 1 and 2, because
  fixtures must write old objects; `checkRefCkptInvariants` for old bodies; `validateEpochSealGrammarContextual`
  (its rule needs only the first key); `committed_through` as `RefChainOrigin::old_frontier` and as a GC witness
  for a body below generation 3.
- Texts: `CASRefCheckpointPublished`, `CASRefCheckpointIdenticalSkip`, `CASRefCheckpointNotAdvanced` describe
  creation, the stamp and snapshot publication; `CASRefRecoveryEpochSealed` and `CASRefRecoveryEpochSealAdopted`
  say "tombstone", one per namespace per writing epoch; `CASRefAppendSealRejected` says "a successor's tombstone".
  The five comments justify the missing materialization wait by §4's rule (the successor seals before install
  when the chain ends in an epoch that may have a request in flight), not by "recovery closes every dead epoch".

- [ ] **Step 1: Pin before deleting.** Run the gate (`t7_before`); record the count.
- [ ] **Step 2: Delete** symbol by symbol with the `grep` rule; delete the tests whose only subject is a deleted
  symbol (`T/gtest_cas_ref_ckpt_join.cpp` merge tests; in `T/gtest_cas_ref_ckpt.cpp` the `Superseded` and
  `SuccessorSeal` cases and Task 1's two `publishCkpt` tests, which pinned builds below this series' switch);
  list them in the report.
- [ ] **Step 3: Verify.** `grep -rn 'SuccessorSeal\|Superseded\|mergeCkpt\|publishCkptContribution\|chainLinkFor\|crossEpochFromSeal\|nextRefLogIdWithinCommittedFrontier\|CASRefCheckpointFrontierRefused\|checkpoint frontier was not confirmed' $WT/src`
  prints nothing.
- [ ] **Step 4: Gates.** `CAS*` on `build` (`t7_gate`; expected: the count of Step 1 minus the deleted tests,
  listed) and `build_asan` (`t7_asan`). Style: `cd $WT && python3 ci/jobs/check_style.py > build/test_t7_style.log 2>&1`.
- [ ] **Step 5: Commit.** `git add` every file of the **Files** block that changed and the deleted or edited tests,
  each by name. Subject: `Remove the CAS checkpoint frontier and its dead helpers`. Body: nothing
  publishes or merges `committed_through` any more; keeping the code would keep a second, unused definition of
  where history ends.

### Task 8: Commit 8, docs {#task-8}

Implementer `ca-impl` (sonnet). Reviewer `ca-review-lite`. Plain words, no invariant tags, every heading with
an explicit `{#anchor}`.

**Files:**
- Modify: `D/bucket-requirements.md:92` (the `_ckpt` row: written at namespace creation, once at the upgrade
  stamp, and at snapshot publication)
- Modify: `D/architecture/manifests-and-refs.md:238-314` ("Publishing a ref mutation", "Recovery"): the commit
  rule, the tombstone and its forward link, how a reader finds the end, the request table of §1
- Modify: `D/architecture/mounts-and-leases.md` (the `last_unclean_epoch` field and what a clean farewell
  guarantees), `D/architecture/garbage-collection.md` (the cut at the first absent key, the 60 s intake slice,
  one more `GET` per quiet namespace per round), `D/architecture/storage-layout.md` (the two `_ckpt` fields)
- Modify: `D/configuration.md:346-350` and `D/operations/migration.md` (new section
  `## Upgrading a pool to reader generation 3 {#upgrade-generation-3}`: stop every older server, the floor is
  raised by the first writable mount, no switch, no downgrade, a 26.6 or older 26.8 server refuses to mount)
- Modify: `D/operations/monitoring.md` (the re-described counters, the retired one),
  `D/operations/debugging.md:295` (an older GC leader holds on a stamped namespace)
- Modify: `CA/Formats/README.md:21-24`, `:90-95`

- [ ] **Step 1:** Write the changes. Every number comes from §1 and is labelled "counted from the code".
- [ ] **Step 2:** `grep -n '^#' <each changed file> | grep -v '{#'` prints nothing; `grep -rn 'INV-\|W-ANCHOR' <changed D/ files>`
  prints nothing new.
- [ ] **Step 3: Commit.**
  `git add $D/bucket-requirements.md $D/configuration.md $D/architecture/manifests-and-refs.md $D/architecture/mounts-and-leases.md $D/architecture/garbage-collection.md $D/architecture/storage-layout.md $D/operations/migration.md $D/operations/monitoring.md $D/operations/debugging.md $CA/Formats/README.md`.
  Subject: `Document the CAS one-request ref append and generation 3`. No body.

### After Task 8 (controller, no commit in `WT`) {#after-task-8}

- [ ] Whole-branch review, then the `CAS*` gate on `build` and `build_asan` on the final head.
- [ ] In `M`, not in `WT`: record in `docs/superpowers/cas/AGENTS.md` (invariant 5) that the per-append `_ckpt`
  step was removed by an owner decision, and amend the CAS-321 note "the acknowledgement follows the checkpoint".
- [ ] Measurement: the A/B of CAS-487 again; `INSERT` is 25 requests today and 19 by §1's arithmetic.
