# CAS blob deletion intents: implementation plan {#cas-blob-deletion-intents-plan}

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
> An implementer receives three texts: [Global Constraints](#global-constraints), its own task, and the spec.

**Goal:** A CAS writer publishes a blob body with one `PUT` and no marker, reuses a body after `HEAD` and an intent
`GET`, and GC alone warns writers with immutable deletion intents; the build runs only this protocol, at pool
generation 4, and `clickhouse-disks cas-migrate-pool` moves an older pool once, offline.

**Architecture:** Fourteen commits in three phases.

- **Inert, Tasks 1 to 8**: identity, test facilities, generation 4 codecs, the intent object, the payload hash
  check, the new GC's decision rules, G4's `HEAD` in the fold, the migration tool. Markers run unchanged; `G_BUILD`
  stays 3, so no commit opens a generation 4 pool or decodes a header 4 `gc/state`.
- **The switch, Task 9**: `G_BUILD = 4`, the generation gate, generation 4 pool creation, the two-read writer and the
  intent GC in one commit, with every test it breaks. A marker writer beside an intent GC, or the reverse, adopts
  bodies GC is deleting.
- **After the switch, Tasks 10 to 14**: orphan adoption, marker code removal, `cas-fsck`, stateless and integration
  tests, measurements.

**Tech Stack:** C++ (`CA/`, `programs/disks`), GoogleTest (`unit_tests_dbms`), stateless and integration tests
(RustFS, GCS emulator), Markdown, Python (`utils/cas-yardstick`, `utils/ca-soak` in `M`).

**Spec:** `docs/superpowers/specs/2026-11-17-cas-blob-deletion-intents-design.md` on branch `cas-gc-rebuild`, at the
commit that adds this paragraph. It is binding; "§N" below is a section of it. A conflict between this plan and the
spec resolves to the spec, except the readings listed in [Spec readings](#spec-readings).

**Preconditions, not tasks.** Two model results of §10.1, committed under `M/docs/superpowers/models/` and
confirmed by the owner: "Required before the migration task" gates Task 8; "Required before the switch task" gates
Task 9. Tasks 1 to 7 need neither.

**Follow-up outside the product branch**, in `M/docs/superpowers/models/` once Task 11 lands: move
`CaGcCondemnMarkerGate` and `CaRetiredInRunFoldAbortWitness` to the README's removed models, and add a header note
to `CaRetiredInRun`, `CaBlobPublishCore`, `CaEdgeBeforeObserve`, `CaGcDestructiveGateCore`, `CaGcAckFloorCore`,
`CaGcAckFloorZombie`, `CaGcRoundDeferCore`, `CaRefDeltaIntakeCore`, `CaRelinkConfirmCore`, `CaIncarnationCore`,
`CaGcShardIncarnationCore`, `CaGcRootLocalPartManifestCore`: what each now stands for under intents.

## Global Constraints {#global-constraints}

### Paths {#paths}

| Name | Value |
|---|---|
| `WT` | `/home/mfilimonov/workspace/ClickHouse/cas-pr1`; branch `work/antalya-26.8/cas-blob-intents` from `0aa837e942e` (`antalya-26.8` after the merge of PR 2831) |
| `M` | `/home/mfilimonov/workspace/ClickHouse/master`, branch `cas-gc-rebuild` (spec, plan, models, `utils/cas-yardstick`, `utils/ca-soak`) |
| `CA/` | `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/` |
| `T/` | `src/Disks/tests/` |
| `D/` | `docs/en/antalya/cas/` |
| `W:` `G:` `N:` | `CA/Pool/CasPartWriteTxn.cpp`, `CA/Gc/CasGc.cpp`, `CA/Gc/CasBlobInDegree.cpp` |
| `B:` `Q:` `SR:` | `CA/Backend/CasObjectStorageBackend.cpp`, `CA/Backend/CasRequests.cpp`, `CA/Pool/CasServerRoot.h` |

Cited lines point into the base `0aa837e942e`, except lines in the files commit `ac144e848aa` changed
(`CA/Backend/CasBackend.h`, `B:`, `CA/ContentAddressedTransaction.{h,cpp}`): those were read at `ac144e848aa`, which
is not on the base, and differ by a few lines. Lines move as tasks land: find the named symbol, use the line as a
hint.

### Rules {#rules}

- **Intermediate commits.** No commit of this branch before the last one is ever run against a persistent pool
  (a stand, a soak, a developer's data). Such a run can leave states the final build trusts but did not write, for
  example Task 8's intents beside markers that a marker-era GC keeps condemning.
- **Inert means inert.** Tasks 1 to 8 change no decision of the marker protocol: no generation 4 object is written
  by a server path, and `G_BUILD` stays 3. Task 7 moves a request without changing what is sent or decided.
- **TDD.** Each task starts with failing tests, run red on an assertion, not a compile error: add a new signature
  with a stub first. Paste the red output into the task report. A test marked "pin" passes before and after.
- **Positive controls.** A test that asserts the absence of a request or an effect also asserts the work that would
  produce it happened: the round reclaimed at least one blob, an intent existed and was read, the run listed slots.
- **`LOGICAL_ERROR` tests** are split: the catching test under `#ifndef DEBUG_OR_SANITIZER_BUILD`, an
  `EXPECT_DEATH` twin in a `*DeathTest` suite under `#else`; include `<base/defines.h>`. Prove the intended arm
  compiled with `--gtest_list_tests` on `build` and on `build_asan`. A death suite never constructs
  `DiskObjectStorage` in the parent; a death-test child exits with `std::_Exit`.
- **C++.** Allman braces. No `sleep` to fix a race: hooks, latches, `FakeClock` (`T/cas_test_helpers.h:346`), the
  engine's injectable sleep. No fallback paths: an error propagates; nothing substitutes a default. Say
  "exception", not "crash", for logical errors. Comments say why in one or two lines; no task ids, no plan,
  backlog or history references.
- **Protocol constraints.** `HEAD` before `PUT` stays, and a body is never created conditionally. A condemned
  object is never read back to revive it. GC never throws on a 404: it records and continues. A `LIST` nominates
  and never decides a destruction. The on-store format is frozen: every new field is generation 4. No machinery
  without a schedule that needs it.
- **Shared code** outside `CA/`, `T/`, `programs/disks/`, `D/` changes only where a task lists it: `ProfileEvents.cpp`
  (Tasks 4 to 6, 10, 11), `ContentAddressedGarbageCollectionLog.cpp` (Task 9), `InterpreterSystemQuery.cpp` (Task 12),
  the tests Tasks 9 and 13 name, pages under `docs/en/` (Tasks 11, 12). Anything else is a stop.
- **Docs** under `docs/`: every heading has an explicit `{#anchor}`; plain words in `D/` (no invariant tags such
  as `I4`, `W3`, `G7`); functions as `f`, not `f()`; SQL names and classes in backticks; present behaviour, no history.
- **Stateless and integration tests** are written in their task and run by the controller. New stateless tests
  come from `tests/queries/0_stateless/add-test <name>[.sh]`. No `no-*` tag unless the test cannot run otherwise;
  a test with a failpoint carries `no-fasttest`. A reference value that was not run is listed as "predicted".

### Build and test {#build-and-test}

Build directories in `WT`: `build` (release), `build_asan` (ASan + UBSan). One foreground build at a time, `ninja`
without `-j`, output to a log there. A task that adds a `.cpp` first runs `cmake . > cmake_<tag>.log 2>&1` there.

```bash
cd $WT/build && ninja unit_tests_dbms > build_<tag>.log 2>&1; \
  echo "NINJA_EXIT=$? HEAD=$(git -C $WT rev-parse --short HEAD)" >> build_<tag>.log
mkdir -p $WT/build/gtest_tmp_<tag> && cd $WT/build && \
  TMPDIR=$WT/build/gtest_tmp_<tag> ./src/unit_tests_dbms --gtest_filter='<filter>' > test_<tag>.log 2>&1; \
  echo "GTEST_EXIT=$?" >> test_<tag>.log; rm -rf $WT/build/gtest_tmp_<tag>
grep -a 'NINJA_EXIT' $WT/build/build_<tag>.log
grep -a '\[  PASSED  \]\|\[  FAILED  \]\|GTEST_EXIT' $WT/build/test_<tag>.log | tail -20
```

- A test log counts only after `NINJA_EXIT=0` of the same tag. No `[  PASSED  ]`: the last `[ RUN      ]` aborted.
- **Gate** at the end of every task: the filter is EXACTLY `CAS*`, on `build`, on the committed tree. The same
  gate on `build_asan` after Tasks 4, 7, 8, 9, 10, 11 and 12. A red gate is fixed in a further commit; a red test
  the task does not explain is a stop: report the test, the assertion and the log.

### Commit {#commit}

One commit per task, by exact file list; never `git add -A` or `git add .`; no amend, no rebase, no push.

```bash
cd $WT && git add <files> && git diff --cached --name-only   # must equal the task's list
git commit -F - <<'EOF'
<imperative subject from the task, at most 72 characters>

<body only when the why is not obvious; literal names in backticks>

Signed-off-by: Mikhail Filimonov <mfilimonov@altinity.com>
Co-Authored-By: Claude Fable 5.2 <noreply@anthropic.com>
EOF
```

### Test vocabulary {#test-vocabulary}

- A live `Etag` exists only through a request: `openRequestsForTest(backend).admit().head(key, Retry::standard())`
  (`T/cas_test_helpers.h:126`). `ScopedReadCeiling(n)` (`:370`) sets the read ceiling to `n`, below or above
  `G_BUILD`: 3 is "an older reader"; `G_BLOB_INTENTS` lets Tasks 3 to 8 read what generation 4 writes.
- Backends: `InMemoryBackend` (dialect `emulated`), `CountingBackend` (`T/cas_test_helpers.h:1862`),
  `BatchCapabilityBackend` (`:2120`), `LandedButAckLostOnceBackend` (`:2473`), and the backends of Task 2.
- Fixtures: `writeBlobBody` (`T/cas_test_helpers.h:1098`), `displaceBlobToken` (`:1039`), `writeMetaClean`
  (`:1133`), `condemnMeta` (`:1144`), `seedPoolMetaForRestart` (`:1082`), `runRegularRoundReclaiming` (`:918`),
  `runRoundsUntilAbsent` (`:929`), `currentRetiredSet` (`:949`). Task 2 adds `seedOlderPoolForTest` and
  `DialectOverrideBackend`.
- **Request journal** (Task 2): `CountingBackend::journal()` is the ordered list of requests the backend received,
  each `(verb, keys)`, recorded on entry, under a mutex. `markCutComplete(gc, backend)` makes the round append the
  boundary `cut_complete` when intake ends. Assertions read "no request of kind X after Y" and "every X after
  `cut_complete`".

## Review Focus {#review-focus}

The inputs most likely to break this for an operator, which the spec implies and no spec test exercises directly.
Each is pinned to a test in the task that owns the code.

1. **A decision `HEAD` sent during intake** decides on a stale body. Task 9, `DecisionHeadsAreSentAfterTheCut`.
2. **One incarnation spelled two ways**, quoted in the row and bare at the `HEAD`. Task 9,
   `RespelledTokenAtTheDeleteHeadIsTheSameIncarnation`: the body is deleted and no transition happens.
3. **Several GC shards.** Task 9 runs the lifecycle and G7 with the blob in shard 3 of 4; Task 10,
   `OrphanAdoptionWithFourShards`, puts the only nomination in a shard with no delta.
4. **A marker listing longer than one page.** Task 8, `MarkersAcrossListPagesAreAllRead`, pages of 3 keys.
5. **A 26.6 pool.** Task 8, `FloorOnePoolIsRaisedAfterM5AndCoversEveryBody`; Task 13 on a 26.6.4 pool.
6. **A migrated pool rebuilt.** Task 10, `RebuildOnAMigratedPoolKeepsHeader4AndAdoptsEveryIntent`.

## Task map {#task-map}

"Green" below means the gate passes on the committed tree. "Mixed" means a writer and a GC on different protocols.

| # | Task | Depends on | Kind, size | After the commit: why green, why no mixed path |
|---|---|---|---|---|
| 1 | Token identity and the probe check | — | inert, S | new functions, their own tests; the probe refuses one more store. Markers only |
| 2 | Shared test facilities | — | test-only, M | helpers and their own tests; no production change |
| 3 | Generation 4 codecs, inert | 1 | inert, M | codecs tested under `ScopedReadCeiling(4)`; production stamps today's headers. Ceiling 3 refuses any floor 4 pool, so no build opens one |
| 4 | Intent object, key, request class, create confirmation | 1, 3 | inert, M | nothing creates an intent |
| 5 | Verified streaming publication | 2 | protocol-neutral, S | every streamed body is hashed; every fixture supplies its digest; markers only |
| 6 | Intent rules, settings and counters, inert | 1, 3, 4 | inert, M | rules and the delete sender tested directly; no round calls them; the new setting reaches `PoolConfig` unused |
| 7 | G4's `HEAD` in the fold, under the marker protocol | 2, 6 | protocol-neutral, M | same requests and outcomes; the `HEAD` is still after the cut; the redelete suites move with it |
| 8 | `clickhouse-disks cas-migrate-pool` | 1 to 4 | inert, L | tested under `ScopedReadCeiling(4)`; a pool it migrates is refused by every server of this phase (ceiling 3) |
| 9 | The switch | 1 to 8 | activating, XL | gate, generation 4 creation, writer and GC in one commit; every breaking test retargeted or retired in it |
| 10 | Adopt orphan intents | 9 | intent protocol, M | adds a reclamation path; no rule of Task 9 changes |
| 11 | Remove the marker code; protocol docs | 9, 10 | removal, M | removed code has no caller; kept fixtures rewritten on raw requests |
| 12 | `cas-fsck` for intents and legacy markers | 8, 9 | intent protocol, M | the pairing classes, advisory until now, are replaced |
| 13 | Stateless and integration tests | 9 to 12 | tests, M | run by the controller |
| 14 | Measurements, soak, defaults | 13 | measurement, L | commits cards in `M`; a default changes only after the owner decides |

| Producer → consumer | Interface |
|---|---|
| 1 → 3, 4, 6, 8, 9, 12 | `CanonicalIdentity`, `canonicalIdentity`, `Etag::canonical`, `PersistedEtag::canonical`, `sameIncarnation`, `dialectWord`, `dialectFromWord`, `opaqueTagFromHex` |
| 2 → 5 to 13 | `CountingBackend::journal`, `JournalEntry`, `markCutComplete`, `EtagFaithfulPublicationBackend` (`T/cas_publication_test_support.h`), `HeadHookBackend`, `ContentEtagBackend`, `seedOlderPoolForTest`, `DialectOverrideBackend` |
| 3 → 4, 6, 8, 9, 10 | `G_BLOB_INTENTS`, `PoolMeta::token_dialect`, `GcState::header_generation`, `GcState::intent_sweep_cursor`, `RetiredEntry::standing`, `CondemnedRow::standing`, `SourceEdgeRecord::standing`, `requireGeneration4Pool`, `CasOperation::backendDialect` |
| 4 → 6, 8, 9, 10, 12 | `Layout::blobIntentKey`, `writeOnceBlobIntentKey`, `parseBlobIntentKey`, `Layout::ParsedBlobIntentKey`, `blobIntentsPrefix`; `blobIntentRecord`, `isBlobIntentRecord`; `createBlobIntent`, `IntentCreateResult` |
| 6 → 9, 10 | `decideIntentStep`, `IntentRuleInput`, `IntentStep`, `classifyDecisionHead`, `Gc::sendIntentDeletes`, `PoolConfig::gc_round_intent_create_budget` |
| 7 → 9 | `BodyDelete`, `RetiredMergeResult::body_deletes` |
| 8 → 9, 12, 13 | `migratePool`, `Pool::openForMigration`, `removeLegacyMarkers` |
| 9 → 10, 11, 12 | `IntentCreateFn` parameter of `foldDeltasIntoGeneration`, `RetiredMergeResult::intent_deletes` |

---

### Task 1: Token identity and the probe check {#task-1}

Read §6.1. Nothing calls the identity functions yet except the probe.

**Files:**
- Modify: `CA/Backend/CasEtag.h:10-14` (comment: two grammar levels), `:64-88` (`PersistedEtag`); new declarations
- Modify: `CA/Backend/CasEtag.cpp` (`isIncarnationValue`, `:10-32`, stays byte for byte; new functions after it)
- Modify: `CA/Backend/CasProbe.cpp:47` (`runCapabilityProbe`)
- Create: `T/gtest_cas_token_identity.cpp`; modify `T/gtest_cas_probe.cpp`

**Interfaces:**
- Consumes: `enum class Dialect { ETag = 1, Generation = 2, Emulated = 3 }` (`CA/Primitives/CasTypes.h:269`).
- Produces, in `CA/Backend/CasEtag.h`:

  ```cpp
  struct CanonicalIdentity
  {
      Dialect dialect;
      String tag_hex;   /// lowercase hex of the opaque tag
      bool operator==(const CanonicalIdentity &) const = default;
  };
  std::string_view dialectWord(Dialect dialect);                   /// "etag" | "generation" | "emulated"
  std::optional<Dialect> dialectFromWord(std::string_view word);
  std::optional<CanonicalIdentity> canonicalIdentity(Dialect dialect, std::string_view raw_value);
  std::optional<String> opaqueTagFromHex(Dialect dialect, std::string_view tag_hex);   /// inverse, for key parsing
  std::optional<CanonicalIdentity> Etag::canonical() const;
  std::optional<CanonicalIdentity> PersistedEtag::canonical() const;   /// unknown dialect word: nothing
  bool sameIncarnation(const PersistedEtag & row, const Etag & live);  /// both canonicalise and are equal
  ```

- Rules: `etag` and `emulated` strip one pair of double quotes when the value has at least two bytes and both ends
  are `"`; then reject an empty tag, a `W/` prefix on the raw value, any `"` left, a byte below `0x20` or `0x7F`,
  a leading or trailing space, the tag `*`, a comma, a tag over 128 bytes. `generation`: `[1-9][0-9]{0,19}`, no
  strip. No case folding. `PersistedEtag::matches` stays: the manifest nomination compares with it (`G:1550`). Its
  blob-row callers (`G:593`, `N:616`, `CA/Tools/CasFsck.cpp:1134`) move to `sameIncarnation` in Tasks 9 and 12.
- Probe: after its scratch writes, `runCapabilityProbe` takes the token of one scratch `HEAD`; no identity →
  `NOT_IMPLEMENTED`, `the store returns a token that cannot serve as a blob identity: <token>`. It runs where the
  probe runs today (writable mount and pool creation, `CA/Pool/CasPool.cpp:546`; `skip_access_check` skips it) and
  in Task 8.

- [ ] **Step 1: Write the failing tests,** suite `CASTokenIdentity`:
  `QuotedAndBareSpellingsAreOneIdentity` (`"\"abc\""` and `abc` under `ETag` give `{ETag, "616263"}`);
  `CaseIsNotFolded`; `RejectedEtagForms` (each of `""`, `"\"\""`, `W/"abc"`, `"abc`, `abc"`, `a"b`, ` abc`, `abc `,
  `a\tb`, `a\x7f`, `*`, `"*"`, `a,b`, a 129-byte tag gives nothing; a 128-byte tag is accepted);
  `GenerationGrammar` (accept `1`, a 20-digit value; reject `0`, `01`, empty, `1a`, `"1"`, 21 digits);
  `EmulatedUsesTheEtagGrammar`; `SameIncarnationComparesCanonically` (persisted `{etag, "\"abc\""}` against live
  `abc` is true; another dialect false; `W/"x"` against `W/"x"` false); `OpaqueTagFromHexRoundTrips` (rejects
  uppercase, odd length, hex of a rejected tag); pin `ControlKeyGrammarIsUnchanged`
  (`isIncarnationValue(ETag, "W/\"x\"")` true, `(ETag, "a,b")` false); pin `EveryTestBackendTokenCanonicalises`
  (`InMemoryBackend`, and `ObjectStorageBackend` over `makeLocalObjectStorageForTest`, `T/cas_test_helpers.h:439`).
  `CASProbe.RefusesATokenThatIsNotAnIdentity` (a backend whose `HEAD` returns `W/"x"`: the exact text above, no
  `_pool_meta` written; positive control: the same probe over `InMemoryBackend` writes `_pool_meta`).
- [ ] **Step 2: Run red.** Stubs: `canonicalIdentity` returns nothing. Filter `CASTokenIdentity*:CASProbe*`.
- [ ] **Step 3: Implement** the functions and the probe check.
- [ ] **Step 4: Run green,** then the gate.
- [ ] **Step 5: Commit.** Subject: `Add a canonical token identity for CAS blob incarnations`

### Task 2: Shared test facilities {#task-2}

Test code only. Later tasks consume these by the names below.

**Files:**
- Modify: `T/cas_test_helpers.h`: the journal in `CountingBackend` (`:1862-2094`); new `markCutComplete`,
  `seedOlderPoolForTest`, `DialectOverrideBackend`. Move `HeadHookBackend`, `ApplyParking`, `ContentEtagBackend` there
  from `T/cas_gc_marker_test_support.h:250-370`, which keeps the rest
- Create: `T/cas_publication_test_support.h` with `EtagFaithfulPublicationBackend`, moved out of the anonymous
  namespace of `T/gtest_cas_s3_staging.cpp:53` (definition `:227`), now a `CountingBackend`; its constructor (`:237`)
  also takes `FaultScript::None`, an honest store. Create `T/gtest_cas_test_support.cpp`

**Interfaces:**
- Produces:

  ```cpp
  /// T/cas_test_helpers.h
  enum class JournalVerb : uint8_t { Read, Head, List, Write, Remove, RemoveMany, Publish, Stream, Mark };
  /// keys: one, every key of a batch delete, or none for a mark. detail: write condition or mark label.
  struct JournalEntry { JournalVerb verb; std::vector<String> keys; String detail; };
  std::vector<JournalEntry> CountingBackend::journal() const;            /// copy, in issue order
  void CountingBackend::mark(std::string_view label);
  void CountingBackend::clearJournal();
  /// Wraps the round's phase sink: appends `cut_complete` when the `fold_ref_intake` row is emitted (`G:3389`),
  /// on the round thread, so every request issued later, on any thread, follows it in the journal.
  void markCutComplete(DB::Cas::Gc & gc, CountingBackend & backend);
  /// `_pool_meta` at `floor` (1, 2 or 3) with no dialect, written with raw `encodePoolMeta`, and an empty catalog,
  /// as `seedPoolMetaForRestart` (`:1082`) does.
  void seedOlderPoolForTest(DB::Cas::Backend & backend, uint64_t floor, const String & pool_prefix = "p",
                            uint64_t gc_shards = 1);
  class DialectOverrideBackend;   /// a CountingBackend whose dialect() returns the constructor argument
  ```

- Rules: an entry is appended before the request reaches the inner backend, so the journal order is the issue order
  across threads. A batch delete is one entry with every key. A failed or refused request still has its entry.
- [ ] **Step 1: Write the failing tests,** suite `CASTestSupport`: `JournalKeepsIssueOrderAcrossThreads` (two
  threads, latched, interleaved `HEAD`s: the journal order equals the latch order); `BatchDeleteIsOneEntryWithEveryKey`;
  `RefusedRequestIsJournaled`; `CutCompleteFollowsIntakeAndPrecedesTheFold` (a round over two blobs at zero: the mark
  sits after the last ref-log read of intake and before the first blob `HEAD`; positive control: both exist);
  `PublicationFakeWithoutAScriptPublishesOnce`; `SeedOlderPoolWritesTheFloor` (floors 1 and 3).
- [ ] **Step 2: Run red.** Filter `CASTestSupport*`.
- [ ] **Step 3: Implement and move.** `cmake .` first (new `.cpp`). The moved classes keep their names; their
  includers (`T/gtest_cas_gc_read_ahead.cpp`, `T/gtest_cas_gc_meta_writer.cpp`) compile unchanged.
- [ ] **Step 4: Run green,** then the gate.
- [ ] **Step 5: Commit.** Subject: `Add a request journal and shared publication fake to CAS tests`

### Task 3: Generation 4 codecs, inert {#task-3}

Read §2, §2.2, §3.5. The codecs read and write every generation 4 field. No server path writes one: `G_BUILD`
stays 3, so this build still refuses a floor 4 `_pool_meta` and a header 4 `gc/state`.

**Files:**
- Modify: `CA/Formats/CasFormat.h:16` (new `G_BLOB_INTENTS = 4`; `G_BUILD` unchanged)
- Modify: `CA/Formats/CasPoolMetaFormat.h:25-33` (`PoolMeta`), `CA/Formats/CasPoolMetaFormat.cpp` (encode; decode
  after the ceiling check, `:155-158`)
- Modify: `CA/Formats/CasGcStateFormat.h:31-40`, `CA/Formats/CasGcStateFormat.cpp:38-56` (`:42` header), decode `:58-`
- Modify: `CA/Formats/CasRecordStreamFormat.h:84-94`, `CA/Formats/CasRecordStreamFormat.cpp:198-205` and its reader
- Modify: `CA/Gc/CasBlobInDegree.h:27-43` (`RetiredEntry`), `:77-92` (`CondemnedRow`); `N:262-290` (packed row),
  `:320-327`, `:490-498`, `:655-665`
- Modify: `CA/Tools/CasInspect.cpp:413-418` (render `standing`)
- Modify: `CA/Pool/CasPool.h`, `CA/Pool/CasPool.cpp` (`requireGeneration4Pool`, no caller); `CA/Backend/CasRequests.h`
  (`backendDialect`)
- Test: `T/gtest_cas_pool_meta.cpp`, `T/gtest_cas_gc_state_format.cpp`, `T/gtest_cas_record_stream_format.cpp`,
  `T/gtest_cas_blob_indegree.cpp`, `T/gtest_cas_format_battery.cpp`; create `T/gtest_cas_blob_intent_generation.cpp`

**Interfaces:**
- Consumes: `writeHeaderLine(out, id, compatibility_version)` (`CA/Formats/CasTextFormat.h:153`), `expectHeaderLine`
  (`:302`), `rmwPoolMeta` (`CA/Pool/CasPoolMeta.cpp:69-86`), `dialectWord` (Task 1).
- Produces:

  ```cpp
  constexpr uint32_t G_BLOB_INTENTS = 4;
  struct PoolMeta { /* existing */ std::optional<Dialect> token_dialect; };   /// present iff floor >= 4
  struct GcState { /* existing */ String intent_sweep_cursor; uint32_t header_generation = <today's stamp>; };
  struct RetiredEntry { /* existing */ bool standing = false; };      /// standing implies delete_pending
  struct CondemnedRow { /* existing */ bool standing = false; };      /// also in operator==
  struct SourceEdgeRecord { /* existing */ bool standing = false; };
  /// CasPool.h. Refuses a pool below generation 4 or one whose recorded dialect differs from `backend_dialect`.
  void requireGeneration4Pool(const PoolMeta & meta, Dialect backend_dialect);
  /// CasRequests.h, in CasOperation
  Dialect backendDialect() const;
  ```

- Rules:
  - `_pool_meta`: wire key `token_dialect` after `algos_used`, written iff set, value `dialectWord`. Decode, after
    the ceiling check: present below floor 4 → `CORRUPTED_DATA`; missing at floor 4 → `CORRUPTED_DATA`; unknown
    word → `UNKNOWN_FORMAT_VERSION`. Every read-modify-write keeps it because it re-encodes the decoded struct.
  - `gc/state`: `encodeGcState` stamps `header_generation` with `writeHeaderLine(out, FormatId::GcState,
    state.header_generation)`; its default is today's stamp, so every production writer still writes today's
    bytes. `decodeGcState` sets `header_generation` from the header. `intent_sweep_cursor` is written after
    `manifest_sweep_cursor` only at header 4; absent decodes as empty; present below header 4 → `CORRUPTED_DATA`.
  - Run rows: key `standing` on a `Condemned` record, written only when true, so candidate and pending rows keep
    today's bytes. `standing` without `pending`, or on a non-condemned record → `CORRUPTED_DATA`. Packed row: flag
    `4` is `standing`; `flags > 7`, or `4` without `1` → `CORRUPTED_DATA` (`N:287`). Because a standing row is
    also `delete_pending`, `CondemnedSummary::pending_total` counts it and `graduationDue` folds the shard
    (`G:4373-4403`); the fold seal format does not change.
  - `requireGeneration4Pool`: the texts and codes of §7.5 for a floor below 4 and for another dialect.
  - The header 4 `gc/state` battery row supplies `make_future_version` (header version `G_BLOB_INTENTS + 1`): the
    default search for `"v":<currentCompatibilityVersion>` (`T/cas_format_test_battery.h:143-152`) finds nothing.
- [ ] **Step 1: Write the failing tests.** Under `ScopedReadCeiling(G_BLOB_INTENTS)` unless stated. `CASPoolMeta`:
  `TokenDialectRoundTrips`, `TokenDialectMissingAtFloor4IsCorrupted`, `TokenDialectBelowFloor4IsCorrupted`,
  `UnknownTokenDialectWordIsUnknownFormat`, `TokenDialectSurvivesAdmitOrValidate` (`CA/Pool/CasPoolMeta.cpp:98-123`),
  `TokenDialectSurvivesEveryReadModifyWrite`. `CASGCStateFormat`: `EncoderStampsTheHeaderGeneration` (4 gives
  header 4; pin: the default gives today's golden bytes), `DecoderReportsTheHeaderGeneration` (headers 1, 3, 4),
  `IntentSweepCursorRoundTrips`, `AbsentIntentSweepCursorDecodesEmpty`, `IntentSweepCursorBelowHeader4IsCorrupted`.
  `CASBlobIntentGeneration.ReadCeiling3RefusesPoolMetaAndGcState`, at the default ceiling: the first two texts of
  §7.5. `CASBlobIntentGeneration.RequireGeneration4PoolRefusesFloor3`,
  `.RequireGeneration4PoolRefusesAnotherDialect`, `.RequireGeneration4PoolAcceptsItsDialect`. `CASRecordStream`:
  `StandingRowRoundTrips`, `AbsentStandingDecodesFalse`, `StandingWithoutPendingIsCorrupted`,
  `StandingOnANonCondemnedRecordIsCorrupted`, pin `CandidateAndPendingBytesAreUnchanged` (golden bytes captured
  before Step 3). `CASBlobInDegree`: `PackedStandingFlagRoundTrips`, `PackedFlagsAboveSevenAreCorrupted`,
  `PackedStandingWithoutPendingIsCorrupted`, `RunViewAndRetiredEntryCarryStanding`. Format battery: the header 4
  `gc/state` row.
- [ ] **Step 2: Run red.** Fields added, codecs unchanged. Filter `CASPoolMeta*:CASGCStateFormat*:CASRecordStream*:CASBlobInDegree*:CASFormat*:CASBlobIntentGeneration*`.
- [ ] **Step 3: Implement.** `cmake .` first.
- [ ] **Step 4: Run green,** then the gate. No existing test changes: production bytes are unchanged.
- [ ] **Step 5: Commit.** Subject: `Add generation 4 codecs for CAS blob intents`

### Task 4: Intent object, key, request class, create confirmation {#task-4}

Read §2.1 and "What confirms a create" in §3.2. Nothing creates an intent in production yet.

**Files:**
- Modify: `CA/Formats/CasFormat.h:31-64` (`BlobIntent = 26`); `CA/Formats/CasFormat.cpp:47-73` (change points
  `{4, 4}` for `BlobIntent`), `:132-160` (`TRAITS` row)
- Create: `CA/Formats/CasBlobIntentFormat.h`, `CA/Formats/CasBlobIntentFormat.cpp`
- Modify: `CA/Formats/CasLayout.h:118-131`, `CA/Formats/CasLayout.cpp:35-45`
- Modify (contract comments): `CA/Primitives/CasWriteOnceKey.h:10-14`, `CA/Backend/CasBackend.h:203-210`
  (`removeManyWriteOnce`), `:140-147` (the "never reused" sentence holds for the in-memory backend only; the
  emulated local backend can repeat a token inside one mtime tick, A1)
- Modify: `CA/Backend/CasInstrumentedBackend.h:27-36`, `CA/Backend/CasInstrumentedBackend.cpp:115-136`;
  `src/Common/ProfileEvents.cpp` (four counters near `:833`)
- Create: `CA/Gc/CasBlobIntents.h`, `CA/Gc/CasBlobIntents.cpp`
- Test: create `T/gtest_cas_blob_intent.cpp`; modify `T/gtest_cas_format_battery.cpp`, `T/gtest_cas_observability.cpp`

**Interfaces:**
- Consumes (Task 1): `CanonicalIdentity`, `dialectWord`, `dialectFromWord`, `opaqueTagFromHex`. Existing:
  `CasOperation::create` (`CA/Backend/CasRequests.h:355`), `WriteResult` (`CA/Backend/CasWriteResult.h:71`).
- Produces:

  ```cpp
  /// CasLayout.h, in Layout
  static constexpr size_t kBlobIntentKeyMaxBytes = 1024;
  static constexpr size_t kBlobIntentKeySuffixMaxBytes = 356;   /// after the pool prefix
  static constexpr size_t maxPoolPrefixBytesForBlobIntents() { return kBlobIntentKeyMaxBytes - kBlobIntentKeySuffixMaxBytes; }
  String blobIntentsPrefix() const;                             /// "<prefix>/blob-intents/"
  String blobIntentKey(const BlobRef & ref, const CanonicalIdentity & identity) const;
  WriteOnceKey writeOnceBlobIntentKey(const BlobRef & ref, const CanonicalIdentity & identity) const;
  struct ParsedBlobIntentKey { BlobRef ref; CanonicalIdentity identity; };   /// consumers write Layout::ParsedBlobIntentKey
  std::optional<ParsedBlobIntentKey> parseBlobIntentKey(std::string_view key) const;
  /// CasBlobIntentFormat.h
  const String & blobIntentRecord();                 /// {"type":"cas_blob_intent","v":4}\n{}\n
  bool isBlobIntentRecord(std::string_view bytes);   /// byte equality only
  /// CasBlobIntents.h
  enum class IntentCreate : uint8_t { Created, ConfirmedExisting, Unconfirmed };
  struct IntentCreateResult { IntentCreate outcome; String detail; bool confirmed() const; };
  IntentCreateResult createBlobIntent(CasOperation & op, const Layout & layout, const BlobRef & ref,
                                      const CanonicalIdentity & identity, const Retry & policy);
  /// CasInstrumentedBackend.h
  enum class CasNs : uint8_t { Blob = 0, Manifest, Root, Gc, Server, Other, BlobIntent };   /// CAS_NS_COUNT = 7
  ```

- Rules:
  - Key `<prefix>/blob-intents/<algo>/<hex[0:2]>/<hex>/<dialect>/<tag_hex>`: the segments of `blobKey` with
    `blob-intents` for `blobs`, then the identity. Over 1024 bytes: `LOGICAL_ERROR`. `parseBlobIntentKey` accepts
    only what `blobIntentKey` produces (§2.1 "Parse"); `parseBlobKey` keeps rejecting an intent key.
  - `TRAITS` row: `{FormatId::BlobIntent, "cas_blob_intent", TextFamily::Control, CompressionPolicy::PinnedRaw, 4 * kKiB, 1 * kKiB}`.
    Its battery row decodes under `ScopedReadCeiling(G_BLOB_INTENTS)` and supplies `make_future_version` (`"v":5`).
  - `classifyCasNs` tests `/blob-intents/` before `/blobs/`. Counters `CASBlobIntentGet`, `CASBlobIntentPut`,
    `CASBlobIntentDelete`, `CASBlobIntentList`; operations this protocol never sends on an intent key map to the
    `CASOther*` counters of their kind.
  - `createBlobIntent` sends `op.create(key, blobIntentRecord(), policy)` and maps the result by §3.2:
    `Committed` → `Created` (an unanswered attempt resolved by equal bytes is `Committed`, `Q:1127-1128`);
    `Conflict` whose observation is an `Object` with `isBlobIntentRecord` bytes → `ConfirmedExisting`; any other
    `Conflict`, `GaveUp`, `Refused`, `Declined`, or a `DB::Exception` other than `LOGICAL_ERROR` → `Unconfirmed`
    with the reason in `detail`. A `LOGICAL_ERROR` propagates.
- [ ] **Step 1: Write the failing tests.** `CASBlobIntentKey`: `KeyMirrorsTheBlobKeyAndAppendsTheIdentity` (a
  literal key for a fixed ref and `{ETag, "616263"}`), `ParseRoundTrips` (each dialect),
  `ParseRejectsEachForeignSpelling` (unknown algo; shard other than `hex[0:2]`; unknown dialect; uppercase hex; odd
  length; a tag the grammar rejects; one segment more; one less; a `.meta` suffix), pin
  `BlobKeyParserRejectsAnIntentKey`, `LongestKeyIs356BytesAfterThePrefix`, `MaxPoolPrefixIs668Bytes`, the split pair
  `KeyOver1024BytesIsALogicalError` / `CASBlobIntentKeyDeathTest.KeyOver1024BytesIsALogicalError`,
  `WriteOnceKeysDeleteInOneRequest` (`BatchCapabilityBackend`, five keys, one absent: one journal entry, no exception).
  `CASBlobIntentFormat`: `RecordIsTheLiteralBytes`, `OnlyTheLiteralBytesAreARecord` (one byte more, `"v":3`, an
  extra key: false), the battery row. `CASBlobIntentCreate`: `CommittedIsCreated`,
  `UnansweredCreateResolvedByEqualBytesIsCreated` (`LandedButAckLostOnceBackend`), `EqualOccupantIsConfirmedExisting`,
  `OtherBytesAreUnconfirmed`, `ProvenAbsentConflictIsUnconfirmed`, `GaveUpIsUnconfirmed`, `RefusedIsUnconfirmed`.
  `CASObservability.IntentRequestsCountInTheirOwnClass` (a `GET`, a create, a delete and a `LIST` on an intent key
  move only the four new counters; `CASBlobGet`, `CASBlobPut`, `CASBlobDelete` stay).
- [ ] **Step 2: Run red** with stubs. Filter `CASBlobIntent*:CASObservability*:CASFormat*`.
- [ ] **Step 3: Implement.** `cmake .` first (new `.cpp` files).
- [ ] **Step 4: Run green,** then the gate on `build` and `build_asan`.
- [ ] **Step 5: Commit.** Subject: `Add the CAS blob intent object, key and create confirmation`

### Task 5: Verified streaming publication {#task-5}

Read §6.2. Safe under either protocol.

**Files:**
- Modify: `CA/Backend/CasBackend.h:55-60` (`StreamingBlobPublication` gains the expected `BlobRef`)
- Modify: `B:937-955` (check before `out->finalize`), `B:564` (emulated path, before its rename);
  `CA/Backend/CasInMemoryBackend.cpp:194-230`
- Modify: `W:497` (pass `ref`); `src/Common/ProfileEvents.cpp` (`CASBlobPublishDigestMismatch`)
- Modify: every test construction of `StreamingBlobPublication`, enumerated by
  `grep -rn 'StreamingBlobPublication{' $WT/src/Disks`: `T/gtest_cas_backend.cpp:61`, `:110`, `:336`,
  `T/gtest_cas_backend_generation.cpp:682`, `:713`, `T/gtest_cas_blob_publish_transport.cpp:62`
- Test: create `T/gtest_cas_blob_publish_digest.cpp`

**Interfaces:**
- Consumes: `makeBlobHashingWriteBuffer(BlobHashAlgo, WriteBuffer &)` (`CA/Primitives/CasBlobHashingWriteBuffer.h:40`);
  `EtagFaithfulPublicationBackend` (Task 2).
- Produces: `struct StreamingBlobPublication { uint64_t payload_size; String fresh_envelope; std::function<std::unique_ptr<ReadBuffer>()> open_payload; BlobRef expected_ref; };`
- Rules: hash exactly the payload bytes forwarded, not the envelope, with the hashing buffer of `expected_ref.algo`,
  and compare with the key's digest before the object completes. Mismatch: `out->cancel`, increment the counter,
  `CORRUPTED_DATA` `payload of <key> does not hash to its key; upload aborted, nothing published`. The writer
  treats it as deterministic: no further attempt. The verbatim staged copy (`B:959-977`) is not hashed. Each
  existing fixture sets `expected_ref` to the ref of its own payload; a fixture that tests a mismatch keeps a
  wrong one.
- [ ] **Step 1: Write the failing tests,** suite `CASBlobPublishDigest`: `FlippedByteIsRefusedBeforeCompletion`
  (local object storage and `InMemoryBackend`: no object at the key afterwards, counter 1);
  `StaleSourceOfASecondAbsentPublisherIsRefused` (S10: the source changes to equal-length other bytes between
  `HEAD` and publish; the earlier body stays); `S3StagedRestreamIsVerified` (over `EtagFaithfulPublicationBackend`);
  `MismatchIsNotRetried` (one publication attempt); `LargePayloadUsesTheStreamingConvention` (a payload over one
  hashing block publishes).
- [ ] **Step 2: Run red.** Field added, no check. Filter `CASBlobPublishDigest*`.
- [ ] **Step 3: Implement,** then update the existing fixtures. `cmake .` first.
- [ ] **Step 4: Run green,** then the gate.
- [ ] **Step 5: Commit.** Subject: `Hash every streamed CAS blob before its object completes`

### Task 6: Intent rules, settings and counters, inert {#task-6}

Read §3.1, §3.2, §3.5. The decision rules of the new GC as pure functions, the intent delete sender, the create
budget setting, and the counters and reasons. Nothing calls them in a round until Task 9.

**Files:**
- Create: `CA/Gc/CasIntentRules.h`, `CA/Gc/CasIntentRules.cpp`
- Modify: `CA/Gc/CasGc.h`, `G:541-580` (`sendIntentDeletes` beside `removeCohortWriteOnce` and `bulkDeleteChunkKeys`)
- Modify: `CA/ContentAddressedSettings.cpp:69` (new `gc_round_intent_create_budget` after
  `gc_round_graduation_budget`, default 0, description: `Maximum deletion-intent creates per GC round, summed over
  every GC shard; a blob over the cap stays a candidate and is created in a later round (0 = unbounded)`);
  `CA/Pool/CasPool.h:105` (`PoolConfig::gc_round_intent_create_budget` beside the old field);
  `CA/ContentAddressedMetadataStorage.h:649` (member beside the old one), `CA/ContentAddressedMetadataStorage.cpp:73`
  (extern), `:299` (initializer), `:800` (copy into `pool_config`)
- Modify: `src/Common/ProfileEvents.cpp` (`CASGCIntent` + `Created`, `ConfirmedExisting`, `UnconfirmedCarry`,
  `Deleted`, `LeftOnSpare`, `TokenRejected`; `CASBlobReuseIntentPresent`, `CASBlobReuseTokenRejected`);
  `CA/Pool/CasPartWriteTxn.h:86-90` (`BlobPublicationReason` gains `IntentPresent`, `TokenRejected`)
- Test: create `T/gtest_cas_intent_rules.cpp`; modify `T/gtest_cas_settings.cpp`

**Interfaces:**
- Consumes (Tasks 1, 3, 4): `sameIncarnation`, `Etag::canonical`, `PersistedEtag::canonical`, `RetiredEntry::standing`,
  `writeOnceBlobIntentKey`. Existing: `removeCohortWriteOnce` (`G:562`), `bulkDeleteChunkKeys` (`G:576`).
- Produces:

  ```cpp
  /// CasIntentRules.h
  enum class HeadSeen : uint8_t { Same, Absent, Other, Uncanonical };
  struct DecisionHead { HeadSeen seen; std::optional<Meta> observed; };
  DecisionHead classifyDecisionHead(const PersistedEtag & row_token, const std::optional<Meta> & observed);
  /// None: no row, no request. Create: G2, G3 (pending on a confirmed result, else candidate). Carry. Drop (G6).
  /// DeleteBody: standing(t) and DELETE B If-Match observed (G4). Standing: G4 on an absent body. Transition (G7).
  /// DeleteIntent: delete I(B,t), drop the row (G8). Anomaly: standing(t) whose HEAD shows t; carried, one ERROR.
  enum class IntentStep : uint8_t { None, Create, Carry, Drop, DeleteBody, Standing, Transition, DeleteIntent, Anomaly };
  struct IntentRuleInput
  {
      std::optional<RetiredEntry> row;   /// from the adopted state
      bool at_zero = false, fresh_transition = false, condemned_this_round = false, suppressed = false;
      bool create_budget_admits = false, redelete_budget_admits = false;
      std::optional<DecisionHead> head;  /// the decision HEAD of this round, when one was sent
  };
  IntentStep decideIntentStep(const IntentRuleInput & input);
  /// Gc: deletes intent keys in chunks of `bulkDeleteChunkKeys` through `removeCohortWriteOnce`; an absent key is
  /// success; a refused or unanswered chunk throws naming one of its keys.
  void Gc::sendIntentDeletes(CasOperation & op, const std::vector<WriteOnceKey> & keys);
  ```

- Rules: `decideIntentStep` is the table of §3.2 and the uncanonical rows of §3.5, nothing more:
  - not at zero, any row → `Drop` (G6);
  - no row, fresh transition, `head` `Same` → `Create` when the budget admits, else `None` with the caller writing
    `candidate(t)` (G2); `Absent` or `Uncanonical` → `None`;
  - `candidate(t)` at zero, not condemned this round, not suppressed, budget admits → `Create` (G3), else `Carry`;
  - `pending(t)` at zero, not suppressed, redelete budget admits: `Same` → `DeleteBody`, `Absent` → `Standing`,
    `Other` → `Transition`, `Uncanonical` → `Carry`; otherwise `Carry`;
  - `standing(t)` at zero, not suppressed: `head` absent or `Absent` → `DeleteIntent` (G8); `Other` → `Transition`;
    `Same` → `Anomaly`; `Uncanonical` → `Carry`; suppressed → `Carry`;
  - any row whose `head` shows `Other` takes `Transition` before `DeleteBody` or `DeleteIntent` (G7 order);
  - a row whose own token does not canonicalise: never `DeleteBody`, `DeleteIntent` or an intent delete of its token;
    `Other` gives `Transition` without the intent delete (§3.5).
- [ ] **Step 1: Write the failing tests.** `CASIntentRules`: `EveryRowOfTheStateTable` (parametrized over row state
  × in-degree × head × suppression × budget, unreachable cells excluded and listed; each expected step written by
  hand from §3.2, not computed),
  `TransitionPrecedesBodyAndIntentDeletes`, `SpareDropsEveryRowState`, `UncanonicalRowTokenNeverNamesAnIntent`,
  `StandingRowWhoseHeadShowsItsTokenIsAnAnomaly`, `RespelledTokenIsTheSameIncarnation`. `CASGcIntentDeletes`:
  `ChunksShareOneBatchRequest` (`BatchCapabilityBackend`, chunk 2, five keys: three journal entries),
  `AbsentKeyIsSuccess`, `RefusedChunkThrowsNamingAKey`, `OneByOneOnAStoreWithoutBatchDelete`.
  `CASContentAddressedSettings`: `IntentCreateBudgetDefaultsToUnbounded`, `IntentCreateBudgetReachesThePool` (a
  disk built as in `T/gtest_cas_gc_stop_start.cpp:94` with `cas_gc_round_intent_create_budget = 7`:
  `poolForTest()->poolConfig().gc_round_intent_create_budget == 7`). `CASObservability.IntentCountersAreRegistered`.
- [ ] **Step 2: Run red** with stubs. Filter `CASIntentRules*:CASGcIntentDeletes*:CASContentAddressedSettings*:CASObservability*`.
- [ ] **Step 3: Implement.** `cmake .` first.
- [ ] **Step 4: Run green,** then the gate.
- [ ] **Step 5: Commit.** Subject: `Add the CAS intent decision rules and create budget, unused`

### Task 7: G4's `HEAD` in the fold, under the marker protocol {#task-7}

Read §3.2 G4 and G12. Today's pending delete sends `HEAD B` and the exact `DELETE` in a job after the fold
(`G:583-595`). This task sends the `HEAD` from the fold and leaves only the `DELETE` to the job.

It preserves behaviour under the marker protocol. The requests are the same: `HEAD B`, then `DELETE B If-Match`
the observed token when `matches` holds. The outcomes `Removed`, `Gone`, `Mismatch` reach `applyRedeleteOutcome`
(`G:597-650`) unchanged, with its marker delete. The fold starts after intake, so the `HEAD` still follows the cut.
A failed `HEAD` still fails the round before its commit, now before the batch sends any `DELETE`. The touched-key
`peek_head` (`N:612-631`) is not reused as the delete `HEAD` here; Task 9 does that.

**Files:**
- Modify: `CA/Gc/CasBlobInDegree.h:246-264` (`RetiredMergeResult::redelete` becomes `body_deletes`),
  `N:545-557` (the pending branch of `settleEntry` takes its `HEAD` from the fold's read-ahead, as `head_blob` does)
- Modify: `CA/Gc/CasGc.h:800` (`performRedeleteIo` takes a `BodyDelete` and sends only the `DELETE`), `:811-817`
  (`redeleteBlobs` takes `const std::vector<BodyDelete> &`); `G:583-595`, `G:659-762`, `G:2154-2175` (hints)
- Test: create `T/gtest_cas_gc_redelete_head.cpp`; modify `T/gtest_cas_gc_redelete_concurrency.cpp` and every suite
  the gate turns red that asserts the `HEAD` inside the job

**Interfaces:**
- Consumes: `reads.hintHead` and the fold's `HEAD` callbacks (`G:2163-2200`), the journal (Task 2).
- Produces: `struct BodyDelete { RetiredEntry entry; std::optional<Meta> observed; };` in `CA/Gc/CasBlobInDegree.h`,
  where `observed` is the fold's `HEAD` answer and empty means absent.
- Rules: the keys of `delete_pending` rows the merge can settle are hinted through `reads.hintHead` after
  `fold_ref_intake` ends, as G2's candidates are (`G:2163-2172`), so the `HEAD`s run concurrently on the GC I/O
  pool; an unhinted key is read inline. No hint is issued before the cut.
- [ ] **Step 1: Write the failing tests,** suite `CASGcRedeleteHead`: `DeleteHeadIsSentInTheFoldAfterTheCut` (the
  journal: every pending row's `HEAD` follows `cut_complete` and precedes the round's first run-object write; at
  least one such `HEAD`); `PendingHeadsAreReadAhead` (`gc_io_concurrency = 8`, four pending rows at zero, latched
  `HEAD`s: more than one in flight); `FailedHeadFailsTheRoundBeforeAnyDelete`. Pins, run green before Step 3:
  `RequestsPerPendingRowAreUnchanged` (same token: `HEAD`, `DELETE`; absent: `HEAD`; other: `HEAD`),
  `MarkerDeleteFollowsRemovedAndGone`, `MismatchKeepsTheMarker`.
- [ ] **Step 2: Run red.** Filter `CASGcRedeleteHead*`.
- [ ] **Step 3: Implement,** then re-target `T/gtest_cas_gc_redelete_concurrency.cpp`: its cases that park the
  `HEAD` inside a job now park it in the fold; its interleavings keep their schedule.
- [ ] **Step 4: Run green,** then the gate on `build` and `build_asan`.
- [ ] **Step 5: Commit.** Subject: `Send the CAS pending-delete HEAD from the GC fold`

### Task 8: `clickhouse-disks cas-migrate-pool` {#task-8}

Read §7 in full, §7.5 and Q1, Q3, Q7, Q8, Q10, Q11. Precondition: the migration model of §10.1. Until Task 9 no
server opens a pool this tool migrated: `G_BUILD` is 3 and refuses floor 4. Tests read what the tool wrote under
`ScopedReadCeiling(G_BLOB_INTENTS)`.

**Files:**
- Create: `CA/Tools/CasMigratePool.h`, `CA/Tools/CasMigratePool.cpp`, `programs/disks/CommandCaMigratePool.cpp`
- Modify: `programs/disks/ICommand.h:144` (declaration), `programs/disks/DisksApp.cpp:351-355` (registration and the
  deferred open), `programs/disks/CMakeLists.txt:22-25`
- Modify: `CA/Pool/CasPool.h:441-447`, `CA/Pool/CasPool.cpp` (`Pool::openForMigration`); `CA/Pool/CasPool.h:943`
  (`raiseReaderFloor` becomes public, unchanged)
- Modify: `CA/ContentAddressedMetadataStorage.{h,cpp}` (`startup` at `:846` defers the pool open when the process flag
  is set; new `openPoolForMigration`, built as `openPoolView` builds its config, `:722-844`)
- Modify: `CA/Formats/CasPoolMetaFormat.h`, `CA/Pool/CasPoolMeta.cpp` (`raiseToGeneration4`)
- Modify: `CA/Tools/CasFsck.h`, `CA/Tools/CasFsck.cpp` (`removeLegacyMarkers` only; Task 12 adds the classes)
- Modify docs: `D/operations/migration.md`, "Migrating a pool to generation 4": stop, run the tool, start; the
  refusals; the points of no return; the downtime; `--intents-for-all-bodies`, when forced and when to pass it; R1
  to R4 in plain words. `D/operations/troubleshooting.md`: the refusals.
- Test: create `T/gtest_cas_migrate_pool.cpp`

**Interfaces:**
- Consumes (Tasks 1 to 4): `runCapabilityProbe`, `createBlobIntent`, `encodeGcState`, `GcState::header_generation`,
  `requireGeneration4Pool`, `seedOlderPoolForTest`. Existing: `Pool::raiseReaderFloor` (`CA/Pool/CasPool.cpp:628-640`),
  `computeHeartbeatFloor` and `mountObservationThresholdMs` (`SR:387`, `:425-466`), `probeNonTerminalMountSlots`
  (`SR:505`), `deleteMetaExact`, `decodeBlobMeta`.
- Produces:

  ```cpp
  struct MigratePoolOptions
  {
      uint64_t grace_ms = 120'000;
      bool intents_for_all_bodies = false;   /// forced on when M1 reads a floor below 3
      bool dry_run = false;
      uint64_t poll_step_ms = 1'000;
      std::optional<size_t> stop_after_step_for_test;   /// throws after step N (1..9)
  };
  struct MigratePoolReport { std::array<uint64_t, 9> counts{}; uint64_t floor_read = 0; bool completed = false; };
  MigratePoolReport migratePool(Pool & pool, const MigratePoolOptions & options,
                                const std::function<void(std::string_view)> & progress);
  static PoolPtr Pool::openForMigration(BackendPtr backend, PoolConfig config);   /// floor 1 to 4; no mount, slot, writer or GC
  static PoolMeta PoolMeta::raiseToGeneration4(CasOperation & op, const Layout & layout, Dialect dialect);
  bool deferPoolOpenForMigration();   /// process flag, set only by DisksApp for this command
  /// F3: deletes each key with `op.removeCurrent`; returns the count; a refused or unanswered delete throws
  /// with the count deleted so far in the message.
  uint64_t removeLegacyMarkers(CasOperation & op, const std::vector<String> & meta_keys);
  ```

- Rules: M1 to M9 in the order of §7.1, with the refusal texts of §7.5.
  - **M1** accepts floors 1 to 4; below 3 it sets `intents_for_all_bodies` and the progress line says so. **M2**
    watches slots on the tool's clock (the disk's `mount_lease_ttl_ms` as observer lease, `poll_step_ms` as cadence),
    then fences through the replace `computeHeartbeatFloor` uses; waits use the request engine's injectable sleep.
    **M5** confirms every create by `createBlobIntent`; `Unconfirmed` fails the run before M6.
  - **M6**: a floor below 3 is raised first by `Pool::raiseReaderFloor(pool, G_REF_CHAIN)`, the mount's own function
    (`CA/Pool/CasPool.cpp:628-640`, called at `:904`). It writes only `min_reader_generation` through
    `PoolMeta::raiseMinReaderGeneration` (`CA/Pool/CasPoolMeta.cpp:114-123`), and needs `_pool_meta`, the layout
    and an admitted operation, which the tool's pool has; it needs no mount, slot or lease. The raise runs after
    M5 because it erases the floor that forced the option: a rerun that finds floor 3 has finished M5 with every
    body. Then `gc/state`, header 4. M6 is skipped when `gc/state` already has header 4.
  - **M8** `raiseToGeneration4` refuses a body at floor 4 with another dialect. **M9** deletes with `If-Match` the
    token of the M4 read; on a migrated pool it runs `removeLegacyMarkers`. The command prints the per-step counts,
    then `migration complete`. `--dry-run` runs M1 without the probe, `probeNonTerminalMountSlots`, M3 and M4, and
    writes nothing, the raise included.
  - No server path reaches `Pool::openForMigration`: `git -C $WT grep -n 'openForMigration\|deferPoolOpenForMigration' -- src programs`
    lists only `CasPool.*`, `ContentAddressedMetadataStorage.*`, `CasMigratePool.*`, `DisksApp.cpp`,
    `CommandCaMigratePool.cpp`.
  - Fixtures are written with raw requests: `_pool_meta` by `seedOlderPoolForTest` at floor 1 or 3, markers by
    `writeMetaClean` and `condemnMeta`, condemned rows as encoded runs under a `gc/state` at header 1 or 3. No
    fixture runs a marker-era round, so the tests stay valid through Tasks 9 and 11.
- [ ] **Step 1: Write the failing tests,** suite `CASMigratePool`: `RefusesASlotThatRenews`,
  `FencesASilentSlotAfterTheThreshold`, `RefusesAMovingGcLease`, `RefusesAnotherDialectOnAMigratedPool`,
  `RefusesAPoolPrefixOver668Bytes`, `CondemnedMarkerGetsAnIntentForTheCurrentToken`, `UndecodableMarkerCountsAsCondemned`,
  `EveryCondemnedRowGetsAnIntent`, `PendingRowWithAnAbsentOrReplacedBodyGetsAnIntentForItsOwnToken`,
  `AbsentBodyAndUncanonicalTokenGetNoIntentForTheCurrentBody`, `UnconfirmedCreateFailsTheRun`,
  `RowsAreCarriedAsTheyAre`, `GcStateIsRewrittenAtHeader4WithItsFieldsAsRead`, `AbsentGcStateIsCreatedAtHeader4`,
  `LateOldGcCommitRestartsTheRun`, `RecheckRefusesASlotThatAppearedAfterTheGcStateRewrite`, `RecheckRefusesAMovedGcPair`,
  `FloorIsTheLastProtocolWrite`, `NoMarkerIsDeletedBeforeTheFloor`, `CrashAfterEachStepThenRerunCompletes` (one case
  per step), `RerunOfAMigratedPoolOnlyRemovesMarkers`, `TwoRunsConverge`, `ARunWithAnotherDialectLosesM8AndIsRefused`,
  `DryRunWritesNothingAndSendsNoProbe` (positive control: it read `_pool_meta` and listed slots),
  `ProbeFailureRefusesAtM1`, `AllBodiesOptionGivesEveryListedBodyAnIntent`, `MarkersAcrossListPagesAreAllRead`,
  `DeferredStartupSendsNoRequest` (with the flag set, a disk `startup` sends nothing).
  Floor 1: `FloorOnePoolIsRaisedAfterM5AndCoversEveryBody` (no option passed: every listed body gets an intent; the
  journal shows the floor 3 write after the last intent create and before the `gc/state` write, then floor 4);
  `CrashAfterTheRaiseThenRerunCompletes` (stop after the raise: floor 3; the rerun does not force the option; every
  intent of the first run stands); `DryRunOnAFloorOnePoolWritesNothing`.
  `CASRemoveLegacyMarkers`: `DeletesEveryKey`, `RefusedDeleteThrowsWithTheCount`.
- [ ] **Step 2: Run red.** Filter `CASMigratePool*:CASRemoveLegacyMarkers*`.
- [ ] **Step 3: Implement.** `cmake .` first.
- [ ] **Step 4: Run green,** then the gate on `build` and `build_asan`; if `clickhouse` is already built (do not
  build it here), note the command in `./programs/clickhouse disks --help`.
- [ ] **Step 5: Commit.** Subject: `Add clickhouse-disks cas-migrate-pool for generation 4`

### Task 9: The switch {#task-9}

Read §2.3, §3.1, §3.2, §3.4, §4.3, §6.1 (the dialect rule), §7.3, §7.5 and the ledger of §1. Precondition: the model
of record of §10.1. One commit: before it every path runs markers and no build opens a generation 4 pool; after it
every path runs intents and no build opens anything else. The marker code (`CA/Gc/CasGcMetaWriter.*`, the writer
helpers of `CA/Pool/CasBlobMeta.*`) stays compiled and loses every caller.

**Files (production):**
- Gate: `CA/Formats/CasFormat.h:16` (`G_BUILD = 4`); `CA/Pool/CasPool.h:450` (`mount_reader_floor = G_BLOB_INTENTS`);
  `CA/Pool/CasPool.cpp:425-611` (`Pool::open`: the prefix check before a mint, `requireGeneration4Pool` right after
  `_pool_meta` is read), `:904` (the call of `raiseReaderFloor` deleted; the function stays for Task 8), `:1011`
  (`openForDecommission` calls `requireGeneration4Pool`); `CA/Pool/CasPoolMeta.cpp:125-175` (a mint writes floor 4
  and `token_dialect = op.backendDialect()` in the one create); `CA/Formats/CasGcStateFormat.h` (the default of
  `header_generation` becomes `G_BLOB_INTENTS`)
- G1: `G:5185-5270` (`acquireOrRenewLease`), `G:4488` (`rebuildBaseline`), `G:5061` (`previewDeletes`)
- Writer: `W:274-570` (`ensureBlobPresent`); `CA/Pool/CasPartWriteTxn.h:86-90` (`BlobPublicationReason` keeps
  `Absent`, `IntentPresent`, `TokenRejected`)
- GC: `CA/Gc/CasBlobInDegree.h:246-264` (`RetiredMergeResult`: `graduated` goes, `intent_deletes` added),
  `:283-290` (`max_graduations` and its accessors renamed `max_intent_creates`), `:395-468` (contract and signature
  of `foldDeltasIntoGeneration`; `GraduationReadHints` deleted); `N:517-668` (`settleEntry`, `closeBlob` call
  `decideIntentStep`); `G:583-763` (the batch sends `body_deletes`, then `sendIntentDeletes`), `G:818-824` (the round
  copies `gc_round_intent_create_budget`), `G:1052` (phase `pending_deletes` also sends intent deletes),
  `G:2154-2287` (the hooks), `G:5061` (`previewDeletes` reasons), the round's `meta_writer` calls
- Settings: `CA/Pool/CasPool.h:105` (`PoolConfig::gc_round_graduation_budget` deleted);
  `CA/ContentAddressedMetadataStorage.h:649`, `.cpp:73`, `:299`, `:800` (the old budget's member, extern,
  initializer and copy deleted); `CA/ContentAddressedSettings.cpp:69` (`gc_round_graduation_budget` stays declared,
  described as ignored)
- Report: `CA/Gc/CasGc.h:168-169` (comments of `graduated`, `redeleted`);
  `src/Interpreters/ContentAddressedGarbageCollectionLog.cpp:50-51` (the two column descriptions only)
- `src/Common/ProfileEvents.cpp` (descriptions of the counters whose meaning changes)

**Files (tests):** create `T/gtest_cas_blob_intent_writer.cpp`, `T/gtest_cas_gc_intents.cpp`; modify the generation,
migration and helper files of Tasks 2, 3, 8, `T/gtest_cas_pluggable_hash.cpp:713`, and every file of the audit below.

**Interfaces:**
- Consumes: Tasks 1 to 8.
- Produces:

  ```cpp
  /// CasBlobInDegree.h; `confirm_condemned_marker` and `graduation_reads` leave the signature
  using IntentCreateFn = std::function<IntentCreateResult(const BlobRef &, const CanonicalIdentity &)>;
  /// new parameter of foldDeltasIntoGeneration, after `peek_head`:  const IntentCreateFn & create_intent
  struct RetiredMergeResult
  {
      /* still_retired, spared, replaced, unmatched_removes stay; graduated goes */
      std::vector<BodyDelete> body_deletes;      /// G4, HEAD showed the row's identity
      std::vector<WriteOnceKey> intent_deletes;  /// G7, G8
  };
  ```

- **Gate rules.** `requireGeneration4Pool` runs on every open (writable, read-only, decommission) right after
  `_pool_meta` is read, before any request on a blob or an intent. Before a mint, `Pool::open` refuses a pool prefix
  longer than `maxPoolPrefixBytesForBlobIntents()`: `BAD_ARGUMENTS`, the prefix text of §7.5 with limit 668. G1: a
  decoded `gc/state` with `header_generation < 4` fails the lease step, the rebuild and the dry run with
  `INVALID_STATE`, `cas_gc_state is at generation <n> in a generation 4 pool`, and no request after that read. A
  mount never raises the floor.
- **Writer rules (W1 to W7).** `HEAD B` (W2); present → size checks as today, then `t.canonical()`: none → publish,
  reason `TokenRejected`, `CASBlobReuseTokenRejected`, one ERROR per mount (W3); else `op.read` of
  `blobIntentKey(ref, t)` (W4): an object → publish, reason `IntentPresent`, event `BlobReuseResurrect`,
  `CASBlobReuseIntentPresent`; an answered 404 → adopt, `CASBlobBodyPutAvoided`, event `BlobReuseAdopt`. A refused
  or unanswered `HEAD` or `GET` throws: never absence. The verbatim staged copy only when W2 answered absent and
  `beginPublication` is the first (W5). No request after a publication (W6). Both reads through `op.head` and
  `op.read` of this attempt (W7). The loop, its deadline and the cap of 8 stay (`W:296`, `:406`, `:415-423`), and
  so do the admission checks before each proof (`W:454`, `:466`, `:545`, `:562`).
- **GC rules.** All decided in the fold after the cut, by `decideIntentStep` (§3.2 "Order inside one round"):
  - G2 in `closeBlob`, G3 and G4 in `settleEntry`; `Create` calls `create_intent` and writes `pending(t)` on a
    confirmed result, else `candidate(t)`; only a create that is sent consumes a budget slot.
  - The decision `HEAD` of a touched key (`N:612-633`) is the `HEAD` of every rule for that row in the round, for
    every answer: no second `HEAD`. Other keys use Task 7's read-ahead `HEAD`.
  - The batch sends `op.remove(key, observed.etag)` per `BodyDelete`; `Removed`, `Gone`, `Mismatch` are success;
    then `sendIntentDeletes`. Any failed job throws and the round fails before its commit (G5).
  - Task 6's counters; `CASGCIntentUnconfirmedCarry` and `CASGCIntentTokenRejected` log one ERROR per round.
    `previewDeletes` reasons: `unreachable`, `candidate`, `pending` (body delete), `standing` (intent delete).
  - The create budget is one `GcRoundWorkBudget` per round (`G:818-824`), passed to each shard's fold in turn
    (`G:3757-3778`): per round, summed over shards.
- **Round report.** `RoundReport::graduated` counts rows committed `pending` this round (confirmed creates of G2,
  G3, G7); `redeleted` counts body deletes answered this round. `system.cas_gc_log` keeps `entries_graduated` and
  `entries_redeleted` with those meanings; Task 11 documents them.

- [ ] **Step 1: Write the failing gate tests,** suites `CASBlobIntentGeneration` and `CASBlobIntentDialect`, each
  asserting zero blob and zero intent requests in the journal after the refusal, with the `_pool_meta` read as the
  positive control: `WritableMountOfAGeneration3PoolIsRefusedAndNamesTheTool`, `ReadOnlyOpenIsRefused`,
  `DecommissionOpenIsRefused`, `NewPoolIsCreatedAtGeneration4WithItsDialect`, `MountNeverRaisesTheFloor`,
  `GcStateBelowHeader4FailsTheRoundWithNoRequest`, `RebuildAndDryRunRefuseAGcStateBelowHeader4`,
  `PoolPrefixOver668BytesIsRefusedAtCreation` (668 accepted), `CASMigratePool.OnlyThisCommandReachesOpenForMigration`;
  `CASBlobIntentDialect.WritableMountIsRefused`, `.ReadOnlyOpenIsRefused` (so `cas-fsck`, `cas-gc-dryrun` and
  `cas-gc-rebuild` never run), both over `DialectOverrideBackend(Dialect::ETag)` on an `emulated` pool.
  `ReadCeiling3RefusesPoolMetaAndGcState` now runs under `ScopedReadCeiling(3)`.
- [ ] **Step 2: Write the failing writer tests,** suite `CASBlobIntentWriter`, on `EtagFaithfulPublicationBackend`
  and `ContentEtagBackend`, each asserting the journal: `FirstCreationIsHeadAndOnePut`, `CleanReuseIsHeadAndGet`,
  `IntentPresentPublishesFreshOnce` (also with other bytes at the intent key), `NoSecondHeadOnAnyPath`,
  `LandedAmbiguousPublicationIsAdoptedInThreeReads` (`HEAD`, `PUT` unanswered and landed, `HEAD`, `GET`),
  `FailedIntentReadIsNotAbsence` (refused and unanswered `GET`: exception, no adoption),
  `WriterNeverMutatesIntents` (positive control: an intent existed and was read), `NoMetaRequestOnAnyPath`,
  `VerbatimCopyIsSelectedOnlyAfterAnAbsentFirstHead`, `WellFormedUncanonicalTokenPublishesFresh` (`W/"x"`),
  `MalformedTokenFailsTheRead`, `AdoptionReadsReachTheStoreAfterAPrimedAbsence` (read `I(B,t)` absent through the
  same pool, let GC confirm and commit, then write: the backend sees this attempt's `HEAD` and `GET`, and the writer
  publishes).
- [ ] **Step 3: Write the failing GC tests,** suite `CASGcIntents`, driving `Gc::runRegularRound`:
  - States: `CondemnCreatesInTheSameRound`, `ConfirmedCreateCommitsPending`, `EqualOccupantConfirms`,
    `UnansweredCreateCommitsCandidate`, `CandidateRetriesTheCreate`, `PendingDeletesTheBodyOnlyOnALaterCut`,
    `BodyDeleteCommitsStandingAndKeepsTheIntent`, `AbsentBodyAtTheDeleteCommitsStanding`,
    `StandingRemovesTheIntentOnALaterZeroCut`, `SuppressedRoundDeletesNoIntent`,
    `SpareDropsStandingAndLeavesAnOrphanIntent`, `SpareDropsPendingAndKeepsTheIntent`,
    `StandingRowWhoseHeadShowsItsTokenIsCarried` (`ContentEtagBackend` repeats `t` at the key; one ERROR, nothing
    deleted), `TouchedPendingRowWithAnAbsentBodyGoesStandingWithOneHead`, `IdlePoolFoldsForEachRowState` (only a
    `candidate`, then only a `standing` row, no deltas: `graduationDueForTest` is true and the next round runs G3, G8).
  - Cut order: `DecisionHeadsAreSentAfterTheCut`. A `HeadHookBackend` hook on the last intake read publishes a body
    for a key at zero after the final ref-log `LIST` and before intake ends. Assert at least one decision `HEAD`,
    every blob `HEAD` after `cut_complete`, and the row naming the landed body's token: a `HEAD` sent before the
    boundary sees no body and writes no row.
  - Budget and shards: `CreatesOverTheBudgetAreCandidates` (budget 2, five zero transitions: two creates, three
    candidates, no request for those three), `CreateBudgetIsSharedByEveryShardOfARound` (`gc_shards = 4`, budget 3,
    two zero transitions in each shard: three creates, five candidates), `UnboundedCreateBudgetCreatesEveryCandidate`,
    `IntentDeletesShareABatchRequest` (`gc_shards = 4`, on `BatchCapabilityBackend`).
  - Deletes: `DeleteHeadDecidesTheRowInTheFold`, `MismatchCommitsStanding`,
    `UnansweredBodyDeleteFailsTheRoundAndKeepsPending`, `FailedIntentDeleteFailsTheRound`,
    `FiveRequestsPerReclaimedBlob` (condemn 2, delete 2, remove 1), `NoMetaRequestInARound` (positive control: the
    round reclaimed a blob).
  - Re-created key: `RecreatedKeyMovesStandingToPendingInOneCommit` (`HEAD`, intent `PUT`, intent `DELETE`, one
    commit), `TransitionRunsBeforeThePlainIntentDelete`, `PendingWithAnotherTokenAtTheDeleteHeadTransitions`,
    `RespelledTokenAtTheDeleteHeadIsTheSameIncarnation`, `UncanonicalRowTokenIsCarriedAtTheDeleteHead`,
    `NoIntentDeleteOnAnAbsentBodyWithoutACut`, `PreviewNamesEachRowState`.
  - Parametrized over `gc_shards` ∈ {1, 4} with the blob in shard 3 when 4: `FiveRequestsPerReclaimedBlob`,
    `RecreatedKeyMovesStandingToPendingInOneCommit`, `PendingWithAnotherTokenAtTheDeleteHeadTransitions`.
  - Replacing the marker-suite cases that drive rounds, with the same leader change or failure schedule:
    `NewLeaderDeletesTheBodyOfAPendingRow`, `DeposedLeaderBodyDeleteMissesARepublishedBody` (both orders of the
    deposed release), `DeposedLeaderLateCreateIsConfirmedByTheNextLeader` (S8), `SparedThenRecondemnedBlobIsDeleted`,
    `ThrowingRoundDrainsItsDeleteJobsBeforeReturning`, `UndecodableIntentBytesCarryTheRow`.
  - Migrated pool, using Task 8's tool: `InheritedCandidateConfirmsOnTheToolsIntent`,
    `InheritedPendingRowIsDeletedByTheFirstRoundAtZero`, `CleanMarkedBodyIsAdoptedWithNoUpload`,
    `LateOldDeleteMissesAfterARepublish`, `FloorOnePoolMigratesThenServes`.
- [ ] **Step 4: Run red.** Filter `CASBlobIntentWriter*:CASGcIntents*:CASBlobIntentGeneration*:CASBlobIntentDialect*`.
- [ ] **Step 5: Implement.**
- [ ] **Step 6: Re-target the existing tests.** The audit has three parts; the report lists every test edited,
  retired (with its replacement) or deleted.
  - **The grep.** `grep -lE 'blobMetaKey|MetaState::|condemnMeta|writeMetaClean|CASMeta|CASGCMeta|CondemnMarker|graduat|GraduationReadHints|confirm_condemned_marker|redelete|runRoundsUntilAbsent' $WT/src/Disks/tests/*.cpp $WT/src/Disks/tests/*.h`.
    At the base it names `gtest_ca_wiring.cpp` and, after `gtest_cas_`: `gc_ack_floor`, `blob_indegree`,
    `part_write`, `gc_read_ahead`, `gc_meta_writer`, `gc_redelete_concurrency`, `gc_round_defer`, `upload_fanout`,
    `upload_detached`, `gc_frontier_gate`, `gc_round`, `blob_meta`, `protocol_scenarios`, `pluggable_hash`,
    `s3_staging`, `gc_rebuild`, `settings`, `gc_undercount_repro`, `gc_log`, `gc_leak`, `gc_fold`, `blob_meta_format`,
    `layout`, `truncate_reclaim`, `txn_apply_ledger`, `ref_writer`, `ref_gc`, `holey_list_detector`,
    `gc_shard_incarnation`, `gc_bounded_walk`, `fsck`, `retirement_sweep`, `observability`, `list_liar_end_to_end`,
    `gc_resume`, `gc_hold_grammar`; and the headers `cas_test_helpers.h`, `cas_gc_marker_test_support.h`. Every
    matching test declaration is read and classified.
  - **The red gate.** Count-based tests the grep cannot see go red; each is fixed or retired here.
  - **The ceiling and floor sweep.** `grep -rn 'ScopedReadCeiling\|G_BUILD\|mount_reader_floor\|raiseMinReaderGeneration\|raiseReaderFloor\|initial_min_reader_generation\|G_REF_CHAIN\|G_REF_MOVE\|min_reader_generation' $WT/src/Disks/tests`.
    A scope at 3 that means "this build" becomes `G_BUILD` or is dropped; one that means an older reader stays. A
    test whose subject is the mount's floor raise is deleted; one that only needs a pool moves to generation 4.

  Classes:
  - A test whose subject is the marker codec (`T/gtest_cas_blob_meta_format.cpp`) or a marker helper on its own
    stays until Task 11. That is `T/gtest_cas_blob_meta.cpp`, and the `T/gtest_cas_gc_meta_writer.cpp` cases
    that drive no round: `RealCondemnMarkerJobCompletesAcrossOwnerDestruction`,
    `RealConfirmedMetaDeleteCompletesAcrossOwnerDestruction`, the five `ConfirmedMetaDelete*` cases,
    `CondemnMarkerWriteRestampsAnOlderRound`, `CondemnMarkerWriteLeavesAnEqualOrNewerRound`.
  - The `T/gtest_cas_gc_meta_writer.cpp` cases that drive real rounds are deleted here, each replaced by
    the named `CASGcIntents` case or by a state test above: `ThrowingRoundDrainsBeforeReturning`,
    `NewLeaderDeletesAGraduatedEntry`, `GraduationAcceptsANewerRoundMarker`,
    `StaleMetaDeleteSparesSuccessorMarkerBeforeGraduation`, `StaleMetaDeleteSparesSuccessorMarkerAfterGraduation`,
    `GraduationReadsStayWithinTheBudget`, `GraduationRereadsTheMarkerOwnMarker`,
    `GraduationRereadsTheMarkerLostAttemptMarker`, `GraduationAndDeleteRequestCounts`,
    `LatePublisherCleanMissesTheRetriedMarker`, `GraduationCarriesAnUndecodableMarker`,
    `GraduationRefusesAnOlderRoundMarker`, `SparedThenRecondemnedBlobGraduates`,
    `CASGcMarker.RedeleteRemovesMarkerRestampedByGate`.
  - A test that asserts marker requests, the graduation gate or three rounds to reclamation moves to the new ledger:
    the body delete in the round after the condemning one, the intent removal in the next zero round. Fix the
    comment of `runRoundsUntilAbsent`. A test whose subject no longer exists (the graduation read-ahead) is deleted
    and named in the report.
  - `CASPartWriteTxn.AdoptBeforePrecommitFailsClosed` (`T/gtest_cas_part_write.cpp:595-622`) moves into suite
    `CASPartWriteTxnDeathTest`, keeping its build gating.
  - The `LOGICAL_ERROR` splits in `T/gtest_cas_upload_fanout.cpp` and `T/gtest_cas_upload_detached.cpp` stay.
- [ ] **Step 7: Integration and stateless tests the switch breaks,** written here, run by the controller with Task 13.
  Enumerate with `git -C $WT grep -lE 'publication_reason|condemned|graduat|redelete|cas_gc_log|OLD_TAG|with_installed_binary|CASMeta|CASGCMeta|CondemnMarker|\.meta' -- tests/integration tests/queries`.
  At the base it names, among others:
  - `test_cas_ref_move_compat` mounts a 26.6.4 pool at floor 1, now refused: retired; Task 13 covers that pool.
  - `test_gcs_live/test.py:1115-1254` (`publication_reason` asserts) and
    `test_live_cas_condemned_staged_source_retags_by_streaming`: the condemned reason becomes `intent_present`.
  - `test_cas_gc_bulk_delete`, `test_cas_gc_s3`, `test_cas_shared_pool`, `test_cas_replicated_relink`,
    `test_cas_drop_pool_member`, `test_cas_janitor_drain`, `test_cas_early_commit_restart`, `test_cas_ref_snaplog`,
    `test_cas_s3`, `test_cas_gcs`, `tests/queries/0_stateless/05008_cas_gc_snapshot_prune.sh`: graduation counts,
    rounds to reclamation and marker requests move to the new ledger.
- [ ] **Step 8: Run green,** then the gate on `build` and `build_asan`; `py_compile` each edited `test.py`.
- [ ] **Step 9: Commit.** Subject: `Switch CAS writers and GC to deletion intents at generation 4`

### Task 10: Adopt orphan intents {#task-10}

Read G9, §3.4 and Q12.

**Files:**
- Modify: `CA/ContentAddressedSettings.cpp:67` (new `intent_sweep_list_budget_keys`, default 1000, after
  `manifest_sweep_list_budget_keys`; 0 is refused at validation); `CA/Pool/CasPool.h:93` (`PoolConfig` field);
  `CA/ContentAddressedMetadataStorage.h:647` (member), `CA/ContentAddressedMetadataStorage.cpp:71` (extern), `:297`
  (initializer), `:798` (copy), each beside its `manifest_sweep_list_budget_keys` neighbour
- Modify: `G:3609` (one `LIST` page beside the orphan manifest sweep, after suppression is final at `G:3553-3555`
  and before the per-shard reduce; only when destruction is not suppressed; phase row `intent_sweep_list`),
  `G:3714` and `G:3759` (both pure-carry predicates also require an empty nomination bucket), `G:1326` (commit
  stores the next cursor), `G:5028` (rebuild resets `intent_sweep_cursor`)
- Modify: `CA/Gc/CasBlobInDegree.h` (parameter), `N:590-668` (`closeBlob`)
- Modify: `src/Common/ProfileEvents.cpp` (`CASGCIntentAdopted`)
- Test: `T/gtest_cas_gc_intents.cpp`, `T/gtest_cas_settings.cpp`

**Interfaces:**
- Consumes (Task 4): `parseBlobIntentKey`, `blobIntentsPrefix`, `Layout::ParsedBlobIntentKey`. (Task 3):
  `GcState::intent_sweep_cursor`. (Tasks 6, 9): `decideIntentStep`, `peek_head`.
- Produces: `foldDeltasIntoGeneration(..., std::vector<Layout::ParsedBlobIntentKey> intent_nominations)`, after
  `source_retirements`; `PoolConfig::intent_sweep_list_budget_keys`.
- Rules: the page comes from `op.list(blobIntentsPrefix(), cursor, budget, Retry::standard())`; keys that do not
  parse are skipped; the next cursor is the page's continuation, empty at the end of the prefix. Nominations are
  routed to the shard of their `BlobRef` and enter the merge as `source_retirements` do, so a key with no edge is
  still visited. A nomination does not set `cur_touched`, so `closeBlob`'s fresh-condemn branch (`cur_edges == 0 &&
  cur_touched && head_blob`) never fires on it. A nominated key is adopted only if it has no row in the adopted
  state, gets none from this fold, and is at zero; at most one per key per round. Then `peek_head`: same identity →
  `candidate(t)`; absent or another identity → `standing(t)`; uncanonical → no row. Adoption deletes nothing in its
  round, and advances only in rounds that fold: a round `shouldDeferRound` defers lists nothing.
- [ ] **Step 1: Write the failing tests,** suite `CASGcIntents`: `OrphanOverTheCurrentBodyBecomesACandidate`,
  `OrphanOverAnAbsentBodyBecomesStanding`, `OrphanOverAnotherTokenBecomesStanding`, `AdoptionDeletesNothingInItsRound`,
  `SweepSkipsAKeyWithARow`, `SweepSkipsAKeyTheFoldGivesARow` (also a fold `candidate` over the create budget),
  `SweepSkipsAKeyAtPositiveInDegree`, `OneOrphanPerKeyPerRound`, `SweepCursorAdvancesOnePagePerRound` (budget 2,
  five intents: three rounds to wrap), `RebuildResetsTheIntentCursor`, `SuppressedRoundListsNothing`,
  `DeferredRoundListsNothing`, `UncanonicalTokenWritesNoRow`, `UncanonicalRowTokenIsMovedByACanonicalBody`,
  `NominationDoesNotTriggerAFreshCondemn`, `OrphanAdoptionWithFourShards` (the only nomination in a shard with no
  delta and no row: adopted), `RebuildOnAMigratedPoolKeepsHeader4AndAdoptsEveryIntent`,
  `LeaderDiesAfterTheCreateThenAdoptionConfirms` (§3.4 row 1). `CASContentAddressedSettings`:
  `IntentSweepPageReachesThePool` (non-default value through the disk), `ZeroIntentSweepPageIsRefused`.
- [ ] **Step 2: Run red.** Filter `CASGcIntents*:CASContentAddressedSettings*`.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run green,** then the gate on `build` and `build_asan`.
- [ ] **Step 5: Commit.** Subject: `Adopt orphan CAS blob intents with a paged sweep`

### Task 11: Remove the marker code; protocol docs {#task-11}

Read §9.2. Deletes what Task 9 left unreferenced. The `.meta` codec (`CA/Formats/CasBlobMetaFormat.*`),
`Layout::blobMetaKey` and `deleteMetaExact` (`CA/Pool/CasBlobMeta.cpp:42`) stay for Tasks 8 and 12 and `cas-inspect`.

**Files:**
- Delete: `CA/Gc/CasGcMetaWriter.h`, `CA/Gc/CasGcMetaWriter.cpp`, `T/gtest_cas_gc_meta_writer.cpp`, `T/gtest_cas_blob_meta.cpp`
- Modify: `CA/Pool/CasBlobMeta.{h,cpp}` (keep `deleteMetaExact` without its `CASMetaDelete` increment, `:45`, and the
  decode helper the tool needs); `CA/Gc/CasGc.{h,cpp}` (`meta_writer`, its pool, `G:781`, phase `meta_pool_wait`
  `G:1268-1284`)
- Modify: `CA/ContentAddressedSettings.cpp:86` (`gc_meta_pool_size` stays declared, described as ignored);
  `CA/Pool/CasPool.h:176` (field deleted); `CA/ContentAddressedMetadataStorage.h:670` (member deleted),
  `CA/ContentAddressedMetadataStorage.cpp:89`, `:315`, `:808` (extern, initializer, copy deleted)
- Modify: `src/Common/ProfileEvents.cpp` (delete the marker counters §9.2 lists as removed, `:962-971`)
- Modify: `T/cas_test_helpers.h`: `condemnMeta` (`:1144`), `loadMetaForTest` (`:1159`) and `writeMetaClean` (`:1133`)
  rewritten on raw `CasOperation::read` and `replace` (or `create`) with `decodeBlobMeta` and `encodeBlobMeta`; they
  call no helper this task deletes. `T/cas_gc_marker_test_support.h` deleted; its includers updated
- Modify docs: `D/architecture/blob-protocol.md`, `D/architecture/garbage-collection.md`,
  `D/architecture/storage-layout.md`, `D/architecture/correctness.md`, `D/bucket-requirements.md`,
  `D/configuration.md`, `D/operations/monitoring.md`, `docs/en/antalya/metrics_and_events.md`,
  `docs/en/operations/system-tables/cas_gc_log.md`, `docs/en/operations/system-tables/cas_log.md`

**Interfaces:** Consumes Task 9 and 10 names. Produces nothing new.

- Docs content: the writer's two reads and the reuse cost after a standing intent; GC's three row states and the
  round order; intents in the storage layout. Bucket requirements as §9.2 lists them (`GET` answering 404 needs
  `s3:ListBucket`). Configuration: no mixed versions; `gc_meta_pool_size` and `cas_gc_round_graduation_budget`
  ignored; `cas_gc_round_intent_create_budget` (default 0, unbounded); `cas_intent_sweep_list_budget_keys` (0
  refused). The counters of §9.2; phase rows `intent_sweep_list` added, `meta_pool_wait` gone; event reasons
  `intent_present`, `token_rejected`; `cas_gc_log` columns as Task 9 defines them.
- [ ] **Step 1: Write the failing tests.** `CASObservability.NoMarkerCounterExists` (iterate `ProfileEvents::getName(e)`
  for every `e < ProfileEvents::end()`; none of the nine removed names appears; positive control: `CASBlobIntentGet`
  appears); `CASGcIntents.RoundHasNoMetaPoolWaitPhase` (the round's phase rows, through the phase sink; positive
  control: `fold_ref_intake` appears).
- [ ] **Step 2: Run red.**
- [ ] **Step 3: Delete, rewrite the fixtures, move.** Gate for completeness, empty except the kept codec,
  `blobMetaKey`, `deleteMetaExact`, `CasInspect.cpp`, `CasMigratePool.cpp`, `CasFsck.cpp`, the rewritten fixtures
  and the tests of the codec:
  `git -C $WT grep -nE 'CasGcMetaWriter|loadMeta\b|casMeta\b|reconcileMetaClean|validateMetaSize|scheduleCondemnMarkerWrite|CASMeta|CASGCMeta|CondemnMarker|meta_pool' -- src programs`.
- [ ] **Step 4: Docs.** Gate: `git -C $WT grep -nE '\.meta|CASMeta|Clean marker|Condemned marker|freshness marker|meta_pool' -- docs/en`
  lists only the migration page (Task 8) and legacy-marker text (Task 12).
- [ ] **Step 5: Run green,** then the gate on `build` and `build_asan`.
- [ ] **Step 6: Commit.** Subject: `Remove the CAS blob marker code and document the intent protocol`

### Task 12: `cas-fsck` for intents and legacy markers {#task-12}

Read §3.3. **Files:**
- Modify: `CA/Tools/CasFsck.h:22-120` (classes, report), `:200-210` (`kFsckHardFindings`), `:268` (`runFsck`);
  `CA/Tools/CasFsck.cpp:445-449` (operation), `:890-914` (listing), `:1125-1140` (`sameIncarnation` for the
  `PendingGc` class), `:1204-1222` (pairing deleted)
- Modify: `programs/disks/CommandFsck.cpp` (option `--fix legacy-markers`); `CA/Pool/CasPool.cpp:445-446` (comment:
  a read-only open never writes, and deletes only `.meta` under an explicit fix)
- Modify: `src/Interpreters/InterpreterSystemQuery.cpp:2637`, `:2688` (six columns appended)
- Modify: `tests/queries/0_stateless/05020_cas_fsck.sh` and its reference if the summary line changes (predicted)
- Modify docs: `D/operations/troubleshooting.md`, `D/operations/debugging.md` (classes, the fix option)
- Test: `T/gtest_cas_fsck.cpp`

**Interfaces:**
- Consumes (Tasks 1, 4, 8): `parseBlobIntentKey`, `isBlobIntentRecord`, `canonicalIdentity`, `sameIncarnation`,
  `removeLegacyMarkers`.
- Produces:

  ```cpp
  enum class FsckFix : uint8_t { None, LegacyMarkers };
  /// new FsckReport counters
  uint64_t intent_current = 0, intent_replaced = 0, intent_body_gone = 0, intent_malformed = 0,
           intent_unread = 0, legacy_marker = 0, legacy_markers_deleted = 0;
  FsckReport runFsck(Pool & store, bool detail, FsckProgress on_progress = {},
                     std::optional<std::chrono::steady_clock::time_point> deadline = {},
                     bool partial_on_deadline = false, const String & namespace_prefix = {},
                     FsckFix fix = FsckFix::None);
  ```

- Rules: F1 to F5 as written. An intent is an accounted object in every class total. F2 reads the intent with an
  exact `GET`, then `HEAD B`; a refused or unanswered read, or a token that does not canonicalise, is
  `intent_unread`. F3 runs `removeLegacyMarkers` on the read-only open's own operation (`Q:656`); a refused or
  unanswered delete stops the run with the count deleted so far. `body_without_meta` and `meta_without_body` are
  deleted from the report and the summary line. `cas-fsck` never deletes an intent.
  - `clean()`: `intent_malformed` joins `kFsckHardFindings` with its three surfaces (summary line, nonzero exit,
    SQL column); nothing this build writes produces it. The other new classes are advisory.
  - `SYSTEM CAS FSCK` gains six columns: the five intent classes and `legacy_marker`. `legacy_markers_deleted` is
    only in the `clickhouse-disks` summary: `SYSTEM CAS FSCK` never fixes.
- [ ] **Step 1: Write the failing tests,** suite `CASFsck`: `IntentClassesCurrentReplacedBodyGoneMalformed`,
  `MalformedKeyIsIntentMalformed`, `MalformedIntentMakesTheReportUnclean`, `BodyGoneIntentKeepsTheReportClean`,
  `FailedReadIsUnread`, `UncanonicalHeadTokenIsUnread`, `NoIntentIsEverDeleted` (the journal: no `DELETE` on
  `blob-intents/`; positive control: intents were classified), `RefusedDeleteStopsTheRun`,
  `LegacyMarkersAreDebrisAndTheFixDeletesThem`, `FixRunWritesNothingAndMountsNothing`,
  `IntentsAreAccountedInTheTotals`. Failure cases assert that no key outside `.meta` was deleted.
- [ ] **Step 2: Run red.** Filter `CASFsck*`.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run green,** then the gate on `build` and `build_asan`.
- [ ] **Step 5: Commit.** Subject: `Teach cas-fsck about blob intents and legacy markers`

### Task 13: Stateless and integration tests {#task-13}

Read §10.2, rows "Stateless" and "Integration". Written here, run by the controller, together with the tests
Task 9 edited; references are predicted.

**Files:**
- Create (with `add-test`): `tests/queries/0_stateless/<n>_cas_blob_intent_requests.sh` and its reference
- Create: `tests/integration/test_cas_blob_intent_migration/` (`__init__.py`, `test.py`, `configs/`), modelled on
  `tests/integration/test_cas_ref_move_compat/test.py:13-25` at the base (the 26.6.4 image with
  `with_installed_binary` and a current instance on one bucket). Modify `tests/integration/test_cas_gc_s3/test.py`

- [ ] **Step 1: Stateless test,** tags `no-fasttest` only if it uses a failpoint. Counters per blob, from
  `system.query_log`: new content: 1 `CASBlobHeadMiss`, 1 `CASBlobPut`, 0 `CASBlobIntent*`. A duplicate: 1
  `CASBlobHead`, 1 `CASBlobIntentGet`, 0 `CASBlobPut`. After `DROP` and one `SYSTEM CAS GC RUN`, the duplicate: 1
  `CASBlobHead`, 1 `CASBlobIntentGet` (the intent exists), 1 `CASBlobPut`. After `DROP` and two runs: 1
  `CASBlobHeadMiss` (the body is gone), 1 `CASBlobPut`, 0 `CASBlobIntentGet`.
- [ ] **Step 2: Integration, migration,** on RustFS and the GCS emulator; the 26.6.4 image creates the pool at floor
  1. `test_old_image_builds_condemned_rows` (GC runs until candidate and pending rows exist),
  `test_tool_refuses_a_live_older_server` (names it), `test_tool_completes_with_every_server_stopped` (the output
  says the option was forced; floor 4), `test_new_image_starts_and_old_image_is_refused` (§7.5 text),
  `test_data_reads_back`, `test_duplicate_insert_reuses_with_two_reads`, `test_fsck_reports_no_dangling_ref_and_no_marker`,
  `test_head_token_form_per_store` (multipart upload and multipart copy above 32 MiB included).
- [ ] **Step 3: `test_cas_gc_s3`:** `test_gc_requests_per_reclaimed_blob` asserts that the rounds reclaimed at least
  one blob, then at most 5 requests per reclaimed blob from `system.blob_storage_log`.
- [ ] **Step 4: Style check** of the shell test (`ci/jobs/check_style.py` functions); `py_compile` the new `test.py`.
- [ ] **Step 5: Commit.** Subject: `Add CAS blob intent request and migration tests`. The report lists every predicted value.

### Task 14: Measurements, soak, defaults {#task-14}

Read §10.3 and the soak row of §10.2. The owner fixes the defaults from the report. **Wide-insert benchmark:** a
script in `M/utils/cas-yardstick/`, outside the product tree, runs the workload against a given binary and
configuration and writes one card: BEFORE on the base `0aa837e942e`, AFTER on the branch, the script unchanged.

- Workload: one `INSERT ... SELECT` into a 109-column table that produces about 340 `Wide` parts and 55,000 to 62,000
  blobs, on local RustFS 1.0.6.
- Scenarios: the first insert (birth); an identical second insert (reuse); `DROP`, then GC rounds while a small
  insert runs, with `cas_gc_round_intent_create_budget` at 0 and at 5000. The base build rejects that unknown key,
  so its card runs the `DROP` scenario once with default budgets.
- Metrics: wall time; peak query memory; requests by kind from `system.blob_storage_log`; upload thread-seconds;
  round durations, including the delete round with G4's `HEAD`s; insert latency during the rounds.
- Acceptance: request counts match the ledger of §1 exactly (birth: per blob 1 `HEAD`, 1 `PUT`, no marker `PUT`;
  reuse: 2 reads per blob, no `PUT`). Times are reported, not gated.

Further acceptance:

| Measure | Pass |
|---|---|
| GC per reclaimed blob | at most 5 requests, 2 rounds from "no reference" to the body delete |
| The stand's mass drop (about 55,000 blobs) with the create budget at 0 and at 5000, S3 and GCS: round time, creates per round, rounds to condemn, insert latency during the rounds | reported for both settings; the default stays 0 unless the unbounded round outlasts the GC lease or slows inserts, then the owner picks the value |
| The delete round of that drop: time of G4's `HEAD`s in the fold | reported |
| Publication hash on a large body | CPU per GiB reported |
| Standing intents, equal parts dropped and re-inserted within one round | count of extra uploads reported |
| The tool on a pool like the stand's | requests per step against the migration ledger of §1; wall time; then a duplicated insert: two reads, no upload |
| Soak, 2 hours, with process kills | fsck: zero dangling refs; after writers stop and GC runs until the sweep passed every intent and two more rounds, no `intent_body_gone`; `CASBlobPublishDigestMismatch` is 0 |

- [ ] **Step 1:** Write the benchmark script; record the BEFORE cards (yardstick and wide insert) on the base binary.
- [ ] **Step 2:** Build the branch's `clickhouse` (`ninja clickhouse > build_t14.log 2>&1`); record the AFTER cards
  with the same script, and the round-time measurements on S3 and GCS.
- [ ] **Step 3:** Run the tool on a copy of the stand's pool, then the duplicate insert.
- [ ] **Step 4:** Run the soak.
- [ ] **Step 5:** Record the decisions of §9.1 in `M/docs/superpowers/cas/AGENTS.md`: invariant 5 (a durable write
  step removed, a read added, as a protocol change with a model) and the amendment of decision-4 (generation 4,
  offline tool, no downgrade). Apply [Backlog effects](#backlog-effects) to the backlog tasks.
- [ ] **Step 6:** Commit the script, the cards, `AGENTS.md` and the backlog files in `M` by exact path. Subject:
  `Add yardstick and soak cards for CAS blob intents`. A default change is a separate commit in `WT` after the
  owner decides.

## Spec readings {#spec-readings}

Places where the spec leaves a choice; the plan takes the reading on the right.

| Spec text | Reading |
|---|---|
| Run bit `standing` "beside `pending` and `confirmed`" | a standing row keeps `delete_pending = true`, so the fold seal summary counts it and no seal field changes |
| G3 "the create of G2 again" | the create of `I(B,t)` for the row's token, without a new `HEAD` |
| A candidate row whose own token does not canonicalise | carried, counted as `CASGCIntentTokenRejected`; G7 moves it when a canonical body appears |
| G7 in a round that suppresses destruction | runs its create and moves the row, but sends no intent delete; `I(B,t)` stays for G9. So `SuppressedRoundDeletesNoIntent` holds for every rule |
| "Any exception" does not confirm a create | a `DB::Exception` other than `LOGICAL_ERROR` maps to `Unconfirmed`; a `LOGICAL_ERROR` propagates |
| G9 anomaly "counted", for an adopted `standing(t)` | one rule for every `standing(t)` whose `HEAD` of the round shows `t`: carried, one ERROR, a `RoundReport` field, nothing deleted; §9.2 names no counter |
| A touched row's supersede `HEAD` and G4's `HEAD` | one `HEAD` per row per round: the supersede answer, whatever it is, is G4's |
| Q10 "pool open deferred for this command only" | a process flag set by `DisksApp` for `cas-migrate-pool` before the disk starts |
| M2 "the tool's poll step" | `poll_step_ms = 1000` |
| M6 "a floor below 3 is first raised" | `Pool::raiseReaderFloor` made public and called unchanged, as the first write of M6 |
| The create budget "per round" | per round, summed over GC shards, as the code applies `GcRoundWorkBudget` today. The key is new, `gc_round_intent_create_budget`; `gc_round_graduation_budget` is ignored and documented as ignored, like `gc_meta_pool_size` |
| G9's page size "a pool setting" | `intent_sweep_list_budget_keys = 0` is refused: without G9 orphan intents are never reclaimed |
| G9 "each round that does not suppress destruction" | each round that folds; a deferred round lists nothing, so adoption advances on folds forced by changes or `gc_fold_max_defer_rounds` |
| Which `cas-fsck` classes make a report unclean (F1 to F5 say nothing) | `intent_malformed` only |
| `RoundReport::graduated` and `redeleted`, whose steps are gone | rows committed `pending` this round; body deletes answered this round. `system.cas_gc_log` keeps both columns |

## Backlog effects {#backlog-effects}

| Backlog task | Effect | When |
|---|---|---|
| CAS-384, CAS-330, CAS-334, CAS-345, CAS-29.3, CAS-439, CAS-326 | moot: they exist for the marker or its residuals | after Task 11 |
| CAS-29.4, CAS-336, CAS-284.5 | re-targeted from marker requests to intent requests | after Task 9 |
| CAS-10, CAS-11, CAS-289.6 | intents replace `.meta` in the fsck and byte classes; the pairing counters go | after Task 12 |
| CAS-33.2, CAS-33.3 | unchanged | — |
| new | the tool, the generation gate, the intent sweep: one task each, closed by Tasks 8, 9 and 10 | before Task 1 |
