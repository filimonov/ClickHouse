# CAS blob deletion intents: implementation plan {#cas-blob-deletion-intents-plan}

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
> An implementer receives three texts: [Global Constraints](#global-constraints), its own task, and the spec.

**Goal:** A CAS writer publishes a blob body with one `PUT` and no marker, reuses a body after `HEAD` and an intent
`GET`, and GC alone warns writers with immutable deletion intents; the build runs only this protocol, at pool
generation 4, and `clickhouse-disks cas-migrate-pool` moves an older pool once, offline.

**Architecture:** Twelve commits. Tasks 1 to 4 add inert parts: the canonical token identity, the generation-4
control fields, the intent object with its create confirmation, the payload hash check. Task 5 makes every new pool
generation 4 and refuses every open of an older one; the marker protocol still runs, on generation-4 pools only.
Task 6 switches the writer and GC to intents in one commit, because a marker writer beside an intent GC, or the
reverse, adopts bodies GC is deleting. Task 7 adds orphan adoption. Task 8 deletes the marker code that Task 6 left
unreferenced. Tasks 9 and 10 bring `cas-fsck` and the migration tool. Task 11 adds the stateless and integration
tests, Task 12 the measurements that fix the defaults.

**Tech Stack:** C++ (`CA/` metadata storage, `programs/disks`), GoogleTest (`unit_tests_dbms`), stateless tests
(shell), integration tests (pytest; RustFS and the GCS emulator), Markdown docs, Python (`utils/cas-yardstick`,
`utils/ca-soak` in `M`).

**Spec:** `docs/superpowers/specs/2026-11-17-cas-blob-deletion-intents-design.md` at commit `9296f2e2f0d` (branch
`cas-gc-rebuild`). It is binding; "§N" below is a section of it. A conflict between this plan and the spec resolves
to the spec, except the readings listed in [Spec readings](#spec-readings).

**Precondition, not a task:** the model work of §10.1 "Required before implementation" is committed under
`M/docs/superpowers/models/` with its results file: the consolidated model as the model of record in place of
`CaBlobIntentProtocol.tla`, every run that paragraph lists, and the migration model re-run with the M5 rule for a
pending row's own token and case (e) on. No task starts before the owner confirms that commit.

## Global Constraints {#global-constraints}

### Paths {#paths}

| Name | Value |
|---|---|
| `WT` | `/home/mfilimonov/workspace/ClickHouse/cas-pr1`; new branch `work/antalya-26.8/cas-blob-intents` from `ac144e848aa` |
| `M` | `/home/mfilimonov/workspace/ClickHouse/master`, branch `cas-gc-rebuild` (spec, plan, models, `utils/cas-yardstick`, `utils/ca-soak`) |
| `CA/` | `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/` |
| `T/` | `src/Disks/tests/` |
| `D/` | `docs/en/antalya/cas/` |
| `W:` `G:` `N:` | `CA/Pool/CasPartWriteTxn.cpp`, `CA/Gc/CasGc.cpp`, `CA/Gc/CasBlobInDegree.cpp` |
| `B:` `Q:` `SR:` | `CA/Backend/CasObjectStorageBackend.cpp`, `CA/Backend/CasRequests.cpp`, `CA/Pool/CasServerRoot.h` |

Every `path:line` was read in `WT` at `ac144e848aa`. Lines move as tasks land: find the named symbol, use the line
as a hint.

### Rules {#rules}

- **TDD.** Each task starts with failing tests, run red on an assertion, not a compile error: add a new signature
  with a stub first. Paste the red output into the task report. A test marked "pin" passes before and after.
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
- **Shared code** outside `CA/`, `T/`, `programs/disks/`, `D/` is changed only where a task lists it:
  `src/Common/ProfileEvents.cpp`, `src/Interpreters/InterpreterSystemQuery.cpp` (Task 9), the system-table and
  metrics pages under `docs/en/` (Tasks 8, 9). Anything else is a stop.
- **Docs** under `docs/`: every heading has an explicit `{#anchor}`; plain words in `D/` (no invariant tags such
  as `I4`, `W3`, `G7`); functions as `f`, not `f()`; SQL names and classes in backticks; present behaviour, no history.
- **Stateless and integration tests** are written in their task and run by the controller. New stateless tests
  come from `tests/queries/0_stateless/add-test <name>[.sh]`. No `no-*` tag unless the test cannot run otherwise;
  a test with a failpoint carries `no-fasttest`. A reference value that was not run is listed as "predicted".

### Build and test {#build-and-test}

Build directories in `WT`: `build` (release) and `build_asan` (ASan + UBSan). One build at a time, in the
foreground, `ninja` with no `-j`, output to a log in the build directory. A task that adds a `.cpp` runs
`cmake . > cmake_<tag>.log 2>&1` in the build directory first.

```bash
cd $WT/build && ninja unit_tests_dbms > build_<tag>.log 2>&1; \
  echo "NINJA_EXIT=$? HEAD=$(git -C $WT rev-parse --short HEAD)" >> build_<tag>.log
mkdir -p $WT/build/gtest_tmp_<tag> && cd $WT/build && \
  TMPDIR=$WT/build/gtest_tmp_<tag> ./src/unit_tests_dbms --gtest_filter='<filter>' > test_<tag>.log 2>&1; \
  echo "GTEST_EXIT=$?" >> test_<tag>.log; rm -rf $WT/build/gtest_tmp_<tag>
grep -a 'NINJA_EXIT' $WT/build/build_<tag>.log
grep -a '\[  PASSED  \]\|\[  FAILED  \]\|GTEST_EXIT' $WT/build/test_<tag>.log | tail -20
```

- A test log counts only if the build log of the same tag ends with `NINJA_EXIT=0`. A log without
  `[  PASSED  ]` means the binary aborted; the last `[ RUN      ]` names the test.
- **Gate** at the end of every task: the filter is EXACTLY `CAS*`, on `build`, on the committed tree. The same
  gate on `build_asan` after Tasks 3, 5, 6, 7, 8 and 10. A red gate is fixed in a further commit; a red test the
  task does not explain is a stop: report the test, the assertion and the log.

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
  (`T/cas_test_helpers.h:126`). `ScopedReadCeiling` (`:370`) means "this build is an older reader".
- Backends: `InMemoryBackend` (dialect `emulated`), `CountingBackend` (`T/cas_test_helpers.h:1862`),
  `BatchCapabilityBackend` (`:2120`), `HeadHookBackend` and `ContentEtagBackend` (`T/cas_gc_marker_test_support.h:250`,
  `:301`; moved by Task 8), `EtagFaithfulPublicationBackend` (`T/gtest_cas_s3_staging.cpp:227`).
- Fixtures: `writeBlobBody` (`T/cas_test_helpers.h:1098`), `displaceBlobToken` (`:1039`), `writeMetaClean`
  (`:1133`), `condemnMeta` (`:1144`), `seedPoolMetaForRestart` (`:1082`), `runRegularRoundReclaiming` (`:918`),
  `runRoundsUntilAbsent` (`:929`), `currentRetiredSet` (`:949`). Task 5 adds `seedGeneration3PoolForTest` and
  `DialectOverrideBackend`.
- A **request journal** assertion reads the ordered list of `(verb, key)` a `CountingBackend` saw, so a test can
  state "no request after X" and "Y before Z".

## Review Focus {#review-focus}

The inputs most likely to break this for an operator, which the spec implies and no spec test exercises directly.
Each is pinned to a test in the task that owns the code.

1. **A read-ahead `HEAD` issued before the cut.** The fold's `HEAD` hints (`G:2163-2172`) feed G2; a hint sent
   during intake would decide on a stale body. Task 6, `CASGcIntents.DecisionHeadsAreSentAfterTheCut` asserts, in
   the journal, that every blob `HEAD` of the round follows the last ref-log `LIST` page of the intake.
2. **One incarnation spelled two ways.** A row captured with a quoted ETag, a `HEAD` answered bare. Task 6,
   `CASGcIntents.RespelledTokenAtTheDeleteHeadIsTheSameIncarnation`: the body is deleted and no transition happens.
3. **Several GC shards.** Nominations and intent deletes routed by shard. Task 6,
   `CASGcIntents.IntentDeletesShareABatchRequest` and Task 7,
   `CASGcIntents.OrphanAdoptionWithFourShards`, both at `gc_shards = 4`.
4. **A marker listing longer than one page.** M4 reads every `.meta` across `LIST` pages where body and `.meta`
   keys interleave. Task 10, `CASMigratePool.MarkersAcrossListPagesAreAllRead` with a page of 3 keys.
5. **A migrated pool rebuilt.** `cas-gc-rebuild` on a migrated pool writes header 4 and resets the intent cursor.
   Task 7, `CASGcIntents.RebuildOnAMigratedPoolKeepsHeader4AndAdoptsEveryIntent`.

## Task map {#task-map}

| # | Task | Consistency after the commit |
|---|---|---|
| 1 | Token identity and the probe check | inert; the probe refuses a store whose token cannot be an identity |
| 2 | Generation-4 control formats | inert fields; `gc/state` written at header 4 by every writer |
| 3 | Intent object, key, request class, create confirmation | inert: nothing creates an intent |
| 4 | Verified streaming publication | every streamed body is hashed; protocol unchanged |
| 5 | Generation gate and the pool dialect | pools are generation 4; markers still run, writer and GC alike |
| 6 | Switch the writer and GC to intents | intents run, writer and GC alike; marker code compiled, unreferenced |
| 7 | Adopt orphan intents | adds reclamation of orphan intents |
| 8 | Remove the marker code; protocol docs | marker code gone except the codec the tool and `cas-inspect` read |
| 9 | `cas-fsck` for intents and legacy markers | |
| 10 | `clickhouse-disks cas-migrate-pool` | the only path from a generation-3 pool |
| 11 | Stateless and integration tests | |
| 12 | Measurements, soak, defaults | |

| Producer → consumer | Interface |
|---|---|
| 1 → 3, 5, 6, 7, 9, 10 | `CanonicalIdentity`, `canonicalIdentity`, `Etag::canonical`, `PersistedEtag::canonical`, `sameIncarnation`, `dialectWord`, `dialectFromWord`, `opaqueTagFromHex` |
| 2 → 5, 6, 7, 10 | `G_BLOB_INTENTS`, `PoolMeta::token_dialect`, `GcState::header_generation`, `GcState::intent_sweep_cursor`, `RetiredEntry::standing`, `CondemnedRow::standing`, `SourceEdgeRecord::standing` |
| 3 → 6, 7, 9, 10 | `Layout::blobIntentKey`, `writeOnceBlobIntentKey`, `parseBlobIntentKey`, `blobIntentsPrefix`; `blobIntentRecord`, `isBlobIntentRecord`; `createBlobIntent`, `IntentCreateResult` |
| 5 → 6 to 11 | `requireGeneration4Pool`, `seedGeneration3PoolForTest`, `DialectOverrideBackend` |
| 6 → 7, 8, 9 | `IntentCreateFn` parameter of `foldDeltasIntoGeneration`, `RetiredMergeResult::body_deletes`, `intent_deletes` |
| 9 → 10 | `FsckFix`, `removeLegacyMarkers` |

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
  strip. No case folding. `PersistedEtag::matches` stays: it compares control-object tokens elsewhere
  (`G:1550`, `CA/Tools/CasFsck.cpp:1134`).
- Probe: after its scratch writes, `runCapabilityProbe` takes the token of one scratch `HEAD`; no identity →
  `NOT_IMPLEMENTED`, `the store returns a token that cannot serve as a blob identity: <token>`. It runs where the
  probe runs today (writable mount and pool creation, `CA/Pool/CasPool.cpp:546`; `skip_access_check` skips it) and
  in Task 10.

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
  `_pool_meta` written).
- [ ] **Step 2: Run red.** Stubs: `canonicalIdentity` returns nothing. Filter `CASTokenIdentity*:CASProbe*`.
- [ ] **Step 3: Implement** the functions and the probe check.
- [ ] **Step 4: Run green,** then the gate.
- [ ] **Step 5: Commit.** Subject: `Add a canonical token identity for CAS blob incarnations`

### Task 2: Generation-4 control formats {#task-2}

Read §2, §2.2, §3.5. Fields are written and read; no decision uses them.

**Files:**
- Modify: `CA/Formats/CasFormat.h:16` (`G_BUILD = 4`, new `G_BLOB_INTENTS = 4`)
- Modify: `CA/Formats/CasPoolMetaFormat.h:25-33` (`PoolMeta`), `CA/Formats/CasPoolMetaFormat.cpp` (encode; decode
  after the ceiling check, `:155-158`)
- Modify: `CA/Formats/CasGcStateFormat.h:31-40`, `CA/Formats/CasGcStateFormat.cpp:38-56` (`:42` header), decode `:58-`
- Modify: `CA/Formats/CasRecordStreamFormat.h:84-94`, `CA/Formats/CasRecordStreamFormat.cpp:198-205` and its reader
- Modify: `CA/Gc/CasBlobInDegree.h:27-43` (`RetiredEntry`), `:77-92` (`CondemnedRow`); `N:262-290` (packed row),
  `:320-327`, `:490-498`, `:655-665`
- Modify: `CA/Tools/CasInspect.cpp:413-418` (render `standing`)
- Test: `T/gtest_cas_pool_meta.cpp`, `T/gtest_cas_gc_state_format.cpp`, `T/gtest_cas_record_stream_format.cpp`,
  `T/gtest_cas_blob_indegree.cpp`, `T/gtest_cas_format_battery.cpp`, and every file Step 4 edits

**Interfaces:**
- Consumes: `writeHeaderLine(out, id, compatibility_version)` (`CA/Formats/CasTextFormat.h:153`), `expectHeaderLine`
  (`:302`), `rmwPoolMeta` (`CA/Pool/CasPoolMeta.cpp:69-86`).
- Produces:

  ```cpp
  constexpr uint32_t G_BUILD = 4;
  constexpr uint32_t G_BLOB_INTENTS = 4;
  struct PoolMeta { /* existing */ std::optional<Dialect> token_dialect; };   /// present iff floor >= 4
  struct GcState { /* existing */ String intent_sweep_cursor; uint32_t header_generation = G_BLOB_INTENTS; };
  struct RetiredEntry { /* existing */ bool standing = false; };      /// standing implies delete_pending
  struct CondemnedRow { /* existing */ bool standing = false; };      /// also in operator==
  struct SourceEdgeRecord { /* existing */ bool standing = false; };
  ```

- Rules:
  - `_pool_meta`: wire key `token_dialect` after `algos_used`, written iff set, value `dialectWord`. Decode, after
    the ceiling check: present below floor 4 → `CORRUPTED_DATA`; missing at floor 4 → `CORRUPTED_DATA`; unknown
    word → `UNKNOWN_FORMAT_VERSION`. Every read-modify-write keeps it because it re-encodes the decoded struct.
  - `gc/state`: `encodeGcState` writes `writeHeaderLine(out, FormatId::GcState, G_BLOB_INTENTS)`, so every writer
    of this build (bootstrap `G:5204-5211`, renewal, commit, rebuild) writes header 4. `decodeGcState` sets
    `header_generation` from the header. `intent_sweep_cursor` is written after `manifest_sweep_cursor`; absent
    decodes as empty.
  - Run rows: key `standing` on a `Condemned` record, written only when true, so candidate and pending rows keep
    today's bytes. `standing` without `pending`, or on a non-condemned record → `CORRUPTED_DATA`. Packed row: flag
    `4` is `standing`; `flags > 7`, or `4` without `1` → `CORRUPTED_DATA` (`N:287`). Because a standing row is
    also `delete_pending`, `CondemnedSummary::pending_total` counts it and `graduationDue` folds the shard
    (`G:4373-4403`); the fold seal format does not change.
- [ ] **Step 1: Write the failing tests.** `CASPoolMeta`: `TokenDialectRoundTrips`, `TokenDialectMissingAtFloor4IsCorrupted`,
  `TokenDialectBelowFloor4IsCorrupted`, `UnknownTokenDialectWordIsUnknownFormat`, `TokenDialectSurvivesAdmitOrValidate`
  (`CA/Pool/CasPoolMeta.cpp:98-123`), `TokenDialectSurvivesEveryReadModifyWrite` (floor-4 meta seeded with
  `encodePoolMeta`). `CASGCStateFormat`: `EncoderWritesHeader4`, `DecoderReportsTheHeaderGeneration` (headers 1, 3,
  4), `IntentSweepCursorRoundTrips`, `AbsentIntentSweepCursorDecodesEmpty`.
  `CASBlobIntentGeneration.ReadCeiling3RefusesPoolMetaAndGcState` (under `ScopedReadCeiling(3)`: the texts
  `CAS pool meta: pool requires reader generation 4 but this build supports at most 3` and `CAS cas_gc_state:
  object requires reader generation 4 but this build supports at most 3`). `CASRecordStream`: `StandingRowRoundTrips`, `AbsentStandingDecodesFalse`,
  `StandingWithoutPendingIsCorrupted`, `StandingOnANonCondemnedRecordIsCorrupted`, pin
  `CandidateAndPendingBytesAreUnchanged` (golden bytes captured before Step 3). `CASBlobInDegree`:
  `PackedStandingFlagRoundTrips`, `PackedFlagsAboveSevenAreCorrupted`, `PackedStandingWithoutPendingIsCorrupted`,
  `RunViewAndRetiredEntryCarryStanding`. Format battery: `gc/state` row at header 4.
- [ ] **Step 2: Run red.** Fields added, codecs unchanged. Filter `CASPoolMeta*:CASGCStateFormat*:CASRecordStream*:CASBlobInDegree*:CASFormat*`.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run green, then sweep the ceiling scopes.** With `G_BUILD = 4`, `ScopedReadCeiling` at 3 now lowers
  the ceiling. Enumerate with `grep -rn 'ScopedReadCeiling\|G_BUILD\|mount_reader_floor' $WT/src/Disks/tests`; a scope
  at 3 that means "this build" becomes `G_BUILD` or is dropped; one that means an older reader stays. Run the gate;
  list every edited test in the report. A red test outside this sweep is a stop.
- [ ] **Step 5: Commit.** Subject: `Add generation 4 control fields for CAS blob intents`

### Task 3: Intent object, key, request class, create confirmation {#task-3}

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
  struct ParsedBlobIntentKey { BlobRef ref; CanonicalIdentity identity; };
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
  `WriteOnceKeysDeleteInOneRequest` (`BatchCapabilityBackend`, five keys, one absent: one request, no exception).
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

### Task 4: Verified streaming publication {#task-4}

Read §6.2. Safe under either protocol; lands before the switch.

**Files:**
- Modify: `CA/Backend/CasBackend.h:55-60` (`StreamingBlobPublication` gains the expected `BlobRef`)
- Modify: `B:937-955` (check before `out->finalize`), `B:564` (emulated path, before its rename);
  `CA/Backend/CasInMemoryBackend.cpp:194-230`
- Modify: `W:497` (pass `ref`); `src/Common/ProfileEvents.cpp` (`CASBlobPublishDigestMismatch`)
- Test: create `T/gtest_cas_blob_publish_digest.cpp`

**Interfaces:**
- Consumes: `makeBlobHashingWriteBuffer(BlobHashAlgo, WriteBuffer &)` (`CA/Primitives/CasBlobHashingWriteBuffer.h:40`).
- Produces: `struct StreamingBlobPublication { uint64_t payload_size; String fresh_envelope; std::function<std::unique_ptr<ReadBuffer>()> open_payload; BlobRef expected_ref; };`
- Rules: hash exactly the payload bytes forwarded, not the envelope, with the hashing buffer of `expected_ref.algo`,
  and compare with the key's digest before the object completes. Mismatch: `out->cancel`, increment the counter,
  `CORRUPTED_DATA` `payload of <key> does not hash to its key; upload aborted, nothing published`. The writer
  treats it as deterministic: no further attempt. The verbatim staged copy (`B:959-977`) is not hashed.
- [ ] **Step 1: Write the failing tests,** suite `CASBlobPublishDigest`: `FlippedByteIsRefusedBeforeCompletion`
  (local object storage and `InMemoryBackend`: no object at the key afterwards, counter 1);
  `StaleSourceOfASecondAbsentPublisherIsRefused` (S10: the source changes to equal-length other bytes between
  `HEAD` and publish; the earlier body stays); `S3StagedRestreamIsVerified` (over `EtagFaithfulPublicationBackend`);
  `MismatchIsNotRetried` (one publication attempt); `LargePayloadUsesTheStreamingConvention` (a payload over one
  hashing block publishes).
- [ ] **Step 2: Run red.** Field added, no check. Filter `CASBlobPublishDigest*`.
- [ ] **Step 3: Implement.** `cmake .` first.
- [ ] **Step 4: Run green,** then the gate.
- [ ] **Step 5: Commit.** Subject: `Hash every streamed CAS blob before its object completes`

### Task 5: Generation gate and the pool dialect {#task-5}

Read §2.3, the dialect rule of §6.1, §7.5. After this commit every pool this build opens is generation 4, and the
marker protocol runs on it unchanged, writer and GC alike.

**Files:**
- Modify: `CA/Pool/CasPool.h:450` (`mount_reader_floor = G_BLOB_INTENTS`)
- Modify: `CA/Pool/CasPool.cpp:425-611` (`Pool::open`: prefix check before a mint, `requireGeneration4Pool` after
  `createOrValidate`), `:628-640` (`raiseReaderFloor` deleted), `:904` (its call deleted), `:1011`
  (`openForDecommission` calls `requireGeneration4Pool`)
- Modify: `CA/Formats/CasPoolMetaFormat.h:53-71` (drop `initial_min_reader_generation`; delete
  `raiseMinReaderGeneration`, `:61`); `CA/Pool/CasPoolMeta.cpp:111-123` (`raiseMinReaderGeneration`), `:125-175` (a mint writes floor 4 and the dialect)
- Modify: `CA/Backend/CasRequests.h` (accessor below)
- Modify: `G:5185-5270` (`acquireOrRenewLease`), `G:4488` (`rebuildBaseline`) and `G:5061` (`previewDeletes`): rule G1
- Modify: `T/cas_test_helpers.h` (two fixtures); `T/gtest_cas_pluggable_hash.cpp:713`
- Test: create `T/gtest_cas_blob_intent_generation.cpp`; every test file Step 4 edits

**Interfaces:**
- Consumes (Task 2): `G_BLOB_INTENTS`, `PoolMeta::token_dialect`, `GcState::header_generation`. (Task 3):
  `Layout::maxPoolPrefixBytesForBlobIntents`.
- Produces:

  ```cpp
  /// CasPool.h. Refuses a pool below generation 4 or one whose recorded dialect differs from `backend_dialect`.
  void requireGeneration4Pool(const PoolMeta & meta, Dialect backend_dialect);
  /// CasRequests.h, in CasOperation
  Dialect backendDialect() const;
  /// T/cas_test_helpers.h
  void seedGeneration3PoolForTest(DB::Cas::Backend & backend, const String & pool_prefix = "p", uint64_t gc_shards = 1);
  class DialectOverrideBackend;   /// an InMemoryBackend whose dialect() returns the constructor argument
  ```

- Rules:
  - A mint (`createOrValidate` with `allow_mint`) writes `min_reader_generation = G_BLOB_INTENTS` and
    `token_dialect = op.backendDialect()`, in the one create. Before it, `Pool::open` refuses a pool prefix longer
    than `maxPoolPrefixBytesForBlobIntents()`: `BAD_ARGUMENTS`, `pool prefix of <n> bytes leaves no room for blob
    intent keys; the limit is 668`.
  - `requireGeneration4Pool` runs on every open (writable, read-only, decommission) right after `_pool_meta` is
    read, before any request on a blob or an intent. Floor below 4 → `UNKNOWN_FORMAT_VERSION`, `CAS pool meta:
    pool is at reader generation <n>, this build needs 4; stop every server of the pool and run 'clickhouse-disks
    cas-migrate-pool'`. Another dialect → `BAD_ARGUMENTS`, `CAS pool records token dialect '<a>', this disk uses
    '<b>': identities are not comparable`.
  - G1: a decoded `gc/state` with `header_generation < 4` fails the lease step, the rebuild and the dry run with
    `INVALID_STATE`, `cas_gc_state is at generation <n> in a generation 4 pool`, and no request after that read.
  - `seedGeneration3PoolForTest` writes `_pool_meta` at floor 3 with no dialect (raw `encodePoolMeta`) and an empty
    catalog, as `seedPoolMetaForRestart` (`T/cas_test_helpers.h:1082`) does.
- [ ] **Step 1: Write the failing tests,** suites `CASBlobIntentGeneration` and `CASBlobIntentDialect`, each
  asserting zero blob and zero intent requests in the journal after the refusal:
  `WritableMountOfAGeneration3PoolIsRefusedAndNamesTheTool`, `ReadOnlyOpenIsRefused`, `DecommissionOpenIsRefused`,
  `NewPoolIsCreatedAtGeneration4WithItsDialect`, `MountNeverRaisesTheFloor` (no `_pool_meta` write on a reopen),
  `GcStateBelowHeader4FailsTheRoundWithNoRequest` (bytes of a header-4 encoding with the header rewritten to 3),
  `RebuildAndDryRunRefuseAGcStateBelowHeader4`, `PoolPrefixOver668BytesIsRefusedAtCreation` (668 accepted);
  `CASBlobIntentDialect.WritableMountIsRefused`, `.ReadOnlyOpenIsRefused` (so `cas-fsck`, `cas-gc-dryrun` and
  `cas-gc-rebuild` never run), both over `DialectOverrideBackend(Dialect::ETag)` on an `emulated` pool.
- [ ] **Step 2: Run red.** Filter `CASBlobIntentGeneration*:CASBlobIntentDialect*`.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run green, then the sweep.** Tests that build an older pool or assert the floor raise go red.
  Enumerate with `grep -rln 'raiseMinReaderGeneration\|raiseReaderFloor\|initial_min_reader_generation\|G_REF_CHAIN\|G_REF_MOVE\|min_reader_generation' $WT/src/Disks/tests`.
  A test whose subject is the floor raise is deleted; one that only needs a pool is moved to generation 4. Gate on
  `build` and `build_asan`; list each edit and deletion in the report.
- [ ] **Step 5: Commit.** Subject: `Open only generation 4 CAS pools and record their token dialect`

### Task 6: Switch the writer and GC to intents {#task-6}

Read §3.1, §3.2, §3.4, §4.3 and the ledger of §1. One commit, because the writer and GC must agree. The marker
code (`CA/Gc/CasGcMetaWriter.*`, the writer helpers of `CA/Pool/CasBlobMeta.*`) stays compiled and loses every
caller; its own unit suites stay green until Task 8.

**Files:**
- Modify: `W:274-570` (`ensureBlobPresent`), `CA/Pool/CasPartWriteTxn.h:86-90` (`BlobPublicationReason`)
- Modify: `CA/Gc/CasBlobInDegree.h:246-264` (`RetiredMergeResult`), `:395-468` (contract and signature of
  `foldDeltasIntoGeneration`; delete `GraduationReadHints`); `N:517-668` (`settleEntry`, `closeBlob`)
- Modify: `G:583-763` (body deletes without a `HEAD`, the intent delete batch), `G:2154-2287` (the hooks),
  `G:1052` (phase `pending_deletes` also sends intent deletes), `G:5061` (`previewDeletes`), the round's
  `meta_writer` calls
- Modify: `CA/ContentAddressedSettings.cpp:69` and `CA/Pool/CasPool.h:105` (`gc_round_graduation_budget` default
  1000; description: intent creates per round); `src/Common/ProfileEvents.cpp`
- Test: create `T/gtest_cas_blob_intent_writer.cpp`, `T/gtest_cas_gc_intents.cpp`; modify every suite Step 4 lists

**Interfaces:**
- Consumes (Tasks 1, 3): `canonicalIdentity`, `sameIncarnation`, `Layout::blobIntentKey`, `writeOnceBlobIntentKey`,
  `createBlobIntent`. (Task 2): `standing`. Existing: `removeCohortWriteOnce` (`G:562`), `bulkDeleteChunkKeys`
  (`G:576`), `GcRoundWorkBudget` (`CA/Gc/CasBlobInDegree.h:283`).
- Produces:

  ```cpp
  /// CasPartWriteTxn.h; words "absent", "intent_present", "token_rejected" in events
  enum class BlobPublicationReason : uint8_t { Absent, IntentPresent, TokenRejected };
  /// CasBlobInDegree.h; `confirm_condemned_marker` and `graduation_reads` leave the signature
  using IntentCreateFn = std::function<IntentCreateResult(const BlobRef &, const CanonicalIdentity &)>;
  /// new parameter of foldDeltasIntoGeneration, after `peek_head`:  const IntentCreateFn & create_intent
  struct BodyDelete { RetiredEntry entry; Meta observed; };   /// DELETE B If-Match observed.etag
  struct RetiredMergeResult
  {
      /* still_retired, spared, replaced, unmatched_removes stay; graduated and redelete go */
      std::vector<BodyDelete> body_deletes;      /// G4
      std::vector<WriteOnceKey> intent_deletes;  /// G7, G8
  };
  ```

- Writer rules (W1 to W7): `HEAD B` (W2); present → size checks as today, then `t.canonical()`: none → publish,
  reason `TokenRejected`, `CASBlobReuseTokenRejected`, one ERROR per mount (W3); else `op.read` of
  `blobIntentKey(ref, t)` (W4): an object → publish, reason `IntentPresent`, event `BlobReuseResurrect`,
  `CASBlobReuseIntentPresent`; an answered 404 → adopt, `CASBlobBodyPutAvoided`, event `BlobReuseAdopt`. A refused
  or unanswered `HEAD` or `GET` throws: never absence. The verbatim staged copy only when W2 answered absent and
  `beginPublication` is the first (W5). No request after a publication (W6). Both reads through `op.head` and
  `op.read` of this attempt (W7). The loop, its deadline and the cap of 8 stay (`W:296`, `:406`, `:415-423`), and
  so do the admission checks before each proof (`W:454`, `:466`, `:545`, `:562`).
- GC rules, all decided in the fold after the cut (§3.2 "Order inside one round"):
  - `head_blob` (G2): canonical present body → `create_intent` when the budget admits, else no request;
    confirmed → `pending(t)`, else `candidate(t)`; absent or uncanonical → no row. No marker write.
  - `settleEntry` at zero, by state: `candidate` with `condemn_round < current_round`, not suppressed, budget
    admits → G3, the create only, no `HEAD`; `pending`, not suppressed, redelete budget admits → G4 `peek_head`:
    same identity → write `standing(t)` and push a `BodyDelete`; absent → `standing(t)`; another canonical `u` →
    G7; uncanonical → carried, `CASGCIntentTokenRejected`; `standing`, not suppressed → G8: push
    `writeOnceBlobIntentKey(ref, t)`, write no row (no request when `t` does not canonicalise).
  - Touched key at zero with a row of any state (`N:612-633`): the `peek_head` there is the decision `HEAD`; another
    canonical `u` → G7 before any G4 or G8 of that row; same identity on a pending row is reused as the G4 `HEAD`.
    G7: create `I(B,u)` as G2, push `I(B,t)` to `intent_deletes`, row `pending(u)` or `candidate(u)`.
  - In-degree above 0 (G6): drop the row, `CASGCIntentLeftOnSpare`, no request.
  - Batch (`redeleteBlobs`): `op.remove(key, observed.etag)` per `BodyDelete`, no `HEAD`; `Removed`, `Gone`,
    `Mismatch` are success; then `intent_deletes` in chunks of `bulkDeleteChunkKeys` through
    `removeCohortWriteOnce`. Any failed job throws and the round fails before its commit (G5); the error names the
    key.
  - Counters: `CASGCIntentCreated`, `CASGCIntentConfirmedExisting`, `CASGCIntentUnconfirmedCarry` (one ERROR per
    round), `CASGCIntentDeleted`, `CASGCIntentLeftOnSpare`, `CASGCIntentTokenRejected` (one ERROR per round).
  - `previewDeletes` reasons: `unreachable`, `candidate`, `pending` (body delete), `standing` (intent delete).
- [ ] **Step 1: Write the failing writer tests,** suite `CASBlobIntentWriter`, on `EtagFaithfulPublicationBackend`
  and `ContentEtagBackend`, each asserting the request journal: `FirstCreationIsHeadAndOnePut`,
  `CleanReuseIsHeadAndGet`, `IntentPresentPublishesFreshOnce` (also with other bytes at the intent key),
  `NoSecondHeadOnAnyPath`,
  `LandedAmbiguousPublicationIsAdoptedInThreeReads` (`HEAD`, `PUT` unanswered and landed, `HEAD`, `GET`),
  `FailedIntentReadIsNotAbsence` (refused and unanswered `GET`: exception, no adoption),
  `WriterNeverMutatesIntents`, `NoMetaRequestOnAnyPath`, `VerbatimCopyIsSelectedOnlyAfterAnAbsentFirstHead`,
  `WellFormedUncanonicalTokenPublishesFresh` (`W/"x"`), `MalformedTokenFailsTheRead`,
  `AdoptionReadsReachTheStoreAfterAPrimedAbsence` (read `I(B,t)` absent through the same pool, let GC confirm and
  commit, then write: the backend sees this attempt's `HEAD` and `GET`, and the writer publishes).
- [ ] **Step 2: Write the failing GC tests,** suite `CASGcIntents`, driving `Gc::runRegularRound` on a pool
  of this build: `CondemnCreatesInTheSameRound`, `ConfirmedCreateCommitsPending`, `EqualOccupantConfirms`,
  `UnansweredCreateCommitsCandidate`, `CandidateRetriesTheCreate`, `PendingDeletesTheBodyOnlyOnALaterCut`,
  `BodyDeleteCommitsStandingAndKeepsTheIntent`, `AbsentBodyAtTheDeleteCommitsStanding`,
  `StandingRemovesTheIntentOnALaterZeroCut`, `SuppressedRoundDeletesNoIntent`,
  `SpareDropsStandingAndLeavesAnOrphanIntent`, `SpareDropsPendingAndKeepsTheIntent`,
  `DecisionHeadsAreSentAfterTheCut`, `CreatesOverTheBudgetAreCandidates` (budget 2, five zero transitions: two
  creates, three candidates, no request for those three), `DeleteHeadDecidesTheRowInTheFold`, `MismatchCommitsStanding`,
  `UnansweredBodyDeleteFailsTheRoundAndKeepsPending`, `FailedIntentDeleteFailsTheRound`,
  `IntentDeletesShareABatchRequest` (`gc_shards = 4`, intent deletes of every shard in one request on
  `BatchCapabilityBackend`),
  `FiveRequestsPerReclaimedBlob` (condemn 2, delete 2, remove 1), `NoMetaRequestInARound`,
  `RecreatedKeyMovesStandingToPendingInOneCommit` (`HEAD`, intent `PUT`, intent `DELETE`, one commit),
  `TransitionRunsBeforeThePlainIntentDelete`, `PendingWithAnotherTokenAtTheDeleteHeadTransitions`,
  `RespelledTokenAtTheDeleteHeadIsTheSameIncarnation`, `UncanonicalRowTokenIsCarriedAtTheDeleteHead`,
  `NoIntentDeleteOnAnAbsentBodyWithoutACut`, `PreviewNamesEachRowState`.
- [ ] **Step 3: Run red.** Filter `CASBlobIntentWriter*:CASGcIntents*`.
- [ ] **Step 4: Implement, then re-target the existing suites.** Enumerate with
  `grep -lE 'blobMetaKey|MetaState::|condemnMeta|writeMetaClean|CASMeta|CASGCMeta|CondemnMarker|graduat|GraduationReadHints|confirm_condemned_marker|redelete|runRoundsUntilAbsent' $WT/src/Disks/tests/*.cpp $WT/src/Disks/tests/*.h`
  plus every red test of the gate. A test whose subject is the marker itself (`T/gtest_cas_gc_meta_writer.cpp`,
  `T/gtest_cas_blob_meta.cpp`) is untouched. A test that asserts marker requests, the graduation gate or three
  rounds to reclamation is moved to the new ledger: the body delete in the round after the condemning one, the
  intent removal in the next zero round. Fix the comment of `runRoundsUntilAbsent`. Delete a test whose subject no
  longer exists (the graduation read-ahead) and name it in the report.
- [ ] **Step 5: Run green,** then the gate on `build` and `build_asan`.
- [ ] **Step 6: Commit.** Subject: `Switch the CAS writer and GC from blob markers to deletion intents`

### Task 7: Adopt orphan intents {#task-7}

Read G9, §3.4 and Q12.

**Files:**
- Modify: `CA/ContentAddressedSettings.cpp:67` (new `intent_sweep_list_budget_keys`, default 1000, after
  `manifest_sweep_list_budget_keys`); `CA/Pool/CasPool.h:93`; `CA/ContentAddressedMetadataStorage.cpp:315`, `:808`
- Modify: `G:1302` area (round: one `LIST` page after `fold_ref_intake`, before `fold_reduce`, only when destruction
  is not suppressed; phase row `intent_sweep_list`), `G:1326` (commit stores the next cursor), `G:5028` (rebuild
  resets `intent_sweep_cursor`)
- Modify: `CA/Gc/CasBlobInDegree.h` (parameter), `N:590-668` (`closeBlob`)
- Modify: `src/Common/ProfileEvents.cpp` (`CASGCIntentAdopted`)
- Test: `T/gtest_cas_gc_intents.cpp`

**Interfaces:**
- Consumes (Task 3): `parseBlobIntentKey`, `blobIntentsPrefix`. (Task 2): `GcState::intent_sweep_cursor`.
  (Task 6): `peek_head`.
- Produces: `foldDeltasIntoGeneration(..., std::vector<ParsedBlobIntentKey> intent_nominations)`, after
  `source_retirements`; `PoolConfig::intent_sweep_list_budget_keys`.
- Rules: the page comes from `op.list(blobIntentsPrefix(), cursor, budget, Retry::standard())`; keys that do not
  parse are skipped; the next cursor is the page's continuation, empty at the end of the prefix. Nominations are
  routed to the shard of their `BlobRef` and enter the merge as `source_retirements` do, so a key with no edge is
  still visited. A nominated key is adopted only if it has no row in the adopted state, gets none from this fold,
  and is at zero; at most one per key per round. Then `peek_head`: same identity → `candidate(t)` at this round;
  absent or another identity → `standing(t)`; uncanonical → no row. Adoption deletes nothing in its round. A
  `standing` row whose later `HEAD` shows `t` is an anomaly: carried, one ERROR, counted in `RoundReport`.
- [ ] **Step 1: Write the failing tests,** suite `CASGcIntents`: `OrphanOverTheCurrentBodyBecomesACandidate`,
  `OrphanOverAnAbsentBodyBecomesStanding`, `OrphanOverAnotherTokenBecomesStanding`, `AdoptionDeletesNothingInItsRound`,
  `SweepSkipsAKeyWithARow`, `SweepSkipsAKeyTheFoldGivesARow` (also a fold `candidate` over the create budget),
  `SweepSkipsAKeyAtPositiveInDegree`, `OneOrphanPerKeyPerRound`, `SweepCursorAdvancesOnePagePerRound` (budget 2,
  five intents: three rounds to wrap), `RebuildResetsTheIntentCursor`, `SuppressedRoundListsNothing`,
  `UncanonicalTokenWritesNoRow`, `UncanonicalRowTokenIsMovedByACanonicalBody`, `OrphanAdoptionWithFourShards`,
  `RebuildOnAMigratedPoolKeepsHeader4AndAdoptsEveryIntent`, `LeaderDiesAfterTheCreateThenAdoptionConfirms` (§3.4 row 1).
- [ ] **Step 2: Run red.** Filter `CASGcIntents*`.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run green,** then the gate on `build` and `build_asan`.
- [ ] **Step 5: Commit.** Subject: `Adopt orphan CAS blob intents with a paged sweep`

### Task 8: Remove the marker code; protocol docs {#task-8}

Read §9.2. Deletes what Task 6 left unreferenced; the `.meta` codec (`CA/Formats/CasBlobMetaFormat.*`),
`Layout::blobMetaKey` and `deleteMetaExact` (`CA/Pool/CasBlobMeta.cpp:42`) stay for Tasks 9 and 10 and `cas-inspect`.

**Files:**
- Delete: `CA/Gc/CasGcMetaWriter.h`, `CA/Gc/CasGcMetaWriter.cpp`, `T/gtest_cas_gc_meta_writer.cpp`, `T/gtest_cas_blob_meta.cpp`
- Modify: `CA/Pool/CasBlobMeta.{h,cpp}` (keep `deleteMetaExact` and the decode helper the tool needs);
  `CA/Gc/CasGc.{h,cpp}` (`meta_writer`, its pool, `G:781`, phase `meta_pool_wait` `G:1268-1284`)
- Modify: `CA/ContentAddressedSettings.cpp:86` (`gc_meta_pool_size` stays declared, described as ignored);
  `CA/Pool/CasPool.h:176` (field deleted); `CA/ContentAddressedMetadataStorage.cpp:89`, `:315`, `:808`
- Modify: `src/Common/ProfileEvents.cpp` (delete `CASMetaPut`, `CASMetaCompareSwap`, `CASMetaDelete`,
  `CASMetaCreateClean`, `CASMetaAbsentRepublish`, `CASMetaResurrectClean`, `CASGCMetaOps`, `CASGCMetaWriteAnomaly`,
  `CASGCCondemnMarkerUnconfirmedCarry`; `:962-971`)
- Modify: `T/cas_gc_marker_test_support.h` deleted after `HeadHookBackend` and `ContentEtagBackend` move into
  `T/cas_test_helpers.h`; its includers updated
- Modify docs: `D/architecture/blob-protocol.md`, `D/architecture/garbage-collection.md`,
  `D/architecture/storage-layout.md`, `D/architecture/correctness.md`, `D/bucket-requirements.md`,
  `D/configuration.md`, `D/operations/monitoring.md`, `docs/en/antalya/metrics_and_events.md`,
  `docs/en/operations/system-tables/cas_gc_log.md`, `docs/en/operations/system-tables/cas_log.md`

**Interfaces:** Consumes Task 6 and 7 names. Produces nothing new.

- Docs content: the writer's two reads and the reuse cost after a standing intent; GC's three row states and the
  round order; intents in the storage layout; `LIST` and delete rights under `blob-intents/`, batch delete, one
  token dialect per pool (bucket requirements); no mixed versions on one pool, `gc_meta_pool_size` ignored,
  `cas_gc_round_graduation_budget` default 1000, `cas_intent_sweep_list_budget_keys`; the counters of §9.2; phase
  rows `intent_sweep_list` added and `meta_pool_wait` gone; event reasons `intent_present`, `token_rejected`.
- [ ] **Step 1: Write the failing tests.** `CASObservability.NoMarkerCounterExists` (iterate `ProfileEvents::getName(e)`
  for every `e < ProfileEvents::end()`; none of the nine removed names appears);
  `CASGcIntents.RoundHasNoMetaPoolWaitPhase` (the round's phase rows, through the phase sink).
- [ ] **Step 2: Run red.**
- [ ] **Step 3: Delete and move.** Gate for completeness, empty except the kept codec, `blobMetaKey`,
  `deleteMetaExact`, `CasInspect.cpp`, the fixtures `writeMetaClean`/`condemnMeta` and the tests of the codec:
  `git -C $WT grep -nE 'CasGcMetaWriter|loadMeta|reconcileMetaClean|validateMetaSize|scheduleCondemnMarkerWrite|CASMeta|CASGCMeta|CondemnMarker|meta_pool' -- src programs`.
- [ ] **Step 4: Docs.** Gate: `git -C $WT grep -nE '\.meta|CASMeta|Clean marker|Condemned marker|freshness marker|meta_pool' -- docs/en`
  lists only the migration page (Task 10) and legacy-marker text (Task 9).
- [ ] **Step 5: Run green,** then the gate on `build` and `build_asan`.
- [ ] **Step 6: Commit.** Subject: `Remove the CAS blob marker code and document the intent protocol`

### Task 9: `cas-fsck` for intents and legacy markers {#task-9}

Read §3.3.

**Files:**
- Modify: `CA/Tools/CasFsck.h:22-120` (classes, report), `:268` (`runFsck`); `CA/Tools/CasFsck.cpp:445-449`
  (operation), `:890-914` (listing), `:1125-1140` (`sameIncarnation` for the `PendingGc` class), `:1204-1222`
  (pairing deleted)
- Modify: `programs/disks/CommandFsck.cpp` (option `--fix legacy-markers`); `CA/Pool/CasPool.cpp:445-446` (comment:
  a read-only open never writes, and deletes only `.meta` under an explicit fix)
- Modify: `src/Interpreters/InterpreterSystemQuery.cpp:2637`, `:2688` (six columns appended)
- Modify: `tests/queries/0_stateless/05020_cas_fsck.sh` and its reference if the summary line changes (predicted)
- Modify docs: `D/operations/troubleshooting.md`, `D/operations/debugging.md` (classes, the fix option)
- Test: `T/gtest_cas_fsck.cpp`

**Interfaces:**
- Consumes (Tasks 1, 3): `parseBlobIntentKey`, `isBlobIntentRecord`, `canonicalIdentity`, `sameIncarnation`.
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
  /// F3: deletes each key with `op.removeCurrent`; returns the count; a refused or unanswered delete throws
  /// with the count deleted so far in the message.
  uint64_t removeLegacyMarkers(CasOperation & op, const std::vector<String> & meta_keys);
  ```

- Rules: F1 to F5 as written. An intent is an accounted object in every class total. F2 reads the intent with an
  exact `GET`, then `HEAD B`; a refused or unanswered read, or a token that does not canonicalise, is
  `intent_unread`. F3 deletes each `.meta` with `op.removeCurrent` (`Q:656`) on the read-only open's own
  operation; a refused or unanswered delete stops the run with the count deleted so far. `body_without_meta` and
  `meta_without_body` are deleted from the report and the summary line. `cas-fsck` never deletes an intent.
- [ ] **Step 1: Write the failing tests,** suite `CASFsck`: `IntentClassesCurrentReplacedBodyGoneMalformed`,
  `MalformedKeyIsIntentMalformed`, `FailedReadIsUnread`, `UncanonicalHeadTokenIsUnread`, `NoIntentIsEverDeleted`
  (journal: no `DELETE` on `blob-intents/`), `RefusedDeleteStopsTheRun`, `LegacyMarkersAreDebrisAndTheFixDeletesThem`,
  `FixRunWritesNothingAndMountsNothing`, `IntentsAreAccountedInTheTotals`. Failure cases assert that no key outside
  `.meta` was deleted.
- [ ] **Step 2: Run red.** Filter `CASFsck*`.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run green,** then the gate.
- [ ] **Step 5: Commit.** Subject: `Teach cas-fsck about blob intents and legacy markers`

### Task 10: `clickhouse-disks cas-migrate-pool` {#task-10}

Read §7 in full, §7.5 and Q1, Q3, Q7, Q8, Q10, Q11.

**Files:**
- Create: `CA/Tools/CasMigratePool.h`, `CA/Tools/CasMigratePool.cpp`, `programs/disks/CommandCaMigratePool.cpp`
- Modify: `programs/disks/ICommand.h:144` (declaration), `programs/disks/DisksApp.cpp:351-355` (registration and the
  deferred open), `programs/disks/CMakeLists.txt:22-25`
- Modify: `CA/Pool/CasPool.h:441-447`, `CA/Pool/CasPool.cpp` (`Pool::openForMigration`)
- Modify: `CA/ContentAddressedMetadataStorage.{h,cpp}` (`startup` at `:846` defers the pool open when the process flag
  is set; new `openPoolForMigration`, built as `openPoolView` builds its config, `:722-844`)
- Modify: `CA/Formats/CasPoolMetaFormat.h`, `CA/Pool/CasPoolMeta.cpp` (`raiseToGeneration4`)
- Modify docs: `D/operations/migration.md`, section "Migrating a pool to generation 4": stop every server, run
  the tool, start; the refusals; the two points of no return (M6, M8); the downtime; `--intents-for-all-bodies`
  and when to use it; the residuals R1 to R4 in plain words. `D/operations/troubleshooting.md`: the refusals.
- Test: create `T/gtest_cas_migrate_pool.cpp`

**Interfaces:**
- Consumes (Tasks 1 to 5, 9): `runCapabilityProbe`, `createBlobIntent`, `encodeGcState`, `requireGeneration4Pool`,
  `seedGeneration3PoolForTest`, `removeLegacyMarkers` (F3, for M9 on a rerun). Existing: `computeHeartbeatFloor`
  and `mountObservationThresholdMs` (`SR:387`, `:425-466`), `probeNonTerminalMountSlots` (`SR:505`),
  `deleteMetaExact`, `decodeBlobMeta`.
- Produces:

  ```cpp
  struct MigratePoolOptions
  {
      uint64_t grace_ms = 120'000;
      bool intents_for_all_bodies = false;
      bool dry_run = false;
      uint64_t poll_step_ms = 1'000;
      std::optional<size_t> stop_after_step_for_test;   /// throws after step N (1..9)
  };
  struct MigratePoolReport { std::array<uint64_t, 9> counts{}; bool completed = false; };
  MigratePoolReport migratePool(Pool & pool, const MigratePoolOptions & options,
                                const std::function<void(std::string_view)> & progress);
  static PoolPtr Pool::openForMigration(BackendPtr backend, PoolConfig config);   /// floor 3 or 4; no mount, slot, writer or GC
  static PoolMeta PoolMeta::raiseToGeneration4(CasOperation & op, const Layout & layout, Dialect dialect);
  bool deferPoolOpenForMigration();   /// process flag, set only by DisksApp for this command
  ```

- Rules: M1 to M9 in the order of §7.1, with its refusal texts from §7.5. M2 watches slots on the tool's clock
  with the disk's `mount_lease_ttl_ms` as the observer lease and `poll_step_ms` as the cadence, then fences through
  the replace `computeHeartbeatFloor` uses. Waits go through the request engine's injectable sleep. M5 confirms
  every create by `createBlobIntent`; `Unconfirmed` fails the run before M6. M6 is skipped when `gc/state` already
  has header 4. `raiseToGeneration4` refuses a body at floor 4 with another dialect. M9 deletes with `If-Match` the
  token of the M4 read; on a migrated pool it runs F3. The command prints the per-step counts, then
  `migration complete`. `--dry-run` runs M1 without the probe, `probeNonTerminalMountSlots`, M3 and M4, and writes
  nothing. No server path reaches `Pool::openForMigration`: check with
  `git -C $WT grep -n 'openForMigration\|deferPoolOpenForMigration' -- src programs`, which lists only
  `CasPool.*`, `ContentAddressedMetadataStorage.*`, `CasMigratePool.*`, `DisksApp.cpp`, `CommandCaMigratePool.cpp`.
- [ ] **Step 1: Write the failing tests,** suite `CASMigratePool`, on `seedGeneration3PoolForTest` fixtures with
  markers (`writeMetaClean`, `condemnMeta`) and condemned rows written with header-3 `gc/state` bytes:
  `RefusesASlotThatRenews`, `FencesASilentSlotAfterTheThreshold`, `RefusesAMovingGcLease`, `RefusesAPoolBelowGeneration3`,
  `RefusesAnotherDialectOnAMigratedPool` (`CASBlobIntentDialect.ToolOnAMigratedPoolOfAnotherDialectIsRefused`),
  `RefusesAPoolPrefixOver668Bytes`, `CondemnedMarkerGetsAnIntentForTheCurrentToken`, `UndecodableMarkerCountsAsCondemned`,
  `EveryCondemnedRowGetsAnIntent`, `PendingRowWithAnAbsentOrReplacedBodyGetsAnIntentForItsOwnToken`,
  `AbsentBodyAndUncanonicalTokenGetNoIntentForTheCurrentBody`, `UnconfirmedCreateFailsTheRun`,
  `RowsAreCarriedAsTheyAre`, `GcStateIsRewrittenAtHeader4WithItsFieldsAsRead`, `LateOldGcCommitRestartsTheRun`,
  `RecheckRefusesASlotThatAppearedAfterTheGcStateRewrite`, `RecheckRefusesAMovedGcPair`, `FloorIsTheLastProtocolWrite`,
  `NoMarkerIsDeletedBeforeTheFloor`, `CrashAfterEachStepThenRerunCompletes` (one case per step),
  `RerunOfAMigratedPoolOnlyRemovesMarkers`, `TwoRunsConverge`, `DryRunWritesNothingAndSendsNoProbe`,
  `AllBodiesOptionGivesEveryListedBodyAnIntent`, `OnlyThisCommandReachesOpenForMigration` (a disk `startup` on a
  generation-3 pool refuses with the §7.5 text unless the flag is set, and with it sends no request),
  `InheritedCandidateConfirmsOnTheToolsIntent`, `InheritedPendingRowIsDeletedByTheFirstRoundAtZero`,
  `CleanMarkedBodyIsAdoptedWithNoUpload`, `LateOldDeleteMissesAfterARepublish`, `MarkersAcrossListPagesAreAllRead`.
- [ ] **Step 2: Run red.** Filter `CASMigratePool*:CASBlobIntentDialect*`.
- [ ] **Step 3: Implement.** `cmake .` first.
- [ ] **Step 4: Run green,** then the gate on `build` and `build_asan`; also run
  `./programs/clickhouse disks --help` from the release build only if it is already built (do not build `clickhouse`
  in this task) and note the command listed.
- [ ] **Step 5: Commit.** Subject: `Add clickhouse-disks cas-migrate-pool for generation 4`

### Task 11: Stateless and integration tests {#task-11}

Read §10.2, rows "Stateless" and "Integration". Written here, run by the controller; references are predicted.

**Files:**
- Create (with `add-test`): `tests/queries/0_stateless/<n>_cas_blob_intent_requests.sh` and its reference
- Create: `tests/integration/test_cas_blob_intent_migration/` (`__init__.py`, `test.py`, `configs/`), modelled on
  `tests/integration/test_cas_ref_move_compat/test.py:13-25` (an older `altinity/clickhouse-server` instance and a
  current one on one bucket)
- Modify: `tests/integration/test_cas_gc_s3/test.py` (new test), `tests/integration/test_cas_s3/test.py` and
  `tests/integration/test_cas_gcs/test.py` (marker assertions re-targeted); enumerate with
  `git -C $WT grep -lE 'CASMeta|CASGCMeta|CondemnMarker|\.meta' -- tests/integration tests/queries`

- [ ] **Step 1: Stateless test,** tags `no-fasttest` only if it uses a failpoint: one `INSERT` of new content →
  per blob 1 `CASBlobHead`, 1 `CASBlobPut`, 0 `CASBlobIntent*`; a duplicate `INSERT` → 1 `CASBlobHead`,
  1 `CASBlobIntentGet`, 0 `CASBlobPut`; `DROP`, two `SYSTEM CAS GC RUN`, the duplicate again → 1 `HEAD`,
  1 intent `GET`, 1 `PUT` (the standing intent forces one upload). Counters from `system.query_log`.
- [ ] **Step 2: Integration, migration,** on RustFS and the GCS emulator: `test_old_image_builds_condemned_rows`
  (writes, drops, runs GC until candidate and pending rows exist); `test_tool_refuses_a_live_older_server` (names it);
  `test_tool_completes_with_every_server_stopped`; `test_new_image_starts_and_old_image_is_refused` (the §7.5 text);
  `test_data_reads_back`; `test_duplicate_insert_reuses_with_two_reads`; `test_fsck_reports_no_dangling_ref_and_no_marker`;
  `test_head_token_form_per_store` (recorded per store, multipart upload and multipart copy above 32 MiB included).
- [ ] **Step 3: `test_cas_gc_s3`:** `test_gc_requests_per_reclaimed_blob` asserts at most 5 requests per reclaimed
  blob from `system.blob_storage_log`.
- [ ] **Step 4: Style check** of the new shell test with `ci/jobs/check_style.py` functions; `python3 -m py_compile`
  on the new `test.py`.
- [ ] **Step 5: Commit.** Subject: `Add CAS blob intent request and migration tests`. The report lists every predicted value.

### Task 12: Measurements, soak, defaults {#task-12}

Read §10.3 and the soak row of §10.2. Runs on the stand and in `M` (`utils/cas-yardstick`, `utils/ca-soak`); the
owner fixes the defaults from the report. The task commits the yardstick reference cards and soak cards, and a
change to a default only after the owner's decision.

Acceptance:

| Measure | Pass |
|---|---|
| `INSERT` of new content | per blob 1 `HEAD`, 1 `PUT`, no intent request |
| `INSERT` of duplicate content | per blob 1 `HEAD`, 1 intent `GET`, no `PUT` |
| GC per reclaimed blob | at most 5 requests, 2 rounds from "no reference" to the body delete |
| Wide insert like the stand's (§1) | marker `PUT`s 54,997 → 0; upload thread time and wall time reported against the 2,047 + 1,325 thread-seconds baseline |
| Round time with synchronous creates, per 1,000 creates, S3 and GCS | reported; the default `gc_round_graduation_budget = 1000` stands if 1,000 creates fit the spec's estimate of about 24 s; otherwise the owner picks the value |
| Publication hash on a large body | CPU per GiB reported |
| Standing intents, equal parts dropped and re-inserted within one round | count of extra uploads reported |
| The tool on a pool like the stand's | requests per step against the §1 migration ledger; wall time; then a duplicated insert: two reads, no upload |
| Soak, 2 hours, with process kills | fsck: zero dangling refs; after writers stop and GC runs until the sweep passed every intent and two more rounds, no `intent_body_gone`; `CASBlobPublishDigestMismatch` is 0 |

- [ ] **Step 1:** Record the "before" yardstick cards on the current binary.
- [ ] **Step 2:** Build the branch's `clickhouse` (`ninja clickhouse > build_t12.log 2>&1`), run the cards, the wide
  insert and the round-time measurement on S3 and GCS.
- [ ] **Step 3:** Run the tool on a copy of the stand's pool, then the duplicate insert.
- [ ] **Step 4:** Run the soak.
- [ ] **Step 5:** Record the decisions of §9.1 in `M/docs/superpowers/cas/AGENTS.md`: invariant 5 (a durable write
  step removed, a read added, as a protocol change with a model) and the amendment of decision-4 (generation 4,
  offline tool, no downgrade). Apply [Backlog effects](#backlog-effects) to the backlog tasks.
- [ ] **Step 6:** Commit the cards, `AGENTS.md` and the backlog files in `M` by exact path. Subject:
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
| G9 anomaly "counted" | a `RoundReport` field and one ERROR line; §9.2 names no counter for it |
| Q10 "pool open deferred for this command only" | a process flag set by `DisksApp` for `cas-migrate-pool` before the disk starts |
| M2 "the tool's poll step" | `poll_step_ms = 1000` |

## Backlog effects {#backlog-effects}

| Backlog task | Effect | When |
|---|---|---|
| CAS-384, CAS-330, CAS-334, CAS-345, CAS-29.3, CAS-439, CAS-326 | moot: they exist for the marker or its residuals | after Task 8 |
| CAS-29.4, CAS-336, CAS-284.5 | re-targeted from marker requests to intent requests | after Task 6 |
| CAS-10, CAS-11, CAS-289.6 | intents replace `.meta` in the fsck and byte classes; the pairing counters go | after Task 9 |
| CAS-33.2, CAS-33.3 | unchanged | — |
| new | the tool, the generation gate, the intent sweep: one task each, closed by Tasks 10, 5 and 7 | before Task 1 |
