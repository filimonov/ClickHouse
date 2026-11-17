# CAS blob deletion intents: implementation plan {#cas-blob-deletion-intents-plan}

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan
> task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. A dispatch hands the implementer three texts:
> [Global Constraints](#global-constraints), the task's own section, and the spec.

**Goal:** A CAS writer publishes a blob body with one `PUT` and no `Clean` marker, reuses a body after
`HEAD`, intent `GET`, `HEAD`, and GC alone warns writers with an immutable deletion intent keyed by the body
incarnation. Ships as format generation 4 with one upward switch per pool.

**Architecture:** Twenty-one commits. Tasks 1 to 8 add inert parts: the token identity, the generation 4 control
fields, the intent object, the content type in the shared S3 code, the mark on `publish` and `HEAD`, the learned
flag, the mark bit and the payload hash check, the test fixtures. Tasks 9 to 14 add the protocol behind the
pool's `blob_protocol`, which only tests set until Task 15: the GC mode read, GC intent mode, the writer rules on
a learned mount, the switch, the new-pool sequence, the tools. Task 15 switches it on: a pool created by this
build records `created_generation = 4`, and a mount that saw the switch without learning never adopts. Task 16
removes the writer's marker path and moves the writer tests. Tasks 17 to 21 add the SQL command, the transition
tests, integration and stateless tests, docs and measurements.

**Tech Stack:** C++ (`CA/` metadata storage of the ClickHouse fork, the shared S3 write and copy code),
GoogleTest (`unit_tests_dbms`), stateless tests (shell), integration tests (pytest, RustFS, the GCS mock),
Markdown docs, Python (`utils/cas-yardstick`, `utils/ca-soak`).

**Spec:** `/home/mfilimonov/workspace/ClickHouse/master/docs/superpowers/specs/2026-11-17-cas-blob-deletion-intents-design.md`
at commit `c182f39a138` (binding: a conflict between this plan and the spec resolves to the spec, except the items
under [Spec items this plan settles](#settled) and the defaults under [Spec questions](#spec-questions)). "§N"
below is a section of that spec. The plan-level tables the spec dropped are in [Effects](#effects).

**GC lands before the writer.** A tree where the writer follows §3.2 and GC still condemns by marker is
safe on an unswitched pool (§7.1, stage T1), but there the writer never adopts, so every reuse test of the fleet
would go red until its pool is switched, and a switched pool needs intent-mode GC. So GC intent mode lands first
(Tasks 9 and 10) and is reachable only on a switched pool. The writer rules land next (Task 11) and apply to a
learned mount. A mount that is not learned keeps today's marker path for commits 11 to 14, where no production
path switches a pool. Task 15 creates new pools switched and, in the same commit, stops a mount that saw the
switch without learning from adopting. Task 16 removes the marker path. The reverse order (writer first, or a
switched pool with today's writer) is the unsafe state X1 of §7.8.

## Global Constraints {#global-constraints}

### Paths {#paths}

| Name | Value |
|---|---|
| `WT` | `/home/mfilimonov/workspace/ClickHouse/cas-pr1`, new branch `work/antalya-26.8/cas-blob-intents`, base `a966a374b17` |
| `M` | `/home/mfilimonov/workspace/ClickHouse/master`, branch `cas-gc-rebuild` (spec, this plan, `utils/cas-yardstick`, `utils/ca-soak`) |
| `R` | `$M/tmp/intents/impl` (`LEDGER.md`, `reports/task<N>.md`, saved logs `logs/`) |
| `CA/` | `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/` |
| `T/` | `src/Disks/tests/` |
| `D/` | `docs/en/antalya/cas/` |
| `Q/` | `tests/queries/0_stateless/` |
| `I/` | `tests/integration/` |
| `W:` `G:` `N:` | `CA/Pool/CasPartWriteTxn.cpp`, `CA/Gc/CasGc.cpp`, `CA/Gc/CasBlobInDegree.cpp` |
| `B:` `Q:` `X:` | `CA/Backend/CasObjectStorageBackend.cpp`, `CA/Backend/CasRequests.cpp`, `CA/ContentAddressedTransaction.cpp` |
| `S3:` | `src/Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.cpp` |

Every `path:line` was read in `WT` at `a966a374b17`. Lines move as tasks land: locate by the named symbol, use
the line as a hint. In commands `CA`, `T`, `D`, `Q`, `I` are shell variables with these values, without the slash.

### Before Task 1 (controller) {#before-task-1}

- §10.1 requires the TLA+ models with a results file before implementation. The model study commits them
  under `$M/docs/superpowers/models/`: `CaBlobIntentProtocol*.tla` with its configurations, the fresh-pool
  module `CaBlobIntentFreshPool.tla`, and `CaBlobIntentProtocol*_RESULTS.md`. They carry the content §10.1 lists
  under the names `CaBlobIntentCore` and `CaBlobIntentTransition`. The controller records the results file path
  and its commit in `$R/LEDGER.md`. No task starts before that ([Spec questions](#spec-questions), Q1).
- `git -C $WT status --short` prints nothing. `git -C $WT switch -c work/antalya-26.8/cas-blob-intents a966a374b17`.
- Build `build` and `build_asan`, run `CAS*` on both, and record the pass counts as `N_rel` and `N_asan` in
  `$R/LEDGER.md`. Each task report gives the new release count and the number of tests it added and deleted; a
  milestone report also gives the ASan count.

### Binding rules {#binding-rules}

- **Worktree.** Modify files only under `WT`; Task 21 alone works in `M`. No push, no pull request. The only
  writes outside: the task report and logs under `$R`.
- **No destructive commands.** No `git clean`, `git reset --hard`, `git checkout -- <file>`, `git stash`,
  `git rebase`, `git commit --amend`. Never delete or re-create a build directory. A hung test binary is stopped
  by its exact PID.
- **C++.** Allman braces. No `sleep` to fix a race: tests use hooks, latches and fake clocks, every wait is
  bounded. No fallback paths: an error propagates; no outcome is replaced by a default. Say "exception", not
  "crash", for logical errors. Function names in prose and comments as `f`, not `f()`. Comments say why, in one
  or two lines, with no task ids, no plan or review references, no history.
- **The on-store format is frozen.** Every new field belongs to generation 4, and the task that adds it states
  what an older build does when it meets it (§2.4).
- **`LOGICAL_ERROR` tests.** A test that expects `LOGICAL_ERROR` is split: the catching test under
  `#ifndef DEBUG_OR_SANITIZER_BUILD`, an `EXPECT_DEATH` twin in a `*DeathTest` suite under
  `#if defined(DEBUG_OR_SANITIZER_BUILD)` (form: `T/gtest_cas_promote_republish.cpp:353-402`). A death suite
  never constructs `DiskObjectStorage` in the parent; a death-test child exits with `std::_Exit`.
- **Shared, non-CAS code.** Changed only by the tasks that list it in [Shared code changes](#shared-code), each
  change minimal, motivated in the commit body and portable upstream. No CAS field enters a generic structure
  beyond the one optional content type.
- **Builds.** Build directories in `WT`: `build` (release), `build_asan` (ASan + UBSan), `build_fast` (no S3).
  No `-j`. One build at a time, in the foreground. Never build the `clickhouse` target in a task. A task that
  adds a `.cpp` file runs `cmake . > cmake_<tag>.log 2>&1` in the build directory first. Logs are read with
  `grep -a`.

  ```bash
  cd $WT/build && ninja unit_tests_dbms > build_<tag>.log 2>&1; \
    echo "NINJA_EXIT=$? HEAD=$(git -C $WT rev-parse --short HEAD)" >> build_<tag>.log
  mkdir -p $WT/tmp/gtest_<tag> && cd $WT/build && \
    TMPDIR=$WT/tmp/gtest_<tag> ./src/unit_tests_dbms --gtest_filter='<filter>' > test_<tag>.log 2>&1; \
    echo "GTEST_EXIT=$?" >> test_<tag>.log; rm -rf $WT/tmp/gtest_<tag>
  grep -a 'NINJA_EXIT' $WT/build/build_<tag>.log; grep -a -c ' error: ' $WT/build/build_<tag>.log
  grep -a '\[  PASSED  \]\|\[  FAILED  \]\|GTEST_EXIT' $WT/build/test_<tag>.log | tail -20
  ```

  Tags: `t<N>_red`, `t<N>_green`, `t<N>_pre`, `t<N>_gate`, `t<N>_asan`, `t<N>_m<k>` (a mutant). A test log is
  evidence only if the build log of the same tag ends with `NINJA_EXIT=0`. A test log without a `[  PASSED  ]`
  line means the binary aborted: the last `[ RUN      ]` line names the test.
- **Gate.** At the end of every task: EXACTLY `--gtest_filter='CAS*'` on `build`, on the committed tree, with
  the `HEAD` marker in the build log (tag `t<N>_gate`). The same filter on `build_asan` (tag `t<N>_asan`) runs
  at the milestones: after Tasks 3, 8, 10, 11, 13, 14, 16 and 18, and in the [branch gates](#branch-gates).
  Between milestones, a task that adds a `*DeathTest` twin or a test with a background thread runs its new
  suites on `build_asan` before its commit (Tasks 2 and 6). A red gate after the commit is fixed in a further
  commit (same staging and trailer rules) and the gate reruns. A red ASan milestone is traced to its commit by
  rerunning the red tests on the commits since the last green milestone. A red existing test that the task
  does not name and this plan does not explain is a stop: report the test, the assertion and the log.
- **Red first.** Write the tests before the product change and run them. The red run fails on an assertion, not
  on a compile error: a new signature is added first with today's behaviour or a stub. Copy the red log to
  `$R/logs/red_t<N>.log`. A test marked "pin" is green before and after.
- **Predicted numbers.** Unit-level request counts are authoritative. A stateless or integration reference is
  edited to the predicted value and listed in the report as "predicted, not run". If a number differs from the
  prediction, stop and report; never adjust the product to a number.
- **Stateless and integration tests** are written but not run in a task (no server, no docker); the controller
  runs them. New stateless tests are created with `./tests/queries/0_stateless/add-test <name>[.sh]`. No `no-*`
  tag unless the test cannot run otherwise; a test that uses a failpoint carries `no-fasttest`.
- **Mutants.** A mutant exists in the tree only while it is being built in the foreground. Before it:
  `cp <file> $R/logs/t<N>_<name>.keep`. After it: copy back and `cmp` the two.
- **Docs** under `docs/`: every heading has an explicit `{#anchor}`; no invariant tags (`I4`, `W3`, `G7`) in
  user docs; present behaviour, no history phrasing.
- **Commit.** One commit per task, by exact file list. Never `git add -A` or `git add .`.

  ```bash
  cd $WT && git add <files> && git diff --cached --name-only   # must equal the task's list
  git commit -F - <<'EOF'
  <subject from the task, imperative, at most 72 characters>

  <body: the why, literal names in backticks; for shared code, why the change is needed and that the default is unchanged>

  Signed-off-by: Mikhail Filimonov <mfilimonov@altinity.com>
  Co-Authored-By: Claude Fable 5.2 <noreply@anthropic.com>
  EOF
  ```

  No other trailer, no session link, no backlog id, none of the words "spec", "plan", "task", "review".
- **Report.** `$R/reports/task<N>.md`: what changed, red evidence, the pass count on `build` (and on
  `build_asan` at a milestone), every existing test that was edited or deleted with the reason, numbers that are
  predicted and not run, deviations, doubts.

### Shared code changes {#shared-code}

The complete list. Anything else outside `CA/`, `T/`, `Q/`, `I/`, `D/` and `programs/disks/` is a stop.

| # | File | Change | Task | Default for other callers |
|---|---|---|---|---|
| 1 | `src/IO/WriteSettings.h` | one field `String object_storage_content_type`, after `object_storage_copy_mode` (`:105`) | 4 | empty: `binary/octet-stream`, as today |
| 2 | `src/IO/WriteBufferFromS3.cpp:442`, `:775` | `CreateMultipartUpload` and `PutObject` send the field when it is set | 4 | unchanged bytes |
| 3 | `src/IO/S3/copyS3File.h:48-63`, `.cpp:148`, `:748`, `:1011` | one trailing parameter `const String & content_type = {}` on `copyS3File`, used by the multipart create and by `CopyObject` | 4 | unchanged bytes |
| 4 | `S3:1122-1137` | `S3ObjectStorage::copyObject` passes the field of its `WriteSettings` | 4 | empty |
| 5 | `src/IO/S3/getObjectInfo.h:14-23`, `.cpp:87-93`; `src/Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h:128`; `S3:973`, `:1012` | `String content_type` in `S3::ObjectInfo` and `ObjectMetadata`, filled from the `HEAD` response | 4 | no caller reads it |
| 6 | `src/Common/ProfileEvents.cpp` | new `CAS*` counters (Tasks 3, 7, 10, 11); three removed (Task 16, `:968-970`) | 3, 7, 10, 11, 16 | CAS-only names |
| 7 | `src/Parsers/ASTSystemQuery.h:172-178`, `ASTSystemQuery.cpp:304-307`, `ParserSystemQuery.cpp:546-558`, `src/Interpreters/InterpreterSystemQuery.{h,cpp}` (`:1164`, `:2792`, `:3685`), `src/Access/Common/AccessType.h:369-375`, `Q/01271_show_privileges.reference:177` | one `SYSTEM` query type beside `CAS_FSCK` | 17 | additive |
| 8 | `src/Storages/System/StorageSystemContentAddressedMounts.cpp:40-63` | three columns | 6 | CAS-only table |
| 9 | `src/IO/S3/tests/CountingS3Server.h` (test-only) | `Request` (`:85-94`) records the request headers; `ObjectStore` (`:270`) keeps the `Content-Type` of a `PutObject` and returns it on `HeadObject` | 4 | additive: its other users (`src/IO/S3/tests/gtest_s3_client_single_attempt_completion.cpp`, `T/gtest_cas_blob_publish_transport.cpp`, `T/gtest_cas_s3_single_attempt_client.cpp`) read no header |
| 10 | `src/Interpreters/InterpreterSystemQuery.cpp:2637` (`contentAddressedFsckColumns`), `:2688` (`appendContentAddressedFsckRow`) | the result of `SYSTEM CAS FSCK` gains one column per new report counter | 14 | CAS-only columns, appended |
| 11 | `docs/en/operations/system-tables/cas_mounts.md`, `cas_gc_log.md`, `cas_log.md`; `docs/en/antalya/metrics_and_events.md` | the reference pages of the changed tables and counters | 20 | CAS-only sections |

The Native-format specification is not touched: no task changes the `Native` format.

### Test vocabulary {#test-vocabulary}

- A live `Etag` exists only through a request: `openRequestsForTest(backend).admit().head(key, Retry::standard())`
  (`T/cas_test_helpers.h:126`); `OperationForTest` (`:185`). `FakeClock` (`:346`), `ScopedReadCeiling` (`:370`,
  "this build is an older reader"), `ManualBarrier` (`:215`), `expectThrowsCodeWithMessage` (`:2935`).
- Backends: `InMemoryBackend` (`CA/Backend/CasInMemoryBackend.h:28`, dialect `emulated`, tokens `"1"`, `"2"`, …);
  `CountingBackend` (`T/cas_test_helpers.h:1862`), `BatchCapabilityBackend` (`:2120`), `OrderedFaultBackend`
  (`:2194`), `LandedButAckLostOnceBackend` (`:2473`); `HeadHookBackend` and `ContentEtagBackend`
  (`T/cas_gc_marker_test_support.h:250`, `:301`); `EtagFaithfulPublicationBackend` (`T/gtest_cas_s3_staging.cpp:227`).
- Raw fixtures: `idOf(bytes)` (`T/cas_test_helpers.h:506`, the one-shot hash: equal to the streaming hash only up
  to one hashing block; a larger payload uses `streamingHexOf`, `:491`), `writeBlobBody` (`:1098`),
  `writeManifestRaw` (`:539`), `displaceBlobToken` (`:1039`), `writeMetaClean` (`:1133`), `condemnMeta` (`:1144`),
  `seedPoolMetaForRestart` (`:1082`), `runRegularRoundReclaiming` (`:918`), `currentRetiredSet` (`:949`).
- From Task 8 on, in `T/cas_blob_intent_test_support.h`: `seedUpgradedPoolForTest`, `openSwitchedPoolForTest`,
  `publishBlobRawForTest`, `ProtocolJournalBackend`, `DialectOverrideBackend` (signatures in [Task 8](#task-8)).
- **An unswitched pool** in a test of Tasks 6 to 14 is a pool without `created_generation`: Task 13 switches a
  pool that records it, and Task 15 makes every plain `Pool::open` of an empty prefix end switched. Tasks 6 and
  7 seed it by hand; from Task 8 on it is `seedUpgradedPoolForTest`.
- No `ASSERT_*` in a non-void helper. A test with a parked thread declares, in this order: the guard that clears
  hooks, every `std::future`, the guard that releases the parked thread.

## Review Focus {#review-focus}

The six failures no single task's own tests exercise by construction. Each is pinned to a test in an owning task.

1. **A cache or an earlier request answers an adoption read.** Owner: Task 11,
   `CASBlobIntentWriter.AdoptionReadsReachTheStoreAfterAPrimedAbsence` (the journal of the attempt holds both
   `HEAD`s and the intent `GET`, all after the edge, and the writer publishes).
2. **A GC round takes marker mode from a failed `_pool_meta` read.** Owner: Task 9,
   `CASGcIntentsMode.FailedPoolMetaReadFailsTheRound` and `.AbsentPoolMetaFailsTheRound` (zero requests after
   the read), plus the mutant "a failed read yields marker mode".
3. **The mark is lost on a multipart path.** Owner: Task 4, `S3ObjectContentType.MultipartCopyCreateCarriesTheSetType`
   and `.MultipartCreateCarriesTheSetType` (the wire request); Task 19, `test_mark_on_multipart_copy` and
   `test_mark_on_multipart_upload` per store.
4. **One incarnation spelled two ways by two requests.** Owner: Task 1,
   `CASTokenIdentity.SameIncarnationComparesCanonically`; Task 10, `CASGcIntents.RespelledTokenAtTheDeleteHeadStillDeletes`
   and `.RespelledTokenIsNotASupersede`; Task 14, `CASFsckIntents.RespelledTokenIsCurrentNotStale`.
5. **An intent removed while its incarnation is current.** Owner: Task 10, `CASGcIntents.SpareLeavesTheIntent`,
   `.SupersedeDeletesTheOldIntentOnce`, `.UnansweredBodyDeleteKeepsTheIntent`; Task 14,
   `CASFsckIntents.FixReadsTheIntentBeforeTheBody`.
6. **A rule wired to an entry point production never runs.** Owner: Task 6,
   `CASBlobIntentLearning.RenewerRereadsAfterSixtySecondsUntilLearned`, which runs the background renewal
   worker, plus the mutant "the re-read is called only from `Pool::renewWatermarkOnce`".

## Task list and pre-flight {#preflight}

Numbering against the reviewed revision of this plan (`42b93ede3c8`): was 9 -> 9, 10; was 14 -> 15, 16;
was 10 to 13 -> 11 to 14; was 15 to 19 -> 17 to 21. Tasks run in the order of their numbers.

Tier: implementer / reviewer. `ca-surgeon` and `ca-review` are the stronger pair; **bold** tasks are protocol or
GC safety and need it. Task 10 also gets a second, independent review. "ASan" marks a milestone of the
[gate](#binding-rules).

| # | Task | Product files, lines | Test lines | Risk | Tier | ASan |
|---|---|---|---|---|---|---|
| 1 | Token identity | 2, 120 | 250 | low | `ca-impl` / `ca-review` | |
| 2 | Control formats, generation 4 | 16, 380 | 650 | medium | `ca-surgeon` / `ca-review` | new suites |
| 3 | Intent object and key class | 9, 250 | 350 | low | `ca-impl` / `ca-review-lite` | yes |
| 4 | Shared S3: content type | 8, 80 | 300 | medium | `ca-surgeon` / `ca-review` | `S3*:WBS3*` |
| 5 | Backend mark and the mark probe | 10, 400 | 500 | medium | `ca-impl` / `ca-review` | |
| 6 | Learning the switch, mount rules, `system.cas_mounts` | 6, 400 | 550 | medium | `ca-impl` / `ca-review` | new suites |
| 7 | Publication: mark bit, payload hash check | 9, 250 | 350 | medium | `ca-surgeon` / `ca-review` | |
| 8 | Test fixtures; marker-mode suites pinned | 0 | 450 | low | `ca-impl` / `ca-review-lite` | yes |
| **9** | GC: the per-round mode read, the dialect check, leaving GC | 5, 200 | 350 | high | `ca-surgeon` / `ca-review` | |
| **10** | GC intent mode: the fold and the delete batch | 7, 700 | 900 | high | `ca-surgeon` / `ca-review` twice | yes |
| **11** | Writer rules on a learned mount | 3, 350 | 700 | high | `ca-surgeon` / `ca-review` | yes |
| **12** | The switch: checks, stamp, free lease | 4, 350 | 650 | high | `ca-surgeon` / `ca-review` | |
| **13** | New-pool sequence in `Pool::open` | 2, 120 | 450 | high | `ca-surgeon` / `ca-review` | yes |
| **14** | Tools: `cas-fsck`, `cas-inspect`, `cas-gc-dryrun` | 9, 650 | 700 | high | `ca-surgeon` / `ca-review` | yes |
| **15** | New pools are created switched | 3, 60 | ±700 | high | `ca-surgeon` / `ca-review` | |
| **16** | Remove the writer's marker path | 3, −350 | ±900 | high | `ca-surgeon` / `ca-review` | yes |
| 17 | `SYSTEM CAS ENABLE BLOB INTENTS` | 7, 120 | 60 | low | `ca-impl` / `ca-review-lite` | |
| 18 | Transition gtests | 0 | 800 | medium | `ca-impl` / `ca-review` | yes |
| 19 | Integration and stateless tests | 1, 30 | 900 | medium | `ca-impl` / `ca-review-lite` | |
| 20 | Docs | 16, 700 | — | low | `ca-impl` / `ca-review-lite` | |
| 21 | Yardstick references and soak cards | 10, 300 | — | low | `ca-impl` / `ca-review-lite` | |

Pairs of tasks that share a file or an interface: what the first produces against what the second consumes.

| Producer → consumer | Interface or file |
|---|---|
| 1 → 3, 5, 10, 11, 14 | `CanonicalIdentity`, `canonicalIdentity`, `Etag::canonical`, `PersistedEtag::canonical`, `sameIncarnation`, `dialectWord`, `dialectFromWord`, `opaqueTagFromHex` (`CA/Backend/CasEtag.h`) |
| 2 → 3 | `G_BLOB_INTENTS`, `G_BUILD = 4`: the intent record has header 4 |
| 2 → 6, 9, 12, 13, 14, 15 | `BlobProtocol`, `PoolMeta::blob_protocol`, `token_dialect`, `created_generation` |
| 2 → 8, 12, 13 | `PoolMeta::switchToBlobIntents` → `BlobIntentsSwitch { meta, written }` |
| 2 → 9, 10, 12 | `GcState::generation`; `RetiredEntry::intent`, `CondemnedRow::intent`, `SourceEdgeRecord::intent`, the run header version |
| 2 → 6, 12 | `MountLease::protocol_generation` (read through `MountInfo::lease`); `ServerEpoch::protocol_generation` |
| 3 → 10, 11, 14 | `Layout::blobIntentKey`, `writeOnceBlobIntentKey`, `parseBlobIntentKey`, `blobIntentsPrefix`, `blobIntentRecord`, `isBlobIntentRecord` |
| 3 → 6, 12, 13 | `Layout::maxPoolPrefixBytesForBlobIntents` |
| 4 → 5, 7 | `WriteSettings::object_storage_content_type`, `ObjectMetadata::content_type`; the headers of `CountingS3Server` |
| 5 → 6, 12, 13 | `BlobMarkProbeResult`, `runBlobMarkProbe` |
| 5 → 7, 10, 11, 14 | `Meta::marked`, `BlobPublishRequest::marked`, `kBlobMarkContentType` |
| 5, 7 share | `B:` `publish` and `emuPublishBlobAtomically`; `CA/Backend/CasBackend.h:54-78`; `CA/Backend/CasInMemoryBackend.cpp:194-240`; `CA/Backend/CasProbe.cpp` |
| 6 → 7, 8, 11, 12, 13, 15, 16 | `Pool::blobIntentsLearned`, `applyBlobIntentsObservation`, `refreshBlobIntentsLearning`, `noteMarkedBodyWhileNotLearned`, `blobMarkProbe`, `observedBlobProtocol` |
| 6, 13 share | `CA/Pool/CasPool.cpp` `Pool::open` (`:440-611`) |
| 7 → 8, 11 | `StreamingBlobPublication::expected_ref`, `BlobSource::marked`, `PendingBlob::marked`, `stageBlobPartFile(..., bool marked)` |
| 7, 11, 15, 16 share | `W:274-570` (`ensureBlobPresent`) |
| 8 → 9 to 18 | `T/cas_blob_intent_test_support.h`: `seedUpgradedPoolForTest`, `seedNewPoolAtN2ForTest`, `openSwitchedPoolForTest`, `publishBlobRawForTest`, `ProtocolJournalBackend`, `DialectOverrideBackend` |
| 9 → 10 | the round's `BlobProtocol`; Task 10 replaces the `NOT_IMPLEMENTED` of intent mode with the fold |
| 9 → 14 | `Gc::readRoundBlobProtocol(op, leased_state)`: consumed by `previewDeletes` only. `runFsck` has no `Gc` and makes its own read |
| 9, 12 share | `Gc::acquireOrRenewLease` (`G:5185`): Task 12 adds the owner-0 branch |
| 10 internal | `GraduationReadHints::admit(ref, token)`: its callers in `T/gtest_cas_blob_indegree.cpp:1182`, `:1259` |
| 10, 14 share | `G:5061` (`previewDeletes`); `CA/Tools/CasInspect.cpp:413-418` reads `CondemnedRow::intent` |
| 11 → 15, 16 | `BlobPublicationReason`, `PartWriteTxn::ensureBlobPresentByIntents` |
| 12 → 13, 17, 18 | `Pool::enableBlobIntents`, `EnableBlobIntentsResult`; Task 13 reuses `switchToBlobIntentsAndLearn` |
| 14 → 17, 20 | `FsckReport` classes, `FsckFix`, `runFsck(..., FsckFix)`; the `SYSTEM CAS FSCK` columns |
| 14, 16 share | `T/gtest_cas_fsck.cpp` |
| 15 → 16 | the suites pinned to `seedUpgradedPoolForTest`; the "saw the switch, not learned" branch of `ensureBlobPresent` |
| 15, 16 → 19, 21 | the request ledger of a pool created by this build |

---

### Task 1: Token identity {#task-1}

Implementer `ca-impl`. Reviewer `ca-review`. Read §6.1. No behaviour changes: nothing calls the new functions yet.

**Files:**
- Modify: `CA/Backend/CasEtag.h:10-14` (grammar comment: two levels), `:24-51` (`Etag`), `:69-88` (`PersistedEtag`)
- Modify: `CA/Backend/CasEtag.cpp:10-32` (`isIncarnationValue` stays byte for byte; new functions after it)
- Create: `T/gtest_cas_token_identity.cpp`

**Interfaces:**
- Consumes: `enum class Dialect { ETag = 1, Generation = 2, Emulated = 3 }` (`CA/Primitives/CasTypes.h:269`);
  the words of `Etag::render` (`CA/Backend/CasEtag.h:53-62`).
- Produces, all in `CA/Backend/CasEtag.h`:

  ```cpp
  struct CanonicalIdentity
  {
      Dialect dialect;
      String tag_hex;        /// lowercase hex of the opaque tag
      bool operator==(const CanonicalIdentity &) const = default;
  };
  inline constexpr size_t kCanonicalTagMaxBytes = 128;          /// etag, emulated
  inline constexpr size_t kCanonicalGenerationMaxDigits = 20;
  std::string_view dialectWord(Dialect dialect);                /// "etag" | "generation" | "emulated"
  std::optional<Dialect> dialectFromWord(std::string_view word);
  std::optional<CanonicalIdentity> canonicalIdentity(Dialect dialect, std::string_view raw_value);
  /// Inverse of `tag_hex`, for key parsing: the opaque tag, or nothing unless `tag_hex` is lowercase,
  /// of even length, and decodes to a tag the grammar accepts for `dialect`.
  std::optional<String> opaqueTagFromHex(Dialect dialect, std::string_view tag_hex);
  std::optional<CanonicalIdentity> Etag::canonical() const;
  std::optional<CanonicalIdentity> PersistedEtag::canonical() const;   /// unknown dialect word: nothing
  bool sameIncarnation(const PersistedEtag & row, const Etag & live);
  bool sameIncarnation(const Etag & a, const Etag & b);
  ```

- Rules the signatures do not state:
  - `etag` and `emulated`: the opaque tag is the raw value with one pair of double quotes removed, when the value
    has at least two bytes and both its first and last byte are `"`. Rejected (no identity): an empty value or an
    empty tag; a value that starts with `W/`; any `"` left after the strip; a byte below `0x20` or equal to `0x7F`;
    a first or last byte `0x20`; the tag `*`; a comma; a tag over 128 bytes. No case folding.
  - `generation`: `[1-9][0-9]{0,19}` and nothing else, no quote strip (the backend already strips,
    `CA/Backend/CasObjectStorageBackend.h:175-182`). The tag is the digit string.
  - `sameIncarnation` is true only when both sides have an identity and the identities are equal. Two equal raw
    values that do not canonicalise are not the same incarnation.
  - `PersistedEtag::matches` (`:85-88`) stays: marker mode keeps the textual compare.

- [ ] **Step 1: Write the failing tests,** suite `CASTokenIdentity`. A live `Etag` comes from a file-local
  `FixedTokenBackend(Dialect, String value)` (an `InMemoryBackend` whose `dialect` and `head` value are fixed).
  - `QuotedAndBareSpellingsAreOneIdentity`: `"\"abc\""` and `abc` under `ETag` give `{ETag, "616263"}`.
  - `CaseIsNotFolded`: `ABC` and `abc` differ.
  - `RejectedEtagForms`: each of `""`, `"\"\""`, `W/"abc"`, `"abc`, `abc"`, `a"b`, `"a"b"`, ` abc`, `abc `, `a\tb`,
    `a\x7f`, `*`, `"*"`, `a,b`, a 129-byte tag gives nothing; a 128-byte tag is accepted.
  - `GenerationGrammar`: accepted `1`, `18446744073709551615`, a 20-digit value; rejected `0`, `01`, the empty
    value, `1a`, `"1"`, a 21-digit value.
  - `EmulatedUsesTheEtagGrammar`.
  - `SameIncarnationComparesCanonically`: persisted `{etag, "\"abc\""}` against live `abc` is true; another dialect
    is false; `W/"x"` against `W/"x"` is false.
  - `PersistedEtagWithAnUnknownDialectWordHasNoIdentity`.
  - `OpaqueTagFromHexRoundTrips`: also rejects uppercase hex, odd length, and hex of a rejected tag.
  - `ControlKeyGrammarIsUnchanged` (pin): `isIncarnationValue(ETag, "W/\"x\"")` is true, `(ETag, "a,b")` false,
    `(Generation, "7")` true, `(Emulated, "x y")` true.
  - `EveryTestBackendTokenCanonicalises` (pin after Step 3): a `HEAD` token of `InMemoryBackend` and of
    `ObjectStorageBackend` over `makeLocalObjectStorageForTest` (`T/cas_test_helpers.h:439`).
- [ ] **Step 2: Run red.** Add the declarations with stubs: `canonicalIdentity` returns nothing,
  `sameIncarnation` compares `render` text. Filter `CASTokenIdentity*`. Expected: the accept cases and
  `SameIncarnationComparesCanonically` fail on assertions.
- [ ] **Step 3: Implement.** Correct the comment at `CA/Backend/CasEtag.h:10-14`: it describes the first level;
  the tight grammar applies where an identity is used.
- [ ] **Step 4: Run green,** then `CAS*` on `build` (tag `t1_pre`). No existing test changes.
- [ ] **Step 5: One mutant,** tag `t1_m1`: the quote strip removes an unmatched leading quote. Expected:
  `RejectedEtagForms` fails.
- [ ] **Step 6: Commit,** then the gate. Files: the two product files and the test. Subject:
  `Add a canonical token identity for CAS incarnation compares`

### Task 2: Control formats, generation 4 {#task-2}

Implementer `ca-surgeon`. Reviewer `ca-review`. Read §2.3, §2.4, §3.6. After this task a writable mount raises a
pool to floor 4 and a `gc/state` this build creates has header 4; nothing reads the new fields for a decision.

**Files:**
- Modify: `CA/Formats/CasFormat.h:16` (`G_BUILD`), new `G_BLOB_INTENTS`; `CA/Formats/CasFormat.cpp:47-70`
  (change points of `GcState` and `RunFile`: append `{4, 4}`)
- Modify: `CA/Pool/CasPool.h:450` (`mount_reader_floor = G_BLOB_INTENTS`)
- Modify: `CA/Formats/CasPoolMetaFormat.h` (`PoolMeta`), `CA/Formats/CasPoolMetaFormat.cpp:72-96` (encode),
  `:98-161` (decode); `CA/Pool/CasPoolMeta.cpp:113-123` (new function after `raiseMinReaderGeneration`)
- Modify: `CA/Formats/CasGcStateFormat.h:31-40`, `CA/Formats/CasGcStateFormat.cpp:38-56` (`:42` header), `:58-`
- Modify: `CA/Formats/CasRecordStreamFormat.h:84-94`, `:121-127`; `CA/Formats/CasRecordStreamFormat.cpp:145-160`,
  `:198-205`, `:280-311`
- Modify: `CA/Gc/CasBlobInDegree.h:27-44`, `:78-96`; `N:262-290` (packed row), `:320-327` (run view), `:490-498`
  (`toRetiredEntry`), `:655-664` (run writer append)
- Modify: `CA/Formats/CasServerRootFormats.h:53-60` (`ServerEpoch`), `:66-96` (`MountLease`, `fencedCopy`);
  `CA/Formats/CasServerRootFormats.cpp:112-152` (the epoch codec), `:155-225` (the mount-lease codec)
- Modify: `CA/Pool/CasServerRoot.h:220`, `CA/Pool/CasServerRoot.cpp:817` (`allocateWriterEpoch`; callers
  `CA/Pool/CasPool.cpp:704`, `:827`, `:890`, `:1589` are not edited), `:658-664` (the rejoin body), `:912-926`
  (`makeMountBody`), `:1704-1722` (`MountLeaseRenewer::encodeBody`), `:2187-2190` (`MountLeaseRenewer::terminate`)
- Test: `T/gtest_cas_pool_meta.cpp`, `T/gtest_cas_gc_state_format.cpp`, `T/gtest_cas_record_stream_format.cpp`,
  `T/gtest_cas_blob_indegree.cpp`, `T/gtest_cas_server_root_format.cpp`, `T/gtest_cas_gc_round.cpp`,
  `T/gtest_cas_mount.cpp`, `T/gtest_cas_format.cpp`, `T/gtest_cas_format_battery.cpp`,
  `T/cas_format_test_battery.h`, and every test file that Step 4 edits (the commit lists each by name)

**Interfaces:**
- Consumes (existing): `rmwPoolMeta` (`CA/Pool/CasPoolMeta.cpp:69-86`); `writeHeaderLine(out, id, version)`
  (`CA/Formats/CasTextFormat.h:153`); `JsonObjectReader::skipUnknown`, which throws on a key that starts with `!`
  (`CA/Formats/CasTextFormat.cpp:315-322`); `checkCompatibility` (`CA/Formats/CasFormat.cpp:102-109`).
- Produces:

  ```cpp
  /// CA/Formats/CasFormat.h
  constexpr uint32_t G_BUILD = 4;
  constexpr uint32_t G_BLOB_INTENTS = 4;
  /// CA/Formats/CasPoolMetaFormat.h
  enum class BlobProtocol : uint8_t { Markers = 1, Intents = 2 };
  struct PoolMeta
  {
      /* existing fields */
      BlobProtocol blob_protocol = BlobProtocol::Markers;   /// wire key absent = Markers; "intents"
      std::optional<Dialect> token_dialect;                 /// present iff Intents
      std::optional<uint64_t> created_generation;           /// 4, or absent for a pool an older build created
      bool switched() const { return blob_protocol == BlobProtocol::Intents; }
      /// Monotone. No `blob_protocol`: writes `intents` and `backend_dialect` in one write. `intents` with the
      /// same dialect: no write. `intents` with another dialect: BAD_ARGUMENTS, the dialect text of §7.5.
      /// `meta` is the body as last read, so an unanswered write is settled by the read of the retry.
      /// `written` is true iff the last decision of this call was a write and that write was answered.
      static BlobIntentsSwitch switchToBlobIntents(CasOperation & op, const Layout & layout, Dialect backend_dialect);
  };
  struct BlobIntentsSwitch { PoolMeta meta; bool written = false; };
  /// CA/Formats/CasGcStateFormat.h, in GcState
  uint32_t generation = G_BLOB_INTENTS;     /// the header `v`; a decode is the only source of a lower value
  /// CA/Formats/CasRecordStreamFormat.h
  struct SourceEdgeRecord { /* existing */ bool intent = false; };
  explicit SourceEdgeRunWriter(WriteBuffer & out_, uint32_t header_version = currentCompatibilityVersion());
  uint32_t SourceEdgeRunReader::headerVersion() const;
  /// CA/Gc/CasBlobInDegree.h
  struct RetiredEntry { /* existing */ bool intent = false; };
  struct CondemnedRow { /* existing */ bool intent = false; };   /// packed flag value 4; in operator== too
  /// CA/Formats/CasServerRootFormats.h
  struct MountLease { /* existing */ uint64_t protocol_generation = 0; };       /// what the body declares; 0 = no key
  struct ServerEpoch { /* existing */ std::optional<uint64_t> protocol_generation; };
  ```

- Rules the signatures do not state:
  - `_pool_meta` wire keys `blob_protocol`, `token_dialect`, `created_generation`, written after `algos_used`, in
    that order, each only when set. Decode failures exactly as the §3.6 table: unknown `blob_protocol` word
    `UNKNOWN_FORMAT_VERSION` with the §7.5 text; `intents` without `token_dialect`, or `token_dialect` without
    `intents`, `CORRUPTED_DATA`; unknown dialect word `UNKNOWN_FORMAT_VERSION`; `created_generation` below 4
    `CORRUPTED_DATA`, above 4 through `checkCompatibility`; both `created_generation` checks run after the floor
    check (`CA/Formats/CasPoolMetaFormat.cpp:155-158`). `createOrValidate` does not write `created_generation`
    yet (Task 15 does).
  - `encodeGcState` writes `writeHeaderLine(out, FormatId::GcState, state.generation)`. `decodeGcState` sets
    `generation` from the header. Every rewrite keeps what it read: the renew (`G:5220-5226`), the steal
    (`G:5263-5268`), the round commit (`G:1300`). The bootstrap (`G:5204-5211`) constructs, so it writes 4.
  - A run: the writer stamps `header_version` and writes the key `intent` for every condemned record iff the
    header is 4. A record with `intent = true` given to a writer below header 4 is `LOGICAL_ERROR`. The reader:
    header below 4 and the key present, header 4 and the key missing on a condemned record, the key on a
    non-condemned record, `intent` without `pending`: `CORRUPTED_DATA`. Header below 4 reads `intent = false`.
  - Packed row: flag 4 is `intent`; `flags > 7`, or 4 without 1: `CORRUPTED_DATA` (`N:266`, `:287`). `intent`
    rides `SourceEdgeRunView::next` (`N:320-327`), `toRetiredEntry` (`N:490-498`) and the append at `N:661-664`.
  - Mount slot: the slot holder writes its own generation, and a fence never raises another server's
    ([Spec questions](#spec-questions), Q3). `encodeMountLease` writes the key `protocol_generation` iff the
    field is not 0; a decode sets the field from the key, 0 when it is absent. Every body its holder builds sets
    `G_BLOB_INTENTS`: `makeMountBody`, `MountLeaseRenewer::encodeBody`, `MountLeaseRenewer::terminate`, the
    rejoin body. `MountLease::fencedCopy` keeps the decoded value, as it keeps `write_attempt_id`: a GC fence
    over an older server's slot still declares 0, and the switch command still names that root. A holder site
    that forgets the field declares 0, which blocks the command and deletes nothing.
  - Epoch counter: `allocateWriterEpoch` writes the critical key `!protocol_generation` = 4 with each allocation.
    The decoder reads it and passes the value to `checkCompatibility` under the name `server-epoch`.
  - What an older build does with each field: the §2.4 table, row by row. Quote the row in the code comment of
    each field as a reason, not as a reference.

- [ ] **Step 1: Write the failing tests.**
  - `CASPoolMeta`: `BlobProtocolFieldsRoundTrip`; `UnknownBlobProtocolWordFailsTheDecode` (code and the §7.5 text);
    `IntentsWithoutDialectIsCorrupted`; `DialectWithoutIntentsIsCorrupted`; `UnknownDialectWordFailsTheDecode`;
    `CreatedGenerationBelowFourIsCorrupted`; `CreatedGenerationAboveFourIsRefusedAsNewer`;
    `FloorCheckRefusesBeforeCreatedGeneration` (`ScopedReadCeiling(3)`: the floor text wins);
    `NewFieldsSurviveAdmitOrValidate`; `NewFieldsSurviveRaiseMinReaderGeneration`;
    `SwitchWritesIntentsAndDialectInOneWrite`; `SwitchIsANoOpOnTheSameDialect` (no write in the journal);
    `ReadCeilingThreeRejectsFloorFour` (text of §7.5, first row).
  - `CASBlobIntentDialect.SwitchWithAnotherRecordedDialectIsRefused`: `BAD_ARGUMENTS`, the dialect text, no write.
  - `CASGCStateFormat`: `ConstructedStateEncodesHeaderFour`; `DecodedGenerationIsKeptByTheEncoder` (headers 1, 3,
    4); `ReadCeilingThreeRejectsHeaderFour` (text of §7.5, second row).
  - `CASGCRound.LeaseAndCommitKeepTheGcStateGeneration`: a seeded header-1 `gc/state` is still header 1 after a
    renew and a committed round; an absent `gc/state` is created with header 4.
  - `CASRecordStream`: `IntentRowRoundTripsAtHeaderFour`; `IntentKeyBelowHeaderFourIsCorrupted`;
    `MissingIntentAtHeaderFourIsCorrupted`; `IntentWithoutPendingIsCorrupted`;
    `IntentOnANonCondemnedRecordIsCorrupted`; `HeaderBelowFourReadsIntentFalse`;
    `ReadCeilingThreeRejectsARunAtHeaderFour`; the split pair `WriterBelowHeaderFourRefusesAnIntentRow` and
    `CASRecordStreamDeathTest.WriterBelowHeaderFourRefusesAnIntentRow`.
  - `CASBlobInDegree`: `PackedFlagFourRoundTrips`; `PackedFlagsAboveSevenAreCorrupted`;
    `PackedIntentWithoutPendingIsCorrupted`; `RunViewCarriesIntent`.
  - `CASMountLeaseFormat`: `ProtocolGenerationRoundTrips`; `BodyWithoutTheKeyDecodesZero`;
    `ZeroEncodesNoKey`; `FencedCopyKeepsTheDeclaredGeneration` (0 stays 0, 4 stays 4).
  - `CASServerEpochFormat`: `EpochCarriesTheCriticalProtocolKey` (the encoded body contains
    `"!protocol_generation"`); `ProtocolGenerationAboveTheCeilingIsRefused`; `UnknownCriticalKeyFailsTheDecode`
    (pin: a body with a key `!x` that this build does not know fails with the critical-key text of §7.5).
  - `CASMount`: `HolderBodiesDeclareProtocolGenerationFour` (the slot after the claim, after one renew and after
    the farewell); `GcFenceOverAnOlderSlotKeepsItsDeclaration` (a slot written without the key, then fenced by
    the heartbeat-floor pass: the fenced body has no key).
  - The other dialect in these tests is an argument: `switchToBlobIntents(op, layout, Dialect::ETag)` on an
    `InMemoryBackend`. No second backend is needed in this task.
- [ ] **Step 2: Run red.** Add the fields and the constructor parameter first; encoders and decoders unchanged.
  Filter `CASPoolMeta*:CASGCStateFormat*:CASGCRound*:CASRecordStream*:CASBlobInDegree*:CASMountLeaseFormat*:CASServerEpochFormat*:CASMount.*:CASBlobIntentDialect*`.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run green,** then `CAS*` on `build` (tag `t2_pre`). Existing tests that pin generation 3: 127
  lines in 26 files; find them with
  `grep -n 'min_reader_generation\|G_BUILD\|mount_reader_floor\|G_REF_CHAIN\|ScopedReadCeiling' $T/gtest_cas_*.cpp $T/cas_*.h`.
  - A literal 3 that means "this build" or "the mount floor", and a pinned `gc/state` header byte: replace it by
    the named constant or by 4.
  - `ScopedReadCeiling c(3)`, 51 scopes: a no-op today (`G_BUILD` is 3, `CA/Formats/CasFormat.h:16`) and a
    lowered ceiling under `G_BUILD = 4`. A test that opens a floor-4 pool or decodes a header-4 object inside
    one is then refused (`CASGCRebuild.RebuildFollowsASkippedEpoch`, `T/gtest_cas_gc_rebuild.cpp:758`). Drop
    the scope, or pass `G_BUILD`, unless the test means an older reader.
  - `ScopedReadCeiling c(2)` scopes mean an older reader: leave them.

  Do not change what a test proves. List each edit in the report. Any other red test is a stop.
- [ ] **Step 5: Three mutants,** tags `t2_m1` to `t2_m3`: (1) `encodeGcState` writes `G_BLOB_INTENTS` always:
  expect `DecodedGenerationIsKeptByTheEncoder` and `LeaseAndCommitKeepTheGcStateGeneration` to fail;
  (2) `toRetiredEntry` drops `intent`: expect `RunViewCarriesIntent` or a round-trip test to fail. If (2)
  passes, add the missing carrier test first; (3) `fencedCopy` sets `protocol_generation = G_BLOB_INTENTS`:
  expect `FencedCopyKeepsTheDeclaredGeneration` and `GcFenceOverAnOlderSlotKeepsItsDeclaration` to fail.
- [ ] **Step 6:** Run the filter of Step 2 plus `CASRecordStreamDeathTest*` on `build_asan` (tag `t2_asan`).
- [ ] **Step 7: Commit,** then the gate. Subject: `Add generation 4 control fields for CAS blob intents`

### Task 3: Intent object and key class {#task-3}

Implementer `ca-impl`. Reviewer `ca-review-lite`. Read §2.1. Nothing creates an intent yet.

**Files:**
- Modify: `CA/Formats/CasFormat.h:30-66` (`FormatId::BlobIntent = 26`), `CA/Formats/CasFormat.cpp:47-70` (change
  points `{4, 4}`), `:132-160` (`TRAITS` row)
- Create: `CA/Formats/CasBlobIntentFormat.h`, `CA/Formats/CasBlobIntentFormat.cpp`
- Modify: `CA/Formats/CasLayout.h:118-131`, `:415`; `CA/Formats/CasLayout.cpp:35-45`
- Modify: `CA/Primitives/CasWriteOnceKey.h:10-14` and `CA/Backend/CasBackend.h:197-202` (contract comments),
  `CA/Backend/CasBackend.h:135-136` (the "never reused" sentence holds for the in-memory backend only)
- Modify: `CA/Backend/CasInstrumentedBackend.h:27-36`, `CA/Backend/CasInstrumentedBackend.cpp:107-136`;
  `src/Common/ProfileEvents.cpp` (four counters, near `:833`)
- Test: `T/gtest_cas_layout.cpp`, `T/gtest_cas_format_battery.cpp`, `T/cas_format_test_battery.h`,
  `T/gtest_cas_write_once_key.cpp`, `T/gtest_cas_observability.cpp`

**Interfaces:**
- Consumes (Task 1): `CanonicalIdentity`, `dialectWord`, `dialectFromWord`, `opaqueTagFromHex`. (Task 2):
  `G_BLOB_INTENTS`. Existing: `Layout::blobKey` (`CA/Formats/CasLayout.cpp:35-38`), `WriteOnceKey`
  (minted only by `Layout`), `CasOperation::removeManyWriteOnce` (`Q:634`).
- Produces:

  ```cpp
  /// CA/Formats/CasLayout.h
  static constexpr size_t kBlobIntentKeyMaxBytes = 1024;
  static constexpr size_t kBlobIntentKeySuffixMaxBytes = 356;      /// "/blob-intents/..." after the pool prefix
  static constexpr size_t maxPoolPrefixBytesForBlobIntents() { return kBlobIntentKeyMaxBytes - kBlobIntentKeySuffixMaxBytes; }
  String blobIntentsPrefix() const;                                /// "<prefix>/blob-intents/"
  String blobIntentKey(const BlobRef & ref, const CanonicalIdentity & identity) const;
  WriteOnceKey writeOnceBlobIntentKey(const BlobRef & ref, const CanonicalIdentity & identity) const;
  struct ParsedBlobIntentKey { BlobRef ref; CanonicalIdentity identity; };
  std::optional<ParsedBlobIntentKey> parseBlobIntentKey(std::string_view key) const;
  /// CA/Formats/CasBlobIntentFormat.h
  const String & blobIntentRecord();                 /// the bytes  {"type":"cas_blob_intent","v":4}\n{}\n
  bool isBlobIntentRecord(std::string_view bytes);   /// byte equality with the record, nothing looser
  /// CA/Backend/CasInstrumentedBackend.h
  enum class CasNs : uint8_t { Blob = 0, Manifest, Root, Gc, Server, Other, BlobIntent };   /// CAS_NS_COUNT = 7
  ```

- Rules the signatures do not state:
  - Key: `<prefix>/blob-intents/<algo>/<hex[0:2]>/<hex>/<dialect word>/<tag_hex>`: the first four segments as
    `blobKey` builds them, with `blob-intents` for `blobs`. Over 1024 bytes: `LOGICAL_ERROR`.
  - `parseBlobIntentKey` accepts only what `blobIntentKey` produces (the list in §2.1 "Parse").
    `parseBlobKey` (`CA/Formats/CasLayout.cpp:45`) keeps rejecting such a key.
  - `TRAITS` row: `{FormatId::BlobIntent, "cas_blob_intent", TextFamily::Control, CompressionPolicy::PinnedRaw, 4 * kKiB, 1 * kKiB}`.
  - `classifyCasNs` tests `/blob-intents/` first. Counters `CASBlobIntentGet`, `CASBlobIntentPut`,
    `CASBlobIntentDelete`, `CASBlobIntentList`: the row maps `Put` and `PutDeduplicated` to `CASBlobIntentPut`,
    `Get` and `GetStream` to `CASBlobIntentGet`, `Delete` to `CASBlobIntentDelete`, `List` to `CASBlobIntentList`,
    and the five operations this protocol never sends on an intent key to their `CASOther*` counters
    ([Spec questions](#spec-questions), Q4).
  - Comments: the `WriteOnceKey` contract widens to "one byte string in every life of the key, and a delete that
    stays right for every life" (§2.1 "One body, many lives").

- [ ] **Step 1: Write the failing tests.**
  - `CASBlobIntentKey`: `KeyMirrorsTheBlobKeyAndAppendsTheIdentity` (a literal expected key for a fixed ref and
    `{ETag, "616263"}`); `ParseRoundTrips` (each dialect); `ParseRejectsEachForeignSpelling` (unknown algo word;
    shard other than `hex[0:2]`; unknown dialect word; uppercase tag hex; odd length; a tag the grammar rejects;
    one segment more; one segment less; a `.meta` suffix); `BlobKeyParserRejectsAnIntentKey` (pin);
    `LongestKeyIs356BytesAfterThePrefix`; `MaxPoolPrefixIs668Bytes`; the split pair
    `KeyOver1024BytesIsALogicalError` and `CASBlobIntentKeyDeathTest.KeyOver1024BytesIsALogicalError`.
  - `CASBlobIntentFormat`: `RecordIsTheLiteralBytes`; `OnlyTheLiteralBytesAreARecord` (one byte more, `"v":3`, an
    extra key: false); the battery row; `ReadCeilingThreeRejectsTheRecordHeader`.
  - `CASBlobIntentKey.WriteOnceKeysDeleteInOneRequest`: on `BatchCapabilityBackend`, five intent keys, one of
    them absent: one request, no exception.
  - `CASObservability.IntentRequestsCountInTheirOwnClass`: a `GET`, a create, a delete and a list on an intent
    key move only the four new counters; `CASBlobGet`, `CASBlobPut`, `CASBlobDelete` do not move.
  - Pins: `isBlobBodyKey` (`T/gtest_cas_s3_staging.cpp`, `T/gtest_cas_part_write.cpp:2851`) and `isMarker`
    (`T/cas_gc_marker_test_support.h`) are false for an intent key. If one is true, fix the predicate.
- [ ] **Step 2: Run red.** Add the declarations with stubs (`parseBlobIntentKey` returns nothing,
  `classifyCasNs` unchanged). Filter `CASBlobIntentKey*:CASBlobIntentFormat*:CASObservability*`.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run green,** then `CAS*` on `build` (tag `t3_pre`). No existing test changes.
- [ ] **Step 5: Commit,** then the gate and the ASan milestone. Subject:
  `Add the CAS blob intent object, its key layout and request class`

### Task 4: Shared S3 code: an optional content type on write, copy and `HEAD` {#task-4}

Implementer `ca-surgeon`. Reviewer `ca-review`. Read §6.2 ("Plumbing" rows 6 to 9). This task holds no CAS
logic and no CAS name. It is rows 1 to 5 and 9 of [Shared code changes](#shared-code) and nothing else.

**Files:**
- Modify: `src/IO/WriteSettings.h:105` (new field after it); `src/IO/WriteBufferFromS3.cpp:442`, `:775`
- Modify: `src/IO/S3/copyS3File.h:48-63`, `src/IO/S3/copyS3File.cpp:140-150`, `:742-750`, `:1011-1030` (thread the
  parameter into the copy helper); `S3:1122-1137`
- Modify: `src/IO/S3/getObjectInfo.h:14-23`, `src/IO/S3/getObjectInfo.cpp:87-93`;
  `src/Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h:128-142`; `S3:968-975`, `:1009-1014`
- Modify (test-only): `src/IO/S3/tests/CountingS3Server.h:85-94` (`Request` gains
  `Poco::Net::NameValueCollection headers`, filled where the handler builds the request, `:116-121`),
  `:270-345` (`ObjectStore` keeps the `Content-Type` of a `PutObject` per key and sends it on `HeadObject`;
  an object seeded into the store without a type sends no header)
- Test: `T/gtest_s3_object_creation_requests.cpp`, over `T/S3ObjectCreationTestStorage.h` and the
  `CountingS3Server` above

**Interfaces:**
- Produces:

  ```cpp
  /// src/IO/WriteSettings.h
  /// The Content-Type of the object an S3 write or copy creates; empty keeps `binary/octet-stream`.
  String object_storage_content_type;
  /// src/IO/S3/copyS3File.h, last parameter of copyS3File
  const String & content_type = {});
  /// src/IO/S3/getObjectInfo.h (ObjectInfo) and IObjectStorage.h (ObjectMetadata)
  String content_type;      /// from the HEAD response; empty when the store sent none
  ```

- Rules: the four requests that define an object's type use the field: `PutObject` (`WriteBufferFromS3.cpp:775`),
  `CreateMultipartUpload` (`:442`), `CopyObject` (`copyS3File.cpp:748`), the multipart-copy create (`:148`).
  `UploadPart` (`WriteBufferFromS3.cpp:546`, `copyS3File.cpp:596`) and the buffered-copy `PutObject`
  (`copyS3File.cpp:506`) keep `binary/octet-stream`. The multipart create at `:148` is in the upload helper
  that `copyDataToS3File` shares (`:439-454`, `:908`): that caller passes `{}`, so its bytes do not change.
  The copy keeps the default metadata directive: no
  `object_metadata` is added. `copyS3FileRange` and every other caller of `copyS3File`
  (`src/Backups/BackupIO_S3.cpp:506`, `:656`, `:702`, `S3:1043`,
  `src/Storages/ObjectStorageQueue/ObjectStorageQueuePostProcessor.cpp:367`) are not edited.

- [ ] **Step 1: Write the failing tests,** suite `S3ObjectContentType`, each asserting the `Content-Type` header of
  the recorded wire request (`Request::headers`). Extend `CountingS3Server` first; its existing users must
  build and pass unchanged:
  - `DefaultWriteKeepsBinaryOctetStream`, `DefaultCopyKeepsBinaryOctetStream`, `UploadPartKeepsBinaryOctetStream`
    (pins).
  - `PutObjectCarriesTheSetType`, `MultipartCreateCarriesTheSetType`, `CopyObjectCarriesTheSetType`,
    `MultipartCopyCreateCarriesTheSetType` (force the multipart paths as `S3SingleAttemptMultipartCreationPath`
    and `CopyRefusedAsTooLargeBecomesOneMultipartCopy` in the same file do, `:180`, `:356`).
  - `HeadReturnsTheContentType`, `HeadWithoutTheHeaderReturnsEmpty` (through `tryGetObjectMetadataWithNativeToken`).
- [ ] **Step 2: Run red.** Add the field and the parameter, unused. Filter `S3ObjectContentType*`.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run green,** then `--gtest_filter='S3*:WBS3*'` on `build` and `build_asan` (tag `t4_s3`), then
  `CAS*` on `build` (tag `t4_pre`). No existing test changes.
- [ ] **Step 5: Commit,** then the gate. The body says: one optional field, default bytes unchanged, pinned by
  three tests; the test server records headers. Subject:
  `Let an S3 write, copy and HEAD carry an object content type`

### Task 5: Backend mark and the mark probe {#task-5}

Implementer `ca-impl`. Reviewer `ca-review`. Read §2.2, §6.2. After this task a `HEAD` reports the mark and a
publication can set it; every publication still passes `marked = false`.

**Files:**
- Modify: `CA/Backend/CasBackend.h:74-78` (`BlobPublishRequest`), `:161` (`RawMeta`), new pure virtual near `:266`;
  `CA/Backend/CasWriteResult.h:29` (`Meta`); `Q:549` (`headUnder` copies the mark)
- Modify: `B:230-238` (`nativeHead`), `:505-560` (`emuPublishBlobAtomically`), `:732-749` (`headUnder`),
  `:894-900` (`blobPublicationWriteSettings`), `:902-973` (`publish`); `CA/Backend/CasObjectStorageBackend.h`
- Modify: `CA/Backend/CasInMemoryBackend.h:205` (`Object`), `CA/Backend/CasInMemoryBackend.cpp:99-109`, `:194-240`
- Modify: `CA/Backend/CasInstrumentedBackend.h`, `CA/Backend/CasThrottlingBackend.h` (forward the predicate)
- Modify: `CA/Backend/CasProbe.h`, `CA/Backend/CasProbe.cpp` (new function after `runCapabilityProbe`, `:47`)
- Test: create `T/gtest_cas_blob_mark.cpp` and `T/cas_blob_mark_test_support.h` (`MarkLiarBackend`: an
  `InMemoryBackend` whose `HEAD` misreports the mark, or returns a token outside the grammar, for the one
  probe row it is given; Tasks 6, 12 and 13 use it); modify the ten test backends that derive from `Backend` directly
  (`T/gtest_cas_decommission.cpp:973`; `T/gtest_cas_part_write.cpp:186`, `:232`, `:983`, `:1077`, `:1672`;
  `T/gtest_cas_pool.cpp:73` `WriteCountingBackend`, `:250` `ProbeWatchingBackend`, `:338` `ForwardingBackend`,
  `:1510` `FenceInAdoptWindowBackend`) and the generation fakes (`T/gtest_cas_backend_generation.cpp:101`,
  `:429`). The native tests run an `ObjectStorageBackend` over the `S3ObjectStorage` of
  `T/S3ObjectCreationTestStorage.h` against `CountingS3Server` (`src/IO/S3/tests/CountingS3Server.h`, with the
  headers Task 4 added)

**Interfaces:**
- Consumes (Task 4): `WriteSettings::object_storage_content_type`, `ObjectMetadata::content_type`. (Task 1):
  `Etag::canonical`.
- Produces:

  ```cpp
  /// CA/Backend/CasBackend.h
  inline constexpr std::string_view kBlobMarkContentType = "application/x-clickhouse-cas-blob-v2";
  struct BlobPublishRequest { String destination_key; BlobPublication publication; bool marked = false; };
  struct RawMeta { uint64_t size; String value; bool marked = false; };
  virtual bool supportsVerbatimStagedPublication() const = 0;
  /// CA/Backend/CasWriteResult.h
  struct Meta { uint64_t size; Etag etag; bool marked = false; };
  /// CA/Backend/CasObjectStorageBackend.h
  WriteSettings blobPublicationWriteSettings(bool marked) const;
  /// CA/Backend/CasProbe.h
  enum class BlobMarkProbeOutcome : uint8_t { Ok, Skipped, Assumed, FailedPreservation, FailedTokenForm, FalsePresence };
  struct BlobMarkProbeResult
  {
      BlobMarkProbeOutcome outcome = BlobMarkProbeOutcome::Skipped;
      String step;                   /// "P1".."P7" for a failure, else empty
      String text() const;           /// "ok" | "skipped" | "assumed" | the step
  };
  BlobMarkProbeResult runBlobMarkProbe(CasOperation & op, const String & probe_prefix);
  ```

- Rules the signatures do not state:
  - Native: `nativeHead` sets `marked` iff `ObjectMetadata::content_type == kBlobMarkContentType`, exactly. Token,
    size and mark come from the one `HEAD`. `blobPublicationWriteSettings(marked)` sets the content type; it feeds
    `writeObject` (`B:923-928`) and `copyObject` (`B:969-973`).
  - Emulated over local files: the mark is the owner-execute bit (`S_IXUSR`, `0100`) of the object file
    ([settled](#settled), S1). `emuPublishBlobAtomically` sets or clears it on the temporary file with
    `std::filesystem::permissions` after `out->finalize` and before the rename (`B:560`). `headUnder` reads it
    from `std::filesystem::status` of the resolved path, under `emu_mutex`. No other write path sets it.
    The emulated `HEAD` is then three file-system reads under one lock (size `B:740`, token `B:748`, mode), not
    one response; `emu_mutex` excludes only this process. Say so in the comment at the read and in the report.
    The writer's second `HEAD` reads all three again before it adopts.
  - In-memory: `Object::marked`. A streaming publication and a verbatim copy both store `request.marked`
    ([Spec questions](#spec-questions), Q5); `write` stores `false`.
  - `supportsVerbatimStagedPublication`: `ObjectStorageBackend`: native mode and
    `supportsCopyMode(ObjectStorageCopyMode::NativeOnly)` (the test of `B:955-967`); `InMemoryBackend`: true;
    decorators forward.
  - The probe runs P1 to P7 of §6.2 through `op.publish` and `op.head` on keys under `probe_prefix + "/mark/"`,
    P4 to P6 only when the backend supports the verbatim copy, and deletes its keys best effort as
    `runCapabilityProbe` does. It runs every row, then classifies: any preservation failure wins, then token form
    (P7), then false presence; `step` is the first failing row of the winning class. A mark failure is a result;
    a request failure propagates.

- [ ] **Step 1: Write the failing tests.**
  - `CASBlobMark`: `InMemoryPublishSetsAndClearsTheMark`; `InMemoryUnmarkedPublishOverAMarkedObjectReadsUnmarked`;
    `InMemoryCopyTakesTheMarkOfTheRequest`; `PlainWriteIsNeverMarked`; `EmulatedLocalMarkIsTheOwnerExecuteBit`
    (publish marked, then `stat`: mode `& 0100`); `EmulatedLocalRenameReplacesTheMark`;
    `EmulatedLocalObjectWrittenAnyOtherWayIsUnmarked`; `NativeHeadTakesTokenSizeAndMarkFromOneResponse` (the
    server records one `HeadObject`); `NativePublishSendsTheMarkTypeOnPutAndOnCopy`;
    `NativeUnmarkedPublishSendsBinaryOctetStream`; `AnyOtherContentTypeIsUnmarked`.
  - `CASBlobMarkProbe`, with `MarkLiarBackend`, which fails one row: `PassesOnTheInMemoryBackend`;
    `P1LostMarkIsAPreservationFailure`; `P2FalseMarkIsFalsePresence`; `P3KeptMarkIsFalsePresence`;
    `P4LostMarkOnCopyIsAPreservationFailure`; `P5FalseMarkOnCopyIsFalsePresence`;
    `P6KeptMarkOnCopyIsFalsePresence`; `P7UncanonicalTokenIsATokenFormFailure`;
    `CopyRowsAreSkippedWithoutCopySupport` (the journal holds no copy); `PreservationOutranksFalsePresence`;
    `RequestFailurePropagates`.
- [ ] **Step 2: Run red.** Add the fields, the predicate and a probe stub that returns `Ok`. Filter
  `CASBlobMark*`.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run green,** then `CAS*` on `build` (tag `t5_pre`). Existing tests change only by the new pure
  virtual (the listed test backends; a compile error names any that the list misses) and by brace
  initialisers of `RawMeta`, `Meta` or `BlobPublishRequest`.
- [ ] **Step 5: One mutant,** tag `t5_m1`: `nativeHead` compares the content type by prefix. Expected:
  `AnyOtherContentTypeIsUnmarked` fails.
- [ ] **Step 6: Commit,** then the gate. Subject: `Carry the CAS blob mark through publish and HEAD, and probe it`

### Task 6: Learning the switch, mount rules, `system.cas_mounts` {#task-6}

Implementer `ca-impl`. Reviewer `ca-review`. Read §3.1, §6.1 ("The dialect rule"), §6.2 (the bullets after the
probe table), §7.5, §7.7. No pool is switched in production yet; the rules are exercised on seeded pools.

**Files:**
- Modify: `CA/Pool/CasPool.h` (members near `:790`, `:837`), `CA/Pool/CasPool.cpp:282` (the runtime is built
  with the new callback), `:297` (`refreshAdmittedAlgos` is the model of an exact re-read), `:526-560` (the
  probe call and the skip branch), `:583-611` (rules between `createOrValidate` and `mountWritable`)
- Modify: `CA/Pool/CasMountRuntime.h:165-177` (constructor), `CA/Pool/CasMountRuntime.cpp:65-74`, `:626-664`
  (`renewRenewerOnce`: the callback after a committed renew)
- Modify: `src/Storages/System/StorageSystemContentAddressedMounts.cpp:40-63` and the row fill (`:185`)
- Test: create `T/gtest_cas_blob_intent_learning.cpp`

**Interfaces:**
- Consumes (Task 2): `PoolMeta::switched`, `token_dialect`, `created_generation`,
  `MountLease::protocol_generation`, which `Cas::listMounts` already returns in `MountInfo::lease`
  (`CA/Pool/CasServerRoot.h:512-524`; that file is not edited). (Task 3):
  `Layout::maxPoolPrefixBytesForBlobIntents`. (Task 5): `runBlobMarkProbe`, `BlobMarkProbeResult`,
  `MarkLiarBackend` (`T/cas_blob_mark_test_support.h`). Existing: `Retry::once` (`CA/Backend/CasRetry.h`),
  `MountRenewOutcome::Committed` (`CA/Pool/CasServerRoot.h:44`).
- Produces:

  ```cpp
  /// CA/Pool/CasPool.h
  static constexpr uint64_t blob_intents_reread_ms = 60'000;               /// renewer re-read while not switched
  static constexpr uint64_t blob_intents_marked_body_reread_ms = 10'000;   /// re-read asked by a writer
  bool blobIntentsLearned() const noexcept;        /// monotone: once true, never false, never re-read
  /// The one rule of "learned", applied to a `_pool_meta` this mount read itself. Returns the new state.
  bool applyBlobIntentsObservation(const PoolMeta & observed);
  /// One exact GET of `_pool_meta`, then the rule. Throws what the read throws.
  bool refreshBlobIntentsLearning(const Retry & policy = Retry::standard());
  void noteMarkedBodyWhileNotLearned();            /// at most one re-read per 10 s
  BlobMarkProbeResult blobMarkProbe() const;
  BlobProtocol observedBlobProtocol() const;       /// the last value this mount read
  /// CA/Pool/CasMountRuntime.h: one more constructor parameter, after `remount_attempt_`.
  /// Called on the renewing thread after each renew that ended `Committed`, from the background worker and
  /// from `renewWatermarkOnce`, without `driver_mutex`. Never throws.
  std::function<void()> after_renew_committed_
  ```

- Rules the signatures do not state:
  - `created` below means `observed.created_generation == 4`. Learned iff `observed.switched()` and
    `*observed.token_dialect == backend dialect` and (`probe == Ok`, or `created` and the probe is `Skipped` or
    `FalsePresence`). A skipped probe on a `created` pool is stored as `Assumed`. A token-form failure never
    learns, on any pool.
  - Probe: `runBlobMarkProbe` runs right after `runCapabilityProbe` (`CA/Pool/CasPool.cpp:546`), with the same
    per-mount prefix. Under `skip_access_check` (`:550`) the result is `Skipped`. A read-only open runs no probe.
  - Writable `Pool::open`, on the `_pool_meta` that `createOrValidate` returned (`:583-588`), before
    `mountWritable` (`:611`):

    | Pool | Condition | Result |
    |---|---|---|
    | switched | recorded dialect differs | `BAD_ARGUMENTS`, the dialect text of §7.5 |
    | switched | prefix over 668 bytes | `BAD_ARGUMENTS`, the prefix text of §7.5 |
    | switched | probe `Ok` | mounts, learned, one log line |
    | switched, not `created` | probe `FalsePresence` | `NOT_IMPLEMENTED`, the false-presence text of §7.5 |
    | switched | probe `FailedPreservation` or `FailedTokenForm`; or not `created` and `Skipped` | mounts, not learned, WARNING naming the step |
    | switched, `created` | probe `FalsePresence` | mounts, learned, WARNING |
    | switched, `created` | probe `Skipped` | mounts, learned, one INFO line, `Assumed` |
    | not switched | — | mounts, not learned |

  - Read points: the mount; the renewer; `noteMarkedBodyWhileNotLearned`. A failed or undecodable read leaves
    the state and the next point retries. A read that shows another dialect: not learned for the life of the
    mount, one ERROR with the dialect text, no further re-read.
  - **The renewer re-read runs on the production path.** `Pool` passes the callback when it builds
    `mount_runtime`; `CasMountRuntime::renewRenewerOnce` calls it after `consumeRenewResult` when the result is
    `Committed` and the call is the worker's or the direct one (not the startup and remount re-anchors). So
    `renewalLoop` (`CA/Pool/CasMountRuntime.cpp:760`) and `renewWatermarkOnce` (`:251`) both run it.
    `Pool::renewWatermarkOnce` (`CA/Pool/CasPool.cpp:1745`) gets no re-read of its own.
  - The callback: nothing while the last read showed the switch, the dialect latch is set, or less than 60 s
    passed on the mount's boot clock since the last read or attempt. Otherwise one
    `refreshBlobIntentsLearning` with `Retry::once` and a window of half `mount_renew_period`. It catches
    every exception, logs it once per mount at WARNING, and returns: a failed read never fails a renew, is not
    retried before the next 60 s, and adds no wait to the renewal loop.
  - A read-only open applies no rule and is not refused on another dialect.
  - `system.cas_mounts`: `protocol_generation` (`UInt64`, per slot, from `MountInfo::lease.protocol_generation`,
    0 for an older body), `blob_protocol` (`String`, `markers` or `intents`, this server's last read),
    `blob_mark_probe` (`Nullable(String)`, `BlobMarkProbeResult::text`, filled for this server's own slot).
  - Log: one line when a mount learns, one when it assumes the mark.

- [ ] **Step 1: Write the failing tests.** Pools are seeded by hand: `encodePoolMeta` of a `PoolMeta` with the
  wanted fields, written with `op.create`, plus `CasRefCatalog::initializeEmptyForNewPool`. A pool "switched in
  another dialect" is a hand-written `_pool_meta` with `token_dialect = etag` under an `InMemoryBackend`
  (`emulated`); no second backend is needed. An unswitched pool has no `created_generation`.
  - `CASBlobIntentDialect.MountOfASwitchedPoolIsRefused`; `CASBlobIntentDialect.LearningStaysNotLearned` (mounted
    before the switch, switched in another dialect: one ERROR, the second refresh sends no request).
  - `CASBlobIntentLearning`: `UnswitchedPoolIsNotLearned`; `SwitchSeenAtMountLearns`;
    `RenewerRereadsAfterSixtySecondsUntilLearned`; `DirectRenewAlsoRereads`;
    `FailedRenewerRereadLeavesTheRenewCommitted`; `MarkedBodyTriggerIsLimitedToOncePerTenSeconds`;
    `LearnedIsNeverUnlearned`; `FailedReadLeavesNotLearnedAndTheNextPointRetries`;
    `UndecodableMetaLeavesNotLearned`; `SkippedProbeOnAnUpgradedPoolNeverLearns`;
    `FalsePresenceFailsTheMountOfASwitchedUpgradedPool`; `LostMarkMountsUnlearnedWithAWarning`;
    `TokenFormFailureOnAnUpgradedPoolMountsUnlearned`;
    `TokenFormFailureOnAPoolCreatedByThisBuildMountsUnlearned`; `LongPrefixRefusesTheMountOfASwitchedPoolOnly`;
    `ReadOnlyOpenIsNotRefusedOnAnotherDialect`.
  - `RenewerRereadsAfterSixtySecondsUntilLearned` runs the background worker, never
    `Pool::renewWatermarkOnce`: `background_watermark = true`, `mount_renew_period` of 50 ms,
    `mount_lease_ttl_ms` of 10 minutes, the boot clock a `FakeClock` through `PoolConfig::boot_ms_fn` and
    `wait_sleep_fn` (the form of `T/gtest_cas_pool.cpp:3060-3085`). The test advances the clock in steps of
    1 s and waits, bounded, for the mount-slot write of each renew on a counting backend. Before 60 s: several
    committed renews and no `_pool_meta` `GET`. Past 60 s: exactly one `GET` after the next renew. The pool is
    then switched by hand: the next due re-read learns, and no `GET` follows.
  - `FailedRenewerRereadLeavesTheRenewCommitted`: the `_pool_meta` `GET` throws. The renew of that tick is
    committed, the next renew is sent on its period, and no second `GET` is sent before another 60 s.
  - `CASBlobIntentNewPool.SkippedProbeMountLearnsOnAPoolCreatedByThisBuild` and
    `.FalsePresenceWarnsAndTheMountLearns` (both on a pool seeded switched with `created_generation = 4`).
- [ ] **Step 2: Run red.** Add the members with `blobIntentsLearned` always false, the callback parameter
  unused, and no rule in `open`.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run green,** then `CAS*` on `build` (tag `t6_pre`). Existing tests change in one way only: a
  writable `Pool::open` without `skip_access_check` now sends the probe's requests (the publications, `HEAD`s
  and deletes of P1 to P7 under `_probe/`; on the in-memory backend the copy rows run too). A test that counts
  the requests of an open moves by exactly that; list each in the report. No test changes for a learning
  reason: every existing pool is unswitched.
- [ ] **Step 5: Two mutants,** tags `t6_m1`, `t6_m2`: (1) the rule drops the dialect comparison: expect
  `CASBlobIntentDialect.LearningStaysNotLearned` to fail; (2) the re-read is called from
  `Pool::renewWatermarkOnce` instead of the runtime callback: expect
  `RenewerRereadsAfterSixtySecondsUntilLearned` to fail while `DirectRenewAlsoRereads` passes.
- [ ] **Step 6:** Run `CASBlobIntentLearning*:CASBlobIntentDialect*:CASBlobIntentNewPool*:CASMount*` on
  `build_asan` (tag `t6_asan`): the worker test runs a background thread.
- [ ] **Step 7: Commit,** then the gate. Subject:
  `Learn the CAS blob intent switch per mount and show it in cas_mounts`

### Task 7: Publication: the mark bit and the payload hash check {#task-7}

Implementer `ca-surgeon`. Reviewer `ca-review`. Read §3.1 ("Mark bit"), §6.2 ("Plumbing" rows 1 to 5), §6.3. The
hash check is active for every pool from this task on; the mark bit is false everywhere until a mount learns.

**Files:**
- Modify: `CA/ContentAddressedTransaction.h:156-166` (`PendingBlob`), `:187`; `X:244-253` (hardlink copy),
  `:296-344` (source build), `:763-772` (`stageBlobPartFile`), `:939-977` (the two sinks), `:1036`
- Modify: `CA/Pool/CasPartWriteTxn.h:26-38` (`BlobSource`); `W:478-509` (the publication is built with the mark
  and the ref)
- Modify: `CA/Backend/CasBackend.h:54-59` (`StreamingBlobPublication`), `:86-110` (the bounded copy);
  `B:505-560`, `:902-952`; `CA/Backend/CasInMemoryBackend.cpp:194-232`; `CA/Backend/CasProbe.cpp` (the probe
  publishes with a matching ref); `src/Common/ProfileEvents.cpp` (`CASBlobPublishDigestMismatch`)
- Test: create `T/gtest_cas_blob_publish_digest.cpp`, `T/gtest_cas_blob_intent_writer.cpp`; modify
  `T/gtest_cas_s3_staging.cpp` (`FakeNativeCopyObjectStorage`, `:91-130`); the test backends that implement
  `publish` themselves build with the new field (`grep -n 'void publish(const' $T/*.cpp $T/*.h`: 19 sites in
  10 `.cpp` files and one in `T/cas_test_helpers.h:1941`)

**Interfaces:**
- Consumes (Task 5): `BlobPublishRequest::marked`, `kBlobMarkContentType`. (Task 6): `Pool::blobIntentsLearned`.
  (Task 4): `WriteSettings::object_storage_content_type`. Existing: `makeBlobHashingWriteBuffer`
  (`CA/Primitives/CasBlobHashingWriteBuffer.h:40`), the streaming convention described at `W:116-130`.
- Produces:

  ```cpp
  struct PendingBlob { /* existing */ bool marked = false; };       /// decided once, when the sink is created
  struct BlobSource  { /* existing */ bool marked = false; };       /// beside publication_attempted
  struct StreamingBlobPublication
  {
      uint64_t payload_size;
      String fresh_envelope;
      std::function<std::unique_ptr<ReadBuffer>()> open_payload;
      BlobRef expected_ref;       /// the key's digest; the forwarded payload must hash to it
  };
  void ContentAddressedTransaction::stageBlobPartFile(
      const ContentAddressedMetadataStorage::Route & route, const Cas::BlobRef & ref, size_t size,
      const std::string & staging_key, Cas::StagingBackend backend, bool marked);
  ```

- Rules the signatures do not state:
  - `marked` is read once, `store->blobIntentsLearned()`, where the staging sink is created (`X:939-977`, both
    branches, and `:1036`), and passed to `stageBlobPartFile`. The S3 staging sink (`X:951`) passes
    `WriteSettings` with the mark content type iff `marked`. A hardlink copies the record (`X:252`), so the copies
    share the bit. `source.marked = pb.marked` beside `publication_attempted` (`X:300`). The writer passes
    `BlobPublishRequest::marked = source.marked` on every publication of that blob (`W:509`).
  - Hash check: every streaming publication hashes exactly the payload bytes it forwards, with the algorithm of
    `expected_ref.algo` in the streaming convention, and compares before the object is completed: before
    `out->finalize` (`B:951`), before the rename (`B:560`), before the map insert (in-memory). On a mismatch:
    `out->cancel`, `CORRUPTED_DATA` with the text `payload of <key> does not hash to its key; upload aborted,
    nothing published`, the counter, nothing published. The code is in the deterministic class of
    `isDeterministicBlobPublicationFailure` (`W:89-106`): one attempt, no retry.
  - The verbatim copy is not hashed.

- [ ] **Step 1: Write the failing tests.**
  - `CASBlobPublishDigest`: `FlippedByteIsRefusedBeforeCompletion` (in-memory and emulated local: the destination
    is absent after the exception, exactly one publication attempt, the counter moved);
    `StaleSourceOfASecondAbsentPublisherIsRefused` (the schedule S7 of §4.3: the source turns into another
    payload of equal length after staging); `S3StagedRestreamIsVerified` (on `EtagFaithfulPublicationBackend`,
    a second publication of an S3-staged blob streams and is checked); `MatchingPayloadPublishes` (pin);
    `VerbatimCopyIsNotHashed` (pin: a copy sends no payload read).
  - `CASBlobIntentWriter.MarkBitIsSharedByHardlinkCopies`: two parts over one staged blob on a learned mount:
    both publications carry `marked = true`; staged on a mount that is not learned: both `false`, also when the
    mount learns before the upload. The fixtures of Task 8 do not exist yet, so the pools are seeded as Task 6
    seeds them. Learned: a hand-written `_pool_meta` with `blob_protocol = intents`, `token_dialect = emulated`
    and `created_generation = 4`, plus the empty catalog, then `Pool::open` on an `InMemoryBackend` (the probe
    passes); assert `blobIntentsLearned`. Learns later: a pool without `created_generation`, opened unswitched;
    after staging, the test replaces `_pool_meta` with the switched body and calls
    `refreshBlobIntentsLearning`. The test asserts the `marked` field of the publish requests a recording
    backend saw, not the writer's request sequence.
  - `CASS3Staging.StagingObjectCarriesTheMarkType` (the staging `writeObject` got the content type iff learned).
    `FakeNativeCopyObjectStorage` is a `LocalObjectStorage` and drops the type, so P1 of the probe would fail
    and the mount would never learn. Extend the fake: it keeps `WriteSettings::object_storage_content_type` per
    path on `writeObject`, gives a copy destination the type of its source, and reports it as
    `ObjectMetadata::content_type`.
- [ ] **Step 2: Run red.** Add the fields and the parameter; no check, `marked` never set.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run green,** then `CAS*` on `build` (tag `t7_pre`). Existing tests that publish a payload under a
  key it does not hash to now fail with `CORRUPTED_DATA`: change the key to `idOf(payload)` (or the streaming
  hash for a payload over one hashing block) and list each in the report. More than 40 such tests, or one whose
  point is the mismatched key: stop and report.
- [ ] **Step 5: One mutant,** tag `t7_m1`: the check runs after `finalize`. Expected:
  `FlippedByteIsRefusedBeforeCompletion` fails on "destination absent".
- [ ] **Step 6: Commit,** then the gate. Subject:
  `Decide the CAS blob mark at staging and hash every streamed publication`

### Task 8: Test fixtures; marker-mode suites pinned to an upgraded pool {#task-8}

Implementer `ca-impl`. Reviewer `ca-review-lite`. Test code only. Read §10.2 (first paragraph), §7.4. Every
existing test stays green before and after: today every pool is unswitched, so pinning changes nothing yet.

**Files:**
- Create: `T/cas_blob_intent_test_support.h`, `T/gtest_cas_blob_intent_fixtures.cpp`
- Modify: the pool setup of `T/gtest_cas_gc_meta_writer.cpp`, `T/gtest_cas_gc_round.cpp`,
  `T/gtest_cas_gc_read_ahead.cpp`, `T/gtest_cas_blob_meta.cpp`, and of each test in `T/gtest_cas_gc_ack_floor.cpp`,
  `T/gtest_cas_protocol_scenarios.cpp`, `T/gtest_cas_gc_frontier_gate.cpp`,
  `T/gtest_cas_gc_redelete_concurrency.cpp`, `T/gtest_cas_gc_leak.cpp`, `T/gtest_cas_rebuild_condemn_nothing.cpp`
  whose assertions name a marker (`blobMetaKey`, `MetaState`, `loadMetaForTest`, `writeMetaClean`,
  `condemnMeta`, a `CASMeta*` or `CASGCMeta*` or `CASGCCondemnMarker*` counter, the meta pool)

**Interfaces:**
- Consumes (Task 2): `PoolMeta` fields, `PoolMeta::switchToBlobIntents`. (Task 5): `BlobPublishRequest::marked`.
  (Task 6): `Pool::refreshBlobIntentsLearning`, `blobIntentsLearned`.
- Produces, in `T/cas_blob_intent_test_support.h`, namespace `DB::Cas::tests`:

  ```cpp
  /// A pool as an older build leaves it: `_pool_meta` at floor 3, no `created_generation`, no `blob_protocol`,
  /// plus the canonical empty catalog. Idempotent. A later `Pool::open` mounts it as an upgraded pool.
  void seedUpgradedPoolForTest(Backend & backend, const String & pool_prefix = "p", uint64_t gc_shards = 1);
  /// State N2 of a pool created by this build: floor 4, `created_generation = 4`, not switched, empty catalog.
  void seedNewPoolAtN2ForTest(Backend & backend, const String & pool_prefix = "p", uint64_t gc_shards = 1);
  /// Opens a pool created by this build, switched and learned: seeds N2 when `_pool_meta` is absent, opens,
  /// switches with `PoolMeta::switchToBlobIntents` when the open did not, refreshes the learning.
  /// Throws std::runtime_error unless the pool ends learned.
  PoolPtr openSwitchedPoolForTest(BackendPtr backend, PoolConfig config);
  /// One unconditional publication of `payload` under its own hash, with a fresh envelope, through `op.publish`.
  BlobRef publishBlobRawForTest(const PoolPtr & store, const String & payload, bool marked);
  /// An ordered journal of every request, one entry per request: "HEAD <key>", "GET <key>", "PUT <key>",
  /// "CAS <key>", "DELETE <key>", "PUBLISH <key> marked|unmarked copy|stream", "LIST <prefix>",
  /// "BULK-DELETE <n> <first key>". A `CountingBackend`.
  class ProtocolJournalBackend;
  std::vector<String> ProtocolJournalBackend::journal() const;
  std::vector<String> ProtocolJournalBackend::journalFor(std::string_view key_substring) const;
  void ProtocolJournalBackend::clearJournal();
  /// A second client of one store that speaks another token dialect: forwards every call to `inner` and
  /// overrides `dialect` only. Tokens keep the inner backend's values.
  class DialectOverrideBackend;      /// DialectOverrideBackend(BackendPtr inner, Dialect dialect)
  ```

- Rules:
  - `seedUpgradedPoolForTest` encodes its `PoolMeta` by hand and writes it with `op.create`; no product
    function gains a test parameter.
  - `PoolMeta::createOrValidate` returns an existing `_pool_meta` unchanged and `Pool::open` then takes
    `gc_shards` from it (`CA/Pool/CasPool.cpp:588`). So a seed with the default `gc_shards = 1` silently
    turns a sharded test into an unsharded one, and the pass count cannot show it. Every pin passes the
    test's own prefix and `gc_shards`.

- [ ] **Step 1: Write the tests** (`CASBlobIntentFixtures`): `UpgradedSeedMountsUnswitchedAtFloorFour`;
  `UpgradedSeedHasNoCreatedGeneration`; `SwitchedPoolIsLearned`; `RawPublicationReadsMarkedOrUnmarked`;
  `JournalRecordsEachVerbInOrder`; `UpgradedSeedKeepsTheShardCountOfTheTest` (seeded with `gc_shards = 4`:
  the opened pool reports 4); `DialectOverrideChangesOnlyTheDialect`. Red: the header does not exist; add it
  with stubs that throw.
- [ ] **Step 2: Implement the fixtures.** Run green.
- [ ] **Step 3: Pin the marker-mode suites.** In each file of the list, the pool setup calls
  `seedUpgradedPoolForTest(*backend, config.pool_prefix, config.gc_shards)` before `Pool::open`, with the
  values of the `PoolConfig` that the open gets. A fixture that calls `seedPoolMetaForRestart`
  (`T/cas_test_helpers.h:1082`) calls the new function instead, with the same arguments. Fixtures that set
  `gc_shards` above 1 today: `T/gtest_cas_gc_round.cpp:723`, `:1188`, `:1915`;
  `T/gtest_cas_gc_redelete_concurrency.cpp:184-191`; `T/gtest_cas_gc_read_ahead.cpp:1008`; find the rest with
  `grep -n 'gc_shards' <file>`. Write into the report, per pinned fixture: the file, the count of tests, and
  that `store->poolConfig().gc_shards` after the open equals the configured value.
- [ ] **Step 4:** `CAS*` on `build` (tag `t8_pre`): the same pass count as before the task plus the seven new
  tests. A test that turns red is a stop: the pin must be neutral.
- [ ] **Step 5: Commit,** then the gate and the ASan milestone. Subject:
  `Add test fixtures for upgraded and switched CAS pools`

### Task 9: GC: the per-round mode read, the dialect check, leaving GC {#task-9}

Implementer `ca-surgeon`. Reviewer `ca-review`. Read §3.3 (G1, G10, "A mount with another dialect leaves GC"),
§3.5, §3.6. After this task a round reads its mode from `_pool_meta`; on an unswitched pool it runs today's
code plus that one read. On a switched pool the round stops after the read: the intent-mode fold is Task 10.
Only test pools are switched, so no production round reaches that stop.

**Files:**
- Modify: `CA/Gc/CasGc.h` (new member), `G:765-793` (round start), `:4488`, `:4660` (`rebuildBaseline`)
- Modify: `CA/Gc/CasGcScheduler.cpp:358-366` (manual round), `:516-528` (heartbeat loop), the round loop
- Modify: `CA/Pool/CasPool.h`, `CA/Pool/CasPool.cpp` (the flag below)
- Test: create `T/gtest_cas_gc_intents.cpp`; modify `T/gtest_cas_gc_round.cpp`, `T/gtest_cas_gc_read_ahead.cpp`,
  `T/gtest_cas_gc_log.cpp` (the one new request and phase per round)

**Interfaces:**
- Consumes (Task 2): `BlobProtocol`, `PoolMeta` fields, `GcState::generation`. (Task 8): the fixtures,
  `DialectOverrideBackend`. Existing: `Gc::acquireOrRenewLease` (`G:5185`), `decodePoolMeta`.
- Produces:

  ```cpp
  /// CA/Gc/CasGc.h
  /// One exact GET of `_pool_meta`, sent after the lease. Throws; never returns a default.
  BlobProtocol Gc::readRoundBlobProtocol(CasOperation & op, const GcState & leased_state);
  /// CA/Pool/CasPool.h
  void markGcLeftForDialect(Dialect recorded, Dialect own);    /// once per mount; logs the ERROR of §3.3
  bool gcLeftForDialect() const noexcept;
  ```

  The mode is the `BlobProtocol` of Task 2. No second enum names the same two values.
- Rules the signatures do not state (G1 of §3.3 is the authority; these are the placements):
  - **G1.** `runRegularRound` calls `readRoundBlobProtocol` right after the `lease` phase (`G:786-793`), in a new
    phase `blob_protocol_mode` (metrics `intents` 0 or 1, `gc_state_generation`), before `on_lease_acquired` and
    every other step. Outcomes: decoded without `blob_protocol`: `Markers`. `intents` with the backend's dialect:
    `Intents`, and `leased_state.generation < 4` throws the `INVALID_STATE` text of §7.5. `intents` with another
    dialect: `markGcLeftForDialect`, then `BAD_ARGUMENTS` with the dialect text. Absent: `INVALID_STATE`, `CAS pool
    meta '<key>' is absent; the GC round cannot take its mode`. A refused or unanswered read, a decode failure:
    the exception propagates. No `catch` on this path yields a mode. Nothing caches the mode across rounds.
  - **Intent mode, in this task only.** A round whose read returned `Intents` throws `NOT_IMPLEMENTED`, `CAS gc:
    intent mode needs the intent fold`, right after the phase, with no further request. Task 10 removes the
    throw. Marker mode is today's code.
  - **Leaving GC.** When `gcLeftForDialect`: the scheduler loop starts no round and sends no heartbeat
    (`CA/Gc/CasGcScheduler.cpp:516-528`); `runOneRoundNow` (`:358-366`) and `Gc::rebuildBaseline` throw
    `BAD_ARGUMENTS` with the dialect text before any `gc/state` request. `rebuildBaseline` also makes the G1
    dialect check on its own `_pool_meta` read before its lease (`G:4660`), and never lowers `blob_protocol`.
    The flag lives on `Pool`: it outlives a `Gc` object.

- [ ] **Step 1: Write the failing tests** in `T/gtest_cas_gc_intents.cpp`, over a `ProtocolJournalBackend`.
  - `CASGcIntentsMode`: `ModeIsReadAfterTheLeaseInEveryRound` (an upgraded pool: in each of three rounds the
    `_pool_meta` `GET` follows the `gc/state` write and precedes every other request);
    `AbsentPoolMetaFailsTheRound`; `FailedPoolMetaReadFailsTheRound`; `UnknownProtocolWordFailsTheRound`;
    `IntentModeBelowGenerationFourFails` (a switched pool whose `gc/state` is seeded at header 3);
    `IntentModeStopsUntilTheFoldExists` (`openSwitchedPoolForTest`: `NOT_IMPLEMENTED`; Task 10 deletes this
    test). Each failing case asserts the code, the text, and zero requests after the read.
  - `CASBlobIntentDialect.GcRoundFailsAndTheMountLeavesGc`: the second member opens the same store through a
    `DialectOverrideBackend`. After its failed round, no further `gc/state` write and no heartbeat from that
    mount; its `runOneRoundNow` and `rebuildBaseline` throw the dialect text; the member with the recorded
    dialect takes the lease after its two observations.
- [ ] **Step 2: Run red.** Add the signatures with `readRoundBlobProtocol` returning `Markers` after its read.
  Expected: the mode tests fail on "the round did not fail".
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run green,** filter `CASGc*:CASGC*:CASBlobIntentDialect*`, then `CAS*` on `build` (tag `t9_pre`).
  Existing tests change in one way only: a round sends one more request, the exact `GET` of `_pool_meta` after
  the lease, and emits the phase `blob_protocol_mode`. A test that counts a round's requests, `CASOtherGet`, or
  the phases of `cas_gc_log` moves by exactly that. Any other red test is a stop.
- [ ] **Step 5: Two mutants,** tags `t9_m1`, `t9_m2`: (1) a failed `_pool_meta` read yields `Markers`: expect
  `FailedPoolMetaReadFailsTheRound` to fail; (2) the scheduler ignores `gcLeftForDialect`: expect
  `GcRoundFailsAndTheMountLeavesGc` to fail.
- [ ] **Step 6: Commit,** then the gate. Subject: `Read the CAS blob protocol of each GC round from the pool meta`

### Task 10: GC intent mode: the fold and the delete batch {#task-10}

Implementer `ca-surgeon`. Reviewers: `ca-review`, then a second independent `ca-review` that gets the diff and
the spec, not the first report. Read §3.3 whole, §3.5, §3.6, §4, §7.6. After this task a round on a switched
pool condemns, graduates by intent and deletes under G2 to G10.

**Files:**
- Modify: `CA/Gc/CasGc.h` (`RedeleteIo` `:763-767`), `G:541-556` (`removeChunkWriteOnceOrOneByOne`, reused),
  `:583-595` (`performRedeleteIo`), `:597-641` (`applyRedeleteOutcome`), `:659-762` (`redeleteBlobs`),
  `:765-793` (the throw of Task 9 goes), `:1276-1283` (`meta_pool_wait`), `:2183-2229` (`head_blob`),
  `:2247-2254` (`peek_head`), `:2264-2287` (the marker gate), `:3667-3700` (graduation hints), `:3727`, `:3772`
  (fold calls)
- Modify: `CA/Gc/CasBlobInDegree.h:105-118` (`GraduationReadHints`), `:446-468` (fold signature); `N:431`,
  `:520-583` (`settleEntry`), `:586-664` (`closeBlob`); `CA/Gc/CasGcShardPlan.cpp:57`
- Modify: `CA/Gc/CasGcReadAhead.h:53-57`, `CA/Gc/CasGcReadAhead.cpp` (a create verb)
- Modify: `src/Common/ProfileEvents.cpp` (nine counters)
- Test: `T/gtest_cas_gc_intents.cpp`; `T/gtest_cas_blob_indegree.cpp` (`acceptAllHints`, `:1182`, and the hints
  at `:1259` build `GraduationReadHints`, whose `admit` gains the row token)

**Interfaces:**
- Consumes (Task 1): `sameIncarnation`, `Etag::canonical`, `PersistedEtag::canonical`. (Task 2):
  `RetiredEntry::intent`, the run header version. (Task 3): `Layout::blobIntentKey`,
  `writeOnceBlobIntentKey`, `blobIntentRecord`, `isBlobIntentRecord`. (Task 5): `Meta::marked`. (Task 8): the
  fixtures. (Task 9): the round's `BlobProtocol`. Existing: `CasOperation::create` (`Q:1154`; a clean refusal
  is `Conflict`, `Q:1118-1119`), `removeManyWriteOnce` (`Q:634`), `removeCurrent` (`Q:656`),
  `Gc::bulkDeleteChunkKeys` (`G:576`).
- Produces:

  ```cpp
  /// CA/Gc/CasGc.h
  struct RedeleteIo { String blob_key; Removal del = Removal::Gone; bool token_rejected = false; bool removed_unmarked = false; };
  /// CA/Gc/CasBlobInDegree.h
  struct IntentModeFold                    /// null in marker mode
  {
      /// G3. PUT I(B,t) with If-None-Match and the canonical record. True iff confirmed.
      std::function<bool(const RetiredEntry &)> confirm_intent;
      /// G7. One best-effort write-once delete of the old row's intent; a failure is counted, never thrown.
      std::function<void(const RetiredEntry & superseded)> delete_superseded_intent;
      /// A token that does not canonicalise ended or skipped a row at `site` ("condemn", "supersede", "row").
      std::function<void(const BlobRef &, std::string_view site)> token_rejected;
  };
  /// foldDeltasIntoGeneration: one more trailing parameter
  const IntentModeFold * intent_mode = nullptr);
  struct GraduationReadHints { std::function<bool(const BlobRef &, const PersistedEtag &)> admit; /* offer unchanged */ };
  /// CA/Gc/CasGcReadAhead.h
  void hintCreate(const String & key, const String & bytes);
  WriteResult takeCreate(const String & key, const String & bytes);    /// issues the create now if it was not hinted
  ```

- Rules the signatures do not state (G2 to G10 of §3.3 are the authority; these are the placements):
  - **Marker mode** passes `intent_mode = nullptr` and is today's code. A row with `intent` met by a marker-mode
    fold: `CORRUPTED_DATA`, `condemned row carries an intent but the pool is not switched`.
  - **Intent mode, fold.** `confirm_condemned_marker` is not passed and the marker pool is not used; the phase
    `meta_pool_wait` (`G:1276-1283`) is emitted in marker mode only. Runs are written with header 4.
    - G2, in `head_blob` (`G:2183-2229`): no `scheduleCondemnMarkerWrite`; a head token that does not
      canonicalise: `token_rejected(ref, "condemn")`, no row.
    - G8, at the top of `settleEntry` (`N:520`), before the in-degree branch reads `delete_pending` and before the
      look-ahead offers keys: a row with `delete_pending` and without `intent` is handled as not pending; count
      `CASGCInheritedPendingDemoted` once per row and round.
    - G6: the spare branch (`N:520-543`) sends nothing; a spared row with `intent` counts
      `CASGCIntentLeftOnSpare`.
    - G3, in the graduation branch (`N:559-583`): `confirm_intent(e)` replaces the marker gate. Confirmed: the row
      gets `delete_pending = true`, `intent = true`, `marker_confirmed = false`. Not confirmed: carried unchanged,
      `CASGCIntentUnconfirmedCarry`. A row whose own token does not canonicalise never reaches the gate: it is
      dropped (`token_rejected(ref, "row")`) unless G7 supersedes it.
    - G7, in `closeBlob` (`N:612-631`): the compare is `sameIncarnation`, not `matches`. Another canonical
      identity `u`: supersede as today, and when the old row had `intent`, `delete_superseded_intent(old)`. A head
      token that does not canonicalise: no new row, the old row is dropped, its intent stays.
  - **`confirm_intent`** (in `G:`, replacing the role of `G:2264-2287`): `takeCreate(key, blobIntentRecord())`.
    Confirmed iff the §3.3 table "G3: what confirms" says yes: `Committed`; or `Conflict` whose `seen` is an
    `Object` with `isBlobIntentRecord(bytes)`. Everything else, an exception included, is not confirmed; other
    bytes at the key add one ERROR per round. Counters `CASGCIntentCreated`, `CASGCIntentConfirmedExisting`.
  - **Read-ahead.** `GraduationReadHints::admit` gets the row token (`N:559-571` passes `e.token`). In intent mode
    the hint is `hintCreate(blobIntentKey(ref, identity), record)` and only `offer` may issue it ahead: a carried
    row that no delta or retirement of this round touches (`G:3690-3697`). An inherited pending row is demoted
    before read-ahead picks its keys, so it gets a create hint, never a delete.
  - The seal's `CondemnedSummary` (`CA/Formats/CasFoldSealFormat.h:174-180`) does not change: a row with
    `pending` counts in `pending_total` with or without `intent`, so `graduationDue` (`G:4373`) forces the fold
    that demotes an inherited row. A shard carried without a fold keeps its older run, header and rows.
  - **G4, G5, the delete batch** (`G:583-762`), for a row read with `delete_pending` and `intent`:
    `performRedeleteIo`: `HEAD`; absent: `Gone`; `sameIncarnation(entry.token, observed)`: `op.remove` with the
    raw token of this `HEAD`; another identity: `Mismatch`, no body request; a token that does not canonicalise:
    `token_rejected`, no body request and no intent delete. After the per-row jobs, `redeleteBlobs` collects
    `writeOnceBlobIntentKey` of every row whose body step answered and was not `token_rejected`, and deletes them
    in chunks of `bulkDeleteChunkKeys` with `removeChunkWriteOnceOrOneByOne` and `Retry::standard()`. A row whose
    job threw contributes no intent key. Any throw, in a job or in a chunk, fails the round before its commit
    (`G:752-762` stays). After the chunks: for each row with `removed_unmarked`, `op.removeCurrent(blobMetaKey)`,
    best effort, counted `CASGCLegacyMarkerDeleted`. Counter `CASGCIntentDeleted` per key; `CASGCIntentDeleteFailed`
    for a failed supersede delete.
  - Phase metrics on `pending_deletes`: `intent_deletes`, `intent_delete_requests`, `legacy_marker_deletes`,
    `token_rejected`. Every `token_rejected` call counts `CASGCIntentTokenRejected`; one ERROR per round.
  - Tests drive references through manifests and raw helpers (`writeManifestRaw`, `publishBlobRawForTest`,
    `displaceBlobToken`), never through writer adoption, which changes in the next task.

- [ ] **Step 1: Write the failing tests** in `T/gtest_cas_gc_intents.cpp`, on `openSwitchedPoolForTest` over a
  `ProtocolJournalBackend` unless stated. Delete `CASGcIntentsMode.IntentModeStopsUntilTheFoldExists`.
  - `CASGcIntentsMode`: `ModeIsReadAfterTheLeaseInEveryRound` gains the switched pool (three intent-mode
    rounds, the same order); `MarkerModeRoundFailsOnARowWithIntent` (an upgraded pool whose run, written by
    hand at header 4, holds a row with `intent`: the code, the text, zero requests after the fold met it).
  - `CASGcIntents`, the names of §10.2: `CondemnIsOneHead`, `GraduationCreatesAndCommitsPending`,
    `EqualOccupantConfirms`, `AbsentOrDivergentOccupantCarries`, `UnansweredCreateCarries`,
    `DeleteIsThreeRequests`, `IntentDeletesShareABatchRequest`,
    `FailedIntentDeleteFailsTheRoundAndTheNextFinishes`, `RefusedIntentDeleteKeepsTheRowPending`,
    `UnansweredBodyDeleteKeepsTheIntent`, `MismatchDeletesTheOldIntentAndDropsTheRow`, `SpareLeavesTheIntent`,
    `RecondemnationConfirmsOnTheStandingIntent`, `SupersedeDeletesTheOldIntentOnce`,
    `InheritedPendingIsDemotedBeforeAnyDelete`, `LegacyBodyDeleteRemovesItsMarkerWithHeadAndDelete`,
    `RebuildKeepsGenerationFourAndReadsNoIntent`, `UncanonicalTokenWritesNoRowAndDropsAnInheritedOne`,
    `UncanonicalRowTokenIsSupersededByACanonicalBody`, `UncanonicalTokenAtTheDeleteHeadDropsTheRowWithNoRequest`.
    Assertions that the names do not give: `GraduationCreatesAndCommitsPending`: no body `DELETE` in the
    graduating round, the committed row has `pending` and `intent`. `DeleteIsThreeRequests`: the journal for the
    blob in the delete round is `HEAD`, `DELETE` body, one bulk or single `DELETE` of the intent, in this order;
    the total over the three rounds is 5. `SpareLeavesTheIntent`: the intent key still holds the record and the
    journal has no request on it. `UnansweredBodyDeleteKeepsTheIntent`: no intent delete follows, the round
    fails, the row stays pending. `InheritedPendingIsDemotedBeforeAnyDelete`: with read-ahead on, the journal of
    the demoting round holds no `HEAD` and no `DELETE` of that body before its intent create.
  - Plan-named: `RespelledTokenAtTheDeleteHeadStillDeletes` (a backend that returns the token bare at condemn and
    quoted at the delete: the body is deleted); `RespelledTokenIsNotASupersede`;
    `IntentModeSendsNoMetaRequestForAMarkedBody`; `ReadAheadCreateIsOnlyForAnUntouchedCarriedRow`;
    `IntentModeRunHasHeaderFour`.
- [ ] **Step 2: Run red.** Add the signatures with marker behaviour (the `intent_mode` parameter ignored) and
  remove the throw of Task 9. Expected: the intent tests fail on marker requests in the journal.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run green,** filter `CASGc*:CASGC*:CASBlobInDegree*:CASBlobIntentDialect*`, then `CAS*` on `build`
  (tag `t10_pre`). Existing tests change in one way only: the brace initialisers of `GraduationReadHints` in
  `T/gtest_cas_blob_indegree.cpp`. Any other red test is a stop.
- [ ] **Step 5: Four mutants,** tags `t10_m1` to `t10_m4`: (1) the spare branch deletes the intent: expect
  `SpareLeavesTheIntent` to fail; (2) `confirm_intent` returns true for `Conflict` with `ProvenAbsent`: expect
  `AbsentOrDivergentOccupantCarries` to fail; (3) read-ahead offers an inherited pending row before the G8
  demotion: expect `InheritedPendingIsDemotedBeforeAnyDelete` to fail; (4) the intent delete runs for a row
  whose body job threw: expect `UnansweredBodyDeleteKeepsTheIntent` to fail.
- [ ] **Step 6: Commit,** then the gate and the ASan milestone. Subject: `Add the intent mode of CAS blob GC`

### Task 11: Writer rules on a learned mount {#task-11}

Implementer `ca-surgeon`. Reviewer `ca-review`. Read §3.2 whole, §4.2 ("Adoption"), §4.3 (S1, S12). After this
task `ensureBlobPresent` follows W1 to W8 when the mount is learned. A mount that is not learned keeps today's
body unchanged; Task 15 narrows that path and Task 16 removes it.

**Files:**
- Modify: `CA/Pool/CasPartWriteTxn.h:84-105` (`BlobPublicationReason`), `:351`; `W:274-570`
- Modify: `src/Common/ProfileEvents.cpp` (four counters, near `:834`)
- Test: `T/gtest_cas_blob_intent_writer.cpp` (created by Task 7)

**Interfaces:**
- Consumes (Task 1): `Etag::canonical`, `sameIncarnation`. (Task 3): `Layout::blobIntentKey`. (Task 5):
  `Meta::marked`. (Task 6): `Pool::blobIntentsLearned`, `noteMarkedBodyWhileNotLearned`. (Task 7):
  `BlobSource::marked`, `StreamingBlobPublication::expected_ref`. (Task 8): the fixtures. Existing: the loop, its
  frozen deadline and the cap of 8 (`W:296-297`, `:406`, `:415-423`), `buildHeader` (`W:322-331`),
  `requireAdmitted`, `source.beginPublication` (`CA/Pool/CasPartWriteTxn.h:39-42`).
- Produces:

  ```cpp
  /// CA/Pool/CasPartWriteTxn.h; `Condemned` stays until the marker path goes
  enum class BlobPublicationReason : uint8_t
  { Absent, Condemned, IntentPresent, LegacyBody, TokenRejected, ChangedAtSecondHead };
  /// private
  BlobUploadResult PartWriteTxn::ensureBlobPresentByIntents(const BlobUploadRequest & req, CasOperation & op) const;
  ```

  `ensureBlobPresent` keeps W1 (`W:277-281`), then: `if (store->blobIntentsLearned()) return ensureBlobPresentByIntents(...)`.
- Rules the signatures do not state (the §3.2 table is the authority):
  - W2: `op.head(key, policy)`. Present: the two size checks of `W:427-444`, unchanged.
  - W3: present and unmarked: reason `LegacyBody`, `CASBlobReuseLegacyRepublish`. Present, marked, token without
    an identity: reason `TokenRejected`, `CASBlobReuseTokenRejected`, one ERROR per mount (a flag on the pool),
    never an exception. Both go to W6 with no other request.
  - W4: `op.read(blobIntentKey(ref, identity), policy)`. Any object: reason `IntentPresent`,
    `CASBlobReuseIntentPresent`, event `BlobReuseResurrect`, to W6. Nothing: W5.
  - W5: `op.head(key, policy)` again. `sameIncarnation` with the W2 token and a valid size: adopt
    (`requireAdmitted` twice as at `W:454`, `:466`; `CASBlobBodyPutAvoided`; event `BlobReuseAdopt`). The same
    identity with an invalid size: `CORRUPTED_DATA`. Absent, another identity, or no identity: reason
    `ChangedAtSecondHead`, `CASBlobReuseChangedAtSecondHead`, to W6 once, with no return to W2.
  - W6: one unconditional publication, sent once (`publication_policy`, `W:299`), fresh envelope, `marked =
    source.marked`, `expected_ref = ref`. The verbatim copy is chosen iff `first_publication` and W2 answered
    absent and `source.server_side_copy_from`. Answered success: `Materialized`, no further request. Unanswered:
    the next loop iteration from W2 with the backoff, as `W:511-540`.
  - W7: no request after W6. The function contains no marker code.
  - W8: W2, W4 and W5 are three requests of this attempt, each sent after the answer of the one before. No
    result is taken from a cache or from a request sent before the precommit edge.
  - A refused or unanswered `HEAD` or intent `GET` propagates as an exception. It is never read as absence.
  - Event detail `publication_reason` words: `absent`, `intent_present`, `legacy_body`, `token_rejected`,
    `changed_at_second_head`.

- [ ] **Step 1: Write the failing tests,** suite `CASBlobIntentWriter`, on `openSwitchedPoolForTest` over a
  `ProtocolJournalBackend`; token-faithful cases on `EtagFaithfulPublicationBackend`. The names of §10.2, each
  asserting the journal of the blob key and its intent key for one attempt:
  - `FirstCreationIsHeadAndOnePut`: `HEAD`, `PUBLISH marked`; no intent request, no `.meta` request.
  - `CleanReuseIsHeadGetHead`: `HEAD`, `GET` intent, `HEAD`; no `PUBLISH`.
  - `IntentPresentPublishesFreshOnce`: `HEAD`, `GET` intent, `PUBLISH`; one publication, a new token.
  - `LegacyBodyIsRepublishedMarkedWithNoMetaRequest`: body written unmarked with a `Clean` marker beside it:
    `HEAD`, `PUBLISH marked`; the journal has no `.meta` key.
  - `ChangedTokenAtSecondHeadPublishesWithoutRestart`: a hook replaces the body between the intent `GET` and the
    second `HEAD`: `HEAD`, `GET`, `HEAD`, `PUBLISH`; no third `HEAD`.
  - `BodyGoneAtSecondHeadPublishes`; `SameTokenInvalidSizeAtSecondHeadIsCorrupted`;
    `LandedAmbiguousPublicationIsAdoptedInFourReads` (`HEAD`, `PUBLISH` unanswered and landed, `HEAD`, `GET`,
    `HEAD`: adopted); `FailedIntentReadIsNotAbsence` (exception, no adoption, no publication);
    `WriterNeverMutatesIntents` (over every test of the file: no `PUT`, `CAS` or `DELETE` on an intent key);
    `VerbatimCopyIsSelectedOnlyAfterAnAbsentFirstHead`; `UncanonicalTokenPublishesFreshAndNeverThrows`;
    `UncanonicalTokenAtSecondHeadPublishesFresh`.
  - `AdoptionReadsReachTheStoreAfterAPrimedAbsence` (as §10.2 states it): read `I(B,t)` absent through the same
    pool, let GC confirm the intent and commit, then write: the backend saw both `HEAD`s and the intent `GET` of
    this attempt, and the writer publishes.
  - `UnansweredPublicationsStopAtEightAttempts` (pin of the cap under the new path).
- [ ] **Step 2: Run red.** Add the dispatch line with `ensureBlobPresentByIntents` calling today's body. Expected:
  journals show a `.meta` `GET` and `PUT`.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run green,** then `CAS*` on `build` (tag `t11_pre`). One existing test may change:
  `CASBlobIntentWriter.MarkBitIsSharedByHardlinkCopies` (Task 7) runs a writer on a learned mount, so its
  request sequence moves to the new path; its assertions on `marked` must hold unchanged. If they do not, stop.
  No other existing test runs a writer on a learned mount: the GC tests of Tasks 9 and 10 use raw helpers.
- [ ] **Step 5: Three mutants,** tags `t11_m1` to `t11_m3`: (1) adopt after the intent `GET` without the second
  `HEAD`: expect `ChangedTokenAtSecondHeadPublishesWithoutRestart` and `BodyGoneAtSecondHeadPublishes` to fail;
  (2) a failed intent `GET` is read as absent: expect `FailedIntentReadIsNotAbsence` to fail; (3) the second
  `HEAD` compares raw text: expect a respelled-token case to fail; if none exists in this file, add
  `RespelledTokenAtSecondHeadAdopts` first.
- [ ] **Step 6: Commit,** then the gate and the ASan milestone. Subject:
  `Reuse a CAS blob after HEAD, intent GET, HEAD on a learned mount`

### Task 12: The switch: checks, stamp, free lease {#task-12}

Implementer `ca-surgeon`. Reviewer `ca-review`. Read §7.2, §7.3, §7.5. This is the body of the operator command
as a `Pool` function; the SQL wiring is Task 17. Nothing calls it in production yet.

**Files:**
- Modify: `CA/Pool/CasPool.h`, `CA/Pool/CasPool.cpp` (new function, near `raiseReaderFloor`, `:628-640`)
- Modify: `G:5185-5282` (`acquireOrRenewLease`: the owner-0 branch)
- Test: create `T/gtest_cas_blob_intent_switch.cpp`

**Interfaces:**
- Consumes (Task 2): `PoolMeta::switchToBlobIntents` and its `BlobIntentsSwitch`, `GcState::generation`,
  `MountLease::protocol_generation`. (Task 3): `Layout::maxPoolPrefixBytesForBlobIntents`. (Task 6):
  `blobMarkProbe`, `applyBlobIntentsObservation`. (Task 5): `MarkLiarBackend`. (Task 8): the fixtures,
  `DialectOverrideBackend`. Existing:
  `Cas::listMounts` and `MountInfo::lease` (`CA/Pool/CasServerRoot.h:512-524`), `CasOperation::readModifyWrite`
  (`Q:1166`), the catalog's server roots.
- Produces:

  ```cpp
  struct EnableBlobIntentsResult { bool stamp_written = false; bool switch_written = false; };
  /// Checks, stamp, switch, learn. Every step is monotone; a rerun completes it.
  EnableBlobIntentsResult Pool::enableBlobIntents();
  /// Steps 3 and 4 alone: the switch and the learning. Shared with the new-pool sequence.
  bool Pool::switchToBlobIntentsAndLearn();        /// `BlobIntentsSwitch::written` of its switch
  ```

  `switch_written` is `BlobIntentsSwitch::written`: a switch that the retry's read resolved reports false.

- Rules the signatures do not state:
  - Checks, in this order, each a refusal that writes nothing: read-only mount (`INVALID_STATE`); probe not `Ok`
    (`NOT_IMPLEMENTED`: `blob mark probe failed on this disk: <step>`; a skipped probe gives the step `skipped`,
    also when the mount stores it as `Assumed`; for `FalsePresence` the false-presence text); prefix over the
    limit; a fresh exact read of `_pool_meta` with `min_reader_generation < 4` (`INVALID_STATE`) or with another
    recorded dialect (`BAD_ARGUMENTS`, the dialect text; no stamp is written); a mount slot that declares
    `protocol_generation < 4` or does not decode, unless its owner is a verified retirement (`INVALID_STATE`, the
    slot text of §7.5, naming the `server_root_id`). Slots: exact reads for the roots the catalog names, plus one
    `LIST` of `gc/server-roots/`. Texts without a §7.5 row: [Spec questions](#spec-questions), Q6.
  - Stamp: `readModifyWrite` of `gc/state`. Generation 4 or above: no write. Below 4: the same fields with
    generation 4. Absent: `GcState` with `gc_shards` from `_pool_meta`, `lease = {0, 0}`, generation 4. Confirmed
    by `orThrow`: an answered write or the resolve read. An exhausted policy throws, and nothing else was written.
  - Switch: `PoolMeta::switchToBlobIntents`, only after the stamp is confirmed; then
    `applyBlobIntentsObservation` on the meta it returned. An unconfirmed switch throws and this server does not
    learn.
  - One log line at the stamp and one at the switch.
  - Free lease: in `acquireOrRenewLease`, a decoded `gc/state` whose `lease.owner` is 0 is taken as an absent one
    is: `next.lease = {gc_id, current.lease.seq + 1}`, every other field kept, with or without `allow_steal`,
    before the heartbeat read (`G:5228`).

- [ ] **Step 1: Write the failing tests.** Upgraded pools: `seedUpgradedPoolForTest`, then `Pool::open` with the
  probe running.
  - `CASBlobIntentSwitch`: `StampsThenSwitchesThenLearns` (in the journal the `gc/state` write precedes the
    `_pool_meta` write; the pool is learned); `RerunWritesNothing`; `CommandRefusesUnderSkipAccessCheck`;
    `RefusesOnFalsePresence`; `RefusesOnAFailedProbeStep`; `RefusesALongPrefix`; `RefusesASlotBelowFour` (the text
    names the root); `RefusesAnUndecodableSlot`; `VerifiedRetirementDoesNotBlock`;
    `SlotOfARootWithoutNamespacesIsFoundByTheList`; `UnconfirmedStampWritesNothingElse`;
    `StampResolvedByReadIsConfirmed` (`LandedButAckLostOnceBackend`); `LostSwitchAnswerIsResolvedByRead`;
    `TwoCommandsConverge`; `StampKeepsEveryOtherFieldOfGcState`; `StampOverGenerationFourWritesNothing`;
    `RoundInFlightLosesItsCommitAndTheNextKeepsGenerationFour`; `RefusesOnAReadOnlyMount`;
    `RefusesBelowReaderGenerationFour` (the floor of `_pool_meta` is rewritten to 3 by hand after the mount);
    `RecordedOtherDialectIsRefusedBeforeTheStamp` (the journal holds no `gc/state` write);
    `TwoCommandsWithTwoDialectsOneWins` (the second mount opens the store through a `DialectOverrideBackend`;
    both run the command: one switch is written, the other command gets the dialect text, and `_pool_meta`
    records the winner's dialect); `SwitchResolvedByReadReportsNotWritten`. Each refusal asserts zero writes.
  - `CASGcIntentsMode.StampCreatedStateIsAFreeLease`: on a pool whose `gc/state` the stamp created, a manual
    round (`allow_steal = false`) leads at once.
- [ ] **Step 2: Run red.** Add the function throwing `NOT_IMPLEMENTED`.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run green,** then `CAS*` on `build` (tag `t12_pre`). No existing test changes.
- [ ] **Step 5: Two mutants,** tags `t12_m1`, `t12_m2`: (1) the switch runs before the stamp: expect
  `StampsThenSwitchesThenLearns` and `UnconfirmedStampWritesNothingElse` to fail; (2) the owner-0 branch is
  removed: expect `StampCreatedStateIsAFreeLease` to fail.
- [ ] **Step 6: Commit,** then the gate. Subject: `Add the pool step that switches a CAS pool to blob intents`

### Task 13: New-pool sequence in `Pool::open` {#task-13}

Implementer `ca-surgeon`. Reviewer `ca-review`. Read §7.4. The sequence runs for a pool that records
`created_generation = 4`. No creation records it until Task 15, so tests seed state N2.

**Files:**
- Modify: `CA/Pool/CasPool.cpp:607-613` (after `mountWritable` returns, before `open` returns)
- Test: create `T/gtest_cas_blob_intent_new_pool.cpp`

**Interfaces:**
- Consumes (Task 12): `Pool::switchToBlobIntentsAndLearn`. (Task 6): the probe result and the rule. (Task 8):
  `seedNewPoolAtN2ForTest`, `seedUpgradedPoolForTest`. (Task 5): `MarkLiarBackend`
  (`T/cas_blob_mark_test_support.h`).
- Rules:
  - A writable `Pool::open` that read `created_generation == 4` on a pool that is not switched runs, after
    `mountWritable` returned: the prefix check (`BAD_ARGUMENTS`, the prefix text); the probe check: a
    `FailedPreservation` or `FailedTokenForm` result throws `NOT_IMPLEMENTED` with `blob mark probe failed on
    this disk: <step>` (a store on which every reuse uploads again and nothing is reclaimed must not come up
    silently); `Skipped` proceeds as `Assumed` with one INFO line; `FalsePresence` proceeds with a WARNING;
    then `switchToBlobIntentsAndLearn`. No stamp, no slot check, no `gc/state` request.
  - Any failure throws out of `open`. The `PoolPtr` is destroyed by the exception, so the mount says its normal
    farewell. No mount is admitted on an unswitched pool with `created_generation = 4`.
  - `Pool::openForDecommission` (`CA/Pool/CasPool.cpp:1102`) does not run the sequence.
  - A pool without `created_generation` is never switched by `open`.

- [ ] **Step 1: Write the failing tests,** suite `CASBlobIntentNewPool`: `CrashAfterPoolMetaResumes` (seeded N2:
  the open ends switched and learned); `LostSwitchAnswerResumes`; `FailedSwitchFailsTheMountAndRestartResumes`
  (the first open throws the request error and leaves no admitted pool; the second ends switched);
  `FailedProbeFailsTheMount` (two cases: a preservation step, and the token form P7; each throws
  `NOT_IMPLEMENTED` naming the step and leaves `_pool_meta` unswitched);
  `SkippedProbeSwitchesWithTheMarkAssumed` (`blobMarkProbe().text() == "assumed"`);
  `SequenceSendsNoGcStateRequest` (the journal has no `gc/state` key); `LongPrefixFailsTheMount`;
  `DecommissionOpenDoesNotRunTheSequence`; `UpgradedPoolIsNotSwitchedAutomatically`.
- [ ] **Step 2: Run red.** Expected: the pool stays unswitched after `open`.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run green,** then `CAS*` on `build` (tag `t13_pre`). No existing test changes.
  `openSwitchedPoolForTest` now finds the pool switched by the open; its own switch step is a no-op.
- [ ] **Step 5: One mutant,** tag `t13_m1`: a failed switch is logged and the open returns. Expected:
  `FailedSwitchFailsTheMountAndRestartResumes` fails.
- [ ] **Step 6: Commit,** then the gate and the ASan milestone. Subject:
  `Switch a CAS pool created at generation 4 on its first writable mount`

### Task 14: Tools: `cas-fsck`, `cas-inspect`, `cas-gc-dryrun` {#task-14}

Implementer `ca-surgeon`. Reviewer `ca-review`. Read §3.4, §7.7. `cas-fsck` gets its first mutating options;
they delete only what F3 and F4 allow.

**Files:**
- Modify: `CA/Tools/CasFsck.h:92-184` (`FsckReport`), `:194-250` (the hard-findings comment), `:268`
  (`runFsck`), `CA/Tools/CasFsck.cpp:443-448`, `:888-914` (listing), `:1206-1222` (pairing), `:1323`
  (`formatFsckSummary`); `programs/disks/CommandFsck.cpp:22-58`
- Modify: `src/Interpreters/InterpreterSystemQuery.cpp:2637` (`contentAddressedFsckColumns`), `:2688`
  (`appendContentAddressedFsckRow`): row 10 of [Shared code changes](#shared-code); `Q/05020_cas_fsck.reference`
  if it prints the columns (predicted, not run)
- Modify: `CA/Tools/CasInspect.cpp:413-418`, `:558-564`, and its renderers of `_pool_meta`, the mount slot and a
  body
- Modify: `G:5061` (`previewDeletes`), `CA/Gc/CasGc.h:497-513`; `programs/disks/CommandCaGcDryRun.cpp:42-44`
- Modify: `CA/Pool/CasPool.cpp:445-446` (the comment: a read-only open never writes, and deletes only under an
  explicit `--fix`)
- Test: create `T/gtest_cas_fsck_intents.cpp`; modify `T/gtest_cas_fsck.cpp`, `T/gtest_cas_inspect.cpp`

**Interfaces:**
- Consumes (Task 1): `sameIncarnation`, `Etag::canonical`. (Task 2): `BlobProtocol`, `PoolMeta` fields. (Task 3):
  `blobIntentsPrefix`, `parseBlobIntentKey`, `writeOnceBlobIntentKey`, `isBlobIntentRecord`. (Task 9):
  `Gc::readRoundBlobProtocol`, for `previewDeletes` only. (Task 8): the fixtures, `DialectOverrideBackend`.
  Existing: `removeChunkWriteOnceOrOneByOne` (`G:541`), the tool's own operation (`CA/Tools/CasFsck.cpp:447`),
  `CasOperation::removeCurrent` (`Q:656`).
- Produces:

  ```cpp
  enum class FsckFix : uint8_t { None, StaleIntents, LegacyMarkers };
  /// FsckReport: new counters
  uint64_t intent_current = 0, intent_stale = 0, intent_malformed = 0, intent_unread = 0, legacy_marker = 0;
  uint64_t intents_deleted = 0, legacy_markers_deleted = 0;
  bool intents_processed = false;          /// F0 admitted this run to F1..F3
  /// runFsck: one more parameter, FsckFix fix = FsckFix::None
  /// cas-fsck: option --fix <stale-intents|legacy-markers>
  ```

- Rules the signatures do not state (F0 to F6 of §3.4 are the authority):
  - F0: one exact `GET` of `_pool_meta` before any listing or read of an intent; a failed read stops the run. The
    admission is decided once: a run admitted on an unswitched pool lists, reads and deletes no intent for its
    whole life. `intents` with another dialect stops the run with the dialect text. `runFsck` makes this read
    itself, through the tool's own operation: it has no `Gc` and no lease, and it must not call
    `Gc::readRoundBlobProtocol`, whose dialect branch marks the mount as having left GC.
  - F2: per listed key: exact `GET` of the intent, and only after its answer `HEAD` of the body. Classes as in
    §3.4. A respelled token of the same incarnation is `intent_current`.
  - F3: a fix deletes only `intent_stale` keys, each on the two reads of this run in that order, with the
    write-once verb in chunks. F4: `op.removeCurrent` per `.meta`; refused with `BAD_ARGUMENTS`, `markers are
    protocol state until the pool uses blob intents`, on a pool that is not switched.
  - F5: on a switched pool `body_without_meta` and `meta_without_body` are not computed (left 0); every `.meta`
    is `legacy_marker`; intents count in the class totals.
  - F6: a fix run sends deletes and no write, on the read-only open. A refused or unanswered delete stops the run
    with the error and the count deleted so far.
  - **Every new class is advisory.** `intent_current`, `intent_stale`, `intent_malformed`, `intent_unread` and
    `legacy_marker` are not hard findings: `kFsckHardFindings` keeps its 5 rows (`CA/Tools/CasFsck.h:243`),
    `FsckReport::clean` and the exit set of `CommandFsck::executeImpl` do not change. Reason: each is a state
    the protocol leaves by design or a read that proved nothing. Say so in the comment of each field. The
    counts appear on the two rendering surfaces: the `formatFsckSummary` line and the `SYSTEM CAS FSCK` row.
  - `SYSTEM CAS FSCK` reports the new counters, one `UInt64` column each, appended after the existing columns,
    and has no fix.
  - `cas-inspect`: renders an intent key (`parseBlobIntentKey`, record valid or not), a body's mark, the three
    `_pool_meta` fields, a slot's `protocol_generation`, a row's `intent`; the "unrecognized key layout" text
    lists `blob-intents`.
  - `previewDeletes`: calls `readRoundBlobProtocol(op, state)` with the `gc/state` it decoded (`G:5061-5068`)
    and stops on the same failures as G1. In intent mode it previews only rows with `pending` and `intent` as
    `delete_pending`; an inherited pending row is `awaiting_graduation`.

- [ ] **Step 1: Write the failing tests.** Intents are made by GC rounds on `openSwitchedPoolForTest`.
  - `CASFsckIntents`: `ClassesCurrentStaleMalformedUnread`; `FixReadsTheIntentBeforeTheBody` (a hook publishes a
    new body and lets GC confirm between the two reads: the intent is not deleted);
    `FixNeverDeletesFromTheListingAlone`; `FailedReadIsNeverStale`; `UncanonicalHeadTokenIsUnread`;
    `RespelledTokenIsCurrentNotStale`; `RefusedDeleteStopsTheRun`; `FixRefusesLegacyMarkersOnAnUnswitchedPool`;
    `LegacyMarkersAreDeletedWithHeadThenDelete`; `UnswitchedRunTouchesNoIntent`;
    `MetaPairingIsNotComputedOnASwitchedPool`; `IntentsCountInTheClassTotals`; `FixRunSendsNoWrite`. The failure
    cases assert that no key outside `intent_stale` was deleted.
  - `CASBlobIntentDialect`: `FsckOnAReadOnlyOpenStops`; `FsckStartedBeforeTheSwitchTouchesNoIntent` (the pool is
    switched in another dialect during the run); `DryRunStops`. Each asserts zero delete and zero intent
    requests.
  - `CASGcIntents.DryRunFollowsTheModeAndShowsNoInheritedDelete`.
  - `CASFsckSummary.IntentClassesAreAdvisoryAndRendered`: a report with each new counter set is `clean`, and
    the summary line names each counter.
  - `CASInspect`: `RendersAnIntentKey`, `RendersTheBlobProtocolFields`, `RendersARowIntent`.
- [ ] **Step 2: Run red.** Add the report fields, the parameter and the option, with no behaviour.
- [ ] **Step 3: Implement.** In `T/gtest_cas_fsck.cpp`, `BodyWithoutMetaIsBenign` and
  `MetaWithoutBodyIsAdvisoryNotHard` move to `seedUpgradedPoolForTest`.
- [ ] **Step 4: Run green,** then `CAS*` on `build` (tag `t14_pre`).
- [ ] **Step 5: Two mutants,** tags `t14_m1`, `t14_m2`: (1) the fix reads the body before the intent: expect
  `FixReadsTheIntentBeforeTheBody` to fail; (2) a failed `HEAD` classifies as stale: expect
  `FailedReadIsNeverStale` to fail.
- [ ] **Step 6: Commit,** then the gate and the ASan milestone. The body says why the shared interpreter file
  changes: the result columns of `SYSTEM CAS FSCK`. Subject:
  `Teach cas-fsck, cas-inspect and the GC dry run about blob intents`

### Task 15: New pools are created switched {#task-15}

Implementer `ca-surgeon`. Reviewer `ca-review`. Read §1, §3.1, §3.2 (last bullet), §7.1, §7.4, and
[Effects on tests](#test-effects). After this task a pool created by this build ends switched, and a mount that
saw the switch without learning never adopts. The writer's marker path stays for a mount whose last read showed
no switch; Task 16 removes it.

**Files:**
- Modify: `CA/Pool/CasPoolMeta.cpp:169-174` (the mint sets `created_generation`)
- Modify: `W:274-281` (the dispatch of `ensureBlobPresent`), `CA/Pool/CasPartWriteTxn.h:84-105` (`NotLearned`
  comes; `Condemned` stays)
- Test: `T/gtest_cas_blob_intent_writer.cpp`, `T/gtest_cas_blob_intent_new_pool.cpp`, and the fleet below

**Interfaces:**
- Consumes (Task 6): `Pool::blobIntentsLearned`, `observedBlobProtocol`. (Task 11): `BlobPublicationReason`,
  `ensureBlobPresentByIntents`. (Task 13): the new-pool sequence. (Task 5): `MarkLiarBackend`. (Task 8): the
  fixtures.
- Produces: `BlobPublicationReason::NotLearned` (event word `not_learned`).
- Rules:
  - `createOrValidate` mints `_pool_meta` with `created_generation = G_BLOB_INTENTS` on every mint. A test helper
    that mints through it (`seedPoolMetaForRestart`, `T/cas_test_helpers.h:1082`) therefore makes a pool of this
    build, and the first writable `Pool::open` switches it.
  - `ensureBlobPresent` after W1, in this order: a learned mount: `ensureBlobPresentByIntents`. A mount that is
    not learned and whose `observedBlobProtocol` is `Intents`: W2 (`HEAD` and the size checks); a present or an
    absent body both go to W6, one publication, unmarked (the bit was false at staging), reason `NotLearned`
    for a present body and `Absent` otherwise. It never adopts and sends no `.meta` and no intent request.
    Any other mount: today's marker path.
  - Why in this commit: from here a production pool can be switched. A later mount of it whose probe fails
    preservation is not learned (Task 6); on the marker path it would adopt a body on a `Clean` marker while
    intent-mode GC deletes that body.

- [ ] **Step 1: Write the failing tests.**
  - `CASBlobIntentNewPool`: `EndsSwitchedBeforeTheFirstWriter` (an empty prefix: after `open` the pool is switched
    and learned, and the journal holds no blob key); `ManualGcRoundLeadsRightAfterCreation`;
    `ConcurrentCreatorsConverge`; `CreatorWithAnotherDialectIsRefused` (the second creator opens the store
    through a `DialectOverrideBackend`).
  - `CASBlobIntentWriter.SwitchSeenButNotLearnedNeverAdopts`: a pool created and switched by one mount; a second
    mount over a `MarkLiarBackend` that fails P1 is not learned. Its writer over a present marked body: the
    journal is `HEAD`, `PUBLISH unmarked`; no `.meta` request, no intent request, reason `not_learned`.
- [ ] **Step 2: Run red,** then `CAS*` on `build` once with the product change applied (tag `t15_fleet`) and save
  the list of red tests to `$R/logs/t15_fleet_red.txt`.
- [ ] **Step 3: Classify before editing.** Write a table into the report: every red test, its file, its class,
  the action. Classes and actions:

  | Class | Sign | Action |
  |---|---|---|
  | P. Writer suite | a red test of one of the six writer files below | pin its pool with `seedUpgradedPoolForTest(*backend, config.pool_prefix, config.gc_shards)`, nothing else. It must turn green: the pool is upgraded and the marker path exists |
  | C. Marker-mode GC, not yet pinned | a GC test whose assertion names a marker | pin it the same way, unchanged otherwise |
  | H. GC shape on a default pool | counts a round's requests, phases or counters and names no marker: the phase `meta_pool_wait` is gone, `CASGCMetaOps` stops moving, a reclaimed blob costs 5 requests where it cost 8 | edit to intent mode: phases without `meta_pool_wait`; `HEAD`; intent `PUT`; `HEAD`, body `DELETE`, intent `DELETE` |
  | R. Restart seed | seeds through `seedPoolMetaForRestart` (about 55 call sites in 20 files) and then fails at the mode read or at the sequence | the pool is now of this build. A raw `gc/state` it seeds must have header 4 (`encodeGcState` of a constructed `GcState`), else the round fails with the generation text of §7.5. If the test's point is marker mode, pin it as class C |
  | B. Request shape outside the writer suites | counts `HEAD`, `PUT`, `GET` on a blob or `.meta` key, or attempts | edit to the ledger of §1: new content `HEAD`, `PUT`; clean reuse `HEAD`, intent `GET`, `HEAD` |
  | E. Object census | counts objects or keys under `blobs/`, or expects a `.meta` beside a body | edit: no `.meta` on a pool of this build; intents exist only for condemned bodies |
  | F. Pool bootstrap | expects a new pool unswitched, or its first `_pool_meta` bytes | edit: a new pool ends switched; states N0 to N3 |
  | G. Anything else | — | stop and report; do not edit |

  The six writer files, with tests and marker references at the base (`grep -c
  'blobMetaKey\|MetaState\|loadMetaForTest\|writeMetaClean\|condemnMeta\|CASMeta\|CASGCMeta\|CASGCCondemnMarker\|\.meta'`):
  `T/gtest_cas_part_write.cpp` (73, 73), `T/gtest_cas_gc_ack_floor.cpp` (35, 33),
  `T/gtest_cas_upload_detached.cpp` (14, 29), `T/gtest_cas_upload_fanout.cpp` (17, 28),
  `T/gtest_cas_s3_staging.cpp` (33, 13), `T/gtest_cas_protocol_scenarios.cpp` (15, 11). Other expected files,
  classes B, E, F, H and R: `T/gtest_cas_pool.cpp`, `T/gtest_cas_bootstrap_ordering.cpp`,
  `T/gtest_cas_observability.cpp`, `T/gtest_cas_pluggable_hash.cpp`, `T/gtest_cas_ref_writer.cpp`,
  `T/gtest_cas_fence_generation.cpp`, `T/gtest_cas_blob_publish_transport.cpp`, `T/gtest_cas_gc_log.cpp`, and
  the GC suites that Task 8 did not pin. `T/gtest_cas_blob_meta.cpp` and `T/gtest_cas_blob_meta_format.cpp` do
  not change: the codec stays.
- [ ] **Step 4: Apply the table.** Then filter the edited files, then `CAS*` on `build` (tag `t15_pre`). Write
  the list of class P pins into the report by file and test: Task 16 removes them.
- [ ] **Step 5: Two mutants,** tags `t15_m1`, `t15_m2`: (1) a mount that saw the switch without learning takes
  the marker path: expect `SwitchSeenButNotLearnedNeverAdopts` to fail; (2) the mint omits
  `created_generation`: expect `EndsSwitchedBeforeTheFirstWriter` to fail.
- [ ] **Step 6: Commit,** then the gate. Subject: `Create CAS pools switched to blob intents`

### Task 16: Remove the writer's marker path {#task-16}

Implementer `ca-surgeon`. Reviewer `ca-review`. Read §1, §3.2 whole, §7.1 (stage T1), and
[Effects on tests](#test-effects). After this task the writer has no `.meta` request on any path, and a writer
that is not learned publishes fresh and never adopts.

**Files:**
- Modify: `W:274-570` (the marker path goes: `loadMeta`, `reconcileMetaClean`, `validateMetaSize`, `meta_key`),
  `CA/Pool/CasPartWriteTxn.h:84-105` (`Condemned` goes), includes of `CA/Pool/CasBlobMeta.h` in the writer
- Modify: `src/Common/ProfileEvents.cpp:968-970` (remove `CASMetaCreateClean`, `CASMetaAbsentRepublish`,
  `CASMetaResurrectClean`) and their increments
- Test: `T/gtest_cas_blob_intent_writer.cpp` and the writer suites below

**Interfaces:**
- Consumes (Task 15): the branch of a mount that saw the switch without learning; its report's list of class P
  pins. (Task 6): `noteMarkedBodyWhileNotLearned`. (Task 11): `ensureBlobPresentByIntents`.
- Rules:
  - `ensureBlobPresent` is `ensureBlobPresentByIntents` for a learned mount and the not-learned branch of
    Task 15 for every other mount, whatever its last read showed. A not-learned mount whose last read showed no
    switch and that meets a marked body also calls `noteMarkedBodyWhileNotLearned`.
  - The writer contains no marker code: no `.meta` read, write or key on any path.
  - GC keeps its marker mode for pools that are not switched; `CA/Pool/CasBlobMeta.{h,cpp}` and the GC marker
    writer stay.

- [ ] **Step 1: Write the failing tests.**
  `CASBlobIntentWriter.UnlearnedWriterNeverAdoptsAndPublishesUnmarked` (on `seedUpgradedPoolForTest`: over a
  present body with a `Clean` marker the journal is `HEAD`, `PUBLISH unmarked`; no `.meta` request);
  `.UnlearnedWriterMeetingAMarkedBodyAsksForARereadOnce` (two marked bodies inside 10 s: one `_pool_meta` `GET`).
- [ ] **Step 2: Run red.** Then remove every class P pin of Task 15 from the six writer files: those tests go
  back to a default pool, which is switched and learned. Apply the product change and run `CAS*` on `build`
  once (tag `t16_fleet`); save the list of red tests to `$R/logs/t16_fleet_red.txt`.
- [ ] **Step 3: Classify before editing.** Write a table into the report: every red test, its file, its class,
  the action.

  | Class | Sign | Action |
  |---|---|---|
  | A. Writer marker test | asserts a `Clean` create, an absent-marker republish, a resurrect over `Condemned`, or one of the three removed counters | delete it if a `CASBlobIntentWriter` test covers the rule, and name that test; else re-target it to the intent rule and say which |
  | B. Request shape | counts `HEAD`, `PUT`, `GET` on a blob or `.meta` key, or attempts | edit to the ledger of §1: new content `HEAD`, `PUT`; clean reuse `HEAD`, intent `GET`, `HEAD` |
  | D. Marker-mode GC that needs writer reuse | pinned in Task 8, and expects a writer to adopt or to resurrect through the marker | re-target to intent mode on a default pool as a twin, or delete with the covering `CASGcIntents` test named |
  | G. Anything else | — | stop and report; do not edit |

  A deletion is reviewable only with its cover: the table names, per deleted test, the rule it proved and the
  test that proves it now. `T/gtest_cas_s3_staging.cpp`: the test of the copy over a present marker-less body
  is class A (that case ends, §3.2 "Verbatim staged copy").
- [ ] **Step 4: Apply the table.** Then filter the edited files, then `CAS*` on `build` (tag `t16_pre`).
- [ ] **Step 5: Stateless references.** `Q/05386` to `Q/05389` (`*_cas_publish_counts_*`) and
  `Q/05485_cas_ref_append_requests` select no blob counter: their references are expected unchanged; do not edit
  them. Report any stateless test under `Q/*_cas_*` that selects a removed counter
  (`grep -ln 'CASMetaCreateClean\|CASMetaAbsentRepublish\|CASMetaResurrectClean' $WT/$Q/*`).
- [ ] **Step 6: Two mutants,** tags `t16_m1`, `t16_m2`: (1) a writer that is not learned adopts on an absent
  intent: expect `UnlearnedWriterNeverAdoptsAndPublishesUnmarked` to fail; (2) the marked-body trigger is not
  called: expect `UnlearnedWriterMeetingAMarkedBodyAsksForARereadOnce` to fail.
- [ ] **Step 7: Commit,** then the gate and the ASan milestone. The body lists the three removed counters.
  Subject: `Publish CAS blobs without a birth marker`

### Task 17: `SYSTEM CAS ENABLE BLOB INTENTS` {#task-17}

Implementer `ca-impl`. Reviewer `ca-review-lite`. Read §7.2, §7.3. Row 7 of [Shared code changes](#shared-code).

**Files:**
- Modify: `src/Parsers/ASTSystemQuery.h:172-178` (`CAS_ENABLE_BLOB_INTENTS` after `CAS_FSCK`),
  `src/Parsers/ASTSystemQuery.cpp:304-307`, `src/Parsers/ParserSystemQuery.cpp:546-558` (the case list of
  `CAS_FSCK`: the disk is required)
- Modify: `src/Interpreters/InterpreterSystemQuery.h:81`, `src/Interpreters/InterpreterSystemQuery.cpp:1164-1169`,
  `:2792` (a sibling of `runContentAddressedFsck`), `:3685-3688` (required access, beside the `CAS_FSCK` case)
- Modify: `src/Access/Common/AccessType.h:372` (new line after it), `Q/01271_show_privileges.reference:177`
- Test: `add-test cas_enable_blob_intents.sh`

**Interfaces:**
- Consumes (Task 12): `Pool::enableBlobIntents`.
- Produces: `ASTSystemQuery::Type::CAS_ENABLE_BLOB_INTENTS`; access type
  `M(SYSTEM_CAS_ENABLE_BLOB_INTENTS, "SYSTEM CAS ENABLE BLOB INTENTS", GLOBAL, SYSTEM)`;
  `BlockIO InterpreterSystemQuery::runContentAddressedEnableBlobIntents(const String & disk_name)`.
- Rules: the grammar is that of `SYSTEM CAS FSCK <disk>` ([Spec questions](#spec-questions), Q7). The
  interpreter resolves the disk as `runContentAddressedFsck` does, refuses a disk that is not content-addressed
  with `BAD_ARGUMENTS`, calls `enableBlobIntents`, and returns one row: `stamp_written`, `switch_written`
  (`UInt8`). Exceptions of the pool step propagate unchanged.

- [ ] **Step 1: Write the stateless test** (a CAS disk over local object storage, as
  `Q/05485_cas_ref_append_requests.sh:17`). The disk is created by this build, so it is already switched: a
  stateless test cannot show the transition, which the gtests of Task 12 and the upgrade test of Task 19 cover.
  It pins the surface:
  - On a disk that does not set `skip_access_check`: the command prints `0	0` on the first run and on a
    rerun; `SELECT blob_protocol, blob_mark_probe FROM system.cas_mounts` prints `intents	ok`.
  - On a second disk with `skip_access_check`: the command fails with `NOT_IMPLEMENTED` and
    `blob mark probe failed on this disk: skipped`; `blob_mark_probe` is `assumed`.
  - The command on a disk that is not content-addressed fails with `BAD_ARGUMENTS`; `clickhouse-format`
    round-trips the query.

  Every printed value is "predicted, not run".
- [ ] **Step 2: Implement.** Build `unit_tests_dbms` on `build` (the parser and interpreter compile into it) and
  run `CAS*` (tag `t17_pre`).
- [ ] **Step 3: Commit,** then the gate. Subject: `Add SYSTEM CAS ENABLE BLOB INTENTS`

### Task 18: Transition gtests {#task-18}

Implementer `ca-impl`. Reviewer `ca-review`. Tests, plus at most one test hook in `Gc`. Read §7 whole and §2.4.
These are the unit-level echoes of the model rows `safe_transition` to `safe_late_old_requests` of §10.1.

**Files:**
- Create: `T/gtest_cas_blob_intent_transition.cpp`; modify `T/cas_blob_intent_test_support.h`

**Interfaces:**
- Consumes: the fixtures (Task 8), `Pool::enableBlobIntents` (Task 12), `ScopedReadCeiling`.
- Produces, in the support header:

  ```cpp
  /// The blob steps of a build that predates blob intents, by raw requests: HEAD; a present body with a `Clean`
  /// marker read after it is adopted; otherwise one unmarked publication and a `Clean` marker write.
  /// Returns true iff it adopted.
  bool olderBuildEnsureBlobForTest(const PoolPtr & store, const String & payload);
  /// Parks a round between its mode read and its fold. Released by the returned barrier.
  std::shared_ptr<ManualBarrier> parkRoundAfterModeReadForTest(Gc & gc);
  ```

  If the round has no hook between the mode read and the fold, add a test hook member to `Gc` in this task
  (`setAfterModeReadHookForTest`) and list `CA/Gc/CasGc.{h,cpp}` in the commit.

- [ ] **Step 1: Write the tests,** suite `CASBlobIntentTransition`. Upgraded pools start from
  `seedUpgradedPoolForTest`.
  - Stage T1: `MarkerModeRoundBeforeTheSwitchReclaims`; `NewWriterBeforeTheSwitchNeverAdoptsAndWritesNoMarker`;
    `OlderWriterRepublishesAMarkerlessBodyOnce`; `MarkerRoundCondemnsOnAnAbsentMarkerAndGraduates`.
  - Stage T2: `StampRefusesTheLeaseReadOfAnOlderBuild` (`ScopedReadCeiling(3)`: the text of §7.5, second row);
    `OlderBuildIsRefusedAtTheFloor`; `OlderRoundPastItsLeaseReadFailsItsCommit`;
    `SlotRewrittenWithoutTheKeyBlocksTheCommand`; `EpochLatchRefusesAnOlderAllocation`. This build knows the key
    `!protocol_generation`, so `ScopedReadCeiling(3)` cannot make it fail as an older build does. The test
    asserts two things: under `ScopedReadCeiling(3)` the allocation on a latched root is refused with the
    `checkCompatibility` text (`CAS server-epoch: object requires reader generation 4 but this build supports
    at most 3`); and the stored epoch body, read key by key with a `JsonObjectReader` that knows only
    `next_writer_epoch` and `allocation_nonce` and passes the rest to `skipUnknown`
    (`CA/Formats/CasTextFormat.cpp:315-322`), throws the critical-key text of §7.5. The second is what an
    older build runs.
  - Stage T3: `RoundThatSampledBeforeTheSwitchCondemnsAMarkedTokenAsACandidate`;
    `MarkerAuthorityDeleteAfterTheSwitchMissesARepublishedBody` (a parked marker-mode round holds a pending row of
    an unmarked token; a learned writer meets the body, publishes over it; the delete answers `Mismatch`);
    `PendingRowLeftByAMarkerRoundIsDemotedNotExecuted`; `CandidateRowAtTheSwitchGraduatesByIntent`;
    `LegacyBlobIsUploadedOnceThenReusedWithThreeReads`; `UnlearnedMemberAfterTheSwitchPublishesUnmarkedAndNeverAdopts`;
    `BlobStagedBeforeLearningIsPublishedUnmarked`; `MarkerOfALegacyBodyIsRemovedWithItsDelete`.
  - Each test ends with a `runFsck` report: zero dangling refs.
- [ ] **Step 2: Run.** Every test is expected green: the product rules exist. A red test is a product finding:
  stop and report it with the journal; do not edit the product in this task.
- [ ] **Step 3: One mutant per stage,** tags `t18_m1` to `t18_m3`, to prove the tests can fail: (1) the G8
  demotion is skipped: expect `PendingRowLeftByAMarkerRoundIsDemotedNotExecuted` to fail; (2) the stamp is
  skipped by the switch: expect `StampRefusesTheLeaseReadOfAnOlderBuild` to fail; (3) the mark bit is read at
  upload time: expect `BlobStagedBeforeLearningIsPublishedUnmarked` to fail.
- [ ] **Step 4: Commit,** then the gate and the ASan milestone. Subject:
  `Test the CAS marker to intent transition with mixed actors`

### Task 19: Integration and stateless tests {#task-19}

Implementer `ca-impl`. Reviewer `ca-review-lite`. Read §10.2 (rows "Stateless", "Integration"), §6.2 ("Paths and
where each is verified"). Written, not run; every number is "predicted, not run".

**Files:**
- Create: `I/test_cas_blob_intents/` (`__init__.py`, `test.py`, `configs/`), modelled on `I/test_cas_s3`
- Create: `I/test_cas_blob_intents_upgrade/`, modelled on `I/test_cas_ref_move_compat` (the same older image)
- Modify: `I/test_cas_s3/test.py`, `I/test_cas_gcs/test.py`, `I/test_cas_gcs/gcs_mocks/server.py`,
  `I/test_cas_gc_s3/test.py`
- Create with `add-test`: `cas_blob_intent_request_ledger.sh`, `cas_blob_intent_mounts_columns.sql`

**Tests:**
- `I/test_cas_blob_intents` (RustFS), with `HeadObject` through the S3 API on the blob key:
  `test_mark_on_single_put`; `test_mark_on_multipart_upload` (a body above 32 MiB);
  `test_mark_on_server_side_copy` (S3 staging); `test_mark_on_multipart_copy` (S3 staging, a body above 32 MiB);
  `test_unmarked_publication_reads_unmarked` (an upgraded pool before the switch, each of the four paths);
  `test_head_token_form` (the token of each path canonicalises: the recorded form per store).
- `I/test_cas_gcs`: the same for the paths GCS has (single, multipart upload, single copy); the mock stores and
  returns `Content-Type`. Two tests that use `.meta` writes as their vehicle move to the intent create. The
  budget test follows the ledger. State in the file header that a mock verifies the client side only.
- `I/test_cas_blob_intents_upgrade`, one test, in this order: the older image writes and runs GC; stop; the new
  image starts; two GC rounds reclaim in marker mode; the command refuses while a slot declares below 4 (the
  text names the root); after every server restarted, the command succeeds; a server of the older image is
  refused with the floor text; a legacy blob is uploaded once on its first reuse (`CASBlobReuseLegacyRepublish`
  grows by its blob count) and reused with three reads on the next (`CASBlobBodyPutAvoided`); `cas-fsck` reports
  zero dangling.
- `I/test_cas_s3`: marker counters go; one `HEAD` more per duplicate blob. `I/test_cas_gc_s3`: at most 5 requests
  per reclaimed blob (`HEAD`; intent `PUT`; `HEAD`, body `DELETE`, intent `DELETE`).
- Stateless `cas_blob_intent_request_ledger.sh`, on a CAS disk over local object storage, counters per query as
  `Q/05485_cas_ref_append_requests.sh` reads them and GC by `SYSTEM CAS GC RUN`. If a blob counter does not reach
  the query's `ProfileEvents` (uploads run on a pool thread), count the `system.cas_log` events `blob_put`,
  `blob_reuse_adopt`, `blob_reuse_resurrect` by `query_id` instead, as `Q/05217_cas_packed_parts.sh:195-200`
  does, and say so in the report. An `INSERT` of new content: per blob `CASBlobHead` 1,
  `CASBlobPut` 1, `CASBlobIntentGet` 0; a duplicate `INSERT`: per blob `CASBlobHead` 2, `CASBlobIntentGet` 1,
  `CASBlobPut` 0; after `DROP TABLE` and two GC rounds, the same `INSERT`: `CASBlobIntentGet` 1, `CASBlobPut` 1.
  The test prints ratios to the blob count of the part, so it does not depend on the column count.
- Stateless `cas_blob_intent_mounts_columns.sql`: `protocol_generation = 4`, `blob_protocol = 'intents'`,
  `blob_mark_probe = 'ok'` for this server's slot of a new CAS disk.

- [ ] **Step 1:** Write the tests. No `no-*` tag beyond what `Q/05485_cas_ref_append_requests.sh:1-3` carries for
  the same reason.
- [ ] **Step 2:** `python3 -m py_compile` on each new or changed `.py`; `bash -n` on each `.sh`.
- [ ] **Step 3: Commit,** then the gate (`ninja` has no work; the run records the tree). Subject:
  `Test the CAS blob mark per store, the upgrade and the request ledger`

### Task 20: Docs {#task-20}

Implementer `ca-impl`. Reviewer `ca-review-lite`. Read §1, §3, §7, §8.2, and [Effects on docs](#doc-effects).
User docs state present behaviour in plain words: no invariant tags, no rule numbers.

**Files** (under `D/` unless a full path is given; the spec's `operations/bucket-requirements.md` is
`D/bucket-requirements.md`):
- `architecture/blob-protocol.md`: the publication sequence, the reuse rule (two `HEAD`s and the intent read),
  the writer-versus-GC race, the request budget table of §1; the section on the `.meta` sidecar becomes the
  note on pools that are not switched.
- `architecture/garbage-collection.md`: the mode read, condemn, graduation by intent, the delete step, the cost
  tables; marker mode as the mode of a pool that is not switched.
- `architecture/storage-layout.md`: `blob-intents/`, the key shape, the body mark.
- `architecture/correctness.md`, `architecture/design-history.md`, `architecture/backend.md` (the token
  identity and the contract "a body token never recurs at a key").
- `operations/migration.md`: a section "Upgrading a pool to reader generation 4" with the procedure of §7.2,
  both points of no return, what `SYSTEM CAS ENABLE BLOB INTENTS` checks and its refusals, and the risks of
  §7.8 in plain words.
- `configuration.md`: mixed versions on one pool are a precondition of the switch; `skip_access_check` and the
  assumed mark.
- `bucket-requirements.md`: `Content-Type` returned on `HEAD` and kept on copy; write and delete rights under
  `blob-intents/`, batch delete where the store has it; one token dialect per pool across all clients and tools.
- `operations/monitoring.md` (the new and removed counters of §9.2, the three `system.cas_mounts` columns),
  `operations/troubleshooting.md` (a pool that never reuses: `blob_mark_probe`, `CASBlobReuseLegacyRepublish`
  that keeps growing, `CASBlobReuseTokenRejected`; a mount that left GC), `operations/debugging.md`
  (`cas-fsck --fix stale-intents`, `--fix legacy-markers`, the intent classes, `cas-inspect` on an intent key).
- Four reference pages outside `D/`, row 11 of [Shared code changes](#shared-code):
  - `docs/en/operations/system-tables/cas_mounts.md`: the columns `protocol_generation`, `blob_protocol`,
    `blob_mark_probe` and their values (§7.7).
  - `docs/en/operations/system-tables/cas_gc_log.md`: the phase `blob_protocol_mode` in every round;
    `meta_pool_wait` (its row at `:85` and the caveat at `:129`) in rounds of a pool that is not switched only;
    the `pending_deletes` metrics `intent_deletes`, `intent_delete_requests`, `legacy_marker_deletes`,
    `token_rejected`.
  - `docs/en/operations/system-tables/cas_log.md`: the `publication_reason` words `absent`, `intent_present`,
    `legacy_body`, `not_learned`, `token_rejected`, `changed_at_second_head`.
  - `docs/en/antalya/metrics_and_events.md`: the 18 new counters (4 `CASBlobIntent*` request counters, 4
    `CASBlobReuse*`, `CASBlobPublishDigestMismatch`, 9 `CASGC*`), each with the description of its
    `ProfileEvents.cpp` entry; the rows of `CASMetaCreateClean` (`:284`), `CASMetaAbsentRepublish` and
    `CASMetaResurrectClean` go.
- `$WT/docs/superpowers/cas/AGENTS.md` is not in this worktree: the decision-1 note goes to the controller's
  list in [Effects on backlog tasks](#backlog-effects).

- [ ] **Step 1:** Write the changes.
- [ ] **Step 2:** `grep -n '^#' <each changed file> | grep -v '{#'` prints nothing new (a heading this task did
  not add keeps its form). `grep -n 'I[1-8]\b\|W[1-8]\b\|G[0-9]\+\b\|F[0-6]\b' <each changed file>` shows
  no rule tag. `grep -c 'CASBlobIntent\|CASBlobReuse\|CASBlobPublishDigestMismatch\|CASGCIntent\|CASGCInherited\|CASGCLegacyMarker'
  docs/en/antalya/metrics_and_events.md` prints at least 18: one row per new counter.
- [ ] **Step 3: Commit,** then the gate. Subject: `Document CAS blob publication with deletion intents`

### Task 21: Yardstick references and soak cards {#task-21}

Implementer `ca-impl`. Reviewer `ca-review-lite`. Works in `M` on branch `cas-gc-rebuild`; the commit rules are
the same, with `git -C $M`. Read §10.2 (row "Soak"), §10.3.

**Files:**
- Modify: `$M/utils/cas-yardstick/yardstick/cards/cycle.py`, `wide_insert.py`, `mutation.py`, `dup_content.py`,
  `attach_200.py` (expected request shapes: no marker `PUT`; a second `HEAD` and an intent `GET` per reused
  blob), `$M/utils/cas-yardstick/tests/test_cards.py`, `$M/utils/cas-yardstick/README.md`
- Modify: `$M/utils/ca-soak/scenarios/cards/s15_s18_shards_lifecycle.py` (the S16 direct check, `:373-388`,
  reads the intent branch: `blob_reuse_resurrect` on "intent present"),
  `$M/utils/ca-soak/scenarios/cards/s41_wide_insert_baseline.py` (S41: marker `PUT`s 0)
- Create: a soak step that runs `cas-fsck --fix stale-intents` every 10 minutes while writers and GC run, and
  the end check: writers and GC stopped, one more fix run, then a report with `intent_stale = 0`, zero dangling
  refs, `CASBlobPublishDigestMismatch = 0`. Place it in `$M/utils/ca-soak/scenarios/framework/` beside
  `observe.py` and wire it into the 2-hour scenario with process kills.
- Not in this task: a new reference under `$M/utils/cas-yardstick/reference/`. The controller records it from a
  run on the finished branch ([Gates](#branch-gates)).

- [ ] **Step 1:** Edit the cards and the unit tests of the yardstick. Run
  `cd $M/utils/cas-yardstick && python3 -m pytest tests -q > $R/logs/t21_yardstick.log 2>&1`.
- [ ] **Step 2:** `python3 -m py_compile` on each changed soak file.
- [ ] **Step 3: Commit** in `M` by exact file list. Never `git add -A`: `utils/ca-soak` holds run artifacts.
  Subject: `Move the yardstick cards and soak checks to blob intents`

---

## Gates for the whole branch (controller) {#branch-gates}

In this order, each on the exact tree of the last commit, each log with its build marker.

1. `CAS*` on `build` (release) and on `build_asan`.
2. `--gtest_filter='S3*:WBS3*'` on both builds (the shared S3 change).
3. The stateless `*_cas_*` set (129 tests at the base plus the new ones) in three setups: tests that create
   their own CAS disk on an ordinary server; CAS as the default disk over local object storage; CAS as the
   default disk over S3. Every "predicted, not run" number of Tasks 14, 16, 17 and 19 is checked here; a
   difference is a finding, not a reference edit.
4. Fast-test compile (`build_fast`, no S3) and the style check (`ci/jobs/check_style.py`), including
   `failpoint_tests_are_isolated`.
5. Integration: `test_cas_blob_intents`, `test_cas_blob_intents_upgrade`, `test_cas_s3`, `test_cas_gcs`,
   `test_cas_gc_s3`, and the suites that count requests or reclaim: `test_cas_gc_sharded`,
   `test_cas_gc_bulk_delete`, `test_cas_shared_pool`, `test_cas_ref_move_compat`.
6. Measurements of §10.3, before (`a966a374b17`) and after: the request ledger per `INSERT` of new and of
   duplicate content; the yardstick cards with a new reference; a wide insert like the stand's (marker `PUT`s to
   0, upload thread time, wall time); a fully duplicated second insert on a legacy pool and a third; GC requests
   per reclaimed blob in both modes; CPU of the publication hash on a large body.
7. A 2-hour soak with process kills and the periodic fix step of Task 21: zero dangling refs, `intent_stale = 0`
   at the end, no `CASBlobPublishDigestMismatch`.

## Effects {#effects}

The plan-level tables of the earlier spec revision (`d2f130ebfef`, §9.1 to §9.3), corrected for the rule
changes since.

### Effects on backlog tasks {#backlog-effects}

Applied by the controller in `$M/docs/superpowers/cas/backlog/` after the branch gates; no implementer task
edits the backlog.

| Task | Effect |
|---|---|
| CAS-384, CAS-330, CAS-334, CAS-345, CAS-29.3, CAS-439 | moot: they exist for the writer's marker or its residuals |
| CAS-326 | moot after the switch: intents carry no round |
| DRAFT-8, CAS-30, CAS-331 | answered: a lazy warning keyed by incarnation; no shared marker to protect |
| CAS-360 | reversed: an absent warning now adopts, over an immutable per-incarnation object; recurrence is closed by CAS-361 and CAS-400 (`a966a374b17`) and stated as the store contract |
| CAS-361, CAS-400 | prerequisites, done. CAS-361's sentence on an empty result `ETag` is dropped: nothing consumes a publication token |
| CAS-197 | delivered by the token identity, except the recorded token form per store; a token outside the grammar is a counted residual, never an exception |
| DRAFT-53 | delivered for streamed publications; open for the verbatim copy |
| CAS-33.2, CAS-33.3 | unchanged; CAS-33.2 records that the class is the same under both protocols |
| CAS-329 | its request cut is delivered differently (8 to 5); "delete without `HEAD`" stays open |
| CAS-29.4, CAS-336, CAS-284.5 | re-target from marker reads and deletes to intent creates and deletes |
| CAS-10, CAS-11, CAS-289.6 | fsck and GC byte classes: intents replace `.meta`; the two pairing counters are not computed on a switched pool; new class `intent_unread` |
| CAS-5, CAS-43 | emitters move: `BlobReuseResurrect` on "intent present"; the raced-condemn count is the spare with a standing intent |
| CAS-18, CAS-17, CAS-321.7 | re-measure; CAS-18's text "the protocol is fixed" is amended |
| CAS-187, CAS-238, CAS-240, CAS-375, CAS-175, CAS-231 | texts, comments and model guidance follow the new sequence |
| decision-1, invariant 5 of `docs/superpowers/cas/AGENTS.md` | record: the `HEAD` before every `PUT` stays and a body is never created conditionally; a durable write step is removed and reads are added, as a protocol change with a model |
| New task | the offline `cas-gc-rebuild` under another dialect follows `Gc::rebuildBaseline` (Q8) |

### Effects on tests {#test-effects}

| Suite | Deleted | Changed | Added | Task |
|---|---|---|---|---|
| Writer gtests (`T/gtest_cas_part_write.cpp`, `gtest_cas_upload_detached.cpp`, `gtest_cas_upload_fanout.cpp`, `gtest_cas_s3_staging.cpp`) | `Clean` create, absent-marker republish, marker counters, the copy over a present marker-less body | adopt and republish fixtures, request shapes, attempt counts | `CASBlobIntentWriter.*` | 11, 15, 16 |
| GC gtests (`gtest_cas_gc_meta_writer.cpp`, `gtest_cas_gc_round.cpp`, `gtest_cas_gc_read_ahead.cpp`, marker tests of `gtest_cas_gc_ack_floor.cpp` and the others of Task 8) | none: marker mode stays | each runs on a pool as an older build leaves it and serves the mode read of each round | `CASGcIntents.*`, `CASGcIntentsMode.*` | 8, 9, 10 |
| `gtest_cas_blob_meta*.cpp` | none | pinned to the upgraded pool | — | 8 |
| `gtest_cas_fsck.cpp` | — | `BodyWithoutMetaIsBenign`, `MetaWithoutBodyIsAdvisoryNotHard` run on an upgraded pool; the summary line and the `SYSTEM CAS FSCK` row gain the advisory counters | `CASFsckIntents.*` | 14 |
| Pool bootstrap gtests (`gtest_cas_pool.cpp`, `gtest_cas_bootstrap_ordering.cpp`, `gtest_cas_pool_meta.cpp`) | — | a new pool ends switched; floor 4 | states N0 to N3, `CASBlobIntentNewPool.*` | 2, 13, 15 |
| Format gtests | — | generation pins 3 to 4 | the fields of §2.3 | 2, 3 |
| Integration | — | `test_cas_s3`, `test_cas_gcs`, `test_cas_gc_s3` | upgrade test; mark per path and store | 19 |
| Stateless | — | none expected (`05386` to `05389`, `05485` select no blob counter) | request ledger; `system.cas_mounts` columns; the command | 16, 17, 19 |
| GC request, phase and counter shape on a pool created by this build (`gtest_cas_gc_log.cpp`, the GC suites Task 8 did not pin) | — | no `meta_pool_wait` phase, `CASGCMetaOps` does not move, 5 requests per reclaimed blob where there were 8 | — | 15 |
| Fixtures seeded with `seedPoolMetaForRestart` (about 55 call sites in 20 files) | — | the pool is of this build and ends switched; a raw `gc/state` seed has header 4, or the test is pinned | — | 15 |
| `utils/cas-yardstick`, `utils/ca-soak` | — | cards `cycle`, `wide_insert`, `mutation`, `dup_content`, `attach_200` and the reference; soak cards S16 and S41 | a periodic `cas-fsck --fix stale-intents` step | 21 |

### Effects on docs {#doc-effects}

The file list is in [Task 20](#task-20). Corrections to the earlier list: `bucket-requirements.md` and
`configuration.md` are directly under `D/`; `operations/troubleshooting.md` is added; the migration section
states both points of no return and that a pool created by this build needs no command. Four reference pages
outside `D/` are added: the three `system.cas_*` table pages and `docs/en/antalya/metrics_and_events.md`.

## Spec items this plan settles {#settled}

| # | Item | Settlement |
|---|---|---|
| S1 | The mode bit of the emulated local mark (§6.2 leaves "an execute bit") | owner execute, `S_IXUSR` (`0100`). File creation uses mode `0666` (`src/IO/WriteBufferFromFile.h:37`) under the server `umask` `027` (`src/Daemon/BaseDaemon.cpp:265`), so no `umask` yields an execute bit; `chmod` ignores the `umask`, so the set is exact; the owner bit survives a tool that strips group and other bits |
| S2 | A token-form probe failure (P7) on a pool created by this build | withdrawn: the spec holds (§6.2, §7.4). A probe that runs and fails the token form fails the creating mount, as a preservation failure does: on such a store every reuse uploads again and nothing is reclaimed. Only false presence is a WARNING there |
| S3 | A later mount of a switched pool created by this build whose probe fails preservation or the token form | mounts not learned with a WARNING and `blob_mark_probe` naming the step (§6.2 as written); only the creating sequence fails on it |
| S4 | `blob_mark_probe` values (§7.7: "the failed step") | `ok`, `skipped`, `assumed`, or `P1` to `P7` |
| S5 | "The lease renewer every 60 s" against a renew period of 10 s (`CA/Pool/CasPool.h:229`) | a 60 s limit on the mount's boot clock, checked by a callback that `CasMountRuntime` runs after each committed renew of the background worker and of the direct call; 10 s for the writer's trigger |
| S6 | Where the "left GC" state lives (§3.3 names no owner) | on `Pool`: it outlives a `Gc` object and is read by the scheduler, the manual round and the rebuild |
| S7 | `cas_gc_log` phases | new phase `blob_protocol_mode` in every round; `meta_pool_wait` in marker mode only; the `pending_deletes` metrics of Task 10 |
| S8 | Publication reason words for events | `absent`, `intent_present`, `legacy_body`, `not_learned`, `token_rejected`, `changed_at_second_head` |
| S9 | Probe classification when several rows fail | preservation, then token form, then false presence |
| S10 | When `created_generation` is first written | by the mint of `createOrValidate`, from Task 15; before it the sequence is tested on seeded N2 pools |

## Spec questions {#spec-questions}

The spec is silent or ambiguous here. The plan takes the default; the controller may rule otherwise before the
task named.

| # | Question | Default taken | Task |
|---|---|---|---|
| Q1 | §10.1 requires the TLA+ models "before implementation". May Tasks 1 to 8, which add no protocol behaviour, start before the model results exist? | No: no task starts before the results file is recorded under `$M/docs/superpowers/models/` ([Before Task 1](#before-task-1)) | all |
| Q2 | Commits 11 to 15 keep today's marker path for a mount that is not learned, which §7.1 (T1) does not describe | Accepted inside the branch history only: no production path switches a pool before Task 15; Task 15 stops a mount that saw the switch from adopting; Task 16 removes the path | 11, 15, 16 |
| Q3 | §2.3: the slot carries "4 in every body this build writes". Does a slot that this build's GC fences over an older build's body declare 4? | No. The slot holder writes its own `protocol_generation`; a fence copies the value it decoded, as it copies `write_attempt_id`, and never raises it. So the declaration keeps telling the owner's build, and the command keeps naming that root | 2 |
| Q4 | §2.1 names four `CASBlobIntent*` counters; the instrumented backend has 11 operation classes per namespace | The mapping of Task 3; operations never sent on an intent key count as `CASOther*` | 3 |
| Q5 | §6.2 lists no copy for the in-memory backend, but `InMemoryBackend::publish` has a verbatim branch (`CA/Backend/CasInMemoryBackend.cpp:235`) | The copy stores the mark of the request, as the multipart copy takes the type of its create | 5 |
| Q6 | §7.5 is the only registry of refusal texts and has none for "the disk is not mounted writable" and "`min_reader_generation < 4`" | `INVALID_STATE`: `SYSTEM CAS ENABLE BLOB INTENTS needs a writable mount of disk '<disk>'`; `INVALID_STATE`: `pool is at reader generation <n>, need 4` | 12 |
| Q7 | §7.3 writes the disk as a quoted literal; `SYSTEM CAS FSCK` and its siblings take the disk through `parseQueryWithOnClusterAndTarget` | The grammar of `SYSTEM CAS FSCK`, `ON CLUSTER` included: each server's run is monotone and two runs converge | 17 |
| Q8 | The offline `cas-gc-rebuild` under another dialect is not specified; only `SYSTEM CAS GC REBUILD` is | Both call `Gc::rebuildBaseline`, which makes the check | 9 |
| Q9 | §3.2 does not name a counter for "present body, writer not learned" | None added; the event reason `not_learned` carries it | 15 |
| Q10 | §7.4 does not say what happens to the lease when the sequence throws after `mountWritable` | The exception destroys the `PoolPtr`; the mount says its normal farewell | 13 |
| Q11 | §9.2 and the earlier doc list name `D/operations/bucket-requirements.md`; the file is `D/bucket-requirements.md` | The existing file | 20 |
| Q12 | §10.2 puts the GCS paths on "the GCS emulator"; `I/test_cas_gcs` uses a Python mock that must be taught to keep `Content-Type` | Extend the mock and say in the test that it verifies the client side only | 19 |
| Q13 | §3.4 and §7.7 add five `cas-fsck` classes. `CA/Tools/CasFsck.h:194-250` requires each new class to be declared a hard finding or an advisory one | Advisory, all five: `kFsckHardFindings` keeps 5 rows and the exit code does not change; the counts show in the summary line and in the `SYSTEM CAS FSCK` row | 14 |
