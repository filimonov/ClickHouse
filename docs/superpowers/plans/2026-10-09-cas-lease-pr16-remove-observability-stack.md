# CAS lease PR 16: remove the thread-local renewal observability stack; sweep the counters — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:executing-plans (or
> superpowers:subagent-driven-development). Steps use checkbox (`- [ ]`) syntax.

**Goal:** a renewal returns why it ended as a value, and the runtime writes the `watermark_renew` event and its log
lines from that value. The thread-local `MountRenewObservabilityStack` and its two free functions go. Then the
`ProfileEvents` sweep: every CAS event that no code path can increment is removed, `CASMountRenewalDeadlineExceeded`
and the two deadline classifications go, and the counters the spec keeps get their names
(`CASMountReacquire*`, `CASMountLeaseExpired`).
**Architecture:** `MountRenewResult` gets `classification` plus the identity of the attempted body (`writer_epoch`, `seq`,
`write_attempt_id`, `confirmed_deadline_boot_ms`). `MountLeaseRenewer::renewOn` sets them where it calls
`markMountRenewTermination` today. `CasMountRuntime::consumeRenewResult` calls a new `reportRenewal(result)` where it
calls `reportMountRenewCompletion` today. The request counters lose the unreachable `Server` row and `Cas` column of
`cas_event_table`.
**Tech stack:** C++23, gtest (`unit_tests_dbms`), praktika integration, Python (soak cards, Grafana builder).

**Spec:** `/home/mfilimonov/workspace/ClickHouse/master/docs/superpowers/specs/2026-10-07-cas-mount-lease-series-design.md`
— §7 row 16 and test row «15, 16», §5 «Что удаляется» and the paragraph after its table. Backlog: CAS-2.1.

**Base:** `fix/antalya-26.6/cas-remove-remount-machine` (PR 15) at execution time (`a087959dfcb` at plan time).
**Branch:** `fix/antalya-26.6/cas-remove-observability-stack`. `$WT` = the executor's own worktree (never
`cas-janitor-budget`; read it only with `git show <head>:<path>`). `$CA` =
`src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed`. `$T` = `src/Disks/tests`. `$M` =
`/home/mfilimonov/workspace/ClickHouse/master`. Every commit message ends with:

```
Signed-off-by: Mikhail Filimonov <mfilimonov@altinity.com>
Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
```

**Files outside `$CA`:** `src/Common/ProfileEvents.cpp` (18 events removed, 4 added, 1 description); `docs/en/antalya/`
(`metrics_and_events.md`, `cas/operations/{monitoring,debugging,troubleshooting}.md`). Outside the PR, in `$M`
(branch `cas-gc-rebuild`, one separate commit): `utils/ca-soak/scenarios/cards/*.py`, `utils/cas-grafana/`.

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
- Every build, test and gate command runs with `TMPDIR=<build_dir>/tmp`.
- After every task: release and ASan `CAS*` green on the committed tree. A deletion task never leaves a red.
- A test is deleted only when this plan names it; a retarget that cannot express the original property stops the
  task and is reported.

## Review Focus

- **Failure asymmetry.** Removing a counter that can still move hides a real signal from every dashboard silently;
  keeping one that cannot move costs a row of noise. So a counter is removed only with a structural proof that no
  path increments it (no producer of its enum value, or no reference at all) — "never moved on otel.demo" is not
  proof (`tmp/otel_cas/f25.md` lists 99 such counters; most are live). The one exception is
  `CASMountRenewalDeadlineExceeded`, which the spec removes although it still moves (Code vs spec 1).
- **Event equivalence.** Task 3 must leave every `watermark_renew` row and log line byte-identical except
  `elapsed_ms` / `remaining_confirmed_budget_ms`, which now use the runtime's injected boot clock instead of a
  real `CLOCK_BOOTTIME` read (same value in production). Every existing event test stays unchanged in Task 3.
- **Dead gates in the soak cards.** Six cards read `CASRootCompareSwap`, which cannot move; `s06` gates on it being
  non-zero. Retargeting to `CASRootOverwrite` can turn a card that never really checked into one that fails.

## Code vs spec (flagged; the plan follows the spec except where stated)

1. **The deadline classifications still happen at startup.** Spec §5: `CASMountRenewalDeadlineExceeded`,
   `external_lease_deadline`, `request_deadline` "не могут произойти для фонового продления". True for the
   background renewal (`renewUntilDefinitive`). But the startup renewal (`renewRenewerForStartupOnce` →
   `MountLeaseRenewer::renew`, `Retry::untilLeaseSafe`) still ends this way in production:
   `CASLosslessClaim…` expects `Pool::open` to throw `"external_lease_deadline, no attempt sent"`. Plan: delete the
   counter and both names (spec), and name the startup ending `deadline` — a startup failure without a name would be
   `terminal_unclassified`, which the docs call a defect.
2. **`CASRemount*` → `CASMountReacquire*`.** Spec: they "остаются и переименовываются" here. Code: PR 15 deleted the
   four `CASRemount*` events. Plan: add `CASMountReacquireAttempts`, `…Succeeded`, `…Failed` fresh; no analog of
   `CASRemountHeldTransient` (a held reacquire step is a `Retry`, counted in attempts).
3. **`CASMountLeaseExpired`** is listed as remaining; it does not exist. Plan: add it, incremented where
   `warnOnceIfLeaseExpired` writes its one `WARNING`.
4. **CAS-2.1 option A or B** (`needs:decision`); the spec does not choose. Plan: A — delete the `Server` row.
   B would move mount/lease/epoch traffic out of `CASGC*`, which `utils/cas-grafana` and the GC cards read.
   A changes no live number.
5. **Scope beyond CAS-2.1.** The `CAS*CompareSwap` column (`CasOp::Cas` has no producer) and
   `CASConditionalWriteFenceLostPostWrite` (no reference outside `ProfileEvents.cpp`) are swept too.
6. **Size.** Spec −300; estimate ≈ −200 production. The stack is ≈ −290, the value-based emitter is ≈ +80, and the
   four new counters add ≈ +25.

## Inventory (as read at `a087959dfcb`; Task 1 re-verifies it at execution time)

| Item | Increment sites | Verdict | Reason |
|---|---|---|---|
| `thread_local MountRenewObservabilityStack` (`Pool/CasServerRoot.cpp`), `configureMountRenewObservability`, `reportMountRenewCompletion`, `markMountRenewTermination`, `MountRenewObservabilityCallGuard` | — | remove | outcome travels in `MountRenewResult` |
| `thread_local SplitCache tls_split_cache` (`Parts/PartPathParser.cpp`); `thread_local_rng` users | — | keep | a parse cache and an RNG, not observability |
| `CASServer{Put,PutDeduplicated,Overwrite,CompareSwap,CompareSwapConflict,Head,HeadMiss,Get,GetStream,Delete,List}` (11) | table row only | remove | `classifyCasNs` never returns `Server`; `/gc/server-roots/` is `Gc` |
| `CAS{Blob,Manifest,Root,GC,Other}CompareSwap` (5) | table column only | remove | no caller passes `CasOp::Cas` |
| `CASConditionalWriteFenceLostPostWrite` | none | remove; readers → `CASRequestFenceLostPostWrite` | same meaning, live |
| `CASMountRenewalDeadlineExceeded` | `consumeRenewResult`, startup only | remove (spec) | Code vs spec 1 |
| `CASMountLeaseLost` | `latchMountEnd`, `noteLeaseLost` | keep; description | one per end or fence decision |
| `CASMountRenewal{Attempts,Retries,Resolved,Recovered}` | `renewalEnvironment`, `consumeRenewResult` | keep | physical attempts |
| other 156 `CAS*` events | ≥ 1 site each | keep | out of scope unless Task 1 finds a dead site |
| — | — | add | `CASMountReacquire{Attempts,Succeeded,Failed}`, `CASMountLeaseExpired` |

Readers of removed names: four gtest files (Tasks 3–5), four docs pages, and in `$M` seven soak cards and the
Grafana drilldown (Task 7).

---

### Task 1: Re-verify the inventory at the execution head

**Files:** none committed. Output: `$M/tmp/lease-series/pr16-inventory.txt`.

- [ ] Producers of each `CasOp`/`CasNs` value: `git grep -n 'CasOp::\|CasNs::' -- $CA`. Expected: no `CasOp::Cas`,
  no `CasNs::Server` outside `CasInstrumentedBackend.{h,cpp}` declarations and the table.
- [ ] Every `CAS*` event without a non-declaration reference:
  ```bash
  grep -oP 'M\(\KCAS\w+' src/Common/ProfileEvents.cpp | while read e; do
    n=$(git grep -n "\b$e\b" -- src ':!src/Common/ProfileEvents.cpp' ':!src/Disks/tests' | grep -vc 'extern const'); echo "$n $e"; done | awk '$1==0'
  ```
  Expected exactly `0 CASConditionalWriteFenceLostPostWrite`.
- [ ] `git grep -n 'thread_local ' -- $CA` — expected the two rows of the table.
- [ ] For each mount event (`CASMount*`, `CASDataRootVanished`, `CASRefRecoveryCancelled`), name the enclosing function
  of each increment and one production caller of it. Write all results to the inventory file. **Stop and report** if
  any result differs from the table above.

### Task 2: Pin what must not move (golden, green before and after)

**Files:** `$T/gtest_cas_backend.cpp`, `$T/gtest_cas_event_log.cpp`.

- [ ] In `CASInstrumentedBackend.ClassifierAndPerNamespaceOpEvents`, after the `pool/gc/state` line, add:
  ```cpp
      /// The per-server control subtree is GC-owned state and is counted with it.
      EXPECT_EQ(classifyCasNs(Layout("pool").mountKey("srv")), CasNs::Gc);
      EXPECT_EQ(classifyCasNs(Layout("pool").epochKey("srv")), CasNs::Gc);
  ```
- [ ] In `RenewalEventBackend` add `bool land_then_throw_next_write = false;` and, in `write` after the
  `throw_nonretryable_next_write` branch:
  ```cpp
          if (std::exchange(land_then_throw_next_write, false))
          {
              (void)InMemoryBackend::write(key, bytes, expected_value, access);
              throw Poco::TimeoutException("injected renewal timeout after commit");
          }
  ```
  and the test (no test asserts `committed_by_read` today):
  ```cpp
  TEST(CASEvent, ARenewalProvedByItsResolvingReadIsCommittedByRead)
  {
      auto backend = std::make_shared<RenewalEventBackend>();
      auto boot_ms = std::make_shared<std::atomic<uint64_t>>(100);
      auto events = std::make_shared<DB::Cas::tests::SharedEventLog>();
      auto store = openRenewalEventPool(backend, boot_ms, renewalEventBudget(), "renewal-committed-by-read");
      store->setEventSink([events](CasEvent event) { events->push(std::move(event)); });
      backend->land_then_throw_next_write = true;
      EXPECT_NO_THROW(store->renewWatermarkOnce());
      const std::vector<CasEvent> renewals = watermarkRenewEvents(events->snapshot());
      ASSERT_EQ(renewals.size(), 1u);
      EXPECT_EQ(renewals[0].outcome, "recovered");
      EXPECT_EQ(renewals[0].detail.at("classification"), "committed_by_read");
      EXPECT_EQ(renewals[0].detail.at("attempts_sent"), "1");
      EXPECT_EQ(renewals[0].detail.at("seq"), "2");
  }
  ```
- [ ] Build, run `--gtest_filter='CASInstrumentedBackend.*:CASEvent.*'`: green (golden). If red, stop and report — the
  premise of the inventory is wrong.
- [ ] Commit `$T/gtest_cas_backend.cpp $T/gtest_cas_event_log.cpp`: `Pin the CAS server-root key class and the
  read-proved renewal event`.

### Task 3: Return the renewal classification as a value; delete the stack

**Files:** `$CA/Pool/CasServerRoot.{h,cpp}`, `$CA/Pool/CasMountRuntime.{h,cpp}`, `$T/gtest_cas_event_log.cpp`.
**Produces:** `MountRenewClassification`, `toString(MountRenewClassification)`, the new `MountRenewResult` fields,
`CasMountRuntime::reportRenewal`.

- [ ] Failing test first (`gtest_cas_event_log.cpp`; red on base: does not compile):
  ```cpp
  TEST(CASEvent, EveryRenewalClassificationHasItsEventName)
  {
      EXPECT_EQ(toString(MountRenewClassification::DeterministicFailure), "deterministic_failure");
      EXPECT_EQ(toString(MountRenewClassification::Conflict), "conflict");
      EXPECT_EQ(toString(MountRenewClassification::Vanished), "vanished");
      EXPECT_EQ(toString(MountRenewClassification::Cancelled), "cancelled");
      EXPECT_EQ(toString(MountRenewClassification::FenceOrLifecycleLost), "fence_or_lifecycle_lost");
      EXPECT_EQ(toString(MountRenewClassification::ExternalLeaseDeadline), "external_lease_deadline");
      EXPECT_EQ(toString(MountRenewClassification::RequestDeadline), "request_deadline");
      EXPECT_EQ(toString(MountRenewClassification::Unresolved), "unresolved");
      EXPECT_EQ(toString(MountRenewClassification::Unclassified), "terminal_unclassified");
  }
  ```
- [ ] `CasServerRoot.h`, before `struct MountRenewResult`: the enum `MountRenewClassification` with the nine values in
  the order of `MountRenewTerminalClassification` (moved, renamed), its comment "Why a renewal ended without a commit,
  as its `watermark_renew` event names it. Set from exactly one arm of the write's verdict, so the event never
  re-derives a reason the engine did not report.", and `std::string_view toString(MountRenewClassification);`.
  In `MountRenewResult` add:
  ```cpp
      MountRenewClassification classification = MountRenewClassification::Unclassified;
      /// The attempted body, which the `watermark_renew` event names.
      uint64_t writer_epoch = 0;
      uint64_t seq = 0;
      UInt128 write_attempt_id{};
      /// The confirmed lease deadline this renewal started under.
      uint64_t confirmed_deadline_boot_ms = 0;
  ```
- [ ] `CasServerRoot.cpp`: delete the two forward declarations after `namespace DB::Cas {`; delete everything from
  `enum class MountRenewTerminalClassification` through the end of `deliverMountRenewObservability` (keep the
  `describeMountHolder` forward declaration that follows); delete the definitions of `configureMountRenewObservability`
  and `reportMountRenewCompletion`. Define `toString` (the body of `terminalClassificationName`) outside the anonymous
  namespace. Add to the anonymous namespace:
  ```cpp
  /// A conflict names a holder, proves the slot absent, or settles nothing.
  MountRenewClassification slotClassification(MountSlotVerdict verdict)
  {
      switch (verdict)
      {
          case MountSlotVerdict::FencedByGc:
          case MountSlotVerdict::SlotTaken:
          case MountSlotVerdict::SlotCorrupt:
              return MountRenewClassification::Conflict;
          case MountSlotVerdict::SlotAbsent:
              return MountRenewClassification::Vanished;
          case MountSlotVerdict::Ours:
          case MountSlotVerdict::NotObserved:
              return MountRenewClassification::Unresolved;
      }
      return MountRenewClassification::Unresolved;
  }
  ```
- [ ] `renewConflictFailure`: delete its five `markMountRenewTermination` lines. `renewOn`: delete the registration,
  the guard and the `initializeMountRenewObservability` block; after `result.attempt_start_boot_ms = …;` set
  `result.writer_epoch = writer_epoch; result.seq = next_seq; result.write_attempt_id = write_attempt_id;
  result.confirmed_deadline_boot_ms = confirmed_deadline_boot_ms;`. Replace each
  `markMountRenewTermination(X);` with `result.classification = X;` (renamed enum). In the `Conflict` arm, after
  `result.slot = classifyMountSlot(…);` add `result.classification = slotClassification(result.slot->verdict);`. The
  `GaveUp` message uses `toString(classification)`.
- [ ] `CasMountRuntime.cpp`: delete the two forward declarations and the `configureMountRenewObservability` call in
  `renewRenewerOnce`; in `consumeRenewResult` replace both `reportMountRenewCompletion(result)` with
  `reportRenewal(result)`. Declare `void reportRenewal(const MountRenewResult & result) const noexcept;` next to
  `consumeRenewResult` with the comment "One `watermark_renew` event and its log lines for a renewal that recovered or
  ended; a first-attempt commit is silent." Define it:
  ```cpp
  void CasMountRuntime::reportRenewal(const MountRenewResult & result) const noexcept
  {
      const bool recovered = result.outcome == MountRenewOutcome::Committed
          && (result.attempts_sent > 1 || result.resolved_by_read);
      if (!recovered && result.outcome != MountRenewOutcome::Terminal)
          return;
      try
      {
          const uint64_t now = bootMsNow();
          const uint64_t elapsed_ms = now > result.attempt_start_boot_ms ? now - result.attempt_start_boot_ms : 0;
          const uint64_t remaining_ms = result.confirmed_deadline_boot_ms > now ? result.confirmed_deadline_boot_ms - now : 0;
          const std::string_view classification = !recovered ? toString(result.classification)
              : result.resolved_by_read ? "committed_by_read" : "committed_after_retry";
          if (event_sink)
          {
              CasEvent event;
              event.type = CasEventType::WatermarkRenew;
              event.outcome = recovered ? "recovered" : "failed";
              event.reason = recovered
                  ? "CAS mount renewal committed after a retry or a settling read"
                  : "CAS mount renewal ended without retained authority and fenced the mount";
              event.detail = {
                  {"server_root_id", server_root_id},
                  {"writer_epoch", std::to_string(result.writer_epoch)},
                  {"seq", std::to_string(result.seq)},
                  {"write_attempt_id", u128ToHex(result.write_attempt_id).substr(0, 12)},
                  {"attempts_sent", std::to_string(result.attempts_sent)},
                  {"elapsed_ms", std::to_string(elapsed_ms)},
                  {"remaining_confirmed_budget_ms", std::to_string(remaining_ms)},
                  {"classification", String{classification}},
              };
              try
              {
                  event_sink(std::move(event));
              }
              catch (...) // NOLINT(bugprone-empty-catch)
              {
                  /// A failing sink must not suppress the log lines below.
              }
          }
          auto log = getLogger("CasMountLeaseRenewer");
          for (uint32_t attempt_no = 2; attempt_no <= result.attempts_sent; ++attempt_no)
              LOG_DEBUG(log, "CAS mount renewal '{}' physical retry attempt {} (writer_epoch={}, seq={})",
                  server_root_id, attempt_no, result.writer_epoch, result.seq);
          if (recovered)
              LOG_INFO(log, "CAS mount renewal '{}' recovered after {} physical attempts in {} ms "
                  "(classification={}, confirmed_deadline_boot_ms={})",
                  server_root_id, result.attempts_sent, elapsed_ms, classification, result.confirmed_deadline_boot_ms);
          else
              LOG_WARNING(log, "CAS mount renewal '{}' fenced after {} physical attempts in {} ms "
                  "(classification={}, confirmed_deadline_boot_ms={})",
                  server_root_id, result.attempts_sent, elapsed_ms, classification, result.confirmed_deadline_boot_ms);
      }
      catch (...) // NOLINT(bugprone-empty-catch)
      {
          /// A lost diagnostic must not change how the renewal ends.
      }
  }
  ```
- [ ] `gtest_cas_event_log.cpp`: delete the two forward declarations and `CASEvent.DeepReentrancyPreservesDeterministicPhysicalAttemptTruth`
  — it drives the removed free functions to test the eight-slot suppression, which no longer exists; physical attempts
  now ride the result itself. `ReentrantRenewalSinkPreservesOuterObservationIdentity` and
  `PreCompletionConflictReentrancyPreservesOuterTerminalObservation` stay unchanged (reentrancy golden).
- [ ] `git grep -n 'MountRenewObservab\|markMountRenewTermination\|reportMountRenewCompletion' -- src` — empty.
- [ ] Build; `CAS*` release and ASan green; every other event test unchanged. Commit the five files:
  `Return the CAS renewal classification by value, not a thread-local`.

### Task 4: Remove the deadline classifications and `CASMountRenewalDeadlineExceeded`

**Files:** `$CA/Pool/CasServerRoot.{h,cpp}`, `$CA/Pool/CasMountRuntime.cpp`, `src/Common/ProfileEvents.cpp`,
`$T/gtest_cas_event_log.cpp`, `$T/gtest_cas_heartbeat.cpp`, `$T/gtest_cas_lossless_claim.cpp`,
`$T/gtest_cas_observability.cpp`, `docs/en/antalya/metrics_and_events.md`, `docs/en/antalya/cas/operations/{monitoring,debugging,troubleshooting}.md`.

- [ ] Tests first (red on base): in `EveryRenewalClassificationHasItsEventName` replace the two deadline lines with
  `EXPECT_EQ(toString(MountRenewClassification::Deadline), "deadline");`. Replace `"external_lease_deadline"` with
  `"deadline"` in `gtest_cas_event_log.cpp` (two asserts) and in `gtest_cas_lossless_claim.cpp`
  (`"deadline, no attempt sent"`). In `gtest_cas_heartbeat.cpp` replace the message check and the two
  `deadline_source` lines with `EXPECT_EQ(result.classification, MountRenewClassification::Deadline);`. In
  `gtest_cas_observability.cpp` drop the `deadline_exceeded` field, its extern, its argument and both call-site
  arguments; rename `ExternalLeaseDeadlineCountsOnceWithoutReconstructingAttempts` to
  `ADeadlineRenewalCountsNoAttempt` (same body, four counters).
- [ ] Code: enum `ExternalLeaseDeadline`, `RequestDeadline` → one `Deadline` (`"deadline"`); the `GaveUp::Why::Deadline`
  case sets `Deadline`; delete `MountRenewResult::deadline_source`, its assignment in `renewOn`, the counter increment
  and extern in `CasMountRuntime.cpp`, and the `M(CASMountRenewalDeadlineExceeded, …)` line.
- [ ] Docs: delete its row in `metrics_and_events.md` and `monitoring.md` and from the `system.events` query
  ("those four counters"); in `debugging.md` replace the two deadline bullets with "`deadline` (only the startup
  renewal, which is bounded by the lease it was claimed under: the lease-safety margin or the request policy ran out
  before the write settled — check object-store latency and `BOOTTIME` advancement)" and "only `deadline` is about a
  deadline"; in `troubleshooting.md` case 2 say `classification = 'deadline'`, drop the counter sentence and the
  `request_deadline` sibling sentence.
- [ ] `git grep -n 'external_lease_deadline\|request_deadline\|CASMountRenewalDeadlineExceeded\|deadline_source' -- src/Disks docs/en tests`
  — only `GaveUp::deadline_source` in `$CA/Backend` and its tests. Build; gates green. Commit the twelve files:
  `Name the CAS startup renewal deadline once and drop its counter`.

### Task 5: Remove the request counters no key can reach

**Files:** `$CA/Backend/CasInstrumentedBackend.{h,cpp}`, `src/Common/ProfileEvents.cpp`, `$T/gtest_cas_backend.cpp`,
`docs/en/antalya/metrics_and_events.md`, `docs/en/antalya/cas/operations/monitoring.md`.

- [ ] Test first (red on base: `5 != 6`, `10 != 11`), in `ClassifierAndPerNamespaceOpEvents`:
  ```cpp
      /// One table row per class a key can land in, one column per outcome a primitive reports.
      static_assert(CAS_NS_COUNT == 5);
      static_assert(CAS_OP_COUNT == 10);
  ```
- [ ] Header: drop `Server` from `CasNs` and `Cas` from `CasOp`; counts 5 and 10; the class comment says 5 classes,
  `/gc/server-roots/…` is `Gc`; delete the "`Cas` has no current producer" paragraph and the `write … CompareSwap` line.
  Source: delete the 11 `CASServer*` externs, the 5 `CAS*CompareSwap` externs, the `Server` row and the
  `CompareSwap` entry of every row. `ProfileEvents.cpp`: delete those 16 `M(…)` lines and
  `M(CASConditionalWriteFenceLostPostWrite, …)`.
- [ ] Docs: delete the "Server-object requests {#cas-server-requests}" section, the five `CAS*CompareSwap` rows and the
  `CASConditionalWriteFenceLostPostWrite` row; "outside the blob, manifest, root, and GC prefixes" in the Other section;
  `monitoring.md`: `CASBlobCompareSwapConflict` is "near zero relative to `CASBlobOverwrite`".
- [ ] Build; gates green. Commit the seven files: `Drop the CAS request counters that no key can reach`.

### Task 6: Name the reacquire and lease-expiry counters

**Files:** `$CA/Pool/CasMountRuntime.{h,cpp}`, `src/Common/ProfileEvents.cpp`, `$T/gtest_cas_reacquire.cpp`,
`$T/gtest_cas_pool.cpp`, `docs/en/antalya/metrics_and_events.md`, `docs/en/antalya/cas/operations/monitoring.md`.

- [ ] Tests first (red on base: undeclared events). `gtest_cas_reacquire.cpp` (externs for the three events):
  ```cpp
  TEST(CASReacquire, CountersSayHowEachReacquireEnded)
  {
      const auto at = [](ProfileEvents::Event e) { return ProfileEvents::global_counters[e].load(); };
      {
          Fenced f;
          const uint64_t attempts = at(ProfileEvents::CASMountReacquireAttempts);
          const uint64_t succeeded = at(ProfileEvents::CASMountReacquireSucceeded);
          ASSERT_EQ(f.store->runReacquireForTest(), ReacquireOutcome::Open);
          EXPECT_EQ(at(ProfileEvents::CASMountReacquireAttempts) - attempts, 1u);
          EXPECT_EQ(at(ProfileEvents::CASMountReacquireSucceeded) - succeeded, 1u);
      }
      {
          Fenced f;
          erase(*f.backend, kLayout.mountKey(kSrid));
          const uint64_t failed = at(ProfileEvents::CASMountReacquireFailed);
          expectStoppedOn(f, f.store->runReacquireForTest(), MountEnd::SlotAbsent);
          EXPECT_EQ(at(ProfileEvents::CASMountReacquireFailed) - failed, 1u);
      }
  }
  ```
  In `CASMountRuntime.ALeaseExpiryIsLoggedOnceAndReported` read `CASMountLeaseExpired` before
  `startBackgroundWorkers` and assert `+1` next to `EXPECT_EQ(warnings, 1u)`.
- [ ] Code: `ProfileEvents.cpp`, after `CASMountRenewalRecovered`:
  ```
  M(CASMountLeaseExpired, "Number of CAS mount-lease expiries while the renewal kept retrying, counted once per expiry at the first renewal attempt after it. Writes resume after a confirmed renewal that leaves enough lease.", ValueType::Number) \
  M(CASMountReacquireAttempts, "Number of steps of the CAS mount reacquire after a GC fence of the active writer epoch. A step that cannot finish is retried and counted again.", ValueType::Number) \
  M(CASMountReacquireSucceeded, "Number of CAS mount reacquires that opened writes under a new writer epoch.", ValueType::Number) \
  M(CASMountReacquireFailed, "Number of CAS mount reacquires that ended the mount: the slot, the anchor or the epoch counter showed another owner or was gone. The server stops.", ValueType::Number) \
  ```
  and `CASMountLeaseLost`'s text: "Number of CAS mount-lease losses: one per GC fence of the active writer epoch and
  one per end that stops the server. Shutdown and `SYSTEM CAS FORGET` do not count." In `CasMountRuntime`, a private
  `ReacquireOutcome runReacquireStep(ReacquireStep & step);` that calls `reacquire_attempt(step)` and increments
  (`incrementNoTrace`) Attempts always, Succeeded on `Open`, Failed on `Ended`; `reacquireUntilSettled` and
  `runReacquireForTest` call it instead of `reacquire_attempt`. In `warnOnceIfLeaseExpired`, after the `exchange`
  guard: `ProfileEvents::incrementNoTrace(ProfileEvents::CASMountLeaseExpired);`.
- [ ] Docs: four rows in `metrics_and_events.md` (texts as above) and in the `monitoring.md` table and query; the
  `CASMountLeaseLost` paragraph matches the new text.
- [ ] Build; gates green. Commit the eight files: `Count CAS mount reacquires and lease expiries`.

### Task 7: No reader of a removed name is left

**Files:** in `$M` only (separate commit on `cas-gc-rebuild`, exact files):
`utils/ca-soak/scenarios/cards/{s06_s08_manifest_parts,s15_s18_shards_lifecycle,s19_s22_clone_fetch,s23_s27_misc,s34_s35_d1_churn,s36_s37_disk_move,s39_lease_fault_tolerance}.py`,
`utils/cas-grafana/casgrafana/drilldown.py`, `utils/cas-grafana/dashboards/cas-drilldown.json`.

- [ ] Check (save as `$M/tmp/lease-series/pr16-removed-names.sh`, not committed), run in `$WT` and in `$M`:
  ```bash
  P='\bCASServer[A-Z]\w*|\bCAS(Blob|Manifest|Root|GC|Other)CompareSwap\b|CASConditionalWriteFenceLostPostWrite|CASMountRenewalDeadlineExceeded|external_lease_deadline|request_deadline\b|CASRemount|MountRenewObservab|reportMountRenewCompletion'
  git grep -nE "$P" -- src tests docs/en utils ':!*.tla' | grep -v 'cas_request_deadline'
  ```
  Before the retarget the `$M` run lists the cards and `drilldown.py`; `$WT` must be empty.
- [ ] Retarget: `CASRootCompareSwap` → `CASRootOverwrite` (keep `CASRootCompareSwapConflict`); in `s06` keep the gate's
  meaning ("no root overwrite observed in the creation window"). `s39`: drop `CASMountRenewalDeadlineExceeded`;
  `CASRemountAttempts/Succeeded/Failed` → `CASMountReacquireAttempts/Succeeded/Failed`. `drilldown.py`: drop the
  "Deadline exceeded" series; "Fence lost post-write" reads `CASRequestFenceLostPostWrite`. Regenerate with
  `python3 utils/cas-grafana/build_dashboards.py`; run `python3 -m unittest discover utils/cas-grafana/tests` and
  `python3 -m py_compile` on each card. Do not push dashboards (otel.demo Grafana is add-only).
- [ ] Re-run the check in both trees: empty. Commit in `$M`: `Retarget CAS soak cards and Grafana off removed counters`.

### Task 8: Gate and PR

- [ ] Release and ASan `CAS*` on the committed head; `python -m ci.praktika run "integration" --test test_cas_mount_renewal_retry`
  (reads `watermark_renew` rows and the renewal counters). Logs in the build dir; a subagent summarises each.
- [ ] Line delta: `git diff --stat <base>..HEAD -- src/Disks/DiskObjectStorage src/Common` — expected ≈ −200.
- [ ] PR text per `.github/PULL_REQUEST_TEMPLATE.md`: what goes and why (the inventory table, the removed-name check),
  mixed versions ("counters and classification names differ between versions; no on-store format change"),
  Code vs spec 1–5. Changelog category `Improvement`: "Remove CAS profile events that can never increment, add
  `CASMountReacquire*` and `CASMountLeaseExpired`, and rename the startup renewal deadline classification to
  `deadline`." `Closes:` CAS-2.1 is a backlog id: the controller updates the backlog in `$M`.
