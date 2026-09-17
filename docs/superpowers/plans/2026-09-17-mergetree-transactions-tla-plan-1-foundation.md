---
description: 'Implementation plan 1 (revision 2, as-built) of the TLA+ model of MergeTree transactions: install the verified foundation modules under utils/tla/transactions, the TLC runner and witness runner, the Base scenario with its properties, a state-space budget that TLC finishes in minutes, the witness verification of every Base property, and the README with code map and run table. Later plans add cleanup, merges, non-transactional batches, Keeper faults, restart, mutations, disk faults and calibration.'
sidebar_label: 'TLA+ transactions, plan 1'
sidebar_position: 21
slug: /superpowers/plans/mergetree-transactions-tla-plan-1-foundation
title: 'MergeTree transactions TLA+ model, plan 1: foundation and Base scenario'
doc_type: 'plan'
---

# MergeTree transactions TLA+ model, plan 1: foundation and Base scenario {#tla-plan-1}

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

Revision 2, 2026-09-17. Revision 1 embedded the TLA+ as prose and a review found 23 defects in it; the modules
were then written and checked with SANY and TLC in `tmp/tla_check/` (git-ignored). Revision 2 starts from those
verified files: `Types.tla`, `Keeper.tla`, `Disk.tla`, `History.tla`, `Parts.tla`, `Server.tla`,
`Invariants.tla`, `MergeTreeTransactions.tla`, `MC_Schema.{tla,cfg}`, `MC_BaseSmallProps.{tla,cfg}`.
Their state at the start of this plan: SANY clean; `MC_Schema` green (1 state); `MC_BaseSmallProps` (one
session, two tids, two parts, every `Base` property) green, 24,667 distinct states in one second; the
two-session, three-tid configuration exceeded 90 million distinct states without finishing, which is a
state-space defect this plan must resolve (Task 3).

**Goal:** A TLC-checkable TLA+ specification under `utils/tla/transactions/` whose `Base` scenario (`BEGIN`,
`INSERT`, `SELECT`, `DROP PARTITION`, `COMMIT`, `ROLLBACK`, `KILL TRANSACTION`, the updating thread, the
three-step metadata store) runs green on the isolation, conflict and durability properties in minutes, whose
witness harness turns each property red on demand, and whose README maps every action to the C++.

**Architecture:** One root module declares every variable of the spec (records `zk`, `disk`, `mdisk`, `h`,
`part`, `tlog`, `txn`, `sys`, `client`, `stmt`, `mut`, `task`), `TypeOK`, `Init` and action groups; scenario
modules compose their own `Next` from the groups (`BaseNext`) so that later plans add actions without enabling
them in `Base`. Actions of later plans are stubs (`FALSE`). Witnesses are `Witness("Name")` guards inside the
actions, selected by the constant `WITNESS_NAME`.

**Tech Stack:** TLA+ (TLA+2), TLC from `tla2tools.jar` (Java 21), bash.

**Spec:** `docs/superpowers/specs/2026-09-17-mergetree-transactions-tla-design.md` (revision 11). The spec is the
authority; the model's action names are the spec's.

## Global Constraints {#global-constraints}

- Baseline C++ is upstream master `2c24b6b9291e`; the code map cites files and functions of that tree.
- All model files live under `utils/tla/transactions/`; temporary files under `tmp/` (never `/tmp`).
- Every TLC run uses its own `-metadir` under `tmp/tla/<run>/states` (two runs sharing a state directory with
  `-cleanup` destroyed each other during plan verification) and its log goes to `tmp/tla/<run>/tlc.log`.
- A TLC run that passes 30 million distinct states or 45 minutes is a defect of the model or of the bounds, not
  something to wait for: kill it, record it, reduce.
- Commit after every task with `git commit -- <paths>`; never `git add -A`; do not push. Commit messages end
  with the attribution lines from the session's system reminder.
- Witness contract (spec): `witness.sh Scenario Property` checks only that property with `WITNESS_NAME` set and
  exits zero only when TLC reports a violation of it. Two-change witnesses must be minimal.
- Expected colours (spec matrix): every property named for `Base` is green on the baseline.
- The four counterexample classes and what to do with them: (a) TLC parse or type error: fix the model; (b) a
  property red on the baseline because the property or the action misreads the code: fix the model, note it in
  the report with the C++ line that decided; (c) a property red because the C++ has the defect: do not change
  the model, add the trace to `utils/tla/transactions/FINDINGS.md` with the action sequence and the C++ call
  sequence; (d) a witness that does not fire: fix the hook or the property.

## Plans that follow this one {#later-plans}

Written after plan 1 has executed: plan 2 (`SetSnapshot`, `Cleanup*`, `UpdRemoveOldEntries*`, `Merge*` with the
task set, `NtInsert`, `NtBatch*`, `NtDropCover`; scenarios `SetSnapshot`, `Merge`, `NonTxn`); plan 3 (Keeper
faults, `CommitUnknown`, `Crash`, `Restart*`, `Layered` disk; `Keeper`, `Crash`, `SnapshotCrash`, `NonTxnCrash`);
plan 4 (mutations; `Mutation`, `MutationChain`, `MutationCrash`, `MergeMutation`, `MutationCleanup`,
`QueryFault`); plan 5 (disk faults, `ProcessDown`, `StoreRetry`, `KillRetry`, `Implicit`, `Live`,
`RetryProgress`, calibration re-run).

---

### Task 1: Install the verified modules and the TLC runner {#task-1}

**Files:**
- Create: `utils/tla/transactions/run_tlc.sh`
- Create: `utils/tla/transactions/{Types,Keeper,Disk,History,Parts,Server,Invariants,MergeTreeTransactions}.tla`
  (copied from `tmp/tla_check/`, unchanged except the header comment below)
- Create: `utils/tla/transactions/MC_Schema.{tla,cfg}`, `MC_BaseSmall.{tla,cfg}` (from
  `tmp/tla_check/MC_Schema.*` and `MC_BaseSmallProps.*`, renamed), `MC_Base.{tla,cfg}` (two sessions, see below)
- Check: `git check-ignore tmp` must succeed; if it does not, add `tmp/` to the repository root `.gitignore`.

**Interfaces:**
- Produces: `run_tlc.sh <Scenario> [workers]`: runs `MC_<Scenario>` with `-metadir tmp/tla/<Scenario>/states`,
  log at `tmp/tla/<Scenario>/tlc.log`, trace at `tmp/tla/<Scenario>/trace.txt`; exit 0 green, 1 violation, 2
  tool or parse error; removes the state directory after the run. The module names, variable names and
  operator names of the copied files are the interface of every later task and plan.

- [ ] **Step 1: Write `run_tlc.sh`**

```bash
#!/usr/bin/env bash
# Run TLC on MC_<Scenario>. Usage: run_tlc.sh <Scenario> [workers]
# Exit: 0 green, 1 violation, 2 parse/config error or missing tool.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
SCENARIO="${1:?scenario name, e.g. Base}"
WORKERS="${2:-auto}"
JAR="$ROOT/tmp/tla2tools.jar"
OUT="$ROOT/tmp/tla/$SCENARIO"
mkdir -p "$OUT"
if ! unzip -l "$JAR" 2>/dev/null | grep -q 'tlc2/TLC.class'; then
  curl -fsSL -o "$JAR" https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar || { echo "cannot download tla2tools.jar" >&2; exit 2; }
fi
MODULE="MC_$SCENARIO"
[ -f "$HERE/$MODULE.tla" ] && [ -f "$HERE/$MODULE.cfg" ] || { echo "missing $HERE/$MODULE.tla or .cfg" >&2; exit 2; }
cd "$HERE"
timeout 2700 java -XX:+UseParallelGC -Xmx16g -cp "$JAR" tlc2.TLC -workers "$WORKERS" -deadlock \
     -metadir "$OUT/states" -config "$MODULE.cfg" "$MODULE.tla" > "$OUT/tlc.log" 2>&1
RC=$?
rm -rf "$OUT/states"
if grep -qE '^Error: (Invariant|Action property|Temporal properties)' "$OUT/tlc.log"; then
  awk '/^Error: /,0' "$OUT/tlc.log" > "$OUT/trace.txt"
  echo "VIOLATION scenario=$SCENARIO $(grep -oE 'Error: (Invariant|Action property) [A-Za-z_]+' "$OUT/tlc.log" | head -1) (trace: $OUT/trace.txt)"
  exit 1
fi
if grep -q 'Model checking completed. No error has been found' "$OUT/tlc.log"; then
  grep -E 'states generated|Finished in' "$OUT/tlc.log" | tail -2
  echo "GREEN scenario=$SCENARIO"
  exit 0
fi
[ $RC -eq 124 ] && echo "TIMEOUT scenario=$SCENARIO after 45 min: a bounds or model defect, see $OUT/tlc.log"
echo "TLC ERROR scenario=$SCENARIO rc=$RC (see $OUT/tlc.log)"; tail -15 "$OUT/tlc.log"
exit 2
```

- [ ] **Step 2: Copy the modules and add the header comment**

Copy the eight modules from `tmp/tla_check/` unchanged. Insert into each, after the `---- MODULE X ----` line,
two comment lines: `\* Part of the TLA+ model of MergeTree transactions; see
docs/superpowers/specs/2026-09-17-mergetree-transactions-tla-design.md` and `\* Baseline C++: upstream
ClickHouse master 2c24b6b9291e`. Copy `MC_Schema.tla/.cfg`. Create `MC_BaseSmall.tla/.cfg` from
`MC_BaseSmallProps.tla/.cfg` (rename the module inside). Create `MC_Base.tla/.cfg` as `MC_BaseSmall` with
`Sessions = {k1, k2}`, `TID_MAX = 2`, `CSN_MAX = 35`, and `SYMMETRY SymSessions` where `MC_Base.tla` defines
`SymSessions == Permutations(Sessions)`. (TLC accepts symmetry with the action properties used here, which are
safety properties; if TLC refuses, drop the `SYMMETRY` line and say so in the report.) Delete any stale
`states/` directory under `tmp/tla_check/`.

- [ ] **Step 3: Run**

Run: `utils/tla/transactions/run_tlc.sh Schema && utils/tla/transactions/run_tlc.sh BaseSmall`
Expected: both `GREEN`; `BaseSmall` about 25,000 distinct states. Then run `run_tlc.sh Base` in the background
with a 20-minute cap: it is the measurement Task 3 needs. If it has not finished after 20 minutes, kill it and
record the last `Progress` line of the log in the report; do not wait for it.

- [ ] **Step 4: Commit**

```bash
git commit -m "tla(transactions): foundation modules, Schema/BaseSmall/Base scenarios, TLC runner" -- utils/tla/transactions .gitignore
```

---

### Task 2: Witness harness and witness verification of the Base properties {#task-2}

**Files:**
- Create: `utils/tla/transactions/witness.sh`
- Modify: `utils/tla/transactions/{Server,Parts,Invariants}.tla` only where a hook is missing or does not fire
- Create: `utils/tla/transactions/WITNESSES.md` (the witness table)

**Interfaces:**
- Consumes: the `Witness("Name")` hooks already present in `Parts.tla` and `Server.tla`. Find them with
  `grep -n 'Witness("' *.tla`; the spec's witness table (section "Witnesses") names the property each serves and
  the change each makes.
- Produces: `witness.sh <Scenario> <Property> [<WitnessName>]`: generates `tmp/tla/w_<Property>/MC_W.cfg` from
  `MC_<Scenario>.cfg` with every `INVARIANT`/`PROPERTY`/`INVARIANTS`/`PROPERTIES` line (and their continuation
  lines) removed, `WITNESS_NAME` set to `<WitnessName>` (default `<Property>`), and one `INVARIANT <Property>` or
  `PROPERTY <Property>` line (a property is an action property iff `Invariants.tla` defines it as `[][...]_vars`;
  detect with `grep -E "^<Property> == \[\]\["`); generates `MC_W.tla` from `MC_<Scenario>.tla` with the module
  name replaced; runs TLC with its own metadir `tmp/tla/w_<Property>/states` and log; exits 0 iff the log
  contains `Error: Invariant <Property> is violated` or `Error: Action property <Property> is violated`; exits
  1 when TLC finishes green (witness did not fire); exits 2 on a TLC error. Prints one line
  `RED|GREEN|ERROR <Property> states=<distinct> time=<s>`.

- [ ] **Step 1: Write `witness.sh` to the interface above**

Use the `Base` bounds (`Sessions = {k1, k2}`, `TID_MAX = 2`, `CSN_MAX = 35`) for every witness.

- [ ] **Step 2: Run every witness and make the table**

Run `witness.sh Base <Property> [<WitnessName>]` for every property that has a hook (`Assert_validateInfo_*`
hooks target the property `Assert_validateInfo` with the variant as `WitnessName`). Record in `WITNESSES.md` a
table `Property | Witness name | The model change (one sentence) | Scenario | Result | States | Time`.
Expected: every row `RED`. For a row that stays green, read the hook in the action, decide whether the hook or
the property is wrong (the spec's witness table says what the change must be), fix it, rerun, and say in the
report what was wrong. For `ActiveSetShape` (the `Base` universe has no covering relation, so its witness needs
`Merge`) and `NoAvoidableTermination` (needs `ProcessDown`, plan 5), record `deferred to plan N` rather than
running. A witness whose run exceeds 20 minutes is killed and recorded as `TIMEOUT`; Task 3 reruns it.

- [ ] **Step 3: Commit**

```bash
git commit -m "tla(transactions): witness runner and Base witness table" -- utils/tla/transactions
```

---

### Task 3: State-space budget for the two-session Base scenario {#task-3}

**Files:**
- Modify: `utils/tla/transactions/MC_Base.{tla,cfg}`, possibly `Server.tla`, `Parts.tla`,
  `MergeTreeTransactions.tla`
- Create: `utils/tla/transactions/STATE_SPACE.md`

**Interfaces:**
- Produces: `run_tlc.sh Base` finishes green in under 15 minutes with `Sessions = {k1, k2}` and `TID_MAX = 2`,
  and `STATE_SPACE.md` explains where the states come from and which reductions were applied, each with its
  measured effect (distinct states before and after).

- [ ] **Step 1: Measure**

Take the Task 1 measurement of `Base` (finished or killed at 20 minutes). Then measure the contribution of the
suspects with `BaseSmall`-sized variants (copies under `tmp/tla/measure/`, never committed) that disable one
thing at a time and compare distinct-state counts: (a) the updater actions; (b) `KillTransaction`; (c) `Drop*`;
(d) the three-step store replaced by one combined step; (e) `SYMMETRY`. Record each count.

- [ ] **Step 2: Reduce without losing interleavings the spec needs**

Allowed reductions (they remove states without removing behaviours the spec's properties distinguish):
(1) a `VIEW` in `MC_Base.tla` that drops from the fingerprint the fields no property reads (for example
`h.content` while `NoLostVisibleData` is not in `Base`; a frame's `tentative` when its `op`/`val` determine it);
(2) removing the `Fsync` action from `BaseNext` in `Durable` mode (a no-op there); (3) `CSN_MAX = 35`;
(4) `SYMMETRY` on `Sessions`; (5) making the session-local bookkeeping that no property reads a function of the
program counter instead of a stored field. Not allowed: merging store steps, removing `KillTransaction`, removing
the updater's two-step publication, bounding `TID_MAX` below 2.

Apply reductions one at a time, measure, keep the ones that help, record all of them in `STATE_SPACE.md` with
the numbers. If after every allowed reduction the two-session run still does not finish in 15 minutes, add a
`CONSTRAINT` limiting the number of concurrently open frames to 2 and the number of transactions that reached
`CommitAck` or `RollbackFinalize` to `TID_MAX`; document it as a bound, not a reduction, and list it under the
README's refinement parameters (Task 4).

- [ ] **Step 3: Confirm the properties stay green and the witnesses stay red**

Run `run_tlc.sh Base` (green) and the Task 2 witness loop on the reduced configuration (every row red or
deferred; rerun any Task 2 `TIMEOUT` row). Update `WITNESSES.md` and record the final counts in `STATE_SPACE.md`.

- [ ] **Step 4: Commit**

```bash
git commit -m "tla(transactions): state-space budget for the two-session Base scenario" -- utils/tla/transactions
```

---

### Task 4: README with code map, run table, findings file {#task-4}

**Files:**
- Create: `utils/tla/transactions/README.md`
- Create: `utils/tla/transactions/FINDINGS.md` (a header and an empty table if no code finding was recorded in
  Tasks 1 to 3)

- [ ] **Step 1: Write the README**

Sections, in this order (each header with a `{#kebab-anchor}`):

1. Goal and scope: from the spec's goal and scope sections, with the "not covered in the first version" list.
2. How to run: `run_tlc.sh`, `witness.sh`, exit codes, where logs and traces go, the 45-minute timeout and
   what a timeout means.
3. Code map: one row per action defined in `Server.tla` and `Parts.tla` that is not a stub, columns
   `Action | C++ file | Function | Step boundary`, values from the spec's action tables (for example
   `DropEnrol | src/Interpreters/MergeTreeTransaction.cpp | MergeTreeTransaction::removeOldPart | mutex taken, checkIsNotCancelled, lockRemovalTID, push to removing_parts`).
   Stubs are listed with `(plan N)` in the function column. Every row is checked against the C++ at
   `2c24b6b9291e` by opening the function; a row whose function name or boundary does not match the code is a
   defect of the row, fix it.
4. Run table: `Scenario | Date | Commit | Distinct states | Time | Result` with the rows of Tasks 1 to 3.
5. Witness table: a pointer to `WITNESSES.md`.
6. Refinement parameters: `MAX_STORE_RETRIES` 2 vs 20, `NOEXCEPT_RETRY_BUDGET` 2 vs 60 s, `Tasks` bounded,
   `CSN_MAX`, any `CONSTRAINT` from Task 3 (read `STATE_SPACE.md`).
7. Expected-red findings (from the spec): the four properties and the scenarios where they will be run, none in
   plan 1, plus a pointer to `FINDINGS.md`.
8. Model decisions that differ from a literal reading of the code, each with the reason: tids are monotonic
   integers (no `local_tid_counter` reset); `NoActor` and record sentinels instead of `"None"` strings (TLC
   cannot compare a string with a tuple); isolation properties are checked at `SelectFinish` over the read that
   step produces, not as state invariants over a stale read; the session waits for its own rollback to finish
   (`RollbackWait`) and a query on a rolled-back transaction is refused with `INVALID_TRANSACTION`.

- [ ] **Step 2: Commit**

```bash
git commit -m "tla(transactions): README with code map, run table and findings file" -- utils/tla/transactions
```

---

## Self-review {#self-review}

Spec coverage of plan 1's slice: the `Base` row of the spec's matrix enables `Begin`, `Insert*`, `Select*`,
`Drop*`, `Commit*`, `Rollback*`, `KillTransaction`, the updater; every one exists in the verified `Server.tla`;
Task 2 verifies the `Base` witnesses, Task 3 makes the two-session run finish, Task 4 documents. Every other
spec action is a stub named in the later-plans section. Type consistency: the interfaces are the file contents
of `tmp/tla_check/`, which SANY and TLC have already accepted.
