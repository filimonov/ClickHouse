---
description: 'Implementation plan 1 of the TLA+ model of MergeTree transactions: TLC tooling, the complete state schema with TypeOK, the Keeper, Disk and History modules, and the Base scenario with its isolation, conflict and durability properties and their witnesses. Later plans add cleanup, merges, non-transactional batches, Keeper faults, restart, mutations, disk faults and calibration.'
sidebar_label: 'TLA+ transactions, plan 1'
sidebar_position: 21
slug: /superpowers/plans/mergetree-transactions-tla-plan-1-foundation
title: 'MergeTree transactions TLA+ model, plan 1: foundation and Base scenario'
doc_type: 'plan'
---

# MergeTree transactions TLA+ model, plan 1: foundation and Base scenario {#tla-plan-1}

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce a TLC-checkable TLA+ specification whose state schema declares every variable of the design
spec with a `TypeOK` invariant, whose `Base` scenario (`BEGIN`, `INSERT`, `SELECT`, `DROP PARTITION`, `COMMIT`,
`ROLLBACK`, `KILL TRANSACTION`, the updating thread, the three-step metadata store) runs green on the isolation,
conflict and durability properties, and whose witness harness turns each of those properties red on demand.

**Architecture:** One root module `MergeTreeTransactions.tla` declares all variables and composes operator
modules (`Types`, `Keeper`, `Disk`, `History`, `Parts`, `Server`, `Invariants`) that define operators over those
variables; scenario modules `MC_<Scenario>.tla` fix constants and enabled actions, and `MC_<Scenario>.cfg` names
the properties. Actions not enabled in plan 1 exist as stubs (`FALSE`) so that `Next` is complete from the
first commit and later plans only replace stubs. Every task ends with a TLC run whose expected outcome is
stated.

**Tech Stack:** TLA+ (TLA+2 syntax), TLC from `tla2tools.jar` (Java 21 is installed), bash, no CI.

**Spec:** `docs/superpowers/specs/2026-09-17-mergetree-transactions-tla-design.md` (revision 11). The plan
argues from it; executors read both. The spec's action names are the operator names below.

## Global Constraints {#global-constraints}

- Baseline C++ is upstream master `2c24b6b9291e`; the code map in `utils/tla/transactions/README.md` cites
  files and functions of that tree.
- All files live under `utils/tla/transactions/`; temporary files under `tmp/tla/` (never `/tmp`).
- Every module compiles with TLC from `tla2tools.jar` downloaded to `tmp/tla2tools.jar` by `run_tlc.sh`.
- Every TLC run is redirected to `tmp/tla/<Scenario>/tlc.log`; a subagent summarises logs, the main session
  reads only the summary and the exit code.
- Commit after every task with `git commit -- <paths>` (path-only, the worktree is shared); never `git add -A`.
- Commit messages end with the attribution lines from the session's system reminder.
- Do not push.
- No `sleep`-based waiting in scripts other than polling for a file marker.
- Witness contract (spec, invariants section): `witness.sh Scenario Property` applies the named override,
  checks only that property, and must exit zero only if TLC reports a violation of it. Two-change witnesses must
  be minimal.
- Expected colours (spec, scenario matrix): every property named in a scenario's "Checks" is green on the
  baseline unless the row says "expected red".
- Model values: `Sessions`, `Parts`, `Tasks`, `Mutations` are TLC model values with symmetry where the spec
  allows it (`Sessions`, `Tids` never, because tids are ordered).

## Plans that follow this one {#later-plans}

Written after plan 1 has executed, because they fill stubs whose shape plan 1 fixes:

- Plan 2: `SetSnapshot`, `Cleanup*` (`CleanupGrab`, `CleanupValidate`, `CleanupDeleteOk/Fail`),
  `UpdRemoveOldEntries*`, `Merge*` with the task set, `NtInsert`, `NtBatch*`, `NtDropCover`; scenarios
  `SetSnapshot`, `Merge`, `NonTxn`, `MergeMutation` without mutations.
- Plan 3: Keeper faults (`FailBefore`, `LostAfter`, `SessionExpire`, `UpdReconnect`, `UpdSwapUnknownLists`,
  `UpdFinalizeUnknown`, `CommitUnknown`, `WAIT_UNKNOWN`), `Crash`, `Restart*` with the coverage tree, `Layered`
  disk; scenarios `Keeper`, `Crash`, `SnapshotCrash`, `NonTxnCrash`.
- Plan 4: mutations (`MutPrepare*`, `MutRegister`, `MutSelect`, `MutWrite`, `MutPublish*`, `MutWait`, `Kill*`,
  `KillMutation`, `MutDestroyOwner`), payload versions; scenarios `Mutation`, `MutationChain`, `MutationCrash`,
  `MergeMutation`, `MutationCleanup`, `QueryFault`.
- Plan 5: disk faults, `ProcessDown`, `StoreRetry`, `KillRetry`, both policies, `Implicit`, `Live`,
  `RetryProgress`, the calibration table re-run, README with measured numbers.

## File structure {#file-structure}

| File | Responsibility |
|---|---|
| `utils/tla/transactions/run_tlc.sh` | download `tla2tools.jar` once, run TLC on `MC_<Scenario>`, write logs, exit non-zero on violation |
| `utils/tla/transactions/witness.sh` | run TLC on `MC_<Scenario>` with `WITNESS = "<Property>"` and only that property, exit zero iff violated |
| `utils/tla/transactions/Types.tla` | constants, special CSN and tid values, covering relation, payload records, helper operators; no variables |
| `utils/tla/transactions/Keeper.tla` | operators over `zk_log`, `zk_seq`, `zk_tail_ptr`, `zk_session` |
| `utils/tla/transactions/Disk.tla` | operators over `disk`, `mdisk`, `DISK_MODE`, `Fsync`, crash effect |
| `utils/tla/transactions/History.tla` | ghost variables' type, `OracleVisible`, history update operators |
| `utils/tla/transactions/Parts.tla` | `VersionInfo` records, `IsVisibleImpl` (the code's `isVisible`), `CanBeRemoved`, the three-step store operators |
| `utils/tla/transactions/Server.tla` | transaction, client, updater, drop, rollback, kill actions of plan 1; stubs for the rest |
| `utils/tla/transactions/Invariants.tla` | the properties of the `Base` scenario and their witness overrides |
| `utils/tla/transactions/MergeTreeTransactions.tla` | `VARIABLES`, `vars`, `TypeOK`, `Init`, `Next`, `Spec` |
| `utils/tla/transactions/MC_Schema.tla`, `.cfg` | constants only, `Next = FALSE`, checks `TypeOK` on `Init` |
| `utils/tla/transactions/MC_Base.tla`, `.cfg` | the `Base` scenario |
| `utils/tla/transactions/README.md` | goal, scope, code map, run table, witness table, refinement parameters, counterexample log |

Encoding decisions that every task relies on:

- Tids are integers `1..TID_MAX`; `EmptyTID = 0`, `NonTransactionalTID = -1`, `DummyTID = -2`. A tid's
  `start_csn` is `tid_start[t]`; the C++ `(start_csn, local_tid, host)` triple is not needed for uniqueness in
  a one-host model.
- CSNs are integers: `UnknownCSN = 0`, `NonTransactionalCSN = 1`, `CommittingCSN = 2`, `EverythingVisibleCSN
  = 3`, `MaxReservedCSN = 32`, real CSNs are `33..CSN_MAX`, `RolledBackCSN = CSN_MAX + 1`.
- A `VersionInfo` is the record `[ctid, ccsn, rtid, rcsn, sv]` (`creation_tid`, `creation_csn`,
  `removal_tid`, `removal_csn`, `storing_version`).
- A part record: `[pstate, mem, lock, deferrable, deferred, pins, frames]`; a frame:
  `[owner, tentative, pc, retries, interferences, noexcept_retries, noexcept_owner]`.
- `Covers` is a constant function `Parts -> SUBSET Parts` (direct children); `Base` uses `{P1, P2}` with
  `Covers = [p \in Parts |-> {}]`.

---

### Task 0: TLC tooling and a sanity module {#task-0}

**Files:**
- Create: `utils/tla/transactions/run_tlc.sh`
- Create: `utils/tla/transactions/witness.sh`
- Create: `utils/tla/transactions/Sanity.tla`, `utils/tla/transactions/MC_Sanity.tla`, `utils/tla/transactions/MC_Sanity.cfg`

**Interfaces:**
- Produces: `run_tlc.sh <Scenario> [workers]` exits 0 on a green run, 1 on a violation, 2 on a parse or
  configuration error; log at `tmp/tla/<Scenario>/tlc.log`, trace (if any) at `tmp/tla/<Scenario>/trace.txt`.
  `witness.sh <Scenario> <Property>` exits 0 iff TLC reported a violation of `<Property>`.

- [ ] **Step 1: Write the sanity module with one true and one false invariant**

`utils/tla/transactions/Sanity.tla`:

```tla
---- MODULE Sanity ----
EXTENDS Naturals
VARIABLE x
Init == x = 0
Next == x < 3 /\ x' = x + 1
Spec == Init /\ [][Next]_x
AlwaysTrue == x <= 3
AlwaysFalse == x < 3
====
```

`utils/tla/transactions/MC_Sanity.tla`:

```tla
---- MODULE MC_Sanity ----
EXTENDS Sanity
====
```

`utils/tla/transactions/MC_Sanity.cfg`:

```
SPECIFICATION Spec
INVARIANT AlwaysTrue
```

- [ ] **Step 2: Write `run_tlc.sh`**

```bash
#!/usr/bin/env bash
# Run TLC on MC_<Scenario>. Usage: run_tlc.sh <Scenario> [workers] [extra TLC args...]
# Exit: 0 green, 1 violation, 2 parse/config error or missing tool.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
SCENARIO="${1:?scenario name, e.g. Base}"
WORKERS="${2:-auto}"
shift $(( $# >= 2 ? 2 : $# ))
JAR="$ROOT/tmp/tla2tools.jar"
OUT="$ROOT/tmp/tla/$SCENARIO"
mkdir -p "$OUT"
if [ ! -s "$JAR" ] || ! unzip -l "$JAR" 2>/dev/null | grep -q 'tlc2/TLC.class'; then
  curl -fsSL -o "$JAR" https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar || { echo "cannot download tla2tools.jar" >&2; exit 2; }
fi
MODULE="MC_$SCENARIO"
[ -f "$HERE/$MODULE.tla" ] || { echo "missing $HERE/$MODULE.tla" >&2; exit 2; }
[ -f "$HERE/$MODULE.cfg" ] || { echo "missing $HERE/$MODULE.cfg" >&2; exit 2; }
cd "$HERE"
java -XX:+UseParallelGC -Xmx8g -cp "$JAR" tlc2.TLC -workers "$WORKERS" -deadlock -cleanup \
     -metadir "$OUT/states" -config "$MODULE.cfg" "$@" "$MODULE.tla" > "$OUT/tlc.log" 2>&1
RC=$?
if grep -q 'Error: Invariant\|Error: Temporal properties were violated\|Error: Action property' "$OUT/tlc.log"; then
  awk '/^Error: /,0' "$OUT/tlc.log" > "$OUT/trace.txt"
  echo "VIOLATION scenario=$SCENARIO (trace: $OUT/trace.txt)"
  exit 1
fi
if grep -q 'Model checking completed. No error has been found' "$OUT/tlc.log"; then
  grep -E 'states generated|distinct states|Finished in' "$OUT/tlc.log" | tail -3
  echo "GREEN scenario=$SCENARIO"
  exit 0
fi
echo "TLC ERROR scenario=$SCENARIO rc=$RC (see $OUT/tlc.log)"
tail -20 "$OUT/tlc.log"
exit 2
```

`-deadlock` disables deadlock reporting: the model's bounded counters make every run end in a state with no
enabled action, which is not a defect.

- [ ] **Step 3: Write `witness.sh`**

```bash
#!/usr/bin/env bash
# Apply the witness of <Property> in MC_<Scenario> and check only that property.
# Exit: 0 iff TLC reports a violation of the property (the witness "passes").
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
SCENARIO="${1:?scenario}"; PROP="${2:?property}"
OUT="$ROOT/tmp/tla/${SCENARIO}_witness_$PROP"
mkdir -p "$OUT"
CFG="$OUT/MC_$SCENARIO.cfg"
# Same constants as the scenario, WITNESS overridden, only the target property checked.
grep -v -E '^(INVARIANT|PROPERTY|WITNESS)' "$HERE/MC_$SCENARIO.cfg" | grep -v -E '^\s*(Assert_|No|Acked|Error|Unknown|Atomicity|Rollback|Committed|Stable|ReadYour|Lock|Single|Active|Flip|Mutation|Legacy|Registered|Nt|Log|Pinned|Client|Retry|Chain|Outdated|RolledBack)' > "$CFG"
sed -i "s/^\(WITNESS *= *\).*/\1\"$PROP\"/" "$CFG"
grep -q '^WITNESS' "$CFG" || echo "WITNESS = \"$PROP\"" | sed 's/^WITNESS = /CONSTANT WITNESS = /' >> "$CFG"
if grep -q "^ *$PROP *$" <(grep -A200 '^PROPERTY' "$HERE/MC_$SCENARIO.cfg" 2>/dev/null); then
  echo "PROPERTY $PROP" >> "$CFG"
else
  echo "INVARIANT $PROP" >> "$CFG"
fi
cp "$HERE"/*.tla "$OUT/"
cd "$OUT"
java -XX:+UseParallelGC -Xmx8g -cp "$ROOT/tmp/tla2tools.jar" tlc2.TLC -workers auto -deadlock -cleanup \
     -metadir "$OUT/states" -config "MC_$SCENARIO.cfg" "MC_$SCENARIO.tla" > "$OUT/tlc.log" 2>&1
if grep -q "Error: Invariant $PROP is violated\|Error: Temporal properties were violated\|Error: Action property $PROP is violated" "$OUT/tlc.log"; then
  echo "WITNESS RED (as required) scenario=$SCENARIO property=$PROP"
  exit 0
fi
echo "WITNESS DID NOT FIRE scenario=$SCENARIO property=$PROP (see $OUT/tlc.log)"
exit 1
```

The property-name filter is deliberately a list of prefixes of every property family in the spec, so that a
scenario config that lists all its properties is reduced to constants plus the one target. `WITNESS` is a
constant of every `MC_*` module (Task 5): the string `""` means no witness, and each witness override in
`Invariants.tla` is guarded by `WITNESS = "<Property>"`.

- [ ] **Step 4: Run the sanity module green, then red**

Run: `chmod +x utils/tla/transactions/*.sh && utils/tla/transactions/run_tlc.sh Sanity`
Expected: `GREEN scenario=Sanity`, exit 0, `tmp/tla2tools.jar` downloaded.

Then edit `MC_Sanity.cfg` to `INVARIANT AlwaysFalse`, run again.
Expected: `VIOLATION scenario=Sanity`, exit 1, `tmp/tla/Sanity/trace.txt` contains the state `x = 3`.
Restore `INVARIANT AlwaysTrue`.

- [ ] **Step 5: Commit**

```bash
git add utils/tla/transactions/run_tlc.sh utils/tla/transactions/witness.sh utils/tla/transactions/Sanity.tla utils/tla/transactions/MC_Sanity.tla utils/tla/transactions/MC_Sanity.cfg
git commit -m "tla(transactions): TLC runner, witness runner, sanity module" -- utils/tla/transactions
```

---

### Task 1: `Types.tla` {#task-1}

**Files:**
- Create: `utils/tla/transactions/Types.tla`
- Create: `utils/tla/transactions/MC_Types.tla`, `MC_Types.cfg`

**Interfaces:**
- Produces: constants `Sessions, Parts, Tasks, Mutations, TID_MAX, CSN_MAX, Covers, BG_TASKS, ...`; values
  `EmptyTID, NonTransactionalTID, DummyTID, UnknownCSN, NonTransactionalCSN, CommittingCSN,
  EverythingVisibleCSN, MaxReservedCSN, RolledBackCSN, FirstCSN`; sets `Tids, RealCSNs, AllCSNs, AllTids`;
  operators `IsNonTransactional(tid)`, `IsTransactional(tid)`, `CoveredBy(p)`, `Expand(S)` (base parts
  reachable from a set of parts through `Covers`), `VersionInfoType`, `EmptyInfo`.

- [ ] **Step 1: Write the module**

```tla
---- MODULE Types ----
EXTENDS Naturals, FiniteSets, Sequences

CONSTANTS
  Sessions,        \* model values k1, k2
  Parts,           \* model values, e.g. P1, P2
  Tasks,           \* model values, background tasks
  Mutations,       \* model values, may be {}
  TID_MAX,         \* bound on Begin
  CSN_MAX,         \* bound on CommitCreateCSN
  Covers,          \* [Parts -> SUBSET Parts], direct children
  RESTARTS_MAX, KEEPER_FAULTS_MAX, DISK_FAULTS_MAX, QUERY_FAULTS_MAX,
  MAX_STORE_RETRIES, NOEXCEPT_RETRY_BUDGET,
  NOEXCEPT_STORE_FAULT_POLICY,   \* "Terminate" | "Retry"
  DISK_MODE,                     \* "Durable" | "Layered"
  FSYNC_PART_DIRECTORY,          \* BOOLEAN
  LEGACY_PARTS,                  \* SUBSET Parts, roots with a Legacy record initially
  WAIT_MODE,                     \* "WAIT" | "WAIT_UNKNOWN" | "ASYNC"
  WITNESS                        \* "" or a property name

ASSUME Covers \in [Parts -> SUBSET Parts]
ASSUME NOEXCEPT_STORE_FAULT_POLICY \in {"Terminate", "Retry"}
ASSUME DISK_MODE \in {"Durable", "Layered"}
ASSUME WAIT_MODE \in {"WAIT", "WAIT_UNKNOWN", "ASYNC"}

\* Transaction identifiers (one host: an integer is enough, start_csn lives in tid_start)
EmptyTID            == 0
NonTransactionalTID == -1
DummyTID            == -2
Tids                == 1..TID_MAX
AllTids             == Tids \cup {EmptyTID, NonTransactionalTID, DummyTID}
IsTransactional(t)  == t \in Tids
IsNonTransactional(t) == t = NonTransactionalTID

\* Commit sequence numbers
UnknownCSN           == 0
NonTransactionalCSN  == 1
CommittingCSN        == 2
EverythingVisibleCSN == 3
MaxReservedCSN       == 32
FirstCSN             == MaxReservedCSN + 1
RolledBackCSN        == CSN_MAX + 1
RealCSNs             == FirstCSN..CSN_MAX
AllCSNs              == {UnknownCSN, NonTransactionalCSN, CommittingCSN, EverythingVisibleCSN, RolledBackCSN} \cup RealCSNs

\* Covering relation helpers
CoveredBy(p) == { q \in Parts : p \in Covers[q] }          \* direct covers of p
RECURSIVE Expand(_)
Expand(S) == IF S = {} THEN {}
             ELSE LET p == CHOOSE q \in S : TRUE
                  IN (IF Covers[p] = {} THEN {p} ELSE Expand(Covers[p])) \cup Expand(S \ {p})
Overlap(p, q) == p /= q /\ (Expand({p}) \cap Expand({q}) /= {})

\* VersionInfo (creation_tid, creation_csn, removal_tid, removal_csn, storing_version)
VersionInfoType == [ctid : AllTids, ccsn : AllCSNs, rtid : AllTids, rcsn : AllCSNs, sv : -1..64]
EmptyInfo == [ctid |-> EmptyTID, ccsn |-> UnknownCSN, rtid |-> EmptyTID, rcsn |-> UnknownCSN, sv |-> -1]

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Min(S) == CHOOSE x \in S : \A y \in S : y >= x
====
```

`MC_Types.tla`:

```tla
---- MODULE MC_Types ----
EXTENDS Types
VARIABLE dummy
Init == dummy = 0
Next == FALSE
Spec == Init /\ [][Next]_dummy
TypesOK == /\ Expand({}) = {}
           /\ \A p \in Parts : Expand({p}) /= {}
           /\ RolledBackCSN > CSN_MAX
====
```

`MC_Types.cfg`:

```
SPECIFICATION Spec
INVARIANT TypesOK
CONSTANTS
  Sessions = {k1, k2}
  Parts = {P1, P2}
  Tasks = {i1}
  Mutations = {}
  TID_MAX = 3
  CSN_MAX = 40
  Covers = [p \in {P1, P2} |-> {}]
  RESTARTS_MAX = 0
  KEEPER_FAULTS_MAX = 0
  DISK_FAULTS_MAX = 0
  QUERY_FAULTS_MAX = 0
  MAX_STORE_RETRIES = 2
  NOEXCEPT_RETRY_BUDGET = 2
  NOEXCEPT_STORE_FAULT_POLICY = "Terminate"
  DISK_MODE = "Durable"
  FSYNC_PART_DIRECTORY = FALSE
  LEGACY_PARTS = {}
  WAIT_MODE = "WAIT"
  WITNESS = ""
```

`Covers` as a function literal in a `.cfg` is not accepted by TLC; define it in `MC_Types.tla` instead:
replace the `Covers = ...` line by nothing and add to `MC_Types.tla` after `EXTENDS`:
`CoversDef == [p \in Parts |-> {}]` and in the cfg `Covers <- CoversDef`. The same pattern is used by every
`MC_*` module.

- [ ] **Step 2: Run**

Run: `utils/tla/transactions/run_tlc.sh Types`
Expected: `GREEN scenario=Types` (one state).

- [ ] **Step 3: Commit**

```bash
git commit -m "tla(transactions): Types module (tids, CSNs, covering relation, VersionInfo)" -- utils/tla/transactions/Types.tla utils/tla/transactions/MC_Types.tla utils/tla/transactions/MC_Types.cfg
```

---

### Task 2: `Keeper.tla` {#task-2}

**Files:**
- Create: `utils/tla/transactions/Keeper.tla`, `MC_Keeper.tla`, `MC_Keeper.cfg`

**Interfaces:**
- Consumes: `Types`.
- Produces: variable set `zk_vars == <<zk_log, zk_seq, zk_tail_ptr, zk_session>>` declared by the root; operators
  `KeeperInit`, `KeeperTypeOK`, `KeeperAppend(tid)` (the effect of a successful sequential `create`, returns
  the new csn as `zk_seq'`), `KeeperEntries` (set of `[csn, tid]`), `KeeperCsnOf(tid)`, `KeeperRemove(csn)`,
  `KeeperSetTail(csn)`, `KeeperExpire`, `KeeperUnchanged`.

Keeper is a module of operators over variables declared in the root, because TLC needs one flat variable
list. Every module of this plan follows that pattern: it `EXTENDS Types` and refers to the root's variables by
name, and the root declares them.

- [ ] **Step 1: Write the module**

```tla
---- MODULE Keeper ----
EXTENDS Types
VARIABLES zk_log, zk_seq, zk_tail_ptr, zk_session
zk_vars == <<zk_log, zk_seq, zk_tail_ptr, zk_session>>

\* zk_log : a function csn -> tid over the csns that exist as znodes (EmptyTID for placeholders)
KeeperInit ==
  /\ zk_log = [c \in {FirstCSN} |-> EmptyTID]        \* loadLogFromZooKeeper's placeholder
  /\ zk_seq = FirstCSN
  /\ zk_tail_ptr = MaxReservedCSN
  /\ zk_session = "Alive"

KeeperTypeOK ==
  /\ zk_seq \in MaxReservedCSN..CSN_MAX
  /\ DOMAIN zk_log \subseteq FirstCSN..zk_seq
  /\ \A c \in DOMAIN zk_log : zk_log[c] \in AllTids
  /\ zk_tail_ptr \in MaxReservedCSN..CSN_MAX
  /\ zk_session \in {"Alive", "Expired"}

KeeperCanAppend == zk_seq < CSN_MAX
KeeperAppend(tid) ==
  /\ KeeperCanAppend
  /\ zk_seq' = zk_seq + 1
  /\ zk_log' = zk_log @@ (zk_seq + 1 :> tid)
KeeperEntries == { [csn |-> c, tid |-> zk_log[c]] : c \in DOMAIN zk_log }
KeeperHas(tid) == \E c \in DOMAIN zk_log : zk_log[c] = tid
KeeperCsnOf(tid) == CHOOSE c \in DOMAIN zk_log : zk_log[c] = tid
KeeperRemove(c) == zk_log' = [d \in DOMAIN zk_log \ {c} |-> zk_log[d]]
KeeperSetTail(c) == zk_tail_ptr' = c
KeeperExpire == zk_session' = "Expired"
KeeperRenew == zk_session' = "Alive"
KeeperUnchanged == UNCHANGED zk_vars

\* Log entries only ever increase in csn and are never rewritten
KeeperMonotone == \A c \in DOMAIN zk_log : c <= zk_seq
====
```

`MC_Keeper.tla` exercises the operators alone:

```tla
---- MODULE MC_Keeper ----
EXTENDS Keeper
CoversDef == [p \in Parts |-> {}]
Init == KeeperInit
Next == \/ \E t \in Tids : KeeperAppend(t) /\ UNCHANGED <<zk_tail_ptr, zk_session>>
        \/ \E c \in DOMAIN zk_log : c /= Max(DOMAIN zk_log) /\ KeeperRemove(c) /\ UNCHANGED <<zk_seq, zk_tail_ptr, zk_session>>
        \/ \E c \in zk_tail_ptr..zk_seq : KeeperSetTail(c) /\ UNCHANGED <<zk_log, zk_seq, zk_session>>
        \/ KeeperExpire /\ UNCHANGED <<zk_log, zk_seq, zk_tail_ptr>>
        \/ KeeperRenew /\ UNCHANGED <<zk_log, zk_seq, zk_tail_ptr>>
Spec == Init /\ [][Next]_zk_vars
Inv == KeeperTypeOK /\ KeeperMonotone
====
```

`MC_Keeper.cfg`: as `MC_Types.cfg` with `SPECIFICATION Spec`, `INVARIANT Inv`, `Covers <- CoversDef`.

- [ ] **Step 2: Run**

Run: `utils/tla/transactions/run_tlc.sh Keeper`
Expected: `GREEN scenario=Keeper`, a few hundred states.

- [ ] **Step 3: Commit**

```bash
git commit -m "tla(transactions): Keeper module (CSN log, tail_ptr, session)" -- utils/tla/transactions/Keeper.tla utils/tla/transactions/MC_Keeper.tla utils/tla/transactions/MC_Keeper.cfg
```

---

### Task 3: `Disk.tla` {#task-3}

**Files:**
- Create: `utils/tla/transactions/Disk.tla`, `MC_Disk.tla`, `MC_Disk.cfg`

**Interfaces:**
- Consumes: `Types`.
- Produces: `disk` (per part) and `mdisk` (per mutation) records; `DiskInit`, `DiskTypeOK`,
  `DiskWriteInfo(p, info)` (the effect of tmp+fsync+rename on `disk[p]`), `DiskRead(p)` (what a reader sees:
  `cached` in `Layered`, the single layer in `Durable`), `DiskDurable(p)`, `DiskFsync(p)`, `DiskCrashEffect`
  (the new value of `disk` after a crash), `DiskCreateDir(p)`, `DiskRemoveDir(p)`, `DiskTmpOnly(p)`.

- [ ] **Step 1: Write the module**

```tla
---- MODULE Disk ----
EXTENDS Types
VARIABLES disk, mdisk
disk_vars == <<disk, mdisk>>

NoRecord == "None"
Legacy == "Legacy"
RecordType == VersionInfoType \cup {NoRecord, Legacy}
\* One part: durable and cached copies of txn_version.txt, of txn_version.txt.tmp, and of the directory.
PartDiskType == [durable : RecordType, cached : RecordType,
                 tmp_durable : BOOLEAN, tmp_cached : BOOLEAN,
                 dir_durable : BOOLEAN, dir_cached : BOOLEAN]
MutDiskType == [file_durable : BOOLEAN, file_cached : BOOLEAN,
                tid : AllTids, csn_durable : AllCSNs, csn_cached : AllCSNs]

AbsentPart == [durable |-> NoRecord, cached |-> NoRecord, tmp_durable |-> FALSE, tmp_cached |-> FALSE,
               dir_durable |-> FALSE, dir_cached |-> FALSE]
LegacyPart == [durable |-> Legacy, cached |-> Legacy, tmp_durable |-> FALSE, tmp_cached |-> FALSE,
               dir_durable |-> TRUE, dir_cached |-> TRUE]
AbsentMut == [file_durable |-> FALSE, file_cached |-> FALSE, tid |-> EmptyTID,
              csn_durable |-> UnknownCSN, csn_cached |-> UnknownCSN]

DiskInit ==
  /\ disk = [p \in Parts |-> IF p \in LEGACY_PARTS THEN LegacyPart ELSE AbsentPart]
  /\ mdisk = [m \in Mutations |-> AbsentMut]

DiskTypeOK == /\ disk \in [Parts -> PartDiskType]
              /\ mdisk \in [Mutations -> MutDiskType]
              /\ DISK_MODE = "Layered" \/ \A p \in Parts : disk[p].durable = disk[p].cached
                                                          /\ disk[p].tmp_durable = disk[p].tmp_cached
                                                          /\ disk[p].dir_durable = disk[p].dir_cached

Layered == DISK_MODE = "Layered"
DiskRead(p) == disk[p].cached
DiskDurable(p) == disk[p].durable
DiskDirExists(p) == disk[p].dir_cached
DiskTmpOnly(p) == disk[p].cached = NoRecord /\ disk[p].tmp_cached

\* storeInfoToDataPartStorage: write tmp, fsync tmp, rename. The rename is cached; it is durable at once only
\* with FSYNC_PART_DIRECTORY (directory fsync) or in Durable mode.
DiskWriteInfo(p, info) ==
  disk' = [disk EXCEPT ![p] =
    [@ EXCEPT !.cached = info,
              !.tmp_cached = FALSE,
              !.tmp_durable = IF Layered /\ ~FSYNC_PART_DIRECTORY THEN TRUE ELSE FALSE,
              !.durable = IF Layered /\ ~FSYNC_PART_DIRECTORY THEN @ ELSE info]]
DiskCreateDir(p) ==
  disk' = [disk EXCEPT ![p].dir_cached = TRUE, ![p].dir_durable = IF Layered THEN @ ELSE TRUE]
DiskRemoveDir(p) == disk' = [disk EXCEPT ![p] = AbsentPart]
DiskFsync(p) ==
  disk' = [disk EXCEPT ![p].durable = @, ![p].durable = disk[p].cached,
                       ![p].tmp_durable = disk[p].tmp_cached, ![p].dir_durable = disk[p].dir_cached]
\* After a crash the cached layers are reset to the durable ones
DiskCrashEffect == [p \in Parts |-> [disk[p] EXCEPT !.cached = disk[p].durable,
                                                     !.tmp_cached = disk[p].tmp_durable,
                                                     !.dir_cached = disk[p].dir_durable]]
MutDiskCrashEffect == [m \in Mutations |-> [mdisk[m] EXCEPT !.file_cached = mdisk[m].file_durable,
                                                            !.csn_cached = mdisk[m].csn_durable]]
DiskUnchanged == UNCHANGED disk_vars
====
```

`MC_Disk.tla`:

```tla
---- MODULE MC_Disk ----
EXTENDS Disk
CoversDef == [p \in Parts |-> {}]
VARIABLE written   \* the last info written per part, to check durability claims
Init == DiskInit /\ written = [p \in Parts |-> NoRecord]
SomeInfo(n) == [EmptyInfo EXCEPT !.ctid = 1, !.sv = n]
Next == \/ \E p \in Parts, n \in 0..2 : DiskWriteInfo(p, SomeInfo(n)) /\ written' = [written EXCEPT ![p] = SomeInfo(n)] /\ UNCHANGED mdisk
        \/ \E p \in Parts : DiskFsync(p) /\ UNCHANGED <<mdisk, written>>
        \/ disk' = DiskCrashEffect /\ UNCHANGED <<mdisk, written>>
Spec == Init /\ [][Next]_<<disk, mdisk, written>>
\* what a reader sees is always the last write until a crash, and durable never runs ahead of cached
Inv == /\ DiskTypeOK
       /\ \A p \in Parts : disk[p].durable \in {NoRecord, disk[p].cached, written[p]} \/ Layered
====
```

`MC_Disk.cfg`: two configs are needed, one with `DISK_MODE = "Layered"` and `FSYNC_PART_DIRECTORY = FALSE`,
one with `DISK_MODE = "Durable"`; name the second `MC_DiskDurable` (a copy of the module with the other
constant). Both check `Inv`.

- [ ] **Step 2: Run both**

Run: `utils/tla/transactions/run_tlc.sh Disk && utils/tla/transactions/run_tlc.sh DiskDurable`
Expected: both `GREEN`.

- [ ] **Step 3: Commit**

```bash
git commit -m "tla(transactions): Disk module (layered records, tmp/rename/fsync, crash effect)" -- utils/tla/transactions/Disk.tla utils/tla/transactions/MC_Disk.tla utils/tla/transactions/MC_Disk.cfg utils/tla/transactions/MC_DiskDurable.tla utils/tla/transactions/MC_DiskDurable.cfg
```

---

### Task 4: `History.tla` with the visibility oracle {#task-4}

**Files:**
- Create: `utils/tla/transactions/History.tla`

**Interfaces:**
- Consumes: `Types`.
- Produces: history variables (spec, "History"): `h_outcome, h_committed, h_csn, h_snapshot, h_loaded,
  h_creating, h_removing, h_mutations, h_rolled_back, h_unknown, h_removers, h_selected, h_content,
  h_truncated, h_batch, h_batch_outcome, h_prepared_files, down_cause`; `HistoryInit`, `HistoryTypeOK`,
  `OracleVisible(p, s, u, ctid)` (needs the creator of `p`, passed by the caller), `HistoryUnchanged`.

- [ ] **Step 1: Write the module**

```tla
---- MODULE History ----
EXTENDS Types
VARIABLES h_outcome, h_committed, h_csn, h_snapshot, h_loaded, h_creating, h_removing, h_mutations,
          h_rolled_back, h_unknown, h_removers, h_selected, h_content, h_truncated,
          h_batch, h_batch_outcome, h_prepared_files, down_cause
h_vars == <<h_outcome, h_committed, h_csn, h_snapshot, h_loaded, h_creating, h_removing, h_mutations,
            h_rolled_back, h_unknown, h_removers, h_selected, h_content, h_truncated,
            h_batch, h_batch_outcome, h_prepared_files, down_cause>>

Outcomes == {"None", "Acked", "Error", "UnknownStatus"}
NoBatch == [targets |-> <<>>, before |-> <<>>]

HistoryInit ==
  /\ h_outcome = [t \in Tids |-> "None"]
  /\ h_committed = {}
  /\ h_csn = [t \in Tids \cup {NonTransactionalTID} |-> IF t = NonTransactionalTID THEN NonTransactionalCSN ELSE UnknownCSN]
  /\ h_snapshot = [t \in Tids |-> UnknownCSN]
  /\ h_loaded = [t \in Tids |-> FALSE]
  /\ h_creating = [t \in Tids |-> {}]
  /\ h_removing = [t \in Tids |-> {}]
  /\ h_mutations = [t \in Tids |-> {}]
  /\ h_rolled_back = [t \in Tids |-> FALSE]
  /\ h_unknown = [t \in Tids |-> "None"]
  /\ h_removers = [p \in Parts |-> {}]
  /\ h_selected = {}
  /\ h_content = [t \in Tids |-> {}]
  /\ h_truncated = {}
  /\ h_batch = NoBatch
  /\ h_batch_outcome = "None"
  /\ h_prepared_files = {}
  /\ down_cause = "None"

HistoryTypeOK ==
  /\ h_outcome \in [Tids -> Outcomes]
  /\ h_committed \subseteq Tids
  /\ h_csn \in [Tids \cup {NonTransactionalTID} -> AllCSNs]
  /\ h_snapshot \in [Tids -> AllCSNs]
  /\ h_loaded \in [Tids -> BOOLEAN]
  /\ h_creating \in [Tids -> SUBSET Parts]
  /\ h_removing \in [Tids -> SUBSET Parts]
  /\ h_mutations \in [Tids -> SUBSET Mutations]
  /\ h_rolled_back \in [Tids -> BOOLEAN]
  /\ h_unknown \in [Tids -> {"None", "Committed", "RolledBack"}]
  /\ h_removers \in [Parts -> SUBSET (Tids \cup {NonTransactionalTID})]
  /\ h_selected \subseteq (Mutations \X Parts \X BOOLEAN)
  /\ h_content \in [Tids -> SUBSET (Parts \X Nat)]
  /\ h_truncated \subseteq Tids
  /\ h_batch_outcome \in {"None", "Done", "Refused"}
  /\ h_prepared_files \subseteq Mutations
  /\ down_cause \in {"None", "StoreFault", "RetryExhausted", "Other"}

\* The declarative visibility oracle (spec, invariants preamble). ctid is the creator of p.
OracleVisible(p, s, u, ctid) ==
  /\ \/ ctid = u
     \/ ctid = NonTransactionalTID
     \/ ctid \in h_committed /\ h_csn[ctid] <= s
  /\ ~ (u \in Tids /\ p \in h_removing[u])
  /\ ~ (\E r \in h_removers[p] : r = NonTransactionalTID \/ h_csn[r] <= s)

HistoryUnchanged == UNCHANGED h_vars
====
```

- [ ] **Step 2: Type-check by extending it from `MC_Types`**

Add `EXTENDS History` to a throwaway copy of `MC_Types` (or simply parse): run
`java -cp tmp/tla2tools.jar tla2sany.SANY utils/tla/transactions/History.tla > tmp/tla/sany_history.log 2>&1`
Expected: `Semantic processing of module History` with no errors in the log.

- [ ] **Step 3: Commit**

```bash
git commit -m "tla(transactions): History module (ghost state, visibility oracle)" -- utils/tla/transactions/History.tla
```

---

### Task 5: The state schema `MergeTreeTransactions.tla` with `TypeOK` and action stubs {#task-5}

**Files:**
- Create: `utils/tla/transactions/Parts.tla` (types and stubs only in this task; operators in Task 6)
- Create: `utils/tla/transactions/Server.tla` (all action names as stubs)
- Create: `utils/tla/transactions/Invariants.tla` (empty shell with `WITNESS` guards)
- Create: `utils/tla/transactions/MergeTreeTransactions.tla`
- Create: `utils/tla/transactions/MC_Schema.tla`, `MC_Schema.cfg`

**Interfaces:**
- Produces: every variable of the spec, `vars`, `TypeOK`, `Init`, `Next` as a disjunction of every action
  name in the spec's code map (stubs are `FALSE`), `Spec`. Later tasks and plans replace stubs; they never add
  variables without extending `TypeOK`.

- [ ] **Step 1: Write `Parts.tla` types**

```tla
---- MODULE Parts ----
EXTENDS Types, Disk, History
VARIABLES part      \* [Parts -> PartRecord]

PStates == {"Absent", "Temporary", "PreActive", "Active", "Outdated", "Deleting", "Deleted"}
Pins == {"Merge", "MutationExec"} \cup { <<"Txn", t>> : t \in Tids } \cup { <<"Rollback", t>> : t \in Tids }
        \cup { <<"Select", k>> : k \in Sessions } \cup { <<"Task", i>> : i \in Tasks }
FrameOwners == { <<"Session", k>> : k \in Sessions } \cup { <<"Task", i>> : i \in Tasks } \cup {"Updater", "Cleanup", "Restart"}
FrameType == [owner : FrameOwners, tentative : VersionInfoType, pc : {"Read", "Persist", "Publish"},
              retries : 0..MAX_STORE_RETRIES, interferences : 0..MAX_STORE_RETRIES,
              noexcept_retries : 0..NOEXCEPT_RETRY_BUDGET, noexcept_owner : BOOLEAN]
PartRecord == [pstate : PStates, mem : VersionInfoType, lock : AllTids,
               deferrable : BOOLEAN, deferred : VersionInfoType \cup {"None"},
               pins : SUBSET Pins, frames : SUBSET FrameType,
               payload : [ver : Nat, tomb : BOOLEAN]]
AbsentPartRecord == [pstate |-> "Absent", mem |-> EmptyInfo, lock |-> EmptyTID, deferrable |-> TRUE,
                     deferred |-> "None", pins |-> {}, frames |-> {}, payload |-> [ver |-> 0, tomb |-> FALSE]]
PartsInit == part = [p \in Parts |-> AbsentPartRecord]
PartsTypeOK == part \in [Parts -> PartRecord]
====
```

`lock` uses `AllTids` with `EmptyTID` meaning `0` (unlocked). `frames` is a set; two frames of the same owner
on one part never coexist, which `TypeOK` does not need to say.

- [ ] **Step 2: Write `Server.tla` stubs**

Every action of the spec's code map, as `Name == FALSE` or `Name(args) == FALSE`, grouped and commented
with the spec anchor, so that `Next` in the root is complete. The full list (arguments in parentheses):

```tla
---- MODULE Server ----
EXTENDS Parts, Keeper
VARIABLES txn, tid_start, tid_to_csn, latest_snapshot, local_tid_counter, last_loaded_entry, running_list,
          snapshots_in_use, tail_ptr, updated_tail_ptr, unknown_state_list, unknown_state_list_loaded,
          server, server_completely_started, async_loading_jobs, loaded_parts, loaded_mutations,
          restarts, keeper_faults, disk_faults, query_faults,
          merges_blocker, reserved, parts_lock, nt_batch,
          client, stmt, mut, task, updater_pc, cleanup_pc

\* --- client and session (spec #actions-client)
Begin(k) == FALSE
SetSnapshot(k, c) == FALSE
InsertWrite(k, p) == FALSE
InsertPreActive(k, p) == FALSE
PublishStart(a, p) == FALSE
PublishEnrol(a, q) == FALSE
PublishStore(a, q) == FALSE
PublishFlip(a) == FALSE
StmtRollback(a) == FALSE
SelectCapture(k) == FALSE
SelectCheck(k, p) == FALSE
SelectFinish(k) == FALSE
DropStart(k) == FALSE
DropEnrol(k, p) == FALSE
DropStore(k, p) == FALSE
DropOutdate(k) == FALSE
MutPrepareWrite(k, m) == FALSE
MutPrepareAttach(k, m) == FALSE
MutRegister(k, m) == FALSE
CommitBefore(k) == FALSE
CommitCreateCSN(k) == FALSE
CommitReadOnly(k) == FALSE
CommitStoreCreation(k, p) == FALSE
CommitStoreRemoval(k, p) == FALSE
CommitStoreMutation(k, m) == FALSE
CommitFlip(k) == FALSE
CommitFinalize(k) == FALSE
CommitAck(k) == FALSE
CommitUnknown(k) == FALSE
CommitError(k) == FALSE
Refuse(k) == FALSE
RollbackStart(k) == FALSE
RollbackCopyLists(k) == FALSE
RollbackKill(k, m) == FALSE
RollbackMarkCreated(k, p) == FALSE
RollbackOutdateCreated(k, p) == FALSE
RollbackRestore(k, p) == FALSE
RollbackUnlock(k, p) == FALSE
RollbackFinalize(k) == FALSE
KillTransaction(k, t) == FALSE
KillMutation(k, m) == FALSE
Fail(k) == FALSE
\* --- updating thread (spec #actions-updating)
UpdReconnect == FALSE
UpdLoadEntriesMap == FALSE
UpdPublishSnapshot == FALSE
UpdRemoveOldEntriesSetTail == FALSE
UpdRemoveOldEntriesDelete(c) == FALSE
UpdSwapUnknownLists == FALSE
UpdFinalizeUnknown(t) == FALSE
\* --- cleanup thread (spec #actions-cleanup)
CleanupGrab(p) == FALSE
CleanupValidate(p) == FALSE
CleanupDeleteOk(p) == FALSE
CleanupDeleteFail(p) == FALSE
\* --- background tasks: merge (spec #actions-merge) and mutation (spec #actions-mutation)
MergeBegin(i) == FALSE
MergeSelect(i) == FALSE
MergeWrite(i) == FALSE
MergeRename(i) == FALSE
MergeFail(i) == FALSE
MutSelect(i, m, p) == FALSE
MutWrite(i, m, p) == FALSE
MutRename(i, m, p) == FALSE
MutWait(k, m) == FALSE
MutFail(i) == FALSE
MutDestroyOwner(m) == FALSE
KillUnregister(m) == FALSE
KillRollbackTxn(m) == FALSE
KillCancelTask(m, i) == FALSE
KillRemoveFile(m) == FALSE
\* --- non-transactional (spec #actions-nontransactional)
NtInsert(p) == FALSE
NtBatchStart(B) == FALSE
NtBatchPreflight(p) == FALSE
NtBatchLock(p) == FALSE
NtBatchStore(p) == FALSE
NtBatchEnd == FALSE
NtDropCover == FALSE
\* --- disk and store (spec #actions-disk)
StoreRead(p, o) == FALSE
StorePersist(p, o) == FALSE
StorePublish(p, o) == FALSE
Fsync(p) == FALSE
StoreRetry(p, o) == FALSE
KillRetry(m) == FALSE
\* --- failures and restart (spec #failures, #actions-restart)
Crash == FALSE
ProcessDown(cause) == FALSE
RestartLoadLog == FALSE
RestartTableStart == FALSE
RestartLoadPart(p) == FALSE
RestartLoadMutation(m) == FALSE
RestartTablePublished == FALSE
RestartOutdatedDone == FALSE
RestartDone == FALSE
====
```

(`RollbackKill(k, m)` stands for the `RollbackKill*` group, which plan 4 expands into the four `Kill*` steps
driven by the rollback; `StoreRead(p, o)` takes the frame owner `o` rather than a frame value.)

- [ ] **Step 3: Write the root module**

```tla
---- MODULE MergeTreeTransactions ----
EXTENDS Server, Invariants

vars == <<zk_vars, disk_vars, h_vars, part,
          txn, tid_start, tid_to_csn, latest_snapshot, local_tid_counter, last_loaded_entry, running_list,
          snapshots_in_use, tail_ptr, updated_tail_ptr, unknown_state_list, unknown_state_list_loaded,
          server, server_completely_started, async_loading_jobs, loaded_parts, loaded_mutations,
          restarts, keeper_faults, disk_faults, query_faults,
          merges_blocker, reserved, parts_lock, nt_batch,
          client, stmt, mut, task, updater_pc, cleanup_pc>>

TxnStates == {"Absent", "Running", "Committing", "Committed", "RolledBack"}
Holders == { <<"Session", k>> : k \in Sessions } \cup { <<"Task", i>> : i \in Tasks }
TxnPcs == {"Idle", "CommitBefore", "CommitCreateCSN", "CommitStoreCreation", "CommitStoreRemoval",
           "CommitStoreMutation", "CommitFlip", "CommitFinalize", "RollbackCopyLists", "RollbackKill",
           "RollbackMarkCreated", "RollbackOutdateCreated", "RollbackRestore", "RollbackUnlock", "RollbackFinalize"}
TxnRecord == [state : TxnStates, csn : AllCSNs, snapshot : AllCSNs, protected_snapshot : AllCSNs,
              creating : Seq(Parts), removing : Seq(Parts), mutations : SUBSET Mutations,
              holders : SUBSET Holders, mutex : Holders \cup {"None"}, csn_notified : BOOLEAN,
              pc : TxnPcs, work : Seq(Parts)]
AbsentTxn == [state |-> "Absent", csn |-> UnknownCSN, snapshot |-> UnknownCSN, protected_snapshot |-> UnknownCSN,
              creating |-> <<>>, removing |-> <<>>, mutations |-> {}, holders |-> {}, mutex |-> "None",
              csn_notified |-> FALSE, pc |-> "Idle", work |-> <<>>]

ClientPcs == {"Idle", "InsertWrite", "InsertPreActive", "PublishStart", "PublishEnrol", "PublishStore",
              "PublishFlip", "SelectCapture", "SelectCheck", "SelectFinish", "DropStart", "DropEnrol",
              "DropStore", "DropOutdate", "MutPrepareWrite", "MutPrepareAttach", "MutRegister",
              "Commit", "Rollback"}
ReadType == [parts : SUBSET Parts, frags : SUBSET (Parts \X Nat)]
NoRead == [parts |-> {}, frags |-> {}]
ClientRecord == [current : Tids \cup {EmptyTID}, outcome : Outcomes, outcome_tid : Tids \cup {EmptyTID},
                 last_error : {"None", "SERIALIZATION_ERROR", "INVALID_TRANSACTION", "STALE_VERSION", "LOGICAL_ERROR", "Injected"},
                 first_read : ReadType, last_read : ReadType, capture : SUBSET Parts, checked : SUBSET Parts,
                 waiting : {"None", "ForState", "ForLoad"}, pc : ClientPcs, work : Seq(Parts), part : Parts \cup {"None"}]
IdleClient == [current |-> EmptyTID, outcome |-> "None", outcome_tid |-> EmptyTID, last_error |-> "None",
               first_read |-> NoRead, last_read |-> NoRead, capture |-> {}, checked |-> {},
               waiting |-> "None", pc |-> "Idle", work |-> <<>>, part |-> "None"]

Actors == { <<"Session", k>> : k \in Sessions } \cup { <<"Task", i>> : i \in Tasks }
StmtRecord == [precommitted : SUBSET Parts, covered : SUBSET Parts, attached : SUBSET Parts]
NoStmt == [precommitted |-> {}, covered |-> {}, attached |-> {}]

MutStates == {"Absent", "Written", "Attached", "Registered", "Unregistered", "Killed"}
MutRecord == [mstate : MutStates, tasks : SUBSET Tasks, tid : AllTids, csn : AllCSNs,
              fail_reason : {"None", "Deadlock"}, file_owner : SUBSET {"Preparing", "Map"}]
AbsentMut == [mstate |-> "Absent", tasks |-> {}, tid |-> EmptyTID, csn |-> UnknownCSN,
              fail_reason |-> "None", file_owner |-> {}]

TaskRecord == [kind : {"Idle", "Merge", "Mutation"}, pc : {"Idle", "Select", "Write", "Rename", "Publish", "Commit", "Fail"},
               txn : Tids \cup {EmptyTID}, mutation : Mutations \cup {"None"}, source : Parts \cup {"None"}]
IdleTask == [kind |-> "Idle", pc |-> "Idle", txn |-> EmptyTID, mutation |-> "None", source |-> "None"]

BatchType == [targets : Seq(Parts), cursor : Nat, phase : {"Lock", "Store"}, locked : SUBSET Parts, skipped : SUBSET Parts]

TypeOK ==
  /\ KeeperTypeOK /\ DiskTypeOK /\ HistoryTypeOK /\ PartsTypeOK
  /\ txn \in [Tids -> TxnRecord]
  /\ tid_start \in [Tids -> AllCSNs]
  /\ tid_to_csn \in [Tids -> AllCSNs]                  \* UnknownCSN = absent
  /\ latest_snapshot \in AllCSNs
  /\ local_tid_counter \in 0..TID_MAX
  /\ last_loaded_entry \in AllCSNs
  /\ running_list \subseteq Tids
  /\ snapshots_in_use \in [Tids -> AllCSNs]            \* UnknownCSN = not in the bag; the bag is indexed by tid
  /\ tail_ptr \in AllCSNs
  /\ updated_tail_ptr \in BOOLEAN
  /\ unknown_state_list \subseteq Tids
  /\ unknown_state_list_loaded \subseteq Tids
  /\ server \in {"Down", "LogUp", "TableLoading", "TableUp"}
  /\ server_completely_started \in BOOLEAN
  /\ async_loading_jobs \in 0..1
  /\ loaded_parts \subseteq Parts
  /\ loaded_mutations \subseteq Mutations
  /\ restarts \in 0..RESTARTS_MAX
  /\ keeper_faults \in 0..KEEPER_FAULTS_MAX
  /\ disk_faults \in 0..DISK_FAULTS_MAX
  /\ query_faults \in 0..QUERY_FAULTS_MAX
  /\ merges_blocker \in Nat
  /\ reserved \in [Tasks -> SUBSET Parts]
  /\ parts_lock \in Actors \cup {"None"}
  /\ nt_batch \in BatchType \cup {"None"}
  /\ client \in [Sessions -> ClientRecord]
  /\ stmt \in [Actors -> StmtRecord]
  /\ mut \in [Mutations -> MutRecord]
  /\ task \in [Tasks -> TaskRecord]
  /\ updater_pc \in {"Idle", "Reconnect", "LoadMap", "PublishSnapshot", "SetTail", "Delete", "Swap", "Finalize"}
  /\ cleanup_pc \in {"Idle", "Grab", "Validate", "Delete"}

Init ==
  /\ KeeperInit /\ DiskInit /\ HistoryInit /\ PartsInit
  /\ txn = [t \in Tids |-> AbsentTxn]
  /\ tid_start = [t \in Tids |-> UnknownCSN]
  /\ tid_to_csn = [t \in Tids |-> UnknownCSN]
  /\ latest_snapshot = FirstCSN
  /\ local_tid_counter = 0
  /\ last_loaded_entry = FirstCSN
  /\ running_list = {}
  /\ snapshots_in_use = [t \in Tids |-> UnknownCSN]
  /\ tail_ptr = MaxReservedCSN
  /\ updated_tail_ptr = FALSE
  /\ unknown_state_list = {} /\ unknown_state_list_loaded = {}
  /\ server = "TableUp" /\ server_completely_started = TRUE /\ async_loading_jobs = 0
  /\ loaded_parts = Parts /\ loaded_mutations = Mutations
  /\ restarts = 0 /\ keeper_faults = 0 /\ disk_faults = 0 /\ query_faults = 0
  /\ merges_blocker = 0 /\ reserved = [i \in Tasks |-> {}] /\ parts_lock = "None" /\ nt_batch = "None"
  /\ client = [k \in Sessions |-> IdleClient]
  /\ stmt = [a \in Actors |-> NoStmt]
  /\ mut = [m \in Mutations |-> AbsentMut]
  /\ task = [i \in Tasks |-> IdleTask]
  /\ updater_pc = "Idle" /\ cleanup_pc = "Idle"

Next ==
  \/ \E k \in Sessions :
       \/ Begin(k) \/ CommitBefore(k) \/ CommitCreateCSN(k) \/ CommitReadOnly(k) \/ CommitFlip(k)
       \/ CommitFinalize(k) \/ CommitAck(k) \/ CommitUnknown(k) \/ CommitError(k) \/ Refuse(k) \/ Fail(k)
       \/ RollbackStart(k) \/ RollbackCopyLists(k) \/ RollbackFinalize(k)
       \/ SelectCapture(k) \/ SelectFinish(k) \/ DropStart(k) \/ DropOutdate(k)
       \/ \E p \in Parts : InsertWrite(k, p) \/ InsertPreActive(k, p) \/ SelectCheck(k, p)
                           \/ DropEnrol(k, p) \/ DropStore(k, p)
                           \/ CommitStoreCreation(k, p) \/ CommitStoreRemoval(k, p)
                           \/ RollbackMarkCreated(k, p) \/ RollbackOutdateCreated(k, p)
                           \/ RollbackRestore(k, p) \/ RollbackUnlock(k, p)
       \/ \E c \in RealCSNs \cup {NonTransactionalCSN, EverythingVisibleCSN} : SetSnapshot(k, c)
       \/ \E m \in Mutations : MutPrepareWrite(k, m) \/ MutPrepareAttach(k, m) \/ MutRegister(k, m)
                               \/ CommitStoreMutation(k, m) \/ RollbackKill(k, m) \/ MutWait(k, m) \/ KillMutation(k, m)
       \/ \E t \in Tids : KillTransaction(k, t)
  \/ \E a \in Actors : PublishStart(a, CHOOSE p \in Parts : TRUE) \/ PublishFlip(a) \/ StmtRollback(a)
                       \/ \E p \in Parts : PublishStart(a, p) \/ PublishEnrol(a, p) \/ PublishStore(a, p)
  \/ UpdReconnect \/ UpdLoadEntriesMap \/ UpdPublishSnapshot \/ UpdRemoveOldEntriesSetTail \/ UpdSwapUnknownLists
  \/ \E c \in RealCSNs : UpdRemoveOldEntriesDelete(c)
  \/ \E t \in Tids : UpdFinalizeUnknown(t)
  \/ \E p \in Parts : CleanupGrab(p) \/ CleanupValidate(p) \/ CleanupDeleteOk(p) \/ CleanupDeleteFail(p) \/ Fsync(p)
                      \/ NtInsert(p) \/ NtBatchPreflight(p) \/ NtBatchLock(p) \/ NtBatchStore(p)
                      \/ \E o \in FrameOwners : StoreRead(p, o) \/ StorePersist(p, o) \/ StorePublish(p, o) \/ StoreRetry(p, o)
  \/ \E i \in Tasks : MergeBegin(i) \/ MergeSelect(i) \/ MergeWrite(i) \/ MergeRename(i) \/ MergeFail(i) \/ MutFail(i)
                      \/ \E m \in Mutations, p \in Parts : MutSelect(i, m, p) \/ MutWrite(i, m, p) \/ MutRename(i, m, p)
                      \/ \E m \in Mutations : KillCancelTask(m, i)
  \/ \E m \in Mutations : MutDestroyOwner(m) \/ KillUnregister(m) \/ KillRollbackTxn(m) \/ KillRemoveFile(m) \/ KillRetry(m)
                          \/ RestartLoadMutation(m)
  \/ \E B \in SUBSET Parts : NtBatchStart(B)
  \/ NtBatchEnd \/ NtDropCover
  \/ Crash \/ \E c \in {"StoreFault", "RetryExhausted", "Other"} : ProcessDown(c)
  \/ RestartLoadLog \/ RestartTableStart \/ RestartTablePublished \/ RestartOutdatedDone \/ RestartDone
  \/ \E p \in Parts : RestartLoadPart(p)

Spec == Init /\ [][Next]_vars
====
```

`Invariants.tla` in this task is the shell:

```tla
---- MODULE Invariants ----
EXTENDS Server
Witness(name) == WITNESS = name
====
```

`MC_Schema.tla`:

```tla
---- MODULE MC_Schema ----
EXTENDS MergeTreeTransactions
CoversDef == [p \in Parts |-> {}]
====
```

`MC_Schema.cfg`: `SPECIFICATION Spec`, `INVARIANT TypeOK`, the constants of `MC_Types.cfg` with
`Tasks = {i1, i2}` and `Covers <- CoversDef`.

- [ ] **Step 4: Run the schema check**

Run: `utils/tla/transactions/run_tlc.sh Schema`
Expected: `GREEN scenario=Schema`, exactly 1 state (every action is `FALSE`). A `TypeOK` failure here means
`Init` and a record type disagree; fix the record, not `TypeOK`.

- [ ] **Step 5: Commit**

```bash
git commit -m "tla(transactions): state schema, TypeOK, Init, Next with every action stubbed" -- utils/tla/transactions/Parts.tla utils/tla/transactions/Server.tla utils/tla/transactions/Invariants.tla utils/tla/transactions/MergeTreeTransactions.tla utils/tla/transactions/MC_Schema.tla utils/tla/transactions/MC_Schema.cfg
```

---

### Task 6: `Parts.tla` operators: `isVisible`, the three-step store, `canBeRemoved` {#task-6}

**Files:**
- Modify: `utils/tla/transactions/Parts.tla`
- Modify: `utils/tla/transactions/Server.tla` (replace the `Store*` and `Fsync` stubs)
- Create: `utils/tla/transactions/MC_Store.tla`, `MC_Store.cfg`

**Interfaces:**
- Consumes: Task 5's records.
- Produces: `InfoIsVisible(info, s, u)` (the fast path of `VersionInfo::isVisible`, returns
  `"TRUE" | "FALSE" | "UNKNOWN"`), `IsVisibleImpl(p, s, u)` (the full `VersionMetadata::isVisible` over
  `part[p].mem` and `tid_to_csn`), `CanBeRemovedImpl(p)`, `ValidateInfoOK(info, p)` (the `validateInfo`
  predicate), `UpdateCsnIfNeeded(info)`; the store actions `StoreRead(p, o)`, `StorePersist(p, o)`,
  `StorePublish(p, o)`, `Fsync(p)`; the helper `BeginStore(p, o, f)` that a caller uses to start a store with
  update function `f` (a function from `VersionInfoType` to `VersionInfoType`), encoded as the caller writing
  the frame with its `tentative` already updated.

- [ ] **Step 1: Write the visibility and validation operators**

Append to `Parts.tla` (before `====`):

```tla
\* VersionInfo::isVisible fast path (spec #actions-client, code VersionInfo.cpp)
InfoIsVisible(info, s, u) ==
  IF info.rtid = NonTransactionalTID THEN "FALSE"
  ELSE IF u = NonTransactionalTID THEN (IF info.rtid = EmptyTID THEN "TRUE" ELSE "FALSE")
  ELSE IF s = EverythingVisibleCSN THEN "TRUE"
  ELSE IF info.ccsn /= UnknownCSN /\ s < info.ccsn THEN "FALSE"
  ELSE IF info.rcsn /= UnknownCSN /\ info.rcsn <= s THEN "FALSE"
  ELSE IF u /= EmptyTID /\ info.rtid = u THEN "FALSE"
  ELSE IF info.ccsn /= UnknownCSN /\ info.ccsn <= s /\ info.rtid = EmptyTID THEN "TRUE"
  ELSE IF info.ccsn /= UnknownCSN /\ info.ccsn <= s /\ info.rcsn /= UnknownCSN /\ s < info.rcsn THEN "TRUE"
  ELSE IF u /= EmptyTID /\ info.ctid = u THEN "TRUE"
  ELSE "UNKNOWN"

\* TransactionLog::getCSN over the loaded map
LookupCsn(t) == IF t = NonTransactionalTID THEN NonTransactionalCSN
                ELSE IF t \in Tids THEN tid_to_csn[t] ELSE UnknownCSN

\* VersionMetadata::isVisible: fast path, then the slow path with the log lookup
IsVisibleImpl(p, s, u) ==
  LET info == part[p].mem
      fast == InfoIsVisible(info, s, u)
  IN IF fast /= "UNKNOWN" THEN fast = "TRUE"
     ELSE IF s <= tid_start[info.ctid] THEN FALSE
     ELSE LET ccsn == IF info.ccsn /= UnknownCSN THEN info.ccsn ELSE LookupCsn(info.ctid)
          IN IF ccsn = UnknownCSN THEN FALSE
             ELSE LET rcsn == IF info.rtid = EmptyTID THEN info.rcsn ELSE LookupCsn(info.rtid)
                  IN ccsn <= s /\ (rcsn = UnknownCSN \/ s < rcsn)

OldestSnapshot == IF running_list = {} THEN latest_snapshot
                  ELSE Min({ snapshots_in_use[t] : t \in running_list })

\* VersionMetadata::canBeRemoved
CanBeRemovedImpl(p) ==
  LET info == part[p].mem IN
  IF info.rtid = NonTransactionalTID THEN TRUE
  ELSE IF info.ccsn = RolledBackCSN THEN TRUE
  ELSE IF info.rtid = EmptyTID THEN FALSE
  ELSE LET ccsn == IF info.ccsn /= UnknownCSN THEN info.ccsn ELSE LookupCsn(info.ctid) IN
       IF ccsn = UnknownCSN THEN FALSE
       ELSE IF OldestSnapshot < ccsn THEN FALSE
       ELSE IF info.rcsn /= UnknownCSN /\ info.rcsn <= OldestSnapshot THEN TRUE
       ELSE LET rcsn == IF info.rcsn /= UnknownCSN THEN info.rcsn ELSE LookupCsn(info.rtid) IN
            rcsn /= UnknownCSN /\ rcsn <= OldestSnapshot

\* VersionMetadata::validateInfo, the exempt shape included
ValidateInfoOK(info) ==
  \/ info.ccsn = RolledBackCSN /\ info.ctid = DummyTID /\ info.rtid = EmptyTID /\ info.rcsn = UnknownCSN
  \/ /\ info.ctid /= EmptyTID
     /\ (info.ctid \in running_list /\ info.ccsn \notin {UnknownCSN, RolledBackCSN} /\ txn[info.ctid].csn /= CommittingCSN
         => txn[info.ctid].csn = info.ccsn)
     /\ (info.ccsn = UnknownCSN => info.rcsn = UnknownCSN /\ info.rtid \in {EmptyTID, info.ctid})
     /\ (info.ccsn /= UnknownCSN =>
           /\ (info.rcsn = UnknownCSN \/ info.rcsn = NonTransactionalCSN \/ info.ccsn <= info.rcsn)
           /\ (info.ctid = NonTransactionalTID \/ tid_start[info.ctid] <= info.ccsn))
     /\ (info.rcsn /= UnknownCSN => info.rtid /= EmptyTID /\ (info.rtid = NonTransactionalTID \/ tid_start[info.rtid] <= info.rcsn))

\* VersionMetadata::updateCSNIfNeeded (the part that fills CSNs from the log; the tryGetCSN rolled-back case
\* needs running_list, which is why it lives here and not in History)
TryGetCsn(t) == IF LookupCsn(t) /= UnknownCSN THEN LookupCsn(t)
                ELSE IF t \in running_list THEN UnknownCSN ELSE RolledBackCSN
UpdateCsnIfNeeded(info) ==
  LET i1 == IF info.ccsn = UnknownCSN /\ TryGetCsn(info.ctid) /= UnknownCSN
            THEN [info EXCEPT !.ccsn = TryGetCsn(info.ctid)] ELSE info
  IN IF i1.rcsn = UnknownCSN /\ i1.rtid /= EmptyTID
     THEN IF TryGetCsn(i1.rtid) = RolledBackCSN THEN [i1 EXCEPT !.rtid = EmptyTID]
          ELSE IF TryGetCsn(i1.rtid) /= UnknownCSN THEN [i1 EXCEPT !.rcsn = TryGetCsn(i1.rtid)] ELSE i1
     ELSE i1
```

`tid_start`, `tid_to_csn`, `running_list`, `snapshots_in_use`, `latest_snapshot`, `txn` are root variables;
`Parts.tla` must declare them too (`VARIABLES` in TLA+ modules that `EXTENDS` each other share names).
Move those six declarations from `Server.tla` into `Parts.tla` so that `Server` inherits them.

- [ ] **Step 2: Write the three-step store and `Fsync` as actions in `Server.tla`**

Replace the four stubs:

```tla
\* A caller starts a store by placing a frame with pc = "Read"; the frame's tentative is the caller's
\* intended VersionInfo as computed from part[p].mem by the caller's update function.
FrameOf(p, o) == CHOOSE f \in part[p].frames : f.owner = o
HasFrame(p, o) == \E f \in part[p].frames : f.owner = o
NewFrame(o, tent, nx) == [owner |-> o, tentative |-> tent, pc |-> "Read", retries |-> 0, interferences |-> 0,
                          noexcept_retries |-> 0, noexcept_owner |-> nx]
ReplaceFrame(p, f, g) == [part EXCEPT ![p].frames = (part[p].frames \ {f}) \cup {g}]
DropFrame(p, f) == [part EXCEPT ![p].frames = part[p].frames \ {f}]

\* StoreRead: attempt 1 uses getInfo (mem), a retry uses loadMetadata (the stored record); the caller's
\* update is re-applied by the caller through tentative (kept), then updateCSNIfNeeded and validateInfo.
StoreRead(p, o) ==
  /\ server = "TableUp" /\ HasFrame(p, o)
  /\ LET f == FrameOf(p, o)
         base == IF f.retries = 0 THEN part[p].mem
                 ELSE IF DiskRead(p) \in VersionInfoType THEN DiskRead(p) ELSE part[p].mem
         tent == [f.tentative EXCEPT !.sv = base.sv]
         upd == UpdateCsnIfNeeded(tent)
     IN /\ f.pc = "Read"
        /\ ValidateInfoOK(upd)             \* a failure is a Refuse(LOGICAL_ERROR), handled by the caller's Refuse
        /\ part' = ReplaceFrame(p, f, [f EXCEPT !.tentative = upd, !.pc = "Persist"])
  /\ UNCHANGED <<zk_vars, disk_vars, h_vars>>   \* plus every other root variable, see UnchangedExcept below

\* StorePersist: compare storing_version under persisted_info_mutex, then tmp + fsync + rename, or defer.
StorePersist(p, o) ==
  /\ server = "TableUp" /\ HasFrame(p, o)
  /\ LET f == FrameOf(p, o)
         stored == IF part[p].deferred /= "None" THEN part[p].deferred
                   ELSE IF DiskRead(p) \in VersionInfoType THEN DiskRead(p) ELSE EmptyInfo
         expected == stored.sv
     IN /\ f.pc = "Persist"
        /\ \/ \* deferred branch: never-transactional part with no file yet
              /\ part[p].deferrable /\ ~Involved(f.tentative)
              /\ part' = ReplaceFrame(p, f, [f EXCEPT !.pc = "Publish", !.tentative.sv = @ + 1])
              /\ part'[p].deferred = [f.tentative EXCEPT !.sv = @ + 1]
              /\ UNCHANGED disk
           \/ \* version mismatch: TOO_OLD_VERSION, back to Read (Refuse on exhaustion is the caller's)
              /\ expected /= f.tentative.sv
              /\ f.retries < MAX_STORE_RETRIES
              /\ part' = ReplaceFrame(p, f, [f EXCEPT !.pc = "Read", !.retries = @ + 1])
              /\ UNCHANGED disk
           \/ \* the write
              /\ expected = f.tentative.sv
              /\ ~(part[p].deferrable /\ ~Involved(f.tentative))
              /\ DiskWriteInfo(p, [f.tentative EXCEPT !.sv = @ + 1])
              /\ part' = [ReplaceFrame(p, f, [f EXCEPT !.pc = "Publish", !.tentative.sv = @ + 1])
                          EXCEPT ![p].deferrable = FALSE, ![p].deferred = "None"]
        /\ \* another frame on p in its window learns of the interference
           TRUE
  /\ UNCHANGED <<zk_vars, mdisk, h_vars>>

Involved(info) == \/ info.ctid /= NonTransactionalTID
                  \/ info.rcsn = UnknownCSN /\ info.rtid \notin {NonTransactionalTID, EmptyTID}
                  \/ info.rcsn \notin {NonTransactionalCSN, UnknownCSN}

\* StorePublish: setInfo under version_info_mutex, ignored if the stored version is lower
StorePublish(p, o) ==
  /\ server = "TableUp" /\ HasFrame(p, o)
  /\ LET f == FrameOf(p, o) IN
     /\ f.pc = "Publish"
     /\ part' = [DropFrame(p, f) EXCEPT ![p].mem = IF f.tentative.sv < part[p].mem.sv THEN part[p].mem ELSE f.tentative]
  /\ UNCHANGED <<zk_vars, disk_vars, h_vars>>

Fsync(p) == Layered /\ DiskFsync(p) /\ UNCHANGED <<zk_vars, mdisk, h_vars, part>>
```

Interference bookkeeping: in `StorePersist`'s write branch, every other frame `g` on `p` with `g.pc = "Persist"`
gets `interferences + 1` (it read before this write). Implement it as a second `EXCEPT` over `frames`:

```tla
BumpOthers(frames, f) == { IF g /= f /\ g.pc = "Persist" THEN [g EXCEPT !.interferences = @ + 1] ELSE g : g \in frames }
```

and use `part' = [part EXCEPT ![p].frames = BumpOthers((@ \ {f}) \cup {[f EXCEPT !.pc = "Publish", !.tentative.sv = @ + 1]}, f), ...]`
in the write branch. The `UNCHANGED` lists above are abbreviated; every action must leave every root variable
it does not mention unchanged. Define in `Server.tla` an operator per group, e.g.
`UnchangedTxnLog == UNCHANGED <<tid_start, tid_to_csn, latest_snapshot, ...>>`, and conjoin the right groups.
The executor writes these lists once and reuses them.

`Involved` mirrors `VersionInfo::wasInvolvedInTransaction`; it is defined before its first use in the final
file (TLA+ requires definition before use, so place it above `StorePersist`).

- [ ] **Step 3: Unit-check the store with a driver scenario**

`MC_Store.tla` adds a driver that starts stores from two owners on one part and checks the store contract
(this is the only place where a scenario module adds actions of its own; it is a unit test of `Parts.tla`):

```tla
---- MODULE MC_Store ----
EXTENDS MergeTreeTransactions
CoversDef == [p \in Parts |-> {}]
Owners == { <<"Session", k>> : k \in Sessions }
\* driver: an owner starts a store that sets creation_tid := 1 on P1 (a NonTransactional part becomes txn-created)
StartStore(o) ==
  /\ server = "TableUp" /\ ~HasFrame(P1, o)
  /\ part' = [part EXCEPT ![P1].frames = @ \cup {NewFrame(o, [part[P1].mem EXCEPT !.ctid = 1, !.ccsn = UnknownCSN], FALSE)}]
  /\ UNCHANGED <<zk_vars, disk_vars, h_vars>> /\ UnchangedTxnLog /\ UnchangedRest
DriverInit == Init /\ tid_start' = tid_start   \* placeholder to keep TLC happy; real init below
StoreInit == Init /\ running_list = {1} /\ txn = [Init_txn EXCEPT ![1].state = "Running"]
StoreNext == \/ \E o \in Owners : StartStore(o) \/ StoreRead(P1, o) \/ StorePersist(P1, o) \/ StorePublish(P1, o)
             \/ Fsync(P1)
StoreSpec == StoreInit /\ [][StoreNext]_vars
\* contract: after every StorePublish, mem equals the last persisted record (or a newer one); sv never decreases
StoreOK == /\ TypeOK
           /\ DiskRead(P1) \in VersionInfoType => part[P1].mem.sv >= DiskRead(P1).sv - 1
           /\ \A f \in part[P1].frames : f.retries <= MAX_STORE_RETRIES
====
```

Simplify `StoreInit`: write `Init` with the two overrides inline (copy `Init`'s conjuncts and replace
`running_list = {}` by `running_list = {1}` and `txn[1].state` by `"Running"`); the executor knows the schema
from Task 5.

`MC_Store.cfg`: `SPECIFICATION StoreSpec`, `INVARIANT StoreOK`, constants as `MC_Schema.cfg`.

Run: `utils/tla/transactions/run_tlc.sh Store`
Expected: `GREEN`, some hundreds of states; the trace of any violation points at the store logic, not at the
rest of the model, because nothing else is enabled.

- [ ] **Step 4: Commit**

```bash
git commit -m "tla(transactions): isVisible, canBeRemoved, validateInfo, three-step metadata store" -- utils/tla/transactions/Parts.tla utils/tla/transactions/Server.tla utils/tla/transactions/MC_Store.tla utils/tla/transactions/MC_Store.cfg
```

---

### Task 7: `Base` actions, group A: begin, insert, select, commit, ack, updater {#task-7}

**Files:**
- Modify: `utils/tla/transactions/Server.tla`
- Create: `utils/tla/transactions/MC_BaseA.tla`, `MC_BaseA.cfg`

**Interfaces:**
- Produces: `Begin(k)`, `InsertWrite(k, p)`, `InsertPreActive(k, p)`, `PublishStart(a, p)`,
  `PublishEnrol(a, q)`, `PublishStore(a, q)`, `PublishFlip(a)`, `SelectCapture(k)`, `SelectCheck(k, p)`,
  `SelectFinish(k)`, `CommitBefore(k)`, `CommitCreateCSN(k)` (`Ok` outcome only in this plan),
  `CommitReadOnly(k)`, `CommitStoreCreation(k, p)`, `CommitStoreRemoval(k, p)`, `CommitFlip(k)`,
  `CommitFinalize(k)`, `CommitAck(k)`, `UpdLoadEntriesMap`, `UpdPublishSnapshot`.

For every action below: guard `server = "TableUp"`; `pc` advances as the spec's advance rule says; every
root variable not mentioned is `UNCHANGED` through the group operators of Task 6. The code map row for each is
the spec's table; the executor copies the anchor into a comment above each action.

- [ ] **Step 1: `Begin`, the updater's two steps**

```tla
Begin(k) ==
  /\ server = "TableUp" /\ client[k].current = EmptyTID /\ client[k].pc = "Idle"
  /\ local_tid_counter < TID_MAX
  /\ LET t == local_tid_counter + 1 IN
     /\ local_tid_counter' = t
     /\ tid_start' = [tid_start EXCEPT ![t] = latest_snapshot]
     /\ txn' = [txn EXCEPT ![t] = [AbsentTxn EXCEPT !.state = "Running", !.snapshot = latest_snapshot,
                                     !.protected_snapshot = latest_snapshot, !.holders = {<<"Session", k>>}]]
     /\ running_list' = running_list \cup {t}
     /\ snapshots_in_use' = [snapshots_in_use EXCEPT ![t] = latest_snapshot]
     /\ client' = [client EXCEPT ![k].current = t, ![k].first_read = NoRead, ![k].last_read = NoRead]
     /\ h_content' = [h_content EXCEPT ![t] = { <<p, part[p].payload.ver>> : p \in { q \in Parts :
                        part[q].pstate \in {"Active", "Outdated"} /\ OracleVisible(q, latest_snapshot, t, part[q].mem.ctid) } }]
  /\ UNCHANGED <<zk_vars, disk_vars, part>> /\ UnchangedHistoryExcept({"h_content"}) /\ ...

\* loadEntries, first block: tid_to_csn under TransactionLog::mutex
UpdLoadEntriesMap ==
  /\ server \in {"LogUp", "TableLoading", "TableUp"} /\ updater_pc = "Idle"
  /\ LET new == { c \in DOMAIN zk_log : c > last_loaded_entry } IN
     /\ new /= {}
     /\ tid_to_csn' = [t \in Tids |-> IF \E c \in new : zk_log[c] = t THEN CHOOSE c \in new : zk_log[c] = t ELSE tid_to_csn[t]]
     /\ h_loaded' = [t \in Tids |-> h_loaded[t] \/ \E c \in new : zk_log[c] = t]
     /\ updater_pc' = "PublishSnapshot"
  /\ ...

\* loadEntries, second block: latest_snapshot under running_list_mutex
UpdPublishSnapshot ==
  /\ updater_pc = "PublishSnapshot"
  /\ latest_snapshot' = Max(DOMAIN zk_log)
  /\ last_loaded_entry' = Max(DOMAIN zk_log)
  /\ updater_pc' = "Idle"
  /\ ...
```

- [ ] **Step 2: Insert and publication**

```tla
Sess(k) == <<"Session", k>>

InsertWrite(k, p) ==
  /\ server = "TableUp" /\ client[k].pc = "Idle" /\ client[k].current /= EmptyTID
  /\ txn[client[k].current].state = "Running"
  /\ part[p].pstate = "Absent" /\ Covers[p] = {}          \* a client INSERT creates a base part
  /\ LET t == client[k].current
         info == [EmptyInfo EXCEPT !.ctid = t] IN
     /\ DiskCreateDir(p)
     /\ part' = [part EXCEPT ![p].pstate = "Temporary", ![p].deferrable = FALSE,
                             ![p].frames = {NewFrame(Sess(k), info, FALSE)}]
     /\ client' = [client EXCEPT ![k].pc = "InsertWrite", ![k].part = p]
  /\ ...
\* the store of creation_tid runs through StoreRead/Persist/Publish; InsertPreActive waits for it
InsertPreActive(k, p) ==
  /\ client[k].pc = "InsertWrite" /\ client[k].part = p /\ ~HasFrame(p, Sess(k))
  /\ part' = [part EXCEPT ![p].pstate = "PreActive"]
  /\ stmt' = [stmt EXCEPT ![Sess(k)].precommitted = @ \cup {p}]
  /\ client' = [client EXCEPT ![k].pc = "PublishStart"]
  /\ ...

\* Transaction::commit under lockParts: covered parts computed, part attached to the outer transaction
Visible(q, t) == IsVisibleImpl(q, txn[t].snapshot, t)
CoveredNow(p, t) == { q \in Parts : q \in Expand({p}) /\ q /= p /\ part[q].pstate \in {"Active", "Outdated"}
                                    /\ (part[q].pstate = "Active" \/ Visible(q, t)) }
PublishStart(a, p) ==
  /\ server = "TableUp" /\ parts_lock = "None" /\ p \in stmt[a].precommitted
  /\ ActorPc(a) = "PublishStart"
  /\ LET t == ActorTxn(a) IN
     /\ txn[t].state = "Running"
     /\ parts_lock' = a
     /\ stmt' = [stmt EXCEPT ![a].covered = CoveredNow(p, t), ![a].attached = @ \cup {p}]
     /\ txn' = [txn EXCEPT ![t].creating = Append(@, p), ![t].work = SetToSeq(CoveredNow(p, t))]
     /\ h_creating' = [h_creating EXCEPT ![t] = @ \cup {p}]
     /\ part' = [part EXCEPT ![p].pins = @ \cup {<<"Txn", t>>}]
     /\ SetActorPc(a, IF CoveredNow(p, t) = {} THEN "PublishFlip" ELSE "PublishEnrol")
  /\ ...
```

`ActorPc(a)`, `ActorTxn(a)`, `SetActorPc(a, v)` read and write `client[k].pc`/`client[k].current` for
`a = <<"Session", k>>` and `task[i].pc`/`task[i].txn` for `a = <<"Task", i>>`; define them in `Server.tla`.
`SetToSeq(S)` is any fixed enumeration (`CHOOSE s \in [1..Cardinality(S) -> S] : \A x \in S : \E n : s[n] = x`).

`PublishEnrol(a, q)` and `PublishStore(a, q)` are `DropEnrol`/`DropStore` (Task 8) with `parts_lock = a`
kept and the work list `txn[t].work`; write them as thin wrappers once Task 8 defines the two.
`PublishFlip(a)`: every part in `stmt[a].covered` to `Outdated`, `p` to `Active`, `stmt[a] := NoStmt`,
`parts_lock := "None"`, actor `pc := "Idle"`; on the way it stores `remove_time`-free state only.

- [ ] **Step 3: Select**

```tla
SelectCapture(k) ==
  /\ server = "TableUp" /\ parts_lock = "None" /\ client[k].pc = "Idle" /\ client[k].current /= EmptyTID
  /\ LET S == { p \in Parts : part[p].pstate \in {"Active", "Outdated"} } IN
     /\ part' = [p \in Parts |-> IF p \in S THEN [part[p] EXCEPT !.pins = @ \cup {<<"Select", k>>}] ELSE part[p]]
     /\ client' = [client EXCEPT ![k].capture = S, ![k].checked = {}, ![k].pc = "SelectCheck"]
  /\ ...
SelectCheck(k, p) ==
  /\ client[k].pc = "SelectCheck" /\ p \in client[k].capture /\ p \notin client[k].checked
  /\ LET t == client[k].current
         vis == IsVisibleImpl(p, txn[t].snapshot, t) IN
     /\ client' = [client EXCEPT ![k].checked = @ \cup {p},
                                 ![k].capture = IF vis THEN @ ELSE @ \ {p}]
  /\ ...
SelectFinish(k) ==
  /\ client[k].pc = "SelectCheck" /\ client[k].checked = client[k].capture \cup (client[k].capture)  \* every captured part checked
  /\ LET V == client[k].capture
         R == [parts |-> V, frags |-> { <<q, part[q].payload.ver>> : q \in Expand(V) }] IN
     /\ client' = [client EXCEPT ![k].first_read = IF @ = NoRead THEN R ELSE @, ![k].last_read = R,
                                 ![k].capture = {}, ![k].checked = {}, ![k].pc = "Idle"]
     /\ part' = [p \in Parts |-> [part[p] EXCEPT !.pins = @ \ {<<"Select", k>>}]]
  /\ ...
```

`SelectCheck` removes an invisible part from `capture` so that `capture` ends as the visible set; `checked`
tracks progress. `SelectFinish`'s guard is "every originally captured part has been checked": keep the
original capture in a third field `captured0` set by `SelectCapture`, and guard on `checked = captured0`.
Add `captured0 : SUBSET Parts` to `ClientRecord` and `TypeOK`.

- [ ] **Step 4: Commit machine, `Ok` outcome only**

```tla
Effects(t) == txn[t].creating /= <<>> \/ txn[t].removing /= <<>> \/ txn[t].mutations /= {}

CommitBefore(k) ==
  /\ server = "TableUp" /\ client[k].pc = "Idle" /\ client[k].current /= EmptyTID
  /\ LET t == client[k].current IN
     /\ txn[t].pc = "Idle"
     /\ \/ /\ txn[t].state = "Running"
           /\ txn' = [txn EXCEPT ![t].state = "Committing", ![t].csn = CommittingCSN, ![t].csn_notified = FALSE,
                                  ![t].pc = IF Effects(t) THEN "CommitCreateCSN" ELSE "CommitFlip"]
           /\ h_snapshot' = [h_snapshot EXCEPT ![t] = txn[t].snapshot]
           /\ client' = [client EXCEPT ![k].pc = "Commit"]
        \/ /\ txn[t].state = "RolledBack"          \* cancelled by KILL TRANSACTION: INVALID_TRANSACTION
           /\ client' = [client EXCEPT ![k].outcome = "Error", ![k].outcome_tid = t, ![k].current = EmptyTID,
                                       ![k].last_error = "INVALID_TRANSACTION"]
           /\ h_outcome' = [h_outcome EXCEPT ![t] = "Error"]
           /\ UNCHANGED <<txn, h_snapshot>>
  /\ ...

\* the commit point: one sequential create; plan 1 has the Ok outcome only
CommitCreateCSN(k) ==
  /\ LET t == client[k].current IN
     /\ txn[t].pc = "CommitCreateCSN" /\ KeeperCanAppend /\ zk_session = "Alive"
     /\ KeeperAppend(t)
     /\ LET c == zk_seq + 1 IN
        /\ h_committed' = h_committed \cup {t}
        /\ h_csn' = [h_csn EXCEPT ![t] = c]
        /\ h_removers' = [p \in Parts |-> IF p \in h_removing[t] THEN h_removers[p] \cup {t} ELSE h_removers[p]]
        /\ txn' = [txn EXCEPT ![t].pc = "CommitStoreCreation", ![t].work = txn[t].creating]
  /\ ...
CommitReadOnly(k) ==     \* isReadOnly branch: csn := snapshot, no Keeper request, no h_committed
  /\ LET t == client[k].current IN
     /\ txn[t].pc = "CommitFlip" /\ ~Effects(t) /\ txn[t].state = "Committing"
     /\ txn' = [txn EXCEPT ![t].state = "Committed", ![t].csn = txn[t].snapshot, ![t].csn_notified = TRUE, ![t].pc = "CommitFinalize"]
     /\ h_csn' = [h_csn EXCEPT ![t] = txn[t].snapshot]
  /\ ...

\* afterCommit, one setAndStoreCreationCSN: start a frame; the store steps run; then the next part
CommitStoreCreation(k, p) ==
  /\ LET t == client[k].current IN
     /\ txn[t].pc = "CommitStoreCreation" /\ txn[t].work /= <<>> /\ Head(txn[t].work) = p
     /\ ~HasFrame(p, Sess(k))
     /\ \/ \* start the store
           /\ part[p].mem.ccsn /= h_csn[t]
           /\ part' = [part EXCEPT ![p].frames = @ \cup {NewFrame(Sess(k), [part[p].mem EXCEPT !.ccsn = h_csn[t]], TRUE)}]
           /\ UNCHANGED txn
        \/ \* the store has completed (mem carries the csn): advance
           /\ part[p].mem.ccsn = h_csn[t]
           /\ txn' = [txn EXCEPT ![t].work = Tail(@),
                                  ![t].pc = IF Tail(txn[t].work) = <<>> THEN "CommitStoreRemoval" ELSE @]
           /\ txn'[t].work = IF Tail(txn[t].work) = <<>> THEN txn[t].removing ELSE Tail(txn[t].work)
           /\ UNCHANGED part
  /\ ...
```

`CommitStoreRemoval(k, p)` is the same with `rcsn`, `removing`, and next `pc = "CommitFlip"` (plan 4 inserts
`CommitStoreMutation` between them). Write the `work`-list handling once as
`AdvanceWork(t, nextpc, nextwork)` and reuse it.

```tla
CommitFlip(k) ==
  /\ LET t == client[k].current IN
     /\ txn[t].pc = "CommitFlip" /\ Effects(t) /\ txn[t].state = "Committing"
     /\ txn' = [txn EXCEPT ![t].state = "Committed", ![t].csn = h_csn[t], ![t].csn_notified = TRUE, ![t].pc = "CommitFinalize"]
  /\ ...
CommitFinalize(k) ==
  /\ LET t == client[k].current IN
     /\ txn[t].pc = "CommitFinalize"
     /\ running_list' = running_list \ {t}
     /\ snapshots_in_use' = [snapshots_in_use EXCEPT ![t] = UnknownCSN]
     /\ txn' = [txn EXCEPT ![t].pc = "Idle", ![t].creating = <<>>, ![t].removing = <<>>, ![t].mutations = {},
                            ![t].holders = {}]
     /\ part' = [p \in Parts |-> [part[p] EXCEPT !.pins = @ \ {<<"Txn", t>>}]]
     /\ client' = [client EXCEPT ![k].waiting = IF WAIT_MODE = "ASYNC" THEN "None" ELSE "ForLoad"]
  /\ ...
CommitAck(k) ==
  /\ client[k].pc = "Commit" /\ LET t == client[k].current IN
     /\ txn[t].state = "Committed" /\ txn[t].pc = "Idle"
     /\ (client[k].waiting = "ForLoad" => latest_snapshot >= txn[t].csn)     \* waitForCSNLoaded
     /\ client' = [client EXCEPT ![k].outcome = "Acked", ![k].outcome_tid = t, ![k].current = EmptyTID,
                                 ![k].waiting = "None", ![k].pc = "Idle"]
     /\ h_outcome' = [h_outcome EXCEPT ![t] = "Acked"]
  /\ ...
```

`CommitFinalize` also runs `afterFinalize`'s clearing; the spec's `CommitFinalize` row says "erase from
`running_list` and `snapshots_in_use`, `afterFinalize`". `CommitAck` after a read-only commit needs no
`ForLoad` wait beyond `latest_snapshot >= snapshot`, which already holds.

- [ ] **Step 5: `MC_BaseA` and a first property**

`MC_BaseA.tla`:

```tla
---- MODULE MC_BaseA ----
EXTENDS MergeTreeTransactions
CoversDef == [p \in Parts |-> {}]
====
```

`MC_BaseA.cfg`: `SPECIFICATION Spec`, `INVARIANT TypeOK`, constants as `MC_Schema.cfg` with `TID_MAX = 3`,
`CSN_MAX = 40`. Symmetry: `SYMMETRY SymSessions` with `SymSessions == Permutations(Sessions)` added to the
module (`EXTENDS TLC`).

Run: `utils/tla/transactions/run_tlc.sh BaseA`
Expected: `GREEN` on `TypeOK`; the state count is the plan's first measurement, record it in the README run
table (Task 9). If the run does not finish in 15 minutes, lower `TID_MAX` to 2 for this task only and note it.

- [ ] **Step 6: Commit**

```bash
git commit -m "tla(transactions): Base group A actions (begin, insert, publish, select, commit, ack, updater)" -- utils/tla/transactions/Server.tla utils/tla/transactions/MergeTreeTransactions.tla utils/tla/transactions/MC_BaseA.tla utils/tla/transactions/MC_BaseA.cfg
```

---

### Task 8: `Base` actions, group B: drop, rollback, kill, refuse, fail, statement rollback {#task-8}

**Files:**
- Modify: `utils/tla/transactions/Server.tla`
- Create: `utils/tla/transactions/MC_Base.tla`, `MC_Base.cfg`

**Interfaces:**
- Produces: `DropStart(k)`, `DropEnrol(k, p)`, `DropStore(k, p)`, `DropOutdate(k)`, `RollbackStart(k)`,
  `RollbackCopyLists(k)`, `RollbackMarkCreated(k, p)`, `RollbackOutdateCreated(k, p)`, `RollbackRestore(k, p)`,
  `RollbackUnlock(k, p)`, `RollbackFinalize(k)`, `KillTransaction(k, t)`, `Refuse(k)`, `Fail(k)`,
  `StmtRollback(a)`, `CommitError(k)`, and `PublishEnrol`/`PublishStore` as wrappers.

- [ ] **Step 1: Drop**

```tla
DropStart(k) ==
  /\ server = "TableUp" /\ client[k].pc = "Idle" /\ client[k].current /= EmptyTID
  /\ txn[client[k].current].state = "Running"
  /\ \A i \in Tasks : reserved[i] = {}              \* stopMergesAndWait
  /\ parts_lock = "None"
  /\ LET t == client[k].current
         V == { p \in Parts : part[p].pstate \in {"Active", "Outdated"} /\ IsVisibleImpl(p, txn[t].snapshot, t) } IN
     /\ merges_blocker' = merges_blocker + 1
     /\ parts_lock' = Sess(k)
     /\ client' = [client EXCEPT ![k].pc = IF V = {} THEN "DropOutdate" ELSE "DropEnrol", ![k].work = SetToSeq(V)]
  /\ ...

\* removeOldPart, first half: take the transaction mutex, checkIsNotCancelled, lockRemovalTID, enrol
DropEnrol(k, p) ==
  /\ client[k].pc = "DropEnrol" /\ Head(client[k].work) = p
  /\ LET t == client[k].current IN
     /\ txn[t].mutex = "None"
     /\ \/ \* cancelled: INVALID_TRANSACTION -> Refuse
           /\ txn[t].state = "RolledBack"
           /\ client' = [client EXCEPT ![k].last_error = "INVALID_TRANSACTION", ![k].pc = "Refuse"]
           /\ UNCHANGED <<txn, part, h_removing>>
        \/ \* lock held or removal committed: SERIALIZATION_ERROR -> Refuse
           /\ txn[t].state = "Running"
           /\ (part[p].lock /= EmptyTID \/ part[p].mem.rcsn /= UnknownCSN)
           /\ client' = [client EXCEPT ![k].last_error = "SERIALIZATION_ERROR", ![k].pc = "Refuse"]
           /\ UNCHANGED <<txn, part, h_removing>>
        \/ \* success: CAS on the lock, enrol, keep the mutex for DropStore
           /\ txn[t].state = "Running" /\ part[p].lock = EmptyTID /\ part[p].mem.rcsn = UnknownCSN
           /\ part' = [part EXCEPT ![p].lock = t, ![p].pins = @ \cup {<<"Txn", t>>},
                                   ![p].frames = @ \cup {NewFrame(Sess(k), [part[p].mem EXCEPT !.rtid = t], FALSE)}]
           /\ txn' = [txn EXCEPT ![t].mutex = Sess(k), ![t].removing = Append(@, p)]
           /\ h_removing' = [h_removing EXCEPT ![t] = @ \cup {p}]
           /\ client' = [client EXCEPT ![k].pc = "DropStore"]
  /\ ...

\* removeOldPart, second half: the store ran (frame gone), release the mutex, next part
DropStore(k, p) ==
  /\ client[k].pc = "DropStore" /\ Head(client[k].work) = p /\ ~HasFrame(p, Sess(k))
  /\ LET t == client[k].current IN
     /\ part[p].mem.rtid = t
     /\ txn' = [txn EXCEPT ![t].mutex = "None"]
     /\ client' = [client EXCEPT ![k].work = Tail(@), ![k].pc = IF Tail(client[k].work) = <<>> THEN "DropOutdate" ELSE "DropEnrol"]
  /\ ...

DropOutdate(k) ==
  /\ client[k].pc = "DropOutdate"
  /\ LET t == client[k].current
         D == { p \in Parts : part[p].lock = t /\ part[p].pstate = "Active" } IN
     /\ part' = [p \in Parts |-> IF p \in D THEN [part[p] EXCEPT !.pstate = "Outdated"] ELSE part[p]]
     /\ merges_blocker' = merges_blocker - 1
     /\ parts_lock' = "None"
     /\ client' = [client EXCEPT ![k].pc = "Idle"]
  /\ ...
```

Which parts `DropOutdate` flips: the ones this transaction enrolled in this batch. Keep the batch in
`client[k].batch : SUBSET Parts` (set by `DropStart` to `V`, cleared by `DropOutdate`) rather than deriving it
from locks; add the field to `ClientRecord` and `TypeOK`.

`PublishEnrol(a, q)` == `DropEnrol` with the actor's transaction and `txn[t].work` as the work list, without
touching `merges_blocker`; `PublishStore(a, q)` likewise; the executor factors the shared body into
`EnrolBody(a, t, p, nextpc)` and `StoreDoneBody(a, t, p, nextpc)`.

- [ ] **Step 2: Rollback machine**

```tla
RollbackStart(k) ==
  /\ server = "TableUp" /\ client[k].pc \in {"Rollback"}  \* reached from ROLLBACK, Refuse, Fail
  /\ LET t == client[k].current IN
     /\ \/ /\ txn[t].state = "Running" /\ txn[t].pc = "Idle"
           /\ txn' = [txn EXCEPT ![t].state = "RolledBack", ![t].csn = RolledBackCSN, ![t].csn_notified = TRUE,
                                  ![t].pc = "RollbackCopyLists"]
        \/ /\ txn[t].state \in {"RolledBack", "Committed", "Committing"}   \* nothing to do
           /\ client' = [client EXCEPT ![k].pc = "Idle", ![k].current = EmptyTID]
           /\ UNCHANGED txn
  /\ ...
RollbackCopyLists(k) ==
  /\ LET t == client[k].current IN
     /\ txn[t].pc = "RollbackCopyLists" /\ txn[t].mutex = "None"
     /\ part' = [p \in Parts |-> IF p \in Range(txn[t].creating) \cup Range(txn[t].removing)
                                 THEN [part[p] EXCEPT !.pins = @ \cup {<<"Rollback", t>>}] ELSE part[p]]
     /\ txn' = [txn EXCEPT ![t].pc = "RollbackMarkCreated", ![t].work = txn[t].creating]
  /\ ...
```

(`Range(s) == { s[i] : i \in DOMAIN s }`; the mutation kill steps are plan 4's, in plan 1 `RollbackCopyLists`
goes straight to `RollbackMarkCreated`.)

`RollbackMarkCreated(k, p)`: per part of `creating`: start a frame with `ccsn := RolledBackCSN`
(`noexcept_owner = TRUE`), advance when `mem.ccsn = RolledBackCSN`; next `pc = "RollbackOutdateCreated"` with
`work = creating` again. `RollbackOutdateCreated(k, p)`: `pstate := "Outdated"` if `Active` or `PreActive`;
next `RollbackRestore` with `work = removing`. `RollbackRestore(k, p)`: if `p \notin Range(creating)` and
`pstate = "Outdated"` then `pstate := "Active"`; next `RollbackUnlock` with `work = removing`.
`RollbackUnlock(k, p)`: start a frame with `rtid := EmptyTID` (`noexcept_owner = TRUE`); when it completes
(`mem.rtid = EmptyTID`), `lock := EmptyTID`, advance; next `RollbackFinalize`. `RollbackFinalize(k)`: as
`CommitFinalize` (running list, snapshot, pins of `Txn(t)` and `Rollback(t)`, lists cleared), plus
`h_rolled_back[t] := TRUE`, `client[k].current := EmptyTID`, `pc := "Idle"`. All follow the
`CommitStoreCreation` two-branch pattern (start the frame / advance when done).

- [ ] **Step 3: Kill, refuse, fail, statement rollback, commit error**

```tla
KillTransaction(k, t) ==      \* another session's KILL TRANSACTION: rollbackTransaction(t)
  /\ server = "TableUp" /\ client[k].current /= t /\ txn[t].state = "Running" /\ txn[t].pc = "Idle"
  /\ ~(\E k2 \in Sessions : client[k2].current = t /\ client[k2].pc = "Rollback")
  /\ txn' = [txn EXCEPT ![t].state = "RolledBack", ![t].csn = RolledBackCSN, ![t].csn_notified = TRUE,
                         ![t].pc = "RollbackCopyLists", ![t].holders = @ \cup {Sess(k)}]
  /\ ...
```

The rollback steps of a killed transaction are then driven by whichever session holds it: make the
`Rollback*` actions take the transaction from `txn[t].pc` rather than from `client[k].current` when `k` is a
killer (guard `Sess(k) \in txn[t].holders`). The simplest encoding: every `Rollback*(k, ...)` action reads `t`
as `LET t == IF client[k].current /= EmptyTID /\ txn[client[k].current].pc /= "Idle" THEN client[k].current
ELSE CHOOSE t2 \in Tids : Sess(k) \in txn[t2].holders /\ txn[t2].pc /= "Idle"`.

```tla
Refuse(k) ==                  \* the natural exception: last_error set by the refusing step; release, then rollback
  /\ client[k].pc = "Refuse"
  /\ LET t == client[k].current IN
     /\ txn' = [txn EXCEPT ![t].mutex = "None"]
     /\ parts_lock' = IF parts_lock = Sess(k) THEN "None" ELSE parts_lock
     /\ merges_blocker' = IF parts_lock = Sess(k) /\ client[k].batch /= {} THEN merges_blocker - 1 ELSE merges_blocker
     /\ part' = [p \in Parts |-> [part[p] EXCEPT !.frames = { f \in @ : f.owner /= Sess(k) }]]
     /\ client' = [client EXCEPT ![k].pc = IF stmt[Sess(k)].precommitted \ stmt[Sess(k)].attached /= {} THEN "StmtRollback" ELSE "Rollback",
                                 ![k].work = <<>>, ![k].batch = {}]
  /\ ...
Fail(k) ==                    \* an injected exception between two steps
  /\ query_faults < QUERY_FAULTS_MAX
  /\ client[k].pc \in {"InsertWrite", "InsertPreActive", "PublishStart", "PublishEnrol", "PublishStore",
                       "SelectCapture", "SelectCheck", "DropStart", "DropEnrol", "DropStore"}
  /\ query_faults' = query_faults + 1
  /\ client' = [client EXCEPT ![k].last_error = "Injected", ![k].pc = "Refuse"]
  /\ ...
StmtRollback(a) ==            \* MergeTreeData::Transaction::rollback
  /\ ActorPc(a) = "StmtRollback"
  /\ LET R == stmt[a].precommitted \ stmt[a].attached IN
     /\ part' = [p \in Parts |-> IF p \in R THEN AbsentPartRecord ELSE part[p]]
     /\ disk' = [p \in Parts |-> IF p \in R THEN AbsentPart ELSE disk[p]]
     /\ stmt' = [stmt EXCEPT ![a] = NoStmt]
     /\ SetActorPc(a, "Rollback")
  /\ ...
CommitError(k) ==  \* covered by the second branch of CommitBefore in plan 1; plan 3 adds the WAIT_UNKNOWN case
  FALSE
```

`Fail` at `SelectCapture`/`SelectCheck` must also drop the `Select(k)` pins; `Refuse` handles that by
clearing pins of `<<"Select", k>>` when `client[k].capture /= {}`.

- [ ] **Step 4: `MC_Base` with `TypeOK` only, then measure**

`MC_Base.tla` as `MC_BaseA.tla`. `MC_Base.cfg`: constants `Sessions = {k1, k2}`, `Parts = {P1, P2}`,
`Tasks = {i1, i2}`, `Mutations = {}`, `TID_MAX = 3`, `CSN_MAX = 40`, `QUERY_FAULTS_MAX = 1`, the rest zero,
`DISK_MODE = "Durable"`, `WAIT_MODE = "WAIT"`, `WITNESS = ""`; `INVARIANT TypeOK`; `SYMMETRY SymSessions`.

Run: `utils/tla/transactions/run_tlc.sh Base`
Expected: `GREEN` on `TypeOK`. Record states, distinct states and time.

- [ ] **Step 5: Commit**

```bash
git commit -m "tla(transactions): Base group B actions (drop, rollback, kill, refuse, fail, statement rollback)" -- utils/tla/transactions/Server.tla utils/tla/transactions/MergeTreeTransactions.tla utils/tla/transactions/MC_Base.tla utils/tla/transactions/MC_Base.cfg
```

---

### Task 9: `Base` properties and their witnesses {#task-9}

**Files:**
- Modify: `utils/tla/transactions/Invariants.tla`
- Modify: `utils/tla/transactions/Server.tla` (witness hooks)
- Modify: `utils/tla/transactions/MC_Base.cfg`

**Interfaces:**
- Produces: the `Base` green set of the spec's scenario matrix: `StableRead`, `ReadYourWrites`,
  `NoUncommittedRead`, `NoFutureRead`, `NoDoubleRead`, `NoLostRead`, `Atomicity`, `AckedWriteIsDurable`,
  `ErrorIsAbsent`, `RollbackRestores`, `SingleRemover`, `LockConsistent`, `ActiveSetShape`,
  `FlipAfterStores`, `NoSpuriousStaleVersion`, `NoAvoidableTermination`, the `validateInfo` and `isVisible`
  assertion rows, `Assert_getOldestSnapshot`; and one witness override per property.

- [ ] **Step 1: Write the state invariants**

```tla
---- MODULE Invariants ----
EXTENDS Server
Witness(name) == WITNESS = name
Frags(V) == { <<q, part[q].payload.ver>> : q \in Expand(V) }
Cur(k) == client[k].current

\* --- isolation (spec #invariants-isolation); skipped for EverythingVisibleCSN snapshots
IsoApplies(k) == Cur(k) /= EmptyTID /\ txn[Cur(k)].snapshot /= EverythingVisibleCSN

StableRead == \A k \in Sessions : IsoApplies(k) /\ client[k].first_read /= NoRead /\ client[k].last_read /= NoRead =>
  LET t == Cur(k)
      own == Frags(h_creating[t]) \cup Frags(h_removing[t]) IN
  (client[k].first_read.frags \ own) = (client[k].last_read.frags \ own)

NoUncommittedRead == \A k \in Sessions : IsoApplies(k) => \A p \in client[k].last_read.parts :
  part[p].mem.ctid \in h_committed \cup {Cur(k), NonTransactionalTID}

NoFutureRead == \A k \in Sessions : IsoApplies(k) => \A p \in client[k].last_read.parts :
  OracleVisible(p, txn[Cur(k)].snapshot, Cur(k), part[p].mem.ctid)

NoLostRead == \A k \in Sessions : IsoApplies(k) /\ client[k].last_read /= NoRead => \A p \in Parts :
  (part[p].pstate \in {"Active", "Outdated"} /\ OracleVisible(p, txn[Cur(k)].snapshot, Cur(k), part[p].mem.ctid))
  => (p \in client[k].last_read.parts \/ \E c \in client[k].last_read.parts : p \in Expand({c}))

NoDoubleRead == \A k \in Sessions : \A f1, f2 \in client[k].last_read.frags : f1[1] = f2[1] => f1 = f2

\* --- conflicts (spec #invariants-conflicts)
SingleRemover == \A p \in Parts : Cardinality(h_removers[p]) <= 1
LockConsistent == \A p \in Parts :
  LET l == part[p].lock, m == part[p].mem IN
  /\ (l \in Tids => m.rtid \in {EmptyTID, l})
  /\ (l = NonTransactionalTID => m.rtid \in {EmptyTID, NonTransactionalTID})
  /\ (l = EmptyTID => m.rtid = EmptyTID \/ m.rcsn /= UnknownCSN)
ActiveSetShape == /\ \A p, q \in Parts : part[p].pstate = "Active" /\ part[q].pstate = "Active" => ~Overlap(p, q)
                  /\ \A i, j \in Tasks : i /= j => reserved[i] \cap reserved[j] = {}

\* --- code assertions (spec #invariants-code)
Assert_validateInfo == \A p \in Parts : part[p].pstate /= "Absent" => ValidateInfoOK(part[p].mem)
Assert_isVisible_fast == \A p \in Parts : LET m == part[p].mem IN
  /\ (m.rcsn /= UnknownCSN => m.ccsn /= UnknownCSN)
  /\ m.ccsn \in {UnknownCSN, NonTransactionalCSN, RolledBackCSN} \cup RealCSNs
  /\ m.rcsn \in {UnknownCSN, NonTransactionalCSN} \cup RealCSNs
Assert_getOldestSnapshot == \A t \in running_list : snapshots_in_use[t] /= UnknownCSN
NoAvoidableTermination == down_cause \in {"None", "RetryExhausted"}
NoSpuriousStaleVersion == \A k \in Sessions : client[k].last_error = "STALE_VERSION" => client[k].stale_interferences = MAX_STORE_RETRIES
====
```

`NoSpuriousStaleVersion` needs the frame's `interferences` at the moment of exhaustion: when `StorePersist`
would exceed `MAX_STORE_RETRIES`, the caller's `Refuse` copies `f.interferences` into
`client[k].stale_interferences` (add the field). `ReadYourWrites`, `Atomicity`, `AckedWriteIsDurable`,
`ErrorIsAbsent`, `RollbackRestores`, `FlipAfterStores` are action properties or need "after event X" facts;
write them as follows.

- [ ] **Step 2: Write the action and history properties**

```tla
\* ReadYourWrites: state form over the last read of the current transaction
ReadYourWrites == \A k \in Sessions : IsoApplies(k) /\ client[k].last_read /= NoRead =>
  LET t == Cur(k) IN
  /\ \A p \in h_creating[t] \ h_removing[t] : part[p].pstate \in {"Active", "Outdated"} => p \in client[k].last_read.parts
  /\ \A p \in h_removing[t] : p \notin client[k].last_read.parts

\* Atomicity: for readers whose snapshot >= h_csn[t] of a loaded committed t
Atomicity == \A k \in Sessions : IsoApplies(k) /\ client[k].last_read /= NoRead => \A t \in h_committed :
  h_loaded[t] /\ txn[Cur(k)].snapshot >= h_csn[t] /\ Cur(k) /= t =>
  LET s == txn[Cur(k)].snapshot
      V == client[k].last_read.parts
      C == { p \in h_creating[t] \ h_removing[t] : ~\E r \in h_removers[p] \ {t} : h_csn[r] <= s }
      R == h_removing[t] \ h_creating[t] IN
  /\ (C \subseteq V /\ V \cap R = {}) \/ (C \cap V = {} /\ R \subseteq V)
  /\ V \cap (h_creating[t] \cap h_removing[t]) = {}

\* Durability (Durable disk mode in Base: memory and disk coincide, restart is plan 3's)
AckedWriteIsDurable == \A t \in Tids : h_outcome[t] = "Acked" /\ (h_creating[t] \cup h_removing[t]) /= {} =>
  /\ t \in h_committed
  /\ \A p \in h_creating[t] : part[p].pstate = "Active" \/ (part[p].pstate \in {"Outdated", "Deleting", "Deleted"} /\ h_removers[p] /= {})
  /\ \A p \in h_removing[t] : part[p].pstate /= "Active"
ErrorIsAbsent == \A t \in Tids : h_outcome[t] = "Error" => t \notin h_committed /\ (h_rolled_back[t] => \A p \in h_creating[t] : part[p].pstate /= "Active")

\* Action properties (checked as PROPERTY in the cfg)
RollbackRestoresStep == \A k \in Sessions : \A t \in Tids :
  (RollbackFinalize(k) /\ txn[t].pc = "RollbackFinalize") =>
    \A p \in h_removing[t] \ h_creating[t] :
      part'[p].pstate = "Active" \/ part[p].lock \notin {EmptyTID, t} \/ (h_removers[p] \ {t}) /= {}
RollbackRestores == [][RollbackRestoresStep]_vars
FlipAfterStoresStep == \A k \in Sessions : CommitFlip(k) =>
  LET t == Cur(k) IN /\ \A p \in h_creating[t] : part[p].mem.ccsn = h_csn[t]
                     /\ \A p \in h_removing[t] : part[p].mem.rcsn = h_csn[t]
FlipAfterStores == [][FlipAfterStoresStep]_vars
```

`RollbackRestores` also has a state clause ("no part of `h_creating[t]` is ever in a read of another
transaction"): `\A k : \A t \in Tids : Cur(k) /= t => client[k].last_read.parts \cap h_creating[t] = {}`; add
it as `RollbackNoLeak` in the invariant list.

- [ ] **Step 3: Witness hooks**

Each witness is a guarded change in the action it names. Add to `Server.tla` the hooks the spec's tables name
for the `Base` scenario; each is one conjunct that reads `WITNESS`. Examples, the executor does the same for
every property in the `Base` green set:

```tla
\* Witness_StableRead: SelectCheck uses latest_snapshot instead of txn.snapshot
SnapshotFor(t) == IF Witness("StableRead") THEN latest_snapshot ELSE txn[t].snapshot
\* in SelectCheck: vis == IsVisibleImpl(p, SnapshotFor(t), t)

\* Witness_ReadYourWrites: the creation_tid = current_tid clause removed from isVisible
\* in InfoIsVisible: the branch `u /= EmptyTID /\ info.ctid = u THEN "TRUE"` is skipped when Witness("ReadYourWrites")

\* Witness_NoUncommittedRead: SelectCheck treats creation_csn = 0 as creation_csn = snapshot
\* in IsVisibleImpl: ccsn == IF Witness("NoUncommittedRead") /\ info.ccsn = UnknownCSN THEN s ELSE ...

\* Witness_NoFutureRead: SelectCheck compares against latest_snapshot when mem.creation_csn is unknown
\* Witness_NoLostRead: SelectCapture skips Outdated parts
\* Witness_SingleRemover: the CAS in DropEnrol replaced by an unconditional write
\* Witness_LockConsistent: RollbackUnlock unlocks before clearing removal_tid
\* Witness_ActiveSetShape: PublishFlip does not outdate the covered parts
\* Witness_FlipAfterStores: CommitFlip enabled at pc = "CommitStoreCreation" (moved before the store loops)
\* Witness_NoSpuriousStaleVersion: StoreRead on a retry uses mem instead of the stored record
\* Witness_AckedWriteIsDurable: CommitAck enabled at pc = "CommitCreateCSN" and Fail allowed after it  (needs Crash: plan 3; in plan 1 record as "witness deferred to plan 3")
\* Witness_ErrorIsAbsent: RollbackOutdateCreated skipped
\* Witness_Atomicity: SelectCheck uses mem only and skips the tid_to_csn lookup (multi-part DROP: needs two parts, Base has them)
\* Witness_Assert_validateInfo (running creator): CommitStoreCreation writes h_csn + 1
\* Witness_Assert_validateInfo (order): CommitStoreCreation writes CSN_MAX
\* Witness_Assert_validateInfo (removal): DropStore skipped so the lock is in memory only
\* Witness_Assert_isVisible_fast: CommitStoreCreation skipped for a part both created and removed by t, StoreRead skips validateInfo (two changes)
\* Witness_Assert_getOldestSnapshot: needs SetSnapshot (plan 2); deferred
\* Witness_NoAvoidableTermination: Fail allowed inside afterCommit with ProcessDown(Other) (needs ProcessDown: plan 5); deferred
```

Because `Invariants` extends `Server`, the hooks must live in `Server.tla` (or `Parts.tla` for
`InfoIsVisible`/`IsVisibleImpl`), guarded by `Witness("Name")`; `WITNESS = ""` on the baseline makes every
hook a no-op.

- [ ] **Step 4: Run the green set, then every witness**

`MC_Base.cfg` invariants: `TypeOK StableRead NoUncommittedRead NoFutureRead NoLostRead NoDoubleRead
ReadYourWrites Atomicity SingleRemover LockConsistent ActiveSetShape Assert_validateInfo Assert_isVisible_fast
Assert_getOldestSnapshot NoAvoidableTermination NoSpuriousStaleVersion AckedWriteIsDurable ErrorIsAbsent
RollbackNoLeak`; properties: `RollbackRestores FlipAfterStores`.

Run: `utils/tla/transactions/run_tlc.sh Base`
Expected: `GREEN`. Any violation here is either a model defect (most likely at this stage) or a code finding;
apply the spec's fidelity checks before deciding, and record it in the README counterexample log either way.

Then for each property with a plan-1 witness:
`for P in StableRead ReadYourWrites NoUncommittedRead NoFutureRead NoLostRead NoDoubleRead SingleRemover LockConsistent ActiveSetShape FlipAfterStores NoSpuriousStaleVersion ErrorIsAbsent Atomicity Assert_validateInfo Assert_isVisible_fast; do utils/tla/transactions/witness.sh Base $P || echo "FAILED WITNESS $P"; done`
Expected: every line `WITNESS RED (as required)`. A witness that does not fire is a defect of the property
or of the hook; fix it before continuing (spec: a property without a passing witness is not accepted).

- [ ] **Step 5: Commit**

```bash
git commit -m "tla(transactions): Base properties and witnesses" -- utils/tla/transactions/Invariants.tla utils/tla/transactions/Server.tla utils/tla/transactions/Parts.tla utils/tla/transactions/MC_Base.cfg
```

---

### Task 10: README with code map, run table, witness table {#task-10}

**Files:**
- Create: `utils/tla/transactions/README.md`

- [ ] **Step 1: Write the README**

Sections, in this order, each with the content named:

1. Goal and scope: two paragraphs copied from the spec's goal and scope sections, with the "not covered" list.
2. How to run: `run_tlc.sh`, `witness.sh`, where logs go, the meaning of exit codes.
3. Code map: one row per action defined so far, columns `Action | File | Function | Step boundary`, values from
   the spec's tables (for example `DropEnrol | src/Interpreters/MergeTreeTransaction.cpp | MergeTreeTransaction::removeOldPart | mutex taken, lockRemovalTID, push to removing_parts`).
   Stubbed actions are listed with `(plan N)` in the function column.
4. Run table: `Scenario | Date | Commit | States | Distinct | Time | Result`, with the `Schema`, `Store`,
   `BaseA`, `Base` rows from Tasks 5 to 9 filled with the measured numbers.
5. Witness table: `Property | Witness | Scenario | Last verified`, one row per property of Task 9, including
   the deferred ones marked with the plan that will verify them.
6. Refinement parameters: `MAX_STORE_RETRIES` 2 vs 20, `NOEXCEPT_RETRY_BUDGET` 2 vs 60 s, `BG_TASKS` 2 vs a
   pool.
7. Counterexample log: empty table with the columns `Scenario | Trace | C++ sequence | Verdict | Follow-up`.
8. Expected-red findings (from the spec): the four properties and the scenarios where they are run, none of
   them in plan 1.

- [ ] **Step 2: Commit**

```bash
git commit -m "tla(transactions): README with code map, run table, witness table" -- utils/tla/transactions/README.md
```

---

## Self-review {#self-review}

Spec coverage for plan 1's slice: the spec's `Base` row enables `Begin`, `Insert*`, `Select*`, `Drop*`,
`Commit*`, `Rollback*`, `KillTransaction`, `Updater`; Tasks 7 and 8 define each, Task 6 the store they all use,
Task 9 the `Base` green set and witnesses. Everything else in the spec is a stub by design and named in the
later-plans section. Placeholders: the `...` at the end of action bodies stands for the `UNCHANGED` groups that
Task 6 tells the executor to define once; no other placeholder remains. Type consistency: `client[k].batch`,
`captured0` and `stale_interferences` are added to `ClientRecord` where first used (Tasks 8, 7, 9); `Range`,
`SetToSeq`, `ActorPc`, `ActorTxn`, `SetActorPc`, `Sess` are defined where first used (Tasks 7, 8) and used with
the same signatures afterwards.
