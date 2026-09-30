# A publisher clears GC's marker on its own fresh incarnation: reachability, model, fix

- Code authority: `altinity/antalya-26.6` @ `e2dd0f5d343` (worktree `/home/mfilimonov/workspace/ClickHouse/cas-30`).
- Paths are relative to `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`.
- Four GC changes are assumed in place. The CAS-30 three:
  1. the meta-delete job deletes only `Condemned` at a round ≤ its own;
  2. `writeCondemnedMeta` re-stamps an older round;
  3. the graduation re-read requires `Condemned` at a round ≥ the entry's.
- The fourth, from the codex review: the in-process confirmation records its round, and graduation requires a round ≥ `entry.condemn_round`.
- Model, cfgs, runner and logs are in this directory. Nothing in `src/` was changed, built or run.

## 1. Verdict {#verdict}

**Reachable, and it loses data.** It is not closed by the four GC changes. The reviewer's exact schedule is confirmed in the code and reproduced by the model (`rv_bug`):

1. Transaction U stages a manifest, runs `precommitAdd`, then abandons without publishing X (`Pool/CasPartWriteTxn.cpp:1109-1149`).
2. Cut `c` folds U's `+1` and `-1`. X is touched with zero edges (`Gc/CasBlobInDegree.cpp:638-664`), so `head_blob` fires (`:565-577`).
3. Writer W precommits after cut `c`. Its HEAD finds the key empty, and it publishes T1.
4. GC's candidate HEAD, which runs after intake (`Gc/CasGc.cpp:3449-3452`), observes T1. It condemns T1 and `putMetaIfAbsent(Condemned(c))` lands. The in-process record is `(X, T1, c)` (`Gc/CasGcMetaWriter.cpp:155-156`).
5. W's `create(Clean)` conflicts. Its `readModifyWrite` overwrites any non-`Clean` meta (`Pool/CasPartWriteTxn.cpp:351-389`).
6. W abandons before cut `c+1`.
7. Round `c+1` graduates on the in-process record (`Gc/CasGc.cpp:2087`). The record's round `c` is ≥ the entry's `c`, so the fourth change does not stop it.
8. W2 precommits after cut `c+2` and adopts T1 through `Clean` (`Pool/CasPartWriteTxn.cpp:424-461`). The exact-token delete removes T1 (`Gc/CasGc.cpp:419-421`), and W2's committed part dangles.

**Why it happens.** Nothing ties the condemned token to the cut: HEAD returns only `{size, etag}` (`Backend/CasWriteResult.h:29`). EDGE-BEFORE-OBSERVE (`Gc/CasGc.cpp:2075-2084`) protects T1 only while W's edge survives to the next cut.

**The same defect on two other paths**, both confirmed:

- resurrect over a spare-left `Condemned`, via a fresh condemn;
- the supersede path.

**Race width.** GC's marker write, or on today's resurrect path GC's confirming GET, must land between W's PUT and W's reconcile. On the `head_blob` path W must be about 2 RTT late: one slow or retried S3 request inside its 90-second policy is enough. On the supersede path W must be late by the rest of the fold.

**Other conditions:**

- a recurring content hash;
- W's whole lifetime inside one GC interval;
- a third writer of the same content inside round `c+2`'s cut-to-delete window.

Each is ordinary; their conjunction is rare. Not measured.

**Restart variant: safe.** After a restart or leader change the registry is empty. The re-read sees `Clean`, refuses, and rewrites the marker (`Gc/CasGc.cpp:2091-2112`).

**Codex finding 1 is confirmed in the code.** The registry is created once per `Gc` (`Gc/CasGc.cpp:345`). Nothing clears it on lease loss. A losing attempt's marker job records its confirmation anyway (`Gc/CasGcMetaWriter.cpp:155-156, 204-208`). A supersede forgets only the old token (`Gc/CasGc.cpp:1026`). The model reproduces the schedule (`f1_bug`), and the fourth change closes it when publications do not race (`f1_fix`).

## 2. The fix {#fix}

**G1: delete the in-process shortcut at `Gc/CasGc.cpp:2087-2088`.** Graduation then always re-reads the meta and requires `Condemned` at round ≥ `entry.condemn_round`, which is CAS-30 change 3.

- **Lines.** −2. The registry becomes dead code, so the fourth change is subsumed.
- **Format and settings.** No format change, no setting, no writer request.
- **Cost.** +1 meta GET per graduation (7 → 8 requests per reclaimed blob).
  - Today that GET is a serial `loadMeta` inside the merge: roughly N × RTT per round for N graduations.
  - Hiding it behind the read-ahead needs a list of graduation candidates. None exists today; `hintRead` at `Gc/CasGcReadAhead.h:48` is the only building block.
- **Why G1 is sound.** Only T's publisher writes `Clean` while the body is still T. Every other publisher changes the token. The publisher's own `-1` follows its reconcile, because the upload fan-out waits for every task (`ContentAddressedTransaction.cpp:1780-1782`). So the reconcile lands before the cut that makes the row zero, and the graduation re-read sees it. Adopters that read `Clean` before that re-read have their edge in the delete round's cut, so they are spared.
- **Failure asymmetry.** Over-refusing costs one round of delay plus one PUT. Under-refusing is silent data loss.

**Residual G1 does not close (`pm_g1_extdrop`).** A namespace removal becomes durable while W's reconcile is in flight:

- cancellation follows the removal (`Pool/CasMountRuntime.cpp:231-245`);
- `requireAlive` runs only at the loop head (`Pool/CasPartWriteTxn.cpp:395`).

W's reconcile must also be delayed past the next round's gate, which means more than one GC interval inside its 90-second policy. Then an adopter in another namespace must hit the delete window. Adding W1 narrows this, but the combination still fails on an equal-round ABA (`pm_g1w1_extdrop` below).

## 3. Candidates {#candidates}

| | Shape | Lines | Requests | Model verdict | Decision |
|---|---|---|---|---|---|
| **W1**: the writer leaves a `Condemned` it lost to | the `readModifyWrite` lambda returns `nullopt` on `Condemned` (`Pool/CasPartWriteTxn.cpp:378`) | ~2 | none; the next writer republishes once | violated with stale attempts or markers (`pm_w1_stale`: equal-round ABA, the `replace` wins on identical bytes) and without the re-stamp | unsound alone |
| **W2**: overwrite only a provably stale marker | the only free round is the pre-publication meta's | ~4 | none | has W1's ABA hole | dominated by W1 |
| **G1**: the gate always re-reads | delete `Gc/CasGc.cpp:2087-2088` | −2 | +1 GET per graduation, serial | green everywhere except `ExternalDrop` | **chosen** |
| **G2**: the redelete re-reads first | GET before HEAD in `performRedeleteIo`; a refusal leaks the entry | ~20 | +1 GET per delete | `pm_g2_all` violated: a stale `Condemned` lands after the adopter read `Clean`, and no later cut catches that adopter | unsound |
| **G3**: condemn only tokens published before the cut | needs a body GET of the envelope provenance plus a clock comparison | — | +1 GET per condemn | — | not free |
| **G7**: forget the in-process record when the row is touched | a callback in `closeBlob` | ~10 | only touched rows | green in the first model; **violated once stale attempts exist** (`pm_g7_all`, `pm_g7_big`: an equal-round stale attempt confirms before the row exists, so the touch never forgets it) | rejected |

## 4. Model {#model}

`CaGcCondemnMarkerGate.tla` in this directory.

**Changes in this round:**

- **`RealCandidate`**: the code's candidate condition. A fresh condemn needs a delta in this cut. The supersede pre-empts every other settlement and reads `body`.
- **`Respawn`**: a dropped writer slot starts a new build, which supplies the third writer.
- **`AsyncMarker`**: the marker job lands after the HEAD. The next cut waits for it.
- **`premeta`, `FirstWriteWins`, `ReconcileResult`**: the reconcile's `create` or `replace`, then `readModifyWrite`. Equal bytes win.
- **Fix switches:**
  - `GateAlwaysReads` (G1)
  - `WriterLeavesMarker` (W1)
  - `DeleteReads` (G2)
  - `ForgetOnTouch` and `GateTouched` (G7)
- **Fault switches:**
  - `ExternalDrop`
  - `SuppressRounds`
- **`Registry`, `reg`**: the in-process confirmation as a set of `<<token, round>>` that outlives rounds and leadership changes. It forgets exactly where the code forgets: spare, redelete, supersede of the old token.
- **`RegRound`**: the fourth change.
- **`StaleAttempts`, `GStaleAttempt`**: one uncommitted attempt of this process HEADs the current token, writes its marker and keeps the confirmation. `GRestart` empties the registry.
- **`ExactAbsentPath`**: publication only into a key with neither body nor meta.
- **New invariants:**
  - `MarkerPreservedPending`: a `delete_pending` entry whose token is still the body keeps a `Condemned` marker.
  - `MarkerPreservedConfirmed`: the same, for a condemned entry the gate would accept on its in-process record.
- **`WitNoDeleteAfterRefusal`**: a reachability witness, expected to fail.

**Base for the new cfgs:** the four changes plus `SplitPublish`, `RealCandidate` and `Respawn`. "All" means `AsyncMarker`, `Prefetch`, `PersistConf`, `ReconcileFault`, `StaleMarkers` and `StaleAttempts`. Runs used 8 workers, `nice -n 10`, and a 20-minute cap. The machine was shared, so times are about 3× the earlier runs.

| cfg | flags | expect | result | distinct | s |
|---|---|---|---|---:|---:|
| `rv_bug` | reviewer schedule (`ExactAbsentPath`) | violation | `NoDangle` violated, trace read | 17,866 | 5 |
| `rv_bug_mp` | same | violation | `MarkerPreservedPending` violated | 3,177 | 4 |
| `rv_bug_async` | + Async | violation | `NoDangle` violated | 14,165 | 5 |
| `f1_bug` | codex finding 1: no `RegRound`, stale attempts, no split publication | violation | `NoDangle` violated, trace read | 1,333,161 | 31 |
| `f1_bug_mp` | same | violation | `MarkerPreservedPending` violated | 321,419 | 8 |
| `f1_fix` | four changes, all, Suppress, no split publication | green | green, all four invariants | 37,162,840 | 114 |
| `f1_fix_big` | same, `MaxGen 4`, `MaxRounds 8` | green | green | 241,031,093 | 726 |
| `pm_bug` | base | violation | violated | 90,777 | 7 |
| `pm_bug_all` | all | violation | violated | 1,641,510 | 45 |
| `pm_overapprox` | old candidate rule | violation | violated | 61,647 | 7 |
| `rv_g1` | G1, reviewer path, all | green | green, all invariants | 1,951,931 | 65 |
| `rv_g1_big` | same, Suppress, `MaxRounds 8` | green | green | 3,906,064 | 12 |
| `pm_g1` | G1 | green | green | 241,547 | 2 |
| `pm_g1_all` | G1, all, Suppress | green | green, all invariants | 108,547,117 | 326 |
| `pm_g1_noreg` | same, fourth change off | green | green | 108,547,117 | 306 |
| `pm_g1_overapprox` | G1, all, old candidate rule | green | green | 57,644,865 | 157 |
| `pm_g1_norestamp` | G1, all, no re-stamp | green | green | 79,572,691 | 207 |
| `pm_g1_nogateround` | G1, all, no CAS-30 round check | violation | violated | 35,332,696 | 860 |
| `pm_g1_extdrop` | G1, ExternalDrop | violation (residual) | violated | 73,907 | 8 |
| `pm_g1_wit` | G1, Async, witness | reachable | witness hit: a refused entry is deleted later | 11,400 | 6 |
| `pm_g1_big` | G1, all, `MaxGen 4`, `MaxRounds 8` | green | **timeout at 20 min** (≈228M distinct, no error) | — | 1200 |
| `pm_g1_r8` / `pm_g1_g4` | reduced-bound replacements | green | `pm_g1_g4` still running at report time (223M distinct, no error); `pm_g1_r8` not yet run | — | — |
| `pm_g7_all` / `pm_g7_big` | G7, all | green | **violated** (`MarkerPreservedPending`) | 180,871 / 261,366 | 0 / 1 |
| `pm_g7_mpc` | G7 | violation | `MarkerPreservedConfirmed` violated | 5,511 | 5 |
| `pm_g7_naive` | G7 naive | violation | violated | 164,032 | 9 |
| `pm_w1` | W1, no stale | green | green | 1,532,050 | 4 |
| `pm_w1_stale` | W1, all | violation | violated | 9,526,734 | 226 |
| `pm_g2_all` | G2, all | violation | violated | 1,890,991 | 60 |
| `pm_g1w1_extdrop` | G1+W1, ExternalDrop, stale attempts | expected green | violated: equal-round ABA through `StaleAttempts` | 57,061 | 0 |
| `pm_g1w1_extdrop_stale` | same + StaleMarkers | violation | violated | 6,626,988 | 168 |

**CAS-30 regression.** All 22 cfgs keep their verdicts, and every green one keeps its exact distinct count (`final2_regress.txt`).

**Traces read:**

- `rv_bug` is the reviewer's schedule step by step: U precommits and drops, cut, W publishes into the empty key, condemn at 2 with `reg {<<1,2>>}`, `Clean` overwrite, drop, graduate on `reg`, adopt, delete.
- `f1_bug` is codex finding 1 in its fresh-condemn form:
  - a stale attempt at round 1 confirms `(T2, 1)`;
  - the live condemn at 4 loses its marker write;
  - the round-1 job of the older incarnation deletes `Condemned(1)`;
  - graduation on the cached pair, then the delete.
- `pm_g7_all`: the equal-round stale confirmation predates the row, so the touch never forgets it.
- `pm_g1w1_extdrop`: the equal-round stale marker leads to W's `replace` winning on identical bytes after the gate.

## 5. Limits {#limits}

**Not run:**

- Nothing in `src/` was built, run or provoked.
- Race widths, likelihood and G1's latency are derived, not measured.

**Bounds:**

- One hash, two writer slots with respawn, at most one stale attempt per behaviour.
- `MaxGen` ≤ 4, `MaxRounds` ≤ 8.
- `pm_g1_big` timed out. The reduced-bound replacements were still running when this report was written.
- `pm_nosplit` and `f1_fix` are the same configuration.

**Model simplifications:**

- Clamps are modelled only as `SuppressRounds`.
- G2's read and delete are one step.

**Premises assumed:**

- The per-request lease fence stops a superseded writer epoch's reconcile (`Backend/CasRequests.h:323`, `Pool/CasRefLedger.cpp:4790-4806`).
- An aborted attempt's marker job still lands (`drainOnExitNoThrow`).
