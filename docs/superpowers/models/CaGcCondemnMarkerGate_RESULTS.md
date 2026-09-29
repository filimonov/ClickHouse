---
description: 'TLC evidence for durable condemn-marker gating and logical-content NoDangle safety'
sidebar_label: 'CAS condemn-marker TLC results'
sidebar_position: 5
slug: /superpowers/models/CaGcCondemnMarkerGate-results
title: 'CAS condemn-marker TLC results'
doc_type: 'reference'
---

# `CaGcCondemnMarkerGate` results {#cagccondemnmarkergate-results}

## Verdict {#verdict}

The focused gate passed twice from identical final model inputs. The missing-marker sabotage violated the exact
invariant `NoDangle`, while the durable-marker gate exhausted its state graph without an error. The
runner printed `ALL EXPECTATIONS MET` only after both exact expectations matched.

`NoDangle` now expresses logical content identity rather than incarnation-token identity. This
model has one fixed content-addressed key and every present body carries that key's content, so a
committed reference requires `body.present`; it does not require the replacement body's current
token to equal the token previously observed by the writer. Tokens continue to authorize exact
deletion.

## Checker and finite scope {#checker-and-finite-scope}

- TLC: `2026.07.18.145032`, revision `30cc360`.
- Pinned `tla2tools.jar` SHA-256:
  `cc4803dce2a8ffaf0f5920a9dc39df4b5ee34ab4cb53fb58ac557277a7e516b3`.
- Workers: `1`.
- One fixed content-addressed body slot and one writer edge.
- `MaxGen = 3` and `MaxRounds = 6` in both configurations.
- Breadth-first search, deadlock checking, `TypeOK`, and `NoDangle` are enabled.
- Raw TLC logs, metadata, and generated traces are retained below repository `tmp`; generated
  source-adjacent trace modules were moved there before staging.

The model covers a swallowed condemn-marker write, same-token writer adoption in the cut/redelete
window, round graduation, confirmed durable marker evidence, marker retry, and exact-token deletion.
It is a focused gate for that interleaving, not the broader split publication protocol owned by
`CaBlobPublishCore`.

## Safety invariant {#safety-invariant}

```tla
NoDangle == committed => body.present
```

Presence is content identity in this one-key abstraction. `body.tok` is deliberately absent from
the invariant: an equivalent body replacement may have a new provider token without invalidating a
logical content-addressed reference.

## First complete run {#first-complete-run}

Command output: `build/task2_condemnmarker_tla.log`. TLC row-log run ID:
`CaGcCondemnMarkerGate-11-1787386011121917359`.

| Configuration | Expected | Exact observed result | Generated | Distinct | Queued | Depth | Seconds |
|---|---|---|---:|---:|---:|---:|---:|
| `CaGcCondemnMarkerGate_bug.cfg` | violation | `NoDangle` violated | 381 | 227 | 48 | 11 | 0 |
| `CaGcCondemnMarkerGate_fix.cfg` | green | `green` | 1,540 | 808 | 0 | 17 | 0 |

The runner exited `0` and its final line was exactly `ALL EXPECTATIONS MET`.

## Second complete run {#second-complete-run}

Command output: `build/task2_condemnmarker_rerun.log`. TLC row-log run ID:
`CaGcCondemnMarkerGate-11-1787411329587200063`.

| Configuration | Expected | Exact observed result | Generated | Distinct | Queued | Depth | Seconds |
|---|---|---|---:|---:|---:|---:|---:|
| `CaGcCondemnMarkerGate_bug.cfg` | violation | `NoDangle` violated | 381 | 227 | 48 | 11 | 1 |
| `CaGcCondemnMarkerGate_fix.cfg` | green | `green` | 1,540 | 808 | 0 | 17 | 0 |

The rerun also exited `0` and ended with exact `ALL EXPECTATIONS MET`. The deterministic state
counts and depths match the first complete run.

## Counterexample audit {#counterexample-audit}

The missing-marker trace was inspected rather than accepted from a generic TLC error:

- Before the invariant edit, `run_condemnmarker.sh bug` produced the exact `NoDangle` violation at
  depth `11`, with `381` generated, `227` distinct, and `48` queued states.
- After changing `NoDangle`, the same selected sabotage produced the same exact violation and state
  counts. This sensitivity check proves the logical invariant was not weakened into green.
- In the trace, `WCommit` makes `committed = TRUE` while
  `body = [present |-> TRUE, tok |-> 1]`. `GSettleRedelete` then consumes the retired entry and sets
  `body = [present |-> FALSE, tok |-> 0]` while `committed` remains true. The violation is caused by
  absence of the key's content, not by token mismatch.

The fixed configuration gates graduation on confirmed durable `Condemned` evidence or a synchronous
metadata re-read. Without confirmation, the retired entry is carried and the marker write retried.
Its entire graph completes with zero queued states.

## Commands {#commands}

```bash
docs/superpowers/models/run_condemnmarker.sh bug \
    > build/task2_condemnmarker_red_control.log 2>&1
docs/superpowers/models/run_condemnmarker.sh bug \
    > build/task2_condemnmarker_red_after_nodangle.log 2>&1
docs/superpowers/models/run_condemnmarker.sh \
    > build/task2_condemnmarker_tla.log 2>&1
docs/superpowers/models/run_condemnmarker.sh \
    > build/task2_condemnmarker_rerun.log 2>&1
```

The two selected negative controls each exited `0` because their declared exact sabotage
expectation matched. Both complete batteries were accepted only through the runner's exact row
parser and final marker.

## Scope note {#scope-note}

This is a bounded safety proof for durable condemn-marker gating. It does not model provider
wire-format behavior, multipart publication, or the split `HEAD`/publication transition system;
those are separate implementation/live-test obligations and `CaBlobPublishCore` model scope.

## 2026-09-30: persisted confirmation and prefetched observation {#persisted-confirmation-and-prefetch}

All four new configurations meet their expectations, and the two original configurations reproduce
their recorded results. `NoDangle` holds with the persisted confirmation, with the earlier
observation, and with both. The pre-fix gate with both extensions still reaches the missing-marker
dangle.

### What the extensions model {#extensions}

`PersistConf` persists the graduation evidence in the run row as `ret.pconf`. The flag is set
wherever `ret.conf` becomes true: a landed marker write at condemn, a landed retry, and graduation.
`GRestart` clears `ret.conf` and keeps `ret.pconf`. `GSettleGraduate` accepts
`ret.conf \/ (PersistConf /\ ret.pconf) \/ meta = "condemned"`, and `GSettleCarryRetry` fires only
when that gate fails. Two actions exist only under `PersistConf`. `GSettleGraduateDeferred` is the
carry after a passed gate with the graduation budget spent, and it sets `ret.pconf`.
`GSettleSupersede` replaces the row when the body carries another token, and the new row has only
its own marker outcome. Setting `ret.pconf` at condemn over-approximates the code, which persists
the flag only at the deferred carry; a larger set of flagged rows can only open the gate more often.

`Prefetch` separates the settle's `HEAD` from the settle. `GObserve` records the body's token, or
`0` when the body is absent, at any point between `GCut` and the settle. With an observation,
`GSettleCondemn`, `GSettleIdle` and `GSettleSupersede` decide on the observed token; without one they
read the live body, as an inline `HEAD` after a read-ahead miss does. `EndRound` clears the
observation. Both constants are `FALSE` in `_fix.cfg` and `_bug.cfg`, whose state graphs are
unchanged.

### Runs {#runs-2026-09-30}

TLC `2026.07.18.145032`, revision `30cc360`, the pinned jar above, `MaxGen = 3`, `MaxRounds = 6`,
`nice 10`. Battery: `run_condemnmarker.sh` with `TLC_WORKERS=8`, output `tmp/gc/tlc_condemnmarker_battery_final.log`,
ending `ALL EXPECTATIONS MET`.

| Configuration | `GateOnMarker` | `PersistConf` | `Prefetch` | Generated | Distinct | Depth | Seconds | Verdict |
|---|---|---|---|---:|---:|---:|---:|---|
| `_fix.cfg` | `TRUE` | `FALSE` | `FALSE` | 1,540 | 808 | 17 | 2 | green, equal to the recorded run |
| `_fix_pconf.cfg` | `TRUE` | `TRUE` | `FALSE` | 1,752 | 768 | 16 | 2 | green |
| `_fix_prefetch.cfg` | `TRUE` | `FALSE` | `TRUE` | 3,072 | 1,337 | 19 | 2 | green |
| `_fix_both.cfg` | `TRUE` | `TRUE` | `TRUE` | 3,586 | 1,285 | 17 | 2 | green |

The violation rows were rerun with `TLC_WORKERS=1`, because the counts at a violation depend on
worker scheduling. Output: `tmp/gc/tlc_condemnmarker_violations_w1_final.log`.

| Configuration | `GateOnMarker` | `PersistConf` | `Prefetch` | Generated | Distinct | Queued | Depth | Seconds | Verdict |
|---|---|---|---|---:|---:|---:|---:|---:|---|
| `_bug.cfg` | `FALSE` | `FALSE` | `FALSE` | 381 | 227 | 48 | 11 | 3 | `NoDangle` violated, equal to the recorded run |
| `_bug_both.cfg` | `FALSE` | `TRUE` | `TRUE` | 1,268 | 528 | 157 | 11 | 2 | `NoDangle` violated |

Green distinct counts are stable across repeated 8-worker runs. The reported depth of
`_fix_prefetch.cfg` was 18 in one run and 19 in another.

The `_bug_both.cfg` trace has the recorded shape: `Seed`, `GCut`, `GSettleCondemn` with the marker
swallowed (`meta = "clean"`), `GCut`, unconditional `GSettleGraduate`, `GCut`, `WPrecommit`,
`WAdopt` of token `1`, `WCommit`, then `GSettleRedelete` deletes token `1` under the committed
edge.

### Sensitivity and reachability {#sensitivity-2026-09-30}

Scratch variants of the model, run through a copy of the runner with 8 workers. None is committed.

| Variant | Base | Prediction | Result |
|---|---|---|---|
| swallowed condemn marker sets `ret.pconf` | `_fix_pconf` | `NoDangle` violated | violated |
| state with `ret.pconf`, no `ret.conf`, `meta = "clean"`, body replaced, gate evaluated | `_fix_pconf` | reachable | reachable |
| `observed_tok = 0` and body present | `_fix_prefetch` | reachable | reachable |
| body replaced after `GObserve` saw a token | `_fix_prefetch` | reachable | reachable |
| fresh condemn of a token the body no longer carries | `_fix_prefetch` | reachable | **unreachable** |
| `GSettleSupersede` disabled | `_fix_both` | distinct count changes | 1,237 against 1,285 |
| `GSettleGraduateDeferred` disabled | `_fix_both` | distinct count changes | **1,285, unchanged** |
| superseding row keeps `ret.pconf` | `_fix_both` | green, one writer cannot adopt the new token after the cut | green |

The persisted flag is load-bearing: evidence without a landed marker produces the dangle. The
cleared-marker case, where a writer replaced the token and set `Clean`, reaches the gate with only
`ret.pconf`, and `NoDangle` still holds.

### Limits {#limits-2026-09-30}

- **Fresh condemn on a stale observation needs `TwoWriters`.** With one writer, a body is replaced
  only while an existing retired row keeps the marker `Condemned`, so a fresh condemn always sees
  the live token. The two-writer runs below cover it.
- **Deferred carry adds no distinct state.** Every state it reaches is also reached by condemning in
  a later round. Its safety is checked, but it is not an independent path in this graph.
- **A wrong supersede needs `TwoWriters` to be detected.** With one writer, the superseding token's
  writer is folded by the next cut. The two-writer sabotage below catches it.
- **Marker writes land inside the settle.** A marker retry landing after the settle is represented
  only by a retry settling after the writer's replacement.

## 2026-09-30: two writers {#two-writers}

`NoDangle` holds when a fresh condemn uses a stale prefetched token, and that row's exact-token
delete finds the replacement. The pre-fix gate with every extension still reaches the dangle.

### What the extension models {#two-writers-extension}

`TwoWriters` makes `edge`, `adopted` and `committed` functions over `Writers`, which is `{1, 2}`
instead of `{1}`, and enables `WDrop`. `WDrop` removes a writer's edge and its committed reference,
as a dropped part or an insert aborted before commit does. Without it an edge is permanent, every
later cut folds the hash, and no body is condemned after a writer lands. `GCut` folds when any
writer's edge has landed. `NoDangle` becomes `\A w \in Writers : committed[w] => body.present`.
With `TwoWriters = FALSE` the functions have one point and `WDrop` is disabled; all six earlier
configurations were rerun and reproduced their generated, distinct and depth counts exactly.

### Runs {#two-writers-runs}

Same checker, jar, `MaxGen = 3`, `MaxRounds = 6` and `nice 10`. Output:
`tmp/gc/tlc_condemnmarker_2w.log` (8 workers) and `tmp/gc/tlc_condemnmarker_2w_bug_w1.log`
(1 worker).

| Configuration | Constants | Workers | Generated | Distinct | Queued | Depth | Seconds | Verdict |
|---|---|---:|---:|---:|---:|---:|---:|---|
| `_fix_both_2w.cfg` | gate, `PersistConf`, `Prefetch`, `TwoWriters` | 8 | 79,999 | 20,574 | 0 | 21 | 4 | green |
| `_bug_both_2w.cfg` | `PersistConf`, `Prefetch`, `TwoWriters`, no gate | 1 | 8,960 | 3,113 | 1,228 | 11 | 3 | `NoDangle` violated |

The `_bug_both_2w.cfg` trace has the recorded shape with writer `1`: swallowed marker,
unconditional graduation, `WPrecommit(1)`, `WAdopt(1)` of token `1`, `WCommit(1)`, then
`GSettleRedelete` deletes token `1`.

### Reachability {#two-writers-reachability}

Scratch variants of `_fix_both_2w.cfg` with one worker. `stale_tok` is a scratch-only variable
holding the token of the last fresh condemn whose observation no longer matched the body.

| Witness | Prediction | Result | Distinct at violation | Depth |
|---|---|---|---:|---:|
| settle about to fresh-condemn an observed token the body no longer carries | reachable | reachable | 4,115 | 12 |
| that row graduated, cut taken, exact-token delete about to find the replacement | reachable | reachable | 17,369 | 17 |
| sabotage: a superseding row with a swallowed marker keeps `ret.pconf` | `NoDangle` violated | violated | 14,878 | 16 |

The second trace is the full interleaving. Writer `1` lands after the round-1 cut, and token `1` is
condemned with its marker. Writer `1` drops, and round 2 spares the row. In round 3, writer `2`
lands after the cut, and `GObserve` records token `1`. Writer `2` then resurrects to token `2` and
drops. `GSettleCondemn` condemns token `1`, and round 4 graduates it. At round 5 the pending delete
of token `1` finds token `2`.

### Limits {#two-writers-limits}

- **Bounds.** At most three tokens and five complete rounds. The interleaving uses all three tokens'
  worth of transitions and ends in round 5, so longer chains, such as a second replacement, are
  outside the bound.
- **Supersede reset is load-bearing.** In the sabotage trace, writer `1` resurrects to token `2` and
  drops. The row supersedes to token `2` with its marker swallowed but flagged, and graduates. Writer
  `2` then adopts token `2` under a `Clean` marker, and the redelete removes it.
