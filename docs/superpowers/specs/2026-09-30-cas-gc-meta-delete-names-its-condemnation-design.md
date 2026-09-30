---
description: 'Fix for CAS-30: a delayed GC `.meta` delete job can remove a later incarnation''s condemn marker, after which a writer adopts a token GC then deletes. The job, the condemn-marker write and the graduation re-read all compare `condemn_round`; no format change, no request on the writer path.'
sidebar_label: 'CAS GC: meta delete names its condemnation'
sidebar_position: 102
slug: /superpowers/specs/cas-gc-meta-delete-names-its-condemnation-design
title: 'CAS GC: a `.meta` delete removes only the condemnation it was scheduled for'
doc_type: 'reference'
---

# CAS GC: a `.meta` delete removes only the condemnation it was scheduled for — design {#cas-gc-meta-delete-names-its-condemnation}

- Date: 2026-09-30. Status: rev.1, for owner review.
- Base: `altinity/antalya-26.6` @ `e2dd0f5d343`. All code citations are against it. Paths are relative to
  `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/` unless they start with `src/` or `docs/`.
- Backlog: CAS-30. Inputs: `docs/superpowers/reports/2026-09-30-cas-30-stale-meta-delete-adjudication.md`
  (the adjudication), `docs/superpowers/reports/2026-09-30-cas-gc-request-cuts/05-memo-vs-deposed-leader-adjudication.md`.
- Model evidence for this spec was run on a variant of `CaGcCondemnMarkerGate.tla` in
  `tmp/cas30-meta-model/` (master worktree). Section 7 lists every run.

## 1. Decision and rule {#decision}

**Take candidate (a), with one addition the adjudication missed.** The code enforces one rule:

> A `Condemned` meta at round `r` guards every committed condemnation of its hash at a round `≤ r`.
> A `.meta` delete scheduled for a condemnation at round `c` removes the meta only while it is
> `Condemned` at a round `≤ c`. Confirming a condemnation at round `c` requires a `Condemned` meta at a round `≥ c`.

Three sites change, all in GC:

| Site | Today | After |
|---|---|---|
| `deleteConfirmedMeta` (`Gc/CasGcMetaWriter.cpp:86-92`) | deletes whatever meta it reads | deletes only `Condemned` with `condemn_round ≤ c`, by the etag it read |
| `writeCondemnedMeta` (`:70-80`) | leaves any `Condemned` meta alone and reports it confirmed | re-stamps a `Condemned` meta of an older round; leaves an equal or newer one |
| graduation re-read in `confirm_condemned_marker` (`Gc/CasGc.cpp:2085-2113`) | accepts any `Condemned` meta | accepts only `Condemned` with `condemn_round ≥ entry.condemn_round` |

- **No format change.** `BlobMeta::condemn_round` and `RetiredEntry::condemn_round` already exist.
- **No request on the writer path.** GC's normal path is unchanged. The re-stamp costs one PUT, and
  only when an older-round `Condemned` meta is present at condemn time.
- **About 30 changed source lines** in three files, plus tests, the model and 4 lines of user docs.

## 2. The defect {#defect}

### 2.1 Code facts {#code-facts}

| # | Fact | Where |
|---|---|---|
| F1 | A `Removed` or `Gone` redelete schedules `scheduleConfirmedMetaDelete(entry.ref)`: the job gets the ref only | `Gc/CasGc.cpp:464-467` (`applyRedeleteOutcome`) |
| F2 | The job runs `loadMeta` then `deleteMetaExact(lm->etag)`: it deletes whatever meta is current | `Gc/CasGcMetaWriter.cpp:86-92, 160-167` |
| F3 | The job admits its own `CasOperation` on `openRequests()`; the GC fence is generation 0; no lease or `gc/state` check exists on its path | `Gc/CasGcMetaWriter.cpp:164`, `Pool/CasPool.cpp:197` |
| F4 | The round drains the meta pool (`meta_pool_wait`) before its `gc/state` CAS, so a job of a round that later loses the CAS still runs | `Gc/CasGc.cpp:1102` before `:1149` |
| F5 | The graduation gate returns true on the in-process `(ref, token)` record without reading the meta | `Gc/CasGc.cpp:2087` |
| F6 | The redelete never reads the meta: `HEAD`, then a conditional `remove` by the observed body etag | `Gc/CasGc.cpp:411-423` (`performRedeleteIo`) |
| F7 | A writer that finds the body present and the meta not `Condemned` adopts the token (`Observed`), backfilling `Clean` if absent | `Pool/CasPartWriteTxn.cpp:424-461` |
| F8 | `writeCondemnedMeta` leaves an existing `Condemned` meta alone and returns true | `Gc/CasGcMetaWriter.cpp:79` |
| F9 | The graduation re-read accepts any `Condemned` meta | `Gc/CasGc.cpp:2091` |
| F10 | The meta has no token field. Its bytes are a deterministic function of `(state, condemn_round, size)` | `Formats/CasBlobMetaFormat.h:35-42`, `encodeBlobMeta` |
| F11 | On S3 an etag is content-derived, so two `Condemned` metas of one hash differ in etag only if their rounds differ | `Backend/CasBackend.h:120-140` |

### 2.2 S1 — deposed leader, base, no restart {#s1}

**Actors:** GC leader L1, successor L2, writers W1 and W2. Content hash X.

1. L1's round deletes X@T0 (`delete_pending` from its committed parent, condemned at `c0`). F1 queues the job.
2. L1 freezes before its pool runs the job: SIGSTOP, a VM pause, a swap storm, or an S3 partition. Its
   heartbeat goes silent too.
3. L2 steals after two frozen observations and runs rounds. It inherits T0 `delete_pending`. Its redelete
   finds T0 `Gone`, and its own job removes T0's marker.
4. W1 re-creates X as T1 (`Clean` meta), then drops it.
5. L2 condemns T1 at `c1` (a fresh condemn, or a supersede of the T0 row, `CasBlobInDegree.cpp`
   `closeBlob`). The marker `Condemned(c1)` lands and is confirmed in process.
6. L1 resumes. Its job reads `Condemned(c1)` and deletes it (F2).
7. L2 graduates T1 on the in-process record (F5).
8. W2 lands its edge after L2's next cut, reads an absent meta, and adopts T1 (F7).
9. L2's redelete removes T1 (F6). W2's committed part names a deleted blob.

- **Stall:** L1 frozen from step 2 to step 6. That is at least the steal (two follower ticks) plus one L2
  round: a few minutes at the default 60 s `gc_interval_sec`. Derived from the steal condition, not measured.
- **End state:** a dangling reference. This is data loss.
- **Restart does not matter:** L2 never restarts.

### 2.3 S2 — the stale delete lands between graduation and redelete {#s2}

Same as S1, except L1's job runs after step 7 and before step 9's cut. Neither graduation nor the redelete
reads the meta after that point (F5, F6), so the fresh re-read at graduation that a restart would force
does not help either.

### 2.4 S3 — the old marker object guards the new incarnation {#s3}

F8 lets the object written for T0 end up standing for T1. Then a job of T0's round is entitled to remove
it under any rule that looks only at the meta. Two single-fault ways to get there:

- **S3a, writer reconcile throws.** W1 re-creates T1 while T0's `Condemned(c0)` is still present. Its
  `reconcileMetaClean` (`Pool/CasPartWriteTxn.cpp:338-390`) throws after the publication. L2's redelete of
  T0 then HEADs T1, ends `Mismatch`, and schedules no job. L2 condemns T1 at `c1`: F8 leaves
  `Condemned(c0)` in place and confirms. L1's stale job then deletes `Condemned(c0)`.
- **S3b, a deposed leader writes an old-round marker.** A leader stalled even longer writes `Condemned(r)`
  with `r ≤ c0` over T1's `Clean` meta. L2's condemn leaves it alone (F8) and confirms.

In both, a graduation re-read after a restart sees `Condemned` and accepts it (F9). This is the same
object a stale job may delete.

### 2.5 Where this spec departs from the adjudication {#corrections}

The code is the authority. Five points differ:

1. **The fix it proposes is incomplete.** The graduation re-read (F9) also accepts an older-round marker.
   Without a round check there, S3 survives the job fix and the re-stamp. Model runs `bug_gateround` and
   `nogate_stale` violate `NoDangle` (section 7).
2. **The "left alone" case needs one fault, not two.** The adjudication required L2's own meta delete to
   be lost. S3a needs only the writer's reconcile throw: L2's redelete ends `Mismatch` and schedules
   nothing. S3b needs no failure at all, only a second stale leader.
3. **On S3, "exact etag" means "exact bytes"** (F10, F11). "T1's marker is a new version with a new etag"
   holds only because `c1 ≠ c0`. The round is the only field that tells two markers of one hash apart. So
   candidate (b) is a round comparison in disguise (section 4.2).
4. **fsck gives no false finding.** A `Condemned` meta left over an absent body is counted in
   `meta_without_body`. That counter is advisory and excluded from `clean()` (`Tools/CasFsck.h:109-117, 175`;
   `Tools/CasFsck.cpp:1034-1050`). The dry-run (`previewDeletes`) reads run rows, never the meta.
5. **Line numbers moved.** At `e2dd0f5d343`: the gate is `Gc/CasGc.cpp:2085-2113`, not `:2166`, and the
   job is scheduled at `:466`.

## 3. Premises the fix leans on {#premises}

| Premise | Discriminator | Verdict |
|---|---|---|
| P1. A committed later incarnation has a larger `condemn_round` than any committed earlier one of the same hash | Only the redelete deletes a blob body (`Gc/CasGc.cpp:879-881`; other `remove` sites delete manifests, generations, staging or mount objects). The redelete takes rows `delete_pending` in a committed parent, and graduation needs `condemn_round < current_round` (`CasBlobInDegree.cpp:485`). So T0's body goes after round `c0+1` committed. A round commits only by CAS on the `gc/state` etag it adopted (`Gc/CasGc.cpp:1149`), with `new_round = state.round + 1` (`:643`). A round whose HEAD saw T1 started after T0's deletion, so it adopted a state at round `≥ c0+1` and numbers itself `≥ c0+2` | **holds**, see the limit below |
| P2. `gc/state.round` never decreases | Writers: the round CAS (`:1124`, `+1`), the lease acquire (`:4899-4977`, copies it), `rebuildBaseline` (`:4681`, `max(max_fence_round, state.round, max_gen) + 1`) | **holds while `gc/state` is readable.** Bootstrap on an absent `gc/state` starts at 0 (`:4909-4916`). A rebuild from an undecodable one is bounded below only by surviving fence and generation numbers |
| P3. A condemn or supersede stamps the round that makes it | `condemn_round = state.round + 1` (`:1968`) feeds `head_blob` (`:2046`) and the supersede (`CasBlobInDegree.cpp` `closeBlob`, `fresh.condemn_round = condemn_round`) | **holds** |
| P4. A carried row keeps its round | `toRetiredEntry` copies `condemn_round`; graduation copies the entry | **holds** |
| P5. Stale jobs carry rounds of committed rows only | The job's round comes from `entry` in `redelete`, which holds only prior-run rows (`CasBlobInDegree.cpp` `settleEntry`, `e.delete_pending` branch) | **holds** |
| P6. Every writer of a blob `.meta` is known | `git grep` for `blobMetaKey`, `loadMeta`, `putMetaIfAbsent`, `casMeta`, `deleteMetaExact` at `e2dd0f5d343`: GC `writeCondemnedMeta`, GC `deleteConfirmedMeta`, writer backfill (`CasPartWriteTxn.cpp:437`), writer `reconcileMetaClean` (`:338-390`). Adoption is the backfill. `CasInspect` and fsck only read | **holds**; the reconcile is the suspected sibling of section 6 |

**The limit of P1/P2.** If `gc/state` is lost or undecodable and then rebuilt, new rounds can be lower than
rounds already stamped into `.meta` objects. After the fix:

- An older, higher-round marker still counts as confirmation. That is safe and conservative.
- A legit job leaves such a marker behind. It is advisory in fsck, and the next writer heals it.
- A job from a pre-rebuild process, captured at a high round, could delete a post-rebuild marker with a
  lower round. That needs a GC process to stay frozen across an operator-run disaster recovery.

Today's code deletes unconditionally, so the fix is never worse here.

**Why the rule then holds** (checked by the model, section 7):

1. After the fix, a confirmation of `(T, c)` means that at that moment the meta was `Condemned` at a round
   `≥ c`. `writeCondemnedMeta` returns true only after creating it, re-stamping it, or observing round
   `≥ c`. The re-read requires `≥ c`.
2. After confirmation, no GC actor lowers or removes that guard while T is present:
   - `writeCondemnedMeta` only creates or raises.
   - A job for another incarnation carries a round `< c` (P1, P5), so it leaves round `≥ c` alone.
   - T's own job runs only after T's body is `Removed` or `Gone`.
3. So no writer can adopt T after confirmation, and the existing argument applies: a pre-confirmation
   adopter's edge is folded before the delete.
4. Read, compare and conditional delete act on one etag. The decision depends only on the bytes, and on S3
   the etag is the bytes. So an ABA re-creation of identical bytes cannot change the outcome.

## 4. Candidates {#candidates}

### 4.1 (a) The job carries the retired entry's `condemn_round` — chosen {#cand-a}

- **Code shape:** section 5.
- **Requests:** none added to the writer path or to the normal GC path. It saves the meta `DELETE`
  when the job finds `Clean`. That meta belongs to a live incarnation, and today's delete of it costs the
  next adopter a backfill PUT. The re-stamp costs one PUT when it fires. A refused re-read costs one round
  of delay and one PUT.
- **Lines:** about 30 across `Gc/CasGcMetaWriter.{h,cpp}` and `Gc/CasGc.cpp`.
- **Does not close:**
  - the writer's reconcile overwriting a marker placed on its own fresh incarnation (section 6);
  - the rebuild-regression corner of P2.

### 4.2 (b) The job carries a meta etag captured when the delete was decided {#cand-b}

**Where the etag can come from, without a format change:**

| Source | Available? |
|---|---|
| The graduation gate | No. The in-process path reads nothing (F5). Graduation and delete are different rounds, possibly different processes, and the run row has no etag field (frozen). |
| `writeCondemnedMeta`'s result, kept in the in-process registry | Only in the process that wrote the marker. In S1 the stale job may belong to a leader that inherited T0. |
| A meta `GET` in the redelete worker, **before** the body `HEAD` | Yes. It must come first: a `GET` after a `Gone` HEAD can read the successor's marker for a re-created incarnation, which is S1 again. |

**Assessment:**

- **Closes:** S1 and S2. The successor's marker has another round, so another etag on S3 (F11) and another
  generation on GCS.
- **Does not close S3.** The left-alone object keeps its bytes and its etag. Closing S3 needs the re-stamp
  and the re-read round check, both of which are round comparisons.
- **Requests:** the `GET` moves from the job into the worker, the same count on `Removed`/`Gone`. It adds
  one wasted `GET` per `Replaced` outcome and one serial round trip per redelete on the `gc_io_concurrency`
  worker path.
- **Lines:** about 25 (`RedeleteIo` gains the etag; `performRedeleteIo` reads it; the job signature changes)
  plus the 5 lines of re-stamp and re-read.
- **Only advantage:** on the S1/S2 path it does not rely on P1/P2. So it also covers the rebuild-regression
  corner.
- **Verdict:** (a) plus extra requests. Rejected.

### 4.3 (c) A leadership or lease check inside the job {#cand-c}

- **Sound only in the order read meta → read `gc/state` (`refreshAuthority`) → delete by the meta etag.**
  A freeze between the check and the delete is then closed by the etag, which is again the round argument (F11).
- **Does not close S3:** the left-alone object's etag is still current.
- **Requests:** one `gc/state` `GET` per meta delete, up to `gc_round_redelete_budget` per round.
- **Lines:** about 10. Rejected.

### 4.4 (d) Delete the meta synchronously in the redelete worker {#cand-d}

- **Shape:** after the body `DELETE` in `performRedeleteIo`, the worker itself runs `GET` + `DELETE` of the
  meta; no job is scheduled.
- **Does removing the delay remove the defect? No.**
  - A freeze can land between the body `DELETE` and the meta `GET` in the worker: the same window without
    the queue.
  - A partitioned worker keeps retrying its `GET` inside the 90 s `Retry::standard()` window
    (`Backend/CasRetry.h:41`) and reads the successor's state after the heal.
  - The exposure shrinks from queue depth to one request gap per in-flight worker, but it does not close.
- **Does not close S3.**
- **Cost:** each redelete becomes `HEAD`, `DELETE`, `GET`, `DELETE` serially on `gc_io_concurrency`
  workers, about twice the phase-11 worker time on delete-heavy rounds. It also moves meta work off the
  bounded meta pool.
- **Lines:** about 20. Rejected.

### 4.5 Comparison {#comparison}

| | (a) round | (b) etag | (c) lease check | (d) synchronous |
|---|---|---|---|---|
| S1, S2 | closed | closed | closed (with etag) | narrowed only |
| S3 | closed with re-stamp + re-read check | needs (a)'s two additions | not closed | not closed |
| Requests added | none on the normal path | +1 GET per `Replaced`, +1 serial RTT per redelete | +1 GET per meta delete | none; about 2× worker time |
| Relies on P1/P2 | yes | no, for S1/S2 | no | — |
| Lines | ~30 | ~30 | ~15 | ~25 |

## 5. The fix {#fix}

**`Gc/CasGcMetaWriter.h`:**

```cpp
/// Drop the freshness meta of a blob whose body is confirmed deleted or absent, but only while the meta
/// still records this condemnation or an older one.
void scheduleConfirmedMetaDelete(const BlobRef & ref, uint64_t condemn_round);
```

**`Gc/CasGcMetaWriter.cpp`:**

```cpp
bool writeCondemnedMeta(CasOperation & op, const Layout & layout, const BlobRef & ref,
                        uint64_t condemn_round, uint64_t size)
{
    const auto lm = loadMeta(op, layout, ref);
    const BlobMeta desired{.state = MetaState::Condemned, .condemn_round = condemn_round, .size = size};
    if (!lm)
        return std::holds_alternative<Committed>(putMetaIfAbsent(op, layout, ref, desired));
    /// An older round may be a previous incarnation's marker, which that incarnation's delete job may
    /// still remove; re-stamp it so this condemnation owns the object.
    if (lm->meta.state != MetaState::Condemned || lm->meta.condemn_round < condemn_round)
        return std::holds_alternative<Committed>(casMeta(op, layout, ref, lm->etag, desired));
    return true;
}

/// A later incarnation is always condemned at a larger round, so a meta newer than `condemn_round`, or
/// one that is not `Condemned`, belongs to a live incarnation and stays.
void deleteConfirmedMeta(CasOperation & op, const Layout & layout, const BlobRef & ref, uint64_t condemn_round)
{
    const auto lm = loadMeta(op, layout, ref);
    if (!lm || lm->meta.state != MetaState::Condemned || lm->meta.condemn_round > condemn_round)
        return;
    deleteMetaExact(op, layout, ref, lm->etag);
}
```

`scheduleConfirmedMetaDelete` captures `condemn_round` by value, like its sibling. The header comment block
(`:36-59`) changes one clause: GC "may REMOVE the meta once the exact body token is confirmed
deleted/absent" becomes "…and only while the meta is `Condemned` at a round no newer than the entry's".
The comment on `writeCondemnedMeta` (`:61-69`) drops "left alone rather than clobbering a possibly-newer
condemn_round" in favour of the rule above.

**`Gc/CasGc.cpp`:**

- `applyRedeleteOutcome`: `meta_writer->scheduleConfirmedMetaDelete(entry.ref, entry.condemn_round);`
- `confirm_condemned_marker`:

```cpp
/// An older round is a previous incarnation's marker, which a stale delete job may still remove.
if (const auto lm = loadMeta(op, layout, entry.ref);
    lm && lm->meta.state == MetaState::Condemned && lm->meta.condemn_round >= entry.condemn_round)
```

  On refusal the existing carry-and-retry runs. The retry's `writeCondemnedMeta` then re-stamps.

**`docs/en/antalya/cas/architecture/garbage-collection.md`**, section `#condemn-delete`:

- The edge `"Deleted or Absent" → "then drop the .meta"` becomes "drop the `.meta` if it is still
  `Condemned` at round ≤ n".
- One sentence: a `Condemned` marker from an older round is re-stamped to the current round, and the
  graduation re-read accepts only a marker at the entry's round or newer.

## 6. Sibling sites {#siblings}

Every site that reads a blob `.meta` and then writes or deletes it by the etag it read, after a decision
made earlier:

| Site | What it does | Class |
|---|---|---|
| `deleteConfirmedMeta` (`Gc/CasGcMetaWriter.cpp:86-92`) | delete by a fresh etag, decided a round or more earlier | **the defect**; fixed |
| `writeCondemnedMeta` left-alone branch (`:79`) | confirms whatever `Condemned` object is there | **same defect** (S3); fixed by the re-stamp |
| Graduation re-read (`Gc/CasGc.cpp:2091`) | confirms whatever `Condemned` object is there | **same defect** (S3); fixed by the round check |
| `writeCondemnedMeta` create and `Clean → Condemned` (`:75-78`) | only adds protection; a stale one makes a writer republish once | safe: the gate already accepts it (comment at `Gc/CasGc.cpp:2105-2110`) |
| Writer backfill `putMetaIfAbsent(Clean)` (`Pool/CasPartWriteTxn.cpp:437-442`) | create-if-absent only | safe: never replaces a marker |
| Writer `reconcileMetaClean` (`Pool/CasPartWriteTxn.cpp:338-390`) | after publishing a fresh incarnation, writes `Clean` over whatever `Condemned` it finds (`replace` by the etag read before publishing, or `readModifyWrite`) | **suspected, separate mechanism**, see below |

**The suspected sibling: the publisher clears a marker placed on its own new incarnation.**

- **Schedule.**
  1. GC round `c` takes its cut.
  2. Writer W lands its edge after the cut, HEADs, and publishes a fresh incarnation T2.
  3. In the same round, GC's `head_blob` or supersede `HEAD` observes T2 and condemns it. The marker lands
     and is confirmed.
  4. W's reconcile writes `Clean` over it (lines 369-389). On the resurrect path the plain `replace` by W's
     pre-publication etag succeeds whenever GC left that object alone (F8).
  5. W's edge drops before cut `c+1`: the insert aborts after publication.
  6. `c+1` graduates T2 on the in-process record.
  7. A writer adopts T2 inside `c+2`'s cut-to-delete window, and `c+2` deletes it.
- **Model:** `fix_splitpub` violates `NoDangle` with the full fix in place (section 7).
- **Not verified in the code:** step 3 needs X to be a GC candidate in round `c` while W's edge is not in
  the cut. That means a `+1`/`−1` pair folded at cut `c` with no surviving edge, for example a writer that
  precommitted and aborted before publishing. The model over-approximates this candidate condition.
- **Why it is not in this diff:** it overwrites rather than deletes, the actor is the writer, and the writer
  holds no round. No CAS-30 candidate touches it. The likely fix is on the writer path: on a lost race,
  leave a `Condemned` meta instead of forcing `Clean`. The writer's own reference is protected by its
  landed edge, not by the meta. That fix is not verified here.
- **It breaks the same premise** that spec `2026-09-29-cas-gc-request-cuts-design.md` §2 point 1 relies on.
  Open question 1.

## 7. Model {#model}

**Changes to `docs/superpowers/models/CaGcCondemnMarkerGate.tla`.** All were run in `tmp/cas30-meta-model/`.

- **`meta`** becomes `[st : {"absent", "clean", "condemned"}, cr : 0..MaxRounds]`.
- **`WAdopt`** backfills `clean` over `absent`.
- **`mdq`**: the rounds captured by outstanding meta-delete jobs.
- **`GSettleRedelete`**, with `SplitMetaDelete`:
  - it deletes only the body;
  - on a removal or an absent body it adds `ret.cr` to `mdq`;
  - on a token mismatch it adds nothing.
  - With `SplitMetaDelete = FALSE` it keeps today's atomic body+meta delete.
- **`GMetaDeleteJob(c)`** for `c \in mdq`, enabled in every state and not fenced by `EndRound`, so any past
  leader can take it. `JobRoundCheck` selects the fixed rule over today's unconditional delete. Read and
  conditional delete are one step: the decision depends only on the bytes, which the etag names (§3, point 4).
- **`WPublish`** merges `WResurrect` and `WCreate` and mints a fresh token.
  - `ReconcileFault`: the reconcile to `clean` may throw, leaving the meta untouched.
  - `SplitPublish`: `WReconcile(w)` becomes its own step.
- **`CondemnedAfterWrite(r)`** is `writeCondemnedMeta`. `Restamp` raises an older round.
  `GSettleCondemn`, `GSettleSupersede` and `GSettleCarryRetry` use it.
- **`MetaGuards`** is the graduation re-read. `GateRoundCheck` adds `meta.cr >= ret.cr`.
- **`GStaleMarkerWrite(r)`**, `r \in 1..round`: a deposed leader's marker write at an old round.
  `StaleMarkers` enables it.

**Measured runs** (TwoWriters, `MaxGen = 3`, `MaxRounds = 7` unless noted, 8 workers, nice 10; each ≤ 6 s):

| cfg | Flags on (beyond gate) | Expected | Result | Distinct |
|---|---|---|---|---:|
| `atomic_regress` | — | green | green | 20,802 |
| `bug_jobdelay` (S1/S2) | Split | violation | `NoDangle` violated | 25,879 |
| `bug_restamp` (S3a) | Split, Job, Gate, Fault | violation | violated | 55,489 |
| `bug_gateround` (S3a via re-read) | Split, Job, Restamp, Fault | violation | violated | 50,677 |
| `norestamp_stale` (S3b) | Split, Job, Gate, Stale, PConf, Prefetch | violation | violated | 296,714 |
| `nogate_stale` (S3b via re-read) | Split, Job, Restamp, Stale, PConf, Prefetch | violation | violated | 299,929 |
| `job_only` | Split, Job, PConf, Prefetch | green | green | 40,197 |
| `bug_stale` | Split, Stale | violation | violated | 119,853 |
| `fix` | Split, Job, Restamp, Gate, Fault | green | green | 52,645 |
| `fix_both` | `fix` + PConf, Prefetch | green | green | 104,271 |
| `fix_stale` | `fix_both` + Stale | green | green | 719,379 |
| `fix_stale_big` | `fix_stale`, `MaxGen = 4`, `MaxRounds = 8` | green | green | 2,591,915 |
| `fix_splitpub` | Split, Job, Restamp, Gate, SplitPublish | violation (section 6) | violated | 34,980 |

- **Every counterexample was read, not just counted:**
  - `bug_jobdelay` is S1: redelete at round 3, a job with round 1 queued, T2 condemned at 4, the job
    deletes `Condemned(4)`, then adopt and redelete.
  - `bug_restamp` is S3a.
  - `norestamp_stale` is S3b.
- **The eight existing cfgs** keep their verdicts when run against the variant with every new constant
  `FALSE` (3 violated, 5 green). Their distinct counts change because `absent` and `clean` are now two
  states. `CaGcCondemnMarkerGate_RESULTS.md` must be regenerated.
- **Cfgs to add to `run_condemnmarker.sh`:**
  - violation: `bug_jobdelay`, `bug_restamp`, `bug_gateround`, `norestamp_stale`, `nogate_stale`;
  - green: `fix`, `fix_both`, `fix_stale`.
  - `fix_splitpub` joins as an expected violation until section 6 is decided.
- **State-space order:** 10^4 to 10^6 distinct states, seconds at 8 workers.

## 8. Tests, failing first {#tests}

- **Framework:** gtest, deterministic, no sleeps. No `LOGICAL_ERROR` is added, so no death tests.
- **Fixtures:**
  - Unit tests use `GcMetaWriter` through `Gc::metaWriterForTest` over `MetaWriteLatchBackend` or a plain
    `InMemoryBackend`, as in `src/Disks/tests/gtest_cas_gc_meta_writer.cpp`.
  - Round tests reuse the part-written-and-dropped fixture of `ThrowingRoundDrainsBeforeReturning` in the
    same file.
- **Failing first:** write tests 1–5 against today's signature, `scheduleConfirmedMetaDelete(ref)`, to see
  them red at run time. Then change the signature.

| # | Test (`CASGcMetaWriter.*`) | Inputs | Expected after the fix | Today | AC |
|---|---|---|---|---|---|
| 1 | `ConfirmedMetaDeleteSparesALaterCondemnation` | meta `Condemned(5)`; job with round 3; `drain` | meta `Condemned(5)`, same etag | meta absent | #1 |
| 2 | `ConfirmedMetaDeleteSparesACleanMeta` | meta `Clean`; job with round 3 | meta `Clean` | meta absent | #1 |
| 3 | `ConfirmedMetaDeleteRemovesItsOwnCondemnation` | `Condemned(3)` with job 3; separately `Condemned(2)` with job 3 | both absent: no leaked `.meta` | green (guard) | #1 |
| 4 | `CondemnMarkerWriteRestampsAnOlderRound` | meta `Condemned(3)`; `scheduleCondemnMarkerWrite(ref, T, 5, size)`; `drain`; then job with round 3 | meta `Condemned(5)` after the write, confirmed in process; still `Condemned(5)` after the job | stays `Condemned(3)`, then deleted | #1 |
| 5 | `CondemnMarkerWriteLeavesANewerRound` | meta `Condemned(7)`; marker write at 5; job with round 5 | `Condemned(7)` throughout, confirmed. This pins the accepted leftover | job deletes it | #1 |
| 6 | `GraduationReReadRefusesAnOlderRoundMarker` | fixture; run rounds until X is condemned at `c ≥ 1`; `casMeta` the meta to `Condemned(c-1)`; destroy the `Gc` and build a fresh one with the same id (empty registry); run the graduation round | no `delete_pending` for X in `previewDeletes`; `CASGCCondemnMarkerUnconfirmedCarry` +1; after the round the meta is `Condemned(c)`; the next round graduates | graduates on `Condemned(c-1)` | #2 |
| 7 | `DeposedLeaderMetaDeleteSparesSuccessorMarker` | see below | A's job leaves `Condemned(c1)`; W2 publishes rather than adopts; after B's redelete round W2's blob is present | meta absent; W2 `Observed`; blob deleted | #1 |

**Test 7, step by step:**

1. Two `Gc` instances, A and B, share one store over a new `MetaReadLatchBackend`. It is about 25 lines
   in `src/Disks/tests/cas_test_helpers.h`: a one-shot latch on the first `read` of a `.meta` key after `arm`.
   The existing `MetaWriteLatchBackend` latches after the job's `GET`, so the job would already hold T0's
   etag and could not reproduce the defect.
2. Drive A until X@T0 is `delete_pending`. Then `arm` and run A's deleting round on `std::async`.
3. `awaitLatchEntered`: A's job is parked before its read. A's round blocks in `meta_pool_wait`.
4. B steals: two `runRegularRound` calls, as in `gtest_cas_gc_round.cpp` `StealAfterObservedNonRenewalAdvancesLease`.
   B's redelete of T0 is `Gone`, and B's own job removes T0's marker. The latch is one-shot, so it does not park B.
5. W1 writes X (T1), then drops it. B runs rounds until T1 is condemned at `c1`.
6. Release the latch. Assert the meta is `Condemned(c1)`. A's `runRegularRound` throws on its `gc/state` CAS.
7. B graduates T1. W2's `ensureBlobPresent` for X returns an action other than `Observed`.
8. B's next round ends `Replaced` for T1. HEAD of X is present, and W2's part reads back.

**AC #3** (existing `CASGcMetaWriter*` green under ASan and TSan) runs the file as a whole.

## 9. Scope {#scope}

| File | Change | Lines |
|---|---|---|
| `Gc/CasGcMetaWriter.h` | signature + comment | ~4 |
| `Gc/CasGcMetaWriter.cpp` | the two predicates, the capture, comments | ~20 |
| `Gc/CasGc.cpp` | pass the round; re-read round check + comment | ~5 |
| `docs/en/antalya/cas/architecture/garbage-collection.md` | `#condemn-delete` edge and one sentence | ~4 |
| `src/Disks/tests/gtest_cas_gc_meta_writer.cpp`, `cas_test_helpers.h` | 7 tests, `MetaReadLatchBackend` | ~280 |
| `docs/superpowers/models/CaGcCondemnMarkerGate*` | model, 8 cfgs, runner rows, regenerated RESULTS | ~150 |

**Out of scope:**

- the manifest memo (CAS-29.12);
- the carried `marker_confirmed` (A0);
- any leadership fence;
- the writer's `reconcileMetaClean` (section 6);
- the `LoadedMeta` comment in `Pool/CasBlobMeta.h:13-20` (`LoadedMeta`). It says comparing encoded bytes would not give an
  etag's protection, but on S3 the etag is those bytes (F11). This is prose only, and the fix does not
  depend on it.

**Ordering:** A0 and the memo must not ship before this fix, as the CAS-30 notes already say. With this fix
in place, A0's premise "the marker leaves `Condemned` only after T is displaced or deleted" holds against
GC's own actors. It does not hold against section 6.

## 10. Risks and open questions for the owner {#questions}

1. **Section 6, the publisher clears its own incarnation's marker.**
   - It is model-confirmed under an over-approximation. One code premise is unverified.
   - It is base behaviour, and it breaks the premise A0 and the memo rely on.
   - Decide: a separate task with its own spec now (recommended, since the fix is on the writer path), or
     fold it into this change.
2. **CAS-30's acceptance criterion #2 and title name the etag variant.** Under (a) the job still reads
   after the decision and deletes by that etag. Its rule makes that safe. Proposed wording: "No call path
   deletes a blob meta unless the meta it read is `Condemned` at a round no newer than the condemnation the
   delete was scheduled for." Title: "Delete a blob meta only while it records the condemnation the delete
   was scheduled for."
3. **The rebuild-regression corner (P2).** After a lost or undecodable `gc/state`, a pre-rebuild GC
   process frozen through the rebuild could still delete a post-rebuild marker. Recommend accepting it:
   operator-run disaster recovery, strictly better than today. The alternative is (b) for the job, at the
   request cost of section 4.2.

## Limits {#limits}

- **Nothing in `src/` was built or run.** The fix and all seven tests are designs.
- **What was run:** the TLA+ variant in section 7.
- **Stall lengths** are derived from the steal condition, not measured.
- **S3a** relies on `reconcileMetaClean` being able to throw after the publication. The code allows it
  (`orThrow`, lines 365 and 369-389). It was not provoked.
- **Section 6's schedule** was reasoned from the code and confirmed only in the model.
- **P1 assumes no out-of-band `.meta` writes.** An operator or tool writing `.meta` objects directly is not
  covered.
- **Checked on:** S3 (content-derived etag) and the in-memory and GCS dialects (version-unique values).
  Other backends are not covered.
