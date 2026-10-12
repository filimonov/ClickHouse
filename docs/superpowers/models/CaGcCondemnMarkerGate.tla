--------------------------- MODULE CaGcCondemnMarkerGate ---------------------------
(* A retired (hash, token) entry must not delete a body a writer adopted. The per-hash     *)
(* meta is the writer's adopt gate: absent or Clean lets a writer adopt the CURRENT token   *)
(* without re-upload, while Condemned forces a fresh upload. One hash, one fixed content-   *)
(* addressed key, single-leader GC.                                                         *)
(*                                                                                          *)
(* THE HAZARD. A Condemned marker that guards an entry can disappear: a writer's Clean      *)
(* overwrites it, a lost marker write never creates it, or a delayed delete job removes a   *)
(* marker written by a LATER condemnation. A writer then adopts the token, and the entry    *)
(* later deletes the body under that writer's landed edge (NoDangle).                       *)
(*                                                                                          *)
(* THE FIX. The marker carries the condemn round (meta.cr).                                 *)
(*   - GMetaDeleteJob removes only a marker at round <= the round it captured               *)
(*     (JobRoundCheck).                                                                     *)
(*   - GSettleCarryRetry stamps the CURRENT round (RetryStampsCurrent), and a Condemned     *)
(*     marker of an older round is re-stamped (Restamp).                                    *)
(*   - Graduation always reads the marker (GateAlwaysReads) and accepts only Condemned at   *)
(*     round >= the entry's (GateRoundCheck, MetaGuards). Nothing in-process is trusted:    *)
(*     Registry is FALSE.                                                                   *)
(*                                                                                          *)
(* WHY IT HOLDS. A same-token adoption needs a read of a marker that is not Condemned, so   *)
(* the adopter's edge landed before the marker turned Condemned. Graduation reads the       *)
(* marker, and GSettleRedelete runs in a LATER round whose cut postdates that read: the cut *)
(* folds the edge and the entry is spared. The round check keeps a stale job or a stale     *)
(* marker from standing in for the guard, and the current-round retry keeps a carried       *)
(* entry's marker from lagging behind it.                                                   *)
(*                                                                                          *)
(* CAS-360. The adopter's body HEAD and marker GET are two requests (SplitAdopt). A body      *)
(* deleted between them, and its marker deleted before the GET, reads as "absent marker":   *)
(* the writer backfills Clean and adopts a deleted body. FixMode A/B/C are the candidates.  *)
(*                                                                                          *)
(* The cfgs switch each rule off one at a time and expect the invariant named in the        *)
(* runner. Losses the fix leaves open (a late Clean request that matches recreated bytes,   *)
(* an externally dropped edge) are the cfgs late_resolved_stale, late_gaveup and            *)
(* extdrop_noregistry, which are expected to FAIL.                                          *)
(*                                                                                          *)
(* Added:                                                                              *)
(*   RealCandidate      a fresh condemn needs a delta for the hash in this round's cut *)
(*                      (`closeBlob`: cur_edges == 0 && cur_touched); a carried row is *)
(*                      superseded, not settled, when touched at zero with a different *)
(*                      current token (peek_head is not read ahead: it reads `body`).  *)
(*   Respawn            a dropped writer slot starts a new build (a third writer).     *)
(*   AsyncMarker        the condemn-marker write is a job that lands after the HEAD,   *)
(*                      drained before the next cut (meta_pool_wait before commit).    *)
(*   GateAlwaysReads    G1: graduation ignores the in-process confirmation.            *)
(*   WriterLeavesMarker W1: a reconcile whose conditional write loses leaves a         *)
(*                      Condemned meta instead of overwriting it with Clean.           *)
(*   DeleteReads        G2: the redelete re-reads the meta; a refusal drops the entry. *)
(*   ExternalDrop       the edge may vanish while the reconcile is still owed          *)
(*                      (namespace removal cancels a build mid-upload).                *)
(*   Witness            records a gate refusal over a writer's Clean, then a delete.   *)
(*   GateTouched        G7 naive: the in-process confirmation does not count in a     *)
(*                      round whose cut touched the hash.                              *)
(*   Registry           the in-process confirmation is a per-process set of (token,    *)
(*                      round) pairs that outlives rounds, failed attempts and         *)
(*                      leadership changes.                                            *)
(*   RegRound           a pair confirms only at round >= entry's.                      *)
(*   StaleAttempts      an uncommitted attempt of this process HEADs the current token,*)
(*                      writes its marker and keeps the confirmation (GStaleAttempt).  *)
(*   ExactAbsentPath    publication only into a key with neither body nor meta.        *)
(*   ForgetOnTouch      G7: a cut that touches a carried condemned row drops its       *)
(*                      in-process confirmation for good (`closeBlob`, cur_touched).   *)
(*   SuppressRounds     a round may carry the row without graduating or deleting      *)
(*                      (`suppress_destructive`: a clamped namespace).                 *)
(*   RetryStampsCurrent the gate's retry write stamps the refusing round, not the      *)
(*                      entry's.                                                       *)
(*   OutstandingReconcile "none" | "resolved" | "gaveup": a reconcile may leave one    *)
(*                      Clean If-Match request in flight after its task ends.          *)
(*   EtagIsContent      a late request matches equal bytes (S3); FALSE: it matches     *)
(*                      only while no write replaced the version it read (GCS).        *)
(*   TrackRetry         records a completed retry write for NoWedgedCarry.             *)
(*   MarkerPrefetch     the gate decides on a marker GET issued earlier in the round   *)
(*                      (the graduation read-ahead), not on the marker at the gate.    *)
(*   SplitAdopt         the adopter's body `HEAD` and marker `GET` are separate steps  *)
(*                      (WHead, WMetaRead, WReHead, WBackfill) instead of WAdopt.      *)
(*   FixMode            "none" | "A" | "B" | "C": CAS-360 candidates for the gap       *)
(*                      between those two reads. A: the marker is deleted one round    *)
(*                      after the body, against that round's in-degree. B: an absent   *)
(*                      marker is followed by a second `HEAD` and adoption only of an  *)
(*                      unchanged token. C: an absent marker always republishes.       *)
(*   FixGuard           FALSE removes the candidate's decisive check (sabotage).       *)
(*   AKeepOnSpare       A: a spared body-gone entry is carried, not dropped, until a   *)
(*                      round at in-degree 0 deletes its marker.                       *)
(*   MetaDeleteFault    a marker delete may fail; a scheduled job is then lost.        *)
(*   GoneDeleteAnyRound A: the synchronous marker delete removes a Condemned marker of   *)
(*                      any round below the current one, not only <= the entry's round *)
(*                      (a gate retry stamps a later round than the entry's).          *)
(*   DeposedDelete      once per behaviour, a deposed attempt's pre-commit redelete of  *)
(*                      a published delete_pending entry removes the body at any time  *)
(*                      (and today schedules its marker delete job).                  *)
(*   AGoneDropsNow      A, sabotage of the two-pass rule: a body already Gone at the   *)
(*                      first redelete pass deletes the marker in that same round.     *)
EXTENDS Naturals

CONSTANTS
  GateOnMarker, PersistConf, Prefetch, TwoWriters, MaxGen, MaxRounds,
  SplitMetaDelete, JobRoundCheck, Restamp, GateRoundCheck, ReconcileFault, SplitPublish, StaleMarkers,
  RealCandidate, Respawn, AsyncMarker, GateAlwaysReads, WriterLeavesMarker, DeleteReads, ExternalDrop,
  Witness, GateTouched, ForgetOnTouch, SuppressRounds,
  Registry, RegRound, StaleAttempts, ExactAbsentPath,
  RetryStampsCurrent, OutstandingReconcile, EtagIsContent, TrackRetry,
  MarkerPrefetch,
  SplitAdopt, FixMode, FixGuard, AKeepOnSpare, MetaDeleteFault, GoneDeleteAnyRound,
  DeposedDelete, AGoneDropsNow

NoTok == 0
Writers == IF TwoWriters THEN {1, 2} ELSE {1}
NoRet == [st |-> "none", tok |-> NoTok, cr |-> 0, conf |-> FALSE, pconf |-> FALSE, retried |-> FALSE]
NoJob == [round |-> 0, retry |-> FALSE]
Absent == [st |-> "absent", cr |-> 0]
CleanMeta == [st |-> "clean", cr |-> 0]
MetaVals == [st : {"absent", "clean", "condemned"}, cr : 0..MaxRounds]
NoLate == [pending |-> FALSE, expect |-> Absent]

VARIABLES
  round, cut_taken, folded, body, meta, next_gen, seeded, edge, adopted, committed, ret,
  observed, observed_tok,
  mdq,         \* condemn rounds captured by outstanding meta-delete jobs
  recon,       \* per writer: published, reconcile to Clean still owed
  dirty,       \* an edge of the hash changed since the last cut
  touched,     \* this round's cut carries a delta for the hash
  premeta,     \* per writer: the meta its publication decided from (Absent on the absent-body path)
  mjob,        \* the queued condemn-marker job: the round it stamps, and whether it is a gate retry
  wit,         \* 0; 1 = gate refused over a writer's Clean; 2 = then a body delete happened
  reg,         \* in-process confirmations <<token, round>> held by the live leader's process
  sa_done,     \* the one stale attempt a behaviour may contain has happened
  late,        \* a publisher's Clean If-Match request still in flight: [pending, expect]
  late_moved,  \* the marker changed since that request read it
  mobs,        \* the graduation read-ahead's marker GET: taken this round, and the meta it returned
  wst,         \* per writer, SplitAdopt: "idle" | "headed" | "rehead" | "backfill" | "pub"
  hd,          \* per writer, SplitAdopt: the token its `HEAD` returned
  dd_done      \* the one deposed redelete a behaviour may contain has happened

vars == <<round, cut_taken, folded, body, meta, next_gen, seeded, edge, adopted, committed, ret,
          observed, observed_tok, mdq, recon, dirty, touched, premeta, mjob, wit, reg, sa_done, late, late_moved,
          mobs, wst, hd, dd_done>>

WKeep == UNCHANGED <<wst, hd, dd_done>>

TypeOK ==
  /\ round \in 1..MaxRounds
  /\ body \in [present : BOOLEAN, tok : 0..MaxGen]
  /\ meta \in MetaVals
  /\ mdq \subseteq 0..MaxRounds
  /\ recon \in [Writers -> BOOLEAN]
  /\ premeta \in [Writers -> MetaVals]
  /\ dirty \in BOOLEAN /\ touched \in BOOLEAN /\ mjob \in [round : 0..MaxRounds, retry : BOOLEAN]
  /\ wit \in 0..2
  /\ reg \subseteq (0..MaxGen) \X (0..MaxRounds)
  /\ late \in [pending : BOOLEAN, expect : MetaVals]
  /\ late_moved \in BOOLEAN
  /\ mobs \in [on : BOOLEAN, m : MetaVals]
  /\ wst \in [Writers -> {"idle", "headed", "rehead", "backfill", "pub"}]
  /\ hd \in [Writers -> 0..MaxGen]
  /\ dd_done \in BOOLEAN

Init ==
  /\ round = 1 /\ cut_taken = FALSE /\ folded = FALSE
  /\ body = [present |-> FALSE, tok |-> NoTok]
  /\ meta = Absent
  /\ next_gen = 1
  /\ seeded = FALSE
  /\ edge = [w \in Writers |-> "none"]
  /\ adopted = [w \in Writers |-> NoTok]
  /\ committed = [w \in Writers |-> FALSE]
  /\ ret = NoRet
  /\ observed = FALSE /\ observed_tok = NoTok
  /\ mdq = {}
  /\ recon = [w \in Writers |-> FALSE]
  /\ dirty = FALSE /\ touched = FALSE
  /\ premeta = [w \in Writers |-> Absent]
  /\ mjob = NoJob
  /\ wit = 0
  /\ reg = {}
  /\ sa_done = FALSE
  /\ late = NoLate /\ late_moved = FALSE
  /\ mobs = [on |-> FALSE, m |-> Absent]
  /\ wst = [w \in Writers |-> "idle"]
  /\ hd = [w \in Writers |-> NoTok]
  /\ dd_done = FALSE

Fresh == next_gen <= MaxGen
Cond == meta.st = "condemned"
RegAdd(t, r) == IF Registry THEN reg \cup {<<t, r>>} ELSE reg
RegForget(t) == IF Registry THEN {p \in reg : p[1] /= t} ELSE reg
Touch == dirty' = IF RealCandidate THEN TRUE ELSE dirty
RetryStamp == IF RetryStampsCurrent THEN round ELSE ret.cr
LateStart == OutstandingReconcile /= "none" /\ ~late.pending /\ Cond

(* The seeded body stands for a blob whose last owner edge dropped in this interval. *)
Seed ==
  /\ ~seeded /\ Fresh
  /\ body' = IF ExactAbsentPath THEN body ELSE [present |-> TRUE, tok |-> next_gen]
  /\ next_gen' = IF ExactAbsentPath THEN next_gen ELSE next_gen + 1
  /\ seeded' = TRUE
  /\ IF ExactAbsentPath THEN dirty' = dirty ELSE Touch
  /\ UNCHANGED <<round, cut_taken, folded, meta, edge, adopted, committed, ret, observed, observed_tok, mdq, recon,
                 touched, premeta, mjob, wit, reg, sa_done, mobs, mobs>>

WPrecommit(w) ==
  /\ seeded /\ edge[w] = "none"
  /\ edge' = [edge EXCEPT ![w] = "landed"]
  /\ Touch
  /\ UNCHANGED <<round, cut_taken, folded, body, meta, next_gen, seeded, adopted, committed, ret, observed, observed_tok,
                 mdq, recon, touched, premeta, mjob, wit, reg, sa_done, mobs, mobs>>

(* Absent meta is backfilled Clean (putMetaIfAbsent); either reads as not condemned. *)
WAdopt(w) ==
  /\ ~SplitAdopt
  /\ edge[w] = "landed" /\ adopted[w] = NoTok /\ ~recon[w]
  /\ body.present /\ ~Cond
  /\ adopted' = [adopted EXCEPT ![w] = body.tok]
  /\ meta' = IF meta.st = "absent" THEN CleanMeta ELSE meta
  /\ UNCHANGED <<round, cut_taken, folded, body, next_gen, seeded, edge, committed, ret, observed, observed_tok, mdq,
                 recon, dirty, touched, premeta, mjob, wit, reg, sa_done, mobs, mobs>>

(* SplitAdopt: `PartWriteTxn::ensureBlobPresent`, one loop iteration, one request per step.
   WHead is `op.head(key)`: present -> remember the token; absent -> publish. *)
WHead(w) ==
  /\ SplitAdopt
  /\ edge[w] = "landed" /\ adopted[w] = NoTok /\ ~recon[w] /\ wst[w] = "idle"
  /\ wst' = [wst EXCEPT ![w] = IF body.present THEN "headed" ELSE "pub"]
  /\ hd' = [hd EXCEPT ![w] = IF body.present THEN body.tok ELSE NoTok]
  /\ UNCHANGED <<dd_done, round, cut_taken, folded, body, meta, next_gen, seeded, edge, adopted, committed, ret, observed,
                 observed_tok, mdq, recon, dirty, touched, premeta, mjob, wit, reg, sa_done, mobs>>

(* `loadMeta`: Clean adopts the HEAD's token; Condemned publishes (resurrect); absent goes to
   the backfill today, to a second HEAD under B, to a publication under C. *)
AbsentNext == IF FixMode = "B" THEN "rehead"
              ELSE IF FixMode = "C" /\ FixGuard THEN "pub"
              ELSE "backfill"

WMetaRead(w) ==
  /\ SplitAdopt
  /\ edge[w] = "landed" /\ wst[w] = "headed"
  /\ IF meta.st = "clean"
       THEN /\ adopted' = [adopted EXCEPT ![w] = hd[w]]
            /\ wst' = [wst EXCEPT ![w] = "idle"]
       ELSE /\ adopted' = adopted
            /\ wst' = [wst EXCEPT ![w] = IF meta.st = "condemned" THEN "pub" ELSE AbsentNext]
  /\ UNCHANGED <<dd_done, round, cut_taken, folded, body, meta, next_gen, seeded, edge, committed, ret, observed,
                 observed_tok, mdq, recon, dirty, touched, premeta, mjob, wit, reg, sa_done, mobs, hd>>

(* B: the second `op.head(key)`. The same token goes on to the backfill; anything else starts
   the next loop iteration. Without FixGuard the result is ignored. *)
WReHead(w) ==
  /\ SplitAdopt /\ FixMode = "B"
  /\ edge[w] = "landed" /\ wst[w] = "rehead"
  /\ wst' = [wst EXCEPT ![w] = IF ~FixGuard \/ (body.present /\ body.tok = hd[w]) THEN "backfill" ELSE "idle"]
  /\ UNCHANGED <<dd_done, round, cut_taken, folded, body, meta, next_gen, seeded, edge, adopted, committed, ret, observed,
                 observed_tok, mdq, recon, dirty, touched, premeta, mjob, wit, reg, sa_done, mobs, hd>>

(* `putMetaIfAbsent(Clean)`, its outcome not read, then the `Observed` adoption of the HEAD's token. *)
WBackfill(w) ==
  /\ SplitAdopt
  /\ edge[w] = "landed" /\ wst[w] = "backfill"
  /\ meta' = IF meta.st = "absent" THEN CleanMeta ELSE meta
  /\ adopted' = [adopted EXCEPT ![w] = hd[w]]
  /\ wst' = [wst EXCEPT ![w] = "idle"]
  /\ UNCHANGED <<dd_done, round, cut_taken, folded, body, next_gen, seeded, edge, committed, ret, observed,
                 observed_tok, mdq, recon, dirty, touched, premeta, mjob, wit, reg, sa_done, mobs, hd>>

(* Publication of a fresh incarnation (resurrect over Condemned, or create over an absent
   body), followed by the reconcile to Clean. Atomic unless SplitPublish; the reconcile may
   throw (ReconcileFault): the body stays, the meta is untouched, the attempt fails.
   `premeta` is what the reconcile's first conditional write is conditioned on: absence on
   the absent-body path (`create`), the read meta's bytes on the resurrect path (`replace`). *)
WPublish(w) ==
  /\ edge[w] = "landed" /\ adopted[w] = NoTok /\ ~recon[w] /\ Fresh
  /\ IF SplitAdopt THEN wst[w] = "pub"
     ELSE IF ExactAbsentPath THEN ~body.present /\ meta.st = "absent" ELSE (~body.present \/ Cond)
  /\ wst' = IF SplitAdopt THEN [wst EXCEPT ![w] = "idle"] ELSE wst
  /\ hd' = hd
  /\ body' = [present |-> TRUE, tok |-> next_gen]
  /\ next_gen' = next_gen + 1
  /\ premeta' = IF WriterLeavesMarker
                  THEN [premeta EXCEPT ![w] = IF body.present THEN meta ELSE Absent]
                  ELSE premeta
  /\ \/ /\ SplitPublish
        /\ adopted' = [adopted EXCEPT ![w] = next_gen]
        /\ recon' = [recon EXCEPT ![w] = TRUE]
        /\ meta' = meta
     \/ /\ ~SplitPublish
        /\ adopted' = [adopted EXCEPT ![w] = next_gen]
        /\ meta' = CleanMeta
        /\ recon' = recon
     \/ /\ ReconcileFault
        /\ adopted' = adopted
        /\ meta' = meta
        /\ recon' = recon
  /\ UNCHANGED <<dd_done, round, cut_taken, folded, seeded, edge, committed, ret, observed, observed_tok, mdq, dirty, touched,
                 mjob, wit, reg, sa_done, mobs, mobs>>

(* `reconcileMetaClean`: the first conditional write wins only if the meta is still what
   the publication decided from; on S3 the etag is the bytes, so equal bytes win (ABA).
   A lost first write falls into `readModifyWrite`, which today overwrites any non-Clean
   meta; W1 leaves a Condemned one. *)
FirstWriteWins(w) == IF premeta[w].st = "absent" THEN meta.st = "absent" ELSE meta = premeta[w]
ReconcileResult(w) ==
  IF ~WriterLeavesMarker \/ FirstWriteWins(w) THEN CleanMeta
  ELSE IF Cond THEN meta
  ELSE CleanMeta

WReconcile(w) ==
  /\ recon[w]
  /\ recon' = [recon EXCEPT ![w] = FALSE]
  /\ \/ /\ meta' = ReconcileResult(w)
        /\ adopted' = adopted
        /\ late' = late
     \/ /\ OutstandingReconcile = "resolved" /\ LateStart
        /\ meta' = ReconcileResult(w)
        /\ adopted' = adopted
        /\ late' = [pending |-> TRUE, expect |-> meta]
     \/ /\ ReconcileFault
        /\ meta' = meta
        /\ adopted' = [adopted EXCEPT ![w] = NoTok]
        /\ late' = late
     \/ /\ OutstandingReconcile = "gaveup" /\ ReconcileFault /\ LateStart
        /\ meta' = meta
        /\ adopted' = [adopted EXCEPT ![w] = NoTok]
        /\ late' = [pending |-> TRUE, expect |-> meta]
  /\ UNCHANGED <<round, cut_taken, folded, body, next_gen, seeded, edge, committed, ret, observed, observed_tok, mdq,
                 dirty, touched, premeta, mjob, wit, reg, sa_done, mobs, mobs>>

(* The request a reconcile left in flight lands, or dies. It writes Clean only if its precondition still
   matches: equal bytes under EtagIsContent, otherwise an unreplaced version. *)
WLateLand ==
  /\ late.pending
  /\ IF EtagIsContent THEN meta = late.expect ELSE ~late_moved
  /\ meta' = CleanMeta
  /\ late' = NoLate
  /\ UNCHANGED <<round, cut_taken, folded, body, next_gen, seeded, edge, adopted, committed, ret, observed,
                 observed_tok, mdq, recon, dirty, touched, premeta, mjob, wit, reg, sa_done, mobs, mobs>>

WLateDie ==
  /\ late.pending
  /\ late' = NoLate
  /\ UNCHANGED <<round, cut_taken, folded, body, meta, next_gen, seeded, edge, adopted, committed, ret, observed,
                 observed_tok, mdq, recon, dirty, touched, premeta, mjob, wit, reg, sa_done, mobs, mobs>>

WCommit(w) ==
  /\ edge[w] = "landed" /\ adopted[w] /= NoTok /\ ~committed[w] /\ ~recon[w]
  /\ committed' = [committed EXCEPT ![w] = TRUE]
  /\ UNCHANGED <<round, cut_taken, folded, body, meta, next_gen, seeded, edge, adopted, ret, observed, observed_tok,
                 mdq, recon, dirty, touched, premeta, mjob, wit, reg, sa_done, mobs, mobs>>

(* The writer's own abort follows its reconcile (the upload fan-out waits for every task
   before unwinding); ExternalDrop is a namespace removal that lands while it is owed. *)
WDrop(w) ==
  /\ TwoWriters /\ edge[w] = "landed" /\ (ExternalDrop \/ ~recon[w])
  /\ edge' = [edge EXCEPT ![w] = "dropped"]
  /\ committed' = [committed EXCEPT ![w] = FALSE]
  /\ Touch
  /\ UNCHANGED <<round, cut_taken, folded, body, meta, next_gen, seeded, adopted, ret, observed, observed_tok, mdq,
                 recon, touched, premeta, mjob, wit, reg, sa_done, mobs, mobs>>

WRespawn(w) ==
  /\ Respawn /\ edge[w] = "dropped" /\ ~recon[w]
  /\ edge' = [edge EXCEPT ![w] = "none"]
  /\ adopted' = [adopted EXCEPT ![w] = NoTok]
  /\ wst' = [wst EXCEPT ![w] = "idle"]
  /\ hd' = [hd EXCEPT ![w] = NoTok]
  /\ UNCHANGED <<dd_done, round, cut_taken, folded, body, meta, next_gen, seeded, committed, ret, observed, observed_tok, mdq,
                 recon, dirty, touched, premeta, mjob, wit, reg, sa_done, mobs, mobs>>

GCut ==
  /\ ~cut_taken /\ round < MaxRounds
  /\ mjob = NoJob
  /\ folded' = (\E w \in Writers : edge[w] = "landed")
  /\ cut_taken' = TRUE
  /\ touched' = IF RealCandidate THEN dirty ELSE touched
  /\ dirty' = IF RealCandidate THEN FALSE ELSE dirty
  /\ ret' = IF ForgetOnTouch /\ dirty /\ ret.st = "condemned" THEN [ret EXCEPT !.conf = FALSE, !.pconf = FALSE] ELSE ret
  /\ reg' = IF ForgetOnTouch /\ dirty /\ ret.st = "condemned" THEN RegForget(ret.tok) ELSE reg
  /\ UNCHANGED <<round, body, meta, next_gen, seeded, edge, adopted, committed, observed, observed_tok, mdq, recon,
                 premeta, mjob, wit, sa_done, mobs, mobs>>

EndRound == /\ cut_taken' = FALSE
            /\ round' = round + 1
            /\ observed' = FALSE /\ observed_tok' = NoTok
            /\ mobs' = [on |-> FALSE, m |-> Absent]

GObserve ==
  /\ Prefetch /\ cut_taken /\ ~observed
  /\ observed' = TRUE
  /\ observed_tok' = IF body.present THEN body.tok ELSE NoTok
  /\ UNCHANGED <<round, cut_taken, folded, body, meta, next_gen, seeded, edge, adopted, committed, ret, mdq, recon,
                 dirty, touched, premeta, mjob, wit, reg, sa_done, mobs, mobs>>

(* The graduation read-ahead: the marker GET is issued after the cut and before the gate, and
   the gate decides on what it returned. Anything that lands on the marker in between is unseen. *)
GObserveMeta ==
  /\ MarkerPrefetch /\ cut_taken /\ ~mobs.on
  /\ mobs' = [on |-> TRUE, m |-> meta]
  /\ UNCHANGED <<round, cut_taken, folded, body, meta, next_gen, seeded, edge, adopted, committed, ret, observed,
                 observed_tok, mdq, recon, dirty, touched, premeta, mjob, wit, reg, sa_done>>

SeenMeta == IF MarkerPrefetch /\ mobs.on THEN mobs.m ELSE meta

SeenPresent == IF Prefetch /\ observed THEN observed_tok /= NoTok ELSE body.present
SeenTok     == IF Prefetch /\ observed THEN observed_tok ELSE body.tok

InProc == IF Registry THEN \E p \in reg : p[1] = ret.tok /\ (~RegRound \/ p[2] >= ret.cr) ELSE ret.conf
Confirmed == InProc \/ (PersistConf /\ ret.pconf)

(* writeCondemnedMeta: create or Clean -> Condemned(r); an older-round Condemned is
   re-stamped only with Restamp; otherwise an existing Condemned is left alone. *)
CondemnedAfterWrite(r) ==
  IF ~Cond THEN [st |-> "condemned", cr |-> r]
  ELSE IF Restamp /\ meta.cr < r THEN [st |-> "condemned", cr |-> r]
  ELSE meta

(* The graduation re-read: a Condemned meta guards this entry. *)
MetaGuards == Cond /\ (~GateRoundCheck \/ meta.cr >= ret.cr)

(* The same test on the value the gate holds: the marker itself, or the read-ahead's copy of it. *)
GateMetaGuards == SeenMeta.st = "condemned" /\ (~GateRoundCheck \/ SeenMeta.cr >= ret.cr)

(* G1 drops the in-process shortcut; the re-read decides alone. G7 drops it only in a
   touched round. *)
EffConfirmed == Confirmed /\ ~(GateTouched /\ touched)
GateOK == ~GateOnMarker \/ (IF GateAlwaysReads THEN GateMetaGuards ELSE EffConfirmed \/ GateMetaGuards)

(* `closeBlob`'s supersede branch: a carried row, touched at zero, whose current token
   differs. It pre-empts every other settlement of the row. *)
SupersedeDue ==
  /\ RealCandidate
  /\ ret.st \in {"condemned", "pending", "gone"} /\ touched /\ ~folded
  /\ body.present /\ body.tok /= ret.tok

(* A condemnation or supersede of token t at round r: the marker write either lands in the
   same step or, with AsyncMarker, is queued; a lost write confirms nothing. *)
Condemn(t, r) ==
  IF AsyncMarker
    THEN /\ meta' = meta
         /\ ret' = [st |-> "condemned", tok |-> t, cr |-> r, conf |-> FALSE, pconf |-> FALSE, retried |-> FALSE]
         /\ mjob' = [round |-> r, retry |-> FALSE]
         /\ reg' = RegForget(ret.tok)
    ELSE /\ mjob' = mjob
         /\ \/ /\ meta' = CondemnedAfterWrite(r)
               /\ ret' = [st |-> "condemned", tok |-> t, cr |-> r, conf |-> TRUE, pconf |-> PersistConf, retried |-> FALSE]
               /\ reg' = IF Registry THEN {p \in reg : p[1] /= ret.tok} \cup {<<t, r>>} ELSE reg
            \/ /\ meta' = meta
               /\ ret' = [st |-> "condemned", tok |-> t, cr |-> r, conf |-> FALSE, pconf |-> FALSE, retried |-> FALSE]
               /\ reg' = RegForget(ret.tok)

GMarkerJob ==
  /\ mjob /= NoJob
  /\ mjob' = NoJob
  /\ \/ /\ meta' = CondemnedAfterWrite(mjob.round)
        /\ ret' = [ret EXCEPT !.conf = TRUE, !.pconf = PersistConf, !.retried = ret.retried \/ mjob.retry]
        /\ reg' = RegAdd(ret.tok, ret.cr)
     \/ /\ meta' = meta
        /\ ret' = ret
        /\ reg' = reg
  /\ UNCHANGED <<round, cut_taken, folded, body, next_gen, seeded, edge, adopted, committed, observed, observed_tok,
                 mdq, recon, dirty, touched, premeta, wit, sa_done, mobs, mobs>>

GSettleCondemn ==
  /\ cut_taken /\ ret.st = "none" /\ SeenPresent /\ ~folded
  /\ (~RealCandidate \/ touched)
  /\ Condemn(SeenTok, round)
  /\ EndRound
  /\ UNCHANGED <<folded, body, next_gen, seeded, edge, adopted, committed, mdq, recon, dirty, touched, premeta, wit, sa_done>>

GSettleIdle ==
  /\ cut_taken /\ ret.st = "none" /\ (~SeenPresent \/ folded \/ (RealCandidate /\ ~touched))
  /\ EndRound
  /\ UNCHANGED <<folded, body, meta, next_gen, seeded, edge, adopted, committed, ret, mdq, recon, dirty, touched,
                 premeta, mjob, wit, reg, sa_done>>

GSettleSpare ==
  /\ cut_taken /\ ret.st = "condemned" /\ folded
  /\ ret' = NoRet
  /\ reg' = RegForget(ret.tok)
  /\ EndRound
  /\ UNCHANGED <<folded, body, meta, next_gen, seeded, edge, adopted, committed, mdq, recon, dirty, touched, premeta,
                 mjob, wit, sa_done>>

GSettleGraduate ==
  /\ cut_taken /\ ret.st = "condemned" /\ ~folded /\ ret.cr < round /\ ~SupersedeDue
  /\ GateOK
  /\ ret' = [ret EXCEPT !.st = "pending", !.conf = TRUE, !.pconf = PersistConf]
  /\ reg' = IF ~(~GateAlwaysReads /\ EffConfirmed) /\ GateMetaGuards THEN RegAdd(ret.tok, SeenMeta.cr) ELSE reg
  /\ EndRound
  /\ UNCHANGED <<folded, body, meta, next_gen, seeded, edge, adopted, committed, mdq, recon, dirty, touched, premeta,
                 mjob, wit, sa_done>>

GSettleGraduateDeferred ==
  /\ PersistConf
  /\ cut_taken /\ ret.st = "condemned" /\ ~folded /\ ret.cr < round /\ ~SupersedeDue
  /\ GateOK
  /\ ret' = [ret EXCEPT !.conf = TRUE, !.pconf = TRUE]
  /\ reg' = IF ~(~GateAlwaysReads /\ EffConfirmed) /\ GateMetaGuards THEN RegAdd(ret.tok, SeenMeta.cr) ELSE reg
  /\ EndRound
  /\ UNCHANGED <<folded, body, meta, next_gen, seeded, edge, adopted, committed, mdq, recon, dirty, touched, premeta,
                 mjob, wit, sa_done>>

GSettleSupersede ==
  /\ cut_taken /\ ~folded
  /\ IF RealCandidate
       THEN SupersedeDue
       ELSE PersistConf /\ ret.st = "condemned" /\ SeenPresent /\ SeenTok /= ret.tok
  /\ Condemn(IF RealCandidate THEN body.tok ELSE SeenTok, round)
  /\ EndRound
  /\ UNCHANGED <<folded, body, next_gen, seeded, edge, adopted, committed, mdq, recon, dirty, touched, premeta, wit, sa_done>>

GSettleCarryRetry ==
  /\ cut_taken /\ ret.st = "condemned" /\ ~folded /\ ret.cr < round /\ ~SupersedeDue
  /\ ~GateOK
  /\ wit' = IF Witness /\ wit = 0 /\ meta.st = "clean" THEN 1 ELSE wit
  /\ IF AsyncMarker
       THEN /\ meta' = meta /\ ret' = ret /\ mjob' = [round |-> RetryStamp, retry |-> TrackRetry] /\ reg' = reg
       ELSE /\ mjob' = mjob
            /\ \/ /\ meta' = CondemnedAfterWrite(RetryStamp)
                  /\ ret' = [ret EXCEPT !.conf = TRUE, !.pconf = PersistConf, !.retried = ret.retried \/ TrackRetry]
                  /\ reg' = RegAdd(ret.tok, ret.cr)
               \/ /\ meta' = meta
                  /\ ret' = ret
                  /\ reg' = reg
  /\ EndRound
  /\ UNCHANGED <<folded, body, next_gen, seeded, edge, adopted, committed, mdq, recon, dirty, touched, premeta, sa_done>>

GSettleSuppressed ==
  /\ SuppressRounds
  /\ cut_taken /\ ret.st \in {"condemned", "pending", "gone"} /\ ~folded /\ ~SupersedeDue
  /\ EndRound
  /\ UNCHANGED <<folded, body, meta, next_gen, seeded, edge, adopted, committed, ret, mdq, recon, dirty, touched, premeta,
                 mjob, wit, reg, sa_done>>

GSettleSpareLoud ==
  /\ cut_taken /\ ret.st = "pending" /\ folded
  /\ ret' = NoRet
  /\ reg' = RegForget(ret.tok)
  /\ EndRound
  /\ UNCHANGED <<folded, body, meta, next_gen, seeded, edge, adopted, committed, mdq, recon, dirty, touched, premeta,
                 mjob, wit, sa_done>>

(* Removed or Gone schedules the meta delete; Mismatch schedules nothing. With DeleteReads
   a meta that no longer guards the entry refuses the delete and drops the entry: the run
   that would carry it is already written, so a refusal leaks the body rather than risk it. *)
DeleteRefused == DeleteReads /\ ~MetaGuards

GSettleRedelete ==
  /\ cut_taken /\ ret.st = "pending" /\ ~folded /\ ~SupersedeDue
  /\ IF ~DeleteRefused /\ body.present /\ body.tok = ret.tok
       THEN body' = [present |-> FALSE, tok |-> NoTok]
       ELSE body' = body
  /\ wit' = IF Witness /\ wit = 1 /\ ~DeleteRefused /\ body.present /\ body.tok = ret.tok THEN 2 ELSE wit
  /\ IF FixMode = "A" /\ AGoneDropsNow /\ ~body.present
       THEN /\ meta' = IF Cond /\ meta.cr <= (IF GoneDeleteAnyRound THEN round - 1 ELSE ret.cr) THEN Absent ELSE meta
            /\ mdq' = mdq
     ELSE IF DeleteRefused \/ FixMode = "A"
       THEN /\ meta' = meta /\ mdq' = mdq
       ELSE IF SplitMetaDelete
         THEN /\ meta' = meta
              /\ mdq' = IF body.present /\ body.tok /= ret.tok THEN mdq ELSE mdq \cup {ret.cr}
         ELSE /\ meta' = IF body.present /\ body.tok = ret.tok THEN Absent ELSE meta
              /\ mdq' = mdq
  \* A: Removed or Gone keeps the entry as "gone" (still `delete_pending`, marker owed); Mismatch drops it.
  /\ ret' = IF FixMode = "A" /\ ~DeleteRefused /\ ~(body.present /\ body.tok /= ret.tok)
                /\ ~(AGoneDropsNow /\ ~body.present)
             THEN [ret EXCEPT !.st = "gone"]
             ELSE NoRet
  /\ reg' = RegForget(ret.tok)
  /\ EndRound
  /\ UNCHANGED <<folded, next_gen, seeded, edge, adopted, committed, recon, dirty, touched, premeta, mjob, sa_done>>

(* A, the round after the body delete: `settleEntry` on a body-gone entry. In-degree > 0 spares
   it and leaves the marker Condemned (dropped, or carried under AKeepOnSpare). *)
GSettleGoneSpare ==
  /\ FixMode = "A" /\ FixGuard
  /\ cut_taken /\ ret.st = "gone" /\ folded
  /\ ret' = IF AKeepOnSpare THEN ret ELSE NoRet
  /\ EndRound
  /\ UNCHANGED <<folded, body, meta, next_gen, seeded, edge, adopted, committed, mdq, recon, dirty, touched, premeta,
                 mjob, wit, reg, sa_done>>

(* A, in-degree 0: the synchronous `deleteConfirmedMeta` (GET + DELETE If-Match of a Condemned
   marker at round <= the entry's, one step as in GMetaDeleteJob), then the entry is dropped. A
   failed delete keeps the entry for the next round. The preceding `HEAD` (expect gone) is not a
   separate step: with unique tokens a body-gone entry's token never reappears. Without FixGuard
   the in-degree is ignored. *)
GSettleGoneDelete ==
  /\ FixMode = "A"
  /\ cut_taken /\ ret.st = "gone" /\ (~folded \/ ~FixGuard) /\ ~SupersedeDue
  /\ \/ /\ meta' = IF Cond /\ meta.cr <= (IF GoneDeleteAnyRound THEN round - 1 ELSE ret.cr) THEN Absent ELSE meta
        /\ ret' = NoRet
     \/ /\ MetaDeleteFault
        /\ meta' = meta
        /\ ret' = ret
  /\ EndRound
  /\ UNCHANGED <<folded, body, next_gen, seeded, edge, adopted, committed, mdq, recon, dirty, touched, premeta,
                 mjob, wit, reg, sa_done>>

(* The delayed meta delete. Unfenced: enabled in any state, so it also stands for a
   deposed leader's job. The read and the conditional delete act on the token the HEAD
   returned (the content etag on S3, the generation on GCS; the latter is stricter), so
   modelling them as one step loses no interleaving that changes the outcome. *)
GMetaDeleteJob(c) ==
  /\ c \in mdq
  /\ mdq' = mdq \ {c}
  /\ \/ meta' = IF ~JobRoundCheck THEN Absent
                ELSE IF Cond /\ meta.cr <= c THEN Absent ELSE meta
     \/ MetaDeleteFault /\ meta' = meta   \* the job fails; `GcMetaWriter` only logs it
  /\ UNCHANGED <<round, cut_taken, folded, body, next_gen, seeded, edge, adopted, committed, ret, observed, observed_tok,
                 recon, dirty, touched, premeta, mjob, wit, reg, sa_done, mobs, mobs>>

(* A deposed attempt of an earlier round, before its losing gc/state CAS, runs the redelete of an
   entry the last committed pass published (`Gc::performRedeleteIo`): exact-token, so only the
   entry's token. Today it then schedules the marker delete; under A it does not. *)
GDeposedRedelete ==
  /\ DeposedDelete /\ ~dd_done
  /\ ret.st = "pending" /\ body.present /\ body.tok = ret.tok
  /\ dd_done' = TRUE
  /\ body' = [present |-> FALSE, tok |-> NoTok]
  /\ mdq' = IF FixMode = "A" THEN mdq ELSE mdq \cup {ret.cr}
  /\ UNCHANGED <<round, cut_taken, folded, meta, next_gen, seeded, edge, adopted, committed, ret, observed,
                 observed_tok, recon, dirty, touched, premeta, mjob, wit, reg, sa_done, mobs, wst, hd>>

(* A deposed leader's condemn-marker write at its own (possibly old) round, or a marker an
   aborted attempt of the current round left behind. Confirms nothing the live leader reads. *)
GStaleMarkerWrite(r) ==
  /\ StaleMarkers
  /\ meta' = CondemnedAfterWrite(r)
  /\ UNCHANGED <<round, cut_taken, folded, body, next_gen, seeded, edge, adopted, committed, ret, observed, observed_tok,
                 mdq, recon, dirty, touched, premeta, mjob, wit, reg, sa_done, mobs, mobs>>

(* A restart, or leadership passing to a process whose registry holds nothing for this
   hash. With Registry a process that regains leadership keeps its registry, so
   confirmations from its own earlier attempts survive (GStaleAttempt adds them). *)
GRestart ==
  /\ mjob = NoJob
  /\ IF Registry
       THEN /\ reg /= {} /\ reg' = {} /\ ret' = ret
       ELSE /\ ret.st = "condemned" /\ ret.conf /\ ret' = [ret EXCEPT !.conf = FALSE] /\ reg' = reg
  /\ UNCHANGED <<round, cut_taken, folded, body, meta, next_gen, seeded, edge, adopted, committed, observed, observed_tok,
                 mdq, recon, dirty, touched, premeta, mjob, wit, sa_done, mobs, mobs>>

(* An uncommitted attempt of this process at round r (stalled, then losing its gc/state
   CAS): its HEAD sees the current token, its marker job lands and its confirmation stays
   in the registry, which the process keeps if it later leads again. *)
GStaleAttempt(r) ==
  /\ StaleAttempts /\ body.present /\ ~sa_done
  /\ sa_done' = TRUE
  /\ meta' = CondemnedAfterWrite(r)
  /\ reg' = RegAdd(body.tok, r)
  /\ UNCHANGED <<round, cut_taken, folded, body, next_gen, seeded, edge, adopted, committed, ret, observed, observed_tok,
                 mdq, recon, dirty, touched, premeta, mjob, wit, mobs, mobs>>

(* Every step that changes the marker moves the version a late request read. *)
LateMovedTrack ==
  late_moved' = IF ~late'.pending THEN FALSE
                ELSE IF ~late.pending THEN meta' /= late'.expect
                ELSE late_moved \/ meta' /= meta

(* Each disjunct is its own action so TLC names the step in a counterexample. *)
LateStill == late' = late /\ LateMovedTrack

Next ==
  \/ Seed /\ LateStill /\ WKeep
  \/ \E w \in Writers : WPrecommit(w) /\ LateStill /\ WKeep
  \/ \E w \in Writers : WAdopt(w) /\ LateStill /\ WKeep
  \/ \E w \in Writers : WHead(w) /\ LateStill
  \/ \E w \in Writers : WMetaRead(w) /\ LateStill
  \/ \E w \in Writers : WReHead(w) /\ LateStill
  \/ \E w \in Writers : WBackfill(w) /\ LateStill
  \/ \E w \in Writers : WPublish(w) /\ LateStill
  \/ \E w \in Writers : WCommit(w) /\ LateStill /\ WKeep
  \/ \E w \in Writers : WDrop(w) /\ LateStill /\ WKeep
  \/ \E w \in Writers : WRespawn(w) /\ LateStill
  \/ GCut /\ LateStill /\ WKeep
  \/ GObserve /\ LateStill /\ WKeep
  \/ GObserveMeta /\ LateStill /\ WKeep
  \/ GMarkerJob /\ LateStill /\ WKeep
  \/ GSettleCondemn /\ LateStill /\ WKeep
  \/ GSettleIdle /\ LateStill /\ WKeep
  \/ GSettleSpare /\ LateStill /\ WKeep
  \/ GSettleGraduate /\ LateStill /\ WKeep
  \/ GSettleGraduateDeferred /\ LateStill /\ WKeep
  \/ GSettleSupersede /\ LateStill /\ WKeep
  \/ GSettleCarryRetry /\ LateStill /\ WKeep
  \/ GSettleSpareLoud /\ LateStill /\ WKeep
  \/ GSettleRedelete /\ LateStill /\ WKeep
  \/ GSettleGoneSpare /\ LateStill /\ WKeep
  \/ GSettleGoneDelete /\ LateStill /\ WKeep
  \/ GSettleSuppressed /\ LateStill /\ WKeep
  \/ \E c \in mdq : GMetaDeleteJob(c) /\ LateStill /\ WKeep
  \/ \E r \in 1..round : GStaleMarkerWrite(r) /\ LateStill /\ WKeep
  \/ GRestart /\ LateStill /\ WKeep
  \/ GDeposedRedelete /\ LateStill
  \/ \E r \in 1..round : GStaleAttempt(r) /\ LateStill /\ WKeep
  \/ \E w \in Writers : WReconcile(w) /\ LateMovedTrack /\ WKeep
  \/ WLateLand /\ LateMovedTrack /\ WKeep
  \/ WLateDie /\ LateMovedTrack /\ WKeep

Spec == Init /\ [][Next]_vars

NoDangle == \A w \in Writers : committed[w] => body.present

(* Marker preservation: while a delete_pending entry's token is still the body, the key is
   Condemned; likewise for a condemned entry the gate would accept on its in-process record. *)
TokCurrent == body.present /\ body.tok = ret.tok
MarkerPreservedPending == (ret.st = "pending" /\ TokCurrent) => Cond
GateConfirmed == ~GateAlwaysReads /\ EffConfirmed
MarkerPreservedConfirmed == (ret.st = "condemned" /\ GateConfirmed /\ TokCurrent) => Cond

(* No queued job can remove the guard of a pending entry whose token is current. *)
JobWouldDelete(c) == ~JobRoundCheck \/ (Cond /\ meta.cr <= c)
JobSparesPendingGuard == \A c \in mdq : ~(ret.st = "pending" /\ TokCurrent /\ JobWouldDelete(c))

(* After a retry write that completed, the marker is not an older round than the entry's. *)
NoWedgedCarry == ~(ret.st = "condemned" /\ ret.retried /\ Cond /\ meta.cr < ret.cr)

(* Reachability witness, checked as an invariant that is expected to FAIL: a garbage blob
   whose marker a writer cleared is refused at the gate and still deleted later. *)
WitNoDeleteAfterRefusal == wit /= 2

(* Orphan-marker leak proxy: a Condemned marker over an absent body, with no writer holding a
   landed edge and no delete job queued, is still owed by a retired entry. *)
OrphanTracked ==
  (Cond /\ ~body.present /\ mdq = {} /\ \A w \in Writers : edge[w] /= "landed") => ret.st /= "none"

=====================================================================================
