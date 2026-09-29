--------------------------- MODULE CaGcCondemnMarkerGate ---------------------------
(* Gate for triage 2026-07-17 §3.4 (codex review №4): the swallowed condemn-marker    *)
(* write vs same-token adopt. This focused GC gate models one fixed content-addressed *)
(* key, so every present body has that key's logical content; incarnation tokens      *)
(* authorize exact deletion but are not part of a committed reference's identity.     *)
(*                                                                                    *)
(* THE HAZARD (pre-fix). `Gc::scheduleMetaJob` swallows every exception from          *)
(* `writeCondemnedMeta`, while the round commits the retired (hash, token) entry      *)
(* regardless. The per-hash meta is the WRITER's adopt gate (absent/Clean => the      *)
(* writer may adopt the CURRENT token without re-upload), so a lost marker lets a     *)
(* writer adopt the exact token a graduated entry later deleteExact-s. EDGE-BEFORE-   *)
(* OBSERVE does not close it: it orders the writer's edge before the writer's OWN     *)
(* observation, not before GC's once-per-fold discovery cut — a writer landing in     *)
(* the [cut, deleteExact] window of the deleting round is invisible to that round's   *)
(* merge. Result: exact-token delete of a body under a live committed edge — a        *)
(* dangling manifest (INV_NO_LOSS).                                                   *)
(*                                                                                    *)
(* THE FIX (verifier option (a), GateOnMarker = TRUE). Graduation to delete_pending   *)
(* — the one edge that authorizes an irreversible delete — requires CONFIRMED         *)
(* durable Condemned evidence: the in-process confirmation recorded when the          *)
(* scheduled marker write succeeded (ret.conf), OR a synchronous loadMeta re-check    *)
(* observing meta = condemned. No evidence => the entry is CARRIED (fail-safe         *)
(* delay) and the marker write is RETRIED (liveness). Everything else stays           *)
(* async/advisory. The disaster-recovery rebuild publishes markers synchronously —    *)
(* in this model that is simply the condemn action's marker-ok branch.                *)
(*                                                                                    *)
(* WHY THE FIX HOLDS (the argument the model checks): a same-token adoption           *)
(* requires observing meta /= condemned, so the adopting writer's edge (durable       *)
(* BEFORE its observation) landed before the meta turned condemned; graduation        *)
(* confirms only at/after that write; and the redelete runs in a LATER round whose    *)
(* cut postdates the confirming round — that cut folds the edge and the entry is      *)
(* SPARED, never deleted.                                                             *)
(*                                                                                    *)
(* SHAPE. One hash, one writer build, single-leader GC. A GC round is TWO actions:    *)
(* GCut (the discovery LIST — folds the edge visibility snapshot) and GSettle (the    *)
(* merge settlement + pre-CAS delete + this round's marker writes, atomic). Writer    *)
(* actions interleave freely between them — the [cut, settle] window IS the C1        *)
(* window that makes the pre-fix dangle reachable. GRestart models a leader change:   *)
(* the in-process confirmation is lost, forcing graduation onto the loadMeta          *)
(* re-check branch. `meta` is {"clean", "condemned"} with absence == clean (the       *)
(* current production semantics: deleteConfirmedMeta drops the meta; no tombstone).   *)
(*                                                                                    *)
(* PersistConf: the confirmation is also persisted in the run row (ret.pconf); it     *)
(* survives GRestart, a superseding row starts without it, and it is a third          *)
(* graduation evidence. GSettleGraduateDeferred is the budget-exhausted carry.        *)
(* Prefetch: GObserve records the body's token (0 = absent) between GCut and the      *)
(* settle, as the HEAD read-ahead does; condemn and supersede use that observation.   *)
(* TwoWriters: a second writer, and a writer may drop its edge (WDrop), so a hash     *)
(* returns to in-degree 0 and can be condemned again after a replacement.             *)
(* All three default FALSE, which leaves the original state graph unchanged.          *)
EXTENDS Naturals

CONSTANTS
  GateOnMarker,   \* FALSE = pre-fix graduation (unconditional); TRUE = the fixed gate
  PersistConf,    \* TRUE = the confirmation is persisted in the run row and survives GRestart
  Prefetch,       \* TRUE = the settle uses a token observed earlier by GObserve
  TwoWriters,     \* TRUE = a second writer, and writers may drop their edge
  MaxGen,         \* bound on fresh incarnation tokens
  MaxRounds       \* bound on GC rounds

NoTok == 0

Writers == IF TwoWriters THEN {1, 2} ELSE {1}

NoRet == [st |-> "none", tok |-> NoTok, cr |-> 0, conf |-> FALSE, pconf |-> FALSE]

VARIABLES
  round,       \* GC round counter (bounds the run)
  cut_taken,   \* between GCut and GSettle of the current round
  folded,      \* the cut's edge-visibility snapshot: was the writer's edge folded at this round's cut
  body,        \* [present |-> BOOLEAN, tok |-> 0..MaxGen] — the one blob body (exact-token deletes)
  meta,        \* "clean" | "condemned" — the per-hash writer adopt gate (absence == clean)
  next_gen,    \* fresh incarnation-token allocator
  seeded,      \* the pre-existing unowned incarnation was created (once)
  edge,        \* per writer: "none" | "landed" | "dropped" — its durable edge (EDGE-BEFORE-OBSERVE)
  adopted,     \* per writer: the token it acquired (0 = none yet)
  committed,   \* per writer: promote succeeded — a LIVE committed reference stands over `adopted`
  ret,         \* the retired entry: [st |-> "none"|"condemned"|"pending", tok, cr, conf, pconf]
  observed,    \* GObserve ran in this round
  observed_tok \* the token GObserve saw (0 = body absent)

vars == <<round, cut_taken, folded, body, meta, next_gen, seeded, edge, adopted, committed, ret,
          observed, observed_tok>>

TypeOK ==
  /\ round \in 1..MaxRounds
  /\ cut_taken \in BOOLEAN /\ folded \in BOOLEAN
  /\ body \in [present : BOOLEAN, tok : 0..MaxGen]
  /\ meta \in {"clean", "condemned"}
  /\ next_gen \in 1..(MaxGen + 1)
  /\ seeded \in BOOLEAN
  /\ edge \in [Writers -> {"none", "landed", "dropped"}]
  /\ adopted \in [Writers -> 0..MaxGen]
  /\ committed \in [Writers -> BOOLEAN]
  /\ ret \in [st : {"none", "condemned", "pending"}, tok : 0..MaxGen, cr : 0..MaxRounds,
             conf : BOOLEAN, pconf : BOOLEAN]
  /\ observed \in BOOLEAN /\ observed_tok \in 0..MaxGen

Init ==
  /\ round = 1 /\ cut_taken = FALSE /\ folded = FALSE
  /\ body = [present |-> FALSE, tok |-> NoTok]
  /\ meta = "clean"
  /\ next_gen = 1
  /\ seeded = FALSE
  /\ edge = [w \in Writers |-> "none"]
  /\ adopted = [w \in Writers |-> NoTok]
  /\ committed = [w \in Writers |-> FALSE]
  /\ ret = NoRet
  /\ observed = FALSE /\ observed_tok = NoTok

Fresh == next_gen <= MaxGen

------------------------------------------------------------------------------------
(* The pre-existing unowned incarnation: a prior part published this body and its ref
   was dropped before the model starts — folded in-degree 0, the condemnable prey. A
   Clean blob carries a clean-reading meta. *)
Seed ==
  /\ ~seeded /\ Fresh
  /\ body' = [present |-> TRUE, tok |-> next_gen]
  /\ next_gen' = next_gen + 1
  /\ seeded' = TRUE
  /\ UNCHANGED <<round, cut_taken, folded, meta, edge, adopted, committed, ret, observed, observed_tok>>

------------------------------------------------------------------------------------
(* Writers (one build each). The durable edge lands BEFORE any observation (EDGE-BEFORE-
   OBSERVE); acquisition then reads the meta: clean => adopt the CURRENT token
   without re-upload; condemned => resurrect (displace the body with a FRESH token
   and drive the meta back to clean — the sole condemned -> clean transition); body
   absent => create fresh. *)
WPrecommit(w) ==
  /\ seeded /\ edge[w] = "none"
  /\ edge' = [edge EXCEPT ![w] = "landed"]
  /\ UNCHANGED <<round, cut_taken, folded, body, meta, next_gen, seeded, adopted, committed, ret, observed, observed_tok>>

WAdopt(w) ==
  /\ edge[w] = "landed" /\ adopted[w] = NoTok
  /\ body.present /\ meta = "clean"
  /\ adopted' = [adopted EXCEPT ![w] = body.tok]
  /\ UNCHANGED <<round, cut_taken, folded, body, meta, next_gen, seeded, edge, committed, ret, observed, observed_tok>>

WResurrect(w) ==
  /\ edge[w] = "landed" /\ adopted[w] = NoTok
  /\ body.present /\ meta = "condemned" /\ Fresh
  /\ body' = [present |-> TRUE, tok |-> next_gen]
  /\ meta' = "clean"
  /\ adopted' = [adopted EXCEPT ![w] = next_gen]
  /\ next_gen' = next_gen + 1
  /\ UNCHANGED <<round, cut_taken, folded, seeded, edge, committed, ret, observed, observed_tok>>

WCreate(w) ==
  /\ edge[w] = "landed" /\ adopted[w] = NoTok
  /\ ~body.present /\ Fresh
  /\ body' = [present |-> TRUE, tok |-> next_gen]
  /\ meta' = "clean"
  /\ adopted' = [adopted EXCEPT ![w] = next_gen]
  /\ next_gen' = next_gen + 1
  /\ UNCHANGED <<round, cut_taken, folded, seeded, edge, committed, ret, observed, observed_tok>>

WCommit(w) ==
  /\ edge[w] = "landed" /\ adopted[w] /= NoTok /\ ~committed[w]
  /\ committed' = [committed EXCEPT ![w] = TRUE]
  /\ UNCHANGED <<round, cut_taken, folded, body, meta, next_gen, seeded, edge, adopted, ret, observed, observed_tok>>

(* The writer's reference goes away: its part is dropped, or its insert aborts before
   commit. Without this an edge is permanent, every later cut folds the hash, and no
   body is condemned again once any writer lands. *)
WDrop(w) ==
  /\ TwoWriters /\ edge[w] = "landed"
  /\ edge' = [edge EXCEPT ![w] = "dropped"]
  /\ committed' = [committed EXCEPT ![w] = FALSE]
  /\ UNCHANGED <<round, cut_taken, folded, body, meta, next_gen, seeded, adopted, ret, observed, observed_tok>>

------------------------------------------------------------------------------------
(* GC. GCut snapshots the round's edge visibility (the discovery LIST / merge input);
   GSettle settles the retired entry against that snapshot and executes the round's
   pre-CAS delete + marker writes. Writer actions between the two ARE the C1 window. *)
GCut ==
  /\ ~cut_taken /\ round < MaxRounds
  /\ folded' = (\E w \in Writers : edge[w] = "landed")
  /\ cut_taken' = TRUE
  /\ UNCHANGED <<round, body, meta, next_gen, seeded, edge, adopted, committed, ret, observed, observed_tok>>

EndRound == /\ cut_taken' = FALSE
            /\ round' = round + 1
            /\ observed' = FALSE /\ observed_tok' = NoTok

(* The HEAD read-ahead: the body's token is observed at some point after the cut and
   before the settle. Without an observation the settle reads the live body, as an
   inline HEAD after a read-ahead miss does. *)
GObserve ==
  /\ Prefetch /\ cut_taken /\ ~observed
  /\ observed' = TRUE
  /\ observed_tok' = IF body.present THEN body.tok ELSE NoTok
  /\ UNCHANGED <<round, cut_taken, folded, body, meta, next_gen, seeded, edge, adopted, committed, ret>>

SeenPresent == IF Prefetch /\ observed THEN observed_tok /= NoTok ELSE body.present
SeenTok     == IF Prefetch /\ observed THEN observed_tok ELSE body.tok

Confirmed == ret.conf \/ (PersistConf /\ ret.pconf)

(* Fresh condemn at folded in-degree 0: the entry enters the retired set and the
   marker write is scheduled — it either lands durable (conf) or is SWALLOWED (the
   §3.4 failure; the entry stays unconfirmed). The rebuild's synchronous marker
   publish is the marker-ok branch of this same action. *)
GSettleCondemn ==
  /\ cut_taken /\ ret.st = "none" /\ SeenPresent /\ ~folded
  /\ \/ /\ meta' = "condemned"   \* marker write landed durable
        /\ ret' = [st |-> "condemned", tok |-> SeenTok, cr |-> round, conf |-> TRUE, pconf |-> PersistConf]
     \/ /\ meta' = meta          \* marker write SWALLOWED (exception eaten by the pool wrapper)
        /\ ret' = [st |-> "condemned", tok |-> SeenTok, cr |-> round, conf |-> FALSE, pconf |-> FALSE]
  /\ EndRound
  /\ UNCHANGED <<folded, body, next_gen, seeded, edge, adopted, committed>>

GSettleIdle ==
  /\ cut_taken /\ ret.st = "none" /\ (~SeenPresent \/ folded)
  /\ EndRound
  /\ UNCHANGED <<folded, body, meta, next_gen, seeded, edge, adopted, committed, ret>>

(* Folded in-degree recovered before graduation: spared (entry drops, body stays). *)
GSettleSpare ==
  /\ cut_taken /\ ret.st = "condemned" /\ folded
  /\ ret' = NoRet
  /\ EndRound
  /\ UNCHANGED <<folded, body, meta, next_gen, seeded, edge, adopted, committed>>

(* Floor-passed graduation attempt. The GATE: pre-fix (GateOnMarker = FALSE) it
   graduates unconditionally; post-fix it requires the in-process confirmation OR a
   loadMeta re-check observing condemned. Refused => CARRY + retry the marker write
   (which may land durable or be swallowed again). *)
GSettleGraduate ==
  /\ cut_taken /\ ret.st = "condemned" /\ ~folded /\ ret.cr < round
  /\ (~GateOnMarker \/ Confirmed \/ meta = "condemned")
  /\ ret' = [ret EXCEPT !.st = "pending", !.conf = TRUE, !.pconf = PersistConf]
  /\ EndRound
  /\ UNCHANGED <<folded, body, meta, next_gen, seeded, edge, adopted, committed>>

(* The gate passed but the round's graduation budget is spent: the row is carried and,
   with PersistConf, carries the confirmation. *)
GSettleGraduateDeferred ==
  /\ PersistConf
  /\ cut_taken /\ ret.st = "condemned" /\ ~folded /\ ret.cr < round
  /\ (~GateOnMarker \/ Confirmed \/ meta = "condemned")
  /\ ret' = [ret EXCEPT !.conf = TRUE, !.pconf = TRUE]
  /\ EndRound
  /\ UNCHANGED <<folded, body, meta, next_gen, seeded, edge, adopted, committed>>

(* The body now carries another token: the row is replaced by a fresh row for that
   token, with its own marker write and without the old row's confirmation. *)
GSettleSupersede ==
  /\ PersistConf
  /\ cut_taken /\ ret.st = "condemned" /\ ~folded /\ SeenPresent /\ SeenTok /= ret.tok
  /\ \/ /\ meta' = "condemned"
        /\ ret' = [st |-> "condemned", tok |-> SeenTok, cr |-> round, conf |-> TRUE, pconf |-> TRUE]
     \/ /\ meta' = meta
        /\ ret' = [st |-> "condemned", tok |-> SeenTok, cr |-> round, conf |-> FALSE, pconf |-> FALSE]
  /\ EndRound
  /\ UNCHANGED <<folded, body, next_gen, seeded, edge, adopted, committed>>

GSettleCarryRetry ==
  /\ cut_taken /\ ret.st = "condemned" /\ ~folded /\ ret.cr < round
  /\ GateOnMarker /\ ~Confirmed /\ meta /= "condemned"
  /\ \/ /\ meta' = "condemned"   \* the carry-time retry landed durable
        /\ ret' = [ret EXCEPT !.conf = TRUE, !.pconf = PersistConf]
     \/ /\ meta' = meta          \* the retry was swallowed again — still unconfirmed
        /\ ret' = ret
  /\ EndRound
  /\ UNCHANGED <<folded, body, next_gen, seeded, edge, adopted, committed>>

(* A delete_pending entry whose edge recovered by this round's cut: spared loudly. *)
GSettleSpareLoud ==
  /\ cut_taken /\ ret.st = "pending" /\ folded
  /\ ret' = NoRet
  /\ EndRound
  /\ UNCHANGED <<folded, body, meta, next_gen, seeded, edge, adopted, committed>>

(* The redelete: the pre-CAS exact-token delete of a previously-published pending
   entry. UNGUARDED against edges landing after this round's cut — the C1 window. A
   token mismatch (resurrected body) is a no-op; the entry drops either way. An
   actual delete drops the meta alongside (deleteConfirmedMeta; absence == clean). *)
GSettleRedelete ==
  /\ cut_taken /\ ret.st = "pending" /\ ~folded
  /\ IF body.present /\ body.tok = ret.tok
       THEN /\ body' = [present |-> FALSE, tok |-> NoTok]
            /\ meta' = "clean"
       ELSE /\ body' = body
            /\ meta' = meta
  /\ ret' = NoRet
  /\ EndRound
  /\ UNCHANGED <<folded, next_gen, seeded, edge, adopted, committed>>

(* Leader change / process restart: the in-process confirmation set is lost (the
   durable meta, the durable pending bit and the persisted pconf survive). Forces the graduation gate onto
   the loadMeta re-check branch. *)
GRestart ==
  /\ ret.st = "condemned" /\ ret.conf
  /\ ret' = [ret EXCEPT !.conf = FALSE]
  /\ UNCHANGED <<round, cut_taken, folded, body, meta, next_gen, seeded, edge, adopted, committed, observed, observed_tok>>

------------------------------------------------------------------------------------
Next ==
  \/ Seed
  \/ \E w \in Writers : WPrecommit(w) \/ WAdopt(w) \/ WResurrect(w) \/ WCreate(w) \/ WCommit(w) \/ WDrop(w)
  \/ GCut \/ GObserve
  \/ GSettleCondemn \/ GSettleIdle \/ GSettleSpare
  \/ GSettleGraduate \/ GSettleGraduateDeferred \/ GSettleSupersede \/ GSettleCarryRetry
  \/ GSettleSpareLoud \/ GSettleRedelete
  \/ GRestart

Spec == Init /\ [][Next]_vars

(* THE invariant: a live committed edge names the key's logical content, not the
   incarnation token observed by the writer. Because this model has one fixed content-addressed
   key, presence is also content identity; a safe equivalent replacement may change body.tok. *)
NoDangle == \A w \in Writers : committed[w] => body.present

=====================================================================================
