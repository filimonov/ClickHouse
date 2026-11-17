---------------------------- MODULE CaBlobIntentProtocol ----------------------------
(*
Blob publication without a birth marker, with GC-owned deletion intents keyed by the body
incarnation: the writer rules W1 to W7 and the GC rules G2 to G12 of
docs/superpowers/specs/2026-11-17-cas-blob-deletion-intents-design.md, section 3.

One blob key B. Writers and GC actors race on the store and on one `gc/state`. Two GC actors in a
safety run are a leader and a deposed leader whose requests still land.

With `Sab = {}` the module states the protocol as specified. Each name in `Sab` weakens one rule;
the comment beside it in `KnownSab` names the rule.

Requests. A read (`HEAD`, `GET`, `LIST`) is one atomic observation. A write is three steps: send,
apply (whatever its sender does by now), answer. A sender may stop waiting; with the kind in `Late`
the request stays in flight and may apply later, otherwise it is never applied.
*)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
  Writers,        \* writer slots (model values)
  GCs,            \* GC actors (model values)
  NoG,            \* "nobody waits" (model value)
  Seed,           \* initial objects, see Init
  TokenKind,      \* "Generation": unique per landing. "Content": derived from the envelope bytes
  MaxPuts,        \* body publications the writers may send in total
  MaxResends,     \* transport resends of a landed publication (0 unless testing A1)
  MaxVer,         \* `gc/state` round commits. 0: unbounded (one GC actor; liveness runs)
  Late,           \* kinds that stay in flight after the sender stopped waiting: "put", "ic", "id", "bd"
  Faults,         \* GC rounds may die at any step and GC requests may stay unanswered
  FaultBudget,    \* at most this many GC fault steps (0: unbounded while Faults)
  Trigger,        \* "any": a round may see a fresh transition and a touched key at any cut (a superset).
                  \* "zero": exact, from the writers' edge events (one GC actor)
  Linker,         \* the no-bytes reference path runs (`adoptEvidence`)
  StagedCopy,     \* the verbatim staged copy exists; every writer slot is an alias of ONE staging object
  SourceChanges,  \* a writer's local source may differ from what was hashed at staging
  Rebuild,        \* `gc/state` may lose its row once (`SYSTEM CAS GC REBUILD`); intents stay
  StaleList,      \* the G9 listing may name an intent that was deleted since
  OverBudget,     \* a create of G2, G3 or G7 may fall over the round's creation budget
  Budget,         \* count the writer's requests
  GBudget,        \* count the requests of each GC round
  Sab             \* weakened rules, see KnownSab

KnownSab ==
  {"delete_on_graduation_cut",   \* G4: the body is deleted in the round that confirmed, on that round's cut (S5)
   "attempted_create_confirms",  \* confirmation table: a sent create counts as confirmed (S6)
   "absent_conflict_confirms",   \* confirmation table: a resolve read that finds nothing counts as confirmed
   "spare_deletes_intent",       \* G6, G11: the intent is deleted on a spare (S4)
   "id_in_body_job",             \* G8, G11: the intent is deleted in the job that deletes the body (S1)
   "dfr_despite_indegree",       \* G8: the intent is deleted at positive in-degree (S3)
   "cut_before_commit",          \* G8: the cut is taken before the lease read of `gc/state` (S2)
   "adopt_delete_now",           \* G9: the adopted intent is deleted in the adopting round (S7)
   "ambiguous_delete_standing",  \* G5: an unanswered body DELETE still commits standing(t) (S9)
   "delete_despite_indegree",    \* G4, G6: a pending row is executed although the cut holds an edge
   "stale_read",                 \* W7: W4 is answered from an observation made before the edge (S12)
   "staging_alias_two_copies",   \* A1: the once-only flag of the staged copy is not shared by aliases
   "unchecked_stream",           \* section 6.2: a streaming publication is not hashed (S10)
   "no_touched_head",            \* G7: a touched key gets no supersede `HEAD` (G7 only from G4's `HEAD`)
   "g8_before_g7",               \* G7 order: G8 wins over G7 on a standing row whose `HEAD` shows `u`
   "g9_before_g2",               \* G9: may adopt in the round whose fold condemns the key (G2)
   "precut_head",                \* G12: the `HEAD`s of G2 and G7 may be read after the lease, before the cut
   "precut_g2_only",             \* G12: as precut_head, only G2 uses the early answer
   "no_g9",                      \* G9 removed (control: an orphan intent is never removed)
   "keep_standing_on_spare",     \* contrast, G6: a standing row survives a cut with in-degree above 0
   "g9_at_indegree"}             \* contrast, G9: adopts at in-degree above 0

Has(s) == s \in Sab
LateOk(k) == k \in Late

MaxTok == MaxPuts + MaxResends + 2     \* seed token, publications, the staged envelope
StagedEnv == MaxTok                    \* envelope of the one staging object (minted once, A1)
Tokens == 1..MaxTok
Good == "P"
Bad == "X"

NoBody == [present |-> FALSE, tok |-> 0, ser |-> 0, pay |-> "none"]

(* A row of `gc/state` for B. `conf` is ghost: the create was confirmed by the confirmation table. *)
NoEntry == [st |-> "none", tok |-> 0, conf |-> FALSE]
Cand(t) == [st |-> "cand", tok |-> t, conf |-> FALSE]
Pend(t, c) == [st |-> "pend", tok |-> t, conf |-> c]
Stand(t) == [st |-> "stand", tok |-> t, conf |-> TRUE]

VARIABLES
  body,      \* the object at B: [present, tok, ser, pay]; `ser` names the physical landing
  nextSer,   \* landings so far + 1
  gone,      \* ghost: tokens that stopped being the incarnation of B
  intents,   \* tokens t with I(B,t) present
  wr,        \* per writer slot: the part write in progress
  nextEnv, putq, sentq, nsend, nres,
  copied,    \* writer slots that used the staged copy (the once-only flag, A1)
  gs,        \* durable `gc/state`: [e : row, ver, rb]
  gc,        \* per GC actor: its round in progress
  req,       \* GC write requests in flight, or applied and not yet consumed
  dirty,     \* Trigger = "zero": an edge was withdrawn and no committed round folded it (G2's trigger)
  dseen,     \* the round in progress took its cut after that withdrawal
  touched,   \* Trigger = "zero": an edge of B was added or withdrawn and no committed round folded it
  tpost,     \* Trigger = "zero": an edge event happened after the cut of the round in progress
  ever,      \* ghost: every token that ever had an intent (the stale G9 listing)
  nf,        \* GC fault steps so far (FaultBudget > 0)
  fRecur, fI4, fOther, fAdopt   \* ghost violation bits

bodyVars == <<body, nextSer, gone>>
putVars == <<nextEnv, putq, sentq, nsend, nres, copied>>
ghostVars == <<fRecur, fI4, fOther, fAdopt>>
trigVars == <<dirty, dseen, touched, tpost, ever, nf>>
vars == <<bodyVars, intents, wr, putVars, gs, gc, req, ghostVars, trigVars>>

W0 == [ph |-> "Idle", seen |-> 0, env |-> 0, landed |-> FALSE, cp |-> FALSE,
       c404 |-> 0, path |-> "none", rd |-> 0, pt |-> 0, ga |-> 0]

Cnt0 == [h |-> 0, ic |-> 0, id |-> 0, bd |-> 0, rr |-> 0]
(* `ref`: the cut shows in-degree above 0. `fr`: the cut folds a fresh transition to zero (G2).
   `ts`: the key is touched in this round's window. `g7`: the token whose intent G7 deletes. *)
G0 == [ph |-> "Idle", base |-> NoEntry, bver |-> 0, ref |-> FALSE, ecut |-> FALSE, fr |-> FALSE,
       next |-> NoEntry, ct |-> 0, g7 |-> 0, ts |-> FALSE, cnt |-> Cnt0, ra |-> 0, rah |-> FALSE]

Rq(k, o, t, s, ok) == [k |-> k, o |-> o, tok |-> t, ser |-> s, ok |-> ok, ap |-> FALSE, res |-> "none"]

(* Seeds. empty: nothing. orphan: body 1, unreferenced, no row. pend: pending(1), I(B,1), body 1.
   cand: candidate(1), body 1. spared: I(B,1) over body 1, no row (a crash between create and
   commit). lost: I(B,1), no body, no row. orphans2: I(B,1) and I(B,2) over body 2, no row. *)
SeedBody == Seed \notin {"empty", "lost"}
Fresh0 == Trigger = "zero" /\ Seed \in {"orphan", "spared", "orphans2"}

Init ==
  /\ body = IF SeedBody THEN [present |-> TRUE, tok |-> IF Seed = "orphans2" THEN 2 ELSE 1, ser |-> 1, pay |-> Good]
            ELSE NoBody
  /\ nextSer = IF Seed = "orphans2" THEN 3 ELSE IF SeedBody \/ Seed = "lost" THEN 2 ELSE 1
  /\ nextEnv = IF Seed = "orphans2" THEN 3 ELSE 2
  /\ gone = IF Seed \in {"lost", "orphans2"} THEN {1} ELSE {}
  /\ intents = IF Seed \in {"pend", "spared", "lost"} THEN {1} ELSE IF Seed = "orphans2" THEN {1, 2} ELSE {}
  /\ wr = [w \in Writers |-> W0]
  /\ putq = {} /\ sentq = {} /\ nsend = 0 /\ nres = 0 /\ copied = {}
  /\ gs = [e |-> CASE Seed = "cand" -> Cand(1) [] Seed = "pend" -> Pend(1, TRUE) [] OTHER -> NoEntry,
           ver |-> 0, rb |-> 0]
  /\ gc = [g \in GCs |-> G0]
  /\ req = {}
  /\ dirty = Fresh0 /\ dseen = FALSE /\ touched = Fresh0 /\ tpost = FALSE
  /\ ever = intents /\ nf = 0
  /\ fRecur = FALSE /\ fI4 = FALSE /\ fOther = FALSE /\ fAdopt = FALSE

--------------------------------------------------------------------------------------
(* Writer request counters. They are not reset when the writer's loop iterates. *)
Rd(r) == IF Budget THEN [r EXCEPT !.rd = @ + 1] ELSE r
Pt(r) == IF Budget THEN [r EXCEPT !.pt = @ + 1] ELSE r
SetPath(r, p) ==
  IF ~Budget THEN r
  ELSE IF r.ga = 0 THEN [r EXCEPT !.path = p]
  ELSE IF r.path = "amb_new" /\ r.ga = 1 /\ p = "reuse" THEN [r EXCEPT !.path = "amb_new_reuse"]
  ELSE [r EXCEPT !.path = "amb"]

(* An intent DELETE is sent for `t`. Ghost for I4: `t` must not be the incarnation of B. *)
IdBad(t) == body.present /\ body.tok = t
IdReq(o, t) == Rq("id", o, t, 0, TRUE)

(* Publications of `w` after `w` stopped waiting. Without "put" in Late an unapplied one is dropped. *)
DropPuts(w) == IF LateOk("put") THEN putq ELSE {q \in putq : q.w /= w}

(* An edge of B is added or withdrawn: the key is touched in the window of the next cut. *)
EdgeEv ==
  /\ touched' = (touched \/ Trigger = "zero") /\ tpost' = (Trigger = "zero")
  /\ UNCHANGED <<ever, nf>>

--------------------------------------------------------------------------------------
(* WRITER, section 3.1. *)

(* W1: the precommit edge is durable before the first request. *)
WPrecommit(w) ==
  /\ wr[w].ph = "Idle"
  /\ wr' = [wr EXCEPT ![w] = [W0 EXCEPT !.ph = "H1", !.c404 = wr[w].c404]]
  /\ UNCHANGED <<bodyVars, intents, putVars, gs, gc, req, ghostVars>>
  /\ UNCHANGED <<dirty, dseen>> /\ EdgeEv

(* Sabotage of W7: an answer "I(B,t) absent" obtained before the edge exists is kept and reused. *)
WPrime(w) ==
  /\ Has("stale_read") /\ wr[w].ph = "Idle"
  /\ body.present /\ body.tok \notin intents /\ wr[w].c404 /= body.tok
  /\ wr' = [wr EXCEPT ![w].c404 = body.tok]
  /\ UNCHANGED <<bodyVars, intents, putVars, gs, gc, req, ghostVars, trigVars>>

(* W2: `HEAD B`. Absent -> W5, first-publication form. Present -> W4 for its token. *)
WHead(w) ==
  /\ wr[w].ph = "H1"
  /\ LET r == Rd(wr[w])
     IN wr' = [wr EXCEPT ![w] =
          IF ~body.present
            THEN [SetPath(r, "new") EXCEPT !.ph = "Pub", !.cp = StagedCopy]
            ELSE [r EXCEPT !.ph = "G", !.seen = body.tok]]
  /\ UNCHANGED <<bodyVars, intents, putVars, gs, gc, req, ghostVars, trigVars>>

(* W4: `GET I(B,t)`, sent after the answer of W2. An object -> W5. An answered 404 -> adopt. *)
WGetIntent(w) ==
  /\ wr[w].ph = "G"
  /\ LET cached == Has("stale_read") /\ wr[w].c404 = wr[w].seen
         r == IF cached THEN wr[w] ELSE Rd(wr[w])
     IN IF r.seen \in intents /\ ~cached
          THEN /\ wr' = [wr EXCEPT ![w] = [SetPath(r, "condemned") EXCEPT !.ph = "Pub", !.seen = 0, !.cp = FALSE]]
               /\ UNCHANGED fAdopt
          ELSE /\ wr' = [wr EXCEPT ![w] = [SetPath(r, "reuse") EXCEPT !.ph = "Ready", !.seen = 0]]
               /\ fAdopt' = (fAdopt \/ cached)
  /\ UNCHANGED <<bodyVars, intents, putVars, gs, gc, req, fRecur, fI4, fOther, trigVars>>

(* The once-only rule of the verbatim staged copy (A1): aliases of one staging object share it. *)
CopyAllowed(w) == IF Has("staging_alias_two_copies") THEN w \notin copied ELSE copied = {}

(* W5: one unconditional publication, sent once. Streaming: a fresh envelope per attempt (A1), the
   payload is hashed; a source that no longer hashes to the key is refused before the object
   completes (the writer's exception is WAbandon). Verbatim staged copy: only as the first
   publication of the staging object and only after W2 answered absent; not hashed, rests on A6. *)
WSendPut(w) ==
  /\ wr[w].ph = "Pub" /\ nsend < MaxPuts
  /\ \E copy \in (IF wr[w].cp /\ CopyAllowed(w) THEN {TRUE, FALSE} ELSE {FALSE}) :
     \E p \in (IF SourceChanges /\ ~copy /\ Has("unchecked_stream") THEN {Good, Bad} ELSE {Good}) :
       LET e == IF copy THEN StagedEnv ELSE nextEnv
       IN /\ putq' = putq \cup {[w |-> w, env |-> e, pay |-> p]}
          /\ nextEnv' = IF copy THEN nextEnv ELSE nextEnv + 1
          /\ copied' = IF copy THEN copied \cup {w} ELSE copied
          /\ wr' = [wr EXCEPT ![w] = [Pt(@) EXCEPT !.ph = "Wait", !.env = e, !.landed = FALSE]]
  /\ nsend' = nsend + 1
  /\ UNCHANGED <<bodyVars, intents, sentq, nres, gs, gc, req, ghostVars, trigVars>>

TokOf(e) == IF TokenKind = "Generation" THEN nextSer ELSE e

LandBody(e, p) ==
  LET t == TokOf(e)
      stays == body.present /\ body.tok = t
  IN /\ body' = [present |-> TRUE, tok |-> t, ser |-> nextSer, pay |-> p]
     /\ nextSer' = nextSer + 1
     /\ fRecur' = (fRecur \/ t \in gone)
     /\ gone' = IF body.present /\ ~stays THEN gone \cup {body.tok} ELSE gone

(* Store: a publication applies, whatever its sender does by now. *)
PutLand(q) ==
  /\ q \in putq
  /\ LandBody(q.env, q.pay)
  /\ putq' = putq \ {q}
  /\ sentq' = IF MaxResends > 0 THEN sentq \cup {q} ELSE sentq
  /\ wr' = IF wr[q.w].ph = "Wait" /\ wr[q.w].env = q.env THEN [wr EXCEPT ![q.w].landed = TRUE] ELSE wr
  /\ UNCHANGED <<intents, nextEnv, nsend, nres, copied, gs, gc, req, fI4, fOther, fAdopt, trigVars>>

(* A1 broken on purpose: the transport reissues a request that was already applied. *)
PutResend(q) ==
  /\ q \in sentq /\ nres < MaxResends
  /\ LandBody(q.env, q.pay)
  /\ nres' = nres + 1
  /\ UNCHANGED <<intents, wr, nextEnv, putq, sentq, nsend, copied, gs, gc, req, fI4, fOther, fAdopt, trigVars>>

(* W5 answered success -> `Materialized`. W6: no request follows. *)
WPutOk(w) ==
  /\ wr[w].ph = "Wait" /\ wr[w].landed
  /\ wr' = [wr EXCEPT ![w] = [@ EXCEPT !.ph = "Ready", !.env = 0, !.landed = FALSE]]
  /\ UNCHANGED <<bodyVars, intents, putVars, gs, gc, req, ghostVars, trigVars>>

(* W5 unanswered: the next loop iteration from W2. The request may have applied, and with "put" in
   Late may still apply. *)
WPutGiveUp(w) ==
  /\ wr[w].ph = "Wait"
  /\ wr' = [wr EXCEPT ![w] =
       [@ EXCEPT !.ph = "H1", !.env = 0, !.landed = FALSE, !.cp = FALSE,
                 !.path = IF ~Budget THEN "none" ELSE IF @ = "new" /\ wr[w].ga = 0 THEN "amb_new" ELSE "amb",
                 !.ga = IF Budget /\ @ < 2 THEN @ + 1 ELSE @]]
  /\ putq' = DropPuts(w)
  /\ UNCHANGED <<bodyVars, intents, nextEnv, sentq, nsend, nres, copied, gs, gc, req, ghostVars, trigVars>>

(* I3: commit only while the precommit is the live owner. *)
WCommit(w) ==
  /\ wr[w].ph = "Ready"
  /\ wr' = [wr EXCEPT ![w] = [@ EXCEPT !.ph = "Done", !.rd = 0, !.pt = 0, !.ga = 0,
                                        !.path = IF @ = "reuse" THEN "reuse" ELSE "none"]]
  /\ UNCHANGED <<bodyVars, intents, putVars, gs, gc, req, ghostVars, trigVars>>

(* The part write ends before its commit and its edge is withdrawn: an exception at any step, a
   refused or unanswered read, or the death of the process and the cleanup of its precommit. *)
WAbandon(w) ==
  /\ wr[w].ph \notin {"Idle", "Done"}
  /\ wr' = [wr EXCEPT ![w] = [W0 EXCEPT !.c404 = wr[w].c404]]
  /\ putq' = DropPuts(w)
  /\ UNCHANGED <<bodyVars, intents, nextEnv, sentq, nsend, nres, copied, gs, gc, req, ghostVars>>
  /\ dirty' = (dirty \/ Trigger = "zero") /\ dseen' = FALSE /\ EdgeEv

(* The committed part is dropped: its edge is withdrawn. *)
WDrop(w) ==
  /\ wr[w].ph = "Done"
  /\ wr' = [wr EXCEPT ![w] = [W0 EXCEPT !.c404 = wr[w].c404]]
  /\ UNCHANGED <<bodyVars, intents, putVars, gs, gc, req, ghostVars>>
  /\ dirty' = (dirty \/ Trigger = "zero") /\ dseen' = FALSE /\ EdgeEv

(* `adoptEvidence`: an edge is added with no request while a source edge (a committed ref of
   another slot) is live. *)
WLink(w) ==
  /\ Linker /\ wr[w].ph = "Idle"
  /\ \E v \in Writers \ {w} : wr[v].ph = "Done"
  /\ wr' = [wr EXCEPT ![w] = [W0 EXCEPT !.ph = "Done", !.path = "link", !.c404 = wr[w].c404]]
  /\ UNCHANGED <<bodyVars, intents, putVars, gs, gc, req, ghostVars>>
  /\ UNCHANGED <<dirty, dseen>> /\ EdgeEv

--------------------------------------------------------------------------------------
(* GC, section 3.2. One round: lease (read `gc/state`), cut, the fold (every decision and every
   `HEAD` and create), the delete batch, one commit by compare-and-swap. An actor whose commit loses
   is a deposed leader; what it sent stays sent. One key: a round takes at most one decision for B. *)

HasEdge == \E w \in Writers : wr[w].ph /= "Idle"
OwnReq(g) == {q \in req : q.o = g}

StopWaiting(g, k) ==
  {q \in req : q.o /= g}
    \cup (IF LateOk(k) THEN {[q EXCEPT !.o = NoG] : q \in {x \in OwnReq(g) : ~x.ap}} ELSE {})

Inc(r, f) == IF GBudget THEN [r EXCEPT !.cnt[f] = @ + 1] ELSE r
IncIf(r, f, c) == IF c THEN Inc(r, f) ELSE r

FaultOk == Faults /\ (FaultBudget = 0 \/ nf < FaultBudget)
FaultStep == nf' = IF FaultBudget = 0 THEN nf ELSE nf + 1
NoTrig == UNCHANGED trigVars
KeepTrigFault == UNCHANGED <<dirty, dseen, touched, tpost, ever>> /\ FaultStep

(* The lease read-modify-write of `gc/state` starts the round. *)
GStart(g) ==
  /\ gc[g].ph = IF Has("cut_before_commit") THEN "Pre" ELSE "Idle"
  /\ gc' = [gc EXCEPT ![g] = [G0 EXCEPT !.ph = "Read", !.ecut = gc[g].ecut, !.base = gs.e, !.bver = gs.ver]]
  /\ UNCHANGED <<bodyVars, intents, wr, putVars, gs, req, ghostVars, trigVars>>

(* Sabotage "cut_before_commit": G8 decides on a cut taken before the lease read. *)
GPreCut(g) ==
  /\ Has("cut_before_commit") /\ gc[g].ph = "Idle"
  /\ gc' = [gc EXCEPT ![g] = [G0 EXCEPT !.ph = "Pre", !.ecut = HasEdge]]
  /\ UNCHANGED <<bodyVars, intents, wr, putVars, gs, req, ghostVars, trigVars>>

(* Sabotage of G12: a `HEAD B` read after the lease and before the cut. *)
PreCut == Has("precut_head") \/ Has("precut_g2_only")
GReadAhead(g) ==
  /\ PreCut /\ gc[g].ph = "Read" /\ ~gc[g].rah
  /\ gc' = [gc EXCEPT ![g] = [@ EXCEPT !.rah = TRUE, !.ra = IF body.present THEN body.tok ELSE 0]]
  /\ UNCHANGED <<bodyVars, intents, wr, putVars, gs, req, ghostVars, trigVars>>

(* The round's complete reference cut (A7), after the lease. With Trigger = "zero" the fresh
   transition and the touched flag are exact; with "any" each is a free choice per round. *)
GCut(g) ==
  /\ gc[g].ph = "Read"
  /\ \E ts \in (IF Trigger = "zero" THEN {touched} ELSE BOOLEAN) :
     \E fr \in (IF Trigger = "zero" THEN {dirty} ELSE BOOLEAN) :
       gc' = [gc EXCEPT ![g] = [@ EXCEPT !.ph = "Work", !.ref = HasEdge, !.ts = ts, !.fr = fr]]
  /\ dseen' = dirty /\ tpost' = FALSE
  /\ UNCHANGED <<dirty, touched, ever, nf>>
  /\ UNCHANGED <<bodyVars, intents, wr, putVars, gs, req, ghostVars>>

IsPend(g) == gc[g].base.st = "pend"
Zero(g) == ~gc[g].ref

(* G7, G12: a row gets a `HEAD B` in this round iff it is pending (G4 step 1) or its key is touched
   (the supersede `HEAD`), sent after the cut. A row at zero whose `HEAD` shows `u` takes G7. *)
HeadSent(g) == (gc[g].ts /\ ~Has("no_touched_head")) \/ IsPend(g)
SupTok(g) ==
  IF gc[g].rah /\ ~IsPend(g) /\ ~Has("precut_g2_only") THEN gc[g].ra
  ELSE IF body.present THEN body.tok ELSE 0
G7Cond(g) ==
  /\ gc[g].base.st /= "none" /\ Zero(g) /\ HeadSent(g)
  /\ SupTok(g) /= 0 /\ SupTok(g) /= gc[g].base.tok

(* G2: no row, zero, a fresh transition, a body present. *)
G2Obs(g) == IF gc[g].rah THEN gc[g].ra ELSE IF body.present THEN body.tok ELSE 0
G2Cond(g) == Zero(g) /\ gc[g].fr /\ G2Obs(g) /= 0
(* G9: one `LIST` page nominates the intents of B. A stale listing may name one deleted since. *)
Listable == IF StaleList THEN ever ELSE intents
G9Able(g) ==
  /\ ~Has("no_g9") /\ Listable /= {}
  /\ Zero(g) \/ Has("g9_at_indegree")
  /\ ~G2Cond(g) \/ Has("g9_before_g2")

(* G2: `HEAD B` (in G2Obs), then the create of I(B,t) in the fold (GCreateSendFold). Absent: no row.
   A row-less key with nothing to condemn and nothing to adopt ends the round for B. *)
GCondemn(g) ==
  /\ gc[g].ph = "Work" /\ gc[g].base.st = "none"
  /\ IF G2Cond(g)
       THEN gc' = [gc EXCEPT ![g] = [Inc(@, "h") EXCEPT !.ph = "ICr", !.next = Cand(G2Obs(g)), !.ct = G2Obs(g)]]
       ELSE /\ ~G9Able(g)
            /\ gc' = [gc EXCEPT ![g] = G0]
  /\ UNCHANGED <<bodyVars, intents, wr, putVars, gs, req, ghostVars>>
  /\ NoTrig

(* G9: one listed intent of a key with no row, at zero, that G2 does not condemn in this round.
   `HEAD B` after the `LIST` and the cut: same identity -> candidate; absent or another identity ->
   standing. Deletes nothing. *)
GAdoptOrphan(g) ==
  /\ gc[g].ph = "Work" /\ gc[g].base.st = "none" /\ G9Able(g)
  /\ \E t \in Listable :
       LET r == Inc(gc[g], "h") IN
       IF body.present /\ body.tok = t
         THEN /\ gc' = [gc EXCEPT ![g] = [r EXCEPT !.ph = "Commit", !.next = Cand(t)]]
              /\ UNCHANGED <<req, fI4>>
       ELSE IF Has("adopt_delete_now")
         THEN /\ req' = req \cup {IdReq(g, t)}
              /\ gc' = [gc EXCEPT ![g] = [Inc(r, "id") EXCEPT !.ph = "IdWait", !.next = NoEntry]]
              /\ fI4' = (fI4 \/ IdBad(t))
       ELSE /\ gc' = [gc EXCEPT ![g] = [r EXCEPT !.ph = "Commit", !.next = Stand(t)]]
            /\ UNCHANGED <<req, fI4>>
  /\ UNCHANGED <<bodyVars, intents, wr, putVars, gs, fRecur, fOther, fAdopt>>
  /\ NoTrig

(* G6: at in-degree above 0 every row is dropped, with no request; an intent stays. *)
GSpare(g) ==
  /\ gc[g].ph = "Work" /\ gc[g].base.st /= "none" /\ gc[g].ref
  /\ ~(Has("delete_despite_indegree") /\ IsPend(g))
  /\ ~(gc[g].base.st = "stand" /\ (Has("keep_standing_on_spare") \/ Has("dfr_despite_indegree")))
  /\ gc' = [gc EXCEPT ![g] = [@ EXCEPT !.ph = "Commit", !.next = NoEntry]]
  /\ IF Has("spare_deletes_intent") /\ gc[g].base.st /= "cand"
       THEN /\ req' = req \cup {IdReq(NoG, gc[g].base.tok)}
            /\ fI4' = (fI4 \/ IdBad(gc[g].base.tok))
       ELSE UNCHANGED <<req, fI4>>
  /\ UNCHANGED <<bodyVars, intents, wr, putVars, gs, fRecur, fOther, fAdopt>>
  /\ NoTrig

(* Contrast "keep_standing_on_spare": a standing row at in-degree above 0 is carried, no commit. *)
GStandKeep(g) ==
  /\ Has("keep_standing_on_spare") /\ gc[g].ph = "Work" /\ gc[g].base.st = "stand" /\ gc[g].ref
  /\ gc' = [gc EXCEPT ![g] = G0]
  /\ UNCHANGED <<bodyVars, intents, wr, putVars, gs, req, ghostVars>>
  /\ NoTrig

(* G7: a row of `t` (any state) at zero whose `HEAD` of this round shows `u`: `PUT I(B,u)` in the
   fold as in G2 (GCreateSendFold), then `DELETE I(B,t)` in the delete batch (GG7Delete), then one
   commit of pending(u) or candidate(u). *)
GSupersede(g) ==
  /\ gc[g].ph = "Work" /\ G7Cond(g) /\ ~(Has("g8_before_g7") /\ gc[g].base.st = "stand")
  /\ gc' = [gc EXCEPT ![g] = [Inc(@, "h") EXCEPT !.ph = "ICr", !.next = Cand(SupTok(g)), !.ct = SupTok(g),
                                                  !.g7 = gc[g].base.tok]]
  /\ UNCHANGED <<bodyVars, intents, wr, putVars, gs, req, ghostVars>>
  /\ NoTrig

(* G3: a candidate read from the adopted state, at zero, no G7: the create again. *)
GCreateRetry(g) ==
  /\ gc[g].ph = "Work" /\ gc[g].base.st = "cand" /\ Zero(g) /\ ~G7Cond(g)
  /\ gc' = [gc EXCEPT ![g] = [IncIf(@, "h", gc[g].ts) EXCEPT !.ph = "ICr", !.ct = gc[g].base.tok]]
  /\ UNCHANGED <<bodyVars, intents, wr, putVars, gs, req, ghostVars>>
  /\ NoTrig

(* After the create, the row's fate: pending if confirmed, else candidate. G7 goes on to the delete
   of I(B,t). An unconfirmed retry of G3 carries its row: nothing to commit. Sabotage: G4 runs on
   this round's cut. *)
AfterCreate(g) == IF gc[g].g7 /= 0 THEN "IdSend" ELSE "Commit"
Confirmed(g, real) ==
  [gc[g] EXCEPT !.ph = IF Has("delete_on_graduation_cut") THEN "Work2" ELSE AfterCreate(g),
                !.next = Pend(gc[g].ct, real)]
Carried(g) ==
  IF gc[g].base.st = "cand" /\ gc[g].base.tok = gc[g].ct /\ gc[g].g7 = 0 THEN G0
  ELSE [gc[g] EXCEPT !.ph = AfterCreate(g), !.next = Cand(gc[g].ct)]

(* The create of G2, G3 or G7: `PUT I(B,ct) If-None-Match: *`, answered before the row is written.
   Over the creation budget: no request, not confirmed. *)
GCreateSendFold(g) ==
  /\ gc[g].ph = "ICr"
  /\ \E over \in (IF OverBudget THEN BOOLEAN ELSE {FALSE}) :
       IF over
         THEN /\ gc' = [gc EXCEPT ![g] = Carried(g)]
              /\ UNCHANGED req
       ELSE LET r == Inc(gc[g], "ic") IN
         IF Has("attempted_create_confirms")
           THEN /\ req' = req \cup {Rq("ic", NoG, gc[g].ct, 0, TRUE)}
                /\ gc' = [gc EXCEPT ![g] = [Confirmed(g, FALSE) EXCEPT !.cnt = r.cnt]]
           ELSE /\ req' = req \cup {Rq("ic", g, gc[g].ct, 0, TRUE)}
                /\ gc' = [gc EXCEPT ![g] = [r EXCEPT !.ph = "CrWait"]]
  /\ UNCHANGED <<bodyVars, intents, wr, putVars, gs, ghostVars>>
  /\ NoTrig

GCreateAnswer(g) ==
  /\ gc[g].ph = "CrWait"
  /\ \E q \in OwnReq(g) :
       /\ q.ap
       /\ req' = req \ {q}
       /\ gc' = [gc EXCEPT ![g] = IF q.res = "created" THEN Confirmed(g, TRUE) ELSE [@ EXCEPT !.ph = "CrRes"]]
  /\ UNCHANGED <<bodyVars, intents, wr, putVars, gs, ghostVars>>
  /\ NoTrig

GCreateUnanswered(g) ==
  /\ FaultOk /\ gc[g].ph = "CrWait"
  /\ req' = StopWaiting(g, "ic")
  /\ gc' = [gc EXCEPT ![g] = [@ EXCEPT !.ph = "CrRes"]]
  /\ UNCHANGED <<bodyVars, intents, wr, putVars, gs, ghostVars>>
  /\ KeepTrigFault

(* The resolve read: an occupant with the canonical bytes confirms; absence or a failed read does not. *)
GCreateResolve(g) ==
  /\ gc[g].ph = "CrRes"
  /\ \E observed \in (IF FaultOk THEN {TRUE, FALSE} ELSE {TRUE}) :
       LET nx == IF observed /\ gc[g].ct \in intents THEN Confirmed(g, TRUE)
                 ELSE IF Has("absent_conflict_confirms") THEN Confirmed(g, FALSE)
                 ELSE Carried(g)
       IN /\ gc' = [gc EXCEPT ![g] = IF nx.ph = "Idle" THEN nx ELSE Inc(nx, "rr")]
          /\ IF observed THEN UNCHANGED nf ELSE FaultStep
  /\ UNCHANGED <<bodyVars, intents, wr, putVars, gs, req, ghostVars>>
  /\ UNCHANGED <<dirty, dseen, touched, tpost, ever>>

(* G7, in the delete batch: `DELETE I(B,t)` for the token the row named. *)
GG7Delete(g) ==
  /\ gc[g].ph = "IdSend"
  /\ req' = req \cup {IdReq(g, gc[g].g7)}
  /\ gc' = [gc EXCEPT ![g] = [Inc(@, "id") EXCEPT !.ph = "IdWait"]]
  /\ fI4' = (fI4 \/ IdBad(gc[g].g7))
  /\ UNCHANGED <<bodyVars, intents, wr, putVars, gs, fRecur, fOther, fAdopt>>
  /\ NoTrig

(* G4 (1), in the fold: pending read from the adopted state, at zero, no G7. `HEAD B`: same
   identity -> `DELETE B If-Match t` to the batch; absent -> standing(t), no body request.
   Work2 is the sabotage's same-round path. *)
GDelHead(g) ==
  /\ \/ gc[g].ph = "Work" /\ IsPend(g) /\ (Zero(g) \/ Has("delete_despite_indegree")) /\ ~G7Cond(g)
     \/ gc[g].ph = "Work2"
  /\ LET row == IF gc[g].ph = "Work2" THEN gc[g].next ELSE gc[g].base
         okv == gc[g].ph = "Work" /\ row.conf /\ Zero(g)
         r == Inc(gc[g], "h")
     IN IF body.present /\ body.tok = row.tok
          THEN /\ req' = req \cup {Rq("bd", g, row.tok, body.ser, okv)}
               /\ gc' = [gc EXCEPT ![g] = [Inc(r, "bd") EXCEPT !.ph = "DelWait", !.next = row]]
               /\ UNCHANGED fI4
          ELSE IF Has("id_in_body_job")
            THEN /\ req' = req \cup {IdReq(g, row.tok)}
                 /\ gc' = [gc EXCEPT ![g] = [Inc(r, "id") EXCEPT !.ph = "IdWait", !.next = NoEntry]]
                 /\ fI4' = (fI4 \/ IdBad(row.tok))
            ELSE /\ gc' = [gc EXCEPT ![g] = [r EXCEPT !.ph = "Commit", !.next = Stand(row.tok)]]
                 /\ UNCHANGED <<req, fI4>>
  /\ UNCHANGED <<bodyVars, intents, wr, putVars, gs, fRecur, fOther, fAdopt>>
  /\ NoTrig

(* G4 (2): `Removed`, `Gone` and `Mismatch` each record standing(t). *)
GDelAnswer(g) ==
  /\ gc[g].ph = "DelWait"
  /\ \E q \in OwnReq(g) :
       /\ q.ap
       /\ IF Has("id_in_body_job")
            THEN /\ req' = (req \ {q}) \cup {IdReq(g, gc[g].next.tok)}
                 /\ gc' = [gc EXCEPT ![g] = [Inc(@, "id") EXCEPT !.ph = "IdWait", !.next = NoEntry]]
                 /\ fI4' = (fI4 \/ IdBad(gc[g].next.tok))
            ELSE /\ req' = req \ {q}
                 /\ gc' = [gc EXCEPT ![g] = [@ EXCEPT !.ph = "Commit", !.next = Stand(gc[g].next.tok)]]
                 /\ UNCHANGED fI4
  /\ UNCHANGED <<bodyVars, intents, wr, putVars, gs, fRecur, fOther, fAdopt>>
  /\ NoTrig

(* G5: a refused or unanswered body DELETE fails the round; the row keeps its state. A refused
   delete is one that never applies, the same step. *)
GDelUnanswered(g) ==
  /\ FaultOk /\ gc[g].ph = "DelWait"
  /\ req' = StopWaiting(g, "bd")
  /\ gc' = [gc EXCEPT ![g] = IF Has("ambiguous_delete_standing")
                               THEN [@ EXCEPT !.ph = "Commit", !.next = Stand(gc[g].next.tok)]
                               ELSE G0]
  /\ UNCHANGED <<bodyVars, intents, wr, putVars, gs, ghostVars>>
  /\ KeepTrigFault

(* G8: standing(t) read from the adopted state, at zero, no G7 in this round: `DELETE I(B,t)`, no
   `HEAD` of its own. *)
GStandWork(g) ==
  /\ gc[g].ph = "Work" /\ gc[g].base.st = "stand" /\ (~G7Cond(g) \/ Has("g8_before_g7"))
  /\ IF Has("cut_before_commit") THEN ~gc[g].ecut ELSE Zero(g) \/ Has("dfr_despite_indegree")
  /\ req' = req \cup {IdReq(g, gc[g].base.tok)}
  /\ gc' = [gc EXCEPT ![g] = [IncIf(Inc(@, "id"), "h", gc[g].ts) EXCEPT !.ph = "IdWait", !.next = NoEntry]]
  /\ fI4' = (fI4 \/ IdBad(gc[g].base.tok))
  /\ UNCHANGED <<bodyVars, intents, wr, putVars, gs, fRecur, fOther, fAdopt>>
  /\ NoTrig

(* An intent DELETE answered (an absent key is success): the round goes on to its commit. *)
GIdAnswer(g) ==
  /\ gc[g].ph = "IdWait"
  /\ \E q \in OwnReq(g) :
       /\ q.ap
       /\ req' = req \ {q}
       /\ gc' = [gc EXCEPT ![g] = [@ EXCEPT !.ph = "Commit"]]
  /\ UNCHANGED <<bodyVars, intents, wr, putVars, gs, ghostVars>>
  /\ NoTrig

(* G5: an intent DELETE without an answer fails the round; the row keeps its state. *)
GIdUnanswered(g) ==
  /\ FaultOk /\ gc[g].ph = "IdWait"
  /\ req' = StopWaiting(g, "id")
  /\ gc' = [gc EXCEPT ![g] = G0]
  /\ UNCHANGED <<bodyVars, intents, wr, putVars, gs, ghostVars>>
  /\ KeepTrigFault

(* Once another commit moves the version, an actor with only its commit left can only fail. *)
OnlyCommitLeft(h) == gc[h].ph = "Commit"

(* The round commit: a compare-and-swap of `gc/state` on the version the lease read. *)
GCommit(g) ==
  /\ gc[g].ph = "Commit" /\ gc[g].bver = gs.ver /\ (MaxVer = 0 \/ gs.ver < MaxVer)
  /\ gs' = [gs EXCEPT !.e = gc[g].next, !.ver = IF MaxVer = 0 THEN 1 - @ ELSE @ + 1]
  /\ gc' = [h \in GCs |-> IF h = g \/ OnlyCommitLeft(h) THEN G0 ELSE gc[h]]
  /\ UNCHANGED <<bodyVars, intents, wr, putVars, req, ghostVars>>
  /\ dirty' = (dirty /\ ~dseen) /\ dseen' = FALSE
  /\ touched' = (touched /\ ~(gc[g].ts /\ ~tpost)) /\ UNCHANGED <<tpost, ever, nf>>

GCommitLost(g) ==
  /\ gc[g].ph = "Commit" /\ gc[g].bver /= gs.ver
  /\ gc' = [gc EXCEPT ![g] = G0]
  /\ UNCHANGED <<bodyVars, intents, wr, putVars, gs, req, ghostVars, trigVars>>

(* The round ends without a commit: the process died, the lease was lost, or a read failed. *)
GAbort(g) ==
  /\ FaultOk
  /\ gc[g].ph \in {"Read", "Work", "Work2", "ICr", "CrRes", "IdSend", "Commit"}
  /\ gc' = [gc EXCEPT ![g] = G0]
  /\ UNCHANGED <<bodyVars, intents, wr, putVars, gs, req, ghostVars>>
  /\ KeepTrigFault

(* `SYSTEM CAS GC REBUILD`: `gc/state` is rewritten without the row; intents stay. Once. *)
RebuildState ==
  /\ Rebuild /\ gs.rb = 0 /\ gs.e /= NoEntry /\ (MaxVer = 0 \/ gs.ver < MaxVer)
  /\ gs' = [gs EXCEPT !.e = NoEntry, !.ver = IF MaxVer = 0 THEN 1 - @ ELSE @ + 1, !.rb = 1]
  /\ UNCHANGED <<bodyVars, intents, wr, putVars, gc, req, ghostVars, trigVars>>

--------------------------------------------------------------------------------------
(* STORE: a GC write request applies. *)
ReqApply(q) ==
  /\ q \in req /\ ~q.ap
  /\ LET hit == q.k = "bd" /\ body.present /\ body.tok = q.tok
         res == CASE q.k = "ic" -> IF q.tok \in intents THEN "refused" ELSE "created"
                  [] q.k = "bd" -> IF hit THEN "removed" ELSE "miss"
                  [] OTHER -> "done"
     IN /\ body' = IF hit THEN NoBody ELSE body
        /\ gone' = IF hit THEN gone \cup {body.tok} ELSE gone
        /\ fOther' = (fOther \/ (hit /\ body.ser /= q.ser))
        /\ intents' = CASE q.k = "ic" -> intents \cup {q.tok}
                        [] q.k = "id" -> intents \ {q.tok}
                        [] OTHER -> intents
        /\ fI4' = (fI4 \/ (q.k = "id" /\ q.tok \in intents /\ body.present /\ body.tok = q.tok))
        /\ req' = IF q.o = NoG THEN req \ {q} ELSE (req \ {q}) \cup {[q EXCEPT !.ap = TRUE, !.res = res]}
  /\ ever' = IF q.k = "ic" THEN ever \cup {q.tok} ELSE ever
  /\ UNCHANGED <<nextSer, wr, putVars, gs, gc, fRecur, fAdopt>>
  /\ UNCHANGED <<dirty, dseen, touched, tpost, nf>>

--------------------------------------------------------------------------------------
Next ==
  \/ \E w \in Writers :
       \/ WPrecommit(w) \/ WPrime(w) \/ WHead(w) \/ WGetIntent(w)
       \/ WSendPut(w) \/ WPutOk(w) \/ WPutGiveUp(w) \/ WCommit(w) \/ WAbandon(w) \/ WDrop(w) \/ WLink(w)
  \/ \E q \in putq : PutLand(q)
  \/ \E q \in sentq : PutResend(q)
  \/ \E g \in GCs :
       \/ GPreCut(g) \/ GStart(g) \/ GReadAhead(g) \/ GCut(g)
       \/ GCondemn(g) \/ GAdoptOrphan(g) \/ GSpare(g) \/ GStandKeep(g) \/ GSupersede(g) \/ GCreateRetry(g)
       \/ GCreateSendFold(g) \/ GCreateAnswer(g) \/ GCreateUnanswered(g) \/ GCreateResolve(g) \/ GG7Delete(g)
       \/ GDelHead(g) \/ GDelAnswer(g) \/ GDelUnanswered(g) \/ GStandWork(g) \/ GIdAnswer(g) \/ GIdUnanswered(g)
       \/ GCommit(g) \/ GCommitLost(g) \/ GAbort(g)
  \/ \E q \in req : ReqApply(q)
  \/ RebuildState

Spec == Init /\ [][Next]_vars

(* Liveness runs: one GC actor, no symmetry, unbounded rounds. *)
Fair(A) == WF_vars(A)
Fairness ==
  /\ \A g \in GCs :
       /\ Fair(GStart(g)) /\ Fair(GCut(g)) /\ Fair(GCondemn(g)) /\ Fair(GAdoptOrphan(g)) /\ Fair(GSpare(g))
       /\ Fair(GStandKeep(g)) /\ Fair(GSupersede(g)) /\ Fair(GCreateRetry(g)) /\ Fair(GCreateSendFold(g))
       /\ Fair(GCreateAnswer(g)) /\ Fair(GCreateResolve(g)) /\ Fair(GG7Delete(g)) /\ Fair(GDelHead(g))
       /\ Fair(GDelAnswer(g)) /\ Fair(GStandWork(g)) /\ Fair(GIdAnswer(g)) /\ Fair(GCommit(g))
       /\ Fair(GCommitLost(g))
  /\ Fair(\E q \in req : ReqApply(q))
  /\ Fair(\E q \in putq : PutLand(q))
LiveSpec == Spec /\ Fairness

Symm == Permutations(Writers) \cup Permutations(GCs)

--------------------------------------------------------------------------------------
(* INVARIANTS, numbered as in section 4.1 of the specification. *)

Entries == [st : {"none", "cand", "pend", "stand"}, tok : 0..MaxTok, conf : BOOLEAN]

TypeOK ==
  /\ Sab \subseteq KnownSab /\ Late \subseteq {"put", "ic", "id", "bd"}
  /\ TokenKind \in {"Generation", "Content"} /\ Trigger \in {"any", "zero"}
  /\ Seed \in {"empty", "lost", "orphan", "cand", "pend", "spared", "orphans2"}
  /\ ((Trigger = "zero" \/ MaxVer = 0) => Cardinality(GCs) = 1)
  /\ body \in [present : BOOLEAN, tok : 0..MaxTok, ser : 0..MaxTok, pay : {"none", Good, Bad}]
  /\ intents \subseteq Tokens /\ gone \subseteq Tokens /\ ever \subseteq Tokens
  /\ nsend \in 0..MaxPuts /\ nres \in 0..MaxResends
  /\ \A w \in Writers :
       /\ wr[w].ph \in {"Idle", "H1", "G", "Pub", "Wait", "Ready", "Done"}
       /\ wr[w].seen \in 0..MaxTok /\ wr[w].rd \in 0..9 /\ wr[w].pt \in 0..3
  /\ gs.e \in Entries /\ gs.ver \in 0..(IF MaxVer = 0 THEN 1 ELSE MaxVer)
  /\ \A g \in GCs :
       /\ gc[g].ph \in {"Idle", "Pre", "Read", "Work", "Work2", "ICr", "CrWait", "CrRes", "IdSend",
                        "DelWait", "IdWait", "Commit"}
       /\ gc[g].base \in Entries /\ gc[g].next \in Entries
       /\ Cardinality(OwnReq(g)) <= 1
  /\ \A q \in req : q.k \in {"ic", "id", "bd"} /\ q.o \in GCs \cup {NoG}

(* I1. Once `t` stops being the incarnation of B, it never is again. *)
INV_I1_NO_RECURRENCE == ~fRecur

(* I2. Every body ever at B has the payload that hashes to B. *)
INV_I2_KEY_NAMES_PAYLOAD == body.present => body.pay = Good

(* I3. A writer observes B only after its edge is durable (edges are the non-idle slots). *)
INV_I3_EDGE_BEFORE_OBSERVE ==
  \A w \in Writers : (wr[w].seen /= 0 \/ wr[w].ph \in {"G", "Pub", "Wait", "Ready"}) => wr[w].ph /= "Idle"

(* I4. No intent DELETE is sent, and none applies on an existing intent, while `t` is the
   incarnation of B. *)
INV_I4_INTENT_PERMANENT == ~fI4

(* I5. Every body DELETE follows a confirmed create, the commit of pending(t), and a cut taken from
   that committed state with in-degree 0; its `HEAD` showed `t` (the request names that landing). *)
INV_I5_DELETE_EVIDENCE == \A q \in req : q.k = "bd" => q.ok

(* A3: a conditional delete removes only the landing its `HEAD` observed. *)
INV_A3_DELETE_IS_EXACT == ~fOther

(* I6. An adoption follows a W4 answer from the store, sent after the edge. *)
INV_I6_ADOPTION == ~fAdopt

(* I8, the goal. A committed ref never names a blob key with no body. *)
INV_I8_COMMITTED_REF_HAS_BODY == \A w \in Writers : wr[w].ph = "Done" => body.present

(* Writer requests per attempt on the successful path. `ga` counts unanswered publications. *)
INV_REQUEST_BUDGET ==
  \A w \in Writers :
    (Budget /\ wr[w].ph = "Ready") =>
      CASE wr[w].path = "new" -> wr[w].rd = 1 /\ wr[w].pt = 1
        [] wr[w].path = "reuse" -> wr[w].rd = 2 /\ wr[w].pt = 0
        [] wr[w].path = "condemned" -> wr[w].rd = 2 /\ wr[w].pt = 1
        [] wr[w].path = "amb_new_reuse" -> wr[w].rd = 3 /\ wr[w].pt = 1
        [] OTHER -> wr[w].rd <= 2 * (wr[w].ga + 1) /\ wr[w].pt <= wr[w].ga + 1

(* GC requests of one round for B, at its commit: `h` HEADs, `ic` creates, `id` intent deletes,
   `bd` body deletes, `rr` resolve reads. *)
INV_GC_ROUND_BUDGET ==
  GBudget => \A g \in GCs : gc[g].ph = "Commit" =>
    LET b == gc[g].base  n == gc[g].next  c == gc[g].cnt IN
    CASE b.st = "stand" /\ n.st = "none" /\ ~gc[g].ref ->                     \* G8
           c.id = 1 /\ c.ic = 0 /\ c.bd = 0 /\ c.rr = 0 /\ c.h = (IF gc[g].ts THEN 1 ELSE 0)
      [] b.st /= "none" /\ n.st \in {"pend", "cand"} /\ n.tok /= b.tok ->     \* G7
           c.h = 1 /\ c.id = 1 /\ c.ic <= 1 /\ c.bd = 0 /\ c.rr <= 1
      [] b.st = "none" /\ n.st \in {"pend", "cand"} /\ c.ic = 1 ->             \* G2
           c.h = 1 /\ c.id = 0 /\ c.bd = 0 /\ c.rr <= 1
      [] b.st = "none" /\ n.st \in {"cand", "stand"} /\ c.ic = 0 ->            \* G9, or G2 over budget
           c.h = 1 /\ c.id = 0 /\ c.bd = 0 /\ c.rr = 0
      [] b.st = "pend" /\ n.st = "stand" ->                                    \* G4
           c.h = 1 /\ c.bd <= 1 /\ c.ic = 0 /\ c.id = 0
      [] b.st /= "none" /\ n.st = "none" /\ gc[g].ref ->                       \* G6
           c = Cnt0
      [] OTHER -> TRUE

(* WITNESSES: each is an invariant that must FAIL, showing the state is reachable. *)
WIT_TWO_READ_ADOPT_COMMITTED == \A w \in Writers : ~(wr[w].ph = "Done" /\ wr[w].path = "reuse" /\ gs.ver >= 1)
(* A writer between its `HEAD` = t and its `GET I(B,t)` while a G8 delete of I(B,t) is in flight. *)
WIT_TWO_READ_WRITER_AT_G8 ==
  \A w \in Writers : ~(wr[w].ph = "G" /\ wr[w].seen \in intents /\ wr[w].seen \in gone
                       /\ \E q \in req : q.k = "id" /\ q.tok = wr[w].seen /\ ~q.ap)
(* The body is removed by a round that read the commit of the round that condemned it. *)
WIT_BODY_DELETE_SECOND_ROUND ==
  ~(\E q \in req : q.k = "bd" /\ q.ap /\ q.res = "removed" /\ gs.ver = 1)
WIT_G7_FROM_STAND == \A g \in GCs : ~(gc[g].ph = "Commit" /\ gc[g].base.st = "stand" /\ gc[g].next.st = "pend"
                                       /\ gc[g].next.tok /= gc[g].base.tok)
WIT_G7_FROM_PEND == \A g \in GCs : ~(gc[g].ph = "Commit" /\ gc[g].base.st = "pend" /\ gc[g].next.st = "pend"
                                      /\ gc[g].next.tok /= gc[g].base.tok)
WIT_G7_FROM_CAND == \A g \in GCs : ~(gc[g].ph = "Commit" /\ gc[g].base.st = "cand" /\ gc[g].next.st \in {"pend", "cand"}
                                      /\ gc[g].next.tok /= gc[g].base.tok)
WIT_G8_NO_HEAD == \A g \in GCs : ~(gc[g].ph = "Commit" /\ gc[g].base.st = "stand" /\ gc[g].next.st = "none"
                                    /\ ~gc[g].ref /\ ~gc[g].ts /\ (GBudget => gc[g].cnt.h = 0))
(* G4 meets `Mismatch` and still records standing(t). *)
WIT_G4_MISMATCH_STANDING ==
  \A g \in GCs : ~(gc[g].ph = "Commit" /\ gc[g].base.st = "pend" /\ gc[g].next.st = "stand"
                   /\ body.present /\ body.tok /= gc[g].base.tok)
WIT_G9_CAND == \A g \in GCs : ~(gc[g].ph = "Commit" /\ gc[g].base.st = "none" /\ gc[g].next.st = "cand"
                                 /\ gc[g].cnt.ic = 0 /\ gc[g].next.tok \in intents /\ gs.ver >= 1)
WIT_G9_STAND == \A g \in GCs : ~(gc[g].ph = "Commit" /\ gc[g].base.st = "none" /\ gc[g].next.st = "stand")
WIT_G8_STALE_LISTED == \A g \in GCs : ~(gc[g].ph = "IdWait" /\ gc[g].base.st = "stand"
                                         /\ gc[g].base.tok \notin intents /\ gc[g].base.tok \in ever)
(* G6 drops a standing row over a re-created `u` while I(B,t) stays. *)
WIT_STANDING_DROPPED ==
  \A g \in GCs : ~(gc[g].ph = "Commit" /\ gc[g].base.st = "stand" /\ gc[g].ref /\ gc[g].next.st = "none"
                   /\ body.present /\ body.tok /= gc[g].base.tok /\ gc[g].base.tok \in intents)
WIT_REUSER_ADOPTS_U_OVER_OLD_INTENT ==
  \A w \in Writers : ~(wr[w].ph = "Done" /\ wr[w].path = "reuse" /\ body.present /\ body.tok \notin intents
                        /\ intents /= {})
WIT_DELETE_MISSES == ~(\E q \in req : q.k = "bd" /\ q.ap /\ q.res = "miss")
WIT_LINK == \A w \in Writers : ~(wr[w].ph = "Done" /\ wr[w].path = "link")
WIT_OVER_BUDGET_CAND == \A g \in GCs : ~(gc[g].ph = "Commit" /\ gc[g].next.st = "cand" /\ gc[g].cnt.ic = 0
                                          /\ gc[g].base.st = "none" /\ gc[g].next.tok \notin intents)

--------------------------------------------------------------------------------------
(* LIVENESS, once the writers are done for good. Not promised by section 8.2 for a body that lands
   after its writer's edge removal was folded (the late `PUT`). *)
Quiet == (\A w \in Writers : wr[w].ph = "Idle") /\ putq = {}
LIVE_UNREFERENCED_BODY_DELETED == <>[]Quiet => <>[](~body.present)
LIVE_NO_INTENT_LEFT == <>[]Quiet => <>[](intents = {})

(*
ABSTRACTIONS AND BOUNDS
  - One blob key, one row; a round takes at most one decision for it. Several keys, shards, batching
    across rows and read-ahead are absent.
  - A read is one atomic observation. A cut is complete and atomic (A7, assumed).
  - Trigger = "any" (safety): a round may see a fresh transition and a touched key at every cut, a
    superset of the code. Trigger = "zero" (liveness, one GC actor): both are exact.
  - A model round commits only when it changes the row; a real round commits every time.
  - "Destruction suppressed" is not modelled: every cut is complete.
  - The redelete budget of G4 is a round that ends for B with no commit (GAbort with Faults).
  - Not modelled: sizes, malformed intent bytes, a token that does not canonicalise, token spelling,
    multipart transport, `cas-fsck`, the migration.
*)
======================================================================================
