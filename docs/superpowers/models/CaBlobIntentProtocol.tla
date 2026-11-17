---------------------------- MODULE CaBlobIntentProtocol ----------------------------
(*
Model of docs/superpowers/specs/2026-11-17-cas-blob-deletion-intents-design.md: blob publication
without a birth marker, GC-owned immutable deletion intents keyed by the body incarnation,
write-ahead GC, and the one-time transition from the marker protocol.

One blob key B. Writers of the new and of the older build, GC actors of the new and of the older
build that race on one `gc/state`, `cas-fsck`, the operator's stamp and switch.

Every action names the rule of the specification it stands for (section and rule id). A rule that
is not in the specification is not in this model; a place where the specification says nothing and
the model follows the code is marked SILENT. Sabotage switches (`Sab`) each remove exactly one rule.

Request granularity. A read (`HEAD`, `GET`) is one atomic observation taken between its send and
its answer; the answer is used arbitrarily later. A write is three steps: send (the request joins
`putq` or `req`), apply (the store acts on it, whatever its sender does by now), respond (the
sender consumes the result). A sender may stop waiting at any moment; with the kind in `Late` the
request stays in flight and may apply later, otherwise it is never applied.

Abstractions and bounds are listed at the end of the file.
*)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
  Writers,        \* writer slots (model values); a slot is one mount of one server
  GCs,            \* GC actors (model values)
  OldGCs,         \* the GC actors that run the older build (always marker mode, stopped by the stamp)
  NoG,            \* "nobody waits" (model value)
  Start,          \* "T3": pool switched and stamped, every writer learned. "T1": floor 4, not stamped, not switched
  Seed,           \* initial objects, see Init
  Builds,         \* builds a writer slot may run: subset of {"new", "old"}
  TokenKind,      \* "Generation": unique per landing. "Content": derived from the envelope bytes
  MaxPuts,        \* body publications the writers may send in total
  MaxResends,     \* transport resends of a landed publication (sabotage of A1; 0 = one send)
  MaxVer,         \* `gc/state` round commits. 0: unbounded (one GC actor; liveness runs)
  Late,           \* kinds that stay in flight after the sender stopped waiting: "put", "ic", "id", "bd"
  Faults,         \* GC rounds may die at any step and GC requests may stay unanswered
  Trigger,        \* "any": GC may condemn whenever a cut shows in-degree 0. "zero": only when the cut folds
                  \* an edge withdrawal no committed round folded yet (the code's trigger; one GC actor)
  Fsck,           \* cas-fsck --fix stale-intents runs concurrently (section 3.4)
  Linker,         \* the no-bytes reference path runs (section 4.2, "No-bytes references")
  StagedCopy,     \* the verbatim staged copy exists and every writer slot is an alias of ONE staging object
  SourceChanges,  \* a writer's local source may differ from what was hashed at staging (section 6.3)
  Budget,         \* count the writer's requests (section 1, "Budget")
  Sab             \* sabotage switches

KnownSab ==
  {"no_second_head",                 \* W5 removed: adopt after W2 and an absent intent (S1)
   "alt_adopt_changed_token",        \* W5: adopt the other token it sees (section 4.3, considered and not adopted)
   "attempted_create_confirms",      \* G3: a sent create counts as a confirmation (S4)
   "absent_conflict_confirms",       \* G3: a resolve read that finds nothing, or fails, counts as a confirmation
   "delete_on_graduation_cut",       \* G3/G4: delete in the round that confirmed, on that round's cut
   "spare_deletes_intent",           \* G6/G9: the intent is deleted on a spare (S3)
   "ambiguous_delete_cleans_intent", \* G5: an unanswered body DELETE still leads to the intent DELETE (S9)
   "delete_despite_indegree",        \* G4/G6: a pending row is executed although the cut holds an edge
   "candidate_from_intent",          \* G9: GC derives a pending row from an intent it reads
   "writer_deletes_intent",          \* decision 1.1: the writer deletes the intent it met before it publishes
   "cleanup_head_first",             \* F2: fsck observes the body first, the intent second (S2)
   "cleanup_raw_spelling",           \* section 6.1: fsck compares raw tokens; one incarnation may render twice (S8)
   "cleanup_foreign_dialect",        \* F0: a fsck client of another dialect is admitted (S10)
   "gc_raw_spelling",                \* section 6.1: G4 and G7 compare raw tokens
   "stale_read",                     \* W8: W4 is answered from an observation made before the edge (S12)
   "resend_envelope",                \* A1: named for the run; the resend itself is MaxResends > 0
   "staging_alias_two_copies",       \* A1: the once-only flag of the staged copy is not shared by aliases
   "unchecked_stream",               \* section 6.3: a streaming publication is not hashed (S7)
   "adopt_unmarked",                 \* W3: a learned writer runs W4, W5 on an unmarked body
   "mark_before_learned",            \* section 3.1: the mark bit is set without the learned flag
   "no_stamp",                       \* section 7.3: the switch does not need the stamp
   "old_gc_after_stamp",             \* section 2.4: the older GC starts rounds on a stamped `gc/state`
   "lost_stamp_old_bootstrap",       \* X2: `gc/state` is deleted and an older build bootstraps it
   "mode_cached",                    \* G1: a GC actor keeps the mode of its first round
   "mode_on_failed_read",            \* G1: a failed `_pool_meta` read means marker mode (S11)
   "old_writer_after_switch",        \* A8: a writer of the older build starts a build after the switch (X1)
   "inherited_executed"}             \* G8: an inherited pending row is executed in intent mode

Has(s) == s \in Sab
LateOk(k) == k \in Late
Transition == Start = "T1"

MaxTok == MaxPuts + MaxResends + 2     \* seed token 1, publications, the staged envelope
StagedEnv == MaxTok                    \* envelope of the one staging object (minted once, A1)
Tokens == 1..MaxTok
Good == "P"
Bad == "X"

NoBody == [present |-> FALSE, tok |-> 0, ser |-> 0, pay |-> "none", mk |-> FALSE]

(* A condemned row of `gc/state` for B. `int` is the row's `intent` flag (section 2.3); a row with
   `int` is pending. `conf` is ghost: the confirmation of G3 was a real one. *)
NoEntry == [st |-> "none", tok |-> 0, int |-> FALSE, conf |-> FALSE]
Cand(t) == [st |-> "cand", tok |-> t, int |-> FALSE, conf |-> FALSE]
PendI(t, c) == [st |-> "pend", tok |-> t, int |-> TRUE, conf |-> c]
PendM(t) == [st |-> "pend", tok |-> t, int |-> FALSE, conf |-> FALSE]

VARIABLES
  body,      \* the object at B: [present, tok, ser, pay, mk]; `ser` names the physical landing, `mk` the mark
  nextSer,   \* landings so far + 1
  gone,      \* ghost: tokens that stopped being the incarnation of B
  intents,   \* tokens t with I(B,t) present
  meta,      \* the legacy marker `<blob>.meta`: "absent" | "clean" | "cond"
  pm,        \* `_pool_meta.blob_protocol = intents`
  wr,        \* per writer slot: the build in progress
  lrn,       \* per writer slot: learned (section 3.1)
  nextEnv, putq, sentq, nsend, nres,
  copied,    \* writer slots that used the staged copy (the once-only flag, A1)
  gs,        \* durable `gc/state`: [e : row, ver, gen4]
  gc,        \* per GC actor: its round in progress
  gmode,     \* per GC actor: the mode it cached (only under "mode_cached")
  req,       \* GC and fsck write requests in flight or applied and not yet consumed
  fs,        \* cas-fsck: [ph, tok]
  dirty,     \* the trigger of G2 (N:637-648): an edge was withdrawn and no committed round folded it yet.
             \* Kept only with Trigger = "zero": set by WAbandon and WDrop, folded by GCommit
  dseen,     \* the round in progress took its cut after that withdrawal (set by GCut)
  fRecur, fI4, fOther, fWMut, fAdopt, fMMA   \* ghost violation bits

bodyVars == <<body, nextSer, gone>>
sigVars == <<intents, meta>>
putVars == <<nextEnv, putq, sentq, nsend, nres, copied>>
gcVars == <<gs, gc, gmode>>
ghostVars == <<fRecur, fI4, fOther, fWMut, fAdopt, fMMA>>
trigVars == <<dirty, dseen>>
vars == <<bodyVars, sigVars, pm, wr, lrn, putVars, gcVars, req, fs, ghostVars, dirty, dseen>>

W0 == [ph |-> "Idle", b |-> "new", mk |-> FALSE, seen |-> 0, env |-> 0, landed |-> FALSE, cp |-> FALSE,
       c404 |-> 0, path |-> "none", rd |-> 0, pt |-> 0, ga |-> 0]

G0 == [ph |-> "Idle", base |-> NoEntry, bver |-> 0, bgen |-> FALSE, ref |-> FALSE, mode |-> "none",
       next |-> NoEntry]

Rq(k, o, t, s, ok, auth) ==
  [k |-> k, o |-> o, tok |-> t, ser |-> s, ok |-> ok, auth |-> auth, ap |-> FALSE, res |-> "none"]

Marked == Start = "T3" /\ Seed \in {"orphan", "cand", "pend", "spared"}
SeedBody == Seed /= "empty"

Init ==
  /\ body = IF SeedBody THEN [present |-> TRUE, tok |-> 1, ser |-> 1, pay |-> Good, mk |-> Marked] ELSE NoBody
  /\ nextSer = IF SeedBody THEN 2 ELSE 1
  /\ nextEnv = 2
  /\ gone = {}
  /\ intents = IF Seed \in {"pend", "spared"} THEN {1} ELSE {}
  /\ meta = CASE Seed = "legacy" -> "clean"
              [] Seed \in {"legacy_cand", "legacy_pend"} -> "cond"
              [] OTHER -> "absent"
  /\ pm = (Start = "T3")
  /\ wr = [w \in Writers |-> W0]
  /\ lrn = [w \in Writers |-> Start = "T3"]
  /\ putq = {} /\ sentq = {} /\ nsend = 0 /\ nres = 0 /\ copied = {}
  /\ gs = [e |-> CASE Seed \in {"cand", "legacy_cand"} -> Cand(1)
                   [] Seed = "pend" -> PendI(1, TRUE)
                   [] Seed = "legacy_pend" -> PendM(1)
                   [] OTHER -> NoEntry,
           ver |-> 0, gen4 |-> (Start = "T3")]
  /\ gc = [g \in GCs |-> G0]
  /\ gmode = [g \in GCs |-> "none"]
  /\ req = {}
  /\ fs = [ph |-> "Idle", tok |-> 0]
  /\ dirty = (Trigger = "zero" /\ Seed \in {"orphan", "spared", "legacy"}) /\ dseen = FALSE
  /\ fRecur = FALSE /\ fI4 = FALSE /\ fOther = FALSE /\ fWMut = FALSE /\ fAdopt = FALSE /\ fMMA = FALSE

--------------------------------------------------------------------------------------
(* Request counters of section 1, "Budget". They are not reset when the writer's loop iterates. *)
Rd(r) == IF Budget THEN [r EXCEPT !.rd = @ + 1] ELSE r
Pt(r) == IF Budget THEN [r EXCEPT !.pt = @ + 1] ELSE r
SetPath(r, p) ==
  IF ~Budget THEN r
  ELSE IF r.ga = 0 THEN [r EXCEPT !.path = p]
  ELSE IF r.path = "amb_new" /\ r.ga = 1 /\ p = "reuse" THEN [r EXCEPT !.path = "amb_new_reuse"]
  ELSE [r EXCEPT !.path = "amb"]

(* Canonical identity, section 6.1: every comparison is on the canonical form, so two renderings of
   one incarnation are equal. The sabotages compare raw tokens: an equal incarnation MAY read as
   another one. `Differs(sab)` is the set of answers "these two observations name different
   incarnations" that a comparison of two equal incarnations can give. *)
RawMayDiffer(sab) == IF Has(sab) THEN {FALSE, TRUE} ELSE {FALSE}

(* An intent DELETE is sent for `t`. Ghost for I4: `t` must not be the incarnation of B. *)
IdBad(t) == body.present /\ body.tok = t
IdReq(o, t) == Rq("id", o, t, 0, TRUE, "none")

(* Publications of `w` after `w` stopped waiting. Without "put" in Late an unapplied one is never applied. *)
DropPuts(w) == IF LateOk("put") THEN putq ELSE {q \in putq : q.w /= w}

(* A8: no older build starts protocol work after the switch. A build of the older writer whose edge
   was durable before the switch may go on and commit: the procedure of section 7.2 (step 3) is
   stricter and stops such a process, which is WAbandon here. The sabotage lets one start. *)
OldAlive == ~pm \/ Has("old_writer_after_switch")

--------------------------------------------------------------------------------------
(* WRITER, section 3.2. *)

(* W1: the precommit edge is durable before the first request. The mark bit is decided when the
   staging sink is created (section 3.1, "Mark bit"): it is the learned flag of that moment. *)
WPrecommit(w, b) ==
  /\ wr[w].ph = "Idle" /\ b \in Builds
  /\ (b = "old" => OldAlive)
  /\ wr' = [wr EXCEPT ![w] = [W0 EXCEPT !.ph = "H1", !.b = b, !.c404 = wr[w].c404,
                                         !.mk = (b = "new" /\ (lrn[w] \/ Has("mark_before_learned")))]]
  /\ UNCHANGED <<bodyVars, sigVars, pm, lrn, putVars, gcVars, req, fs, ghostVars>>
  /\ UNCHANGED trigVars

(* Section 3.1: the slot reads `_pool_meta` and finds `intents` with its own dialect, probe passed.
   Monotone. A failed read is "not learned": the step is simply not taken. *)
WLearn(w) ==
  /\ Transition /\ pm /\ ~lrn[w]
  /\ lrn' = [lrn EXCEPT ![w] = TRUE]
  /\ UNCHANGED <<bodyVars, sigVars, pm, wr, putVars, gcVars, req, fs, ghostVars>>
  /\ UNCHANGED trigVars

(* Sabotage of W8: an observation "I(B,t) absent" made before the edge exists is kept and reused. *)
WPrime(w) ==
  /\ Has("stale_read") /\ wr[w].ph = "Idle" /\ lrn[w]
  /\ body.present /\ body.mk /\ body.tok \notin intents /\ wr[w].c404 /= body.tok
  /\ wr' = [wr EXCEPT ![w].c404 = body.tok]
  /\ UNCHANGED <<bodyVars, sigVars, pm, lrn, putVars, gcVars, req, fs, ghostVars>>
  /\ UNCHANGED trigVars

(* W2: `HEAD B`. Absent -> W6, first-publication form.
   W3: body unmarked, or writer not learned -> W6, no other request.
   Older build (W:424-475): present -> read the marker. *)
WHead1(w) ==
  /\ wr[w].ph = "H1"
  /\ LET r == Rd(wr[w])
     IN wr' = [wr EXCEPT ![w] =
          IF ~body.present
            THEN [SetPath(r, "new") EXCEPT !.ph = "Pub", !.cp = StagedCopy]
          ELSE IF r.b = "old"
            THEN [r EXCEPT !.ph = "M", !.seen = body.tok]
          ELSE IF lrn[w] /\ (body.mk \/ Has("adopt_unmarked"))
            THEN [r EXCEPT !.ph = "G", !.seen = body.tok]
          ELSE [SetPath(r, "legacy") EXCEPT !.ph = "Pub", !.cp = FALSE]]
  /\ UNCHANGED <<bodyVars, sigVars, pm, lrn, putVars, gcVars, req, fs, ghostVars>>
  /\ UNCHANGED trigVars

(* W4: `GET I(B,t)` for the identity of W2. An object -> W6. An answered 404 -> W5. *)
WGetIntent(w) ==
  /\ wr[w].ph = "G"
  /\ LET cached == Has("stale_read") /\ wr[w].c404 = wr[w].seen
         r == IF cached THEN wr[w] ELSE Rd(wr[w])
     IN IF r.seen \in intents /\ ~cached
          THEN /\ wr' = [wr EXCEPT ![w] = [SetPath(r, "condemned") EXCEPT !.ph = "Pub", !.seen = 0, !.cp = FALSE]]
               /\ IF Has("writer_deletes_intent")
                    THEN /\ req' = req \cup {IdReq(NoG, r.seen)}
                         /\ fWMut' = TRUE
                         /\ fI4' = (fI4 \/ IdBad(r.seen))
                    ELSE UNCHANGED <<req, fWMut, fI4>>
               /\ UNCHANGED fAdopt
          ELSE /\ wr' = [wr EXCEPT ![w] =
                    IF Has("no_second_head")
                      THEN [SetPath(r, "reuse") EXCEPT !.ph = "Ready", !.seen = 0]
                      ELSE [r EXCEPT !.ph = "H2"]]
               /\ fAdopt' = (fAdopt \/ cached \/ Has("no_second_head"))
               /\ UNCHANGED <<req, fWMut, fI4>>
  /\ UNCHANGED <<bodyVars, sigVars, pm, lrn, putVars, gcVars, fs, fRecur, fOther, fMMA>>
  /\ UNCHANGED trigVars

(* W5: `HEAD B` again. Same canonical identity -> adopt. Absent or another identity -> W6, once,
   with no return to W2. I6 (ghost `fAdopt`): an adoption is of a marked body by a learned writer. *)
WHead2(w) ==
  /\ wr[w].ph = "H2"
  /\ LET r == Rd(wr[w])
         same == body.present /\ body.tok = r.seen
         alt == Has("alt_adopt_changed_token") /\ body.present /\ body.mk /\ ~same
     IN /\ wr' = [wr EXCEPT ![w] =
             IF same \/ alt
               THEN [SetPath(r, "reuse") EXCEPT !.ph = "Ready", !.seen = 0]
               ELSE [SetPath(r, "changed") EXCEPT !.ph = "Pub", !.seen = 0, !.cp = FALSE]]
        /\ fAdopt' = (fAdopt \/ ((same \/ alt) /\ (~body.mk \/ ~lrn[w])))
  /\ UNCHANGED <<bodyVars, sigVars, pm, lrn, putVars, gcVars, req, fs, fRecur, fI4, fOther, fWMut, fMMA>>
  /\ UNCHANGED trigVars

(* Older build: `GET <blob>.meta` after the HEAD. `Clean` adopts (W:446-452); anything else republishes. *)
WGetMetaOld(w) ==
  /\ wr[w].ph = "M"
  /\ wr' = [wr EXCEPT ![w] = IF meta = "clean" THEN [@ EXCEPT !.ph = "Ready", !.seen = 0]
                                               ELSE [@ EXCEPT !.ph = "Pub", !.seen = 0]]
  /\ UNCHANGED <<bodyVars, sigVars, pm, lrn, putVars, gcVars, req, fs, ghostVars>>
  /\ UNCHANGED trigVars

(* The once-only rule of the verbatim staged copy (A1): hardlink copies of one staging object share
   the flag. The sabotage gives every alias its own flag. *)
CopyAllowed(w) == IF Has("staging_alias_two_copies") THEN w \notin copied ELSE copied = {}

(* W6: one unconditional publication, sent once, marked iff the mark bit is set.
   Streaming: a fresh envelope per attempt (A1), the payload is hashed (section 6.3): a source that
   no longer hashes to the key is refused before the object completes, so no request exists (the
   writer's exception is WAbandon). Verbatim staged copy: only as the first publication of the
   staging object and only after W2 found no body; it carries the staged envelope, is not hashed and
   rests on A6. *)
WSendPut(w) ==
  /\ wr[w].ph = "Pub" /\ nsend < MaxPuts
  /\ \E copy \in (IF wr[w].cp /\ CopyAllowed(w) THEN {TRUE, FALSE} ELSE {FALSE}) :
     \E p \in (IF SourceChanges /\ ~copy /\ Has("unchecked_stream") THEN {Good, Bad} ELSE {Good}) :
       LET e == IF copy THEN StagedEnv ELSE nextEnv
       IN /\ putq' = putq \cup {[w |-> w, env |-> e, pay |-> p, mk |-> wr[w].mk]}
          /\ nextEnv' = IF copy THEN nextEnv ELSE nextEnv + 1
          /\ copied' = IF copy THEN copied \cup {w} ELSE copied
          /\ wr' = [wr EXCEPT ![w] = [Pt(@) EXCEPT !.ph = "Wait", !.env = e, !.landed = FALSE]]
  /\ nsend' = nsend + 1
  /\ UNCHANGED <<bodyVars, sigVars, pm, lrn, sentq, nres, gcVars, req, fs, ghostVars>>
  /\ UNCHANGED trigVars

TokOf(e) == IF TokenKind = "Generation" THEN nextSer ELSE e

LandBody(e, p, m) ==
  LET t == TokOf(e)
      stays == body.present /\ body.tok = t
  IN /\ body' = [present |-> TRUE, tok |-> t, ser |-> nextSer, pay |-> p, mk |-> m]
     /\ nextSer' = nextSer + 1
     /\ fRecur' = (fRecur \/ t \in gone)
     /\ gone' = IF body.present /\ ~stays THEN gone \cup {body.tok} ELSE gone

(* Store: a publication applies, whatever its sender does by now. It replaces the object and its mark. *)
PutLand(q) ==
  /\ q \in putq
  /\ LandBody(q.env, q.pay, q.mk)
  /\ putq' = putq \ {q}
  /\ sentq' = IF MaxResends > 0 THEN sentq \cup {q} ELSE sentq
  /\ wr' = IF wr[q.w].ph = "Wait" /\ wr[q.w].env = q.env THEN [wr EXCEPT ![q.w].landed = TRUE] ELSE wr
  /\ UNCHANGED <<sigVars, pm, lrn, nextEnv, nsend, nres, copied, gcVars, req, fs, fI4, fOther, fWMut, fAdopt, fMMA>>
  /\ UNCHANGED trigVars

(* Sabotage of A1: the transport reissues a request that was already applied. *)
PutResend(q) ==
  /\ q \in sentq /\ nres < MaxResends
  /\ LandBody(q.env, q.pay, q.mk)
  /\ nres' = nres + 1
  /\ UNCHANGED <<sigVars, pm, wr, lrn, nextEnv, putq, sentq, nsend, copied, gcVars, req, fs,
                 fI4, fOther, fWMut, fAdopt, fMMA>>
  /\ UNCHANGED trigVars

(* W6, answered success -> `Materialized`. W7: no request follows.
   Older build: it then writes `Clean` (W:368, :542); one step with the answer. *)
WPutOk(w) ==
  /\ wr[w].ph = "Wait" /\ wr[w].landed
  /\ wr' = [wr EXCEPT ![w] = [@ EXCEPT !.ph = "Ready", !.env = 0, !.landed = FALSE]]
  /\ meta' = IF wr[w].b = "old" THEN "clean" ELSE meta
  /\ UNCHANGED <<bodyVars, intents, pm, lrn, putVars, gcVars, req, fs, ghostVars>>
  /\ UNCHANGED trigVars

(* W6, unanswered: the next loop iteration starts at W2. The request may have been applied, and
   with "put" in Late may still apply. *)
WPutGiveUp(w) ==
  /\ wr[w].ph = "Wait"
  /\ wr' = [wr EXCEPT ![w] =
       [@ EXCEPT !.ph = "H1", !.env = 0, !.landed = FALSE, !.cp = FALSE,
                 !.path = IF ~Budget THEN "none" ELSE IF @ = "new" /\ wr[w].ga = 0 THEN "amb_new" ELSE "amb",
                 !.ga = IF Budget /\ @ < 2 THEN @ + 1 ELSE @]]
  /\ putq' = DropPuts(w)
  /\ UNCHANGED <<bodyVars, sigVars, pm, lrn, nextEnv, sentq, nsend, nres, copied, gcVars, req, fs, ghostVars>>
  /\ UNCHANGED trigVars

(* I3: commit only while the precommit is the live owner. *)
WCommit(w) ==
  /\ wr[w].ph = "Ready"
  /\ wr' = [wr EXCEPT ![w] = [@ EXCEPT !.ph = "Done", !.rd = 0, !.pt = 0, !.ga = 0,
                                        !.path = IF @ = "reuse" THEN "reuse" ELSE "none"]]
  /\ UNCHANGED <<bodyVars, sigVars, pm, lrn, putVars, gcVars, req, fs, ghostVars>>
  /\ UNCHANGED trigVars

(* The build ends before its commit and its edge is withdrawn: an exception at any step, a refused
   read, a digest mismatch, or the death of the process followed by the cleanup of its precommit.
   A process that died keeps its edge until this step; a stalled phase stands for that interval. *)
WAbandon(w) ==
  /\ wr[w].ph \notin {"Idle", "Done"}
  /\ wr' = [wr EXCEPT ![w] = [W0 EXCEPT !.c404 = wr[w].c404]]
  /\ putq' = DropPuts(w)
  /\ UNCHANGED <<bodyVars, sigVars, pm, lrn, nextEnv, sentq, nsend, nres, copied, gcVars, req, fs, ghostVars>>
  /\ dirty' = (dirty \/ Trigger = "zero") /\ dseen' = FALSE

(* The committed part is dropped: its edge is withdrawn. *)
WDrop(w) ==
  /\ wr[w].ph = "Done"
  /\ wr' = [wr EXCEPT ![w] = [W0 EXCEPT !.c404 = wr[w].c404]]
  /\ UNCHANGED <<bodyVars, sigVars, pm, lrn, putVars, gcVars, req, fs, ghostVars>>
  /\ dirty' = (dirty \/ Trigger = "zero") /\ dseen' = FALSE

(* Section 4.2, "No-bytes references" (`adoptEvidence`): an edge is added with no request while a
   source edge is live. The source is a committed ref of another slot. *)
WLink(w) ==
  /\ Linker /\ wr[w].ph = "Idle"
  /\ \E v \in Writers \ {w} : wr[v].ph = "Done"
  /\ wr' = [wr EXCEPT ![w] = [W0 EXCEPT !.ph = "Done", !.path = "link", !.c404 = wr[w].c404]]
  /\ UNCHANGED <<bodyVars, sigVars, pm, lrn, putVars, gcVars, req, fs, ghostVars>>
  /\ UNCHANGED trigVars

--------------------------------------------------------------------------------------
(* GC, section 3.3. One round: lease (read `gc/state`), mode (G1), cut, the fold and the delete
   batch, one commit by compare-and-swap. Deletes are sent before the commit. An actor whose commit
   loses is the deposed leader; what it sent stays sent. *)

HasEdge == \E w \in Writers : wr[w].ph /= "Idle"
IsOld(g) == g \in OldGCs
OwnReq(g) == {q \in req : q.o = g}
NoOwned(g) == OwnReq(g) = {}

(* The sender stops waiting. An applied request is done; an unapplied one stays in flight only with
   its kind in Late, otherwise it is never applied. *)
StopWaiting(g, k) ==
  {q \in req : q.o /= g}
    \cup (IF LateOk(k) THEN {[q EXCEPT !.o = NoG] : q \in {x \in OwnReq(g) : ~x.ap}} ELSE {})

(* The lease read-modify-write of `gc/state` starts the round. The older build cannot decode a
   stamped state (section 2.4): no round starts. *)
GStart(g) ==
  /\ gc[g].ph = "Idle"
  /\ (IsOld(g) => (~gs.gen4 \/ Has("old_gc_after_stamp")))
  /\ gc' = [gc EXCEPT ![g] = [G0 EXCEPT !.ph = IF Transition THEN "Mode" ELSE "Read",
                                         !.mode = IF Transition THEN "none" ELSE "intent",
                                         !.base = gs.e, !.bver = gs.ver, !.bgen = gs.gen4]]
  /\ UNCHANGED <<bodyVars, sigVars, pm, wr, lrn, putVars, gs, gmode, req, fs, ghostVars>>
  /\ UNCHANGED trigVars

(* G1: after the lease and before any other step the round reads `_pool_meta`. No `blob_protocol`
   -> marker mode for this round. `intents` -> intent mode; it requires `gc/state` at generation 4,
   else the round fails. A failed read fails the round (GAbort). The older build never reads it.
   A marker-mode round that reads a row with `intent` fails (section 3.6). *)
GMode(g) ==
  /\ gc[g].ph = "Mode"
  /\ \E m \in (IF IsOld(g) THEN {"marker"}
               ELSE IF Has("mode_cached") /\ gmode[g] /= "none" THEN {gmode[g]}
               ELSE IF pm THEN (IF Has("mode_on_failed_read") THEN {"intent", "marker"} ELSE {"intent"})
               ELSE {"marker"}) :
       /\ gc' = [gc EXCEPT ![g] =
            IF (m = "intent" /\ ~gc[g].bgen) \/ (m = "marker" /\ gc[g].base.int)
              THEN G0                                       \* the round fails; the next one reads again
              ELSE [@ EXCEPT !.ph = "Read", !.mode = m]]
       /\ gmode' = IF Has("mode_cached") THEN [gmode EXCEPT ![g] = m] ELSE gmode
  /\ UNCHANGED <<bodyVars, sigVars, pm, wr, lrn, putVars, gs, req, fs, ghostVars>>
  /\ UNCHANGED trigVars

(* The round's complete reference cut (A7), after the lease and the mode. *)
GCut(g) ==
  /\ gc[g].ph = "Read"
  /\ gc' = [gc EXCEPT ![g] = [@ EXCEPT !.ph = "Work", !.ref = HasEdge]]
  /\ UNCHANGED <<bodyVars, sigVars, pm, wr, lrn, putVars, gs, gmode, req, fs, ghostVars>>
  /\ dseen' = dirty /\ UNCHANGED dirty

IntentMode(g) == gc[g].mode = "intent"
(* G8: in intent mode a pending row without `intent` is a candidate. *)
Demoted(g) == IntentMode(g) /\ gc[g].base.st = "pend" /\ ~gc[g].base.int /\ ~Has("inherited_executed")
IsCand(g) == gc[g].base.st = "cand" \/ Demoted(g)
IsPend(g) == gc[g].base.st = "pend" /\ ~Demoted(g)

(* G2: no row, the cut shows in-degree 0. `HEAD B`: present -> a non-pending row with the observed
   token; absent -> no row. Marker mode also writes `Condemned` (G:2227).
   Sabotage of G9: a row is derived from an intent the round reads. *)
GCondemn(g) ==
  /\ gc[g].ph = "Work" /\ gc[g].base.st = "none"
  /\ IF ~gc[g].ref /\ body.present /\ (Trigger = "any" \/ dseen)
       THEN /\ gc' = [gc EXCEPT ![g] = [@ EXCEPT !.ph = "Commit", !.next =
                        IF Has("candidate_from_intent") /\ IntentMode(g) /\ body.tok \in intents
                          THEN PendI(body.tok, TRUE) ELSE Cand(body.tok)]]
            /\ meta' = IF IntentMode(g) THEN meta ELSE "cond"
       ELSE /\ gc' = [gc EXCEPT ![g] = G0]
            /\ meta' = meta
  /\ UNCHANGED <<bodyVars, intents, pm, wr, lrn, putVars, gs, gmode, req, fs, ghostVars>>
  /\ UNCHANGED trigVars

(* G6: a row whose cut shows in-degree above 0 is dropped. No request; an intent stays (G9). *)
GSpare(g) ==
  /\ gc[g].ph = "Work" /\ gc[g].base.st /= "none" /\ gc[g].ref
  /\ ~(Has("delete_despite_indegree") /\ IsPend(g))
  /\ gc' = [gc EXCEPT ![g] = [@ EXCEPT !.ph = "Commit", !.next = NoEntry]]
  /\ IF Has("spare_deletes_intent") /\ gc[g].base.int
       THEN /\ req' = req \cup {IdReq(NoG, gc[g].base.tok)}
            /\ fI4' = (fI4 \/ IdBad(gc[g].base.tok))
       ELSE UNCHANGED <<req, fI4>>
  /\ UNCHANGED <<bodyVars, sigVars, pm, wr, lrn, putVars, gs, gmode, fs, fRecur, fOther, fWMut, fAdopt, fMMA>>
  /\ UNCHANGED trigVars

(* G7: a touched row at in-degree 0 whose `HEAD` shows another canonical identity `u` becomes a
   candidate for (B,u). If the old row had `intent`: one delete of I(B,t), best effort (it is in
   flight and nobody waits; it may never apply).
   SILENT: G7 names only "another identity". A row whose `HEAD` shows no body, or the same identity,
   is settled by the other rules, as in the code (N:612-631). *)
GSupersede(g) ==
  /\ gc[g].ph = "Work" /\ gc[g].base.st /= "none" /\ ~gc[g].ref
  /\ body.present
  /\ \E d \in (IF body.tok /= gc[g].base.tok THEN {TRUE} ELSE RawMayDiffer("gc_raw_spelling")) :
       /\ d
       /\ gc' = [gc EXCEPT ![g] = [@ EXCEPT !.ph = "Commit", !.next = Cand(body.tok)]]
       /\ IF gc[g].base.int /\ IntentMode(g)
            THEN /\ req' = req \cup {IdReq(NoG, gc[g].base.tok)}
                 /\ fI4' = (fI4 \/ IdBad(gc[g].base.tok))
            ELSE UNCHANGED <<req, fI4>>
  /\ UNCHANGED <<bodyVars, sigVars, pm, wr, lrn, putVars, gs, gmode, fs, fRecur, fOther, fWMut, fAdopt, fMMA>>
  /\ UNCHANGED trigVars

(* G3, the create: a row an earlier round committed, not pending (or demoted by G8), in-degree 0 in
   this round's cut: `PUT I(B,t) If-None-Match: *` is sent. *)
GCreateSend(g) ==
  /\ gc[g].ph = "Work" /\ IntentMode(g) /\ IsCand(g) /\ ~gc[g].ref
  /\ IF Has("attempted_create_confirms")
       THEN /\ req' = req \cup {Rq("ic", NoG, gc[g].base.tok, 0, TRUE, "none")}
            /\ gc' = [gc EXCEPT ![g] = [@ EXCEPT !.ph = "Commit", !.next = PendI(gc[g].base.tok, FALSE)]]
       ELSE /\ req' = req \cup {Rq("ic", g, gc[g].base.tok, 0, TRUE, "none")}
            /\ gc' = [gc EXCEPT ![g] = [@ EXCEPT !.ph = "CrWait"]]
  /\ UNCHANGED <<bodyVars, sigVars, pm, wr, lrn, putVars, gs, gmode, fs, ghostVars>>
  /\ UNCHANGED trigVars

(* G3, confirmed: the row gets `pending` and `intent` in this round's commit; nothing is deleted in
   this round. The sabotage goes on to G4 on this round's cut. *)
Confirmed(g, real) ==
  [gc[g] EXCEPT !.ph = IF Has("delete_on_graduation_cut") THEN "Work2" ELSE "Commit",
                !.next = PendI(gc[g].base.tok, real)]
(* G3, not confirmed: the row is carried unchanged; the round has nothing else to commit for B. *)
Carried == G0

(* G3, the create answered. Success -> `Committed`: confirmed. A clean refusal -> `Conflict`: the
   occupant must be read (Q:1118-1119, :1154-1158). *)
GCreateAnswer(g) ==
  /\ gc[g].ph = "CrWait"
  /\ \E q \in OwnReq(g) :
       /\ q.ap
       /\ req' = req \ {q}
       /\ gc' = [gc EXCEPT ![g] = IF q.res = "created" THEN Confirmed(g, TRUE) ELSE [@ EXCEPT !.ph = "CrRes"]]
  /\ UNCHANGED <<bodyVars, sigVars, pm, wr, lrn, putVars, gs, gmode, fs, ghostVars>>
  /\ UNCHANGED trigVars

(* G3, the create unanswered: the attempt is resolved by a read (Q:1127-1128). *)
GCreateUnanswered(g) ==
  /\ Faults /\ gc[g].ph = "CrWait"
  /\ req' = StopWaiting(g, "ic")
  /\ gc' = [gc EXCEPT ![g] = [@ EXCEPT !.ph = "CrRes"]]
  /\ UNCHANGED <<bodyVars, sigVars, pm, wr, lrn, putVars, gs, gmode, fs, ghostVars>>
  /\ UNCHANGED trigVars

(* G3, the resolve read, its own request. An occupant with the canonical bytes confirms.
   `ProvenAbsent` does not; neither does a read that fails (`NotObserved`, GaveUp). *)
GCreateResolve(g) ==
  /\ gc[g].ph = "CrRes"
  /\ \E observed \in (IF Faults THEN {TRUE, FALSE} ELSE {TRUE}) :
       gc' = [gc EXCEPT ![g] =
         IF observed /\ gc[g].base.tok \in intents THEN Confirmed(g, TRUE)
         ELSE IF Has("absent_conflict_confirms") THEN Confirmed(g, FALSE)
         ELSE Carried]
  /\ UNCHANGED <<bodyVars, sigVars, pm, wr, lrn, putVars, gs, gmode, req, fs, ghostVars>>
  /\ UNCHANGED trigVars

(* Marker mode, today's graduation gate (G:2264-2287): a `Condemned` marker read publishes pending;
   anything else rewrites the marker and carries the row. *)
GGraduateMarker(g) ==
  /\ gc[g].ph = "Work" /\ ~IntentMode(g) /\ gc[g].base.st = "cand" /\ ~gc[g].ref
  /\ IF meta = "cond"
       THEN gc' = [gc EXCEPT ![g] = [@ EXCEPT !.ph = "Commit", !.next = PendM(gc[g].base.tok)]] /\ meta' = meta
       ELSE gc' = [gc EXCEPT ![g] = G0] /\ meta' = "cond"
  /\ UNCHANGED <<bodyVars, intents, pm, wr, lrn, putVars, gs, gmode, req, fs, ghostVars>>
  /\ UNCHANGED trigVars

(* G4 (1), (2): a row read with `pending` and `intent` from the adopted state, after this round's
   cut showed in-degree 0. `HEAD B`. Absent -> `Gone`. Same canonical identity -> `DELETE B If-Match`
   is sent. Another identity -> `Mismatch`, no body request. (3): after an answered outcome the
   intent DELETE is sent.
   Marker mode (today, G:583-640): the same on a pending row, with the marker in place of the intent.
   Ghosts: `ok` is I5 (confirmed intent, committed pending row, later cut at in-degree 0, this HEAD);
   `fMMA` is T3 (a): a marked token under marker authority. *)
GDelHead(g) ==
  /\ \/ gc[g].ph = "Work" /\ IsPend(g) /\ (~gc[g].ref \/ Has("delete_despite_indegree"))
     \/ gc[g].ph = "Work2"
  /\ LET row == IF gc[g].ph = "Work2" THEN gc[g].next ELSE gc[g].base
         im == IntentMode(g)
         okv == gc[g].ph = "Work" /\ row.int /\ row.conf /\ ~gc[g].ref
     IN \E d \in (IF body.present /\ body.tok = row.tok THEN RawMayDiffer("gc_raw_spelling") ELSE {TRUE}) :
        IF ~d
          THEN /\ req' = req \cup {Rq("bd", g, row.tok, body.ser, okv, IF im THEN "intent" ELSE "marker")}
               /\ gc' = [gc EXCEPT ![g] = [@ EXCEPT !.ph = "DelWait", !.next = row]]
               /\ fMMA' = (fMMA \/ (body.mk /\ ~(row.int /\ row.conf)))
               /\ UNCHANGED <<fI4, meta>>
          ELSE IF im
            THEN /\ req' = req \cup {IdReq(g, row.tok)}
                 /\ gc' = [gc EXCEPT ![g] = [@ EXCEPT !.ph = "IdWait", !.next = row]]
                 /\ fI4' = (fI4 \/ IdBad(row.tok))
                 /\ UNCHANGED <<fMMA, meta>>
            ELSE /\ gc' = [gc EXCEPT ![g] = [@ EXCEPT !.ph = "Commit", !.next = NoEntry]]
                 /\ meta' = IF body.present THEN meta ELSE "absent"
                 /\ UNCHANGED <<req, fI4, fMMA>>
  /\ UNCHANGED <<bodyVars, intents, pm, wr, lrn, putVars, gs, gmode, fs, fRecur, fOther, fWMut, fAdopt>>
  /\ UNCHANGED trigVars

(* G4 (2) answered: `Removed`, `Gone` or `Mismatch`. (3): the intent DELETE is sent. (4): a removed
   unmarked body takes its marker with it, best effort. Marker mode: the marker is deleted and the
   row dropped. *)
GDelAnswer(g) ==
  /\ gc[g].ph = "DelWait"
  /\ \E q \in OwnReq(g) :
       /\ q.ap
       /\ IF IntentMode(g)
            THEN /\ req' = (req \ {q}) \cup {IdReq(g, gc[g].next.tok)}
                 /\ gc' = [gc EXCEPT ![g] = [@ EXCEPT !.ph = "IdWait"]]
                 /\ fI4' = (fI4 \/ IdBad(gc[g].next.tok))
                 /\ \E m \in (IF q.res = "removed_unmarked" THEN {meta, "absent"} ELSE {meta}) : meta' = m
            ELSE /\ req' = req \ {q}
                 /\ gc' = [gc EXCEPT ![g] = [@ EXCEPT !.ph = "Commit", !.next = NoEntry]]
                 /\ meta' = IF q.res = "miss" THEN meta ELSE "absent"
                 /\ UNCHANGED fI4
  /\ UNCHANGED <<bodyVars, intents, pm, wr, lrn, putVars, gs, gmode, fs, fRecur, fOther, fWMut, fAdopt, fMMA>>
  /\ UNCHANGED trigVars

(* G5: a body DELETE without an answer fails the round before its commit; the row stays pending and
   the intent DELETE is not sent. The sabotage sends it. *)
GDelUnanswered(g) ==
  /\ Faults /\ gc[g].ph = "DelWait"
  /\ IF Has("ambiguous_delete_cleans_intent") /\ IntentMode(g)
       THEN /\ req' = StopWaiting(g, "bd") \cup {IdReq(g, gc[g].next.tok)}
            /\ gc' = [gc EXCEPT ![g] = [@ EXCEPT !.ph = "IdWait"]]
            /\ fI4' = (fI4 \/ IdBad(gc[g].next.tok))
       ELSE /\ req' = StopWaiting(g, "bd")
            /\ gc' = [gc EXCEPT ![g] = G0]
            /\ UNCHANGED fI4
  /\ UNCHANGED <<bodyVars, sigVars, pm, wr, lrn, putVars, gs, gmode, fs, fRecur, fOther, fWMut, fAdopt, fMMA>>
  /\ UNCHANGED trigVars

(* G4 (3) answered, an absent key is success. (5): the row leaves the state, in this round's commit. *)
GIdAnswer(g) ==
  /\ gc[g].ph = "IdWait"
  /\ \E q \in OwnReq(g) :
       /\ q.ap
       /\ req' = req \ {q}
       /\ gc' = [gc EXCEPT ![g] = [@ EXCEPT !.ph = "Commit", !.next = NoEntry]]
  /\ UNCHANGED <<bodyVars, sigVars, pm, wr, lrn, putVars, gs, gmode, fs, ghostVars>>
  /\ UNCHANGED trigVars

(* G5: an intent DELETE without an answer fails the round; the row stays pending. *)
GIdUnanswered(g) ==
  /\ Faults /\ gc[g].ph = "IdWait"
  /\ req' = StopWaiting(g, "id")
  /\ gc' = [gc EXCEPT ![g] = G0]
  /\ UNCHANGED <<bodyVars, sigVars, pm, wr, lrn, putVars, gs, gmode, fs, ghostVars>>
  /\ UNCHANGED trigVars

(* An actor that only has its commit left. Once another commit moves the version its
   compare-and-swap can only fail, and failing has no effect, so the winner's step ends it too. *)
OnlyCommitLeft(h) == gc[h].ph = "Commit"

(* The round commit: a compare-and-swap of `gc/state` on the body the lease read (G:1326). The stamp
   rewrites the object, so a round that read the unstamped state loses (section 7.3). An applied
   commit whose answer is lost has the same effect: the actor starts over from the committed state. *)
GCommit(g) ==
  /\ gc[g].ph = "Commit" /\ gc[g].bver = gs.ver /\ gc[g].bgen = gs.gen4 /\ (MaxVer = 0 \/ gs.ver < MaxVer)
  /\ gs' = [gs EXCEPT !.e = gc[g].next, !.ver = IF MaxVer = 0 THEN 1 - @ ELSE @ + 1]
  /\ gc' = [h \in GCs |-> IF h = g \/ OnlyCommitLeft(h) THEN G0 ELSE gc[h]]
  /\ UNCHANGED <<bodyVars, sigVars, pm, wr, lrn, putVars, gmode, req, fs, ghostVars>>
  /\ dirty' = (dirty /\ ~dseen) /\ dseen' = FALSE

(* The commit loses its compare-and-swap: another round committed, or the stamp rewrote the object.
   The round ends; what it sent stays sent. *)
GCommitLost(g) ==
  /\ gc[g].ph = "Commit" /\ (gc[g].bver /= gs.ver \/ gc[g].bgen /= gs.gen4)
  /\ gc' = [gc EXCEPT ![g] = G0]
  /\ UNCHANGED <<bodyVars, sigVars, pm, wr, lrn, putVars, gs, gmode, req, fs, ghostVars>>
  /\ UNCHANGED trigVars

(* The round ends without a commit: the process died, the lease was lost, or a read failed (G1, G2,
   G7 in section 3.6). Requests it sent are still sent. *)
GAbort(g) ==
  /\ Faults
  /\ gc[g].ph \in {"Mode", "Read", "Work", "Work2", "CrRes", "Commit"}
  /\ gc' = [gc EXCEPT ![g] = G0]
  /\ UNCHANGED <<bodyVars, sigVars, pm, wr, lrn, putVars, gs, gmode, req, fs, ghostVars>>
  /\ UNCHANGED trigVars

--------------------------------------------------------------------------------------
(* STORE: a GC or fsck write request applies. *)
ReqApply(q) ==
  /\ q \in req /\ ~q.ap
  /\ LET hit == q.k = "bd" /\ body.present /\ body.tok = q.tok
         res == CASE q.k = "ic" -> IF q.tok \in intents THEN "refused" ELSE "created"
                  [] q.k = "bd" -> IF hit THEN (IF body.mk THEN "removed" ELSE "removed_unmarked") ELSE "miss"
                  [] OTHER -> "done"
     IN /\ body' = IF hit THEN NoBody ELSE body
        /\ gone' = IF hit THEN gone \cup {body.tok} ELSE gone
        /\ fOther' = (fOther \/ (hit /\ body.ser /= q.ser))
        /\ intents' = CASE q.k = "ic" -> intents \cup {q.tok}
                        [] q.k = "id" -> intents \ {q.tok}
                        [] OTHER -> intents
        /\ fI4' = (fI4 \/ (q.k = "id" /\ q.tok \in intents /\ body.present /\ body.tok = q.tok))
        /\ req' = IF q.o = NoG THEN req \ {q} ELSE (req \ {q}) \cup {[q EXCEPT !.ap = TRUE, !.res = res]}
  /\ UNCHANGED <<nextSer, meta, pm, wr, lrn, putVars, gcVars, fs, fRecur, fWMut, fAdopt, fMMA>>
  /\ UNCHANGED trigVars

--------------------------------------------------------------------------------------
(* CAS-FSCK, section 3.4. One intent at a time; runs repeat for ever. F1: the listing only nominates. *)

(* The delete of F3: write-once verb, no leadership, nobody waits (F6: a refused or unanswered delete
   stops the run). Without "id" in Late it applies at once. *)
FsckDelete(t) ==
  /\ fI4' = (fI4 \/ IdBad(t))
  /\ IF LateOk("id")
       THEN req' = req \cup {IdReq(NoG, t)} /\ intents' = intents
       ELSE req' = req /\ intents' = intents \ {t}

(* F2, first read: exact `GET` of a listed intent. Gone -> skip (no step). *)
FsckGetIntent(t) ==
  /\ Fsck /\ ~Has("cleanup_head_first") /\ fs.ph = "Idle" /\ t \in intents
  /\ fs' = [ph |-> "H", tok |-> t]
  /\ UNCHANGED <<bodyVars, sigVars, pm, wr, lrn, putVars, gcVars, req, ghostVars>>
  /\ UNCHANGED trigVars

(* F2, second read: `HEAD B`, sent after the answer of the `GET`. Same canonical identity ->
   `intent_current`. Absent or another identity -> `intent_stale`, and F3 deletes it.
   F0: a client of another dialect is stopped at admission; the sabotage admits it, and to it the
   same body renders as another identity. *)
FsckHeadBody ==
  /\ fs.ph = "H"
  /\ \E d \in (IF body.present /\ body.tok = fs.tok
                 THEN (IF Has("cleanup_foreign_dialect") THEN {TRUE} ELSE RawMayDiffer("cleanup_raw_spelling"))
                 ELSE {TRUE}) :
       IF d THEN FsckDelete(fs.tok) ELSE UNCHANGED <<req, intents, fI4>>
  /\ fs' = [ph |-> "Idle", tok |-> 0]
  /\ UNCHANGED <<bodyVars, meta, pm, wr, lrn, putVars, gcVars, fRecur, fOther, fWMut, fAdopt, fMMA>>
  /\ UNCHANGED trigVars

(* Sabotage of the order of F2: the body is observed first. *)
FsckHeadFirst ==
  /\ Fsck /\ Has("cleanup_head_first") /\ fs.ph = "Idle"
  /\ fs' = [ph |-> "G", tok |-> IF body.present THEN body.tok ELSE 0]
  /\ UNCHANGED <<bodyVars, sigVars, pm, wr, lrn, putVars, gcVars, req, ghostVars>>
  /\ UNCHANGED trigVars

FsckGetAfterHead(t) ==
  /\ fs.ph = "G" /\ t \in intents
  /\ IF t /= fs.tok THEN FsckDelete(t) ELSE UNCHANGED <<req, intents, fI4>>
  /\ fs' = [ph |-> "Idle", tok |-> 0]
  /\ UNCHANGED <<bodyVars, meta, pm, wr, lrn, putVars, gcVars, fRecur, fOther, fWMut, fAdopt, fMMA>>
  /\ UNCHANGED trigVars

--------------------------------------------------------------------------------------
(* THE SWITCH, section 7.3. *)

(* Step 2, the stamp: `gc/state` gets generation 4. The object is rewritten: a round in flight
   loses its commit. *)
Stamp ==
  /\ Transition /\ ~gs.gen4
  /\ gs' = [gs EXCEPT !.gen4 = TRUE]
  /\ UNCHANGED <<bodyVars, sigVars, pm, wr, lrn, putVars, gc, gmode, req, fs, ghostVars>>
  /\ UNCHANGED trigVars

(* Step 3, the switch: refused while the stamp is not confirmed. *)
Switch ==
  /\ Transition /\ ~pm
  /\ (gs.gen4 \/ Has("no_stamp"))
  /\ pm' = TRUE
  /\ UNCHANGED <<bodyVars, sigVars, wr, lrn, putVars, gcVars, req, fs, ghostVars>>
  /\ UNCHANGED trigVars

(* X2 (unsupported): `gc/state` is deleted after the switch and an older build bootstraps it. *)
LoseStamp ==
  /\ Has("lost_stamp_old_bootstrap") /\ pm /\ gs.gen4
  /\ gs' = [gs EXCEPT !.gen4 = FALSE, !.e = NoEntry]
  /\ UNCHANGED <<bodyVars, sigVars, pm, wr, lrn, putVars, gc, gmode, req, fs, ghostVars>>
  /\ UNCHANGED trigVars

--------------------------------------------------------------------------------------
Next ==
  \/ \E w \in Writers :
       \/ \E b \in Builds : WPrecommit(w, b)
       \/ WLearn(w) \/ WPrime(w) \/ WHead1(w) \/ WGetIntent(w) \/ WHead2(w) \/ WGetMetaOld(w)
       \/ WSendPut(w) \/ WPutOk(w) \/ WPutGiveUp(w) \/ WCommit(w) \/ WAbandon(w) \/ WDrop(w) \/ WLink(w)
  \/ \E q \in putq : PutLand(q)
  \/ \E q \in sentq : PutResend(q)
  \/ \E g \in GCs :
       \/ GStart(g) \/ GMode(g) \/ GCut(g) \/ GCondemn(g) \/ GSpare(g) \/ GSupersede(g)
       \/ GCreateSend(g) \/ GCreateAnswer(g) \/ GCreateUnanswered(g) \/ GCreateResolve(g) \/ GGraduateMarker(g)
       \/ GDelHead(g) \/ GDelAnswer(g) \/ GDelUnanswered(g) \/ GIdAnswer(g) \/ GIdUnanswered(g)
       \/ GCommit(g) \/ GCommitLost(g) \/ GAbort(g)
  \/ \E q \in req : ReqApply(q)
  \/ \E t \in intents : FsckGetIntent(t) \/ FsckGetAfterHead(t)
  \/ FsckHeadBody \/ FsckHeadFirst
  \/ Stamp \/ Switch \/ LoseStamp

Spec == Init /\ [][Next]_vars

(* Liveness runs: one GC actor, no faults, no symmetry, unbounded rounds. *)
Fair(A) == WF_vars(A)
Fairness ==
  /\ \A g \in GCs :
       /\ Fair(GStart(g)) /\ Fair(GMode(g)) /\ Fair(GCut(g)) /\ Fair(GCondemn(g)) /\ Fair(GSpare(g))
       /\ Fair(GCreateSend(g)) /\ Fair(GCreateAnswer(g)) /\ Fair(GCreateResolve(g)) /\ Fair(GGraduateMarker(g))
       /\ Fair(GDelHead(g)) /\ Fair(GDelAnswer(g)) /\ Fair(GIdAnswer(g)) /\ Fair(GCommit(g))
       /\ Fair(GCommitLost(g))
  /\ Fair(\E q \in req : ReqApply(q))
  /\ Fair(\E q \in putq : PutLand(q))
  /\ \A t \in Tokens : Fair(FsckGetIntent(t))
  /\ Fair(FsckHeadBody)
LiveSpec == Spec /\ Fairness

Symm == Permutations(Writers) \cup (IF OldGCs = {} THEN Permutations(GCs) ELSE {})

--------------------------------------------------------------------------------------
(* INVARIANTS. The number in a name is the number of the specification (section 4.1, 7.1). *)

Entries == [st : {"none", "cand", "pend"}, tok : 0..MaxTok, int : BOOLEAN, conf : BOOLEAN]

TypeOK ==
  /\ Sab \subseteq KnownSab /\ Late \subseteq {"put", "ic", "id", "bd"} /\ Builds \subseteq {"new", "old"}
  /\ Start \in {"T1", "T3"} /\ TokenKind \in {"Generation", "Content"} /\ OldGCs \subseteq GCs
  /\ Seed \in {"empty", "orphan", "cand", "pend", "spared", "legacy", "legacy_cand", "legacy_pend"}
  /\ body \in [present : BOOLEAN, tok : 0..MaxTok, ser : 0..MaxTok, pay : {"none", Good, Bad}, mk : BOOLEAN]
  /\ intents \subseteq Tokens /\ gone \subseteq Tokens /\ meta \in {"absent", "clean", "cond"}
  /\ pm \in BOOLEAN /\ nsend \in 0..MaxPuts /\ nres \in 0..MaxResends
  /\ \A w \in Writers :
       /\ wr[w].ph \in {"Idle", "H1", "G", "H2", "M", "Pub", "Wait", "Ready", "Done"}
       /\ wr[w].b \in {"new", "old"} /\ wr[w].seen \in 0..MaxTok /\ wr[w].rd \in 0..9 /\ wr[w].pt \in 0..3
  /\ gs.e \in Entries /\ gs.ver \in 0..(IF MaxVer = 0 THEN 1 ELSE MaxVer) /\ (gs.e.int => gs.e.st = "pend")
  /\ Trigger \in {"any", "zero"} /\ ((Trigger = "zero" \/ MaxVer = 0) => Cardinality(GCs) = 1)
  /\ \A g \in GCs :
       /\ gc[g].ph \in {"Idle", "Mode", "Read", "Work", "Work2", "CrWait", "CrRes", "DelWait", "IdWait", "Commit"}
       /\ gc[g].base \in Entries /\ gc[g].next \in Entries /\ gc[g].mode \in {"none", "marker", "intent"}
       /\ Cardinality(OwnReq(g)) <= 1
  /\ \A q \in req : q.k \in {"ic", "id", "bd"} /\ q.o \in GCs \cup {NoG}
  /\ fs.ph \in {"Idle", "H", "G"}

(* I1. Once `t` stops being the incarnation of B, it never is again. *)
INV_I1_NO_RECURRENCE == ~fRecur

(* I2. Every body ever at B has the payload that hashes to B. *)
INV_I2_KEY_NAMES_PAYLOAD == body.present => body.pay = Good

(* I3. A writer's edge is durable before its first `HEAD B`. Edges are the non-idle slots; the
   actions that observe the body are enabled only in phases that follow W1. *)
INV_I3_EDGE_BEFORE_OBSERVE ==
  \A w \in Writers : (wr[w].seen /= 0 \/ wr[w].ph \in {"G", "H2", "M", "Pub", "Wait", "Ready"}) => wr[w].ph /= "Idle"

(* I4. A delete of I(B,t) is sent only when `t` is gone from B, so an existing I(B,t) stays while
   `t` is current: no intent DELETE is sent, and none applies on an existing intent, while `t` is
   the incarnation of B. *)
INV_I4_INTENT_PERMANENT == ~fI4

(* I5 and T3 (c). Every body DELETE of an intent-mode round follows, in this order: a confirmation
   that I(B,t) exists, the commit of the pending row, a cut taken from that committed state with
   in-degree 0, a `HEAD B` that shows `t`. *)
INV_I5_DELETE_EVIDENCE == \A q \in req : (q.k = "bd" /\ q.auth = "intent") => q.ok

(* The exactness of the conditional delete (A3): it removes only the landing its `HEAD` observed. *)
INV_A3_DELETE_IS_EXACT == ~fOther

(* I6 and T3 (b). An adoption is of a marked body, by a learned writer, after W4 answered 404 from
   the store and W5 showed the identity of W2. *)
INV_I6_ADOPTION == ~fAdopt

(* I7 and T1. A marked body, a marked publication and an intent exist only on a switched pool. *)
INV_I7_MARKED_MEANS_NEW ==
  ~pm => /\ ~(body.present /\ body.mk)
         /\ \A q \in putq : ~q.mk
         /\ intents = {}
         /\ \A q \in req : q.k /= "ic"

(* T3 (a). A marked token is under intent authority only: no body DELETE is sent for a marked body
   on a row whose intent was not confirmed. *)
INV_T3A_MARKED_NEVER_MARKER_AUTHORITY == ~fMMA

(* I8, the goal. A committed ref never names a blob key with no body. *)
INV_I8_COMMITTED_REF_HAS_BODY == \A w \in Writers : wr[w].ph = "Done" => body.present

(* Decision 1.1. Writers never create, change or delete an intent. *)
INV_WRITERS_NEVER_MUTATE_INTENTS == ~fWMut

(* Section 1, "Budget", per blob and writer attempt on the successful path. `ga` counts unanswered
   publications of the attempt; the counters are not reset by them. *)
INV_REQUEST_BUDGET ==
  \A w \in Writers :
    (Budget /\ wr[w].ph = "Ready" /\ wr[w].b = "new") =>
      CASE wr[w].path = "new" -> wr[w].rd = 1 /\ wr[w].pt = 1            \* first creation
        [] wr[w].path = "reuse" -> wr[w].rd = 3 /\ wr[w].pt = 0          \* clean reuse
        [] wr[w].path = "condemned" -> wr[w].rd = 2 /\ wr[w].pt = 1      \* reuse after condemnation
        [] wr[w].path = "legacy" -> wr[w].rd = 1 /\ wr[w].pt = 1         \* legacy body, or writer not learned
        [] wr[w].path = "changed" -> wr[w].rd = 3 /\ wr[w].pt = 1        \* W5 saw another token or no body
        [] wr[w].path = "amb_new_reuse" -> wr[w].rd = 4 /\ wr[w].pt = 1  \* unanswered publication that landed
        [] OTHER -> wr[w].rd <= 3 * (wr[w].ga + 1) /\ wr[w].pt <= wr[w].ga + 1

(* Reachability witnesses, each an invariant that must FAIL. *)
WIT_ADOPTION == \A w \in Writers : ~(wr[w].ph = "Done" /\ wr[w].path = "reuse")
WIT_PUBLISH_OVER_INTENT ==         \* a writer met I(B,t) and its fresh body replaced `t`
  ~(\E t \in intents : t \in gone /\ body.present /\ \E w \in Writers : wr[w].ph \in {"Ready", "Done"})
WIT_SPARE_STANDING_INTENT ==       \* a pending row with `intent` is spared and the intent stays
  \A g \in GCs : ~(gc[g].ph = "Commit" /\ gc[g].base.int /\ gc[g].ref /\ gc[g].next.st = "none"
                    /\ gc[g].base.tok \in intents /\ body.present /\ body.tok = gc[g].base.tok)
WIT_RECONDEMN_ON_EXISTING_INTENT ==   \* the create is refused by the equal occupant and the read confirms
  \A g \in GCs : ~(gc[g].ph = "CrRes" /\ gc[g].base.tok \in intents /\ body.present /\ body.tok = gc[g].base.tok
                    /\ NoOwned(g) /\ \A q \in req : q.k /= "ic")
WIT_LATE_CREATE_AFTER_ROW_LEFT ==  \* a create applies when no row names its token
  ~(\E q \in req : q.k = "ic" /\ ~q.ap /\ q.o = NoG /\ gs.e.st = "none" /\ \A g \in GCs : gc[g].ph = "Idle")
WIT_STALE_INTENT_CLEANED ==        \* fsck is about to delete an intent whose token is gone
  ~(fs.ph = "H" /\ fs.tok \in intents /\ fs.tok \in gone)
WIT_LEGACY_REUPLOAD ==             \* a learned writer replaced an unmarked body with a marked one
  ~(body.present /\ body.mk /\ 1 \in gone /\ Seed \in {"legacy", "legacy_cand", "legacy_pend"})
WIT_CHANGED_AT_W5 == \A w \in Writers : ~(wr[w].ph = "Pub" /\ wr[w].path = "changed")
WIT_DEPOSED_DELETE_LANDS ==        \* a body DELETE of an actor whose base is stale is applied as `removed`
  \A g \in GCs : ~(gc[g].ph = "DelWait" /\ gc[g].bver < gs.ver
                    /\ \E q \in OwnReq(g) : q.ap /\ q.res \in {"removed", "removed_unmarked"})
WIT_BODY_DELETED == ~(SeedBody /\ ~body.present /\ nextSer = 2 /\ intents = {} /\ gs.ver >= 2)
WIT_TWO_ABSENT_PUBLISHERS == ~(Cardinality(putq) = 2 /\ ~body.present /\ nextSer = 1)
WIT_STAGED_COPY == ~(\E q \in putq : q.env = StagedEnv)
WIT_SUPERSEDE == \A g \in GCs : ~(gc[g].ph = "Commit" /\ gc[g].base.st /= "none" /\ gc[g].next.st = "cand"
                                   /\ gc[g].next.tok /= gc[g].base.tok)
WIT_INHERITED_DEMOTED ==           \* an inherited pending row is confirmed by G3 in intent mode
  \A g \in GCs : ~(gc[g].ph = "Commit" /\ gc[g].base.st = "pend" /\ ~gc[g].base.int /\ gc[g].next.int)
WIT_MARKER_ROUND_AFTER_SWITCH ==   \* a round that sampled before the switch works after it
  \A g \in GCs : ~(g \notin OldGCs /\ gc[g].mode = "marker" /\ pm /\ gc[g].ph = "Work")
WIT_MARKED_CONDEMNED_BY_MARKER_ROUND ==
  \A g \in GCs : ~(gc[g].mode = "marker" /\ gc[g].ph = "Commit" /\ gc[g].next.st = "cand" /\ body.present
                    /\ body.mk /\ body.tok = gc[g].next.tok)
WIT_LATE_OLD_PUT == ~(pm /\ \E q \in putq : wr[q.w].b = "old" /\ ~q.mk /\ body.present /\ body.mk)
WIT_LATE_OLD_DELETE == ~(pm /\ \E q \in req : q.k = "bd" /\ q.auth = "marker" /\ ~q.ap)
WIT_LINK == \A w \in Writers : ~(wr[w].ph = "Done" /\ wr[w].path = "link")
WIT_UNLEARNED_AFTER_SWITCH ==      \* a slot that has not learned publishes unmarked on a switched pool
  ~(pm /\ \E q \in putq : ~q.mk /\ wr[q.w].b = "new")
WIT_T3_ADOPTION ==                 \* the transition ends in a clean reuse of a marked body
  \A w \in Writers : ~(Transition /\ wr[w].ph = "Done" /\ wr[w].path = "reuse" /\ wr[w].b = "new")
WIT_INTENT_DELETE_AFTER_BODY ==    \* the full pipeline: body removed, then its intent
  ~(\E q \in req : q.k = "id" /\ q.o /= NoG /\ q.ap /\ ~body.present /\ q.tok \in gone)
WIT_OLD_WRITER_ADOPTS_AFTER_SWITCH ==   \* an older writer whose edge predates the switch adopts a marked body
  \A w \in Writers : ~(wr[w].b = "old" /\ wr[w].ph = "M" /\ body.present /\ body.mk
                       /\ wr[w].seen = body.tok /\ meta = "clean")
WIT_UNANSWERED_PUT_ADOPTED ==      \* the 4 reads, 1 PUT path: an unanswered publication landed and is adopted
  \A w \in Writers : ~(wr[w].ph = "Ready" /\ wr[w].path = "amb_new_reuse")
WIT_DELETE_MISSES ==               \* a conditional body DELETE finds another incarnation or none
  ~(\E q \in req : q.k = "bd" /\ q.ap /\ q.res = "miss")
WIT_DERIVED_ROW_DELETES ==         \* under "candidate_from_intent": a derived row reaches its body DELETE
  ~(Seed = "spared" /\ gs.ver = 1 /\ \E q \in req : q.k = "bd" /\ q.ap /\ q.res = "removed")
WIT_ALT_ADOPT_CHANGED ==           \* under "alt_adopt_changed_token": the other token is adopted and committed
  \A w \in Writers : ~(wr[w].ph = "Done" /\ wr[w].path = "reuse" /\ 1 \in gone /\ nsend = 1 /\ wr[w].pt = 0
                       /\ \A v \in Writers \ {w} : wr[v].ph = "Idle")
WIT_INHERITED_EXECUTED ==          \* under "inherited_executed": an intent-mode round deletes on an inherited row
  ~(\E q \in req : q.k = "bd" /\ q.auth = "intent" /\ ~q.ok /\ q.ap /\ q.res = "removed_unmarked")

--------------------------------------------------------------------------------------
(* LIVENESS, once the writers are done for good. *)
Quiet == (\A w \in Writers : wr[w].ph = "Idle") /\ putq = {}

(* Claimed: no row is stuck and, with cas-fsck running, no intent of a dead token is left. *)
LIVE_ROW_LEAVES_AND_NO_STALE_INTENT ==
  <>[]Quiet => <>[](gs.e.st = "none" /\ \A t \in intents : body.present /\ body.tok = t)
(* NOT claimed by the specification (section 8.2): every unreferenced body is deleted. *)
LIVE_UNREFERENCED_BODY_DELETED == <>[]Quiet => <>[](~body.present)
(* NOT claimed (section 8.2): no intent is left at all. *)
LIVE_NO_INTENT_LEFT == <>[]Quiet => <>[](intents = {})

(*
ABSTRACTIONS AND BOUNDS
  - One blob key. Batching of intent deletes across rows, budgets, read-ahead and shards are absent.
  - A read is one atomic observation. A cut is complete and atomic (A7 is assumed, not checked).
  - Safety runs use Trigger = "any": GC may condemn whenever a cut shows in-degree 0 and a body is
    present. The code condemns only on a fresh transition to zero; "any" allows more condemnations,
    never fewer. Liveness runs use Trigger = "zero".
  - A touched row (G7) is any row: the supersede may fire whenever the `HEAD` shows another token.
  - A crash of a writer is WAbandon (its requests stay in flight with "put" in Late); the stalled
    phase before it stands for the time the dead process still owns its edge. A crash of a GC actor
    is GAbort or one of the `Unanswered` steps.
  - A writer of the older build may finish, after the switch, a build whose edge was durable before
    it. Section 7.2 stops such a process; the model does not need that.
  - The older build's marker protocol is reduced: three marker states, no round stamp, marker writes
    are one step with the observation that precedes them. It is the environment of the transition,
    not its subject.
  - Token spelling: one switch per comparison site ("may read as another incarnation").
  - Not modelled: sizes and the envelope-length check, malformed intent bytes, a token that does not
    canonicalise, multipart transport, the mark probe, mount slots and the checks of the command,
    `SYSTEM CAS GC REBUILD`, more than one switch command at once (see CaBlobIntentFreshPool).
  - Bounds: |Writers|, |GCs|, MaxPuts publications, MaxVer round commits; builds are unbounded.
*)
======================================================================================
