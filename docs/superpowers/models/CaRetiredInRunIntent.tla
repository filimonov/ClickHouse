---------------------------- MODULE CaRetiredInRunIntent ----------------------------
(* `CaRetiredInRun` with the marker replaced by the deletion intents of
   `docs/superpowers/specs/2026-11-17-cas-blob-deletion-intents-design.md`.

   What it keeps from `CaRetiredInRun`: the append-only edge journal, the incremental fold, and
   the cut that may be any point from the coverage floor (the journal length at the previous
   adoption) to the head. So an edge journalled after the previous adoption may be missing from a
   round's cut. The consolidated intent model takes an atomic cut of every edge; this one does not.

   What it adds:
   - Clamp suppression. A suppressed round may cut anywhere at or after the adopted cut, also below
     the coverage floor. Per the spec, G3, G4, G8 and G9 do not run in it; G2, G6 and G7 do.
   - The writer split as W1 (edge), W2 (`HEAD B`), W4 (`GET I(B,t)`), W5 (publish a fresh token).

   What it drops: attempt keys and artifacts (orthogonal, owned by `CaRetiredInRun`), late requests,
   unconfirmed creates, two GC actors (owned by the consolidated intent model). A round is one atomic
   step: lease read, cut, every decision `HEAD`, the creates, the deletes, the commit (G12 holds).

   Row states: none, cand(t), pend(t), stand(t). `intent[b]` is the set of tokens t with I(b,t).
   Sabotages each remove one rule: "g8_suppressed", "g4_suppressed", "g8_positive" (S3),
   "spare_deletes_intent" (S4), "delete_on_condemn_cut" (S5), "no_floor" (an unsuppressed cut may
   also miss edges journalled before the previous adoption: shows which cut completeness the
   protocol needs), "g7_gated" (not a sabotage: G7 also waits for an unsuppressed round). The base
   configuration runs G7 in suppressed rounds, as the spec has it (witness `WIT_G7_SUPPRESSED`). *)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS Blobs, MaxRound, MaxToken, MaxJournal, Suppress, Adopt, Sabotage

ASSUME Sabotage \in {"none", "g8_suppressed", "g4_suppressed", "g8_positive",
                     "spare_deletes_intent", "delete_on_condemn_cut", "g7_gated", "no_floor"}

VARIABLES
  journal,     \* Seq of <<blob, op>>, op \in {"add","rm"}
  phys,        \* [Blobs -> 0..MaxToken]: token of the body at B; 0 = absent
  nextTok,     \* [Blobs -> 1..MaxToken+1]: tokens never recur (A1)
  intent,      \* [Blobs -> SUBSET 1..MaxToken]
  wph,         \* [Blobs -> {"idle","head","get","pub","done"}]: one writer per blob, as in CaRetiredInRun
  seen,        \* [Blobs -> 0..MaxToken]: the token of the writer's W2
  adopted,     \* [cut, edges: [Blobs -> Int], row: [Blobs -> Row], round]
  lenAtAdopt,  \* coverage floor of the next unsuppressed cut
  ever         \* witnesses: set of strings

vars == <<journal, phys, nextTok, intent, wph, seen, adopted, lenAtAdopt, ever>>

NoRow == [st |-> "none", tok |-> 0]
Row(s, t) == [st |-> s, tok |-> t]

EdgeCount(b, cut) ==
  Cardinality({i \in 1..cut : journal[i][1] = b /\ journal[i][2] = "add"})
    - Cardinality({i \in 1..cut : journal[i][1] = b /\ journal[i][2] = "rm"})
Delta(b, lo, hi) ==
  Cardinality({i \in (lo + 1)..hi : journal[i][1] = b /\ journal[i][2] = "add"})
    - Cardinality({i \in (lo + 1)..hi : journal[i][1] = b /\ journal[i][2] = "rm"})
Touched(b, lo, hi) == \E i \in (lo + 1)..hi : journal[i][1] = b

Init ==
  /\ journal = <<>>
  /\ phys = [b \in Blobs |-> 0]
  /\ nextTok = [b \in Blobs |-> 1]
  /\ intent = [b \in Blobs |-> {}]
  /\ wph = [b \in Blobs |-> "idle"]
  /\ seen = [b \in Blobs |-> 0]
  /\ adopted = [cut |-> 0, edges |-> [b \in Blobs |-> 0], row |-> [b \in Blobs |-> NoRow], round |-> 0]
  /\ lenAtAdopt = 0
  /\ ever = {}

---------------------------------------------------------------------------------------------
(* Writer. W1: the edge is journalled before the first request. *)
WEdge(b) ==
  /\ Len(journal) < MaxJournal /\ wph[b] = "idle"
  /\ journal' = Append(journal, <<b, "add">>)
  /\ wph' = [wph EXCEPT ![b] = "head"]
  /\ UNCHANGED <<phys, nextTok, intent, seen, adopted, lenAtAdopt, ever>>

(* W2: `HEAD B`. Absent -> W5. *)
WHead(b) ==
  /\ wph[b] = "head"
  /\ wph' = [wph EXCEPT ![b] = IF phys[b] = 0 THEN "pub" ELSE "get"]
  /\ seen' = [seen EXCEPT ![b] = phys[b]]
  /\ UNCHANGED <<journal, phys, nextTok, intent, adopted, lenAtAdopt, ever>>

(* W4: `GET I(B,t)` for the token of W2. Present -> W5. An answered 404 -> adopt. *)
WGet(b) ==
  /\ wph[b] = "get"
  /\ IF seen[b] \in intent[b]
       THEN /\ wph' = [wph EXCEPT ![b] = "pub"] /\ UNCHANGED ever
       ELSE /\ wph' = [wph EXCEPT ![b] = "done"]
            /\ ever' = ever \cup {"adopt"}
  /\ UNCHANGED <<journal, phys, nextTok, intent, seen, adopted, lenAtAdopt>>

(* W5: one unconditional publication of a fresh token. *)
WPub(b) ==
  /\ wph[b] = "pub" /\ nextTok[b] <= MaxToken
  /\ phys' = [phys EXCEPT ![b] = nextTok[b]]
  /\ nextTok' = [nextTok EXCEPT ![b] = @ + 1]
  /\ wph' = [wph EXCEPT ![b] = "done"]
  /\ UNCHANGED <<journal, intent, seen, adopted, lenAtAdopt, ever>>

(* The edge is withdrawn: after the commit, or by an abandoned build at any step. *)
WRemove(b) ==
  /\ Len(journal) < MaxJournal /\ wph[b] /= "idle"
  /\ journal' = Append(journal, <<b, "rm">>)
  /\ wph' = [wph EXCEPT ![b] = "idle"]
  /\ UNCHANGED <<phys, nextTok, intent, seen, adopted, lenAtAdopt, ever>>

---------------------------------------------------------------------------------------------
(* GC. One round decides each blob from its adopted row `pc`, the merged in-degree `d`, the body
   token `p` seen by this round's `HEAD` (sent after the cut), whether the key was touched in the
   window, and whether the round is suppressed. Result: [row, del (body delete), mk (intent
   creates), rm (intent deletes), w (witness tags)]. *)
Out(row, del, mk, rm, w) == [row |-> row, del |-> del, mk |-> mk, rm |-> rm, w |-> w]

Decide(b, pc, d, p, touched, sup, adoptOn) ==
  LET headSent == (pc.st = "pend" /\ (~sup \/ Sabotage = "g4_suppressed")) \/ touched
      g7       == pc.st /= "none" /\ d = 0 /\ headSent /\ p /= 0 /\ p /= pc.tok
                  /\ (~sup \/ Sabotage /= "g7_gated")
  IN
  IF pc.st = "none" THEN
    IF d = 0 /\ p /= 0 /\ touched
      THEN IF Sabotage = "delete_on_condemn_cut"
             THEN Out(Row("stand", p), TRUE, {p}, {}, {"g2"})        \* S5: create and delete at once
             ELSE Out(Row("pend", p), FALSE, {p}, {}, {"g2"})        \* G2: create, pending if confirmed
    ELSE IF d = 0 /\ ~sup /\ adoptOn /\ intent[b] /= {}
      THEN LET t == CHOOSE x \in intent[b] : TRUE IN                 \* G9: one orphan per key per round
           IF p = t THEN Out(Row("cand", t), FALSE, {}, {}, {"g9"})
                    ELSE Out(Row("stand", t), FALSE, {}, {}, {"g9"})
    ELSE Out(NoRow, FALSE, {}, {}, {})
  ELSE IF d > 0 THEN                                                 \* G6: every row dropped
    IF Sabotage = "g8_positive" /\ pc.st = "stand"
      THEN Out(NoRow, FALSE, {}, {pc.tok}, {})
    ELSE IF Sabotage = "spare_deletes_intent"
      THEN Out(NoRow, FALSE, {}, {pc.tok}, {})
    ELSE Out(NoRow, FALSE, {}, {}, {})
  ELSE IF g7 THEN                                                    \* G7: before G4 and G8
    Out(Row("pend", p), FALSE, {p}, {pc.tok}, IF sup THEN {"g7", "g7sup"} ELSE {"g7"})
  ELSE IF pc.st = "cand" THEN
    IF sup THEN Out(pc, FALSE, {}, {}, {}) ELSE Out(Row("pend", pc.tok), FALSE, {pc.tok}, {}, {"g3"})
  ELSE IF pc.st = "pend" THEN                                        \* G4
    IF sup /\ Sabotage /= "g4_suppressed" THEN Out(pc, FALSE, {}, {}, {})
    ELSE IF p = pc.tok THEN Out(Row("stand", pc.tok), TRUE, {}, {}, {"g4"})
    ELSE IF p = 0 THEN Out(Row("stand", pc.tok), FALSE, {}, {}, {"g4"})
    ELSE Out(pc, FALSE, {}, {}, {})                                  \* unreachable: G7 took it
  ELSE                                                               \* standing: G8, no HEAD
    IF sup /\ Sabotage /= "g8_suppressed" THEN Out(pc, FALSE, {}, {}, {})
    ELSE Out(NoRow, FALSE, {}, {pc.tok}, IF sup THEN {"g8", "g8sup"} ELSE {"g8"})

Round ==
  /\ adopted.round < MaxRound
  /\ \E sup \in (IF Suppress THEN BOOLEAN ELSE {FALSE}) :
     \E cut \in (IF sup \/ Sabotage = "no_floor" THEN adopted.cut ELSE lenAtAdopt)..Len(journal) :
     LET d(b)   == adopted.edges[b] + Delta(b, adopted.cut, cut)
         o(b)   == Decide(b, adopted.row[b], d(b), phys[b], Touched(b, adopted.cut, cut), sup, Adopt)
     IN /\ phys' = [b \in Blobs |-> IF o(b).del /\ phys[b] = o(b).row.tok THEN 0 ELSE phys[b]]
        /\ intent' = [b \in Blobs |-> (intent[b] \ o(b).rm) \cup o(b).mk]
        /\ adopted' = [cut |-> cut, edges |-> [b \in Blobs |-> d(b)],
                       row |-> [b \in Blobs |-> o(b).row], round |-> adopted.round + 1]
        /\ lenAtAdopt' = Len(journal)
        /\ ever' = ever \cup UNION {o(b).w : b \in Blobs}
  /\ UNCHANGED <<journal, nextTok, wph, seen>>

NoOp == UNCHANGED vars

Next ==
  \/ \E b \in Blobs : WEdge(b) \/ WHead(b) \/ WGet(b) \/ WPub(b) \/ WRemove(b)
  \/ Round
  \/ NoOp

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ \A b \in Blobs : phys[b] \in 0..MaxToken /\ phys[b] < nextTok[b]
  /\ \A b \in Blobs : intent[b] \subseteq 1..MaxToken
  /\ \A b \in Blobs : adopted.row[b].st \in {"none", "cand", "pend", "stand"}
  /\ lenAtAdopt <= Len(journal)

(* I8: a writer that adopted or published holds a reference to a present body. *)
INV_NO_LOSS == \A b \in Blobs : wph[b] = "done" => phys[b] > 0

(* The adopted edge count is the journal truth at the adopted cut. *)
INV_COVERAGE == \A b \in Blobs : adopted.edges[b] = EdgeCount(b, adopted.cut)

(* Negated reachability: TLC reporting a violation is the evidence. *)
WIT_ADOPT == "adopt" \notin ever
WIT_G4 == "g4" \notin ever
WIT_G8 == "g8" \notin ever
WIT_G7 == "g7" \notin ever
WIT_G7_SUPPRESSED == "g7sup" \notin ever
WIT_G9 == "g9" \notin ever
WIT_ADOPT_AFTER_G8 == ~("g8" \in ever /\ \E b \in Blobs : wph[b] = "done" /\ phys[b] > 0 /\ seen[b] > 0
                                                        /\ adopted.row[b].st = "none")
===============================================================================
