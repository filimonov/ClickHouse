---------------------------- MODULE CaRefLogOnlyAppendCore ----------------------------
(***************************************************************************)
(* Log-only ref append with a lazy, forward-linked epoch tombstone         *)
(* (CAS-487, spec 2026-11-14-cas-one-request-ref-append-design).           *)
(*                                                                         *)
(* One namespace, one physical life. Writers are mount epochs 1..MaxEpoch; *)
(* at most one holds the lease. The store holds ref-log objects keyed by   *)
(* <<epoch, seq>> and one snapshot base (the `_ckpt` pointer).             *)
(*                                                                         *)
(* Protocol under test:                                                    *)
(*   - an ordinary append is ONE create-if-absent of `<<e, s>>`; a durable *)
(*     object on the chain is committed history;                           *)
(*   - recovery reads the base, then exact keys one by one to the first    *)
(*     absent one, following tombstones; it re-reads the base after the    *)
(*     absent read and restarts when the base moved;                       *)
(*   - the first append of an epoch `W` whose chain ends in another epoch  *)
(*     is preceded by a create-if-absent of a tombstone in the chain's     *)
(*     next slot; the tombstone names `W`; the head `<<W, 1>>` names the   *)
(*     tombstone; a conflict makes the writer read the chain again;        *)
(*   - a never-born stream follows the same rule (uniform genesis);        *)
(*   - recovery writes that tombstone before the table is trusted iff the  *)
(*     chain ends in an epoch at or below `lastUnclean`;                   *)
(*   - the acknowledgement fence is the mount lease only;                  *)
(*   - GC keeps the id of the last folded record and resolves its          *)
(*     successor without trusting an object that cleanup may have deleted. *)
(*                                                                         *)
(* Abstractions: ref payloads are opaque txn numbers; the snapshot is a    *)
(* ghost copy of the covered prefix; GC is one cursor without rounds or a  *)
(* destructive gate; one life (no `DROP` followed by re-creation).         *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    MaxEpoch,           \* writer epochs 1..MaxEpoch
    MaxSeq,             \* slots 1..MaxSeq in every epoch
    MaxTxn,             \* ordinary appends attempted in a behaviour
    LateLand,           \* a request of a superseded writer may land after the takeover
    ClockLies,          \* a superseded writer still passes its local lease check
    EagerSeal,          \* seal at every recovery (today's behaviour)
    SelectiveSeal,      \* seal at recovery only when the chain ends at or below `lastUnclean`
    SelectiveImmediate, \* SABOTAGE: the selective rule looks only at the immediate predecessor's stop
    SealBlind,          \* SABOTAGE: the tombstone PUT overwrites an occupant
    SealSkip,           \* SABOTAGE: a new epoch appends at <<W, 1>> without closing the old epoch
    GcCursorNaive,      \* SABOTAGE: GC trusts the same-epoch probe and the object at its cursor key
    GcCursorProbeFirst, \* SABOTAGE: GC probes the same-epoch successor first, then reads the cursor object, then back-chains
    GcCursorAnyHead,    \* SABOTAGE: below the snapshot GC takes any head that names its cursor
    WalkNoRecheck,      \* SABOTAGE: recovery does not re-read the base after its absent read
    SealNoRecheck,      \* SABOTAGE: a tombstone is installed without re-reading the base after its create
    NoCleanup,          \* ref cleanup never deletes
    Legacy,             \* the life starts with a seal of the old format written by epoch 1
    Terminal            \* one `RemoveNamespace` append may be sent

VARIABLES
    store,      \* [Ids -> record or Nil]
    live,       \* the epoch that holds the mount lease
    wst,        \* [epoch -> "none" | "mounted" | "recovered" | "deposed"]
    mem,        \* [epoch -> sequence of records]: the writer's in-memory history
    tail,       \* [epoch -> id]: the next slot of the writer's chain
    pend,       \* [epoch -> record or Nil]: the exact attempt not yet resolved
    inflight,   \* set of records travelling to the store
    acked,      \* txn numbers acknowledged to a client
    ackedLive,  \* ghost: the subset acknowledged by the epoch that held the lease at that moment
    cov,        \* ghost: the records covered by the published snapshot, in order
    baseNext,   \* the id the published snapshot says to read next (also the `_ckpt` token)
    gcFold,     \* ghost: the records GC has folded, in order
    gcCur,      \* the id of the last record GC folded, or NoId
    nextTxn,
    intent,     \* [epoch -> BOOLEAN]: the epoch was asked to write
    blobGone,   \* GC deleted the blob that the tracked ref protects
    lastUnclean,\* the greatest epoch known to have ended without a clean farewell (0: none)
    walk,       \* [epoch -> [on, base, seq, pos]]: a recovery walk in progress
    ckptFlight  \* snapshot publications travelling to the store

vars == <<store, live, wst, mem, tail, pend, inflight, acked, ackedLive, cov, baseNext, gcFold, gcCur, nextTxn,
          intent, blobGone, lastUnclean, walk, ckptFlight>>

Epochs == 1..MaxEpoch
Ids == Epochs \X (1..MaxSeq)
NoId == <<0, 0>>
Genesis == <<1, 1>>
InIds(id) == id[1] \in Epochs /\ id[2] \in 1..MaxSeq

\* kind: "op" | "seal" (tombstone with a forward link) | "oseal" (old format) | "term" | "nil".
\* v: txn number, or the epoch a tombstone names. prev: the tombstone a head names, or NoId.
\* src: "lane" | "recovery" for a tombstone, "na" otherwise.
Rec(id, kind, v, prev, src) == [id |-> id, kind |-> kind, v |-> v, prev |-> prev, src |-> src]
Nil == Rec(NoId, "nil", 0, NoId, "na")

\* A tombstone names the epoch in which history continues. A seal of the old format has no forward
\* link: history continues in the next numeric epoch. A terminal record ends the life.
NextAfter(rec) ==
    CASE rec.kind = "seal" -> <<rec.v, 1>>
      [] rec.kind = "oseal" -> <<rec.id[1] + 1, 1>>
      [] rec.kind = "term" -> <<MaxEpoch + 1, 1>>
      [] OTHER -> <<rec.id[1], rec.id[2] + 1>>

RECURSIVE Chase(_)
Chase(id) == IF ~InIds(id) \/ store[id] = Nil THEN <<>>
             ELSE <<store[id]>> \o Chase(NextAfter(store[id]))

RECURSIVE ChaseEnd(_)
ChaseEnd(id) == IF ~InIds(id) \/ store[id] = Nil THEN id
                ELSE ChaseEnd(NextAfter(store[id]))

\* Committed history as a cold reader derives it at this instant: the snapshot, then the chain.
Hist == cov \o Chase(baseNext)

IsPrefix(a, b) == Len(a) <= Len(b) /\ SubSeq(b, 1, Len(a)) = a

\* One tracked ref exists from the start and protects one blob. The append with txn number `RmTxn`
\* removes it, and so does the terminal record; every other append touches other refs.
RmTxn == 1
Removes(seq) == \E i \in 1..Len(seq) : (seq[i].kind = "op" /\ seq[i].v = RmTxn) \/ seq[i].kind = "term"

\* The local lease check. A superseded writer passes it only when its clock undercounts.
Admitted(e) == e = live \/ (ClockLies /\ wst[e] # "none")

NoWalk == [on |-> FALSE, base |-> NoId, seq |-> <<>>, pos |-> NoId]
LegacySeal == Rec(<<1, 1>>, "oseal", 0, NoId, "na")

Init ==
    /\ store = [id \in Ids |-> IF Legacy /\ id = <<1, 1>> THEN LegacySeal ELSE Nil]
    /\ live = IF Legacy THEN 2 ELSE 1
    /\ wst = [e \in Epochs |-> IF Legacy
                                THEN (IF e = 1 THEN "deposed" ELSE IF e = 2 THEN "mounted" ELSE "none")
                                ELSE (IF e = 1 THEN "recovered" ELSE "none")]
    /\ mem = [e \in Epochs |-> <<>>]
    /\ tail = [e \in Epochs |-> Genesis]
    /\ pend = [e \in Epochs |-> Nil]
    /\ inflight = {}
    /\ acked = {}
    /\ ackedLive = {}
    /\ cov = <<>>
    /\ baseNext = Genesis
    /\ gcFold = <<>>
    /\ gcCur = NoId
    /\ nextTxn = 1
    /\ intent = [e \in Epochs |-> e = 1]
    /\ blobGone = FALSE
    /\ lastUnclean = IF Legacy THEN 1 ELSE 0
    /\ walk = [e \in Epochs |-> NoWalk]
    /\ ckptFlight = {}

\* A request belongs to the epoch that sent it: ops go to the sender's own epoch, a tombstone names it.
Belongs(r, e) == (r.kind \in {"op", "term"} /\ r.id[1] = e) \/ (r.kind = "seal" /\ r.v = e)
RecoverySeal(r) == r.kind = "seal" /\ r.src = "recovery"

\* Clean stop: the lanes are drained and a farewell is written. A recovery tombstone whose create was
\* unresolved is not a lane attempt and may still be travelling, and so may a snapshot publication:
\* the drain does not wait for it.
TakeoverClean ==
    /\ live < MaxEpoch
    /\ pend[live] = Nil \/ RecoverySeal(pend[live])
    /\ \A r \in inflight : Belongs(r, live) => RecoverySeal(r)
    /\ live' = live + 1
    /\ wst' = [wst EXCEPT ![live] = "deposed", ![live + 1] = "mounted"]
    /\ pend' = [pend EXCEPT ![live] = Nil]
    /\ walk' = [walk EXCEPT ![live] = NoWalk]
    /\ inflight' = IF LateLand THEN inflight ELSE {}
    /\ UNCHANGED <<store, mem, tail, acked, ackedLive, cov, baseNext, gcFold, gcCur, nextTxn, intent, blobGone,
                   lastUnclean, ckptFlight>>

\* Unclean stop: the lease expires and the next mount takes over. Under the lease assumption on
\* landings (`~LateLand`) no request of the old holder is still travelling.
TakeoverUnclean ==
    /\ live < MaxEpoch
    /\ live' = live + 1
    /\ wst' = [wst EXCEPT ![live + 1] = "mounted"]
    /\ inflight' = IF LateLand THEN inflight ELSE {}
    /\ ckptFlight' = IF LateLand THEN ckptFlight ELSE {}
    /\ lastUnclean' = live
    /\ UNCHANGED <<store, mem, tail, pend, acked, ackedLive, cov, baseNext, gcFold, gcCur, nextTxn, intent, blobGone,
                   walk>>

\* The chain of writer `e` ends in an epoch whose requests may still land, so `e` must close it
\* before it trusts its in-memory table.
NeedsEager(e) ==
    \/ EagerSeal
    \/ /\ SelectiveSeal
       /\ IF SelectiveImmediate THEN lastUnclean = live - 1 ELSE tail[e][1] <= lastUnclean
AtRest(e) == wst[e] = "recovered" /\ pend[e] = Nil /\ (tail[e][1] = e \/ ~NeedsEager(e))

\* Recovery writes nothing while it reads. It samples the base, reads one key per step, and after the
\* absent read compares the base again: cleanup deletes only below a base, so an unchanged base means
\* the absent key was never deleted.
WalkStart(e) ==
    /\ Admitted(e)
    /\ wst[e] = "mounted"
    /\ pend[e] = Nil
    /\ ~walk[e].on
    /\ walk' = [walk EXCEPT ![e] = [on |-> TRUE, base |-> baseNext, seq |-> cov, pos |-> baseNext]]
    /\ UNCHANGED <<store, live, wst, mem, tail, pend, inflight, acked, ackedLive, cov, baseNext, gcFold, gcCur,
                   nextTxn, intent, blobGone, lastUnclean, ckptFlight>>

WalkStep(e) ==
    /\ Admitted(e)
    /\ walk[e].on
    /\ InIds(walk[e].pos)
    /\ store[walk[e].pos] # Nil
    /\ walk' = [walk EXCEPT ![e].seq = Append(@, store[walk[e].pos]),
                            ![e].pos = NextAfter(store[walk[e].pos])]
    /\ UNCHANGED <<store, live, wst, mem, tail, pend, inflight, acked, ackedLive, cov, baseNext, gcFold, gcCur,
                   nextTxn, intent, blobGone, lastUnclean, ckptFlight>>

\* The absent read. A chain that ends in a higher epoch means this writer was deposed.
WalkEnd(e) ==
    /\ Admitted(e)
    /\ walk[e].on
    /\ IF InIds(walk[e].pos) THEN store[walk[e].pos] = Nil ELSE TRUE
    /\ IF ~WalkNoRecheck /\ baseNext # walk[e].base
       THEN /\ walk' = [walk EXCEPT ![e] = [on |-> TRUE, base |-> baseNext, seq |-> cov, pos |-> baseNext]]
            /\ UNCHANGED <<mem, tail, wst>>
       ELSE /\ mem' = [mem EXCEPT ![e] = walk[e].seq]
            /\ tail' = [tail EXCEPT ![e] = walk[e].pos]
            /\ wst' = [wst EXCEPT ![e] = IF walk[e].pos[1] > e THEN "deposed" ELSE "recovered"]
            /\ walk' = [walk EXCEPT ![e] = [on |-> FALSE, base |-> walk[e].base, seq |-> <<>>, pos |-> NoId]]
    /\ UNCHANGED <<store, live, pend, inflight, acked, ackedLive, cov, baseNext, gcFold, gcCur, nextTxn, intent,
                   blobGone, lastUnclean, ckptFlight>>

Intend(e) ==
    /\ Admitted(e)
    /\ wst[e] # "none"
    /\ ~intent[e]
    /\ intent' = [intent EXCEPT ![e] = TRUE]
    /\ UNCHANGED <<store, live, wst, mem, tail, pend, inflight, acked, ackedLive, cov, baseNext, gcFold, gcCur,
                   nextTxn, blobGone, lastUnclean, walk, ckptFlight>>

\* The backlink a record at the writer's next slot carries: the tombstone that selected this epoch.
PrevFor(e) ==
    IF tail[e][2] = 1 /\ Len(mem[e]) > 0 /\ mem[e][Len(mem[e])].kind \in {"seal", "oseal"}
    THEN mem[e][Len(mem[e])].id
    ELSE NoId

\* Close the epoch in which the chain ends, in the chain's next slot.
StartSeal(e) ==
    /\ ~SealSkip
    /\ Admitted(e)
    /\ wst[e] = "recovered"
    /\ pend[e] = Nil
    /\ tail[e][1] # e
    /\ InIds(tail[e])
    /\ NeedsEager(e) \/ intent[e]
    /\ LET rec == Rec(tail[e], "seal", e, PrevFor(e), IF NeedsEager(e) THEN "recovery" ELSE "lane") IN
        /\ pend' = [pend EXCEPT ![e] = rec]
        /\ inflight' = inflight \cup {rec}
    /\ UNCHANGED <<store, live, wst, mem, tail, acked, ackedLive, cov, baseNext, gcFold, gcCur, nextTxn, intent,
                   blobGone, lastUnclean, walk, ckptFlight>>

\* SABOTAGE `SealSkip`: jump to the writer's own epoch without closing the old one.
SkipSeal(e) ==
    /\ SealSkip
    /\ Admitted(e)
    /\ wst[e] = "recovered"
    /\ pend[e] = Nil
    /\ tail[e][1] # e
    /\ intent[e]
    /\ tail' = [tail EXCEPT ![e] = <<e, 1>>]
    /\ UNCHANGED <<store, live, wst, mem, pend, inflight, acked, ackedLive, cov, baseNext, gcFold, gcCur, nextTxn,
                   intent, blobGone, lastUnclean, walk, ckptFlight>>

NoTerm ==
    /\ \A id \in Ids : store[id].kind # "term"
    /\ \A r \in inflight : r.kind # "term"
    /\ \A e \in Epochs : pend[e].kind # "term"

\* An ordinary append, or `RemoveNamespace` (the last record of the life).
StartAppend(e, kind) ==
    /\ kind = "term" => (Terminal /\ NoTerm)
    /\ Admitted(e)
    /\ wst[e] = "recovered"
    /\ pend[e] = Nil
    /\ tail[e][1] = e
    /\ InIds(tail[e])
    /\ intent[e]
    /\ nextTxn <= MaxTxn
    /\ LET rec == Rec(tail[e], kind, nextTxn, PrevFor(e), "na") IN
        /\ pend' = [pend EXCEPT ![e] = rec]
        /\ inflight' = inflight \cup {rec}
    /\ nextTxn' = nextTxn + 1
    /\ UNCHANGED <<store, live, wst, mem, tail, acked, ackedLive, cov, baseNext, gcFold, gcCur, intent, blobGone,
                   lastUnclean, walk, ckptFlight>>

\* The exact attempt is sent again: same key, same bytes.
Resend(e) ==
    /\ Admitted(e)
    /\ pend[e] # Nil
    /\ pend[e] \notin inflight
    /\ inflight' = inflight \cup {pend[e]}
    /\ UNCHANGED <<store, live, wst, mem, tail, pend, acked, ackedLive, cov, baseNext, gcFold, gcCur, nextTxn, intent,
                   blobGone, lastUnclean, walk, ckptFlight>>

\* The store applies a create-if-absent.
Land(r) ==
    /\ r \in inflight
    /\ inflight' = inflight \ {r}
    /\ store' = IF store[r.id] = Nil \/ (SealBlind /\ r.kind = "seal")
                THEN [store EXCEPT ![r.id] = r]
                ELSE store
    /\ UNCHANGED <<live, wst, mem, tail, pend, acked, ackedLive, cov, baseNext, gcFold, gcCur, nextTxn, intent,
                   blobGone, lastUnclean, walk, ckptFlight>>

\* The writer learns the outcome of its attempt by the response or by an exact read of the slot.
\* Own bytes in the slot: install and acknowledge, after the lease recheck. A foreign occupant: the
\* attempt is refused and the chain must be read again.
\* A tombstone goes into a slot of ANOTHER epoch. Its create proves that slot was the end of the chain
\* only if the base did not move since the walk: otherwise cleanup may have emptied the slot. So
\* recovery re-reads the base after its tombstone create and reads the chain again when it moved. A
\* lane tombstone needs no re-read: it is written only when the chain's epoch ended with a clean
\* farewell, so no writer of that epoch can still move the base past the slot.
Resolve(e) ==
    /\ pend[e] # Nil
    /\ store[pend[e].id] # Nil
    /\ LET p == pend[e]
           stale == RecoverySeal(p) /\ ~SealNoRecheck /\ baseNext # walk[e].base
       IN
        IF store[p.id] = p
        THEN IF Admitted(e)
             THEN IF stale
                  THEN /\ wst' = [wst EXCEPT ![e] = "mounted"]
                       /\ UNCHANGED <<mem, tail, acked, ackedLive>>
                  ELSE /\ mem' = [mem EXCEPT ![e] = Append(@, p)]
                       /\ tail' = [tail EXCEPT ![e] = NextAfter(p)]
                       /\ acked' = IF p.kind = "op" THEN acked \cup {p.v} ELSE acked
                       /\ ackedLive' = IF p.kind = "op" /\ e = live THEN ackedLive \cup {p.v} ELSE ackedLive
                       /\ wst' = wst
             ELSE /\ wst' = [wst EXCEPT ![e] = "deposed"]
                  /\ UNCHANGED <<mem, tail, acked, ackedLive>>
        ELSE /\ wst' = [wst EXCEPT ![e] = IF Admitted(e) THEN "mounted" ELSE "deposed"]
             /\ UNCHANGED <<mem, tail, acked, ackedLive>>
    /\ pend' = [pend EXCEPT ![e] = Nil]
    /\ UNCHANGED <<store, live, inflight, cov, baseNext, gcFold, gcCur, nextTxn, intent, blobGone,
                   lastUnclean, walk, ckptFlight>>

\* A writer that passes its lease check sends a snapshot of its installed state. Only an epoch that
\* wrote the namespace does this, and a tombstone is never a snapshot id.
SnapshotSend(e) ==
    /\ Admitted(e)
    /\ wst[e] = "recovered"
    /\ pend[e] = Nil
    /\ tail[e][1] = e
    /\ Len(mem[e]) > Len(cov)
    /\ mem[e][Len(mem[e])].kind = "op"
    /\ ckptFlight' = ckptFlight \cup {[e |-> e, cov |-> mem[e], next |-> tail[e]]}
    /\ UNCHANGED <<store, live, wst, mem, tail, pend, inflight, acked, ackedLive, cov, baseNext, gcFold, gcCur,
                   nextTxn, intent, blobGone, lastUnclean, walk>>

\* The `_ckpt` publication lands. A snapshot id that does not increase in id order writes nothing.
IdLess(a, b) == a[1] < b[1] \/ (a[1] = b[1] /\ a[2] < b[2])
SnapshotLand(c) ==
    /\ c \in ckptFlight
    /\ ckptFlight' = ckptFlight \ {c}
    /\ IF IdLess(baseNext, c.next)
       THEN cov' = c.cov /\ baseNext' = c.next
       ELSE UNCHANGED <<cov, baseNext>>
    /\ UNCHANGED <<store, live, wst, mem, tail, pend, inflight, acked, ackedLive, gcFold, gcCur, nextTxn, intent,
                   blobGone, lastUnclean, walk>>

\* Ref cleanup deletes a log strictly below the snapshot id that GC has folded, except the tombstone
\* that the snapshot's own log names. A tombstone is otherwise deleted like any log.
Cleanup ==
    /\ ~NoCleanup
    /\ \E i \in 1..Len(cov) :
        /\ i < Len(cov)
        /\ i <= Len(gcFold)
        /\ cov[i].id # cov[Len(cov)].prev
        /\ store[cov[i].id] = cov[i]
        /\ store' = [store EXCEPT ![cov[i].id] = Nil]
    /\ UNCHANGED <<live, wst, mem, tail, pend, inflight, acked, ackedLive, cov, baseNext, gcFold, gcCur, nextTxn,
                   intent, blobGone, lastUnclean, walk, ckptFlight>>

\* GC knows only the id of the last record it folded. The object at that key is intact iff the cursor
\* is at or above the snapshot id; below it cleanup may have deleted the object and a late request may
\* have created another one there. Then GC finds the successor from records above the cursor: it
\* walks back from the head of the snapshot's epoch through the backlinks. A head that names the
\* cursor: the cursor was a tombstone and that head follows it. The walk reaches the cursor's epoch
\* without naming it: the cursor was ordinary. A head without a backlink on the way: no proof, hold.
SameNext(c) == <<c[1], c[2] + 1>>
CursorIntact == Len(gcFold) >= Len(cov)
HeadsNaming(c) == {h \in Ids : h[2] = 1 /\ h[1] > c[1] /\ store[h] # Nil /\ store[h].prev = c}
RECURSIVE BackChainSucc(_, _)
BackChainSucc(ep, c) ==
    IF ep <= c[1] THEN SameNext(c)
    ELSE IF store[<<ep, 1>>] = Nil \/ store[<<ep, 1>>].prev = NoId THEN NoId
    ELSE IF store[<<ep, 1>>].prev = c THEN <<ep, 1>>
    ELSE IF store[<<ep, 1>>].prev[1] < c[1] THEN NoId
    ELSE BackChainSucc(store[<<ep, 1>>].prev[1], c)
GcNext ==
    IF gcCur = NoId THEN Genesis
    ELSE IF GcCursorNaive
         THEN IF InIds(SameNext(gcCur)) /\ store[SameNext(gcCur)] # Nil THEN SameNext(gcCur)
              ELSE IF store[gcCur] # Nil THEN NextAfter(store[gcCur])
              ELSE SameNext(gcCur)
    ELSE IF GcCursorProbeFirst
         THEN IF InIds(SameNext(gcCur)) /\ store[SameNext(gcCur)] # Nil THEN SameNext(gcCur)
              ELSE IF store[gcCur] # Nil THEN NextAfter(store[gcCur])
              ELSE IF HeadsNaming(gcCur) # {} THEN CHOOSE h \in HeadsNaming(gcCur) : TRUE
              ELSE SameNext(gcCur)
    ELSE IF CursorIntact THEN NextAfter(store[gcCur])
    ELSE IF GcCursorAnyHead
         THEN IF HeadsNaming(gcCur) # {} THEN CHOOSE h \in HeadsNaming(gcCur) : TRUE ELSE SameNext(gcCur)
    ELSE BackChainSucc(cov[Len(cov)].id[1], gcCur)

\* GC reads its expected-next key. Present: fold it. Absent: this round's cut.
GcStep ==
    /\ InIds(GcNext)
    /\ store[GcNext] # Nil
    /\ gcFold' = Append(gcFold, store[GcNext])
    /\ gcCur' = GcNext
    /\ UNCHANGED <<store, live, wst, mem, tail, pend, inflight, acked, ackedLive, cov, baseNext, nextTxn, intent,
                   blobGone, lastUnclean, walk, ckptFlight>>

\* GC has folded the removal of the tracked ref, so the blob's in-degree is zero and a later pass
\* deletes it. Condemnation and graduation rounds are collapsed into this one step.
GcDelete ==
    /\ ~blobGone
    /\ Removes(gcFold)
    /\ blobGone' = TRUE
    /\ UNCHANGED <<store, live, wst, mem, tail, pend, inflight, acked, ackedLive, cov, baseNext, gcFold, gcCur,
                   nextTxn, intent, lastUnclean, walk, ckptFlight>>

Next ==
    \/ TakeoverClean
    \/ TakeoverUnclean
    \/ \E e \in Epochs : WalkStart(e) \/ WalkStep(e) \/ WalkEnd(e) \/ Intend(e) \/ StartSeal(e) \/ SkipSeal(e)
                         \/ StartAppend(e, "op") \/ StartAppend(e, "term") \/ Resend(e) \/ Resolve(e)
                         \/ SnapshotSend(e)
    \/ \E r \in inflight : Land(r)
    \/ \E c \in ckptFlight : SnapshotLand(c)
    \/ Cleanup
    \/ GcStep
    \/ GcDelete

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* Every acknowledged append is in the history a cold reader derives.
INV_ACKED_REACHABLE ==
    \A t \in acked : \E i \in 1..Len(Hist) : Hist[i].kind = "op" /\ Hist[i].v = t

\* Every append acknowledged by the lease holder is in history, also when a superseded writer with a
\* lying clock keeps writing.
INV_LIVE_ACKED_REACHABLE ==
    \A t \in ackedLive : \E i \in 1..Len(Hist) : Hist[i].kind = "op" /\ Hist[i].v = t

\* GC never folds a record that is not history, and folds history in order.
INV_GC_PREFIX == IsPrefix(gcFold, Hist)

\* When GC's expected-next key is absent, GC has folded all of history: an absent key is a valid cut.
INV_GC_CUT_SOUND == (GcNext # NoId /\ (IF InIds(GcNext) THEN store[GcNext] = Nil ELSE TRUE)) => gcFold = Hist

\* GC always finds the successor of its cursor: a deleted cursor has a durable successor and a proof.
INV_GC_NO_HOLD == GcNext # NoId

\* No writer holds a record that is not history.
INV_WRITER_PREFIX == \A e \in Epochs : wst[e] = "recovered" => IsPrefix(mem[e], Hist)

\* The live owner at rest holds exactly the committed history. This is the read-window property:
\* it needs the lease assumption on late landings, or a tombstone before the table is trusted.
INV_OWNER_EXACT == AtRest(live) => mem[live] = Hist

\* GC never deletes a blob that the live owner at rest still resolves from memory.
INV_NO_DELETE_SERVABLE == (AtRest(live) /\ ~Removes(mem[live])) => ~blobGone

\* A head names exactly the tombstone that selected its epoch.
INV_RECIPROCAL ==
    \A i \in 1..Len(Hist) :
        (i > 1 /\ Hist[i - 1].kind \in {"seal", "oseal"}) => Hist[i].prev = Hist[i - 1].id

\* An epoch that was not asked to write is named by no object.
INV_NO_TRACE ==
    \A id \in Ids : store[id] # Nil =>
        /\ intent[id[1]]
        /\ store[id].kind = "seal" => intent[store[id].v]

\* With the selective rule a tombstone may name an epoch that was not asked to write, but only to
\* close an epoch that may have stopped uncleanly.
INV_NO_TRACE_SELECTIVE ==
    \A id \in Ids : store[id].kind = "seal" => (intent[store[id].v] \/ id[1] <= lastUnclean)

\* History only grows: a record that any reader could count as history stays history.
PROP_MONOTONE == [][IsPrefix(Hist, Hist')]_vars

\* Nothing follows a terminal record.
PROP_TERMINAL_FINAL == [][(Len(Hist) > 0 /\ Hist[Len(Hist)].kind = "term") => Hist' = Hist]_vars

\* Negated reachability witnesses.
W_SealHappens == \A id \in Ids : store[id].kind # "seal"
W_SkipEpochHappens == \A id \in Ids : store[id].kind = "seal" => store[id].v = id[1] + 1
\* A writer holds an op of another epoch that no client was ever told about.
W_AdoptHappens ==
    \A e \in Epochs : \A i \in 1..Len(mem[e]) :
        mem[e][i].kind = "op" => (mem[e][i].v \in acked \/ mem[e][i].id[1] = e)
\* An epoch after a clean stop rests on a chain of another epoch without a tombstone.
W_LazyAfterCleanHappens == \A e \in Epochs : ~(e > 1 /\ AtRest(e) /\ tail[e][1] # e /\ Len(mem[e]) > 0)
\* A writer stopped between its tombstone and its first append; a successor closed the seal-only epoch.
W_SealOnlyEpochHappens == \A id \in Ids : ~(store[id].kind = "seal" /\ id[2] = 1 /\ id[1] > 1)
\* A tombstone attempt found a foreign occupant in its slot.
W_SealConflictHappens ==
    \A e \in Epochs : ~(pend[e].kind = "seal" /\ store[pend[e].id] # Nil /\ store[pend[e].id] # pend[e])
\* A tombstone was still travelling when its writer was superseded, and landed afterwards.
W_LateSealHappens ==
    \A id \in Ids : ~(store[id].kind = "seal" /\ store[id].v < live /\ pend[store[id].v] = store[id])
\* A recovery tombstone landed after its writer's clean farewell.
W_SealAfterFarewellHappens ==
    \A id \in Ids : ~(RecoverySeal(store[id]) /\ wst[store[id].v] = "deposed" /\ store[id].v > lastUnclean
                      /\ \A e \in Epochs : \A i \in 1..Len(mem[e]) : mem[e][i] # store[id])
W_TerminalHappens == \A id \in Ids : store[id].kind # "term"
\* A chain holds a seal of the old format followed by a tombstone with a forward link.
W_MixedChainHappens ==
    ~(\E i, j \in 1..Len(Hist) : i < j /\ Hist[i].kind = "oseal" /\ Hist[j].kind = "seal")
W_CleanupSealHappens == \A i \in 1..Len(cov) : cov[i].kind = "seal" => store[cov[i].id] = cov[i]
\* The base moved while a recovery walk was in progress.
W_BaseMovedUnderWalkHappens == \A e \in Epochs : ~(walk[e].on /\ walk[e].base # baseNext)
\* Cleanup deleted the object at the GC cursor.
W_CursorDeletedHappens == ~(gcCur # NoId /\ store[gcCur] = Nil)
\* A superseded writer created an object at the key of the GC cursor.
W_CursorReusedHappens == ~(gcCur # NoId /\ store[gcCur] # Nil /\ store[gcCur] # gcFold[Len(gcFold)])
=============================================================================
