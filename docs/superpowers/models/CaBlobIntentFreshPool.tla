---------------------------- MODULE CaBlobIntentFreshPool ----------------------------
(*
Section 7.4 of docs/superpowers/specs/2026-11-17-cas-blob-deletion-intents-design.md: a pool created
by this build switches itself. States N0 to N4, a crash after every step, concurrent creators, a
creator with another token dialect, a mount whose mark probe did not pass, an older build that races
the creation.

Objects: the canonical empty catalog, `_pool_meta`, `gc/state`. Every step of the sequence is a
create-if-absent or a read-modify-write that decides on the body it reads. The two writes of the
switch (stamp, switch) are send and apply: a crashed mount leaves its conditional write in flight.

What is NOT here: the blob protocol itself (CaBlobIntentProtocol), the operator command and its slot
checks, more than two mounts.
*)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
  Mounts,        \* writable mounts of this build (model values)
  OtherDialect,  \* the mounts whose backend dialect is "b"; the others use "a"
  BadProbe,      \* the mounts whose mark probe did not pass, or was skipped
  OldCreator,    \* an older build races the creation of the pool
  MaxCrashes,    \* crashes in total
  Sab            \* sabotage switches

KnownSab ==
  {"switch_before_stamp",       \* the sequence runs step 3 of section 7.3 before step 2
   "auto_switch_upgraded",      \* the sequence runs on a pool without `created_generation`
   "writer_before_sequence",    \* the mount admits writers before the sequence ends
   "no_dialect_rule"}           \* the switch of a second creator accepts a recorded foreign dialect

Has(s) == s \in Sab
Dial(m) == IF m \in OtherDialect THEN "b" ELSE "a"
ProbeOk(m) == m \notin BadProbe

NoMeta == [here |-> FALSE, by |-> "none", floor4 |-> FALSE, sw |-> FALSE, dial |-> "none"]

VARIABLES
  cat,       \* the canonical empty catalog exists
  pmeta,     \* `_pool_meta`: [here, by ("new": created_generation = 4), floor4, sw, dial]
  gst,       \* `gc/state`: "absent" | "g3" | "g4"
  mt,        \* per mount: [ph, learned]
  old,       \* the older build's process: "none" | "racing" | "up" | "refused"
  pw,        \* conditional writes in flight: [k ("stamp" | "switch"), preg, prem, dial]
  crashes,
  crashedAt, \* ghost: the durable states (N0..N3) at which some mount crashed
  fDialRewritten

vars == <<cat, pmeta, gst, mt, old, pw, crashes, crashedAt, fDialRewritten>>

M0 == [ph |-> "Down", learned |-> FALSE]

Init ==
  /\ cat = FALSE /\ pmeta = NoMeta /\ gst = "absent"
  /\ mt = [m \in Mounts |-> M0]
  /\ old = IF OldCreator THEN "racing" ELSE "none"
  /\ pw = {} /\ crashes = 0 /\ crashedAt = {} /\ fDialRewritten = FALSE

(* The durable state of the table in section 7.4. *)
NState ==
  IF ~pmeta.here THEN (IF cat THEN "N1" ELSE "N0")
  ELSE IF pmeta.sw THEN "N4"
  ELSE IF gst = "g4" THEN "N3" ELSE "N2"

(* The sequence of 7.4 applies: `created_generation = 4`, not switched, the mark probe passed. *)
RunsSequence(m) == (pmeta.by = "new" \/ Has("auto_switch_upgraded")) /\ ~pmeta.sw /\ ProbeOk(m)

(* A writable mount starts: it resumes at the state it reads. N0 -> create the catalog. N1 -> create
   `_pool_meta`. Otherwise validate the body: a switched pool with another dialect refuses the mount
   (section 6.1, the dialect rule); every writable mount raises the floor to 4. *)
MRead(m) ==
  /\ mt[m].ph = "Down"
  /\ IF ~pmeta.here
       THEN /\ mt' = [mt EXCEPT ![m].ph = IF cat THEN "MkMeta" ELSE "MkCat"]
            /\ UNCHANGED pmeta
       ELSE /\ ~(pmeta.sw /\ pmeta.dial /= Dial(m))
            /\ pmeta' = [pmeta EXCEPT !.floor4 = TRUE]
            /\ mt' = [mt EXCEPT ![m].ph = "Seq"]
  /\ UNCHANGED <<cat, gst, old, pw, crashes, crashedAt, fDialRewritten>>

(* N0: create the empty catalog (create-if-absent). *)
MCreateCatalog(m) ==
  /\ mt[m].ph = "MkCat"
  /\ cat' = TRUE
  /\ mt' = [mt EXCEPT ![m].ph = "MkMeta"]
  /\ UNCHANGED <<pmeta, gst, old, pw, crashes, crashedAt, fDialRewritten>>

(* N1: create `_pool_meta` unswitched, floor 4, `created_generation = 4`. One create wins; the loser
   validates the winner's body (CasPoolMeta.cpp:176-182) and raises the floor. *)
MCreateMeta(m) ==
  /\ mt[m].ph = "MkMeta"
  /\ pmeta' = IF pmeta.here THEN [pmeta EXCEPT !.floor4 = TRUE]
              ELSE [here |-> TRUE, by |-> "new", floor4 |-> TRUE, sw |-> FALSE, dial |-> "none"]
  /\ mt' = [mt EXCEPT ![m].ph = "Seq"]
  /\ UNCHANGED <<cat, gst, old, pw, crashes, crashedAt, fDialRewritten>>

(* The end of `mountWritable`: the sequence runs before `Pool::open` returns, or the mount is
   admitted as it is (not learned on an unswitched pool; learned on a switched one). *)
MDecide(m) ==
  /\ mt[m].ph = "Seq"
  /\ ~(pmeta.sw /\ pmeta.dial /= Dial(m))
  /\ mt' = [mt EXCEPT ![m] =
       IF RunsSequence(m) /\ ~Has("writer_before_sequence")
         THEN [@ EXCEPT !.ph = IF Has("switch_before_stamp") THEN "Switch" ELSE "Stamp"]
         ELSE [ph |-> "Up", learned |-> pmeta.sw /\ ProbeOk(m)]]
  /\ UNCHANGED <<cat, pmeta, gst, old, pw, crashes, crashedAt, fDialRewritten>>

(* Step 2 of section 7.3, the stamp: read-modify-write of `gc/state`. Generation 4: no write.
   Otherwise a conditional write is sent. *)
MStampSend(m) ==
  /\ mt[m].ph = "Stamp"
  /\ IF gst = "g4"
       THEN mt' = [mt EXCEPT ![m].ph = IF Has("switch_before_stamp") THEN "Learn" ELSE "Switch"] /\ pw' = pw
       ELSE mt' = [mt EXCEPT ![m].ph = "StampWait"] /\ pw' = pw \cup {[k |-> "stamp", preg |-> gst, prem |-> NoMeta, dial |-> "none"]}
  /\ UNCHANGED <<cat, pmeta, gst, old, crashes, crashedAt, fDialRewritten>>

(* The stamp is confirmed by an answered write or by a read that shows generation 4. *)
MStampConfirm(m) ==
  /\ mt[m].ph = "StampWait" /\ gst = "g4"
  /\ mt' = [mt EXCEPT ![m].ph = IF Has("switch_before_stamp") THEN "Learn" ELSE "Switch"]
  /\ UNCHANGED <<cat, pmeta, gst, old, pw, crashes, crashedAt, fDialRewritten>>

(* The stamp was refused by its condition (another writer changed `gc/state`): decide again. *)
MStampRetry(m) ==
  /\ mt[m].ph = "StampWait" /\ gst /= "g4" /\ \A w \in pw : w.k /= "stamp" \/ w.preg /= gst
  /\ mt' = [mt EXCEPT ![m].ph = "Stamp"]
  /\ UNCHANGED <<cat, pmeta, gst, old, pw, crashes, crashedAt, fDialRewritten>>

(* Step 3, the switch: read-modify-write of `_pool_meta`, decided on the body it reads. Not
   switched: write `intents` and this backend's dialect. Switched with the same dialect: no write.
   Switched with another dialect: refused, and the mount with it. *)
MSwitchSend(m) ==
  /\ mt[m].ph = "Switch"
  /\ IF pmeta.sw
       THEN /\ (pmeta.dial = Dial(m) \/ Has("no_dialect_rule"))
            /\ mt' = [mt EXCEPT ![m].ph = IF Has("switch_before_stamp") THEN "Stamp" ELSE "Learn"]
            /\ pw' = pw
       ELSE /\ mt' = [mt EXCEPT ![m].ph = "SwitchWait"]
            /\ pw' = pw \cup {[k |-> "switch", preg |-> "none", prem |-> pmeta, dial |-> Dial(m)]}
  /\ UNCHANGED <<cat, pmeta, gst, old, crashes, crashedAt, fDialRewritten>>

MSwitchRefused(m) ==
  /\ mt[m].ph = "Switch" /\ pmeta.sw /\ pmeta.dial /= Dial(m) /\ ~Has("no_dialect_rule")
  /\ mt' = [mt EXCEPT ![m] = M0]
  /\ UNCHANGED <<cat, pmeta, gst, old, pw, crashes, crashedAt, fDialRewritten>>

(* The switch write answered, or an exact read settles it: decide again on the current body. *)
MSwitchSettle(m) ==
  /\ mt[m].ph = "SwitchWait"
  /\ (pmeta.sw \/ \A w \in pw : w.k /= "switch" \/ w.prem /= pmeta)
  /\ mt' = [mt EXCEPT ![m].ph = "Switch"]
  /\ UNCHANGED <<cat, pmeta, gst, old, pw, crashes, crashedAt, fDialRewritten>>

(* Step 4: this server learns (section 3.1: `intents`, its own dialect, probe passed); `Pool::open`
   returns and writers and GC rounds of this mount may exist. *)
MLearn(m) ==
  /\ mt[m].ph = "Learn"
  /\ mt' = [mt EXCEPT ![m] = [ph |-> "Up", learned |-> pmeta.sw /\ (pmeta.dial = Dial(m) \/ Has("no_dialect_rule"))]]
  /\ UNCHANGED <<cat, pmeta, gst, old, pw, crashes, crashedAt, fDialRewritten>>

(* A mount that was admitted not learned learns at a later read point (section 3.1). A dialect that
   differs keeps it not learned for its life. *)
MLearnLater(m) ==
  /\ mt[m].ph = "Up" /\ ~mt[m].learned /\ pmeta.sw /\ pmeta.dial = Dial(m) /\ ProbeOk(m)
  /\ mt' = [mt EXCEPT ![m].learned = TRUE]
  /\ UNCHANGED <<cat, pmeta, gst, old, pw, crashes, crashedAt, fDialRewritten>>

(* The first GC lease of an admitted mount creates `gc/state` at header 4 (section 3.5). *)
MGcBootstrap(m) ==
  /\ mt[m].ph = "Up" /\ gst = "absent"
  /\ gst' = "g4"
  /\ UNCHANGED <<cat, pmeta, mt, old, pw, crashes, crashedAt, fDialRewritten>>

(* Store: a conditional write applies if its condition still holds; otherwise it is refused. *)
WriteApply(w) ==
  /\ w \in pw
  /\ pw' = pw \ {w}
  /\ IF w.k = "stamp"
       THEN /\ gst' = IF gst = w.preg THEN "g4" ELSE gst
            /\ UNCHANGED <<pmeta, fDialRewritten>>
       ELSE /\ pmeta' = IF pmeta = w.prem THEN [pmeta EXCEPT !.sw = TRUE, !.dial = w.dial] ELSE pmeta
            /\ fDialRewritten' = (fDialRewritten \/ (pmeta = w.prem /\ pmeta.dial /= "none"))
            /\ UNCHANGED gst
  /\ UNCHANGED <<cat, mt, old, crashes, crashedAt>>

(* A mount dies after any step. Its writes in flight stay. It restarts at MRead. *)
Crash(m) ==
  /\ crashes < MaxCrashes /\ mt[m].ph /= "Down"
  /\ mt' = [mt EXCEPT ![m] = M0]
  /\ crashes' = crashes + 1
  /\ crashedAt' = crashedAt \cup {NState}
  /\ UNCHANGED <<cat, pmeta, gst, old, pw, fDialRewritten>>

(* An older build that races the creation. If its `_pool_meta` wins, the pool has no
   `created_generation`. If it reads a body at floor 4 it stops. *)
OldCreate ==
  /\ old = "racing"
  /\ IF pmeta.here
       THEN old' = (IF pmeta.floor4 THEN "refused" ELSE "up") /\ UNCHANGED <<cat, pmeta>>
       ELSE /\ cat' = TRUE
            /\ pmeta' = [here |-> TRUE, by |-> "old", floor4 |-> FALSE, sw |-> FALSE, dial |-> "none"]
            /\ old' = "up"
  /\ UNCHANGED <<gst, mt, pw, crashes, crashedAt, fDialRewritten>>

(* The older build's GC bootstraps `gc/state` at its own generation. *)
OldGcBootstrap ==
  /\ old = "up" /\ gst = "absent"
  /\ gst' = "g3"
  /\ UNCHANGED <<cat, pmeta, mt, old, pw, crashes, crashedAt, fDialRewritten>>

Next ==
  \/ \E m \in Mounts :
       \/ MRead(m) \/ MCreateCatalog(m) \/ MCreateMeta(m) \/ MDecide(m)
       \/ MStampSend(m) \/ MStampConfirm(m) \/ MStampRetry(m)
       \/ MSwitchSend(m) \/ MSwitchRefused(m) \/ MSwitchSettle(m)
       \/ MLearn(m) \/ MLearnLater(m) \/ MGcBootstrap(m) \/ Crash(m)
  \/ \E w \in pw : WriteApply(w)
  \/ OldCreate \/ OldGcBootstrap

Spec == Init /\ [][Next]_vars

Fairness ==
  /\ \A m \in Mounts :
       /\ WF_vars(MRead(m)) /\ WF_vars(MCreateCatalog(m)) /\ WF_vars(MCreateMeta(m)) /\ WF_vars(MDecide(m))
       /\ WF_vars(MStampSend(m)) /\ WF_vars(MStampConfirm(m)) /\ WF_vars(MStampRetry(m))
       /\ WF_vars(MSwitchSend(m)) /\ WF_vars(MSwitchSettle(m)) /\ WF_vars(MLearn(m)) /\ WF_vars(MLearnLater(m))
  /\ WF_vars(\E w \in pw : WriteApply(w))
LiveSpec == Spec /\ Fairness

--------------------------------------------------------------------------------------
TypeOK ==
  /\ Sab \subseteq KnownSab /\ OtherDialect \subseteq Mounts /\ BadProbe \subseteq Mounts
  /\ cat \in BOOLEAN /\ gst \in {"absent", "g3", "g4"}
  /\ pmeta.by \in {"none", "new", "old"} /\ pmeta.dial \in {"none", "a", "b"}
  /\ \A m \in Mounts : mt[m].ph \in {"Down", "MkCat", "MkMeta", "Seq", "Stamp", "StampWait", "Switch",
                                      "SwitchWait", "Learn", "Up"}
  /\ old \in {"none", "racing", "up", "refused"}

(* Section 7.3: the switch is written only after the stamp is confirmed. *)
FP_SWITCH_NEEDS_STAMP == pmeta.sw => gst = "g4"

(* Section 7.4, "the residual checks stay": `_pool_meta` is written before `gc/state`. *)
FP_NO_GC_STATE_WITHOUT_POOL_META == gst /= "absent" => pmeta.here

(* Section 3.1: a learned mount is on a switched pool that records its own dialect, probe passed. *)
FP_LEARNED_IS_SOUND ==
  \A m \in Mounts : mt[m].learned => (pmeta.sw /\ pmeta.dial = Dial(m) /\ ProbeOk(m))

(* Section 7.4: on a pool created by this build, a mount whose probe passed has no writer and no GC
   round before the pool is switched. *)
FP_SWITCHED_BEFORE_FIRST_WRITER ==
  \A m \in Mounts : (mt[m].ph = "Up" /\ ProbeOk(m) /\ pmeta.by = "new") => pmeta.sw

(* A8 without anyone's statement: no older build is admitted to a pool this build created, and a
   pool an older build created is never switched by the sequence. *)
FP_NO_OLD_MEMBER_ON_SWITCHED_POOL == (old = "up" => pmeta.by = "old") /\ (pmeta.by = "old" => ~pmeta.sw)

(* Section 6.1: `token_dialect` is written once. *)
FP_DIALECT_WRITTEN_ONCE == ~fDialRewritten

(* Witnesses, each an invariant that must FAIL. *)
WIT_T3_AFTER_CRASH_AT_N2 == ~(pmeta.sw /\ "N2" \in crashedAt /\ \E m \in Mounts : mt[m].learned)
WIT_T3_AFTER_CRASH_AT_N3 == ~(pmeta.sw /\ "N3" \in crashedAt /\ \E m \in Mounts : mt[m].learned)
WIT_T3_AFTER_CRASH_AT_N0_N1 == ~(pmeta.sw /\ {"N0", "N1"} \subseteq crashedAt)
WIT_LATE_SWITCH_WRITE ==          \* the switch lands while its sender is down
  ~(\E w \in pw : w.k = "switch" /\ w.prem = pmeta /\ \A m \in Mounts : mt[m].ph = "Down")
WIT_FOREIGN_DIALECT_REFUSED ==    \* a creator with another dialect meets a switched pool
  \A m \in Mounts : ~(mt[m].ph \in {"Switch", "Seq"} /\ pmeta.sw /\ pmeta.dial /= Dial(m))
WIT_BAD_PROBE_ADMITTED_UNSWITCHED == \A m \in Mounts : ~(mt[m].ph = "Up" /\ ~ProbeOk(m) /\ ~pmeta.sw)
WIT_UPGRADED_POOL == ~(pmeta.by = "old" /\ \E m \in Mounts : mt[m].ph = "Up")
WIT_EARLY_T2 == ~(gst = "g4" /\ ~pmeta.sw /\ \E m \in Mounts : mt[m].ph = "Up")

(* Liveness: a pool created by this build ends switched, and every mount that can learn has
   learned, once crashes stop. Checked with every mount on one dialect and a passing probe. *)
LIVE_REACHES_T3 == <>[](pmeta.sw /\ \A m \in Mounts : mt[m].learned)
======================================================================================
