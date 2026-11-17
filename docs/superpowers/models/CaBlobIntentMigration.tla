---------------------------- MODULE CaBlobIntentMigration ----------------------------
(*
Model of the offline migration of docs/superpowers/specs/2026-11-17-cas-blob-deletion-intents-design.md,
section 7: `clickhouse-disks cas-migrate-pool`, steps M1 to M9, the requests of stopped older members
that land late (section 7.4), older members that start or resume (R2, R4), and then one writer, one
linker and one GC of this build (sections 3.1, 3.2, 4.2).

One blob key B, one server root that an older member may own. Every action names the rule it stands
for (section and rule id). A rule that is not in the specification is not in this model.

The older world is SEEDED, not run: Init picks every combination of body, marker, `gc/state` row,
older committed ref and late requests that `Consistent` admits. `Consistent` is the argument of
section 7.4, "Which tokens a late delete can name": while `t` is current and under older delete
authority, the marker is `Condemned`, no `Clean` write can still land on it, and no older writer
adopted `t`. The model takes that argument as given; `Anomaly` drops one clause of it at a time
(cases (a) to (e) of section 7.4). An older member that starts or resumes runs a reduced older
build: the marker writer (W:424-475, W:509, W:368) and the marker GC (G:2194, G:2264-2287, G:583-595).

Abstractions and bounds are listed at the end of the file.
*)
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS
  Tools,          \* concurrent runs of the tool (model values)
  OldMember,      \* "none" | "start" | "start_before_recheck" | "suspended_mounted" |
                  \* "suspended_in_mount_seq" | "suspended_gc_leader"
  StoodClock,     \* a resumed member's boot clock stood still: it works before its next renewal (R2 row 1)
  SeedRows,       \* `gc/state` rows the seed may hold: subset of {"none", "cand", "pend"}
  SeedMetas,      \* markers the seed may hold: subset of {"absent", "clean", "cond"}
  SeedBodyAbsent, \* the seed may have no body at B
  SeedRef,        \* the seed may have a ref committed by the older build
  Late,           \* late older requests the seed may hold: "del", "put", "mclean_create", "mclean_swap",
                  \* "mcond", "mdel", "gccommit", "gclease"
  Anomaly,        \* clauses of `Consistent` dropped: "marker" (cases (a) to (c)), "ref" (R3),
                  \* "listing" (case (d)), "recur" (case (e))
  AllBodies,      \* --intents-for-all-bodies
  Faults,         \* a tool intent create may stay unanswered (and land later, or never)
  Linker,         \* the no-bytes reference path of this build (section 4.2)
  MaxCrash,       \* tool crashes in total
  MaxRuns,        \* tool starts in total, reruns included
  MaxNewPuts,     \* publications of the writer of this build
  MaxOldPuts,     \* publications of an older member's writer
  MaxVer,         \* `gc/state` writes in total
  Sab

KnownSab ==
  {"floor_before_intents",    \* S12: M8 runs right after M4
   "floor_before_gc_state",   \* M8 runs before M6
   "markers_before_floor",    \* Q8: M9 runs before M8
   "unconfirmed_create",      \* S13: M5 accepts a create it could not confirm
   "no_recheck",              \* M7 passes without reading
   "gc_state_not_rewritten",  \* M6 is skipped
   "rows_only"}               \* M5 ignores the markers M4 read

Has(s) == s \in Sab
LateOk(k) == k \in Late
Anom(a) == a \in Anomaly

\* Tokens: 1 is the seed body, 2 an older token already replaced, fresh tokens from 3 (A1).
MaxTok == 3 + MaxNewPuts + MaxOldPuts
Tokens == 1..MaxTok

NoBody == [p |-> FALSE, t |-> 0]
Row(st, t, c) == [st |-> st, tok |-> t, conf |-> c]
NoRow == Row("none", 0, FALSE)

\* The tool's steps in the order of section 7.1, "Order"; the sabotages permute them.
Ord ==
  IF Has("floor_before_intents") THEN <<"M1", "M2a", "M2b", "M3", "M4", "M8", "M5", "M6", "M7", "M9">>
  ELSE IF Has("floor_before_gc_state") THEN <<"M1", "M2a", "M2b", "M3", "M4", "M5", "M8", "M6", "M7", "M9">>
  ELSE IF Has("markers_before_floor") THEN <<"M1", "M2a", "M2b", "M3", "M4", "M5", "M6", "M7", "M9", "M8">>
  ELSE <<"M1", "M2a", "M2b", "M3", "M4", "M5", "M6", "M7", "M8", "M9">>
NextOf(s) == LET i == CHOOSE j \in 1..Len(Ord) : Ord[j] = s IN IF i = Len(Ord) THEN "Done" ELSE Ord[i + 1]

VARIABLES
  body, nextTok, gone,  \* the object at B; next fresh token; ghost: tokens no longer current
  meta, mver,           \* `<blob>.meta`: "absent" | "clean" | "cond"; its version (the If-Match token)
  intents,              \* tokens t with I(B,t) present
  pm,                   \* `_pool_meta` floor 4 (section 2.2)
  gs,                   \* `gc/state`: [row, ver (its token), hdr (3 | 4), lp (the lease pair)]
  hb,                   \* `gc/hb` pair
  slot,                 \* the mount slot of the older member's root: [st : none|live|fenced, v]
  odel,                 \* older GC `DELETE B If-Match t` in flight (tokens)
  lput,                 \* older writer body PUT in flight: "none" | "fresh" | "recur"
  lmeta,                \* older marker request in flight: [k, v]
  lgc,                  \* older GC `gc/state` commit or lease write in flight: [k, v, row]
  tl,                   \* per tool run
  tcr,                  \* tool intent creates in flight: [x, t]
  toolConf,             \* ghost: tokens whose intent M5 confirmed
  crashes, runs,
  nw, nput,             \* the writer of this build
  lnk,                  \* the linker's ref: "none" | the kind of its source
  oldRef,               \* a ref committed by the older build before the tool
  ng,                   \* the GC of this build, its round
  om, osusp, olease,    \* older member: "off"|"readpm"|"mounted"|"dead"; suspended; lease valid on its clock
  ow, oput, og,         \* its writer, publications, its GC round
  fEvid, fI4, fToolDel, fRecur   \* ghost violation bits

storeV == <<body, nextTok, gone, meta, mver, intents, pm, gs, hb, slot>>
lateV == <<odel, lput, lmeta, lgc, tcr>>
toolV == <<tl, toolConf, crashes, runs>>
newV == <<nw, nput, lnk, oldRef, ng>>
oldV == <<om, osusp, olease, ow, oput, og>>
ghostV == <<fEvid, fI4, fToolDel, fRecur>>
vars == <<storeV, lateV, toolV, newV, oldV, ghostV>>

T0 == [ph |-> "Idle", s0 |-> [st |-> "none", v |-> 0], lp0 |-> 0, hb0 |-> 0, sl |-> [st |-> "none", v |-> 0],
       e0 |-> 0, e6 |-> 0, row |-> NoRow, mc |-> "none", mv |-> 0, bl |-> FALSE, ht |-> 0]
NW0 == [ph |-> "Idle", seen |-> 0, adopted |-> FALSE]
OW0 == [ph |-> "Idle", seen |-> 0]
NG0 == [ph |-> "Idle", base |-> NoRow, bver |-> 0, ref |-> FALSE, next |-> NoRow]

--------------------------------------------------------------------------------------
(* SEED: the older world when the tool starts. *)

\* Older delete authority on `t`: a committed pending row, or a body DELETE already sent.
Auth(row, del, t) == t \in del \/ (row.st = "pend" /\ row.tok = t)

\* Section 7.4, "Which tokens a late delete can name": while `t` is current and under authority, the
\* marker is `Condemned` (pending was set only after reading `Condemned`, G:2264-2287; GC never writes
\* `Clean`, MW:39-41; a writer writes `Clean` only after a new publication, W:509, :542), no marker
\* request still in flight can change it (its If-Match names an older version), and no older writer
\* adopted `t` (it adopts only on `Clean`, W:446-452).
Consistent(b, m, row, del, lm, ref) ==
  b.p /\ Auth(row, del, b.t) =>
    /\ (m = "cond" \/ Anom("marker"))
    /\ (lm.k \in {"clean_swap", "del"} /\ lm.v = 1 => Anom("marker"))
    /\ (ref => Anom("ref"))

SeedLmeta ==
  {[k |-> "none", v |-> 0]}
    \cup (IF LateOk("mclean_create") THEN {[k |-> "clean_create", v |-> 0]} ELSE {})
    \cup (IF LateOk("mclean_swap") THEN {[k |-> "clean_swap", v |-> v] : v \in {0, 1}} ELSE {})
    \cup (IF LateOk("mcond") THEN {[k |-> "cond", v |-> 0]} ELSE {})
    \cup (IF LateOk("mdel") THEN {[k |-> "del", v |-> v] : v \in {0, 1}} ELSE {})

SeedRowsAll ==
  (IF "none" \in SeedRows THEN {NoRow} ELSE {}) \cup {Row(st, t, FALSE) : st \in SeedRows \ {"none"}, t \in {1, 2}}

Init ==
  /\ \E bp \in (IF SeedBodyAbsent THEN {TRUE, FALSE} ELSE {TRUE}),
        m \in SeedMetas,
        row \in SeedRowsAll,
        del \in (IF LateOk("del") THEN {{}, {1}, {2}} ELSE {{}}),
        lp \in (IF LateOk("put") THEN {"none", "fresh"} \cup (IF Anom("recur") THEN {"recur"} ELSE {}) ELSE {"none"}),
        lm \in SeedLmeta,
        ref \in (IF SeedRef THEN {FALSE, TRUE} ELSE {FALSE}),
        gcr \in {[k |-> "none", v |-> 0, row |-> NoRow]}
                  \cup (IF LateOk("gclease") THEN {[k |-> "lease", v |-> 0, row |-> NoRow]} ELSE {})
                  \cup (IF LateOk("gccommit")
                          THEN {[k |-> "commit", v |-> 0, row |-> r] :
                                  r \in {NoRow, Row("cand", 1, FALSE)}
                                        \cup (IF "cand" \in SeedRows THEN {Row("pend", 1, FALSE)} ELSE {})}
                          ELSE {}) :
       LET b == IF bp THEN [p |-> TRUE, t |-> 1] ELSE NoBody
       IN /\ Consistent(b, m, row, del, lm, ref)
          \* a late commit that graduates (G:2264-2287) read `Condemned` with the row a candidate
          /\ (gcr.row.st = "pend" => row.st = "cand" /\ row.tok = 1 /\ m = "cond")
          \* a recurring publication needs a delete of the recurring token (case (e)); else it is A1
          /\ (lp = "recur" => del = {2})
          \* the older world itself kept I8: an older ref names a present body
          /\ (ref => bp)
          /\ body = b /\ meta = m /\ gs = [row |-> row, ver |-> 0, hdr |-> 3, lp |-> 0]
          /\ odel = del /\ lput = lp /\ lmeta = lm /\ oldRef = ref /\ lgc = gcr
  /\ nextTok = 3 /\ gone = {2} /\ mver = 1 /\ intents = {} /\ pm = FALSE /\ hb = 0
  /\ slot = IF OldMember \in {"suspended_mounted", "suspended_gc_leader"} THEN [st |-> "live", v |-> 1]
            ELSE [st |-> "none", v |-> 0]
  /\ tl = [x \in Tools |-> T0] /\ tcr = {} /\ toolConf = {} /\ crashes = 0 /\ runs = 0
  /\ nw = NW0 /\ nput = 0 /\ lnk = "none" /\ ng = NG0
  /\ om = CASE OldMember \in {"suspended_mounted", "suspended_gc_leader"} -> "mounted"
            [] OldMember = "suspended_in_mount_seq" -> "readpm"
            [] OTHER -> "off"
  /\ osusp = (OldMember \in {"suspended_mounted", "suspended_gc_leader", "suspended_in_mount_seq"})
  /\ olease = FALSE
  /\ ow = OW0 /\ oput = 0
  \* R2 row 2: a GC leader suspended inside a round, after its lease read
  /\ og = IF OldMember = "suspended_gc_leader"
            THEN [ph |-> "Cut", base |-> gs.row, bver |-> 0, ref |-> FALSE, next |-> NoRow]
            ELSE NG0
  /\ fEvid = FALSE /\ fI4 = FALSE /\ fToolDel = FALSE /\ fRecur = FALSE

--------------------------------------------------------------------------------------
(* STORE helpers. *)

Land(t) ==
  /\ body' = [p |-> TRUE, t |-> t]
  /\ gone' = IF body.p /\ body.t /= t THEN gone \cup {body.t} ELSE gone
  /\ fRecur' = (fRecur \/ t \in gone)

Unland == body' = NoBody /\ gone' = gone \cup {body.t}

\* Edges every complete cut counts (A7): precommit and committed refs of both builds.
HasEdge ==
  \/ nw.ph /= "Idle" \/ lnk /= "none" \/ oldRef
  \/ ow.ph /= "Idle"

--------------------------------------------------------------------------------------
(* LATE REQUESTS of stopped older members, section 7.4. Each lands at most once, at any time. *)

\* GC `DELETE B If-Match t`: removes `t` if it is still current.
LateDelLand(t) ==
  /\ t \in odel
  /\ odel' = odel \ {t}
  /\ IF body.p /\ body.t = t THEN Unland ELSE UNCHANGED <<body, gone>>
  /\ UNCHANGED <<nextTok, meta, mver, intents, pm, gs, hb, slot, lput, lmeta, lgc, tcr, toolV, newV, oldV,
                 fEvid, fI4, fToolDel, fRecur>>

\* Writer body PUT: a new incarnation (A1 held in that build), or the recurring token (case (e)).
LatePutLand ==
  /\ lput /= "none"
  /\ IF lput = "recur" THEN Land(2) /\ UNCHANGED nextTok ELSE Land(nextTok) /\ nextTok' = nextTok + 1
  /\ lput' = "none"
  /\ UNCHANGED <<meta, mver, intents, pm, gs, hb, slot, odel, lmeta, lgc, tcr, toolV, newV, oldV, fEvid, fI4, fToolDel>>

\* Marker requests: the writer's `Clean` create (If-None-Match) or `Condemned` to `Clean` swap
\* (If-Match), W:352-404; the GC marker write (MW:45-51) or delete (If-Match, MW:57-63).
LateMetaLand ==
  /\ lmeta.k /= "none"
  /\ CASE lmeta.k = "clean_create" ->
            IF meta = "absent" THEN meta' = "clean" /\ mver' = mver + 1 ELSE UNCHANGED <<meta, mver>>
       [] lmeta.k = "clean_swap" ->
            IF meta = "cond" /\ mver = lmeta.v THEN meta' = "clean" /\ mver' = mver + 1 ELSE UNCHANGED <<meta, mver>>
       [] lmeta.k = "cond" -> meta' = "cond" /\ mver' = mver + 1
       [] lmeta.k = "del" ->
            IF meta /= "absent" /\ mver = lmeta.v THEN meta' = "absent" /\ mver' = mver + 1 ELSE UNCHANGED <<meta, mver>>
  /\ lmeta' = [k |-> "none", v |-> 0]
  /\ UNCHANGED <<body, nextTok, gone, intents, pm, gs, hb, slot, odel, lput, lgc, tcr, toolV, newV, oldV, ghostV>>

\* `gc/state` commit or lease write, If-Match the token it read (G:1326, G:5194). Refused after any
\* other write of `gc/state`, M6 included (section 7.4).
LateGcLand ==
  /\ lgc.k /= "none"
  /\ IF gs.ver = lgc.v /\ gs.ver < MaxVer
       THEN gs' = [gs EXCEPT !.row = IF lgc.k = "commit" THEN lgc.row ELSE @,
                             !.lp = IF lgc.k = "lease" THEN @ + 1 ELSE @, !.ver = @ + 1]
       ELSE UNCHANGED gs
  /\ lgc' = [k |-> "none", v |-> 0, row |-> NoRow]
  /\ UNCHANGED <<body, nextTok, gone, meta, mver, intents, pm, hb, slot, odel, lput, lmeta, tcr, toolV, newV, oldV, ghostV>>

\* A tool intent create applies (If-None-Match: an equal occupant stays).
ToolCreateLand(q) ==
  /\ q \in tcr
  /\ tcr' = tcr \ {q}
  /\ intents' = intents \cup {q.t}
  /\ UNCHANGED <<body, nextTok, gone, meta, mver, pm, gs, hb, slot, odel, lput, lmeta, lgc, toolV, newV, oldV, ghostV>>

--------------------------------------------------------------------------------------
(* THE TOOL, section 7.1. *)

Running(x) == tl[x].ph \notin {"Idle", "Done", "Refused", "Failed"}
Go(x, r) == tl' = [tl EXCEPT ![x] = r]
KeepRest == UNCHANGED <<lateV, newV, oldV>>

\* A run starts (the operator runs or reruns the tool). After a refusal the operator stops the member
\* the tool named (M2, M7: "stop that server and run the tool again").
TStart(x) ==
  /\ tl[x].ph \in {"Idle", "Refused", "Failed"} /\ runs < MaxRuns
  /\ Go(x, [T0 EXCEPT !.ph = "M1"])
  /\ runs' = runs + 1
  /\ IF tl[x].ph = "Refused" /\ om \in {"readpm", "mounted"}
       THEN om' = "dead" /\ ow' = (IF ow.ph = "Done" THEN ow ELSE OW0) /\ og' = NG0
       ELSE UNCHANGED <<om, ow, og>>
  /\ UNCHANGED <<storeV, lateV, toolConf, crashes, newV, osusp, olease, oput, ghostV>>

\* A crash after any step: the run ends; what it sent stays sent (its create may land later).
TCrash(x) ==
  /\ Running(x) /\ crashes < MaxCrash
  /\ Go(x, T0)
  /\ crashes' = crashes + 1
  /\ UNCHANGED <<storeV, lateV, toolConf, runs, newV, oldV, ghostV>>

\* M1: floor 4 -> M9 (the code of F4); else the probe, which the model does not refuse.
TM1(x) ==
  /\ tl[x].ph = "M1"
  /\ Go(x, [tl[x] EXCEPT !.ph = IF pm THEN "M9f" ELSE NextOf("M1")])
  /\ UNCHANGED <<storeV, lateV, toolConf, crashes, runs, newV, oldV, ghostV>>

\* M2, first observation: every slot, the lease pair of `gc/state`, the pair of `gc/hb`.
TM2a(x) ==
  /\ tl[x].ph = "M2a"
  /\ Go(x, [tl[x] EXCEPT !.ph = "M2b", !.s0 = slot, !.lp0 = gs.lp, !.hb0 = hb])
  /\ UNCHANGED <<storeV, lateV, toolConf, crashes, runs, newV, oldV, ghostV>>

\* M2, after the grace interval: read again. A moved slot or pair refuses. A live slot that did not
\* renew is fenced with the incarnation-guarded replace (SR:425-466). The model reads and fences in
\* one step; a renewal between them is the "fence met a conflict" refusal, the same outcome.
TM2b(x) ==
  /\ tl[x].ph = "M2b"
  /\ IF slot /= tl[x].s0 \/ gs.lp /= tl[x].lp0 \/ hb /= tl[x].hb0
       THEN Go(x, [tl[x] EXCEPT !.ph = "Refused"]) /\ UNCHANGED slot
       ELSE LET s == IF slot.st = "live" THEN [st |-> "fenced", v |-> slot.v + 1] ELSE slot
            IN /\ slot' = s
               /\ Go(x, [tl[x] EXCEPT !.ph = NextOf("M2b"), !.sl = s])
  \* A slot silent for the observation threshold, measured with the member's `mount_lease_ttl_ms`
  \* (M2), means its lease also expired on its own clock, unless that clock stood still (R2 row 1).
  /\ olease' = (olease /\ (StoodClock \/ slot' = slot))
  /\ UNCHANGED <<body, nextTok, gone, meta, mver, intents, pm, gs, hb, lateV, toolConf, crashes, runs, newV,
                 om, osusp, ow, oput, og, ghostV>>

\* M3: `gc/state` with its token e0; every condemned row.
TM3(x) ==
  /\ tl[x].ph = "M3"
  /\ Go(x, [tl[x] EXCEPT !.ph = NextOf("M3"), !.e0 = gs.ver, !.row = gs.row])
  /\ UNCHANGED <<storeV, lateV, toolConf, crashes, runs, newV, oldV, ghostV>>

\* M4: `LIST` of `blobs/`, exact `GET` of every `.meta`. Case (d): the listing may omit the key, its
\* marker and its body.
TM4(x) ==
  /\ tl[x].ph = "M4"
  /\ \E listed \in (IF Anom("listing") THEN {TRUE, FALSE} ELSE {TRUE}) :
       Go(x, [tl[x] EXCEPT !.ph = NextOf("M4"),
                           !.mc = IF ~listed \/ meta = "absent" THEN "none"
                                  ELSE IF meta = "clean" THEN "clean" ELSE "notclean",
                           !.mv = mver, !.bl = listed /\ body.p])
  /\ UNCHANGED <<storeV, lateV, toolConf, crashes, runs, newV, oldV, ghostV>>

\* M5: for a marker that is not `Clean`, a condemned row, or (option) a listed body: `HEAD B`;
\* absent -> nothing; else `PUT I(B,t)` If-None-Match for the current t.
TM5(x) ==
  /\ tl[x].ph = "M5"
  /\ LET need == (tl[x].mc = "notclean" /\ ~Has("rows_only")) \/ tl[x].row.st /= "none" \/ (AllBodies /\ tl[x].bl)
     IN IF need /\ body.p
          THEN /\ tcr' = tcr \cup {[x |-> x, t |-> body.t]}
               /\ Go(x, [tl[x] EXCEPT !.ph = "M5w", !.ht = body.t])
          ELSE /\ Go(x, [tl[x] EXCEPT !.ph = NextOf("M5")])
               /\ UNCHANGED tcr
  /\ UNCHANGED <<storeV, odel, lput, lmeta, lgc, toolConf, crashes, runs, newV, oldV, ghostV>>

\* M5, the create answered: `Committed`, or `Conflict` with an equal occupant (the bytes are
\* constant): confirmed (the table of G3).
TM5Answer(x) ==
  /\ tl[x].ph = "M5w"
  /\ [x |-> x, t |-> tl[x].ht] \notin tcr /\ tl[x].ht \in intents
  /\ Go(x, [tl[x] EXCEPT !.ph = NextOf("M5")])
  /\ toolConf' = toolConf \cup {tl[x].ht}
  /\ UNCHANGED <<storeV, lateV, crashes, runs, newV, oldV, ghostV>>

\* M5, the create unanswered: the resolve read confirms only on an equal occupant; else the run fails.
\* The request stays in flight. S13's sabotage proceeds on a create it could not confirm.
TM5Unanswered(x) ==
  /\ Faults /\ tl[x].ph = "M5w"
  /\ [x |-> x, t |-> tl[x].ht] \in tcr
  /\ IF tl[x].ht \in intents
       THEN Go(x, [tl[x] EXCEPT !.ph = NextOf("M5")]) /\ toolConf' = toolConf \cup {tl[x].ht}
       ELSE /\ Go(x, [tl[x] EXCEPT !.ph = IF Has("unconfirmed_create") THEN NextOf("M5") ELSE "Failed"])
            /\ UNCHANGED toolConf
  /\ UNCHANGED <<storeV, lateV, crashes, runs, newV, oldV, ghostV>>

\* M6: replace `gc/state` If-Match e0 with header 4, every field as read. Header 4 already: skipped
\* (section 7.2). A conflict restarts the run at M2.
TM6(x) ==
  /\ tl[x].ph = "M6"
  /\ IF gs.hdr = 4 \/ Has("gc_state_not_rewritten")
       THEN Go(x, [tl[x] EXCEPT !.ph = NextOf("M6"), !.e6 = tl[x].e0]) /\ UNCHANGED gs
       ELSE IF gs.ver = tl[x].e0 /\ gs.ver < MaxVer
         THEN /\ gs' = [gs EXCEPT !.hdr = 4, !.ver = @ + 1]
              /\ Go(x, [tl[x] EXCEPT !.ph = NextOf("M6"), !.e6 = gs.ver + 1])
         ELSE Go(x, [tl[x] EXCEPT !.ph = IF gs.ver = tl[x].e0 THEN "M6" ELSE "M2a"]) /\ UNCHANGED gs
  /\ UNCHANGED <<body, nextTok, gone, meta, mver, intents, pm, hb, slot, lateV, toolConf, crashes, runs, newV, oldV, ghostV>>

\* M7: right before M8, read every slot and both GC pairs again; anything moved refuses.
TM7(x) ==
  /\ tl[x].ph = "M7"
  /\ IF Has("no_recheck") \/ (gs.ver = tl[x].e6 /\ gs.lp = tl[x].lp0 /\ hb = tl[x].hb0 /\ slot = tl[x].sl)
       THEN Go(x, [tl[x] EXCEPT !.ph = NextOf("M7")])
       ELSE Go(x, [tl[x] EXCEPT !.ph = "Refused"])
  /\ UNCHANGED <<storeV, lateV, toolConf, crashes, runs, newV, oldV, ghostV>>

\* M8: raise the floor (read-modify-write of `_pool_meta`), the completion mark.
TM8(x) ==
  /\ tl[x].ph = "M8"
  /\ pm' = TRUE
  /\ Go(x, [tl[x] EXCEPT !.ph = NextOf("M8")])
  /\ UNCHANGED <<body, nextTok, gone, meta, mver, intents, gs, hb, slot, lateV, toolConf, crashes, runs, newV, oldV, ghostV>>

\* M9: delete the marker read at M4, If-Match the version of that read; one that changed is left to F4.
\* "M9f" is the rerun of a migrated pool: the code of F4 (a `HEAD`, a delete conditional on it).
TM9(x) ==
  /\ tl[x].ph \in {"M9", "M9f"}
  /\ LET del == IF tl[x].ph = "M9f" THEN meta /= "absent" ELSE tl[x].mc /= "none" /\ meta /= "absent" /\ mver = tl[x].mv
     IN /\ IF del THEN meta' = "absent" /\ mver' = mver + 1 ELSE UNCHANGED <<meta, mver>>
        /\ fToolDel' = (fToolDel \/ (del /\ ~pm))
  /\ Go(x, [tl[x] EXCEPT !.ph = IF tl[x].ph = "M9f" THEN "Done" ELSE NextOf("M9")])
  /\ UNCHANGED <<body, nextTok, gone, intents, pm, gs, hb, slot, lateV, toolConf, crashes, runs, newV, oldV,
                 fEvid, fI4, fRecur>>

--------------------------------------------------------------------------------------
(* THIS BUILD, after the floor. Section 2.3: every open below generation 4 is refused, so nothing
   of this build but the tool runs before M8 (I7). *)

\* W1: the precommit edge is durable before the first request.
NWStart ==
  /\ pm /\ nw.ph = "Idle"
  /\ nw' = [NW0 EXCEPT !.ph = "H1"]
  /\ UNCHANGED <<storeV, lateV, toolV, nput, lnk, oldRef, ng, oldV, ghostV>>

\* W2: `HEAD B`. Absent -> W6.
NWHead1 ==
  /\ nw.ph = "H1"
  /\ nw' = IF body.p THEN [nw EXCEPT !.ph = "G", !.seen = body.t] ELSE [nw EXCEPT !.ph = "Pub"]
  /\ UNCHANGED <<storeV, lateV, toolV, nput, lnk, oldRef, ng, oldV, ghostV>>

\* W4: `GET I(B,t)`. An object -> W6; an answered 404 -> W5.
NWGet ==
  /\ nw.ph = "G"
  /\ nw' = [nw EXCEPT !.ph = IF nw.seen \in intents THEN "Pub" ELSE "H2"]
  /\ UNCHANGED <<storeV, lateV, toolV, nput, lnk, oldRef, ng, oldV, ghostV>>

\* W5: `HEAD B` again. The same identity -> adopt; else W6.
NWHead2 ==
  /\ nw.ph = "H2"
  /\ nw' = IF body.p /\ body.t = nw.seen THEN [nw EXCEPT !.ph = "Ready", !.adopted = TRUE]
           ELSE [nw EXCEPT !.ph = "Pub"]
  /\ UNCHANGED <<storeV, lateV, toolV, nput, lnk, oldRef, ng, oldV, ghostV>>

\* W6: one unconditional publication under a fresh envelope. Answered (late publications of this
\* build are the subject of CaBlobIntentProtocol).
NWPub ==
  /\ nw.ph = "Pub" /\ nput < MaxNewPuts
  /\ Land(nextTok) /\ nextTok' = nextTok + 1 /\ nput' = nput + 1
  /\ nw' = [nw EXCEPT !.ph = "Ready"]
  /\ UNCHANGED <<meta, mver, intents, pm, gs, hb, slot, lateV, toolV, lnk, oldRef, ng, oldV, fEvid, fI4, fToolDel>>

NWCommit ==
  /\ nw.ph = "Ready"
  /\ nw' = [nw EXCEPT !.ph = "Done"]
  /\ UNCHANGED <<storeV, lateV, toolV, nput, lnk, oldRef, ng, oldV, ghostV>>

\* The part is dropped, or the build ends before its commit: the edge is withdrawn.
NWDrop ==
  /\ nw.ph \notin {"Idle"}
  /\ nw' = NW0
  /\ UNCHANGED <<storeV, lateV, toolV, nput, lnk, oldRef, ng, oldV, ghostV>>

\* Section 4.2, "No-bytes references": a ref copied with no request from a live source ref.
LinkFrom(k) ==
  /\ Linker /\ pm /\ lnk = "none"
  /\ \/ k = "old" /\ oldRef
     \/ k = "obs" /\ nw.ph = "Done"
     \/ k = "om" /\ ow.ph = "Done"
  /\ lnk' = k
  /\ UNCHANGED <<storeV, lateV, toolV, nw, nput, oldRef, ng, oldV, ghostV>>

LinkDrop ==
  /\ lnk /= "none" /\ lnk' = "none"
  /\ UNCHANGED <<storeV, lateV, toolV, nw, nput, oldRef, ng, oldV, ghostV>>

\* The inherited part is dropped by this build.
OldRefDrop ==
  /\ pm /\ oldRef /\ oldRef' = FALSE
  /\ UNCHANGED <<storeV, lateV, toolV, nw, nput, lnk, ng, oldV, ghostV>>

\* GC of this build. G1: a round runs only on `gc/state` at header 4. The lease read and the round's
\* complete cut (A7) are one step.
NGStart ==
  /\ pm /\ ng.ph = "Idle" /\ gs.hdr = 4 /\ gs.ver < MaxVer
  /\ ng' = [NG0 EXCEPT !.ph = "Work", !.base = gs.row, !.bver = gs.ver, !.ref = HasEdge]
  /\ UNCHANGED <<storeV, lateV, toolV, nw, nput, lnk, oldRef, oldV, ghostV>>

\* The fold and the delete batch for B, one step: G2 condemn, G6 spare, G7 supersede, G3 graduate
\* (create answered; the faults of G3 are the subject of CaBlobIntentProtocol), G4 delete.
\* Ghost I5: a body DELETE needs a confirmed intent (G3's, or M5's for the token, G8) on a row read
\* pending from the committed state, this round's cut at in-degree 0, and the `HEAD` showing `t`.
NGWork ==
  /\ ng.ph = "Work"
  /\ LET r == ng.base IN
     CASE r.st = "none" ->
            /\ IF ~ng.ref /\ body.p
                 THEN ng' = [ng EXCEPT !.ph = "Commit", !.next = Row("cand", body.t, FALSE)]   \* G2
                 ELSE ng' = NG0
            /\ UNCHANGED <<body, gone, intents, fEvid, fI4>>
       [] r.st /= "none" /\ ng.ref ->                                                           \* G6
            /\ ng' = [ng EXCEPT !.ph = "Commit", !.next = NoRow]
            /\ UNCHANGED <<body, gone, intents, fEvid, fI4>>
       [] r.st = "cand" ->
            IF body.p /\ body.t /= r.tok
              THEN /\ ng' = [ng EXCEPT !.ph = "Commit", !.next = Row("cand", body.t, FALSE)]   \* G7
                   /\ UNCHANGED <<body, gone, intents, fEvid, fI4>>
              ELSE /\ intents' = intents \cup {r.tok}                                           \* G3
                   /\ ng' = [ng EXCEPT !.ph = "Commit", !.next = Row("pend", r.tok, TRUE)]
                   /\ UNCHANGED <<body, gone, fEvid, fI4>>
       [] OTHER ->                                                                              \* pending
            IF body.p /\ body.t = r.tok
              THEN /\ Unland                                                                    \* G4 Removed
                   /\ fEvid' = (fEvid \/ ~(r.conf \/ r.tok \in toolConf))
                   /\ intents' = intents \ {r.tok}
                   /\ ng' = [ng EXCEPT !.ph = "Commit", !.next = NoRow]
                   /\ UNCHANGED fI4
              ELSE \E sup \in (IF body.p THEN {FALSE, TRUE} ELSE {FALSE}) :                      \* G4 Gone or Mismatch, or G7
                   /\ intents' = intents \ {r.tok}
                   /\ fI4' = (fI4 \/ (r.tok \in intents /\ body.p /\ body.t = r.tok))
                   /\ ng' = [ng EXCEPT !.ph = "Commit", !.next = IF sup THEN Row("cand", body.t, FALSE) ELSE NoRow]
                   /\ UNCHANGED <<body, gone, fEvid>>
  /\ UNCHANGED <<nextTok, meta, mver, pm, gs, hb, slot, lateV, toolV, nw, nput, lnk, oldRef, oldV, fToolDel, fRecur>>

\* The round commit: compare-and-swap on the token its lease read, header 4 (G:1326).
NGCommit ==
  /\ ng.ph = "Commit"
  /\ IF gs.ver = ng.bver /\ gs.hdr = 4 /\ gs.ver < MaxVer
       THEN gs' = [gs EXCEPT !.row = ng.next, !.ver = @ + 1]
       ELSE UNCHANGED gs
  /\ ng' = NG0
  /\ UNCHANGED <<body, nextTok, gone, meta, mver, intents, pm, hb, slot, lateV, toolV, nw, nput, lnk, oldRef, oldV, ghostV>>

--------------------------------------------------------------------------------------
(* OLDER MEMBERS, section 7.4 R2 and R4. A reduced older build: the marker writer and the marker GC. *)

OldWorks == om = "mounted" /\ ~osusp

\* Its mount sequence: `_pool_meta` read (refused at floor 4: CasPoolMetaFormat.cpp:155-158).
OMReadPm ==
  /\ OldMember \in {"start", "start_before_recheck"} /\ om = "off"
  /\ om' = IF pm THEN "dead" ELSE "readpm"
  /\ UNCHANGED <<storeV, lateV, toolV, newV, osusp, olease, ow, oput, og, ghostV>>

\* Then it claims its slot, which a fence leaves reclaimable by the same server (SR:464-466). It never
\* reads `_pool_meta` again (CasPool.cpp:630-631). "start_before_recheck" restricts the claim to before
\* a passed M7: the precondition violation M7 must detect.
OMClaim ==
  /\ om = "readpm" /\ ~osusp /\ slot.st \in {"none", "fenced"}
  /\ (OldMember = "start_before_recheck" => ~pm /\ \A x \in Tools : tl[x].ph /= "M8")
  /\ slot' = [st |-> "live", v |-> slot.v + 1]
  /\ om' = "mounted" /\ olease' = TRUE
  /\ UNCHANGED <<body, nextTok, gone, meta, mver, intents, pm, gs, hb, lateV, toolV, newV, osusp, ow, oput, og, ghostV>>

\* Resume after a suspension. Its mount plane checks the lease on its boot clock before every attempt
\* (CasMountRuntime.cpp:156-170): after a long pause it starts nothing until it renews, unless that
\* clock stood still.
OMResume ==
  /\ osusp
  /\ osusp' = FALSE
  /\ olease' = (StoodClock /\ om = "mounted")
  /\ UNCHANGED <<storeV, lateV, toolV, newV, om, ow, oput, og, ghostV>>

\* Slot renewal: refused on a fenced slot (SR:461-463), and the member stops; its precommit edge is
\* withdrawn, its committed ref stays.
OMRenew ==
  /\ OldWorks /\ (slot.st = "live" => slot.v < 3)   \* bound: at most two renewals of one claim
  /\ IF slot.st = "live"
       THEN slot' = [slot EXCEPT !.v = @ + 1] /\ olease' = TRUE /\ UNCHANGED <<om, ow, og>>
       ELSE /\ om' = "dead" /\ olease' = FALSE /\ og' = NG0
            /\ ow' = IF ow.ph = "Done" THEN ow ELSE OW0
            /\ UNCHANGED slot
  /\ UNCHANGED <<body, nextTok, gone, meta, mver, intents, pm, gs, hb, lateV, toolV, newV, osusp, oput, ghostV>>

\* The GC leader's heartbeat on `gc/hb`.
OMHeartbeat ==
  /\ OldWorks /\ hb < 2
  /\ hb' = hb + 1
  /\ UNCHANGED <<body, nextTok, gone, meta, mver, intents, pm, gs, slot, lateV, toolV, newV, oldV, ghostV>>

\* Older writer: edge, `HEAD B`, `GET .meta`: `Clean` adopts (W:446-452); else a body PUT (W:509) and
\* a `Clean` write (W:368, :542).
OWStep ==
  /\ OldWorks /\ olease
  /\ \/ /\ ow.ph = "Idle"
        /\ ow' = [ow EXCEPT !.ph = "H"]
        /\ UNCHANGED <<body, nextTok, gone, meta, mver, oput, fRecur>>
     \/ /\ ow.ph = "H"
        /\ ow' = IF body.p THEN [ph |-> "M", seen |-> body.t] ELSE [ph |-> "Put", seen |-> 0]
        /\ UNCHANGED <<body, nextTok, gone, meta, mver, oput, fRecur>>
     \/ /\ ow.ph = "M"
        /\ ow' = [ow EXCEPT !.ph = IF meta = "clean" THEN "Ready" ELSE "Put"]
        /\ UNCHANGED <<body, nextTok, gone, meta, mver, oput, fRecur>>
     \/ /\ ow.ph = "Put" /\ oput < MaxOldPuts
        /\ Land(nextTok) /\ nextTok' = nextTok + 1 /\ oput' = oput + 1
        /\ ow' = [ow EXCEPT !.ph = "Clean"]
        /\ UNCHANGED <<meta, mver>>
     \/ /\ ow.ph = "Clean"
        /\ meta' = "clean" /\ mver' = mver + 1 /\ ow' = [ow EXCEPT !.ph = "Ready"]
        /\ UNCHANGED <<body, nextTok, gone, oput, fRecur>>
     \/ /\ ow.ph = "Ready"
        /\ ow' = [ow EXCEPT !.ph = "Done"]
        /\ UNCHANGED <<body, nextTok, gone, meta, mver, oput, fRecur>>
     \/ /\ ow.ph = "Done"
        /\ ow' = OW0
        /\ UNCHANGED <<body, nextTok, gone, meta, mver, oput, fRecur>>
  /\ UNCHANGED <<intents, pm, gs, hb, slot, lateV, toolV, newV, om, osusp, olease, og, fEvid, fI4, fToolDel>>

\* Older GC. A round starts with its lease read-modify-write of `gc/state`; header 4 throws (G:5214),
\* so no round starts. Then the cut.
OGStart ==
  /\ OldWorks /\ og.ph = "Idle" /\ gs.hdr = 3 /\ gs.ver < MaxVer
  /\ gs' = [gs EXCEPT !.ver = @ + 1, !.lp = @ + 1]
  /\ og' = [NG0 EXCEPT !.ph = "Cut", !.base = gs.row, !.bver = gs.ver + 1]
  /\ UNCHANGED <<body, nextTok, gone, meta, mver, intents, pm, hb, slot, lateV, toolV, newV, om, osusp, olease, ow, oput, ghostV>>

OGCut ==
  /\ OldWorks /\ og.ph = "Cut"
  /\ og' = [og EXCEPT !.ph = "Work", !.ref = HasEdge]
  /\ UNCHANGED <<storeV, lateV, toolV, newV, om, osusp, olease, ow, oput, ghostV>>

\* Condemn writes `Condemned` (G:2194, :2227); graduation needs a `Condemned` read (G:2264-2287); a
\* pending row at in-degree 0 sends `DELETE B If-Match t` (G:583-595), which may land at any time.
OGWork ==
  /\ OldWorks /\ og.ph = "Work"
  /\ LET r == og.base IN
     \/ /\ r.st = "none" /\ ~og.ref /\ body.p
        /\ og' = [og EXCEPT !.ph = "Commit", !.next = Row("cand", body.t, FALSE)]
        /\ meta' = "cond" /\ mver' = mver + 1
        /\ UNCHANGED odel
     \/ /\ r.st = "none" /\ ~(~og.ref /\ body.p)
        /\ og' = NG0
        /\ UNCHANGED <<meta, mver, odel>>
     \/ /\ r.st /= "none" /\ og.ref
        /\ og' = [og EXCEPT !.ph = "Commit", !.next = NoRow]
        /\ UNCHANGED <<meta, mver, odel>>
     \/ /\ r.st = "cand" /\ ~og.ref /\ meta = "cond"
        /\ og' = [og EXCEPT !.ph = "Commit", !.next = Row("pend", r.tok, FALSE)]
        /\ UNCHANGED <<meta, mver, odel>>
     \/ /\ r.st = "cand" /\ ~og.ref /\ meta /= "cond"
        /\ og' = NG0
        /\ meta' = "cond" /\ mver' = mver + 1
        /\ UNCHANGED odel
     \/ /\ r.st = "pend" /\ ~og.ref
        /\ odel' = (IF body.p /\ body.t = r.tok THEN odel \cup {r.tok} ELSE odel)
        /\ og' = [og EXCEPT !.ph = "Commit", !.next = NoRow]
        /\ UNCHANGED <<meta, mver>>
  /\ UNCHANGED <<body, nextTok, gone, intents, pm, gs, hb, slot, lput, lmeta, lgc, tcr, toolV, newV, om, osusp, olease, ow, oput, ghostV>>

OGCommit ==
  /\ OldWorks /\ og.ph = "Commit"
  /\ IF gs.ver = og.bver /\ gs.hdr = 3 /\ gs.ver < MaxVer
       THEN gs' = [gs EXCEPT !.row = og.next, !.ver = @ + 1]
       ELSE UNCHANGED gs
  /\ og' = NG0
  /\ UNCHANGED <<body, nextTok, gone, meta, mver, intents, pm, hb, slot, lateV, toolV, newV, om, osusp, olease, ow, oput, ghostV>>

--------------------------------------------------------------------------------------
Next ==
  \/ \E t \in odel : LateDelLand(t)
  \/ LatePutLand \/ LateMetaLand \/ LateGcLand
  \/ \E q \in tcr : ToolCreateLand(q)
  \/ \E x \in Tools :
       \/ TStart(x) \/ TCrash(x) \/ TM1(x) \/ TM2a(x) \/ TM2b(x) \/ TM3(x) \/ TM4(x) \/ TM5(x)
       \/ TM5Answer(x) \/ TM5Unanswered(x) \/ TM6(x) \/ TM7(x) \/ TM8(x) \/ TM9(x)
  \/ NWStart \/ NWHead1 \/ NWGet \/ NWHead2 \/ NWPub \/ NWCommit \/ NWDrop
  \/ \E k \in {"old", "obs", "om"} : LinkFrom(k)
  \/ LinkDrop \/ OldRefDrop
  \/ NGStart \/ NGWork \/ NGCommit
  \/ OMReadPm \/ OMClaim \/ OMResume \/ OMRenew \/ OMHeartbeat \/ OWStep \/ OGStart \/ OGCut \/ OGWork \/ OGCommit

Spec == Init /\ [][Next]_vars

\* Liveness: the tool's own steps and the store's answers are weakly fair; crashes and reruns are
\* bounded by MaxCrash and MaxRuns.
ToolSteps(x) ==
  TStart(x) \/ TM1(x) \/ TM2a(x) \/ TM2b(x) \/ TM3(x) \/ TM4(x) \/ TM5(x) \/ TM5Answer(x) \/ TM6(x)
    \/ TM7(x) \/ TM8(x) \/ TM9(x)
LiveSpec == Spec /\ \A x \in Tools : WF_vars(ToolSteps(x)) /\ WF_vars(\E q \in tcr : ToolCreateLand(q))

Symm == Permutations(Tools)

--------------------------------------------------------------------------------------
(* INVARIANTS *)

TypeOK ==
  /\ Sab \subseteq KnownSab
  /\ Anomaly \subseteq {"marker", "ref", "listing", "recur"}
  /\ Late \subseteq {"del", "put", "mclean_create", "mclean_swap", "mcond", "mdel", "gccommit", "gclease"}
  /\ OldMember \in {"none", "start", "start_before_recheck", "suspended_mounted", "suspended_in_mount_seq",
                    "suspended_gc_leader"}
  /\ body.p \in BOOLEAN /\ body.t \in 0..MaxTok /\ intents \subseteq Tokens /\ odel \subseteq Tokens
  /\ meta \in {"absent", "clean", "cond"} /\ gs.hdr \in {3, 4} /\ gs.ver \in 0..MaxVer
  /\ \A x \in Tools : tl[x].ph \in {"Idle", "M1", "M2a", "M2b", "M3", "M4", "M5", "M5w", "M6", "M7", "M8", "M9", "M9f",
                                     "Done", "Refused", "Failed"}
  /\ om \in {"off", "readpm", "mounted", "dead"}

\* I8, the goal, unqualified: a committed ref never names a blob key with no body.
INV_COMMITTED_REF_HAS_BODY ==
  (oldRef \/ nw.ph = "Done" \/ lnk /= "none" \/ ow.ph = "Done") => body.p

\* I8 as qualified in section 4.1: a ref this build commits by observation (W2 to W6), and a copy of
\* one, never names a key with no body.
INV_REF_BY_OBSERVATION_HAS_BODY == (nw.ph = "Done" \/ lnk = "obs") => body.p

\* I5 with G8: every body DELETE of this build follows a confirmed intent for its token (G3's on the
\* row, or M5's), the commit of the pending row and a later cut at in-degree 0.
INV_DELETE_EVIDENCE == ~fEvid

\* I4: no intent DELETE while its token is current.
INV_I4_INTENT_PERMANENT == ~fI4

\* Section 7.1, "Order": the tool deletes nothing before the pool is migrated.
INV_TOOL_DESTROYS_NOTHING_BEFORE_FLOOR == ~fToolDel

\* A8 as M2, M7 and the fence enforce it: on a migrated pool no older member holds a live slot.
INV_NO_LIVE_OLD_SLOT_AFTER_FLOOR == pm => ~(om = "mounted" /\ slot.st = "live")

\* I1 (diagnostic; red only under case (e)).
INV_I1_NO_RECURRENCE == ~fRecur

(* LIVENESS: section 7.2, a run that is not refused reaches "migration complete". *)
LIVE_MIGRATION_COMPLETES == <>(\E x \in Tools : tl[x].ph = "Done") /\ <>[]pm

(* WITNESSES: each must FAIL. *)
WIT_MIGRATED == ~pm
WIT_M7_REFUSES == \A x \in Tools : ~(tl[x].ph = "Refused" /\ tl[x].e6 /= 0)
WIT_M2_REFUSES == \A x \in Tools : ~(tl[x].ph = "Refused" /\ tl[x].e6 = 0)
WIT_M2_FENCES == ~(slot.st = "fenced" /\ ~pm)
WIT_M6_CONFLICT_RESTARTS == \A x \in Tools : ~(tl[x].ph = "M2a" /\ tl[x].mv /= 0)
WIT_RERUN_SKIPS_M6 == \A x \in Tools : ~(tl[x].ph = "M7" /\ gs.hdr = 4 /\ tl[x].e6 = tl[x].e0 /\ runs >= 2)
WIT_CRASH_THEN_COMPLETE == ~(crashes >= 1 /\ \E x \in Tools : tl[x].ph = "Done")
WIT_INHERITED_PENDING_EXECUTED ==       \* this build deletes a row read pending before the tool, on M5's intent
  ~(ng.ph = "Commit" /\ ng.base.st = "pend" /\ ~ng.base.conf /\ ~body.p /\ ng.base.tok \in gone)
WIT_INHERITED_CANDIDATE_CONFIRMS ==     \* G3 on a candidate the tool covered meets the equal occupant
  ~(ng.ph = "Commit" /\ ng.base.st = "cand" /\ ng.next.st = "pend" /\ ng.base.tok \in toolConf)
WIT_ADOPT_INHERITED_BODY == ~(nw.ph = "Done" /\ nw.adopted /\ body.t = 1)
WIT_MEETS_TOOL_INTENT == ~(nw.ph = "Ready" /\ ~nw.adopted /\ 1 \in toolConf /\ 1 \in gone)
WIT_MARKER_ONLY_INTENT == ~(gs.row.st = "none" /\ 1 \in toolConf /\ pm)
WIT_LATE_DELETE_MISSES == ~(odel = {} /\ pm /\ body.p /\ body.t >= 3 /\ nw.ph = "Done")
WIT_M9_LEAVES_CHANGED_MARKER == \A x \in Tools : ~(tl[x].ph = "Done" /\ meta /= "absent" /\ pm)
WIT_OLD_GC_ROUND == ~(og.ph = "Commit")
WIT_OPTION_INTENT_ON_CLEAN == ~(AllBodies /\ 1 \in toolConf /\ gs.row.st = "none" /\ meta = "absent" /\ pm)
WIT_TWO_RUNS_DONE == ~(\A x \in Tools : tl[x].ph = "Done") \/ Cardinality(Tools) < 2

(*
ABSTRACTIONS AND BOUNDS
  - One blob key; one server root an older member may own; `gc/server-roots/` and the catalog reads of
    M2 and M7 are one slot. The observation of M2 is one re-read after the grace step, and its fence is
    one step with that re-read.
  - The older world is seeded under `Consistent` (section 7.4). The marker protocol that produces it is
    the subject of CaBlobPublishCore and CaGcCondemnMarkerGate, not of this model.
  - Late requests: at most one of each kind in the seed. Tokens: the seed body, one replaced token,
    fresh publications.
  - This build's writer publishes with an answer; its GC's create and deletes are answered, and its fold
    and delete batch are one step after the cut. The faults of those requests, `cas-fsck` and the
    rebuild are the subject of CaBlobIntentProtocol.
  - An older member renews its slot at most twice per claim and writes `gc/hb` at most twice. Its GC
    deletes no marker (a kept `Condemned` only adds intents).
  - Not modelled: the probe, the dialect, the prefix length, `--dry-run`, malformed markers, tokens that
    do not canonicalise, a `gc/state` missing at M3.
*)
======================================================================================
