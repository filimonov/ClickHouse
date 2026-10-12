-------------------------- MODULE CaEpochAllocCore --------------------------
(***************************************************************************)
(* Writer-epoch allocation, the mount slot and the decommission tail of   *)
(* one CAS server root, at release line 2a034cdb374 (antalya-26.6-20261008 *)
(* + PR #2512).  CAS-362 (twin allocation through resolve-by-bytes) and    *)
(* CAS-363 (decommission deletes `epoch`, a restart re-mints epoch 1).     *)
(* Round 2: today's tail kept; a re-created counter restarts above every   *)
(* earlier epoch from store time (V1, StoreTime) or a `_pool_meta` high-   *)
(* water mark (V2, PoolHwm).                                               *)
(*                                                                         *)
(* Code paths, relative to                                                 *)
(* src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/:         *)
(*   Pool/CasServerRoot.cpp: claimOwnerOrThrow, allocateWriterEpoch,       *)
(*     serverRootSubtreeEmpty, claimMount                                  *)
(*   Backend/CasRequests.cpp: CasOperation::readModifyWrite, writeLoop     *)
(*   Tools/CasDecommission.cpp: decommissionPoolMember, deleteSlotObject   *)
(*   Gc/CasOrphanManifestSweep.cpp: floorForNamespace,                     *)
(*     prefixEligibleUnder, the sweep loop                                 *)
(*                                                                         *)
(* One uuid, one server root.  Every process in Procs carries that uuid:   *)
(* the server instances in Inst (twins when |Inst| > 1) and, with Decom,   *)
(* the decommission tool "d" (Pool::openForDecommission impersonates the   *)
(* victim uuid).  The epoch-keyed artifact is the manifest key             *)
(* (ns, epoch, build_seq = 1): a set of epochs.                            *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Inst,               \* server processes with the same uuid
    S3,                 \* TRUE: ETag = MD5 of the body (identical bytes => identical token); FALSE: GCS generation
    Nonce,              \* fix_nonce: a fresh allocation nonce in every counter body
    NonceInResolve,     \* the writeLoop byte comparison sees the nonce (sabotage: FALSE)
    Retire,             \* fix_retire: the decommission tail retires `epoch` by one If-Match write, never deletes it
    AllocHonorsRetire,  \* allocateWriterEpoch refuses a retired counter (sabotage / 26.6.4 binary: FALSE)
    RetireDeletesMount, \* the retiring tail still deletes the farewell mount at the end
    StrictClaim,        \* PR #2512: claimMount never writes over its own (uuid, epoch); FALSE = d76b59f9295 refresh
    Decom,              \* the decommission tool runs
    MaxStarts,          \* starts per server instance
    MaxAmbig,           \* ambiguous counter writes in the whole run
    MaxGcRounds,        \* orphan-sweep rounds
    EMax,               \* state constraint on the counter value
    \* CAS-363 re-creation above every earlier epoch (counter still deleted by decommission):
    StoreTime,          \* V1: an absent counter restarts at Base = store_time * Scale + 1
    Scale,              \* stands for 2^20: allocations per store-time tick stay below it
    MaxClock,           \* store-time ticks the environment may take
    TimeAfterAbsent,    \* V1 sample: TRUE = own probe written after the absent read; FALSE = open-time probe
    ClockBack,          \* V1 sabotage: the store clock may step back once
    TailTakesASecond,   \* V1 assumption: the counter is deleted at a later store second than it was created
    PoolHwm,            \* V2: decommission raises `_pool_meta.hwm` to the captured counter before deleting it;
                        \*     an absent counter restarts at max(hwm + 1, 1)
    HwmFresh,           \* V2 read: TRUE = GET `_pool_meta` after the absent read; FALSE = the open-time read
    HwmAfterDelete,     \* V2 sabotage: the raise runs after the epoch delete
    OldAdmitDropsHwm,   \* V2 compat: a 26.6.4 `admitOrValidate` rewrites `_pool_meta` without the hwm field
    MintChecksCounter,  \* proposed: claimMount re-reads `epoch` before minting over an absent slot and mints
                        \* only if the counter still holds this process's own allocation bytes (nonce)
    NoZombie            \* no request is delayed across a decommission delete: the tail's DELETE of `epoch`
                        \* (`mount`) never lands between an allocator's (claimer's) decision and its write

ASSUME MaxStarts \in Nat /\ MaxAmbig \in Nat /\ MaxGcRounds \in Nat /\ EMax \in Nat
ASSUME Scale \in Nat \ {0} /\ MaxClock \in Nat
ASSUME ~(StoreTime /\ PoolHwm)

Procs == Inst \cup (IF Decom THEN {"d"} ELSE {})

\* `epoch` object. v = next_writer_epoch (0 = absent; the decoder rejects 0), n = allocation nonce,
\* r = retired marker.
EAbsent == [v |-> 0, n |-> 0, r |-> FALSE]
\* `mount` object. holder is a ghost used only to abstract claimMountAwaitingExpiry's observation wait.
MAbsent == [e |-> 0, clean |-> FALSE, holder |-> "none"]
NONE == <<"none", 0, 0, FALSE>>

VARIABLES
    epObj, epGen,       \* counter body; GCS generation (bumped on every write and delete)
    mount, mGen,        \* mount body; mount token (write_attempt_id is random: every write is a new token)
    ownerRetired,       \* owner tombstone (owner always present for our uuid)
    manifests, refs,    \* epoch-keyed manifest keys present; committed references to them
    inflight,           \* counter writes sent, response lost, not yet landed: [exp, body]
    ambUsed, nonceCtr,
    allocLog, dup,      \* ghost: every epoch value allocateWriterEpoch returned; a value returned twice
    slotMax,            \* ghost: highest writer_epoch ever written into the mount slot
    pc, myEpoch, myBody, myExp, myAmb, myMTok, starts, mf,
    dCap, dFar, dOwn,   \* decommission captures: epoch (body, token), farewell mount token, owner body
    gpc, gfloor, gview, gRounds,
    epBornAt,           \* ghost: store time at which the current counter object was created
    maxCounter, regress,\* ghost: highest counter value ever written; a write at or below it (non-retire)
    clock, backUsed,    \* store time (seconds); the one backwards step was taken
    hwm, hwmGen, admitUsed, \* `_pool_meta` high-water mark and token; the one concurrent admission was taken
    myTime, myHwm,      \* per process: sampled store time, `_pool_meta.hwm` read
    dHwm                \* decommission: `_pool_meta` read [v, tok]

vars == <<epObj, epGen, mount, mGen, ownerRetired, manifests, refs, inflight, ambUsed, nonceCtr,
          allocLog, dup, slotMax, pc, myEpoch, myBody, myExp, myAmb, myMTok, starts, mf,
          dCap, dFar, dOwn, gpc, gfloor, gview, gRounds,
          epBornAt, maxCounter, regress, clock, backUsed, hwm, hwmGen, admitUsed, myTime, myHwm, dHwm>>

EpVars == <<epObj, epGen, epBornAt, maxCounter, regress>>
XVars == <<clock, backUsed, hwm, hwmGen, admitUsed, myTime, myHwm, dHwm>>

\* Token of the counter: S3 = the bytes (single-PUT MD5), GCS = generation.
Tok == IF S3 THEN <<"b", epObj.v, epObj.n, epObj.r>> ELSE <<"g", epGen, 0, FALSE>>

\* A conditional write's precondition: If-None-Match for a create, If-Match(token) otherwise.
PreHolds(exp) == IF exp = NONE THEN epObj.v = 0 ELSE epObj.v > 0 /\ Tok = exp

\* writeLoop: `obj->bytes == bytes`. With the nonce the sabotage compares only the epoch value.
SameBytes(o, b) == IF Nonce /\ NonceInResolve THEN o = b ELSE o.v = b.v /\ o.r = b.r

NewN == IF Nonce THEN nonceCtr + 1 ELSE 0
BumpNonce == nonceCtr' = IF Nonce THEN nonceCtr + 1 ELSE nonceCtr

ApplyEpoch(body) == /\ epObj' = body
                    /\ epGen' = IF S3 THEN epGen ELSE epGen + 1
                    /\ epBornAt' = IF epObj.v = 0 THEN clock ELSE epBornAt
                    /\ maxCounter' = IF body.v > maxCounter THEN body.v ELSE maxCounter
                    /\ regress' = (regress \/ (body.v <= maxCounter /\ ~body.r))

Max2(a, b) == IF a > b THEN a ELSE b

\* Start value of a counter created over an absent key, from a store-time sample t or an hwm read h.
Base(t, h) == IF StoreTime THEN t * Scale + 1
              ELSE IF PoolHwm THEN Max2(h + 1, 1)
              ELSE 1
FreshSample == (StoreTime /\ TimeAfterAbsent) \/ (PoolHwm /\ HwmFresh)

\* A request decided before, and possibly landing after, a decommission DELETE.
PendingCounterWrite == inflight # {} \/ \E q \in Procs : pc[q] \in {"awrite", "aresolve"}
PendingMountClaim == \E q \in Procs : pc[q] \in {"cmint", "crecl"}

Down(p) == IF p = "d" THEN "dstop" ELSE "down"
Hold(p) == IF p = "d" THEN "dlive" ELSE "live"
HoldStates == {"live", "dlive", "dcap", "dfarewell"}

\* Ghost for claimMountAwaitingExpiry's token-stability wait: a holder is proven dead when the process
\* that wrote the body no longer holds that epoch.
\* IF, not \/: TLC explores every disjunct of an action-level \/ and would apply pc to "none".
ProvenDead(h, e) == IF h = "none" THEN TRUE ELSE ~(pc[h] \in HoldStates /\ myEpoch[h] = e)

\* prefixEligibleUnder with build_seq = 1: a live holder publishes min_active_build_sequence = 1,
\* the farewell publishes UINT64_MAX (clean).
Eligible(f, k) == f.e > 0 /\ (k < f.e \/ (k = f.e /\ f.clean))

Init ==
    /\ epObj = EAbsent /\ epGen = 0
    /\ mount = MAbsent /\ mGen = 0
    /\ ownerRetired = FALSE
    /\ manifests = {} /\ refs = {}
    /\ inflight = {} /\ ambUsed = 0 /\ nonceCtr = 0
    /\ allocLog = {} /\ dup = FALSE /\ slotMax = 0
    /\ epBornAt = 0 /\ maxCounter = 0 /\ regress = FALSE
    /\ clock = 0 /\ backUsed = FALSE /\ hwm = 0 /\ hwmGen = 0 /\ admitUsed = FALSE
    /\ myTime = [p \in Procs |-> 0] /\ myHwm = [p \in Procs |-> 0]
    /\ dHwm = [v |-> 0, tok |-> 0]
    /\ pc = [p \in Procs |-> IF p = "d" THEN "open" ELSE "down"]
    /\ myEpoch = [p \in Procs |-> 0]
    /\ myBody = [p \in Procs |-> EAbsent]
    /\ myExp = [p \in Procs |-> NONE]
    /\ myAmb = [p \in Procs |-> FALSE]
    /\ myMTok = [p \in Procs |-> 0]
    /\ starts = [p \in Procs |-> 0]
    /\ mf = [p \in Procs |-> "none"]
    /\ dCap = [obj |-> EAbsent, tok |-> NONE] /\ dFar = 0 /\ dOwn = FALSE
    /\ gpc = "gidle" /\ gfloor = MAbsent /\ gview = {} /\ gRounds = 0

Locals == <<myEpoch, myBody, myExp, myAmb, myMTok, starts, mf>>
Store == <<EpVars, mount, mGen, ownerRetired, manifests, refs>>
DVars == <<dCap, dFar, dOwn>>
GVars == <<gpc, gfloor, gview, gRounds>>
Ghost == <<allocLog, dup, slotMax>>

-----------------------------------------------------------------------------
(* Server start                                                            *)

\* Process (re)start: in-memory only.
Start(p) ==
    /\ p \in Inst /\ pc[p] = "down" /\ starts[p] < MaxStarts
    /\ pc' = [pc EXCEPT ![p] = "open"]
    /\ starts' = [starts EXCEPT ![p] = @ + 1]
    /\ mf' = [mf EXCEPT ![p] = "none"]
    /\ myEpoch' = [myEpoch EXCEPT ![p] = 0]
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, myBody, myExp, myAmb, myMTok, DVars, GVars>>
    /\ UNCHANGED XVars

\* Pool::open before the server-root steps: runCapabilityProbe writes `_probe/<uid>/...` (V1 takes
\* store time from its Last-Modified) and PoolMeta::createOrValidate reads `_pool_meta` (V2 hwm).
\* Fused: a config uses at most one of the two samples (ASSUME ~(StoreTime /\ PoolHwm)).
OpenProbe(p) ==
    /\ pc[p] = "open"
    /\ myTime' = [myTime EXCEPT ![p] = clock]
    /\ myHwm' = [myHwm EXCEPT ![p] = hwm]
    /\ pc' = [pc EXCEPT ![p] = "owner"]
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, Locals, DVars, GVars,
                   clock, backUsed, hwm, hwmGen, admitUsed, dHwm>>

\* claimOwnerOrThrow: GET owner; same uuid; throwIfOwnerRetired.
OwnerCheck(p) ==
    /\ pc[p] = "owner"
    /\ pc' = [pc EXCEPT ![p] = IF ownerRetired THEN Down(p) ELSE "aread"]
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, Locals, DVars, GVars>>
    /\ UNCHANGED XVars

-----------------------------------------------------------------------------
(* allocateWriterEpoch through CasOperation::readModifyWrite / writeLoop   *)

\* readModifyWrite's GET (or a conflict's resolve read) + `decide` on it (in-memory).
AllocRead(p) ==
    /\ pc[p] = "aread"
    /\ IF epObj.v > 0
       THEN IF epObj.r /\ AllocHonorsRetire
            THEN /\ pc' = [pc EXCEPT ![p] = Down(p)]
                 /\ UNCHANGED <<myEpoch, myBody, myExp, myAmb, nonceCtr>>
            \* A 26.6.4 decoder skips the retired field and re-encodes without it.
            ELSE /\ myEpoch' = [myEpoch EXCEPT ![p] = epObj.v]
                 /\ myBody' = [myBody EXCEPT ![p] = [v |-> epObj.v + 1, n |-> NewN, r |-> FALSE]]
                 /\ myExp' = [myExp EXCEPT ![p] = Tok]
                 /\ myAmb' = [myAmb EXCEPT ![p] = FALSE]
                 /\ BumpNonce
                 /\ pc' = [pc EXCEPT ![p] = "awrite"]
       ELSE /\ pc' = [pc EXCEPT ![p] = "aempty"]
            /\ UNCHANGED <<myEpoch, myBody, myExp, myAmb, nonceCtr>>
    /\ UNCHANGED <<Store, inflight, ambUsed, Ghost, myMTok, starts, mf, DVars, GVars>>
    /\ UNCHANGED XVars

\* serverRootSubtreeEmpty: LIST manifests (+ roots; catalog folded into refs).
AllocEmpty(p) ==
    /\ pc[p] = "aempty"
    /\ pc' = [pc EXCEPT ![p] = IF manifests = {} /\ refs = {} THEN "aprobe" ELSE Down(p)]
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, Locals, DVars, GVars>>
    /\ UNCHANGED XVars

\* probeSentinel(mountKey). Absent: bootstrap at Base (current code: 1). Present: Normal refuses;
\* DecommissionRecovery mints at least surviving.writer_epoch + 1 unless the lease looks live.
\* myEpoch carries the lower bound to ABase when the base needs a fresh sample.
Decide(p, e) ==
    /\ myEpoch' = [myEpoch EXCEPT ![p] = e]
    /\ myBody' = [myBody EXCEPT ![p] = [v |-> e + 1, n |-> NewN, r |-> FALSE]]
    /\ myExp' = [myExp EXCEPT ![p] = NONE]
    /\ myAmb' = [myAmb EXCEPT ![p] = FALSE]
    /\ BumpNonce
    /\ pc' = [pc EXCEPT ![p] = "awrite"]

AllocProbe(p) ==
    /\ pc[p] = "aprobe"
    /\ LET recover == mount.e > 0 /\ p = "d" /\ (mount.clean \/ ProvenDead(mount.holder, mount.e))
           lower == IF mount.e = 0 THEN 1 ELSE mount.e + 1
       IN IF mount.e = 0 \/ recover
          THEN IF FreshSample
               THEN /\ myEpoch' = [myEpoch EXCEPT ![p] = lower]
                    /\ pc' = [pc EXCEPT ![p] = "abase"]
                    /\ UNCHANGED <<myBody, myExp, myAmb, nonceCtr>>
               ELSE Decide(p, Max2(lower, Base(myTime[p], myHwm[p])))
          ELSE /\ pc' = [pc EXCEPT ![p] = Down(p)]
               /\ UNCHANGED <<myEpoch, myBody, myExp, myAmb, nonceCtr>>
    /\ UNCHANGED <<Store, inflight, ambUsed, Ghost, myMTok, starts, mf, DVars, GVars, XVars>>

\* V1: PUT `_probe/<uid>/time` + HEAD for its Last-Modified (fused: the key is private to this process,
\* so only the clock can move between the two).  V2: GET `_pool_meta`.
ABase(p) ==
    /\ pc[p] = "abase"
    /\ Decide(p, Max2(myEpoch[p], Base(clock, hwm)))
    /\ UNCHANGED <<Store, inflight, ambUsed, Ghost, myMTok, starts, mf, DVars, GVars, XVars>>

Allocated(p) ==
    /\ allocLog' = allocLog \cup {myEpoch[p]}
    /\ dup' = (dup \/ myEpoch[p] \in allocLog)
    /\ UNCHANGED slotMax
    /\ pc' = [pc EXCEPT ![p] = "cread"]

\* writeLoop attempt: backend->write with If-Match / If-None-Match.  Three disjuncts:
\*   (1) the store answers (2xx or 412) and the client sees it;
\*   (2) the store applies it and the response is lost (landing step; the client's next step is the
\*       separate resolve read AllocResolve);
\*   (3) the request is still in flight when the client gives up on it (LateLand lands it later).
AllocWrite(p) ==
    /\ pc[p] = "awrite"
    /\ \/ /\ PreHolds(myExp[p])
          /\ ApplyEpoch(myBody[p])
          /\ Allocated(p)
          /\ UNCHANGED <<inflight, ambUsed>>
       \/ /\ ~PreHolds(myExp[p])
          \* 412: settled by ONE read; with an earlier ambiguity of this inner write it is a resolve.
          /\ pc' = [pc EXCEPT ![p] = IF myAmb[p] THEN "aresolve" ELSE "aread"]
          /\ UNCHANGED <<EpVars, inflight, ambUsed, Ghost>>
       \/ /\ ambUsed < MaxAmbig /\ PreHolds(myExp[p])
          /\ ApplyEpoch(myBody[p])
          /\ ambUsed' = ambUsed + 1
          /\ pc' = [pc EXCEPT ![p] = "aresolve"]
          /\ UNCHANGED <<inflight, Ghost>>
       \/ /\ ambUsed < MaxAmbig
          /\ inflight' = inflight \cup {[exp |-> myExp[p], body |-> myBody[p]]}
          /\ ambUsed' = ambUsed + 1
          /\ pc' = [pc EXCEPT ![p] = "aresolve"]
          /\ UNCHANGED <<EpVars, Ghost>>
    /\ myAmb' = [myAmb EXCEPT ![p] = myAmb[p] \/ pc'[p] = "aresolve"]
    /\ UNCHANGED <<mount, mGen, ownerRetired, manifests, refs, nonceCtr, myEpoch, myBody, myExp,
                   myMTok, starts, mf, DVars, GVars>>
    /\ UNCHANGED XVars

\* Store side: a sent counter write lands late (or is refused by its precondition).
LateLand(r) ==
    /\ inflight' = inflight \ {r}
    /\ IF PreHolds(r.exp) THEN ApplyEpoch(r.body) ELSE UNCHANGED EpVars
    /\ UNCHANGED <<mount, mGen, ownerRetired, manifests, refs, ambUsed, nonceCtr, Ghost, pc, Locals,
                   DVars, GVars>>
    /\ UNCHANGED XVars

\* writeLoop resolve read: precondition still satisfiable -> reissue the same bytes; moved and
\* `obj->bytes == bytes` -> Committed(resolved_by_read); otherwise Conflict -> decide again.
AllocResolve(p) ==
    /\ pc[p] = "aresolve"
    /\ IF PreHolds(myExp[p])
       THEN /\ pc' = [pc EXCEPT ![p] = "awrite"]
            /\ UNCHANGED Ghost
       ELSE IF epObj.v > 0 /\ SameBytes(epObj, myBody[p])
            THEN Allocated(p)
            ELSE /\ pc' = [pc EXCEPT ![p] = "aread"]
                 /\ UNCHANGED Ghost
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Locals, DVars, GVars>>
    /\ UNCHANGED XVars

-----------------------------------------------------------------------------
(* claimMount (PR #2512 rules)                                             *)

\* GET mount + the in-memory branch.  Older epoch not clean and not proven dead: no step
\* (claimMountAwaitingExpiry keeps polling).
ClaimRead(p) ==
    /\ pc[p] = "cread"
    /\ \/ mount.e = 0
       \/ mount.e >= myEpoch[p]
       \/ mount.clean \/ ProvenDead(mount.holder, mount.e)
    /\ IF mount.e = 0
       THEN /\ pc' = [pc EXCEPT ![p] = IF MintChecksCounter THEN "cchk" ELSE "cmint"]
            /\ UNCHANGED myMTok
       ELSE IF mount.e > myEpoch[p]
       THEN /\ pc' = [pc EXCEPT ![p] = Down(p)] /\ UNCHANGED myMTok          \* newer_epoch -> ABORTED
       ELSE IF mount.e = myEpoch[p] /\ StrictClaim
       THEN /\ pc' = [pc EXCEPT ![p] = Down(p)] /\ UNCHANGED myMTok          \* same_epoch_occupied
       ELSE /\ pc' = [pc EXCEPT ![p] = "crecl"]                            \* reclaim (or base refresh)
            /\ myMTok' = [myMTok EXCEPT ![p] = mGen]
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, myEpoch, myBody, myExp, myAmb, starts, mf,
                   DVars, GVars>>
    /\ UNCHANGED XVars

\* Proposed check before the mint: GET `epoch`; our own bytes must still be there.
ClaimCheckCounter(p) ==
    /\ pc[p] = "cchk"
    /\ pc' = [pc EXCEPT ![p] = IF epObj = myBody[p] THEN "cmint" ELSE Down(p)]
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, Locals, DVars, GVars, XVars>>

\* op.create(mountKey): If-None-Match; a race is racedDoubleStart.
ClaimMint(p) ==
    /\ pc[p] = "cmint"
    /\ IF mount.e = 0
       THEN /\ mount' = [e |-> myEpoch[p], clean |-> FALSE, holder |-> p]
            /\ mGen' = mGen + 1
            /\ myMTok' = [myMTok EXCEPT ![p] = mGen + 1]
            /\ pc' = [pc EXCEPT ![p] = Hold(p)]
            /\ slotMax' = IF myEpoch[p] > slotMax THEN myEpoch[p] ELSE slotMax
       ELSE /\ pc' = [pc EXCEPT ![p] = Down(p)] /\ UNCHANGED <<mount, mGen, myMTok, slotMax>>
    /\ UNCHANGED <<EpVars, ownerRetired, manifests, refs, inflight, ambUsed, nonceCtr, allocLog, dup,
                   myEpoch, myBody, myExp, myAmb, starts, mf, DVars, GVars>>
    /\ UNCHANGED XVars

\* op.replace(mountKey, body, got->etag): If-Match on the token ClaimRead saw.
ClaimReclaim(p) ==
    /\ pc[p] = "crecl"
    /\ IF mount.e > 0 /\ mGen = myMTok[p]
       THEN /\ mount' = [e |-> myEpoch[p], clean |-> FALSE, holder |-> p]
            /\ mGen' = mGen + 1
            /\ myMTok' = [myMTok EXCEPT ![p] = mGen + 1]
            /\ pc' = [pc EXCEPT ![p] = Hold(p)]
            /\ slotMax' = IF myEpoch[p] > slotMax THEN myEpoch[p] ELSE slotMax
       ELSE /\ pc' = [pc EXCEPT ![p] = Down(p)] /\ UNCHANGED <<mount, mGen, myMTok, slotMax>>
    /\ UNCHANGED <<EpVars, ownerRetired, manifests, refs, inflight, ambUsed, nonceCtr, allocLog, dup,
                   myEpoch, myBody, myExp, myAmb, starts, mf, DVars, GVars>>
    /\ UNCHANGED XVars

-----------------------------------------------------------------------------
(* A live writer                                                           *)

\* Manifest create (If-None-Match) of (ns, epoch, build_seq 1): in flight, protected only by the floor.
WriteManifest(p) ==
    /\ pc[p] = "live" /\ mf[p] = "none"
    /\ IF myEpoch[p] \notin manifests
       THEN manifests' = manifests \cup {myEpoch[p]} /\ mf' = [mf EXCEPT ![p] = "written"]
       ELSE UNCHANGED manifests /\ mf' = [mf EXCEPT ![p] = "failed"]
    /\ UNCHANGED <<EpVars, mount, mGen, ownerRetired, refs, inflight, ambUsed, nonceCtr, Ghost, pc,
                   myEpoch, myBody, myExp, myAmb, myMTok, starts, DVars, GVars>>
    /\ UNCHANGED XVars

\* Ref commit naming the manifest; it does not re-read the manifest.
Publish(p) ==
    /\ pc[p] = "live" /\ mf[p] = "written"
    /\ refs' = refs \cup {myEpoch[p]}
    /\ mf' = [mf EXCEPT ![p] = "published"]
    /\ UNCHANGED <<EpVars, mount, mGen, ownerRetired, manifests, inflight, ambUsed, nonceCtr, Ghost,
                   pc, myEpoch, myBody, myExp, myAmb, myMTok, starts, DVars, GVars>>
    /\ UNCHANGED XVars

\* MountLeaseRenewer::terminate: If-Match farewell (min_active_build_sequence = UINT64_MAX).
\* Also admin.reset() for the decommission tool.
Farewell(p) ==
    /\ \/ p \in Inst /\ pc[p] = "live"
       \/ p = "d" /\ pc[p] = "dfarewell"
    /\ IF mGen = myMTok[p] /\ mount.e > 0
       THEN mount' = [mount EXCEPT !.clean = TRUE] /\ mGen' = mGen + 1
       ELSE UNCHANGED <<mount, mGen>>
    /\ pc' = [pc EXCEPT ![p] = IF p = "d" THEN "dreadm" ELSE "down"]
    /\ UNCHANGED <<EpVars, ownerRetired, manifests, refs, inflight, ambUsed, nonceCtr, Ghost, Locals,
                   DVars, GVars>>
    /\ UNCHANGED XVars

\* Process death: no store request.
Crash(p) ==
    /\ p \in Inst /\ pc[p] \notin {"down"}
    /\ pc' = [pc EXCEPT ![p] = "down"]
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, Locals, DVars, GVars>>
    /\ UNCHANGED XVars

-----------------------------------------------------------------------------
(* decommissionPoolMember tail                                             *)

\* The namespace drain under the admin claim, abstracted: the victim's namespaces are dropped.
DDrain ==
    /\ pc["d"] = "dlive"
    /\ refs' = {}
    /\ pc' = [pc EXCEPT !["d"] = "dcap"]
    /\ UNCHANGED <<EpVars, mount, mGen, ownerRetired, manifests, inflight, ambUsed, nonceCtr, Ghost,
                   Locals, DVars, GVars>>
    /\ UNCHANGED XVars

\* op.read(epoch_key) under the claim: claimed_epoch.
DCapture ==
    /\ pc["d"] = "dcap"
    /\ IF epObj.v > 0
       THEN dCap' = [obj |-> epObj, tok |-> Tok] /\ pc' = [pc EXCEPT !["d"] = "dfarewell"]
       ELSE UNCHANGED dCap /\ pc' = [pc EXCEPT !["d"] = "dstop"]
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, Locals, dFar, dOwn, GVars>>
    /\ UNCHANGED XVars

\* op.read(mount_key) + captures_match check (in-memory).
DReadMount ==
    /\ pc["d"] = "dreadm"
    /\ IF /\ mount.e > 0 /\ mount.clean
          /\ mount.e + 1 = dCap.obj.v
          /\ ~dCap.obj.r
       THEN dFar' = mGen /\ pc' = [pc EXCEPT !["d"] = IF Retire THEN "dretire" ELSE "ddelm"]
       ELSE UNCHANGED dFar /\ pc' = [pc EXCEPT !["d"] = "dstop"]
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, Locals, dCap, dOwn, GVars>>
    /\ UNCHANGED XVars

\* deleteSlotObject(mount_key, farewell etag).  In the fix it runs last.
DDelMount ==
    /\ pc["d"] = "ddelm"
    /\ NoZombie => ~PendingMountClaim
    /\ IF mount.e > 0 /\ mGen = dFar
       THEN /\ mount' = MAbsent /\ mGen' = mGen + 1
            /\ pc' = [pc EXCEPT !["d"] = IF Retire THEN "dstop"
                                         ELSE IF PoolHwm /\ ~HwmAfterDelete THEN "dhwmr" ELSE "ddele"]
       ELSE UNCHANGED <<mount, mGen>> /\ pc' = [pc EXCEPT !["d"] = "dstop"]
    /\ UNCHANGED <<EpVars, ownerRetired, manifests, refs, inflight, ambUsed, nonceCtr, Ghost, Locals,
                   DVars, GVars>>
    /\ UNCHANGED XVars

\* deleteSlotObject(epoch_key, claimed_epoch->etag)  (current code only).
DDelEpoch ==
    /\ pc["d"] = "ddele"
    /\ NoZombie => ~PendingCounterWrite
    /\ TailTakesASecond => clock > epBornAt
    /\ IF PreHolds(dCap.tok)
       THEN /\ epObj' = EAbsent /\ epGen' = IF S3 THEN epGen ELSE epGen + 1
            /\ UNCHANGED <<epBornAt, maxCounter, regress>>
            /\ pc' = [pc EXCEPT !["d"] = IF PoolHwm /\ HwmAfterDelete THEN "dhwmr" ELSE "drechm"]
       ELSE UNCHANGED EpVars /\ pc' = [pc EXCEPT !["d"] = "dstop"]
    /\ UNCHANGED <<mount, mGen, ownerRetired, manifests, refs, inflight, ambUsed, nonceCtr, Ghost, Locals,
                   DVars, GVars>>
    /\ UNCHANGED XVars

\* Liveness recheck: op.read(mount_key), then op.read(epoch_key).
DRecheckMount ==
    /\ pc["d"] = "drechm"
    /\ pc' = [pc EXCEPT !["d"] = IF mount.e = 0 THEN "dreche" ELSE "dstop"]
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, Locals, DVars, GVars>>
    /\ UNCHANGED XVars

DRecheckEpoch ==
    /\ pc["d"] = "dreche"
    /\ pc' = [pc EXCEPT !["d"] = IF epObj.v = 0 THEN "downer" ELSE "dstop"]
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, Locals, DVars, GVars>>
    /\ UNCHANGED XVars

\* fix_retire: op.replace(epoch_key, retired body, claimed_epoch->etag).  retired_at_ms makes the
\* bytes unique, so no other writer produces them.
DRetire ==
    /\ pc["d"] = "dretire"
    /\ IF PreHolds(dCap.tok)
       THEN /\ ApplyEpoch([v |-> dCap.obj.v, n |-> dCap.obj.n, r |-> TRUE])
            /\ pc' = [pc EXCEPT !["d"] = "downer"]
       ELSE UNCHANGED EpVars /\ pc' = [pc EXCEPT !["d"] = "dstop"]
    /\ UNCHANGED <<mount, mGen, ownerRetired, manifests, refs, inflight, ambUsed, nonceCtr, Ghost, Locals,
                   DVars, GVars>>
    /\ UNCHANGED XVars

\* op.read(owner_key).
DReadOwner ==
    /\ pc["d"] = "downer"
    /\ dOwn' = ownerRetired
    /\ pc' = [pc EXCEPT !["d"] = "dtomb"]
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, Locals, dCap, dFar, GVars>>
    /\ UNCHANGED XVars

\* op.replace(owner_key, tombstone, owner->etag).  Owner bytes change only by the tombstone.
DTombstone ==
    /\ pc["d"] = "dtomb"
    /\ IF ownerRetired = dOwn THEN ownerRetired' = TRUE ELSE UNCHANGED ownerRetired
    /\ pc' = [pc EXCEPT !["d"] = IF Retire /\ RetireDeletesMount THEN "ddelm" ELSE "dstop"]
    /\ UNCHANGED <<EpVars, mount, mGen, manifests, refs, inflight, ambUsed, nonceCtr, Ghost, Locals,
                   DVars, GVars>>
    /\ UNCHANGED XVars

\* V2: `_pool_meta` read-modify-write (CasOperation::readModifyWrite, as admitOrValidate does):
\* GET, then If-Match write of max(hwm, captured next_writer_epoch); a conflict re-reads.
DHwmRead ==
    /\ pc["d"] = "dhwmr"
    /\ dHwm' = [v |-> hwm, tok |-> hwmGen]
    /\ pc' = [pc EXCEPT !["d"] = "dhwmw"]
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, Locals, DVars, GVars,
                   clock, backUsed, hwm, hwmGen, admitUsed, myTime, myHwm>>

DHwmWrite ==
    /\ pc["d"] = "dhwmw"
    /\ LET next == IF HwmAfterDelete THEN "drechm" ELSE "ddele" IN
       IF dHwm.v >= dCap.obj.v
       THEN /\ pc' = [pc EXCEPT !["d"] = next] /\ UNCHANGED <<hwm, hwmGen>>      \* decide declines
       ELSE IF hwmGen = dHwm.tok
       THEN /\ hwm' = dCap.obj.v /\ hwmGen' = hwmGen + 1
            /\ pc' = [pc EXCEPT !["d"] = next]
       ELSE /\ pc' = [pc EXCEPT !["d"] = "dhwmr"] /\ UNCHANGED <<hwm, hwmGen>>
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, Locals, DVars, GVars,
                   clock, backUsed, admitUsed, myTime, myHwm, dHwm>>

\* A concurrent open's PoolMeta admitOrValidate write (rare: a new hash algorithm admitted).
\* A 26.6.4 binary re-encodes the decoded struct, dropping the unknown hwm field.
PoolMetaAdmit ==
    /\ PoolHwm /\ ~admitUsed
    /\ admitUsed' = TRUE
    /\ hwmGen' = hwmGen + 1
    /\ hwm' = IF OldAdmitDropsHwm THEN 0 ELSE hwm
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, pc, Locals, DVars, GVars,
                   clock, backUsed, myTime, myHwm, dHwm>>

\* Environment: store time advances; optionally steps back once.
Tick ==
    /\ StoreTime /\ clock < MaxClock
    /\ clock' = clock + 1
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, pc, Locals, DVars, GVars,
                   backUsed, hwm, hwmGen, admitUsed, myTime, myHwm, dHwm>>

ClockStepBack ==
    /\ StoreTime /\ ClockBack /\ ~backUsed /\ clock > 0
    /\ clock' = clock - 1 /\ backUsed' = TRUE
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, pc, Locals, DVars, GVars,
                   hwm, hwmGen, admitUsed, myTime, myHwm, dHwm>>

-----------------------------------------------------------------------------
(* Orphan manifest sweep                                                   *)

\* floorForNamespace: GET mount.
GcFloor ==
    /\ gpc = "gidle" /\ gRounds < MaxGcRounds
    /\ gfloor' = [e |-> mount.e, clean |-> mount.clean, holder |-> "none"]
    /\ gpc' = "gview"
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, pc, Locals, DVars, gview, gRounds>>
    /\ UNCHANGED XVars

\* Protection view: the committed refs (catalog-authoritative recovery), read after the floor.
GcView ==
    /\ gpc = "gview"
    /\ gview' = refs
    /\ gpc' = "gsweep"
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, pc, Locals, DVars, gfloor, gRounds>>
    /\ UNCHANGED XVars

\* Exact-token delete of one listed manifest: not in the view, eligible under the cached floor.
GcDelete(k) ==
    /\ gpc = "gsweep"
    /\ k \in manifests /\ k \notin gview /\ Eligible(gfloor, k)
    /\ manifests' = manifests \ {k}
    /\ UNCHANGED <<EpVars, mount, mGen, ownerRetired, refs, inflight, ambUsed, nonceCtr, Ghost, pc,
                   Locals, DVars, GVars>>
    /\ UNCHANGED XVars

GcEnd ==
    /\ gpc = "gsweep"
    /\ gpc' = "gidle" /\ gRounds' = gRounds + 1
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, pc, Locals, DVars, gfloor, gview>>
    /\ UNCHANGED XVars

-----------------------------------------------------------------------------

Next ==
    \/ \E p \in Procs :
        \/ Start(p) \/ OpenProbe(p) \/ OwnerCheck(p) \/ AllocRead(p) \/ AllocEmpty(p) \/ AllocProbe(p)
        \/ ABase(p)
        \/ AllocWrite(p) \/ AllocResolve(p)
        \/ ClaimRead(p) \/ ClaimCheckCounter(p) \/ ClaimMint(p) \/ ClaimReclaim(p)
        \/ WriteManifest(p) \/ Publish(p) \/ Farewell(p) \/ Crash(p)
    \/ \E r \in inflight : LateLand(r)
    \/ Decom /\ (DDrain \/ DCapture \/ DReadMount \/ DDelMount \/ DDelEpoch \/ DRecheckMount
                 \/ DRecheckEpoch \/ DRetire \/ DReadOwner \/ DTombstone \/ DHwmRead \/ DHwmWrite)
    \/ PoolMetaAdmit \/ Tick \/ ClockStepBack
    \/ GcFloor \/ GcView \/ GcEnd \/ \E k \in manifests : GcDelete(k)

Spec == Init /\ [][Next]_vars

StateBound == epObj.v <= EMax

-----------------------------------------------------------------------------
(* Invariants                                                              *)

\* No epoch value is ever returned twice by allocateWriterEpoch for this uuid, across decommission.
EpochNeverReused == ~dup

\* Every committed reference names a manifest that exists.
NoDangle == refs \subseteq manifests

\* No server instance holds the slot while the root is retired (owner tombstone or retired counter).
RetiredStaysRetired ==
    ~(\E i \in Inst : pc[i] = "live" /\ (ownerRetired \/ (epObj.v > 0 /\ epObj.r)))

\* Harm through epoch reuse specifically: a dangling reference in a run that re-minted an epoch.
\* Separates the re-mint path from the delayed-claim path that also violates NoDangle.
NoDangleAfterReuse == dup => NoDangle

\* Every counter write lands above every counter value ever written (a retire keeps its value):
\* a re-created counter starts above every epoch the server root ever had.
CounterNeverRegresses == ~regress

\* The mount slot never holds a writer_epoch below one it held before.
SlotNeverRegresses == mount.e = 0 \/ mount.e >= slotMax

TypeOK ==
    /\ epObj.v \in Nat /\ mount.e \in Nat
    /\ manifests \subseteq Nat /\ refs \subseteq Nat
    /\ dup \in BOOLEAN
=============================================================================
