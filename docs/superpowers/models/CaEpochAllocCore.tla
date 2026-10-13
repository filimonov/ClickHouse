-------------------------- MODULE CaEpochAllocCore --------------------------
(***************************************************************************)
(* Writer-epoch allocation, the mount slot and the decommission tail of   *)
(* one CAS server root, at release line 2a034cdb374 (antalya-26.6-20261008 *)
(* + PR #2512).  CAS-362 (twin allocation through resolve-by-bytes) and    *)
(* CAS-363 (decommission deletes `epoch`, a restart re-mints epoch 1).     *)
(* Round 2: today's tail kept; a re-created counter restarts above every   *)
(* earlier epoch from store time (V1, StoreTime) or a `_pool_meta` high-   *)
(* water mark (V2, PoolHwm).                                               *)
(* Round 3: the floor rides the owner tombstone (OwnerFloor).  Round 4:    *)
(* decommission keeps owner, epoch and mount (KeepAll); the proposed       *)
(* cas-readd-member (Rv* actions) reopens the server root.                 *)
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
(* One server root.  Every process in Procs carries uuid "u1" except the  *)
(* instances in NewUuidInst ("u2", a re-add with a new uuid): the server   *)
(* instances in Inst (twins when |Inst| > 1) and, with Decom,              *)
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
    NoZombie,           \* no request is delayed across a decommission delete: the tail's DELETE of `epoch`
                        \* (`mount`) never lands between an allocator's (claimer's) decision and its write
    NoZombieCounter,    \* only the `epoch` half of NoZombie: a delayed mount mint may still land late
    \* Owner floor: the tail tombstones `owner` with epoch_floor = captured next_writer_epoch BEFORE it
    \* deletes `mount` and `epoch`; an absent counter restarts at max(floor + 1, 1) from a fresh owner GET.
    OwnerFloor,
    TombAfterDelete,    \* sabotage: the floor tombstone runs after the deletes (today's order)
    FloorStale,         \* sabotage: the absent-counter base uses the floor read by claimOwnerOrThrow
    FloorIgnoresTomb,   \* sabotage: the fresh owner GET does not refuse a tombstoned owner
    UntombOnMove,       \* S11 mitigation: after the tombstone, GET `epoch`; moved off the capture ->
                        \* un-tombstone (If-Match on our tombstone, floor kept) and stop
    UntombOnDelFail,    \* also un-tombstone when a delete after the tombstone finds its token moved
    Revive,             \* operator revival: clear retired_at_ms by If-Match, floor kept
    ReviveDeletes,      \* hostile revival: the operator deletes `owner` instead
    LateInst,           \* instances that start only after a revival: a fresh, empty member
    DecomRetry,         \* the tool may die anywhere in its run and be re-run once (it accepts a tombstone)
    TombAmbig,          \* the tombstone write may stay unresolved: the tool stops, the PUT lands later
    KeepSlot,           \* owner-floor tail that stops after the tombstone: `mount` and `epoch` stay
    \* Keep-everything design: decommission deletes only data; `owner` (tombstoned), `epoch` and the
    \* farewell `mount` stay.  Revival (proposed cas-readd-member) reopens the srid.
    KeepAll,
    TombLast,           \* sabotage: the tombstone runs after the farewell, not under the admin claim
    DecomDeletesEpoch,  \* sabotage: the tail also deletes `epoch` after the farewell
    ClaimChecksOwner,   \* proposed: claimMount GETs `owner` after its mount GET, before its write
    NewUuidInst,        \* instances carrying the second server_uuid "u2"
    Revivers,           \* concurrent revival runs
    ReviveUuid,         \* uuid the revival writes into `owner` ("u1" = same identity)
    ReviveOwnerFirst,   \* sabotage: revival rewrites `owner` before it removes the farewell `mount`
    RetryRefusesTomb,   \* sabotage: a decommission re-run refuses its own tombstone (today's claimOwnerOrThrow)
    OwnerNonce,         \* every owner write carries fresh random bytes (FALSE: S3 tokens repeat for equal bodies)
    ClaimRecheckOwner,  \* proposed: GET `owner` after a landed claim; tombstoned/foreign -> terminal body, stop
    RefusedFarewellFences, \* proposed: that terminal body is gc_fenced, not a clean farewell
    AdminConfirmsClaim, \* proposed: after the tombstone the tool GETs `mount`; not its own claim -> un-tombstone, stop
    GcFences,           \* GC may fence the decommission tool's lease while the tool keeps running
    ReaddAcceptsFenced, \* sabotage: re-add accepts a gc_fenced mount, not only a clean farewell
    ZombieRun,          \* a re-run leaves the paused earlier run's decided tombstone and next steps to land later
    ReviveRewritesMount,\* proposed: re-add replaces the farewell `mount` (new uuid, clean) instead of deleting it
    \* Round 5 (spec r3):
    TombAlways,         \* every run rewrites the tombstone by If-Match, also over an existing one (sabotage: FALSE)
    VerifyRetired,      \* after the farewell: GET mount (own clean farewell), GET owner (own tombstone) -> done
    ReaddOneWrite,      \* re-add: GET owner, GET epoch, GET mount (clean), emptiness, one If-Match owner write
    ReaddSkipsClean,    \* sabotage: re-add accepts any mount body
    ClaimForeignClean,  \* claimMount reclaims a clean farewell of another uuid
    ClaimForeignFenced, \* sabotage: claimMount also reclaims a gc_fenced body of another uuid
    RaceUuids,          \* reviver "r2" targets the other uuid
    DrainFenced,        \* the admin drains on its own lease: no drain step once its claim is gone
    DrainInFlight,      \* a drain request sent under the lease may land after it (a late delete)
    AdminRefusesAbsent, \* the decommission tool never creates an absent counter
    \* Round 6 (spec r4):
    ReaddAnyTerminal,   \* re-add accepts a gc_fenced mount as well as a clean farewell
    RecheckRetiredOnly, \* sabotage: the post-claim owner re-read refuses only a retired owner
    GcFencesDead,       \* GC fences the expired lease of a dead holder (any uuid)
    RollbackOnlyIfIntroduced, \* un-tombstone only when this run's owner read found it not retired
    RollbackFenced,     \* sabotage: the un-tombstone runs on the tool's lease plane (refused once fenced)
    TombChecksUuid,     \* the tool's owner read stops on an owner of another uuid
    VerifiedMark,       \* proposed: step 6 marks the tombstone verified; a re-run rolls back unverified ones
    Rejoin,             \* a server start over a retired owner (any uuid) rejoins inside claimOwnerOrThrow
    RejoinAnyMount,     \* sabotage: rejoin accepts a live-looking mount
    \* Round 7 (spec r7):
    RemountOn,          \* a process whose lease is lost self-remounts: owner check without rejoin, new claim
    GcFencesLive,       \* GC fences the lease of a live (partitioned) server instance
    KeepVerified,       \* step 2 keeps retirement_verified when it refreshes a verified tombstone
    DRestarts,          \* decommission re-runs allowed (1 = one re-run)
    DrainLeavesRows,    \* the drain leaves catalog rows Removing until a later GC step
    JoinReplaceMount,   \* new-uuid rejoin replaces the terminal mount body by If-Match before the owner write
    JoinOwnerFirst,     \* sabotage: the owner write comes before the mount replacement
    OldInst,            \* 26.6.4 starters: no rejoin, no foreign reclaim, no claim recheck
    AdminRemountConfirms, \* proposed: a failed admin remount after this run's tombstone goes to step 3 (rollback)
    ForeignNeedsOwner,  \* proposed: claimMount reclaims a foreign body only after GET owner shows its own uuid
    \* Round 8 (spec r8): the owner token is a monotone owner_life (ownerGen; every owner write is +1).
    OwnerLife,          \* a process records the life it started under; recheck, remount and foreign reclaim
                        \* require exactly (own uuid, recorded life, not retired); the admin rechecks too
    RecheckIgnoresLife, \* sabotage: the claim recheck does not compare the life
    RollbackIgnoresLife \* sabotage: a rollback clears any retired owner of the victim uuid, not only its own

ASSUME MaxStarts \in Nat /\ MaxAmbig \in Nat /\ MaxGcRounds \in Nat /\ EMax \in Nat
ASSUME Scale \in Nat \ {0} /\ MaxClock \in Nat
ASSUME ~(StoreTime /\ PoolHwm) /\ ~(OwnerFloor /\ (StoreTime \/ PoolHwm))
ASSUME LateInst \subseteq Inst /\ NewUuidInst \subseteq Inst /\ OldInst \subseteq Inst /\ DRestarts \in Nat
ASSUME ~(KeepAll /\ (OwnerFloor \/ Retire \/ PoolHwm \/ StoreTime))
ASSUME ~(KeepAll /\ (OwnerFloor \/ Retire \/ PoolHwm \/ StoreTime))

Procs == Inst \cup (IF Decom THEN {"d"} ELSE {})

\* `epoch` object. v = next_writer_epoch (0 = absent; the decoder rejects 0), n = allocation nonce,
\* r = retired marker.
EAbsent == [v |-> 0, n |-> 0, r |-> FALSE]
\* `mount` object. holder is a ghost used only to abstract claimMountAwaitingExpiry's observation wait.
MAbsent == [e |-> 0, clean |-> FALSE, holder |-> "none", u |-> "none", fenced |-> FALSE]
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
    dHwm,               \* decommission: `_pool_meta` read [v, tok]
    ownerFloor, ownerGen, ownerGone, \* owner epoch_floor, token (every write is new bytes), deleted
    dOwnTok, dTombTok,  \* decommission: owner token read before the tombstone; token of our tombstone
    myFloor,            \* per process: epoch_floor read by claimOwnerOrThrow (FloorStale sabotage)
    life,               \* ghost: revivals so far
    allocLife,          \* ghost: <<epoch, life at the allocation>> for every allocation
    rpc, rTok,          \* operator revival: state, owner token read
    oInfl,              \* a tombstone PUT decided, not yet landed: [on, tok, floor, life, ep, mtok]
    ownerUuid,          \* owner server_uuid
    rv, rvMTok, rvOTok, \* keep-everything revival runs: state, mount token, owner token
    tombOverLive,       \* ghost (lifecycle rows only): a tombstone landed while an instance held the slot live
    dTombRefused,       \* ghost: a decommission re-run refused on the tombstoned owner
    oStamp,             \* owner retired_at_ms (0 = none); unique per tombstone write
    zDrain,             \* a paused earlier decommission run's next step: [on, mode, ep, mtok, tok]
    dLife,              \* life in which the current decommission run read the owner
    staleDecom,         \* ghost: a decommission tombstone landed in a later life than its owner read
    staleDrain,         \* ghost: a decommission drain removed a reference of an epoch newer than its run's own
    lateDel,            \* a drain request in flight: [on, ep]
    doneLives,          \* ghost: every life in which a decommission run verified its end
    tombForeign,        \* ghost: a decommission tombstone landed on an owner of another uuid
    vTok,               \* owner token written by the verified mark (the owner is verified while OTok = vTok)
    jTok,               \* per process: owner token read by claimOwnerOrThrow (rejoin If-Match)
    selfFenced,         \* ghost (rejoin rows): a claimer refused after its claim fenced its own lease
    stLife,             \* per process: the owner's rejoin generation (life) when the process started
    remnt,              \* per process: the current claim attempt is a self-remount
    resumedStale,       \* ghost: a self-remount resumed a process that started before a later rejoin
    dClaimEp,           \* the decommission run's writer_epoch recorded at its first claim
    catRows,            \* catalog rows of the victim still Removing (GC not done)
    jRepl,              \* per process: rejoin mount replacement [tok, need]
    mfEp,               \* per process (remount rows): epoch of the manifest the current build staged
    pLife               \* per process: owner life (ownerGen) recorded at start, after any rejoin

vars == <<epObj, epGen, mount, mGen, ownerRetired, manifests, refs, inflight, ambUsed, nonceCtr,
          allocLog, dup, slotMax, pc, myEpoch, myBody, myExp, myAmb, myMTok, starts, mf,
          dCap, dFar, dOwn, gpc, gfloor, gview, gRounds,
          epBornAt, maxCounter, regress, clock, backUsed, hwm, hwmGen, admitUsed, myTime, myHwm, dHwm,
          ownerFloor, ownerGen, ownerGone, dOwnTok, dTombTok, myFloor, life, allocLife, rpc, rTok,
          oInfl, ownerUuid, rv, rvMTok, rvOTok, tombOverLive, dTombRefused, oStamp, zDrain, dLife,
          staleDecom, staleDrain, lateDel, doneLives, tombForeign, vTok, jTok, selfFenced,
          stLife, remnt, resumedStale, dClaimEp, catRows, jRepl, mfEp, pLife>>

EpVars == <<epObj, epGen, epBornAt, maxCounter, regress>>
LVars == <<oStamp, zDrain, dLife, staleDecom, staleDrain>>
NVarsNoRv == <<ownerFloor, ownerGen, ownerGone, dOwnTok, dTombTok, myFloor, life, rpc, rTok, oInfl,
               ownerUuid, tombOverLive, dTombRefused, LVars>>
KVars == <<ownerUuid, rv, rvMTok, rvOTok, tombOverLive, dTombRefused, LVars>>
NVars == <<ownerFloor, ownerGen, ownerGone, dOwnTok, dTombTok, myFloor, life, rpc, rTok, oInfl,
           ownerUuid, rv, rvMTok, rvOTok, tombOverLive, dTombRefused, LVars>>
XVars == <<clock, backUsed, hwm, hwmGen, admitUsed, myTime, myHwm, dHwm, NVars>>

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
\* With OwnerFloor, h is the owner's epoch_floor.
Base(t, h) == IF StoreTime THEN t * Scale + 1
              ELSE IF PoolHwm \/ OwnerFloor THEN Max2(h + 1, 1)
              ELSE 1
FreshSample == (StoreTime /\ TimeAfterAbsent) \/ (PoolHwm /\ HwmFresh) \/ (OwnerFloor /\ ~FloorStale)
StaleBound(p) == IF OwnerFloor THEN myFloor[p] ELSE myHwm[p]

\* A request decided before, and possibly landing after, a decommission DELETE.
PendingCounterWrite == inflight # {} \/ \E q \in Procs : pc[q] \in {"awrite", "aresolve"}
PendingMountClaim == \E q \in Procs : pc[q] \in {"cmint", "crecl"}

Down(p) == IF p = "d" THEN "dstop" ELSE "down"

\* Owner-floor tail: the tombstone with the floor comes first, so a re-run of the tool must accept it
\* (openForDecommission -> claimOwnerOrThrow).  Today's tool refuses a tombstoned owner.
Tail2 == OwnerFloor /\ ~TombAfterDelete
AcceptsTomb(p) == p = "d" /\ (OwnerFloor \/ (KeepAll /\ ~RetryRefusesTomb))
AfterTombNext == IF KeepSlot THEN "dstop" ELSE "ddelm"
Uuid(p) == IF p \in NewUuidInst THEN "u2" ELSE "u1"
OtherUuid(u) == IF u = "u1" THEN "u2" ELSE "u1"
\* Target uuid of re-add run r.
RvUuid(r) == IF RaceUuids /\ r = "r2" THEN OtherUuid(ReviveUuid) ELSE ReviveUuid
\* claimMount over a body of another uuid.
ForeignReclaimable == (ClaimForeignClean /\ mount.clean /\ ~mount.fenced) \/ (ClaimForeignFenced /\ mount.fenced)
ForeignOk(p) == p \notin OldInst /\ ForeignReclaimable
\* The decommission tool's claim is still in the slot, unfenced (its lease-deadline fence has not fired).
OwnClaim(t) == mGen = t /\ mount.holder = "d" /\ ~mount.fenced
MintNext == IF MintChecksCounter THEN "cchk" ELSE "cmint"
RvAfterMount == IF ReviveOwnerFirst THEN "rdone" ELSE "roread"
AfterKeepTomb == IF AdminConfirmsClaim THEN "dchk" ELSE "ddrain"

\* Owner token.  Without the nonce an S3 ETag is the MD5 of the body, so equal bodies share a token;
\* a tombstone's retired_at_ms (oStamp) is unique.  With the nonce every write is new bytes.
OTokOf(u, r, f, st, g) == IF S3 /\ ~OwnerNonce THEN <<u, r, f, st>> ELSE <<g>>
OTok == OTokOf(ownerUuid, ownerRetired, ownerFloor, oStamp, ownerGen)
\* Any later owner write changes the token, so the mark lasts until the next owner write.
OwnerVerified == OTok = vTok
Hold(p) == IF p = "d" THEN "dlive" ELSE "live"
AfterClaim(p) == IF ClaimRecheckOwner /\ (p \in Inst \ OldInst \/ (OwnerLife /\ p = "d")) THEN "cpost" ELSE Hold(p)
\* Owner life check: the owner is exactly the one this process started under.
LifeOk(p) == ~OwnerLife \/ ownerGen = pLife[p]
\* A claim that makes p live after a self-remount, for a process that started before a later rejoin.
ResumesStale(p) == remnt[p] /\ stLife[p] < life
HoldStates == {"live", "dlive", "dcap", "dfarewell", "cpost", "cpfare", "dchk"} \cup (IF KeepAll THEN {"downer", "dtomb", "ddrain"} ELSE {})

\* Ghost for claimMountAwaitingExpiry's token-stability wait: a holder is proven dead when the process
\* that wrote the body no longer holds that epoch.
\* IF, not \/: TLC explores every disjunct of an action-level \/ and would apply pc to "none".
\* An instance holds the slot with an unclean lease of its own epoch.
LiveHolder == \E i \in Inst : pc[i] = "live" /\ mount.holder = i /\ mount.e = myEpoch[i] /\ ~mount.clean

\* Remount rows: a process counts as live only while it holds its own unfenced lease; a fenced one is
\* refused by the mount plane and remounts.
OwnLease(p) == mount.holder = p /\ mount.e = myEpoch[p] /\ ~mount.fenced
LiveP(i) == pc[i] = "live" /\ (RemountOn => OwnLease(i))

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
    /\ ownerFloor = 0 /\ ownerGen = 0 /\ ownerGone = FALSE /\ dOwnTok = <<>> /\ dTombTok = <<>>
    /\ myFloor = [p \in Procs |-> 0] /\ life = 0 /\ allocLife = {}
    /\ rpc = "ridle" /\ rTok = <<>>
    /\ oInfl = [on |-> FALSE, tok |-> <<>>, floor |-> 0, life |-> 0, ep |-> 0, mtok |-> 0, intro |-> FALSE]
    /\ ownerUuid = "u1" /\ rv = [r \in Revivers |-> "idle"]
    /\ rvMTok = [r \in Revivers |-> 0] /\ rvOTok = [r \in Revivers |-> <<>>]
    /\ tombOverLive = FALSE /\ dTombRefused = FALSE
    /\ oStamp = 0 /\ zDrain = [on |-> FALSE, mode |-> "none", ep |-> 0, mtok |-> 0, tok |-> <<>>, intro |-> FALSE]
    /\ dLife = 0 /\ staleDecom = FALSE /\ staleDrain = FALSE
    /\ lateDel = [on |-> FALSE, ep |-> 0] /\ doneLives = {} /\ tombForeign = FALSE /\ vTok = <<>> /\ jTok = [p \in Procs |-> <<>>] /\ selfFenced = FALSE
    /\ stLife = [p \in Procs |-> 0] /\ remnt = [p \in Procs |-> FALSE] /\ resumedStale = FALSE
    /\ dClaimEp = 0 /\ catRows = FALSE /\ jRepl = [p \in Procs |-> [tok |-> 0, need |-> FALSE]]
    /\ mfEp = [p \in Procs |-> 0] /\ pLife = [p \in Procs |-> 0]
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
G5 == <<lateDel, doneLives, tombForeign, vTok, jTok, selfFenced>>
G7 == <<stLife, remnt, resumedStale, dClaimEp, catRows, jRepl, mfEp, pLife>>
Ghost == <<allocLog, dup, slotMax, allocLife, G5, G7>>

-----------------------------------------------------------------------------
(* Server start                                                            *)

\* Process (re)start: in-memory only.
Start(p) ==
    /\ p \in Inst /\ pc[p] = "down" /\ starts[p] < (IF p \in LateInst THEN 1 ELSE MaxStarts)
    /\ p \in LateInst => life > 0
    /\ pc' = [pc EXCEPT ![p] = "open"]
    /\ starts' = [starts EXCEPT ![p] = @ + 1]
    /\ mf' = [mf EXCEPT ![p] = "none"]
    /\ myEpoch' = [myEpoch EXCEPT ![p] = 0]
    /\ remnt' = [remnt EXCEPT ![p] = FALSE]
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, allocLog, dup, slotMax, allocLife, G5,
                   stLife, resumedStale, dClaimEp, catRows, jRepl, pLife, mfEp, myBody, myExp, myAmb, myMTok, DVars, GVars>>
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
                   clock, backUsed, hwm, hwmGen, admitUsed, dHwm, NVars>>

\* claimOwnerOrThrow: GET owner; same uuid; throwIfOwnerRetired.  Absent owner: the create branch.
OwnerCheck(p) ==
    /\ pc[p] = "owner"
    /\ pc' = [pc EXCEPT ![p] = IF ownerGone THEN "oempty"
                              ELSE IF Rejoin /\ p \in Inst \ OldInst /\ ownerRetired
                              THEN "jepoch"
                              ELSE IF ownerUuid # Uuid(p) THEN Down(p)
                              ELSE IF ownerRetired /\ ~AcceptsTomb(p) THEN Down(p) ELSE "aread"]
    /\ jTok' = IF Rejoin THEN [jTok EXCEPT ![p] = OTok] ELSE jTok
    /\ stLife' = IF RemountOn THEN [stLife EXCEPT ![p] = life] ELSE stLife
    /\ pLife' = IF OwnerLife THEN [pLife EXCEPT ![p] = ownerGen] ELSE pLife
    /\ myFloor' = [myFloor EXCEPT ![p] = ownerFloor]
    /\ dTombRefused' = (dTombRefused \/ (p = "d" /\ ~ownerGone /\ ownerRetired /\ ~AcceptsTomb(p)))
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, allocLog, dup, slotMax, allocLife,
                   lateDel, doneLives, tombForeign, vTok, selfFenced, remnt, resumedStale, dClaimEp, catRows,
                   jRepl, mfEp, Locals, DVars, GVars>>
    /\ UNCHANGED <<clock, backUsed, hwm, hwmGen, admitUsed, myTime, myHwm, dHwm,
                   ownerFloor, ownerGen, ownerGone, dOwnTok, dTombTok, life, rpc, rTok, oInfl,
                   ownerUuid, rv, rvMTok, rvOTok, tombOverLive, LVars>>

\* Proposed rejoin in claimOwnerOrThrow over a retired owner: GET epoch, GET mount (terminal),
\* serverRootSubtreeEmpty, then If-Match owner rewrite {this uuid, no tombstone}; a conflict re-reads.
JoinEpoch(p) ==
    /\ pc[p] = "jepoch"
    /\ pc' = [pc EXCEPT ![p] = IF epObj.v > 0 THEN "jmount" ELSE "down"]
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, Locals, DVars, GVars, XVars>>

JoinMount(p) ==
    /\ pc[p] = "jmount"
    /\ pc' = [pc EXCEPT ![p] = IF RejoinAnyMount \/ (mount.e > 0 /\ (mount.clean \/ mount.fenced))
                              THEN "jempty" ELSE "down"]
    /\ jRepl' = IF JoinReplaceMount THEN [jRepl EXCEPT ![p] = [tok |-> mGen, need |-> mount.u # Uuid(p)]]
                ELSE jRepl
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, allocLog, dup, slotMax, allocLife, G5,
                   stLife, remnt, resumedStale, dClaimEp, catRows, mfEp, pLife, Locals, DVars, GVars, XVars>>

JoinEmpty(p) ==
    /\ pc[p] = "jempty"
    /\ pc' = [pc EXCEPT ![p] = IF manifests = {} /\ refs = {} /\ ~catRows
                              THEN (IF jRepl[p].need /\ ~JoinOwnerFirst THEN "jrepl" ELSE "jwrite")
                              ELSE "down"]
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, Locals, DVars, GVars, XVars>>

\* Proposed (new uuid): replace the terminal mount body by If-Match with a clean terminal body of our uuid,
\* same writer_epoch.  A token mismatch refuses (retryable).
JoinRepl(p) ==
    /\ pc[p] = "jrepl"
    /\ IF mGen = jRepl[p].tok /\ mount.e > 0
       THEN /\ mount' = [mount EXCEPT !.u = Uuid(p), !.holder = "none", !.clean = TRUE, !.fenced = FALSE]
            /\ mGen' = mGen + 1
            /\ pc' = [pc EXCEPT ![p] = IF JoinOwnerFirst THEN "aread" ELSE "jwrite"]
       ELSE /\ UNCHANGED <<mount, mGen>> /\ pc' = [pc EXCEPT ![p] = "down"]
    /\ UNCHANGED <<EpVars, ownerRetired, manifests, refs, inflight, ambUsed, nonceCtr, Ghost, Locals,
                   DVars, GVars, XVars>>

JoinWrite(p) ==
    /\ pc[p] = "jwrite"
    /\ IF OTok = jTok[p] /\ ~ownerGone
       THEN /\ ownerRetired' = FALSE /\ ownerUuid' = Uuid(p) /\ ownerGen' = ownerGen + 1 /\ oStamp' = 0
            /\ life' = life + 1
            /\ pc' = [pc EXCEPT ![p] = IF JoinOwnerFirst /\ jRepl[p].need THEN "jrepl" ELSE "aread"]
            /\ stLife' = IF RemountOn /\ ~remnt[p] THEN [stLife EXCEPT ![p] = life + 1] ELSE stLife
            /\ pLife' = IF OwnerLife THEN [pLife EXCEPT ![p] = ownerGen + 1] ELSE pLife
       ELSE /\ UNCHANGED <<ownerRetired, ownerUuid, ownerGen, oStamp, life, stLife, pLife>>
            /\ pc' = [pc EXCEPT ![p] = IF remnt[p] THEN "down" ELSE "owner"]
    /\ UNCHANGED <<EpVars, mount, mGen, manifests, refs, inflight, ambUsed, nonceCtr,
                   allocLog, dup, slotMax, allocLife, G5, remnt, resumedStale, dClaimEp, catRows, jRepl, mfEp,
                   Locals, DVars, GVars>>
    /\ UNCHANGED <<clock, backUsed, hwm, hwmGen, admitUsed, myTime, myHwm, dHwm, myFloor, ownerFloor, ownerGone, dOwnTok, dTombTok, rpc, rTok, oInfl,
                   rv, rvMTok, rvOTok, tombOverLive, dTombRefused, zDrain, dLife, staleDecom, staleDrain>>

\* claimOwnerOrThrow, owner absent: serverRootSubtreeEmpty (LIST manifests; catalog folded into refs).
OwnerEmpty(p) ==
    /\ pc[p] = "oempty"
    /\ pc' = [pc EXCEPT ![p] = IF manifests = {} /\ refs = {} THEN "ocreate" ELSE Down(p)]
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, Locals, DVars, GVars, XVars>>

\* claimOwnerOrThrow: op.create(owner) If-None-Match, no floor.  A conflict fails closed here; the code
\* re-checks emptiness and accepts a same-uuid unretired owner (an under-approximation of one path).
OwnerCreate(p) ==
    /\ pc[p] = "ocreate"
    /\ IF ownerGone
       THEN /\ ownerGone' = FALSE /\ ownerGen' = ownerGen + 1 /\ ownerFloor' = 0
            /\ pc' = [pc EXCEPT ![p] = "aread"]
            /\ pLife' = IF OwnerLife THEN [pLife EXCEPT ![p] = ownerGen + 1] ELSE pLife
       ELSE /\ pc' = [pc EXCEPT ![p] = Down(p)]
            /\ UNCHANGED <<ownerGone, ownerGen, ownerFloor, pLife>>
    /\ myFloor' = [myFloor EXCEPT ![p] = 0]
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, allocLog, dup, slotMax, allocLife, G5,
                   stLife, remnt, resumedStale, dClaimEp, catRows, jRepl, mfEp, Locals, DVars, GVars>>
    /\ UNCHANGED <<clock, backUsed, hwm, hwmGen, admitUsed, myTime, myHwm, dHwm,
                   dOwnTok, dTombTok, life, rpc, rTok, oInfl, KVars>>

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
       ELSE /\ pc' = [pc EXCEPT ![p] = IF p = "d" /\ AdminRefusesAbsent THEN "dstop" ELSE "aempty"]
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
    /\ LET recover == mount.e > 0 /\ p = "d" /\ (mount.clean \/ mount.fenced \/ ProvenDead(mount.holder, mount.e))
           lower == IF mount.e = 0 THEN 1 ELSE mount.e + 1
       IN IF mount.e = 0 \/ recover
          THEN IF FreshSample
               THEN /\ myEpoch' = [myEpoch EXCEPT ![p] = lower]
                    /\ pc' = [pc EXCEPT ![p] = "abase"]
                    /\ UNCHANGED <<myBody, myExp, myAmb, nonceCtr>>
               ELSE Decide(p, Max2(lower, Base(myTime[p], StaleBound(p))))
          ELSE /\ pc' = [pc EXCEPT ![p] = Down(p)]
               /\ UNCHANGED <<myEpoch, myBody, myExp, myAmb, nonceCtr>>
    /\ UNCHANGED <<Store, inflight, ambUsed, Ghost, myMTok, starts, mf, DVars, GVars, XVars>>

\* V1: PUT `_probe/<uid>/time` + HEAD for its Last-Modified (fused: the key is private to this process,
\* so only the clock can move between the two).  V2: GET `_pool_meta`.  Owner floor: GET `owner`;
\* absent or tombstoned -> refuse.
ABase(p) ==
    /\ pc[p] = "abase"
    /\ IF OwnerFloor /\ (ownerGone \/ (ownerRetired /\ ~FloorIgnoresTomb /\ ~AcceptsTomb(p)))
       THEN /\ pc' = [pc EXCEPT ![p] = Down(p)]
            /\ UNCHANGED <<myEpoch, myBody, myExp, myAmb, nonceCtr>>
       ELSE Decide(p, Max2(myEpoch[p], Base(clock, IF OwnerFloor THEN ownerFloor ELSE hwm)))
    /\ UNCHANGED <<Store, inflight, ambUsed, Ghost, myMTok, starts, mf, DVars, GVars, XVars>>

Allocated(p) ==
    /\ allocLog' = allocLog \cup {myEpoch[p]}
    /\ allocLife' = allocLife \cup {<<myEpoch[p], life>>}
    /\ dup' = (dup \/ myEpoch[p] \in allocLog)
    /\ UNCHANGED <<slotMax, G5, G7>>
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
       \/ mount.clean \/ mount.fenced \/ ProvenDead(mount.holder, mount.e)
       \/ mount.u # Uuid(p)
    /\ IF mount.e = 0
       THEN /\ pc' = [pc EXCEPT ![p] = IF ClaimChecksOwner THEN "cownm" ELSE MintNext]
            /\ UNCHANGED myMTok
       ELSE IF mount.u # Uuid(p) /\ ~ForeignOk(p)
       THEN /\ pc' = [pc EXCEPT ![p] = Down(p)] /\ UNCHANGED myMTok          \* ForeignOwner
       ELSE IF mount.e > myEpoch[p]
       THEN /\ pc' = [pc EXCEPT ![p] = Down(p)] /\ UNCHANGED myMTok          \* newer_epoch -> ABORTED
       ELSE IF mount.e = myEpoch[p] /\ StrictClaim
       THEN /\ pc' = [pc EXCEPT ![p] = Down(p)] /\ UNCHANGED myMTok          \* same_epoch_occupied
       ELSE /\ pc' = [pc EXCEPT ![p] = IF ForeignNeedsOwner /\ mount.u # Uuid(p) THEN "cfown"
                                       ELSE IF ClaimChecksOwner THEN "cownr" ELSE "crecl"] \* reclaim
            /\ myMTok' = [myMTok EXCEPT ![p] = mGen]
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, myEpoch, myBody, myExp, myAmb, starts, mf,
                   DVars, GVars>>
    /\ UNCHANGED XVars

\* Proposed: claimMount GETs `owner` after its mount GET and before its write; a tombstone or a
\* foreign uuid refuses.  The decommission tool accepts its own tombstone.
ClaimCheckOwner(p) ==
    /\ pc[p] \in {"cownm", "cownr"}
    /\ pc' = [pc EXCEPT ![p] = IF ownerGone \/ ownerUuid # Uuid(p) \/ (ownerRetired /\ ~AcceptsTomb(p))
                              THEN Down(p)
                              ELSE IF pc[p] = "cownm" THEN MintNext ELSE "crecl"]
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, Locals, DVars, GVars, XVars>>

\* Proposed (mountWritable, tryRemountOnce): GET `owner` after the claim landed.
ClaimPostOwner(p) ==
    /\ pc[p] = "cpost"
    /\ LET refuse == IF RecheckRetiredOnly THEN ownerRetired
                     ELSE ownerGone \/ ownerUuid # Uuid(p) \/ (ownerRetired /\ ~AcceptsTomb(p))
                          \/ (~RecheckIgnoresLife /\ ~LifeOk(p)) IN
       /\ pc' = [pc EXCEPT ![p] = IF refuse THEN "cpfare" ELSE Hold(p)]
       /\ resumedStale' = (resumedStale \/ (~refuse /\ ResumesStale(p)))
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, allocLog, dup, slotMax, allocLife, G5,
                   stLife, remnt, dClaimEp, catRows, jRepl, pLife, mfEp, Locals, DVars, GVars, XVars>>

\* The refused claimer writes a terminal body (If-Match on its own claim) and throws.
ClaimPostFarewell(p) ==
    /\ pc[p] = "cpfare"
    /\ IF mGen = myMTok[p] /\ mount.e > 0
       THEN /\ mount' = IF RefusedFarewellFences THEN [mount EXCEPT !.fenced = TRUE]
                                                ELSE [mount EXCEPT !.clean = TRUE]
            /\ mGen' = mGen + 1
            /\ selfFenced' = (selfFenced \/ (Rejoin /\ RefusedFarewellFences))
       ELSE UNCHANGED <<mount, mGen, selfFenced>>
    /\ pc' = [pc EXCEPT ![p] = Down(p)]
    /\ UNCHANGED <<EpVars, ownerRetired, manifests, refs, inflight, ambUsed, nonceCtr,
                   allocLog, G7, dup, slotMax, allocLife, lateDel, doneLives, tombForeign, vTok, jTok, Locals,
                   DVars, GVars, XVars>>

\* GC fences the decommission tool's lease (gc_fenced); the tool keeps running until it notices.
GcFence ==
    /\ GcFences /\ mount.holder = "d" /\ mount.e > 0 /\ ~mount.clean /\ ~mount.fenced
    /\ pc["d"] \in HoldStates
    /\ mount' = [mount EXCEPT !.fenced = TRUE] /\ mGen' = mGen + 1
    /\ UNCHANGED <<EpVars, ownerRetired, manifests, refs, inflight, ambUsed, nonceCtr, Ghost, pc, Locals,
                   DVars, GVars, XVars>>

\* GC fences the expired lease of a holder that is gone (any uuid).
GcFenceDead ==
    /\ GcFencesDead /\ mount.e > 0 /\ ~mount.clean /\ ~mount.fenced /\ mount.holder # "none"
    /\ ProvenDead(mount.holder, mount.e)
    /\ mount' = [mount EXCEPT !.fenced = TRUE] /\ mGen' = mGen + 1
    /\ UNCHANGED <<EpVars, ownerRetired, manifests, refs, inflight, ambUsed, nonceCtr, Ghost, pc, Locals,
                   DVars, GVars, XVars>>

\* Self-remount (Pool::tryRemountOnce) of a process whose lease is lost: a live server instance, or the
\* decommission tool before its farewell.  The in-memory state is kept.
RemountStart(p) ==
    /\ RemountOn /\ p \notin OldInst /\ ~remnt[p]
    /\ IF p = "d" THEN pc[p] \in {"dlive", "dtomb", "dchk", "ddrain"} ELSE pc[p] = "live"
    /\ ~(mount.holder = p /\ mount.e = myEpoch[p] /\ ~mount.fenced)
    /\ pc' = [pc EXCEPT ![p] = "rmowner"]
    /\ remnt' = [remnt EXCEPT ![p] = TRUE]
    \* A build admitted under the lost incarnation cannot commit after the remount (admission generation).
    /\ mf' = [mf EXCEPT ![p] = IF mf[p] = "written" THEN "failed" ELSE mf[p]]
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, allocLog, dup, slotMax, allocLife, G5,
                   stLife, resumedStale, dClaimEp, catRows, jRepl, pLife, mfEp,
                   myEpoch, myBody, myExp, myAmb, myMTok, starts, DVars, GVars, XVars>>

\* claimOwnerOrThrow from the remount: no NoWait, no rejoin (sabotage: rejoin).  Then a new epoch and claim.
RemountOwner(p) ==
    /\ pc[p] = "rmowner"
    /\ LET fail == IF p = "d" /\ AdminRemountConfirms /\ dTombTok # <<>> THEN "dchk" ELSE Down(p) IN
       pc' = [pc EXCEPT ![p] = IF ownerGone \/ ownerUuid # Uuid(p) THEN fail
                              ELSE IF ~LifeOk(p) THEN fail
                              ELSE IF ownerRetired THEN fail
                              ELSE "aread"]
    /\ myEpoch' = [myEpoch EXCEPT ![p] = 0]
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, allocLog, dup, slotMax, allocLife,
                   lateDel, doneLives, tombForeign, vTok, jTok, selfFenced, G7,
                   myBody, myExp, myAmb, myMTok, starts, mf, DVars, GVars, XVars>>

\* GC fences the lease of a partitioned server instance that is still running.
GcFenceLive ==
    /\ GcFencesLive /\ mount.e > 0 /\ ~mount.clean /\ ~mount.fenced /\ mount.holder \in Inst \ OldInst
    /\ mount' = [mount EXCEPT !.fenced = TRUE] /\ mGen' = mGen + 1
    /\ UNCHANGED <<EpVars, ownerRetired, manifests, refs, inflight, ambUsed, nonceCtr, Ghost, pc, Locals,
                   DVars, GVars, XVars>>

\* GC deletes the victim's Removing catalog rows.
CatalogGc ==
    /\ catRows
    /\ catRows' = FALSE
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, allocLog, dup, slotMax, allocLife, G5,
                   stLife, remnt, resumedStale, dClaimEp, jRepl, pLife, mfEp, pc, Locals, DVars, GVars, XVars>>

\* Proposed: before reclaiming a foreign terminal body, GET owner; it must carry our uuid and be live.
ClaimForeignOwner(p) ==
    /\ pc[p] = "cfown"
    /\ pc' = [pc EXCEPT ![p] = IF ~ownerGone /\ ownerUuid = Uuid(p) /\ ~ownerRetired /\ LifeOk(p)
                              THEN "crecl" ELSE Down(p)]
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, Locals, DVars, GVars, XVars>>

\* Proposed check before the mint: GET `epoch`; our own bytes must still be there.
ClaimCheckCounter(p) ==
    /\ pc[p] = "cchk"
    /\ pc' = [pc EXCEPT ![p] = IF epObj = myBody[p] THEN "cmint" ELSE Down(p)]
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, Locals, DVars, GVars, XVars>>

\* op.create(mountKey): If-None-Match; a race is racedDoubleStart.
ClaimMint(p) ==
    /\ pc[p] = "cmint"
    /\ IF mount.e = 0
       THEN /\ mount' = [e |-> myEpoch[p], clean |-> FALSE, holder |-> p, u |-> Uuid(p), fenced |-> FALSE]
            /\ mGen' = mGen + 1
            /\ myMTok' = [myMTok EXCEPT ![p] = mGen + 1]
            /\ pc' = [pc EXCEPT ![p] = AfterClaim(p)]
            /\ slotMax' = IF myEpoch[p] > slotMax THEN myEpoch[p] ELSE slotMax
            /\ resumedStale' = (resumedStale \/ (AfterClaim(p) # "cpost" /\ ResumesStale(p)))
            /\ dClaimEp' = IF RemountOn /\ p = "d" /\ dClaimEp = 0 THEN myEpoch[p] ELSE dClaimEp
       ELSE /\ pc' = [pc EXCEPT ![p] = Down(p)] /\ UNCHANGED <<mount, mGen, myMTok, slotMax, resumedStale, dClaimEp>>
    /\ UNCHANGED <<EpVars, ownerRetired, manifests, refs, inflight, ambUsed, nonceCtr, allocLog, dup,
                   allocLife, G5, stLife, remnt, catRows, jRepl, pLife, mfEp, myEpoch, myBody, myExp, myAmb, starts, mf,
                   DVars, GVars>>
    /\ UNCHANGED XVars

\* op.replace(mountKey, body, got->etag): If-Match on the token ClaimRead saw.
ClaimReclaim(p) ==
    /\ pc[p] = "crecl"
    /\ IF mount.e > 0 /\ mGen = myMTok[p]
       THEN /\ mount' = [e |-> myEpoch[p], clean |-> FALSE, holder |-> p, u |-> Uuid(p), fenced |-> FALSE]
            /\ mGen' = mGen + 1
            /\ myMTok' = [myMTok EXCEPT ![p] = mGen + 1]
            /\ pc' = [pc EXCEPT ![p] = AfterClaim(p)]
            /\ slotMax' = IF myEpoch[p] > slotMax THEN myEpoch[p] ELSE slotMax
            /\ resumedStale' = (resumedStale \/ (AfterClaim(p) # "cpost" /\ ResumesStale(p)))
            /\ dClaimEp' = IF RemountOn /\ p = "d" /\ dClaimEp = 0 THEN myEpoch[p] ELSE dClaimEp
       ELSE /\ pc' = [pc EXCEPT ![p] = Down(p)] /\ UNCHANGED <<mount, mGen, myMTok, slotMax, resumedStale, dClaimEp>>
    /\ UNCHANGED <<EpVars, ownerRetired, manifests, refs, inflight, ambUsed, nonceCtr, allocLog, dup,
                   allocLife, G5, stLife, remnt, catRows, jRepl, pLife, mfEp, myEpoch, myBody, myExp, myAmb, starts, mf,
                   DVars, GVars>>
    /\ UNCHANGED XVars

-----------------------------------------------------------------------------
(* A live writer                                                           *)

\* Manifest create (If-None-Match) of (ns, epoch, build_seq 1): in flight, protected only by the floor.
WriteManifest(p) ==
    /\ pc[p] = "live" /\ mf[p] = "none" /\ (RemountOn => OwnLease(p))
    /\ IF myEpoch[p] \notin manifests
       THEN manifests' = manifests \cup {myEpoch[p]} /\ mf' = [mf EXCEPT ![p] = "written"]
       ELSE UNCHANGED manifests /\ mf' = [mf EXCEPT ![p] = "failed"]
    \* The manifest key carries the epoch at staging; a later remount does not change it.
    /\ mfEp' = IF RemountOn THEN [mfEp EXCEPT ![p] = myEpoch[p]] ELSE mfEp
    /\ UNCHANGED <<EpVars, mount, mGen, ownerRetired, refs, inflight, ambUsed, nonceCtr, pc,
                   allocLog, dup, slotMax, allocLife, G5, stLife, remnt, resumedStale, dClaimEp, catRows, jRepl, pLife,
                   myEpoch, myBody, myExp, myAmb, myMTok, starts, DVars, GVars>>
    /\ UNCHANGED XVars

\* Ref commit naming the manifest; it does not re-read the manifest.
Publish(p) ==
    /\ pc[p] = "live" /\ mf[p] = "written" /\ (RemountOn => OwnLease(p))
    /\ refs' = refs \cup {IF RemountOn THEN mfEp[p] ELSE myEpoch[p]}
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
    /\ pc' = [pc EXCEPT ![p] = IF p # "d" THEN "down"
                              ELSE IF ~KeepAll THEN "dreadm"
                              ELSE IF DecomDeletesEpoch THEN "ddele"
                              ELSE IF TombLast THEN "downer" ELSE IF VerifyRetired THEN "dverm" ELSE "ddone"]
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
    /\ pc["d"] = IF KeepAll /\ ~TombLast THEN "ddrain" ELSE "dlive"
    /\ LET next == IF KeepAll /\ ~DecomDeletesEpoch THEN "dfarewell" ELSE "dcap" IN
       IF DrainFenced /\ ~OwnClaim(myMTok["d"])
       THEN \* The lease fence refuses the request locally; the run fails.
            /\ pc' = [pc EXCEPT !["d"] = "dstop"]
            /\ UNCHANGED <<refs, staleDrain, lateDel, catRows>>
       ELSE \/ /\ refs' = {}
               /\ staleDrain' = (staleDrain \/ \E e \in refs : e > myEpoch["d"])
               /\ pc' = [pc EXCEPT !["d"] = next]
               /\ catRows' = (catRows \/ DrainLeavesRows)
               /\ UNCHANGED lateDel
            \* Sent under the lease, response lost: the delete lands later (LateDelete).
            \/ /\ DrainInFlight /\ ~lateDel.on
               /\ lateDel' = [on |-> TRUE, ep |-> myEpoch["d"]]
               /\ pc' = [pc EXCEPT !["d"] = next]
               /\ UNCHANGED <<refs, staleDrain, catRows>>
    /\ UNCHANGED <<EpVars, mount, mGen, ownerRetired, manifests, inflight, ambUsed, nonceCtr,
                   allocLog, dup, slotMax, allocLife, doneLives, tombForeign, vTok, jTok, selfFenced,
                   stLife, remnt, resumedStale, dClaimEp, jRepl, pLife, mfEp, Locals, DVars, GVars>>
    /\ UNCHANGED <<clock, backUsed, hwm, hwmGen, admitUsed, myTime, myHwm, dHwm, myFloor, ownerFloor, ownerGen, ownerGone, dOwnTok, dTombTok, life, rpc, rTok, oInfl,
                   ownerUuid, rv, rvMTok, rvOTok, tombOverLive, dTombRefused, oStamp, zDrain, dLife, staleDecom>>

\* Store side of a drain request sent before the lease was lost.
LateDelete ==
    /\ lateDel.on
    /\ refs' = {}
    /\ staleDrain' = (staleDrain \/ \E e \in refs : e > lateDel.ep)
    /\ lateDel' = [lateDel EXCEPT !.on = FALSE]
    /\ UNCHANGED <<EpVars, mount, mGen, ownerRetired, manifests, inflight, ambUsed, nonceCtr,
                   allocLog, G7, dup, slotMax, allocLife, doneLives, tombForeign, vTok, jTok, selfFenced, pc, Locals, DVars, GVars>>
    /\ UNCHANGED <<clock, backUsed, hwm, hwmGen, admitUsed, myTime, myHwm, dHwm, myFloor, ownerFloor, ownerGen, ownerGone, dOwnTok, dTombTok, life, rpc, rTok, oInfl,
                   ownerUuid, rv, rvMTok, rvOTok, tombOverLive, dTombRefused, oStamp, zDrain, dLife, staleDecom>>

\* Proposed verify: GET mount; the tool's own clean farewell.
DVerifyMount ==
    /\ pc["d"] = "dverm"
    /\ pc' = [pc EXCEPT !["d"] = IF mount.e = myEpoch["d"] /\ mount.u = Uuid("d") /\ mount.clean
                                 THEN "dvero" ELSE "dstop"]
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, Locals, DVars, GVars, XVars>>

\* Proposed verify: GET owner; still retired and, when this run wrote one, its own tombstone.
DVerifyOwner ==
    /\ pc["d"] = "dvero"
    /\ IF ownerRetired /\ ~ownerGone /\ (dTombTok = <<>> \/ OTok = dTombTok)
       THEN IF VerifiedMark
            THEN pc' = [pc EXCEPT !["d"] = "dmark"] /\ UNCHANGED doneLives
            ELSE pc' = [pc EXCEPT !["d"] = "ddone"] /\ doneLives' = doneLives \cup {life}
       ELSE pc' = [pc EXCEPT !["d"] = "dstop"] /\ UNCHANGED doneLives
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, allocLog, G7, dup, slotMax, allocLife, lateDel, tombForeign, vTok, jTok,
                   selfFenced, Locals, DVars, GVars, XVars>>

\* Proposed: PUT owner If-Match on this run's tombstone, same tombstone marked verified (fresh nonce).
DMarkVerified ==
    /\ pc["d"] = "dmark"
    /\ IF OTok = dTombTok /\ ~ownerGone
       THEN /\ ownerGen' = ownerGen + 1
            /\ vTok' = OTokOf(ownerUuid, ownerRetired, ownerFloor, oStamp, ownerGen + 1)
            /\ pc' = [pc EXCEPT !["d"] = "ddone"] /\ doneLives' = doneLives \cup {life}
       ELSE /\ pc' = [pc EXCEPT !["d"] = "dstop"] /\ UNCHANGED <<ownerGen, vTok, doneLives>>
    /\ UNCHANGED <<EpVars, mount, mGen, ownerRetired, manifests, refs, inflight, ambUsed, nonceCtr,
                   allocLog, G7, dup, slotMax, allocLife, lateDel, tombForeign, jTok, selfFenced, Locals, DVars, GVars>>
    /\ UNCHANGED <<clock, backUsed, hwm, hwmGen, admitUsed, myTime, myHwm, dHwm, myFloor, life, ownerFloor, ownerGone, dOwnTok, dTombTok, rpc, rTok, oInfl,
                   ownerUuid, rv, rvMTok, rvOTok, tombOverLive, dTombRefused, oStamp, zDrain, dLife, staleDecom,
                   staleDrain>>

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
       THEN dFar' = mGen /\ pc' = [pc EXCEPT !["d"] = IF Retire THEN "dretire"
                                                    ELSE IF Tail2 THEN "downer" ELSE "ddelm"]
       ELSE UNCHANGED dFar /\ pc' = [pc EXCEPT !["d"] = "dstop"]
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, Locals, dCap, dOwn, GVars>>
    /\ UNCHANGED XVars

\* A delete after the owner-floor tombstone found its token moved: a successor holds the slot.
DelFailNext == IF Tail2 /\ UntombOnDelFail THEN "duntomb" ELSE "dstop"

\* deleteSlotObject(mount_key, farewell etag).  In the fix it runs last.
DDelMount ==
    /\ pc["d"] = "ddelm"
    /\ NoZombie => ~PendingMountClaim
    /\ IF mount.e > 0 /\ mGen = dFar
       THEN /\ mount' = MAbsent /\ mGen' = mGen + 1
            /\ pc' = [pc EXCEPT !["d"] = IF Retire THEN "dstop"
                                         ELSE IF PoolHwm /\ ~HwmAfterDelete THEN "dhwmr" ELSE "ddele"]
       ELSE UNCHANGED <<mount, mGen>> /\ pc' = [pc EXCEPT !["d"] = DelFailNext]
    /\ UNCHANGED <<EpVars, ownerRetired, manifests, refs, inflight, ambUsed, nonceCtr, Ghost, Locals,
                   DVars, GVars>>
    /\ UNCHANGED XVars

\* deleteSlotObject(epoch_key, claimed_epoch->etag)  (current code only).
DDelEpoch ==
    /\ pc["d"] = "ddele"
    /\ (NoZombie \/ NoZombieCounter) => ~PendingCounterWrite
    /\ TailTakesASecond => clock > epBornAt
    /\ IF PreHolds(dCap.tok)
       THEN /\ epObj' = EAbsent /\ epGen' = IF S3 THEN epGen ELSE epGen + 1
            /\ UNCHANGED <<epBornAt, maxCounter, regress>>
            /\ pc' = [pc EXCEPT !["d"] = IF KeepAll THEN (IF TombLast THEN "downer" ELSE "ddone")
                                         ELSE IF PoolHwm /\ HwmAfterDelete THEN "dhwmr" ELSE "drechm"]
       ELSE UNCHANGED EpVars /\ pc' = [pc EXCEPT !["d"] = DelFailNext]
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
    /\ pc' = [pc EXCEPT !["d"] = IF epObj.v = 0 /\ ~Tail2 THEN "downer" ELSE "dstop"]
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
    /\ pc["d"] = IF KeepAll /\ ~TombLast THEN "dlive" ELSE "downer"
    /\ dOwn' = (ownerRetired /\ (VerifiedMark => OwnerVerified))
    /\ dOwnTok' = OTok
    /\ dTombTok' = <<>>
    /\ dLife' = life
    /\ pc' = [pc EXCEPT !["d"] = IF ownerGone \/ (TombChecksUuid /\ ownerUuid # Uuid("d")) THEN "dstop"
                                 ELSE IF KeepAll /\ ownerRetired /\ ~TombAlways
                                 THEN (IF TombLast THEN "ddone" ELSE AfterKeepTomb)
                                 ELSE "dtomb"]
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, Locals, dCap, dFar, GVars>>
    /\ UNCHANGED <<clock, backUsed, hwm, hwmGen, admitUsed, myTime, myHwm, dHwm, myFloor, life, ownerFloor, ownerGen, ownerGone, rpc, rTok, oInfl,
                   ownerUuid, rv, rvMTok, rvOTok, tombOverLive, dTombRefused, oStamp, zDrain,
                   staleDecom, staleDrain>>

\* Floor carried by the tombstone: the captured next_writer_epoch (never below a floor already there).
TombFloor == IF OwnerFloor THEN Max2(ownerFloor, dCap.obj.v) ELSE ownerFloor

\* op.replace(owner_key, tombstone, owner->etag).  The token is a generation: retired_at_ms makes every
\* tombstone new bytes, and a cleared tombstone keeps the floor, so no owner write repeats earlier bytes.
\* Disjunct 2: the response is lost and the resolve budget runs out; the tool stops ("dfail") and the
\* PUT may land later (OwnerLateLand).
DTombstone ==
    /\ pc["d"] = "dtomb"
    /\ \/ /\ IF OTok = dOwnTok /\ ~ownerGone
             THEN /\ ownerRetired' = TRUE /\ ownerFloor' = TombFloor
                  /\ ownerGen' = ownerGen + 1 /\ oStamp' = ownerGen + 1
                  /\ dTombTok' = OTokOf(ownerUuid, TRUE, TombFloor, ownerGen + 1, ownerGen + 1)
                  /\ vTok' = IF KeepVerified /\ dOwn
                             THEN OTokOf(ownerUuid, TRUE, TombFloor, ownerGen + 1, ownerGen + 1) ELSE vTok
                  /\ pc' = [pc EXCEPT !["d"] = IF Retire /\ RetireDeletesMount THEN "ddelm"
                                               ELSE IF Tail2 THEN (IF UntombOnMove THEN "dpost"
                                                                   ELSE AfterTombNext)
                                               ELSE IF KeepAll THEN (IF TombLast THEN "ddone" ELSE AfterKeepTomb)
                                               ELSE "dstop"]
                  /\ tombOverLive' = (tombOverLive \/ (KeepAll /\ LiveHolder))
                  /\ staleDecom' = (staleDecom \/ life > dLife)
                  /\ tombForeign' = (tombForeign \/ ownerUuid # Uuid("d"))
             ELSE /\ UNCHANGED <<ownerRetired, ownerFloor, ownerGen, oStamp, dTombTok, tombOverLive, staleDecom,
                                tombForeign, vTok>>
                  /\ pc' = [pc EXCEPT !["d"] = "dstop"]
          /\ UNCHANGED oInfl
       \/ /\ TombAmbig /\ ~oInfl.on
          /\ oInfl' = [on |-> TRUE, tok |-> dOwnTok, floor |-> TombFloor, life |-> dLife,
                       ep |-> myEpoch["d"], mtok |-> myMTok["d"], intro |-> ~dOwn]
          /\ pc' = [pc EXCEPT !["d"] = "dfail"]
          /\ UNCHANGED <<ownerRetired, ownerFloor, ownerGen, oStamp, dTombTok, tombOverLive, staleDecom,
                         tombForeign, vTok>>
    /\ UNCHANGED <<EpVars, mount, mGen, manifests, refs, inflight, ambUsed, nonceCtr,
                   allocLog, dup, slotMax, allocLife, lateDel, doneLives, jTok, selfFenced, G7, Locals, DVars, GVars>>
    /\ UNCHANGED <<clock, backUsed, hwm, hwmGen, admitUsed, myTime, myHwm, dHwm, myFloor, life, ownerGone, dOwnTok, rpc, rTok,
                   ownerUuid, rv, rvMTok, rvOTok, dTombRefused, zDrain, dLife, staleDrain>>

\* Store side of an unresolved tombstone PUT.
OwnerLateLand ==
    /\ oInfl.on
    /\ oInfl' = [oInfl EXCEPT !.on = FALSE]
    /\ IF OTok = oInfl.tok /\ ~ownerGone
       THEN /\ ownerRetired' = TRUE /\ ownerFloor' = oInfl.floor
            /\ ownerGen' = ownerGen + 1 /\ oStamp' = ownerGen + 1
            /\ staleDecom' = (staleDecom \/ life > oInfl.life)
            /\ tombOverLive' = (tombOverLive \/ (KeepAll /\ LiveHolder))
            /\ tombForeign' = (tombForeign \/ ownerUuid # Uuid("d"))
            /\ vTok' = IF KeepVerified /\ ~oInfl.intro
                       THEN OTokOf(ownerUuid, TRUE, oInfl.floor, ownerGen + 1, ownerGen + 1) ELSE vTok
            \* Lifecycle: the paused run goes on to its confirm (or straight to its drain).
            /\ zDrain' = IF KeepAll
                         THEN [on |-> TRUE, mode |-> IF AdminConfirmsClaim THEN "confirm" ELSE "drain",
                               ep |-> oInfl.ep, mtok |-> oInfl.mtok, intro |-> oInfl.intro,
                               tok |-> OTokOf(ownerUuid, TRUE, oInfl.floor, ownerGen + 1, ownerGen + 1)]
                         ELSE zDrain
       ELSE UNCHANGED <<ownerRetired, ownerFloor, ownerGen, oStamp, staleDecom, tombOverLive, zDrain, tombForeign, vTok>>
    /\ UNCHANGED <<EpVars, mount, mGen, manifests, refs, inflight, ambUsed, nonceCtr,
                   allocLog, dup, slotMax, allocLife, lateDel, doneLives, jTok, selfFenced, G7, pc, Locals, DVars, GVars>>
    /\ UNCHANGED <<clock, backUsed, hwm, hwmGen, admitUsed, myTime, myHwm, dHwm, myFloor, life, ownerGone, dOwnTok, dTombTok, rpc, rTok,
                   ownerUuid, rv, rvMTok, rvOTok, dTombRefused, dLife, staleDrain>>

\* A paused earlier decommission run's drain (dropNamespace, staging and roots prefix deletes).
ZombieDrain ==
    /\ zDrain.on /\ zDrain.mode = "drain"
    /\ zDrain' = [zDrain EXCEPT !.on = FALSE]
    \* DrainFenced: the paused run's lease fence refuses the request once its claim is gone.
    /\ IF DrainFenced /\ ~OwnClaim(zDrain.mtok)
       THEN UNCHANGED <<refs, staleDrain>>
       ELSE /\ refs' = {}
            /\ staleDrain' = (staleDrain \/ \E e \in refs : e > zDrain.ep)
    /\ UNCHANGED <<EpVars, mount, mGen, ownerRetired, manifests, inflight, ambUsed, nonceCtr, Ghost, pc,
                   Locals, DVars, GVars>>
    /\ UNCHANGED <<clock, backUsed, hwm, hwmGen, admitUsed, myTime, myHwm, dHwm, myFloor, life, ownerFloor, ownerGen, ownerGone, dOwnTok, dTombTok, rpc, rTok, oInfl,
                   ownerUuid, rv, rvMTok, rvOTok, tombOverLive, dTombRefused, oStamp, dLife, staleDecom>>

\* The paused run's claim confirmation (GET mount): its own token -> drain, otherwise un-tombstone.
ZombieConfirm ==
    /\ zDrain.on /\ zDrain.mode = "confirm"
    /\ zDrain' = IF mGen = zDrain.mtok THEN [zDrain EXCEPT !.mode = "drain"]
                 ELSE IF RollbackOnlyIfIntroduced /\ ~zDrain.intro THEN [zDrain EXCEPT !.on = FALSE]
                 ELSE [zDrain EXCEPT !.mode = "untomb"]
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, pc, Locals, DVars, GVars>>
    /\ UNCHANGED <<clock, backUsed, hwm, hwmGen, admitUsed, myTime, myHwm, dHwm, myFloor, life, ownerFloor, ownerGen, ownerGone, dOwnTok, dTombTok, rpc, rTok, oInfl,
                   ownerUuid, rv, rvMTok, rvOTok, tombOverLive, dTombRefused, oStamp, dLife, staleDecom,
                   staleDrain>>

ZombieUntomb ==
    /\ zDrain.on /\ zDrain.mode = "untomb"
    /\ zDrain' = [zDrain EXCEPT !.on = FALSE]
    /\ IF (IF RollbackIgnoresLife THEN ownerRetired /\ ownerUuid = Uuid("d") ELSE OTok = zDrain.tok)
          /\ ~ownerGone /\ ~(RollbackFenced /\ ~OwnClaim(zDrain.mtok))
       THEN ownerRetired' = FALSE /\ ownerGen' = ownerGen + 1 /\ oStamp' = 0
       ELSE UNCHANGED <<ownerRetired, ownerGen, oStamp>>
    /\ UNCHANGED <<EpVars, mount, mGen, manifests, refs, inflight, ambUsed, nonceCtr, Ghost, pc, Locals,
                   DVars, GVars>>
    /\ UNCHANGED <<clock, backUsed, hwm, hwmGen, admitUsed, myTime, myHwm, dHwm, myFloor, life, ownerFloor, ownerGone, dOwnTok, dTombTok, rpc, rTok, oInfl,
                   ownerUuid, rv, rvMTok, rvOTok, tombOverLive, dTombRefused, dLife, staleDecom, staleDrain>>

\* Proposed: GET `mount` after the tombstone; the tool's own claim token must still be there.
DConfirmClaim ==
    /\ pc["d"] = "dchk"
    /\ pc' = [pc EXCEPT !["d"] = IF (IF RemountOn
                                     THEN mount.holder = "d" /\ mount.e = dClaimEp /\ ~mount.fenced /\ ~mount.clean
                                     ELSE mGen = myMTok["d"] /\ mount.holder = "d")
                                 THEN "ddrain"
                                 ELSE IF RollbackOnlyIfIntroduced /\ dOwn THEN "dstop" ELSE "duntomb"]
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, Locals, DVars, GVars, XVars>>

\* Proposed S11 check: GET `epoch` after the tombstone; a successor moved it -> un-tombstone.
DPostTombCheck ==
    /\ pc["d"] = "dpost"
    /\ pc' = [pc EXCEPT !["d"] = IF epObj.v > 0 /\ Tok = dCap.tok THEN AfterTombNext ELSE "duntomb"]
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, Locals, DVars, GVars, XVars>>

\* Proposed: op.replace(owner_key, owner without retired_at_ms, floor kept, our tombstone etag).
DUntomb ==
    /\ pc["d"] = "duntomb"
    /\ IF (IF RollbackIgnoresLife THEN ownerRetired /\ ownerUuid = Uuid("d") ELSE OTok = dTombTok)
          /\ ~ownerGone /\ ~(RollbackFenced /\ ~OwnClaim(myMTok["d"]))
       THEN ownerRetired' = FALSE /\ ownerGen' = ownerGen + 1 /\ oStamp' = 0
       ELSE UNCHANGED <<ownerRetired, ownerGen, oStamp>>
    /\ pc' = [pc EXCEPT !["d"] = "dstop"]
    /\ UNCHANGED <<EpVars, mount, mGen, manifests, refs, inflight, ambUsed, nonceCtr, Ghost, Locals,
                   DVars, GVars>>
    /\ UNCHANGED <<clock, backUsed, hwm, hwmGen, admitUsed, myTime, myHwm, dHwm, myFloor, life, ownerFloor, ownerGone, dOwnTok, dTombTok, rpc, rTok, oInfl,
                   ownerUuid, rv, rvMTok, rvOTok, tombOverLive, dTombRefused, zDrain, dLife, staleDecom, staleDrain>>

\* The tool dies anywhere after it opened and is re-run once from Pool::openForDecommission.
DRestart ==
    /\ DecomRetry /\ starts["d"] < DRestarts
    /\ pc["d"] # "open"
    /\ pc' = [pc EXCEPT !["d"] = "open"]
    /\ starts' = [starts EXCEPT !["d"] = @ + 1]
    /\ remnt' = [remnt EXCEPT !["d"] = FALSE]
    /\ dClaimEp' = 0
    /\ dTombTok' = IF RemountOn THEN <<>> ELSE dTombTok
    /\ myEpoch' = [myEpoch EXCEPT !["d"] = 0]
    /\ dCap' = [obj |-> EAbsent, tok |-> NONE] /\ dFar' = 0 /\ dOwn' = FALSE
    \* ZombieRun: the earlier run is paused, not dead; its decided tombstone and next steps land later.
    /\ oInfl' = IF ZombieRun /\ pc["d"] = "dtomb" /\ ~oInfl.on
                THEN [on |-> TRUE, tok |-> dOwnTok, floor |-> TombFloor, life |-> dLife,
                      ep |-> myEpoch["d"], mtok |-> myMTok["d"], intro |-> ~dOwn]
                ELSE oInfl
    /\ zDrain' = IF ZombieRun /\ pc["d"] \in {"dchk", "ddrain"}
                 THEN [on |-> TRUE, mode |-> IF pc["d"] = "dchk" THEN "confirm" ELSE "drain",
                       ep |-> myEpoch["d"], mtok |-> myMTok["d"], tok |-> dTombTok, intro |-> ~dOwn]
                 ELSE zDrain
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, allocLog, dup, slotMax, allocLife, G5,
                   stLife, resumedStale, catRows, jRepl, pLife, mfEp, myBody, myExp, myAmb, myMTok, mf, GVars>>
    /\ UNCHANGED <<clock, backUsed, hwm, hwmGen, admitUsed, myTime, myHwm, dHwm, myFloor, ownerFloor, ownerGen, ownerGone, dOwnTok, life, rpc, rTok,
                   ownerUuid, rv, rvMTok, rvOTok, tombOverLive, dTombRefused, oStamp, dLife, staleDecom,
                   staleDrain>>

\* Operator revival: GET owner, then If-Match write without retired_at_ms (floor kept).  Hostile
\* variant: an unconditional DELETE of `owner`.  Either opens a new life (ghost).
RRead ==
    /\ Revive /\ rpc = "ridle" /\ ownerRetired /\ ~ownerGone
    /\ rTok' = OTok
    /\ rpc' = IF ReviveDeletes THEN "rdel" ELSE "rwrite"
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, pc, Locals, DVars, GVars>>
    /\ UNCHANGED <<clock, backUsed, hwm, hwmGen, admitUsed, myTime, myHwm, dHwm, myFloor, life, ownerFloor, ownerGen, ownerGone, dOwnTok, dTombTok, oInfl, KVars>>

RWrite ==
    /\ rpc = "rwrite"
    /\ IF OTok = rTok /\ ~ownerGone
       THEN /\ ownerRetired' = FALSE /\ ownerGen' = ownerGen + 1 /\ oStamp' = 0 /\ life' = life + 1
            /\ rpc' = "rdone"
       ELSE /\ UNCHANGED <<ownerRetired, ownerGen, oStamp, life>> /\ rpc' = "ridle"
    /\ UNCHANGED <<EpVars, mount, mGen, manifests, refs, inflight, ambUsed, nonceCtr, Ghost, pc, Locals,
                   DVars, GVars>>
    /\ UNCHANGED <<clock, backUsed, hwm, hwmGen, admitUsed, myTime, myHwm, dHwm, myFloor, ownerFloor, ownerGone, dOwnTok, dTombTok, rTok, oInfl,
                   ownerUuid, rv, rvMTok, rvOTok, tombOverLive, dTombRefused, zDrain, dLife, staleDecom, staleDrain>>

RDelete ==
    /\ rpc = "rdel"
    /\ ownerGone' = TRUE /\ ownerRetired' = FALSE /\ ownerFloor' = 0 /\ ownerGen' = ownerGen + 1
    /\ oStamp' = 0 /\ life' = life + 1 /\ rpc' = "rdone"
    /\ UNCHANGED <<EpVars, mount, mGen, manifests, refs, inflight, ambUsed, nonceCtr, Ghost, pc, Locals,
                   DVars, GVars>>
    /\ UNCHANGED <<clock, backUsed, hwm, hwmGen, admitUsed, myTime, myHwm, dHwm, myFloor, dOwnTok, dTombTok, rTok, oInfl,
                   ownerUuid, rv, rvMTok, rvOTok, tombOverLive, dTombRefused, zDrain, dLife, staleDecom, staleDrain>>

\* V2: `_pool_meta` read-modify-write (CasOperation::readModifyWrite, as admitOrValidate does):
\* GET, then If-Match write of max(hwm, captured next_writer_epoch); a conflict re-reads.
DHwmRead ==
    /\ pc["d"] = "dhwmr"
    /\ dHwm' = [v |-> hwm, tok |-> hwmGen]
    /\ pc' = [pc EXCEPT !["d"] = "dhwmw"]
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, Locals, DVars, GVars,
                   clock, backUsed, hwm, hwmGen, admitUsed, myTime, myHwm, NVars>>

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
                   clock, backUsed, admitUsed, myTime, myHwm, dHwm, NVars>>

\* A concurrent open's PoolMeta admitOrValidate write (rare: a new hash algorithm admitted).
\* A 26.6.4 binary re-encodes the decoded struct, dropping the unknown hwm field.
PoolMetaAdmit ==
    /\ PoolHwm /\ ~admitUsed
    /\ admitUsed' = TRUE
    /\ hwmGen' = hwmGen + 1
    /\ hwm' = IF OldAdmitDropsHwm THEN 0 ELSE hwm
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, pc, Locals, DVars, GVars,
                   clock, backUsed, myTime, myHwm, dHwm, NVars>>

\* Environment: store time advances; optionally steps back once.
Tick ==
    /\ StoreTime /\ clock < MaxClock
    /\ clock' = clock + 1
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, pc, Locals, DVars, GVars,
                   backUsed, hwm, hwmGen, admitUsed, myTime, myHwm, dHwm, NVars>>

ClockStepBack ==
    /\ StoreTime /\ ClockBack /\ ~backUsed /\ clock > 0
    /\ clock' = clock - 1 /\ backUsed' = TRUE
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, pc, Locals, DVars, GVars,
                   hwm, hwmGen, admitUsed, myTime, myHwm, dHwm, NVars>>

-----------------------------------------------------------------------------
(* Keep-everything revival: proposed cas-readd-member                      *)

\* Precondition: GET `owner` is tombstoned.
RvStart(r) ==
    /\ KeepAll /\ rv[r] = "idle" /\ ownerRetired /\ ~ownerGone
    /\ rv' = [rv EXCEPT ![r] = IF ReaddOneWrite THEN "repoch" ELSE "rempty"]
    /\ rvOTok' = IF ReaddOneWrite THEN [rvOTok EXCEPT ![r] = OTok] ELSE rvOTok
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, pc, Locals, DVars, GVars>>
    /\ UNCHANGED <<clock, backUsed, hwm, hwmGen, admitUsed, myTime, myHwm, dHwm, myFloor, NVarsNoRv, rvMTok>>

\* ReaddOneWrite: GET epoch, it must exist.
RvReadEpoch(r) ==
    /\ rv[r] = "repoch"
    /\ rv' = [rv EXCEPT ![r] = IF epObj.v > 0 THEN "rmread" ELSE "rstop"]
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, pc, Locals, DVars, GVars>>
    /\ UNCHANGED <<clock, backUsed, hwm, hwmGen, admitUsed, myTime, myHwm, dHwm, myFloor, NVarsNoRv, rvMTok, rvOTok>>

\* serverRootSubtreeEmpty (LIST; catalog folded into refs).
RvEmpty(r) ==
    /\ rv[r] = "rempty"
    /\ rv' = [rv EXCEPT ![r] = IF manifests = {} /\ refs = {}
                              THEN (IF ReaddOneWrite THEN "rowrite" ELSE IF ReviveOwnerFirst THEN "roread" ELSE "rmread")
                              ELSE "rstop"]
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, pc, Locals, DVars, GVars>>
    /\ UNCHANGED <<clock, backUsed, hwm, hwmGen, admitUsed, myTime, myHwm, dHwm, myFloor, NVarsNoRv, rvMTok, rvOTok>>

\* GET `mount`: absent = an earlier run removed it; only a clean farewell may be removed.
RvReadMount(r) ==
    /\ rv[r] = "rmread"
    /\ rvMTok' = [rvMTok EXCEPT ![r] = mGen]
    /\ rv' = [rv EXCEPT ![r] = IF ReaddOneWrite
                              THEN (IF ReaddSkipsClean \/ (mount.e > 0 /\ (mount.clean \/ (ReaddAnyTerminal /\ mount.fenced)))
                                    THEN "rempty" ELSE "rstop")
                              ELSE IF mount.e = 0 THEN RvAfterMount
                              ELSE IF ~(mount.clean \/ (mount.fenced /\ ReaddAcceptsFenced)) THEN "rstop"
                              ELSE IF ReviveRewritesMount /\ mount.u = RvUuid(r) THEN RvAfterMount
                              ELSE "rmdel"]
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, pc, Locals, DVars, GVars>>
    /\ UNCHANGED <<clock, backUsed, hwm, hwmGen, admitUsed, myTime, myHwm, dHwm, myFloor, NVarsNoRv, rvOTok>>

\* DELETE `mount` If-Match the farewell token; ReviveRewritesMount: PUT If-Match of the farewell with the
\* revived uuid instead, so the slot never becomes absent.
RvDelMount(r) ==
    /\ rv[r] = "rmdel"
    /\ NoZombie => ~PendingMountClaim
    /\ IF mount.e > 0 /\ mGen = rvMTok[r]
       THEN /\ mount' = IF ReviveRewritesMount
                             THEN [mount EXCEPT !.u = RvUuid(r), !.holder = "none", !.clean = TRUE, !.fenced = FALSE]
                             ELSE MAbsent
            /\ mGen' = mGen + 1
            /\ rv' = [rv EXCEPT ![r] = RvAfterMount]
       ELSE /\ UNCHANGED <<mount, mGen>> /\ rv' = [rv EXCEPT ![r] = "rstop"]
    /\ UNCHANGED <<EpVars, ownerRetired, manifests, refs, inflight, ambUsed, nonceCtr, Ghost, pc, Locals,
                   DVars, GVars>>
    /\ UNCHANGED <<clock, backUsed, hwm, hwmGen, admitUsed, myTime, myHwm, dHwm, NVarsNoRv, rvMTok, rvOTok>>

\* GET `owner`: still tombstoned.
RvReadOwner(r) ==
    /\ rv[r] = "roread"
    /\ rvOTok' = [rvOTok EXCEPT ![r] = OTok]
    /\ rv' = [rv EXCEPT ![r] = IF ownerRetired /\ ~ownerGone THEN "rowrite" ELSE "rstop"]
    /\ UNCHANGED <<Store, inflight, ambUsed, nonceCtr, Ghost, pc, Locals, DVars, GVars>>
    /\ UNCHANGED <<clock, backUsed, hwm, hwmGen, admitUsed, myTime, myHwm, dHwm, myFloor, NVarsNoRv, rvMTok>>

\* PUT `owner` If-Match: {ReviveUuid, no tombstone}.  Opens a new life (ghost).
RvWriteOwner(r) ==
    /\ rv[r] = "rowrite"
    /\ IF OTok = rvOTok[r] /\ ~ownerGone
       THEN /\ ownerRetired' = FALSE /\ ownerUuid' = RvUuid(r) /\ ownerGen' = ownerGen + 1 /\ oStamp' = 0
            /\ life' = life + 1
            /\ rv' = [rv EXCEPT ![r] = IF ReviveOwnerFirst THEN "rmread" ELSE "rdone"]
       ELSE /\ UNCHANGED <<ownerRetired, ownerUuid, ownerGen, oStamp, life>> /\ rv' = [rv EXCEPT ![r] = "rstop"]
    /\ UNCHANGED <<EpVars, mount, mGen, manifests, refs, inflight, ambUsed, nonceCtr, Ghost, pc, Locals,
                   DVars, GVars>>
    /\ UNCHANGED <<clock, backUsed, hwm, hwmGen, admitUsed, myTime, myHwm, dHwm, myFloor, ownerFloor, ownerGone, dOwnTok, dTombTok, rpc, rTok, oInfl,
                   rvMTok, rvOTok, tombOverLive, dTombRefused, zDrain, dLife, staleDecom, staleDrain>>

-----------------------------------------------------------------------------
(* Orphan manifest sweep                                                   *)

\* floorForNamespace: GET mount.
GcFloor ==
    /\ gpc = "gidle" /\ gRounds < MaxGcRounds
    /\ gfloor' = [e |-> mount.e, clean |-> mount.clean, holder |-> "none", u |-> "none", fenced |-> FALSE]
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
        \/ Start(p) \/ OpenProbe(p) \/ OwnerCheck(p) \/ OwnerEmpty(p) \/ OwnerCreate(p) \/ AllocRead(p)
        \/ JoinEpoch(p) \/ JoinMount(p) \/ JoinEmpty(p) \/ JoinRepl(p) \/ JoinWrite(p)
        \/ RemountStart(p) \/ RemountOwner(p) \/ ClaimForeignOwner(p) \/ AllocEmpty(p) \/ AllocProbe(p)
        \/ ABase(p)
        \/ AllocWrite(p) \/ AllocResolve(p)
        \/ ClaimRead(p) \/ ClaimCheckOwner(p) \/ ClaimCheckCounter(p) \/ ClaimPostOwner(p)
        \/ ClaimPostFarewell(p) \/ ClaimMint(p) \/ ClaimReclaim(p)
        \/ WriteManifest(p) \/ Publish(p) \/ Farewell(p) \/ Crash(p)
    \/ \E r \in inflight : LateLand(r)
    \/ Decom /\ (DDrain \/ DCapture \/ DReadMount \/ DDelMount \/ DDelEpoch \/ DRecheckMount
                 \/ DRecheckEpoch \/ DRetire \/ DReadOwner \/ DTombstone \/ DHwmRead \/ DHwmWrite
                 \/ DPostTombCheck \/ DUntomb \/ DRestart \/ DConfirmClaim \/ DVerifyMount \/ DVerifyOwner \/ DMarkVerified)
    \/ OwnerLateLand \/ LateDelete \/ GcFenceDead \/ GcFenceLive \/ CatalogGc \/ ZombieDrain \/ ZombieConfirm \/ ZombieUntomb \/ GcFence \/ RRead \/ RWrite \/ RDelete
    \/ \E r \in Revivers : RvStart(r) \/ RvReadEpoch(r) \/ RvEmpty(r) \/ RvReadMount(r) \/ RvDelMount(r) \/ RvReadOwner(r) \/ RvWriteOwner(r)
    \/ PoolMetaAdmit \/ Tick \/ ClockStepBack
    \/ GcFloor \/ GcView \/ GcEnd \/ \E k \in manifests : GcDelete(k)

Spec == Init /\ [][Next]_vars

StateBound == epObj.v <= EMax

\* Every step of the decommission tool except its restart.
DProc ==
    \/ OpenProbe("d") \/ OwnerCheck("d") \/ OwnerEmpty("d") \/ OwnerCreate("d") \/ AllocRead("d")
    \/ AllocEmpty("d") \/ AllocProbe("d") \/ ABase("d") \/ AllocWrite("d") \/ AllocResolve("d")
    \/ ClaimRead("d") \/ ClaimCheckOwner("d") \/ ClaimCheckCounter("d") \/ ClaimMint("d") \/ ClaimReclaim("d")
    \/ Farewell("d") \/ DDrain \/ DCapture \/ DReadOwner \/ DTombstone \/ DDelEpoch \/ DConfirmClaim \/ DUntomb
    \/ DVerifyMount \/ DVerifyOwner \/ DMarkVerified

FairSpec == Spec /\ WF_vars(DProc)

\* A decommission re-run after a crash at any step finishes.
RetryCompletes == (starts["d"] >= 1) ~> (pc["d"] = "ddone")

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

\* The tool finished (or refused) and the owner is tombstoned: no unclean lease in the slot.  Checked at
\* rest because the S11 un-tombstone legitimately follows a successor's mint.
RetiredOwnerHasNoLiveMount == (pc["d"] \in {"dstop", "ddone"} /\ ownerRetired) => (mount.e = 0 \/ mount.clean)

\* RetiredStaysRetired once the tool has stopped.
RetiredAtRest == pc["d"] \in {"dstop", "ddone"} => RetiredStaysRetired

\* Revival safety: every allocation after a revival is above every allocation before it, whichever
\* process makes it.
RevivalAboveEarlierLives == \A x, y \in allocLife : x[2] < y[2] => x[1] < y[1]

\* Keep-everything: every instance running live has the owner's uuid and an untombstoned owner.
LiveMatchesOwner ==
    \A i \in Inst : LiveP(i) => (~ownerRetired /\ ~ownerGone /\ ownerUuid = Uuid(i))

\* A 26.6.4 starter never runs under an owner of another uuid (a new-uuid rejoin).
OldNotUnderNewOwner == \A i \in OldInst : LiveP(i) => ownerUuid = Uuid(i)

\* LiveMatchesOwner once the tool finished and no step of a paused earlier run is pending.
DecomQuiet == pc["d"] \in {"dstop", "ddone"} /\ ~zDrain.on /\ ~oInfl.on
LiveMatchesOwnerAtRest == DecomQuiet => LiveMatchesOwner

\* CAS-158: no tombstone lands while an instance holds the slot live.
NoTombOverLive == ~tombOverLive

\* A decommission re-run never refuses on the tombstone.
RetryAcceptsTomb == ~dTombRefused

\* Witness (expected violated): a decommission re-run reaches its end.
RetryNeverCompletes == ~(starts["d"] >= 1 /\ pc["d"] = "ddone")

\* A decommission that verified its end leaves the owner retired until a re-add (a new life), whatever
\* a later run of the tool does.
DoneMeansRetired == (VerifyRetired /\ life \in doneLives) => ownerRetired

\* No permanent ForeignOwner wedge: the owner is live with uuid U, the slot holds another uuid's body that
\* nothing will change (no process of that uuid can still write it, no fence or reclaim rule applies),
\* so every start of U ends ForeignOwner.  Re-add needs a retired owner and the tool's claim is refused
\* too, so such a state is permanent.
WedgeQuiet == \A q \in Procs : Uuid(q) = mount.u => pc[q] \in {"down", "open", "owner", "dstop", "ddone", "dfail"}
SlotStuck == IF mount.clean THEN ~ClaimForeignClean
             ELSE IF mount.fenced THEN ~ClaimForeignFenced
             ELSE ~(GcFencesDead /\ ClaimForeignFenced)
NoWedge == ~(~ownerRetired /\ ~ownerGone /\ mount.e > 0 /\ mount.u # ownerUuid /\ WedgeQuiet /\ SlotStuck)

\* No process resumes in place after its srid was retired and rejoined: a self-remount never makes a
\* process live again when it started before a later rejoin.
NoResumeAfterRetire == ~resumedStale

\* No decommission tombstone lands on an owner of another uuid.
NoForeignTombstone == ~tombForeign

\* Witness, expected violated: after a claimer fenced itself on the recheck, an instance runs live again.
NoLiveAfterSelfFence == ~(selfFenced /\ \E i \in Inst : pc[i] = "live")

\* Witness, expected violated: a member with the second uuid runs live after a re-add.
SecondUuidNeverLive == ~\E i \in Inst : pc[i] = "live" /\ Uuid(i) = "u2"

\* No decommission tombstone lands in a later life than the owner read it was decided on.
NoStaleTombstone == ~staleDecom

\* No decommission drain removes a reference of an epoch newer than the draining run's own epoch.
NoStaleDrain == ~staleDrain

\* A stale drain needs a decommission re-run: without one, re-add never meets a clean farewell while the
\* tool that lost its claim still drains.
NoStaleDrainWithoutRerun == staleDrain => starts["d"] >= 1

TypeOK ==
    /\ epObj.v \in Nat /\ mount.e \in Nat
    /\ manifests \subseteq Nat /\ refs \subseteq Nat
    /\ dup \in BOOLEAN
=============================================================================
