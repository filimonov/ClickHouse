-------------------- MODULE CaRefTableSnapshotLogCore --------------------
(* One namespace's ref stream: contiguous append-only log + snapshot + `_ckpt` — spec
   2026-07-27-cas-ref-chain-complete-cut-design.md (v9), §2 INV-1/INV-2/INV-4 and §4 Recovery.
   TLA+ phase (gate), Task 1.

   Scope:

   - The namespace's true history is a sequence of immutable ref transactions with per-namespace
     CONTIGUOUS ids (`writtenEver`, the recovery oracle — it never shrinks even after cleanup
     deletes objects). Each durable id has one op: `birth` | `mut` | `remove`. Folding the ops of
     an id set in id order is the table state (`WState` = Replay(full history)).
   - INV-1, the allocator: there is no pool-wide atomic. The next id is derived from state
     (`NextSlot` = `MaxWritten` + 1) and at most one append is in flight (`pendingSlot`). An id is
     freed for reuse only under the EVERY-ATTEMPT rule: nothing was sent, or every sent attempt has
     its own conclusive rejection. A slot that ever had an AMBIGUOUS attempt (`ambiguousEver`)
     keeps the lane wedged; a successor's `EpochSeal` at that slot is itself a conclusive rejection.
   - INV-2, the seal: recovery closes the dead tail in-band with the `slot-occupy` primitive. At the
     first absent expected-next id the store's conditional create either succeeds (`Created` —
     `RSealSlot` occupies the slot, and the dead lane's straggler can never land there) or fails
     (`Occupied` — `RAdoptStraggler`: the straggler landed first, so adopt it and keep walking).
     The store's conditional create IS the fence; `_sab_blindput` shows an unconditional PUT breaks
     it.
   - INV-4, `_ckpt`: `ckpt` = `[base, seal]` is a token-CAS object, merged by semantic maximum.
     Logs are deletable only at or below `ckpt.base`, snapshots only STRICTLY below it. Recovery
     POINT-READS the base from `ckpt` and walks the tail by ARITHMETIC (`last + 1`); a missing base
     with an unchanged token is corruption, a missing base with an advanced token is a restart.
   - Recovery (§4): `_ckpt` → exact-key snapshot (three-way revalidation) → arithmetic tail →
     CAS-walk + seal → install. Acked => durable => dense => found.

   Deliberate scoping of this focused model (documented so the oracle stays decisive):

   - ONE namespace, ONE incarnation. The catalog, incarnations, namespace rebirth and the durable
     `Completed` marker of rev.4 are DELETED from this module — namespace lifecycle is
     `CaRefCatalogCore`'s and `CaRefNsCleanupStaleLeaderCore`'s subject. Ops reduce to
     birth/mut/remove.
   - The LIST hint is NOT modelled on the honest path, because v9 consumes it nowhere: recovery is a
     point read plus arithmetic. It survives as `RScanStep`/`RScanInstall`, an OMISSION-CAPABLE
     enumeration reachable only under `SabotageScanIsTruth`, whose whole purpose is to reproduce the
     observed `0x1430c`/`0x1430d` hidden-hole defect as a model counterexample.
   - Ordinary appends are frozen while a reader recovers (`ReaderInactive`) because recovery is a
     fresh mount with the previous writer's lane drained. What the drained lane can still do — land
     its in-flight PUT — is exactly `LatePredecessorPut` (`LatePred`) and `RAdoptStraggler`.
   - `LatePredecessorPut` is the rev.4 EXPECTED-FAIL, FLIPPED. Under v9 it is GREEN
     (`_v9_flip_latepred`): contiguity puts every straggler at the frontier, never below a published
     snapshot, and the slot-occupy seal fences the one that never landed. `_sab_noseal` is the same
     behaviour with the occupancy removed and reproduces the rev.4 counterexample, so the flip is
     proven, not assumed.
   - Seals are never cleaned by the legacy and frontier slices (they are tiny, and a walk crossing
     a dead epoch must find them). The epoch slice (`EpochVariant # "off"`, CAS-366) is the one that
     gives ids an epoch, lets GC delete seals below the checkpoint, and lets a zombie append land
     late in the reclaimed slot.

   Ghost variables carry sabotage detection and are empty/FALSE in every honest run:
   `concludedEver` (slots a completed recovery declared final), `burnedIds` (ids the allocator
   skipped), `orphanIds` (attempts abandoned while still in flight), `phantomEver` (a conclusively
   rejected write became durable).

   Each Sabotage* constant breaks exactly one load-bearing rule and MUST yield a counterexample;
   with all Sabotage* false the model is GREEN for both `LatePred` settings. *)
EXTENDS Integers, FiniteSets

CONSTANTS
    MaxSeq,                          \* a writer may append ids 1..MaxSeq
    MaxRestarts,                     \* bounded recovery restarts (spec §4)
    LatePred,                        \* the drained lane's PUT lands while a successor recovers
    SabotageReuseAfterAmbiguous,     \* free an id whose attempt was AMBIGUOUS (breaks every-attempt)
    SabotageGapOnFail,               \* rev.4 allocator: a failed create burns its id, leaving a gap
    SabotageNoSeal,                  \* recovery concludes the frontier without OCCUPYING the slot
    SabotageBlindPut,                \* the lane PUTs unconditionally instead of conditional-create
    SabotageScanIsTruth,             \* the reader folds what the LIST hint returned, not the walk
    SabotageCleanupAboveCkpt,        \* cleanup deletes logs above `ckpt.base`
    SabotageStaleCkptCorruption,     \* cleanup deletes the snapshot `ckpt.base` still points at
    SabotageSealClobbersBase,        \* the sealer writes back its SAMPLED base instead of merging
    FrontierSlice,                   \* Task 5b's focused log -> frontier -> install/ack state machine
    SabotageAckBeforeFrontier,       \* acknowledge a durable log before `_ckpt.committed_through`
    SabotageAllocateBeforeFrontier,  \* allocate the successor id before the frontier is durable
    SabotageInstallAboveFrontier,    \* recovery installs an unfrontiered object
    SabotageStaleFrontier,           \* a stale writer advances without a valid same-fence proof
    SabotageSnapshotAboveFrontier,   \* checkpoint snapshot exceeds `committed_through`
    SabotageSealAboveFrontier,       \* checkpoint seal exceeds `committed_through`
    SabotageSnapshotAtSeal,          \* checkpoint snapshot names an epoch-seal record
    EpochVariant,                    \* CAS-366 epoch slice: "off" | "cur" | "fixA" | "sabA" |
                                     \* "fixA2" | "sabA2le" | "fixB" | "sabB" | "fixB2" | "fixB3" |
                                     \* "sabB3"
    EpochCount,                      \* writer epochs 1..EpochCount (2 or 3)
    T1Holds                          \* admission tracks the lease (TRUE) or is a free choice (T1 broken)

Seqs == 1..MaxSeq                    \* ids a writer may append
Ids  == 1..(MaxSeq + 1)              \* + the slot a frontier seal may occupy
Ops  == {"none", "birth", "mut", "remove"}
Lifes == {"empty", "live", "removed"}
EmptyState == [life |-> "empty", committed |-> {}, removeSeq |-> 0]

FrontierPhases ==
    {"Idle", "Prepared", "LogDurable", "FrontierIssued", "FrontierDurable",
     "CheckpointRead", "RecoveryValidated", "SealDurable",
     "Installed", "Acknowledged", "NeedsRecovery", "Rejected"}
FrontierKinds == {"none", "log", "seal"}
FrontierFences == 1..2
FrontierState ==
    [phase : FrontierPhases,
     currentFence : FrontierFences,
     candidateId : 0..MaxSeq,
     candidateFence : 0..2,
     candidateKind : FrontierKinds,
     objectKind : [Seqs -> FrontierKinds],
     objectFence : [Seqs -> 0..2],
     objectPrev : [Seqs -> 0..MaxSeq],
     committedThrough : 0..MaxSeq,
     checkpointSnapshot : 0..MaxSeq,
     snapshotBodyThrough : 0..MaxSeq,
     lastEpochSeal : 0..MaxSeq,
     installedThrough : 0..MaxSeq,
     acknowledgedThrough : 0..MaxSeq,
     nextId : 1..(MaxSeq + 1),
     issuedId : 0..MaxSeq,
     issuedFence : 0..2,
     issuedKind : FrontierKinds,
     issuedToken : 0..(3 * MaxSeq + 4),
     checkpointToken : 0..(3 * MaxSeq + 4),
     sampledThrough : 0..MaxSeq,
     sampledToken : 0..(3 * MaxSeq + 4),
     lostResponsePending : BOOLEAN,
     recoveryAdoptingSuccessor : BOOLEAN,
     validFrontierProofs : SUBSET Seqs]

FrontierWitnessState ==
    [crashPrepared : BOOLEAN,
     crashLogDurable : BOOLEAN,
     crashFrontierDurable : BOOLEAN,
     crashInstalled : BOOLEAN,
     lostFrontierResponse : BOOLEAN,
     exactSuccessorAdopted : BOOLEAN,
     oldWriterLostToSeal : BOOLEAN,
     issuedCasLinearizedAfterFenceMove : BOOLEAN,
     snapshotPublishedAtFrontier : BOOLEAN,
     sealPublishedAtFrontier : BOOLEAN,
     lostSealResponseResolved : BOOLEAN]

VARIABLES
    op,            \* [Ids -> Ops]   content bound to an id at ATTEMPT time (durable iff in writtenEver)
    writtenEver,   \* SUBSET Seqs    every id ever durably appended (recovery oracle; never shrinks)
    logs,          \* SUBSET Seqs    present _log/<id> data objects
    snaps,         \* SUBSET Seqs    present _snap/<id> objects
    publishedEver, \* SUBSET Seqs    snapshot ids ever published
    snapCov,       \* [Ids -> state] frozen body captured at each snapshot publish
    ckpt,          \* [base |-> 0..MaxSeq, seal |-> 0..MaxSeq+1]  the _ckpt object; 0 = none
                   \* `base` = checkpoint_snapshot_id, `seal` = last_epoch_seal. Both writers
                   \* read-modify-write the WHOLE body and merge by semantic maximum per field
                   \* (INV-4), so a stale reader-writer can regress a field it did not intend to
                   \* touch — see `_sab_sealclobbersbase`. `seal` carries no proof obligation in
                   \* THIS module and is deliberately write-only here: its consumers are a later
                   \* mount locating the previous epoch's terminating record and the GC fold
                   \* crossing epochs (spec §5), neither of which exists in a single-recoverer,
                   \* single-namespace model. Its obligation belongs to `CaCasMountCore` (task 5)
                   \* and `CaRefDeltaIntakeCore` (task 2). It is still merged and typed here so
                   \* those models inherit a field that behaves correctly.
    pendingSlot,   \* 0..MaxSeq      the single in-flight append (0 = the lane is idle)
    ambiguousEver, \* SUBSET Seqs    slots that ever had an attempt of unknown outcome
    sealedIds,     \* SUBSET Ids     slots PHYSICALLY occupied by an EpochSeal
    concludedEver, \* ghost: slots a completed recovery declared final (= sealedIds when honest)
    burnedIds,     \* ghost: ids the allocator skipped without writing them (sabotage only)
    orphanIds,     \* ghost: attempts abandoned while their PUT may still land (sabotage only)
    phantomEver,   \* ghost: a write whose failure was reported to the caller became durable
    rPhase,        \* "idle"|"ckpt"|"base"|"walk"|"scan"|"done"|"stuck"|"corrupt"
    rBase,         \* the ckpt.base this recovery sampled
    rWalkPos,      \* arithmetic walk cursor (last id proven present)
    rTail,         \* SUBSET Seqs    ids this recovery will fold above the base
    rScanPos,      \* hint-enumeration cursor (resume-after-last-returned-key)
    rSeenLogs,     \* SUBSET Seqs    ids the hint enumeration returned
    rRestarts,     \* restart counter
    frontier,      \* Task 5b's focused checkpoint-frontier state
    frontierWitness, \* reachability controls for every required honest crash/race window
    epObj,         \* CAS-366 slice: [EIds -> {"none","log","seal"}] the `_log/<id>` keys in the store
    epLink,        \* [EIds -> 0..EIdMax] the `prev_epoch_seal` carried in each object's bytes
    epHist,        \* ghost: [EIds -> kind] what logical history holds at each id (never shrinks)
    epCkpt,        \* `_ckpt` = [ct, seal, snap]: committed_through, last_epoch_seal, checkpoint_snapshot_id
    epSnapBody,    \* the snapshot body the live publisher PUT and has not yet checkpointed (0 = none)
    epLive,        \* the epoch that holds the mount lease
    zPh,           \* the zombie lane of epoch 1
    zSeq,          \* its greatest installed sequence
    zOut,          \* what the store did with its in-flight create
    zAcked,        \* ids the zombie acknowledged to callers
    zAckLate,      \* ghost: the zombie acknowledged after a takeover (straggler-ack witness)
    mPh,           \* [2..EpochCount -> phase] honest successor mounts
    mTgt,          \* [2..EpochCount -> id] the id the mount acts on next
    mApp,          \* [2..EpochCount -> 0..SAppMax] appends of the mount in its own epoch
    mSeal,         \* [2..EpochCount -> id] last seal the mount's walk saw or minted (its chain link)
    sAcked,        \* ids the successors acknowledged
    gPh,           \* GC round phase
    gSnap,         \* plan sample: checkpoint_snapshot_id
    gCur,          \* plan sample: fold cursor (= committed_through)
    gSeal,         \* plan sample: last_epoch_seal (fixB/sabB only)
    gProof         \* plan sample: retained_log_proof (the base's predecessor seal)

storeVars  == << op, writtenEver, logs, snaps, publishedEver, snapCov >>
laneVars   == << pendingSlot, ambiguousEver >>
sealVars   == << sealedIds, concludedEver >>
ghostVars  == << burnedIds, orphanIds, phantomEver >>
readerVars == << rPhase, rBase, rWalkPos, rTail, rScanPos, rSeenLogs, rRestarts >>
legacyVars == << op, writtenEver, logs, snaps, publishedEver, snapCov, ckpt,
                 pendingSlot, ambiguousEver, sealedIds, concludedEver,
                 burnedIds, orphanIds, phantomEver,
                 rPhase, rBase, rWalkPos, rTail, rScanPos, rSeenLogs, rRestarts >>
frontierVars == << frontier, frontierWitness >>
epochVars == << epObj, epLink, epHist, epCkpt, epSnapBody, epLive,
                zPh, zSeq, zOut, zAcked, zAckLate,
                mPh, mTgt, mApp, mSeal, sAcked,
                gPh, gSnap, gCur, gSeal, gProof >>

vars == << legacyVars, frontierVars, epochVars >>

MinOf(S) == CHOOSE x \in S : \A y \in S : x <= y
MaxOf(S) == CHOOSE x \in S : \A y \in S : x >= y
MaxOr0(S) == IF S = {} THEN 0 ELSE MaxOf(S)
Max2(a, b) == IF a >= b THEN a ELSE b

(* INV-4's ONE update algorithm, shared by both `_ckpt` writers (the snapshot publisher and the
   sealer): read the whole body, merge by SEMANTIC MAXIMUM per field, token-CAS. Writing the whole
   body is what makes a stale field dangerous — a writer that skips the merge and writes back the
   value it sampled earlier silently regresses the OTHER writer's progress. Modelling it as a
   record-valued merge rather than an `EXCEPT !.field` surgical update is deliberate: the surgical
   form cannot express that regression, and the real code does not have it. *)
CkptMerge(base, seal) == [base |-> Max2(ckpt.base, base), seal |-> Max2(ckpt.seal, seal)]

(* Fold one op into the table state. birth/remove reset content; mut adds its id. Never invoked
   with "none" on the honest path (folds range only over durable ids); OTHER is an identity guard,
   which also makes an `EpochSeal` an applied no-op (spec §5, B1/B2). *)
ApplyOp(s, id) ==
    CASE op[id] = "birth"  -> [life |-> "live",    committed |-> {},                   removeSeq |-> 0]
      [] op[id] = "mut"    -> [life |-> s.life,    committed |-> s.committed \cup {id}, removeSeq |-> s.removeSeq]
      [] op[id] = "remove" -> [life |-> "removed", committed |-> {},                   removeSeq |-> id]
      [] OTHER             -> s

RECURSIVE FoldIds(_, _)
FoldIds(base, S) ==
    IF S = {} THEN base
    ELSE LET m == MinOf(S) IN FoldIds(ApplyOp(base, m), S \ {m})

WState == FoldIds(EmptyState, writtenEver)               \* Replay(full history) — the oracle

(* INV-1: ids derive from state. `burnedIds` is the rev.4 allocator's gap and is empty when honest. *)
MaxWritten == MaxOr0(writtenEver \cup sealedIds \cup burnedIds)
NextSlot == MaxWritten + 1

(* ---- CAS-366 epoch slice: definitions shared by `Init` and the slice's actions ----

   Ids carry an epoch: id = epoch * EBase + sequence, so integer order is the `RefTxnId` order
   (writer_epoch, then ref_sequence). Sequence 0 is never an object; `EId(e, 0)` only orders. *)
EpochSlice == EpochVariant # "off"
EBase == 4
SMax == 3                            \* sequences 1..SMax per epoch (a seal may take the last one)
ZSeqMax == 2                         \* the zombie (epoch 1) appends at most this many records
SAppMax == 2                         \* each successor appends at most this many in its own epoch
EId(e, q) == e * EBase + q
Ep(id) == id \div EBase
Sq(id) == id % EBase
EIdMax == EId(EpochCount, SMax)
EIds == { EId(e, q) : e \in 1..EpochCount, q \in 1..SMax }
Succs == 2..EpochCount
EKinds == {"none", "log", "seal"}
NoCkpt == [ct |-> 0, seal |-> 0, snap |-> 0]
ZPhases == {"idle", "prepared", "gated", "inflight", "settle", "wedged", "durable", "published",
            "installed", "closed", "unresolved", "needsrec", "fenced"}
MPhases == {"down", "ckpt", "walk", "adoptlog", "adoptseal", "sealing", "sealpub", "ready",
            "appendpub", "stopped", "failed", "refused"}

EpochInit ==
    /\ epObj = [i \in EIds |-> "none"]
    /\ epLink = [i \in EIds |-> 0]
    /\ epHist = [i \in EIds |-> "none"]
    /\ epCkpt = NoCkpt
    /\ epSnapBody = 0
    /\ epLive = 1
    /\ zPh = "idle"
    /\ zSeq = 0
    /\ zOut = "none"
    /\ zAcked = {}
    /\ zAckLate = FALSE
    /\ mPh = [m \in Succs |-> "down"]
    /\ mTgt = [m \in Succs |-> 0]
    /\ mApp = [m \in Succs |-> 0]
    /\ mSeal = [m \in Succs |-> 0]
    /\ sAcked = {}
    /\ gPh = "idle"
    /\ gSnap = 0
    /\ gCur = 0
    /\ gSeal = 0
    /\ gProof = 0

Init ==
    /\ op = [i \in Ids |-> "none"]
    /\ writtenEver = {}
    /\ logs = {}
    /\ snaps = {}
    /\ publishedEver = {}
    /\ snapCov = [i \in Ids |-> EmptyState]
    /\ ckpt = [base |-> 0, seal |-> 0]
    /\ pendingSlot = 0
    /\ ambiguousEver = {}
    /\ sealedIds = {}
    /\ concludedEver = {}
    /\ burnedIds = {}
    /\ orphanIds = {}
    /\ phantomEver = FALSE
    /\ rPhase = "idle"
    /\ rBase = 0
    /\ rWalkPos = 0
    /\ rTail = {}
    /\ rScanPos = 0
    /\ rSeenLogs = {}
    /\ rRestarts = 0
    /\ frontier =
        [phase |-> "Idle",
         currentFence |-> 1,
         candidateId |-> 0,
         candidateFence |-> 0,
         candidateKind |-> "none",
         objectKind |-> [i \in Seqs |-> "none"],
         objectFence |-> [i \in Seqs |-> 0],
         objectPrev |-> [i \in Seqs |-> 0],
         committedThrough |-> 0,
         checkpointSnapshot |-> 0,
         snapshotBodyThrough |-> 0,
         lastEpochSeal |-> 0,
         installedThrough |-> 0,
         acknowledgedThrough |-> 0,
         nextId |-> 1,
         issuedId |-> 0,
         issuedFence |-> 0,
         issuedKind |-> "none",
         issuedToken |-> 0,
         checkpointToken |-> 0,
         sampledThrough |-> 0,
         sampledToken |-> 0,
         lostResponsePending |-> FALSE,
         recoveryAdoptingSuccessor |-> FALSE,
         validFrontierProofs |-> {}]
    /\ frontierWitness =
        [crashPrepared |-> FALSE,
         crashLogDurable |-> FALSE,
         crashFrontierDurable |-> FALSE,
         crashInstalled |-> FALSE,
         lostFrontierResponse |-> FALSE,
         exactSuccessorAdopted |-> FALSE,
         oldWriterLostToSeal |-> FALSE,
         issuedCasLinearizedAfterFenceMove |-> FALSE,
         snapshotPublishedAtFrontier |-> FALSE,
         sealPublishedAtFrontier |-> FALSE,
         lostSealResponseResolved |-> FALSE]
    /\ EpochInit

ReaderInactive == rPhase = "idle"

(* ---- writer lane: at most one in-flight append, ids derived from state (INV-1) ---- *)

(* The bytes are bound to the id when the attempt is FORMED, not when it lands: an abandoned attempt
   that reaches the store later carries exactly these bytes. Durability is `writtenEver` alone. *)
WAppendStart(o) ==
    /\ ReaderInactive
    /\ pendingSlot = 0
    /\ NextSlot <= MaxSeq
    /\ \/ (o = "birth"  /\ WState.life = "empty")
       \/ (o = "mut"    /\ WState.life = "live")
       \/ (o = "remove" /\ WState.life = "live")
    /\ pendingSlot' = NextSlot
    /\ op' = [op EXCEPT ![NextSlot] = o]
    /\ UNCHANGED << writtenEver, logs, snaps, publishedEver, snapCov >>
    /\ UNCHANGED << ckpt, ambiguousEver >>
    /\ UNCHANGED sealVars /\ UNCHANGED ghostVars /\ UNCHANGED readerVars

WriterAppend == \E o \in {"birth", "mut", "remove"} : WAppendStart(o)

(* An attempt was sent and its outcome is unknown. The lane is now WEDGED: it may not free the id. *)
WAttemptAmbiguous ==
    /\ pendingSlot # 0
    /\ pendingSlot \notin ambiguousEver
    /\ ambiguousEver' = ambiguousEver \cup {pendingSlot}
    /\ UNCHANGED << pendingSlot, ckpt >>
    /\ UNCHANGED storeVars /\ UNCHANGED sealVars /\ UNCHANGED ghostVars /\ UNCHANGED readerVars

(* The conditional create succeeded: the record is durable at its slot. `SabotageBlindPut` drops the
   conditional-create fence, letting the lane overwrite a slot a successor already sealed. *)
WResolveDurable ==
    /\ ReaderInactive
    /\ pendingSlot # 0
    /\ (pendingSlot \notin sealedIds \/ SabotageBlindPut)
    /\ writtenEver' = writtenEver \cup {pendingSlot}
    /\ logs' = logs \cup {pendingSlot}
    /\ pendingSlot' = 0
    /\ UNCHANGED << op, snaps, publishedEver, snapCov >>
    /\ UNCHANGED << ckpt, ambiguousEver >>
    /\ UNCHANGED sealVars /\ UNCHANGED ghostVars /\ UNCHANGED readerVars

(* Every-attempt rule: only a PROVEN-never-applied attempt frees the id, and the freed id is REUSED
   by the next WAppendStart — no gap. Under `SabotageReuseAfterAmbiguous` the lane also frees a slot
   whose attempt was ambiguous; that attempt's bytes are still out there (`orphanIds`) and its caller
   has already been told the operation FAILED. Under `SabotageGapOnFail` the freed id is burned
   instead of reused — the rev.4 "a definitely-failed create leaves a safe id gap" allocator. *)
WResolveConclusiveReject ==
    /\ ReaderInactive
    /\ pendingSlot # 0
    /\ (pendingSlot \notin ambiguousEver \/ SabotageReuseAfterAmbiguous)
    /\ IF pendingSlot \in ambiguousEver
       THEN /\ orphanIds' = orphanIds \cup {pendingSlot}
            /\ UNCHANGED op
       ELSE /\ orphanIds' = orphanIds
            /\ op' = [op EXCEPT ![pendingSlot] = "none"]
    /\ burnedIds' = IF SabotageGapOnFail THEN burnedIds \cup {pendingSlot} ELSE burnedIds
    /\ pendingSlot' = 0
    /\ UNCHANGED << writtenEver, logs, snaps, publishedEver, snapCov >>
    /\ UNCHANGED << ckpt, ambiguousEver, phantomEver >>
    /\ UNCHANGED sealVars /\ UNCHANGED readerVars

(* A successor's EpochSeal found at the slot is itself a conclusive rejection: the wedged lane
   retries T+1 (state-derived), it never mints T+2. *)
WResolveSealRejected ==
    /\ pendingSlot # 0
    /\ pendingSlot \in sealedIds
    /\ op' = [op EXCEPT ![pendingSlot] = "none"]
    /\ pendingSlot' = 0
    /\ UNCHANGED << writtenEver, logs, snaps, publishedEver, snapCov >>
    /\ UNCHANGED << ckpt, ambiguousEver >>
    /\ UNCHANGED sealVars /\ UNCHANGED ghostVars /\ UNCHANGED readerVars

(* An abandoned attempt reaches the store after its caller was told it failed. Reachable only after
   `SabotageReuseAfterAmbiguous` produced an orphan; the conditional create still fences it out of
   any slot that is taken. Its landing is what makes the reported failure a lie. *)
WOrphanLands ==
    /\ \E k \in orphanIds :
        /\ k \notin writtenEver
        /\ k \notin sealedIds
        /\ k # pendingSlot
        /\ op[k] # "none"
        /\ writtenEver' = writtenEver \cup {k}
        /\ logs' = logs \cup {k}
        /\ orphanIds' = orphanIds \ {k}
    /\ phantomEver' = TRUE
    /\ UNCHANGED << op, snaps, publishedEver, snapCov >>
    /\ UNCHANGED << ckpt, burnedIds >>
    /\ UNCHANGED laneVars /\ UNCHANGED sealVars /\ UNCHANGED readerVars

(* ---- adversarial: the drained lane's PUT lands while a successor recovers ----

   rev.4 §late-predecessor-put was the documented EXPECTED-FAIL: a fenced predecessor's PUT
   materialized BELOW an already-published snapshot. Under v9 it cannot: ids are contiguous, so a
   straggler is always at the frontier (`pendingSlot` = MaxWritten + 1 > every published id), and
   the frontier slot is either adopted (`RAdoptStraggler`) or occupied by the seal — the store's
   conditional create is the fence (INV-2). Hence `_v9_flip_latepred` is GREEN, and `_sab_noseal`
   reproduces the old counterexample by removing the occupancy. *)
LatePredecessorPut ==
    /\ LatePred
    /\ ~ReaderInactive
    /\ pendingSlot # 0
    /\ (pendingSlot \notin sealedIds \/ SabotageBlindPut)
    /\ writtenEver' = writtenEver \cup {pendingSlot}
    /\ logs' = logs \cup {pendingSlot}
    /\ pendingSlot' = 0
    /\ UNCHANGED << op, snaps, publishedEver, snapCov >>
    /\ UNCHANGED << ckpt, ambiguousEver >>
    /\ UNCHANGED sealVars /\ UNCHANGED ghostVars /\ UNCHANGED readerVars

(* ---- snapshot publication and the `_ckpt` CAS (INV-4), off-lane ---- *)

(* Publication is two steps — body PUT, then the `_ckpt` token-CAS — so cleanup can interleave
   between them (spec §9: "`_ckpt` races (cleanup between PUT and CAS ...)"). *)
WriterPublishSnapshot ==
    /\ \E X \in writtenEver :
        /\ X \notin publishedEver
        /\ X > ckpt.base
        /\ publishedEver' = publishedEver \cup {X}
        /\ snaps' = snaps \cup {X}
        /\ snapCov' = [snapCov EXCEPT ![X] = FoldIds(EmptyState, { i \in writtenEver : i <= X })]
    /\ UNCHANGED << op, writtenEver, logs, ckpt >>
    /\ UNCHANGED laneVars /\ UNCHANGED sealVars /\ UNCHANGED ghostVars /\ UNCHANGED readerVars

(* The publisher half of the one update algorithm: it knows a new base, knows nothing new about the
   seal, and merges both (INV-4). *)
WriterCkptAdvance ==
    /\ \E X \in snaps :
        /\ X > ckpt.base
        /\ ckpt' = CkptMerge(X, ckpt.seal)
    /\ UNCHANGED storeVars
    /\ UNCHANGED laneVars /\ UNCHANGED sealVars /\ UNCHANGED ghostVars /\ UNCHANGED readerVars

(* ---- GC ref-object cleanup, gated by `_ckpt` (INV-4) ---- *)

(* Logs are covered by the checkpoint snapshot at `ckpt.base`, so they are deletable AT OR BELOW it.
   The sabotage drops the gate: recovery's arithmetic walk then reads a 404 at a DURABLE id with an
   unchanged token, mistakes it for the frontier and seals there — a truncated install. *)
CleanupLogGate(L) == IF SabotageCleanupAboveCkpt THEN TRUE ELSE L <= ckpt.base
GcCleanupLog ==
    /\ \E L \in logs :
        /\ CleanupLogGate(L)
        /\ logs' = logs \ {L}
    /\ UNCHANGED << op, writtenEver, snaps, publishedEver, snapCov, ckpt >>
    /\ UNCHANGED laneVars /\ UNCHANGED sealVars /\ UNCHANGED ghostVars /\ UNCHANGED readerVars

(* Snapshots are deletable only STRICTLY BELOW `ckpt.base`, so a stale pointer can only
   under-clean. The sabotage races the base away, making a sampled base vanish under an UNCHANGED
   token — which the reader is required to call corruption. *)
CleanupSnapGate(S) == IF SabotageStaleCkptCorruption THEN S <= ckpt.base ELSE S < ckpt.base
GcCleanupSnap ==
    /\ \E S \in snaps :
        /\ CleanupSnapGate(S)
        /\ snaps' = snaps \ {S}
    /\ UNCHANGED << op, writtenEver, logs, publishedEver, snapCov, ckpt >>
    /\ UNCHANGED laneVars /\ UNCHANGED sealVars /\ UNCHANGED ghostVars /\ UNCHANGED readerVars

(* ---- reader: `_ckpt` point read -> base -> arithmetic walk -> slot-occupy seal (§4) ---- *)

(* `NextSlot <= MaxSeq + 1` keeps the frontier seal inside the modelled id space; it is a bound, not
   a protocol rule. *)
ReaderStart ==
    /\ rPhase = "idle"
    /\ NextSlot <= MaxSeq + 1
    /\ rPhase' = "ckpt"
    /\ UNCHANGED << rBase, rWalkPos, rTail, rScanPos, rSeenLogs, rRestarts >>
    /\ UNCHANGED storeVars /\ UNCHANGED << ckpt >>
    /\ UNCHANGED laneVars /\ UNCHANGED sealVars /\ UNCHANGED ghostVars

(* The base is a POINT READ of `_ckpt`, never a listing. Each attempt re-reads it. *)
RReadCkpt ==
    /\ rPhase = "ckpt"
    /\ rBase' = ckpt.base
    /\ rWalkPos' = ckpt.base
    /\ rTail' = {}
    /\ rScanPos' = 0
    /\ rSeenLogs' = {}
    /\ rPhase' = "base"
    /\ UNCHANGED rRestarts
    /\ UNCHANGED storeVars /\ UNCHANGED << ckpt >>
    /\ UNCHANGED laneVars /\ UNCHANGED sealVars /\ UNCHANGED ghostVars

RRestartOrStuck ==
    IF rRestarts < MaxRestarts
    THEN /\ rPhase' = "ckpt"
         /\ rRestarts' = rRestarts + 1
         /\ UNCHANGED << rBase, rWalkPos, rTail, rScanPos, rSeenLogs >>
    ELSE /\ rPhase' = "stuck"
         /\ UNCHANGED << rBase, rWalkPos, rTail, rScanPos, rSeenLogs, rRestarts >>

(* Exact-key fetch of the sampled base, with the three-way revalidation of INV-4: present -> walk;
   missing with an ADVANCED token -> restart; missing with an UNCHANGED token -> corruption, which
   the deletion gate makes unreachable in an honest run. *)
RFetchBase ==
    /\ rPhase = "base"
    /\ \/ /\ (rBase = 0 \/ rBase \in snaps)
          /\ rPhase' = IF SabotageScanIsTruth THEN "scan" ELSE "walk"
          /\ UNCHANGED << rBase, rWalkPos, rTail, rScanPos, rSeenLogs, rRestarts >>
       \/ /\ rBase # 0
          /\ rBase \notin snaps
          /\ IF ckpt.base > rBase
             THEN RRestartOrStuck
             ELSE /\ rPhase' = "corrupt"
                  /\ UNCHANGED << rBase, rWalkPos, rTail, rScanPos, rSeenLogs, rRestarts >>
    /\ UNCHANGED storeVars /\ UNCHANGED << ckpt >>
    /\ UNCHANGED laneVars /\ UNCHANGED sealVars /\ UNCHANGED ghostVars

WalkNext == rWalkPos + 1

(* Arithmetic advance: the expected next id is `rWalkPos + 1` and its presence is an exact-key point
   read. A dead epoch is crossed by consuming its seal, which folds as a no-op. The hint plays no
   part. *)
RWalkStep ==
    /\ rPhase = "walk"
    /\ WalkNext \in Ids
    /\ (WalkNext \in logs \/ WalkNext \in sealedIds)
    /\ rWalkPos' = WalkNext
    /\ rTail' = IF WalkNext \in logs THEN rTail \cup {WalkNext} ELSE rTail
    /\ UNCHANGED << rPhase, rBase, rScanPos, rSeenLogs, rRestarts >>
    /\ UNCHANGED storeVars /\ UNCHANGED << ckpt >>
    /\ UNCHANGED laneVars /\ UNCHANGED sealVars /\ UNCHANGED ghostVars

(* A 404 BELOW the checkpoint means cleanup advanced the base past this id while we walked: restart
   from the newer base (bounded), never fail. *)
RWalkVanish ==
    /\ rPhase = "walk"
    /\ WalkNext \in Ids
    /\ WalkNext \notin logs
    /\ WalkNext \notin sealedIds
    /\ ckpt.base >= WalkNext
    /\ RRestartOrStuck
    /\ UNCHANGED storeVars /\ UNCHANGED << ckpt >>
    /\ UNCHANGED laneVars /\ UNCHANGED sealVars /\ UNCHANGED ghostVars

(* slot-occupy = Created: the expected next id is absent, so this is the frontier. Occupying it
   closes the epoch in-band and fences the dead lane's straggler forever. `SabotageNoSeal` keeps the
   CONCLUSION (`concludedEver`) but skips the OCCUPANCY — the rev.4 protocol, whose ghost then lands
   in a slot recovery already declared final. *)
RSealSlot ==
    /\ rPhase = "walk"
    /\ WalkNext \in Ids
    /\ WalkNext \notin logs
    /\ WalkNext \notin sealedIds
    /\ ckpt.base < WalkNext
    /\ sealedIds' = IF SabotageNoSeal THEN sealedIds ELSE sealedIds \cup {WalkNext}
    /\ concludedEver' = concludedEver \cup {WalkNext}
    (* The sealer half of the one update algorithm. Its knowledge of the base is `rBase`, sampled
       at the START of this recovery, so the merge is what keeps a base another writer advanced
       meanwhile from being dragged backwards. `SabotageSealClobbersBase` writes the sampled body
       back verbatim — the read-modify-write with the merge left out. *)
    /\ ckpt' = IF SabotageSealClobbersBase
               THEN [base |-> rBase, seal |-> WalkNext]
               ELSE CkptMerge(rBase, WalkNext)
    /\ rPhase' = "done"
    /\ UNCHANGED << rBase, rWalkPos, rTail, rScanPos, rSeenLogs, rRestarts >>
    /\ UNCHANGED storeVars /\ UNCHANGED laneVars /\ UNCHANGED ghostVars

(* slot-occupy = Occupied: the in-flight PUT reached the store first. Adopt it and keep walking —
   the adopted id costs the conditional PUT plus an exact GET (spec §8). *)
RAdoptStraggler ==
    /\ rPhase = "walk"
    /\ WalkNext \in Ids
    /\ WalkNext \notin logs
    /\ WalkNext \notin sealedIds
    /\ pendingSlot = WalkNext
    /\ writtenEver' = writtenEver \cup {pendingSlot}
    /\ logs' = logs \cup {pendingSlot}
    /\ pendingSlot' = 0
    /\ UNCHANGED << op, snaps, publishedEver, snapCov, ckpt, ambiguousEver >>
    /\ UNCHANGED sealVars /\ UNCHANGED ghostVars /\ UNCHANGED readerVars

(* ---- the LIST hint, reachable only under `SabotageScanIsTruth` ----

   One page = one key, resume strictly after the last returned key — but the enumeration MAY SKIP
   present keys, which is the observed `0x1430c`/`0x1430d` shape: a listing that looks complete and
   silently omits a record. `RScanInstall` requires the cursor to be past every present key, so the
   counterexample is a complete-LOOKING scan with a hole, not a reader that gave up early. *)
RScanStep ==
    /\ rPhase = "scan"
    /\ \E k \in logs :
        /\ k > rScanPos
        /\ rScanPos' = k
        /\ rSeenLogs' = rSeenLogs \cup {k}
    /\ UNCHANGED << rPhase, rBase, rWalkPos, rTail, rRestarts >>
    /\ UNCHANGED storeVars /\ UNCHANGED << ckpt >>
    /\ UNCHANGED laneVars /\ UNCHANGED sealVars /\ UNCHANGED ghostVars

RScanInstall ==
    /\ rPhase = "scan"
    /\ ~ \E k \in logs : k > rScanPos
    /\ rTail' = { i \in rSeenLogs : i > rBase }
    /\ rPhase' = "done"
    /\ UNCHANGED << rBase, rWalkPos, rScanPos, rSeenLogs, rRestarts >>
    /\ UNCHANGED storeVars /\ UNCHANGED << ckpt >>
    /\ UNCHANGED laneVars /\ UNCHANGED sealVars /\ UNCHANGED ghostVars

(* A finished or stuck mount is torn down; the next mount recovers again, walking across the seal
   the previous one left. `corrupt` is terminal. *)
ReaderReset ==
    /\ rPhase \in {"done", "stuck"}
    /\ rPhase' = "idle"
    /\ rBase' = 0
    /\ rWalkPos' = 0
    /\ rTail' = {}
    /\ rScanPos' = 0
    /\ rSeenLogs' = {}
    /\ rRestarts' = 0
    /\ UNCHANGED storeVars /\ UNCHANGED << ckpt >>
    /\ UNCHANGED laneVars /\ UNCHANGED sealVars /\ UNCHANGED ghostVars

(* ---- Task 5b: exact committed frontier ----

   This focused slice is selected by `FrontierSlice`; legacy v9 configs leave it frozen. It makes
   the four externally meaningful states distinct: the immutable log is durable, its exact-chain
   checkpoint contribution is durable, the value is installed, and the caller is acknowledged
   (which is also when the successor id becomes allocatable). The checkpoint contribution carries
   an explicit proof bit for each committed id. The object's immutable producer fence is distinct
   from the checkpoint request's admission fence: recovery may contribute an exact old-fence
   successor using a CAS issued under its new current fence. Every request binds the exact sampled
   checkpoint token. An already-issued request may linearize after only the fence moves, but an
   intervening checkpoint update invalidates it and a stale actor may not issue a new request.

   `objectPrev` is the exact chain link. A record or seal at `T+1` is the only object writer recovery
   may adopt above `committedThrough`; no action chases a second successor. Snapshot and seal fields
   share the checkpoint frontier and may never exceed it. *)

FrontierObjectIsExact(id, kind) ==
    /\ id \in Seqs
    /\ frontier.objectKind[id] = kind
    /\ frontier.objectFence[id] \in FrontierFences
    /\ frontier.objectPrev[id] = id - 1

FrontierCandidateMatchesObject ==
    /\ FrontierObjectIsExact(frontier.candidateId, frontier.candidateKind)
    /\ frontier.objectFence[frontier.candidateId] = frontier.candidateFence

FrontierStartLog ==
    /\ frontier.phase = "Idle"
    /\ frontier.nextId \in Seqs
    /\ frontier' =
        [frontier EXCEPT
            !.phase = "Prepared",
            !.candidateId = frontier.nextId,
            !.candidateFence = frontier.currentFence,
            !.candidateKind = "log",
            !.issuedId = 0,
            !.issuedFence = 0,
            !.issuedKind = "none",
            !.issuedToken = 0,
            !.sampledThrough = 0,
            !.sampledToken = 0,
            !.lostResponsePending = FALSE,
            !.recoveryAdoptingSuccessor = FALSE]
    /\ UNCHANGED frontierWitness

FrontierLogDurable ==
    /\ frontier.phase = "Prepared"
    /\ frontier.candidateKind = "log"
    /\ frontier.objectKind[frontier.candidateId] = "none"
    /\ frontier' =
        [frontier EXCEPT
            !.phase = "LogDurable",
            !.objectKind[frontier.candidateId] = "log",
            !.objectFence[frontier.candidateId] = frontier.candidateFence,
            !.objectPrev[frontier.candidateId] = frontier.candidateId - 1]
    /\ UNCHANGED frontierWitness

(* A new runtime fence can arrive before the old request resolves. It prevents NEW old-fence
   checkpoint requests; it does not cancel one the store already admitted. *)
FrontierMoveFence ==
    /\ frontier.currentFence = 1
    /\ frontier' = [frontier EXCEPT !.currentFence = 2]
    /\ UNCHANGED frontierWitness

FrontierIssueCheckpoint ==
    /\ frontier.phase = "LogDurable"
    /\ frontier.candidateId = frontier.committedThrough + 1
    /\ FrontierCandidateMatchesObject
    /\ frontier.currentFence = frontier.candidateFence
    /\ frontier' =
        [frontier EXCEPT
            !.phase = "FrontierIssued",
            !.issuedId = frontier.candidateId,
            !.issuedFence = frontier.currentFence,
            !.issuedKind = frontier.candidateKind,
            !.issuedToken = frontier.checkpointToken,
            !.recoveryAdoptingSuccessor = FALSE]
    /\ UNCHANGED frontierWitness

(* After a crash, recovery first samples the exact checkpoint and its token. If the one exact
   successor belongs to the old writer, the NEW mount admits the checkpoint CAS under its CURRENT
   fence; the immutable object's producer fence is intentionally independent. *)
FrontierIssueRecoverySuccessor ==
    /\ frontier.phase = "CheckpointRead"
    /\ frontier.checkpointToken = frontier.sampledToken
    /\ frontier.candidateId = frontier.sampledThrough + 1
    /\ frontier.sampledThrough = frontier.committedThrough
    /\ FrontierCandidateMatchesObject
    /\ frontier' =
        [frontier EXCEPT
            !.phase = "FrontierIssued",
            !.issuedId = frontier.candidateId,
            !.issuedFence = frontier.currentFence,
            !.issuedKind = frontier.candidateKind,
            !.issuedToken = frontier.sampledToken,
            !.recoveryAdoptingSuccessor = TRUE]
    /\ UNCHANGED frontierWitness

FrontierCheckpointLinearizes(response) ==
    /\ response \in {"delivered", "lost"}
    /\ frontier.phase = "FrontierIssued"
    /\ frontier.checkpointToken = frontier.issuedToken
    /\ frontier.issuedId = frontier.committedThrough + 1
    /\ FrontierObjectIsExact(frontier.issuedId, frontier.issuedKind)
    /\ frontier.issuedFence \in FrontierFences
    /\ frontier' =
        [frontier EXCEPT
            !.phase = IF response = "lost" THEN "NeedsRecovery" ELSE "FrontierDurable",
            !.committedThrough = frontier.issuedId,
            !.lastEpochSeal =
                IF frontier.issuedKind = "seal" THEN frontier.issuedId ELSE @,
            !.checkpointToken = @ + 1,
            !.lostResponsePending = (response = "lost"),
            !.validFrontierProofs = @ \cup {frontier.issuedId}]
    /\ frontierWitness' =
        [frontierWitness EXCEPT
            !.issuedCasLinearizedAfterFenceMove =
                @ \/ (frontier.currentFence # frontier.issuedFence)]

FrontierInstall ==
    /\ frontier.phase = "FrontierDurable"
    /\ frontier.issuedKind = "log"
    /\ frontier.candidateId <= frontier.committedThrough
    /\ FrontierCandidateMatchesObject
    /\ frontier' =
        [frontier EXCEPT
            !.phase = "Installed",
            !.installedThrough = frontier.candidateId]
    /\ frontierWitness' =
        [frontierWitness EXCEPT
            !.exactSuccessorAdopted =
                @ \/ (frontier.recoveryAdoptingSuccessor
                       /\ frontier.objectFence[frontier.candidateId] # frontier.issuedFence)]

FrontierAcknowledge ==
    /\ frontier.phase = "Installed"
    /\ frontier.installedThrough = frontier.candidateId
    /\ frontier.candidateId <= frontier.committedThrough
    /\ frontier' =
        [frontier EXCEPT
            !.phase = "Acknowledged",
            !.acknowledgedThrough = frontier.candidateId,
            !.nextId = frontier.candidateId + 1]
    /\ UNCHANGED frontierWitness

FrontierFinish ==
    /\ frontier.phase \in {"Acknowledged", "Rejected"}
    /\ frontier' =
        [frontier EXCEPT
            !.phase = "Idle",
            !.candidateId = 0,
            !.candidateFence = 0,
            !.candidateKind = "none",
            !.issuedId = 0,
            !.issuedFence = 0,
            !.issuedKind = "none",
            !.issuedToken = 0,
            !.sampledThrough = 0,
            !.sampledToken = 0,
            !.lostResponsePending = FALSE,
            !.recoveryAdoptingSuccessor = FALSE]
    /\ UNCHANGED frontierWitness

(* A crash loses only runtime state. Durable objects and the checkpoint remain. *)
FrontierCrash ==
    /\ frontier.phase \in {"Prepared", "LogDurable", "FrontierDurable", "Installed"}
    /\ frontier' =
        [frontier EXCEPT
            !.phase = "NeedsRecovery",
            !.installedThrough = frontier.acknowledgedThrough]
    /\ frontierWitness' =
        [frontierWitness EXCEPT
            !.crashPrepared = @ \/ (frontier.phase = "Prepared"),
            !.crashLogDurable = @ \/ (frontier.phase = "LogDurable"),
            !.crashFrontierDurable = @ \/ (frontier.phase = "FrontierDurable"),
            !.crashInstalled = @ \/ (frontier.phase = "Installed")]

(* Every ambiguous/lost-response recovery begins with a real exact checkpoint read. The sampled
   token must still match before either install or a successor contribution is allowed. *)
FrontierReadCheckpoint ==
    /\ frontier.phase = "NeedsRecovery"
    /\ frontier' =
        [frontier EXCEPT
            !.phase = "CheckpointRead",
            !.sampledThrough = frontier.committedThrough,
            !.sampledToken = frontier.checkpointToken]
    /\ UNCHANGED frontierWitness

FrontierRecoverAbsentAttempt ==
    /\ frontier.phase = "CheckpointRead"
    /\ frontier.checkpointToken = frontier.sampledToken
    /\ frontier.candidateId = frontier.sampledThrough + 1
    /\ frontier.objectKind[frontier.candidateId] = "none"
    /\ frontier' =
        [frontier EXCEPT
            !.phase = "Idle",
            !.candidateId = 0,
            !.candidateFence = 0,
            !.candidateKind = "none",
            !.issuedId = 0,
            !.issuedFence = 0,
            !.issuedKind = "none",
            !.issuedToken = 0,
            !.sampledThrough = 0,
            !.sampledToken = 0,
            !.lostResponsePending = FALSE,
            !.recoveryAdoptingSuccessor = FALSE]
    /\ UNCHANGED frontierWitness

FrontierValidateDurableCandidate ==
    /\ frontier.phase = "CheckpointRead"
    /\ frontier.checkpointToken = frontier.sampledToken
    /\ frontier.candidateId # 0
    /\ frontier.candidateId <= frontier.sampledThrough
    /\ FrontierCandidateMatchesObject
    /\ frontier' = [frontier EXCEPT !.phase = "RecoveryValidated"]
    /\ UNCHANGED frontierWitness

FrontierInstallValidated ==
    /\ frontier.phase = "RecoveryValidated"
    /\ frontier.checkpointToken = frontier.sampledToken
    /\ frontier.candidateId <= frontier.sampledThrough
    /\ FrontierCandidateMatchesObject
    /\ frontier' =
        [frontier EXCEPT
            !.phase = "Installed",
            !.installedThrough = frontier.candidateId,
            !.lostResponsePending = FALSE]
    /\ frontierWitness' =
        [frontierWitness EXCEPT
            !.lostFrontierResponse =
                @ \/ (frontier.lostResponsePending
                       /\ frontier.recoveryAdoptingSuccessor
                       /\ frontier.objectFence[frontier.candidateId] # frontier.issuedFence),
            !.exactSuccessorAdopted =
                @ \/ (frontier.recoveryAdoptingSuccessor
                       /\ frontier.objectFence[frontier.candidateId] # frontier.issuedFence)]

(* The successor's seal first wins the conditional-create slot. Its CURRENT-fence checkpoint CAS is
   a separate request; that request atomically publishes both `lastEpochSeal` and the frontier and
   may itself lose its response. *)
FrontierSuccessorSealCreate ==
    /\ frontier.phase = "Prepared"
    /\ frontier.currentFence = 2
    /\ frontier.candidateFence = 1
    /\ frontier.candidateId = frontier.committedThrough + 1
    /\ frontier.objectKind[frontier.candidateId] = "none"
    /\ frontier' =
        [frontier EXCEPT
            !.phase = "SealDurable",
            !.objectKind[frontier.candidateId] = "seal",
            !.objectFence[frontier.candidateId] = frontier.currentFence,
            !.objectPrev[frontier.candidateId] = frontier.committedThrough]
    /\ UNCHANGED frontierWitness

FrontierIssueSuccessorSeal ==
    /\ frontier.phase = "SealDurable"
    /\ frontier.candidateId = frontier.committedThrough + 1
    /\ FrontierObjectIsExact(frontier.candidateId, "seal")
    /\ frontier.objectFence[frontier.candidateId] = frontier.currentFence
    /\ frontier' =
        [frontier EXCEPT
            !.phase = "FrontierIssued",
            !.issuedId = frontier.candidateId,
            !.issuedFence = frontier.currentFence,
            !.issuedKind = "seal",
            !.issuedToken = frontier.checkpointToken,
            !.recoveryAdoptingSuccessor = FALSE]
    /\ UNCHANGED frontierWitness

FrontierFinishSealPublication ==
    /\ frontier.phase = "FrontierDurable"
    /\ frontier.issuedKind = "seal"
    /\ frontier.lastEpochSeal = frontier.issuedId
    /\ frontier.committedThrough = frontier.issuedId
    /\ frontier' = [frontier EXCEPT !.phase = "Rejected"]
    /\ frontierWitness' =
        [frontierWitness EXCEPT !.sealPublishedAtFrontier = TRUE]

FrontierResolveLostSeal ==
    /\ frontier.phase = "CheckpointRead"
    /\ frontier.lostResponsePending
    /\ frontier.issuedKind = "seal"
    /\ frontier.checkpointToken = frontier.sampledToken
    /\ frontier.sampledThrough = frontier.candidateId
    /\ frontier.lastEpochSeal = frontier.candidateId
    /\ FrontierObjectIsExact(frontier.candidateId, "seal")
    /\ frontier' =
        [frontier EXCEPT
            !.phase = "Rejected",
            !.lostResponsePending = FALSE]
    /\ frontierWitness' =
        [frontierWitness EXCEPT
            !.sealPublishedAtFrontier = TRUE,
            !.lostSealResponseResolved = TRUE]

FrontierOldWriterLosesToSeal ==
    /\ frontier.phase \in {"FrontierDurable", "NeedsRecovery", "CheckpointRead", "Rejected"}
    /\ frontier.candidateKind = "log"
    /\ frontier.objectKind[frontier.candidateId] = "seal"
    /\ frontier.objectFence[frontier.candidateId] = frontier.currentFence
    /\ frontier.candidateFence # frontier.currentFence
    /\ frontier.committedThrough = frontier.candidateId
    /\ frontier.lastEpochSeal = frontier.candidateId
    /\ UNCHANGED frontier
    /\ frontierWitness' =
        [frontierWitness EXCEPT !.oldWriterLostToSeal = TRUE]

FrontierPublishSnapshotBody ==
    /\ frontier.snapshotBodyThrough < frontier.committedThrough
    /\ frontier.committedThrough # frontier.lastEpochSeal
    /\ frontier' =
        [frontier EXCEPT !.snapshotBodyThrough = frontier.committedThrough]
    /\ UNCHANGED frontierWitness

FrontierPublishSnapshotCheckpoint ==
    /\ frontier.checkpointSnapshot < frontier.snapshotBodyThrough
    /\ frontier.snapshotBodyThrough <= frontier.committedThrough
    /\ frontier' =
        [frontier EXCEPT
            !.checkpointSnapshot = frontier.snapshotBodyThrough,
            !.checkpointToken = @ + 1]
    /\ frontierWitness' =
        [frontierWitness EXCEPT !.snapshotPublishedAtFrontier = TRUE]

(* ---- Task 5b load-bearing sabotages ---- *)

FrontierAckBeforeCheckpoint ==
    /\ SabotageAckBeforeFrontier
    /\ frontier.phase = "LogDurable"
    /\ frontier' =
        [frontier EXCEPT !.acknowledgedThrough = frontier.candidateId]
    /\ UNCHANGED frontierWitness

FrontierAllocateBeforeCheckpoint ==
    /\ SabotageAllocateBeforeFrontier
    /\ frontier.phase = "LogDurable"
    /\ frontier' = [frontier EXCEPT !.nextId = frontier.candidateId + 1]
    /\ UNCHANGED frontierWitness

FrontierInstallUncommitted ==
    /\ SabotageInstallAboveFrontier
    /\ frontier.phase = "LogDurable"
    /\ frontier' =
        [frontier EXCEPT !.installedThrough = frontier.candidateId]
    /\ UNCHANGED frontierWitness

FrontierStaleAdvance ==
    /\ SabotageStaleFrontier
    /\ frontier.phase = "Prepared"
    /\ frontier.currentFence # frontier.candidateFence
    /\ frontier.candidateId = frontier.committedThrough + 1
    /\ frontier.objectKind[frontier.candidateId] = "none"
    /\ frontier' =
        [frontier EXCEPT !.committedThrough = frontier.candidateId]
    /\ UNCHANGED frontierWitness

FrontierSnapshotAboveCheckpoint ==
    /\ SabotageSnapshotAboveFrontier
    /\ frontier.committedThrough < MaxSeq
    /\ frontier' =
        [frontier EXCEPT !.checkpointSnapshot = frontier.committedThrough + 1]
    /\ UNCHANGED frontierWitness

FrontierSealAboveCheckpoint ==
    /\ SabotageSealAboveFrontier
    /\ frontier.committedThrough < MaxSeq
    /\ frontier' =
        [frontier EXCEPT !.lastEpochSeal = frontier.committedThrough + 1]
    /\ UNCHANGED frontierWitness

FrontierSnapshotAtSeal ==
    /\ SabotageSnapshotAtSeal
    /\ frontier.lastEpochSeal # 0
    /\ frontier.checkpointSnapshot # frontier.lastEpochSeal
    /\ frontier' =
        [frontier EXCEPT
            !.checkpointSnapshot = frontier.lastEpochSeal,
            !.checkpointToken = @ + 1]
    /\ UNCHANGED frontierWitness

(* ---- CAS-366 epoch slice: a zombie append lands in a reclaimed seal slot ----

   Base `2a034cdb374`. Actors: the zombie lane of epoch 1 (`Z`), honest successor mounts of epochs
   2..EpochCount, and GC ref cleanup. The zombie's lease is taken over at any time (`Takeover`).
   `T1Holds` decides whether `op.admitted()` tracks the lease (TRUE) or is a free choice — the
   failure of the boot-clock lease check after a SIGSTOP-class pause (audit R4/R5, `T1`).

   The defect: Z passes `gate`, pauses; the successor seals Z's slot `(1,k)`, appends twice in its
   own epoch and checkpoints a snapshot at `(2,2)`; GC deletes the seal (below the checkpoint, not
   the base's predecessor seal); Z's `If-None-Match` create lands in the empty key; its contribution
   `{committed_through = (1,k)}` merges to the durable body (`IdenticalSkip`), and only admission
   stands between Z and acknowledging a record that logical history (`epHist`) holds as a SEAL.

   Variants (`EpochVariant`):
     "cur"   the code at `2a034cdb374`.
     "fixA"  `publishCkpt` refuses a non-seal contribution whose `committed_through` equals the
             durable `last_epoch_seal` (`SuccessorSeal`).
     "sabA"  fixA compared against the writer's cached `rt->last_epoch_seal` instead of the durable one.
     "fixA2" fixA plus: a non-seal contribution from an epoch BELOW the durable frontier's epoch is
             acknowledged only if the durable seal closes that same epoch strictly above it;
             otherwise the outcome is unresolved (not acknowledged, not rejected). As in the code,
             the refusal runs on EVERY publish: the successors' adoptions, appends and snapshots
             (whose contribution carries `committed_through = checkpoint_snapshot_id`) too.
     "sabA2le" fixA2 with the epoch test `<=` instead of `<`: refuses honest successors.
     "fixB"  `planRefCleanup` keeps the log named by the plan sample's `last_epoch_seal`.
     "sabB"  fixB with `last_epoch_seal` taken from an earlier, separate `_ckpt` read.
     "fixB2" `planRefCleanup` keeps the newest object of every epoch below the frontier's epoch.
     "fixB3" fixB2 limited to the epoch immediately below the frontier's epoch: two epochs below,
             the zombie's contribution already fails `mergeCommittedThrough`.
     "sabB3" fixB3 skipped when the frontier is itself a seal ("the older seal is obsolete"). *)

storeE == << epObj, epLink, epHist, epCkpt, epSnapBody, epLive >>
zVarsE == << zPh, zSeq, zOut, zAcked, zAckLate >>
mVarsE == << mPh, mTgt, mApp, mSeal, sAcked >>
gVarsE == << gPh, gSnap, gCur, gSeal, gProof >>

(* `op.admitted()`: under T1 it is exactly "this epoch holds the lease"; without T1 it can say
   either. *)
Admits(m) == IF T1Holds THEN {epLive = m} ELSE {TRUE, FALSE}

(* `mergeCommittedThrough` (Pool/CasRefCkpt.cpp). Two frontiers in different epochs merge only if
   the higher side is the next epoch and its seal is at or above the lower frontier. *)
MergeCt(d, c) ==
    IF d.ct = 0 THEN [ok |-> TRUE, v |-> c.ct]
    ELSE IF c.ct = 0 THEN [ok |-> TRUE, v |-> d.ct]
    ELSE IF Ep(d.ct) = Ep(c.ct) THEN [ok |-> TRUE, v |-> Max2(d.ct, c.ct)]
    ELSE LET hi == IF d.ct > c.ct THEN d ELSE c
             lo == IF d.ct > c.ct THEN c ELSE d
         IN IF \/ Ep(lo.ct) + 1 # Ep(hi.ct)
               \/ hi.seal = 0
               \/ hi.seal < lo.ct
               \/ hi.ct < hi.seal
            THEN [ok |-> FALSE, v |-> 0]
            ELSE [ok |-> TRUE, v |-> hi.ct]

(* `checkRefCkptInvariants` (Formats/CasRefCkptFormat.cpp), with `life_epoch` = 1. *)
CkptValid(k) ==
    IF k.ct = 0 THEN k.seal = 0 /\ k.snap = 0
    ELSE /\ k.snap <= k.ct
         /\ k.seal <= k.ct
         /\ (k.seal # 0 => (k.seal = k.ct \/ Ep(k.seal) + 1 = Ep(k.ct)))
         /\ (k.seal = 0 => Ep(k.ct) = 1)

(* `mergeCkpt`: per-field semantic maximum, then the shape check. `ok = FALSE` is the
   `CORRUPTED_DATA` throw. *)
EMerge(d, c) ==
    LET mc == MergeCt(d, c)
        r == [ct |-> mc.v, seal |-> Max2(d.seal, c.seal), snap |-> Max2(d.snap, c.snap)]
    IN [ok |-> mc.ok /\ CkptValid(r), r |-> r]

(* Logical history grows exactly when a `_ckpt` CAS moves `committed_through` onto an object. *)
HistAfter(d, r, kind) == IF r.ct # d.ct THEN [epHist EXCEPT ![r.ct] = kind] ELSE epHist

ZTgt == EId(1, zSeq + 1)
ZCachedSeal == 0                     \* the zombie's `rt->last_epoch_seal`: it never read a seal

A2Variants == {"fixA2", "sabA2le"}

(* `classifyFrontierAgainstDurable` (Pool/CasRefCkpt.cpp). *)
A2Refusal(d, c) ==
    IF c.seal # c.ct /\ d.seal # 0 /\ c.ct = d.seal THEN "closed"
    ELSE IF /\ c.seal # c.ct
            /\ d.ct # 0
            /\ IF EpochVariant = "sabA2le" THEN Ep(c.ct) <= Ep(d.ct) ELSE Ep(c.ct) < Ep(d.ct)
            /\ ~(d.seal # 0 /\ Ep(d.seal) = Ep(c.ct) /\ c.ct < d.seal)
         THEN "unresolved"
         ELSE "none"

(* The decide-time refusal each variant adds to `publishCkpt`, before the merge. *)
ZRefusal(d, c) ==
    CASE EpochVariant = "fixA" ->
            IF c.seal # c.ct /\ d.seal # 0 /\ c.ct = d.seal THEN "closed" ELSE "none"
      [] EpochVariant = "sabA" ->
            IF c.seal # c.ct /\ ZCachedSeal # 0 /\ c.ct = ZCachedSeal THEN "closed" ELSE "none"
      [] EpochVariant \in A2Variants -> A2Refusal(d, c)
      [] OTHER -> "none"

(* The refusal the code applies to every publish, the successors' included. *)
PubRefusal(d, c) == IF EpochVariant \in A2Variants THEN A2Refusal(d, c) ELSE "none"

(* CasRefLedger::flushRefBatch / prepareRefChunk: derive the next id from state (in memory). *)
ZPrepare ==
    /\ zPh = "idle"
    /\ zSeq < ZSeqMax
    /\ zPh' = "prepared"
    /\ UNCHANGED << storeE, zSeq, zOut, zAcked, zAckLate, mVarsE, gVarsE >>

(* CasOperation::writeLoop: `gate(reservedFor(0, 2))` before each attempt. `resolveWedgeOnce`
   re-enters the same gate for its one retry of the same key and bytes (R5). *)
ZGate ==
    /\ zPh \in {"prepared", "wedged"}
    /\ \E ok \in Admits(1) : zPh' = IF ok THEN "gated" ELSE "fenced"
    /\ UNCHANGED << storeE, zSeq, zOut, zAcked, zAckLate, mVarsE, gVarsE >>

(* The store applies the `If-None-Match` create: landing step. Arbitrarily late after `ZGate`. *)
ZLand ==
    /\ zPh = "gated"
    /\ IF epObj[ZTgt] = "none"
       THEN /\ epObj' = [epObj EXCEPT ![ZTgt] = "log"]
            /\ epLink' = [epLink EXCEPT ![ZTgt] = 0]
            /\ zOut' = "created"
       ELSE /\ zOut' = "occupied"
            /\ UNCHANGED << epObj, epLink >>
    /\ zPh' = "inflight"
    /\ UNCHANGED << epHist, epCkpt, epSnapBody, epLive, zSeq, zAcked, zAckLate, mVarsE, gVarsE >>

(* The request never reached the store. *)
ZDrop ==
    /\ zPh = "gated"
    /\ zPh' = "inflight"
    /\ zOut' = "notlanded"
    /\ UNCHANGED << storeE, zSeq, zAcked, zAckLate, mVarsE, gVarsE >>

(* Response step: delivered. A timeout without a landing is ambiguous to the writer: wedge. *)
ZRespond ==
    /\ zPh = "inflight"
    /\ zPh' = CASE zOut = "created" -> "durable"
                [] zOut = "occupied" -> "settle"
                [] OTHER -> "wedged"
    /\ zOut' = "none"
    /\ UNCHANGED << storeE, zSeq, zAcked, zAckLate, mVarsE, gVarsE >>

(* Response step: lost after the store applied the request. *)
ZRespondLost ==
    /\ zPh = "inflight"
    /\ zOut \in {"created", "occupied"}
    /\ zPh' = "wedged"
    /\ zOut' = "none"
    /\ UNCHANGED << storeE, zSeq, zAcked, zAckLate, mVarsE, gVarsE >>

(* commitRefChunk / resolveWedgeOnce: the settling GET after `412`, `classifyRefLogOccupant`.
   A seal is a conclusive rejection (`Closed`); our own bytes are durable; nothing is still wedged. *)
ZSettle ==
    /\ zPh = "settle"
    /\ zPh' = CASE epObj[ZTgt] = "seal" -> "closed"
                [] epObj[ZTgt] = "log" -> "durable"
                [] OTHER -> "wedged"
    /\ UNCHANGED << storeE, zSeq, zOut, zAcked, zAckLate, mVarsE, gVarsE >>

(* publishCkpt (Pool/CasRefCkpt.cpp) for `commit_contribution` (commitRefChunk) or the wedge
   frontier (resolveWedgeOnce): ONE readModifyWrite. A merge to the durable body is `IdenticalSkip`
   if admitted; a decline or gate refusal while not admitted is `FencedOut` -> NeedsRecovery. *)
ZPublish ==
    /\ zPh = "durable"
    /\ LET c == [ct |-> ZTgt, seal |-> 0, snap |-> 0]
           d == epCkpt
           mg == EMerge(d, c)
           refusal == ZRefusal(d, c)
       IN \E ok \in Admits(1) :
            IF refusal # "none"
            THEN /\ zPh' = refusal
                 /\ UNCHANGED << epCkpt, epHist >>
            ELSE IF ~mg.ok \/ ~ok
            THEN /\ zPh' = "needsrec"
                 /\ UNCHANGED << epCkpt, epHist >>
            ELSE /\ zPh' = "published"
                 /\ epCkpt' = mg.r
                 /\ epHist' = HistAfter(d, mg.r, "log")
    /\ UNCHANGED << epObj, epLink, epSnapBody, epLive, zSeq, zOut, zAcked, zAckLate, mVarsE, gVarsE >>

(* commitRefChunk: `refuseUnlessAdmitted` inside the `state_mutex` hold, then the install. *)
ZInstall ==
    /\ zPh = "published"
    /\ \E ok \in Admits(1) :
        IF ok THEN /\ zPh' = "installed"
                   /\ zSeq' = zSeq + 1
              ELSE /\ zPh' = "needsrec"
                   /\ UNCHANGED zSeq
    /\ UNCHANGED << storeE, zOut, zAcked, zAckLate, mVarsE, gVarsE >>

(* commitRefChunk: `complete_*` wakes the callers — the acknowledgment. *)
ZAck ==
    /\ zPh = "installed"
    /\ zAcked' = zAcked \cup {EId(1, zSeq)}
    /\ zAckLate' = (zAckLate \/ epLive # 1)
    /\ zPh' = "idle"
    /\ UNCHANGED << storeE, zSeq, zOut, mVarsE, gVarsE >>

(* claimMount: the next epoch takes the lease. The superseded successor stops (honest, T1 for it). *)
Takeover ==
    /\ epLive < EpochCount
    /\ epLive' = epLive + 1
    /\ mPh' = [m \in Succs |-> IF m = epLive + 1 THEN "ckpt"
                               ELSE IF m = epLive THEN "stopped" ELSE mPh[m]]
    /\ UNCHANGED << epObj, epLink, epHist, epCkpt, epSnapBody, zVarsE,
                    mTgt, mApp, mSeal, sAcked, gVarsE >>

MLive(m) == epLive = m

(* runRecoveryWalkOnce: readCkpt + chooseRecoveryGrounding. The walk starts after the frontier;
   a frontier that is a seal continues at the next epoch's first sequence. *)
MReadCkpt(m) ==
    /\ mPh[m] = "ckpt"
    /\ MLive(m)
    /\ mTgt' = [mTgt EXCEPT ![m] =
                  IF epCkpt.ct = 0 THEN EId(1, 1)
                  ELSE IF epCkpt.ct = epCkpt.seal THEN EId(Ep(epCkpt.ct) + 1, 1)
                  ELSE epCkpt.ct + 1]
    /\ mSeal' = [mSeal EXCEPT ![m] = epCkpt.seal]
    /\ mPh' = [mPh EXCEPT ![m] = "walk"]
    /\ UNCHANGED << storeE, zVarsE, mApp, sAcked, gVarsE >>

(* runRecoveryWalkOnce: exact GET of the arithmetic successor in a dead epoch. *)
MWalkGet(m) ==
    /\ mPh[m] = "walk"
    /\ MLive(m)
    /\ Ep(mTgt[m]) < m
    /\ Sq(mTgt[m]) \in 1..SMax
    /\ mPh' = [mPh EXCEPT ![m] = CASE epObj[mTgt[m]] = "log" -> "adoptlog"
                                   [] epObj[mTgt[m]] = "seal" -> "adoptseal"
                                   [] OTHER -> "sealing"]
    /\ UNCHANGED << storeE, zVarsE, mTgt, mApp, mSeal, sAcked, gVarsE >>

(* runRecoveryWalkOnce: the walk reached its own epoch; the table is installed. *)
MWalkDone(m) ==
    /\ mPh[m] = "walk"
    /\ MLive(m)
    /\ Ep(mTgt[m]) >= m
    /\ mPh' = [mPh EXCEPT ![m] = "ready"]
    /\ UNCHANGED << storeE, zVarsE, mTgt, mApp, mSeal, sAcked, gVarsE >>

(* runRecoveryWalkOnce: `publish_recovered_frontier` for a walked record or seal. *)
MAdoptPub(m) ==
    /\ mPh[m] \in {"adoptlog", "adoptseal"}
    /\ MLive(m)
    /\ LET kind == IF mPh[m] = "adoptlog" THEN "log" ELSE "seal"
           t == mTgt[m]
           c == [ct |-> t, seal |-> IF kind = "seal" THEN t ELSE epLink[t], snap |-> 0]
           mg == EMerge(epCkpt, c)
       IN IF PubRefusal(epCkpt, c) # "none"
          THEN /\ mPh' = [mPh EXCEPT ![m] = "refused"]
               /\ UNCHANGED << epCkpt, epHist, mTgt, mSeal >>
          ELSE IF mg.ok
          THEN /\ epCkpt' = mg.r
               /\ epHist' = HistAfter(epCkpt, mg.r, kind)
               /\ mTgt' = [mTgt EXCEPT ![m] = IF kind = "seal" THEN EId(Ep(t) + 1, 1) ELSE t + 1]
               /\ mSeal' = IF kind = "seal" THEN [mSeal EXCEPT ![m] = t] ELSE mSeal
               /\ mPh' = [mPh EXCEPT ![m] = "walk"]
          ELSE /\ mPh' = [mPh EXCEPT ![m] = "failed"]
               /\ UNCHANGED << epCkpt, epHist, mTgt, mSeal >>
    /\ UNCHANGED << epObj, epLink, epSnapBody, epLive, zVarsE, mApp, sAcked, gVarsE >>

(* runRecoveryWalkOnce: `op.create(seal, Retry::once())` into the absent slot. `412` re-reads
   the occupant through the walk. Landing and response are fused (see RESULTS, Atomicity). *)
MSealCreate(m) ==
    /\ mPh[m] = "sealing"
    /\ MLive(m)
    /\ LET t == mTgt[m] IN
       IF epObj[t] = "none"
       THEN /\ epObj' = [epObj EXCEPT ![t] = "seal"]
            /\ epLink' = [epLink EXCEPT ![t] = mSeal[m]]
            /\ mPh' = [mPh EXCEPT ![m] = "sealpub"]
       ELSE /\ mPh' = [mPh EXCEPT ![m] = "walk"]
            /\ UNCHANGED << epObj, epLink >>
    /\ UNCHANGED << epHist, epCkpt, epSnapBody, epLive, zVarsE, mTgt, mApp, mSeal, sAcked, gVarsE >>

(* runRecoveryWalkOnce: `publish_recovered_frontier(seal_txn)` = {ct = seal, seal = seal}. *)
MSealPub(m) ==
    /\ mPh[m] = "sealpub"
    /\ MLive(m)
    /\ LET t == mTgt[m]
           c == [ct |-> t, seal |-> t, snap |-> 0]
           mg == EMerge(epCkpt, c)
       IN IF PubRefusal(epCkpt, c) # "none"
          THEN /\ mPh' = [mPh EXCEPT ![m] = "refused"]
               /\ UNCHANGED << epCkpt, epHist, mTgt, mSeal >>
          ELSE IF mg.ok
          THEN /\ epCkpt' = mg.r
               /\ epHist' = HistAfter(epCkpt, mg.r, "seal")
               /\ mTgt' = [mTgt EXCEPT ![m] = EId(Ep(t) + 1, 1)]
               /\ mSeal' = [mSeal EXCEPT ![m] = t]
               /\ mPh' = [mPh EXCEPT ![m] = "walk"]
          ELSE /\ mPh' = [mPh EXCEPT ![m] = "failed"]
               /\ UNCHANGED << epCkpt, epHist, mTgt, mSeal >>
    /\ UNCHANGED << epObj, epLink, epSnapBody, epLive, zVarsE, mApp, sAcked, gVarsE >>

(* commitRefChunk: the successor's own `If-None-Match` append; `(m,1)` carries the chain link. *)
MAppendLog(m) ==
    /\ mPh[m] = "ready"
    /\ MLive(m)
    /\ mApp[m] < SAppMax
    /\ LET t == EId(m, mApp[m] + 1) IN
       /\ epObj[t] = "none"
       /\ epObj' = [epObj EXCEPT ![t] = "log"]
       /\ epLink' = [epLink EXCEPT ![t] = IF mApp[m] = 0 THEN mSeal[m] ELSE 0]
    /\ mPh' = [mPh EXCEPT ![m] = "appendpub"]
    /\ UNCHANGED << epHist, epCkpt, epSnapBody, epLive, zVarsE, mTgt, mApp, mSeal, sAcked, gVarsE >>

(* commitRefChunk: frontier publication, install and acknowledgment of the honest successor.
   Fused: the successor is not the subject and holds the lease (see RESULTS, Atomicity). *)
MAppendPub(m) ==
    /\ mPh[m] = "appendpub"
    /\ MLive(m)
    /\ LET t == EId(m, mApp[m] + 1)
           c == [ct |-> t, seal |-> epLink[t], snap |-> 0]
           mg == EMerge(epCkpt, c)
       IN IF PubRefusal(epCkpt, c) # "none"
          THEN /\ mPh' = [mPh EXCEPT ![m] = "refused"]
               /\ UNCHANGED << epCkpt, epHist, sAcked, mApp >>
          ELSE IF mg.ok
          THEN /\ epCkpt' = mg.r
               /\ epHist' = HistAfter(epCkpt, mg.r, "log")
               /\ sAcked' = sAcked \cup {t}
               /\ mApp' = [mApp EXCEPT ![m] = @ + 1]
               /\ mPh' = [mPh EXCEPT ![m] = "ready"]
          ELSE /\ mPh' = [mPh EXCEPT ![m] = "failed"]
               /\ UNCHANGED << epCkpt, epHist, sAcked, mApp >>
    /\ UNCHANGED << epObj, epLink, epSnapBody, epLive, zVarsE, mTgt, mSeal, gVarsE >>

(* tryPublishSnapshotAndAdvanceCheckpointOnce: the snapshot body PUT at the greatest applied
   record, which is a log of the mount's own epoch (never a seal). *)
MSnapBody(m) ==
    /\ mPh[m] = "ready"
    /\ MLive(m)
    /\ mApp[m] >= 1
    /\ epSnapBody = 0
    /\ EId(m, mApp[m]) > epCkpt.snap
    /\ epSnapBody' = EId(m, mApp[m])
    /\ UNCHANGED << epObj, epLink, epHist, epCkpt, epLive, zVarsE, mVarsE, gVarsE >>

(* Same function: publishCkpt with `{checkpoint_snapshot_id}`. Unfused from the lease: the
   publisher may have been superseded since `MSnapBody`. Under A2 the contribution carries
   `committed_through = checkpoint_snapshot_id` as in the code; a refused or failed publish leaves
   the checkpoint where it was (`ckpt_advanced = false`), and a refusal while the publisher still
   holds the lease marks it `refused`. *)
MSnapCkpt ==
    /\ epSnapBody # 0
    /\ IF EpochVariant \in A2Variants
       THEN LET c == [ct |-> epSnapBody, seal |-> 0, snap |-> epSnapBody]
                mg == EMerge(epCkpt, c)
                refused == PubRefusal(epCkpt, c) # "none"
                pub == Ep(epSnapBody)
            IN /\ epCkpt' = IF ~refused /\ mg.ok THEN mg.r ELSE epCkpt
               /\ mPh' = IF refused /\ epLive = pub THEN [mPh EXCEPT ![pub] = "refused"] ELSE mPh
               /\ UNCHANGED << mTgt, mApp, mSeal, sAcked >>
       ELSE LET mg == EMerge(epCkpt, [ct |-> 0, seal |-> 0, snap |-> epSnapBody]) IN
            /\ mg.ok
            /\ epCkpt' = mg.r
            /\ UNCHANGED mVarsE
    /\ epSnapBody' = 0
    /\ UNCHANGED << epObj, epLink, epHist, epLive, zVarsE, gVarsE >>

(* sabB only: `last_epoch_seal` from an earlier, separate `_ckpt` read. *)
GcReadSeal ==
    /\ EpochVariant = "sabB"
    /\ gPh = "idle"
    /\ gSeal' = epCkpt.seal
    /\ gPh' = "sealread"
    /\ UNCHANGED << storeE, zVarsE, mVarsE, gSnap, gCur, gProof >>

(* CasGc ref cleanup: readCkpt + readCheckpointSnapshotBase (one sample): the checkpoint, the fold
   cursor (at most `committed_through`), and `retained_log_proof` = the base's `prev_epoch_seal`. *)
GcPlan ==
    /\ gPh = IF EpochVariant = "sabB" THEN "sealread" ELSE "idle"
    /\ epCkpt.snap # 0
    /\ gSnap' = epCkpt.snap
    /\ gCur' = epCkpt.ct
    /\ gSeal' = IF EpochVariant = "sabB" THEN gSeal ELSE epCkpt.seal
    /\ gProof' = epLink[epCkpt.snap]
    /\ gPh' = "planned"
    /\ UNCHANGED << storeE, zVarsE, mVarsE >>

NewestPresent(e) == MaxOr0({ j \in EIds : Ep(j) = e /\ epObj[j] # "none" })

(* planRefCleanup (Pool/CasRefProtocol.cpp) plus each variant's extra keep rule. *)
GcKeep(id) ==
    \/ id >= gSnap
    \/ id > gCur
    \/ id = gProof
    \/ (EpochVariant \in {"fixB", "sabB"} /\ id = gSeal)
    \/ (EpochVariant = "fixB2" /\ Ep(id) < Ep(gCur) /\ id = NewestPresent(Ep(id)))
    \/ (EpochVariant = "fixB3" /\ Ep(id) + 1 = Ep(gCur) /\ id = NewestPresent(Ep(id)))
    \/ (EpochVariant = "sabB3" /\ gCur # gSeal /\ Ep(id) + 1 = Ep(gCur) /\ id = NewestPresent(Ep(id)))

(* removeCohortWriteOnce: unconditional `DeleteObjects` of a planned key, one key per step. The
   LIST is taken to be fresh at the delete (a superset of the real behaviours). *)
GcDelete ==
    /\ gPh = "planned"
    /\ \E id \in EIds :
        /\ epObj[id] # "none"
        /\ ~GcKeep(id)
        /\ epObj' = [epObj EXCEPT ![id] = "none"]
    /\ UNCHANGED << epLink, epHist, epCkpt, epSnapBody, epLive, zVarsE, mVarsE, gVarsE >>

GcEnd ==
    /\ gPh = "planned"
    /\ gPh' = "idle"
    /\ UNCHANGED << storeE, zVarsE, mVarsE, gSnap, gCur, gSeal, gProof >>

EpochStep ==
    \/ ZPrepare \/ ZGate \/ ZLand \/ ZDrop \/ ZRespond \/ ZRespondLost \/ ZSettle
    \/ ZPublish \/ ZInstall \/ ZAck
    \/ Takeover
    \/ \E m \in Succs :
        \/ MReadCkpt(m) \/ MWalkGet(m) \/ MWalkDone(m) \/ MAdoptPub(m)
        \/ MSealCreate(m) \/ MSealPub(m) \/ MAppendLog(m) \/ MAppendPub(m) \/ MSnapBody(m)
    \/ MSnapCkpt
    \/ GcReadSeal \/ GcPlan \/ GcDelete \/ GcEnd

(* Self-loop so bounded counters exhausting is not a TLC deadlock (house pattern). *)
NoOp == UNCHANGED vars

LegacyStep ==
    \/ WriterAppend \/ WAttemptAmbiguous \/ WResolveDurable
    \/ WResolveConclusiveReject \/ WResolveSealRejected \/ WOrphanLands
    \/ LatePredecessorPut
    \/ WriterPublishSnapshot \/ WriterCkptAdvance
    \/ GcCleanupLog \/ GcCleanupSnap
    \/ ReaderStart \/ RReadCkpt \/ RFetchBase
    \/ RWalkStep \/ RWalkVanish \/ RSealSlot \/ RAdoptStraggler
    \/ RScanStep \/ RScanInstall
    \/ ReaderReset

FrontierStep ==
    \/ FrontierStartLog \/ FrontierLogDurable \/ FrontierMoveFence
    \/ FrontierIssueCheckpoint
    \/ \E response \in {"delivered", "lost"} : FrontierCheckpointLinearizes(response)
    \/ FrontierInstall \/ FrontierAcknowledge \/ FrontierFinish \/ FrontierCrash
    \/ FrontierReadCheckpoint \/ FrontierRecoverAbsentAttempt
    \/ FrontierValidateDurableCandidate \/ FrontierInstallValidated
    \/ FrontierIssueRecoverySuccessor
    \/ FrontierSuccessorSealCreate \/ FrontierIssueSuccessorSeal
    \/ FrontierFinishSealPublication \/ FrontierResolveLostSeal
    \/ FrontierOldWriterLosesToSeal
    \/ FrontierPublishSnapshotBody \/ FrontierPublishSnapshotCheckpoint
    \/ FrontierAckBeforeCheckpoint \/ FrontierAllocateBeforeCheckpoint
    \/ FrontierInstallUncommitted \/ FrontierStaleAdvance
    \/ FrontierSnapshotAboveCheckpoint \/ FrontierSealAboveCheckpoint
    \/ FrontierSnapshotAtSeal

LegacyNext ==
    /\ ~FrontierSlice
    /\ ~EpochSlice
    /\ LegacyStep
    /\ UNCHANGED frontierVars
    /\ UNCHANGED epochVars

FrontierNext ==
    /\ FrontierSlice
    /\ FrontierStep
    /\ UNCHANGED legacyVars
    /\ UNCHANGED epochVars

EpochNext ==
    /\ EpochSlice
    /\ EpochStep
    /\ UNCHANGED legacyVars
    /\ UNCHANGED frontierVars

Next == LegacyNext \/ FrontierNext \/ EpochNext \/ NoOp

Spec == Init /\ [][Next]_vars

(* ---- invariants ---- *)

(* The installed state = checkpoint base + the ids the walk proved present above it. *)
Reconstruct ==
    LET base == IF rBase = 0 THEN EmptyState ELSE snapCov[rBase]
    IN FoldIds(base, rTail)

(* (1) Central recovery invariant (spec §4: acked => durable => dense => found):
   Replay(checkpoint base, walked tail) = Replay(full history). *)
INV_RECOVERY == (rPhase = "done") => (Reconstruct = WState)

(* (2) Recovery never fails: a vanished object is a restart signal and, at worst, `stuck`.
   `corrupt` is the fail-closed branch of the three-way revalidation, and INV-4's deletion gate
   makes it unreachable while the gate holds. *)
INV_NOFAIL == rPhase # "corrupt"

(* (3) INV-1: within the namespace, durable ids are dense 1..T. The in-flight slot is the only id
   that may be absent, and only until it resolves. *)
INV_DENSE ==
    \A i \in (writtenEver \cup sealedIds) :
        \A j \in 1..i : j \in (writtenEver \cup sealedIds) \/ j = pendingSlot

(* (4) INV-2: nothing lands in a slot the recovery occupied with a seal. This is the property the
   store's conditional create provides and the whole in-band epoch close rests on. *)
INV_NO_GHOST == \A i \in writtenEver : i \notin sealedIds

(* (5) INV-1's every-attempt rule: an operation whose failure was reported to its caller never
   becomes durable. Freeing an id whose attempt was ambiguous is exactly how that promise breaks. *)
INV_NO_PHANTOM == ~ phantomEver

(* Non-vacuity witness for `_witness_hintlie` (spec §1; the observed 0x1430c/0x1430d shape): an
   enumeration that runs to the end of the key space can still omit a log that is PRESENT, so a
   reader that folds the hint installs a state missing a record nothing ever deleted. EXPECTED TO
   VIOLATE — the violation is the evidence that the hidden-hole shape is in the explored space, not
   a defect. It is the reason `_sab_scanistruth` cannot be dismissed as "the reader merely missed a
   vanish": under this witness there is nothing to vanish. *)
W_NO_HINT_HOLE == ~ (rPhase = "done" /\ \E i \in logs : i > rBase /\ i \notin rTail)

(* Task 5b safety: durable logical history is exactly the contiguous, token-CAS-proved chain named
   by `_ckpt.committed_through`. Install, acknowledgement and successor allocation may trail that
   fact but can never lead it. *)
INV_EXACT_COMMITTED_FRONTIER ==
    ~FrontierSlice \/
    \A i \in 1..frontier.committedThrough :
        /\ frontier.objectKind[i] # "none"
        /\ frontier.objectFence[i] \in FrontierFences
        /\ frontier.objectPrev[i] = i - 1
        /\ i \in frontier.validFrontierProofs

INV_INSTALL_NOT_ABOVE_FRONTIER ==
    ~FrontierSlice \/ frontier.installedThrough <= frontier.committedThrough

INV_ACK_NOT_BEFORE_FRONTIER ==
    ~FrontierSlice \/ frontier.acknowledgedThrough <= frontier.committedThrough

INV_ACK_NOT_BEFORE_INSTALL ==
    ~FrontierSlice \/ frontier.acknowledgedThrough <= frontier.installedThrough

INV_NEXT_ID_NOT_BEFORE_FRONTIER ==
    ~FrontierSlice \/ frontier.nextId <= frontier.committedThrough + 1

INV_SNAPSHOT_NOT_ABOVE_FRONTIER ==
    ~FrontierSlice \/ frontier.checkpointSnapshot <= frontier.committedThrough

INV_SEAL_NOT_ABOVE_FRONTIER ==
    ~FrontierSlice \/ frontier.lastEpochSeal <= frontier.committedThrough

(* A snapshot is always a state-bearing record. An epoch seal only changes arithmetic geometry, so
   publishing it as a checkpoint base would require recovery to retain a durable kind bit or backwalk.
   The production guard consequently forbids it rather than making that special case persistent. *)
INV_SNAPSHOT_NOT_EPOCH_SEAL ==
    ~FrontierSlice \/ frontier.checkpointSnapshot = 0
        \/ frontier.objectKind[frontier.checkpointSnapshot] # "seal"

(* Reachability controls. Each is checked in its own witness config; violation means the honest
   crash/race window was reached while all safety invariants preceding it remained green. *)
W_CRASH_PREPARED == ~frontierWitness.crashPrepared
W_CRASH_LOG_DURABLE == ~frontierWitness.crashLogDurable
W_CRASH_FRONTIER_DURABLE == ~frontierWitness.crashFrontierDurable
W_CRASH_INSTALLED == ~frontierWitness.crashInstalled
W_LOST_FRONTIER_RESPONSE == ~frontierWitness.lostFrontierResponse
W_EXACT_SUCCESSOR_ADOPTED == ~frontierWitness.exactSuccessorAdopted
W_OLD_WRITER_LOST_TO_SEAL == ~frontierWitness.oldWriterLostToSeal
W_ISSUED_CAS_LINEARIZED_AFTER_FENCE_MOVE ==
    ~frontierWitness.issuedCasLinearizedAfterFenceMove
W_SNAPSHOT_PUBLISHED_AT_FRONTIER == ~frontierWitness.snapshotPublishedAtFrontier
W_SEAL_PUBLISHED_AT_FRONTIER == ~frontierWitness.sealPublishedAtFrontier
W_LOST_SEAL_RESPONSE_RESOLVED == ~frontierWitness.lostSealResponseResolved

(* ---- CAS-366 epoch slice invariants ---- *)

(* No acknowledged record is unreachable by recovery or fold: every id a caller was told is
   committed is a LOG in logical history — the chain `_ckpt.committed_through` certifies, which
   recovery walks above the checkpoint and every snapshot and GC fold below it covers. *)
INV_ACKED_REACHABLE == \A id \in zAcked \cup sAcked : epHist[id] = "log"

(* A conclusive rejection (`SuccessorSeal` -> `Closed`) is never told about a record that history
   holds. The fixes' refusal must not lie. *)
INV_REJECT_TRUTHFUL == zPh = "closed" => epHist[ZTgt] # "log"

(* The honest successors never hit the `_ckpt` corruption throw. *)
INV_SUCCESSOR_NO_CORRUPTION == \A m \in Succs : mPh[m] # "failed"

(* The A2 refusal never refuses a successor's own publish while it holds the lease. *)
INV_SUCCESSOR_NOT_REFUSED == \A m \in Succs : mPh[m] # "refused"

(* Reachability witnesses (a violation is the evidence). *)
W_SEAL_SLOT_RELANDED == ~\E id \in EIds : epHist[id] = "seal" /\ epObj[id] = "log"
W_STRAGGLER_ACKED_AFTER_TAKEOVER == ~zAckLate
W_ZOMBIE_UNRESOLVED == zPh # "unresolved"

EpochTypeOK ==
    /\ epObj \in [EIds -> EKinds]
    /\ epLink \in [EIds -> 0..EIdMax]
    /\ epHist \in [EIds -> EKinds]
    /\ epCkpt \in [ct : 0..EIdMax, seal : 0..EIdMax, snap : 0..EIdMax]
    /\ epSnapBody \in 0..EIdMax
    /\ epLive \in 1..EpochCount
    /\ zPh \in ZPhases
    /\ zSeq \in 0..ZSeqMax
    /\ zOut \in {"none", "created", "occupied", "notlanded"}
    /\ zAcked \subseteq EIds
    /\ zAckLate \in BOOLEAN
    /\ mPh \in [Succs -> MPhases]
    /\ mTgt \in [Succs -> 0..(EIdMax + EBase)]
    /\ mApp \in [Succs -> 0..SAppMax]
    /\ mSeal \in [Succs -> 0..EIdMax]
    /\ sAcked \subseteq EIds
    /\ gPh \in {"idle", "sealread", "planned"}
    /\ {gSnap, gCur, gSeal, gProof} \subseteq 0..EIdMax

TypeOK ==
    /\ op \in [Ids -> Ops]
    /\ writtenEver \subseteq Seqs
    /\ logs \subseteq writtenEver
    /\ publishedEver \subseteq writtenEver
    /\ snaps \subseteq publishedEver
    /\ snapCov \in [Ids -> [life : Lifes, committed : SUBSET Seqs, removeSeq : 0..MaxSeq]]
    /\ ckpt.base \in 0..MaxSeq
    /\ ckpt.seal \in 0..(MaxSeq + 1)
    /\ pendingSlot \in 0..MaxSeq
    /\ ambiguousEver \subseteq Seqs
    /\ sealedIds \subseteq Ids
    /\ concludedEver \subseteq Ids
    /\ burnedIds \subseteq Seqs
    /\ orphanIds \subseteq Seqs
    /\ phantomEver \in BOOLEAN
    /\ rPhase \in {"idle", "ckpt", "base", "walk", "scan", "done", "stuck", "corrupt"}
    /\ rBase \in 0..MaxSeq
    /\ rWalkPos \in 0..(MaxSeq + 1)
    /\ rTail \subseteq Seqs
    /\ rScanPos \in 0..MaxSeq
    /\ rSeenLogs \subseteq Seqs
    /\ rRestarts \in 0..MaxRestarts
    /\ frontier \in FrontierState
    /\ frontierWitness \in FrontierWitnessState

THEOREM Spec =>
    [](TypeOK /\ INV_RECOVERY /\ INV_NOFAIL /\ INV_DENSE /\ INV_NO_GHOST /\ INV_NO_PHANTOM
      /\ INV_EXACT_COMMITTED_FRONTIER /\ INV_INSTALL_NOT_ABOVE_FRONTIER
      /\ INV_ACK_NOT_BEFORE_FRONTIER /\ INV_ACK_NOT_BEFORE_INSTALL
      /\ INV_NEXT_ID_NOT_BEFORE_FRONTIER /\ INV_SNAPSHOT_NOT_ABOVE_FRONTIER
      /\ INV_SEAL_NOT_ABOVE_FRONTIER /\ INV_SNAPSHOT_NOT_EPOCH_SEAL)
=============================================================================
