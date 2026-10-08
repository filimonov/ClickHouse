--------------------------- MODULE CaDiskLifecycle ---------------------------
(***************************************************************************)
(* Pool lifecycle of a writable CAS mount, its ONE lease thread, the GC    *)
(* scheduler, and the `SYSTEM CONTENT ADDRESSED FORGET` protocol           *)
(* (`Pool::forgetDisk`).                                                   *)
(*                                                                         *)
(* Lifecycle: Live, Transient, and two absorbing states: Stopping (a loss  *)
(* observation, rows R9-R17 of the lease-series design) and Closed         *)
(* (FORGET, R18). Only an own `gc_fenced` body (R8) leads to a remount.    *)
(*                                                                         *)
(* The lease thread renews and reclaims in sequence (design 2026-10-01,    *)
(* "One loop"):                                                            *)
(*   until stopped or `LeaseTerminal`:                                     *)
(*     a remount is pending (reqGen > handledGen): reclaim for the         *)
(*       generation it snapshots, or fail and retry;                       *)
(*     otherwise renew: success sets the deadline; an own fenced body     *)
(*       trips the fence and requests a remount; a loss observation       *)
(*       enters Stopping.                                                  *)
(* `deadlineOk` abstracts "the fence deadline admits a write". One arming  *)
(* rule (`CanArm`): the fence is armed and Live reported only with no      *)
(* remount request pending, no stop and no terminal lifecycle. A reclaim   *)
(* arms when its claim admits a write; otherwise the next successful       *)
(* renewal arms in the same step that reports Live.                        *)
(*                                                                         *)
(* Entering Stopping publishes the terminal latch in the observing step;   *)
(* the process-stop request is a later step of the same thread. A second   *)
(* disk of the same process is reduced to its own observe and raise steps *)
(* over the shared once-flag.                                              *)
(*                                                                         *)
(* FORGET steps in `Pool::forgetDisk` order: mutex -> idempotent           *)
(* terminal short-circuit -> publish intent -> trip#1 -> GC stop           *)
(* (signal, join) -> lease thread stop (signal, join) -> trip#2 -> drain   *)
(* -> teardown (farewell only if drained and the renewer is Active) ->     *)
(* renewer reset -> Closed, first terminal wins.                           *)
(*                                                                         *)
(* Not modelled: time and retry spacing, the open's own readiness renewal, *)
(* the content of a claim or a renewal, the slot certificate, the fence    *)
(* generation counter, the shutdown that a process-stop request starts.    *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS
    EnableTrip2,       \* FORGET re-trips the fence after joining the lease thread
    DrainGate,         \* farewell only when the ref lanes provably drained
    EnableGcSelfExit,  \* the GC scheduler exits at a wake that sees a terminal pool
    RenewChecksIntent, \* a renewal never starts once the FORGET intent is published
    ReadinessGate,     \* a reclaim with a too-old claim reports Live only after a renewal
    LiveChecksPending, \* a renewal arms nothing while a remount request is pending
    ReclaimChecksPending, \* a reclaim arms nothing while a newer request is pending
    ArmChecksTerminal, \* nothing arms after a stop or with a terminal lifecycle
    AckSnapshot,       \* a reclaim acknowledges only the generation it snapshotted
    ReclaimTrips,      \* a reclaim's step 0 trips the fence, not only the lifecycle
    LossStops,         \* a loss observation enters Stopping; FALSE = trip and request a remount
    RenewOkChecksLatch,\* a renewal success arms nothing on a latched or Stopping pool
    ArmRechecksAtOpen, \* a reclaim re-checks the arming rule in the step that opens writes
    StopOnce,          \* the process-stop request sits under one process-wide once-flag
    MaxGen             \* bound on remount requests (state-space bound only)

TerminalSet == {"Stopping", "Closed"}
PStates == {"Live", "Transient"} \cup TerminalSet

VARIABLES
    pstate,         \* CasMountRuntime.pool_lifecycle
    intent,         \* the terminal latch: FORGET's published intent or a Stopping entry
    termPublished,  \* terminal_state_published
    lost,           \* MountFence.lost
    deadlineOk,     \* the fence deadline still admits a write
    keeper,         \* renewer: "Active" | "Terminal" | "StoppedClean" | "StoppedNoFarewell" | "None"
    farewell,       \* the clean-release marker was written
    drained,        \* "Unset" | "Yes" | "No" (drainRefLanesForShutdown)
    lpc,            \* lease thread: "Top" | "Renewing" | "InAttempt" | "Arming" | "Exited"
    stop,           \* workers_stop_requested
    reqGen,         \* remount_requested_generation
    handledGen,     \* remount_handled_generation
    snapGen,        \* the generation the latest reclaim snapshotted
    armReady,       \* the reclaim's arming decision, carried to the step that opens writes
    epoch,          \* writer epoch: one per installed incarnation
    gcsched,        \* "Running" | "Stopped" | "Destroyed"
    gcstopping,     \* CasGcScheduler.stopping
    round,          \* a GC round is in flight
    mutex,          \* lifecycle_mutex: "None" | "Forget"
    fpc,            \* FORGET pc
    d1stop,         \* this disk's process-stop request: "None" | "Pending" | "Raised"
    other,          \* the second disk: "Live" | "Latched" | "Done"
    stopRequests,   \* process-stop requests issued (the callback ran)
    naturalWon,     \* history: a loss observation published the terminal state
    everStopping,   \* history: Stopping was entered
    everClosed,     \* history: Closed was entered
    lossSeen,       \* history: a loss observation (R9-R17) happened on this disk
    stopByInterference, \* history: an interference report published Stopping
    pauseEpoch,     \* history: epoch at the last expiry of an armed Live lease, -1 if none
    liveByRenewal   \* history: a renewal armed the fence and reported Live

vars == <<pstate, intent, termPublished, lost, deadlineOk, keeper, farewell,
          drained, lpc, stop, reqGen, handledGen, snapGen, armReady, epoch,
          gcsched, gcstopping, round, mutex, fpc, d1stop, other, stopRequests,
          naturalWon, everStopping, everClosed, lossSeen, stopByInterference,
          pauseEpoch, liveByRenewal>>

leaseVars == <<lpc, reqGen, handledGen, snapGen, armReady, epoch, liveByRenewal>>
gcVars == <<gcsched, gcstopping, round>>
forgetVars == <<mutex, fpc, drained, farewell, stop>>
stopVars == <<d1stop, other, stopRequests>>
historyVars == <<naturalWon, everStopping, everClosed, lossSeen,
                 stopByInterference, pauseEpoch>>

FpcStates == {"NotStarted", "Check", "ShortJoin", "Intent", "Trip1",
              "GcStopSignal", "GcStopJoin", "JoinSignal", "JoinWait",
              "Trip2", "Drain", "Teardown", "KeeperReset", "Enter", "Done"}

TypeOK ==
    /\ pstate \in PStates
    /\ intent \in BOOLEAN /\ termPublished \in BOOLEAN /\ lost \in BOOLEAN
    /\ deadlineOk \in BOOLEAN
    /\ keeper \in {"Active", "Terminal", "StoppedClean", "StoppedNoFarewell", "None"}
    /\ farewell \in BOOLEAN
    /\ drained \in {"Unset", "Yes", "No"}
    /\ lpc \in {"Top", "Renewing", "InAttempt", "Arming", "Exited"}
    /\ stop \in BOOLEAN
    /\ reqGen \in 0..MaxGen /\ handledGen \in 0..MaxGen /\ snapGen \in 0..MaxGen
    /\ armReady \in BOOLEAN /\ epoch \in 0..MaxGen
    /\ gcsched \in {"Running", "Stopped", "Destroyed"}
    /\ gcstopping \in BOOLEAN /\ round \in BOOLEAN
    /\ mutex \in {"None", "Forget"}
    /\ fpc \in FpcStates
    /\ d1stop \in {"None", "Pending", "Raised"}
    /\ other \in {"Live", "Latched", "Done"}
    /\ stopRequests \in 0..2
    /\ naturalWon \in BOOLEAN /\ everStopping \in BOOLEAN /\ everClosed \in BOOLEAN
    /\ lossSeen \in BOOLEAN /\ stopByInterference \in BOOLEAN
    /\ pauseEpoch \in -1..MaxGen
    /\ liveByRenewal \in BOOLEAN

Init ==
    /\ pstate = "Live" /\ intent = FALSE /\ termPublished = FALSE
    /\ lost = FALSE /\ deadlineOk = TRUE /\ keeper = "Active" /\ farewell = FALSE
    /\ drained = "Unset" /\ lpc = "Top" /\ stop = FALSE
    /\ reqGen = 0 /\ handledGen = 0 /\ snapGen = 0
    /\ armReady = FALSE /\ epoch = 0
    /\ gcsched = "Running" /\ gcstopping = FALSE /\ round = FALSE
    /\ mutex = "None" /\ fpc = "NotStarted"
    /\ d1stop = "None" /\ other = "Live" /\ stopRequests = 0
    /\ naturalWon = FALSE /\ everStopping = FALSE /\ everClosed = FALSE
    /\ lossSeen = FALSE /\ stopByInterference = FALSE /\ pauseEpoch = -1
    /\ liveByRenewal = FALSE

\* `remountTerminal`. Every terminal entry publishes the latch.
LeaseTerminal == intent

Pending == reqGen > handledGen

\* `tripMountLost`: latch the fence; count a lease loss (Live -> Transient)
\* only when no terminal latch is published.
TripMountLost ==
    /\ lost' = TRUE
    /\ pstate' = IF pstate = "Live" /\ ~intent THEN "Transient" ELSE pstate

Max(a, b) == IF a > b THEN a ELSE b

\* The one arming rule; `pendingAfter` is whether a request stays pending
\* once this step's acknowledgement is applied.
CanArm(pendingAfter, checkPending) ==
    /\ (~checkPending \/ ~pendingAfter)
    /\ (~ArmChecksTerminal \/ (~stop /\ ~LeaseTerminal))

\* A loss observation: publish the latch; the first terminal transition
\* enters Stopping and owes one process-stop request. Touches `intent`,
\* `lossSeen`, `pstate`, `termPublished`, `naturalWon`, `everStopping`, `d1stop`.
EnterStopping ==
    /\ intent' = TRUE /\ lossSeen' = TRUE
    /\ IF termPublished
       THEN UNCHANGED <<pstate, termPublished, naturalWon, everStopping, d1stop>>
       ELSE /\ pstate' = "Stopping" /\ termPublished' = TRUE
            /\ naturalWon' = TRUE /\ everStopping' = TRUE /\ d1stop' = "Pending"

\* The callback under the process-wide once-flag: only the first raise of
\* the process issues a request.
IssueStop ==
    stopRequests' = IF StopOnce /\ stopRequests > 0 THEN stopRequests
                    ELSE stopRequests + 1

(***************************************************************************)
(* THE LEASE THREAD                                                        *)
(***************************************************************************)
LExit ==
    /\ lpc = "Top"
    /\ (stop \/ LeaseTerminal)
    /\ lpc' = "Exited"
    /\ UNCHANGED <<pstate, intent, termPublished, lost, deadlineOk, keeper,
                   reqGen, handledGen, snapGen, armReady, epoch, liveByRenewal,
                   gcVars, forgetVars, stopVars, historyVars>>

\* Step 0 of `Pool::tryRemountOnce` presumes the lease lost. A reclaim can
\* begin on an armed fence only when a request was raised during the
\* previous reclaim, whose arm then overrode that request's trip; with
\* `LossStops` nothing raises a request off the lease thread.
LAttemptBegin ==
    /\ lpc = "Top"
    /\ ~stop /\ ~LeaseTerminal /\ Pending
    /\ lpc' = "InAttempt" /\ snapGen' = reqGen
    /\ IF ReclaimTrips
       THEN TripMountLost
       ELSE /\ pstate' = IF pstate = "Live" THEN "Transient" ELSE pstate
            /\ UNCHANGED lost
    /\ UNCHANGED <<intent, termPublished, deadlineOk, keeper, reqGen,
                   handledGen, armReady, epoch, liveByRenewal, gcVars,
                   forgetVars, stopVars, historyVars>>

\* Verdict StayTransient, a probe that could not reach the store, an
\* unclaimable mount, or any failed later step.
LAttemptFail ==
    /\ lpc = "InAttempt"
    /\ lpc' = "Top"
    /\ UNCHANGED <<pstate, intent, termPublished, lost, deadlineOk, keeper,
                   reqGen, handledGen, snapGen, armReady, epoch, liveByRenewal,
                   gcVars, forgetVars, stopVars, historyVars>>

\* Verdict Recover: install and start a fresh renewer under a new epoch,
\* acknowledge the snapshot, and decide whether to arm. `ready` = the claim
\* still admits a write. The arm itself is the next step (`LArmOpen`).
LReclaim ==
    /\ lpc = "InAttempt" /\ pstate = "Transient"
    /\ \E ready \in BOOLEAN : deadlineOk' = ready
    /\ armReady' = CanArm(reqGen > snapGen, ReclaimChecksPending)
    /\ keeper' = "Active" /\ lpc' = "Arming" /\ epoch' = epoch + 1
    /\ handledGen' = IF AckSnapshot THEN Max(handledGen, snapGen) ELSE reqGen
    /\ UNCHANGED <<pstate, intent, termPublished, lost, reqGen, snapGen,
                   liveByRenewal, gcVars, forgetVars, stopVars, historyVars>>

\* Open writes under `driver_mutex`: an interference report or FORGET may
\* have published the latch since the decision, so the rule is read again in
\* this step, as is the deadline. Without an arm the pool stays Transient
\* for the next renewal.
LArmOpen ==
    /\ lpc = "Arming"
    /\ IF /\ armReady
          /\ (deadlineOk \/ ~ReadinessGate)
          /\ (~ArmRechecksAtOpen \/ CanArm(reqGen > snapGen, ReclaimChecksPending))
       THEN pstate' = "Live" /\ lost' = FALSE
       ELSE UNCHANGED <<pstate, lost>>
    /\ lpc' = "Top" /\ armReady' = FALSE
    /\ UNCHANGED <<intent, termPublished, deadlineOk, keeper, reqGen,
                   handledGen, snapGen, epoch, liveByRenewal, gcVars,
                   forgetVars, stopVars, historyVars>>

\* The attempt observes a loss row: the identity probe (`IdentityLost`,
\* `Replaced`) or any other R9-R17 observation. The fence was tripped by
\* step 0; an attempt past its step-0 bail can land this mid-FORGET, and
\* the first terminal transition wins.
LAttemptLoss ==
    /\ lpc = "InAttempt"
    /\ lpc' = "Top"
    /\ EnterStopping
    /\ UNCHANGED <<lost, deadlineOk, keeper, reqGen, handledGen, snapGen,
                   armReady, epoch, liveByRenewal, gcVars, forgetVars,
                   other, stopRequests, everClosed, stopByInterference,
                   pauseEpoch>>

\* The period wait is abstracted: a renewal may start whenever the loop is
\* idle with no pending request.
LRenewBegin ==
    /\ lpc = "Top"
    /\ ~stop /\ ~Pending
    /\ pstate \notin TerminalSet
    /\ (~RenewChecksIntent \/ ~intent)
    /\ lpc' = "Renewing"
    /\ UNCHANGED <<pstate, intent, termPublished, lost, deadlineOk, keeper,
                   reqGen, handledGen, snapGen, armReady, epoch, liveByRenewal,
                   gcVars, forgetVars, stopVars, historyVars>>

\* Success. A request already under way can succeed after a stop, a trip,
\* a remount request or the latch; `ready` = start + TTL still admits a
\* write. On a pool a reclaim left Transient with a too-old claim, arm the
\* fence and report Live in the same step.
LRenewOk ==
    /\ lpc = "Renewing"
    /\ lpc' = "Top"
    /\ \E ready \in BOOLEAN :
         LET arm == /\ ready
                    /\ IF RenewOkChecksLatch
                       THEN pstate = "Transient" /\ CanArm(Pending, LiveChecksPending)
                       ELSE /\ pstate \in {"Transient", "Stopping"}
                            /\ (~LiveChecksPending \/ ~Pending)
             report == arm /\ pstate = "Transient"
         IN /\ deadlineOk' = ready
            /\ pstate' = IF report THEN "Live" ELSE pstate
            /\ lost' = IF arm THEN FALSE ELSE lost
            /\ liveByRenewal' = (liveByRenewal \/ report)
    /\ UNCHANGED <<intent, termPublished, keeper, reqGen, handledGen,
                   snapGen, armReady, epoch, gcVars, forgetVars, stopVars,
                   historyVars>>

\* An early end on a stop, a pending remount request or the latch; a
\* definitive answer that arrives under them ends the same way. The renewer
\* becomes RenewalTerminal, which forbids the farewell. A stop trips the
\* fence without counting a lease loss.
LRenewEndEarly ==
    /\ lpc = "Renewing"
    /\ (stop \/ LeaseTerminal \/ Pending)
    /\ lpc' = "Top" /\ keeper' = "Terminal"
    /\ IF stop
       THEN lost' = TRUE /\ UNCHANGED pstate
       ELSE TripMountLost
    /\ UNCHANGED <<intent, termPublished, deadlineOk, reqGen, handledGen,
                   snapGen, armReady, epoch, liveByRenewal, gcVars, forgetVars,
                   stopVars, historyVars>>

\* R8: our own body carries `gc_fenced`. Trip and request a remount.
LRenewFenced ==
    /\ lpc = "Renewing"
    /\ ~stop /\ ~LeaseTerminal /\ ~Pending /\ reqGen < MaxGen
    /\ lpc' = "Top" /\ keeper' = "Terminal"
    /\ TripMountLost
    /\ reqGen' = reqGen + 1
    /\ UNCHANGED <<intent, termPublished, deadlineOk, handledGen, snapGen,
                   armReady, epoch, liveByRenewal, gcVars, forgetVars,
                   stopVars, historyVars>>

\* R9-R17 observed by the renewal: latch the fence and enter Stopping. With
\* `LossStops` off, the pre-Stopping product: trip and request a remount.
LRenewLoss ==
    /\ lpc = "Renewing"
    /\ ~stop
    /\ lpc' = "Top" /\ keeper' = "Terminal"
    /\ IF LossStops
       THEN /\ lost' = TRUE
            /\ EnterStopping
            /\ UNCHANGED reqGen
       ELSE /\ (LeaseTerminal \/ Pending \/ reqGen < MaxGen)
            /\ TripMountLost /\ lossSeen' = TRUE
            /\ reqGen' = IF LeaseTerminal \/ Pending THEN reqGen ELSE reqGen + 1
            /\ UNCHANGED <<intent, termPublished, naturalWon, everStopping,
                           d1stop>>
    /\ UNCHANGED <<deadlineOk, handledGen, snapGen, armReady, epoch,
                   liveByRenewal, gcVars, forgetVars, other, stopRequests,
                   everClosed, stopByInterference, pauseEpoch>>

(***************************************************************************)
(* OUTSIDE THE THREAD                                                      *)
(***************************************************************************)
\* `reportImpossibleInterference` (R14, R15, R17 of the ref machine) from any
\* thread, while a renewer exists. With `LossStops` off, today's
\* `tripMountLost` + `scheduleRemount`.
XInterfere ==
    /\ keeper \in {"Active", "Terminal"}
    /\ IF LossStops
       THEN /\ lost' = TRUE
            /\ EnterStopping
            /\ stopByInterference' = (stopByInterference \/ ~termPublished)
            /\ UNCHANGED reqGen
       ELSE /\ (stop \/ LeaseTerminal \/ reqGen < MaxGen)
            /\ TripMountLost /\ lossSeen' = TRUE
            /\ reqGen' = IF stop \/ LeaseTerminal THEN reqGen ELSE reqGen + 1
            /\ UNCHANGED <<intent, termPublished, naturalWon, everStopping,
                           d1stop, stopByInterference>>
    /\ UNCHANGED <<deadlineOk, keeper, lpc, handledGen, snapGen, armReady,
                   epoch, liveByRenewal, gcVars, forgetVars, other,
                   stopRequests, everClosed, pauseEpoch>>

\* The thread that latched this disk raises the process stop.
XRaise ==
    /\ d1stop = "Pending"
    /\ d1stop' = "Raised"
    /\ IssueStop
    /\ UNCHANGED <<pstate, intent, termPublished, lost, deadlineOk, keeper,
                   leaseVars, gcVars, forgetVars, other, historyVars>>

\* The second disk observes a loss and latches, then raises.
OtherObserve ==
    /\ other = "Live"
    /\ other' = "Latched"
    /\ UNCHANGED <<pstate, intent, termPublished, lost, deadlineOk, keeper,
                   leaseVars, gcVars, forgetVars, d1stop, stopRequests,
                   historyVars>>

OtherRaise ==
    /\ other = "Latched"
    /\ other' = "Done"
    /\ IssueStop
    /\ UNCHANGED <<pstate, intent, termPublished, lost, deadlineOk, keeper,
                   leaseVars, gcVars, forgetVars, d1stop, historyVars>>

\* The lease expires while no renewal succeeds (`paused`). Neither the fence
\* latch, the lifecycle, the epoch nor a request generation changes.
XExpire ==
    /\ deadlineOk
    /\ deadlineOk' = FALSE
    /\ pauseEpoch' = IF pstate = "Live" /\ ~lost THEN epoch ELSE pauseEpoch
    /\ UNCHANGED <<pstate, intent, termPublished, lost, keeper, leaseVars,
                   gcVars, forgetVars, stopVars, naturalWon, everStopping,
                   everClosed, lossSeen, stopByInterference>>

(***************************************************************************)
(* GC SCHEDULER LOOP + ADMIN VERBS                                         *)
(***************************************************************************)
GcRoundStart ==
    /\ gcsched = "Running" /\ ~gcstopping /\ ~round
    /\ pstate \notin TerminalSet
    /\ round' = TRUE
    /\ UNCHANGED <<pstate, intent, termPublished, lost, deadlineOk, keeper,
                   leaseVars, gcsched, gcstopping, forgetVars, stopVars,
                   historyVars>>

GcRoundEnd ==
    /\ round
    /\ round' = FALSE
    /\ UNCHANGED <<pstate, intent, termPublished, lost, deadlineOk, keeper,
                   leaseVars, gcsched, gcstopping, forgetVars, stopVars,
                   historyVars>>

\* Self-exit at a wake that observes a terminal pool, after the round ends.
GcSelfExit ==
    /\ EnableGcSelfExit
    /\ gcsched = "Running" /\ pstate \in TerminalSet /\ ~round
    /\ gcsched' = "Stopped"
    /\ UNCHANGED <<pstate, intent, termPublished, lost, deadlineOk, keeper,
                   leaseVars, gcstopping, round, forgetVars, stopVars,
                   historyVars>>

\* `GC STOP`: under lifecycle_mutex; stop() joins an in-flight round.
VGcStop ==
    /\ mutex = "None" /\ gcsched = "Running" /\ ~round
    /\ gcsched' = "Stopped"
    /\ UNCHANGED <<pstate, intent, termPublished, lost, deadlineOk, keeper,
                   leaseVars, gcstopping, round, forgetVars, stopVars,
                   historyVars>>

\* `GC START`: checkOpAdmitted(Admin) refuses unless the pool is Live.
VGcStart ==
    /\ mutex = "None" /\ gcsched = "Stopped" /\ pstate = "Live"
    /\ gcsched' = "Running"
    /\ UNCHANGED <<pstate, intent, termPublished, lost, deadlineOk, keeper,
                   leaseVars, gcstopping, round, forgetVars, stopVars,
                   historyVars>>

(***************************************************************************)
(* FORGET (one instance; each step one atomic action)                      *)
(***************************************************************************)
FStart ==
    /\ fpc = "NotStarted" /\ mutex = "None"
    /\ mutex' = "Forget" /\ fpc' = "Check"
    /\ UNCHANGED <<pstate, intent, termPublished, lost, deadlineOk, keeper,
                   farewell, drained, leaseVars, stop, gcVars, stopVars,
                   historyVars>>

\* Already terminal: return. The storage-side scheduler local still
\* stop()s on return (signal + join).
FCheckShort ==
    /\ fpc = "Check" /\ pstate \in TerminalSet
    /\ gcstopping' = TRUE /\ fpc' = "ShortJoin"
    /\ UNCHANGED <<pstate, intent, termPublished, lost, deadlineOk, keeper,
                   farewell, drained, leaseVars, stop, gcsched, round, mutex,
                   stopVars, historyVars>>

FShortJoin ==
    /\ fpc = "ShortJoin" /\ ~round
    /\ gcsched' = "Destroyed" /\ fpc' = "Done" /\ mutex' = "None"
    /\ UNCHANGED <<pstate, intent, termPublished, lost, deadlineOk, keeper,
                   farewell, drained, leaseVars, stop, gcstopping, round,
                   stopVars, historyVars>>

FCheckProceed ==
    /\ fpc = "Check" /\ pstate \notin TerminalSet
    /\ fpc' = "Intent"
    /\ UNCHANGED <<pstate, intent, termPublished, lost, deadlineOk, keeper,
                   farewell, drained, leaseVars, stop, gcVars, mutex,
                   stopVars, historyVars>>

FIntent ==
    /\ fpc = "Intent"
    /\ intent' = TRUE /\ fpc' = "Trip1"
    /\ UNCHANGED <<pstate, termPublished, lost, deadlineOk, keeper, farewell,
                   drained, leaseVars, stop, gcVars, mutex, stopVars,
                   historyVars>>

FTrip1 ==
    /\ fpc = "Trip1"
    /\ TripMountLost
    /\ fpc' = "GcStopSignal"
    /\ UNCHANGED <<intent, termPublished, deadlineOk, keeper, farewell,
                   drained, leaseVars, stop, gcVars, mutex, stopVars,
                   historyVars>>

FGcStopSignal ==
    /\ fpc = "GcStopSignal"
    /\ gcstopping' = TRUE /\ fpc' = "GcStopJoin"
    /\ UNCHANGED <<pstate, intent, termPublished, lost, deadlineOk, keeper,
                   farewell, drained, leaseVars, stop, gcsched, round, mutex,
                   stopVars, historyVars>>

FGcStopJoin ==
    /\ fpc = "GcStopJoin" /\ ~round
    /\ gcsched' = "Destroyed" /\ fpc' = "JoinSignal"
    /\ UNCHANGED <<pstate, intent, termPublished, lost, deadlineOk, keeper,
                   farewell, drained, leaseVars, stop, gcstopping, round,
                   mutex, stopVars, historyVars>>

\* stopBackgroundWorkers: latch the stop, then join the lease thread.
FJoinSignal ==
    /\ fpc = "JoinSignal"
    /\ stop' = TRUE /\ fpc' = "JoinWait"
    /\ UNCHANGED <<pstate, intent, termPublished, lost, deadlineOk, keeper,
                   farewell, drained, leaseVars, gcVars, mutex, stopVars,
                   historyVars>>

FJoinWait ==
    /\ fpc = "JoinWait" /\ lpc = "Exited"
    /\ fpc' = IF EnableTrip2 THEN "Trip2" ELSE "Drain"
    /\ UNCHANGED <<pstate, intent, termPublished, lost, deadlineOk, keeper,
                   farewell, drained, leaseVars, stop, gcVars, mutex,
                   stopVars, historyVars>>

\* Re-latch after the join. Under `CanArm` nothing arms once the intent is
\* published; this covers an arm that ignores the intent.
FTrip2 ==
    /\ fpc = "Trip2"
    /\ TripMountLost
    /\ fpc' = "Drain"
    /\ UNCHANGED <<intent, termPublished, deadlineOk, keeper, farewell,
                   drained, leaseVars, stop, gcVars, mutex, stopVars,
                   historyVars>>

FDrain ==
    /\ fpc = "Drain"
    /\ \E d \in {"Yes", "No"} : drained' = d
    /\ fpc' = "Teardown"
    /\ UNCHANGED <<pstate, intent, termPublished, lost, deadlineOk, keeper,
                   farewell, leaseVars, stop, gcVars, mutex, stopVars,
                   historyVars>>

\* finishTeardown: `release` (the farewell) only from an Active renewer and
\* only if drained.
FTeardown ==
    /\ fpc = "Teardown"
    /\ IF keeper \in {"Active", "Terminal"}
       THEN IF keeper = "Active" /\ (drained = "Yes" \/ ~DrainGate)
            THEN keeper' = "StoppedClean" /\ farewell' = TRUE
            ELSE keeper' = "StoppedNoFarewell" /\ UNCHANGED farewell
       ELSE UNCHANGED <<keeper, farewell>>
    /\ fpc' = "KeeperReset"
    /\ UNCHANGED <<pstate, intent, termPublished, lost, deadlineOk, drained,
                   leaseVars, stop, gcVars, mutex, stopVars, historyVars>>

FKeeperReset ==
    /\ fpc = "KeeperReset"
    /\ keeper' = "None" /\ fpc' = "Enter"
    /\ UNCHANGED <<pstate, intent, termPublished, lost, deadlineOk, farewell,
                   drained, leaseVars, stop, gcVars, mutex, stopVars,
                   historyVars>>

\* Closed (R18); a raced Stopping makes it a no-op.
FEnter ==
    /\ fpc = "Enter"
    /\ intent' = TRUE
    /\ IF termPublished
       THEN UNCHANGED <<pstate, termPublished, everClosed>>
       ELSE /\ pstate' = "Closed" /\ termPublished' = TRUE
            /\ everClosed' = TRUE
    /\ fpc' = "Done" /\ mutex' = "None"
    /\ UNCHANGED <<lost, deadlineOk, keeper, farewell, drained, leaseVars,
                   stop, gcVars, stopVars, naturalWon, everStopping, lossSeen,
                   stopByInterference, pauseEpoch>>

(***************************************************************************)
Next ==
    \/ LExit \/ LAttemptBegin \/ LAttemptFail \/ LReclaim \/ LArmOpen
    \/ LAttemptLoss \/ LRenewBegin \/ LRenewOk \/ LRenewEndEarly
    \/ LRenewFenced \/ LRenewLoss
    \/ XInterfere \/ XRaise \/ OtherObserve \/ OtherRaise \/ XExpire
    \/ GcRoundStart \/ GcRoundEnd \/ GcSelfExit \/ VGcStop \/ VGcStart
    \/ FStart \/ FCheckShort \/ FShortJoin \/ FCheckProceed \/ FIntent
    \/ FTrip1 \/ FGcStopSignal \/ FGcStopJoin \/ FJoinSignal \/ FJoinWait
    \/ FTrip2 \/ FDrain \/ FTeardown \/ FKeeperReset \/ FEnter

Spec == Init /\ [][Next]_vars

\* Every FORGET step, the lease thread's exit, the ends of its attempt, of
\* its arm and of a renewal that must end early, the process-stop raise, a
\* GC round, and the GC self-exit make progress. FStart is not fair: FORGET
\* is operator-initiated.
FairSpec ==
    /\ Spec
    /\ WF_vars(FCheckShort) /\ WF_vars(FShortJoin) /\ WF_vars(FCheckProceed)
    /\ WF_vars(FIntent) /\ WF_vars(FTrip1) /\ WF_vars(FGcStopSignal)
    /\ WF_vars(FGcStopJoin) /\ WF_vars(FJoinSignal) /\ WF_vars(FJoinWait)
    /\ WF_vars(FTrip2) /\ WF_vars(FDrain) /\ WF_vars(FTeardown)
    /\ WF_vars(FKeeperReset) /\ WF_vars(FEnter)
    /\ WF_vars(LExit) /\ WF_vars(LAttemptFail) /\ WF_vars(LArmOpen)
    /\ WF_vars(LRenewEndEarly) /\ WF_vars(XRaise)
    /\ WF_vars(GcRoundEnd) /\ WF_vars(GcSelfExit)

(***************************************************************************)
(* PROPERTIES                                                              *)
(***************************************************************************)
ForgetDone == fpc = "Done"

\* FORGET-complete => fence latched + terminal state.
I1ForgetTerminal == ForgetDone => (lost /\ pstate \in TerminalSet)

\* Unless a loss observation won the race, Closed.
I1bForgetWinsUnlessNatural ==
    (ForgetDone /\ ~naturalWon) => pstate = "Closed"

\* No unearned farewell: the lanes drained and the lease thread was joined.
I2EarnedFarewell == farewell => (drained = "Yes" /\ lpc = "Exited")

I3OneWay ==
    /\ everStopping => pstate = "Stopping"
    /\ everClosed => pstate = "Closed"

I4aTransientImpliesLost == pstate = "Transient" => lost

\* A terminal pool never coexists with an armed fence.
I4bTerminalImpliesLost == pstate \in TerminalSet => lost

I6GcStoppedAfterForget == ForgetDone => gcsched = "Destroyed"

\* A reclaim acknowledges no request raised after its snapshot.
I7RequestNotLost == handledGen <= snapGen

\* The thread renews only a renewer that is not RenewalTerminal.
I8RenewsActiveRenewer == lpc = "Renewing" => keeper = "Active"

\* Every loss observation (R9-R17) ends terminal; it never remounts.
I9LossTerminates == lossSeen => pstate \in TerminalSet

\* One process-stop request, however many disks observe a loss.
OneProcessStop == stopRequests <= 1

\* A stop is owed only by a disk that published the latch and is Stopping.
StopFollowsLatch == d1stop # "None" => (pstate = "Stopping" /\ intent)

\* FORGET closes the disk without stopping the process.
ClosedRequestsNoStop == pstate = "Closed" => d1stop = "None"

\* No renewal starts once the FORGET intent is published.
RenewNotAfterIntent ==
    [][(lpc # "Renewing" /\ lpc' = "Renewing") => ~intent]_vars

\* No renewal starts while a remount request is pending.
RenewNotWhilePending ==
    [][(lpc # "Renewing" /\ lpc' = "Renewing") => ~Pending]_vars

\* The fence is never armed while a remount request stays pending.
NoArmWhileRequestPending == [][(lost /\ ~lost') => ~Pending']_vars

\* Live is reported only when a write can be admitted.
LiveOnlyWhenAdmissible ==
    [][(pstate # "Live" /\ pstate' = "Live") => (~lost' /\ deadlineOk')]_vars

\* No path out of Stopping or Closed.
TerminalAbsorbing == [][(pstate \in TerminalSet) => (pstate' = pstate)]_vars

\* Nothing arms the fence once the latch is published, a late renewal
\* success included.
NoArmAfterLatch == [][(intent => ~(lost /\ ~lost'))]_vars

\* Nothing arms the fence once FORGET itself has published its intent.
ForgetPastIntent ==
    fpc \in {"Trip1", "GcStopSignal", "GcStopJoin", "JoinSignal", "JoinWait",
            "Trip2", "Drain", "Teardown", "KeeperReset", "Enter"}
ForgetWinsArm == [][(ForgetPastIntent => ~(lost /\ ~lost'))]_vars

\* An expiry changes no epoch, request generation, fence latch or lifecycle.
PauseKeepsEpochAndGeneration ==
    [][((deadlineOk /\ ~deadlineOk' /\ lpc' = lpc)
          => UNCHANGED <<epoch, reqGen, handledGen, lost, pstate>>)]_vars

\* Liveness (FairSpec): a started FORGET always completes.
ForgetCompletes == (fpc = "Check") ~> ForgetDone

\* Liveness (FairSpec): once terminal, the GC scheduler stops ticking.
GcExitsAfterTerminal == (pstate \in TerminalSet) ~> (gcsched # "Running")

\* Liveness (FairSpec): the lease thread exits on a stop or the latch.
LeaseThreadExits == (stop \/ LeaseTerminal) ~> (lpc = "Exited")

\* Liveness (FairSpec): a Stopping disk gets the process stop requested.
StopIsRequested == (pstate = "Stopping") ~> (stopRequests > 0)

\* With `LossStops`, the races the request generations guard are
\* unreachable: no request appears while the thread is away from Top, and a
\* reclaim never begins on an armed fence.
NoRequestRace ==
    /\ lpc \in {"InAttempt", "Arming"} => reqGen = snapGen
    /\ lpc = "Renewing" => ~Pending
    /\ (lpc = "Top" /\ Pending) => lost

\* Witnesses (expected VIOLATED in their configs = reachability).
WForgetNeverDone == ~ForgetDone
WNoRacedStopping == ~(ForgetDone /\ pstate = "Stopping")
\* No reclaim arms the fence inside FORGET's join window.
NoJoinWindowReclaim == ~(fpc = "Trip2" /\ pstate = "Live" /\ ~lost)
WNoLiveByRenewal == ~liveByRenewal
WNoRequestDuringReclaim == ~(lpc = "InAttempt" /\ reqGen > snapGen)
WNoForgetFarewell == ~(ForgetDone /\ farewell)
\* Expiry, then a renewal, then writes admitted under the same epoch.
WNoPauseResume ==
    ~(pauseEpoch = epoch /\ pstate = "Live" /\ ~lost /\ deadlineOk)
\* Interference stops this disk; both disks raise; one request.
WNoInterferenceStop ==
    ~(stopByInterference /\ pstate = "Stopping" /\ d1stop = "Raised"
      /\ other = "Done" /\ stopRequests = 1)

=============================================================================
