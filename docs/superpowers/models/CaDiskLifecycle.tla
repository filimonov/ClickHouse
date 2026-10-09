--------------------------- MODULE CaDiskLifecycle ---------------------------
(***************************************************************************)
(* Pool lifecycle of a writable CAS mount, its ONE lease thread, the GC    *)
(* scheduler, and the `SYSTEM CONTENT ADDRESSED FORGET` protocol           *)
(* (`Pool::forgetDisk`).                                                   *)
(*                                                                         *)
(* Lifecycle: Live, Reacquiring, and two absorbing states: Stopping (a     *)
(* loss observation, rows R9-R17 of the lease-series design) and Closed    *)
(* (FORGET, R18). Only an own `gc_fenced` body (R8) is reacquired.         *)
(*                                                                         *)
(* The lease thread renews and reacquires in sequence:                     *)
(*   until stopped or `LeaseTerminal`:                                     *)
(*     renew: success sets the deadline; an own fenced body (R8) starts    *)
(*       the reacquire; a loss observation enters Stopping.                *)
(*   Reacquire: the thread latches the fence and closes the old epoch      *)
(*     (`Closing`), takes one new epoch per fence (`LAlloc`), claims the   *)
(*     slot under it (`LClaimed`) and opens writes in the arming step.     *)
(*     The pool is `Reacquiring` meanwhile, and an operation admitted      *)
(*     under the old epoch is refused when it completes under the new one. *)
(* `deadlineOk` abstracts "the fence deadline admits a write". One arming  *)
(* rule (`CanArm`): the fence is armed and Live reported only with no stop *)
(* and no terminal lifecycle. A claim arms when it admits a write;         *)
(* otherwise the next successful renewal arms in the same step that        *)
(* reports Live.                                                           *)
(*                                                                         *)
(* Entering Stopping publishes the terminal latch in the observing step;   *)
(* the process-stop request is a later step of the same thread. A second   *)
(* disk of the same process is reduced to its own observe and raise steps  *)
(* over the shared once-flag.                                              *)
(*                                                                         *)
(* FORGET steps in `Pool::forgetDisk` order: mutex -> idempotent terminal  *)
(* short-circuit -> publish intent -> trip -> GC stop (signal, join) ->    *)
(* lease thread stop (signal, join) -> drain -> teardown (farewell only if *)
(* drained and the renewer is Active) -> renewer reset -> Closed, first    *)
(* terminal wins.                                                          *)
(*                                                                         *)
(* Not modelled: time and retry spacing, the open's own readiness renewal, *)
(* the content of a claim or a renewal, the slot certificate, the fence    *)
(* generation counter, the shutdown that a process-stop request starts.    *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS
    DrainGate,         \* farewell only when the ref lanes provably drained
    EnableGcSelfExit,  \* the GC scheduler exits at a wake that sees a terminal pool
    RenewChecksIntent, \* a renewal never starts once the FORGET intent is published
    ReadinessGate,     \* a claim with a too-old lease reports Live only after a renewal
    ArmChecksTerminal, \* nothing arms after a stop or with a terminal lifecycle
    RenewOkChecksLatch,\* a renewal success arms nothing on a latched or Stopping pool
    ArmRechecksAtOpen, \* the claim re-checks the arming rule in the step that opens writes
    StopOnce,          \* the process-stop request sits under one process-wide once-flag
    SabFarewellOnStopping, \* FALSE = a Stopping pool writes no farewell; TRUE = the teardown writes it
    SabReacqAllocPerAttempt, \* TRUE = every claim attempt allocates an epoch, not one per fence
    SabOldOpUnchecked, \* TRUE = an operation admitted under the old epoch lands under the new one
    SabLossReacquires, \* TRUE = a loss found by the claim goes back to reacquiring instead of stopping
    DrainWaitsPublish, \* the drain reports Yes only with no snapshot publish in flight
    FarewellLeaseBound, \* TRUE = the farewell needs an unexpired lease (the old terminate bound)
    SabFarewellFromTerminal, \* TRUE = a renewer whose last renewal ended unconfirmed writes the farewell
    MaxGen             \* bound on epochs (state-space bound only)

TerminalSet == {"Stopping", "Closed"}
PStates == {"Live", "Reacquiring"} \cup TerminalSet

VARIABLES
    pstate,         \* CasMountRuntime.pool_lifecycle
    intent,         \* the terminal latch: FORGET's published intent or a Stopping entry
    termPublished,  \* terminal_state_published
    lost,           \* MountFence.lost
    deadlineOk,     \* the fence deadline still admits a write
    keeper,         \* renewer: "Active" | "Terminal" | "StoppedClean" | "StoppedNoFarewell" | "None"
    farewell,       \* the clean-release marker was written
    drained,        \* "Unset" | "Yes" | "No" (drainRefLanesForShutdown)
    lpc,            \* lease thread: "Top" | "Renewing" | "Closing" | "Claiming" | "Arming" | "Exited"
    stop,           \* workers_stop_requested
    armReady,       \* the claim's arming decision, carried to the step that opens writes
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
    liveByRenewal,  \* history: a renewal armed the fence and reported Live
    allocated,      \* the reacquire in progress has allocated its epoch
    allocCount,     \* epochs allocated for the current fence
    fenceEpoch,     \* epoch at the last R8, -1 if none
    opEpoch,        \* epoch under which an operation was admitted, -1 if none in flight
    staleOpLanded,  \* history: an operation admitted under an older epoch landed
    reacqLive,      \* history: a reacquire opened writes under the next epoch
    publishing,     \* a snapshot publish of this incarnation is in flight
    lateLanding,    \* history: a publish settled after the farewell was written
    farewellExpired, \* history: the farewell was written after the lease expired
    renewUnclear    \* history: a renewal ended without a confirmed lease and no claim followed

vars == <<pstate, intent, termPublished, lost, deadlineOk, keeper, farewell,
          drained, lpc, stop, armReady, epoch,
          gcsched, gcstopping, round, mutex, fpc, d1stop, other, stopRequests,
          naturalWon, everStopping, everClosed, lossSeen, stopByInterference,
          pauseEpoch, liveByRenewal, allocated, allocCount, fenceEpoch, opEpoch,
          staleOpLanded, reacqLive, publishing, lateLanding, farewellExpired,
          renewUnclear>>

reacqVars == <<allocated, allocCount, fenceEpoch, opEpoch, staleOpLanded, reacqLive>>
farewellVars == <<publishing, lateLanding, farewellExpired, renewUnclear>>

leaseVars == <<lpc, armReady, epoch, liveByRenewal>>
gcVars == <<gcsched, gcstopping, round>>
forgetVars == <<mutex, fpc, drained, farewell, stop>>
stopVars == <<d1stop, other, stopRequests>>
historyVars == <<naturalWon, everStopping, everClosed, lossSeen,
                 stopByInterference, pauseEpoch>>

FpcStates == {"NotStarted", "Check", "ShortJoin", "Intent", "Trip1",
              "GcStopSignal", "GcStopJoin", "JoinSignal", "JoinWait",
              "Drain", "Teardown", "KeeperReset", "Enter", "Done"}

TypeOK ==
    /\ pstate \in PStates
    /\ intent \in BOOLEAN /\ termPublished \in BOOLEAN /\ lost \in BOOLEAN
    /\ deadlineOk \in BOOLEAN
    /\ keeper \in {"Active", "Terminal", "StoppedClean", "StoppedNoFarewell", "None"}
    /\ farewell \in BOOLEAN
    /\ drained \in {"Unset", "Yes", "No"}
    /\ lpc \in {"Top", "Renewing", "Arming", "Closing", "Claiming", "Exited"}
    /\ stop \in BOOLEAN
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
    /\ allocated \in BOOLEAN /\ allocCount \in 0..2
    /\ fenceEpoch \in -1..MaxGen /\ opEpoch \in -1..MaxGen
    /\ staleOpLanded \in BOOLEAN /\ reacqLive \in BOOLEAN
    /\ publishing \in BOOLEAN /\ lateLanding \in BOOLEAN
    /\ farewellExpired \in BOOLEAN /\ renewUnclear \in BOOLEAN

Init ==
    /\ pstate = "Live" /\ intent = FALSE /\ termPublished = FALSE
    /\ lost = FALSE /\ deadlineOk = TRUE /\ keeper = "Active" /\ farewell = FALSE
    /\ drained = "Unset" /\ lpc = "Top" /\ stop = FALSE
    /\ armReady = FALSE /\ epoch = 0
    /\ gcsched = "Running" /\ gcstopping = FALSE /\ round = FALSE
    /\ mutex = "None" /\ fpc = "NotStarted"
    /\ d1stop = "None" /\ other = "Live" /\ stopRequests = 0
    /\ naturalWon = FALSE /\ everStopping = FALSE /\ everClosed = FALSE
    /\ lossSeen = FALSE /\ stopByInterference = FALSE /\ pauseEpoch = -1
    /\ liveByRenewal = FALSE
    /\ allocated = FALSE /\ allocCount = 0 /\ fenceEpoch = -1 /\ opEpoch = -1
    /\ staleOpLanded = FALSE /\ reacqLive = FALSE
    /\ publishing = FALSE /\ lateLanding = FALSE
    /\ farewellExpired = FALSE /\ renewUnclear = FALSE

\* `remountTerminal`. Every terminal entry publishes the latch.
LeaseTerminal == intent

\* The one arming rule.
CanArm == ~ArmChecksTerminal \/ (~stop /\ ~LeaseTerminal)

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
    /\ UNCHANGED <<farewellVars, pstate, intent, termPublished, lost, deadlineOk, keeper,
                   armReady, epoch, liveByRenewal,
                   gcVars, forgetVars, stopVars, historyVars, reacqVars>>

\* Open writes under `driver_mutex`: an interference report or FORGET may
\* have published the latch since the decision, so the rule is read again in
\* this step, as is the deadline. Without an arm the pool stays `Reacquiring`
\* for the next renewal.
LArmOpen ==
    /\ lpc = "Arming"
    /\ IF /\ armReady
          /\ (deadlineOk \/ ~ReadinessGate)
          /\ (~ArmRechecksAtOpen \/ CanArm)
       THEN /\ pstate' = "Live" /\ lost' = FALSE
            /\ reacqLive' = (reacqLive \/ (pstate = "Reacquiring" /\ epoch = fenceEpoch + 1))
       ELSE UNCHANGED <<pstate, lost, reacqLive>>
    /\ lpc' = "Top" /\ armReady' = FALSE
    /\ UNCHANGED <<farewellVars, intent, termPublished, deadlineOk, keeper, epoch,
                   liveByRenewal, gcVars,
                   forgetVars, stopVars, historyVars,
                   allocated, allocCount, fenceEpoch, opEpoch, staleOpLanded>>

\* The period wait is abstracted: a renewal may start whenever the loop is idle.
LRenewBegin ==
    /\ lpc = "Top"
    /\ ~stop
    /\ pstate \notin TerminalSet
    /\ (~RenewChecksIntent \/ ~intent)
    /\ lpc' = "Renewing"
    /\ UNCHANGED <<farewellVars, pstate, intent, termPublished, lost, deadlineOk, keeper,
                   armReady, epoch, liveByRenewal,
                   gcVars, forgetVars, stopVars, historyVars, reacqVars>>

\* Success. A request already under way can succeed after a stop, a trip
\* or the latch; `ready` = start + TTL still admits a write. On a pool a
\* claim left `Reacquiring` with a too-old lease, arm the fence and report
\* Live in the same step.
LRenewOk ==
    /\ lpc = "Renewing"
    /\ lpc' = "Top"
    /\ \E ready \in BOOLEAN :
         LET arm == /\ ready
                    /\ IF RenewOkChecksLatch
                       THEN pstate = "Reacquiring" /\ CanArm
                       ELSE pstate \in {"Reacquiring", "Stopping"}
             report == arm /\ pstate = "Reacquiring"
         IN /\ deadlineOk' = ready
            /\ pstate' = IF report THEN "Live" ELSE pstate
            /\ lost' = IF arm THEN FALSE ELSE lost
            /\ liveByRenewal' = (liveByRenewal \/ report)
    /\ UNCHANGED <<farewellVars, intent, termPublished, keeper,
                   armReady, epoch, gcVars, forgetVars, stopVars,
                   historyVars, reacqVars>>

\* An early end on a stop or the latch; a definitive answer that arrives
\* under them ends the same way. The renewer becomes RenewalTerminal, which
\* forbids the farewell. The fence is tripped without a lifecycle change.
LRenewEndEarly ==
    /\ lpc = "Renewing"
    /\ (stop \/ LeaseTerminal)
    /\ lpc' = "Top" /\ keeper' = "Terminal"
    /\ renewUnclear' = TRUE
    /\ lost' = TRUE
    /\ UNCHANGED <<publishing, lateLanding, farewellExpired, pstate, intent, termPublished, deadlineOk,
                   armReady, epoch, liveByRenewal, gcVars, forgetVars,
                   stopVars, historyVars, reacqVars>>

\* R8: our own body carries `gc_fenced`. The lease thread itself starts the
\* reacquire: it latches the fence and closes the old epoch next.
LRenewFenced ==
    /\ lpc = "Renewing"
    /\ ~stop /\ ~LeaseTerminal
    /\ keeper' = "Terminal"
    /\ renewUnclear' = TRUE
    /\ epoch < MaxGen
    /\ lpc' = "Closing" /\ lost' = TRUE
    /\ pstate' = IF pstate = "Live" THEN "Reacquiring" ELSE pstate
    /\ fenceEpoch' = epoch /\ allocated' = FALSE /\ allocCount' = 0
    /\ UNCHANGED <<publishing, lateLanding, farewellExpired, intent, termPublished, deadlineOk,
                   armReady, epoch, liveByRenewal, gcVars, forgetVars,
                   stopVars, historyVars, opEpoch, staleOpLanded, reacqLive>>

\* R9-R17 observed by the renewal: latch the fence and enter Stopping.
LRenewLoss ==
    /\ lpc = "Renewing"
    /\ ~stop
    /\ lpc' = "Top" /\ keeper' = "Terminal"
    /\ lost' = TRUE
    /\ EnterStopping
    /\ UNCHANGED <<farewellVars, deadlineOk, armReady, epoch,
                   liveByRenewal, gcVars, forgetVars, other, stopRequests,
                   everClosed, stopByInterference, pauseEpoch, reacqVars>>

(***************************************************************************)
(* THE REACQUIRE IN THE LEASE THREAD                                       *)
(*                                                                         *)
(* After R8: Closing (the old epoch ends, in-flight ref work included) ->  *)
(* Claiming (one epoch is allocated per fence; a failed step repeats the   *)
(* claim with the epoch already fixed) -> Arming (writes open only after   *)
(* the arming rule is read again) or a loss found by the claim, which      *)
(* stops. Closing ignores a stop: it finishes under the old epoch.         *)
(***************************************************************************)
LClose ==
    /\ lpc = "Closing"
    /\ lpc' = "Claiming"
    /\ UNCHANGED <<farewellVars, pstate, intent, termPublished, lost, deadlineOk, keeper,
                   armReady, epoch, liveByRenewal,
                   gcVars, forgetVars, stopVars, historyVars, reacqVars>>

\* The one allocation of the fence. `SabReacqAllocPerAttempt` allocates again after every failed step.
LAlloc ==
    /\ lpc = "Claiming"
    /\ ~stop /\ ~LeaseTerminal
    /\ (~allocated \/ SabReacqAllocPerAttempt)
    /\ allocCount < 2
    /\ allocated' = TRUE /\ allocCount' = allocCount + 1
    /\ UNCHANGED <<farewellVars, pstate, intent, termPublished, lost, deadlineOk, keeper,
                   lpc, armReady, epoch, liveByRenewal,
                   gcVars, forgetVars, stopVars, historyVars,
                   fenceEpoch, opEpoch, staleOpLanded, reacqLive>>

\* A step that failed transiently. The allocation survives it.
LStepFail ==
    /\ lpc = "Claiming"
    /\ allocated' = (allocated /\ ~SabReacqAllocPerAttempt)
    /\ UNCHANGED <<farewellVars, pstate, intent, termPublished, lost, deadlineOk, keeper,
                   lpc, armReady, epoch, liveByRenewal,
                   gcVars, forgetVars, stopVars, historyVars,
                   allocCount, fenceEpoch, opEpoch, staleOpLanded, reacqLive>>

\* The claim finds anything but the fenced body it read, its own claim or a transient failure: a loss.
LClaimLoss ==
    /\ lpc = "Claiming"
    /\ IF SabLossReacquires
       THEN /\ lpc' = "Closing" /\ lossSeen' = TRUE
            /\ UNCHANGED <<intent, termPublished, pstate, naturalWon, everStopping, d1stop>>
       ELSE /\ lpc' = "Top" /\ EnterStopping
    /\ UNCHANGED <<farewellVars, lost, deadlineOk, keeper,
                   armReady, epoch, liveByRenewal, gcVars, forgetVars,
                   other, stopRequests, everClosed, stopByInterference,
                   pauseEpoch, reacqVars>>

\* The claim lands under the allocated epoch. The arming decision is taken here and read again at the open.
LClaimed ==
    /\ lpc = "Claiming" /\ allocated /\ ~stop /\ epoch < MaxGen
    /\ lpc' = "Arming" /\ keeper' = "Active" /\ epoch' = epoch + 1
    /\ renewUnclear' = FALSE
    /\ \E ready \in BOOLEAN : deadlineOk' = ready
    /\ armReady' = CanArm
    /\ UNCHANGED <<publishing, lateLanding, farewellExpired, pstate, intent, termPublished, lost,
                   liveByRenewal, gcVars, forgetVars, stopVars, historyVars, reacqVars>>

\* A stop or the latch ends the reacquire before it claims.
LReacqAbandon ==
    /\ lpc = "Claiming" /\ (stop \/ LeaseTerminal)
    /\ lpc' = "Top"
    /\ UNCHANGED <<farewellVars, pstate, intent, termPublished, lost, deadlineOk, keeper,
                   armReady, epoch, liveByRenewal,
                   gcVars, forgetVars, stopVars, historyVars, reacqVars>>

(***************************************************************************)
(* AN OPERATION ADMITTED UNDER ONE EPOCH AND MADE DURABLE LATER            *)
(***************************************************************************)
OpAdmit ==
    /\ pstate = "Live" /\ ~lost /\ opEpoch = -1
    /\ opEpoch' = epoch
    /\ UNCHANGED <<farewellVars, pstate, intent, termPublished, lost, deadlineOk, keeper,
                   leaseVars, gcVars, forgetVars, stopVars, historyVars,
                   allocated, allocCount, fenceEpoch, staleOpLanded, reacqLive>>

\* The write lands unless the fence is tripped; the honest write also re-presents the admission epoch.
OpDurable ==
    /\ opEpoch # -1
    /\ LET landed == IF SabOldOpUnchecked THEN ~lost ELSE ~lost /\ opEpoch = epoch
       IN staleOpLanded' = (staleOpLanded \/ (landed /\ opEpoch # epoch))
    /\ opEpoch' = -1
    /\ UNCHANGED <<farewellVars, pstate, intent, termPublished, lost, deadlineOk, keeper,
                   leaseVars, gcVars, forgetVars, stopVars, historyVars,
                   allocated, allocCount, fenceEpoch, reacqLive>>

(***************************************************************************)
(* OUTSIDE THE THREAD                                                      *)
(***************************************************************************)
\* `reportImpossibleInterference` (R14, R15, R17 of the ref machine) from any
\* thread, while a renewer exists.
XInterfere ==
    /\ keeper \in {"Active", "Terminal"}
    /\ lost' = TRUE
    /\ EnterStopping
    /\ stopByInterference' = (stopByInterference \/ ~termPublished)
    /\ UNCHANGED <<farewellVars, deadlineOk, keeper, lpc, armReady,
                   epoch, liveByRenewal, gcVars, forgetVars, other,
                   stopRequests, everClosed, pauseEpoch, reacqVars>>

\* The thread that latched this disk raises the process stop.
XRaise ==
    /\ d1stop = "Pending"
    /\ d1stop' = "Raised"
    /\ IssueStop
    /\ UNCHANGED <<farewellVars, pstate, intent, termPublished, lost, deadlineOk, keeper,
                   leaseVars, gcVars, forgetVars, other, historyVars, reacqVars>>

\* The second disk observes a loss and latches, then raises.
OtherObserve ==
    /\ other = "Live"
    /\ other' = "Latched"
    /\ UNCHANGED <<farewellVars, pstate, intent, termPublished, lost, deadlineOk, keeper,
                   leaseVars, gcVars, forgetVars, d1stop, stopRequests,
                   historyVars, reacqVars>>

OtherRaise ==
    /\ other = "Latched"
    /\ other' = "Done"
    /\ IssueStop
    /\ UNCHANGED <<farewellVars, pstate, intent, termPublished, lost, deadlineOk, keeper,
                   leaseVars, gcVars, forgetVars, d1stop, historyVars, reacqVars>>

\* The lease expires while no renewal succeeds (`paused`). Neither the fence
\* latch, the lifecycle, the epoch nor a request generation changes.
XExpire ==
    /\ deadlineOk
    /\ deadlineOk' = FALSE
    /\ pauseEpoch' = IF pstate = "Live" /\ ~lost THEN epoch ELSE pauseEpoch
    /\ UNCHANGED <<farewellVars, pstate, intent, termPublished, lost, keeper, leaseVars,
                   gcVars, forgetVars, stopVars, naturalWon, everStopping,
                   everClosed, lossSeen, stopByInterference, reacqVars>>

(***************************************************************************)
(* GC SCHEDULER LOOP + ADMIN VERBS                                         *)
(***************************************************************************)
GcRoundStart ==
    /\ gcsched = "Running" /\ ~gcstopping /\ ~round
    /\ pstate \notin TerminalSet
    /\ round' = TRUE
    /\ UNCHANGED <<farewellVars, pstate, intent, termPublished, lost, deadlineOk, keeper,
                   leaseVars, gcsched, gcstopping, forgetVars, stopVars,
                   historyVars, reacqVars>>

GcRoundEnd ==
    /\ round
    /\ round' = FALSE
    /\ UNCHANGED <<farewellVars, pstate, intent, termPublished, lost, deadlineOk, keeper,
                   leaseVars, gcsched, gcstopping, forgetVars, stopVars,
                   historyVars, reacqVars>>

\* Self-exit at a wake that observes a terminal pool, after the round ends.
GcSelfExit ==
    /\ EnableGcSelfExit
    /\ gcsched = "Running" /\ pstate \in TerminalSet /\ ~round
    /\ gcsched' = "Stopped"
    /\ UNCHANGED <<farewellVars, pstate, intent, termPublished, lost, deadlineOk, keeper,
                   leaseVars, gcstopping, round, forgetVars, stopVars,
                   historyVars, reacqVars>>

\* `GC STOP`: under lifecycle_mutex; stop() joins an in-flight round.
VGcStop ==
    /\ mutex = "None" /\ gcsched = "Running" /\ ~round
    /\ gcsched' = "Stopped"
    /\ UNCHANGED <<farewellVars, pstate, intent, termPublished, lost, deadlineOk, keeper,
                   leaseVars, gcstopping, round, forgetVars, stopVars,
                   historyVars, reacqVars>>

\* `GC START`: checkOpAdmitted(Admin) refuses unless the pool is Live.
VGcStart ==
    /\ mutex = "None" /\ gcsched = "Stopped" /\ pstate = "Live"
    /\ gcsched' = "Running"
    /\ UNCHANGED <<farewellVars, pstate, intent, termPublished, lost, deadlineOk, keeper,
                   leaseVars, gcstopping, round, forgetVars, stopVars,
                   historyVars, reacqVars>>

(***************************************************************************)
(* A SNAPSHOT PUBLISH IN FLIGHT                                            *)
(***************************************************************************)
\* Begins only on a Live, unfenced pool; settles at any time. Not fair.
PubBegin ==
    /\ ~publishing /\ ~lost /\ pstate = "Live"
    /\ publishing' = TRUE
    /\ UNCHANGED <<lateLanding, farewellExpired, renewUnclear, pstate, intent, termPublished, lost, deadlineOk,
                   keeper, farewell, drained, leaseVars, stop, gcVars, mutex, fpc, stopVars,
                   historyVars, reacqVars>>

PubSettle ==
    /\ publishing
    /\ publishing' = FALSE /\ lateLanding' = (lateLanding \/ farewell)
    /\ UNCHANGED <<farewellExpired, renewUnclear, pstate, intent, termPublished, lost, deadlineOk,
                   keeper, farewell, drained, leaseVars, stop, gcVars, mutex, fpc, stopVars,
                   historyVars, reacqVars>>

(***************************************************************************)
(* FORGET (one instance; each step one atomic action)                      *)
(***************************************************************************)
FStart ==
    /\ fpc = "NotStarted" /\ mutex = "None"
    /\ mutex' = "Forget" /\ fpc' = "Check"
    /\ UNCHANGED <<farewellVars, pstate, intent, termPublished, lost, deadlineOk, keeper,
                   farewell, drained, leaseVars, stop, gcVars, stopVars,
                   historyVars, reacqVars>>

\* Already terminal: return. The storage-side scheduler local still
\* stop()s on return (signal + join).
FCheckShort ==
    /\ fpc = "Check" /\ pstate \in TerminalSet
    /\ gcstopping' = TRUE /\ fpc' = "ShortJoin"
    /\ UNCHANGED <<farewellVars, pstate, intent, termPublished, lost, deadlineOk, keeper,
                   farewell, drained, leaseVars, stop, gcsched, round, mutex,
                   stopVars, historyVars, reacqVars>>

FShortJoin ==
    /\ fpc = "ShortJoin" /\ ~round
    /\ gcsched' = "Destroyed" /\ fpc' = "Done" /\ mutex' = "None"
    /\ UNCHANGED <<farewellVars, pstate, intent, termPublished, lost, deadlineOk, keeper,
                   farewell, drained, leaseVars, stop, gcstopping, round,
                   stopVars, historyVars, reacqVars>>

FCheckProceed ==
    /\ fpc = "Check" /\ pstate \notin TerminalSet
    /\ fpc' = "Intent"
    /\ UNCHANGED <<farewellVars, pstate, intent, termPublished, lost, deadlineOk, keeper,
                   farewell, drained, leaseVars, stop, gcVars, mutex,
                   stopVars, historyVars, reacqVars>>

FIntent ==
    /\ fpc = "Intent"
    /\ intent' = TRUE /\ fpc' = "Trip1"
    /\ UNCHANGED <<farewellVars, pstate, termPublished, lost, deadlineOk, keeper, farewell,
                   drained, leaseVars, stop, gcVars, mutex, stopVars,
                   historyVars, reacqVars>>

FTrip1 ==
    /\ fpc = "Trip1"
    /\ lost' = TRUE
    /\ fpc' = "GcStopSignal"
    /\ UNCHANGED <<farewellVars, pstate, intent, termPublished, deadlineOk, keeper, farewell,
                   drained, leaseVars, stop, gcVars, mutex, stopVars,
                   historyVars, reacqVars>>

FGcStopSignal ==
    /\ fpc = "GcStopSignal"
    /\ gcstopping' = TRUE /\ fpc' = "GcStopJoin"
    /\ UNCHANGED <<farewellVars, pstate, intent, termPublished, lost, deadlineOk, keeper,
                   farewell, drained, leaseVars, stop, gcsched, round, mutex,
                   stopVars, historyVars, reacqVars>>

FGcStopJoin ==
    /\ fpc = "GcStopJoin" /\ ~round
    /\ gcsched' = "Destroyed" /\ fpc' = "JoinSignal"
    /\ UNCHANGED <<farewellVars, pstate, intent, termPublished, lost, deadlineOk, keeper,
                   farewell, drained, leaseVars, stop, gcstopping, round,
                   mutex, stopVars, historyVars, reacqVars>>

\* stopBackgroundWorkers: latch the stop, then join the lease thread.
FJoinSignal ==
    /\ fpc = "JoinSignal"
    /\ stop' = TRUE /\ fpc' = "JoinWait"
    /\ UNCHANGED <<farewellVars, pstate, intent, termPublished, lost, deadlineOk, keeper,
                   farewell, drained, leaseVars, gcVars, mutex, stopVars,
                   historyVars, reacqVars>>

FJoinWait ==
    /\ fpc = "JoinWait" /\ lpc = "Exited"
    /\ fpc' = "Drain"
    /\ UNCHANGED <<farewellVars, pstate, intent, termPublished, lost, deadlineOk, keeper,
                   farewell, drained, leaseVars, stop, gcVars, mutex,
                   stopVars, historyVars, reacqVars>>

FDrain ==
    /\ fpc = "Drain"
    /\ \E d \in {"Yes", "No"} : drained' = d /\ ((d = "Yes" /\ DrainWaitsPublish) => ~publishing)
    /\ fpc' = "Teardown"
    /\ UNCHANGED <<farewellVars, pstate, intent, termPublished, lost, deadlineOk, keeper,
                   farewell, leaseVars, stop, gcVars, mutex, stopVars,
                   historyVars, reacqVars>>

\* finishTeardown: `release` (the farewell) only from an Active renewer, only if drained, and not on a
\* Stopping pool: a lost mount writes nothing. The lease may have expired: the write is conditional on
\* our own token, so only `FarewellLeaseBound` makes it wait for an unexpired lease.
FTeardown ==
    /\ fpc = "Teardown"
    /\ IF keeper \in {"Active", "Terminal"}
       THEN IF /\ (keeper = "Active" \/ (SabFarewellFromTerminal /\ keeper = "Terminal"))
               /\ (drained = "Yes" \/ ~DrainGate)
               /\ (pstate # "Stopping" \/ SabFarewellOnStopping)
               /\ (deadlineOk \/ ~FarewellLeaseBound)
            THEN keeper' = "StoppedClean" /\ farewell' = TRUE /\ farewellExpired' = ~deadlineOk
            ELSE keeper' = "StoppedNoFarewell" /\ UNCHANGED <<farewell, farewellExpired>>
       ELSE UNCHANGED <<keeper, farewell, farewellExpired>>
    /\ fpc' = "KeeperReset"
    /\ UNCHANGED <<publishing, lateLanding, renewUnclear, pstate, intent, termPublished, lost, deadlineOk, drained,
                   leaseVars, stop, gcVars, mutex, stopVars, historyVars, reacqVars>>

FKeeperReset ==
    /\ fpc = "KeeperReset"
    /\ keeper' = "None" /\ fpc' = "Enter"
    /\ UNCHANGED <<farewellVars, pstate, intent, termPublished, lost, deadlineOk, farewell,
                   drained, leaseVars, stop, gcVars, mutex, stopVars,
                   historyVars, reacqVars>>

\* Closed (R18); a raced Stopping makes it a no-op.
FEnter ==
    /\ fpc = "Enter"
    /\ intent' = TRUE
    /\ IF termPublished
       THEN UNCHANGED <<pstate, termPublished, everClosed>>
       ELSE /\ pstate' = "Closed" /\ termPublished' = TRUE
            /\ everClosed' = TRUE
    /\ fpc' = "Done" /\ mutex' = "None"
    /\ UNCHANGED <<farewellVars, lost, deadlineOk, keeper, farewell, drained, leaseVars,
                   stop, gcVars, stopVars, naturalWon, everStopping, lossSeen,
                   stopByInterference, pauseEpoch, reacqVars>>

(***************************************************************************)
Next ==
    \/ LExit \/ LArmOpen
    \/ LRenewBegin \/ LRenewOk \/ LRenewEndEarly
    \/ LRenewFenced \/ LRenewLoss
    \/ LClose \/ LAlloc \/ LStepFail \/ LClaimLoss \/ LClaimed \/ LReacqAbandon
    \/ OpAdmit \/ OpDurable
    \/ PubBegin \/ PubSettle
    \/ XInterfere \/ XRaise \/ OtherObserve \/ OtherRaise \/ XExpire
    \/ GcRoundStart \/ GcRoundEnd \/ GcSelfExit \/ VGcStop \/ VGcStart
    \/ FStart \/ FCheckShort \/ FShortJoin \/ FCheckProceed \/ FIntent
    \/ FTrip1 \/ FGcStopSignal \/ FGcStopJoin \/ FJoinSignal \/ FJoinWait
    \/ FDrain \/ FTeardown \/ FKeeperReset \/ FEnter

Spec == Init /\ [][Next]_vars

\* Every FORGET step, the lease thread's exit, its arm, the end of a renewal that must end
\* early, the process-stop raise, a GC round, and the GC self-exit make progress. FStart is
\* not fair: FORGET is operator-initiated. The thread also finishes a reacquire it is in the
\* middle of. `*Ready`: the storage answers and the claim or the renewal leaves enough lease
\* to open. An idle thread starts its next renewal: a claim that leaves too little lease
\* waits for that renewal.
LClaimedReady == LClaimed /\ deadlineOk'
LRenewOkReady == LRenewOk /\ deadlineOk'
FairSpec ==
    /\ Spec
    /\ WF_vars(FCheckShort) /\ WF_vars(FShortJoin) /\ WF_vars(FCheckProceed)
    /\ WF_vars(FIntent) /\ WF_vars(FTrip1) /\ WF_vars(FGcStopSignal)
    /\ WF_vars(FGcStopJoin) /\ WF_vars(FJoinSignal) /\ WF_vars(FJoinWait)
    /\ WF_vars(FDrain) /\ WF_vars(FTeardown)
    /\ WF_vars(FKeeperReset) /\ WF_vars(FEnter)
    /\ WF_vars(LExit) /\ WF_vars(LArmOpen)
    /\ WF_vars(LRenewEndEarly) /\ WF_vars(XRaise)
    /\ WF_vars(GcRoundEnd) /\ WF_vars(GcSelfExit)
    /\ WF_vars(LClose) /\ WF_vars(LAlloc) /\ WF_vars(LReacqAbandon) /\ WF_vars(LRenewBegin)
    /\ SF_vars(LClaimedReady) /\ SF_vars(LRenewOkReady)

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

\* A pool that was ever Stopping never writes the farewell.
NoFarewellOnStopping == ~(farewell /\ everStopping)

\* A publish settling after the farewell would land behind it.
NoLandingAfterFarewell == ~lateLanding

\* The farewell is withheld when the last renewal ended without a confirmed lease.
NoFarewellAfterUnclearRenewal == ~(farewell /\ renewUnclear)

I3OneWay ==
    /\ everStopping => pstate = "Stopping"
    /\ everClosed => pstate = "Closed"

\* A terminal pool never coexists with an armed fence.
I4bTerminalImpliesLost == pstate \in TerminalSet => lost

\* A pool that reacquires has the fence latched.
I4cReacquiringImpliesLost == pstate = "Reacquiring" => lost

\* One epoch is allocated per fence, however many steps fail.
OneAllocationPerFence == allocCount <= 1

\* An operation admitted under an older epoch never lands under a newer one.
OldEpochOpRefused == ~staleOpLanded

\* Live is left only by a loss, FORGET, or the start of a reacquire.
LiveLeavesOnlyByFenceOrEnd ==
    [][(pstate = "Live" /\ pstate' # "Live") =>
        (pstate' \in TerminalSet \/ (pstate' = "Reacquiring" /\ lpc' = "Closing"))]_vars

\* A reacquire opens writes under exactly the next epoch.
OneEpochPerFence ==
    [][(pstate = "Reacquiring" /\ pstate' = "Live") => epoch' = fenceEpoch + 1]_vars

\* Writes never open over a stop, the latch or an expired lease.
NoOpenOverEnd ==
    [][(pstate = "Reacquiring" /\ pstate' = "Live") => (~intent /\ ~stop /\ deadlineOk')]_vars

\* Liveness: a reacquire ends, or the process is stopping.
ReacquireSettles == (pstate = "Reacquiring") ~> (pstate # "Reacquiring" \/ stop)

I6GcStoppedAfterForget == ForgetDone => gcsched = "Destroyed"

\* The thread renews only a renewer that is not RenewalTerminal.
I8RenewsActiveRenewer == lpc = "Renewing" => keeper = "Active"

\* Every loss observation (R9-R17) ends terminal; it never reacquires.
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
            "Drain", "Teardown", "KeeperReset", "Enter"}
ForgetWinsArm == [][(ForgetPastIntent => ~(lost /\ ~lost'))]_vars

\* An expiry changes no epoch, fence latch or lifecycle.
PauseKeepsEpoch ==
    [][((deadlineOk /\ ~deadlineOk' /\ lpc' = lpc)
          => UNCHANGED <<epoch, lost, pstate>>)]_vars

\* Liveness (FairSpec): a started FORGET always completes.
ForgetCompletes == (fpc = "Check") ~> ForgetDone

\* Liveness (FairSpec): once terminal, the GC scheduler stops ticking.
GcExitsAfterTerminal == (pstate \in TerminalSet) ~> (gcsched # "Running")

\* Liveness (FairSpec): the lease thread exits on a stop or the latch.
LeaseThreadExits == (stop \/ LeaseTerminal) ~> (lpc = "Exited")

\* Liveness (FairSpec): a Stopping disk gets the process stop requested.
StopIsRequested == (pstate = "Stopping") ~> (stopRequests > 0)

\* Witnesses (expected VIOLATED in their configs = reachability).
WForgetNeverDone == ~ForgetDone
WNoRacedStopping == ~(ForgetDone /\ pstate = "Stopping")
\* No reacquire arms the fence once FORGET has joined the lease thread.
NoJoinWindowReclaim == ~(fpc = "Drain" /\ pstate = "Live" /\ ~lost)
WNoLiveByRenewal == ~liveByRenewal
\* A reacquire opens writes under the next epoch.
WNoReacquire == ~reacqLive
WNoForgetFarewell == ~(ForgetDone /\ farewell)
\* The farewell is written after the lease expired.
WNoFarewellAfterExpiry == ~farewellExpired
\* Expiry, then a renewal, then writes admitted under the same epoch.
WNoPauseResume ==
    ~(pauseEpoch = epoch /\ pstate = "Live" /\ ~lost /\ deadlineOk)
\* Interference stops this disk; both disks raise; one request.
WNoInterferenceStop ==
    ~(stopByInterference /\ pstate = "Stopping" /\ d1stop = "Raised"
      /\ other = "Done" /\ stopRequests = 1)

=============================================================================
