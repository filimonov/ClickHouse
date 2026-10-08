-------------------------- MODULE CaMountRenewRetryCore --------------------------
(*
Focused safety model for split-phase conditional mount renewal without a lease deadline.

One old-epoch runtime starts from a confirmed predecessor and runs two logical renewals in
sequence. A logical renewal captures its attempt ID, body, expected token, body deadline, start,
and cadence anchor before I/O. Physical retries are collapsed to `outstanding`: every copy carries
that one tuple. A renewal is never bounded by the lease: it sends and retries past the confirmed
deadline and ends only on success, a definitive answer, or a cancel. Backend landing, response
delivery/loss, exact reads, local terminalization, and post-terminal delivery remain separate
actions. Copies still in flight when the next renewal starts become `staleOutstanding`.

Write authority is the holder's own timer, `LocalAuthority`: `clock < confirmedDeadline`, unless a
terminal outcome tripped the fence. Success sets the deadline to the renewal's start plus the lease.

The durable observers deliberately run at any time, also while local confirmation is pending. They
can install a same-pair twin, GC-fence or reclaim a body, install a successor epoch, or install a
foreign holder. They never touch local state. A takeover is entitled once the current token has
been stable for one lease (`tokenStableUntil`), or for the successor once the old runtime is locally
terminal. Honest conditional landing consumes the expected token; the multiplicity sabotage is the
sole action that lets two copies replace it.

Each logical renewal runs as one or more engine calls (`CasOperation::writeLoop`). `callAmbiguous`
is the engine's "an attempt of this call has an unknown fate". A definite refusal of the current
attempt (`DenyCurrent`) or a clean `412` (`RefuseCurrent`) ends the call only while the call has no
ambiguous attempt; otherwise a read settles it. `Refused` continues with a new engine call that
carries the same tuple. A clean `412` whose resolving read fails (`Conflict{NotObserved}`) repeats
only the read. Copies whose answer the holder no longer waits for are ghosts (`GhostExists`).
Writes open only through `OpenWrites`, and the holder's own timer closes them in `AdvanceTime`.
*)
EXTENDS Naturals, FiniteSets

CONSTANTS
    AttemptIds,
    FirstAttempt,
    SecondAttempt,
    OtherAttempt,
    Holders,
    SelfHolder,
    ForeignHolder,
    PredecessorToken,
    FirstRenewalToken,
    SecondRenewalToken,
    TwinToken,
    SuccessorToken,
    ForeignToken,
    InitialConfirmedDeadline,
    MaxTime,
    LeaseDuration,
    CadencePeriod,
    MaxRetries,
    MaxOutstanding,
    MaxEngineCalls,
    Defects

AllowedDefects ==
    {"ignore_attempt_id",
     "refresh_deadline_from_response",
     "anchor_deadline_at_retry",
     "retry_with_new_body",
     "accept_after_terminal",
     "accept_successor",
     "drop_pending_on_terminal",
     "late_rearm",
     "response_relative_cadence",
     "double_conditional_landing",
     "refused_while_ambiguous",
     "repeat_put_after_not_observed",
     "not_observed_concludes",
     "open_writes_on_late_success",
     "refresh_resets_start",
     "new_call_new_attempt_id"}

Defect(name) == name \in Defects

NoAttempt == "NoAttempt"
NoHolder == "NoHolder"
NoToken == "NoToken"

RenewalBody == "RenewalBody"
RetryBody == "RetryBody"
PredecessorBody == "PredecessorBody"
TwinBody == RenewalBody
SuccessorBody == "SuccessorBody"
ForeignBody == "ForeignBody"
NoBody == "NoBody"

Rounds == 1..2

AttemptOf(r) == IF r = 1 THEN FirstAttempt ELSE SecondAttempt
RenewalTokenOf(r) == IF r = 1 THEN FirstRenewalToken ELSE SecondRenewalToken

BodyNames ==
    {RenewalBody, RetryBody, PredecessorBody, SuccessorBody, ForeignBody, NoBody}
DurableKinds == {"Predecessor", "Renewal", "Twin", "Successor", "Foreign", "Absent"}
LocalStates ==
    {"Idle", "Sent", "Landed", "ResponseLost", "Resolved", "RetryWait",
     "Committed", "Conflict", "Unresolved", "Refused", "CleanConflict", "NotObserved"}
ResolveOutcomes == {"None", "Exact", "Predecessor", "Conflict", "NotObserved"}
AdoptedKinds == {"None", "Renewal", "Twin", "Successor", "Foreign", "GCFenced"}

TokenSet ==
    {PredecessorToken, FirstRenewalToken, SecondRenewalToken, TwinToken, SuccessorToken,
     ForeignToken, NoToken}
AttemptSet == AttemptIds \cup {NoAttempt}
HolderSet == Holders \cup {NoHolder}
DeadlineRange == 0..(MaxTime + LeaseDuration)

DurableState ==
    [kind : DurableKinds,
     token : TokenSet,
     holder : HolderSet,
     epoch : 0..1,
     attempt : AttemptSet,
     body : BodyNames,
     deadline : DeadlineRange]

HistoryState ==
    [tupleChanged : BOOLEAN,
     deadlineFromResponse : BOOLEAN,
     rearmedAfterTerminal : BOOLEAN,
     badAcknowledgement : BOOLEAN,
     lateOverwroteSuccessor : BOOLEAN,
     terminalDroppedPending : BOOLEAN,
     entitledTakeover : BOOLEAN,
     sawNonLandedTransient : BOOLEAN,
     sawLandedResponseLoss : BOOLEAN,
     sawRetrySent : BOOLEAN,
     directRetryReached : BOOLEAN,
     readAdoptionReached : BOOLEAN,
     lateOldLanded : BOOLEAN,
     lateBeforeReclaimReached : BOOLEAN,
     oldCopyRefused : BOOLEAN,
     lateAfterSuccessorReached : BOOLEAN,
     slowSuccess : BOOLEAN,
     catchupReached : BOOLEAN,
     retryPastDeadlineReached : BOOLEAN,
     staleSuccessSeen : BOOLEAN,
     authorityRestoredReached : BOOLEAN,
     ownLandingLost : BOOLEAN,
     putAfterCleanConflict : BOOLEAN,
     concludedUnobserved : BOOLEAN,
     refusedThenCommitReached : BOOLEAN,
     notObservedThenObservedReached : BOOLEAN,
     refreshThenCommitReached : BOOLEAN,
     writesReopenedReached : BOOLEAN]

InitialDurable ==
    [kind |-> "Predecessor",
     token |-> PredecessorToken,
     holder |-> SelfHolder,
     epoch |-> 0,
     attempt |-> NoAttempt,
     body |-> PredecessorBody,
     deadline |-> InitialConfirmedDeadline]

EmptyDurable ==
    [kind |-> "Absent",
     token |-> NoToken,
     holder |-> NoHolder,
     epoch |-> 0,
     attempt |-> NoAttempt,
     body |-> NoBody,
     deadline |-> 0]

InitialHistory ==
    [tupleChanged |-> FALSE,
     deadlineFromResponse |-> FALSE,
     rearmedAfterTerminal |-> FALSE,
     badAcknowledgement |-> FALSE,
     lateOverwroteSuccessor |-> FALSE,
     terminalDroppedPending |-> FALSE,
     entitledTakeover |-> FALSE,
     sawNonLandedTransient |-> FALSE,
     sawLandedResponseLoss |-> FALSE,
     sawRetrySent |-> FALSE,
     directRetryReached |-> FALSE,
     readAdoptionReached |-> FALSE,
     lateOldLanded |-> FALSE,
     lateBeforeReclaimReached |-> FALSE,
     oldCopyRefused |-> FALSE,
     lateAfterSuccessorReached |-> FALSE,
     slowSuccess |-> FALSE,
     catchupReached |-> FALSE,
     retryPastDeadlineReached |-> FALSE,
     staleSuccessSeen |-> FALSE,
     authorityRestoredReached |-> FALSE,
     ownLandingLost |-> FALSE,
     putAfterCleanConflict |-> FALSE,
     concludedUnobserved |-> FALSE,
     refusedThenCommitReached |-> FALSE,
     notObservedThenObservedReached |-> FALSE,
     refreshThenCommitReached |-> FALSE,
     writesReopenedReached |-> FALSE]

VARIABLES
    clock,
    durable,
    gcFenced,
    tokenStableUntil,
    confirmedBody,
    confirmedToken,
    confirmedDeadline,
    confirmedAnchor,
    committedRound,
    fenceLost,
    localState,
    resolveOutcome,
    round,
    requestAttempt,
    requestBody,
    requestExpectedToken,
    requestDeadline,
    attemptStartedAt,
    cadenceAnchor,
    firstSendAt,
    retryCount,
    outstanding,
    staleOutstanding,
    landings,
    adoptedAttempt,
    adoptedKind,
    terminalSeen,
    cancelled,
    successObservedAt,
    nextBeatScheduled,
    nextBeat,
    reclaimSeen,
    callAmbiguous,
    engineCalls,
    refreshedInCall,
    preconditionRefused,
    writesOpen,
    history

CallVars == <<callAmbiguous, engineCalls, refreshedInCall, preconditionRefused>>

vars ==
    <<clock, durable, gcFenced, tokenStableUntil,
      confirmedBody, confirmedToken, confirmedDeadline, confirmedAnchor, committedRound,
      fenceLost, localState, resolveOutcome,
      round, requestAttempt, requestBody, requestExpectedToken, requestDeadline,
      attemptStartedAt, cadenceAnchor, firstSendAt,
      retryCount, outstanding, staleOutstanding, landings,
      adoptedAttempt, adoptedKind, terminalSeen, cancelled, successObservedAt,
      nextBeatScheduled, nextBeat, reclaimSeen, CallVars, writesOpen, history>>

Init ==
    /\ clock = 0
    /\ durable = InitialDurable
    /\ gcFenced = FALSE
    /\ tokenStableUntil = InitialConfirmedDeadline
    /\ confirmedBody = PredecessorBody
    /\ confirmedToken = PredecessorToken
    /\ confirmedDeadline = InitialConfirmedDeadline
    /\ confirmedAnchor = 0
    /\ committedRound = 0
    /\ fenceLost = FALSE
    /\ localState = "Idle"
    /\ resolveOutcome = "None"
    /\ round = 0
    /\ requestAttempt = FirstAttempt
    /\ requestBody = RenewalBody
    /\ requestExpectedToken = PredecessorToken
    /\ requestDeadline = 0
    /\ attemptStartedAt = 0
    /\ cadenceAnchor = 0
    /\ firstSendAt = [r \in Rounds |-> 0]
    /\ retryCount = 0
    /\ outstanding = 0
    /\ staleOutstanding = 0
    /\ landings = [r \in Rounds |-> 0]
    /\ adoptedAttempt = NoAttempt
    /\ adoptedKind = "None"
    /\ terminalSeen = FALSE
    /\ cancelled = FALSE
    /\ successObservedAt = 0
    /\ nextBeatScheduled = FALSE
    /\ nextBeat = 0
    /\ reclaimSeen = FALSE
    /\ callAmbiguous = FALSE
    /\ engineCalls = 0
    /\ refreshedInCall = FALSE
    /\ preconditionRefused = FALSE
    /\ writesOpen = TRUE
    /\ history = InitialHistory

LogicalCreated == round > 0

(* The holder's write authority: its own timer and its own fence, nothing another process wrote. *)
LocalAuthority == ~fenceLost /\ clock < confirmedDeadline

(* An observer that waited one lease after the current token appeared may act on the slot. *)
ObserverWaitedOut == clock >= tokenStableUntil

RequestDurable ==
    [kind |-> "Renewal",
     token |-> RenewalTokenOf(round),
     holder |-> SelfHolder,
     epoch |-> 0,
     attempt |-> requestAttempt,
     body |-> requestBody,
     deadline |-> requestDeadline]

StaleDurable ==
    [kind |-> "Renewal",
     token |-> FirstRenewalToken,
     holder |-> SelfHolder,
     epoch |-> 0,
     attempt |-> FirstAttempt,
     body |-> RenewalBody,
     deadline |-> firstSendAt[1] + LeaseDuration]

TwinDurable ==
    [kind |-> "Twin",
     token |-> TwinToken,
     holder |-> SelfHolder,
     epoch |-> 0,
     attempt |-> OtherAttempt,
     body |-> TwinBody,
     deadline |-> requestDeadline]

SuccessorDurable ==
    [kind |-> "Successor",
     token |-> SuccessorToken,
     holder |-> SelfHolder,
     epoch |-> 1,
     attempt |-> OtherAttempt,
     body |-> SuccessorBody,
     deadline |-> MaxTime + LeaseDuration]

ForeignDurable ==
    [kind |-> "Foreign",
     token |-> ForeignToken,
     holder |-> ForeignHolder,
     epoch |-> 0,
     attempt |-> OtherAttempt,
     body |-> ForeignBody,
     deadline |-> MaxTime + LeaseDuration]

ExactRequestDurable ==
    /\ ~gcFenced
    /\ durable.kind = "Renewal"
    /\ durable.token = RenewalTokenOf(round)
    /\ durable.holder = SelfHolder
    /\ durable.epoch = 0
    /\ durable.attempt = AttemptOf(round)
    /\ durable.body = RenewalBody
    /\ durable.deadline = requestDeadline

TwinMatchesIgnoringId ==
    /\ ~gcFenced
    /\ durable.kind = "Twin"
    /\ durable.holder = SelfHolder
    /\ durable.epoch = 0
    /\ durable.attempt = OtherAttempt
    /\ durable.body = RenewalBody

PredecessorStillCurrent ==
    /\ ~gcFenced
    /\ durable.token = requestExpectedToken

TypeOK ==
    /\ AttemptIds = {FirstAttempt, SecondAttempt, OtherAttempt}
    /\ Cardinality(AttemptIds) = 3
    /\ Holders = {SelfHolder, ForeignHolder}
    /\ SelfHolder /= ForeignHolder
    /\ Cardinality({PredecessorToken, FirstRenewalToken, SecondRenewalToken, TwinToken,
                    SuccessorToken, ForeignToken}) = 6
    /\ NoToken \notin {PredecessorToken, FirstRenewalToken, SecondRenewalToken, TwinToken,
                       SuccessorToken, ForeignToken}
    /\ InitialConfirmedDeadline = 3
    /\ MaxTime = 5
    /\ LeaseDuration = 4
    /\ CadencePeriod = 2
    /\ MaxRetries = 1
    /\ MaxOutstanding = 2
    /\ MaxEngineCalls = 1
    /\ Defects \subseteq AllowedDefects
    /\ Cardinality(Defects) <= 1
    /\ clock \in 0..MaxTime
    /\ durable \in DurableState
    /\ gcFenced \in BOOLEAN
    /\ tokenStableUntil \in DeadlineRange
    /\ confirmedBody \in BodyNames
    /\ confirmedToken \in TokenSet
    /\ confirmedDeadline \in DeadlineRange
    /\ confirmedAnchor \in 0..MaxTime
    /\ committedRound \in 0..2
    /\ fenceLost \in BOOLEAN
    /\ localState \in LocalStates
    /\ resolveOutcome \in ResolveOutcomes
    /\ round \in 0..2
    /\ requestAttempt \in AttemptIds
    /\ requestBody \in {RenewalBody, RetryBody}
    /\ requestExpectedToken \in TokenSet
    /\ requestDeadline \in DeadlineRange
    /\ attemptStartedAt \in 0..MaxTime
    /\ cadenceAnchor \in 0..MaxTime
    /\ firstSendAt \in [Rounds -> 0..MaxTime]
    /\ retryCount \in 0..MaxRetries
    /\ outstanding \in 0..MaxOutstanding
    /\ staleOutstanding \in 0..MaxOutstanding
    /\ landings \in [Rounds -> 0..MaxOutstanding]
    /\ adoptedAttempt \in AttemptSet
    /\ adoptedKind \in AdoptedKinds
    /\ terminalSeen \in BOOLEAN
    /\ cancelled \in BOOLEAN
    /\ successObservedAt \in 0..MaxTime
    /\ nextBeatScheduled \in BOOLEAN
    /\ nextBeat \in 0..(MaxTime + CadencePeriod)
    /\ reclaimSeen \in BOOLEAN
    /\ callAmbiguous \in BOOLEAN
    /\ engineCalls \in 0..MaxEngineCalls
    /\ refreshedInCall \in BOOLEAN
    /\ preconditionRefused \in BOOLEAN
    /\ writesOpen \in BOOLEAN
    /\ history \in HistoryState
    /\ (LogicalCreated => requestDeadline = firstSendAt[round] + LeaseDuration)
    /\ (nextBeatScheduled => localState = "Committed")

(* The holder's own timer closes writes at the confirmed deadline; nothing else needs to. *)
AdvanceTime ==
    /\ clock < MaxTime
    /\ clock' = clock + 1
    /\ writesOpen' = (writesOpen /\ clock + 1 < confirmedDeadline)
    /\ UNCHANGED
        <<durable, gcFenced, tokenStableUntil,
          confirmedBody, confirmedToken, confirmedDeadline, confirmedAnchor, committedRound,
          fenceLost, localState, resolveOutcome,
          round, requestAttempt, requestBody, requestExpectedToken, requestDeadline,
          attemptStartedAt, cadenceAnchor, firstSendAt,
          retryCount, outstanding, staleOutstanding, landings,
          adoptedAttempt, adoptedKind, terminalSeen, cancelled, successObservedAt,
          nextBeatScheduled, nextBeat, reclaimSeen, CallVars, history>>

(* The first renewal starts at any time; the second one when the cadence after a commit is due.
   Neither needs authority: the start may already be past the confirmed deadline. *)
SendRenewal ==
    /\ ~terminalSeen
    /\ \/ round = 0
       \/ /\ round = 1
          /\ localState = "Committed"
          /\ nextBeatScheduled
          /\ clock >= nextBeat
          /\ ~cancelled
    /\ round' = round + 1
    /\ requestAttempt' = AttemptOf(round + 1)
    /\ requestBody' = RenewalBody
    /\ requestExpectedToken' = confirmedToken
    /\ requestDeadline' = clock + LeaseDuration
    /\ attemptStartedAt' = clock
    /\ cadenceAnchor' = clock
    /\ firstSendAt' = [firstSendAt EXCEPT ![round + 1] = clock]
    /\ localState' = "Sent"
    /\ resolveOutcome' = "None"
    /\ retryCount' = 0
    /\ outstanding' = 1
    /\ staleOutstanding' = staleOutstanding + outstanding
    /\ nextBeatScheduled' = FALSE
    /\ callAmbiguous' = FALSE
    /\ engineCalls' = 0
    /\ refreshedInCall' = FALSE
    /\ preconditionRefused' = FALSE
    /\ UNCHANGED
        <<clock, durable, gcFenced, tokenStableUntil,
          confirmedBody, confirmedToken, confirmedDeadline, confirmedAnchor, committedRound,
          fenceLost, landings, adoptedAttempt, adoptedKind,
          terminalSeen, cancelled, successObservedAt, nextBeat, reclaimSeen, writesOpen, history>>

(* The current attempt's answer is lost, so its fate is unknown for the rest of this engine call. *)
ResponseLoss ==
    /\ LogicalCreated
    /\ ~terminalSeen
    /\ localState \in {"Sent", "Landed"}
    /\ localState' = "ResponseLost"
    /\ resolveOutcome' = "None"
    /\ callAmbiguous' = TRUE
    /\ history' =
        [history EXCEPT
            !.sawNonLandedTransient = @ \/ (localState = "Sent"),
            !.sawLandedResponseLoss = @ \/ (localState = "Landed")]
    /\ UNCHANGED
        <<clock, durable, gcFenced, tokenStableUntil,
          confirmedBody, confirmedToken, confirmedDeadline, confirmedAnchor, committedRound,
          fenceLost, round, requestAttempt, requestBody, requestExpectedToken, requestDeadline,
          attemptStartedAt, cadenceAnchor, firstSendAt,
          retryCount, outstanding, staleOutstanding, landings,
          adoptedAttempt, adoptedKind, terminalSeen, cancelled, successObservedAt,
          nextBeatScheduled, nextBeat, reclaimSeen,
          engineCalls, refreshedInCall, preconditionRefused, writesOpen>>

(* In `Sent` one of the outstanding copies is the attempt the holder is waiting on. *)
GhostExists ==
    outstanding > (IF localState = "Sent" THEN 1 ELSE 0)

(* The attempt the holder is waiting on lands and its success response is still to come. *)
LandCurrent ==
    /\ LogicalCreated
    /\ ~terminalSeen
    /\ localState = "Sent"
    /\ outstanding > 0
    /\ PredecessorStillCurrent
    /\ landings[round] < MaxOutstanding
    /\ durable' = RequestDurable
    /\ gcFenced' = FALSE
    /\ tokenStableUntil' = clock + LeaseDuration
    /\ outstanding' = outstanding - 1
    /\ landings' = [landings EXCEPT ![round] = @ + 1]
    /\ localState' = "Landed"
    /\ UNCHANGED
        <<clock, confirmedBody, confirmedToken, confirmedDeadline, confirmedAnchor,
          committedRound, fenceLost, resolveOutcome,
          round, requestAttempt, requestBody, requestExpectedToken, requestDeadline,
          attemptStartedAt, cadenceAnchor, firstSendAt, retryCount, staleOutstanding,
          adoptedAttempt, adoptedKind, terminalSeen, cancelled, successObservedAt,
          nextBeatScheduled, nextBeat, reclaimSeen, CallVars, writesOpen, history>>

(* An earlier copy of this renewal lands; nobody waits for its answer. *)
LandGhost ==
    /\ LogicalCreated
    /\ ~terminalSeen
    /\ localState \notin {"Committed", "Unresolved"}
    /\ GhostExists
    /\ PredecessorStillCurrent
    /\ landings[round] < MaxOutstanding
    /\ durable' = RequestDurable
    /\ gcFenced' = FALSE
    /\ tokenStableUntil' = clock + LeaseDuration
    /\ outstanding' = outstanding - 1
    /\ landings' = [landings EXCEPT ![round] = @ + 1]
    /\ UNCHANGED
        <<clock, confirmedBody, confirmedToken, confirmedDeadline, confirmedAnchor,
          committedRound, fenceLost, localState, resolveOutcome,
          round, requestAttempt, requestBody, requestExpectedToken, requestDeadline,
          attemptStartedAt, cadenceAnchor, firstSendAt, retryCount, staleOutstanding,
          adoptedAttempt, adoptedKind, terminalSeen, cancelled, successObservedAt,
          nextBeatScheduled, nextBeat, reclaimSeen, CallVars, writesOpen, history>>

(* A `412` for the attempt the holder waits on. With an ambiguous attempt in the call the read must
   settle that attempt; without one the `412` is the answer and the read only names the occupant. *)
RefuseCurrent ==
    /\ LogicalCreated
    /\ ~terminalSeen
    /\ localState = "Sent"
    /\ outstanding > 0
    /\ ~PredecessorStillCurrent
    /\ outstanding' = outstanding - 1
    /\ localState' = IF callAmbiguous THEN "ResponseLost" ELSE "CleanConflict"
    /\ resolveOutcome' = "None"
    /\ preconditionRefused' = (preconditionRefused \/ ~callAmbiguous)
    /\ UNCHANGED
        <<clock, durable, gcFenced, tokenStableUntil,
          confirmedBody, confirmedToken, confirmedDeadline, confirmedAnchor, committedRound,
          fenceLost, round, requestAttempt, requestBody, requestExpectedToken, requestDeadline,
          attemptStartedAt, cadenceAnchor, firstSendAt,
          retryCount, staleOutstanding, landings,
          adoptedAttempt, adoptedKind, terminalSeen, cancelled, successObservedAt,
          nextBeatScheduled, nextBeat, reclaimSeen,
          callAmbiguous, engineCalls, refreshedInCall, writesOpen, history>>

RefuseGhost ==
    /\ LogicalCreated
    /\ ~terminalSeen
    /\ localState \notin {"Committed", "Unresolved"}
    /\ GhostExists
    /\ ~PredecessorStillCurrent
    /\ outstanding' = outstanding - 1
    /\ UNCHANGED
        <<clock, durable, gcFenced, tokenStableUntil,
          confirmedBody, confirmedToken, confirmedDeadline, confirmedAnchor, committedRound,
          fenceLost, localState, resolveOutcome,
          round, requestAttempt, requestBody, requestExpectedToken, requestDeadline,
          attemptStartedAt, cadenceAnchor, firstSendAt,
          retryCount, staleOutstanding, landings,
          adoptedAttempt, adoptedKind, terminalSeen, cancelled, successObservedAt,
          nextBeatScheduled, nextBeat, reclaimSeen, CallVars, writesOpen, history>>

(* A definite refusal (`AccessDenied`, `MalformedXML`, ...): the attempt did not apply. The engine
   returns `Refused` only when no attempt of the call is ambiguous; otherwise the read settles. *)
DenyCurrent ==
    /\ LogicalCreated
    /\ ~terminalSeen
    /\ localState = "Sent"
    /\ outstanding > 0
    /\ outstanding' = outstanding - 1
    /\ localState' =
        IF callAmbiguous /\ ~Defect("refused_while_ambiguous")
        THEN "ResponseLost"
        ELSE "Refused"
    /\ resolveOutcome' = "None"
    /\ UNCHANGED
        <<clock, durable, gcFenced, tokenStableUntil,
          confirmedBody, confirmedToken, confirmedDeadline, confirmedAnchor, committedRound,
          fenceLost, round, requestAttempt, requestBody, requestExpectedToken, requestDeadline,
          attemptStartedAt, cadenceAnchor, firstSendAt,
          retryCount, staleOutstanding, landings,
          adoptedAttempt, adoptedKind, terminalSeen, cancelled, successObservedAt,
          nextBeatScheduled, nextBeat, reclaimSeen, CallVars, writesOpen, history>>

(* The first credential answer of a call refreshes credentials. Without an ambiguous attempt the
   engine reissues at once; with one the read settles first. The tuple and start stay. *)
CredentialRefresh ==
    /\ LogicalCreated
    /\ ~terminalSeen
    /\ localState = "Sent"
    /\ outstanding > 0
    /\ ~refreshedInCall
    /\ refreshedInCall' = TRUE
    /\ IF callAmbiguous
       THEN /\ outstanding' = outstanding - 1
            /\ localState' = "ResponseLost"
            /\ resolveOutcome' = "None"
       ELSE UNCHANGED <<outstanding, localState, resolveOutcome>>
    /\ attemptStartedAt' =
        IF Defect("refresh_resets_start") THEN clock ELSE attemptStartedAt
    /\ history' =
        [history EXCEPT
            !.putAfterCleanConflict = @ \/ (~callAmbiguous /\ preconditionRefused)]
    /\ UNCHANGED
        <<clock, durable, gcFenced, tokenStableUntil,
          confirmedBody, confirmedToken, confirmedDeadline, confirmedAnchor, committedRound,
          fenceLost, round, requestAttempt, requestBody, requestExpectedToken, requestDeadline,
          cadenceAnchor, firstSendAt,
          retryCount, staleOutstanding, landings,
          adoptedAttempt, adoptedKind, terminalSeen, cancelled, successObservedAt,
          nextBeatScheduled, nextBeat, reclaimSeen,
          callAmbiguous, engineCalls, preconditionRefused, writesOpen>>

(* `Refused` is not an outcome of the renewal: a new engine call resends the same tuple. *)
NewEngineCall ==
    /\ LogicalCreated
    /\ ~terminalSeen
    /\ ~cancelled
    /\ localState = "Refused"
    /\ engineCalls < MaxEngineCalls
    /\ outstanding < MaxOutstanding
    /\ localState' = "Sent"
    /\ resolveOutcome' = "None"
    /\ outstanding' = outstanding + 1
    /\ engineCalls' = engineCalls + 1
    /\ callAmbiguous' = FALSE
    /\ refreshedInCall' = FALSE
    /\ requestAttempt' =
        IF Defect("new_call_new_attempt_id") THEN OtherAttempt ELSE requestAttempt
    /\ history' =
        [history EXCEPT
            !.tupleChanged = @ \/ Defect("new_call_new_attempt_id"),
            !.putAfterCleanConflict = @ \/ preconditionRefused]
    /\ UNCHANGED
        <<clock, durable, gcFenced, tokenStableUntil,
          confirmedBody, confirmedToken, confirmedDeadline, confirmedAnchor, committedRound,
          fenceLost, round, requestBody, requestExpectedToken, requestDeadline,
          attemptStartedAt, cadenceAnchor, firstSendAt,
          retryCount, staleOutstanding, landings,
          adoptedAttempt, adoptedKind, terminalSeen, cancelled, successObservedAt,
          nextBeatScheduled, nextBeat, reclaimSeen, preconditionRefused, writesOpen>>

(* The read after a clean `412`, repeated while it fails. Only an observation of the slot concludes;
   the loss table classifies it outside this model. *)
ResolvingRead ==
    /\ LogicalCreated
    /\ ~terminalSeen
    /\ localState \in {"CleanConflict", "NotObserved"}
    /\ \/ /\ localState' = "Conflict"
          /\ resolveOutcome' = "Conflict"
          /\ history' =
              [history EXCEPT
                  !.ownLandingLost = @ \/ ExactRequestDurable,
                  !.notObservedThenObservedReached = @ \/ (localState = "NotObserved")]
       \/ /\ ~Defect("not_observed_concludes")
          /\ localState' = "NotObserved"
          /\ resolveOutcome' = "NotObserved"
          /\ UNCHANGED history
       \/ /\ Defect("not_observed_concludes")
          /\ localState' = "Conflict"
          /\ resolveOutcome' = "Conflict"
          /\ history' = [history EXCEPT !.concludedUnobserved = TRUE]
    /\ UNCHANGED
        <<clock, durable, gcFenced, tokenStableUntil,
          confirmedBody, confirmedToken, confirmedDeadline, confirmedAnchor, committedRound,
          fenceLost, round, requestAttempt, requestBody, requestExpectedToken, requestDeadline,
          attemptStartedAt, cadenceAnchor, firstSendAt,
          retryCount, outstanding, staleOutstanding, landings,
          adoptedAttempt, adoptedKind, terminalSeen, cancelled, successObservedAt,
          nextBeatScheduled, nextBeat, reclaimSeen, CallVars, writesOpen>>

RepeatPutAfterNotObserved ==
    /\ Defect("repeat_put_after_not_observed")
    /\ LogicalCreated
    /\ ~terminalSeen
    /\ ~cancelled
    /\ localState = "NotObserved"
    /\ outstanding < MaxOutstanding
    /\ localState' = "Sent"
    /\ resolveOutcome' = "None"
    /\ outstanding' = outstanding + 1
    /\ history' =
        [history EXCEPT !.putAfterCleanConflict = @ \/ preconditionRefused]
    /\ UNCHANGED
        <<clock, durable, gcFenced, tokenStableUntil,
          confirmedBody, confirmedToken, confirmedDeadline, confirmedAnchor, committedRound,
          fenceLost, round, requestAttempt, requestBody, requestExpectedToken, requestDeadline,
          attemptStartedAt, cadenceAnchor, firstSendAt,
          retryCount, staleOutstanding, landings,
          adoptedAttempt, adoptedKind, terminalSeen, cancelled, successObservedAt,
          nextBeatScheduled, nextBeat, reclaimSeen, CallVars, writesOpen>>

(* A copy of the previous renewal is conditional on a token that renewal already replaced. *)
StaleCopyDelivery ==
    /\ staleOutstanding > 0
    /\ staleOutstanding' = staleOutstanding - 1
    /\ IF ~gcFenced /\ durable.token = PredecessorToken
       THEN
           /\ durable' = StaleDurable
           /\ tokenStableUntil' = clock + LeaseDuration
           /\ landings' = [landings EXCEPT ![1] = @ + 1]
       ELSE
           UNCHANGED <<durable, tokenStableUntil, landings>>
    /\ UNCHANGED
        <<clock, gcFenced,
          confirmedBody, confirmedToken, confirmedDeadline, confirmedAnchor, committedRound,
          fenceLost, localState, resolveOutcome,
          round, requestAttempt, requestBody, requestExpectedToken, requestDeadline,
          attemptStartedAt, cadenceAnchor, firstSendAt, retryCount, outstanding,
          adoptedAttempt, adoptedKind, terminalSeen, cancelled, successObservedAt,
          nextBeatScheduled, nextBeat, reclaimSeen, CallVars, writesOpen, history>>

DoubleConditionalLanding ==
    /\ Defect("double_conditional_landing")
    /\ LogicalCreated
    /\ ~terminalSeen
    /\ outstanding >= 2
    /\ landings[round] = 0
    /\ PredecessorStillCurrent
    /\ durable' = RequestDurable
    /\ gcFenced' = FALSE
    /\ tokenStableUntil' = clock + LeaseDuration
    /\ outstanding' = outstanding - 2
    /\ landings' = [landings EXCEPT ![round] = @ + 2]
    /\ localState' = IF localState = "ResponseLost" THEN localState ELSE "Landed"
    /\ UNCHANGED
        <<clock, confirmedBody, confirmedToken, confirmedDeadline, confirmedAnchor,
          committedRound, fenceLost, resolveOutcome,
          round, requestAttempt, requestBody, requestExpectedToken, requestDeadline,
          attemptStartedAt, cadenceAnchor, firstSendAt, retryCount, staleOutstanding,
          adoptedAttempt, adoptedKind, terminalSeen, cancelled, successObservedAt,
          nextBeatScheduled, nextBeat, reclaimSeen, CallVars, writesOpen, history>>

(* The history a commit of the current renewal at `clock` leaves; `newDeadline` is what it sets. *)
CommitHistory(newDeadline) ==
    [history EXCEPT
        !.slowSuccess = @ \/ (clock >= cadenceAnchor + CadencePeriod),
        !.staleSuccessSeen =
            @ \/ (round = 1 /\ firstSendAt[1] + LeaseDuration < clock),
        !.authorityRestoredReached =
            @ \/ (round = 2 /\ history.staleSuccessSeen /\ ~fenceLost /\ clock < newDeadline),
        !.refusedThenCommitReached = @ \/ (engineCalls > 0),
        !.refreshThenCommitReached = @ \/ (refreshedInCall /\ firstSendAt[round] < clock)]

CommittedDeadline ==
    IF Defect("refresh_deadline_from_response")
    THEN clock + LeaseDuration
    ELSE attemptStartedAt + LeaseDuration

AcknowledgeRenewal ==
    /\ LogicalCreated
    /\ ~terminalSeen
    /\ localState = "Landed"
    /\ ExactRequestDurable
    /\ ( ~Defect("refresh_deadline_from_response")
         \/ clock > attemptStartedAt )
    /\ localState' = "Committed"
    /\ confirmedBody' = durable.body
    /\ confirmedToken' = durable.token
    /\ confirmedDeadline' = CommittedDeadline
    /\ confirmedAnchor' = attemptStartedAt
    /\ committedRound' = round
    /\ adoptedAttempt' = durable.attempt
    /\ adoptedKind' = "Renewal"
    /\ successObservedAt' = clock
    /\ history' =
        [CommitHistory(CommittedDeadline) EXCEPT
            !.deadlineFromResponse = @ \/ Defect("refresh_deadline_from_response"),
            !.badAcknowledgement = @ \/ ~ExactRequestDurable,
            !.directRetryReached =
                @ \/ (history.sawNonLandedTransient /\ history.sawRetrySent)]
    /\ UNCHANGED
        <<clock, durable, gcFenced, tokenStableUntil, fenceLost, resolveOutcome,
          round, requestAttempt, requestBody, requestExpectedToken, requestDeadline,
          attemptStartedAt, cadenceAnchor, firstSendAt,
          retryCount, outstanding, staleOutstanding, landings,
          terminalSeen, cancelled, nextBeatScheduled, nextBeat, reclaimSeen, CallVars, writesOpen>>

(* The read that settles an ambiguous attempt. A failed read settles nothing, so the engine
   reissues, as `preconditionStillSatisfiable` does for `NotObserved`. *)
ExactResolve ==
    /\ LogicalCreated
    /\ ~terminalSeen
    /\ localState = "ResponseLost"
    /\ \/ IF ExactRequestDurable
          THEN
              /\ localState' = "Resolved"
              /\ resolveOutcome' = "Exact"
          ELSE IF PredecessorStillCurrent
          THEN
              /\ localState' = "Resolved"
              /\ resolveOutcome' = "Predecessor"
          ELSE IF Defect("ignore_attempt_id") /\ TwinMatchesIgnoringId
          THEN
              /\ localState' = "Resolved"
              /\ resolveOutcome' = "Exact"
          ELSE
              /\ localState' = "Conflict"
              /\ resolveOutcome' = "Conflict"
       \/ /\ localState' = "Resolved"
          /\ resolveOutcome' = "NotObserved"
    /\ UNCHANGED
        <<clock, durable, gcFenced, tokenStableUntil,
          confirmedBody, confirmedToken, confirmedDeadline, confirmedAnchor, committedRound,
          fenceLost, round, requestAttempt, requestBody, requestExpectedToken, requestDeadline,
          attemptStartedAt, cadenceAnchor, firstSendAt,
          retryCount, outstanding, staleOutstanding, landings,
          adoptedAttempt, adoptedKind, terminalSeen, cancelled, successObservedAt,
          nextBeatScheduled, nextBeat, reclaimSeen, CallVars, writesOpen, history>>

AcceptResolved ==
    /\ LogicalCreated
    /\ ~terminalSeen
    /\ localState = "Resolved"
    /\ resolveOutcome = "Exact"
    /\ (ExactRequestDurable
        \/ (Defect("ignore_attempt_id") /\ TwinMatchesIgnoringId))
    /\ localState' = "Committed"
    /\ confirmedBody' = durable.body
    /\ confirmedToken' = durable.token
    /\ confirmedDeadline' = attemptStartedAt + LeaseDuration
    /\ confirmedAnchor' = attemptStartedAt
    /\ committedRound' = round
    /\ adoptedAttempt' = durable.attempt
    /\ adoptedKind' = durable.kind
    /\ successObservedAt' = clock
    /\ history' =
        [CommitHistory(attemptStartedAt + LeaseDuration) EXCEPT
            !.readAdoptionReached =
                @ \/ (history.sawLandedResponseLoss /\ ExactRequestDurable)]
    /\ UNCHANGED
        <<clock, durable, gcFenced, tokenStableUntil, fenceLost, resolveOutcome,
          round, requestAttempt, requestBody, requestExpectedToken, requestDeadline,
          attemptStartedAt, cadenceAnchor, firstSendAt,
          retryCount, outstanding, staleOutstanding, landings,
          terminalSeen, cancelled, nextBeatScheduled, nextBeat, reclaimSeen, CallVars, writesOpen>>

EnterRetryWait ==
    /\ LogicalCreated
    /\ ~terminalSeen
    /\ ~cancelled
    /\ localState = "Resolved"
    /\ resolveOutcome \in {"Predecessor", "NotObserved"}
    /\ localState' = "RetryWait"
    /\ UNCHANGED
        <<clock, durable, gcFenced, tokenStableUntil,
          confirmedBody, confirmedToken, confirmedDeadline, confirmedAnchor, committedRound,
          fenceLost, resolveOutcome,
          round, requestAttempt, requestBody, requestExpectedToken, requestDeadline,
          attemptStartedAt, cadenceAnchor, firstSendAt,
          retryCount, outstanding, staleOutstanding, landings,
          adoptedAttempt, adoptedKind, terminalSeen, cancelled, successObservedAt,
          nextBeatScheduled, nextBeat, reclaimSeen, CallVars, writesOpen, history>>

(* `MaxRetries` bounds the state space only: running out of it is not a transition. *)
RetrySend ==
    /\ LogicalCreated
    /\ ~terminalSeen
    /\ ~cancelled
    /\ localState = "RetryWait"
    /\ retryCount < MaxRetries
    /\ outstanding < MaxOutstanding
    /\ localState' = "Sent"
    /\ resolveOutcome' = "None"
    /\ retryCount' = retryCount + 1
    /\ outstanding' = outstanding + 1
    /\ requestBody' =
        IF Defect("retry_with_new_body") THEN RetryBody ELSE requestBody
    /\ attemptStartedAt' =
        IF Defect("anchor_deadline_at_retry") THEN clock ELSE attemptStartedAt
    /\ history' =
        [history EXCEPT
            !.tupleChanged = @ \/ Defect("retry_with_new_body"),
            !.sawRetrySent = TRUE,
            !.retryPastDeadlineReached = @ \/ (clock >= confirmedDeadline),
            !.putAfterCleanConflict = @ \/ preconditionRefused]
    /\ UNCHANGED
        <<clock, durable, gcFenced, tokenStableUntil,
          confirmedBody, confirmedToken, confirmedDeadline, confirmedAnchor, committedRound,
          fenceLost, round, requestAttempt, requestExpectedToken, requestDeadline,
          cadenceAnchor, firstSendAt, staleOutstanding, landings,
          adoptedAttempt, adoptedKind, terminalSeen, cancelled, successObservedAt,
          nextBeatScheduled, nextBeat, reclaimSeen, CallVars, writesOpen>>

(* A stop or a remount request after the renewal sent something. *)
Cancel ==
    /\ LogicalCreated
    /\ ~terminalSeen
    /\ localState /= "Committed"
    /\ ~cancelled
    /\ cancelled' = TRUE
    /\ UNCHANGED
        <<clock, durable, gcFenced, tokenStableUntil,
          confirmedBody, confirmedToken, confirmedDeadline, confirmedAnchor, committedRound,
          fenceLost, localState, resolveOutcome,
          round, requestAttempt, requestBody, requestExpectedToken, requestDeadline,
          attemptStartedAt, cadenceAnchor, firstSendAt,
          retryCount, outstanding, staleOutstanding, landings,
          adoptedAttempt, adoptedKind, terminalSeen, successObservedAt,
          nextBeatScheduled, nextBeat, reclaimSeen, CallVars, writesOpen, history>>

(* Only a cancel or an observed occupant ends the renewal; `Refused` and `NotObserved` do not. *)
LocalTerminalize ==
    /\ LogicalCreated
    /\ ~terminalSeen
    /\ localState /= "Committed"
    /\ (cancelled \/ localState = "Conflict")
    /\ terminalSeen' = TRUE
    /\ fenceLost' = TRUE
    /\ writesOpen' = FALSE
    /\ localState' = "Unresolved"
    /\ resolveOutcome' = "Conflict"
    /\ outstanding' =
        IF Defect("drop_pending_on_terminal") /\ outstanding > 0
        THEN 0
        ELSE outstanding
    /\ history' =
        [history EXCEPT
            !.terminalDroppedPending =
                @ \/ (Defect("drop_pending_on_terminal") /\ outstanding > 0)]
    /\ UNCHANGED
        <<clock, durable, gcFenced, tokenStableUntil,
          confirmedBody, confirmedToken, confirmedDeadline, confirmedAnchor, committedRound,
          round, requestAttempt, requestBody, requestExpectedToken, requestDeadline,
          attemptStartedAt, cadenceAnchor, firstSendAt,
          retryCount, staleOutstanding, landings,
          adoptedAttempt, adoptedKind, cancelled, successObservedAt,
          nextBeatScheduled, nextBeat, reclaimSeen, CallVars>>

(* The explicit arm step: a commit opens writes only while its own deadline is still ahead. *)
OpenWrites ==
    /\ ~writesOpen
    /\ ~terminalSeen
    /\ ~fenceLost
    /\ localState = "Committed"
    /\ (clock < confirmedDeadline \/ Defect("open_writes_on_late_success"))
    /\ writesOpen' = TRUE
    /\ history' = [history EXCEPT !.writesReopenedReached = TRUE]
    /\ UNCHANGED
        <<clock, durable, gcFenced, tokenStableUntil,
          confirmedBody, confirmedToken, confirmedDeadline, confirmedAnchor, committedRound,
          fenceLost, localState, resolveOutcome,
          round, requestAttempt, requestBody, requestExpectedToken, requestDeadline,
          attemptStartedAt, cadenceAnchor, firstSendAt,
          retryCount, outstanding, staleOutstanding, landings,
          adoptedAttempt, adoptedKind, terminalSeen, cancelled, successObservedAt,
          nextBeatScheduled, nextBeat, reclaimSeen, CallVars>>

SamePairTwin ==
    /\ LogicalCreated
    /\ durable.token = requestExpectedToken
    /\ durable.token /= TwinToken
    /\ ~gcFenced
    /\ durable' = TwinDurable
    /\ tokenStableUntil' = clock + LeaseDuration
    /\ UNCHANGED
        <<clock, gcFenced,
          confirmedBody, confirmedToken, confirmedDeadline, confirmedAnchor, committedRound,
          fenceLost, localState, resolveOutcome,
          round, requestAttempt, requestBody, requestExpectedToken, requestDeadline,
          attemptStartedAt, cadenceAnchor, firstSendAt,
          retryCount, outstanding, staleOutstanding, landings,
          adoptedAttempt, adoptedKind, terminalSeen, cancelled, successObservedAt,
          nextBeatScheduled, nextBeat, reclaimSeen, CallVars, writesOpen, history>>

GCFence ==
    /\ durable.kind /= "Absent"
    /\ ~gcFenced
    /\ gcFenced' = TRUE
    /\ tokenStableUntil' = clock + LeaseDuration
    /\ history' =
        [history EXCEPT !.entitledTakeover = @ \/ ObserverWaitedOut]
    /\ UNCHANGED
        <<clock, durable,
          confirmedBody, confirmedToken, confirmedDeadline, confirmedAnchor, committedRound,
          fenceLost, localState, resolveOutcome,
          round, requestAttempt, requestBody, requestExpectedToken, requestDeadline,
          attemptStartedAt, cadenceAnchor, firstSendAt,
          retryCount, outstanding, staleOutstanding, landings,
          adoptedAttempt, adoptedKind, terminalSeen, cancelled, successObservedAt,
          nextBeatScheduled, nextBeat, reclaimSeen, CallVars, writesOpen>>

(* Same process, new epoch: entitled by waiting, or by the old runtime being locally terminal. *)
SuccessorClaim ==
    /\ LogicalCreated
    /\ durable.kind /= "Successor"
    /\ durable' = SuccessorDurable
    /\ gcFenced' = FALSE
    /\ tokenStableUntil' = clock + LeaseDuration
    /\ history' =
        [history EXCEPT !.entitledTakeover = @ \/ ObserverWaitedOut \/ terminalSeen]
    /\ UNCHANGED
        <<clock,
          confirmedBody, confirmedToken, confirmedDeadline, confirmedAnchor, committedRound,
          fenceLost, localState, resolveOutcome,
          round, requestAttempt, requestBody, requestExpectedToken, requestDeadline,
          attemptStartedAt, cadenceAnchor, firstSendAt,
          retryCount, outstanding, staleOutstanding, landings,
          adoptedAttempt, adoptedKind, terminalSeen, cancelled, successObservedAt,
          nextBeatScheduled, nextBeat, reclaimSeen, CallVars, writesOpen>>

ForeignHolderWrite ==
    /\ LogicalCreated
    /\ durable.kind /= "Foreign"
    /\ durable' = ForeignDurable
    /\ gcFenced' = FALSE
    /\ tokenStableUntil' = clock + LeaseDuration
    /\ history' =
        [history EXCEPT !.entitledTakeover = @ \/ ObserverWaitedOut]
    /\ UNCHANGED
        <<clock,
          confirmedBody, confirmedToken, confirmedDeadline, confirmedAnchor, committedRound,
          fenceLost, localState, resolveOutcome,
          round, requestAttempt, requestBody, requestExpectedToken, requestDeadline,
          attemptStartedAt, cadenceAnchor, firstSendAt,
          retryCount, outstanding, staleOutstanding, landings,
          adoptedAttempt, adoptedKind, terminalSeen, cancelled, successObservedAt,
          nextBeatScheduled, nextBeat, reclaimSeen, CallVars, writesOpen>>

Reclaim ==
    /\ gcFenced
    /\ durable.kind /= "Absent"
    /\ durable' = EmptyDurable
    /\ gcFenced' = FALSE
    /\ tokenStableUntil' = clock + LeaseDuration
    /\ reclaimSeen' = TRUE
    /\ history' =
        [history EXCEPT
            !.lateBeforeReclaimReached =
                @ \/ (history.lateOldLanded /\ terminalSeen /\ ~LocalAuthority)]
    /\ UNCHANGED
        <<clock,
          confirmedBody, confirmedToken, confirmedDeadline, confirmedAnchor, committedRound,
          fenceLost, localState, resolveOutcome,
          round, requestAttempt, requestBody, requestExpectedToken, requestDeadline,
          attemptStartedAt, cadenceAnchor, firstSendAt,
          retryCount, outstanding, staleOutstanding, landings,
          adoptedAttempt, adoptedKind, terminalSeen, cancelled, successObservedAt,
          nextBeatScheduled, nextBeat, CallVars, writesOpen>>

LateDeliveryLands ==
    /\ terminalSeen
    /\ localState = "Unresolved"
    /\ outstanding > 0
    /\ PredecessorStillCurrent
    /\ landings[round] < MaxOutstanding
    /\ durable' = RequestDurable
    /\ gcFenced' = FALSE
    /\ tokenStableUntil' = clock + LeaseDuration
    /\ outstanding' = outstanding - 1
    /\ landings' = [landings EXCEPT ![round] = @ + 1]
    /\ fenceLost' = ~Defect("late_rearm")
    /\ history' =
        [history EXCEPT
            !.rearmedAfterTerminal = @ \/ Defect("late_rearm"),
            !.lateOverwroteSuccessor = @ \/ (durable.kind = "Successor"),
            !.lateOldLanded = TRUE]
    /\ UNCHANGED
        <<clock,
          confirmedBody, confirmedToken, confirmedDeadline, confirmedAnchor, committedRound,
          localState, resolveOutcome,
          round, requestAttempt, requestBody, requestExpectedToken, requestDeadline,
          attemptStartedAt, cadenceAnchor, firstSendAt, retryCount, staleOutstanding,
          adoptedAttempt, adoptedKind, terminalSeen, cancelled, successObservedAt,
          nextBeatScheduled, nextBeat, reclaimSeen, CallVars, writesOpen>>

LateDeliveryRefused ==
    /\ terminalSeen
    /\ localState = "Unresolved"
    /\ outstanding > 0
    /\ ~PredecessorStillCurrent
    /\ outstanding' = outstanding - 1
    /\ history' =
        [history EXCEPT
            !.oldCopyRefused = @ \/ (durable.kind = "Successor"),
            !.lateAfterSuccessorReached =
                @ \/ (durable.kind = "Successor" /\ ~gcFenced)]
    /\ UNCHANGED
        <<clock, durable, gcFenced, tokenStableUntil,
          confirmedBody, confirmedToken, confirmedDeadline, confirmedAnchor, committedRound,
          fenceLost, localState, resolveOutcome,
          round, requestAttempt, requestBody, requestExpectedToken, requestDeadline,
          attemptStartedAt, cadenceAnchor, firstSendAt,
          retryCount, staleOutstanding, landings,
          adoptedAttempt, adoptedKind, terminalSeen, cancelled, successObservedAt,
          nextBeatScheduled, nextBeat, reclaimSeen, CallVars, writesOpen>>

AcceptAfterTerminal ==
    /\ Defect("accept_after_terminal")
    /\ terminalSeen
    /\ localState = "Unresolved"
    /\ ExactRequestDurable
    /\ localState' = "Committed"
    /\ fenceLost' = FALSE
    /\ confirmedBody' = durable.body
    /\ confirmedToken' = durable.token
    /\ confirmedDeadline' = attemptStartedAt + LeaseDuration
    /\ confirmedAnchor' = attemptStartedAt
    /\ committedRound' = round
    /\ adoptedAttempt' = durable.attempt
    /\ adoptedKind' = "Renewal"
    /\ successObservedAt' = clock
    /\ history' =
        [history EXCEPT
            !.rearmedAfterTerminal = TRUE]
    /\ UNCHANGED
        <<clock, durable, gcFenced, tokenStableUntil, resolveOutcome,
          round, requestAttempt, requestBody, requestExpectedToken, requestDeadline,
          attemptStartedAt, cadenceAnchor, firstSendAt,
          retryCount, outstanding, staleOutstanding, landings,
          terminalSeen, cancelled, nextBeatScheduled, nextBeat, reclaimSeen, CallVars, writesOpen>>

AcceptSuccessor ==
    /\ Defect("accept_successor")
    /\ ~terminalSeen
    /\ localState = "Conflict"
    /\ durable.kind = "Successor"
    /\ ~gcFenced
    /\ localState' = "Committed"
    /\ confirmedBody' = durable.body
    /\ confirmedToken' = durable.token
    /\ confirmedDeadline' = durable.deadline
    /\ confirmedAnchor' = attemptStartedAt
    /\ committedRound' = round
    /\ adoptedAttempt' = durable.attempt
    /\ adoptedKind' = "Successor"
    /\ successObservedAt' = clock
    /\ history' =
        [history EXCEPT
            !.badAcknowledgement = TRUE]
    /\ UNCHANGED
        <<clock, durable, gcFenced, tokenStableUntil, fenceLost, resolveOutcome,
          round, requestAttempt, requestBody, requestExpectedToken, requestDeadline,
          attemptStartedAt, cadenceAnchor, firstSendAt,
          retryCount, outstanding, staleOutstanding, landings,
          terminalSeen, cancelled, nextBeatScheduled, nextBeat, reclaimSeen, CallVars, writesOpen>>

ScheduleNextBeat ==
    /\ localState = "Committed"
    /\ ~nextBeatScheduled
    /\ nextBeatScheduled' = TRUE
    /\ nextBeat' =
        IF Defect("response_relative_cadence")
        THEN successObservedAt + CadencePeriod
        ELSE cadenceAnchor + CadencePeriod
    /\ history' =
        [history EXCEPT
            !.catchupReached =
                @ \/ (history.slowSuccess
                      /\ cadenceAnchor + CadencePeriod <= clock)]
    /\ UNCHANGED
        <<clock, durable, gcFenced, tokenStableUntil,
          confirmedBody, confirmedToken, confirmedDeadline, confirmedAnchor, committedRound,
          fenceLost, localState, resolveOutcome,
          round, requestAttempt, requestBody, requestExpectedToken, requestDeadline,
          attemptStartedAt, cadenceAnchor, firstSendAt,
          retryCount, outstanding, staleOutstanding, landings,
          adoptedAttempt, adoptedKind, terminalSeen, cancelled, successObservedAt,
          reclaimSeen, CallVars, writesOpen>>

Next ==
    \/ AdvanceTime
    \/ SendRenewal
    \/ ResponseLoss
    \/ LandCurrent
    \/ LandGhost
    \/ RefuseCurrent
    \/ RefuseGhost
    \/ DenyCurrent
    \/ CredentialRefresh
    \/ NewEngineCall
    \/ ResolvingRead
    \/ RepeatPutAfterNotObserved
    \/ StaleCopyDelivery
    \/ DoubleConditionalLanding
    \/ AcknowledgeRenewal
    \/ ExactResolve
    \/ AcceptResolved
    \/ EnterRetryWait
    \/ RetrySend
    \/ Cancel
    \/ LocalTerminalize
    \/ OpenWrites
    \/ SamePairTwin
    \/ GCFence
    \/ SuccessorClaim
    \/ ForeignHolderWrite
    \/ Reclaim
    \/ LateDeliveryLands
    \/ LateDeliveryRefused
    \/ AcceptAfterTerminal
    \/ AcceptSuccessor
    \/ ScheduleNextBeat

Spec == Init /\ [][Next]_vars

(* Only the exact symbolic `write_attempt_id` of the committed renewal may be adopted. *)
ExactAttemptOnly ==
    adoptedAttempt = NoAttempt \/ adoptedAttempt = AttemptOf(committedRound)

(* Twins, GC-fenced bodies, successor epochs, and foreign holders remain fail closed. *)
ForeignOrSuccessorNeverAdopted ==
    adoptedKind \in {"None", "Renewal"}

(* A commit sets the deadline from the start it used, never from a response timestamp. *)
ConfirmedDeadlineNeverExtendedByResponse ==
    /\ ~history.deadlineFromResponse
    /\ (committedRound > 0 => confirmedDeadline = confirmedAnchor + LeaseDuration)

(* Once locally terminal, old-epoch authority remains fenced through all late outcomes. *)
TerminalNeverRearmsAuthority ==
    /\ ~history.rearmedAfterTerminal
    /\ (terminalSeen => ~LocalAuthority)

(* All physical copies of a renewal retain its body, attempt, and expected token. *)
OneLogicalBodyPerExpectedToken ==
    /\ ~history.tupleChanged
    /\ (LogicalCreated
        => /\ requestAttempt = AttemptOf(round)
           /\ requestBody = RenewalBody
           /\ requestExpectedToken =
                IF round = 1 THEN PredecessorToken ELSE RenewalTokenOf(1))

(* Direct acknowledgement is possible only after that exact body and token are durable. *)
AcknowledgedRenewalIsDurable ==
    ~history.badAcknowledgement

(* A post-terminal old conditional copy cannot replace an installed successor. *)
LateDeliveryCannotOverwriteSuccessor ==
    ~history.lateOverwroteSuccessor

(* This is a transition-history guard, not a claim that all unresolved results have pending I/O. *)
PendingSurvivesLocalTerminal ==
    ~history.terminalDroppedPending

(* Conditional atomicity lets at most one physical copy consume each renewal's expected token. *)
OneIncarnationPerPredecessor ==
    \A r \in Rounds : landings[r] <= 1

(* The next beat derives from the renewal's first send, not result delivery time. *)
CadenceAnchoredAtAttemptStart ==
    nextBeatScheduled
        => /\ nextBeat = cadenceAnchor + CadencePeriod
           /\ cadenceAnchor = firstSendAt[round]

(* The first conjunct holds by the definition of `LocalAuthority`; the second compares the
   deadline with `firstSendAt`, which only `SendRenewal` writes. *)
AuthorityEndsAtConfirmedDeadline ==
    /\ (LocalAuthority => clock < confirmedDeadline)
    /\ confirmedDeadline =
        IF committedRound = 0
        THEN InitialConfirmedDeadline
        ELSE firstSendAt[committedRound] + LeaseDuration

(* No authority once an observer that waited, or the successor after a local terminal, took over. *)
NoAuthorityAfterEntitledTakeover ==
    history.entitledTakeover => ~LocalAuthority

(* A clean `412` never meets our own landed body: no earlier attempt of the renewal can have landed
   once the engine reported the call free of ambiguity. *)
OwnLandedBodyNeverLost ==
    ~history.ownLandingLost

(* After a clean `412` the renewal sends no further `PUT` on the expected token it refused. *)
NotObservedRepeatsOnlyTheRead ==
    ~history.putAfterCleanConflict

(* A failed resolving read concludes nothing: not an absence, not an occupant. *)
NotObservedNeverConcludes ==
    ~history.concludedUnobserved

(* Open writes imply the confirmed deadline is ahead and the fence is intact. *)
LateSuccessNeverOpensWrites ==
    writesOpen => (~fenceLost /\ clock < confirmedDeadline)

(* Negated reachability checks: TLC must violate each exact witness name. *)
WitnessDirectRetry == ~history.directRetryReached
WitnessReadAdoption == ~history.readAdoptionReached
WitnessLateBeforeReclaim == ~history.lateBeforeReclaimReached
WitnessLateAfterSuccessor == ~history.lateAfterSuccessorReached
WitnessImmediateCatchup == ~history.catchupReached
WitnessRetryPastDeadline == ~history.retryPastDeadlineReached
WitnessAuthorityRestored == ~history.authorityRestoredReached
WitnessRefusedThenCommit == ~history.refusedThenCommitReached
WitnessNotObservedThenObserved == ~history.notObservedThenObservedReached
WitnessRefreshThenCommit == ~history.refreshThenCommitReached
WitnessWritesReopened == ~history.writesReopenedReached

================================================================================
