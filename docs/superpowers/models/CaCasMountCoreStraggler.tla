------------------------ MODULE CaCasMountCoreStraggler ------------------------
(***************************************************************************)
(* History wrapper over `CaCasMountCore` for one witness: an adopt write   *)
(* of an incarnation lands while its fence is latched, i.e. after          *)
(* `AllocBegin` and before the claim that arms the new epoch.              *)
(*                                                                         *)
(* A state predicate cannot tell such an `AdoptWrite` from a claim or a    *)
(* renewal, so the wrapper compares the state before and after each step.  *)
(* Every step of `NextW` is a step of `Next`, and `Next` is unchanged, so  *)
(* the wrapped model reaches exactly the states of `CaCasMountCore`.       *)
(***************************************************************************)
EXTENDS CaCasMountCore

VARIABLES stragglerLanded,  \* TRUE once such an adopt write has landed
          reacqStraggler    \* TRUE once one landed while its incarnation held a reacquire certificate

wvars == << vars, stragglerLanded, reacqStraggler >>

InitW == Init /\ stragglerLanded = FALSE /\ reacqStraggler = FALSE

\* A successful `AdoptWrite` consumes `adoptObs[a]` and bumps the token; a claim does the same but
\* clears the latch, and `Die` consumes `adoptObs[a]` without a token bump.
LandedWhileLatched ==
    \E a \in Incs : /\ fenceLatched[a] /\ fenceLatched'[a]
                    /\ adoptObs[a] # None /\ adoptObs'[a] = None
                    /\ mtoken' = mtoken + 1

\* The same, restricted to an incarnation whose reacquire is running. With the immutable claim a claim
\* also consumes `adoptObs[a]` and bumps the token while the latch stays set, so a step that moves
\* `claimState[a]` is a claim, not an adopt write.
LandedInReacquire ==
    \E a \in Incs : /\ Certified(a) /\ fenceLatched[a] /\ fenceLatched'[a]
                    /\ adoptObs[a] # None /\ adoptObs'[a] = None
                    /\ claimState'[a] = claimState[a]
                    /\ mtoken' = mtoken + 1

NextW == /\ Next
         /\ stragglerLanded' = (stragglerLanded \/ LandedWhileLatched)
         /\ reacqStraggler' = (reacqStraggler \/ LandedInReacquire)

SpecW == InitW /\ [][NextW]_wvars

TypeOKW == stragglerLanded \in BOOLEAN /\ reacqStraggler \in BOOLEAN

\* No adopt write lands while a reacquire holds the fence latched: the renewal read cannot overlap it.
NoStragglerInReacquire == ~reacqStraggler

\* The view of `CaCasMountCore`, plus the one wrapper variable the checked invariant reads: `stragglerLanded`
\* is read by `W_StragglerLanded` only, which no cfg with this view lists.
ViewW == << ViewNoHistory, reacqStraggler >>

\* Witness, negated reachability: a straggler adopt write lands.
W_StragglerLanded == ~stragglerLanded
=============================================================================
