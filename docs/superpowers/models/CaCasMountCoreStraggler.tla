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

VARIABLE stragglerLanded  \* TRUE once such an adopt write has landed

wvars == << vars, stragglerLanded >>

InitW == Init /\ stragglerLanded = FALSE

\* A successful `AdoptWrite` consumes `adoptObs[a]` and bumps the token; a claim does the same but
\* clears the latch, and `Die` consumes `adoptObs[a]` without a token bump.
LandedWhileLatched ==
    \E a \in Incs : /\ fenceLatched[a] /\ fenceLatched'[a]
                    /\ adoptObs[a] # None /\ adoptObs'[a] = None
                    /\ mtoken' = mtoken + 1

NextW == Next /\ stragglerLanded' = (stragglerLanded \/ LandedWhileLatched)

SpecW == InitW /\ [][NextW]_wvars

TypeOKW == stragglerLanded \in BOOLEAN

\* Witness, negated reachability: a straggler adopt write lands.
W_StragglerLanded == ~stragglerLanded
=============================================================================
