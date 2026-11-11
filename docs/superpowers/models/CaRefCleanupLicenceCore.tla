-------------------- MODULE CaRefCleanupLicenceCore --------------------
(* Per-life licence of covered ref-object cleanup (`Gc::cleanupRefObjects`) -- spec
   2026-11-11-cas-ref-cleanup-per-life-licence-design.md, section "TLA+ impact".

   One logical namespace name, up to `MaxInc` lives of it, one ref stream per life. A GC leader folds,
   plans the covered objects of the life its catalog cut names, and deletes them one key per chunk.
   Before each chunk it re-reads the catalog and `gc/state` (`Check`); the delete is a later step
   (`Delete`), so every catalog and lease change can land in between.

   The licence terms are constants so that each one can be dropped:

     row    the namespace's row equals the fold-time row by value;
     life   the catalog still names the fold-time incarnation as `Live` or `Removing`;
     lease  `gc/state` still carries the lease the fold adopted;
     whole  the whole catalog object is unchanged since the fold (the term the spec removes).

   What the model shows:

   1. `NamedLifeIntact` holds with the specified licence (`_safe`) and also with no licence term at
      all (`_datafloor`). The data invariant rests on three facts outside the licence: the plan bounds,
      the monotone `_ckpt` and fold cursor of one life, and keys built from the fold-time incarnation
      of a life whose incarnation is never reused.
   2. Each of the last two facts is load-bearing: `_sab_rederive` and `_sab_reuseincarnation` break
      `NamedLifeIntact` with every licence term in place, the removed `whole` term included
      (`_old_reuseincarnation`).
   3. The `lease` and `row` terms bound stale work: a deposed leader, or a leader whose namespace row
      changed, deletes at most the one chunk it had already licensed.
   4. The `whole` term stops cleanup after any unrelated catalog write (`_old_churnblocked`); without
      it cleanup proceeds (`_witness_churnprogress`).

   Not modelled: the `retained_log_proof` exception of the plan (it only shrinks the plan), the per-round
   work budget, more than one namespace (refusal scope is a liveness matter), and an ambiguous catalog
   (two rows naming one incarnation). *)
EXTENDS Integers

CONSTANTS
    MaxInc,                     \* lives of the one logical name
    MaxId,                      \* ref ids of one life; ids restart at 1 in every life
    DropRowTerm,
    DropLifeTerm,
    DropLeaseTerm,
    WholeCatalogTerm,           \* TRUE = the licence also requires an unchanged catalog object
    SabotageRederive,           \* `Delete` resolves the name again instead of using the fold-time life
    SabotageReuseIncarnation    \* rebirth reuses the erased incarnation

ASSUME MaxInc \in Nat /\ MaxInc >= 2
ASSUME MaxId \in Nat /\ MaxId >= 2

Incs == 1..MaxInc
Ids == 1..MaxId
Absent == [state |-> "absent", inc |-> 0]

VARIABLES
    row,            \* the name's catalog row: [state, inc]
    catalogMoved,   \* the catalog object was written since the last fold (its etag differs)
    nextInc,        \* fresh-incarnation allocator
    erasedInc,      \* incarnation of the last erased row; read only by `SabotageReuseIncarnation`
    head,           \* [Incs -> 0..MaxId] last published log id
    ckpt,           \* [Incs -> 0..MaxId] snapshot id `_ckpt` names; 0 = none
    cursor,         \* [Incs -> 0..MaxId] durable fold cursor (`last_folded_ref_id`)
    logs,           \* [Incs -> SUBSET Ids] `_log` objects present
    snaps,          \* [Incs -> SUBSET Ids] `_snap` objects present
    leaseMoved,     \* `gc/state` no longer carries the lease the cleaner's fold adopted
    phase,          \* cleaner: "idle" | "planned" | "licensed"
    obs,            \* cleaner: the row its fold's catalog cut held
    planLogs,       \* cleaner: log ids still to delete
    planSnaps,      \* cleaner: snapshot ids still to delete
    key,            \* cleaner: the licensed chunk, [kind, id]
    staleLeaseDeletes,  \* ghost: this pass's deletes executed after the lease moved, capped at 2
    staleRowDeletes,    \* ghost: this pass's deletes executed after the row changed, capped at 2
    licensedUnderMove   \* ghost: a chunk was licensed after a catalog write

catalogVars == << row, catalogMoved, nextInc, erasedInc >>
streamVars == << head, ckpt, cursor, logs, snaps >>
cleanerVars == << phase, obs, planLogs, planSnaps, key >>
ghostVars == << staleLeaseDeletes, staleRowDeletes, licensedUnderMove >>
vars == << catalogVars, streamVars, leaseMoved, cleanerVars, ghostVars >>

Named == row.state \in {"live", "removing"}
Min(S) == CHOOSE x \in S : \A y \in S : x <= y
Bump(n) == IF n < 2 THEN n + 1 ELSE 2
NoKey == [kind |-> "none", id |-> 0]

Init ==
    /\ row = [state |-> "live", inc |-> 1]
    /\ catalogMoved = FALSE
    /\ nextInc = 2
    /\ erasedInc = 0
    /\ head = [i \in Incs |-> 0]
    /\ ckpt = [i \in Incs |-> 0]
    /\ cursor = [i \in Incs |-> 0]
    /\ logs = [i \in Incs |-> {}]
    /\ snaps = [i \in Incs |-> {}]
    /\ leaseMoved = FALSE
    /\ phase = "idle"
    /\ obs = Absent
    /\ planLogs = {}
    /\ planSnaps = {}
    /\ key = NoKey
    /\ staleLeaseDeletes = 0
    /\ staleRowDeletes = 0
    /\ licensedUnderMove = FALSE

(* ---- the writer of the named life ---- *)

Append ==
    /\ row.state = "live"
    /\ head[row.inc] < MaxId
    /\ head' = [head EXCEPT ![row.inc] = @ + 1]
    /\ logs' = [logs EXCEPT ![row.inc] = @ \cup {head[row.inc] + 1}]
    /\ UNCHANGED << catalogVars, ckpt, cursor, snaps, leaseMoved, cleanerVars, ghostVars >>

(* A snapshot lands before the `_ckpt` CAS that names it. *)
PublishSnap ==
    /\ row.state = "live"
    /\ \E s \in (ckpt[row.inc] + 1)..head[row.inc] :
        /\ s \notin snaps[row.inc]
        /\ snaps' = [snaps EXCEPT ![row.inc] = @ \cup {s}]
    /\ UNCHANGED << catalogVars, head, ckpt, cursor, logs, leaseMoved, cleanerVars, ghostVars >>

(* `_ckpt` moves forward only, and only onto a complete triple: `_snap` and `_log` of the same id. *)
CkptCas ==
    /\ row.state = "live"
    /\ \E s \in snaps[row.inc] \cap logs[row.inc] :
        /\ s > ckpt[row.inc]
        /\ ckpt' = [ckpt EXCEPT ![row.inc] = s]
    /\ UNCHANGED << catalogVars, head, cursor, logs, snaps, leaseMoved, cleanerVars, ghostVars >>

(* ---- catalog writers ---- *)

Drop ==
    /\ row.state = "live"
    /\ row' = [row EXCEPT !.state = "removing"]
    /\ catalogMoved' = TRUE
    /\ UNCHANGED << nextInc, erasedInc, streamVars, leaseMoved, cleanerVars, ghostVars >>

Erase ==
    /\ row.state = "removing"
    /\ row' = Absent
    /\ erasedInc' = row.inc
    /\ catalogMoved' = TRUE
    /\ UNCHANGED << nextInc, streamVars, leaseMoved, cleanerVars, ghostVars >>

(* Rebirth of the name. A reused incarnation starts its stream over under the same key prefix. *)
Recreate ==
    /\ row.state = "absent"
    /\ IF SabotageReuseIncarnation
       THEN /\ erasedInc # 0
            /\ row' = [state |-> "live", inc |-> erasedInc]
            /\ head' = [head EXCEPT ![erasedInc] = 0]
            /\ ckpt' = [ckpt EXCEPT ![erasedInc] = 0]
            /\ cursor' = [cursor EXCEPT ![erasedInc] = 0]
            /\ logs' = [logs EXCEPT ![erasedInc] = {}]
            /\ snaps' = [snaps EXCEPT ![erasedInc] = {}]
            /\ UNCHANGED nextInc
       ELSE /\ nextInc <= MaxInc
            /\ row' = [state |-> "live", inc |-> nextInc]
            /\ nextInc' = nextInc + 1
            /\ UNCHANGED streamVars
    /\ catalogMoved' = TRUE
    /\ UNCHANGED << erasedInc, leaseMoved, cleanerVars, ghostVars >>

(* A write to the catalog object that leaves this name's row alone: another namespace's change. *)
Churn ==
    /\ catalogMoved' = TRUE
    /\ UNCHANGED << row, nextInc, erasedInc, streamVars, leaseMoved, cleanerVars, ghostVars >>

(* The namespace janitor removes the stream of an incarnation the catalog does not name. Its own
   stale-page safety is `CaRefNsCleanupStaleLeaderCore`; here the check and the delete are one step. *)
Janitor ==
    /\ \E i \in Incs :
        /\ i # row.inc
        /\ logs[i] # {} \/ snaps[i] # {}
        /\ logs' = [logs EXCEPT ![i] = {}]
        /\ snaps' = [snaps EXCEPT ![i] = {}]
    /\ UNCHANGED << catalogVars, head, ckpt, cursor, leaseMoved, cleanerVars, ghostVars >>

(* ---- GC ---- *)

(* The cleaner takes the lease, cuts the catalog, advances the durable cursor of the named life and
   plans from the listing: logs at or below the cursor and below the checkpoint, snapshots below the
   checkpoint. *)
Fold ==
    /\ phase = "idle"
    /\ Named
    /\ \E c \in cursor[row.inc]..head[row.inc] :
        /\ cursor' = [cursor EXCEPT ![row.inc] = c]
        /\ planLogs' = {l \in logs[row.inc] : l <= c /\ l < ckpt[row.inc]}
        /\ planSnaps' = {s \in snaps[row.inc] : s < ckpt[row.inc]}
    /\ obs' = row
    /\ phase' = "planned"
    /\ key' = NoKey
    /\ leaseMoved' = FALSE
    /\ catalogMoved' = FALSE
    /\ staleLeaseDeletes' = 0
    /\ staleRowDeletes' = 0
    /\ UNCHANGED << row, nextInc, erasedInc, head, ckpt, logs, snaps, licensedUnderMove >>

Licence ==
    /\ DropRowTerm \/ row = obs
    /\ DropLifeTerm \/ (Named /\ row.inc = obs.inc)
    /\ DropLeaseTerm \/ ~leaseMoved
    /\ ~WholeCatalogTerm \/ ~catalogMoved

NextKey ==
    IF planLogs # {} THEN [kind |-> "log", id |-> Min(planLogs)]
    ELSE [kind |-> "snap", id |-> Min(planSnaps)]

(* Revalidation before one chunk. A refusal ends the pass; an empty plan ends it too. *)
Check ==
    /\ phase = "planned"
    /\ IF (planLogs # {} \/ planSnaps # {}) /\ Licence
       THEN /\ phase' = "licensed"
            /\ key' = NextKey
            /\ licensedUnderMove' = (licensedUnderMove \/ catalogMoved)
            /\ UNCHANGED << planLogs, planSnaps >>
       ELSE /\ phase' = "idle"
            /\ key' = NoKey
            /\ planLogs' = {}
            /\ planSnaps' = {}
            /\ UNCHANGED licensedUnderMove
    /\ UNCHANGED << catalogVars, streamVars, leaseMoved, obs, staleLeaseDeletes, staleRowDeletes >>

(* 0 = the name resolves to nothing; the sabotaged delete then removes nothing. *)
Target == IF SabotageRederive THEN (IF Named THEN row.inc ELSE 0) ELSE obs.inc

Delete ==
    /\ phase = "licensed"
    /\ IF Target = 0
       THEN UNCHANGED << logs, snaps >>
       ELSE IF key.kind = "log"
            THEN /\ logs' = [logs EXCEPT ![Target] = @ \ {key.id}]
                 /\ UNCHANGED snaps
            ELSE /\ snaps' = [snaps EXCEPT ![Target] = @ \ {key.id}]
                 /\ UNCHANGED logs
    /\ planLogs' = IF key.kind = "log" THEN planLogs \ {key.id} ELSE planLogs
    /\ planSnaps' = IF key.kind = "snap" THEN planSnaps \ {key.id} ELSE planSnaps
    /\ phase' = "planned"
    /\ key' = NoKey
    /\ staleLeaseDeletes' = IF leaseMoved THEN Bump(staleLeaseDeletes) ELSE staleLeaseDeletes
    /\ staleRowDeletes' = IF row # obs THEN Bump(staleRowDeletes) ELSE staleRowDeletes
    /\ UNCHANGED << catalogVars, head, ckpt, cursor, leaseMoved, obs, licensedUnderMove >>

(* Another GC node takes the lease. It folds and cleans from current durable state. *)
Steal ==
    /\ ~leaseMoved
    /\ leaseMoved' = TRUE
    /\ UNCHANGED << catalogVars, streamVars, cleanerVars, ghostVars >>

SuccessorFold ==
    /\ leaseMoved
    /\ Named
    /\ \E c \in (cursor[row.inc] + 1)..head[row.inc] :
        cursor' = [cursor EXCEPT ![row.inc] = c]
    /\ UNCHANGED << catalogVars, head, ckpt, logs, snaps, leaseMoved, cleanerVars, ghostVars >>

SuccessorDelete ==
    /\ leaseMoved
    /\ Named
    /\ \/ \E l \in logs[row.inc] :
            /\ l <= cursor[row.inc] /\ l < ckpt[row.inc]
            /\ logs' = [logs EXCEPT ![row.inc] = @ \ {l}]
            /\ UNCHANGED snaps
       \/ \E s \in snaps[row.inc] :
            /\ s < ckpt[row.inc]
            /\ snaps' = [snaps EXCEPT ![row.inc] = @ \ {s}]
            /\ UNCHANGED logs
    /\ UNCHANGED << catalogVars, head, ckpt, cursor, leaseMoved, cleanerVars, ghostVars >>

Next ==
    \/ Append \/ PublishSnap \/ CkptCas
    \/ Drop \/ Erase \/ Recreate \/ Churn \/ Janitor
    \/ Fold \/ Check \/ Delete
    \/ Steal \/ SuccessorFold \/ SuccessorDelete

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ row \in [state : {"absent", "live", "removing"}, inc : 0..MaxInc]
    /\ (row.state = "absent") <=> (row.inc = 0)
    /\ catalogMoved \in BOOLEAN
    /\ nextInc \in 1..(MaxInc + 1)
    /\ erasedInc \in 0..MaxInc
    /\ head \in [Incs -> 0..MaxId]
    /\ ckpt \in [Incs -> 0..MaxId]
    /\ cursor \in [Incs -> 0..MaxId]
    /\ logs \in [Incs -> SUBSET Ids]
    /\ snaps \in [Incs -> SUBSET Ids]
    /\ leaseMoved \in BOOLEAN
    /\ phase \in {"idle", "planned", "licensed"}
    /\ obs \in [state : {"absent", "live", "removing"}, inc : 0..MaxInc]
    /\ planLogs \subseteq Ids
    /\ planSnaps \subseteq Ids
    /\ key \in [kind : {"none", "log", "snap"}, id : 0..MaxId]
    /\ staleLeaseDeletes \in 0..2
    /\ staleRowDeletes \in 0..2
    /\ licensedUnderMove \in BOOLEAN

(* The life the catalog names keeps what recovery and the next fold read: the checkpoint snapshot,
   the log of the same id, every log above the checkpoint, and every log above the fold cursor. *)
NamedLifeIntact ==
    Named =>
        /\ \A l \in 1..head[row.inc] :
            (l >= ckpt[row.inc] \/ l > cursor[row.inc]) => l \in logs[row.inc]
        /\ ckpt[row.inc] # 0 => ckpt[row.inc] \in snaps[row.inc]

DeposedLeaderDeletesAtMostOneChunk == staleLeaseDeletes <= 1

ChangedRowDeletesAtMostOneChunk == staleRowDeletes <= 1

(* Holds only with `WholeCatalogTerm`: no chunk is licensed once the catalog object was written. *)
NoLicenceAfterCatalogWrite == ~licensedUnderMove

(* Negated witnesses: a violation is the reachability evidence. *)

(* A chunk is licensed after a catalog write that left the name's row unchanged. *)
WITNESS_LICENSED_AFTER_UNRELATED_WRITE == ~licensedUnderMove

(* A licensed chunk is still pending when the name is live again under another incarnation. *)
WITNESS_PENDING_CHUNK_ACROSS_REBIRTH ==
    ~(phase = "licensed" /\ row.state = "live" /\ row.inc # obs.inc /\ head[row.inc] > 0)

(* One pass holds a plan of two keys, so the "at most one chunk" bounds are not vacuous. *)
WITNESS_TWO_KEY_PLAN == ~(phase = "planned" /\ planLogs # {} /\ planSnaps # {})

=============================================================================
