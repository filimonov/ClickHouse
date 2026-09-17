---- MODULE MC_Schema ----
EXTENDS MergeTreeTransactions
CoversDef == [p \in Parts |-> {}]
NoNext == FALSE
SchemaSpec == Init /\ [][NoNext]_vars
====
