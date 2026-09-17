---- MODULE MC_Base ----
EXTENDS MergeTreeTransactions
CoversDef == [p \in Parts |-> {}]
SymSessions == Permutations(Sessions)
====
