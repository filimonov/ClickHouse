---- MODULE MC_BaseSmall ----
EXTENDS MergeTreeTransactions
CoversDef == [p \in Parts |-> {}]
SymSessions == Permutations(Sessions)
====
