---- MODULE MC_BaseWitness ----
\* The Base actions with the transaction count the scenario matrix asks for (TID_MAX = 3) and enough log entries
\* for three commits. It is a witness-only scenario, never run green: it exists because three Base witnesses
\* cannot reach their target at Base's reduced TID_MAX = 2. The part universe starts empty, so the part a second
\* transaction sees costs one transaction to create and commit, which leaves one transaction where those three
\* defects need two. See WITNESSES.md.
EXTENDS MC_Base
====
