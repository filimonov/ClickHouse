---- MODULE Keeper ----
\* Part of the TLA+ model of MergeTree transactions; see
\* docs/superpowers/specs/2026-09-17-mergetree-transactions-tla-design.md
\* Baseline C++: upstream ClickHouse master 2c24b6b9291e
EXTENDS Types
\* zk = [log : csn -> tid over existing znodes, seq : last allocated csn, tail : tail_ptr znode, session]
VARIABLE zk

KeeperInit == zk = [log |-> (FirstCSN :> EmptyTID), seq |-> FirstCSN, tail |-> MaxReservedCSN, session |-> "Alive"]

KeeperTypeOK ==
  /\ zk.seq \in LogCSNs
  /\ DOMAIN zk.log \subseteq FirstCSN..zk.seq
  /\ \A c \in DOMAIN zk.log : zk.log[c] \in AllTids
  /\ zk.tail \in LogCSNs
  /\ zk.session \in {"Alive", "Expired"}

KeeperCanAppend == zk.seq < CSN_MAX
KeeperNextCsn == zk.seq + 1
\* the effect of a successful sequential create: the new znode carries tid
KeeperAppended(tid) == [zk EXCEPT !.seq = @ + 1, !.log = @ @@ ((zk.seq + 1) :> tid)]
KeeperHas(tid) == \E c \in DOMAIN zk.log : zk.log[c] = tid
KeeperCsnOf(tid) == IF KeeperHas(tid) THEN CHOOSE c \in DOMAIN zk.log : zk.log[c] = tid ELSE UnknownCSN
KeeperRemoved(c) == [zk EXCEPT !.log = [d \in DOMAIN zk.log \ {c} |-> zk.log[d]]]
KeeperWithTail(c) == [zk EXCEPT !.tail = c]
KeeperExpired == [zk EXCEPT !.session = "Expired"]
KeeperRenewed == [zk EXCEPT !.session = "Alive"]
KeeperLatest == Max(DOMAIN zk.log)

KeeperMonotone == \A c \in DOMAIN zk.log : c <= zk.seq
====
