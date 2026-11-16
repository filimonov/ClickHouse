"""A retried UPDATE must not be sent twice when its first attempt created the mutation.

Live-soak finding (2026-11-16): a server stopped a few milliseconds after it received
`ALTER TABLE ... UPDATE v = v + 1 ...` answered 394 "Query ... is killed in pending state" AFTER it
wrote the mutation entry to Keeper (https://github.com/ClickHouse/ClickHouse/issues/131967). The
driver retried, a second entry was created, and the increment was applied twice.

`Driver.apply_barrier` now reads the newest mutation id from `system.mutations` before the first
attempt and, before every later attempt, does not resubmit if an UPDATE of the same bucket appeared
after it. Every such read follows `SYSTEM SYNC REPLICA ... PULL`, which loads the mutation entries."""

import time

from soak import run as soak_run
from soak.cluster import QueryError
from soak.ledger import Op, OpType


class FakeKeeper:
    """Mutation entries shared by both fake nodes: name -> bucket."""

    def __init__(self):
        self.entries = {"0000000007": 3}

    def add_update(self, bucket):
        name = f"{max(int(n) for n in self.entries) + 1:010d}"
        self.entries[name] = bucket
        return name


class FakeNode:
    def __init__(self, name, keeper, fail_first_command_after_landing=False, fail_before_landing=False):
        self.name = name
        self.keeper = keeper
        self.fail_after_landing = fail_first_command_after_landing
        self.fail_before_landing = fail_before_landing
        self.update_commands = 0
        self.pulled = False

    def __repr__(self):
        return f"FakeNode({self.name})"

    def scalar(self, sql):
        assert self.pulled, "system.mutations read without a pull"
        self.pulled = False
        if "max(mutation_id)" in sql:
            return max(self.keeper.entries)
        if "count()" in sql and "system.mutations" in sql:
            after = sql.split("mutation_id > '")[1].split("'")[0]
            bucket = int(sql.split("WHERE bucket = ")[1].split("(")[0])
            return str(sum(1 for n, b in self.keeper.entries.items() if n > after and b == bucket))
        raise AssertionError(f"unexpected query: {sql}")

    def command(self, sql, timeout=None, settings=None):
        if sql.startswith("SYSTEM SYNC REPLICA") and sql.endswith(" PULL"):
            self.pulled = True
            return
        assert "UPDATE" in sql
        self.update_commands += 1
        if self.fail_before_landing:
            self.fail_before_landing = False
            raise ConnectionRefusedError("[Errno 111] Connection refused")
        bucket = int(sql.split("WHERE bucket = ")[1])
        self.keeper.add_update(bucket)
        if self.fail_after_landing:
            self.fail_after_landing = False
            raise QueryError(self, 500, "Code: 394. DB::Exception: Query 'q' is killed in pending state. "
                                        "(QUERY_WAS_CANCELLED)", sql)


class FakeCluster:
    def __init__(self, *nodes):
        self._nodes = list(nodes)

    def nodes(self):
        return self._nodes


class FakeModel:
    def __init__(self):
        self.applied = []

    def _pred_bucket(self, param):
        return param % 64

    def apply(self, op):
        self.applied.append(op.op_id)


def make_driver(monkeypatch, cluster):
    monkeypatch.setattr(time, "sleep", lambda s: None)
    monkeypatch.setattr(soak_run, "node_for", lambda c, target: c.nodes()[target])
    d = soak_run.Driver.__new__(soak_run.Driver)
    d.cluster = cluster
    d.model = FakeModel()
    d.model_lock = __import__("threading").Lock()
    d.transport_resilient = True
    d.inflight = []
    d.drain = lambda: None
    d._note_transport_retry = lambda *a, **k: None
    return d


def update_op(bucket):
    return Op(op_id=42, type=OpType.UPDATE, target=0, param=bucket)


def test_update_whose_first_attempt_landed_is_not_sent_again(monkeypatch):
    keeper = FakeKeeper()
    n1 = FakeNode("ch1", keeper, fail_first_command_after_landing=True)
    n2 = FakeNode("ch2", keeper)
    d = make_driver(monkeypatch, FakeCluster(n1, n2))

    d.apply_barrier(update_op(46))

    assert n1.update_commands + n2.update_commands == 1
    assert sum(1 for b in keeper.entries.values() if b == 46) == 1
    assert d.model.applied == [42]


def test_update_whose_first_attempt_never_landed_is_sent_again(monkeypatch):
    keeper = FakeKeeper()
    n1 = FakeNode("ch1", keeper, fail_before_landing=True)
    n2 = FakeNode("ch2", keeper)
    d = make_driver(monkeypatch, FakeCluster(n1, n2))

    d.apply_barrier(update_op(46))

    assert n1.update_commands + n2.update_commands == 2
    assert sum(1 for b in keeper.entries.values() if b == 46) == 1
    assert d.model.applied == [42]


def test_an_older_update_of_the_same_bucket_does_not_count(monkeypatch):
    keeper = FakeKeeper()
    keeper.entries["0000000008"] = 46   # an earlier op updated the same bucket
    n1 = FakeNode("ch1", keeper, fail_before_landing=True)
    n2 = FakeNode("ch2", keeper)
    d = make_driver(monkeypatch, FakeCluster(n1, n2))

    d.apply_barrier(update_op(46))

    assert sum(1 for b in keeper.entries.values() if b == 46) == 2
