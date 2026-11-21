"""Stale-edge convergence: a checkpoint fails only on keys that both scans report."""

import json

import pytest

from soak import convergence, harness_failures
from soak import run as run_mod
from soak.checker import CheckpointFailure
from soak.convergence import RoundWaitTimeout


def _scan(*keys, stale_edge=None):
    return {"stale_edge": len(keys) if stale_edge is None else stale_edge,
            "detail": [{"class": "stale-edge", "key": k, "size": 1} for k in keys]}


def _converge(first, scans, wait=lambda: [11, 12]):
    logs = []
    it = iter(scans)
    out = convergence.converge(first, rescan=lambda: next(it), wait_rounds=wait, log_fn=logs.append, label="t")
    return out, logs


def test_disjoint_sets_pass_and_log_the_sizes():
    (verdict, _, second), logs = _converge(_scan("a", "b"), [_scan("c")])
    assert verdict == "converged" and second["detail"][0]["key"] == "c"
    assert "|S1|=2 |S2|=1 |S1&S2|=0" in logs[0]


def test_empty_second_scan_passes():
    (verdict, _, _), logs = _converge(_scan("a"), [_scan()])
    assert verdict == "converged" and "|S1&S2|=0" in logs[0]


def test_intersecting_sets_fail():
    (verdict, why, _), logs = _converge(_scan("a", "b"), [_scan("b", "c")])
    assert verdict == "persistent" and "'b'" in why
    assert "|S1&S2|=1" in logs[0]


def test_round_wait_timeout_is_undecided_not_a_pass():
    def wait():
        raise RoundWaitTimeout("only 1 of 2")

    (verdict, why, second), logs = _converge(_scan("a"), [], wait=wait)
    assert verdict == "undecided" and second is None and "only 1 of 2" in why


def test_a_count_without_keys_fails_closed():
    (verdict, _, _), _ = _converge(_scan(stale_edge=3), [])
    assert verdict == "persistent"
    (verdict, _, _), _ = _converge(_scan("a"), [_scan(stale_edge=2)])
    assert verdict == "persistent"


class _FakeNode:
    def __init__(self, rows):
        self.rows = list(rows)

    def query(self, sql, timeout=None, settings=None):
        return "\n".join(json.dumps(r) for r in self.rows.pop(0))


def test_wait_counts_only_led_non_deferred_rounds_above_the_bound():
    node = _FakeNode([
        [{"acquired_lease": 1, "deferred": 0, "round": 5}],     # at or below the bound
        [{"acquired_lease": 1, "deferred": 1, "round": 9}],     # deferred
        [{"acquired_lease": 0, "deferred": 0, "round": 0}],     # not the leader
        [{"acquired_lease": 1, "deferred": 0, "round": 8}],
        [{"acquired_lease": 1, "deferred": 0, "round": 9}],
    ])
    assert convergence.wait_for_committed_rounds([node], above_round=5, need=2, sleep_fn=lambda s: None) == [8, 9]


def test_wait_times_out():
    clock = iter(range(0, 1000, 100))
    node = _FakeNode([[{"acquired_lease": 0, "deferred": 0, "round": 0}]] * 20)
    with pytest.raises(RoundWaitTimeout):
        convergence.wait_for_committed_rounds([node], above_round=0, timeout_s=250, now_fn=lambda: next(clock),
                                              sleep_fn=lambda s: None)


# --- the same logic through the real checkpoint -------------------------------------------------------
from tests.test_checkpoint_stale_edge import CLEAN_SUMMARY, _Cluster, _Driver, _Model   # noqa: E402,F401
from tests.test_checkpoint_stale_edge import stubbed   # noqa: E402,F401


def _fsck(first_keys, second_keys):
    scans = {"n": 0}

    def fsck(detail):
        r = dict(CLEAN_SUMMARY, unreachable=1)
        if detail:
            keys = first_keys if scans["n"] == 0 else second_keys
            scans["n"] += 1
            r["stale_edge"] = len(keys)
            r["detail"] = [{"class": "stale-edge", "key": k, "size": 1} for k in keys]
        return r
    return fsck


def _checkpoint():
    return run_mod.checkpoint(_Driver(), _Cluster(), _Model(), 1)


def test_checkpoint_passes_when_the_stale_keys_were_transient(stubbed):
    stubbed["install"](_fsck(["x"], []))
    _checkpoint()
    assert any("|S1|=1 |S2|=0 |S1&S2|=0" in m for m in stubbed["log"])


def test_checkpoint_fails_on_a_persistent_stale_edge(stubbed):
    stubbed["install"](_fsck(["x"], ["x"]))
    with pytest.raises(CheckpointFailure, match="stale_edge"):
        _checkpoint()


def test_checkpoint_records_a_harness_failure_when_the_round_wait_times_out(stubbed, monkeypatch):
    def timeout(nodes, **k):
        raise RoundWaitTimeout("no rounds")
    monkeypatch.setattr(run_mod.convergence, "wait_for_committed_rounds", timeout)
    stubbed["install"](_fsck(["x"], ["x"]))
    _checkpoint()   # the run goes on; the verdict fails through the harness-failure list
    assert stubbed["harness"] and "no rounds" in stubbed["harness"][0]
