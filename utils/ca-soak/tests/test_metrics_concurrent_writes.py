import sqlite3
import threading

import pytest

from soak import metrics


def _snap(ts, node="node1"):
    return dict(ts=ts, node=node, parts_active=1, parts_inactive=0, table_rows=1, bytes_on_disk=1, pool_objects=1,
                pool_bytes=1, repl_queue=0, mutations_pending=0, merges=0, restarts=0)


def _phase_rows(ts):
    return [dict(ts=ts, checkpoint="t", node="n", phase="p%d" % i, rounds=1, calls=1, total_us=1, max_us=1,
                 logs_accounted=0, logs_applied=0, transactions_unapplied=0, ref_folding_aborted=0,
                 metrics_json="{}", events_json="{}") for i in range(3)]


@pytest.fixture(autouse=True)
def _clean_failures():
    metrics.reset_write_failures()
    yield
    metrics.reset_write_failures()


def _hammer(conn, n=300):
    errors = []

    def ticker():
        try:
            for i in range(n):
                metrics.record(conn, _snap(i))
        except BaseException as e:   # noqa: BLE001
            errors.append(e)

    def checkpoint():
        try:
            for i in range(n):
                metrics.record_phases(conn, _phase_rows(i))
        except BaseException as e:   # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=ticker), threading.Thread(target=checkpoint)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return errors


def test_two_threads_share_one_connection_without_losing_rows(tmp_path):
    conn = metrics.open_db(str(tmp_path / "m.db"))
    assert _hammer(conn) == []
    assert metrics.write_failures() == []
    assert len(metrics.rows(conn)) == 300
    assert len(metrics.gc_phase_rows(conn)) == 900


def test_transient_error_is_retried(tmp_path):
    conn = metrics.open_db(str(tmp_path / "m.db"))
    calls = []

    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise sqlite3.OperationalError("database is locked")

    assert metrics.write_retrying("x", flaky, backoff_s=0)
    assert len(calls) == 3
    assert metrics.write_failures() == []


def test_persistent_error_is_a_recorded_failure_not_an_exception():
    def broken():
        raise sqlite3.OperationalError("disk I/O error")

    assert not metrics.write_retrying("x", broken, attempts=3, backoff_s=0)
    failures = metrics.write_failures()
    assert len(failures) == 1 and "disk I/O error" in failures[0] and "3 attempts" in failures[0]
