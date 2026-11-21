"""Failures of the harness itself (not of the product) that must fail the run's verdict without ending the run."""

import threading

_lock = threading.Lock()
_failures: list = []


def add(text: str) -> None:
    with _lock:
        _failures.append(text)


def all_failures() -> list:
    with _lock:
        return list(_failures)


def reset() -> None:
    with _lock:
        _failures.clear()
