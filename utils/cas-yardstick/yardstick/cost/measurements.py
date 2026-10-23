"""Measurement records (`measurement/1`) on disk: validated on write, so an invalid record never lands."""
from __future__ import annotations

import json
import os
import sys

TOOLS = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', '..', 'storage-cost-calculator', 'tools')


def _validator():
    if TOOLS not in sys.path:
        sys.path.insert(0, TOOLS)
    from artifacts import validate_measurement  # noqa: PLC0415 (the tools directory is not a package)
    return validate_measurement


def write(path: str, record: dict) -> None:
    problems = _validator()(record)
    if problems:
        raise ValueError('invalid measurement record: ' + '; '.join(problems))
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(record, f, indent=2, sort_keys=True)
        f.write('\n')


def read(path: str) -> dict:
    with open(path, encoding='utf-8') as f:
        return json.load(f)
