"""Sample cards of the phase-0 runtime estimate now run on their research cards.

The merge sample runs on the lifecycle card `merge-natural` (`lifecycle.py`); the GC sample on the CAS card
`gc-scale` (`cas.py`). The phase-0 `gc-scale.sample` manifest predates the CAS card's parameters, so it fails
closed (a missing parameter) instead of running a card it was not written for.
"""
