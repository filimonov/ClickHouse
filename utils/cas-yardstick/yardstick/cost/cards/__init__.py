"""Registry of cost cards; each card module adds itself here."""
CARDS: dict = {}

# The phase-0 probes, the lifecycle cards (`merge-natural` runs the merge sample) and the CAS cards (`gc-scale` runs
# the GC sample).
from . import cas, cas_probe, lifecycle, probes, samples  # noqa: E402,F401
