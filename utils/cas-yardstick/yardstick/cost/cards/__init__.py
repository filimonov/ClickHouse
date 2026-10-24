"""Registry of cost cards; each card module adds itself here."""
CARDS: dict = {}

# The phase-0 probes, the estimate's GC sample and the lifecycle cards (whose `merge-natural` runs the merge sample).
from . import cas_probe, lifecycle, probes, samples  # noqa: E402,F401
