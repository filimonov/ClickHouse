"""Registry of cost cards; each card module adds itself here."""
CARDS: dict = {}

# The phase-0 probes, the lifecycle cards (`merge-natural` runs the merge sample), the CAS cards (`gc-scale` runs the
# GC sample) and the s3 shape and operation cards.
from . import cas, cas_probe, io_sizes, lifecycle, ops, probes, samples, shapes  # noqa: E402,F401
