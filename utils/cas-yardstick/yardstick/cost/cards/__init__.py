"""Registry of cost cards; each card module adds itself here."""
CARDS: dict = {}

# The phase-0 probes, the lifecycle cards (`merge-natural` runs the merge sample), the CAS cards (`gc-scale` runs the
# GC sample), the s3 shape and operation cards, and the replication and fault cards.
from . import cas, cas_probe, faults, io_sizes, lifecycle, ops, probes, replication, samples, shapes  # noqa: E402,F401
