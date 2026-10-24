"""Registry of cost cards; each card module adds itself here."""
CARDS: dict = {}

from . import cas_probe, probes, samples  # noqa: E402,F401  (register the phase-0 probes and the estimate's sample cards)
