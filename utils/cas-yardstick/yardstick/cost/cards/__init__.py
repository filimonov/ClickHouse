"""Registry of cost cards; each card module adds itself here."""
CARDS: dict = {}

from . import probes  # noqa: E402,F401  (registers the phase-0 probes and the estimate's sample cards)
