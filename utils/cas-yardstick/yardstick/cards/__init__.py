"""Registered cards in report order."""
from .cycle import Cycle

CARDS = {card.name: card for card in (Cycle(),)}
