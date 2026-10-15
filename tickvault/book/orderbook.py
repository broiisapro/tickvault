"""Price-level order book interface.

The live implementation (Epic 1) replaces every ``NotImplementedError`` body
while keeping these signatures. All levels are exact ``Decimal`` pairs and
each side is kept sorted best-first: bids descending by price, asks ascending.
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

from tickvault.models import PriceLevel, Side


class OrderBook:
    """One symbol's price-level book.

    Holds every level received until :meth:`truncate` is applied. ``size`` of
    ``0`` removes a level. ``len(book)`` is the total number of levels on both
    sides.
    """

    def apply_snapshot(self, bids: Sequence[PriceLevel], asks: Sequence[PriceLevel]) -> None:
        """Replace the whole book with ``bids`` and ``asks`` (any level order)."""
        raise NotImplementedError

    def apply_delta(self, side: Side, price: Decimal, size: Decimal) -> None:
        """Set ``price`` on ``side`` to absolute ``size``; ``size == 0`` removes it."""
        raise NotImplementedError

    def best_bid(self) -> PriceLevel | None:
        """Return the best ``(price, size)`` bid, or ``None`` if there is none."""
        raise NotImplementedError

    def best_ask(self) -> PriceLevel | None:
        """Return the best ``(price, size)`` ask, or ``None`` if there is none."""
        raise NotImplementedError

    def top(self, n: int) -> tuple[list[PriceLevel], list[PriceLevel]]:
        """Return up to ``n`` levels per side as ``(bids, asks)``, best first."""
        raise NotImplementedError

    def truncate(self, depth: int) -> None:
        """Drop all but the best ``depth`` levels per side."""
        raise NotImplementedError

    def __len__(self) -> int:
        """Total number of levels on both sides."""
        raise NotImplementedError
