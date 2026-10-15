"""Price-level order book.

One :class:`sortedcontainers.SortedDict` per side keeps levels ordered by
price ascending: the best bid is the highest key, the best ask the lowest.
All levels are exact ``Decimal`` pairs and each side is kept best-first when
read (bids descending by price, asks ascending). ``size`` of ``0`` removes a
level. ``len(book)`` is the total number of levels on both sides.

The book also tracks two defensive counters read by
:func:`tickvault.book.invariants.check_invariants`:

* ``negative_size_count``: levels currently stored with a negative size (a
  malformed venue payload the invariants must flag);
* ``duplicate_level_count``: duplicate prices seen while applying the most
  recent snapshot. A price-level book cannot hold a duplicate key, so the
  book records the occurrence at insert time and the next snapshot clears it.

Keeping the counters on the book makes invariant checking O(1) per update,
which matters because Coinbase books keep full depth (~40k levels for
BTC-USD) and invariants run after every applied update.
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

from sortedcontainers import SortedDict

from tickvault.models import PriceLevel, Side

_Levels = SortedDict[Decimal, Decimal]


class OrderBook:
    """One symbol's price-level book.

    Holds every level received until :meth:`truncate` is applied. ``size`` of
    ``0`` removes a level. ``len(book)`` is the total number of levels on both
    sides.
    """

    def __init__(self) -> None:
        self._bids: _Levels = SortedDict()
        self._asks: _Levels = SortedDict()
        self._negative_sizes = 0
        self._duplicate_levels = 0

    @property
    def negative_size_count(self) -> int:
        """Number of stored levels whose size is negative."""
        return self._negative_sizes

    @property
    def duplicate_level_count(self) -> int:
        """Duplicate prices seen while applying the most recent snapshot."""
        return self._duplicate_levels

    def apply_snapshot(self, bids: Sequence[PriceLevel], asks: Sequence[PriceLevel]) -> None:
        """Replace the whole book with ``bids`` and ``asks`` (any level order)."""
        self._bids.clear()
        self._asks.clear()
        self._negative_sizes = 0
        self._duplicate_levels = 0
        for price, size in bids:
            self._store(self._bids, price, size, from_snapshot=True)
        for price, size in asks:
            self._store(self._asks, price, size, from_snapshot=True)

    def apply_delta(self, side: Side, price: Decimal, size: Decimal) -> None:
        """Set ``price`` on ``side`` to absolute ``size``; ``size == 0`` removes it."""
        self._store(self._bids if side == "bid" else self._asks, price, size, from_snapshot=False)

    def _store(
        self, levels: _Levels, price: Decimal, size: Decimal, *, from_snapshot: bool
    ) -> None:
        previous = levels.get(price)
        if previous is not None:
            if from_snapshot:
                self._duplicate_levels += 1
            if previous < 0:
                self._negative_sizes -= 1
        if size < 0:
            self._negative_sizes += 1
        if size == 0:
            if previous is not None:
                del levels[price]
        else:
            levels[price] = size

    def best_bid(self) -> PriceLevel | None:
        """Return the best ``(price, size)`` bid, or ``None`` if there is none."""
        if not self._bids:
            return None
        return self._bids.peekitem(-1)

    def best_ask(self) -> PriceLevel | None:
        """Return the best ``(price, size)`` ask, or ``None`` if there is none."""
        if not self._asks:
            return None
        return self._asks.peekitem(0)

    def top(self, n: int) -> tuple[list[PriceLevel], list[PriceLevel]]:
        """Return up to ``n`` levels per side as ``(bids, asks)``, best first."""
        if n <= 0:
            return [], []
        bids = self._bids.items()[-n:][::-1]
        asks = self._asks.items()[:n]
        return bids, asks

    def truncate(self, depth: int) -> None:
        """Drop all but the best ``depth`` levels per side."""
        keep = max(depth, 0)
        self._discard(self._bids, 0, max(len(self._bids) - keep, 0))
        self._discard(self._asks, keep, max(len(self._asks) - keep, 0))

    def _discard(self, levels: _Levels, start: int, count: int) -> None:
        if count <= 0:
            return
        for price in list(levels.islice(start, start + count)):
            if levels[price] < 0:
                self._negative_sizes -= 1
            del levels[price]

    def __len__(self) -> int:
        """Total number of levels on both sides."""
        return len(self._bids) + len(self._asks)
