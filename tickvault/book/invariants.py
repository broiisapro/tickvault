"""Book invariant checks.

``check_invariants`` returns the violation codes of the current book state,
in a fixed order, or an empty list when the book is consistent:

* ``crossed``: the best bid is at or above the best ask;
* ``negative_size``: a level is stored with a negative size;
* ``duplicate_level``: the most recent snapshot contained the same price
  twice (defensive: a price-level book cannot hold duplicate keys, so the
  occurrence is recorded by the book at insert time).

The checks are O(1): the book maintains the counters as levels change, so the
processor can run them after every applied update even on full-depth Coinbase
books (~40k levels).
"""

from __future__ import annotations

from tickvault.book.orderbook import OrderBook


def check_invariants(book: OrderBook) -> list[str]:
    """Return the invariant violation codes of ``book`` (empty when OK)."""
    violations: list[str] = []
    best_bid = book.best_bid()
    best_ask = book.best_ask()
    if best_bid is not None and best_ask is not None and best_bid[0] >= best_ask[0]:
        violations.append("crossed")
    if book.negative_size_count > 0:
        violations.append("negative_size")
    if book.duplicate_level_count > 0:
        violations.append("duplicate_level")
    return violations
