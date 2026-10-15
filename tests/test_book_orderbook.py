"""OrderBook unit tests and a Hypothesis model test.

The model test drives random snapshots, deltas and truncations through both
the real book and a naive ``dict`` reference and requires identical
``top(n)``, best levels, length and negative-size counters.
"""

from __future__ import annotations

import time
from decimal import Decimal

from hypothesis import given, settings
from hypothesis import strategies as st

from tickvault.book.orderbook import OrderBook
from tickvault.models import PriceLevel, Side


def _lvl(price: str, size: str) -> PriceLevel:
    return (Decimal(price), Decimal(size))


def test_apply_snapshot_replaces_the_previous_book() -> None:
    book = OrderBook()
    book.apply_snapshot([_lvl("100", "1")], [_lvl("101", "1")])
    book.apply_snapshot([_lvl("200", "2")], [_lvl("201", "3")])
    assert book.top(10) == ([_lvl("200", "2")], [_lvl("201", "3")])
    assert len(book) == 2


def test_snapshot_levels_are_sorted_by_price_whatever_the_input_order() -> None:
    book = OrderBook()
    book.apply_snapshot(
        [_lvl("99.5", "1"), _lvl("100", "2"), _lvl("99", "3")],
        [_lvl("101", "4"), _lvl("100.5", "5"), _lvl("102", "6")],
    )
    bids, asks = book.top(10)
    assert bids == [_lvl("100", "2"), _lvl("99.5", "1"), _lvl("99", "3")]
    assert asks == [_lvl("100.5", "5"), _lvl("101", "4"), _lvl("102", "6")]


def test_apply_delta_sets_absolute_size_and_zero_removes() -> None:
    book = OrderBook()
    book.apply_snapshot([_lvl("100", "1")], [_lvl("101", "1")])
    book.apply_delta("bid", Decimal("100"), Decimal("5"))
    assert book.best_bid() == _lvl("100", "5")
    book.apply_delta("bid", Decimal("100"), Decimal("0"))
    assert book.best_bid() is None
    assert len(book) == 1


def test_removing_a_missing_level_is_a_noop() -> None:
    book = OrderBook()
    book.apply_snapshot([_lvl("100", "1")], [])
    book.apply_delta("bid", Decimal("42"), Decimal("0"))
    book.apply_delta("ask", Decimal("42"), Decimal("0"))
    assert book.top(10) == ([_lvl("100", "1")], [])
    assert len(book) == 1


def test_best_levels_are_none_on_an_empty_side() -> None:
    book = OrderBook()
    assert book.best_bid() is None
    assert book.best_ask() is None
    book.apply_snapshot([], [_lvl("101", "1")])
    assert book.best_bid() is None
    assert book.best_ask() == _lvl("101", "1")


def test_top_is_best_first_and_clamped_per_side() -> None:
    book = OrderBook()
    book.apply_snapshot(
        [_lvl("100", "1"), _lvl("99", "2")],
        [_lvl("101", "3"), _lvl("102", "4"), _lvl("103", "5")],
    )
    assert book.top(2) == (
        [_lvl("100", "1"), _lvl("99", "2")],
        [_lvl("101", "3"), _lvl("102", "4")],
    )
    assert book.top(5) == book.top(10)
    assert book.top(0) == ([], [])
    assert book.top(-1) == ([], [])


def test_truncate_keeps_the_best_levels() -> None:
    book = OrderBook()
    book.apply_snapshot(
        [_lvl("100", "1"), _lvl("99", "2"), _lvl("98", "3")],
        [_lvl("101", "4"), _lvl("102", "5"), _lvl("103", "6")],
    )
    book.truncate(2)
    assert book.top(10) == (
        [_lvl("100", "1"), _lvl("99", "2")],
        [_lvl("101", "4"), _lvl("102", "5")],
    )
    assert len(book) == 4
    book.truncate(0)
    assert len(book) == 0
    book.truncate(5)
    assert len(book) == 0


def test_duplicate_snapshot_prices_keep_the_last_size_and_are_counted() -> None:
    book = OrderBook()
    book.apply_snapshot([_lvl("100", "1"), _lvl("100", "7")], [])
    assert book.best_bid() == _lvl("100", "7")
    assert book.duplicate_level_count == 1
    book.apply_snapshot([_lvl("100", "1")], [])
    assert book.duplicate_level_count == 0


def test_negative_size_counter_tracks_the_stored_levels() -> None:
    book = OrderBook()
    book.apply_delta("bid", Decimal("100"), Decimal("-1"))
    assert book.negative_size_count == 1
    book.apply_delta("bid", Decimal("100"), Decimal("2"))
    assert book.negative_size_count == 0
    book.apply_snapshot([_lvl("100", "-3"), _lvl("99", "1")], [])
    assert book.negative_size_count == 1
    book.truncate(1)
    assert book.negative_size_count == 1  # the best (negative) level survives
    book.truncate(0)
    assert book.negative_size_count == 0


def test_len_counts_both_sides() -> None:
    book = OrderBook()
    book.apply_snapshot([_lvl("100", "1"), _lvl("99", "1")], [_lvl("101", "1")])
    assert len(book) == 3


def test_full_depth_coinbase_snapshot_is_fast() -> None:
    bids = [(Decimal("81000") - Decimal(index) / 100, Decimal("0.5")) for index in range(20_000)]
    asks = [(Decimal("81001") + Decimal(index) / 100, Decimal("0.5")) for index in range(20_470)]
    book = OrderBook()
    started = time.perf_counter()
    book.apply_snapshot(bids, asks)
    elapsed = time.perf_counter() - started
    assert len(book) == 40_470
    assert book.best_bid() == bids[0]
    assert book.best_ask() == asks[0]
    assert elapsed < 0.5, f"40k-level snapshot took {elapsed:.3f}s"


class _NaiveBook:
    """Reference implementation mirroring the documented OrderBook semantics."""

    def __init__(self) -> None:
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}

    def _side(self, side: Side) -> dict[Decimal, Decimal]:
        return self.bids if side == "bid" else self.asks

    @staticmethod
    def _apply(
        levels: dict[Decimal, Decimal], price: Decimal, size: Decimal
    ) -> None:
        if size == 0:
            levels.pop(price, None)
        else:
            levels[price] = size

    def apply_snapshot(
        self, bids: list[PriceLevel], asks: list[PriceLevel]
    ) -> None:
        self.bids = {}
        self.asks = {}
        for price, size in bids:
            self._apply(self.bids, price, size)
        for price, size in asks:
            self._apply(self.asks, price, size)

    def apply_delta(self, side: Side, price: Decimal, size: Decimal) -> None:
        self._apply(self._side(side), price, size)

    def truncate(self, depth: int) -> None:
        keep = max(depth, 0)
        self.bids = dict(sorted(self.bids.items(), reverse=True)[:keep])
        self.asks = dict(sorted(self.asks.items())[:keep])

    def top(self, n: int) -> tuple[list[PriceLevel], list[PriceLevel]]:
        if n <= 0:
            return [], []
        bids = sorted(self.bids.items(), key=lambda item: item[0], reverse=True)[:n]
        asks = sorted(self.asks.items(), key=lambda item: item[0])[:n]
        return bids, asks

    def best_bid(self) -> PriceLevel | None:
        if not self.bids:
            return None
        return max(self.bids.items())

    def best_ask(self) -> PriceLevel | None:
        if not self.asks:
            return None
        return min(self.asks.items())

    def negative_size_count(self) -> int:
        return sum(
            1 for size in list(self.bids.values()) + list(self.asks.values()) if size < 0
        )


_PRICES = st.integers(min_value=1, max_value=200).map(lambda value: Decimal(value) / 100)
_SIZES = st.integers(min_value=0, max_value=10).map(lambda value: Decimal(value) / 10)
_LEVELS = st.lists(st.tuples(_PRICES, _SIZES), max_size=8)
_SIDES = st.sampled_from(["bid", "ask"])


@settings(derandomize=True, max_examples=100)
@given(st.data())
def test_matches_naive_model(data: st.DataObject) -> None:
    book = OrderBook()
    naive = _NaiveBook()
    for _ in range(data.draw(st.integers(min_value=1, max_value=30))):
        operation = data.draw(st.sampled_from(["snapshot", "delta", "truncate"]))
        if operation == "snapshot":
            bids = data.draw(_LEVELS)
            asks = data.draw(_LEVELS)
            book.apply_snapshot(bids, asks)
            naive.apply_snapshot(bids, asks)
        elif operation == "delta":
            side: Side = data.draw(_SIDES)
            price = data.draw(_PRICES)
            size = data.draw(_SIZES)
            book.apply_delta(side, price, size)
            naive.apply_delta(side, price, size)
        else:
            depth = data.draw(st.integers(min_value=0, max_value=6))
            book.truncate(depth)
            naive.truncate(depth)

    for n in (0, 1, 2, 3, 5, 10):
        assert book.top(n) == naive.top(n), f"top({n}) diverged"
    assert book.best_bid() == naive.best_bid()
    assert book.best_ask() == naive.best_ask()
    assert len(book) == len(naive.bids) + len(naive.asks)
    assert book.negative_size_count == naive.negative_size_count()
