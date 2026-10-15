"""Invariant-check tests."""

from __future__ import annotations

from decimal import Decimal

from tickvault.book.invariants import check_invariants
from tickvault.book.orderbook import OrderBook
from tickvault.models import PriceLevel


def _lvl(price: str, size: str) -> PriceLevel:
    return (Decimal(price), Decimal(size))


def test_consistent_book_has_no_violations() -> None:
    book = OrderBook()
    assert check_invariants(book) == []
    book.apply_snapshot([_lvl("100", "1")], [_lvl("101", "2")])
    assert check_invariants(book) == []


def test_crossed_book_is_flagged() -> None:
    book = OrderBook()
    book.apply_snapshot([_lvl("102", "1")], [_lvl("101", "1")])
    assert check_invariants(book) == ["crossed"]


def test_touching_best_prices_are_crossed() -> None:
    book = OrderBook()
    book.apply_snapshot([_lvl("100", "1")], [_lvl("100", "1")])
    assert check_invariants(book) == ["crossed"]


def test_negative_size_from_a_delta_is_flagged_until_fixed() -> None:
    book = OrderBook()
    book.apply_snapshot([_lvl("100", "1")], [_lvl("101", "1")])
    book.apply_delta("bid", Decimal("100"), Decimal("-1"))
    assert check_invariants(book) == ["negative_size"]
    book.apply_delta("bid", Decimal("100"), Decimal("1"))
    assert check_invariants(book) == []


def test_negative_size_inside_a_snapshot_is_flagged() -> None:
    book = OrderBook()
    book.apply_snapshot([_lvl("100", "-0.5")], [_lvl("101", "1")])
    assert check_invariants(book) == ["negative_size"]


def test_negative_level_removed_by_truncate_clears_the_violation() -> None:
    book = OrderBook()
    book.apply_snapshot([_lvl("100", "1"), _lvl("99", "-1")], [])
    book.truncate(1)
    assert check_invariants(book) == []


def test_duplicate_snapshot_price_is_flagged_until_the_next_snapshot() -> None:
    book = OrderBook()
    book.apply_snapshot([_lvl("100", "1"), _lvl("100", "2")], [])
    assert check_invariants(book) == ["duplicate_level"]
    book.apply_snapshot([_lvl("100", "1")], [])
    assert check_invariants(book) == []


def test_violations_are_reported_in_a_fixed_order() -> None:
    book = OrderBook()
    book.apply_snapshot(
        [_lvl("100", "-1"), _lvl("100", "2"), _lvl("99", "-1")],
        [_lvl("99.5", "1")],
    )
    assert check_invariants(book) == ["crossed", "negative_size", "duplicate_level"]
