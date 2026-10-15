"""Tests for :mod:`tickvault.reconcile.tardis`.

The fixture test reproduces ``reconcile_expected.json`` field by field from the
committed Tardis CSV and TickVault Parquet sample; the unit tests cover the
loader's decimal handling, id matching, window clipping and the renderers.
"""

from __future__ import annotations

import gzip
import json
from decimal import Decimal
from pathlib import Path

import pytest

from tickvault.models import TradeSide
from tickvault.reconcile.tardis import (
    ReconcileWindow,
    TardisTrade,
    TickVaultTrade,
    load_tardis_trades,
    load_tickvault_trades,
    reconcile,
    report_to_json,
    report_to_text,
    window_from_iso,
)

HEADER = "exchange,symbol,timestamp,local_timestamp,id,side,price,amount"


def _csv_bytes(rows: list[str], header: str = HEADER) -> str:
    return header + "\n" + "\n".join(rows) + ("\n" if rows else "")


def _write_gzip_csv(path: Path, rows: list[str], header: str = HEADER) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.write(_csv_bytes(rows, header))


def _tardis(
    trade_id: str,
    ts_ns: int = 1_000_000_000,
    price: str = "100.0",
    amount: str = "1.0",
    side: TradeSide = "buy",
) -> TardisTrade:
    return TardisTrade(
        exchange="coinbase",
        symbol="BTC-USD",
        exchange_ts_ns=ts_ns,
        local_ts_ns=ts_ns,
        trade_id=trade_id,
        side=side,
        price=Decimal(price),
        amount=Decimal(amount),
    )


def _tv(
    trade_id: str,
    ts_ns: int = 1_000_000_000,
    price: str = "100.0",
    size: str = "1.0",
    side: TradeSide = "buy",
) -> TickVaultTrade:
    return TickVaultTrade(
        venue="coinbase",
        symbol="BTC-USD",
        exchange_ts_ns=ts_ns,
        recv_ts_ns=ts_ns,
        connection_id="conn-a",
        ingest_seq=0,
        trade_id=trade_id,
        side=side,
        price=Decimal(price),
        size=Decimal(size),
    )


def test_fixture_reconciliation_matches_expected(
    tardis_sample_path: Path,
    tv_trades_sample_root: Path,
    reconcile_expected: dict[str, object],
) -> None:
    tardis = load_tardis_trades(tardis_sample_path)
    tickvault = load_tickvault_trades(tv_trades_sample_root, "coinbase", "BTC-USD")
    expected_window = reconcile_expected["window"]
    assert isinstance(expected_window, dict)
    window = window_from_iso(str(expected_window["start"]), str(expected_window["end"]))
    report = reconcile(tardis, tickvault, window)

    assert report.window == window
    assert report.tardis_trades == reconcile_expected["tardis_trades"] == 2089
    assert report.tickvault_trades == reconcile_expected["tickvault_trades"] == 2087
    assert report.matched == reconcile_expected["matched"] == 2084
    assert len(report.missing_in_tickvault_ids) == reconcile_expected["missing_in_tickvault"]
    assert len(report.extra_in_tickvault_ids) == reconcile_expected["extra_in_tickvault"]
    assert len(report.price_mismatch_ids) == reconcile_expected["price_mismatches"]
    assert len(report.size_mismatch_ids) == reconcile_expected["size_mismatches"]
    assert len(report.side_mismatch_ids) == reconcile_expected["side_mismatches"]
    assert list(report.missing_in_tickvault_ids) == reconcile_expected["missing_in_tickvault_ids"]
    assert list(report.extra_in_tickvault_ids) == reconcile_expected["extra_in_tickvault_ids"]
    assert list(report.price_mismatch_ids) == reconcile_expected["price_mismatch_ids"]
    assert list(report.size_mismatch_ids) == reconcile_expected["size_mismatch_ids"]
    assert list(report.side_mismatch_ids) == reconcile_expected["side_mismatch_ids"]


def test_default_window_is_the_dataset_overlap(tmp_path: Path) -> None:
    tardis = [_tardis("a", ts_ns=100), _tardis("b", ts_ns=200), _tardis("c", ts_ns=300)]
    tickvault = [_tv("b", ts_ns=200), _tv("c", ts_ns=300), _tv("d", ts_ns=400)]
    report = reconcile(tardis, tickvault)
    assert report.window == ReconcileWindow(start_ns=200, end_ns=300)
    assert report.tardis_trades == 2
    assert report.tickvault_trades == 2
    assert report.matched == 2
    assert report.missing_in_tickvault_ids == ()
    assert report.extra_in_tickvault_ids == ()


def test_explicit_window_clips_both_sides_inclusively() -> None:
    tardis = [_tardis("a", ts_ns=100), _tardis("b", ts_ns=200), _tardis("c", ts_ns=300)]
    tickvault = [_tv("b", ts_ns=200), _tv("c", ts_ns=300), _tv("d", ts_ns=400)]
    report = reconcile(tardis, tickvault, ReconcileWindow(start_ns=200, end_ns=300))
    assert report.window == ReconcileWindow(start_ns=200, end_ns=300)
    assert report.tardis_trades == 2
    assert report.tickvault_trades == 2
    assert report.matched == 2
    assert report.missing_in_tickvault_ids == ()
    assert report.extra_in_tickvault_ids == ()


def test_window_can_be_a_tuple() -> None:
    tardis = [_tardis("a", ts_ns=100), _tardis("b", ts_ns=200)]
    tickvault = [_tv("b", ts_ns=200)]
    by_tuple = reconcile(tardis, tickvault, (150, 250))
    by_object = reconcile(tardis, tickvault, ReconcileWindow(start_ns=150, end_ns=250))
    assert by_tuple == by_object
    assert by_tuple.window == ReconcileWindow(start_ns=150, end_ns=250)
    assert by_tuple.matched == 1
    assert by_tuple.missing_in_tickvault_ids == ()
    assert by_tuple.extra_in_tickvault_ids == ()


def test_window_excluding_everything_reports_zeroes() -> None:
    tardis = [_tardis("a", ts_ns=100)]
    tickvault = [_tv("a", ts_ns=100)]
    report = reconcile(tardis, tickvault, (200, 300))
    assert report.window == ReconcileWindow(start_ns=200, end_ns=300)
    assert (report.tardis_trades, report.tickvault_trades, report.matched) == (0, 0, 0)
    assert report.missing_in_tickvault_ids == ()
    assert report.extra_in_tickvault_ids == ()


def test_empty_side_has_no_window_and_no_matches() -> None:
    report = reconcile([], [_tv("x")])
    assert report.window is None
    assert report.tardis_trades == 0
    assert report.tickvault_trades == 1
    assert report.matched == 0
    assert report.extra_in_tickvault_ids == ("x",)
    assert report.missing_in_tickvault_ids == ()


def test_missing_and_extra_ids_are_sorted() -> None:
    tardis = [_tardis("2"), _tardis("1"), _tardis("3")]
    tickvault = [_tv("3"), _tv("4"), _tv("0")]
    report = reconcile(tardis, tickvault)
    assert report.matched == 1
    assert report.missing_in_tickvault_ids == ("1", "2")
    assert report.extra_in_tickvault_ids == ("0", "4")


def test_side_mismatch_is_a_subset_of_matched() -> None:
    tardis = [_tardis("1"), _tardis("2", side="sell"), _tardis("3")]
    tickvault = [_tv("1"), _tv("2", side="buy")]
    report = reconcile(tardis, tickvault)
    assert report.matched == 2
    assert report.side_mismatch_ids == ("2",)
    assert report.price_mismatch_ids == ()
    assert report.size_mismatch_ids == ()
    assert report.missing_in_tickvault_ids == ("3",)


def test_price_and_size_compare_exactly_as_decimals() -> None:
    tardis = [_tardis("1", price="100.50", amount="1.0"), _tardis("2", price="1", amount="5e-8")]
    tickvault = [
        _tv("1", price="100.5", size="1.00"),
        _tv("2", price="1.000000000000000001", size="0.00000005"),
    ]
    report = reconcile(tardis, tickvault)
    assert report.matched == 2
    assert report.size_mismatch_ids == ()
    assert report.price_mismatch_ids == ("2",)


def test_load_tardis_trades_reads_exponent_amounts(tmp_path: Path) -> None:
    path = tmp_path / "trades.csv.gz"
    _write_gzip_csv(
        path,
        [
            "coinbase,BTC-USD,1790856000515155,1790856000555341,1100888818,buy,83882.03,5e-8",
            "coinbase,BTC-USD,1790856000515156,1790856000555342,1100888819,sell,8.5e4,0.00000005",
        ],
    )
    trades = load_tardis_trades(path)
    assert len(trades) == 2
    first, second = trades
    assert first.exchange_ts_ns == 1790856000515155000
    assert first.local_ts_ns == 1790856000555341000
    assert first.trade_id == "1100888818"
    assert first.side == "buy"
    assert first.price == Decimal("83882.03")
    assert first.amount == Decimal("5e-8") == Decimal("0.00000005")
    assert second.side == "sell"
    assert second.price == Decimal("85000")


def test_load_tardis_trades_reads_plain_csv(tmp_path: Path) -> None:
    path = tmp_path / "trades.csv"
    path.write_text(
        _csv_bytes(
            ["coinbase,BTC-USD,1790856000515155,1790856000555341,1,buy,100.0,0.5"]
        ),
        encoding="utf-8",
    )
    (trade,) = load_tardis_trades(path)
    assert trade.trade_id == "1"
    assert trade.amount == Decimal("0.5")


def test_load_tardis_trades_rejects_wrong_header(tmp_path: Path) -> None:
    path = tmp_path / "trades.csv"
    path.write_text("exchange,symbol,timestamp,price\n", encoding="utf-8")
    with pytest.raises(ValueError, match="unexpected Tardis header"):
        load_tardis_trades(path)


def test_load_tardis_trades_rejects_bad_side_and_price(tmp_path: Path) -> None:
    bad_side = tmp_path / "side.csv"
    bad_side.write_text(
        _csv_bytes(["coinbase,BTC-USD,1,2,1,BUY,100.0,0.5"]), encoding="utf-8"
    )
    with pytest.raises(ValueError, match="unexpected side"):
        load_tardis_trades(bad_side)

    bad_price = tmp_path / "price.csv"
    bad_price.write_text(
        _csv_bytes(["coinbase,BTC-USD,1,2,1,buy,abc,0.5"]), encoding="utf-8"
    )
    with pytest.raises(ValueError, match="invalid decimal"):
        load_tardis_trades(bad_price)


def test_load_tickvault_trades_time_bounds(
    tv_trades_sample_root: Path,
) -> None:
    first_ns = 1790856000515155000
    everything = load_tickvault_trades(tv_trades_sample_root, "coinbase", "BTC-USD")
    assert len(everything) == 2087
    assert everything[0].exchange_ts_ns == first_ns

    same_moment = load_tickvault_trades(
        tv_trades_sample_root, "coinbase", "BTC-USD", start_ns=first_ns, end_ns=first_ns
    )
    assert len(same_moment) == 6

    second = load_tickvault_trades(
        tv_trades_sample_root,
        "coinbase",
        "BTC-USD",
        start_ns=first_ns + 1_000_000_000,
        end_ns=first_ns + 2_000_000_000,
    )
    assert len(second) == 5
    assert all(trade.price > Decimal("0") for trade in second)


def test_load_tickvault_trades_empty_cases(
    tmp_path: Path, tv_trades_sample_root: Path
) -> None:
    assert load_tickvault_trades(tmp_path, "coinbase", "BTC-USD") == []
    assert load_tickvault_trades(tv_trades_sample_root, "kraken", "BTC-USD") == []


def test_report_to_json_is_deterministic_and_sorted(
    tardis_sample_path: Path,
    tv_trades_sample_root: Path,
    reconcile_expected: dict[str, object],
) -> None:
    expected_window = reconcile_expected["window"]
    assert isinstance(expected_window, dict)
    window = window_from_iso(str(expected_window["start"]), str(expected_window["end"]))
    report = reconcile(
        load_tardis_trades(tardis_sample_path),
        load_tickvault_trades(tv_trades_sample_root, "coinbase", "BTC-USD"),
        window,
    )
    first = report_to_json(report)
    assert first == report_to_json(report)
    payload = json.loads(first)
    assert list(payload) == sorted(payload)
    assert payload["missing_in_tickvault"] == 5
    assert payload["missing_in_tickvault_ids"] == reconcile_expected["missing_in_tickvault_ids"]
    assert payload["price_mismatches"] == 2
    assert payload["side_mismatches"] == 1
    assert payload["window"] == {
        "start": "2026-10-01T12:00:00.000000000Z",
        "end": "2026-10-01T12:10:00.000000000Z",
    }


def test_report_to_text_lists_counts_and_ids() -> None:
    tardis = [_tardis("1"), _tardis("2", price="101.0"), _tardis("3")]
    tickvault = [_tv("1"), _tv("2", price="100.0"), _tv("4")]
    report = reconcile(tardis, tickvault)
    assert report_to_text(report) == (
        "window: 1970-01-01T00:00:01.000000000Z .. 1970-01-01T00:00:01.000000000Z\n"
        "tardis trades: 3\n"
        "tickvault trades: 3\n"
        "matched: 2\n"
        "missing in tickvault: 1 (3)\n"
        "extra in tickvault: 1 (4)\n"
        "price mismatches: 1 (2)\n"
        "size mismatches: 0\n"
        "side mismatches: 0\n"
    )


def test_report_to_text_without_window() -> None:
    text = report_to_text(reconcile([], []))
    assert text.startswith("window: none\n")
    assert "tardis trades: 0\n" in text
