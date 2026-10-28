"""Feature computation tests: hand-built data roots with known answers (issue #118).

The fixture below is a thirteen-second session built from explicit rows, so
every column of every bar has an answer that can be worked out on paper: a
sample exactly at a bar boundary, a trade exactly at a bar boundary, a stale
sample, a one-sided book, a bar without trades and a ``venue``-source snapshot
that must never be used as book state.
"""

from __future__ import annotations

import math
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import cast

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tickvault.features import MAX_SAMPLE_AGE_NS, compute_features, features_path, write_features
from tickvault.models import (
    FEATURES_SCHEMA,
    BookSnapshotLevel,
    Side,
    SnapshotSource,
    Trade,
    TradeSide,
)
from tickvault.storage.parquet_writer import ParquetWriter

_SECOND = 1_000_000_000
_BASE_NS = 1_767_225_600 * _SECOND
"""2026-01-01T00:00:00Z, the start of the fixture's trading day."""

_DAY = "2026-01-01"
_NEXT_DAY_NS = _BASE_NS + 86_400 * _SECOND
_VENUE = "testvenue"
_SYMBOL = "BTC-USD"

type _Levels = tuple[tuple[str, str], ...]
"""``(price, size)`` pairs, outermost level first."""

# Book samples for _VENUE/_SYMBOL, by offset from midnight. Levels 5 are huge
# outliers that would dominate the imbalance if they leaked into the L0-4 sum.
_SAMPLES: tuple[tuple[int, _Levels, _Levels], ...] = (
    (
        200_000_000,
        (("100.00", "2"), ("99.90", "1"), ("99.80", "1"), ("99.70", "1"), ("99.60", "1")),
        (("101.00", "5"), ("101.10", "1"), ("101.20", "1"), ("101.30", "1"), ("101.40", "1")),
    ),
    (
        1_200_000_000,
        (("100.00", "2"), ("99.90", "2"), ("99.80", "1"), ("99.70", "1"), ("99.60", "1")),
        (("102.00", "3"), ("102.10", "1"), ("102.20", "1"), ("102.30", "1"), ("102.40", "1")),
    ),
    (
        3_200_000_000,
        (("99.00", "4"), ("98.90", "1"), ("98.80", "1"), ("98.70", "1"), ("98.60", "1")),
        (("101.00", "4"), ("101.10", "1"), ("101.20", "1"), ("101.30", "1"), ("101.40", "1")),
    ),
    (
        5_000_000_000,
        (("98.00", "1"), ("97.90", "1"), ("97.80", "1"), ("97.70", "1"), ("97.60", "1")),
        (("100.00", "2"), ("100.10", "1"), ("100.20", "1"), ("100.30", "1"), ("100.40", "1")),
    ),
    (
        12_500_000_000,
        (("97.00", "3"), ("96.90", "1"), ("96.80", "1"), ("96.70", "1"), ("96.60", "1")),
        (),
    ),
)

# (offset, price, size, taker side)
_TRADES: tuple[tuple[int, str, str, str], ...] = (
    (500_000_000, "100.00", "1", "buy"),
    (1_500_000_000, "102.00", "3", "sell"),
    (4_000_000_000, "99.00", "2", "buy"),
    (5_000_000_000, "98.00", "4", "sell"),
    (6_500_000_000, "100.50", "0.5", "buy"),
)

# A second venue with trades only: no book samples at all.
_TRADES_ONLY_VENUE = "a-venue"
_TRADES_ONLY_SYMBOL = "ETH-USD"
_TRADES_ONLY: tuple[tuple[int, str, str, str], ...] = (
    (3_500_000_000, "50.00", "2", "buy"),
    (6_500_000_000, "51.00", "1", "sell"),
)

_SPREAD_FIRST_BAR_BPS = 1 / 100.5 * 10_000


def _snapshot_levels(
    venue: str,
    symbol: str,
    exchange_ts_ns: int,
    bids: _Levels,
    asks: _Levels,
    *,
    source: str = "sample",
    ingest_seq: int = 0,
    snapshot_id: str = "",
) -> Iterator[BookSnapshotLevel]:
    """Yield one row per level of one snapshot, both sides."""
    for side, levels in (("bid", bids), ("ask", asks)):
        yield from _level_rows(
            cast(Side, side),
            levels,
            venue=venue,
            symbol=symbol,
            exchange_ts_ns=exchange_ts_ns,
            source=cast(SnapshotSource, source),
            ingest_seq=ingest_seq,
            snapshot_id=snapshot_id or f"{venue}-snap-{exchange_ts_ns}",
        )


def _level_rows(
    side: Side,
    levels: _Levels,
    *,
    venue: str,
    symbol: str,
    exchange_ts_ns: int,
    source: SnapshotSource,
    ingest_seq: int,
    snapshot_id: str,
) -> Iterator[BookSnapshotLevel]:
    for level, (price, size) in enumerate(levels):
        yield BookSnapshotLevel(
            venue=venue,
            symbol=symbol,
            exchange_ts_ns=exchange_ts_ns,
            recv_ts_ns=exchange_ts_ns,
            connection_id=f"{venue}-conn",
            ingest_seq=ingest_seq,
            snapshot_id=snapshot_id,
            source=source,
            side=side,
            level=level,
            price=Decimal(price),
            size=Decimal(size),
        )


def _trade_row(
    venue: str,
    symbol: str,
    exchange_ts_ns: int,
    price: str,
    size: str,
    side: str,
    ingest_seq: int,
) -> Trade:
    return Trade(
        venue=venue,
        symbol=symbol,
        exchange_ts_ns=exchange_ts_ns,
        recv_ts_ns=exchange_ts_ns,
        connection_id=f"{venue}-conn",
        ingest_seq=ingest_seq,
        trade_id=f"trade-{ingest_seq}",
        price=Decimal(price),
        size=Decimal(size),
        side=cast(TradeSide, side),
    )


def _write_fixture(root: Path) -> None:
    """Write the normalized rows of the hand-built session into ``root``."""
    snapshots: list[BookSnapshotLevel] = []
    for index, (offset, bids, asks) in enumerate(_SAMPLES):
        snapshots.extend(
            _snapshot_levels(_VENUE, _SYMBOL, _BASE_NS + offset, bids, asks, ingest_seq=index)
        )
    # A venue-sent snapshot in the middle of the session: source='sample' is
    # the contract for book state, so this row must never be used.
    snapshots.extend(
        _snapshot_levels(
            _VENUE,
            _SYMBOL,
            _BASE_NS + 2_600_000_000,
            (("1.00", "10"),),
            (("1000.00", "10"),),
            source="venue",
            ingest_seq=100,
            snapshot_id="venue-snapshot",
        )
    )
    trades: list[Trade] = []
    for index, (offset, price, size, side) in enumerate(_TRADES):
        trades.append(_trade_row(_VENUE, _SYMBOL, _BASE_NS + offset, price, size, side, index))
    for index, (offset, price, size, side) in enumerate(_TRADES_ONLY):
        trades.append(
            _trade_row(
                _TRADES_ONLY_VENUE,
                _TRADES_ONLY_SYMBOL,
                _BASE_NS + offset,
                price,
                size,
                side,
                100 + index,
            )
        )
    # Next-day rows, in their own exchange-time partition: a computation for
    # _DAY must not open them.
    trades.append(_trade_row(_VENUE, _SYMBOL, _NEXT_DAY_NS + _SECOND, "1.00", "1000", "buy", 200))
    snapshots.extend(
        _snapshot_levels(
            _VENUE,
            _SYMBOL,
            _NEXT_DAY_NS + _SECOND,
            (("1.00", "10"),),
            (("2.00", "10"),),
            ingest_seq=201,
        )
    )

    writer = ParquetWriter(root)
    writer.write("book_snapshots", snapshots)
    writer.write("trades", trades)
    writer.close()


@pytest.fixture(scope="module")
def tables(tmp_path_factory: pytest.TempPathFactory) -> dict[str, pa.Table]:
    root = tmp_path_factory.mktemp("features-fixture")
    _write_fixture(root)
    return compute_features(root, _DAY)


def _bar(
    table: pa.Table,
    venue: str,
    symbol: str,
    bar_start_ns: int,
) -> dict[str, object]:
    """The one row for ``(venue, symbol, bar_start_ns)``."""
    rows = [
        row
        for row in table.to_pylist()
        if row["venue"] == venue and row["symbol"] == symbol and row["bar_start_ns"] == bar_start_ns
    ]
    assert len(rows) == 1, f"expected one bar at {bar_start_ns}, got {len(rows)}"
    return rows[0]


def test_tables_use_the_frozen_schema_and_cover_both_resolutions(
    tables: dict[str, pa.Table],
) -> None:
    assert set(tables) == {"1s", "1m"}
    for table in tables.values():
        assert table.schema == FEATURES_SCHEMA


def test_one_second_bars_span_the_session_and_end_at_a_bar_boundary(
    tables: dict[str, pa.Table],
) -> None:
    bars = tables["1s"]
    starts = [
        row["bar_start_ns"]
        for row in bars.to_pylist()
        if (row["venue"], row["symbol"]) == (_VENUE, _SYMBOL)
    ]
    # The first sample is at 0.2 s and the last at 12.5 s, so the bars run from
    # [0,1) to [12,13): thirteen bars, none of them empty padding.
    assert starts == [_BASE_NS + index * _SECOND for index in range(13)]
    first = _bar(bars, _VENUE, _SYMBOL, _BASE_NS)
    assert first["bar_end_ns"] == _BASE_NS + _SECOND


def test_rows_are_sorted_by_venue_symbol_and_bar_start(tables: dict[str, pa.Table]) -> None:
    for table in tables.values():
        keys = [
            (row["venue"], row["symbol"], row["bar_start_ns"], row["bar_end_ns"])
            for row in table.to_pylist()
        ]
        assert keys == sorted(keys)


def test_first_bar_book_state_and_a_buy_taker_trade(tables: dict[str, pa.Table]) -> None:
    row = _bar(tables["1s"], _VENUE, _SYMBOL, _BASE_NS)
    assert row["resolution"] == "1s"
    assert row["bar_end_ns"] == _BASE_NS + _SECOND
    assert row["best_bid"] == pytest.approx(100.0)
    assert row["best_ask"] == pytest.approx(101.0)
    assert row["mid"] == pytest.approx(100.5)
    assert row["spread_bps"] == pytest.approx(_SPREAD_FIRST_BAR_BPS)
    assert row["depth_imbalance_5"] == pytest.approx((6 - 9) / 15)  # level 5 excluded
    assert row["book_sample_ts_ns"] == _BASE_NS + 200_000_000
    assert row["trade_count"] == 1
    assert row["volume"] == pytest.approx(1.0)
    assert row["signed_volume"] == pytest.approx(1.0)
    assert row["vwap"] == pytest.approx(100.0)
    # There is no sample before bar_start, so the one return has no endpoint.
    assert row["realized_vol"] is None


def test_second_bar_realized_vol_is_one_whole_second_return(
    tables: dict[str, pa.Table],
) -> None:
    row = _bar(tables["1s"], _VENUE, _SYMBOL, _BASE_NS + _SECOND)
    assert row["book_sample_ts_ns"] == _BASE_NS + 1_200_000_000
    assert row["depth_imbalance_5"] == pytest.approx(0.0)
    assert row["trade_count"] == 1
    assert row["signed_volume"] == pytest.approx(-3.0)  # sell taker
    assert row["vwap"] == pytest.approx(102.0)
    # m(1 s) is the 0.2 s sample (mid 100.5), m(2 s) the 1.2 s sample (101.0).
    assert row["realized_vol"] == pytest.approx(math.log(101.0 / 100.5))


def test_venue_source_snapshot_is_not_used_as_book_state(tables: dict[str, pa.Table]) -> None:
    row = _bar(tables["1s"], _VENUE, _SYMBOL, _BASE_NS + 2 * _SECOND)
    assert row["book_sample_ts_ns"] == _BASE_NS + 1_200_000_000
    assert row["best_bid"] == pytest.approx(100.0)
    assert row["best_ask"] == pytest.approx(102.0)
    assert row["realized_vol"] == pytest.approx(0.0)


def test_sample_exactly_at_bar_end_belongs_to_the_next_bar(
    tables: dict[str, pa.Table],
) -> None:
    row = _bar(tables["1s"], _VENUE, _SYMBOL, _BASE_NS + 4 * _SECOND)
    # The 5.0 s sample is not used: book state comes from 3.2 s.
    assert row["book_sample_ts_ns"] == _BASE_NS + 3_200_000_000
    assert row["mid"] == pytest.approx(100.0)
    # The 5.0 s trade is not counted either.
    assert row["trade_count"] == 1
    assert row["vwap"] == pytest.approx(99.0)
    assert row["realized_vol"] == pytest.approx(0.0)


def test_trade_exactly_at_bar_start_belongs_to_that_bar(tables: dict[str, pa.Table]) -> None:
    row = _bar(tables["1s"], _VENUE, _SYMBOL, _BASE_NS + 5 * _SECOND)
    assert row["book_sample_ts_ns"] == _BASE_NS + 5_000_000_000
    assert row["mid"] == pytest.approx(99.0)
    assert row["trade_count"] == 1
    assert row["vwap"] == pytest.approx(98.0)
    assert row["signed_volume"] == pytest.approx(-4.0)
    assert row["volume"] == pytest.approx(4.0)
    # m(5 s) is still the 3.2 s sample: the 5.0 s one is not strictly earlier.
    # r = ln(m(6 s) / m(5 s)) = ln(99 / 100), and realized_vol carries |r|.
    assert row["realized_vol"] == pytest.approx(abs(math.log(99.0 / 100.0)))


def test_bar_without_trades_has_zero_counts_and_a_null_vwap(
    tables: dict[str, pa.Table],
) -> None:
    row = _bar(tables["1s"], _VENUE, _SYMBOL, _BASE_NS + 7 * _SECOND)
    assert row["trade_count"] == 0
    assert row["volume"] == 0.0
    assert row["signed_volume"] == 0.0
    assert row["vwap"] is None
    assert row["mid"] == pytest.approx(99.0)


def test_a_sample_exactly_five_seconds_old_is_still_fresh(
    tables: dict[str, pa.Table],
) -> None:
    row = _bar(tables["1s"], _VENUE, _SYMBOL, _BASE_NS + 9 * _SECOND)
    assert row["book_sample_ts_ns"] == _BASE_NS + 5_000_000_000
    assert row["mid"] == pytest.approx(99.0)
    assert row["realized_vol"] == pytest.approx(0.0)


def test_a_sample_older_than_the_limit_leaves_book_columns_null(
    tables: dict[str, pa.Table],
) -> None:
    row = _bar(tables["1s"], _VENUE, _SYMBOL, _BASE_NS + 10 * _SECOND)
    bar_end_ns = row["bar_end_ns"]
    assert isinstance(bar_end_ns, int)
    age_ns = bar_end_ns - (_BASE_NS + 5 * _SECOND)
    assert age_ns > MAX_SAMPLE_AGE_NS
    for column in (
        "best_bid",
        "best_ask",
        "mid",
        "spread_bps",
        "depth_imbalance_5",
        "book_sample_ts_ns",
        "realized_vol",
    ):
        assert row[column] is None, column


def test_one_sided_book_keeps_the_touch_and_nulls_the_cross_side(
    tables: dict[str, pa.Table],
) -> None:
    row = _bar(tables["1s"], _VENUE, _SYMBOL, _BASE_NS + 12 * _SECOND)
    assert row["book_sample_ts_ns"] == _BASE_NS + 12_500_000_000
    assert row["best_bid"] == pytest.approx(97.0)
    assert row["best_ask"] is None
    assert row["mid"] is None
    assert row["spread_bps"] is None
    assert row["depth_imbalance_5"] == pytest.approx(1.0)


def test_a_venue_with_trades_only_has_null_book_columns(tables: dict[str, pa.Table]) -> None:
    rows = [
        row
        for row in tables["1s"].to_pylist()
        if (row["venue"], row["symbol"]) == (_TRADES_ONLY_VENUE, _TRADES_ONLY_SYMBOL)
    ]
    # Bars cover the whole span between the first and the last trade, so the
    # quiet seconds in between are present with zero counts.
    assert [row["bar_start_ns"] for row in rows] == [
        _BASE_NS + offset * _SECOND for offset in range(3, 7)
    ]
    assert [row["trade_count"] for row in rows] == [1, 0, 0, 1]
    assert [row["vwap"] for row in rows] == [
        pytest.approx(50.0),
        None,
        None,
        pytest.approx(51.0),
    ]
    assert [row["signed_volume"] for row in rows] == [
        pytest.approx(2.0),
        0.0,
        0.0,
        pytest.approx(-1.0),
    ]
    for row in rows:
        for column in ("best_bid", "best_ask", "mid", "spread_bps", "book_sample_ts_ns"):
            assert row[column] is None, column
        assert row["realized_vol"] is None


def test_one_minute_bar_aggregates_seconds_and_whole_second_returns(
    tables: dict[str, pa.Table],
) -> None:
    row = _bar(tables["1m"], _VENUE, _SYMBOL, _BASE_NS)
    assert row["bar_end_ns"] == _BASE_NS + 60 * _SECOND
    # The last sample (12.5 s) is 47.5 s older than the close: past the 5 s
    # staleness limit, so a one-minute bar over this session has no book state.
    assert row["book_sample_ts_ns"] is None
    assert row["best_bid"] is None
    assert row["best_ask"] is None
    assert row["mid"] is None
    assert row["depth_imbalance_5"] is None
    assert row["trade_count"] == 5
    assert row["volume"] == pytest.approx(10.5)
    assert row["signed_volume"] == pytest.approx(-3.5)
    assert row["vwap"] == pytest.approx(
        (100.0 * 1 + 102.0 * 3 + 99.0 * 2 + 98.0 * 4 + 100.5 * 0.5) / 10.5
    )
    # m(1 s)=100.5, m(2 s)=101, m(3 s)=101, m(4 s)=100, m(5 s)=100, m(6 s)=99;
    # the 1 s return is skipped (no sample before the bar) and the stale tail
    # after 10 s contributes nothing.
    expected = math.sqrt(
        math.log(101.0 / 100.5) ** 2 + math.log(100.0 / 101.0) ** 2 + math.log(99.0 / 100.0) ** 2
    )
    assert row["realized_vol"] == pytest.approx(expected)


def test_one_minute_bar_of_a_trades_only_venue(tables: dict[str, pa.Table]) -> None:
    row = _bar(tables["1m"], _TRADES_ONLY_VENUE, _TRADES_ONLY_SYMBOL, _BASE_NS)
    assert row["trade_count"] == 2
    assert row["volume"] == pytest.approx(3.0)
    assert row["signed_volume"] == pytest.approx(1.0)
    assert row["vwap"] == pytest.approx(151.0 / 3)
    assert row["best_bid"] is None
    assert row["realized_vol"] is None


def test_other_days_are_never_read(tables: dict[str, pa.Table]) -> None:
    starts = {row["bar_start_ns"] for row in tables["1s"].to_pylist()}
    assert max(starts) == _BASE_NS + 12 * _SECOND
    assert all(start < _NEXT_DAY_NS for start in starts)


def test_requested_resolutions_only(
    tmp_path: Path,
) -> None:
    _write_fixture(tmp_path)
    only_seconds = compute_features(tmp_path, _DAY, ("1s",))
    assert set(only_seconds) == {"1s"}
    minutes = compute_features(tmp_path, _DAY, ("1m",))
    assert {row["resolution"] for row in minutes["1m"].to_pylist()} == {"1m"}


def test_an_empty_data_root_yields_typed_empty_tables(tmp_path: Path) -> None:
    tables = compute_features(tmp_path / "missing", _DAY)
    assert set(tables) == {"1s", "1m"}
    for table in tables.values():
        assert table.num_rows == 0
        assert table.schema == FEATURES_SCHEMA
    assert write_features(tables, tmp_path / "out", _DAY) == []


def test_invalid_arguments_are_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="invalid date"):
        compute_features(tmp_path, "2026-1-1")
    with pytest.raises(ValueError, match="unsupported resolution"):
        compute_features(tmp_path, _DAY, ("5m",))


def test_write_features_creates_one_file_per_symbol_in_the_agreed_layout(
    tables: dict[str, pa.Table], tmp_path: Path
) -> None:
    paths = write_features(tables, tmp_path, _DAY)
    expected = [
        features_path(tmp_path, resolution, venue, _DAY, symbol)
        for resolution in ("1s", "1m")
        for venue, symbol in (
            (_TRADES_ONLY_VENUE, _TRADES_ONLY_SYMBOL),
            (_VENUE, _SYMBOL),
        )
    ]
    assert paths == expected
    assert all(path.is_file() for path in paths)

    for path in paths:
        table = pq.read_table(path)
        assert table.schema == FEATURES_SCHEMA
        rows = table.to_pylist()
        resolution = path.parents[2].name.removeprefix("resolution=")
        venue = path.parents[1].name.removeprefix("venue=")
        symbol = path.name.removeprefix("part-").removesuffix(".parquet")
        assert {row["resolution"] for row in rows} == {resolution}
        assert {row["venue"] for row in rows} == {venue}
        assert {row["symbol"] for row in rows} == {symbol}
        starts = [row["bar_start_ns"] for row in rows]
        assert starts == sorted(starts)


def test_two_runs_write_byte_identical_files(
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    data = tmp_path_factory.mktemp("features-determinism-data")
    _write_fixture(data)
    outputs = []
    for name in ("first", "second"):
        out = tmp_path_factory.mktemp(f"features-determinism-{name}")
        tables = compute_features(data, _DAY)
        write_features(tables, out, _DAY)
        outputs.append(
            {
                path.relative_to(out).as_posix(): path.read_bytes()
                for path in sorted(out.rglob("*.parquet"))
            }
        )
    assert outputs[0].keys() == outputs[1].keys()
    for name, content in outputs[0].items():
        assert content == outputs[1][name], name


def test_write_features_rejects_a_foreign_schema(tmp_path: Path) -> None:
    foreign = pa.table({"venue": ["x"]})
    with pytest.raises(ValueError, match="FEATURES_SCHEMA"):
        write_features({"1s": foreign}, tmp_path, _DAY)


def test_bar_columns_are_exactly_the_schema_columns(tables: dict[str, pa.Table]) -> None:
    expected = [field.name for field in FEATURES_SCHEMA]
    for table in tables.values():
        assert table.column_names == expected
