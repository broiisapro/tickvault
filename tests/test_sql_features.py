"""Tests for the DuckDB features view (issue #119).

The features tree is written directly with :data:`tickvault.models.FEATURES_SCHEMA`
and :func:`tickvault.features.features_path`, so these tests do not depend on
the feature computation or on its writer.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from tickvault.features import RESOLUTIONS, features_path
from tickvault.models import FEATURES_SCHEMA, TRADES_SCHEMA
from tickvault.sql import connect, run_query, view_ddl
from tickvault.timeutil import parse_rfc3339_ns

_DAY = "2026-10-15"
_NEXT_DAY = "2026-10-16"
_DAY_START = parse_rfc3339_ns("2026-10-15T00:00:00Z")


def _bar(
    venue: str,
    symbol: str,
    resolution: str,
    index: int,
) -> dict[str, object]:
    start = _DAY_START + index * RESOLUTIONS[resolution]
    return {
        "venue": venue,
        "symbol": symbol,
        "resolution": resolution,
        "bar_start_ns": start,
        "bar_end_ns": start + RESOLUTIONS[resolution],
        "best_bid": 100.0,
        "best_ask": 100.5,
        "mid": 100.25,
        "spread_bps": 49.9,
        "depth_imbalance_5": 0.1,
        "book_sample_ts_ns": start + 1,
        "trade_count": 2,
        "volume": 1.5,
        "signed_volume": 0.5,
        "vwap": 100.2,
        "realized_vol": 0.001,
    }


def _write_bars(root: Path, bars: list[dict[str, object]]) -> None:
    """Write one Parquet file per (resolution, venue, day, symbol) group."""
    next_day_start = parse_rfc3339_ns("2026-10-16T00:00:00Z")
    groups: dict[tuple[str, str, str, str], list[dict[str, object]]] = {}
    for bar in bars:
        key = (
            str(bar["resolution"]),
            str(bar["venue"]),
            _DAY if bar["bar_start_ns"] < next_day_start else _NEXT_DAY,
            str(bar["symbol"]),
        )
        groups.setdefault(key, []).append(bar)
    for (resolution, venue, day, symbol), rows in groups.items():
        path = features_path(root, resolution, venue, day, symbol)
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(rows, schema=FEATURES_SCHEMA), path)


def _small_tree(root: Path) -> None:
    _write_bars(
        root,
        [
            _bar("kraken", "BTC-USD", "1s", 0),
            _bar("kraken", "BTC-USD", "1s", 1),
            _bar("kraken", "ETH-USD", "1s", 0),
            _bar("coinbase", "BTC-USD", "1m", 0),
            # A second day, so the hive `date` key is not constant.
            _bar("kraken", "BTC-USD", "1s", 86_400),
        ],
    )


def test_features_view_columns_types_and_row_counts(tmp_path: Path) -> None:
    root = tmp_path / "data"
    _small_tree(root)

    columns, rows = run_query(
        root,
        "SELECT * FROM features ORDER BY resolution, venue, symbol, bar_start_ns",
    )

    assert columns == [field.name for field in FEATURES_SCHEMA] + ["date"]
    assert rows[0][-1] == date(2026, 10, 15)
    assert run_query(
        root,
        "SELECT typeof(resolution), typeof(venue), typeof(date) FROM features LIMIT 1",
    )[1] == [("VARCHAR", "VARCHAR", "DATE")]
    assert run_query(
        root,
        "SELECT resolution, count(*) FROM features GROUP BY 1 ORDER BY 1",
    )[1] == [("1m", 1), ("1s", 4)]
    assert run_query(
        root,
        "SELECT venue, symbol, count(*) FROM features WHERE resolution = '1s' "
        "GROUP BY 1, 2 ORDER BY 1, 2",
    )[1] == [("kraken", "BTC-USD", 3), ("kraken", "ETH-USD", 1)]
    assert run_query(
        root,
        "SELECT count(*) FROM features WHERE date = DATE '2026-10-15'",
    )[1] == [(4,)]
    assert run_query(
        root,
        "SELECT count(*) FROM features WHERE date = DATE '2026-10-16'",
    )[1] == [(1,)]


def test_features_ddl_is_appended_and_features_absent_without_tree(tmp_path: Path) -> None:
    root = tmp_path / "data"
    empty_ddl = view_ddl(root)

    assert empty_ddl.count("CREATE VIEW") == 4
    assert "features" not in empty_ddl

    _small_tree(root)
    populated_ddl = view_ddl(root)

    assert populated_ddl.count("CREATE VIEW") == 5
    assert populated_ddl.startswith(empty_ddl), "normalized DDL changed when features appeared"
    assert "features/**/*.parquet" in populated_ddl
    assert (
        "hive_types = {'resolution': 'VARCHAR', 'venue': 'VARCHAR', 'date': 'DATE'}"
        in populated_ddl
    )
    assert view_ddl(root) == populated_ddl


def test_features_only_root_keeps_typed_empty_normalized_views(tmp_path: Path) -> None:
    root = tmp_path / "data"
    _small_tree(root)
    assert not (root / "norm").exists()

    connection = connect(root)
    try:
        names = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        assert names == {"trades", "book_deltas", "book_snapshots", "events", "features"}
        description = connection.execute("SELECT * FROM trades").description or []
        assert [column[0] for column in description] == [
            field.name for field in TRADES_SCHEMA
        ] + ["date", "hour"]
    finally:
        connection.close()

    assert run_query(root, "SELECT count(*) FROM trades")[1] == [(0,)]
    assert run_query(root, "SELECT count(*) FROM book_deltas")[1] == [(0,)]
    assert run_query(root, "SELECT count(*) FROM features")[1] == [(5,)]


def test_features_view_reads_only_the_features_tree(tmp_path: Path) -> None:
    root = tmp_path / "data"
    _write_bars(root, [_bar("kraken", "BTC-USD", "1s", 0)])
    # A Parquet file outside `features/` must not leak into the view.
    stray = root / "norm" / "trades" / "venue=kraken" / f"date={_DAY}" / "hour=00"
    stray.mkdir(parents=True)
    pq.write_table(pa.table({"x": [1]}), stray / "part-x.parquet")

    assert run_query(root, "SELECT count(*) FROM features")[1] == [(1,)]
    assert "features/**/*.parquet" in view_ddl(root)
    assert "norm/trades/**/*.parquet" in view_ddl(root)
