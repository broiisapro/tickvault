"""Tests for the DuckDB views over the normalized tree (issue #28)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

from tickvault.models import EVENTS_SCHEMA, TRADES_SCHEMA, Event, Trade
from tickvault.sql import connect, run_query, view_ddl
from tickvault.storage.parquet_writer import ParquetWriter
from tickvault.timeutil import parse_rfc3339_ns

_HOUR_10 = parse_rfc3339_ns("2026-10-15T10:00:00Z")
_DAY = date(2026, 10, 15)


def _trade(index: int, *, venue: str = "kraken", price: str = "1") -> Trade:
    return Trade(
        venue=venue,
        symbol="BTC-USD",
        exchange_ts_ns=_HOUR_10 + index,
        recv_ts_ns=_HOUR_10 + index,
        connection_id=f"{venue}-20261015T031901Z-0000000a",
        ingest_seq=index,
        trade_id=f"{venue}-{index}",
        price=Decimal(price),
        size=Decimal("2"),
        side="buy",
        sequence=index,
    )


def _write_trades(root: Path) -> None:
    writer = ParquetWriter(root, max_rows=10)
    writer.write(
        "trades",
        [_trade(index, venue="kraken") for index in range(3)]
        + [_trade(index, venue="coinbase") for index in range(2)],
    )
    writer.close()


def test_group_by_venue_over_a_written_tree(tmp_path: Path) -> None:
    root = tmp_path / "data"
    _write_trades(root)

    columns, rows = run_query(root, "SELECT venue, count(*) FROM trades GROUP BY 1 ORDER BY 1")

    assert columns[0] == "venue"
    assert rows == [("coinbase", 2), ("kraken", 3)]


def test_views_expose_all_columns_and_typed_partitions(tmp_path: Path) -> None:
    root = tmp_path / "data"
    _write_trades(root)

    columns, rows = run_query(root, "SELECT * FROM trades")

    assert columns == [field.name for field in TRADES_SCHEMA] + ["date", "hour"]
    assert rows[0][-2:] == (_DAY, 10)
    columns, rows = run_query(
        root, "SELECT date, hour, typeof(date), typeof(hour) FROM trades LIMIT 1"
    )
    assert rows == [(_DAY, 10, "DATE", "INTEGER")]
    columns, rows = run_query(root, "SELECT venue FROM trades WHERE trade_id = 'kraken-0'")
    assert rows == [("kraken",)]


def test_empty_tables_get_typed_zero_row_views(tmp_path: Path) -> None:
    root = tmp_path / "data"

    columns, rows = run_query(root, "SELECT * FROM events")

    assert columns == [field.name for field in EVENTS_SCHEMA] + ["date", "hour"]
    assert rows == []
    assert run_query(root, "SELECT count(*) FROM book_deltas")[1] == [(0,)]
    assert run_query(root, "SELECT count(*) FROM book_snapshots")[1] == [(0,)]


def test_mixed_populated_and_empty_tables(tmp_path: Path) -> None:
    root = tmp_path / "data"
    _write_trades(root)

    assert run_query(root, "SELECT count(*) FROM trades")[1] == [(5,)]
    assert run_query(root, "SELECT count(*) FROM events")[1] == [(0,)]
    writer = ParquetWriter(root, max_rows=10)
    writer.write(
        "events",
        [
            Event(
                venue="kraken",
                recv_ts_ns=_HOUR_10,
                connection_id="kraken-20261015T031901Z-0000000a",
                ingest_seq=0,
                event_type="gap",
                symbol="BTC-USD",
                detail='{"reason":"gap"}',
            )
        ],
    )
    writer.close()
    assert run_query(root, "SELECT event_type FROM events")[1] == [("gap",)]


def test_decimals_come_back_exact(tmp_path: Path) -> None:
    root = tmp_path / "data"
    writer = ParquetWriter(root, max_rows=10)
    writer.write("trades", [_trade(0, price="0.00006120")])
    writer.close()

    _, rows = run_query(root, "SELECT price, size FROM trades")

    assert rows == [(Decimal("0.00006120"), Decimal("2"))]
    assert str(rows[0][0]) == "0.000061200000000000"


def test_view_ddl_is_deterministic_and_state_dependent(tmp_path: Path) -> None:
    root = tmp_path / "data"
    empty_ddl = view_ddl(root)
    assert view_ddl(root) == empty_ddl
    assert empty_ddl.count("CREATE VIEW") == 4
    assert "read_parquet(" not in empty_ddl
    assert "WHERE FALSE" in empty_ddl

    _write_trades(root)
    populated_ddl = view_ddl(root)
    assert view_ddl(root) == populated_ddl
    assert populated_ddl.count("CREATE VIEW") == 4
    assert populated_ddl.count("read_parquet(") == 1
    assert "norm/trades/**/*.parquet" in populated_ddl
    assert "hive_partitioning = true" in populated_ddl


def test_connect_creates_all_four_views(tmp_path: Path) -> None:
    root = tmp_path / "data"
    _write_trades(root)
    connection = connect(root)
    try:
        names = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        assert names == {"trades", "book_deltas", "book_snapshots", "events"}
    finally:
        connection.close()


def test_root_path_with_quotes_is_escaped(tmp_path: Path) -> None:
    root = tmp_path / "it's data"
    _write_trades(root)
    assert run_query(root, "SELECT count(*) FROM trades")[1] == [(5,)]
