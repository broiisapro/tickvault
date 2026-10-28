from decimal import Decimal

import pyarrow as pa

from tickvault.models import (
    BOOK_DELTAS_SCHEMA,
    BOOK_SNAPSHOTS_SCHEMA,
    EVENTS_SCHEMA,
    FEATURES_SCHEMA,
    FEATURES_TABLE,
    TABLES,
    TRADES_SCHEMA,
    BookDelta,
    BookSnapshotLevel,
    Event,
    Trade,
    to_arrow,
)


def test_tables_map_to_the_four_schemas() -> None:
    assert set(TABLES) == {"trades", "book_deltas", "book_snapshots", "events"}
    assert TABLES["trades"] is TRADES_SCHEMA
    assert TABLES["book_deltas"] is BOOK_DELTAS_SCHEMA
    assert TABLES["book_snapshots"] is BOOK_SNAPSHOTS_SCHEMA
    assert TABLES["events"] is EVENTS_SCHEMA


def test_trades_schema_fields() -> None:
    assert [field.name for field in TRADES_SCHEMA] == [
        "venue",
        "symbol",
        "exchange_ts_ns",
        "recv_ts_ns",
        "connection_id",
        "ingest_seq",
        "trade_id",
        "price",
        "size",
        "side",
        "sequence",
    ]
    assert TRADES_SCHEMA.field("price").type == pa.decimal128(38, 18)
    assert TRADES_SCHEMA.field("size").type == pa.decimal128(38, 18)
    assert TRADES_SCHEMA.field("exchange_ts_ns").type == pa.int64()
    assert TRADES_SCHEMA.field("recv_ts_ns").type == pa.int64()
    assert TRADES_SCHEMA.field("ingest_seq").type == pa.int64()
    assert TRADES_SCHEMA.field("sequence").type == pa.int64()
    assert TRADES_SCHEMA.field("sequence").nullable is True


def test_book_deltas_and_snapshots_schema_fields() -> None:
    assert [field.name for field in BOOK_DELTAS_SCHEMA] == [
        "venue",
        "symbol",
        "exchange_ts_ns",
        "recv_ts_ns",
        "connection_id",
        "ingest_seq",
        "side",
        "price",
        "size",
        "sequence",
    ]
    assert BOOK_DELTAS_SCHEMA.field("price").type == pa.decimal128(38, 18)
    assert BOOK_SNAPSHOTS_SCHEMA.field("level").type == pa.int16()
    assert BOOK_SNAPSHOTS_SCHEMA.field("snapshot_id").type == pa.string()
    assert BOOK_SNAPSHOTS_SCHEMA.field("source").type == pa.string()


def test_events_schema_nullable_columns() -> None:
    assert [field.name for field in EVENTS_SCHEMA] == [
        "venue",
        "symbol",
        "exchange_ts_ns",
        "recv_ts_ns",
        "connection_id",
        "ingest_seq",
        "event_type",
        "channel",
        "expected_seq",
        "received_seq",
        "detail",
    ]
    assert EVENTS_SCHEMA.field("symbol").nullable is True
    assert EVENTS_SCHEMA.field("exchange_ts_ns").nullable is True
    assert EVENTS_SCHEMA.field("channel").nullable is True
    assert EVENTS_SCHEMA.field("expected_seq").nullable is True
    assert EVENTS_SCHEMA.field("received_seq").nullable is True
    assert EVENTS_SCHEMA.field("detail").type == pa.string()


def test_to_arrow_roundtrip_trades_by_name() -> None:
    trade = Trade(
        venue="coinbase",
        symbol="BTC-USD",
        exchange_ts_ns=10,
        recv_ts_ns=12,
        connection_id="coinbase-20261015T031901Z-00000001",
        ingest_seq=0,
        trade_id="t-1",
        price=Decimal("81203.0"),
        size=Decimal("0.25"),
        side="buy",
    )
    table = to_arrow("trades", [trade])
    assert table.schema == TRADES_SCHEMA
    assert table.to_pylist() == [
        {
            "venue": "coinbase",
            "symbol": "BTC-USD",
            "exchange_ts_ns": 10,
            "recv_ts_ns": 12,
            "connection_id": "coinbase-20261015T031901Z-00000001",
            "ingest_seq": 0,
            "trade_id": "t-1",
            "price": Decimal("81203.0"),
            "size": Decimal("0.25"),
            "side": "buy",
            "sequence": None,
        }
    ]


def test_to_arrow_roundtrip_by_schema_object() -> None:
    delta = BookDelta(
        venue="kraken",
        symbol="BTC-USD",
        exchange_ts_ns=20,
        recv_ts_ns=20,
        connection_id="kraken-20261015T031901Z-00000002",
        ingest_seq=3,
        side="bid",
        price=Decimal("81200.1"),
        size=Decimal("0"),
        sequence=None,
    )
    table = to_arrow(BOOK_DELTAS_SCHEMA, [delta])
    assert table.schema == BOOK_DELTAS_SCHEMA
    assert table.to_pylist()[0]["size"] == Decimal("0")


def test_to_arrow_events_with_nulls() -> None:
    event = Event(
        venue="coinbase",
        recv_ts_ns=30,
        connection_id="coinbase-20261015T031901Z-00000001",
        ingest_seq=4,
        event_type="gap",
        expected_seq=3,
        received_seq=4,
        detail='{"channel": "level2"}',
    )
    table = to_arrow("events", [event])
    assert table.schema == EVENTS_SCHEMA
    row = table.to_pylist()[0]
    assert row["symbol"] is None
    assert row["exchange_ts_ns"] is None
    assert row["expected_seq"] == 3
    assert row["detail"] == '{"channel": "level2"}'


def test_to_arrow_snapshot_levels_preserve_row_order() -> None:
    levels = [
        BookSnapshotLevel(
            venue="kraken",
            symbol="BTC-USD",
            exchange_ts_ns=100,
            recv_ts_ns=101,
            connection_id="kraken-20261015T031901Z-00000002",
            ingest_seq=5,
            snapshot_id="kraken-20261015T031901Z-00000002:5",
            source="venue",
            side="ask",
            level=level,
            price=Decimal("81204.5") + level,
            size=Decimal("1.5"),
        )
        for level in range(3)
    ]
    table = to_arrow("book_snapshots", levels)
    assert table.column("level").to_pylist() == [0, 1, 2]
    assert table.column("source").to_pylist() == ["venue", "venue", "venue"]


def test_decimal_trailing_zeros_survive_arrow() -> None:
    trade = Trade(
        venue="kraken",
        symbol="BTC-USD",
        exchange_ts_ns=1,
        recv_ts_ns=2,
        connection_id="kraken-20261015T031901Z-00000002",
        ingest_seq=0,
        trade_id="t-2",
        price=Decimal("0.00006120"),
        size=Decimal("81203.0"),
        side="sell",
    )
    table = to_arrow("trades", [trade])
    row = table.to_pylist()[0]
    assert row["price"] == Decimal("0.00006120")
    assert row["size"] == Decimal("81203.0")
    assert row["price"].as_tuple().exponent == -18
    assert str(row["price"]) == "0.000061200000000000"
    assert str(row["size"]).startswith("81203.0")


def test_features_schema_columns_in_order() -> None:
    assert FEATURES_TABLE == "features"
    assert [field.name for field in FEATURES_SCHEMA] == [
        "venue",
        "symbol",
        "resolution",
        "bar_start_ns",
        "bar_end_ns",
        "best_bid",
        "best_ask",
        "mid",
        "spread_bps",
        "depth_imbalance_5",
        "book_sample_ts_ns",
        "trade_count",
        "volume",
        "signed_volume",
        "vwap",
        "realized_vol",
    ]


def test_features_schema_types() -> None:
    string_columns = {"venue", "symbol", "resolution"}
    int_columns = {"bar_start_ns", "bar_end_ns", "book_sample_ts_ns", "trade_count"}
    float_columns = {
        "best_bid",
        "best_ask",
        "mid",
        "spread_bps",
        "depth_imbalance_5",
        "volume",
        "signed_volume",
        "vwap",
        "realized_vol",
    }
    for field in FEATURES_SCHEMA:
        if field.name in string_columns:
            expected = pa.string()
        elif field.name in int_columns:
            expected = pa.int64()
        else:
            assert field.name in float_columns
            expected = pa.float64()
        assert field.type == expected, field.name
    assert {field.name for field in FEATURES_SCHEMA if field.nullable is False} == {
        "venue",
        "symbol",
        "resolution",
        "bar_start_ns",
        "bar_end_ns",
        "trade_count",
        "volume",
        "signed_volume",
    }


def test_features_schema_is_not_a_normalized_table() -> None:
    assert "features" not in TABLES
    assert set(TABLES) == {"trades", "book_deltas", "book_snapshots", "events"}
