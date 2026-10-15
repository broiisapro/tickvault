"""Tests for :mod:`tickvault.qa.metrics`.

Every test builds a small normalized tree with ``to_arrow`` +
``partition_path`` + ``pyarrow.parquet.write_table`` so the metric queries run
against exactly the layout the writer produces. Counts are asserted with
their specific values so a broken query cannot pass vacuously.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq

from tickvault.models import (
    BookDelta,
    BookSnapshotLevel,
    Event,
    EventType,
    NormalizedRecord,
    SnapshotSource,
    Trade,
    to_arrow,
)
from tickvault.paths import partition_path
from tickvault.qa.metrics import compute_qa

SECOND_NS = 1_000_000_000
MILLISECOND_NS = 1_000_000
DAY = "2026-10-01"
DAY2 = "2026-10-02"


def _ts(day: str = DAY, hour: int = 12, second: int = 0) -> int:
    year, month, day_of_month = (int(part) for part in day.split("-"))
    return int(datetime(year, month, day_of_month, hour, tzinfo=UTC).timestamp()) * SECOND_NS + (
        second * SECOND_NS
    )


def _partition_ts(record: NormalizedRecord) -> int:
    if isinstance(record, Event) and record.exchange_ts_ns is None:
        return record.recv_ts_ns
    assert record.exchange_ts_ns is not None
    return record.exchange_ts_ns


def _write(root: Path, table: str, records: Sequence[NormalizedRecord]) -> None:
    grouped: dict[Path, list[NormalizedRecord]] = {}
    for record in records:
        path = partition_path(root, table, record.venue, _partition_ts(record))
        grouped.setdefault(path, []).append(record)
    for path, group in grouped.items():
        path.mkdir(parents=True, exist_ok=True)
        pq.write_table(to_arrow(table, group), path / "part-test.parquet")


def _trade(
    venue: str = "coinbase",
    symbol: str = "BTC-USD",
    ts_ns: int = _ts(second=1),
    lag_ms: int = 0,
    connection_id: str = "conn-a",
    ingest_seq: int = 0,
    trade_id: str | None = None,
) -> Trade:
    return Trade(
        venue=venue,
        symbol=symbol,
        exchange_ts_ns=ts_ns,
        recv_ts_ns=ts_ns + lag_ms * MILLISECOND_NS,
        connection_id=connection_id,
        ingest_seq=ingest_seq,
        trade_id=trade_id if trade_id is not None else f"{venue}-{ingest_seq}",
        price=Decimal("100.5"),
        size=Decimal("0.25"),
        side="buy",
    )


def _delta(
    venue: str = "coinbase",
    symbol: str = "BTC-USD",
    ts_ns: int = _ts(second=1),
    lag_ms: int = 0,
    connection_id: str = "conn-a",
    ingest_seq: int = 0,
) -> BookDelta:
    return BookDelta(
        venue=venue,
        symbol=symbol,
        exchange_ts_ns=ts_ns,
        recv_ts_ns=ts_ns + lag_ms * MILLISECOND_NS,
        connection_id=connection_id,
        ingest_seq=ingest_seq,
        side="bid",
        price=Decimal("100.0"),
        size=Decimal("1.5"),
    )


def _snapshot(
    venue: str = "coinbase",
    symbol: str = "BTC-USD",
    ts_ns: int = _ts(second=1),
    snapshot_id: str = "conn-a:0",
    source: SnapshotSource = "venue",
    connection_id: str = "conn-a",
    ingest_seq: int = 0,
) -> BookSnapshotLevel:
    return BookSnapshotLevel(
        venue=venue,
        symbol=symbol,
        exchange_ts_ns=ts_ns,
        recv_ts_ns=ts_ns,
        connection_id=connection_id,
        ingest_seq=ingest_seq,
        snapshot_id=snapshot_id,
        source=source,
        side="bid",
        level=0,
        price=Decimal("100.0"),
        size=Decimal("1.0"),
    )


def _event(
    event_type: EventType,
    venue: str = "coinbase",
    symbol: str | None = "BTC-USD",
    ts_ns: int = _ts(second=1),
    connection_id: str = "conn-a",
    ingest_seq: int = 0,
) -> Event:
    return Event(
        venue=venue,
        recv_ts_ns=ts_ns,
        connection_id=connection_id,
        ingest_seq=ingest_seq,
        event_type=event_type,
        symbol=symbol,
        exchange_ts_ns=ts_ns if symbol is not None else None,
    )


def test_empty_root_returns_no_rows(tmp_path: Path) -> None:
    assert compute_qa(tmp_path) == []


def test_empty_table_directories_return_no_rows(tmp_path: Path) -> None:
    for table in ("trades", "book_deltas", "book_snapshots", "events"):
        (tmp_path / "norm" / table / f"venue=coinbase/date={DAY}/hour=12").mkdir(parents=True)
    assert compute_qa(tmp_path) == []


def test_missing_tables_count_as_empty(tmp_path: Path) -> None:
    _write(tmp_path, "trades", [_trade(lag_ms=2)])
    (rows,) = compute_qa(tmp_path)
    assert rows.venue == "coinbase"
    assert rows.symbol == "BTC-USD"
    assert rows.date == DAY
    assert rows.trades == 1
    assert rows.messages == 1
    assert rows.book_deltas == 0
    assert rows.venue_snapshots == 0
    assert (rows.gaps, rows.resyncs, rows.checksum_failures, rows.reconnects) == (0, 0, 0, 0)
    assert rows.invariant_violations == 0
    assert rows.lag_p50_ms == 2.0
    assert rows.lag_p99_ms == 2.0
    assert rows.coverage == 1.0
    assert rows.unexplained == 0


def test_messages_deduplicate_across_tables(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "trades",
        [
            _trade(ts_ns=_ts(second=1), ingest_seq=0),
            _trade(ts_ns=_ts(second=1), ingest_seq=0, trade_id="second-of-message"),
            _trade(ts_ns=_ts(second=2), ingest_seq=1),
        ],
    )
    _write(tmp_path, "book_deltas", [_delta(ts_ns=_ts(second=1), ingest_seq=0)])
    _write(tmp_path, "book_snapshots", [_snapshot(ts_ns=_ts(second=2), ingest_seq=1)])
    _write(tmp_path, "events", [_event("reconnect", ts_ns=_ts(second=3), ingest_seq=2)])
    (rows,) = compute_qa(tmp_path)
    assert rows.messages == 3
    assert rows.trades == 3
    assert rows.book_deltas == 1


def test_venue_snapshots_count_distinct_venue_snapshot_ids(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "book_snapshots",
        [
            _snapshot(ts_ns=_ts(second=1), snapshot_id="s1", source="venue", ingest_seq=0),
            _snapshot(ts_ns=_ts(second=2), snapshot_id="s1", source="venue", ingest_seq=1),
            _snapshot(ts_ns=_ts(second=3), snapshot_id="s2", source="venue", ingest_seq=2),
            _snapshot(ts_ns=_ts(second=4), snapshot_id="s3", source="sample", ingest_seq=3),
        ],
    )
    (rows,) = compute_qa(tmp_path)
    assert rows.venue_snapshots == 2
    assert rows.messages == 4


def test_lag_percentiles_over_trades_and_deltas(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "trades",
        [
            _trade(ts_ns=_ts(second=1), lag_ms=0, ingest_seq=0),
            _trade(ts_ns=_ts(second=2), lag_ms=1, ingest_seq=1),
        ],
    )
    _write(
        tmp_path,
        "book_deltas",
        [
            _delta(ts_ns=_ts(second=3), lag_ms=2, ingest_seq=2),
            _delta(ts_ns=_ts(second=4), lag_ms=3, ingest_seq=3),
        ],
    )
    (rows,) = compute_qa(tmp_path)
    assert rows.lag_p50_ms == 1.5
    assert rows.lag_p99_ms == 2.97


def test_coverage_counts_distinct_exchange_seconds(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "trades",
        [
            _trade(ts_ns=_ts(second=0), ingest_seq=0),
            _trade(ts_ns=_ts(second=1), ingest_seq=1),
            _trade(ts_ns=_ts(second=2), ingest_seq=2),
            _trade(ts_ns=_ts(second=4), ingest_seq=3),
        ],
    )
    _write(
        tmp_path,
        "book_deltas",
        [_delta(ts_ns=_ts(second=1) + 500 * MILLISECOND_NS, ingest_seq=4)],
    )
    (rows,) = compute_qa(tmp_path)
    assert rows.trades == 4
    assert rows.book_deltas == 1
    assert rows.coverage == 0.8


def test_coverage_is_zero_without_trades_or_deltas(tmp_path: Path) -> None:
    _write(tmp_path, "events", [_event("gap", ts_ns=_ts(second=1), ingest_seq=0)])
    (rows,) = compute_qa(tmp_path)
    assert rows.trades == 0
    assert rows.gaps == 1
    assert rows.coverage == 0.0
    assert rows.lag_p50_ms == 0.0


def test_event_counts_and_venue_level_events(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "events",
        [
            _event("gap", ts_ns=_ts(second=1), ingest_seq=0),
            _event("reconnect", ts_ns=_ts(second=2), ingest_seq=1),
            _event("invariant_violation", ts_ns=_ts(second=3), ingest_seq=2),
            _event("disconnect", symbol=None, ts_ns=_ts(second=4), ingest_seq=3),
        ],
    )
    (rows,) = compute_qa(tmp_path)
    assert rows.gaps == 1
    assert rows.reconnects == 1
    assert rows.invariant_violations == 1
    assert rows.resyncs == 0
    assert rows.messages == 3


def test_disconnect_only_tree_has_no_rows(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "events",
        [_event("disconnect", symbol=None, ts_ns=_ts(second=1), ingest_seq=0)],
    )
    assert compute_qa(tmp_path) == []


def test_unexplained_anomaly_without_resync(tmp_path: Path) -> None:
    _write(tmp_path, "events", [_event("gap", ts_ns=_ts(second=1), ingest_seq=0)])
    (rows,) = compute_qa(tmp_path)
    assert rows.gaps == 1
    assert rows.resyncs == 0
    assert rows.unexplained == 1


def test_resync_before_anomaly_does_not_explain_it(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "events",
        [
            _event("resync", ts_ns=_ts(second=1), ingest_seq=0),
            _event("gap", ts_ns=_ts(second=2), ingest_seq=1),
        ],
    )
    (rows,) = compute_qa(tmp_path)
    assert rows.resyncs == 1
    assert rows.unexplained == 1


def test_resync_after_anomaly_explains_it(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "events",
        [
            _event("gap", ts_ns=_ts(second=1), ingest_seq=0),
            _event("resync", ts_ns=_ts(second=2), ingest_seq=1),
        ],
    )
    (rows,) = compute_qa(tmp_path)
    assert rows.unexplained == 0


def test_one_resync_explains_every_earlier_anomaly(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "events",
        [
            _event("gap", ts_ns=_ts(second=1), ingest_seq=0),
            _event("checksum_failure", ts_ns=_ts(second=2), ingest_seq=1),
            _event("invariant_violation", ts_ns=_ts(second=3), ingest_seq=2),
            _event("resync", ts_ns=_ts(second=4), ingest_seq=3),
        ],
    )
    (rows,) = compute_qa(tmp_path)
    assert rows.gaps == 1
    assert rows.checksum_failures == 1
    assert rows.invariant_violations == 1
    assert rows.unexplained == 0


def test_anomaly_after_last_resync_stays_unexplained(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "events",
        [
            _event("gap", ts_ns=_ts(second=1), ingest_seq=0),
            _event("resync", ts_ns=_ts(second=2), ingest_seq=1),
            _event("gap", ts_ns=_ts(second=3), ingest_seq=2),
        ],
    )
    (rows,) = compute_qa(tmp_path)
    assert rows.gaps == 2
    assert rows.unexplained == 1


def test_unexplained_orders_events_by_recv_time_first(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "events",
        [
            _event("gap", ts_ns=_ts(second=5), ingest_seq=0),
            _event("resync", ts_ns=_ts(second=3), ingest_seq=9),
        ],
    )
    (rows,) = compute_qa(tmp_path)
    assert rows.resyncs == 1
    assert rows.unexplained == 1


def test_resync_does_not_explain_a_second_symbol(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "events",
        [
            _event("gap", venue="coinbase", symbol="BTC-USD", ts_ns=_ts(second=1), ingest_seq=0),
            _event("resync", venue="coinbase", symbol="BTC-USD", ts_ns=_ts(second=2), ingest_seq=1),
            _event("gap", venue="coinbase", symbol="ETH-USD", ts_ns=_ts(second=3), ingest_seq=1),
        ],
    )
    rows = compute_qa(tmp_path)
    assert [(row.symbol, row.gaps, row.resyncs, row.unexplained) for row in rows] == [
        ("BTC-USD", 1, 1, 0),
        ("ETH-USD", 1, 0, 1),
    ]


def test_date_filter_restricts_rows_and_counts(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "trades",
        [
            _trade(ts_ns=_ts(DAY, second=1), ingest_seq=0),
            _trade(ts_ns=_ts(DAY, second=2), ingest_seq=1),
            _trade(ts_ns=_ts(DAY2, second=1), ingest_seq=2),
        ],
    )
    _write(
        tmp_path,
        "events",
        [
            _event("gap", ts_ns=_ts(DAY, second=3), ingest_seq=0),
            _event("resync", ts_ns=_ts(DAY, second=4), ingest_seq=1),
            _event("gap", ts_ns=_ts(DAY2, second=3), ingest_seq=2),
        ],
    )
    all_rows = compute_qa(tmp_path)
    assert [(row.date, row.trades, row.gaps, row.unexplained) for row in all_rows] == [
        (DAY, 2, 1, 0),
        (DAY2, 1, 1, 1),
    ]

    (first_day,) = compute_qa(tmp_path, DAY)
    assert first_day.date == DAY
    assert first_day.trades == 2
    assert first_day.messages == 2
    assert first_day.gaps == 1
    assert first_day.unexplained == 0

    (second_day,) = compute_qa(tmp_path, date(2026, 10, 2))
    assert second_day.date == DAY2
    assert second_day.trades == 1
    assert second_day.unexplained == 1


def test_date_filter_with_no_matching_data_returns_no_rows(tmp_path: Path) -> None:
    _write(tmp_path, "trades", [_trade(ts_ns=_ts(DAY, second=1), ingest_seq=0)])
    assert compute_qa(tmp_path, "2026-10-03") == []


def test_rows_are_sorted_by_venue_symbol_date(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "trades",
        [
            _trade(venue="kraken", ts_ns=_ts(DAY, second=1), ingest_seq=0),
            _trade(venue="coinbase", symbol="ETH-USD", ts_ns=_ts(DAY, second=1), ingest_seq=0),
            _trade(venue="coinbase", symbol="BTC-USD", ts_ns=_ts(DAY2, second=1), ingest_seq=1),
            _trade(venue="coinbase", symbol="BTC-USD", ts_ns=_ts(DAY, second=1), ingest_seq=0),
        ],
    )
    assert [(row.venue, row.symbol, row.date) for row in compute_qa(tmp_path)] == [
        ("coinbase", "BTC-USD", DAY),
        ("coinbase", "BTC-USD", DAY2),
        ("coinbase", "ETH-USD", DAY),
        ("kraken", "BTC-USD", DAY),
    ]


def test_dod_shape_gap_and_checksum_failures_are_resolved(tmp_path: Path) -> None:
    """The Epic 1 DoD shape: one explained anomaly per venue, unexplained 0."""
    _write(
        tmp_path,
        "trades",
        [
            _trade(venue="coinbase", ts_ns=_ts(second=1), ingest_seq=0),
            _trade(venue="kraken", ts_ns=_ts(second=1), connection_id="conn-k", ingest_seq=0),
        ],
    )
    _write(
        tmp_path,
        "book_deltas",
        [
            _delta(venue="coinbase", ts_ns=_ts(second=2), ingest_seq=1),
            _delta(venue="kraken", ts_ns=_ts(second=2), connection_id="conn-k", ingest_seq=1),
        ],
    )
    _write(
        tmp_path,
        "events",
        [
            _event("gap", venue="coinbase", ts_ns=_ts(second=3), ingest_seq=2),
            _event("resync", venue="coinbase", ts_ns=_ts(second=4), ingest_seq=3),
            _event(
                "checksum_failure",
                venue="kraken",
                ts_ns=_ts(second=5),
                connection_id="conn-k",
                ingest_seq=2,
            ),
            _event(
                "resync",
                venue="kraken",
                ts_ns=_ts(second=6),
                connection_id="conn-k",
                ingest_seq=3,
            ),
        ],
    )
    rows = compute_qa(tmp_path)
    assert [(row.venue, row.symbol) for row in rows] == [
        ("coinbase", "BTC-USD"),
        ("kraken", "BTC-USD"),
    ]
    coinbase, kraken = rows
    assert (coinbase.gaps, coinbase.resyncs, coinbase.unexplained) == (1, 1, 0)
    assert coinbase.trades == 1
    assert coinbase.book_deltas == 1
    assert (kraken.checksum_failures, kraken.resyncs, kraken.unexplained) == (1, 1, 0)
    assert kraken.trades == 1
    assert kraken.book_deltas == 1
