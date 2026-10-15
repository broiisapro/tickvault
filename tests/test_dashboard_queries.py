"""Tests for :mod:`dashboard.queries` over replayed fixture trees.

Each query runs against a tree built from the synthetic and recorded raw
fixtures by the same normalizer -> BookProcessor -> ParquetWriter path as
``tickvault.pipeline.run_replay`` (that function is used directly once issue
#51 is merged). Assertions use independently written SQL and specific values,
so a broken query cannot pass vacuously.
"""

from __future__ import annotations

import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import fields, replace
from pathlib import Path

import duckdb
import pytest
import zstandard as zstd

from dashboard import queries
from tickvault.book.processor import BookProcessor, ProcessorOutput
from tickvault.models import NormalizedMessage, RawMessage
from tickvault.normalize import coinbase as coinbase_normalizer
from tickvault.normalize import kraken as kraken_normalizer
from tickvault.qa.metrics import QARow
from tickvault.rawio import encode_line, iter_raw
from tickvault.sql import connect, run_query
from tickvault.storage.parquet_writer import ParquetWriter

_NORMALIZERS = {"coinbase": coinbase_normalizer.normalize, "kraken": kraken_normalizer.normalize}
_DAY_RECORDED = "2026-10-15"
_DAY_SYNTHETIC = "2026-11-01"


def _normalize(raw: RawMessage) -> NormalizedMessage:
    return _NORMALIZERS[raw.venue](raw)


def _replay(root: Path, raw_paths: Sequence[Path]) -> None:
    """Build a normalized tree from raw fixtures with the pipeline path."""
    try:
        from tickvault.pipeline import run_replay
    except ImportError:
        _replay_inline(root, raw_paths)
        return
    for path in raw_paths:
        run_replay(path, root)


def _replay_inline(root: Path, raw_paths: Sequence[Path]) -> None:
    writer = ParquetWriter(root)
    try:
        for path in raw_paths:
            processor = BookProcessor()
            for raw in iter_raw(path):
                _write_output(writer, processor.process(_normalize(raw)))
    finally:
        writer.close()


def _write_output(writer: ParquetWriter, output: ProcessorOutput) -> None:
    writer.write("trades", output.trades)
    writer.write("book_deltas", output.book_deltas)
    writer.write("book_snapshots", output.book_snapshots)
    writer.write("events", output.events)


@contextmanager
def _connection(root: Path) -> Iterator[duckdb.DuckDBPyConnection]:
    con = connect(root)
    try:
        yield con
    finally:
        con.close()


@pytest.fixture(scope="module")
def dashboard_tree(
    tmp_path_factory: pytest.TempPathFactory,
    coinbase_5m_path: Path,
    kraken_5m_path: Path,
    synthetic_gap_path: Path,
) -> Path:
    root = tmp_path_factory.mktemp("dashboard-tree")
    _replay(root, [coinbase_5m_path, kraken_5m_path, synthetic_gap_path])
    return root


def test_message_rate_matches_independent_count(dashboard_tree: Path) -> None:
    with _connection(dashboard_tree) as con:
        rate = queries.message_rate(con, window_seconds=3600, bucket_seconds=60)
    assert list(rate.columns) == ["venue", "bucket", "messages", "msgs_per_second"]
    assert set(rate["venue"]) == {"coinbase", "kraken"}
    assert (rate["messages"] > 0).all()
    assert rate["bucket"].dt.tz is not None
    assert (rate["msgs_per_second"] == rate["messages"] / 60).all()

    _, rows = run_query(
        dashboard_tree,
        """
        WITH messages AS (
            SELECT venue, connection_id, ingest_seq, recv_ts_ns FROM trades
            UNION ALL
            SELECT venue, connection_id, ingest_seq, recv_ts_ns FROM book_deltas
        ), bounds AS (SELECT max(recv_ts_ns) AS newest_ns FROM messages)
        SELECT venue, (recv_ts_ns // 60000000000) * 60000000000 AS bucket_ns,
               count(DISTINCT (connection_id, ingest_seq)) AS messages
        FROM messages, bounds
        WHERE recv_ts_ns > bounds.newest_ns - 3600000000000
        GROUP BY venue, bucket_ns
        ORDER BY venue, bucket_ns
        """,
    )
    expected = {(venue, bucket_ns): messages for venue, bucket_ns, messages in rows}
    actual = {
        (row.venue, int(row.bucket.value)): int(row.messages) for row in rate.itertuples()
    }
    assert actual == expected
    assert len(expected) > 1


def test_kpi_window_is_relative_to_the_newest_data_not_the_wall_clock(
    tmp_path: Path, coinbase_5m_path: Path
) -> None:
    """The messages/s KPI must count the newest data, however old it is.

    The recorded fixture is rewritten onto a 2023 timestamp; a wall-clock
    window would find nothing, while the newest-record window returns the
    whole session.
    """
    messages = list(iter_raw(coinbase_5m_path))
    target_start_ns = 1_700_000_000_000_000_000
    shift_ns = messages[0].recv_ts_ns - target_start_ns
    lines = b"".join(
        encode_line(replace(message, recv_ts_ns=message.recv_ts_ns - shift_ns))
        for message in messages
    )
    raw_path = tmp_path / "old-session.jsonl.zst"
    raw_path.write_bytes(zstd.ZstdCompressor().compress(lines))
    root = tmp_path / "root"
    _replay(root, [raw_path])

    newest_ns = max(message.recv_ts_ns for message in messages) - shift_ns
    assert newest_ns < time.time_ns() - 30 * 24 * 3600 * 10**9
    with _connection(root) as con:
        kpi = queries.message_rate(con, window_seconds=300, bucket_seconds=60)
    assert not kpi.empty
    assert int(kpi["messages"].sum()) > 0
    assert int(kpi["bucket"].min().value) > newest_ns - 360 * 10**9
    assert int(kpi["bucket"].max().value) <= newest_ns


def test_message_rate_window_is_anchored_to_the_newest_data(dashboard_tree: Path) -> None:
    with _connection(dashboard_tree) as con:
        rate = queries.message_rate(con, window_seconds=600, bucket_seconds=60)
    _, rows = run_query(
        dashboard_tree,
        """
        SELECT max(recv_ts_ns) FROM trades
        UNION ALL
        SELECT max(recv_ts_ns) FROM book_deltas
        """,
    )
    newest_ns = max(row[0] for row in rows)
    newest_bucket = int(rate["bucket"].max().value)
    assert newest_ns - 60 * 10**9 < newest_bucket <= newest_ns
    assert int(rate["bucket"].min().value) > newest_ns - 600 * 10**9


def test_message_rate_row_cap_keeps_the_newest_buckets(dashboard_tree: Path) -> None:
    with _connection(dashboard_tree) as con:
        full = queries.message_rate(con, limit=10_000)
        capped = queries.message_rate(con, limit=3)
    assert len(capped) <= 3 * full["venue"].nunique()
    for venue, group in capped.groupby("venue"):
        expected = full[full["venue"] == venue].tail(3)
        assert group["bucket"].tolist() == expected["bucket"].tolist()


def test_message_rate_rejects_non_positive_arguments(dashboard_tree: Path) -> None:
    with _connection(dashboard_tree) as con:
        with pytest.raises(ValueError, match="window_seconds"):
            queries.message_rate(con, window_seconds=0)
        with pytest.raises(ValueError, match="bucket_seconds"):
            queries.message_rate(con, bucket_seconds=0)
        with pytest.raises(ValueError, match="limit"):
            queries.message_rate(con, limit=0)


def test_cumulative_event_counts_match_the_synthetic_session(dashboard_tree: Path) -> None:
    with _connection(dashboard_tree) as con:
        coinbase = queries.cumulative_event_counts(con, venues=("coinbase",))
        kraken = queries.cumulative_event_counts(con, venues=("kraken",))
    assert set(coinbase["venue"]) == {"coinbase"}
    assert set(kraken["venue"]) == {"kraken"}
    coinbase_counts = dict(zip(coinbase["event_type"], coinbase["count"], strict=True))
    assert coinbase_counts["gap"] == 1
    assert coinbase_counts["resync"] == 1
    kraken_counts = dict(zip(kraken["event_type"], kraken["count"], strict=True))
    assert kraken_counts["checksum_failure"] == 1
    assert kraken_counts["resync"] == 1


def test_qa_dates_lists_every_partition_day(dashboard_tree: Path) -> None:
    assert queries.qa_dates(dashboard_tree) == [_DAY_RECORDED, _DAY_SYNTHETIC]


def test_qa_history_covers_dates_and_matches_the_fixture_events(dashboard_tree: Path) -> None:
    queries.clear_qa_cache()
    frame = queries.qa_history(dashboard_tree)
    assert list(frame.columns) == [field.name for field in fields(QARow)]
    assert sorted(frame["date"].unique()) == [_DAY_RECORDED, _DAY_SYNTHETIC]
    assert (frame["unexplained"] == 0).all()

    synthetic = frame[frame["date"] == _DAY_SYNTHETIC]
    coinbase = synthetic[synthetic["venue"] == "coinbase"].iloc[0]
    assert coinbase["gaps"] == 1
    assert coinbase["resyncs"] == 1
    kraken = synthetic[synthetic["venue"] == "kraken"].iloc[0]
    assert kraken["checksum_failures"] == 1
    assert kraken["resyncs"] == 1


def test_qa_history_date_filter(dashboard_tree: Path) -> None:
    frame = queries.qa_history(dashboard_tree, dates=[_DAY_SYNTHETIC])
    assert set(frame["date"]) == {_DAY_SYNTHETIC}
    assert not frame.empty


def test_top_of_book_uses_only_sample_level_zero_rows(dashboard_tree: Path) -> None:
    with _connection(dashboard_tree) as con:
        book = queries.top_of_book(
            con, venues=("coinbase",), symbols=("BTC-USD",), limit=10_000
        )
    assert list(book.columns) == [
        "venue",
        "symbol",
        "ts",
        "bid_price",
        "bid_size",
        "ask_price",
        "ask_size",
    ]
    assert not book.empty
    assert (book["bid_price"] > 0).all()
    assert (book["bid_size"] > 0).all()
    assert (book["ask_price"] > book["bid_price"]).all()
    assert (book["ask_size"] > 0).all()
    assert book["ts"].is_monotonic_increasing
    _, rows = run_query(
        dashboard_tree,
        """
        SELECT count(DISTINCT exchange_ts_ns) FROM book_snapshots
        WHERE source = 'sample' AND level = 0 AND venue = 'coinbase' AND symbol = 'BTC-USD'
        """,
    )
    assert len(book) == rows[0][0]


def test_top_of_book_row_cap_and_since_filter(dashboard_tree: Path) -> None:
    with _connection(dashboard_tree) as con:
        full = queries.top_of_book(
            con, venues=("coinbase",), symbols=("BTC-USD",), limit=10_000
        )
        capped = queries.top_of_book(
            con, venues=("coinbase",), symbols=("BTC-USD",), limit=5
        )
        cutoff_ns = int(full["ts"].astype("int64").iloc[len(full) // 2])
        since = queries.top_of_book(
            con, venues=("coinbase",), symbols=("BTC-USD",), since_ns=cutoff_ns, limit=10_000
        )
    assert capped["ts"].tolist() == full["ts"].tail(5).tolist()
    expected_since = full[full["ts"].astype("int64") >= cutoff_ns]
    assert since["ts"].tolist() == expected_since["ts"].tolist()


def test_spread_mid_is_derived_from_the_same_top_of_book_rows(dashboard_tree: Path) -> None:
    selectors = {"venues": ("coinbase",), "symbols": ("BTC-USD",), "limit": 10_000}
    with _connection(dashboard_tree) as con:
        book = queries.top_of_book(con, **selectors)
        spread = queries.spread_mid(con, **selectors)
    assert list(spread.columns) == ["venue", "symbol", "ts", "mid", "spread_bps"]
    assert not spread.empty
    assert (spread["spread_bps"] >= 0).all()
    merged = spread.merge(book, on=["venue", "symbol", "ts"], how="inner")
    assert len(merged) == len(spread) == len(book)
    expected_mid = (merged["bid_price"] + merged["ask_price"]) / 2
    expected_bps = (merged["ask_price"] - merged["bid_price"]) / expected_mid * 10000
    assert (merged["mid"] - expected_mid).abs().max() < 1e-9
    assert (merged["spread_bps"] - expected_bps).abs().max() < 1e-6


def test_dimensions_lists_replayed_markets(dashboard_tree: Path) -> None:
    with _connection(dashboard_tree) as con:
        frame = queries.dimensions(con)
    assert list(frame.columns) == ["venue", "symbol"]
    assert set(zip(frame["venue"], frame["symbol"], strict=True)) == {
        ("coinbase", "BTC-USD"),
        ("kraken", "BTC-USD"),
    }


def test_data_span_matches_an_independent_min_max(dashboard_tree: Path) -> None:
    with _connection(dashboard_tree) as con:
        span = queries.data_span(con)
    assert list(span.columns) == ["first_ts_ns", "last_ts_ns"]
    assert len(span) == 1
    _, rows = run_query(
        dashboard_tree,
        """
        SELECT min(ts), max(ts) FROM (
            SELECT exchange_ts_ns AS ts FROM trades
            UNION ALL SELECT exchange_ts_ns FROM book_deltas
            UNION ALL SELECT exchange_ts_ns FROM book_snapshots
            UNION ALL SELECT exchange_ts_ns FROM events WHERE exchange_ts_ns IS NOT NULL
        )
        """,
    )
    assert int(span["first_ts_ns"].iloc[0]) == rows[0][0]
    assert int(span["last_ts_ns"].iloc[0]) == rows[0][1]


def test_data_span_is_one_empty_row_on_an_empty_root(tmp_path: Path) -> None:
    with _connection(tmp_path) as con:
        span = queries.data_span(con)
    assert list(span.columns) == ["first_ts_ns", "last_ts_ns"]
    assert len(span) == 1
    assert span["first_ts_ns"].isna().all()
    assert span["last_ts_ns"].isna().all()


def test_queries_on_empty_root_return_empty_frames(tmp_path: Path) -> None:
    with _connection(tmp_path) as con:
        rate = queries.message_rate(con)
        events = queries.cumulative_event_counts(con)
        book = queries.top_of_book(con)
        spread = queries.spread_mid(con)
        dims = queries.dimensions(con)
    assert rate.empty
    assert list(rate.columns) == ["venue", "bucket", "messages", "msgs_per_second"]
    assert events.empty
    assert list(events.columns) == ["venue", "event_type", "count"]
    assert book.empty
    assert list(book.columns) == [
        "venue",
        "symbol",
        "ts",
        "bid_price",
        "bid_size",
        "ask_price",
        "ask_size",
    ]
    assert spread.empty
    assert list(spread.columns) == ["venue", "symbol", "ts", "mid", "spread_bps"]
    assert dims.empty
    assert queries.qa_dates(tmp_path) == []
    history = queries.qa_history(tmp_path)
    assert history.empty
    assert list(history.columns) == [field.name for field in fields(QARow)]
