"""End-to-end Coinbase tests: raw fixture -> normalizer -> BookProcessor.

The normalizer and the processor each have focused unit tests; this file
covers the seam between them over real recorded frames, asserting the
integration properties the acceptance criteria name: no anomalies on the
recorded session, deltas and samples exactly as the venue sent them, the
synthetic gap session reproducing ``synthetic_gap_session.expected.json`` for
Coinbase, and deterministic outputs. Nothing here touches the network.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

from tickvault.book.processor import BookProcessor, ProcessorOutput
from tickvault.models import (
    BookDelta,
    BookSnapshotLevel,
    Event,
    NormalizedMessage,
    RawMessage,
    Trade,
)
from tickvault.normalize.coinbase import normalize

_ANOMALY_TYPES = {"gap", "checksum_failure", "invariant_violation"}


def _normalized(
    path: Path, raw_lines: Callable[[str | Path], list[RawMessage]]
) -> list[NormalizedMessage]:
    return [normalize(raw) for raw in raw_lines(path)]


def _run(messages: list[NormalizedMessage]) -> list[ProcessorOutput]:
    processor = BookProcessor()
    return [processor.process(message) for message in messages]


def _events(outputs: list[ProcessorOutput]) -> list[Event]:
    return [event for output in outputs for event in output.events]


def _deltas(outputs: list[ProcessorOutput]) -> list[BookDelta]:
    return [delta for output in outputs for delta in output.book_deltas]


def _trades(outputs: list[ProcessorOutput]) -> list[Trade]:
    return [trade for output in outputs for trade in output.trades]


def _snapshots(outputs: list[ProcessorOutput], source: str) -> list[BookSnapshotLevel]:
    return [
        row
        for output in outputs
        for row in output.book_snapshots
        if row.source == source
    ]


def _expected_deltas(messages: list[NormalizedMessage]) -> list[BookDelta]:
    """The rows the processor must emit for every non-snapshot book update."""
    expected: list[BookDelta] = []
    for message in messages:
        for update in message.book_updates:
            if update.is_snapshot:
                continue
            for side, levels in (("bid", update.bids), ("ask", update.asks)):
                for price, size in levels:
                    expected.append(
                        BookDelta(
                            venue=message.raw.venue,
                            symbol=update.symbol,
                            exchange_ts_ns=update.exchange_ts_ns,
                            recv_ts_ns=message.raw.recv_ts_ns,
                            connection_id=message.raw.connection_id,
                            ingest_seq=message.raw.ingest_seq,
                            side=side,
                            price=price,
                            size=size,
                            sequence=message.sequence,
                        )
                    )
    return expected


def test_recorded_fixture_is_clean_and_every_update_level_is_emitted(
    coinbase_5m_path: Path,
    raw_lines: Callable[[str | Path], list[RawMessage]],
) -> None:
    messages = _normalized(coinbase_5m_path, raw_lines)
    outputs = _run(messages)

    events = _events(outputs)
    assert [event.event_type for event in events if event.event_type in _ANOMALY_TYPES] == []
    assert [request for output in outputs for request in output.resync_requests] == []

    snapshot_messages = [
        message for message in messages if any(u.is_snapshot for u in message.book_updates)
    ]
    assert len(snapshot_messages) == 1
    (snapshot_message,) = snapshot_messages
    (snapshot_update,) = snapshot_message.book_updates
    snapshot_rows = _snapshots(outputs, "venue")
    assert {row.snapshot_id for row in snapshot_rows} == {
        f"{snapshot_message.raw.connection_id}:{snapshot_message.raw.ingest_seq}"
    }
    assert len(snapshot_rows) == len(snapshot_update.bids) + len(snapshot_update.asks)

    # The book is LIVE after the snapshot: every update level is applied and
    # emitted verbatim, in venue order, with the message's sequence.
    emitted = _deltas(outputs)
    assert emitted == _expected_deltas(messages)
    assert len(emitted) > 0
    assert len(_trades(outputs)) > 0


def test_recorded_fixture_samples_cover_every_exchange_second(
    coinbase_5m_path: Path,
    raw_lines: Callable[[str | Path], list[RawMessage]],
) -> None:
    messages = _normalized(coinbase_5m_path, raw_lines)
    outputs = _run(messages)

    groups: dict[str, list[BookSnapshotLevel]] = {}
    for row in _snapshots(outputs, "sample"):
        groups.setdefault(row.snapshot_id, []).append(row)

    book_seconds = {
        update.exchange_ts_ns // 1_000_000_000
        for message in messages
        for update in message.book_updates
    }
    sample_seconds = {rows[0].exchange_ts_ns // 1_000_000_000 for rows in groups.values()}
    assert len(book_seconds) > 0
    assert sample_seconds == book_seconds
    assert sorted(sample_seconds) == list(range(min(sample_seconds), max(sample_seconds) + 1))

    for rows in groups.values():
        bids = [row for row in rows if row.side == "bid"]
        asks = [row for row in rows if row.side == "ask"]
        assert [row.level for row in bids] == list(range(10))
        assert [row.level for row in asks] == list(range(10))
        assert bids[0].price < asks[0].price


def test_synthetic_session_coinbase_matches_the_expected_file(
    synthetic_gap_path: Path,
    synthetic_expected: dict[str, object],
    raw_lines: Callable[[str | Path], list[RawMessage]],
) -> None:
    raw_messages = [raw for raw in raw_lines(synthetic_gap_path) if raw.venue == "coinbase"]
    messages = [normalize(raw) for raw in raw_messages]
    outputs = _run(messages)
    events = _events(outputs)

    counts = synthetic_expected["counts"]["coinbase"]["BTC-USD"]
    expected_events = [
        entry
        for entry in synthetic_expected["events"]
        if entry["venue"] == "coinbase"
    ]

    # The expected file describes the anomaly and resync events; the closing
    # disconnect is a venue lifecycle event it does not list.
    anomaly_events = [event for event in events if event.event_type != "disconnect"]
    actual_keys = [
        (
            event.event_type,
            event.venue,
            event.symbol,
            event.connection_id,
            event.ingest_seq,
            event.expected_seq,
            event.received_seq,
        )
        for event in anomaly_events
    ]
    expected_keys = [
        (
            entry["event_type"],
            entry["venue"],
            entry["symbol"],
            entry["connection_id"],
            entry["ingest_seq"],
            entry["expected_seq"],
            entry["received_seq"],
        )
        for entry in expected_events
    ]
    assert actual_keys == expected_keys

    assert [event.event_type for event in events] == ["gap", "resync", "disconnect"]
    disconnect = events[-1]
    assert disconnect.ingest_seq == 40
    assert json.loads(disconnect.detail) == {"reason": "session_complete"}

    resyncs = [event for event in events if event.event_type == "resync"]
    assert len(resyncs) == counts["resyncs"]
    resync_detail = json.loads(resyncs[0].detail)
    assert resync_detail["deltas_dropped"] == counts["book_deltas_dropped"]

    assert len(_deltas(outputs)) == counts["book_deltas_applied"]
    assert len(_trades(outputs)) == counts["trades"]
    venue_snapshot_ids = {row.snapshot_id for row in _snapshots(outputs, "venue")}
    assert len(venue_snapshot_ids) == counts["venue_snapshots"]
    assert len([event for event in events if event.event_type == "gap"]) == counts["gaps"]
    assert len([event for event in events if event.event_type in _ANOMALY_TYPES]) == (
        counts["gaps"] + counts["checksum_failures"] + counts["invariant_violations"]
    )


def test_processing_the_recorded_fixture_twice_is_deterministic(
    coinbase_5m_path: Path,
    raw_lines: Callable[[str | Path], list[RawMessage]],
) -> None:
    messages = _normalized(coinbase_5m_path, raw_lines)
    first = _run(messages)
    second = _run(messages)
    assert first == second
    assert any(output.book_deltas for output in first)
    assert any(output.book_snapshots for output in first)
    assert any(output.trades for output in first)
