"""Fixture-level validation of the Kraken normalizer and book processor.

The recorded 5-minute session must decode and verify every checksum without a
single failure (the reference implementation rebuilt the same book with zero
mismatches, so any failure here is a decoding, truncation or ordering bug),
and the synthetic session must reproduce the Kraken counts and events recorded
in ``synthetic_gap_session.expected.json``.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Callable
from pathlib import Path

from tickvault.book.processor import BookProcessor
from tickvault.models import NormalizedMessage, RawMessage
from tickvault.normalize.kraken import normalize

RawLines = Callable[[str | Path], list[RawMessage]]

_ANOMALY_TYPES = {"gap", "checksum_failure", "invariant_violation"}


def _normalized(raw_lines: RawLines, path: Path) -> list[NormalizedMessage]:
    return [normalize(raw) for raw in raw_lines(path)]


def test_recorded_fixture_passes_every_checksum_and_invariant(
    kraken_5m_path: Path, raw_lines: RawLines
) -> None:
    processor = BookProcessor()
    messages = _normalized(raw_lines, kraken_5m_path)
    book_messages = [message for message in messages if message.channel == "book"]
    assert len(book_messages) > 1000
    updates = [update for message in book_messages for update in message.book_updates]
    assert sum(1 for update in updates if update.is_snapshot) == 1
    # Every book message carries a checksum, so every one was verified.
    assert all(update.checksum is not None for update in updates)

    event_types: Counter[str] = Counter()
    applied = 0
    sample_rows = 0
    for message in messages:
        output = processor.process(message)
        applied += len(output.book_deltas)
        sample_rows += sum(1 for row in output.book_snapshots if row.source == "sample")
        event_types.update(event.event_type for event in output.events)

    assert applied > 1000
    assert sample_rows > 0
    assert processor.stale_messages == 0
    assert event_types.get("checksum_failure", 0) == 0
    assert event_types.get("invariant_violation", 0) == 0
    assert event_types.get("gap", 0) == 0
    assert event_types.get("resync", 0) == 0
    # Only the session's closing disconnect control produces an event.
    assert event_types == Counter({"disconnect": 1})


def test_synthetic_kraken_session_matches_expected(
    raw_lines: RawLines, synthetic_gap_path: Path, synthetic_expected: dict[str, object]
) -> None:
    processor = BookProcessor()
    outputs = [
        processor.process(normalize(raw))
        for raw in raw_lines(synthetic_gap_path)
        if raw.venue == "kraken"
    ]
    events = [event for output in outputs for event in output.events]
    anomalies = [event for event in events if event.event_type != "disconnect"]

    expected_events = [
        event
        for event in synthetic_expected["events"]  # type: ignore[union-attr]
        if event["venue"] == "kraken"
    ]
    assert [
        {
            "event_type": event.event_type,
            "venue": event.venue,
            "symbol": event.symbol,
            "connection_id": event.connection_id,
            "ingest_seq": event.ingest_seq,
            "expected_seq": event.expected_seq,
            "received_seq": event.received_seq,
        }
        for event in anomalies
    ] == expected_events

    counts = synthetic_expected["counts"]["kraken"]["BTC-USD"]  # type: ignore[index]
    event_types = Counter(event.event_type for event in events)
    applied = sum(len(output.book_deltas) for output in outputs)
    dropped = sum(
        json.loads(event.detail)["deltas_dropped"]
        for event in events
        if event.event_type == "resync"
    )
    venue_snapshots = len(
        {
            row.snapshot_id
            for output in outputs
            for row in output.book_snapshots
            if row.source == "venue"
        }
    )
    trades = sum(len(output.trades) for output in outputs)
    unexplained = 0
    for index, event in enumerate(anomalies):
        if event.event_type not in _ANOMALY_TYPES:
            continue
        if not any(
            later.event_type == "resync" and later.symbol == event.symbol
            for later in anomalies[index + 1 :]
        ):
            unexplained += 1

    assert counts["gaps"] == event_types["gap"] == 0
    assert counts["checksum_failures"] == event_types["checksum_failure"] == 1
    assert counts["resyncs"] == event_types["resync"] == 1
    assert counts["reconnects"] == event_types["reconnect"] == 0
    assert counts["invariant_violations"] == event_types["invariant_violation"] == 0
    assert counts["book_deltas_applied"] == applied == 50
    assert counts["book_deltas_dropped"] == dropped == 2
    assert counts["venue_snapshots"] == venue_snapshots == 2
    assert counts["trades"] == trades == 3
    assert counts["unexplained"] == unexplained == 0


def test_synthetic_kraken_processing_is_deterministic(
    raw_lines: RawLines, synthetic_gap_path: Path
) -> None:
    def run() -> list[tuple[NormalizedMessage, object]]:
        processor = BookProcessor()
        return [
            (message := normalize(raw), processor.process(message))
            for raw in raw_lines(synthetic_gap_path)
            if raw.venue == "kraken"
        ]

    assert run() == run()
