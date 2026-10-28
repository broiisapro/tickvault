"""Bitfinex book-processor tests.

Synthetic frames drive the state machine through its two anomaly paths (a
missing sequence and a bad checksum) against the real normalizer, and the
committed five-minute fixture is streamed end to end through
:func:`tickvault.rawio.iter_raw` → normalizer → processor to prove that a clean
Bitfinex session verifies all 91 live checksums without a single event.
"""

from __future__ import annotations

import json
from collections import Counter
from decimal import Decimal
from pathlib import Path

import pytest

from tickvault.book.bitfinex_checksum import bitfinex_checksum
from tickvault.book.processor import BookProcessor, ProcessorOutput
from tickvault.models import PriceLevel, RawMessage
from tickvault.normalize import bitfinex
from tickvault.normalize.bitfinex import normalize
from tickvault.rawio import iter_raw

_CONNECTION = "bitfinex-20261028T033934Z-8dad90eb"
_RECV_TS = 1_793_158_778_146_393_000
_BOOK_CHAN = 396954
_TRADES_CHAN = 258792
_TS_MS = 1_793_158_779_000


@pytest.fixture(autouse=True)
def _clean_connection_state() -> None:
    bitfinex._CONNECTIONS.clear()
    yield
    bitfinex._CONNECTIONS.clear()


def _raw(payload: str, ingest_seq: int, *, kind: str = "msg") -> RawMessage:
    return RawMessage(
        venue="bitfinex",
        connection_id=_CONNECTION,
        ingest_seq=ingest_seq,
        recv_ts_ns=_RECV_TS,
        kind="msg" if kind == "msg" else "control",
        payload=payload,
    )


def _apply(processor: BookProcessor, payload: str, ingest_seq: int) -> ProcessorOutput:
    return processor.process(normalize(_raw(payload, ingest_seq)))


def _connect(processor: BookProcessor) -> None:
    payload = json.dumps(
        {"event": "connect", "url": "wss://api-pub.bitfinex.com/ws/2", "symbols": ["BTC-USD"]}
    )
    processor.process(normalize(_raw(payload, 0, kind="control")))


def _subscribe(processor: BookProcessor, channel: str, chan_id: int, ingest_seq: int) -> None:
    payload = json.dumps(
        {
            "event": "subscribed",
            "channel": channel,
            "chanId": chan_id,
            "symbol": "tBTCUSD",
            "pair": "BTCUSD",
        }
    )
    _apply(processor, payload, ingest_seq)


def _book(processor: BookProcessor, levels: list[list[object]], sequence: int) -> ProcessorOutput:
    """Apply a book snapshot frame whose sequence number is ``sequence``."""
    text = json.dumps([_BOOK_CHAN, levels, sequence, _TS_MS + sequence])
    return _apply(processor, text, sequence)


def _update(processor: BookProcessor, level: list[object], sequence: int) -> ProcessorOutput:
    """Apply a one-level book update frame whose sequence number is ``sequence``."""
    text = json.dumps([_BOOK_CHAN, level, sequence, _TS_MS + sequence])
    return _apply(processor, text, sequence)


def _checksum(processor: BookProcessor, checksum: int, sequence: int) -> ProcessorOutput:
    """Apply a ``cs`` frame whose sequence number is ``sequence``."""
    text = json.dumps([_BOOK_CHAN, "cs", checksum, sequence, _TS_MS + sequence])
    return _apply(processor, text, sequence)


def _start(processor: BookProcessor) -> None:
    """Connect and acknowledge both channels, in the connector's order."""
    _connect(processor)
    _subscribe(processor, "trades", _TRADES_CHAN, 1)
    _subscribe(processor, "book", _BOOK_CHAN, 2)


def _levels(
    bids: list[tuple[str, str]], asks: list[tuple[str, str]]
) -> tuple[list[PriceLevel], list[PriceLevel]]:
    return (
        [(Decimal(price), Decimal(size)) for price, size in bids],
        [(Decimal(price), Decimal(size)) for price, size in asks],
    )


def test_bitfinex_single_missing_sequence_is_flagged_and_resynced() -> None:
    processor = BookProcessor(checksums={"bitfinex": (bitfinex_checksum, 25)})
    _start(processor)
    _book(processor, [[100, 1, 1], [101, 1, -1]], 3)
    _update(processor, [100, 1, 2], 4)
    _update(processor, [99, 1, 3], 5)

    # Sequence 6 never arrives: the next frame reveals the gap.
    gap = _update(processor, [98, 1, 1], 7)
    assert [event.event_type for event in gap.events] == ["gap"]
    event = gap.events[0]
    assert (event.symbol, event.channel) == ("BTC-USD", "book")
    assert (event.expected_seq, event.received_seq) == (6, 7)
    assert [(request.reason, request.symbol) for request in gap.resync_requests] == [
        ("gap", "BTC-USD")
    ]
    # The gap-revealing frame's level is discarded, not emitted.
    assert gap.book_deltas == []

    # SYNCING: updates are dropped and counted for the next resync event.
    dropped = _update(processor, [97, 1, 1], 8)
    assert dropped.book_deltas == []
    assert dropped.events == []

    recovered = _book(processor, [[100, 1, 5], [101, 1, -5]], 9)
    assert [event.event_type for event in recovered.events] == ["resync"]
    assert recovered.events[0].symbol == "BTC-USD"
    assert json.loads(recovered.events[0].detail) == {"deltas_dropped": 1, "reason": "gap"}
    assert recovered.resync_requests == []


def test_bitfinex_checksum_failure_triggers_resync() -> None:
    calls: list[int] = []

    def recording(bids: tuple[PriceLevel, ...], asks: tuple[PriceLevel, ...]) -> int:
        calls.append(len(bids))
        return bitfinex_checksum(bids, asks)

    processor = BookProcessor(checksums={"bitfinex": (recording, 25)})
    _start(processor)
    _book(processor, [[6000, 1, 1], [5900, 1, 2], [6100, 1, -3], [6200, 1, -4]], 3)

    # A correct checksum for the state right after the update frame before it.
    good_bids, good_asks = _levels(
        [("6000", "1.5"), ("5900", "2")], [("6100", "3"), ("6200", "4")]
    )
    _update(processor, [6000, 1, 1.5], 4)
    matched = _checksum(processor, bitfinex_checksum(good_bids, good_asks), 5)
    assert matched.events == []
    assert matched.resync_requests == []
    assert matched.book_deltas == []

    # One corrupted level: the cs frame no longer matches the rebuilt state.
    applied = _update(processor, [6100, 1, -2.5], 6)
    assert [(row.side, row.size) for row in applied.book_deltas] == [("ask", Decimal("2.5"))]
    corrupted_bids, corrupted_asks = _levels(
        [("6000", "1.5"), ("5900", "2")], [("6100", "2.5"), ("6200", "4")]
    )
    computed = bitfinex_checksum(corrupted_bids, corrupted_asks)
    failed = _checksum(processor, computed + 1, 7)

    assert [event.event_type for event in failed.events] == ["checksum_failure"]
    event = failed.events[0]
    assert (event.symbol, event.channel) == ("BTC-USD", "book")
    assert json.loads(event.detail) == {"computed": computed, "expected": computed + 1}
    assert [(request.reason, request.symbol) for request in failed.resync_requests] == [
        ("checksum_failure", "BTC-USD")
    ]
    # The cs frame carries no levels: the failing state's deltas were emitted by
    # the update frame before it, and nothing is counted as dropped here (A19).
    assert failed.book_deltas == []

    recovered = _book(processor, [[6000, 1, 1], [6100, 1, -1]], 8)
    assert [event.event_type for event in recovered.events] == ["resync"]
    assert json.loads(recovered.events[0].detail) == {
        "deltas_dropped": 0,
        "reason": "checksum_failure",
    }
    assert calls == [2, 2]


def test_checksum_function_receives_the_registered_depth() -> None:
    """The processor passes the venue's depth, not a fixed top 10 (wave 07 review)."""
    seen: list[tuple[int, int]] = []

    def recording(bids: tuple[PriceLevel, ...], asks: tuple[PriceLevel, ...]) -> int:
        seen.append((len(bids), len(asks)))
        return 424242

    processor = BookProcessor(checksums={"bitfinex": (recording, 25)})
    _start(processor)
    bids = [[10_000 - index, 1, index + 1] for index in range(30)]
    asks = [[10_100 + index, 1, -(index + 1)] for index in range(30)]
    _book(processor, bids + asks, 3)
    matched = _checksum(processor, 424242, 4)

    assert seen == [(25, 25)]
    assert matched.events == []


def test_empty_checksum_frame_does_not_touch_the_book() -> None:
    processor = BookProcessor(checksums={"bitfinex": (bitfinex_checksum, 25)})
    _start(processor)
    _book(processor, [[100, 1, 1], [101, 1, -1]], 3)
    expected = bitfinex_checksum(
        [(Decimal("100"), Decimal("1"))], [(Decimal("101"), Decimal("1"))]
    )
    matched = _checksum(processor, expected, 4)
    assert matched.book_deltas == []
    assert matched.events == []

    # The next update still checksums against the state the cs frame left
    # untouched, so the empty delta neither added nor removed a level.
    _update(processor, [100, 1, 2], 5)
    after = bitfinex_checksum(
        [(Decimal("100"), Decimal("2"))], [(Decimal("101"), Decimal("1"))]
    )
    assert _checksum(processor, after, 6).events == []


def test_recorded_fixture_streams_clean(fixtures_dir: Path) -> None:
    """Every live checksum verifies, no gap and nothing resyncs, twice in a row."""
    path = fixtures_dir / "bitfinex_btc_5m.jsonl.zst"

    def run() -> tuple[list[ProcessorOutput], list[tuple[int, int]], Counter[str]]:
        calls: list[tuple[int, int]] = []

        def counting(bids: tuple[PriceLevel, ...], asks: tuple[PriceLevel, ...]) -> int:
            calls.append((len(bids), len(asks)))
            return bitfinex_checksum(bids, asks)

        processor = BookProcessor(checksums={"bitfinex": (counting, 25)})
        outputs: list[ProcessorOutput] = []
        events: Counter[str] = Counter()
        for raw in iter_raw(path):
            output = processor.process(normalize(raw))
            outputs.append(output)
            events.update(event.event_type for event in output.events)
        return outputs, calls, events

    first, first_calls, events = run()
    second, second_calls, second_events = run()

    assert first == second
    assert first_calls == second_calls
    assert events == second_events == Counter({"disconnect": 1})
    assert sum(len(output.trades) for output in first) == 135
    assert sum(len(output.book_deltas) for output in first) == 7832
    # 91 live checksums, every one compared against a full 25-level book.
    assert len(first_calls) == 91
    assert set(first_calls) == {(25, 25)}
    assert events.get("gap", 0) == 0
    assert events.get("checksum_failure", 0) == 0
    assert events.get("invariant_violation", 0) == 0
    assert events.get("resync", 0) == 0


def test_snapshot_levels_stay_exact_decimals() -> None:
    processor = BookProcessor(checksums={"bitfinex": (bitfinex_checksum, 25)})
    _start(processor)
    output = _book(processor, [[81164, 1, 0.00022], [81171, 1, -0.00602037]], 3)
    sizes = {row.side: row.size for row in output.book_snapshots if row.source == "venue"}
    assert sizes == {"bid": Decimal("0.00022"), "ask": Decimal("0.00602037")}
    assert isinstance(sizes["bid"], Decimal)
