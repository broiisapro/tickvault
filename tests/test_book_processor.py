"""BookProcessor tests: checksum, invariants, sample cadence and determinism."""

from __future__ import annotations

import json
from decimal import Decimal

from tickvault.book.kraken_checksum import kraken_checksum
from tickvault.book.processor import BookProcessor, ProcessorOutput
from tickvault.models import BookUpdate, NormalizedMessage, PriceLevel, RawMessage, Trade

_BASE_TS = 1_793_491_200_000_000_000  # 2026-11-01T00:00:00Z
_COINBASE = "coinbase-20261101T000000Z-0000000a"
_KRAKEN = "kraken-20261101T000000Z-0000000b"
_COINBASE_BTC = "coinbase-20261101T000000Z-0000000c"
_COINBASE_ETH = "coinbase-20261101T000000Z-0000000d"
_COINBASE_ETH_2 = "coinbase-20261101T000000Z-0000000e"
_COINBASE_LEGACY_A = "coinbase-20261101T000000Z-0000000f"
_COINBASE_LEGACY_B = "coinbase-20261101T000000Z-00000010"
_KRAKEN_2 = "kraken-20261101T000000Z-00000011"


def lvl(price: str, size: str) -> PriceLevel:
    return (Decimal(price), Decimal(size))


def update(
    symbol: str = "BTC-USD",
    *,
    snapshot: bool,
    bids: tuple[PriceLevel, ...] = (),
    asks: tuple[PriceLevel, ...] = (),
    ts: int = _BASE_TS,
    checksum: int | None = None,
) -> BookUpdate:
    return BookUpdate(
        symbol=symbol,
        is_snapshot=snapshot,
        bids=bids,
        asks=asks,
        exchange_ts_ns=ts,
        checksum=checksum,
    )


def msg(
    connection_id: str,
    ingest_seq: int,
    *,
    venue: str = "coinbase",
    sequence: int | None = None,
    channel: str | None = None,
    book_updates: tuple[BookUpdate, ...] = (),
    trades: tuple[Trade, ...] = (),
    recv_ts_ns: int = _BASE_TS,
    control: dict[str, object] | None = None,
) -> NormalizedMessage:
    raw = RawMessage(
        venue=venue,
        connection_id=connection_id,
        ingest_seq=ingest_seq,
        recv_ts_ns=recv_ts_ns,
        kind="control" if control is not None else "msg",
        payload="{}",
    )
    return NormalizedMessage(
        raw=raw,
        channel=channel,
        sequence=sequence,
        trades=trades,
        book_updates=book_updates,
        control=control,
    )


def _samples(output: ProcessorOutput) -> list[str]:
    return [row.snapshot_id for row in output.book_snapshots if row.source == "sample"]


def test_first_snapshot_goes_live_without_a_resync_event() -> None:
    processor = BookProcessor()
    output = processor.process(
        msg(
            _COINBASE,
            0,
            sequence=1,
            channel="l2_data",
            book_updates=(update(snapshot=True, bids=(lvl("100", "1"),), asks=(lvl("101", "1"),)),),
        )
    )
    assert output.events == []
    assert output.resync_requests == []
    venue_rows = [row for row in output.book_snapshots if row.source == "venue"]
    assert [(row.side, row.level, row.price) for row in venue_rows] == [
        ("bid", 0, Decimal("100")),
        ("ask", 0, Decimal("101")),
    ]
    assert all(row.snapshot_id == f"{_COINBASE}:0" for row in venue_rows)


def test_live_deltas_are_applied_and_emitted_with_the_message_sequence() -> None:
    processor = BookProcessor()
    processor.process(
        msg(
            _COINBASE,
            0,
            sequence=1,
            channel="l2_data",
            book_updates=(update(snapshot=True, bids=(lvl("100", "1"),), asks=(lvl("101", "1"),)),),
        )
    )
    output = processor.process(
        msg(
            _COINBASE,
            1,
            sequence=2,
            channel="l2_data",
            book_updates=(
                update(snapshot=False, bids=(lvl("100", "2"),), asks=(lvl("101", "0"),)),
            ),
        )
    )
    assert [(row.side, row.price, row.size) for row in output.book_deltas] == [
        ("bid", Decimal("100"), Decimal("2")),
        ("ask", Decimal("101"), Decimal("0")),
    ]
    assert {row.sequence for row in output.book_deltas} == {2}
    assert {row.ingest_seq for row in output.book_deltas} == {1}


def test_stale_message_ignores_book_levels_and_increments_the_counter() -> None:
    processor = BookProcessor()
    processor.process(
        msg(
            _COINBASE,
            0,
            sequence=5,
            channel="l2_data",
            book_updates=(update(snapshot=True, bids=(lvl("100", "1"),), asks=(lvl("101", "1"),)),),
        )
    )
    output = processor.process(
        msg(
            _COINBASE,
            1,
            sequence=4,
            channel="l2_data",
            book_updates=(update(snapshot=False, bids=(lvl("100", "9"),)),),
        )
    )
    assert output.book_deltas == []
    assert output.events == []
    assert processor.stale_messages == 1


def test_checksum_failure_flags_resync_and_discards_the_update() -> None:
    processor = BookProcessor()
    bids = (lvl("100", "1"),)
    asks = (lvl("101", "1"),)
    snapshot = update(snapshot=True, bids=bids, asks=asks, checksum=kraken_checksum(bids, asks))
    processor.process(msg(_KRAKEN, 0, venue="kraken", channel="book", book_updates=(snapshot,)))

    updated_bids = (lvl("100", "5"),)
    correct = kraken_checksum(updated_bids, asks)
    wrong = (correct + 1) % 2**32
    output = processor.process(
        msg(
            _KRAKEN,
            1,
            venue="kraken",
            channel="book",
            book_updates=(update(snapshot=False, bids=updated_bids, checksum=wrong),),
        )
    )
    assert output.book_deltas == []
    assert [event.event_type for event in output.events] == ["checksum_failure"]
    event = output.events[0]
    assert json.loads(event.detail) == {"computed": correct, "expected": wrong}
    assert event.channel == "book"
    assert [request.reason for request in output.resync_requests] == ["checksum_failure"]
    assert output.resync_requests[0].symbol == "BTC-USD"

    recovery = (lvl("99", "2"),)
    output = processor.process(
        msg(
            _KRAKEN,
            2,
            venue="kraken",
            channel="book",
            book_updates=(
                update(
                    snapshot=True,
                    bids=recovery,
                    asks=asks,
                    checksum=kraken_checksum(recovery, asks),
                ),
            ),
        )
    )
    resyncs = [event for event in output.events if event.event_type == "resync"]
    assert len(resyncs) == 1
    assert json.loads(resyncs[0].detail) == {"deltas_dropped": 0, "reason": "checksum_failure"}


def test_failing_snapshot_checksum_keeps_syncing_and_discards_the_rows() -> None:
    processor = BookProcessor()
    bids = (lvl("100", "1"),)
    asks = (lvl("101", "1"),)
    output = processor.process(
        msg(
            _KRAKEN,
            0,
            venue="kraken",
            channel="book",
            book_updates=(
                update(
                    snapshot=True, bids=bids, asks=asks, checksum=kraken_checksum(bids, asks) + 1
                ),
            ),
        )
    )
    assert output.book_snapshots == []
    assert [event.event_type for event in output.events] == ["checksum_failure"]
    assert [request.reason for request in output.resync_requests] == ["checksum_failure"]


def test_invariant_violation_flags_resync_and_syncing() -> None:
    processor = BookProcessor()
    processor.process(
        msg(
            _COINBASE,
            0,
            sequence=1,
            channel="l2_data",
            book_updates=(update(snapshot=True, bids=(lvl("100", "1"),), asks=(lvl("101", "1"),)),),
        )
    )
    output = processor.process(
        msg(
            _COINBASE,
            1,
            sequence=2,
            channel="l2_data",
            book_updates=(update(snapshot=False, bids=(lvl("102", "1"),)),),
        )
    )
    assert [event.event_type for event in output.events] == ["invariant_violation"]
    assert json.loads(output.events[0].detail) == {"codes": ["crossed"]}
    assert output.book_deltas and output.book_deltas[0].price == Decimal("102")
    assert [request.reason for request in output.resync_requests] == ["invariant_violation"]

    dropped = processor.process(
        msg(
            _COINBASE,
            2,
            sequence=3,
            channel="l2_data",
            book_updates=(update(snapshot=False, bids=(lvl("99", "1"),)),),
        )
    )
    assert dropped.book_deltas == []

    recovered = processor.process(
        msg(
            _COINBASE,
            3,
            sequence=4,
            channel="l2_data",
            book_updates=(update(snapshot=True, bids=(lvl("100", "1"),), asks=(lvl("101", "1"),)),),
        )
    )
    resyncs = [event for event in recovered.events if event.event_type == "resync"]
    assert len(resyncs) == 1
    assert json.loads(resyncs[0].detail) == {
        "deltas_dropped": 1,
        "reason": "invariant_violation",
    }


def test_sample_cadence_is_one_top10_snapshot_per_exchange_second() -> None:
    processor = BookProcessor()
    bids = tuple(lvl(str(100 - index), "1") for index in range(12))
    asks = tuple(lvl(str(101 + index), "1") for index in range(12))
    first = processor.process(
        msg(
            _COINBASE,
            0,
            sequence=1,
            channel="l2_data",
            book_updates=(update(snapshot=True, bids=bids, asks=asks, ts=_BASE_TS),),
        )
    )
    first_samples = [row for row in first.book_snapshots if row.source == "sample"]
    assert len(first_samples) == 20  # 10 per side
    assert _samples(first)

    same_second = processor.process(
        msg(
            _COINBASE,
            1,
            sequence=2,
            channel="l2_data",
            book_updates=(
                update(snapshot=False, bids=(lvl("100", "2"),), ts=_BASE_TS + 100_000_000),
            ),
        )
    )
    assert _samples(same_second) == []

    next_second = processor.process(
        msg(
            _COINBASE,
            2,
            sequence=3,
            channel="l2_data",
            book_updates=(
                update(snapshot=False, bids=(lvl("100", "3"),), ts=_BASE_TS + 1_000_000_000),
            ),
        )
    )
    next_samples = [row for row in next_second.book_snapshots if row.source == "sample"]
    assert len(next_samples) == 20
    bid_rows = [row for row in next_samples if row.side == "bid"]
    assert [row.level for row in bid_rows] == list(range(10))
    assert bid_rows[0].price == Decimal("100")
    assert bid_rows[0].size == Decimal("3")

    skipped = processor.process(
        msg(
            _COINBASE,
            3,
            sequence=4,
            channel="l2_data",
            book_updates=(
                update(snapshot=False, bids=(lvl("100", "4"),), ts=_BASE_TS + 3_000_000_000),
            ),
        )
    )
    skipped_samples = [row for row in skipped.book_snapshots if row.source == "sample"]
    assert len(skipped_samples) == 20  # one sample, not one per missed second


def test_trades_pass_through_unchanged() -> None:
    processor = BookProcessor()
    trade = Trade(
        venue="coinbase",
        symbol="BTC-USD",
        exchange_ts_ns=_BASE_TS,
        recv_ts_ns=_BASE_TS,
        connection_id=_COINBASE,
        ingest_seq=7,
        trade_id="9100001",
        price=Decimal("81200.10"),
        size=Decimal("0.02"),
        side="buy",
        sequence=9,
    )
    output = processor.process(msg(_COINBASE, 7, sequence=9, trades=(trade,)))
    assert output.trades == [trade]
    assert output.book_deltas == [] and output.book_snapshots == []


def test_declared_symbols_scope_each_connection() -> None:
    """Two Coinbase products connect back-to-back; both books go live alone."""
    processor = BookProcessor()
    btc_connect = processor.process(
        msg(_COINBASE_BTC, 0, control={"event": "connect", "symbols": ["BTC-USD"]})
    )
    eth_connect = processor.process(
        msg(_COINBASE_ETH, 0, control={"event": "connect", "symbols": ["ETH-USD"]})
    )
    assert btc_connect.events == []
    assert eth_connect.events == []

    processor.process(
        msg(
            _COINBASE_BTC,
            1,
            sequence=1,
            channel="l2_data",
            book_updates=(update(snapshot=True, bids=(lvl("100", "1"),), asks=(lvl("101", "1"),)),),
        )
    )
    processor.process(
        msg(
            _COINBASE_ETH,
            1,
            sequence=1,
            channel="l2_data",
            book_updates=(
                update("ETH-USD", snapshot=True, bids=(lvl("10", "1"),), asks=(lvl("11", "1"),)),
            ),
        )
    )

    btc_delta = processor.process(
        msg(
            _COINBASE_BTC,
            2,
            sequence=2,
            channel="l2_data",
            book_updates=(update(snapshot=False, bids=(lvl("100", "2"),)),),
        )
    )
    eth_delta = processor.process(
        msg(
            _COINBASE_ETH,
            2,
            sequence=2,
            channel="l2_data",
            book_updates=(update("ETH-USD", snapshot=False, asks=(lvl("11", "3"),)),),
        )
    )
    assert [(row.symbol, row.side, row.price) for row in btc_delta.book_deltas] == [
        ("BTC-USD", "bid", Decimal("100"))
    ]
    assert [(row.symbol, row.side, row.price) for row in eth_delta.book_deltas] == [
        ("ETH-USD", "ask", Decimal("11"))
    ]
    assert btc_delta.events == []
    assert eth_delta.events == []


def test_reconnect_scopes_to_the_declared_symbols_only() -> None:
    processor = BookProcessor()
    processor.process(
        msg(_COINBASE_BTC, 0, control={"event": "connect", "symbols": ["BTC-USD"]})
    )
    processor.process(
        msg(_COINBASE_ETH, 0, control={"event": "connect", "symbols": ["ETH-USD"]})
    )
    processor.process(
        msg(
            _COINBASE_BTC,
            1,
            sequence=1,
            channel="l2_data",
            book_updates=(update(snapshot=True, bids=(lvl("100", "1"),), asks=(lvl("101", "1"),)),),
        )
    )
    processor.process(
        msg(
            _COINBASE_ETH,
            1,
            sequence=1,
            channel="l2_data",
            book_updates=(
                update("ETH-USD", snapshot=True, bids=(lvl("10", "1"),), asks=(lvl("11", "1"),)),
            ),
        )
    )

    reconnect = processor.process(
        msg(_COINBASE_ETH_2, 0, control={"event": "connect", "symbols": ["ETH-USD"]})
    )
    assert [(event.event_type, event.symbol) for event in reconnect.events] == [
        ("reconnect", "ETH-USD")
    ]

    stale = processor.process(
        msg(
            _COINBASE_ETH,
            2,
            sequence=2,
            channel="l2_data",
            book_updates=(update("ETH-USD", snapshot=False, asks=(lvl("11", "9"),)),),
        )
    )
    assert stale.book_deltas == []

    recovered = processor.process(
        msg(
            _COINBASE_ETH_2,
            1,
            sequence=1,
            channel="l2_data",
            book_updates=(
                update("ETH-USD", snapshot=True, bids=(lvl("10", "2"),), asks=(lvl("11", "2"),)),
            ),
        )
    )
    assert [event.event_type for event in recovered.events] == ["resync"]
    assert json.loads(recovered.events[0].detail) == {
        "deltas_dropped": 1,
        "reason": "reconnect",
    }

    btc_delta = processor.process(
        msg(
            _COINBASE_BTC,
            2,
            sequence=2,
            channel="l2_data",
            book_updates=(update(snapshot=False, bids=(lvl("100", "3"),)),),
        )
    )
    assert [row.symbol for row in btc_delta.book_deltas] == ["BTC-USD"]


def test_kraken_reconnect_marks_every_declared_symbol() -> None:
    processor = BookProcessor()
    processor.process(
        msg(
            _KRAKEN,
            0,
            venue="kraken",
            control={"event": "connect", "symbols": ["BTC-USD", "ETH-USD"]},
        )
    )
    processor.process(
        msg(
            _KRAKEN,
            1,
            venue="kraken",
            channel="book",
            book_updates=(update(snapshot=True, bids=(lvl("100", "1"),), asks=(lvl("101", "1"),)),),
        )
    )
    processor.process(
        msg(
            _KRAKEN,
            2,
            venue="kraken",
            channel="book",
            book_updates=(
                update("ETH-USD", snapshot=True, bids=(lvl("10", "1"),), asks=(lvl("11", "1"),)),
            ),
        )
    )

    reconnect = processor.process(
        msg(
            _KRAKEN_2,
            0,
            venue="kraken",
            control={"event": "connect", "symbols": ["ETH-USD", "BTC-USD", "ETH-USD"]},
        )
    )
    assert [(event.event_type, event.symbol) for event in reconnect.events] == [
        ("reconnect", "BTC-USD"),
        ("reconnect", "ETH-USD"),
    ]

    recovered = processor.process(
        msg(
            _KRAKEN_2,
            1,
            venue="kraken",
            channel="book",
            book_updates=(
                update("ETH-USD", snapshot=True, bids=(lvl("10", "2"),), asks=(lvl("11", "2"),)),
            ),
        )
    )
    assert [event.event_type for event in recovered.events] == ["resync"]
    assert json.loads(recovered.events[0].detail)["reason"] == "reconnect"


def test_legacy_connect_leaves_new_symbols_to_their_first_connection() -> None:
    """Recordings without ``symbols`` keep venue-wide reconnect for known ones.

    A symbol first seen later belongs to the connection that delivers it, not
    to the venue's most recent connection.
    """
    processor = BookProcessor()
    processor.process(
        msg(_COINBASE_LEGACY_A, 0, control={"event": "connect", "subscriptions": []})
    )
    processor.process(
        msg(
            _COINBASE_LEGACY_A,
            1,
            sequence=1,
            channel="l2_data",
            book_updates=(update(snapshot=True, bids=(lvl("100", "1"),), asks=(lvl("101", "1"),)),),
        )
    )

    reconnect = processor.process(
        msg(_COINBASE_LEGACY_B, 0, control={"event": "connect", "subscriptions": []})
    )
    assert [(event.event_type, event.symbol) for event in reconnect.events] == [
        ("reconnect", "BTC-USD")
    ]

    late = processor.process(
        msg(
            _COINBASE_LEGACY_A,
            2,
            sequence=2,
            channel="l2_data",
            book_updates=(
                update("ETH-USD", snapshot=True, bids=(lvl("10", "1"),), asks=(lvl("11", "1"),)),
            ),
        )
    )
    venue_rows = [row for row in late.book_snapshots if row.source == "venue"]
    assert [(row.symbol, row.side) for row in venue_rows] == [
        ("ETH-USD", "bid"),
        ("ETH-USD", "ask"),
    ]

    dropped = processor.process(
        msg(
            _COINBASE_LEGACY_B,
            1,
            sequence=1,
            channel="l2_data",
            book_updates=(update("ETH-USD", snapshot=False, bids=(lvl("10", "5"),)),),
        )
    )
    assert dropped.book_deltas == []


def test_disconnect_is_a_venue_level_event() -> None:
    processor = BookProcessor()
    output = processor.process(
        msg(
            _COINBASE,
            0,
            control={"event": "disconnect", "reason": "session_complete"},
        )
    )
    assert len(output.events) == 1
    event = output.events[0]
    assert event.event_type == "disconnect"
    assert event.symbol is None
    assert event.exchange_ts_ns is None
    assert json.loads(event.detail) == {"reason": "session_complete"}


def _session_messages() -> list[NormalizedMessage]:
    bids = (lvl("100", "1"), lvl("99", "1"))
    asks = (lvl("101", "1"),)
    messages = [
        msg(_COINBASE, 0, sequence=0, channel="subscriptions"),
        msg(
            _COINBASE,
            1,
            sequence=1,
            channel="l2_data",
            book_updates=(update(snapshot=True, bids=bids, asks=asks),),
        ),
        msg(
            _COINBASE,
            2,
            sequence=2,
            channel="l2_data",
            book_updates=(update(snapshot=False, bids=(lvl("100", "2"),)),),
        ),
        msg(_COINBASE, 3, sequence=3, channel="heartbeats"),
        msg(
            _COINBASE,
            4,
            sequence=4,
            channel="l2_data",
            book_updates=(update(snapshot=False, bids=(lvl("99", "5"),)),),
        ),
        msg(
            _COINBASE,
            5,
            sequence=6,
            channel="l2_data",
            book_updates=(update(snapshot=False, bids=(lvl("98", "1"),)),),
        ),
        msg(
            _COINBASE,
            6,
            sequence=7,
            channel="l2_data",
            book_updates=(update(snapshot=False, bids=(lvl("97", "1"),)),),
        ),
        msg(
            _COINBASE,
            7,
            sequence=8,
            channel="l2_data",
            book_updates=(
                update(snapshot=True, bids=bids, asks=asks, ts=_BASE_TS + 2_000_000_000),
            ),
        ),
        msg(
            _COINBASE,
            8,
            sequence=9,
            channel="l2_data",
            book_updates=(
                update(snapshot=False, bids=(lvl("100", "7"),), ts=_BASE_TS + 3_000_000_000),
            ),
        ),
        msg(_COINBASE, 9, control={"event": "disconnect", "reason": "done"}),
    ]
    return messages


def test_determinism_same_inputs_produce_identical_outputs() -> None:
    def run() -> list[ProcessorOutput]:
        processor = BookProcessor()
        return [processor.process(message) for message in _session_messages()]

    first = run()
    second = run()
    assert first == second
    event_counts = [len(output.events) for output in first]
    assert any(count == 1 for count in event_counts)  # the gap was seen
    assert [event.event_type for output in first for event in output.events] == [
        "gap",
        "resync",
        "disconnect",
    ]


def test_default_checksum_map_comes_from_the_venue_registry() -> None:
    processor = BookProcessor()

    assert processor.checksum_registry == {"kraken": kraken_checksum}
    assert processor.checksum_depths == {"kraken": 25}
    assert processor.kraken_depth == 25


def test_kraken_depth_argument_overrides_the_registered_depth() -> None:
    processor = BookProcessor(kraken_depth=5)

    assert processor.checksum_registry == {"kraken": kraken_checksum}
    assert processor.checksum_depths == {"kraken": 5}


def test_custom_checksum_map_controls_truncation_and_verification() -> None:
    seen: list[tuple[int, int]] = []

    def recording_checksum(bids: tuple[PriceLevel, ...], asks: tuple[PriceLevel, ...]) -> int:
        seen.append((len(bids), len(asks)))
        return 424242

    processor = BookProcessor(checksums={"kraken": (recording_checksum, 2)})
    output = processor.process(
        msg(
            _KRAKEN,
            0,
            venue="kraken",
            channel="book",
            book_updates=(
                update(
                    snapshot=True,
                    bids=(lvl("100", "1"), lvl("99", "2"), lvl("98", "3")),
                    asks=(lvl("101", "1"), lvl("102", "2"), lvl("103", "3")),
                    checksum=424242,
                ),
            ),
        )
    )

    assert seen == [(2, 2)]
    assert output.events == []
    assert {row.price for row in output.book_snapshots if row.source == "venue"} == {
        Decimal("100"),
        Decimal("99"),
        Decimal("101"),
        Decimal("102"),
    }


def test_an_empty_checksum_map_disables_verification() -> None:
    processor = BookProcessor(checksums={})
    bids = (lvl("100", "1"),)
    asks = (lvl("101", "1"),)

    output = processor.process(
        msg(
            _KRAKEN,
            0,
            venue="kraken",
            channel="book",
            book_updates=(update(snapshot=True, bids=bids, asks=asks, checksum=1),),
        )
    )

    assert processor.checksum_registry == {}
    assert processor.checksum_depths == {}
    assert output.events == []
    assert [row.side for row in output.book_snapshots if row.source == "venue"] == ["bid", "ask"]
