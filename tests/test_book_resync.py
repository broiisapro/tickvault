"""Reconnect race-guard tests.

The classic bug: after a reconnect, a delta still in flight from the previous
connection must never be applied, not before the new snapshot and not after
the book has recovered.
"""

from __future__ import annotations

import json
from decimal import Decimal

from tickvault.book.processor import BookProcessor
from tickvault.models import BookUpdate, NormalizedMessage, PriceLevel, RawMessage

_BASE_TS = 1_793_491_200_000_000_000  # 2026-11-01T00:00:00Z
_CONNECTION_A = "coinbase-20261101T000000Z-0000000a"
_CONNECTION_B = "coinbase-20261101T000100Z-0000000b"


def lvl(price: str, size: str) -> PriceLevel:
    return (Decimal(price), Decimal(size))


def msg(
    connection_id: str,
    ingest_seq: int,
    *,
    sequence: int | None = None,
    book_updates: tuple[BookUpdate, ...] = (),
    recv_ts_ns: int = _BASE_TS,
    control: dict[str, object] | None = None,
) -> NormalizedMessage:
    raw = RawMessage(
        venue="coinbase",
        connection_id=connection_id,
        ingest_seq=ingest_seq,
        recv_ts_ns=recv_ts_ns,
        kind="control" if control is not None else "msg",
        payload="{}",
    )
    return NormalizedMessage(raw=raw, sequence=sequence, book_updates=book_updates, control=control)


def update(
    *,
    snapshot: bool,
    bids: tuple[PriceLevel, ...] = (),
    asks: tuple[PriceLevel, ...] = (),
    ts: int = _BASE_TS,
) -> BookUpdate:
    return BookUpdate(
        symbol="BTC-USD",
        is_snapshot=snapshot,
        bids=bids,
        asks=asks,
        exchange_ts_ns=ts,
    )


def test_no_pre_reconnect_delta_applied_after_resync() -> None:
    processor = BookProcessor()
    processor.process(msg(_CONNECTION_A, 0, control={"event": "connect", "subscriptions": []}))
    processor.process(
        msg(
            _CONNECTION_A,
            1,
            book_updates=(update(snapshot=True, bids=(lvl("100", "1"),), asks=(lvl("101", "1"),)),),
        )
    )
    applied = processor.process(
        msg(_CONNECTION_A, 2, book_updates=(update(snapshot=False, bids=(lvl("100", "2"),)),)),
    )
    assert [row.size for row in applied.book_deltas] == [Decimal("2")]

    reconnect = processor.process(
        msg(_CONNECTION_B, 0, control={"event": "connect", "subscriptions": []})
    )
    assert [event.event_type for event in reconnect.events] == ["reconnect"]
    assert reconnect.events[0].symbol == "BTC-USD"

    stale_delta = processor.process(
        msg(_CONNECTION_A, 3, book_updates=(update(snapshot=False, bids=(lvl("100", "3"),)),)),
    )
    assert stale_delta.book_deltas == []

    recovered = processor.process(
        msg(
            _CONNECTION_B,
            1,
            book_updates=(update(snapshot=True, bids=(lvl("100", "7"),), asks=(lvl("101", "7"),)),),
        )
    )
    assert [event.event_type for event in recovered.events] == ["resync"]
    assert json.loads(recovered.events[0].detail) == {
        "deltas_dropped": 1,
        "reason": "reconnect",
    }

    late_delta = processor.process(
        msg(
            _CONNECTION_A,
            4,
            book_updates=(update(snapshot=False, bids=(lvl("50", "999"),)),),
        )
    )
    assert late_delta.book_deltas == []

    sampled = processor.process(
        msg(
            _CONNECTION_B,
            2,
            book_updates=(
                update(snapshot=False, bids=(lvl("100", "8"),), ts=_BASE_TS + 2_000_000_000),
            ),
        )
    )
    assert [row.size for row in sampled.book_deltas] == [Decimal("8")]
    sample_rows = [row for row in sampled.book_snapshots if row.source == "sample"]
    assert sample_rows
    prices = {row.price for row in sample_rows}
    assert Decimal("50") not in prices  # the late delta from A never reached B's book
    assert Decimal("100") in prices and Decimal("101") in prices


def test_first_connect_emits_no_reconnect_event() -> None:
    processor = BookProcessor()
    output = processor.process(
        msg(_CONNECTION_A, 0, control={"event": "connect", "subscriptions": []})
    )
    assert output.events == []
