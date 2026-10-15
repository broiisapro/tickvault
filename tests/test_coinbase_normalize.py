"""Tests for the Coinbase Advanced Trade normalizer.

Live fixtures prove the happy path over real frames; hand-built frames prove
the exact field mapping, ordering and error handling. No network is used.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path

import pytest

from tickvault.models import NormalizedMessage, RawMessage, Trade
from tickvault.normalize import NormalizeError
from tickvault.normalize.coinbase import normalize
from tickvault.timeutil import parse_rfc3339_ns

CONNECTION_ID = "coinbase-20261015T115619Z-00000001"
RECV_TS_NS = 1_762_531_000_123_456_789


def _raw(
    payload: object,
    *,
    kind: str = "msg",
    ingest_seq: int = 0,
    recv_ts_ns: int = RECV_TS_NS,
) -> RawMessage:
    return RawMessage(
        venue="coinbase",
        connection_id=CONNECTION_ID,
        ingest_seq=ingest_seq,
        recv_ts_ns=recv_ts_ns,
        kind="msg" if kind == "msg" else "control",
        payload=payload if isinstance(payload, str) else json.dumps(payload),
    )


def _snapshot_message() -> dict[str, object]:
    return {
        "channel": "l2_data",
        "timestamp": "2026-10-15T03:18:51.909462959Z",
        "sequence_num": 1,
        "events": [
            {
                "type": "snapshot",
                "product_id": "BTC-USD",
                "updates": [
                    {
                        "side": "bid",
                        "event_time": "2026-10-15T03:18:51.588352Z",
                        "price_level": "81209.31",
                        "new_quantity": "0.04534153",
                    },
                    {
                        "side": "offer",
                        "event_time": "2026-10-15T03:18:51.588352Z",
                        "price_level": "81209.41",
                        "new_quantity": "0.10000000",
                    },
                ],
            }
        ],
    }


def _trade_message(
    trades: list[dict[str, str]], *, event_type: str = "update"
) -> dict[str, object]:
    return {
        "channel": "market_trades",
        "timestamp": "2026-10-15T03:18:52.428837Z",
        "sequence_num": 11,
        "events": [{"type": event_type, "trades": trades}],
    }


def _trade(trade_id: str, side: str, size: str = "0.50000000") -> dict[str, str]:
    return {
        "product_id": "BTC-USD",
        "trade_id": trade_id,
        "price": "81196.76",
        "size": size,
        "time": "2026-10-15T03:18:52.428837Z",
        "side": side,
    }


def test_recorded_fixture_normalizes_every_line(
    coinbase_5m_path: Path,
    raw_lines: Callable[[str | Path], list[RawMessage]],
) -> None:
    messages = raw_lines(coinbase_5m_path)
    normalized = [normalize(message) for message in messages]

    snapshots = [update for n in normalized for update in n.book_updates if update.is_snapshot]
    deltas = [update for n in normalized for update in n.book_updates if not update.is_snapshot]
    trades = [trade for n in normalized for trade in n.trades]

    assert len(snapshots) == 1
    assert len(deltas) > 0
    assert len(trades) > 0

    sequences = [n.sequence for n in normalized if n.raw.kind == "msg"]
    assert None not in sequences
    assert sequences[0] == 0
    assert sequences == list(range(sequences[0], sequences[0] + len(sequences)))


def test_recorded_fixture_normalization_is_deterministic(
    coinbase_5m_path: Path,
    raw_lines: Callable[[str | Path], list[RawMessage]],
) -> None:
    messages = raw_lines(coinbase_5m_path)
    first = [normalize(message) for message in messages]
    second = [normalize(message) for message in messages]
    assert first == second


def test_snapshot_uses_message_timestamp_with_nanoseconds() -> None:
    message = normalize(_raw(_snapshot_message()))
    assert message.channel == "l2_data"
    assert message.sequence == 1
    (update,) = message.book_updates
    assert update.is_snapshot
    assert update.exchange_ts_ns == parse_rfc3339_ns("2026-10-15T03:18:51.909462959Z")
    assert update.exchange_ts_ns % 1_000_000_000 == 909_462_959
    assert update.bids == ((Decimal("81209.31"), Decimal("0.04534153")),)
    assert update.asks == ((Decimal("81209.41"), Decimal("0.10000000")),)


def test_update_groups_levels_by_distinct_event_time() -> None:
    message_payload = _snapshot_message() | {
        "sequence_num": 2,
        "events": [
            {
                "type": "update",
                "product_id": "BTC-USD",
                "updates": [
                    {
                        "side": "bid",
                        "event_time": "2026-10-15T03:18:52.000000Z",
                        "price_level": "81200.00",
                        "new_quantity": "0.5",
                    },
                    {
                        "side": "offer",
                        "event_time": "2026-10-15T03:18:53.000000Z",
                        "price_level": "81210.00",
                        "new_quantity": "0.25",
                    },
                    {
                        "side": "offer",
                        "event_time": "2026-10-15T03:18:52.000000Z",
                        "price_level": "81201.00",
                        "new_quantity": "0",
                    },
                ],
            }
        ],
    }
    message = normalize(_raw(message_payload))
    first, second = message.book_updates
    assert not first.is_snapshot
    assert first.exchange_ts_ns == parse_rfc3339_ns("2026-10-15T03:18:52.000000Z")
    assert first.bids == ((Decimal("81200.00"), Decimal("0.5")),)
    assert first.asks == ((Decimal("81201.00"), Decimal("0")),)
    assert second.exchange_ts_ns == parse_rfc3339_ns("2026-10-15T03:18:53.000000Z")
    assert second.bids == ()
    assert second.asks == ((Decimal("81210.00"), Decimal("0.25")),)


def test_trades_are_sorted_by_integer_trade_id_and_invert_the_maker_side() -> None:
    message = normalize(
        _raw(
            _trade_message(
                [
                    _trade("1106252618", "BUY"),
                    _trade("1106252616", "SELL"),
                    _trade("1106252617", "BUY"),
                ]
            )
        )
    )
    assert [trade.trade_id for trade in message.trades] == [
        "1106252616",
        "1106252617",
        "1106252618",
    ]
    assert [trade.side for trade in message.trades] == ["buy", "sell", "sell"]


def test_trade_snapshot_events_emit_no_trades() -> None:
    message = normalize(_raw(_trade_message([_trade("1", "BUY")], event_type="snapshot")))
    assert message.channel == "market_trades"
    assert message.sequence == 11
    assert message.trades == ()


def test_trade_keeps_tiny_size_exact() -> None:
    message = normalize(_raw(_trade_message([_trade("1", "BUY", size="0.00000003")])))
    (trade,) = message.trades
    assert trade.size == Decimal("0.00000003")
    assert format(trade.size, "f") == "0.00000003"
    assert not isinstance(trade.size, float)
    assert trade.price == Decimal("81196.76")
    assert trade.exchange_ts_ns == parse_rfc3339_ns("2026-10-15T03:18:52.428837Z")


def test_trade_carries_raw_provenance() -> None:
    message = normalize(_raw(_trade_message([_trade("1", "BUY")]), ingest_seq=7))
    (trade,) = message.trades
    assert isinstance(trade, Trade)
    assert trade.venue == "coinbase"
    assert trade.symbol == "BTC-USD"
    assert trade.recv_ts_ns == RECV_TS_NS
    assert trade.connection_id == CONNECTION_ID
    assert trade.ingest_seq == 7
    assert trade.sequence == 11


@pytest.mark.parametrize(
    "channel",
    ["heartbeats", "subscriptions", "status"],
)
def test_housekeeping_and_unknown_channels_keep_their_sequence(channel: str) -> None:
    payload = {
        "channel": channel,
        "timestamp": "2026-10-15T03:18:52.428837Z",
        "sequence_num": 42,
        "events": [],
    }
    message = normalize(_raw(payload))
    assert message.channel == channel
    assert message.sequence == 42
    assert message.trades == ()
    assert message.book_updates == ()
    assert message.control is None


def test_control_message_is_parsed() -> None:
    payload = {"event": "resync_request", "symbol": "BTC-USD", "reason": "gap"}
    message = normalize(_raw(payload, kind="control"))
    assert message.control == payload
    assert message.channel is None
    assert message.sequence is None
    assert message.trades == ()
    assert message.book_updates == ()


def test_normalized_message_carries_raw() -> None:
    raw = _raw(_snapshot_message())
    message = normalize(raw)
    assert isinstance(message, NormalizedMessage)
    assert message.raw is raw


@pytest.mark.parametrize(
    "payload",
    [
        "not json",
        "[1, 2, 3]",
        '{"timestamp":"2026-10-15T03:18:52Z","sequence_num":1}',
        '{"channel":"l2_data","sequence_num":1,"events":[]}',
        '{"channel":"l2_data","timestamp":"2026-10-15T03:18:52Z","sequence_num":"1","events":[]}',
        json.dumps(_snapshot_message() | {"timestamp": None}),
        json.dumps(_snapshot_message() | {"events": "no"}),
        json.dumps(
            _snapshot_message()
            | {
                "events": [
                    {
                        "type": "snapshot",
                        "product_id": "BTCUSD",
                        "updates": [],
                    }
                ]
            }
        ),
        json.dumps(
            _snapshot_message()
            | {
                "events": [
                    {
                        "type": "snapshot",
                        "product_id": "BTC-USD",
                        "updates": [
                            {
                                "side": "middle",
                                "event_time": "2026-10-15T03:18:52Z",
                                "price_level": "1",
                                "new_quantity": "1",
                            }
                        ],
                    }
                ]
            }
        ),
        json.dumps(
            _snapshot_message()
            | {
                "events": [
                    {
                        "type": "snapshot",
                        "product_id": "BTC-USD",
                        "updates": [
                            {
                                "side": "bid",
                                "event_time": "2026-10-15T03:18:52Z",
                                "price_level": 81200.1,
                                "new_quantity": "1",
                            }
                        ],
                    }
                ]
            }
        ),
        json.dumps(
            _snapshot_message()
            | {
                "events": [
                    {
                        "type": "snapshot",
                        "product_id": "BTC-USD",
                        "updates": [
                            {
                                "side": "bid",
                                "event_time": "2026-10-15T03:18:52Z",
                                "price_level": "1",
                                "new_quantity": "NaN",
                            }
                        ],
                    }
                ]
            }
        ),
        json.dumps(
            _snapshot_message()
            | {
                "events": [
                    {
                        "type": "update",
                        "product_id": "BTC-USD",
                        "updates": [
                            {
                                "side": "bid",
                                "price_level": "1",
                                "new_quantity": "1",
                            }
                        ],
                    }
                ]
            }
        ),
        json.dumps(_trade_message([_trade("1", "MAKER")])),
        json.dumps(_trade_message([_trade("not-a-number", "BUY")])),
        json.dumps(_trade_message([_trade("1", "BUY")], event_type="correction")),
    ],
)
def test_malformed_payloads_raise_normalize_error(payload: str) -> None:
    with pytest.raises(NormalizeError):
        normalize(_raw(payload))
