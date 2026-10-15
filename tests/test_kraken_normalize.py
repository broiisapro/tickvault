"""Kraken v2 normalizer unit tests."""

from __future__ import annotations

import json
from decimal import Decimal

import pytest

from tickvault.models import NormalizedMessage, RawMessage
from tickvault.normalize import NormalizeError
from tickvault.normalize.kraken import normalize

_CONNECTION = "kraken-20261015T115618Z-920d7f99"
_RECV_TS = 1_792_065_379_378_252_000


def _raw(payload: object, *, kind: str = "msg", req_seq: int = 0) -> RawMessage:
    text = payload if isinstance(payload, str) else json.dumps(payload)
    return RawMessage(
        venue="kraken",
        connection_id=_CONNECTION,
        ingest_seq=req_seq,
        recv_ts_ns=_RECV_TS,
        kind="msg" if kind == "msg" else "control",
        payload=text,
    )


def _book_payload(
    *,
    message_type: str = "update",
    timestamp: str | None = "2026-10-15T11:56:20.123456Z",
    checksum: object = 1234567,
    symbol: object = "BTC/USD",
    bids: list[object] | None = None,
    asks: list[object] | None = None,
) -> dict[str, object]:
    entry: dict[str, object] = {
        "symbol": symbol,
        "bids": [{"price": 80939.9, "qty": 1.85741721}] if bids is None else bids,
        "asks": [{"price": 80940.0, "qty": 0.5}] if asks is None else asks,
        "checksum": checksum,
    }
    if timestamp is not None:
        entry["timestamp"] = timestamp
    return {"channel": "book", "type": message_type, "data": [entry]}


def test_book_update_normalizes_symbol_levels_and_checksum() -> None:
    message = normalize(_raw(_book_payload()))
    assert message.channel == "book"
    assert message.sequence is None
    assert len(message.book_updates) == 1
    update = message.book_updates[0]
    assert (update.symbol, update.is_snapshot, update.checksum) == ("BTC-USD", False, 1234567)
    assert update.bids == ((Decimal("80939.9"), Decimal("1.85741721")),)
    assert update.asks == ((Decimal("80940.0"), Decimal("0.5")),)
    assert update.exchange_ts_ns == 1_792_065_380_123_456_000


def test_snapshot_is_flagged() -> None:
    message = normalize(_raw(_book_payload(message_type="snapshot")))
    assert message.book_updates[0].is_snapshot is True


def test_snapshot_without_timestamp_uses_recv_ts_ns() -> None:
    message = normalize(_raw(_book_payload(message_type="snapshot", timestamp=None)))
    assert message.book_updates[0].exchange_ts_ns == _RECV_TS


def test_trailing_zero_quantities_survive_exactly() -> None:
    # Raw JSON text, not a Python dict: float literals would already have
    # lost the trailing zero before the normalizer saw the payload.
    payload = (
        '{"channel":"book","type":"update","data":[{"symbol":"BTC/USD",'
        '"bids":[{"price":81203.0,"qty":0.00006120}],"asks":[],'
        '"checksum":1234567,"timestamp":"2026-10-15T11:56:20.256336Z"}]}'
    )
    update = normalize(_raw(payload)).book_updates[0]
    price, size = update.bids[0]
    assert str(price) == "81203.0"
    assert str(size) == "0.00006120"
    assert format(size, "f") == "0.00006120"
    assert update.checksum == 1234567


def test_multiple_data_entries_become_multiple_updates() -> None:
    payload = _book_payload()
    first = payload["data"][0]  # type: ignore[index]
    payload["data"] = [first, dict(first, symbol="ETH/USD")]  # type: ignore[index]
    message = normalize(_raw(payload))
    assert [update.symbol for update in message.book_updates] == ["BTC-USD", "ETH-USD"]


def test_trades_keep_message_order_and_fields() -> None:
    payload = (
        '{"channel":"trade","type":"update","data":['
        '{"symbol":"BTC/USD","side":"buy","price":81203.0,"qty":0.00006120,'
        '"ord_type":"limit","trade_id":211000002,"timestamp":"2026-10-15T11:56:20.256336Z"},'
        '{"symbol":"BTC/USD","side":"sell","price":81200.1,"qty":0.001,'
        '"ord_type":"limit","trade_id":211000001,"timestamp":"2026-10-15T11:56:20.200000Z"}'
        "]}"
    )
    message = normalize(_raw(payload))
    assert message.channel == "trade"
    assert message.sequence is None
    assert [trade.trade_id for trade in message.trades] == ["211000002", "211000001"]
    assert [trade.side for trade in message.trades] == ["buy", "sell"]
    first = message.trades[0]
    assert (first.venue, first.symbol) == ("kraken", "BTC-USD")
    assert first.price == Decimal("81203.0")
    assert str(first.size) == "0.00006120"
    assert first.exchange_ts_ns == 1_792_065_380_256_336_000
    assert (first.recv_ts_ns, first.connection_id, first.ingest_seq) == (_RECV_TS, _CONNECTION, 0)
    assert first.sequence is None


@pytest.mark.parametrize("channel", ["status", "heartbeat", "unknown_channel"])
def test_housekeeping_and_unknown_channels_yield_no_records(channel: str) -> None:
    payload = {"channel": channel}
    if channel == "status":
        payload["type"] = "update"
        payload["data"] = [{"system": "online"}]
    message = normalize(_raw(payload))
    assert message.channel == channel
    assert message.book_updates == () and message.trades == () and message.sequence is None


def test_subscribe_ack_names_its_channel() -> None:
    payload = {
        "method": "subscribe",
        "result": {"channel": "book", "depth": 25, "snapshot": True, "symbol": "BTC/USD"},
        "success": True,
        "time_in": "2026-10-15T11:56:19.310772Z",
        "time_out": "2026-10-15T11:56:19.310798Z",
    }
    message = normalize(_raw(payload))
    assert message.channel == "book"
    assert message.book_updates == () and message.trades == ()


def test_failed_subscribe_ack_is_still_an_empty_message() -> None:
    payload = {"method": "subscribe", "result": "already subscribed", "success": False}
    message = normalize(_raw(payload))
    assert message.channel is None
    assert message.book_updates == () and message.trades == ()


def test_control_line_yields_the_parsed_object() -> None:
    payload = {"event": "connect", "url": "wss://ws.kraken.com/v2"}
    message = normalize(_raw(payload, kind="control"))
    assert message.control == payload
    assert message.channel is None


@pytest.mark.parametrize(
    "payload",
    [
        "not json",
        "[]",
        {"no_channel": True},
        {"channel": "book"},
        {"channel": "book", "type": "delete", "data": [{}]},
        {"channel": "book", "type": "update", "data": []},
        {"channel": "book", "type": "update", "data": ["x"]},
        {
            "channel": "book",
            "type": "update",
            "data": [
                {"symbol": "BTCUSD", "bids": [], "asks": [], "checksum": 1, "timestamp": "x"}
            ],
        },
        {
            "channel": "book",
            "type": "update",
            "data": [
                {
                    "symbol": "BTC/USD",
                    "bids": [{"price": 1.5, "qty": "bad"}],
                    "asks": [],
                    "checksum": 1,
                    "timestamp": "2026-10-15T11:56:20.256336Z",
                }
            ],
        },
        {
            "channel": "book",
            "type": "update",
            "data": [
                {
                    "symbol": "BTC/USD",
                    "bids": [{"price": 1.5}],
                    "asks": [],
                    "checksum": 1,
                    "timestamp": "2026-10-15T11:56:20.256336Z",
                }
            ],
        },
        {
            "channel": "book",
            "type": "update",
            "data": [
                {
                    "symbol": "BTC/USD",
                    "bids": [],
                    "asks": [],
                    "checksum": 1.5,
                    "timestamp": "2026-10-15T11:56:20.256336Z",
                }
            ],
        },
        {
            "channel": "book",
            "type": "update",
            "data": [
                {
                    "symbol": "BTC/USD",
                    "bids": [],
                    "asks": [],
                    "checksum": 1,
                    "timestamp": "not a timestamp",
                }
            ],
        },
        {"channel": "trade", "type": "snapshot", "data": [{}]},
        {"channel": "trade", "type": "update", "data": [{"side": "hold"}]},
        {
            "channel": "trade",
            "type": "update",
            "data": [
                {
                    "symbol": "BTC/USD",
                    "side": "buy",
                    "price": 1.5,
                    "qty": 1.5,
                    "trade_id": 1,
                }
            ],
        },
        {
            "channel": "trade",
            "type": "update",
            "data": [
                {
                    "symbol": "BTC/USD",
                    "side": "buy",
                    "price": 1.5,
                    "qty": 1.5,
                    "trade_id": 1.5,
                    "timestamp": "2026-10-15T11:56:20.256336Z",
                }
            ],
        },
    ],
)
def test_malformed_payloads_raise_normalize_error(payload: object) -> None:
    with pytest.raises(NormalizeError):
        normalize(_raw(payload))


def test_normalize_is_pure_and_repeatable() -> None:
    raw = _raw(_book_payload())
    assert normalize(raw) == normalize(raw)


def test_normalized_message_keeps_the_raw_envelope() -> None:
    raw = _raw(_book_payload())
    message: NormalizedMessage = normalize(raw)
    assert message.raw is raw
