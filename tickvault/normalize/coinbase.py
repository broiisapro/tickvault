"""Pure normalizer for the Coinbase Advanced Trade public websocket feed.

Advanced Trade carries a per-connection ``sequence_num`` on every message and
serves ``level2``, ``market_trades`` and ``heartbeats`` on one connection per
product. This module turns one raw envelope into one
:class:`~tickvault.models.NormalizedMessage`: book updates for ``l2_data``,
trades for ``market_trades`` and nothing but the sequence number for
housekeeping channels (``heartbeats``, ``subscriptions``), whose sequence
numbers must still reach the tracker.

Trade side
----------

The normalized ``trades.side`` is the **taker** side. Both Coinbase feeds
report the **maker** side: the Exchange feed documents ``side`` as "The side
of the maker order" and Advanced Trade documents it as "The maker's side of
the trade". A 60-second live capture on 2026-10-15 compared Exchange
``matches`` with Advanced Trade ``market_trades`` for BTC-USD: every common
trade id (1982 in one capture, 628 in another) carried the same side modulo
case, so the mapping inverts the venue value -- ``BUY`` (maker bought, taker
sold) becomes ``sell`` and ``SELL`` (maker sold, taker bought) becomes
``buy``.
"""

from __future__ import annotations

import json
import re
from decimal import Decimal, InvalidOperation
from typing import Any

from tickvault.models import (
    BookUpdate,
    NormalizedMessage,
    PriceLevel,
    RawMessage,
    Trade,
    TradeSide,
)
from tickvault.normalize import NormalizeError
from tickvault.timeutil import parse_rfc3339_ns

_SYMBOL_PATTERN = re.compile(r"^[A-Z0-9]+-[A-Z0-9]+$")

_TAKER_SIDE: dict[str, TradeSide] = {"BUY": "sell", "SELL": "buy"}


def normalize(raw: RawMessage) -> NormalizedMessage:
    """Normalize one Coinbase Advanced Trade raw message, exactly and purely.

    A ``control`` envelope yields the parsed control object and no records. A
    ``msg`` envelope always carries ``channel`` and ``sequence``; unknown
    channels yield no records. Malformed JSON or a missing/invalid required
    field raises :class:`~tickvault.normalize.NormalizeError`.
    """
    if raw.kind == "control":
        return NormalizedMessage(raw=raw, control=_payload_object(raw))
    message = _payload_object(raw)
    channel = message.get("channel")
    if not isinstance(channel, str):
        raise NormalizeError(f"coinbase message has no 'channel': {raw.payload[:120]!r}")
    sequence = _required_int(message, "sequence_num")
    if channel == "l2_data":
        return NormalizedMessage(
            raw=raw, channel=channel, sequence=sequence, book_updates=_l2_data(message)
        )
    if channel == "market_trades":
        return NormalizedMessage(
            raw=raw,
            channel=channel,
            sequence=sequence,
            trades=_market_trades(raw, sequence, message),
        )
    return NormalizedMessage(raw=raw, channel=channel, sequence=sequence)


def _payload_object(raw: RawMessage) -> dict[str, Any]:
    try:
        document = json.loads(raw.payload)
    except json.JSONDecodeError as exc:
        raise NormalizeError(f"coinbase payload is not valid JSON: {raw.payload[:120]!r}") from exc
    if not isinstance(document, dict):
        raise NormalizeError("coinbase payload must be a JSON object")
    return document


def _required_int(document: dict[str, Any], key: str) -> int:
    value = document.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise NormalizeError(f"coinbase {key!r} must be an integer, got {value!r}")
    return value


def _decimal(value: object, field: str) -> Decimal:
    """Parse a venue decimal string without ever going through ``float``."""
    if not isinstance(value, str):
        raise NormalizeError(f"coinbase {field} must be a decimal string, got {value!r}")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise NormalizeError(f"coinbase {field} is not a decimal: {value!r}") from exc
    if not parsed.is_finite():
        raise NormalizeError(f"coinbase {field} is not finite: {value!r}")
    return parsed


def _timestamp(value: object, field: str) -> int:
    if not isinstance(value, str):
        raise NormalizeError(f"coinbase {field} must be an RFC 3339 string, got {value!r}")
    try:
        return parse_rfc3339_ns(value)
    except ValueError as exc:
        raise NormalizeError(f"coinbase {field} is not valid RFC 3339: {value!r}") from exc


def _symbol(product_id: object) -> str:
    if not isinstance(product_id, str) or _SYMBOL_PATTERN.fullmatch(product_id) is None:
        raise NormalizeError(f"coinbase product id is not BASE-QUOTE: {product_id!r}")
    return product_id


def _events(document: dict[str, Any]) -> list[Any]:
    events = document.get("events")
    if not isinstance(events, list) or not events:
        raise NormalizeError("coinbase message has no non-empty 'events' list")
    return events


def _l2_data(document: dict[str, Any]) -> tuple[BookUpdate, ...]:
    """One snapshot :class:`BookUpdate` per snapshot event, one per event time otherwise."""
    updates: list[BookUpdate] = []
    for event in _events(document):
        if not isinstance(event, dict):
            raise NormalizeError("coinbase l2_data event must be a JSON object")
        event_type = event.get("type")
        symbol = _symbol(event.get("product_id"))
        levels = event.get("updates")
        if not isinstance(levels, list):
            raise NormalizeError("coinbase l2_data event has no 'updates' list")
        if event_type == "snapshot":
            bids, asks = _split_levels(levels)
            updates.append(
                BookUpdate(
                    symbol=symbol,
                    is_snapshot=True,
                    bids=bids,
                    asks=asks,
                    exchange_ts_ns=_timestamp(document.get("timestamp"), "timestamp"),
                )
            )
        elif event_type == "update":
            updates.extend(_update_batches(symbol, levels))
        else:
            raise NormalizeError(f"coinbase l2_data event type not snapshot/update: {event_type!r}")
    return tuple(updates)


def _split_levels(levels: list[Any]) -> tuple[tuple[PriceLevel, ...], tuple[PriceLevel, ...]]:
    bids: list[PriceLevel] = []
    asks: list[PriceLevel] = []
    for level in levels:
        side, price, size = _level(level)
        (bids if side == "bid" else asks).append((price, size))
    return tuple(bids), tuple(asks)


def _update_batches(symbol: str, levels: list[Any]) -> list[BookUpdate]:
    """Group an update event's levels into one batch per distinct ``event_time``.

    Batches are emitted in order of first appearance; within a batch bids and
    asks keep their venue order.
    """
    bids: dict[int, list[PriceLevel]] = {}
    asks: dict[int, list[PriceLevel]] = {}
    order: list[int] = []
    for level in levels:
        side, price, size = _level(level)
        event_time = _timestamp(level.get("event_time"), "event_time")
        if event_time not in bids:
            bids[event_time] = []
            asks[event_time] = []
            order.append(event_time)
        (bids if side == "bid" else asks)[event_time].append((price, size))
    return [
        BookUpdate(
            symbol=symbol,
            is_snapshot=False,
            bids=tuple(bids[event_time]),
            asks=tuple(asks[event_time]),
            exchange_ts_ns=event_time,
        )
        for event_time in order
    ]


def _level(level: object) -> tuple[str, Decimal, Decimal]:
    if not isinstance(level, dict):
        raise NormalizeError("coinbase l2_data level must be a JSON object")
    side = level.get("side")
    if side not in ("bid", "offer"):
        raise NormalizeError(f"coinbase l2_data side is not bid/offer: {side!r}")
    price = _decimal(level.get("price_level"), "price_level")
    size = _decimal(level.get("new_quantity"), "new_quantity")
    return side, price, size


def _market_trades(
    raw: RawMessage, sequence: int, document: dict[str, Any]
) -> tuple[Trade, ...]:
    """Trade rows for one message, ascending by integer ``trade_id``.

    Snapshot events are subscribe backfills; they are kept in the raw layer
    but emit no rows.
    """
    trades: list[Trade] = []
    for event in _events(document):
        if not isinstance(event, dict):
            raise NormalizeError("coinbase market_trades event must be a JSON object")
        event_type = event.get("type")
        if event_type == "snapshot":
            continue
        if event_type != "update":
            raise NormalizeError(
                f"coinbase market_trades event type not snapshot/update: {event_type!r}"
            )
        event_trades = event.get("trades")
        if not isinstance(event_trades, list):
            raise NormalizeError("coinbase market_trades event has no 'trades' list")
        for trade in event_trades:
            trades.append(_trade(raw, sequence, trade))
    trades.sort(key=lambda trade: int(trade.trade_id))
    return tuple(trades)


def _trade(raw: RawMessage, sequence: int, trade: object) -> Trade:
    if not isinstance(trade, dict):
        raise NormalizeError("coinbase market_trades trade must be a JSON object")
    venue_side = trade.get("side")
    side = _TAKER_SIDE.get(venue_side) if isinstance(venue_side, str) else None
    if side is None:
        raise NormalizeError(f"coinbase trade side is not BUY/SELL: {venue_side!r}")
    trade_id = trade.get("trade_id")
    if not isinstance(trade_id, str) or not trade_id.isdigit():
        raise NormalizeError(f"coinbase trade id is not a digit string: {trade_id!r}")
    return Trade(
        venue=raw.venue,
        symbol=_symbol(trade.get("product_id")),
        exchange_ts_ns=_timestamp(trade.get("time"), "time"),
        recv_ts_ns=raw.recv_ts_ns,
        connection_id=raw.connection_id,
        ingest_seq=raw.ingest_seq,
        trade_id=trade_id,
        price=_decimal(trade.get("price"), "price"),
        size=_decimal(trade.get("size"), "size"),
        side=side,
        sequence=sequence,
    )
