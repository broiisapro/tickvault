"""Pure normalizer for the Kraken WS v2 public websocket feed.

Kraken v2 has no sequence numbers; correctness relies on the per-message book
``checksum`` instead. This module turns one raw envelope into one
:class:`~tickvault.models.NormalizedMessage`: one :class:`BookUpdate` per
``data`` entry of a ``book`` message, trade rows for ``trade`` updates, and an
empty message for housekeeping frames (``status``, ``heartbeat`` and the
subscribe acknowledgements, which carry no ``channel`` field but name it in
``result.channel``).

Decimals
--------

Kraken sends prices and quantities as JSON numbers and the book checksum is
computed over their exact digit text, so the payload is decoded with
``json.loads(..., parse_float=Decimal)``: the source text becomes the number,
keeping ``0.00006120`` and ``81203.0`` exactly as sent (a float parse turns
the first into ``6.12e-05`` and breaks every checksum).

Timestamps
----------

Book ``data`` entries carry an RFC 3339 ``timestamp`` on updates; entries
without one (the venue omits it on snapshots) fall back to the raw envelope's
``recv_ts_ns``. Trade rows always carry one.

Symbols are normalized from the venue's ``BASE/QUOTE`` to ``BASE-QUOTE``.
Trade ``side`` is the taker side, exactly as the venue sends it.
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

_SYMBOL_PATTERN = re.compile(r"^[A-Z0-9]+/[A-Z0-9]+$")

_TRADE_SIDES: dict[str, TradeSide] = {"buy": "buy", "sell": "sell"}


def normalize(raw: RawMessage) -> NormalizedMessage:
    """Normalize one Kraken v2 raw message, exactly and purely.

    A ``control`` envelope yields the parsed control object and no records. A
    ``msg`` envelope always carries ``channel`` (subscribe acknowledgements
    name it in ``result.channel``) and never a sequence number. Unknown
    channels yield no records. Malformed JSON or a missing/invalid required
    field raises :class:`~tickvault.normalize.NormalizeError`.
    """
    if raw.kind == "control":
        return NormalizedMessage(raw=raw, control=_payload_object(raw))
    message = _payload_object(raw)
    if "method" in message:
        return NormalizedMessage(raw=raw, channel=_ack_channel(message))
    channel = message.get("channel")
    if not isinstance(channel, str):
        raise NormalizeError(f"kraken message has no 'channel': {raw.payload[:120]!r}")
    if channel == "book":
        return NormalizedMessage(raw=raw, channel=channel, book_updates=_book(raw, message))
    if channel == "trade":
        return NormalizedMessage(raw=raw, channel=channel, trades=_trades(raw, message))
    return NormalizedMessage(raw=raw, channel=channel)


def _payload_object(raw: RawMessage) -> dict[str, Any]:
    try:
        document = json.loads(raw.payload, parse_float=Decimal)
    except json.JSONDecodeError as exc:
        raise NormalizeError(f"kraken payload is not valid JSON: {raw.payload[:120]!r}") from exc
    if not isinstance(document, dict):
        raise NormalizeError("kraken payload must be a JSON object")
    return document


def _ack_channel(message: dict[str, Any]) -> str | None:
    result = message.get("result")
    channel = result.get("channel") if isinstance(result, dict) else None
    return channel if isinstance(channel, str) else None


def _decimal(value: object, field: str) -> Decimal:
    """Parse a venue number without ever going through ``float``.

    ``json.loads`` with ``parse_float=Decimal`` hands over already exact
    decimals in the venue's own digits; JSON integers and the rare decimal
    string are converted exactly as well. Floats are rejected outright.
    """
    if isinstance(value, bool) or not isinstance(value, (int, str, Decimal)):
        raise NormalizeError(f"kraken {field} must be a JSON number, got {value!r}")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise NormalizeError(f"kraken {field} is not a decimal: {value!r}") from exc
    if not parsed.is_finite():
        raise NormalizeError(f"kraken {field} is not finite: {value!r}")
    return parsed


def _timestamp(value: object, field: str) -> int:
    if not isinstance(value, str):
        raise NormalizeError(f"kraken {field} must be an RFC 3339 string, got {value!r}")
    try:
        return parse_rfc3339_ns(value)
    except ValueError as exc:
        raise NormalizeError(f"kraken {field} is not valid RFC 3339: {value!r}") from exc


def _symbol(value: object) -> str:
    if not isinstance(value, str) or _SYMBOL_PATTERN.fullmatch(value) is None:
        raise NormalizeError(f"kraken symbol is not BASE/QUOTE: {value!r}")
    return value.replace("/", "-")


def _data(message: dict[str, Any]) -> list[Any]:
    data = message.get("data")
    if not isinstance(data, list) or not data:
        raise NormalizeError("kraken message has no non-empty 'data' list")
    return data


def _message_type(message: dict[str, Any], expected: tuple[str, ...]) -> str:
    message_type = message.get("type")
    if not isinstance(message_type, str) or message_type not in expected:
        raise NormalizeError(
            f"kraken {message.get('channel')} type is not {'/'.join(expected)}: {message_type!r}"
        )
    return message_type


def _book(raw: RawMessage, message: dict[str, Any]) -> tuple[BookUpdate, ...]:
    """One :class:`BookUpdate` per ``data`` entry, in venue order.

    An entry without a ``timestamp`` field (venue snapshots may omit it) uses
    the raw envelope's ``recv_ts_ns``.
    """
    message_type = _message_type(message, ("snapshot", "update"))
    updates: list[BookUpdate] = []
    for entry in _data(message):
        if not isinstance(entry, dict):
            raise NormalizeError("kraken book entry must be a JSON object")
        timestamp = entry.get("timestamp")
        exchange_ts_ns = (
            raw.recv_ts_ns if timestamp is None else _timestamp(timestamp, "timestamp")
        )
        checksum = entry.get("checksum")
        if not isinstance(checksum, int) or isinstance(checksum, bool):
            raise NormalizeError(f"kraken book checksum must be an integer, got {checksum!r}")
        updates.append(
            BookUpdate(
                symbol=_symbol(entry.get("symbol")),
                is_snapshot=message_type == "snapshot",
                bids=_levels(entry.get("bids"), "bids"),
                asks=_levels(entry.get("asks"), "asks"),
                exchange_ts_ns=exchange_ts_ns,
                checksum=checksum,
            )
        )
    return tuple(updates)


def _levels(value: object, field: str) -> tuple[PriceLevel, ...]:
    if not isinstance(value, list):
        raise NormalizeError(f"kraken book {field} must be a JSON array, got {value!r}")
    levels: list[PriceLevel] = []
    for level in value:
        if not isinstance(level, dict):
            raise NormalizeError(f"kraken book {field} entry must be a JSON object")
        levels.append(
            (
                _decimal(level.get("price"), f"{field}.price"),
                _decimal(level.get("qty"), f"{field}.qty"),
            )
        )
    return tuple(levels)


def _trades(raw: RawMessage, message: dict[str, Any]) -> tuple[Trade, ...]:
    """Trade rows for one ``trade`` update, in message order."""
    _message_type(message, ("update",))
    trades: list[Trade] = []
    for entry in _data(message):
        if not isinstance(entry, dict):
            raise NormalizeError("kraken trade entry must be a JSON object")
        side_value = entry.get("side")
        side = _TRADE_SIDES.get(side_value) if isinstance(side_value, str) else None
        if side is None:
            raise NormalizeError(f"kraken trade side is not buy/sell: {side_value!r}")
        trades.append(
            Trade(
                venue=raw.venue,
                symbol=_symbol(entry.get("symbol")),
                exchange_ts_ns=_timestamp(entry.get("timestamp"), "timestamp"),
                recv_ts_ns=raw.recv_ts_ns,
                connection_id=raw.connection_id,
                ingest_seq=raw.ingest_seq,
                trade_id=_trade_id(entry.get("trade_id")),
                price=_decimal(entry.get("price"), "price"),
                size=_decimal(entry.get("qty"), "qty"),
                side=side,
                sequence=None,
            )
        )
    return tuple(trades)


def _trade_id(value: object) -> str:
    if isinstance(value, bool):
        raise NormalizeError(f"kraken trade id is not a positive integer: {value!r}")
    if isinstance(value, int):
        if value < 0:
            raise NormalizeError(f"kraken trade id is not a positive integer: {value!r}")
        return str(value)
    if isinstance(value, str) and value.isdigit():
        return value
    raise NormalizeError(f"kraken trade id is not a positive integer: {value!r}")
