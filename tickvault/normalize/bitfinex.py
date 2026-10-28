"""Pure normalizer for the Bitfinex WS v2 public websocket feed.

Bitfinex v2 carries a per-connection sequence number on every frame (with the
``SEQ_ALL`` conf flag) and a CRC32 book checksum on ``cs`` frames (with
``OB_CHECKSUM``). One connection carries one symbol (decision A65): a ``book``
subscription at ``prec=P0``/``len=25`` and a ``trades`` subscription. This
module turns one raw envelope into one
:class:`~tickvault.models.NormalizedMessage`:

* a book snapshot frame → one ``BookUpdate(is_snapshot=True)`` holding all
  levels of the snapshot;
* a book update frame → one ``BookUpdate`` with the single changed level;
* a ``cs`` frame → one empty ``BookUpdate`` carrying the venue's checksum, so
  the processor compares it against the book state the frame refers to (the
  state after the update immediately before it) without inventing rows;
* a ``te`` frame → one executed :class:`~tickvault.models.Trade`;
* every other sequenced frame (``hb``, ``tu``, the trades snapshot) → no
  records but the sequence number, which must still reach the tracker (A25);
* dict frames (``info``, ``conf``, ``subscribed``, ``unsubscribed``) → no
  records and no sequence, because they do not consume one.

Decimals
--------

Prices and amounts arrive as JSON numbers and the checksum is computed over the
digits the venue's JavaScript client prints, so payloads are decoded with
``json.loads(..., parse_float=Decimal)``: the source text becomes the number
and ``0.00007191`` stays exact (a float parse would render it as
``7.191e-05`` and break the checksum). Amounts are signed: ``amount > 0`` is a
bid (or a taker buy), ``amount < 0`` an ask (or a taker sell), and the stored
size is ``abs(amount)``. A book update with ``count == 0`` removes the level
(size 0); the removal's ``±1`` amount carries no size information.

Channel state
-------------

Array frames name only a numeric channel id; the id is announced once, in the
``subscribed`` acknowledgement, and never repeated. The normalizer therefore
keeps a module-level mapping keyed by ``connection_id``:

* a ``connect`` control line replaces the entry, taking the connection's
  symbol from the declared ``symbols`` list (decision A29);
* a ``subscribed`` acknowledgement adds ``chanId → (channel, symbol)``;
* a ``disconnect`` control line drops the entry.

The state is rebuilt only from raw lines in canonical ``(recv_ts_ns,
connection_id, ingest_seq)`` order, so replay in a fresh process and replay in
a process that already replayed the same file produce identical output. A data
frame whose channel id has no ``subscribed`` acknowledgement yet raises
:class:`~tickvault.normalize.NormalizeError` rather than guessing.

Checksum accounting (decision A19)
----------------------------------

For Bitfinex the message that reveals a checksum failure is the ``cs`` frame,
which carries no levels at all: the checksummed state was already emitted by
the update frames before it. So nothing is double-counted or silently dropped
at the failure point, and the frames received while the symbol is SYNCING
count their levels in the next ``resync`` event's ``detail.deltas_dropped``
exactly like the other venues.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
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

_MS_TO_NS = 1_000_000
_SYMBOL_PATTERN = re.compile(r"^[A-Z0-9]+-[A-Z0-9]+$")
_BOOK = "book"
_TRADES = "trades"
_HEARTBEAT = "hb"
_CHECKSUM = "cs"
_EXECUTED_TRADE = "te"
_UPDATED_TRADE = "tu"


@dataclass(slots=True)
class _Channel:
    """One subscribed Bitfinex channel and the symbol it streams."""

    channel: str
    symbol: str | None


@dataclass(slots=True)
class _Connection:
    """Per-connection normalizer state, rebuilt from the raw lines."""

    symbols: tuple[str, ...] = ()
    channels: dict[int, _Channel] = field(default_factory=dict)

    def symbol_for(self, venue_symbol: object) -> str | None:
        """Resolve a ``subscribed`` acknowledgement's venue symbol.

        A connection carries one symbol (A65), so the declared symbol wins.
        The acknowledgement's own ``tBTCUSD``/``tUST:USD`` form is the fallback
        for recordings that predate declared symbols; a plain concatenated
        venue symbol (``tBTCUSD``) is ambiguous on its own and resolves only
        against a declared symbol.
        """
        if len(self.symbols) == 1:
            return self.symbols[0]
        if isinstance(venue_symbol, str):
            if venue_symbol.startswith("t") and ":" in venue_symbol:
                return venue_symbol[1:].replace(":", "-")
            for symbol in self.symbols:
                if venue_symbol in _venue_symbol_forms(symbol):
                    return symbol
        return None


_CONNECTIONS: dict[str, _Connection] = {}
"""Channel and symbol state per connection id, in raw-file order."""


def normalize(raw: RawMessage) -> NormalizedMessage:
    """Normalize one Bitfinex WS v2 raw message, exactly.

    A ``control`` envelope updates the per-connection state and yields the
    parsed control object with no records. A ``msg`` envelope is either a dict
    frame (no channel id, no sequence) or an array frame, whose channel id is
    resolved through the state of its connection. Malformed JSON or a
    missing/invalid required field raises
    :class:`~tickvault.normalize.NormalizeError`.
    """
    if raw.kind == "control":
        return _control(raw)
    document = _payload(raw)
    if isinstance(document, dict):
        return _dict_frame(raw, document)
    if not isinstance(document, list) or not document:
        raise NormalizeError(
            f"bitfinex frame must be a JSON object or array: {raw.payload[:120]!r}"
        )
    return _array_frame(raw, document)


# ----------------------------------------------------------------- payloads


def _payload(raw: RawMessage) -> Any:
    try:
        return json.loads(raw.payload, parse_float=Decimal)
    except json.JSONDecodeError as exc:
        raise NormalizeError(f"bitfinex payload is not valid JSON: {raw.payload[:120]!r}") from exc


def _control(raw: RawMessage) -> NormalizedMessage:
    """Track connect/disconnect state and pass the control payload through."""
    document = _payload(raw)
    if not isinstance(document, dict):
        raise NormalizeError("bitfinex control payload must be a JSON object")
    event = document.get("event")
    if event == "connect":
        _CONNECTIONS[raw.connection_id] = _Connection(symbols=_declared_symbols(document))
    elif event == "disconnect":
        _CONNECTIONS.pop(raw.connection_id, None)
    return NormalizedMessage(raw=raw, control=document)


def _declared_symbols(control: dict[str, Any]) -> tuple[str, ...]:
    """The deduplicated, sorted symbols a ``connect`` payload declares."""
    value = control.get("symbols")
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        return ()
    symbols: list[str] = []
    for item in value:
        if _SYMBOL_PATTERN.fullmatch(item) is None:
            raise NormalizeError(f"bitfinex connect symbol is not BASE-QUOTE: {item!r}")
        symbols.append(item)
    return tuple(sorted(set(symbols)))


def _venue_symbol_forms(symbol: str) -> tuple[str, ...]:
    """The venue spellings a normalized ``BASE-QUOTE`` symbol can take."""
    base, quote = symbol.split("-")
    return (f"t{base}{quote}", f"t{base}:{quote}")


def _dict_frame(raw: RawMessage, document: dict[str, Any]) -> NormalizedMessage:
    """Housekeeping and subscription frames: no records, no sequence."""
    event = document.get("event")
    if event == "subscribed":
        _register_channel(raw, document)
    return NormalizedMessage(raw=raw, channel=_dict_channel(document))


def _dict_channel(document: dict[str, Any]) -> str | None:
    channel = document.get("channel")
    return channel if isinstance(channel, str) else None


def _register_channel(raw: RawMessage, document: dict[str, Any]) -> None:
    """Learn ``chanId → (channel, symbol)`` from a ``subscribed`` acknowledgement.

    The mapping is kept across ``unsubscribed`` acknowledgements: Bitfinex keeps
    sending frames for a short while after an unsubscribe, and a resubscribe
    reuses the channel id.
    """
    channel_id = _int(document.get("chanId"), "subscribed.chanId")
    channel = _dict_channel(document)
    if channel is None:
        raise NormalizeError(f"bitfinex subscribed ack has no 'channel': {raw.payload[:120]!r}")
    connection = _CONNECTIONS.get(raw.connection_id)
    if connection is None:
        connection = _CONNECTIONS.setdefault(raw.connection_id, _Connection())
    connection.channels[channel_id] = _Channel(
        channel=channel, symbol=connection.symbol_for(document.get("symbol"))
    )


# -------------------------------------------------------------- array frames


def _array_frame(raw: RawMessage, frame: list[Any]) -> NormalizedMessage:
    channel_id = _int(frame[0], "channel id")
    channel = _channel(raw, channel_id)
    payload = frame[1] if len(frame) > 1 else None
    if isinstance(payload, str):
        if payload == _HEARTBEAT:
            return _sequenced(raw, channel.channel, _seq(frame, 2, "hb"))
        if payload == _CHECKSUM:
            return _checksum_frame(raw, frame, channel)
        if payload in (_EXECUTED_TRADE, _UPDATED_TRADE):
            return _trade_frame(raw, frame, channel, updated=payload == _UPDATED_TRADE)
        raise NormalizeError(f"bitfinex frame type is not hb/cs/te/tu: {payload!r}")
    if isinstance(payload, list):
        if channel.channel == _BOOK:
            return _book_frame(raw, frame, channel)
        if channel.channel == _TRADES:
            return _sequenced(raw, channel.channel, _seq(frame, 2, "trades snapshot"))
        # An unknown (but acknowledged) channel type still consumes a sequence
        # number and yields no records (A25).
        return _sequenced(raw, channel.channel, _seq(frame, 2, channel.channel))
    raise NormalizeError(f"bitfinex frame payload has an unknown shape: {raw.payload[:120]!r}")


def _channel(raw: RawMessage, channel_id: int) -> _Channel:
    connection = _CONNECTIONS.get(raw.connection_id)
    if connection is None:
        raise NormalizeError(
            f"bitfinex channel {channel_id} on a connection with no connect line: "
            f"{raw.connection_id!r}"
        )
    channel = connection.channels.get(channel_id)
    if channel is None:
        raise NormalizeError(
            f"bitfinex channel {channel_id} has no 'subscribed' ack yet: {raw.payload[:120]!r}"
        )
    return channel


def _sequenced(raw: RawMessage, channel: str, sequence: int) -> NormalizedMessage:
    """A frame that yields no records but consumes its sequence number."""
    return NormalizedMessage(raw=raw, channel=channel, sequence=sequence)


def _book_frame(raw: RawMessage, frame: list[Any], channel: _Channel) -> NormalizedMessage:
    """A book snapshot (list of levels) or a one-level book update."""
    sequence = _seq(frame, 2, "book")
    exchange_ts_ns = _ms(frame, 3, "book timestamp")
    symbol = _channel_symbol(raw, channel)
    payload = frame[1]
    if payload and isinstance(payload[0], list):
        bids, asks = _snapshot_levels(payload)
        update = BookUpdate(
            symbol=symbol,
            is_snapshot=True,
            bids=bids,
            asks=asks,
            exchange_ts_ns=exchange_ts_ns,
        )
    else:
        side, price, size = _level(payload)
        update = BookUpdate(
            symbol=symbol,
            is_snapshot=False,
            bids=((price, size),) if side == "bid" else (),
            asks=((price, size),) if side == "ask" else (),
            exchange_ts_ns=exchange_ts_ns,
        )
    return NormalizedMessage(
        raw=raw, channel=channel.channel, sequence=sequence, book_updates=(update,)
    )


def _checksum_frame(raw: RawMessage, frame: list[Any], channel: _Channel) -> NormalizedMessage:
    """The ``cs`` frame: an empty update carrying the checksum of the state before it.

    The frame's shape is ``[chanId, "cs", checksum, sequence, timestamp_ms]``:
    the checksum sits where the other frames keep their payload and the
    sequence moves one slot to the right.
    """
    if channel.channel != _BOOK:
        raise NormalizeError(f"bitfinex checksum frame on a {channel.channel!r} channel")
    checksum = _int(frame[2], "checksum")
    update = BookUpdate(
        symbol=_channel_symbol(raw, channel),
        is_snapshot=False,
        bids=(),
        asks=(),
        exchange_ts_ns=_ms(frame, 4, "checksum timestamp"),
        checksum=checksum,
    )
    return NormalizedMessage(
        raw=raw,
        channel=channel.channel,
        sequence=_seq(frame, 3, "checksum"),
        book_updates=(update,),
    )


def _trade_frame(
    raw: RawMessage, frame: list[Any], channel: _Channel, *, updated: bool
) -> NormalizedMessage:
    """``te`` yields a trade row; ``tu`` is the same trade finalised, no row.

    Both carry their sequence number (A25); only ``te`` is normalized, exactly
    like the Coinbase trades backfill.
    """
    if channel.channel != _TRADES:
        raise NormalizeError(f"bitfinex trade frame on a {channel.channel!r} channel")
    sequence = _seq(frame, 3, "trade")
    if updated:
        return _sequenced(raw, channel.channel, sequence)
    entry = frame[2]
    if not isinstance(entry, list) or len(entry) != 4:
        raise NormalizeError(f"bitfinex te entry must be [id, ts, amount, price]: {entry!r}")
    trade_id = _trade_id(entry[0])
    amount = _decimal(entry[2], "trade amount")
    price = _decimal(entry[3], "trade price")
    if amount == 0:
        raise NormalizeError("bitfinex trade amount must not be zero")
    side: TradeSide = "buy" if amount > 0 else "sell"
    trade = Trade(
        venue=raw.venue,
        symbol=_channel_symbol(raw, channel),
        exchange_ts_ns=_ms(entry, 1, "trade timestamp"),
        recv_ts_ns=raw.recv_ts_ns,
        connection_id=raw.connection_id,
        ingest_seq=raw.ingest_seq,
        trade_id=trade_id,
        price=price,
        size=abs(amount),
        side=side,
        sequence=sequence,
    )
    return NormalizedMessage(
        raw=raw, channel=channel.channel, sequence=sequence, trades=(trade,)
    )


def _channel_symbol(raw: RawMessage, channel: _Channel) -> str:
    if channel.symbol is None:
        raise NormalizeError(
            f"bitfinex {channel.channel} channel has no symbol; "
            f"its connect line declared none: {raw.payload[:120]!r}"
        )
    return channel.symbol


# -------------------------------------------------------------------- levels


def _snapshot_levels(
    levels: list[Any],
) -> tuple[tuple[PriceLevel, ...], tuple[PriceLevel, ...]]:
    """Split a snapshot's signed levels into bids and asks, keeping venue order."""
    bids: list[PriceLevel] = []
    asks: list[PriceLevel] = []
    for level in levels:
        side, price, size = _level(level)
        (bids if side == "bid" else asks).append((price, size))
    return tuple(bids), tuple(asks)


def _level(level: Any) -> tuple[str, Decimal, Decimal]:
    """Parse one ``[price, count, amount]`` entry into ``(side, price, size)``.

    ``count == 0`` removes the level (size 0); otherwise the size is the
    magnitude of the signed amount.
    """
    if not isinstance(level, list) or len(level) != 3:
        raise NormalizeError(f"bitfinex book level must be [price, count, amount]: {level!r}")
    price = _decimal(level[0], "book price")
    count = _int(level[1], "book count")
    amount = _decimal(level[2], "book amount")
    if amount == 0:
        raise NormalizeError(f"bitfinex book amount must not be zero: {level!r}")
    side = "bid" if amount > 0 else "ask"
    size = Decimal(0) if count == 0 else abs(amount)
    return side, price, size


# ---------------------------------------------------------------- primitives


def _decimal(value: object, field_name: str) -> Decimal:
    """Parse a venue number without ever going through ``float``.

    ``json.loads`` with ``parse_float=Decimal`` hands over already exact
    decimals in the venue's own digits; JSON integers are converted exactly as
    well. Floats are rejected outright.
    """
    if isinstance(value, bool) or not isinstance(value, (int, str, Decimal)):
        raise NormalizeError(f"bitfinex {field_name} must be a JSON number, got {value!r}")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise NormalizeError(f"bitfinex {field_name} is not a decimal: {value!r}") from exc
    if not parsed.is_finite():
        raise NormalizeError(f"bitfinex {field_name} is not finite: {value!r}")
    return parsed


def _int(value: object, field_name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise NormalizeError(f"bitfinex {field_name} must be an integer, got {value!r}")
    return value


def _seq(frame: list[Any], index: int, frame_name: str) -> int:
    if len(frame) <= index:
        raise NormalizeError(f"bitfinex {frame_name} frame has no sequence number: {frame!r}")
    return _int(frame[index], f"{frame_name} sequence")


def _ms(entry: list[Any], index: int, field_name: str) -> int:
    """A venue epoch-millisecond field as int64 nanoseconds."""
    if len(entry) <= index:
        raise NormalizeError(f"bitfinex {field_name} is missing: {entry!r}")
    return _int(entry[index], field_name) * _MS_TO_NS


def _trade_id(value: object) -> str:
    if isinstance(value, bool):
        raise NormalizeError(f"bitfinex trade id is not a positive integer: {value!r}")
    if isinstance(value, int):
        if value < 0:
            raise NormalizeError(f"bitfinex trade id is not a positive integer: {value!r}")
        return str(value)
    if isinstance(value, str) and value.isdigit():
        return value
    raise NormalizeError(f"bitfinex trade id is not a positive integer: {value!r}")
