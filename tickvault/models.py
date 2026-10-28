"""Shared data contracts for TickVault.

This module is the single source of truth for the record types and the Arrow
schemas of the normalized layer. Prices and sizes are ``decimal.Decimal``,
never ``float``, so venue text such as ``0.00006120`` reaches
``decimal128(38, 18)`` without rounding; timestamps are int64 nanoseconds since
the Unix epoch, UTC.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

import pyarrow as pa

type RawKind = Literal["msg", "control"]
"""``msg`` for a venue frame, ``control`` for a TickVault lifecycle record."""

type Side = Literal["bid", "ask"]
"""Book side, normalized to ``bid`` / ``ask`` for every venue."""

type TradeSide = Literal["buy", "sell"]
"""Trade side, always the taker side."""

type EventType = Literal[
    "gap",
    "resync",
    "checksum_failure",
    "invariant_violation",
    "reconnect",
    "disconnect",
]

type SnapshotSource = Literal["venue", "sample"]
"""``venue`` for a venue-sent snapshot, ``sample`` for a TickVault top-10 sample."""

type PriceLevel = tuple[Decimal, Decimal]
"""One book level: ``(price, size)``, both exact decimals."""


@dataclass(frozen=True, slots=True)
class RawMessage:
    """One raw envelope line exactly as persisted in the raw layer.

    ``payload`` is the websocket text frame unmodified (or, for ``control``
    records, a JSON object string describing the lifecycle event).
    """

    venue: str
    connection_id: str
    ingest_seq: int
    recv_ts_ns: int
    kind: RawKind
    payload: str


@dataclass(frozen=True, slots=True)
class Trade:
    """One trade row for the ``trades`` table."""

    venue: str
    symbol: str
    exchange_ts_ns: int
    recv_ts_ns: int
    connection_id: str
    ingest_seq: int
    trade_id: str
    price: Decimal
    size: Decimal
    side: TradeSide
    sequence: int | None = None


@dataclass(frozen=True, slots=True)
class BookDelta:
    """One absolute book-level change for the ``book_deltas`` table.

    ``size`` is the new absolute size of the level; ``0`` removes it.
    """

    venue: str
    symbol: str
    exchange_ts_ns: int
    recv_ts_ns: int
    connection_id: str
    ingest_seq: int
    side: Side
    price: Decimal
    size: Decimal
    sequence: int | None = None


@dataclass(frozen=True, slots=True)
class BookSnapshotLevel:
    """One level of a snapshot for the ``book_snapshots`` table."""

    venue: str
    symbol: str
    exchange_ts_ns: int
    recv_ts_ns: int
    connection_id: str
    ingest_seq: int
    snapshot_id: str
    source: SnapshotSource
    side: Side
    level: int
    price: Decimal
    size: Decimal


@dataclass(frozen=True, slots=True)
class Event:
    """One data-quality event for the ``events`` table.

    ``detail`` is JSON text with sorted keys.
    """

    venue: str
    recv_ts_ns: int
    connection_id: str
    ingest_seq: int
    event_type: EventType
    symbol: str | None = None
    exchange_ts_ns: int | None = None
    channel: str | None = None
    expected_seq: int | None = None
    received_seq: int | None = None
    detail: str = "{}"


@dataclass(frozen=True, slots=True)
class BookUpdate:
    """A book snapshot or incremental update parsed from one venue message."""

    symbol: str
    is_snapshot: bool
    bids: tuple[PriceLevel, ...]
    asks: tuple[PriceLevel, ...]
    exchange_ts_ns: int
    checksum: int | None = None


@dataclass(frozen=True, slots=True)
class NormalizedMessage:
    """The pure output of a venue normalizer for one raw message.

    Carries the originating :class:`RawMessage` so downstream rows can be
    stamped with ``recv_ts_ns``, ``connection_id`` and ``ingest_seq``.
    """

    raw: RawMessage
    channel: str | None = None
    sequence: int | None = None
    trades: tuple[Trade, ...] = ()
    book_updates: tuple[BookUpdate, ...] = ()
    control: dict[str, object] | None = None


type NormalizedRecord = Trade | BookDelta | BookSnapshotLevel | Event
"""Any record type that can be written to a normalized table."""

_DECIMAL_TYPE = pa.decimal128(38, 18)
_INT64 = pa.int64()
_INT16 = pa.int16()
_STRING = pa.string()


def _field(name: str, data_type: pa.DataType, *, nullable: bool = True) -> pa.Field[pa.DataType]:
    return pa.field(name, data_type, nullable=nullable)


TRADES_SCHEMA = pa.schema(
    [
        _field("venue", _STRING, nullable=False),
        _field("symbol", _STRING, nullable=False),
        _field("exchange_ts_ns", _INT64, nullable=False),
        _field("recv_ts_ns", _INT64, nullable=False),
        _field("connection_id", _STRING, nullable=False),
        _field("ingest_seq", _INT64, nullable=False),
        _field("trade_id", _STRING, nullable=False),
        _field("price", _DECIMAL_TYPE, nullable=False),
        _field("size", _DECIMAL_TYPE, nullable=False),
        _field("side", _STRING, nullable=False),
        _field("sequence", _INT64, nullable=True),
    ]
)

BOOK_DELTAS_SCHEMA = pa.schema(
    [
        _field("venue", _STRING, nullable=False),
        _field("symbol", _STRING, nullable=False),
        _field("exchange_ts_ns", _INT64, nullable=False),
        _field("recv_ts_ns", _INT64, nullable=False),
        _field("connection_id", _STRING, nullable=False),
        _field("ingest_seq", _INT64, nullable=False),
        _field("side", _STRING, nullable=False),
        _field("price", _DECIMAL_TYPE, nullable=False),
        _field("size", _DECIMAL_TYPE, nullable=False),
        _field("sequence", _INT64, nullable=True),
    ]
)

BOOK_SNAPSHOTS_SCHEMA = pa.schema(
    [
        _field("venue", _STRING, nullable=False),
        _field("symbol", _STRING, nullable=False),
        _field("exchange_ts_ns", _INT64, nullable=False),
        _field("recv_ts_ns", _INT64, nullable=False),
        _field("connection_id", _STRING, nullable=False),
        _field("ingest_seq", _INT64, nullable=False),
        _field("snapshot_id", _STRING, nullable=False),
        _field("source", _STRING, nullable=False),
        _field("side", _STRING, nullable=False),
        _field("level", _INT16, nullable=False),
        _field("price", _DECIMAL_TYPE, nullable=False),
        _field("size", _DECIMAL_TYPE, nullable=False),
    ]
)

EVENTS_SCHEMA = pa.schema(
    [
        _field("venue", _STRING, nullable=False),
        _field("symbol", _STRING, nullable=True),
        _field("exchange_ts_ns", _INT64, nullable=True),
        _field("recv_ts_ns", _INT64, nullable=False),
        _field("connection_id", _STRING, nullable=False),
        _field("ingest_seq", _INT64, nullable=False),
        _field("event_type", _STRING, nullable=False),
        _field("channel", _STRING, nullable=True),
        _field("expected_seq", _INT64, nullable=True),
        _field("received_seq", _INT64, nullable=True),
        _field("detail", _STRING, nullable=False),
    ]
)

TABLES: dict[str, pa.Schema] = {
    "trades": TRADES_SCHEMA,
    "book_deltas": BOOK_DELTAS_SCHEMA,
    "book_snapshots": BOOK_SNAPSHOTS_SCHEMA,
    "events": EVENTS_SCHEMA,
}

FEATURES_TABLE = "features"
"""Name of the derived feature table, written outside the normalized layer."""

_FLOAT64 = pa.float64()

FEATURES_SCHEMA = pa.schema(
    [
        _field("venue", _STRING, nullable=False),
        _field("symbol", _STRING, nullable=False),
        _field("resolution", _STRING, nullable=False),
        _field("bar_start_ns", _INT64, nullable=False),
        _field("bar_end_ns", _INT64, nullable=False),
        _field("best_bid", _FLOAT64, nullable=True),
        _field("best_ask", _FLOAT64, nullable=True),
        _field("mid", _FLOAT64, nullable=True),
        _field("spread_bps", _FLOAT64, nullable=True),
        _field("depth_imbalance_5", _FLOAT64, nullable=True),
        _field("book_sample_ts_ns", _INT64, nullable=True),
        _field("trade_count", _INT64, nullable=False),
        _field("volume", _FLOAT64, nullable=False),
        _field("signed_volume", _FLOAT64, nullable=False),
        _field("vwap", _FLOAT64, nullable=True),
        _field("realized_vol", _FLOAT64, nullable=True),
    ]
)
"""Schema of the derived features table (decision A68).

Features are research values, so they are ``float64`` (the exact
``decimal128(38, 18)`` prices stay in the normalized tables) and counts are
``int64``. The table is deliberately **not** part of :data:`TABLES`: it is
derived analytics written under ``features/``, so compaction, manifests and QA
are unchanged.
"""


def to_arrow(table: str | pa.Schema, records: Sequence[NormalizedRecord]) -> pa.Table:
    """Convert ``records`` to a :class:`pyarrow.Table` with ``table``'s schema.

    ``table`` is either a key of :data:`TABLES` or a schema itself. Column
    order follows the schema and row order follows ``records``; ``Decimal``
    values are converted to ``decimal128(38, 18)`` without going through
    ``float``.
    """
    schema = TABLES[table] if isinstance(table, str) else table
    columns = [
        pa.array([getattr(record, schema_field.name) for record in records], type=schema_field.type)
        for schema_field in schema
    ]
    return pa.Table.from_arrays(columns, schema=schema)
