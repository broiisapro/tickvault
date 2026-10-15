"""Offline trades reconciliation against Tardis.dev reference data.

Tardis publishes per-day gzipped CSVs with columns
``exchange,symbol,timestamp,local_timestamp,id,side,price,amount`` whose
timestamps are microseconds UTC and whose ``id`` is the venue trade id.
:func:`reconcile` matches Tardis rows against TickVault ``norm/trades`` rows by
trade id inside an overlap window and reports unmatched ids and
price/size/side mismatches as exact ``Decimal`` comparisons.
"""

from __future__ import annotations

import csv
import gzip
import json
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import IO, cast

import duckdb

from tickvault.models import TradeSide
from tickvault.timeutil import ns_to_iso, parse_rfc3339_ns, us_to_ns

_TARDIS_HEADER = (
    "exchange",
    "symbol",
    "timestamp",
    "local_timestamp",
    "id",
    "side",
    "price",
    "amount",
)


@dataclass(frozen=True, slots=True)
class TardisTrade:
    """One reference trade from a Tardis CSV row."""

    exchange: str
    symbol: str
    exchange_ts_ns: int
    local_ts_ns: int
    trade_id: str
    side: TradeSide
    price: Decimal
    amount: Decimal


@dataclass(frozen=True, slots=True)
class TickVaultTrade:
    """One TickVault trade read from the normalized trades table."""

    venue: str
    symbol: str
    exchange_ts_ns: int
    recv_ts_ns: int
    connection_id: str
    ingest_seq: int
    trade_id: str
    side: TradeSide
    price: Decimal
    size: Decimal


@dataclass(frozen=True, slots=True)
class ReconcileWindow:
    """Inclusive exchange-time window in nanoseconds since the Unix epoch."""

    start_ns: int
    end_ns: int


@dataclass(frozen=True, slots=True)
class ReconcileReport:
    """Result of one reconciliation pass.

    ``missing_in_tickvault_ids``/``extra_in_tickvault_ids`` are the ids present
    on only one side; the price/size/side id tuples are subsets of the matched
    ids. All tuples are sorted.
    """

    window: ReconcileWindow | None
    tardis_trades: int
    tickvault_trades: int
    matched: int
    missing_in_tickvault_ids: tuple[str, ...]
    extra_in_tickvault_ids: tuple[str, ...]
    price_mismatch_ids: tuple[str, ...]
    size_mismatch_ids: tuple[str, ...]
    side_mismatch_ids: tuple[str, ...]


def window_from_iso(start: str, end: str) -> ReconcileWindow:
    """Build a window from two RFC 3339 strings (inclusive bounds)."""
    return ReconcileWindow(start_ns=parse_rfc3339_ns(start), end_ns=parse_rfc3339_ns(end))


def _open_text(path: Path) -> IO[str]:
    with path.open("rb") as probe:
        compressed = probe.read(2) == b"\x1f\x8b"
    if compressed:
        return gzip.open(path, "rt", encoding="utf-8", newline="")
    return path.open("rt", encoding="utf-8", newline="")


def _decimal(text: str, column: str, line_number: int) -> Decimal:
    try:
        return Decimal(text)
    except InvalidOperation as exc:
        raise ValueError(
            f"invalid decimal in column {column!r} at line {line_number}: {text!r}"
        ) from exc


def _side(text: str, where: object) -> TradeSide:
    if text not in ("buy", "sell"):
        raise ValueError(f"unexpected side at {where!r}: {text!r}")
    return cast(TradeSide, text)


def load_tardis_trades(path: str | Path) -> list[TardisTrade]:
    """Read a Tardis trades CSV (plain or gzip) into exact-decimals trades.

    Timestamps are microseconds in the file and become nanoseconds. Prices and
    amounts use :class:`decimal.Decimal` so exponent notation such as ``5e-8``
    keeps its exact value.
    """
    trades: list[TardisTrade] = []
    with _open_text(Path(path)) as handle:
        reader = csv.reader(handle)
        header = tuple(next(reader, ()))
        if header != _TARDIS_HEADER:
            raise ValueError(f"unexpected Tardis header: {header!r}")
        for line_number, row in enumerate(reader, start=2):
            if not row:
                continue
            if len(row) != len(_TARDIS_HEADER):
                raise ValueError(f"malformed Tardis row at line {line_number}: {row!r}")
            exchange, symbol, timestamp, local_timestamp, trade_id, side, price, amount = row
            trades.append(
                TardisTrade(
                    exchange=exchange,
                    symbol=symbol,
                    exchange_ts_ns=us_to_ns(int(timestamp)),
                    local_ts_ns=us_to_ns(int(local_timestamp)),
                    trade_id=trade_id,
                    side=_side(side, line_number),
                    price=_decimal(price, "price", line_number),
                    amount=_decimal(amount, "amount", line_number),
                )
            )
    return trades


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def load_tickvault_trades(
    root: str | Path,
    venue: str,
    symbol: str,
    start_ns: int | None = None,
    end_ns: int | None = None,
) -> list[TickVaultTrade]:
    """Read ``norm/trades`` rows for one venue/symbol with both bounds inclusive.

    Returns an empty list when the trades tree is missing or has no Parquet
    files. Rows are ordered by ``(exchange_ts_ns, connection_id, ingest_seq)``.
    """
    directory = Path(root) / "norm" / "trades"
    if not any(directory.rglob("*.parquet")):
        return []
    pattern = (directory / "**" / "*.parquet").as_posix()
    conditions = ["venue = ?", "symbol = ?"]
    parameters: list[object] = [venue, symbol]
    if start_ns is not None:
        conditions.append("exchange_ts_ns >= ?")
        parameters.append(start_ns)
    if end_ns is not None:
        conditions.append("exchange_ts_ns <= ?")
        parameters.append(end_ns)
    query = (
        "select venue, symbol, exchange_ts_ns, recv_ts_ns, connection_id, ingest_seq, "
        "trade_id, price, size, side from "
        f"read_parquet({_sql_quote(pattern)}, hive_partitioning=true) where "
        + " and ".join(conditions)
        + " order by exchange_ts_ns, connection_id, ingest_seq"
    )
    con = duckdb.connect()
    try:
        rows = con.execute(query, parameters).fetchall()
    finally:
        con.close()
    return [
        TickVaultTrade(
            venue=venue_value,
            symbol=symbol_value,
            exchange_ts_ns=exchange_ts_ns,
            recv_ts_ns=recv_ts_ns,
            connection_id=connection_id,
            ingest_seq=ingest_seq,
            trade_id=trade_id,
            side=_side(side, f"{connection_id}:{ingest_seq}"),
            price=Decimal(price),
            size=Decimal(size),
        )
        for (
            venue_value,
            symbol_value,
            exchange_ts_ns,
            recv_ts_ns,
            connection_id,
            ingest_seq,
            trade_id,
            price,
            size,
            side,
        ) in rows
    ]


def _resolve_window(
    tardis: Sequence[TardisTrade],
    tickvault: Sequence[TickVaultTrade],
    window: ReconcileWindow | tuple[int, int] | None,
) -> ReconcileWindow | None:
    if window is not None:
        if isinstance(window, ReconcileWindow):
            return window
        return ReconcileWindow(start_ns=window[0], end_ns=window[1])
    if not tardis or not tickvault:
        return None
    start_ns = max(
        min(trade.exchange_ts_ns for trade in tardis),
        min(trade.exchange_ts_ns for trade in tickvault),
    )
    end_ns = min(
        max(trade.exchange_ts_ns for trade in tardis),
        max(trade.exchange_ts_ns for trade in tickvault),
    )
    return ReconcileWindow(start_ns=start_ns, end_ns=end_ns)


def reconcile(
    tardis: Sequence[TardisTrade],
    tickvault: Sequence[TickVaultTrade],
    window: ReconcileWindow | tuple[int, int] | None = None,
) -> ReconcileReport:
    """Match ``tardis`` and ``tickvault`` trades by id inside a window.

    The window is the explicit ``window`` when given, otherwise the overlap of
    the two datasets: ``[max(first ts), min(last ts)]``. When a side is empty
    and no window is given, no window filtering happens. Trade ids are assumed
    unique per side, as Coinbase trade ids are.
    """
    resolved = _resolve_window(tardis, tickvault, window)
    tardis_in = [
        trade
        for trade in tardis
        if resolved is None
        or resolved.start_ns <= trade.exchange_ts_ns <= resolved.end_ns
    ]
    tickvault_in = [
        trade
        for trade in tickvault
        if resolved is None
        or resolved.start_ns <= trade.exchange_ts_ns <= resolved.end_ns
    ]
    tardis_by_id = {trade.trade_id: trade for trade in tardis_in}
    tickvault_by_id = {trade.trade_id: trade for trade in tickvault_in}
    matched_ids = sorted(set(tardis_by_id) & set(tickvault_by_id))
    return ReconcileReport(
        window=resolved,
        tardis_trades=len(tardis_in),
        tickvault_trades=len(tickvault_in),
        matched=len(matched_ids),
        missing_in_tickvault_ids=tuple(sorted(set(tardis_by_id) - set(tickvault_by_id))),
        extra_in_tickvault_ids=tuple(sorted(set(tickvault_by_id) - set(tardis_by_id))),
        price_mismatch_ids=tuple(
            trade_id
            for trade_id in matched_ids
            if tardis_by_id[trade_id].price != tickvault_by_id[trade_id].price
        ),
        size_mismatch_ids=tuple(
            trade_id
            for trade_id in matched_ids
            if tardis_by_id[trade_id].amount != tickvault_by_id[trade_id].size
        ),
        side_mismatch_ids=tuple(
            trade_id
            for trade_id in matched_ids
            if tardis_by_id[trade_id].side != tickvault_by_id[trade_id].side
        ),
    )


def report_to_json(report: ReconcileReport) -> str:
    """Render ``report`` as deterministic JSON with sorted keys."""
    payload = {
        "window": None
        if report.window is None
        else {
            "start": ns_to_iso(report.window.start_ns),
            "end": ns_to_iso(report.window.end_ns),
        },
        "tardis_trades": report.tardis_trades,
        "tickvault_trades": report.tickvault_trades,
        "matched": report.matched,
        "missing_in_tickvault": len(report.missing_in_tickvault_ids),
        "missing_in_tickvault_ids": list(report.missing_in_tickvault_ids),
        "extra_in_tickvault": len(report.extra_in_tickvault_ids),
        "extra_in_tickvault_ids": list(report.extra_in_tickvault_ids),
        "price_mismatches": len(report.price_mismatch_ids),
        "price_mismatch_ids": list(report.price_mismatch_ids),
        "size_mismatches": len(report.size_mismatch_ids),
        "size_mismatch_ids": list(report.size_mismatch_ids),
        "side_mismatches": len(report.side_mismatch_ids),
        "side_mismatch_ids": list(report.side_mismatch_ids),
    }
    return json.dumps(payload, sort_keys=True, indent=2) + "\n"


def _id_line(label: str, ids: tuple[str, ...]) -> str:
    if not ids:
        return f"{label}: 0"
    return f"{label}: {len(ids)} ({', '.join(ids)})"


def report_to_text(report: ReconcileReport) -> str:
    """Render ``report`` as a short human-readable summary."""
    if report.window is None:
        window = "none"
    else:
        window = f"{ns_to_iso(report.window.start_ns)} .. {ns_to_iso(report.window.end_ns)}"
    lines = [
        f"window: {window}",
        f"tardis trades: {report.tardis_trades}",
        f"tickvault trades: {report.tickvault_trades}",
        f"matched: {report.matched}",
        _id_line("missing in tickvault", report.missing_in_tickvault_ids),
        _id_line("extra in tickvault", report.extra_in_tickvault_ids),
        _id_line("price mismatches", report.price_mismatch_ids),
        _id_line("size mismatches", report.size_mismatch_ids),
        _id_line("side mismatches", report.side_mismatch_ids),
    ]
    return "\n".join(lines) + "\n"
