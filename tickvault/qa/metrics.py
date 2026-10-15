"""Data-quality metrics over the normalized Parquet tree.

:func:`compute_qa` reads the four normalized tables directly with DuckDB
(``read_parquet(..., hive_partitioning=true)``) and aggregates one
:class:`QARow` per venue/symbol/day. Missing tables and empty partitions count
as empty. Nothing here depends on wall-clock time, so repeated runs over the
same tree are deterministic.

``date`` restricts every table to one exchange-time partition day before the
metrics are computed, including the "later resync" lookup used for
``unexplained``. With a date filter, a resync in the next day's partition whose
``recv_ts_ns`` is at most 300 s past the end of the filtered day also explains
an anomaly of that day (decision A31). A venue/symbol/day row with messages
but no venue snapshot is flagged ``book_missing`` (decision A30). Raw-layer
message counting is intentionally out of scope here; Epic 2 adds it when a raw
tree is present (decision A24).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from datetime import date as Date
from pathlib import Path

import duckdb

_NS_PER_SECOND = 1_000_000_000
_CROSS_MIDNIGHT_GRACE_NS = 300 * _NS_PER_SECOND
_TABLE_NAMES = ("trades", "book_deltas", "book_snapshots", "events")
_COUNTED_TABLES = ("trades", "book_deltas", "book_snapshots")
_ANOMALY_EVENT_TYPES = ("gap", "checksum_failure", "invariant_violation")
_UNEXPLAINED_EVENT_TYPES = (*_ANOMALY_EVENT_TYPES, "resync")

type QAGroup = tuple[str, str, str]
"""A venue/symbol/day key; day is the partition date as ``YYYY-MM-DD``."""

type EventOrder = tuple[int, str, int]
"""Canonical event order: ``(recv_ts_ns, connection_id, ingest_seq)``."""

type EventRow = tuple[str, str, str, str, int, str, int]
"""One row of the unexplained lookup: venue, symbol, day, type, order parts."""


@dataclass(frozen=True, slots=True)
class QARow:
    """One venue/symbol/day row of the quality report.

    Counts are exact integers; ``lag_p50_ms``/``lag_p99_ms`` are rounded to
    three decimals and ``coverage`` to four. Lag and coverage default to
    ``0.0`` when a row has no trades or book deltas. ``book_missing`` is true
    when messages arrived but no venue snapshot was stored (decision A30).
    """

    venue: str
    symbol: str
    date: str
    messages: int
    trades: int
    book_deltas: int
    venue_snapshots: int
    gaps: int
    resyncs: int
    checksum_failures: int
    reconnects: int
    invariant_violations: int
    lag_p50_ms: float
    lag_p99_ms: float
    coverage: float
    unexplained: int
    book_missing: bool


@dataclass
class _Partial:
    """Mutable per-group accumulator filled in by the individual queries."""

    messages: int = 0
    trades: int = 0
    book_deltas: int = 0
    venue_snapshots: int = 0
    gaps: int = 0
    resyncs: int = 0
    checksum_failures: int = 0
    reconnects: int = 0
    invariant_violations: int = 0
    lag_p50_ms: float = 0.0
    lag_p99_ms: float = 0.0
    coverage: float = 0.0
    unexplained: int = 0


def _normalize_date(value: str | Date | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, Date):
        return value.isoformat()
    return Date.fromisoformat(value).isoformat()


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _create_views(
    con: duckdb.DuckDBPyConnection, root: Path, iso_date: str | None
) -> set[str]:
    """Create one view per non-empty normalized table and return their names."""
    available: set[str] = set()
    for table in _TABLE_NAMES:
        directory = root / "norm" / table
        if not any(directory.rglob("*.parquet")):
            continue
        pattern = (directory / "**" / "*.parquet").as_posix()
        where = f" where date = DATE {_sql_quote(iso_date)}" if iso_date else ""
        con.execute(
            f"create or replace view {table} as select * from "
            f"read_parquet({_sql_quote(pattern)}, hive_partitioning=true){where}"
        )
        available.add(table)
    return available


def _partials_for(
    accumulators: dict[QAGroup, _Partial], venue: str, symbol: str, day: str
) -> _Partial:
    return accumulators.setdefault((venue, symbol, day), _Partial())


def _add_trade_delta_metrics(
    con: duckdb.DuckDBPyConnection,
    available: set[str],
    accumulators: dict[QAGroup, _Partial],
) -> None:
    parts = [
        "select venue, symbol, cast(date as varchar) as day, exchange_ts_ns, "
        f"recv_ts_ns - exchange_ts_ns as lag_ns, {_sql_quote(table)} as src from {table}"
        for table in ("trades", "book_deltas")
        if table in available
    ]
    rows = con.execute(
        f"""
        select venue, symbol, day,
            count(*) filter (where src = 'trades') as trades,
            count(*) filter (where src = 'book_deltas') as book_deltas,
            quantile_cont(lag_ns, 0.5) / 1000000.0 as lag_p50_ms,
            quantile_cont(lag_ns, 0.99) / 1000000.0 as lag_p99_ms,
            count(distinct exchange_ts_ns // {_NS_PER_SECOND}) as seconds,
            min(exchange_ts_ns) // {_NS_PER_SECOND} as first_second,
            max(exchange_ts_ns) // {_NS_PER_SECOND} as last_second
        from ({_union(parts)}) group by venue, symbol, day
        """
    ).fetchall()
    for venue, symbol, day, trades, book_deltas, lag50, lag99, seconds, first, last in rows:
        partial = _partials_for(accumulators, venue, symbol, day)
        partial.trades = trades
        partial.book_deltas = book_deltas
        partial.lag_p50_ms = round(lag50, 3)
        partial.lag_p99_ms = round(lag99, 3)
        partial.coverage = round(seconds / (last - first + 1), 4)


def _add_message_counts(
    con: duckdb.DuckDBPyConnection,
    available: set[str],
    accumulators: dict[QAGroup, _Partial],
) -> None:
    parts = [
        "select distinct venue, symbol, cast(date as varchar) as day, "
        f"connection_id, ingest_seq from {table}"
        for table in _COUNTED_TABLES
        if table in available
    ]
    if "events" in available:
        parts.append(
            "select distinct venue, symbol, cast(date as varchar) as day, "
            "connection_id, ingest_seq from events where symbol is not null"
        )
    rows = con.execute(
        f"select venue, symbol, day, count(*) as messages from ("
        f"select distinct venue, symbol, day, connection_id, ingest_seq "
        f"from ({_union(parts)})) group by venue, symbol, day"
    ).fetchall()
    for venue, symbol, day, messages in rows:
        _partials_for(accumulators, venue, symbol, day).messages = messages


def _add_venue_snapshots(
    con: duckdb.DuckDBPyConnection, accumulators: dict[QAGroup, _Partial]
) -> None:
    rows = con.execute(
        "select venue, symbol, cast(date as varchar) as day, count(distinct snapshot_id) "
        "from book_snapshots where source = 'venue' group by venue, symbol, day"
    ).fetchall()
    for venue, symbol, day, snapshots in rows:
        _partials_for(accumulators, venue, symbol, day).venue_snapshots = snapshots


def _add_event_counts(
    con: duckdb.DuckDBPyConnection, accumulators: dict[QAGroup, _Partial]
) -> None:
    rows = con.execute(
        """
        select venue, symbol, cast(date as varchar) as day,
            count(*) filter (where event_type = 'gap') as gaps,
            count(*) filter (where event_type = 'resync') as resyncs,
            count(*) filter (where event_type = 'checksum_failure') as checksum_failures,
            count(*) filter (where event_type = 'reconnect') as reconnects,
            count(*) filter (where event_type = 'invariant_violation') as invariant_violations
        from events where symbol is not null group by venue, symbol, day
        """
    ).fetchall()
    for venue, symbol, day, gaps, resyncs, checksums, reconnects, violations in rows:
        partial = _partials_for(accumulators, venue, symbol, day)
        partial.gaps = gaps
        partial.resyncs = resyncs
        partial.checksum_failures = checksums
        partial.reconnects = reconnects
        partial.invariant_violations = violations


def _event_rows(con: duckdb.DuckDBPyConnection) -> list[EventRow]:
    types = ", ".join(_sql_quote(event_type) for event_type in _UNEXPLAINED_EVENT_TYPES)
    return con.execute(
        f"""
        select venue, symbol, cast(date as varchar) as day, event_type,
            recv_ts_ns, connection_id, ingest_seq
        from events
        where symbol is not null and event_type in ({types})
        order by venue, symbol, day, recv_ts_ns, connection_id, ingest_seq
        """
    ).fetchall()


def _day_start_ns(day: str) -> int:
    parsed = Date.fromisoformat(day)
    return int(datetime(parsed.year, parsed.month, parsed.day, tzinfo=UTC).timestamp()) * (
        _NS_PER_SECOND
    )


def _cross_midnight_resync_rows(
    con: duckdb.DuckDBPyConnection, root: Path, iso_date: str
) -> list[EventRow]:
    """Resyncs in the next day's partition that explain a previous day.

    Decision A31: with a date filter, a resync for the same venue/symbol whose
    ``recv_ts_ns`` is at most 300 s past the end of the filtered day explains
    an anomaly of that day. The rows are keyed to ``iso_date`` so they land in
    the filtered day's group. A missing next-day partition counts as empty.
    """
    directory = root / "norm" / "events"
    if not any(directory.rglob("*.parquet")):
        return []
    pattern = (directory / "**" / "*.parquet").as_posix()
    next_day = (Date.fromisoformat(iso_date) + timedelta(days=1)).isoformat()
    deadline_ns = _day_start_ns(next_day) + _CROSS_MIDNIGHT_GRACE_NS
    rows = con.execute(
        f"""
        select venue, symbol, recv_ts_ns, connection_id, ingest_seq
        from read_parquet({_sql_quote(pattern)}, hive_partitioning=true)
        where date = DATE {_sql_quote(next_day)}
            and event_type = 'resync'
            and symbol is not null
            and recv_ts_ns <= {deadline_ns}
        """
    ).fetchall()
    return [
        (venue, symbol, iso_date, "resync", recv_ts_ns, connection_id, ingest_seq)
        for venue, symbol, recv_ts_ns, connection_id, ingest_seq in rows
    ]


def _add_unexplained(
    con: duckdb.DuckDBPyConnection,
    root: Path,
    iso_date: str | None,
    accumulators: dict[QAGroup, _Partial],
) -> None:
    """Count anomalies with no strictly later resync for the same group.

    Events are ordered by ``(recv_ts_ns, connection_id, ingest_seq)``; a resync
    explains every anomaly before it, and a resync recorded earlier never
    explains a later anomaly. An anomaly and a resync at the exact same order
    key are treated as unordered, so the anomaly stays unexplained. With a date
    filter, the next day's first 300 s of resyncs join the lookup (A31).
    """
    rows = _event_rows(con)
    if iso_date is not None:
        rows.extend(_cross_midnight_resync_rows(con, root, iso_date))
        rows.sort(key=lambda row: (row[0], row[1], row[2], row[4], row[5], row[6]))
    pending: dict[QAGroup, list[EventOrder]] = {}
    for venue, symbol, day, event_type, recv_ts_ns, connection_id, ingest_seq in rows:
        key = (venue, symbol, day)
        order: EventOrder = (recv_ts_ns, connection_id, ingest_seq)
        if event_type == "resync":
            pending[key] = [entry for entry in pending.get(key, []) if entry >= order]
        else:
            pending.setdefault(key, []).append(order)
    for key, entries in pending.items():
        if entries:
            _partials_for(accumulators, *key).unexplained = len(entries)


def _union(parts: list[str]) -> str:
    return " union all ".join(parts)


def _to_row(venue: str, symbol: str, day: str, partial: _Partial) -> QARow:
    return QARow(
        venue=venue,
        symbol=symbol,
        date=day,
        messages=partial.messages,
        trades=partial.trades,
        book_deltas=partial.book_deltas,
        venue_snapshots=partial.venue_snapshots,
        gaps=partial.gaps,
        resyncs=partial.resyncs,
        checksum_failures=partial.checksum_failures,
        reconnects=partial.reconnects,
        invariant_violations=partial.invariant_violations,
        lag_p50_ms=partial.lag_p50_ms,
        lag_p99_ms=partial.lag_p99_ms,
        coverage=partial.coverage,
        unexplained=partial.unexplained,
        book_missing=partial.messages > 0 and partial.venue_snapshots == 0,
    )


def compute_qa(root: str | Path, date: str | Date | None = None) -> list[QARow]:
    """Aggregate quality metrics for every venue/symbol/day in ``root``.

    ``root`` is a data root containing ``norm/<table>/venue=.../date=.../hour=...``.
    ``date`` (``YYYY-MM-DD`` string or :class:`datetime.date`) restricts the
    computation to one partition day and, for that day, reads the next day's
    first 300 s of resyncs (decision A31). Rows are sorted by venue, symbol,
    date.
    """
    iso_date = _normalize_date(date)
    accumulators: dict[QAGroup, _Partial] = {}
    root_path = Path(root)
    con = duckdb.connect()
    try:
        available = _create_views(con, root_path, iso_date)
        if {"trades", "book_deltas"} & available:
            _add_trade_delta_metrics(con, available, accumulators)
        if available:
            _add_message_counts(con, available, accumulators)
        if "book_snapshots" in available:
            _add_venue_snapshots(con, accumulators)
        if "events" in available:
            _add_event_counts(con, accumulators)
            _add_unexplained(con, root_path, iso_date, accumulators)
    finally:
        con.close()
    return [
        _to_row(venue, symbol, day, accumulators[(venue, symbol, day)])
        for venue, symbol, day in sorted(accumulators)
    ]
