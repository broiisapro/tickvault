# ruff: noqa: RUF002
"""Feature computation for the derived analytics table (decision A67).

Definitions, verbatim:

Per venue/symbol/day at resolutions ``1s`` and ``1m``. A bar is the half-open
interval ``[bar_start_ns, bar_end_ns)``; a bar uses only rows with
``exchange_ts_ns < bar_end_ns`` (stricter than the PRD's "at or before the
close"). Columns: ``venue``, ``symbol``, ``resolution``, ``bar_start_ns``,
``bar_end_ns``; book state from the **last ``book_snapshots`` sample (A9) with
``exchange_ts_ns < bar_end_ns``**, only if that sample is at most 5 s older
than ``bar_end_ns`` (else null): ``best_bid``, ``best_ask``, ``mid``,
``spread_bps`` = (ask − bid) / mid × 10⁴, ``depth_imbalance_5`` = (Σ bid size
L0–4 − Σ ask size L0–4) / (sum of both), ``book_sample_ts_ns``; from trades
with ``bar_start_ns ≤ exchange_ts_ns < bar_end_ns``: ``trade_count``,
``volume``, ``signed_volume`` (+size taker buy, −size taker sell), ``vwap``
(null if no trades); ``realized_vol`` = sqrt(Σ r²) with r_k =
ln(m(T_k) / m(T_k − 1 s)) for every whole second T_k with
``bar_start_ns < T_k ≤ bar_end_ns``, where m(T) is the as-of mid at T (last
sample with ``exchange_ts_ns < T``, at most 5 s old); a ``1s`` bar therefore
has one return, a ``1m`` bar sixty; returns with a missing endpoint are
skipped and the value is null when none remain. Bars cover the span of that
day's data for the venue/symbol (first to last sample or trade), not 86,400
empty seconds. No statistic may use data at or after ``bar_end_ns``: no
centred windows, no whole-day normalization, no forward-fill past the 5 s
staleness limit. The classic-bug test is ``tests/test_features_lookahead.py``:
features for bars with ``bar_end_ns ≤ t`` are identical when every row with
``exchange_ts_ns ≥ t`` is deleted and features are recomputed (fixed cases
plus a Hypothesis property over ``t``).

Storage (decision A68):
``<out>/features/resolution=<1s|1m>/venue=<v>/date=<YYYY-MM-DD>/part-<symbol>.parquet``,
rows sorted by ``(symbol, bar_start_ns)``, deterministic bytes (two runs →
``diff -r`` clean). The schema is :data:`tickvault.models.FEATURES_SCHEMA`,
which is deliberately not one of the four normalized tables.

Implementation notes:

- The normalized tables are read once per call with DuckDB, restricted to the
  ``date=<D>`` hive partitions of the requested day; no other day is opened.
- Book samples are collapsed to one row per ``snapshot_id`` before any bar is
  built: level 0 gives the touch and levels 0–4 give the imbalance sums.
- Prices and sizes arrive as ``decimal128`` and are summed as ``Decimal``.
  Only the finished features are cast to ``float64`` (decision A68).
- The as-of mid is evaluated on the whole-second grid that every bar boundary
  falls on, so consecutive returns share endpoints: m(T_k − 1 s) is exactly
  m(T_(k−1)). A return is skipped unless both endpoints are present and
  fresh; a bar's ``realized_vol`` is null when no return survives.
- Every bar statistic is a function of rows strictly before ``bar_end_ns``,
  which is what makes deleting later rows a no-op for earlier bars.
"""

from __future__ import annotations

import itertools
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date as Date
from decimal import Decimal
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from tickvault.models import FEATURES_SCHEMA

RESOLUTIONS: Mapping[str, int] = {"1s": 1_000_000_000, "1m": 60_000_000_000}
"""Supported resolution labels and their width in nanoseconds."""

MAX_SAMPLE_AGE_NS: int = 5_000_000_000
"""How old a book sample may be at ``bar_end_ns`` before it counts as missing."""

_NS_PER_SECOND = 1_000_000_000
_IMBALANCE_LEVELS = 5
_BPS_SCALE = Decimal(10_000)
_ROW_GROUP_SIZE = 65_536
"""Same row-group size as :mod:`tickvault.storage.parquet_writer`."""


def features_path(
    out_root: str | Path, resolution: str, venue: str, date: str, symbol: str
) -> Path:
    """Return the one Parquet file holding ``symbol``'s bars for one day.

    The layout is
    ``<out_root>/features/resolution=<r>/venue=<v>/date=<YYYY-MM-DD>/part-<symbol>.parquet``;
    every bar of that resolution/venue/date/symbol pair lives in this file.
    """
    return (
        Path(out_root)
        / "features"
        / f"resolution={resolution}"
        / f"venue={venue}"
        / f"date={date}"
        / f"part-{symbol}.parquet"
    )


def compute_features(
    data_root: str | Path, date: str, resolutions: Sequence[str] = ("1s", "1m")
) -> dict[str, pa.Table]:
    """Compute the ``FEATURES_SCHEMA`` bars for ``date`` under ``data_root``.

    One table per resolution label in ``resolutions``, each holding every
    venue/symbol of that day, sorted by ``(symbol, bar_start_ns)``.
    """
    iso_date = _iso_date(date)
    widths = [_resolution_width(label) for label in resolutions]
    root = Path(data_root)
    samples = _read_samples(root, iso_date)
    trades = _read_trades(root, iso_date)
    tables: dict[str, pa.Table] = {}
    for label, width in zip(resolutions, widths, strict=True):
        rows: list[dict[str, object]] = []
        for venue, symbol in sorted(set(samples) | set(trades)):
            rows.extend(
                _symbol_bars(
                    venue,
                    symbol,
                    label,
                    width,
                    samples.get((venue, symbol), []),
                    trades.get((venue, symbol), []),
                )
            )
        rows.sort(key=_order_key)
        tables[label] = pa.Table.from_pylist(rows, schema=FEATURES_SCHEMA)
    return tables


def write_features(tables: Mapping[str, pa.Table], out_root: str | Path, date: str) -> list[Path]:
    """Write one table per resolution and return the files in resolution order.

    Files go through :func:`features_path` and hold deterministic bytes, so
    computing twice and comparing the trees with ``diff -r`` is clean.
    """
    iso_date = _iso_date(date)
    written: list[Path] = []
    for resolution, table in tables.items():
        _resolution_width(resolution)
        if not table.schema.equals(FEATURES_SCHEMA, check_metadata=False):
            raise ValueError(f"features table {resolution!r} does not use FEATURES_SCHEMA")
        for (venue, symbol), rows in _rows_by_symbol(table):
            rows.sort(key=_bar_start)
            path = features_path(out_root, resolution, venue, iso_date, symbol)
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.parent / f"{path.name}.tmp"
            pq.write_table(
                pa.Table.from_pylist(rows, schema=FEATURES_SCHEMA),
                temporary,
                compression="zstd",
                write_statistics=True,
                row_group_size=_ROW_GROUP_SIZE,
            )
            temporary.replace(path)
            written.append(path)
    return written


@dataclass(frozen=True, slots=True)
class _BookSample:
    """One collapsed ``source='sample'`` snapshot for a venue/symbol."""

    exchange_ts_ns: int
    best_bid: Decimal | None
    best_ask: Decimal | None
    bid_size_5: Decimal
    ask_size_5: Decimal


@dataclass(frozen=True, slots=True)
class _Trade:
    """The fields of a trade row that features consume."""

    exchange_ts_ns: int
    price: Decimal
    size: Decimal
    side: str


def _iso_date(value: str) -> str:
    """Return ``value`` unchanged if it is a canonical ``YYYY-MM-DD`` date."""
    try:
        parsed = Date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"invalid date: {value!r} (expected YYYY-MM-DD)") from exc
    if parsed.isoformat() != value:
        raise ValueError(f"invalid date: {value!r} (expected YYYY-MM-DD)")
    return value


def _resolution_width(resolution: str) -> int:
    try:
        return RESOLUTIONS[resolution]
    except KeyError as exc:
        supported = ", ".join(RESOLUTIONS)
        raise ValueError(
            f"unsupported resolution: {resolution!r} (expected one of: {supported})"
        ) from exc


def _partition_files(root: Path, table: str, iso_date: str) -> list[Path]:
    """Files of one normalized table for one exchange-time day, sorted."""
    directory = root / "norm" / table
    if not directory.is_dir():
        return []
    return sorted(directory.glob(f"venue=*/date={iso_date}/hour=*/*.parquet"))


def _source_list(files: Sequence[Path]) -> str:
    return "[" + ", ".join(_sql_quote(path.as_posix()) for path in files) + "]"


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _query(files: Sequence[Path], sql: str) -> list[tuple[object, ...]]:
    """Run ``sql`` against ``files`` on a fresh in-memory DuckDB connection.

    ``sql`` contains a ``{source}`` placeholder that expands to the file list,
    so only the files of the requested day are opened.
    """
    connection = duckdb.connect()
    try:
        return connection.execute(sql.format(source=_source_list(files))).fetchall()
    finally:
        connection.close()


def _read_samples(root: Path, iso_date: str) -> dict[tuple[str, str], list[_BookSample]]:
    """Read and collapse the day's ``source='sample'`` book snapshots."""
    files = _partition_files(root, "book_snapshots", iso_date)
    if not files:
        return {}
    rows = _query(
        files,
        """
        SELECT venue, symbol, exchange_ts_ns,
               max(CASE WHEN side = 'bid' AND level = 0 THEN price END) AS best_bid,
               max(CASE WHEN side = 'ask' AND level = 0 THEN price END) AS best_ask,
               coalesce(sum(CASE WHEN side = 'bid' AND level < 5 THEN size END), 0) AS bid_size_5,
               coalesce(sum(CASE WHEN side = 'ask' AND level < 5 THEN size END), 0) AS ask_size_5
        FROM read_parquet({source})
        WHERE source = 'sample'
        GROUP BY venue, symbol, exchange_ts_ns, snapshot_id
        ORDER BY venue, symbol, exchange_ts_ns, snapshot_id
        """,
    )
    samples: dict[tuple[str, str], list[_BookSample]] = {}
    for venue, symbol, exchange_ts_ns, best_bid, best_ask, bid_size_5, ask_size_5 in rows:
        samples.setdefault((str(venue), str(symbol)), []).append(
            _BookSample(
                exchange_ts_ns=_int(exchange_ts_ns),
                best_bid=_decimal_or_none(best_bid),
                best_ask=_decimal_or_none(best_ask),
                bid_size_5=_decimal(bid_size_5),
                ask_size_5=_decimal(ask_size_5),
            )
        )
    return samples


def _read_trades(root: Path, iso_date: str) -> dict[tuple[str, str], list[_Trade]]:
    """Read the day's trades, ordered by exchange time."""
    files = _partition_files(root, "trades", iso_date)
    if not files:
        return {}
    rows = _query(
        files,
        """
        SELECT venue, symbol, exchange_ts_ns, price, size, side
        FROM read_parquet({source})
        ORDER BY venue, symbol, exchange_ts_ns, ingest_seq
        """,
    )
    trades: dict[tuple[str, str], list[_Trade]] = {}
    for venue, symbol, exchange_ts_ns, price, size, side in rows:
        trades.setdefault((str(venue), str(symbol)), []).append(
            _Trade(
                exchange_ts_ns=_int(exchange_ts_ns),
                price=_decimal(price),
                size=_decimal(size),
                side=str(side),
            )
        )
    return trades


def _decimal(value: object) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _int(value: object) -> int:
    if isinstance(value, int):
        return value
    return int(str(value))


def _decimal_or_none(value: object) -> Decimal | None:
    if value is None:
        return None
    return _decimal(value)


def _symbol_bars(
    venue: str,
    symbol: str,
    resolution: str,
    width: int,
    samples: list[_BookSample],
    trades: list[_Trade],
) -> list[dict[str, object]]:
    """Build every bar of one resolution for one venue/symbol."""
    timestamps = [sample.exchange_ts_ns for sample in samples]
    timestamps.extend(trade.exchange_ts_ns for trade in trades)
    if not timestamps:
        return []
    first_ns = min(timestamps)
    last_ns = max(timestamps)
    first_bar_start = first_ns - first_ns % width
    last_bar_start = last_ns - last_ns % width
    last_bar_end = last_bar_start + width

    mids = _as_of_mids(samples, first_bar_start, last_bar_end)

    rows: list[dict[str, object]] = []
    sample_index = 0
    trade_index = 0
    for bar_start_ns in range(first_bar_start, last_bar_end, width):
        bar_end_ns = bar_start_ns + width

        while sample_index < len(samples) and samples[sample_index].exchange_ts_ns < bar_end_ns:
            sample_index += 1
        sample = samples[sample_index - 1] if sample_index else None
        if sample is not None and bar_end_ns - sample.exchange_ts_ns > MAX_SAMPLE_AGE_NS:
            sample = None

        while trade_index < len(trades) and trades[trade_index].exchange_ts_ns < bar_start_ns:
            trade_index += 1
        bar_trades: list[_Trade] = []
        while trade_index < len(trades) and trades[trade_index].exchange_ts_ns < bar_end_ns:
            bar_trades.append(trades[trade_index])
            trade_index += 1

        rows.append(
            _bar_row(
                venue=venue,
                symbol=symbol,
                resolution=resolution,
                bar_start_ns=bar_start_ns,
                bar_end_ns=bar_end_ns,
                sample=sample,
                trades=bar_trades,
                mids=mids,
                grid_start_ns=first_bar_start,
            )
        )
    return rows


def _bar_row(
    *,
    venue: str,
    symbol: str,
    resolution: str,
    bar_start_ns: int,
    bar_end_ns: int,
    sample: _BookSample | None,
    trades: Sequence[_Trade],
    mids: Sequence[float | None],
    grid_start_ns: int,
) -> dict[str, object]:
    """Compute one bar from its (already lookahead-free) inputs."""
    best_bid: float | None = None
    best_ask: float | None = None
    mid: float | None = None
    spread_bps: float | None = None
    depth_imbalance_5: float | None = None
    book_sample_ts_ns: int | None = None
    if sample is not None:
        book_sample_ts_ns = sample.exchange_ts_ns
        if sample.best_bid is not None:
            best_bid = float(sample.best_bid)
        if sample.best_ask is not None:
            best_ask = float(sample.best_ask)
        if sample.best_bid is not None and sample.best_ask is not None:
            exact_mid = (sample.best_bid + sample.best_ask) / 2
            mid = float(exact_mid)
            spread_bps = float((sample.best_ask - sample.best_bid) / exact_mid * _BPS_SCALE)
        total_size = sample.bid_size_5 + sample.ask_size_5
        if total_size > 0:
            depth_imbalance_5 = float((sample.bid_size_5 - sample.ask_size_5) / total_size)

    trade_count = len(trades)
    volume = sum((trade.size for trade in trades), Decimal(0))
    signed_volume = sum(
        (trade.size if trade.side == "buy" else -trade.size for trade in trades),
        Decimal(0),
    )
    vwap: float | None = None
    if trade_count and volume > 0:
        notional = sum((trade.price * trade.size for trade in trades), Decimal(0))
        vwap = float(notional / volume)

    offset = (bar_start_ns - grid_start_ns) // _NS_PER_SECOND
    count = (bar_end_ns - bar_start_ns) // _NS_PER_SECOND
    realized_vol = _realized_vol(mids[offset : offset + count + 1])

    return {
        "venue": venue,
        "symbol": symbol,
        "resolution": resolution,
        "bar_start_ns": bar_start_ns,
        "bar_end_ns": bar_end_ns,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "mid": mid,
        "spread_bps": spread_bps,
        "depth_imbalance_5": depth_imbalance_5,
        "book_sample_ts_ns": book_sample_ts_ns,
        "trade_count": trade_count,
        "volume": float(volume),
        "signed_volume": float(signed_volume),
        "vwap": vwap,
        "realized_vol": realized_vol,
    }


def _as_of_mids(
    samples: Sequence[_BookSample], first_second_ns: int, last_second_ns: int
) -> list[float | None]:
    """As-of mid at every whole second in ``[first_second_ns, last_second_ns]``.

    The mid at ``T`` is the mid of the last sample with ``exchange_ts_ns < T``,
    null when there is none or it is older than :data:`MAX_SAMPLE_AGE_NS`. The
    bounds are whole seconds because every bar boundary is one.
    """
    mids: list[float | None] = []
    index = 0
    for second_ns in range(first_second_ns, last_second_ns + _NS_PER_SECOND, _NS_PER_SECOND):
        while index < len(samples) and samples[index].exchange_ts_ns < second_ns:
            index += 1
        if index == 0:
            mids.append(None)
            continue
        sample = samples[index - 1]
        if (
            second_ns - sample.exchange_ts_ns > MAX_SAMPLE_AGE_NS
            or sample.best_bid is None
            or sample.best_ask is None
        ):
            mids.append(None)
        else:
            mids.append(float((sample.best_bid + sample.best_ask) / 2))
    return mids


def _realized_vol(mids: Sequence[float | None]) -> float | None:
    """``sqrt(sum(r²))`` over consecutive whole-second mid log returns."""
    total = 0.0
    used = 0
    for previous, current in itertools.pairwise(mids):
        if previous is None or current is None or previous <= 0 or current <= 0:
            continue
        log_return = math.log(current / previous)
        total += log_return * log_return
        used += 1
    if not used:
        return None
    return math.sqrt(total)


def _rows_by_symbol(table: pa.Table) -> list[tuple[tuple[str, str], list[dict[str, object]]]]:
    """Group a features table's rows by ``(venue, symbol)``, sorted."""
    groups: dict[tuple[str, str], list[dict[str, object]]] = {}
    for row in table.to_pylist():
        key = (str(row["venue"]), str(row["symbol"]))
        groups.setdefault(key, []).append(row)
    return sorted(groups.items())


def _order_key(row: Mapping[str, object]) -> tuple[str, str, int]:
    """Sort key of the features table: ``(venue, symbol, bar_start_ns)``."""
    return (str(row["venue"]), str(row["symbol"]), _int(row["bar_start_ns"]))


def _bar_start(row: Mapping[str, object]) -> int:
    return _int(row["bar_start_ns"])
