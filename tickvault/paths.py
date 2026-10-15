"""Path helpers for the raw and normalized layers.

Every path is derived from UTC timestamps. Raw files are keyed by receive time;
normalized partitions are keyed by exchange time (events without an exchange
time use receive time). Partition keys never appear inside Parquet files
(hive-style partitioning).
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

_NS_PER_SECOND = 1_000_000_000


def _utc_datetime(ts_ns: int) -> datetime:
    seconds = ts_ns // _NS_PER_SECOND
    return datetime.fromtimestamp(seconds, tz=UTC)


def partition_path(root: str | Path, table: str, venue: str, ts_ns: int) -> Path:
    """Return ``root/norm/<table>/venue=<v>/date=<YYYY-MM-DD>/hour=<HH>``.

    ``ts_ns`` is the exchange timestamp (or receive timestamp when the record
    has no exchange time), in nanoseconds since the Unix epoch, UTC.
    """
    day = _utc_datetime(ts_ns)
    return (
        Path(root)
        / "norm"
        / table
        / f"venue={venue}"
        / f"date={day:%Y-%m-%d}"
        / f"hour={day:%H}"
    )


def raw_file_path(
    root: str | Path,
    venue: str,
    connection_id: str,
    recv_ts_ns: int,
    rotation: int,
) -> Path:
    """Return the raw file path for one hourly rotation of a connection.

    Files are
    ``root/raw/venue=<v>/date=<YYYY-MM-DD>/hour=<HH>/<connection_id>-<rotation:05d>.jsonl.zst``,
    with date and hour taken from ``recv_ts_ns`` (UTC).
    """
    day = _utc_datetime(recv_ts_ns)
    return (
        Path(root)
        / "raw"
        / f"venue={venue}"
        / f"date={day:%Y-%m-%d}"
        / f"hour={day:%H}"
        / f"{connection_id}-{rotation:05d}.jsonl.zst"
    )
