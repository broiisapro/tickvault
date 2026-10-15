"""Compaction of normalized Parquet partitions.

``compact(root, date)`` merges every ``norm/<table>/venue=*/date=<date>/hour=*``
directory that holds more than one file into a single
``compacted-<date>-<HH>.parquet``. Rows are sorted by
``(recv_ts_ns, connection_id, ingest_seq)`` with a stable sort, so the relative
order of rows that share a key (all rows of one raw message) survives.

The merged file is written to a temporary name with the same settings as
:class:`tickvault.storage.parquet_writer.ParquetWriter` (zstd, statistics,
fixed row groups), read back and checked, and only then renamed into place.
Before that point no input is deleted or overwritten: the temporary file is
removed and :class:`CompactionError` is raised if the check fails.

Directories with one file are left alone, re-running is a no-op and ``raw/``
is never read, written or deleted.
"""

from __future__ import annotations

import contextlib
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal, cast

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

_ROW_GROUP_SIZE = 65_536
"""Must match ``tickvault.storage.parquet_writer._ROW_GROUP_SIZE``."""

type _SortOrder = Literal["ascending", "descending"]
type _Key = tuple[str, int]

_SORT_KEYS: list[tuple[str, _SortOrder]] = [
    ("recv_ts_ns", "ascending"),
    ("connection_id", "ascending"),
    ("ingest_seq", "ascending"),
]


class CompactionError(RuntimeError):
    """A compacted file failed verification; the inputs were kept."""


@dataclass(frozen=True, slots=True)
class CompactedPartition:
    """One partition directory that was merged into a single file."""

    table: str
    venue: str
    hour: str
    rows: int
    inputs: int
    output: str
    """Path of the merged file relative to the data root."""


@dataclass(frozen=True, slots=True)
class CompactionResult:
    """What ``compact`` did for one date; empty ``partitions`` means no-op."""

    date: str
    partitions: tuple[CompactedPartition, ...]

    @property
    def rows(self) -> int:
        return sum(partition.rows for partition in self.partitions)

    @property
    def files_removed(self) -> int:
        return sum(partition.inputs for partition in self.partitions)

    @property
    def files_written(self) -> int:
        return len(self.partitions)


def compact(root: str | Path, date: str) -> CompactionResult:
    """Merge multi-file partitions of ``date`` under ``root/norm/``.

    Raises :class:`ValueError` for a malformed date and
    :class:`CompactionError` when a merged file fails verification (in which
    case the partition's inputs are left untouched).
    """
    _validate_date(date)
    root_path = Path(root)
    compacted: list[CompactedPartition] = []
    norm_root = root_path / "norm"
    if not norm_root.is_dir():
        return CompactionResult(date=date, partitions=())
    for table_dir in _subdirectories(norm_root):
        for venue_dir in _subdirectories(table_dir, prefix="venue="):
            date_dir = venue_dir / f"date={date}"
            if not date_dir.is_dir():
                continue
            for hour_dir in _subdirectories(date_dir, prefix="hour="):
                files = sorted(hour_dir.glob("*.parquet"))
                if len(files) <= 1:
                    continue
                hour = hour_dir.name.removeprefix("hour=")
                output = hour_dir / f"compacted-{date}-{hour}.parquet"
                rows = _compact_partition(files, output)
                compacted.append(
                    CompactedPartition(
                        table=table_dir.name,
                        venue=venue_dir.name.removeprefix("venue="),
                        hour=hour,
                        rows=rows,
                        inputs=len(files),
                        output=output.relative_to(root_path).as_posix(),
                    )
                )
    return CompactionResult(date=date, partitions=tuple(compacted))


def _subdirectories(parent: Path, *, prefix: str = "") -> list[Path]:
    return sorted(
        path
        for path in parent.iterdir()
        if path.is_dir() and path.name.startswith(prefix)
    )


def _compact_partition(files: list[Path], output: Path) -> int:
    merged = pa.concat_tables([pq.read_table(path) for path in files])
    expected_rows = merged.num_rows
    expected_keys = _key_counter(merged)
    indices = pc.sort_indices(merged, sort_keys=_SORT_KEYS)
    temporary = output.parent / f"{output.name}.tmp"
    try:
        _write_compacted(merged.take(indices), temporary)
        problem = _verify_output(temporary, expected_rows, expected_keys)
        if problem is not None:
            raise CompactionError(
                f"compaction of {output} failed verification: {problem}"
            )
        temporary.replace(output)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()
        raise
    for path in files:
        if path != output:
            path.unlink()
    return expected_rows


def _write_compacted(table: pa.Table, destination: Path) -> None:
    """Write one file with the settings of ``ParquetWriter`` (zstd, statistics)."""
    pq.write_table(
        table,
        destination,
        compression="zstd",
        write_statistics=True,
        row_group_size=_ROW_GROUP_SIZE,
    )


def _verify_output(output: Path, expected_rows: int, expected_keys: Counter[_Key]) -> str | None:
    try:
        table = pq.read_table(output)
    except (OSError, pa.ArrowException) as exc:
        return f"unreadable: {exc}"
    if table.num_rows != expected_rows:
        return f"row count {table.num_rows} != {expected_rows}"
    if _key_counter(table) != expected_keys:
        return "message key multiset changed"
    return None


def _key_counter(table: pa.Table) -> Counter[_Key]:
    connections = cast("list[str]", table.column("connection_id").to_pylist())
    sequences = cast("list[int]", table.column("ingest_seq").to_pylist())
    return Counter(zip(connections, sequences, strict=True))


def _validate_date(date: str) -> None:
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
        raise ValueError(f"date must be formatted YYYY-MM-DD, got {date!r}")
    try:
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"date must be a valid calendar date, got {date!r}") from exc
