#!/usr/bin/env python3
"""Build the size-capped public sample from one day of a data root.

The sample is one symbol and one UTC hour, taken from every venue that holds
rows for it (FR-3.2)::

    uv run python scripts/build_public_sample.py \\
        --data dashboard/sample --date 2026-10-15 --out /tmp/tv-public-sample

For each normalized table the builder reads every partition file of that
``date``/``hour`` under ``<data>/norm/<table>/venue=*/``, keeps the rows whose
``symbol`` matches, sorts them by ``(recv_ts_ns, connection_id, ingest_seq)``
(the canonical replay order) and writes one Parquet file per table and venue
into::

    <out>/norm/<table>/venue=<v>/date=<YYYY-MM-DD>/hour=<HH>/sample-<date>-<HH>.parquet

with the deterministic writer options of
:class:`tickvault.storage.parquet_writer.ParquetWriter` (zstd, statistics,
fixed row groups) and the schema of the normalized table from
:data:`tickvault.models.TABLES`. Only tables that end up with rows get files.

The output root then holds:

- ``norm/`` — the sampled normalized rows, partitioned like the input;
- ``manifests/date=<date>/manifest.json`` — written by
  :func:`tickvault.storage.manifest.write_manifest`; ``tickvault verify
  --manifest`` (and :func:`tickvault.storage.manifest.verify_manifest`) proves
  the files still match it;
- ``SHA256SUMS`` — ``sha256sum -c`` compatible, one ``<sha256>  <path>`` line
  per file (paths relative to the output root, sorted, the checksums file
  itself excluded);
- ``sample.json`` — ``date``, ``hour``, ``symbol``, ``venues``, ``rows`` per
  table and ``source``. ``source`` is ``null`` unless the input root has
  ``reports/qa-<date>.json``, in which case it summarises that report
  (``report``, ``date``, ``rows``, ``unexplained``, ``book_missing``,
  ``exit_code``) the same way the QA site's ``latest.json`` does; the report's
  own absolute paths are not copied.

Without ``--hour`` the hour with the most rows for the symbol is used, ties
going to the earliest hour. The build is deterministic (two runs produce
byte-identical trees) and never writes to ``--data``.

Everything is assembled in a temporary directory next to ``--out`` and moved
into place only after the size cap was checked after every written file, so a
refused build leaves no partial sample behind. The exit code is ``2`` for a
bad invocation (missing data root, malformed date/hour, ``--out`` inside
``--data``) and ``1`` for a refusal: the finished sample would exceed
``--max-mb``, ``--out`` exists and is not empty, or the day has no rows for the
symbol.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import shutil
import sys
import tempfile
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import Literal

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from tickvault.models import TABLES
from tickvault.storage.manifest import write_manifest

_DEFAULT_SYMBOL = "BTC-USD"
_DEFAULT_MAX_MB = 50.0
_MIB = 1024 * 1024
_ROW_GROUP_SIZE = 65_536
"""Must match ``tickvault.storage.parquet_writer._ROW_GROUP_SIZE``."""

type _SortOrder = Literal["ascending", "descending"]

_SORT_KEYS: list[tuple[str, _SortOrder]] = [
    ("recv_ts_ns", "ascending"),
    ("connection_id", "ascending"),
    ("ingest_seq", "ascending"),
]
"""The canonical replay order (A4), as used by compaction."""

_CHECKSUMS_NAME = "SHA256SUMS"


class SampleError(RuntimeError):
    """A refused build: the caller asked for a sample that cannot be produced."""


class SampleUsageError(ValueError):
    """A malformed invocation; nothing was read or written."""


@dataclass(frozen=True, slots=True)
class SampleBuild:
    """What a successful build produced (everything is relative to the root)."""

    date: str
    hour: str
    symbol: str
    venues: tuple[str, ...]
    rows: dict[str, int]
    files: int
    bytes: int

    @property
    def payload(self) -> dict[str, object]:
        """The ``sample.json`` body without its ``source`` (filled in by the build)."""
        return {
            "date": self.date,
            "hour": self.hour,
            "symbol": self.symbol,
            "venues": list(self.venues),
            "rows": dict(sorted(self.rows.items())),
        }


def build_sample(
    *,
    data: str | Path,
    date: str,
    out: str | Path,
    symbol: str = _DEFAULT_SYMBOL,
    hour: str | None = None,
    max_mb: float = _DEFAULT_MAX_MB,
) -> SampleBuild:
    """Build the public sample of ``date`` into ``out``; return what was written.

    Raises :class:`SampleUsageError` for a malformed invocation and
    :class:`SampleError` for every refusal (non-empty ``out``, no rows for the
    symbol, output over ``max_mb``). On success ``out`` holds the complete
    tree; on failure it is left exactly as it was.
    """
    root = Path(data)
    destination = Path(out)
    iso_date = _validate_date(date)
    chosen_symbol = _validate_symbol(symbol)
    _validate_max_mb(max_mb)
    _validate_data_root(root)
    _validate_separation(root, destination)
    _check_out(destination)
    source = _qa_summary(root, iso_date)

    chosen_hour = (
        _normalize_hour(hour)
        if hour is not None
        else _pick_hour(root, iso_date, chosen_symbol)
    )
    sources = _partition_files(root, iso_date, chosen_hour, chosen_symbol)

    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(prefix=f".{destination.name}.", dir=destination.parent)
    )
    published = False
    try:
        staging.chmod(0o755)
        max_bytes = int(max_mb * _MIB)
        rows: dict[str, int] = {}
        venues: set[str] = set()
        files = 0
        for table_name in TABLES:
            rows[table_name] = 0
            for venue, paths in sorted(sources[table_name].items()):
                table = _read_rows(paths, table_name, chosen_symbol)
                if table.num_rows == 0:
                    continue
                target = (
                    staging
                    / "norm"
                    / table_name
                    / f"venue={venue}"
                    / f"date={iso_date}"
                    / f"hour={chosen_hour}"
                    / f"sample-{iso_date}-{chosen_hour}.parquet"
                )
                _write_partition(table, target)
                rows[table_name] += table.num_rows
                venues.add(venue)
                files += 1
                _enforce_cap(staging, max_bytes, max_mb)

        if not venues:
            raise SampleError(
                f"no {chosen_symbol} rows on {iso_date} at hour {chosen_hour} "
                f"under {root / 'norm'}"
            )

        build = SampleBuild(
            date=iso_date,
            hour=chosen_hour,
            symbol=chosen_symbol,
            venues=tuple(sorted(venues)),
            rows=rows,
            files=files,
            bytes=0,
        )
        write_manifest(staging, iso_date)
        _enforce_cap(staging, max_bytes, max_mb)
        payload = build.payload
        payload["source"] = source
        (staging / "sample.json").write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        _enforce_cap(staging, max_bytes, max_mb)
        _write_sha256sums(staging)
        _enforce_cap(staging, max_bytes, max_mb)

        if destination.exists():
            destination.rmdir()
        staging.replace(destination)
        published = True
    finally:
        if not published:
            shutil.rmtree(staging, ignore_errors=True)

    total_bytes = _tree_bytes(destination)
    return replace(build, bytes=total_bytes)


def _partition_files(
    root: Path, date: str, hour: str, symbol: str
) -> dict[str, dict[str, list[Path]]]:
    """Partition files of ``date``/``hour`` per table and venue.

    Only partitions that hold at least one row of ``symbol`` are returned, and
    their paths are sorted so the read order is deterministic.
    """
    sources: dict[str, dict[str, list[Path]]] = {}
    for table_name in TABLES:
        per_venue: dict[str, list[Path]] = {}
        pattern = f"norm/{table_name}/venue=*/date={date}/hour={hour}/*.parquet"
        for path in sorted(root.glob(pattern)):
            if _count_rows(path, symbol) == 0:
                continue
            venue = path.parent.parent.parent.name.removeprefix("venue=")
            per_venue.setdefault(venue, []).append(path)
        sources[table_name] = per_venue
    return sources


def _pick_hour(root: Path, date: str, symbol: str) -> str:
    """Return the hour with the most rows for ``symbol`` (ties: earliest)."""
    counts: Counter[str] = Counter()
    for table_name in TABLES:
        pattern = f"norm/{table_name}/venue=*/date={date}/hour=*/*.parquet"
        for path in sorted(root.glob(pattern)):
            hour = path.parent.name.removeprefix("hour=")
            counts[hour] += _count_rows(path, symbol)
    if not counts:
        raise SampleError(f"no {symbol} rows under {root / 'norm'} on {date}")
    return min(counts, key=lambda hour: (-counts[hour], hour))


def _count_rows(path: Path, symbol: str) -> int:
    """Count the rows of ``path`` whose symbol matches, reading only that column."""
    column = pq.read_table(path, columns=["symbol"], filters=[("symbol", "=", symbol)])
    return column.num_rows


def _read_rows(paths: Sequence[Path], table_name: str, symbol: str) -> pa.Table:
    """Read, filter and canonically sort the rows of ``table_name``."""
    schema = TABLES[table_name]
    parts = [
        _conform(
            pq.read_table(path, filters=[("symbol", "=", symbol)]),
            schema,
            table_name,
            path,
        )
        for path in paths
    ]
    merged = parts[0] if len(parts) == 1 else pa.concat_tables(parts)
    if merged.num_rows == 0:
        return merged
    indices = pc.sort_indices(merged, sort_keys=_SORT_KEYS)
    return merged.take(indices)


def _conform(
    table: pa.Table, schema: pa.Schema, table_name: str, path: Path
) -> pa.Table:
    """Return ``table`` with the normalized schema, refusing an incompatible file."""
    if table.schema.equals(schema):
        return table
    missing = [name for name in schema.names if name not in table.column_names]
    if missing:
        raise SampleError(f"{path} is missing column(s) {', '.join(missing)}")
    conformed = table.select(schema.names).cast(schema)
    if not conformed.schema.equals(schema):
        raise SampleError(f"{path} does not have the {table_name} schema")
    return conformed


def _write_partition(table: pa.Table, destination: Path) -> None:
    """Write one Parquet file with the storage layer's deterministic options."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        table,
        destination,
        compression="zstd",
        write_statistics=True,
        row_group_size=_ROW_GROUP_SIZE,
    )


def _write_sha256sums(root: Path) -> Path:
    """Write ``SHA256SUMS`` over every file of ``root`` except itself."""
    files = sorted(
        (path.relative_to(root).as_posix(), path)
        for path in root.rglob("*")
        if path.is_file() and path.name != _CHECKSUMS_NAME
    )
    lines = [f"{_sha256(path)}  {relative}" for relative, path in files]
    target = root / _CHECKSUMS_NAME
    target.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return target


def _qa_summary(root: Path, date: str) -> dict[str, object] | None:
    """Summarise ``<root>/reports/qa-<date>.json``, or ``None`` when absent.

    Only stable, machine-readable fields are kept (the QA site's ``latest.json``
    shape); the report's ``generated_from`` path is deliberately not copied
    into a tree meant for publication.
    """
    report_path = root / "reports" / f"qa-{date}.json"
    if not report_path.is_file():
        return None
    try:
        document = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SampleError(f"cannot read QA report {report_path}: {exc}") from exc
    if not isinstance(document, dict) or not isinstance(document.get("rows"), list):
        raise SampleError(f"unexpected QA report shape in {report_path}")
    rows = [row for row in document["rows"] if isinstance(row, dict)]
    unexplained = sum(int(row.get("unexplained", 0)) for row in rows)
    book_missing = sum(1 for row in rows if row.get("book_missing"))
    return {
        "report": report_path.relative_to(root).as_posix(),
        "date": date,
        "rows": len(rows),
        "unexplained": unexplained,
        "book_missing": book_missing,
        "exit_code": 1 if unexplained or book_missing else 0,
    }


def _enforce_cap(root: Path, max_bytes: int, max_mb: float) -> None:
    """Refuse as soon as the staged tree is over ``max_bytes``."""
    written = _tree_bytes(root)
    if written > max_bytes:
        raise SampleError(
            f"sample would be over --max-mb {max_mb:g}: {written} bytes written, "
            "stopping"
        )


def _tree_bytes(root: Path) -> int:
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_date(date: str) -> str:
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
        raise SampleUsageError(f"--date must be formatted YYYY-MM-DD, got {date!r}")
    try:
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError as exc:
        raise SampleUsageError(f"--date must be a valid calendar date, got {date!r}") from exc
    return date


def _normalize_hour(hour: str) -> str:
    text = hour.strip()
    if not re.fullmatch(r"\d{1,2}", text):
        raise SampleUsageError(f"--hour must be an hour of the day (00-23), got {hour!r}")
    value = int(text)
    if not 0 <= value <= 23:
        raise SampleUsageError(f"--hour must be an hour of the day (00-23), got {hour!r}")
    return f"{value:02d}"


def _validate_symbol(symbol: str) -> str:
    text = symbol.strip()
    if not text:
        raise SampleUsageError("--symbol must not be empty")
    return text


def _validate_max_mb(max_mb: float) -> None:
    if not max_mb > 0 or not math.isfinite(max_mb):
        raise SampleUsageError("--max-mb must be positive and finite")


def _validate_data_root(root: Path) -> None:
    if not root.is_dir():
        raise SampleUsageError(f"data root not found: {root}")


def _validate_separation(root: Path, destination: Path) -> None:
    if destination.resolve().is_relative_to(root.resolve()):
        raise SampleUsageError("--out must not be inside --data")


def _check_out(destination: Path) -> None:
    if not destination.exists():
        return
    if not destination.is_dir():
        raise SampleError(f"{destination} exists and is not a directory")
    if any(destination.iterdir()):
        raise SampleError(f"{destination} exists and is not empty")


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the size-capped public sample of one day and hour.",
    )
    parser.add_argument(
        "--data",
        type=Path,
        required=True,
        metavar="ROOT",
        help="data root with a normalized tree (norm/)",
    )
    parser.add_argument(
        "--date",
        required=True,
        metavar="YYYY-MM-DD",
        help="UTC date to sample",
    )
    parser.add_argument(
        "--out",
        type=Path,
        required=True,
        metavar="DIR",
        help="output directory; must not exist or be empty",
    )
    parser.add_argument(
        "--symbol",
        default=_DEFAULT_SYMBOL,
        help=f"symbol to sample (default: {_DEFAULT_SYMBOL})",
    )
    parser.add_argument(
        "--hour",
        default=None,
        metavar="HH",
        help="UTC hour to sample (default: the hour with the most rows for the symbol)",
    )
    parser.add_argument(
        "--max-mb",
        type=float,
        default=_DEFAULT_MAX_MB,
        metavar="MB",
        help=f"fail when the finished tree is larger than this many MiB "
        f"(default: {_DEFAULT_MAX_MB:g})",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        build = build_sample(
            data=args.data,
            date=args.date,
            out=args.out,
            symbol=args.symbol,
            hour=args.hour,
            max_mb=args.max_mb,
        )
    except SampleUsageError as exc:
        print(f"build_public_sample: {exc}", file=sys.stderr)
        return 2
    except SampleError as exc:
        print(f"build_public_sample: {exc}", file=sys.stderr)
        return 1
    venue_list = ", ".join(build.venues)
    print(
        f"{args.out}: {build.date} hour {build.hour} {build.symbol} from "
        f"{len(build.venues)} venue(s) ({venue_list}), {build.files} file(s), "
        f"{build.bytes} bytes ({build.bytes / _MIB:.2f} MiB) within "
        f"--max-mb {args.max_mb:g}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
