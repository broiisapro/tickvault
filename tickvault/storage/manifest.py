"""SHA-256 manifests of the normalized layer per date.

``write_manifest(root, date)`` writes
``<root>/manifests/date=<date>/manifest.json``: every normalized Parquet file
of that date with its path relative to the data root, table, venue, hour, row
count, SHA-256 and byte size, sorted by path, plus totals. ``verify_manifest``
recomputes all of it from disk and returns human-readable mismatches (empty
list means the manifest matches), including files that are missing, modified,
unlisted or part of a totals mismatch.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import TypedDict

import pyarrow as pa
import pyarrow.parquet as pq

_CHUNK = 1 << 20


class _ManifestEntry(TypedDict):
    path: str
    table: str
    venue: str
    hour: str
    row_count: int
    sha256: str
    bytes: int


class _Totals(TypedDict):
    files: int
    row_count: int
    bytes: int


class _ManifestDocument(TypedDict):
    date: str
    files: list[_ManifestEntry]
    totals: _Totals


def write_manifest(root: str | Path, date: str) -> Path:
    """Write the manifest of ``date`` under ``root/manifests/`` and return its path."""
    _validate_date(date)
    root_path = Path(root)
    entries: list[_ManifestEntry] = []
    for file_path in _normalized_files(root_path, date):
        relative = file_path.relative_to(root_path).as_posix()
        table, venue, hour = _partition_parts(relative)
        entries.append(
            _ManifestEntry(
                path=relative,
                table=table,
                venue=venue,
                hour=hour,
                row_count=_row_count(file_path),
                sha256=_sha256(file_path),
                bytes=file_path.stat().st_size,
            )
        )
    entries.sort(key=lambda entry: entry["path"])
    document = _ManifestDocument(
        date=date,
        files=entries,
        totals=_Totals(
            files=len(entries),
            row_count=sum(entry["row_count"] for entry in entries),
            bytes=sum(entry["bytes"] for entry in entries),
        ),
    )
    manifest_path = root_path / "manifests" / f"date={date}" / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return manifest_path


def verify_manifest(path: str | Path) -> list[str]:
    """Recheck every file in the manifest; return mismatches (empty is clean)."""
    manifest_path = Path(path)
    document: _ManifestDocument = json.loads(manifest_path.read_text(encoding="utf-8"))
    root_path = manifest_path.parent.parent.parent
    date = document["date"]
    listed: set[str] = set()
    problems: list[str] = []
    totals = _Totals(files=0, row_count=0, bytes=0)
    for entry in document["files"]:
        relative = entry["path"]
        listed.add(relative)
        file_path = root_path / relative
        if not file_path.is_file():
            problems.append(f"missing file: {relative}")
            continue
        size = file_path.stat().st_size
        if size != entry["bytes"]:
            problems.append(f"bytes mismatch: {relative}")
        if _sha256(file_path) != entry["sha256"]:
            problems.append(f"sha256 mismatch: {relative}")
        try:
            rows = _row_count(file_path)
        except (OSError, pa.ArrowException):
            problems.append(f"unreadable file: {relative}")
            continue
        if rows != entry["row_count"]:
            problems.append(f"row_count mismatch: {relative}")
        totals["files"] += 1
        totals["bytes"] += size
        totals["row_count"] += rows
    for extra_path in _normalized_files(root_path, date):
        relative = extra_path.relative_to(root_path).as_posix()
        if relative not in listed:
            problems.append(f"unlisted file: {relative}")
    for field in ("files", "row_count", "bytes"):
        if totals[field] != document["totals"][field]:
            problems.append(f"totals.{field} mismatch")
    return problems


def _normalized_files(root: Path, date: str) -> list[Path]:
    pattern = f"*/venue=*/date={date}/hour=*/*.parquet"
    return sorted((root / "norm").glob(pattern))


def _partition_parts(relative: str) -> tuple[str, str, str]:
    parts = relative.split("/")
    if len(parts) != 6 or parts[0] != "norm":
        raise ValueError(f"not a normalized file path: {relative!r}")
    _, table, venue_part, date_part, hour_part, _ = parts
    for part, prefix in ((venue_part, "venue="), (date_part, "date="), (hour_part, "hour=")):
        if not part.startswith(prefix):
            raise ValueError(f"not a normalized file path: {relative!r}")
    return table, venue_part.removeprefix("venue="), hour_part.removeprefix("hour=")


def _row_count(path: Path) -> int:
    metadata = pq.ParquetFile(path).metadata
    return 0 if metadata is None else metadata.num_rows


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(_CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_date(date: str) -> None:
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
        raise ValueError(f"date must be formatted YYYY-MM-DD, got {date!r}")
    try:
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"date must be a valid calendar date, got {date!r}") from exc
