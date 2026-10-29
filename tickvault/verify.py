"""Remote manifest verification for published data samples (Epic 8).

``tickvault verify --manifest <path|url>`` verifies a manifest against the
files it describes. The manifest is the document
:func:`tickvault.storage.manifest.write_manifest` writes
(``<root>/manifests/date=<d>/manifest.json``); the data root is the manifest's
location minus ``manifests/date=<d>/manifest.json``, for local paths and
``http(s)`` URLs alike.

Every listed file is checked for existence, byte size, SHA-256 and Parquet row
count, and the manifest's totals are recomputed. For a local manifest the
normalized tree is also scanned for files the manifest does not list; that is
impossible over HTTP, so a remote check catches every mismatch except an
unlisted file. Mismatches never raise: each one becomes one line in
:attr:`VerifyReport.problems`.
"""

from __future__ import annotations

import json
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import TypedDict, cast
from urllib.parse import quote, urljoin, urlsplit, urlunsplit

import httpx
import pyarrow as pa

from tickvault.storage.manifest import (
    ManifestDocument,
    ManifestEntry,
    file_sha256,
    normalized_files,
    parquet_row_count,
    validate_date,
)

_TEMPORARY_DIRECTORY = tempfile.TemporaryDirectory
"""Factory for the download directory; replaced in tests to observe cleanup."""


@dataclass
class VerifyReport:
    """Outcome of verifying one manifest against its files."""

    manifest: str
    """The manifest path or URL that was verified."""

    files: int = 0
    """How many files the manifest lists."""

    ok: int = 0
    """How many of them matched their recorded size, checksum and row count."""

    problems: list[str] = field(default_factory=list)
    """One human-readable line per mismatch; empty means everything matched."""


class _Totals(TypedDict):
    files: int
    row_count: int
    bytes: int


@dataclass
class _FileCheck:
    """What checking one file produced, including the values totals add up."""

    problems: list[str]
    readable: bool
    size: int = 0
    rows: int = 0


def verify_remote(manifest: str, *, timeout_s: float = 30.0) -> VerifyReport:
    """Verify the manifest at ``manifest`` (a path or an http(s) URL).

    Downloads the manifest and every file it lists with ``timeout_s`` seconds
    per request, into a temporary directory that is always removed. Checks each
    file's existence, byte size, SHA-256 and Parquet row count, recomputes the
    totals, and returns one problem line per mismatch. A URL manifest cannot
    reveal unlisted files: only the files it names are downloaded and checked.

    Raises ``ValueError`` only when the argument cannot be used (an unsupported
    URL scheme, a missing local manifest, a manifest that is not valid JSON or
    does not follow the ``manifests/date=<d>/manifest.json`` layout). A file
    that is missing, modified, unreadable or times out is reported, not raised.
    """
    if timeout_s <= 0:
        raise ValueError(f"timeout_s must be positive, got {timeout_s!r}")
    if "://" in manifest:
        parsed = urlsplit(manifest)
        if parsed.scheme.lower() not in ("http", "https"):
            raise ValueError(
                f"only http and https manifest URLs are supported, got {parsed.scheme!r}"
            )
        if not parsed.netloc:
            raise ValueError(f"manifest URL has no host: {manifest!r}")
        return _verify_url(manifest, timeout_s=timeout_s)
    return _verify_path(manifest)


def _verify_path(manifest: str) -> VerifyReport:
    manifest_path = Path(manifest).expanduser()
    if not manifest_path.is_file():
        raise ValueError(f"manifest not found: {manifest}")
    try:
        raw = manifest_path.read_bytes()
    except OSError as exc:
        raise ValueError(f"cannot read manifest {manifest}: {exc}") from None
    document = _load_document(raw, manifest)
    root = manifest_path.parent.parent.parent
    report = VerifyReport(manifest=manifest, files=len(document["files"]))
    totals = _Totals(files=0, row_count=0, bytes=0)
    listed: set[str] = set()
    for entry in document["files"]:
        relative = entry["path"]
        listed.add(relative)
        unsafe = _unsafe_path(relative)
        if unsafe is not None:
            report.problems.append(f"{unsafe}: {relative}")
            continue
        _record(report, totals, _check_file(root / relative, relative, entry))
    for extra_path in normalized_files(root, document["date"]):
        relative = extra_path.relative_to(root).as_posix()
        if relative not in listed:
            report.problems.append(f"unlisted file: {relative}")
    _check_totals(report, totals, document["totals"])
    return report


def _verify_url(manifest: str, *, timeout_s: float) -> VerifyReport:
    base = _data_root_url(manifest)
    with _TEMPORARY_DIRECTORY(prefix="tickvault-verify-") as temp_dir:
        temp_root = Path(temp_dir)
        with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
            document = _load_document(_fetch_manifest(client, manifest), manifest)
            report = VerifyReport(manifest=manifest, files=len(document["files"]))
            totals = _Totals(files=0, row_count=0, bytes=0)
            for index, entry in enumerate(document["files"]):
                relative = entry["path"]
                unsafe = _unsafe_path(relative)
                if unsafe is not None:
                    report.problems.append(f"{unsafe}: {relative}")
                    continue
                url = urljoin(base, quote(relative, safe="/="))
                destination = temp_root / f"{index:06d}.parquet"
                failure = _download_file(
                    client, url, destination, relative=relative, timeout_s=timeout_s
                )
                if failure is not None:
                    report.problems.append(failure)
                    continue
                _record(report, totals, _check_file(destination, relative, entry))
            _check_totals(report, totals, document["totals"])
    return report


def _check_file(
    path: Path, relative: str, entry: ManifestEntry
) -> _FileCheck:
    """Compare one on-disk file with its manifest entry."""
    if not path.is_file():
        return _FileCheck(problems=[f"missing file: {relative}"], readable=False)
    problems: list[str] = []
    size = path.stat().st_size
    if size != entry["bytes"]:
        problems.append(f"bytes mismatch: {relative}")
    if file_sha256(path) != entry["sha256"]:
        problems.append(f"sha256 mismatch: {relative}")
    try:
        rows = parquet_row_count(path)
    except (OSError, pa.ArrowException):
        problems.append(f"unreadable file: {relative}")
        return _FileCheck(problems=problems, readable=False)
    if rows != entry["row_count"]:
        problems.append(f"row_count mismatch: {relative}")
    return _FileCheck(problems=problems, readable=True, size=size, rows=rows)


def _record(report: VerifyReport, totals: _Totals, check: _FileCheck) -> None:
    """Add one file check to the report and to the recomputed totals."""
    report.problems.extend(check.problems)
    if not check.readable:
        return
    if not check.problems:
        report.ok += 1
    totals["files"] += 1
    totals["bytes"] += check.size
    totals["row_count"] += check.rows


def _check_totals(report: VerifyReport, totals: _Totals, recorded: _Totals) -> None:
    for name in ("files", "row_count", "bytes"):
        if totals[name] != recorded[name]:
            report.problems.append(f"totals.{name} mismatch")


def _fetch_manifest(client: httpx.Client, url: str) -> bytes:
    try:
        with client.stream("GET", url) as response:
            response.raise_for_status()
            return b"".join(response.iter_bytes())
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        raise ValueError(f"cannot download manifest {url}: HTTP {status}") from None
    except (httpx.HTTPError, httpx.InvalidURL) as exc:
        raise ValueError(f"cannot download manifest {url}: {exc}") from None


def _download_file(
    client: httpx.Client,
    url: str,
    destination: Path,
    *,
    relative: str,
    timeout_s: float,
) -> str | None:
    """Stream ``url`` into ``destination``; return a problem line on failure."""
    try:
        with client.stream("GET", url) as response:
            status = response.status_code
            if status in (httpx.codes.NOT_FOUND, httpx.codes.GONE):
                return f"missing file: {relative} (HTTP {status})"
            if status != httpx.codes.OK:
                return f"download failed: {relative} (HTTP {status})"
            with destination.open("wb") as handle:
                for chunk in response.iter_bytes():
                    handle.write(chunk)
    except httpx.TimeoutException:
        return f"timeout downloading {relative} after {timeout_s:g}s"
    except (httpx.HTTPError, httpx.InvalidURL) as exc:
        return f"download failed: {relative} ({exc})"
    except OSError as exc:
        return f"download failed: {relative} ({exc})"
    return None


def _load_document(raw: bytes, source: str) -> ManifestDocument:
    """Parse and validate a manifest document, or raise ``ValueError``."""
    try:
        document = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid manifest {source}: {exc}") from None
    if not isinstance(document, dict):
        raise ValueError(f"invalid manifest {source}: not a JSON object")
    files = document.get("files")
    totals = document.get("totals")
    date = document.get("date")
    if not isinstance(date, str):
        raise ValueError(f"invalid manifest {source}: missing date")
    if not isinstance(files, list):
        raise ValueError(f"invalid manifest {source}: missing files list")
    if not isinstance(totals, dict):
        raise ValueError(f"invalid manifest {source}: missing totals")
    validate_date(date)
    for entry in files:
        _validate_entry(entry, source)
    for name in ("files", "row_count", "bytes"):
        value = totals.get(name)
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError(f"invalid manifest {source}: totals.{name} is not an integer")
    return cast(ManifestDocument, document)


def _validate_entry(entry: object, source: str) -> None:
    if not isinstance(entry, dict):
        raise ValueError(f"invalid manifest {source}: file entry is not an object")
    path = entry.get("path")
    if not isinstance(path, str) or not path:
        raise ValueError(f"invalid manifest {source}: file entry has no path")
    if not isinstance(entry.get("sha256"), str):
        raise ValueError(f"invalid manifest {source}: {path} has no sha256")
    for name in ("row_count", "bytes"):
        value = entry.get(name)
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError(f"invalid manifest {source}: {path} has no integer {name}")


def _unsafe_path(relative: str) -> str | None:
    """Reject entries that would escape the data root (they are data, not code)."""
    parts = relative.split("/")
    if relative.startswith("/") or "\\" in relative or any(
        part in ("", ".", "..") for part in parts
    ):
        return "unsafe path in manifest"
    return None


def _data_root_url(manifest_url: str) -> str:
    """Return the data root URL: the manifest location minus ``manifests/…``."""
    parsed = urlsplit(manifest_url)
    segments = parsed.path.split("/")
    if (
        len(segments) < 4
        or segments[-1] != "manifest.json"
        or segments[-3] != "manifests"
        or not segments[-2].startswith("date=")
    ):
        raise ValueError(
            "manifest URL must end with manifests/date=<date>/manifest.json, "
            f"got {manifest_url!r}"
        )
    root_path = "/".join(segments[:-3])
    if not root_path.startswith("/"):
        root_path = "/" + root_path
    if not root_path.endswith("/"):
        root_path += "/"
    return urlunsplit((parsed.scheme, parsed.netloc, root_path, "", ""))
