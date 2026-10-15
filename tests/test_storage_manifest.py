"""Tests for SHA-256 manifests of the normalized layer (issue #27)."""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tickvault.models import Event, Trade
from tickvault.storage.manifest import verify_manifest, write_manifest
from tickvault.storage.parquet_writer import ParquetWriter
from tickvault.timeutil import parse_rfc3339_ns

_CONNECTION = "kraken-20261015T031901Z-3fa2c1d0"
_HOUR_10 = parse_rfc3339_ns("2026-10-15T10:00:00Z")
_DATE = "2026-10-15"


def _trade(index: int) -> Trade:
    return Trade(
        venue="kraken",
        symbol="BTC-USD",
        exchange_ts_ns=_HOUR_10 + index,
        recv_ts_ns=_HOUR_10 + index,
        connection_id=_CONNECTION,
        ingest_seq=index,
        trade_id=str(index),
        price=Decimal("1"),
        size=Decimal("2"),
        side="buy",
        sequence=index,
    )


def _build_tree(root: Path) -> None:
    writer = ParquetWriter(root, max_rows=2)
    writer.write("trades", [_trade(index) for index in range(5)])
    writer.write(
        "events",
        [
            Event(
                venue="coinbase",
                recv_ts_ns=_HOUR_10 + 7_200_000_000_000,
                connection_id="coinbase-20261015T031902Z-0000000b",
                ingest_seq=0,
                event_type="resync",
                symbol="BTC-USD",
                detail='{"reason":"gap"}',
            )
        ],
    )
    writer.close()


def _load(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def test_manifest_lists_every_file_with_sorted_keys_and_totals(tmp_path: Path) -> None:
    root = tmp_path / "data"
    _build_tree(root)
    files = sorted(
        path
        for path in (root / "norm").rglob("*.parquet")
    )
    assert len(files) == 4

    manifest_path = write_manifest(root, _DATE)

    assert manifest_path == root / "manifests" / f"date={_DATE}" / "manifest.json"
    document = _load(manifest_path)
    assert list(document) == ["date", "files", "totals"]
    entries = document["files"]
    assert [entry["path"] for entry in entries] == sorted(entry["path"] for entry in entries)
    assert [entry["path"] for entry in entries] == [
        path.relative_to(root).as_posix() for path in files
    ]
    for entry in entries:
        assert list(entry) == sorted(entry)
        file_path = root / entry["path"]
        assert entry["sha256"] == hashlib.sha256(file_path.read_bytes()).hexdigest()
        assert entry["bytes"] == file_path.stat().st_size
        assert entry["row_count"] == pq.ParquetFile(file_path).metadata.num_rows
        parts = entry["path"].split("/")
        assert entry["table"] == parts[1]
        assert entry["venue"] == parts[2].removeprefix("venue=")
        assert entry["hour"] == parts[4].removeprefix("hour=")
    totals = document["totals"]
    assert totals["files"] == len(entries)
    assert totals["row_count"] == sum(entry["row_count"] for entry in entries)
    assert totals["bytes"] == sum(entry["bytes"] for entry in entries)
    assert totals["row_count"] == 6


def test_fresh_manifest_verifies_clean(tmp_path: Path) -> None:
    root = tmp_path / "data"
    _build_tree(root)
    manifest_path = write_manifest(root, _DATE)
    assert verify_manifest(manifest_path) == []


def test_verify_manifest_detects_a_modified_file(tmp_path: Path) -> None:
    root = tmp_path / "data"
    _build_tree(root)
    manifest_path = write_manifest(root, _DATE)
    document = _load(manifest_path)
    entry = next(item for item in document["files"] if item["table"] == "trades")
    target = root / entry["path"]
    table = pq.read_table(target)
    price_type = table.schema.field("price").type
    table = table.set_column(
        table.schema.get_field_index("price"),
        "price",
        pa.array([Decimal("9")] * table.num_rows, type=price_type),
    )
    pq.write_table(table, target)

    problems = verify_manifest(manifest_path)

    relative = target.relative_to(root).as_posix()
    assert f"sha256 mismatch: {relative}" in problems


def test_verify_manifest_detects_a_missing_file(tmp_path: Path) -> None:
    root = tmp_path / "data"
    _build_tree(root)
    manifest_path = write_manifest(root, _DATE)
    document = _load(manifest_path)
    target = root / document["files"][-1]["path"]
    target.unlink()

    problems = verify_manifest(manifest_path)

    assert f"missing file: {target.relative_to(root).as_posix()}" in problems
    assert any(problem.startswith("totals.") for problem in problems)


def test_verify_manifest_detects_an_unlisted_file(tmp_path: Path) -> None:
    root = tmp_path / "data"
    _build_tree(root)
    manifest_path = write_manifest(root, _DATE)
    writer = ParquetWriter(root, max_rows=10)
    writer.write(
        "trades",
        [
            Trade(
                venue="kraken",
                symbol="BTC-USD",
                exchange_ts_ns=_HOUR_10 + 10_800_000_000_000,
                recv_ts_ns=_HOUR_10 + 10_800_000_000_000,
                connection_id=_CONNECTION,
                ingest_seq=0,
                trade_id="late",
                price=Decimal("1"),
                size=Decimal("2"),
                side="buy",
            )
        ],
    )
    writer.close()

    problems = verify_manifest(manifest_path)

    assert any(problem.startswith("unlisted file: ") for problem in problems)


def test_manifest_only_covers_its_own_date_and_is_deterministic(tmp_path: Path) -> None:
    root = tmp_path / "data"
    _build_tree(root)
    writer = ParquetWriter(root, max_rows=10)
    writer.write(
        "trades",
        [
            Trade(
                venue="kraken",
                symbol="BTC-USD",
                exchange_ts_ns=parse_rfc3339_ns("2026-10-16T01:00:00Z"),
                recv_ts_ns=parse_rfc3339_ns("2026-10-16T01:00:00Z"),
                connection_id=_CONNECTION,
                ingest_seq=0,
                trade_id="next-day",
                price=Decimal("1"),
                size=Decimal("2"),
                side="buy",
            )
        ],
    )
    writer.close()

    first = write_manifest(root, _DATE).read_bytes()
    second = write_manifest(root, _DATE).read_bytes()

    assert first == second
    document = json.loads(first.decode("utf-8"))
    assert all(entry["path"].startswith("norm/") for entry in document["files"])
    assert all(f"date={_DATE}" in entry["path"] for entry in document["files"])
    assert verify_manifest(root / "manifests" / f"date={_DATE}" / "manifest.json") == []


def test_manifest_for_a_date_without_files_is_empty_and_verifies(tmp_path: Path) -> None:
    root = tmp_path / "data"
    _build_tree(root)
    manifest_path = write_manifest(root, "2026-10-17")
    document = _load(manifest_path)
    assert document["files"] == []
    assert document["totals"] == {"bytes": 0, "files": 0, "row_count": 0}
    assert verify_manifest(manifest_path) == []


def test_manifest_rejects_malformed_dates(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        write_manifest(tmp_path, "2026-10-15T00:00:00Z")
    with pytest.raises(ValueError):
        write_manifest(tmp_path, "2026-02-30")
