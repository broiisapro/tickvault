"""Tests for the committed dashboard sample and its builder.

``dashboard/sample`` is a data root built by
``scripts/build_dashboard_sample.py`` from a 30-minute live capture of
BTC-USD and ETH-USD on Coinbase and Kraken (decision A44); the app falls back
to it when no ``TICKVAULT_DATA_DIR`` is set. The raw capture is not committed,
so these tests assert the sample's invariants instead of reproducing it: four
venue/symbol pairs, manifests whose SHA-256 and row counts match the files,
QA clean (exit code 0, no missing book, no unexplained anomaly), under the
60 MiB root cap with no file over 45 MiB, and no ``raw/`` tree. The builder
itself is driven into temporary roots with the synthetic fixture.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pytest
from pyarrow import parquet as pq

from dashboard import queries
from scripts.build_dashboard_sample import main as build_sample
from tickvault.qa.report import exit_code as qa_exit_code
from tickvault.qa.report import run_qa
from tickvault.sql import connect
from tickvault.storage.manifest import verify_manifest

ROOT = Path(__file__).resolve().parents[1]
SAMPLE = ROOT / "dashboard" / "sample"
MAX_SAMPLE_BYTES = 60 * 1024 * 1024
MAX_SAMPLE_FILE_BYTES = 45 * 1024 * 1024
_EXPECTED_MARKETS = {
    ("coinbase", "BTC-USD"),
    ("coinbase", "ETH-USD"),
    ("kraken", "BTC-USD"),
    ("kraken", "ETH-USD"),
}


@contextmanager
def _connection(root: Path) -> Iterator[duckdb.DuckDBPyConnection]:
    con = connect(root)
    try:
        yield con
    finally:
        con.close()


def _tree_digest(root: Path) -> dict[str, str]:
    digest: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        if path.is_file():
            digest[path.relative_to(root).as_posix()] = hashlib.sha256(
                path.read_bytes()
            ).hexdigest()
    return digest


def _manifests(root: Path) -> list[Path]:
    return sorted(root.glob("manifests/date=*/manifest.json"))


def _sample_files() -> list[Path]:
    return [path for path in SAMPLE.rglob("*") if path.is_file()]


def test_committed_sample_is_under_the_size_caps() -> None:
    total = sum(path.stat().st_size for path in _sample_files())
    assert 0 < total < MAX_SAMPLE_BYTES
    oversized = [path for path in _sample_files() if path.stat().st_size > MAX_SAMPLE_FILE_BYTES]
    assert oversized == []


def test_committed_sample_holds_no_raw_tree() -> None:
    assert not (SAMPLE / "raw").exists()
    assert not (SAMPLE / "reports").exists()


def test_committed_sample_manifests_verify() -> None:
    manifests = _manifests(SAMPLE)
    assert manifests
    assert {path.parent.name.removeprefix("date=") for path in manifests} == set(
        queries.qa_dates(SAMPLE)
    )
    for manifest in manifests:
        assert verify_manifest(manifest) == []
        document = json.loads(manifest.read_text(encoding="utf-8"))
        assert document["files"]
        for entry in document["files"]:
            file_path = SAMPLE / entry["path"]
            assert file_path.is_file()
            assert hashlib.sha256(file_path.read_bytes()).hexdigest() == entry["sha256"]
            metadata = pq.ParquetFile(file_path).metadata
            assert metadata is not None
            assert metadata.num_rows == entry["row_count"]


def test_committed_sample_qa_is_clean() -> None:
    report = run_qa(SAMPLE)
    assert qa_exit_code(report) == 0
    assert report.rows
    assert {(row.venue, row.symbol) for row in report.rows} == _EXPECTED_MARKETS
    for row in report.rows:
        assert row.unexplained == 0
        assert not row.book_missing
        assert row.messages > 0
        assert row.trades > 0
        assert row.book_deltas > 0
        assert row.venue_snapshots > 0


def test_queries_return_data_on_the_committed_sample() -> None:
    with _connection(SAMPLE) as con:
        rate = queries.message_rate(con)
        events = queries.cumulative_event_counts(con)
        book = queries.top_of_book(con, limit=500)
        spread = queries.spread_mid(con, limit=500)
        dims = queries.dimensions(con)
    assert set(rate["venue"]) == {"coinbase", "kraken"}
    assert (rate["msgs_per_second"] > 0).all()
    assert not events.empty
    assert set(zip(dims["venue"], dims["symbol"], strict=True)) == _EXPECTED_MARKETS
    assert not book.empty
    assert set(zip(book["venue"], book["symbol"], strict=True)) == _EXPECTED_MARKETS
    assert (book["ask_price"] > book["bid_price"]).all()
    assert not spread.empty
    assert (spread["spread_bps"] >= 0).all()
    qa = queries.qa_history(SAMPLE)
    assert not qa.empty
    assert (qa["unexplained"] == 0).all()
    assert not qa["book_missing"].any()


def test_builder_rebuilds_a_verifiable_root(tmp_path: Path, synthetic_gap_path: Path) -> None:
    out = tmp_path / "sample"
    code = build_sample(["--raw", str(synthetic_gap_path), "--out", str(out), "--max-mb", "100"])
    assert code == 0
    manifests = _manifests(out)
    assert len(manifests) == 1
    assert verify_manifest(manifests[0]) == []


def test_builder_is_deterministic(tmp_path: Path, synthetic_gap_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    for out in (first, second):
        code = build_sample(
            ["--raw", str(synthetic_gap_path), "--out", str(out), "--max-mb", "100"]
        )
        assert code == 0
    assert _tree_digest(first) == _tree_digest(second)


def test_builder_fails_over_the_size_cap(
    tmp_path: Path,
    synthetic_gap_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    out = tmp_path / "sample"
    code = build_sample(["--raw", str(synthetic_gap_path), "--out", str(out), "--max-mb", "0.0001"])
    assert code == 1
    assert "over --max-mb" in capsys.readouterr().err


def test_builder_fails_over_the_per_file_cap(
    tmp_path: Path,
    synthetic_gap_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    out = tmp_path / "sample"
    code = build_sample(
        [
            "--raw",
            str(synthetic_gap_path),
            "--out",
            str(out),
            "--max-mb",
            "100",
            "--max-file-mb",
            "0.0001",
        ]
    )
    assert code == 1
    assert "over --max-file-mb" in capsys.readouterr().err


def test_builder_rejects_a_non_positive_file_cap(
    tmp_path: Path,
    synthetic_gap_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    code = build_sample(
        [
            "--raw",
            str(synthetic_gap_path),
            "--out",
            str(tmp_path / "sample"),
            "--max-file-mb",
            "0",
        ]
    )
    assert code == 2
    assert "--max-file-mb must be positive" in capsys.readouterr().err


def test_builder_rejects_a_missing_input(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    code = build_sample(["--raw", str(tmp_path / "missing.jsonl.zst"), "--out", str(tmp_path)])
    assert code == 2
    assert "raw input not found" in capsys.readouterr().err


def test_builder_refuses_to_rebuild_a_raw_tree(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    raw = tmp_path / "raw" / "venue=coinbase"
    raw.mkdir(parents=True)
    (raw / "conn-00000.jsonl.zst").write_bytes(b"")
    code = build_sample(["--raw", str(tmp_path / "raw"), "--out", str(tmp_path), "--max-mb", "100"])
    assert code == 1
    assert "holds a raw tree" in capsys.readouterr().err


def test_builder_cli_exits_zero(tmp_path: Path, synthetic_gap_path: Path) -> None:
    script = ROOT / "scripts" / "build_dashboard_sample.py"
    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            "--raw",
            str(synthetic_gap_path),
            "--out",
            str(tmp_path / "cli"),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0
    assert "largest file:" in completed.stdout
    assert verify_manifest(_manifests(tmp_path / "cli")[0]) == []
