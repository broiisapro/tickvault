"""Tests for the committed dashboard sample and its builder.

``dashboard/sample`` is a data root built by
``scripts/build_dashboard_sample.py`` from the two recorded fixtures; the app
falls back to it when no ``TICKVAULT_DATA_DIR`` is set. These tests read the
committed tree (manifests must verify, the root must stay under the size cap,
``dashboard.queries`` must return data) and drive the builder into temporary
roots, including a byte-for-byte reproduction of the committed sample.
"""

from __future__ import annotations

import hashlib
import subprocess
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pytest

from dashboard import queries
from scripts.build_dashboard_sample import main as build_sample
from tickvault.sql import connect
from tickvault.storage.manifest import verify_manifest

ROOT = Path(__file__).resolve().parents[1]
SAMPLE = ROOT / "dashboard" / "sample"
MAX_SAMPLE_BYTES = 100 * 1024 * 1024


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


def test_committed_sample_is_under_the_size_cap() -> None:
    total = sum(path.stat().st_size for path in SAMPLE.rglob("*") if path.is_file())
    assert 0 < total < MAX_SAMPLE_BYTES


def test_committed_sample_manifests_verify() -> None:
    manifests = _manifests(SAMPLE)
    assert manifests
    assert {path.parent.name.removeprefix("date=") for path in manifests} == set(
        queries.qa_dates(SAMPLE)
    )
    for manifest in manifests:
        assert verify_manifest(manifest) == []


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
    assert not book.empty
    assert (book["ask_price"] > book["bid_price"]).all()
    assert not spread.empty
    assert (spread["spread_bps"] >= 0).all()
    assert set(zip(dims["venue"], dims["symbol"], strict=True)) == {
        ("coinbase", "BTC-USD"),
        ("kraken", "BTC-USD"),
    }
    qa = queries.qa_history(SAMPLE)
    assert not qa.empty
    assert (qa["gaps"] == 0).all()
    assert (qa["checksum_failures"] == 0).all()
    assert (qa["unexplained"] == 0).all()


def test_builder_rebuilds_a_verifiable_root(
    tmp_path: Path, synthetic_gap_path: Path
) -> None:
    out = tmp_path / "sample"
    code = build_sample(
        ["--raw", str(synthetic_gap_path), "--out", str(out), "--max-mb", "100"]
    )
    assert code == 0
    manifests = _manifests(out)
    assert len(manifests) == 1
    assert verify_manifest(manifests[0]) == []


def test_builder_fails_over_the_size_cap(
    tmp_path: Path,
    synthetic_gap_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    out = tmp_path / "sample"
    code = build_sample(
        ["--raw", str(synthetic_gap_path), "--out", str(out), "--max-mb", "0.0001"]
    )
    assert code == 1
    assert "over --max-mb" in capsys.readouterr().err


def test_builder_reproduces_the_committed_sample(
    tmp_path: Path, coinbase_5m_path: Path, kraken_5m_path: Path
) -> None:
    out = tmp_path / "sample"
    code = build_sample(
        [
            "--raw",
            str(coinbase_5m_path),
            "--raw",
            str(kraken_5m_path),
            "--out",
            str(out),
            "--max-mb",
            "100",
        ]
    )
    assert code == 0
    assert _tree_digest(out) == _tree_digest(SAMPLE)


def test_builder_rejects_a_missing_input(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    code = build_sample(
        ["--raw", str(tmp_path / "missing.jsonl.zst"), "--out", str(tmp_path)]
    )
    assert code == 2
    assert "raw input not found" in capsys.readouterr().err


def test_builder_refuses_to_rebuild_a_raw_tree(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    raw = tmp_path / "raw" / "venue=coinbase"
    raw.mkdir(parents=True)
    (raw / "conn-00000.jsonl.zst").write_bytes(b"")
    code = build_sample(
        ["--raw", str(tmp_path / "raw"), "--out", str(tmp_path), "--max-mb", "100"]
    )
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
    assert verify_manifest(_manifests(tmp_path / "cli")[0]) == []
