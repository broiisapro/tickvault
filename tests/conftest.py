"""Shared fixtures for the TickVault test suite."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import pytest

from tickvault.models import RawMessage
from tickvault.rawio import iter_raw

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


@pytest.fixture(scope="session")
def fixtures_dir() -> Path:
    return FIXTURES_DIR


@pytest.fixture(scope="session")
def coinbase_5m_path(fixtures_dir: Path) -> Path:
    return fixtures_dir / "coinbase_btc_5m.jsonl.zst"


@pytest.fixture(scope="session")
def kraken_5m_path(fixtures_dir: Path) -> Path:
    return fixtures_dir / "kraken_btc_5m.jsonl.zst"


@pytest.fixture(scope="session")
def synthetic_gap_path(fixtures_dir: Path) -> Path:
    return fixtures_dir / "synthetic_gap_session.jsonl.zst"


@pytest.fixture(scope="session")
def synthetic_expected(fixtures_dir: Path) -> dict[str, object]:
    path = fixtures_dir / "synthetic_gap_session.expected.json"
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.fixture(scope="session")
def tardis_sample_path(fixtures_dir: Path) -> Path:
    return fixtures_dir / "tardis_coinbase_trades_sample.csv.gz"


@pytest.fixture(scope="session")
def tv_trades_sample_root(fixtures_dir: Path) -> Path:
    return fixtures_dir / "tv_coinbase_trades_sample"


@pytest.fixture(scope="session")
def reconcile_expected(fixtures_dir: Path) -> dict[str, object]:
    path = fixtures_dir / "reconcile_expected.json"
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.fixture(scope="session")
def raw_lines() -> Callable[[str | Path], list[RawMessage]]:
    """Read a raw fixture (or raw tree) in canonical order."""

    def _raw_lines(path: str | Path) -> list[RawMessage]:
        return list(iter_raw(path))

    return _raw_lines
