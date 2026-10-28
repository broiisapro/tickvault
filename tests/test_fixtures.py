"""Fixture integrity tests.

Every raw fixture must decode through ``tickvault.rawio``, follow the raw
envelope contract, and keep the venue text intact; the manifest hashes must
match the committed bytes; the synthetic generator must be reproducible; and
the reconciliation fixture's expected counts must be recomputed independently
from the CSV and Parquet samples. Counts are asserted with their specific
values so a broken fixture cannot pass vacuously.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import re
import subprocess
import sys
from collections.abc import Callable
from decimal import Decimal
from itertools import pairwise
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from tickvault.models import RawMessage

REPO_ROOT = Path(__file__).resolve().parent.parent
FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"
JSONL_FIXTURES = sorted(path.name for path in FIXTURES_DIR.glob("*.jsonl.zst"))
RECONCILE_PARTITION = (
    "norm/trades/venue=coinbase/date=2026-10-01/hour=12/"
    "part-coinbase-20261001T115900Z-00000001-000000000000.parquet"
)

RawLines = Callable[[str | Path], list[RawMessage]]


def _by_connection(lines: list[RawMessage]) -> dict[str, list[RawMessage]]:
    grouped: dict[str, list[RawMessage]] = {}
    for line in lines:
        grouped.setdefault(line.connection_id, []).append(line)
    return grouped


def test_jsonl_fixtures_are_discovered() -> None:
    assert JSONL_FIXTURES == [
        "bitfinex_btc_5m.jsonl.zst",
        "coinbase_btc_5m.jsonl.zst",
        "kraken_btc_5m.jsonl.zst",
        "synthetic_gap_session.jsonl.zst",
    ]


@pytest.mark.parametrize("name", JSONL_FIXTURES)
def test_raw_fixture_structure(
    fixtures_dir: Path, raw_lines: RawLines, name: str
) -> None:
    lines = raw_lines(fixtures_dir / name)
    assert len(lines) > 100
    connections = _by_connection(lines)
    assert connections
    for connection_id, records in connections.items():
        assert records[0].kind == "control"
        connect = json.loads(records[0].payload)
        assert connect["event"] == "connect"
        assert isinstance(connect["subscriptions"], list)
        assert connect["subscriptions"]
        assert records[-1].kind == "control"
        assert json.loads(records[-1].payload)["event"] == "disconnect"
        assert [record.ingest_seq for record in records] == list(range(len(records))), (
            f"{connection_id} ingest_seq is not contiguous"
        )
        assert all(
            earlier.recv_ts_ns <= later.recv_ts_ns for earlier, later in pairwise(records)
        ), f"{connection_id} recv_ts_ns is not non-decreasing"
        for record in records:
            if record.kind == "msg":
                json.loads(record.payload)


def test_fixtures_manifest_matches_files(fixtures_dir: Path, raw_lines: RawLines) -> None:
    manifest = json.loads((fixtures_dir / "fixtures.json").read_text(encoding="utf-8"))
    entries = manifest["fixtures"]
    assert len(entries) == 3
    for entry in entries:
        path = REPO_ROOT / entry["path"]
        assert path.exists(), entry["path"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == entry["sha256"]
        assert len(raw_lines(path)) == entry["line_count"]
        assert entry["venue"] in {"coinbase", "kraken", "bitfinex"}
        assert entry["recorded_at"].endswith("Z")
        assert entry["source_url"].startswith("wss://")
        assert entry["generator"] == "scripts/record_fixture.py"


def test_kraken_payloads_keep_trailing_zeros(
    kraken_5m_path: Path, raw_lines: RawLines
) -> None:
    payloads = [line.payload for line in raw_lines(kraken_5m_path) if line.kind == "msg"]
    assert len(payloads) > 1000
    joined = "\n".join(payloads)
    # These exact texts only survive if the venue bytes are never round-tripped
    # through a float (a float parse would emit "6.12e-05" or strip zeros).
    assert '"qty":0.07250000' in joined
    assert '"qty":0.00020000' in joined
    assert '"qty":0.0725,' not in joined
    assert '"qty":0.0725}' not in joined
    trailing_zero_quantities = re.findall(r'"qty":\d+\.\d*0[,}]', joined)
    assert len(trailing_zero_quantities) > 100


def test_fixture_budget(fixtures_dir: Path) -> None:
    total_bytes = sum(path.stat().st_size for path in fixtures_dir.rglob("*") if path.is_file())
    assert total_bytes < 50 * 1024 * 1024
    # The recordings are real, not empty placeholders.
    assert total_bytes > 1024 * 1024


def test_synthetic_expected_anomalies(
    synthetic_gap_path: Path, synthetic_expected: dict[str, object], raw_lines: RawLines
) -> None:
    counts = synthetic_expected["counts"]
    assert counts["coinbase"]["BTC-USD"]["gaps"] == 1
    assert counts["coinbase"]["BTC-USD"]["resyncs"] == 1
    assert counts["kraken"]["BTC-USD"]["checksum_failures"] == 1
    assert counts["kraken"]["BTC-USD"]["resyncs"] == 1
    assert counts["coinbase"]["BTC-USD"]["unexplained"] == 0
    assert counts["kraken"]["BTC-USD"]["unexplained"] == 0

    sequences = []
    for record in raw_lines(synthetic_gap_path):
        if record.kind != "msg":
            continue
        payload = json.loads(record.payload)
        if "sequence_num" in payload:
            sequences.append(payload["sequence_num"])
    assert sorted(set(range(39)) - set(sequences)) == [22]

    gap, coinbase_resync, checksum_failure, kraken_resync = synthetic_expected["events"]
    assert gap["event_type"] == "gap"
    assert gap["ingest_seq"] == 23
    assert gap["expected_seq"] == 22
    assert gap["received_seq"] == 23
    assert coinbase_resync["event_type"] == "resync"
    assert checksum_failure["event_type"] == "checksum_failure"
    assert checksum_failure["ingest_seq"] == 35
    assert kraken_resync["event_type"] == "resync"


def test_synthetic_generator_check_exits_zero() -> None:
    result = subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts" / "make_synthetic_fixtures.py"), "--check"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    assert result.stdout.count(": OK") == 2


def test_reconcile_counts_match_samples(
    tardis_sample_path: Path,
    tv_trades_sample_root: Path,
    reconcile_expected: dict[str, object],
) -> None:
    with gzip.open(tardis_sample_path, "rt", encoding="utf-8") as handle:
        header = handle.readline().strip()
        rows = [line.rstrip("\n").split(",") for line in handle]
    assert header == "exchange,symbol,timestamp,local_timestamp,id,side,price,amount"
    assert len(rows) == reconcile_expected["tardis_trades"] == 2089
    tardis = {row[4]: (Decimal(row[6]), Decimal(row[7]), row[5]) for row in rows}
    assert len(tardis) == len(rows)

    partition = tv_trades_sample_root / RECONCILE_PARTITION
    assert partition.exists()
    table = pq.read_table(partition)
    tickvault = {
        row["trade_id"]: (row["price"], row["size"], row["side"]) for row in table.to_pylist()
    }
    assert len(tickvault) == table.num_rows == reconcile_expected["tickvault_trades"] == 2087

    missing = sorted(set(tardis) - set(tickvault))
    extra = sorted(set(tickvault) - set(tardis))
    matched = set(tardis) & set(tickvault)
    price_mismatches = sorted(
        trade_id for trade_id in matched if tardis[trade_id][0] != tickvault[trade_id][0]
    )
    size_mismatches = sorted(
        trade_id for trade_id in matched if tardis[trade_id][1] != tickvault[trade_id][1]
    )
    side_mismatches = sorted(
        trade_id for trade_id in matched if tardis[trade_id][2] != tickvault[trade_id][2]
    )

    assert len(missing) == reconcile_expected["missing_in_tickvault"] == 5
    assert len(extra) == reconcile_expected["extra_in_tickvault"] == 3
    assert len(matched) == reconcile_expected["matched"] == 2084
    assert len(price_mismatches) == reconcile_expected["price_mismatches"] == 2
    assert len(size_mismatches) == reconcile_expected["size_mismatches"] == 1
    assert len(side_mismatches) == reconcile_expected["side_mismatches"] == 1
    assert missing == reconcile_expected["missing_in_tickvault_ids"]
    assert extra == reconcile_expected["extra_in_tickvault_ids"]
    assert price_mismatches == reconcile_expected["price_mismatch_ids"]
    assert size_mismatches == reconcile_expected["size_mismatch_ids"]
    assert side_mismatches == reconcile_expected["side_mismatch_ids"]
