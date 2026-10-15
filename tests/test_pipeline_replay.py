"""Replay pipeline tests: DoD shape, byte determinism, raw-layer immutability (issue #51)."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from tickvault.models import RawMessage
from tickvault.pipeline import run_replay
from tickvault.qa.metrics import QARow
from tickvault.qa.report import exit_code, run_qa
from tickvault.rawio import iter_raw
from tickvault.storage.raw_writer import RawWriter

_BAD_RECV_TS_NS = 1_793_491_200_003_000_000


def _tree_digest(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _rows_by_symbol(report_rows: tuple[QARow, ...]) -> dict[tuple[str, str], QARow]:
    return {(row.venue, row.symbol): row for row in report_rows}


def test_synthetic_session_reproduces_the_dod_qa_shape(
    tmp_path: Path, synthetic_gap_path: Path, synthetic_expected: dict[str, Any]
) -> None:
    out = tmp_path / "data"
    stats = run_replay(synthetic_gap_path, out)

    assert stats.mode == "replay"
    assert stats.stopped_by == "completed"
    assert stats.raw_lines == len(list(iter_raw(synthetic_gap_path)))
    assert stats.messages == stats.raw_lines
    assert stats.normalize_errors == 0

    report = run_qa(out)
    rows = _rows_by_symbol(report.rows)
    assert set(rows) == {("coinbase", "BTC-USD"), ("kraken", "BTC-USD")}
    coinbase = rows[("coinbase", "BTC-USD")]
    kraken = rows[("kraken", "BTC-USD")]
    assert (coinbase.gaps, coinbase.resyncs, coinbase.checksum_failures) == (1, 1, 0)
    assert (kraken.gaps, kraken.resyncs, kraken.checksum_failures) == (0, 1, 1)
    assert coinbase.unexplained == 0
    assert kraken.unexplained == 0
    assert exit_code(report) == 0

    expected_counts = synthetic_expected["counts"]
    expected_coinbase = expected_counts["coinbase"]["BTC-USD"]
    expected_kraken = expected_counts["kraken"]["BTC-USD"]
    assert coinbase.trades == expected_coinbase["trades"]
    assert coinbase.book_deltas == expected_coinbase["book_deltas_applied"]
    assert coinbase.venue_snapshots == expected_coinbase["venue_snapshots"]
    assert kraken.trades == expected_kraken["trades"]
    assert kraken.book_deltas == expected_kraken["book_deltas_applied"]
    assert kraken.venue_snapshots == expected_kraken["venue_snapshots"]

    assert stats.trades == expected_coinbase["trades"] + expected_kraken["trades"]
    assert stats.book_deltas == (
        expected_coinbase["book_deltas_applied"] + expected_kraken["book_deltas_applied"]
    )
    assert stats.gaps == 1
    assert stats.resyncs == 2
    assert stats.checksum_failures == 1
    assert stats.invariant_violations == 0
    assert stats.disconnects == 2
    assert stats.resync_requests == 2
    assert stats.events == stats.to_dict()["rows"]["events"]


def test_two_replays_are_byte_identical(tmp_path: Path, synthetic_gap_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    run_replay(synthetic_gap_path, first)
    run_replay(synthetic_gap_path, second)

    first_digest = _tree_digest(first)
    assert first_digest
    assert first_digest == _tree_digest(second)
    assert all(path.startswith("norm/") for path in first_digest)


def test_replay_writes_only_norm_and_never_copies_raw(
    tmp_path: Path, synthetic_gap_path: Path
) -> None:
    out = tmp_path / "data"
    run_replay(synthetic_gap_path, out)

    assert sorted(path.name for path in out.iterdir()) == ["norm"]
    assert not (out / "raw").exists()


def test_recorded_fixtures_replay_with_zero_anomalies(
    tmp_path: Path, coinbase_5m_path: Path, kraken_5m_path: Path
) -> None:
    out = tmp_path / "data"
    coinbase_stats = run_replay(coinbase_5m_path, out)
    kraken_stats = run_replay(kraken_5m_path, out)

    report = run_qa(out)
    rows = _rows_by_symbol(report.rows)
    assert set(rows) == {("coinbase", "BTC-USD"), ("kraken", "BTC-USD")}
    for row in report.rows:
        assert row.gaps == 0
        assert row.checksum_failures == 0
        assert row.invariant_violations == 0
        assert row.unexplained == 0
        assert row.book_deltas > 0
        assert row.venue_snapshots > 0
    assert rows[("coinbase", "BTC-USD")].trades == coinbase_stats.trades > 0
    assert rows[("kraken", "BTC-USD")].trades == kraken_stats.trades > 0
    assert exit_code(report) == 0


def test_normalize_errors_are_counted_and_skipped(
    tmp_path: Path, synthetic_gap_path: Path
) -> None:
    raw_root = tmp_path / "raw-root"
    messages = list(iter_raw(synthetic_gap_path))
    with RawWriter(raw_root) as raw_writer:
        for message in messages:
            raw_writer.write(message)
        raw_writer.write(
            RawMessage(
                venue="coinbase",
                connection_id="coinbase-20261101T000010Z-bad0bad0",
                ingest_seq=0,
                recv_ts_ns=_BAD_RECV_TS_NS,
                kind="msg",
                payload="{not json",
            )
        )

    out = tmp_path / "data"
    stats = run_replay(raw_root, out)

    assert stats.raw_lines == len(messages) + 1
    assert stats.messages == len(messages)
    assert stats.normalize_errors == 1
    report = run_qa(out)
    assert exit_code(report) == 0
    rows = _rows_by_symbol(report.rows)
    assert rows[("coinbase", "BTC-USD")].gaps == 1
    assert rows[("kraken", "BTC-USD")].checksum_failures == 1


def test_stats_to_dict_is_stable_and_json_friendly(
    tmp_path: Path, synthetic_gap_path: Path
) -> None:
    first = run_replay(synthetic_gap_path, tmp_path / "first")
    second = run_replay(synthetic_gap_path, tmp_path / "second")

    assert first.to_dict() == second.to_dict()
    document = first.to_dict()
    assert list(document) == [
        "mode",
        "stopped_by",
        "raw_lines",
        "messages",
        "normalize_errors",
        "rows",
        "events",
        "resync_requests",
    ]
    assert document["rows"] == {
        "trades": first.trades,
        "book_deltas": first.book_deltas,
        "book_snapshots": first.book_snapshots,
        "events": first.events,
    }
    assert document["events"] == {
        "gap": 1,
        "resync": 2,
        "checksum_failure": 1,
        "invariant_violation": 0,
        "reconnect": 0,
        "disconnect": 2,
    }
    json.dumps(document)
