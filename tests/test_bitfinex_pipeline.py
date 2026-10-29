"""End-to-end Bitfinex pipeline tests (issue #139).

The venue registry entry is what connects the three wave-08 pieces: this module
replays the recorded five-minute session through the public pipeline entry
points (:func:`tickvault.pipeline.run_replay` and the ``tickvault`` CLI) and
checks that the registration is actually exercised -- not merely imported.
"""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from tickvault import cli
from tickvault.book.bitfinex_checksum import bitfinex_checksum
from tickvault.book.processor import BookProcessor
from tickvault.cli import app
from tickvault.models import RawMessage
from tickvault.normalize.bitfinex import normalize
from tickvault.pipeline import PipelineStats, run_replay
from tickvault.qa.report import exit_code, run_qa

runner = CliRunner()

_CONNECTION = "bitfinex-pipeline-test"
_RECV_TS = 1_793_158_778_146_393_000
_BOOK_CHAN = 396954
_TS_MS = 1_793_158_779_000


def _tree_digest(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def test_recorded_session_replays_clean_through_the_registry(
    tmp_path: Path, fixtures_dir: Path
) -> None:
    """Trades and deltas flow, no gap and no checksum failure, QA passes."""
    out = tmp_path / "data"
    stats = run_replay(fixtures_dir / "bitfinex_btc_5m.jsonl.zst", out)

    assert stats.normalize_errors == 0
    assert stats.trades > 0
    assert stats.book_deltas > 0
    assert stats.gaps == 0
    assert stats.checksum_failures == 0
    assert stats.invariant_violations == 0
    assert stats.resyncs == 0
    assert stats.stopped_by == "completed"

    report = run_qa(out)
    assert len(report.rows) == 1
    row = report.rows[0]
    assert (row.venue, row.symbol) == ("bitfinex", "BTC-USD")
    assert row.trades == stats.trades
    assert row.book_deltas == stats.book_deltas
    assert row.venue_snapshots >= 1
    assert row.gaps == 0
    assert row.checksum_failures == 0
    assert row.unexplained == 0
    assert row.book_missing is False
    assert exit_code(report) == 0

    result = runner.invoke(app, ["qa", "--data", str(out)])
    assert result.exit_code == 0, result.output
    assert "bitfinex" in result.output


def test_two_replays_are_byte_identical(tmp_path: Path, fixtures_dir: Path) -> None:
    """The per-connection normalizer state rebuilds identically in one process."""
    first = tmp_path / "first"
    second = tmp_path / "second"
    run_replay(fixtures_dir / "bitfinex_btc_5m.jsonl.zst", first)
    run_replay(fixtures_dir / "bitfinex_btc_5m.jsonl.zst", second)

    first_digest = _tree_digest(first)
    assert first_digest
    assert all(path.startswith("norm/") for path in first_digest)
    assert first_digest == _tree_digest(second)


def test_cli_accepts_the_three_venues(monkeypatch: pytest.MonkeyPatch) -> None:
    """``--venues coinbase,kraken,bitfinex`` validates and reaches ``run_live``."""
    calls: dict[str, Any] = {}

    async def fake_run_live(*args: object, **kwargs: object) -> PipelineStats:
        calls["args"] = args
        calls["kwargs"] = kwargs
        return PipelineStats(mode="live")

    monkeypatch.setattr(cli, "run_live", fake_run_live)

    result = runner.invoke(
        app,
        [
            "ingest",
            "--venues",
            "coinbase,kraken,bitfinex",
            "--symbols",
            "BTC-USD",
            "--out",
            str(Path("unused-data-root")),
        ],
    )

    assert result.exit_code == 0, result.output
    assert list(calls["args"][0]) == ["coinbase", "kraken", "bitfinex"]


def _raw(payload: str, ingest_seq: int, *, kind: str = "msg") -> RawMessage:
    return RawMessage(
        venue="bitfinex",
        connection_id=_CONNECTION,
        ingest_seq=ingest_seq,
        recv_ts_ns=_RECV_TS,
        kind="msg" if kind == "msg" else "control",
        payload=payload,
    )


def test_default_registry_verifies_a_bitfinex_checksum() -> None:
    """The default processor (no explicit map) hashes Bitfinex books at depth 25.

    Without the registry entry the processor would not checksum Bitfinex at
    all, and the deliberately wrong ``cs`` value below would pass silently.
    """
    processor = BookProcessor()
    assert processor.checksum_depths["bitfinex"] == 25

    connect = json.dumps({"event": "connect", "symbols": ["BTC-USD"]})
    processor.process(normalize(_raw(connect, 0, kind="control")))

    subscribed = json.dumps({"event": "subscribed", "channel": "book", "chanId": _BOOK_CHAN})
    processor.process(normalize(_raw(subscribed, 1)))

    snapshot = json.dumps([_BOOK_CHAN, [[100, 1, 1], [101, 1, -1]], 2, _TS_MS])
    processor.process(normalize(_raw(snapshot, 2)))

    expected = bitfinex_checksum([(Decimal("100"), Decimal("1"))], [(Decimal("101"), Decimal("1"))])
    checksum = json.dumps([_BOOK_CHAN, "cs", expected + 1, 3, _TS_MS])

    output = processor.process(normalize(_raw(checksum, 3)))

    assert [event.event_type for event in output.events] == ["checksum_failure"]

    disconnect = json.dumps({"event": "disconnect", "reason": "test_done"})
    processor.process(normalize(_raw(disconnect, 4, kind="control")))
