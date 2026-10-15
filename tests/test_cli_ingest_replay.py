"""CLI ``ingest`` and ``replay`` bodies (issue #52)."""

from __future__ import annotations

import asyncio
import filecmp
import json
import logging
from collections.abc import AsyncIterator, Callable, Sequence
from pathlib import Path

import pytest
from typer.testing import CliRunner

from tickvault.cli import app
from tickvault.models import RawMessage
from tickvault.rawio import iter_raw
from tickvault.sql import run_query

runner = CliRunner()

_BASE_NS = 1_793_491_200_000_000_000
_EXCHANGE_TS = "2026-11-01T00:00:00.000000000Z"


def _l2_message(sequence: int, recv_ts_ns: int) -> RawMessage:
    """A valid endless Coinbase l2 session: snapshot first, updates after."""
    if sequence == 1:
        event: dict[str, object] = {
            "type": "snapshot",
            "product_id": "BTC-USD",
            "updates": [
                {
                    "side": "bid",
                    "event_time": _EXCHANGE_TS,
                    "price_level": "81199.9",
                    "new_quantity": "0.5",
                },
                {
                    "side": "offer",
                    "event_time": _EXCHANGE_TS,
                    "price_level": "81200.1",
                    "new_quantity": "0.6",
                },
            ],
        }
    else:
        event = {
            "type": "update",
            "product_id": "BTC-USD",
            "updates": [
                {
                    "side": "bid",
                    "event_time": _EXCHANGE_TS,
                    "price_level": "81199.9",
                    "new_quantity": "0.55",
                }
            ],
        }
    payload = json.dumps(
        {
            "channel": "l2_data",
            "timestamp": _EXCHANGE_TS,
            "sequence_num": sequence,
            "events": [event],
        }
    )
    return RawMessage(
        venue="coinbase",
        connection_id="coinbase-cli-fake",
        ingest_seq=sequence - 1,
        recv_ts_ns=recv_ts_ns,
        kind="msg",
        payload=payload,
    )


class StubConnector:
    """Streams a fixed message list once and records ``resync`` calls."""

    def __init__(self, venue: str, messages: Sequence[RawMessage] = ()) -> None:
        self.venue = venue
        self.messages = list(messages)
        self.resync_calls: list[tuple[str, str]] = []
        self.closed = False
        self.symbols: tuple[str, ...] = ()

    async def subscribe(self, symbols: Sequence[str]) -> None:
        self.symbols = tuple(symbols)

    async def stream(self) -> AsyncIterator[RawMessage]:
        for message in self.messages:
            if self.closed:
                return
            yield message

    async def resync(self, symbol: str, reason: str) -> None:
        self.resync_calls.append((symbol, reason))

    async def close(self) -> None:
        self.closed = True


class EndlessConnector(StubConnector):
    """Streams a valid Coinbase l2 session until closed."""

    def __init__(self, venue: str = "coinbase", delay: float = 0.001) -> None:
        super().__init__(venue)
        self.delay = delay

    async def stream(self) -> AsyncIterator[RawMessage]:
        sequence = 1
        recv_ts_ns = _BASE_NS
        while not self.closed:
            yield _l2_message(sequence, recv_ts_ns)
            sequence += 1
            recv_ts_ns += 1_000_000
            await asyncio.sleep(self.delay)


def _patch_connector_factories(
    monkeypatch: pytest.MonkeyPatch, connectors: dict[str, StubConnector]
) -> None:
    def factories() -> dict[str, Callable[[Sequence[str]], StubConnector]]:
        return {
            venue: (lambda symbols, connector=connector: connector)
            for venue, connector in connectors.items()
        }

    monkeypatch.setattr("tickvault.pipeline.default_connector_factories", factories)


def _assert_trees_equal(first: Path, second: Path) -> None:
    comparison = filecmp.dircmp(first, second)
    assert comparison.left_only == []
    assert comparison.right_only == []
    assert comparison.diff_files == []
    assert comparison.funny_files == []
    for name in comparison.common_dirs:
        _assert_trees_equal(first / name, second / name)


def test_replay_synthetic_fixture_produces_expected_rows(
    tmp_path: Path, synthetic_gap_path: Path
) -> None:
    out = tmp_path / "data"
    result = runner.invoke(app, ["replay", "--input", str(synthetic_gap_path), "--out", str(out)])

    assert result.exit_code == 0, result.output
    assert "trades=9" in result.output
    assert "gap=1" in result.output
    assert "resync=2" in result.output
    assert "checksum_failure=1" in result.output
    assert "replay stopped: completed" in result.output
    _, trades = run_query(out, "select count(*) from trades")
    assert trades == [(9,)]
    _, events = run_query(out, "select count(*) from events")
    assert events == [(6,)]


def test_replay_twice_produces_identical_trees(tmp_path: Path, synthetic_gap_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    first_result = runner.invoke(
        app, ["replay", "--input", str(synthetic_gap_path), "--out", str(first)]
    )
    second_result = runner.invoke(
        app, ["replay", "--input", str(synthetic_gap_path), "--out", str(second)]
    )

    assert first_result.exit_code == 0, first_result.output
    assert second_result.exit_code == 0, second_result.output
    _assert_trees_equal(first, second)
    first_files = sorted(path.relative_to(first) for path in first.rglob("*.parquet"))
    assert first_files
    for relative in first_files:
        assert (first / relative).read_bytes() == (second / relative).read_bytes()


def test_replay_missing_input_exits_1(tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        ["replay", "--input", str(tmp_path / "missing"), "--out", str(tmp_path / "out")],
    )
    assert result.exit_code == 1
    assert "error:" in result.stderr


def test_ingest_streams_injected_connectors(
    tmp_path: Path, synthetic_gap_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    messages = list(iter_raw(synthetic_gap_path))
    coinbase = StubConnector("coinbase", [m for m in messages if m.venue == "coinbase"])
    kraken = StubConnector("kraken", [m for m in messages if m.venue == "kraken"])
    _patch_connector_factories(monkeypatch, {"coinbase": coinbase, "kraken": kraken})
    out = tmp_path / "data"

    result = runner.invoke(
        app,
        [
            "ingest",
            "--venues",
            "coinbase,kraken",
            "--symbols",
            "BTC-USD",
            "--out",
            str(out),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "raw messages: 105" in result.output
    assert "live stopped: completed" in result.output
    assert list(iter_raw(out)) == messages
    assert coinbase.resync_calls == [("BTC-USD", "gap")]
    assert kraken.resync_calls == [("BTC-USD", "checksum_failure")]
    _, trades = run_query(out, "select count(*) from trades")
    assert trades == [(9,)]


def test_ingest_max_gb_exits_3(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    connector = EndlessConnector()
    _patch_connector_factories(monkeypatch, {"coinbase": connector})
    out = tmp_path / "data"

    result = runner.invoke(
        app,
        [
            "ingest",
            "--venues",
            "coinbase",
            "--symbols",
            "BTC-USD",
            "--out",
            str(out),
            "--max-gb",
            "0.000000001",
        ],
    )

    assert result.exit_code == 3, result.output
    assert "live stopped: max_gb" in result.output
    assert connector.closed
    assert list(iter_raw(out))


def test_ingest_stops_after_duration(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    connector = EndlessConnector()
    _patch_connector_factories(monkeypatch, {"coinbase": connector})
    out = tmp_path / "data"

    result = runner.invoke(
        app,
        [
            "ingest",
            "--venues",
            "coinbase",
            "--symbols",
            "BTC-USD",
            "--out",
            str(out),
            "--duration",
            "1",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "live stopped: duration" in result.output
    assert connector.closed
    assert list(iter_raw(out))


def test_ingest_progress_interval_controls_log_lines(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="tickvault.pipeline")
    _patch_connector_factories(monkeypatch, {"coinbase": EndlessConnector()})

    result = runner.invoke(
        app,
        [
            "ingest",
            "--venues",
            "coinbase",
            "--symbols",
            "BTC-USD",
            "--out",
            str(tmp_path / "data"),
            "--duration",
            "1",
            "--progress-interval",
            "0.1",
            "--log-level",
            "DEBUG",
        ],
    )

    assert result.exit_code == 0, result.output
    progress = [
        record for record in caplog.records if "ingest progress" in record.message
    ]
    assert 2 <= len(progress) <= 12

    caplog.clear()
    _patch_connector_factories(monkeypatch, {"coinbase": EndlessConnector()})
    disabled = runner.invoke(
        app,
        [
            "ingest",
            "--venues",
            "coinbase",
            "--symbols",
            "BTC-USD",
            "--out",
            str(tmp_path / "data-disabled"),
            "--duration",
            "1",
            "--progress-interval",
            "0",
        ],
    )

    assert disabled.exit_code == 0, disabled.output
    assert not [
        record for record in caplog.records if "ingest progress" in record.message
    ]


def test_ingest_rejects_unknown_log_level(tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        [
            "ingest",
            "--venues",
            "coinbase",
            "--symbols",
            "BTC-USD",
            "--out",
            str(tmp_path / "data"),
            "--log-level",
            "loud",
        ],
    )

    assert result.exit_code == 2
    assert "unknown log level" in result.stderr
    assert not (tmp_path / "data").exists()


@pytest.mark.parametrize(
    ("option", "value", "message"),
    [
        ("--venues", "binance", "unsupported venue"),
        ("--venues", "coinbase,ftx", "unsupported venue"),
        ("--venues", "", "at least one venue"),
        ("--symbols", "btc-usd", "invalid symbol"),
        ("--symbols", "BTC/USD", "invalid symbol"),
        ("--symbols", "", "at least one symbol"),
    ],
)
def test_ingest_argument_validation(
    tmp_path: Path, option: str, value: str, message: str
) -> None:
    args = [
        "ingest",
        "--venues",
        "coinbase",
        "--symbols",
        "BTC-USD",
        "--out",
        str(tmp_path / "data"),
    ]
    index = args.index(option)
    args[index + 1] = value

    result = runner.invoke(app, args)

    assert result.exit_code == 2
    assert message in result.stderr
    assert not (tmp_path / "data").exists()
