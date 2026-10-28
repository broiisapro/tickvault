"""Phase 2 CLI surface: options, env fallbacks, stubs and exit codes (issue #113)."""

from __future__ import annotations

import asyncio
import json
import re
from pathlib import Path

import pyarrow as pa
import pytest
from typer.testing import CliRunner

import tickvault.cli as cli
from tickvault.buffer import open_buffer
from tickvault.cli import app
from tickvault.features import features_path
from tickvault.models import RawMessage
from tickvault.pipeline import PipelineStats
from tickvault.rawio import encode_line, iter_raw
from tickvault.verify import VerifyReport

runner = CliRunner()

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _plain(text: str) -> str:
    return _ANSI_RE.sub("", text)


class _FakeServer:
    def __init__(self) -> None:
        self.port = 0
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _FakeMetrics:
    """Stands in for PipelineMetrics, so no test binds a real port."""

    def __init__(self) -> None:
        self.servers: list[_FakeServer] = []

    def serve(self, port: int, addr: str = "0.0.0.0") -> _FakeServer:
        server = _FakeServer()
        server.port = port
        self.servers.append(server)
        return server


def _stub_run_live(calls: dict[str, object]) -> object:
    async def run_live(*args: object, **kwargs: object) -> PipelineStats:
        calls["kwargs"] = kwargs
        return PipelineStats(mode="live")

    return run_live


def test_help_lists_the_phase2_commands() -> None:
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    output = _plain(result.output)
    for command in ("features", "consume", "verify"):
        assert command in output


@pytest.mark.parametrize(
    ("command", "options"),
    [
        ("ingest", ["--buffer", "--metrics-port"]),
        ("consume", ["--buffer", "--out", "--metrics-port", "--duration", "--log-level"]),
        ("features", ["--data", "--date", "--out", "--resolution"]),
        ("verify", ["--manifest"]),
    ],
)
def test_new_options_are_in_help(command: str, options: list[str]) -> None:
    result = runner.invoke(app, [command, "--help"])

    assert result.exit_code == 0
    output = _plain(result.output)
    for option in options:
        assert option in output, f"{command} help is missing {option}"


def test_ingest_help_names_both_environment_variables() -> None:
    result = runner.invoke(app, ["ingest", "--help"])

    output = _plain(result.output)
    assert "TICKVAULT_BUFFER" in output
    assert "TICKVAULT_METRICS_PORT" in output


def test_ingest_buffer_option_reaches_the_pipeline(monkeypatch: pytest.MonkeyPatch) -> None:
    """``--buffer`` is opened and handed to ``run_live`` instead of writing locally."""
    monkeypatch.delenv("TICKVAULT_BUFFER", raising=False)
    calls: dict[str, object] = {}
    monkeypatch.setattr(cli, "run_live", _stub_run_live(calls))
    opened: list[str] = []
    sentinel = object()

    def fake_open_buffer(address: str) -> object:
        opened.append(address)
        return sentinel

    monkeypatch.setattr(cli, "open_buffer", fake_open_buffer)

    result = runner.invoke(
        app,
        [
            "ingest",
            "--venues",
            "coinbase",
            "--symbols",
            "BTC-USD",
            "--out",
            "data",
            "--buffer",
            "memory://cli-buffer",
        ],
    )

    assert result.exit_code == 0, result.output
    assert opened == ["memory://cli-buffer"]
    assert calls["kwargs"]["buffer"] is sentinel  # type: ignore[index]


def test_ingest_reads_the_buffer_env_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict[str, object] = {}
    monkeypatch.setattr(cli, "run_live", _stub_run_live(calls))
    monkeypatch.setenv("TICKVAULT_BUFFER", "memory://from-env")
    opened: list[str] = []
    sentinel = object()

    def fake_open_buffer(address: str) -> object:
        opened.append(address)
        return sentinel

    monkeypatch.setattr(cli, "open_buffer", fake_open_buffer)

    result = runner.invoke(
        app,
        ["ingest", "--venues", "coinbase", "--symbols", "BTC-USD", "--out", "data"],
    )

    assert result.exit_code == 0, result.output
    assert opened == ["memory://from-env"]
    assert calls["kwargs"]["buffer"] is sentinel  # type: ignore[index]


def test_ingest_treats_an_empty_buffer_env_as_off(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict[str, object] = {}
    monkeypatch.setattr(cli, "run_live", _stub_run_live(calls))
    monkeypatch.setenv("TICKVAULT_BUFFER", "")
    monkeypatch.setattr(cli, "open_buffer", lambda address: pytest.fail("buffer must stay off"))

    result = runner.invoke(
        app,
        ["ingest", "--venues", "coinbase", "--symbols", "BTC-USD", "--out", "data"],
    )

    assert result.exit_code == 0, result.output
    assert calls["kwargs"]["buffer"] is None  # type: ignore[index]


def test_ingest_serves_metrics_and_passes_them_to_the_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, object] = {}
    metrics = _FakeMetrics()
    monkeypatch.setattr(cli, "run_live", _stub_run_live(calls))
    monkeypatch.setattr(cli, "PipelineMetrics", lambda: metrics)

    result = runner.invoke(
        app,
        [
            "ingest",
            "--venues",
            "coinbase",
            "--symbols",
            "BTC-USD",
            "--out",
            "data",
            "--metrics-port",
            "9108",
        ],
    )

    assert result.exit_code == 0, result.output
    assert calls["kwargs"]["metrics"] is metrics  # type: ignore[index]
    assert [server.port for server in metrics.servers] == [9108]
    assert metrics.servers[0].closed is True


def test_ingest_reads_the_metrics_port_env_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict[str, object] = {}
    metrics = _FakeMetrics()
    monkeypatch.setattr(cli, "run_live", _stub_run_live(calls))
    monkeypatch.setattr(cli, "PipelineMetrics", lambda: metrics)
    monkeypatch.setenv("TICKVAULT_METRICS_PORT", "9109")

    result = runner.invoke(
        app,
        ["ingest", "--venues", "coinbase", "--symbols", "BTC-USD", "--out", "data"],
    )

    assert result.exit_code == 0, result.output
    assert [server.port for server in metrics.servers] == [9109]


def test_ingest_metrics_port_zero_means_off(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict[str, object] = {}
    metrics = _FakeMetrics()
    monkeypatch.setattr(cli, "run_live", _stub_run_live(calls))
    monkeypatch.setattr(cli, "PipelineMetrics", lambda: metrics)

    result = runner.invoke(
        app,
        [
            "ingest",
            "--venues",
            "coinbase",
            "--symbols",
            "BTC-USD",
            "--out",
            "data",
            "--metrics-port",
            "0",
        ],
    )

    assert result.exit_code == 0, result.output
    assert calls["kwargs"]["metrics"] is None  # type: ignore[index]
    assert metrics.servers == []


def test_ingest_rejects_an_out_of_range_metrics_port(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli, "run_live", _stub_run_live({}))

    result = runner.invoke(
        app,
        [
            "ingest",
            "--venues",
            "coinbase",
            "--symbols",
            "BTC-USD",
            "--out",
            "data",
            "--metrics-port",
            "70000",
        ],
    )

    assert result.exit_code == 2
    assert "metrics-port" in result.stderr


def test_ingest_rejects_an_unparseable_buffer_address(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli, "run_live", _stub_run_live({}))

    result = runner.invoke(
        app,
        [
            "ingest",
            "--venues",
            "coinbase",
            "--symbols",
            "BTC-USD",
            "--out",
            "data",
            "--buffer",
            "not-an-address",
        ],
    )

    assert result.exit_code == 1
    assert "error: buffer address must be HOST:PORT or memory://NAME" in result.stderr


def test_consume_writes_a_buffered_record(tmp_path: Path) -> None:
    """``consume`` is the real writer now: it writes what the buffer holds."""
    address = "memory://cli-consume"
    publisher = open_buffer(address)
    message = RawMessage(
        venue="coinbase",
        connection_id="coinbase-cli-consume",
        ingest_seq=0,
        recv_ts_ns=1_793_491_200_000_000_000,
        kind="msg",
        payload=json.dumps(
            {
                "channel": "l2_data",
                "timestamp": "2026-11-01T00:00:00.000000000Z",
                "sequence_num": 1,
                "events": [
                    {
                        "type": "snapshot",
                        "product_id": "BTC-USD",
                        "updates": [
                            {
                                "side": "bid",
                                "event_time": "2026-11-01T00:00:00.000000000Z",
                                "price_level": "81199.9",
                                "new_quantity": "0.5",
                            }
                        ],
                    }
                ],
            }
        ),
    )
    asyncio.run(publisher.start())
    asyncio.run(publisher.publish_raw(encode_line(message)))
    out = tmp_path / "data"

    result = runner.invoke(
        app,
        [
            "consume",
            "--buffer",
            address,
            "--out",
            str(out),
            "--duration",
            "0.2",
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert "writer stopped: duration" in result.output
    assert list(iter_raw(out)) == [message]


@pytest.mark.parametrize("arguments", [["--out", "data"], ["--buffer", "memory://x"]])
def test_consume_requires_buffer_and_out(arguments: list[str]) -> None:
    result = runner.invoke(app, ["consume", *arguments])

    assert result.exit_code == 2


def test_consume_passes_the_buffer_metrics_and_duration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    metrics = _FakeMetrics()
    monkeypatch.setattr(cli, "PipelineMetrics", lambda: metrics)
    monkeypatch.setattr(cli, "open_buffer", lambda address: f"buffer:{address}")

    async def fake_run_writer(*args: object, **kwargs: object) -> PipelineStats:
        captured["args"] = args
        captured["kwargs"] = kwargs
        return PipelineStats(mode="live")

    monkeypatch.setattr(cli, "run_writer", fake_run_writer)

    result = runner.invoke(
        app,
        [
            "consume",
            "--buffer",
            "memory://writer",
            "--out",
            "data",
            "--duration",
            "5",
            "--metrics-port",
            "9109",
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured["args"] == ("buffer:memory://writer", Path("data"))  # type: ignore[comparison-overlap]
    assert captured["kwargs"]["metrics"] is metrics  # type: ignore[index]
    assert captured["kwargs"]["duration"] == 5.0  # type: ignore[index]
    assert metrics.servers[0].closed is True


def test_features_reaches_the_stub() -> None:
    result = runner.invoke(
        app,
        ["features", "--data", "data", "--date", "2026-10-15", "--out", "features"],
    )

    assert result.exit_code == 1
    assert "error: features: implemented in wave 08" in result.stderr


@pytest.mark.parametrize(
    ("date", "resolution"),
    [("2026-13-01", "1s"), ("15-10-2026", "1s"), ("2026-10-15", "5m"), ("2026-10-15", "")],
)
def test_features_rejects_invalid_arguments(date: str, resolution: str) -> None:
    result = runner.invoke(
        app,
        [
            "features",
            "--data",
            "data",
            "--date",
            date,
            "--out",
            "features",
            "--resolution",
            resolution,
        ],
    )

    assert result.exit_code == 2
    assert "error:" in result.stderr


def test_features_prints_one_summary_line_per_resolution(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    table_1s = pa.table(
        {"bar_start_ns": [1, 2, 3]},
        schema=pa.schema([pa.field("bar_start_ns", pa.int64())]),
    )
    table_1m = pa.table(
        {"bar_start_ns": [1]},
        schema=pa.schema([pa.field("bar_start_ns", pa.int64())]),
    )
    paths = [
        features_path(tmp_path, resolution, "coinbase", "2026-10-15", "BTC-USD")
        for resolution in ("1s", "1m")
    ]
    monkeypatch.setattr(
        cli,
        "compute_features",
        lambda data, date, resolutions: {"1s": table_1s, "1m": table_1m},
    )
    monkeypatch.setattr(cli, "write_features", lambda tables, out, date: paths)

    result = runner.invoke(
        app,
        [
            "features",
            "--data",
            str(tmp_path),
            "--date",
            "2026-10-15",
            "--out",
            str(tmp_path),
            "--resolution",
            "1s,1m",
        ],
    )

    assert result.exit_code == 0, result.output
    output = _plain(result.output)
    assert "features 1s: 3 rows, 1 files" in output
    assert "features 1m: 1 rows, 1 files" in output


def test_verify_reaches_the_stub() -> None:
    result = runner.invoke(app, ["verify", "--manifest", "manifest.json"])

    assert result.exit_code == 1
    assert "error: verify: implemented in wave 08" in result.stderr


def test_verify_prints_a_clean_summary_and_exits_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = VerifyReport(manifest="https://example.test/manifest.json", files=2, ok=2)
    monkeypatch.setattr(cli, "verify_remote", lambda manifest: report)

    result = runner.invoke(app, ["verify", "--manifest", "https://example.test/manifest.json"])

    assert result.exit_code == 0, result.output
    assert (
        "verify https://example.test/manifest.json: 2/2 files ok" in _plain(result.output)
    )


def test_verify_prints_each_problem_and_exits_one(monkeypatch: pytest.MonkeyPatch) -> None:
    report = VerifyReport(
        manifest="manifest.json",
        files=2,
        ok=1,
        problems=["part-a.parquet: size 12 != 34"],
    )
    monkeypatch.setattr(cli, "verify_remote", lambda manifest: report)

    result = runner.invoke(app, ["verify", "--manifest", "manifest.json"])

    assert result.exit_code == 1
    output = _plain(result.output)
    assert "verify manifest.json: 1/2 files ok" in output
    assert "part-a.parquet: size 12 != 34" in output
