"""Pipeline metrics tests: counters match the stats, no effect on output (issue #122)."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable, Sequence
from pathlib import Path

import pytest

from tickvault.buffer import MemoryBroker, MemoryBuffer
from tickvault.connectors.base import Connector
from tickvault.metrics import PipelineMetrics
from tickvault.models import RawMessage
from tickvault.pipeline import run_live, run_writer
from tickvault.rawio import encode_line, iter_raw

_BASE_NS = 1_793_491_200_000_000_000
_EXCHANGE_TS = "2026-11-01T00:00:00.000000000Z"


def _coinbase_message(sequence: int, recv_ts_ns: int, connection_id: str) -> RawMessage:
    event: dict[str, object]
    if sequence == 1:
        event = {
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
        connection_id=connection_id,
        ingest_seq=sequence - 1,
        recv_ts_ns=recv_ts_ns,
        kind="msg",
        payload=payload,
    )


class ScriptedConnector:
    """Yield fixed raw messages, then optionally hold the connection open."""

    def __init__(
        self, venue: str, messages: Sequence[RawMessage], *, hold: bool = False
    ) -> None:
        self.venue = venue
        self.messages = list(messages)
        self.hold = hold
        self.symbols: tuple[str, ...] = ()
        self.closed = False
        self.yielded = 0
        self.yielded_at_close: int | None = None
        self._stop = asyncio.Event()

    async def subscribe(self, symbols: Sequence[str]) -> None:
        self.symbols = tuple(symbols)

    async def resync(self, symbol: str, reason: str) -> None:
        return None

    async def close(self) -> None:
        self.closed = True
        if self.yielded_at_close is None:
            self.yielded_at_close = self.yielded
        self._stop.set()

    async def stream(self) -> AsyncIterator[RawMessage]:
        for message in self.messages:
            if self.closed:
                return
            self.yielded += 1
            yield message
        if not self.hold:
            return
        sequence = 2
        recv_ts_ns = _BASE_NS + 1_000_000
        while not self.closed:
            self.yielded += 1
            yield _coinbase_message(sequence, recv_ts_ns, "metric-connector")
            sequence += 1
            recv_ts_ns += 1_000_000


def _factory(connector: Connector) -> Callable[[Sequence[str]], Connector]:
    def build(symbols: Sequence[str]) -> Connector:
        connector.symbols = tuple(symbols)
        return connector

    return build


def _total(metrics: PipelineMetrics, name: str, **labels: str) -> float:
    """Sum every sample of ``name`` whose labels match, over all series."""
    total = 0.0
    for family in metrics.registry.collect():
        for sample in family.samples:
            if sample.name != name:
                continue
            if any(sample.labels.get(key) != value for key, value in labels.items()):
                continue
            total += sample.value
    return total


def _gauge(metrics: PipelineMetrics, name: str, **labels: str) -> float | None:
    return metrics.registry.get_sample_value(name, labels)


async def _wait_until(predicate: Callable[[], bool], *, timeout: float = 10.0) -> None:
    async def poll() -> None:
        while not predicate():
            await asyncio.sleep(0.005)

    await asyncio.wait_for(poll(), timeout=timeout)


async def test_live_metrics_counters_equal_the_stats(
    tmp_path: Path, synthetic_gap_path: Path
) -> None:
    messages = list(iter_raw(synthetic_gap_path))
    connectors = {
        venue: ScriptedConnector(
            venue, [message for message in messages if message.venue == venue]
        )
        for venue in ("coinbase", "kraken")
    }
    metrics = PipelineMetrics()

    stats = await run_live(
        ["coinbase", "kraken"],
        ["BTC-USD"],
        tmp_path / "data",
        connector_factories={venue: _factory(connector) for venue, connector in connectors.items()},
        metrics=metrics,
        progress_interval=0,
    )

    assert _total(metrics, "tickvault_messages_total") == stats.raw_lines
    assert _total(metrics, "tickvault_messages_total") == len(messages)
    assert _total(metrics, "tickvault_raw_bytes_total") == sum(
        len(encode_line(message)) for message in messages
    )
    assert _total(metrics, "tickvault_rows_total", table="trades") == stats.trades
    assert _total(metrics, "tickvault_rows_total", table="book_deltas") == stats.book_deltas
    assert _total(metrics, "tickvault_rows_total", table="book_snapshots") == stats.book_snapshots
    assert _total(metrics, "tickvault_rows_total", table="events") == stats.events
    assert _total(metrics, "tickvault_events_total", event_type="gap") == stats.gaps
    assert (
        _total(metrics, "tickvault_events_total", event_type="resync") == stats.resyncs
    )
    assert (
        _total(metrics, "tickvault_events_total", event_type="checksum_failure")
        == stats.checksum_failures
    )
    assert (
        _total(metrics, "tickvault_events_total", event_type="invariant_violation")
        == stats.invariant_violations
    )
    assert (
        _total(metrics, "tickvault_events_total", event_type="reconnect") == stats.reconnects
    )
    assert _total(metrics, "tickvault_normalize_errors_total") == stats.normalize_errors
    assert _gauge(metrics, "tickvault_book_live", venue="coinbase", symbol="BTC-USD") == 1.0
    assert _gauge(metrics, "tickvault_book_live", venue="kraken", symbol="BTC-USD") == 1.0


async def test_metrics_do_not_change_the_normalized_output(
    tmp_path: Path, synthetic_gap_path: Path
) -> None:
    messages = list(iter_raw(synthetic_gap_path))
    outputs: list[Path] = []

    for index, metrics in enumerate((None, PipelineMetrics())):
        connectors = {
            venue: ScriptedConnector(
                venue, [message for message in messages if message.venue == venue]
            )
            for venue in ("coinbase", "kraken")
        }
        out = tmp_path / f"data-{index}"
        await run_live(
            ["coinbase", "kraken"],
            ["BTC-USD"],
            out,
            connector_factories={
                venue: _factory(connector) for venue, connector in connectors.items()
            },
            metrics=metrics,
            progress_interval=0,
        )
        outputs.append(out)

    files = sorted(path.relative_to(outputs[0]) for path in (outputs[0] / "norm").rglob("*"))
    assert files
    assert files == sorted(
        path.relative_to(outputs[1]) for path in (outputs[1] / "norm").rglob("*")
    )
    for relative in files:
        if (outputs[0] / relative).is_dir():
            continue
        assert (outputs[0] / relative).read_bytes() == (outputs[1] / relative).read_bytes()


async def test_the_live_tick_refreshes_the_last_message_age(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import tickvault.pipeline as pipeline_module

    monkeypatch.setattr(pipeline_module, "_LIVE_TICK_S", 0.01)
    connector = ScriptedConnector("coinbase", [], hold=True)
    metrics = PipelineMetrics()
    stop_event = asyncio.Event()
    task = asyncio.create_task(
        run_live(
            ["coinbase"],
            ["BTC-USD"],
            tmp_path / "data",
            connector_factories={"coinbase": _factory(connector)},
            metrics=metrics,
            stop_event=stop_event,
            progress_interval=0,
        )
    )
    try:
        await _wait_until(
            lambda: _gauge(
                metrics, "tickvault_last_message_age_seconds", venue="coinbase"
            )
            is not None
        )
    finally:
        stop_event.set()
        stats = await asyncio.wait_for(task, timeout=20)

    age = _gauge(metrics, "tickvault_last_message_age_seconds", venue="coinbase")
    assert age is not None
    assert age >= 0.0
    assert stats.stopped_by == "stop_event"


async def test_the_writer_counts_consumed_records(
    tmp_path: Path, synthetic_gap_path: Path
) -> None:
    messages = list(iter_raw(synthetic_gap_path))
    broker = MemoryBroker()
    buffer = MemoryBuffer(broker)
    await buffer.start()
    for message in messages:
        await buffer.publish_raw(encode_line(message))
    metrics = PipelineMetrics()
    stop_event = asyncio.Event()

    task = asyncio.create_task(
        run_writer(
            MemoryBuffer(broker),
            tmp_path / "data",
            metrics=metrics,
            stop_event=stop_event,
        )
    )
    await _wait_until(
        lambda: _total(metrics, "tickvault_buffer_consumed_total") >= len(messages)
    )
    stop_event.set()
    stats = await asyncio.wait_for(task, timeout=20)

    assert stats.mode == "writer"
    assert stats.stopped_by == "stop_event"
    assert _total(metrics, "tickvault_buffer_consumed_total") == len(messages)
    assert _total(metrics, "tickvault_rows_total", table="trades") == stats.trades
    assert _gauge(metrics, "tickvault_book_live", venue="coinbase", symbol="BTC-USD") == 1.0
    assert _gauge(metrics, "tickvault_book_live", venue="kraken", symbol="BTC-USD") == 1.0
