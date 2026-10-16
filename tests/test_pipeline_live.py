"""Live pipeline tests: raw-first persistence, resync routing, stop paths (issue #51)."""

from __future__ import annotations

import asyncio
import gc
import json
import logging
import re
import signal
import subprocess
import sys
import time
from collections.abc import AsyncIterator, Callable, Sequence
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from tickvault.connectors.base import Connector
from tickvault.models import RawMessage
from tickvault.pipeline import run_live
from tickvault.qa.report import exit_code, run_qa
from tickvault.rawio import iter_raw

_BASE_NS = 1_793_491_200_000_000_000
_EXCHANGE_TS = "2026-11-01T00:00:00.000000000Z"


def _coinbase_message(sequence: int, recv_ts_ns: int, connection_id: str) -> RawMessage:
    """One valid Coinbase l2 session message: a snapshot first, updates after."""
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
        connection_id=connection_id,
        ingest_seq=sequence - 1,
        recv_ts_ns=recv_ts_ns,
        kind="msg",
        payload=payload,
    )


class FakeConnector:
    """A connector replaying fixed messages or an endless valid l2 session."""

    def __init__(
        self,
        venue: str,
        messages: Sequence[RawMessage] = (),
        *,
        infinite: bool = False,
        delay: float = 0.0,
        connection_id: str = "fake-connector",
    ) -> None:
        self.venue = venue
        self.messages = list(messages)
        self.infinite = infinite
        self.delay = delay
        self.connection_id = connection_id
        self.resync_calls: list[tuple[str, str]] = []
        self.closed = False
        self.symbols: tuple[str, ...] = ()

    async def subscribe(self, symbols: Sequence[str]) -> None:
        self.symbols = tuple(symbols)

    async def resync(self, symbol: str, reason: str) -> None:
        self.resync_calls.append((symbol, reason))

    async def close(self) -> None:
        self.closed = True

    async def stream(self) -> AsyncIterator[RawMessage]:
        for message in self.messages:
            if self.closed:
                return
            yield message
            if self.delay:
                await asyncio.sleep(self.delay)
        if not self.infinite:
            return
        sequence = 1
        recv_ts_ns = _BASE_NS
        while not self.closed:
            yield _coinbase_message(sequence, recv_ts_ns, self.connection_id)
            sequence += 1
            recv_ts_ns += 1_000_000
            if self.delay:
                await asyncio.sleep(self.delay)


class FastConnector:
    """A valid Coinbase l2 session yielded as fast as the consumer takes it.

    ``yielded`` counts the messages the generator handed to the pipeline and
    ``yielded_at_close`` freezes that count when :meth:`close` runs, which is
    the set of frames the pipeline must persist during a graceful stop.
    """

    def __init__(self, venue: str = "coinbase") -> None:
        self.venue = venue
        self.symbols: tuple[str, ...] = ()
        self.closed = False
        self.yielded = 0
        self.yielded_at_close: int | None = None

    async def subscribe(self, symbols: Sequence[str]) -> None:
        self.symbols = tuple(symbols)

    async def resync(self, symbol: str, reason: str) -> None:
        return None

    async def close(self) -> None:
        self.closed = True
        if self.yielded_at_close is None:
            self.yielded_at_close = self.yielded

    async def stream(self) -> AsyncIterator[RawMessage]:
        sequence = 1
        recv_ts_ns = _BASE_NS
        while not self.closed:
            self.yielded += 1
            yield _coinbase_message(sequence, recv_ts_ns, "fast-connector")
            sequence += 1
            recv_ts_ns += 1_000_000


def _factory(connector: Connector) -> Callable[[Sequence[str]], Connector]:
    def build(symbols: Sequence[str]) -> Connector:
        connector.symbols = tuple(symbols)
        return connector

    return build


async def test_live_writes_raw_and_norm_and_routes_resync(
    tmp_path: Path, synthetic_gap_path: Path
) -> None:
    messages = list(iter_raw(synthetic_gap_path))
    coinbase = FakeConnector("coinbase", [m for m in messages if m.venue == "coinbase"])
    kraken = FakeConnector("kraken", [m for m in messages if m.venue == "kraken"])
    out = tmp_path / "data"

    stats = await run_live(
        ["coinbase", "kraken"],
        ["BTC-USD"],
        out,
        connector_factories={"coinbase": _factory(coinbase), "kraken": _factory(kraken)},
    )

    assert stats.mode == "live"
    assert stats.stopped_by == "completed"
    assert stats.raw_lines == len(messages)
    assert stats.normalize_errors == 0
    assert stats.gaps == 1
    assert stats.checksum_failures == 1
    assert stats.resync_requests == 2
    assert coinbase.resync_calls == [("BTC-USD", "gap")]
    assert kraken.resync_calls == [("BTC-USD", "checksum_failure")]
    assert coinbase.closed and kraken.closed

    assert list(iter_raw(out)) == messages
    report = run_qa(out)
    assert exit_code(report) == 0
    parquet_files = list((out / "norm").rglob("*.parquet"))
    assert parquet_files
    assert sum(pq.read_table(path).num_rows for path in parquet_files) > 0


async def test_raw_is_persisted_even_when_normalization_fails(tmp_path: Path) -> None:
    bad = RawMessage(
        venue="coinbase",
        connection_id="coinbase-20261101T000000Z-feedfeed",
        ingest_seq=0,
        recv_ts_ns=_BASE_NS,
        kind="msg",
        payload="{not json",
    )
    good = RawMessage(
        venue="coinbase",
        connection_id="coinbase-20261101T000000Z-feedfeed",
        ingest_seq=1,
        recv_ts_ns=_BASE_NS + 1_000_000,
        kind="msg",
        payload=json.dumps(
            {
                "channel": "heartbeats",
                "timestamp": _EXCHANGE_TS,
                "sequence_num": 1,
                "events": [],
            }
        ),
    )
    connector = FakeConnector("coinbase", [bad, good])
    out = tmp_path / "data"

    stats = await run_live(
        ["coinbase"], ["BTC-USD"], out, connector_factories={"coinbase": _factory(connector)}
    )

    assert stats.raw_lines == 2
    assert stats.normalize_errors == 1
    assert stats.messages == 1
    assert list(iter_raw(out)) == [bad, good]


async def test_stop_event_stops_live_gracefully(tmp_path: Path) -> None:
    connector = FakeConnector("coinbase", infinite=True, delay=0.001)
    out = tmp_path / "data"
    stop_event = asyncio.Event()

    async def wait_for_raw_then_stop() -> None:
        while not (out / "raw").exists():
            await asyncio.sleep(0.01)
        stop_event.set()

    task = asyncio.create_task(
        run_live(
            ["coinbase"],
            ["BTC-USD"],
            out,
            connector_factories={"coinbase": _factory(connector)},
            stop_event=stop_event,
        )
    )
    await asyncio.wait_for(wait_for_raw_then_stop(), timeout=15)
    stats = await asyncio.wait_for(task, timeout=15)

    assert stats.stopped_by == "stop_event"
    assert connector.closed
    raw_messages = list(iter_raw(out))
    assert raw_messages
    parquet_files = list((out / "norm").rglob("*.parquet"))
    assert parquet_files
    assert sum(pq.read_table(path).num_rows for path in parquet_files) > 0


async def test_max_gb_stops_live_gracefully(tmp_path: Path) -> None:
    connector = FakeConnector("coinbase", infinite=True, delay=0.001)
    out = tmp_path / "data"

    stats = await asyncio.wait_for(
        run_live(
            ["coinbase"],
            ["BTC-USD"],
            out,
            max_gb=1e-9,
            connector_factories={"coinbase": _factory(connector)},
        ),
        timeout=20,
    )

    assert stats.stopped_by == "max_gb"
    assert connector.closed
    assert list(iter_raw(out))


async def test_max_gb_uses_writer_counters_without_walking_the_tree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def forbidden_rglob(self: Path, pattern: str) -> object:
        raise AssertionError("the live loop must not walk the data tree")

    monkeypatch.setattr(Path, "rglob", forbidden_rglob)
    connector = FakeConnector("coinbase", infinite=True, delay=0.001)
    out = tmp_path / "data"

    stats = await asyncio.wait_for(
        run_live(
            ["coinbase"],
            ["BTC-USD"],
            out,
            max_gb=1e-9,
            connector_factories={"coinbase": _factory(connector)},
        ),
        timeout=20,
    )

    assert stats.stopped_by == "max_gb"
    assert connector.closed


async def test_duration_stops_a_queue_that_never_empties(tmp_path: Path) -> None:
    """A feed faster than the consumer must not delay ``--duration`` (#105)."""
    connector = FastConnector()
    out = tmp_path / "data"

    started = time.monotonic()
    stats = await asyncio.wait_for(
        run_live(
            ["coinbase"],
            ["BTC-USD"],
            out,
            duration=0.2,
            connector_factories={"coinbase": _factory(connector)},
            progress_interval=0,
        ),
        timeout=2.0,
    )
    elapsed = time.monotonic() - started

    assert stats.stopped_by == "duration"
    assert connector.closed
    assert 0.2 <= elapsed < 2.0
    assert stats.raw_lines > 0
    assert list(iter_raw(out))


async def test_stop_event_wins_over_a_queue_that_never_empties(tmp_path: Path) -> None:
    """A set stop event must be observed before another item is taken (#105)."""
    connector = FastConnector()
    out = tmp_path / "data"
    stop_event = asyncio.Event()
    task = asyncio.create_task(
        run_live(
            ["coinbase"],
            ["BTC-USD"],
            out,
            connector_factories={"coinbase": _factory(connector)},
            stop_event=stop_event,
            progress_interval=0,
        )
    )
    await asyncio.sleep(0.05)
    assert not task.done()

    stop_event.set()
    started = time.monotonic()
    stats = await asyncio.wait_for(task, timeout=2.0)
    elapsed = time.monotonic() - started

    assert stats.stopped_by == "stop_event"
    assert connector.closed
    assert elapsed < 2.0
    assert list(iter_raw(out))


async def test_max_gb_stops_a_queue_that_never_empties(tmp_path: Path) -> None:
    """The byte guard must not be delayed by a queue that never empties (#105)."""
    connector = FastConnector()
    out = tmp_path / "data"

    stats = await asyncio.wait_for(
        run_live(
            ["coinbase"],
            ["BTC-USD"],
            out,
            max_gb=1e-9,
            connector_factories={"coinbase": _factory(connector)},
            progress_interval=0,
        ),
        timeout=5.0,
    )

    assert stats.stopped_by == "max_gb"
    assert connector.closed
    assert list(iter_raw(out))


async def test_graceful_stop_persists_every_frame_reaching_the_queue(
    tmp_path: Path,
) -> None:
    """Nothing dequeued before the stop is lost: raw lines == frames yielded (#105)."""
    connector = FastConnector()
    out = tmp_path / "data"

    stats = await asyncio.wait_for(
        run_live(
            ["coinbase"],
            ["BTC-USD"],
            out,
            duration=0.2,
            connector_factories={"coinbase": _factory(connector)},
            progress_interval=0,
        ),
        timeout=5.0,
    )

    assert stats.stopped_by == "duration"
    assert connector.yielded_at_close is not None
    raw_messages = list(iter_raw(out))
    assert len(raw_messages) == stats.raw_lines
    assert stats.raw_lines == connector.yielded_at_close


async def test_merge_queue_is_bounded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The merge queue is created with ``_LIVE_QUEUE_MAXSIZE`` and never exceeds it."""
    import tickvault.pipeline as pipeline_module

    created: list[asyncio.Queue[tuple[Connector, RawMessage | None]]] = []

    class RecordingQueue(asyncio.Queue[tuple[Connector, RawMessage | None]]):
        """An ``asyncio.Queue`` that records its high-water mark."""

        def __init__(self, maxsize: int = 0) -> None:
            super().__init__(maxsize)
            self.high_water = 0
            created.append(self)

        def _observe(self) -> None:
            self.high_water = max(self.high_water, self.qsize())

        def put_nowait(self, item: tuple[Connector, RawMessage | None]) -> None:
            super().put_nowait(item)
            self._observe()

        async def put(self, item: tuple[Connector, RawMessage | None]) -> None:
            await super().put(item)
            self._observe()

        def get_nowait(self) -> tuple[Connector, RawMessage | None]:
            item = super().get_nowait()
            self._observe()
            return item

        async def get(self) -> tuple[Connector, RawMessage | None]:
            item = await super().get()
            self._observe()
            return item

    monkeypatch.setattr(pipeline_module, "_LIVE_QUEUE_MAXSIZE", 8)
    monkeypatch.setattr(pipeline_module.asyncio, "Queue", RecordingQueue)
    connector = FastConnector()
    out = tmp_path / "data"

    stats = await asyncio.wait_for(
        run_live(
            ["coinbase"],
            ["BTC-USD"],
            out,
            duration=0.2,
            connector_factories={"coinbase": _factory(connector)},
            progress_interval=0,
        ),
        timeout=5.0,
    )

    assert stats.stopped_by == "duration"
    assert len(created) == 1
    merge_queue = created[0]
    assert merge_queue.maxsize == 8
    assert merge_queue.high_water <= 8
    assert connector.yielded > 8


async def test_live_logs_start_progress_and_summary(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    connector = FakeConnector("coinbase", infinite=True, delay=0.001)
    out = tmp_path / "data"
    caplog.set_level(logging.INFO, logger="tickvault.pipeline")

    stats = await asyncio.wait_for(
        run_live(
            ["coinbase"],
            ["BTC-USD"],
            out,
            duration=0.5,
            connector_factories={"coinbase": _factory(connector)},
            progress_interval=0.1,
        ),
        timeout=20,
    )

    assert stats.stopped_by == "duration"
    messages = [
        record.message for record in caplog.records if record.name == "tickvault.pipeline"
    ]
    start = [message for message in messages if message.startswith("ingest starting")]
    progress = [message for message in messages if message.startswith("ingest progress")]
    final = [message for message in messages if message.startswith("ingest finished")]

    assert len(start) == 1
    assert "venues=coinbase" in start[0]
    assert "symbols=BTC-USD" in start[0]
    assert f"out={out}" in start[0]
    assert len(final) == 1
    assert "stopped=duration" in final[0]

    # One line per interval, not per message: the endless connector produced
    # far more messages than the schedule allows lines.
    assert 2 <= len(progress) <= 8
    assert stats.raw_lines > len(progress)
    fields = re.compile(
        r"^ingest progress: elapsed=\d+s raw_msgs=\d+ \(coinbase=\d+\) "
        r"trades=\d+ book_deltas=\d+ anomalies=0 resyncs=0 written=\d+\.\dMB$"
    )
    assert all(fields.match(message) for message in progress)


async def test_progress_interval_zero_disables_progress_lines(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    connector = FakeConnector("coinbase", infinite=True, delay=0.001)
    out = tmp_path / "data"
    caplog.set_level(logging.INFO, logger="tickvault.pipeline")

    stats = await asyncio.wait_for(
        run_live(
            ["coinbase"],
            ["BTC-USD"],
            out,
            duration=1.0,
            connector_factories={"coinbase": _factory(connector)},
            progress_interval=0,
        ),
        timeout=20,
    )

    assert stats.stopped_by == "duration"
    assert not [
        record for record in caplog.records if "ingest progress" in record.message
    ]
    assert any("ingest starting" in record.message for record in caplog.records)


async def test_unknown_venue_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="unsupported venue"):
        await run_live(["binance"], ["BTC-USD"], tmp_path / "data")


async def test_negative_progress_interval_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="progress_interval"):
        await run_live(
            ["coinbase"], ["BTC-USD"], tmp_path / "data", progress_interval=-1
        )


async def test_cancelling_run_live_mid_stream_leaves_nothing_pending(
    tmp_path: Path,
) -> None:
    loop = asyncio.get_running_loop()
    recorded: list[dict[str, object]] = []
    previous_handler = loop.get_exception_handler()
    previous_debug = loop.get_debug()
    loop.set_exception_handler(lambda _loop, context: recorded.append(context))
    loop.set_debug(True)
    try:
        connector = FakeConnector("coinbase", infinite=True, delay=0.001)
        out = tmp_path / "data"
        task = asyncio.create_task(
            run_live(
                ["coinbase"],
                ["BTC-USD"],
                out,
                connector_factories={"coinbase": _factory(connector)},
            )
        )
        while not (out / "raw").exists():
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.05)

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.sleep(0.05)

        pending = [
            pending_task
            for pending_task in asyncio.all_tasks()
            if pending_task is not asyncio.current_task() and not pending_task.done()
        ]
        assert pending == []
        assert connector.closed
        gc.collect()
        assert recorded == []
    finally:
        loop.set_exception_handler(previous_handler)
        loop.set_debug(previous_debug)


_SUBPROCESS_SCRIPT = '''\
import asyncio
import json
import sys
from pathlib import Path

from tickvault.models import RawMessage
from tickvault.pipeline import run_live

BASE_NS = 1_793_491_200_000_000_000
EXCHANGE_TS = "2026-11-01T00:00:00.000000000Z"


def message(sequence, recv_ts_ns):
    if sequence == 1:
        event = {
            "type": "snapshot",
            "product_id": "BTC-USD",
            "updates": [
                {"side": "bid", "event_time": EXCHANGE_TS, "price_level": "81199.9",
                 "new_quantity": "0.5"},
                {"side": "offer", "event_time": EXCHANGE_TS, "price_level": "81200.1",
                 "new_quantity": "0.6"},
            ],
        }
    else:
        event = {
            "type": "update",
            "product_id": "BTC-USD",
            "updates": [
                {"side": "bid", "event_time": EXCHANGE_TS, "price_level": "81199.9",
                 "new_quantity": "0.55"},
            ],
        }
    payload = json.dumps({
        "channel": "l2_data",
        "timestamp": EXCHANGE_TS,
        "sequence_num": sequence,
        "events": [event],
    })
    return RawMessage(
        venue="coinbase",
        connection_id="coinbase-sigterm",
        ingest_seq=sequence - 1,
        recv_ts_ns=recv_ts_ns,
        kind="msg",
        payload=payload,
    )


class EndlessConnector:
    venue = "coinbase"

    def __init__(self, symbols):
        self.symbols = tuple(symbols)
        self.closed = False

    async def subscribe(self, symbols):
        self.symbols = tuple(symbols)

    async def resync(self, symbol, reason):
        return None

    async def close(self):
        self.closed = True

    async def stream(self):
        sequence = 1
        recv_ts_ns = BASE_NS
        while not self.closed:
            yield message(sequence, recv_ts_ns)
            sequence += 1
            recv_ts_ns += 1_000_000
            await asyncio.sleep(0.001)


async def main():
    out = Path(sys.argv[1])
    stats = await run_live(
        ["coinbase"], ["BTC-USD"], out, connector_factories={"coinbase": EndlessConnector}
    )
    print(json.dumps(stats.to_dict()))


asyncio.run(main())
'''


def test_sigterm_stops_live_gracefully_in_a_subprocess(tmp_path: Path) -> None:
    out = tmp_path / "data"
    script = tmp_path / "endless_live.py"
    script.write_text(_SUBPROCESS_SCRIPT, encoding="utf-8")
    process = subprocess.Popen(
        [sys.executable, str(script), str(out)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and not (out / "raw").exists():
            if process.poll() is not None:
                _, stderr = process.communicate()
                pytest.fail(f"live process exited before SIGTERM: {stderr}")
            time.sleep(0.05)
        assert (out / "raw").exists(), "live process never wrote a raw file"
        time.sleep(0.3)
        process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=30)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()

    assert process.returncode == 0, stderr
    stats = json.loads(stdout.strip().splitlines()[-1])
    assert stats["stopped_by"] == "signal"
    assert list(iter_raw(out))
    parquet_files = list((out / "norm").rglob("*.parquet"))
    assert parquet_files
    assert sum(pq.read_table(path).num_rows for path in parquet_files) > 0
