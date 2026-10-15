"""Live pipeline tests: raw-first persistence, resync routing, stop paths (issue #51)."""

from __future__ import annotations

import asyncio
import json
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


async def test_unknown_venue_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="unsupported venue"):
        await run_live(["binance"], ["BTC-USD"], tmp_path / "data")


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
