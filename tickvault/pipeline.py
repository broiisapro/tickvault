"""Replay and live pipelines sharing one normalization/book/writer path.

:func:`run_replay` reads a raw file or raw tree in canonical order and feeds
every message through the venue normalizer, the :class:`BookProcessor` and a
:class:`ParquetWriter` rooted at ``out``. It never writes outside ``out/norm/``
and never modifies the input; resync requests are ignored because replay has
nothing to resynchronize.

:func:`run_live` runs the venue connectors, persists every raw message to
:class:`RawWriter` **before** it is normalized, and then processes it exactly
like replay. It merges the connector streams through an
:class:`asyncio.Queue`, routes every :class:`ResyncRequest` back to its venue's
connector, flushes idle Parquet buffers once per second, and stops gracefully
on ``duration``, on ``stop_event``, on SIGTERM/SIGINT, or when the data root
grows past ``max_gb``. A graceful stop closes the connectors, waits for their
streams to end, processes the remaining queued messages, and closes the raw
and Parquet writers, so every file on disk is complete and readable.

Both entry points return the same frozen :class:`PipelineStats`.
"""

from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from tickvault.book.processor import BookProcessor, ResyncRequest
from tickvault.connectors.base import Connector
from tickvault.connectors.coinbase import CoinbaseConnector
from tickvault.connectors.kraken import KrakenConnector
from tickvault.models import NormalizedMessage, RawMessage
from tickvault.normalize import NormalizeError
from tickvault.normalize import coinbase as coinbase_normalize
from tickvault.normalize import kraken as kraken_normalize
from tickvault.rawio import iter_raw
from tickvault.storage.parquet_writer import ParquetWriter
from tickvault.storage.raw_writer import RawWriter

_LOGGER = logging.getLogger(__name__)

_LIVE_IDLE_FLUSH_S = 60.0
"""Live Parquet buffers untouched for this many seconds are flushed."""

_LIVE_TICK_S = 1.0
"""Idle-flush and ``--max-gb`` check interval of the live loop."""

_NORMALIZERS: dict[str, Callable[[RawMessage], NormalizedMessage]] = {
    "coinbase": coinbase_normalize.normalize,
    "kraken": kraken_normalize.normalize,
}

type ConnectorFactory = Callable[[Sequence[str]], Connector]
"""Builds one connector for a venue and a fixed symbol set."""

type StopReason = str
"""One of ``completed``, ``duration``, ``stop_event``, ``signal``, ``max_gb``."""


@dataclass(frozen=True, slots=True)
class PipelineStats:
    """Counters collected while replaying or ingesting raw messages.

    Every count is exact. ``to_dict`` renders the same counters as a stable,
    JSON-serializable dictionary with table rows nested under ``rows`` and
    per-type counts under ``events``; the key order never changes.
    """

    mode: str
    raw_lines: int = 0
    messages: int = 0
    normalize_errors: int = 0
    trades: int = 0
    book_deltas: int = 0
    book_snapshots: int = 0
    gaps: int = 0
    resyncs: int = 0
    checksum_failures: int = 0
    invariant_violations: int = 0
    reconnects: int = 0
    disconnects: int = 0
    resync_requests: int = 0
    stopped_by: StopReason = "completed"

    @property
    def events(self) -> int:
        """Total rows written to the events table."""
        return (
            self.gaps
            + self.resyncs
            + self.checksum_failures
            + self.invariant_violations
            + self.reconnects
            + self.disconnects
        )

    def to_dict(self) -> dict[str, object]:
        """Render the counters as a stable JSON-serializable dictionary."""
        return {
            "mode": self.mode,
            "stopped_by": self.stopped_by,
            "raw_lines": self.raw_lines,
            "messages": self.messages,
            "normalize_errors": self.normalize_errors,
            "rows": {
                "trades": self.trades,
                "book_deltas": self.book_deltas,
                "book_snapshots": self.book_snapshots,
                "events": self.events,
            },
            "events": {
                "gap": self.gaps,
                "resync": self.resyncs,
                "checksum_failure": self.checksum_failures,
                "invariant_violation": self.invariant_violations,
                "reconnect": self.reconnects,
                "disconnect": self.disconnects,
            },
            "resync_requests": self.resync_requests,
        }


def default_connector_factories() -> dict[str, ConnectorFactory]:
    """Return the production connector factory for each supported venue."""
    return {"coinbase": CoinbaseConnector, "kraken": KrakenConnector}


class _Recorder:
    """Normalize, process and count one raw message; used by both entry points."""

    def __init__(self, processor: BookProcessor, writer: ParquetWriter) -> None:
        self.processor = processor
        self.writer = writer
        self.raw_lines = 0
        self.messages = 0
        self.normalize_errors = 0
        self.trades = 0
        self.book_deltas = 0
        self.book_snapshots = 0
        self.gaps = 0
        self.resyncs = 0
        self.checksum_failures = 0
        self.invariant_violations = 0
        self.reconnects = 0
        self.disconnects = 0
        self.resync_requests = 0

    def process(self, message: RawMessage) -> list[ResyncRequest]:
        """Record one raw message and return the resync requests it produced.

        Ordinary malformed venue frames raise :class:`NormalizeError` in the
        normalizer; they are counted in ``normalize_errors`` and skipped. A
        venue without a normalizer is a programming error and raises
        :class:`ValueError`.
        """
        self.raw_lines += 1
        normalize = _NORMALIZERS.get(message.venue)
        if normalize is None:
            raise ValueError(f"no normalizer for venue {message.venue!r}")
        try:
            normalized = normalize(message)
        except NormalizeError:
            self.normalize_errors += 1
            return []
        self.messages += 1
        output = self.processor.process(normalized)
        self.trades += len(output.trades)
        self.book_deltas += len(output.book_deltas)
        self.book_snapshots += len(output.book_snapshots)
        for event in output.events:
            self._count_event(event.event_type)
        self.resync_requests += len(output.resync_requests)
        self.writer.write("trades", output.trades)
        self.writer.write("book_deltas", output.book_deltas)
        self.writer.write("book_snapshots", output.book_snapshots)
        self.writer.write("events", output.events)
        return output.resync_requests

    def _count_event(self, event_type: str) -> None:
        if event_type == "gap":
            self.gaps += 1
        elif event_type == "resync":
            self.resyncs += 1
        elif event_type == "checksum_failure":
            self.checksum_failures += 1
        elif event_type == "invariant_violation":
            self.invariant_violations += 1
        elif event_type == "reconnect":
            self.reconnects += 1
        elif event_type == "disconnect":
            self.disconnects += 1
        else:
            raise ValueError(f"unknown event type: {event_type!r}")

    def stats(self, mode: str, stopped_by: StopReason = "completed") -> PipelineStats:
        """Freeze the current counters into a :class:`PipelineStats`."""
        return PipelineStats(
            mode=mode,
            raw_lines=self.raw_lines,
            messages=self.messages,
            normalize_errors=self.normalize_errors,
            trades=self.trades,
            book_deltas=self.book_deltas,
            book_snapshots=self.book_snapshots,
            gaps=self.gaps,
            resyncs=self.resyncs,
            checksum_failures=self.checksum_failures,
            invariant_violations=self.invariant_violations,
            reconnects=self.reconnects,
            disconnects=self.disconnects,
            resync_requests=self.resync_requests,
            stopped_by=stopped_by,
        )


def run_replay(input: str | Path, out: str | Path) -> PipelineStats:
    """Replay ``input`` (a raw file or raw tree) into ``out/norm/``.

    Messages are read in canonical order and processed exactly like live
    ingestion. Resync requests are counted and discarded; malformed frames are
    counted and skipped. Only ``out/norm/`` is written: the raw layer under
    ``out`` is never touched and the input is never copied.
    """
    writer = ParquetWriter(out)
    recorder = _Recorder(BookProcessor(), writer)
    try:
        for message in iter_raw(input):
            recorder.process(message)
    finally:
        writer.close()
    return recorder.stats("replay")


class _Stopper:
    """One stop signal shared by duration, wall clock, signals and stop_event."""

    def __init__(self, event: asyncio.Event) -> None:
        self.event = event
        self.reason: StopReason | None = None

    def request(self, reason: StopReason) -> None:
        """Record the first stop reason and wake the live loop."""
        if self.reason is None:
            self.reason = reason
        self.event.set()


async def run_live(
    venues: Sequence[str],
    symbols: Sequence[str],
    out: str | Path,
    duration: float | None = None,
    max_gb: float | None = None,
    connector_factories: Mapping[str, ConnectorFactory] | None = None,
    stop_event: asyncio.Event | None = None,
) -> PipelineStats:
    """Stream live venue data into the raw and normalized layers under ``out``.

    One connector per venue (``connector_factories``, the production connectors
    by default) is built for ``symbols`` and merged through one queue. Every
    message is written to the raw layer before it is normalized. The run stops
    gracefully on ``duration`` seconds, on ``stop_event``, on SIGTERM/SIGINT,
    or when the data root exceeds ``max_gb``; the returned stats'
    ``stopped_by`` records which happened.
    """
    out_path = Path(out)
    factories = (
        default_connector_factories() if connector_factories is None else connector_factories
    )
    connectors: list[Connector] = []
    for venue in venues:
        factory = factories.get(venue)
        if factory is None:
            raise ValueError(f"unsupported venue: {venue!r}")
        connectors.append(factory(tuple(symbols)))
    if not connectors:
        raise ValueError("at least one venue is required")

    stopper = _Stopper(stop_event if stop_event is not None else asyncio.Event())
    raw_writer = RawWriter(out_path)
    parquet = ParquetWriter(out_path, idle_flush_s=_LIVE_IDLE_FLUSH_S)
    recorder = _Recorder(BookProcessor(), parquet)
    queue: asyncio.Queue[tuple[Connector, RawMessage | None]] = asyncio.Queue()
    producers = [asyncio.create_task(_pump(connector, queue)) for connector in connectors]
    ticker = asyncio.create_task(_tick(parquet, stopper, out_path, max_gb))
    timer = asyncio.create_task(_stop_after(duration, stopper)) if duration is not None else None
    loop = asyncio.get_running_loop()
    handled_signals = _install_signal_handlers(loop, stopper)
    finished = 0
    shutting_down = False
    try:
        while finished < len(connectors):
            if shutting_down:
                _, message = await queue.get()
            else:
                item = await _next_or_stop(queue, stopper)
                if item is None:
                    shutting_down = True
                    await _close_connectors(connectors)
                    continue
                _, message = item
            if message is None:
                finished += 1
                continue
            raw_writer.write(message)
            for request in recorder.process(message):
                await _route_resync(connectors, request)
    finally:
        for signum in handled_signals:
            loop.remove_signal_handler(signum)
        for task in (ticker, timer):
            if task is not None:
                task.cancel()
        pending = [task for task in (ticker, timer) if task is not None]
        await asyncio.gather(*pending, return_exceptions=True)
        await _close_connectors(connectors)
        await asyncio.gather(*producers, return_exceptions=True)
        while not queue.empty():
            queue.get_nowait()
        raw_writer.close()
        parquet.close()
    return recorder.stats("live", stopper.reason if stopper.reason is not None else "completed")


async def _pump(
    connector: Connector, queue: asyncio.Queue[tuple[Connector, RawMessage | None]]
) -> None:
    """Forward one connector's stream into ``queue``, ending with ``None``."""
    try:
        async for message in connector.stream():
            await queue.put((connector, message))
    except Exception:
        _LOGGER.exception("%s connector stream failed", connector.venue)
    finally:
        await queue.put((connector, None))


async def _next_or_stop(
    queue: asyncio.Queue[tuple[Connector, RawMessage | None]], stopper: _Stopper
) -> tuple[Connector, RawMessage | None] | None:
    """Await the next queued item, or ``None`` once a stop has been requested."""
    get_task: asyncio.Task[tuple[Connector, RawMessage | None]] = asyncio.ensure_future(
        queue.get()
    )
    stop_task: asyncio.Task[bool] = asyncio.ensure_future(stopper.event.wait())
    try:
        done, _ = await asyncio.wait(
            {get_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
        )
    except BaseException:
        get_task.cancel()
        stop_task.cancel()
        raise
    if get_task in done:
        stop_task.cancel()
        await asyncio.gather(stop_task, return_exceptions=True)
        return get_task.result()
    get_task.cancel()
    await asyncio.gather(get_task, return_exceptions=True)
    if stopper.reason is None:
        stopper.reason = "stop_event"
    return None


async def _stop_after(seconds: float, stopper: _Stopper) -> None:
    await asyncio.sleep(seconds)
    stopper.request("duration")


async def _tick(
    parquet: ParquetWriter, stopper: _Stopper, out: Path, max_gb: float | None
) -> None:
    """Flush idle Parquet buffers and enforce ``max_gb`` once per second."""
    while True:
        await asyncio.sleep(_LIVE_TICK_S)
        parquet.flush_idle()
        if max_gb is None:
            continue
        size_bytes = _tree_bytes(out)
        limit_bytes = max_gb * 1_000_000_000
        if size_bytes > limit_bytes:
            _LOGGER.warning(
                "stopping ingestion: %s holds %.3f GB, past the %.3f GB limit",
                out,
                size_bytes / 1_000_000_000,
                max_gb,
            )
            stopper.request("max_gb")
            return


def _tree_bytes(root: Path) -> int:
    """Total size of every file under ``root`` in bytes (0 when it is absent)."""
    if not root.exists():
        return 0
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())


async def _close_connectors(connectors: Sequence[Connector]) -> None:
    for connector in connectors:
        try:
            await connector.close()
        except Exception:
            _LOGGER.exception("error closing the %s connector", connector.venue)


async def _route_resync(connectors: Sequence[Connector], request: ResyncRequest) -> None:
    for connector in connectors:
        if connector.venue == request.venue:
            await connector.resync(request.symbol, request.reason)
            return
    raise ValueError(f"no connector for venue {request.venue!r}")


def _install_signal_handlers(loop: asyncio.AbstractEventLoop, stopper: _Stopper) -> list[int]:
    """Request a graceful stop on SIGTERM/SIGINT; return the installed signals."""
    installed: list[int] = []
    for signum in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(signum, stopper.request, "signal")
        except (NotImplementedError, RuntimeError):  # pragma: no cover - non-POSIX
            continue
        installed.append(signum)
    return installed
