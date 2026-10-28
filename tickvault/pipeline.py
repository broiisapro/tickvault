"""Replay and live pipelines sharing one normalization/book/writer path.

:func:`run_replay` reads a raw file or raw tree in canonical order and feeds
every message through the venue normalizer, the :class:`BookProcessor` and a
:class:`ParquetWriter` rooted at ``out``. It never writes outside ``out/norm/``
and never modifies the input; resync requests are ignored because replay has
nothing to resynchronize.

:func:`run_live` runs the venue connectors, persists every raw message to
:class:`RawWriter` **before** it is normalized, and then processes it exactly
like replay. It merges the connector streams through a bounded
:class:`asyncio.Queue` (``_LIVE_QUEUE_MAXSIZE``, so a slow consumer
backpressures the connectors instead of growing memory), routes every
:class:`ResyncRequest` back to its venue's connector, flushes idle Parquet
buffers once per second, and stops gracefully on ``duration``, on
``stop_event``, on SIGTERM/SIGINT, or when the data root grows past ``max_gb``
(measured from the writers' byte counters plus the data root's size once at
start-up, never by walking the tree). A set stop event always wins over a
queued item, so a queue that never empties cannot delay a stop. It logs a
start line, one progress line every ``progress_interval`` seconds and a final
summary through the ``tickvault.pipeline`` logger; the library never
configures logging itself. A graceful stop closes the connectors, drains
every message that already reached the queue through the raw writer and the
processor, and closes the raw and Parquet writers, so every file on disk is
complete and readable.

Both entry points return the same frozen :class:`PipelineStats`.
"""

from __future__ import annotations

import asyncio
import logging
import signal
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from tickvault.book.processor import BookProcessor, ResyncRequest
from tickvault.buffer import Buffer
from tickvault.connectors.base import Connector
from tickvault.metrics import PipelineMetrics
from tickvault.models import NormalizedMessage, RawMessage
from tickvault.normalize import NormalizeError
from tickvault.rawio import iter_raw
from tickvault.storage.parquet_writer import ParquetWriter
from tickvault.storage.raw_writer import RawWriter
from tickvault.venues import VENUES, ConnectorFactory

_LOGGER = logging.getLogger(__name__)

_LIVE_IDLE_FLUSH_S = 60.0
"""Live Parquet buffers untouched for this many seconds are flushed."""

_LIVE_TICK_S = 1.0
"""Idle-flush and ``--max-gb`` check interval of the live loop."""

_LIVE_PROGRESS_S = 10.0
"""Default seconds between ``ingest progress`` log lines; 0 disables them."""

_LIVE_QUEUE_MAXSIZE = 10_000
"""Bound of the live merge queue; a slow consumer backpressures the connectors."""

_NORMALIZERS: dict[str, Callable[[RawMessage], NormalizedMessage]] = {
    name: spec.normalize for name, spec in VENUES.items()
}

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
    return {name: spec.connector for name, spec in VENUES.items()}


class _Recorder:
    """Normalize, process and count one raw message; used by both entry points."""

    def __init__(self, processor: BookProcessor, writer: ParquetWriter) -> None:
        self.processor = processor
        self.writer = writer
        self.raw_lines = 0
        self.raw_by_venue: dict[str, int] = {}
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

    @property
    def anomalies(self) -> int:
        """Gap, checksum and invariant failures seen so far."""
        return self.gaps + self.checksum_failures + self.invariant_violations

    def process(self, message: RawMessage) -> list[ResyncRequest]:
        """Record one raw message and return the resync requests it produced.

        Ordinary malformed venue frames raise :class:`NormalizeError` in the
        normalizer; they are counted in ``normalize_errors`` and skipped. A
        venue without a normalizer is a programming error and raises
        :class:`ValueError`.
        """
        self.raw_lines += 1
        self.raw_by_venue[message.venue] = self.raw_by_venue.get(message.venue, 0) + 1
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
    progress_interval: float = _LIVE_PROGRESS_S,
    buffer: Buffer | None = None,
    metrics: PipelineMetrics | None = None,
) -> PipelineStats:
    """Stream live venue data into the raw and normalized layers under ``out``.

    One connector per venue (``connector_factories``, the production connectors
    by default) is built for ``symbols`` and merged through one queue bounded
    at ``_LIVE_QUEUE_MAXSIZE`` entries. Every message is written to the raw
    layer before it is normalized. The run stops gracefully on ``duration``
    seconds, on ``stop_event``, on SIGTERM/SIGINT, or when the data root
    exceeds ``max_gb``; a set stop event is observed before another item is
    taken, so a queue that never empties cannot delay the stop. The returned
    stats' ``stopped_by`` records which happened.

    On a stop the connectors are closed first (nothing new starts flowing),
    then every message that already reached the queue is drained through the
    raw writer and the processor, so no frame in the queue is lost.

    Logging goes through the ``tickvault.pipeline`` logger: a start line, one
    ``ingest progress`` line every ``progress_interval`` seconds (0 disables
    the periodic lines), and a final summary. The library never configures
    logging itself; the CLI does. ``max_gb`` is enforced from the writers'
    ``bytes_written`` counters plus the data root's size measured once at
    start-up, so no directory walk happens while the run is live.

    ``buffer`` and ``metrics`` are the Phase 2 hooks. Both default to ``None``,
    which leaves the Phase 1 path above untouched. ``metrics`` is accepted and
    ignored until wave 08 wires it; a non-``None`` ``buffer`` raises
    :class:`NotImplementedError` because buffered ingest lands in wave 08.
    """
    if buffer is not None:
        raise NotImplementedError("buffered ingest: implemented in wave 08")
    if progress_interval < 0:
        raise ValueError("progress_interval must be non-negative")
    out_path = Path(out)
    initial_bytes = _tree_bytes(out_path)
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
    queue: asyncio.Queue[tuple[Connector, RawMessage | None]] = asyncio.Queue(
        maxsize=_LIVE_QUEUE_MAXSIZE
    )
    producers = [asyncio.create_task(_pump(connector, queue)) for connector in connectors]
    started = time.monotonic()
    _LOGGER.info(
        "ingest starting: venues=%s symbols=%s out=%s",
        ",".join(venues),
        ",".join(symbols),
        out_path,
    )
    ticker = asyncio.create_task(
        _tick(parquet, raw_writer, stopper, out_path, initial_bytes, max_gb)
    )
    timer = asyncio.create_task(_stop_after(duration, stopper)) if duration is not None else None
    progress = (
        asyncio.create_task(
            _report_progress(
                recorder, raw_writer, parquet, venues, progress_interval, started
            )
        )
        if progress_interval > 0
        else None
    )
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
        background = [task for task in (ticker, timer, progress) if task is not None]
        for task in background:
            task.cancel()
        await asyncio.gather(*background, return_exceptions=True)
        await _close_connectors(connectors)
        for producer in producers:
            producer.cancel()
        discarded = asyncio.ensure_future(_discard(queue))
        try:
            await asyncio.gather(*producers, return_exceptions=True)
        finally:
            discarded.cancel()
            await asyncio.gather(discarded, return_exceptions=True)
        raw_writer.close()
        parquet.close()
    stats = recorder.stats("live", stopper.reason if stopper.reason is not None else "completed")
    _LOGGER.info(
        "ingest finished: stopped=%s elapsed=%.0fs %s",
        stats.stopped_by,
        time.monotonic() - started,
        _progress_fields(recorder, raw_writer, parquet, venues),
    )
    return stats


async def run_writer(
    buffer: Buffer,
    out: str | Path,
    *,
    duration: float | None = None,
    metrics: PipelineMetrics | None = None,
    stop_event: asyncio.Event | None = None,
) -> PipelineStats:
    """Consume a raw buffer and write it like replay does.

    The buffered-writer path: read :meth:`Buffer.consume_raw`, write every raw
    line through :class:`RawWriter` first, then run it through the unchanged
    normalizer → processor → Parquet pipeline, commit offsets only once the raw
    writer has flushed, publish resync requests to the control topic and serve
    ``metrics`` when given. ``duration`` and ``stop_event`` stop it gracefully.

    Wave 08 implements this; the Phase 2 foundation only freezes the
    signature.
    """
    raise NotImplementedError("writer: implemented in wave 08")


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
    """Await the next queued item, or ``None`` once a stop has been requested.

    A set stop event is observed before another item is taken: the event is
    checked before every dequeue and wins even while the queue is never empty,
    so ``duration``, signals and ``max_gb`` cannot be delayed by a busy feed.
    Items are taken with ``get_nowait`` (no task pair per message); only an
    empty queue falls back to waiting for an item and the stop event together.
    One event-loop turn is yielded per item so the timer, signal and writer
    tasks keep running while messages arrive faster than they are processed.
    """
    while True:
        await asyncio.sleep(0)
        if stopper.event.is_set():
            if stopper.reason is None:
                stopper.reason = "stop_event"
            return None
        try:
            return queue.get_nowait()
        except asyncio.QueueEmpty:
            pass
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


async def _discard(queue: asyncio.Queue[tuple[Connector, RawMessage | None]]) -> None:
    """Empty ``queue`` forever.

    Used during shutdown so a producer cancelled while blocked on a full
    queue can finish its final ``put`` instead of deadlocking the join.
    """
    while True:
        await queue.get()


async def _stop_after(seconds: float, stopper: _Stopper) -> None:
    await asyncio.sleep(seconds)
    stopper.request("duration")


async def _report_progress(
    recorder: _Recorder,
    raw_writer: RawWriter,
    parquet: ParquetWriter,
    venues: Sequence[str],
    interval: float,
    started: float,
) -> None:
    """Log one ``ingest progress`` line every ``interval`` seconds.

    The schedule is anchored to ``started`` so a slow loop never produces two
    lines for one interval; ticks missed while the loop was blocked are
    skipped. Counters are read when the line is written.
    """
    loop = asyncio.get_running_loop()
    schedule = started + interval
    while True:
        now = loop.time()
        if schedule > now:
            await asyncio.sleep(schedule - now)
        _LOGGER.info(
            "ingest progress: elapsed=%.0fs %s",
            time.monotonic() - started,
            _progress_fields(recorder, raw_writer, parquet, venues),
        )
        now = loop.time()
        schedule = max(schedule + interval, now + interval)


def _progress_fields(
    recorder: _Recorder,
    raw_writer: RawWriter,
    parquet: ParquetWriter,
    venues: Sequence[str],
) -> str:
    """Render the shared counter section of the progress and summary lines."""
    by_venue = " ".join(f"{venue}={recorder.raw_by_venue.get(venue, 0)}" for venue in venues)
    written_mb = (raw_writer.bytes_written + parquet.bytes_written) / 1_000_000
    return (
        f"raw_msgs={recorder.raw_lines} ({by_venue}) "
        f"trades={recorder.trades} book_deltas={recorder.book_deltas} "
        f"anomalies={recorder.anomalies} resyncs={recorder.resyncs} "
        f"written={written_mb:.1f}MB"
    )


async def _tick(
    parquet: ParquetWriter,
    raw_writer: RawWriter,
    stopper: _Stopper,
    out: Path,
    initial_bytes: int,
    max_gb: float | None,
) -> None:
    """Flush idle Parquet buffers and enforce ``max_gb`` once per second.

    The size check adds the writers' monotonic byte counters to the data
    root's size measured once at start-up, so it never walks the tree while
    the run is live. Stop behaviour of ``run_live`` is unchanged.
    """
    while True:
        await asyncio.sleep(_LIVE_TICK_S)
        parquet.flush_idle()
        if max_gb is None:
            continue
        size_bytes = initial_bytes + raw_writer.bytes_written + parquet.bytes_written
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
