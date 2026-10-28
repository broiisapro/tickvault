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

Every entry point returns the same frozen :class:`PipelineStats`.

Phase 2 adds two independent hops without changing that path (decisions A70 and
A71). ``run_live(..., buffer=...)`` is the ingester of the buffered mode: it
runs the connectors, publishes every raw envelope line with
:meth:`Buffer.publish_raw` (exactly the bytes :func:`tickvault.rawio.encode_line`
produced) and writes no raw or Parquet file; a second task reads
:meth:`Buffer.consume_control` and executes the resync requests the writer
sends back. :func:`run_writer` is the writer: it reads the raw records back,
writes each of them through :class:`RawWriter` **before** normalizing it, runs
the unchanged normalizer → processor → Parquet path, publishes the resync
requests it produces to the control topic, and commits an offset only after
the raw writer has flushed everything up to it. A writer restarted after a
crash therefore replays the uncommitted tail instead of losing it; it drops the
envelopes whose ``(connection_id, ingest_seq)`` it already wrote in that
process, resumes in the same raw tree beside the earlier process's files, asks
the still-connected ingester for a fresh snapshot the first time a symbol has
no LIVE book, and reports its backlog through the buffer-lag gauge.

``metrics`` is observability only: every ``None`` default leaves the Phase 1
code path byte-identical.
"""

from __future__ import annotations

import asyncio
import logging
import signal
import time
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from tickvault.book.processor import BookProcessor, ProcessorOutput, ResyncRequest
from tickvault.buffer import Buffer, BufferRecord, decode_control, encode_control
from tickvault.connectors.base import Connector
from tickvault.metrics import PipelineMetrics
from tickvault.models import NormalizedMessage, RawMessage
from tickvault.normalize import NormalizeError
from tickvault.rawio import decode_line, encode_line, iter_raw
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

_SYNCING_EVENTS = frozenset({"gap", "checksum_failure", "invariant_violation", "reconnect"})
"""Events that move a book back to SYNCING."""

_WRITER_COMMIT_RECORDS = 1_000
"""Writer records between an explicit raw flush and an offset commit."""

_WRITER_SYNC_REASON = "writer_sync"
"""Reason of the resync request a writer sends for a symbol that never went LIVE."""

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


class _BookStates:
    """Each book's LIVE/SYNCING state, derived from the processor's output.

    :class:`BookProcessor` keeps its per-symbol state private (another lane owns
    that file), so the pipeline reads the same transitions from the rows and
    events it emits: a ``venue`` snapshot switches a book LIVE, and a gap,
    checksum failure, invariant violation or reconnect switches it back. The
    processor emits a venue snapshot *before* flagging an invariant violation
    on the same message, so events are applied last and win.
    """

    def __init__(self) -> None:
        self._live: dict[tuple[str, str], bool] = {}

    def is_live(self, venue: str, symbol: str) -> bool:
        """True while the book of ``(venue, symbol)`` is LIVE."""
        return self._live.get((venue, symbol), False)

    def observe(self, output: ProcessorOutput) -> dict[tuple[str, str], bool]:
        """Apply one output and return the states that changed."""
        changes: dict[tuple[str, str], bool] = {}
        for row in output.book_snapshots:
            if row.source == "venue":
                changes[(row.venue, row.symbol)] = True
        for event in output.events:
            if event.symbol is not None and event.event_type in _SYNCING_EVENTS:
                changes[(event.venue, event.symbol)] = False
        changed: dict[tuple[str, str], bool] = {}
        for key, live in changes.items():
            if self._live.get(key) is not live:
                self._live[key] = live
                changed[key] = live
        return changed


class _Recorder:
    """Normalize, process and count one raw message; used by every entry point.

    ``metrics`` and ``books`` are optional. With both ``None`` the recorder is
    exactly the Phase 1 one: it only counts. ``books`` tracks each book's LIVE
    state from the rows and events the processor emits (the processor keeps its
    own state private), which the writer needs to decide when a snapshot is
    missing and the gauges need to follow state changes.
    """

    def __init__(
        self,
        processor: BookProcessor,
        writer: ParquetWriter,
        *,
        metrics: PipelineMetrics | None = None,
        books: _BookStates | None = None,
    ) -> None:
        self.processor = processor
        self.writer = writer
        self.metrics = metrics
        self.books = books
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

    def process(
        self,
        message: RawMessage,
        *,
        on_normalized: Callable[[NormalizedMessage], None] | None = None,
    ) -> list[ResyncRequest]:
        """Record one raw message and return the resync requests it produced.

        Ordinary malformed venue frames raise :class:`NormalizeError` in the
        normalizer; they are counted in ``normalize_errors`` and skipped. A
        venue without a normalizer is a programming error and raises
        :class:`ValueError`. ``on_normalized`` sees each message that did
        normalize, before the processor runs, so a caller can inspect the
        updates the processor is about to apply (or drop).
        """
        self.raw_lines += 1
        self.raw_by_venue[message.venue] = self.raw_by_venue.get(message.venue, 0) + 1
        if self.metrics is not None:
            self.metrics.observe_raw(
                message.venue, len(encode_line(message)), time.time()
            )
        normalize = _NORMALIZERS.get(message.venue)
        if normalize is None:
            raise ValueError(f"no normalizer for venue {message.venue!r}")
        try:
            normalized = normalize(message)
        except NormalizeError:
            self.normalize_errors += 1
            if self.metrics is not None:
                self.metrics.observe_normalize_error(message.venue)
            return []
        if on_normalized is not None:
            on_normalized(normalized)
        self.messages += 1
        output = self.processor.process(normalized)
        self.trades += len(output.trades)
        self.book_deltas += len(output.book_deltas)
        self.book_snapshots += len(output.book_snapshots)
        for event in output.events:
            self._count_event(event.event_type)
        self.resync_requests += len(output.resync_requests)
        self._observe(message, output)
        self.writer.write("trades", output.trades)
        self.writer.write("book_deltas", output.book_deltas)
        self.writer.write("book_snapshots", output.book_snapshots)
        self.writer.write("events", output.events)
        return output.resync_requests

    def _observe(self, message: RawMessage, output: ProcessorOutput) -> None:
        """Feed one output batch to the metrics and the book-state tracker."""
        metrics = self.metrics
        books = self.books
        if metrics is None and books is None:
            return
        if metrics is not None:
            venue = message.venue
            metrics.observe_rows("trades", venue, len(output.trades))
            metrics.observe_rows("book_deltas", venue, len(output.book_deltas))
            metrics.observe_rows("book_snapshots", venue, len(output.book_snapshots))
            metrics.observe_rows("events", venue, len(output.events))
            for event in output.events:
                metrics.observe_event(venue, event.event_type)
        if books is None:
            return
        changed = books.observe(output)
        if metrics is not None:
            for (venue, symbol), live in changed.items():
                metrics.set_book_live(venue, symbol, live)

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

    ``buffer`` and ``metrics`` are the Phase 2 hooks (decisions A70, A71).
    Both default to ``None``, which leaves the Phase 1 path above untouched.
    ``metrics`` only observes: raw messages, rows and events per output batch,
    book LIVE/SYNCING changes, normalize errors, and last-message ages from the
    same one-second tick that flushes idle buffers. A non-``None`` ``buffer``
    switches this call to the buffered ingester (:func:`_run_buffered_ingest`):
    the connectors still run here, but every message is published to the buffer
    instead of being written under ``out``, so this process normalizes nothing
    and its stats carry zero rows (the writer produces them).
    """
    if progress_interval < 0:
        raise ValueError("progress_interval must be non-negative")
    out_path = Path(out)
    if buffer is not None:
        return await _run_buffered_ingest(
            venues,
            symbols,
            out_path,
            buffer,
            duration=duration,
            max_gb=max_gb,
            connector_factories=connector_factories,
            stop_event=stop_event,
            progress_interval=progress_interval,
            metrics=metrics,
        )
    initial_bytes = _tree_bytes(out_path)
    connectors = _build_connectors(venues, symbols, connector_factories)

    stopper = _Stopper(stop_event if stop_event is not None else asyncio.Event())
    raw_writer = RawWriter(out_path)
    parquet = ParquetWriter(out_path, idle_flush_s=_LIVE_IDLE_FLUSH_S)
    books = _BookStates() if metrics is not None else None
    recorder = _Recorder(BookProcessor(), parquet, metrics=metrics, books=books)
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
        _tick(parquet, raw_writer, stopper, out_path, initial_bytes, max_gb, metrics)
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


async def _run_buffered_ingest(
    venues: Sequence[str],
    symbols: Sequence[str],
    out_path: Path,
    buffer: Buffer,
    *,
    duration: float | None,
    max_gb: float | None,
    connector_factories: Mapping[str, ConnectorFactory] | None,
    stop_event: asyncio.Event | None,
    progress_interval: float,
    metrics: PipelineMetrics | None,
) -> PipelineStats:
    """Run the connectors and publish every raw envelope line to ``buffer``.

    The buffered ingester of decision A70: no raw file and no Parquet file is
    written in this process. Messages are published as exactly the bytes
    :func:`tickvault.rawio.encode_line` returns, so the writer's raw layer is
    identical to what the Phase 1 path would have written. A second task serves
    the control topic: each :func:`tickvault.buffer.decode_control` request is
    routed to the connector of its venue (``resync(symbol, reason)``), which is
    how the writer's snapshot requests reach a connection that never dropped.
    A malformed control record is logged and skipped, never fatal.

    Stop priority and draining are the A55 ones: a set stop event is observed
    before the next item is taken, the connectors are closed first, and every
    frame that already reached the queue is still published before the buffer is
    closed. ``max_gb`` does not apply here (nothing is written locally) and is
    only logged. Stats are those of an ingester: ``raw_lines`` counts published
    lines, every normalized counter stays zero.
    """
    connectors = _build_connectors(venues, symbols, connector_factories)
    if max_gb is not None:
        _LOGGER.info("max_gb does not apply to a buffered ingester; the writer enforces it")
    await buffer.start()
    stopper = _Stopper(stop_event if stop_event is not None else asyncio.Event())
    queue: asyncio.Queue[tuple[Connector, RawMessage | None]] = asyncio.Queue(
        maxsize=_LIVE_QUEUE_MAXSIZE
    )
    counters = _BufferedCounters()
    producers = [asyncio.create_task(_pump(connector, queue)) for connector in connectors]
    control = asyncio.create_task(_pump_control(buffer, connectors))
    started = time.monotonic()
    _LOGGER.info(
        "ingest starting: venues=%s symbols=%s out=%s buffer=publish-only",
        ",".join(venues),
        ",".join(symbols),
        out_path,
    )
    ticker = asyncio.create_task(_metrics_tick(metrics)) if metrics is not None else None
    timer = asyncio.create_task(_stop_after(duration, stopper)) if duration is not None else None
    progress = (
        asyncio.create_task(
            _report_buffered_progress(counters, venues, progress_interval, started)
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
                    control.cancel()
                    await _close_connectors(connectors)
                    continue
                _, message = item
            if message is None:
                finished += 1
                continue
            line = encode_line(message)
            await buffer.publish_raw(line)
            counters.published += 1
            counters.by_venue[message.venue] = counters.by_venue.get(message.venue, 0) + 1
            if metrics is not None:
                metrics.observe_raw(message.venue, len(line), time.time())
    finally:
        for signum in handled_signals:
            loop.remove_signal_handler(signum)
        background = [task for task in (ticker, timer, progress) if task is not None]
        for task in background:
            task.cancel()
        await asyncio.gather(*background, return_exceptions=True)
        await _close_connectors(connectors)
        control.cancel()
        for producer in producers:
            producer.cancel()
        discarded = asyncio.ensure_future(_discard(queue))
        try:
            await asyncio.gather(*producers, return_exceptions=True)
        finally:
            discarded.cancel()
            await asyncio.gather(discarded, return_exceptions=True)
        await asyncio.gather(control, return_exceptions=True)
        await buffer.close()
    stats = PipelineStats(
        mode="live",
        raw_lines=counters.published,
        stopped_by=stopper.reason if stopper.reason is not None else "completed",
    )
    _LOGGER.info(
        "ingest finished: stopped=%s elapsed=%.0fs published=%d (%s)",
        stats.stopped_by,
        time.monotonic() - started,
        counters.published,
        " ".join(f"{venue}={counters.by_venue.get(venue, 0)}" for venue in venues),
    )
    return stats


class _BufferedCounters:
    """Mutable publish counters shared with the buffered ingester's log task."""

    __slots__ = ("by_venue", "published")

    def __init__(self) -> None:
        self.published = 0
        self.by_venue: dict[str, int] = {}


async def _report_buffered_progress(
    counters: _BufferedCounters,
    venues: Sequence[str],
    interval: float,
    started: float,
) -> None:
    """Log one ``ingest progress`` line per interval while publishing."""
    loop = asyncio.get_running_loop()
    schedule = started + interval
    while True:
        now = loop.time()
        if schedule > now:
            await asyncio.sleep(schedule - now)
        by_venue = " ".join(
            f"{venue}={counters.by_venue.get(venue, 0)}" for venue in venues
        )
        _LOGGER.info(
            "ingest progress: elapsed=%.0fs published=%d (%s)",
            time.monotonic() - started,
            counters.published,
            by_venue,
        )
        now = loop.time()
        schedule = max(schedule + interval, now + interval)


async def _pump_control(buffer: Buffer, connectors: Sequence[Connector]) -> None:
    """Execute the resync requests the writer publishes to the control topic."""
    try:
        async for payload in buffer.consume_control():
            try:
                request = decode_control(payload)
            except ValueError as exc:
                _LOGGER.warning("ignoring a malformed control record: %s", exc)
                continue
            try:
                await _route_resync(connectors, request)
            except ValueError as exc:
                _LOGGER.warning("ignoring a control request: %s", exc)
    except asyncio.CancelledError:
        raise
    except Exception:
        _LOGGER.exception("control consumer failed")


async def _metrics_tick(metrics: PipelineMetrics) -> None:
    """Refresh the last-message ages once per live tick."""
    while True:
        await asyncio.sleep(_LIVE_TICK_S)
        metrics.tick(time.time())


async def _writer_tick(parquet: ParquetWriter, metrics: PipelineMetrics | None) -> None:
    """Flush idle buffers and refresh message ages once per live tick."""
    while True:
        await asyncio.sleep(_LIVE_TICK_S)
        parquet.flush_idle()
        if metrics is not None:
            metrics.tick(time.time())


async def _writer_lag_tick(
    buffer: Buffer, metrics: PipelineMetrics, position: _ConsumePosition
) -> None:
    """Refresh the buffer-lag gauge once per live tick.

    One second is the contract (decision A71 asks for a lag gauge the writer
    feeds "at most once per second"), and a fresh ``end_offset`` per tick keeps
    the request rate on the broker negligible next to the record traffic.
    """
    while True:
        await asyncio.sleep(_LIVE_TICK_S)
        await _refresh_buffer_lag(buffer, metrics, position)


async def _refresh_buffer_lag(
    buffer: Buffer, metrics: PipelineMetrics, position: _ConsumePosition
) -> None:
    """Set ``tickvault_buffer_lag_messages`` from the broker's end offset.

    The gauge is ``end - next_to_consume``: records the ingester published that
    this writer has not handled yet. Nothing is set before the writer has
    handled its first record (it does not know where a resumed consumer starts)
    nor when the buffer reports no end offset. A broker error is logged at debug
    and swallowed, like every other metrics observation: a lag query must never
    stop a writer that is otherwise making progress.
    """
    next_offset = position.next_offset
    if next_offset is None:
        return
    try:
        end = await buffer.end_offset()
    except Exception:
        _LOGGER.debug("could not read the buffer end offset", exc_info=True)
        return
    if end is None:
        return
    metrics.set_buffer_lag(max(end - next_offset, 0))


class _EndOfRecords:
    """Queue sentinel: the buffer's raw iterator ended."""

    __slots__ = ()


_RECORDS_DONE = _EndOfRecords()


async def _pump_records(
    records: AsyncIterator[BufferRecord], queue: asyncio.Queue[BufferRecord | _EndOfRecords]
) -> None:
    """Forward the buffer's raw records into ``queue``, ending with the sentinel."""
    try:
        async for record in records:
            await queue.put(record)
    except Exception:
        _LOGGER.exception("buffer consumer failed")
    finally:
        await queue.put(_RECORDS_DONE)


async def _next_record_or_stop(
    queue: asyncio.Queue[BufferRecord | _EndOfRecords], stopper: _Stopper
) -> BufferRecord | _EndOfRecords | None:
    """Await the next buffer record, or ``None`` once a stop has been requested.

    The A55 rule of the live loop, applied to the writer: the stop event is
    checked before another record is taken, so a backlog cannot delay a
    ``duration``, signal or ``stop_event``. A record taken with ``get_nowait``
    costs no task pair, which keeps the writer at live message rates.
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
        get_task: asyncio.Task[BufferRecord | _EndOfRecords] = asyncio.ensure_future(
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


async def _discard_records(
    queue: asyncio.Queue[BufferRecord | _EndOfRecords],
) -> None:
    """Empty the writer's record queue forever, so a blocked producer can finish."""
    while True:
        await queue.get()


class _ConsumePosition:
    """Where the writer has got to on the raw topic, for the lag gauge.

    ``next_offset`` is the offset of the first raw record this writer has not
    handled yet. It stays ``None`` until the writer has handled a record,
    because a resumed writer only learns its starting position from the first
    record the broker delivers (the committed offset is not part of the
    :class:`Buffer` protocol).
    """

    __slots__ = ("next_offset",)

    def __init__(self) -> None:
        self.next_offset: int | None = None


async def run_writer(
    buffer: Buffer,
    out: str | Path,
    *,
    duration: float | None = None,
    metrics: PipelineMetrics | None = None,
    stop_event: asyncio.Event | None = None,
) -> PipelineStats:
    """Consume a raw buffer and write it like replay does.

    Every record is written through :class:`RawWriter` **before** it reaches the
    normalizer → processor → Parquet path, so a crash can lose at most the
    uncommitted tail. An offset is committed only after the raw writer has
    flushed everything up to it (an explicit block flush every
    ``_WRITER_COMMIT_RECORDS`` records and once more on a graceful stop), which
    is what makes delivery at-least-once: a restarted writer replays the
    uncommitted tail instead of skipping it.

    Records whose ``(connection_id, ingest_seq)`` this process already wrote are
    dropped (only the records written since the last commit can be redelivered,
    so that set stays bounded). Every book starts SYNCING because this is a new
    processor, while the connectors of the ingester keep their connections: the
    writer therefore publishes one resync request per ``(venue, symbol)`` the
    first time it sees a delta for a symbol that has no LIVE book
    (``reason="writer_sync"``), and the ingester answers it with a fresh
    snapshot. The processor's own resync requests (gaps, checksum failures,
    invariants) are published to the same control topic.

    ``duration``, ``stop_event``, SIGTERM and SIGINT stop it gracefully: the raw
    writer is flushed, the last offset committed, and the writers and the buffer
    closed. An unexpected exception or cancellation closes the writers and the
    buffer without committing, so the next writer replays that tail. The raw
    writer is built with ``resume=True``, so a restart in the same data root
    continues beside the previous process's files instead of failing on them.

    ``metrics`` counts consumed records and, at most once per second, sets
    ``tickvault_buffer_lag_messages`` from :meth:`Buffer.end_offset` minus the
    offset of the next record this writer has to handle. A buffer that cannot
    report its end offset, or raises while doing so, leaves the gauge at its
    previous value: the lag is observability and never fails the writer.
    """
    out_path = Path(out)
    stopper = _Stopper(stop_event if stop_event is not None else asyncio.Event())
    loop = asyncio.get_running_loop()
    await buffer.start()
    raw_writer = RawWriter(out_path, resume=True)
    parquet = ParquetWriter(out_path, idle_flush_s=_LIVE_IDLE_FLUSH_S)
    books = _BookStates()
    recorder = _Recorder(BookProcessor(), parquet, metrics=metrics, books=books)
    queue: asyncio.Queue[BufferRecord | _EndOfRecords] = asyncio.Queue(
        maxsize=_LIVE_QUEUE_MAXSIZE
    )
    pumped = asyncio.create_task(_pump_records(buffer.consume_raw(), queue))
    position = _ConsumePosition()
    started = time.monotonic()
    written: set[tuple[str, int]] = set()
    resync_requested: set[tuple[str, str]] = set()
    pending: list[ResyncRequest] = []
    consumed = 0
    duplicates = 0
    last_offset = -1
    committed = -1

    def note_sync_requests(normalized: NormalizedMessage) -> None:
        """Queue a snapshot request for a symbol that never went LIVE."""
        venue = normalized.raw.venue
        for update in normalized.book_updates:
            key = (venue, update.symbol)
            if update.is_snapshot or key in resync_requested:
                continue
            if books.is_live(venue, update.symbol):
                continue
            resync_requested.add(key)
            pending.append(
                ResyncRequest(
                    venue=venue,
                    connection_id=normalized.raw.connection_id,
                    symbol=update.symbol,
                    reason=_WRITER_SYNC_REASON,
                )
            )

    async def commit_progress() -> None:
        """Flush the raw layer, then commit every record handled so far."""
        nonlocal committed
        if last_offset <= committed:
            return
        raw_writer.flush()
        await buffer.commit(last_offset)
        committed = last_offset
        written.clear()

    _LOGGER.info("writer starting: out=%s", out_path)
    ticker = asyncio.create_task(_writer_tick(parquet, metrics))
    lag = (
        asyncio.create_task(_writer_lag_tick(buffer, metrics, position))
        if metrics is not None
        else None
    )
    timer = asyncio.create_task(_stop_after(duration, stopper)) if duration is not None else None
    handled_signals = _install_signal_handlers(loop, stopper)
    abrupt = False
    shutting_down = False
    try:
        while True:
            if shutting_down:
                try:
                    record = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
            else:
                item = await _next_record_or_stop(queue, stopper)
                if item is None:
                    shutting_down = True
                    continue
                record = item
            if isinstance(record, _EndOfRecords):
                break
            consumed += 1
            try:
                message = decode_line(record.value)
            except ValueError:
                _LOGGER.warning(
                    "writer skipped a malformed buffer record at offset %d", record.offset
                )
            else:
                key = (message.connection_id, message.ingest_seq)
                if key in written:
                    duplicates += 1
                else:
                    del pending[:]
                    raw_writer.write(message)
                    written.add(key)
                    pending.extend(
                        recorder.process(message, on_normalized=note_sync_requests)
                    )
                    for request in pending:
                        await buffer.publish_control(encode_control(request))
            last_offset = record.offset
            position.next_offset = last_offset + 1
            if metrics is not None:
                metrics.observe_consumed()
            if consumed % _WRITER_COMMIT_RECORDS == 0:
                await commit_progress()
    except BaseException:
        abrupt = True
        raise
    finally:
        for signum in handled_signals:
            loop.remove_signal_handler(signum)
        background = [task for task in (ticker, lag, timer) if task is not None]
        for task in background:
            task.cancel()
        await asyncio.gather(*background, return_exceptions=True)
        if not abrupt:
            try:
                await commit_progress()
            except Exception:
                _LOGGER.exception("writer could not commit its final offset")
        pumped.cancel()
        discarded = asyncio.ensure_future(_discard_records(queue))
        try:
            await asyncio.gather(pumped, return_exceptions=True)
        finally:
            discarded.cancel()
            await asyncio.gather(discarded, return_exceptions=True)
        raw_writer.close()
        parquet.close()
        try:
            await buffer.close()
        except Exception:
            _LOGGER.exception("error closing the buffer")
    stats = recorder.stats("writer", stopper.reason if stopper.reason is not None else "completed")
    _LOGGER.info(
        "writer finished: stopped=%s elapsed=%.0fs consumed=%d duplicates=%d "
        "raw_msgs=%d trades=%d book_deltas=%d anomalies=%d resync_requests=%d",
        stats.stopped_by,
        time.monotonic() - started,
        consumed,
        duplicates,
        recorder.raw_lines,
        recorder.trades,
        recorder.book_deltas,
        recorder.anomalies,
        recorder.resync_requests,
    )
    return stats


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


def _build_connectors(
    venues: Sequence[str],
    symbols: Sequence[str],
    connector_factories: Mapping[str, ConnectorFactory] | None,
) -> list[Connector]:
    """Build one connector per venue, validating both lists first."""
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
    return connectors


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
    metrics: PipelineMetrics | None = None,
) -> None:
    """Flush idle Parquet buffers, refresh metrics and enforce ``max_gb``.

    Runs once per second. The size check adds the writers' monotonic byte
    counters to the data root's size measured once at start-up, so it never
    walks the tree while the run is live. ``metrics`` only reads the ages of
    the last raw message per venue from the same tick; it never changes a flush
    or a stop decision.
    """
    while True:
        await asyncio.sleep(_LIVE_TICK_S)
        parquet.flush_idle()
        if metrics is not None:
            metrics.tick(time.time())
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
