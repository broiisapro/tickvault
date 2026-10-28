"""Prometheus metrics for the pipeline (decision A71).

One :class:`PipelineMetrics` instance owns its own
:class:`prometheus_client.CollectorRegistry`, never the process-wide default
one, so two instances (or two tests) in one process never collide. Every
metric name is prefixed ``tickvault_``; no label carries anything secret or
host-specific.

``ingest --metrics-port N`` and ``consume --metrics-port N`` build one
instance, call :meth:`PipelineMetrics.serve` and feed it from the pipeline.
The metrics are observability only: nothing in the data path depends on them.
"""

from __future__ import annotations

from wsgiref.simple_server import WSGIServer

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    generate_latest,
    start_http_server,
)


class MetricsServer:
    """A running Prometheus HTTP endpoint, started on a daemon thread."""

    def __init__(self, httpd: WSGIServer) -> None:
        self._httpd = httpd
        self._port = int(httpd.socket.getsockname()[1])

    @property
    def port(self) -> int:
        """The TCP port actually bound (useful with ``port=0``); stable after close."""
        return self._port

    def close(self) -> None:
        """Stop serving and release the socket."""
        self._httpd.shutdown()
        self._httpd.server_close()


class PipelineMetrics:
    """Counters and gauges describing one ingest or writer process."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry if registry is not None else CollectorRegistry()
        self.messages_total = Counter(
            "tickvault_messages_total",
            "Raw venue messages received.",
            ["venue"],
            registry=self.registry,
        )
        self.rows_total = Counter(
            "tickvault_rows_total",
            "Normalized rows written, by table and venue.",
            ["table", "venue"],
            registry=self.registry,
        )
        self.events_total = Counter(
            "tickvault_events_total",
            "Data-quality events emitted, by venue and type.",
            ["venue", "event_type"],
            registry=self.registry,
        )
        self.book_live = Gauge(
            "tickvault_book_live",
            "1 while the book is LIVE, 0 while it is SYNCING.",
            ["venue", "symbol"],
            registry=self.registry,
        )
        self.raw_bytes_total = Counter(
            "tickvault_raw_bytes_total",
            "Raw envelope bytes published or written.",
            registry=self.registry,
        )
        self.last_message_age_seconds = Gauge(
            "tickvault_last_message_age_seconds",
            "Seconds since the last raw message of a venue.",
            ["venue"],
            registry=self.registry,
        )
        self.normalize_errors_total = Counter(
            "tickvault_normalize_errors_total",
            "Raw messages a venue normalizer rejected.",
            ["venue"],
            registry=self.registry,
        )
        self.buffer_consumed_total = Counter(
            "tickvault_buffer_consumed_total",
            "Raw records consumed from the buffer by the writer.",
            registry=self.registry,
        )
        self.buffer_lag_messages = Gauge(
            "tickvault_buffer_lag_messages",
            "Raw records published but not yet consumed.",
            registry=self.registry,
        )
        self._last_message_s: dict[str, float] = {}

    def observe_raw(self, venue: str, nbytes: int, now_s: float) -> None:
        """Count one raw message, its bytes and the venue's last-message time."""
        self.messages_total.labels(venue).inc()
        self.raw_bytes_total.inc(nbytes)
        self._last_message_s[venue] = now_s

    def observe_rows(self, table: str, venue: str, count: int) -> None:
        """Add ``count`` normalized rows for ``venue``'s ``table``.

        A zero count still exposes the ``(table, venue)`` series at zero, so a
        dashboard shows a table that wrote nothing instead of a gap.
        """
        self.rows_total.labels(table, venue).inc(count)

    def observe_event(self, venue: str, event_type: str) -> None:
        """Count one data-quality event."""
        self.events_total.labels(venue, event_type).inc()

    def observe_normalize_error(self, venue: str) -> None:
        """Count one rejected raw message."""
        self.normalize_errors_total.labels(venue).inc()

    def set_book_live(self, venue: str, symbol: str, live: bool) -> None:
        """Set the book's LIVE (1) / SYNCING (0) gauge."""
        self.book_live.labels(venue, symbol).set(1.0 if live else 0.0)

    def tick(self, now_s: float) -> None:
        """Refresh every known venue's last-message age from ``now_s``."""
        for venue, last_s in self._last_message_s.items():
            self.last_message_age_seconds.labels(venue).set(max(now_s - last_s, 0.0))

    def observe_consumed(self, count: int = 1) -> None:
        """Count raw records consumed from the buffer."""
        self.buffer_consumed_total.inc(count)

    def set_buffer_lag(self, lag: int) -> None:
        """Set the buffer lag gauge, in raw records."""
        self.buffer_lag_messages.set(lag)

    def render(self) -> bytes:
        """Render this registry in the Prometheus text exposition format."""
        return generate_latest(self.registry)

    def serve(self, port: int, addr: str = "0.0.0.0") -> MetricsServer:
        """Expose this registry over HTTP; ``port=0`` picks an ephemeral port."""
        httpd, _thread = start_http_server(port, addr=addr, registry=self.registry)
        return MetricsServer(httpd)
