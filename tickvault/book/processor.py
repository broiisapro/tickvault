"""Per-(venue, symbol) book state machine interface.

The live implementation (Epic 1) replaces the ``NotImplementedError`` body
while keeping this signature. State-machine rules, from the architecture
document, section 8:

- ``BookProcessor`` holds per-``(venue, symbol)`` state: ``SYNCING`` or
  ``LIVE``, the authoritative ``connection_id``, and an ``OrderBook``.
- Snapshot from the authoritative connection → replace book, ``LIVE``; if the
  state was SYNCING because of an anomaly, emit ``resync`` (with ``reason``,
  ``deltas_dropped``).
- Delta while ``SYNCING`` → dropped (counted). Delta from a non-authoritative
  (older) connection → dropped, always. This is the reconnect race guard.
- Coinbase: ``sequence`` is tracked per ``connection_id``. A gap emits ``gap``
  and moves every symbol on that connection to SYNCING, and emits a resync
  request (live: unsubscribe+resubscribe ``level2`` for those products).
- Kraken: after applying a message's levels and truncating to subscribed depth
  (25), compute the checksum; mismatch → ``checksum_failure``, SYNCING, resync
  request (live: unsubscribe+resubscribe ``book`` for that symbol).
- Invariants after every applied update: best bid < best ask, no negative
  size, no duplicate levels → ``invariant_violation``, SYNCING, resync
  request.
- Control ``connect`` with a new ``connection_id`` for a venue → that
  connection becomes authoritative for its symbols, symbols go SYNCING, emit
  ``reconnect`` (not for the very first connection of a venue in a session).
  Control ``disconnect`` → ``disconnect`` event.
- While LIVE, emit a ``book_snapshots`` ``sample`` (top 10 per side) for each
  symbol every 1 s of exchange time (first update at or after each whole
  second). This feeds the dashboard's top-of-book and spread/mid charts
  without rebuilding books in the UI.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from tickvault.models import BookDelta, BookSnapshotLevel, Event, NormalizedMessage, Trade


@dataclass(frozen=True, slots=True)
class ResyncRequest:
    """Ask the live connector to resynchronize one symbol after an anomaly."""

    venue: str
    connection_id: str
    symbol: str
    reason: str


@dataclass(slots=True)
class ProcessorOutput:
    """Records and side effects produced by processing one normalized message."""

    trades: list[Trade] = field(default_factory=list)
    book_deltas: list[BookDelta] = field(default_factory=list)
    book_snapshots: list[BookSnapshotLevel] = field(default_factory=list)
    events: list[Event] = field(default_factory=list)
    resync_requests: list[ResyncRequest] = field(default_factory=list)


class BookProcessor:
    """Deterministic processor turning normalized messages into table rows.

    ``kraken_depth`` is the Kraken book depth used for checksum truncation.
    ``sample_interval_ns`` is the interval between top-10 ``sample`` snapshots
    per symbol while LIVE.
    """

    def __init__(self, kraken_depth: int = 25, sample_interval_ns: int = 1_000_000_000) -> None:
        self.kraken_depth = kraken_depth
        self.sample_interval_ns = sample_interval_ns

    def process(self, msg: NormalizedMessage) -> ProcessorOutput:
        """Apply one message and return the rows and resync requests it produced."""
        raise NotImplementedError
