"""Per-(venue, symbol) book state machine.

State-machine rules, from the architecture document, section 8:

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

Implementation notes:

- Sequence tracking applies to any message carrying a ``sequence``, including
  venue housekeeping frames: Coinbase heartbeats consume ``sequence_num`` and
  must reach the tracker. The gap-revealing message's book levels are
  discarded and count neither as applied nor as dropped.
- A ``STALE`` sequence ignores the message's book levels and increments the
  public ``stale_messages`` counter; no event is emitted.
- The anomaly-message accounting of decision A19 applies to checksum failures
  too: the update's levels are applied to the internal book only so the
  checksum can be verified, and its rows are not emitted.
- ``detail`` strings are ``json.dumps`` with sorted keys and no whitespace, so
  the output is deterministic in replay.
- A connection's symbols are learned from the book updates it sends; a gap
  affects those symbols, one event and one resync request each, in sorted
  order.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from tickvault.book.invariants import check_invariants
from tickvault.book.kraken_checksum import kraken_checksum
from tickvault.book.orderbook import OrderBook
from tickvault.book.sequence import GAP, STALE, SequenceTracker
from tickvault.models import (
    BookDelta,
    BookSnapshotLevel,
    BookUpdate,
    Event,
    NormalizedMessage,
    PriceLevel,
    Side,
    SnapshotSource,
    Trade,
)

ChecksumFn = Callable[[Sequence[PriceLevel], Sequence[PriceLevel]], int]
_TOP_LEVELS = 10
_EMPTY_DETAIL = "{}"
_SIDES: tuple[Side, Side] = ("bid", "ask")


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


@dataclass(slots=True)
class _SymbolState:
    """Mutable per-(venue, symbol) processor state."""

    venue: str
    symbol: str
    connection_id: str
    book: OrderBook = field(default_factory=OrderBook)
    live: bool = False
    resync_reason: str | None = None
    dropped_deltas: int = 0
    last_sample_second: int | None = None


def _detail(**fields: object) -> str:
    return json.dumps(fields, sort_keys=True, separators=(",", ":"))


def _message_exchange_ts_ns(msg: NormalizedMessage) -> int | None:
    if msg.book_updates:
        return msg.book_updates[0].exchange_ts_ns
    if msg.trades:
        return msg.trades[0].exchange_ts_ns
    return None


class BookProcessor:
    """Deterministic processor turning normalized messages into table rows.

    ``kraken_depth`` is the Kraken book depth used for checksum truncation.
    ``sample_interval_ns`` is the interval between top-10 ``sample`` snapshots
    per symbol while LIVE.
    """

    def __init__(self, kraken_depth: int = 25, sample_interval_ns: int = 1_000_000_000) -> None:
        self.kraken_depth = kraken_depth
        self.sample_interval_ns = sample_interval_ns
        self.stale_messages = 0
        self.checksum_registry: dict[str, ChecksumFn] = {"kraken": kraken_checksum}
        self._tracker = SequenceTracker()
        self._states: dict[tuple[str, str], _SymbolState] = {}
        self._connection_symbols: dict[str, set[str]] = {}
        self._venue_connections: dict[str, set[str]] = {}
        self._venue_current: dict[str, str] = {}

    def process(self, msg: NormalizedMessage) -> ProcessorOutput:
        """Apply one message and return the rows and resync requests it produced."""
        output = ProcessorOutput()
        output.trades.extend(msg.trades)
        if msg.control is not None:
            self._process_control(msg, output)
        else:
            self._process_message(msg, output)
        return output

    # ------------------------------------------------------------------ books

    def _process_message(self, msg: NormalizedMessage, output: ProcessorOutput) -> None:
        raw = msg.raw
        known = self._connection_symbols.setdefault(raw.connection_id, set())
        for update in msg.book_updates:
            known.add(update.symbol)

        if msg.sequence is not None:
            result = self._tracker.observe(raw.connection_id, msg.sequence)
            if isinstance(result, GAP):
                self._handle_gap(msg, result, output)
                return
            if isinstance(result, STALE):
                self.stale_messages += 1
                return

        for update in msg.book_updates:
            if update.is_snapshot:
                self._handle_snapshot(msg, update, output)
            else:
                self._handle_delta(msg, update, output)

    def _handle_gap(self, msg: NormalizedMessage, gap: GAP, output: ProcessorOutput) -> None:
        raw = msg.raw
        exchange_ts_ns = _message_exchange_ts_ns(msg)
        for symbol in sorted(self._connection_symbols.get(raw.connection_id, ())):
            state = self._ensure_state(raw.venue, symbol, raw.connection_id)
            if state.connection_id != raw.connection_id:
                continue  # the gapping connection is no longer authoritative
            state.live = False
            state.resync_reason = "gap"
            output.events.append(
                Event(
                    venue=raw.venue,
                    symbol=symbol,
                    exchange_ts_ns=exchange_ts_ns,
                    recv_ts_ns=raw.recv_ts_ns,
                    connection_id=raw.connection_id,
                    ingest_seq=raw.ingest_seq,
                    event_type="gap",
                    channel=msg.channel,
                    expected_seq=gap.expected,
                    received_seq=gap.received,
                    detail=_EMPTY_DETAIL,
                )
            )
            output.resync_requests.append(
                ResyncRequest(
                    venue=raw.venue,
                    connection_id=raw.connection_id,
                    symbol=symbol,
                    reason="gap",
                )
            )

    def _handle_snapshot(
        self, msg: NormalizedMessage, update: BookUpdate, output: ProcessorOutput
    ) -> None:
        raw = msg.raw
        state = self._ensure_state(raw.venue, update.symbol, raw.connection_id)
        if state.connection_id != raw.connection_id:
            return
        state.book.apply_snapshot(update.bids, update.asks)

        computed = self._verify_checksum(state, update)
        if computed is not None and update.checksum is not None and computed != update.checksum:
            self._flag_checksum_failure(msg, update, state, computed, output)
            return

        rows = self._snapshot_rows(msg, update, state, source="venue", depth=len(state.book))
        output.book_snapshots.extend(rows)
        violations = check_invariants(state.book)
        if violations:
            self._flag_invariant_violation(msg, update, state, violations, output)
            return

        state.live = True
        if state.resync_reason is not None:
            output.events.append(self._resync_event(msg, update, state))
            state.resync_reason = None
            state.dropped_deltas = 0
        self._maybe_sample(msg, update, state, output)

    def _handle_delta(
        self, msg: NormalizedMessage, update: BookUpdate, output: ProcessorOutput
    ) -> None:
        raw = msg.raw
        state = self._ensure_state(raw.venue, update.symbol, raw.connection_id)
        level_count = len(update.bids) + len(update.asks)
        if not state.live or state.connection_id != raw.connection_id:
            if not state.live:
                state.dropped_deltas += level_count
            return

        deltas: list[BookDelta] = []
        for side in _SIDES:
            levels = update.bids if side == "bid" else update.asks
            for price, size in levels:
                state.book.apply_delta(side, price, size)
                deltas.append(
                    BookDelta(
                        venue=raw.venue,
                        symbol=update.symbol,
                        exchange_ts_ns=update.exchange_ts_ns,
                        recv_ts_ns=raw.recv_ts_ns,
                        connection_id=raw.connection_id,
                        ingest_seq=raw.ingest_seq,
                        side=side,
                        price=price,
                        size=size,
                        sequence=msg.sequence,
                    )
                )

        computed = self._verify_checksum(state, update)
        if computed is not None and update.checksum is not None and computed != update.checksum:
            self._flag_checksum_failure(msg, update, state, computed, output)
            return

        output.book_deltas.extend(deltas)
        violations = check_invariants(state.book)
        if violations:
            self._flag_invariant_violation(msg, update, state, violations, output)
            return
        self._maybe_sample(msg, update, state, output)

    def _verify_checksum(self, state: _SymbolState, update: BookUpdate) -> int | None:
        """Truncate a registered venue's book to the subscribed depth and hash it.

        Returns the computed checksum, or ``None`` when the venue is not in the
        registry or the message carries no checksum to compare against.
        """
        checksum_fn = self.checksum_registry.get(state.venue)
        if checksum_fn is None:
            return None
        state.book.truncate(self.kraken_depth)
        if update.checksum is None:
            return None
        bids, asks = state.book.top(_TOP_LEVELS)
        return checksum_fn(bids, asks)

    def _flag_checksum_failure(
        self,
        msg: NormalizedMessage,
        update: BookUpdate,
        state: _SymbolState,
        computed: int,
        output: ProcessorOutput,
    ) -> None:
        state.live = False
        state.resync_reason = "checksum_failure"
        output.events.append(
            Event(
                venue=msg.raw.venue,
                symbol=update.symbol,
                exchange_ts_ns=update.exchange_ts_ns,
                recv_ts_ns=msg.raw.recv_ts_ns,
                connection_id=msg.raw.connection_id,
                ingest_seq=msg.raw.ingest_seq,
                event_type="checksum_failure",
                channel=msg.channel,
                detail=_detail(computed=computed, expected=update.checksum),
            )
        )
        output.resync_requests.append(
            ResyncRequest(
                venue=msg.raw.venue,
                connection_id=msg.raw.connection_id,
                symbol=update.symbol,
                reason="checksum_failure",
            )
        )

    def _flag_invariant_violation(
        self,
        msg: NormalizedMessage,
        update: BookUpdate,
        state: _SymbolState,
        violations: list[str],
        output: ProcessorOutput,
    ) -> None:
        state.live = False
        state.resync_reason = "invariant_violation"
        output.events.append(
            Event(
                venue=msg.raw.venue,
                symbol=update.symbol,
                exchange_ts_ns=update.exchange_ts_ns,
                recv_ts_ns=msg.raw.recv_ts_ns,
                connection_id=msg.raw.connection_id,
                ingest_seq=msg.raw.ingest_seq,
                event_type="invariant_violation",
                channel=msg.channel,
                detail=_detail(codes=violations),
            )
        )
        output.resync_requests.append(
            ResyncRequest(
                venue=msg.raw.venue,
                connection_id=msg.raw.connection_id,
                symbol=update.symbol,
                reason="invariant_violation",
            )
        )

    def _resync_event(
        self, msg: NormalizedMessage, update: BookUpdate, state: _SymbolState
    ) -> Event:
        return Event(
            venue=msg.raw.venue,
            symbol=update.symbol,
            exchange_ts_ns=update.exchange_ts_ns,
            recv_ts_ns=msg.raw.recv_ts_ns,
            connection_id=msg.raw.connection_id,
            ingest_seq=msg.raw.ingest_seq,
            event_type="resync",
            channel=msg.channel,
            detail=_detail(reason=state.resync_reason, deltas_dropped=state.dropped_deltas),
        )

    def _snapshot_rows(
        self,
        msg: NormalizedMessage,
        update: BookUpdate,
        state: _SymbolState,
        *,
        source: SnapshotSource,
        depth: int,
    ) -> list[BookSnapshotLevel]:
        raw = msg.raw
        snapshot_id = f"{raw.connection_id}:{raw.ingest_seq}"
        bids, asks = state.book.top(depth)
        rows: list[BookSnapshotLevel] = []
        for side in _SIDES:
            levels = bids if side == "bid" else asks
            for level, (price, size) in enumerate(levels):
                rows.append(
                    BookSnapshotLevel(
                        venue=raw.venue,
                        symbol=update.symbol,
                        exchange_ts_ns=update.exchange_ts_ns,
                        recv_ts_ns=raw.recv_ts_ns,
                        connection_id=raw.connection_id,
                        ingest_seq=raw.ingest_seq,
                        snapshot_id=snapshot_id,
                        source=source,
                        side=side,
                        level=level,
                        price=price,
                        size=size,
                    )
                )
        return rows

    def _maybe_sample(
        self,
        msg: NormalizedMessage,
        update: BookUpdate,
        state: _SymbolState,
        output: ProcessorOutput,
    ) -> None:
        if not state.live:
            return
        second = update.exchange_ts_ns // self.sample_interval_ns
        if state.last_sample_second is not None and second <= state.last_sample_second:
            return
        state.last_sample_second = second
        output.book_snapshots.extend(
            self._snapshot_rows(msg, update, state, source="sample", depth=_TOP_LEVELS)
        )

    # --------------------------------------------------------------- controls

    def _process_control(self, msg: NormalizedMessage, output: ProcessorOutput) -> None:
        control = msg.control
        if control is None:
            return
        event = control.get("event")
        if event == "connect":
            self._handle_connect(msg, output)
        elif event == "disconnect":
            reason = control.get("reason")
            detail = _EMPTY_DETAIL if reason is None else _detail(reason=reason)
            output.events.append(
                Event(
                    venue=msg.raw.venue,
                    recv_ts_ns=msg.raw.recv_ts_ns,
                    connection_id=msg.raw.connection_id,
                    ingest_seq=msg.raw.ingest_seq,
                    event_type="disconnect",
                    detail=detail,
                )
            )

    def _handle_connect(self, msg: NormalizedMessage, output: ProcessorOutput) -> None:
        raw = msg.raw
        seen = self._venue_connections.setdefault(raw.venue, set())
        first_connection = not seen
        seen.add(raw.connection_id)
        self._venue_current[raw.venue] = raw.connection_id
        if first_connection:
            return
        for symbol in self._venue_symbols(raw.venue):
            state = self._states[(raw.venue, symbol)]
            state.connection_id = raw.connection_id
            state.live = False
            state.resync_reason = "reconnect"
            output.events.append(
                Event(
                    venue=raw.venue,
                    symbol=symbol,
                    recv_ts_ns=raw.recv_ts_ns,
                    connection_id=raw.connection_id,
                    ingest_seq=raw.ingest_seq,
                    event_type="reconnect",
                    detail=_EMPTY_DETAIL,
                )
            )

    def _venue_symbols(self, venue: str) -> list[str]:
        return sorted(symbol for (state_venue, symbol) in self._states if state_venue == venue)

    def _ensure_state(self, venue: str, symbol: str, connection_id: str) -> _SymbolState:
        state = self._states.get((venue, symbol))
        if state is None:
            authoritative = self._venue_current.get(venue, connection_id)
            state = _SymbolState(venue=venue, symbol=symbol, connection_id=authoritative)
            self._states[(venue, symbol)] = state
        return state
