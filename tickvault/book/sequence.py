"""Sequence-gap detection shared by the venue connectors.

A tracker keeps the last accepted sequence per key (for Coinbase the key is
``connection_id``). The rule is ``expected = last + 1``: a received sequence
above ``expected`` is a gap and becomes the new last; a received sequence
below ``expected`` is stale and leaves the last unchanged. One missing number
is exactly one :class:`GAP` with ``expected = n + 1`` and ``received = n + 2``.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class FIRST:
    """The first sequence observed for a key."""


@dataclass(frozen=True, slots=True)
class OK:
    """The observed sequence is exactly the next expected one."""


@dataclass(frozen=True, slots=True)
class GAP:
    """The observed sequence skipped ahead of the expected one."""

    expected: int
    received: int


@dataclass(frozen=True, slots=True)
class STALE:
    """The observed sequence repeats or goes backwards; the last is unchanged."""

    received: int


type SeqResult = FIRST | OK | GAP | STALE


class SequenceTracker:
    """Tracks the last accepted sequence per key and classifies observations."""

    def __init__(self) -> None:
        self._last: dict[str, int] = {}

    def observe(self, key: str, seq: int) -> SeqResult:
        """Classify ``seq`` for ``key`` and update the tracked last sequence.

        ``seq > expected`` returns :class:`GAP` and advances the last sequence;
        ``seq < expected`` returns :class:`STALE` and leaves it unchanged; the
        first observation for a key returns :class:`FIRST`.
        """
        previous = self._last.get(key)
        if previous is None:
            self._last[key] = seq
            return FIRST()
        expected = previous + 1
        if seq > expected:
            self._last[key] = seq
            return GAP(expected=expected, received=seq)
        if seq < expected:
            return STALE(received=seq)
        self._last[key] = seq
        return OK()

    def reset(self, key: str) -> None:
        """Forget ``key`` so its next observation is :class:`FIRST` again."""
        self._last.pop(key, None)

    def last(self, key: str) -> int | None:
        """Return the last accepted sequence for ``key``, or ``None``."""
        return self._last.get(key)
