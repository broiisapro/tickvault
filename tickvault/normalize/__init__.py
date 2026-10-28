"""Pure normalizers from raw venue frames to normalized messages.

Every venue normalizer raises :class:`NormalizeError` on a malformed payload.
A normalizer is a function of raw lines and nothing else: no wall clock, no
network and no ambient configuration. Most are pure functions of one
:class:`~tickvault.models.RawMessage`; a venue whose frames only name a numeric
channel id (Bitfinex) may keep per-connection state, provided that state is
derived **only** from raw lines seen in canonical
``(recv_ts_ns, connection_id, ingest_seq)`` order and is replaced or dropped on
that connection's ``connect``/``disconnect`` control lines. Replay therefore
rebuilds the same state in a fresh process as in one that already replayed the
same file, and the normalizer stays deterministic.
"""

from __future__ import annotations


class NormalizeError(ValueError):
    """Raised when a raw venue frame cannot be normalized."""
