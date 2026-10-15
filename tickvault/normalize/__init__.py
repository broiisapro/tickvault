"""Pure normalizers from raw venue frames to normalized messages.

Every venue normalizer raises :class:`NormalizeError` on a malformed payload.
Normalizers are pure functions of one :class:`~tickvault.models.RawMessage`:
no wall clock, no state, no network.
"""

from __future__ import annotations


class NormalizeError(ValueError):
    """Raised when a raw venue frame cannot be normalized."""
