"""Kraken WS v2 CRC32 book checksum.

Kraken sends a ``checksum`` on every book message: CRC32 over the top 10
levels per side of the book *after* the message has been applied and the book
truncated to the subscribed depth. The digest input is the asks (ascending
price) followed by the bids (descending price); each level contributes its
price digits then its size digits, with the decimal point removed and leading
zeros stripped. Trailing zeros are significant, so prices and sizes must be
parsed from the venue's JSON text as :class:`~decimal.Decimal` and rendered
with ``format(value, "f")``: ``str`` on a ``Decimal`` may use exponent
notation for small values, and a float parse loses the trailing zeros
entirely (``0.00006120`` becomes ``6.12e-05``).
"""

from __future__ import annotations

import zlib
from collections.abc import Sequence
from decimal import Decimal

from tickvault.models import PriceLevel

_TOP_LEVELS = 10


def _canonical_digits(value: Decimal) -> str:
    """Digit text of one number: no point, leading zeros stripped, zeros kept."""
    digits = format(value, "f").replace(".", "").lstrip("0")
    return digits or "0"


def kraken_checksum(bids: Sequence[PriceLevel], asks: Sequence[PriceLevel]) -> int:
    """CRC32 over the top 10 levels of ``bids`` and ``asks``, best first.

    ``bids`` must be ordered best-first (descending price) and ``asks``
    best-first (ascending). Callers truncate the book to the subscribed depth
    before calling; only the first 10 levels per side are hashed.
    """
    payload = "".join(
        _canonical_digits(price) + _canonical_digits(size) for price, size in asks[:_TOP_LEVELS]
    )
    payload += "".join(
        _canonical_digits(price) + _canonical_digits(size) for price, size in bids[:_TOP_LEVELS]
    )
    return zlib.crc32(payload.encode("ascii")) & 0xFFFFFFFF
