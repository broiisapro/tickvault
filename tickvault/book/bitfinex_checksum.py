"""Bitfinex WS v2 CRC32 book checksum.

Bitfinex sends a ``cs`` frame after a batch of book changes. The checksum is
CRC32 over the top 25 levels per side (the subscribed ``len``), rendered the
way the venue's reference client renders numbers: each level contributes
``price:amount``, the bid and the ask of the same rank alternate (bid first),
asks carry a negative amount, and the whole list is joined with ``:``. A book
with fewer than 25 levels on a side simply contributes the levels it has.

Number rendering is the venue's own quirk, the Bitfinex form of the Kraken
trailing-zero trap: the reference client is JavaScript, so a price or size is
rendered by ``Number.prototype.toString`` -- the shortest round-trip digits of
the IEEE-754 double, plain notation for ``1e-6 <= |x| < 1e21`` and exponent
form outside that window (``1e-7``, never Python's ``1e-07``; ``1e+21``), no
trailing zeros (``0.50`` renders as ``0.5``) and no negative zero. Parsing the
venue text as :class:`~decimal.Decimal` keeps the stored book exact; only the
checksum input goes through the double conversion, because that is what the
venue hashes.

The CRC32 is reported as a **signed** 32-bit integer: about half of the live
``cs`` frames are negative, and comparing them with ``zlib.crc32``'s unsigned
value silently never matches.
"""

from __future__ import annotations

import math
import zlib
from collections.abc import Sequence
from decimal import Decimal

from tickvault.models import PriceLevel

_TOP_LEVELS = 25
_UNSIGNED = 0x1_0000_0000
_SIGN_BIT = 0x8000_0000


def js_number(value: Decimal) -> str:
    """Render ``value`` as JavaScript ``Number.prototype.toString`` would.

    The shortest round-trip digits come from Python's ``repr`` of the float64
    (CPython has used shortest-repr since 3.1), then the ECMAScript
    ``Number::toString`` layout rules are applied: plain decimal notation for
    ``1e-6 <= |x| < 1e21`` and exponent form (``1e-7``, ``1.5e-8``, ``1e+21``)
    outside it, never a trailing zero and never a negative zero.
    """
    number = float(value)
    if math.isnan(number):
        return "NaN"
    if math.isinf(number):
        return "-Infinity" if number < 0 else "Infinity"
    if number == 0:
        return "0"
    negative = number < 0
    _sign, digits, exponent = Decimal(repr(abs(number))).as_tuple()
    assert isinstance(exponent, int)  # finite decimals always have an int exponent
    # Shortest repr can keep trailing zeros (``100.0``); JavaScript never does.
    significant = list(digits)
    while len(significant) > 1 and significant[-1] == 0:
        significant.pop()
        exponent += 1
    digit_text = "".join(str(digit) for digit in significant)
    count = len(digit_text)
    # value == digit_text * 10**(point - count), ECMAScript's n and k.
    point = count + exponent
    if count <= point <= 21:
        rendered = digit_text + "0" * (point - count)
    elif 0 < point <= 21:
        rendered = f"{digit_text[:point]}.{digit_text[point:]}"
    elif -6 < point <= 0:
        rendered = "0." + "0" * -point + digit_text
    else:
        mantissa = digit_text if count == 1 else f"{digit_text[0]}.{digit_text[1:]}"
        power = point - 1
        rendered = f"{mantissa}e{'+' if power >= 0 else '-'}{abs(power)}"
    return f"-{rendered}" if negative else rendered


def bitfinex_checksum(bids: Sequence[PriceLevel], asks: Sequence[PriceLevel]) -> int:
    """CRC32 over the top 25 levels of ``bids`` and ``asks``, best first.

    ``bids`` must be ordered best-first (descending price) and ``asks``
    best-first (ascending); sizes are positive. Only the first 25 levels per
    side are hashed. The digest input alternates ``bid:ask`` for each rank,
    with ask amounts negated, and the CRC32 is returned as a signed 32-bit int
    so it compares equal to the venue's ``cs`` value.
    """
    parts: list[str] = []
    for level in range(_TOP_LEVELS):
        if level < len(bids):
            price, size = bids[level]
            parts.append(f"{js_number(price)}:{js_number(size)}")
        if level < len(asks):
            price, size = asks[level]
            parts.append(f"{js_number(price)}:{js_number(-size)}")
    crc = zlib.crc32(":".join(parts).encode("ascii"))
    return crc - _UNSIGNED if crc >= _SIGN_BIT else crc
