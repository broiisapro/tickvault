"""Kraken CRC32 checksum tests.

The two reference vectors come from Kraken's documentation and from a live
BTC/USD depth-10 snapshot captured on 2026-10-15; the live vector is the one
that exposes the classic float-parsing bug, because two of its quantities end
in a significant zero.
"""

from __future__ import annotations

import zlib
from decimal import Decimal

from tickvault.book.kraken_checksum import kraken_checksum
from tickvault.models import PriceLevel

DOCUMENTED_ASKS = [  # ascending, best first
    ("45285.2", "0.00100000"),
    ("45286.4", "1.54571953"),
    ("45286.6", "1.54571109"),
    ("45289.6", "1.54560911"),
    ("45290.2", "0.15890660"),
    ("45291.8", "1.54553491"),
    ("45294.7", "0.04454749"),
    ("45296.1", "0.35380000"),
    ("45297.5", "0.09945542"),
    ("45299.5", "0.18772827"),
]
DOCUMENTED_BIDS = [  # descending, best first
    ("45283.5", "0.10000000"),
    ("45283.4", "1.54582015"),
    ("45282.1", "0.10000000"),
    ("45281.0", "0.10000000"),
    ("45280.3", "1.54592586"),
    ("45279.0", "0.07990000"),
    ("45277.6", "0.03310103"),
    ("45277.5", "0.30000000"),
    ("45277.3", "1.54602737"),
    ("45276.6", "0.15445238"),
]
LIVE_ASKS = [  # ascending, best first
    ("81207.2", "0.84869087"),
    ("81207.7", "0.18471158"),
    ("81209.7", "0.09431125"),
    ("81209.8", "1.57483831"),
    ("81209.9", "3.07844585"),
    ("81211.0", "0.00006120"),
    ("81212.1", "3.07836042"),
    ("81213.0", "0.01900000"),
    ("81213.2", "0.00019000"),
    ("81213.8", "0.00010000"),
]
LIVE_BIDS = [  # descending, best first
    ("81207.1", "0.00056179"),
    ("81203.0", "0.00006120"),
    ("81199.8", "0.01000000"),
    ("81199.1", "0.18473102"),
    ("81199.0", "0.00129514"),
    ("81198.9", "0.05405688"),
    ("81196.9", "0.00020000"),
    ("81195.9", "0.22736237"),
    ("81195.8", "3.07897538"),
    ("81194.9", "0.00006120"),
]
LIVE_EXPECTED = 1_264_451_060
FLOAT_BUG_EXPECTED = 3_408_421_861


def _levels(pairs: list[tuple[str, str]]) -> list[PriceLevel]:
    return [(Decimal(price), Decimal(size)) for price, size in pairs]


def _canonical_text(text: str) -> str:
    return text.replace(".", "").lstrip("0") or "0"


def test_checksum_matches_reference_vectors() -> None:
    assert (
        kraken_checksum(_levels(DOCUMENTED_BIDS), _levels(DOCUMENTED_ASKS)) == 3_310_070_434
    ), "Kraken's documented depth-10 example must hash to 3310070434"
    assert kraken_checksum(_levels(LIVE_BIDS), _levels(LIVE_ASKS)) == LIVE_EXPECTED, (
        "the live 2026-10-15 BTC/USD depth-10 snapshot must hash to 1264451060"
    )


def test_float_parsing_breaks_checksum() -> None:
    """A float parse loses the trailing zeros and hashes the wrong payload."""

    def naive_level(price: str, size: str) -> str:
        return _canonical_text(str(float(price))) + _canonical_text(str(float(size)))

    float_payload = "".join(naive_level(*pair) for pair in LIVE_ASKS)
    float_payload += "".join(naive_level(*pair) for pair in LIVE_BIDS)
    assert zlib.crc32(float_payload.encode("ascii")) & 0xFFFFFFFF == FLOAT_BUG_EXPECTED
    assert float_payload != "".join(
        _canonical_text(price) + _canonical_text(size) for price, size in LIVE_ASKS + LIVE_BIDS
    )
    assert kraken_checksum(_levels(LIVE_BIDS), _levels(LIVE_ASKS)) == LIVE_EXPECTED


def test_trailing_zeros_are_significant() -> None:
    with_trailing_zero = kraken_checksum([(Decimal("81211.0"), Decimal("0.00006120"))], [])
    trimmed = kraken_checksum([(Decimal("81211.0"), Decimal("0.0000612"))], [])
    assert with_trailing_zero == zlib.crc32(b"8121106120") & 0xFFFFFFFF
    assert with_trailing_zero != trimmed


def test_exponent_decimal_formats_like_its_plain_digits() -> None:
    assert kraken_checksum([(Decimal("1E+1"), Decimal("1E-8"))], []) == zlib.crc32(b"101")


def test_only_the_top_ten_levels_per_side_are_hashed() -> None:
    deep_bids = [*_levels(LIVE_BIDS), (Decimal("1"), Decimal("1"))]
    deep_asks = [*_levels(LIVE_ASKS), (Decimal("99999"), Decimal("1"))]
    assert kraken_checksum(deep_bids, deep_asks) == LIVE_EXPECTED


def test_empty_book_hashes_the_empty_payload() -> None:
    assert kraken_checksum([], []) == 0
