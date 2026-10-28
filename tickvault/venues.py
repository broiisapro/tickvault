"""Venue registry: one :class:`VenueSpec` per supported venue.

Everything the pipeline needs to know about a venue lives in one place: its
normalizer, its production connector, and (for venues whose feed carries a
book checksum) the checksum function and the book depth the checksum is
computed at.

Adding a venue is one new :class:`VenueSpec` entry in :data:`VENUES`; nothing
else in the shared files changes. ``tickvault.pipeline`` derives its
normalizer map and its connector factories from :data:`VENUES`, ``tickvault.cli``
derives its ``--venues`` choices from :func:`venue_names`, and
:func:`checksum_registry` feeds the book processor's default checksum map.

Import direction: this module imports the normalizers, the connectors and the
:class:`~tickvault.book.processor.ChecksumFn` type only. The processor imports
this module lazily inside ``BookProcessor.__init__`` when no checksum map is
given, so ``venues`` ↔ ``processor`` has no import-time cycle.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass

from tickvault.book.bitfinex_checksum import bitfinex_checksum
from tickvault.book.kraken_checksum import kraken_checksum
from tickvault.book.processor import ChecksumFn
from tickvault.connectors.base import Connector
from tickvault.connectors.bitfinex import BitfinexConnector
from tickvault.connectors.coinbase import CoinbaseConnector
from tickvault.connectors.kraken import KrakenConnector
from tickvault.models import NormalizedMessage, RawMessage
from tickvault.normalize import bitfinex as bitfinex_normalize
from tickvault.normalize import coinbase as coinbase_normalize
from tickvault.normalize import kraken as kraken_normalize

type ConnectorFactory = Callable[[Sequence[str]], Connector]
"""Builds one connector for a venue and a fixed symbol set."""


@dataclass(frozen=True, slots=True)
class VenueSpec:
    """Everything the pipeline needs to know about one venue."""

    name: str
    """Venue name used in paths, rows and ``connect`` control payloads."""

    normalize: Callable[[RawMessage], NormalizedMessage]
    """Pure function turning one raw envelope into a normalized message."""

    connector: ConnectorFactory
    """Production connector factory for live ingestion."""

    checksum: ChecksumFn | None = None
    """Book checksum function, or ``None`` when the venue's feed carries none."""

    checksum_depth: int | None = None
    """Book depth the checksum is computed at; required with ``checksum``."""


VENUES: Mapping[str, VenueSpec] = {
    "coinbase": VenueSpec(
        name="coinbase",
        normalize=coinbase_normalize.normalize,
        connector=CoinbaseConnector,
    ),
    "kraken": VenueSpec(
        name="kraken",
        normalize=kraken_normalize.normalize,
        connector=KrakenConnector,
        checksum=kraken_checksum,
        checksum_depth=25,
    ),
    "bitfinex": VenueSpec(
        name="bitfinex",
        normalize=bitfinex_normalize.normalize,
        connector=BitfinexConnector,
        checksum=bitfinex_checksum,
        checksum_depth=25,
    ),
}
"""Every supported venue, in display order (Coinbase, Kraken, then Bitfinex)."""


def venue_names() -> tuple[str, ...]:
    """Return the supported venue names in registry order."""
    return tuple(VENUES)


def checksum_registry() -> dict[str, tuple[ChecksumFn, int]]:
    """Return ``{venue: (checksum, depth)}`` for every checksum-verified venue.

    A fresh dictionary is returned on every call, so callers may adjust their
    copy (the processor overrides Kraken's depth from its legacy constructor
    argument) without affecting the registry.
    """
    return {
        name: (spec.checksum, spec.checksum_depth)
        for name, spec in VENUES.items()
        if spec.checksum is not None and spec.checksum_depth is not None
    }
