"""Venue registry tests: one spec per venue, no import cycle (issue #113)."""

from __future__ import annotations

import subprocess
import sys

from tickvault.book.bitfinex_checksum import bitfinex_checksum
from tickvault.book.kraken_checksum import kraken_checksum
from tickvault.connectors.bitfinex import BitfinexConnector
from tickvault.connectors.coinbase import CoinbaseConnector
from tickvault.connectors.kraken import KrakenConnector
from tickvault.normalize import bitfinex as bitfinex_normalize
from tickvault.normalize import coinbase as coinbase_normalize
from tickvault.normalize import kraken as kraken_normalize
from tickvault.pipeline import _NORMALIZERS, default_connector_factories
from tickvault.venues import VENUES, VenueSpec, checksum_registry, venue_names


def test_registry_holds_exactly_coinbase_kraken_then_bitfinex() -> None:
    assert venue_names() == ("coinbase", "kraken", "bitfinex")
    assert list(VENUES) == ["coinbase", "kraken", "bitfinex"]
    assert all(isinstance(spec, VenueSpec) for spec in VENUES.values())


def test_each_spec_carries_name_normalizer_and_connector() -> None:
    coinbase = VENUES["coinbase"]
    assert coinbase.name == "coinbase"
    assert coinbase.normalize is coinbase_normalize.normalize
    assert coinbase.connector is CoinbaseConnector
    assert coinbase.checksum is None
    assert coinbase.checksum_depth is None

    kraken = VENUES["kraken"]
    assert kraken.name == "kraken"
    assert kraken.normalize is kraken_normalize.normalize
    assert kraken.connector is KrakenConnector

    bitfinex = VENUES["bitfinex"]
    assert bitfinex.name == "bitfinex"
    assert bitfinex.normalize is bitfinex_normalize.normalize
    assert bitfinex.connector is BitfinexConnector


def test_checksum_registry_lists_kraken_and_bitfinex_at_depth_25() -> None:
    assert checksum_registry() == {
        "kraken": (kraken_checksum, 25),
        "bitfinex": (bitfinex_checksum, 25),
    }


def test_checksum_registry_returns_a_fresh_copy() -> None:
    first = checksum_registry()
    first["kraken"] = (kraken_checksum, 10)
    first["coinbase"] = (kraken_checksum, 1)
    assert checksum_registry() == {
        "kraken": (kraken_checksum, 25),
        "bitfinex": (bitfinex_checksum, 25),
    }


def test_pipeline_derives_its_maps_from_the_registry() -> None:
    assert tuple(_NORMALIZERS) == venue_names()
    assert _NORMALIZERS["coinbase"] is coinbase_normalize.normalize
    assert _NORMALIZERS["kraken"] is kraken_normalize.normalize
    assert _NORMALIZERS["bitfinex"] is bitfinex_normalize.normalize
    assert default_connector_factories() == {
        "coinbase": CoinbaseConnector,
        "kraken": KrakenConnector,
        "bitfinex": BitfinexConnector,
    }


def test_cli_venue_choices_come_from_the_registry() -> None:
    from tickvault.cli import _SUPPORTED_VENUES

    assert venue_names() == _SUPPORTED_VENUES


def test_registry_modules_import_without_a_cycle() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import tickvault.venues, tickvault.pipeline, tickvault.cli",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
