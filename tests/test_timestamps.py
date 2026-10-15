import pytest

from tickvault.timeutil import ns_to_iso, parse_rfc3339_ns, us_to_ns

COINBASE_NS = 1_792_034_331_909_462_959
KRAKEN_NS = 1_792_034_331_909_462_000
TARDIS_US = 1_792_034_331_909_462


def test_iso8601_and_rfc3339_parse_to_same_ns() -> None:
    assert parse_rfc3339_ns("2026-10-15T03:18:51.909462959Z") == COINBASE_NS
    assert parse_rfc3339_ns("2026-10-15T03:18:51.909462Z") == KRAKEN_NS
    assert parse_rfc3339_ns("2026-10-14T23:18:51.909462959-04:00") == COINBASE_NS
    assert parse_rfc3339_ns("2026-10-15T08:48:51.909462959+05:30") == COINBASE_NS
    assert us_to_ns(TARDIS_US) == KRAKEN_NS


def test_nine_fractional_digits_are_not_truncated() -> None:
    parsed = parse_rfc3339_ns("2026-10-15T03:18:51.909462959Z")
    assert parsed % 1_000_000_000 == 909_462_959
    assert parsed - parse_rfc3339_ns("2026-10-15T03:18:51.909462000Z") == 959
    assert parsed - parse_rfc3339_ns("2026-10-15T03:18:51.909462Z") == 959


def test_space_separator_no_fraction_and_lowercase_z() -> None:
    assert parse_rfc3339_ns("2026-10-15 03:18:51Z") == 1_792_034_331_000_000_000
    assert parse_rfc3339_ns("2026-10-15t03:18:51z") == 1_792_034_331_000_000_000


@pytest.mark.parametrize(
    "text",
    [
        "",
        "not a timestamp",
        "2026-10-15T03:18:51",
        "2026-13-01T00:00:00Z",
        "2026-10-32T00:00:00Z",
        "2026-10-15T03:18:51.1234567890Z",
        "2026-10-15T03:18:51.Z",
        "2026-10-15T03:18:51+0500",
    ],
)
def test_invalid_timestamps_raise(text: str) -> None:
    with pytest.raises(ValueError):
        parse_rfc3339_ns(text)


def test_ns_to_iso_formats_and_round_trips() -> None:
    assert ns_to_iso(KRAKEN_NS) == "2026-10-15T03:18:51.909462000Z"
    assert ns_to_iso(COINBASE_NS) == "2026-10-15T03:18:51.909462959Z"
    assert ns_to_iso(0) == "1970-01-01T00:00:00.000000000Z"
    for ts_ns in (0, 1, 1_000_000_000, KRAKEN_NS, COINBASE_NS):
        assert parse_rfc3339_ns(ns_to_iso(ts_ns)) == ts_ns


def test_us_to_ns_scales_by_a_thousand() -> None:
    assert us_to_ns(0) == 0
    assert us_to_ns(1) == 1000
    assert us_to_ns(TARDIS_US) == 1_792_034_331_909_462_000
