"""Nanosecond timestamp parsing and formatting.

One parser for every venue: :func:`parse_rfc3339_ns` accepts RFC 3339 text
with 0-9 fractional digits, ``T`` or a space as the date/time separator and
``Z`` or ``±HH:MM`` as the zone. The fractional part is parsed as digits and
right-padded to nanoseconds because ``datetime.fromisoformat`` keeps only
microseconds and would truncate Coinbase timestamps (9 digits).
"""

from __future__ import annotations

import re
from datetime import UTC, datetime

_EPOCH = datetime(1970, 1, 1)
_NS_PER_SECOND = 1_000_000_000

_RFC3339_RE = re.compile(
    r"^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})"
    r"[Tt ]"
    r"(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})"
    r"(?:\.(?P<fraction>\d{1,9}))?"
    r"(?P<zone>Z|z|[+-]\d{2}:\d{2})$"
)


def parse_rfc3339_ns(text: str) -> int:
    """Parse RFC 3339 ``text`` into nanoseconds since the Unix epoch, UTC.

    Raises :class:`ValueError` when ``text`` is not a valid RFC 3339 timestamp.
    """
    match = _RFC3339_RE.match(text)
    if match is None:
        raise ValueError(f"invalid RFC 3339 timestamp: {text!r}")
    fraction = match.group("fraction") or ""
    nanosecond = int(fraction.ljust(9, "0")) if fraction else 0
    zone = match.group("zone")
    offset_seconds = 0
    if zone not in ("Z", "z"):
        sign = 1 if zone[0] == "+" else -1
        offset_seconds = sign * (int(zone[1:3]) * 3600 + int(zone[4:6]) * 60)
    try:
        moment = datetime(
            int(match.group("year")),
            int(match.group("month")),
            int(match.group("day")),
            int(match.group("hour")),
            int(match.group("minute")),
            int(match.group("second")),
        )
    except ValueError as exc:
        raise ValueError(f"invalid RFC 3339 timestamp: {text!r}") from exc
    delta = moment - _EPOCH
    seconds = delta.days * 86400 + delta.seconds
    return (seconds - offset_seconds) * _NS_PER_SECOND + nanosecond


def us_to_ns(microseconds: int) -> int:
    """Convert microseconds since the Unix epoch to nanoseconds."""
    return microseconds * 1000


def ns_to_iso(ts_ns: int) -> str:
    """Format ``ts_ns`` as RFC 3339 UTC text with nine fractional digits."""
    seconds, nanosecond = divmod(ts_ns, _NS_PER_SECOND)
    moment = datetime.fromtimestamp(seconds, tz=UTC)
    return f"{moment:%Y-%m-%dT%H:%M:%S}.{nanosecond:09d}Z"
