"""Replay benchmark behind ``tickvault bench``.

The benchmark counts the raw messages per venue, then times a full
:func:`tickvault.pipeline.run_replay` of the inputs into a temporary directory
(so the measurement is pure replay work, no counting). Peak RSS comes from
``resource.getrusage(RUSAGE_SELF).ru_maxrss``, which is bytes on macOS and KiB
on Linux; :func:`run_benchmark` normalizes it to MB. The result is a
measurement, never a failure: ``passed`` only records whether the PRD targets
(>= 2,000 messages/s per venue, RSS < 400 MB) were met.
"""

from __future__ import annotations

import resource
import sys
import tempfile
import time
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from tickvault.pipeline import run_replay
from tickvault.rawio import iter_raw

TARGET_MESSAGES_PER_SECOND = 2_000.0
"""PRD target: at least this many raw messages per second per venue."""

TARGET_MAX_RSS_MB = 400.0
"""PRD target: peak resident set below this many MB."""

_FIXTURES_DIR = Path(__file__).resolve().parent.parent / "tests" / "fixtures"

DEFAULT_INPUTS: tuple[Path, ...] = (
    _FIXTURES_DIR / "coinbase_btc_5m.jsonl.zst",
    _FIXTURES_DIR / "kraken_btc_5m.jsonl.zst",
)
"""The two recorded 5-minute fixtures, the default ``bench`` input (A26)."""


@dataclass(frozen=True, slots=True)
class VenueThroughput:
    """Messages and messages/s for one venue."""

    venue: str
    messages: int
    messages_per_second: float


@dataclass(frozen=True, slots=True)
class BenchmarkReport:
    """One benchmark run: inputs, timings, RSS and the pass/fail verdict."""

    inputs: tuple[str, ...]
    messages: int
    seconds: float
    messages_per_second: float
    venues: tuple[VenueThroughput, ...]
    max_rss_mb: float
    target_messages_per_second: float
    target_max_rss_mb: float
    passed: bool

    def to_dict(self) -> dict[str, object]:
        """Render the report as a stable, JSON-serializable dictionary."""
        return {
            "inputs": list(self.inputs),
            "messages": self.messages,
            "seconds": self.seconds,
            "messages_per_second": self.messages_per_second,
            "venues": [
                {
                    "venue": venue.venue,
                    "messages": venue.messages,
                    "messages_per_second": venue.messages_per_second,
                }
                for venue in self.venues
            ],
            "max_rss_mb": self.max_rss_mb,
            "target_messages_per_second": self.target_messages_per_second,
            "target_max_rss_mb": self.target_max_rss_mb,
            "passed": self.passed,
        }

    def to_text(self) -> str:
        """Render the report as the one-screen human summary."""
        per_venue = ", ".join(
            f"{venue.venue} {venue.messages_per_second:,.0f}" for venue in self.venues
        )
        verdict = "PASS" if self.passed else "FAIL"
        lines = [
            f"inputs: {len(self.inputs)} file(s)",
            f"messages: {self.messages:,} ({_message_breakdown(self.venues)})",
            f"wall time: {self.seconds:.3f} s",
            f"throughput: {self.messages_per_second:,.0f} msg/s overall ({per_venue})",
            f"peak RSS: {self.max_rss_mb:.1f} MB",
            (
                f"targets: >= {self.target_messages_per_second:,.0f} msg/s per venue and "
                f"< {self.target_max_rss_mb:,.0f} MB RSS: {verdict}"
            ),
        ]
        return "\n".join(lines) + "\n"


def _message_breakdown(venues: Sequence[VenueThroughput]) -> str:
    return ", ".join(f"{venue.venue} {venue.messages:,}" for venue in venues)


def default_inputs() -> tuple[Path, ...]:
    """Return the default benchmark inputs (the recorded 5-minute fixtures)."""
    return DEFAULT_INPUTS


def max_rss_mb() -> float:
    """Peak resident set size of this process in MB.

    ``ru_maxrss`` is bytes on macOS and KiB on Linux.
    """
    raw = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return raw / (1024 * 1024)
    return raw / 1024


def run_benchmark(
    inputs: Sequence[str | Path] | None = None,
    *,
    target_messages_per_second: float = TARGET_MESSAGES_PER_SECOND,
    target_max_rss_mb: float = TARGET_MAX_RSS_MB,
) -> BenchmarkReport:
    """Replay ``inputs`` (default: the recorded fixtures) and report throughput.

    Each input is a raw file or raw tree. Raises :class:`FileNotFoundError`
    for a missing input and :class:`ValueError` for an empty input list.
    """
    paths = tuple(Path(path) for path in (default_inputs() if inputs is None else inputs))
    if not paths:
        raise ValueError("bench needs at least one input")
    for path in paths:
        if not path.exists():
            raise FileNotFoundError(path)

    counts: Counter[str] = Counter()
    for path in paths:
        for message in iter_raw(path):
            counts[message.venue] += 1
    messages = sum(counts.values())

    with tempfile.TemporaryDirectory(prefix="tickvault-bench-") as temporary:
        out = Path(temporary) / "data"
        start = time.perf_counter()
        for path in paths:
            run_replay(path, out)
        seconds = time.perf_counter() - start

    rss_mb = max_rss_mb()
    venues = tuple(
        VenueThroughput(
            venue=venue,
            messages=count,
            messages_per_second=count / seconds if seconds > 0 else float("inf"),
        )
        for venue, count in sorted(counts.items())
    )
    passed = rss_mb < target_max_rss_mb and all(
        venue.messages_per_second >= target_messages_per_second for venue in venues
    )
    return BenchmarkReport(
        inputs=tuple(str(path) for path in paths),
        messages=messages,
        seconds=seconds,
        messages_per_second=messages / seconds if seconds > 0 else float("inf"),
        venues=venues,
        max_rss_mb=rss_mb,
        target_messages_per_second=target_messages_per_second,
        target_max_rss_mb=target_max_rss_mb,
        passed=passed,
    )
