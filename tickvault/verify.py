"""Remote manifest verification for published data samples (Epic 8).

``tickvault verify --manifest <path|url>`` downloads a manifest (a path or an
``http(s)`` URL), compares it with the files it describes and reports every
mismatch. The implementation lands in wave 08; this module freezes the
interface.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class VerifyReport:
    """Outcome of verifying one manifest against its files."""

    manifest: str
    """The manifest path or URL that was verified."""

    files: int = 0
    """How many files the manifest lists."""

    ok: int = 0
    """How many of them matched their recorded size and checksum."""

    problems: list[str] = field(default_factory=list)
    """One human-readable line per mismatch; empty means everything matched."""


def verify_remote(manifest: str, *, timeout_s: float = 30.0) -> VerifyReport:
    """Verify the manifest at ``manifest`` (a path or an http(s) URL).

    Downloads the manifest with ``timeout_s`` seconds per request, checks every
    entry it lists and returns a :class:`VerifyReport`. Network access happens
    only here, and only for an http(s) manifest.
    """
    raise NotImplementedError("verify: implemented in wave 08")
