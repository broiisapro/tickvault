"""Storage sinks for normalized records."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from tickvault.models import NormalizedRecord


class Sink(Protocol):
    """Destination for normalized table rows.

    ``write`` appends records for one table; ``flush`` persists buffered rows
    without closing; ``close`` flushes and finalizes every open file.
    """

    def write(self, table: str, records: Sequence[NormalizedRecord]) -> None:
        """Append ``records`` to ``table`` (a key of ``tickvault.models.TABLES``)."""
        ...

    def flush(self) -> None:
        """Persist buffered rows to their destinations."""
        ...

    def close(self) -> None:
        """Flush and close all open destinations."""
        ...
