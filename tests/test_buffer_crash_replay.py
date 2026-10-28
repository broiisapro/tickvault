"""Crash-replay equality: a writer restart must not change the normalized tree.

Decision A70 promises that "replay of a tree written across writer restarts
equals replay of the same messages written once". This is the test of that
promise, with the three pieces it needs together: the buffered writer's
at-least-once commits (#122), ``rawio``'s exact-duplicate skipping (#124) and
the raw writer's resume (#141).

A writer crashes on its third commit: the records it wrote since the second one
are on disk but uncommitted, so the restarted writer is handed them again and
stores a second copy. Root A therefore holds duplicates in its raw layer; root
B is a fresh, uninterrupted writer over a copy of the same broker contents.
Both raw trees must replay to byte-identical ``norm/`` trees.

The session is the first ``_FRAMES_PER_VENUE`` frames of each recorded fixture
rather than all of it: real venue frames, but short enough that three writer
passes and two full replays stay inexpensive. The commit cadence itself is not
changed, so the crash point is the production one.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import zstandard as zstd
from test_buffer_restart import _Fixture, _gauge, _total, _wait_until

import tickvault.pipeline as pipeline_module
from tickvault.buffer import Buffer, BufferRecord, MemoryBroker, MemoryBuffer
from tickvault.metrics import PipelineMetrics
from tickvault.pipeline import run_replay, run_writer
from tickvault.rawio import RawReadStats, encode_line, iter_raw

_SYMBOL = "BTC-USD"
_FAIL_ON_COMMIT = 3
"""The writer crashes while committing for the third time."""

_FRAMES_PER_VENUE = _FAIL_ON_COMMIT * pipeline_module._WRITER_COMMIT_RECORDS
"""Frames kept per venue: enough for the writer to reach its third commit."""


class _CommitCrashBuffer:
    """Delegates to a buffer but crashes the writer on its Nth commit.

    A real crash needs a moment when the writer has written records it has not
    committed yet, and finding that moment by waiting would be racy: a writer
    that has caught up sits exactly on a commit boundary. Failing the
    ``fail_on_commit``-th commit instead fixes the crash point: every earlier
    commit is durable, and the ``_WRITER_COMMIT_RECORDS`` records since the last
    one are on disk but uncommitted, ready to be redelivered.
    """

    def __init__(self, inner: Buffer, *, fail_on_commit: int) -> None:
        self.inner = inner
        self.fail_on_commit = fail_on_commit
        self.commits = 0

    async def start(self) -> None:
        await self.inner.start()

    async def publish_raw(self, line: bytes) -> None:
        await self.inner.publish_raw(line)

    async def publish_control(self, payload: bytes) -> None:
        await self.inner.publish_control(payload)

    def consume_raw(self) -> AsyncIterator[BufferRecord]:
        return self.inner.consume_raw()

    def consume_control(self) -> AsyncIterator[bytes]:
        return self.inner.consume_control()

    async def commit(self, offset: int) -> None:
        self.commits += 1
        if self.commits >= self.fail_on_commit:
            raise RuntimeError("simulated writer crash")
        await self.inner.commit(offset)

    async def end_offset(self) -> int | None:
        return await self.inner.end_offset()

    async def close(self) -> None:
        await self.inner.close()


def _write_prefix(source: Path, target: Path, frames: int) -> Path:
    """Write the first ``frames`` raw envelopes of ``source`` to ``target``."""
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("wb") as handle, zstd.ZstdCompressor().stream_writer(handle) as writer:
        for index, message in enumerate(iter_raw(source)):
            if index == frames:
                break
            writer.write(encode_line(message))
    return target


def _tree_digest(root: Path) -> dict[str, bytes]:
    """Every file under ``root`` keyed by its relative path, in sorted order."""
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


async def _run_copy_of_broker(
    broker: MemoryBroker, out: Path, *, records: int
) -> None:
    """Write a fresh broker copy with a new, uninterrupted writer into ``out``."""
    copy = MemoryBroker()
    copy.raw = list(broker.raw)
    metrics = PipelineMetrics()
    stop_event = asyncio.Event()
    task = asyncio.create_task(
        run_writer(MemoryBuffer(copy), out, metrics=metrics, stop_event=stop_event)
    )
    try:
        await _wait_until(lambda: _total(metrics, "tickvault_buffer_consumed_total") >= records)
        stop_event.set()
        stats = await asyncio.wait_for(task, timeout=30)
    finally:
        stop_event.set()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    assert stats.mode == "writer"
    assert stats.stopped_by == "stop_event"


async def test_replay_after_writer_crash_equals_single_writer_replay(
    tmp_path: Path, coinbase_5m_path: Path, kraken_5m_path: Path
) -> None:
    paths = (
        _write_prefix(coinbase_5m_path, tmp_path / "prefix-coinbase.jsonl.zst", _FRAMES_PER_VENUE),
        _write_prefix(kraken_5m_path, tmp_path / "prefix-kraken.jsonl.zst", _FRAMES_PER_VENUE),
    )
    fixture = _Fixture(tmp_path, paths)
    root_a = fixture.out
    fixture.start_ingester()

    crashed_metrics = PipelineMetrics()
    crashed = asyncio.create_task(
        run_writer(
            _CommitCrashBuffer(MemoryBuffer(fixture.broker), fail_on_commit=_FAIL_ON_COMMIT),
            root_a,
            metrics=crashed_metrics,
        )
    )
    with pytest.raises(RuntimeError, match="simulated writer crash"):
        await asyncio.wait_for(crashed, timeout=60)

    # Both books were LIVE before the crash, so the raw layer it left is a real
    # session and not a partial first frame.
    for venue in fixture.connectors:
        assert _gauge(crashed_metrics, "tickvault_book_live", venue=venue, symbol=_SYMBOL) == 1.0

    # Exactly two commits survived; the records of the third batch did not.
    committed_before_restart = fixture.broker.committed
    committed_per_batch = pipeline_module._WRITER_COMMIT_RECORDS
    assert committed_before_restart == (_FAIL_ON_COMMIT - 1) * committed_per_batch - 1

    # A 3000-frame session is published in full before the connector holds, so
    # every record is already on the broker when the writer crashes; releasing
    # states that explicitly even though there is nothing left to release.
    fixture.release()
    await _wait_until(lambda: len(fixture.broker.raw) >= fixture.fixture_records)

    restarted_metrics = PipelineMetrics()
    stop_event = asyncio.Event()
    restarted = asyncio.create_task(
        run_writer(
            MemoryBuffer(fixture.broker),
            root_a,
            metrics=restarted_metrics,
            stop_event=stop_event,
        )
    )
    try:
        await _wait_until(fixture.served_all)
        await fixture.stop_ingester()
        published = len(fixture.broker.raw)
        await _wait_until(
            lambda: _total(restarted_metrics, "tickvault_buffer_consumed_total")
            >= published - committed_before_restart - 1
        )
        stop_event.set()
        restarted_stats = await asyncio.wait_for(restarted, timeout=30)
    finally:
        stop_event.set()
        if not restarted.done():
            restarted.cancel()
            await asyncio.gather(restarted, return_exceptions=True)

    assert restarted_stats.stopped_by == "stop_event"

    root_b = tmp_path / "single-writer"
    await _run_copy_of_broker(fixture.broker, root_b, records=published)

    read_stats = RawReadStats()
    from_a = list(iter_raw(root_a, stats=read_stats))
    assert read_stats.duplicates == committed_per_batch
    assert read_stats.conflicts == 0
    from_b = list(iter_raw(root_b))
    assert from_a == from_b

    replay_a = run_replay(root_a, tmp_path / "replay-a")
    replay_b = run_replay(root_b, tmp_path / "replay-b")
    assert replay_a.to_dict() == replay_b.to_dict()

    digest_a = _tree_digest(tmp_path / "replay-a" / "norm")
    assert digest_a
    assert digest_a == _tree_digest(tmp_path / "replay-b" / "norm")
