"""Tests for the shared websocket connector helpers."""

from __future__ import annotations

import asyncio
import random

import pytest

from tickvault.connectors._ws import (
    Backoff,
    RawEmitter,
    control_payload,
    make_connection_id,
    utc_stamp,
    wait_first,
)


def test_utc_stamp() -> None:
    assert utc_stamp(1_793_491_200_123_456_789) == "20261101T000000Z"


def test_make_connection_id() -> None:
    connection_id = make_connection_id(
        "coinbase", 1_793_491_200_000_000_000, token_hex=lambda _: "3fa2c1d0"
    )
    assert connection_id == "coinbase-20261101T000000Z-3fa2c1d0"


def test_control_payload_is_compact_json_with_event_first() -> None:
    payload = control_payload("resync_request", symbol="BTC-USD", reason="gap")
    assert payload == '{"event":"resync_request","symbol":"BTC-USD","reason":"gap"}'


def test_raw_emitter_ingest_seq_is_contiguous_over_all_lines() -> None:
    ticks = iter([10, 20, 30, 40])
    emitter = RawEmitter(
        "coinbase", "coinbase-20261101T000000Z-00000001", clock=lambda: next(ticks)
    )
    messages = [
        emitter.control('{"event":"connect"}'),
        emitter.message('{"channel":"l2_data"}'),
        emitter.message('{"channel":"heartbeats"}'),
        emitter.control('{"event":"disconnect","reason":"closed"}'),
    ]
    assert [message.ingest_seq for message in messages] == [0, 1, 2, 3]
    assert [message.recv_ts_ns for message in messages] == [10, 20, 30, 40]
    assert [message.kind for message in messages] == ["control", "msg", "msg", "control"]
    assert [message.venue for message in messages] == ["coinbase"] * 4
    assert [message.connection_id for message in messages] == [
        "coinbase-20261101T000000Z-00000001"
    ] * 4


def test_backoff_schedule_is_one_two_four_capped_at_thirty() -> None:
    backoff = Backoff(jitter=0.0)
    assert [backoff.next_delay() for _ in range(8)] == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0, 30.0]


def test_backoff_jitter_only_shortens_the_schedule() -> None:
    backoff = Backoff(jitter=0.5, rng=random.Random(0))
    schedule = [1.0, 2.0, 4.0, 8.0]
    delays = [backoff.next_delay() for _ in schedule]
    assert [delay <= expected for delay, expected in zip(delays, schedule, strict=True)] == [
        True
    ] * len(schedule)
    assert [delay > expected * 0.5 for delay, expected in zip(delays, schedule, strict=True)] == [
        True
    ] * len(schedule)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"base": 0.0},
        {"base": -1.0},
        {"base": 2.0, "cap": 1.0},
        {"jitter": 1.0},
        {"jitter": -0.1},
    ],
)
def test_backoff_rejects_invalid_parameters(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError):
        Backoff(**kwargs)


def _pending_tasks() -> list[asyncio.Task[object]]:
    current = asyncio.current_task()
    return [task for task in asyncio.all_tasks() if task is not current and not task.done()]


async def test_wait_first_cancels_and_awaits_the_pending_task() -> None:
    cancelled = asyncio.Event()

    async def slow() -> str:
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            cancelled.set()
            raise
        return "slow"

    quick = asyncio.create_task(asyncio.sleep(0, result="quick"))
    slow_task = asyncio.create_task(slow())

    done = await wait_first((quick, slow_task))

    assert done == {quick}
    assert quick.result() == "quick"
    assert cancelled.is_set()
    assert slow_task.cancelled()
    assert _pending_tasks() == []


async def test_wait_first_propagates_cancellation_and_cleans_up_every_task() -> None:
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def waiter() -> None:
        started.set()
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    async def helper() -> None:
        await wait_first((asyncio.create_task(waiter()), asyncio.create_task(waiter())))

    task = asyncio.create_task(helper())
    await started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert cancelled.is_set()
    assert task.cancelled()
    assert _pending_tasks() == []
