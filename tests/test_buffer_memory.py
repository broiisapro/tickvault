"""Memory buffer tests: order, offsets, commits, restarts, control (issue #113)."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest

from tickvault.book.processor import ResyncRequest
from tickvault.buffer import (
    BufferRecord,
    MemoryBroker,
    MemoryBuffer,
    decode_control,
    encode_control,
    open_buffer,
)


async def _take[T](iterator: AsyncIterator[T], count: int) -> list[T]:
    """Collect ``count`` items, failing the test instead of hanging forever."""
    return await asyncio.wait_for(_take_items(iterator, count), timeout=2.0)


async def _take_items[T](iterator: AsyncIterator[T], count: int) -> list[T]:
    items: list[T] = []
    async for item in iterator:
        items.append(item)
        if len(items) == count:
            break
    return items


@pytest.fixture
async def memory_buffer() -> AsyncIterator[MemoryBuffer]:
    opened = MemoryBuffer(MemoryBroker())
    await opened.start()
    try:
        yield opened
    finally:
        await opened.close()


async def test_publish_then_consume_keeps_order_and_offsets(
    memory_buffer: MemoryBuffer,
) -> None:
    for index in range(3):
        await memory_buffer.publish_raw(f"line-{index}".encode())

    records = await _take(memory_buffer.consume_raw(), 3)

    assert [record.offset for record in records] == [0, 1, 2]
    assert [record.value for record in records] == [b"line-0", b"line-1", b"line-2"]


async def test_consumer_receives_records_published_while_it_waits() -> None:
    broker = MemoryBroker()
    consumer = MemoryBuffer(broker)
    writer = MemoryBuffer(broker)
    await consumer.start()
    await writer.start()
    stream = consumer.consume_raw()
    pending = asyncio.ensure_future(_take(stream, 1))
    await asyncio.sleep(0)

    await writer.publish_raw(b"late")
    records = await pending

    assert [record.value for record in records] == [b"late"]
    await consumer.close()
    await writer.close()


async def test_new_buffer_resumes_after_the_committed_offset() -> None:
    broker = MemoryBroker()
    first = MemoryBuffer(broker)
    await first.start()
    for index in range(4):
        await first.publish_raw(f"line-{index}".encode())
    await _take(first.consume_raw(), 4)
    await first.commit(1)
    await first.close()

    restarted = MemoryBuffer(broker)
    await restarted.start()
    records = await _take(restarted.consume_raw(), 2)

    assert [(record.offset, record.value) for record in records] == [
        (2, b"line-2"),
        (3, b"line-3"),
    ]
    await restarted.close()


async def test_uncommitted_records_are_redelivered_after_a_crash() -> None:
    broker = MemoryBroker()
    crashed = MemoryBuffer(broker)
    await crashed.start()
    await crashed.publish_raw(b"a")
    await crashed.publish_raw(b"b")
    await crashed.close()

    restarted = MemoryBuffer(broker)
    await restarted.start()
    records = await _take(restarted.consume_raw(), 2)

    assert [record.offset for record in records] == [0, 1]
    await restarted.close()


async def test_commit_never_moves_backwards() -> None:
    broker = MemoryBroker()
    buffer = MemoryBuffer(broker)
    await buffer.start()
    for index in range(3):
        await buffer.publish_raw(bytes([index]))
    await buffer.commit(2)
    await buffer.commit(0)

    assert broker.committed == 2
    await buffer.close()


async def test_control_records_are_only_delivered_after_start() -> None:
    broker = MemoryBroker()
    writer = MemoryBuffer(broker)
    await writer.start()
    await writer.publish_control(b"before")

    reader = MemoryBuffer(broker)
    await reader.start()
    await writer.publish_control(b"after")
    payloads = await _take(reader.consume_control(), 1)

    assert payloads == [b"after"]
    await reader.close()
    await writer.close()


async def test_control_round_trip() -> None:
    request = ResyncRequest(
        venue="kraken",
        connection_id="kraken-20261101T000000Z-00000001",
        symbol="BTC-USD",
        reason="checksum_failure",
    )

    payload = encode_control(request)

    assert payload == (
        b'{"connection_id":"kraken-20261101T000000Z-00000001","reason":"checksum_failure",'
        b'"symbol":"BTC-USD","venue":"kraken"}'
    )
    assert decode_control(payload) == request


def test_decode_control_rejects_malformed_payloads() -> None:
    with pytest.raises(ValueError, match="invalid control payload"):
        decode_control(b"not json")
    with pytest.raises(ValueError, match="must be a JSON object"):
        decode_control(b'["venue"]')
    with pytest.raises(ValueError, match="field 'reason' must be a string"):
        decode_control(b'{"venue":"kraken","connection_id":"c","symbol":"BTC-USD"}')
    with pytest.raises(ValueError, match="field 'venue' must be a string"):
        decode_control(b'{"venue":1,"connection_id":"c","symbol":"BTC-USD","reason":"gap"}')


def test_open_buffer_memory_addresses_share_one_named_broker() -> None:
    first = open_buffer("memory://shared")
    second = open_buffer("memory://shared")
    other = open_buffer("memory://other")

    assert isinstance(first, MemoryBuffer)
    assert isinstance(second, MemoryBuffer)
    assert isinstance(other, MemoryBuffer)
    assert first.broker is second.broker
    assert other.broker is not first.broker


def test_open_buffer_kafka_address_reports_the_documented_error() -> None:
    with pytest.raises(RuntimeError, match="Kafka buffer not available yet"):
        open_buffer("127.0.0.1:9092")


@pytest.mark.parametrize("address", ["", "memory://", "broker", "broker:port", "http://x:1"])
def test_open_buffer_rejects_malformed_addresses(address: str) -> None:
    with pytest.raises(ValueError):
        open_buffer(address)


def test_buffer_record_is_frozen() -> None:
    record = BufferRecord(offset=0, value=b"x")
    with pytest.raises(AttributeError):
        record.offset = 1  # type: ignore[misc]
