"""Kafka buffer tests: protocol behaviour without a broker, plus an opt-in round trip.

The unit tests replace the three aiokafka client classes with fakes, so they
run everywhere (Docker is not in CI, decision A34). The broker test is marked
``network`` and only runs when ``TICKVAULT_TEST_KAFKA=host:port`` points at a
throwaway broker (issue #123).
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, ClassVar

import pytest
from aiokafka.errors import ConsumerStoppedError, KafkaError
from aiokafka.structs import OffsetAndMetadata, TopicPartition

from tickvault import buffer_kafka
from tickvault.buffer import CONTROL_TOPIC, RAW_TOPIC
from tickvault.buffer_kafka import KafkaBuffer, OffsetGapError

RAW_PARTITION = TopicPartition(RAW_TOPIC, 0)
CONTROL_PARTITION = TopicPartition(CONTROL_TOPIC, 0)


@dataclass(frozen=True, slots=True)
class FakeRecord:
    """The fields of an ``aiokafka`` consumer record this module reads."""

    offset: int
    value: bytes | None


class FakeAdmin:
    """Stand-in for ``AIOKafkaAdminClient``; records topic creation."""

    instances: ClassVar[list[FakeAdmin]] = []
    error_code: ClassVar[int] = 0

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.created: list[Any] = []
        self.timeout_ms: int | None = None
        self.started = False
        self.stopped = False
        FakeAdmin.instances.append(self)

    async def start(self) -> None:
        self.started = True

    async def create_topics(self, new_topics: list[Any], timeout_ms: int | None = None) -> Any:
        self.created.extend(new_topics)
        self.timeout_ms = timeout_ms
        return SimpleNamespace(
            topic_errors=[(topic.name, FakeAdmin.error_code) for topic in new_topics]
        )

    async def stop(self) -> None:
        self.stopped = True


class FakeProducer:
    """Stand-in for ``AIOKafkaProducer``; records every send."""

    instances: ClassVar[list[FakeProducer]] = []

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.sent: list[tuple[str, bytes, int | None]] = []
        self.started = False
        self.stopped = False
        FakeProducer.instances.append(self)

    async def start(self) -> None:
        self.started = True

    async def send(self, topic: str, value: bytes, partition: int | None = None) -> None:
        self.sent.append((topic, value, partition))

    async def stop(self) -> None:
        self.stopped = True


class FakeConsumer:
    """Stand-in for ``AIOKafkaConsumer`` driven by :meth:`push`.

    ``getone`` blocks until a record is pushed or :meth:`stop` is called, which
    is exactly the behaviour the iterator shutdown tests need.
    """

    instances: ClassVar[list[FakeConsumer]] = []

    def __init__(self, *topics: str, **kwargs: Any) -> None:
        self.topics = topics
        self.kwargs = kwargs
        self.messages: list[FakeRecord] = []
        self.assigned: set[TopicPartition] = set()
        self.seeked: list[TopicPartition] = []
        self.commits: list[dict[TopicPartition, OffsetAndMetadata]] = []
        self.started = False
        self.stopped = False
        self._waiters: list[asyncio.Event] = []
        FakeConsumer.instances.append(self)

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True
        self._notify()

    def assign(self, partitions: list[TopicPartition]) -> None:
        self.assigned.update(partitions)

    async def seek_to_end(self, *partitions: TopicPartition) -> None:
        self.seeked.extend(partitions)

    async def commit(self, offsets: dict[TopicPartition, OffsetAndMetadata]) -> None:
        self.commits.append(dict(offsets))

    async def getone(self) -> FakeRecord:
        while True:
            if self.messages:
                return self.messages.pop(0)
            if self.stopped:
                raise ConsumerStoppedError()
            waiter = asyncio.Event()
            self._waiters.append(waiter)
            try:
                await waiter.wait()
            finally:
                if waiter in self._waiters:
                    self._waiters.remove(waiter)

    def push(self, offset: int, value: bytes | None) -> None:
        self.messages.append(FakeRecord(offset=offset, value=value))
        self._notify()

    def _notify(self) -> None:
        waiters, self._waiters = self._waiters, []
        for waiter in waiters:
            waiter.set()


def _raw_consumer() -> FakeConsumer:
    """The fake consumer built with a group id (the raw topic consumer)."""
    return next(consumer for consumer in FakeConsumer.instances if "group_id" in consumer.kwargs)


def _control_consumer() -> FakeConsumer:
    """The fake consumer built without a group id (the control topic consumer)."""
    return next(
        consumer for consumer in FakeConsumer.instances if "group_id" not in consumer.kwargs
    )


@pytest.fixture
def fake_clients(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace the aiokafka clients with in-process fakes for one test."""
    FakeAdmin.instances.clear()
    FakeAdmin.error_code = 0
    FakeProducer.instances.clear()
    FakeConsumer.instances.clear()
    monkeypatch.setattr(buffer_kafka, "AIOKafkaAdminClient", FakeAdmin)
    monkeypatch.setattr(buffer_kafka, "AIOKafkaProducer", FakeProducer)
    monkeypatch.setattr(buffer_kafka, "AIOKafkaConsumer", FakeConsumer)


async def _take[T](iterator: AsyncIterator[T], count: int, *, timeout: float = 2.0) -> list[T]:
    """Collect ``count`` items, failing the test instead of hanging forever."""
    return await asyncio.wait_for(_take_items(iterator, count), timeout=timeout)


async def _take_items[T](iterator: AsyncIterator[T], count: int) -> list[T]:
    items: list[T] = []
    async for item in iterator:
        items.append(item)
        if len(items) == count:
            break
    return items


@pytest.mark.parametrize(
    "address",
    ["", "broker", "broker:port", ":9092", "host:0", "host:65536", "http://127.0.0.1:9092"],
)
def test_address_must_be_host_and_port(address: str) -> None:
    with pytest.raises(ValueError):
        KafkaBuffer(address)


def test_address_is_normalised_for_bootstrap_servers() -> None:
    buffer = KafkaBuffer("127.0.0.1:019092")

    assert buffer.address == "127.0.0.1:019092"
    assert buffer.bootstrap_servers == "127.0.0.1:19092"
    assert buffer.group_id == "tickvault-writer"
    assert buffer.client_id == "tickvault"


def test_consume_raw_before_start_raises() -> None:
    with pytest.raises(RuntimeError, match="not started"):
        KafkaBuffer("127.0.0.1:19092").consume_raw()


async def test_close_before_start_is_safe() -> None:
    await KafkaBuffer("127.0.0.1:19092").close()


async def test_start_creates_one_partition_topics_and_starts_clients(
    fake_clients: None,
) -> None:
    buffer = KafkaBuffer("127.0.0.1:19092", group_id="tickvault-test", client_id="tv")
    await buffer.start()

    admin = FakeAdmin.instances[0]
    assert admin.started
    created = [
        (topic.name, topic.num_partitions, topic.replication_factor) for topic in admin.created
    ]
    assert created == [(RAW_TOPIC, 1, 1), (CONTROL_TOPIC, 1, 1)]

    producer = FakeProducer.instances[0]
    assert producer.started
    assert producer.kwargs["client_id"] == "tv"
    assert producer.kwargs["acks"] == "all"
    assert producer.kwargs["enable_idempotence"] is True
    assert 0 <= producer.kwargs["linger_ms"] <= 20

    raw = _raw_consumer()
    assert raw.started
    assert raw.topics == (RAW_TOPIC,)
    assert raw.kwargs["client_id"] == "tv-raw"
    assert raw.kwargs["group_id"] == "tickvault-test"
    assert raw.kwargs["enable_auto_commit"] is False
    assert raw.kwargs["auto_offset_reset"] == "earliest"

    control = _control_consumer()
    assert control.started
    assert control.topics == ()
    assert control.kwargs["client_id"] == "tv-control"
    assert control.assigned == {CONTROL_PARTITION}
    assert control.seeked == [CONTROL_PARTITION]

    await buffer.close()

    assert admin.stopped
    assert producer.stopped
    assert raw.stopped
    assert control.stopped


async def test_start_is_idempotent(fake_clients: None) -> None:
    buffer = KafkaBuffer("127.0.0.1:19092")
    await buffer.start()
    await buffer.start()

    assert len(FakeAdmin.instances) == 1
    assert len(FakeProducer.instances) == 1
    assert len(FakeConsumer.instances) == 2
    await buffer.close()


async def test_existing_topics_are_not_an_error(fake_clients: None) -> None:
    FakeAdmin.error_code = 36
    buffer = KafkaBuffer("127.0.0.1:19092")

    await buffer.start()

    assert FakeProducer.instances[0].started
    await buffer.close()


async def test_topic_creation_failure_is_reported_and_clients_stopped(
    fake_clients: None,
) -> None:
    FakeAdmin.error_code = 37
    buffer = KafkaBuffer("127.0.0.1:19092")

    with pytest.raises(KafkaError, match="creating topic"):
        await buffer.start()

    assert FakeAdmin.instances[0].stopped
    assert FakeProducer.instances == []
    assert FakeConsumer.instances == []


async def test_publish_sends_exact_bytes_to_partition_zero(fake_clients: None) -> None:
    buffer = KafkaBuffer("127.0.0.1:19092")
    await buffer.start()

    await buffer.publish_raw(b'{"v":1,"venue":"coinbase"}')
    await buffer.publish_control(b'{"venue":"kraken","reason":"gap"}')

    assert FakeProducer.instances[0].sent == [
        (RAW_TOPIC, b'{"v":1,"venue":"coinbase"}', 0),
        (CONTROL_TOPIC, b'{"venue":"kraken","reason":"gap"}', 0),
    ]
    await buffer.close()


async def test_commit_stores_the_next_offset(fake_clients: None) -> None:
    buffer = KafkaBuffer("127.0.0.1:19092")
    await buffer.start()

    await buffer.commit(4)
    await buffer.commit(0)

    assert _raw_consumer().commits == [
        {RAW_PARTITION: OffsetAndMetadata(5, "")},
        {RAW_PARTITION: OffsetAndMetadata(1, "")},
    ]
    await buffer.close()


async def test_commit_rejects_a_negative_offset(fake_clients: None) -> None:
    buffer = KafkaBuffer("127.0.0.1:19092")
    await buffer.start()

    with pytest.raises(ValueError, match="negative offset"):
        await buffer.commit(-1)

    assert _raw_consumer().commits == []
    await buffer.close()


async def test_consume_raw_keeps_order_and_offsets(fake_clients: None) -> None:
    buffer = KafkaBuffer("127.0.0.1:19092")
    await buffer.start()
    raw = _raw_consumer()
    for offset, line in enumerate([b"a", b"b", b"c"]):
        raw.push(offset, line)

    records = await _take(buffer.consume_raw(), 3)

    assert [record.offset for record in records] == [0, 1, 2]
    assert [record.value for record in records] == [b"a", b"b", b"c"]
    await buffer.close()


async def test_consume_raw_receives_records_pushed_while_it_waits(fake_clients: None) -> None:
    buffer = KafkaBuffer("127.0.0.1:19092")
    await buffer.start()
    raw = _raw_consumer()
    stream = buffer.consume_raw()
    pending = asyncio.ensure_future(_take(stream, 1))
    await asyncio.sleep(0)

    raw.push(0, b"late")
    records = await pending

    assert [record.value for record in records] == [b"late"]
    await buffer.close()


async def test_consume_raw_rejects_non_bytes_values(fake_clients: None) -> None:
    buffer = KafkaBuffer("127.0.0.1:19092")
    await buffer.start()
    _raw_consumer().push(0, None)

    with pytest.raises(ValueError, match="must be bytes"):
        await anext(buffer.consume_raw())

    await buffer.close()


async def test_offset_gap_raises_a_clear_error(fake_clients: None) -> None:
    buffer = KafkaBuffer("127.0.0.1:19092")
    await buffer.start()
    raw = _raw_consumer()
    raw.push(5, b"a")
    raw.push(7, b"b")
    stream = buffer.consume_raw()

    first = await anext(stream)

    assert first.offset == 5
    with pytest.raises(OffsetGapError, match="expected 6, got 7"):
        await anext(stream)
    await buffer.close()


async def test_consume_control_yields_published_payloads(fake_clients: None) -> None:
    buffer = KafkaBuffer("127.0.0.1:19092")
    await buffer.start()
    control = _control_consumer()
    control.push(0, b"first")
    control.push(1, b"second")

    payloads = await _take(buffer.consume_control(), 2)

    assert payloads == [b"first", b"second"]
    await buffer.close()


async def test_close_ends_open_iterators(fake_clients: None) -> None:
    buffer = KafkaBuffer("127.0.0.1:19092")
    await buffer.start()
    raw = _raw_consumer()
    stream = buffer.consume_raw()
    raw.push(0, b"only")

    assert (await anext(stream)).value == b"only"
    pending = asyncio.ensure_future(_take(stream, 1))
    await asyncio.sleep(0)

    await buffer.close()

    assert await asyncio.wait_for(pending, timeout=2) == []
    assert raw.stopped


async def test_operations_after_close_raise(fake_clients: None) -> None:
    buffer = KafkaBuffer("127.0.0.1:19092")
    await buffer.start()
    await buffer.close()

    with pytest.raises(RuntimeError, match="closed"):
        await buffer.publish_raw(b"x")
    with pytest.raises(RuntimeError, match="closed"):
        buffer.consume_raw()
    with pytest.raises(RuntimeError, match="closed"):
        buffer.consume_control()
    with pytest.raises(RuntimeError, match="closed"):
        await buffer.commit(0)
    with pytest.raises(RuntimeError, match="closed"):
        await buffer.start()


async def _take_after_marker(
    iterator: AsyncIterator[Any], marker: bytes, count: int, *, timeout: float
) -> list[Any]:
    """Skip any backlog, then collect ``count`` records starting at ``marker``."""

    async def collect() -> list[Any]:
        records: list[Any] = []
        async for record in iterator:
            if record.value == marker:
                records.append(record)
                break
        async for record in iterator:
            records.append(record)
            if len(records) == count:
                break
        return records

    return await asyncio.wait_for(collect(), timeout=timeout)


@pytest.mark.network
async def test_redpanda_round_trip() -> None:
    """Publish, consume, commit, resume, control round trip on a real broker."""
    address = os.environ.get("TICKVAULT_TEST_KAFKA", "").strip()
    if not address:
        pytest.skip("set TICKVAULT_TEST_KAFKA=host:port to a throwaway broker")
    group_id = f"tickvault-test-{uuid.uuid4().hex}"
    run = uuid.uuid4().hex
    lines = [f'{{"v":1,"line":{index},"run":"{run}"}}'.encode() for index in range(1000)]

    ingester = KafkaBuffer(address, group_id=group_id, client_id="tv-test-ingester")
    await ingester.start()
    try:
        for line in lines:
            await ingester.publish_raw(line)
        records = await _take_after_marker(ingester.consume_raw(), lines[0], 1000, timeout=60)
        first_offset = records[0].offset
        await ingester.commit(first_offset + 500)
    finally:
        await ingester.close()

    assert [record.offset for record in records] == list(range(first_offset, first_offset + 1000))
    assert [record.value for record in records] == lines

    restarted = KafkaBuffer(address, group_id=group_id, client_id="tv-test-restarted")
    await restarted.start()
    try:
        resumed = await _take(restarted.consume_raw(), 1000 - 501, timeout=60)
        assert resumed[0].offset == first_offset + 501
        assert [record.value for record in resumed] == lines[501:]

        payload = b'{"connection_id":"c","reason":"gap","symbol":"BTC-USD","venue":"kraken"}'
        control = restarted.consume_control()
        pending = asyncio.ensure_future(_take(control, 1, timeout=30))
        await asyncio.sleep(0)
        await restarted.publish_control(payload)
        assert await pending == [payload]
    finally:
        await restarted.close()
