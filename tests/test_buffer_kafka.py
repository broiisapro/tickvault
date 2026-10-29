"""Kafka buffer tests: protocol behaviour without a broker, plus an opt-in round trip.

The unit tests replace the three aiokafka client classes with fakes, so they
run everywhere (Docker is not in CI, decision A34). They also pin the two
production bugs of the buffered Compose run (issue #141): a 2.4 MB Coinbase
snapshot needs a 16 MiB producer request and consumer fetch limit, and an
ingester must never create the raw consumer, because a second member of
``tickvault-writer`` takes the single raw partition away from the writer.

The broker tests are marked ``network`` and only run when
``TICKVAULT_TEST_KAFKA=host:port`` points at a throwaway broker whose cluster
``kafka_batch_max_bytes`` and topic ``max.message.bytes`` are raised to 16 MiB
(issue #123).
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
from tickvault.buffer import CONTROL_TOPIC, RAW_TOPIC, BufferRecord
from tickvault.buffer_kafka import KafkaBuffer, OffsetGapError

RAW_PARTITION = TopicPartition(RAW_TOPIC, 0)
CONTROL_PARTITION = TopicPartition(CONTROL_TOPIC, 0)

MAX_RECORD_BYTES = 16 * 1024 * 1024
"""The size limit both clients must carry; a snapshot envelope is ~2.4 MB."""

SNAPSHOT_ENVELOPE_BYTES = 2_449_656
"""Size of the Coinbase level2 snapshot that broke the Compose run."""


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
    is exactly the behaviour the iterator shutdown tests need. ``committed``
    reports the offset a group consumer would resume from (``None`` before any
    commit), and the constructor kwargs record whether the consumer joined a
    group at all.
    """

    instances: ClassVar[list[FakeConsumer]] = []

    def __init__(self, *topics: str, **kwargs: Any) -> None:
        self.topics = topics
        self.kwargs = kwargs
        self.messages: list[FakeRecord] = []
        self.assigned: set[TopicPartition] = set()
        self.seeked: list[TopicPartition] = []
        self.commits: list[dict[TopicPartition, OffsetAndMetadata]] = []
        self.end_queries: list[list[TopicPartition]] = []
        self.committed_queries: list[TopicPartition] = []
        self.end_offset: int | None = 0
        self.committed_offset: int | None = None
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

    async def committed(self, partition: TopicPartition) -> int | None:
        self.committed_queries.append(partition)
        return self.committed_offset

    async def end_offsets(self, partitions: list[TopicPartition]) -> dict[TopicPartition, int]:
        self.end_queries.append(list(partitions))
        if self.end_offset is None:
            return {}
        return {partition: self.end_offset for partition in partitions}

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


def _consumer(client_id: str) -> FakeConsumer:
    """The fake consumer built with ``client_id`` (raw, control or offsets)."""
    return next(
        consumer
        for consumer in FakeConsumer.instances
        if consumer.kwargs.get("client_id") == client_id
    )


def _group_consumers() -> list[FakeConsumer]:
    """Every fake consumer that joined a consumer group."""
    return [consumer for consumer in FakeConsumer.instances if "group_id" in consumer.kwargs]


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


async def _open_raw(
    buffer: KafkaBuffer, count: int
) -> tuple[AsyncIterator[BufferRecord], FakeConsumer, asyncio.Task[list[BufferRecord]]]:
    """Open a raw stream and wait until its lazily created consumer exists.

    The raw consumer joins ``group_id`` on the first iteration, so a test that
    wants to push records has to start collecting first: the returned task
    completes once ``count`` pushed records have arrived, and ``stream`` stays
    usable for further reads.
    """
    stream = buffer.consume_raw()
    pending = asyncio.ensure_future(_take(stream, count))
    raw_id = f"{buffer.client_id}-raw"
    for _ in range(10):
        if any(consumer.kwargs.get("client_id") == raw_id for consumer in FakeConsumer.instances):
            break
        await asyncio.sleep(0)
    return stream, _consumer(raw_id), pending


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
    assert producer.kwargs["max_request_size"] == MAX_RECORD_BYTES
    assert producer.kwargs["max_request_size"] > SNAPSHOT_ENVELOPE_BYTES

    control = _consumer("tv-control")
    assert control.started
    assert control.topics == ()
    assert control.kwargs["client_id"] == "tv-control"
    assert control.assigned == {CONTROL_PARTITION}
    assert control.seeked == [CONTROL_PARTITION]

    # The raw consumer joins the writer's group, so start() must not build it:
    # that is what keeps an ingester from taking the single raw partition.
    assert _group_consumers() == []
    assert len(FakeConsumer.instances) == 1

    await buffer.close()

    assert admin.stopped
    assert producer.stopped
    assert control.stopped


async def test_start_is_idempotent(fake_clients: None) -> None:
    buffer = KafkaBuffer("127.0.0.1:19092")
    await buffer.start()
    await buffer.start()

    assert len(FakeAdmin.instances) == 1
    assert len(FakeProducer.instances) == 1
    assert len(FakeConsumer.instances) == 1
    assert _group_consumers() == []
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

    raw = _consumer("tickvault-raw")
    assert raw.commits == [
        {RAW_PARTITION: OffsetAndMetadata(5, "")},
        {RAW_PARTITION: OffsetAndMetadata(1, "")},
    ]
    assert raw.kwargs["group_id"] == "tickvault-writer"
    await buffer.close()


async def test_commit_rejects_a_negative_offset(fake_clients: None) -> None:
    buffer = KafkaBuffer("127.0.0.1:19092")
    await buffer.start()

    with pytest.raises(ValueError, match="negative offset"):
        await buffer.commit(-1)

    assert len(FakeConsumer.instances) == 1
    assert _group_consumers() == []
    await buffer.close()


async def test_end_offset_queries_partition_zero(fake_clients: None) -> None:
    buffer = KafkaBuffer("127.0.0.1:19092")
    await buffer.start()
    assert await buffer.end_offset() == 0
    offsets = _consumer("tickvault-offsets")
    offsets.end_offset = 42

    assert await buffer.end_offset() == 42
    assert offsets.end_queries == [[RAW_PARTITION], [RAW_PARTITION]]
    await buffer.close()


async def test_end_offset_is_none_when_the_broker_reports_no_offsets(
    fake_clients: None,
) -> None:
    buffer = KafkaBuffer("127.0.0.1:19092")
    await buffer.start()
    assert await buffer.end_offset() == 0
    _consumer("tickvault-offsets").end_offset = None

    assert await buffer.end_offset() is None
    await buffer.close()


async def test_end_offset_before_start_raises() -> None:
    with pytest.raises(RuntimeError, match="not started"):
        await KafkaBuffer("127.0.0.1:19092").end_offset()


async def test_raw_consumer_joins_the_group_only_on_first_iteration(
    fake_clients: None,
) -> None:
    buffer = KafkaBuffer("127.0.0.1:19092", group_id="tickvault-test", client_id="tv")
    await buffer.start()
    assert _group_consumers() == []

    stream = buffer.consume_raw()
    await asyncio.sleep(0)
    assert _group_consumers() == []

    pending = asyncio.ensure_future(anext(stream))
    await asyncio.sleep(0)
    raw = _consumer("tv-raw")
    assert _group_consumers() == [raw]
    raw.push(0, b"first")

    assert (await pending).value == b"first"
    assert raw.topics == (RAW_TOPIC,)
    assert raw.kwargs["group_id"] == "tickvault-test"
    assert raw.kwargs["enable_auto_commit"] is False
    assert raw.kwargs["auto_offset_reset"] == "earliest"
    assert raw.kwargs["max_partition_fetch_bytes"] == MAX_RECORD_BYTES
    assert raw.kwargs["max_partition_fetch_bytes"] > SNAPSHOT_ENVELOPE_BYTES
    assert raw.committed_queries == [RAW_PARTITION]
    await buffer.close()


async def test_ingester_never_joins_the_writer_group(fake_clients: None) -> None:
    buffer = KafkaBuffer("127.0.0.1:19092", client_id="ingester")
    await buffer.start()
    control = _consumer("ingester-control")
    control.push(0, b'{"reason":"gap"}')

    await buffer.publish_raw(b'{"v":1,"venue":"coinbase"}')
    await buffer.publish_control(b'{"reason":"gap"}')
    assert await _take(buffer.consume_control(), 1) == [b'{"reason":"gap"}']
    await buffer.close()

    assert _group_consumers() == []
    assert [consumer.topics for consumer in FakeConsumer.instances] == [()]


async def test_end_offset_does_not_join_the_writer_group(fake_clients: None) -> None:
    buffer = KafkaBuffer("127.0.0.1:19092", client_id="ingester")
    await buffer.start()

    assert await buffer.end_offset() == 0

    offsets = _consumer("ingester-offsets")
    assert offsets.topics == ()
    assert "group_id" not in offsets.kwargs
    assert _group_consumers() == []
    await buffer.close()
    assert offsets.stopped


async def test_end_offset_reuses_the_raw_consumer_of_a_writer(
    fake_clients: None,
) -> None:
    buffer = KafkaBuffer("127.0.0.1:19092")
    await buffer.start()
    _stream, raw, pending = await _open_raw(buffer, 1)
    raw.push(0, b"a")
    assert len(await pending) == 1

    raw.end_offset = 7
    assert await buffer.end_offset() == 7
    assert len(FakeConsumer.instances) == 2
    await buffer.close()


async def test_consume_raw_keeps_order_and_offsets(fake_clients: None) -> None:
    buffer = KafkaBuffer("127.0.0.1:19092")
    await buffer.start()
    _, raw, pending = await _open_raw(buffer, 3)
    for offset, line in enumerate([b"a", b"b", b"c"]):
        raw.push(offset, line)

    records = await pending

    assert [record.offset for record in records] == [0, 1, 2]
    assert [record.value for record in records] == [b"a", b"b", b"c"]
    await buffer.close()


async def test_consume_raw_receives_records_pushed_while_it_waits(fake_clients: None) -> None:
    buffer = KafkaBuffer("127.0.0.1:19092")
    await buffer.start()
    _, raw, pending = await _open_raw(buffer, 1)

    raw.push(0, b"late")
    records = await pending

    assert [record.value for record in records] == [b"late"]
    await buffer.close()


async def test_consume_raw_rejects_non_bytes_values(fake_clients: None) -> None:
    buffer = KafkaBuffer("127.0.0.1:19092")
    await buffer.start()
    stream = buffer.consume_raw()
    pending = asyncio.ensure_future(anext(stream))
    await asyncio.sleep(0)
    _consumer("tickvault-raw").push(0, None)

    with pytest.raises(ValueError, match="must be bytes"):
        await pending

    await buffer.close()


async def test_offset_gap_raises_a_clear_error(fake_clients: None) -> None:
    buffer = KafkaBuffer("127.0.0.1:19092")
    await buffer.start()
    stream, raw, pending = await _open_raw(buffer, 1)
    raw.push(5, b"a")

    first = (await pending)[0]

    assert first.offset == 5
    raw.push(7, b"b")
    with pytest.raises(OffsetGapError, match="expected 6, got 7"):
        await anext(stream)
    await buffer.close()


async def test_consume_raw_rewinds_to_a_redelivered_offset(fake_clients: None) -> None:
    """A rebalance resumes at the committed offset, so redelivery is expected."""
    buffer = KafkaBuffer("127.0.0.1:19092")
    await buffer.start()
    stream, raw, pending = await _open_raw(buffer, 3)
    for offset in range(3):
        raw.push(offset, bytes([offset]))

    assert [record.offset for record in await pending] == [0, 1, 2]

    raw.push(0, b"again")
    raw.push(1, b"again")
    redelivered = await _take(stream, 2)

    assert [record.offset for record in redelivered] == [0, 1]
    assert [record.value for record in redelivered] == [b"again", b"again"]
    await buffer.close()


async def test_consume_raw_rewinds_to_zero_when_nothing_was_committed(
    fake_clients: None,
) -> None:
    """The Compose failure: a rebalanced consumer restarted at offset 0."""
    buffer = KafkaBuffer("127.0.0.1:19092")
    await buffer.start()
    stream, raw, pending = await _open_raw(buffer, 36)
    for offset in range(36):
        raw.push(offset, bytes([offset]))

    assert len(await pending) == 36

    raw.push(0, b"redelivered")
    assert (await anext(stream)).offset == 0
    await buffer.close()


async def test_consume_raw_still_rejects_a_forward_gap_after_a_rewind(
    fake_clients: None,
) -> None:
    buffer = KafkaBuffer("127.0.0.1:19092")
    await buffer.start()
    stream, raw, pending = await _open_raw(buffer, 2)
    raw.push(0, b"a")
    raw.push(1, b"b")
    assert len(await pending) == 2

    raw.push(1, b"b again")
    assert (await anext(stream)).offset == 1

    raw.push(4, b"lost 2 and 3")
    with pytest.raises(OffsetGapError, match="expected 2, got 4"):
        await anext(stream)
    await buffer.close()


async def test_consume_raw_rejects_a_rewind_below_the_committed_offset(
    fake_clients: None,
) -> None:
    buffer = KafkaBuffer("127.0.0.1:19092")
    await buffer.start()
    stream, raw, pending = await _open_raw(buffer, 3)
    for offset in range(3):
        raw.push(offset, bytes([offset]))
    assert len(await pending) == 3
    await buffer.commit(2)

    raw.push(1, b"before the commit")
    with pytest.raises(OffsetGapError, match="before the committed offset 3"):
        await anext(stream)
    await buffer.close()


async def test_consume_control_yields_published_payloads(fake_clients: None) -> None:
    buffer = KafkaBuffer("127.0.0.1:19092")
    await buffer.start()
    control = _consumer("tickvault-control")
    control.push(0, b"first")
    control.push(1, b"second")

    payloads = await _take(buffer.consume_control(), 2)

    assert payloads == [b"first", b"second"]
    await buffer.close()


async def test_close_ends_open_iterators(fake_clients: None) -> None:
    buffer = KafkaBuffer("127.0.0.1:19092")
    await buffer.start()
    stream, raw, pending = await _open_raw(buffer, 1)
    raw.push(0, b"only")

    assert (await pending)[0].value == b"only"
    pending = asyncio.ensure_future(_take(stream, 1))
    await asyncio.sleep(0)

    await buffer.close()

    assert await asyncio.wait_for(pending, timeout=2) == []
    assert raw.stopped


async def test_close_stops_the_offsets_consumer(fake_clients: None) -> None:
    buffer = KafkaBuffer("127.0.0.1:19092")
    await buffer.start()
    await buffer.end_offset()
    offsets = _consumer("tickvault-offsets")

    await buffer.close()

    assert offsets.stopped


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
        await buffer.end_offset()
    with pytest.raises(RuntimeError, match="closed"):
        await buffer.start()


def _snapshot_line(size: int) -> bytes:
    """A raw envelope line of exactly ``size`` bytes shaped like a snapshot.

    A Coinbase ``level2`` snapshot envelope carries the whole frame as an
    escaped JSON string, so its payload dominates the line; filling it makes the
    record realistic in size without needing a recorded fixture.
    """
    prefix = (
        b'{"v":1,"venue":"coinbase","connection_id":"large",'
        b'"ingest_seq":0,"kind":"msg","payload":"'
    )
    suffix = b'"}'
    return prefix + b"x" * (size - len(prefix) - len(suffix)) + suffix


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
        while len(records) < count:
            records.append(await anext(iterator))
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


@pytest.mark.network
async def test_redpanda_round_trip_accepts_a_large_snapshot() -> None:
    """One 2.5 MB snapshot record survives producer, topic and raw consumer.

    With aiokafka's defaults the producer raises ``MessageSizeTooLargeError``
    before the record reaches the broker (``max_request_size`` is 1 MiB), and a
    raw consumer could never fetch it (``max_partition_fetch_bytes`` is
    1 MiB), so this pins both limits against a real broker whose cluster
    ``kafka_batch_max_bytes`` and topic ``max.message.bytes`` are raised too.
    """
    address = os.environ.get("TICKVAULT_TEST_KAFKA", "").strip()
    if not address:
        pytest.skip("set TICKVAULT_TEST_KAFKA=host:port to a throwaway broker")
    group_id = f"tickvault-test-{uuid.uuid4().hex}"
    line = _snapshot_line(2_500_000)
    assert len(line) == 2_500_000 > SNAPSHOT_ENVELOPE_BYTES

    writer = KafkaBuffer(address, group_id=group_id, client_id="tv-test-large")
    await writer.start()
    try:
        await writer.publish_raw(line)
        records = await _take_after_marker(writer.consume_raw(), line, 1, timeout=60)
    finally:
        await writer.close()

    assert records[0].value == line

    # An ingester reads the lag without joining the writer's group.
    ingester = KafkaBuffer(address, group_id=group_id, client_id="tv-test-lag")
    await ingester.start()
    try:
        await ingester.publish_raw(b'{"v":1,"line":"after-the-snapshot"}')
        end = await ingester.end_offset()
    finally:
        await ingester.close()

    assert end is not None
    assert end >= records[0].offset + 2
