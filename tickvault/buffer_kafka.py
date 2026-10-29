"""Kafka-API buffer for a real broker (Redpanda), decision A70.

``tickvault.buffer.open_buffer("host:port")`` imports this module lazily and
builds a :class:`KafkaBuffer`, so callers keep speaking only the small
:class:`~tickvault.buffer.Buffer` protocol. The topics are the ones frozen in
``tickvault.buffer``: :data:`~tickvault.buffer.RAW_TOPIC` (one partition, so
consumption order equals publish order) and
:data:`~tickvault.buffer.CONTROL_TOPIC` (one partition, resync requests back to
the ingester).

Offsets on ``RAW_TOPIC`` are contiguous for a non-transactional, idempotent
producer; :meth:`KafkaBuffer.consume_raw` checks that and raises
:class:`OffsetGapError` instead of silently skipping records, which would make
the normalized layer miss data without an anomaly event. A rebalance hands the
partition back to this consumer at its committed offset, so the same method
accepts a rewind to a redelivered offset (at or above the commit point) and
only refuses a forward jump.

The two roles of decision A70 need different broker resources, so they are
built lazily: :meth:`KafkaBuffer.start` connects the admin client, the producer
and the group-less control consumer, and the raw consumer joins ``group_id``
only when :meth:`KafkaBuffer.consume_raw` is first iterated (or when
:meth:`KafkaBuffer.commit` needs it). An ingester publishes and reads control
records and therefore never joins the writer's group, where its extra member
would take the single raw partition away from the writer. :meth:`end_offset`
falls back to a group-less consumer, so reading the lag never joins the group
either.

Constructing a buffer is side-effect free and nothing contacts the broker
before :meth:`KafkaBuffer.start`, so address validation and offset arithmetic
are unit-testable without a broker.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import Final

from aiokafka import (  # type: ignore[import-untyped]
    AIOKafkaConsumer,
    AIOKafkaProducer,
)
from aiokafka.admin import (  # type: ignore[import-untyped]
    AIOKafkaAdminClient,
    NewTopic,
)
from aiokafka.errors import (  # type: ignore[import-untyped]
    ConsumerStoppedError,
    for_code,
)
from aiokafka.structs import (  # type: ignore[import-untyped]
    OffsetAndMetadata,
    TopicPartition,
)

from tickvault.buffer import CONTROL_TOPIC, RAW_TOPIC, BufferRecord

_LOGGER = logging.getLogger(__name__)

_PARTITION: Final = 0
"""Both topics have one partition, so the partition is always zero."""

_TOPIC_PARTITIONS: Final = 1
"""One partition per topic: publish order is consumption order."""

_REPLICATION_FACTOR: Final = 1
"""The development broker is a single node."""

_LINGER_MS: Final = 5
"""Small batching window; records still leave as soon as a poll completes."""

_MAX_RECORD_BYTES: Final = 16 * 1024 * 1024
"""Largest record the buffer moves in either direction: 16 MiB.

A Coinbase ``level2`` snapshot envelope is one JSON line of about 2.4 MB
(2,449,656 bytes in the Compose run that first hit this). aiokafka defaults to
1 MiB — ``max_request_size`` for the producer, which then raises
``MessageSizeTooLargeError`` on the very first snapshot, and
``max_partition_fetch_bytes`` for a consumer, which then can never fetch such
a record. 16 MiB leaves about six times the headroom and matches the cluster
``kafka_batch_max_bytes`` and topic ``max.message.bytes`` that the
``redpanda-init`` Compose service raises before the first client connects.
"""

_CREATE_TOPICS_TIMEOUT_MS: Final = 10_000
"""How long the broker may take to create the two topics on ``start``."""

_TOPIC_ALREADY_EXISTS: Final = 36
"""Kafka error code for ``TOPIC_ALREADY_EXISTS``; an existing topic is fine."""


class OffsetGapError(RuntimeError):
    """``RAW_TOPIC`` skipped offsets, so the raw stream is not contiguous.

    The producer is non-transactional and idempotent, so a forward gap means
    records were lost on the broker (unclean leader election, retention, an
    operator deleting records). Continuing would write a hole into the raw
    layer without an anomaly event, so the consumer stops instead. A backward
    move is only an error below the last committed offset; the rewind a
    rebalance causes is expected redelivery and is accepted.
    """


def parse_address(address: str) -> str:
    """Return the ``bootstrap_servers`` string for a ``HOST:PORT`` address.

    Raises :class:`ValueError` for anything else, mirroring
    :func:`tickvault.buffer.open_buffer`'s address rules so a direct
    :class:`KafkaBuffer` construction fails the same way.
    """
    if "://" in address:
        raise ValueError(f"Kafka buffer address must be HOST:PORT: {address!r}")
    host, separator, port = address.rpartition(":")
    if not separator or not host or not port.isdigit():
        raise ValueError(f"Kafka buffer address must be HOST:PORT: {address!r}")
    number = int(port)
    if not 1 <= number <= 65535:
        raise ValueError(f"Kafka buffer address port out of range: {address!r}")
    return f"{host}:{number}"


class KafkaBuffer:
    """A :class:`~tickvault.buffer.Buffer` over a Kafka-API broker.

    The producer sends with ``acks=all`` and idempotence and is sized for the
    2.4 MB Coinbase snapshot envelopes (``_MAX_RECORD_BYTES``). The raw
    consumer joins ``group_id`` with auto-commit off, so a restarted writer
    resumes from the last offset :meth:`commit` stored; it is created on the
    first :meth:`consume_raw` iteration (see :meth:`_ensure_raw`) so that an
    ingester never becomes a second member of the writer's group. The control
    consumer reads without a group, assigned to the single control partition
    and positioned at its end when :meth:`start` returns, so every ingester
    sees only requests published after it started.
    """

    def __init__(
        self,
        address: str,
        *,
        group_id: str = "tickvault-writer",
        client_id: str = "tickvault",
    ) -> None:
        self.address = address
        self.bootstrap_servers = parse_address(address)
        self.group_id = group_id
        self.client_id = client_id
        self._admin: AIOKafkaAdminClient | None = None
        self._producer: AIOKafkaProducer | None = None
        self._raw: AIOKafkaConsumer | None = None
        self._control: AIOKafkaConsumer | None = None
        self._offsets: AIOKafkaConsumer | None = None
        self._resume_floor: int | None = None
        self._closed = False

    async def start(self) -> None:
        """Create the topics if needed and connect producer and control consumer.

        The raw consumer is deliberately not started here: creating it joins
        ``group_id``, and an ingester that only publishes (and reads control)
        must not become a member of the writer's group on the single raw
        partition.
        """
        if self._closed:
            raise RuntimeError("Kafka buffer is closed")
        if self._admin is not None:
            return
        admin = AIOKafkaAdminClient(
            bootstrap_servers=self.bootstrap_servers,
            client_id=self.client_id,
        )
        await admin.start()
        producer: AIOKafkaProducer | None = None
        control: AIOKafkaConsumer | None = None
        try:
            await self._create_topics(admin)
            producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                client_id=self.client_id,
                acks="all",
                enable_idempotence=True,
                linger_ms=_LINGER_MS,
                max_request_size=_MAX_RECORD_BYTES,
            )
            await producer.start()
            control = AIOKafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                client_id=f"{self.client_id}-control",
                enable_auto_commit=False,
            )
            control_partition = TopicPartition(CONTROL_TOPIC, _PARTITION)
            control.assign([control_partition])
            await control.start()
            # Requests published before this point belong to an earlier
            # ingester (or an earlier run); a fresh one starts at the end.
            await control.seek_to_end(control_partition)
        except BaseException:
            for client in (control, producer, admin):
                if client is not None:
                    await _stop_quietly(client)
            raise
        self._admin = admin
        self._producer = producer
        self._control = control
        _LOGGER.debug(
            "Kafka buffer connected to %s (group %s)",
            self.bootstrap_servers,
            self.group_id,
        )

    async def _create_topics(self, admin: AIOKafkaAdminClient) -> None:
        """Create both topics with one partition unless they already exist."""
        response = await admin.create_topics(
            [
                NewTopic(
                    RAW_TOPIC,
                    num_partitions=_TOPIC_PARTITIONS,
                    replication_factor=_REPLICATION_FACTOR,
                ),
                NewTopic(
                    CONTROL_TOPIC,
                    num_partitions=_TOPIC_PARTITIONS,
                    replication_factor=_REPLICATION_FACTOR,
                ),
            ],
            timeout_ms=_CREATE_TOPICS_TIMEOUT_MS,
        )
        created: list[str] = []
        for topic_error in response.topic_errors:
            # Decoded array fields are tuples: (topic, error_code[, error_message]).
            topic = str(topic_error[0])
            code = int(topic_error[1])
            if code == _TOPIC_ALREADY_EXISTS:
                continue
            if code != 0:
                error_type = for_code(code)
                detail = topic_error[2] if len(topic_error) > 2 else None
                reason = f"{error_type.__name__} (code {code})"
                if detail:
                    reason = f"{reason}: {detail}"
                raise error_type(
                    f"creating topic {topic!r} on {self.bootstrap_servers} failed with {reason}"
                )
            created.append(topic)
        if created:
            _LOGGER.info(
                "created buffer topics %s on %s",
                ", ".join(created),
                self.bootstrap_servers,
            )

    async def publish_raw(self, line: bytes) -> None:
        """Send one raw envelope line to ``RAW_TOPIC`` partition 0."""
        producer = self._producer_or_raise()
        # Awaiting the future means acks=all: the line is durable before the
        # ingester moves on, matching the raw writer's append-then-continue.
        await producer.send(RAW_TOPIC, line, partition=_PARTITION)

    async def publish_control(self, payload: bytes) -> None:
        """Send one encoded resync request to ``CONTROL_TOPIC`` partition 0."""
        producer = self._producer_or_raise()
        await producer.send(CONTROL_TOPIC, payload, partition=_PARTITION)

    def consume_raw(self) -> AsyncIterator[BufferRecord]:
        """Yield raw records from the committed offset + 1 until closed.

        Raising here (not started, already closed) keeps the contract of every
        other method; the consumer itself is created and started by the first
        iteration, which is what keeps an ingester out of ``group_id``.
        """
        self._require_started()
        return self._iter_raw()

    def consume_control(self) -> AsyncIterator[bytes]:
        """Yield control records published after :meth:`start` until closed."""
        return self._iter_control(self._control_or_raise())

    async def commit(self, offset: int) -> None:
        """Commit ``offset + 1``: Kafka stores the next offset to read.

        Only a group member can commit, so this creates the raw consumer when
        nothing has consumed yet; only the writer commits, and it always
        iterates :meth:`consume_raw` first. The committed offset also becomes
        the floor a later rebalance may rewind to.
        """
        if offset < 0:
            raise ValueError(f"cannot commit a negative offset: {offset}")
        consumer = await self._ensure_raw()
        partition = TopicPartition(RAW_TOPIC, _PARTITION)
        await consumer.commit({partition: OffsetAndMetadata(offset + 1, "")})
        floor = 0 if self._resume_floor is None else self._resume_floor
        self._resume_floor = max(floor, offset + 1)

    async def end_offset(self) -> int | None:
        """Next offset the broker would assign on ``RAW_TOPIC`` partition 0.

        aiokafka's ``end_offsets`` is the offset of the upcoming message — the
        last available offset plus one — so ``end_offset() - next_to_consume``
        is the number of records still waiting. It does not move the consumer's
        position, so the reader can be called while the writer is consuming.
        ``None`` when the broker reports no end offset for the partition.

        The query uses the writer's raw consumer when it exists and otherwise a
        group-less one (``_offsets_consumer``): reading the lag must never turn
        an ingester into a member of ``group_id``.
        """
        self._require_started()
        consumer = self._raw if self._raw is not None else await self._offsets_consumer()
        partition = TopicPartition(RAW_TOPIC, _PARTITION)
        offsets = await consumer.end_offsets([partition])
        end = offsets.get(partition)
        return None if end is None else int(end)

    async def close(self) -> None:
        """Stop every client and end this buffer's open iterators."""
        if self._closed:
            return
        self._closed = True
        control, raw = self._control, self._raw
        offsets, producer, admin = self._offsets, self._producer, self._admin
        self._control = None
        self._raw = None
        self._offsets = None
        self._producer = None
        self._admin = None
        for client in (control, raw, offsets, producer, admin):
            if client is not None:
                await _stop_quietly(client)

    async def _raw_consumer(self) -> AIOKafkaConsumer:
        """Return a started raw consumer, creating it on first use.

        Joining ``group_id`` is the expensive, group-visible step, so it
        happens only for a process that really reads raw records. The committed
        offset read here is the lowest offset a rebalance may legitimately
        redeliver from (``None`` when nothing was committed yet, which is the
        same as a floor of zero).
        """
        consumer = AIOKafkaConsumer(
            RAW_TOPIC,
            bootstrap_servers=self.bootstrap_servers,
            client_id=f"{self.client_id}-raw",
            group_id=self.group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_partition_fetch_bytes=_MAX_RECORD_BYTES,
        )
        try:
            await consumer.start()
            self._resume_floor = await self._committed_offset(consumer)
        except BaseException:
            await _stop_quietly(consumer)
            raise
        _LOGGER.debug(
            "Kafka raw consumer joined group %s (resume floor %s)",
            self.group_id,
            self._resume_floor,
        )
        return consumer

    async def _ensure_raw(self) -> AIOKafkaConsumer:
        """Return the raw consumer, starting it on first use."""
        self._require_started()
        if self._raw is None:
            self._raw = await self._raw_consumer()
        return self._raw

    async def _offsets_consumer(self) -> AIOKafkaConsumer:
        """Return a group-less consumer used only to query end offsets.

        No ``group_id`` and no subscription mean the broker sees no group
        member, which is what lets an ingester read the lag without ever
        joining ``tickvault-writer``. ``end_offsets`` resolves the partition
        leader from metadata on its own, so nothing has to be assigned.
        """
        if self._offsets is not None:
            return self._offsets
        consumer = AIOKafkaConsumer(
            client_id=f"{self.client_id}-offsets",
            bootstrap_servers=self.bootstrap_servers,
            enable_auto_commit=False,
        )
        try:
            await consumer.start()
        except BaseException:
            await _stop_quietly(consumer)
            raise
        self._offsets = consumer
        return consumer

    async def _committed_offset(self, consumer: AIOKafkaConsumer) -> int | None:
        """Last committed offset of the raw partition, or ``None`` if unknown.

        This is the next offset to read, so it doubles as the lowest offset a
        rebalance may rewind to. A broker error only costs the strict check (the
        cursor then accepts a rewind from any offset the broker delivers).
        """
        partition = TopicPartition(RAW_TOPIC, _PARTITION)
        try:
            committed = await consumer.committed(partition)
        except Exception:
            _LOGGER.debug("could not read the committed raw offset", exc_info=True)
            return None
        return None if committed is None else int(committed)

    async def _iter_raw(self) -> AsyncIterator[BufferRecord]:
        """Yield records, ending when the buffer closes or the broker stops us."""
        consumer = await self._ensure_raw()
        expected: int | None = None
        try:
            while not self._closed:
                message = await consumer.getone()
                offset = int(message.offset)
                value = _as_bytes(message.value)
                expected = self._check_offset(offset, expected)
                yield BufferRecord(offset=offset, value=value)
        except ConsumerStoppedError:
            if not self._closed:
                raise

    def _check_offset(self, offset: int, expected: int | None) -> int:
        """Return the offset after ``offset``, keeping the stream contiguous.

        A rebalance gives the partition back to this consumer, which resumes at
        its committed offset, so records it already handled arrive again: an
        offset below ``expected`` that is still at or above the commit point
        rewinds the cursor and continues. Those records are redelivered on
        purpose — the writer drops envelopes it already wrote and
        :func:`tickvault.rawio.iter_raw` drops duplicate lines, so the raw layer
        stays replay-identical. A forward jump loses records for good and a
        rewind below the commit point disagrees with what this process already
        committed; both raise :class:`OffsetGapError`.
        """
        if expected is None or offset == expected:
            return offset + 1
        if offset > expected:
            raise OffsetGapError(
                f"non-contiguous offsets on {RAW_TOPIC} partition {_PARTITION}: "
                f"expected {expected}, got {offset}"
            )
        floor = self._resume_floor
        if floor is not None and offset < floor:
            raise OffsetGapError(
                f"offset {offset} on {RAW_TOPIC} partition {_PARTITION} is before "
                f"the committed offset {floor} (expected {expected})"
            )
        _LOGGER.info(
            "rewinding %s partition %d from %d to %d after a rebalance redelivery",
            RAW_TOPIC,
            _PARTITION,
            expected,
            offset,
        )
        return offset + 1

    async def _iter_control(self, consumer: AIOKafkaConsumer) -> AsyncIterator[bytes]:
        """Yield control payloads, ending when the buffer closes."""
        try:
            while not self._closed:
                message = await consumer.getone()
                yield _as_bytes(message.value)
        except ConsumerStoppedError:
            if not self._closed:
                raise

    def _producer_or_raise(self) -> AIOKafkaProducer:
        self._require_started()
        assert self._producer is not None
        return self._producer

    def _control_or_raise(self) -> AIOKafkaConsumer:
        self._require_started()
        assert self._control is not None
        return self._control

    def _require_started(self) -> None:
        if self._closed:
            raise RuntimeError("Kafka buffer is closed")
        if self._admin is None:
            raise RuntimeError("Kafka buffer is not started; call start() first")


def _as_bytes(value: object) -> bytes:
    """Return one record value as ``bytes``, rejecting anything else."""
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray | memoryview):
        return bytes(value)
    raise ValueError(f"buffer record value must be bytes, got {type(value).__name__}")


async def _stop_quietly(client: object) -> None:
    """Stop one aiokafka client, logging (never raising) a shutdown error."""
    stop = getattr(client, "stop", None)
    if stop is None:
        return
    try:
        await stop()
    except Exception:
        _LOGGER.debug("ignoring error while stopping Kafka client", exc_info=True)
