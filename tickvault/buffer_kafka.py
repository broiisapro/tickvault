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
the normalized layer miss data without an anomaly event.

The broker is only contacted by :meth:`KafkaBuffer.start`; constructing a
buffer is side-effect free, so address validation and offset arithmetic are
unit-testable without a broker.
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

_CREATE_TOPICS_TIMEOUT_MS: Final = 10_000
"""How long the broker may take to create the two topics on ``start``."""

_TOPIC_ALREADY_EXISTS: Final = 36
"""Kafka error code for ``TOPIC_ALREADY_EXISTS``; an existing topic is fine."""


class OffsetGapError(RuntimeError):
    """``RAW_TOPIC`` skipped offsets, so the raw stream is not contiguous.

    The producer is non-transactional and idempotent, so a gap means records
    were lost on the broker (unclean leader election, retention, an operator
    deleting records). Continuing would write a hole into the raw layer
    without an anomaly event, so the consumer stops instead.
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

    The raw consumer joins ``group_id`` with auto-commit off, so a restarted
    writer resumes from the last offset :meth:`commit` stored. The control
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
        self._closed = False

    async def start(self) -> None:
        """Create the topics if needed and connect producer and consumers."""
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
        raw: AIOKafkaConsumer | None = None
        control: AIOKafkaConsumer | None = None
        try:
            await self._create_topics(admin)
            producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                client_id=self.client_id,
                acks="all",
                enable_idempotence=True,
                linger_ms=_LINGER_MS,
            )
            await producer.start()
            raw = AIOKafkaConsumer(
                RAW_TOPIC,
                bootstrap_servers=self.bootstrap_servers,
                client_id=f"{self.client_id}-raw",
                group_id=self.group_id,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
            )
            await raw.start()
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
            for client in (control, raw, producer, admin):
                if client is not None:
                    await _stop_quietly(client)
            raise
        self._admin = admin
        self._producer = producer
        self._raw = raw
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
        """Yield raw records from the committed offset + 1 until closed."""
        return self._iter_raw(self._raw_or_raise())

    def consume_control(self) -> AsyncIterator[bytes]:
        """Yield control records published after :meth:`start` until closed."""
        return self._iter_control(self._control_or_raise())

    async def commit(self, offset: int) -> None:
        """Commit ``offset + 1``: Kafka stores the next offset to read."""
        consumer = self._raw_or_raise()
        if offset < 0:
            raise ValueError(f"cannot commit a negative offset: {offset}")
        partition = TopicPartition(RAW_TOPIC, _PARTITION)
        await consumer.commit({partition: OffsetAndMetadata(offset + 1, "")})

    async def close(self) -> None:
        """Stop every client and end this buffer's open iterators."""
        if self._closed:
            return
        self._closed = True
        control, raw, producer, admin = self._control, self._raw, self._producer, self._admin
        self._control = None
        self._raw = None
        self._producer = None
        self._admin = None
        for client in (control, raw, producer, admin):
            if client is not None:
                await _stop_quietly(client)

    async def _iter_raw(self, consumer: AIOKafkaConsumer) -> AsyncIterator[BufferRecord]:
        """Yield records, ending when the buffer closes or the broker stops us."""
        expected: int | None = None
        try:
            while not self._closed:
                message = await consumer.getone()
                offset = int(message.offset)
                if expected is not None and offset != expected:
                    raise OffsetGapError(
                        f"non-contiguous offsets on {RAW_TOPIC} partition {_PARTITION}: "
                        f"expected {expected}, got {offset}"
                    )
                expected = offset + 1
                yield BufferRecord(offset=offset, value=_as_bytes(message.value))
        except ConsumerStoppedError:
            if not self._closed:
                raise

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

    def _raw_or_raise(self) -> AIOKafkaConsumer:
        self._require_started()
        assert self._raw is not None
        return self._raw

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
