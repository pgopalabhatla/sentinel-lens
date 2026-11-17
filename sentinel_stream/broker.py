"""The few Kafka calls the publisher, the consumer and the tests share.

Client: confluent-kafka (the Python wrapper around librdkafka, the reference C client). It has a
ready-made wheel for this project's Python, its calls are synchronous (poll, produce, flush, commit),
so the consumer loop reads top to bottom, and it has the three things the consumer needs: commits
with an explicit offset, message timestamps set by the producer, and looking up an offset by time.

Every topic here has ONE partition. Kafka orders messages only inside a partition, and the stream
has to stay in event-time order.
"""
from __future__ import annotations

import os
import uuid
from dataclasses import dataclass

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from sentinel_stream import wire

DEFAULT_BOOTSTRAP = os.environ.get("SENTINEL_KAFKA", "127.0.0.1:19092")     # docker-compose.yml
PARTITION = 0


@dataclass(frozen=True)
class Topics:
    telemetry: str = "telemetry"
    alerts: str = "alerts"
    pages: str = "pages"

    def names(self) -> list:
        return [self.telemetry, self.alerts, self.pages]


def client_config(bootstrap: str, **extra) -> dict:
    # IPv4 only: "localhost" resolves to ::1 first on macOS and the broker's port is published on 127.0.0.1
    return {"bootstrap.servers": bootstrap, "broker.address.family": "v4", **extra}


def reachable(bootstrap: str = DEFAULT_BOOTSTRAP, timeout: float = 2.0) -> bool:
    try:
        AdminClient(client_config(bootstrap, **{"log_level": 0})).list_topics(timeout=timeout)
        return True
    except KafkaException:
        return False


def ensure_topics(bootstrap: str, names) -> None:
    """Create the topics that do not exist yet (one partition, kept forever) and refuse to go on if
    one exists with more than one partition.

    Kept forever (retention.ms = -1) because the messages carry event time as their timestamp, which
    for replayed data is weeks old: a time-based retention rule could delete them at once. And
    `alerts` is the consumer's memory of which alerts have fired, so it must not expire.
    """
    admin = AdminClient(client_config(bootstrap))
    existing = admin.list_topics(timeout=10).topics
    new = [NewTopic(n, num_partitions=1, replication_factor=1, config={"retention.ms": "-1"})
           for n in names if n not in existing]
    for name, future in admin.create_topics(new).items() if new else ():
        try:
            future.result(timeout=30)
        except KafkaException as e:
            if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
    topics = admin.list_topics(timeout=10).topics
    for n in names:
        if n in topics and len(topics[n].partitions) != 1:
            raise RuntimeError(f"topic {n!r} has {len(topics[n].partitions)} partitions; it must have exactly 1")


def delete_topics(bootstrap: str, names) -> None:
    """Delete the topics and wait until the broker confirms. A topic that does not exist is fine;
    any other failure is raised, so leftovers do not pile up unnoticed."""
    names = list(names)
    if not names:
        return
    failed = {}
    admin = AdminClient(client_config(bootstrap))      # keep it alive: the futures die with the client
    for name, future in admin.delete_topics(names, operation_timeout=30).items():
        try:
            future.result(timeout=60)
        except KafkaException as e:
            if e.args[0].code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                failed[name] = e.args[0].str()
    if failed:
        raise RuntimeError(f"could not delete topics: {failed}")


def _reader(bootstrap: str) -> Consumer:
    """A consumer for reading a topic directly: it never commits and belongs to no lasting group."""
    return Consumer(client_config(bootstrap, **{"group.id": f"reader-{uuid.uuid4().hex}", "enable.auto.commit": False}))


def end_offset(bootstrap: str, topic: str) -> int:
    reader = _reader(bootstrap)
    try:
        return reader.get_watermark_offsets(TopicPartition(topic, PARTITION), timeout=10)[1]
    finally:
        reader.close()


def read_topic(bootstrap: str, topic: str, start: int | None = None, end: int | None = None) -> list:
    """The records at offsets start <= offset < end, decoded, in order. Default: everything that is
    in the topic at the moment of the call."""
    reader = _reader(bootstrap)
    try:
        tp = TopicPartition(topic, PARTITION)
        low, high = reader.get_watermark_offsets(tp, timeout=10)
        start = low if start is None else max(start, low)
        end = high if end is None else min(end, high)
        out = []
        if start >= end:
            return out
        reader.assign([TopicPartition(topic, PARTITION, start)])
        while start + len(out) < end:
            # ask for exactly what is left: consume() waits for the full count or the timeout
            batch = reader.consume(num_messages=min(1000, end - start - len(out)), timeout=10)
            if not batch:
                raise RuntimeError(f"topic {topic!r}: no message for 10 s while reading up to offset {end}")
            for msg in batch:
                if msg.error():
                    raise KafkaException(msg.error())
                out.append(wire.loads(msg.value()))
        return out
    finally:
        reader.close()


def first_offset_at_or_after(consumer: Consumer, topic: str, ts) -> int | None:
    """Offset of the first message whose timestamp (event time) is at or after `ts`; None if there
    is no such message yet."""
    found = consumer.offsets_for_times([TopicPartition(topic, PARTITION, wire.millis(ts))], timeout=10)[0]
    if found.error:
        raise KafkaException(found.error)
    return found.offset if found.offset >= 0 else None
