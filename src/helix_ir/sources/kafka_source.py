"""Kafka source (requires confluent-kafka)."""

from __future__ import annotations

import json
from typing import Any, Iterable

try:
    from confluent_kafka import Consumer, KafkaException  # type: ignore[import]
    _KAFKA_AVAILABLE = True
except ImportError:
    _KAFKA_AVAILABLE = False


class KafkaSource:
    """Read documents from a Kafka topic."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str = "helix_ir_consumer",
        max_messages: int = 10000,
        timeout_seconds: float = 10.0,
        consumer_config: dict[str, Any] | None = None,
    ) -> None:
        if not _KAFKA_AVAILABLE:
            raise ImportError(
                "confluent-kafka is required for KafkaSource. "
                "Install it with: pip install 'helix-ir[kafka]'"
            )
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.max_messages = max_messages
        self.timeout_seconds = timeout_seconds
        self.consumer_config = consumer_config or {}

    def read(self) -> Iterable[dict[str, Any]]:
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": "earliest",
            **self.consumer_config,
        }
        consumer = Consumer(config)
        try:
            consumer.subscribe([self.topic])
            count = 0
            while count < self.max_messages:
                msg = consumer.poll(timeout=self.timeout_seconds)
                if msg is None:
                    break  # Timeout: no more messages
                if msg.error():
                    raise KafkaException(msg.error())
                value = msg.value()
                if value:
                    try:
                        doc = json.loads(value.decode("utf-8"))
                        if isinstance(doc, dict):
                            yield doc
                        count += 1
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        continue
        finally:
            consumer.close()

    def schema_hint(self) -> dict[str, Any] | None:
        return None
