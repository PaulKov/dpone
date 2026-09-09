"""Kafka bounded batch offset planning and state."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol

from dpone.runtime.kafka.config import KafkaReadMode, KafkaSourceOptions


@dataclass(frozen=True)
class KafkaOffsetState:
    topic: str
    group_id: str
    partition_offsets: dict[int, int]
    high_watermarks: dict[int, int] = field(default_factory=dict)
    read_mode: str = "offsets"
    started_at: datetime | None = None
    completed_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "topic": self.topic,
            "group_id": self.group_id,
            "partition_offsets": {str(k): v for k, v in self.partition_offsets.items()},
            "high_watermarks": {str(k): v for k, v in self.high_watermarks.items()},
            "read_mode": self.read_mode,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any] | None) -> KafkaOffsetState | None:
        if not raw:
            return None
        return cls(
            topic=str(raw["topic"]),
            group_id=str(raw["group_id"]),
            partition_offsets={int(k): int(v) for k, v in (raw.get("partition_offsets") or {}).items()},
            high_watermarks={int(k): int(v) for k, v in (raw.get("high_watermarks") or {}).items()},
            read_mode=str(raw.get("read_mode", "offsets")),
            started_at=_parse_dt(raw.get("started_at")),
            completed_at=_parse_dt(raw.get("completed_at")),
        )


@dataclass(frozen=True)
class KafkaBatchWindow:
    topic: str
    group_id: str
    start_offsets: dict[int, int]
    end_offsets: dict[int, int]
    read_mode: str

    def to_state(self) -> KafkaOffsetState:
        now = datetime.now(UTC)
        return KafkaOffsetState(
            topic=self.topic,
            group_id=self.group_id,
            partition_offsets=dict(self.end_offsets),
            high_watermarks=dict(self.end_offsets),
            read_mode=self.read_mode,
            started_at=now,
            completed_at=now,
        )


class KafkaOffsetStateStorage(Protocol):
    def load_state(self, topic: str, group_id: str) -> KafkaOffsetState | None: ...

    def save_state(self, state: KafkaOffsetState) -> None: ...


class InMemoryKafkaOffsetStateStorage:
    def __init__(self, state: KafkaOffsetState | None = None):
        self.state = state

    def load_state(self, topic: str, group_id: str) -> KafkaOffsetState | None:
        if self.state and self.state.topic == topic and self.state.group_id == group_id:
            return self.state
        return None

    def save_state(self, state: KafkaOffsetState) -> None:
        self.state = state


class KafkaBatchPlanner:
    """Plans a fixed Kafka batch window before consumption starts."""

    def __init__(self, connector: Any, state_storage: KafkaOffsetStateStorage | None = None):
        self.connector = connector
        self.state_storage = state_storage or InMemoryKafkaOffsetStateStorage()

    def plan(self, topic: str, options: KafkaSourceOptions) -> KafkaBatchWindow:
        group_id = options.resolved_group_id(topic)
        partitions = list(self.connector.topic_partitions(topic))
        high_watermarks = {partition: int(self.connector.watermarks(topic, partition)[1]) for partition in partitions}
        if options.read_mode == KafkaReadMode.TIME_WINDOW:
            start_ts = options.time_window_start_ms
            end_ts = options.time_window_end_ms
            if start_ts is None or end_ts is None:
                raise ValueError("Kafka time_window mode requires time_window_start_ms and time_window_end_ms")
            start_offsets = self.connector.offsets_for_times(topic, {partition: start_ts for partition in partitions})
            end_offsets = self.connector.offsets_for_times(topic, {partition: end_ts for partition in partitions})
            return KafkaBatchWindow(
                topic, group_id, _normalize_offsets(start_offsets), _normalize_offsets(end_offsets), "time_window"
            )
        start_offsets = self._start_offsets(topic, group_id, options, partitions)
        return KafkaBatchWindow(topic, group_id, start_offsets, high_watermarks, options.read_mode.value)

    def _start_offsets(
        self,
        topic: str,
        group_id: str,
        options: KafkaSourceOptions,
        partitions: list[int],
    ) -> dict[int, int]:
        stored = self.state_storage.load_state(topic, group_id) if options.offset_storage == "dpone" else None
        result: dict[int, int] = {}
        for partition in partitions:
            if stored and partition in stored.partition_offsets:
                result[partition] = int(stored.partition_offsets[partition])
            elif options.start_from == "latest":
                result[partition] = int(self.connector.watermarks(topic, partition)[1])
            else:
                result[partition] = int(self.connector.watermarks(topic, partition)[0])
        return result


def _normalize_offsets(raw: Mapping[int, int]) -> dict[int, int]:
    return {int(k): int(v) for k, v in raw.items()}


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))
