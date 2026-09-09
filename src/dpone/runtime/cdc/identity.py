"""CDC event identity and idempotency helpers."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol


class CDCChangeLike(Protocol):
    """Structural CDC change contract required for stable event identity."""

    source_schema: str
    source_table: str
    operation: object
    position: str | None
    transaction_id: str | None
    sequence: int | None
    data: Mapping[str, object]
    before: Mapping[str, object] | None


@dataclass(frozen=True, slots=True)
class CDCIdempotencyReport:
    total_events: int
    unique_events: int
    duplicate_event_ids: tuple[str, ...]

    @property
    def duplicate_count(self) -> int:
        return self.total_events - self.unique_events

    @property
    def passed(self) -> bool:
        return not self.duplicate_event_ids

    def to_dict(self) -> dict[str, object]:
        return {
            "passed": self.passed,
            "total_events": self.total_events,
            "unique_events": self.unique_events,
            "duplicate_count": self.duplicate_count,
            "duplicate_event_ids": list(self.duplicate_event_ids),
        }


class CDCEventIdentityService:
    """Build deterministic 64-char SHA-256 identities for CDC changes."""

    def event_id(self, change: CDCChangeLike, *, unique_key: Sequence[str] | None = None) -> str:
        payload = {
            "source_schema": change.source_schema,
            "source_table": change.source_table,
            "operation": _operation_value(change.operation),
            "position": change.position,
            "transaction_id": change.transaction_id,
            "sequence": change.sequence,
            "key": self._key_payload(change, unique_key),
            "row": {"data": dict(change.data), "before": dict(change.before or {})},
        }
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    @staticmethod
    def _key_payload(change: CDCChangeLike, unique_key: Sequence[str] | None) -> object:
        if not unique_key:
            return {"data": dict(change.data), "before": dict(change.before or {})}
        before = change.before or {}
        values: dict[str, object] = {}
        for key in unique_key:
            if key in change.data:
                values[key] = change.data[key]
            elif key in before:
                values[key] = before[key]
            else:
                values[key] = None
        return values


def _operation_value(operation: object) -> object:
    return getattr(operation, "value", operation)


class CDCIdempotencyService:
    """Evaluate whether a CDC batch contains duplicate event identities."""

    def __init__(self, *, identity_service: CDCEventIdentityService | None = None) -> None:
        self._identity_service = identity_service or CDCEventIdentityService()

    def evaluate(
        self,
        changes: Iterable[CDCChangeLike],
        *,
        unique_key: Sequence[str] | None = None,
    ) -> CDCIdempotencyReport:
        seen: set[str] = set()
        duplicates: list[str] = []
        total = 0
        for change in changes:
            total += 1
            event_id = self._identity_service.event_id(change, unique_key=unique_key)
            if event_id in seen and event_id not in duplicates:
                duplicates.append(event_id)
            seen.add(event_id)
        return CDCIdempotencyReport(
            total_events=total,
            unique_events=len(seen),
            duplicate_event_ids=tuple(duplicates),
        )
