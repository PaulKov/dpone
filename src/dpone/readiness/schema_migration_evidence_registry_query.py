"""Query helpers for schema migration evidence registry stores."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


def target_key(record: Mapping[str, Any]) -> str:
    target = record.get("target", {})
    if not isinstance(target, Mapping):
        return ""
    return f"{target.get('sink_type')}.{target.get('table')}"


def record_matches(record: Mapping[str, Any], query: Any) -> bool:
    if query.target and target_key(record) != query.target:
        return False
    if query.environment and record.get("environment") != query.environment:
        return False
    if query.stage and record.get("stage") != query.stage:
        return False
    if query.status and record.get("status") != query.status:
        return False
    recorded = str(record.get("recorded_at", ""))
    if query.date_from and recorded[:10] < query.date_from:
        return False
    return not (query.date_to and recorded[:10] > query.date_to)


def logical_key(record: Mapping[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(record.get("environment", "")),
        str(record.get("stage", "")),
        str(record.get("pack_id", "")),
        str(record.get("bundle_id", "")),
    )


def sort_records(records: Sequence[Mapping[str, Any]]) -> tuple[dict[str, Any], ...]:
    return tuple(
        sorted(
            (dict(item) for item in records),
            key=lambda item: (str(item.get("recorded_at")), str(item.get("record_id"))),
        )
    )


__all__ = ["logical_key", "record_matches", "sort_records", "target_key"]
