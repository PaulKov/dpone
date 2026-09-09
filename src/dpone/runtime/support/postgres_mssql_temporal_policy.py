"""PostgreSQL timestamptz fidelity policy at the pre-COPY boundary."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone.runtime.support.temporal_fidelity import TemporalFidelityPolicy, is_offset_timestamp_type


def configured_temporal_blocker(
    relation_schema: Sequence[tuple[str, str]],
    load_config: Any,
) -> tuple[str, str] | None:
    """Return a blocker when a configured policy cannot be proved by COPY."""

    options = getattr(load_config, "options", {}) or {}
    raw = options.get("type_fidelity") if isinstance(options, Mapping) else None
    if not _has_explicit_offset_policy(raw):
        return None
    policy = TemporalFidelityPolicy.from_config(raw)
    for column, dtype in relation_schema:
        if not is_offset_timestamp_type(dtype):
            continue
        effective = policy.for_column(str(column))
        reason = (
            "preserve_offset_original_offset_unavailable"
            if effective.offset_timestamp_mode == "preserve_offset"
            else f"temporal_policy_unsupported.{effective.offset_timestamp_mode}"
        )
        return f"postgres_mssql.type_contract.{reason}", str(column)
    return None


def _has_explicit_offset_policy(raw: Any) -> bool:
    if not isinstance(raw, Mapping):
        return False
    if any(key in raw for key in ("datetimeoffset", "datetimeoffset_mode", "datetimeoffset_timezone")):
        return True
    temporal = raw.get("temporal")
    return isinstance(temporal, Mapping) and "offset_timestamp" in temporal


__all__ = ["configured_temporal_blocker"]
