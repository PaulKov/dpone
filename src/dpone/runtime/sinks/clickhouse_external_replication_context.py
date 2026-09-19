"""Pure staged-lifecycle context for external ClickHouse publication."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, fields, is_dataclass
from enum import Enum
from pathlib import Path
from typing import Any

from dpone.ports.clickhouse_external_replication import (
    ExternalPublicationRequest,
    ExternalReplicationReceipt,
    canonical_json,
    digest_payload,
)


@dataclass(frozen=True, slots=True)
class ExternalStagedContext:
    """Carry only redacted immutable identity across governance boundaries."""

    request: ExternalPublicationRequest
    staged_receipt: ExternalReplicationReceipt
    candidate_name: str
    content_row_budget: int = 100_000

    def __post_init__(self) -> None:
        if self.staged_receipt.phase != "STAGED":
            raise ValueError("external staged context requires STAGED authority")
        if self.request.operation_id != self.staged_receipt.operation_id:
            raise ValueError("external staged context operation differs")
        if self.request.generation_id != self.staged_receipt.generation_id:
            raise ValueError("external staged context generation differs")
        if not self.candidate_name:
            raise ValueError("external staged context candidate is missing")
        if isinstance(self.content_row_budget, bool) or self.content_row_budget <= 0:
            raise ValueError("external staged context row budget must be positive")


@dataclass(frozen=True, slots=True)
class ExternalStagedValidation:
    """Bind validation to the exact authority version and logical generation."""

    operation_id: str
    generation_id: str
    authority_version: int


def derive_semantic_plan_digest(
    load_config: Any,
    *,
    cluster: str,
    database: str,
    target: str,
    runtime_option_keys: frozenset[str] = frozenset(),
) -> str:
    """Bind operation identity to normalized source, target, and transformation semantics."""

    if not is_dataclass(load_config):
        raise TypeError("clickhouse external replication requires a dataclass load configuration")
    projected = {
        field.name: _stable_plan_value(getattr(load_config, field.name))
        for field in fields(load_config)
        if field.name != "options"
    }
    options = {
        str(key): _stable_plan_value(value)
        for key, value in _mapping(getattr(load_config, "options", None)).items()
        if key not in runtime_option_keys
    }
    return digest_payload(
        {
            "version": 2,
            "resolved_target": {"cluster": cluster, "database": database, "table": target},
            "load_config": projected,
            "options": options,
        }
    )


def _stable_plan_value(value: Any) -> Any:
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, Enum):
        return _stable_plan_value(value.value)
    if isinstance(value, Path):
        return str(value)
    if is_dataclass(value):
        return {field.name: _stable_plan_value(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, Mapping):
        return {str(key): _stable_plan_value(item) for key, item in value.items()}
    if isinstance(value, tuple | list):
        return [_stable_plan_value(item) for item in value]
    if isinstance(value, set | frozenset):
        normalized = [_stable_plan_value(item) for item in value]
        return sorted(normalized, key=canonical_json)
    raise TypeError(f"unsupported clickhouse external plan value: {type(value).__name__}")


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


__all__ = ["ExternalStagedContext", "ExternalStagedValidation", "derive_semantic_plan_digest"]
