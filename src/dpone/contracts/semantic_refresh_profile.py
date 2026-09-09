"""Platform-owned activation policy for the bounded semantic-refresh cell."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from dpone.contracts.dbt_contract_validation import canonical_fingerprint

SEMANTIC_REFRESH_PROFILE_SCHEMA = "dpone.semantic-refresh-profile.v1"


@dataclass(frozen=True, slots=True)
class SemanticRefreshProfilePolicy:
    """Closed platform policy; dbt authors cannot construct or override it."""

    profile_sha256: str
    policy: Mapping[str, object]

    def __post_init__(self) -> None:
        normalized = _normalize(self.policy)
        _validate(normalized)
        expected = canonical_fingerprint(normalized)
        if self.profile_sha256 != expected:
            raise ValueError("semantic refresh profile digest differs from its policy")
        object.__setattr__(self, "policy", MappingProxyType(normalized))

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> SemanticRefreshProfilePolicy:
        """Validate and bind one explicit profile without production defaults."""

        normalized = _normalize(value)
        _validate(normalized)
        return cls(canonical_fingerprint(normalized), normalized)

    def to_jsonable(self) -> dict[str, object]:
        """Return a detached JSON-compatible policy mapping."""

        return dict(self.policy)


def _normalize(value: Mapping[str, Any]) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError("semantic refresh profile must be an object")
    return {str(key): _normalize_value(item) for key, item in value.items()}


def _normalize_value(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _normalize_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_value(item) for item in value]
    return value


def _validate(value: Mapping[str, object]) -> None:
    expected: dict[str, object] = {
        "schema": SEMANTIC_REFRESH_PROFILE_SCHEMA,
        "enabled": True,
        "capability": "scope_stable_event_fact",
        "scope": {
            "grain": "day",
            "timezone": "UTC",
            "interval": "half_open",
        },
        "mutation": {
            "protocol": "update_insert_v1",
            "deletes": "ignore_missing",
        },
        "initial_load": "require_existing_complete_relation",
        "concurrency": "exclusive_workflow",
        "source_snapshot": "snapshot",
        "publication": {
            "database_engine": "Atomic",
            "table_engine": "MergeTree",
            "replica_count": 1,
            "strategy": "full_table_exchange",
        },
        "workflow_publish_atomicity": "none",
        "automatic_sql_retry": False,
    }
    if value != expected:
        raise ValueError("semantic refresh profile must equal the closed V2.0 capability cell")


__all__ = ["SEMANTIC_REFRESH_PROFILE_SCHEMA", "SemanticRefreshProfilePolicy"]
