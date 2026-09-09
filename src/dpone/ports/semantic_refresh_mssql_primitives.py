"""Validated primitive claims and canonical hashes for MSSQL admission."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

_SHA256_PREFIX = "sha256:"


def _require_text(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")


def _require_digest(value: str, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or not value.startswith(_SHA256_PREFIX)
        or len(value) != len(_SHA256_PREFIX) + 64
        or any(character not in "0123456789abcdef" for character in value[len(_SHA256_PREFIX) :])
    ):
        raise ValueError(f"{field_name} must be a lowercase sha256 digest")


def _require_non_negative(value: int, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")


def mssql_strategy_authority_sha256(canonical_json: str) -> str:
    """Hash the exact NVARCHAR payload used by SQL Server ``HASHBYTES``."""

    _require_text(canonical_json, "canonical_json")
    return _SHA256_PREFIX + hashlib.sha256(canonical_json.encode("utf-16le")).hexdigest()


@dataclass(frozen=True, slots=True)
class MssqlGuardClaim:
    """One compare-and-set guard epoch requested during atomic admission."""

    resource_id: str
    expected_predecessor_epoch: int
    fencing_epoch: int

    def __post_init__(self) -> None:
        _require_text(self.resource_id, "resource_id")
        _require_non_negative(self.expected_predecessor_epoch, "expected_predecessor_epoch")
        if self.fencing_epoch != self.expected_predecessor_epoch + 1:
            raise ValueError("fencing_epoch must immediately succeed expected_predecessor_epoch")


def mssql_guard_set_sha256(
    workflow_guard: MssqlGuardClaim,
    resource_guards: tuple[MssqlGuardClaim, ...],
) -> str:
    """Hash the canonical workflow/resource guard closure."""

    payload = {
        "resource_ids": [item.resource_id for item in resource_guards],
        "schema": "dpone.semantic-refresh-mssql-guard-set.v1",
        "workflow_resource_id": workflow_guard.resource_id,
    }
    raw = json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
    return _SHA256_PREFIX + hashlib.sha256(raw).hexdigest()


@dataclass(frozen=True, slots=True)
class MssqlImageKeyColumn:
    """One authenticated image ordering key and its SQL Server encoding."""

    name: str
    order_encoding: str

    def __post_init__(self) -> None:
        _require_text(self.name, "image key name")
        if "\x00" in self.name:
            raise ValueError("image key name contains an unsupported NUL character")
        if self.order_encoding not in {"NATIVE", "UUID_TEXT"}:
            raise ValueError("image key order_encoding is unsupported")


@dataclass(frozen=True, slots=True)
class MssqlWorkflowResourceBudget:
    """Protected aggregate limits reserved before workflow allocations."""

    max_workflow_prepared_models: int
    max_workflow_sealed_extract_bytes: int
    max_workflow_clickhouse_staging_bytes: int
    max_workflow_shadow_bytes: int
    max_workflow_peak_bytes: int

    def __post_init__(self) -> None:
        for field_name in self.__dataclass_fields__:
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{field_name} must be a positive integer")


@dataclass(frozen=True, order=True, slots=True)
class MssqlRuntimeAssuranceClaim:
    """Exact protected runtime-assurance document expected at admission."""

    assurance_kind: str
    receipt_sha256: str
    subject_json: str

    def __post_init__(self) -> None:
        if self.assurance_kind not in {"ddl_freeze", "utc_semantics", "writer_exclusivity"}:
            raise ValueError("runtime assurance kind is unsupported")
        _require_digest(self.receipt_sha256, "runtime assurance receipt_sha256")
        _require_text(self.subject_json, "runtime assurance subject_json")


@dataclass(frozen=True, order=True, slots=True)
class MssqlPrerequisiteAuthorityClaim:
    """Deployment-bound receipt closure rechecked inside admission."""

    release_id: str
    deployment_id: str
    model_unique_id: str
    route_certification_receipt_sha256: str
    runtime_assurances: tuple[MssqlRuntimeAssuranceClaim, ...]

    def __post_init__(self) -> None:
        if any(not isinstance(item, MssqlRuntimeAssuranceClaim) for item in self.runtime_assurances):
            raise ValueError("runtime assurance claims must be typed")
        _require_digest(self.release_id, "release_id")
        _require_digest(self.deployment_id, "deployment_id")
        _require_text(self.model_unique_id, "model_unique_id")
        _require_digest(
            self.route_certification_receipt_sha256,
            "route_certification_receipt_sha256",
        )
        kinds = tuple(item.assurance_kind for item in self.runtime_assurances)
        if (
            not self.runtime_assurances
            or kinds != tuple(sorted(set(kinds)))
            or not {"ddl_freeze", "writer_exclusivity"}.issubset(set(kinds))
        ):
            raise ValueError("runtime assurance claims must be a canonical complete closure")


def mssql_image_key_columns_json(columns: tuple[MssqlImageKeyColumn, ...]) -> str:
    """Serialize the authenticated SQL Server image ordering specification."""

    if (
        not isinstance(columns, tuple)
        or not columns
        or any(not isinstance(item, MssqlImageKeyColumn) for item in columns)
        or len({item.name for item in columns}) != len(columns)
    ):
        raise ValueError("image key columns are invalid")
    return json.dumps(
        [{"name": item.name, "order_encoding": item.order_encoding} for item in columns],
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


__all__ = [
    "MssqlGuardClaim",
    "MssqlImageKeyColumn",
    "MssqlPrerequisiteAuthorityClaim",
    "MssqlRuntimeAssuranceClaim",
    "MssqlWorkflowResourceBudget",
    "mssql_guard_set_sha256",
    "mssql_image_key_columns_json",
    "mssql_strategy_authority_sha256",
]
