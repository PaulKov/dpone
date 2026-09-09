"""Durable document boundary between ClickHouse PREPARE and COMMIT tasks."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Protocol

_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")


@dataclass(frozen=True, slots=True)
class DurableClickHousePreparedPublication:
    """Create-once canonical plan, receipt, and version-pinned manifest documents."""

    workflow_execution_binding_sha256: str
    operation_id: str
    prepare_plan_sha256: str
    prepare_plan_json: str
    prepared_receipt_sha256: str
    prepared_receipt_json: str
    artifact_manifest_key: str
    artifact_manifest_version: str
    artifact_manifest_sha256: str

    def __post_init__(self) -> None:
        for field_name in (
            "workflow_execution_binding_sha256",
            "operation_id",
            "prepare_plan_sha256",
            "prepared_receipt_sha256",
            "artifact_manifest_sha256",
        ):
            if _DIGEST.fullmatch(getattr(self, field_name)) is None:
                raise ValueError(f"durable PREPARED {field_name} is invalid")
        for field_name in (
            "prepare_plan_json",
            "prepared_receipt_json",
            "artifact_manifest_key",
            "artifact_manifest_version",
        ):
            if not getattr(self, field_name).strip():
                raise ValueError(f"durable PREPARED {field_name} is empty")


class SemanticRefreshClickHousePreparedPublicationPort(Protocol):
    """Load one create-once PREPARED boundary by protected run identity."""

    def load_prepared(
        self,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> DurableClickHousePreparedPublication:
        """Return exact durable documents or fail closed."""

    def find_prepared(
        self,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> DurableClickHousePreparedPublication | None:
        """Return exact documents, or ``None`` only for a locked PREPARING row."""


__all__ = [
    "DurableClickHousePreparedPublication",
    "SemanticRefreshClickHousePreparedPublicationPort",
]
