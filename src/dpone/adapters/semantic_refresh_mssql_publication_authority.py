"""In-transaction canonical target/scope authority for MSSQL publication state."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, Protocol

from dpone.adapters.semantic_refresh_mssql_publication_authority_document import (
    authenticate_canonical_authority_document,
)

PUBLICATION_AUTHORITY_FIELDS = frozenset(
    {
        "publication_authority_sha256",
        "target_resource_id",
        "target_authority_id",
        "clickhouse_cluster_authority_id",
        "workflow_execution_id",
        "target_predecessor_generation_id",
        "scope_predecessor_operation_id",
        "predecessor_target_generation",
        "predecessor_target_uuid",
        "predecessor_target_operation_id",
        "predecessor_scope_revision",
        "predecessor_checkpoint_sha256",
        "predecessor_checkpoint_operation_id",
        "predecessor_checkpoint_version",
        "database",
        "target_table",
        "scope_id",
        "scope_revision",
    }
)


class _Cursor(Protocol):
    def execute(self, sql: str, *parameters: object) -> _Cursor: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...


class MssqlPublicationCanonicalAuthority:
    """Lock and compare the authenticated operation/target/scope projection."""

    def __init__(
        self,
        table: Callable[[str], str],
        *,
        required: bool,
    ) -> None:
        if type(required) is not bool:
            raise TypeError("required must be a boolean")
        self._table = table
        self.required = required

    def assert_execution_and_guard(
        self,
        cursor: _Cursor,
        request: Mapping[str, object],
        journal: tuple[Any, ...],
    ) -> None:
        """Lock and authenticate the admitted run plus its active write fence."""

        cursor.execute(
            f"""
SELECT workflow_execution_id, workflow_plan_sha256, workflow_execution_binding_sha256
FROM {self._table("semantic_refresh_workflow_executions")} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_id = ?;
""".strip(),
            str(journal[6]),
        )
        row = cursor.fetchone()
        if row is None or tuple(row) != (
            request["workflow_execution_id"],
            request.get("workflow_plan_sha256", row[1]),
            request["workflow_execution_binding_sha256"],
        ):
            raise ValueError("workflow execution authority differs")
        cursor.execute(
            f"""
SELECT operation_id, attempt_binding_sha256, fencing_epoch, status
FROM {self._table("semantic_refresh_guards")} WITH (UPDLOCK, HOLDLOCK)
WHERE resource_id = ?;
""".strip(),
            journal[7],
        )
        row = cursor.fetchone()
        if row is None or tuple(row) != (
            request["operation_id"],
            request["attempt_binding_sha256"],
            request["fence_epoch"],
            "HELD",
        ):
            raise ValueError("publication fence authority differs")

    def assert_authorized(
        self,
        cursor: _Cursor,
        request: Mapping[str, object],
        journal: tuple[Any, ...],
    ) -> None:
        protected_identity = {
            "publication_authority_sha256",
            "target_resource_id",
            "target_authority_id",
        }.intersection(request)
        if not protected_identity:
            if self.required:
                raise ValueError("protected publication authority is required")
            return
        present = PUBLICATION_AUTHORITY_FIELDS.intersection(request)
        if present != PUBLICATION_AUTHORITY_FIELDS:
            raise ValueError("protected publication authority fields are incomplete")
        cursor.execute(
            f"""
SELECT authority.workflow_execution_id, authority.authority_sha256, authority.authority_json,
       operation.operation_plan_sha256,
       JSON_VALUE(authority.authority_json, '$.workflow_plan.workflow_plan_sha256'),
       resource.target_resource_id, resource.target_authority_id,
       resource.clickhouse_cluster_authority_id,
       resource.publication_database, resource.publication_target_table,
       resource.publication_scope_id, operation.scope_revision,
       operation.target_predecessor_generation_id, operation.scope_predecessor_operation_id
FROM {self._table("semantic_refresh_canonical_authorities")} AS authority WITH (UPDLOCK, HOLDLOCK)
CROSS APPLY OPENJSON(authority.authority_json, '$.operation_plans') WITH (
    operation_id nvarchar(512) '$.operation_id',
    operation_plan_sha256 varchar(71) '$.operation_plan_sha256',
    model_unique_id nvarchar(512) '$.model_unique_id',
    scope_revision bigint '$.scope_revision'
    ,target_predecessor_generation_id varchar(71) '$.target_predecessor_generation_id'
    ,scope_predecessor_operation_id nvarchar(512) '$.scope_predecessor_operation_id'
) AS operation
CROSS APPLY OPENJSON(authority.authority_json, '$.model_resources') WITH (
    model_unique_id nvarchar(512) '$.model_unique_id',
    target_resource_id nvarchar(512) '$.target_resource_id',
    target_authority_id nvarchar(512) '$.target_authority_id',
    clickhouse_cluster_authority_id nvarchar(512) '$.clickhouse_cluster_authority_id',
    publication_database nvarchar(128) '$.publication_database',
    publication_target_table nvarchar(128) '$.publication_target_table',
    publication_scope_id nvarchar(512) '$.publication_scope_id'
) AS resource
WHERE authority.workflow_execution_binding_sha256 = ?
  AND authority.status = N'ACTIVE'
  AND operation.operation_id = ?
  AND resource.model_unique_id = operation.model_unique_id;
""".strip(),
            request["workflow_execution_binding_sha256"],
            request["operation_id"],
        )
        row = cursor.fetchone()
        if row is None or cursor.fetchone() is not None:
            raise ValueError("protected publication authority is absent or ambiguous")
        authenticate_canonical_authority_document(
            workflow_execution_id=row[0],
            workflow_execution_binding_sha256=request["workflow_execution_binding_sha256"],
            authority_sha256=row[1],
            authority_json=row[2],
        )
        expected = (
            row[0],
            request["publication_authority_sha256"],
            row[2],
            request["operation_plan_sha256"],
            request["workflow_plan_sha256"],
            request["target_resource_id"],
            request["target_authority_id"],
            request["clickhouse_cluster_authority_id"],
            request["database"],
            request["target_table"],
            request["scope_id"],
            request["scope_revision"],
        )
        if tuple(row[:12]) != expected or journal[7] != request["target_resource_id"]:
            raise ValueError("protected publication target/scope authority differs")
        if (row[0], row[12], row[13]) != (
            request["workflow_execution_id"],
            request["target_predecessor_generation_id"],
            request["scope_predecessor_operation_id"],
        ):
            raise ValueError("protected publication predecessor authority differs")


__all__ = [
    "MssqlPublicationCanonicalAuthority",
    "PUBLICATION_AUTHORITY_FIELDS",
]
