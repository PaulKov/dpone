"""Transactional create-only MSSQL activation store for semantic refresh V2."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import TYPE_CHECKING

from dpone.adapters.semantic_refresh_mssql_activation_support import (
    MssqlActivatedPackQueries,
    MssqlActivatedPackRow,
    SemanticRefreshMssqlActivationError,
)
from dpone.adapters.semantic_refresh_mssql_activation_transaction import (
    ActivationConnection as _Connection,
)
from dpone.adapters.semantic_refresh_mssql_activation_transaction import (
    ActivationCursor as _Cursor,
)
from dpone.adapters.semantic_refresh_mssql_activation_transaction import (
    MssqlSemanticRefreshActivationTransactionMixin,
)
from dpone.adapters.semantic_refresh_mssql_activation_transaction import (
    row as _row,
)
from dpone.ports.semantic_refresh_mssql_worker_pack import (
    mssql_static_projection_identity_json,
)

if TYPE_CHECKING:
    from dpone.ports.semantic_refresh_mssql_activation import (
        MssqlActivatedPackRegistration,
        MssqlDeploymentActivationRequest,
        MssqlRunAuthorityRegistration,
    )


class MssqlSemanticRefreshActivationStore(MssqlSemanticRefreshActivationTransactionMixin):
    """Persist activation prerequisites without update-or-replace semantics."""

    def __init__(
        self,
        connection_factory: Callable[[], _Connection],
        *,
        control_schema: str = "dpone_control",
    ) -> None:
        if not control_schema.replace("_", "a").isalnum() or not control_schema[0].isalpha():
            raise ValueError("control_schema must be a simple SQL identifier")
        self._connection_factory = connection_factory
        self._control_schema = control_schema
        self._activated_packs = MssqlActivatedPackQueries(control_schema)

    def activate_deployment(self, request: MssqlDeploymentActivationRequest) -> None:
        """Install baselines, owners, initial heads, and guards atomically."""

        self._transaction(
            f"deployment:{request.deployment_id}",
            lambda cursor: self._activate_deployment(cursor, request),
        )

    def register_run_authority(self, request: MssqlRunAuthorityRegistration) -> None:
        """Persist one ACTIVE authority with exact guard predecessor epochs."""

        self._transaction(
            f"authority:{request.record.workflow_execution_binding_sha256}",
            lambda cursor: self._register_run_authority(cursor, request),
        )

    def register_activated_pack(self, request: MssqlActivatedPackRegistration) -> None:
        """Persist one run-bound activated pack without replacement semantics."""

        self._transaction(
            f"activated-pack:{request.pack_fingerprint}",
            lambda cursor: self.register_activated_pack_in_transaction(cursor, request),
        )

    def register_activated_pack_in_transaction(
        self,
        cursor: _Cursor,
        request: MssqlActivatedPackRegistration,
    ) -> None:
        """Create/replay one run pack on an owner SERIALIZABLE transaction."""

        closure = request.run_guard_closure
        self._activated_packs.register(
            cursor,
            MssqlActivatedPackRow(
                pack_fingerprint=request.pack_fingerprint,
                activation_authority_receipt_sha256=(request.activation_authority_receipt_sha256),
                authority_store_ref=request.authority_store_ref,
                workflow_execution_id=request.workflow_execution_id,
                workflow_execution_binding_sha256=(request.workflow_execution_binding_sha256),
                workflow_plan_sha256=request.workflow_plan_sha256,
                plan_bundle_sha256=request.plan_bundle_sha256,
                run_execution_bundle_sha256=request.run_execution_bundle_sha256,
                run_guard_closure_sha256=closure.run_guard_closure_sha256,
                workflow_guard_resource_id=closure.workflow_guard_resource_id,
                resource_guard_ids_json=json.dumps(
                    closure.resource_guard_ids,
                    ensure_ascii=True,
                    separators=(",", ":"),
                ),
                static_projection_identity_json=mssql_static_projection_identity_json(request.projection_identity),
            ),
        )

    def prepare_run_guards_in_transaction(
        self,
        cursor: _Cursor,
        request: MssqlActivatedPackRegistration,
    ) -> None:
        """Create the workflow guard and prove all target guards before admission."""

        closure = request.run_guard_closure
        self._ensure_pack_guard(
            cursor,
            closure.workflow_guard_resource_id,
            allow_create=True,
        )
        for resource_id in closure.resource_guard_ids:
            self._ensure_pack_guard(cursor, resource_id, allow_create=False)

    def _ensure_pack_guard(
        self,
        cursor: _Cursor,
        resource_id: str,
        *,
        allow_create: bool,
    ) -> None:
        cursor.execute(
            f"""
SELECT fencing_epoch, owner_id, workflow_id, operation_id, status
FROM {self._table("semantic_refresh_guards")} WITH (UPDLOCK, HOLDLOCK)
WHERE resource_id = ?;
""".strip(),
            resource_id,
        )
        existing = _row(cursor)
        if existing is None and allow_create:
            cursor.execute(
                f"""
INSERT INTO {self._table("semantic_refresh_guards")} (
    resource_id, fencing_epoch, owner_id, workflow_id, operation_id, status
) VALUES (?, 0, NULL, NULL, NULL, N'AVAILABLE');
""".strip(),
                resource_id,
            )
            return
        if existing is None:
            raise SemanticRefreshMssqlActivationError("activated pack resource guard is absent")
        epoch, owner_id, workflow_id, _operation_id, status = existing
        if (
            isinstance(epoch, bool)
            or not isinstance(epoch, int)
            or epoch < 0
            or status not in {"AVAILABLE", "RELEASED"}
            or (status == "AVAILABLE" and (owner_id is not None or workflow_id is not None))
            or (
                status == "RELEASED"
                and (
                    not isinstance(owner_id, str)
                    or not owner_id.strip()
                    or not isinstance(workflow_id, str)
                    or not workflow_id.strip()
                )
            )
        ):
            raise SemanticRefreshMssqlActivationError("activated pack guard is not admissible")

    def _activate_deployment(self, cursor: _Cursor, request: MssqlDeploymentActivationRequest) -> None:
        cursor.execute(
            f"""
SELECT deployment_subject_sha256, status
FROM {self._table("semantic_refresh_deployment_activations")} WITH (UPDLOCK, HOLDLOCK)
WHERE deployment_id = ?;
""".strip(),
            request.deployment_id,
        )
        existing = _row(cursor)
        if existing is not None:
            if existing != (request.deployment_subject_sha256, "ACTIVE"):
                raise SemanticRefreshMssqlActivationError("deployment activation replay differs")
            return
        for baseline in request.baselines:
            self._insert_absent_exact(
                cursor,
                select_sql=f"""
SELECT model_unique_id, baseline_kind, baseline_receipt_sha256,
       baseline_receipt_json, status, is_current
FROM {self._table("semantic_refresh_baselines")} WITH (UPDLOCK, HOLDLOCK)
WHERE target_resource_id = ?;
""".strip(),
                select_parameters=(baseline.target_resource_id,),
                expected=(
                    baseline.model_unique_id,
                    baseline.baseline_kind,
                    baseline.baseline_receipt_sha256,
                    baseline.baseline_receipt_json,
                    "COMPLETE",
                    True,
                ),
                insert_sql=f"""
INSERT INTO {self._table("semantic_refresh_baselines")} (
    target_resource_id, model_unique_id, baseline_kind,
    baseline_receipt_sha256, baseline_receipt_json, status, is_current
) VALUES (?, ?, ?, ?, ?, N'COMPLETE', 1);
""".strip(),
                insert_parameters=(
                    baseline.target_resource_id,
                    baseline.model_unique_id,
                    baseline.baseline_kind,
                    baseline.baseline_receipt_sha256,
                    baseline.baseline_receipt_json,
                ),
                label="baseline",
            )
        for owner in request.target_owners:
            self._insert_absent_exact(
                cursor,
                select_sql=f"""
SELECT model_unique_id, deployment_id, owner_generation, status
FROM {self._table("semantic_refresh_target_owners")} WITH (UPDLOCK, HOLDLOCK)
WHERE target_authority_id = ?;
""".strip(),
                select_parameters=(owner.target_authority_id,),
                expected=(owner.model_unique_id, owner.deployment_id, owner.owner_generation, "ACTIVE"),
                insert_sql=f"""
INSERT INTO {self._table("semantic_refresh_target_owners")} (
    target_authority_id, model_unique_id, deployment_id, owner_generation, status
) VALUES (?, ?, ?, ?, N'ACTIVE');
""".strip(),
                insert_parameters=(
                    owner.target_authority_id,
                    owner.model_unique_id,
                    owner.deployment_id,
                    owner.owner_generation,
                ),
                label="target owner",
            )
        for head in request.target_heads:
            self._insert_absent_exact(
                cursor,
                select_sql=f"""
SELECT target_generation, target_generation_id, target_uuid, operation_id
FROM {self._table("semantic_refresh_target_heads")} WITH (UPDLOCK, HOLDLOCK)
WHERE database_name = ? AND target_table = ?;
""".strip(),
                select_parameters=(head.database_name, head.target_table),
                expected=(
                    head.target_generation,
                    head.target_generation_id,
                    head.target_uuid,
                    head.baseline_operation_id,
                ),
                insert_sql=f"""
INSERT INTO {self._table("semantic_refresh_target_heads")} (
    database_name, target_table, target_generation, target_generation_id,
    target_uuid, operation_id
) VALUES (?, ?, ?, ?, ?, ?);
""".strip(),
                insert_parameters=(
                    head.database_name,
                    head.target_table,
                    head.target_generation,
                    head.target_generation_id,
                    head.target_uuid,
                    head.baseline_operation_id,
                ),
                label="target head",
            )
        for resource_id in request.target_guard_resource_ids:
            self._insert_guard(cursor, resource_id, 0)
        cursor.execute(
            f"""
INSERT INTO {self._table("semantic_refresh_deployment_activations")} (
    deployment_id, deployment_subject_sha256, status
) VALUES (?, ?, N'ACTIVE');
""".strip(),
            request.deployment_id,
            request.deployment_subject_sha256,
        )

    def _register_run_authority(self, cursor: _Cursor, request: MssqlRunAuthorityRegistration) -> None:
        record = request.record
        cursor.execute(
            f"""
SELECT workflow_execution_id, authority_sha256, authority_json, status
FROM {self._table("semantic_refresh_canonical_authorities")} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_execution_binding_sha256 = ?;
""".strip(),
            record.workflow_execution_binding_sha256,
        )
        existing = _row(cursor)
        expected = (
            record.workflow_execution_id,
            record.authority_sha256,
            record.authority_json,
            "ACTIVE",
        )
        if existing is not None:
            if existing != expected:
                raise SemanticRefreshMssqlActivationError("canonical run authority replay differs")
            return
        for resource_id, epoch in request.guard_epochs:
            self._insert_guard(cursor, resource_id, epoch)
        cursor.execute(
            f"""
INSERT INTO {self._table("semantic_refresh_canonical_authorities")} (
    workflow_execution_binding_sha256, workflow_execution_id,
    authority_sha256, authority_json, status
) VALUES (?, ?, ?, ?, N'ACTIVE');
""".strip(),
            record.workflow_execution_binding_sha256,
            record.workflow_execution_id,
            record.authority_sha256,
            record.authority_json,
        )

    def _insert_guard(self, cursor: _Cursor, resource_id: str, epoch: int) -> None:
        cursor.execute(
            f"""
SELECT fencing_epoch, owner_id, workflow_id, operation_id, status
FROM {self._table("semantic_refresh_guards")} WITH (UPDLOCK, HOLDLOCK)
WHERE resource_id = ?;
""".strip(),
            resource_id,
        )
        existing = _row(cursor)
        if existing is None:
            cursor.execute(
                f"""
INSERT INTO {self._table("semantic_refresh_guards")} (
    resource_id, fencing_epoch, owner_id, workflow_id, operation_id, status
) VALUES (?, ?, NULL, NULL, NULL, N'AVAILABLE');
""".strip(),
                resource_id,
                epoch,
            )
            return
        actual_epoch, owner_id, workflow_id, _operation_id, status = existing
        available = existing == (epoch, None, None, None, "AVAILABLE")
        released = (
            actual_epoch == epoch
            and status == "RELEASED"
            and isinstance(owner_id, str)
            and bool(owner_id.strip())
            and isinstance(workflow_id, str)
            and bool(workflow_id.strip())
        )
        if not (available or released):
            raise SemanticRefreshMssqlActivationError("guard predecessor authority differs")

    def _table(self, table_name: str) -> str:
        return f"[{self._control_schema}].[{table_name}]"


__all__ = ["MssqlSemanticRefreshActivationStore", "SemanticRefreshMssqlActivationError"]
