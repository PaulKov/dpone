"""Application services for protected MSSQL admission and operation authority."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING

from dpone.ports.semantic_refresh_mssql import (
    MssqlAdmissionReceipt,
    SemanticRefreshMssqlAdmissionPort,
    mssql_strategy_authority_sha256,
)
from dpone.ports.semantic_refresh_mssql_admission_authority import (
    compose_admission,
    mssql_prerequisite_authority,
)
from dpone.ports.semantic_refresh_mssql_authority import (
    MssqlProtectedOperationAuthority,
    MssqlProtectedOperationStateRecord,
    SemanticRefreshMssqlCanonicalAuthorityPort,
    SemanticRefreshMssqlProtectedOperationStatePort,
    SemanticRefreshMssqlProtectedPlanStatePort,
)
from dpone.ports.semantic_refresh_mssql_authority_codec import (
    authority_from_record,
)
from dpone.ports.semantic_refresh_mssql_authority_models import (
    MssqlCanonicalAdmissionBundle,
    MssqlModelResourceAuthority,
    mssql_model_resource_authority_from_plan_target,
)
from dpone.ports.semantic_refresh_mssql_authority_validation import (
    validate_runtime_operation_state,
)
from dpone.services.semantic_refresh_mssql_authority_documents import (
    semantic_refresh_mssql_authority_from_record,
    semantic_refresh_mssql_authority_json,
    semantic_refresh_mssql_authority_sha256,
)

if TYPE_CHECKING:
    from dpone.services.semantic_refresh_mssql_replacement import (
        SemanticRefreshMssqlReplacementService,
    )

_SHA256_PREFIX = "sha256:"


@dataclass(frozen=True, slots=True)
class SemanticRefreshMssqlAuthorityAdmissionService:
    """Derive and atomically admit one protected canonical workflow execution."""

    authority: SemanticRefreshMssqlCanonicalAuthorityPort
    admission: SemanticRefreshMssqlAdmissionPort
    replacement: SemanticRefreshMssqlReplacementService | None = None

    def admit(self, workflow_execution_binding_sha256: str) -> MssqlAdmissionReceipt:
        """Load, authenticate, compose, and admit without caller plan claims."""

        _require_digest(
            workflow_execution_binding_sha256,
            "workflow_execution_binding_sha256",
        )
        bundle = authority_from_record(self.authority.load(workflow_execution_binding_sha256))
        if bundle.execution_binding.workflow_execution_binding_sha256 != workflow_execution_binding_sha256:
            raise ValueError("protected authority returned a different execution binding")
        request = compose_admission(bundle)
        if bundle.replacement_plan is not None:
            if self.replacement is None:
                raise ValueError("replacement admission requires predecessor state policy")
            request = self.replacement.bind_successor(
                admission=request,
                replacement_plan=bundle.replacement_plan,
            )
        return self.admission.admit(request)


@dataclass(frozen=True, slots=True)
class SemanticRefreshMssqlProtectedAuthorityService:
    """Resolve one operation from ACTIVE authority and the same locked state."""

    state: SemanticRefreshMssqlProtectedOperationStatePort

    def load_operation(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> MssqlProtectedOperationAuthority:
        """Return authenticated operation, attempt, target, and predecessor state."""

        _require_digest(workflow_execution_binding_sha256, "workflow_execution_binding_sha256")
        _require_digest(operation_id, "operation_id")
        state = self.state.load_operation_state(
            workflow_execution_binding_sha256=workflow_execution_binding_sha256,
            operation_id=operation_id,
        )
        bundle = authority_from_record(state.authority)
        operation = next(
            (item for item in bundle.operation_plans if item.operation_id == operation_id),
            None,
        )
        if operation is None:
            raise ValueError("operation is absent from protected canonical authority")
        resource = next(item for item in bundle.model_resources if item.model_unique_id == operation.model_unique_id)
        attempt = next(item for item in bundle.attempt_bindings if item.operation_id == operation.operation_id)
        if resource.publication_scope_id != f"{operation.scope_start}/{operation.scope_end}":
            raise ValueError("publication scope differs from protected operation scope")
        validate_runtime_operation_state(bundle, operation, attempt, resource, state)
        if mssql_strategy_authority_sha256(state.strategy_authority_json) != state.strategy_authority_sha256:
            raise ValueError("durable attempt strategy JSON differs from its digest")
        receipt_sha256 = _scope_map_receipt_sha256(bundle.authority_sha256, state)
        return MssqlProtectedOperationAuthority(
            workflow_execution_id=bundle.workflow_execution_id,
            workflow_id=operation.workflow_id,
            workflow_execution_binding_sha256=(bundle.execution_binding.workflow_execution_binding_sha256),
            canonical_authority_sha256=bundle.authority_sha256,
            workflow_plan_sha256=bundle.workflow_plan.workflow_plan_sha256,
            operation_id=operation.operation_id,
            operation_plan_sha256=operation.operation_plan_sha256,
            attempt_binding_sha256=attempt.attempt_binding_sha256,
            fencing_epoch=attempt.fencing_epoch,
            owner_id=bundle.owner_id,
            guard_resource_id=resource.target_resource_id,
            guard_status=state.guard_status,
            journal_status=state.journal_status,
            journal_version=state.journal_version,
            strategy_authority_json=state.strategy_authority_json,
            strategy_authority_sha256=state.strategy_authority_sha256,
            model_unique_id=operation.model_unique_id,
            target_resource_id=resource.target_resource_id,
            target_authority_id=resource.target_authority_id,
            mssql_connection_authority_id=resource.mssql_connection_authority_id,
            mssql_target_authority_id=resource.mssql_target_authority_id,
            clickhouse_cluster_authority_id=resource.clickhouse_cluster_authority_id,
            publication_database=resource.publication_database,
            publication_target_table=resource.publication_target_table,
            publication_scope_id=resource.publication_scope_id,
            scope_family_id=operation.scope_family_id,
            scope_start=operation.scope_start,
            scope_end=operation.scope_end,
            scope_revision=operation.scope_revision,
            mutation_closure_sha256=operation.mutation_closure_sha256,
            target_predecessor_generation_id=operation.target_predecessor_generation_id,
            scope_predecessor_operation_id=operation.scope_predecessor_operation_id,
            predecessor_target_generation=state.predecessor_target_generation,
            predecessor_target_uuid=state.predecessor_target_uuid,
            predecessor_target_operation_id=state.predecessor_target_operation_id,
            predecessor_scope_revision=state.predecessor_scope_revision,
            predecessor_checkpoint_sha256=state.predecessor_checkpoint_sha256,
            predecessor_checkpoint_operation_id=(state.predecessor_checkpoint_operation_id),
            predecessor_checkpoint_version=state.predecessor_checkpoint_version,
            clickhouse_target_uuid=resource.clickhouse_target_uuid,
            model_definition_proof_sha256=resource.model_definition_proof_sha256,
            effective_key_template_sha256=resource.effective_key_template_sha256,
            effective_key_mapping_sha256=resource.effective_key_mapping_sha256,
            writable_columns=resource.writable_columns,
            writable_schema_sha256=resource.writable_schema_sha256,
            resource_policy=resource.resource_policy,
            route_certification_receipt_sha256=(resource.route_certification_receipt_sha256),
            writer_exclusivity_assurance_receipt_sha256=(resource.writer_exclusivity_assurance_receipt_sha256),
            ddl_freeze_assurance_receipt_sha256=(resource.ddl_freeze_assurance_receipt_sha256),
            utc_semantics_assurance_receipt_sha256=(resource.utc_semantics_assurance_receipt_sha256),
            artifact_authority=resource.artifact_authority,
            before_image_relation=state.before_image_relation,
            before_image_sha256=state.before_image_sha256,
            after_image_relation=state.after_image_relation,
            after_image_sha256=state.after_image_sha256,
            scope_map_authority_receipt_sha256=receipt_sha256,
            prerequisite_authority=mssql_prerequisite_authority(operation, resource),
            baseline_receipt_sha256=resource.baseline_receipt_sha256,
        )


@dataclass(frozen=True, slots=True)
class SemanticRefreshMssqlScopeMapService:
    """Build the only dbt scope map accepted from protected admitted state."""

    authority: SemanticRefreshMssqlProtectedAuthorityService

    def build(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> dict[str, object]:
        """Return a closed verified map without accepting caller payload or digests."""

        protected = self.authority.load_operation(
            workflow_execution_binding_sha256=workflow_execution_binding_sha256,
            operation_id=operation_id,
        )
        try:
            strategy = json.loads(protected.strategy_authority_json)
        except json.JSONDecodeError as exc:
            raise ValueError("protected strategy authority JSON is invalid") from exc
        if not isinstance(strategy, dict) or strategy.get("model_unique_id") != protected.model_unique_id:
            raise ValueError("protected strategy model identity differs")
        unsigned = {
            "authorities": {protected.model_unique_id: protected.strategy_authority_json},
            "models": {protected.model_unique_id: strategy},
            "schema": "dpone.semantic-refresh-mssql-scope-map.v1",
        }
        raw = json.dumps(unsigned, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
        return {
            **unsigned,
            "scope_map_sha256": _SHA256_PREFIX + hashlib.sha256(raw.encode()).hexdigest(),
            # Compatibility name in the dbt macro; this is an authority receipt, not a caller signature.
            "signature_sha256": protected.scope_map_authority_receipt_sha256,
            "verification_status": "VERIFIED",
        }


@dataclass(frozen=True, slots=True)
class SemanticRefreshMssqlPlanScopeMapLoader:
    """Load the exact multi-model scope map from one canonical admitted plan."""

    state: SemanticRefreshMssqlProtectedPlanStatePort

    def load(
        self,
        *,
        workflow_execution_binding_sha256: str,
        model_unique_ids: tuple[str, ...],
    ) -> dict[str, object]:
        """Return the complete plan-bound model strategy closure for dbt."""

        _require_digest(workflow_execution_binding_sha256, "workflow_execution_binding_sha256")
        if (
            not model_unique_ids
            or model_unique_ids != tuple(sorted(set(model_unique_ids)))
            or any(not isinstance(item, str) or not item.strip() for item in model_unique_ids)
        ):
            raise ValueError("model_unique_ids must be a non-empty canonical closure")
        states = self.state.load_plan_states(
            workflow_execution_binding_sha256=workflow_execution_binding_sha256,
        )
        if not states or any(item.authority != states[0].authority for item in states):
            raise ValueError("protected plan state authority closure differs")
        bundle = authority_from_record(states[0].authority)
        operations = tuple(sorted(bundle.operation_plans, key=lambda item: item.model_unique_id))
        if tuple(item.model_unique_id for item in operations) != model_unique_ids:
            raise ValueError("dbt model closure differs from canonical admission authority")
        state_by_operation = {item.operation_id: item for item in states}
        if len(state_by_operation) != len(states) or set(state_by_operation) != {
            item.operation_id for item in operations
        }:
            raise ValueError("protected plan operation state closure differs")
        protected = tuple(
            SemanticRefreshMssqlProtectedAuthorityService(
                _ExactOperationState(state_by_operation[operation.operation_id])
            ).load_operation(
                workflow_execution_binding_sha256=workflow_execution_binding_sha256,
                operation_id=operation.operation_id,
            )
            for operation in operations
        )
        models: dict[str, object] = {}
        authorities: dict[str, str] = {}
        receipts: list[dict[str, str]] = []
        for operation, item in zip(operations, protected, strict=True):
            try:
                strategy = json.loads(item.strategy_authority_json)
            except json.JSONDecodeError as exc:
                raise ValueError("protected strategy authority JSON is invalid") from exc
            if (
                not isinstance(strategy, dict)
                or strategy.get("model_unique_id") != operation.model_unique_id
                or item.workflow_execution_binding_sha256 != workflow_execution_binding_sha256
            ):
                raise ValueError("protected strategy differs from canonical plan operation")
            models[operation.model_unique_id] = strategy
            authorities[operation.model_unique_id] = item.strategy_authority_json
            receipts.append(
                {
                    "model_unique_id": operation.model_unique_id,
                    "scope_map_authority_receipt_sha256": item.scope_map_authority_receipt_sha256,
                }
            )
        unsigned = {
            "authorities": authorities,
            "models": models,
            "schema": "dpone.semantic-refresh-mssql-scope-map.v1",
        }
        raw = json.dumps(unsigned, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
        receipt_raw = json.dumps(
            {
                "canonical_authority_sha256": bundle.authority_sha256,
                "model_receipts": receipts,
                "schema": "dpone.semantic-refresh-mssql-plan-scope-map-authority.v1",
                "workflow_execution_binding_sha256": workflow_execution_binding_sha256,
            },
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        return {
            **unsigned,
            "scope_map_sha256": _SHA256_PREFIX + hashlib.sha256(raw.encode()).hexdigest(),
            "signature_sha256": _SHA256_PREFIX + hashlib.sha256(receipt_raw.encode()).hexdigest(),
            "verification_status": "VERIFIED",
        }


@dataclass(frozen=True, slots=True)
class _ExactOperationState:
    state: MssqlProtectedOperationStateRecord

    def load_operation_state(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> MssqlProtectedOperationStateRecord:
        if (
            self.state.authority.workflow_execution_binding_sha256 != workflow_execution_binding_sha256
            or self.state.operation_id != operation_id
        ):
            raise ValueError("protected operation state identity differs")
        return self.state


def _scope_map_receipt_sha256(
    authority_sha256: str,
    state: MssqlProtectedOperationStateRecord,
) -> str:
    fields = {
        "after_image_relation": state.after_image_relation,
        "after_image_sha256": state.after_image_sha256,
        "attempt_binding_sha256": state.attempt_binding_sha256,
        "before_image_relation": state.before_image_relation,
        "before_image_sha256": state.before_image_sha256,
        "canonical_authority_sha256": authority_sha256,
        "fencing_epoch": state.fencing_epoch,
        "guard_resource_id": state.guard_resource_id,
        "guard_status": state.guard_status,
        "journal_status": state.journal_status,
        "operation_id": state.operation_id,
        "predecessor_checkpoint_operation_id": (state.predecessor_checkpoint_operation_id),
        "predecessor_checkpoint_sha256": state.predecessor_checkpoint_sha256,
        "predecessor_checkpoint_version": state.predecessor_checkpoint_version,
        "predecessor_scope_revision": state.predecessor_scope_revision,
        "predecessor_target_generation": state.predecessor_target_generation,
        "predecessor_target_operation_id": state.predecessor_target_operation_id,
        "predecessor_target_uuid": state.predecessor_target_uuid,
        "schema": "dpone.semantic-refresh-mssql-scope-map-authority-receipt.v1",
        "strategy_authority_sha256": state.strategy_authority_sha256,
        "workflow_execution_id": state.workflow_execution_id,
    }
    raw = json.dumps(fields, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return _SHA256_PREFIX + hashlib.sha256(raw.encode()).hexdigest()


def _require_digest(value: str, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or not value.startswith(_SHA256_PREFIX)
        or len(value) != len(_SHA256_PREFIX) + 64
        or any(character not in "0123456789abcdef" for character in value[7:])
    ):
        raise ValueError(f"{field_name} must be a lowercase sha256 digest")


__all__ = [
    "MssqlCanonicalAdmissionBundle",
    "MssqlModelResourceAuthority",
    "SemanticRefreshMssqlAuthorityAdmissionService",
    "SemanticRefreshMssqlProtectedAuthorityService",
    "SemanticRefreshMssqlPlanScopeMapLoader",
    "SemanticRefreshMssqlScopeMapService",
    "mssql_model_resource_authority_from_plan_target",
    "semantic_refresh_mssql_authority_from_record",
    "semantic_refresh_mssql_authority_json",
    "semantic_refresh_mssql_authority_sha256",
]
