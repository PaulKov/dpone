"""Atomic MSSQL materialization of canonical worker-run authority."""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol, cast

from dpone.adapters.semantic_refresh_mssql_activation import MssqlSemanticRefreshActivationStore
from dpone.adapters.semantic_refresh_mssql_activation_authority import (
    MssqlSemanticRefreshActivationAuthorityStore,
)
from dpone.adapters.semantic_refresh_mssql_attempt_continuation import (
    MssqlSemanticRefreshAttemptContinuationStore,
)
from dpone.adapters.semantic_refresh_mssql_connection_lifecycle import (
    MssqlWorkerConnection,
    SemanticRefreshMssqlAtomicWorkerAdmissionError,
    close_handle,
    prepare_transaction_cursor,
    rollback_if_open,
)
from dpone.adapters.semantic_refresh_mssql_state import MssqlSemanticRefreshStateAdapter
from dpone.ports.semantic_refresh_attempt_quiescence import (
    ClickHouseAttemptQuiescenceProof,
    SemanticRefreshClickHouseAttemptQuiescencePort,
)
from dpone.ports.semantic_refresh_mssql_admission_authority import compose_admission
from dpone.ports.semantic_refresh_mssql_authority_codec import (
    authority_from_record,
    authority_json,
    canonical_authority_record,
)
from dpone.ports.semantic_refresh_mssql_authority_models import (
    mssql_model_resource_authority_from_plan_target,
)
from dpone.ports.semantic_refresh_mssql_authority_records import (
    MssqlCanonicalAuthorityRecord,
    mssql_artifact_retention_authorizes,
)
from dpone.ports.semantic_refresh_mssql_worker_admission import (
    MssqlGuardEpochSnapshot,
    MssqlTrustedAttemptCoordinate,
    MssqlWorkerAdmissionCoordinates,
    SemanticRefreshMssqlReplacementAdmissionBinderPort,
    SemanticRefreshMssqlWorkerAttemptAuthorityPort,
    compose_worker_admission_bundle,
    validate_worker_admission_bundle,
)

if TYPE_CHECKING:
    from dpone.ports.semantic_refresh_mssql import MssqlAdmissionReceipt, MssqlAdmissionRequest
    from dpone.ports.semantic_refresh_mssql_activation import MssqlActivatedPackRegistration

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class _Cursor(Protocol):
    def execute(self, sql: str, *parameters: object) -> _Cursor: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...

    def fetchall(self) -> Sequence[tuple[Any, ...]]: ...

    def close(self) -> None: ...


class _AdmissionState(Protocol):
    def lock_admissible_guard_epoch(self, cursor: _Cursor, resource_id: str) -> int: ...

    def admit_in_transaction(
        self,
        cursor: _Cursor,
        request: MssqlAdmissionRequest,
    ) -> MssqlAdmissionReceipt: ...


class _RunActivation(Protocol):
    def register_activated_pack_in_transaction(
        self,
        cursor: _Cursor,
        request: MssqlActivatedPackRegistration,
    ) -> None: ...

    def prepare_run_guards_in_transaction(
        self,
        cursor: _Cursor,
        request: MssqlActivatedPackRegistration,
    ) -> None: ...


class _UnavailableClickHouseQuiescence:
    def prove_quiescent(self, **_: object) -> ClickHouseAttemptQuiescenceProof:
        raise SemanticRefreshMssqlAtomicWorkerAdmissionError("ClickHouse attempt quiescence authority is unavailable")


class MssqlSemanticRefreshAtomicWorkerAdmission:
    """Lock the activated pack and guards, allocate fences, and admit atomically."""

    def __init__(
        self,
        connection_factory: Callable[[], MssqlWorkerConnection],
        *,
        control_schema: str = "dpone_control",
        clock: Callable[[], datetime],
        attempt_authority: SemanticRefreshMssqlWorkerAttemptAuthorityPort,
        replacement: SemanticRefreshMssqlReplacementAdmissionBinderPort | None = None,
        clickhouse_quiescence: SemanticRefreshClickHouseAttemptQuiescencePort | None = None,
    ) -> None:
        if not isinstance(control_schema, str) or _IDENTIFIER.fullmatch(control_schema) is None:
            raise ValueError("control_schema must be a simple SQL identifier")
        self._connection_factory = connection_factory
        self._control_schema = control_schema
        self._clock = clock
        self._attempt_authority = attempt_authority
        self._state = cast(
            _AdmissionState,
            MssqlSemanticRefreshStateAdapter(
                cast(Any, connection_factory),
                control_schema=control_schema,
            ),
        )
        self._activation = cast(
            _RunActivation,
            MssqlSemanticRefreshActivationStore(
                cast(Any, connection_factory),
                control_schema=control_schema,
            ),
        )
        self._replacement = replacement
        self._continuations = MssqlSemanticRefreshAttemptContinuationStore(
            control_schema=control_schema,
            clock=clock,
            clickhouse_quiescence=(clickhouse_quiescence or _UnavailableClickHouseQuiescence()),
        )

    def admit_run(
        self,
        *,
        activated_pack: MssqlActivatedPackRegistration,
        plan_bundle,
        run_execution,
    ) -> MssqlAdmissionReceipt:
        """Persist/replay canonical authority and admission in one SERIALIZABLE transaction."""

        connection: MssqlWorkerConnection | None = None
        cursor: _Cursor | None = None
        commit_started = False
        try:
            connection = self._connection_factory()
            cursor = cast(
                _Cursor,
                prepare_transaction_cursor(
                    connection,
                    min(item.resource_policy.max_statement_seconds for item in plan_bundle.targets),
                ),
            )
            cursor.execute("SET XACT_ABORT ON; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            self._require_current_artifact_authority(plan_bundle)
            self._validate_pack(activated_pack, plan_bundle, run_execution)
            self._require_activation_authority(cursor, activated_pack, plan_bundle)
            coordinates = self._attempt_authority.load_attempts(
                plan_bundle_sha256=plan_bundle.plan_bundle_sha256,
                workflow_execution_id=run_execution.workflow_execution_binding.workflow_execution_id,
                operation_ids=tuple(item.operation_id for item in plan_bundle.operation_plans),
            )
            self._activation.register_activated_pack_in_transaction(cursor, activated_pack)
            self._activation.prepare_run_guards_in_transaction(cursor, activated_pack)
            record = self._existing_authority(cursor, run_execution)
            if record is None:
                guard_epochs = tuple(
                    MssqlGuardEpochSnapshot(
                        resource_id,
                        self._state.lock_admissible_guard_epoch(cursor, resource_id),
                    )
                    for resource_id in sorted(
                        (
                            plan_bundle.run_guard_closure.workflow_guard_resource_id,
                            *plan_bundle.run_guard_closure.resource_guard_ids,
                        )
                    )
                )
                bundle = compose_worker_admission_bundle(
                    plan_bundle=plan_bundle,
                    run_execution=run_execution,
                    coordinates=coordinates,
                    guard_epochs=guard_epochs,
                )
                record = MssqlCanonicalAuthorityRecord(
                    workflow_execution_binding_sha256=(
                        run_execution.workflow_execution_binding.workflow_execution_binding_sha256
                    ),
                    workflow_execution_id=(run_execution.workflow_execution_binding.workflow_execution_id),
                    authority_sha256=bundle.authority_sha256,
                    authority_json=authority_json(bundle),
                    status="ACTIVE",
                )
                self._insert_authority(cursor, record)
            else:
                bundle = authority_from_record(record)
                validate_worker_admission_bundle(
                    bundle,
                    plan_bundle=plan_bundle,
                    run_execution=run_execution,
                    coordinates=_original_coordinates(bundle),
                )
                self._continuations.reconcile(
                    cursor,
                    bundle=bundle,
                    coordinates=coordinates.attempts,
                )
            request = compose_admission(bundle)
            if bundle.replacement_plan is not None:
                if self._replacement is None:
                    raise SemanticRefreshMssqlAtomicWorkerAdmissionError(
                        "replacement run requires protected predecessor admission policy"
                    )
                request = self._replacement.bind_successor(
                    admission=request,
                    replacement_plan=bundle.replacement_plan,
                )
            receipt = self._state.admit_in_transaction(cursor, request)
            commit_started = True
            connection.commit()
            return receipt
        except SemanticRefreshMssqlAtomicWorkerAdmissionError as exc:
            if commit_started:
                raise SemanticRefreshMssqlAtomicWorkerAdmissionError(
                    "DPONE_SEMANTIC_REFRESH_WORKER_ADMISSION_COMMIT_UNKNOWN: "
                    "SQL Server admission commit acknowledgement was not observed"
                ) from exc
            rollback_if_open(connection)
            raise
        except Exception as exc:
            if commit_started:
                raise SemanticRefreshMssqlAtomicWorkerAdmissionError(
                    "DPONE_SEMANTIC_REFRESH_WORKER_ADMISSION_COMMIT_UNKNOWN: "
                    "SQL Server admission commit acknowledgement was not observed"
                ) from exc
            rollback_if_open(connection)
            raise SemanticRefreshMssqlAtomicWorkerAdmissionError(
                "atomic semantic-refresh worker admission failed"
            ) from exc
        finally:
            close_handle(cursor)
            close_handle(connection)

    @staticmethod
    def _validate_pack(activated_pack: MssqlActivatedPackRegistration, plan_bundle, run_execution) -> None:
        binding = run_execution.workflow_execution_binding
        closure = plan_bundle.run_guard_closure
        deployment = plan_bundle.release_deployment_authority
        projection = activated_pack.projection_identity
        expected = (
            plan_bundle.plan_bundle_sha256,
            run_execution.run_execution_bundle_sha256,
            binding.workflow_execution_id,
            plan_bundle.workflow_plan.workflow_plan_sha256,
            closure,
            deployment.release_id,
            deployment.deployment_id,
            plan_bundle.pre_release_bundle_sha256,
            plan_bundle.package_artifacts_sha256,
        )
        actual = (
            activated_pack.plan_bundle_sha256,
            activated_pack.run_execution_bundle_sha256,
            activated_pack.workflow_execution_id,
            activated_pack.workflow_plan_sha256,
            activated_pack.run_guard_closure,
            projection.release_id,
            projection.deployment_id,
            projection.pre_release_bundle_sha256,
            projection.package_artifacts_sha256,
        )
        if actual != expected or activated_pack.workflow_execution_binding_sha256 != (
            binding.workflow_execution_binding_sha256
        ):
            raise SemanticRefreshMssqlAtomicWorkerAdmissionError("activated pack differs from canonical worker inputs")

    def _require_current_artifact_authority(self, plan_bundle) -> None:
        as_of = self._clock()
        if not all(
            mssql_artifact_retention_authorizes(
                mssql_model_resource_authority_from_plan_target(target).artifact_authority,
                as_of=as_of,
            )
            for target in plan_bundle.targets
        ):
            raise SemanticRefreshMssqlAtomicWorkerAdmissionError(
                "artifact retention authority is not current at worker admission"
            )

    def _require_activation_authority(self, cursor: _Cursor, activated_pack, plan_bundle) -> None:
        deployment = plan_bundle.release_deployment_authority
        receipt = MssqlSemanticRefreshActivationAuthorityStore(
            cast(Any, self._connection_factory),
            authority_store_ref=activated_pack.authority_store_ref,
            control_schema=self._control_schema,
        ).load_exact_in_transaction(
            cast(Any, cursor),
            release_id=deployment.release_id,
            deployment_id=deployment.deployment_id,
            plan_bundle_sha256=plan_bundle.plan_bundle_sha256,
        )
        if receipt.activation_authority_receipt_sha256 != activated_pack.activation_authority_receipt_sha256:
            raise SemanticRefreshMssqlAtomicWorkerAdmissionError(
                "activated pack is not backed by the exact deployment authority"
            )

    def _existing_authority(self, cursor: _Cursor, run_execution) -> MssqlCanonicalAuthorityRecord | None:
        binding = run_execution.workflow_execution_binding
        cursor.execute(
            f"""
SELECT workflow_execution_binding_sha256, workflow_execution_id,
       authority_sha256, authority_json, status
FROM {self._table("semantic_refresh_canonical_authorities")} WITH (UPDLOCK, HOLDLOCK)
WHERE workflow_execution_binding_sha256 = ? OR workflow_execution_id = ?;
""".strip(),
            binding.workflow_execution_binding_sha256,
            binding.workflow_execution_id,
        )
        rows = tuple(tuple(row) for row in cursor.fetchall())
        if not rows:
            return None
        if len(rows) != 1 or rows[0][0:2] != (
            binding.workflow_execution_binding_sha256,
            binding.workflow_execution_id,
        ):
            raise SemanticRefreshMssqlAtomicWorkerAdmissionError("canonical worker execution identity conflicts")
        return canonical_authority_record(
            workflow_execution_binding_sha256=str(rows[0][0]),
            workflow_execution_id=str(rows[0][1]),
            authority_sha256=str(rows[0][2]),
            authority_json=str(rows[0][3]),
            status=str(rows[0][4]),
        )

    def _insert_authority(self, cursor: _Cursor, record: MssqlCanonicalAuthorityRecord) -> None:
        cursor.execute(
            f"""
INSERT INTO {self._table("semantic_refresh_canonical_authorities")} (
    workflow_execution_binding_sha256, workflow_execution_id,
    authority_sha256, authority_json, status
)
OUTPUT inserted.authority_sha256
VALUES (?, ?, ?, ?, N'ACTIVE');
""".strip(),
            record.workflow_execution_binding_sha256,
            record.workflow_execution_id,
            record.authority_sha256,
            record.authority_json,
        )
        inserted = cursor.fetchone()
        if inserted is None or tuple(inserted) != (record.authority_sha256,):
            raise SemanticRefreshMssqlAtomicWorkerAdmissionError("canonical worker authority insert was not exact")

    def _table(self, name: str) -> str:
        return f"[{self._control_schema}].[{name}]"


def _original_coordinates(bundle) -> MssqlWorkerAdmissionCoordinates:
    return MssqlWorkerAdmissionCoordinates(
        attempts=tuple(
            MssqlTrustedAttemptCoordinate(
                operation_id=item.operation_id,
                task_id=item.task_id,
                try_number=item.try_number,
                pod_uid=item.pod_uid,
            )
            for item in bundle.attempt_bindings
        )
    )


__all__ = [
    "MssqlSemanticRefreshAtomicWorkerAdmission",
    "SemanticRefreshMssqlAtomicWorkerAdmissionError",
]
