"""Compose the ordinary PostgreSQL→MSSQL worker from verified parent authority."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.adapters.composition_mssql_attempts import (
    MssqlCompositionAttemptStore,
    composition_control_transaction,
)
from dpone.adapters.composition_mssql_execution_evidence import persist_execution_proof
from dpone.adapters.composition_mssql_login_gate import MssqlCompositionLoginGate
from dpone.adapters.composition_mssql_transaction_binding import MssqlCompositionTransactionBindings
from dpone.adapters.composition_mssql_transaction_fence import MssqlCompositionTransactionFence
from dpone.adapters.composition_mssql_transfer_access import MssqlCompositionTransferAccess
from dpone.adapters.composition_mssql_transfer_outcome import (
    CompositionMssqlTransferOutcomeObserver,
    CompositionTransferObservation,
    resolve_transfer_principal,
)
from dpone.app.composition_dbt_execution_factory import CompositionDbtControlAuthority
from dpone.app.composition_transfer_execution import (
    CompositionTransferExecutionDependencies,
    CompositionTransferExecutionRoot,
)
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.runtime.bootstrap_hydrator import DefaultRuntimeHydrator
from dpone.runtime.bootstrap_runner import DefaultProcessRunner


def build_composition_transfer_execution_dependencies(
    *,
    control: CompositionDbtControlAuthority,
    read_active: Any,
    sink_target: Any,
    state_target: Any,
    read_plan: Any,
    verify_operation: Any,
    observe: Any | None = None,
    state_config: Any | None = None,
    payload_root: Path | None = None,
    verified_manifest: Any | None = None,
    source_target: Any | None = None,
    parent_context: Any | None = None,
) -> CompositionTransferExecutionDependencies:
    """Return protected collaborators of one supervised ordinary transfer cell."""

    control.__post_init__()
    if not callable(read_plan) or not callable(verify_operation):
        raise CompositionAdmissionError("transfer_source_plan")
    bindings = MssqlCompositionTransactionBindings(
        control.connection_factory,
        expected_service_id=control.expected_service_id,
        control_database=control.control_database,
        read_plan=read_plan,
        verify_operation=verify_operation,
        control_schema=control.control_schema,
    )

    def register(
        attempt: Any, operation: Any, write: Any, mutation: bytes, *, preplan_document_sha256: str | None = None
    ) -> MssqlCompositionTransactionFence:
        binding = bindings.bind(attempt, operation, write, mutation, preplan_document_sha256=preplan_document_sha256)
        return MssqlCompositionTransactionFence(binding, control.control_schema)

    def principal(attempt: Any) -> str:
        with composition_control_transaction(
            control.connection_factory, control.control_schema, control.expected_service_id
        ) as ledger:
            return resolve_transfer_principal(ledger, attempt, control.expected_service_id)

    capture_lifecycle = preplan_factory = None
    if (state_config is None) != (payload_root is None):
        raise CompositionAdmissionError("transfer_observation_configuration")
    if payload_root is not None:
        if observe is not None:
            raise CompositionAdmissionError("transfer_observation_configuration")
        if verified_manifest is None or source_target is None or parent_context is None:
            raise CompositionAdmissionError("transfer_preplan_configuration")
        from dpone.app.composition_transfer_observation_factory import build_composition_transfer_observation
        from dpone.app.composition_transfer_preplan_factory import (
            build_transfer_commit_verifier,
            build_transfer_preplan_factory,
        )

        preplan_factory = build_transfer_preplan_factory(
            control=control,
            verified_manifest=verified_manifest,
            source_target=source_target,
            sink_target=sink_target,
            state_target=state_target,
            state_config=state_config,
            parent_context=parent_context,
            payload_root=payload_root,
            read_plan=read_plan,
        )

        observe, capture_lifecycle = build_composition_transfer_observation(
            control=control,
            sink_target=sink_target,
            state_target=state_target,
            state_config=state_config,
            payload_root=payload_root,
            read_plan=read_plan,
            verify_operation=verify_operation,
            verify_retained_commit=build_transfer_commit_verifier(payload_root),
        )

    return CompositionTransferExecutionDependencies(
        read_active=read_active,
        attempts=MssqlCompositionAttemptStore(
            control.connection_factory,
            expected_service_id=control.expected_service_id,
            control_schema=control.control_schema,
        ),
        gate=MssqlCompositionLoginGate(
            control.connection_factory,
            transfer_access=MssqlCompositionTransferAccess(),
            expected_service_id=control.expected_service_id,
            control_database=control.control_database,
            control_schema=control.control_schema,
        ),
        hydrator=DefaultRuntimeHydrator(),
        runner=DefaultProcessRunner(),
        outcome_observer=CompositionMssqlTransferOutcomeObserver(
            transaction=lambda: composition_control_transaction(
                control.connection_factory,
                control.control_schema,
                control.expected_service_id,
            ),
            expected_service_id=control.expected_service_id,
            principal_id=control.expected_service_id,
            resolve_principal=principal,
            observe=observe or _unknown_transfer_observation,
            persist=persist_execution_proof,
        ),
        sink_target=sink_target,
        state_target=state_target,
        expected_service_id=control.expected_service_id,
        operation_registrar=register,
        capture_lifecycle=capture_lifecycle,
        preplan_factory=preplan_factory,
    )


def build_composition_transfer_execution_root(
    **kwargs: Any,
) -> CompositionTransferExecutionRoot:
    """Construct the ordinary transfer root from verified parent authority."""

    dependencies = kwargs.get("dependencies")
    if not isinstance(dependencies, CompositionTransferExecutionDependencies):
        dependencies = build_composition_transfer_execution_dependencies(**kwargs)
    return CompositionTransferExecutionRoot(dependencies)


def _unknown_transfer_observation(_attempt: Any) -> CompositionTransferObservation:
    """Missing independent proof is COMMIT_UNKNOWN, never a silent success."""

    return CompositionTransferObservation(False, False, False, False, False)


__all__ = [
    "build_composition_transfer_execution_dependencies",
    "build_composition_transfer_execution_root",
]


def transfer_payload_root(environment: Mapping[str, str]) -> Path:
    """Reopen the administrator-provisioned private supervisor capture directory."""
    import os

    from dpone.adapters.composition_supervisor_filesystem import (
        absolute_supervisor_path,
        open_protected,
        require_supervisor,
    )

    require_supervisor()
    root = absolute_supervisor_path(
        Path(environment.get("DPONE_COMPOSITION_SUPERVISOR_ROOT") or "/var/lib/dpone/composition")
    )
    payloads = root / "transfers"
    descriptor = open_protected(payloads, traversable=False)
    os.close(descriptor)
    return payloads
