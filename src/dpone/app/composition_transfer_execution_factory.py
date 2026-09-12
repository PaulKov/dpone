"""Compose the ordinary PostgreSQL→MSSQL worker from verified parent authority."""

from __future__ import annotations

from typing import Any

from dpone.adapters.composition_mssql_attempts import (
    MssqlCompositionAttemptStore,
    composition_control_transaction,
)
from dpone.adapters.composition_mssql_execution_evidence import persist_execution_proof
from dpone.adapters.composition_mssql_login_gate import MssqlCompositionLoginGate
from dpone.adapters.composition_mssql_transaction_binding import MssqlCompositionTransactionBindings
from dpone.adapters.composition_mssql_transfer_outcome import (
    CompositionMssqlTransferOutcomeObserver,
    CompositionTransferObservation,
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
    return CompositionTransferExecutionDependencies(
        read_active=read_active,
        attempts=MssqlCompositionAttemptStore(
            control.connection_factory,
            expected_service_id=control.expected_service_id,
            control_schema=control.control_schema,
        ),
        gate=MssqlCompositionLoginGate(
            control.connection_factory,
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
            observe=observe or _unknown_transfer_observation,
            persist=persist_execution_proof,
        ),
        sink_target=sink_target,
        state_target=state_target,
        expected_service_id=control.expected_service_id,
        operation_registrar=bindings.bind,
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
