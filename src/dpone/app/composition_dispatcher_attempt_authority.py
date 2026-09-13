"""Reopen dispatcher control authority before resolving any business credential.

Staging omits previous_deployment_id; this reader never fabricates it. The five
provided context coordinates and full staged plan must match the retained parent,
whose complete request digest (including its real predecessor) is pinned by the
candidate attempt. Inspection grants no execution permit and performs no admission;
the worker's existing admit_once transaction remains the decisive race fence.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID

from dpone.adapters.composition_mssql_attempts import composition_control_transaction
from dpone.adapters.composition_mssql_existing_operation import require_existing_execution_in
from dpone.adapters.composition_mssql_operations import read_shared_operation_in
from dpone.adapters.composition_mssql_terminal import require_execution_terminal_in
from dpone.app.composition_authority_connections import CompositionAuthorityConnections
from dpone.app.composition_clickhouse_execution import build_composition_clickhouse_attempt
from dpone.app.composition_dbt_execution_factory import CompositionDbtControlAuthority
from dpone.app.composition_dispatcher_context import StagedDispatcherAttempt
from dpone.contracts.airflow_correlation import AirflowAttemptCorrelation
from dpone.contracts.airflow_run_identity import AirflowRunIdentity
from dpone.contracts.composition_activation import CompositionActivationOccurrence, CompositionOccurrenceContext
from dpone.contracts.composition_dispatcher_binding import require_dispatcher_connection_ref
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.composition_persistence import CompositionAttemptReceipt
from dpone.contracts.runtime_connection import ResolvedBindingConnection


class _ConnectorFactory(Protocol):
    def __call__(self, connection: ResolvedBindingConnection, *, autocommit: bool) -> Any: ...


@dataclass(frozen=True, slots=True)
class _PinnedInputs:
    context: CompositionOccurrenceContext
    reference: str
    connection: ResolvedBindingConnection

    def resolve_connection(
        self, context: CompositionOccurrenceContext, connection_ref: str
    ) -> ResolvedBindingConnection:
        if context != self.context or connection_ref != self.reference:
            raise CompositionAdmissionError("dispatcher_control_scope")
        return self.connection


class DispatcherAttemptAuthority:
    """Read current or historical originals through a verified control endpoint.

    The supplied connector factory owns request-budget propagation. Constructor
    resolution is limited to the configured control alias; source/target aliases
    and signed registry references must remain distinct from that control alias.
    Each public read opens one independently verified, caller-owned SQL session.
    """

    def __init__(
        self,
        selected: StagedDispatcherAttempt,
        *,
        control_connection_ref: str,
        expected_control_service_id: str,
        control_schema: str,
        connector_factory: _ConnectorFactory,
    ) -> None:
        try:
            reference = require_dispatcher_connection_ref(control_connection_ref)
            if (
                str(UUID(expected_control_service_id)) != expected_control_service_id
                or UUID(expected_control_service_id).int == 0
            ):
                raise ValueError("service")
            if not callable(connector_factory):
                raise ValueError("factory")
            selected.attempt.__post_init__()
            context = selected.context.occurrence
            context.__post_init__()
            manifest = selected.manifest
            source = require_dispatcher_connection_ref(manifest["source"]["connection_ref"])
            target = require_dispatcher_connection_ref(manifest["sink"]["connection_ref"])
            if target != selected.write.connection_ref or target != selected.context.target_binding_ref:
                raise ValueError("target")
            bindings = selected.context.runtime.binding_set["bindings"]
            control_registry = bindings[reference]["connection_ref"]
            if reference in {source, target} or control_registry in {
                bindings[source]["connection_ref"],
                bindings[target]["connection_ref"],
            }:
                raise ValueError("control alias")
            resolved = selected.context.runtime.resolver.resolve(reference)
            if (
                resolved.descriptor is None
                or resolved.descriptor.connection_type != "mssql"
                or resolved.descriptor.properties.get("composition_service_id") != expected_control_service_id
            ):
                raise ValueError("control service")
            authority = CompositionAuthorityConnections(
                inputs=_PinnedInputs(context, reference, resolved),
                authority_connection_ref=reference,
                control_schema=control_schema,
                connector_factory=connector_factory,
            )
            self.control = CompositionDbtControlAuthority(
                lambda: authority.control_connection(context),
                expected_control_service_id,
                resolved.credentials.database,
                control_schema,
            )
        except Exception:
            raise CompositionAdmissionError("dispatcher_control_authority") from None
        self._selected = selected

    def inspect(
        self, run_identity: AirflowRunIdentity, airflow_attempt: AirflowAttemptCorrelation
    ) -> CompositionAttemptReceipt | None:
        """Return verified retained receipt, or positively absent ACTIVE candidate."""
        self._require_scheduler(run_identity, airflow_attempt)
        control, attempt = self.control, self._selected.attempt
        with composition_control_transaction(
            control.connection_factory, control.control_schema, control.expected_service_id
        ) as ledger:
            transaction = ledger.require_transaction()
            observed = read_shared_operation_in(
                ledger, attempt.attempt_sha256, expected_service_id=control.expected_service_id
            )
            ledger.require_transaction(transaction)
            if observed is not None:
                occurrence, receipt = require_existing_execution_in(
                    ledger,
                    attempt,
                    expected_service_id=control.expected_service_id,
                    terminal_validator=ledger.terminal_validator,
                )
                self._require_parent(occurrence)
                if receipt.state in {"SUCCEEDED", "FAILED"}:
                    ledger.require_transaction(transaction)
                    require_execution_terminal_in(
                        ledger, occurrence, receipt, expected_service_id=control.expected_service_id
                    )
                    ledger.require_transaction(transaction)
                final = read_shared_operation_in(
                    ledger, attempt.attempt_sha256, expected_service_id=control.expected_service_id
                )
                if final != observed:
                    raise CompositionAdmissionError("dispatcher_attempt_changed")
                ledger.require_transaction(transaction)
                return receipt
            active = ledger.read(self._selected.context.occurrence.activation_id)
            if active is None:
                raise CompositionAdmissionError("dispatcher_parent_missing")
            self._require_parent(active)
            active.require_state("ACTIVE")
            candidate = build_composition_clickhouse_attempt(
                active,
                manifest=self._selected.manifest,
                plan_sha256=self._selected.context.plan.sources.subject_sha256,
                run_identity=run_identity,
                airflow_attempt=airflow_attempt,
            )
            if candidate != attempt:
                raise CompositionAdmissionError("dispatcher_attempt_identity")
            ledger.require_transaction(transaction)
            return None

    def read_active(self) -> CompositionActivationOccurrence:
        """Reopen current parent; no inherited ACTIVE assertion from staging."""
        control = self.control
        with composition_control_transaction(
            control.connection_factory, control.control_schema, control.expected_service_id
        ) as ledger:
            transaction = ledger.require_transaction()
            occurrence = ledger.read(self._selected.context.occurrence.activation_id)
            if occurrence is None:
                raise CompositionAdmissionError("dispatcher_parent_missing")
            self._require_parent(occurrence)
            occurrence.require_state("ACTIVE")
            ledger.require_transaction(transaction)
            return occurrence

    def _require_parent(self, occurrence: CompositionActivationOccurrence) -> None:
        staged, request = self._selected.context, occurrence.request
        fields = ("activation_id", "environment", "release_id", "deployment_id", "runtime_context_sha256")
        if (
            tuple(getattr(request.context, field) for field in fields)
            != tuple(getattr(staged.occurrence, field) for field in fields)
            or request.source_subject_sha256 != staged.plan.sources.subject_sha256
            or request.workloads != staged.plan.workloads
            or request.request_sha256 != self._selected.attempt.activation_request_sha256
            or self._selected.attempt.plan_sha256 != staged.plan.sources.subject_sha256
        ):
            raise CompositionAdmissionError("dispatcher_parent_identity")

    def _require_scheduler(self, run: AirflowRunIdentity, correlation: AirflowAttemptCorrelation) -> None:
        try:
            AirflowRunIdentity.from_mapping(run.to_dict())
            AirflowAttemptCorrelation.from_mapping(correlation.to_dict())
            if run.dag_spec is not None and run.dag_spec.id != correlation.dag_id:
                raise ValueError("scheduler DAG")
            attempt, context = self._selected.attempt, self._selected.context.occurrence
            if (run.release_id, run.deployment_id, run.workload_pack.id, run.workload_pack.sha256) != (
                context.release_id,
                context.deployment_id,
                attempt.workload_id,
                attempt.pack_sha256,
            ) or (correlation.run_id, correlation.task_id, correlation.try_number, correlation.map_index) != (
                attempt.dag_run_id,
                attempt.task_id,
                attempt.try_number,
                attempt.map_index,
            ):
                raise ValueError("scheduler")
        except Exception:
            raise CompositionAdmissionError("dispatcher_scheduler_identity") from None
