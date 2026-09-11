"""Compose sealed binding resolution with protected per-backend observations."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, Protocol

from dpone.adapters.composition_mssql_enrollment import mssql_target_pin
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.composition_physical import CompositionDomainObservation, CompositionPhysicalDomain
from dpone.contracts.dbt_sqlserver_macro_authority_baseline import DBT_SQLSERVER_MACRO_AUTHORITY_BASELINE_SHA256
from dpone.contracts.dbt_workspace_activation import dbt_relation_write_subject
from dpone.contracts.dbt_workspace_observation import MssqlWorkspaceObservationRequest

if TYPE_CHECKING:
    from dpone.adapters.composition_clickhouse_enrollment import ClickHouseCompositionEnrollmentReader
    from dpone.adapters.composition_mssql_enrollment import MssqlCompositionEnrollmentReader
    from dpone.contracts.composition_activation import CompositionOccurrenceContext
    from dpone.contracts.composition_execution import CompositionExecutionPlan
    from dpone.contracts.dbt_relation_writes import DbtRelationWrite
    from dpone.contracts.runtime_connection import ResolvedBindingConnection
    from dpone.ports.dbt_workspace_activation import DbtWorkspaceMssqlObservationPort


class _Inputs(Protocol):
    def resolve_connection(
        self, context: CompositionOccurrenceContext, connection_ref: str
    ) -> ResolvedBindingConnection: ...


class _ExecutionCapabilities(Protocol):
    @property
    def execution_cells(self) -> frozenset[str]: ...

    def require_execution(self, plan: CompositionExecutionPlan, context: CompositionOccurrenceContext) -> None: ...


class CompositionProtectedPhysicalBackend:
    """Resolve every alias independently, then observe one complete domain union.

    The caller uses CompositionPhysicalAdmissionService for union grouping and
    collision rejection. This backend rechecks every binding before its one SQL
    catalog comparison; alias/principal/version changes never redefine identity.
    Execution cells come solely from the injected installed runtime capability,
    which must verify exact source/sink/state/staging effects and publisher gates.
    Catalog success alone does not install a route or authorize any SQL mutation.
    """

    def __init__(
        self,
        *,
        inputs: _Inputs,
        execution_capabilities: _ExecutionCapabilities,
        mssql_enrollment: MssqlCompositionEnrollmentReader,
        clickhouse_enrollment: ClickHouseCompositionEnrollmentReader,
        mssql_observer: DbtWorkspaceMssqlObservationPort,
    ) -> None:
        self._inputs = inputs
        self._execution = execution_capabilities
        self._mssql_enrollment = mssql_enrollment
        self._clickhouse_enrollment = clickhouse_enrollment
        self._mssql_observer = mssql_observer

    @property
    def execution_cells(self) -> frozenset[str]:
        """Advertise only the capabilities installed by the app composition root."""
        return self._execution.execution_cells

    def require_execution(self, plan: CompositionExecutionPlan, context: CompositionOccurrenceContext) -> None:
        """Require the exact full-parent runtime and each native invocation DB pin."""
        plan.__post_init__()
        context.__post_init__()
        if plan.sources.release_id != context.release_id:
            raise CompositionAdmissionError("source_context_identity")
        plan.require_installed_cells(self.execution_cells)
        self._execution.require_execution(plan, context)
        for workflow in plan.sources.native.workflows:
            profile = workflow.execution.invocation_profile()
            writes = tuple(
                write
                for write in plan.writes
                if write.connection_ref == profile.connection_ref and write.kind != "transfer"
            )
            if not writes:
                raise CompositionAdmissionError("native_invocation_membership")
            connection = self._inputs.resolve_connection(context, profile.connection_ref)
            mssql_target_pin(connection, replace(writes[0], database=profile.database))

    def resolve_domain(
        self, write: DbtRelationWrite, context: CompositionOccurrenceContext
    ) -> CompositionPhysicalDomain:
        """Reopen signed binding and actual independently protected incarnation."""
        context.__post_init__()
        connection = self._inputs.resolve_connection(context, write.connection_ref)
        if write.connector == "mssql":
            return self._mssql_enrollment.resolve(connection, write, context)
        if write.connector == "clickhouse":
            return self._clickhouse_enrollment.resolve(connection, write, context)
        raise CompositionAdmissionError("physical_binding_connector")

    def observe_domain(
        self,
        domain: CompositionPhysicalDomain,
        writes: tuple[DbtRelationWrite, ...],
        context: CompositionOccurrenceContext,
    ) -> CompositionDomainObservation:
        """Read all aliases together, rejecting drift before the catalog query."""
        domain.__post_init__()
        context.__post_init__()
        if not isinstance(writes, tuple) or not 1 <= len(writes) <= 8192:
            raise CompositionAdmissionError("physical_write_budget")
        connections = []
        for write in writes:
            if write.connector != domain.connector:
                raise CompositionAdmissionError("physical_binding_connector")
            connection = self._inputs.resolve_connection(context, write.connection_ref)
            # Recheck the exact connection that will be passed to the observer.
            reader = self._mssql_enrollment if domain.connector == "mssql" else self._clickhouse_enrollment
            if reader.resolve(connection, write, context) != domain:
                raise CompositionAdmissionError("physical_binding_drift")
            connections.append(connection)
        if domain.connector == "clickhouse":
            return self._clickhouse_enrollment.observe(domain, writes, connections[0], context)
        if domain.connector != "mssql":
            raise CompositionAdmissionError("physical_binding_connector")
        pin = mssql_target_pin(connections[0], writes[0])
        normalized = []
        for connection, write in zip(connections, writes):
            actual = mssql_target_pin(connection, write)
            if (actual.database_id, actual.database_guid, actual.create_token) != (
                pin.database_id,
                pin.database_guid,
                pin.create_token,
            ):
                raise CompositionAdmissionError("physical_binding_drift")
            normalized.append(replace(write, database=pin.database_name))
        request = MssqlWorkspaceObservationRequest(
            release_id=context.release_id,
            runtime_context_sha256=context.runtime_context_sha256,
            macro_authority_sha256=DBT_SQLSERVER_MACRO_AUTHORITY_BASELINE_SHA256,
            pin=pin,
            default_database=str(connections[0].credentials.database),
            invocation_databases=(pin.database_name,),
            writes=tuple(normalized),
        )
        observation = self._mssql_observer.observe(request, connections[0])
        if observation.request != request or tuple(slot.slot_id for slot in observation.slots) != tuple(
            range(len(writes))
        ):
            raise CompositionAdmissionError("physical_observation_closure")
        # A dependent view outside the complete selected footprint could be
        # dropped by pinned dbt macros. Never reserve only the direct targets.
        selected = {slot.object_id for slot in observation.slots if slot.object_id is not None}
        if any(
            edge.object_id not in selected or edge.database_arg != pin.database_name
            for edge in observation.dependencies
        ):
            raise CompositionAdmissionError("mssql_dependency_write_closure")
        return CompositionDomainObservation(
            domain,
            tuple(
                (dbt_relation_write_subject(write), slot.equivalence_class)
                for write, slot in zip(writes, observation.slots)
            ),
            observation.observation_sha256,
        )
