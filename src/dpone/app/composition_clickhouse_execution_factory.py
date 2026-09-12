"""Compose the MSSQL→ClickHouse worker from verified parent authority."""

from __future__ import annotations

from typing import Any

from dpone.adapters.composition_mssql_attempts import MssqlCompositionAttemptStore
from dpone.app.composition_clickhouse_execution import (
    CompositionClickHouseExecutionDependencies,
    CompositionClickHouseExecutionRoot,
)
from dpone.app.composition_dbt_execution_factory import CompositionDbtControlAuthority
from dpone.contracts.composition_activation import CompositionAdmissionError


def build_composition_clickhouse_execution_dependencies(
    *,
    control: CompositionDbtControlAuthority,
    read_active: Any,
    gate: Any,
    publisher_gate: Any,
    publisher: Any,
    bind_transport: Any,
    read_source: Any,
    require_enrollment: Any,
    outcome_observer: Any,
    target: Any,
    attach_publisher_transport: Any | None = None,
) -> CompositionClickHouseExecutionDependencies:
    """Return protected collaborators of one supervised ClickHouse cell."""

    control.__post_init__()
    if not callable(bind_transport) or not callable(read_source) or not callable(require_enrollment):
        raise CompositionAdmissionError("clickhouse_transport_binding")
    return CompositionClickHouseExecutionDependencies(
        read_active=read_active,
        attempts=MssqlCompositionAttemptStore(
            control.connection_factory,
            expected_service_id=control.expected_service_id,
            control_schema=control.control_schema,
        ),
        gate=gate,
        publisher_gate=publisher_gate,
        publisher=publisher,
        bind_transport=bind_transport,
        read_source=read_source,
        require_enrollment=require_enrollment,
        outcome_observer=outcome_observer,
        target=target,
        expected_service_id=control.expected_service_id,
        attach_publisher_transport=attach_publisher_transport,
    )


def build_composition_clickhouse_execution_root(
    **kwargs: Any,
) -> CompositionClickHouseExecutionRoot:
    """Construct the ClickHouse root from verified parent authority."""

    dependencies = kwargs.get("dependencies")
    if not isinstance(dependencies, CompositionClickHouseExecutionDependencies):
        dependencies = build_composition_clickhouse_execution_dependencies(**kwargs)
    return CompositionClickHouseExecutionRoot(dependencies)


__all__ = [
    "build_composition_clickhouse_execution_dependencies",
    "build_composition_clickhouse_execution_root",
]
