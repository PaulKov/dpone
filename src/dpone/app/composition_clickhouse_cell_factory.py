"""One concrete ClickHouse capture/ingest/publication/terminal dependency graph.

Callers supply already verified parent/workload selection and explicit custody
collaborators. This constructor neither chooses a profile from environment nor
selects a relation by name. File lifetime belongs to its caller. Construction
resolves the configured source but performs no source/target SQL or host reads.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.adapters.composition_clickhouse_admin import ClickHousePrincipalHttpClient
from dpone.adapters.composition_clickhouse_gate import ClickHouseSupervisorObserver, MssqlClickHouseGate
from dpone.adapters.composition_clickhouse_principal import ClickHousePrincipalAdmin
from dpone.adapters.composition_clickhouse_supervisor_enrollment import (
    require_attempt_enrollment_original,
)
from dpone.adapters.composition_clickhouse_terminal_store import ClickHouseTerminalStore
from dpone.adapters.composition_clickhouse_transport import ClickHouseDispatchTransport
from dpone.adapters.composition_mssql_attempts import MssqlCompositionAttemptStore, composition_control_transaction
from dpone.adapters.composition_snapshot_sql_store import MssqlSnapshotPublicationStore
from dpone.app.composition_clickhouse_capture_factory import build_clickhouse_capture_components
from dpone.app.composition_clickhouse_execution import (
    CompositionClickHouseExecutionDependencies,
    CompositionClickHouseExecutionRoot,
)
from dpone.app.composition_clickhouse_publication import (
    ClickHouseDispatchBudgetPolicy,
    ClickHouseHttpSnapshotExecutor,
    ObservingSnapshotPublicationAuthority,
    clickhouse_http_endpoint,
    snapshot_limits_from_manifest,
    snapshot_target_for_write,
)
from dpone.app.composition_clickhouse_terminal import ClickHouseTerminalWorkerGate
from dpone.app.composition_dbt_execution_factory import CompositionDbtControlAuthority
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_snapshot import SnapshotLimits
from dpone.contracts.composition_snapshot_capture import attempt_snapshot_target
from dpone.runtime.composition_snapshot import ClickHouseAtomicSnapshotPublisher
from dpone.runtime.credentials.resolved_connector_factory import ResolvedConnectorFactory


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
    can_classify_publication: bool = False,
    limits: SnapshotLimits | None = None,
    capture: Any | None = None,
    worker_gate: Any | None = None,
    require_effect: Callable[[], None] | None = None,
) -> CompositionClickHouseExecutionDependencies:
    """Return protected collaborators of one supervised ClickHouse cell."""

    control.__post_init__()
    if not callable(bind_transport) or not callable(read_source) or not callable(require_enrollment):
        raise CompositionAdmissionError("clickhouse_transport_binding")
    if any(item is None for item in (gate, publisher_gate, publisher, outcome_observer, target)):
        raise CompositionAdmissionError("clickhouse_publisher")
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
        can_classify_publication=can_classify_publication,
        limits=limits,
        capture=capture,
        worker_gate=worker_gate,
        require_effect=require_effect,
    )


def build_composition_clickhouse_execution_root(
    **kwargs: Any,
) -> CompositionClickHouseExecutionRoot:
    """Construct the ClickHouse root from verified parent authority."""

    dependencies = kwargs.get("dependencies")
    if not isinstance(dependencies, CompositionClickHouseExecutionDependencies):
        dependencies = build_composition_clickhouse_execution_dependencies(**kwargs)
    return CompositionClickHouseExecutionRoot(dependencies)


@dataclass(frozen=True, slots=True, repr=False)
class ProtectedClickHouseCell:
    """The real execution dependencies and their shared retained-result reader."""

    dependencies: CompositionClickHouseExecutionDependencies
    terminal: ClickHouseTerminalStore
    capture: Any


def build_protected_clickhouse_cell(
    *,
    parent: Mapping[str, Any],
    manifest: Mapping[str, Any],
    plan: Any,
    attempt: Any,
    write: Any,
    enrollment: Any,
    supervisor: ClickHouseSupervisorObserver,
    files: Any,
    root: Path,
    require_custody: Callable[[], None] | None = None,
    require_enrollment_in: Callable[..., None] | None = None,
    absolute_deadline: Callable[[], float] | None = None,
    require_effect: Callable[[], None] | None = None,
    begin_cleanup: Callable[[], Any] | None = None,
    source_connector_factory: Callable[..., Any] = ResolvedConnectorFactory.create,
) -> ProtectedClickHouseCell:
    """Compose actual collaborators after the caller verified all authority.

    Host custody observations must be SQL-free; enrollment checks use the
    supplied capture-ledger transaction. Neither callback runs during assembly.
    Passing both as None preserves the independently verified root-owned profile.
    Optional deadlines reach every HTTP client. Effect guards precede source
    opening and target mutations; cleanup phase changes remain caller-owned.
    """
    if (
        parent.get("context") is None
        or parent.get("resolver") is None
        or not callable(getattr(supervisor, "observe", None))
        or any(not callable(getattr(files, name, None)) for name in ("read", "write_once"))
        or any(value is not None and not callable(value) for value in (require_custody, require_enrollment_in))
        or any(value is not None and not callable(value) for value in (absolute_deadline, require_effect))
        or not callable(source_connector_factory)
        or (require_custody is None) != (require_enrollment_in is None)
    ):
        raise CompositionAdmissionError("clickhouse_cell_configuration")
    if begin_cleanup is not None and not callable(begin_cleanup):
        raise CompositionAdmissionError("clickhouse_cell_configuration")
    control = parent["control"]
    control.__post_init__()
    target = attempt_snapshot_target(snapshot_target_for_write(enrollment, write), attempt)
    limits = snapshot_limits_from_manifest(manifest)
    endpoint, admin_credentials, ca_file = clickhouse_http_endpoint(parent["target"])
    components = build_clickhouse_capture_components(
        parent=parent,
        manifest=manifest,
        plan=plan,
        attempt=attempt,
        target=target,
        limits=limits,
        root=root,
        files=files,
        require_custody=require_custody,
        require_enrollment_in=require_enrollment_in,
        absolute_deadline=absolute_deadline,
        require_effect=require_effect,
        source_connector_factory=source_connector_factory,
        endpoint=endpoint,
        credentials=admin_credentials,
        ca_file=ca_file,
    )
    admin = ClickHousePrincipalAdmin(
        ClickHousePrincipalHttpClient(
            endpoint=endpoint,
            credentials=admin_credentials,
            timeout_seconds=30.0,
            ca_file=ca_file,
            absolute_deadline=absolute_deadline,
            require_effect=require_effect,
        )
    )
    policy = ClickHouseDispatchBudgetPolicy(limits)
    gate = MssqlClickHouseGate(
        control.connection_factory,
        expected_service_id=control.expected_service_id,
        target=target,
        purpose="ingest",
        require_effect=require_effect,
        principal_admin=admin,
        supervisor=supervisor,
        dispatch_policy=policy,
        enrollment_sha256=enrollment.enrollment_sha256,
        control_schema=control.control_schema,
    )
    publisher_gate = MssqlClickHouseGate(
        control.connection_factory,
        expected_service_id=control.expected_service_id,
        target=target,
        purpose="publisher",
        require_effect=require_effect,
        principal_admin=admin,
        supervisor=supervisor,
        dispatch_policy=policy,
        enrollment_sha256=enrollment.enrollment_sha256,
        control_schema=control.control_schema,
    )
    executor = ClickHouseHttpSnapshotExecutor()

    authority = ObservingSnapshotPublicationAuthority(
        read_active=parent["read_active"],
        connection_factory=control.connection_factory,
        control_schema=control.control_schema,
        target=target,
        limits=limits,
        enrollment_sha256=enrollment.enrollment_sha256,
        load_generation=components.capture.load_generation,
        load_generation_in=components.capture.load_generation_in,
        publisher_gate=publisher_gate,
        ingest_gate=gate,
        control_service_id=control.expected_service_id,
    )
    store = MssqlSnapshotPublicationStore(
        control.connection_factory,
        expected_service_id=control.expected_service_id,
        authority=authority,
        attempt=attempt,
        control_schema=control.control_schema,
    )
    catalog = components.catalog
    publisher = ClickHouseAtomicSnapshotPublisher(
        authority=authority,
        store=store,
        catalog=catalog,
        executor=executor,
    )

    def bind_transport(credentials: Any, journal: Any) -> ClickHouseDispatchTransport:
        return ClickHouseDispatchTransport(
            endpoint=endpoint,
            credentials=credentials,
            journal=journal,
            timeout_seconds=60.0,
            ca_file=ca_file,
            absolute_deadline=absolute_deadline,
            require_effect=require_effect,
        )

    def require_enrollment(attempt: Any, phase: str) -> bytes:
        if phase not in {"dispatch", "publication"}:
            raise CompositionAdmissionError("clickhouse_enrollment")
        with composition_control_transaction(
            control.connection_factory, control.control_schema, control.expected_service_id
        ) as ledger:
            original = require_attempt_enrollment_original(ledger, enrollment.enrollment_sha256, attempt, target)
        return original.document

    if components.store is None:
        raise CompositionAdmissionError("snapshot_capture_storage")
    terminal = ClickHouseTerminalStore(
        control.connection_factory,
        expected_service_id=control.expected_service_id,
        control_schema=control.control_schema,
        target=target,
        capture_store=components.store,
        publication_store=store,
        load_generation_in=components.capture.load_generation_in,
    )
    worker_gate = ClickHouseTerminalWorkerGate(
        ingest=gate, publisher=publisher_gate, terminal=terminal, begin_cleanup=begin_cleanup
    )

    dependencies = build_composition_clickhouse_execution_dependencies(
        control=control,
        read_active=parent["read_active"],
        gate=gate,
        publisher_gate=publisher_gate,
        publisher=publisher,
        bind_transport=bind_transport,
        read_source=components.source,
        capture=components.capture,
        require_enrollment=require_enrollment,
        outcome_observer=worker_gate,
        worker_gate=worker_gate,
        require_effect=require_effect,
        target=target,
        attach_publisher_transport=executor.attach,
        can_classify_publication=catalog.can_observe_capture(),
        limits=limits,
    )

    return ProtectedClickHouseCell(dependencies, terminal, components.capture)
