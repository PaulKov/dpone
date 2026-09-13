"""Execute one verified whole cell without reconstructing an existing attempt.

The staged loader is an injected protected verifier, composed at startup from
the exact configuration catalog. Control inspection precedes business credential
resolution. Capture custody, source and target clients share one request budget;
only retained SQL receipts and independently reopened terminal originals can
become responses. No transport result or local row value establishes success.
"""

from __future__ import annotations

from dataclasses import asdict
from typing import Any, cast

from dpone.adapters.composition_clickhouse_gate import ClickHouseSupervisorObserver
from dpone.adapters.composition_clickhouse_supervisor import RemoteClickHouseLocalSupervisor
from dpone.adapters.composition_clickhouse_supervisor_enrollment import (
    ClickHouseSupervisorEnrollment,
    read_service_enrollment,
)
from dpone.adapters.composition_mssql_attempts import composition_control_transaction
from dpone.app.composition_clickhouse_cell_factory import ProtectedClickHouseCell, build_protected_clickhouse_cell
from dpone.app.composition_clickhouse_execution import (
    CompositionClickHouseExecutionRequest,
    CompositionClickHouseExecutionRoot,
)
from dpone.app.composition_dispatcher_attempt_authority import DispatcherAttemptAuthority
from dpone.app.composition_dispatcher_capture_custody import DispatcherCaptureCustody
from dpone.app.composition_dispatcher_context import StagedDispatcherAttempt, StagedDispatcherContextLoader
from dpone.app.composition_dispatcher_service_config import DispatcherServiceConfig, decode_dispatcher_service_config
from dpone.app.composition_mssql_execution_deadline import BudgetedMssqlConnectorFactory
from dpone.contracts.airflow_correlation import AirflowAttemptCorrelation
from dpone.contracts.airflow_run_identity import AirflowRunIdentity
from dpone.contracts.composition_dispatch_v2 import DispatchV2Request, DispatchV2Response
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.composition_persistence import CompositionAttemptReceipt
from dpone.contracts.composition_remote_transfer_result import evidence_digest
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.ports.composition_execution import ExecutionBudget


def _require(condition: bool) -> None:
    if not condition:
        raise CompositionAdmissionError("dispatcher_transfer_unknown")


def _status(
    request: DispatchV2Request,
    receipt: CompositionAttemptReceipt,
    *,
    duplicate: bool,
) -> DispatchV2Response:
    """Serialize the observed receipt; this never creates or changes SQL state."""
    receipt.__post_init__()
    _require(receipt.attempt == request.attempt)
    status = {"FAILED": "FAILED", "COMMIT_UNKNOWN": "UNKNOWN", "RUNNING": "IN_PROGRESS" if duplicate else "UNKNOWN"}[
        receipt.state
    ]
    observed = canonical_json_bytes(
        {
            "schema": "dpone.composition-remote-attempt-observation.v1",
            "attempt_document": {"schema": "dpone.composition-attempt.v1", **asdict(receipt.attempt)},
            "attempt_sha256": receipt.attempt.attempt_sha256,
            "state": receipt.state,
            "closed_gates_sha256": receipt.closed_gates_sha256,
            "quiescence_sha256": receipt.quiescence_sha256,
            "outcome_evidence_sha256": receipt.outcome_evidence_sha256,
        }
    )
    document = canonical_json_bytes(
        {
            "schema": "dpone.composition-remote-transfer-status.v1",
            "receipt": {"document": strict_json_object(observed), "sha256": evidence_digest(observed)},
            "references": [],
        }
    )
    return DispatchV2Response.for_request(request, document, status=status)


class DispatcherTransferHandler:
    """Concrete default graph for an authenticated metadata-only v2 request.

    ``loader`` must be the protected staged verifier constructed from this exact
    configuration's authority-to-context-digest catalog. No loader or connector
    is discovered from request metadata or the ambient process environment.
    """

    def __init__(self, config: DispatcherServiceConfig, loader: StagedDispatcherContextLoader) -> None:
        try:
            _require(type(config) is DispatcherServiceConfig and callable(loader.load_attempt))
            self._config = decode_dispatcher_service_config(
                config.document,
                expected_sha256=config.configuration_sha256,
                bootstrap_uid=config.dispatcher_uid,
                bootstrap_gid=config.dispatcher_gid,
            )
            _require(self._config == config)
            self._loader = loader
        except Exception:
            raise CompositionAdmissionError("dispatcher_transfer_unknown") from None

    def __call__(self, request: DispatchV2Request, budget: ExecutionBudget) -> DispatchV2Response:
        """Inspect first, execute at most once, then reopen authoritative outcome."""
        try:
            _require(type(request) is DispatchV2Request)
            request.__post_init__()
            budget.io_deadline()
            _require(request.dispatcher_id == self._config.dispatcher_id)
            binding = self._config.authorities[request.runtime_authority_sha256]
            selected = self._loader.load_attempt(request.runtime_authority_sha256, request.attempt)
            context = selected.context
            _require(
                selected.attempt == request.attempt
                and context.binding.dispatcher_id == self._config.dispatcher_id
                and context.binding.identity_kind == self._config.binding_identity_kind
                and context.binding.identity_sha256 == self._config.binding_identity_sha256
                and context.runtime.authority_subject_sha256 == request.runtime_authority_sha256
                and context.occurrence.runtime_context_sha256 == request.runtime_authority_sha256
                and selected.write.connection_ref == context.target_binding_ref
            )
            authority = DispatcherAttemptAuthority(
                selected,
                control_connection_ref=binding.control_connection_ref,
                expected_control_service_id=binding.expected_control_service_id,
                control_schema=binding.control_schema,
                connector_factory=BudgetedMssqlConnectorFactory(io_deadline=budget.io_deadline),
            )
            subject = strict_json_object(request.subject)
            run = AirflowRunIdentity.from_mapping(subject["airflow_run_identity"])
            airflow = AirflowAttemptCorrelation.from_mapping(subject["airflow_attempt"])
            if request.operation == "READ_STATUS":
                receipt = authority.inspect_existing(run, airflow)
                if receipt is None:
                    response = DispatchV2Response.for_request(
                        request,
                        canonical_json_bytes(
                            {
                                "schema": "dpone.composition-remote-transfer-absence.v1",
                                "attempt_sha256": request.attempt.attempt_sha256,
                                "observation": "ABSENT",
                            }
                        ),
                        status="UNKNOWN",
                    )
                    budget.io_deadline()
                    return response
            else:
                receipt = authority.inspect(run, airflow)
            if receipt is not None and receipt.state != "SUCCEEDED":
                return self._respond(request, receipt, budget, duplicate=True)
            if receipt is None:
                budget.require_effect()
            parent, enrollment = self._enrolled_parent(selected, authority)
            custody = DispatcherCaptureCustody(
                enrollment=enrollment,
                socket_path=self._config.host_probe_socket,
                deadline=budget.execution_deadline,
                absolute_deadline=budget.io_deadline,
            )
            files = custody.open_files()
            try:
                # Acquisition owns a descriptor before __enter__ performs its
                # fresh custody check. A failed entry still requires close.
                files.__enter__()
                cell = self._cell(parent, selected, enrollment, custody, files, budget)
                if receipt is None:
                    try:
                        budget.require_effect()
                        CompositionClickHouseExecutionRoot(cell.dependencies).execute(
                            CompositionClickHouseExecutionRequest(
                                manifest=selected.manifest,
                                plan_sha256=selected.attempt.plan_sha256,
                                run_identity=run,
                                airflow_attempt=airflow,
                            )
                        )
                    except Exception:
                        # Lost ACK, expiry and execution failure require a fresh
                        # receipt; never refund admission or replay source work.
                        budget.begin_cleanup()
                    receipt = authority.inspect(run, airflow)
                _require(receipt is not None)
                assert receipt is not None
                return self._respond(request, receipt, budget, duplicate=False, cell=cell)
            finally:
                files.close()
        except Exception:
            raise CompositionAdmissionError("dispatcher_transfer_unknown") from None

    def _enrolled_parent(
        self,
        selected: StagedDispatcherAttempt,
        authority: DispatcherAttemptAuthority,
    ) -> tuple[dict[str, Any], ClickHouseSupervisorEnrollment]:
        context, control = selected.context, authority.control
        target = context.runtime.resolver.resolve(context.target_binding_ref)
        _require(target.descriptor is not None and target.descriptor.connection_type == "clickhouse")
        assert target.descriptor is not None
        service = target.descriptor.properties.get("composition_service_id")
        _require(type(service) is str)
        assert isinstance(service, str)
        with composition_control_transaction(
            control.connection_factory,
            control.control_schema,
            control.expected_service_id,
        ) as ledger:
            enrollment = read_service_enrollment(ledger, service)
        enrollment.__post_init__()
        body = enrollment.body
        _require(
            enrollment.enrollment_sha256 == self._config.supervisor_enrollment_sha256
            and body["schema"] == "dpone.composition-clickhouse-supervisor-enrollment.v2"
            and body["service_id"] == service
        )
        policy = body["policy"]["capture_custody"]
        _require(
            canonical_json_bytes({name: policy[name] for name in ("profile", "uid", "gid", "destination")})
            == canonical_json_bytes(
                {
                    "profile": self._config.capture_custody,
                    "uid": self._config.dispatcher_uid,
                    "gid": self._config.dispatcher_gid,
                    "destination": str(self._config.capture_root),
                }
            )
            and canonical_json_bytes(body["facts"]["linux"]["capture_custody"]["root_identity"])
            == canonical_json_bytes(asdict(self._config.capture_root_identity))
        )
        return {
            "control": control,
            "target": target,
            "resolver": context.runtime.resolver,
            "read_active": authority.read_active,
            "context": context.occurrence,
        }, enrollment

    def _cell(
        self,
        parent: dict[str, Any],
        selected: StagedDispatcherAttempt,
        enrollment: ClickHouseSupervisorEnrollment,
        custody: DispatcherCaptureCustody,
        files: Any,
        budget: ExecutionBudget,
    ) -> ProtectedClickHouseCell:
        supervisor = RemoteClickHouseLocalSupervisor(
            enrollment_sha256=enrollment.enrollment_sha256,
            socket_path=self._config.host_probe_socket,
            dispatcher_gid=self._config.dispatcher_gid,
            absolute_deadline=budget.io_deadline,
        )
        return build_protected_clickhouse_cell(
            parent=parent,
            manifest=selected.manifest,
            plan=selected.context.plan,
            attempt=selected.attempt,
            write=selected.write,
            enrollment=enrollment,
            supervisor=cast(ClickHouseSupervisorObserver, supervisor),
            files=files,
            root=self._config.capture_root,
            require_custody=custody.require_host,
            require_enrollment_in=custody.require_enrollment_in,
            absolute_deadline=budget.io_deadline,
            require_effect=budget.require_effect,
            begin_cleanup=budget.begin_cleanup,
            source_connector_factory=BudgetedMssqlConnectorFactory(
                require_execution=budget.require_effect,
                io_deadline=budget.io_deadline,
            ),
        )

    @staticmethod
    def _respond(
        request: DispatchV2Request,
        receipt: CompositionAttemptReceipt,
        budget: ExecutionBudget,
        *,
        duplicate: bool,
        cell: ProtectedClickHouseCell | None = None,
    ) -> DispatchV2Response:
        receipt.__post_init__()
        _require(receipt.attempt == request.attempt)
        budget.io_deadline()
        if receipt.state in {"SUCCEEDED", "FAILED"}:
            _require(not budget.execution_expired)
        if receipt.state == "SUCCEEDED":
            _require(cell is not None)
            assert cell is not None
            result = cell.terminal.read_result(request.attempt)
            response = DispatchV2Response.for_request(request, result.document, status="SUCCEEDED")
        else:
            response = _status(request, receipt, duplicate=duplicate)
        # Evidence decoding itself may consume the remainder of the deadline;
        # cleanup I/O authority does not extend terminal-response authority.
        budget.io_deadline()
        if receipt.state in {"SUCCEEDED", "FAILED"}:
            _require(not budget.execution_expired)
        return response
