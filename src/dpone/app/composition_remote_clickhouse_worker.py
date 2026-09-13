"""Least-privilege remote worker selected from loader-verified registry metadata.

Only control and dedicated dispatcher credentials are resolved. The worker reads
retained parent authority but never admits an attempt or opens source/target
connections. A failed explicit remote profile cannot become local execution.
"""

from __future__ import annotations

import ssl
import time
from collections.abc import Mapping
from dataclasses import replace
from threading import Lock
from typing import Any
from uuid import uuid4

from dpone.adapters.composition_dispatch_v2_http_client import DispatchV2HttpClient
from dpone.adapters.composition_mssql_schema import require_control_schema
from dpone.adapters.composition_mssql_store import MssqlCompositionActivationStore
from dpone.adapters.dbt_runtime import build_hvac_kubernetes_vault_kv_v2_reader
from dpone.app.composition_authority_connections import CompositionAuthorityConnections
from dpone.app.composition_clickhouse_execution import (
    CompositionClickHouseExecutionRequest,
    derive_retained_clickhouse_attempt,
)
from dpone.app.composition_mssql_execution_deadline import BudgetedMssqlConnectorFactory
from dpone.app.composition_pack_cache import cache_root_from_environment
from dpone.app.composition_pack_execution_dispatcher import reopen_composition_plan
from dpone.contracts.composition_activation import CompositionOccurrenceContext
from dpone.contracts.composition_control import dbt_relation_write_subject
from dpone.contracts.composition_dispatch_v2 import DispatchV2Request, DispatchV2Response, decode_response
from dpone.contracts.composition_dispatcher_binding import (
    CompositionDispatcherBinding,
    require_dispatcher_connection_ref,
)
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.composition_persistence import CompositionAttemptIdentity, encode_attempt_identity
from dpone.contracts.composition_remote_transfer_result import RemoteTransferResult, decode_result, evidence_digest
from dpone.contracts.dbt_relation_writes import transfer_relation_write
from dpone.contracts.dbt_runtime import (
    AIRFLOW_DEPLOYMENT_IDENTITY_ENV,
    AIRFLOW_RUN_IDENTITY_ENV,
    DBT_WORKSPACE_AUTHORITY_CONNECTION_REF_ENV,
    airflow_attempt_from_environment,
    parse_airflow_deployment_identity_json,
    parse_airflow_run_identity_json,
)
from dpone.contracts.dbt_workspace_attempt import require_workspace_authority_connection_ref
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.manifest.bounded_yaml import load_bounded_yaml
from dpone.runtime.composition_verified_dispatch import CompositionDispatchRequest
from dpone.runtime.credentials.runtime_context import RuntimeConnectionContextLoader


def _require(condition: bool) -> None:
    if not condition:
        raise CompositionAdmissionError("remote_clickhouse_unverified")


def _entry(runtime: Any, alias: str) -> tuple[str, Mapping[str, Any]]:
    require_dispatcher_connection_ref(alias)
    name = runtime.binding_set["bindings"][alias]["connection_ref"]
    require_dispatcher_connection_ref(name)
    entry = runtime.connection_registry["connections"][name]
    _require(isinstance(entry, Mapping))
    return name, entry


class _ControlInputs:
    def __init__(self, context: CompositionOccurrenceContext, reference: str, resolved: Any):
        self._context, self._reference, self._resolved = context, reference, resolved

    def resolve_connection(self, context: CompositionOccurrenceContext, connection_ref: str) -> Any:
        _require(context == self._context and connection_ref == self._reference)
        return self._resolved


def _verified_manifest(plan: Any, manifest: Mapping[str, Any]) -> None:
    workload_id = manifest["name"]
    originals = [body for name, body in plan.sources.transfer_manifests if name == workload_id]
    _require(len(originals) == 1)
    _require(canonical_json_bytes(load_bounded_yaml(originals[0])) == canonical_json_bytes(manifest))
    workloads = [row for row in plan.workloads if row.workload_id == workload_id]
    writes = [row for row in plan.writes if row.kind == "transfer" and row.resource_id == workload_id]
    _require(len(workloads) == 1 and len(writes) == 1)
    write = writes[0]
    _require(
        workloads[0].execution_cell == "mssql_clickhouse_full_refresh_v1"
        and workloads[0].write_subjects == (dbt_relation_write_subject(write),)
        and write.connector == "clickhouse"
        and transfer_relation_write(
            project_path=write.project_path,
            workflow_id=write.workflow_id,
            workload_id=workload_id,
            manifest=manifest,
        )
        == write
    )


def _read_parent(runtime: Any, identity: Any, control_ref: str, plan: Any) -> Any:
    context = CompositionOccurrenceContext(
        identity.activation_id,
        runtime.environment,
        identity.release_id,
        identity.deployment_id,
        None,
        runtime.authority_subject_sha256,
    )
    _require((runtime.release_id, runtime.deployment_id) == (identity.release_id, identity.deployment_id))
    resolved = runtime.resolver.resolve(control_ref)
    _require(resolved.descriptor is not None and resolved.descriptor.connection_type == "mssql")
    service = resolved.descriptor.properties["composition_service_id"]
    schema = require_control_schema(resolved.descriptor.properties.get("composition_control_schema", "dpone_control"))
    deadline = time.monotonic() + 30.0
    connections = CompositionAuthorityConnections(
        control_schema=schema,
        inputs=_ControlInputs(context, control_ref, resolved),
        authority_connection_ref=control_ref,
        connector_factory=BudgetedMssqlConnectorFactory(io_deadline=lambda: deadline),
    )
    store = MssqlCompositionActivationStore(
        lambda: connections.control_connection(context),
        expected_service_id=service,
        control_schema=schema,
    )
    retained = store.read(identity.activation_id)
    if retained is None or time.monotonic() >= deadline:
        raise CompositionAdmissionError("remote_clickhouse_unverified")
    actual = retained.request.context
    _require(
        all(
            getattr(actual, key) == getattr(context, key)
            for key in (
                "activation_id",
                "environment",
                "release_id",
                "deployment_id",
                "runtime_context_sha256",
            )
        )
    )
    _require(retained.request.source_subject_sha256 == plan.sources.subject_sha256)
    _require(retained.request.workloads == plan.workloads)
    # The predecessor remains the SQL original; it is not replaced by projection None.
    return retained


def _client(resolved: Any) -> DispatchV2HttpClient:
    _require(resolved.descriptor is not None and resolved.descriptor.connection_type == "api")
    credentials = resolved.credentials
    accept, execution = credentials.connect_timeout, credentials.send_receive_timeout
    _require(type(accept) is int and 1 <= accept <= 30)
    _require(type(execution) is int and 1 <= execution <= 900)
    context = ssl.create_default_context(cafile=credentials.ssl_ca_location)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    return DispatchV2HttpClient(
        credentials.endpoint,
        ssl_context=context,
        bearer_token=credentials.token,
        accept_timeout_seconds=accept,
        execution_timeout_seconds=execution,
    )


class RemoteClickHouseNonSuccess(CompositionAdmissionError):
    """Validated non-success evidence for the worker's nonpassing evidence adapter.

    Transport errors and malformed replies never create this exception. The
    canonical response retains its complete correlated receipt, without secrets.
    """

    def __init__(self, response: DispatchV2Response, *, request: DispatchV2Request) -> None:
        self._response = decode_response(response.to_bytes(), request)
        _require(self._response.status in {"FAILED", "IN_PROGRESS", "UNKNOWN"})
        super().__init__("remote_clickhouse_non_success")

    @property
    def response(self) -> DispatchV2Response:
        return self._response

    @property
    def response_document(self) -> bytes:
        return self._response.to_bytes()


class RemoteClickHouseExecutionRoot:
    """One configured metadata-only submission, never a local-row result or permit."""

    def __init__(
        self,
        *,
        expected: CompositionClickHouseExecutionRequest,
        candidate: CompositionAttemptIdentity,
        dispatcher_id: str,
        runtime_authority_sha256: str,
        client: DispatchV2HttpClient,
    ) -> None:
        self._manifest = canonical_json_bytes(expected.manifest)
        self._expected = replace(expected, manifest=strict_json_object(self._manifest))
        self._candidate = candidate
        self._dispatcher, self._authority = dispatcher_id, runtime_authority_sha256
        self._client = client
        self._submitted = False
        self._submit_lock = Lock()

    def can_execute_attempt(self) -> bool:
        """Report installed collaborators only; this is not live certification."""
        return callable(getattr(self._client, "call", None)) and not self._submitted

    def execute(self, request: CompositionClickHouseExecutionRequest) -> RemoteTransferResult:
        """Submit the exact verified candidate once; uncertainty never triggers replay."""
        try:
            wire = self._wire(request, "EXECUTE_TRANSFER")
            with self._submit_lock:
                _require(not self._submitted)
                self._submitted = True
            response = self._client.call(wire)
            response = decode_response(response.to_bytes(), wire)
            if response.status != "SUCCEEDED":
                raise RemoteClickHouseNonSuccess(response, request=wire)
            return decode_result(
                response.evidence_document, evidence_digest(response.evidence_document), attempt=self._candidate
            )
        except RemoteClickHouseNonSuccess:
            raise
        except Exception:
            raise CompositionAdmissionError("remote_clickhouse_unknown") from None

    def _wire(self, request: CompositionClickHouseExecutionRequest, operation: str) -> DispatchV2Request:
        _require(type(request) is CompositionClickHouseExecutionRequest)
        _require(canonical_json_bytes(request.manifest) == self._manifest and request == self._expected)
        return DispatchV2Request(
            str(uuid4()),
            self._dispatcher,
            self._authority,
            operation,
            canonical_json_bytes(
                {
                    "attempt_document": strict_json_object(encode_attempt_identity(self._candidate)),
                    "attempt_sha256": self._candidate.attempt_sha256,
                    "airflow_run_identity": request.run_identity.to_dict(),
                    "airflow_attempt": request.airflow_attempt.to_dict(),
                }
            ),
        )

    def read_status(self, request: CompositionClickHouseExecutionRequest) -> DispatchV2Response:
        """Observe retained status once, before or after execution, without admission.

        Absence remains a validated UNKNOWN observation. This method neither
        consumes nor restores execution submission, and never retries transport.
        """
        try:
            wire = self._wire(request, "READ_STATUS")
            response = self._client.call(wire)
            return decode_response(response.to_bytes(), wire)
        except Exception:
            raise CompositionAdmissionError("remote_clickhouse_status_unknown") from None


def compose_remote_clickhouse_pack_root(
    *,
    request: CompositionDispatchRequest,
    manifest: Mapping[str, Any],
) -> RemoteClickHouseExecutionRoot | None:
    """Select signed remote configuration before resolving any privileged binding.

    None means the signed target entry has no dispatcher property. Every other
    failure is closed, including absent projected control authority and unavailable
    historical parent observations. The signed control descriptor may specify
    composition_control_schema; omission retains dpone_control. Its validated
    value selects both protected SQL readers, never an ambient/request override.
    Only the service may admit or execute effects.
    """
    try:
        environment = dict(request.env)
        runtime = RuntimeConnectionContextLoader(
            vault_reader_factory=build_hvac_kubernetes_vault_kv_v2_reader,
        ).load(environment)
        if runtime is None or request.kind != "ordinary_transfer":
            raise CompositionAdmissionError("remote_clickhouse_unverified")
        authority = runtime.authority_subject_sha256
        if not isinstance(authority, str):
            raise CompositionAdmissionError("remote_clickhouse_unverified")
        target_name, target = _entry(runtime, manifest["sink"]["connection_ref"])
        _require(target["type"] == "clickhouse" and isinstance(target["connection"], Mapping))
        if "composition_dispatcher" not in target["connection"]:
            return None
        binding = CompositionDispatcherBinding.from_mapping(target["connection"]["composition_dispatcher"])
        identity = parse_airflow_deployment_identity_json(environment.get(AIRFLOW_DEPLOYMENT_IDENTITY_ENV, ""))
        plan = reopen_composition_plan(cache_root_from_environment(environment), identity.release_id)
        _verified_manifest(plan, manifest)
        source_name, source = _entry(runtime, manifest["source"]["connection_ref"])
        _require(source["type"] == "mssql")
        control_ref = require_workspace_authority_connection_ref(
            environment.get(DBT_WORKSPACE_AUTHORITY_CONNECTION_REF_ENV)
        )
        control_name, control = _entry(runtime, control_ref)
        api_name, api = _entry(runtime, binding.connection_ref)
        _require(control["type"] == "mssql" and api["type"] == "api")
        _require(
            control_name not in {source_name, target_name} and api_name not in {source_name, target_name, control_name}
        )
        retained = _read_parent(runtime, identity, control_ref, plan)
        run = parse_airflow_run_identity_json(environment.get(AIRFLOW_RUN_IDENTITY_ENV, ""))
        scheduler = airflow_attempt_from_environment(environment, run)
        candidate = derive_retained_clickhouse_attempt(
            retained,
            manifest=manifest,
            plan_sha256=plan.sources.subject_sha256,
            run_identity=run,
            airflow_attempt=scheduler,
        )
        expected = CompositionClickHouseExecutionRequest(manifest, plan.sources.subject_sha256, run, scheduler)
        return RemoteClickHouseExecutionRoot(
            expected=expected,
            candidate=candidate,
            dispatcher_id=binding.dispatcher_id,
            runtime_authority_sha256=authority,
            client=_client(runtime.resolver.resolve(binding.connection_ref)),
        )
    except Exception:
        raise CompositionAdmissionError("remote_clickhouse_unverified") from None
