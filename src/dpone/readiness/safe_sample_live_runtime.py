"""Safe composition root for explicit live safe-sample execution.

This module is the only boundary that combines authoring facts, immutable
deployment identities, runtime credential adapters, and database clients. It
performs all local validation before returning ports that can perform I/O.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.manifest.authoring import AuthoringCompiler, AuthoringSourceDependency
    from dpone.readiness.route_attestation import RouteAttestationSignatureVerifier, RouteAttestationVerification
    from dpone.services.safe_sample_execution_plan import SafeSampleExecutionPlan
    from dpone.services.safe_sample_runtime_executor import SafeSampleDataCopier


import importlib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.contracts.connector_declarations import canonical_endpoint_type
from dpone.gitops.schema_validation import GitOpsSchemaValidator
from dpone.manifest.authoring import default_authoring_compiler
from dpone.readiness.credential_runtime_checks import validate_credential_runtime
from dpone.readiness.route_attestation import expected_subject_for_safe_sample, verify_route_attestation_files
from dpone.readiness.route_attestation_files import RouteAttestationFileError, read_bounded_file
from dpone.readiness.safe_sample_clickhouse_target import CredentialResolvingClickHouseTemporaryTargetAdapter
from dpone.readiness.safe_sample_live_errors import LiveSafeSampleRuntimeAssemblyError
from dpone.readiness.safe_sample_live_integrity import verify_pinned_environment_inputs, verify_pinned_source
from dpone.runtime.credentials.binding_resolver import BindingCredentialResolver
from dpone.runtime.credentials.runtime_context import build_required_vault_kv_reader
from dpone.runtime.credentials.workload_scope import WorkloadScopedCredentialResolver
from dpone.services.mssql_clickhouse_safe_sample_runtime_assembly import (
    build_mssql_clickhouse_safe_sample_sql_registry_from_pipeline_source,
)
from dpone.services.safe_sample_data_copier_registry import RegistryBackedSafeSampleDataCopier
from dpone.services.safe_sample_live_authorization import SafeSampleLiveExecutionAuthorizer
from dpone.services.safe_sample_policy import load_pipeline_source_snapshot_from_file
from dpone.services.safe_sample_target_lifecycle import TemporaryTargetLifecycleExecutor

_LIVE_INPUT_MAX_BYTES = 4 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class LiveSafeSampleRuntimeInputs:
    pipeline_source: dict[str, Any]
    pipeline_source_path: Path
    pipeline_source_sha256: str
    binding_set: dict[str, Any]
    connection_registry: dict[str, Any]
    credential_runtime: dict[str, Any]
    pipeline_source_dependencies: tuple[AuthoringSourceDependency, ...] = ()


class LiveSafeSampleRuntimeInputLoader:
    """Load local, non-secret live inputs without resolving credentials."""

    def __init__(self, *, authoring_compiler: AuthoringCompiler | None = None) -> None:
        self._authoring_compiler = authoring_compiler or default_authoring_compiler()

    def load(
        self,
        *,
        pipeline_source_path: str | Path,
        binding_set_path: str | Path,
        connection_registry_path: str | Path,
        credential_runtime_path: str | Path,
    ) -> LiveSafeSampleRuntimeInputs:
        source = load_pipeline_source_snapshot_from_file(
            pipeline_source_path,
            source_reader=_read_pipeline_source_bytes,
            authoring_compiler=self._authoring_compiler,
        )
        return LiveSafeSampleRuntimeInputs(
            pipeline_source=source.payload,
            pipeline_source_path=source.path,
            pipeline_source_sha256=source.sha256,
            pipeline_source_dependencies=source.dependencies,
            binding_set=_read_mapping(binding_set_path),
            connection_registry=_read_mapping(connection_registry_path),
            credential_runtime=_read_mapping(credential_runtime_path),
        )


@dataclass(frozen=True, slots=True)
class LiveSafeSampleRuntimeAssembly:
    plan: SafeSampleExecutionPlan
    data_copier: SafeSampleDataCopier
    temporary_target_executor: TemporaryTargetLifecycleExecutor
    route_attestation_verification: RouteAttestationVerification


def build_live_safe_sample_runtime_assembly(
    *,
    plan: SafeSampleExecutionPlan,
    pipeline_source_path: str | Path,
    binding_set_path: str | Path,
    connection_registry_path: str | Path,
    credential_runtime_path: str | Path,
    route_attestation_path: str | Path,
    route_attestation_bundle_path: str | Path,
    route_certification_bundle_path: str | Path,
    route_attestation_policy_path: str | Path,
    cache_root: str | Path = ".dpone-cache",
    source_root: str | Path | None = None,
    process_name: str | None = None,
    evidence_context: Mapping[str, Any] | None = None,
    route_attestation_signature_verifier: RouteAttestationSignatureVerifier | None = None,
    input_loader: LiveSafeSampleRuntimeInputLoader | None = None,
) -> LiveSafeSampleRuntimeAssembly:
    """Authorize and assemble one live workload without performing external I/O."""

    inputs = (input_loader or LiveSafeSampleRuntimeInputLoader()).load(
        pipeline_source_path=pipeline_source_path,
        binding_set_path=binding_set_path,
        connection_registry_path=connection_registry_path,
        credential_runtime_path=credential_runtime_path,
    )
    verify_pinned_source(plan, inputs, cache_root=cache_root, source_root=source_root)
    verification = verify_route_attestation_files(
        attestation_path=route_attestation_path,
        sigstore_bundle_path=route_attestation_bundle_path,
        certification_bundle_path=route_certification_bundle_path,
        policy_path=route_attestation_policy_path,
        expected=expected_subject_for_safe_sample(plan, inputs.pipeline_source),
        signature_verifier=route_attestation_signature_verifier,
    )
    if not verification.is_verified:
        raise LiveSafeSampleRuntimeAssemblyError(
            verification.code,
            verification.message,
            verification=verification,
        )
    authorized_plan = SafeSampleLiveExecutionAuthorizer().authorize(
        plan,
        pipeline_source=inputs.pipeline_source,
        verified_route_ids=verification.verified_route_ids,
    )
    target_plan = authorized_plan.temporary_target_plan
    if target_plan is None:
        raise LiveSafeSampleRuntimeAssemblyError(
            "DPONE_SAFE_SAMPLE_LIVE_POLICY_INVALID",
            "Live safe-sample execution requires a temporary target plan.",
        )
    if process_name is not None and process_name != target_plan.process:
        raise LiveSafeSampleRuntimeAssemblyError(
            "DPONE_SAFE_SAMPLE_LIVE_POLICY_INVALID",
            "Requested process does not match the pinned temporary target process.",
        )

    verify_pinned_environment_inputs(authorized_plan, inputs)
    _validate_input_contracts(
        authorized_plan,
        inputs,
        credential_runtime_path=Path(credential_runtime_path),
    )
    _validate_bound_route(authorized_plan, inputs)

    resolver = WorkloadScopedCredentialResolver(
        BindingCredentialResolver(
            binding_set=inputs.binding_set,
            connection_registry=inputs.connection_registry,
            vault_kv_reader=build_required_vault_kv_reader(
                credential_runtime=inputs.credential_runtime,
                connection_registry=inputs.connection_registry,
            ),
            evidence_context={
                **dict(evidence_context or {}),
                **_plan_evidence_context(authorized_plan),
                **_credential_runtime_evidence_context(inputs.credential_runtime),
            },
        )
    )
    sql_clients = importlib.import_module("dpone.runtime.safe_sample_sql_clients")
    registry = build_mssql_clickhouse_safe_sample_sql_registry_from_pipeline_source(
        inputs.pipeline_source,
        process_name=target_plan.process,
        credential_resolver=resolver,
        mssql_client_factory=sql_clients.MssqlCredentialsSafeSampleSqlClientFactory(),
        clickhouse_client_factory=sql_clients.ClickHouseCredentialsSafeSampleSqlClientFactory(),
    )
    target_adapter = CredentialResolvingClickHouseTemporaryTargetAdapter(
        credential_resolver=resolver,
        connection_ref=target_plan.connection_ref,
        connection_provider_factory=sql_clients.ClickHouseCredentialsConnectionProviderFactory(),
    )
    return LiveSafeSampleRuntimeAssembly(
        plan=authorized_plan,
        data_copier=RegistryBackedSafeSampleDataCopier(registry),
        temporary_target_executor=TemporaryTargetLifecycleExecutor(adapter=target_adapter),
        route_attestation_verification=verification,
    )


def _validate_input_contracts(
    plan: SafeSampleExecutionPlan,
    inputs: LiveSafeSampleRuntimeInputs,
    *,
    credential_runtime_path: Path,
) -> None:
    expected_environment = _normalized_environment(plan.environment)
    credential_errors = validate_credential_runtime(
        inputs.credential_runtime,
        str(inputs.credential_runtime.get("environment") or expected_environment),
        credential_runtime_path,
    )
    if credential_errors:
        codes = ", ".join(str(error.get("code") or "DPONE_CREDENTIAL_RUNTIME_INVALID") for error in credential_errors)
        raise LiveSafeSampleRuntimeAssemblyError(
            "DPONE_SAFE_SAMPLE_LIVE_COPY_INPUT_INVALID",
            f"credential-runtime validation failed: {codes}",
        )

    validator = GitOpsSchemaValidator()
    for label, payload, kind in (
        ("binding-set", inputs.binding_set, "dpone.binding-set.v1"),
        ("connection-registry", inputs.connection_registry, "dpone.connection-registry.v1"),
        ("credential-runtime", inputs.credential_runtime, "dpone.credential-runtime.v1"),
    ):
        issues = validator.validate(payload, expected_kind=kind)
        if issues:
            summary = ", ".join(f"{issue.code}:{issue.path}" for issue in issues[:5])
            raise LiveSafeSampleRuntimeAssemblyError(
                "DPONE_SAFE_SAMPLE_LIVE_COPY_INPUT_INVALID",
                f"{label} schema validation failed ({summary}).",
            )

    _require_environment(inputs.binding_set, expected_environment, "DPONE_BINDING_SET_ENVIRONMENT_MISMATCH")
    _require_environment(
        inputs.connection_registry,
        expected_environment,
        "DPONE_CONNECTION_REGISTRY_ENVIRONMENT_MISMATCH",
        optional=True,
    )
    _require_environment(
        inputs.credential_runtime,
        expected_environment,
        "DPONE_CREDENTIAL_RUNTIME_ENVIRONMENT_MISMATCH",
    )


def _validate_bound_route(plan: SafeSampleExecutionPlan, inputs: LiveSafeSampleRuntimeInputs) -> None:
    target_plan = plan.temporary_target_plan
    assert target_plan is not None
    process = _selected_process(inputs.pipeline_source, target_plan.process)
    bindings = _mapping(inputs.binding_set.get("bindings"))
    connections = _mapping(inputs.connection_registry.get("connections"))
    for role in ("source", "sink"):
        endpoint = _mapping(process.get(role))
        logical_ref = str(endpoint.get("connection_ref") or "")
        binding = _mapping(bindings.get(logical_ref))
        registry_ref = str(binding.get("connection_ref") or "")
        entry = _mapping(connections.get(registry_ref))
        if not logical_ref or not registry_ref:
            raise LiveSafeSampleRuntimeAssemblyError(
                "DPONE_SAFE_SAMPLE_LIVE_COPY_INPUT_INVALID",
                f"{role} connection_ref is not bound for the selected process.",
            )
        if not entry:
            raise LiveSafeSampleRuntimeAssemblyError(
                "DPONE_SAFE_SAMPLE_LIVE_COPY_INPUT_INVALID",
                f"{role} connection_ref is missing from the connection registry.",
            )
        registry_type = canonical_endpoint_type(str(entry.get("type") or ""))
        endpoint_type = canonical_endpoint_type(str(endpoint.get("type") or ""))
        if registry_type != endpoint_type:
            raise LiveSafeSampleRuntimeAssemblyError(
                "DPONE_SAFE_SAMPLE_LIVE_COPY_INPUT_INVALID",
                f"{role} connection registry type does not match the selected pipeline route.",
            )


def _selected_process(pipeline_source: Mapping[str, Any], process_name: str) -> Mapping[str, Any]:
    processes = pipeline_source.get("processes")
    if isinstance(processes, list):
        for process in processes:
            if isinstance(process, Mapping) and str(process.get("name") or "") == process_name:
                return process
    raise LiveSafeSampleRuntimeAssemblyError(
        "DPONE_SAFE_SAMPLE_LIVE_POLICY_INVALID",
        "Pinned temporary target process is missing from the pipeline source.",
    )


def _read_mapping(path: str | Path) -> dict[str, Any]:
    yaml_module = importlib.import_module("yaml")
    source = Path(path)
    try:
        raw = read_bounded_file(source, max_bytes=_LIVE_INPUT_MAX_BYTES, label=source.name)
        payload = yaml_module.safe_load(raw.decode("utf-8"))
    except RouteAttestationFileError as exc:
        if isinstance(exc.__cause__, FileNotFoundError):
            raise LiveSafeSampleRuntimeAssemblyError(
                "DPONE_SAFE_SAMPLE_LIVE_COPY_INPUT_INVALID",
                f"{source.name} does not exist or is no longer available.",
            ) from exc
        raise LiveSafeSampleRuntimeAssemblyError(
            "DPONE_SAFE_SAMPLE_LIVE_INPUT_PATH_INVALID",
            "Live runtime input could not be read through a stable local path.",
        ) from exc
    except (UnicodeDecodeError, yaml_module.YAMLError) as exc:
        raise LiveSafeSampleRuntimeAssemblyError(
            "DPONE_SAFE_SAMPLE_LIVE_COPY_INPUT_INVALID",
            f"{source.name} must contain UTF-8 YAML or JSON.",
        ) from exc
    if not isinstance(payload, Mapping):
        raise LiveSafeSampleRuntimeAssemblyError(
            "DPONE_SAFE_SAMPLE_LIVE_COPY_INPUT_INVALID",
            f"{Path(path).name} must contain a YAML/JSON object.",
        )
    return dict(payload)


def _read_pipeline_source_bytes(path: Path) -> bytes:
    try:
        return read_bounded_file(path, max_bytes=_LIVE_INPUT_MAX_BYTES, label="pipeline source")
    except RouteAttestationFileError as exc:
        raise LiveSafeSampleRuntimeAssemblyError(
            "DPONE_SAFE_SAMPLE_LIVE_INPUT_PATH_INVALID",
            "Pipeline source could not be read through a stable local path.",
        ) from exc


def _require_environment(
    payload: Mapping[str, Any],
    expected: str,
    code: str,
    *,
    optional: bool = False,
) -> None:
    actual = payload.get("environment")
    if optional and actual in (None, ""):
        return
    if _normalized_environment(str(actual or "")) != expected:
        raise LiveSafeSampleRuntimeAssemblyError(code, "Runtime input environment does not match the plan.")


def _plan_evidence_context(plan: SafeSampleExecutionPlan) -> dict[str, object]:
    context = plan.deployment_context
    if context is None:
        return {}
    return {
        "release_id": context.release_id,
        "deployment_id": context.deployment_id,
        "binding_set_fingerprint": context.binding_set_ref,
        "connection_registry_fingerprint": context.connection_registry_ref,
        "credential_runtime_fingerprint": context.credential_runtime_ref,
        "runtime_image_digest": context.runtime_image_digest,
        "airflow_bundle_ref": context.airflow_bundle_ref,
    }


def _credential_runtime_evidence_context(payload: Mapping[str, Any]) -> dict[str, str]:
    metadata = {"credential_runtime_environment": str(payload.get("environment") or "")}
    vault = _mapping(payload.get("vault"))
    auth = _mapping(vault.get("auth"))
    method = str(auth.get("method") or "")
    if method:
        metadata["credential_runtime_auth_method"] = method
    return {key: value for key, value in metadata.items() if value}


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _normalized_environment(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"dev", "development", "local"}:
        return "development"
    if normalized in {"prod", "production"}:
        return "production"
    return normalized


__all__ = [
    "LiveSafeSampleRuntimeAssembly",
    "LiveSafeSampleRuntimeAssemblyError",
    "LiveSafeSampleRuntimeInputLoader",
    "LiveSafeSampleRuntimeInputs",
    "build_live_safe_sample_runtime_assembly",
]
