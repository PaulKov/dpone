"""Compose the production dispatcher for one verified pack-exec command.

The CLI stays a facade: admission is read from the authenticated command
environment, then this module either returns ``None`` for legacy workloads or
always returns a ``CompositionPackExecutionDispatcher``. Authenticated v3
therefore cannot degrade to generic ``Popen`` or native-v2.

When activation identity, workspace authority, control, target and
``read_active`` can be resolved from the verified command plus already-projected
identity env, this cell wraps the real ``CompositionDbtExecutionRoot``.
Materialization openers and the undispatched-closure observer are occurrence
authority owned by the activation factory. Missing parent context or a missing
cell factory fail-closes instead of inventing a second SQL opener.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any

from dpone.adapters.composition_mssql_store import MssqlCompositionActivationStore
from dpone.adapters.dbt_runtime import build_hvac_kubernetes_vault_kv_v2_reader
from dpone.app.composition_authority_connections import CompositionAuthorityConnections
from dpone.app.composition_dbt_execution import CompositionDbtExecutionRoot
from dpone.app.composition_dbt_execution_factory import (
    CompositionDbtControlAuthority,
    build_composition_dbt_execution_dependencies,
)
from dpone.app.composition_execution_cells import (
    SQLSERVER_DBT_V1,
    build_installed_execution_capabilities,
)
from dpone.app.composition_materialization_seams import bind_composition_materialization_seams
from dpone.app.composition_pack_execution_dispatcher import (
    CompositionPackExecutionDispatcher,
    compose_pack_execution_root,
)
from dpone.contracts.composition_activation import CompositionAdmissionError, CompositionOccurrenceContext
from dpone.contracts.composition_execution_authority import supervisor_from_transport
from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.contracts.dbt_runtime import (
    AIRFLOW_DEPLOYMENT_IDENTITY_ENV,
    DBT_WORKSPACE_AUTHORITY_CONNECTION_REF_ENV,
    DbtExecutionPack,
    parse_airflow_deployment_identity_json,
    require_workspace_authority_connection_ref,
)
from dpone.contracts.runtime_connection import RuntimeConnectionAuthorityError
from dpone.ports.composition_dbt import CompositionNativeDbtExecutor
from dpone.runtime.composition_native_dbt_dispatch import (
    NATIVE_WORKER_UNAVAILABLE,
    ORDINARY_WORKER_UNAVAILABLE,
)
from dpone.runtime.composition_verified_dispatch import (
    COMPOSITION_SUPERVISOR_B64_ENV,
    CompositionDispatchRejection,
    CompositionDispatchRequest,
    CompositionVerifiedDispatcher,
    composition_dispatch_required,
)
from dpone.runtime.credentials.resolved_connector_factory import ResolvedConnectorFactory
from dpone.runtime.credentials.runtime_context import RuntimeConnectionContextLoader
from dpone.runtime.dbt_execution_bootstrap import MAX_DBT_EXECUTION_PACK_BYTES
from dpone.runtime.dbt_execution_pack_reader import execution_pack_payload
from dpone.runtime.verified_pack_launcher import VerifiedPackCommand

_NATIVE_DBT_PREFIX = ("dpone", "dbt", "execute-pack")
_PARENT_ERRORS = (
    CompositionAdmissionError,
    CompositionDispatchRejection,
    DbtPublishingError,
    OSError,
    RuntimeConnectionAuthorityError,
    TypeError,
    ValueError,
)


class _UnavailableCompositionDispatcher:
    """Fail-closed worker used when supervisor authority cannot be parsed."""

    def run(self, request: CompositionDispatchRequest) -> int:
        if request.kind != "native_dbt":
            raise CompositionDispatchRejection(ORDINARY_WORKER_UNAVAILABLE)
        raise CompositionDispatchRejection(NATIVE_WORKER_UNAVAILABLE)


def compose_verified_pack_dispatcher(
    command: VerifiedPackCommand,
) -> CompositionVerifiedDispatcher | None:
    """Return the production dispatcher, or ``None`` when admission is legacy."""

    try:
        required = composition_dispatch_required(command.env)
    except CompositionDispatchRejection:
        return _UnavailableCompositionDispatcher()
    if not required:
        return None
    return _admitted_dispatcher(command)


def _admitted_dispatcher(command: VerifiedPackCommand) -> CompositionVerifiedDispatcher:
    try:
        supervisor = supervisor_from_transport(command.env.get(COMPOSITION_SUPERVISOR_B64_ENV))
    except ValueError:
        return _UnavailableCompositionDispatcher()
    capabilities = build_installed_execution_capabilities()
    return CompositionPackExecutionDispatcher(
        supervisor=supervisor,
        capabilities=capabilities,
        native_executor=_native_executor(command, supervisor, capabilities),
        ordinary_root=lambda request, cell, manifest: _ordinary_executor(
            command, request, cell, manifest, capabilities
        ),
    )


def _native_executor(
    command: VerifiedPackCommand,
    supervisor: Any,
    capabilities: Any,
) -> CompositionNativeDbtExecutor | None:
    """Compose the supervised root, or omit it so dispatch fail-closes."""

    try:
        parent = _parent_context(command)
        if parent.get("context") is None:
            return None
        factory = capabilities.factory(SQLSERVER_DBT_V1)
        seams = _materialization_seams(parent)
        if seams is None or not callable(factory):
            return None
        return _compose_supervised_root(command, supervisor, seams, parent, factory)
    except _PARENT_ERRORS:
        return None


def _ordinary_executor(
    command: VerifiedPackCommand,
    request: CompositionDispatchRequest,
    cell: str,
    manifest: Mapping[str, Any],
    capabilities: Any,
) -> Any | None:
    """Compose an ordinary root when parent context and the sealed plan exist."""

    try:
        parent = _parent_context(command, manifest=manifest)
        if parent.get("context") is None:
            return None
        return compose_pack_execution_root(
            request=request,
            cell=cell,
            manifest=manifest,
            capabilities=capabilities,
            parent=parent,
        )
    except _PARENT_ERRORS:
        return None


def _compose_supervised_root(
    command: VerifiedPackCommand,
    supervisor: Any,
    seams: Mapping[str, Any],
    parent: Mapping[str, Any],
    factory: Any,
) -> CompositionDbtExecutionRoot:
    dependencies = build_composition_dbt_execution_dependencies(
        supervisor=supervisor,
        control=parent["control"],
        read_active=parent["read_active"],
        target=parent["target"],
        open_materialization_target=seams["open_target"],
        require_materialization_target=seams["require_target"],
        observe_undispatched_closure=seams["observe_undispatched_closure"],
    )
    root = factory(dependencies=dependencies)
    if type(root) is not CompositionDbtExecutionRoot:
        raise CompositionAdmissionError("execution_capability")
    del command
    return root


def _parent_context(
    command: VerifiedPackCommand,
    *,
    manifest: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    environment = _merged_environment(command)
    identity = parse_airflow_deployment_identity_json(
        _preferred(command.env, environment, AIRFLOW_DEPLOYMENT_IDENTITY_ENV)
    )
    authority_ref = require_workspace_authority_connection_ref(
        _preferred(command.env, environment, DBT_WORKSPACE_AUTHORITY_CONNECTION_REF_ENV)
    )
    context = RuntimeConnectionContextLoader(
        vault_reader_factory=build_hvac_kubernetes_vault_kv_v2_reader,
    ).load(environment)
    if context is None:
        raise CompositionAdmissionError("control_connection")
    pack_connection_ref = _pack_connection_ref(command, manifest)
    if authority_ref == pack_connection_ref:
        raise CompositionAdmissionError("control_connection")
    control = _control_authority(context.resolver.resolve(authority_ref))
    target = context.resolver.resolve(pack_connection_ref)
    store = MssqlCompositionActivationStore(
        control.connection_factory,
        expected_service_id=control.expected_service_id,
        control_schema=control.control_schema,
    )
    activation_id = identity.activation_id
    connections = CompositionAuthorityConnections(
        inputs=_PackResolverInputs(context.resolver),
        authority_connection_ref=authority_ref,
        control_schema=control.control_schema,
    )
    return {
        "control": control,
        "target": target,
        "resolver": context.resolver,
        "read_active": lambda: _read_active(store, activation_id),
        "manifest": manifest,
        "require_target_service": connections.require_mssql_target_service,
        "context": _occurrence_context(identity, context),
    }


def _control_authority(resolved: Any) -> CompositionDbtControlAuthority:
    descriptor = resolved.descriptor
    if descriptor is None or descriptor.connection_type != "mssql":
        raise CompositionAdmissionError("control_connection")
    service_id = descriptor.properties.get("composition_service_id")
    if not isinstance(service_id, str):
        raise CompositionAdmissionError("control_service_id")
    database = resolved.credentials.database
    if not isinstance(database, str) or not database:
        raise CompositionAdmissionError("control_connection")

    def connection_factory() -> Any:
        return ResolvedConnectorFactory.create(resolved, autocommit=False).connection

    return CompositionDbtControlAuthority(
        connection_factory=connection_factory,
        expected_service_id=service_id,
        control_database=database,
    )


def _read_active(store: MssqlCompositionActivationStore, activation_id: str) -> Any:
    occurrence = store.read(activation_id)
    if occurrence is None:
        raise CompositionAdmissionError("occurrence_state")
    return occurrence.require_state("ACTIVE")


def _pack_connection_ref(command: VerifiedPackCommand, manifest: Mapping[str, Any] | None) -> str:
    if manifest is not None:
        sink = manifest.get("sink")
        if isinstance(sink, Mapping) and isinstance(sink.get("connection_ref"), str):
            return str(sink["connection_ref"])
        raise CompositionAdmissionError("execution_capability")
    if command.argv[:3] != _NATIVE_DBT_PREFIX or len(command.argv) < 4:
        raise CompositionAdmissionError("execution_capability")
    pack = DbtExecutionPack.from_mapping(
        execution_pack_payload(
            command.working_directory,
            command.argv[3],
            max_bytes=MAX_DBT_EXECUTION_PACK_BYTES,
        )
    )
    return pack.profile.connection_ref


def _materialization_seams(parent: Mapping[str, Any]) -> Mapping[str, Any] | None:
    """Return production openers from the activation factory's seam builders."""

    try:
        return bind_composition_materialization_seams(
            require_target_service=parent.get("require_target_service"),
            target=parent["target"],
            expected_service_id=parent["control"].expected_service_id,
            context=parent.get("context"),
        )
    except _PARENT_ERRORS:
        return None


class _PackResolverInputs:
    def __init__(self, resolver: Any) -> None:
        self._resolver = resolver

    def resolve_connection(self, context: Any, connection_ref: str) -> Any:
        del context
        return self._resolver.resolve(connection_ref)


def _occurrence_context(identity: Any, runtime: Any) -> Any | None:
    digest = getattr(runtime, "authority_subject_sha256", None)
    environment = getattr(runtime, "environment", None)
    if not isinstance(digest, str) or not isinstance(environment, str):
        return None
    try:
        return CompositionOccurrenceContext(
            identity.activation_id,
            environment,
            identity.release_id,
            identity.deployment_id,
            None,
            digest,
        )
    except CompositionAdmissionError:
        return None


def _merged_environment(command: VerifiedPackCommand) -> dict[str, str]:
    merged = dict(os.environ)
    merged.update(command.env)
    return merged


def _preferred(command_env: Mapping[str, str], merged: Mapping[str, str], key: str) -> str:
    if key in command_env:
        return str(command_env[key])
    return str(merged.get(key) or "")


__all__ = ["compose_verified_pack_dispatcher"]
