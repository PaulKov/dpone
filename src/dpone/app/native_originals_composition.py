"""Production composition for offline native preflight and pinned ACTIVE readback."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from dpone.adapters.native_delivery_originals import NativeOriginalVerifier
from dpone.app.dbt_promotion_composition import RuntimeDbtProjectBundleOperations
from dpone.app.dbt_workspace_activation_composition import build_dbt_workspace_activation_coordinator
from dpone.app.dbt_workspace_runtime_resolver import DbtWorkspaceRuntimeResolverFactory
from dpone.app.deployment_cache_workspace_activation_inputs import DeploymentCacheWorkspaceActivationInputs
from dpone.app.native_originals_activation import build_native_originals_activation_callback
from dpone.contracts.airflow_run_identity import AirflowDeploymentIdentity
from dpone.contracts.native_delivery import NativeOriginalsRefV1
from dpone.manifest.confined_files import read_confined_file
from dpone.services.dbt_release_source_reader import DbtReleaseSourceReader


def build_native_original_verifier(
    *,
    cache_root: Path,
    originals: NativeOriginalsRefV1,
    invocation_identity: AirflowDeploymentIdentity,
    environment: str,
    authority_connection_ref: str,
    control_schema: str,
    connect_timeout_seconds: int,
    statement_timeout_seconds: int,
    max_policy_bytes: int,
    max_bundle_bytes: int,
    package_environment: Mapping[str, str],
) -> NativeOriginalVerifier:
    """Create a context-managed verifier without acquiring credentials or SQL.

    The application supplies its configured cache and pinned scheduler identity;
    neither is inferred from an arbitrary projection ancestor or current pointer.
    All source, resolver and coordinator capabilities share the same input owner.
    This factory does not construct a generation writer, qualify an executable
    runtime, activate a workspace or authorize a public release.
    """
    if not isinstance(cache_root, Path) or not cache_root.is_absolute() or ".." in cache_root.parts:
        raise ValueError("native cache root must be explicitly configured and absolute")
    if type(originals) is not NativeOriginalsRefV1 or type(invocation_identity) is not AirflowDeploymentIdentity:
        raise ValueError("native composition requires exact originals and pinned invocation contracts")
    originals.__post_init__()
    identity = AirflowDeploymentIdentity.from_mapping(invocation_identity.to_dict())
    bundles = RuntimeDbtProjectBundleOperations(package_environment=package_environment)
    inputs = DeploymentCacheWorkspaceActivationInputs(
        cache_root=cache_root,
        source_reader=DbtReleaseSourceReader(bundle_operations=bundles, read_file=read_confined_file),
        resolver_factory=DbtWorkspaceRuntimeResolverFactory(),
    )
    coordinator = build_dbt_workspace_activation_coordinator(
        inputs=inputs,
        authority_connection_ref=authority_connection_ref,
        control_schema=control_schema,
    )
    active = build_native_originals_activation_callback(
        inputs=inputs,
        coordinator=coordinator,
        invocation_identity=identity,
        projection_root=originals.projection_root,
        environment=environment,
        authority_connection_ref=authority_connection_ref,
        control_schema=control_schema,
        connect_timeout_seconds=connect_timeout_seconds,
        statement_timeout_seconds=statement_timeout_seconds,
    )
    return NativeOriginalVerifier(
        inputs=inputs,
        invocation_identity=identity,
        require_active=active,
        bundles=bundles,
        read_file=read_confined_file,
        release_root=cache_root / "releases" / identity.release_id.replace(":", "-"),
        max_policy_bytes=max_policy_bytes,
        max_bundle_bytes=max_bundle_bytes,
    )
