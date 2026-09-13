"""Shared verified configuration preflight before runtime resources are created.

Hydration and independent transfer planning use the same defaults, signed
source/database authority selection and runtime state identity policy.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from dpone.config.load_config import LoadConfig
from dpone.dag.load_config_builder import LoadConfigBuilder
from dpone.runtime.bootstrap_config import mapping_or_empty
from dpone.runtime.bootstrap_mssql_authority import (
    apply_connection_database_defaults,
    preflight_target_atomic_database_authority,
)
from dpone.runtime.bootstrap_postgres_source_authority import preflight_postgres_source_authority
from dpone.runtime.credentials.authority import canonical_runtime_endpoint_type
from dpone.runtime.errors import RuntimeConfigurationError
from dpone.runtime.postgres_xmin_execution import PostgresXminExecutionMode, postgres_xmin_execution_policy
from dpone.runtime.source_materialization_location import bind_source_materialization_location
from dpone.runtime.storage_policy import RuntimeStoragePolicy


@dataclass(frozen=True)
class RuntimeInputPreflight:
    """Selected policies/verifiers; no connector construction or mutation."""

    storage_policy: RuntimeStoragePolicy
    source_verifier: Any
    database_verifier: Any


def preflight_runtime_inputs(
    *,
    config: Mapping[str, Any],
    load_config: Any,
    connections: Any,
    context: Any,
    postgres_verifier_factory: Callable[..., Any] | None = None,
    mssql_verifier_factory: Callable[..., Any] | None = None,
) -> RuntimeInputPreflight:
    """Apply the shared ordered preflight to an existing load configuration."""
    state = mapping_or_empty(config.get("state"))
    apply_connection_database_defaults(load_config=load_config, connections=connections)
    bind_source_materialization_location(load_config=load_config, connections=connections)
    storage = RuntimeStoragePolicy.from_sources(
        runtime=mapping_or_empty(config.get("runtime")), source_options=load_config.options
    )
    source = preflight_postgres_source_authority(
        connections=connections, state_config=state, load_config=load_config, verifier_factory=postgres_verifier_factory
    )
    database = preflight_target_atomic_database_authority(
        connections=connections, state_config=state, load_config=load_config, verifier_factory=mssql_verifier_factory
    )
    apply_runtime_state_identity(config=config, load_config=load_config, context=context)
    return RuntimeInputPreflight(storage, source, database)


def prepare_verified_transfer_config(manifest: Mapping[str, Any], *, connections: Any, context: Any) -> LoadConfig:
    """Reconstruct private config solely from verified manifest and resolved bindings.

    No prepared worker boundary or mutable worker option dictionary is copied.
    The caller must supply the already verified parent context and connections.
    """
    if not connections.strict:
        raise RuntimeConfigurationError("transfer_preplan_verified_connections")
    config = deepcopy(dict(manifest))
    for name in ("source", "sink", "state"):
        if name in config:
            config[name] = canonical_endpoint_config(mapping_or_empty(config[name]))
    load_config = LoadConfigBuilder().build(config)
    preflight_runtime_inputs(config=config, load_config=load_config, connections=connections, context=context)
    return load_config


def canonical_endpoint_config(config: Mapping[str, Any]) -> Mapping[str, Any]:
    """Detach and canonicalize one endpoint type without mutating the manifest."""

    if not config:
        return config
    canonical = dict(config)
    if canonical.get("type") not in (None, ""):
        canonical["type"] = canonical_runtime_endpoint_type(canonical["type"])
    return canonical


def apply_runtime_state_identity(*, config: Mapping[str, Any], load_config: Any, context: Any) -> None:
    """Attach verified environment/process dimensions for collision-safe state."""

    options = getattr(load_config, "options", {}) or {}
    policy = options.get("reconciliation")
    xmin_execution = postgres_xmin_execution_policy(options)
    reconciliation_enabled = isinstance(policy, Mapping) and bool(policy.get("enabled", True))
    if not reconciliation_enabled and xmin_execution.mode is PostgresXminExecutionMode.AUTO:
        return
    process = str(config.get("name") or "").strip()
    environment = str(getattr(context, "environment", "") or "").strip()
    if not environment or not process:
        raise RuntimeConfigurationError(
            "PostgreSQL XMin key_snapshot/handoff requires verified runtime environment and process identity"
        )
    load_config.options["state_identity"] = {
        "environment": environment,
        "process": process,
    }
