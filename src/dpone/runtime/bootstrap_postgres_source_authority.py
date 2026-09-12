"""PostgreSQL source-authority and optional MSSQL route composition for hydration."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any

from dpone.runtime.errors import RuntimeConfigurationError

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig


def bind_postgres_source_authority(
    *,
    source_obj: Any,
    verifier: Any | None,
) -> None:
    """Bind the descriptor-selected verifier before any source method runs."""

    if verifier is None:
        return
    binder = getattr(source_obj, "bind_postgres_source_authority", None)
    if not callable(binder):
        raise RuntimeConfigurationError("Strict PostgreSQL→MSSQL governance requires a source-authority-aware source")
    binder(verifier)


def preflight_postgres_source_authority(
    *,
    connections: Any,
    state_config: Mapping[str, Any],
    load_config: Any,
    verifier_factory: Callable[..., Any] | None,
) -> Any | None:
    """Select signed PostgreSQL authority without touching the source."""

    if not bool(getattr(connections, "strict", False)):
        return None
    source_connection = getattr(connections, "source", None)
    target_connection = getattr(connections, "sink", None)
    source_descriptor = getattr(source_connection, "descriptor", None)
    target_descriptor = getattr(target_connection, "descriptor", None)
    source_type = str(getattr(source_descriptor, "connection_type", "") or "").strip().lower()
    target_type = str(getattr(target_descriptor, "connection_type", "") or "").strip().lower()
    state_type = str(state_config.get("type") or "").strip().lower()
    atomicity = str(state_config.get("atomicity") or "").strip().lower()
    if (
        source_type not in {"postgres", "postgresql"}
        or target_type != "mssql"
        or state_type != "mssql"
        or atomicity != "target_atomic"
    ):
        return None
    if source_connection is None:
        raise RuntimeConfigurationError("Strict PostgreSQL→MSSQL governance requires signed source authority")
    if verifier_factory is None:
        from dpone.runtime.sources.postgres_source_authority import (
            PostgresSourceAuthorityVerifier,
        )

        verifier_factory = PostgresSourceAuthorityVerifier.from_connection
    verifier = verifier_factory(source_connection)
    identity = verifier.preflight(load_config)
    from dpone.runtime.sources.postgres_source_authority import (
        POSTGRES_SOURCE_AUTHORITY_SHA256_OPTION,
    )

    options = getattr(load_config, "options", None)
    if not isinstance(options, dict):
        raise RuntimeConfigurationError("Strict PostgreSQL→MSSQL governance requires mutable load options")
    authored = options.get(POSTGRES_SOURCE_AUTHORITY_SHA256_OPTION)
    if authored not in (None, identity.authority_sha256):
        raise RuntimeConfigurationError("PostgreSQL source-authority digest is runtime-owned")
    options[POSTGRES_SOURCE_AUTHORITY_SHA256_OPTION] = identity.authority_sha256
    return verifier


_SOURCE_SCHEMA_RUNTIME_REQUIRED = "DPONE_POSTGRES_MSSQL_SOURCE_SCHEMA_AUTHORITY_RUNTIME_REQUIRED"


def _source_schema_runtime_error() -> RuntimeConfigurationError:
    error = RuntimeConfigurationError(_SOURCE_SCHEMA_RUNTIME_REQUIRED)
    setattr(error, "code", _SOURCE_SCHEMA_RUNTIME_REQUIRED)
    return error


def _require_source_schema_runtime(runtime: Any, *, verifier: Any | None = None) -> Any:
    from dpone.runtime.postgres_mssql_source_schema_runtime import (
        PostgresMssqlSourceSchemaRuntimeV1,
    )

    if type(runtime) is not PostgresMssqlSourceSchemaRuntimeV1:
        raise _source_schema_runtime_error()
    invalid = False
    try:
        runtime.require_exact_bundle()
    except (asyncio.CancelledError, KeyboardInterrupt, SystemExit, GeneratorExit) as cancellation:
        cancellation.__cause__ = cancellation.__context__ = None
        raise
    except Exception:
        invalid = True
    if invalid:
        raise _source_schema_runtime_error()
    if verifier is not None and runtime.verifier is not verifier:
        raise _source_schema_runtime_error()
    return runtime


def build_postgres_mssql_source_schema_runtime(
    activation: Any | None,
    factory: Any | None,
    connections: Any,
    load_config: Any,
    verifier: Any | None,
) -> Any | None:
    """Build the exact schema authority bundle before endpoint construction."""

    if activation is None:
        return None
    if factory is None or verifier is None:
        raise _source_schema_runtime_error()
    failed = False
    try:
        runtime = factory.build(
            activation=activation,
            connections=connections,
            load_config=load_config,
            verifier=verifier,
        )
    except (asyncio.CancelledError, KeyboardInterrupt, SystemExit, GeneratorExit) as cancellation:
        cancellation.__cause__ = cancellation.__context__ = None
        raise
    except Exception:
        failed = True
        runtime = None
    if failed:
        raise _source_schema_runtime_error()
    return _require_source_schema_runtime(runtime, verifier=verifier)


def bind_postgres_mssql_source_schema_runtime(*, source_obj: Any, runtime: Any | None) -> None:
    """Bind the prevalidated schema runtime without exposing a lower-level seam."""

    if runtime is None:
        return
    selected = _require_source_schema_runtime(runtime)
    binder = getattr(source_obj, "bind_postgres_mssql_source_schema_runtime", None)
    if not callable(binder):
        raise _source_schema_runtime_error()
    binder(selected)


__all__ = [
    "bind_postgres_source_authority",
    "build_postgres_mssql_source_schema_runtime",
    "bind_postgres_mssql_source_schema_runtime",
    "preflight_postgres_source_authority",
]


def build_postgres_mssql_correctness_runtime(
    *,
    activation: Any | None,
    factory: Any | None,
    load_config: LoadConfig,
    connections: Any,
    sink_obj: Any,
    state_bindings: Any,
) -> Any | None:
    """Build the admitted optional route runtime after target-state binding."""
    if activation is None:
        return None
    if factory is None:  # protected before any endpoint construction
        raise RuntimeConfigurationError("DPONE_POSTGRES_MSSQL_PROFILE_WEAKER_THAN_REQUIRED")
    return factory.build(
        activation=activation,
        load_config=load_config,
        connections=connections,
        sink=sink_obj,
        state_bindings=state_bindings,
    )
