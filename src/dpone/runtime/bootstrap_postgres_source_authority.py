"""Strict PostgreSQL source-authority composition for runtime hydration."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from dpone.runtime.errors import RuntimeConfigurationError


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


__all__ = [
    "bind_postgres_source_authority",
    "preflight_postgres_source_authority",
]
