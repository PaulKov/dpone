"""Lazy composition boundary for the optional SqlClient companion."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from importlib import import_module
from typing import Any

from packaging.version import InvalidVersion, Version

from dpone.ports.mssql_native import (
    NativeChunkPlan,
    NativeStageWriteObservation,
    NativeStageWriteRequest,
    NativeVerificationIdentityV2,
    build_sqlclient_target_local_verification_identity,
)
from dpone.runtime.sinks.mssql_native_composition import compose_native_stage_context
from dpone.runtime.sinks.mssql_sqlclient_stage_writer import MssqlSqlClientStageWriter
from dpone.version import installed_version


@dataclass(frozen=True, slots=True)
class ResolvedSqlClientBackend:
    """One verified companion, durable identity, and injected writer."""

    companion: Any
    identity: NativeVerificationIdentityV2
    writer: MssqlSqlClientStageWriter


@dataclass(frozen=True, slots=True)
class SqlClientStageComposition:
    """Production stage context paired with its exact companion authority."""

    stage_context: Any
    backend: ResolvedSqlClientBackend


def _require_compatible_minor(companion_version: str) -> None:
    try:
        core = Version(installed_version())
        companion = Version(companion_version)
    except (InvalidVersion, TypeError) as error:
        raise RuntimeError("mssql_sqlclient.incompatible_package_version") from error
    if core.release[:2] != companion.release[:2]:
        raise RuntimeError("mssql_sqlclient.incompatible_package_version")


def resolve_sqlclient_backend(
    plan: NativeChunkPlan,
    *,
    timeout_seconds: int,
    credentials_provider: Callable[[Any], Any],
    process_started: Callable[[int, NativeStageWriteRequest], None] | None = None,
    write_observer: Callable[[NativeStageWriteObservation], None] | None = None,
    diagnostic_sink: Callable[[str], None] | None = None,
    layout_version: int = 1,
) -> ResolvedSqlClientBackend:
    """Discover the optional package only after explicit backend selection."""
    try:
        provider = import_module("dpone_mssql_sqlclient")
    except ImportError as error:
        raise RuntimeError("mssql_sqlclient.optional_package_required") from error
    companion = provider.locate()
    _require_compatible_minor(companion.package_version)
    identity = build_sqlclient_target_local_verification_identity(
        plan,
        timeout_seconds=timeout_seconds,
        companion=companion,
        layout_version=layout_version,
    )
    writer = MssqlSqlClientStageWriter(
        companion.command,
        credentials_provider=credentials_provider,
        writer_identity_sha256=companion.writer_identity_sha256,
        runtime_identity_sha256=companion.runtime_identity_sha256,
        process_started=process_started,
        observation_sink=write_observer,
        diagnostic_sink=diagnostic_sink,
    )
    return ResolvedSqlClientBackend(companion, identity, writer)


def compose_sqlclient_stage_context(
    plan: NativeChunkPlan,
    *,
    timeout_seconds: int,
    credentials_provider: Callable[[Any], Any],
    process_started: Callable[[int, NativeStageWriteRequest], None] | None = None,
    write_observer: Callable[[NativeStageWriteObservation], None] | None = None,
    diagnostic_sink: Callable[[str], None] | None = None,
    persisted_hash_layout: bool = False,
    **context_options: Any,
) -> SqlClientStageComposition:
    """Resolve SqlClient and inject its exact writer identity into native staging."""
    reserved = {"plan", "verification_identity", "native_stage_writer", "target_local_timeout_seconds"}
    if reserved & context_options.keys():
        raise ValueError("mssql_sqlclient.reserved_composition_option")
    if type(persisted_hash_layout) is not bool:
        raise ValueError("mssql_native.persisted_hash_layout_selection")
    backend = resolve_sqlclient_backend(
        plan,
        timeout_seconds=timeout_seconds,
        credentials_provider=credentials_provider,
        process_started=process_started,
        write_observer=write_observer,
        diagnostic_sink=diagnostic_sink,
        layout_version=2 if persisted_hash_layout else 1,
    )
    stage_context = compose_native_stage_context(
        **context_options,
        plan=plan,
        verification_identity=backend.identity,
        native_stage_writer=backend.writer,
        target_local_timeout_seconds=timeout_seconds,
        persisted_hash_layout=persisted_hash_layout,
    )
    return SqlClientStageComposition(stage_context, backend)


__all__ = [
    "ResolvedSqlClientBackend",
    "SqlClientStageComposition",
    "compose_sqlclient_stage_context",
    "resolve_sqlclient_backend",
]
