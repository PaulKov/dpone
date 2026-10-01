"""Verified operator scope and owned target admission, without source access."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import asdict
from typing import TYPE_CHECKING, Any
from uuid import UUID

from dpone.contracts.clickhouse_cluster_publication import digest_payload
from dpone.contracts.publication_authority_binding import PublicationAuthorityBinding
from dpone.runtime.clickhouse_file_stage_contract import EndpointBinding, identity_sql, require_endpoint_row
from dpone.runtime.credentials.resolved_connector_factory import ResolvedConnectorFactory
from dpone.runtime.credentials.runtime_context import RuntimeConnectionContextLoader
from dpone.runtime.publication_authority_composition import require_publication_endpoint_pin

if TYPE_CHECKING:
    from dpone.contracts.runtime_connection import ResolvedBindingConnection
    from dpone.runtime.credentials.runtime_context import RuntimeConnectionContext


class OperatorScopeBlocked(ValueError):
    """Application-owned fixed codes only, never connector exception messages."""


def verified_context(environ: Mapping[str, str] | None, environment: str) -> RuntimeConnectionContext:
    context = RuntimeConnectionContextLoader().load(environ)
    if context is None:
        raise OperatorScopeBlocked("verified_runtime_context_required")
    if context.environment != environment:
        raise OperatorScopeBlocked("runtime_environment_differs")
    return context


def context_subject(context: RuntimeConnectionContext) -> str:
    """Convert only the loader's canonical prefixed digest to plan representation."""
    subject = context.authority_subject_sha256
    if not isinstance(subject, str) or re.fullmatch(r"sha256:[0-9a-f]{64}", subject) is None:
        raise OperatorScopeBlocked("verified_context_subject_required")
    return subject[7:]


def resolve_authority(
    context: RuntimeConnectionContext, connection_ref: str
) -> tuple[ResolvedBindingConnection, PublicationAuthorityBinding, str]:
    resolved = context.resolver.resolve(connection_ref)
    descriptor = resolved.descriptor
    if descriptor is None or descriptor.connection_type != "mssql":
        raise OperatorScopeBlocked("mssql_registry_binding_required")
    properties = descriptor.properties
    policy = properties.get("publication_authority")
    if not isinstance(policy, Mapping):
        raise OperatorScopeBlocked("deployment_endpoint_pin_required")
    binding = PublicationAuthorityBinding.from_mapping(
        dict(
            backend="mssql",
            connection_ref=connection_ref,
            database=properties.get("database"),
            schema=properties.get("schema"),
            service_id=policy.get("service_id"),
            environment=context.environment,
        )
    )
    pin = require_publication_endpoint_pin(resolved, binding, context.environment)
    return resolved, binding, pin


@contextmanager
def owned_recovery_target(
    resolved: ResolvedBindingConnection,
    database: str,
    factory: Callable[[ResolvedBindingConnection], Any] | None,
) -> Iterator[tuple[Any, str]]:
    """One native handle, shared for observations and DDL, closed before ACK.

    A physical endpoint digest binds confirmation, not writer exclusion. Closing
    failures propagate as unknown; never acknowledge completion before disposal.
    """
    descriptor = resolved.descriptor
    if (
        descriptor is None
        or descriptor.connection_type != "clickhouse"
        or descriptor.properties.get("database") != database
        or resolved.credentials.database != database
    ):
        raise OperatorScopeBlocked("clickhouse_registry_database_required")
    connector = (factory or ResolvedConnectorFactory.create)(resolved)
    try:
        if (
            connector.driver != "native"
            or connector.database != database
            or connector.host != resolved.credentials.host
        ):
            raise OperatorScopeBlocked("native_clickhouse_target_required")
        if (
            not isinstance(connector.host, str)
            or not connector.host
            or type(connector.port) is not int
            or not 0 < connector.port < 65536
            or type(connector.secure) is not bool
        ):
            raise OperatorScopeBlocked("native_clickhouse_endpoint_required")
        server, database_uuid = require_endpoint_row(
            connector.get_records(identity_sql(database, formatted=False)), database
        )
        endpoint = EndpointBinding(
            str(UUID(server)), database, connector.host, connector.port, connector.secure, str(UUID(database_uuid))
        )
        digest = digest_payload(
            {"contract": "dpone.native-recovery-endpoint.v1", "transport": "native", **asdict(endpoint)}
        )
        yield connector, digest
    finally:
        try:
            connector.close()
        except Exception:
            raise RuntimeError("recovery target close outcome unknown") from None
