"""Read every bootstrap authority's enrollment through control-only pinned SQL.

This prerequisite grants no attempt or ACTIVE authority. The startup owner must
still verify the real host/process/listener and retained bootstrap pathname before
opening admission. All SQL handles close before an enrollment is returned.
"""

from __future__ import annotations

import math
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.adapters.composition_clickhouse_enrollment import clickhouse_domain_for_write, clickhouse_physical_domain
from dpone.adapters.composition_clickhouse_supervisor_enrollment import (
    ClickHouseSupervisorEnrollment,
    read_service_enrollment,
)
from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.app.composition_authority_connections import CompositionAuthorityConnections
from dpone.app.composition_dispatcher_context import StagedDispatcherContext, StagedDispatcherContextLoader
from dpone.app.composition_dispatcher_enrollment_validation import require_dispatcher_enrollment
from dpone.app.composition_dispatcher_service_config import (
    DispatcherAuthorityConfig,
    DispatcherServiceConfig,
    decode_dispatcher_service_config,
)
from dpone.app.composition_mssql_execution_deadline import BudgetedMssqlConnectorFactory
from dpone.contracts.composition_activation import CompositionOccurrenceContext
from dpone.contracts.composition_dispatcher_binding import require_dispatcher_connection_ref
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.composition_physical import CompositionPhysicalDomain
from dpone.contracts.runtime_connection import ResolvedBindingConnection
from dpone.manifest.bounded_yaml import load_bounded_yaml


def _require(condition: bool) -> None:
    if not condition:
        raise CompositionAdmissionError("dispatcher_bootstrap_sql_unverified")


def _time(deadline: float) -> None:
    _require(type(deadline) in (int, float) and math.isfinite(deadline) and time.monotonic() < deadline)


@dataclass(frozen=True)
class _ControlInputs:
    context: CompositionOccurrenceContext
    reference: str
    connection: ResolvedBindingConnection

    def resolve_connection(
        self, context: CompositionOccurrenceContext, connection_ref: str
    ) -> ResolvedBindingConnection:
        _require(context == self.context and connection_ref == self.reference)
        return self.connection


def _registry(runtime: Any, alias: str) -> str:
    require_dispatcher_connection_ref(alias)
    return require_dispatcher_connection_ref(runtime.binding_set["bindings"][alias]["connection_ref"])


def _scope(context: StagedDispatcherContext, control_ref: str) -> CompositionPhysicalDomain:
    """Bind logical writes to signed physical pins, without constructing credentials."""
    runtime, plan = context.runtime, context.plan
    control_registry = _registry(runtime, control_ref)
    business = {write.connection_ref for write in plan.writes}
    for _, original in plan.sources.transfer_manifests:
        manifest = load_bounded_yaml(original)
        business.update(manifest[role]["connection_ref"] for role in ("source", "sink"))
    _require(control_ref not in business and all(_registry(runtime, alias) != control_registry for alias in business))
    target = runtime.connection_registry["connections"][_registry(runtime, context.target_binding_ref)]
    _require(target["type"] == "clickhouse" and isinstance(target["connection"], Mapping))
    writes = [write for write in plan.writes if write.connection_ref == context.target_binding_ref]
    _require(bool(writes))
    domains = [clickhouse_domain_for_write(target["connection"], write) for write in writes]
    _require(all(domain == domains[0] for domain in domains))
    return domains[0]


def _read(
    config: DispatcherServiceConfig,
    context: StagedDispatcherContext,
    authority: DispatcherAuthorityConfig,
    domain: CompositionPhysicalDomain,
    deadline: float,
) -> ClickHouseSupervisorEnrollment:
    _time(deadline)
    resolved = context.runtime.resolver.resolve(authority.control_connection_ref)
    _require(resolved.descriptor is not None and resolved.descriptor.connection_type == "mssql")
    assert resolved.descriptor is not None
    _require(resolved.descriptor.properties.get("composition_service_id") == authority.expected_control_service_id)
    connections = CompositionAuthorityConnections(
        inputs=_ControlInputs(context.occurrence, authority.control_connection_ref, resolved),
        authority_connection_ref=authority.control_connection_ref,
        control_schema=authority.control_schema,
        connector_factory=BudgetedMssqlConnectorFactory(io_deadline=lambda: deadline),
    )
    connection = cursor = None
    result = None
    failure = False
    try:
        _time(deadline)
        connection, service = connections.control_connection_with_service(context.occurrence)
        _require(service == authority.expected_control_service_id)
        _time(deadline)
        connection.autocommit = False
        cursor = connection.cursor()
        ledger = CompositionMssqlLedger(cursor, authority.control_schema)
        transaction = ledger.begin(service)
        cursor.execute(
            "SELECT TOP (2) connector,LOWER(CONVERT(char(36),service_id)),physical_subject_sha256 "
            f"FROM {ledger.table('domains')} WITH (HOLDLOCK) WHERE guard_id=?;",
            domain.guard_id,
        )
        _require(
            tuple(tuple(row) for row in cursor.fetchall())
            == ((domain.connector, domain.service_id, domain.physical_subject_sha256),)
        )
        _time(deadline)
        result = read_service_enrollment(ledger, domain.service_id)
        require_dispatcher_enrollment(config, result, domain.service_id)
        body = result.body
        _require(clickhouse_physical_domain(body["service_id"], body["database_uuid"]) == domain)
        _time(deadline)
        repeated = read_service_enrollment(ledger, domain.service_id)
        _require(repeated.document == result.document and repeated.enrollment_sha256 == result.enrollment_sha256)
        ledger.require_transaction(transaction)
        _time(deadline)
    except Exception:
        failure = True
    finally:
        for operation in (
            connection.rollback if connection is not None else None,
            cursor.close if cursor is not None else None,
            connection.close if connection is not None else None,
        ):
            if operation is not None:
                try:
                    operation()
                except Exception:
                    failure = True
    _require(not failure and result is not None)
    assert result is not None
    _time(deadline)
    return result


def read_bootstrap_enrollment(
    config: DispatcherServiceConfig,
    loader: StagedDispatcherContextLoader,
    deadline: float,
) -> ClickHouseSupervisorEnrollment:
    """Verify every configured authority/target against one exact enrolled original.

    The caller supplies protected configuration and loader. No source/target
    credentials, host RPC, attempt lookup or activation-state read occurs here.
    The same absolute deadline bounds all contexts and is never renewed.
    """
    try:
        _time(deadline)
        _require(type(config) is DispatcherServiceConfig and callable(loader.load_bootstrap))
        _require(
            decode_dispatcher_service_config(
                config.document,
                expected_sha256=config.configuration_sha256,
                bootstrap_uid=config.dispatcher_uid,
                bootstrap_gid=config.dispatcher_gid,
            )
            == config
        )
        original = None
        for digest, authority in config.authorities.items():
            _time(deadline)
            contexts = loader.load_bootstrap(digest)
            _require(type(contexts) is tuple and bool(contexts))
            for context in contexts:
                _time(deadline)
                context.occurrence.__post_init__()
                _require(
                    context.runtime.authority_subject_sha256 == digest
                    and context.occurrence.runtime_context_sha256 == digest
                    and context.binding.dispatcher_id == config.dispatcher_id
                    and context.binding.identity_kind == config.binding_identity_kind
                    and context.binding.identity_sha256 == config.binding_identity_sha256
                )
                domain = _scope(context, authority.control_connection_ref)
                observed = _read(config, context, authority, domain, deadline)
                if original is not None:
                    _require(original.document == observed.document)
                original = observed
        _require(original is not None)
        assert original is not None
        _time(deadline)
        return original
    except Exception:
        raise CompositionAdmissionError("dispatcher_bootstrap_sql_unverified") from None
