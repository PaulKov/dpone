"""Admission preparation uses complete sources and loader-owned runtime authority."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from dpone.contracts.dbt_relation_writes import DbtRelationWrite
from dpone.contracts.dbt_workspace_activation import (
    DbtWorkspaceActivationError,
    DbtWorkspacePhysicalResource,
)
from dpone.contracts.runtime_connection import ResolvedBindingConnection, ResolvedConnectionDescriptor
from dpone.runtime.credentials.config import CredentialsConfig
from dpone.runtime.credentials.runtime_context import RuntimeConnectionContext
from dpone.runtime.credentials.workload_scope import WorkloadScopedCredentialResolver
from dpone.services.dbt_workspace_activation_preparation import DbtWorkspaceActivationPreparation

ACTIVATION_ID = "164a3c74-cf85-4a4a-a087-07c9b07050ff"
RELEASE_ID = "sha256:" + "a" * 64
DEPLOYMENT_ID = "sha256:" + "b" * 64
CONTEXT_ID = "sha256:" + "c" * 64


def _digest(character: str) -> str:
    return "sha256:" + character * 64


def _write(*, connector="mssql", database="warehouse"):
    return DbtRelationWrite(
        "dbt/demo",
        "orders",
        "model.demo.orders",
        "model",
        connector,
        "warehouse",
        database,
        "mart",
        "orders",
    )


def _sources(*writes):
    profile = SimpleNamespace(connection_ref="warehouse", database="warehouse")
    execution = SimpleNamespace(invocation_profile=lambda: profile)
    workflow = SimpleNamespace(execution=execution)
    return SimpleNamespace(
        release_id=RELEASE_ID,
        inventory=SimpleNamespace(snapshot_sha256=_digest("d")),
        workflows=(workflow,),
        relation_writes=writes or (_write(),),
    )


def _connection():
    return ResolvedBindingConnection(
        credentials=CredentialsConfig(host="fixture", database="warehouse", username="fixture", password="secret"),
        safe_metadata={},
        descriptor=ResolvedConnectionDescriptor(
            "mssql",
            {
                "database": "warehouse",
                "database_authorities": {
                    "warehouse": {
                        "database_id": 7,
                        "create_token": "2026-01-01T00:00:00.000",
                        "database_guid": "00000000-0000-0000-0000-000000000007",
                    }
                },
            },
        ),
    )


class _Resolver:
    def __init__(self, connection=None):
        self.connection = connection or _connection()
        self.calls = []

    def resolve(self, connection_ref):
        self.calls.append(connection_ref)
        return self.connection


def _context(*, authority=CONTEXT_ID, release=RELEASE_ID):
    resolver = _Resolver()
    return (
        RuntimeConnectionContext(
            environment="prod",
            binding_set={},
            connection_registry={},
            credential_runtime={},
            resolver=WorkloadScopedCredentialResolver(resolver),
            release_id=release,
            deployment_id=DEPLOYMENT_ID,
            init_fetch_plan_sha256=_digest("e"),
            authority_subject_sha256=authority,
        ),
        resolver,
    )


class _Observer:
    def __init__(self):
        self.calls = []

    def observe(self, request, connection):
        self.calls.append((request, connection))
        return SimpleNamespace(request=request, subject_sha256=request.subject_sha256)


class _Authority:
    def __init__(self, *, incomplete=False):
        self.incomplete = incomplete
        self.calls = []

    def authorize_mssql(self, *, connection_ref, observation, write_subjects):
        self.calls.append((connection_ref, observation, write_subjects))
        subjects = () if self.incomplete else write_subjects
        if not subjects:
            return ()
        return (
            DbtWorkspacePhysicalResource(
                guard_id="mssql://platform-service/warehouse/mart/orders",
                connector="mssql",
                service_authority_sha256=_digest("1"),
                target_authority_sha256=_digest("2"),
                observation_sha256=observation.subject_sha256,
                write_subjects=subjects,
            ),
        )


def test_preparation_joins_sources_runtime_pin_observation_and_physical_authority():
    observer, authority = _Observer(), _Authority()
    context, resolver = _context()
    request = DbtWorkspaceActivationPreparation(
        physical_authority=authority,
        mssql_observer=observer,
    ).prepare(
        activation_id=ACTIVATION_ID,
        previous_deployment_id=None,
        sources=_sources(),
        runtime_context=context,
    )

    assert request.release_id == RELEASE_ID
    assert request.deployment_id == DEPLOYMENT_ID
    assert request.runtime_context_sha256 == CONTEXT_ID
    assert resolver.calls == ["warehouse"]
    observation_request = observer.calls[0][0]
    assert observation_request.default_database == "warehouse"
    assert observation_request.invocation_databases == ("warehouse",)
    assert authority.calls[0][0] == "warehouse"


@pytest.mark.parametrize("authority,release", [(None, RELEASE_ID), (CONTEXT_ID, _digest("f"))])
def test_unverified_or_foreign_runtime_context_is_rejected_before_resolution(authority, release):
    context, resolver = _context(authority=authority, release=release)
    with pytest.raises(DbtWorkspaceActivationError, match="runtime_context"):
        DbtWorkspaceActivationPreparation(physical_authority=_Authority(), mssql_observer=_Observer()).prepare(
            activation_id=ACTIVATION_ID,
            previous_deployment_id=None,
            sources=_sources(),
            runtime_context=context,
        )
    assert resolver.calls == []


def test_connector_without_complete_authority_is_rejected_before_credentials():
    context, resolver = _context()
    with pytest.raises(DbtWorkspaceActivationError, match="connector_authority_unavailable"):
        DbtWorkspaceActivationPreparation(physical_authority=_Authority(), mssql_observer=_Observer()).prepare(
            activation_id=ACTIVATION_ID,
            previous_deployment_id=None,
            sources=_sources(_write(connector="clickhouse")),
            runtime_context=context,
        )
    assert resolver.calls == []


def test_missing_physical_partition_never_yields_an_activation_request():
    context, _ = _context()
    with pytest.raises(DbtWorkspaceActivationError, match="resource_(?:closure|partition)"):
        DbtWorkspaceActivationPreparation(
            physical_authority=_Authority(incomplete=True),
            mssql_observer=_Observer(),
        ).prepare(
            activation_id=ACTIVATION_ID,
            previous_deployment_id=None,
            sources=_sources(),
            runtime_context=context,
        )
