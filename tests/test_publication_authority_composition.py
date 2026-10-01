"""A registry-owned endpoint pin, not an alias, authorizes every SQL session."""

from dataclasses import replace
from importlib import import_module

import pytest

from dpone.contracts.clickhouse_cluster_publication import digest_payload
from dpone.contracts.publication_authority_binding import PublicationAuthorityBinding
from dpone.contracts.runtime_connection import ResolvedBindingConnection, ResolvedConnectionDescriptor
from dpone.runtime.credentials.config import CredentialsConfig
from tests.test_mssql_publication_admission import Catalog

_BINDING = PublicationAuthorityBinding("mssql", "metadata", "Example_System", "dbo", "example", "test")
_OBSERVED = ("synthetic-sql-server", "Example_System", "11111111-2222-4333-8444-555555555555")
_PIN = digest_payload(
    {
        "contract": "dpone.mssql-publication-endpoint.v1",
        "server": _OBSERVED[0],
        "database": _OBSERVED[1],
        "database_guid": _OBSERVED[2],
    }
)


def api():
    return import_module("dpone.runtime.publication_authority_composition")


def connection(*, pin=_PIN, service="example", environment="test", database="Example_System", include_policy=True):
    properties = {"database": database, "schema": "dbo"}
    if include_policy:
        properties["publication_authority"] = {
            "service_id": service,
            "environment": environment,
            "endpoint_identity_sha256": pin,
        }
    return ResolvedBindingConnection(
        CredentialsConfig(host="alias.local", database=database), {}, ResolvedConnectionDescriptor("mssql", properties)
    )


class Connector(Catalog):
    def __init__(self, observed=_OBSERVED):
        super().__init__()
        self.observed = observed
        self.closed = False
        self.connection = self
        self.autocommit = True

    def get_records(self, sql, params=()):
        if "SERVERPROPERTY" in sql:
            self.calls.append(sql)
            return [self.observed]
        return super().get_records(sql, params)

    def close(self):
        self.closed = True


def test_build_uses_one_admitted_catalog_and_closes_its_session():
    opened = []

    def factory(value):
        client = Connector()
        opened.append(client)
        return client

    authority = api().build_publication_authority(
        connection=connection(), binding=_BINDING, environment="test", connector_factory=factory
    )
    assert authority is not None
    assert len(opened) == 1 and opened[0].closed


@pytest.mark.parametrize(
    "overrides",
    [
        {"pin": "f" * 64},
        {"service": "other"},
        {"environment": "prod"},
        {"database": "Other_System"},
        {"include_policy": False},
        {"pin": "bad"},
    ],
)
def test_wrong_endpoint_or_namespace_has_no_admitted_authority(overrides):
    opened = []

    def factory(value):
        client = Connector()
        opened.append(client)
        return client

    with pytest.raises(ValueError, match="publication_authority"):
        api().build_publication_authority(
            connection=connection(**overrides), binding=_BINDING, environment="test", connector_factory=factory
        )
    assert all(client.closed for client in opened)


def test_environment_must_match_verified_runtime_context_before_io():
    with pytest.raises(ValueError, match="publication_authority"):
        api().build_publication_authority(
            connection=connection(),
            binding=_BINDING,
            environment="prod",
            connector_factory=lambda _: pytest.fail("connector I/O"),
        )


def test_fresh_write_session_cannot_drift_to_another_endpoint():
    # Catalog admission succeeds, then DNS/backend changes before a read/write
    # transaction. No transaction cursor may open against the second endpoint.
    clients = [Connector(), Connector((_OBSERVED[0], _OBSERVED[1], "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"))]
    remaining = iter(clients)
    authority = api().build_publication_authority(
        connection=connection(), binding=_BINDING, environment="test", connector_factory=lambda _: next(remaining)
    )
    with pytest.raises(Exception, match="READ_UNKNOWN"):
        authority.read_versioned("a" * 64)
    assert all(client.closed for client in clients)
    assert len(clients[1].calls) == 1  # endpoint observation only, no slot read


def test_alias_rotation_keeps_the_registry_endpoint_admission():
    for alias in ("metadata", "rotated_metadata"):
        assert (
            api().build_publication_authority(
                connection=connection(),
                binding=replace(_BINDING, connection_ref=alias),
                environment="test",
                connector_factory=lambda _: Connector(),
            )
            is not None
        )


def test_unverifiable_endpoint_closes_connection_and_redacts_driver_detail():
    class Broken(Connector):
        def get_records(self, sql, params=()):
            raise RuntimeError("synthetic-sensitive-detail")

    client = Broken()
    with pytest.raises(ValueError, match="publication_authority") as error:
        api().build_publication_authority(
            connection=connection(), binding=_BINDING, environment="test", connector_factory=lambda _: client
        )
    assert client.closed and "synthetic-sensitive-detail" not in str(error.value)


def test_catalog_close_failure_cannot_leak_driver_details_or_admit_authority():
    class BrokenClose(Connector):
        def close(self):
            raise RuntimeError("synthetic-sensitive-detail")

    with pytest.raises(ValueError, match="publication_authority") as error:
        api().build_publication_authority(
            connection=connection(), binding=_BINDING, environment="test", connector_factory=lambda _: BrokenClose()
        )
    assert "synthetic-sensitive-detail" not in str(error.value)


def test_bound_provider_readiness_rechecks_same_factory_without_ch_bootstrap():
    admitted = []

    def factory():
        authority = object()
        admitted.append(authority)
        return authority

    provider = api().BoundPublicationAuthorityProvider(factory)
    assert admitted == []  # construction is import-/network-free
    provider.ensure("replicas", "Business_A", ("one", "two"))
    assert provider.for_database("Business_A") is admitted[0]
    assert provider.for_database("Business_B") is admitted[0]
    provider.ensure("replicas", "Business_B", ("one", "two"))
    assert len(admitted) == 2
    assert provider.for_database("Business_A") is admitted[1]


def test_bound_provider_rejects_failed_readmission_without_stale_cached_authority():
    attempts = []

    def factory():
        attempts.append(1)
        if len(attempts) > 1:
            raise ValueError("catalog no longer admitted")
        return object()

    provider = api().BoundPublicationAuthorityProvider(factory)
    provider.ensure("replicas", "Business", ("one", "two"))
    with pytest.raises(ValueError, match="no longer admitted"):
        provider.ensure("replicas", "Business", ("one", "two"))
    with pytest.raises(ValueError, match="no longer admitted"):
        provider.for_database("Business")


def test_native_provider_keeps_origin_observation_and_mutation_on_one_admitted_handle():
    from tests.test_native_prepared_recovery import Authority, Catalog

    authority = Authority(Catalog())
    provider = api().BoundPublicationAuthorityProvider[Authority](lambda: authority)
    provider.ensure("cluster", "analytics", ("one", "two"))
    observed = provider.for_database("analytics")
    original = observed.read_native_preparation(
        authority.current.record.target_key, authority.current.record.operation_id
    )
    assert original.prepared == authority.current
    assert observed is provider.for_database("other") is authority


def test_schema_operator_plans_without_early_catalog_or_connector_admission():
    operator = api().build_publication_schema(
        connection=connection(),
        binding=_BINDING,
        environment="test",
        connector_factory=lambda _: pytest.fail("schema plan attempted SQL I/O"),
    )
    assert operator.plan().endpoint_identity == _PIN


@pytest.mark.parametrize("action", ["inspect", "apply"])
def test_schema_operator_requires_endpoint_pin_before_catalog_or_ddl(action):
    client = Connector((_OBSERVED[0], _OBSERVED[1], "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"))
    operator = api().build_publication_schema(
        connection=connection(),
        binding=_BINDING,
        environment="test",
        connector_factory=lambda _: client,
    )
    plan = operator.plan()
    result = operator.inspect(plan) if action == "inspect" else operator.apply(plan, confirmation_digest=plan.digest)
    assert result.status == "outcome_unknown"
    assert client.closed
    assert len(client.calls) == 1


def test_schema_operator_uses_real_shared_session_path_without_requiring_catalog_first():
    from tests.test_mssql_publication_schema import SchemaSession

    class Client(SchemaSession):
        @property
        def connection(self):
            return self

        def get_records(self, sql, params=()):
            if "SERVERPROPERTY" in sql:
                return [_OBSERVED]
            return super().get_records(sql, params)

    client = Client()
    operator = api().build_publication_schema(
        connection=connection(),
        binding=_BINDING,
        environment="test",
        connector_factory=lambda _: client,
    )
    assert operator.inspect(operator.plan()).status == "ready"
    result = operator.apply(operator.plan(), confirmation_digest=operator.plan().digest)
    assert result.status == "completed"
    assert len(client.ddl) == 3


def test_schema_operator_rejects_unpinned_environment_before_io():
    with pytest.raises(ValueError, match="publication_authority"):
        api().build_publication_schema(
            connection=connection(),
            binding=_BINDING,
            environment="prod",
            connector_factory=lambda _: pytest.fail("unadmitted environment performed I/O"),
        )
