from __future__ import annotations

from collections.abc import Mapping
from dataclasses import FrozenInstanceError

import pytest

from dpone.contracts.credential_resolution import BACKEND_UNAVAILABLE, CredentialResolutionError
from dpone.contracts.runtime_connection import ResolvedBindingConnection, ResolvedConnectionDescriptor
from dpone.runtime.credentials.binding_resolver import BindingCredentialResolver
from dpone.runtime.credentials.binding_resolver import ResolvedBindingConnection as RuntimeResolvedBindingConnection
from dpone.runtime.credentials.config import CredentialsConfig


def test_resolved_binding_connection_preserves_existing_positional_constructor() -> None:
    credentials = CredentialsConfig(host="postgres.internal")
    safe_metadata = {"connection_ref": "pg_prod"}

    resolved = ResolvedBindingConnection(credentials, safe_metadata)

    assert resolved.credentials is credentials
    assert resolved.safe_metadata is safe_metadata
    assert resolved.descriptor is None
    assert RuntimeResolvedBindingConnection is ResolvedBindingConnection


def test_connection_descriptor_recursively_freezes_a_copy_and_hides_properties_from_repr() -> None:
    properties = {
        "host": "postgres.internal",
        "parameters": {
            "application_name": "orders",
            "retry_delays": [1, 2],
        },
    }

    descriptor = ResolvedConnectionDescriptor(connection_type="postgres", properties=properties)
    properties["host"] = "changed.example"
    properties["parameters"]["application_name"] = "changed"
    properties["parameters"]["retry_delays"].append(3)

    assert descriptor.properties["host"] == "postgres.internal"
    parameters = descriptor.properties["parameters"]
    assert isinstance(parameters, Mapping)
    assert parameters["application_name"] == "orders"
    assert parameters["retry_delays"] == (1, 2)
    with pytest.raises(TypeError):
        descriptor.properties["host"] = "forbidden"
    with pytest.raises(TypeError):
        parameters["application_name"] = "forbidden"
    with pytest.raises(FrozenInstanceError):
        descriptor.connection_type = "changed"
    assert "postgres.internal" not in repr(descriptor)
    assert "application_name" not in repr(descriptor)


@pytest.mark.parametrize(
    ("authored", "canonical"),
    [
        ("postgresql", "postgres"),
        ("PostgreSQL", "postgres"),
        ("mssql", "mssql"),
        ("MSSQL", "mssql"),
        ("microsoft mssql", "mssql"),
        ("microsoft_mssql", "mssql"),
        ("odbc", "mssql"),
        ("sqlserver", "mssql"),
        ("sql_server", "mssql"),
        ("sql-server", "mssql"),
        ("api", "api"),
    ],
)
def test_connection_descriptor_owns_canonical_endpoint_identity(authored: str, canonical: str) -> None:
    descriptor = ResolvedConnectionDescriptor(connection_type=authored, properties={})

    assert descriptor.connection_type == canonical


def test_vault_resolution_projects_declared_metadata_and_only_declared_extra_values() -> None:
    class VaultReader:
        def get_secret(self, *, mount_point: str, path: str) -> dict[str, object]:
            return {
                "db_user": "runtime",
                "db_password": "declared-password",
                "client_id": "declared-runtime-client",
                "hmac_key": "declared-hmac-key",
                "undeclared_backend_value": "must-never-copy",
                "_metadata": {"version": 41},
            }

    connection = {
        "host": "postgres.internal",
        "parameters": {
            "application_name": "orders",
            "retry_delays": [1, 2],
        },
    }
    resolver = BindingCredentialResolver(
        binding_set={
            "environment": "prod",
            "bindings": {"pg_source": {"connection_ref": "pg_prod"}},
        },
        connection_registry={
            "connections": {
                "pg_prod": {
                    "type": "postgres",
                    "connection": connection,
                    "credentials": {
                        "resolver": "vault_kv",
                        "mount": "kv",
                        "kv_version": 2,
                        "path": "dpone/prod/credentials/pg_source",
                        "fields": {
                            "username": "db_user",
                            "password": "db_password",
                            "client_id": "client_id",
                            "hmac_access_key": "hmac_key",
                        },
                        "version_policy": "latest",
                        "resolution_scope": "workload_start",
                    },
                }
            }
        },
        vault_kv_reader=VaultReader(),
    )

    resolved = resolver.resolve("pg_source")
    connection["host"] = "changed.example"
    connection["parameters"]["retry_delays"].append(3)

    assert resolved.descriptor is not None
    assert resolved.descriptor.connection_type == "postgres"
    assert resolved.descriptor.properties["host"] == "postgres.internal"
    assert resolved.descriptor.properties["parameters"]["retry_delays"] == (1, 2)
    assert resolved.credentials.client_id == "declared-runtime-client"
    assert resolved.credentials.additional_params == {
        "application_name": "orders",
        "retry_delays": [1, 2],
        "hmac_access_key": "declared-hmac-key",
    }
    assert "undeclared_backend_value" not in vars(resolved.credentials)
    assert "must-never-copy" not in repr(resolved)
    assert "must-never-copy" not in repr(resolved.safe_metadata)
    assert "declared-password" not in repr(resolved)
    assert "declared-hmac-key" not in repr(resolved)


def test_vault_credential_fields_cannot_override_registry_endpoint_identity() -> None:
    class VaultReader:
        def get_secret(self, *, mount_point: str, path: str) -> dict[str, object]:
            return {
                "db_user": "runtime",
                "db_password": "declared-password",
                "secret_host": "attacker.example",
                "_metadata": {"version": 41},
            }

    resolver = BindingCredentialResolver(
        binding_set={
            "environment": "prod",
            "bindings": {"pg_source": {"connection_ref": "pg_prod"}},
        },
        connection_registry={
            "connections": {
                "pg_prod": {
                    "type": "postgres",
                    "connection": {"host": "postgres.internal", "port": 5432},
                    "credentials": {
                        "resolver": "vault_kv",
                        "mount": "kv",
                        "kv_version": 2,
                        "path": "dpone/prod/credentials/pg_source",
                        "fields": {
                            "username": "db_user",
                            "password": "db_password",
                            "host": "secret_host",
                        },
                        "version_policy": "latest",
                        "resolution_scope": "workload_start",
                    },
                }
            }
        },
        vault_kv_reader=VaultReader(),
    )

    with pytest.raises(ValueError, match="DPONE_RUNTIME_CONNECTION_VALUE_MISMATCH"):
        resolver.resolve("pg_source")


def test_vault_resolution_requires_an_explicitly_injected_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    from dpone.runtime.credentials import binding_resolver as binding_resolver_module

    ambient_reader_called = False

    def ambient_reader() -> object:
        nonlocal ambient_reader_called
        ambient_reader_called = True
        raise AssertionError("strict resolver must not create an ambient Vault reader")

    monkeypatch.setattr(
        binding_resolver_module,
        "_default_vault_kv_reader",
        ambient_reader,
        raising=False,
    )
    resolver = BindingCredentialResolver(
        binding_set={
            "environment": "prod",
            "bindings": {"pg_source": {"connection_ref": "pg_prod"}},
        },
        connection_registry={
            "connections": {
                "pg_prod": {
                    "type": "postgres",
                    "connection": {"host": "postgres.internal"},
                    "credentials": {
                        "resolver": "vault_kv",
                        "mount": "kv",
                        "kv_version": 2,
                        "path": "dpone/prod/credentials/pg_source",
                        "fields": {
                            "username": "username",
                            "password": "password",
                        },
                        "version_policy": "latest",
                        "resolution_scope": "workload_start",
                    },
                }
            }
        },
    )

    with pytest.raises(CredentialResolutionError) as exc:
        resolver.resolve("pg_source")

    assert exc.value.code == BACKEND_UNAVAILABLE
    assert ambient_reader_called is False
    assert "dpone/prod/credentials" not in str(exc.value)


def test_kubernetes_resolution_never_copies_undeclared_backend_payload() -> None:
    class KubernetesReader:
        def read_secret(self, *, namespace: str, name: str) -> dict[str, str]:
            return {
                "user": "runtime",
                "region": "eu-central-1",
                "undeclared_backend_value": "must-never-copy",
            }

    resolver = BindingCredentialResolver(
        binding_set={
            "environment": "prod",
            "bindings": {"pg_source": {"connection_ref": "pg_prod"}},
        },
        connection_registry={
            "connections": {
                "pg_prod": {
                    "type": "postgres",
                    "connection": {"parameters": {"sslmode": "require"}},
                    "credentials": {
                        "resolver": "kubernetes_secret_api",
                        "namespace": "airflow-example",
                        "name": "pg-source",
                        "fields": {
                            "username": "user",
                            "region_name": "region",
                        },
                    },
                }
            }
        },
        kubernetes_secret_reader=KubernetesReader(),
    )

    resolved = resolver.resolve("pg_source")

    assert resolved.credentials.additional_params == {
        "sslmode": "require",
        "region_name": "eu-central-1",
    }
    assert "must-never-copy" not in repr(resolved)
