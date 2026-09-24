"""Runtime registry rewrite for the closed Airflow Connection init-fetch bridge."""

from __future__ import annotations

import pytest

from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.readiness.airflow_connection_runtime_registry import (
    runtime_connection_registry_for_init_fetch,
    runtime_connection_snapshot_fingerprint,
    runtime_connection_snapshots,
)
from dpone.runtime.credentials.binding_resolver import BindingCredentialResolver


def _env_runtime_resolver(environment="production"):
    source = {
        "connections": {
            "warehouse": {
                "type": "postgres",
                "connection": {"database": "registry_catalog", "schema": "registry_schema"},
                "credentials": {
                    "resolver": "airflow_connection",
                    "connection_id": "shared_db",
                    "version_policy": "latest",
                    "resolution_scope": "workload_start",
                },
            }
        }
    }
    projection = {
        "mode": "env",
        "connections": [
            {
                "connection_ref": "source",
                "registry_connection_ref": "warehouse",
                "connection_id": "shared_db",
            }
        ],
    }
    registry = runtime_connection_registry_for_init_fetch(source, projection=projection)
    assert registry["connections"]["warehouse"]["credentials"] == {
        "resolver": "airflow_env",
        "connection_id": "shared_db",
        "version_policy": "latest",
        "resolution_scope": "workload_start",
    }
    assert source["connections"]["warehouse"]["credentials"]["resolver"] == "airflow_connection"
    return BindingCredentialResolver(
        binding_set={"environment": environment, "bindings": {"source": {"connection_ref": "warehouse"}}},
        connection_registry=registry,
    )


@pytest.mark.parametrize("environment", ["dev", "prod", "production"])
def test_env_projection_resolves_uri_with_registry_coordinates(monkeypatch, environment):
    monkeypatch.setenv("AIRFLOW_CONN_SHARED_DB", "postgresql://runtime:sentinel@db.invalid:5432/old_catalog")
    resolved = _env_runtime_resolver(environment).resolve("source")
    assert resolved.credentials.database == "registry_catalog"
    assert resolved.credentials.schema == "registry_schema"
    assert resolved.credentials.host == "db.invalid"
    assert resolved.credentials.password == "sentinel"
    assert resolved.safe_metadata["resolver"] == "airflow_env"
    assert resolved.safe_metadata["version_policy"] == "latest"
    assert resolved.safe_metadata["credential_ref_fingerprint"].startswith("sha256:")
    assert "sentinel" not in repr(resolved.safe_metadata)


@pytest.mark.parametrize("value", [None, "", "sentinel", "postgresql://host:sentinel/db", "sentinel://host"])
def test_env_projection_fails_closed_without_secret_diagnostics(monkeypatch, value):
    monkeypatch.delenv("AIRFLOW_CONN_SHARED_DB", raising=False)
    if value is not None:
        monkeypatch.setenv("AIRFLOW_CONN_SHARED_DB", value)
    with pytest.raises(ValueError, match="airflow_env projected connection") as failure:
        _env_runtime_resolver().resolve("source")
    assert "sentinel" not in str(failure.value)
    assert failure.value.__suppress_context__ or value in (None, "")


def test_env_projection_rejects_normalized_name_collision():
    projection = {
        "mode": "env",
        "connections": [
            {"registry_connection_ref": "one", "connection_id": "shared-db"},
            {"registry_connection_ref": "two", "connection_id": "shared_db"},
        ],
    }
    with pytest.raises(ValueError, match="conflicting connection identities"):
        runtime_connection_registry_for_init_fetch({"connections": {"one": {}}}, projection=projection)


def test_env_resolver_preserves_unsupported_policy_rejection(monkeypatch):
    resolver = _env_runtime_resolver()
    resolver._connection_registry["connections"]["warehouse"]["credentials"]["version_policy"] = "pinned"
    monkeypatch.setenv("AIRFLOW_CONN_SHARED_DB", "postgresql://user:secret@db.invalid/catalog")
    with pytest.raises(ValueError, match="(?i)pinned"):
        resolver.resolve("source")


@pytest.mark.parametrize("entry", [None, {}, {"credentials": {"resolver": "vault_kv"}}])
def test_env_projection_rejects_missing_or_incompatible_registry_entry(entry):
    projection = {
        "mode": "env",
        "connections": [
            {
                "registry_connection_ref": "warehouse",
                "connection_id": "shared_db",
            }
        ],
    }
    registry = {"connections": {"warehouse": entry}} if entry is not None else {}
    with pytest.raises(ValueError, match="existing airflow_connection"):
        runtime_connection_registry_for_init_fetch(registry, projection=projection)


def test_env_registry_supports_shared_physical_connection_for_distinct_catalogs(monkeypatch):
    monkeypatch.setenv("AIRFLOW_CONN_SHARED_DB", "postgresql://user:secret@db.invalid/old")
    source = {
        "connections": {
            name: {
                "connection": {"database": name},
                "credentials": {
                    "resolver": "airflow_connection",
                    "connection_id": "shared_db",
                },
            }
            for name in ("first_catalog", "second_catalog")
        }
    }
    projection = {
        "mode": "env",
        "connections": [
            {"registry_connection_ref": name, "connection_id": "shared_db"} for name in source["connections"]
        ],
    }
    resolver = BindingCredentialResolver(
        binding_set={
            "environment": "prod",
            "bindings": {name: {"connection_ref": name} for name in source["connections"]},
        },
        connection_registry=runtime_connection_registry_for_init_fetch(source, projection=projection),
    )
    for name in source["connections"]:
        assert resolver.resolve(name).credentials.database == name


def test_runtime_registry_rewrites_airflow_connection_to_secret_volume_mounts(tmp_path) -> None:
    source_registry = {
        "schema": "dpone.connection-registry.v1",
        "connections": {
            "warehouse": {
                "type": "postgres",
                "connection": {"host": "db.internal", "port": 5432, "database": "dwh"},
                "credentials": {
                    "resolver": "airflow_connection",
                    "connection_id": "warehouse",
                    "execution_mode": "operator_bridge",
                },
            },
            "vaulted": {
                "type": "postgres",
                "connection": {"host": "vaulted.internal", "port": 5432, "database": "dwh"},
                "credentials": {
                    "resolver": "vault_kv",
                    "mount": "secret",
                    "path": "data/pg",
                    "fields": {"username": "username", "password": "password"},
                    "version_policy": "latest",
                    "resolution_scope": "workload_start",
                },
            },
        },
    }

    runtime_registry = runtime_connection_registry_for_init_fetch(source_registry)

    assert source_registry["connections"]["warehouse"]["credentials"]["resolver"] == "airflow_connection"
    warehouse = runtime_registry["connections"]["warehouse"]["credentials"]
    assert warehouse == {
        "resolver": "kubernetes_secret_volume",
        "secret_name": "dpone-airflow-connection-bridge",
        "mount_path": "/run/secrets/dpone/airflow-connections/warehouse",
        "payload_format": "airflow_connection_uri",
        "fields": {"uri": "uri"},
    }
    assert runtime_registry["connections"]["vaulted"]["credentials"]["resolver"] == "vault_kv"

    mount = tmp_path / "warehouse"
    mount.mkdir(parents=True)
    (mount / "uri").write_text(
        "postgresql://runtime:secret@db.internal:5432/dwh",
        encoding="utf-8",
    )
    runtime_registry["connections"]["warehouse"]["credentials"]["mount_path"] = mount.as_posix()
    resolver = BindingCredentialResolver(
        binding_set={
            "schema": "dpone.binding-set.v1",
            "environment": "dev",
            "bindings": {"warehouse": {"connection_ref": "warehouse"}},
        },
        connection_registry=runtime_registry,
    )

    resolved = resolver.resolve("warehouse")

    assert resolved.credentials.host == "db.internal"
    assert resolved.credentials.username == "runtime"
    assert resolved.credentials.password == "secret"
    assert resolved.safe_metadata["resolver"] == "kubernetes_secret_volume"


def test_runtime_snapshot_fingerprint_tracks_rewritten_registry_not_source() -> None:
    source_registry = {
        "schema": "dpone.connection-registry.v1",
        "environment": "dev",
        "connections": {
            "warehouse": {
                "type": "postgres",
                "connection": {"host": "db.internal", "port": 5432, "database": "dwh"},
                "credentials": {
                    "resolver": "airflow_connection",
                    "connection_id": "warehouse",
                    "execution_mode": "operator_bridge",
                },
            }
        },
    }
    binding_set = {
        "schema": "dpone.binding-set.v1",
        "environment": "dev",
        "bindings": {"warehouse": {"connection_ref": "warehouse"}},
    }
    credential_runtime = {
        "schema": "dpone.credential-runtime.v1",
        "environment": "dev",
        "runtime": {"mode": "airflow_dev"},
    }

    snapshots = runtime_connection_snapshots(
        binding_set=binding_set,
        connection_registry=source_registry,
        credential_runtime=credential_runtime,
    )
    rewritten = runtime_connection_registry_for_init_fetch(source_registry)

    assert canonical_fingerprint(source_registry) != canonical_fingerprint(rewritten)
    assert runtime_connection_snapshot_fingerprint(snapshots["connection_registry"]) == (
        canonical_fingerprint(rewritten)
    )


@pytest.mark.parametrize(
    ("section", "payload"),
    [
        (
            "connection_registry",
            {
                "schema": "dpone.connection-registry.v1",
                "environment": "prod",
                "connections": {
                    "warehouse": {
                        "type": "postgres",
                        "connection": {
                            "host": "db.internal",
                            "password": "must-not-enter-artifact",
                        },
                        "credentials": {
                            "resolver": "vault_kv",
                            "mount": "kv",
                            "path": "prod/warehouse",
                            "fields": {"password": "password"},
                            "version_policy": "latest",
                            "resolution_scope": "workload_start",
                        },
                    }
                },
            },
        ),
        (
            "credential_runtime",
            {
                "schema": "dpone.credential-runtime.v1",
                "environment": "prod",
                "vault": {
                    "address": "https://vault.internal",
                    "auth": {
                        "method": "kubernetes",
                        "role": "dpone-runtime",
                        "token": "must-not-enter-artifact",
                    },
                },
            },
        ),
    ],
)
def test_runtime_connection_snapshots_reject_inline_secrets(
    section: str,
    payload: dict[str, object],
) -> None:
    binding_set = {
        "schema": "dpone.binding-set.v1",
        "environment": "prod",
        "bindings": {"warehouse": {"connection_ref": "warehouse"}},
    }
    connection_registry = (
        payload
        if section == "connection_registry"
        else {
            "schema": "dpone.connection-registry.v1",
            "environment": "prod",
            "connections": {
                "warehouse": {
                    "type": "postgres",
                    "connection": {"host": "db.internal"},
                    "credentials": {
                        "resolver": "env_var",
                        "support": "development_only",
                        "fields": {"password": "WAREHOUSE_PASSWORD"},
                    },
                }
            },
        }
    )
    credential_runtime = (
        payload
        if section == "credential_runtime"
        else {
            "schema": "dpone.credential-runtime.v1",
            "environment": "prod",
        }
    )

    with pytest.raises(ValueError, match="DPONE_SECRET_VALUE_IN_REGISTRY") as exc_info:
        runtime_connection_snapshots(
            binding_set=binding_set,
            connection_registry=connection_registry,
            credential_runtime=credential_runtime,
        )

    assert "must-not-enter-artifact" not in str(exc_info.value)
