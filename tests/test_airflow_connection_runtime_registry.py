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
