"""Workspace resolver factory preserves strict init-fetch credential policy."""

from __future__ import annotations

import pytest

from dpone.app.dbt_workspace_runtime_resolver import DbtWorkspaceRuntimeResolverFactory
from dpone.contracts.dbt_workspace_runtime_authority import DbtWorkspaceRuntimeAuthority
from dpone.runtime.credentials.binding_resolver import VaultSecretSnapshot


def _digest(character: str) -> str:
    return "sha256:" + character * 64


def _authority(*, environment: str = "prod") -> DbtWorkspaceRuntimeAuthority:
    return DbtWorkspaceRuntimeAuthority.build(
        environment=environment,
        release_id=_digest("a"),
        deployment_id=_digest("b"),
        release_sha256=_digest("c"),
        deployment_sha256=_digest("d"),
        binding_set_sha256=_digest("e"),
        connection_registry_sha256=_digest("f"),
        credential_runtime_sha256=_digest("1"),
    )


class _Vault:
    def get_secret(self, *, mount_point: str, path: str) -> VaultSecretSnapshot:
        assert (mount_point, path) == ("secret", "dwh/mssql")
        return VaultSecretSnapshot(
            data={"username": "runtime-user", "password": "runtime-secret"},
            version=7,
        )


def _snapshots(environment: str = "prod") -> dict[str, dict[str, object]]:
    return {
        "binding_set": {
            "schema": "dpone.binding-set.v1",
            "environment": environment,
            "bindings": {"warehouse": {"connection_ref": "warehouse-prod"}},
        },
        "connection_registry": {
            "schema": "dpone.connection-registry.v1",
            "environment": environment,
            "connections": {
                "warehouse-prod": {
                    "type": "mssql",
                    "connection": {"host": "sql.internal", "database": "warehouse"},
                    "credentials": {
                        "resolver": "vault_kv",
                        "mount": "secret",
                        "path": "dwh/mssql",
                        "kv_version": 2,
                        "version_policy": "latest",
                        "resolution_scope": "workload_start",
                        "fields": {"username": "username", "password": "password"},
                    },
                }
            },
        },
        "credential_runtime": {
            "schema": "dpone.credential-runtime.v1",
            "environment": environment,
        },
    }


def test_factory_resolves_through_exact_runtime_snapshots_without_authority_secrets() -> None:
    authority = _authority()
    snapshots = _snapshots()
    resolver = DbtWorkspaceRuntimeResolverFactory(vault_reader_factory=lambda _: _Vault()).build(
        authority=authority,
        **snapshots,  # type: ignore[arg-type]
    )

    resolved = resolver.resolve("warehouse")

    assert resolved.credentials.username == "runtime-user"
    assert resolved.credentials.password == "runtime-secret"
    assert resolved.safe_metadata["resolved_version"] == 7
    assert resolved.safe_metadata["release_id"] == authority.release_id
    assert "runtime-secret" not in repr(authority)


def test_foreign_environment_is_rejected_before_backend_resolution() -> None:
    with pytest.raises(ValueError, match="environment differs"):
        DbtWorkspaceRuntimeResolverFactory(vault_reader_factory=lambda _: _Vault()).build(
            authority=_authority(),
            **_snapshots("dev"),  # type: ignore[arg-type]
        )
