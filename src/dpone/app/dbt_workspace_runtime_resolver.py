"""Compose the production resolver for verified workspace runtime snapshots."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dpone.runtime.credentials.binding_resolver import BindingCredentialResolver
from dpone.runtime.credentials.runtime_context import build_required_vault_kv_reader
from dpone.runtime.credentials.workload_scope import WorkloadScopedCredentialResolver

if TYPE_CHECKING:
    from dpone.contracts.dbt_workspace_runtime_authority import DbtWorkspaceRuntimeAuthority
    from dpone.runtime.credentials.binding_resolver import KubernetesSecretReader
    from dpone.runtime.credentials.runtime_context import RuntimeVaultReaderFactory


class DbtWorkspaceRuntimeResolverFactory:
    """Build the same lazy credential authority used by strict init-fetch."""

    def __init__(
        self,
        *,
        vault_reader_factory: RuntimeVaultReaderFactory | None = None,
        kubernetes_secret_reader: KubernetesSecretReader | None = None,
    ) -> None:
        self._vault_reader_factory = vault_reader_factory
        self._kubernetes_secret_reader = kubernetes_secret_reader

    def build(
        self,
        *,
        authority: DbtWorkspaceRuntimeAuthority,
        binding_set: dict[str, Any],
        connection_registry: dict[str, Any],
        credential_runtime: dict[str, Any],
    ) -> WorkloadScopedCredentialResolver:
        """Return a lazy resolver; credential payloads are never retained in authority DTOs."""

        authority.__post_init__()
        for payload in (binding_set, connection_registry, credential_runtime):
            if payload.get("environment") != authority.environment:
                raise ValueError("runtime snapshot environment differs from workspace authority")
        vault_reader = build_required_vault_kv_reader(
            credential_runtime=credential_runtime,
            connection_registry=connection_registry,
            factory=self._vault_reader_factory,
        )
        return WorkloadScopedCredentialResolver(
            BindingCredentialResolver(
                binding_set=binding_set,
                connection_registry=connection_registry,
                vault_kv_reader=vault_reader,
                kubernetes_secret_reader=self._kubernetes_secret_reader,
                evidence_context={
                    "release_id": authority.release_id,
                    "deployment_id": authority.deployment_id,
                    "credential_runtime_environment": authority.environment,
                },
            )
        )


__all__ = ["DbtWorkspaceRuntimeResolverFactory"]
