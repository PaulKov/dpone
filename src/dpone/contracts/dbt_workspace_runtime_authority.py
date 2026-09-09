"""Workload-neutral runtime connection authority used during workspace activation."""

from __future__ import annotations

import re
from dataclasses import dataclass

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.contracts.runtime_connection import runtime_connection_authority_subject

_ENVIRONMENT = re.compile(r"^[a-z0-9][a-z0-9_-]{0,62}$")


@dataclass(frozen=True, slots=True)
class DbtWorkspaceRuntimeAuthority:
    """Exact verified shared descriptors, without workload-specific plan identity."""

    environment: str
    release_id: str
    deployment_id: str
    release_sha256: str
    deployment_sha256: str
    binding_set_sha256: str
    connection_registry_sha256: str
    credential_runtime_sha256: str
    authority_subject_sha256: str

    def __post_init__(self) -> None:
        if _ENVIRONMENT.fullmatch(self.environment) is None:
            raise ValueError("workspace runtime environment is invalid")
        digests = (
            self.release_id,
            self.deployment_id,
            self.release_sha256,
            self.deployment_sha256,
            self.binding_set_sha256,
            self.connection_registry_sha256,
            self.credential_runtime_sha256,
            self.authority_subject_sha256,
        )
        if any(not is_canonical_sha256_digest(value) for value in digests):
            raise ValueError("workspace runtime authority digest is invalid")
        if self.authority_subject_sha256 != runtime_connection_authority_subject(
            environment=self.environment,
            release_id=self.release_id,
            deployment_id=self.deployment_id,
            release_sha256=self.release_sha256,
            deployment_sha256=self.deployment_sha256,
            binding_set_sha256=self.binding_set_sha256,
            connection_registry_sha256=self.connection_registry_sha256,
            credential_runtime_sha256=self.credential_runtime_sha256,
        ):
            raise ValueError("workspace runtime authority subject differs from descriptors")

    @classmethod
    def build(
        cls,
        *,
        environment: str,
        release_id: str,
        deployment_id: str,
        release_sha256: str,
        deployment_sha256: str,
        binding_set_sha256: str,
        connection_registry_sha256: str,
        credential_runtime_sha256: str,
    ) -> DbtWorkspaceRuntimeAuthority:
        return cls(
            environment=environment,
            release_id=release_id,
            deployment_id=deployment_id,
            release_sha256=release_sha256,
            deployment_sha256=deployment_sha256,
            binding_set_sha256=binding_set_sha256,
            connection_registry_sha256=connection_registry_sha256,
            credential_runtime_sha256=credential_runtime_sha256,
            authority_subject_sha256=runtime_connection_authority_subject(
                environment=environment,
                release_id=release_id,
                deployment_id=deployment_id,
                release_sha256=release_sha256,
                deployment_sha256=deployment_sha256,
                binding_set_sha256=binding_set_sha256,
                connection_registry_sha256=connection_registry_sha256,
                credential_runtime_sha256=credential_runtime_sha256,
            ),
        )


__all__ = ["DbtWorkspaceRuntimeAuthority"]
