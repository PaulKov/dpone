"""Read-only inputs consumed by desired-state publication policy."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from dpone.contracts.airflow_credential_promotion import CredentialPromotionEvidence


class DesiredStatePublicationAuthority(Protocol):
    """Credential-free authority facts required by preparation policy."""

    @property
    def environment(self) -> str: ...

    @property
    def source_project(self) -> str: ...

    @property
    def source_ref(self) -> str: ...

    @property
    def registry_scope_id(self) -> str: ...

    @property
    def publish_authority_sha256(self) -> str: ...

    @property
    def workspace_authority_connection_ref(self) -> str | None: ...


class DesiredStatePromotionInput(Protocol):
    """Immutable promotion facts consumed by preparation policy."""

    @property
    def environment(self) -> str: ...

    @property
    def registry_scope_id(self) -> str: ...

    @property
    def release_id(self) -> str: ...

    @property
    def deployment_id(self) -> str: ...

    @property
    def source_git_sha(self) -> str: ...

    @property
    def runtime_image_digest(self) -> str: ...

    @property
    def runtime_image_dbt_digest(self) -> str | None: ...

    @property
    def airflow_index_sha256(self) -> str: ...

    @property
    def expected_dag_ids(self) -> tuple[str, ...]: ...

    @property
    def evidence_sha256(self) -> str: ...

    @property
    def credential_projection(self) -> CredentialPromotionEvidence | None: ...


__all__ = [
    "DesiredStatePromotionInput",
    "DesiredStatePublicationAuthority",
]
