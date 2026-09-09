"""Protected persistence boundary for workspace activation occurrences."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

from dpone.contracts.dbt_workspace_control import (
    DbtReleaseSources,
    DbtWorkspaceActivationReceipt,
    DbtWorkspaceActivationRequest,
    DbtWorkspaceActiveActivation,
    DbtWorkspacePhysicalResource,
    DbtWorkspacePreparedActivation,
    DbtWorkspaceRetiredActivation,
    DbtWorkspaceRetiringActivation,
    DbtWorkspaceRuntimeAuthority,
    MssqlWorkspaceObservation,
    MssqlWorkspaceObservationRequest,
    ResolvedBindingConnection,
)


class DbtWorkspaceActivationAdmissionPort(Protocol):
    """Reserve, activate and reconcile an exact durable occurrence."""

    def prepare(self, request: DbtWorkspaceActivationRequest) -> DbtWorkspaceActivationReceipt:
        """Persist/read back PREPARED with the complete sorted guard closure."""

    def activate(self, request: DbtWorkspaceActivationRequest) -> DbtWorkspaceActivationReceipt:
        """Transition the exact PREPARED occurrence and read back ACTIVE."""

    def require_active(self, request: DbtWorkspaceActivationRequest) -> DbtWorkspaceActivationReceipt:
        """Read exact ACTIVE authority without creating a reservation."""

    def begin_retirement(self, request: DbtWorkspaceActivationRequest) -> DbtWorkspaceActivationReceipt:
        """Fence new attempts and read back RETIRING."""

    def finalize_retirement(self, request: DbtWorkspaceActivationRequest) -> DbtWorkspaceActivationReceipt:
        """Release exact epochs only after durable terminal/quiescence proof."""


class DbtWorkspaceActivationCoordinatorPort(Protocol):
    """Own source/context preparation and durable activation saga operations."""

    def prepare(
        self,
        *,
        projection_root: Path,
        activation_id: str,
        environment: str,
        release_id: str,
        deployment_id: str,
        previous_deployment_id: str | None,
    ) -> DbtWorkspacePreparedActivation:
        """Build, persist and read back the exact PREPARED occurrence."""

    def activate(
        self,
        prepared: DbtWorkspacePreparedActivation,
        *,
        projection_root: Path,
    ) -> DbtWorkspaceActiveActivation:
        """Transition and read back the exact ACTIVE occurrence."""

    def require_active(
        self,
        *,
        projection_root: Path,
        activation_id: str,
        environment: str,
        release_id: str,
        deployment_id: str,
        previous_deployment_id: str | None,
    ) -> DbtWorkspaceActiveActivation:
        """Verify an existing ACTIVE occurrence without reserving again."""

    def begin_retirement(
        self,
        active: DbtWorkspaceActiveActivation,
        *,
        projection_root: Path,
    ) -> DbtWorkspaceRetiringActivation:
        """Close new task admission while preserving in-flight attempt ownership."""

    def finalize_retirement(
        self,
        retiring: DbtWorkspaceRetiringActivation,
        *,
        projection_root: Path,
    ) -> DbtWorkspaceRetiredActivation:
        """Release the exact occurrence after durable terminal quiescence."""


class DbtWorkspacePhysicalAuthorityPort(Protocol):
    """Map a complete live observation to platform-global collision guards."""

    def authorize_mssql(
        self,
        *,
        connection_ref: str,
        observation: MssqlWorkspaceObservation,
        write_subjects: tuple[str, ...],
    ) -> tuple[DbtWorkspacePhysicalResource, ...]:
        """Return a complete partition; aliases and observed IDs are insufficient."""


class DbtWorkspaceMssqlObservationPort(Protocol):
    """Read one complete bounded SQL Server catalog observation."""

    def observe(
        self,
        request: MssqlWorkspaceObservationRequest,
        connection: ResolvedBindingConnection,
    ) -> MssqlWorkspaceObservation:
        """Return complete read-only evidence or fail closed."""


class DbtWorkspaceActivationInputPort(Protocol):
    """Load exact source and shared runtime authorities from a sealed projection."""

    def load_sources(self, *, projection_root: Path, release_id: str) -> DbtReleaseSources:
        """Read the complete immutable source/write closure."""

    def load_runtime_authority(
        self,
        *,
        projection_root: Path,
        environment: str,
        release_id: str,
        deployment_id: str,
    ) -> DbtWorkspaceRuntimeAuthority:
        """Read verified shared descriptors without a workload-specific plan."""

    def resolve_connection(
        self,
        authority: DbtWorkspaceRuntimeAuthority,
        connection_ref: str,
    ) -> ResolvedBindingConnection:
        """Resolve one connection through that exact bounded authority."""


class DbtWorkspaceConnectionResolver(Protocol):
    """Resolve credentials lazily after a verified shared authority is selected."""

    def resolve(self, connection_ref: str) -> ResolvedBindingConnection:
        """Resolve one logical ref without exposing secret material to contracts."""


class DbtWorkspaceActivationAdmissionFactoryPort(Protocol):
    """Rebuild durable admission from the exact sealed runtime authority."""

    def build(
        self,
        resolver: DbtWorkspaceConnectionResolver,
    ) -> DbtWorkspaceActivationAdmissionPort:
        """Return a fresh connection-capable adapter for one coordinator phase."""


class DbtWorkspaceRuntimeResolverFactory(Protocol):
    """Compose a platform resolver from exact verified runtime snapshots."""

    def build(
        self,
        *,
        authority: DbtWorkspaceRuntimeAuthority,
        binding_set: dict[str, Any],
        connection_registry: dict[str, Any],
        credential_runtime: dict[str, Any],
    ) -> DbtWorkspaceConnectionResolver:
        """Return the environment-specific resolver capability."""


__all__ = [
    "DbtWorkspaceActivationAdmissionPort",
    "DbtWorkspaceActivationAdmissionFactoryPort",
    "DbtWorkspaceActivationCoordinatorPort",
    "DbtWorkspaceActivationInputPort",
    "DbtWorkspaceConnectionResolver",
    "DbtWorkspaceMssqlObservationPort",
    "DbtWorkspacePhysicalAuthorityPort",
    "DbtWorkspaceRuntimeResolverFactory",
]
