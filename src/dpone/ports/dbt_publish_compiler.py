"""Capability ports for dbt artifact compilation orchestration."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from dpone.contracts.dbt_publish_models import (
        CompiledDbtModel,
        DbtManifestArtifact,
        DbtModelArtifact,
        DbtPublishIntent,
        DbtPublishIssue,
        DbtPublishProfile,
        DbtPublishStrategyPolicy,
        DbtWorkflowProfile,
    )


class DbtArtifactReaderPort(Protocol):
    """Read a compiled dbt artifact into the canonical immutable model."""

    def read(self, path: str | Path) -> tuple[DbtManifestArtifact | None, tuple[DbtPublishIssue, ...]]: ...


class DbtPublishIntentResolverPort(Protocol):
    """Resolve one publish intent from dbt model metadata."""

    def resolve(self, model: DbtModelArtifact) -> tuple[DbtPublishIntent | None, tuple[DbtPublishIssue, ...]]: ...


class DbtModelCompilerPort(Protocol):
    """Compile one model and trusted profile into a runtime workload."""

    def compile(
        self,
        model: DbtModelArtifact,
        intent: DbtPublishIntent,
        profile: DbtPublishProfile,
        strategy_policy: DbtPublishStrategyPolicy,
        *,
        supported_strategies: tuple[str, ...] | None = None,
    ) -> CompiledDbtModel: ...

    def strategy_candidates(
        self,
        model: DbtModelArtifact,
        intent: DbtPublishIntent,
        policy: DbtPublishStrategyPolicy,
    ) -> tuple[str, ...]: ...


class DbtPublishProfileRegistryPort(Protocol):
    """Provide trusted publish and workflow profiles by stable id."""

    def profile(self, name: str) -> DbtPublishProfile | None: ...

    def workflow(self, name: str) -> DbtWorkflowProfile | None: ...

    def strategy_policy(self, name: str) -> DbtPublishStrategyPolicy | None: ...


class DbtPublishProfileLoaderPort(Protocol):
    """Load the trusted profile registry selected for a dbt artifact."""

    def load(
        self,
        manifest_path: str | Path,
        explicit_path: str | Path | None = None,
    ) -> tuple[DbtPublishProfileRegistryPort | None, tuple[DbtPublishIssue, ...]]: ...


class DbtRouteCapabilityPort(Protocol):
    """Resolve one generated route through canonical capability authority."""

    def resolve(
        self,
        *,
        source: str,
        sink: str,
        strategy: str,
        transport: str | None,
        schema_evolution: str | None,
        airflow_runtime_mode: str | None,
        path: str,
    ) -> tuple[dict[str, Any] | None, tuple[DbtPublishIssue, ...]]: ...


class DbtSelectedGraphPolicyPort(Protocol):
    """Validate every manifest-preview workflow closure as one project."""

    def validate(
        self,
        manifest_path: str | Path,
        selected_unique_ids_by_workflow: Mapping[str, tuple[str, ...]],
    ) -> tuple[DbtPublishIssue, ...]: ...
