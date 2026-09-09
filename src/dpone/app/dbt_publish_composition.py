"""Composition root for the canonical dbt publishing build plane."""

from __future__ import annotations

import inspect
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from dpone.adapters.dbt_legacy_preview import (
    LegacyPreviewGraphPolicy,
    LegacyPreviewProjectPolicy,
)
from dpone.adapters.dbt_legacy_preview import (
    build_legacy_preview_execution_pack as _build_legacy_preview_execution_pack,
)
from dpone.adapters.dbt_publish_artifact_reader import DbtArtifactReader
from dpone.adapters.dbt_sqlserver_graph_policy import (
    DbtSqlserverPreviewGraphPolicyValidator,
)
from dpone.adapters.dbt_workflow_selection import (
    DbtCliSelectionResolver,
    ManifestPreviewSelectionResolver,
)
from dpone.app.dbt_promotion_composition import RuntimeDbtProjectBundleOperations, build_dbt_release_source_reader
from dpone.app.dbt_publish_legacy_preview import (
    LegacyPreviewRouteCapabilities as _LegacyPreviewRouteCapabilities,
)
from dpone.app.dbt_publish_legacy_preview import (
    LegacyRouteCapabilityAdapter as _LegacyRouteCapabilityAdapter,
)
from dpone.contracts.dbt_publish_models import (
    CompiledDbtModel,
    DbtModelArtifact,
    DbtPublishIntent,
    DbtPublishIssue,
    DbtPublishProfile,
    DbtPublishStrategyPolicy,
    fingerprint,
)
from dpone.manifest.dbt_publish_intent_resolver import DbtPublishIntentResolver
from dpone.manifest.dbt_publish_profiles import DbtPublishProfileRegistry
from dpone.ports.dbt_publishing import DbtProjectPolicyValidator
from dpone.readiness.capability_discovery_composition import (
    build_capability_discovery_service,
)
from dpone.readiness.dbt_airflow_execution_pack import DbtAirflowExecutionPackBuilder
from dpone.readiness.dbt_airflow_pack_adapter import DbtAirflowPackBuilder
from dpone.readiness.dbt_publish_atomic_publisher import DbtArtifactTreePublisher
from dpone.readiness.dbt_publish_capability_policy import DbtRouteCapabilityPolicy
from dpone.readiness.dbt_publish_release_materializer import DbtReleaseMaterializer
from dpone.readiness.dbt_sqlserver_project_policy import (
    DbtSqlserverProjectPolicyValidator,
)
from dpone.services.dbt_publish_artifact_writer import (
    DbtArtifactWriter as CanonicalDbtArtifactWriter,
)
from dpone.services.dbt_publish_compiler import (
    DbtArtifactReaderPort,
    DbtModelCompilerPort,
    DbtPublishIntentResolverPort,
    DbtPublishProfileLoaderPort,
    DbtPublishProfileRegistryPort,
    DbtRouteCapabilityPort,
    DbtSelectedGraphPolicyPort,
)
from dpone.services.dbt_publish_compiler import (
    DbtDponeCompiler as CanonicalDbtDponeCompiler,
)
from dpone.services.dbt_publish_model_compiler import DbtModelToWorkloadCompiler
from dpone.services.dbt_publish_planning import (
    DbtPublishPlanner as CanonicalDbtPublishPlanner,
)
from dpone.version import installed_version

if TYPE_CHECKING:
    from dpone.contracts.dbt_semantic_refresh_certification import (
        SemanticRefreshCertificationVerifierPort,
    )

_PREVIEW_STRATEGY_POLICY = DbtPublishStrategyPolicy(
    allowed_strategies=("incremental_merge", "partition_replace"),
)


class DbtPublishProfileLoader:
    """Bind the compiler port to the trusted YAML profile adapter."""

    def load(
        self,
        manifest_path: str | Path,
        explicit_path: str | Path | None = None,
    ) -> tuple[DbtPublishProfileRegistryPort | None, tuple[DbtPublishIssue, ...]]:
        return DbtPublishProfileRegistry.load(manifest_path, explicit_path)


class LegacyDbtArtifactWriter(CanonicalDbtArtifactWriter):
    """Historical constructor constrained to non-runnable local preview."""

    compatibility_mode = "local_preview"

    def __init__(self, **kwargs: Any) -> None:
        for name, value in legacy_preview_artifact_writer_defaults().items():
            kwargs.setdefault(name, value)
        super().__init__(**kwargs)


class LegacyDbtExecutionPackBuilder:
    """Historical execution-pack builder constrained to a preview artifact."""

    compatibility_mode = "local_preview"

    def build(self, workflow: Any) -> dict[str, Any]:
        return build_legacy_preview_execution_pack(workflow)


class LegacyDbtDponeCompiler(CanonicalDbtDponeCompiler):
    """Historical compiler constructor backed by canonical preview adapters."""

    compatibility_mode = "local_preview"

    def __init__(
        self,
        *,
        reader: Any | None = None,
        resolver: Any | None = None,
        model_compiler: Any | None = None,
        profile_loader: Any | None = None,
        route_capabilities: Any | None = None,
        project_policy: Any | None = None,
        graph_policy: Any | None = None,
    ) -> None:
        dependencies = legacy_preview_compiler_dependencies(
            reader=reader,
            resolver=resolver,
            model_compiler=model_compiler,
            profile_loader=profile_loader,
            route_capabilities=route_capabilities,
            project_policy=project_policy,
            graph_policy=graph_policy,
        )
        super().__init__(
            reader=cast(
                DbtArtifactReaderPort,
                dependencies["reader"],
            ),
            resolver=cast(
                DbtPublishIntentResolverPort,
                dependencies["resolver"],
            ),
            model_compiler=cast(
                DbtModelCompilerPort,
                dependencies["model_compiler"],
            ),
            profile_loader=cast(
                DbtPublishProfileLoaderPort,
                dependencies["profile_loader"],
            ),
            route_capabilities=cast(
                DbtRouteCapabilityPort,
                dependencies["route_capabilities"],
            ),
            project_policy=cast(
                DbtProjectPolicyValidator,
                dependencies["project_policy"],
            ),
            graph_policy=cast(
                DbtSelectedGraphPolicyPort,
                dependencies["graph_policy"],
            ),
        )


class LegacyDbtPublishPlanner(CanonicalDbtPublishPlanner):
    """Historical two-argument strategy call with explicit preview policy."""

    compatibility_mode = "local_preview"

    def strategy(
        self,
        model: DbtModelArtifact,
        intent: DbtPublishIntent,
        policy: DbtPublishStrategyPolicy | None = None,
        *,
        supported_strategies: tuple[str, ...] | None = None,
    ) -> tuple[dict[str, Any], tuple[DbtPublishIssue, ...]]:
        return super().strategy(
            model,
            intent,
            policy or _PREVIEW_STRATEGY_POLICY,
            supported_strategies=supported_strategies,
        )


def build_dbt_dpone_compiler(
    *,
    root: Path | None = None,
    require_certified_routes: bool = False,
    reader: DbtArtifactReaderPort | None = None,
    profile_loader: DbtPublishProfileLoaderPort | None = None,
    graph_policy: DbtSelectedGraphPolicyPort | None = None,
    semantic_refresh_certification_coordinate_sha256: str | None = None,
    semantic_refresh_certification_verifier: SemanticRefreshCertificationVerifierPort | None = None,
    semantic_refresh_certification_verification_time: str | None = None,
) -> CanonicalDbtDponeCompiler:
    """Assemble concrete adapters around the injectable compiler service."""

    snapshot = build_capability_discovery_service(
        root=(root or Path.cwd()),
    ).snapshot()
    return CanonicalDbtDponeCompiler(
        reader=reader if reader is not None else DbtArtifactReader(),
        resolver=DbtPublishIntentResolver(),
        model_compiler=DbtModelToWorkloadCompiler(planner=CanonicalDbtPublishPlanner()),
        profile_loader=profile_loader if profile_loader is not None else DbtPublishProfileLoader(),
        route_capabilities=DbtRouteCapabilityPolicy(
            snapshot,
            require_certified=require_certified_routes,
        ),
        project_policy=DbtSqlserverProjectPolicyValidator(project_root=root),
        graph_policy=graph_policy if graph_policy is not None else DbtSqlserverPreviewGraphPolicyValidator(),
        require_certified_routes=require_certified_routes,
        semantic_refresh_certification_coordinate_sha256=(semantic_refresh_certification_coordinate_sha256),
        semantic_refresh_certification_verifier=semantic_refresh_certification_verifier,
        semantic_refresh_certification_verification_time=(semantic_refresh_certification_verification_time),
    )


def build_dbt_artifact_writer(
    *,
    dbt_profiles_dir: Path | None,
) -> CanonicalDbtArtifactWriter:
    """Assemble the build-plane writer with explicit adapters."""

    return CanonicalDbtArtifactWriter(
        pack_builder=DbtAirflowPackBuilder(),
        dbt_pack_builder=DbtAirflowExecutionPackBuilder(),
        publisher=DbtArtifactTreePublisher(),
        selection_resolver=DbtCliSelectionResolver(),
        bundle_operations=RuntimeDbtProjectBundleOperations(),
        project_policy=DbtSqlserverProjectPolicyValidator(),
        dbt_profiles_dir=dbt_profiles_dir,
        producer_version=installed_version(),
    )


def build_dbt_release_materializer() -> DbtReleaseMaterializer:
    """Build the immutable local release installer."""

    return DbtReleaseMaterializer(workspace_source_reader=build_dbt_release_source_reader())


def legacy_preview_artifact_writer_defaults() -> dict[str, object]:
    """Return adapters for the deprecated, non-runnable writer constructor."""

    return {
        "selection_resolver": ManifestPreviewSelectionResolver(),
        "bundle_operations": RuntimeDbtProjectBundleOperations(),
        "project_policy": LegacyPreviewProjectPolicy(),
    }


def legacy_preview_compiler_dependencies(
    *,
    reader: Any | None,
    resolver: Any | None,
    model_compiler: Any | None,
    profile_loader: Any | None,
    route_capabilities: Any | None,
    project_policy: Any | None,
    graph_policy: Any | None,
) -> dict[str, object]:
    """Compose the historical compiler shape with unverified preview authority."""

    compiler = model_compiler or DbtModelToWorkloadCompiler(planner=CanonicalDbtPublishPlanner())
    capabilities = (
        _LegacyRouteCapabilityAdapter(route_capabilities)
        if route_capabilities is not None
        else _LegacyPreviewRouteCapabilities(fingerprint=fingerprint, issue_factory=DbtPublishIssue)
    )
    return {
        "reader": reader or DbtArtifactReader(),
        "resolver": resolver or DbtPublishIntentResolver(),
        "model_compiler": _LegacyModelCompilerAdapter(compiler),
        "profile_loader": profile_loader or DbtPublishProfileLoader(),
        "route_capabilities": capabilities,
        "project_policy": project_policy or LegacyPreviewProjectPolicy(),
        "graph_policy": graph_policy or LegacyPreviewGraphPolicy(),
    }


def build_legacy_preview_execution_pack(workflow: Any) -> dict[str, Any]:
    """Build the deprecated preview through an explicitly injected hasher."""

    return _build_legacy_preview_execution_pack(
        workflow,
        fingerprint=fingerprint,
    )


class _LegacyModelCompilerAdapter:
    def __init__(self, delegate: Any) -> None:
        self._delegate = delegate

    def compile(
        self,
        model: DbtModelArtifact,
        intent: DbtPublishIntent,
        profile: DbtPublishProfile,
        strategy_policy: DbtPublishStrategyPolicy,
        *,
        supported_strategies: tuple[str, ...] | None = None,
    ) -> CompiledDbtModel:
        method = self._delegate.compile
        parameters = tuple(inspect.signature(method).parameters.values())
        accepts_policy = (
            any(
                parameter.kind in {inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD}
                for parameter in parameters
            )
            or len(parameters) >= 4
        )
        accepts_supported = any(
            parameter.kind is inspect.Parameter.VAR_KEYWORD or parameter.name == "supported_strategies"
            for parameter in parameters
        )
        if accepts_policy and accepts_supported:
            result = method(
                model,
                intent,
                profile,
                strategy_policy,
                supported_strategies=supported_strategies,
            )
        elif accepts_policy:
            result = method(model, intent, profile, strategy_policy)
        else:
            result = method(model, intent, profile)
        return cast(CompiledDbtModel, result)

    def strategy_candidates(
        self,
        model: DbtModelArtifact,
        intent: DbtPublishIntent,
        policy: DbtPublishStrategyPolicy,
    ) -> tuple[str, ...]:
        method = getattr(self._delegate, "strategy_candidates", None)
        if callable(method):
            return cast(tuple[str, ...], method(model, intent, policy))
        return CanonicalDbtPublishPlanner.strategy_candidates(
            model,
            intent,
            policy,
        )


__all__ = [
    "DbtPublishProfileLoader",
    "build_legacy_preview_execution_pack",
    "build_dbt_artifact_writer",
    "build_dbt_dpone_compiler",
    "build_dbt_release_materializer",
    "legacy_preview_artifact_writer_defaults",
    "legacy_preview_compiler_dependencies",
]

LEGACY_ARTIFACT_WRITER_EXPORTS = ["DbtArtifactWriter", "DbtExecutionPackBuilder"]
LEGACY_COMPILER_EXPORTS = ["DbtDponeCompiler"]
LEGACY_PLANNING_EXPORTS = ["DbtPublishPlanner"]
