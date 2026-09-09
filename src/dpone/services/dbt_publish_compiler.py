"""Orchestrate dbt artifact reading, policy validation, and workflow grouping."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dpone.contracts.dbt_publish_models import (
    CompiledDbtModel,
    DbtCompileReport,
    DbtModelArtifact,
    DbtPublishIntent,
    DbtPublishIssue,
)
from dpone.contracts.dbt_toolchain import DBT_SQLSERVER_1_12_CERTIFIED
from dpone.ports.dbt_publish_compiler import (
    DbtArtifactReaderPort,
    DbtModelCompilerPort,
    DbtPublishIntentResolverPort,
    DbtPublishProfileLoaderPort,
    DbtRouteCapabilityPort,
    DbtSelectedGraphPolicyPort,
)
from dpone.ports.dbt_publish_compiler import (
    DbtPublishProfileRegistryPort as DbtPublishProfileRegistryPort,
)
from dpone.services.dbt_publish_compiler_support import (
    build_workflows,
    distinct_issues,
    identity_issues,
    model_issue,
    model_policy,
    one_certified_toolchain,
    select_models,
    semantic_refresh_capability,
    semantic_refresh_profile_issues,
    workflow_identifier_issue,
)

if TYPE_CHECKING:
    from dpone.contracts.dbt_semantic_refresh_certification import SemanticRefreshCertificationVerifierPort
    from dpone.ports.dbt_publishing import DbtProjectPolicyValidator


class DbtDponeCompiler:
    """Connector-neutral build-plane facade for ``check``, ``explain`` and ``compile``."""

    def __init__(
        self,
        *,
        reader: DbtArtifactReaderPort,
        resolver: DbtPublishIntentResolverPort,
        model_compiler: DbtModelCompilerPort,
        profile_loader: DbtPublishProfileLoaderPort,
        route_capabilities: DbtRouteCapabilityPort,
        project_policy: DbtProjectPolicyValidator,
        graph_policy: DbtSelectedGraphPolicyPort,
        require_certified_routes: bool = False,
        semantic_refresh_certification_coordinate_sha256: str | None = None,
        semantic_refresh_certification_verifier: SemanticRefreshCertificationVerifierPort | None = None,
        semantic_refresh_certification_verification_time: str | None = None,
    ) -> None:
        self._reader = reader
        self._resolver = resolver
        self._model_compiler = model_compiler
        self._profile_loader = profile_loader
        self._route_capabilities = route_capabilities
        self._project_policy = project_policy
        self._graph_policy = graph_policy
        self._require_certified_routes = require_certified_routes
        self._semantic_refresh_certification_coordinate_sha256 = semantic_refresh_certification_coordinate_sha256
        self._semantic_refresh_certification_verifier = semantic_refresh_certification_verifier
        self._semantic_refresh_certification_verification_time = semantic_refresh_certification_verification_time

    def build(
        self,
        manifest_path: str | Path,
        *,
        profiles_path: str | Path | None = None,
        require_contracts: bool = True,
        allow_empty: bool = False,
        model_selector: str | None = None,
    ) -> DbtCompileReport:
        artifact, artifact_issues = self._reader.read(manifest_path)
        if artifact is None:
            return DbtCompileReport(
                manifest_path=str(manifest_path),
                manifest_schema_version=None,
                blockers=artifact_issues,
            )
        blockers: list[DbtPublishIssue] = [issue for issue in artifact_issues if issue.severity == "error"]
        warnings: list[DbtPublishIssue] = [issue for issue in artifact_issues if issue.severity != "error"]
        project_issues = self._project_policy.validate_manifest(manifest_path)
        blockers.extend(issue for issue in project_issues if issue.severity == "error")
        warnings.extend(issue for issue in project_issues if issue.severity != "error")
        if project_issues:
            return DbtCompileReport(
                manifest_path=artifact.path,
                manifest_schema_version=artifact.schema_version,
                manifest_sha256=artifact.sha256,
                dbt_version=artifact.dbt_version,
                warnings=tuple(warnings),
                blockers=tuple(blockers),
            )
        selected, selection_issues = select_models(
            artifact.models,
            model_selector,
            path=artifact.path,
        )
        if selection_issues:
            return DbtCompileReport(
                manifest_path=artifact.path,
                manifest_schema_version=artifact.schema_version,
                manifest_sha256=artifact.sha256,
                dbt_version=artifact.dbt_version,
                warnings=tuple(warnings),
                blockers=(*blockers, *selection_issues),
            )
        resolved: list[tuple[DbtModelArtifact, DbtPublishIntent]] = []
        for model in selected:
            intent, issues = self._resolver.resolve(model)
            blockers.extend(issue for issue in issues if issue.severity == "error")
            warnings.extend(issue for issue in issues if issue.severity != "error")
            if intent is not None:
                workflow_issue = workflow_identifier_issue(
                    intent.workflow,
                    path=model.original_file_path,
                )
                if workflow_issue is not None:
                    blockers.append(workflow_issue)
                    continue
                resolved.append((model, intent))
        if not resolved:
            if not blockers and not allow_empty:
                blockers.append(
                    DbtPublishIssue(
                        code="DPONE_DBT_NO_PUBLISH_MODELS",
                        message="No publish-enabled dbt models were selected",
                        path=artifact.path,
                        remediation="Enable at least one model or use --allow-empty for a report-only check.",
                    )
                )
            return DbtCompileReport(
                manifest_path=artifact.path,
                manifest_schema_version=artifact.schema_version,
                manifest_sha256=artifact.sha256,
                dbt_version=artifact.dbt_version,
                warnings=tuple(warnings),
                blockers=tuple(blockers),
            )
        registry, registry_issues = self._profile_loader.load(manifest_path, profiles_path)
        if registry is None:
            return DbtCompileReport(
                manifest_path=artifact.path,
                manifest_schema_version=artifact.schema_version,
                manifest_sha256=artifact.sha256,
                dbt_version=artifact.dbt_version,
                warnings=tuple(warnings),
                blockers=(*blockers, *registry_issues),
            )
        selected_by_workflow = {
            workflow: tuple(model.unique_id for model, intent in resolved if intent.workflow == workflow)
            for workflow in sorted({intent.workflow for _model, intent in resolved})
        }
        semantic_workflows = {
            workflow
            for workflow in selected_by_workflow
            if any(
                intent.workflow == workflow
                and (profile := registry.profile(intent.profile)) is not None
                and profile.semantic_refresh is not None
                for _model, intent in resolved
            )
        }
        regular_selection = {
            workflow: selected
            for workflow, selected in selected_by_workflow.items()
            if workflow not in semantic_workflows
        }
        semantic_selection = {
            workflow: selected for workflow, selected in selected_by_workflow.items() if workflow in semantic_workflows
        }
        graph_issues = list(self._graph_policy.validate(artifact.path, regular_selection)) if regular_selection else []
        if semantic_selection:
            semantic_validator = getattr(self._graph_policy, "validate_semantic_refresh", None)
            if semantic_validator is None:
                graph_issues.append(
                    DbtPublishIssue(
                        code="DPONE_DBT_V2_GRAPH_POLICY_UNAVAILABLE",
                        message="The platform V2 graph proof adapter is unavailable",
                        path=artifact.path,
                        remediation="Use the certified semantic-refresh compiler composition.",
                    )
                )
            else:
                graph_issues.extend(
                    semantic_validator(
                        artifact.path,
                        semantic_selection,
                        require_target_independence=self._require_certified_routes,
                    )
                )
        graph_issues = list(distinct_issues(graph_issues))
        blockers.extend(issue for issue in graph_issues if issue.severity == "error")
        warnings.extend(issue for issue in graph_issues if issue.severity != "error")
        if graph_issues:
            return DbtCompileReport(
                manifest_path=artifact.path,
                manifest_schema_version=artifact.schema_version,
                manifest_sha256=artifact.sha256,
                dbt_version=artifact.dbt_version,
                warnings=tuple(warnings),
                blockers=tuple(blockers),
            )
        compiled: list[CompiledDbtModel] = []
        for model, intent in resolved:
            model_issues = model_policy(
                model,
                require_contracts=require_contracts,
            )
            blockers.extend(model_issues)
            profile = registry.profile(intent.profile)
            workflow_profile = registry.workflow(intent.workflow)
            strategy_policy = registry.strategy_policy(intent.profile)
            if profile is None:
                blockers.append(
                    model_issue(
                        model,
                        "DPONE_DBT_PROFILE_UNKNOWN",
                        f"Unknown publish profile: {intent.profile}",
                        remediation="Select a published profile or ask its platform owner to add one.",
                    )
                )
                continue
            if workflow_profile is None:
                blockers.append(
                    model_issue(
                        model,
                        "DPONE_DBT_WORKFLOW_UNKNOWN",
                        f"Unknown workflow profile: {intent.workflow}",
                        remediation="Select a published workflow or ask its platform owner to add one.",
                    )
                )
                continue
            if strategy_policy is None:
                blockers.append(
                    model_issue(
                        model,
                        "DPONE_DBT_PROFILE_UNKNOWN",
                        f"Publish profile has no strategy policy: {intent.profile}",
                        remediation="Ask the platform owner to publish a strategy policy for this profile.",
                    )
                )
                continue
            semantic_issues = semantic_refresh_profile_issues(model, profile)
            blockers.extend(issue for issue in semantic_issues if issue.severity == "error")
            warnings.extend(issue for issue in semantic_issues if issue.severity != "error")
            if any(issue.severity == "error" for issue in semantic_issues):
                continue
            if profile.semantic_refresh is not None:
                result = self._model_compiler.compile(
                    model,
                    intent,
                    profile,
                    strategy_policy,
                    supported_strategies=(),
                )
                result_blockers = tuple(item for item in result.warnings if item.severity == "error")
                blockers.extend(result_blockers)
                warnings.extend(item for item in result.warnings if item.severity != "error")
                semantic_capability, certification_issue = semantic_refresh_capability(
                    model=model,
                    manifest_sha256=artifact.sha256,
                    profile_sha256=profile.semantic_refresh.profile_sha256,
                    coordinate_sha256=self._semantic_refresh_certification_coordinate_sha256,
                    verifier=self._semantic_refresh_certification_verifier,
                    verification_time=self._semantic_refresh_certification_verification_time,
                )
                if certification_issue is not None:
                    if self._require_certified_routes:
                        blockers.append(certification_issue)
                    else:
                        warnings.append(replace(certification_issue, severity="warning"))
                if (
                    not model_issues
                    and not result_blockers
                    and (not self._require_certified_routes or semantic_capability["status"] == "CERTIFIED")
                ):
                    compiled.append(
                        replace(
                            result,
                            route_capability=semantic_capability,
                        )
                    )
                continue
            candidates = self._model_compiler.strategy_candidates(
                model,
                intent,
                strategy_policy,
            )
            capabilities: dict[str, dict[str, Any]] = {}
            candidate_issues: list[DbtPublishIssue] = []
            for candidate in candidates:
                capability, capability_issues = self._route_capabilities.resolve(
                    source=profile.source_type,
                    sink=profile.sink_type,
                    strategy=candidate,
                    transport=(profile.certification.transport if profile.certification else None),
                    schema_evolution=(profile.certification.schema_evolution if profile.certification else None),
                    airflow_runtime_mode=(
                        profile.certification.airflow_runtime_mode if profile.certification else None
                    ),
                    path=model.original_file_path,
                )
                if capability is not None and not capability_issues:
                    capabilities[candidate] = capability
                else:
                    candidate_issues.extend(capability_issues)
            result = self._model_compiler.compile(
                model,
                intent,
                profile,
                strategy_policy,
                supported_strategies=tuple(capabilities),
            )
            result_blockers = tuple(item for item in result.warnings if item.severity == "error")
            blockers.extend(result_blockers)
            warnings.extend(item for item in result.warnings if item.severity != "error")
            selected_mode = str(result.strategy.get("mode") or "")
            capability = capabilities.get(selected_mode)
            if capability is None:
                blockers.extend(distinct_issues(candidate_issues))
            if not model_issues and not result_blockers and capability is not None:
                compiled.append(replace(result, route_capability=capability))
        workflows, workflow_issues = build_workflows(
            tuple(compiled),
            registry,
        )
        blockers.extend(workflow_issues)
        blockers.extend(identity_issues(tuple(compiled), workflows))
        return DbtCompileReport(
            manifest_path=artifact.path,
            manifest_schema_version=artifact.schema_version,
            manifest_sha256=artifact.sha256,
            dbt_version=artifact.dbt_version,
            dbt_adapter=(DBT_SQLSERVER_1_12_CERTIFIED.adapter_name if one_certified_toolchain(compiled) else None),
            dbt_adapter_version=(
                DBT_SQLSERVER_1_12_CERTIFIED.adapter_version if one_certified_toolchain(compiled) else None
            ),
            models=tuple(sorted(compiled, key=lambda item: item.model.unique_id)),
            workflows=workflows if not workflow_issues else (),
            warnings=tuple(warnings),
            blockers=tuple(blockers),
        )


__all__ = ["DbtDponeCompiler"]
