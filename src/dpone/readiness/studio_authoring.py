"""Canonical draft, plan, and static-check services for Studio v1."""

from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

from dpone.readiness.airflow_authoring_check_service import AirflowAuthoringCheckService
from dpone.readiness.managed import ConnectorScaffoldService
from dpone.readiness.studio_errors import StudioError
from dpone.readiness.studio_manifest_validation import (
    load_manifest_path,
    load_manifest_yaml,
    validate_manifest_yaml,
)
from dpone.readiness.studio_planning import StudioManifestPlanningService
from dpone.readiness.studio_process_evidence import StudioProcessEvidencePolicy
from dpone.readiness.studio_project_paths import StudioProjectPathPolicy
from dpone.readiness.studio_route_validation import StudioRouteValidator


class StudioAuthoringService:
    """Validate every draft before exposing it as valid or plannable."""

    def __init__(
        self,
        *,
        root: Path,
        routes: StudioRouteValidator,
        authoring_check: AirflowAuthoringCheckService,
        scaffold: ConnectorScaffoldService,
        planning: StudioManifestPlanningService,
        paths: StudioProjectPathPolicy,
        evidence: StudioProcessEvidencePolicy,
    ) -> None:
        self._root = root.resolve(strict=True)
        self._routes = routes
        self._authoring_check = authoring_check
        self._scaffold = scaffold
        self._planning = planning
        self._paths = paths
        self._evidence = evidence

    def draft_manifest(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        _validate_object_keys(
            payload,
            allowed={
                "source_type",
                "sink_type",
                "strategy",
                "source_connection",
                "sink_connection",
                "source_schema",
                "source_table",
                "target_schema",
                "target_table",
                "unique_key",
                "quality_checks",
                "manifest_path",
            },
            required={"source_type", "sink_type", "strategy"},
        )
        source_type = required_text(payload, "source_type")
        sink_type = required_text(payload, "sink_type")
        strategy = required_text(payload, "strategy")
        self.require_supported_route(source_type, sink_type, strategy)
        unique_key = optional_text(payload.get("unique_key"))
        source_schema = required_text(payload, "source_schema", default="public")
        source_table = required_text(payload, "source_table", default="orders")
        manifest = self._scaffold.manifest_payload(
            source_type=source_type,
            sink_type=sink_type,
            source_connection=required_text(payload, "source_connection", default="source"),
            sink_connection=required_text(payload, "sink_connection", default="sink"),
            source_schema=source_schema,
            source_table=source_table,
            target_schema=required_text(payload, "target_schema", default="landing"),
            target_table=required_text(payload, "target_table", default=source_table),
            strategy=strategy,
            unique_key=unique_key,
        )
        configured_quality = payload.get("quality_checks")
        if configured_quality is None:
            quality_names = ["min_rows"]
        elif not isinstance(configured_quality, list) or not configured_quality:
            raise StudioError(
                "DPONE_STUDIO_REQUEST_SCHEMA_INVALID",
                "quality_checks must be a non-empty array when provided.",
            )
        else:
            quality_names = configured_quality
        quality_checks = quality_checks_from_names(quality_names, unique_key=unique_key or "id")
        quality_gates = manifest_quality_gates(quality_names)
        manifest["quality"] = {"gates": quality_gates}
        manifest_yaml = yaml.safe_dump(manifest, sort_keys=False, allow_unicode=True)
        validate_manifest_yaml(manifest_yaml)
        manifest_path = self.project_relative(
            required_text(
                payload,
                "manifest_path",
                default=f"manifests/{source_schema}_{source_table}.batch.yaml",
            )
        )
        return {
            "schema": "dpone.manifest-draft.v1",
            "valid": True,
            "status": "validated",
            "manifest_path": manifest_path,
            "manifest_yaml": manifest_yaml,
            "quality_checks": quality_checks,
            "quality_gates": quality_gates,
            "commands": self.gitops_prepare({"manifest_path": manifest_path})["commands"],
            "warnings": draft_warnings(
                source_type=source_type,
                sink_type=sink_type,
                strategy=strategy,
                unique_key=unique_key,
                gates=quality_gates,
            ),
        }

    def plan(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        return self._planning.plan(payload)

    def static_check(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        _validate_object_keys(
            payload,
            allowed={"pipeline_ref", "manifest_yaml"},
            required=set(),
        )
        pipeline_ref = optional_text(payload.get("pipeline_ref"))
        manifest_yaml = optional_text(payload.get("manifest_yaml"))
        if (pipeline_ref is None) == (manifest_yaml is None):
            raise StudioError(
                "DPONE_STUDIO_REQUEST_SCHEMA_INVALID",
                "Exactly one of pipeline_ref or manifest_yaml is required.",
            )
        if pipeline_ref is not None:
            checked = self._authoring_check.inspect(pipeline_ref)
            if checked.result.passed:
                manifest = load_manifest_path(checked.source_path)
                self._routes.validate_processes(manifest.processes)
                self._evidence.validate_processes(
                    manifest.processes,
                    manifest_dir=checked.source_path.parent,
                )
            return checked.result.to_dict()
        assert manifest_yaml is not None
        manifest = load_manifest_yaml(manifest_yaml)
        self._routes.validate_processes(manifest.processes)
        self._evidence.validate_processes(
            manifest.processes,
            manifest_dir=self._root,
        )
        return {
            "passed": True,
            "changes": [],
            "status": "validated",
            "errors": [],
            "network": False,
            "secrets": False,
            "source_queries": False,
        }

    def gitops_prepare(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        _validate_object_keys(
            payload,
            allowed={"manifest_path", "artifact_path", "branch"},
            required=set(),
        )
        manifest_path = self.project_relative(
            required_text(payload, "manifest_path", default="manifests/pipeline.batch.yaml")
        )
        artifact_path = self.project_relative(
            required_text(
                payload,
                "artifact_path",
                default="test_artifacts/plans/pipeline-plan.md",
            )
        )
        branch = required_text(payload, "branch", default=_branch_for(manifest_path))
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,127}", branch) or ".." in Path(branch).parts:
            raise StudioError(
                "DPONE_STUDIO_GIT_BRANCH_INVALID",
                "Git branch must be a bounded option-safe reference.",
            )
        return {
            "branch": branch,
            "manifest_path": manifest_path,
            "artifact_path": artifact_path,
            "commands": [
                {"argv": ["git", "switch", "-c", branch]},
                {
                    "argv": ["dpone", "plan", manifest_path, "--format", "md"],
                    "stdout_path": artifact_path,
                },
                {"argv": ["uv", "run", "pytest", "-m", "not integration_live", "-q"]},
                {"argv": ["git", "add", manifest_path, artifact_path]},
                {"argv": ["git", "commit", "-m", "Add dpone pipeline manifest"]},
                {"argv": ["gh", "pr", "create", "--fill", "--draft"]},
            ],
            "pull_request": {"draft": True, "provider": "github"},
        }

    def require_supported_route(self, source: str, sink: str, strategy: str) -> None:
        self._routes.require_supported_route(source, sink, strategy)

    def project_relative(self, value: str) -> str:
        return self._paths.relative(value)


def required_text(
    payload: Mapping[str, Any],
    key: str,
    *,
    default: str | None = None,
) -> str:
    value = payload.get(key, default)
    if not isinstance(value, str) or not value.strip():
        raise StudioError(
            "DPONE_STUDIO_REQUEST_SCHEMA_INVALID",
            f"{key} must be a non-empty string.",
        )
    return value.strip()


def optional_text(value: object) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _validate_object_keys(
    payload: Mapping[str, Any],
    *,
    allowed: set[str],
    required: set[str],
) -> None:
    unknown = sorted(set(payload) - allowed)
    missing = sorted(required - set(payload))
    if unknown or missing:
        raise StudioError(
            "DPONE_STUDIO_REQUEST_SCHEMA_INVALID",
            "Request fields do not match the Studio API schema.",
        )


def quality_checks_from_names(names: object, *, unique_key: str) -> list[dict[str, Any]]:
    result = []
    for name in quality_names(names):
        if name == "min_rows":
            result.append({"type": "min_rows", "value": 1, "mode": "warn"})
        elif name == "not_null":
            result.append({"type": "not_null", "column": unique_key, "mode": "fail"})
        elif name == "unique":
            result.append({"type": "unique", "columns": [unique_key], "mode": "fail"})
        elif name == "source_target_count":
            result.append({"type": "source_target_count", "mode": "fail"})
        else:
            _unsupported_quality()
    return result


def manifest_quality_gates(names: object) -> list[dict[str, Any]]:
    gates = []
    for name in quality_names(names):
        if name == "min_rows":
            gates.append(
                {
                    "id": "target_min_rows",
                    "type": "min_rows",
                    "side": "target",
                    "threshold": 1,
                    "severity": "warning",
                }
            )
        elif name == "source_target_count":
            gates.append(
                {
                    "id": "source_target_rows",
                    "type": "row_count_reconciliation",
                    "severity": "error",
                    "tolerance": {"mode": "pct", "value": 0},
                }
            )
        elif name in {"not_null", "unique"}:
            gates.append(
                {
                    "id": f"selected_key_{name}",
                    "type": name,
                    "side": "target",
                    "severity": "error",
                }
            )
        else:
            _unsupported_quality()
    return gates


def quality_names(value: object) -> tuple[str, ...]:
    values = value if isinstance(value, list) else [value]
    names = tuple(str(item).strip().lower() for item in values)
    if any(not name for name in names) or len(names) != len(set(names)):
        raise StudioError(
            "DPONE_STUDIO_QUALITY_CHECK_INVALID",
            "Quality checks must be non-empty and unique.",
        )
    return names


def draft_warnings(
    *,
    source_type: str,
    sink_type: str,
    strategy: str,
    unique_key: str | None,
    gates: list[dict[str, Any]],
) -> list[str]:
    warnings = []
    if strategy == "incremental_merge" and unique_key is None:
        warnings.append("incremental_merge should define unique_key before production use")
    if sink_type == "mssql":
        warnings.append("MSSQL production loads should verify ODBC Driver 18 and bcp availability")
    unavailable = sorted(str(gate["type"]) for gate in gates if gate["type"] in {"not_null", "unique"})
    if unavailable:
        warnings.append(
            f"Manifest runtime gates {', '.join(unavailable)} are declared but not yet executable and will fail closed."
        )
    if source_type == "api":
        warnings.append("API endpoint family uses the canonical rest connector provider.")
    return warnings


def _unsupported_quality() -> None:
    raise StudioError(
        "DPONE_STUDIO_QUALITY_CHECK_UNSUPPORTED",
        "Selected quality check is unsupported by manifest scaffolding.",
    )


def _branch_for(manifest_path: str) -> str:
    stem = Path(manifest_path).stem.replace(".", "-").replace("_", "-")
    return f"codex/add-{stem[:42]}"


__all__ = [
    "StudioAuthoringService",
    "optional_text",
    "required_text",
    "validate_manifest_yaml",
]
