"""Application facade behind the read-only dpone Studio API v1."""

from __future__ import annotations

import threading
import warnings
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.readiness.airflow_explain_service import AirflowExplainService
from dpone.readiness.capability_discovery_protocols import (
    CapabilitySnapshotProvider,
)
from dpone.readiness.managed import QualityService
from dpone.readiness.studio_authoring import StudioAuthoringService
from dpone.readiness.studio_errors import StudioError
from dpone.readiness.studio_http_models import (
    StudioApiError,
    StudioAuditLog,
    StudioHttpConfig,
)
from dpone.readiness.studio_legacy_operations import StudioLegacyOperations
from dpone.readiness.studio_pipeline_summary import (
    StudioPipelineCatalog,
    StudioPipelineCatalogError,
)
from dpone.readiness.studio_recovery import (
    explain_next_actions,
    structured_studio_errors,
)
from dpone.readiness.studio_ui_assets import StudioUiAssetsStatus

_LEGACY_WARNINGS_EMITTED: set[str] = set()
_LEGACY_WARNING_LOCK = threading.Lock()


class _StudioPythonCompatibilityMixin:
    """Deprecated direct-Python methods kept outside the v1 application API."""

    _legacy: StudioLegacyOperations

    def openapi(self) -> dict[str, Any]:
        _warn_legacy_python_api("StudioApiService.openapi()")
        from dpone.readiness.studio_openapi import studio_openapi_document

        return studio_openapi_document()

    def certification_matrix(self) -> dict[str, Any]:
        _warn_legacy_python_api("StudioApiService.certification_matrix()")
        return self._legacy.certification_matrix()

    def capabilities(self) -> dict[str, Any]:
        _warn_legacy_python_api("StudioApiService.capabilities()")
        return self._legacy.connection_capabilities()

    def record_audit(
        self,
        *,
        method: str,
        path: str,
        status: int,
        actor: str = "local_operator",
    ) -> None:
        _warn_legacy_python_api("StudioApiService.record_audit()")
        self._legacy.record_audit(
            method=method,
            path=path,
            status=status,
            actor=actor,
        )


class StudioApplicationService(_StudioPythonCompatibilityMixin):
    """Strict-DI facade shared by the HTTP adapter and contract tests."""

    version = "v1"

    def __init__(
        self,
        *,
        capability_snapshot_factory: CapabilitySnapshotProvider,
        pipelines: StudioPipelineCatalog,
        explain: AirflowExplainService,
        authoring: StudioAuthoringService,
        legacy: StudioLegacyOperations,
        quality: QualityService,
        ui_assets: StudioUiAssetsStatus,
    ) -> None:
        self._capabilities = capability_snapshot_factory
        self._pipelines = pipelines
        self._explain = explain
        self._authoring = authoring
        self._legacy = legacy
        self._quality = quality
        self._ui_assets = ui_assets

    def health(self) -> dict[str, Any]:
        return {"status": "ok", "service": "dpone-studio-api", "version": self.version}

    def meta(self) -> dict[str, Any]:
        capabilities = self._capabilities()
        return {
            "schema": "dpone.studio-meta.v1",
            "service": "dpone-studio-api",
            "version": self.version,
            "mode": "local_development_adapter",
            "read_only": True,
            **self._ui_assets.to_dict(),
            "snapshot_id": capabilities.snapshot_id,
        }

    def capability_snapshot(self) -> dict[str, Any]:
        return self._capabilities().to_dict()

    def recipe_list(self) -> dict[str, Any]:
        capabilities = self._capabilities()
        issues = [item.to_dict() for item in capabilities.issues]
        return {
            "schema": "dpone.recipe-discovery-list.v1",
            "passed": not issues,
            "snapshot_id": capabilities.snapshot_id,
            "recipes": [item.to_dict() for item in capabilities.recipes],
            "issues": issues,
        }

    def pipeline_list(self, *, limit: int = 200) -> dict[str, Any]:
        try:
            return self._pipelines.list(limit=limit)
        except StudioPipelineCatalogError as exc:
            raise StudioError(
                exc.code,
                exc.public_message,
                stage="studio_pipeline_catalog",
            ) from None

    def pipeline_explain(self, pipeline_id: str) -> dict[str, Any]:
        result = self._explain.explain(pipeline_id)
        payload = result.to_dict()
        return {
            **payload,
            "errors": structured_studio_errors(payload.get("errors")),
            "next_actions": explain_next_actions(pipeline_id, payload),
        }

    def draft_manifest(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        return self._authoring.draft_manifest(payload)

    def plan(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        return self._authoring.plan(payload)

    def static_check(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        result = self._authoring.static_check(payload)
        return {
            **result,
            "errors": structured_studio_errors(result.get("errors")),
        }

    def quality_check(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        if set(payload) != {"rows", "checks"}:
            raise StudioError(
                "DPONE_STUDIO_QUALITY_INPUT_INVALID",
                "Quality input must contain exactly rows and checks.",
            )
        rows = payload.get("rows")
        checks = payload.get("checks")
        if (
            not isinstance(rows, list)
            or not isinstance(checks, list)
            or any(not isinstance(item, Mapping) for item in rows)
            or any(not isinstance(item, Mapping) for item in checks)
        ):
            raise StudioError(
                "DPONE_STUDIO_QUALITY_INPUT_INVALID",
                "rows and checks must be JSON arrays of objects.",
            )
        return self._quality.run_checks(rows, checks).to_dict()

    def legacy_connection_capabilities(self) -> dict[str, Any]:
        return self._legacy.connection_capabilities()

    def legacy_certification_matrix(self) -> dict[str, Any]:
        return self._legacy.certification_matrix()

    def gitops_prepare(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        return self._authoring.gitops_prepare(payload)

    def state_inspect(self, query: Mapping[str, str]) -> dict[str, Any]:
        return self._legacy.state_inspect(query)

    def runs(self, *, limit: int = 200, cursor: int = 0) -> dict[str, Any]:
        return self._legacy.runs(limit=limit, cursor=cursor)

    def performance(self) -> dict[str, Any]:
        return self._legacy.performance()

    def doctor(self) -> dict[str, Any]:
        return self._legacy.doctor()

    def audit_events(self, *, limit: int = 100) -> dict[str, Any]:
        return self._legacy.audit_events(limit=limit)

    def security_policy(
        self,
        *,
        token_required: bool = False,
        remote_enabled: bool = False,
    ) -> dict[str, Any]:
        return self._legacy.security_policy(
            token_required=token_required,
            remote_enabled=remote_enabled,
        )

    def observability_slo(self) -> dict[str, Any]:
        return self._legacy.observability_slo()

    def deployment_guide(self) -> dict[str, Any]:
        return self._legacy.deployment_guide()

    def schema_explorer(self, query: dict[str, str]) -> dict[str, Any]:
        return self._legacy.schema_explorer(query)

    def reconciliation_preview(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        return self._legacy.reconciliation_preview(payload)


class StudioApiService:
    """Deprecated zero-argument Python facade retained for compatibility."""

    version = StudioApplicationService.version

    def __init__(
        self,
        *,
        artifact_roots: tuple[str, ...] = (".dpone/runs", "test_artifacts"),
    ) -> None:
        _warn_legacy_python_api("StudioApiService(artifact_roots=...)")
        from dpone.readiness.studio_composition import build_studio_api_service

        self._delegate = build_studio_api_service(
            root=Path.cwd(),
            artifact_roots=artifact_roots,
        )
        self.artifact_roots = artifact_roots

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)


def _warn_legacy_python_api(symbol: str) -> None:
    with _LEGACY_WARNING_LOCK:
        if symbol in _LEGACY_WARNINGS_EMITTED:
            return
        _LEGACY_WARNINGS_EMITTED.add(symbol)
    warnings.warn(
        f"{symbol} is deprecated; use build_studio_api_service() or the /api/v1 HTTP contract. "
        "Compatibility is retained through at least 2027-07-23.",
        DeprecationWarning,
        stacklevel=3,
    )


__all__ = [
    "StudioApiError",
    "StudioApplicationService",
    "StudioApiService",
    "StudioAuditLog",
    "StudioHttpConfig",
]
