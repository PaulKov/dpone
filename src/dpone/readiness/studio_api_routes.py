"""Single route registry for Studio dispatch and OpenAPI generation."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote

from dpone.readiness.studio_api import StudioApplicationService
from dpone.readiness.studio_errors import StudioError


@dataclass(frozen=True, slots=True)
class StudioRoute:
    method: str
    path: str
    operation_id: str
    public: bool = False
    deprecated: bool = False
    request_schema: str | None = None


@dataclass(frozen=True, slots=True)
class StudioRouteMatch:
    route: StudioRoute
    parameters: Mapping[str, str]


_ROUTES = (
    StudioRoute("GET", "/healthz", "health", public=True),
    StudioRoute("GET", "/openapi.json", "openapi", public=True),
    StudioRoute("GET", "/api/v1/meta", "meta"),
    StudioRoute("GET", "/api/v1/capabilities", "capabilities"),
    StudioRoute("GET", "/api/v1/recipes", "recipes"),
    StudioRoute("GET", "/api/v1/pipelines", "pipelines"),
    StudioRoute("GET", "/api/v1/pipelines/{id}/explain", "pipeline_explain"),
    StudioRoute(
        "POST",
        "/api/v1/manifests/draft",
        "draft_manifest",
        request_schema="ManifestDraftRequest",
    ),
    StudioRoute("POST", "/api/v1/plans", "plan", request_schema="PlanRequest"),
    StudioRoute(
        "POST",
        "/api/v1/checks/static",
        "static_check",
        request_schema="StaticCheckRequest",
    ),
    StudioRoute("GET", "/api/studio", "legacy_studio", deprecated=True),
    StudioRoute("GET", "/api/connectors", "legacy_studio", deprecated=True),
    StudioRoute(
        "GET",
        "/api/connections/capabilities",
        "legacy_connection_capabilities",
        deprecated=True,
    ),
    StudioRoute("GET", "/api/doctor", "doctor", deprecated=True),
    StudioRoute("GET", "/api/perf", "performance", deprecated=True),
    StudioRoute("GET", "/api/state/inspect", "state_inspect", deprecated=True),
    StudioRoute("GET", "/api/runs", "runs", deprecated=True),
    StudioRoute(
        "GET",
        "/api/certification/matrix",
        "legacy_certification_matrix",
        deprecated=True,
    ),
    StudioRoute(
        "GET",
        "/api/security/policy",
        "security_policy",
        deprecated=True,
    ),
    StudioRoute("GET", "/api/audit/events", "audit_events", deprecated=True),
    StudioRoute(
        "GET",
        "/api/observability/slo",
        "observability_slo",
        deprecated=True,
    ),
    StudioRoute(
        "GET",
        "/api/deploy/guide",
        "deployment_guide",
        deprecated=True,
    ),
    StudioRoute(
        "GET",
        "/api/schema/explorer",
        "schema_explorer",
        deprecated=True,
    ),
    StudioRoute(
        "POST",
        "/api/manifests/draft",
        "draft_manifest",
        deprecated=True,
        request_schema="ManifestDraftRequest",
    ),
    StudioRoute(
        "POST",
        "/api/plan",
        "plan",
        deprecated=True,
        request_schema="PlanRequest",
    ),
    StudioRoute(
        "POST",
        "/api/quality/check",
        "quality_check",
        deprecated=True,
        request_schema="QualityCheckRequest",
    ),
    StudioRoute(
        "POST",
        "/api/gitops/prepare",
        "gitops_prepare",
        deprecated=True,
        request_schema="GitOpsPrepareRequest",
    ),
    StudioRoute(
        "POST",
        "/api/reconciliation/preview",
        "reconciliation_preview",
        deprecated=True,
        request_schema="ReconciliationPreviewRequest",
    ),
)


class StudioApiRouter:
    """Dispatch validated HTTP shapes to the injected application service."""

    def __init__(
        self,
        service: StudioApplicationService,
        *,
        openapi: Callable[[], dict[str, Any]],
        token_required: bool,
        remote_enabled: bool,
        legacy_studio_metadata: Mapping[str, Any],
    ) -> None:
        self._service = service
        self._openapi = openapi
        self._token_required = token_required
        self._remote_enabled = remote_enabled
        self._legacy_studio_metadata = dict(legacy_studio_metadata)

    @staticmethod
    def allowed_methods(path: str) -> tuple[str, ...]:
        return allowed_methods(path)

    @staticmethod
    def is_public(method: str, path: str) -> bool:
        return route_is_public(method, path)

    @staticmethod
    def is_deprecated(method: str, path: str) -> bool:
        return route_is_deprecated(method, path)

    @staticmethod
    def audit_path(method: str, path: str) -> str:
        match = match_route(method, path)
        return match.route.path if match is not None else "/<unmatched>"

    def dispatch(
        self,
        *,
        method: str,
        path: str,
        query: Mapping[str, str],
        body: Mapping[str, Any],
    ) -> dict[str, Any]:
        match = match_route(method, path)
        if match is None:
            allowed = allowed_methods(path)
            if allowed:
                raise StudioError(
                    "DPONE_STUDIO_METHOD_NOT_ALLOWED",
                    "HTTP method is not allowed for this Studio endpoint.",
                    stage="studio_http",
                )
            raise StudioError(
                "DPONE_STUDIO_ROUTE_NOT_FOUND",
                "Studio endpoint was not found.",
                stage="studio_http",
            )
        operation = match.route.operation_id
        if operation == "health":
            return self._service.health()
        if operation == "openapi":
            return self._openapi()
        if operation == "meta":
            return self._service.meta()
        if operation == "legacy_studio":
            return dict(self._legacy_studio_metadata)
        if operation == "capabilities":
            return self._service.capability_snapshot()
        if operation == "recipes":
            return self._service.recipe_list()
        if operation == "pipelines":
            return self._service.pipeline_list(limit=_query_int(query, "limit", 200))
        if operation == "pipeline_explain":
            return self._service.pipeline_explain(match.parameters["id"])
        if operation == "draft_manifest":
            return self._service.draft_manifest(body)
        if operation == "plan":
            return self._service.plan(body)
        if operation == "static_check":
            return self._service.static_check(body)
        if operation == "doctor":
            return self._service.doctor()
        if operation == "performance":
            return self._service.performance()
        if operation == "state_inspect":
            return self._service.state_inspect(query)
        if operation == "runs":
            return self._service.runs(
                limit=_query_int(query, "limit", 200),
                cursor=_query_offset(query, "cursor"),
            )
        if operation == "security_policy":
            return self._service.security_policy(
                token_required=self._token_required,
                remote_enabled=self._remote_enabled,
            )
        if operation == "audit_events":
            return self._service.audit_events(limit=_query_int(query, "limit", 100))
        if operation == "observability_slo":
            return self._service.observability_slo()
        if operation == "deployment_guide":
            return self._service.deployment_guide()
        if operation == "schema_explorer":
            return self._service.schema_explorer(dict(query))
        if operation == "quality_check":
            return self._service.quality_check(body)
        if operation == "legacy_connection_capabilities":
            return self._service.legacy_connection_capabilities()
        if operation == "legacy_certification_matrix":
            return self._service.legacy_certification_matrix()
        if operation == "gitops_prepare":
            return self._service.gitops_prepare(body)
        if operation == "reconciliation_preview":
            return self._service.reconciliation_preview(body)
        raise StudioError(
            "DPONE_STUDIO_ROUTE_NOT_IMPLEMENTED",
            "Studio route is registered but has no application operation.",
            stage="studio_http",
        )


def studio_routes() -> tuple[StudioRoute, ...]:
    return _ROUTES


def match_route(method: str, path: str) -> StudioRouteMatch | None:
    decoded = _decoded_path(path)
    for route in _ROUTES:
        if route.method != method:
            continue
        parameters = _match_path(route.path, decoded)
        if parameters is not None:
            return StudioRouteMatch(route=route, parameters=parameters)
    return None


def allowed_methods(path: str) -> tuple[str, ...]:
    decoded = _decoded_path(path)
    return tuple(sorted({route.method for route in _ROUTES if _match_path(route.path, decoded) is not None}))


def route_is_public(method: str, path: str) -> bool:
    match = match_route(method, path)
    return bool(match and match.route.public)


def route_is_deprecated(method: str, path: str) -> bool:
    match = match_route(method, path)
    return bool(match and match.route.deprecated)


def _decoded_path(path: str) -> str:
    decoded = unquote(path)
    if "\x00" in decoded or not decoded.startswith("/"):
        raise StudioError(
            "DPONE_STUDIO_ROUTE_INVALID",
            "Studio request path is invalid.",
            stage="studio_http",
        )
    return decoded.rstrip("/") or "/"


def _match_path(template: str, path: str) -> dict[str, str] | None:
    if "{id}" not in template:
        return {} if template == path else None
    prefix, suffix = template.split("{id}", maxsplit=1)
    pattern = f"^{re.escape(prefix)}(?P<id>[A-Za-z0-9_.-]+){re.escape(suffix)}$"
    match = re.fullmatch(pattern, path)
    return match.groupdict() if match else None


def _query_int(query: Mapping[str, str], key: str, default: int) -> int:
    raw = query.get(key)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise StudioError(
            "DPONE_STUDIO_PAGE_LIMIT_INVALID",
            "Pagination limit must be an integer between 1 and 200.",
        ) from exc


def _query_offset(query: Mapping[str, str], key: str) -> int:
    raw = query.get(key)
    if raw is None:
        return 0
    try:
        value = int(raw)
    except ValueError as exc:
        raise StudioError(
            "DPONE_STUDIO_PAGE_LIMIT_INVALID",
            "Pagination cursor must be a non-negative integer.",
        ) from exc
    if value < 0:
        raise StudioError(
            "DPONE_STUDIO_PAGE_LIMIT_INVALID",
            "Pagination cursor must be a non-negative integer.",
        )
    return value


__all__ = [
    "StudioApiRouter",
    "StudioRoute",
    "allowed_methods",
    "match_route",
    "route_is_deprecated",
    "route_is_public",
    "studio_routes",
]
