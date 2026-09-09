"""OpenAPI 3.1 generation from the canonical Studio route registry."""

from __future__ import annotations

from typing import Any

from dpone.readiness.studio_api_routes import StudioRoute, studio_routes
from dpone.readiness.studio_openapi_components import studio_openapi_schemas
from dpone.readiness.studio_openapi_parameters import studio_query_parameters

_ERROR_STATUSES = (
    "400",
    "401",
    "403",
    "404",
    "405",
    "408",
    "413",
    "415",
    "422",
    "429",
    "500",
)
_RESPONSE_SCHEMAS = {
    "health": "HealthResponse",
    "openapi": "OpenApiDocument",
    "meta": "StudioMetaResponse",
    "capabilities": "CapabilityDiscoveryResponse",
    "recipes": "RecipeListResponse",
    "pipelines": "PipelineSummaryListResponse",
    "pipeline_explain": "PipelineExplainResponse",
    "draft_manifest": "ManifestDraftResponse",
    "plan": "PlanResponse",
    "static_check": "StaticCheckResponse",
}
_OPERATION_DOCS = {
    "health": ("Check Studio API health", "Return a dependency-free process health response."),
    "openapi": ("Read the Studio OpenAPI contract", "Return the contract for this server authentication profile."),
    "meta": ("Read Studio metadata", "Return API mode, UI availability, and capability snapshot identity."),
    "capabilities": (
        "Discover capabilities",
        "List connector, route, recipe, support, certification, and evidence axes.",
    ),
    "recipes": ("Discover recipes", "List recipe metadata already projected by dpone policy."),
    "pipelines": ("List pipelines", "Return bounded summaries for direct project pipeline sources."),
    "pipeline_explain": ("Explain one pipeline", "Return target-scoped Airflow and artifact diagnostics."),
    "draft_manifest": ("Validate a manifest draft", "Build and canonically validate an in-memory manifest draft."),
    "plan": ("Plan a manifest", "Calculate a credential-free dry-run execution plan."),
    "static_check": ("Check authoring statically", "Validate one pipeline or in-memory manifest without live I/O."),
}
_REQUEST_EXAMPLES = {
    "draft_manifest": {
        "source_type": "mssql",
        "sink_type": "clickhouse",
        "strategy": "incremental_merge",
        "source_table": "orders",
        "unique_key": "id",
    },
    "plan": {"manifest_path": "manifests/orders.batch.yaml", "selector": "dbo.orders"},
    "static_check": {"pipeline_ref": "orders_daily"},
}


def studio_openapi_document(*, token_required: bool = False) -> dict[str, Any]:
    paths: dict[str, dict[str, Any]] = {}
    for route in studio_routes():
        paths.setdefault(route.path, {})[route.method.lower()] = _operation(
            route,
            token_required=token_required,
        )
    security: list[dict[str, list[str]]] = [{"bearerAuth": []}] if token_required else []
    return {
        "openapi": "3.1.0",
        "info": {
            "title": "dpone Studio API",
            "version": "v1",
            "description": (
                "Local development adapter. Loopback requests may use the local-operator identity; "
                "non-loopback mode requires bearer authentication. Remote production deployment is not supported."
            ),
        },
        "paths": paths,
        "security": security,
        "x-dpone-auth-policy": {
            "loopback": "bearer_required" if token_required else "local_operator",
            "non_loopback": "bearer_required",
        },
        "components": {
            "securitySchemes": {
                "bearerAuth": {
                    "type": "http",
                    "scheme": "bearer",
                }
            },
            "schemas": studio_openapi_schemas(),
        },
    }


def _operation(route: StudioRoute, *, token_required: bool) -> dict[str, Any]:
    summary, description = _OPERATION_DOCS.get(
        route.operation_id,
        ("Deprecated compatibility operation", "Compatibility-only endpoint; do not add new clients."),
    )
    operation: dict[str, Any] = {
        "operationId": (
            f"{route.operation_id}_legacy_{_operation_suffix(route.path)}" if route.deprecated else route.operation_id
        ),
        "summary": summary,
        "description": description,
        "deprecated": route.deprecated,
        "security": [] if route.public or not token_required else [{"bearerAuth": []}],
        "responses": {
            "200": {
                "description": "Successful response",
                "content": {
                    "application/json": {
                        "schema": {
                            "$ref": (
                                f"#/components/schemas/{_RESPONSE_SCHEMAS.get(route.operation_id, 'ObjectResponse')}"
                            )
                        }
                    }
                },
            },
            **{
                status: {
                    "description": "Structured dpone error",
                    "content": {"application/json": {"schema": {"$ref": "#/components/schemas/ErrorEnvelope"}}},
                }
                for status in _ERROR_STATUSES
            },
        },
    }
    if route.request_schema is not None:
        media: dict[str, Any] = {
            "schema": {"$ref": f"#/components/schemas/{route.request_schema}"},
        }
        if route.operation_id in _REQUEST_EXAMPLES:
            media["example"] = _REQUEST_EXAMPLES[route.operation_id]
        operation["requestBody"] = {
            "required": True,
            "content": {"application/json": media},
        }
    if "{id}" in route.path:
        operation["parameters"] = [
            {
                "name": "id",
                "in": "path",
                "required": True,
                "schema": {
                    "type": "string",
                    "pattern": "^[A-Za-z0-9_.-]+$",
                },
            }
        ]
    query_parameters = studio_query_parameters(route.operation_id)
    if query_parameters:
        operation.setdefault("parameters", []).extend(query_parameters)
    if route.deprecated:
        operation["responses"]["200"]["headers"] = _deprecation_headers()
    return operation


def _operation_suffix(path: str) -> str:
    return "_".join(part for part in path.split("/") if part).replace("{id}", "id")


def _deprecation_headers() -> dict[str, Any]:
    return {
        "Deprecation": {
            "description": "Signals that this compatibility endpoint is deprecated.",
            "schema": {"type": "string", "const": "true"},
        },
        "Sunset": {
            "description": "Last supported date for the compatibility endpoint.",
            "schema": {
                "type": "string",
                "const": "Fri, 23 Jul 2027 00:00:00 GMT",
            },
        },
        "Link": {
            "description": "Points to the canonical Studio API contract.",
            "schema": {"type": "string"},
        },
    }


__all__ = ["studio_openapi_document"]
