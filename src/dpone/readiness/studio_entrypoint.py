"""Stable application entrypoint used by the Studio CLI and HTTP adapter."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.readiness.managed import ConnectorScaffoldService, ManagedRenderer
from dpone.readiness.studio_api import StudioApplicationService
from dpone.readiness.studio_api_routes import StudioApiRouter, studio_routes
from dpone.readiness.studio_composition import build_studio_api_service
from dpone.readiness.studio_errors import StudioError
from dpone.readiness.studio_http_models import (
    StudioAuditLog,
    StudioHttpConfig,
)
from dpone.readiness.studio_openapi import studio_openapi_document
from dpone.readiness.studio_ui_assets import StudioUiAssetsStatus


def build_studio_router(
    service: StudioApplicationService,
    *,
    config: StudioHttpConfig,
    legacy_studio_metadata: Mapping[str, Any],
) -> StudioApiRouter:
    """Bind one application facade to transport-neutral route policy."""

    return StudioApiRouter(
        service,
        openapi=lambda: studio_openapi_document(token_required=bool(config.token)),
        token_required=bool(config.token),
        remote_enabled=config.allow_remote,
        legacy_studio_metadata=legacy_studio_metadata,
    )


def studio_metadata(
    *,
    host: str,
    port: int,
    config: StudioHttpConfig | None = None,
    ui_assets: StudioUiAssetsStatus | None = None,
) -> dict[str, Any]:
    """Build compatibility metadata projected by the Studio CLI."""

    endpoints = tuple(route.path for route in studio_routes() if not route.deprecated)
    legacy = ConnectorScaffoldService().studio_payload(
        host=host,
        port=port,
        ui_assets=ui_assets,
        endpoints=endpoints,
    )
    effective_config = config or StudioHttpConfig(host=host)
    return {
        **legacy,
        "schema": "dpone.studio-cli.v1",
        "host": host,
        "port": port,
        "auth": "shared_token" if effective_config.token else "local_only",
        "remote_enabled": effective_config.allow_remote,
        "endpoints": list(endpoints),
    }


def render_studio_metadata_markdown(payload: Mapping[str, Any]) -> str:
    return ManagedRenderer.render_markdown("dpone studio api", dict(payload))


__all__ = [
    "StudioError",
    "StudioApplicationService",
    "StudioAuditLog",
    "StudioHttpConfig",
    "build_studio_api_service",
    "build_studio_router",
    "render_studio_metadata_markdown",
    "studio_metadata",
]
