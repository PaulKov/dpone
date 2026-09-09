"""Shared CDC command-handler context resolution."""

from __future__ import annotations

from dpone.ops.service_catalog import OpsServiceCatalog, ReleaseOpsCatalog


def ops_catalog(ctx: object) -> ReleaseOpsCatalog:
    injected = getattr(ctx, "ops_services", None)
    if isinstance(injected, ReleaseOpsCatalog):
        return injected
    if isinstance(injected, OpsServiceCatalog):
        return injected.release
    return OpsServiceCatalog.default().release
