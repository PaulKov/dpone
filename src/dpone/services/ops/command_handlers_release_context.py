from __future__ import annotations

from dpone.ops.service_catalog import OpsServiceCatalog, ReleaseOpsCatalog


def release_ops(ctx: object) -> ReleaseOpsCatalog:
    injected = getattr(ctx, "ops_services", None)
    if isinstance(injected, ReleaseOpsCatalog):
        return injected
    if isinstance(injected, OpsServiceCatalog):
        return injected.release
    return OpsServiceCatalog.default().release


__all__ = ["OpsServiceCatalog", "ReleaseOpsCatalog", "release_ops"]
