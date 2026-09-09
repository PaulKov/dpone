"""Compatibility re-exports for the pre-0.73.32 cache-retention API."""

from __future__ import annotations

from dpone.runtime.deployment_cache import (
    DeploymentCacheRetentionApplier,
    build_deployment_cache_retention_applier,
)

__all__ = ["DeploymentCacheRetentionApplier", "build_deployment_cache_retention_applier"]
