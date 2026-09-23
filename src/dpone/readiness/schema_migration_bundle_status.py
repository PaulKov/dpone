"""Compatibility facade for schema migration bundle status policy."""

from dpone.readiness.schema_migration_bundle_status_policy import (
    STATUS_CHECKS as STATUS_CHECKS,
)
from dpone.readiness.schema_migration_bundle_status_policy import (
    BundleStatusArtifact as BundleStatusArtifact,
)
from dpone.readiness.schema_migration_bundle_status_policy import (
    status_blockers as status_blockers,
)

__all__ = ["STATUS_CHECKS", "status_blockers"]
