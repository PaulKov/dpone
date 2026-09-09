"""Compatibility re-exports for private readiness identity operations."""

from dpone.manifest.ci_shadow_readiness_pair_fs import (
    identity_is_current as idempotent,
)
from dpone.manifest.ci_shadow_readiness_pair_fs import (
    publish_identity as publish,
)
from dpone.manifest.ci_shadow_readiness_pair_fs import (
    require_identity,
)

__all__ = ("idempotent", "publish", "require_identity")
