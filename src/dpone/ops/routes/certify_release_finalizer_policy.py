"""Compatibility facade for route certification release finalization policy."""

from dpone.ops.routes.certify_release_finalizer_policy_impl import (
    RouteCertificationReleaseFinalizerDecision as RouteCertificationReleaseFinalizerDecision,
)
from dpone.ops.routes.certify_release_finalizer_policy_impl import (
    RouteCertificationReleaseFinalizerPolicy as RouteCertificationReleaseFinalizerPolicy,
)

__all__ = ["RouteCertificationReleaseFinalizerDecision", "RouteCertificationReleaseFinalizerPolicy"]
