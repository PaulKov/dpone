"""Deprecated planning imports delegated to the canonical composition root."""

from dpone.app import dbt_publish_composition as _composition

DbtPublishPlanner = _composition.LegacyDbtPublishPlanner
__all__ = _composition.LEGACY_PLANNING_EXPORTS
