"""Deprecated dbt artifact imports delegated to the canonical composition root."""

from dpone.app import dbt_publish_composition as _composition

DbtArtifactWriter = _composition.LegacyDbtArtifactWriter
DbtExecutionPackBuilder = _composition.LegacyDbtExecutionPackBuilder
__all__ = _composition.LEGACY_ARTIFACT_WRITER_EXPORTS
