"""Deprecated compiler imports delegated to the canonical composition root."""

from dpone.app import dbt_publish_composition as _composition

DbtDponeCompiler = _composition.LegacyDbtDponeCompiler
__all__ = _composition.LEGACY_COMPILER_EXPORTS
