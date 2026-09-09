"""Shared data product governance export constants."""

from __future__ import annotations

SUPPORTED_GOVERNANCE_PROVIDERS = frozenset({"datahub", "openmetadata", "openlineage", "opa", "json"})

__all__ = ["SUPPORTED_GOVERNANCE_PROVIDERS"]
