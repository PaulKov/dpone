"""Compatibility facade for manifest validation helpers."""

from __future__ import annotations

from dpone.manifest.validation_engine import validate_manifest
from dpone.manifest.validation_models import Severity, ValidationIssue, ValidationProfile
from dpone.manifest.validation_profiles import _profile_from_manifest, get_profile
from dpone.manifest.validation_rules import (
    _extract_source_type,
    _validate_description_source_path,
    _validate_process,
    _validate_universal_process,
    has_errors,
)

__all__ = [
    "Severity",
    "ValidationIssue",
    "ValidationProfile",
    "get_profile",
    "_profile_from_manifest",
    "validate_manifest",
    "_validate_universal_process",
    "_extract_source_type",
    "_validate_process",
    "_validate_description_source_path",
    "has_errors",
]
