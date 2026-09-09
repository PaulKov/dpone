"""Manifest filesystem boundary used by workload-index promotion composition."""

from __future__ import annotations

from dpone.manifest.confined_mutations import ConfinedMutationError, replace_file_if_digest
from dpone.manifest.project_root import inspect_project_root

__all__ = [
    "ConfinedMutationError",
    "inspect_project_root",
    "replace_file_if_digest",
]
