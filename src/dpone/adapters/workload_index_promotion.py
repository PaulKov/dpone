"""Adapter facade for workload-index promotion composition."""

from __future__ import annotations

from dpone.adapters.project_authoring_lock import project_authoring_lock
from dpone.adapters.workload_index_baseline_store import ConfinedWorkloadIndexBaselineStore

__all__ = [
    "ConfinedWorkloadIndexBaselineStore",
    "project_authoring_lock",
]
