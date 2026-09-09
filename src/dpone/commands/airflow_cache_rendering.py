"""Routing facade for human-readable Airflow cache command output."""

from __future__ import annotations

from collections.abc import Mapping

from dpone.commands.airflow_cache_recovery_rendering import (
    self_service_cache_recovery_apply_text,
    self_service_cache_recovery_plan_text,
)
from dpone.commands.airflow_cache_retention_rendering import (
    self_service_cache_retention_apply_text,
    self_service_cache_retention_plan_text,
)
from dpone.commands.airflow_cache_sync_rendering import self_service_cache_sync_text


def self_service_cache_text(payload: Mapping[str, object], *, command: str, target: str) -> str | None:
    """Return command-specific cache output text, or None for non-cache commands."""

    if command == "airflow_cache_recovery_plan":
        return self_service_cache_recovery_plan_text(payload, cache_root=target)
    if command == "airflow_cache_recovery_apply":
        return self_service_cache_recovery_apply_text(payload)
    if command == "airflow_cache_retention_plan":
        return self_service_cache_retention_plan_text(payload, cache_root=target)
    if command == "airflow_cache_retention_apply":
        return self_service_cache_retention_apply_text(payload)
    if command == "airflow_cache_sync":
        return self_service_cache_sync_text(payload)
    return None


__all__ = ["self_service_cache_text"]
