"""Composition-root adapter for an Airflow mapping item environment input."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.backfill.mapping import (
    AIRFLOW_MAPPING_ITEM_ENV,
    AIRFLOW_MAPPING_SELECTION_CONTEXT_KEY,
    parse_airflow_mapping_item_json,
)


class AirflowMappingContextService:
    """Translate provider environment input into a typed runtime context."""

    def from_environ(self, environ: Mapping[str, str]) -> dict[str, Any]:
        raw = environ.get(AIRFLOW_MAPPING_ITEM_ENV)
        if raw is None:
            return {}
        return {AIRFLOW_MAPPING_SELECTION_CONTEXT_KEY: parse_airflow_mapping_item_json(raw)}


__all__ = ["AIRFLOW_MAPPING_SELECTION_CONTEXT_KEY", "AirflowMappingContextService"]
