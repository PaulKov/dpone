"""Author-owned scaffold payloads for one pinned external recipe."""

from __future__ import annotations

import copy
from collections.abc import Mapping, Sequence
from typing import Any


def recipe_pipeline_payload(
    pipeline_id: str,
    *,
    domain: str,
    recipe_block: Mapping[str, Any],
    source_path: str | None = None,
    airflow: bool = True,
) -> dict[str, Any]:
    """Return one flow primary source without generated process duplication."""

    return {
        "kind": "dpone.flow.v1",
        "authoring": {
            "mode": "flow",
            "source": source_path or f"pipelines/{pipeline_id}/pipeline.yaml",
        },
        "metadata": {
            "id": pipeline_id,
            "domain": domain,
            "tags": ["dpone", *(["airflow"] if airflow else [])],
            "airflow": airflow,
        },
        "recipe": copy.deepcopy(dict(recipe_block)),
    }


def recipe_domain_payload(
    pipeline_id: str,
    *,
    domain: str,
    connection_refs: Sequence[str],
    airflow: bool,
    start_date: str,
) -> dict[str, Any]:
    """Return the existing domain-catalog contract for an expanded recipe."""

    payload: dict[str, Any] = {
        "schema": "dpone.domain-catalog.v1",
        "domain": domain,
        "workloads": {
            pipeline_id: {
                "authoring_source": f"pipelines/{pipeline_id}/pipeline.yaml",
                "connection_refs": list(connection_refs),
                "tags": ["airflow"] if airflow else [],
            }
        },
    }
    if airflow:
        payload["dags"] = {
            pipeline_id: {
                "workloads": [pipeline_id],
                "schedule": None,
                "start_date": start_date,
                "catchup": False,
            }
        }
    return payload


__all__ = ["recipe_domain_payload", "recipe_pipeline_payload"]
