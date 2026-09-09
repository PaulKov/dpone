"""Deprecated non-runnable adapters for historical dbt preview APIs."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any


class LegacyPreviewProjectPolicy:
    """Preserve the historical synthetic-reader API without runtime authority."""

    compatibility_mode = "local_preview"

    def validate_manifest(
        self,
        manifest_path: str | Path,
    ) -> tuple[Any, ...]:
        del manifest_path
        return ()

    def validate_root(
        self,
        project_root: Path,
    ) -> tuple[Any, ...]:
        del project_root
        return ()


class LegacyPreviewGraphPolicy:
    """Preserve historical graph stubs only for unverified local preview."""

    compatibility_mode = "local_preview"

    def validate(
        self,
        manifest_path: str | Path,
        selected_unique_ids_by_workflow: Mapping[str, tuple[str, ...]],
    ) -> tuple[Any, ...]:
        del manifest_path, selected_unique_ids_by_workflow
        return ()


def build_legacy_preview_execution_pack(
    workflow: Any,
    *,
    fingerprint: Callable[[dict[str, Any]], str],
) -> dict[str, Any]:
    """Build the historical pack shape without granting runtime authority."""

    models = tuple(workflow.models)
    if not models:
        raise ValueError("Legacy dbt execution-pack preview requires at least one model")
    first = models[0]
    workflow_id = str(workflow.workflow)
    workload_id = f"dbt__{workflow_id}"
    task_id = f"{workload_id}__preview"
    runtime = dict(first.profile.runtime)
    payload: dict[str, Any] = {
        "kind": "gitops.airflow_pack",
        "schema_version": "3",
        "producer": "dpone.dbt_publish legacy preview",
        "compatibility": {
            "api": "dpone.dbt_publish",
            "status": "deprecated",
            "mode": "local_preview",
            "authority": "none",
        },
        "runtime_artifact_delivery": {"mode": "local_preview"},
        "airflow": {"execution": {}},
        "steps": [],
        "runtime_command": "",
        "kpo_kwargs": {
            "task_id": task_id,
            "name": task_id.replace("_", "-")[:63],
            "namespace": str(runtime.get("namespace") or "default"),
            "image": first.profile.runtime_image,
            "labels": {
                "app.kubernetes.io/component": "dbt-preview",
                "dpone.dev/workflow": workflow_id,
            },
        },
        "connection_projection": {},
        "xcom": {},
        "workload": {
            "workload_id": workload_id,
            "manifest": "target/manifest.json",
            "domain": first.model.group,
            "effective_config": {
                "authoring": {"mode": "dbt"},
                "compatibility": {"mode": "local_preview"},
            },
        },
    }
    payload["pack_fingerprint"] = fingerprint(payload)
    return payload


__all__ = [
    "LegacyPreviewGraphPolicy",
    "LegacyPreviewProjectPolicy",
    "build_legacy_preview_execution_pack",
]
