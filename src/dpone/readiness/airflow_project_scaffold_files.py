"""Desired file projection for a new Airflow self-service project."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from dpone.manifest.project_config import DEFAULT_DOMAIN_FIRST_ROOT, DEFAULT_SYSTEM_ROOT, PROJECT_CONFIG_PATH
from dpone.readiness.airflow_loader_migration import AIRFLOW_DAG_FILE
from dpone.readiness.airflow_scaffold_apply import ScaffoldFile
from dpone.readiness.airflow_self_service_templates import (
    airflow_loader_template,
    binding_set_payload,
    connection_registry_payload,
    credential_runtime_payload,
)


def project_scaffold_files(
    *,
    airflow: bool,
    requested_layout: str,
    normalized_layout: str | None,
) -> tuple[ScaffoldFile, ...]:
    """Return the complete create-only file set for one project layout."""

    config: dict[str, Any] = {
        "schema": "dpone.project.v1",
        "authoring": {"primary_source_policy": "one_per_pipeline"},
        "airflow": {"enabled": bool(airflow), "index_path": ".dpone-cache/current/airflow-index.json"},
    }
    if normalized_layout is not None:
        config["layout"] = {
            "mode": normalized_layout,
            "root": DEFAULT_DOMAIN_FIRST_ROOT,
            "pipeline_id_scope": "project",
        }
    files = [ScaffoldFile.yaml(Path(PROJECT_CONFIG_PATH), config)]
    if requested_layout == "domain_first":
        files.append(
            ScaffoldFile.yaml(
                Path(DEFAULT_SYSTEM_ROOT) / "config/project.yaml",
                {"gitops": {"includes": [{"path": "domains/*.yaml"}]}},
            )
        )
    if airflow:
        files.extend(
            (
                ScaffoldFile(Path(AIRFLOW_DAG_FILE), airflow_loader_template()),
                ScaffoldFile.yaml(Path("environments/dev/binding-set.yaml"), binding_set_payload("dev")),
                ScaffoldFile.yaml(
                    Path("environments/dev/credential-runtime.yaml"),
                    credential_runtime_payload("dev"),
                ),
                ScaffoldFile.yaml(
                    Path("platform/connection-registries/dev.yaml"),
                    connection_registry_payload("dev"),
                ),
            )
        )
    return tuple(files)


__all__ = ["project_scaffold_files"]
