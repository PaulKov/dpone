"""Reusable fixture repository builders for Airflow dag-spec tests.

The helpers materialize a minimal GitOps workload-set layout on disk:

- ``gitops.yaml`` workload-set root with a ``domains/*.yaml`` include glob;
- one domain catalog with ``workloads``, ``workflow_groups`` and a ``dags:``
  block (the authoring surface introduced by the dag-spec contract);
- runnable single-process manifests (metadata-only parse friendly) and an
  optional ``dpone.batch.v1`` manifest.

Every builder/contract/CLI/e2e test reuses these helpers instead of
copy-pasting YAML bodies.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

DOMAIN_DIR = "dpone_workloads/gitops/domains"
MANIFEST_DIR = "workloads/marketing/dpone/manifests"


def write_workload_set(repo_root: Path) -> Path:
    """Write the workload-set root that includes ``domains/*.yaml``."""

    root = repo_root / "dpone_workloads/gitops"
    root.mkdir(parents=True, exist_ok=True)
    path = root / "gitops.yaml"
    path.write_text(
        yaml.safe_dump({"gitops": {"includes": [{"path": "domains/*.yaml"}]}}, sort_keys=False),
        encoding="utf-8",
    )
    return path


def write_domain(
    repo_root: Path,
    *,
    domain: str = "marketing",
    workloads: dict[str, str] | None = None,
    workflow_groups: dict[str, list[str]] | None = None,
    dags: dict[str, Any] | None = None,
) -> Path:
    """Write one domain catalog document with an optional ``dags:`` block."""

    payload: dict[str, Any] = {"domain": domain}
    if workflow_groups:
        payload["workflow_groups"] = {name: {"workload_ids": ids} for name, ids in workflow_groups.items()}
    payload["workloads"] = {workload_id: {"manifest": manifest} for workload_id, manifest in (workloads or {}).items()}
    if dags is not None:
        payload["dags"] = dags
    path = repo_root / DOMAIN_DIR / f"{domain}.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def write_manifest(
    repo_root: Path,
    name: str,
    *,
    depends_on: list[str] | None = None,
    task_group: str | None = None,
    outlets: list[Any] | None = None,
    inlets: list[Any] | None = None,
    source_schema: str = "public",
    source_table: str | None = None,
    sink_schema: str = "dst",
    sink_table: str | None = None,
) -> str:
    """Write a minimal single-process manifest; returns the repo-relative path."""

    table_name = source_table or name
    target_name = sink_table or name
    payload: dict[str, Any] = {
        "name": name,
        "source": {
            "type": "postgres",
            "connection_id": "pg_src",
            "table": {"schema": source_schema, "name": table_name},
        },
        "sink": {
            "type": "postgres",
            "connection_id": "pg_dst",
            "table": {"schema": sink_schema, "name": target_name},
            "mode": "append",
        },
    }
    if depends_on:
        payload["depends_on"] = list(depends_on)
    if task_group:
        payload["task_group"] = task_group
    if outlets or inlets:
        execution: dict[str, Any] = {}
        if outlets:
            execution["outlets"] = list(outlets)
        if inlets:
            execution["inlets"] = list(inlets)
        payload["gitops"] = {"airflow": {"execution": execution}}
    path = repo_root / MANIFEST_DIR / f"{name}.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path.relative_to(repo_root).as_posix()


def write_flow_manifest(
    repo_root: Path,
    name: str,
    *,
    depends_on: list[str] | None = None,
) -> str:
    """Write the one-process self-service flow equivalent of a manifest."""

    legacy_path = write_manifest(repo_root, name, depends_on=depends_on)
    path = repo_root / legacy_path
    process = yaml.safe_load(path.read_text(encoding="utf-8"))
    payload = {
        "kind": "dpone.flow.v1",
        "authoring": {"mode": "flow", "source": legacy_path},
        "metadata": {"id": name, "domain": "marketing", "airflow": True},
        "processes": [process],
    }
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return legacy_path


def write_batch_manifest(repo_root: Path, name: str = "pipeline") -> str:
    """Write a two-process ``dpone.batch.v1`` manifest with one internal edge."""

    body = {
        "kind": "dpone.batch.v1",
        "defaults": {
            "source": {
                "type": "postgres",
                "connection_id": "pg_src",
                "table": {"schema": "{{ src_schema }}", "name": "{{ src_table }}"},
            },
            "sink": {
                "type": "postgres",
                "connection_id": "pg_dst",
                "table": {"schema": "dst", "name": "{{ src_schema }}__{{ src_table }}"},
                "mode": "append",
            },
        },
        "naming": {"process_name": "{{ src_schema }}__{{ src_table }}"},
        "schemas": {
            "public": {
                "tables": [
                    {"table": "t1"},
                    {"table": "t2", "depends_on": ["#public.t1"]},
                ]
            }
        },
    }
    path = repo_root / MANIFEST_DIR / f"{name}.batch.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(body, sort_keys=False), encoding="utf-8")
    return path.relative_to(repo_root).as_posix()


def manifest_ref(manifest_repo_relative: str) -> str:
    """Convert a repo-relative manifest path into a domain-doc relative ref."""

    return "../../../" + manifest_repo_relative


def dag_declaration(**overrides: Any) -> dict[str, Any]:
    """A valid baseline ``dags:`` entry; tests override individual fields."""

    entry: dict[str, Any] = {
        "description": "governed sync",
        "schedule": None,
        "start_date": "2026-07-07",
        "timezone": "Europe/Moscow",
        "catchup": False,
        "max_active_runs": 1,
        "tags": ["dpone", "marketing"],
        "default_args": {"retries": 0, "retry_delay_minutes": 5},
        "operator_overrides": {"in_cluster": True},
        "workloads": ["marketing_app", "marketing_web"],
        "wiring": {"mode": "waves", "max_parallel_workloads": 2},
    }
    entry.update(overrides)
    return entry


def standard_repo(repo_root: Path, *, dags: dict[str, Any]) -> Path:
    """Materialize the default two-workload marketing repo; returns workload-set path."""

    app = write_manifest(repo_root, "app", outlets=["postgres://dst/app"])
    web = write_manifest(repo_root, "web")
    write_domain(
        repo_root,
        workloads={"marketing_app": manifest_ref(app), "marketing_web": manifest_ref(web)},
        workflow_groups={"wa": ["marketing_app", "marketing_web"]},
        dags=dags,
    )
    return write_workload_set(repo_root)


__all__ = [
    "DOMAIN_DIR",
    "MANIFEST_DIR",
    "dag_declaration",
    "manifest_ref",
    "standard_repo",
    "write_batch_manifest",
    "write_domain",
    "write_manifest",
    "write_workload_set",
]
