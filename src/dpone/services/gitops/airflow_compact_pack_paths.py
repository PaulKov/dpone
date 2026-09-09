"""Path helpers for compact Airflow pack / reconcile outputs."""

from __future__ import annotations

from pathlib import Path


def pack_output_path(args: object, workload_id: str) -> str:
    explicit = getattr(args, "output_path", None)
    if explicit:
        return str(explicit)
    output_dir = str(getattr(args, "output_dir", ".dpone/gitops") or ".dpone/gitops")
    return f"{output_dir.rstrip('/')}/airflow/{workload_id}/airflow-pack.json"


def pack_output_path_under(output_dir: Path, workload_id: str) -> str:
    return (output_dir / "airflow" / workload_id / "airflow-pack.json").as_posix()


def dag_spec_dir(output_dir: Path) -> str:
    return (output_dir / "airflow" / "_dags").as_posix()


def managed_artifact_root(output_dir: Path) -> str:
    return (output_dir / "airflow").as_posix()


__all__ = [
    "dag_spec_dir",
    "managed_artifact_root",
    "pack_output_path",
    "pack_output_path_under",
]
