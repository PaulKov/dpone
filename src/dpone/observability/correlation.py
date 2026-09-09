"""Bounded Airflow correlation loading and telemetry attribute projection."""

from __future__ import annotations

import json
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any

MAX_AIRFLOW_EVIDENCE_BUNDLE_BYTES = 2 * 1024 * 1024


def _symbol(path: str) -> Any:
    module_name, attr = path.split(":", 1)
    return getattr(import_module(module_name), attr)


@dataclass(frozen=True, slots=True)
class AirflowCorrelationLoadResult:
    correlation: Any | None
    blockers: tuple[str, ...] = ()


def load_airflow_correlation(path: str | Path | None) -> AirflowCorrelationLoadResult:
    """Load one complete correlation from a local immutable evidence bundle."""

    if path is None:
        return AirflowCorrelationLoadResult(correlation=None)
    bundle_path = Path(path)
    if bundle_path.is_symlink():
        return AirflowCorrelationLoadResult(correlation=None, blockers=("airflow_correlation.symlink_forbidden",))
    try:
        if not bundle_path.is_file():
            return AirflowCorrelationLoadResult(correlation=None, blockers=("airflow_correlation.missing",))
        if bundle_path.stat().st_size > MAX_AIRFLOW_EVIDENCE_BUNDLE_BYTES:
            return AirflowCorrelationLoadResult(correlation=None, blockers=("airflow_correlation.too_large",))
        payload = json.loads(bundle_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return AirflowCorrelationLoadResult(correlation=None, blockers=("airflow_correlation.invalid",))
    if not isinstance(payload, dict) or payload.get("kind") != "gitops.airflow_evidence_bundle":
        return AirflowCorrelationLoadResult(correlation=None, blockers=("airflow_correlation.invalid",))
    try:
        correlation = _symbol("dpone.contracts.airflow_correlation:parse_airflow_correlation")(
            payload.get("correlation")
        )
    except ValueError:
        return AirflowCorrelationLoadResult(correlation=None, blockers=("airflow_correlation.invalid",))
    if not correlation.complete:
        return AirflowCorrelationLoadResult(correlation=None, blockers=("airflow_correlation.incomplete",))
    return AirflowCorrelationLoadResult(correlation=correlation)


def otel_resource_attributes(correlation: Any) -> dict[str, str]:
    """Project stable artifact and physical runtime resource identity."""

    attributes = {
        "dpone.release.id": correlation.artifacts.release_id,
        "dpone.deployment.id": correlation.artifacts.deployment_id,
        "dpone.workload.id": correlation.artifacts.workload_id,
        "dpone.workload.pack.sha256": correlation.artifacts.workload_pack_sha256,
        "k8s.namespace.name": correlation.pod.namespace,
        "k8s.pod.name": correlation.pod.name,
        "k8s.pod.uid": correlation.pod.uid,
        "container.image.id": correlation.pod.image_digest,
    }
    return {key: value for key, value in attributes.items() if value is not None}


def otel_point_attributes(correlation: Any) -> dict[str, str]:
    """Project high-cardinality attempt identity for OTel points, never Prometheus."""

    attributes = {
        "dpone.correlation.id": correlation.correlation_id,
        "dpone.run.id": correlation.dpone.run_id,
        "dpone.process.name": correlation.dpone.process,
        "dpone.evidence.sha256": correlation.artifacts.runtime_evidence_sha256,
        "airflow.dag.id": correlation.airflow.dag_id,
        "airflow.task.id": correlation.airflow.task_id,
        "airflow.run.id": correlation.airflow.run_id,
        "airflow.task.try_number": str(correlation.airflow.try_number),
        "airflow.task.map_index": str(correlation.airflow.map_index),
    }
    return {key: value for key, value in attributes.items() if value is not None}


__all__ = [
    "AirflowCorrelationLoadResult",
    "MAX_AIRFLOW_EVIDENCE_BUNDLE_BYTES",
    "load_airflow_correlation",
    "otel_point_attributes",
    "otel_resource_attributes",
]
