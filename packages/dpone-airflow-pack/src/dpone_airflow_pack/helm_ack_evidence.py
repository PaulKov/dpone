"""Secret-free structural evidence for official Airflow Helm chart renders."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any

from dpone_airflow_pack.helm_ack_mounts import loader_ack_mount_summary


def build_helm_ack_evidence(
    documents: list[object],
    *,
    source_commit: str,
    helm_version: str,
    chart_name: str,
    chart_version: str,
    chart_sha256: str,
    pack_wheel_sha256: str,
    provider_wheel_sha256: str,
    values_sha256: str,
    renderer_sha256: str,
) -> dict[str, Any]:
    """Describe only resource kinds and ACK authority; never persist manifests."""

    resources = _resources(documents)
    kind_counts = Counter(str(item.get("kind", "<missing>")) for item in resources)
    projection = {
        "resource_kind_counts": {key: kind_counts[key] for key in sorted(kind_counts)},
        "ack_workloads": list(loader_ack_mount_summary(documents)),
    }
    commit = _required_commit(source_commit)
    chart = {
        "name": _required_text(chart_name, "chart name"),
        "version": _required_text(chart_version, "chart version"),
        "sha256": _required_sha256(chart_sha256),
    }
    package_artifacts = {
        "dpone_airflow_pack_wheel_sha256": _required_sha256(pack_wheel_sha256),
        "apache_airflow_provider_wheel_sha256": _required_sha256(provider_wheel_sha256),
    }
    inputs = {
        "source_commit": commit,
        "helm_version": _required_text(helm_version, "Helm version"),
        "chart": chart,
        "package_artifacts": package_artifacts,
        "values_sha256": _required_sha256(values_sha256),
        "renderer_sha256": _required_sha256(renderer_sha256),
        "rendered_structure_sha256": _structural_sha256(projection),
    }
    return {
        "schema": "dpone.internal.airflow-helm-ack-evidence.v1",
        **inputs,
        "subject_sha256": _structural_sha256(inputs),
        "resource_kind_counts": projection["resource_kind_counts"],
        "rendered_secret_document_count": kind_counts.get("Secret", 0),
        "ack_workloads": projection["ack_workloads"],
    }


def _resources(documents: Sequence[object]) -> tuple[Mapping[str, object], ...]:
    resources: list[Mapping[str, object]] = []
    for document in documents:
        if not isinstance(document, Mapping):
            continue
        if document.get("kind") == "List":
            items = document.get("items")
            if not isinstance(items, list):
                raise ValueError("Kubernetes List items must be a list")
            resources.extend(_resources(items))
        else:
            resources.append(document)
    return tuple(resources)


def _required_text(value: str, label: str) -> str:
    if not value.strip() or len(value) > 256:
        raise ValueError(f"{label} must be non-empty and bounded")
    return value.strip()


def _required_commit(value: str) -> str:
    normalized = value.strip()
    if len(normalized) != 40 or any(character not in "0123456789abcdef" for character in normalized):
        raise ValueError("source commit must be a full lowercase 40-character Git SHA")
    return normalized


def _required_sha256(value: str) -> str:
    normalized = value.removeprefix("sha256:")
    if len(normalized) != 64 or any(character not in "0123456789abcdef" for character in normalized):
        raise ValueError("chart sha256 must be a lowercase sha256 digest")
    return "sha256:" + normalized


def _structural_sha256(projection: Mapping[str, object]) -> str:
    encoded = json.dumps(projection, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


__all__ = ["build_helm_ack_evidence"]
