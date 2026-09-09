"""Closed workload-index validation and change-impact contracts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any

from dpone.contracts.workload_index import (
    MAX_PROJECT_WORKLOADS,
    WORKLOAD_INDEX_AIRFLOW_FIELDS,
    WORKLOAD_INDEX_DEPENDENCY_FIELDS,
    WORKLOAD_INDEX_FIELDS,
    WORKLOAD_INDEX_ITEM_FIELDS,
    WORKLOAD_INDEX_SCHEMA,
)
from dpone.manifest.project_discovery_identity import (
    is_canonical_sha256,
    normalized_domain_id,
    normalized_pipeline_id,
)
from dpone.manifest.project_discovery_identity import (
    project_fingerprint as compute_project_fingerprint,
)
from dpone.manifest.project_discovery_identity import (
    workload_fingerprint as compute_workload_fingerprint,
)
from dpone.manifest.project_discovery_models import (
    DiscoveredWorkload,
    ProjectDiscoveryProjectionError,
    ProjectDiscoverySnapshot,
)


@dataclass(frozen=True, slots=True)
class WorkloadIndexChangeImpact:
    """Project-wide semantic delta used by CI and retirement planning."""

    added: tuple[str, ...]
    modified: tuple[str, ...]
    removed: tuple[str, ...]

    def to_dict(self) -> dict[str, list[str]]:
        return {
            "added": list(self.added),
            "modified": list(self.modified),
            "removed": list(self.removed),
        }


def validate_workload_change_impact(payload: Mapping[str, Any]) -> None:
    """Validate the closed change-impact projection produced for CI approval."""

    expected_fields = {
        "schema",
        "baseline_fingerprint",
        "current_fingerprint",
        "baseline_content_sha256",
        "current_content_sha256",
        "current_source",
        "validation_status",
        "discovery_status",
        "issues",
        "added",
        "modified",
        "removed",
    }
    if set(payload) != expected_fields or payload.get("schema") != "dpone.workload-change-impact.v1":
        raise ProjectDiscoveryProjectionError("Workload change-impact fields or schema are invalid.")
    for field in ("baseline_fingerprint", "baseline_content_sha256", "current_content_sha256"):
        value = payload.get(field)
        if value is not None and not is_canonical_sha256(value):
            raise ProjectDiscoveryProjectionError(f"Workload change-impact {field} is invalid.")
    if not is_canonical_sha256(payload.get("current_fingerprint")):
        raise ProjectDiscoveryProjectionError("Workload change-impact current fingerprint is invalid.")
    if payload.get("current_source") not in {"candidate", "discovery"}:
        raise ProjectDiscoveryProjectionError("Workload change-impact source is invalid.")
    if payload.get("validation_status") != "passed":
        raise ProjectDiscoveryProjectionError("Workload change-impact validation status is invalid.")
    if payload.get("discovery_status") not in {"passed", "failed", "not_run"}:
        raise ProjectDiscoveryProjectionError("Workload change-impact discovery status is invalid.")
    issues = payload.get("issues")
    if not isinstance(issues, list) or any(not _valid_impact_issue(item) for item in issues):
        raise ProjectDiscoveryProjectionError("Workload change-impact issues are invalid.")
    for field in ("added", "modified", "removed"):
        values = payload.get(field)
        if (
            not isinstance(values, list)
            or any(normalized_pipeline_id(item) != item for item in values)
            or values != sorted(set(values))
        ):
            raise ProjectDiscoveryProjectionError(f"Workload change-impact {field} entries are invalid.")


def compare_workload_indexes(
    baseline: Mapping[str, Any] | ProjectDiscoverySnapshot,
    current: Mapping[str, Any] | ProjectDiscoverySnapshot,
) -> WorkloadIndexChangeImpact:
    """Compare self-verifying composite identities of closed workload entries."""

    before = _index_fingerprints(baseline)
    after = _index_fingerprints(current)
    return WorkloadIndexChangeImpact(
        added=tuple(sorted(set(after) - set(before))),
        modified=tuple(sorted(key for key in set(before) & set(after) if before[key] != after[key])),
        removed=tuple(sorted(set(before) - set(after))),
    )


def workload_index_from_snapshot(snapshot: ProjectDiscoverySnapshot) -> dict[str, Any]:
    """Serialize one successful domain-first snapshot into the closed CI contract."""

    if not snapshot.layout.is_domain_first:
        raise ProjectDiscoveryProjectionError("Workload indexes require a domain-first project snapshot.")
    if snapshot.issues:
        raise ProjectDiscoveryProjectionError("A failed discovery snapshot cannot emit a workload index.")
    payload = {
        "schema": WORKLOAD_INDEX_SCHEMA,
        "project_fingerprint": snapshot.project_fingerprint,
        "layout_mode": snapshot.layout.mode,
        "layout_root": snapshot.layout.root,
        "pipeline_id_scope": snapshot.layout.pipeline_id_scope,
        "workloads": [_workload_index_item(item) for item in snapshot.workloads],
    }
    validate_workload_index(payload)
    return payload


def validate_workload_index(value: Mapping[str, Any] | ProjectDiscoverySnapshot) -> None:
    """Validate the closed workload-index contract before it drives CI decisions."""

    payload = workload_index_from_snapshot(value) if isinstance(value, ProjectDiscoverySnapshot) else value
    if set(payload) != set(WORKLOAD_INDEX_FIELDS) or payload.get("schema") != WORKLOAD_INDEX_SCHEMA:
        raise ProjectDiscoveryProjectionError("Workload index fields or schema are invalid.")
    if not is_canonical_sha256(payload.get("project_fingerprint")):
        raise ProjectDiscoveryProjectionError("Workload index project fingerprint is invalid.")
    if payload.get("layout_mode") != "domain_first":
        raise ProjectDiscoveryProjectionError("Workload index layout mode is invalid.")
    layout_root = _require_project_path(payload.get("layout_root"), "layout root")
    if layout_root != layout_root.strip():
        raise ProjectDiscoveryProjectionError("Workload index layout root is invalid.")
    pipeline_id_scope = payload.get("pipeline_id_scope")
    if pipeline_id_scope != "project":
        raise ProjectDiscoveryProjectionError("Workload index pipeline id scope is invalid.")
    raw = payload.get("workloads")
    if not isinstance(raw, list):
        raise ProjectDiscoveryProjectionError("Workload index workloads must be a list.")
    if len(raw) > MAX_PROJECT_WORKLOADS:
        raise ProjectDiscoveryProjectionError("Workload index exceeds the project workload limit.")
    pipeline_ids = [_workload_pipeline_id(item) for item in raw]
    if pipeline_ids != sorted(pipeline_ids):
        raise ProjectDiscoveryProjectionError("Workload index workloads are not in canonical pipeline-id order.")
    seen: set[str] = set()
    for item in raw:
        _validate_workload_index_item(item, seen=seen, layout_root=layout_root)
        assert isinstance(item, Mapping)
        if item["workload_fingerprint"] != compute_workload_fingerprint(item):
            raise ProjectDiscoveryProjectionError("Workload index workload fingerprint does not match its entry.")
    expected_project_fingerprint = compute_project_fingerprint(
        layout_mode=str(payload["layout_mode"]),
        layout_root=layout_root,
        pipeline_id_scope=pipeline_id_scope,
        workloads=raw,
    )
    if payload["project_fingerprint"] != expected_project_fingerprint:
        raise ProjectDiscoveryProjectionError("Workload index project fingerprint does not match its entries.")


def _index_fingerprints(
    value: Mapping[str, Any] | ProjectDiscoverySnapshot,
) -> dict[str, str]:
    payload = workload_index_from_snapshot(value) if isinstance(value, ProjectDiscoverySnapshot) else value
    validate_workload_index(payload)
    raw = payload.get("workloads")
    assert isinstance(raw, list)
    result: dict[str, str] = {}
    for item in raw:
        assert isinstance(item, Mapping)
        pipeline_id = _optional_text(item.get("pipeline_id"))
        fingerprint = _optional_text(item.get("workload_fingerprint"))
        assert pipeline_id is not None and fingerprint is not None
        result[pipeline_id] = fingerprint
    return result


def _validate_workload_index_item(
    value: object,
    *,
    seen: set[str],
    layout_root: str,
) -> None:
    if not isinstance(value, Mapping) or set(value) != set(WORKLOAD_INDEX_ITEM_FIELDS):
        raise ProjectDiscoveryProjectionError("Workload index entry fields are invalid.")
    pipeline_id = _parse_pipeline_id(value.get("pipeline_id"))
    _parse_domain_id(value.get("domain"))
    if pipeline_id in seen:
        raise ProjectDiscoveryProjectionError("Workload index contains duplicate pipeline ids.")
    seen.add(pipeline_id)
    owner = value.get("owner")
    if owner is not None and (not isinstance(owner, str) or not owner or len(owner) > 256):
        raise ProjectDiscoveryProjectionError("Workload index owner is invalid.")
    authoring_source = _require_project_path(value.get("authoring_source"), "authoring source")
    expected_source = PurePosixPath(
        layout_root,
        _parse_domain_id(value.get("domain")),
        "pipelines",
        pipeline_id,
        "pipeline.yaml",
    ).as_posix()
    if authoring_source != expected_source:
        raise ProjectDiscoveryProjectionError("Workload index authoring source is not canonical.")
    for name in (
        "ownership_fingerprint",
        "source_sha256",
        "semantic_fingerprint",
        "workload_fingerprint",
    ):
        if not is_canonical_sha256(value.get(name)):
            raise ProjectDiscoveryProjectionError(f"Workload index {name} is invalid.")
    refs = value.get("connection_refs")
    if (
        not isinstance(refs, list)
        or any(not isinstance(item, str) or not item for item in refs)
        or len(refs) != len(set(refs))
        or refs != sorted(refs)
    ):
        raise ProjectDiscoveryProjectionError("Workload index connection refs are invalid.")
    dependencies = value.get("dependencies")
    if not isinstance(dependencies, list):
        raise ProjectDiscoveryProjectionError("Workload index dependencies are invalid.")
    dependency_order: list[tuple[str, str, str]] = []
    for dependency in dependencies:
        if not isinstance(dependency, Mapping) or set(dependency) != set(WORKLOAD_INDEX_DEPENDENCY_FIELDS):
            raise ProjectDiscoveryProjectionError("Workload index dependency fields are invalid.")
        kind = _require_text(dependency.get("kind"), "dependency kind")
        path = _require_project_path(dependency.get("path"), "dependency path")
        digest = dependency.get("sha256")
        if not is_canonical_sha256(digest):
            raise ProjectDiscoveryProjectionError("Workload index dependency digest is invalid.")
        assert isinstance(digest, str)
        dependency_order.append((kind, path, digest))
    if dependency_order != sorted(set(dependency_order)):
        raise ProjectDiscoveryProjectionError("Workload index dependencies are not canonical.")
    airflow = value.get("airflow")
    if not isinstance(airflow, Mapping) or set(airflow) != set(WORKLOAD_INDEX_AIRFLOW_FIELDS):
        raise ProjectDiscoveryProjectionError("Workload index Airflow projection is invalid.")
    if not isinstance(airflow.get("enabled"), bool):
        raise ProjectDiscoveryProjectionError("Workload index Airflow enabled flag is invalid.")
    if _require_text(airflow.get("dag_id"), "Airflow DAG id") != pipeline_id:
        raise ProjectDiscoveryProjectionError("Workload index Airflow DAG id is not canonical.")
    schedule = airflow.get("schedule")
    if schedule is not None and not isinstance(schedule, str):
        raise ProjectDiscoveryProjectionError("Workload index Airflow schedule is invalid.")


def _workload_pipeline_id(value: object) -> str:
    if not isinstance(value, Mapping):
        raise ProjectDiscoveryProjectionError("Workload index entry fields are invalid.")
    return _parse_pipeline_id(value.get("pipeline_id"))


def _workload_index_item(workload: DiscoveredWorkload) -> dict[str, Any]:
    material = workload.identity_material()
    material["workload_fingerprint"] = workload.workload_fingerprint
    return material


def _parse_pipeline_id(value: object) -> str:
    pipeline_id = normalized_pipeline_id(value)
    if pipeline_id is None:
        raise ProjectDiscoveryProjectionError("Workload index pipeline id is invalid.")
    return pipeline_id


def _parse_domain_id(value: object) -> str:
    domain_id = normalized_domain_id(value)
    if domain_id is None:
        raise ProjectDiscoveryProjectionError("Workload index domain id is invalid.")
    return domain_id


def _require_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ProjectDiscoveryProjectionError(f"Workload index {label} is invalid.")
    return value


def _require_project_path(value: object, label: str) -> str:
    path = _require_text(value, label)
    parsed = PurePosixPath(path)
    if (
        "\\" in path
        or "\x00" in path
        or parsed.is_absolute()
        or not parsed.parts
        or "." in parsed.parts
        or ".." in parsed.parts
        or path != parsed.as_posix()
    ):
        raise ProjectDiscoveryProjectionError(f"Workload index {label} is invalid.")
    return path


def _optional_text(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _valid_impact_issue(value: object) -> bool:
    return (
        isinstance(value, Mapping)
        and set(value) == {"code", "message", "path"}
        and isinstance(value.get("code"), str)
        and bool(value["code"])
        and isinstance(value.get("message"), str)
        and bool(value["message"])
        and (value.get("path") is None or isinstance(value.get("path"), str))
    )


__all__ = [
    "WORKLOAD_INDEX_FIELDS",
    "WORKLOAD_INDEX_ITEM_FIELDS",
    "WORKLOAD_INDEX_SCHEMA",
    "WorkloadIndexChangeImpact",
    "compare_workload_indexes",
    "validate_workload_change_impact",
    "validate_workload_index",
    "workload_index_from_snapshot",
]
