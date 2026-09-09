from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from dpone.gitops.models import GitOpsIssue

AIRFLOW_ARTIFACT_INDEX_SOURCE = "dpone gitops airflow artifact-index"
YamlLoader = Callable[[str], object]


@dataclass(frozen=True, slots=True)
class GitOpsAirflowArtifactIndexEntry:
    name: str
    path: str
    format: str
    expected_kind: str
    actual_kind: str | None
    schema_version: str | None
    producer: str | None
    required: bool
    exists: bool
    sha256: str | None
    bytes: int | None
    passed: bool
    reason: str

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "path": self.path,
            "format": self.format,
            "expected_kind": self.expected_kind,
            "actual_kind": self.actual_kind,
            "schema_version": self.schema_version,
            "producer": self.producer,
            "required": self.required,
            "exists": self.exists,
            "sha256": self.sha256,
            "bytes": self.bytes,
            "passed": self.passed,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class GitOpsAirflowArtifactIndexReport:
    artifact_dir: str
    output_path: str
    created_at: str
    entries: tuple[GitOpsAirflowArtifactIndexEntry, ...]
    warnings: tuple[GitOpsIssue, ...] = ()
    blockers: tuple[GitOpsIssue, ...] = ()
    kind: str = "gitops.airflow_artifact_index"
    schema_version: str = "1"
    producer: str = AIRFLOW_ARTIFACT_INDEX_SOURCE

    @property
    def passed(self) -> bool:
        return not self.blockers and all(entry.passed for entry in self.entries if entry.required)

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "schema_version": self.schema_version,
            "producer": self.producer,
            "artifact_dir": self.artifact_dir,
            "output_path": self.output_path,
            "created_at": self.created_at,
            "entries": [entry.to_jsonable() for entry in self.entries],
            "warnings": [warning.to_jsonable() for warning in self.warnings],
            "blockers": [blocker.to_jsonable() for blocker in self.blockers],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_jsonable(), ensure_ascii=False, indent=2) + "\n"


@dataclass(frozen=True, slots=True)
class GitOpsAirflowArtifactContent:
    spec: GitOpsAirflowArtifactSpec
    content: str | None


@dataclass(frozen=True, slots=True)
class GitOpsAirflowArtifactSpec:
    name: str
    filename: str
    format: str
    expected_kind: str
    required: bool


AIRFLOW_ARTIFACT_SPECS: tuple[GitOpsAirflowArtifactSpec, ...] = (
    GitOpsAirflowArtifactSpec("run_spec", "run-spec.json", "json", "gitops.airflow_run_spec", True),
    GitOpsAirflowArtifactSpec(
        "runtime_profile",
        "runtime-profile.json",
        "json",
        "gitops.airflow_runtime_profile",
        True,
    ),
    GitOpsAirflowArtifactSpec("pod_contract", "pod-contract.json", "json", "gitops.airflow_pod_contract", True),
    GitOpsAirflowArtifactSpec("pod_spec", "pod-spec.yaml", "yaml", "kubernetes.Pod", True),
    GitOpsAirflowArtifactSpec("kpo_kwargs", "kpo-kwargs.json", "json", "airflow.kpo_kwargs", True),
    GitOpsAirflowArtifactSpec("xcom_summary", "xcom-summary.json", "json", "gitops.airflow_xcom_summary", True),
    GitOpsAirflowArtifactSpec(
        "connection_bridge_plan",
        "connection-bridge-plan.json",
        "json",
        "gitops.airflow_connection_bridge_plan",
        False,
    ),
    GitOpsAirflowArtifactSpec(
        "cluster_doctor",
        "airflow-cluster-doctor.json",
        "json",
        "gitops.airflow_cluster_doctor",
        False,
    ),
    GitOpsAirflowArtifactSpec(
        "k8s_manifests_report",
        "airflow-k8s-manifests.json",
        "json",
        "gitops.airflow_k8s_manifests",
        False,
    ),
    GitOpsAirflowArtifactSpec(
        "k8s_manifests_yaml",
        "airflow-k8s-manifests.yaml",
        "yaml_multi",
        "kubernetes.ManifestSet",
        False,
    ),
    GitOpsAirflowArtifactSpec(
        "admission_check",
        "airflow-admission-check.json",
        "json",
        "gitops.airflow_admission_check",
        False,
    ),
    GitOpsAirflowArtifactSpec(
        "airflow_runtime_pack",
        "airflow-runtime-pack.json",
        "json",
        "gitops.airflow_pack",
        False,
    ),
    GitOpsAirflowArtifactSpec(
        "runtime_evidence",
        "runtime-evidence.json",
        "json",
        "gitops.airflow_runtime_evidence",
        False,
    ),
    GitOpsAirflowArtifactSpec("dag_factory", "airflow_dag_factory.py", "python", "airflow.dag_factory", False),
    GitOpsAirflowArtifactSpec("outcome_gate", "outcome_gate.py", "python", "airflow.outcome_gate", False),
)


class GitOpsAirflowArtifactIndexBuilder:
    """Build a deterministic inventory for generated Airflow runtime artifacts."""

    def build(
        self,
        *,
        artifact_dir: str,
        output_path: str,
        created_at: str,
        artifacts: tuple[GitOpsAirflowArtifactContent, ...],
        yaml_loader: YamlLoader,
    ) -> GitOpsAirflowArtifactIndexReport:
        normalized = tuple(
            _normalize_artifact(artifact_dir=artifact_dir, artifact=artifact, yaml_loader=yaml_loader)
            for artifact in artifacts
        )
        blockers = tuple(blocker for entry, blockers in normalized for blocker in blockers)
        return GitOpsAirflowArtifactIndexReport(
            artifact_dir=artifact_dir,
            output_path=output_path,
            created_at=created_at,
            entries=tuple(entry for entry, _ in normalized),
            blockers=blockers,
        )


def _normalize_artifact(
    *,
    artifact_dir: str,
    artifact: GitOpsAirflowArtifactContent,
    yaml_loader: YamlLoader,
) -> tuple[GitOpsAirflowArtifactIndexEntry, tuple[GitOpsIssue, ...]]:
    spec = artifact.spec
    path = _join_artifact_path(artifact_dir, spec.filename)
    if artifact.content is None:
        issue = _issue(
            code="airflow_artifact_missing",
            message="Required Airflow artifact is missing" if spec.required else "Optional Airflow artifact is missing",
            path=path,
        )
        return (
            _entry(spec=spec, path=path, exists=False, passed=not spec.required, reason="missing"),
            (issue,) if spec.required else (),
        )

    sha256 = hashlib.sha256(artifact.content.encode("utf-8")).hexdigest()
    bytes_count = len(artifact.content.encode("utf-8"))
    metadata, metadata_issue = _artifact_metadata(
        spec=spec,
        content=artifact.content,
        yaml_loader=yaml_loader,
        path=path,
    )
    passed = metadata_issue is None and metadata["actual_kind"] == spec.expected_kind
    blockers: tuple[GitOpsIssue, ...] = ()
    if metadata_issue is not None:
        blockers = (metadata_issue,)
    elif not passed:
        blockers = (
            _issue(
                code="airflow_artifact_kind_mismatch",
                message=f"Airflow artifact kind must be {spec.expected_kind}",
                path=path,
            ),
        )
    return (
        _entry(
            spec=spec,
            path=path,
            exists=True,
            sha256=sha256,
            bytes_count=bytes_count,
            actual_kind=metadata["actual_kind"],
            schema_version=metadata["schema_version"],
            producer=metadata["producer"],
            passed=not blockers,
            reason="passed" if not blockers else "blocked",
        ),
        blockers,
    )


def _artifact_metadata(
    *,
    spec: GitOpsAirflowArtifactSpec,
    content: str,
    yaml_loader: YamlLoader,
    path: str,
) -> tuple[dict[str, str | None], GitOpsIssue | None]:
    if spec.format == "json":
        return _json_metadata(spec=spec, content=content, path=path)
    if spec.format == "yaml":
        return _yaml_metadata(content=content, yaml_loader=yaml_loader, path=path)
    return {"actual_kind": spec.expected_kind, "schema_version": None, "producer": None}, None


def _json_metadata(
    *,
    spec: GitOpsAirflowArtifactSpec,
    content: str,
    path: str,
) -> tuple[dict[str, str | None], GitOpsIssue | None]:
    try:
        payload = json.loads(content)
    except json.JSONDecodeError as exc:
        return _empty_metadata(), _issue(
            code="airflow_artifact_json_invalid",
            message=f"Airflow JSON artifact could not be parsed: {exc.msg}",
            path=path,
        )
    data = payload if isinstance(payload, Mapping) else {}
    actual_kind: str | None
    if spec.name == "kpo_kwargs":
        actual_kind = spec.expected_kind
    else:
        actual_kind = _text(data.get("kind"))
    return {
        "actual_kind": actual_kind,
        "schema_version": _text(data.get("schema_version")),
        "producer": _text(data.get("producer")),
    }, None


def _yaml_metadata(
    *,
    content: str,
    yaml_loader: YamlLoader,
    path: str,
) -> tuple[dict[str, str | None], GitOpsIssue | None]:
    try:
        payload = yaml_loader(content)
    except Exception as exc:  # noqa: BLE001 - parser differences become contract blockers
        return _empty_metadata(), _issue(
            code="airflow_artifact_yaml_invalid",
            message=f"Airflow YAML artifact could not be parsed: {exc}",
            path=path,
        )
    data = payload if isinstance(payload, Mapping) else {}
    kind = _text(data.get("kind"))
    return {"actual_kind": f"kubernetes.{kind}" if kind else None, "schema_version": None, "producer": None}, None


def _entry(
    *,
    spec: GitOpsAirflowArtifactSpec,
    path: str,
    exists: bool,
    passed: bool,
    reason: str,
    sha256: str | None = None,
    bytes_count: int | None = None,
    actual_kind: str | None = None,
    schema_version: str | None = None,
    producer: str | None = None,
) -> GitOpsAirflowArtifactIndexEntry:
    return GitOpsAirflowArtifactIndexEntry(
        name=spec.name,
        path=path,
        format=spec.format,
        expected_kind=spec.expected_kind,
        actual_kind=actual_kind,
        schema_version=schema_version,
        producer=producer,
        required=spec.required,
        exists=exists,
        sha256=sha256,
        bytes=bytes_count,
        passed=passed,
        reason=reason,
    )


def _join_artifact_path(artifact_dir: str, filename: str) -> str:
    clean_dir = artifact_dir.rstrip("/")
    return f"{clean_dir}/{filename}" if clean_dir and clean_dir != "." else filename


def _empty_metadata() -> dict[str, str | None]:
    return {"actual_kind": None, "schema_version": None, "producer": None}


def _text(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _issue(*, code: str, message: str, path: str) -> GitOpsIssue:
    return GitOpsIssue(code=code, message=message, path=path, source=AIRFLOW_ARTIFACT_INDEX_SOURCE)


__all__ = [
    "AIRFLOW_ARTIFACT_SPECS",
    "AIRFLOW_ARTIFACT_INDEX_SOURCE",
    "GitOpsAirflowArtifactContent",
    "GitOpsAirflowArtifactIndexEntry",
    "GitOpsAirflowArtifactIndexReport",
    "GitOpsAirflowArtifactIndexBuilder",
    "GitOpsAirflowArtifactSpec",
]
