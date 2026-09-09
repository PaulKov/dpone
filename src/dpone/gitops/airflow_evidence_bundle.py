from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.gitops.airflow_correlation_evidence import correlate_airflow_evidence
from dpone.gitops.airflow_evidence_bundle_models import (
    AIRFLOW_EVIDENCE_BUNDLE_SOURCE,
    AirflowRunIdentity,
    AirflowRunIdentityError,
    GitOpsAirflowAttemptCorrelation,
    GitOpsAirflowEvidenceArtifact,
    GitOpsAirflowEvidenceBundleReport,
    GitOpsAirflowPodCorrelation,
    GitOpsIssue,
)


@dataclass(frozen=True, slots=True)
class GitOpsAirflowEvidenceArtifactInput:
    name: str
    path: str
    expected_kind: str
    required: bool
    content: str | None


@dataclass(frozen=True, slots=True)
class _NormalizedArtifact:
    artifact: GitOpsAirflowEvidenceArtifact
    payload: Mapping[str, Any] | None
    warnings: tuple[GitOpsIssue, ...]
    blockers: tuple[GitOpsIssue, ...]


class GitOpsAirflowEvidenceBundleCollector:
    """Builds a single release-review evidence bundle from already-emitted artifacts."""

    def collect(
        self,
        *,
        dag_id: str,
        task_id: str,
        run_id: str,
        try_number: int,
        map_index: int,
        runner_policy: str,
        artifacts: tuple[GitOpsAirflowEvidenceArtifactInput, ...],
        pod_name: str | None = None,
        pod_uid: str | None = None,
        warnings: tuple[GitOpsIssue, ...] = (),
        blockers: tuple[GitOpsIssue, ...] = (),
    ) -> GitOpsAirflowEvidenceBundleReport:
        normalized = tuple(_normalize_artifact(artifact) for artifact in artifacts)
        payloads = {item.artifact.name: item.payload for item in normalized if item.payload is not None}
        pod = _build_pod_correlation(payloads=payloads, pod_name=pod_name, pod_uid=pod_uid)
        run_identity, identity_warnings, identity_blockers = _correlate_run_identity(
            payloads=payloads,
            runner_policy=runner_policy,
            dag_id=dag_id,
            image_digest=pod.image_digest,
        )
        attempt = GitOpsAirflowAttemptCorrelation(
            dag_id=dag_id,
            task_id=task_id,
            run_id=run_id,
            try_number=try_number,
            map_index=map_index,
        )
        correlation = correlate_airflow_evidence(
            run_identity=run_identity,
            attempt=attempt.to_jsonable(),
            pod={
                "name": pod.pod_name,
                "uid": pod.pod_uid,
                "namespace": pod.namespace,
                "image_digest": pod.image_digest,
            },
            payloads=payloads,
            artifact_sha256={
                item.artifact.name: item.artifact.sha256 for item in normalized if item.artifact.sha256 is not None
            },
            runner_policy=runner_policy,
        )
        all_warnings = (
            *warnings,
            *(warning for item in normalized for warning in item.warnings),
            *identity_warnings,
            *correlation.warnings,
        )
        all_blockers = (
            *blockers,
            *(blocker for item in normalized for blocker in item.blockers),
            *identity_blockers,
            *correlation.blockers,
        )
        return GitOpsAirflowEvidenceBundleReport(
            attempt=attempt,
            pod=pod,
            runner_policy=runner_policy,
            artifacts=tuple(item.artifact for item in normalized),
            run_identity=run_identity,
            correlation=correlation.correlation,
            warnings=all_warnings,
            blockers=all_blockers,
        )


def _correlate_run_identity(
    *,
    payloads: Mapping[str, Mapping[str, Any]],
    runner_policy: str,
    dag_id: str,
    image_digest: str | None,
) -> tuple[AirflowRunIdentity | None, tuple[GitOpsIssue, ...], tuple[GitOpsIssue, ...]]:
    observations: list[tuple[str, AirflowRunIdentity]] = []
    blockers: list[GitOpsIssue] = []
    for name, payload in payloads.items():
        raw = payload.get("run_identity")
        if raw is None:
            continue
        try:
            observations.append((name, AirflowRunIdentity.from_mapping(raw)))
        except AirflowRunIdentityError:
            blockers.append(
                _issue(
                    code="DPONE_AIRFLOW_RUN_IDENTITY_INVALID",
                    message="Evidence artifact contains an invalid or unsafe run identity",
                    path=name,
                )
            )
    if not observations:
        issue = _issue(
            code="DPONE_AIRFLOW_RUN_IDENTITY_MISSING",
            message="Airflow attempt evidence does not contain a composite dpone run identity",
            path="xcom_summary.run_identity",
        )
        return (
            None,
            (issue,) if runner_policy != "release" else (),
            (*blockers, issue) if runner_policy == "release" else tuple(blockers),
        )
    identity = observations[0][1]
    if any(item != identity for _, item in observations[1:]):
        blockers.append(
            _issue(
                code="DPONE_AIRFLOW_RUN_IDENTITY_MISMATCH",
                message="Evidence artifacts report different dpone run identities",
                path="run_identity",
            )
        )
    if image_digest is not None and identity.runtime_image_digest is not None:
        if image_digest != identity.runtime_image_digest:
            blockers.append(
                _issue(
                    code="DPONE_AIRFLOW_RUN_IDENTITY_MISMATCH",
                    message="Observed pod image digest does not match the dpone run identity",
                    path="pod.image_digest",
                )
            )
    if identity.dag_spec is not None and identity.dag_spec.id != dag_id:
        blockers.append(
            _issue(
                code="DPONE_AIRFLOW_RUN_IDENTITY_MISMATCH",
                message="Evidence attempt DAG id does not match the pinned DAG spec identity",
                path="attempt.dag_id",
            )
        )
    return identity, (), tuple(blockers)


def _normalize_artifact(raw: GitOpsAirflowEvidenceArtifactInput) -> _NormalizedArtifact:
    if raw.content is None:
        issue = _issue(
            code="airflow_evidence_artifact_missing" if raw.required else "airflow_evidence_optional_artifact_missing",
            message="Required evidence artifact is missing"
            if raw.required
            else "Optional evidence artifact is missing",
            path=raw.path,
        )
        artifact = GitOpsAirflowEvidenceArtifact(
            name=raw.name,
            path=raw.path,
            expected_kind=raw.expected_kind,
            actual_kind=None,
            required=raw.required,
            exists=False,
            sha256=None,
            bytes=None,
            passed=not raw.required,
            reason="missing",
        )
        return _NormalizedArtifact(
            artifact=artifact,
            payload=None,
            warnings=() if raw.required else (issue,),
            blockers=(issue,) if raw.required else (),
        )

    encoded = raw.content.encode("utf-8")
    sha256 = hashlib.sha256(encoded).hexdigest()
    bytes_count = len(encoded)
    try:
        parsed = json.loads(raw.content)
    except json.JSONDecodeError as exc:
        blocker = _issue(
            code="airflow_evidence_json_invalid",
            message=f"Evidence artifact JSON could not be parsed: {exc.msg}",
            path=raw.path,
        )
        return _NormalizedArtifact(
            artifact=GitOpsAirflowEvidenceArtifact(
                name=raw.name,
                path=raw.path,
                expected_kind=raw.expected_kind,
                actual_kind=None,
                required=raw.required,
                exists=True,
                sha256=sha256,
                bytes=bytes_count,
                passed=False,
                reason="invalid_json",
            ),
            payload=None,
            warnings=(),
            blockers=(blocker,),
        )

    payload = parsed if isinstance(parsed, Mapping) else {}
    actual_kind = _optional_string(payload.get("kind"))
    blockers: list[GitOpsIssue] = []
    warnings: list[GitOpsIssue] = []
    if actual_kind != raw.expected_kind:
        blockers.append(
            _issue(
                code="airflow_evidence_kind_mismatch",
                message=f"Evidence artifact kind must be {raw.expected_kind}",
                path=raw.path,
            )
        )
    blockers.extend(_child_blockers(raw.path, payload))
    warnings.extend(_child_warnings(raw.path, payload))
    child_failed = _child_failed(payload)
    if child_failed:
        blockers.append(
            _issue(
                code="airflow_evidence_child_failed",
                message="Evidence artifact reports a failed status",
                path=raw.path,
            )
        )

    passed = not blockers
    return _NormalizedArtifact(
        artifact=GitOpsAirflowEvidenceArtifact(
            name=raw.name,
            path=raw.path,
            expected_kind=raw.expected_kind,
            actual_kind=actual_kind,
            required=raw.required,
            exists=True,
            sha256=sha256,
            bytes=bytes_count,
            passed=passed,
            reason="passed" if passed else "blocked",
        ),
        payload=payload,
        warnings=tuple(warnings),
        blockers=tuple(blockers),
    )


def _child_blockers(path: str, payload: Mapping[str, Any]) -> tuple[GitOpsIssue, ...]:
    raw_blockers = payload.get("blockers")
    if not isinstance(raw_blockers, list) or not raw_blockers:
        return ()
    return (
        _issue(
            code="airflow_evidence_child_blocked",
            message="Evidence artifact contains blockers",
            path=path,
        ),
    )


def _child_warnings(path: str, payload: Mapping[str, Any]) -> tuple[GitOpsIssue, ...]:
    raw_warnings = payload.get("warnings")
    if not isinstance(raw_warnings, list) or not raw_warnings:
        return ()
    return (
        _issue(
            code="airflow_evidence_child_warning",
            message="Evidence artifact contains warnings",
            path=path,
        ),
    )


def _child_failed(payload: Mapping[str, Any]) -> bool:
    status = _optional_string(payload.get("status"))
    if status in {"failed", "blocked"}:
        return True
    passed = payload.get("passed")
    return passed is False


def _build_pod_correlation(
    *,
    payloads: Mapping[str, Mapping[str, Any]],
    pod_name: str | None,
    pod_uid: str | None,
) -> GitOpsAirflowPodCorrelation:
    pod_launch = payloads.get("pod_launch_evidence") or {}
    observed_pod = pod_launch.get("observed_pod")
    observed = observed_pod if isinstance(observed_pod, Mapping) else {}
    pod_contract = payloads.get("pod_contract") or {}
    runtime_profile = payloads.get("runtime_profile") or {}
    kpo_kwargs = pod_contract.get("kpo_kwargs")
    kpo = kpo_kwargs if isinstance(kpo_kwargs, Mapping) else {}
    return GitOpsAirflowPodCorrelation(
        pod_name=_first_string(pod_name, observed.get("pod_name"), pod_launch.get("pod_name"), kpo.get("name")),
        pod_uid=_first_string(pod_uid, observed.get("pod_uid"), observed.get("uid"), pod_launch.get("pod_uid")),
        namespace=_first_string(observed.get("namespace"), pod_launch.get("namespace"), pod_contract.get("namespace")),
        service_account=_first_string(
            pod_launch.get("service_account"),
            pod_contract.get("service_account"),
            runtime_profile.get("service_account"),
        ),
        image=_first_string(pod_launch.get("image"), pod_contract.get("image"), runtime_profile.get("image")),
        image_digest=_first_string(pod_launch.get("image_digest"), runtime_profile.get("image_digest")),
    )


def _first_string(*values: object) -> str | None:
    for value in values:
        if isinstance(value, str) and value:
            return value
    return None


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _issue(*, code: str, message: str, path: str) -> GitOpsIssue:
    return GitOpsIssue(code=code, message=message, path=path, source=AIRFLOW_EVIDENCE_BUNDLE_SOURCE)


__all__ = ["GitOpsAirflowEvidenceArtifactInput", "GitOpsAirflowEvidenceBundleCollector"]
