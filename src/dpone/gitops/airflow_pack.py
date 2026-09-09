from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from dpone.gitops.airflow_artifact_index import GitOpsAirflowArtifactContent
from dpone.gitops.airflow_pack_models import (
    GitOpsAirflowPackArtifact,
    GitOpsAirflowPackReport,
    GitOpsAirflowPackStep,
    airflow_pack_issue,
)
from dpone.gitops.models import GitOpsIssue


@dataclass(frozen=True, slots=True)
class GitOpsAirflowPackPlanner:
    """Plan and verify the Airflow runtime artifact pack without executing heavy checks."""

    def plan(
        self,
        *,
        artifact_dir: str,
        output_path: str,
        bundle_path: str,
        image: str | None,
        image_digest: str | None,
        mode: str,
        runner_policy: str,
        include_live_gates: bool,
        artifacts: tuple[GitOpsAirflowArtifactContent, ...],
    ) -> GitOpsAirflowPackReport:
        normalized = tuple(_normalize_artifact(artifact_dir=artifact_dir, artifact=artifact) for artifact in artifacts)
        artifacts_view = tuple(artifact for artifact, _ in normalized)
        artifact_issues = tuple(issue for _, issue in normalized if issue is not None)
        missing_required = tuple(issue for issue in artifact_issues if issue.code == "airflow_pack_artifact_missing")
        invalid_required = tuple(issue for issue in artifact_issues if issue.code != "airflow_pack_artifact_missing")
        image_warning = _image_warning(image)
        blockers = (*missing_required, *invalid_required) if mode == "verify" else ()
        warnings = tuple(
            issue for issue in (*artifact_issues, image_warning) if issue is not None and issue not in blockers
        )
        steps = _build_steps(
            artifact_dir=artifact_dir,
            bundle_path=bundle_path,
            image=image or "<IMAGE>",
            image_digest=image_digest,
            runner_policy=runner_policy,
            include_live_gates=include_live_gates,
        )
        return GitOpsAirflowPackReport(
            artifact_dir=artifact_dir,
            output_path=output_path,
            bundle_path=bundle_path,
            image=image,
            image_digest=image_digest,
            mode=mode,
            runner_policy=runner_policy,
            include_live_gates=include_live_gates,
            artifacts=artifacts_view,
            steps=steps,
            next_actions=_next_actions(missing_required=missing_required, image_missing=image is None),
            warnings=warnings,
            blockers=blockers,
        )


def _normalize_artifact(
    *,
    artifact_dir: str,
    artifact: GitOpsAirflowArtifactContent,
) -> tuple[GitOpsAirflowPackArtifact, GitOpsIssue | None]:
    spec = artifact.spec
    path = _join_artifact_path(artifact_dir, spec.filename)
    if artifact.content is None:
        issue = _missing_issue(path) if spec.required else None
        return _artifact(spec=spec, path=path, exists=False, passed=not spec.required, reason="missing"), issue

    digest = hashlib.sha256(artifact.content.encode("utf-8")).hexdigest()
    bytes_count = len(artifact.content.encode("utf-8"))
    actual_kind, issue = _actual_kind(
        spec_name=spec.name, expected_kind=spec.expected_kind, content=artifact.content, path=path
    )
    passed = issue is None and actual_kind == spec.expected_kind
    if issue is None and not passed:
        issue = airflow_pack_issue(
            code="airflow_pack_artifact_kind_mismatch",
            message=f"Airflow artifact kind must be {spec.expected_kind}",
            path=path,
        )
    return (
        _artifact(
            spec=spec,
            path=path,
            exists=True,
            passed=issue is None,
            reason="passed" if issue is None else "blocked",
            sha256=digest,
            bytes_count=bytes_count,
            actual_kind=actual_kind,
        ),
        issue if spec.required else None,
    )


def _actual_kind(
    *, spec_name: str, expected_kind: str, content: str, path: str
) -> tuple[str | None, GitOpsIssue | None]:
    if spec_name == "kpo_kwargs":
        return expected_kind, None
    if expected_kind.startswith("kubernetes.") or expected_kind.startswith("airflow."):
        return expected_kind, None
    try:
        payload = json.loads(content)
    except json.JSONDecodeError as exc:
        return None, airflow_pack_issue(
            code="airflow_pack_artifact_json_invalid",
            message=f"Airflow JSON artifact could not be parsed: {exc.msg}",
            path=path,
        )
    actual_kind = payload.get("kind") if isinstance(payload, dict) else None
    return actual_kind if isinstance(actual_kind, str) else None, None


def _build_steps(
    *,
    artifact_dir: str,
    bundle_path: str,
    image: str,
    image_digest: str | None,
    runner_policy: str,
    include_live_gates: bool,
) -> tuple[GitOpsAirflowPackStep, ...]:
    digest_arg = f" --image-digest {image_digest}" if image_digest else ""
    steps = [
        _step(
            "render",
            "render",
            f"dpone gitops airflow render {bundle_path} --output-dir {artifact_dir} --image {image}{digest_arg}",
        ),
        _step("run-spec", "runtime", f"dpone gitops airflow run-spec {bundle_path} --image {image}{digest_arg}"),
        _step(
            "runtime-profile",
            "runtime",
            f"dpone gitops airflow runtime-profile {bundle_path} --image {image}{digest_arg} --runner-policy {runner_policy}",
        ),
        _step("pod-contract", "runtime", f"dpone gitops airflow pod-contract {bundle_path}"),
        _step(
            "connection-bridge-plan",
            "support",
            f"dpone gitops airflow connection-bridge-plan --artifact-dir {artifact_dir}",
        ),
        _step("k8s-manifests", "support", f"dpone gitops airflow k8s-manifests --artifact-dir {artifact_dir}"),
        _step("artifact-index", "evidence", f"dpone gitops airflow artifact-index --artifact-dir {artifact_dir}"),
        _step(
            "preflight",
            "evidence",
            f"dpone gitops airflow preflight --artifact-dir {artifact_dir} --runner-policy {runner_policy}",
        ),
        _step(
            "cluster-doctor",
            "evidence",
            f"dpone gitops airflow cluster-doctor --artifact-dir {artifact_dir} --mode plan --runner-policy {runner_policy}",
        ),
        _step(
            "admission-check",
            "evidence",
            f"dpone gitops airflow admission-check --artifact-dir {artifact_dir} --mode plan --runner-policy {runner_policy}",
        ),
        _step("k8s-smoke", "evidence", f"dpone gitops airflow k8s-smoke --mode plan --runner-policy {runner_policy}"),
        _step("pod-watch", "evidence", f"dpone gitops airflow pod-watch --mode plan --runner-policy {runner_policy}"),
        _step(
            "evidence-bundle",
            "evidence",
            f"dpone gitops airflow evidence-bundle --bundle-path {bundle_path} --dag-id <DAG_ID> --task-id <TASK_ID> --run-id <RUN_ID> --try-number 1",
        ),
    ]
    if include_live_gates:
        steps.extend(_live_steps(artifact_dir=artifact_dir, runner_policy=runner_policy))
    return tuple(steps)


def _live_steps(*, artifact_dir: str, runner_policy: str) -> tuple[GitOpsAirflowPackStep, ...]:
    return (
        _step(
            "cluster-doctor-live",
            "live",
            f"dpone gitops airflow cluster-doctor --artifact-dir {artifact_dir} --mode live --runner-policy {runner_policy}",
            credential_required=True,
        ),
        _step(
            "k8s-smoke-live",
            "live",
            f"dpone gitops airflow k8s-smoke --mode live --runner-policy {runner_policy}",
            credential_required=True,
        ),
        _step(
            "pod-watch-live",
            "live",
            f"dpone gitops airflow pod-watch --mode live --runner-policy {runner_policy}",
            credential_required=True,
        ),
    )


def _step(
    name: str,
    phase: str,
    command: str,
    *,
    credential_required: bool = False,
) -> GitOpsAirflowPackStep:
    return GitOpsAirflowPackStep(
        name=name,
        phase=phase,
        command=command,
        required=True,
        credential_required=credential_required,
        produces=(),
        reason="golden_path",
    )


def _next_actions(*, missing_required: tuple[GitOpsIssue, ...], image_missing: bool) -> tuple[str, ...]:
    actions: list[str] = []
    if image_missing:
        actions.append("Pass --image with the custom dpone runtime image used by Airflow workers")
    if missing_required:
        actions.extend(
            [
                "Run dpone gitops airflow render to create the initial Airflow runner artifacts",
                "Run dpone gitops airflow pod-contract after runtime-profile changes",
                "Run dpone gitops airflow artifact-index after every artifact change",
                "Run dpone gitops airflow preflight --runner-policy release before merging",
            ]
        )
    return tuple(dict.fromkeys(actions))


def _artifact(
    *,
    spec: Any,
    path: str,
    exists: bool,
    passed: bool,
    reason: str,
    sha256: str | None = None,
    bytes_count: int | None = None,
    actual_kind: str | None = None,
) -> GitOpsAirflowPackArtifact:
    return GitOpsAirflowPackArtifact(
        name=spec.name,
        path=path,
        format=spec.format,
        expected_kind=spec.expected_kind,
        actual_kind=actual_kind,
        required=spec.required,
        exists=exists,
        passed=passed,
        reason=reason,
        sha256=sha256,
        bytes=bytes_count,
    )


def _missing_issue(path: str) -> GitOpsIssue:
    return airflow_pack_issue(
        code="airflow_pack_artifact_missing",
        message="Required Airflow runtime artifact is missing",
        path=path,
    )


def _image_warning(image: str | None) -> GitOpsIssue | None:
    if image:
        return None
    return airflow_pack_issue(
        code="airflow_pack_image_missing",
        message="Pack commands use <IMAGE> until --image is provided",
        path="--image",
    )


def _join_artifact_path(artifact_dir: str, filename: str) -> str:
    clean_dir = artifact_dir.rstrip("/")
    return f"{clean_dir}/{filename}" if clean_dir and clean_dir != "." else filename


__all__ = ["GitOpsAirflowPackPlanner"]
