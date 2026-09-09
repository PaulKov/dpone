from __future__ import annotations

import hashlib
import json

from dpone.gitops.airflow_evidence_bundle import (
    GitOpsAirflowEvidenceArtifactInput,
    GitOpsAirflowEvidenceBundleCollector,
)


def _payload(kind: str, **extra: object) -> dict[str, object]:
    payload: dict[str, object] = {"kind": kind, "blockers": [], "warnings": []}
    payload.update(extra)
    return payload


def _run_identity() -> dict[str, object]:
    return {
        "schema": "dpone.airflow-run-identity.v1",
        "release_id": "sha256:" + "a" * 64,
        "deployment_id": "sha256:" + "b" * 64,
        "dag_spec": {"id": "dpone_gitops", "sha256": "sha256:" + "c" * 64},
        "workload_pack": {"id": "dpone_orders", "sha256": "sha256:" + "d" * 64},
        "runtime_image_digest": "sha256:" + "a" * 64,
        "binding_set_ref": "sha256:" + "1" * 64,
        "connection_registry_ref": "sha256:" + "2" * 64,
        "credential_runtime_ref": "sha256:" + "3" * 64,
        "airflow_bundle": {
            "backend": "git",
            "ref": "git:7ac31f2",
            "versioned": True,
            "version": "7ac31f2",
            "snapshot_ref": None,
        },
    }


def _artifact(
    name: str,
    path: str,
    kind: str,
    *,
    required: bool = True,
    payload: dict[str, object] | None = None,
) -> GitOpsAirflowEvidenceArtifactInput:
    content = json.dumps(payload or _payload(kind), ensure_ascii=False, sort_keys=True)
    return GitOpsAirflowEvidenceArtifactInput(
        name=name,
        path=path,
        expected_kind=kind,
        required=required,
        content=content,
    )


def _payload_sha(payload: dict[str, object]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def test_airflow_evidence_bundle_collector_builds_correlated_digest_report() -> None:
    pod_launch = _payload(
        "gitops.airflow_pod_launch_evidence",
        pod_name="dpone-gitops-runtime",
        namespace="dpone-runners",
        service_account="dpone-runner",
        image="ghcr.io/acme/dpone:0.13.0",
        image_digest="sha256:" + "a" * 64,
        observed_pod={
            "pod_name": "dpone-gitops-runtime",
            "uid": "pod-uid-123",
            "namespace": "dpone-runners",
            "phase": "Succeeded",
        },
    )
    runtime_profile = _payload(
        "gitops.airflow_runtime_profile",
        namespace="dpone-runners",
        service_account="dpone-runner",
        image="ghcr.io/acme/dpone:0.13.0",
        image_digest="sha256:" + "a" * 64,
    )
    runtime_evidence = _payload("gitops.airflow_runtime_evidence", status="passed")
    xcom_summary = _payload(
        "gitops.airflow_xcom_summary",
        run_identity=_run_identity(),
        runtime_evidence_sha256=_payload_sha(runtime_evidence),
        runtime_evidence={
            "schema_version": "dpone.airflow.inline_runtime_evidence.v1",
            "status": "passed",
            "dpone_run": {"run_id": "orders-run-1", "process": "orders"},
        },
    )

    report = GitOpsAirflowEvidenceBundleCollector().collect(
        dag_id="dpone_gitops",
        task_id="dpone_orders",
        run_id="manual__2026-06-16T10:00:00+00:00",
        try_number=2,
        map_index=-1,
        runner_policy="release",
        artifacts=(
            _artifact("bundle", ".dpone/gitops/bundle/bundle.json", "gitops.bundle"),
            _artifact("run_spec", ".dpone/gitops/airflow/run-spec.json", "gitops.airflow_run_spec"),
            _artifact(
                "runtime_profile",
                ".dpone/gitops/airflow/runtime-profile.json",
                "gitops.airflow_runtime_profile",
                payload=runtime_profile,
            ),
            _artifact("pod_contract", ".dpone/gitops/airflow/pod-contract.json", "gitops.airflow_pod_contract"),
            _artifact(
                "runtime_evidence",
                ".dpone/gitops/airflow/runtime-evidence.json",
                "gitops.airflow_runtime_evidence",
                payload=runtime_evidence,
            ),
            _artifact(
                "xcom_summary",
                ".dpone/gitops/airflow/xcom-summary.json",
                "gitops.airflow_xcom_summary",
                payload=xcom_summary,
            ),
            _artifact(
                "pod_launch_evidence",
                ".dpone/gitops/airflow/airflow-pod-launch-evidence.json",
                "gitops.airflow_pod_launch_evidence",
                required=False,
                payload=pod_launch,
            ),
        ),
    )
    payload = report.to_jsonable()

    assert report.passed
    assert payload["kind"] == "gitops.airflow_evidence_bundle"
    assert payload["attempt"] == {
        "dag_id": "dpone_gitops",
        "task_id": "dpone_orders",
        "run_id": "manual__2026-06-16T10:00:00+00:00",
        "try_number": 2,
        "map_index": -1,
    }
    assert payload["pod"] == {
        "pod_name": "dpone-gitops-runtime",
        "pod_uid": "pod-uid-123",
        "namespace": "dpone-runners",
        "service_account": "dpone-runner",
        "image": "ghcr.io/acme/dpone:0.13.0",
        "image_digest": "sha256:" + "a" * 64,
    }
    assert payload["run_identity"] == _run_identity()
    assert payload["correlation"]["schema"] == "dpone.airflow-correlation.v1"
    assert payload["correlation"]["dpone"] == {"run_id": "orders-run-1", "process": "orders"}
    assert payload["correlation"]["artifacts"]["release_id"] == "sha256:" + "a" * 64
    assert payload["correlation"]["pod"]["uid"] == "pod-uid-123"
    assert report.correlation is not None and report.correlation.complete
    assert [artifact["name"] for artifact in payload["artifacts"]] == [
        "bundle",
        "run_spec",
        "runtime_profile",
        "pod_contract",
        "runtime_evidence",
        "xcom_summary",
        "pod_launch_evidence",
    ]
    first_artifact = payload["artifacts"][0]
    expected_sha = hashlib.sha256(
        json.dumps(_payload("gitops.bundle"), ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()
    assert first_artifact["sha256"] == expected_sha
    assert first_artifact["exists"] is True
    assert first_artifact["passed"] is True
    assert "/Users/" not in json.dumps(payload)


def test_release_evidence_blocks_incomplete_correlation_and_advisory_warns() -> None:
    artifacts = (
        _artifact(
            "xcom_summary",
            "xcom-summary.json",
            "gitops.airflow_xcom_summary",
            payload=_payload("gitops.airflow_xcom_summary", run_identity=_run_identity()),
        ),
    )
    kwargs = {
        "dag_id": "dpone_gitops",
        "task_id": "dpone_orders",
        "run_id": "manual__2026-07-17",
        "try_number": 1,
        "map_index": -1,
        "artifacts": artifacts,
    }

    release = GitOpsAirflowEvidenceBundleCollector().collect(runner_policy="release", **kwargs)
    advisory = GitOpsAirflowEvidenceBundleCollector().collect(runner_policy="advisory", **kwargs)

    assert not release.passed
    assert any(issue.code == "DPONE_AIRFLOW_CORRELATION_INCOMPLETE" for issue in release.blockers)
    assert any(issue.code == "DPONE_AIRFLOW_CORRELATION_INCOMPLETE" for issue in advisory.warnings)
    assert not any(issue.code == "DPONE_AIRFLOW_CORRELATION_INCOMPLETE" for issue in advisory.blockers)


def test_evidence_bundle_blocks_runtime_evidence_digest_mismatch() -> None:
    runtime_evidence = _payload("gitops.airflow_runtime_evidence", status="passed")
    report = GitOpsAirflowEvidenceBundleCollector().collect(
        dag_id="dpone_gitops",
        task_id="dpone_orders",
        run_id="manual__2026-07-17",
        try_number=1,
        map_index=-1,
        runner_policy="release",
        pod_name="runtime-pod",
        pod_uid="pod-uid-1",
        artifacts=(
            _artifact(
                "runtime_profile",
                "runtime-profile.json",
                "gitops.airflow_runtime_profile",
                payload=_payload(
                    "gitops.airflow_runtime_profile",
                    namespace="airflow-example",
                    image_digest="sha256:" + "a" * 64,
                ),
            ),
            _artifact(
                "runtime_evidence",
                "runtime-evidence.json",
                "gitops.airflow_runtime_evidence",
                payload=runtime_evidence,
            ),
            _artifact(
                "xcom_summary",
                "xcom-summary.json",
                "gitops.airflow_xcom_summary",
                payload=_payload(
                    "gitops.airflow_xcom_summary",
                    run_identity=_run_identity(),
                    runtime_evidence_sha256="sha256:" + "9" * 64,
                    runtime_evidence={"dpone_run": {"run_id": "run-1", "process": "orders"}},
                ),
            ),
        ),
    )

    assert not report.passed
    assert any(issue.code == "DPONE_AIRFLOW_CORRELATION_MISMATCH" for issue in report.blockers)


def test_airflow_evidence_bundle_collector_blocks_missing_required_and_warns_missing_optional() -> None:
    report = GitOpsAirflowEvidenceBundleCollector().collect(
        dag_id="dpone_gitops",
        task_id="dpone_orders",
        run_id="scheduled__2026-06-16",
        try_number=1,
        map_index=0,
        runner_policy="release",
        artifacts=(
            _artifact("bundle", ".dpone/gitops/bundle/bundle.json", "gitops.bundle"),
            GitOpsAirflowEvidenceArtifactInput(
                name="runtime_evidence",
                path=".dpone/gitops/airflow/runtime-evidence.json",
                expected_kind="gitops.airflow_runtime_evidence",
                required=True,
                content=None,
            ),
            GitOpsAirflowEvidenceArtifactInput(
                name="k8s_smoke",
                path=".dpone/gitops/airflow/airflow-k8s-smoke.json",
                expected_kind="gitops.airflow_k8s_smoke",
                required=False,
                content=None,
            ),
        ),
    )

    assert not report.passed
    assert any(blocker.code == "airflow_evidence_artifact_missing" for blocker in report.blockers)
    assert any(warning.code == "airflow_evidence_optional_artifact_missing" for warning in report.warnings)


def test_airflow_evidence_bundle_collector_blocks_failed_child_artifact() -> None:
    report = GitOpsAirflowEvidenceBundleCollector().collect(
        dag_id="dpone_gitops",
        task_id="dpone_orders",
        run_id="manual__2026-06-16",
        try_number=1,
        map_index=-1,
        runner_policy="release",
        artifacts=(
            _artifact("bundle", ".dpone/gitops/bundle/bundle.json", "gitops.bundle"),
            _artifact(
                "xcom_summary",
                ".dpone/gitops/airflow/xcom-summary.json",
                "gitops.airflow_xcom_summary",
                payload=_payload(
                    "gitops.airflow_xcom_summary",
                    status="failed",
                    blockers=[
                        {
                            "code": "airflow_outcome_failed",
                            "message": "Runtime outcome failed",
                            "path": ".dpone/gitops/airflow/xcom-summary.json",
                            "source": "dpone gitops airflow outcome-gate",
                        }
                    ],
                ),
            ),
        ),
    )

    assert not report.passed
    blocker_codes = {blocker.code for blocker in report.blockers}
    assert "airflow_evidence_child_blocked" in blocker_codes
    assert "airflow_evidence_child_failed" in blocker_codes


def test_airflow_evidence_bundle_collector_blocks_kind_mismatch() -> None:
    report = GitOpsAirflowEvidenceBundleCollector().collect(
        dag_id="dpone_gitops",
        task_id="dpone_orders",
        run_id="manual__2026-06-16",
        try_number=1,
        map_index=-1,
        runner_policy="release",
        artifacts=(
            _artifact(
                "runtime_evidence",
                ".dpone/gitops/airflow/runtime-evidence.json",
                "gitops.airflow_runtime_evidence",
                payload=_payload("gitops.airflow_xcom_summary"),
            ),
        ),
    )

    assert not report.passed
    assert any(blocker.code == "airflow_evidence_kind_mismatch" for blocker in report.blockers)


def test_release_evidence_bundle_blocks_missing_composite_identity() -> None:
    report = GitOpsAirflowEvidenceBundleCollector().collect(
        dag_id="dpone_gitops",
        task_id="dpone_orders",
        run_id="manual__2026-07-16",
        try_number=1,
        map_index=-1,
        runner_policy="release",
        artifacts=(
            _artifact("bundle", "bundle.json", "gitops.bundle"),
            _artifact("xcom_summary", "xcom-summary.json", "gitops.airflow_xcom_summary"),
        ),
    )

    assert not report.passed
    assert any(blocker.code == "DPONE_AIRFLOW_RUN_IDENTITY_MISSING" for blocker in report.blockers)


def test_evidence_bundle_blocks_runtime_image_identity_mismatch() -> None:
    report = GitOpsAirflowEvidenceBundleCollector().collect(
        dag_id="dpone_gitops",
        task_id="dpone_orders",
        run_id="manual__2026-07-16",
        try_number=1,
        map_index=-1,
        runner_policy="release",
        pod_name="dpone-runtime",
        artifacts=(
            _artifact(
                "runtime_profile",
                "runtime-profile.json",
                "gitops.airflow_runtime_profile",
                payload=_payload("gitops.airflow_runtime_profile", image_digest="sha256:" + "f" * 64),
            ),
            _artifact(
                "xcom_summary",
                "xcom-summary.json",
                "gitops.airflow_xcom_summary",
                payload=_payload("gitops.airflow_xcom_summary", run_identity=_run_identity()),
            ),
        ),
    )

    assert not report.passed
    assert any(blocker.code == "DPONE_AIRFLOW_RUN_IDENTITY_MISMATCH" for blocker in report.blockers)


def test_evidence_bundle_blocks_attempt_dag_identity_mismatch() -> None:
    report = GitOpsAirflowEvidenceBundleCollector().collect(
        dag_id="other_daily",
        task_id="dpone_orders",
        run_id="manual__2026-07-16",
        try_number=1,
        map_index=-1,
        runner_policy="release",
        artifacts=(
            _artifact(
                "xcom_summary",
                "xcom-summary.json",
                "gitops.airflow_xcom_summary",
                payload=_payload("gitops.airflow_xcom_summary", run_identity=_run_identity()),
            ),
        ),
    )

    assert not report.passed
    assert any(
        blocker.code == "DPONE_AIRFLOW_RUN_IDENTITY_MISMATCH" and blocker.path == "attempt.dag_id"
        for blocker in report.blockers
    )
