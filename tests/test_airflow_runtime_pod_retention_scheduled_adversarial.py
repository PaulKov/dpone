from __future__ import annotations

import json
import re
import subprocess
from copy import deepcopy
from pathlib import Path

import pytest

from dpone.contracts.airflow_runtime_pod_retention_template import (
    runtime_pod_retention_template_sha256,
)

ROOT = Path(__file__).parents[1]
RUNBOOK = ROOT / "docs" / "airflow-runtime-pod-retention-kubernetes.md"
MODE_KEY = "dpone.dev/runtime-pod-retention-mode"
SHA_KEY = "dpone.dev/runtime-pod-retention-template-sha256"


def _scheduled_section() -> str:
    content = RUNBOOK.read_text(encoding="utf-8")
    return content.split("### Validate one retained scheduled Job", 1)[1]


def _activation_section() -> str:
    content = RUNBOOK.read_text(encoding="utf-8")
    return content.split("#### Activate apply after two plan cycles", 1)[1].split(
        "### Validate one retained scheduled Job", 1
    )[0]


def _embedded_script(section: str, invocation: str) -> str:
    fragment = section.split(invocation, 1)[1]
    return fragment.split("<<'PY'", 1)[1].split("\nPY", 1)[0]


def _run_python(script: str, *arguments: Path | str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["python3", "-", *(str(argument) for argument in arguments)],
        input=script,
        text=True,
        capture_output=True,
        check=False,
    )


def _cronjob(mode: str = "apply") -> dict[str, object]:
    annotations = {MODE_KEY: mode}
    labels = {
        "app.kubernetes.io/name": "dpone-runtime-pod-retention",
        "app.kubernetes.io/managed-by": "dpone",
    }
    template: dict[str, object] = {
        "metadata": {"labels": labels, "annotations": annotations},
        "spec": {
            "activeDeadlineSeconds": 300,
            "ttlSecondsAfterFinished": 86_400,
            "backoffLimit": 0,
            "template": {
                "metadata": {"labels": labels, "annotations": annotations},
                "spec": {
                    "serviceAccountName": "dpone-runtime-pod-retention",
                    "restartPolicy": "Never",
                    "containers": [
                        {
                            "name": "retention",
                            "image": "registry/dpone@sha256:" + "a" * 64,
                            "command": ["dpone", "airflow", f"runtime-pod-retention-{mode}"],
                        }
                    ],
                },
            },
        },
    }
    digest = runtime_pod_retention_template_sha256(template)
    annotations[SHA_KEY] = digest
    return {
        "metadata": {"uid": "cron-uid", "resourceVersion": "42", "generation": 7},
        "spec": {"jobTemplate": template},
    }


def _marker(cronjob: dict[str, object]) -> dict[str, object]:
    metadata = cronjob["metadata"]
    template = cronjob["spec"]["jobTemplate"]
    return {
        "schema": "dpone.airflow-runtime-pod-retention-rollout-marker.v1",
        "rollout_not_before": "2026-08-04T10:00:00Z",
        "cronjob_uid": metadata["uid"],
        "cronjob_generation": metadata["generation"],
        "mode": template["metadata"]["annotations"][MODE_KEY],
        "template_sha256": template["metadata"]["annotations"][SHA_KEY],
    }


def _job(cronjob: dict[str, object]) -> dict[str, object]:
    template = cronjob["spec"]["jobTemplate"]
    annotations = deepcopy(template["metadata"]["annotations"])
    labels = deepcopy(template["metadata"]["labels"])
    job_spec = deepcopy(template["spec"])
    job_spec["template"]["metadata"]["labels"].update(
        {
            "batch.kubernetes.io/controller-uid": "job-uid",
            "batch.kubernetes.io/job-name": "retention-apply-1",
        }
    )
    job_spec.update(
        {
            "completionMode": "NonIndexed",
            "completions": 1,
            "parallelism": 1,
            "suspend": False,
            "manualSelector": False,
            "selector": {"matchLabels": {"batch.kubernetes.io/controller-uid": "job-uid"}},
        }
    )
    return {
        "metadata": {
            "name": "retention-apply-1",
            "uid": "job-uid",
            "creationTimestamp": "2026-08-04T10:30:00Z",
            "labels": labels,
            "annotations": annotations,
            "ownerReferences": [{"kind": "CronJob", "uid": "cron-uid", "controller": True}],
        },
        "spec": job_spec,
        "status": {
            "active": 0,
            "failed": 0,
            "succeeded": 1,
            "completionTime": "2026-08-04T11:00:00Z",
            "conditions": [{"type": "Complete", "status": "True"}],
        },
    }


def _run_selector(
    tmp_path: Path,
    *,
    cronjob: dict[str, object] | None = None,
    marker: dict[str, object] | None = None,
    job: dict[str, object] | None = None,
) -> subprocess.CompletedProcess[str]:
    cronjob_value = cronjob or _cronjob()
    selected_path = tmp_path / "selected-job.json"
    values = {
        "cronjob.json": cronjob_value,
        "rollout-marker.json": marker or _marker(cronjob_value),
        "jobs.json": {"items": [job or _job(cronjob_value)]},
    }
    for name, value in values.items():
        (tmp_path / name).write_text(json.dumps(value), encoding="utf-8")
    section = _scheduled_section()
    marker_script = _embedded_script(
        section,
        'python3 - "${evidence_dir}/rollout-marker.json"',
    )
    marker_result = _run_python(
        marker_script,
        tmp_path / "rollout-marker.json",
        tmp_path / "cronjob.json",
        tmp_path / "verified-rollout.json",
    )
    if marker_result.returncode != 0:
        return marker_result
    selector_script = _embedded_script(section, 'python3 - "${evidence_dir}/cronjob.json"')
    candidate_path = tmp_path / "candidate-job.json"
    selector_result = _run_python(
        selector_script,
        tmp_path / "cronjob.json",
        tmp_path / "jobs.json",
        candidate_path,
        "2026-08-04T10:00:00Z",
    )
    if selector_result.returncode != 0:
        return selector_result
    verifier_script = _embedded_script(
        section,
        'python3 - "${evidence_dir}/cronjob.json" "${evidence_dir}/verified-rollout.json"',
    )
    return _run_python(
        verifier_script,
        tmp_path / "cronjob.json",
        tmp_path / "verified-rollout.json",
        candidate_path,
        selected_path,
        "2026-08-04T12:00:00Z",
        "7200",
    )


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("cronjob_uid", "replacement-uid"),
        ("cronjob_generation", 8),
        ("template_sha256", "sha256:" + "b" * 64),
    ),
)
def test_selector_rejects_rollout_marker_identity_drift(tmp_path: Path, field: str, value: object) -> None:
    cronjob = _cronjob()
    marker = _marker(cronjob)
    marker[field] = value

    result = _run_selector(tmp_path, cronjob=cronjob, marker=marker, job=_job(cronjob))

    assert result.returncode != 0
    assert not (tmp_path / "selected-job.json").exists()


@pytest.mark.parametrize(
    "mutation",
    (
        "stale_completion",
        "missing_completion",
        "active",
        "parallelism_drift",
        "deadline_drift",
        "template_metadata_drift",
    ),
)
def test_selector_rejects_non_recent_inactive_or_exact_job(
    tmp_path: Path,
    mutation: str,
) -> None:
    cronjob = _cronjob()
    job = _job(cronjob)
    if mutation == "stale_completion":
        job["status"]["completionTime"] = "2026-08-04T09:59:59Z"
        job["metadata"]["creationTimestamp"] = "2026-08-04T08:00:00Z"
    elif mutation == "missing_completion":
        job["status"].pop("completionTime")
    elif mutation == "active":
        job["status"]["active"] = 1
    elif mutation == "parallelism_drift":
        job["spec"]["parallelism"] = 2
    elif mutation == "deadline_drift":
        job["spec"]["activeDeadlineSeconds"] = 600
    else:
        job["spec"]["template"]["metadata"]["labels"]["unexpected"] = "drift"

    result = _run_selector(tmp_path, cronjob=cronjob, marker=_marker(cronjob), job=job)

    assert result.returncode != 0
    assert not (tmp_path / "selected-job.json").exists()


def test_selector_accepts_recent_completed_inactive_exact_job(tmp_path: Path) -> None:
    cronjob = _cronjob()

    result = _run_selector(tmp_path, cronjob=cronjob, marker=_marker(cronjob), job=_job(cronjob))

    assert result.returncode == 0, result.stderr
    assert json.loads((tmp_path / "selected-job.json").read_text(encoding="utf-8"))["metadata"]["uid"] == "job-uid"


@pytest.mark.parametrize(("mode", "filter_index"), (("plan", 0), ("apply", 1)))
def test_rollout_marker_builder_captures_exact_cronjob_identity(mode: str, filter_index: int) -> None:
    content = RUNBOOK.read_text(encoding="utf-8")
    filters = re.findall(
        r'jq -e --arg rollout_not_before "\$\{[^}]+\}" \'\n(.*?)\n\' "\$\{(?:plan|apply)_cronjob_tmp\}"',
        content,
        flags=re.DOTALL,
    )
    cronjob = _cronjob(mode)

    result = subprocess.run(
        ["jq", "-e", "--arg", "rollout_not_before", "2026-08-04T10:00:00Z", filters[filter_index]],
        input=json.dumps(cronjob),
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    marker = json.loads(result.stdout)
    assert marker["cronjob_uid"] == cronjob["metadata"]["uid"]
    assert marker["cronjob_generation"] == cronjob["metadata"]["generation"]
    assert marker["mode"] == mode
    assert marker["template_sha256"] == cronjob["spec"]["jobTemplate"]["metadata"]["annotations"][SHA_KEY]


def test_activation_requires_two_distinct_plan_cycles_bound_to_one_rollout() -> None:
    activation = _activation_section()

    assert "PLAN_CYCLE_EVIDENCE_1" in activation
    assert "PLAN_CYCLE_EVIDENCE_2" in activation
    assert "two distinct completed plan cycles" in activation
    assert "duplicate plan Job UID" in activation
    assert "plan cycle does not belong to the current plan rollout" in activation


def test_activation_plan_cycle_verifier_accepts_two_distinct_cycles(tmp_path: Path) -> None:
    cronjob = _cronjob("plan")
    cronjob_path = tmp_path / "cronjob.json"
    cronjob_path.write_text(json.dumps(cronjob), encoding="utf-8")
    marker = _marker(cronjob)
    evidence_paths = []
    for index in (1, 2):
        directory = tmp_path / f"cycle-{index}"
        directory.mkdir()
        (directory / "rollout-occurrence.json").write_text(
            json.dumps(
                {
                    **marker,
                    "schema": "dpone.airflow-runtime-pod-retention-rollout-occurrence.v1",
                    "job_uid": f"job-{index}",
                }
            ),
            encoding="utf-8",
        )
        (directory / "report.json").write_text(
            json.dumps({"schema": "dpone.airflow-runtime-pod-retention-plan.v1", "status": "ok"}),
            encoding="utf-8",
        )
        evidence_paths.append(directory)
    script = _embedded_script(_activation_section(), 'python3 - "${PLAN_CYCLE_EVIDENCE_1}"')

    result = _run_python(script, *evidence_paths, cronjob_path)

    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("adversary", ("duplicate_job", "generation_drift", "apply_report"))
def test_activation_plan_cycle_verifier_rejects_adversarial_evidence(tmp_path: Path, adversary: str) -> None:
    cronjob = _cronjob("plan")
    cronjob_path = tmp_path / "cronjob.json"
    cronjob_path.write_text(json.dumps(cronjob), encoding="utf-8")
    marker = _marker(cronjob)
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()
    for directory, job_uid in ((first, "job-1"), (second, "job-2")):
        occurrence = {
            **marker,
            "schema": "dpone.airflow-runtime-pod-retention-rollout-occurrence.v1",
            "job_uid": job_uid,
        }
        report = {"schema": "dpone.airflow-runtime-pod-retention-plan.v1", "status": "ok"}
        (directory / "rollout-occurrence.json").write_text(json.dumps(occurrence), encoding="utf-8")
        (directory / "report.json").write_text(json.dumps(report), encoding="utf-8")
    if adversary == "duplicate_job":
        occurrence = json.loads((second / "rollout-occurrence.json").read_text(encoding="utf-8"))
        occurrence["job_uid"] = "job-1"
        (second / "rollout-occurrence.json").write_text(json.dumps(occurrence), encoding="utf-8")
    elif adversary == "generation_drift":
        occurrence = json.loads((second / "rollout-occurrence.json").read_text(encoding="utf-8"))
        occurrence["cronjob_generation"] = 8
        (second / "rollout-occurrence.json").write_text(json.dumps(occurrence), encoding="utf-8")
    else:
        (second / "report.json").write_text(
            json.dumps({"schema": "dpone.airflow-runtime-pod-retention-apply.v1", "status": "ok"}),
            encoding="utf-8",
        )
    script = _embedded_script(_activation_section(), 'python3 - "${PLAN_CYCLE_EVIDENCE_1}"')

    result = _run_python(script, first, second, cronjob_path)

    assert result.returncode != 0


def test_every_kubectl_call_has_a_request_timeout() -> None:
    content = RUNBOOK.read_text(encoding="utf-8")
    logical_lines = content.replace("\\\n", " ").splitlines()
    commands = [line for line in logical_lines if line.startswith("kubectl ")]

    assert commands
    assert all('--request-timeout="${KUBECTL_REQUEST_TIMEOUT}"' in command for command in commands)


def test_runbook_bash_blocks_are_syntactically_valid() -> None:
    content = RUNBOOK.read_text(encoding="utf-8")
    blocks = [fragment.split("```", 1)[0] for fragment in content.split("```bash")[1:]]

    assert blocks
    for block in blocks:
        result = subprocess.run(["bash", "-n"], input=block, text=True, capture_output=True, check=False)
        assert result.returncode == 0, result.stderr
