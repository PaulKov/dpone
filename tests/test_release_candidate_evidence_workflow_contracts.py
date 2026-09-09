from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from tests.agent_policy._release_candidate_evidence_helpers import policy

ROOT = Path(__file__).resolve().parents[1]


def _workflow(name: str) -> dict[str, Any]:
    payload = yaml.safe_load((ROOT / ".github" / "workflows" / name).read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _events(workflow: dict[str, Any]) -> dict[str, Any]:
    value = workflow.get("on") or workflow.get(True)
    assert isinstance(value, dict)
    return value


def _job(workflow: dict[str, Any], job_id: str) -> dict[str, Any]:
    value = workflow["jobs"][job_id]
    assert isinstance(value, dict)
    return value


def _step(job: dict[str, Any], name: str) -> dict[str, Any]:
    for value in job.get("steps", []):
        if isinstance(value, dict) and value.get("name") == name:
            return value
    raise AssertionError(f"missing workflow step: {name}")


def _run(step: dict[str, Any]) -> str:
    value = step.get("run")
    assert isinstance(value, str)
    return value


def _needs(job: dict[str, Any]) -> set[str]:
    value = job.get("needs", [])
    if isinstance(value, str):
        return {value}
    assert isinstance(value, list)
    return {str(item) for item in value}


def _ancestors(workflow: dict[str, Any], job_id: str) -> set[str]:
    observed: set[str] = set()
    pending = list(_needs(_job(workflow, job_id)))
    while pending:
        current = pending.pop()
        if current in observed:
            continue
        observed.add(current)
        pending.extend(_needs(_job(workflow, current)))
    return observed


def _step_names(job: dict[str, Any]) -> list[str]:
    return [str(step.get("name", "")) for step in job.get("steps", []) if isinstance(step, dict)]


def _assert_immediate_freshness(
    job: dict[str, Any],
    *,
    freshness_name: str,
    first_write_name: str,
    workflow_path: str,
) -> None:
    steps = [step for step in job.get("steps", []) if isinstance(step, dict)]
    write_index = next(index for index, step in enumerate(steps) if step.get("name") == first_write_name)
    assert write_index > 0
    freshness = steps[write_index - 1]
    assert freshness.get("name") == freshness_name
    assert freshness.get("if") is None
    assert freshness.get("continue-on-error") is None
    expected_verifier_dir = {
        ".github/workflows/release.yml": "${{ runner.temp }}/release-verifier",
        ".github/workflows/runtime-image.yml": (
            "${{ runner.temp }}/runtime-image-preflight/release-candidate-verifier"
        ),
    }[workflow_path]
    expected_env = {
        "GITHUB_TOKEN": "${{ github.token }}",
        "RELEASE_COMMIT": "${{ github.sha }}",
        "RELEASE_TAG": "${{ github.ref_name }}",
        "VERIFIER_DIR": expected_verifier_dir,
    }
    if workflow_path == ".github/workflows/release.yml":
        expected_env["AUTHORITY_DIR"] = "${{ runner.temp }}/release-preflight-authority"
    assert freshness["env"] == expected_env
    command = _run(freshness)
    if workflow_path == ".github/workflows/release.yml":
        assert command.index("release_commit_artifact_gate.py") < command.index("release_merge_receipt_gate.py")
        assert command.index("release_merge_receipt_gate.py") < command.index("release_candidate_evidence_gate.py")
        assert "--authorization-report" in command
        assert "--authorization-ruleset-report" in command
    assert "release_candidate_evidence_gate.py" in command
    assert f'--publication-workflow-path "{workflow_path}"' in command
    assert '--publication-run-id "${GITHUB_RUN_ID}"' in command
    assert '--publication-run-attempt "${GITHUB_RUN_ATTEMPT}"' in command
    assert "--tagged-at" not in command
    assert "taggerdate" not in command


def test_release_candidate_workflow_is_exact_manual_master_dispatch() -> None:
    workflow = _workflow("release-candidate-evidence.yml")
    events = _events(workflow)

    assert workflow["run-name"] == policy.workflow_run_name(
        release="${{ inputs.release }}",
        commit_sha="${{ inputs.commit_sha }}",
    )
    assert set(events) == {"workflow_dispatch"}
    inputs = events["workflow_dispatch"]["inputs"]
    assert set(inputs) == {"release", "commit_sha"}
    assert all(inputs[name]["required"] is True for name in inputs)
    assert all(inputs[name]["type"] == "string" for name in inputs)
    assert all("default" not in inputs[name] for name in inputs)
    assert workflow["concurrency"] == {
        "group": "release-candidate-evidence-${{ github.sha }}",
        "cancel-in-progress": False,
    }

    terminal = _job(workflow, "release-candidate-evidence")
    assert terminal["env"]["EXPECTED_COMMIT_SHA"] == "${{ inputs.commit_sha }}"
    assert terminal["env"]["RELEASE"] == "${{ inputs.release }}"
    checkout = _step(terminal, "Checkout exact workflow commit")
    assert checkout["with"] == {
        "ref": "${{ github.sha }}",
        "fetch-depth": 0,
        "persist-credentials": False,
    }
    binding = _run(_step(terminal, "Verify immutable master identity"))
    for invariant in (
        '"${GITHUB_EVENT_NAME}" != "workflow_dispatch"',
        '"${GITHUB_REF}" != "refs/heads/master"',
        '"${GITHUB_REF_TYPE}" != "branch"',
        "^[0-9a-f]{40}$",
        "^v(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)$",
        'checked_out_commit="$(git rev-parse HEAD)"',
        'master_commit="$(git rev-parse refs/remotes/origin/master)"',
        '"${EXPECTED_COMMIT_SHA}" != "${GITHUB_SHA}"',
        '"${EXPECTED_COMMIT_SHA}" != "${checked_out_commit}"',
        '"${EXPECTED_COMMIT_SHA}" != "${master_commit}"',
    ):
        assert invariant in binding


def test_release_candidate_has_one_fail_closed_terminal_native_check() -> None:
    workflow = _workflow("release-candidate-evidence.yml")
    jobs = workflow["jobs"]

    assert set(jobs) == {
        "collect-live-evidence",
        "release-candidate-evidence",
    }
    terminal_jobs = [job_id for job_id, job in jobs.items() if job.get("name") == "Release candidate evidence"]
    assert terminal_jobs == ["release-candidate-evidence"]
    assert _needs(_job(workflow, terminal_jobs[0])) == {"collect-live-evidence"}
    assert all(terminal_jobs[0] not in _needs(job) for job in jobs.values())
    assert _job(workflow, terminal_jobs[0]).get("if") is None
    assert _job(workflow, terminal_jobs[0]).get("continue-on-error") is None

    raw = (ROOT / ".github/workflows/release-candidate-evidence.yml").read_text(encoding="utf-8")
    assert "checks: write" not in raw
    assert "/check-runs" not in raw
    assert workflow["permissions"] == {"contents": "read"}
    assert _job(workflow, terminal_jobs[0])["permissions"]["checks"] == "read"


def test_release_candidate_wires_exact_attempt_live_and_authority_artifacts() -> None:
    workflow = _workflow("release-candidate-evidence.yml")
    collector = _job(workflow, "collect-live-evidence")
    terminal = _job(workflow, "release-candidate-evidence")
    exact_live_name = "release-candidate-live-${{ github.sha }}-${{ github.run_id }}-${{ github.run_attempt }}"

    assert collector["uses"] == "./.github/workflows/live-certification.yml"
    assert collector["with"] == {
        "profile": "native_transfer",
        "row_count": "25000",
        "run_native_benchmark_suite": False,
        "expected_commit_sha": "${{ inputs.commit_sha }}",
        "local_artifact_name": exact_live_name,
    }
    assert "secrets" not in collector
    download = _step(terminal, "Download observed live evidence from this exact attempt")
    assert download["with"]["name"] == exact_live_name
    assert download["with"]["path"] == "test_artifacts/release-candidate/input/live"

    upload = _step(terminal, "Upload immutable release-candidate authority")
    assert upload["with"] == {
        "name": "release-candidate-evidence-${{ github.sha }}-${{ github.run_id }}-${{ github.run_attempt }}",
        "path": "test_artifacts/release-candidate/authority/",
        "if-no-files-found": "error",
        "retention-days": 90,
        "overwrite": False,
    }
    terminal_steps = _step_names(terminal)
    assert (
        terminal_steps.index("Build closed release-candidate authority")
        < terminal_steps.index("Verify closed authority before provider upload")
        < terminal_steps.index("Upload immutable release-candidate authority")
    )


def test_reusable_live_workflow_preserves_exact_commit_and_attempt_binding() -> None:
    workflow = _workflow("live-certification.yml")
    call_inputs = _events(workflow)["workflow_call"]["inputs"]
    assert call_inputs["expected_commit_sha"] == {
        "description": "Optional exact commit assertion for a reusable release-candidate call.",
        "required": False,
        "default": "",
        "type": "string",
    }
    assert call_inputs["local_artifact_name"]["default"] == "live-certification-local"
    assert call_inputs["local_artifact_name"]["type"] == "string"

    job = _job(workflow, "local-live-certification")
    binding = _run(_step(job, "Verify optional exact commit binding"))
    for invariant in (
        "^[0-9a-f]{40}$",
        'actual_commit="$(git rev-parse HEAD)"',
        'master_commit="$(git rev-parse refs/remotes/origin/master)"',
        '"${GITHUB_EVENT_NAME}" != "workflow_dispatch"',
        '"${GITHUB_REF}" != "refs/heads/master"',
        '"${GITHUB_REF_TYPE}" != "branch"',
        '"${EXPECTED_COMMIT_SHA}" != "${GITHUB_SHA}"',
        '"${EXPECTED_COMMIT_SHA}" != "${actual_commit}"',
        '"${EXPECTED_COMMIT_SHA}" != "${master_commit}"',
    ):
        assert invariant in binding

    upload = _step(job, "Upload live certification artifacts")
    assert upload["if"] == "always()"
    assert upload["with"] == {
        "name": "${{ inputs.local_artifact_name || 'live-certification-local' }}",
        "path": "test_artifacts/live_certification",
        "if-no-files-found": "error",
        "retention-days": 90,
        "overwrite": False,
    }
    raw = (ROOT / ".github/workflows/live-certification.yml").read_text(encoding="utf-8")
    assert "checks: write" not in raw
    assert "/check-runs" not in raw


def test_synthetic_metrics_state_and_checklist_literals_cannot_enter_authority() -> None:
    candidate = _workflow("release-candidate-evidence.yml")
    candidate_script = "\n".join(
        _run(step)
        for job in candidate["jobs"].values()
        for step in job.get("steps", [])
        if isinstance(step, dict) and isinstance(step.get("run"), str)
    )
    forbidden = ("--metrics-json", "$CERT_ROOT/state_evidence.json", "dpone ops pre-release-checklist")
    assert all(marker not in candidate_script for marker in forbidden)

    live = _job(_workflow("live-certification.yml"), "local-live-certification")
    for marker in forbidden:
        containing = [step for step in live["steps"] if marker in str(step.get("run", ""))]
        assert containing
        assert all(step.get("if") == "${{ false }}" for step in containing)
        assert all(str(step.get("name", "")).startswith("Disabled (UNVERIFIED):") for step in containing)


def test_release_mutation_jobs_reverify_fresh_provider_boundary_before_write() -> None:
    workflow = _workflow("release.yml")
    assert set(_events(workflow)) == {"push"}
    assert _events(workflow)["push"]["tags"] == ["v*.*.*"]

    mutation_steps: dict[str, set[str]] = {}
    for job_id, job in workflow["jobs"].items():
        for step in job.get("steps", []):
            action = str(step.get("uses", ""))
            command = str(step.get("run", ""))
            public_write = action.startswith(
                ("actions/attest@", "pypa/gh-action-pypi-publish@", "softprops/action-gh-release@")
            ) or any(marker in command for marker in ("twine upload", "gh release create"))
            if public_write:
                mutation_steps.setdefault(job_id, set()).add(str(step.get("name", "")))
    assert mutation_steps == {
        "attest": {"Attest release artifacts"},
        "github-release": {"Create GitHub Release"},
    }
    assert all("preflight" in _ancestors(workflow, job_id) for job_id in mutation_steps)
    cases = (
        (
            "attest",
            "Prove fresh live authority immediately before attestation",
            "Attest release artifacts",
        ),
        (
            "github-release",
            "Prove fresh live authority immediately before GitHub Release",
            "Create GitHub Release",
        ),
    )
    for job_id, freshness_name, first_write_name in cases:
        job = _job(workflow, job_id)
        assert job["permissions"]["actions"] == "read"
        assert job["permissions"]["checks"] == "read"
        _assert_immediate_freshness(
            job,
            freshness_name=freshness_name,
            first_write_name=first_write_name,
            workflow_path=".github/workflows/release.yml",
        )
    publish_steps = [step for step in _job(workflow, "publish")["steps"] if isinstance(step, dict)]
    handoff_index = next(
        index
        for index, step in enumerate(publish_steps)
        if step.get("name") == "Confirm controller is the sole PyPI publisher"
    )
    authority_gate = publish_steps[handoff_index - 1]
    public_gate = publish_steps[handoff_index - 2]
    assert authority_gate.get("name") == "Prove fresh release authority before controller handoff"
    assert public_gate.get("name") == "Prove safe public PyPI prepublication state"
    assert public_gate.get("if") is None
    assert public_gate.get("continue-on-error") is None
    assert "pypi_prepublication_gate.py" in _run(public_gate)


def test_ghcr_mutation_jobs_reverify_fresh_provider_boundary_before_write() -> None:
    workflow = _workflow("runtime-image.yml")
    preflight = _job(workflow, "release-preflight")
    assert preflight["if"] == "${{ github.event_name == 'push' && github.ref_type == 'tag' }}"
    assert "Validate runtime release identity" in _step_names(preflight)

    expected_mutations = {
        "push-attest-runtime-candidate": {
            "Push digest-only runtime candidate",
            "Attest runtime image provenance",
            "Attest runtime image SBOM",
        },
        "promote-certified-image": {"Reconcile certified runtime image aliases"},
    }
    observed_mutations: dict[str, set[str]] = {}
    for job_id, job in workflow["jobs"].items():
        for step in job.get("steps", []):
            command = str(step.get("run", ""))
            public_mutation = (
                any(marker in command for marker in ("crane push", "docker push"))
                or ("runtime_image_promotion.py" in command and " promote " in command)
                or step.get("with", {}).get("push-to-registry") is True
            )
            if public_mutation:
                observed_mutations.setdefault(job_id, set()).add(str(step.get("name", "")))
    assert observed_mutations == expected_mutations
    for job_id in expected_mutations:
        assert "release-preflight" in _ancestors(workflow, job_id)
        assert _job(workflow, job_id)["permissions"]["actions"] == "read"
        assert _job(workflow, job_id)["permissions"]["checks"] == "read"

    _assert_immediate_freshness(
        _job(workflow, "push-attest-runtime-candidate"),
        freshness_name="Reverify release candidate immediately before GHCR publication",
        first_write_name="Push digest-only runtime candidate",
        workflow_path=".github/workflows/runtime-image.yml",
    )
    _assert_immediate_freshness(
        _job(workflow, "promote-certified-image"),
        freshness_name="Reverify release candidate immediately before GHCR alias promotion",
        first_write_name="Reconcile certified runtime image aliases",
        workflow_path=".github/workflows/runtime-image.yml",
    )

    development_script = "\n".join(str(step.get("run", "")) for step in _job(workflow, "development-build")["steps"])
    assert "crane push" not in development_script
    assert "runtime_image_promotion.py" not in development_script or " promote " not in development_script
    assert all(
        step.get("with", {}).get("push-to-registry") is not True
        for step in _job(workflow, "development-build")["steps"]
    )
    promotion_names = _step_names(_job(workflow, "promote-certified-image"))
    assert promotion_names.index("Validate runtime image certification") < promotion_names.index(
        "Reconcile certified runtime image aliases"
    )
