from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "docs/feature-design-ci-shadow-pr3a-ci-hygiene.md"
JOB_MARKER = "job boundary is:"
RECOVERY_MARKER = "The exact Pages recovery/evidence policy is:"
CURRENT_SHA = "a" * 40
WORKFLOW_ID = 288538896
DEPLOY_PAGES_PIN = "actions/deploy-pages@cd2ce8fcbc39b97be8ca5fce6e763baed58fa128"


def _yaml_after(marker: str) -> dict[str, object]:
    text = SPEC.read_text(encoding="utf-8")
    assert text.count(marker) == 1
    tail = text.split(marker, maxsplit=1)[1]
    block = tail.split("```yaml\n", maxsplit=1)[1].split("\n```", maxsplit=1)[0]
    parsed = yaml.safe_load(block)
    assert isinstance(parsed, dict)
    return parsed


def _normalized(expression: str) -> str:
    return " ".join(expression.split())


def _is_positive_int(value: object) -> bool:
    return type(value) is int and value > 0


def _build_eligible(condition: str, *, event_name: str, run_attempt: int) -> bool:
    assert condition == "github.event_name == 'pull_request' || github.run_attempt == 1"
    return event_name == "pull_request" or run_attempt == 1


def _non_pr_job_eligible(
    condition: str,
    *,
    event_name: str,
    ref: str,
    run_attempt: int,
) -> bool:
    assert _normalized(condition).startswith(
        "github.event_name != 'pull_request' && github.ref == 'refs/heads/master' && github.run_attempt == 1"
    )
    return event_name != "pull_request" and ref == "refs/heads/master" and run_attempt == 1


@dataclasses.dataclass(frozen=True)
class DispatchReceipt:
    workflow_run_id: object = 101
    run_url: str = "https://api.github.com/repos/PaulKov/dpone/actions/runs/101"
    html_url: str = "https://github.com/PaulKov/dpone/actions/runs/101"


@dataclasses.dataclass(frozen=True)
class WorkflowRunObservation:
    repository: str = "PaulKov/dpone"
    workflow_id: object = WORKFLOW_ID
    workflow_path: str = ".github/workflows/pages.yml"
    run_id: object = 101
    run_attempt: object = 1
    event_name: str = "workflow_dispatch"
    head_branch: str = "master"
    head_sha: str = CURRENT_SHA


def _dispatch_identity_is_exact(
    receipt: DispatchReceipt,
    run: WorkflowRunObservation,
    *,
    prior_run_id: object = 100,
) -> bool:
    return all(
        (
            _is_positive_int(prior_run_id),
            _is_positive_int(receipt.workflow_run_id),
            receipt.workflow_run_id != prior_run_id,
            receipt.workflow_run_id == run.run_id,
            receipt.run_url == f"https://api.github.com/repos/PaulKov/dpone/actions/runs/{run.run_id}",
            receipt.html_url == f"https://github.com/PaulKov/dpone/actions/runs/{run.run_id}",
            _run_identity_is_exact(run),
            run.event_name == "workflow_dispatch",
        )
    )


def _run_identity_is_exact(run: WorkflowRunObservation) -> bool:
    return all(
        (
            run.repository == "PaulKov/dpone",
            _is_positive_int(run.workflow_id),
            run.workflow_id == WORKFLOW_ID,
            run.workflow_path == ".github/workflows/pages.yml",
            _is_positive_int(run.run_id),
            _is_positive_int(run.run_attempt),
            run.run_attempt == 1,
            run.event_name in {"push", "workflow_dispatch"},
            run.head_branch == "master",
            run.head_sha == CURRENT_SHA,
        )
    )


@dataclasses.dataclass(frozen=True)
class WorkflowDeploymentContract:
    job_key: str = "deploy"
    job_display_name: str = "Deploy GitHub Pages documentation"
    configure_step_name: str = "Configure Pages"
    deploy_step_name: str = "Deploy Pages"
    deploy_step_number: int = 3
    deploy_step_uses: str = DEPLOY_PAGES_PIN
    configure_step_occurrences: int = 1
    deploy_step_occurrences: int = 1


@dataclasses.dataclass(frozen=True)
class ProviderStep:
    name: str
    number: int
    conclusion: str


@dataclasses.dataclass(frozen=True)
class DeploymentObservation:
    run: WorkflowRunObservation = WorkflowRunObservation()
    job_display_name: str = "Deploy GitHub Pages documentation"
    deploy_job_id: int = 202
    deploy_job_conclusion: str = "success"
    steps: tuple[ProviderStep, ...] = (
        ProviderStep("Set up job", 1, "success"),
        ProviderStep("Configure Pages", 2, "success"),
        ProviderStep("Deploy Pages", 3, "success"),
        ProviderStep("Complete job", 4, "success"),
    )


def _deployment_is_pass(
    policy: dict[str, object],
    observation: DeploymentObservation,
    source: WorkflowDeploymentContract,
    *,
    expected_run_id: int = 101,
    expected_job_id: int = 202,
) -> bool:
    evidence = policy["deployment_evidence"]
    assert evidence["pass_requires"] == [
        "authenticated_workflow_deploy_display_name_order_and_action_pin",
        "exact_repository_workflow_run_attempt_one_head_branch_event_sha_and_job",
        "unique_provider_display_name_job_and_ordered_step_succeeded",
    ]
    deploy_steps = [step for step in observation.steps if step.name == source.deploy_step_name]
    configure_steps = [step for step in observation.steps if step.name == source.configure_step_name]
    configure_step_ok = (
        len(configure_steps) == 1
        and configure_steps[0].number < source.deploy_step_number
        and configure_steps[0].conclusion == "success"
    )
    deploy_step_ok = (
        len(deploy_steps) == 1
        and deploy_steps[0].number == source.deploy_step_number
        and deploy_steps[0].conclusion == "success"
    )
    return all(
        (
            source.job_key == "deploy",
            source.job_display_name == "Deploy GitHub Pages documentation",
            source.configure_step_name == "Configure Pages",
            source.deploy_step_name == "Deploy Pages",
            source.deploy_step_number == 3,
            source.deploy_step_uses == DEPLOY_PAGES_PIN,
            source.configure_step_occurrences == 1,
            source.deploy_step_occurrences == 1,
            _run_identity_is_exact(observation.run),
            observation.run.run_id == expected_run_id,
            observation.job_display_name == source.job_display_name,
            observation.deploy_job_id == expected_job_id,
            observation.deploy_job_conclusion == "success",
            configure_step_ok,
            deploy_step_ok,
        )
    )


def _select_deploy_observation(
    pages: tuple[tuple[DeploymentObservation, ...], ...] | None,
    *,
    pagination_complete: bool,
) -> DeploymentObservation | None:
    if pages is None or not pagination_complete:
        return None
    matches = [job for page in pages for job in page if job.job_display_name == "Deploy GitHub Pages documentation"]
    return matches[0] if len(matches) == 1 else None


@pytest.mark.parametrize(
    ("event_name", "run_attempt", "eligible"),
    (
        ("pull_request", 1, True),
        ("pull_request", 2, True),
        ("push", 1, True),
        ("push", 2, False),
        ("workflow_dispatch", 1, True),
        ("workflow_dispatch", 2, False),
    ),
)
def test_pages_build_rejects_non_pr_reruns_before_artifact_upload(
    event_name: str,
    run_attempt: int,
    eligible: bool,
) -> None:
    build = _yaml_after(JOB_MARKER)["build"]
    assert _build_eligible(build["if"], event_name=event_name, run_attempt=run_attempt) is eligible


@pytest.mark.parametrize(
    ("job_name", "event_name", "ref", "run_attempt", "eligible"),
    (
        ("verify_current_master", "workflow_dispatch", "refs/heads/master", 1, True),
        ("verify_current_master", "workflow_dispatch", "refs/heads/master", 2, False),
        ("verify_current_master", "workflow_dispatch", "refs/heads/foreign", 1, False),
        ("deploy", "push", "refs/heads/master", 1, True),
        ("deploy", "push", "refs/heads/master", 2, False),
        ("deploy", "pull_request", "refs/heads/master", 1, False),
    ),
)
def test_pages_non_pr_verification_and_deploy_are_attempt_one_only(
    job_name: str,
    event_name: str,
    ref: str,
    run_attempt: int,
    eligible: bool,
) -> None:
    condition = _yaml_after(JOB_MARKER)[job_name]["if"]
    assert _non_pr_job_eligible(condition, event_name=event_name, ref=ref, run_attempt=run_attempt) is eligible


def test_pages_recovery_forbids_every_non_pr_rerun() -> None:
    recovery = _yaml_after(RECOVERY_MARKER)["pages_recovery"]
    assert recovery == {
        "rerun_all_jobs": "FORBIDDEN",
        "rerun_deploy_only": "FORBIDDEN",
        "rerun_verify_current_master": "FORBIDDEN",
        "only_recovery": {"action": "DISPATCH_NEW_RUN_ON_CURRENT_MASTER"},
    }


def test_pages_dispatch_receipt_binds_new_exact_run() -> None:
    receipt = DispatchReceipt()
    run = WorkflowRunObservation()
    assert _dispatch_identity_is_exact(receipt, run)

    invalid_pairs = (
        (dataclasses.replace(receipt, workflow_run_id=100), run),
        (dataclasses.replace(receipt, workflow_run_id=0), run),
        (dataclasses.replace(receipt, workflow_run_id=-1), run),
        (dataclasses.replace(receipt, workflow_run_id=101.5), run),
        (dataclasses.replace(receipt, workflow_run_id=True), run),
        (dataclasses.replace(receipt, workflow_run_id="101"), run),
        (dataclasses.replace(receipt, workflow_run_id=999), run),
        (dataclasses.replace(receipt, run_url="https://example.invalid"), run),
        (receipt, dataclasses.replace(run, repository="Attacker/fork")),
        (receipt, dataclasses.replace(run, workflow_id=999)),
        (receipt, dataclasses.replace(run, workflow_id=float(WORKFLOW_ID))),
        (receipt, dataclasses.replace(run, workflow_path=".github/workflows/foreign.yml")),
        (receipt, dataclasses.replace(run, run_id=True)),
        (receipt, dataclasses.replace(run, run_attempt=2)),
        (receipt, dataclasses.replace(run, run_attempt=1.0)),
        (receipt, dataclasses.replace(run, event_name="push")),
        (receipt, dataclasses.replace(run, head_branch="foreign")),
        (receipt, dataclasses.replace(run, head_sha="b" * 40)),
    )
    assert all(not _dispatch_identity_is_exact(candidate, observed) for candidate, observed in invalid_pairs)

    for prior in (0, -1, 1.5, True, "100"):
        assert not _dispatch_identity_is_exact(receipt, run, prior_run_id=prior)


def test_pages_skipped_deploy_is_unverified() -> None:
    skipped = _yaml_after(RECOVERY_MARKER)["deployment_evidence"]["skipped_job"]
    assert skipped == {
        "provider_check": "SKIPPED_SUCCESS",
        "deployment_outcome": "NOT_RUN",
        "evidence_status": "UNVERIFIED",
    }


def test_pages_deployment_pass_binds_authenticated_source_and_provider_fields() -> None:
    policy = _yaml_after(RECOVERY_MARKER)
    observation = DeploymentObservation()
    source = WorkflowDeploymentContract()
    assert _deployment_is_pass(policy, observation, source)
    assert _deployment_is_pass(
        policy,
        dataclasses.replace(observation, run=dataclasses.replace(observation.run, event_name="push")),
        source,
    )

    invalid_observations = (
        dataclasses.replace(
            observation,
            run=dataclasses.replace(observation.run, repository="Attacker/fork"),
        ),
        dataclasses.replace(
            observation,
            run=dataclasses.replace(observation.run, run_attempt=2),
        ),
        dataclasses.replace(
            observation,
            run=dataclasses.replace(observation.run, event_name="pull_request"),
        ),
        dataclasses.replace(observation, job_display_name="Build GitHub Pages documentation"),
        dataclasses.replace(observation, deploy_job_id=999),
        dataclasses.replace(observation, deploy_job_conclusion="skipped"),
        dataclasses.replace(
            observation,
            steps=(ProviderStep("Configure Pages", 2, "success"),),
        ),
        dataclasses.replace(
            observation,
            steps=(ProviderStep("Deploy Pages", 2, "success"),),
        ),
        dataclasses.replace(
            observation,
            steps=(ProviderStep("Deploy Pages", 3, "failure"),),
        ),
        dataclasses.replace(
            observation,
            steps=(
                ProviderStep("Configure Pages", 2, "success"),
                ProviderStep("Deploy Pages", 3, "success"),
                ProviderStep("Deploy Pages", 4, "success"),
            ),
        ),
    )
    assert all(not _deployment_is_pass(policy, candidate, source) for candidate in invalid_observations)

    invalid_sources = (
        dataclasses.replace(source, job_key="foreign"),
        dataclasses.replace(source, job_display_name="foreign"),
        dataclasses.replace(source, deploy_step_name="foreign"),
        dataclasses.replace(source, deploy_step_number=2),
        dataclasses.replace(source, deploy_step_uses="actions/deploy-pages@" + "f" * 40),
        dataclasses.replace(source, deploy_step_occurrences=2),
    )
    assert all(not _deployment_is_pass(policy, observation, candidate) for candidate in invalid_sources)


@pytest.mark.parametrize("deploy_page", (0, 1, 2))
def test_pages_jobs_pagination_selects_one_exact_deploy_job(deploy_page: int) -> None:
    deploy = DeploymentObservation()
    build = dataclasses.replace(deploy, job_display_name="Build GitHub Pages documentation")
    pages = tuple((deploy,) if index == deploy_page else (build,) for index in range(3))

    selected = _select_deploy_observation(pages, pagination_complete=True)
    assert selected == deploy
    assert _deployment_is_pass(
        _yaml_after(RECOVERY_MARKER),
        selected,
        WorkflowDeploymentContract(),
    )


def test_pages_jobs_pagination_fails_closed_on_absence_duplicate_partial_or_api_failure() -> None:
    deploy = DeploymentObservation()
    build = dataclasses.replace(deploy, job_display_name="Build GitHub Pages documentation")

    assert _select_deploy_observation(((build,),), pagination_complete=True) is None
    assert _select_deploy_observation(((deploy,), (deploy,)), pagination_complete=True) is None
    assert _select_deploy_observation(((deploy,),), pagination_complete=False) is None
    assert _select_deploy_observation(None, pagination_complete=False) is None
