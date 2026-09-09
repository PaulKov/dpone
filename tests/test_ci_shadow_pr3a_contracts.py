from __future__ import annotations

from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "docs/feature-design-ci-shadow-pr3a-ci-hygiene.md"
PARENT = ROOT / "docs/feature-design-ci-pr-gate-exact-sha-evidence.md"
TASK = ROOT / "test_artifacts/agent-policy/dpone-ci-shadow-pr3a-spec.yml"
ADR = ROOT / "docs/adr/0048-exact-sha-readiness-evidence.md"
BASE_COMMIT = "6fdbcf90f38a88d5aa1793213160e0eb0fb6939d"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _squash(text: str) -> str:
    return " ".join(text.split())


def _section(text: str, heading: str, next_heading: str) -> str:
    assert text.count(heading) == 1
    assert text.count(next_heading) == 1
    return text.split(heading, maxsplit=1)[1].split(next_heading, maxsplit=1)[0]


def _yaml_block_after(text: str, marker: str) -> dict[str, object]:
    assert text.count(marker) == 1
    tail = text.split(marker, maxsplit=1)[1]
    block = tail.split("```yaml\n", maxsplit=1)[1].split("\n```", maxsplit=1)[0]
    parsed = yaml.safe_load(block)
    assert isinstance(parsed, dict)
    return parsed


def _require_valid_amendment_status(text: str) -> None:
    metadata = text.split("## Executive summary", maxsplit=1)[0]
    status_lines = [line for line in metadata.splitlines() if line.startswith("- Status:")]
    pending = "- [ ] Maintainer changes the amended status to `APPROVED` on the exact reviewed commit."
    approved = "- [x] Maintainer changes the amended status to `APPROVED` on the exact reviewed commit."
    valid = (status_lines == ["- Status: RESEARCHED"] and text.count(pending) == 1 and approved not in text) or (
        status_lines == ["- Status: APPROVED"] and text.count(approved) == 1 and pending not in text
    )
    assert valid


def test_pr3a_security_amendment_requires_exact_head_approval() -> None:
    raw_spec = _read(SPEC)
    spec = _squash(raw_spec)

    _require_valid_amendment_status(raw_spec)
    approved = raw_spec.replace("- Status: RESEARCHED", "- Status: APPROVED", 1).replace(
        "- [ ] Maintainer changes the amended status to `APPROVED` on the exact reviewed commit.",
        "- [x] Maintainer changes the amended status to `APPROVED` on the exact reviewed commit.",
        1,
    )
    _require_valid_amendment_status(approved)
    assert BASE_COMMIT in spec
    assert "No production workflow or Dependabot configuration is changed by this specification PR" in spec
    assert "Merge only that exact `APPROVED` specification commit without bypass" in spec
    assert "implementation task contract cites the merged approved child-specification commit" in spec


def test_pr3a_freezes_exact_workflow_scope_and_release_exclusion() -> None:
    spec = _squash(_read(SPEC))

    mutable = (
        ".github/dependabot.yml",
        ".github/workflows/ci.yml",
        ".github/workflows/airflow-pack-compat.yml",
        ".github/workflows/airflow-pack-compat-nightly.yml",
        ".github/workflows/pages.yml",
        ".github/workflows/dependency-review.yml",
        ".agents/policy/workflow-security.yml",
    )
    frozen = (
        ".github/workflows/release.yml",
        ".github/workflows/runtime-image.yml",
        ".github/workflows/certification-release-summary.yml",
        ".github/workflows/route-certification-release.yml",
        ".github/workflows/route-release-finalize.yml",
    )

    assert all(path in spec for path in mutable)
    assert all(path in spec for path in frozen)
    assert "byte-identical to the PR3A implementation base" in spec
    assert "uv sync --locked --all-extras" in spec
    assert "uv sync --locked`" in spec
    assert "intentional isolated `uv pip` environments are unchanged" in spec


def test_pr3a_dependabot_contract_and_live_label_stop_are_exact() -> None:
    raw_spec = _read(SPEC)
    spec = _squash(
        _section(
            raw_spec,
            "### Dependabot",
            "### Pull-request and nightly concurrency",
        )
    )

    for expected in (
        'package-ecosystem: "uv"',
        'day: "monday"',
        'time: "08:30"',
        'package-ecosystem: "github-actions"',
        'day: "wednesday"',
        'time: "08:00"',
        'timezone: "Europe/Berlin"',
        "open-pull-requests-limit: 3",
        "open-pull-requests-limit: 2",
        "`dependencies`, `python:uv`, and `github-actions`",
    ):
        assert expected in spec
    assert raw_spec.count("`github-actions` returned HTTP 404") == 1
    assert raw_spec.count("Repository automation does not create or mutate labels") == 1
    assert "blocks implementation" in spec
    assert "does not block review or approval of this specification" in spec
    assert "gh auth status --active --hostname github.com >/dev/null 2>&1" in spec


def test_pr3a_concurrency_and_matrix_topology_is_executable() -> None:
    spec = _squash(
        _section(
            _read(SPEC),
            "### Pull-request and nightly concurrency",
            "### GitHub Pages capability split",
        )
    )

    for expected in (
        "airflow_max_parallel",
        "`required: false` and `default: 2`",
        "`max-parallel: ${{ inputs.airflow_max_parallel || 2 }}`",
        "It must not branch on `github.event_name`",
        "Direct `push`, `pull_request`, and `workflow_dispatch` invocations therefore resolve the fallback/default `2`",
        "receives the raw `${{ inputs.airflow_max_parallel }}`",
        "explicit `0`, any other number, a string, or an empty value on a called run stops",
        "raw boundaries `0/1/2/4/5`",
        "passes `airflow_max_parallel: 4`",
        "runtime-wheel-smoke` remains `max-parallel: 1`",
        "cron: '23 1 * * *'",
        "timezone: Europe/Berlin",
        "group: airflow-pack-compat-nightly",
        "queue: max",
        "cancel-in-progress: false",
        "the 101st is provider-cancelled",
        'gh run rerun "$RUN_ID" --repo PaulKov/dpone',
        "gh workflow run airflow-pack-compat-nightly.yml",
        "github.event.pull_request.number",
        "the same run gains a new attempt and retains its original SHA/ref",
    ):
        assert expected in spec


def test_pr3a_closes_pages_and_dependency_review_capabilities() -> None:
    raw_spec = _read(SPEC)
    spec = _squash(
        _section(
            raw_spec,
            "### GitHub Pages capability split",
            "### Read-only Dependency Review",
        )
        + _section(
            raw_spec,
            "### Read-only Dependency Review",
            "### actionlint compatibility boundary",
        )
    )

    for expected in (
        "top-level `permissions: {}`",
        "group: pages-${{ github.event_name == 'pull_request' && github.event.pull_request.number || github.ref }}",
        "cancel-in-progress: ${{ github.event_name == 'pull_request' }}",
        "build` receives only `contents: read`",
        "deploy` receives only `pages: write` and `id-token: write`",
        "No Pages job uses `queue`",
        "verify_current_master",
        "gh api --hostname github.com",
        '"repos/${GITHUB_REPOSITORY}/git/ref/heads/master"',
        "verified_attempt: ${{ steps.verify.outputs.attempt }}",
        "github.event_name == 'pull_request' || github.run_attempt == 1",
        "needs: [build, verify_current_master]",
        "needs.verify_current_master.outputs.verified_attempt == format('{0}', github.run_attempt)",
        "only `pull_request` and `push`, each restricted to `master`",
        "top-level `contents: read`",
        "comment-summary-in-pr: never",
        "base-ref: ${{ github.event.before }}",
        "head-ref: ${{ github.sha }}",
        "synthetic check-run backfill are deleted",
        "Dependency Review` remains the native job/check result",
        "release exact-commit evidence",
        "Manual invocation is absent and therefore `N/A`",
        "governance tests parse the actual `.github/workflows/pages.yml`",
    ):
        assert expected in spec

    pages_concurrency = _yaml_block_after(
        raw_spec,
        "The entire Pages workflow uses this exact concurrency object",
    )["concurrency"]
    assert pages_concurrency == {
        "group": "pages-${{ github.event_name == 'pull_request' && github.event.pull_request.number || github.ref }}",
        "cancel-in-progress": "${{ github.event_name == 'pull_request' }}",
    }
    pages_section = _section(
        raw_spec,
        "### GitHub Pages capability split",
        "### Read-only Dependency Review",
    )
    assert pages_section.count("queue: max") == 0
    assert "never rerun any job of the obsolete subject" in _squash(raw_spec)


def test_pr3a_exact_commit_and_pages_ordering_decisions_are_closed() -> None:
    spec = _squash(_read(SPEC))

    for expected in (
        "PR run is attached to `refs/pull/<N>/merge`",
        "push run is required because the unchanged release preflight queries all nineteen required checks on the exact `master`/tag commit",
        "Push PASS applies only to the exact `master` commit",
        "If the PR run is absent, provider recovery requires a new reviewed PR head/event",
        "if none exists, use a new reviewed successor commit",
        "Pages subject is proven stale",
        "rerun_deploy_only: FORBIDDEN",
        "never rerun any existing non-PR job",
        "The only Pages recovery uses the versioned workflow-dispatch REST endpoint",
        "X-GitHub-Api-Version: ${API_VERSION}",
        "workflow_run_id",
        "PAGES_RECOVERY_RUN_ID=%s",
        'event `workflow_dispatch`, provider-visible `head_branch == "master"`',
        "The Workflow Runs API does not expose a `ref` field",
        "provider job `SKIPPED/SUCCESS`; deployment `NOT_RUN`; evidence `UNVERIFIED`",
        "PR 3A deliberately does not select a Pages REST deployment by SHA",
        "single pinned `actions/deploy-pages` step must conclude `success`",
        "provider step number `3`",
        "Jobs API does not expose YAML job keys or action `uses`",
        "actions/deploy-pages@cd2ce8fcbc39b97be8ca5fce6e763baed58fa128",
        "GET /repos/PaulKov/dpone/actions/runs/{run_id}",
        "GET /repos/PaulKov/dpone/actions/runs/{run_id}/attempts/1/jobs?per_page=100",
        "partially paginated run, job, or step identity is `UNVERIFIED`",
        "stable workflow group keeps the newer run pending",
        "Non-PR Pages build/upload is ineligible after attempt `1`",
        "they parse the actual `pages.yml`",
    ):
        assert expected in spec
    assert "gh workflow run pages.yml" not in spec


def test_pr3a_nightly_queue_is_location_specific() -> None:
    concurrency = _yaml_block_after(
        _read(SPEC),
        "has this exact trigger/concurrency contract:",
    )["concurrency"]

    assert concurrency == {
        "group": "airflow-pack-compat-nightly",
        "queue": "max",
        "cancel-in-progress": False,
    }


def test_pr3a_actionlint_strategy_is_pinned_and_narrow() -> None:
    raw_spec = _read(SPEC)
    actionlint_section = _section(
        raw_spec,
        "### actionlint compatibility boundary",
        "## Detailed implementation algorithm",
    )
    spec = _squash(actionlint_section)

    for expected in (
        "actionlint `1.7.12`",
        "8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8",
        "aba9ced2dee8d27fecca3dc7feb1a7f9a52caefa1eb46f3271ea66b6e0e6953f",
        'unexpected key "queue" for "concurrency" section',
        'unexpected key "queue" for "concurrency" section. expected one of "cancel-in-progress", "group"',
        '-ignore \'^unexpected key "queue" for "concurrency" section\\. expected one of "cancel-in-progress", "group"$\'',
        'kind: "syntax-check"',
        "first line of `actionlint -version`",
        "c872d6db8c6bf83a8eaa704fc93999f027d55dffbc63b8a6abdccb47df5f4cd4",
        "6,074,530-byte length",
        "only `.github/workflows/airflow-pack-compat-nightly.yml`",
        "The waiver is file-scoped",
        "used only on that one original workflow byte",
        "All other diagnostics remain fatal",
        "-no-color -format '{{json .}}' -shellcheck '' -pyflakes ''",
        "requires exit `0`, stdout exactly `[]\\n`, and empty stderr",
        "must exit `1`, write one JSON array to stdout, and leave stderr empty",
        "must contain exactly one object with the exact original filepath",
        "The one queue-bearing file is then checked twice",
        "must exit `0`, write exactly `[]\\n` to stdout, and leave stderr empty",
        "leave stderr empty",
        "stdout exactly `[]\\n`",
        "ACTIONLINT_QUEUE_WAIVER_OBSOLETE",
        "upstream actionlint release supports `concurrency.queue`",
    ):
        assert expected in spec

    assert "`pages.yml`, and `dependency-review.yml`" in spec
    assert actionlint_section.count("`.github/workflows/pages.yml` may use") == 0


def test_pr3a_authoritative_sections_and_yaml_markers_reject_duplicates() -> None:
    raw_spec = _read(SPEC)

    with pytest.raises(AssertionError):
        _require_valid_amendment_status(
            raw_spec.replace(
                "## Executive summary",
                "- Status: RESEARCHED\n\n## Executive summary",
                1,
            )
        )
    with pytest.raises(AssertionError):
        _section(
            raw_spec + "\n### Dependabot\ncontradictory duplicate\n",
            "### Dependabot",
            "### Pull-request and nightly concurrency",
        )
    marker = "has this exact trigger/concurrency contract:"
    with pytest.raises(AssertionError):
        _yaml_block_after(raw_spec + f"\n{marker}\n```yaml\n{{}}\n```\n", marker)


def test_pr3a_documentation_journey_and_rollback_are_complete() -> None:
    raw_spec = _read(SPEC)
    spec = _squash(raw_spec)

    for path in (
        "docs/ci-cd.md",
        "docs/cicd/workflows.md",
        "docs/cicd/runbooks.md",
        "docs/developer-ci-cd.md",
        "docs/cicd/release-and-pages.md",
        "docs/github-pages.md",
        "docs/github-branch-protection.md",
        "docs/testing/overview.md",
        "docs/testing/index.md",
        "CHANGELOG.md",
    ):
        assert path in spec

    for stage in (
        "Discover",
        "Prepare",
        "Configure",
        "Execute",
        "Observe",
        "Diagnose",
        "Recover",
        "Operate",
        "Upgrade",
    ):
        assert f"**{stage}**" in spec
    assert "reviewed revert of the PR3A implementation commit" in spec
    assert "never manufacture a successful check" in spec
    assert "uv lock --check" in spec
    assert "uv sync --locked --all-extras" in spec

    documentation_plan = _squash(_section(raw_spec, "## Documentation plan", "## Rollout and rollback"))
    for expected in (
        "pinned-actionlint installation/streams/obsolete-waiver recovery",
        "pinned actionlint version and exact per-path invocation",
        "one-file waiver and removal tripwire",
        "exact actionlint exits/stdout/stderr expectations",
    ):
        assert expected in documentation_plan


def test_parent_distinguishes_approved_baseline_from_amendment_lifecycle() -> None:
    parent = _squash(_read(PARENT))

    assert "[PR 3A executable child specification](feature-design-ci-shadow-pr3a-ci-hygiene.md)" in parent
    assert "approved `dd45dd85` CI-hygiene baseline" in parent
    assert "Any separately reviewed provider-bound security amendment must be `APPROVED`" in parent
    assert "linked child's status and checklist are the authoritative current lifecycle state" in parent


def test_adr_0048_owns_pages_attempt_recovery_and_evidence() -> None:
    adr = _squash(_read(ADR))

    for expected in (
        "Every non-PR build/upload, current-master verification, and deploy job is eligible only for provider run attempt `1`",
        "Recovery always dispatches a new `pages.yml` run",
        "No recovery selects, deletes, overwrites, or reuses a prior run's immutable `github-pages` artifact",
        "deployment outcome is `NOT_RUN` and its evidence is `UNVERIFIED`",
        "SHA-pinned `actions/deploy-pages` invocation",
    ):
        assert expected in adr


def test_pr3a_spec_task_contract_binds_real_base_and_forbids_runtime_changes() -> None:
    payload = yaml.safe_load(_read(TASK))

    assert payload["base_commit"] == BASE_COMMIT
    assert payload["specification"].endswith("docs/feature-design-ci-shadow-pr3a-ci-hygiene.md")
    assert str(SPEC.relative_to(ROOT)) in payload["owned_paths"]
    assert str(Path(__file__).relative_to(ROOT)) in payload["owned_paths"]
    assert "tests/test_ci_shadow_pr3a_pages_recovery.py" in payload["owned_paths"]
    assert str(ADR.relative_to(ROOT)) in payload["owned_paths"]
    assert ".github/workflows" in payload["forbidden_paths"]
    assert ".github/dependabot.yml" in payload["forbidden_paths"]
    assert "src" in payload["forbidden_paths"]
    assert "Status is APPROVED" in " ".join(payload["acceptance_criteria"])
    assert "git diff --cached --check" in payload["required_checks"]["broad"]
