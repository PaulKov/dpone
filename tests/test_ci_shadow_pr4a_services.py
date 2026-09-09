from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from dpone.services.ci.shadow import (
    ShadowContractError,
    build_claims,
    build_plan,
    changed_paths_from_git,
    event_from_pull_request_payload,
    provider_outcomes,
    read_object,
    write_create_new,
)


def _event(paths: list[str]) -> dict[str, object]:
    return {
        "repository_id": 1,
        "pr_number": 2,
        "base_sha": "a" * 40,
        "head_sha": "b" * 40,
        "merge_sha": "c" * 40,
        "changed_paths": paths,
    }


def _policy() -> dict[str, str]:
    return {"schema_version": "dpone.ci-shadow-route-policy.v1", "default_route": "full"}


def test_docs_route_is_closed_and_deterministic() -> None:
    plan = build_plan(_event(["docs/a.md", "README.md", "docs/a.md"]), _policy())
    assert plan["changed_paths"] == ["README.md", "docs/a.md"]
    assert plan["jobs"] == {
        "static": "RUN",
        "contracts": "RUN",
        "docs": "RUN",
        "python-3.11": "N/A",
        "python-3.12": "N/A",
        "packaging": "N/A",
        "postgresql": "N/A",
        "airflow": "N/A",
        "runtime-wheel-smoke": "N/A",
    }
    assert plan == build_plan(_event(["README.md", "docs/a.md"]), _policy())


def test_unknown_route_and_ambiguity_select_full_work() -> None:
    assert set(build_plan(_event(["unrecognised.file"]), _policy())["jobs"].values()) == {"RUN"}
    assert set(build_plan({**_event(["docs/a.md"]), "semantic_ambiguous": True}, _policy())["jobs"].values()) == {"RUN"}


@pytest.mark.parametrize(
    ("paths", "expected_running"),
    [
        (
            [],
            {
                "static",
                "contracts",
                "docs",
                "python-3.11",
                "python-3.12",
                "packaging",
                "postgresql",
                "airflow",
                "runtime-wheel-smoke",
            },
        ),
        (["docs/a.md"], {"static", "contracts", "docs"}),
        (["uv.lock"], {"static", "contracts", "python-3.11", "python-3.12", "packaging", "runtime-wheel-smoke"}),
        (
            ["pyproject.toml"],
            {
                "static",
                "contracts",
                "docs",
                "python-3.11",
                "python-3.12",
                "packaging",
                "postgresql",
                "airflow",
                "runtime-wheel-smoke",
            },
        ),
        (
            [".github/workflows/pr-gate-shadow.yml"],
            {
                "static",
                "contracts",
                "docs",
                "python-3.11",
                "python-3.12",
                "packaging",
                "postgresql",
                "airflow",
                "runtime-wheel-smoke",
            },
        ),
        (["src/dpone/runtime/job.py"], {"static", "contracts", "python-3.11", "python-3.12", "packaging"}),
        (
            ["tests/integration/postgres/test_xmin.py"],
            {"static", "contracts", "python-3.11", "python-3.12", "packaging", "postgresql"},
        ),
        (
            ["packages/dpone-airflow-pack/src/dpone_airflow_pack/job.py"],
            {"static", "contracts", "python-3.11", "python-3.12", "packaging", "airflow", "runtime-wheel-smoke"},
        ),
        (
            ["constraints/airflow.txt"],
            {"static", "contracts", "python-3.11", "python-3.12", "packaging", "airflow", "runtime-wheel-smoke"},
        ),
        (
            ["unrecognised.file"],
            {
                "static",
                "contracts",
                "docs",
                "python-3.11",
                "python-3.12",
                "packaging",
                "postgresql",
                "airflow",
                "runtime-wheel-smoke",
            },
        ),
    ],
)
def test_closed_route_map(paths: list[str], expected_running: set[str]) -> None:
    plan = build_plan(_event(paths), _policy())
    assert {job for job, selection in plan["jobs"].items() if selection == "RUN"} == expected_running


def test_missing_or_invalid_product_outcome_is_unverified() -> None:
    plan = build_plan(_event(["docs/a.md"]), _policy())
    claims = build_claims(plan, {"static": "PASS", "contracts": "PASS", "docs": "SKIPPED"})
    assert claims["repository_id"] == plan["repository_id"]
    assert claims["pr_number"] == plan["pr_number"]
    assert claims["base_sha"] == plan["base_sha"]
    assert claims["head_sha"] == plan["head_sha"]
    assert claims["merge_sha"] == plan["merge_sha"]
    assert claims["status"] == "UNVERIFIED"
    assert claims["jobs"]["docs"] == {"selection": "RUN", "outcome": "UNVERIFIED"}
    with pytest.raises(ShadowContractError):
        build_claims(plan, {"static": "PASS", "not-a-job": "PASS"})


def test_failure_dominates_pass_when_all_selected_work_is_terminal() -> None:
    plan = build_plan(_event(["docs/a.md"]), _policy())
    claims = build_claims(plan, {"static": "PASS", "contracts": "FAIL", "docs": "PASS"})
    assert claims["status"] == "FAIL"


def test_provider_rows_require_one_completed_exact_name_per_product() -> None:
    selections = build_plan(_event(["docs/a.md"]), _policy())["jobs"]
    rows = [
        {"name": "PR Gate shadow static", "status": "completed", "conclusion": "success"},
        {"name": "PR Gate shadow contracts", "status": "completed", "conclusion": "failure"},
        {"name": "PR Gate shadow docs", "status": "completed", "conclusion": "cancelled"},
    ]
    assert provider_outcomes(selections, rows) == {"static": "PASS", "contracts": "FAIL", "docs": "UNVERIFIED"}
    assert provider_outcomes(selections, rows + [rows[0]])["static"] == "UNVERIFIED"


def test_airflow_product_requires_all_eight_exact_matrix_cells() -> None:
    selections = build_plan(_event(["packages/dpone-airflow-pack/pyproject.toml"]), _policy())["jobs"]
    rows = [
        {
            "name": f"PR Gate shadow Airflow {version} / py{python}",
            "status": "completed",
            "conclusion": "success",
        }
        for version in ("2.10.5", "2.11.0", "3.2.0", "3.3.0")
        for python in ("3.11", "3.12")
    ]
    assert provider_outcomes(selections, rows)["airflow"] == "PASS"
    assert provider_outcomes(selections, rows[:-1])["airflow"] == "UNVERIFIED"


def test_create_only_output_and_strict_json_reader(tmp_path: Path) -> None:
    output = tmp_path / "plan.json"
    write_create_new(output, {"x": 1})
    assert read_object(output) == {"x": 1}
    with pytest.raises(FileExistsError):
        write_create_new(output, {"x": 1})
    duplicate = tmp_path / "duplicate.json"
    duplicate.write_text('{"x":1,"x":2}', encoding="utf-8")
    with pytest.raises(ShadowContractError):
        read_object(duplicate)
    oversized = tmp_path / "oversized.json"
    oversized.write_bytes(b"{" + b" " * 1_048_576)
    with pytest.raises(ShadowContractError, match="exceeds"):
        read_object(oversized)
    nested = tmp_path / "nested.json"
    nested.write_text('{"x":' * 9 + "0" + "}" * 9, encoding="utf-8")
    with pytest.raises(ShadowContractError, match="nesting"):
        read_object(nested)


def test_rejects_incomplete_event_and_non_closed_plan() -> None:
    with pytest.raises(ShadowContractError):
        build_plan({}, _policy())
    with pytest.raises(ShadowContractError):
        build_claims({"schema_version": "dpone.ci-change-plan.v1", "jobs": {}}, {})
    with pytest.raises(ShadowContractError):
        build_plan(_event(["docs/a.md"]), {"schema_version": "dpone.ci-shadow-route-policy.v1"})
    malformed = _event(["docs/a.md"])
    malformed["head_sha"] = "g" * 40
    with pytest.raises(ShadowContractError):
        build_plan(malformed, _policy())
    with pytest.raises(ShadowContractError):
        build_plan({**_event(["docs/a.md"]), "semantic_ambiguous": "false"}, _policy())
    with pytest.raises(ShadowContractError):
        build_plan({**_event(["../escape"]), "extra": 1}, _policy())


def test_exact_git_diff_is_rename_disabled_and_deduplicated_by_planner(tmp_path: Path) -> None:
    def git(*args: str) -> str:
        return subprocess.check_output(["git", *args], cwd=tmp_path, text=True).strip()

    git("init", "-q")
    git("config", "user.email", "test@example.invalid")
    git("config", "user.name", "test")
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "old.md").write_text("one", encoding="utf-8")
    git("add", ".")
    git("commit", "-qm", "base")
    base = git("rev-parse", "HEAD")
    (tmp_path / "docs" / "old.md").rename(tmp_path / "docs" / "new.md")
    git("add", "-A")
    git("commit", "-qm", "head")
    head = git("rev-parse", "HEAD")
    assert changed_paths_from_git(repository_root=tmp_path, base_sha=base, head_sha=head) == [
        "docs/new.md",
        "docs/old.md",
    ]


def test_github_pull_request_event_extracts_only_identity() -> None:
    payload = {
        "number": 2,
        "repository": {"id": 1, "full_name": "ignored/repository"},
        "pull_request": {"base": {"sha": "a" * 40}, "head": {"sha": "b" * 40}},
    }
    assert event_from_pull_request_payload(payload, merge_sha="c" * 40) == {
        "repository_id": 1,
        "pr_number": 2,
        "base_sha": "a" * 40,
        "head_sha": "b" * 40,
        "merge_sha": "c" * 40,
        "changed_paths": [],
    }


def test_github_pull_request_event_fails_closed_without_merge_identity() -> None:
    with pytest.raises(ShadowContractError):
        event_from_pull_request_payload(
            {"repository": {"id": 1}, "pull_request": {"base": {}, "head": {}}, "number": 1}, merge_sha="c" * 40
        )
