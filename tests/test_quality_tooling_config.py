from __future__ import annotations

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_pyproject_contains_quality_tooling_dev_dependencies() -> None:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    project = data["project"]
    dev_group = data["dependency-groups"]["dev"]
    pytest_config = data["tool"]["pytest"]["ini_options"]
    joined = "\n".join(dev_group)
    assert project["requires-python"] == ">=3.11,<3.13"
    assert "Programming Language :: Python :: 3.10" not in project["classifiers"]
    assert "Programming Language :: Python :: 3.11" in project["classifiers"]
    assert "Programming Language :: Python :: 3.12" in project["classifiers"]
    assert all("python_version < '3.11'" not in requirement for requirement in project["dependencies"])
    assert "ruff" in joined
    assert "mypy" in joined
    assert "pre-commit" in joined
    assert "types-PyYAML" in joined
    assert "types-requests" in joined
    assert data["tool"]["uv"]["default-groups"] == ["dev"]
    assert pytest_config["testpaths"] == ["tests"]
    assert data["tool"]["ruff"]["target-version"] == "py311"
    assert data["tool"]["ruff"]["line-length"] == 120


def test_ci_matrix_matches_python_support_policy() -> None:
    ci = yaml.safe_load((ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8"))
    python_versions = ci["jobs"]["quality"]["strategy"]["matrix"]["python-version"]

    assert python_versions == ["3.11", "3.12"]


def test_ci_binds_module_size_ratchet_to_exact_execution_identity() -> None:
    ci = yaml.safe_load((ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8"))
    steps = ci["jobs"]["quality-preflight"]["steps"]
    checkout = next(step for step in steps if step.get("name") == "Checkout")
    module_step = next(step for step in steps if step.get("name") == "Module size")
    command = module_step["run"]
    exact_head = "${{ github.event.pull_request.head.sha || github.sha }}"

    assert checkout["with"]["ref"] == exact_head
    assert "--baseline docs/module_size_baseline.json" in command
    assert "--base-ref" in command
    assert 'git merge-base "$HEAD_SHA" "origin/$DEFAULT_BRANCH"' in command
    assert 'git rev-parse "${HEAD_SHA}^"' in command
    assert "--head-ref" in command
    assert "github.event.pull_request.base.sha || github.event.before" in module_step["env"]["EVENT_BASE_SHA"]
    assert module_step["env"]["HEAD_SHA"] == exact_head


def test_public_docs_do_not_advertise_python_310_support() -> None:
    docs = [
        ROOT / "docs" / "getting-started" / "installation.md",
        ROOT / "docs" / "cicd" / "workflows.md",
        ROOT / "docs" / "cicd" / "runbooks.md",
    ]

    offenders = [path.relative_to(ROOT).as_posix() for path in docs if "3.10" in path.read_text(encoding="utf-8")]

    assert offenders == []


def test_pre_commit_has_ruff_and_mypy_hooks() -> None:
    config = yaml.safe_load((ROOT / ".pre-commit-config.yaml").read_text(encoding="utf-8"))
    repos = config["repos"]
    repo_urls = {repo["repo"] for repo in repos}
    assert "https://github.com/astral-sh/ruff-pre-commit" in repo_urls
    assert "https://github.com/pre-commit/mirrors-mypy" in repo_urls


def test_mypy_ini_targets_clean_layers() -> None:
    text = (ROOT / "mypy.ini").read_text(encoding="utf-8")
    assert "src/dpone/app" in text
    assert "src/dpone/ports" in text
    assert "src/dpone/adapters" in text
    assert "src/dpone/services" in text
    assert "src/dpone/manifest" in text
    assert "src/dpone/dag" in text
