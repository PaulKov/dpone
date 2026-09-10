"""Select risk-aware dpone validation commands from changed repository paths."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections.abc import Iterable
from pathlib import Path
from typing import TypedDict


class ValidationPlan(TypedDict):
    changed_paths: list[str]
    categories: list[str]
    commands: list[str]
    manual_reviews: list[str]
    release_gates: list[str]
    live_evidence_review_required: bool
    note: str


BASE_PYTHON_CHECKS = (
    "uv run ruff check .",
    "uv run ruff format --check .",
    "uv run mypy --config-file mypy.ini",
    "uv run python tools/agent_policy/import_mutation_gate.py",
    "uv run dpone docs check-import-rules",
    "uv run dpone docs check-layer-metrics --baseline docs/layer_metrics_baseline.json",
    (
        'HEAD_SHA="$(git rev-parse HEAD)"; '
        'BASE_SHA="$(git merge-base "$HEAD_SHA" origin/master)"; '
        'if [ "$BASE_SHA" = "$HEAD_SHA" ]; then BASE_SHA="$(git rev-parse "${HEAD_SHA}^")"; fi; '
        "uv run dpone docs check-module-size --baseline docs/module_size_baseline.json "
        '--base-ref "$BASE_SHA" --head-ref "$HEAD_SHA"'
    ),
)
CHANGED_DIFF_FILTER = "ACDMRT"

CATEGORY_COMMANDS: dict[str, tuple[str, ...]] = {
    "python": ('uv run pytest -m "not integration_live" -n auto --dist loadfile',),
    "cli": ('uv run pytest -k "cli or command" -q',),
    "manifest_schema": (
        'uv run pytest -k "manifest or schema or compatibility" -q',
        "uv run dpone docs check-compatibility",
    ),
    "runtime_state": ('uv run pytest -k "runtime or state or checkpoint or replay" -q',),
    "nested_identity": ('uv run pytest -k "nested or parent_id or hierarchical or schema_identity" -q',),
    "connector_route": ('uv run pytest -k "connector or source_sink or strategy or route" -q',),
    "airflow": (
        'uv run pytest -k "airflow" -q',
        "uv run dpone docs check-airflow-public-contracts",
        "uv build packages/dpone-airflow-pack --out-dir dist",
        "uv build packages/apache-airflow-providers-dpone --out-dir dist",
    ),
    "dbt": ('uv run pytest -k "dbt" -q',),
    "docs": (
        "uv run dpone docs check-docs",
        "uv run dpone docs check-generated-references",
        "uv run pytest tests/test_docs_language_contracts.py -q",
        "uv run mkdocs build --strict",
    ),
    "packaging": (
        "uv build",
        "uv build packages/dpone-native-accel --out-dir dist",
        "uv build packages/dpone-airflow-pack --out-dir dist",
        "uv build packages/apache-airflow-providers-dpone --out-dir dist",
        "uv tool run twine check dist/*",
    ),
    "workflow_security": ("uv run python tools/agent_policy/workflow_security.py .",),
    "agent_policy": (
        "uv run python tools/agent_policy/validate_setup.py .",
        "uv run python tools/agent_policy/task_contract.py docs/agent-templates/agent-task-contract.yml --template",
        "uv run python tools/agent_policy/branch_protection.py .agents/policy/github-branch-protection.yml",
        "uv run python tools/agent_policy/workflow_security.py .",
        "uv run python tools/agent_policy/governance_gate.py --base-ref origin/master --output test_artifacts/agent-policy/agent_governance_gate.json",
        "uv run dpone docs check-module-size --package tools/agent_policy --no-baseline --warn-lines 350 --max-lines 400 --warn-sloc 300 --max-sloc 350",
        "uv run dpone docs check-module-size --package tests/agent_policy --no-baseline --warn-lines 350 --max-lines 400 --warn-sloc 300 --max-sloc 350",
        "uv run pytest tests/agent_policy tests/test_module_size_gate.py -q",
    ),
}

CATEGORY_MANUAL_REVIEWS: dict[str, tuple[str, ...]] = {
    "workflow_security": (
        "Review workflow permissions, secret exposure, action pinning, and untrusted-code boundaries.",
    ),
}

CATEGORY_RELEASE_GATES: dict[str, tuple[str, ...]] = {
    "cli": ("R1 CLI correctness and UX", "R2 run CLI/Python parity"),
    "manifest_schema": ("R2 run CLI/Python parity", "R5 contracts and guardrails"),
    "runtime_state": (
        "R2 run CLI/Python parity",
        "R5 contracts and guardrails",
        "R9 recovery/observability/performance",
    ),
    "nested_identity": ("R3 hierarchical identity", "R4 route/strategy matrix"),
    "connector_route": ("R4 route/strategy matrix", "R5 contracts and guardrails"),
    "airflow": ("R7 Airflow and dbt", "R8 packaging/security/supply chain"),
    "dbt": ("R7 Airflow and dbt",),
    "docs": ("R6 documentation and CJM",),
    "packaging": ("R8 packaging/security/supply chain",),
    "workflow_security": ("R5 contracts and guardrails", "R8 packaging/security/supply chain"),
}

AGENT_POLICY_FILES = {
    ".github/CODEOWNERS",
    ".github/ISSUE_TEMPLATE/feature_design.yml",
    ".github/ISSUE_TEMPLATE/release_readiness.yml",
    ".github/codex/prompts/review.md",
    ".github/pull_request_template.md",
    ".worktreeinclude",
    "CONTRIBUTING.md",
}

AGENT_POLICY_DOCS = {
    "docs/agent-development.md",
    "docs/agent-governance.md",
    "docs/agent-permissions.md",
    "docs/agent-release-protocol.md",
    "docs/agent-risk-register.md",
    "docs/agent-security-mapping.md",
    "docs/agent-task-contracts.md",
    "docs/feature-design-agent-task-contract-gate.md",
    "docs/feature-design-agent-permission-tool-registry.md",
    "docs/github-branch-protection.md",
    "docs/supply-chain-slsa.md",
}


def _git(*args: str) -> list[str]:
    completed = subprocess.run(("git", *args), check=True, text=True, capture_output=True)
    return [line for line in completed.stdout.splitlines() if line]


def changed_paths(base_ref: str) -> list[str]:
    try:
        merge_base = _git("merge-base", base_ref, "HEAD")[0]
    except (subprocess.CalledProcessError, IndexError):
        merge_base = base_ref
    paths: set[str] = set()
    for args in (
        ("diff", "--name-only", f"--diff-filter={CHANGED_DIFF_FILTER}", f"{merge_base}...HEAD"),
        ("diff", "--name-only", f"--diff-filter={CHANGED_DIFF_FILTER}"),
        ("diff", "--cached", "--name-only", f"--diff-filter={CHANGED_DIFF_FILTER}"),
    ):
        try:
            paths.update(_git(*args))
        except subprocess.CalledProcessError:
            continue
    try:
        paths.update(_git("ls-files", "--others", "--exclude-standard"))
    except subprocess.CalledProcessError:
        pass
    return sorted(paths)


def categories_for_path(path: str) -> set[str]:
    value = path.replace("\\", "/")
    if value == "AGENTS.md" or value.endswith("/AGENTS.md"):
        return {"agent_policy"}
    if value in AGENT_POLICY_DOCS:
        return {"agent_policy", "docs"}
    if value in AGENT_POLICY_FILES or value.startswith((".codex/", ".agents/", "evals/agent/")):
        return {"agent_policy"}
    if value.startswith(("tests/agent_policy/", "tools/agent_policy/")) or value == "tests/test_module_size_gate.py":
        return {"agent_policy", "python"}
    categories: set[str] = set()
    if value.endswith(".py"):
        categories.add("python")
    if value.startswith(("src/dpone/commands/", "src/dpone/cli/")) or "cli" in Path(value).name:
        categories.add("cli")
    if any(token in value for token in ("manifest", "schema", "compatibility")):
        categories.add("manifest_schema")
    if any(token in value for token in ("runtime", "state", "checkpoint", "replay")):
        categories.add("runtime_state")
    if any(token in value for token in ("nested", "hierarchical", "schema_identity")):
        categories.add("nested_identity")
    if any(token in value for token in ("connector", "source_sink", "source-sink", "strategy", "route")):
        categories.add("connector_route")
    if "airflow" in value:
        categories.add("airflow")
    if "dbt" in value:
        categories.add("dbt")
    if value.startswith("docs/") or value == "mkdocs.yml" or value.endswith(".md"):
        categories.add("docs")
    if value in {"pyproject.toml", "uv.lock"} or (value.startswith("packages/") and not value.endswith("/AGENTS.md")):
        categories.add("packaging")
    if value.startswith(".github/workflows/"):
        categories.add("workflow_security")
    return categories


def plan(paths: Iterable[str]) -> ValidationPlan:
    normalized = sorted({path for path in paths if path})
    categories = sorted(set().union(*(categories_for_path(path) for path in normalized)) if normalized else set())
    commands: list[str] = []
    if "python" in categories:
        commands.extend(BASE_PYTHON_CHECKS)
    for category in categories:
        commands.extend(CATEGORY_COMMANDS.get(category, ()))
    deduplicated = list(dict.fromkeys(commands))
    manual_reviews = list(
        dict.fromkeys(review for category in categories for review in CATEGORY_MANUAL_REVIEWS.get(category, ()))
    )
    release_gates = list(
        dict.fromkeys(gate for category in categories for gate in CATEGORY_RELEASE_GATES.get(category, ()))
    )
    live_required = bool({"connector_route", "airflow", "dbt"} & set(categories))
    return {
        "changed_paths": normalized,
        "categories": categories,
        "commands": deduplicated,
        "manual_reviews": manual_reviews,
        "release_gates": release_gates,
        "live_evidence_review_required": live_required,
        "note": "Live checks require an approved environment; SKIP/UNVERIFIED is not PASS.",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="*")
    parser.add_argument("--base-ref", default="origin/master")
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    args = parser.parse_args()
    paths = args.paths or changed_paths(args.base_ref)
    payload = plan(paths)
    if args.format == "json":
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print("# Change-aware validation plan\n")
        print("Categories: " + (", ".join(payload["categories"]) or "none"))
        print("\n## Commands")
        for command in payload["commands"]:
            print(f"- `{command}`")
        if payload["manual_reviews"]:
            print("\n## Manual reviews")
            for review in payload["manual_reviews"]:
                print(f"- {review}")
        if payload["release_gates"]:
            print("\n## Potential release gates")
            for gate in payload["release_gates"]:
                print(f"- {gate}")
        print(f"\n{payload['note']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
