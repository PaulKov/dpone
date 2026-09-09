from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _read(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


def test_release_rc_collector_user_docs_cover_cli_and_github_export_flow() -> None:
    text = _read("docs/release-rc-collector.md")
    required = [
        "dpone ops release-rc-collect",
        "gh pr view",
        "statusCheckRollup",
        "merge_train.json",
        "release_rc_inputs.json",
        "release-rc-finalize",
    ]
    for item in required:
        assert item in text


def test_release_rc_collector_developer_docs_cover_boundaries_and_di() -> None:
    text = _read("docs/developer-release-rc-collector.md")
    required = [
        "dpone.ops.release_rc_collector",
        "dpone.ops.release_rc_collector_models",
        "dpone.ops.release_rc_payloads",
        "ReleaseRcCollectorService",
        "Do not call GitHub APIs from the collector service",
        "Do not add finalizer policy decisions to the collector",
    ]
    for item in required:
        assert item in text


def test_release_rc_collector_is_linked_from_nav_architecture_ops_ci_and_cli_reference() -> None:
    paths = [
        "mkdocs.yml",
        "docs/architecture.md",
        "docs/ops-cli.md",
        "docs/ci-cd.md",
        "docs/developer-ci-cd.md",
        "docs/README.md",
        "docs/release.md",
        "docs/cli-reference.md",
    ]
    for path in paths:
        text = _read(path)
        assert "release-rc-collect" in text
        assert "Release RC collector" in text
