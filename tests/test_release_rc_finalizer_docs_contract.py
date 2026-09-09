from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _read(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


def test_release_rc_finalizer_user_docs_cover_cli_artifacts_and_runbook() -> None:
    text = _read("docs/release-rc-finalizer.md")
    required = [
        "dpone ops release-rc-finalize",
        "merge_train.json",
        "release_rc_finalizer.json",
        "route_release_finalizer",
        "release_evidence_pack",
        "v0.10.0",
        "Runbook",
    ]
    for item in required:
        assert item in text


def test_release_rc_finalizer_developer_docs_cover_boundaries_and_policy() -> None:
    text = _read("docs/developer-release-rc-finalizer.md")
    required = [
        "dpone.ops.release_rc_finalizer",
        "dpone.ops.release_rc_models",
        "dpone.ops.release_rc_policy",
        "ReleaseRcFinalizerService",
        "ReleaseRcFinalizerPolicy",
        "Do not add GitHub API calls",
        "Do not add release RC business logic to CLI handlers",
    ]
    for item in required:
        assert item in text


def test_release_rc_finalizer_is_linked_from_nav_architecture_ops_and_ci_docs() -> None:
    mkdocs = _read("mkdocs.yml")
    architecture = _read("docs/architecture.md")
    ops_cli = _read("docs/ops-cli.md")
    ci_cd = _read("docs/ci-cd.md")
    developer_ci = _read("docs/developer-ci-cd.md")
    docs_readme = _read("docs/README.md")
    release_doc = _read("docs/release.md")

    for text in (mkdocs, architecture, ops_cli, ci_cd, developer_ci, docs_readme, release_doc):
        assert "release-rc-finalize" in text
        assert "Release RC finalizer" in text
