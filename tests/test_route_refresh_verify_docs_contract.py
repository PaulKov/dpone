from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_route_refresh_verify_docs_are_self_service() -> None:
    required_fragments = {
        "docs/route-refresh-execute.md": (
            "Route refresh verification",
            "route-refresh-verify",
            "route_refresh_verification.json",
            "typed hash",
        ),
        "docs/developer-route-refresh-execute.md": (
            "Route refresh verification architecture",
            "RouteRefreshVerificationService",
            "RouteRefreshSnapshotReader",
            "RouteRefreshVerificationPolicy",
        ),
        "docs/ci-cd.md": (
            "route_refresh_verification.json",
            "test_route_refresh_verify.py",
            "test_cli_route_refresh_verify_command.py",
        ),
        "docs/architecture.md": (
            "RouteRefreshVerificationService",
            "route_refresh_verification.json",
        ),
        "docs/source-sink/postgres-to-mssql.md": (
            "route-refresh-verify",
            "route_refresh_verification.json",
        ),
    }

    for relative_path, fragments in required_fragments.items():
        text = (ROOT / relative_path).read_text(encoding="utf-8")
        missing = [fragment for fragment in fragments if fragment not in text]
        assert missing == [], f"{relative_path} missing {missing}"


def test_live_certification_workflow_publishes_route_refresh_verification_artifacts() -> None:
    workflow = (ROOT / ".github/workflows/live-certification.yml").read_text(encoding="utf-8")

    assert "route_refresh_verification.json" in workflow
    assert "route_refresh_verification" in workflow
    assert "refresh-executor/mssql-clickhouse" in workflow
    assert "refresh-executor/postgres-mssql" in workflow
