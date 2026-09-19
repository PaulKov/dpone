"""Exact tracked-source identity shared by ClickHouse Docker evidence producers."""

from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path

FIXTURE_FILES = (
    Path("tests/integration/clickhouse_cluster/docker-compose.yml"),
    Path("tests/integration/clickhouse_cluster/conftest.py"),
    Path("tests/integration/clickhouse_cluster/evidence.py"),
    Path("tests/integration/clickhouse_cluster/evidence_identity.py"),
    Path("tests/integration/clickhouse_cluster/external_replication_live_support.py"),
    Path("tests/integration/clickhouse_cluster/external_replication_performance_budget.json"),
    Path("tests/integration/clickhouse_cluster/external_replication_performance_evidence.py"),
    Path("tests/integration/clickhouse_cluster/test_clickhouse_external_replication_live.py"),
    Path("tests/integration/clickhouse_cluster/test_clickhouse_external_replication_performance_live.py"),
    Path("tests/integration/clickhouse_cluster/config/keeper/keeper.xml"),
    Path("tests/integration/clickhouse_cluster/config/node1/cluster.xml"),
    Path("tests/integration/clickhouse_cluster/config/node2/cluster.xml"),
)


def source_binding() -> tuple[str, str]:
    """Return exact commit/tree only when the tracked checkout is clean."""

    if str(_git("status", "--porcelain=v1", "--untracked-files=no")).strip():
        raise RuntimeError("tracked worktree must be clean before writing Docker evidence")
    return (
        str(_git("rev-parse", "HEAD")).strip(),
        str(_git("rev-parse", "HEAD^{tree}")).strip(),
    )


def fixture_digest(source_commit: str) -> str:
    """Digest exact tracked fixture bytes from the bound commit."""

    digest = hashlib.sha256()
    for path in FIXTURE_FILES:
        digest.update(path.as_posix().encode("utf-8"))
        digest.update(b"\0")
        content = _git("show", f"{source_commit}:{path.as_posix()}", text=False)
        if not isinstance(content, bytes):
            raise TypeError("git blob output must be bytes")
        digest.update(content)
        digest.update(b"\0")
    return digest.hexdigest()


def _git(*args: str, text: bool = True) -> str | bytes:
    return subprocess.run(
        ("git", *args),
        check=True,
        capture_output=True,
        text=text,
    ).stdout


__all__ = ["FIXTURE_FILES", "fixture_digest", "source_binding"]
