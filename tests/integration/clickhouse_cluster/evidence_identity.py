"""Exact tracked-source identity shared by ClickHouse Docker evidence producers."""

from __future__ import annotations

import base64
import hashlib
import json
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

FIXTURE_FILES = (
    Path("tests/integration/clickhouse_cluster/docker-compose.yml"),
    Path("tests/integration/clickhouse_cluster/conftest.py"),
    Path("tests/integration/clickhouse_cluster/evidence.py"),
    Path("tests/integration/clickhouse_cluster/evidence_identity.py"),
    Path("tests/integration/clickhouse_cluster/external_replication_live_support.py"),
    Path("tests/integration/clickhouse_cluster/external_replication_performance_budget.json"),
    Path("tests/integration/clickhouse_cluster/external_replication_performance_evidence.py"),
    Path("tests/integration/clickhouse_cluster/external_replication_performance_validation.py"),
    Path("tests/integration/clickhouse_cluster/test_clickhouse_external_replication_live.py"),
    Path("tests/integration/clickhouse_cluster/test_clickhouse_external_replication_performance_live.py"),
    Path("tests/integration/clickhouse_cluster/config/keeper/keeper.xml"),
    Path("tests/integration/clickhouse_cluster/config/node1/cluster.xml"),
    Path("tests/integration/clickhouse_cluster/config/node2/cluster.xml"),
)

PERFORMANCE_V1_FIXTURE_FILES = (
    Path("tests/integration/clickhouse_cluster/docker-compose.yml"),
    Path("tests/integration/clickhouse_cluster/conftest.py"),
    Path("tests/integration/clickhouse_cluster/evidence.py"),
    Path("tests/integration/clickhouse_cluster/external_replication_live_support.py"),
    Path("tests/integration/clickhouse_cluster/external_replication_performance_budget.json"),
    Path("tests/integration/clickhouse_cluster/test_clickhouse_external_replication_live.py"),
    Path("tests/integration/clickhouse_cluster/test_clickhouse_external_replication_performance_live.py"),
    Path("tests/integration/clickhouse_cluster/config/keeper/keeper.xml"),
    Path("tests/integration/clickhouse_cluster/config/node1/cluster.xml"),
    Path("tests/integration/clickhouse_cluster/config/node2/cluster.xml"),
)

_FULL_LOWERCASE_SHA1 = re.compile(r"[0-9a-f]{40}")
_FULL_SHA256 = re.compile(r"[0-9a-f]{64}")
_PERFORMANCE_V1_SCHEMA = "dpone.clickhouse.external-publication-benchmark.v1"
_PERFORMANCE_V1_BINDING_SCHEMA = "dpone.clickhouse.external-publication-benchmark-source-binding.v1"
PERFORMANCE_V1_SOURCE_COMMIT = "5451b1989796258a022c21cdd3059e442f81b1dc"
_PERFORMANCE_V1_SOURCE_TREE = "b5c90123c42fef1a7059a5bcff450ff4e42122e9"
_PERFORMANCE_V1_FIXTURE_DIGEST = "a6dfa6864c7181a41666a470866f86b9058c3622ca0c681c6a2edfcd7d3f6d7c"
_PERFORMANCE_V1_SOURCE_BINDING = Path(
    "tests/fixtures/release_evidence/clickhouse-external-benchmark-v1-source-binding-5451b198.json"
)


@dataclass(frozen=True)
class HistoricalPerformanceBinding:
    """Self-contained immutable inputs for one historical evidence schema."""

    source_tree: str
    fixture_digest: str
    benchmark_config: bytes


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

    return _fixture_digest(source_commit, FIXTURE_FILES)


def performance_fixture_digest(source_commit: str, schema_version: str) -> str:
    """Reproduce the fixture identity algorithm owned by each evidence schema."""

    files = (
        PERFORMANCE_V1_FIXTURE_FILES
        if schema_version == "dpone.clickhouse.external-publication-benchmark.v1"
        else FIXTURE_FILES
    )
    return _fixture_digest(source_commit, files)


def historical_performance_binding(
    source_commit: str,
    schema_version: str,
) -> HistoricalPerformanceBinding | None:
    """Load the reviewed v1 binding without requiring an orphan Git object."""

    if (source_commit, schema_version) != (PERFORMANCE_V1_SOURCE_COMMIT, _PERFORMANCE_V1_SCHEMA):
        return None
    payload: Any = json.loads(_PERFORMANCE_V1_SOURCE_BINDING.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or set(payload) != {
        "benchmark_config_base64",
        "benchmark_config_sha256",
        "fixture_digest",
        "fixture_files",
        "schema_version",
        "source_commit",
        "source_tree",
    }:
        raise ValueError("historical performance binding has an invalid top-level schema")
    if payload["schema_version"] != _PERFORMANCE_V1_BINDING_SCHEMA:
        raise ValueError("historical performance binding has an unsupported schema")
    if payload["source_commit"] != PERFORMANCE_V1_SOURCE_COMMIT:
        raise ValueError("historical performance binding has the wrong source commit")
    if payload["source_tree"] != _PERFORMANCE_V1_SOURCE_TREE:
        raise ValueError("historical performance binding has the wrong source tree")
    if payload["fixture_digest"] != _PERFORMANCE_V1_FIXTURE_DIGEST:
        raise ValueError("historical performance binding has the wrong fixture digest")
    _validate_historical_fixture_files(payload["fixture_files"])
    encoded = payload["benchmark_config_base64"]
    if not isinstance(encoded, str):
        raise ValueError("historical benchmark config must be base64 text")
    try:
        benchmark_config = base64.b64decode(encoded, validate=True)
    except ValueError as error:
        raise ValueError("historical benchmark config is not canonical base64") from error
    expected_sha256 = payload["benchmark_config_sha256"]
    if not isinstance(expected_sha256, str) or _FULL_SHA256.fullmatch(expected_sha256) is None:
        raise ValueError("historical benchmark config digest is invalid")
    if hashlib.sha256(benchmark_config).hexdigest() != expected_sha256:
        raise ValueError("historical benchmark config digest does not match its bytes")
    budget_entry = next(
        item
        for item in payload["fixture_files"]
        if item["path"] == "tests/integration/clickhouse_cluster/external_replication_performance_budget.json"
    )
    if budget_entry["sha256"] != expected_sha256 or budget_entry["size"] != len(benchmark_config):
        raise ValueError("historical benchmark config does not match the source inventory")
    return HistoricalPerformanceBinding(
        source_tree=_PERFORMANCE_V1_SOURCE_TREE,
        fixture_digest=_PERFORMANCE_V1_FIXTURE_DIGEST,
        benchmark_config=benchmark_config,
    )


def _validate_historical_fixture_files(value: Any) -> None:
    if not isinstance(value, list) or len(value) != len(PERFORMANCE_V1_FIXTURE_FILES):
        raise ValueError("historical fixture inventory is incomplete")
    expected_paths = [path.as_posix() for path in PERFORMANCE_V1_FIXTURE_FILES]
    actual_paths: list[str] = []
    for item in value:
        if not isinstance(item, dict) or set(item) != {"path", "sha256", "size"}:
            raise ValueError("historical fixture inventory entry is invalid")
        path, digest, size = item["path"], item["sha256"], item["size"]
        if not isinstance(path, str) or not isinstance(digest, str) or _FULL_SHA256.fullmatch(digest) is None:
            raise ValueError("historical fixture inventory identity is invalid")
        if type(size) is not int or size <= 0:
            raise ValueError("historical fixture inventory size is invalid")
        actual_paths.append(path)
    if actual_paths != expected_paths:
        raise ValueError("historical fixture inventory paths are not canonical")


def _fixture_digest(source_commit: str, files: tuple[Path, ...]) -> str:
    digest = hashlib.sha256()
    for path in files:
        digest.update(path.as_posix().encode("utf-8"))
        digest.update(b"\0")
        content = _git("show", f"{source_commit}:{path.as_posix()}", text=False)
        if not isinstance(content, bytes):
            raise TypeError("git blob output must be bytes")
        digest.update(content)
        digest.update(b"\0")
    return digest.hexdigest()


def tracked_file_bytes(source_commit: str, path: Path) -> bytes:
    """Read one immutable tracked input from the receipt's source commit."""

    content = _git("show", f"{source_commit}:{path.as_posix()}", text=False)
    if not isinstance(content, bytes):
        raise TypeError("git blob output must be bytes")
    return content


def tracked_source_tree(source_commit: str) -> str:
    """Resolve the tree only for an existing, exact lowercase commit SHA."""

    require_exact_source_commit(source_commit)
    try:
        resolved_commit = str(_git("rev-parse", "--verify", f"{source_commit}^{{commit}}")).strip()
    except subprocess.CalledProcessError as error:
        raise ValueError("source commit must resolve to an existing commit") from error
    if resolved_commit != source_commit:
        raise ValueError("source commit must resolve to the exact requested commit")
    return str(_git("rev-parse", f"{source_commit}^{{tree}}")).strip()


def require_exact_source_commit(source_commit: str) -> None:
    """Reject mutable or non-canonical Git revision syntax without invoking Git."""

    if _FULL_LOWERCASE_SHA1.fullmatch(source_commit) is None:
        raise ValueError("source commit must be a full lowercase 40-character SHA-1")


def _git(*args: str, text: bool = True) -> str | bytes:
    return subprocess.run(
        ("git", "--no-replace-objects", *args),
        check=True,
        capture_output=True,
        text=text,
    ).stdout


__all__ = [
    "FIXTURE_FILES",
    "HistoricalPerformanceBinding",
    "PERFORMANCE_V1_SOURCE_COMMIT",
    "fixture_digest",
    "historical_performance_binding",
    "performance_fixture_digest",
    "require_exact_source_commit",
    "source_binding",
    "tracked_file_bytes",
    "tracked_source_tree",
]
